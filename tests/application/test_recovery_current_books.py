"""Verify current worker books reach central recovery planning before signing."""

import asyncio
import time
from dataclasses import replace
from decimal import Decimal

import pytest

from prediction_markets.application.engine import EngineConfig, TradingEngine
from prediction_markets.application.events import (
    MarketMatchesUpdated, OpportunityValidationRef, OrderBookUpdated,
    RecoveryBooksReceived, RecoveryPlanningRequested, SubmissionReceived, SubmitOrder,
    TradingSafetyStop,
)
from prediction_markets.application.markets.models import MarketCycle
from prediction_markets.application.pipeline import EventLoop, EventSink, OutputDispatcher, RingBuffer
from prediction_markets.application.state import EventDispatcher, TradingState
from prediction_markets.domain.market_matching.value_objects import MatchedContractPair, Underlying
from prediction_markets.domain.orderbook.value_objects import OrderBookLevel
from prediction_markets.domain.shared.value_objects import Price, Quantity, Timestamp, VenueID
from prediction_markets.domain.trading.enums import ArbitrageExecutionStatus, OrderSide
from prediction_markets.infrastructure.binary_journal import BinaryJournal
from tests.application.test_engine import _ZeroFees, _book, _contract, _fill, _process
from tests.application.test_pipeline import _BalanceDelayedExecution, _Execution

import prediction_markets.application.pipeline.order_dispatch as order_dispatch
import prediction_markets.application.engine as engine_module


def _setup(tmp_path, *, max_loss="1", short=False, short_prices=("0.12", "0.92")):
    """Create a filled source leg and hold the empty peer until pipeline processing."""
    left = _contract("poly-down", VenueID("POLYMARKET"), "no")
    right = _contract("predict-up", VenueID("PREDICT"), "yes")
    pair = MatchedContractPair(left, right, Timestamp.now())
    state = TradingState()
    engine = TradingEngine(EventDispatcher(state), {
        left.venue_id: _ZeroFees(), right.venue_id: _ZeroFees(),
    })
    engine.enable(EngineConfig(
        max_notional_by_venue={left.venue_id: Decimal("20"), right.venue_id: Decimal("20")},
        max_recovery_loss=Decimal(max_loss), allowed_underlyings=("BTC",),
        allowed_intervals_seconds=(300,),
        execute_long=not short, execute_short=short,
        predict_limit_slippage_ticks=0 if short else 2,
        short_market_keys=frozenset({"cycle:BTC:300"}) if short else frozenset(),
        short_pair_keys=frozenset({pair.key}) if short else frozenset(),
        short_inventory_by_contract={left.id: Quantity(Decimal("5")),
                                     right.id: Quantity(Decimal("5"))} if short else {},
    ))
    _process(engine, MarketMatchesUpdated(
        MarketCycle(Underlying("BTC"), 300), (pair,),
    ))
    source_price, missing_price = short_prices if short else ("0.52", "0.42")
    _process(engine, OrderBookUpdated(left.venue_id, left.id, _book(left, source_price, "8", bid=short)))
    planned = _process(engine, OrderBookUpdated(right.venue_id, right.id, _book(right, missing_price, "10", bid=short)))
    commands = {event.intent.contract_id: event for event in planned if isinstance(event, SubmitOrder)}
    source, missing = commands[left.id], commands[right.id]
    _process(engine, _fill(source, Price(Decimal(source_price)), source.intent.quantity))
    state.execution_worker_refs[source.execution_id] = OpportunityValidationRef(
        "intent", "crypto-slow", "generation", 1, "old-left", "old-right",
    )
    journal = BinaryJournal(tmp_path / "recovery.log")
    inputs, outputs = RingBuffer(32), RingBuffer(32)
    loop = EventLoop(inputs, outputs, journal, engine)
    dispatch = OutputDispatcher(outputs, EventSink(inputs), journal, state,
                                enforce_source_age=False, guard_recovery=engine.guard_recovery)
    return engine, left, right, missing, journal, inputs, outputs, loop, dispatch


@pytest.mark.parametrize("predict_price,expected_price,side", [
    ("0.49", "0.49", OrderSide.BUY), ("0.65", "0.44", OrderSide.SELL),
])
def test_recovery_plans_and_submits_with_latest_worker_price(
    tmp_path, predict_price, expected_price, side,
):
    """Neither stale admission prices nor REST can select the recovery limit."""
    async def run():
        engine, left, right, missing, journal, inputs, outputs, loop, dispatch = _setup(tmp_path)
        calls = []
        books = {
            left.id: _book(left, "0.44", "8", bid=True),
            right.id: _book(right, predict_price, "8"),
        }

        async def current_books(execution_id, reference, contracts):
            calls.append(contracts)
            return tuple(books[contract_id] for contract_id in contracts)

        class NoREST:
            async def get_order_book(self, contract_id):
                raise AssertionError("Recovery must read worker memory, not REST")

        adapters = {left.venue_id: _BalanceDelayedExecution(journal, left.venue_id),
                    right.venue_id: _Execution(journal, right.venue_id)}
        dispatch.configure(adapters, market_data={venue: NoREST() for venue in adapters})
        dispatch.set_worker_validation(None, recovery_books=current_books)
        try:
            await loop.process(_fill(missing, Price(Decimal("0.42")), Quantity(Decimal("0"))))
            request = await outputs.get()
            assert isinstance(request, RecoveryPlanningRequested)
            assert not engine.state.recoveries
            await dispatch._request_recovery_plan(request)
            received = await inputs.get()
            assert isinstance(received, RecoveryBooksReceived)
            assert engine.state.books[right.id].best_ask().price.value == Decimal("0.42")
            await loop.process(received)
            command = await outputs.get()
            assert isinstance(command, SubmitOrder)
            assert command.intent.limit_price.value == Decimal(expected_price)
            assert command.intent.side is side
            assert len(calls) == 1
            # A duplicate answer cannot choose or enqueue a second order.
            await loop.process(received)
            assert outputs.size == 0
            await asyncio.wait_for(dispatch._submit_single(command), 0.5)
            assert len(adapters[command.venue_id].submitted) == 1
            assert len(calls) == 2
            await dispatch._submit_single(command)
            assert len(adapters[command.venue_id].submitted) == 1
            result = await inputs.get()
            assert isinstance(result, SubmissionReceived)
            assert result.result.reference.recovery_data != b"local-pre-submission-guard"
            assert not any(isinstance(entry.event, (RecoveryBooksReceived, RecoveryPlanningRequested))
                           for entry in journal.entries())
        finally:
            await dispatch.close()
            journal.close()
    asyncio.run(run())


@pytest.mark.parametrize("stale_clock", ["local", "source"])
def test_recovery_routes_to_fresh_book_before_signing_without_rest(tmp_path, stale_clock):
    """Reproduce BNB's SELL residual and admit the fresh one-dollar unwind."""
    async def run():
        engine, left, right, missing, journal, inputs, outputs, loop, dispatch = _setup(tmp_path, short=True)
        dispatch._enforce_source_age = True
        for contract_id in (left.id, right.id):
            engine.state.books[contract_id] = replace(
                engine.state.books[contract_id], received_at_ns=time.monotonic_ns() - 10_000_000_000,
            )
        reads = 0

        async def current_books(execution_id, reference, contracts):
            nonlocal reads
            reads += 1
            now_ns, wall_ns = time.monotonic_ns(), time.time_ns()
            poly = replace(
                _book(left, "0.20", "45.43"),
                asks=(OrderBookLevel(Price(Decimal("0.10")), Quantity(Decimal("1.42"))),
                      OrderBookLevel(Price(Decimal("0.20")), Quantity(Decimal("45.43")))),
                received_at_ns=now_ns - 50_000_000,
                source_at_ns=wall_ns - 148_000_000, source_timestamp_kind="venue_update",
            )
            predict = replace(
                _book(right, "0.85", "10", bid=True),
                received_at_ns=now_ns - (2_241_000_000 if stale_clock == "local" else 10_000_000),
                source_at_ns=wall_ns - (200_000_000 if stale_clock == "local" else 500_000_000),
                source_timestamp_kind="venue_update",
            )
            books = {left.id: poly, right.id: predict}
            return tuple(books[contract_id] for contract_id in contracts)

        class NoREST:
            async def get_order_book(self, contract_id):
                raise AssertionError("Route selection must not fetch REST")

        adapter = _Execution(journal, left.venue_id)
        dispatch.configure({left.venue_id: adapter}, market_data={left.venue_id: NoREST(), right.venue_id: NoREST()})
        dispatch.set_worker_validation(None, recovery_books=current_books)
        try:
            await loop.process(_fill(missing, Price(Decimal("0.92")), Quantity(Decimal("0"))))
            await dispatch._request_recovery_plan(await outputs.get())
            received = await inputs.get()
            assert received.fresh_contract_ids == frozenset({left.id})
            await loop.process(received)
            command = await outputs.get()
            assert isinstance(command, SubmitOrder)
            assert command.intent.contract_id == left.id
            assert command.intent.side is OrderSide.BUY
            assert command.intent.quantity.value == Decimal("5")
            assert command.intent.limit_price.value == Decimal("0.20")
            assert engine.state.recoveries[command.execution_id].estimated_vwap.value == Decimal("0.1716")
            await asyncio.wait_for(dispatch._submit_single(command), 0.5)
            assert len(adapter.submitted) == 1
            assert reads == 2
            result = await inputs.get()
            assert result.result.reference.recovery_data != b"local-pre-submission-guard"
        finally:
            await dispatch.close()
            journal.close()
    asyncio.run(run())


def test_changed_price_during_signing_replans_without_sending_stale_order(tmp_path):
    """Use a new command identity and latest price after a definitive local rejection."""
    async def run():
        engine, left, right, missing, journal, inputs, outputs, loop, dispatch = _setup(tmp_path)
        reads = 0

        async def current_books(execution_id, reference, contracts):
            nonlocal reads
            reads += 1
            books = {left.id: _book(left, "0.44", "8", bid=True),
                     right.id: _book(right, "0.49" if reads == 1 else "0.50", "8")}
            return tuple(books[contract_id] for contract_id in contracts)

        adapter = _Execution(journal, right.venue_id)
        dispatch.configure({right.venue_id: adapter})
        dispatch.set_worker_validation(None, recovery_books=current_books)
        try:
            await loop.process(_fill(missing, Price(Decimal("0.42")), Quantity(Decimal("0"))))
            await dispatch._request_recovery_plan(await outputs.get())
            await loop.process(await inputs.get())
            original = await outputs.get()
            await dispatch._submit_single(original)
            assert not adapter.submitted
            rejection = await inputs.get()
            assert rejection.result.reference.recovery_data == b"local-pre-submission-guard"
            await loop.process(rejection)
            await dispatch._request_recovery_plan(await outputs.get())
            await loop.process(await inputs.get())
            replacement = await outputs.get()
            assert replacement.intent.limit_price.value == Decimal("0.50")
            assert replacement.intent.client_order_id != original.intent.client_order_id
            assert engine.state.recoveries[original.execution_id].local_rejections == 1
            await dispatch._submit_single(replacement)
            assert len(adapter.submitted) == 1
        finally:
            await dispatch.close()
            journal.close()
    asyncio.run(run())


@pytest.mark.parametrize("next_route", ["predict_same", "predict_reprice", "poly_unwind"])
def test_sparse_predict_quotes_wait_before_creating_first_recovery(tmp_path, next_route):
    """A cheap ineligible unwind cannot burn attempts while waiting on sparse Predict."""
    async def run():
        engine, left, right, missing, journal, inputs, outputs, loop, dispatch = _setup(
            tmp_path, short=True, short_prices=("0.05", "0.96"),
        )
        dispatch._enforce_source_age = True
        for contract_id in (left.id, right.id):
            engine.state.books[contract_id] = replace(
                engine.state.books[contract_id], received_at_ns=time.monotonic_ns() - 10_000_000_000,
            )
        books = {
            left.id: _book(left, "0.06", "257"),
            right.id: replace(_book(right, "0.95", "1035.61", bid=True),
                              received_at_ns=time.monotonic_ns() - 6_000_000_000),
        }
        reads = 0
        polling = asyncio.Event()
        captured = []
        deadlines = []

        async def current_books(execution_id, reference, contracts):
            nonlocal reads
            reads += 1
            if reads >= 3:
                polling.set()
            return tuple(books[contract_id] for contract_id in contracts)

        class NoREST:
            async def get_order_book(self, contract_id):
                raise AssertionError("Quote waits cannot fetch REST")

        adapters = {left.venue_id: _Execution(journal, left.venue_id),
                    right.venue_id: _Execution(journal, right.venue_id)}
        dispatch.configure(adapters, market_data={venue: NoREST() for venue in adapters})
        dispatch.set_worker_validation(None, recovery_books=current_books,
                                       capture=lambda *args, **kwargs: captured.append(args))

        async def next_command():
            while True:
                output = await outputs.get()
                if isinstance(output, SubmitOrder):
                    return output
                assert isinstance(output, RecoveryPlanningRequested)
                deadlines.append(output.deadline_at_ns)
                await dispatch._request_recovery_plan(output)
                await loop.process(await inputs.get())

        task = None
        try:
            await loop.process(_fill(missing, Price(Decimal("0.96")), Quantity(Decimal("0"))))
            task = asyncio.create_task(next_command())
            await asyncio.wait_for(polling.wait(), 0.5)
            assert not engine.state.recoveries and not engine.state.prepared
            assert len(engine.state.commands) == 2
            assert all(not adapter.submitted for adapter in adapters.values())
            assert engine.state.executions[missing.execution_id].status is ArbitrageExecutionStatus.RECOVERY_PENDING
            selected = left if next_route == "poly_unwind" else right
            price = "0.20" if next_route == "poly_unwind" else "0.94" if next_route == "predict_reprice" else "0.95"
            books[selected.id] = replace(
                _book(selected, price, "1035.61", bid=selected is right),
                source_at_ns=time.time_ns() - 224_000_000, source_timestamp_kind="venue_update",
            )
            command = await asyncio.wait_for(task, 0.5)
            assert len(set(deadlines)) == 1 and len(deadlines) >= 3
            assert len(captured) == 1  # Polling cannot flood capture-trigger IPC.
            assert command.intent.contract_id == selected.id
            assert command.intent.quantity.value == Decimal("5")
            assert command.intent.limit_price.value == Decimal(price)
            assert str(command.intent.client_order_id).endswith("-recovery-1")
            assert engine.state.recoveries[command.execution_id].local_rejections == 0
            await asyncio.wait_for(dispatch._submit_single(command), 0.5)
            assert len(adapters[selected.venue_id].submitted) == 1
            assert 3 <= reads < 15
        finally:
            if task is not None and not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
            await dispatch.close()
            journal.close()
    asyncio.run(run())


@pytest.mark.parametrize("unusable", ["local_age", "source_age", "loss"])
def test_quote_deadline_reviews_once_without_fake_attempts(tmp_path, monkeypatch, unusable):
    """Quiet or inadmissible markets produce one bounded review without signed orders."""
    async def run():
        monkeypatch.setattr(engine_module, "_RECOVERY_QUOTE_WAIT_NS", 120_000_000)
        engine, left, right, missing, journal, inputs, outputs, loop, dispatch = _setup(
            tmp_path, short=True, short_prices=("0.05", "0.96"), max_loss="0.1",
        )
        dispatch._enforce_source_age = True
        reads = 0

        async def current_books(execution_id, reference, contracts):
            nonlocal reads
            reads += 1
            predict = _book(right, "0.85" if unusable == "loss" else "0.95", "1000", bid=True)
            if unusable == "local_age":
                predict = replace(predict, received_at_ns=time.monotonic_ns() - 6_000_000_000)
            elif unusable == "source_age":
                predict = replace(predict, source_at_ns=time.time_ns() - 800_000_000,
                                  source_timestamp_kind="venue_update")
            books = {left.id: _book(left, "0.06", "257"), right.id: predict}
            return tuple(books[contract_id] for contract_id in contracts)

        dispatch.set_worker_validation(None, recovery_books=current_books)
        try:
            await loop.process(_fill(missing, Price(Decimal("0.96")), Quantity(Decimal("0"))))
            deadlines = []
            while outputs.size:
                request = await outputs.get()
                assert isinstance(request, RecoveryPlanningRequested)
                deadlines.append(request.deadline_at_ns)
                await asyncio.wait_for(dispatch._request_recovery_plan(request), 0.3)
                response = await inputs.get()
                await loop.process(response)
                assert not engine.state.recoveries and not engine.state.prepared
            execution = engine.state.executions[missing.execution_id]
            assert execution.status is ArbitrageExecutionStatus.NEEDS_REVIEW
            assert "recovery quote wait expired" in execution.last_error
            assert len(engine.state.commands) == 2 and len(set(deadlines)) == 1
            assert 1 <= reads < 10 and not engine._pending_recovery_books
            await loop.process(response)
            assert outputs.size == 0 and engine.state.executions[execution.id] == execution
        finally:
            await dispatch.close()
            journal.close()
    asyncio.run(run())


@pytest.mark.parametrize("phase", ["planning", "fresh_quote", "after_signing"])
@pytest.mark.parametrize("failure", ["reply_timeout", "unresponsive"])
def test_transient_recovery_lookup_timeouts_retry_without_consuming_orders(
    tmp_path, monkeypatch, phase, failure,
):
    """A transient IPC miss must retry serially in every recovery lookup caller."""
    async def run():
        engine, left, right, missing, journal, inputs, outputs, loop, dispatch = _setup(tmp_path)
        monkeypatch.setattr(order_dispatch, "RECOVERY_BOOKS_TIMEOUT_NS", 20_000_000)
        calls = active = cancelled = 0
        first_failure = 1 if phase == "planning" else 2
        returned = []
        if phase == "fresh_quote":
            engine.state.books[right.id] = replace(
                engine.state.books[right.id], received_at_ns=time.monotonic_ns() - 2_000_000_000,
            )

        async def current_books(execution_id, reference, contracts):
            nonlocal calls, active, cancelled
            calls += 1
            active += 1
            assert active == 1
            try:
                if first_failure <= calls < first_failure + 2:
                    if failure == "reply_timeout":
                        raise TimeoutError("Worker recovery books: timeout")
                    try:
                        await asyncio.Event().wait()
                    finally:
                        cancelled += 1
                books = {left.id: _book(left, "0.44", "8", bid=True),
                         right.id: _book(right, "0.49", "8")}
                pair = tuple(books[contract_id] for contract_id in contracts)
                returned.append(pair)
                return pair
            finally:
                active -= 1

        class NoREST:
            async def get_order_book(self, contract_id):
                raise AssertionError("IPC retries must not fetch REST")

        adapter = _Execution(journal, right.venue_id)
        dispatch.configure({right.venue_id: adapter}, market_data={right.venue_id: NoREST()})
        dispatch.set_worker_validation(None, recovery_books=current_books)
        try:
            await loop.process(_fill(missing, Price(Decimal("0.42")), Quantity(Decimal("0"))))
            await asyncio.wait_for(dispatch._request_recovery_plan(await outputs.get()), 0.5)
            received = await inputs.get()
            assert received.error is None
            assert all(book is original for book, original in zip(received.books, returned[0], strict=True))
            await loop.process(received)
            command = await outputs.get()
            assert isinstance(command, SubmitOrder)
            if phase == "fresh_quote":
                engine.state.books[right.id] = replace(
                    engine.state.books[right.id], received_at_ns=time.monotonic_ns() - 1_300_000_000,
                )
            await asyncio.wait_for(dispatch._submit_single(command), 0.5)
            result = await inputs.get()
            assert result.result.reference.recovery_data != b"local-pre-submission-guard"
            assert len(adapter.submitted) == 1
            recovery = engine.state.recoveries[command.execution_id]
            assert recovery.attempts == 1 and recovery.local_rejections == 0
            assert calls == (5 if phase == "fresh_quote" else 4)
            assert active == 0 and cancelled == (2 if failure == "unresponsive" else 0)
            assert inputs.size == 0 and outputs.size == 0
        finally:
            await dispatch.close()
            journal.close()
    asyncio.run(run())


@pytest.mark.parametrize("action", ["cancel", "superseded", "generation", "safety_stop"])
def test_delayed_recovery_poll_cannot_outlive_its_authority(tmp_path, action):
    """Stop before another IPC request if authority changes during the polling delay."""
    async def run():
        engine, _, _, missing, journal, inputs, outputs, loop, dispatch = _setup(tmp_path)
        calls = 0

        async def current_books(*args):
            nonlocal calls
            calls += 1
            raise AssertionError("Changed authority must stop before lookup")

        dispatch.set_worker_validation(None, recovery_books=current_books)
        try:
            await loop.process(_fill(missing, Price(Decimal("0.42")), Quantity(Decimal("0"))))
            request = replace(await outputs.get(), not_before_ns=time.monotonic_ns() + 100_000_000)
            task = asyncio.create_task(dispatch._request_recovery_plan(request))
            await asyncio.sleep(0.01)
            if action == "cancel":
                task.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await task
                assert inputs.size == 0
            else:
                if action == "superseded":
                    engine.state.executions[missing.execution_id] = replace(
                        request.execution, status=ArbitrageExecutionStatus.COMPLETED,
                    )
                elif action == "generation":
                    reference = engine.state.execution_worker_refs[missing.execution_id]
                    engine.state.execution_worker_refs[missing.execution_id] = replace(reference, process_generation="new")
                else:
                    engine.state.execution_safety_stops[missing.execution_id] = TradingSafetyStop(
                        missing.venue_id, "uncertain order", Timestamp.now(), execution_id=missing.execution_id,
                    )
                await asyncio.wait_for(task, 0.3)
                response = await inputs.get()
                assert "recovery changed during quote wait" in response.error
            assert calls == 0 and not engine.state.recoveries
        finally:
            await dispatch.close()
            journal.close()
    asyncio.run(run())


def test_quote_deadline_cancels_a_pending_ipc_read(tmp_path):
    """The outer quote budget bounds even a worker lookup already in flight."""
    async def run():
        engine, _, _, missing, journal, inputs, outputs, loop, dispatch = _setup(tmp_path)
        active = cancelled = 0

        async def current_books(*args):
            nonlocal active, cancelled
            active += 1
            try:
                await asyncio.Event().wait()
            finally:
                active -= 1
                cancelled += 1

        dispatch.set_worker_validation(None, recovery_books=current_books)
        try:
            await loop.process(_fill(missing, Price(Decimal("0.42")), Quantity(Decimal("0"))))
            request = replace(await outputs.get(), deadline_at_ns=time.monotonic_ns() + 40_000_000)
            engine._pending_recovery_books[missing.execution_id] = request
            await asyncio.wait_for(dispatch._request_recovery_plan(request), 0.3)
            response = await inputs.get()
            assert "recovery quote wait expired" in response.error
            await loop.process(response)
            assert active == 0 and cancelled == 1
            assert not engine._pending_recovery_books and outputs.size == 0
            assert not engine.state.recoveries
            assert engine.state.executions[missing.execution_id].status is ArbitrageExecutionStatus.NEEDS_REVIEW
        finally:
            await dispatch.close()
            journal.close()
    asyncio.run(run())


@pytest.mark.parametrize("action", ["cancel", "superseded", "generation", "safety_stop"])
def test_recovery_lookup_retry_stops_on_cancellation_or_changed_authority(tmp_path, action):
    """A timeout retry cannot outlive cancellation or authorize a changed execution."""
    async def run():
        engine, _, _, missing, journal, inputs, outputs, loop, dispatch = _setup(tmp_path)
        entered = asyncio.Event()
        release = asyncio.Event()
        calls = 0

        async def current_books(execution_id, reference, contracts):
            nonlocal calls
            calls += 1
            entered.set()
            await release.wait()
            raise TimeoutError("Worker recovery books: timeout")

        dispatch.set_worker_validation(None, recovery_books=current_books)
        try:
            await loop.process(_fill(missing, Price(Decimal("0.42")), Quantity(Decimal("0"))))
            request = await outputs.get()
            task = asyncio.create_task(dispatch._request_recovery_plan(request))
            await asyncio.wait_for(entered.wait(), 0.5)
            if action == "cancel":
                task.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await task
                assert inputs.size == 0
            else:
                if action == "superseded":
                    engine.state.executions[missing.execution_id] = replace(
                        request.execution, status=ArbitrageExecutionStatus.COMPLETED,
                    )
                elif action == "generation":
                    reference = engine.state.execution_worker_refs[missing.execution_id]
                    engine.state.execution_worker_refs[missing.execution_id] = replace(reference, process_generation="new")
                else:
                    engine.state.execution_safety_stops[missing.execution_id] = TradingSafetyStop(
                        missing.venue_id, "uncertain order", Timestamp.now(), execution_id=missing.execution_id,
                    )
                release.set()
                await asyncio.wait_for(task, 0.5)
                received = await inputs.get()
                assert "recovery changed" in received.error
                assert received.books is None
            assert calls == 1 and outputs.size == 0
        finally:
            await dispatch.close()
            journal.close()
    asyncio.run(run())


@pytest.mark.parametrize("outcome", ["same_price", "reprice", "timeout", "cancel", "superseded", "source_age"])
def test_stale_recovery_waits_for_a_real_update_before_consuming_requotes(tmp_path, monkeypatch, outcome):
    """A quote aging after planning still waits before the final submission guard."""
    async def run():
        engine, left, right, missing, journal, inputs, outputs, loop, dispatch = _setup(tmp_path)
        monkeypatch.setattr(order_dispatch, "_RECOVERY_FRESH_BOOK_WAIT_SECONDS", 0.15)
        old = replace(_book(right, "0.49", "8"), received_at_ns=time.monotonic_ns() - 1_300_000_000)
        engine.state.books[right.id] = replace(
            engine.state.books[right.id], received_at_ns=old.received_at_ns - 1_000_000_000,
        )
        books = {left.id: _book(left, "0.44", "8", bid=True), right.id: _book(right, "0.49", "8")}
        reads = 0

        async def current_books(execution_id, reference, contracts):
            nonlocal reads
            reads += 1
            return tuple(books[contract_id] for contract_id in contracts)

        class NoREST:
            async def get_order_book(self, contract_id):
                raise AssertionError("Fresh recovery waits must not fetch REST")

        adapter = _Execution(journal, right.venue_id)
        dispatch.configure({right.venue_id: adapter}, market_data={right.venue_id: NoREST()})
        dispatch.set_worker_validation(None, recovery_books=current_books)
        try:
            await loop.process(_fill(missing, Price(Decimal("0.42")), Quantity(Decimal("0"))))
            await dispatch._request_recovery_plan(await outputs.get())
            await loop.process(await inputs.get())
            command = await outputs.get()
            books[right.id] = old
            engine.state.books[right.id] = old
            task = asyncio.create_task(dispatch._submit_single(command))
            await asyncio.sleep(0.05)
            assert not task.done()
            assert inputs.size == 0
            assert not adapter.submitted
            recovery = engine.state.recoveries[command.execution_id]
            assert recovery.attempts == 1 and recovery.local_rejections == 0
            assert books[right.id].received_at_ns == old.received_at_ns
            if outcome == "cancel":
                task.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await task
                assert inputs.size == 0
                return
            if outcome == "superseded":
                engine.state.executions[command.execution_id] = replace(
                    engine.state.executions[command.execution_id], status=ArbitrageExecutionStatus.COMPLETED,
                )
            if outcome in ("same_price", "reprice", "source_age"):
                books[right.id] = _book(right, "0.50" if outcome == "reprice" else "0.49", "8")
                if outcome == "source_age":
                    dispatch._enforce_source_age = True
                    books[right.id] = replace(books[right.id], source_timestamp_kind="venue_update",
                                             source_at_ns=time.time_ns() - 2_000_000_000)
            await asyncio.wait_for(task, 0.4)
            result = await inputs.get()
            assert isinstance(result, SubmissionReceived)
            if outcome == "same_price":
                assert len(adapter.submitted) == 1
                assert result.result.reference.recovery_data != b"local-pre-submission-guard"
                assert engine.state.recoveries[command.execution_id].local_rejections == 0
            else:
                assert not adapter.submitted
                assert result.result.reference.recovery_data == b"local-pre-submission-guard"
                if outcome in ("timeout", "source_age"):
                    assert "fresh-book wait expired" in result.result.reason
                if outcome == "reprice":
                    assert "outside buy limit" in result.result.reason
            assert reads < 20  # One serial bounded lookup, never a busy loop.
        finally:
            await dispatch.close()
            journal.close()
    asyncio.run(run())


@pytest.mark.parametrize("failure", ["worker", "timeout", "loss", "superseded"])
def test_recovery_unavailable_or_superseded_books_cannot_submit(tmp_path, monkeypatch, failure):
    """A bounded read failure or changed execution never sends an unbounded recovery."""
    async def run():
        monkeypatch.setattr(order_dispatch, "_RECOVERY_FRESH_BOOK_WAIT_SECONDS", 0.2)
        monkeypatch.setattr(engine_module, "_RECOVERY_QUOTE_WAIT_NS", 150_000_000)
        engine, left, right, missing, journal, inputs, outputs, loop, dispatch = _setup(tmp_path, max_loss="0.1")

        async def current_books(execution_id, reference, contracts):
            if failure == "worker":
                raise RuntimeError("Worker recovery books: process_generation")
            if failure == "timeout":
                await asyncio.Event().wait()
            books = {left.id: _book(left, "0.44", "8", bid=True),
                     right.id: _book(right, "0.65", "8")}
            return tuple(books[contract_id] for contract_id in contracts)

        dispatch.set_worker_validation(None, recovery_books=current_books)
        try:
            await loop.process(_fill(missing, Price(Decimal("0.42")), Quantity(Decimal("0"))))
            request = await outputs.get()
            await asyncio.wait_for(dispatch._request_recovery_plan(request), 0.5)
            if failure == "superseded":
                engine.state.executions[missing.execution_id] = replace(
                    request.execution, status=ArbitrageExecutionStatus.COMPLETED,
                )
            await loop.process(await inputs.get())
            while failure == "loss" and outputs.size:
                retry = await outputs.get()
                assert isinstance(retry, RecoveryPlanningRequested)
                await dispatch._request_recovery_plan(retry)
                await loop.process(await inputs.get())
            assert outputs.size == 0
            expected = (ArbitrageExecutionStatus.COMPLETED if failure == "superseded"
                        else ArbitrageExecutionStatus.NEEDS_REVIEW)
            assert engine.state.executions[missing.execution_id].status is expected
        finally:
            await dispatch.close()
            journal.close()
    asyncio.run(run())


def test_recovery_book_aging_during_signing_waits_before_final_guard(tmp_path):
    """A slow signer still needs a new quote, without resubmitting the same command."""
    async def run():
        engine, left, right, missing, journal, inputs, outputs, loop, dispatch = _setup(tmp_path)
        dispatch._max_book_age_ns = 100_000_000
        books = {left.id: _book(left, "0.44", "8", bid=True), right.id: _book(right, "0.49", "8")}

        async def current_books(execution_id, reference, contracts):
            return tuple(books[contract_id] for contract_id in contracts)

        class SlowSigner(_Execution):
            def prepare(self, intent):
                time.sleep(0.13)
                return super().prepare(intent)

        adapter = SlowSigner(journal, right.venue_id)
        dispatch.configure({right.venue_id: adapter})
        dispatch.set_worker_validation(None, recovery_books=current_books)
        try:
            await loop.process(_fill(missing, Price(Decimal("0.42")), Quantity(Decimal("0"))))
            await dispatch._request_recovery_plan(await outputs.get())
            await loop.process(await inputs.get())
            command = await outputs.get()
            task = asyncio.create_task(dispatch._submit_single(command))
            await asyncio.sleep(0.18)
            assert not task.done() and not adapter.submitted and inputs.size == 0
            books[right.id] = _book(right, "0.49", "8")
            await asyncio.wait_for(task, 0.5)
            assert len(adapter.submitted) == 1
            assert engine.state.recoveries[command.execution_id].local_rejections == 0
        finally:
            await dispatch.close()
            journal.close()
    asyncio.run(run())


@pytest.mark.parametrize("status", [
    ArbitrageExecutionStatus.COMPLETED, ArbitrageExecutionStatus.RECOVERED,
    ArbitrageExecutionStatus.NEEDS_REVIEW,
])
def test_queued_recovery_cannot_submit_after_execution_becomes_terminal(tmp_path, status):
    """A delayed output must not reopen already settled or reviewed exposure."""
    async def run():
        engine, left, right, missing, journal, inputs, outputs, loop, dispatch = _setup(tmp_path)

        async def current_books(execution_id, reference, contracts):
            books = {left.id: _book(left, "0.44", "8", bid=True),
                     right.id: _book(right, "0.49", "8")}
            return tuple(books[contract_id] for contract_id in contracts)

        adapter = _Execution(journal, right.venue_id)
        dispatch.configure({right.venue_id: adapter})
        dispatch.set_worker_validation(None, recovery_books=current_books)
        try:
            await loop.process(_fill(missing, Price(Decimal("0.42")), Quantity(Decimal("0"))))
            await dispatch._request_recovery_plan(await outputs.get())
            await loop.process(await inputs.get())
            command = await outputs.get()
            engine.state.executions[missing.execution_id] = replace(
                engine.state.executions[missing.execution_id], status=status,
            )
            await dispatch._submit_single(command)
            assert not adapter.submitted
            assert not engine.state.prepared
            assert inputs.size == 0
        finally:
            await dispatch.close()
            journal.close()
    asyncio.run(run())


def test_suppressed_recovery_io_during_reconciliation_is_explicit_review(tmp_path):
    """Reconciliation cannot leave an invisible, orphaned pending book request."""
    async def run():
        engine, left, right, missing, journal, inputs, outputs, loop, dispatch = _setup(tmp_path)
        try:
            await loop.process(
                _fill(missing, Price(Decimal("0.42")), Quantity(Decimal("0"))),
                enqueue_commands=False,
            )
            assert outputs.size == 0
            execution = engine.state.executions[missing.execution_id]
            assert execution.status is ArbitrageExecutionStatus.NEEDS_REVIEW
            assert "journal reconciliation" in execution.last_error
            assert not engine._pending_recovery_books
        finally:
            await dispatch.close()
            journal.close()
    asyncio.run(run())
