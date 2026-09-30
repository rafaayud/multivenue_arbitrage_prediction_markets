"""Verify final worker validation, bounded IPC, and cancellation-safe lifecycle."""

import asyncio
import queue
import time
from dataclasses import replace
from decimal import Decimal
from types import SimpleNamespace

import pytest

from prediction_markets.api.runtime.market_workers import (
    MarketWorkerMode,
    MarketWorkerPartition,
    MarketWorkerSupervisor,
    _CONTROL_QUEUE_CAPACITY,
    _METRICS_QUEUE_CAPACITY,
    _VALIDATION_INTENT_CAPACITY,
    _WorkerJournal,
    _WorkerTelemetry,
    _apply_worker_controls,
    _validate_worker_opportunity,
    _wait_for_process_stop,
)
from prediction_markets.application.events import MarketMatchesUpdated, OpportunityValidationRef, OrderBookPairUpdated
from prediction_markets.application.state import TradingState
from prediction_markets.application.worker_validation import (
    WorkerOpportunityValidationRequest,
    WorkerOpportunityValidationResult,
    WorkerValidationReason as Reason,
)
from prediction_markets.domain.orderbook.value_objects import OrderBookLevel
from prediction_markets.domain.shared.value_objects import Currency, Money, Price, Quantity
from prediction_markets.domain.trading.enums import OrderSide
from prediction_markets.infrastructure.operational_metrics import (
    MARKET_WORKER_VALIDATION_TIMEOUTS,
    MARKET_WORKER_VALIDATION_TOTAL,
)
from tests.api.test_market_workers import _Sink, _book, _detected, _pair


class _Fees:
    """Record actual quantities while charging a deterministic settlement fee."""

    def __init__(self, fee: str = "0") -> None:
        self.fee = Decimal(fee)
        self.quantities: list[Quantity] = []

    def calculate(self, _contract, _price, quantity, _side):
        """Calculate the configured fee for the full requested quantity."""
        self.quantities.append(quantity)
        return SimpleNamespace(settlement_cost=Money(self.fee * quantity.value, Currency("USD")))


def _setup(*, generation="generation", output=None, metrics=None):
    """Build one emitted intent and the worker state that originally produced it."""
    cycle, pair, left, right = _pair()
    state = TradingState(books={pair.left.id: left, pair.right.id: right}, matches={cycle: (pair,)})
    output = output if output is not None else queue.Queue(maxsize=4)
    telemetry = _WorkerTelemetry("btc-5m", generation, metrics)
    journal = _WorkerJournal("btc-5m", output, state, generation, telemetry)
    journal.append(_detected(cycle, pair, left, right))
    original = next(iter(journal._validation_intents.values()))
    reference = OpportunityValidationRef(original.intent_id, "btc-5m", generation, original.sequence,
                                         original.left_book_generation, original.right_book_generation)
    request = WorkerOpportunityValidationRequest(
        "request", original.detected.id, reference, cycle, pair, OrderSide.BUY,
        Quantity(Decimal("5")), Quantity(Decimal("5")), Price(Decimal("0.4")), Price(Decimal("0.5")), time.time_ns(),
    )
    fees = _Fees()
    engine = SimpleNamespace(state=state, _config=SimpleNamespace(min_net_edge=Decimal("0.01"), cost_buffer=Decimal("0"), max_skew_ms=80),
                             _fees={pair.left.venue_id: fees, pair.right.venue_id: fees})
    return engine, journal, request


@pytest.mark.parametrize("price,expected", [("0.5", Reason.ACCEPTED), ("0.48", Reason.ACCEPTED), ("0.52", Reason.REPRICE), ("0.7", Reason.EDGE_LOST)])
def test_validation_reads_book_mutated_after_preparation(price, expected):
    """Price changes use current state, and a changed generation alone is harmless."""
    engine, journal, request = _setup()
    latest = replace(_book(request.pair.right, price), source_hash="changed during preparation")
    engine.state.books[request.pair.right.id] = latest
    result = _validate_worker_opportunity(request, engine, journal)
    assert result.reason is expected
    assert result.request == request
    assert result.right_order_book is latest
    assert result.right_book_generation != request.validation_ref.right_book_generation
    assert result.responded_wall_at_ns >= request.sent_wall_at_ns


def test_polymarket_buy_price_improvement_requires_reprice():
    """Re-sign a Polymarket BUY instead of spending its stale FAK notional."""
    engine, journal, request = _setup()
    engine.state.books[request.pair.left.id] = _book(request.pair.left, "0.39")

    assert _validate_worker_opportunity(request, engine, journal).reason is Reason.REPRICE


def test_validation_checks_complete_depth_and_actual_leg_quantities():
    """Aggregate depth at the prepared limit and charge fees at each actual quantity."""
    engine, journal, request = _setup()
    request = replace(request, left_quantity=Quantity(Decimal("6")))
    left = engine.state.books[request.pair.left.id]
    engine.state.books[request.pair.left.id] = replace(left, asks=(
        OrderBookLevel(Price(Decimal("0.39")), Quantity(Decimal("3"))),
        OrderBookLevel(Price(Decimal("0.40")), Quantity(Decimal("3"))),
    ))
    assert _validate_worker_opportunity(request, engine, journal).reason is Reason.ACCEPTED
    assert engine._fees[request.pair.left.venue_id].quantities == [request.left_quantity, request.right_quantity]
    engine.state.books[request.pair.left.id] = replace(left, asks=(OrderBookLevel(Price(Decimal("0.4")), Quantity(Decimal("5"))),))
    assert _validate_worker_opportunity(request, engine, journal).reason is Reason.INSUFFICIENT_DEPTH


def test_validation_rejects_fee_and_configured_edge_loss():
    """The configured buffer and fee costs participate in the final decision."""
    engine, journal, request = _setup()
    engine._config.cost_buffer = Decimal("0.09")
    assert _validate_worker_opportunity(request, engine, journal).reason is Reason.EDGE_LOST
    engine._config.cost_buffer = Decimal("0")
    engine._fees[request.pair.left.venue_id].fee = Decimal("0.06")
    assert _validate_worker_opportunity(request, engine, journal).reason is Reason.EDGE_LOST


@pytest.mark.parametrize("price,expected", [("0.60", Reason.ACCEPTED), ("0.62", Reason.ACCEPTED), ("0.58", Reason.REPRICE), ("0.48", Reason.EDGE_LOST)])
def test_sell_validation_uses_bid_depth_and_protected_minimum(price, expected):
    """SELL validation accepts improved bids and reprices only profitable weaker bids."""
    engine, journal, request = _setup()
    original = journal._validation_intents[request.validation_ref.intent_id]
    journal._validation_intents[original.intent_id] = replace(
        original, detected=replace(original.detected, opportunity=replace(original.detected.opportunity, side=OrderSide.SELL)),
    )
    request = replace(request, side=OrderSide.SELL, left_limit_price=Price(Decimal("0.60")))
    engine.state.books[request.pair.left.id] = _book(request.pair.left, price, bid=True)
    engine.state.books[request.pair.right.id] = _book(request.pair.right, "0.50", bid=True)
    assert _validate_worker_opportunity(request, engine, journal).reason is expected


@pytest.mark.parametrize("change,expected", [
    ("local", Reason.STALE_LOCAL), ("source", Reason.STALE_SOURCE),
    ("missing_source", Reason.STALE_SOURCE), ("missing_arrival", Reason.STALE_LOCAL),
    ("generation", Reason.GENERATION), ("book_reference", Reason.IDENTITY),
    ("execution", Reason.IDENTITY),
    ("pair", Reason.IDENTITY), ("quantity", Reason.IDENTITY),
    ("request_age", Reason.STALE_REQUEST), ("missing_book", Reason.MISSING_BOOK),
])
def test_validation_fails_closed_for_stale_or_mismatched_state(change, expected):
    """Reject unavailable source state and mismatched original admission identities."""
    engine, journal, request = _setup()
    key = request.pair.left.id
    book = engine.state.books[key]
    if change == "local":
        engine.state.books[key] = replace(book, received_at_ns=time.monotonic_ns() - 200_000_000)
    elif change == "source":
        engine.state.books[key] = replace(book, source_timestamp_kind="venue_update", source_at_ns=time.time_ns() - 500_000_000)
    elif change == "missing_source":
        engine.state.books[key] = replace(book, source_timestamp_kind="venue_update", source_at_ns=None)
    elif change == "missing_arrival":
        engine.state.books[key] = replace(book, arrival_wall_at_ns=None)
    elif change == "generation":
        request = replace(request, validation_ref=replace(request.validation_ref, process_generation="old"))
    elif change == "execution":
        request = replace(request, execution_id="different-execution")
    elif change == "book_reference":
        request = replace(request, validation_ref=replace(request.validation_ref, left_book_generation="wrong"))
    elif change == "pair":
        engine.state.matches.clear()
    elif change == "quantity":
        request = replace(request, left_quantity=Quantity(Decimal("0")))
    elif change == "request_age":
        request = replace(request, sent_monotonic_at_ns=time.monotonic_ns() - 100_000_000)
    else:
        engine.state.books.pop(key)
    assert _validate_worker_opportunity(request, engine, journal).reason is expected


def test_snapshot_source_age_and_explicit_local_only_policy():
    """Snapshot mutation time is not treated as the arrival time of a live update."""
    engine, journal, request = _setup()
    key = request.pair.left.id
    old = replace(engine.state.books[key], source_at_ns=time.time_ns() - 60_000_000_000)
    engine.state.books[key] = old
    assert _validate_worker_opportunity(request, engine, journal).reason is Reason.ACCEPTED
    engine.state.books[key] = replace(old, source_timestamp_kind="venue_update")
    assert _validate_worker_opportunity(replace(request, enforce_source_age=False), engine, journal).reason is Reason.ACCEPTED


def _supervisor(request, *, timeout=0.025):
    """Create a healthy parent with bounded in-memory test queues."""
    partition = MarketWorkerPartition("btc-5m", (request.cycle,), (request.pair.left.venue_id, request.pair.right.venue_id))
    supervisor = MarketWorkerSupervisor(SimpleNamespace(sink=_Sink()), min_net_edge=Decimal("0.01"), cost_buffer=Decimal("0"),
                                        mode=MarketWorkerMode.ACTIVE, partitions=(partition,), validation_timeout_seconds=timeout)
    supervisor._process_generations[partition.name] = request.validation_ref.process_generation
    supervisor._processes[partition.name] = SimpleNamespace(is_alive=lambda: True)
    supervisor._worker_started_at[partition.name] = time.monotonic()
    supervisor._controls[partition.name] = queue.Queue(maxsize=_CONTROL_QUEUE_CAPACITY)
    return supervisor


def test_supervisor_preserves_source_and_arrival_clocks():
    """Source and monotonic arrivals survive IPC without wall-based reconstruction."""
    async def check():
        engine, journal, request = _setup()
        supervisor = _supervisor(request)
        task = asyncio.create_task(supervisor.validate_opportunity(request))
        await asyncio.sleep(0)
        reply = _validate_worker_opportunity(request, engine, journal)
        supervisor._consume_validation(reply)
        result = await task
        assert result.reason is Reason.ACCEPTED
        assert result.left_order_book.source_at_ns == reply.left_order_book.source_at_ns
        assert result.left_order_book.arrival_wall_at_ns == reply.left_order_book.arrival_wall_at_ns
        assert result.left_order_book.received_at_ns <= time.monotonic_ns()
        assert result.left_order_book.received_at_ns == reply.left_order_book.received_at_ns
        assert result.left_order_book.arrival_at_ns == reply.left_order_book.arrival_at_ns
        assert not supervisor._pending_validations
    asyncio.run(check())


@pytest.mark.parametrize("action,expected", [("timeout", Reason.TIMEOUT), ("shutdown", Reason.SHUTDOWN), ("restart", Reason.GENERATION),
                                            ("mismatch", Reason.IDENTITY), ("late", Reason.TIMEOUT), ("failure", Reason.UNAVAILABLE)])
def test_supervisor_waiters_fail_closed_and_are_removed(action, expected):
    """Timeout, identity mismatch, lifecycle failure, and old replies cannot authorize."""
    async def check():
        engine, journal, request = _setup()
        supervisor = _supervisor(request, timeout=0.005 if action == "timeout" else 0.025)
        task = asyncio.create_task(supervisor.validate_opportunity(request))
        await asyncio.sleep(0)
        reply = _validate_worker_opportunity(request, engine, journal)
        if action == "shutdown":
            supervisor._resolve_validations(Reason.SHUTDOWN)
        elif action == "failure":
            supervisor._fail(RuntimeError("failed"))
        elif action == "restart":
            supervisor._process_generations["btc-5m"] = "new"
            supervisor._consume_validation(reply)
        elif action == "mismatch":
            supervisor._consume_validation(replace(reply, request=replace(request, execution_id="wrong")))
        elif action == "late":
            supervisor._consume_validation(replace(reply, responded_monotonic_at_ns=request.sent_monotonic_at_ns - 1))
        result = await task
        assert result.reason is expected
        assert not supervisor._pending_validations
        supervisor._consume_validation(reply)
        assert not supervisor._pending_validations
    asyncio.run(check())


def test_validation_cancellation_queue_bounds_and_intent_cache_bounds():
    """No cancelled future remains, and queue or provenance saturation stays bounded."""
    async def check():
        engine, journal, request = _setup()
        supervisor = _supervisor(request)
        task = asyncio.create_task(supervisor.validate_opportunity(request))
        await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert not supervisor._pending_validations
        control = supervisor._controls["btc-5m"]
        while not control.full():
            control.put_nowait(object())
        assert (await supervisor.validate_opportunity(replace(request, request_id="second"))).reason is Reason.QUEUE_FULL
        assert not supervisor._pending_validations
        journal._output = queue.Queue()
        for _ in range(_VALIDATION_INTENT_CAPACITY + 3):
            journal.clear_intent_dedup()
            journal.append(_detected(request.cycle, request.pair, engine.state.books[request.pair.left.id], engine.state.books[request.pair.right.id]))
        assert len(journal._validation_intents) == _VALIDATION_INTENT_CAPACITY
        assert request.validation_ref.intent_id not in journal._validation_intents
    asyncio.run(check())


@pytest.mark.parametrize("budget_ns", [5_000_000, 25_000_000])
@pytest.mark.parametrize("outer_timeout", ["wait_for", "timeout_at"])
def test_shared_deadline_counts_outer_timeout_once(budget_ns, outer_timeout):
    """The outer execution deadline records timeout even when it cancels the inner wait."""
    async def check():
        _, _, request = _setup()
        supervisor = _supervisor(request)
        timeout_count = MARKET_WORKER_VALIDATION_TIMEOUTS.labels("btc-5m")
        outcomes = MARKET_WORKER_VALIDATION_TOTAL.labels("btc-5m", "timeout")
        cancellations = MARKET_WORKER_VALIDATION_TOTAL.labels("btc-5m", "cancelled")
        before = timeout_count._value.get(), outcomes._value.get(), cancellations._value.get()
        deadline = time.monotonic_ns() + budget_ns
        request = replace(request, validation_deadline_at_ns=deadline)
        try:
            if outer_timeout == "wait_for":
                result = await asyncio.wait_for(
                    supervisor.validate_opportunity(request), budget_ns / 1_000_000_000,
                )
                assert result.reason is Reason.TIMEOUT
            else:
                async with asyncio.timeout_at(deadline / 1_000_000_000):
                    await supervisor.validate_opportunity(request)
                pytest.fail("The earlier outer timeout must cancel its pending inner wait")
        except TimeoutError:
            pass
        assert timeout_count._value.get() == before[0] + 1
        assert outcomes._value.get() == before[1] + 1
        assert cancellations._value.get() == before[2]
        assert not supervisor._pending_validations
    asyncio.run(check())


def test_intentional_cancellation_before_shared_deadline_is_not_a_timeout():
    """A user or shutdown cancellation before expiry retains the cancelled outcome."""
    async def check():
        _, _, request = _setup()
        supervisor = _supervisor(request)
        timeout_count = MARKET_WORKER_VALIDATION_TIMEOUTS.labels("btc-5m")
        cancellations = MARKET_WORKER_VALIDATION_TOTAL.labels("btc-5m", "cancelled")
        before = timeout_count._value.get(), cancellations._value.get()
        request = replace(request, validation_deadline_at_ns=time.monotonic_ns() + 25_000_000)
        task = asyncio.create_task(supervisor.validate_opportunity(request))
        await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert timeout_count._value.get() == before[0]
        assert cancellations._value.get() == before[1] + 1
        assert not supervisor._pending_validations
    asyncio.run(check())


@pytest.mark.parametrize("action,expected", [("restart", Reason.GENERATION), ("shutdown", Reason.SHUTDOWN), ("failure", Reason.UNAVAILABLE)])
def test_accepted_reply_cannot_survive_lifecycle_change_before_caller_resumes(action, expected):
    """Invalidate an already resolved success when the worker changes before use."""
    async def check():
        engine, journal, request = _setup()
        supervisor = _supervisor(request)
        task = asyncio.create_task(supervisor.validate_opportunity(request))
        await asyncio.sleep(0)
        supervisor._consume_validation(_validate_worker_opportunity(request, engine, journal))
        if action == "restart":
            supervisor._process_generations["btc-5m"] = "new"
        elif action == "shutdown":
            supervisor._stopping = True
        else:
            supervisor._fail(RuntimeError("failed after reply"))
        assert (await task).reason is expected
        assert not supervisor._pending_validations
    asyncio.run(check())


def test_pending_request_count_is_bounded_even_when_control_queue_drains():
    """A slow responder cannot create unbounded parent futures behind a drained queue."""
    async def check():
        _, _, request = _setup()
        supervisor = _supervisor(request, timeout=1)
        tasks = []
        control = supervisor._controls["btc-5m"]
        try:
            for index in range(_CONTROL_QUEUE_CAPACITY):
                tasks.append(asyncio.create_task(supervisor.validate_opportunity(replace(request, request_id=str(index)))))
                await asyncio.sleep(0)
                control.get_nowait()
            assert len(supervisor._pending_validations) == _CONTROL_QUEUE_CAPACITY
            rejected = await supervisor.validate_opportunity(replace(request, request_id="overflow"))
            assert rejected.reason is Reason.QUEUE_FULL
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
        assert not supervisor._pending_validations
    asyncio.run(check())


def test_worker_reply_queue_saturation_does_not_block_control_shutdown():
    """A full reply queue drops the reply and leaves the control loop cancellable."""
    async def check():
        replies = queue.Queue(maxsize=1)
        replies.put_nowait(object())
        engine, journal, request = _setup(metrics=replies)
        controls = queue.Queue(maxsize=1)
        controls.put_nowait(request)
        task = asyncio.create_task(_apply_worker_controls(controls, engine, journal))
        for _ in range(50):
            if journal._telemetry._drops.get(("validation_reply", "full")):
                break
            await asyncio.sleep(0.002)
        assert journal._telemetry._drops[("validation_reply", "full")] == 1
        assert not task.done()
        assert replies.qsize() == 1
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
    asyncio.run(check())


def _validating_worker(partition, generation, _config, control, events, metrics, stop):
    """Run only public-state validation behind real spawn queues for the test."""
    async def run():
        engine, journal, request = _setup(generation=generation, output=queue.Queue(), metrics=metrics)
        journal._output = events
        journal.append(MarketMatchesUpdated(request.cycle, (request.pair,)))
        journal.clear_intent_dedup()
        journal.append(_detected(request.cycle, request.pair, engine.state.books[request.pair.left.id], engine.state.books[request.pair.right.id]))
        task = asyncio.create_task(_apply_worker_controls(control, engine, journal))
        try:
            await _wait_for_process_stop(stop)
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
    asyncio.run(run())


def test_spawn_validation_roundtrip_and_shutdown_cleanup():
    """Immutable protocol objects survive spawn and parent shutdown joins the worker."""
    async def check():
        _, _, template = _setup()
        supervisor = _supervisor(template, timeout=0.5)
        supervisor._processes.clear()
        supervisor._controls.clear()
        supervisor._worker_target = _validating_worker
        try:
            await supervisor.start()
            assert supervisor._controls["btc-5m"]._maxsize == _CONTROL_QUEUE_CAPACITY
            assert supervisor._metrics_queue._maxsize == _METRICS_QUEUE_CAPACITY
            deadline = time.monotonic() + 15
            while not any(isinstance(event, OrderBookPairUpdated) for event in supervisor._pipeline.sink.events):
                assert time.monotonic() < deadline
                await asyncio.sleep(0.005)
            event = next(event for event in supervisor._pipeline.sink.events if isinstance(event, OrderBookPairUpdated))
            request = replace(template, validation_ref=event.detected.validation_ref, pair=event.detected.pair,
                              sent_wall_at_ns=time.time_ns(), sent_monotonic_at_ns=time.monotonic_ns())
            result = await supervisor.validate_opportunity(request)
            assert result.reason is Reason.ACCEPTED
            assert result.left_book_generation == event.detected.validation_ref.left_book_generation
            assert result.left_order_book.source_at_ns == event.left_order_book.source_at_ns
            processes = tuple(supervisor._processes.values())
        finally:
            await supervisor.stop()
        assert all(not process.is_alive() for process in processes)
        assert not supervisor._pending_validations
        assert not supervisor._tasks
    asyncio.run(check())
