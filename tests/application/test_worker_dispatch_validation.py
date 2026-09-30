"""Verify worker evidence at the parent preparation and durable submission boundary."""

import asyncio
import time
from dataclasses import replace
from decimal import Decimal

import pytest

from prediction_markets.application.engine import EngineConfig, TradingEngine
from prediction_markets.application.events import (
    ExecutionPreparationRequested,
    MarketMatchesUpdated,
    OpportunityValidationRef,
    OrderBookUpdated,
    PreparedExecutionBatch,
    SubmissionReceived,
)
from prediction_markets.application.pipeline import EventSink, OutputDispatcher, RingBuffer
from prediction_markets.application.state import EventDispatcher, TradingState
from prediction_markets.application.worker_validation import (
    WorkerOpportunityValidationResult,
    WorkerValidationReason as Reason,
)
from prediction_markets.domain.shared.value_objects import Price, Quantity, VenueID
from prediction_markets.domain.trading.value_objects import PreparedOrder
from prediction_markets.infrastructure.binary_journal import BinaryJournal
from tests.api.test_market_workers import _book, _detected, _pair
from tests.application.test_pipeline import _Execution, _ZeroFees


class _SignedExecution(_Execution):
    """Record each signed price and quantity so obsolete payload reuse is visible."""

    def __init__(self, journal, venue):
        super().__init__(journal, venue)
        self.prepared_intents = []
        self.prepared_orders = []

    def prepare(self, intent):
        """Produce distinguishable immutable payloads for successive preparations."""
        self.prepared_intents.append(intent)
        reference = super().prepare(intent).reference
        prepared = PreparedOrder(reference, f"{intent.limit_price}:{intent.quantity}:{len(self.prepared_intents)}".encode())
        self.prepared_orders.append(prepared)
        return prepared


class _ForbiddenSnapshots:
    """Fail if final worker validation attempts a REST snapshot request."""

    async def get_order_book(self, _contract):
        """Reject any snapshot fetch during worker revalidation."""
        raise AssertionError("Worker validation must use in-memory books")


def _staged(tmp_path, *, worker=True, edge_budget=False):
    """Stage a real central engine plan and connect recorded execution adapters."""
    cycle, pair, left, right = _pair()
    if edge_budget:
        pair = replace(pair, right=replace(pair.right, venue_id=VenueID("PREDICT")))
    state = TradingState()
    state.apply(MarketMatchesUpdated(cycle, (pair,)))
    for contract, book in ((pair.left, left), (pair.right, right)):
        state.apply(OrderBookUpdated(contract.venue_id, contract.id, book))
    engine = TradingEngine(EventDispatcher(state), {pair.left.venue_id: _ZeroFees(), pair.right.venue_id: _ZeroFees()})
    engine.enable(EngineConfig(
        max_notional_by_venue={pair.left.venue_id: Decimal("10"), pair.right.venue_id: Decimal("10")},
        collateral_by_venue={pair.left.venue_id: Decimal("20"), pair.right.venue_id: Decimal("20")},
        min_net_edge=Decimal("0.01"), predict_limit_slippage_ticks=0,
        predict_use_edge_budget=edge_budget,
    ))
    reference = OpportunityValidationRef("generation:1", "btc-5m", "generation", 1, "initial-left", "initial-right") if worker else None
    detected = replace(_detected(cycle, pair, left, right), validation_ref=reference)
    planned, primary, hedge = engine.stage_opportunity(detected)
    request = ExecutionPreparationRequested(detected, planned, (primary, hedge),
                                           time.monotonic_ns() + 1_000_000_000, time.time_ns() + 1_000_000_000)
    journal = BinaryJournal(tmp_path / "worker-dispatch.log")
    inputs = RingBuffer(16)
    dispatcher = OutputDispatcher(RingBuffer(2), EventSink(inputs), journal, state,
        commit_prepared_execution=engine.commit_prepared_execution,
        abort_staged_execution=engine.abort_staged_execution,
        reprice_staged_execution=engine.reprice_staged_execution)
    adapters = {contract.venue_id: _SignedExecution(journal, contract.venue_id) for contract in (pair.left, pair.right)}
    dispatcher.configure(adapters, market_data={venue: _ForbiddenSnapshots() for venue in adapters})
    return engine, request, journal, inputs, dispatcher, adapters


def _reply(request, reason, *, right_price="0.5", left_price="0.4"):
    """Return fresh complete books with changed identities and the exact request."""
    return WorkerOpportunityValidationResult(
        request, reason, time.time_ns(),
        replace(_book(request.pair.left, left_price), source_hash="latest-left"),
        replace(_book(request.pair.right, right_price), source_hash="latest-right"),
        "latest-left-generation", "latest-right-generation",
    )


@pytest.mark.parametrize("wall_step", [-2_000_000_000, 0, 2_000_000_000])
def test_current_worker_books_are_checked_after_preparation_and_after_durability(tmp_path, wall_step):
    """A changed book identity with executable limits still submits both signed legs."""
    async def run():
        engine, staged, journal, _, dispatcher, adapters = _staged(tmp_path)
        requests = []
        async def validate(request):
            requests.append(request)
            assert all(len(adapter.prepared_intents) == 1 for adapter in adapters.values())
            assert len(journal.entries()) == len(requests) - 1
            reply = _reply(request, Reason.ACCEPTED, right_price="0.48")
            return replace(reply, responded_wall_at_ns=reply.responded_wall_at_ns + wall_step)
        dispatcher.set_worker_validation(validate)
        try:
            await dispatcher._dispatch_prepared_execution(staged)
            assert len(requests) == 2
            assert requests[0].request_id != requests[1].request_id
            assert requests[0].execution_id == requests[1].execution_id == staged.planned.execution.id
            assert requests[0].validation_ref == requests[1].validation_ref == staged.opportunity.validation_ref
            for request in requests:
                for contract, quantity, price in (
                    (request.pair.left, request.left_quantity, request.left_limit_price),
                    (request.pair.right, request.right_quantity, request.right_limit_price),
                ):
                    intent = adapters[contract.venue_id].prepared_intents[0]
                    assert (quantity, price) == (intent.quantity, intent.limit_price)
            assert all(len(adapter.submitted) == 1 for adapter in adapters.values())
            assert engine.state.books[staged.opportunity.pair.right.id].best_ask().price == Price(Decimal("0.48"))
            assert engine.state.timings[staged.planned.execution.id].worker_validation_count == 2
        finally:
            await dispatcher.close()
            journal.close()
    asyncio.run(run())


@pytest.mark.parametrize("final_reason", [Reason.ACCEPTED, Reason.REPRICE, Reason.EDGE_LOST])
def test_one_reprice_repeats_central_risk_and_signing_then_validates_final_books(tmp_path, final_reason):
    """One repricing preserves execution and deadline, while later mutation blocks sends."""
    async def run():
        engine, staged, journal, inputs, dispatcher, adapters = _staged(tmp_path)
        requests = []
        replanned = []
        def reprice(request, left, right):
            revised = engine.reprice_staged_execution(request, left, right)
            replanned.append(revised)
            return revised
        dispatcher._reprice_staged_execution = reprice
        async def validate(request):
            requests.append(request)
            if len(requests) == 1:
                return _reply(request, Reason.REPRICE, right_price="0.52")
            assert len(journal.entries()) == 1
            assert request.right_limit_price == Price(Decimal("0.52"))
            return _reply(request, final_reason, right_price="0.52" if final_reason is Reason.ACCEPTED else "0.54")
        dispatcher.set_worker_validation(validate)
        try:
            await dispatcher._dispatch_prepared_execution(staged)
            assert len(replanned) == 1 and replanned[0] is not None
            revised = replanned[0]
            assert revised.planned.execution.id == staged.planned.execution.id
            assert revised.deadline_at_ns == staged.deadline_at_ns
            assert revised.deadline_wall_at_ns == staged.deadline_wall_at_ns
            assert revised.opportunity.validation_ref == staged.opportunity.validation_ref
            assert len(requests) == 2
            assert all(len(adapter.prepared_orders) == 2 for adapter in adapters.values())
            batch = journal.entries()[0].event
            assert isinstance(batch, PreparedExecutionBatch)
            assert batch.commands == revised.commands
            assert batch.deadline_wall_at_ns == staged.deadline_wall_at_ns
            for contract in (revised.opportunity.pair.left, revised.opportunity.pair.right):
                adapter = adapters[contract.venue_id]
                assert adapter.prepared_orders[0] != adapter.prepared_orders[1]
                expected = [adapter.prepared_orders[-1]] if final_reason is Reason.ACCEPTED else []
                assert adapter.submitted == expected
            if final_reason is not Reason.ACCEPTED:
                rejected = (await inputs.get(), await inputs.get())
                assert all(isinstance(event, SubmissionReceived) and "worker" in event.result.reason for event in rejected)
            assert engine.state.timings[staged.planned.execution.id].worker_reprices == 1
            assert set(engine._notional_reservations) == {staged.planned.execution.id}
        finally:
            await dispatcher.close()
            journal.close()
    asyncio.run(run())


def test_reprice_cannot_bypass_central_collateral_limits(tmp_path):
    """A profitable new worker quote still needs authoritative available collateral."""
    async def run():
        engine, staged, journal, inputs, dispatcher, adapters = _staged(tmp_path)
        calls = 0
        async def validate(request):
            nonlocal calls
            calls += 1
            engine._collateral_balance_by_venue = {venue: Decimal("0") for venue in adapters}
            return _reply(request, Reason.REPRICE, right_price="0.52")
        dispatcher.set_worker_validation(validate)
        try:
            await dispatcher._dispatch_prepared_execution(staged)
            assert calls == 1
            assert all(not adapter.submitted for adapter in adapters.values())
            assert "central admission or risk" in journal.entries()[0].event.rejection_reason
            assert not engine._notional_reservations
            assert not engine._collateral_reservations
            assert inputs.size == 2
        finally:
            await dispatcher.close()
            journal.close()
    asyncio.run(run())


def test_reprice_can_swap_liquidity_roles_without_losing_timing_identity(tmp_path):
    """A changed primary leg retains per-contract history and the original deadline."""
    engine, staged, journal, _, _, _ = _staged(tmp_path)
    try:
        pair = staged.opportunity.pair
        original_primary = staged.commands[0].intent.contract_id
        assert original_primary == pair.left.id
        timings = engine.state.timings[staged.planned.execution.id]
        original_marks = dict(timings.book_received_at_ns)
        left = _book(pair.left, "0.4")
        right = _book(pair.right, "0.52")
        right = replace(right, asks=(replace(right.asks[0], quantity=Quantity(Decimal("6"))),))
        revised = engine.reprice_staged_execution(staged, left, right)
        assert revised is not None
        assert revised.commands[0].intent.contract_id == pair.right.id
        assert revised.commands[1].intent.contract_id == pair.left.id
        assert revised.deadline_at_ns == staged.deadline_at_ns
        assert engine.state.timings[staged.planned.execution.id] is timings
        assert timings.venue_ids == {"primary": str(pair.right.venue_id), "hedge": str(pair.left.venue_id)}
        assert timings.book_received_at_ns == {"primary": original_marks["hedge"], "hedge": original_marks["primary"]}
        assert len(engine._admission_routes) == len(engine._notional_reservations) == len(engine._collateral_reservations) == 1
        assert sum(engine._reserved_notional_by_venue.values()) == sum(engine._notional_reservations[staged.planned.execution.id].values())
    finally:
        journal.close()


def test_edge_budget_reprice_preserves_pricing_history_and_actual_signed_limit(tmp_path):
    """Recompute from new quotes, keep both pricing traces, and sign only final limits."""
    async def run():
        engine, staged, journal, _, dispatcher, adapters = _staged(tmp_path, edge_budget=True)
        requests = []
        async def validate(request):
            requests.append(request)
            if len(requests) == 1:
                assert request.right_limit_price == Price(Decimal("0.58"))
                return _reply(request, Reason.REPRICE, left_price="0.35", right_price="0.60")
            assert request.right_limit_price == Price(Decimal("0.63"))
            return _reply(request, Reason.ACCEPTED, left_price="0.35", right_price="0.62")
        dispatcher.set_worker_validation(validate)
        try:
            await dispatcher._dispatch_prepared_execution(staged)
            assert len(requests) == 2
            timings = engine.state.timings[staged.planned.execution.id]
            trace = timings.snapshot(staged.planned.execution.id)["entry_pricing"]
            assert [decision["mode"] for decision in trace] == ["edge_budget", "edge_budget"]
            assert [decision["detected_limit"] for decision in trace] == ["0.50", "0.60"]
            assert [Decimal(decision["planned_limit"]) for decision in trace] == [Decimal("0.58"), Decimal("0.63")]
            assert all(decision["risk_pricing_ms"] >= decision["edge_budget_search_ms"] >= 0 for decision in trace)
            for adapter in adapters.values():
                assert len(adapter.prepared_orders) == 2
                assert adapter.submitted == [adapter.prepared_orders[-1]]
            assert journal.entries()[0].event.commands[-1].intent.limit_price == Price(Decimal("0.63"))
        finally:
            await dispatcher.close()
            journal.close()
    asyncio.run(run())


@pytest.mark.parametrize("worker", [True, False])
def test_missing_validator_fails_closed_only_for_worker_origin(tmp_path, worker):
    """A local-only pair keeps its existing guard; worker provenance requires a reply."""
    async def run():
        _, staged, journal, inputs, dispatcher, adapters = _staged(tmp_path, worker=worker)
        try:
            await dispatcher._dispatch_prepared_execution(staged)
            assert all(len(adapter.submitted) == (0 if worker else 1) for adapter in adapters.values())
            if worker:
                assert journal.entries()[0].event.rejection_reason == "worker validation unavailable"
                assert inputs.size == 2
        finally:
            await dispatcher.close()
            journal.close()
    asyncio.run(run())


def test_local_polymarket_buy_improvement_reprices_before_signing(tmp_path):
    """Replan a parent-owned Polymarket BUY before creating signed payloads."""
    async def run():
        engine, staged, journal, _, dispatcher, adapters = _staged(
            tmp_path,
            worker=False,
        )
        pair = staged.opportunity.pair
        improved = replace(
            _book(pair.left, "0.39"),
            source_hash="local-polymarket-improvement",
        )
        engine.state.apply(OrderBookUpdated(pair.left.venue_id, pair.left.id, improved))
        try:
            await dispatcher._dispatch_prepared_execution(staged)

            polymarket = adapters[pair.left.venue_id]
            assert len(polymarket.prepared_intents) == 1
            assert polymarket.prepared_intents[0].limit_price == Price(Decimal("0.39"))
            assert polymarket.submitted == polymarket.prepared_orders
            batch = journal.entries()[0].event
            assert isinstance(batch, PreparedExecutionBatch)
            command = next(
                item for item in batch.commands
                if item.intent.contract_id == pair.left.id
            )
            assert command.intent.limit_price == Price(Decimal("0.39"))
        finally:
            await dispatcher.close()
            journal.close()

    asyncio.run(run())


def test_validation_timeout_is_bounded_and_never_submits(tmp_path):
    """A stalled worker is cancelled after the dispatch timeout without sending orders."""
    async def run():
        _, staged, journal, _, dispatcher, adapters = _staged(tmp_path)
        cancelled = asyncio.Event()
        async def validate(_request):
            try:
                await asyncio.Event().wait()
            finally:
                cancelled.set()
        dispatcher.set_worker_validation(validate)
        try:
            await asyncio.wait_for(dispatcher._dispatch_prepared_execution(staged), timeout=0.5)
            assert cancelled.is_set()
            assert all(not adapter.submitted for adapter in adapters.values())
            assert journal.entries()[0].event.rejection_reason == "worker validation timeout"
        finally:
            await dispatcher.close()
            journal.close()
    asyncio.run(run())


def test_cancellation_during_validation_releases_unjournaled_reservations(tmp_path):
    """Cancellation removes the validator waiter and every uncommitted reservation."""
    async def run():
        engine, staged, journal, _, dispatcher, adapters = _staged(tmp_path)
        entered, cancelled = asyncio.Event(), asyncio.Event()
        async def validate(_request):
            entered.set()
            try:
                await asyncio.Event().wait()
            finally:
                cancelled.set()
        dispatcher.set_worker_validation(validate)
        task = asyncio.create_task(dispatcher._dispatch_prepared_execution(staged))
        try:
            await asyncio.wait_for(entered.wait(), timeout=0.5)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            assert cancelled.is_set()
            assert all(not adapter.submitted for adapter in adapters.values())
            assert not journal.entries()
            assert not engine._admission_routes
            assert not engine._notional_reservations
            assert not engine._collateral_reservations
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            await dispatcher.close()
            journal.close()
    asyncio.run(run())
