"""Verify the recoverable order boundary around the hot path."""

import asyncio
import threading
import time
from dataclasses import replace
from datetime import timedelta
from decimal import Decimal
from typing import Literal
from unittest.mock import Mock

import pytest

import prediction_markets.application.pipeline.order_dispatch as order_dispatch
from prediction_markets.application.engine import EngineConfig, TradingEngine
from prediction_markets.application.events import (
    ArbitrageOpportunityFound,
    ArbitragePlanned,
    OpportunityValidationRef,
    OrderPrepared,
    OrderSnapshotUpdated,
    PreparedExecutionBatch,
    SubmissionReceived,
    SubmitOrder,
    TradingSafetyStop,
)
from prediction_markets.application.execution.timings import ExecutionTimings
from prediction_markets.application.pipeline import (
    EventLoop,
    EventSink,
    OutputDispatcher,
    RecoveryCoordinator,
    RecoveryError,
    RingBuffer,
    RingBufferFull,
    TradingPipeline,
)
from prediction_markets.application.events import MarketMatchesUpdated, OrderBookUpdated
from prediction_markets.application.markets.models import MarketCycle
from prediction_markets.application.state import EventDispatcher, TradingState
from prediction_markets.domain.contracts.entities import BinaryContract
from prediction_markets.domain.contracts.value_objects import LotSize, TickSize
from prediction_markets.domain.market_matching.value_objects import (
    MatchedContractPair,
    RegularCandidate,
    Underlying,
)
from prediction_markets.domain.markets.entities import Market, MarketSide
from prediction_markets.domain.markets.enums import BinaryOutcome, MarketStatus
from prediction_markets.domain.markets.value_objects import MarketState
from prediction_markets.domain.orderbook.entities import OrderBook
from prediction_markets.domain.orderbook.value_objects import OrderBookLevel
from prediction_markets.domain.ports.execution import ExecutionPort
from prediction_markets.domain.shared.value_objects import (
    ClientOrderID,
    ContractID,
    Currency,
    MarketID,
    Money,
    OutcomeID,
    Price,
    Quantity,
    Timestamp,
    VenueID,
)
from prediction_markets.domain.trading.entities import OrderIntent, OrderSnapshot
from prediction_markets.domain.trading.enums import (
    OrderSide,
    OrderStatus,
    OrderType,
    ReconciliationStatus,
    SubmissionStatus,
    TimeInForce,
)
from prediction_markets.domain.trading.value_objects import (
    OrderReference,
    PreparedOrder,
    ReconciliationResult,
    SubmissionResult,
    TradingFee,
)
from prediction_markets.domain.arbitrage.value_objects import ArbitrageOpportunity
from prediction_markets.infrastructure.binary_journal import BinaryJournal


class _Execution(ExecutionPort):
    """Record exact prepared requests submitted by the dispatcher."""

    def __init__(
        self,
        journal: BinaryJournal,
        venue_id: VenueID = VenueID("venue"),
    ) -> None:
        self.journal = journal
        self.venue_id = venue_id
        self.submitted: list[PreparedOrder] = []
        self.reconciliation = ReconciliationStatus.NOT_FOUND

    def prepare(self, intent: OrderIntent) -> PreparedOrder:
        reference = OrderReference(
            self.venue_id,
            intent.client_order_id,
            b"recovery-key",
        )
        return PreparedOrder(reference, b"signed-request")

    def submit(self, order: PreparedOrder) -> SubmissionResult:
        latest = self.journal.entries()[-1].event
        assert isinstance(latest, OrderPrepared) or (
            isinstance(latest, PreparedExecutionBatch)
            and any(item.prepared == order for item in latest.prepared)
        )
        self.submitted.append(order)
        return SubmissionResult(SubmissionStatus.REJECTED, order.reference)

    def reconcile(self, reference: OrderReference) -> ReconciliationResult:
        return ReconciliationResult(self.reconciliation, reference)

    def cancel(self, reference: OrderReference) -> ReconciliationResult:
        return ReconciliationResult(ReconciliationStatus.NOT_FOUND, reference)


class _ZeroFees:
    """Return zero settlement cost for pipeline planning tests."""

    def calculate(self, contract_id, price, quantity, side) -> TradingFee:
        zero = Money(Decimal("0"), Currency("USD"))
        return TradingFee(zero, zero)


class _ThreadRecordingJournal:
    """Record the thread that receives prepared-order journal appends."""

    def __init__(self) -> None:
        self.append_thread_ids: list[int] = []

    def append(self, _event) -> None:
        self.append_thread_ids.append(threading.get_ident())


class _PrepareFailureExecution(_Execution):
    """Raise a local preparation error without reaching the venue submit call."""

    def prepare(self, intent: OrderIntent) -> PreparedOrder:
        raise ValueError("invalid price from venue tick size")


class _ConcurrentPrepareExecution(_Execution):
    """Require two independent pairs to reach preparation concurrently."""

    def __init__(
        self,
        journal: BinaryJournal,
        venue_id: VenueID,
        barrier: threading.Barrier,
    ) -> None:
        super().__init__(journal, venue_id)
        self._barrier = barrier

    def prepare(self, intent: OrderIntent) -> PreparedOrder:
        self._barrier.wait(timeout=2)
        return super().prepare(intent)


class _ParallelExecution(_Execution):
    """Require both leg submissions to overlap before either can finish."""

    def __init__(
        self,
        journal: BinaryJournal,
        venue_id: VenueID,
        barrier: threading.Barrier,
    ) -> None:
        super().__init__(journal, venue_id)
        self._barrier = barrier

    def submit(self, order: PreparedOrder) -> SubmissionResult:
        self._barrier.wait(timeout=1)
        return super().submit(order)


class _LegacyFundsGuardExecution(_Execution):
    """Expose the removed per-order funds hook and record accidental calls."""

    def __init__(self, journal, venue_id) -> None:
        super().__init__(journal, venue_id)
        self.preflight_calls = 0

    def preflight_funds(
        self,
        intent: OrderIntent,
        order: PreparedOrder,
    ) -> str | None:
        self.preflight_calls += 1
        raise AssertionError("funds preflight must stay outside order submission")


class _MarketData:
    """Return one deterministic fresh order-book snapshot."""

    def __init__(self, book: OrderBook) -> None:
        self.book = book
        self.requested: list[ContractID] = []

    async def get_order_book(self, contract_id: ContractID) -> OrderBook:
        self.requested.append(contract_id)
        return self.book


class _CancellableExecution(_Execution):
    """Accept one order and return a terminal snapshot when cancelled."""

    def __init__(self, journal: BinaryJournal, venue_id: VenueID) -> None:
        super().__init__(journal, venue_id)
        self.intent: OrderIntent | None = None
        self.cancelled: list[OrderReference] = []

    def prepare(self, intent: OrderIntent) -> PreparedOrder:
        self.intent = intent
        return super().prepare(intent)

    def submit(self, order: PreparedOrder) -> SubmissionResult:
        assert self.intent is not None
        self.submitted.append(order)
        return SubmissionResult(
            SubmissionStatus.ACCEPTED,
            order.reference,
            self._snapshot(order.reference, OrderStatus.ACCEPTED),
        )

    def cancel(self, reference: OrderReference) -> ReconciliationResult:
        self.cancelled.append(reference)
        return ReconciliationResult(
            ReconciliationStatus.FOUND,
            reference,
            self._snapshot(reference, OrderStatus.CANCELLED),
        )

    def _snapshot(
        self,
        reference: OrderReference,
        status: OrderStatus,
    ) -> OrderSnapshot:
        assert self.intent is not None
        return OrderSnapshot(
            status=status,
            contract_id=self.intent.contract_id,
            side=self.intent.side,
            quantity=self.intent.quantity,
            order_type=self.intent.order_type,
            client_order_id=reference.client_order_id,
            limit_price=self.intent.limit_price,
            updated_at=Timestamp.now(),
        )


class _TimedCancellationExecution(_CancellableExecution):
    """Declare an application-owned cancellation window."""

    def cancellation_window_seconds(self, intent: OrderIntent) -> float | None:
        del intent
        return 0.01


class _UnknownCancelExecution(_TimedCancellationExecution):
    """Keep an order uncertain while recording cancellation attempts."""

    def cancel(self, reference: OrderReference) -> ReconciliationResult:
        self.cancelled.append(reference)
        return ReconciliationResult(ReconciliationStatus.UNKNOWN, reference)


class _RecoveredTimedExecution(_TimedCancellationExecution):
    """Return one previously observed partial application-timed order."""

    recovered_snapshot: OrderSnapshot | None = None

    def reconcile(self, reference: OrderReference) -> ReconciliationResult:
        assert self.recovered_snapshot is not None
        return ReconciliationResult(
            ReconciliationStatus.FOUND,
            reference,
            self.recovered_snapshot,
        )


class _FeeExecution(_Execution):
    """Return a terminal fill whose fee appears during reconciliation."""

    def __init__(self, journal: BinaryJournal) -> None:
        super().__init__(journal)
        self.intent: OrderIntent | None = None

    def prepare(self, intent: OrderIntent) -> PreparedOrder:
        self.intent = intent
        return super().prepare(intent)

    def submit(self, order: PreparedOrder) -> SubmissionResult:
        assert self.intent is not None
        return SubmissionResult(
            SubmissionStatus.ACCEPTED,
            order.reference,
            self._snapshot(order.reference, fee=None),
        )

    def reconcile(self, reference: OrderReference) -> ReconciliationResult:
        fee = TradingFee(
            Money(Decimal("0.05926"), Currency("USDC")),
            Money(Decimal("0.05926"), Currency("USD")),
        )
        return ReconciliationResult(
            ReconciliationStatus.FOUND,
            reference,
            self._snapshot(reference, fee=fee),
        )

    def _snapshot(
        self,
        reference: OrderReference,
        *,
        fee: TradingFee | None,
    ) -> OrderSnapshot:
        """Build the cumulative terminal snapshot returned by this fake venue."""
        assert self.intent is not None
        return OrderSnapshot(
            status=OrderStatus.FILLED,
            contract_id=self.intent.contract_id,
            side=self.intent.side,
            quantity=self.intent.quantity,
            order_type=self.intent.order_type,
            client_order_id=reference.client_order_id,
            limit_price=self.intent.limit_price,
            filled_quantity=self.intent.quantity,
            average_price=self.intent.limit_price,
            fee=fee,
            updated_at=Timestamp.now(),
        )


class _BalanceDelayedExecution(_Execution):
    """Accept a Polymarket recovery after two transient balance rejections."""

    def submit(self, order: PreparedOrder) -> SubmissionResult:
        self.submitted.append(order)
        if len(self.submitted) < 3:
            return SubmissionResult(
                SubmissionStatus.REJECTED,
                order.reference,
                reason=(
                    "HTTP 400: not enough balance / allowance: "
                    "the balance is not enough"
                ),
            )
        return SubmissionResult(SubmissionStatus.ACCEPTED, order.reference)


def _command(
    *,
    execution_id: str = "execution-1",
    role: Literal["primary", "hedge", "recovery"] = "primary",
    client_order_id: str = "client-order",
    contract_id: str = "contract",
    venue_id: VenueID = VenueID("venue"),
    side: OrderSide = OrderSide.BUY,
) -> SubmitOrder:
    return SubmitOrder(
        execution_id=execution_id,
        role=role,
        venue_id=venue_id,
        intent=OrderIntent(
            contract_id=ContractID(contract_id),
            side=side,
            quantity=Quantity(Decimal("2")),
            order_type=OrderType.LIMIT,
            client_order_id=ClientOrderID(client_order_id),
            limit_price=Price(Decimal("0.4")),
            time_in_force=TimeInForce.IOC,
        ),
    )


def _contract(
    name: str,
    *,
    venue_id: VenueID = VenueID("left"),
) -> BinaryContract:
    return BinaryContract(
        id=ContractID(name),
        market_id=MarketID(f"{name}-market"),
        outcome_id=OutcomeID(f"{name}-outcome"),
        venue_id=venue_id,
        payout_currency=Currency("USD"),
        tick_size=TickSize(Decimal("0.01")),
        lot_size=LotSize(Decimal("1")),
    )


def _live_book(
    ask: str,
    received_at_ns: int,
    quantity: str = "10",
    *,
    bid: bool = False,
) -> OrderBook:
    """Build the minimal one-sided book required by the dispatch guard."""
    level = OrderBookLevel(Price(Decimal(ask)), Quantity(Decimal(quantity)))
    return OrderBook(
        MarketID("market"),
        OutcomeID("yes"),
        (level,) if bid else (),
        () if bid else (level,),
        received_at_ns=received_at_ns,
    )


def test_execution_preparation_uses_one_pre_submission_journal_frame(tmp_path) -> None:
    """Commit opportunity, plan, commands, and payloads in one append."""

    async def run() -> None:
        journal = BinaryJournal(tmp_path / "execution-batch.log")
        state = TradingState()
        dispatcher = EventDispatcher(state)
        left_venue, right_venue = VenueID("left"), VenueID("right")
        left = _contract("left-contract", venue_id=left_venue)
        right = _contract("right-contract", venue_id=right_venue)
        pair = MatchedContractPair(
            left,
            right,
            Timestamp.now() + timedelta(minutes=5),
        )
        cycle = MarketCycle(Underlying("BTC"), 300)
        now_ns = time.monotonic_ns()
        left_level = OrderBookLevel(Price(Decimal("0.4")), Quantity(Decimal("3")))
        right_level = OrderBookLevel(Price(Decimal("0.4")), Quantity(Decimal("3")))
        state.matches[cycle] = (pair,)
        state.contracts.update({left.id: left, right.id: right})
        state.books[left.id] = OrderBook(
            left.market_id,
            left.outcome_id,
            (),
            (left_level,),
            received_at_ns=now_ns,
        )
        state.books[right.id] = OrderBook(
            right.market_id,
            right.outcome_id,
            (),
            (right_level,),
            received_at_ns=now_ns,
        )
        fees = {left_venue: _ZeroFees(), right_venue: _ZeroFees()}
        engine = TradingEngine(dispatcher, fees)
        engine.enable(
            EngineConfig(
                max_notional_by_venue={
                    left_venue: Decimal("10"),
                    right_venue: Decimal("10"),
                },
            )
        )
        pipeline = TradingPipeline(journal, engine)
        left_execution = _Execution(journal, left_venue)
        right_execution = _Execution(journal, right_venue)
        pipeline.output_dispatcher.configure(
            {left_venue: left_execution, right_venue: right_execution}
        )
        opportunity = ArbitrageOpportunity(
            left.id,
            right.id,
            OrderSide.BUY,
            left_level,
            right_level,
            Quantity(Decimal("3")),
            Decimal("0.2"),
            Decimal("0.2"),
            0,
            Timestamp.now(),
        )

        await pipeline.start()
        await pipeline.event_loop.process(
            ArbitrageOpportunityFound("execution-1", cycle, pair, opportunity)
        )
        await asyncio.wait_for(pipeline.outputs.join(), timeout=2)

        batches = [
            entry.event
            for entry in journal.entries()
            if isinstance(entry.event, PreparedExecutionBatch)
        ]
        assert len(batches) == 1
        assert len(batches[0].commands) == len(batches[0].prepared) == 2
        assert not any(
            isinstance(
                entry.event,
                (ArbitrageOpportunityFound, ArbitragePlanned, SubmitOrder, OrderPrepared),
            )
            for entry in journal.entries()
        )
        assert len(left_execution.submitted) == len(right_execution.submitted) == 1
        await pipeline.drain_inputs()
        await pipeline.stop()

        replayed_state = TradingState()
        replayed_engine = TradingEngine(EventDispatcher(replayed_state), fees)
        TradingPipeline(journal, replayed_engine).replay(journal.entries())
        assert "execution-1" in replayed_state.executions
        assert len(replayed_state.commands) == len(replayed_state.prepared) == 2

        expired_journal = BinaryJournal(tmp_path / "expired-batch.log")
        expired_journal.append(
            replace(batches[0], deadline_wall_at_ns=time.time_ns() - 1)
        )
        recovery_inputs = RingBuffer(2)
        recovery_dispatcher = OutputDispatcher(
            RingBuffer(2),
            EventSink(recovery_inputs),
            expired_journal,
            TradingState(),
        )
        recovery_adapters = {
            left_venue: _Execution(expired_journal, left_venue),
            right_venue: _Execution(expired_journal, right_venue),
        }
        await RecoveryCoordinator(
            expired_journal.entries(),
            recovery_dispatcher,
            recovery_adapters,
        ).recover()
        assert not any(adapter.submitted for adapter in recovery_adapters.values())
        recovered = (await recovery_inputs.get(), await recovery_inputs.get())
        assert all(
            isinstance(event, SubmissionReceived)
            and event.result.reason
            == "recovery discarded expired unsubmitted execution"
            for event in recovered
        )
        expired_journal.close()
        journal.close()

    asyncio.run(run())


def test_ring_buffer_wraps_and_reports_saturation_without_blocking() -> None:
    """Reuse fixed slots and fail immediately instead of awaiting capacity."""

    async def run() -> None:
        buffer = RingBuffer[int](2)
        assert buffer.has_capacity
        assert buffer.try_publish(1)
        assert buffer.try_publish(2)
        assert not buffer.has_capacity
        assert buffer.try_publish(3) is False
        assert buffer.capacity == 2
        assert buffer.high_watermark == 2

        assert await buffer.get() == 1
        buffer.task_done()
        assert buffer.try_publish(3)
        assert await buffer.get() == 2
        buffer.task_done()
        assert await buffer.get() == 3
        buffer.task_done()
        await buffer.join()

        dropped = Mock()
        sink = EventSink(RingBuffer(1), dropped)
        assert await sink.publish(
            MarketMatchesUpdated(MarketCycle(Underlying("BTC"), 300), ()),
        )
        try:
            await sink.publish(
                MarketMatchesUpdated(MarketCycle(Underlying("ETH"), 300), ()),
            )
        except RingBufferFull:
            pass
        else:
            raise AssertionError("Critical events must fail fast on saturation")

        assert await sink.publish(
            OrderBookUpdated(
                VenueID("venue"),
                ContractID("book-contract"),
                OrderBook(MarketID("market"), OutcomeID("yes"), (), ()),
            ),
        ) is False
        assert sink.dropped_order_books == 1
        dropped.assert_called_once_with()

    asyncio.run(run())


@pytest.mark.parametrize(
    ("interval_seconds", "guard_seconds"),
    ((300, 30), (900, 60), (3600, 120), (86400, 120)),
)
def test_market_expiry_guard_blocks_a_stale_paired_submission(
    interval_seconds: int,
    guard_seconds: int,
) -> None:
    """Reject a prepared pair when expiry moved inside the safety window."""
    state = TradingState()
    cycle = MarketCycle(Underlying("BNB"), interval_seconds)
    pair = MatchedContractPair(
        _contract("left"),
        _contract("right", venue_id=VenueID("right")),
        Timestamp.now() + timedelta(seconds=guard_seconds - 1),
    )
    state.matches[cycle] = (pair,)
    state.execution_cycles["execution-1"] = cycle
    state.execution_pairs["execution-1"] = pair.key

    error = order_dispatch._market_expiry_guard_error(
        (_command(), _command(role="hedge", client_order_id="client-hedge")),
        state,
        120,
    )

    assert error == (
        "market expiry guard: paired submission skipped with less than "
        f"{guard_seconds} seconds remaining"
    )


def test_market_expiry_guard_skips_regular_candidate_event_time() -> None:
    """Do not treat a regular sports kickoff as its trading deadline."""
    state = TradingState()
    left = _contract("left")
    right = _contract("right", venue_id=VenueID("right"))
    markets = tuple(
        Market(
            id=contract.market_id,
            venue_id=contract.venue_id,
            title="Regular market",
            state=MarketState(MarketStatus.ACTIVE),
            yes_side=MarketSide(
                OutcomeID(f"{contract.market_id}:yes"),
                BinaryOutcome.YES,
            ),
            no_side=MarketSide(
                OutcomeID(f"{contract.market_id}:no"),
                BinaryOutcome.NO,
            ),
        )
        for contract in (left, right)
    )
    candidate = RegularCandidate(markets)
    pair = MatchedContractPair(
        left,
        right,
        Timestamp.now() - timedelta(hours=1),
    )
    state.matches[candidate] = (pair,)
    state.execution_cycles["execution-1"] = candidate
    state.execution_pairs["execution-1"] = pair.key

    error = order_dispatch._market_expiry_guard_error(
        (_command(), _command(role="hedge", client_order_id="client-hedge")),
        state,
        20,
    )

    assert error is None


def test_submission_guard_rejects_a_locally_fresh_stale_source_update() -> None:
    """Do not certify old venue data merely because it just left a local queue."""
    now_ns = time.monotonic_ns()
    now_wall_ns = time.time_ns()
    command = _command()
    book = replace(
        _live_book("0.4", now_ns),
        source_at_ns=now_wall_ns - 501_000_000,
        arrival_wall_at_ns=now_wall_ns,
        arrival_at_ns=now_ns,
        source_timestamp_kind="venue_update",
    )

    error = order_dispatch._book_guard_error(
        command,
        book,
        120_000_000,
        now_ns,
        now_wall_ns,
    )

    assert error is not None
    assert "source age" in error


def test_submission_guard_can_observe_source_age_without_enforcing_it() -> None:
    """Use local monotonic freshness while external clocks are uncalibrated."""
    now_ns = time.monotonic_ns()
    now_wall_ns = time.time_ns()
    book = replace(
        _live_book("0.4", now_ns),
        source_at_ns=now_wall_ns - 500_000_000,
        arrival_wall_at_ns=now_wall_ns,
        arrival_at_ns=now_ns,
        source_timestamp_kind="venue_update",
    )

    error = order_dispatch._book_guard_error(
        _command(),
        book,
        120_000_000,
        now_ns,
        now_wall_ns,
        enforce_source_age=False,
    )

    assert error is None


def test_prepare_failure_rejects_both_legs_without_stopping_dispatcher(tmp_path) -> None:
    """Turn one adapter preparation failure into terminal local pair outcomes."""

    async def run() -> None:
        journal = BinaryJournal(tmp_path / "prepare-failure.log")
        outputs = RingBuffer[SubmitOrder](2)
        inputs = RingBuffer(4)
        errors: list[BaseException] = []
        dispatcher = OutputDispatcher(
            outputs,
            EventSink(inputs),
            journal,
            TradingState(),
            errors.append,
        )
        dispatcher.configure(
            {
                VenueID("left"): _Execution(journal, VenueID("left")),
                VenueID("right"): _PrepareFailureExecution(
                    journal,
                    VenueID("right"),
                ),
            },
        )
        assert outputs.try_publish(
            _command(venue_id=VenueID("left")),
        )
        assert outputs.try_publish(
            _command(
                role="hedge",
                client_order_id="client-hedge",
                venue_id=VenueID("right"),
            ),
        )
        consumer = asyncio.create_task(dispatcher.run())

        await asyncio.wait_for(outputs.join(), timeout=2)
        results = [await inputs.get(), await inputs.get()]

        assert all(isinstance(event, SubmissionReceived) for event in results)
        assert all(
            event.result.status is SubmissionStatus.REJECTED
            for event in results
        )
        assert all(
            event.result.reason == (
                "pre-submission preparation failed: invalid price from venue tick size"
            )
            for event in results
        )
        assert errors == []
        consumer.cancel()
        await asyncio.gather(consumer, return_exceptions=True)
        await dispatcher.close()
        journal.close()

    asyncio.run(run())


def test_recovery_rejects_pair_when_only_one_prepared_order_is_not_found(tmp_path) -> None:
    """Avoid resubmitting an incomplete pair after a dispatcher crash."""

    async def run() -> None:
        journal = BinaryJournal(tmp_path / "recovery-incomplete-pair.log")
        inputs = RingBuffer(4)
        state = TradingState()
        dispatcher = OutputDispatcher(
            RingBuffer(2),
            EventSink(inputs),
            journal,
            state,
        )
        left = _Execution(journal, VenueID("left"))
        right = _Execution(journal, VenueID("right"))
        dispatcher.configure(
            {VenueID("left"): left, VenueID("right"): right},
        )
        primary = _command(venue_id=VenueID("left"))
        hedge = _command(
            role="hedge",
            client_order_id="client-hedge",
            venue_id=VenueID("right"),
        )
        journal.append(primary)
        journal.append(hedge)
        prepared = left.prepare(primary.intent)
        journal.append(OrderPrepared(primary, prepared))

        await RecoveryCoordinator(
            journal.entries(),
            dispatcher,
            {VenueID("left"): left, VenueID("right"): right},
        ).recover()
        results = [await inputs.get(), await inputs.get()]

        assert all(isinstance(event, SubmissionReceived) for event in results)
        assert all(event.result.status is SubmissionStatus.REJECTED for event in results)
        assert left.submitted == []
        assert right.submitted == []
        journal.close()

    asyncio.run(run())


def test_output_dispatcher_submits_two_commands_concurrently(
    tmp_path,
) -> None:
    """Submit both legs of an admitted arbitrage concurrently."""

    async def run() -> None:
        journal = BinaryJournal(tmp_path / "parallel-orders.log")
        outputs = RingBuffer[SubmitOrder](2)
        inputs = RingBuffer(2)
        errors: list[BaseException] = []
        state = TradingState()
        received_at_ns = time.monotonic_ns()
        state.books[ContractID("left-contract")] = _live_book(
            "0.4",
            received_at_ns,
        )
        state.books[ContractID("right-contract")] = _live_book(
            "0.4",
            received_at_ns,
        )
        state.timings["execution-1"] = ExecutionTimings(
            opportunity_at_ns=received_at_ns,
            venue_ids={"primary": "left", "hedge": "right"},
        )
        dispatcher = OutputDispatcher(
            outputs,
            EventSink(inputs),
            journal,
            state,
            errors.append,
            max_book_age_ms=1_000,
        )
        left_venue, right_venue = VenueID("left"), VenueID("right")
        barrier = threading.Barrier(2)
        left = _ParallelExecution(journal, left_venue, barrier)
        right = _ParallelExecution(journal, right_venue, barrier)
        dispatcher.configure({left_venue: left, right_venue: right})
        assert outputs.try_publish(
            _command(contract_id="left-contract", venue_id=left_venue),
        )
        assert outputs.try_publish(
            _command(
                role="hedge",
                client_order_id="client-order-hedge",
                contract_id="right-contract",
                venue_id=right_venue,
            ),
        )
        consumer = asyncio.create_task(dispatcher.run())

        await asyncio.wait_for(outputs.join(), timeout=2)

        assert len(left.submitted) == 1
        assert len(right.submitted) == 1
        assert barrier.broken is False
        assert inputs.size == 2
        assert errors == []
        timings = state.timings["execution-1"]
        assert set(timings.watch_finished_at_ns) == {"primary", "hedge"}
        assert set(timings.adapter_prepared_at_ns) == {"primary", "hedge"}
        assert set(timings.journaled_at_ns) == {"primary", "hedge"}
        for leg in timings.snapshot("execution-1")["legs"]:
            for name in ("prepare_thread", "submit_thread", "journal_thread"):
                assert all(leg[name][field] is not None for field in (
                    "queue_ms", "wall_ms", "cpu_ms", "resume_ms",
                ))
        consumer.cancel()
        await asyncio.gather(consumer, return_exceptions=True)
        await dispatcher.close()
        journal.close()

    asyncio.run(run())


def test_worker_routed_pair_cannot_bypass_stale_parent_book_guard(
    tmp_path,
) -> None:
    """Apply the same local guard to worker and parent feed executions."""

    async def run() -> None:
        journal = BinaryJournal(tmp_path / "worker-guard.log")
        inputs = RingBuffer(2)
        state = TradingState()
        left_venue, right_venue = VenueID("left"), VenueID("right")
        left_contract = _contract("left-contract", venue_id=left_venue)
        right_contract = _contract("right-contract", venue_id=right_venue)
        cycle = MarketCycle(Underlying("BTC"), 300)
        pair = MatchedContractPair(
            left_contract,
            right_contract,
            Timestamp.now() + timedelta(minutes=5),
        )
        state.matches[cycle] = (pair,)
        state.execution_cycles["execution-1"] = cycle
        state.execution_pairs["execution-1"] = pair.key
        state.execution_worker_refs["execution-1"] = OpportunityValidationRef(
            "generation:1",
            "btc-5m",
            "generation",
            1,
            "left-generation",
            "right-generation",
        )
        now_ns = time.monotonic_ns()
        stale_ns = now_ns - 121 * 1_000_000
        state.books[left_contract.id] = _live_book("0.4", stale_ns)
        state.books[right_contract.id] = _live_book("0.4", stale_ns)
        dispatcher = OutputDispatcher(RingBuffer(2), EventSink(inputs), journal, state)
        left = _Execution(journal, left_venue)
        right = _Execution(journal, right_venue)

        dispatcher.configure({left_venue: left, right_venue: right})
        commands = (
            _command(contract_id="left-contract", venue_id=left_venue),
            _command(
                role="hedge",
                client_order_id="client-hedge",
                contract_id="right-contract",
                venue_id=right_venue,
            ),
        )

        await dispatcher._dispatch_pair(commands)
        await inputs.get()
        await inputs.get()

        assert left.submitted == []
        assert right.submitted == []
        journal.close()

    asyncio.run(run())


def test_output_dispatcher_rejects_queued_pair_after_safety_stop(tmp_path) -> None:
    """Do not submit live commands that remained queued during a venue halt."""

    async def run() -> None:
        journal = BinaryJournal(tmp_path / "safety-stop.log")
        outputs = RingBuffer[SubmitOrder](2)
        inputs = RingBuffer(2)
        state = TradingState()
        state.safety_halted = True
        state.last_error = "Trading halted by venue safety circuit"
        left_venue, right_venue = VenueID("left"), VenueID("right")
        left = _Execution(journal, left_venue)
        right = _Execution(journal, right_venue)
        dispatcher = OutputDispatcher(outputs, EventSink(inputs), journal, state)
        dispatcher.configure({left_venue: left, right_venue: right})
        assert outputs.try_publish(_command(venue_id=left_venue))
        assert outputs.try_publish(
            _command(
                role="hedge",
                client_order_id="client-hedge",
                venue_id=right_venue,
            ),
        )
        consumer = asyncio.create_task(dispatcher.run())

        await asyncio.wait_for(outputs.join(), timeout=1)

        assert left.submitted == []
        assert right.submitted == []
        results = [await inputs.get(), await inputs.get()]
        assert all(
            event.result.status is SubmissionStatus.REJECTED
            for event in results
        )
        consumer.cancel()
        await asyncio.gather(consumer, return_exceptions=True)
        await dispatcher.close()
        journal.close()

    asyncio.run(run())


def test_output_dispatcher_prepares_independent_pairs_concurrently(tmp_path) -> None:
    """Overlap preparation across pairs and submit both legs of each pair."""

    async def run() -> None:
        journal = BinaryJournal(tmp_path / "concurrent-pairs.log")
        outputs = RingBuffer[SubmitOrder](4)
        inputs = RingBuffer(4)
        errors: list[BaseException] = []
        state = TradingState()
        now_ns = time.monotonic_ns()
        left_venue, right_venue = VenueID("left"), VenueID("right")
        barrier = threading.Barrier(4)
        left = _ConcurrentPrepareExecution(journal, left_venue, barrier)
        right = _ConcurrentPrepareExecution(journal, right_venue, barrier)
        dispatcher = OutputDispatcher(
            outputs,
            EventSink(inputs),
            journal,
            state,
            errors.append,
        )
        dispatcher.configure({left_venue: left, right_venue: right})
        for execution_id in ("execution-1", "execution-2"):
            left_contract = f"{execution_id}-left"
            right_contract = f"{execution_id}-right"
            state.books[ContractID(left_contract)] = _live_book("0.4", now_ns)
            state.books[ContractID(right_contract)] = _live_book("0.4", now_ns)
            assert outputs.try_publish(
                _command(
                    execution_id=execution_id,
                    client_order_id=f"{execution_id}-primary",
                    contract_id=left_contract,
                    venue_id=left_venue,
                ),
            )
            assert outputs.try_publish(
                _command(
                    execution_id=execution_id,
                    role="hedge",
                    client_order_id=f"{execution_id}-hedge",
                    contract_id=right_contract,
                    venue_id=right_venue,
                ),
            )
        consumer = asyncio.create_task(dispatcher.run())

        await asyncio.wait_for(outputs.join(), timeout=3)

        assert barrier.broken is False
        assert len(left.submitted) == 2
        assert len(right.submitted) == 2
        assert inputs.size == 4
        assert errors == []
        consumer.cancel()
        await asyncio.gather(consumer, return_exceptions=True)
        await dispatcher.close()
        journal.close()

    asyncio.run(run())


def test_output_dispatcher_refreshes_recovery_book_before_preparing(
    tmp_path,
) -> None:
    """Requote instead of preparing against stale or insufficient depth."""

    async def run() -> None:
        journal = BinaryJournal(tmp_path / "single-recovery.log")
        outputs = RingBuffer[SubmitOrder](1)
        inputs = RingBuffer(2)
        state = TradingState()
        state.books[ContractID("contract")] = _live_book(
            "0.4",
            time.monotonic_ns() - 1_000_000_000,
        )
        adapter = _Execution(journal)
        market_data = _MarketData(
            _live_book("0.4", time.monotonic_ns(), quantity="1"),
        )
        dispatcher = OutputDispatcher(outputs, EventSink(inputs), journal, state)
        dispatcher.configure(
            {VenueID("venue"): adapter},
            market_data={VenueID("venue"): market_data},
        )
        assert outputs.try_publish(_command(role="recovery"))
        consumer = asyncio.create_task(dispatcher.run())

        await asyncio.wait_for(outputs.join(), timeout=1)

        assert market_data.requested == [ContractID("contract")]
        assert adapter.submitted == []
        refreshed = await inputs.get()
        rejected = await inputs.get()
        assert isinstance(refreshed, OrderBookUpdated)
        assert isinstance(rejected, SubmissionReceived)
        assert "only 1 executable shares" in str(rejected.result.reason)
        consumer.cancel()
        await asyncio.gather(consumer, return_exceptions=True)
        await dispatcher.close()
        journal.close()

    asyncio.run(run())


def test_output_dispatcher_retries_delayed_polymarket_recovery_balance(
    tmp_path,
    monkeypatch,
) -> None:
    """Reuse one persisted request until freshly bought tokens become sellable."""

    async def run() -> None:
        journal = BinaryJournal(tmp_path / "retry-recovery.log")
        outputs = RingBuffer[SubmitOrder](1)
        inputs = RingBuffer(1)
        state = TradingState()
        state.books[ContractID("contract")] = _live_book(
            "0.4",
            time.monotonic_ns(),
        )
        venue_id = VenueID("POLYMARKET")
        adapter = _BalanceDelayedExecution(journal, venue_id)
        dispatcher = OutputDispatcher(outputs, EventSink(inputs), journal, state)
        dispatcher.configure({venue_id: adapter})
        monkeypatch.setattr(order_dispatch, "_RECOVERY_SUBMISSION_RETRY_SECONDS", 0)
        assert outputs.try_publish(_command(role="recovery", venue_id=venue_id))
        consumer = asyncio.create_task(dispatcher.run())

        await asyncio.wait_for(outputs.join(), timeout=1)

        assert len(adapter.submitted) == 3
        assert adapter.submitted[0] is adapter.submitted[1]
        assert adapter.submitted[1] is adapter.submitted[2]
        event = await inputs.get()
        assert isinstance(event, SubmissionReceived)
        assert event.result.status is SubmissionStatus.ACCEPTED
        assert inputs.size == 0
        consumer.cancel()
        await asyncio.gather(consumer, return_exceptions=True)
        await dispatcher.close()
        journal.close()

    asyncio.run(run())


def test_output_dispatcher_does_not_treat_ioc_cancel_as_fill_protection(
    tmp_path,
) -> None:
    """Keep reconciling an accepted IOC when its paired venue rejects."""

    async def run() -> None:
        journal = BinaryJournal(tmp_path / "cancel-peer.log")
        outputs = RingBuffer[SubmitOrder](2)
        inputs = RingBuffer(4)
        state = TradingState()
        received_at_ns = time.monotonic_ns()
        state.books[ContractID("left-contract")] = _live_book("0.4", received_at_ns)
        state.books[ContractID("right-contract")] = _live_book("0.4", received_at_ns)
        left_venue, right_venue = VenueID("left"), VenueID("right")
        accepted = _CancellableExecution(journal, left_venue)
        rejected = _Execution(journal, right_venue)
        dispatcher = OutputDispatcher(outputs, EventSink(inputs), journal, state)
        dispatcher.configure({left_venue: accepted, right_venue: rejected})
        assert outputs.try_publish(
            _command(contract_id="left-contract", venue_id=left_venue),
        )
        assert outputs.try_publish(
            _command(
                role="hedge",
                client_order_id="client-order-hedge",
                contract_id="right-contract",
                venue_id=right_venue,
            ),
        )
        consumer = asyncio.create_task(dispatcher.run())

        await asyncio.wait_for(outputs.join(), timeout=1)
        events = [await inputs.get() for _ in range(2)]

        assert accepted.cancelled == []
        assert all(isinstance(event, SubmissionReceived) for event in events)
        assert len(rejected.submitted) == 1
        consumer.cancel()
        await asyncio.gather(consumer, return_exceptions=True)
        await dispatcher.close()
        journal.close()

    asyncio.run(run())


def test_adapter_timed_ioc_is_treated_as_open_until_cancellation() -> None:
    """Honor adapter lifecycle capabilities without inspecting venue identity."""
    adapter = _TimedCancellationExecution(None, VenueID("FUTURE_VENUE"))
    command = _command(venue_id=adapter.venue_id)
    snapshot = OrderSnapshot(
        status=OrderStatus.ACCEPTED,
        contract_id=command.intent.contract_id,
        side=command.intent.side,
        quantity=command.intent.quantity,
        order_type=command.intent.order_type,
        client_order_id=command.intent.client_order_id,
        limit_price=command.intent.limit_price,
        updated_at=Timestamp.now(),
    )
    result = SubmissionResult(
        SubmissionStatus.ACCEPTED,
        OrderReference(
            command.venue_id,
            command.intent.client_order_id,
            b"predict-order",
        ),
        snapshot,
    )

    assert order_dispatch._submission_may_be_open(command, adapter, result)


def test_output_dispatcher_cancels_adapter_timed_order(tmp_path) -> None:
    """Cancel a nonterminal adapter-timed order and publish its final snapshot."""

    async def run() -> None:
        journal = BinaryJournal(tmp_path / "predict-resting-window.log")
        inputs = RingBuffer(4)
        venue_id = VenueID("FUTURE_VENUE")
        adapter = _TimedCancellationExecution(journal, venue_id)
        dispatcher = OutputDispatcher(
            RingBuffer(1),
            EventSink(inputs),
            journal,
            TradingState(),
        )
        dispatcher.configure({venue_id: adapter})

        await dispatcher.execute(_command(venue_id=venue_id))
        assert isinstance(await inputs.get(), SubmissionReceived)
        terminal = await asyncio.wait_for(inputs.get(), timeout=1)

        assert isinstance(terminal, OrderSnapshotUpdated)
        assert terminal.snapshot.status is OrderStatus.CANCELLED
        assert adapter.cancelled == [terminal.reference]
        await dispatcher.close()
        journal.close()

    asyncio.run(run())


def test_output_dispatcher_halts_if_timed_cancel_is_uncertain(tmp_path) -> None:
    """Keep retrying any uncertain timed cancellation and stop new trading."""

    async def run() -> None:
        journal = BinaryJournal(tmp_path / "predict-uncertain-cancel.log")
        inputs = RingBuffer(4)
        venue_id = VenueID("FUTURE_VENUE")
        adapter = _UnknownCancelExecution(journal, venue_id)
        dispatcher = OutputDispatcher(
            RingBuffer(1),
            EventSink(inputs),
            journal,
            TradingState(),
        )
        dispatcher.configure({venue_id: adapter})

        await dispatcher.execute(_command(venue_id=venue_id))
        assert isinstance(await inputs.get(), SubmissionReceived)
        stopped = await asyncio.wait_for(inputs.get(), timeout=1)

        assert isinstance(stopped, TradingSafetyStop)
        assert "could not prove the order terminal" in stopped.reason
        assert stopped.execution_id == _command(venue_id=venue_id).execution_id
        assert stopped.client_order_id == _command(venue_id=venue_id).intent.client_order_id
        assert adapter.cancelled
        await dispatcher.close()
        journal.close()

    asyncio.run(run())


@pytest.mark.parametrize("halt_first", (False, True))
def test_predict_monitor_keeps_late_private_fill_after_cancel_and_rest_failure(tmp_path, monkeypatch, halt_first):
    """Keep the watcher alive after removal and account a late fill exactly once."""
    from prediction_markets.infrastructure.venues.predict.order_updates import PredictOrderUpdateAdapter
    from prediction_markets.domain.shared.value_objects import OrderID
    from unittest.mock import AsyncMock

    class DelayedSettlement(_TimedCancellationExecution):
        """Report removal before settlement and fail every subsequent REST read."""

        def _snapshot(self, reference, status):
            return replace(super()._snapshot(reference, status),
                order_id=OrderID("0xabc"), may_receive_more_fills=True)

        def reconcile(self, reference):
            return ReconciliationResult(ReconciliationStatus.UNKNOWN, reference)

    async def run():
        journal = BinaryJournal(tmp_path / "delayed-settlement.log")
        inputs = RingBuffer(16)
        venue = VenueID("PREDICT")
        adapter = DelayedSettlement(journal, venue)
        updates = PredictOrderUpdateAdapter(lambda: "jwt", api_key="test")
        updates.watch = AsyncMock()
        errors = []
        dispatcher = OutputDispatcher(RingBuffer(1), EventSink(inputs), journal,
            TradingState(), on_error=errors.append)
        dispatcher.configure({venue: adapter}, {venue: updates})
        command = _command(venue_id=venue)
        try:
            await dispatcher.execute(command)
            assert isinstance(await inputs.get(), SubmissionReceived)
            cancelled = await asyncio.wait_for(inputs.get(), 1)
            assert isinstance(cancelled, OrderSnapshotUpdated)
            assert cancelled.snapshot.status is OrderStatus.CANCELLED
            assert cancelled.snapshot.may_receive_more_fills
            assert dispatcher._watchers
            if halt_first:
                monkeypatch.setattr(order_dispatch, "_SETTLEMENT_CONFIRMATION_SECONDS", 0)
                stopped = await asyncio.wait_for(inputs.get(), 1)
                assert isinstance(stopped, TradingSafetyStop)
                assert "settlement remains uncertain" in stopped.reason
                assert stopped.execution_id == command.execution_id
                assert stopped.client_order_id == command.intent.client_order_id
            payload = {"type": "orderTransactionSuccess", "orderHash": "0xabc",
                "timestamp": int(time.time() * 1000), "settlementId": "late",
                "fill": {"executedSizeWei": str(int(command.intent.quantity.value * 10**18)),
                    "executedPriceWei": "180000000000000000"}}
            updates._handle(payload)
            updates._handle(payload)
            filled = await asyncio.wait_for(inputs.get(), 1)
            assert isinstance(filled, OrderSnapshotUpdated)
            assert filled.snapshot.status is OrderStatus.FILLED
            assert filled.snapshot.filled_quantity == command.intent.quantity
            assert len(adapter.submitted) == 1
            assert len(adapter.cancelled) == 1
            assert not errors
        finally:
            await dispatcher.close()
            journal.close()

    asyncio.run(run())


def test_predict_unknown_untimed_order_halt_identifies_its_execution(tmp_path, monkeypatch):
    """Scope uncertainty without a resting timer to its own command and watcher."""
    async def run():
        journal = BinaryJournal(tmp_path / "unknown-predict-outcome.log")
        inputs = RingBuffer(8)
        venue = VenueID("PREDICT")
        command = _command(venue_id=venue)
        adapter = _CancellableExecution(journal, venue)
        adapter.reconciliation = ReconciliationStatus.UNKNOWN
        dispatcher = OutputDispatcher(RingBuffer(1), EventSink(inputs), journal, TradingState())
        dispatcher.configure({venue: adapter})
        monkeypatch.setattr(order_dispatch, "_SETTLEMENT_CONFIRMATION_SECONDS", 0)
        try:
            await dispatcher.execute(command)
            assert isinstance(await inputs.get(), SubmissionReceived)
            stopped = await asyncio.wait_for(inputs.get(), timeout=1)
            assert isinstance(stopped, TradingSafetyStop)
            assert "outcome remains uncertain" in stopped.reason
            assert stopped.execution_id == command.execution_id
            assert stopped.client_order_id == command.intent.client_order_id
            assert dispatcher._watchers
            assert len(adapter.submitted) == 1
        finally:
            await dispatcher.close()
            journal.close()

    asyncio.run(run())


def test_output_dispatcher_cancels_timed_order_during_shutdown(tmp_path) -> None:
    """Remove an application-timed order when its monitor shuts down early."""

    async def run() -> None:
        journal = BinaryJournal(tmp_path / "predict-shutdown-cancel.log")
        inputs = RingBuffer(2)
        venue_id = VenueID("FUTURE_VENUE")
        adapter = _TimedCancellationExecution(journal, venue_id)
        dispatcher = OutputDispatcher(
            RingBuffer(1),
            EventSink(inputs),
            journal,
            TradingState(),
        )
        dispatcher.configure({venue_id: adapter})

        await dispatcher.execute(_command(venue_id=venue_id))
        assert isinstance(await inputs.get(), SubmissionReceived)
        await asyncio.sleep(0)
        await dispatcher.close()

        assert adapter.cancelled
        journal.close()

    asyncio.run(run())


def test_output_dispatcher_guards_sell_immediately_after_prepare(
    tmp_path,
) -> None:
    """Do not yield to the removed funds hook between SELL prepare and guard."""

    async def run() -> None:
        journal = BinaryJournal(tmp_path / "sell-prepare-guard.log")
        outputs = RingBuffer[SubmitOrder](2)
        inputs = RingBuffer(2)
        state = TradingState()
        now_ns = time.monotonic_ns()
        state.books[ContractID("left-contract")] = _live_book(
            "0.4",
            now_ns,
            bid=True,
        )
        state.books[ContractID("right-contract")] = _live_book(
            "0.4",
            now_ns,
            bid=True,
        )
        state.timings["execution-1"] = ExecutionTimings(
            opportunity_at_ns=now_ns,
            venue_ids={"primary": "POLYMARKET", "hedge": "PREDICT"},
            book_received_at_ns={"primary": now_ns, "hedge": now_ns},
        )
        left_venue, right_venue = VenueID("POLYMARKET"), VenueID("PREDICT")
        guarded = _LegacyFundsGuardExecution(journal, left_venue)
        unchecked = _Execution(journal, right_venue)
        dispatcher = OutputDispatcher(outputs, EventSink(inputs), journal, state)
        dispatcher.configure({left_venue: guarded, right_venue: unchecked})
        assert outputs.try_publish(
            _command(
                contract_id="left-contract",
                venue_id=left_venue,
                side=OrderSide.SELL,
            ),
        )
        assert outputs.try_publish(
            _command(
                role="hedge",
                client_order_id="client-order-hedge",
                contract_id="right-contract",
                venue_id=right_venue,
                side=OrderSide.SELL,
            ),
        )
        consumer = asyncio.create_task(dispatcher.run())

        await asyncio.wait_for(outputs.join(), timeout=1)

        results = [await inputs.get(), await inputs.get()]
        assert all(isinstance(event, SubmissionReceived) for event in results)
        assert guarded.preflight_calls == 0
        assert len(guarded.submitted) == 1
        assert len(unchecked.submitted) == 1
        prepare_to_guard_ms = state.timings["execution-1"].snapshot(
            "execution-1",
        )["stages"]["prepare_to_guard_ms"]
        assert prepare_to_guard_ms is not None
        assert prepare_to_guard_ms < 50
        consumer.cancel()
        await asyncio.gather(consumer, return_exceptions=True)
        await dispatcher.close()
        journal.close()

    asyncio.run(run())


def test_prepared_order_journal_append_runs_off_event_loop() -> None:
    """Keep synchronous prepared-order encoding and I/O off the event loop."""

    async def run() -> None:
        journal = _ThreadRecordingJournal()
        dispatcher = OutputDispatcher(
            RingBuffer(1),
            EventSink(RingBuffer(1)),
            journal,
            TradingState(),
        )
        dispatcher.configure({VenueID("venue"): _Execution(journal)})
        loop_thread_id = threading.get_ident()

        await dispatcher._prepare(_command())

        assert journal.append_thread_ids
        assert all(
            thread_id != loop_thread_id
            for thread_id in journal.append_thread_ids
        )

    asyncio.run(run())


@pytest.mark.parametrize(
    ("hedge_ask", "hedge_age_ms", "expected_reason"),
    (
        ("0.42", 0, "best ask 0.42 outside buy limit 0.4"),
        ("0.4", 501, "book age"),
    ),
)
def test_output_dispatcher_rejects_pair_if_one_book_fails_guard(
    tmp_path,
    hedge_ask: str,
    hedge_age_ms: int,
    expected_reason: str,
) -> None:
    """Submit neither leg when either current book is stale or unmarketable."""

    async def run() -> None:
        journal = BinaryJournal(tmp_path / f"guard-{hedge_age_ms}-{hedge_ask}.log")
        outputs = RingBuffer[SubmitOrder](2)
        inputs = RingBuffer(2)
        state = TradingState()
        now_ns = time.monotonic_ns()
        state.books[ContractID("left-contract")] = _live_book("0.4", now_ns)
        state.books[ContractID("right-contract")] = _live_book(
            hedge_ask,
            now_ns - hedge_age_ms * 1_000_000,
        )
        state.timings["execution-1"] = ExecutionTimings(
            opportunity_at_ns=now_ns,
            venue_ids={"primary": "left", "hedge": "right"},
            book_received_at_ns={
                "primary": now_ns,
                "hedge": now_ns - hedge_age_ms * 1_000_000,
            },
        )
        left_venue, right_venue = VenueID("left"), VenueID("right")
        left = _Execution(journal, left_venue)
        right = _Execution(journal, right_venue)
        dispatcher = OutputDispatcher(outputs, EventSink(inputs), journal, state)
        dispatcher.configure({left_venue: left, right_venue: right})
        assert outputs.try_publish(
            _command(contract_id="left-contract", venue_id=left_venue),
        )
        assert outputs.try_publish(
            _command(
                role="hedge",
                client_order_id="client-order-hedge",
                contract_id="right-contract",
                venue_id=right_venue,
            ),
        )
        consumer = asyncio.create_task(dispatcher.run())

        await asyncio.wait_for(outputs.join(), timeout=1)

        results = [await inputs.get(), await inputs.get()]
        assert left.submitted == right.submitted == []
        assert all(isinstance(result, SubmissionReceived) for result in results)
        assert all(
            result.result.status is SubmissionStatus.REJECTED for result in results
        )
        assert all(expected_reason in result.result.reason for result in results)
        trace = state.timings["execution-1"].snapshot("execution-1")
        assert isinstance(trace, dict)
        assert trace["outcome"] == "guard_rejected"
        assert expected_reason in str(trace["error"])
        for leg in trace["legs"]:
            assert leg["prepare_thread"]["wall_ms"] is not None
            assert leg["journal_thread"]["wall_ms"] is not None
            assert "submit_thread" not in leg
        consumer.cancel()
        await asyncio.gather(consumer, return_exceptions=True)
        await dispatcher.close()
        journal.close()

    asyncio.run(run())


def test_output_saturation_fails_before_journaling_command(tmp_path) -> None:
    """Keep an unpublishable order command out of the journal."""

    async def run() -> None:
        journal = BinaryJournal(tmp_path / "saturated.log")
        outputs = RingBuffer[SubmitOrder](1)
        assert outputs.try_publish(_command())
        event_loop = EventLoop(RingBuffer(1), outputs, journal, object())

        try:
            await event_loop.process(_command())
        except RingBufferFull:
            pass
        else:
            raise AssertionError("A saturated output ring must fail before journaling")

        assert journal.entries() == ()
        journal.close()

    asyncio.run(run())


def test_order_book_updates_are_processed_without_growing_the_journal(tmp_path) -> None:
    """Keep disposable public market data out of durable recovery state."""

    async def run() -> None:
        journal = BinaryJournal(tmp_path / "market-data.log")
        engine = Mock()
        engine.process.return_value = ()
        event = OrderBookUpdated(
            VenueID("venue"),
            ContractID("book-contract"),
            OrderBook(MarketID("market"), OutcomeID("yes"), (), ()),
        )
        event_loop = EventLoop(RingBuffer(1), RingBuffer(1), journal, engine)

        await event_loop.process(event)

        engine.process.assert_called_once_with(event)
        assert journal.entries() == ()
        journal.close()

    asyncio.run(run())


def test_prepared_request_is_journaled_before_submit_and_reused_on_recovery(
    tmp_path,
) -> None:
    """Never rebuild a persisted venue request after a restart."""

    async def run() -> None:
        journal = BinaryJournal(tmp_path / "orders.log")
        inputs = RingBuffer(8)
        dispatcher = OutputDispatcher(
            RingBuffer(8), EventSink(inputs), journal, TradingState(),
        )
        adapter = _Execution(journal)
        venue = VenueID("venue")
        dispatcher.configure({venue: adapter})
        command = _command()

        journal.append(command)
        await dispatcher.execute(command)
        received = await inputs.get()
        assert isinstance(received, SubmissionReceived)
        prepared = journal.entries()[1].event.prepared
        assert adapter.submitted == [prepared]

        recovered = OutputDispatcher(
            RingBuffer(8), EventSink(RingBuffer(8)), journal, TradingState(),
        )
        recovered.configure({venue: adapter})
        await RecoveryCoordinator(journal.entries(), recovered, {venue: adapter}).recover()

        assert adapter.submitted == [prepared, prepared]
        journal.close()

    asyncio.run(run())


def test_recovery_keeps_open_timed_partial_fill_under_control(tmp_path) -> None:
    """Cancel a recovered timed partial instead of abandoning its remainder."""

    async def run() -> None:
        journal = BinaryJournal(tmp_path / "predict-partial-recovery.log")
        inputs = RingBuffer(4)
        venue_id = VenueID("FUTURE_VENUE")
        command = _command(venue_id=venue_id)
        adapter = _RecoveredTimedExecution(journal, venue_id)
        prepared = adapter.prepare(command.intent)
        partial = replace(
            adapter._snapshot(prepared.reference, OrderStatus.PARTIALLY_FILLED),
            filled_quantity=Quantity(Decimal("1")),
            average_price=Price(Decimal("0.4")),
            may_receive_more_fills=True,
        )
        adapter.recovered_snapshot = partial
        journal.append(command)
        journal.append(OrderPrepared(command, prepared))
        journal.append(
            OrderSnapshotUpdated(
                command.execution_id,
                command.role,
                prepared.reference,
                partial,
                "get",
            ),
        )
        dispatcher = OutputDispatcher(
            RingBuffer(1),
            EventSink(inputs),
            journal,
            TradingState(),
        )
        dispatcher.configure({venue_id: adapter})

        await RecoveryCoordinator(
            journal.entries(),
            dispatcher,
            {venue_id: adapter},
        ).recover()

        assert isinstance(await asyncio.wait_for(inputs.get(), timeout=1), SubmissionReceived)
        cancelled = await asyncio.wait_for(inputs.get(), timeout=1)
        assert isinstance(cancelled, OrderSnapshotUpdated)
        assert cancelled.snapshot.status is OrderStatus.CANCELLED
        assert adapter.submitted == []
        assert adapter.cancelled == [prepared.reference]
        await dispatcher.close()
        journal.close()

    asyncio.run(run())


def test_recovery_fails_closed_when_observed_order_disappears(tmp_path) -> None:
    """Never resubmit an order already observed before a restart."""

    async def run() -> None:
        journal = BinaryJournal(tmp_path / "missing-observed-order.log")
        venue_id = VenueID("FUTURE_VENUE")
        command = _command(venue_id=venue_id)
        adapter = _Execution(journal, venue_id)
        prepared = adapter.prepare(command.intent)
        journal.append(command)
        journal.append(OrderPrepared(command, prepared))
        journal.append(
            SubmissionReceived(
                command,
                SubmissionResult(SubmissionStatus.ACCEPTED, prepared.reference),
            ),
        )
        dispatcher = OutputDispatcher(
            RingBuffer(1),
            EventSink(RingBuffer(1)),
            journal,
            TradingState(),
        )
        dispatcher.configure({venue_id: adapter})

        with pytest.raises(RecoveryError, match="lost a previously observed order"):
            await RecoveryCoordinator(
                journal.entries(),
                dispatcher,
                {venue_id: adapter},
            ).recover()

        assert adapter.submitted == []
        journal.close()

    asyncio.run(run())


def test_terminal_fill_is_reconciled_once_more_for_venue_fee(tmp_path) -> None:
    """Publish a fee-enriched snapshot after a terminal WebSocket-style fill."""

    async def run() -> None:
        journal = BinaryJournal(tmp_path / "terminal-fee.log")
        inputs = RingBuffer(4)
        dispatcher = OutputDispatcher(
            RingBuffer(2),
            EventSink(inputs),
            journal,
            TradingState(),
        )
        adapter = _FeeExecution(journal)
        dispatcher.configure({VenueID("venue"): adapter})

        await dispatcher.execute(_command())
        initial = await inputs.get()
        enriched = await asyncio.wait_for(inputs.get(), timeout=1)

        assert isinstance(initial, SubmissionReceived)
        assert initial.result.snapshot is not None
        assert initial.result.snapshot.fee is None
        assert isinstance(enriched, OrderSnapshotUpdated)
        assert enriched.snapshot.fee is not None
        assert enriched.snapshot.fee.charged.amount == Decimal("0.05926")

        await dispatcher.close()
        journal.close()

    asyncio.run(run())


def test_terminal_reconciliation_publishes_actual_quantity_before_fees(tmp_path) -> None:
    """Do not hide additional confirmed shares while waiting for taker fees."""
    class DelayedFeeExecution(_FeeExecution):
        """Expose excess shares before the trade fee becomes available."""
        calls = 0

        def reconcile(self, reference):
            self.calls += 1
            result = super().reconcile(reference)
            return replace(result, snapshot=replace(
                result.snapshot,
                filled_quantity=Quantity(Decimal("2.2")),
                fee=result.snapshot.fee if self.calls > 1 else None,
            ))

    async def run() -> None:
        journal = BinaryJournal(tmp_path / "terminal-quantity.log")
        inputs = RingBuffer(8)
        dispatcher = OutputDispatcher(RingBuffer(2), EventSink(inputs), journal, TradingState())
        adapter = DelayedFeeExecution(journal)
        dispatcher.configure({VenueID("venue"): adapter})
        try:
            await dispatcher.execute(_command())
            assert isinstance(await inputs.get(), SubmissionReceived)
            actual = await asyncio.wait_for(inputs.get(), timeout=1)
            assert isinstance(actual, OrderSnapshotUpdated)
            assert actual.snapshot.filled_quantity == Quantity(Decimal("2.2"))
            assert actual.snapshot.fee is None
            enriched = await asyncio.wait_for(inputs.get(), timeout=1)
            assert enriched.snapshot.filled_quantity == Quantity(Decimal("2.2"))
            assert enriched.snapshot.fee is not None
            assert adapter.calls == 2
        finally:
            await dispatcher.close()
            journal.close()

    asyncio.run(run())
