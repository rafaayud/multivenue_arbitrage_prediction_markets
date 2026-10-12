"""Verify central preparation, repricing, and signed execution payloads."""

import asyncio
import time
from dataclasses import replace
from decimal import Decimal
from prediction_markets.application.events import (
    ArbitrageOpportunityFound,
    MarketMatchesUpdated,
)
from prediction_markets.application.markets.models import MarketCycle
from prediction_markets.application.state import TradingState
from prediction_markets.domain.arbitrage.value_objects import ArbitrageOpportunity
from prediction_markets.domain.contracts.entities import BinaryContract
from prediction_markets.domain.contracts.value_objects import LotSize, TickSize
from prediction_markets.domain.market_matching.value_objects import (
    MatchedContractPair,
    Underlying,
)
from prediction_markets.domain.orderbook.entities import OrderBook
from prediction_markets.domain.orderbook.value_objects import OrderBookLevel
from prediction_markets.domain.shared.value_objects import (
    ContractID,
    Currency,
    MarketID,
    OutcomeID,
    Price,
    Quantity,
    Timestamp,
    VenueID,
)
from prediction_markets.domain.trading.enums import OrderSide
from prediction_markets.infrastructure.venues.limitless.mappers import (
    LIMITLESS_VENUE_ID,
)
from prediction_markets.infrastructure.venues.polymarket.mappers import (
    POLYMARKET_VENUE_ID,
)
from prediction_markets.application.engine import EngineConfig, TradingEngine
from prediction_markets.application.events import (
    ExecutionPreparationRequested,
    OrderBookUpdated,
    PreparedExecutionBatch,
)
from prediction_markets.application.pipeline import EventSink, OutputDispatcher, RingBuffer
from prediction_markets.application.state import (
    EventDispatcher,
)
from prediction_markets.domain.trading.value_objects import PreparedOrder
from prediction_markets.infrastructure.binary_journal import BinaryJournal
from tests.application.test_pipeline import _Execution, _ZeroFees

def _contract(name: str, venue: VenueID, outcome: str) -> BinaryContract:
    return BinaryContract(
        id=ContractID(name),
        market_id=MarketID(f"{name}-market"),
        outcome_id=OutcomeID(outcome),
        venue_id=venue,
        payout_currency=Currency("USD"),
        tick_size=TickSize(Decimal("0.01")),
        lot_size=LotSize(Decimal("1")),
    )


def _book(
    contract: BinaryContract,
    price: str,
    *,
    bid: bool = False,
    source_kind: str = "snapshot_state",
    source_age_ms: int = 0,
) -> OrderBook:
    now_ns = time.monotonic_ns()
    now_wall_ns = time.time_ns()
    level = OrderBookLevel(Price(Decimal(price)), Quantity(Decimal("10")))
    return OrderBook(
        contract.market_id,
        contract.outcome_id,
        (level,) if bid else (),
        () if bid else (level,),
        timestamp=Timestamp.now(),
        source_at_ns=now_wall_ns - source_age_ms * 1_000_000,
        arrival_wall_at_ns=now_wall_ns,
        arrival_at_ns=now_ns,
        source_timestamp_kind=source_kind,
        received_at_ns=now_ns,
    )


def _pair() -> tuple[MarketCycle, MatchedContractPair, OrderBook, OrderBook]:
    cycle = MarketCycle(Underlying("BTC"), 300)
    left = _contract("left", POLYMARKET_VENUE_ID, "yes")
    right = _contract("right", LIMITLESS_VENUE_ID, "no")
    pair = MatchedContractPair(
        left,
        right,
        Timestamp.from_iso("2099-01-01T00:00:00Z"),
    )
    return cycle, pair, _book(left, "0.40"), _book(right, "0.50")


def _detected(
    cycle: MarketCycle,
    pair: MatchedContractPair,
    left: OrderBook,
    right: OrderBook,
) -> ArbitrageOpportunityFound:
    return ArbitrageOpportunityFound(
        "opportunity",
        cycle,
        pair,
        ArbitrageOpportunity(
            pair.left.id,
            pair.right.id,
            OrderSide.BUY,
            left.best_ask(),
            right.best_ask(),
            Quantity(Decimal("5")),
            Decimal("0.1"),
            Decimal("0.1"),
            0,
            Timestamp.now(),
        ),
    )

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
    """Fail if local validation attempts a REST snapshot request."""

    async def get_order_book(self, _contract):
        """Reject any snapshot fetch during local validation."""
        raise AssertionError("Local validation must use in-memory books")


def _staged(tmp_path, *, edge_budget=False):
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
    detected = _detected(cycle, pair, left, right)
    planned, primary, hedge = engine.stage_opportunity(detected)
    request = ExecutionPreparationRequested(detected, planned, (primary, hedge),
                                           time.monotonic_ns() + 1_000_000_000, time.time_ns() + 1_000_000_000)
    journal = BinaryJournal(tmp_path / "dispatch.log")
    inputs = RingBuffer(16)
    dispatcher = OutputDispatcher(RingBuffer(2), EventSink(inputs), journal, state,
        commit_prepared_execution=engine.commit_prepared_execution,
        abort_staged_execution=engine.abort_staged_execution,
        reprice_staged_execution=engine.reprice_staged_execution)
    adapters = {contract.venue_id: _SignedExecution(journal, contract.venue_id) for contract in (pair.left, pair.right)}
    dispatcher.configure(adapters, market_data={venue: _ForbiddenSnapshots() for venue in adapters})
    return engine, request, journal, inputs, dispatcher, adapters


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


def test_local_polymarket_buy_improvement_reprices_before_signing(tmp_path):
    """Replan a parent-owned Polymarket BUY before creating signed payloads."""
    async def run():
        engine, staged, journal, _, dispatcher, adapters = _staged(
            tmp_path,
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
