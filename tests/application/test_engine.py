"""Verify deterministic planning and two-leg execution state transitions."""

from collections import deque
from dataclasses import replace
from datetime import timedelta
from decimal import Decimal
import json
import time

import pytest

from prediction_markets.application.codec import decode_event, encode_event
from prediction_markets.application.engine import EngineConfig, TradingEngine
from prediction_markets.application.execution.accounting import is_settled_order
from prediction_markets.application.events import (
    AccountingCorrectionRecorded,
    ApplicationEvent,
    ArbitrageOpportunityFound,
    ArbitragePlanned,
    ExecutionUpdated,
    MarketMatchesUpdated,
    OpportunityValidationRef,
    OrderBookPairUpdated,
    OrderBookUpdated,
    OrderSnapshotUpdated,
    PositionUpdated,
    RecoveryPlanned,
    SubmissionReceived,
    SubmitOrder,
    TradeRecorded,
    TradingSafetyStop,
)
from prediction_markets.application.markets.models import MarketCycle
from prediction_markets.application.state import EventDispatcher, TradingState
from prediction_markets.domain.arbitrage.services import LongArbitrageDetectionService
from prediction_markets.domain.arbitrage.value_objects import ArbitrageOpportunity
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
from prediction_markets.domain.ports.taker_fees import TakerFeeCalculatorPort
from prediction_markets.domain.shared.value_objects import (
    ClientOrderID,
    ContractID,
    Currency,
    MarketID,
    Money,
    OrderID,
    OutcomeID,
    Price,
    Quantity,
    Timestamp,
    VenueID,
)
from prediction_markets.domain.trading.entities import OrderSnapshot
from prediction_markets.domain.trading.enums import (
    ArbitrageExecutionStatus,
    OrderSide,
    OrderStatus,
    OrderType,
    RecoveryStatus,
    SubmissionStatus,
)
from prediction_markets.domain.trading.value_objects import (
    OrderReference,
    SubmissionResult,
    TradingFee,
)


class _ZeroFees(TakerFeeCalculatorPort):
    """Return a prepared zero settlement fee."""

    async def prepare(self, contract_ids: tuple[ContractID, ...]) -> None:
        """Accept contract preparation without I/O."""

    def calculate(self, contract_id, price, quantity, side) -> TradingFee:
        """Return a zero fee in the common settlement currency."""
        zero = Money(Decimal("0"), Currency("USD"))
        return TradingFee(zero, zero)


class _OnePercentFees(TakerFeeCalculatorPort):
    """Charge one percent of price-times-quantity as settlement cost."""

    async def prepare(self, contract_ids: tuple[ContractID, ...]) -> None:
        """Accept contract preparation without I/O."""

    def calculate(self, contract_id, price, quantity, side) -> TradingFee:
        """Return a price-sensitive fee for execution-limit regression tests."""
        amount = price.value * quantity.value * Decimal("0.01")
        fee = Money(amount, Currency("USD"))
        return TradingFee(fee, fee)


def _contract(
    name: str,
    venue: VenueID,
    outcome: str,
    *,
    tick_size: str | None = "0.01",
    lot_size: str = "1",
    minimum_order_size: str | None = None,
) -> BinaryContract:
    return BinaryContract(
        id=ContractID(name),
        market_id=MarketID(f"{name}-market"),
        outcome_id=OutcomeID(outcome),
        venue_id=venue,
        payout_currency=Currency("USD"),
        tick_size=TickSize(Decimal(tick_size)) if tick_size is not None else None,
        lot_size=LotSize(Decimal(lot_size)),
        minimum_order_size=(
            Quantity(Decimal(minimum_order_size))
            if minimum_order_size is not None
            else None
        ),
    )


def _book(
    contract: BinaryContract,
    price: str,
    quantity: str,
    *,
    bid: bool = False,
) -> OrderBook:
    level = OrderBookLevel(Price(Decimal(price)), Quantity(Decimal(quantity)))
    return OrderBook(
        market_id=contract.market_id,
        outcome_id=contract.outcome_id,
        bids=(level,) if bid else (),
        asks=() if bid else (level,),
        timestamp=Timestamp.now(),
        received_at_ns=time.monotonic_ns(),
    )


def _process(engine: TradingEngine, event: ApplicationEvent) -> list[ApplicationEvent]:
    """Drain derived events, answering asynchronous book requests from test memory."""
    from prediction_markets.application.events import RecoveryBooksReceived, RecoveryPlanningRequested

    pending = deque((event,))
    processed = []
    while pending:
        current = pending.popleft()
        if not isinstance(current, (RecoveryPlanningRequested, RecoveryBooksReceived)):
            processed.append(current)
        if isinstance(current, RecoveryPlanningRequested):
            execution = current.execution
            books = tuple(engine.state.books.get(contract_id) for contract_id in (
                execution.leg1_contract_id, execution.leg2_contract_id,
            ))
            pending.append(RecoveryBooksReceived(
                current, books if all(book is not None for book in books) else None,
            ))
            continue
        pending.extend(engine.process(current))
    return processed


def _risk_adjusted(
    left: BinaryContract,
    right: BinaryContract,
    *,
    left_price: str,
    right_price: str,
    quantity: str,
    side: OrderSide,
    config: EngineConfig,
) -> ArbitrageOpportunity | None:
    pair = MatchedContractPair(left, right, Timestamp.now())
    left_level = OrderBookLevel(
        Price(Decimal(left_price)),
        Quantity(Decimal(quantity)),
    )
    right_level = OrderBookLevel(
        Price(Decimal(right_price)),
        Quantity(Decimal(quantity)),
    )
    gross_edge = (
        Decimal("1") - left_level.price.value - right_level.price.value
        if side is OrderSide.BUY
        else left_level.price.value + right_level.price.value - Decimal("1")
    )
    opportunity = ArbitrageOpportunity(
        left_contract_id=left.id,
        right_contract_id=right.id,
        side=side,
        left_level=left_level,
        right_level=right_level,
        quantity=Quantity(Decimal(quantity)),
        gross_edge=gross_edge,
        net_edge=gross_edge,
        skew_ns=0,
        detected_at=Timestamp.now(),
    )
    engine = TradingEngine(
        EventDispatcher(TradingState()),
        {left.venue_id: _ZeroFees(), right.venue_id: _ZeroFees()},
    )
    engine.configure(config)
    return engine._risk_adjusted_opportunity(opportunity, pair, config)


def _fill(
    command: SubmitOrder,
    price: Price,
    quantity: Quantity,
    fee: TradingFee | None = None,
) -> SubmissionReceived:
    reference = OrderReference(
        command.venue_id,
        command.intent.client_order_id,
        f"{command.role}-recovery".encode(),
    )
    return SubmissionReceived(
        command,
        SubmissionResult(
            SubmissionStatus.ACCEPTED,
            reference,
            OrderSnapshot(
                status=OrderStatus.FILLED,
                contract_id=command.intent.contract_id,
                side=command.intent.side,
                quantity=command.intent.quantity,
                order_type=OrderType.LIMIT,
                client_order_id=command.intent.client_order_id,
                order_id=OrderID(f"{command.role}-order"),
                limit_price=command.intent.limit_price,
                filled_quantity=quantity,
                average_price=price,
                fee=fee,
                updated_at=Timestamp.now(),
            ),
        ),
    )


def _regular_candidate(
    left: BinaryContract,
    right: BinaryContract,
) -> RegularCandidate:
    closes_at = Timestamp.now()
    return RegularCandidate(
        (
            Market(
                id=left.market_id,
                venue_id=left.venue_id,
                title="Left regular market",
                state=MarketState(MarketStatus.ACTIVE, close_time=closes_at),
                yes_side=MarketSide(left.outcome_id, BinaryOutcome.YES),
                no_side=MarketSide(OutcomeID("left-no"), BinaryOutcome.NO),
            ),
            Market(
                id=right.market_id,
                venue_id=right.venue_id,
                title="Right regular market",
                state=MarketState(MarketStatus.ACTIVE, close_time=closes_at),
                yes_side=MarketSide(OutcomeID("right-yes"), BinaryOutcome.YES),
                no_side=MarketSide(right.outcome_id, BinaryOutcome.NO),
            ),
        ),
    )


def test_trading_state_keeps_pair_index_consistent_with_match_events() -> None:
    """Replace and clear pair lookups together with stable match snapshots."""
    left_venue, right_venue = VenueID("left"), VenueID("right")
    first = MatchedContractPair(
        _contract("first-left", left_venue, "yes"),
        _contract("first-right", right_venue, "no"),
        Timestamp.now(),
    )
    second = MatchedContractPair(
        _contract("second-left", left_venue, "yes"),
        _contract("second-right", right_venue, "no"),
        Timestamp.now(),
    )
    cycle = MarketCycle(Underlying("BTC"), 300)
    state = TradingState(matches={cycle: (first,)})

    assert state.affected_pairs(first.left.id) == ((cycle, first),)

    state.apply(MarketMatchesUpdated(cycle, (second,)))

    assert state.affected_pairs(first.left.id) == ()
    assert state.affected_pairs(second.right.id) == ((cycle, second),)

    state.apply(MarketMatchesUpdated(cycle, ()))

    assert state.affected_pairs(second.right.id) == ()


def test_detection_uses_skew_not_book_age() -> None:
    left = _contract("left", VenueID("left"), "yes")
    right = _contract("right", VenueID("right"), "no")
    received_at_ns = time.monotonic_ns() - 1_000 * 1_000_000
    source_at_ns = time.time_ns() - 1_000 * 1_000_000
    left_book = replace(
        _book(left, "0.40", "5"),
        received_at_ns=received_at_ns,
        source_at_ns=source_at_ns,
        source_timestamp_kind="venue_update",
    )
    right_book = replace(
        _book(right, "0.50", "5"),
        received_at_ns=received_at_ns,
        source_at_ns=source_at_ns,
        source_timestamp_kind="venue_update",
    )

    opportunity = LongArbitrageDetectionService().detect(
        left.id,
        right.id,
        left_book,
        right_book,
    )

    assert opportunity is not None


def test_atomic_worker_opportunity_enters_central_state_without_redetection() -> None:
    """Store both books and forward the worker result without rerunning detection."""
    left_venue, right_venue = VenueID("left"), VenueID("right")
    left = _contract("left", left_venue, "yes")
    right = _contract("right", right_venue, "no")
    cycle = MarketCycle(Underlying("BTC"), 300)
    pair = MatchedContractPair(left, right, Timestamp.now())
    left_book = _book(left, "0.40", "5")
    right_book = _book(right, "0.50", "5")
    opportunity = ArbitrageOpportunity(
        left.id,
        right.id,
        OrderSide.BUY,
        left_book.best_ask(),
        right_book.best_ask(),
        Quantity(Decimal("5")),
        Decimal("0.1"),
        Decimal("0.1"),
        0,
        Timestamp.now(),
    )
    state = TradingState()
    engine = TradingEngine(EventDispatcher(state), {})
    engine.configure(
        EngineConfig(
            max_notional_by_venue={},
            min_net_edge=Decimal("0.2"),
        ),
    )
    engine.process(MarketMatchesUpdated(cycle, (pair,)))
    reference = OpportunityValidationRef(
        "generation:1",
        "btc-5m",
        "generation",
        1,
        "left-generation",
        "right-generation",
    )

    events = engine.process(
        OrderBookPairUpdated(
            ArbitrageOpportunityFound(
                "worker-opportunity",
                cycle,
                pair,
                opportunity,
                reference,
            ),
            left_book,
            right_book,
        ),
    )

    assert len(events) == 1
    assert isinstance(events[0], ArbitrageOpportunityFound)
    assert events[0].id == "worker-opportunity"
    assert events[0].opportunity == opportunity
    assert events[0].validation_ref == reference
    assert state.books[left.id].best_ask() is not None
    assert state.books[right.id].best_ask() is not None


def test_stale_worker_pair_cannot_mutate_central_books() -> None:
    """Reject a rolled pair before its worker books enter authoritative state."""
    left_venue, right_venue = VenueID("left"), VenueID("right")
    current = MatchedContractPair(
        _contract("left", left_venue, "yes"),
        _contract("right", right_venue, "no"),
        Timestamp.now(),
    )
    stale = replace(
        current,
        ends_at=Timestamp(current.ends_at.value + timedelta(minutes=5)),
    )
    cycle = MarketCycle(Underlying("BTC"), 300)
    state = TradingState()
    engine = TradingEngine(EventDispatcher(state), {})
    engine.configure(EngineConfig(max_notional_by_venue={}))
    engine.process(MarketMatchesUpdated(cycle, (current,)))

    assert engine.process(
        OrderBookPairUpdated(
            ArbitrageOpportunityFound(
                "stale",
                cycle,
                stale,
                ArbitrageOpportunity(
                    stale.left.id,
                    stale.right.id,
                    OrderSide.BUY,
                    _book(stale.left, "0.40", "5").best_ask(),
                    _book(stale.right, "0.50", "5").best_ask(),
                    Quantity(Decimal("5")),
                    Decimal("0.1"),
                    Decimal("0.1"),
                    0,
                    Timestamp.now(),
                ),
            ),
            _book(stale.left, "0.40", "5"),
            _book(stale.right, "0.50", "5"),
        ),
    ) == ()
    assert state.books == {}


def test_engine_detects_and_plans_explicit_regular_candidate() -> None:
    """Bypass recurring-cycle allowlists for explicitly selected markets."""
    left_venue, right_venue = VenueID("left"), VenueID("right")
    left = _contract("left-yes", left_venue, "yes")
    right = _contract("right-no", right_venue, "no")
    candidate = _regular_candidate(left, right)
    pair = MatchedContractPair(left, right, Timestamp.now())
    engine = TradingEngine(
        EventDispatcher(TradingState()),
        {left_venue: _ZeroFees(), right_venue: _ZeroFees()},
    )
    engine.enable(
        EngineConfig(
            max_notional_by_venue={
                left_venue: Decimal("10"),
                right_venue: Decimal("10"),
            },
            allowed_underlyings=("BTC",),
            allowed_intervals_seconds=(300,),
        ),
    )

    _process(engine, MarketMatchesUpdated(candidate, (pair,)))
    _process(engine, OrderBookUpdated(left_venue, left.id, _book(left, "0.50", "10")))
    events = _process(
        engine,
        OrderBookUpdated(right_venue, right.id, _book(right, "0.40", "5")),
    )

    opportunity = next(
        event for event in events if isinstance(event, ArbitrageOpportunityFound)
    )
    plan = next(event for event in events if isinstance(event, ArbitragePlanned))
    assert opportunity.cycle == candidate
    assert {
        event.role for event in events if isinstance(event, SubmitOrder)
    } == {"primary", "hedge"}
    assert decode_event(encode_event(plan)) == plan


def test_engine_waits_for_collateral_refresh_before_planning_buy() -> None:
    """Emit no commands on low cash and admit after a simulated deposit refresh."""
    polymarket, predict = VenueID("POLYMARKET"), VenueID("PREDICT")
    left = _contract("left-yes", polymarket, "yes")
    right = _contract("right-no", predict, "no")
    candidate = _regular_candidate(left, right)
    pair = MatchedContractPair(left, right, Timestamp.now())
    engine = TradingEngine(
        EventDispatcher(TradingState()),
        {polymarket: _ZeroFees(), predict: _ZeroFees()},
    )
    engine.enable(
        EngineConfig(
            max_notional_by_venue={
                polymarket: Decimal("10"),
                predict: Decimal("10"),
            },
            collateral_by_venue={
                polymarket: Decimal("0.973763"),
                predict: Decimal("10"),
            },
        ),
    )

    _process(engine, MarketMatchesUpdated(candidate, (pair,)))
    _process(engine, OrderBookUpdated(polymarket, left.id, _book(left, "0.40", "5")))
    unfunded = _process(
        engine,
        OrderBookUpdated(predict, right.id, _book(right, "0.40", "5")),
    )

    assert any(isinstance(event, ArbitrageOpportunityFound) for event in unfunded)
    assert not any(
        isinstance(event, (ArbitragePlanned, SubmitOrder)) for event in unfunded
    )
    assert engine.refresh_collateral(
        {polymarket: Decimal("10"), predict: Decimal("10")},
    )

    funded = _process(
        engine,
        OrderBookUpdated(predict, right.id, _book(right, "0.39", "5")),
    )

    assert any(isinstance(event, ArbitragePlanned) for event in funded)
    assert len([event for event in funded if isinstance(event, SubmitOrder)]) == 2


def test_engine_releases_local_buy_reservation_after_zero_fill_rejection() -> None:
    """Return reserved venue cash after both unsubmitted BUY legs reject."""
    polymarket, predict = VenueID("POLYMARKET"), VenueID("PREDICT")
    left = _contract("left-yes", polymarket, "yes")
    right = _contract("right-no", predict, "no")
    candidate = _regular_candidate(left, right)
    pair = MatchedContractPair(left, right, Timestamp.now())
    engine = TradingEngine(
        EventDispatcher(TradingState()),
        {polymarket: _ZeroFees(), predict: _ZeroFees()},
    )
    engine.enable(
        EngineConfig(
            max_notional_by_venue={
                polymarket: Decimal("10"),
                predict: Decimal("10"),
            },
            collateral_by_venue={
                polymarket: Decimal("10"),
                predict: Decimal("10"),
            },
        ),
    )

    _process(engine, MarketMatchesUpdated(candidate, (pair,)))
    _process(engine, OrderBookUpdated(polymarket, left.id, _book(left, "0.40", "5")))
    planned = _process(
        engine,
        OrderBookUpdated(predict, right.id, _book(right, "0.40", "5")),
    )
    execution = next(
        event.execution for event in planned if isinstance(event, ArbitragePlanned)
    )

    assert engine._available_collateral(polymarket) == Decimal("7.99")
    assert engine._available_collateral(predict) == Decimal("7.89")
    _process(
        engine,
        ExecutionUpdated(
            replace(execution, status=ArbitrageExecutionStatus.REJECTED),
        ),
    )

    assert engine._available_collateral(polymarket) == Decimal("10")
    assert engine._available_collateral(predict) == Decimal("10")
    assert engine._collateral_reservations == {}


def test_engine_plans_regular_candidate_during_live_sports_window() -> None:
    """Plan from fresh regular books even when the event timestamp is kickoff."""
    left_venue, right_venue = VenueID("left"), VenueID("right")
    left = _contract("left-yes", left_venue, "yes")
    right = _contract("right-no", right_venue, "no")
    candidate = _regular_candidate(left, right)
    pair = MatchedContractPair(
        left,
        right,
        Timestamp.now() - timedelta(hours=1),
    )
    engine = TradingEngine(
        EventDispatcher(TradingState()),
        {left_venue: _ZeroFees(), right_venue: _ZeroFees()},
    )
    engine.enable(
        EngineConfig(
            max_notional_by_venue={
                left_venue: Decimal("10"),
                right_venue: Decimal("10"),
            },
            min_market_time_remaining_seconds=20,
        ),
    )

    _process(engine, MarketMatchesUpdated(candidate, (pair,)))
    _process(engine, OrderBookUpdated(left_venue, left.id, _book(left, "0.50", "10")))
    events = _process(
        engine,
        OrderBookUpdated(right_venue, right.id, _book(right, "0.40", "5")),
    )

    assert any(isinstance(event, ArbitragePlanned) for event in events)


@pytest.mark.parametrize(
    ("interval_seconds", "guard_seconds"),
    ((300, 30), (900, 60), (3600, 120)),
)
def test_engine_does_not_plan_when_market_closes_inside_guard_window(
    interval_seconds: int,
    guard_seconds: int,
) -> None:
    """Keep a stale opportunity out of order planning near market expiry."""
    left_venue, right_venue = VenueID("left"), VenueID("right")
    left = _contract("left-yes", left_venue, "yes")
    right = _contract("right-no", right_venue, "no")
    pair = MatchedContractPair(
        left,
        right,
        Timestamp.now() + timedelta(seconds=guard_seconds - 1),
    )
    engine = TradingEngine(
        EventDispatcher(TradingState()),
        {left_venue: _ZeroFees(), right_venue: _ZeroFees()},
    )
    engine.enable(
        EngineConfig(
            max_notional_by_venue={
                left_venue: Decimal("10"),
                right_venue: Decimal("10"),
            },
            min_market_time_remaining_seconds=120,
        ),
    )

    cycle = MarketCycle(Underlying("BTC"), interval_seconds)
    _process(engine, MarketMatchesUpdated(cycle, (pair,)))
    _process(engine, OrderBookUpdated(left_venue, left.id, _book(left, "0.50", "10")))
    events = _process(
        engine,
        OrderBookUpdated(right_venue, right.id, _book(right, "0.40", "5")),
    )

    assert any(isinstance(event, ArbitrageOpportunityFound) for event in events)
    assert not any(isinstance(event, ArbitragePlanned) for event in events)
    assert not any(isinstance(event, SubmitOrder) for event in events)


def test_engine_executes_sub_dollar_short_only_for_selected_market_key() -> None:
    """Use venue share minimums, not the BUY notional minimum, for SELL orders."""

    def detect(short_market_keys: frozenset[str]) -> list[ApplicationEvent]:
        left_venue, right_venue = VenueID("left"), VenueID("right")
        left = _contract(
            "left-yes",
            left_venue,
            "yes",
            minimum_order_size="5",
        )
        right = _contract("right-no", right_venue, "no")
        cycle = MarketCycle(Underlying("BTC"), 300)
        pair = MatchedContractPair(left, right, Timestamp.now())
        engine = TradingEngine(
            EventDispatcher(TradingState()),
            {left_venue: _ZeroFees(), right_venue: _ZeroFees()},
        )
        engine.enable(
            EngineConfig(
                max_notional_by_venue={
                    left_venue: Decimal("10"),
                    right_venue: Decimal("10"),
                },
                execute_long=False,
                execute_short=True,
                short_market_keys=short_market_keys,
                short_pair_keys=frozenset({pair.key}),
                short_inventory_by_contract={
                    left.id: Quantity(Decimal("5")),
                    right.id: Quantity(Decimal("5")),
                },
                allowed_underlyings=("BTC",),
                allowed_intervals_seconds=(300,),
            ),
        )
        _process(
            engine,
            MarketMatchesUpdated(
                cycle,
                (pair,),
            ),
        )
        _process(
            engine,
            OrderBookUpdated(
                left_venue,
                left.id,
                _book(left, "0.96", "5", bid=True),
            ),
        )
        return _process(
            engine,
            OrderBookUpdated(
                right_venue,
                right.id,
                _book(right, "0.05", "5", bid=True),
            ),
        )

    unselected = detect(frozenset({"cycle:ETH:300"}))
    selected = detect(frozenset({"cycle:BTC:300"}))

    assert any(isinstance(event, ArbitrageOpportunityFound) for event in unselected)
    assert not any(isinstance(event, SubmitOrder) for event in unselected)
    commands = [event for event in selected if isinstance(event, SubmitOrder)]
    assert len(commands) == 2
    assert all(command.intent.side is OrderSide.SELL for command in commands)
    assert all(command.intent.quantity == Quantity(Decimal("5")) for command in commands)


@pytest.mark.parametrize(
    ("side", "left_price", "right_price"),
    (
        (OrderSide.BUY, "0.497", "0.49"),
        (OrderSide.SELL, "0.993", "0.02"),
    ),
)
def test_engine_rejects_edge_lost_to_executable_tick_rounding(
    side: OrderSide,
    left_price: str,
    right_price: str,
) -> None:
    """Discard raw edges that disappear after side-aware tick rounding."""
    left_venue, right_venue = VenueID("left"), VenueID("right")
    left = _contract("left-yes", left_venue, "yes")
    right = _contract("right-no", right_venue, "no")
    cycle = MarketCycle(Underlying("BTC"), 300)
    pair = MatchedContractPair(left, right, Timestamp.now())
    engine = TradingEngine(
        EventDispatcher(TradingState()),
        {left_venue: _ZeroFees(), right_venue: _ZeroFees()},
    )
    is_short = side is OrderSide.SELL
    engine.enable(
        EngineConfig(
            max_notional_by_venue={
                left_venue: Decimal("10"),
                right_venue: Decimal("10"),
            },
            min_net_edge=Decimal("0.011"),
            execute_long=not is_short,
            execute_short=is_short,
            short_market_keys=(
                frozenset({"cycle:BTC:300"}) if is_short else frozenset()
            ),
            short_pair_keys=frozenset({pair.key}) if is_short else frozenset(),
            short_inventory_by_contract=(
                {
                    left.id: Quantity(Decimal("5")),
                    right.id: Quantity(Decimal("5")),
                }
                if is_short
                else {}
            ),
            allowed_underlyings=("BTC",),
            allowed_intervals_seconds=(300,),
        ),
    )

    _process(engine, MarketMatchesUpdated(cycle, (pair,)))
    _process(
        engine,
        OrderBookUpdated(
            left_venue,
            left.id,
            _book(left, left_price, "5", bid=is_short),
        ),
    )
    events = _process(
        engine,
        OrderBookUpdated(
            right_venue,
            right.id,
            _book(right, right_price, "5", bid=is_short),
        ),
    )

    assert not any(
        isinstance(
            event,
            (ArbitrageOpportunityFound, ArbitragePlanned, SubmitOrder),
        )
        for event in events
    )


@pytest.mark.parametrize(
    ("filled_quantity", "expected_second_commands"),
    (("3", 0), ("0", 2)),
)
def test_engine_releases_only_unfilled_short_inventory_after_completion(
    filled_quantity: str,
    expected_second_commands: int,
) -> None:
    """Reuse a completed short reservation only when its tokens were not sold."""
    left_venue, right_venue = VenueID("left"), VenueID("right")
    left = _contract("left-yes", left_venue, "yes")
    right = _contract("right-no", right_venue, "no")
    cycle = MarketCycle(Underlying("BTC"), 300)
    pair = MatchedContractPair(left, right, Timestamp.now())
    state = TradingState()
    engine = TradingEngine(
        EventDispatcher(state),
        {left_venue: _ZeroFees(), right_venue: _ZeroFees()},
    )
    engine.enable(
        EngineConfig(
            max_notional_by_venue={
                left_venue: Decimal("10"),
                right_venue: Decimal("10"),
            },
            max_arbitrages=2,
            execute_long=False,
            execute_short=True,
            short_market_keys=frozenset({"cycle:BTC:300"}),
            short_pair_keys=frozenset({pair.key}),
            short_inventory_by_contract={
                left.id: Quantity(Decimal("3")),
                right.id: Quantity(Decimal("3")),
            },
            allowed_underlyings=("BTC",),
            allowed_intervals_seconds=(300,),
        ),
    )
    _process(engine, MarketMatchesUpdated(cycle, (pair,)))
    _process(
        engine,
        OrderBookUpdated(left_venue, left.id, _book(left, "0.60", "5", bid=True)),
    )
    first = _process(
        engine,
        OrderBookUpdated(right_venue, right.id, _book(right, "0.50", "5", bid=True)),
    )

    commands = [event for event in first if isinstance(event, SubmitOrder)]
    assert len(commands) == 2
    assert all(command.intent.quantity == Quantity(Decimal("3")) for command in commands)
    execution = next(
        event.execution for event in first if isinstance(event, ArbitragePlanned)
    )
    _process(
        engine,
        ExecutionUpdated(
            replace(
                execution,
                status=ArbitrageExecutionStatus.COMPLETED,
                leg1_filled_quantity=Quantity(Decimal(filled_quantity)),
                leg2_filled_quantity=Quantity(Decimal(filled_quantity)),
            ),
        ),
    )
    second = _process(
        engine,
        OrderBookUpdated(right_venue, right.id, _book(right, "0.51", "5", bid=True)),
    )

    assert any(isinstance(event, ArbitrageOpportunityFound) for event in second)
    assert len([event for event in second if isinstance(event, SubmitOrder)]) == (
        expected_second_commands
    )


def test_engine_submits_both_legs_and_accepts_out_of_order_fills() -> None:
    """Finalize equal fills even when the hedge response arrives first."""
    left_venue, right_venue = VenueID("left"), VenueID("right")
    left = _contract("left-yes", left_venue, "yes")
    right = _contract("right-no", right_venue, "no")
    cycle = MarketCycle(Underlying("BTC"), 300)
    pair = MatchedContractPair(left, right, Timestamp.now())
    state = TradingState()
    engine = TradingEngine(
        EventDispatcher(state),
        {left_venue: _ZeroFees(), right_venue: _ZeroFees()},
    )
    engine.enable(
        EngineConfig(
            max_notional_by_venue={
                left_venue: Decimal("10"),
                right_venue: Decimal("10"),
            },
            cost_buffer=Decimal("0.01"),
            allowed_underlyings=("BTC",),
            allowed_intervals_seconds=(300,),
        ),
    )

    _process(engine, MarketMatchesUpdated(cycle, (pair,)))
    _process(engine, OrderBookUpdated(left_venue, left.id, _book(left, "0.50", "10")))
    planned = _process(
        engine,
        OrderBookUpdated(right_venue, right.id, _book(right, "0.40", "5")),
    )
    opportunity = next(
        event for event in planned if isinstance(event, ArbitrageOpportunityFound)
    )
    commands = {
        event.role: event for event in planned if isinstance(event, SubmitOrder)
    }
    primary = commands["primary"]
    hedge = commands["hedge"]
    plan_event = next(
        event for event in planned if isinstance(event, ArbitragePlanned)
    )
    primary_decision = plan_event.execution.leg1_decision
    hedge_decision = plan_event.execution.leg2_decision

    assert opportunity.opportunity.gross_edge == Decimal("0.10")
    assert opportunity.opportunity.net_edge == Decimal("0.09")
    assert primary.role == "primary"
    assert primary.intent.contract_id == right.id
    assert primary.intent.quantity == Quantity(Decimal("5"))
    assert hedge.intent.contract_id == left.id
    assert hedge.intent.quantity == Quantity(Decimal("5"))
    assert hedge.intent.limit_price == Price(Decimal("0.50"))
    assert primary_decision is not None
    assert primary_decision.available_quantity() == Quantity(Decimal("5"))
    assert primary_decision.shortfall_quantity() == Quantity(Decimal("0"))
    assert hedge_decision is not None
    assert hedge_decision.available_quantity() == Quantity(Decimal("10"))
    assert hedge_decision.shortfall_quantity() == Quantity(Decimal("0"))
    assert decode_event(encode_event(plan_event)) == plan_event

    realized = Quantity(Decimal("3"))
    hedge_events = _process(
        engine,
        _fill(hedge, Price(Decimal("0.50")), realized),
    )
    hedge_update = next(
        event.execution
        for event in hedge_events
        if isinstance(event, ExecutionUpdated)
    )
    assert hedge_update.status is ArbitrageExecutionStatus.PRIMARY_PENDING

    completed = _process(engine, _fill(primary, Price(Decimal("0.40")), realized))
    completed_event = next(
        event
        for event in completed
        if isinstance(event, ExecutionUpdated)
    )
    execution = completed_event.execution
    assert execution.status is ArbitrageExecutionStatus.COMPLETED
    assert execution.leg1_filled_quantity == realized
    assert execution.leg2_filled_quantity == realized
    assert execution.residual_quantity == Quantity(Decimal("0"))
    assert set(state.timings[execution.id].terminal_at_ns) == {
        "primary",
        "hedge",
    }
    assert completed_event.latency_trace_json is not None
    assert json.loads(completed_event.latency_trace_json)["outcome"] == "terminal"
    assert decode_event(encode_event(completed_event)) == completed_event

    current = state.orders[primary.intent.client_order_id]
    fee = TradingFee(
        Money(Decimal("0.05926"), Currency("USDC")),
        Money(Decimal("0.05926"), Currency("USD")),
    )
    correction = _process(
        engine,
        OrderSnapshotUpdated(
            execution.id,
            primary.role,
            OrderReference(
                primary.venue_id,
                primary.intent.client_order_id,
                b"fee-reconciliation",
            ),
            replace(current, fee=fee),
            "get",
        ),
    )
    corrected = next(
        trade
        for trade in state.trades.values()
        if trade.client_order_id == primary.intent.client_order_id
    )
    assert corrected.fee == fee.charged
    assert corrected.fee_settlement_cost == fee.settlement_cost
    correction_event = next(
        event
        for event in correction
        if isinstance(event, AccountingCorrectionRecorded)
    )
    assert correction_event.correction.original_trade.fee is None
    assert correction_event.correction.replacement_trade == corrected
    assert sum(isinstance(event, TradeRecorded) for event in correction) == 0


@pytest.mark.parametrize("with_snapshot", [False, True, "delayed_fill", "late_previous"])
def test_engine_requotes_an_empty_recovery_before_resolving_exposure(with_snapshot) -> None:
    """Use a fresh order identity and quote after a terminal zero-fill attempt."""
    left_venue, right_venue = VenueID("left"), VenueID("right")
    left = _contract("left-yes", left_venue, "yes")
    right = _contract("right-no", right_venue, "no")
    cycle = MarketCycle(Underlying("BTC"), 300)
    pair = MatchedContractPair(left, right, Timestamp.now())
    state = TradingState()
    engine = TradingEngine(
        EventDispatcher(state),
        {left_venue: _ZeroFees(), right_venue: _ZeroFees()},
    )
    engine.enable(
        EngineConfig(
            max_notional_by_venue={left_venue: Decimal("10"), right_venue: Decimal("10")},
            max_recovery_loss=Decimal("1"),
            allowed_underlyings=("BTC",),
            allowed_intervals_seconds=(300,),
        ),
    )
    _process(engine, MarketMatchesUpdated(cycle, (pair,)))
    _process(engine, OrderBookUpdated(left_venue, left.id, _book(left, "0.50", "10")))
    planned = _process(
        engine,
        OrderBookUpdated(right_venue, right.id, _book(right, "0.40", "5")),
    )
    commands = {
        event.role: event for event in planned if isinstance(event, SubmitOrder)
    }

    _process(
        engine,
        _fill(commands["primary"], Price(Decimal("0.40")), Quantity(Decimal("5"))),
    )
    residual = _process(
        engine,
        _fill(commands["hedge"], Price(Decimal("0.50")), Quantity(Decimal("3"))),
    )
    recovery_command = next(
        event
        for event in residual
        if isinstance(event, SubmitOrder) and event.role == "recovery"
    )
    recovery_plan = next(
        event for event in residual if isinstance(event, RecoveryPlanned)
    )
    if with_snapshot == "delayed_fill":
        uncertain = replace(
            _fill(recovery_command, recovery_command.intent.limit_price, Quantity(Decimal("0"))).result.snapshot,
            status=OrderStatus.CANCELLED, may_receive_more_fills=True,
        )
        pending = _process(engine, SubmissionReceived(
            recovery_command,
            SubmissionResult(SubmissionStatus.ACCEPTED,
                OrderReference(recovery_command.venue_id, recovery_command.intent.client_order_id, b"pending"),
                uncertain),
        ))
        assert not any(isinstance(event, SubmitOrder) for event in pending)
        assert state.recoveries[recovery_command.execution_id].attempts == 1
        assert not engine.recovery_outputs()
        fill_event = _fill(recovery_command, recovery_command.intent.limit_price,
            recovery_command.intent.quantity)
        completed = _process(engine, fill_event)
        duplicate = _process(engine, fill_event)
        assert not any(isinstance(event, SubmitOrder) for event in completed + duplicate)
        assert not any(isinstance(event, TradeRecorded) for event in duplicate)
        assert state.executions[recovery_command.execution_id].status is ArbitrageExecutionStatus.RECOVERED
        assert state.executions[recovery_command.execution_id].residual_quantity.value == 0
        return
    state.books[recovery_command.intent.contract_id] = _book(
        left,
        "0.55",
        "10",
    )
    rejected_reference = OrderReference(
        recovery_command.venue_id,
        recovery_command.intent.client_order_id,
        b"rejected-recovery",
    )
    rejected_snapshot = OrderSnapshot(
        status=OrderStatus.REJECTED,
        contract_id=recovery_command.intent.contract_id,
        side=recovery_command.intent.side,
        quantity=recovery_command.intent.quantity,
        order_type=recovery_command.intent.order_type,
        client_order_id=recovery_command.intent.client_order_id,
        reason="no orders found to match",
    ) if with_snapshot else None
    retried = _process(
        engine,
        SubmissionReceived(
            recovery_command,
            SubmissionResult(
                SubmissionStatus.ACCEPTED if with_snapshot else SubmissionStatus.REJECTED,
                rejected_reference,
                snapshot=rejected_snapshot,
                reason=None if with_snapshot else "no orders found to match",
            ),
        ),
    )
    retry_command = next(
        event
        for event in retried
        if isinstance(event, SubmitOrder) and event.role == "recovery"
    )
    assert any(
        isinstance(event, ExecutionUpdated)
        and "no orders found to match" in (event.execution.last_error or "")
        for event in retried
    )
    assert retry_command.intent.client_order_id != recovery_command.intent.client_order_id
    assert retry_command.intent.client_order_id == ClientOrderID(
        f"{retry_command.execution_id}-recovery-2",
    )
    assert retry_command.intent.limit_price == Price(Decimal("0.55"))

    if with_snapshot == "late_previous":
        late = _fill(recovery_command, recovery_command.intent.limit_price,
            recovery_command.intent.quantity)
        observed = _process(engine, late)
        assert any(isinstance(event, TradeRecorded) for event in observed)
        assert any(isinstance(event, TradingSafetyStop) for event in observed)
        assert state.executions[recovery_command.execution_id].status is ArbitrageExecutionStatus.NEEDS_REVIEW
        assert not state.trading_enabled
        assert not any(isinstance(event, TradeRecorded) for event in _process(engine, late))
        return

    zero = Money(Decimal("0"), Currency("USD"))
    partial_event = _fill(
        retry_command,
        Price(Decimal("0.55")),
        Quantity(Decimal("0.5")),
        TradingFee(zero, zero),
    )
    partial_snapshot = replace(
        partial_event.result.snapshot,
        status=OrderStatus.PARTIALLY_FILLED,
    )
    assert not is_settled_order(
        retry_command,
        replace(partial_snapshot, may_receive_more_fills=True),
    )
    partial = _process(
        engine,
        replace(
            partial_event,
            result=replace(partial_event.result, snapshot=partial_snapshot),
        ),
    )
    assert (
        state.executions[recovery_command.execution_id].status
        is ArbitrageExecutionStatus.NEEDS_REVIEW
    )
    assert any(
        isinstance(event, ExecutionUpdated)
        and event.execution.status is ArbitrageExecutionStatus.NEEDS_REVIEW
        for event in partial
    )
    recovered = _process(
        engine,
        _fill(
            retry_command,
            Price(Decimal("0.55")),
            Quantity(Decimal("2")),
            TradingFee(zero, zero),
        ),
    )

    recovery = state.recoveries[recovery_command.execution_id]
    assert recovery.status is RecoveryStatus.RESOLVED
    assert recovery.attempts == 2
    assert decode_event(encode_event(recovery_plan)) == recovery_plan
    assert recovery.estimated_net_result == Decimal("0.10")
    assert recovery.actual_net_result == Decimal("0.10")
    assert recovery.filled_quantity == Quantity(Decimal("2"))
    assert state.executions[recovery_command.execution_id].status is ArbitrageExecutionStatus.RECOVERED
    assert state.executions[recovery_command.execution_id].residual_quantity == Quantity(Decimal("0"))
    assert any(
        isinstance(event, ExecutionUpdated)
        and event.execution.status is ArbitrageExecutionStatus.RECOVERED
        for event in recovered
    )


def test_engine_stops_after_successful_recovery_counts_toward_max_arbitrages() -> None:
    """Count a neutralized recovery toward the run quota like a completed hedge."""
    left_venue, right_venue = VenueID("left"), VenueID("right")
    left = _contract("left-yes", left_venue, "yes")
    right = _contract("right-no", right_venue, "no")
    cycle = MarketCycle(Underlying("BTC"), 300)
    pair = MatchedContractPair(left, right, Timestamp.now())
    state = TradingState()
    engine = TradingEngine(
        EventDispatcher(state),
        {left_venue: _ZeroFees(), right_venue: _ZeroFees()},
    )
    engine.enable(
        EngineConfig(
            max_notional_by_venue={
                left_venue: Decimal("10"),
                right_venue: Decimal("10"),
            },
            max_arbitrages=1,
            allowed_underlyings=("BTC",),
            allowed_intervals_seconds=(300,),
        ),
    )
    _process(engine, MarketMatchesUpdated(cycle, (pair,)))
    _process(engine, OrderBookUpdated(left_venue, left.id, _book(left, "0.50", "5")))
    first = _process(
        engine,
        OrderBookUpdated(right_venue, right.id, _book(right, "0.40", "5")),
    )
    execution = next(
        event.execution for event in first if isinstance(event, ArbitragePlanned)
    )

    _process(
        engine,
        ExecutionUpdated(
            replace(
                execution,
                status=ArbitrageExecutionStatus.RECOVERED,
                residual_quantity=Quantity(Decimal("0")),
                last_error=None,
            ),
        ),
    )
    next_opportunity = _process(
        engine,
        OrderBookUpdated(left_venue, left.id, _book(left, "0.49", "5")),
    )

    assert state.trading_enabled is False
    assert not any(isinstance(event, SubmitOrder) for event in next_opportunity)


def test_restart_accounting_records_only_the_unjournaled_fill_delta() -> None:
    """Avoid duplicating an earlier position when replay sees a later terminal fill."""
    left_venue, right_venue = VenueID("left"), VenueID("right")
    left = _contract("left-yes", left_venue, "yes")
    right = _contract("right-no", right_venue, "no")
    cycle = MarketCycle(Underlying("BTC"), 300)
    state = TradingState()
    engine = TradingEngine(
        EventDispatcher(state),
        {left_venue: _ZeroFees(), right_venue: _ZeroFees()},
    )
    engine.enable(
        EngineConfig(
            max_notional_by_venue={left_venue: Decimal("10"), right_venue: Decimal("10")},
            allowed_underlyings=("BTC",),
            allowed_intervals_seconds=(300,),
        ),
    )
    _process(
        engine,
        MarketMatchesUpdated(cycle, (MatchedContractPair(left, right, Timestamp.now()),)),
    )
    _process(engine, OrderBookUpdated(left_venue, left.id, _book(left, "0.50", "10")))
    planned = _process(
        engine,
        OrderBookUpdated(right_venue, right.id, _book(right, "0.40", "5")),
    )
    command = next(
        event
        for event in planned
        if isinstance(event, SubmitOrder) and event.role == "primary"
    )
    reference = OrderReference(
        command.venue_id,
        command.intent.client_order_id,
        b"restart-accounting",
    )
    partial = OrderSnapshot(
        status=OrderStatus.ACCEPTED,
        contract_id=command.intent.contract_id,
        side=command.intent.side,
        quantity=command.intent.quantity,
        order_type=command.intent.order_type,
        client_order_id=command.intent.client_order_id,
        order_id=OrderID("primary-order"),
        limit_price=command.intent.limit_price,
        filled_quantity=Quantity(Decimal("2")),
        average_price=Price(Decimal("0.40")),
        updated_at=Timestamp.now(),
    )
    _process(
        engine,
        SubmissionReceived(
            command,
            SubmissionResult(SubmissionStatus.ACCEPTED, reference, partial),
        ),
    )
    engine.dispatcher.dispatch(
        OrderSnapshotUpdated(
            command.execution_id,
            command.role,
            reference,
            replace(
                partial,
                status=OrderStatus.FILLED,
                filled_quantity=Quantity(Decimal("3")),
            ),
            "get",
        ),
        replay=True,
    )

    outputs = engine.recovery_outputs()
    replayed_trade = next(
        event.trade for event in outputs if isinstance(event, TradeRecorded)
    )

    assert replayed_trade.quantity == Quantity(Decimal("1"))


@pytest.mark.parametrize(
    ("max_concurrent_arbitrages", "second_admitted"),
    ((1, False), (2, True)),
)
def test_engine_limits_independent_arbitrages_by_concurrent_capacity(
    max_concurrent_arbitrages: int,
    second_admitted: bool,
) -> None:
    """Apply the concurrent limit independently of the larger run total."""
    left_venue, right_venue = VenueID("left"), VenueID("right")
    first_left = _contract("first-left-yes", left_venue, "yes")
    first_right = _contract("first-right-no", right_venue, "no")
    second_left = _contract("second-left-yes", left_venue, "yes")
    second_right = _contract("second-right-no", right_venue, "no")
    cycle = MarketCycle(Underlying("BTC"), 300)
    state = TradingState()
    engine = TradingEngine(
        EventDispatcher(state),
        {left_venue: _ZeroFees(), right_venue: _ZeroFees()},
    )
    config = EngineConfig(
        max_notional_by_venue={
            left_venue: Decimal("10"),
            right_venue: Decimal("10"),
        },
        allowed_underlyings=("BTC",),
        allowed_intervals_seconds=(300,),
        max_arbitrages=10,
        max_concurrent_arbitrages=max_concurrent_arbitrages,
    )
    engine.enable(config)
    pairs = (
        MatchedContractPair(first_left, first_right, Timestamp.now()),
        MatchedContractPair(second_left, second_right, Timestamp.now()),
    )
    _process(engine, MarketMatchesUpdated(cycle, pairs))
    _process(
        engine,
        OrderBookUpdated(
            left_venue,
            first_left.id,
            _book(first_left, "0.50", "10"),
        ),
    )
    first_events = _process(
        engine,
        OrderBookUpdated(
            right_venue,
            first_right.id,
            _book(first_right, "0.40", "5"),
        ),
    )
    first_commands = tuple(
        event for event in first_events if isinstance(event, SubmitOrder)
    )
    assert {command.role for command in first_commands} == {"primary", "hedge"}
    with pytest.raises(RuntimeError, match="executions are active"):
        engine.configure(config)

    _process(
        engine,
        OrderBookUpdated(
            left_venue,
            second_left.id,
            _book(second_left, "0.50", "10"),
        ),
    )
    second_events = _process(
        engine,
        OrderBookUpdated(
            right_venue,
            second_right.id,
            _book(second_right, "0.40", "5"),
        ),
    )

    assert any(isinstance(event, ArbitrageOpportunityFound) for event in second_events)
    second_commands = [
        event for event in second_events if isinstance(event, SubmitOrder)
    ]
    assert len(second_commands) == (2 if second_admitted else 0)
    assert engine.active_execution_count() == 1 + second_admitted


def test_engine_blocks_complementary_equivalent_route_until_terminal() -> None:
    """Reserve BUY NO / SELL YES equivalence before plan events are applied."""
    polymarket, predict = VenueID("POLYMARKET"), VenueID("PREDICT")
    poly_market = MarketID("poly-market")
    predict_market = MarketID("predict-market")
    poly_yes = replace(
        _contract("poly-yes", polymarket, "yes"),
        market_id=poly_market,
    )
    poly_no = replace(
        _contract("poly-no", polymarket, "no"),
        market_id=poly_market,
    )
    predict_yes = replace(
        _contract("predict-yes", predict, "yes"),
        market_id=predict_market,
    )
    predict_no = replace(
        _contract("predict-no", predict, "no"),
        market_id=predict_market,
    )
    long_pair = MatchedContractPair(poly_yes, predict_no, Timestamp.now())
    short_pair = MatchedContractPair(poly_no, predict_yes, Timestamp.now())
    cycle = MarketCycle(Underlying("BTC"), 300)
    state = TradingState()
    engine = TradingEngine(
        EventDispatcher(state),
        {polymarket: _ZeroFees(), predict: _ZeroFees()},
    )
    engine.enable(
        EngineConfig(
            max_notional_by_venue={
                polymarket: Decimal("20"),
                predict: Decimal("20"),
            },
            max_arbitrages=2,
            execute_short=True,
            short_market_keys=frozenset({"cycle:BTC:300"}),
            short_pair_keys=frozenset({short_pair.key}),
            short_inventory_by_contract={
                poly_no.id: Quantity(Decimal("5")),
                predict_yes.id: Quantity(Decimal("5")),
            },
            allowed_underlyings=("BTC",),
            allowed_intervals_seconds=(300,),
        ),
    )
    engine.process(MarketMatchesUpdated(cycle, (long_pair, short_pair)))

    def opportunity(
        opportunity_id: str,
        pair: MatchedContractPair,
        side: OrderSide,
        left_price: str,
        right_price: str,
    ) -> ArbitrageOpportunityFound:
        """Build one focused admission event for the route test."""
        left_level = OrderBookLevel(
            Price(Decimal(left_price)),
            Quantity(Decimal("5")),
        )
        right_level = OrderBookLevel(
            Price(Decimal(right_price)),
            Quantity(Decimal("5")),
        )
        gross_edge = (
            Decimal("1") - left_level.price.value - right_level.price.value
            if side is OrderSide.BUY
            else left_level.price.value + right_level.price.value - Decimal("1")
        )
        return ArbitrageOpportunityFound(
            id=opportunity_id,
            cycle=cycle,
            pair=pair,
            opportunity=ArbitrageOpportunity(
                left_contract_id=pair.left.id,
                right_contract_id=pair.right.id,
                side=side,
                left_level=left_level,
                right_level=right_level,
                quantity=Quantity(Decimal("5")),
                gross_edge=gross_edge,
                net_edge=gross_edge,
                skew_ns=0,
                detected_at=Timestamp.now(),
            ),
        )

    long_event = opportunity("long", long_pair, OrderSide.BUY, "0.50", "0.40")
    short_event = opportunity("short", short_pair, OrderSide.SELL, "0.60", "0.50")
    long_outputs = engine.process(long_event)
    blocked_outputs = engine.process(short_event)

    assert any(isinstance(event, ArbitragePlanned) for event in long_outputs)
    assert not any(isinstance(event, ArbitragePlanned) for event in blocked_outputs)
    assert engine.active_execution_count() == 1

    commands = tuple(event for event in long_outputs if isinstance(event, SubmitOrder))
    for output in long_outputs:
        engine.process(output)
    for command in commands:
        _process(
            engine,
            SubmissionReceived(
                command,
                SubmissionResult(
                    SubmissionStatus.REJECTED,
                    OrderReference(
                        command.venue_id,
                        command.intent.client_order_id,
                        f"{command.role}-rejected".encode(),
                    ),
                    reason="no orders found to match",
                ),
            ),
        )

    resumed = engine.process(replace(short_event, id="short-after-terminal"))

    assert any(isinstance(event, ArbitragePlanned) for event in resumed)


@pytest.mark.parametrize(
    ("max_arbitrages", "max_notional", "expected_short_quantity"),
    ((1, "20", None), (2, "2.6", None), (2, "20", "5")),
)
def test_engine_admits_opposite_lane_only_within_run_limit(
    max_arbitrages: int,
    max_notional: str,
    expected_short_quantity: str | None,
) -> None:
    """Allow opposite lanes only within run and shared venue budgets."""
    left_venue, right_venue = VenueID("left"), VenueID("right")
    long_left = _contract("long-left-yes", left_venue, "yes")
    long_right = _contract("long-right-no", right_venue, "no")
    short_left = _contract("short-left-yes", left_venue, "yes")
    short_right = _contract("short-right-no", right_venue, "no")
    long_pair = MatchedContractPair(long_left, long_right, Timestamp.now())
    short_pair = MatchedContractPair(short_left, short_right, Timestamp.now())
    cycle = MarketCycle(Underlying("BTC"), 300)
    state = TradingState()
    engine = TradingEngine(
        EventDispatcher(state),
        {left_venue: _ZeroFees(), right_venue: _ZeroFees()},
    )
    engine.enable(
        EngineConfig(
            max_notional_by_venue={
                left_venue: Decimal(max_notional),
                right_venue: Decimal(max_notional),
            },
            max_arbitrages=max_arbitrages,
            execute_short=True,
            short_market_keys=frozenset({"cycle:BTC:300"}),
            short_pair_keys=frozenset({short_pair.key}),
            short_inventory_by_contract={
                short_left.id: Quantity(Decimal("5")),
                short_right.id: Quantity(Decimal("5")),
            },
            allowed_underlyings=("BTC",),
            allowed_intervals_seconds=(300,),
        ),
    )
    _process(engine, MarketMatchesUpdated(cycle, (long_pair, short_pair)))
    _process(
        engine,
        OrderBookUpdated(left_venue, long_left.id, _book(long_left, "0.50", "5")),
    )
    long_events = _process(
        engine,
        OrderBookUpdated(
            right_venue,
            long_right.id,
            _book(long_right, "0.40", "5"),
        ),
    )
    _process(
        engine,
        OrderBookUpdated(
            left_venue,
            short_left.id,
            _book(short_left, "0.60", "5", bid=True),
        ),
    )
    short_events = _process(
        engine,
        OrderBookUpdated(
            right_venue,
            short_right.id,
            _book(short_right, "0.50", "5", bid=True),
        ),
    )

    assert len([event for event in long_events if isinstance(event, SubmitOrder)]) == 2
    short_commands = [
        event for event in short_events if isinstance(event, SubmitOrder)
    ]
    assert len(short_commands) == (2 if expected_short_quantity is not None else 0)
    if expected_short_quantity is not None:
        assert all(
            command.intent.quantity == Quantity(Decimal(expected_short_quantity))
            for command in short_commands
        )
    assert engine.active_execution_count() == 1 + bool(short_commands)


def test_engine_deduplicates_continuous_signal_and_reemits_after_gap() -> None:
    """Emit one economic signal until it disappears, preserving snapshot IDs."""
    left_venue, right_venue = VenueID("left"), VenueID("right")
    left = _contract("left-yes", left_venue, "yes")
    right = _contract("right-no", right_venue, "no")
    cycle = MarketCycle(Underlying("BTC"), 300)
    state = TradingState()
    engine = TradingEngine(
        EventDispatcher(state),
        {left_venue: _ZeroFees(), right_venue: _ZeroFees()},
    )
    engine.enable(
        EngineConfig(
            max_notional_by_venue={
                left_venue: Decimal("10"),
                right_venue: Decimal("10"),
            },
            allowed_underlyings=("BTC",),
            allowed_intervals_seconds=(300,),
            max_arbitrages=2,
        ),
    )
    _process(
        engine,
        MarketMatchesUpdated(
            cycle,
            (MatchedContractPair(left, right, Timestamp.now()),),
        ),
    )
    _process(
        engine,
        OrderBookUpdated(left_venue, left.id, _book(left, "0.50", "10")),
    )
    right_update = OrderBookUpdated(
        right_venue,
        right.id,
        _book(right, "0.40", "5"),
    )
    first = _process(engine, right_update)
    opportunity = next(
        event for event in first if isinstance(event, ArbitrageOpportunityFound)
    )

    replayed = _process(engine, right_update)
    refreshed = _process(
        engine,
        OrderBookUpdated(right_venue, right.id, _book(right, "0.40", "5")),
    )
    disappeared = _process(
        engine,
        OrderBookUpdated(right_venue, right.id, _book(right, "0.50", "5")),
    )
    returned = _process(
        engine,
        OrderBookUpdated(right_venue, right.id, _book(right, "0.40", "5")),
    )
    returned_opportunity = next(
        event for event in returned if isinstance(event, ArbitrageOpportunityFound)
    )

    for events in (replayed, refreshed, disappeared):
        assert not any(
            isinstance(event, ArbitrageOpportunityFound)
            for event in events
        )
    assert returned_opportunity.id != opportunity.id
    assert len(state.opportunities) == 2
    assert len(state.executions) == 1


@pytest.mark.parametrize(
    ("accepted_with_snapshot", "reason", "expected_error"),
    (
        (
            False,
            "HTTP 400: insufficient balance",
            "primary submission rejected: HTTP 400: insufficient balance",
        ),
        (
            False,
            (
                "pre-submission funds guard: POLYMARKET: "
                "insufficient collateral balance/allowance"
            ),
            (
                "primary submission rejected: pre-submission funds guard: "
                "POLYMARKET: insufficient collateral balance/allowance"
            ),
        ),
        (
            True,
            "settlement status UNMATCHED",
            "primary leg did not fill: settlement status UNMATCHED",
        ),
    ),
)
@pytest.mark.parametrize("source", ["submission", "get", "ws"])
def test_engine_persists_primary_rejection_reason(
    accepted_with_snapshot: bool,
    reason: str,
    expected_error: str,
    source: str,
) -> None:
    """Expose the venue rejection through the durable execution error."""
    left_venue, right_venue = VenueID("left"), VenueID("right")
    left = _contract("left-yes", left_venue, "yes")
    right = _contract("right-no", right_venue, "no")
    cycle = MarketCycle(Underlying("BTC"), 300)
    engine = TradingEngine(
        EventDispatcher(TradingState()),
        {left_venue: _ZeroFees(), right_venue: _ZeroFees()},
    )
    engine.enable(
        EngineConfig(
            max_notional_by_venue={
                left_venue: Decimal("10"),
                right_venue: Decimal("10"),
            },
            allowed_underlyings=("BTC",),
            allowed_intervals_seconds=(300,),
        ),
    )
    pair = MatchedContractPair(left, right, Timestamp.now())
    _process(engine, MarketMatchesUpdated(cycle, (pair,)))
    _process(engine, OrderBookUpdated(left_venue, left.id, _book(left, "0.50", "10")))
    planned = _process(
        engine,
        OrderBookUpdated(right_venue, right.id, _book(right, "0.40", "5")),
    )
    primary = next(
        event
        for event in planned
        if isinstance(event, SubmitOrder) and event.role == "primary"
    )
    reference = OrderReference(
        primary.venue_id,
        primary.intent.client_order_id,
        b"primary-recovery",
    )
    snapshot = (
        OrderSnapshot(
            status=OrderStatus.REJECTED,
            contract_id=primary.intent.contract_id,
            side=primary.intent.side,
            quantity=primary.intent.quantity,
            order_type=primary.intent.order_type,
            client_order_id=primary.intent.client_order_id,
            order_id=OrderID("primary-order"),
            limit_price=primary.intent.limit_price,
            reason=reason,
        )
        if accepted_with_snapshot
        else None
    )

    observation = SubmissionReceived(
        primary,
        SubmissionResult(
            (
                SubmissionStatus.ACCEPTED
                if accepted_with_snapshot
                else SubmissionStatus.REJECTED
            ),
            reference,
            snapshot=snapshot,
            reason=reason,
        ),
    )
    if snapshot is not None and source != "submission":
        _process(engine, SubmissionReceived(
            primary, SubmissionResult(SubmissionStatus.ACCEPTED, reference),
        ))
        observation = OrderSnapshotUpdated(
            primary.execution_id, primary.role, reference, snapshot, source,
        )
        replayed = decode_event(encode_event(observation))
        assert replayed == observation
        from prediction_markets.infrastructure.postgres.projector import _summary
        assert _summary(replayed)["reason"] == reason
        legacy = json.loads(encode_event(observation))
        del legacy["event"]["fields"]["snapshot"]["fields"]["reason"]
        assert decode_event(json.dumps(legacy).encode()).snapshot.reason is None
    rejected = _process(engine, observation)
    execution = next(
        event.execution for event in rejected if isinstance(event, ExecutionUpdated)
    )

    assert execution.last_error == expected_error
    if not accepted_with_snapshot:
        assert execution.status is ArbitrageExecutionStatus.NEEDS_REVIEW
        assert engine.state.trading_enabled is False
        assert engine.state.last_error == expected_error


@pytest.mark.parametrize(
    ("right_minimum", "expected_quantity"),
    (("5", Decimal("7.35")), ("8", None)),
)
def test_engine_sizes_near_budget_without_using_minimum_as_step(
    right_minimum: str,
    expected_quantity: Decimal | None,
) -> None:
    """Round to the quantity increment and enforce the separate venue minimum."""
    left_venue, right_venue = VenueID("left"), VenueID("right")
    left = _contract(
        "left-yes",
        left_venue,
        "yes",
        lot_size="0.000001",
        minimum_order_size="1",
    )
    right = _contract(
        "right-no",
        right_venue,
        "no",
        lot_size="0.01",
        minimum_order_size=right_minimum,
    )
    cycle = MarketCycle(Underlying("BTC"), 300)
    state = TradingState()
    engine = TradingEngine(
        EventDispatcher(state),
        {left_venue: _ZeroFees(), right_venue: _ZeroFees()},
    )
    engine.enable(
        EngineConfig(
            max_notional_by_venue={left_venue: Decimal("5"), right_venue: Decimal("5")},
            allowed_underlyings=("BTC",),
            allowed_intervals_seconds=(300,),
        ),
    )

    pair = MatchedContractPair(left, right, Timestamp.now())
    _process(engine, MarketMatchesUpdated(cycle, (pair,)))
    _process(engine, OrderBookUpdated(left_venue, left.id, _book(left, "0.245", "500")))
    events = _process(
        engine,
        OrderBookUpdated(right_venue, right.id, _book(right, "0.68", "500")),
    )

    plans = [event for event in events if isinstance(event, ArbitragePlanned)]
    if expected_quantity is None:
        assert plans == []
    else:
        assert plans[0].plan.quantity == Quantity(expected_quantity)
        assert all(leg.quantity == Quantity(expected_quantity) for leg in plans[0].plan.legs)


@pytest.mark.parametrize(
    ("polymarket_tick", "expected_quantity"),
    (("0.01", Decimal("7")), (None, None)),
)
def test_engine_sizes_market_buys_to_valid_polymarket_amounts(
    polymarket_tick: str | None,
    expected_quantity: Decimal | None,
) -> None:
    """Align both legs to Polymarket's cent-denominated market-buy amount."""
    limitless = VenueID("LIMITLESS")
    polymarket = VenueID("POLYMARKET")
    left = _contract(
        "limitless-yes",
        limitless,
        "yes",
        tick_size="0.001",
        lot_size="0.000001",
    )
    right = _contract(
        "polymarket-no",
        polymarket,
        "no",
        tick_size=polymarket_tick,
        lot_size="0.01",
        minimum_order_size="5",
    )
    cycle = MarketCycle(Underlying("BTC"), 300)
    engine = TradingEngine(
        EventDispatcher(TradingState()),
        {limitless: _ZeroFees(), polymarket: _ZeroFees()},
    )
    engine.enable(
        EngineConfig(
            max_notional_by_venue={
                limitless: Decimal("5"),
                polymarket: Decimal("5"),
            },
            market_buy_notional_steps={polymarket: Decimal("0.01")},
            allowed_underlyings=("BTC",),
            allowed_intervals_seconds=(300,),
        ),
    )

    _process(
        engine,
        MarketMatchesUpdated(
            cycle,
            (MatchedContractPair(left, right, Timestamp.now()),),
        ),
    )
    _process(engine, OrderBookUpdated(limitless, left.id, _book(left, "0.69", "500")))
    events = _process(
        engine,
        OrderBookUpdated(polymarket, right.id, _book(right, "0.24", "500")),
    )
    plans = [event for event in events if isinstance(event, ArbitragePlanned)]

    if expected_quantity is None:
        assert plans == []
    else:
        assert plans[0].plan.quantity == Quantity(expected_quantity)
        assert expected_quantity * Decimal("0.24") % Decimal("0.01") == 0


@pytest.mark.parametrize(
    ("detected_quantity", "expected_quantity"),
    (
        ("5", "5"),
        ("9", "9"),
        ("10", "7"),
        ("12", "8"),
        ("15", "10"),
    ),
)
def test_engine_keeps_headroom_for_large_detected_liquidity(
    detected_quantity: str,
    expected_quantity: str,
) -> None:
    """Apply the configured factor only at or above the large-order threshold."""
    left_venue, right_venue = VenueID("left"), VenueID("right")
    left = _contract("left-yes", left_venue, "yes")
    right = _contract("right-no", right_venue, "no")
    config = EngineConfig(
        max_notional_by_venue={
            left_venue: Decimal("100"),
            right_venue: Decimal("100"),
        },
    )

    adjusted = _risk_adjusted(
        left,
        right,
        left_price="0.40",
        right_price="0.50",
        quantity=detected_quantity,
        side=OrderSide.BUY,
        config=config,
    )

    assert adjusted is not None
    assert adjusted.quantity == Quantity(Decimal(expected_quantity))


def test_engine_rounds_large_order_headroom_down_to_common_lot_step() -> None:
    """Keep the reduced quantity on the common venue lot grid."""
    left_venue, right_venue = VenueID("left"), VenueID("right")
    left = _contract("left-yes", left_venue, "yes", lot_size="0.5")
    right = _contract("right-no", right_venue, "no", lot_size="0.5")
    config = EngineConfig(
        max_notional_by_venue={
            left_venue: Decimal("100"),
            right_venue: Decimal("100"),
        },
    )

    adjusted = _risk_adjusted(
        left,
        right,
        left_price="0.40",
        right_price="0.50",
        quantity="11",
        side=OrderSide.BUY,
        config=config,
    )

    assert adjusted is not None
    assert adjusted.quantity == Quantity(Decimal("7.5"))


@pytest.mark.parametrize("rejected_by", ("minimum_order_size", "min_notional"))
def test_engine_rejects_large_order_when_headroom_falls_below_minimum(
    rejected_by: str,
) -> None:
    """Reject a quantity that was valid before the large-order reduction."""
    left_venue, right_venue = VenueID("left"), VenueID("right")
    left = _contract("left-yes", left_venue, "yes")
    right = _contract(
        "right-no",
        right_venue,
        "no",
        minimum_order_size="8" if rejected_by == "minimum_order_size" else None,
    )
    config = EngineConfig(
        max_notional_by_venue={
            left_venue: Decimal("100"),
            right_venue: Decimal("100"),
        },
    )

    adjusted = _risk_adjusted(
        left,
        right,
        left_price="0.10" if rejected_by == "min_notional" else "0.40",
        right_price="0.80" if rejected_by == "min_notional" else "0.50",
        quantity="10",
        side=OrderSide.BUY,
        config=config,
    )

    assert adjusted is None


def test_engine_applies_predict_buy_slippage_to_planned_commands() -> None:
    """Journal the worse Predict BUY limit while preserving the Poly limit."""
    polymarket, predict = VenueID("POLYMARKET"), VenueID("PREDICT")
    left = _contract("poly-yes", polymarket, "yes")
    right = _contract("predict-no", predict, "no")
    pair = MatchedContractPair(left, right, Timestamp.now())
    cycle = MarketCycle(Underlying("BTC"), 300)
    engine = TradingEngine(
        EventDispatcher(TradingState()),
        {polymarket: _OnePercentFees(), predict: _OnePercentFees()},
    )
    engine.enable(
        EngineConfig(
            max_notional_by_venue={
                polymarket: Decimal("100"),
                predict: Decimal("100"),
            },
            allowed_underlyings=("BTC",),
            allowed_intervals_seconds=(300,),
        ),
    )
    _process(engine, MarketMatchesUpdated(cycle, (pair,)))
    _process(engine, OrderBookUpdated(polymarket, left.id, _book(left, "0.40", "5")))

    events = _process(
        engine,
        OrderBookUpdated(predict, right.id, _book(right, "0.50", "5")),
    )

    commands = {
        event.venue_id: event
        for event in events
        if isinstance(event, SubmitOrder)
    }
    planned = next(event for event in events if isinstance(event, ArbitragePlanned))
    assert commands[polymarket].intent.limit_price == Price(Decimal("0.40"))
    assert commands[predict].intent.limit_price == Price(Decimal("0.52"))
    assert planned.plan.net_edge == Decimal("0.0708")


def test_engine_applies_predict_sell_slippage() -> None:
    """Move only the Predict SELL limit down by the configured tick headroom."""
    polymarket, predict = VenueID("POLYMARKET"), VenueID("PREDICT")
    left = _contract("poly-yes", polymarket, "yes")
    right = _contract("predict-no", predict, "no")
    inventory = {
        left.id: Quantity(Decimal("5")),
        right.id: Quantity(Decimal("5")),
    }
    config = EngineConfig(
        max_notional_by_venue={
            polymarket: Decimal("100"),
            predict: Decimal("100"),
        },
        short_inventory_by_contract=inventory,
    )

    adjusted = _risk_adjusted(
        left,
        right,
        left_price="0.55",
        right_price="0.50",
        quantity="5",
        side=OrderSide.SELL,
        config=config,
    )

    assert adjusted is not None
    assert adjusted.left_level.price == Price(Decimal("0.55"))
    assert adjusted.right_level.price == Price(Decimal("0.48"))
    assert adjusted.gross_edge == Decimal("0.03")


def test_engine_rejects_edge_consumed_by_predict_slippage() -> None:
    """Reject an apparent top-of-book edge consumed by the worse Predict limit."""
    polymarket, predict = VenueID("POLYMARKET"), VenueID("PREDICT")
    left = _contract("poly-yes", polymarket, "yes")
    right = _contract("predict-no", predict, "no")
    config = EngineConfig(
        max_notional_by_venue={
            polymarket: Decimal("100"),
            predict: Decimal("100"),
        },
    )

    adjusted = _risk_adjusted(
        left,
        right,
        left_price="0.48",
        right_price="0.50",
        quantity="5",
        side=OrderSide.BUY,
        config=config,
    )

    assert adjusted is None


def test_engine_reapplies_predict_budget_after_buy_slippage() -> None:
    """Reduce quantity when the worse Predict BUY limit exceeds its budget."""
    polymarket, predict = VenueID("POLYMARKET"), VenueID("PREDICT")
    left = _contract("poly-yes", polymarket, "yes")
    right = _contract("predict-no", predict, "no")
    config = EngineConfig(
        max_notional_by_venue={
            polymarket: Decimal("100"),
            predict: Decimal("5.1"),
        },
        large_order_contract_threshold=100,
    )

    adjusted = _risk_adjusted(
        left,
        right,
        left_price="0.40",
        right_price="0.50",
        quantity="10",
        side=OrderSide.BUY,
        config=config,
    )

    assert adjusted is not None
    assert adjusted.right_level.price == Price(Decimal("0.52"))
    assert adjusted.quantity == Quantity(Decimal("9"))


def test_engine_can_disable_predict_limit_slippage() -> None:
    """Preserve the detected Predict limit when the configured ticks are zero."""
    polymarket, predict = VenueID("POLYMARKET"), VenueID("PREDICT")
    left = _contract("poly-yes", polymarket, "yes")
    right = _contract("predict-no", predict, "no")
    config = EngineConfig(
        max_notional_by_venue={
            polymarket: Decimal("100"),
            predict: Decimal("100"),
        },
        predict_limit_slippage_ticks=0,
    )

    adjusted = _risk_adjusted(
        left,
        right,
        left_price="0.40",
        right_price="0.50",
        quantity="5",
        side=OrderSide.BUY,
        config=config,
    )

    assert adjusted is not None
    assert adjusted.right_level.price == Price(Decimal("0.50"))
    assert adjusted.gross_edge == Decimal("0.10")


@pytest.mark.parametrize(
    ("side", "left_price", "predict_price", "expected_limit"),
    (
        (OrderSide.BUY, "0.005", "0.99", "0.99"),
        (OrderSide.SELL, "0.995", "0.01", "0.01"),
    ),
)
def test_engine_clamps_predict_slippage_to_tradable_price_range(
    side: OrderSide,
    left_price: str,
    predict_price: str,
    expected_limit: str,
) -> None:
    """Keep the Predict limit between one tick and one minus one tick."""
    polymarket, predict = VenueID("POLYMARKET"), VenueID("PREDICT")
    left = _contract("poly-yes", polymarket, "yes", tick_size="0.001")
    right = _contract("predict-no", predict, "no")
    inventory = (
        {
            left.id: Quantity(Decimal("5")),
            right.id: Quantity(Decimal("5")),
        }
        if side is OrderSide.SELL
        else {}
    )
    config = EngineConfig(
        max_notional_by_venue={
            polymarket: Decimal("100"),
            predict: Decimal("100"),
        },
        min_notional_per_venue=Decimal("0"),
        short_inventory_by_contract=inventory,
    )

    adjusted = _risk_adjusted(
        left,
        right,
        left_price=left_price,
        right_price=predict_price,
        quantity="5",
        side=side,
        config=config,
    )

    assert adjusted is not None
    assert adjusted.right_level.price == Price(Decimal(expected_limit))


@pytest.mark.parametrize(
    ("overrides", "message"),
    (
        ({"large_order_contract_threshold": 0}, "threshold must be positive"),
        (
            {"large_order_liquidity_safety_factor": Decimal("0")},
            "safety factor must be in",
        ),
        (
            {"large_order_liquidity_safety_factor": Decimal("1.1")},
            "safety factor must be in",
        ),
        ({"predict_limit_slippage_ticks": -1}, "ticks must be non-negative"),
        (
            {"max_concurrent_arbitrages": 0},
            "max_concurrent_arbitrages must be positive",
        ),
    ),
)
def test_engine_config_validates_execution_headroom(
    overrides: dict[str, object],
    message: str,
) -> None:
    """Reject invalid quantity and Predict price-headroom settings."""
    with pytest.raises(ValueError, match=message):
        EngineConfig(
            max_notional_by_venue={VenueID("left"): Decimal("10")},
            **overrides,
        )
