"""Verify runtime preparation for selected covered-short markets."""

import asyncio
import json
from dataclasses import replace
from datetime import timedelta
from decimal import Decimal

import pytest

from prediction_markets.api.runtime import ArbitrageRuntime
from prediction_markets.api.runtime.inventory import (
    ShortInventoryCoordinator,
    _accounted_short_quantity,
    _manual_inventory_snapshot,
    _manual_split_snapshot,
    _legacy_short_position_repairs,
    _short_inventory_expiry_error,
)
from prediction_markets.api.runtime.safety import RuntimeSafety, _VenueSafetyStop
from prediction_markets.api.trading.runner import LiveArbitrageConfig
from prediction_markets.application.engine import EngineConfig, TradingEngine
from prediction_markets.application.events import (
    ArbitragePlanned,
    ExecutionUpdated,
    MarketMatchesUpdated,
    OrderBookUpdated,
    PositionUpdated,
    SubmitOrder,
)
from prediction_markets.application.codec import decode_event
from prediction_markets.application.markets.models import MarketCycle
from prediction_markets.application.state import EventDispatcher, TradingState
from prediction_markets.application.venue_health import VenueHealthReport
from prediction_markets.domain.contracts.entities import BinaryContract
from prediction_markets.domain.market_matching.value_objects import (
    MatchedContractPair,
    RegularCandidate,
    Underlying,
)
from prediction_markets.domain.markets.entities import Market, MarketSide
from prediction_markets.domain.markets.enums import BinaryOutcome, MarketStatus
from prediction_markets.domain.markets.value_objects import MarketState
from prediction_markets.domain.outcome_inventory import (
    InventoryOperationID,
    InventoryOperationReference,
    InventoryOperationSnapshot,
    InventoryOperationStatus,
    OutcomeInventoryAction,
    OutcomeInventoryBalance,
)
from prediction_markets.domain.shared.value_objects import (
    ContractID,
    Currency,
    MarketID,
    Money,
    OutcomeID,
    PortfolioID,
    PositionID,
    Price,
    Quantity,
    Timestamp,
    VenueID,
)
from prediction_markets.domain.trading.entities import Position
from prediction_markets.domain.trading.enums import ArbitrageExecutionStatus, PositionSide
from prediction_markets.domain.venue_health import (
    VenueHealthSnapshot,
    VenueHealthStatus,
)
from prediction_markets.infrastructure.venues.predict.mappers import PREDICT_VENUE_ID
from prediction_markets.infrastructure.binary_journal import BinaryJournal


def _contract(name: str, venue_id: str) -> BinaryContract:
    return BinaryContract(
        id=ContractID(name),
        market_id=MarketID(f"{name}-market"),
        outcome_id=OutcomeID(name),
        venue_id=VenueID(venue_id),
        payout_currency=Currency("USD"),
    )


class _Inventory:
    def __init__(
        self,
        quantities: dict[VenueID, Decimal],
        contract_ids: dict[VenueID, ContractID] | None = None,
    ) -> None:
        self.calls = []
        self.reconciliations = []
        self.quantities = quantities
        self.contract_ids = contract_ids or {}

    async def reconcile_pending(self, references):
        """Track recovery before preparation without calling a venue."""
        self.reconciliations.append(references)

    async def ensure_short_inventory(self, markets):
        self.calls.append(dict(markets))
        return tuple(
            OutcomeInventoryBalance(
                venue_id=venue_id,
                market_id=market_id,
                yes=Quantity(self.quantities[venue_id]),
                no=Quantity(self.quantities[venue_id]),
                collateral=Money(Decimal("20"), Currency("USD")),
                observed_at=Timestamp.now(),
                yes_contract_id=self.contract_ids.get(venue_id),
            )
            for venue_id, market_id in markets.items()
        )


class _Collateral:
    """Return a mutable venue collateral observation for runtime tests."""

    def __init__(self, available: Decimal) -> None:
        self.available = available
        self.calls: list[tuple[ContractID, ...]] = []

    def get_available_collateral(
        self,
        contract_ids: tuple[ContractID, ...],
    ) -> Decimal:
        self.calls.append(contract_ids)
        return self.available


def _inventory_coordinator(
    state: TradingState,
    inventory: _Inventory,
    *,
    engine: TradingEngine | None = None,
    execution: dict[VenueID, object] | None = None,
) -> ShortInventoryCoordinator:
    """Build the concrete inventory collaborator with test-owned dependencies."""
    dispatcher = EventDispatcher(state)
    trading_engine = engine or TradingEngine(dispatcher, {})
    safety = RuntimeSafety(
        None,
        state,
        trading_engine,
        object(),  # type: ignore[arg-type]
        execution or {},  # type: ignore[arg-type]
    )
    coordinator = ShortInventoryCoordinator(
        state,
        trading_engine,
        object(),  # type: ignore[arg-type]
        dispatcher,
        [],
    )
    coordinator._service = inventory  # type: ignore[assignment]
    return coordinator


def _runtime_safety(
    state: TradingState,
    engine: TradingEngine,
    execution: dict[VenueID, object],
    *,
    health: object | None = None,
) -> RuntimeSafety:
    """Build the concrete safety collaborator without an event pipeline."""
    return RuntimeSafety(
        health,  # type: ignore[arg-type]
        state,
        engine,
        object(),  # type: ignore[arg-type]
        execution,  # type: ignore[arg-type]
    )


def test_repairs_pending_predict_inventory_with_proven_hash() -> None:
    """Persist a terminal receipt through the existing inventory service."""
    state = TradingState()
    operation_id = InventoryOperationID("split-1")
    reference = InventoryOperationReference(
        venue_id=PREDICT_VENUE_ID,
        operation_id=operation_id,
        recovery_data=b'{"schema":1,"transaction_hash":null}',
        quantity=Quantity(Decimal("5")),
        action=OutcomeInventoryAction.SPLIT,
        portfolio_id=PortfolioID("portfolio"),
    )
    state.pending_inventory_operations[operation_id] = reference
    transaction_hash = "0x" + "ab" * 32

    class Inventory(_Inventory):
        async def reconcile_pending(self, references):
            repaired = references[0]
            assert json.loads(repaired.recovery_data)["transaction_hash"] == transaction_hash
            return (
                InventoryOperationSnapshot(
                    reference=repaired,
                    status=InventoryOperationStatus.CONFIRMED,
                    updated_at=Timestamp.now(),
                    transaction_id=transaction_hash,
                    quantity=Quantity(Decimal("5")),
                ),
            )

    inventory = Inventory({})
    coordinator = _inventory_coordinator(state, inventory)

    snapshot = asyncio.run(
        coordinator.reconcile_predict_transaction(operation_id.value, transaction_hash),
    )

    assert snapshot.status is InventoryOperationStatus.CONFIRMED
    assert snapshot.transaction_id == transaction_hash


def test_prepares_exact_current_markets_and_contract_pair() -> None:
    """Resolve one monitor key and retain only its current covered pair."""
    left = _contract("left", "left-venue")
    right = _contract("right", "right-venue")
    pair = MatchedContractPair(left, right, Timestamp.now())
    cycle = MarketCycle(Underlying("BTC"), 3600)
    state = TradingState()
    state.apply(MarketMatchesUpdated(cycle, (pair,)))
    inventory = _Inventory(
        {
            left.venue_id: Decimal("8"),
            right.venue_id: Decimal("11"),
        },
    )
    coordinator = _inventory_coordinator(state, inventory)

    pair_keys, inventory_by_contract = asyncio.run(
        coordinator.prepare(("cycle:BTC:3600",)),
    )

    assert inventory.calls == [
        {
            left.venue_id: left.market_id,
            right.venue_id: right.market_id,
        },
    ]
    assert pair_keys == frozenset({pair.key})
    assert inventory_by_contract == {
        left.id: Quantity(Decimal("8")),
        right.id: Quantity(Decimal("11")),
    }
    assert coordinator.prepared_market_keys == ("cycle:BTC:3600",)


def test_rejects_short_inventory_inside_cycle_expiry_guard() -> None:
    """Avoid signing inventory transactions for a cycle about to expire."""
    left = _contract("left", "left-venue")
    right = _contract("right", "right-venue")
    pair = MatchedContractPair(
        left,
        right,
        Timestamp(Timestamp.now().value + timedelta(seconds=30)),
    )
    cycle = MarketCycle(Underlying("BTC"), 3600)
    state = TradingState()
    state.apply(MarketMatchesUpdated(cycle, (pair,)))
    inventory = _Inventory(
        {left.venue_id: Decimal("5"), right.venue_id: Decimal("5")},
    )
    coordinator = _inventory_coordinator(state, inventory)

    with pytest.raises(RuntimeError, match="inside its expiry guard"):
        asyncio.run(
            coordinator.prepare(
                ("cycle:BTC:3600",),
                minimum_time_remaining_seconds=120,
            ),
        )

    assert inventory.calls == []


def test_regular_short_inventory_ignores_event_date_expiry() -> None:
    """Let live regular markets trade after their scheduled event date."""
    left = _contract("left", "left-venue")
    right = _contract("right", "right-venue")
    closed_at = Timestamp(Timestamp.now().value - timedelta(days=1))
    candidate = RegularCandidate(
        tuple(
            Market(
                id=contract.market_id,
                venue_id=contract.venue_id,
                title="Unresolved regular market",
                state=MarketState(MarketStatus.ACTIVE, close_time=closed_at),
                yes_side=MarketSide(contract.outcome_id, BinaryOutcome.YES),
                no_side=MarketSide(OutcomeID(f"{contract.id}-no"), BinaryOutcome.NO),
            )
            for contract in (left, right)
        ),
    )
    pair = MatchedContractPair(left, right, closed_at)

    assert _short_inventory_expiry_error("regular:test", candidate, (pair,), 120) is None


@pytest.mark.parametrize("action", ["prepare", "redeem"])
def test_unresolved_inventory_blocks_new_runtime_operations(monkeypatch, action) -> None:
    """Reconcile persisted references before activation, rollover, or redemption."""
    left, right = _contract("left", "LIMITLESS"), _contract("right", "POLYMARKET")
    pair = MatchedContractPair(left, right, Timestamp.now())
    cycle = MarketCycle(Underlying("BTC"), 3600)
    state = TradingState(trading_enabled=True)
    state.apply(MarketMatchesUpdated(cycle, (pair,)))
    reference = InventoryOperationReference(
        left.venue_id, InventoryOperationID("pending-split"), b"durable-reference",
    )
    state.pending_inventory_operations[reference.operation_id] = reference
    inventory = _Inventory({left.venue_id: Decimal("5"), right.venue_id: Decimal("5")})
    coordinator = _inventory_coordinator(state, inventory)
    coordinator._prepared_short_market_keys = ("cycle:BTC:3600",)

    async def unresolved(references):
        assert references == (reference,)
        raise RuntimeError("still pending")

    monkeypatch.setattr(inventory, "reconcile_pending", unresolved)
    with pytest.raises(RuntimeError, match="still pending"):
        asyncio.run(
            coordinator.prepare(("cycle:BTC:3600",)) if action == "prepare"
            else coordinator.redeem_limitless_available()
        )
    assert not inventory.calls
    assert not coordinator.preparing


def test_redemption_waits_for_short_preparation(monkeypatch) -> None:
    """Keep automatic redemption out of a concurrent split's balance window."""
    async def run():
        left, right = _contract("left", "LIMITLESS"), _contract("right", "POLYMARKET")
        cycle = MarketCycle(Underlying("BTC"), 3600)
        state = TradingState()
        state.apply(MarketMatchesUpdated(cycle, (MatchedContractPair(left, right, Timestamp.now()),)))
        inventory = _Inventory({left.venue_id: Decimal("5"), right.venue_id: Decimal("5")})
        coordinator = _inventory_coordinator(state, inventory)
        entered, finish = asyncio.Event(), asyncio.Event()
        ensure = inventory.ensure_short_inventory
        redeemed = []

        async def slow_prepare(markets):
            entered.set()
            await finish.wait()
            return await ensure(markets)

        async def redeem(venue):
            redeemed.append(venue)
            return ()

        monkeypatch.setattr(inventory, "ensure_short_inventory", slow_prepare)
        monkeypatch.setattr(inventory, "redeem_available", redeem, raising=False)
        preparation = asyncio.create_task(coordinator.prepare(("cycle:BTC:3600",)))
        await entered.wait()
        redemption = asyncio.create_task(coordinator.redeem_limitless_available())
        await asyncio.sleep(0)
        assert not redeemed
        finish.set()
        await asyncio.gather(preparation, redemption)
        assert redeemed == [left.venue_id]

    asyncio.run(run())


def test_rejects_physical_short_inventory_owned_by_legacy_portfolio() -> None:
    """Do not sell tokens whose WAC basis remains in the legacy portfolio."""
    left = _contract("left", "left-venue")
    right = _contract("right", "right-venue")
    pair = MatchedContractPair(left, right, Timestamp.now())
    cycle = MarketCycle(Underlying("BTC"), 3600)
    state = TradingState()
    state.apply(MarketMatchesUpdated(cycle, (pair,)))
    state.positions[PositionID("left-venue:default:left")] = Position(
        id=PositionID("left-venue:default:left"),
        contract_id=left.id,
        portfolio_id=PortfolioID("default"),
        quantity=Quantity(Decimal("5")),
        side=PositionSide.LONG,
        venue_id=left.venue_id,
        average_price=Price(Decimal("0.5")),
    )
    coordinator = _inventory_coordinator(
        state,
        _Inventory(
            {left.venue_id: Decimal("5"), right.venue_id: Decimal("5")},
            {left.venue_id: left.id, right.venue_id: right.id},
        ),
    )

    with pytest.raises(RuntimeError, match="not accounted in portfolio"):
        asyncio.run(coordinator.prepare(("cycle:BTC:3600",)))


@pytest.mark.parametrize(("physical", "accounted", "expected"), [
    ("30", "29.879455951881003022", "29.879455951881003022"),
    ("20", "30", "20"),
    ("0", "30", "0"),
    ("30", "0", None),
])
def test_short_inventory_caps_the_selected_token_without_complete_sets(
    physical: str, accounted: str, expected: str | None,
) -> None:
    """Cap sales by wallet and strategy ownership even with no opposite tokens."""
    contract = _contract("selected-yes", "PREDICT")
    portfolio_id = PortfolioID("cross-venue-arbitrage")
    state = TradingState()
    position_id = PositionID(f"{contract.venue_id}:{portfolio_id}:{contract.id}")
    if Decimal(accounted) > 0:
        state.positions[position_id] = Position(
            id=position_id, contract_id=contract.id, portfolio_id=portfolio_id,
            venue_id=contract.venue_id, side=PositionSide.LONG,
            quantity=Quantity(Decimal(accounted)), average_price=Price(Decimal("0.5")),
        )
    balance = OutcomeInventoryBalance(
        venue_id=contract.venue_id, market_id=contract.market_id,
        yes=Quantity(Decimal(physical)), no=Quantity(Decimal("0")),
        collateral=Money(Decimal("0"), Currency("USD")), observed_at=Timestamp.now(),
        yes_contract_id=contract.id, no_contract_id=ContractID("opposite-no"),
    )

    if expected is None:
        with pytest.raises(RuntimeError, match="not accounted in portfolio"):
            _accounted_short_quantity(state, portfolio_id, contract, balance)
    else:
        assert _accounted_short_quantity(state, portfolio_id, contract, balance) == Quantity(Decimal(expected))


def test_asymmetric_inventory_allows_another_sale_after_twelve_tokens_fill() -> None:
    """Reproduce Bolsonaro balances through preparation and two SELL admissions."""
    from tests.application.test_engine import _ZeroFees, _book, _contract as engine_contract, _process

    left = engine_contract("poly-no", VenueID("POLYMARKET"), "no", tick_size="0.001", minimum_order_size="5")
    right = engine_contract("predict-yes", VenueID("PREDICT"), "yes", tick_size="0.001")
    pair = MatchedContractPair(left, right, Timestamp.now())
    cycle = MarketCycle(Underlying("BTC"), 3600)
    state = TradingState()
    state.apply(MarketMatchesUpdated(cycle, (pair,)))
    balances = []
    for contract, yes, no, selected_is_yes in (
        (left, "13", "42", False),
        (right, "41.879455951881003022", "13", True),
    ):
        balance = OutcomeInventoryBalance(
            venue_id=contract.venue_id, market_id=contract.market_id,
            yes=Quantity(Decimal(yes)), no=Quantity(Decimal(no)),
            collateral=Money(Decimal("0"), Currency("USD")), observed_at=Timestamp.now(),
            yes_contract_id=contract.id if selected_is_yes else ContractID("poly-yes"),
            no_contract_id=ContractID("predict-no") if selected_is_yes else contract.id,
        )
        balances.append(balance)
        portfolio_id = PortfolioID("cross-venue-arbitrage")
        position_id = PositionID(f"{contract.venue_id}:{portfolio_id}:{contract.id}")
        state.positions[position_id] = Position(
            id=position_id, contract_id=contract.id, venue_id=contract.venue_id,
            portfolio_id=portfolio_id, side=PositionSide.LONG,
            quantity=balance.yes if selected_is_yes else balance.no,
            average_price=Price(Decimal("0.5")),
        )

    class Inventory(_Inventory):
        """Return the recorded asymmetric balances without venue I/O."""

        async def ensure_short_inventory(self, markets):
            return tuple(balances)

    coordinator = _inventory_coordinator(state, Inventory({}))
    pair_keys, inventory = asyncio.run(coordinator.prepare(("cycle:BTC:3600",)))
    assert inventory[left.id] == Quantity(Decimal("42"))
    assert inventory[right.id] == Quantity(Decimal("41.879455951881003022"))

    engine = TradingEngine(EventDispatcher(state), {left.venue_id: _ZeroFees(), right.venue_id: _ZeroFees()})
    engine.enable(EngineConfig(
        max_notional_by_venue={left.venue_id: Decimal("10"), right.venue_id: Decimal("10")},
        max_arbitrages=3, execute_long=False, execute_short=True,
        short_market_keys=frozenset({"cycle:BTC:3600"}), short_pair_keys=pair_keys,
        short_inventory_by_contract=inventory, allowed_intervals_seconds=(3600,),
        predict_limit_slippage_ticks=0, min_net_edge=Decimal("0.003"), max_skew_ms=1000,
    ))
    _process(engine, OrderBookUpdated(left.venue_id, left.id, _book(left, "0.22", "240", bid=True)))
    first = _process(engine, OrderBookUpdated(right.venue_id, right.id, _book(right, "0.788", "240", bid=True)))
    execution = next(event.execution for event in first if isinstance(event, ArbitragePlanned))
    assert execution.leg1_quantity == execution.leg2_quantity == Quantity(Decimal("12"))
    _process(engine, ExecutionUpdated(replace(
        execution, status=ArbitrageExecutionStatus.COMPLETED,
        leg1_filled_quantity=execution.leg1_quantity, leg2_filled_quantity=execution.leg2_quantity,
    )))

    second = _process(engine, OrderBookUpdated(right.venue_id, right.id, _book(right, "0.789", "240", bid=True)))
    commands = [event for event in second if isinstance(event, SubmitOrder)]
    assert len(commands) == 2
    assert all(command.intent.quantity == Quantity(Decimal("12")) for command in commands)


def test_manual_splits_preserve_basis_and_pnl_across_replay(tmp_path, monkeypatch) -> None:
    """Import a manual top-up once without submitting or changing realized profit."""
    left, right = _contract("left", "PREDICT"), _contract("right", "POLYMARKET")
    state = TradingState()
    matched = MarketMatchesUpdated(
        MarketCycle(Underlying("BTC"), 3600),
        (MatchedContractPair(left, right, Timestamp.now()),),
    )
    state.apply(matched)
    inventory = _Inventory({left.venue_id: Decimal("30"), right.venue_id: Decimal("30")})
    coordinator = _inventory_coordinator(state, inventory)
    portfolio = coordinator.portfolio_id()
    position = Position(
        id=PositionID(f"{left.venue_id}:{portfolio}:{left.id}"),
        contract_id=left.id, venue_id=left.venue_id, portfolio_id=portfolio,
        side=PositionSide.LONG, quantity=Quantity(Decimal("15")),
        average_price=Price(Decimal("0.5")), realized_pnl=Decimal("1.25"),
        fees=Money(Decimal("0.02"), Currency("USD")),
    )
    state.positions[position.id] = position
    invalid_second_balance = True

    async def balances(markets):
        return tuple(
            OutcomeInventoryBalance(
                venue_id=contract.venue_id, market_id=contract.market_id,
                yes=Quantity(Decimal("45") if contract == left else Decimal("30")),
                no=Quantity(Decimal("29") if invalid_second_balance and contract == right else Decimal("30")),
                collateral=Money(Decimal("10"), Currency("USDT")),
                observed_at=Timestamp.now(), yes_contract_id=contract.id,
                no_contract_id=ContractID(f"{contract.id}-no"),
            ) for contract in (left, right)
        )

    monkeypatch.setattr(inventory, "get_balances", balances, raising=False)
    path = tmp_path / "manual-splits.log"
    journal = BinaryJournal(path)
    coordinator.journal = journal
    journal.append(matched)
    journal.append(PositionUpdated(position))
    try:
        with pytest.raises(RuntimeError, match="unaccounted YES=30, NO=29"):
            asyncio.run(coordinator.import_manual_splits(("cycle:BTC:3600",)))
        assert journal.last_sequence == 2
        assert state.positions == {position.id: position}
        invalid_second_balance = False

        async def concurrent_imports():
            return await asyncio.gather(
                coordinator.import_manual_splits(("cycle:BTC:3600",)),
                coordinator.import_manual_splits(("cycle:BTC:3600",)),
            )

        first, duplicate = asyncio.run(concurrent_imports())
        assert duplicate == ()
        assert [item.quantity.value for item in first] == [Decimal("30"), Decimal("30")]
        assert state.positions[position.id].quantity.value == Decimal("45")
        assert state.positions[position.id].average_price == Price(Decimal("0.5"))
        assert state.positions[position.id].realized_pnl == Decimal("1.25")
        assert state.positions[position.id].fees == position.fees
        assert not inventory.calls  # No ensure/prepare/submit operation.
        sequence = journal.last_sequence
        assert asyncio.run(coordinator.import_manual_splits(("cycle:BTC:3600",))) == ()
        assert journal.last_sequence == sequence
    finally:
        journal.close()

    reopened = BinaryJournal(path)
    try:
        replayed = TradingState()
        for entry in reopened.entries():
            replayed.apply(decode_event(entry.payload))
        assert replayed.positions == state.positions
        assert replayed.applied_inventory_operations == state.applied_inventory_operations
        restarted = _inventory_coordinator(replayed, inventory)
        restarted.journal = reopened
        assert asyncio.run(restarted.import_manual_splits(("cycle:BTC:3600",))) == ()
    finally:
        reopened.close()


@pytest.mark.parametrize("other_quantity, surplus_no, rejected", [
    ("5", "30", False), ("5", "29", True), ("40", "30", True), ("-5", "30", True),
])
def test_manual_splits_exclude_other_portfolios_and_reject_discrepancies(
    other_quantity, surplus_no, rejected,
) -> None:
    """Never adopt tracked holdings or hide external sales and short positions."""
    venue, target = VenueID("PREDICT"), PortfolioID("cross-venue-arbitrage")
    state = TradingState()
    quantity = Decimal(other_quantity)
    for name in ("yes", "no"):
        identity = PositionID(f"{venue}:other:{name}")
        state.positions[identity] = Position(
            id=identity, contract_id=ContractID(name), venue_id=venue,
            portfolio_id=PortfolioID("other"),
            side=PositionSide.LONG if quantity > 0 else PositionSide.SHORT,
            quantity=Quantity(abs(quantity)), average_price=Price(Decimal("0.5")),
        )
    balance = OutcomeInventoryBalance(
        venue, MarketID("market"), Quantity(Decimal("35")),
        Quantity(Decimal(surplus_no) + 5), Money(Decimal("10"), Currency("USDT")),
        Timestamp.now(), ContractID("yes"), ContractID("no"),
    )
    if rejected:
        with pytest.raises(RuntimeError):
            _manual_split_snapshot(state, balance, target)
    else:
        snapshot = _manual_split_snapshot(state, balance, target)
        assert snapshot.quantity.value == Decimal("30")
        assert snapshot.reference.portfolio_id == target


def test_manual_splits_and_merges_reconcile_once_and_replay(tmp_path, monkeypatch) -> None:
    """Preserve WAC and cash proceeds for mixed imports without submitting transactions."""
    left, right = _contract("split", "POLYMARKET"), _contract("merge", "PREDICT")
    state = TradingState()
    matched = MarketMatchesUpdated(
        MarketCycle(Underlying("BTC"), 3600),
        (MatchedContractPair(left, right, Timestamp.now()),),
    )
    state.apply(matched)
    inventory = _Inventory({})
    coordinator = _inventory_coordinator(state, inventory)
    portfolio = coordinator.portfolio_id()
    opening = []
    for contract, yes, no in ((left, "17", "0"), (right, "74", "35")):
        for contract_id, quantity, price in (
            (contract.id, yes, "0.2"), (ContractID(f"{contract.id}-no"), no, "0.5"),
        ):
            position = Position(
                id=PositionID(f"{contract.venue_id}:{portfolio}:{contract_id}"),
                contract_id=contract_id, venue_id=contract.venue_id, portfolio_id=portfolio,
                side=PositionSide.LONG if Decimal(quantity) else PositionSide.FLAT,
                quantity=Quantity(Decimal(quantity)),
                average_price=Price(Decimal(price)) if Decimal(quantity) else None,
                realized_pnl=Decimal("1.25"), fees=Money(Decimal("0.02"), Currency("USD")),
            )
            opening.append(PositionUpdated(position))
            state.apply(opening[-1])

    async def balances(markets):
        return tuple(
            OutcomeInventoryBalance(
                contract.venue_id, contract.market_id,
                Quantity(Decimal(yes)), Quantity(Decimal(no)),
                Money(Decimal("25"), Currency("USD")), Timestamp.now(),
                contract.id, ContractID(f"{contract.id}-no"),
            ) for contract, yes, no in ((left, "37", "20"), (right, "39", "0"))
        )

    monkeypatch.setattr(inventory, "get_balances", balances, raising=False)
    path = tmp_path / "manual-operations.log"
    journal = BinaryJournal(path)
    coordinator.journal = journal
    for event in (matched, *opening):
        journal.append(event)
    try:
        sequence = journal.last_sequence
        with pytest.raises(RuntimeError, match="unaccounted YES=-35, NO=-35"):
            asyncio.run(coordinator.import_manual_splits(("cycle:BTC:3600",)))
        assert journal.last_sequence == sequence

        async def concurrent_imports():
            return await asyncio.gather(
                coordinator.import_manual_inventory(("cycle:BTC:3600",), fee_free=True),
                coordinator.import_manual_inventory(("cycle:BTC:3600",), fee_free=True),
            )

        imported, duplicate = asyncio.run(concurrent_imports())
        assert duplicate == ()
        assert [(item.reference.action, item.quantity.value) for item in imported] == [
            (OutcomeInventoryAction.SPLIT, Decimal("20")),
            (OutcomeInventoryAction.MERGE, Decimal("35")),
        ]
        split = state.positions[opening[0].position.id]
        merged = state.positions[opening[2].position.id]
        assert split.quantity.value == Decimal("37")
        assert split.average_price.value == Decimal("13.4") / 37
        assert split.realized_pnl == Decimal("1.25")
        assert merged.quantity.value == Decimal("39")
        assert merged.average_price.value == Decimal("0.2")
        assert merged.realized_pnl == Decimal("11.75")
        assert state.positions[opening[3].position.id].side is PositionSide.FLAT
        assert all(position.fees.amount == Decimal("0.02") for position in state.positions.values())
        assert not inventory.calls
    finally:
        journal.close()

    reopened = BinaryJournal(path)
    try:
        replayed = TradingState()
        for entry in reopened.entries():
            replayed.apply(decode_event(entry.payload))
        assert replayed.positions == state.positions
        restarted = _inventory_coordinator(replayed, inventory)
        restarted.journal = reopened
        sequence = reopened.last_sequence
        assert asyncio.run(restarted.import_manual_inventory(("cycle:BTC:3600",))) == ()
        assert reopened.last_sequence == sequence
    finally:
        reopened.close()


def test_manual_merge_cannot_consume_another_portfolios_inventory() -> None:
    """Reject a deficit that cannot be covered by the selected portfolio alone."""
    venue, target = VenueID("PREDICT"), PortfolioID("cross-venue-arbitrage")
    state = TradingState()
    for portfolio, quantity in ((target, "5"), (PortfolioID("other"), "10")):
        for contract in (ContractID("yes"), ContractID("no")):
            position = Position(
                id=PositionID(f"{venue}:{portfolio}:{contract}"),
                contract_id=contract, venue_id=venue, portfolio_id=portfolio,
                side=PositionSide.LONG, quantity=Quantity(Decimal(quantity)),
                average_price=Price(Decimal("0.5")),
            )
            state.apply(PositionUpdated(position))
    balance = OutcomeInventoryBalance(
        venue, MarketID("market"), Quantity(Decimal("8")), Quantity(Decimal("8")),
        Money(Decimal("0"), Currency("USDT")), Timestamp.now(),
        ContractID("yes"), ContractID("no"),
    )
    with pytest.raises(RuntimeError, match="Manual merge exceeds accounted inventory"):
        _manual_inventory_snapshot(state, balance, target)


def test_manual_inventory_does_not_assume_unverified_fees_are_zero() -> None:
    """Keep missing gas visible instead of declaring an external operation free."""
    balance = OutcomeInventoryBalance(
        VenueID("PREDICT"), MarketID("market"), Quantity(Decimal("20")),
        Quantity(Decimal("20")), Money(Decimal("25"), Currency("USDT")),
        Timestamp.now(), ContractID("yes"), ContractID("no"),
    )
    snapshot = _manual_inventory_snapshot(
        TradingState(), balance, PortfolioID("cross-venue-arbitrage"),
    )
    assert snapshot.fee is None
    assert snapshot.fee_settlement_cost is None
    assert snapshot.fee_observed_at is None
    assert snapshot.quality_flags == ("MISSING_FEES",)


@pytest.mark.parametrize("busy", ["trading", "pending", "execution"])
@pytest.mark.parametrize("method", ["import_manual_splits", "import_manual_inventory"])
def test_manual_import_requires_idle_runtime(monkeypatch, busy, method) -> None:
    """Reject moving balances before any venue call or journal write."""
    state = TradingState(trading_enabled=busy == "trading")
    inventory = _Inventory({})
    coordinator = _inventory_coordinator(state, inventory)
    if busy == "pending":
        reference = InventoryOperationReference(VenueID("PREDICT"), InventoryOperationID("pending"), b"request")
        state.pending_inventory_operations[reference.operation_id] = reference
    if busy == "execution":
        monkeypatch.setattr(coordinator.engine, "active_execution_count", lambda: 1)
    with pytest.raises(RuntimeError, match="Stop trading"):
        asyncio.run(getattr(coordinator, method)(("cycle:BTC:3600",)))


def test_repairs_exact_legacy_long_and_strategy_short_books() -> None:
    """Move covered-short realized PnL into the strategy book once."""
    contract = _contract("contract", "venue")
    state = TradingState()
    state.positions[PositionID("venue:default:contract")] = Position(
        id=PositionID("venue:default:contract"),
        contract_id=contract.id,
        portfolio_id=PortfolioID("default"),
        quantity=Quantity(Decimal("5")),
        side=PositionSide.LONG,
        venue_id=contract.venue_id,
        average_price=Price(Decimal("0.5")),
    )
    state.positions[PositionID("venue:strategy:contract")] = Position(
        id=PositionID("venue:strategy:contract"),
        contract_id=contract.id,
        portfolio_id=PortfolioID("strategy"),
        quantity=Quantity(Decimal("5")),
        side=PositionSide.SHORT,
        venue_id=contract.venue_id,
        average_price=Price(Decimal("0.93")),
    )

    repairs = _legacy_short_position_repairs(state, PortfolioID("strategy"))
    for event in repairs:
        state.apply(event)

    assert len(repairs) == 2
    assert all(position.side is PositionSide.FLAT for position in state.positions.values())
    assert sum(position.realized_pnl for position in state.positions.values()) == Decimal(
        "2.15",
    )
    assert _legacy_short_position_repairs(state, PortfolioID("strategy")) == ()


def test_periodic_collateral_refresh_applies_simulated_deposit() -> None:
    """Replace the local ledger after venue cash increases outside the hot path."""
    left = _contract("left", "left-venue")
    right = _contract("right", "right-venue")
    pair = MatchedContractPair(left, right, Timestamp.now())
    cycle = MarketCycle(Underlying("BTC"), 3600)
    state = TradingState()
    state.apply(MarketMatchesUpdated(cycle, (pair,)))
    engine = TradingEngine(EventDispatcher(state), {})
    engine.enable(
        EngineConfig(
            max_notional_by_venue={},
            collateral_by_venue={
                left.venue_id: Decimal("1"),
                right.venue_id: Decimal("2"),
            },
        ),
    )
    left_collateral = _Collateral(Decimal("7"))
    right_collateral = _Collateral(Decimal("8"))
    safety = _runtime_safety(
        state,
        engine,
        {
            left.venue_id: left_collateral,
            right.venue_id: right_collateral,
        },
    )

    async def run() -> None:
        refresh = asyncio.create_task(safety.refresh_collateral_periodically(0.001))
        try:
            for _ in range(20):
                if engine._available_collateral(left.venue_id) == Decimal("7"):
                    break
                await asyncio.sleep(0.005)
        finally:
            refresh.cancel()
            await asyncio.gather(refresh, return_exceptions=True)

    asyncio.run(run())

    assert engine._available_collateral(left.venue_id) == Decimal("7")
    assert engine._available_collateral(right.venue_id) == Decimal("8")
    assert set(left_collateral.calls) == {(left.id,)}
    assert set(right_collateral.calls) == {(right.id,)}


def test_collateral_401_stops_periodic_refresh() -> None:
    """Do not keep trading after a mid-run collateral authentication failure."""
    left = _contract("left", "left-venue")
    right = _contract("right", "right-venue")
    pair = MatchedContractPair(left, right, Timestamp.now())
    cycle = MarketCycle(Underlying("BTC"), 3600)
    state = TradingState()
    state.apply(MarketMatchesUpdated(cycle, (pair,)))
    engine = TradingEngine(EventDispatcher(state), {})
    engine.enable(EngineConfig(max_notional_by_venue={}))

    class _Unauthorized:
        def get_available_collateral(self, _contract_ids):
            error = RuntimeError("HTTP 401 Unauthorized")
            error.status_code = 401
            raise error

    safety = _runtime_safety(
        state,
        engine,
        {
            left.venue_id: _Unauthorized(),
            right.venue_id: _Collateral(Decimal("8")),
        },
    )

    async def run() -> None:
        task = asyncio.create_task(safety.refresh_collateral_periodically(0.001))
        with pytest.raises(_VenueSafetyStop, match="401"):
            await asyncio.wait_for(task, timeout=1)

    asyncio.run(run())


def test_collateral_5xx_stops_after_three_consecutive_refreshes() -> None:
    """Allow transient server errors but stop after sustained collateral failure."""
    left = _contract("left", "left-venue")
    right = _contract("right", "right-venue")
    pair = MatchedContractPair(left, right, Timestamp.now())
    cycle = MarketCycle(Underlying("BTC"), 3600)
    state = TradingState()
    state.apply(MarketMatchesUpdated(cycle, (pair,)))

    class _ServerError:
        def __init__(self) -> None:
            self.calls = 0

        def get_available_collateral(self, _contract_ids):
            self.calls += 1
            error = RuntimeError("upstream unavailable")
            error.status_code = 503
            raise error

    failing = _ServerError()
    engine = TradingEngine(EventDispatcher(state), {})
    safety = _runtime_safety(
        state,
        engine,
        {
            left.venue_id: failing,
            right.venue_id: _Collateral(Decimal("8")),
        },
    )

    async def run() -> None:
        task = asyncio.create_task(safety.refresh_collateral_periodically(0.001))
        with pytest.raises(_VenueSafetyStop, match="consecutive"):
            await asyncio.wait_for(task, timeout=1)

    asyncio.run(run())
    assert failing.calls == 3


def test_engine_enable_clears_safety_halt_for_a_new_run() -> None:
    """Require a fresh explicit engine enable before trading can resume."""
    state = TradingState()
    engine = TradingEngine(EventDispatcher(state), {})
    engine.fail_run("venue unavailable")

    assert state.trading_enabled is False
    assert state.safety_halted is True
    engine.enable(EngineConfig(max_notional_by_venue={}))

    assert state.trading_enabled is True
    assert state.safety_halted is False
    assert state.last_error is None


def test_degraded_active_venue_stops_health_monitor() -> None:
    """Stop a run when one active venue becomes degraded in a mixed report."""
    left = _contract("left", "left-venue")
    right = _contract("right", "right-venue")
    pair = MatchedContractPair(left, right, Timestamp.now())
    cycle = MarketCycle(Underlying("BTC"), 3600)
    state = TradingState()
    state.apply(MarketMatchesUpdated(cycle, (pair,)))
    now = Timestamp.now()

    class _Health:
        async def get(self, *, force_refresh: bool = False) -> VenueHealthReport:
            assert force_refresh is True
            return VenueHealthReport(
                generated_at=now,
                overall_status=VenueHealthStatus.DEGRADED,
                venues=(
                    VenueHealthSnapshot(
                        venue_id=left.venue_id,
                        status=VenueHealthStatus.DEGRADED,
                        checked_at=now,
                        latency_ms=None,
                        source="test",
                        message="official status unavailable",
                    ),
                    VenueHealthSnapshot(
                        venue_id=right.venue_id,
                        status=VenueHealthStatus.OPERATIONAL,
                        checked_at=now,
                        latency_ms=1,
                        source="test",
                        message="reachable",
                        http_status=200,
                    ),
                ),
            )

    engine = TradingEngine(EventDispatcher(state), {})
    safety = _runtime_safety(
        state,
        engine,
        {},
        health=_Health(),
    )

    async def run() -> None:
        with pytest.raises(_VenueSafetyStop, match="health is degraded"):
            await asyncio.wait_for(
                safety.monitor_active_venue_health(0.001),
                timeout=1,
            )

    asyncio.run(run())


def test_health_monitor_confirms_transient_timeout_before_stopping() -> None:
    """Require three transport failures before stopping an active run."""
    left = _contract("left", "left-venue")
    right = _contract("right", "right-venue")
    pair = MatchedContractPair(left, right, Timestamp.now())
    cycle = MarketCycle(Underlying("BTC"), 3600)
    state = TradingState()
    state.apply(MarketMatchesUpdated(cycle, (pair,)))
    now = Timestamp.now()

    class _Health:
        def __init__(self) -> None:
            self.calls = 0

        async def get(self, *, force_refresh: bool = False) -> VenueHealthReport:
            assert force_refresh is True
            self.calls += 1
            return VenueHealthReport(
                generated_at=now,
                overall_status=VenueHealthStatus.DEGRADED,
                venues=tuple(
                    VenueHealthSnapshot(
                        venue_id=venue_id,
                        status=VenueHealthStatus.UNAVAILABLE,
                        checked_at=now,
                        latency_ms=None,
                        source="test",
                        message="TimeoutError",
                        error_type="TimeoutError",
                        retryable=True,
                    )
                    for venue_id in (left.venue_id, right.venue_id)
                ),
            )

    health = _Health()
    engine = TradingEngine(EventDispatcher(state), {})
    safety = _runtime_safety(
        state,
        engine,
        {},
        health=health,
    )

    async def run() -> None:
        with pytest.raises(_VenueSafetyStop, match="3 consecutive transient"):
            await asyncio.wait_for(
                safety.monitor_active_venue_health(0.001),
                timeout=1,
            )

    asyncio.run(run())
    assert health.calls == 3


def test_runtime_prepares_inventory_before_enabling_engine(monkeypatch) -> None:
    """Do not configure the engine until selected inventory is confirmed."""
    order = []
    pair_key = ("left", "right")
    inventory_by_contract = {
        ContractID("left"): Quantity(Decimal("5")),
        ContractID("right"): Quantity(Decimal("5")),
    }

    class _Engine:
        def __init__(self) -> None:
            self.config = None

        def enable(self, config) -> None:
            order.append("enable")
            self.config = config

        def disable(self) -> None:
            pass

        async def wait_until_done(self) -> None:
            return

    class _Waiting:
        error = None

        async def wait_until_failed(self) -> None:
            await asyncio.Event().wait()

    class _Runs:
        def mark_running(self) -> None:
            order.append("running")

    class _PreparedInventory:
        async def prepare(self, _market_keys, *, minimum_time_remaining_seconds):
            assert minimum_time_remaining_seconds == 120
            order.append("inventory")
            return frozenset({pair_key}), inventory_by_contract

        def clear_prepared(self) -> None:
            raise AssertionError("The selected short market must be prepared")

    runtime = ArbitrageRuntime(enabled=False)
    runtime.state = TradingState()
    runtime.engine = _Engine()
    runtime.pipeline = _Waiting()
    runtime._feed = _Waiting()
    from tests.api.test_websocket import _regular_candidate
    runtime._feed.monitored_market = lambda _key: _regular_candidate()
    runtime.execution_runs = _Runs()
    runtime._inventory = _PreparedInventory()  # type: ignore[assignment]
    runtime._safety = RuntimeSafety(  # type: ignore[arg-type]
        None,
        runtime.state,
        runtime.engine,
        runtime.pipeline,
        runtime._execution,
    )

    async def no_op() -> None:
        return

    monkeypatch.setattr(runtime, "start", no_op)
    monkeypatch.setattr(runtime, "_ensure_execution", no_op)

    asyncio.run(
        runtime._run_execution(
            LiveArbitrageConfig(short_market_keys=("regular:selected",)),
            asyncio.Event(),
        ),
    )

    assert order[:3] == ["inventory", "running", "enable"]
    assert runtime.engine.config.short_pair_keys == frozenset({pair_key})
    assert runtime.engine.config.short_inventory_by_contract == inventory_by_contract
    assert runtime.engine.config.execute_short is True
