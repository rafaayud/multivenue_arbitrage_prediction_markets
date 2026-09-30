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
    _legacy_short_position_repairs,
    _short_inventory_expiry_error,
)
from prediction_markets.api.runtime.safety import RuntimeSafety, _VenueSafetyStop
from prediction_markets.api.trading.runner import LiveArbitrageConfig
from prediction_markets.application.engine import EngineConfig, TradingEngine
from prediction_markets.application.events import MarketMatchesUpdated
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
from prediction_markets.domain.trading.enums import PositionSide
from prediction_markets.domain.venue_health import (
    VenueHealthSnapshot,
    VenueHealthStatus,
)
from prediction_markets.infrastructure.venues.predict.mappers import PREDICT_VENUE_ID


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
        safety.read_available_collateral,
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


@pytest.mark.parametrize("action", ["prepare", "refresh", "redeem"])
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
            else coordinator.refresh(MarketMatchesUpdated(cycle, (pair,))) if action == "refresh"
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


def test_cycle_rollover_prepares_new_short_market_inventory_once() -> None:
    """Split the new cycle and replace the engine's expired short allowlist."""
    old_left = _contract("old-left", "left-venue")
    old_right = _contract("old-right", "right-venue")
    new_left = _contract("new-left", "left-venue")
    new_right = _contract("new-right", "right-venue")
    old_pair = MatchedContractPair(old_left, old_right, Timestamp.now())
    new_pair = MatchedContractPair(new_left, new_right, Timestamp.now())
    cycle = MarketCycle(Underlying("BTC"), 3600)
    market_key = "cycle:BTC:3600"
    state = TradingState()
    state.apply(MarketMatchesUpdated(cycle, (old_pair,)))
    engine = TradingEngine(EventDispatcher(state), {})
    inventory = _Inventory(
        {
            old_left.venue_id: Decimal("8"),
            old_right.venue_id: Decimal("11"),
        },
    )
    coordinator = _inventory_coordinator(
        state,
        inventory,
        engine=engine,
        execution={
            old_left.venue_id: _Collateral(Decimal("20")),
            old_right.venue_id: _Collateral(Decimal("20")),
        },
    )

    async def run() -> None:
        pair_keys, inventory_by_contract = await coordinator.prepare(
            (market_key,),
        )
        engine.enable(
            EngineConfig(
                max_notional_by_venue={
                    old_left.venue_id: Decimal("10"),
                    old_right.venue_id: Decimal("10"),
                },
                execute_short=True,
                short_market_keys=frozenset({market_key}),
                short_pair_keys=pair_keys,
                short_inventory_by_contract=inventory_by_contract,
            ),
        )
        event = MarketMatchesUpdated(cycle, (new_pair,))
        await coordinator.refresh(event)
        await coordinator.refresh(event)

    asyncio.run(run())

    assert inventory.calls == [
        {
            old_left.venue_id: old_left.market_id,
            old_right.venue_id: old_right.market_id,
        },
        {
            new_left.venue_id: new_left.market_id,
            new_right.venue_id: new_right.market_id,
        },
    ]
    assert engine._config is not None
    assert engine._config.short_pair_keys == frozenset({new_pair.key})
    assert engine._config.short_inventory_by_contract == {
        new_left.id: Quantity(Decimal("8")),
        new_right.id: Quantity(Decimal("11")),
    }
    assert engine._available_collateral(old_left.venue_id) == Decimal("20")
    assert engine._available_collateral(old_right.venue_id) == Decimal("20")


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


def test_pair_change_in_same_markets_does_not_prepare_short_inventory() -> None:
    """Ignore contract-pair churn while each venue market remains unchanged."""
    old_left = _contract("old-left", "left-venue")
    old_right = _contract("old-right", "right-venue")
    changed_left = replace(
        old_left,
        id=ContractID("changed-left"),
        outcome_id=OutcomeID("changed-left"),
    )
    changed_right = replace(
        old_right,
        id=ContractID("changed-right"),
        outcome_id=OutcomeID("changed-right"),
    )
    old_pair = MatchedContractPair(old_left, old_right, Timestamp.now())
    changed_pair = MatchedContractPair(changed_left, changed_right, Timestamp.now())
    cycle = MarketCycle(Underlying("BTC"), 3600)
    market_key = "cycle:BTC:3600"
    state = TradingState()
    state.apply(MarketMatchesUpdated(cycle, (old_pair,)))
    engine = TradingEngine(EventDispatcher(state), {})
    inventory = _Inventory(
        {
            old_left.venue_id: Decimal("8"),
            old_right.venue_id: Decimal("11"),
        },
    )
    coordinator = _inventory_coordinator(state, inventory, engine=engine)

    async def run() -> None:
        pair_keys, inventory_by_contract = await coordinator.prepare(
            (market_key,),
        )
        engine.enable(
            EngineConfig(
                max_notional_by_venue={},
                execute_short=True,
                short_market_keys=frozenset({market_key}),
                short_pair_keys=pair_keys,
                short_inventory_by_contract=inventory_by_contract,
            ),
        )
        await coordinator.refresh(
            MarketMatchesUpdated(cycle, (changed_pair,)),
        )

    asyncio.run(run())

    assert inventory.calls == [
        {
            old_left.venue_id: old_left.market_id,
            old_right.venue_id: old_right.market_id,
        },
    ]
    assert engine._config is not None
    assert engine._config.short_pair_keys == frozenset({old_pair.key})


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
            LiveArbitrageConfig(short_market_keys=("cycle:BTC:3600",)),
            asyncio.Event(),
        ),
    )

    assert order[:3] == ["inventory", "running", "enable"]
    assert runtime.engine.config.short_pair_keys == frozenset({pair_key})
    assert runtime.engine.config.short_inventory_by_contract == inventory_by_contract
    assert runtime.engine.config.execute_short is True
