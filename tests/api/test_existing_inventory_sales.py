"""Verify existing-token SELL routes without live I/O or collateral operations."""

import asyncio
from dataclasses import replace
from decimal import Decimal
from unittest.mock import AsyncMock

import pytest

from prediction_markets.application.engine import EngineConfig, TradingEngine
from prediction_markets.application.events import (
    ArbitragePlanned, ExecutionUpdated, MarketMatchesUpdated, OrderBookUpdated, SubmitOrder,
)
from prediction_markets.application.markets.models import monitored_market_key
from prediction_markets.application.state import EventDispatcher, TradingState
from prediction_markets.domain.market_matching.value_objects import MatchedContractPair
from prediction_markets.domain.outcome_inventory import OutcomeInventoryBalance
from prediction_markets.domain.shared.value_objects import (
    ClientOrderID, ContractID, Currency, Money, PortfolioID, PositionID, Price, Quantity, Timestamp, VenueID,
)
from prediction_markets.domain.trading.entities import OrderSnapshot, Position
from prediction_markets.domain.trading.enums import (
    ArbitrageExecutionStatus, OrderSide, OrderStatus, OrderType, PositionSide,
)
from tests.api.test_short_inventory_runtime import _inventory_coordinator
from tests.application.test_engine import _ZeroFees, _book, _contract, _process, _regular_candidate


def _setup():
    """Hold one complementary route, with zero inventory of its inverse."""
    left = _contract("poly-no", VenueID("POLYMARKET"), "no")
    right = _contract("predict-yes", VenueID("PREDICT"), "yes")
    inverse_left = replace(left, id=ContractID("poly-yes"))
    inverse_right = replace(right, id=ContractID("predict-no"))
    pair = MatchedContractPair(left, right, Timestamp.now())
    inverse = MatchedContractPair(inverse_left, inverse_right, Timestamp.now())
    market = _regular_candidate(left, right)
    state = TradingState()
    state.apply(MarketMatchesUpdated(market, (pair, inverse)))
    balances = []
    for contract, opposite, amount, is_yes in ((left, inverse_left, "9", False), (right, inverse_right, "7.9", True)):
        portfolio = PortfolioID("cross-venue-arbitrage")
        pid = PositionID(f"{contract.venue_id}:{portfolio}:{contract.id}")
        state.positions[pid] = Position(
            id=pid, venue_id=contract.venue_id, contract_id=contract.id,
            portfolio_id=portfolio, side=PositionSide.LONG,
            quantity=Quantity(Decimal(amount)), average_price=Price(Decimal("0.4")),
        )
        balances.append(OutcomeInventoryBalance(
            venue_id=contract.venue_id, market_id=contract.market_id,
            yes=Quantity(Decimal(amount if is_yes else "0")),
            no=Quantity(Decimal("0" if is_yes else amount)),
            collateral=Money(Decimal("0"), Currency("USD")), observed_at=Timestamp.now(),
            yes_contract_id=contract.id if is_yes else opposite.id,
            no_contract_id=opposite.id if is_yes else contract.id,
        ))
    service = AsyncMock()
    service.get_balances.return_value = tuple(balances)
    service.ensure_short_inventory.side_effect = AssertionError("Existing-only must never split")
    coordinator = _inventory_coordinator(state, service)
    return state, coordinator, service, market, pair, inverse


def test_exact_pair_sells_only_available_inventory_and_stops_when_exhausted():
    """No split, inverse sale, duplicate reservation, or replenishment is allowed."""
    state, coordinator, service, market, pair, inverse = _setup()
    keys, inventory = asyncio.run(coordinator.prepare((), existing_inventory_pairs=(pair.key,)))
    service.ensure_short_inventory.assert_not_awaited()
    assert keys == frozenset({pair.key})
    assert inventory == {pair.left.id: Quantity(Decimal("9")), pair.right.id: Quantity(Decimal("7.9"))}
    assert coordinator.prepared_market_keys == (monitored_market_key(market),)
    engine = TradingEngine(EventDispatcher(state), {c.venue_id: _ZeroFees() for c in (pair.left, pair.right)})
    engine.enable(EngineConfig(
        execute_long=False, execute_short=True, short_pair_keys=keys,
        short_market_keys=frozenset(coordinator.prepared_market_keys),
        short_inventory_by_contract=inventory, max_arbitrages=10,
        max_notional_by_venue={c.venue_id: Decimal("10") for c in (pair.left, pair.right)},
        predict_limit_slippage_ticks=0, min_net_edge=Decimal("0.003"),
    ))
    for c in (inverse.left, inverse.right):
        outputs = _process(engine, OrderBookUpdated(c.venue_id, c.id, _book(c, "0.6", "100", bid=True)))
        assert not any(isinstance(e, SubmitOrder) for e in outputs)
    _process(engine, OrderBookUpdated(pair.left.venue_id, pair.left.id, _book(pair.left, "0.6", "100", bid=True)))
    observation = OrderBookUpdated(pair.right.venue_id, pair.right.id, _book(pair.right, "0.6", "100", bid=True))
    outputs = _process(engine, observation)
    execution = next(e.execution for e in outputs if isinstance(e, ArbitragePlanned))
    assert execution.leg1_quantity.value == execution.leg2_quantity.value == 7
    assert not any(isinstance(e, SubmitOrder) for e in _process(engine, observation))
    _process(engine, ExecutionUpdated(replace(execution, status=ArbitrageExecutionStatus.COMPLETED,
        leg1_filled_quantity=execution.leg1_quantity, leg2_filled_quantity=execution.leg2_quantity)))
    assert not any(isinstance(e, SubmitOrder) for e in _process(engine, observation))
    service.ensure_short_inventory.assert_not_awaited()


@pytest.mark.parametrize("status,uncertain,expected", [
    (OrderStatus.PARTIALLY_FILLED, True, "4.9"),
    (OrderStatus.CANCELLED, True, "4.9"),
    (OrderStatus.CANCELLED, False, "7.9"),
])
def test_reserves_remaining_unsettled_sell_tokens(status, uncertain, expected):
    """Subtract unfilled SELL quantities, including cancellations awaiting finality."""
    state, coordinator, _, _, pair, _ = _setup()
    client = ClientOrderID("pending-sell")
    state.orders[client] = OrderSnapshot(
        status=status, contract_id=pair.right.id,
        side=OrderSide.SELL, quantity=Quantity(Decimal("5")),
        filled_quantity=Quantity(Decimal("2")), average_price=Price(Decimal("0.6")),
        order_type=OrderType.LIMIT, client_order_id=client, may_receive_more_fills=uncertain,
    )
    _, inventory = asyncio.run(coordinator.prepare((), existing_inventory_pairs=(pair.key,)))
    assert inventory[pair.right.id].value == Decimal(expected)


@pytest.mark.parametrize("failure", ["unknown", "overlap", "inverse", "balance_error", "identity", "wallet_lower"])
def test_existing_inventory_validation_never_falls_back_to_splits(failure):
    """Reject invalid selections or missing balances without funding inventory."""
    _, coordinator, service, market, pair, inverse = _setup()
    selected = (pair.key,)
    split_markets = ()
    if failure == "unknown":
        selected = ((str(pair.left.id), "unknown-contract"),)
    elif failure == "overlap":
        split_markets = (monitored_market_key(market),)
    elif failure == "inverse":
        selected = (inverse.key,)
    elif failure == "balance_error":
        service.get_balances.side_effect = RuntimeError("Wallet unavailable")
    else:
        left, right = service.get_balances.return_value
        service.get_balances.return_value = (left, replace(right,
            yes_contract_id=None if failure == "identity" else right.yes_contract_id,
            yes=Quantity(Decimal("2")) if failure == "wallet_lower" else right.yes))
    if failure == "wallet_lower":
        _, inventory = asyncio.run(coordinator.prepare((), existing_inventory_pairs=selected))
        assert inventory[pair.right.id].value == 2
    else:
        with pytest.raises(RuntimeError):
            asyncio.run(coordinator.prepare(split_markets, existing_inventory_pairs=selected))
    service.ensure_short_inventory.assert_not_awaited()
    assert not coordinator.preparing
