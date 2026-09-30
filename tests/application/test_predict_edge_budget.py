"""Verify bounded, in-memory Predict entry pricing without changing execution size."""

from dataclasses import replace
from decimal import Decimal
from unittest.mock import patch

import pytest

from prediction_markets.application.engine import EngineConfig, TradingEngine
from prediction_markets.application.state import EventDispatcher, TradingState
from prediction_markets.domain.arbitrage.value_objects import ArbitrageOpportunity
from prediction_markets.domain.market_matching.value_objects import MatchedContractPair
from prediction_markets.domain.orderbook.value_objects import OrderBookLevel
from prediction_markets.domain.shared.value_objects import Price, Quantity, Timestamp, VenueID
from prediction_markets.domain.trading.enums import OrderSide
from prediction_markets.infrastructure.venues.predict.taker_fees import PredictTakerFeeCalculator
from tests.application.test_engine import _contract, _OnePercentFees, _ZeroFees


def _case(side=OrderSide.BUY, *, poly_price="0.4", predict_price="0.5",
          tick="0.01", rate=200, budget="100", quantity="5", **overrides):
    """Build a cached-fee scenario with a fixed-size baseline and no venue clients."""
    poly, predict = VenueID("POLYMARKET"), VenueID("PREDICT")
    left = _contract("poly-up", poly, "yes")
    right = _contract("predict:123:no", predict, "no", tick_size=tick)
    pair = MatchedContractPair(left, right, Timestamp.now())
    qty = Quantity(Decimal(quantity))
    prices = (Decimal(poly_price), Decimal(predict_price))
    gross = 1 - sum(prices) if side is OrderSide.BUY else sum(prices) - 1
    opportunity = ArbitrageOpportunity(
        left.id, right.id, side,
        OrderBookLevel(Price(prices[0]), qty), OrderBookLevel(Price(prices[1]), qty),
        qty, gross, gross, 0, Timestamp.now(),
    )
    config = EngineConfig(
        max_notional_by_venue={poly: Decimal("100"), predict: Decimal(budget)},
        short_inventory_by_contract={left.id: qty, right.id: qty},
        predict_use_edge_budget=True, predict_limit_slippage_ticks=90,
        min_notional_per_venue=Decimal("0"),
        large_order_contract_threshold=100,
        **overrides,
    )
    fees = PredictTakerFeeCalculator(fee_rates_bps={"123": rate}, catalog=object())
    engine = TradingEngine(EventDispatcher(TradingState()), {poly: _OnePercentFees(), predict: fees})
    engine.configure(config)
    return engine, opportunity, pair, config


@pytest.mark.parametrize("side,poly,predict", [
    (OrderSide.BUY, "0.4", "0.5"), (OrderSide.BUY, "0.75", "0.2"),
    (OrderSide.SELL, "0.55", "0.5"), (OrderSide.SELL, "0.3", "0.8"),
])
@pytest.mark.parametrize("rate", [0, 200, 1000])
def test_edge_budget_matches_exhaustive_fee_aware_tick_search(side, poly, predict, rate):
    """Select the most aggressive profitable tick at the unchanged baseline size."""
    engine, opportunity, pair, config = _case(
        side, poly_price=poly, predict_price=predict, rate=rate,
        min_net_edge=Decimal("0.002"), cost_buffer=Decimal("0.001"),
    )
    baseline_config = replace(config, predict_use_edge_budget=False, predict_limit_slippage_ticks=0)
    baseline = engine._risk_adjusted_opportunity(opportunity, pair, baseline_config)
    trace = {}
    adjusted = engine._risk_adjusted_opportunity(opportunity, pair, config, pricing=trace)
    if baseline is None:
        assert adjusted is None
        return
    assert adjusted is not None
    assert adjusted.quantity == baseline.quantity
    assert adjusted.left_level == baseline.left_level
    assert adjusted.net_edge > config.min_net_edge
    assert trace["candidate_checks"] <= 7
    assert trace["edge_budget_search_ms"] >= 0
    eligible = []
    for tick_index in range(1, 100):
        price = Decimal(tick_index) / 100
        if (price - Decimal(predict)) * (1 if side is OrderSide.BUY else -1) < 0:
            continue
        candidate = replace(opportunity, right_level=replace(opportunity.right_level, price=Price(price)))
        result = engine._risk_adjusted_opportunity(candidate, pair, baseline_config)
        if result is not None and result.quantity == baseline.quantity:
            eligible.append(price)
    expected = max(eligible) if side is OrderSide.BUY else min(eligible)
    assert adjusted.right_level.price.value == expected
    assert Decimal(trace["edge_spent"]) == baseline.net_edge - adjusted.net_edge


@pytest.mark.parametrize("side,poly,predict,budget", [
    (OrderSide.BUY, "0.4", "0.5", "2.65"),
    (OrderSide.SELL, "0.55", "0.5", "2.575"),
])
def test_budget_limits_price_headroom_instead_of_reducing_size(side, poly, predict, budget):
    """Keep five contracts and reject extra headroom that would exceed the budget."""
    engine, opportunity, pair, config = _case(
        side, poly_price=poly, predict_price=predict, budget=budget,
    )
    adjusted = engine._risk_adjusted_opportunity(opportunity, pair, config)
    assert adjusted is not None and adjusted.quantity.value == 5
    assert adjusted.right_level.price == Price(Decimal("0.51" if side is OrderSide.BUY else "0.50"))


def test_cash_and_existing_reservations_also_limit_headroom():
    """Use the parent's remaining collateral and notional, not the configured maximum."""
    engine, opportunity, pair, config = _case()
    engine._collateral_balance_by_venue = {pair.left.venue_id: Decimal("100"), pair.right.venue_id: Decimal("2.66")}
    engine._reserved_notional_by_venue[pair.right.venue_id] = Decimal("97.35")
    result = engine._risk_adjusted_opportunity(opportunity, pair, config)
    assert result is not None and result.quantity.value == 5
    assert result.right_level.price.value == Decimal("0.51")


def test_too_small_edge_keeps_original_quote_and_never_accepts_break_even():
    """Do not stack fixed ticks or cross the strict zero-profit boundary."""
    engine, opportunity, pair, config = _case(poly_price="0.49", rate=0)
    engine._fees[pair.left.venue_id] = _ZeroFees()
    result = engine._risk_adjusted_opportunity(opportunity, pair, config)
    assert result is not None and result.right_level.price.value == Decimal("0.5")
    assert result.net_edge == Decimal("0.01")
    equal = replace(opportunity, left_level=replace(opportunity.left_level, price=Price(Decimal("0.5"))))
    assert engine._risk_adjusted_opportunity(equal, pair, config) is None


@pytest.mark.parametrize("tick", [None, "0.03"])
def test_missing_or_unaligned_predict_tick_fails_closed(tick):
    """Never guess a venue tick or sign an off-grid price in edge-budget mode."""
    engine, opportunity, pair, config = _case(tick=tick)
    assert engine._risk_adjusted_opportunity(opportunity, pair, config) is None


def test_fine_tick_grid_has_bounded_work_and_no_http():
    """Cap the candidate evaluations even for an eighteen-decimal price grid."""
    engine, opportunity, pair, config = _case(tick="0.000000000000000001")
    trace = {}
    with patch("httpx.Client.request", side_effect=AssertionError("HTTP in pricing")), \
         patch("httpx.AsyncClient.request", side_effect=AssertionError("HTTP in pricing")), \
         patch.object(engine._fees[pair.right.venue_id], "prepare", side_effect=AssertionError("fee preload in pricing")):
        result = engine._risk_adjusted_opportunity(opportunity, pair, config, pricing=trace)
    assert result is not None and result.net_edge > 0
    assert trace["candidate_checks"] == 32
    assert result.quantity == opportunity.quantity


def test_large_order_headroom_is_applied_only_once():
    """Apply the existing liquidity reduction once before searching price limits."""
    engine, opportunity, pair, config = _case(quantity="12")
    config = replace(config, large_order_contract_threshold=10)
    result = engine._risk_adjusted_opportunity(opportunity, pair, config)
    assert result is not None and result.quantity.value == 8


def test_feature_defaults_to_disabled():
    """Preserve fixed-tick behavior for existing API and engine callers."""
    from prediction_markets.api.models import TradingRunStart
    from prediction_markets.api.trading.runner import LiveArbitrageConfig

    assert TradingRunStart(live=True, confirmation="LIVE").predict_use_edge_budget is False
    assert LiveArbitrageConfig().engine_config().predict_use_edge_budget is False


def test_terminal_journal_trace_keeps_experiment_limits_and_latency():
    """Persist entry pricing with the normal terminal execution event and fills."""
    import json
    from prediction_markets.application.events import ArbitrageOpportunityFound, ExecutionUpdated, MarketMatchesUpdated, OrderBookUpdated
    from prediction_markets.application.markets.models import MarketCycle
    from prediction_markets.domain.market_matching.value_objects import Underlying
    from tests.application.test_engine import _book, _fill, _process

    engine, opportunity, pair, config = _case()
    engine.enable(config)
    cycle = MarketCycle(Underlying("BTC"), 300)
    engine.state.apply(MarketMatchesUpdated(cycle, (pair,)))
    for contract, price in ((pair.left, "0.4"), (pair.right, "0.5")):
        engine.state.apply(OrderBookUpdated(contract.venue_id, contract.id, _book(contract, price, "5")))
    event = ArbitrageOpportunityFound("edge-budget-trace", cycle, pair, opportunity)
    planned, first, second = engine.stage_opportunity(event)
    engine.commit(planned)
    for command in (first, second):
        engine.commit(command)
    _process(engine, _fill(first, first.intent.limit_price, first.intent.quantity))
    events = _process(engine, _fill(second, second.intent.limit_price, second.intent.quantity))
    final = [item for item in events if isinstance(item, ExecutionUpdated)][-1]
    trace = json.loads(final.latency_trace_json)["entry_pricing"]
    assert len(trace) == 1 and trace[0]["mode"] == "edge_budget"
    assert Decimal(trace[0]["detected_limit"]) == Decimal("0.5")
    predict_command = next(command for command in (first, second) if command.venue_id == pair.right.venue_id)
    assert Decimal(trace[0]["planned_limit"]) == predict_command.intent.limit_price.value
    assert trace[0]["risk_pricing_ms"] >= trace[0]["edge_budget_search_ms"] >= 0


@pytest.mark.parametrize("side,poly,predict", [
    (OrderSide.BUY, "0.001", "0.98"), (OrderSide.SELL, "0.999", "0.02"),
])
def test_tradable_price_extremes_are_preserved(side, poly, predict):
    """The optimizer cannot move beyond one tick or one-minus-tick."""
    engine, opportunity, pair, config = _case(side, poly_price=poly, predict_price=predict, rate=0)
    engine._fees[pair.left.venue_id] = _ZeroFees()
    result = engine._risk_adjusted_opportunity(opportunity, pair, config)
    assert result is not None
    assert result.right_level.price.value == Decimal("0.99" if side is OrderSide.BUY else "0.01")


def test_predict_can_be_left_leg_and_other_routes_are_unchanged():
    """Use venue identity, not primary/hedge position, and leave other routes alone."""
    engine, opportunity, pair, config = _case()
    expected = engine._risk_adjusted_opportunity(opportunity, pair, config)
    swapped = replace(opportunity, left_contract_id=pair.right.id, right_contract_id=pair.left.id,
                      left_level=opportunity.right_level, right_level=opportunity.left_level)
    result = engine._risk_adjusted_opportunity(swapped, replace(pair, left=pair.right, right=pair.left), config)
    assert result.left_level == expected.right_level
    assert result.right_level == expected.left_level
    other = replace(pair.right, venue_id=VenueID("LIMITLESS"))
    engine._fees[other.venue_id] = _ZeroFees()
    other_config = replace(config, max_notional_by_venue={pair.left.venue_id: Decimal("100"), other.venue_id: Decimal("100")})
    other_pair = replace(pair, right=other)
    enabled = engine._risk_adjusted_opportunity(opportunity, other_pair, other_config)
    disabled = engine._risk_adjusted_opportunity(opportunity, other_pair, replace(other_config, predict_use_edge_budget=False))
    assert enabled == disabled


def test_worker_accepts_movement_inside_limit_but_rejects_lost_edge():
    """Current books may move inside the new limit; existing worker economics still apply."""
    from tests.api.test_worker_opportunity_validation import _setup
    from tests.api.test_market_workers import _book
    from prediction_markets.api.runtime.market_workers import _validate_worker_opportunity
    from prediction_markets.application.worker_validation import WorkerValidationReason as Reason

    worker, journal, request = _setup()
    request = replace(request, right_limit_price=Price(Decimal("0.58")))
    worker.state.books[request.pair.right.id] = _book(request.pair.right, "0.57")
    assert _validate_worker_opportunity(request, worker, journal).reason is Reason.ACCEPTED
    worker.state.books[request.pair.right.id] = _book(request.pair.right, "0.61")
    assert _validate_worker_opportunity(request, worker, journal).reason is Reason.EDGE_LOST
