"""Verify translation of API run settings into engine policy."""

from decimal import Decimal

from prediction_markets.api.trading.runner import LiveArbitrageConfig
from prediction_markets.domain.trading.portfolio import STRATEGY_PORTFOLIO_ID
from prediction_markets.infrastructure.venues.limitless.mappers import LIMITLESS_VENUE_ID
from prediction_markets.infrastructure.venues.polymarket.mappers import POLYMARKET_VENUE_ID
from prediction_markets.infrastructure.venues.predict.mappers import PREDICT_VENUE_ID


def test_live_config_builds_venue_neutral_engine_limits() -> None:
    """Preserve API budgets and defer short activation to the runtime."""
    config = LiveArbitrageConfig(
        max_arbitrages=3,
        max_concurrent_arbitrages=2,
        polymarket_max_notional=Decimal("4"),
        limitless_max_notional=Decimal("2.5"),
        predict_max_notional=Decimal("3"),
        large_order_contract_threshold=12,
        large_order_liquidity_safety_factor=Decimal("0.6"),
        predict_limit_slippage_ticks=4,
        predict_use_edge_budget=True,
        short_market_keys=("cycle:BTC:3600",),
    ).engine_config()

    assert config.max_notional_by_venue == {
        POLYMARKET_VENUE_ID: Decimal("4"),
        LIMITLESS_VENUE_ID: Decimal("2.5"),
        PREDICT_VENUE_ID: Decimal("3"),
    }
    assert config.market_buy_notional_steps == {
        POLYMARKET_VENUE_ID: Decimal("0.01"),
    }
    assert config.max_arbitrages == 3
    assert config.max_concurrent_arbitrages == 2
    assert config.cost_buffer == Decimal("0")
    assert config.min_net_edge == Decimal("0")
    assert config.large_order_contract_threshold == 12
    assert config.large_order_liquidity_safety_factor == Decimal("0.6")
    assert config.predict_limit_slippage_ticks == 4
    assert config.predict_use_edge_budget is True
    assert config.min_market_time_remaining_seconds == 120
    assert config.max_skew_ms == 1_000
    assert config.execute_long is True
    assert config.execute_short is False
    assert config.short_market_keys == frozenset({"cycle:BTC:3600"})
    assert config.portfolio_id == STRATEGY_PORTFOLIO_ID
