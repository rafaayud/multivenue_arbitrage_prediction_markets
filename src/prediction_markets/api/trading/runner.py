"""Define live-run configuration and environment preflight checks.

Responsibilities
----------------
- Validate credentials and PostgreSQL availability before enabling real orders.
- Translate API risk settings into the venue-neutral engine configuration.
"""

import os
from dataclasses import dataclass
from decimal import Decimal

import psycopg

from prediction_markets.application.engine import EngineConfig
from prediction_markets.domain.market_matching.value_objects import Underlying
from prediction_markets.domain.trading.portfolio import STRATEGY_PORTFOLIO_ID
from prediction_markets.infrastructure.venues.limitless.mappers import LIMITLESS_VENUE_ID
from prediction_markets.infrastructure.venues.polymarket.mappers import POLYMARKET_VENUE_ID
from prediction_markets.infrastructure.venues.predict.mappers import PREDICT_VENUE_ID


@dataclass(frozen=True, slots=True)
class LiveArbitrageConfig:
    """Hold monitored cycles and execution limits for one live run.

    Attributes
    ----------
    large_order_contract_threshold
        Detected quantity at which the engine starts retaining liquidity
        headroom.
    large_order_liquidity_safety_factor
        Fraction of detected large-order liquidity sent to planning.
    predict_limit_slippage_ticks
        Predict-only limit-price headroom; zero preserves top of book.
    predict_use_edge_budget
        Replace fixed ticks with fee-aware headroom above the edge and buffer floors.
    max_concurrent_arbitrages
        Maximum independent executions admitted at the same time.
    max_skew_ms
        Maximum receive-time difference between books during detection. Policy
        markets use a wider window because their quotes update infrequently;
        submission retains its separate strict freshness guard.
    min_market_time_remaining_seconds
        Default seconds before cycle expiry required to plan or submit. The
        5-minute cycle uses 30 seconds, the 15-minute cycle uses 60 seconds,
        and other recurring markets use this 120-second default.
    """

    underlyings: tuple[Underlying, ...] = (
        Underlying("BTC"),
        Underlying("ETH"),
        Underlying("BNB"),
        Underlying("NVDA"),
        Underlying("AMZN"),
        Underlying("META"),
        Underlying("TSLA"),
        Underlying("SPY"),
        Underlying("SPCX"),
    )
    intervals_seconds: tuple[int, ...] = (3600, 86400)
    max_arbitrages: int = 1
    max_concurrent_arbitrages: int = 2
    min_notional_per_venue: Decimal = Decimal("1")
    polymarket_max_notional: Decimal = Decimal("5")
    limitless_max_notional: Decimal = Decimal("5")
    predict_max_notional: Decimal = Decimal("5")
    min_net_edge: Decimal = Decimal("0")
    cost_buffer: Decimal = Decimal("0")
    large_order_contract_threshold: int = 10
    large_order_liquidity_safety_factor: Decimal = Decimal("0.7")
    predict_limit_slippage_ticks: int = 2
    predict_use_edge_budget: bool = False
    max_recovery_loss: Decimal = Decimal("1")
    min_market_time_remaining_seconds: int = 120
    max_skew_ms: int = 1_000
    short_market_keys: tuple[str, ...] = ()

    def engine_config(self) -> EngineConfig:
        """Translate live-run fields into engine limits.

        Returns
        -------
        EngineConfig
            Execution policy retaining the requested short-market selection;
            the runtime enables shorts only after confirming inventory.
        """
        return EngineConfig(
            max_notional_by_venue={
                POLYMARKET_VENUE_ID: self.polymarket_max_notional,
                LIMITLESS_VENUE_ID: self.limitless_max_notional,
                PREDICT_VENUE_ID: self.predict_max_notional,
            },
            market_buy_notional_steps={POLYMARKET_VENUE_ID: Decimal("0.01")},
            max_arbitrages=self.max_arbitrages,
            max_concurrent_arbitrages=self.max_concurrent_arbitrages,
            min_notional_per_venue=self.min_notional_per_venue,
            min_net_edge=self.min_net_edge,
            cost_buffer=self.cost_buffer,
            large_order_contract_threshold=self.large_order_contract_threshold,
            large_order_liquidity_safety_factor=(
                self.large_order_liquidity_safety_factor
            ),
            predict_limit_slippage_ticks=self.predict_limit_slippage_ticks,
            predict_use_edge_budget=self.predict_use_edge_budget,
            max_recovery_loss=self.max_recovery_loss,
            max_skew_ms=self.max_skew_ms,
            execute_long=True,
            execute_short=False,
            short_market_keys=frozenset(self.short_market_keys),
            allowed_underlyings=tuple(value.symbol for value in self.underlyings),
            allowed_intervals_seconds=self.intervals_seconds,
            min_market_time_remaining_seconds=self.min_market_time_remaining_seconds,
            portfolio_id=STRATEGY_PORTFOLIO_ID,
        )


def database_url() -> str:
    """Return the configured PostgreSQL DSN.

    Raises
    ------
    RuntimeError
        If neither supported environment variable is configured.
    """
    value = os.getenv("DATABASE_URL") or os.getenv("POSTGRES_DSN")
    if not value:
        raise RuntimeError("Set DATABASE_URL or POSTGRES_DSN in .env")
    return value


def preflight() -> dict[str, object]:
    """Check credentials, PostgreSQL, and unresolved projected executions."""
    required = (
        "POLYMARKET_PK",
        "POLYMARKET_FUNDER",
        "POLYMARKET_API_KEY",
        "POLYMARKET_API_SECRET",
        "POLYMARKET_PASSPHRASE",
        "LIMITLESS_PRIVATE_KEY",
        "LIMITLESS_API_KEY",
        "PREDICT_API_KEY",
        "PREDICT_ACCOUNT_ADDRESS",
        "PREDICT_PRIVY_PRIVATE_KEY",
    )
    missing = [name for name in required if not os.getenv(name)]
    active_journals = unresolved_recoveries = None
    database_ready = False
    try:
        with psycopg.connect(database_url(), connect_timeout=5) as connection:
            active_journals = connection.execute(
                "SELECT count(*) FROM arbitrage_execution_journals "
                "WHERE status = 'needs_review'",
            ).fetchone()[0]
            unresolved_recoveries = connection.execute(
                "SELECT count(*) FROM exposure_recoveries "
                "WHERE status <> 'resolved'",
            ).fetchone()[0]
            database_ready = True
    except (psycopg.Error, RuntimeError):
        pass
    return {
        "ready": (
            not missing
            and database_ready
            and active_journals == 0
            and unresolved_recoveries == 0
        ),
        "missing_credentials": missing,
        "database_ready": database_ready,
        "active_journals": active_journals,
        "unresolved_recoveries": unresolved_recoveries,
    }
