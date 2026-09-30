"""Resolve lifecycle-owned services for FastAPI endpoints.

Responsibilities
----------------
- Read shared runtime dependencies from the active application state.
- Compose request-scoped transactional services.
"""

import os
from collections.abc import Iterator
from functools import lru_cache

from fastapi import HTTPException, Request, WebSocket
import psycopg

from prediction_markets.application.alerting.service import AlertingApplicationService
from prediction_markets.application.markets.arbitrage_candidates import (
    ListArbitrageCandidates,
)
from prediction_markets.application.pnl import PnlService
from prediction_markets.application.venue_health import VenueHealthService
from prediction_markets.api.runtime import (
    ArbitrageRuntime,
    _configured_alert_recipients,
)
from prediction_markets.api.trading.activity import TradingActivityReader
from prediction_markets.api.trading.run_manager import ExecutionRunManager
from prediction_markets.domain.alerting.ports import AlertingPort
from prediction_markets.domain.alerting.service import NotificationPolicy
from prediction_markets.infrastructure.agg.market_explorer import (
    AggMarketExplorerAdapter,
)
from prediction_markets.infrastructure.postgres.repositories import (
    PostgresIncidentRepository,
    PostgresNotificationDeliveryRepository,
)

# ================================
# Arbitrage Runtime
# ================================

def get_arbitrage_runtime(request: Request) -> ArbitrageRuntime:
    """Retrieve the active runtime for arbitrage operations."""
    return request.app.state.arbitrage_runtime


def get_execution_run_manager(request: Request) -> ExecutionRunManager:
    """Retrieve the active execution run manager for cycle processing."""
    return request.app.state.execution_runs


def get_arbitrage_candidate_service(request: Request) -> ListArbitrageCandidates:
    """Retrieve the lifecycle-composed arbitrage candidate query service."""
    return request.app.state.arbitrage_candidate_service


def get_agg_market_explorer(request: Request) -> AggMarketExplorerAdapter:
    """Retrieve the lifecycle-composed AGG market catalog adapter."""
    return request.app.state.agg_market_explorer


def get_pnl_service(request: Request) -> PnlService:
    """Retrieve the lifecycle-composed cross-venue PnL service."""
    return request.app.state.pnl_service


def get_venue_health_service(request: Request) -> VenueHealthService:
    """Retrieve the lifecycle-composed venue-health service."""
    return request.app.state.venue_health_service


def get_alerting_port() -> Iterator[AlertingPort]:
    """Compose transactional alerting use cases for one HTTP request.

    Yields
    ------
    AlertingPort
        Inbound alerting service whose repository changes commit together.

    Raises
    ------
    HTTPException
        With status 503 when PostgreSQL is unavailable or unconfigured.
    """
    dsn = os.getenv("DATABASE_URL") or os.getenv("POSTGRES_DSN")
    if not dsn:
        raise HTTPException(
            status_code=503,
            detail="DATABASE_URL or POSTGRES_DSN is not configured",
        )
    try:
        recipients = _configured_alert_recipients(
            os.getenv("ALERT_RECIPIENTS_JSON", ""),
        )
    except ValueError as error:
        raise HTTPException(status_code=503, detail=str(error)) from error
    try:
        with psycopg.connect(dsn, connect_timeout=5) as connection:
            yield AlertingApplicationService(
                PostgresIncidentRepository(
                    connection,
                    manage_transactions=False,
                ),
                PostgresNotificationDeliveryRepository(
                    connection,
                    manage_transactions=False,
                ),
                NotificationPolicy(recipients),
            )
    except psycopg.Error as error:
        raise HTTPException(
            status_code=503,
            detail="Alerting database is unavailable",
        ) from error


def get_websocket_market_worker(websocket: WebSocket) -> ArbitrageRuntime:
    """Retrieve the runtime used by WebSocket signal subscriptions."""
    return websocket.scope["app"].state.market_worker


@lru_cache(maxsize=1)
def get_trading_activity_reader() -> TradingActivityReader:
    """Retrieve the active trading activity reader for database operations."""
    dsn = os.getenv("DATABASE_URL") or os.getenv("POSTGRES_DSN")
    if not dsn:
        raise HTTPException(
            status_code=503,
            detail="DATABASE_URL or POSTGRES_DSN is not configured",
        )
    return TradingActivityReader(dsn)
