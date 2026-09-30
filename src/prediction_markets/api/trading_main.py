"""Expose the private control surface for the latency-sensitive trading core.

Responsibilities
----------------
- Own feeds, detection, risk, journal, execution, and recovery in one process.
- Expose only health, metrics, runtime controls, and live signal streams.
"""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
import logging

from dotenv import load_dotenv
from fastapi import FastAPI

from prediction_markets.api.routers import (
    candidates,
    system,
    trading_activity,
    trading_runs,
    websocket,
)
from prediction_markets.api.runtime import ArbitrageRuntime
from prediction_markets.application.venue_health import VenueHealthService
from prediction_markets.infrastructure.venues.limitless.health import (
    LimitlessHealthAdapter,
)
from prediction_markets.infrastructure.venues.limitless.mappers import (
    LIMITLESS_VENUE_ID,
)
from prediction_markets.infrastructure.venues.polymarket.health import (
    PolymarketHealthAdapter,
)
from prediction_markets.infrastructure.venues.polymarket.mappers import (
    POLYMARKET_VENUE_ID,
)
from prediction_markets.infrastructure.venues.predict.health import PredictHealthAdapter
from prediction_markets.infrastructure.venues.predict.mappers import PREDICT_VENUE_ID
from prediction_markets.utils.decorators.logger import configure_logging


load_dotenv()
configure_logging(logging.WARNING)
logging.getLogger("prediction_markets.events").setLevel(logging.INFO)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Own the complete trading lifecycle and its private health checks.

    Parameters
    ----------
    app
        Trading application receiving the process-local runtime.

    Yields
    ------
    None
        Control while the trading runtime is active.
    """
    venue_health_service = VenueHealthService(
        {
            POLYMARKET_VENUE_ID: PolymarketHealthAdapter(),
            LIMITLESS_VENUE_ID: LimitlessHealthAdapter(),
            PREDICT_VENUE_ID: PredictHealthAdapter(),
        }
    )
    try:
        async with ArbitrageRuntime(
            venue_health_service=venue_health_service,
        ) as runtime:
            app.state.market_matcher = runtime.market_matcher
            app.state.market_worker = runtime
            app.state.arbitrage_runtime = runtime
            app.state.execution_runs = runtime.execution_runs
            app.state.venue_health_service = venue_health_service
            yield
    finally:
        await venue_health_service.close()


app = FastAPI(lifespan=lifespan)

app.include_router(candidates.runtime_router)
app.include_router(system.public_router)
app.include_router(system.runtime_router)
app.include_router(trading_activity.runtime_router)
app.include_router(trading_runs.router)
app.include_router(websocket.router)
