"""Expose the stateless HTTP and database-backed control plane.

Responsibilities
----------------
- Serve public queries, alerting controls, and projected trading activity.
- Keep venue discovery queries and dashboard polling outside the trading process.
"""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
import logging

from dotenv import load_dotenv
from fastapi import FastAPI

from prediction_markets.api.routers import (
    alertmanager_webhook,
    alerting,
    candidates,
    matches,
    system,
    trading_activity,
)
from prediction_markets.application.markets.arbitrage_candidates import (
    ListArbitrageCandidates,
)
from prediction_markets.application.pnl import PnlService
from prediction_markets.application.venue_health import VenueHealthService
from prediction_markets.infrastructure.agg.arbitrage_stream import (
    AggArbitrageStreamAdapter,
)
from prediction_markets.infrastructure.agg.market_explorer import (
    AggMarketExplorerAdapter,
)
from prediction_markets.infrastructure.venues.limitless.health import (
    LimitlessHealthAdapter,
)
from prediction_markets.infrastructure.venues.limitless.mappers import (
    LIMITLESS_VENUE_ID,
)
from prediction_markets.infrastructure.venues.limitless.pnl import LimitlessPnlAdapter
from prediction_markets.infrastructure.venues.polymarket.health import (
    PolymarketHealthAdapter,
)
from prediction_markets.infrastructure.venues.polymarket.mappers import (
    POLYMARKET_VENUE_ID,
)
from prediction_markets.infrastructure.venues.polymarket.pnl import PolymarketPnlAdapter
from prediction_markets.infrastructure.venues.predict.health import PredictHealthAdapter
from prediction_markets.infrastructure.venues.predict.mappers import PREDICT_VENUE_ID
from prediction_markets.infrastructure.venues.predict.pnl import PredictPnlAdapter
from prediction_markets.utils.decorators.logger import configure_logging


load_dotenv()
configure_logging(logging.WARNING)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Own only services used by control-plane requests.

    Parameters
    ----------
    app
        Control-plane application receiving the initialized services.

    Yields
    ------
    None
        Control while the application is serving requests.
    """
    pnl_service = PnlService(
        {
            POLYMARKET_VENUE_ID: PolymarketPnlAdapter(),
            LIMITLESS_VENUE_ID: LimitlessPnlAdapter(),
            PREDICT_VENUE_ID: PredictPnlAdapter(),
        }
    )
    venue_health_service = VenueHealthService(
        {
            POLYMARKET_VENUE_ID: PolymarketHealthAdapter(),
            LIMITLESS_VENUE_ID: LimitlessHealthAdapter(),
            PREDICT_VENUE_ID: PredictHealthAdapter(),
        }
    )
    app.state.arbitrage_candidate_service = ListArbitrageCandidates(
        AggArbitrageStreamAdapter()
    )
    app.state.agg_market_explorer = AggMarketExplorerAdapter()
    app.state.pnl_service = pnl_service
    app.state.venue_health_service = venue_health_service
    try:
        yield
    finally:
        await pnl_service.close()
        await venue_health_service.close()


app = FastAPI(lifespan=lifespan)

app.include_router(alertmanager_webhook.router)
app.include_router(alerting.router)
app.include_router(candidates.query_router)
app.include_router(matches.router)
app.include_router(system.public_router)
app.include_router(trading_activity.query_router)
