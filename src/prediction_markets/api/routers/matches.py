"""Expose the matches HTTP and WebSocket endpoints.

Responsibilities
----------------
- Validate transport inputs, invoke application services, and map results to API responses.
"""

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query

from prediction_markets.application.markets.models import monitored_market_key
from prediction_markets.api.dependencies import get_trading_activity_reader
from prediction_markets.api.models import (
    MarketMatchesResponse,
    MonitoredMarketMatchesOut,
)
from prediction_markets.domain.market_matching.value_objects import Underlying
from prediction_markets.api.runtime import MONITORED_MARKET_CYCLES
from prediction_markets.api.trading.activity import TradingActivityReader


router = APIRouter(tags=["matches"])


@router.get("/market-matches/all", response_model=list[MonitoredMarketMatchesOut])
def all_market_matches(
    reader: Annotated[TradingActivityReader, Depends(get_trading_activity_reader)],
) -> list[MonitoredMarketMatchesOut]:
    """Return the latest pairs for every configured monitored cycle.

    Parameters
    ----------
    reader
        PostgreSQL read-model accessor populated from the durable journal.

    Returns
    -------
    list[MonitoredMarketMatchesOut]
        One entry for each configured cycle, including cycles with no current
        matched pairs.
    """
    matches_by_cycle = reader.market_matches_by_cycle()
    return [
        MonitoredMarketMatchesOut(
            monitor_key=monitored_market_key(cycle),
            family=cycle.family.value,
            underlying=cycle.underlying.symbol,
            interval_seconds=cycle.interval_seconds,
            pairs=matches_by_cycle.get(
                (cycle.underlying.symbol, cycle.interval_seconds),
                [],
            ),
        )
        for cycle in MONITORED_MARKET_CYCLES.values()
    ]


@router.get("/market-matches", response_model=MarketMatchesResponse)
def market_matches(
    reader: Annotated[TradingActivityReader, Depends(get_trading_activity_reader)],
    underlying: str = Query(min_length=1),
    interval_seconds: int = Query(gt=0)) -> MarketMatchesResponse:
    """
    Return the latest durable matched-contract projection for a monitored cycle.

    Parameters
    ----------
    reader
        PostgreSQL read-model accessor populated from the durable journal.
    underlying
        Symbol identifying the monitored underlying.
    interval_seconds
        Positive interval included in the worker's monitored cycles.

    Returns
    -------
    MarketMatchesResponse
        JSON-compatible match response containing the current pair snapshot.

    Raises
    ------
    HTTPException
        With status 422 when the underlying or cycle is unsupported.
    """
    try:
        normalized_underlying = Underlying(underlying)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    if (normalized_underlying, interval_seconds) not in MONITORED_MARKET_CYCLES:
        raise HTTPException(status_code=422, detail="Market cycle is not monitored")

    return MarketMatchesResponse(
        underlying=normalized_underlying.symbol,
        interval_seconds=interval_seconds,
        pairs=reader.market_matches(
            underlying=normalized_underlying.symbol,
            interval_seconds=interval_seconds,
        ),
    )
