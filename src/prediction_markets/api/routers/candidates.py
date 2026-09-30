"""Expose arbitrage candidate queries through HTTP.

Responsibilities
----------------
- Validate candidate-listing query parameters.
- Invoke the application query service.
- Translate normalized domain candidates into API response models.
- Explicitly add selected candidates to runtime monitoring.

Notes
-----
- Listing is read-only; monitoring is an authenticated control-path action.
"""

from datetime import timedelta
from decimal import Decimal
from typing import Annotated

import httpx
from fastapi import APIRouter, Depends, HTTPException, Query

from prediction_markets.api.auth import require_trading_key
from prediction_markets.api.dependencies import (
    get_agg_market_explorer,
    get_arbitrage_candidate_service,
    get_arbitrage_runtime,
)
from prediction_markets.api.models import (
    ArbitrageCandidateOut,
    ArbitrageVenueMarketOut,
    RegularMarketMonitorIn,
    RegularMarketMonitorOut,
)
from prediction_markets.api.runtime import ArbitrageRuntime
from prediction_markets.application.markets.arbitrage_candidates import (
    ArbitrageCandidateQuery,
    ListArbitrageCandidates,
)
from prediction_markets.application.markets.models import (
    RegularMarketSelection,
    monitored_market_key,
)
from prediction_markets.domain.ports.arbitrage_stream import ArbitrageReturn
from prediction_markets.domain.shared.value_objects import VenueID
from prediction_markets.infrastructure.agg.market_explorer import (
    AggMarketExplorerAdapter,
)

query_router = APIRouter(tags=["candidates"])
runtime_router = APIRouter(tags=["candidates"])
router = APIRouter()


def _candidate_response(candidate: ArbitrageReturn) -> ArbitrageCandidateOut:
    """Translate one normalized candidate into its HTTP response model."""
    return ArbitrageCandidateOut(
        market_id=str(candidate.market_id),
        title=candidate.title,
        event_title=candidate.event_title,
        venue_event_id=(
            str(candidate.venue_event_id) if candidate.venue_event_id is not None else None
        ),
        return_rate=candidate.return_rate,
        observed_at=candidate.observed_at.value,
        starts_at=(
            candidate.starts_at.value if candidate.starts_at is not None else None
        ),
        ends_at=(candidate.ends_at.value if candidate.ends_at is not None else None),
        volume_usd=candidate.volume_usd,
        liquidity_usd=candidate.liquidity_usd,
        liquidity_tier=candidate.liquidity_tier,
        markets=[
            ArbitrageVenueMarketOut(
                venue_id=str(market.venue_id),
                market_id=str(market.market_id),
                external_market_id=market.external_market_id,
                yes_outcome_id=str(market.yes_outcome_id),
                no_outcome_id=str(market.no_outcome_id),
                title=market.title,
                volume_usd=market.volume_usd,
            )
            for market in candidate.markets
        ],
    )


@query_router.get(
    "/arbitrage-candidates",
    response_model=list[ArbitrageCandidateOut],
)
async def list_arbitrage_candidates(
    service: Annotated[
        ListArbitrageCandidates,
        Depends(get_arbitrage_candidate_service),
    ],
    min_return: Annotated[Decimal, Query(ge=0)] = Decimal("0"),
    limit: Annotated[int, Query(ge=1, le=500)] = 100,
    topics: Annotated[list[str] | None, Query()] = None,
    search_text: Annotated[str | None, Query(min_length=1)] = None,
    live_only: Annotated[bool, Query()] = False,
    min_time_to_close_seconds: Annotated[int | None, Query(ge=0)] = None,
    max_time_to_close_seconds: Annotated[int | None, Query(gt=0)] = None,
) -> list[ArbitrageCandidateOut]:
    """Return normalized open arbitrage candidates.

    Parameters
    ----------
    service
        Application query service backed by the configured candidate source.
    min_return
        Minimum decimal return rate, where ``0.01`` represents one percent.
    limit
        Maximum number of candidates to return.
    topics
        Optional repeated topic filters, for example ``?topics=crypto``.
    search_text
        Optional case-insensitive text required in the event payload.
    live_only
        Whether events must have started and not yet ended.
    min_time_to_close_seconds
        Optional minimum remaining market lifetime in seconds.
    max_time_to_close_seconds
        Optional maximum remaining market lifetime in seconds.

    Returns
    -------
    list[ArbitrageCandidateOut]
        JSON-safe candidates and their selectable venue markets.

    Raises
    ------
    HTTPException
        With status 422 for invalid filters, 502 for an upstream failure, or
        503 when the candidate source is not configured.
    """
    if (
        min_time_to_close_seconds is not None
        and max_time_to_close_seconds is not None
        and min_time_to_close_seconds > max_time_to_close_seconds
    ):
        raise HTTPException(
            status_code=422,
            detail="min_time_to_close_seconds cannot exceed max_time_to_close_seconds",
        )

    try:
        candidates = await service.execute(
            ArbitrageCandidateQuery(
                min_return=min_return,
                limit=limit,
                topics=tuple(topics or ()),
                search_text=search_text,
                live_only=live_only,
                min_time_to_close=(
                    timedelta(seconds=min_time_to_close_seconds)
                    if min_time_to_close_seconds is not None
                    else None
                ),
                max_time_to_close=(
                    timedelta(seconds=max_time_to_close_seconds)
                    if max_time_to_close_seconds is not None
                    else None
                ),
            ),
        )
    except ValueError as error:
        status_code = 503 if "requires app_id" in str(error) else 422
        raise HTTPException(status_code=status_code, detail=str(error)) from error
    except (httpx.HTTPError, TypeError) as error:
        raise HTTPException(status_code=502, detail=str(error)) from error

    return [_candidate_response(candidate) for candidate in candidates]


@query_router.get(
    "/market-catalog",
    response_model=list[ArbitrageCandidateOut],
)
async def list_market_catalog(
    explorer: Annotated[
        AggMarketExplorerAdapter,
        Depends(get_agg_market_explorer),
    ],
    limit: Annotated[int, Query(ge=1, le=500)] = 500,
    topics: Annotated[list[str] | None, Query()] = None,
    search_text: Annotated[str | None, Query(min_length=1)] = None,
    live_only: Annotated[bool, Query()] = False,
) -> list[ArbitrageCandidateOut]:
    """Return open AGG markets without requiring a current opportunity.

    Parameters
    ----------
    explorer
        Lifecycle-owned AGG catalog adapter.
    limit
        Maximum number of normalized market groups returned.
    topics
        Optional repeated AGG event categories.
    search_text
        Optional case-insensitive event search text.
    live_only
        Whether events must be inside their reported game window.

    Returns
    -------
    list[ArbitrageCandidateOut]
        Selectable market groups including zero-return and unmatched entries.

    Raises
    ------
    HTTPException
        With status 422 for invalid filters, 502 for an upstream failure, or
        503 when AGG is not configured.
    """
    try:
        markets = await explorer.list_markets(
            limit=limit,
            topics=tuple(topics or ()),
            search_text=search_text,
            live_only=live_only,
        )
    except ValueError as error:
        status_code = 503 if "requires app_id" in str(error) else 422
        raise HTTPException(status_code=status_code, detail=str(error)) from error
    except (httpx.HTTPError, TypeError) as error:
        raise HTTPException(status_code=502, detail=str(error)) from error

    return [_candidate_response(market) for market in markets]


@runtime_router.post(
    "/arbitrage-candidates/monitor",
    response_model=RegularMarketMonitorOut,
    dependencies=[Depends(require_trading_key)],
)
async def monitor_arbitrage_candidate(
    body: RegularMarketMonitorIn,
    runtime: Annotated[ArbitrageRuntime, Depends(get_arbitrage_runtime)],
) -> RegularMarketMonitorOut:
    """Resolve and subscribe one explicitly selected AGG candidate.

    Parameters
    ----------
    body
        Native venue market identifiers returned by candidate discovery.
    runtime
        Lifecycle-owned discovery and market-data runtime.

    Returns
    -------
    RegularMarketMonitorOut
        Stable monitor identity and number of complementary pairs subscribed.

    Raises
    ------
    HTTPException
        With status 422 when selections cannot form a regular candidate, or
        when a selected venue market is no longer available; 502 when native
        venue discovery fails.
    """
    try:
        candidate, pair_count = await runtime.monitor_regular(
            tuple(
                RegularMarketSelection(
                    VenueID(market.venue_id),
                    market.external_market_id,
                    market.search_text,
                )
                for market in body.markets
            ),
        )
    except ValueError as error:
        raise HTTPException(status_code=422, detail=str(error)) from error
    except LookupError as error:
        raise HTTPException(
            status_code=422,
            detail=f"Selected market is no longer available: {error}",
        ) from error
    except httpx.HTTPError as error:
        raise HTTPException(status_code=502, detail=str(error)) from error
    return RegularMarketMonitorOut(
        monitor_key=monitored_market_key(candidate),
        pair_count=pair_count,
        venue_ids=tuple(str(market.venue_id) for market in candidate.markets),
    )


@runtime_router.delete(
    "/arbitrage-candidates/monitor",
    dependencies=[Depends(require_trading_key)],
)
async def unmonitor_arbitrage_candidate(
    monitor_key: Annotated[str, Query(min_length=1)],
    runtime: Annotated[ArbitrageRuntime, Depends(get_arbitrage_runtime)],
) -> dict[str, object]:
    """Stop native market-data monitoring for one regular candidate.

    Parameters
    ----------
    monitor_key
        Stable key returned when the candidate was added to monitoring.
    runtime
        Lifecycle-owned discovery and market-data runtime.

    Returns
    -------
    dict[str, object]
        Updated runtime state after subscriptions are removed.

    Raises
    ------
    HTTPException
        With status 404 when the candidate is not currently monitored, or 409
        while it owns an unfinished execution.
    """
    try:
        removed = await runtime.unmonitor_regular(monitor_key)
    except ValueError as error:
        raise HTTPException(status_code=409, detail=str(error)) from error
    if not removed:
        raise HTTPException(status_code=404, detail="Regular market is not monitored")
    return runtime.status()


router.include_router(query_router)
router.include_router(runtime_router)
