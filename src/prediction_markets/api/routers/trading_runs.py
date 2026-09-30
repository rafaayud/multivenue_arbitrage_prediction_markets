"""Expose the trading runs HTTP and WebSocket endpoints.

Responsibilities
----------------
- Validate transport inputs, invoke application services, and map results to API responses.
"""

import secrets
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Request, Response, status

from prediction_markets.api.auth import (
    TRADING_COOKIE,
    configured_trading_key,
    require_trading_key,
    trading_session_token,
)
from prediction_markets.api.dependencies import (
    get_execution_run_manager,
    get_venue_health_service,
)
from prediction_markets.api.models import (
    TradingPreflightOut,
    TradingRunOut,
    TradingRunStart,
    TradingSessionStart,
)
from prediction_markets.api.trading.run_manager import (
    ExecutionRun,
    ExecutionRunManager,
    PreflightError,
)
from prediction_markets.api.trading.runner import LiveArbitrageConfig
from prediction_markets.application.venue_health import (
    VenueHealthReport,
    VenueHealthService,
)
from prediction_markets.domain.market_matching.value_objects import Underlying
from prediction_markets.domain.venue_health import VenueHealthStatus

router = APIRouter(
    prefix="/trading-runs",
    tags=["trading-runs"],
)


def _response(run: ExecutionRun) -> TradingRunOut:
    """Map the process-local execution run to its HTTP response model."""
    return TradingRunOut(
        id=run.id,
        status=run.status,
        started_at=run.started_at,
        finished_at=run.finished_at,
        error=run.error,
        short_market_keys=run.config.short_market_keys,
    )


def _venue_health_issues(report: VenueHealthReport) -> list[str]:
    """Format unhealthy venue observations for preflight and start errors."""
    issues = []
    for venue in report.venues:
        if venue.status is VenueHealthStatus.OPERATIONAL:
            continue
        details = [venue.error_type]
        if venue.http_status is not None:
            details.append(f"HTTP {venue.http_status}")
        details.append("retryable" if venue.retryable else "not retryable")
        metadata = ", ".join(detail for detail in details if detail)
        issues.append(
            f"{venue.venue_id}: {venue.status.value} - {venue.message} ({metadata})"
        )
    return issues


@router.post("/session")
def create_trading_session(
    body: TradingSessionStart,
    request: Request,
    response: Response,
) -> dict[str, bool]:
    """Exchange a valid trading key for a scoped HttpOnly session cookie."""
    expected = configured_trading_key()
    if not secrets.compare_digest(body.trading_key, expected):
        raise HTTPException(status_code=401, detail="Invalid trading API key")
    response.set_cookie(
        TRADING_COOKIE,
        trading_session_token(expected),
        httponly=True,
        secure=request.url.scheme == "https",
        samesite="strict",
        path="/",
    )
    return {"authenticated": True}


@router.get(
    "/session",
    dependencies=[Depends(require_trading_key)],
)
def get_trading_session() -> dict[str, bool]:
    """Confirm that the current request has a valid trading session."""
    return {"authenticated": True}


@router.post(
    "/preflight",
    response_model=TradingPreflightOut,
    dependencies=[Depends(require_trading_key)],
)
async def trading_preflight(
    manager: Annotated[
        ExecutionRunManager,
        Depends(get_execution_run_manager),
    ],
    venue_health: Annotated[
        VenueHealthService,
        Depends(get_venue_health_service),
    ],
) -> TradingPreflightOut:
    """Evaluate dependencies and venue health before live trading.

    Returns
    -------
    TradingPreflightOut
        The current readiness report.
    """
    report = await manager.preflight()
    health = await venue_health.get()
    report["ready"] = bool(report["ready"]) and (
        health.overall_status is VenueHealthStatus.OPERATIONAL
    )
    report["venue_health_status"] = health.overall_status.value
    report["venue_health_issues"] = _venue_health_issues(health)
    return TradingPreflightOut.model_validate(report)


@router.post(
    "",
    response_model=TradingRunOut,
    status_code=status.HTTP_202_ACCEPTED,
    dependencies=[Depends(require_trading_key)],
)
async def start_trading_run(
    body: TradingRunStart,
    manager: Annotated[
        ExecutionRunManager,
        Depends(get_execution_run_manager),
    ],
    venue_health: Annotated[
        VenueHealthService,
        Depends(get_venue_health_service),
    ],
) -> TradingRunOut:
    """Start one confirmed live trading run with validated risk limits.

    Parameters
    ----------
    body
        Confirmed live configuration supplied as JSON.

    Returns
    -------
    TradingRunOut
        The accepted process-local run state.

    Raises
    ------
    HTTPException
        With status 409 when a venue is unhealthy, preflight fails, or another
        run is active.
    """
    health = await venue_health.get(force_refresh=True)
    has_unavailable_venue = any(
        venue.status is VenueHealthStatus.UNAVAILABLE for venue in health.venues
    )
    degraded_without_override = (
        health.overall_status is VenueHealthStatus.DEGRADED
        and not body.allow_degraded_venues
    )
    if has_unavailable_venue or degraded_without_override:
        raise HTTPException(
            status_code=409,
            detail={
                "message": (
                    "Trading blocked because at least one venue is unavailable"
                    if has_unavailable_venue
                    else "Trading blocked because venue health is degraded"
                ),
                "venue_health_status": health.overall_status.value,
                "venue_health_issues": _venue_health_issues(health),
            },
        )

    config = LiveArbitrageConfig(
        underlyings=tuple(Underlying(value) for value in body.underlyings),
        intervals_seconds=body.intervals_seconds,
        max_arbitrages=body.max_arbitrages,
        max_concurrent_arbitrages=body.max_concurrent_arbitrages,
        polymarket_max_notional=body.polymarket_max_notional,
        limitless_max_notional=body.limitless_max_notional,
        predict_max_notional=body.predict_max_notional,
        predict_limit_slippage_ticks=body.predict_limit_slippage_ticks,
        predict_use_edge_budget=body.predict_use_edge_budget,
        min_net_edge=body.min_net_edge,
        cost_buffer=body.cost_buffer,
        max_recovery_loss=body.max_recovery_loss,
        short_market_keys=body.short_market_keys,
    )
    try:
        run = await manager.start(config)
    except PreflightError as error:
        raise HTTPException(status_code=409, detail=error.report) from error
    except RuntimeError as error:
        raise HTTPException(status_code=409, detail=str(error)) from error
    return _response(run)


@router.get("/current", response_model=TradingRunOut | None)
def get_current_trading_run(
    manager: Annotated[
        ExecutionRunManager,
        Depends(get_execution_run_manager),
    ],
) -> TradingRunOut | None:
    """Return the latest process-local run so the dashboard can restore its state."""
    run = manager.current()
    return _response(run) if run is not None else None


@router.get(
    "/{run_id}",
    response_model=TradingRunOut,
    dependencies=[Depends(require_trading_key)],
)
def get_trading_run(
    run_id: str,
    manager: Annotated[
        ExecutionRunManager,
        Depends(get_execution_run_manager),
    ],
) -> TradingRunOut:
    """Return the process-local state of one live trading run.

    Parameters
    ----------
    run_id
        Identifier returned when the run was started.

    Returns
    -------
    TradingRunOut
        The current run state.

    Raises
    ------
    HTTPException
        With status 404 when the identifier is unknown.
    """
    try:
        return _response(manager.status(run_id))
    except KeyError as error:
        raise HTTPException(status_code=404, detail="Trading run not found") from error


@router.post(
    "/{run_id}/stop",
    response_model=TradingRunOut,
    dependencies=[Depends(require_trading_key)],
)
async def stop_trading_run(
    run_id: str,
    manager: Annotated[
        ExecutionRunManager,
        Depends(get_execution_run_manager),
    ],
) -> TradingRunOut:
    """Request cooperative shutdown of one live trading run.

    Parameters
    ----------
    run_id
        Identifier returned when the run was started.

    Returns
    -------
    TradingRunOut
        The stopping or already terminal run state.

    Raises
    ------
    HTTPException
        With status 404 when the identifier is unknown.
    """
    try:
        return _response(await manager.stop(run_id))
    except KeyError as error:
        raise HTTPException(status_code=404, detail="Trading run not found") from error
