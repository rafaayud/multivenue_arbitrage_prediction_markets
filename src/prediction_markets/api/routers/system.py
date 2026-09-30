"""Expose the system HTTP and WebSocket endpoints.

Responsibilities
----------------
- Validate transport inputs, invoke application services, and map results to API responses.
"""

import os
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Response
import psycopg
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from prediction_markets.api.dependencies import (
    get_arbitrage_runtime,
    get_venue_health_service,
)
from prediction_markets.api.models import (
    SignalSettings,
    VenueHealthOut,
    VenueHealthReportOut,
)
from prediction_markets.api.runtime import ArbitrageRuntime
from prediction_markets.application.venue_health import VenueHealthService

public_router = APIRouter()
runtime_router = APIRouter()
router = APIRouter()


@public_router.get("/health")
async def health_check():
    """Report that the API process is accepting requests.

    Returns
    -------
    dict[str, str]
        A static health acknowledgement.
    """
    return {"message": "OK"}


@public_router.get("/venue-health", response_model=VenueHealthReportOut)
async def venue_health(
    service: Annotated[VenueHealthService, Depends(get_venue_health_service)],
) -> VenueHealthReportOut:
    """Return cached health checks for every configured trading venue.

    Parameters
    ----------
    service
        Lifecycle-owned service that checks venues concurrently outside the
        execution pipeline.

    Returns
    -------
    VenueHealthReportOut
        Overall status and one normalized observation per venue.
    """
    report = await service.get()
    return VenueHealthReportOut(
        generated_at=report.generated_at.value,
        overall_status=report.overall_status.value,
        venues=[
            VenueHealthOut(
                venue_id=str(venue.venue_id),
                status=venue.status.value,
                checked_at=venue.checked_at.value,
                latency_ms=venue.latency_ms,
                source=venue.source,
                message=venue.message,
                error_type=venue.error_type,
                http_status=venue.http_status,
                retryable=venue.retryable,
            )
            for venue in report.venues
        ],
    )


@runtime_router.get("/runtime/status")
def runtime_status(
    runtime: Annotated[ArbitrageRuntime, Depends(get_arbitrage_runtime)],
):
    """Return whether the lifecycle-managed arbitrage worker is running."""
    return runtime.status()


@runtime_router.post("/runtime/start")
async def start_runtime(
    runtime: Annotated[ArbitrageRuntime, Depends(get_arbitrage_runtime)],
):
    """Start signal monitoring and return the resulting runtime state."""
    await runtime.start()
    return runtime.status()


@runtime_router.post("/runtime/stop")
async def stop_runtime(
    runtime: Annotated[ArbitrageRuntime, Depends(get_arbitrage_runtime)],
):
    """Stop signal monitoring and return the resulting runtime state."""
    await runtime.stop()
    return runtime.status()


@runtime_router.put("/runtime/signal-settings")
async def update_signal_settings(
    body: SignalSettings,
    runtime: Annotated[ArbitrageRuntime, Depends(get_arbitrage_runtime)],
):
    """Update fee-aware opportunity thresholds while live trading is disabled.

    Parameters
    ----------
    body
        Validated minimum edge and cost buffer in USD per contract.
    runtime
        Lifecycle-owned arbitrage runtime to reconfigure.

    Returns
    -------
    dict[str, object]
        Runtime status containing the applied signal settings.

    Raises
    ------
    HTTPException
        With status 409 when live trading is enabled or the runtime is unavailable.
    """
    try:
        return runtime.configure_signals(body.min_net_edge, body.cost_buffer)
    except RuntimeError as error:
        raise HTTPException(status_code=409, detail=str(error)) from error


@public_router.get("/metrics")
def metrics():
    """Expose the current Prometheus registry in text format.

    Returns
    -------
    Response
        Prometheus metrics with the standard content type.
    """
    return Response(generate_latest(), headers={"Content-Type": CONTENT_TYPE_LATEST})


@public_router.get("/ready")
def readiness_check():
    """Verify that the configured PostgreSQL database accepts a query.

    Returns
    -------
    dict[str, str]
        A readiness acknowledgement.

    Raises
    ------
    HTTPException
        With status 503 when the DSN is absent or PostgreSQL is unavailable.
    """
    dsn = os.getenv("DATABASE_URL") or os.getenv("POSTGRES_DSN")
    if not dsn:
        raise HTTPException(
            status_code=503,
            detail="DATABASE_URL or POSTGRES_DSN is not configured",
        )

    try:
        with psycopg.connect(
            dsn,
            connect_timeout=5,
        ) as connection:
            connection.execute("SELECT 1")
    except psycopg.Error:
        raise HTTPException(
            status_code=503,
            detail="Failed to connect to the database",
        )

    return {"message": "OK"}


router.include_router(public_router)
router.include_router(runtime_router)
