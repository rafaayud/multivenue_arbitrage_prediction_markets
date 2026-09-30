"""Provide shared pytest fixtures for api tests.

Responsibilities
----------------
- Build reusable test dependencies and representative inputs.
"""

import pytest

from prediction_markets.api.dependencies import get_venue_health_service
from prediction_markets.api.control_main import app as control_app
from prediction_markets.api.trading_main import app as trading_app
from prediction_markets.application.venue_health import VenueHealthReport
from prediction_markets.domain.shared.value_objects import Timestamp, VenueID
from prediction_markets.domain.venue_health import (
    VenueHealthSnapshot,
    VenueHealthStatus,
)


class _HealthyVenueService:
    async def get(self, *, force_refresh: bool = False) -> VenueHealthReport:
        now = Timestamp.now()
        return VenueHealthReport(
            generated_at=now,
            overall_status=VenueHealthStatus.OPERATIONAL,
            venues=(
                VenueHealthSnapshot(
                    venue_id=VenueID("TEST"),
                    status=VenueHealthStatus.OPERATIONAL,
                    checked_at=now,
                    latency_ms=1,
                    source="test",
                    message="reachable",
                ),
            ),
        )


@pytest.fixture(autouse=True)
def disable_live_market_worker(monkeypatch) -> None:
    monkeypatch.setenv("MARKET_WORKER_ENABLED", "0")


@pytest.fixture(autouse=True)
def healthy_venue_service():
    """Avoid external venue I/O in API tests unrelated to venue health."""
    for app in (control_app, trading_app):
        app.dependency_overrides[get_venue_health_service] = _HealthyVenueService
    yield
    for app in (control_app, trading_app):
        app.dependency_overrides.pop(get_venue_health_service, None)
