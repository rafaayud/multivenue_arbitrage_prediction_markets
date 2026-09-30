"""Verify the venue-health HTTP contract."""

from fastapi import FastAPI
from fastapi.testclient import TestClient

from prediction_markets.application.venue_health import VenueHealthReport
from prediction_markets.api.dependencies import get_venue_health_service
from prediction_markets.api.routers.system import router
from prediction_markets.domain.shared.value_objects import Timestamp, VenueID
from prediction_markets.domain.venue_health import (
    VenueHealthSnapshot,
    VenueHealthStatus,
)


class _Service:
    async def get(self) -> VenueHealthReport:
        now = Timestamp.now()
        return VenueHealthReport(
            generated_at=now,
            overall_status=VenueHealthStatus.DEGRADED,
            venues=(
                VenueHealthSnapshot(
                    venue_id=VenueID("POLYMARKET"),
                    status=VenueHealthStatus.UNAVAILABLE,
                    checked_at=now,
                    latency_ms=250.5,
                    source="official status",
                    message="Trading outage",
                    error_type="OfficialMajorOutage",
                    retryable=True,
                ),
            ),
        )


def test_venue_health_endpoint_returns_normalized_report() -> None:
    """Serialize application health values without requiring the live runtime."""
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_venue_health_service] = _Service

    response = TestClient(app).get("/venue-health")

    assert response.status_code == 200
    assert response.json()["overall_status"] == "degraded"
    assert response.json()["venues"][0] == {
        "venue_id": "POLYMARKET",
        "status": "unavailable",
        "checked_at": response.json()["generated_at"],
        "latency_ms": 250.5,
        "source": "official status",
        "message": "Trading outage",
        "error_type": "OfficialMajorOutage",
        "http_status": None,
        "retryable": True,
    }
