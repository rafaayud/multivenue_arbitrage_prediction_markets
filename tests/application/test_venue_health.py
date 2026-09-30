"""Verify concurrent, cached venue-health aggregation."""

import asyncio

from prediction_markets.application.venue_health import VenueHealthService
from prediction_markets.domain.ports.venue_health import VenueHealthPort
from prediction_markets.domain.shared.value_objects import Timestamp, VenueID
from prediction_markets.domain.venue_health import (
    VenueHealthSnapshot,
    VenueHealthStatus,
)


class _Adapter(VenueHealthPort):
    def __init__(self, venue_id: VenueID, *, fails: bool = False) -> None:
        self.venue_id = venue_id
        self.fails = fails
        self.calls = 0

    async def check(self) -> VenueHealthSnapshot:
        self.calls += 1
        if self.fails:
            raise RuntimeError("venue timed out")
        return VenueHealthSnapshot(
            venue_id=self.venue_id,
            status=VenueHealthStatus.OPERATIONAL,
            checked_at=Timestamp.now(),
            latency_ms=12.5,
            source="test",
            message="reachable",
        )


def test_service_caches_partial_health_without_failing_the_report() -> None:
    """Keep successful venues visible when one adapter fails."""

    async def run() -> None:
        healthy = _Adapter(VenueID("HEALTHY"))
        failing = _Adapter(VenueID("FAILING"), fails=True)
        service = VenueHealthService(
            {healthy.venue_id: healthy, failing.venue_id: failing},
            cache_ttl_seconds=30,
        )

        first, second = await asyncio.gather(service.get(), service.get())
        refreshed = await service.get(force_refresh=True)

        assert healthy.calls == failing.calls == 2
        assert first is second
        assert refreshed is not first
        assert first.overall_status is VenueHealthStatus.DEGRADED
        assert [venue.status for venue in first.venues] == [
            VenueHealthStatus.UNAVAILABLE,
            VenueHealthStatus.OPERATIONAL,
        ]
        assert first.venues[0].message == "venue timed out"
        assert first.venues[0].error_type == "RuntimeError"
        assert first.venues[0].http_status is None
        assert first.venues[0].retryable is False
        assert first.venues[0].latency_ms is not None

    asyncio.run(run())
