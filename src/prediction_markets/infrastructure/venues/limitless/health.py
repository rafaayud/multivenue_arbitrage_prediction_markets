"""Check Limitless REST availability with its public active-market resource."""

from time import perf_counter
from typing import Any

from prediction_markets.domain.ports.venue_health import VenueHealthPort
from prediction_markets.domain.shared.value_objects import Timestamp
from prediction_markets.domain.venue_health import (
    VenueHealthSnapshot,
    VenueHealthStatus,
)
from prediction_markets.infrastructure.http_client import instrumented_async_client
from prediction_markets.infrastructure.venues.limitless.mappers import (
    LIMITLESS_VENUE_ID,
)


class LimitlessHealthAdapter(VenueHealthPort):
    """Check the public Limitless endpoint used by market discovery."""

    def __init__(
        self,
        *,
        base_url: str = "https://api.limitless.exchange",
        timeout_seconds: float = 5.0,
        client: Any | None = None,
    ) -> None:
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        self._base_url = base_url.rstrip("/")
        self._owns_client = client is None
        self._client = client or instrumented_async_client(
            "limitless",
            timeout=timeout_seconds,
        )

    async def check(self) -> VenueHealthSnapshot:
        """Read active market slugs and return normalized availability."""
        started = perf_counter()
        response = await self._client.get(
            f"{self._base_url}/markets/active/slugs",
        )
        response.raise_for_status()
        return VenueHealthSnapshot(
            venue_id=LIMITLESS_VENUE_ID,
            status=VenueHealthStatus.OPERATIONAL,
            checked_at=Timestamp.now(),
            latency_ms=round((perf_counter() - started) * 1_000, 1),
            source="Limitless active markets API",
            message="Markets API reachable",
            http_status=response.status_code,
        )

    async def close(self) -> None:
        """Close the owned HTTP client."""
        if self._owns_client:
            await self._client.aclose()
