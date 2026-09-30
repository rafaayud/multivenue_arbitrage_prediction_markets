"""Check Predict REST availability with one bounded market-list request."""

from time import perf_counter
from typing import Any

from prediction_markets.domain.ports.venue_health import VenueHealthPort
from prediction_markets.domain.shared.value_objects import Timestamp
from prediction_markets.domain.venue_health import (
    VenueHealthSnapshot,
    VenueHealthStatus,
)
from prediction_markets.infrastructure.http_client import instrumented_async_client
from prediction_markets.infrastructure.venues.predict.config import (
    predict_api_key,
    predict_headers,
)
from prediction_markets.infrastructure.venues.predict.mappers import PREDICT_VENUE_ID


class PredictHealthAdapter(VenueHealthPort):
    """Check the authenticated Predict markets API used by discovery."""

    def __init__(
        self,
        *,
        api_key: str | None = None,
        base_url: str = "https://api.predict.fun",
        timeout_seconds: float = 5.0,
        client: Any | None = None,
    ) -> None:
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        self._api_key = predict_api_key(api_key)
        self._base_url = base_url.rstrip("/")
        self._owns_client = client is None
        self._client = client or instrumented_async_client(
            "predict",
            timeout=timeout_seconds,
        )

    async def check(self) -> VenueHealthSnapshot:
        """Read one Predict market page and expose rate limiting explicitly."""
        if not self._api_key:
            raise RuntimeError("PREDICT_API_KEY is not configured")
        started = perf_counter()
        response = await self._client.get(
            f"{self._base_url}/v1/markets",
            params={"first": 1},
            headers=predict_headers(self._api_key),
        )
        if response.status_code == 429:
            return VenueHealthSnapshot(
                venue_id=PREDICT_VENUE_ID,
                status=VenueHealthStatus.DEGRADED,
                checked_at=Timestamp.now(),
                latency_ms=round((perf_counter() - started) * 1_000, 1),
                source="Predict Markets API",
                message="Rate limited (HTTP 429)",
                error_type="RateLimitError",
                http_status=429,
                retryable=True,
            )
        response.raise_for_status()
        return VenueHealthSnapshot(
            venue_id=PREDICT_VENUE_ID,
            status=VenueHealthStatus.OPERATIONAL,
            checked_at=Timestamp.now(),
            latency_ms=round((perf_counter() - started) * 1_000, 1),
            source="Predict Markets API",
            message="Markets API reachable",
            http_status=response.status_code,
        )

    async def close(self) -> None:
        """Close the owned HTTP client."""
        if self._owns_client:
            await self._client.aclose()
