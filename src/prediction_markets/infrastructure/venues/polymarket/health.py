"""Check Polymarket CLOB reachability and its official component status."""

import asyncio
from time import perf_counter
from typing import Any

from prediction_markets.domain.ports.venue_health import VenueHealthPort
from prediction_markets.domain.shared.value_objects import Timestamp
from prediction_markets.domain.venue_health import (
    VenueHealthSnapshot,
    VenueHealthStatus,
)
from prediction_markets.infrastructure.http_client import instrumented_async_client
from prediction_markets.infrastructure.venues.polymarket.mappers import (
    POLYMARKET_VENUE_ID,
)


class PolymarketHealthAdapter(VenueHealthPort):
    """Check the public CLOB API and its specific official component."""

    def __init__(
        self,
        *,
        clob_url: str = "https://clob.polymarket.com",
        components_url: str = "https://status.polymarket.com/v3/components.json",
        timeout_seconds: float = 5.0,
        client: Any | None = None,
    ) -> None:
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        self._clob_url = clob_url.rstrip("/")
        self._components_url = components_url
        self._owns_client = client is None
        self._client = client or instrumented_async_client(
            "polymarket",
            timeout=timeout_seconds,
        )

    async def check(self) -> VenueHealthSnapshot:
        """Combine direct CLOB reachability with its official component state."""
        started = perf_counter()
        response, components_response = await asyncio.gather(
            self._client.get(f"{self._clob_url}/time"),
            self._client.get(self._components_url),
            return_exceptions=True,
        )
        if isinstance(response, BaseException):
            raise response
        response.raise_for_status()
        try:
            if isinstance(components_response, BaseException):
                raise components_response
            components_response.raise_for_status()
            components_payload = components_response.json()
            if not isinstance(components_payload, dict):
                raise TypeError("Unexpected Polymarket components response")
            clob_status = next(
                (
                    str(component.get("status") or "").upper()
                    for component in components_payload.get("components", ())
                    if isinstance(component, dict)
                    and "TRADING API (CLOB)"
                    in str(component.get("name", "")).upper()
                ),
                "",
            )
            if not clob_status:
                raise LookupError("Trading API (CLOB) component is missing")
        except Exception as error:
            return self._snapshot(
                started,
                VenueHealthStatus.OPERATIONAL,
                "CLOB reachable; official CLOB component unavailable: "
                f"{str(error) or type(error).__name__}",
                http_status=response.status_code,
            )
        status = (
            VenueHealthStatus.UNAVAILABLE
            if "MAJOROUTAGE" in clob_status
            else VenueHealthStatus.OPERATIONAL
            if clob_status in {"UP", "OPERATIONAL"}
            else VenueHealthStatus.DEGRADED
        )
        message = f"Trading API (CLOB): {clob_status}"
        return self._snapshot(
            started,
            status,
            message,
            error_type=(
                "OfficialMajorOutage"
                if status is VenueHealthStatus.UNAVAILABLE
                else "OfficialDegradation"
                if status is VenueHealthStatus.DEGRADED
                else None
            ),
            http_status=components_response.status_code,
            retryable=status is not VenueHealthStatus.OPERATIONAL,
        )

    async def close(self) -> None:
        """Close the owned HTTP client."""
        if self._owns_client:
            await self._client.aclose()

    @staticmethod
    def _snapshot(
        started: float,
        status: VenueHealthStatus,
        message: str,
        *,
        error_type: str | None = None,
        http_status: int | None = None,
        retryable: bool = False,
    ) -> VenueHealthSnapshot:
        """Build one normalized Polymarket observation."""
        return VenueHealthSnapshot(
            venue_id=POLYMARKET_VENUE_ID,
            status=status,
            checked_at=Timestamp.now(),
            latency_ms=round((perf_counter() - started) * 1_000, 1),
            source="CLOB API + official status",
            message=message,
            error_type=error_type,
            http_status=http_status,
            retryable=retryable,
        )
