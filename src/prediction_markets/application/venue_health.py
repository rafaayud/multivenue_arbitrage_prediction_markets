"""Aggregate cached health checks from independently configured venues.

Responsibilities
----------------
- Check venue adapters concurrently under a bounded timeout.
- Deduplicate dashboard polling with a short process-local cache.
- Preserve per-venue failures instead of failing the whole report.
"""

import asyncio
from dataclasses import dataclass
from time import monotonic
from typing import Mapping

from prediction_markets.domain.ports.venue_health import VenueHealthPort
from prediction_markets.domain.shared.value_objects import Timestamp, VenueID
from prediction_markets.domain.venue_health import (
    VenueHealthSnapshot,
    VenueHealthStatus,
)


@dataclass(frozen=True, slots=True)
class VenueHealthReport:
    """Represent one consistent cross-venue health observation."""

    generated_at: Timestamp
    overall_status: VenueHealthStatus
    venues: tuple[VenueHealthSnapshot, ...]


class VenueHealthService:
    """Check a registry of venue adapters outside the execution pipeline.

    Parameters
    ----------
    adapters
        Health adapters keyed by venue identifier.
    cache_ttl_seconds
        Duration for which dashboard requests reuse one report.
    timeout_seconds
        Maximum duration allowed for each independent venue check.

    Notes
    -----
    - One process-local lock deduplicates refreshes. Venue checks themselves run
      concurrently and never share execution-adapter locks.
    """

    def __init__(
        self,
        adapters: Mapping[VenueID, VenueHealthPort],
        *,
        cache_ttl_seconds: float = 60.0,
        timeout_seconds: float = 6.0,
    ) -> None:
        if not adapters:
            raise ValueError("At least one venue health adapter is required")
        if cache_ttl_seconds < 0:
            raise ValueError("cache_ttl_seconds cannot be negative")
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        self._adapters = dict(adapters)
        self._cache_ttl_seconds = cache_ttl_seconds
        self._timeout_seconds = timeout_seconds
        self._lock = asyncio.Lock()
        self._cached_at = 0.0
        self._cached: VenueHealthReport | None = None

    async def get(self, *, force_refresh: bool = False) -> VenueHealthReport:
        """Return a cached or concurrently refreshed venue-health report.

        Parameters
        ----------
        force_refresh
            Whether to bypass the dashboard cache, for example before enabling
            live trading.
        """
        if not force_refresh:
            cached = self._fresh_cache()
            if cached is not None:
                return cached
        async with self._lock:
            if not force_refresh:
                cached = self._fresh_cache()
                if cached is not None:
                    return cached
            venue_ids = tuple(sorted(self._adapters, key=str))
            venues = tuple(
                await asyncio.gather(
                    *(self._check(venue_id) for venue_id in venue_ids),
                )
            )
            statuses = {venue.status for venue in venues}
            overall = (
                VenueHealthStatus.OPERATIONAL
                if statuses == {VenueHealthStatus.OPERATIONAL}
                else VenueHealthStatus.UNAVAILABLE
                if statuses == {VenueHealthStatus.UNAVAILABLE}
                else VenueHealthStatus.DEGRADED
            )
            report = VenueHealthReport(Timestamp.now(), overall, venues)
            self._cached_at = monotonic()
            self._cached = report
            return report

    async def close(self) -> None:
        """Close every configured venue adapter concurrently."""
        await asyncio.gather(
            *(adapter.close() for adapter in self._adapters.values()),
            return_exceptions=True,
        )

    async def _check(self, venue_id: VenueID) -> VenueHealthSnapshot:
        """Bound one adapter call and normalize failures."""
        adapter = self._adapters[venue_id]
        started_at = monotonic()
        try:
            snapshot = await asyncio.wait_for(
                adapter.check(),
                timeout=self._timeout_seconds,
            )
            if snapshot.venue_id != venue_id:
                raise ValueError(
                    f"Health adapter for {venue_id} returned {snapshot.venue_id}"
                )
            return snapshot
        except Exception as error:
            response = getattr(error, "response", None)
            http_status = getattr(response, "status_code", None)
            error_type = type(error).__name__
            return VenueHealthSnapshot(
                venue_id=venue_id,
                status=VenueHealthStatus.UNAVAILABLE,
                checked_at=Timestamp.now(),
                latency_ms=round((monotonic() - started_at) * 1_000, 1),
                source=type(adapter).__name__,
                message=str(error) or type(error).__name__,
                error_type=error_type,
                http_status=http_status,
                retryable=(
                    error_type.lower().endswith("timeout")
                    or isinstance(error, OSError)
                    or http_status == 429
                    or isinstance(http_status, int)
                    and http_status >= 500
                ),
            )

    def _fresh_cache(self) -> VenueHealthReport | None:
        """Return the process-local report while its TTL remains valid."""
        if (
            self._cached is None
            or monotonic() - self._cached_at > self._cache_ttl_seconds
        ):
            return None
        return self._cached
