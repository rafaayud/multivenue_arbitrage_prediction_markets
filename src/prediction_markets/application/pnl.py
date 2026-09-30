"""Aggregate account PnL from independently refreshable venue adapters.

Responsibilities
----------------
- Refresh configured venues concurrently.
- Deduplicate concurrent reads with one lock and cache per venue.
- Return partial or stale results without hiding venue failures.
"""

import asyncio
from collections import deque
from dataclasses import dataclass
from decimal import Decimal
from time import monotonic
from typing import Mapping

from prediction_markets.domain.ports.pnl import PnlPort, VenuePnlSnapshot
from prediction_markets.domain.shared.value_objects import Timestamp, VenueID


@dataclass(frozen=True, slots=True)
class PnlPoint:
    """Represent one transient venue-reported PnL observation."""

    observed_at: Timestamp
    total_pnl_usd: Decimal

    def __post_init__(self) -> None:
        if not self.total_pnl_usd.is_finite():
            raise ValueError("PnL point must be finite")


@dataclass(frozen=True, slots=True)
class VenuePnlResult:
    """Expose one successful, stale, or unavailable venue read.

    Attributes
    ----------
    venue_id
        Venue requested by the service registry.
    snapshot
        Latest available value, including a stale fallback when present.
    stale
        Whether the snapshot predates a failed refresh.
    error
        Refresh error when the latest attempt failed.
    """

    venue_id: VenueID
    snapshot: VenuePnlSnapshot | None
    stale: bool = False
    error: str | None = None


@dataclass(frozen=True, slots=True)
class ConsolidatedPnl:
    """Represent one cross-venue PnL read and its transient adapter history.

    Notes
    -----
    - Optional aggregate components remain unavailable when any included venue
      does not report that component.
    - ``partial`` identifies unavailable or stale venues.
    """

    generated_at: Timestamp
    realized_pnl_usd: Decimal | None
    unrealized_pnl_usd: Decimal | None
    gross_pnl_usd: Decimal | None
    total_pnl_usd: Decimal | None
    fees_usd: Decimal | None
    partial: bool
    venues: tuple[VenuePnlResult, ...]
    series: tuple[PnlPoint, ...]


class PnlService:
    """Refresh and aggregate a registry of venue PnL adapters.

    Parameters
    ----------
    adapters
        Adapter registry keyed by venue. Adding a venue requires only a new
        mapping entry implementing :class:`PnlPort`.
    cache_ttl_seconds
        Time during which concurrent dashboard requests reuse a venue snapshot.
    timeout_seconds
        Maximum duration allowed for each independent venue refresh.
    history_limit
        Maximum consolidated observations retained by this process.

    Notes
    -----
    - Network I/O runs concurrently across venues.
    - Each lock protects only one venue refresh; aggregation never holds locks.
    """

    def __init__(
        self,
        adapters: Mapping[VenueID, PnlPort],
        *,
        cache_ttl_seconds: float = 30.0,
        timeout_seconds: float = 10.0,
        history_limit: int = 500,
    ) -> None:
        if cache_ttl_seconds < 0:
            raise ValueError("cache_ttl_seconds cannot be negative")
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        if history_limit <= 0:
            raise ValueError("history_limit must be positive")
        self._adapters = dict(adapters)
        self._cache_ttl_seconds = cache_ttl_seconds
        self._timeout_seconds = timeout_seconds
        self._locks = {venue_id: asyncio.Lock() for venue_id in self._adapters}
        self._cache: dict[VenueID, tuple[float, VenuePnlResult]] = {}
        # Kept only in the adapter response for backwards compatibility. The
        # dashboard series is the PostgreSQL projection, not this cache.
        self._history: deque[PnlPoint] = deque(maxlen=history_limit)
        self._history_lock = asyncio.Lock()

    async def get(self) -> ConsolidatedPnl:
        """Return a concurrent cross-venue PnL snapshot.

        Returns
        -------
        ConsolidatedPnl
            Available venue values, explicit partial failures, and the rolling
            total series collected by this API process.
        """
        venues = tuple(sorted(self._adapters, key=str))
        results = tuple(
            await asyncio.gather(
                *(self._read_venue(venue_id) for venue_id in venues),
            )
        )
        snapshots = tuple(
            result.snapshot for result in results if result.snapshot is not None
        )
        total = (
            sum((snapshot.total_pnl_usd for snapshot in snapshots), Decimal("0"))
            if snapshots
            else None
        )
        if total is not None:
            observed_at = max(
                (snapshot.observed_at for snapshot in snapshots),
                key=lambda value: value.value,
            )
            async with self._history_lock:
                point = PnlPoint(observed_at=observed_at, total_pnl_usd=total)
                if not self._history or self._history[-1] != point:
                    self._history.append(point)
                series = tuple(self._history)
        else:
            series = tuple(self._history)
        fees = _complete_sum(
            tuple(snapshot.fees_usd for snapshot in snapshots),
        )
        return ConsolidatedPnl(
            generated_at=Timestamp.now(),
            realized_pnl_usd=_complete_sum(
                tuple(snapshot.realized_pnl_usd for snapshot in snapshots),
            ),
            unrealized_pnl_usd=_complete_sum(
                tuple(snapshot.unrealized_pnl_usd for snapshot in snapshots),
            ),
            gross_pnl_usd=(
                total + fees if total is not None and fees is not None else None
            ),
            total_pnl_usd=total,
            fees_usd=fees,
            partial=not results
            or len(snapshots) != len(results)
            or any(result.stale for result in results),
            venues=results,
            series=series,
        )

    async def close(self) -> None:
        """Close every configured adapter concurrently."""
        await asyncio.gather(
            *(adapter.close() for adapter in self._adapters.values()),
            return_exceptions=True,
        )

    async def _read_venue(self, venue_id: VenueID) -> VenuePnlResult:
        """Refresh one venue while deduplicating concurrent callers."""
        cached = self._fresh_cache(venue_id)
        if cached is not None:
            return cached
        async with self._locks[venue_id]:
            cached = self._fresh_cache(venue_id)
            if cached is not None:
                return cached
            try:
                snapshot = await asyncio.wait_for(
                    self._adapters[venue_id].fetch(),
                    timeout=self._timeout_seconds,
                )
                if snapshot.venue_id != venue_id:
                    raise ValueError(
                        f"PnL adapter for {venue_id} returned {snapshot.venue_id}"
                    )
                result = VenuePnlResult(venue_id=venue_id, snapshot=snapshot)
            except Exception as error:
                stale = self._cache.get(venue_id)
                result = VenuePnlResult(
                    venue_id=venue_id,
                    snapshot=stale[1].snapshot if stale else None,
                    stale=stale is not None and stale[1].snapshot is not None,
                    error=str(error) or type(error).__name__,
                )
            self._cache[venue_id] = (monotonic(), result)
            return result

    def _fresh_cache(self, venue_id: VenueID) -> VenuePnlResult | None:
        cached = self._cache.get(venue_id)
        if cached is None or monotonic() - cached[0] > self._cache_ttl_seconds:
            return None
        return cached[1]


def _complete_sum(values: tuple[Decimal | None, ...]) -> Decimal | None:
    """Sum a component only when every available venue reports it."""
    if not values or any(value is None for value in values):
        return None
    return sum((value for value in values if value is not None), Decimal("0"))
