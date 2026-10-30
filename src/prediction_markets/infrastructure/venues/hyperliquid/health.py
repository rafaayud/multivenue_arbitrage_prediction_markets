"""Check public Hyperliquid outcome API availability."""

from time import perf_counter

from prediction_markets.domain.ports.venue_health import VenueHealthPort
from prediction_markets.domain.shared.value_objects import Timestamp
from prediction_markets.domain.venue_health import VenueHealthSnapshot, VenueHealthStatus
from prediction_markets.infrastructure.venues.hyperliquid.catalog import HyperliquidMarketCatalog
from prediction_markets.infrastructure.venues.hyperliquid.mappers import HYPERLIQUID_VENUE_ID


class HyperliquidHealthAdapter(VenueHealthPort):
    """Check fresh outcome metadata without loading account credentials."""

    def __init__(self, *, catalog: HyperliquidMarketCatalog | None = None) -> None:
        self._owns_catalog = catalog is None
        self._catalog = catalog or HyperliquidMarketCatalog(timeout_seconds=5)

    async def close(self) -> None:
        if self._owns_catalog:
            await self._catalog.close()

    async def check(self) -> VenueHealthSnapshot:
        """Read uncached public metadata and report endpoint availability."""
        start = perf_counter()
        payload = await self._catalog.info({"type": "outcomeMeta"})
        if not isinstance(payload, dict) or not isinstance(payload.get("outcomes"), list):
            raise TypeError("Hyperliquid outcome API returned invalid metadata")
        return VenueHealthSnapshot(
            venue_id=HYPERLIQUID_VENUE_ID, status=VenueHealthStatus.OPERATIONAL,
            checked_at=Timestamp.now(), latency_ms=round((perf_counter() - start) * 1000, 1),
            source="Hyperliquid outcomeMeta API", message="Outcome metadata reachable", http_status=200,
        )
