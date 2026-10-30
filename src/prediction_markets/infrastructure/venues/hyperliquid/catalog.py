"""Cache Hyperliquid outcome metadata and templates outside the order hot path."""

import asyncio
from time import monotonic
from typing import Any

import httpx

from prediction_markets.domain.contracts.entities import BinaryContract
from prediction_markets.domain.markets.entities import Market
from prediction_markets.infrastructure.http_client import instrumented_async_client
from prediction_markets.infrastructure.venues.hyperliquid.config import HyperliquidSettings
from prediction_markets.infrastructure.venues.hyperliquid.mappers import outcome_to_market


class HyperliquidMarketCatalog:
    """Share one atomic metadata cache between discovery and book inspection.

    Parameters
    ----------
    cache_seconds
        Positive maximum age of outcome metadata in seconds, default 30.
    client
        Optional caller-owned HTTP client.
    """

    def __init__(self, *, settings: HyperliquidSettings | None = None,
                 cache_seconds: float = 30, timeout_seconds: float = 10,
                 client: httpx.AsyncClient | None = None) -> None:
        if cache_seconds <= 0 or timeout_seconds <= 0:
            raise ValueError("Catalog cache and timeout must be positive")
        self.settings = settings or HyperliquidSettings.from_env()
        self._cache_seconds = cache_seconds
        self._owns_client = client is None
        self._client = client or instrumented_async_client("hyperliquid", timeout=timeout_seconds)
        self._lock = asyncio.Lock()
        self._loaded_at = float("-inf")
        self._metadata: dict[str, Any] = {}
        self._templates: dict[str, dict[str, Any]] = {}

    async def close(self) -> None:
        if self._owns_client:
            await self._client.aclose()

    async def info(self, request: dict[str, Any]) -> Any:
        """Read the public info endpoint with the shared HTTP instrumentation."""
        response = await self._client.post(self.settings.base_url + "/info", json=request)
        response.raise_for_status()
        return response.json()

    async def metadata(self) -> dict[str, Any]:
        """Return validated active metadata, refreshing expired snapshots atomically."""
        if monotonic() - self._loaded_at < self._cache_seconds:
            return self._metadata
        async with self._lock:
            if monotonic() - self._loaded_at < self._cache_seconds:
                return self._metadata
            metadata, templates = await asyncio.gather(
                self.info({"type": "outcomeMeta"}), self.info({"type": "outcomeTemplates"}),
            )
            if not isinstance(metadata, dict) or not isinstance(metadata.get("outcomes"), list):
                raise TypeError("Hyperliquid returned invalid outcome metadata")
            if any(not isinstance(item, dict) for item in metadata["outcomes"]):
                raise TypeError("Hyperliquid returned invalid outcome entries")
            if not isinstance(metadata.get("questions", []), list) or any(
                not isinstance(item, dict) or not isinstance(item.get("namedOutcomes", []), list)
                for item in metadata.get("questions", [])
            ):
                raise TypeError("Hyperliquid returned invalid outcome questions")
            if not isinstance(templates, list) or any(not isinstance(item, dict) or "id" not in item for item in templates):
                raise TypeError("Hyperliquid returned invalid outcome templates")
            self._metadata = metadata
            self._templates = {item["id"]: item for item in templates}
            self._loaded_at = monotonic()
            return metadata

    async def instruments(self) -> tuple[tuple[Market, tuple[BinaryContract, ...]], ...]:
        """Return supported binary markets and contracts from the same cache version."""
        metadata = await self.metadata()
        parents = {
            outcome: question
            for question in metadata.get("questions", [])
            for outcome in (*question.get("namedOutcomes", []), question.get("fallbackOutcome"))
            if outcome is not None
        }
        return tuple(
            normalized
            for payload in metadata["outcomes"]
            if (normalized := outcome_to_market(payload, self._templates, parents.get(payload["outcome"]))) is not None
        )
