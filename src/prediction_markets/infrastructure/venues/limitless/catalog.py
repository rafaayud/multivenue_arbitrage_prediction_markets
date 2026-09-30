"""Cache raw Limitless market metadata shared by read adapters.

Responsibilities
----------------
- Coalesce repeated market, active-slug, and search requests.
- Share native market payloads between discovery and key extraction.
"""

import asyncio
from collections import defaultdict
from time import monotonic
from typing import Any

import httpx

from prediction_markets.infrastructure.http_client import instrumented_async_client


_ACTIVE_SLUGS_TTL_SECONDS = 30.0
_SEARCH_CATALOG_TTL_SECONDS = 30.0


class LimitlessMarketCatalog:
    """Share short-lived raw Limitless metadata across application read paths.

    Parameters
    ----------
    base_url
        Limitless API base URL.
    timeout_seconds : float, default=10.0
        HTTP timeout in seconds for catalog requests.
    cache_seconds : float, default=30.0
        Maximum age of a detailed market payload.
    client
        Optional caller-owned HTTP client.
    raw_markets
        Initial detailed payloads indexed by their native slugs.

    Notes
    -----
    - Requests for different slugs remain concurrent; requests for one slug
      are single-flight.
    - Search results are cached as incomplete metadata and never replace a
      detailed market payload.
    """

    def __init__(
        self,
        *,
        base_url: str = "https://api.limitless.exchange",
        timeout_seconds: float = 10.0,
        cache_seconds: float = 30.0,
        client: httpx.AsyncClient | None = None,
        raw_markets: tuple[dict[str, Any], ...] = (),
    ) -> None:
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        if cache_seconds <= 0:
            raise ValueError("cache_seconds must be positive")

        self._base_url = base_url.rstrip("/")
        self._cache_seconds = cache_seconds
        self._owns_client = client is None
        self._client = client or instrumented_async_client(
            "limitless",
            timeout=timeout_seconds,
        )
        self._markets: dict[str, tuple[float, dict[str, Any], bool]] = {}
        self._market_locks: defaultdict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
        self._active_slugs: tuple[dict[str, Any], ...] = ()
        self._active_slugs_expires_at = 0.0
        self._active_slugs_lock = asyncio.Lock()
        self._active_markets: dict[
            tuple[int, int],
            tuple[float, tuple[dict[str, Any], ...]],
        ] = {}
        self._active_market_locks: defaultdict[
            tuple[int, int], asyncio.Lock
        ] = defaultdict(asyncio.Lock)
        self._search_catalog: dict[str, tuple[float, tuple[dict[str, Any], ...]]] = {}
        self._search_locks: defaultdict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
        self.remember(raw_markets)

    async def close(self) -> None:
        """Close the catalog-owned HTTP client."""
        if self._owns_client:
            await self._client.aclose()

    def remember(
        self,
        markets: tuple[dict[str, Any], ...] | list[dict[str, Any]],
        *,
        detailed: bool = True,
    ) -> None:
        """Index raw market payloads for subsequent catalog reads.

        Parameters
        ----------
        markets
            Limitless payloads containing a native ``slug``.
        detailed
            Whether the payload came from the market-detail endpoint.
        """
        expires_at = monotonic() + self._cache_seconds
        for market in markets:
            slug = str(market.get("slug") or "").strip()
            if not slug:
                continue
            cached = self._markets.get(slug)
            if cached is not None and cached[2] and not detailed:
                continue
            self._markets[slug] = (expires_at, market, detailed)

    async def get_market(
        self,
        slug: str,
        *,
        missing_ok: bool = False,
    ) -> dict[str, Any] | None:
        """Return one detailed market payload, fetching it when expired.

        Parameters
        ----------
        slug
            Native Limitless market slug.
        missing_ok
            Return ``None`` instead of raising for an upstream 404.

        Returns
        -------
        dict[str, Any] | None
            Detailed market metadata, or ``None`` for an allowed 404.
        """
        slug = slug.strip()
        if not slug:
            raise ValueError("Limitless market slug cannot be blank")

        async with self._market_locks[slug]:
            cached = self._markets.get(slug)
            if cached is not None and cached[0] > monotonic() and cached[2]:
                return cached[1]

            response = await self._client.get(f"{self._base_url}/markets/{slug}")
            if response.status_code == 404:
                if missing_ok:
                    return None
                response.raise_for_status()
            response.raise_for_status()
            data = response.json()
            if not isinstance(data, dict):
                raise TypeError(
                    f"Unexpected Limitless market response for slug {slug}: "
                    f"expected dict, got {type(data).__name__}",
                )
            self.remember((data,))
            return data

    async def get_active_slugs(self) -> tuple[dict[str, Any], ...]:
        """Return the short-lived active-market slug catalog."""
        async with self._active_slugs_lock:
            if monotonic() < self._active_slugs_expires_at:
                return self._active_slugs

            response = await self._client.get(f"{self._base_url}/markets/active/slugs")
            response.raise_for_status()
            data = response.json()
            if not isinstance(data, list):
                raise TypeError("Unexpected Limitless active slugs response")
            self._active_slugs = tuple(item for item in data if isinstance(item, dict))
            self._active_slugs_expires_at = monotonic() + _ACTIVE_SLUGS_TTL_SECONDS
            return self._active_slugs

    async def list_active_markets(
        self,
        *,
        limit: int,
        page: int,
    ) -> tuple[dict[str, Any], ...]:
        """Return one cached page from the active Limitless market catalog."""
        key = (limit, page)
        async with self._active_market_locks[key]:
            cached = self._active_markets.get(key)
            if cached is not None and cached[0] > monotonic():
                return cached[1]

            response = await self._client.get(
                f"{self._base_url}/markets/active",
                params={"limit": limit, "page": page, "tradeType": "clob"},
            )
            response.raise_for_status()
            payload = response.json()
            if not isinstance(payload, dict) or not isinstance(
                payload.get("data"), list,
            ):
                raise TypeError(
                    "Unexpected Limitless markets response: "
                    "missing or invalid 'data' field",
                )
            markets = tuple(
                item for item in payload["data"] if isinstance(item, dict)
            )
            self._active_markets[key] = (
                monotonic() + self._cache_seconds,
                markets,
            )
            self.remember(list(markets), detailed=False)
            return markets

    async def search(self, query: str) -> tuple[dict[str, Any], ...]:
        """Return a short-lived normalized Limitless search catalog."""
        query = " ".join(query.split())
        if not query:
            raise ValueError("Limitless search query cannot be blank")

        async with self._search_locks[query]:
            cached = self._search_catalog.get(query)
            if cached is not None and cached[0] > monotonic():
                return cached[1]

            response = await self._client.get(
                f"{self._base_url}/markets/search",
                params={"query": query},
            )
            response.raise_for_status()
            payload = response.json()
            roots = payload.get("markets") if isinstance(payload, dict) else None
            if not isinstance(roots, list):
                raise TypeError("Unexpected Limitless market search response")
            markets = tuple(
                market
                for root in roots
                if isinstance(root, dict)
                for market in (root, *(root.get("markets") or ()))
                if isinstance(market, dict)
            )
            self._search_catalog[query] = (
                monotonic() + _SEARCH_CATALOG_TTL_SECONDS,
                markets,
            )
            self.remember(list(markets), detailed=False)
            return markets

    async def invalidate_market(self, slug: str) -> None:
        """Remove one detailed payload after local validation rejects it."""
        async with self._market_locks[slug]:
            self._markets.pop(slug, None)

    async def invalidate_search(self, query: str) -> None:
        """Remove one search result so the next lookup refreshes it."""
        async with self._search_locks[query]:
            self._search_catalog.pop(query, None)
