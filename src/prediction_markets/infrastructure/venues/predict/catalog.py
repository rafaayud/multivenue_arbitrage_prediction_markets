"""Own cached raw Predict market metadata shared by read adapters.

Responsibilities
----------------
- Coalesce concurrent list and detail requests.
- Reuse market payloads across discovery, key extraction, and fee preparation.
- Serve stale data during temporary rate limits and upstream failures.
"""

import asyncio
import random
import time
from collections import OrderedDict
from dataclasses import dataclass
from typing import Any

import httpx

from prediction_markets.infrastructure.http_client import instrumented_async_client
from prediction_markets.infrastructure.operational_metrics import (
    HTTP_RETRIES,
    HTTP_SCHEDULER_PENDING,
    HTTP_SCHEDULER_WAIT,
)
from prediction_markets.infrastructure.venues.predict.config import (
    predict_api_key,
    predict_headers,
)


_RATE_LIMIT_COOLDOWN_SECONDS = 30.0
_BACKOFF_MAX_SECONDS = 30.0
_MAX_MARKET_CACHE_ENTRIES = 5_000
_MAX_RESPONSE_CACHE_ENTRIES = 512
_FROM_CACHE = "prediction_markets.from_cache"
_STALE = "prediction_markets.stale"


@dataclass(frozen=True, slots=True)
class _CacheEntry:
    """Retain one payload and both clocks required for rollover expiry."""

    monotonic_at: float
    wall_time_at: float
    payload: Any
    detailed: bool = False


class PredictMarketCatalog:
    """Share bounded Predict market metadata across application read paths.

    Parameters
    ----------
    cache_seconds
        Positive maximum cache age. Rollover-aware reads expire earlier when
        their interval boundary changes.
    requests_per_second
        Positive maximum rate for uncached Predict catalog reads.
    max_market_entries
        Positive maximum number of indexed market payloads retained.
    max_response_entries
        Positive maximum number of list and detail responses retained.

    Notes
    -----
    - One asyncio lock limits concurrency to one and coalesces identical reads.
    - Execution uses a separate client and never waits behind catalog refreshes.
    - Stale payloads are returned only for transport errors, HTTP 429, or 5xx.
    - Least-recently-used entries are evicted when either cache reaches its cap.
    """

    def __init__(
        self,
        *,
        api_key: str | None = None,
        base_url: str = "https://api.predict.fun",
        timeout_seconds: float = 10.0,
        cache_seconds: float = 60.0,
        requests_per_second: float = 3.0,
        max_market_entries: int = _MAX_MARKET_CACHE_ENTRIES,
        max_response_entries: int = _MAX_RESPONSE_CACHE_ENTRIES,
        client: httpx.AsyncClient | None = None,
        raw_markets: tuple[dict[str, Any], ...] = (),
    ) -> None:
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        if cache_seconds <= 0:
            raise ValueError("cache_seconds must be positive")
        if requests_per_second <= 0:
            raise ValueError("requests_per_second must be positive")
        if max_market_entries <= 0:
            raise ValueError("max_market_entries must be positive")
        if max_response_entries <= 0:
            raise ValueError("max_response_entries must be positive")

        self._base_url = base_url.rstrip("/")
        self._host = httpx.URL(self._base_url).host or "unknown"
        self._headers = predict_headers(predict_api_key(api_key))
        self._cache_seconds = cache_seconds
        self._request_interval_seconds = 1.0 / requests_per_second
        self._max_market_entries = max_market_entries
        self._max_response_entries = max_response_entries
        self._owns_client = client is None
        self._client = client or instrumented_async_client(
            "predict",
            timeout=timeout_seconds,
        )
        self._request_lock = asyncio.Lock()
        self._responses: OrderedDict[
            tuple[tuple[str, str], ...], _CacheEntry
        ] = OrderedDict()
        self._markets: OrderedDict[str, _CacheEntry] = OrderedDict()
        self._rate_limited_until = 0.0
        self._rate_limit_error: httpx.HTTPStatusError | None = None
        self._next_request_at = 0.0
        self._backoff_until = 0.0
        self._transient_failures = 0
        self.remember(raw_markets)

    async def close(self) -> None:
        """Close the application-owned client; preserve injected clients."""
        if self._owns_client:
            await self._client.aclose()

    def remember(
        self,
        markets: tuple[dict[str, Any], ...] | list[dict[str, Any]],
        *,
        detailed: bool = True,
    ) -> None:
        """Index raw market payloads using the current cache timestamp.

        Parameters
        ----------
        markets
            Predict payloads containing stable market identifiers.
        detailed
            Whether payloads came from the market-detail endpoint.
        """
        monotonic_at = time.monotonic()
        wall_time_at = time.time()
        for market in markets:
            market_id = market.get("id")
            if market_id is not None:
                key = str(market_id)
                cached = self._markets.get(key)
                if (
                    not detailed
                    and cached is not None
                    and cached.detailed
                    and self._is_fresh(cached, rollover_seconds=None)
                ):
                    self._markets.move_to_end(key)
                    continue
                self._markets[key] = _CacheEntry(
                    monotonic_at,
                    wall_time_at,
                    market,
                    detailed,
                )
                self._markets.move_to_end(key)
        _trim_cache(self._markets, self._max_market_entries)

    async def list_page(
        self,
        params: dict[str, str | int],
        *,
        rollover_seconds: int | None = None,
    ) -> tuple[tuple[dict[str, Any], ...], str | None]:
        """Return one cached, validated Predict market-list page.

        Parameters
        ----------
        params
            Predict list endpoint query parameters.
        rollover_seconds
            Market interval whose boundary invalidates an otherwise fresh page.

        Returns
        -------
        tuple[tuple[dict[str, Any], ...], str | None]
            Raw markets and the optional next-page cursor.
        """
        response = await self._request(
            f"{self._base_url}/v1/markets",
            params=params,
            rollover_seconds=rollover_seconds,
        )
        response.raise_for_status()
        payload = response.json()
        data = _response_data(payload, expected=list)
        markets = tuple(item for item in data if isinstance(item, dict))
        if not response.extensions.get(_FROM_CACHE):
            self.remember(markets, detailed=False)
        cursor = payload.get("cursor")
        return markets, cursor if isinstance(cursor, str) and cursor else None

    async def get_market(self, market_id: str) -> dict[str, Any] | None:
        """Return one market, preferring metadata already loaded by discovery.

        Parameters
        ----------
        market_id
            Native Predict market identifier.

        Returns
        -------
        dict[str, Any] | None
            Raw market metadata, or ``None`` when Predict returns HTTP 404.
        """
        cached = self._markets.get(market_id)
        if (
            cached is not None
            and (cached.detailed or _has_start_price(cached.payload))
            and self._is_fresh(cached, rollover_seconds=None)
        ):
            self._markets.move_to_end(market_id)
            return cached.payload
        try:
            response = await self._request(
                f"{self._base_url}/v1/markets/{market_id}",
            )
            if response.status_code == 404:
                return None
            response.raise_for_status()
            data = _response_data(response.json(), expected=dict)
        except httpx.HTTPStatusError as error:
            if cached is not None and (
                error.response.status_code == 429
                or error.response.status_code >= 500
            ):
                self._markets.move_to_end(market_id)
                return cached.payload
            raise
        except httpx.TransportError:
            if cached is not None:
                self._markets.move_to_end(market_id)
                return cached.payload
            raise
        self.remember((data,), detailed=True)
        return data

    async def _request(
        self,
        url: str,
        *,
        params: dict[str, str | int] | None = None,
        rollover_seconds: int | None = None,
    ) -> httpx.Response:
        """Coalesce one request and use stale data for transient failures."""
        cache_key = _request_cache_key(url, params)
        pending = HTTP_SCHEDULER_PENDING.labels("predict", self._host)
        pending.inc()
        try:
            async with self._request_lock:
                cached = self._responses.get(cache_key)
                if cached is not None:
                    self._responses.move_to_end(cache_key)
                if cached is not None and self._is_fresh(cached, rollover_seconds):
                    return _cached_response(url, params, cached, stale=False)

                now = time.monotonic()
                if self._rate_limit_error is not None:
                    if now < self._rate_limited_until:
                        if cached is not None:
                            return _cached_response(url, params, cached, stale=True)
                        raise self._rate_limit_error
                    HTTP_RETRIES.labels(
                        "predict",
                        self._host,
                        "retry_after",
                    ).inc()
                    self._rate_limit_error = None

                now = time.monotonic()
                if now < self._backoff_until:
                    if cached is not None:
                        return _cached_response(url, params, cached, stale=True)
                    await self._wait(
                        self._backoff_until - now,
                        reason="backoff",
                    )
                    HTTP_RETRIES.labels("predict", self._host, "backoff").inc()

                await self._pace()
                try:
                    response = await self._client.get(
                        url,
                        params=params,
                        headers=self._headers,
                    )
                except httpx.TransportError:
                    self._record_transient_failure()
                    if cached is not None:
                        return _cached_response(url, params, cached, stale=True)
                    raise

                if response.status_code == 429:
                    try:
                        response.raise_for_status()
                    except httpx.HTTPStatusError as error:
                        self._rate_limit_error = error
                        self._rate_limited_until = (
                            time.monotonic() + _retry_after_seconds(response)
                        )
                        if cached is not None:
                            return _cached_response(url, params, cached, stale=True)
                        raise
                else:
                    self._rate_limit_error = None

                if response.status_code >= 500:
                    self._record_transient_failure()
                    if cached is not None:
                        return _cached_response(url, params, cached, stale=True)
                else:
                    self._reset_backoff()
                if response.is_success:
                    self._responses[cache_key] = _CacheEntry(
                        time.monotonic(),
                        time.time(),
                        response.json(),
                    )
                    self._responses.move_to_end(cache_key)
                    _trim_cache(self._responses, self._max_response_entries)
                return response
        finally:
            pending.dec()

    async def _pace(self) -> None:
        """Space uncached reads while retaining quota headroom for execution."""
        delay = max(0.0, self._next_request_at - time.monotonic())
        if delay > 0:
            await self._wait(delay, reason="pacing")
        self._next_request_at = time.monotonic() + self._request_interval_seconds

    async def _wait(self, delay: float, *, reason: str) -> None:
        """Observe and perform one scheduler delay."""
        HTTP_SCHEDULER_WAIT.labels("predict", self._host, reason).observe(delay)
        await asyncio.sleep(delay)

    def _record_transient_failure(self) -> None:
        """Open a bounded exponential cooldown after one transient failure."""
        self._transient_failures += 1
        ceiling = min(
            _BACKOFF_MAX_SECONDS,
            2.0 ** (self._transient_failures - 1),
        )
        delay = ceiling / 2 + random.uniform(0.0, ceiling / 2)
        self._backoff_until = time.monotonic() + delay

    def _reset_backoff(self) -> None:
        """Close the transient-failure circuit after a reachable response."""
        self._transient_failures = 0
        self._backoff_until = 0.0

    def _is_fresh(
        self,
        entry: _CacheEntry,
        rollover_seconds: int | None,
    ) -> bool:
        """Check TTL and optional wall-clock rollover for one entry."""
        if time.monotonic() - entry.monotonic_at >= self._cache_seconds:
            return False
        return rollover_seconds is None or (
            int(time.time() // rollover_seconds)
            == int(entry.wall_time_at // rollover_seconds)
        )


def _cached_response(
    url: str,
    params: dict[str, str | int] | None,
    entry: _CacheEntry,
    *,
    stale: bool,
) -> httpx.Response:
    """Rebuild a successful response from one fresh or stale cache entry."""
    return httpx.Response(
        200,
        json=entry.payload,
        request=httpx.Request("GET", url, params=params),
        extensions={_FROM_CACHE: True, _STALE: stale},
    )


def _trim_cache(
    cache: OrderedDict[Any, _CacheEntry],
    max_entries: int,
) -> None:
    """Evict least-recently-used entries until one cache is within its cap."""
    while len(cache) > max_entries:
        cache.popitem(last=False)


def _has_start_price(market: dict[str, Any]) -> bool:
    """Return whether list metadata can build a Predict crypto key."""
    variant = market.get("variantData")
    return isinstance(variant, dict) and variant.get("startPrice") is not None


def _request_cache_key(
    url: str,
    params: dict[str, str | int] | None,
) -> tuple[tuple[str, str], ...]:
    """Build a stable key for one idempotent Predict request."""
    return tuple(
        sorted(
            [("url", url)]
            + [(str(key), str(value)) for key, value in (params or {}).items()],
        ),
    )


def _retry_after_seconds(response: httpx.Response) -> float:
    """Return a bounded cooldown from Predict's optional rate-limit header."""
    try:
        value = float(response.headers.get("Retry-After", ""))
    except ValueError:
        value = _RATE_LIMIT_COOLDOWN_SECONDS
    return max(1.0, min(value, 120.0))


def _response_data(payload: Any, *, expected: type) -> Any:
    """Validate a Predict response envelope and return its data field."""
    if not isinstance(payload, dict) or payload.get("success") is not True:
        raise TypeError("Unexpected Predict API response")
    data = payload.get("data")
    if not isinstance(data, expected):
        raise TypeError(
            f"Unexpected Predict API data: expected {expected.__name__}, "
            f"got {type(data).__name__}",
        )
    return data
