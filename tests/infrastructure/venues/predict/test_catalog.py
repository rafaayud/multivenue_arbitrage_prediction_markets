"""Verify shared Predict catalog caching and degraded-operation behavior."""

import asyncio

import httpx
import pytest

import prediction_markets.infrastructure.venues.predict.catalog as catalog_module
from prediction_markets.domain.market_matching.value_objects import Underlying
from prediction_markets.domain.ports.instrument_discovery import InstrumentDiscoveryQuery
from prediction_markets.infrastructure.venues.predict.catalog import PredictMarketCatalog
from prediction_markets.infrastructure.venues.predict.instrument_discovery import (
    PredictInstrumentDiscoveryAdapter,
)
from prediction_markets.infrastructure.venues.predict.key_extraction import (
    PredictKeyExtractionAdapter,
)
from prediction_markets.infrastructure.venues.predict.mappers import PREDICT_VENUE_ID
from prediction_markets.infrastructure.venues.predict.taker_fees import (
    PredictTakerFeeCalculator,
)


def _market() -> dict[str, object]:
    return {
        "id": 42,
        "title": "BTC/USD Up or Down",
        "tradingStatus": "OPEN",
        "marketVariant": "CRYPTO_UP_DOWN",
        "boostStartsAt": "2026-07-23T13:30:00Z",
        "boostEndsAt": "2026-07-23T13:44:59Z",
        "feeRateBps": 200,
        "outcomes": [
            {"name": "Up", "indexSet": 1, "onChainId": "421"},
            {"name": "Down", "indexSet": 2, "onChainId": "422"},
        ],
        "stats": {"volumeTotalUsd": 125, "totalLiquidityUsd": 25},
        "variantData": {
            "type": "CRYPTO_UP_DOWN",
            "priceFeedProvider": "PYTH",
            "priceFeedSymbol": "BTC_USD",
            "startPrice": 64700.25,
        },
    }


def test_catalog_reuses_discovery_payload_for_keys_and_fees() -> None:
    """Avoid market-detail requests after one atomic discovery response."""
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            200,
            json={"success": True, "cursor": "", "data": [_market()]},
        )

    async def run() -> None:
        client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        catalog = PredictMarketCatalog(
            api_key="secret",
            base_url="https://example.test",
            client=client,
        )
        discovery = PredictInstrumentDiscoveryAdapter(catalog=catalog)
        keys = PredictKeyExtractionAdapter(catalog=catalog)
        fees = PredictTakerFeeCalculator(catalog=catalog)
        query = InstrumentDiscoveryQuery(
            venue_id=PREDICT_VENUE_ID,
            limit=1,
            underlying=Underlying("BTC"),
            interval_seconds=900,
        )
        try:
            result, duplicate = await asyncio.gather(
                discovery.discover(query),
                discovery.discover(query),
            )
            keyed = await keys.extract_key(
                result.markets,
                underlying=Underlying("BTC"),
            )
            await fees.prepare(tuple(contract.id for contract in result.contracts))
        finally:
            await catalog.close()
            await client.aclose()

        assert len(result.markets) == 1
        assert len(result.contracts) == 2
        assert duplicate == result
        assert len(keyed) == 1

    asyncio.run(run())

    assert len(requests) == 1
    assert requests[0].url.path == "/v1/markets"


def test_catalog_serves_stale_page_after_429() -> None:
    """Keep discovery available when an expired page refresh is rate limited."""
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if len(requests) == 1:
            return httpx.Response(
                200,
                json={"success": True, "cursor": "", "data": [_market()]},
            )
        return httpx.Response(429, headers={"Retry-After": "30"})

    async def run() -> None:
        client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        catalog = PredictMarketCatalog(
            api_key="secret",
            base_url="https://example.test",
            cache_seconds=0.001,
            client=client,
        )
        params: dict[str, str | int] = {
            "first": 100,
            "status": "OPEN",
        }
        try:
            first, _ = await catalog.list_page(params)
            await asyncio.sleep(0.01)
            stale, _ = await catalog.list_page(params)
            cooldown_stale, _ = await catalog.list_page(params)
        finally:
            await catalog.close()
            await client.aclose()

        assert first == stale == cooldown_stale

    asyncio.run(run())

    assert len(requests) == 2


def test_catalog_invalidates_page_at_market_rollover(monkeypatch) -> None:
    """Refresh a page when its interval changes before the TTL expires."""
    wall_time = [899.0]
    requests: list[httpx.Request] = []
    monkeypatch.setattr(catalog_module.time, "time", lambda: wall_time[0])

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            200,
            json={"success": True, "cursor": "", "data": [_market()]},
        )

    async def run() -> None:
        client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        catalog = PredictMarketCatalog(
            api_key="secret",
            base_url="https://example.test",
            cache_seconds=60,
            client=client,
        )
        try:
            await catalog.list_page({"first": 100}, rollover_seconds=900)
            wall_time[0] = 901.0
            await catalog.list_page({"first": 100}, rollover_seconds=900)
        finally:
            await catalog.close()
            await client.aclose()

    asyncio.run(run())

    assert len(requests) == 2


def test_catalog_paces_uncached_reads(monkeypatch) -> None:
    """Spread cold metadata requests instead of emitting one burst."""
    clock = [0.0]
    sleeps: list[float] = []

    async def sleep(delay: float) -> None:
        sleeps.append(delay)
        clock[0] += delay

    monkeypatch.setattr(catalog_module.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(catalog_module.asyncio, "sleep", sleep)

    def handler(request: httpx.Request) -> httpx.Response:
        market = _market()
        market["id"] = int(request.url.path.rsplit("/", 1)[-1])
        return httpx.Response(200, json={"success": True, "data": market})

    async def run() -> None:
        client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        catalog = PredictMarketCatalog(
            api_key="secret",
            base_url="https://example.test",
            requests_per_second=2,
            client=client,
        )
        try:
            await catalog.get_market("1")
            await catalog.get_market("2")
        finally:
            await catalog.close()
            await client.aclose()

    asyncio.run(run())

    assert sleeps == [pytest.approx(0.5)]


def test_catalog_bounds_market_and_response_caches() -> None:
    """Evict old metadata instead of retaining every discovered market forever."""

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={"success": True, "cursor": "", "data": [_market()]},
        )

    async def run() -> None:
        client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        catalog = PredictMarketCatalog(
            api_key="secret",
            base_url="https://example.test",
            requests_per_second=1000,
            max_market_entries=2,
            max_response_entries=2,
            client=client,
        )
        try:
            catalog.remember([{"id": 1}, {"id": 2}, {"id": 3}])
            for cursor in ("a", "b", "c"):
                await catalog.list_page({"first": 1, "after": cursor})

            assert len(catalog._markets) == 2
            assert "1" not in catalog._markets
            assert len(catalog._responses) == 2
        finally:
            await catalog.close()
            await client.aclose()

    asyncio.run(run())


def test_catalog_backs_off_after_server_failure(monkeypatch) -> None:
    """Delay the next cold read after a retryable Predict failure."""
    clock = [0.0]
    sleeps: list[float] = []
    requests = 0

    async def sleep(delay: float) -> None:
        sleeps.append(delay)
        clock[0] += delay

    monkeypatch.setattr(catalog_module.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(catalog_module.asyncio, "sleep", sleep)
    monkeypatch.setattr(catalog_module.random, "uniform", lambda _low, _high: 0.0)

    def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal requests
        requests += 1
        if requests == 1:
            return httpx.Response(503)
        return httpx.Response(200, json={"success": True, "data": _market()})

    async def run() -> None:
        client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        catalog = PredictMarketCatalog(
            api_key="secret",
            base_url="https://example.test",
            requests_per_second=1000,
            client=client,
        )
        try:
            with pytest.raises(httpx.HTTPStatusError):
                await catalog.get_market("42")
            assert await catalog.get_market("42") == _market()
        finally:
            await catalog.close()
            await client.aclose()

    asyncio.run(run())

    assert requests == 2
    assert sleeps == [pytest.approx(0.5)]
