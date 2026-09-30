"""Provide shared pytest fixtures for infrastructure limitless tests.

Responsibilities
----------------
- Build reusable test dependencies and representative inputs.
"""

from collections.abc import Callable
from typing import Any

import httpx
import pytest


@pytest.fixture
def limitless_market_payload() -> dict[str, Any]:
    return {
        "conditionId": "0xabc",
        "title": "BTC Up or Down - Hourly",
        "description": (
            "<p>This market will resolve to Up if the Chainlink BTC/USD price at the end "
            "is greater than or equal to the price captured at the start. "
            "Otherwise, this market resolves Down.</p>"
        ),
        "slug": "btc-up-or-down-hourly-123",
        "tradeType": "clob",
        "status": "FUNDED",
        "expired": False,
        "tokens": {"yes": "111", "no": "222"},
        "collateralToken": {"symbol": "USDC", "decimals": 6},
        "volume": "125000000",
        "volumeFormatted": "125",
        "liquidity": "25000000",
        "liquidityFormatted": "25",
        "expirationTimestamp": 1784048400000,
        "startAt": "2026-07-14T16:00:00Z",
        "metadata": {"minSize": "1000000", "openPrice": "64700.27740085"},
        "priceOracleMetadata": {"symbol": "Crypto.BTC/USD"},
    }


@pytest.fixture
def limitless_orderbook_payload() -> dict[str, Any]:
    return {
        "timestamp": "1710000000000",
        "bids": [
            {"price": "0.44", "size": "1000000"},
            {"price": "0.45", "size": "3000000"},
        ],
        "asks": [
            {"price": "0.56", "size": "2000000"},
            {"price": "0.55", "size": "4000000"},
        ],
    }


@pytest.fixture
def mock_limitless_async_client() -> Callable[
    [dict[str, Any], dict[str, Any]],
    tuple[httpx.AsyncClient, list[httpx.Request]],
]:
    def factory(
        market: dict[str, Any],
        orderbook: dict[str, Any],
    ) -> tuple[httpx.AsyncClient, list[httpx.Request]]:
        requests: list[httpx.Request] = []

        async def handler(request: httpx.Request) -> httpx.Response:
            requests.append(request)
            slug = market["slug"]
            if request.url.path == "/markets/active":
                return httpx.Response(200, json={"data": [market]})
            if request.url.path == f"/markets/{slug}":
                return httpx.Response(200, json=market)
            if request.url.path == f"/markets/{slug}/orderbook":
                return httpx.Response(200, json=orderbook)
            return httpx.Response(404, json={"error": "not found"})

        client = httpx.AsyncClient(
            transport=httpx.MockTransport(handler),
            base_url="https://example.test",
        )
        return client, requests

    return factory
