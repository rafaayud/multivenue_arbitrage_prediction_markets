"""Provide shared pytest fixtures for infrastructure polymarket tests.

Responsibilities
----------------
- Build reusable test dependencies and representative inputs.
"""

from collections.abc import Callable
from typing import Any

import httpx
import pytest


@pytest.fixture
def gamma_market_payload() -> dict[str, Any]:
    return {
        "id": "789",
        "conditionId": "0xabc",
        "question": "Will BTC go up?",
        "active": True,
        "closed": False,
        "archived": False,
        "clobTokenIds": '["123", "456"]',
        "outcomes": '["Yes", "No"]',
        "orderPriceMinTickSize": "0.01",
        "orderMinSize": "1",
        "volume": "100",
        "liquidity": "50",
        "slug": "will-btc-go-up",
        "eventStartTime": "2026-12-31T23:45:00Z",
        "endDate": "2026-12-31T23:59:59Z",
        "resolutionSource": "https://data.chain.link/streams/btc-usd",
        "description": "Resolves Up when the end price is at least the start price.",
        "referencePrice": "67234.50",
    }


@pytest.fixture
def clob_book_payload() -> dict[str, Any]:
    return {
        "market": "0xabc",
        "asset_id": "123",
        "timestamp": "1710000000000",
        "bids": [
            {"price": "0.44", "size": "1"},
            {"price": "0.45", "size": "3"},
        ],
        "asks": [
            {"price": "0.56", "size": "2"},
            {"price": "0.55", "size": "4"},
        ],
    }


@pytest.fixture
def mock_async_client() -> Callable[[dict[str, Any], dict[str, Any]], httpx.AsyncClient]:
    def factory(
        gamma_market: dict[str, Any],
        clob_book: dict[str, Any],
    ) -> httpx.AsyncClient:
        async def handler(request: httpx.Request) -> httpx.Response:
            if request.url.path == "/markets/slug/will-btc-go-up":
                return httpx.Response(200, json=gamma_market)

            if request.url.path == f"/markets/{gamma_market['id']}":
                return httpx.Response(200, json=gamma_market)

            if request.url.path == "/markets":
                if request.url.params.get("condition_ids") == gamma_market["conditionId"]:
                    return httpx.Response(200, json=[gamma_market])
                return httpx.Response(200, json=[gamma_market])

            if request.url.path == "/book":
                return httpx.Response(200, json=clob_book)

            return httpx.Response(404, json={"error": "not found"})

        return httpx.AsyncClient(
            transport=httpx.MockTransport(handler),
            base_url="https://example.test",
        )

    return factory
