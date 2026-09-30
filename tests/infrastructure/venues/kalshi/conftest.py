"""Provide shared pytest fixtures for infrastructure kalshi tests.

Responsibilities
----------------
- Build reusable test dependencies and representative inputs.
"""

from collections.abc import Callable
from typing import Any

import httpx
import pytest


@pytest.fixture
def kalshi_market_payload() -> dict[str, Any]:
    return {
        "ticker": "KXBTC-26JAN01-B100000",
        "title": "Bitcoin above $100,000 on Jan 1, 2026?",
        "status": "open",
        "volume": "100",
        "liquidity": "50",
        "tick_size": "0.01",
        "min_order_size": "1",
        "event_ticker": "KXBTC-26JAN01",
        "open_time": "2026-01-01T14:45:00Z",
        "close_time": "2026-01-01T15:00:00Z",
        "floor_strike": "100000.25",
        "strike_type": "greater",
        "rules_primary": "Resolves Yes if the average BTC price is above the reference price.",
        "rules_secondary": "The average is calculated over the last 1 minute.",
    }


@pytest.fixture
def kalshi_orderbook_payload() -> dict[str, Any]:
    return {
        "timestamp": "1710000000000",
        "orderbook": {
            "yes": [
                [44, 1],
                [45, 3],
            ],
            "no": [
                [40, 2],
                [42, 4],
            ],
        },
    }


@pytest.fixture
def mock_kalshi_async_client() -> Callable[
    [dict[str, Any], list[dict[str, Any]] | None],
    tuple[httpx.AsyncClient, list[httpx.Request]],
]:
    def factory(
        market: dict[str, Any],
        extra_markets: list[dict[str, Any]] | None = None,
    ) -> tuple[httpx.AsyncClient, list[httpx.Request]]:
        requests: list[httpx.Request] = []
        markets = [market, *(extra_markets or [])]

        async def handler(request: httpx.Request) -> httpx.Response:
            requests.append(request)

            if request.url.path == "/markets":
                return httpx.Response(200, json={"markets": markets})

            if request.url.path == f"/markets/{market['ticker']}":
                return httpx.Response(200, json={"market": market})

            return httpx.Response(404, json={"error": "not found"})

        client = httpx.AsyncClient(
            transport=httpx.MockTransport(handler),
            base_url="https://example.test",
        )
        return client, requests

    return factory
