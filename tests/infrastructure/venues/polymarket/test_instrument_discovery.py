"""Exercise instrument discovery behavior in the infrastructure polymarket layer.

Responsibilities
----------------
- Verify instrument discovery contracts, edge cases, and failure handling.
"""

import asyncio
from datetime import datetime, timezone

import httpx
import pytest

from prediction_markets.domain.market_matching.value_objects import Underlying
from prediction_markets.domain.ports.instrument_discovery import InstrumentDiscoveryQuery
from prediction_markets.infrastructure.venues.polymarket.instrument_discovery import (
    PolymarketInstrumentDiscoveryAdapter,
    _active_series_window,
    _short_form_event_slug,
)
from prediction_markets.infrastructure.venues.polymarket.mappers import gamma_market_to_market


def test_discover_contracts_lists_gamma_contracts(
    gamma_market_payload,
    clob_book_payload,
    mock_async_client,
):
    async def run_test():
        client = mock_async_client(gamma_market_payload, clob_book_payload)
        adapter = PolymarketInstrumentDiscoveryAdapter(
            gamma_base_url="https://example.test",
            client=client,
        )

        contracts = await adapter.discover_contracts(InstrumentDiscoveryQuery(limit=10))

        assert [str(contract.id) for contract in contracts] == [
            "polymarket:0xabc:123",
            "polymarket:0xabc:456",
        ]
        await client.aclose()

    asyncio.run(run_test())


def test_discover_contracts_filters_by_token_id(
    gamma_market_payload,
    clob_book_payload,
    mock_async_client,
):
    async def run_test():
        client = mock_async_client(gamma_market_payload, clob_book_payload)
        adapter = PolymarketInstrumentDiscoveryAdapter(
            gamma_base_url="https://example.test",
            client=client,
        )

        contracts = await adapter.discover_contracts(
            InstrumentDiscoveryQuery(limit=10, venue_token_id="456"),
        )

        assert len(contracts) == 1
        assert str(contracts[0].id) == "polymarket:0xabc:456"
        await client.aclose()

    asyncio.run(run_test())


def test_discover_markets_supports_slug_lookup(
    gamma_market_payload,
    clob_book_payload,
    mock_async_client,
):
    async def run_test():
        client = mock_async_client(gamma_market_payload, clob_book_payload)
        adapter = PolymarketInstrumentDiscoveryAdapter(
            gamma_base_url="https://example.test",
            client=client,
        )

        markets = await adapter.discover_markets(
            InstrumentDiscoveryQuery(market_slug="will-btc-go-up"),
        )

        assert len(markets) == 1
        assert str(markets[0].id) == "0xabc"
        await client.aclose()

    asyncio.run(run_test())


def test_discover_contracts_supports_numeric_market_id(
    gamma_market_payload,
    clob_book_payload,
    mock_async_client,
):
    async def run_test():
        client = mock_async_client(gamma_market_payload, clob_book_payload)
        adapter = PolymarketInstrumentDiscoveryAdapter(
            gamma_base_url="https://example.test",
            client=client,
        )

        contracts = await adapter.discover_contracts(
            InstrumentDiscoveryQuery(venue_market_id="789"),
        )

        assert len(contracts) == 2
        await client.aclose()

    asyncio.run(run_test())


def test_discover_markets_targets_condition_id(gamma_market_payload):
    async def run_test():
        requests: list[httpx.Request] = []

        async def handler(request: httpx.Request) -> httpx.Response:
            requests.append(request)
            return httpx.Response(200, json=[gamma_market_payload])

        client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        adapter = PolymarketInstrumentDiscoveryAdapter(client=client)

        markets = await adapter.discover_markets(
            InstrumentDiscoveryQuery(venue_market_id="0xabc"),
        )

        assert len(markets) == 1
        assert requests[0].url.params["condition_ids"] == "0xabc"
        await client.aclose()

    asyncio.run(run_test())


def test_gamma_market_uses_event_start_time_for_short_form_maturity(gamma_market_payload):
    market = gamma_market_to_market(
        {**gamma_market_payload, "startDate": "2026-12-30T00:00:00Z"},
    )

    assert market is not None
    assert str(market.state.start_time) == "2026-12-31T23:45:00+00:00"


@pytest.mark.parametrize(
    ("symbol", "interval", "expected"),
    [
        ("BTC", 300, "btc-updown-5m-1784064000"),
        ("ETH", 900, "eth-updown-15m-1784063700"),
        ("DOGE", 3600, "dogecoin-up-or-down-july-14-2026-5pm-et"),
        ("HYPE", 86400, "hype-up-or-down-on-july-15-2026"),
        ("BNB", 3600, "bnb-up-or-down-july-14-2026-5pm-et"),
        ("BNB", 86400, "bnb-up-or-down-on-july-15-2026"),
    ],
)
def test_short_form_event_slug_supports_all_intervals(symbol, interval, expected):
    query = InstrumentDiscoveryQuery(
        underlying=Underlying(symbol),
        interval_seconds=interval,
    )

    slug = _short_form_event_slug(
        query,
        now=datetime(2026, 7, 14, 21, 23, tzinfo=timezone.utc),
    )

    assert slug == expected


@pytest.mark.parametrize(
    ("symbol", "series_slug"),
    (
        ("NVDA", "nvda-daily-up-down"),
        ("AMZN", "amzn-daily-up-down"),
        ("META", "meta-daily-up-down"),
        ("TSLA", "tsla-daily-up-down"),
        ("SPY", "spy-daily-up-or-down"),
        ("SPCX", "spcx-daily-up-or-down"),
    ),
)
def test_finance_daily_discovery_uses_active_series_window(
    gamma_market_payload,
    symbol,
    series_slug,
):
    async def run_test():
        requests: list[httpx.Request] = []
        series = {
            "events": [
                {
                    "slug": "nvda-previous",
                    "active": True,
                    "closed": True,
                    "endDate": "2099-08-11T20:00:00Z",
                },
                {
                    "slug": "nvda-current",
                    "active": True,
                    "closed": False,
                    "endDate": "2099-08-12T20:00:00Z",
                },
            ],
        }

        async def handler(request: httpx.Request) -> httpx.Response:
            requests.append(request)
            if request.url.path == "/series":
                return httpx.Response(200, json=[series])
            return httpx.Response(
                200,
                json={
                    "markets": [
                        {
                            **gamma_market_payload,
                            "endDate": "2099-08-12T20:00:00Z",
                            "resolutionSource": f"https://pythdata.app/{symbol}",
                        },
                    ],
                },
            )

        client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        adapter = PolymarketInstrumentDiscoveryAdapter(client=client)

        markets = await adapter.discover_markets(
            InstrumentDiscoveryQuery(
                underlying=Underlying(symbol),
                interval_seconds=86400,
            ),
        )

        assert len(markets) == 1
        assert str(markets[0].state.start_time) == "2099-08-11T20:00:00+00:00"
        assert [request.url.path for request in requests] == [
            "/series",
            "/events/slug/nvda-current",
        ]
        assert requests[0].url.params["slug"] == series_slug
        await client.aclose()

    asyncio.run(run_test())


def test_monitored_cycles_fit_two_gamma_request_budget(gamma_market_payload):
    """Require one bulk series request and one bulk event request per refresh."""

    async def run_test():
        requests: list[httpx.Request] = []

        async def handler(request: httpx.Request) -> httpx.Response:
            requests.append(request)
            slugs = request.url.params.get_list("slug")
            if request.url.path == "/series":
                return httpx.Response(
                    200,
                    json=[
                        {
                            "slug": slug,
                            "events": [
                                {
                                    "slug": f"{slug}-previous",
                                    "active": True,
                                    "closed": True,
                                    "endDate": "2099-08-11T20:00:00Z",
                                },
                                {
                                    "slug": f"{slug}-current",
                                    "active": True,
                                    "closed": False,
                                    "endDate": "2099-08-12T20:00:00Z",
                                },
                            ],
                        }
                        for slug in slugs
                    ],
                )

            event_slugs = (
                slugs
                if request.url.path == "/events"
                else (request.url.path.rsplit("/", 1)[-1],)
            )
            events = [
                {
                    "slug": slug,
                    "markets": [
                        {
                            **gamma_market_payload,
                            "id": slug,
                            "conditionId": f"condition-{slug}",
                            "endDate": "2099-08-12T20:00:00Z",
                        },
                    ],
                }
                for slug in event_slugs
            ]
            return httpx.Response(
                200,
                json=events if request.url.path == "/events" else events[0],
            )

        client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        adapter = PolymarketInstrumentDiscoveryAdapter(client=client)
        queries = tuple(
            InstrumentDiscoveryQuery(
                underlying=Underlying(symbol),
                interval_seconds=interval_seconds,
            )
            for symbol, interval_seconds in (
                ("BTC", 3600),
                ("BTC", 86400),
                ("ETH", 3600),
                ("ETH", 86400),
                ("BNB", 3600),
                ("BNB", 86400),
                ("NVDA", 86400),
                ("AMZN", 86400),
                ("META", 86400),
                ("TSLA", 86400),
                ("SPY", 86400),
                ("SPCX", 86400),
            )
        )
        try:
            results = await adapter.discover_many(queries)
        finally:
            await client.aclose()

        assert len(results) == len(queries)
        assert all(result.markets for result in results)
        assert [request.url.path for request in requests] == [
            "/series",
            "/events",
        ]

    asyncio.run(run_test())


def test_active_series_window_skips_expired_open_event():
    window = _active_series_window(
        {
            "events": [
                {
                    "slug": "expired",
                    "active": True,
                    "closed": False,
                    "endDate": "2026-08-11T20:00:00Z",
                },
                {
                    "slug": "next",
                    "active": True,
                    "closed": False,
                    "endDate": "2026-08-12T20:00:00Z",
                },
            ],
        },
        now=datetime(2026, 8, 11, 21, tzinfo=timezone.utc),
    )

    assert window == ("next", "2026-08-11T20:00:00Z")
