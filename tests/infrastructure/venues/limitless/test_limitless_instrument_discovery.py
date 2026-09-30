"""Exercise limitless instrument discovery behavior in the infrastructure limitless layer.

Responsibilities
----------------
- Verify limitless instrument discovery contracts, edge cases, and failure handling.
"""

import asyncio
from decimal import Decimal
import subprocess
import sys

import httpx

from prediction_markets.domain.market_matching.value_objects import Underlying
from prediction_markets.domain.ports.instrument_discovery import InstrumentDiscoveryQuery
from prediction_markets.domain.shared.value_objects import VenueID
from prediction_markets.infrastructure.venues.limitless import instrument_discovery
from prediction_markets.infrastructure.venues.limitless.instrument_discovery import LimitlessInstrumentDiscoveryAdapter


def test_importing_discovery_does_not_import_execution_metrics():
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            (
                "import sys; "
                "import prediction_markets.infrastructure.venues.limitless.instrument_discovery; "
                "print('prediction_markets.infrastructure.metrics' in sys.modules)"
            ),
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    assert result.stdout.strip() == "False"


def test_discover_markets_lists_active_clob_markets(
    limitless_market_payload,
    limitless_orderbook_payload,
    mock_limitless_async_client,
):
    async def run_test():
        client, requests = mock_limitless_async_client(
            limitless_market_payload,
            limitless_orderbook_payload,
        )
        adapter = LimitlessInstrumentDiscoveryAdapter(
            base_url="https://example.test",
            client=client,
        )

        markets = await adapter.discover_markets(InstrumentDiscoveryQuery(limit=10))

        assert str(markets[0].id) == "btc-up-or-down-hourly-123"
        assert requests[0].url.path == "/markets/active"
        assert requests[0].url.params["limit"] == "10"
        assert requests[0].url.params["page"] == "1"
        assert requests[0].url.params["tradeType"] == "clob"
        await client.aclose()

    asyncio.run(run_test())


def test_discover_markets_paginates_above_limitless_page_size(limitless_market_payload):
    async def run_test():
        requests = []

        async def handler(request):
            requests.append(request)
            page = request.url.params["page"]
            if page == "1":
                return httpx.Response(
                    200,
                    json={
                        "data": [
                            {**limitless_market_payload, "slug": f"market-{index}"}
                            for index in range(25)
                        ],
                    },
                )
            return httpx.Response(200, json={"data": []})

        client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        adapter = LimitlessInstrumentDiscoveryAdapter(client=client)

        markets = await adapter.discover_markets(InstrumentDiscoveryQuery(limit=30))

        assert len(markets) == 25
        assert [(request.url.params["page"], request.url.params["limit"]) for request in requests] == [
            ("1", "25"),
            ("2", "5"),
        ]
        await client.aclose()

    asyncio.run(run_test())


def test_discover_markets_supports_slug_lookup(
    limitless_market_payload,
    limitless_orderbook_payload,
    mock_limitless_async_client,
):
    async def run_test():
        client, requests = mock_limitless_async_client(
            limitless_market_payload,
            limitless_orderbook_payload,
        )
        adapter = LimitlessInstrumentDiscoveryAdapter(
            base_url="https://example.test",
            client=client,
        )

        markets = await adapter.discover_markets(
            InstrumentDiscoveryQuery(market_slug="btc-up-or-down-hourly-123"),
        )

        assert str(markets[0].id) == "btc-up-or-down-hourly-123"
        assert requests[0].url.path == "/markets/btc-up-or-down-hourly-123"
        await client.aclose()

    asyncio.run(run_test())


def test_discover_markets_resolves_agg_numeric_id_through_search(
    limitless_market_payload,
):
    """Convert AGG's Limitless ID to the native slug before subscription."""
    async def run_test():
        requests: list[httpx.Request] = []
        market = {
            **limitless_market_payload,
            "id": 36783,
            "slug": "marco-rubio-1768931335058",
            "title": "Marco Rubio",
        }

        async def handler(request: httpx.Request) -> httpx.Response:
            requests.append(request)
            return httpx.Response(200, json={"markets": [market]})

        client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        adapter = LimitlessInstrumentDiscoveryAdapter(client=client)

        markets = await adapter.discover_markets(
            InstrumentDiscoveryQuery(
                venue_market_id="36783",
                search_text="Republican Presidential Nominee 2028",
            ),
        )

        assert str(markets[0].id) == "marco-rubio-1768931335058"
        assert requests[0].url.path == "/markets/search"
        assert requests[0].url.params["query"] == (
            "Republican Presidential Nominee 2028"
        )
        await client.aclose()

    asyncio.run(run_test())


def test_discover_markets_retries_with_token_catalog_when_exact_search_misses(
    limitless_market_payload,
):
    """Resolve a current AGG ID when Limitless search wording differs."""
    async def run_test():
        requests: list[httpx.Request] = []
        market = {
            **limitless_market_payload,
            "id": 36783,
            "slug": "newcastle-liverpool-1768931335058",
        }

        async def handler(request: httpx.Request) -> httpx.Response:
            requests.append(request)
            query = request.url.params["query"]
            return httpx.Response(
                200,
                json={"markets": [{"markets": [market]}] if query == "Newcastle" else []},
            )

        client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        adapter = LimitlessInstrumentDiscoveryAdapter(client=client)

        markets = await adapter.discover_markets(
            InstrumentDiscoveryQuery(
                venue_market_id="36783",
                search_text="Newcastle United FC vs. Liverpool FC",
            ),
        )

        assert str(markets[0].id) == "newcastle-liverpool-1768931335058"
        assert [request.url.params["query"] for request in requests] == [
            "Newcastle United FC vs. Liverpool FC",
            "Newcastle United FC vs. Liverpool FC",
            "Newcastle",
        ]
        await client.aclose()

    asyncio.run(run_test())


def test_discover_markets_reuses_search_catalog(
    limitless_market_payload,
):
    """Reuse a short-lived search catalog for repeated AGG ID resolution."""
    async def run_test():
        requests: list[httpx.Request] = []
        market = {
            **limitless_market_payload,
            "id": 36783,
            "slug": "marco-rubio-1768931335058",
        }

        async def handler(request: httpx.Request) -> httpx.Response:
            requests.append(request)
            return httpx.Response(200, json={"markets": [market]})

        client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        adapter = LimitlessInstrumentDiscoveryAdapter(client=client)
        query = InstrumentDiscoveryQuery(
            venue_market_id="36783",
            search_text="Republican Presidential Nominee 2028",
        )

        first = await adapter.discover_markets(query)
        second = await adapter.discover_markets(query)

        assert len(first) == len(second) == 1
        assert len(requests) == 1
        await client.aclose()

    asyncio.run(run_test())


def test_discover_contracts_filters_by_venue_token_and_market_quality(
    limitless_market_payload,
    limitless_orderbook_payload,
    mock_limitless_async_client,
):
    async def run_test():
        client, _ = mock_limitless_async_client(
            limitless_market_payload,
            limitless_orderbook_payload,
        )
        adapter = LimitlessInstrumentDiscoveryAdapter(
            base_url="https://example.test",
            client=client,
        )

        contracts = await adapter.discover_contracts(
            InstrumentDiscoveryQuery(
                limit=10,
                venue_token_id="no",
                min_volume=Decimal("100"),
                min_liquidity=Decimal("20"),
            ),
        )

        assert [str(contract.id) for contract in contracts] == [
            "limitless:btc-up-or-down-hourly-123:no",
        ]
        await client.aclose()

    asyncio.run(run_test())


def test_discover_contracts_excludes_other_venue(
    limitless_market_payload,
    limitless_orderbook_payload,
    mock_limitless_async_client,
):
    async def run_test():
        client, _ = mock_limitless_async_client(
            limitless_market_payload,
            limitless_orderbook_payload,
        )
        adapter = LimitlessInstrumentDiscoveryAdapter(
            base_url="https://example.test",
            client=client,
        )

        contracts = await adapter.discover_contracts(
            InstrumentDiscoveryQuery(venue_id=VenueID("KALSHI")),
        )

        assert contracts == ()
        await client.aclose()

    asyncio.run(run_test())


def test_short_form_discovery_targets_current_window_and_reuses_long_form_slugs(
    limitless_market_payload,
    monkeypatch,
):
    async def run_test():
        monkeypatch.setattr(instrument_discovery, "time", lambda: 1234)
        requests: list[httpx.Request] = []

        async def handler(request: httpx.Request) -> httpx.Response:
            requests.append(request)
            if request.url.path == "/markets/active/slugs":
                return httpx.Response(
                    200,
                    json=[
                        {"ticker": "DOGE", "slug": "doge-up-or-down-hourly-123"},
                        {"ticker": "XRP", "slug": "xrp-up-or-down-daily-123"},
                        {
                            "ticker": "NVDA",
                            "slug": "nvidia-nvda-up-or-down-daily-123",
                        },
                    ],
                )
            slug = request.url.path.rsplit("/", 1)[-1]
            return httpx.Response(200, json={**limitless_market_payload, "slug": slug})

        client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        adapter = LimitlessInstrumentDiscoveryAdapter(client=client)

        btc, eth, doge, xrp, nvda = await asyncio.gather(
            adapter.discover_markets(
                InstrumentDiscoveryQuery(
                    underlying=Underlying("BTC"),
                    interval_seconds=300,
                ),
            ),
            adapter.discover_markets(
                InstrumentDiscoveryQuery(
                    underlying=Underlying("ETH"),
                    interval_seconds=900,
                ),
            ),
            adapter.discover_markets(
                InstrumentDiscoveryQuery(
                    underlying=Underlying("DOGE"),
                    interval_seconds=3600,
                ),
            ),
            adapter.discover_markets(
                InstrumentDiscoveryQuery(
                    underlying=Underlying("XRP"),
                    interval_seconds=86400,
                ),
            ),
            adapter.discover_markets(
                InstrumentDiscoveryQuery(
                    underlying=Underlying("NVDA"),
                    interval_seconds=86400,
                ),
            ),
        )

        assert len(btc) == len(eth) == len(doge) == len(xrp) == len(nvda) == 1
        paths = [request.url.path for request in requests]
        assert "/markets/btc-up-or-down-5-min-1200" in paths
        assert "/markets/eth-up-or-down-15-min-900" in paths
        assert sum(request.url.path == "/markets/active/slugs" for request in requests) == 1
        await client.aclose()

    asyncio.run(run_test())


def test_short_form_discovery_returns_empty_until_current_market_exists(monkeypatch):
    async def run_test():
        monkeypatch.setattr(instrument_discovery, "time", lambda: 1234)

        async def handler(_request: httpx.Request) -> httpx.Response:
            return httpx.Response(404)

        client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        adapter = LimitlessInstrumentDiscoveryAdapter(client=client)

        markets = await adapter.discover_markets(
            InstrumentDiscoveryQuery(
                underlying=Underlying("BTC"),
                interval_seconds=300,
            ),
        )

        assert markets == ()
        await client.aclose()

    asyncio.run(run_test())
