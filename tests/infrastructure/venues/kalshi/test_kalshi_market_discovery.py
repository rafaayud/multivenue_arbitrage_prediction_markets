"""Exercise kalshi market discovery behavior in the infrastructure kalshi layer.

Responsibilities
----------------
- Verify kalshi market discovery contracts, edge cases, and failure handling.
"""

import asyncio
from decimal import Decimal

from prediction_markets.domain.ports.instrument_discovery import InstrumentDiscoveryQuery
from prediction_markets.infrastructure.venues.kalshi.instrument_discovery import KalshiMarketDiscoveryAdapter


def test_discover_markets_lists_open_kalshi_markets(
    kalshi_market_payload,
    mock_kalshi_async_client,
):
    async def run_test():
        client, requests = mock_kalshi_async_client(kalshi_market_payload)
        adapter = KalshiMarketDiscoveryAdapter(
            base_url="https://example.test",
            client=client,
        )

        markets = await adapter.discover_markets(InstrumentDiscoveryQuery(limit=10))

        assert len(markets) == 1
        assert str(markets[0].id) == "KXBTC-26JAN01-B100000"
        assert requests[0].url.path == "/markets"
        assert requests[0].url.params["limit"] == "10"
        assert requests[0].url.params["status"] == "open"
        await client.aclose()

    asyncio.run(run_test())


def test_discover_markets_supports_ticker_lookup(
    kalshi_market_payload,
    mock_kalshi_async_client,
):
    async def run_test():
        client, requests = mock_kalshi_async_client(kalshi_market_payload)
        adapter = KalshiMarketDiscoveryAdapter(
            base_url="https://example.test",
            client=client,
        )

        markets = await adapter.discover_markets(
            InstrumentDiscoveryQuery(venue_market_id="KXBTC-26JAN01-B100000"),
        )

        assert len(markets) == 1
        assert str(markets[0].id) == "KXBTC-26JAN01-B100000"
        assert requests[0].url.path == "/markets/KXBTC-26JAN01-B100000"
        await client.aclose()

    asyncio.run(run_test())


def test_discover_markets_filters_by_volume_and_liquidity(
    kalshi_market_payload,
    mock_kalshi_async_client,
):
    async def run_test():
        low_quality_market = {
            **kalshi_market_payload,
            "ticker": "KXBTC-26JAN01-B90000",
            "volume": "5",
            "liquidity": "2",
        }
        client, _ = mock_kalshi_async_client(kalshi_market_payload, [low_quality_market])
        adapter = KalshiMarketDiscoveryAdapter(
            base_url="https://example.test",
            client=client,
        )

        markets = await adapter.discover_markets(
            InstrumentDiscoveryQuery(
                limit=10,
                min_volume=Decimal("50"),
                min_liquidity=Decimal("25"),
            ),
        )

        assert [str(market.id) for market in markets] == ["KXBTC-26JAN01-B100000"]
        await client.aclose()

    asyncio.run(run_test())


def test_discover_contracts_maps_kalshi_markets_to_domain_contracts(
    kalshi_market_payload,
    mock_kalshi_async_client,
):
    async def run_test():
        client, _ = mock_kalshi_async_client(kalshi_market_payload)
        adapter = KalshiMarketDiscoveryAdapter(
            base_url="https://example.test",
            client=client,
        )

        contracts = await adapter.discover_contracts(InstrumentDiscoveryQuery(limit=10))

        assert [str(contract.id) for contract in contracts] == [
            "kalshi:KXBTC-26JAN01-B100000:yes",
            "kalshi:KXBTC-26JAN01-B100000:no",
        ]
        assert all(str(contract.venue_id) == "KALSHI" for contract in contracts)
        await client.aclose()

    asyncio.run(run_test())


def test_discover_contracts_filters_by_token_id(
    kalshi_market_payload,
    mock_kalshi_async_client,
):
    async def run_test():
        client, _ = mock_kalshi_async_client(kalshi_market_payload)
        adapter = KalshiMarketDiscoveryAdapter(
            base_url="https://example.test",
            client=client,
        )

        contracts = await adapter.discover_contracts(
            InstrumentDiscoveryQuery(limit=10, venue_token_id="no"),
        )

        assert len(contracts) == 1
        assert str(contracts[0].id) == "kalshi:KXBTC-26JAN01-B100000:no"
        await client.aclose()

    asyncio.run(run_test())
