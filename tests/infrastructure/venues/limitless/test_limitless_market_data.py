"""Exercise limitless market data behavior in the infrastructure limitless layer.

Responsibilities
----------------
- Verify limitless market data contracts, edge cases, and failure handling.
"""

import asyncio
from decimal import Decimal

import httpx

from prediction_markets.domain.shared.value_objects import MarketID, VenueID
from prediction_markets.infrastructure.venues.limitless.market_data import LimitlessMarketDataAdapter
from prediction_markets.infrastructure.venues.limitless.mappers import limitless_market_to_contracts


def test_list_markets_filters_its_stored_markets(
    limitless_market_payload,
    limitless_orderbook_payload,
    mock_limitless_async_client,
):
    async def run_test():
        client, _ = mock_limitless_async_client(
            limitless_market_payload,
            limitless_orderbook_payload,
        )
        adapter = LimitlessMarketDataAdapter(
            base_url="https://example.test",
            client=client,
            contracts=limitless_market_to_contracts(limitless_market_payload),
        )

        assert await adapter.list_markets(VenueID("LIMITLESS")) == ()
        assert await adapter.get_market(MarketID("btc-up-or-down-hourly-123")) is None
        await client.aclose()

    asyncio.run(run_test())


def test_get_order_book_fetches_limitless_book(
    limitless_market_payload,
    limitless_orderbook_payload,
    mock_limitless_async_client,
):
    async def run_test():
        client, requests = mock_limitless_async_client(
            limitless_market_payload,
            limitless_orderbook_payload,
        )
        contracts = limitless_market_to_contracts(limitless_market_payload)
        adapter = LimitlessMarketDataAdapter(
            base_url="https://example.test",
            client=client,
            contracts=contracts,
        )

        order_book = await adapter.get_order_book(contracts[0].id)

        assert order_book is not None
        assert order_book.best_bid().price.value == Decimal("0.45")
        assert order_book.best_ask().price.value == Decimal("0.55")
        assert requests[0].url.path == "/markets/btc-up-or-down-hourly-123/orderbook"
        await client.aclose()

    asyncio.run(run_test())


def test_get_order_book_returns_none_when_not_published(limitless_market_payload):
    async def run_test():
        client = httpx.AsyncClient(
            transport=httpx.MockTransport(lambda _: httpx.Response(404)),
        )
        contracts = limitless_market_to_contracts(limitless_market_payload)
        adapter = LimitlessMarketDataAdapter(client=client, contracts=contracts)

        assert await adapter.get_order_book(contracts[0].id) is None
        await client.aclose()

    asyncio.run(run_test())
