"""Exercise market data behavior in the infrastructure polymarket layer.

Responsibilities
----------------
- Verify market data contracts, edge cases, and failure handling.
"""

import asyncio
from decimal import Decimal

from prediction_markets.domain.shared.value_objects import MarketID, VenueID
from prediction_markets.infrastructure.venues.polymarket.market_data import PolymarketMarketDataAdapter
from prediction_markets.infrastructure.venues.polymarket.mappers import gamma_market_to_contracts
from prediction_markets.infrastructure.venues.polymarket.mappers import gamma_market_to_market


def test_list_markets_fetches_gamma_markets(
    gamma_market_payload,
    clob_book_payload,
    mock_async_client,
):
    async def run_test():
        client = mock_async_client(gamma_market_payload, clob_book_payload)
        market = gamma_market_to_market(gamma_market_payload)
        contracts = gamma_market_to_contracts(gamma_market_payload)
        adapter = PolymarketMarketDataAdapter(
            clob_base_url="https://example.test",
            client=client,
            markets=(market,),
            contracts=contracts,
        )

        markets = await adapter.list_markets()

        assert len(markets) == 1
        assert str(markets[0].id) == "0xabc"
        assert markets[0].title == "Will BTC go up?"
        await client.aclose()

    asyncio.run(run_test())


def test_list_markets_filters_other_venue(
    gamma_market_payload,
    clob_book_payload,
    mock_async_client,
):
    async def run_test():
        client = mock_async_client(gamma_market_payload, clob_book_payload)
        market = gamma_market_to_market(gamma_market_payload)
        contracts = gamma_market_to_contracts(gamma_market_payload)
        adapter = PolymarketMarketDataAdapter(
            clob_base_url="https://example.test",
            client=client,
            markets=(market,),
            contracts=contracts,
        )

        markets = await adapter.list_markets(VenueID("KALSHI"))

        assert markets == ()
        await client.aclose()

    asyncio.run(run_test())


def test_get_market_and_list_contracts_use_gamma_market(
    gamma_market_payload,
    clob_book_payload,
    mock_async_client,
):
    async def run_test():
        client = mock_async_client(gamma_market_payload, clob_book_payload)
        market = gamma_market_to_market(gamma_market_payload)
        contracts = gamma_market_to_contracts(gamma_market_payload)
        adapter = PolymarketMarketDataAdapter(
            clob_base_url="https://example.test",
            client=client,
            markets=(market,),
            contracts=contracts,
        )

        stored_market = await adapter.get_market(MarketID("0xabc"))
        assert stored_market is not None
        contracts = await adapter.list_contracts(stored_market.id)

        assert len(contracts) == 2
        assert str(contracts[0].id) == "polymarket:0xabc:123"
        assert str(contracts[1].id) == "polymarket:0xabc:456"
        await client.aclose()

    asyncio.run(run_test())


def test_get_order_book_fetches_clob_book(
    gamma_market_payload,
    clob_book_payload,
    mock_async_client,
):
    async def run_test():
        client = mock_async_client(gamma_market_payload, clob_book_payload)
        market = gamma_market_to_market(gamma_market_payload)
        contracts = gamma_market_to_contracts(gamma_market_payload)
        adapter = PolymarketMarketDataAdapter(
            clob_base_url="https://example.test",
            client=client,
            markets=(market,),
            contracts=contracts,
        )

        stored_market = await adapter.get_market(MarketID("0xabc"))
        assert stored_market is not None
        contract = (await adapter.list_contracts(stored_market.id))[0]
        order_book = await adapter.get_order_book(contract.id)

        assert order_book.best_bid().price.value == Decimal("0.45")
        assert order_book.best_ask().price.value == Decimal("0.55")
        await client.aclose()

    asyncio.run(run_test())
