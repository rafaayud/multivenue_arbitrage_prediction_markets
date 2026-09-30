"""Exercise kalshi market data stream integration behavior in the infrastructure kalshi layer.

Responsibilities
----------------
- Verify kalshi market data stream integration contracts, edge cases, and failure handling.
"""

import asyncio
import os

import pytest

from prediction_markets.domain.ports.instrument_discovery import InstrumentDiscoveryQuery
from prediction_markets.infrastructure.venues.kalshi.market_data_stream import KalshiMarketDataStreamAdapter
from prediction_markets.infrastructure.venues.kalshi.instrument_discovery import KalshiMarketDiscoveryAdapter


@pytest.mark.integration
def test_kalshi_stream_live_receives_orderbook_snapshot():
    if os.getenv("RUN_KALSHI_INTEGRATION") != "1":
        pytest.skip("Set RUN_KALSHI_INTEGRATION=1 to call the live Kalshi API")
    if not os.getenv("KALSHI_API_KEY_ID"):
        pytest.skip("Set KALSHI_API_KEY_ID to authenticate the Kalshi WebSocket")
    if not os.getenv("KALSHI_PRIVATE_KEY_PATH"):
        pytest.skip("Set KALSHI_PRIVATE_KEY_PATH to authenticate the Kalshi WebSocket")

    async def run_test():
        discovery = KalshiMarketDiscoveryAdapter()
        try:
            contracts = await discovery.discover_contracts(
                InstrumentDiscoveryQuery(limit=20)
            )
        finally:
            await discovery.close()

        assert contracts
        stream = KalshiMarketDataStreamAdapter(contracts=contracts)
        order_books = stream.stream_order_book(contracts[0].id)
        try:
            order_book = await asyncio.wait_for(anext(order_books), timeout=25.0)
        finally:
            await order_books.aclose()

        assert order_book.market_id == contracts[0].market_id
        assert order_book.outcome_id == contracts[0].outcome_id
        assert order_book.bids or order_book.asks

    asyncio.run(run_test())
