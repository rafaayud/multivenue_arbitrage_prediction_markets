"""Exercise market data integration behavior in the infrastructure polymarket layer.

Responsibilities
----------------
- Verify market data integration contracts, edge cases, and failure handling.
"""

import asyncio
import os

import pytest

from prediction_markets.infrastructure.venues.polymarket.market_data import PolymarketMarketDataAdapter


@pytest.mark.integration
def test_list_markets_live_fetches_gamma_markets():
    if os.getenv("RUN_POLYMARKET_INTEGRATION") != "1":
        pytest.skip("Set RUN_POLYMARKET_INTEGRATION=1 to call the live Polymarket API")

    async def run_test():
        adapter = PolymarketMarketDataAdapter(page_size=5)
        try:
            markets = await adapter.list_markets()
        finally:
            await adapter.close()

        assert markets
        assert all(str(market.id) for market in markets)
        assert all(str(market.venue_id) == "POLYMARKET" for market in markets)
        assert all(market.title for market in markets)

    asyncio.run(run_test())
