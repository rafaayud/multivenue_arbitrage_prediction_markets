"""Inspect public outcome metadata and native WebSocket books without credentials."""

import argparse
import asyncio
import json

from prediction_markets.domain.ports.instrument_discovery import InstrumentDiscoveryQuery
from prediction_markets.infrastructure.venues.hyperliquid.catalog import HyperliquidMarketCatalog
from prediction_markets.infrastructure.venues.hyperliquid.config import HyperliquidSettings
from prediction_markets.infrastructure.venues.hyperliquid.health import HyperliquidHealthAdapter
from prediction_markets.infrastructure.venues.hyperliquid.instrument_discovery import HyperliquidInstrumentDiscoveryAdapter
from prediction_markets.infrastructure.venues.hyperliquid.market_data_stream import HyperliquidMarketDataStreamAdapter


async def inspect_outcome(network: str, market_id: str | None) -> None:
    """Print metadata and the first native snapshot for each side within 30 seconds.

    Parameters
    ----------
    network
        Public venue network, either mainnet or testnet.
    market_id
        Native numeric outcome id, or ``None`` for the first supported active one.

    Raises
    ------
    LookupError
        If the selected YES/NO outcome is not listed.
    TimeoutError
        If both initial snapshots do not arrive within 30 seconds.
    """
    settings = HyperliquidSettings(network)
    catalog = HyperliquidMarketCatalog(settings=settings)
    try:
        result = await HyperliquidInstrumentDiscoveryAdapter(catalog=catalog).discover(
            InstrumentDiscoveryQuery(venue_market_id=market_id, limit=1),
        )
        if not result.markets:
            raise LookupError("No supported active Hyperliquid YES/NO outcome found")
        health = await HyperliquidHealthAdapter(catalog=catalog).check()
        market = result.markets[0]
        print(json.dumps({"network": network, "market_id": str(market.id),
                          "title": market.title, "health": health.status.value,
                          "trading_enabled": False}, ensure_ascii=True), flush=True)
        pending = {contract.id for contract in result.contracts}
        stream = HyperliquidMarketDataStreamAdapter(settings=settings).stream_order_books(tuple(pending))
        try:
            async with asyncio.timeout(30):
                async for contract, book in stream:
                    if not book.source_at_ns or contract not in pending:
                        continue
                    print(json.dumps({
                        "contract_id": str(contract), "source_at_ns": book.source_at_ns,
                        "bids": [[str(level.price.value), str(level.quantity.value)] for level in book.bids[:3]],
                        "asks": [[str(level.price.value), str(level.quantity.value)] for level in book.asks[:3]],
                    }), flush=True)
                    pending.remove(contract)
                    if not pending:
                        break
        finally:
            await stream.aclose()
    finally:
        await catalog.close()


def main() -> None:
    """Run the read-only probe with a selected network and optional native outcome."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--network", choices=("mainnet", "testnet"), default="mainnet")
    parser.add_argument("--market", help="Native outcome id; omit to inspect the first active YES/NO outcome")
    args = parser.parse_args()
    asyncio.run(inspect_outcome(args.network, args.market))


if __name__ == "__main__":
    main()
