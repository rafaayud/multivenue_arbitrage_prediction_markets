"""Read Hyperliquid outcome metadata and full L2 books through the public API."""

from prediction_markets.domain.contracts.entities import BinaryContract
from prediction_markets.domain.markets.entities import Market
from prediction_markets.domain.orderbook.entities import OrderBook
from prediction_markets.domain.ports.market_data import MarketDataPort
from prediction_markets.domain.shared.value_objects import ContractID, MarketID, VenueID
from prediction_markets.infrastructure.venues.hyperliquid.catalog import HyperliquidMarketCatalog
from prediction_markets.infrastructure.venues.hyperliquid.mappers import (
    HYPERLIQUID_VENUE_ID, hyperliquid_orderbook_to_order_book, outcome_coin,
)


class HyperliquidMarketDataAdapter(MarketDataPort):
    """Read native outcome books, preserving their independent YES/NO identities."""

    def __init__(self, *, catalog: HyperliquidMarketCatalog | None = None) -> None:
        self._owns_catalog = catalog is None
        self._catalog = catalog or HyperliquidMarketCatalog()

    async def close(self) -> None:
        if self._owns_catalog:
            await self._catalog.close()

    async def list_markets(self, venue_id: VenueID | None = None) -> tuple[Market, ...]:
        if venue_id not in (None, HYPERLIQUID_VENUE_ID):
            return ()
        return tuple(market for market, _ in await self._catalog.instruments())

    async def get_market(self, market_id: MarketID) -> Market | None:
        return next((market for market in await self.list_markets() if market.id == market_id), None)

    async def list_contracts(self, market_id: MarketID) -> tuple[BinaryContract, ...]:
        return tuple(contract for market, contracts in await self._catalog.instruments()
                     if market.id == market_id for contract in contracts)

    async def get_order_book(self, contract_id: ContractID) -> OrderBook | None:
        """Fetch a full native book; a missing venue snapshot remains unavailable."""
        payload = await self._catalog.info({"type": "l2Book", "coin": outcome_coin(contract_id)})
        return None if payload is None else hyperliquid_orderbook_to_order_book(payload, contract_id)
