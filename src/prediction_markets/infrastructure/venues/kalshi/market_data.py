"""Integrate kalshi market data with domain ports.

Responsibilities
----------------
- Perform venue I/O and translate external data into domain models.
"""

from typing import Any

import httpx
from prediction_markets.domain.contracts.entities import BinaryContract
from prediction_markets.domain.markets.entities import Market
from prediction_markets.domain.orderbook.entities import OrderBook
from prediction_markets.domain.ports.market_data import (
    MarketDataPort,
)
from prediction_markets.domain.shared.value_objects import (
    ContractID,
    MarketID,
    VenueID,
)
from prediction_markets.infrastructure.http_client import instrumented_async_client

from prediction_markets.infrastructure.venues.kalshi.mappers import kalshi_orderbook_to_order_book
from prediction_markets.infrastructure.venues.kalshi.mappers import parse_kalshi_contract_id


class KalshiMarketDataAdapter(MarketDataPort):
    """Async Kalshi snapshot market-data adapter using Kalshi REST directly."""

    def __init__(
        self,
        *,
        base_url: str = "https://external-api.kalshi.com/trade-api/v2",
        timeout_seconds: float = 10.0,
        client: httpx.AsyncClient | None = None,
        markets: tuple[Market, ...] = (),
        contracts: tuple[BinaryContract, ...] = (),
    ) -> None:
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")

        self._base_url = base_url.rstrip("/")
        self._owns_client = client is None
        self._client = client or instrumented_async_client(
            "kalshi",
            timeout=timeout_seconds,
        )
        self._markets = {market.id: market for market in markets}
        self._contracts = {contract.id: contract for contract in contracts}


    async def close(self) -> None:
        """Release network resources owned by the adapter.

        Notes
        -----
        - A caller-supplied client remains under caller ownership.
        """
        if self._owns_client:
            await self._client.aclose()


    async def list_markets(self, venue_id: VenueID | None = None) -> tuple[Market, ...]:
        """Return the normalized markets currently cached by the adapter."""
        markets = tuple(self._markets.values())
        if venue_id is None:
            return markets
        return tuple(market for market in markets if market.venue_id == venue_id)


    async def get_market(self, market_id: MarketID) -> Market | None:
        return self._markets.get(market_id)


    async def list_contracts(self, market_id: MarketID) -> tuple[BinaryContract, ...]:
        return tuple(
            contract
            for contract in self._contracts.values()
            if contract.market_id == market_id
        )


    async def get_order_book(self, contract_id: ContractID) -> OrderBook | None:
        """Fetch and normalize the latest kalshi order book for one contract.

        Returns
        -------
        OrderBook
            The current normalized snapshot.
        """
        raw_book = await self.get_raw_order_book(contract_id)
        if not raw_book:
            return None

        contract = self._contracts.get(contract_id)
        return kalshi_orderbook_to_order_book(raw_book, contract=contract, contract_id=contract_id)


    async def get_raw_order_book(self, contract_id: ContractID) -> dict[str, Any] | None:
        """Fetch the latest raw kalshi order-book payload for one contract.

        Notes
        -----
        - Performs venue HTTP I/O and raises on unsuccessful responses.
        """
        ticker, _ = parse_kalshi_contract_id(contract_id)
        response = await self._client.get(
            f"{self._base_url}/markets/{ticker}/orderbook",
        )
        response.raise_for_status()

        data = response.json()
        if not isinstance(data, dict):
            raise TypeError(
                f"Unexpected Kalshi orderbook response for ticker {ticker}: "
                f"{type(data).__name__}"
            )

        return data


    def add_markets(self, markets: tuple[Market, ...]) -> None:
        """Register markets used to translate subsequent venue payloads."""
        for market in markets:
            self._markets[market.id] = market


    def add_contracts(self, contracts: tuple[BinaryContract, ...]) -> None:
        """Register contracts used to translate subsequent venue order-book updates."""
        for contract in contracts:
            self._contracts[contract.id] = contract
