"""Integrate predict market data with domain ports.

Responsibilities
----------------
- Perform venue I/O and translate external data into domain models.
"""

from typing import Any
from dataclasses import replace
import time

import httpx

from prediction_markets.domain.contracts.entities import BinaryContract
from prediction_markets.domain.markets.entities import Market
from prediction_markets.domain.orderbook.entities import OrderBook
from prediction_markets.domain.ports.market_data import MarketDataPort
from prediction_markets.domain.shared.value_objects import (
    ContractID,
    MarketID,
    VenueID,
)
from prediction_markets.infrastructure.http_client import instrumented_async_client
from prediction_markets.infrastructure.venues.predict.config import (
    predict_api_key,
    predict_headers,
)
from prediction_markets.infrastructure.venues.predict.mappers import (
    parse_predict_contract_id,
    predict_orderbook_to_order_book,
)


class PredictMarketDataAdapter(MarketDataPort):
    """Predict.fun REST order-book snapshots."""

    def __init__(
        self,
        *,
        api_key: str | None = None,
        base_url: str = "https://api.predict.fun",
        timeout_seconds: float = 10.0,
        client: httpx.AsyncClient | None = None,
        markets: tuple[Market, ...] = (),
        contracts: tuple[BinaryContract, ...] = (),
    ) -> None:
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")

        self._api_key = predict_api_key(api_key)
        self._base_url = base_url.rstrip("/")
        self._headers = predict_headers(self._api_key)
        self._owns_client = client is None
        self._client = client or instrumented_async_client(
            "predict",
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

    async def list_markets(
        self,
        venue_id: VenueID | None = None,
    ) -> tuple[Market, ...]:
        """Return the normalized markets currently cached by the adapter."""
        markets = tuple(self._markets.values())
        if venue_id is None:
            return markets
        return tuple(market for market in markets if market.venue_id == venue_id)

    async def get_market(self, market_id: MarketID) -> Market | None:
        return self._markets.get(market_id)

    async def list_contracts(
        self,
        market_id: MarketID,
    ) -> tuple[BinaryContract, ...]:
        return tuple(
            contract
            for contract in self._contracts.values()
            if contract.market_id == market_id
        )

    async def get_order_book(self, contract_id: ContractID) -> OrderBook | None:
        """Fetch and normalize the latest predict order book for one contract.

        Returns
        -------
        OrderBook
            The normalized snapshot with its venue mutation time, when supplied,
            and local monotonic REST-receipt time. WebSocket transport-arrival
            fields are not fabricated for an HTTP response.
        """
        raw_book = await self.get_raw_order_book(contract_id)
        if raw_book is None:
            return None
        received_at_ns = time.monotonic_ns()
        book = predict_orderbook_to_order_book(
            raw_book,
            contract=self._contracts.get(contract_id),
            contract_id=contract_id,
        )
        # REST completion is not a WebSocket transport callback. Preserve the
        # venue mutation timestamp and label its snapshot semantics explicitly.
        return replace(book, received_at_ns=received_at_ns,
            source_at_ns=int(book.timestamp.value.timestamp() * 1_000_000_000)
                if book.timestamp is not None else None,
            source_timestamp_kind="snapshot_state")

    async def get_raw_order_book(
        self,
        contract_id: ContractID,
    ) -> dict[str, Any] | None:
        """Fetch the latest raw predict order-book payload for one contract.

        Notes
        -----
        - Performs venue HTTP I/O and raises on unsuccessful responses.
        """
        market_id, _ = parse_predict_contract_id(contract_id)
        response = await self._client.get(
            f"{self._base_url}/v1/markets/{market_id}/orderbook",
            headers=self._headers,
        )
        if response.status_code == 404:
            return None
        response.raise_for_status()
        payload = response.json()
        if (
            not isinstance(payload, dict)
            or payload.get("success") is not True
            or not isinstance(payload.get("data"), dict)
        ):
            raise TypeError("Unexpected Predict orderbook response")
        return payload["data"]

    def add_markets(self, markets: tuple[Market, ...]) -> None:
        """Register markets used to translate subsequent venue payloads."""
        for market in markets:
            self._markets[market.id] = market

    def add_contracts(self, contracts: tuple[BinaryContract, ...]) -> None:
        """Register contracts used to translate subsequent venue order-book updates."""
        for contract in contracts:
            self._contracts[contract.id] = contract
