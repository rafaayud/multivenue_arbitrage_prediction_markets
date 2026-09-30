"""Integrate polynode market data stream with domain ports.

Responsibilities
----------------
- Perform venue I/O and translate external data into domain models.
"""

import json
import time
from collections.abc import AsyncIterator
from dataclasses import replace
from decimal import Decimal
from typing import Any
from urllib.parse import urlencode

from tenacity import AsyncRetrying, retry_if_exception_type, wait_exponential
from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed

from prediction_markets.domain.contracts.entities import BinaryContract
from prediction_markets.domain.orderbook.entities import OrderBook
from prediction_markets.domain.orderbook.value_objects import OrderBookLevel
from prediction_markets.domain.ports.market_data_stream import MarketDataStreamPort
from prediction_markets.domain.shared.value_objects import ContractID, Price, Quantity
from prediction_markets.infrastructure.venues.polymarket.mappers import (
    POLYMARKET_VENUE_ID,
    parse_polymarket_contract_id,
)
from prediction_markets.infrastructure.venues.polynode.config import polynode_api_key
from prediction_markets.infrastructure.venues.polynode.mappers import polynode_orderbook_to_order_book


class PolynodeMarketDataStreamAdapter(MarketDataStreamPort):
    """Streams Polymarket order books from Polynode's direct order-book feed."""

    venue_id = POLYMARKET_VENUE_ID

    def __init__(
        self,
        contracts: tuple[BinaryContract, ...] = (),
        *,
        api_key: str | None = None,
        websocket_url: str = "wss://ob.polynode.dev/ws",
    ) -> None:
        self._contracts = {contract.id: contract for contract in contracts}
        self._api_key = api_key or polynode_api_key()
        self._websocket_url = websocket_url

    async def stream_order_books(
        self,
        contract_ids: tuple[ContractID, ...],
    ) -> AsyncIterator[tuple[ContractID, OrderBook]]:
        """Stream normalized order books from polynode's live transport.

        Yields
        ------
        OrderBook
            Valid snapshots for the requested contracts.
        """
        if not self._api_key:
            raise ValueError("Polynode WebSocket requires api_key")

        contract_ids = tuple(dict.fromkeys(contract_ids))
        if not contract_ids:
            return

        contracts_by_token: dict[str, ContractID] = {}
        for contract_id in contract_ids:
            _, token_id = parse_polymarket_contract_id(contract_id)
            contracts_by_token[token_id] = contract_id
        token_ids = tuple(contracts_by_token)

        async for attempt in AsyncRetrying(
            retry=retry_if_exception_type((ConnectionClosed, OSError)),
            wait=wait_exponential(multiplier=1, min=4, max=15),
            reraise=True,
        ):
            with attempt:
                current_books: dict[str, OrderBook] = {}
                async with connect(
                    _websocket_url(self._websocket_url, self._api_key)
                ) as websocket:
                    await websocket.send(
                        json.dumps({"action": "subscribe", "markets": list(token_ids)})
                    )

                    async for raw_message in websocket:
                        received_at_ns = time.monotonic_ns()
                        for message in _decode_messages(raw_message):
                            message_type = str(message.get("type") or "").lower()

                            if message_type in {
                                "snapshot",
                                "book_snapshot",
                                "snapshot_batch",
                            }:
                                for token_id in token_ids:
                                    snapshot = _snapshot_for_token(message, token_id)
                                    if snapshot is None:
                                        continue
                                    contract_id = contracts_by_token[token_id]
                                    current_book = replace(
                                        polynode_orderbook_to_order_book(
                                            snapshot,
                                            contract=self._contracts.get(contract_id),
                                            contract_id=contract_id,
                                        ),
                                        received_at_ns=received_at_ns,
                                    )
                                    current_books[token_id] = current_book
                                    yield contract_id, current_book

                            elif message_type == "batch":
                                for update in message.get("updates", []):
                                    if not isinstance(update, dict):
                                        continue
                                    for token_id in _update_token_ids(update, token_ids):
                                        updated_book = self._apply_update(
                                            current_book=current_books.get(token_id),
                                            update=update,
                                            token_id=token_id,
                                        )
                                        if updated_book is None:
                                            continue
                                        current_book = replace(
                                            updated_book,
                                            received_at_ns=received_at_ns,
                                        )
                                        current_books[token_id] = current_book
                                        yield contracts_by_token[token_id], current_book

                            elif message_type in {"price_change", "book_update"}:
                                for token_id in _update_token_ids(message, token_ids):
                                    updated_book = self._apply_update(
                                        current_book=current_books.get(token_id),
                                        update=message,
                                        token_id=token_id,
                                    )
                                    if updated_book is not None:
                                        current_book = replace(
                                            updated_book,
                                            received_at_ns=received_at_ns,
                                        )
                                        current_books[token_id] = current_book
                                        yield contracts_by_token[token_id], current_book

                    raise ConnectionError("Polynode WebSocket closed")

    def add_contracts(self, contracts: tuple[BinaryContract, ...]) -> None:
        """Register contracts used to translate subsequent venue order-book updates."""
        for contract in contracts:
            self._contracts[contract.id] = contract

    @staticmethod
    def _apply_update(
        *,
        current_book: OrderBook | None,
        update: dict[str, Any],
        token_id: str,
    ) -> OrderBook | None:
        """Apply a Polynode snapshot or delta and emit every affected normalized book."""
        if current_book is None:
            return None

        update_type = str(update.get("type") or "").lower()
        if update_type == "price_change":
            changes = update.get("assets", [])
        elif update_type == "book_update":
            changes = [
                *(
                    _level_change(level, "BUY", token_id)
                    for level in update.get("bids", [])
                ),
                *(
                    _level_change(level, "SELL", token_id)
                    for level in update.get("asks", [])
                ),
            ]
        else:
            return None

        bids = {level.price.value: level for level in current_book.bids}
        asks = {level.price.value: level for level in current_book.asks}
        changed = False

        for change in changes:
            if not isinstance(change, dict) or str(change.get("asset_id")) != token_id:
                continue
            price = Decimal(str(change["price"]))
            size = Decimal(str(change["size"]))
            levels = bids if str(change.get("side")).upper() == "BUY" else asks
            if size <= 0:
                changed = levels.pop(price, None) is not None or changed
            else:
                levels[price] = OrderBookLevel(Price(price), Quantity(size))
                changed = True

        if not changed:
            return None

        return OrderBook(
            market_id=current_book.market_id,
            outcome_id=current_book.outcome_id,
            bids=tuple(
                sorted(bids.values(), key=lambda level: level.price.value, reverse=True)
            ),
            asks=tuple(sorted(asks.values(), key=lambda level: level.price.value)),
            timestamp=current_book.timestamp,
        )


def _websocket_url(base_url: str, api_key: str) -> str:
    separator = "&" if "?" in base_url else "?"
    return f"{base_url}{separator}{urlencode({'key': api_key})}"


def _snapshot_for_token(
    message: dict[str, Any], token_id: str
) -> dict[str, Any] | None:
    """Build one normalized order book from cached token-side state."""
    snapshots = (
        message.get("snapshots", [])
        if message.get("type") == "snapshot_batch"
        else [message]
    )
    for snapshot in snapshots:
        if isinstance(snapshot, dict) and str(snapshot.get("asset_id")) == token_id:
            return snapshot
    return None


def _level_change(level: Any, side: str, token_id: str) -> dict[str, Any]:
    return (
        {**level, "asset_id": token_id, "side": side} if isinstance(level, dict) else {}
    )


def _update_token_ids(
    update: dict[str, Any],
    subscribed_token_ids: tuple[str, ...],
) -> tuple[str, ...]:
    """Update subscribed token identifiers from a venue control message."""
    subscribed = set(subscribed_token_ids)
    if str(update.get("type") or "").lower() == "price_change":
        return tuple(
            dict.fromkeys(
                str(change.get("asset_id"))
                for change in update.get("assets", [])
                if isinstance(change, dict)
                and str(change.get("asset_id")) in subscribed
            )
        )

    token_id = str(
        update.get("asset_id")
        or update.get("market")
        or update.get("token_id")
        or ""
    )
    if token_id in subscribed:
        return (token_id,)
    return subscribed_token_ids if len(subscribed_token_ids) == 1 else ()


def _decode_messages(raw_message: str | bytes) -> tuple[dict[str, Any], ...]:
    """Decode one transport frame into valid JSON message objects."""
    data = json.loads(raw_message)
    if isinstance(data, list):
        return tuple(item for item in data if isinstance(item, dict))
    return (data,) if isinstance(data, dict) else ()
