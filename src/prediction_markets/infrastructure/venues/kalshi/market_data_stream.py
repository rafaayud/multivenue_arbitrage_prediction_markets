"""Integrate kalshi market data stream with domain ports.

Responsibilities
----------------
- Perform venue I/O and translate external data into domain models.
"""

import base64
import json
import os
import time
from collections.abc import AsyncIterator
from dataclasses import replace
from decimal import Decimal
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import padding
from prediction_markets.domain.contracts.entities import BinaryContract
from prediction_markets.domain.orderbook.entities import OrderBook
from prediction_markets.domain.orderbook.value_objects import OrderBookLevel
from prediction_markets.domain.ports.market_data_stream import MarketDataStreamPort
from prediction_markets.domain.shared.value_objects import ContractID, Price, Quantity
from tenacity import AsyncRetrying, retry_if_exception_type, wait_exponential
from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed

from prediction_markets.infrastructure.venues.kalshi.mappers import (
    KALSHI_VENUE_ID,
    kalshi_orderbook_to_order_book,
    parse_kalshi_contract_id,
)


class KalshiMarketDataStreamAdapter(MarketDataStreamPort):
    """Async Kalshi market-data stream adapter using Kalshi WebSocket messages."""

    venue_id = KALSHI_VENUE_ID

    def __init__(
        self,
        contracts: tuple[BinaryContract, ...] = (),
        websocket_url: str = "wss://external-api-ws.kalshi.com/trade-api/ws/v2",
        api_key_id: str | None = None,
        private_key_path: str | Path | None = None,
    ) -> None:

        self._contracts = {contract.id: contract for contract in contracts}
        self._websocket_url = websocket_url
        self._api_key_id = api_key_id or os.getenv("KALSHI_API_KEY_ID")
        configured_private_key_path = private_key_path or os.getenv(
            "KALSHI_PRIVATE_KEY_PATH"
        )
        self._private_key_path = (
            Path(configured_private_key_path) if configured_private_key_path else None
        )

    async def stream_order_books(
        self,
        contract_ids: tuple[ContractID, ...],
    ) -> AsyncIterator[tuple[ContractID, OrderBook]]:
        """Stream normalized order books from kalshi's live transport.

        Yields
        ------
        OrderBook
            Valid snapshots for the requested contracts.
        """
        contract_ids = tuple(dict.fromkeys(contract_ids))
        if not contract_ids:
            return

        contracts_by_ticker: dict[str, list[tuple[ContractID, str]]] = {}
        for contract_id in contract_ids:
            ticker, outcome = parse_kalshi_contract_id(contract_id)
            contracts_by_ticker.setdefault(ticker, []).append((contract_id, outcome))
        tickers = tuple(contracts_by_ticker)

        async for attempt in AsyncRetrying(
            retry=retry_if_exception_type((ConnectionClosed, OSError)),
            wait=wait_exponential(multiplier=1, min=4, max=15),
            reraise=True,
        ):
            with attempt:
                current_books: dict[ContractID, OrderBook] = {}
                async with connect(
                    self._websocket_url,
                    additional_headers=self._authentication_headers(),
                ) as websocket:
                    await websocket.send(json.dumps(_subscription_message(tickers)))

                    async for raw_message in websocket:
                        received_at_ns = time.monotonic_ns()
                        for message in _decode_messages(raw_message):
                            message_type = _message_type(message)
                            payload = _message_payload(message)
                            ticker = str(
                                payload.get("market_ticker")
                                or payload.get("ticker")
                                or (tickers[0] if len(tickers) == 1 else "")
                            )
                            subscriptions = contracts_by_ticker.get(ticker, ())
                            if not subscriptions:
                                continue

                            if message_type == "orderbook_snapshot":
                                for contract_id, _ in subscriptions:
                                    current_book = replace(
                                        kalshi_orderbook_to_order_book(
                                            payload,
                                            contract=self._contracts.get(contract_id),
                                            contract_id=contract_id,
                                        ),
                                        received_at_ns=received_at_ns,
                                    )
                                    current_books[contract_id] = current_book
                                    yield contract_id, current_book

                            elif message_type == "orderbook_delta":
                                for contract_id, outcome in subscriptions:
                                    updated_book = _apply_orderbook_delta(
                                        current_book=current_books.get(contract_id),
                                        message=payload,
                                        ticker=ticker,
                                        outcome=outcome,
                                    )
                                    if updated_book is None:
                                        continue
                                    current_book = replace(
                                        updated_book,
                                        received_at_ns=received_at_ns,
                                    )
                                    current_books[contract_id] = current_book
                                    yield contract_id, current_book

                    raise ConnectionError("Kalshi WebSocket closed")

    def add_contracts(self, contracts: tuple[BinaryContract, ...]) -> None:
        """Register contracts used to translate subsequent venue order-book updates."""
        for contract in contracts:
            self._contracts[contract.id] = contract

    def _authentication_headers(self) -> dict[str, str]:
        """Sign the Kalshi WebSocket handshake when API credentials are configured."""
        if not self._api_key_id:
            raise ValueError("Kalshi WebSocket requires KALSHI_API_KEY_ID")
        if self._private_key_path is None:
            raise ValueError("Kalshi WebSocket requires KALSHI_PRIVATE_KEY_PATH")

        timestamp = str(int(time.time() * 1000))
        parsed_url = urlparse(self._websocket_url)
        path = parsed_url.path or "/trade-api/ws/v2"
        message = f"{timestamp}GET{path}"

        with self._private_key_path.open("rb") as private_key_file:
            private_key = serialization.load_pem_private_key(
                private_key_file.read(),
                password=None,
            )

        signature = private_key.sign(
            message.encode("utf-8"),
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=padding.PSS.DIGEST_LENGTH,
            ),
            hashes.SHA256(),
        )

        return {
            "KALSHI-ACCESS-KEY": self._api_key_id,
            "KALSHI-ACCESS-SIGNATURE": base64.b64encode(signature).decode("utf-8"),
            "KALSHI-ACCESS-TIMESTAMP": timestamp,
        }


def _subscription_message(tickers: str | tuple[str, ...]) -> dict[str, Any]:
    market_tickers = [tickers] if isinstance(tickers, str) else list(tickers)
    return {
        "id": 1,
        "cmd": "subscribe",
        "params": {
            "channels": ["orderbook_delta"],
            "market_tickers": market_tickers,
        },
    }


def _apply_orderbook_delta(
    *,
    current_book: OrderBook | None,
    message: dict[str, Any],
    ticker: str,
    outcome: str,
) -> OrderBook | None:

    """Apply a Kalshi order-book delta to cached bid and ask maps."""
    if current_book is None:
        return None

    if str(message.get("market_ticker") or message.get("ticker") or ticker) != ticker:
        return None

    side = str(message.get("side") or "").strip().lower()
    if side not in {"yes", "no"}:
        return None

    price = _price_from_message(message)
    if price is None:
        return None

    delta = _quantity_delta(message)
    if delta is None:
        return None

    bids = {level.price.value: level for level in current_book.bids}
    asks = {level.price.value: level for level in current_book.asks}

    if side == outcome:
        _apply_bid_delta(bids, price=price, delta=delta)
    else:
        ask_price = Decimal("1") - price
        _apply_bid_delta(asks, price=ask_price, delta=delta)

    return OrderBook(
        market_id=current_book.market_id,
        outcome_id=current_book.outcome_id,
        bids=tuple(
            sorted(bids.values(), key=lambda level: level.price.value, reverse=True)
        ),
        asks=tuple(sorted(asks.values(), key=lambda level: level.price.value)),
        timestamp=current_book.timestamp,
    )


def _apply_bid_delta(
    levels: dict[Decimal, OrderBookLevel],
    *,
    price: Decimal,
    delta: Decimal,
) -> None:
    current_quantity = (
        levels.get(price).quantity.value if price in levels else Decimal("0")
    )
    next_quantity = current_quantity + delta
    if next_quantity <= 0:
        levels.pop(price, None)
        return

    levels[price] = OrderBookLevel(
        price=Price(price),
        quantity=Quantity(next_quantity),
    )


def _decode_messages(raw_message: str | bytes) -> tuple[dict[str, Any], ...]:
    """Decode one transport frame into valid JSON message objects."""
    data = json.loads(raw_message)
    if isinstance(data, list):
        return tuple(item for item in data if isinstance(item, dict))
    if isinstance(data, dict):
        return (data,)
    return ()


def _message_type(message: dict[str, Any]) -> str:
    return str(message.get("type") or message.get("event_type") or "").strip().lower()


def _message_payload(message: dict[str, Any]) -> dict[str, Any]:
    payload = message.get("msg") or message.get("data") or message
    return payload if isinstance(payload, dict) else message


def _price_from_message(message: dict[str, Any]) -> Decimal | None:
    """Extract the affected price from a venue delta message."""
    value = message.get("price") or message.get("price_dollars")
    if value is None:
        return None

    price = Decimal(str(value))
    if price > 1:
        return price / Decimal("100")
    return price


def _quantity_delta(message: dict[str, Any]) -> Decimal | None:
    """Extract the signed quantity change from a venue delta message."""
    value = message.get("delta")
    if value is None:
        value = (
            message.get("delta_fp")
            or message.get("size_delta")
            or message.get("size_delta_fp")
            or message.get("quantity_delta")
            or message.get("quantity_delta_fp")
        )
    if value is None:
        return None
    return Decimal(str(value))
