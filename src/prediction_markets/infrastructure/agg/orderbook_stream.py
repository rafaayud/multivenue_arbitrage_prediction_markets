"""Integrate agg orderbook stream with domain ports.

Responsibilities
----------------
- Perform venue I/O and translate external data into domain models.
"""

import json
import os
import time
from collections.abc import AsyncIterator
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

from dotenv import load_dotenv
from tenacity import AsyncRetrying, retry_if_exception_type, wait_exponential
from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed

from prediction_markets.domain.contracts.entities import BinaryContract
from prediction_markets.domain.orderbook.entities import OrderBook
from prediction_markets.domain.orderbook.value_objects import OrderBookLevel
from prediction_markets.domain.ports.market_data_stream import MarketDataStreamPort
from prediction_markets.domain.shared.value_objects import (
    ContractID,
    Price,
    Quantity,
    Timestamp,
    VenueID,
)
from prediction_markets.infrastructure.agg.arbitrage_stream import _websocket_url


class AggOrderBookStreamAdapter(MarketDataStreamPort):
    """Streams AGG order books for known venue-market outcomes."""

    # AGG is a multi-venue feed; this identifies the transport, not a venue.
    venue_id = VenueID("AGG")

    def __init__(
        self,
        contracts: tuple[BinaryContract, ...] = (),
        *,
        app_id: str | None = None,
        origin: str | None = None,
        websocket_url: str = "wss://ws.agg.market/ws",
    ) -> None:
        load_dotenv()
        self._contracts = {contract.id: contract for contract in contracts}
        self._app_id = (app_id or os.getenv("AGG_APP_ID") or "").strip()
        self._origin = (origin or os.getenv("AGG_ORIGIN") or "").strip()
        self._websocket_url = websocket_url

    async def stream_order_books(
        self,
        contract_ids: tuple[ContractID, ...],
    ) -> AsyncIterator[tuple[ContractID, OrderBook]]:
        """Stream normalized order books from agg's live transport.

        Yields
        ------
        OrderBook
            Valid snapshots for the requested contracts.
        """
        if not self._app_id:
            raise ValueError("AGG WebSocket requires app_id or AGG_APP_ID")
        if not self._origin:
            raise ValueError("AGG WebSocket requires origin or AGG_ORIGIN")

        contract_ids = tuple(dict.fromkeys(contract_ids))
        if not contract_ids:
            return

        missing = tuple(cid for cid in contract_ids if cid not in self._contracts)
        if missing:
            raise ValueError(f"Unknown AGG contracts: {', '.join(map(str, missing))}")

        by_outcome = {
            str(self._contracts[contract_id].outcome_id): contract_id
            for contract_id in contract_ids
        }

        async for attempt in AsyncRetrying(
            retry=retry_if_exception_type((ConnectionClosed, OSError)),
            wait=wait_exponential(multiplier=1, min=1, max=30),
            reraise=True,
        ):
            with attempt:
                books: dict[ContractID, OrderBook] = {}
                sequences: dict[str, int] = {}
                url = _websocket_url(self._websocket_url, self._app_id)
                async with connect(url, origin=self._origin) as websocket:
                    await websocket.send(_subscription_message(tuple(by_outcome)))

                    async for raw_message in websocket:
                        message = _decode_message(raw_message)
                        if message is None:
                            continue
                        if message.get("type") == "error":
                            detail = message.get("message", "unknown error")
                            raise RuntimeError(f"AGG WebSocket error: {detail}")

                        outcome_id = str(message.get("outcomeId") or "")
                        contract_id = by_outcome.get(outcome_id)
                        if contract_id is None:
                            continue

                        message_type = message.get("type")
                        if message_type == "orderbook_snapshot":
                            book = _snapshot_to_order_book(
                                message,
                                self._contracts[contract_id],
                            )
                        elif message_type == "orderbook_delta":
                            sequence = _int_or_none(message.get("seq"))
                            current_sequence = sequences.get(outcome_id)
                            if sequence is None or current_sequence is None:
                                await websocket.send(_resnapshot_message(outcome_id))
                                continue
                            if sequence <= current_sequence:
                                continue
                            if sequence != current_sequence + 1:
                                books.pop(contract_id, None)
                                sequences.pop(outcome_id, None)
                                await websocket.send(_resnapshot_message(outcome_id))
                                continue
                            book = _apply_delta(books.get(contract_id), message)
                            if book is None:
                                await websocket.send(_resnapshot_message(outcome_id))
                                continue
                        else:
                            continue

                        sequence = _int_or_none(message.get("seq"))
                        if sequence is None:
                            continue
                        books[contract_id] = book
                        sequences[outcome_id] = sequence
                        yield contract_id, book

                    raise ConnectionError("AGG WebSocket closed")


def _subscription_message(outcome_ids: tuple[str, ...]) -> str:
    return json.dumps(
        {
            "action": "subscribe",
            "channel": "orderbook",
            "outcomeIds": list(outcome_ids),
        }
    )


def _resnapshot_message(outcome_id: str) -> str:
    return json.dumps(
        {
            "action": "resnapshot",
            "channel": "orderbook",
            "outcomeIds": [outcome_id],
        }
    )


def _snapshot_to_order_book(
    message: dict[str, Any],
    contract: BinaryContract,
) -> OrderBook:
    return OrderBook(
        market_id=contract.market_id,
        outcome_id=contract.outcome_id,
        bids=_levels(message.get("bids"), reverse=True),
        asks=_levels(message.get("asks")),
        timestamp=_timestamp(message.get("timestamp")),
        received_at_ns=time.monotonic_ns(),
    )


def _apply_delta(
    current_book: OrderBook | None,
    message: dict[str, Any],
) -> OrderBook | None:
    """Apply one order-book delta without mutating prior snapshots."""
    if current_book is None:
        return None

    bids = {level.price.value: level for level in current_book.bids}
    asks = {level.price.value: level for level in current_book.asks}
    _apply_changes(bids, message.get("bidChanges"))
    _apply_changes(asks, message.get("askChanges"))

    # ponytail: sequence gaps resnapshot; add checksum validation once AGG
    # documents the exact CRC32 level serialization used by its TypeScript SDK.
    return OrderBook(
        market_id=current_book.market_id,
        outcome_id=current_book.outcome_id,
        bids=tuple(
            sorted(bids.values(), key=lambda level: level.price.value, reverse=True)
        ),
        asks=tuple(sorted(asks.values(), key=lambda level: level.price.value)),
        timestamp=_timestamp(message.get("timestamp")) or current_book.timestamp,
        received_at_ns=time.monotonic_ns(),
    )


def _levels(
    raw_levels: Any,
    *,
    reverse: bool = False,
) -> tuple[OrderBookLevel, ...]:
    """Normalize external price levels and discard malformed entries."""
    levels: list[OrderBookLevel] = []
    for raw in raw_levels if isinstance(raw_levels, list) else ():
        level = _level(raw)
        if level is not None and level.quantity.value > 0:
            levels.append(level)
    return tuple(
        sorted(levels, key=lambda level: level.price.value, reverse=reverse)
    )


def _apply_changes(
    levels: dict[Decimal, OrderBookLevel],
    raw_changes: Any,
) -> None:
    """Apply price-level changes while removing zero-quantity levels."""
    for raw in raw_changes if isinstance(raw_changes, list) else ():
        level = _level(raw)
        if level is None:
            continue
        if level.quantity.value == 0:
            levels.pop(level.price.value, None)
        else:
            levels[level.price.value] = level


def _level(raw: Any) -> OrderBookLevel | None:
    """Validate and normalize one external price level."""
    if not isinstance(raw, list) or len(raw) < 2:
        return None
    try:
        return OrderBookLevel(
            price=Price(Decimal(str(raw[0]))),
            quantity=Quantity(Decimal(str(raw[1]))),
        )
    except (InvalidOperation, TypeError, ValueError):
        return None


def _timestamp(value: Any) -> Timestamp | None:
    try:
        return Timestamp(datetime.fromtimestamp(float(value) / 1000, tz=timezone.utc))
    except (TypeError, ValueError, OverflowError):
        return None


def _int_or_none(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _decode_message(raw_message: str | bytes) -> dict[str, Any] | None:
    try:
        message = json.loads(raw_message)
    except (json.JSONDecodeError, UnicodeDecodeError):
        return None
    return message if isinstance(message, dict) else None
