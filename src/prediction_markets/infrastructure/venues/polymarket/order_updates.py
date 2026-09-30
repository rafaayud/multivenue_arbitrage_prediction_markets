"""Integrate Polymarket private order updates with domain ports.

Responsibilities
----------------
- Translate private CLOB messages into normalized order snapshots.
"""

import asyncio
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import json
import os
from typing import Any

from nautilus_trader.adapters.polymarket.common.credentials import (
    PolymarketWebSocketAuth,
)
from nautilus_trader.adapters.polymarket.websocket.client import (
    PolymarketWebSocketChannel,
    PolymarketWebSocketClient,
)
from nautilus_trader.common.component import LiveClock

from prediction_markets.domain.shared.value_objects import (
    ContractID,
    OrderID,
    Price,
    Quantity,
    Timestamp,
)
from prediction_markets.domain.trading.enums import OrderStatus
from prediction_markets.infrastructure.order_updates import (
    OrderUpdate,
    TrackedOrderUpdates,
)
from prediction_markets.infrastructure.venues.polymarket.mappers import (
    POLYMARKET_VENUE_ID,
    parse_polymarket_contract_id,
)


class PolymarketOrderUpdateAdapter(TrackedOrderUpdates):
    """Private Polymarket order notifications, subscribed by condition ID."""

    def __init__(
        self,
        *,
        api_key: str | None = None,
        api_secret: str | None = None,
        passphrase: str | None = None,
        client: Any | None = None,
    ) -> None:
        super().__init__(POLYMARKET_VENUE_ID)
        self._credentials = (
            api_key or os.getenv("POLYMARKET_API_KEY"),
            api_secret or os.getenv("POLYMARKET_API_SECRET"),
            passphrase or os.getenv("POLYMARKET_PASSPHRASE"),
        )
        self._client = client
        self._watched: set[str] = set()

    async def watch(self, contract_id: ContractID) -> None:
        """Consume private nautilus order events until the stream closes.

        Notes
        -----
        - Updates are merged into the shared order tracker.
        """
        condition_id, _ = parse_polymarket_contract_id(contract_id)
        if condition_id in self._watched and self.is_ready():
            return
        if self._client is None:
            if not all(self._credentials):
                raise ValueError(
                    "POLYMARKET_API_KEY, POLYMARKET_API_SECRET and "
                    "POLYMARKET_PASSPHRASE are required for private order updates",
                )
            self._client = PolymarketWebSocketClient(
                clock=LiveClock(),
                base_url=None,
                channel=PolymarketWebSocketChannel.USER,
                handler=self._handle,
                handler_reconnect=None,
                loop=asyncio.get_running_loop(),
                auth=PolymarketWebSocketAuth(
                    apiKey=self._credentials[0],
                    secret=self._credentials[1],
                    passphrase=self._credentials[2],
                ),
            )
        await self._client.subscribe(condition_id)
        self._watched.add(condition_id)

    def is_ready(self) -> bool:
        return self._client is not None and self._client.is_connected()

    async def close(self) -> None:
        """Release network resources owned by the adapter.

        Notes
        -----
        - A caller-supplied client remains under caller ownership.
        """
        if self._client is not None:
            await self._client.disconnect()

    def _handle(self, raw: bytes) -> None:
        """Route a decoded private Polymarket message by event type."""
        try:
            payload = json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            return
        if not isinstance(payload, dict):
            return
        if payload.get("event_type") == "order":
            self._handle_order(payload)
        elif (
            payload.get("event_type") == "trade"
            and str(payload.get("status") or "").upper() != "FAILED"
        ):
            self._handle_trade(payload)

    def _handle_order(self, payload: dict[str, Any]) -> None:
        """Normalize an order event and merge it into tracked state."""
        order_id = _order_id(payload.get("id"))
        if order_id is None:
            return
        filled = _quantity(payload.get("size_matched"))
        requested = _quantity(payload.get("original_size"))
        price = _price(payload.get("price"))
        status = _order_status(payload, filled, requested)
        self._record_update(
            OrderUpdate(
                order_id=order_id,
                status=status,
                filled_quantity=filled,
                average_price=(
                    price if filled is not None and filled.value > 0 else None
                ),
                reported_at=_timestamp(payload.get("timestamp")),
                event_id=(
                    f"ORDER:{order_id}:{payload.get('timestamp')}:"
                    f"{payload.get('type')}:{payload.get('size_matched')}:"
                    f"{payload.get('status')}"
                ),
            ),
        )

    def _handle_trade(self, payload: dict[str, Any]) -> None:
        """Normalize a matching trade event and merge its incremental fill."""
        trade_id = payload.get("id")
        timestamp = _timestamp(
            payload.get("timestamp") or payload.get("last_update"),
        )
        taker_order_id = _order_id(payload.get("taker_order_id"))
        taker_quantity = _quantity(payload.get("size"))
        taker_price = _price(payload.get("price"))
        if taker_order_id is not None:
            self._record_update(
                OrderUpdate(
                    order_id=taker_order_id,
                    last_fill_quantity=taker_quantity,
                    last_fill_price=taker_price,
                    reported_at=timestamp,
                    event_id=f"TRADE:{trade_id}:{taker_order_id}",
                ),
            )
        for maker in payload.get("maker_orders") or ():
            if not isinstance(maker, dict):
                continue
            maker_order_id = _order_id(maker.get("order_id"))
            if maker_order_id is None:
                continue
            self._record_update(
                OrderUpdate(
                    order_id=maker_order_id,
                    last_fill_quantity=_quantity(maker.get("matched_amount")),
                    last_fill_price=_price(maker.get("price")),
                    reported_at=timestamp,
                    event_id=f"TRADE:{trade_id}:{maker_order_id}",
                ),
            )


def _decimal(value: Any) -> Decimal | None:
    """Parse an optional external decimal without leaking conversion errors."""
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except InvalidOperation:
        return None


def _quantity(value: Any) -> Quantity | None:
    parsed = _decimal(value)
    return Quantity(parsed) if parsed is not None and parsed >= 0 else None


def _price(value: Any) -> Price | None:
    parsed = _decimal(value)
    return (
        Price(parsed)
        if parsed is not None and Decimal("0") <= parsed <= Decimal("1")
        else None
    )


def _order_id(value: Any) -> OrderID | None:
    return OrderID(str(value)) if value else None


def _order_status(
    payload: dict[str, Any],
    filled: Quantity | None,
    requested: Quantity | None,
) -> OrderStatus:
    """Map venue-specific order states to the normalized lifecycle."""
    if (
        filled is not None
        and requested is not None
        and filled.value >= requested.value
    ):
        return OrderStatus.FILLED
    raw_status = str(payload.get("status") or "").upper()
    event_type = str(payload.get("type") or "").upper()
    if event_type == "CANCELLATION":
        return OrderStatus.CANCELLED
    status = {
        "LIVE": OrderStatus.ACCEPTED,
        "DELAYED": OrderStatus.SUBMITTED,
        "MATCHED": OrderStatus.FILLED,
        "CANCELED": OrderStatus.CANCELLED,
        "CANCELED_MARKET_RESOLVED": OrderStatus.CANCELLED,
        "INVALID": OrderStatus.REJECTED,
        "UNMATCHED": OrderStatus.REJECTED,
    }.get(raw_status)
    if status is not None:
        return status
    if filled is not None and filled.value > 0:
        return OrderStatus.PARTIALLY_FILLED
    return (
        OrderStatus.ACCEPTED
        if event_type == "PLACEMENT"
        else OrderStatus.SUBMITTED
    )


def _timestamp(value: Any) -> Timestamp | None:
    """Normalize a supported external timestamp into a domain timestamp."""
    if value is None:
        return None
    parsed = _decimal(value)
    if parsed is not None:
        if parsed > Decimal("10000000000"):
            parsed /= Decimal("1000")
        return Timestamp(datetime.fromtimestamp(float(parsed), tz=timezone.utc))
    try:
        return Timestamp.from_iso(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
