"""Integrate limitless order updates with domain ports.

Responsibilities
----------------
- Perform venue I/O and translate external data into domain models.
"""

from decimal import Decimal, InvalidOperation
from typing import Any

from limitless_sdk.websocket import WebSocketClient, WebSocketConfig

from prediction_markets.domain.shared.value_objects import (
    ClientOrderID,
    ContractID,
    OrderID,
    Price,
    Quantity,
    Timestamp,
)
from prediction_markets.domain.trading.enums import OrderStatus
from prediction_markets.infrastructure.venues.limitless.mappers import LIMITLESS_VENUE_ID
from prediction_markets.infrastructure.venues.limitless.execution import _limitless_response_reason
from prediction_markets.infrastructure.order_updates import (
    OrderUpdate,
    TrackedOrderUpdates,
)


class LimitlessOrderUpdateAdapter(TrackedOrderUpdates):
    """Account-wide private Limitless order notifications."""

    def __init__(
        self,
        *,
        api_key: str | None = None,
        client: Any | None = None,
    ) -> None:
        super().__init__(LIMITLESS_VENUE_ID)
        self._client = client or WebSocketClient(WebSocketConfig(api_key=api_key))
        self._subscribed = False
        self._client.on("orderEvent", self._handle)

    async def watch(self, contract_id: ContractID) -> None:
        """Consume private limitless order events until the stream closes.

        Notes
        -----
        - Updates are merged into the shared order tracker.
        """
        del contract_id  # Limitless order events are account-wide.
        if not self._client.is_connected():
            await self._client.connect()
        if not self._subscribed:
            await self._client.subscribe("subscribe_order_events")
            self._subscribed = True

    def is_ready(self) -> bool:
        return self._subscribed and self._client.is_connected()

    async def close(self) -> None:
        """Release network resources owned by the adapter.

        Notes
        -----
        - A caller-supplied client remains under caller ownership.
        """
        await self._client.disconnect()
        self._subscribed = False

    async def _handle(self, payload: Any) -> None:
        """Validate a Limitless private event and merge its normalized order fields."""
        if not isinstance(payload, dict):
            return
        source = str(payload.get("source") or "").upper()
        if source == "OME":
            self._record_update(
                OrderUpdate(
                    order_id=_order_id(payload.get("orderId")),
                    client_order_id=_client_order_id(
                        payload.get("clientOrderId"),
                    ),
                    status={
                        "PLACEMENT": OrderStatus.ACCEPTED,
                        "UPDATE": OrderStatus.PARTIALLY_FILLED,
                        "CANCELLATION": OrderStatus.CANCELLED,
                    }.get(str(payload.get("type") or "").upper()),
                    remaining_quantity=_quantity(payload.get("remainingSize")),
                    reason=(
                        _limitless_response_reason(payload)
                        if str(payload.get("type") or "").upper() == "CANCELLATION"
                        else None
                    ),
                    average_price=_price(payload.get("price")),
                    reported_at=_timestamp(payload.get("timestamp")),
                    event_id=(
                        f"OME:{payload.get('eventId')}:"
                        f"{payload.get('orderId')}"
                    ),
                ),
            )
            return

        if source != "SETTLEMENT" or str(payload.get("type")).upper() != "MINED":
            return
        taker_order_id = _order_id(
            payload.get("takerOrderId") or payload.get("orderId"),
        )
        if taker_order_id is not None:
            self._record_update(
                OrderUpdate(
                    order_id=taker_order_id,
                    client_order_id=_client_order_id(
                        payload.get("clientOrderId"),
                    ),
                    last_fill_quantity=_quantity(
                        payload.get("amountContracts"),
                    ),
                    last_fill_price=_price(payload.get("price")),
                    reported_at=_timestamp(payload.get("timestamp")),
                    event_id=(
                        f"SETTLEMENT:{payload.get('tradeEventId')}:"
                        f"{taker_order_id}"
                    ),
                ),
            )
        for match in payload.get("makerMatches") or ():
            if not isinstance(match, dict):
                continue
            maker_order_id = _order_id(match.get("orderId"))
            if maker_order_id is None:
                continue
            self._record_update(
                OrderUpdate(
                    order_id=maker_order_id,
                    last_fill_quantity=_quantity(match.get("matchedSize")),
                    last_fill_price=_price(match.get("price")),
                    reported_at=_timestamp(payload.get("timestamp")),
                    event_id=(
                        f"SETTLEMENT:{payload.get('tradeEventId')}:"
                        f"{maker_order_id}"
                    ),
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


def _client_order_id(value: Any) -> ClientOrderID | None:
    return ClientOrderID(str(value)) if value else None


def _timestamp(value: Any) -> Timestamp | None:
    """Normalize a supported external timestamp into a domain timestamp."""
    if value is None:
        return None
    try:
        return Timestamp.from_iso(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
