"""Consume Predict.fun wallet events and normalize private order updates.

Responsibilities
----------------
- Maintain one account-wide authenticated WebSocket subscription.
- Merge venue order and settlement events through the shared tracker.
"""

import asyncio
import json
from collections.abc import Callable
from dataclasses import replace
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed

from prediction_markets.domain.shared.value_objects import (
    ContractID,
    OrderID,
    Price,
    Quantity,
    Timestamp,
)
from prediction_markets.domain.trading.enums import OrderStatus
from prediction_markets.domain.trading.entities import OrderSnapshot
from prediction_markets.domain.trading.value_objects import OrderReference
from prediction_markets.infrastructure.order_updates import (
    OrderUpdate,
    TrackedOrderUpdates,
)
from prediction_markets.infrastructure.observability.predict_fill_study import observe_native
from prediction_markets.infrastructure.venues.predict.config import (
    predict_api_key,
    predict_headers,
)
from prediction_markets.infrastructure.venues.predict.mappers import PREDICT_VENUE_ID

_PRECISION = Decimal(10**18)


class PredictOrderUpdateAdapter(TrackedOrderUpdates):
    """Account-wide Predict wallet events authenticated by the execution JWT."""

    def __init__(
        self,
        token_provider: Callable[[], str],
        *,
        api_key: str | None = None,
        websocket_url: str = "wss://ws.predict.fun/ws",
    ) -> None:
        """Configure the wallet-event stream.

        Parameters
        ----------
        token_provider
            Synchronous callback returning a valid Predict wallet JWT.
        api_key
            API key sent during the WebSocket handshake.
        websocket_url
            Predict WebSocket endpoint.
        """
        super().__init__(PREDICT_VENUE_ID)
        self._token_provider = token_provider
        self._api_key = predict_api_key(api_key)
        self._websocket_url = websocket_url
        self._task: asyncio.Task[None] | None = None
        self._ready = asyncio.Event()
        self._closed = False

    async def watch(self, contract_id: ContractID) -> None:
        """Ensure the account-wide wallet subscription is connected.

        Parameters
        ----------
        contract_id
            Ignored after validation by runtime; wallet events are account-wide.
        """
        del contract_id
        if self._closed:
            raise RuntimeError("Predict order updates are closed")
        if self.is_ready():
            return
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(
                self._run(),
                name="predict-wallet-events",
            )
        ready = asyncio.create_task(self._ready.wait())
        done, pending = await asyncio.wait(
            (ready, self._task),
            timeout=10,
            return_when=asyncio.FIRST_COMPLETED,
        )
        for task in pending:
            if task is ready:
                task.cancel()
        if ready in done and ready.result():
            return
        if self._task.done():
            self._task.result()
        raise TimeoutError("Predict wallet subscription did not become ready")

    def is_ready(self) -> bool:
        return self._ready.is_set() and self._task is not None and not self._task.done()

    async def close(self) -> None:
        """Stop the account-wide wallet subscription."""
        self._closed = True
        if self._task is not None:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
            self._task = None
        self._ready.clear()

    async def _run(self) -> None:
        """Reconnect transient failures and route wallet event payloads."""
        delay = 1
        while not self._closed:
            try:
                token = await asyncio.to_thread(self._token_provider)
                async with connect(
                    self._websocket_url,
                    additional_headers=predict_headers(self._api_key),
                ) as websocket:
                    await websocket.send(
                        json.dumps(
                            {
                                "method": "subscribe",
                                "requestId": 1,
                                "params": [f"predictWalletEvents/{token}"],
                            }
                        )
                    )
                    async for raw_message in websocket:
                        message = _decode_message(raw_message)
                        if message is None:
                            continue
                        if _is_heartbeat(message):
                            await websocket.send(
                                json.dumps(
                                    {"method": "heartbeat", "data": message["data"]}
                                )
                            )
                            continue
                        if message.get("type") == "R":
                            if message.get("success") is not True:
                                raise RuntimeError(
                                    f"Predict wallet subscription failed: "
                                    f"{message.get('error')}"
                                )
                            self._ready.set()
                            delay = 1
                            continue
                        if message.get("type") == "M" and isinstance(
                            message.get("data"), dict
                        ):
                            self._handle(message["data"])
                    raise ConnectionError("Predict wallet WebSocket closed")
            except (ConnectionClosed, ConnectionError, OSError):
                self._ready.clear()
                await asyncio.sleep(delay)
                delay = min(delay * 2, 30)

    def record_snapshot(
        self, reference: OrderReference, snapshot: OrderSnapshot, source: str,
    ) -> OrderSnapshot:
        """Merge snapshots without allowing stale REST removal to end settlement."""
        state = self._find(reference.client_order_id, snapshot.order_id)
        if state is not None and state.snapshot.settlement_finalized_block is not None:
            return state.snapshot
        if (snapshot.settlement_finalized_block is not None and state is not None
                and snapshot.filled_quantity.value < state.snapshot.filled_quantity.value):
            raise ValueError("Finalized chain evidence regresses private fills")
        incoming = snapshot
        if snapshot.settlement_finalized_block is not None:
            # Merge buffered wallet events against the previous quantity, not
            # against the chain total that already includes those same fills.
            incoming = state.snapshot if state is not None else replace(snapshot,
                status=OrderStatus.ACCEPTED, filled_quantity=Quantity(Decimal(0)),
                average_price=None, fee=None, may_receive_more_fills=True,
                settlement_finalized_block=None)
        merged = super().record_snapshot(reference, incoming, source)
        state = self._find(reference.client_order_id, merged.order_id)
        if state is not None:
            if snapshot.settlement_finalized_block is not None:
                if merged.filled_quantity.value > snapshot.filled_quantity.value:
                    raise ValueError("Finalized chain evidence regresses buffered fills")
                state.snapshot = snapshot
            state.snapshot = self._with_finality(state.snapshot, state.event_ids)
            return state.snapshot
        return merged

    def _record_update(self, update: OrderUpdate) -> None:
        """Keep pending settlements authoritative across cancellation and reordering."""
        state = self._find(update.client_order_id, update.order_id)
        if state is not None and state.snapshot.settlement_finalized_block is not None:
            return
        super()._record_update(update)
        state = self._find(update.client_order_id, update.order_id)
        if state is not None:
            state.snapshot = self._with_finality(state.snapshot, state.event_ids)

    @staticmethod
    def _with_finality(snapshot: OrderSnapshot, event_ids: set[str]) -> OrderSnapshot:
        """Require full fills or an explicit no-match rejection before replacement.

        Notes
        -----
        - Cancelled/expired partial orders remain uncertain even with an empty
          matches response. A timeout cannot prove that no match is in flight.
        - Settlement identifiers make submitted/resolved messages order-independent.
        - Explicit rejection evidence survives REST observation timestamps, which
          do not establish the ordering of native venue events.
        - Finalized chain scans supersede delayed REST and wallet notifications.
        """
        if snapshot.settlement_finalized_block is not None:
            return replace(snapshot, may_receive_more_fills=False)
        submitted = {x.removeprefix("orderTransactionSubmitted:") for x in event_ids
                     if x.startswith("orderTransactionSubmitted:")}
        resolved = {x.split(":", 1)[1] for x in event_ids
                    if x.startswith(("orderTransactionSuccess:", "orderTransactionFailed:"))}
        rejection_reason = next(
            (reason for reason in ("noMarketMatch", "rejectedPostOnly")
             if any(x.startswith(f"orderNotAccepted:{reason}:") for x in event_ids)),
            None,
        )
        if rejection_reason is not None and snapshot.filled_quantity.value < snapshot.quantity.value:
            snapshot = replace(snapshot, status=OrderStatus.REJECTED, reason=rejection_reason)
        no_match = snapshot.status is OrderStatus.REJECTED and snapshot.reason in {
            "noMarketMatch", "rejectedPostOnly",
        }
        final = snapshot.filled_quantity.value >= snapshot.quantity.value or (
            no_match and not submitted.difference(resolved)
        )
        return replace(snapshot, may_receive_more_fills=not final)

    def _handle(self, payload: dict[str, Any]) -> None:
        """Normalize one documented Predict wallet event."""
        observe_native(payload)
        order_id = _order_id(payload.get("orderHash"))
        if order_id is None:
            return
        event_type = str(payload.get("type") or "")
        status = {
            "orderAccepted": OrderStatus.ACCEPTED,
            "orderNotAccepted": OrderStatus.REJECTED,
            "orderExpired": OrderStatus.EXPIRED,
            "orderCancelled": OrderStatus.CANCELLED,
        }.get(event_type)
        timestamp = _timestamp(payload.get("timestamp"))
        if status is not None:
            self._record_update(
                OrderUpdate(
                    order_id=order_id,
                    status=status,
                    may_receive_more_fills=True,
                    reason=str(payload.get("reason") or "") or None,
                    reported_at=timestamp,
                    event_id=(f"{event_type}:{payload.get('reason') or ''}:"
                              f"{order_id}:{payload.get('timestamp')}"),
                )
            )
            return
        if event_type in {"orderTransactionSubmitted", "orderTransactionFailed"}:
            settlement_id = payload.get("settlementId")
            if settlement_id:
                self._record_update(OrderUpdate(
                    order_id=order_id, reported_at=timestamp,
                    event_id=f"{event_type}:{settlement_id}:{order_id}",
                    may_receive_more_fills=True,
                ))
            return
        if event_type != "orderTransactionSuccess" or not payload.get("settlementId"):
            return
        fill = payload.get("fill")
        details = payload.get("details")
        if not isinstance(fill, dict):
            return
        quantity = _wei_quantity(fill.get("executedSizeWei"))
        price = _wei_price(fill.get("executedPriceWei"))
        if quantity is None or quantity.value <= 0 or price is None:
            return
        cumulative = (
            _decimal_quantity(details.get("quantityFilled"))
            if isinstance(details, dict) and timestamp is not None
            else None
        )
        requested = (
            _decimal_quantity(details.get("quantity"))
            if isinstance(details, dict)
            else None
        )
        terminal = (
            cumulative is not None
            and requested is not None
            and cumulative.value >= requested.value
        )
        self._record_update(
            OrderUpdate(
                order_id=order_id,
                status=OrderStatus.FILLED if terminal else OrderStatus.PARTIALLY_FILLED,
                filled_quantity=cumulative,
                last_fill_quantity=quantity,
                last_fill_price=price,
                reported_at=timestamp,
                event_id=(
                    f"orderTransactionSuccess:{payload.get('settlementId')}:"
                    f"{order_id}"
                ),
            )
        )


def _decode_message(raw_message: str | bytes) -> dict[str, Any] | None:
    try:
        message = json.loads(raw_message)
    except (json.JSONDecodeError, UnicodeDecodeError):
        return None
    return message if isinstance(message, dict) else None


def _is_heartbeat(message: dict[str, Any]) -> bool:
    return (
        message.get("type") == "M"
        and message.get("topic") == "heartbeat"
        and message.get("data") is not None
    )


def _decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    try:
        parsed = Decimal(str(value))
        return parsed if parsed.is_finite() else None
    except (InvalidOperation, ValueError):
        return None


def _decimal_quantity(value: Any) -> Quantity | None:
    parsed = _decimal(value)
    return Quantity(parsed) if parsed is not None and parsed >= 0 else None


def _wei_quantity(value: Any) -> Quantity | None:
    parsed = _decimal(value)
    return Quantity(parsed / _PRECISION) if parsed is not None and parsed >= 0 else None


def _wei_price(value: Any) -> Price | None:
    parsed = _decimal(value)
    normalized = parsed / _PRECISION if parsed is not None else None
    return (
        Price(normalized)
        if normalized is not None and Decimal("0") <= normalized <= Decimal("1")
        else None
    )


def _order_id(value: Any) -> OrderID | None:
    return OrderID(str(value)) if value else None


def _timestamp(value: Any) -> Timestamp | None:
    parsed = _decimal(value)
    if parsed is None:
        return None
    if parsed > Decimal("10000000000"):
        parsed /= Decimal("1000")
    try:
        return Timestamp(datetime.fromtimestamp(float(parsed), tz=timezone.utc))
    except (OverflowError, OSError, ValueError):
        return None
