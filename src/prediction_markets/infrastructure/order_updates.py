"""Coordinate asynchronous order updates from execution venues.

Responsibilities
----------------
- Track snapshots and let callers wait for terminal order state.
"""

import asyncio
from dataclasses import dataclass, field, replace
from decimal import Decimal

from prediction_markets.domain.ports.execution import OrderUpdatePort
from prediction_markets.domain.shared.value_objects import (
    ClientOrderID,
    OrderID,
    Price,
    Quantity,
    Timestamp,
    VenueID,
)
from prediction_markets.domain.trading.entities import OrderSnapshot
from prediction_markets.domain.trading.enums import OrderStatus
from prediction_markets.domain.trading.value_objects import OrderReference, TradingFee


@dataclass(frozen=True, slots=True)
class OrderUpdate:
    """Represent a partial normalized update from a venue order stream."""
    order_id: OrderID | None = None
    client_order_id: ClientOrderID | None = None
    status: OrderStatus | None = None
    filled_quantity: Quantity | None = None
    remaining_quantity: Quantity | None = None
    last_fill_quantity: Quantity | None = None
    last_fill_price: Price | None = None
    average_price: Price | None = None
    fee: TradingFee | None = None
    reported_at: Timestamp | None = None
    event_id: str | None = None
    may_receive_more_fills: bool | None = None
    reason: str | None = None


@dataclass(slots=True)
class _TrackedOrder:
    """Hold mutable merge state for one order observed through REST and WebSocket."""
    snapshot: OrderSnapshot
    source: str
    cumulative_filled: Decimal
    fallback_price: Price | None
    trade_fills: dict[
        str,
        tuple[Decimal, Price | None, Timestamp | None],
    ] = field(default_factory=dict)
    event: asyncio.Event = field(default_factory=asyncio.Event)
    version: int = 0
    last_reported_at: Timestamp | None = None
    cumulative_reported_at: Timestamp | None = None
    anchor_filled: Decimal = Decimal("0")
    anchor_notional: Decimal | None = None
    anchor_reported_at: Timestamp | None = None
    event_ids: set[str] = field(default_factory=set)


class TrackedOrderUpdates(OrderUpdatePort):
    """Merge private WS events and REST snapshots into one monotonic state."""

    _MAX_PENDING_UPDATES = 1024

    def __init__(self, venue_id: VenueID) -> None:
        self._venue_id = venue_id
        self._by_client: dict[tuple[VenueID, ClientOrderID], _TrackedOrder] = {}
        self._by_order: dict[tuple[VenueID, OrderID], _TrackedOrder] = {}
        # Unknown orders fall back to REST if this bounded pre-registration window overflows.
        self._pending: list[OrderUpdate] = []

    def record_snapshot(
        self,
        reference: OrderReference,
        snapshot: OrderSnapshot,
        source: str,
    ) -> OrderSnapshot:
        """Register a REST or submission snapshot and merge pending WebSocket updates.

        Returns
        -------
        OrderSnapshot
            The latest monotonic normalized state.
        """
        if reference.venue_id != self._venue_id:
            raise ValueError("Order reference belongs to a different venue")
        if (
            snapshot.client_order_id is not None
            and snapshot.client_order_id != reference.client_order_id
        ):
            raise ValueError("Snapshot and reference client order IDs must match")
        if snapshot.client_order_id is None:
            snapshot = replace(snapshot, client_order_id=reference.client_order_id)
        if source not in {"submit", "get"}:
            raise ValueError("Order snapshot source must be 'submit' or 'get'")
        state = self._find(reference.client_order_id, snapshot.order_id)
        if state is None:
            state = _TrackedOrder(
                snapshot=snapshot,
                source=source,
                cumulative_filled=snapshot.filled_quantity.value,
                fallback_price=snapshot.average_price,
                last_reported_at=(
                    snapshot.updated_at if source == "get" else None
                ),
                cumulative_reported_at=(
                    snapshot.updated_at if source == "get" else None
                ),
                anchor_filled=(
                    snapshot.filled_quantity.value
                    if source == "get"
                    else Decimal("0")
                ),
                anchor_notional=(
                    snapshot.filled_quantity.value * snapshot.average_price.value
                    if source == "get" and snapshot.average_price is not None
                    else None
                ),
                anchor_reported_at=(
                    snapshot.updated_at if source == "get" else None
                ),
            )
            self._index(state)
        else:
            self._apply(
                state,
                OrderUpdate(
                    client_order_id=snapshot.client_order_id,
                    order_id=snapshot.order_id,
                    status=snapshot.status,
                    filled_quantity=snapshot.filled_quantity,
                    average_price=snapshot.average_price,
                    fee=snapshot.fee,
                    may_receive_more_fills=snapshot.may_receive_more_fills,
                    reason=snapshot.reason,
                    reported_at=(
                        snapshot.updated_at if source == "get" else None
                    ),
                ),
                source,
            )
            self._index(state)

        pending = [
            update for update in self._pending if _matches(snapshot, update)
        ]
        if pending:
            self._pending = [
                update for update in self._pending if update not in pending
            ]
            for update in pending:
                self._apply(state, update, "ws")
        return state.snapshot

    async def wait_for_update(
        self,
        reference: OrderReference,
        after: OrderSnapshot,
        timeout: float,
    ) -> OrderSnapshot | None:
        """Wait for state different from the caller's last observed snapshot.

        Returns
        -------
        OrderSnapshot | None
            New normalized state, otherwise ``None`` on timeout.
        """
        if reference.venue_id != self._venue_id:
            raise ValueError("Order reference belongs to a different venue")
        state = self._find(reference.client_order_id, after.order_id)
        if state is None:
            return None
        deadline = asyncio.get_running_loop().time() + timeout
        while state.snapshot == after:
            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                return None
            state.event.clear()
            if state.snapshot != after:
                return state.snapshot
            try:
                await asyncio.wait_for(state.event.wait(), remaining)
            except TimeoutError:
                return None
        return state.snapshot

    def source(
        self,
        snapshot: OrderSnapshot,
    ) -> str | None:
        """Return the source of the latest tracked state for an order.

        Raises
        ------
        KeyError
            If the order has not been registered.
        """
        state = self._find(snapshot.client_order_id, snapshot.order_id)
        return state.source if state is not None else None

    def _record_update(self, update: OrderUpdate) -> None:
        """Apply an update immediately or retain it until its order is registered."""
        if update.client_order_id is None and update.order_id is None:
            return
        state = self._find(update.client_order_id, update.order_id)
        if state is None:
            if update.event_id is None or all(
                pending.event_id != update.event_id for pending in self._pending
            ):
                if len(self._pending) == self._MAX_PENDING_UPDATES:
                    self._pending.pop(0)
                self._pending.append(update)
            return
        self._apply(state, update, "ws")
        self._index(state)

    def _find(
        self,
        client_order_id: ClientOrderID | None,
        order_id: OrderID | None,
    ) -> _TrackedOrder | None:
        """Resolve tracked state by client or venue order identifier."""
        if client_order_id is not None:
            state = self._by_client.get((self._venue_id, client_order_id))
            if state is not None:
                return state
        if order_id is not None:
            return self._by_order.get((self._venue_id, order_id))
        return None

    def _index(self, state: _TrackedOrder) -> None:
        """Index one tracked order by every identifier currently known."""
        snapshot = state.snapshot
        if snapshot.client_order_id is not None:
            self._by_client[(self._venue_id, snapshot.client_order_id)] = state
        if snapshot.order_id is not None:
            self._by_order[(self._venue_id, snapshot.order_id)] = state

    def _apply(
        self,
        state: _TrackedOrder,
        update: OrderUpdate,
        source: str,
    ) -> None:
        """Merge REST and WebSocket fields without regressing quantity, time, or terminal status.

        Notes
        -----
        - Duplicate event ids cannot add fills twice. Cumulative venue fills may
          exceed the requested quantity and must remain visible to accounting.
        """
        if update.event_id is not None:
            if update.event_id in state.event_ids:
                return
            state.event_ids.add(update.event_id)

        current = state.snapshot
        stale = (
            update.reported_at is not None
            and state.last_reported_at is not None
            and update.reported_at.value < state.last_reported_at.value
        )
        if update.filled_quantity is not None:
            state.cumulative_filled = max(
                state.cumulative_filled,
                update.filled_quantity.value,
            )
        if update.remaining_quantity is not None:
            state.cumulative_filled = max(
                state.cumulative_filled,
                current.quantity.value - update.remaining_quantity.value,
            )
        is_trade = (
            update.filled_quantity is None
            and update.remaining_quantity is None
            and update.last_fill_quantity is not None
        )
        if update.last_fill_quantity is not None and update.event_id is not None:
            state.trade_fills[update.event_id] = (
                update.last_fill_quantity.value,
                update.last_fill_price,
                update.reported_at,
            )
        if update.average_price is not None and (
            not stale or state.fallback_price is None
        ):
            state.fallback_price = update.average_price

        if (
            (update.filled_quantity is not None or update.remaining_quantity is not None)
            and update.reported_at is not None
            and not stale
        ):
            state.cumulative_reported_at = update.reported_at
        if source == "get" and update.filled_quantity is not None and not stale:
            state.anchor_filled = update.filled_quantity.value
            state.anchor_notional = (
                update.filled_quantity.value * update.average_price.value
                if update.average_price is not None
                else None
            )
            state.anchor_reported_at = update.reported_at
        trade_filled = sum(
            (quantity for quantity, _, _ in state.trade_fills.values()),
            Decimal("0"),
        )
        uncovered_trade_filled = sum(
            (
                quantity
                for quantity, _, reported_at in state.trade_fills.values()
                if state.cumulative_reported_at is None
                or (
                    reported_at is not None
                    and reported_at.value > state.cumulative_reported_at.value
                )
            ),
            Decimal("0"),
        )
        filled = min(
            max(current.quantity.value, state.cumulative_filled),
            max(
                trade_filled,
                state.cumulative_filled + uncovered_trade_filled,
            ),
        )
        priced_fills = tuple(
            (quantity, price)
            for quantity, price, reported_at in state.trade_fills.values()
            if price is not None
            and (
                state.anchor_notional is None
                or state.anchor_reported_at is None
                or (
                    reported_at is not None
                    and reported_at.value > state.anchor_reported_at.value
                )
            )
        )
        priced_trade_filled = sum(
            (quantity for quantity, _ in priced_fills),
            state.anchor_filled if state.anchor_notional is not None else Decimal("0"),
        )
        trade_notional = sum(
            (quantity * price.value for quantity, price in priced_fills),
            state.anchor_notional or Decimal("0"),
        )
        if filled > 0 and priced_trade_filled >= filled:
            average = Price(trade_notional / priced_trade_filled)
        elif filled > 0 and state.fallback_price is not None:
            average = Price(
                (
                    trade_notional
                    + (filled - priced_trade_filled)
                    * state.fallback_price.value
                )
                / filled,
            )
        else:
            average = None
        status = _merged_status(
            current.status,
            update.status,
            filled,
            current.quantity.value,
            stale,
        )
        reason = current.reason if status is current.status else None
        if not stale and update.status is status and update.reason:
            reason = update.reason
        if status not in {OrderStatus.REJECTED, OrderStatus.CANCELLED, OrderStatus.EXPIRED}:
            reason = None
        updated_at = _latest(current.updated_at, update.reported_at)
        if update.reported_at is not None and not stale and not is_trade:
            state.last_reported_at = update.reported_at
        state.version += 1

        # A positive fill without a price is ambiguous; wake the waiter so REST
        # can reconcile it without constructing an invalid OrderSnapshot.
        if filled == 0 or average is not None:
            state.snapshot = replace(
                current,
                status=status,
                reason=reason,
                client_order_id=update.client_order_id or current.client_order_id,
                order_id=update.order_id or current.order_id,
                filled_quantity=Quantity(filled),
                average_price=average,
                fee=update.fee if update.fee is not None else current.fee,
                updated_at=updated_at,
                may_receive_more_fills=(
                    update.may_receive_more_fills
                    if update.may_receive_more_fills is not None
                    else current.may_receive_more_fills
                ),
            )
        if state.snapshot != current:
            state.source = source
        state.event.set()


def _matches(snapshot: OrderSnapshot, update: OrderUpdate) -> bool:
    return (
        snapshot.client_order_id is not None
        and snapshot.client_order_id == update.client_order_id
    ) or (
        snapshot.order_id is not None
        and snapshot.order_id == update.order_id
    )


def _latest(left: Timestamp | None, right: Timestamp | None) -> Timestamp | None:
    """Select the newest available timestamp while tolerating missing values."""
    if left is None:
        return right
    if right is None:
        return left
    return max(left, right, key=lambda timestamp: timestamp.value)


def _merged_status(
    current: OrderStatus,
    incoming: OrderStatus | None,
    filled: Decimal,
    requested: Decimal,
    stale: bool,
) -> OrderStatus:
    """Merge order status monotonically with quantity and terminal-state rules."""
    if filled >= requested:
        return OrderStatus.FILLED
    if current in {
        OrderStatus.FILLED,
        OrderStatus.CANCELLED,
        OrderStatus.REJECTED,
        OrderStatus.EXPIRED,
    }:
        return current
    if stale or incoming is None:
        return OrderStatus.PARTIALLY_FILLED if filled > 0 else current
    if incoming in {
        OrderStatus.FILLED,
        OrderStatus.CANCELLED,
        OrderStatus.REJECTED,
        OrderStatus.EXPIRED,
    }:
        return incoming
    return OrderStatus.PARTIALLY_FILLED if filled > 0 else incoming
