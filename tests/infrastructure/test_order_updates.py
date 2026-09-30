"""Exercise order updates behavior in the infrastructure layer.

Responsibilities
----------------
- Verify order updates contracts, edge cases, and failure handling.
"""

import asyncio
from dataclasses import replace
from decimal import Decimal
import json

from prediction_markets.domain.shared.value_objects import (
    ClientOrderID,
    ContractID,
    Currency,
    Money,
    OrderID,
    Price,
    Quantity,
    Timestamp,
)
from prediction_markets.domain.trading.entities import OrderSnapshot
from prediction_markets.domain.trading.enums import (
    OrderSide,
    OrderStatus,
    OrderType,
)
from prediction_markets.domain.trading.value_objects import OrderReference, TradingFee
from prediction_markets.infrastructure.venues.limitless.mappers import LIMITLESS_VENUE_ID
from prediction_markets.infrastructure.venues.limitless.order_updates import (
    LimitlessOrderUpdateAdapter,
)
from prediction_markets.infrastructure.venues.polymarket.order_updates import (
    PolymarketOrderUpdateAdapter,
)
from prediction_markets.infrastructure.order_updates import OrderUpdate
from prediction_markets.infrastructure.venues.polymarket.mappers import POLYMARKET_VENUE_ID


class _LimitlessClient:
    """Provide a controllable limitless client test double."""
    def __init__(self):
        self.connected = False
        self.handler = None
        self.subscriptions = []

    def on(self, event, handler):
        assert event == "orderEvent"
        self.handler = handler

    def is_connected(self):
        return self.connected

    async def connect(self):
        self.connected = True

    async def subscribe(self, channel):
        self.subscriptions.append(channel)

    async def disconnect(self):
        self.connected = False


class _PolymarketClient:
    """Provide a controllable polymarket client test double."""
    def __init__(self):
        self.subscriptions = []
        self.connected = True

    async def subscribe(self, condition_id):
        self.subscriptions.append(condition_id)

    def is_connected(self):
        return self.connected

    async def disconnect(self):
        self.connected = False


def _snapshot(order_id: str, quantity: str = "10") -> OrderSnapshot:
    return OrderSnapshot(
        status=OrderStatus.ACCEPTED,
        contract_id=ContractID("polymarket:condition-1:token-1"),
        side=OrderSide.BUY,
        quantity=Quantity(Decimal(quantity)),
        order_type=OrderType.LIMIT,
        client_order_id=ClientOrderID(f"client-{order_id}"),
        order_id=OrderID(order_id),
        limit_price=Price(Decimal("0.5")),
        updated_at=Timestamp.now(),
    )


def _reference(
    snapshot: OrderSnapshot,
    *,
    venue_id=POLYMARKET_VENUE_ID,
) -> OrderReference:
    assert snapshot.client_order_id is not None
    return OrderReference(
        venue_id=venue_id,
        client_order_id=snapshot.client_order_id,
        recovery_data=b"test-recovery-key",
    )


def test_limitless_terminal_event_before_registration_returns_without_rest():
    async def run_test():
        client = _LimitlessClient()
        updates = LimitlessOrderUpdateAdapter(client=client)
        await updates.watch(ContractID("limitless:btc:yes"))
        await client.handler(
            {
                "source": "OME",
                "type": "UPDATE",
                "eventId": 2,
                "orderId": "venue-1",
                "price": "0.5",
                "remainingSize": "0",
                "timestamp": "2026-07-27T10:00:00.000Z",
            },
        )

        submitted = _snapshot("venue-1", "5")
        terminal = updates.record_snapshot(
            _reference(submitted, venue_id=LIMITLESS_VENUE_ID),
            submitted,
            "submit",
        )

        assert client.subscriptions == ["subscribe_order_events"]
        assert terminal.status is OrderStatus.FILLED
        assert terminal.filled_quantity == Quantity(Decimal("5"))
        assert updates.source(terminal) == "ws"

    asyncio.run(run_test())


def test_limitless_cancellation_reason_survives_rest_and_stale_updates():
    """Retain a WS rejection explanation across sparse and stale observations."""
    async def run_test():
        client = _LimitlessClient()
        updates = LimitlessOrderUpdateAdapter(client=client)
        submitted = replace(_snapshot("venue-1"), updated_at=Timestamp.from_iso(
            "2026-09-05T10:00:00+00:00",
        ))
        reference = _reference(submitted, venue_id=LIMITLESS_VENUE_ID)
        updates.record_snapshot(reference, submitted, "submit")
        await client.handler({
            "source": "OME", "type": "CANCELLATION", "orderId": "venue-1",
            "eventId": 1, "reason": "matching rejected", "remainingSize": "10",
            "timestamp": "2026-09-05T10:00:02+00:00",
        })
        cancelled = await updates.wait_for_update(reference, submitted, 0.1)
        assert cancelled.status is OrderStatus.CANCELLED
        assert cancelled.reason == "matching rejected"
        sparse = updates.record_snapshot(reference, replace(
            cancelled, reason=None,
            updated_at=Timestamp.from_iso("2026-09-05T10:00:03+00:00"),
        ), "get")
        assert sparse.reason == cancelled.reason
        stale = updates.record_snapshot(reference, replace(
            cancelled, reason="stale reason", updated_at=submitted.updated_at,
        ), "get")
        assert stale.reason == cancelled.reason
        assert updates.record_snapshot(reference, submitted, "submit").reason == cancelled.reason
        filled = updates.record_snapshot(reference, replace(
            submitted, status=OrderStatus.FILLED, filled_quantity=submitted.quantity,
            average_price=Price(Decimal("0.5")),
            updated_at=Timestamp.from_iso("2026-09-05T10:00:04+00:00"),
        ), "get")
        assert filled.status is OrderStatus.FILLED
        assert filled.reason is None

    asyncio.run(run_test())


def test_unknown_order_updates_are_bounded_until_rest_registers_the_order():
    updates = PolymarketOrderUpdateAdapter(client=_PolymarketClient())

    for index in range(updates._MAX_PENDING_UPDATES + 1):
        updates._record_update(
            OrderUpdate(
                order_id=OrderID(f"unknown-{index}"),
                event_id=f"event-{index}",
            ),
        )

    assert len(updates._pending) == updates._MAX_PENDING_UPDATES
    assert updates._pending[0].order_id == OrderID("unknown-1")


def test_polymarket_trade_is_idempotent_for_taker_and_maker():
    async def run_test():
        updates = PolymarketOrderUpdateAdapter(client=_PolymarketClient())
        await updates.watch(ContractID("polymarket:condition-1:token-1"))
        taker_submitted = _snapshot("taker-1")
        maker_submitted = _snapshot("maker-1")
        taker_reference = _reference(taker_submitted)
        maker_reference = _reference(maker_submitted)
        taker = updates.record_snapshot(taker_reference, taker_submitted, "submit")
        maker = updates.record_snapshot(maker_reference, maker_submitted, "submit")
        payload = json.dumps(
            {
                "event_type": "trade",
                "id": "trade-1",
                "status": "MATCHED",
                "timestamp": "1672290701",
                "taker_order_id": "taker-1",
                "size": "2",
                "price": "0.4",
                "maker_orders": [
                    {
                        "order_id": "maker-1",
                        "matched_amount": "2",
                        "price": "0.4",
                    },
                ],
            },
        ).encode()

        updates._handle(payload)
        updates._handle(payload)
        taker = await updates.wait_for_update(taker_reference, taker, 0)
        maker = await updates.wait_for_update(maker_reference, maker, 0)

        assert taker.filled_quantity == Quantity(Decimal("2"))
        assert maker.filled_quantity == Quantity(Decimal("2"))
        assert taker.average_price == maker.average_price == Price(Decimal("0.4"))
        assert updates.source(taker) == "ws"

    asyncio.run(run_test())


def test_old_event_cannot_reduce_fill_and_cancel_keeps_partial():
    async def run_test():
        updates = PolymarketOrderUpdateAdapter(client=_PolymarketClient())
        submitted = _snapshot("venue-1")
        reference = _reference(submitted)
        snapshot = updates.record_snapshot(reference, submitted, "submit")
        waiter = asyncio.create_task(
            updates.wait_for_update(reference, snapshot, 0.1),
        )
        await asyncio.sleep(0)
        updates._handle(
            b'{"event_type":"order","id":"venue-1","type":"UPDATE",'
            b'"original_size":"10","size_matched":"4","price":"0.5",'
            b'"timestamp":"2000"}',
        )
        updates._handle(
            b'{"event_type":"trade","id":"trade-1","status":"MATCHED",'
            b'"taker_order_id":"venue-1","size":"4","price":"0.4",'
            b'"timestamp":"2000","maker_orders":[]}',
        )
        updates._handle(
            b'{"event_type":"order","id":"venue-1","type":"PLACEMENT",'
            b'"original_size":"10","size_matched":"0","price":"0.5",'
            b'"timestamp":"1000"}',
        )
        updates._handle(
            b'{"event_type":"order","id":"venue-1","type":"CANCELLATION",'
            b'"original_size":"10","size_matched":"4","price":"0.5",'
            b'"timestamp":"3000"}',
        )

        terminal = await waiter

        assert terminal.status is OrderStatus.CANCELLED
        assert terminal.filled_quantity == Quantity(Decimal("4"))
        assert terminal.average_price == Price(Decimal("0.4"))

    asyncio.run(run_test())


def test_rest_reconciliation_cannot_be_regressed_by_stale_ws():
    async def run_test():
        updates = PolymarketOrderUpdateAdapter(client=_PolymarketClient())
        initial = _snapshot("venue-1")
        reference = _reference(initial)
        submitted = updates.record_snapshot(reference, initial, "submit")
        reconciled = updates.record_snapshot(
            reference,
            replace(
                submitted,
                status=OrderStatus.CANCELLED,
                filled_quantity=Quantity(Decimal("3")),
                average_price=Price(Decimal("0.45")),
                fee=TradingFee(
                    charged=Money(Decimal("0.01234"), Currency("USDC")),
                    settlement_cost=Money(Decimal("0.01234"), Currency("USD")),
                ),
                updated_at=Timestamp.from_iso("2026-07-27T10:00:00+00:00"),
            ),
            "get",
        )
        updates._handle(
            b'{"event_type":"order","id":"venue-1","type":"PLACEMENT",'
            b'"original_size":"10","size_matched":"0","price":"0.5",'
            b'"timestamp":"1000"}',
        )
        updates._handle(
            b'{"event_type":"trade","id":"trade-old","status":"MATCHED",'
            b'"taker_order_id":"venue-1","size":"3","price":"0.4",'
            b'"timestamp":"1000","maker_orders":[]}',
        )

        assert await updates.wait_for_update(reference, reconciled, 0) is None
        assert reconciled.status is OrderStatus.CANCELLED
        assert reconciled.filled_quantity == Quantity(Decimal("3"))
        assert reconciled.average_price == Price(Decimal("0.45"))
        assert reconciled.fee == TradingFee(
            charged=Money(Decimal("0.01234"), Currency("USDC")),
            settlement_cost=Money(Decimal("0.01234"), Currency("USD")),
        )
        assert updates.source(reconciled) == "get"

    asyncio.run(run_test())


def test_trade_newer_than_rest_snapshot_advances_cumulative_fill():
    async def run_test():
        updates = PolymarketOrderUpdateAdapter(client=_PolymarketClient())
        initial = _snapshot("venue-1")
        reference = _reference(initial)
        reconciled = updates.record_snapshot(
            reference,
            replace(
                initial,
                status=OrderStatus.PARTIALLY_FILLED,
                filled_quantity=Quantity(Decimal("5")),
                average_price=Price(Decimal("0.45")),
                updated_at=Timestamp.from_iso("1970-01-01T00:16:40+00:00"),
            ),
            "get",
        )
        updates._handle(
            b'{"event_type":"trade","id":"trade-new","status":"MATCHED",'
            b'"taker_order_id":"venue-1","size":"2","price":"0.4",'
            b'"timestamp":"2000","maker_orders":[]}',
        )

        latest = await updates.wait_for_update(reference, reconciled, 0)

        assert latest.filled_quantity == Quantity(Decimal("7"))
        assert latest.average_price == Price(
            (Decimal("5") * Decimal("0.45") + Decimal("2") * Decimal("0.4"))
            / Decimal("7"),
        )

    asyncio.run(run_test())


def test_late_trade_cannot_overfill_an_already_filled_order():
    async def run_test():
        updates = PolymarketOrderUpdateAdapter(client=_PolymarketClient())
        initial = _snapshot("venue-1", quantity="6")
        reference = _reference(initial)
        current = updates.record_snapshot(reference, initial, "submit")
        updates._handle(
            b'{"event_type":"order","id":"venue-1","type":"UPDATE",'
            b'"original_size":"6","size_matched":"6","price":"0.5",'
            b'"timestamp":"1000"}',
        )
        current = await updates.wait_for_update(reference, current, 0)
        updates._handle(
            b'{"event_type":"trade","id":"trade-late","status":"MATCHED",'
            b'"taker_order_id":"venue-1","size":"6","price":"0.4",'
            b'"timestamp":"2000","maker_orders":[]}',
        )

        latest = await updates.wait_for_update(reference, current, 0)

        assert latest.filled_quantity == Quantity(Decimal("6"))
        assert latest.average_price == Price(Decimal("0.4"))

    asyncio.run(run_test())


def test_confirmed_excess_fill_survives_reconciliation_and_late_trade():
    """Keep actual venue quantities without adding an already covered trade."""
    async def run_test():
        updates = PolymarketOrderUpdateAdapter(client=_PolymarketClient())
        initial = _snapshot("venue-1", quantity="5")
        reference = _reference(initial)
        current = updates.record_snapshot(reference, initial, "submit")
        updates._handle(
            b'{"event_type":"order","id":"venue-1","type":"UPDATE",'
            b'"original_size":"5","size_matched":"5.2","price":"0.5",'
            b'"timestamp":"1000"}',
        )
        current = await updates.wait_for_update(reference, current, 0)
        assert current.filled_quantity == Quantity(Decimal("5.2"))
        reconciled = updates.record_snapshot(
            reference,
            replace(current, updated_at=Timestamp.from_iso("1970-01-01T00:16:40+00:00")),
            "get",
        )
        updates._handle(
            b'{"event_type":"trade","id":"trade-late","status":"MATCHED",'
            b'"taker_order_id":"venue-1","size":"5.2","price":"0.5",'
            b'"timestamp":"2000","maker_orders":[]}',
        )
        latest = await updates.wait_for_update(reference, reconciled, 0)
        latest = latest or reconciled
        assert latest.status is OrderStatus.FILLED
        assert latest.quantity == Quantity(Decimal("5"))
        assert latest.filled_quantity == Quantity(Decimal("5.2"))
        assert latest.average_price == Price(Decimal("0.5"))

    asyncio.run(run_test())


def test_rest_can_correct_a_capped_terminal_fill():
    """Authoritative reconciliation must retain shares above the signed target."""
    updates = PolymarketOrderUpdateAdapter(client=_PolymarketClient())
    initial = replace(
        _snapshot("venue-1", quantity="5"),
        status=OrderStatus.FILLED,
        filled_quantity=Quantity(Decimal("5")),
        average_price=Price(Decimal("0.52")),
    )
    reference = _reference(initial)
    updates.record_snapshot(reference, initial, "submit")
    corrected = updates.record_snapshot(
        reference,
        replace(initial, filled_quantity=Quantity(Decimal("5.2")), average_price=Price(Decimal("0.5"))),
        "get",
    )
    assert corrected.filled_quantity == Quantity(Decimal("5.2"))
    assert corrected.average_price == Price(Decimal("0.5"))
