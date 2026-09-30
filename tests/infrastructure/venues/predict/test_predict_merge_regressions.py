"""Verify Predict evidence remains usable when REST reads precede delayed events."""

from dataclasses import replace
from datetime import timedelta
from decimal import Decimal

import pytest

from prediction_markets.domain.shared.value_objects import (
    ClientOrderID, ContractID, OrderID, Price, Quantity, Timestamp,
)
from prediction_markets.domain.trading.entities import OrderSnapshot
from prediction_markets.domain.trading.enums import OrderSide, OrderStatus, OrderType
from prediction_markets.domain.trading.value_objects import OrderReference
from prediction_markets.infrastructure.order_updates import OrderUpdate, TrackedOrderUpdates
from prediction_markets.infrastructure.venues.predict.mappers import PREDICT_VENUE_ID
from prediction_markets.infrastructure.venues.predict.order_updates import PredictOrderUpdateAdapter


def _time(seconds: int) -> Timestamp:
    return Timestamp(Timestamp.from_iso("2026-09-07T08:32:32+00:00").value + timedelta(seconds=seconds))


def _order(filled: str = "0", requested: str = "5") -> tuple[OrderReference, OrderSnapshot]:
    snapshot = OrderSnapshot(
        status=OrderStatus.PARTIALLY_FILLED if Decimal(filled) else OrderStatus.ACCEPTED,
        contract_id=ContractID("predict:1993320:no"), side=OrderSide.SELL,
        quantity=Quantity(Decimal(requested)), order_type=OrderType.LIMIT,
        client_order_id=ClientOrderID("merge-regression"), order_id=OrderID("0xabc"),
        limit_price=Price(Decimal("0.18")), filled_quantity=Quantity(Decimal(filled)),
        average_price=Price(Decimal("0.18")) if Decimal(filled) else None,
        updated_at=_time(8), may_receive_more_fills=True,
    )
    return OrderReference(PREDICT_VENUE_ID, snapshot.client_order_id, b"test"), snapshot


def _event(kind: str, seconds: int = 1, **fields) -> dict:
    return {"type": kind, "orderHash": "0xabc",
            "timestamp": int(_time(seconds).value.timestamp() * 1000), **fields}


def _success(settlement: str, quantity: str, cumulative: str, seconds: int, requested: str = "5") -> dict:
    return _event(
        "orderTransactionSuccess", seconds, settlementId=settlement,
        fill={"executedSizeWei": str(int(Decimal(quantity) * 10**18)),
              "executedPriceWei": "180000000000000000"},
        details={"quantity": requested, "quantityFilled": cumulative},
    )


@pytest.mark.parametrize("reason", ("noMarketMatch", "rejectedPostOnly"))
@pytest.mark.parametrize("status", (OrderStatus.ACCEPTED, OrderStatus.CANCELLED))
def test_native_rejection_survives_later_rest_observation(reason, status):
    """Keep explicit rejection evidence even when REST already observed removal."""
    reference, initial = _order()
    rest = replace(initial, status=status)
    updates = PredictOrderUpdateAdapter(lambda: "jwt", api_key="test")
    updates.record_snapshot(reference, rest, "get")
    updates._handle(_event("orderNotAccepted", reason=reason))
    result = updates.record_snapshot(reference, replace(rest, updated_at=_time(9)), "get")
    assert result.status is OrderStatus.REJECTED
    assert result.reason == reason
    assert result.filled_quantity.value == 0
    assert result.may_receive_more_fills is False


def test_delayed_rejection_does_not_override_pending_settlement():
    """Retain replacement protection until the identified pending match resolves."""
    reference, initial = _order()
    updates = PredictOrderUpdateAdapter(lambda: "jwt", api_key="test")
    updates.record_snapshot(reference, initial, "get")
    updates._handle(_event("orderTransactionSubmitted", 2, settlementId="pending"))
    updates._handle(_event("orderNotAccepted", reason="noMarketMatch"))
    pending = updates.record_snapshot(reference, initial, "get")
    assert pending.reason == "noMarketMatch"
    assert pending.may_receive_more_fills is True
    updates._handle(_event("orderTransactionFailed", 3, settlementId="pending"))
    assert updates.record_snapshot(reference, initial, "get").may_receive_more_fills is False


def test_plain_removal_and_duplicate_rejection_stay_uncertain():
    """Neither polling nor a duplicate submission rejection proves no execution."""
    reference, initial = _order()
    updates = PredictOrderUpdateAdapter(lambda: "jwt", api_key="test")
    cancelled = replace(initial, status=OrderStatus.CANCELLED)
    updates.record_snapshot(reference, cancelled, "get")
    updates._handle(_event("orderNotAccepted", reason="rejectedDuplicate"))
    updates._handle(_event("orderCancelled", 2))
    for seconds in (9, 30, 300):
        result = updates.record_snapshot(reference, replace(cancelled, updated_at=_time(seconds)), "get")
        assert result.filled_quantity.value == 0
        assert result.may_receive_more_fills is True


@pytest.mark.parametrize("requested", ("5", "10"))
def test_delayed_success_uses_cumulative_quantity_without_recounting(requested):
    """Read five confirmed shares from native cumulative evidence after REST saw two."""
    reference, rest = _order("2", requested)
    updates = PredictOrderUpdateAdapter(lambda: "jwt", api_key="test")
    updates.record_snapshot(reference, rest, "get")
    latest = _success("second", "3", "5", 2, requested)
    for event in (latest, latest, _success("first", "2", "2", 1, requested), latest):
        updates._handle(event)
        result = updates.record_snapshot(reference, replace(rest, updated_at=_time(9)), "get")
        assert result.status is (OrderStatus.FILLED if requested == "5" else OrderStatus.PARTIALLY_FILLED)
        assert result.filled_quantity.value == 5
        assert result.average_price.value == Decimal("0.18")
        assert result.may_receive_more_fills is (requested != "5")


def test_missing_native_timestamp_does_not_double_count_cumulative_and_delta():
    """Keep the unique fill when a malformed event cannot establish cumulative coverage."""
    reference, initial = _order(requested="10")
    updates = PredictOrderUpdateAdapter(lambda: "jwt", api_key="test")
    updates.record_snapshot(reference, initial, "submit")
    event = _success("first", "2", "2", 1, "10")
    event.pop("timestamp")
    updates._handle(event)
    result = updates.record_snapshot(reference, initial, "submit")
    assert result.filled_quantity.value == 2
    assert result.may_receive_more_fills is True


def test_shared_tracker_retains_delta_beside_cumulative_evidence():
    """Count unique trade deltas across overlapping REST and cumulative updates."""
    reference, initial = _order(requested="10")
    updates = TrackedOrderUpdates(PREDICT_VENUE_ID)
    updates.record_snapshot(reference, initial, "submit")
    first = OrderUpdate(
        order_id=initial.order_id, filled_quantity=Quantity(Decimal("2")),
        last_fill_quantity=Quantity(Decimal("2")), last_fill_price=Price(Decimal("0.18")),
        reported_at=_time(1), event_id="first",
    )
    updates._record_update(first)
    _, rest = _order("2", "10")
    updates.record_snapshot(reference, rest, "get")
    second = OrderUpdate(
        order_id=initial.order_id, last_fill_quantity=Quantity(Decimal("3")),
        last_fill_price=Price(Decimal("0.18")), reported_at=_time(2), event_id="second",
    )
    for update in (second, first, second):
        updates._record_update(update)
        assert updates.record_snapshot(reference, rest, "get").filled_quantity.value == 5
