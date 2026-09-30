"""Reproduce delayed Predict settlements without venue I/O or live orders."""

import json
from dataclasses import replace
from decimal import Decimal

import httpx
import pytest

from prediction_markets.application.events import SubmitOrder
from prediction_markets.application.execution.accounting import is_settled_order
from prediction_markets.domain.shared.value_objects import ClientOrderID, ContractID, OrderID, Price, Quantity, Timestamp
from prediction_markets.domain.trading.entities import OrderIntent, OrderSnapshot
from prediction_markets.domain.trading.enums import OrderSide, OrderStatus, OrderType, TimeInForce, ReconciliationStatus
from prediction_markets.domain.trading.value_objects import OrderReference
from prediction_markets.infrastructure.venues.predict.execution import PredictExecutionAdapter
from prediction_markets.infrastructure.venues.predict.mappers import PREDICT_VENUE_ID
from prediction_markets.infrastructure.venues.predict.order_updates import PredictOrderUpdateAdapter


def _order():
    """Build the five-contract SELL from the failing recovery sequence."""
    intent = OrderIntent(ContractID("predict:1993320:no"), OrderSide.SELL,
        Quantity(Decimal(5)), OrderType.LIMIT, client_order_id=ClientOrderID("recovery-2"),
        limit_price=Price(Decimal("0.18")), time_in_force=TimeInForce.IOC)
    reference = OrderReference(PREDICT_VENUE_ID, intent.client_order_id, json.dumps({
        "schema": 1, "contract_id": str(intent.contract_id), "order_hash": "0xabc", "side": "sell",
        "quantity": "5", "limit_price": "0.18",
    }).encode())
    snapshot = OrderSnapshot(OrderStatus.ACCEPTED, intent.contract_id, intent.side,
        intent.quantity, intent.order_type, client_order_id=intent.client_order_id,
        order_id=OrderID("0xabc"), limit_price=intent.limit_price, may_receive_more_fills=True,
        updated_at=Timestamp.from_iso("2026-09-07T08:32:32+00:00"))
    return SubmitOrder("execution", "recovery", PREDICT_VENUE_ID, intent), reference, snapshot


def _event(kind, settlement="match-1", quantity="5"):
    """Build a documented native wallet event with a stable settlement identity."""
    return {"type": kind, "orderHash": "0xabc", "timestamp": 1788779553000,
        "settlementId": settlement, "fill": {"executedSizeWei": str(int(Decimal(quantity) * 10**18)),
            "executedPriceWei": "180000000000000000"}}


@pytest.mark.parametrize("cancel_first", (False, True))
def test_cancel_then_delayed_fill_never_authorizes_replacement(cancel_first):
    """Preserve five confirmed fills across cancellation, duplicates and stale REST."""
    command, reference, initial = _order()
    updates = PredictOrderUpdateAdapter(lambda: "jwt", api_key="test")
    updates.record_snapshot(reference, initial, "submit")
    cancelled = replace(initial, status=OrderStatus.CANCELLED, may_receive_more_fills=False)
    if cancel_first:
        updates.record_snapshot(reference, cancelled, "get")
    updates._handle(_event("orderTransactionSubmitted"))
    pending = updates.record_snapshot(reference, cancelled, "get")
    assert pending.filled_quantity.value == 0
    assert not is_settled_order(command, pending)
    updates._handle(_event("orderTransactionSuccess"))
    updates._handle(_event("orderCancelled"))
    updates._handle(_event("orderTransactionSuccess"))
    filled = updates.record_snapshot(reference, cancelled, "get")
    assert filled.status is OrderStatus.FILLED
    assert filled.filled_quantity.value == 5
    assert filled.average_price.value == Decimal("0.18")
    assert is_settled_order(command, filled)


def test_partial_and_failed_settlement_remain_uncertain_after_removal():
    """A failed match or partial success cannot certify all cancelled remainder."""
    command, reference, initial = _order()
    updates = PredictOrderUpdateAdapter(lambda: "jwt", api_key="test")
    updates._handle(_event("orderTransactionSubmitted", "one", "2"))
    updates._handle(_event("orderTransactionSuccess", "one", "2"))
    updates._handle(_event("orderTransactionSubmitted", "two", "3"))
    updates._handle(_event("orderTransactionFailed", "two", "3"))
    updates._handle(_event("orderCancelled"))
    partial = updates.record_snapshot(reference, initial, "submit")
    assert partial.filled_quantity.value == 2
    assert not is_settled_order(command, partial)
    updates._handle(_event("orderTransactionSuccess", "three", "3"))
    assert updates.record_snapshot(reference, initial, "submit").filled_quantity.value == 5


@pytest.mark.parametrize("reason,final", (("noMarketMatch", True), ("rejectedPostOnly", True), ("rejectedDuplicate", False)))
def test_explicit_rejection_is_distinct_from_cancellation(reason, final):
    """Allow a proven rejection but not a duplicate-order ambiguity to settle."""
    command, reference, initial = _order()
    updates = PredictOrderUpdateAdapter(lambda: "jwt", api_key="test")
    updates._handle({**_event("orderNotAccepted"), "reason": reason})
    snapshot = updates.record_snapshot(reference, initial, "submit")
    assert is_settled_order(command, snapshot) is final


def test_pending_match_overrides_rejection_until_it_resolves():
    """Do not let a no-match message overrule a separately submitted settlement."""
    command, reference, initial = _order()
    updates = PredictOrderUpdateAdapter(lambda: "jwt", api_key="test")
    updates._handle(_event("orderTransactionSubmitted"))
    updates._handle({**_event("orderNotAccepted"), "reason": "noMarketMatch"})
    assert not is_settled_order(command, updates.record_snapshot(reference, initial, "submit"))


@pytest.mark.parametrize("status", ("CANCELLED", "MATCHED", "FILLED", "EXPIRED"))
def test_empty_rest_matches_do_not_prove_no_fill(status):
    """Neither a native status label nor empty indexed matches prove execution size."""
    command, reference, _ = _order()
    raw = {"status": status, "marketId": 1993320, "amountFilled": "0", "order": {
        "hash": "0xabc", "side": 1, "makerAmount": str(5 * 10**18),
        "takerAmount": "900000000000000000"}}
    def handler(request):
        return httpx.Response(200, json={"success": True,
            "data": [] if request.url.path.endswith("matches") else raw})
    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        adapter = PredictExecutionAdapter(privy_private_key="0x" + "1" * 64,
            account_address="0x" + "2" * 40, api_key="key", client=client, order_builder=object())
        adapter._jwt = "cached"
        snapshot = adapter.reconcile(reference).snapshot
        assert snapshot is not None
        assert snapshot.filled_quantity.value == 0
        assert not is_settled_order(command, snapshot)


@pytest.mark.parametrize("failure", (404, 503, "wrong_hash", "malformed"))
def test_reconcile_and_cancel_fail_closed_on_missing_or_invalid_identity(failure):
    """An unavailable or foreign order cannot authorize another execution."""
    _, reference, _ = _order()
    def handler(request):
        assert request.method == "GET"
        if isinstance(failure, int):
            return httpx.Response(failure)
        raw = {"order": {"hash": "0xother"}} if failure == "wrong_hash" else {}
        return httpx.Response(200, json={"success": True, "data": raw})
    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        adapter = PredictExecutionAdapter(privy_private_key="0x" + "1" * 64,
            account_address="0x" + "2" * 40, api_key="key", client=client, order_builder=object())
        adapter._jwt = "cached"
        assert adapter.reconcile(reference).status is ReconciliationStatus.UNKNOWN
        assert adapter.cancel(reference).status is ReconciliationStatus.UNKNOWN


def test_rest_maker_fill_uses_own_amount_and_deduplicates_matches():
    """Do not count all taker shares, or repeated rows, as this maker's fill."""
    command, reference, initial = _order()
    own_match = {"amountFilled": "5", "priceExecuted": "0.3", "transactionHash": "tx",
        "taker": {"hash": "another"}, "makers": [{"hash": "0xabc", "amount": "2", "price": "0.18"}]}
    with httpx.Client(transport=httpx.MockTransport(lambda request:
            httpx.Response(200, json={"success": True, "data": [own_match, own_match]}))) as client:
        adapter = PredictExecutionAdapter(privy_private_key="0x" + "1" * 64,
            account_address="0x" + "2" * 40, api_key="key", client=client, order_builder=object())
        adapter._jwt = "cached"
        snapshot = adapter._enrich_fills(initial, {"marketId": 1993320, "status": "CANCELLED"})
        assert snapshot.filled_quantity.value == 2
        assert snapshot.average_price.value == Decimal("0.18")
        assert not is_settled_order(command, snapshot)
