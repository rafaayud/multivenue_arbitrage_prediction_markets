"""Exercise execution behavior in the infrastructure agg layer.

Responsibilities
----------------
- Verify execution contracts, edge cases, and failure handling.
"""

import json
from decimal import Decimal

import httpx

from prediction_markets.domain.ports.execution import ExecutionPort
from prediction_markets.domain.shared.value_objects import ClientOrderID, ContractID, Price, Quantity
from prediction_markets.domain.trading.entities import OrderIntent
from prediction_markets.domain.trading.enums import (
    OrderSide,
    OrderStatus,
    OrderType,
    ReconciliationStatus,
    SubmissionStatus,
)
from prediction_markets.infrastructure.agg.execution import AggPaperExecutionAdapter


def test_paper_order_uses_agg_account_and_maps_fill():
    requests = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            201,
            json={
                "id": "paper-order-1",
                "venueMarketOutcomeId": "outcome-1",
                "side": "buy",
                "status": "filled",
                "filledShares": 3.0,
                "avgPrice": 0.42,
                "createdAt": "2026-07-24T12:00:00Z",
            },
        )

    adapter = AggPaperExecutionAdapter(
        "account-1",
        app_id="app-1",
        api_key="agg_secret",
        slippage_bps=50,
        client=httpx.Client(transport=httpx.MockTransport(handler)),
    )
    client_order_id = adapter.submit_order(
        OrderIntent(
            contract_id=ContractID("agg:outcome-1"),
            side=OrderSide.BUY,
            quantity=Quantity(Decimal("3")),
            order_type=OrderType.LIMIT,
            client_order_id=ClientOrderID("client-1"),
            limit_price=Price(Decimal("0.42")),
        )
    )

    body = json.loads(requests[0].content)
    snapshot = adapter.get_order(client_order_id)
    assert client_order_id == ClientOrderID("client-1")
    assert body == {
        "venueMarketOutcomeId": "outcome-1",
        "side": "buy",
        "shares": 3.0,
        "clientOrderId": "client-1",
        "slippageBps": 50,
    }
    assert requests[0].headers["x-app-id"] == "app-1"
    assert requests[0].headers["x-app-api-key"] == "agg_secret"
    assert snapshot is not None
    assert snapshot.status is OrderStatus.FILLED
    assert snapshot.filled_quantity == Quantity(Decimal("3.0"))
    assert snapshot.average_price == Price(Decimal("0.42"))


def test_paper_order_uses_recoverable_execution_port():
    requests = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.method == "POST":
            return httpx.Response(
                201,
                json={
                    "id": "paper-order-2",
                    "status": "filled",
                    "filledShares": 2.0,
                    "avgPrice": 0.41,
                },
            )
        return httpx.Response(
            200,
            json={
                "data": [
                    {
                        "id": "paper-order-2",
                        "clientOrderId": "client-2",
                        "status": "filled",
                        "filledShares": 2.0,
                        "avgPrice": 0.41,
                    },
                ],
            },
        )

    adapter = AggPaperExecutionAdapter(
        "account-1",
        app_id="app-1",
        api_key="agg_secret",
        client=httpx.Client(transport=httpx.MockTransport(handler)),
    )
    intent = OrderIntent(
        contract_id=ContractID("agg:outcome-2"),
        side=OrderSide.BUY,
        quantity=Quantity(Decimal("2")),
        order_type=OrderType.LIMIT,
        client_order_id=ClientOrderID("client-2"),
        limit_price=Price(Decimal("0.41")),
    )

    assert isinstance(adapter, ExecutionPort)
    prepared = adapter.prepare(intent)
    assert requests == []

    submitted = adapter.submit(prepared)
    reconciled = adapter.reconcile(prepared.reference)

    assert submitted.status is SubmissionStatus.ACCEPTED
    assert submitted.snapshot is not None
    assert submitted.snapshot.status is OrderStatus.FILLED
    assert reconciled.status is ReconciliationStatus.FOUND
    assert reconciled.snapshot is not None
    assert reconciled.snapshot.status is OrderStatus.FILLED
    assert reconciled.snapshot.filled_quantity == Quantity(Decimal("2.0"))
    assert reconciled.snapshot.average_price == Price(Decimal("0.41"))
    assert requests[0].method == "POST"
    assert requests[1].method == "GET"
