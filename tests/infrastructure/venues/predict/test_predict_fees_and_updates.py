"""Verify Predict fee curves and private wallet-event normalization."""

import asyncio
from decimal import Decimal

from prediction_markets.domain.shared.value_objects import (
    ClientOrderID,
    ContractID,
    OrderID,
    Price,
    Quantity,
)
from prediction_markets.domain.trading.entities import OrderSnapshot
from prediction_markets.domain.trading.enums import OrderSide, OrderStatus, OrderType
from prediction_markets.domain.trading.value_objects import OrderReference
from prediction_markets.infrastructure.venues.predict.mappers import PREDICT_VENUE_ID
from prediction_markets.infrastructure.venues.predict.order_updates import (
    PredictOrderUpdateAdapter,
)
from prediction_markets.infrastructure.venues.predict.taker_fees import (
    PredictTakerFeeCalculator,
)


def test_ready_predict_wallet_watch_avoids_the_wait_path(monkeypatch) -> None:
    """Return immediately when the account-wide wallet stream is already ready."""
    adapter = PredictOrderUpdateAdapter(lambda: "jwt", api_key="key")

    async def unexpected_wait(*args, **kwargs):
        raise AssertionError("ready wallet watch entered the wait path")

    monkeypatch.setattr(asyncio, "wait", unexpected_wait)

    async def run() -> None:
        adapter._task = asyncio.create_task(asyncio.Event().wait())
        adapter._ready.set()
        await adapter.watch(ContractID("predict:42:yes"))
        await adapter.close()

    asyncio.run(run())


def test_predict_fee_and_wallet_fill_are_normalized() -> None:
    calculator = PredictTakerFeeCalculator(
        fee_rates_bps={"42": 200, "43": 400},
    )
    contract_id = ContractID("predict:42:yes")
    fee = calculator.calculate(
        contract_id,
        Price(Decimal("0.4")),
        Quantity(Decimal("10")),
        OrderSide.BUY,
    )
    assert fee.charged.amount == Decimal("0.2")
    assert str(fee.charged.currency) == "OUTCOME_TOKEN"

    other_market_fee = calculator.calculate(
        ContractID("predict:43:yes"),
        Price(Decimal("0.4")),
        Quantity(Decimal("10")),
        OrderSide.BUY,
    )
    assert other_market_fee.charged.amount == Decimal("0.4")

    adapter = PredictOrderUpdateAdapter(lambda: "jwt", api_key="key")
    reference = OrderReference(
        PREDICT_VENUE_ID,
        ClientOrderID("client-1"),
        b"recovery",
    )
    snapshot = OrderSnapshot(
        status=OrderStatus.SUBMITTED,
        contract_id=contract_id,
        side=OrderSide.BUY,
        quantity=Quantity(Decimal("10")),
        order_type=OrderType.LIMIT,
        client_order_id=reference.client_order_id,
        order_id=OrderID("0xabc"),
        limit_price=Price(Decimal("0.4")),
    )
    adapter.record_snapshot(reference, snapshot, "submit")
    adapter._handle(
        {
            "type": "orderTransactionSuccess",
            "orderHash": "0xabc",
            "timestamp": 1784813400000,
            "settlementId": "settlement-1",
            "details": {"quantity": "10", "quantityFilled": "2"},
            "fill": {
                "executedSizeWei": "2000000000000000000",
                "executedPriceWei": "400000000000000000",
            },
        }
    )

    tracked = adapter.record_snapshot(reference, snapshot, "submit")
    assert tracked.status is OrderStatus.PARTIALLY_FILLED
    assert tracked.filled_quantity.value == Decimal("2")
    assert tracked.average_price == Price(Decimal("0.4"))
