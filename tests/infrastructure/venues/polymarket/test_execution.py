"""Verify recoverable Polymarket execution behavior."""

import asyncio
import json
from decimal import Decimal

import httpx
import pytest
from py_clob_client_v2.clob_types import CreateOrderOptions
from py_clob_client_v2.exceptions import PolyApiException
from py_clob_client_v2.order_builder.builder import OrderBuilder
from py_clob_client_v2.order_utils.model.order_data_v2 import SignedOrderV2
from py_clob_client_v2.signer import Signer

from prediction_markets.domain.contracts.value_objects import TickSize
from prediction_markets.domain.shared.value_objects import (
    ClientOrderID,
    ContractID,
    OrderID,
    Price,
    Quantity,
)
from prediction_markets.domain.trading.entities import OrderIntent, OrderSnapshot
from prediction_markets.domain.trading.enums import (
    OrderSide,
    OrderStatus,
    OrderType,
    ReconciliationStatus,
    SubmissionStatus,
    TimeInForce,
)
from prediction_markets.infrastructure.venues.polymarket.execution import (
    PolymarketExecutionAdapter,
    PolymarketFillEnricher,
)


class _Client:
    """Provide deterministic signed orders and CLOB responses."""

    def __init__(self) -> None:
        self.created = []
        self.create_options = []
        self.posted = []
        self.cancelled = []
        self.order = None
        self.trades = []
        self.market_info = {"mts": "0.01", "fd": {"r": "0.25", "e": "2"}}
        self.market_info_calls = []
        self.neg_risk_calls = []
        self.version_calls = 0
        self.tick_size = "0.01"
        self._ClobClient__tick_sizes = {}
        self.balance = 10_000_000
        self.allowances = {"exchange": 10_000_000}

    def _ClobClient__resolve_version(self):
        self.version_calls += 1
        return 2

    def get_neg_risk(self, token_id):
        self.neg_risk_calls.append(token_id)
        return False

    def get_tick_size(self, token_id):
        return self._ClobClient__tick_sizes.setdefault(token_id, self.tick_size)

    def create_order(self, args, options=None):
        if Decimal(options.tick_size) < Decimal(
            self._ClobClient__tick_sizes[args.token_id],
        ):
            raise ValueError("SDK tick cache rejected the adapter tick")
        self.created.append(args)
        self.create_options.append(options)
        order = SignedOrderV2(
            salt="1",
            maker="0xmaker",
            signer="0xsigner",
            tokenId=args.token_id,
            makerAmount="2100000",
            takerAmount="5000000",
            side=0 if args.side == "BUY" else 1,
            signatureType=0,
            timestamp="1",
            metadata="0x00",
            builder="0xbuilder",
            expiration=str(args.expiration),
            signature="0xsigned",
        )
        order.order_id = "venue-1"
        return order

    def post_order(self, order, order_type, post_only=False):
        self.posted.append((order, order_type, post_only))
        self.order = {
            "id": "venue-1",
            "status": "LIVE",
            "market": "condition-1",
            "asset_id": "token-1",
            "side": "BUY" if order.side == 0 else "SELL",
            "original_size": "5",
            "size_matched": "0",
            "price": "0.42",
            "created_at": 1,
        }
        return {"success": True, "orderID": "venue-1"}

    def get_order(self, order_id):
        assert order_id == "venue-1"
        return self.order

    def cancel_order(self, payload):
        self.cancelled.append(payload.orderID)
        self.order["status"] = "CANCELED"
        return {"canceled": [payload.orderID], "not_canceled": {}}

    def get_trades(self, params, only_first_page=False):
        return self.trades

    def get_clob_market_info(self, condition_id):
        self.market_info_calls.append(condition_id)
        self.tick_size = self.market_info["mts"]
        return self.market_info

    def get_balance_allowance(self, params):
        return {
            "balance": str(self.balance),
            "allowances": {
                address: str(value) for address, value in self.allowances.items()
            },
        }


def _intent(**changes):
    values = {
        "contract_id": ContractID("polymarket:condition-1:token-1"),
        "side": OrderSide.BUY,
        "quantity": Quantity(Decimal("5")),
        "order_type": OrderType.LIMIT,
        "client_order_id": ClientOrderID("client-1"),
        "limit_price": Price(Decimal("0.42")),
    }
    values.update(changes)
    return OrderIntent(**values)


def _preloaded_adapter(client: _Client, **kwargs) -> PolymarketExecutionAdapter:
    """Build an execution adapter with metadata ready for the signing hot path."""
    adapter = PolymarketExecutionAdapter(client=client, **kwargs)
    adapter.preload((_intent().contract_id,))
    return adapter


class _SigningClient(_Client):
    """Build real SDK signatures with a synthetic key and no venue I/O."""

    def create_order(self, args, options=None):
        """Sign the actual SDK-generated amounts without posting them."""
        self.created.append(args)
        self.create_options.append(options)
        builder = OrderBuilder(Signer("0x" + "01" * 32, 137))
        order = builder.build_order(
            args,
            CreateOrderOptions(tick_size=options.tick_size, neg_risk=options.neg_risk),
            version=2,
        )
        order.order_id = "venue-1"
        return order

    def post_order(self, order, order_type, post_only=False):
        """Expose a resting venue snapshot matching the signed order."""
        response = super().post_order(order, order_type, post_only)
        shares = Decimal(order.takerAmount if order.side == 0 else order.makerAmount)
        cash = Decimal(order.makerAmount if order.side == 0 else order.takerAmount)
        self.order.update(
            asset_id=order.tokenId, original_size=str(shares / 1_000_000),
            price=str(cash / shares),
        )
        return response


@pytest.mark.parametrize(
    "side,price,tick,wire_type",
    [
        (OrderSide.BUY, "0.92", "0.01", "GTC"),
        (OrderSide.BUY, "0.50", "0.01", "FAK"),
        (OrderSide.BUY, "0.999", "0.001", "GTC"),
        (OrderSide.BUY, "0.9975", "0.0025", "GTC"),
        (OrderSide.SELL, "0.92", "0.01", "FAK"),
        (OrderSide.SELL, "0.999", "0.001", "GTC"),
        (OrderSide.SELL, "0.9975", "0.0025", "FAK"),
    ],
)
def test_real_sdk_preserves_exact_ioc_shares_and_valid_wire_precision(side, price, tick, wire_type):
    """Reproduce fractional residuals with the real SDK amount builder."""
    client = _SigningClient()
    client.market_info["mts"] = tick
    adapter = PolymarketExecutionAdapter(client=client, post_only=True)
    intent = _intent(
        contract_id=ContractID("polymarket:condition-1:1"),
        side=side, quantity=Quantity(Decimal("10.16")),
        limit_price=Price(Decimal(price)), time_in_force=TimeInForce.IOC,
    )
    adapter.preload((intent.contract_id,))
    prepared = adapter.prepare(intent)
    request = json.loads(prepared.request)
    order = request["order"]
    shares = int(order["takerAmount"] if side is OrderSide.BUY else order["makerAmount"])
    cash = int(order["makerAmount"] if side is OrderSide.BUY else order["takerAmount"])
    assert shares == 10_160_000
    assert Decimal(cash) / 1_000_000 == Decimal("10.16") * Decimal(price)
    assert request["order_type"] == wire_type
    assert request["post_only"] is False
    assert adapter.cancellation_window_seconds(intent) == (5 if wire_type == "GTC" else None)
    if wire_type == "FAK":
        assert int(order["makerAmount"]) % 10_000 == 0
        assert int(order["takerAmount"]) % 100 == 0
    assert client.posted == []


def test_fok_cannot_silently_change_precision_or_become_a_resting_order():
    """Reject an impossible exact FOK amount before any submission."""
    client = _SigningClient()
    adapter = PolymarketExecutionAdapter(client=client)
    intent = _intent(
        contract_id=ContractID("polymarket:condition-1:1"),
        quantity=Quantity(Decimal("10.16")), limit_price=Price(Decimal("0.92")),
        time_in_force=TimeInForce.FOK,
    )
    adapter.preload((intent.contract_id,))
    with pytest.raises(ValueError, match="maker/taker precision"):
        adapter.prepare(intent)
    assert adapter.cancellation_window_seconds(intent) is None
    assert client.posted == []


def test_fractional_polymarket_ioc_is_cancelled_by_the_dispatcher(tmp_path, monkeypatch):
    """Never leave the precision-preserving GTC resting without cancellation."""
    from prediction_markets.application.events import OrderSnapshotUpdated, SubmissionReceived, SubmitOrder
    from prediction_markets.application.pipeline import EventSink, OutputDispatcher, RingBuffer
    from prediction_markets.application.state import TradingState
    from prediction_markets.domain.shared.value_objects import VenueID
    from prediction_markets.infrastructure.binary_journal import BinaryJournal

    monkeypatch.setattr(
        "prediction_markets.infrastructure.venues.polymarket.execution._IOC_RESTING_WINDOW_SECONDS", 0.01,
    )

    async def run():
        client = _SigningClient()
        adapter = PolymarketExecutionAdapter(client=client)
        intent = _intent(
            contract_id=ContractID("polymarket:condition-1:1"),
            quantity=Quantity(Decimal("10.16")), limit_price=Price(Decimal("0.92")),
            time_in_force=TimeInForce.IOC,
        )
        adapter.preload((intent.contract_id,))
        journal = BinaryJournal(tmp_path / "fractional-recovery.log")
        inputs = RingBuffer(16)
        dispatcher = OutputDispatcher(RingBuffer(1), EventSink(inputs), journal, TradingState())
        dispatcher.configure({VenueID("POLYMARKET"): adapter})
        try:
            await dispatcher.execute(SubmitOrder("fractional", "recovery", VenueID("POLYMARKET"), intent))
            assert isinstance(await inputs.get(), SubmissionReceived)
            while True:
                event = await asyncio.wait_for(inputs.get(), 2)
                if isinstance(event, OrderSnapshotUpdated) and event.snapshot.is_terminal():
                    assert event.snapshot.status is OrderStatus.CANCELLED
                    break
            assert client.posted[0][1:] == ("GTC", False)
            assert client.cancelled == ["venue-1"]
        finally:
            await dispatcher.close()
            journal.close()

    asyncio.run(run())


def test_prepares_exact_signed_request_and_reconciles_after_restart():
    client = _Client()
    prepared = _preloaded_adapter(client, post_only=True).prepare(_intent())

    assert prepared.reference.recovery_data == b"venue-1"
    assert len(client.created) == 1

    restarted = PolymarketExecutionAdapter(client=client, post_only=False)
    assert restarted.reconcile(prepared.reference).status is ReconciliationStatus.NOT_FOUND
    submitted = restarted.submit(prepared)
    reconciled = restarted.reconcile(prepared.reference)

    assert len(client.created) == 1
    assert submitted.status is SubmissionStatus.ACCEPTED
    assert client.posted[0][1:] == ("GTC", True)
    assert reconciled.status is ReconciliationStatus.FOUND
    assert reconciled.snapshot.status is OrderStatus.ACCEPTED
    assert reconciled.snapshot.client_order_id == ClientOrderID("client-1")


def test_reads_collateral_constrained_by_balance_and_allowance():
    """Expose the most restrictive account-wide Polymarket cash value."""
    client = _Client()
    adapter = PolymarketExecutionAdapter(client=client)
    contracts = (_intent().contract_id,)

    client.balance = 973_763
    balance_limited = adapter.get_available_collateral(contracts)
    client.balance = 10_000_000
    client.allowances["exchange"] = 2_100_000
    allowance_limited = adapter.get_available_collateral(contracts)

    assert balance_limited == Decimal("0.973763")
    assert allowance_limited == Decimal("2.1")
    assert client.posted == []


def test_collateral_read_requires_clob_allowances():
    """Fail closed when the CLOB omits account allowance data."""
    client = _Client()
    adapter = PolymarketExecutionAdapter(client=client)
    client.allowances = {}

    try:
        adapter.get_available_collateral((_intent().contract_id,))
    except RuntimeError as error:
        assert str(error) == "Polymarket collateral allowances were not returned"
    else:
        raise AssertionError("missing Polymarket allowances were accepted")


def test_rejects_price_not_supported_by_clob_before_signing():
    client = _Client()
    adapter = _preloaded_adapter(client)

    try:
        adapter.prepare(_intent(limit_price=Price(Decimal("0.992"))))
    except ValueError as exc:
        assert str(exc) == "invalid Polymarket price (0.992), min: 0.01 - max: 0.99"
    else:
        raise AssertionError("unsupported CLOB price was accepted")

    assert client.created == []


def test_live_tick_update_repairs_cache_without_hot_path_http():
    client = _Client()
    adapter = PolymarketExecutionAdapter(client=client)
    contract = ContractID("polymarket:condition-1:token-1")
    adapter.preload((contract,))
    client.market_info["mts"] = "0.001"

    adapter.update_tick_size(contract, TickSize(Decimal("0.001")))
    adapter.prepare(_intent(limit_price=Price(Decimal("0.997"))))

    assert len(client.created) == 1
    assert client.create_options[0].tick_size == "0.001"
    assert client._ClobClient__tick_sizes["token-1"] == "0.001"
    assert client.market_info_calls == ["condition-1"]


def test_prepare_without_preload_fails_without_metadata_requests():
    """Reject cold signing instead of discovering metadata during preparation."""
    client = _Client()
    adapter = PolymarketExecutionAdapter(client=client)

    try:
        adapter.prepare(_intent())
    except RuntimeError as error:
        assert str(error) == "Polymarket order version was not preloaded"
    else:
        raise AssertionError("cold Polymarket preparation was accepted")

    assert client.version_calls == 0
    assert client.market_info_calls == []
    assert client.neg_risk_calls == []
    assert client.created == []


def test_preloads_each_condition_and_order_version_once():
    client = _Client()
    adapter = PolymarketExecutionAdapter(client=client)
    contracts = (
        ContractID("polymarket:condition-1:token-1"),
        ContractID("polymarket:condition-1:token-2"),
        ContractID("polymarket:condition-2:token-3"),
    )

    tick_sizes = adapter.preload(contracts)
    adapter.preload(contracts)
    adapter.prepare(_intent())

    assert client.version_calls == 1
    assert client.market_info_calls == ["condition-1", "condition-2"]
    assert client.neg_risk_calls == ["token-1", "token-2", "token-3"]
    assert {tick.value for tick in tick_sizes.values()} == {Decimal("0.01")}
    assert client.create_options[0].tick_size == "0.01"
    assert client.create_options[0].neg_risk is False


def test_cancels_using_only_the_persisted_reference():
    client = _Client()
    adapter = _preloaded_adapter(client)
    prepared = adapter.prepare(_intent())
    adapter.submit(prepared)

    result = PolymarketExecutionAdapter(client=client).cancel(prepared.reference)

    assert client.cancelled == ["venue-1"]
    assert result.status is ReconciliationStatus.FOUND
    assert result.snapshot.status is OrderStatus.CANCELLED


def test_market_intent_uses_a_bounded_marketable_limit():
    client = _Client()
    client.get_order_book = lambda token_id: {
        "bids": [{"price": "0.28"}],
        "asks": [{"price": "0.35"}],
    }
    prepared = _preloaded_adapter(client, post_only=True).prepare(
        _intent(
            order_type=OrderType.MARKET,
            limit_price=None,
            time_in_force=TimeInForce.IOC,
        ),
    )

    PolymarketExecutionAdapter(client=client).submit(prepared)

    assert client.created[0].price == 0.35
    assert client.posted[0][1:] == ("GTC", False)
    assert _preloaded_adapter(client).cancellation_window_seconds(
        _intent(order_type=OrderType.MARKET, limit_price=None),
    ) == 5


def test_transport_timeout_keeps_submission_uncertain():
    client = _Client()
    adapter = _preloaded_adapter(client)
    prepared = adapter.prepare(_intent())

    def fail(*args, **kwargs):
        raise httpx.ReadTimeout("timeout")

    client.post_order = fail

    assert adapter.submit(prepared).status is SubmissionStatus.UNKNOWN


def test_rejection_keeps_venue_reason_and_returns_submission_result():
    client = _Client()
    adapter = _preloaded_adapter(client)
    prepared = adapter.prepare(_intent())

    def reject(*args, **kwargs):
        response = httpx.Response(
            400,
            json={
                "orderID": "venue-1",
                "error": "no orders found to match with FAK order",
            },
            request=httpx.Request("POST", "https://clob.polymarket.com/order"),
        )
        raise PolyApiException(response)

    client.post_order = reject
    result = adapter.submit(prepared)

    assert result.status is SubmissionStatus.REJECTED
    assert result.reason == "HTTP 400: no orders found to match with FAK order"


def test_fill_enricher_attaches_average_price_and_fee():
    client = _Client()
    client.trades = [
        {
            "taker_order_id": "venue-1",
            "trader_side": "TAKER",
            "size": "2",
            "price": "0.40",
            "maker_orders": [],
        },
        {
            "taker_order_id": "venue-1",
            "trader_side": "TAKER",
            "size": "3",
            "price": "0.50",
            "maker_orders": [],
        },
    ]
    snapshot = OrderSnapshot(
        status=OrderStatus.FILLED,
        contract_id=ContractID("polymarket:condition-1:token-1"),
        side=OrderSide.BUY,
        quantity=Quantity(Decimal("5")),
        order_type=OrderType.LIMIT,
        client_order_id=ClientOrderID("client-1"),
        order_id=OrderID("venue-1"),
        limit_price=Price(Decimal("0.42")),
        filled_quantity=Quantity(Decimal("5")),
        average_price=Price(Decimal("0.42")),
    )

    enriched = PolymarketFillEnricher(client).enrich(snapshot)

    assert enriched.average_price == Price(Decimal("0.46"))
    assert enriched.fee is not None
    assert enriched.fee.charged.currency.code == "USDC"
    assert enriched.fee.charged.amount > 0
    assert enriched.fee.settlement_cost.currency.code == "USD"


def test_reconcile_uses_fill_enricher_for_matched_orders():
    client = _Client()
    client.order = {
        "id": "venue-1",
        "status": "MATCHED",
        "market": "condition-1",
        "asset_id": "token-1",
        "side": "BUY",
        "original_size": "5",
        "size_matched": "5",
        "price": "0.42",
        "created_at": 1,
    }
    client.trades = [
        {
            "taker_order_id": "venue-1",
            "trader_side": "TAKER",
            "size": "5",
            "price": "0.41",
            "maker_orders": [],
        },
    ]
    adapter = PolymarketExecutionAdapter(client=client)
    adapter.preload((_intent().contract_id,))
    prepared = adapter.prepare(_intent())
    result = adapter.reconcile(prepared.reference)

    assert result.status is ReconciliationStatus.FOUND
    assert result.snapshot.average_price == Price(Decimal("0.41"))
    assert result.snapshot.fee is not None
