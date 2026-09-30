"""Exercise predict execution behavior in the infrastructure predict layer.

Responsibilities
----------------
- Verify predict execution contracts, edge cases, and failure handling.
"""

import asyncio
import json
from dataclasses import replace
from decimal import Decimal
from types import SimpleNamespace

import httpx
import pytest
from predict_sdk import (
    Book,
    ChainId,
    LimitHelperInput,
    MarketHelperInput,
    OrderBuilder,
    Side as PredictSide,
)
from predict_sdk.constants import SignatureType

from prediction_markets.domain.shared.value_objects import (
    ClientOrderID,
    ContractID,
    Price,
    Quantity,
)
from prediction_markets.domain.trading.entities import OrderIntent
from prediction_markets.domain.trading.enums import (
    OrderSide,
    OrderStatus,
    OrderType,
    ReconciliationStatus,
    SubmissionStatus,
    TimeInForce,
)
from prediction_markets.infrastructure.venues.predict.execution import (
    PredictExecutionAdapter,
)
from prediction_markets.infrastructure.venues.predict import execution as execution_module
from prediction_markets.infrastructure.observability.execution_timing import (
    ThreadCallTiming,
    timed_to_thread,
)


class _SmartWalletOrderBuilder:
    """Provide a controllable smart wallet order builder test double."""

    def __init__(self, account_address: str, strategy: str = "LIMIT") -> None:
        self._account_address = account_address
        self._strategy = strategy
        self.balance = 10 * 10**18
        self.allowance = 10 * 10**18
        self.spender = "0x" + "3" * 40
        self.contracts = SimpleNamespace(
            usdt=SimpleNamespace(functions=_USDTFunctions(self)),
        )

    def sign_predict_account_message(self, message: str) -> str:
        assert message == "Sign in to Predict"
        return "0xsmart-wallet-auth-signature"

    def get_limit_order_amounts(self, data):
        assert data.side is PredictSide.BUY
        return SimpleNamespace(
            maker_amount=data.price_per_share_wei * data.quantity_wei // 10**18,
            taker_amount=data.quantity_wei,
            price_per_share=data.price_per_share_wei,
        )

    def build_order(self, strategy: str, data):
        assert strategy == self._strategy
        return SimpleNamespace(
            salt="123",
            maker=self._account_address,
            signer=self._account_address,
            taker="0x0000000000000000000000000000000000000000",
            token_id=str(data.token_id),
            maker_amount=str(data.maker_amount),
            taker_amount=str(data.taker_amount),
            expiration="4102444800",
            nonce="0",
            fee_rate_bps=str(data.fee_rate_bps),
            side=data.side,
            signature_type=SignatureType.EOA,
        )

    def build_typed_data(self, order, **kwargs):
        assert kwargs == {"is_neg_risk": False, "is_yield_bearing": False}
        return order

    def sign_typed_data_order(self, order):
        return SimpleNamespace(**order.__dict__, signature="0xsmart-wallet-order-signature")

    def build_typed_data_hash(self, order) -> str:
        return "0xabc"

    def balance_of(self, *, address: str) -> int:
        assert address == self._account_address
        return self.balance

    def get_approval_steps(self, scope):
        assert scope.operation == "TRADE"
        assert scope.is_neg_risk is False
        assert scope.is_yield_bearing is False
        assert scope.side is PredictSide.BUY
        return [SimpleNamespace(type="ERC20_ALLOWANCE", spender=self.spender)]


class _USDTFunctions:
    """Expose a deterministic Predict USDT allowance call."""

    def __init__(self, builder: _SmartWalletOrderBuilder) -> None:
        self._builder = builder

    def allowance(self, owner: str, spender: str):
        assert owner == self._builder._account_address
        assert spender == self._builder.spender
        return SimpleNamespace(call=lambda: self._builder.allowance)


def _collateral_adapter():
    """Build one Predict adapter with deterministic market and collateral reads."""

    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        assert request.url.path == "/v1/markets/29076"
        return httpx.Response(
            200,
            json={
                "success": True,
                "data": {
                    "id": 29076,
                    "feeRateBps": 25,
                    "isNegRisk": False,
                    "isYieldBearing": False,
                    "outcomes": [
                        {"indexSet": 1, "onChainId": "290761"},
                        {"indexSet": 2, "onChainId": "290762"},
                    ],
                },
            },
        )

    account_address = "0x" + "2" * 40
    builder = _SmartWalletOrderBuilder(account_address)
    client = httpx.Client(transport=httpx.MockTransport(handler))
    adapter = PredictExecutionAdapter(
        privy_private_key="0x" + "1" * 64,
        account_address=account_address,
        api_key="api-key",
        base_url="https://example.test",
        client=client,
        order_builder=builder,
    )
    intent = OrderIntent(
        contract_id=ContractID("predict:29076:yes"),
        side=OrderSide.BUY,
        quantity=Quantity(Decimal("5")),
        order_type=OrderType.LIMIT,
        client_order_id=ClientOrderID("client-preflight"),
        limit_price=Price(Decimal("0.42")),
        time_in_force=TimeInForce.IOC,
    )
    return adapter, builder, client, intent, requests


def test_reads_predict_collateral_constrained_by_balance_and_allowance():
    """Expose the most restrictive Predict USDT account value."""
    adapter, builder, client, intent, _ = _collateral_adapter()
    contracts = (intent.contract_id,)

    builder.balance = 973_763 * 10**12
    balance_limited = adapter.get_available_collateral(contracts)
    builder.balance = 10 * 10**18
    builder.allowance = 2_100_000_000_000_000_000
    allowance_limited = adapter.get_available_collateral(contracts)

    assert balance_limited == Decimal("0.973763")
    assert allowance_limited == Decimal("2.1")
    client.close()


def test_predict_collateral_read_ignores_duplicate_market_scopes():
    """Read one allowance for both outcomes of the same Predict market."""
    adapter, builder, client, _, _ = _collateral_adapter()

    assert adapter.get_available_collateral(
        (
            ContractID("predict:29076:yes"),
            ContractID("predict:29076:no"),
        ),
    ) == Decimal("10")
    assert adapter.get_available_collateral(()) is None
    client.close()


@pytest.mark.parametrize(
    ("side", "book", "protected_price"),
    (
        (
            PredictSide.BUY,
            Book(market_id=1, update_timestamp_ms=0, asks=[(0.5, 5.0)], bids=[]),
            Decimal("0.52"),
        ),
        (
            PredictSide.SELL,
            Book(market_id=1, update_timestamp_ms=0, asks=[], bids=[(0.5, 5.0)]),
            Decimal("0.48"),
        ),
    ),
)
def test_price_protected_amounts_match_official_market_helper(
    side,
    book,
    protected_price,
):
    """Match signed amounts for two ticks of manual and official slippage."""
    builder = OrderBuilder.make(ChainId.BNB_MAINNET)
    quantity_wei = 5 * 10**18
    protected = builder.get_limit_order_amounts(
        LimitHelperInput(
            side=side,
            price_per_share_wei=int(protected_price * 10**18),
            quantity_wei=quantity_wei,
        ),
    )
    official = builder.get_market_order_amounts(
        MarketHelperInput(
            side=side,
            quantity_wei=quantity_wei,
            slippage_bps=400,
        ),
        book,
    )

    assert protected.maker_amount == official.maker_amount
    assert protected.taker_amount == official.taker_amount
    assert official.amount == quantity_wei


@pytest.mark.parametrize(
    ("time_in_force", "limit_fok_enabled", "strategy", "is_fill_or_kill"),
    (
        (TimeInForce.IOC, False, "LIMIT", False),
        (TimeInForce.FOK, False, "LIMIT", True),
        (TimeInForce.IOC, True, "LIMIT", True),
    ),
)
def test_predict_execution_submits_price_protected_order(
    time_in_force,
    limit_fok_enabled,
    strategy,
    is_fill_or_kill,
):
    requests: list[httpx.Request] = []
    submitted: dict = {}
    removal = {"confirmed": False}
    stale_order_fill = {"enabled": False}
    matches_available = {"enabled": True}
    order = {
        "hash": "0xabc",
        "tokenId": "290761",
        "makerAmount": "2100000000000000000",
        "takerAmount": "5000000000000000000",
        "side": 0,
    }

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.path == "/v1/auth/message":
            return httpx.Response(
                200,
                json={"success": True, "data": {"message": "Sign in to Predict"}},
            )
        if request.url.path == "/v1/auth":
            return httpx.Response(
                200,
                json={"success": True, "data": {"token": "jwt-token"}},
            )
        if request.url.path == "/v1/markets/29076":
            return httpx.Response(
                200,
                json={
                    "success": True,
                    "data": {
                        "id": 29076,
                        "feeRateBps": 25,
                        "isNegRisk": False,
                        "isYieldBearing": False,
                        "outcomes": [
                            {"indexSet": 1, "onChainId": "290761"},
                            {"indexSet": 2, "onChainId": "290762"},
                        ],
                    },
                },
            )
        if request.url.path == "/v1/orders" and request.method == "POST":
            submitted.update(__import__("json").loads(request.content))
            order.update(submitted["data"]["order"])
            return httpx.Response(
                201,
                json={
                    "success": True,
                    "data": {"orderId": "api-1", "orderHash": order["hash"]},
                },
            )
        if request.url.path == "/v1/orders":
            return httpx.Response(
                200,
                json={
                    "success": True,
                    "cursor": "",
                    "data": [
                        {
                            "id": "api-1",
                            "marketId": 29076,
                            "amountFilled": "0",
                            "status": "OPEN",
                            "order": order,
                        }
                    ],
                },
            )
        if request.url.path == "/v1/orders/remove":
            removal["confirmed"] = True
            return httpx.Response(
                200,
                json={"success": True, "removed": ["api-1"], "noop": []},
            )
        if request.url.path == "/v1/orders/matches":
            if not matches_available["enabled"]:
                return httpx.Response(503)
            return httpx.Response(
                200,
                json={
                    "success": True,
                    "cursor": "",
                    "data": [
                        {
                            "amountFilled": (
                                "5" if stale_order_fill["enabled"] else "2"
                            ),
                            "priceExecuted": "0.39",
                            "taker": {
                                "hash": "0xabc",
                                "fee": {"amount": "0.04", "type": "SHARES"},
                            },
                            "makers": [],
                        }
                    ],
                },
            )
        if request.url.path.startswith("/v1/orders/"):
            return httpx.Response(
                200,
                json={
                    "success": True,
                    "data": {
                        "id": "api-1",
                        "marketId": 29076,
                        "amountFilled": (
                            "0"
                            if stale_order_fill["enabled"]
                            else "2000000000000000000"
                        ),
                        "status": "CANCELLED" if removal["confirmed"] else "OPEN",
                        "order": order,
                    },
                },
            )
        raise AssertionError(f"Unexpected request: {request.method} {request.url}")

    client = httpx.Client(transport=httpx.MockTransport(handler))
    account_address = "0x" + "2" * 40
    adapter = PredictExecutionAdapter(
        privy_private_key="0x" + "1" * 64,
        account_address=account_address,
        api_key="api-key",
        base_url="https://example.test",
        client=client,
        order_builder=_SmartWalletOrderBuilder(account_address, strategy),
        limit_fok_enabled=limit_fok_enabled,
    )
    intent = OrderIntent(
        contract_id=ContractID("predict:29076:yes"),
        side=OrderSide.BUY,
        quantity=Quantity(Decimal("5")),
        order_type=OrderType.LIMIT,
        client_order_id=ClientOrderID("client-1"),
        limit_price=Price(Decimal("0.42")),
        time_in_force=time_in_force,
    )

    adapter.preload((intent.contract_id,))
    market_requests_before_prepare = sum(
        request.url.path == "/v1/markets/29076" for request in requests
    )
    requests_before_prepare = len(requests)
    prepared = adapter.prepare(intent)
    assert adapter.cancellation_window_seconds(intent) == (
        None if is_fill_or_kill else 1.0
    )
    assert len(requests) == requests_before_prepare
    assert sum(
        request.url.path == "/v1/markets/29076" for request in requests
    ) == market_requests_before_prepare
    auth_requests_before_submit = sum(
        request.url.path in {"/v1/auth/message", "/v1/auth"}
        for request in requests
    )
    submitted_result = adapter.submit(prepared)
    assert len(requests) == requests_before_prepare + 1
    assert requests[-1].method == "POST" and requests[-1].url.path == "/v1/orders"
    assert sum(
        request.url.path in {"/v1/auth/message", "/v1/auth"}
        for request in requests
    ) == auth_requests_before_submit
    reconciled = adapter.reconcile(prepared.reference)
    cancelled = adapter.cancel(prepared.reference)

    assert prepared.reference.client_order_id == ClientOrderID("client-1")
    assert submitted_result.status is SubmissionStatus.ACCEPTED
    assert submitted_result.snapshot is not None
    assert submitted_result.snapshot.status is OrderStatus.SUBMITTED
    assert reconciled.status is ReconciliationStatus.FOUND
    assert reconciled.snapshot is not None
    assert reconciled.snapshot.status is OrderStatus.PARTIALLY_FILLED
    assert reconciled.snapshot.may_receive_more_fills is True
    assert reconciled.snapshot.average_price == Price(Decimal("0.39"))
    assert reconciled.snapshot.fee is not None
    assert reconciled.snapshot.fee.charged.amount == Decimal("0.04")
    assert cancelled.status is ReconciliationStatus.FOUND
    assert cancelled.snapshot is not None
    assert cancelled.snapshot.status is OrderStatus.CANCELLED
    assert cancelled.snapshot.may_receive_more_fills is True
    stale_order_fill["enabled"] = True
    late_fill = adapter.reconcile(prepared.reference)
    assert late_fill.status is ReconciliationStatus.FOUND
    assert late_fill.snapshot is not None
    assert late_fill.snapshot.status is OrderStatus.FILLED
    assert late_fill.snapshot.may_receive_more_fills is False
    assert late_fill.snapshot.filled_quantity == Quantity(Decimal("5"))
    assert late_fill.snapshot.average_price == Price(Decimal("0.39"))
    matches_available["enabled"] = False
    uncertain_zero_fill = adapter.reconcile(prepared.reference)
    assert uncertain_zero_fill.status is ReconciliationStatus.UNKNOWN
    assert uncertain_zero_fill.snapshot is None
    assert submitted["data"]["strategy"] == strategy
    assert submitted["data"]["isFillOrKill"] is is_fill_or_kill
    assert submitted["data"]["pricePerShare"] == "420000000000000000"
    assert submitted["data"]["order"]["feeRateBps"] == "25"
    assert submitted["data"]["order"]["makerAmount"] == "2100000000000000000"
    assert submitted["data"]["order"]["takerAmount"] == "5000000000000000000"
    assert submitted["data"]["order"]["maker"] == account_address
    assert submitted["data"]["order"]["signature"] == "0xsmart-wallet-order-signature"
    assert sum(request.url.path == "/v1/auth/message" for request in requests) == 1
    authenticated = [
        request
        for request in requests
        if request.url.path.startswith("/v1/orders")
    ]
    assert all(
        request.headers["authorization"] == "Bearer jwt-token"
        for request in authenticated
    )
    auth_request = next(request for request in requests if request.url.path == "/v1/auth")
    assert __import__("json").loads(auth_request.content) == {
        "signer": account_address,
        "signature": "0xsmart-wallet-auth-signature",
        "message": "Sign in to Predict",
    }


def test_predict_prepare_fails_without_preload_and_does_not_fetch_market():
    """Reject a cold prepare without repairing the cache on the hot path."""
    adapter, _, client, intent, _ = _collateral_adapter()

    with pytest.raises(
        RuntimeError,
        match="Predict market 29076 was not preloaded",
    ):
        adapter.prepare(intent)

    client.close()


def test_predict_prepare_fails_closed_without_prewarmed_auth_and_does_no_http():
    """Keep authentication repair outside request preparation."""
    adapter, _, client, intent, requests = _collateral_adapter()
    adapter.get_available_collateral((intent.contract_id,))
    requests_before_prepare = len(requests)

    with pytest.raises(RuntimeError, match="authentication is not prewarmed"):
        adapter.prepare(intent)

    assert len(requests) == requests_before_prepare
    client.close()


def test_prepare_persists_signed_fractional_quantity_without_http():
    """Recover the exact rounded wire quantity without fetching venue state."""
    adapter, _, client, intent, requests = _collateral_adapter()
    try:
        adapter.get_available_collateral((intent.contract_id,))
        adapter._jwt = "cached-jwt"
        intent = replace(intent, quantity=Quantity(Decimal("2.296296296296296298")))
        before = len(requests)
        prepared = adapter.prepare(intent)
        reference = json.loads(prepared.reference.recovery_data)
        signed = json.loads(prepared.request)["data"]["order"]
        assert Decimal(reference["quantity"]) == Decimal("2.2962")
        assert Decimal(signed["takerAmount"]) / Decimal(10**18) == Decimal(reference["quantity"])
        assert execution_module._snapshot_from_reference(prepared.reference, order_id=None).quantity.value == Decimal("2.2962")
        assert len(requests) == before
    finally:
        client.close()


@pytest.mark.parametrize("ready_auth", [True, False])
def test_prepare_timing_preserves_payload_and_uses_only_cached_data(ready_auth):
    """Record successful and failed cached preparation without adding HTTP."""
    adapter, _, client, intent, requests = _collateral_adapter()
    try:
        adapter.get_available_collateral((intent.contract_id,))
        adapter._jwt = "cached-jwt" if ready_auth else None
        before = len(requests)
        timing = ThreadCallTiming()
        if ready_auth:
            expected = adapter.prepare(intent)
            actual = asyncio.run(timed_to_thread(timing, adapter.prepare, intent))
            assert actual == expected
            assert set(timing.phases_ns) == {
                "predict_cached_jwt_lock_wait", "predict_cached_jwt_check",
                "predict_sdk_build", "predict_sdk_sign", "predict_sdk_hash",
                "predict_payload_serialization",
            }
        else:
            with pytest.raises(RuntimeError, match="authentication is not prewarmed"):
                asyncio.run(timed_to_thread(timing, adapter.prepare, intent))
            assert set(timing.phases_ns) == {
                "predict_cached_jwt_lock_wait", "predict_cached_jwt_check",
            }
        assert len(requests) == before
        assert timing.finished_at_ns is not None
        assert all(elapsed >= 0 for elapsed in timing.phases_ns.values())
    finally:
        client.close()


def test_cached_jwt_timing_separates_lock_acquisition_from_checks(monkeypatch):
    """Attribute lock wait separately from readiness and metric updates."""
    adapter, _, client, _, requests = _collateral_adapter()
    adapter._jwt = "cached-jwt"
    clock = [0]
    released = []

    class DelayedLock:
        """Advance a deterministic wall clock while acquiring the test lock."""

        def __enter__(self):
            clock[0] += 7_000_000

        def __exit__(self, *_):
            released.append(True)

    def update_metrics():
        clock[0] += 3_000_000

    monkeypatch.setattr(adapter, "_auth_lock", DelayedLock())
    monkeypatch.setattr(adapter, "_set_auth_metrics_locked", update_metrics)
    monkeypatch.setattr(execution_module.time, "monotonic_ns", lambda: clock[0])
    timing = ThreadCallTiming()
    try:
        assert asyncio.run(timed_to_thread(timing, adapter._ready_token)) == "cached-jwt"
        assert timing.snapshot()["phases_ms"] == {
            "predict_cached_jwt_lock_wait": 7,
            "predict_cached_jwt_check": 3,
        }
        assert released == [True]
        assert requests == []
    finally:
        client.close()


def test_predict_owned_http_client_enables_keep_alive(monkeypatch) -> None:
    """Create the owned synchronous client through the instrumented factory."""
    captured: dict[str, object] = {}

    def instrumented_client(_venue: str, **kwargs):
        captured.update(kwargs)
        return httpx.Client(transport=httpx.MockTransport(lambda _: httpx.Response(200)))

    monkeypatch.setattr(execution_module, "instrumented_client", instrumented_client)
    account_address = "0x" + "2" * 40
    adapter = PredictExecutionAdapter(
        privy_private_key="0x" + "1" * 64,
        account_address=account_address,
        api_key="api-key",
        base_url="https://example.test",
        order_builder=_SmartWalletOrderBuilder(account_address),
    )

    assert isinstance(captured["limits"], httpx.Limits)
    assert captured["limits"].keepalive_expiry == 60
    adapter.close()
