"""Verify recoverable Predict.fun inventory operations."""

import json
from decimal import Decimal
from types import SimpleNamespace

import httpx
import pytest

from prediction_markets.domain.outcome_inventory import (
    InventoryOperationID,
    InventoryOperationStatus,
    InventorySubmissionStatus,
    OutcomeInventoryAction,
    OutcomeInventoryIntent,
)
from prediction_markets.domain.shared.value_objects import MarketID, Quantity
from prediction_markets.infrastructure.venues.predict.inventory import (
    PredictOutcomeInventoryAdapter,
)
from prediction_markets.infrastructure.venues.predict.mappers import PREDICT_VENUE_ID

_CONDITION_ID = "0x" + "ab" * 32
_ACCOUNT = "0x0000000000000000000000000000000000000001"
_TX_HASH = bytes.fromhex("12" * 32)


class _Function:
    def __init__(self, value):
        self._value = value

    def call(self):
        return self._value


class _Functions:
    def __init__(self, balances):
        self._balances = balances

    def balanceOf(self, account, token):
        assert account == _ACCOUNT
        return _Function(self._balances[token])

    def payoutDenominator(self, condition_id):
        return _Function(1)

    def payoutNumerators(self, condition_id, index):
        return _Function((1, 0)[index])


class _Contract:
    def __init__(self, balances):
        self.functions = _Functions(balances)


class _Builder:
    _predict_account = _ACCOUNT
    _web3 = None

    def __init__(self):
        contract = _Contract({11: 3 * 10**18, 22: 2 * 10**18})
        self.contracts = SimpleNamespace(
            conditional_tokens=_Contract({11: 1, 22: 1}),
            neg_risk_conditional_tokens=_Contract({11: 1, 22: 1}),
            yield_bearing_conditional_tokens=_Contract({11: 1, 22: 1}),
            yield_bearing_neg_risk_conditional_tokens=contract,
        )
        self.calls = []

    def balance_of(self, *, address):
        assert address == _ACCOUNT
        return 10 * 10**18

    def set_approvals(self, **kwargs):
        return kwargs

    def get_approval_steps(self, scope):
        return [scope]

    def run_approvals(self, steps):
        self.calls.append(("approvals", steps))
        return SimpleNamespace(success=True)

    def split_positions(self, **kwargs):
        self.calls.append(("split", kwargs))
        return SimpleNamespace(success=True, receipt={"transactionHash": _TX_HASH})

    def merge_positions(self, **kwargs):
        self.calls.append(("merge", kwargs))
        return SimpleNamespace(success=True, receipt={"transactionHash": _TX_HASH})

    def redeem_positions(self, *args, **kwargs):
        self.calls.append(("redeem", args, kwargs))
        return SimpleNamespace(success=True, receipt={"transactionHash": _TX_HASH})


class _Client:
    def __init__(self, *, status="REGISTERED", trading_status="OPEN"):
        self.status = status
        self.trading_status = trading_status

    def get(self, url, **kwargs):
        assert url.endswith("/v1/markets/29076")
        return httpx.Response(
            200,
            json={
                "success": True,
                "data": {
                    "status": self.status,
                    "tradingStatus": self.trading_status,
                    "conditionId": _CONDITION_ID,
                    "isNegRisk": True,
                    "isYieldBearing": True,
                    "outcomes": [
                        {"indexSet": 1, "onChainId": "11"},
                        {"indexSet": 2, "onChainId": "22"},
                    ],
                },
            },
            request=httpx.Request("GET", url),
        )


def _intent(action, quantity=None):
    return OutcomeInventoryIntent(
        operation_id=InventoryOperationID(f"predict-{action.value}"),
        venue_id=PREDICT_VENUE_ID,
        market_id=MarketID("29076"),
        action=action,
        quantity=Quantity(Decimal(quantity)) if quantity else None,
    )


def test_selects_yield_bearing_negrisk_balances_and_submits_exact_split() -> None:
    builder = _Builder()
    adapter = PredictOutcomeInventoryAdapter(
        account_address=_ACCOUNT,
        client=_Client(),
        order_builder=builder,
    )

    balance = adapter.get_balance(MarketID("29076"))
    settlement = adapter.get_settlement(MarketID("29076"))
    prepared = adapter.prepare(_intent(OutcomeInventoryAction.SPLIT, "1.25"))
    submitted = adapter.submit(prepared)

    assert balance.yes == Quantity(Decimal("3"))
    assert balance.no == Quantity(Decimal("2"))
    assert balance.collateral.amount == Decimal("10")
    assert settlement.yes_payout.value == Decimal("1")
    assert builder.calls == [
        (
            "split",
            {
                "amount": 1_250_000_000_000_000_000,
                "condition_id": _CONDITION_ID,
                "is_neg_risk": True,
                "is_yield_bearing": True,
            },
        )
    ]
    assert submitted.status is InventorySubmissionStatus.ACCEPTED
    assert submitted.snapshot.status is InventoryOperationStatus.CONFIRMED
    assert submitted.snapshot.transaction_id == "0x" + "12" * 32


def test_rejects_insufficient_gas_without_stranding_pending_inventory() -> None:
    """Treat a pre-broadcast gas shortfall as a terminal rejection."""
    builder = _Builder()
    builder.split_positions = lambda **_: SimpleNamespace(
        success=False,
        cause={
            "code": -32000,
            "message": "insufficient funds for gas * price + value",
        },
    )
    adapter = PredictOutcomeInventoryAdapter(
        account_address=_ACCOUNT,
        client=_Client(),
        order_builder=builder,
    )

    submitted = adapter.submit(
        adapter.prepare(_intent(OutcomeInventoryAction.SPLIT, "1.25")),
    )

    assert submitted.status is InventorySubmissionStatus.REJECTED
    assert submitted.snapshot is None
    assert "insufficient funds for gas" in submitted.reason


def test_preserves_broadcast_hash_after_receipt_timeout() -> None:
    """Keep a timed-out broadcast reconcilable instead of stranding its journal."""
    builder = _Builder()
    tx_hash = "0x" + "ab" * 32
    builder.split_positions = lambda **_: SimpleNamespace(
        success=False,
        cause=TimeoutError(
            f"Transaction HexBytes('{tx_hash}') is not in the chain after 120 seconds",
        ),
    )
    adapter = PredictOutcomeInventoryAdapter(
        account_address=_ACCOUNT,
        client=_Client(),
        order_builder=builder,
    )

    submitted = adapter.submit(
        adapter.prepare(_intent(OutcomeInventoryAction.SPLIT, "1.25")),
    )

    assert submitted.status is InventorySubmissionStatus.UNKNOWN
    assert submitted.snapshot is None
    assert json.loads(submitted.reference.recovery_data)["transaction_hash"] == tx_hash


def test_rejects_split_for_resolved_market_before_signing() -> None:
    """Never submit inventory transactions for terminal Predict markets."""
    builder = _Builder()
    adapter = PredictOutcomeInventoryAdapter(
        account_address=_ACCOUNT,
        client=_Client(status="RESOLVED", trading_status="OPEN"),
        order_builder=builder,
    )

    with pytest.raises(ValueError, match="is not active"):
        adapter.prepare(_intent(OutcomeInventoryAction.SPLIT, "1.25"))

    assert builder.calls == []


def test_inventory_sdk_transactions_use_configured_gas_floor(monkeypatch) -> None:
    """Keep SDK-built inventory transactions above a low RPC suggestion."""
    monkeypatch.setenv("PREDICT_TRANSACTION_GAS_PRICE_GWEI", "0.2")
    builder = _Builder()
    captured = {}
    eth = SimpleNamespace(
        gas_price=50_000_000,
        set_gas_price_strategy=lambda strategy: captured.setdefault(
            "strategy",
            strategy,
        ),
    )
    builder._web3 = SimpleNamespace(eth=eth)

    PredictOutcomeInventoryAdapter(
        account_address=_ACCOUNT,
        client=_Client(),
        order_builder=builder,
    )

    assert captured["strategy"](builder._web3, {}) == 200_000_000


def test_setup_approvals_uses_inventory_scopes() -> None:
    builder = _Builder()
    adapter = PredictOutcomeInventoryAdapter(
        account_address=_ACCOUNT,
        client=_Client(),
        order_builder=builder,
    )

    report = adapter.setup_approvals(is_yield_bearing=True)

    assert report.success is True
    assert [call[0] for call in builder.calls] == ["approvals"]
    assert [scope.operation for scope in builder.calls[0][1]] == [
        "SPLIT",
        "SPLIT",
        "MERGE",
    ]
