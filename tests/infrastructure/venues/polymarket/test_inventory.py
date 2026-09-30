"""Verify recoverable Polymarket outcome-inventory operations."""

import json
from decimal import Decimal
from types import SimpleNamespace

import httpx
import pytest
from polymarket.errors import UserInputError

from prediction_markets.domain.outcome_inventory import (
    InventoryOperationID,
    InventoryOperationStatus,
    InventoryReconciliationStatus,
    InventorySubmissionStatus,
    OutcomeInventoryAction,
    OutcomeInventoryIntent,
)
from prediction_markets.domain.shared.value_objects import MarketID, Quantity
from prediction_markets.infrastructure.venues.polymarket.inventory import (
    PolymarketOutcomeInventoryAdapter,
)
from prediction_markets.infrastructure.venues.polymarket.mappers import POLYMARKET_VENUE_ID

_CONDITION_ID = "0x" + "ab" * 32


class _Paginator:
    def __init__(self, items):
        self._items = items

    def first_page(self):
        return SimpleNamespace(items=self._items)


class _Client:
    def __init__(self):
        self.calls = []
        self.error = None
        self.approvals = 0
        self.market = SimpleNamespace(
            condition_id=_CONDITION_ID,
            outcomes=SimpleNamespace(
                yes=SimpleNamespace(token_id="yes-token", winner=True),
                no=SimpleNamespace(token_id="no-token", winner=False),
            ),
        )

    def list_markets(self, **kwargs):
        assert kwargs == {"condition_ids": _CONDITION_ID, "page_size": 1}
        return _Paginator((self.market,))

    def get_balance_allowance(self, *, asset_type, token_id=None):
        balances = {
            ("COLLATERAL", None): 12_500_000,
            ("CONDITIONAL", "yes-token"): 3_250_000,
            ("CONDITIONAL", "no-token"): 2_000_000,
        }
        return SimpleNamespace(balance=balances[(asset_type, token_id)])

    def setup_trading_approvals(self):
        self.approvals += 1

    def split_position(self, **kwargs):
        return self._submit("split", kwargs)

    def merge_positions(self, **kwargs):
        return self._submit("merge", kwargs)

    def redeem_positions(self, **kwargs):
        return self._submit("redeem", kwargs)

    def _submit(self, action, kwargs):
        if self.error:
            raise self.error
        self.calls.append((action, kwargs))
        return SimpleNamespace(transaction_id="relay-1", transaction_hash=None)


class _RelayerHttp:
    def __init__(self, responses=None):
        self.responses = responses or {}
        self.paths = []

    def get(self, path):
        self.paths.append(path)
        status, payload = self.responses.get(path, (404, {"error": "not found"}))
        return httpx.Response(
            status,
            json=payload,
            request=httpx.Request("GET", f"https://relayer.test{path}"),
        )


def _intent(action, quantity=Decimal("1.5")):
    return OutcomeInventoryIntent(
        operation_id=InventoryOperationID(f"operation-{action.value}"),
        venue_id=POLYMARKET_VENUE_ID,
        market_id=MarketID(_CONDITION_ID),
        action=action,
        quantity=None if action is OutcomeInventoryAction.REDEEM else Quantity(quantity),
    )


def test_reads_exact_binary_and_collateral_balances():
    adapter = PolymarketOutcomeInventoryAdapter(
        client=_Client(),
        relayer_http=_RelayerHttp(),
    )

    balance = adapter.get_balance(MarketID(_CONDITION_ID))

    assert balance.collateral.amount == Decimal("12.5")
    assert balance.yes == Quantity(Decimal("3.25"))
    assert balance.no == Quantity(Decimal("2"))
    assert balance.mergeable_quantity == Quantity(Decimal("2"))
    assert adapter.get_settlement(MarketID(_CONDITION_ID)).yes_payout.value == 1


def test_reads_balances_after_market_closes():
    class _ClosedClient(_Client):
        def list_markets(self, **kwargs):
            if kwargs.get("closed") is True:
                return _Paginator((self.market,))
            return _Paginator(())

    balance = PolymarketOutcomeInventoryAdapter(
        client=_ClosedClient(),
        relayer_http=_RelayerHttp(),
    ).get_balance(MarketID(_CONDITION_ID))

    assert balance.mergeable_quantity == Quantity(Decimal("2"))


@pytest.mark.parametrize(
    ("action", "expected"),
    [
        (OutcomeInventoryAction.SPLIT, ("split", 1_500_000)),
        (OutcomeInventoryAction.MERGE, ("merge", 1_500_000)),
        (OutcomeInventoryAction.REDEEM, ("redeem", None)),
    ],
)
def test_submits_position_lifecycle_operations_with_durable_relayer_id(action, expected):
    client = _Client()
    adapter = PolymarketOutcomeInventoryAdapter(
        client=client,
        relayer_http=_RelayerHttp(),
    )

    prepared = adapter.prepare(_intent(action))
    submitted = adapter.submit(prepared)

    call_action, call = client.calls[0]
    assert call_action == expected[0]
    assert call.get("amount") == expected[1]
    assert call["condition_id"] == _CONDITION_ID
    assert submitted.status is InventorySubmissionStatus.ACCEPTED
    assert submitted.snapshot.status is InventoryOperationStatus.PENDING
    assert submitted.snapshot.transaction_id == "relay-1"
    assert b'"transaction_id":"relay-1"' in submitted.reference.recovery_data


def test_reconciles_confirmation_by_relayer_transaction_id():
    relayer = _RelayerHttp(
        {
            "/v1/account/transactions/relay-1": (
                200,
                {
                    "transaction_id": "relay-1",
                    "transaction_hash": "0x123",
                    "state": "STATE_CONFIRMED",
                    "error_msg": None,
                    "updated_at": "2026-08-06T12:00:00Z",
                },
            ),
        },
    )
    adapter = PolymarketOutcomeInventoryAdapter(client=_Client(), relayer_http=relayer)
    submitted = adapter.submit(adapter.prepare(_intent(OutcomeInventoryAction.SPLIT)))

    reconciled = adapter.reconcile(submitted.reference)

    assert reconciled.status is InventoryReconciliationStatus.FOUND
    assert reconciled.snapshot.status is InventoryOperationStatus.CONFIRMED
    assert relayer.paths == ["/v1/account/transactions/relay-1"]


def test_reconciles_unix_millisecond_relayer_timestamp():
    relayer = _RelayerHttp(
        {
            "/v1/account/transactions/relay-1": (
                200,
                {
                    "transactionID": "relay-1",
                    "state": "STATE_CONFIRMED",
                    "updatedAt": 1786443604157,
                },
            ),
        },
    )
    adapter = PolymarketOutcomeInventoryAdapter(client=_Client(), relayer_http=relayer)
    submitted = adapter.submit(adapter.prepare(_intent(OutcomeInventoryAction.SPLIT)))

    reconciled = adapter.reconcile(submitted.reference)

    assert reconciled.status is InventoryReconciliationStatus.FOUND
    assert reconciled.snapshot.status is InventoryOperationStatus.CONFIRMED
    assert reconciled.snapshot.updated_at.to_unix_ms() == 1786443604157


def test_recovers_lost_submission_response_by_unique_metadata():
    client = _Client()
    prepared = PolymarketOutcomeInventoryAdapter(
        client=client,
        relayer_http=_RelayerHttp(),
    ).prepare(_intent(OutcomeInventoryAction.SPLIT))
    metadata = json.loads(prepared.reference.recovery_data)["metadata"]
    relayer = _RelayerHttp(
        {
            "/transactions": (
                200,
                [
                    {
                        "transactionID": "relay-lost-response",
                        "state": "STATE_MINED",
                        "metadata": metadata,
                    },
                ],
            ),
        },
    )
    adapter = PolymarketOutcomeInventoryAdapter(client=client, relayer_http=relayer)

    reconciled = adapter.reconcile(prepared.reference)

    assert reconciled.status is InventoryReconciliationStatus.FOUND
    assert reconciled.snapshot.status is InventoryOperationStatus.PENDING
    assert reconciled.snapshot.transaction_id == "relay-lost-response"


def test_maps_pre_submission_validation_errors_to_rejected():
    client = _Client()
    client.error = UserInputError("insufficient pUSD allowance")
    adapter = PolymarketOutcomeInventoryAdapter(
        client=client,
        relayer_http=_RelayerHttp(),
    )

    submitted = adapter.submit(adapter.prepare(_intent(OutcomeInventoryAction.SPLIT)))

    assert submitted.status is InventorySubmissionStatus.REJECTED
    assert submitted.reason == "insufficient pUSD allowance"
