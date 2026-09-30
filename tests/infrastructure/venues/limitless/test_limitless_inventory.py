"""Verify recoverable Limitless CTF inventory transactions."""

import hashlib
import json
from decimal import Decimal
from types import SimpleNamespace

import pytest
from web3.exceptions import TransactionNotFound

from prediction_markets.domain.outcome_inventory import (
    InventoryOperationID,
    InventoryOperationStatus,
    InventoryReconciliationStatus,
    InventorySubmissionStatus,
    OutcomeInventoryAction,
    OutcomeInventoryIntent,
)
from prediction_markets.domain.shared.value_objects import MarketID, Quantity
from prediction_markets.infrastructure.venues.limitless.inventory import (
    LimitlessOutcomeInventoryAdapter,
)
from prediction_markets.infrastructure.venues.limitless.mappers import LIMITLESS_VENUE_ID

_CONDITION_ID = "0x" + "ab" * 32
_ACCOUNT = "0x0000000000000000000000000000000000000001"
_USDC = "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913"
_ADAPTER = "0x0000000000000000000000000000000000000002"


class _MarketFetcher:
    """Return one standard Limitless CLOB market."""

    def __init__(self, *, neg_risk: bool = False) -> None:
        self.neg_risk = neg_risk

    async def get_market(self, slug):
        assert slug == "btc-up"
        return SimpleNamespace(
            condition_id=_CONDITION_ID,
            collateral_token=SimpleNamespace(address=_USDC, decimals=6, symbol="USDC"),
            position_ids=["111", "222"],
            neg_risk_request_id="request-1" if self.neg_risk else None,
            neg_risk_market_id=None,
            venue=SimpleNamespace(adapter=_ADAPTER) if self.neg_risk else None,
        )


class _PortfolioClient:
    """Return resolved and active account portfolio markets."""

    async def get(self, path):
        assert path == "/portfolio/positions"
        return {
            "clob": [
                {"market": {"slug": "resolved-market", "status": "RESOLVED"}},
                {"market": {"slug": "active-market", "status": "FUNDED"}},
            ],
        }


class _Function:
    """Model one contract read or transaction build."""

    def __init__(self, eth, contract, name, args) -> None:
        self.eth = eth
        self.contract = contract
        self.name = name
        self.args = args

    def call(self):
        if self.contract == "usdc" and self.name == "balanceOf":
            return self.eth.usdc_balance
        if self.contract == "usdc" and self.name == "allowance":
            return self.eth.allowance
        if self.contract == "ctf" and self.name == "balanceOf":
            return self.eth.positions[self.args[1]]
        if self.contract == "ctf" and self.name == "payoutDenominator":
            return self.eth.payout_denominator
        if self.contract == "ctf" and self.name == "payoutNumerators":
            return self.eth.payout_numerators[self.args[1]]
        if self.contract == "ctf" and self.name == "isApprovedForAll":
            return self.eth.neg_risk_approved
        raise AssertionError((self.contract, self.name))

    def build_transaction(self, values):
        self.eth.built.append((self.name, self.args, values))
        data = hashlib.sha256(repr((self.name, self.args)).encode()).hexdigest()
        return {
            **values,
            "to": _ACCOUNT,
            "gas": 100_000,
            "maxFeePerGas": 2,
            "maxPriorityFeePerGas": 1,
            "data": "0x" + data,
        }


class _Functions:
    """Create fake callable contract functions."""

    def __init__(self, eth, contract) -> None:
        self.eth = eth
        self.contract = contract

    def __getattr__(self, name):
        return lambda *args: _Function(self.eth, self.contract, name, args)


class _Contract:
    """Expose fake Web3 contract functions."""

    def __init__(self, eth, contract) -> None:
        self.functions = _Functions(eth, contract)


class _Eth:
    """Model Base reads, broadcasts, and receipts."""

    chain_id = 8453

    def __init__(self) -> None:
        self.usdc_balance = 10_000_000
        self.allowance = 10_000_000
        self.positions = {111: 3_000_000, 222: 2_000_000}
        self.payout_denominator = 1
        self.payout_numerators = (1, 0)
        self.neg_risk_approved = False
        self.built = []
        self.sent = set()
        self.latest_nonce = 7

    def contract(self, address, abi):
        contract = (
            "usdc"
            if address.lower() == _USDC.lower()
            else "adapter"
            if address.lower() == _ADAPTER.lower()
            else "ctf"
        )
        return _Contract(self, contract)

    def get_transaction_count(self, address, block):
        assert address == _ACCOUNT
        return 7 if block == "pending" else self.latest_nonce

    def send_raw_transaction(self, raw):
        transaction_hash = hashlib.sha256(raw).digest()
        self.sent.add("0x" + transaction_hash.hex())
        return transaction_hash

    def wait_for_transaction_receipt(self, transaction_hash, timeout):
        assert timeout == 120
        return {"status": 1}

    def get_transaction_receipt(self, transaction_hash):
        if transaction_hash not in self.sent:
            raise TransactionNotFound(transaction_hash)
        return {"status": 1}

    def get_transaction(self, transaction_hash):
        if transaction_hash not in self.sent:
            raise TransactionNotFound(transaction_hash)
        return {"hash": transaction_hash}


class _Web3:
    """Provide the Web3 surface used by the adapter."""

    def __init__(self) -> None:
        self.eth = _Eth()

    @staticmethod
    def to_checksum_address(address):
        if not address:
            raise ValueError("empty address")
        return address

    @staticmethod
    def keccak(raw):
        return hashlib.sha256(raw).digest()


class _Account:
    """Sign deterministic fake transactions."""

    address = _ACCOUNT

    def sign_transaction(self, transaction):
        raw = json.dumps(transaction, sort_keys=True).encode()
        return SimpleNamespace(
            raw_transaction=raw,
            hash=hashlib.sha256(raw).digest(),
        )


def _intent(
    action: OutcomeInventoryAction,
    quantity: str | None = None,
) -> OutcomeInventoryIntent:
    return OutcomeInventoryIntent(
        operation_id=InventoryOperationID(f"limitless-{action.value}"),
        venue_id=LIMITLESS_VENUE_ID,
        market_id=MarketID("btc-up"),
        action=action,
        quantity=Quantity(Decimal(quantity)) if quantity else None,
    )


def test_prepares_exact_ctf_calls_and_reconciles_the_signed_transaction() -> None:
    """Use documented binary index sets and preserve the Base transaction hash."""
    web3 = _Web3()
    adapter = LimitlessOutcomeInventoryAdapter(
        web3=web3,
        account=_Account(),
        market_fetcher=_MarketFetcher(),
    )

    balance = adapter.get_balance(MarketID("btc-up"))
    settlement = adapter.get_settlement(MarketID("btc-up"))
    split = adapter.prepare(_intent(OutcomeInventoryAction.SPLIT, "1.25"))
    merge = adapter.prepare(_intent(OutcomeInventoryAction.MERGE, "2"))
    redeem = adapter.prepare(_intent(OutcomeInventoryAction.REDEEM))

    assert balance.yes.value == Decimal("3")
    assert balance.no.value == Decimal("2")
    assert balance.collateral.amount == Decimal("10")
    assert settlement.yes_payout.value == Decimal("1")
    assert [call[0] for call in web3.eth.built] == [
        "splitPosition",
        "mergePositions",
        "redeemPositions",
    ]
    assert web3.eth.built[0][1][3:] == ([1, 2], 1_250_000)
    assert web3.eth.built[1][1][3:] == ([1, 2], 2_000_000)
    assert web3.eth.built[2][1][3] == [1, 2]
    assert [call[2]["nonce"] for call in web3.eth.built] == [7, 8, 9]
    assert json.loads(split.reference.recovery_data)["nonce"] == 7

    assert adapter.reconcile(split.reference).status is InventoryReconciliationStatus.NOT_FOUND
    submitted = adapter.submit(split)
    reconciled = adapter.reconcile(split.reference)

    assert submitted.status is InventorySubmissionStatus.ACCEPTED
    assert reconciled.status is InventoryReconciliationStatus.FOUND
    assert reconciled.snapshot.status is InventoryOperationStatus.CONFIRMED
    assert submitted.snapshot.transaction_id == reconciled.snapshot.transaction_id


def test_reconcile_fails_a_missing_transaction_after_its_nonce_is_consumed() -> None:
    """Treat a replaced Base transaction as terminal without resubmitting it."""
    web3 = _Web3()
    adapter = LimitlessOutcomeInventoryAdapter(
        web3=web3,
        account=_Account(),
        market_fetcher=_MarketFetcher(),
    )
    operation = adapter.prepare(_intent(OutcomeInventoryAction.SPLIT, "1"))
    web3.eth.latest_nonce = 8

    reconciled = adapter.reconcile(operation.reference)

    assert reconciled.status is InventoryReconciliationStatus.FOUND
    assert reconciled.snapshot.status is InventoryOperationStatus.FAILED
    assert "nonce was consumed" in reconciled.snapshot.reason


def test_rejects_unsettled_redemption() -> None:
    """Fail before signing when the payout vector is unavailable."""
    web3 = _Web3()
    web3.eth.payout_denominator = 0
    adapter = LimitlessOutcomeInventoryAdapter(
        web3=web3,
        account=_Account(),
        market_fetcher=_MarketFetcher(),
    )
    with pytest.raises(ValueError, match="not settled on-chain"):
        adapter.prepare(_intent(OutcomeInventoryAction.REDEEM))


def test_lists_only_resolved_portfolio_markets_for_automatic_redemption() -> None:
    """Use the account portfolio to find markets absent from live discovery."""
    adapter = LimitlessOutcomeInventoryAdapter(
        web3=_Web3(),
        account=_Account(),
        http_client=_PortfolioClient(),
        market_fetcher=_MarketFetcher(),
    )

    assert adapter.list_redeemable_markets() == (MarketID("resolved-market"),)

def test_prepares_negrisk_adapter_calls() -> None:
    """Use the market-specific adapter for NegRisk inventory operations."""
    web3 = _Web3()
    web3.eth.neg_risk_approved = True
    adapter = LimitlessOutcomeInventoryAdapter(
        web3=web3,
        account=_Account(),
        market_fetcher=_MarketFetcher(neg_risk=True),
    )

    adapter.prepare(_intent(OutcomeInventoryAction.SPLIT, "1.25"))
    adapter.prepare(_intent(OutcomeInventoryAction.MERGE, "2"))
    adapter.prepare(_intent(OutcomeInventoryAction.REDEEM))

    assert [call[0] for call in web3.eth.built] == [
        "splitPosition",
        "mergePositions",
        "redeemPositions",
    ]
    assert web3.eth.built[0][1] == (
        bytes.fromhex(_CONDITION_ID[2:]),
        1_250_000,
    )


def test_discarded_preparation_reuses_nonce_without_colliding_with_peers() -> None:
    """Fill a never-broadcast nonce gap while preserving other reservations."""
    web3 = _Web3()
    adapter = LimitlessOutcomeInventoryAdapter(
        web3=web3, account=_Account(), market_fetcher=_MarketFetcher(),
    )
    abandoned = adapter.prepare(_intent(OutcomeInventoryAction.SPLIT, "1"))
    adapter.prepare(_intent(OutcomeInventoryAction.MERGE, "1"))
    adapter.discard_prepared(abandoned)
    adapter.prepare(_intent(OutcomeInventoryAction.SPLIT, "2"))
    adapter.prepare(_intent(OutcomeInventoryAction.SPLIT, "3"))
    assert [call[2]["nonce"] for call in web3.eth.built] == [7, 8, 7, 9]


@pytest.mark.parametrize("uncertain", [False, True])
def test_only_definitive_rejection_releases_broadcast_nonce(monkeypatch, uncertain) -> None:
    """Keep an uncertain transaction's nonce reserved until reconciliation."""
    web3 = _Web3()
    adapter = LimitlessOutcomeInventoryAdapter(
        web3=web3, account=_Account(), market_fetcher=_MarketFetcher(),
    )
    operation = adapter.prepare(_intent(OutcomeInventoryAction.SPLIT, "1"))

    def reject(raw):
        if uncertain:
            raise ConnectionError("disconnected")
        raise ValueError("insufficient funds")

    monkeypatch.setattr(web3.eth, "send_raw_transaction", reject)
    submitted = adapter.submit(operation)
    adapter.prepare(_intent(OutcomeInventoryAction.SPLIT, "2"))
    assert submitted.status is (
        InventorySubmissionStatus.UNKNOWN if uncertain else InventorySubmissionStatus.REJECTED
    )
    assert web3.eth.built[-1][2]["nonce"] == (8 if uncertain else 7)
