"""Manage recoverable Predict.fun outcome inventory on BNB Chain.

Responsibilities
----------------
- Read USDT and binary outcome balances for one Predict market.
- Prepare, submit, and reconcile split, merge, and redeem transactions.
- Select the correct Conditional Tokens or NegRisk route from market metadata.

Notes
-----
- Predict amounts use 18 decimal base units.
- The Predict SDK signs and broadcasts inventory transactions during submission;
  prepared requests therefore persist the exact SDK arguments and recovery data.
"""

import json
import re
from dataclasses import replace
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

import httpx
from predict_sdk import ApprovalScope, ChainId, OrderBuilder, OrderBuilderOptions
from web3.exceptions import TransactionNotFound, Web3Exception

from prediction_markets.domain.outcome_inventory import (
    InventoryOperationReference,
    InventoryOperationSnapshot,
    InventoryOperationStatus,
    InventoryReconciliationResult,
    InventoryReconciliationStatus,
    InventorySubmissionResult,
    InventorySubmissionStatus,
    OutcomeInventoryAction,
    OutcomeInventoryBalance,
    OutcomeInventoryIntent,
    OutcomeInventorySettlement,
    PreparedInventoryOperation,
)
from prediction_markets.domain.ports.outcome_inventory import OutcomeInventoryPort
from prediction_markets.domain.shared.value_objects import (
    Currency,
    MarketID,
    Money,
    Price,
    Quantity,
    Timestamp,
)
from prediction_markets.infrastructure.venues.predict.config import (
    TRANSACTION_LOCK,
    predict_account_address,
    predict_api_key,
    predict_headers,
    predict_privy_private_key,
    predict_transaction_gas_price_wei,
)
from prediction_markets.infrastructure.venues.predict.mappers import (
    PREDICT_VENUE_ID,
    predict_contract_id,
    predict_market_is_active,
)

_PRECISION = Decimal("1000000000000000000")
_ROOT_COLLECTION = bytes(32)
_BINARY_PARTITION = (1, 2)
_TESTNET_HOST = "api-testnet.predict.fun"


class PredictOutcomeInventoryAdapter(OutcomeInventoryPort):
    """Manage Predict.fun binary inventory for one EOA or Predict Account.

    Attributes
    ----------
    _order_builder : OrderBuilder
        Official SDK builder used for reads and signed transactions.
    _account_address : str
        Address holding the USDT and outcome tokens.

    Notes
    -----
    - ``isNegRisk`` selects the Conditional Tokens versus NegRisk Adapter route.
    - ``isYieldBearing`` selects the standard versus Venus-backed contract set.
    - ``setup_approvals`` is explicit and never runs as a read-side effect.
    """

    def __init__(
        self,
        privy_private_key: str | None = None,
        *,
        account_address: str | None = None,
        api_key: str | None = None,
        base_url: str = "https://api.predict.fun",
        timeout_seconds: float = 10.0,
        client: httpx.Client | None = None,
        order_builder: Any | None = None,
        web3: Any | None = None,
    ) -> None:
        """
        Parameters
        ----------
        privy_private_key
            Private key used by the Predict SDK. Defaults to
            ``PREDICT_PRIVY_PRIVATE_KEY``.
        account_address
            Address holding the Predict Account inventory. Defaults to
            ``PREDICT_ACCOUNT_ADDRESS``.
        api_key
            Predict mainnet API key. Testnet does not require one.
        base_url
            Predict API origin used for market metadata.
        timeout_seconds
            Positive HTTP timeout in seconds.
        client
            Optional caller-owned synchronous HTTP client.
        order_builder
            Optional SDK-compatible builder for tests or custom providers.
        web3
            Optional Web3-compatible client used for transaction reconciliation.

        Raises
        ------
        ValueError
            If credentials or required API settings are missing.
        """
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")

        self._base_url = base_url.rstrip("/")
        self._api_key = predict_api_key(api_key)
        self._account_address = predict_account_address(account_address)
        key = predict_privy_private_key(privy_private_key)
        testnet = _TESTNET_HOST in self._base_url
        if order_builder is None:
            if not key or not self._account_address:
                raise ValueError(
                    "Set PREDICT_PRIVY_PRIVATE_KEY and PREDICT_ACCOUNT_ADDRESS "
                    "to use Predict inventory"
                )
            if not self._api_key and not testnet:
                raise ValueError("Predict mainnet inventory requires PREDICT_API_KEY")
            order_builder = OrderBuilder.make(
                ChainId.BNB_TESTNET if testnet else ChainId.BNB_MAINNET,
                key,
                OrderBuilderOptions(predict_account=self._account_address),
            )
        if not self._account_address:
            self._account_address = getattr(order_builder, "_predict_account", None)
        if not self._account_address:
            signer = getattr(order_builder, "_signer", None)
            self._account_address = getattr(signer, "address", None)
        if not self._account_address:
            raise ValueError("Predict inventory requires an account address")

        self._order_builder = order_builder
        self._web3 = web3 or getattr(order_builder, "_web3", None)
        self._configure_gas_price()
        self._owns_client = client is None
        self._client = client or httpx.Client(timeout=timeout_seconds)
        self._markets: dict[str, dict[str, Any]] = {}

    def _configure_gas_price(self) -> None:
        """Apply the Predict transaction gas floor to SDK-built transactions.

        Notes
        -----
        - The official SDK otherwise accepts the RPC's minimum suggestion,
          which can remain pending even when validators prefer a higher tip.
        - The strategy is scoped to this inventory adapter's Web3 instance.
        """
        eth = getattr(self._web3, "eth", None)
        configure = getattr(eth, "set_gas_price_strategy", None)
        if not callable(configure):
            return

        def strategy(web3: Any, _transaction: dict[str, Any] | None = None) -> int:
            return predict_transaction_gas_price_wei(int(web3.eth.gas_price))

        configure(strategy)

    def setup_approvals(self, *, is_yield_bearing: bool | None = None) -> Any:
        """Set the official Predict approvals required by inventory operations.

        Parameters
        ----------
        is_yield_bearing
            Limit approvals to one contract track, or configure both tracks when
            omitted.

        Returns
        -------
        Any
            SDK approval report.
        """
        tracks = [False, True] if is_yield_bearing is None else [is_yield_bearing]
        steps = [
            step
            for track in tracks
            for scope in (
                ApprovalScope("SPLIT", False, track),
                ApprovalScope("SPLIT", True, track),
                ApprovalScope("MERGE", True, track),
            )
            for step in self._order_builder.get_approval_steps(scope)
        ]
        with TRANSACTION_LOCK:
            return self._order_builder.run_approvals(steps)

    def get_balance(self, market_id: MarketID) -> OutcomeInventoryBalance:
        """Read USDT, YES, and NO balances for one Predict market.

        Parameters
        ----------
        market_id
            Predict numeric market identifier.

        Returns
        -------
        OutcomeInventoryBalance
            Current 18-decimal balances for the account and market.
        """
        market = self._market_data(market_id)
        token_contract = self._conditional_tokens(market)
        yes = token_contract.functions.balanceOf(
            self._account_address, market["yes_token"]
        ).call()
        no = token_contract.functions.balanceOf(
            self._account_address, market["no_token"]
        ).call()
        collateral = self._order_builder.balance_of(address=self._account_address)
        return OutcomeInventoryBalance(
            venue_id=PREDICT_VENUE_ID,
            market_id=market_id,
            yes=Quantity(_from_base_units(yes)),
            no=Quantity(_from_base_units(no)),
            collateral=Money(_from_base_units(collateral), Currency("USDT")),
            observed_at=Timestamp.now(),
            yes_contract_id=predict_contract_id(str(market_id), "yes"),
            no_contract_id=predict_contract_id(str(market_id), "no"),
        )

    def get_settlement(self, market_id: MarketID) -> OutcomeInventorySettlement:
        """Read the final Conditional Tokens payout vector for a Predict market."""
        market = self._market_data(market_id)
        contract = self._conditional_tokens(market)
        condition_id = market["condition_id"]
        denominator = int(contract.functions.payoutDenominator(condition_id).call())
        if denominator <= 0:
            raise ValueError("Predict payout is not settled on-chain yet")
        yes = Decimal(
            contract.functions.payoutNumerators(condition_id, 0).call(),
        ) / denominator
        no = Decimal(
            contract.functions.payoutNumerators(condition_id, 1).call(),
        ) / denominator
        return OutcomeInventorySettlement(
            venue_id=PREDICT_VENUE_ID,
            market_id=market_id,
            yes_contract_id=predict_contract_id(str(market_id), "yes"),
            no_contract_id=predict_contract_id(str(market_id), "no"),
            yes_payout=Price(yes),
            no_payout=Price(no),
            observed_at=Timestamp.now(),
        )

    def prepare(self, intent: OutcomeInventoryIntent) -> PreparedInventoryOperation:
        """Serialize one exact Predict inventory invocation.

        Parameters
        ----------
        intent
            Split, merge, or redeem request for this venue.

        Returns
        -------
        PreparedInventoryOperation
            Persisted SDK arguments and recovery reference.
        """
        _require_venue(intent.venue_id)
        market = self._market_data(intent.market_id)
        if (
            intent.action is OutcomeInventoryAction.SPLIT
            and not market["is_active"]
        ):
            raise ValueError(
                f"Predict market {intent.market_id} is not active; split rejected",
            )
        balance_before = self.get_balance(intent.market_id)
        amount = _to_base_units(intent.quantity.value) if intent.quantity else None
        data = {
            "schema": 1,
            "operation_id": str(intent.operation_id),
            "action": intent.action.value,
            "market_id": str(intent.market_id),
            "condition_id": market["condition_id"],
            "amount": amount,
            "yes_token": market["yes_token"],
            "no_token": market["no_token"],
            "is_neg_risk": market["is_neg_risk"],
            "is_yield_bearing": market["is_yield_bearing"],
        }
        request = json.dumps(data, separators=(",", ":"), sort_keys=True).encode()
        reference = InventoryOperationReference(
            venue_id=PREDICT_VENUE_ID,
            operation_id=intent.operation_id,
            recovery_data=json.dumps(
                {"schema": 1, "transaction_hash": None},
                separators=(",", ":"),
                sort_keys=True,
            ).encode(),
            quantity=intent.quantity,
            action=intent.action,
            portfolio_id=intent.portfolio_id,
            balance_before=balance_before,
        )
        return PreparedInventoryOperation(intent, reference, request)

    def submit(self, operation: PreparedInventoryOperation) -> InventorySubmissionResult:
        """Submit the persisted Predict SDK invocation without rebuilding it.

        Parameters
        ----------
        operation
            Exact operation returned by :meth:`prepare`.

        Returns
        -------
        InventorySubmissionResult
            Confirmed, rejected, or uncertain transaction state.
        """
        _require_venue(operation.reference.venue_id)
        data = _request(operation)
        try:
            result = self._submit_request(data)
        except (ValueError, TypeError, RuntimeError) as error:
            return InventorySubmissionResult(
                InventorySubmissionStatus.REJECTED,
                operation.reference,
                reason=str(error),
            )
        receipt = getattr(result, "receipt", None)
        receipt_hash = _receipt_hash(receipt)
        cause = getattr(result, "cause", None)
        transaction_hash = receipt_hash or _cause_transaction_hash(cause)
        if getattr(result, "success", False):
            if not transaction_hash:
                return InventorySubmissionResult(
                    InventorySubmissionStatus.UNKNOWN,
                    operation.reference,
                    reason="Predict SDK returned no transaction hash",
                )
            reference = _reference(operation, transaction_hash)
            snapshot = InventoryOperationSnapshot(
                reference=reference,
                status=InventoryOperationStatus.CONFIRMED,
                updated_at=Timestamp.now(),
                transaction_id=transaction_hash,
                quantity=reference.quantity,
                fee=_gas_fee(receipt),
                fee_observed_at=_receipt_timestamp(
                    self._web3,
                    receipt,
                ),
            )
            try:
                before = reference.balance_before
                if before is None:
                    raise ValueError("Missing pre-operation balance")
                snapshot = snapshot.with_balance_change(
                    self.get_balance(before.market_id),
                )
            except Exception:
                snapshot = replace(
                    snapshot,
                    quality_flags=(*snapshot.quality_flags, "MISSING_INVENTORY_ECONOMICS"),
                )
            return InventorySubmissionResult(
                InventorySubmissionStatus.ACCEPTED,
                reference,
                snapshot,
            )
        if receipt_hash:
            reference = _reference(operation, transaction_hash)
            return InventorySubmissionResult(
                InventorySubmissionStatus.REJECTED,
                reference,
                InventoryOperationSnapshot(
                    reference=reference,
                    status=InventoryOperationStatus.FAILED,
                    updated_at=Timestamp.now(),
                    transaction_id=transaction_hash,
                    quantity=reference.quantity,
                    reason="Predict inventory transaction reverted",
                ),
            )
        if transaction_hash:
            return InventorySubmissionResult(
                InventorySubmissionStatus.UNKNOWN,
                _reference(operation, transaction_hash),
                reason=str(cause),
            )
        return InventorySubmissionResult(
            (
                InventorySubmissionStatus.REJECTED
                if "insufficient funds for gas" in str(cause).lower()
                else InventorySubmissionStatus.UNKNOWN
            ),
            operation.reference,
            reason=str(cause) if cause else "Predict transaction outcome is unknown",
        )

    def reconcile(
        self,
        reference: InventoryOperationReference,
    ) -> InventoryReconciliationResult:
        """Resolve a persisted Predict transaction from its BNB transaction hash.

        Parameters
        ----------
        reference
            Durable reference containing a transaction hash when submission
            returned one.

        Returns
        -------
        InventoryReconciliationResult
            Mined state, pending state, absence, or RPC uncertainty.
        """
        _require_venue(reference.venue_id)
        data = _recovery_data(reference)
        transaction_hash = data.get("transaction_hash")
        if not transaction_hash or self._web3 is None:
            return InventoryReconciliationResult(
                InventoryReconciliationStatus.UNKNOWN,
                reference,
            )
        try:
            receipt = self._web3.eth.get_transaction_receipt(transaction_hash)
        except TransactionNotFound:
            try:
                self._web3.eth.get_transaction(transaction_hash)
            except TransactionNotFound:
                return InventoryReconciliationResult(
                    InventoryReconciliationStatus.NOT_FOUND,
                    reference,
                )
            except (Web3Exception, OSError, TimeoutError, ValueError):
                return InventoryReconciliationResult(
                    InventoryReconciliationStatus.UNKNOWN,
                    reference,
                )
            status = InventoryOperationStatus.PENDING
        except (Web3Exception, OSError, TimeoutError, ValueError):
            return InventoryReconciliationResult(
                InventoryReconciliationStatus.UNKNOWN,
                reference,
            )
        else:
            receipt_status = _value(receipt, "status")
            if receipt_status is None:
                return InventoryReconciliationResult(
                    InventoryReconciliationStatus.UNKNOWN,
                    reference,
                )
            status = (
                InventoryOperationStatus.CONFIRMED
                if int(receipt_status) == 1
                else InventoryOperationStatus.FAILED
            )
        snapshot = InventoryOperationSnapshot(
            reference=reference,
            status=status,
            updated_at=Timestamp.now(),
            transaction_id=transaction_hash,
            quantity=reference.quantity,
            reason=(
                "Predict inventory transaction reverted"
                if status is InventoryOperationStatus.FAILED
                else None
            ),
        )
        if status is InventoryOperationStatus.CONFIRMED:
            fee = _gas_fee(receipt)
            if fee is not None:
                snapshot = replace(
                    snapshot,
                    fee=fee,
                    fee_observed_at=_receipt_timestamp(self._web3, receipt),
                )
            try:
                before = reference.balance_before
                if before is None:
                    raise ValueError("Missing pre-operation balance")
                snapshot = snapshot.with_balance_change(
                    self.get_balance(before.market_id),
                )
            except Exception:
                snapshot = replace(
                    snapshot,
                    quality_flags=(*snapshot.quality_flags, "MISSING_INVENTORY_ECONOMICS"),
                )
        return InventoryReconciliationResult(
            InventoryReconciliationStatus.FOUND,
            reference,
            snapshot,
        )

    def close(self) -> None:
        """Close the HTTP client owned by this adapter."""
        if self._owns_client:
            self._client.close()
            self._owns_client = False

    def _market_data(self, market_id: MarketID) -> dict[str, Any]:
        """Fetch and validate the market metadata required by CTF calls."""
        key = str(market_id)
        if key not in self._markets:
            response = self._client.get(
                f"{self._base_url}/v1/markets/{key}",
                headers=predict_headers(self._api_key),
            )
            response.raise_for_status()
            payload = response.json()
            if not isinstance(payload, dict) or payload.get("success") is not True:
                raise TypeError("Unexpected Predict market response")
            market = payload.get("data")
            if not isinstance(market, dict):
                raise TypeError("Unexpected Predict market data")
            self._markets[key] = market
        market = self._markets[key]
        outcomes = {
            item.get("indexSet"): item
            for item in market.get("outcomes") or ()
            if isinstance(item, dict)
        }
        yes = outcomes.get(1)
        no = outcomes.get(2)
        if not isinstance(yes, dict) or not isinstance(no, dict):
            raise ValueError(f"Predict market {key} has no binary outcomes")
        condition_id = _bytes32(market.get("conditionId") or market.get("condition_id"))
        return {
            "condition_id": condition_id,
            "yes_token": int(yes["onChainId"]),
            "no_token": int(no["onChainId"]),
            "is_neg_risk": _as_bool(market.get("isNegRisk")),
            "is_yield_bearing": _as_bool(market.get("isYieldBearing")),
            "is_active": predict_market_is_active(market),
        }

    def _conditional_tokens(self, market: dict[str, Any]) -> Any:
        """Select the ERC-1155 contract matching market metadata."""
        contracts = self._order_builder.contracts
        if contracts is None:
            raise RuntimeError("Predict SDK has no initialized contracts")
        if market["is_yield_bearing"]:
            return (
                contracts.yield_bearing_neg_risk_conditional_tokens
                if market["is_neg_risk"]
                else contracts.yield_bearing_conditional_tokens
            )
        return (
            contracts.neg_risk_conditional_tokens
            if market["is_neg_risk"]
            else contracts.conditional_tokens
        )

    def _submit_request(self, data: dict[str, Any]) -> Any:
        """Dispatch one persisted operation through the official SDK."""
        with TRANSACTION_LOCK:
            return self._submit_request_locked(data)

    def _submit_request_locked(self, data: dict[str, Any]) -> Any:
        """Submit while excluding concurrent cancellation nonce allocation."""
        common = {
            "condition_id": data["condition_id"],
            "is_neg_risk": data["is_neg_risk"],
            "is_yield_bearing": data["is_yield_bearing"],
        }
        action = data["action"]
        if action == OutcomeInventoryAction.SPLIT.value:
            return self._order_builder.split_positions(amount=data["amount"], **common)
        if action == OutcomeInventoryAction.MERGE.value:
            return self._order_builder.merge_positions(amount=data["amount"], **common)
        if action == OutcomeInventoryAction.REDEEM.value:
            condition_id = data["condition_id"]
            is_neg_risk = data["is_neg_risk"]
            is_yield_bearing = data["is_yield_bearing"]
            if is_neg_risk:
                amount = self._redeem_amount(data)
                return self._order_builder.redeem_positions(
                    condition_id,
                    1,
                    amount=amount,
                    is_neg_risk=is_neg_risk,
                    is_yield_bearing=is_yield_bearing,
                )
            result = self._order_builder.redeem_positions(
                condition_id,
                1,
                is_neg_risk=is_neg_risk,
                is_yield_bearing=is_yield_bearing,
            )
            if getattr(result, "success", False):
                result = self._order_builder.redeem_positions(
                    condition_id,
                    2,
                    is_neg_risk=is_neg_risk,
                    is_yield_bearing=is_yield_bearing,
                )
            return result
        raise ValueError(f"Unsupported Predict inventory action: {action}")

    def _redeem_amount(self, data: dict[str, Any]) -> int:
        """Return the largest locally available NegRisk outcome balance."""
        market = self._conditional_tokens(data)
        yes = market.functions.balanceOf(self._account_address, data["yes_token"]).call()
        no = market.functions.balanceOf(self._account_address, data["no_token"]).call()
        amount = max(int(yes), int(no))
        if amount <= 0:
            raise ValueError("No Predict outcome inventory is available to redeem")
        return amount


def _to_base_units(value: Decimal) -> int:
    raw = value * _PRECISION
    if raw != raw.to_integral_value():
        raise ValueError("Predict quantities support at most 18 decimal places")
    return int(raw)


def _from_base_units(value: Any) -> Decimal:
    return Decimal(str(value)) / _PRECISION


def _bytes32(value: Any) -> str:
    text = str(value or "")
    if not text.startswith("0x") or len(text) != 66:
        raise ValueError("Predict market has no valid 32-byte condition id")
    try:
        bytes.fromhex(text[2:])
    except ValueError as error:
        raise ValueError("Predict condition id must be hexadecimal") from error
    return text


def _receipt_hash(receipt: Any) -> str | None:
    value = _value(receipt, "transactionHash", "transaction_hash")
    if value is None:
        return None
    if isinstance(value, (bytes, bytearray)):
        return "0x" + bytes(value).hex()
    text = str(value)
    return text if text.startswith("0x") else "0x" + text


def _cause_transaction_hash(cause: Any) -> str | None:
    """Recover a broadcast transaction hash from an SDK receipt timeout."""
    match = re.search(r"0x[0-9a-fA-F]{64}", str(cause))
    return match.group(0) if match is not None else None


def _reference(
    operation: PreparedInventoryOperation,
    transaction_hash: str,
) -> InventoryOperationReference:
    return InventoryOperationReference(
        venue_id=PREDICT_VENUE_ID,
        operation_id=operation.intent.operation_id,
        recovery_data=json.dumps(
            {"schema": 1, "transaction_hash": transaction_hash},
            separators=(",", ":"),
            sort_keys=True,
        ).encode(),
        quantity=operation.intent.quantity,
        action=operation.intent.action,
        portfolio_id=operation.intent.portfolio_id,
        balance_before=operation.reference.balance_before,
    )


def _request(operation: PreparedInventoryOperation) -> dict[str, Any]:
    try:
        data = json.loads(operation.request)
    except (json.JSONDecodeError, UnicodeDecodeError) as error:
        raise ValueError("Invalid persisted Predict inventory request") from error
    if (
        data.get("schema") != 1
        or data.get("operation_id") != str(operation.intent.operation_id)
        or data.get("market_id") != str(operation.intent.market_id)
        or data.get("action") != operation.intent.action.value
    ):
        raise ValueError("Predict inventory request identity does not match")
    return data


def _recovery_data(reference: InventoryOperationReference) -> dict[str, Any]:
    try:
        data = json.loads(reference.recovery_data)
    except (json.JSONDecodeError, UnicodeDecodeError) as error:
        raise ValueError("Invalid Predict inventory recovery data") from error
    if data.get("schema") != 1:
        raise ValueError("Unsupported Predict inventory recovery schema")
    return data


def _value(value: Any, *names: str) -> Any:
    for name in names:
        if isinstance(value, dict) and name in value:
            return value[name]
        if hasattr(value, name):
            return getattr(value, name)
    return None


def _gas_fee(receipt: Any) -> Money | None:
    """Return the exact native BNB fee reported by a mined receipt."""
    gas_used = _value(receipt, "gasUsed", "gas_used")
    gas_price = _value(receipt, "effectiveGasPrice", "effective_gas_price")
    if gas_used is None or gas_price is None:
        return None
    return Money(
        Decimal(str(gas_used)) * Decimal(str(gas_price)) / _PRECISION,
        Currency("BNB"),
    )


def _receipt_timestamp(web3: Any, receipt: Any) -> Timestamp | None:
    """Return the mined block timestamp when the RPC exposes it."""
    block_number = _value(receipt, "blockNumber", "block_number")
    if web3 is None or block_number is None:
        return None
    try:
        block = web3.eth.get_block(block_number)
        value = _value(block, "timestamp")
        return (
            Timestamp(datetime.fromtimestamp(int(value), tz=timezone.utc))
            if value is not None
            else None
        )
    except (AttributeError, OSError, TypeError, ValueError, Web3Exception):
        return None


def _as_bool(value: Any) -> bool:
    """Normalize boolean values returned by Predict API versions."""
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes"}
    return bool(value)


def _require_venue(venue_id: Any) -> None:
    if venue_id != PREDICT_VENUE_ID:
        raise ValueError(f"Expected venue {PREDICT_VENUE_ID}, received {venue_id}")
