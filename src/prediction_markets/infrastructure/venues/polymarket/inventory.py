"""Manage recoverable Polymarket outcome inventory through the official SDK.

Responsibilities
----------------
- Read pUSD and binary outcome balances for one condition.
- Submit split, merge, and redeem operations through the gasless Relayer.
- Recover relayed operations by transaction id or unique metadata.

Notes
-----
- Polymarket amounts use six decimal base units.
- Smart-wallet operations require a Relayer API key; CLOB L2 credentials are
  unrelated to these on-chain operations.
"""

import hashlib
import json
import os
from dataclasses import replace
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

import httpx
from polymarket import RelayerApiKey, SecureClient
from polymarket.errors import (
    InsufficientAllowanceError,
    PolymarketError,
    RequestRejectedError,
    SigningError,
    TransportError,
    UserInputError,
)

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
from prediction_markets.infrastructure.venues.polymarket.mappers import (
    POLYMARKET_VENUE_ID,
    polymarket_contract_id,
)

_BASE_UNITS = Decimal("1000000")
_RELAYER_URL = "https://relayer-v2.polymarket.com"
_PUSD = Currency("pUSD")
_PENDING_STATES = {"STATE_NEW", "STATE_EXECUTED", "STATE_MINED"}
_FAILED_STATES = {"STATE_FAILED", "STATE_INVALID"}


class PolymarketOutcomeInventoryAdapter(OutcomeInventoryPort):
    """Manage pUSD-backed YES/NO inventory for one Polymarket smart wallet.

    Notes
    -----
    - The adapter uses the official ``polymarket-client`` position lifecycle
      methods, which select standard or negative-risk collateral adapters.
    - ``setup_approvals`` is an explicit one-time operation and is never run as
      an implicit side effect of balance reads or submissions.
    """

    def __init__(
        self,
        client: Any | None = None,
        *,
        private_key: str | None = None,
        wallet: str | None = None,
        relayer_api_key: str | None = None,
        relayer_api_key_address: str | None = None,
        relayer_http: Any | None = None,
    ) -> None:
        """
        Parameters
        ----------
        client
            Optional official synchronous ``SecureClient``. Tests may inject a
            compatible fake together with ``relayer_http``.
        private_key
            Signer private key. Defaults to ``POLYMARKET_PK``.
        wallet
            Polymarket account wallet holding pUSD and positions. Defaults to
            ``POLYMARKET_FUNDER``.
        relayer_api_key
            Relayer key created in Polymarket Settings. Defaults to
            ``POLYMARKET_RELAYER_API_KEY``.
        relayer_api_key_address
            Address shown as ``Relayer API Key Address``. Defaults to
            ``POLYMARKET_RELAYER_API_KEY_ADDRESS``.
        relayer_http
            Optional HTTP client used only for durable reconciliation.

        Raises
        ------
        ValueError
            If required smart-wallet or Relayer credentials are missing.
        """
        owns_client = client is None
        key = private_key or os.getenv("POLYMARKET_PK")
        account_wallet = wallet or os.getenv("POLYMARKET_FUNDER")
        relay_key = relayer_api_key or os.getenv("POLYMARKET_RELAYER_API_KEY")
        relay_address = relayer_api_key_address or os.getenv(
            "POLYMARKET_RELAYER_API_KEY_ADDRESS",
        )
        if client is None:
            missing = [
                name
                for name, value in (
                    ("POLYMARKET_PK", key),
                    ("POLYMARKET_FUNDER", account_wallet),
                    ("POLYMARKET_RELAYER_API_KEY", relay_key),
                    ("POLYMARKET_RELAYER_API_KEY_ADDRESS", relay_address),
                )
                if not value
            ]
            if missing:
                raise ValueError(f"Missing Polymarket inventory settings: {', '.join(missing)}")
            client = SecureClient.create(
                private_key=key,
                wallet=account_wallet,
                api_key=RelayerApiKey(key=relay_key, address=relay_address),
            )
        elif relayer_http is None and (not relay_key or not relay_address):
            raise ValueError(
                "Injected client requires relayer_http or Polymarket Relayer credentials",
            )

        self._client = client
        self._owns_client = owns_client
        self._owns_relayer_http = relayer_http is None
        self._relayer_http = relayer_http or httpx.Client(
            base_url=_RELAYER_URL,
            headers={
                "RELAYER_API_KEY": relay_key,
                "RELAYER_API_KEY_ADDRESS": relay_address,
            },
            timeout=30.0,
        )

    def setup_approvals(self) -> None:
        """Submit and wait for any missing Polymarket trading approvals.

        Notes
        -----
        - The official SDK checks current allowances before submitting, so the
          operation is safe to repeat.
        - This includes the collateral adapters required by split, merge, and
          redeem as well as the CLOB exchange approvals.
        """
        self._client.setup_trading_approvals()

    def get_balance(self, market_id: MarketID) -> OutcomeInventoryBalance:
        """Read pUSD, YES, and NO balances for one Polymarket condition.

        Parameters
        ----------
        market_id
            Polymarket CTF condition id.

        Returns
        -------
        OutcomeInventoryBalance
            Exact six-decimal balances reported by Polymarket.

        Raises
        ------
        ValueError
            If the condition is missing or lacks both CLOB token ids.
        """
        condition_id, market = self._market(market_id)
        yes_token = market.outcomes.yes.token_id
        no_token = market.outcomes.no.token_id
        if yes_token is None or no_token is None:
            raise ValueError(f"Polymarket condition has no binary token ids: {condition_id}")

        collateral = self._client.get_balance_allowance(asset_type="COLLATERAL").balance
        yes = self._client.get_balance_allowance(
            asset_type="CONDITIONAL",
            token_id=str(yes_token),
        ).balance
        no = self._client.get_balance_allowance(
            asset_type="CONDITIONAL",
            token_id=str(no_token),
        ).balance
        return OutcomeInventoryBalance(
            venue_id=POLYMARKET_VENUE_ID,
            market_id=market_id,
            yes=Quantity(_from_base_units(yes)),
            no=Quantity(_from_base_units(no)),
            collateral=Money(_from_base_units(collateral), _PUSD),
            observed_at=Timestamp.now(),
            yes_contract_id=polymarket_contract_id(condition_id, str(yes_token)),
            no_contract_id=polymarket_contract_id(condition_id, str(no_token)),
        )

    def get_settlement(self, market_id: MarketID) -> OutcomeInventorySettlement:
        """Read the final winner flags reported for a Polymarket condition."""
        condition_id, market = self._market(market_id)
        yes_outcome = market.outcomes.yes
        no_outcome = market.outcomes.no
        yes_winner = getattr(yes_outcome, "winner", None)
        no_winner = getattr(no_outcome, "winner", None)
        if yes_winner is True and no_winner is False:
            yes, no = Decimal("1"), Decimal("0")
        elif no_winner is True and yes_winner is False:
            yes, no = Decimal("0"), Decimal("1")
        else:
            raise ValueError("Polymarket payout is not settled or is not verifiable")
        yes_token = yes_outcome.token_id
        no_token = no_outcome.token_id
        if yes_token is None or no_token is None:
            raise ValueError(f"Polymarket condition has no binary token ids: {condition_id}")
        return OutcomeInventorySettlement(
            venue_id=POLYMARKET_VENUE_ID,
            market_id=market_id,
            yes_contract_id=polymarket_contract_id(condition_id, str(yes_token)),
            no_contract_id=polymarket_contract_id(condition_id, str(no_token)),
            yes_payout=Price(yes),
            no_payout=Price(no),
            observed_at=Timestamp.now(),
        )

    def prepare(self, intent: OutcomeInventoryIntent) -> PreparedInventoryOperation:
        """Serialize the exact official SDK position-lifecycle invocation.

        Parameters
        ----------
        intent
            Split, merge, or redeem intent for this venue.

        Returns
        -------
        PreparedInventoryOperation
            Durable method arguments and unique Relayer metadata.

        Raises
        ------
        ValueError
            If the venue, condition id, or six-decimal quantity is invalid.
        """
        _require_venue(intent.venue_id)
        balance_before = self.get_balance(intent.market_id)
        condition_id = _condition_id(intent.market_id)
        amount = _to_base_units(intent.quantity.value) if intent.quantity else None
        metadata = _metadata(str(intent.operation_id))
        request = json.dumps(
            {
                "schema": 1,
                "action": intent.action.value,
                "condition_id": condition_id,
                "amount": amount,
                "metadata": metadata,
            },
            separators=(",", ":"),
            sort_keys=True,
        ).encode()
        return PreparedInventoryOperation(
            intent=intent,
            reference=_reference(
                intent,
                metadata=metadata,
                balance_before=balance_before,
            ),
            request=request,
        )

    def submit(self, operation: PreparedInventoryOperation) -> InventorySubmissionResult:
        """Submit one persisted split, merge, or redeem request to the Relayer.

        Parameters
        ----------
        operation
            Exact request produced by :meth:`prepare`.

        Returns
        -------
        InventorySubmissionResult
            Accepted with a Relayer transaction id, rejected, or uncertain.
        """
        _require_venue(operation.reference.venue_id)
        data = _request(operation)
        try:
            handle = self._submit_request(data)
        except (UserInputError, SigningError, InsufficientAllowanceError) as error:
            return InventorySubmissionResult(
                status=InventorySubmissionStatus.REJECTED,
                reference=operation.reference,
                reason=str(error),
            )
        except RequestRejectedError as error:
            status = (
                InventorySubmissionStatus.REJECTED
                if 400 <= error.status < 500
                else InventorySubmissionStatus.UNKNOWN
            )
            return InventorySubmissionResult(
                status=status,
                reference=operation.reference,
                reason=str(error),
            )
        except (TransportError, PolymarketError):
            return InventorySubmissionResult(
                status=InventorySubmissionStatus.UNKNOWN,
                reference=operation.reference,
            )

        transaction_id = getattr(handle, "transaction_id", None)
        if not transaction_id:
            return InventorySubmissionResult(
                status=InventorySubmissionStatus.UNKNOWN,
                reference=operation.reference,
                reason="Polymarket Relayer returned no transaction id",
            )
        reference = _reference(
            operation.intent,
            metadata=data["metadata"],
            transaction_id=str(transaction_id),
            balance_before=operation.reference.balance_before,
        )
        snapshot = InventoryOperationSnapshot(
            reference=reference,
            status=InventoryOperationStatus.PENDING,
            updated_at=Timestamp.now(),
            transaction_id=str(transaction_id),
            quantity=operation.intent.quantity,
        )
        return InventorySubmissionResult(
            status=InventorySubmissionStatus.ACCEPTED,
            reference=reference,
            snapshot=snapshot,
        )

    def reconcile(
        self,
        reference: InventoryOperationReference,
    ) -> InventoryReconciliationResult:
        """Resolve a submitted operation through the Polymarket Relayer.

        Parameters
        ----------
        reference
            Durable reference containing a transaction id or unique metadata.

        Returns
        -------
        InventoryReconciliationResult
            Current operation state, proven absence, or uncertainty.
        """
        _require_venue(reference.venue_id)
        recovery = _recovery_data(reference)
        transaction_id = recovery.get("transaction_id")
        try:
            if transaction_id:
                response = self._relayer_http.get(
                    f"/v1/account/transactions/{transaction_id}",
                )
                if response.status_code == 404:
                    return InventoryReconciliationResult(
                        status=InventoryReconciliationStatus.NOT_FOUND,
                        reference=reference,
                    )
                response.raise_for_status()
                transaction = response.json()
            else:
                response = self._relayer_http.get("/transactions")
                response.raise_for_status()
                matches = [
                    item
                    for item in response.json()
                    if item.get("metadata") == recovery["metadata"]
                ]
                if len(matches) != 1:
                    return InventoryReconciliationResult(
                        status=InventoryReconciliationStatus.UNKNOWN,
                        reference=reference,
                    )
                transaction = matches[0]
        except (httpx.HTTPError, TypeError, ValueError, KeyError):
            return InventoryReconciliationResult(
                status=InventoryReconciliationStatus.UNKNOWN,
                reference=reference,
            )

        snapshot = _snapshot(reference, transaction)
        if snapshot is None:
            return InventoryReconciliationResult(
                status=InventoryReconciliationStatus.UNKNOWN,
                reference=reference,
            )
        if snapshot.status is InventoryOperationStatus.CONFIRMED:
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
            status=InventoryReconciliationStatus.FOUND,
            reference=reference,
            snapshot=snapshot,
        )

    def close(self) -> None:
        """Close HTTP resources owned by this adapter."""
        if self._owns_relayer_http:
            self._relayer_http.close()
        if self._owns_client:
            self._client.close()

    def _market(self, market_id: MarketID) -> tuple[str, Any]:
        """Return one open or closed Polymarket condition."""
        condition_id = _condition_id(market_id)
        for closed in (None, True):
            params: dict[str, Any] = {
                "condition_ids": condition_id,
                "page_size": 1,
            }
            if closed is not None:
                params["closed"] = closed
            page = self._client.list_markets(**params).first_page()
            market = next(
                (
                    item
                    for item in page.items
                    if str(item.condition_id).lower() == condition_id.lower()
                ),
                None,
            )
            if market is not None:
                return condition_id, market
        raise ValueError(f"Unknown Polymarket condition: {condition_id}")

    def _submit_request(self, data: dict[str, Any]) -> Any:
        """Dispatch one validated persisted request through the official SDK."""
        common = {
            "condition_id": data["condition_id"],
            "metadata": data["metadata"],
        }
        if data["action"] == OutcomeInventoryAction.SPLIT.value:
            return self._client.split_position(amount=data["amount"], **common)
        if data["action"] == OutcomeInventoryAction.MERGE.value:
            return self._client.merge_positions(amount=data["amount"], **common)
        if data["action"] == OutcomeInventoryAction.REDEEM.value:
            return self._client.redeem_positions(**common)
        raise ValueError(f"Unsupported Polymarket inventory action: {data['action']}")


def _condition_id(market_id: MarketID) -> str:
    value = str(market_id)
    if not value.startswith("0x") or len(value) != 66:
        raise ValueError("Polymarket market_id must be a 32-byte condition id")
    try:
        bytes.fromhex(value[2:])
    except ValueError as error:
        raise ValueError("Polymarket market_id must be hexadecimal") from error
    return value


def _to_base_units(quantity: Decimal) -> int:
    raw = quantity * _BASE_UNITS
    if raw != raw.to_integral_value():
        raise ValueError("Polymarket quantities support at most six decimal places")
    return int(raw)


def _from_base_units(value: int) -> Decimal:
    return Decimal(value) / _BASE_UNITS


def _metadata(operation_id: str) -> str:
    digest = hashlib.sha256(operation_id.encode()).hexdigest()
    return f"prediction-markets:inventory:{digest}"


def _reference(
    intent: OutcomeInventoryIntent,
    *,
    metadata: str,
    transaction_id: str | None = None,
    balance_before: OutcomeInventoryBalance | None = None,
) -> InventoryOperationReference:
    return InventoryOperationReference(
        venue_id=POLYMARKET_VENUE_ID,
        operation_id=intent.operation_id,
        recovery_data=json.dumps(
            {"metadata": metadata, "transaction_id": transaction_id},
            separators=(",", ":"),
            sort_keys=True,
        ).encode(),
        quantity=intent.quantity,
        action=intent.action,
        portfolio_id=intent.portfolio_id,
        balance_before=balance_before,
    )


def _request(operation: PreparedInventoryOperation) -> dict[str, Any]:
    try:
        data = json.loads(operation.request)
    except (json.JSONDecodeError, UnicodeDecodeError) as error:
        raise ValueError("Invalid persisted Polymarket inventory request") from error
    if data.get("schema") != 1:
        raise ValueError("Unsupported Polymarket inventory request schema")
    if data.get("action") not in {action.value for action in OutcomeInventoryAction}:
        raise ValueError("Invalid persisted Polymarket inventory action")
    return data


def _recovery_data(reference: InventoryOperationReference) -> dict[str, Any]:
    try:
        data = json.loads(reference.recovery_data)
    except (json.JSONDecodeError, UnicodeDecodeError) as error:
        raise ValueError("Invalid Polymarket inventory recovery data") from error
    if not isinstance(data.get("metadata"), str) or not data["metadata"]:
        raise ValueError("Polymarket recovery data has no metadata")
    return data


def _snapshot(
    reference: InventoryOperationReference,
    transaction: dict[str, Any],
) -> InventoryOperationSnapshot | None:
    if not isinstance(transaction, dict):
        return None
    state = str(transaction.get("state") or "")
    if state in _PENDING_STATES:
        status = InventoryOperationStatus.PENDING
    elif state == "STATE_CONFIRMED":
        status = InventoryOperationStatus.CONFIRMED
    elif state in _FAILED_STATES:
        status = InventoryOperationStatus.FAILED
    else:
        return None
    transaction_id = transaction.get("transaction_id") or transaction.get("transactionID")
    updated = transaction.get("updated_at") or transaction.get("updatedAt")
    reason = transaction.get("error_msg") or transaction.get("errorMsg")
    return InventoryOperationSnapshot(
        reference=reference,
        status=status,
        updated_at=_relayer_timestamp(updated),
        transaction_id=str(transaction_id) if transaction_id else None,
        quantity=reference.quantity,
        reason=str(reason) if reason else None,
    )


def _relayer_timestamp(value: Any) -> Timestamp:
    """Parse ISO or Unix-millisecond timestamps returned by the Relayer."""
    if value is None:
        return Timestamp.now()
    if isinstance(value, (int, float)):
        seconds = float(value)
        if seconds > 10_000_000_000:
            seconds /= 1000
        return Timestamp(datetime.fromtimestamp(seconds, tz=timezone.utc))
    return Timestamp.from_iso(str(value).replace("Z", "+00:00"))


def _require_venue(venue_id: Any) -> None:
    if venue_id != POLYMARKET_VENUE_ID:
        raise ValueError(f"Expected venue {POLYMARKET_VENUE_ID}, received {venue_id}")
