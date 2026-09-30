"""Manage recoverable Limitless outcome inventory directly on Base.

Responsibilities
----------------
- Read USDC and binary CTF balances for Limitless CLOB markets.
- Prepare, sign, submit, and reconcile split, merge, and redeem transactions.
- Keep the exact signed transaction durable before broadcasting it.

Notes
-----
- Limitless EOA inventory operations are self-custodial Base transactions and
  consume ETH for gas.
- Standard markets use the canonical CTF contract.
- NegRisk markets use the per-market adapter returned by Limitless venue data.
"""

import asyncio
import json
import os
from collections.abc import Awaitable
from dataclasses import replace
from datetime import datetime, timezone
from decimal import Decimal
from threading import Lock
from typing import Any, TypeVar

from eth_account import Account
from limitless_sdk.api import HttpClient
from limitless_sdk.markets import MarketFetcher
from limitless_sdk.types import HMACCredentials
from web3 import Web3
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
from prediction_markets.infrastructure.venues.limitless.mappers import (
    LIMITLESS_VENUE_ID,
    limitless_contract_id,
)

_T = TypeVar("_T")
_CHAIN_ID = 8453
_CTF_ADDRESS = "0xC9c98965297Bc527861c898329Ee280632B76e18"
_USDC_ADDRESS = "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913"
_ROOT_COLLECTION = bytes(32)
_BINARY_PARTITION = [1, 2]
_MAX_UINT256 = 2**256 - 1

_ERC20_ABI = [
    {
        "inputs": [{"name": "account", "type": "address"}],
        "name": "balanceOf",
        "outputs": [{"name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [
            {"name": "owner", "type": "address"},
            {"name": "spender", "type": "address"},
        ],
        "name": "allowance",
        "outputs": [{"name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [
            {"name": "spender", "type": "address"},
            {"name": "amount", "type": "uint256"},
        ],
        "name": "approve",
        "outputs": [{"name": "", "type": "bool"}],
        "stateMutability": "nonpayable",
        "type": "function",
    },
]

_CTF_ABI = [
    {
        "inputs": [
            {"name": "account", "type": "address"},
            {"name": "id", "type": "uint256"},
        ],
        "name": "balanceOf",
        "outputs": [{"name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [
            {"name": "account", "type": "address"},
            {"name": "operator", "type": "address"},
        ],
        "name": "isApprovedForAll",
        "outputs": [{"name": "", "type": "bool"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [{"name": "conditionId", "type": "bytes32"}],
        "name": "payoutDenominator",
        "outputs": [{"name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [
            {"name": "conditionId", "type": "bytes32"},
            {"name": "outcomeIndex", "type": "uint256"},
        ],
        "name": "payoutNumerators",
        "outputs": [{"name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [
            {"name": "collateralToken", "type": "address"},
            {"name": "parentCollectionId", "type": "bytes32"},
            {"name": "conditionId", "type": "bytes32"},
            {"name": "partition", "type": "uint256[]"},
            {"name": "amount", "type": "uint256"},
        ],
        "name": "splitPosition",
        "outputs": [],
        "stateMutability": "nonpayable",
        "type": "function",
    },
    {
        "inputs": [
            {"name": "collateralToken", "type": "address"},
            {"name": "parentCollectionId", "type": "bytes32"},
            {"name": "conditionId", "type": "bytes32"},
            {"name": "partition", "type": "uint256[]"},
            {"name": "amount", "type": "uint256"},
        ],
        "name": "mergePositions",
        "outputs": [],
        "stateMutability": "nonpayable",
        "type": "function",
    },
    {
        "inputs": [
            {"name": "collateralToken", "type": "address"},
            {"name": "parentCollectionId", "type": "bytes32"},
            {"name": "conditionId", "type": "bytes32"},
            {"name": "indexSets", "type": "uint256[]"},
        ],
        "name": "redeemPositions",
        "outputs": [],
        "stateMutability": "nonpayable",
        "type": "function",
    },
]

_NEG_RISK_ADAPTER_ABI = [
    {
        "inputs": [
            {"name": "conditionId", "type": "bytes32"},
            {"name": "amount", "type": "uint256"},
        ],
        "name": "splitPosition",
        "outputs": [],
        "stateMutability": "nonpayable",
        "type": "function",
    },
    {
        "inputs": [
            {"name": "conditionId", "type": "bytes32"},
            {"name": "amount", "type": "uint256"},
        ],
        "name": "mergePositions",
        "outputs": [],
        "stateMutability": "nonpayable",
        "type": "function",
    },
    {
        "inputs": [
            {"name": "conditionId", "type": "bytes32"},
            {"name": "amounts", "type": "uint256[]"},
        ],
        "name": "redeemPositions",
        "outputs": [],
        "stateMutability": "nonpayable",
        "type": "function",
    },
]


class LimitlessOutcomeInventoryAdapter(OutcomeInventoryPort):
    """Manage Limitless CTF inventory held by one Base EOA.

    Invariants
    ----------
    - One adapter instance owns nonce allocation for one signer.
    - Every prepared operation contains the exact signed transaction and hash.

    Notes
    -----
    - ``setup_approvals`` is an explicit, repeatable prerequisite for splitting.
    - NegRisk operations use the market-specific adapter contract.
    """

    def __init__(
        self,
        private_key: str | None = None,
        *,
        rpc_url: str | None = None,
        api_key: str | None = None,
        api_secret: str | None = None,
        web3: Any | None = None,
        account: Any | None = None,
        http_client: Any | None = None,
        market_fetcher: Any | None = None,
    ) -> None:
        """
        Parameters
        ----------
        private_key
            Base EOA private key. Defaults to ``LIMITLESS_PRIVATE_KEY``.
        rpc_url
            Base Mainnet JSON-RPC endpoint. Defaults to ``LIMITLESS_RPC_URL``.
        api_key
            Optional Limitless API key used for market metadata.
        api_secret
            Optional Limitless HMAC secret paired with ``api_key``.
        web3
            Optional Web3-compatible client for tests or custom providers.
        account
            Optional signer compatible with ``LocalAccount``.
        http_client
            Optional official Limitless HTTP client.
        market_fetcher
            Optional official Limitless market fetcher.

        Raises
        ------
        ValueError
            If signer, RPC, or paired HMAC settings are missing.
        """
        key = private_key or os.getenv("LIMITLESS_PRIVATE_KEY")
        endpoint = rpc_url or os.getenv("LIMITLESS_RPC_URL")
        if account is None:
            if not key:
                raise ValueError("Set LIMITLESS_PRIVATE_KEY or pass private_key")
            account = Account.from_key(key)
        if web3 is None:
            if not endpoint:
                raise ValueError("Set LIMITLESS_RPC_URL or pass rpc_url")
            web3 = Web3(Web3.HTTPProvider(endpoint, request_kwargs={"timeout": 30}))

        owns_http_client = market_fetcher is None and http_client is None
        if market_fetcher is None:
            resolved_api_key = api_key or os.getenv("LIMITLESS_API_KEY")
            resolved_api_secret = api_secret or os.getenv("LIMITLESS_API_SECRET")
            if resolved_api_secret and not resolved_api_key:
                raise ValueError(
                    "LIMITLESS_API_KEY is required when LIMITLESS_API_SECRET is set",
                )
            http_client = http_client or (
                HttpClient(
                    hmac_credentials=HMACCredentials(
                        tokenId=resolved_api_key,
                        secret=resolved_api_secret,
                    ),
                )
                if resolved_api_secret
                else HttpClient(api_key=resolved_api_key)
            )
            market_fetcher = MarketFetcher(http_client)

        self._web3 = web3
        self._account = account
        self._http_client = http_client
        self._market_fetcher = market_fetcher
        self._owns_http_client = owns_http_client
        self._ctf_address = web3.to_checksum_address(_CTF_ADDRESS)
        self._usdc_address = web3.to_checksum_address(_USDC_ADDRESS)
        self._ctf = web3.eth.contract(address=self._ctf_address, abi=_CTF_ABI)
        self._usdc = web3.eth.contract(address=self._usdc_address, abi=_ERC20_ABI)
        self._markets: dict[str, Any] = {}
        self._runner = asyncio.Runner()
        self._runner_lock = Lock()
        self._nonce_lock = Lock()
        self._reserved_nonces: dict[str, int] = {}
        self._closed = False

    def setup_approvals(self) -> str | None:
        """Approve the Limitless CTF to spend this EOA's Base USDC.

        Returns
        -------
        str | None
            Confirmed approval transaction hash, or ``None`` when the existing
            allowance is already effectively unlimited.

        Raises
        ------
        RuntimeError
            If the approval transaction is rejected on-chain.
        """
        allowance = self._usdc.functions.allowance(
            self._account.address,
            self._ctf_address,
        ).call()
        if allowance >= 2**255:
            return None
        raw, transaction_hash, _ = self._sign(
            self._usdc.functions.approve(self._ctf_address, _MAX_UINT256),
        )
        returned_hash = _hash_hex(self._web3.eth.send_raw_transaction(raw))
        if returned_hash != transaction_hash:
            raise RuntimeError("Base RPC returned a different approval transaction hash")
        receipt = self._web3.eth.wait_for_transaction_receipt(returned_hash, timeout=120)
        if int(_value(receipt, "status") or 0) != 1:
            raise RuntimeError(f"Limitless USDC approval failed: {transaction_hash}")
        return transaction_hash

    def get_balance(self, market_id: MarketID) -> OutcomeInventoryBalance:
        """Read Base USDC, YES, and NO balances for one Limitless market.

        Parameters
        ----------
        market_id
            Limitless market slug.

        Returns
        -------
        OutcomeInventoryBalance
            Current EOA balances converted from collateral base units.
        """
        market = self._market_data(market_id)
        scale = Decimal(10) ** market["decimals"]
        yes = self._ctf.functions.balanceOf(
            self._account.address,
            market["yes_token"],
        ).call()
        no = self._ctf.functions.balanceOf(
            self._account.address,
            market["no_token"],
        ).call()
        collateral = self._usdc.functions.balanceOf(self._account.address).call()
        return OutcomeInventoryBalance(
            venue_id=LIMITLESS_VENUE_ID,
            market_id=market_id,
            yes=Quantity(Decimal(yes) / scale),
            no=Quantity(Decimal(no) / scale),
            collateral=Money(
                Decimal(collateral) / scale,
                Currency(market["symbol"]),
            ),
            observed_at=Timestamp.now(),
            yes_contract_id=limitless_contract_id(str(market_id), "yes"),
            no_contract_id=limitless_contract_id(str(market_id), "no"),
        )

    def get_settlement(self, market_id: MarketID) -> OutcomeInventorySettlement:
        """Read the final CTF payout vector for one Limitless market."""
        market = self._market_data(market_id)
        condition_id = market["condition_id"]
        denominator = int(
            self._ctf.functions.payoutDenominator(condition_id).call(),
        )
        if denominator <= 0:
            raise ValueError("Limitless payout is not settled on-chain yet")
        yes = Decimal(
            self._ctf.functions.payoutNumerators(condition_id, 0).call(),
        ) / denominator
        no = Decimal(
            self._ctf.functions.payoutNumerators(condition_id, 1).call(),
        ) / denominator
        return OutcomeInventorySettlement(
            venue_id=LIMITLESS_VENUE_ID,
            market_id=market_id,
            yes_contract_id=limitless_contract_id(str(market_id), "yes"),
            no_contract_id=limitless_contract_id(str(market_id), "no"),
            yes_payout=Price(yes),
            no_payout=Price(no),
            observed_at=Timestamp.now(),
        )

    def list_redeemable_markets(self) -> tuple[MarketID, ...]:
        """Discover resolved Limitless CLOB markets in the account portfolio.

        Returns
        -------
        tuple[MarketID, ...]
            Resolved market slugs reported by the authenticated portfolio API.
            On-chain balances are checked later before a transaction is signed.

        Raises
        ------
        TypeError
            If the portfolio response does not contain the expected CLOB list.

        Notes
        -----
        - The portfolio endpoint is used to find markets that are no longer in
          the live market feed, while ``get_balance`` remains authoritative for
          whether a redeem transaction is needed.
        """
        if self._http_client is None:
            return ()
        payload = self._run(self._http_client.get("/portfolio/positions"))
        if not isinstance(payload, dict):
            raise TypeError("Unexpected Limitless portfolio response")
        clob = payload.get("clob") or []
        if not isinstance(clob, list):
            raise TypeError("Unexpected Limitless CLOB portfolio response")
        markets: list[MarketID] = []
        for item in clob:
            if not isinstance(item, dict):
                continue
            market = item.get("market")
            if not isinstance(market, dict):
                continue
            if not (
                bool(market.get("resolved"))
                or str(market.get("status") or "").upper() == "RESOLVED"
            ):
                continue
            slug = str(market.get("slug") or "").strip()
            if slug and MarketID(slug) not in markets:
                markets.append(MarketID(slug))
        return tuple(markets)

    def prepare(self, intent: OutcomeInventoryIntent) -> PreparedInventoryOperation:
        """Build and sign one exact Limitless inventory transaction.

        Parameters
        ----------
        intent
            Limitless split, merge, or redeem request.

        Returns
        -------
        PreparedInventoryOperation
            Signed transaction bytes and deterministic Base transaction hash.

        Raises
        ------
        ValueError
            If balances, allowance, resolution state, or quantity are invalid.
        NotImplementedError
            If the market uses unsupported collateral.
        """
        _require_venue(intent.venue_id)
        balance_before = self.get_balance(intent.market_id)
        market = self._market_data(intent.market_id)
        amount = (
            _to_base_units(intent.quantity.value, market["decimals"])
            if intent.quantity
            else None
        )
        condition_id = market["condition_id"]
        adapter_address = market["adapter"]

        if adapter_address is not None:
            adapter = self._web3.eth.contract(
                address=adapter_address,
                abi=_NEG_RISK_ADAPTER_ABI,
            )
            if intent.action is OutcomeInventoryAction.SPLIT:
                if self._usdc.functions.balanceOf(self._account.address).call() < amount:
                    raise ValueError("Insufficient Base USDC balance for Limitless split")
                allowance = self._usdc.functions.allowance(
                    self._account.address,
                    adapter_address,
                ).call()
                if allowance < amount:
                    raise ValueError(
                        "Insufficient Limitless NegRisk adapter USDC allowance; "
                        "approve USDC to market venue.adapter",
                    )
                function = adapter.functions.splitPosition(condition_id, amount)
            elif intent.action is OutcomeInventoryAction.MERGE:
                self._require_neg_risk_operator(adapter_address)
                self._require_complete_sets(market, amount)
                function = adapter.functions.mergePositions(condition_id, amount)
            else:
                self._require_neg_risk_operator(adapter_address)
                self._require_settled(condition_id)
                yes, no = self._outcome_balances(market)
                if yes == 0 and no == 0:
                    raise ValueError("No Limitless outcome inventory is available to redeem")
                function = adapter.functions.redeemPositions(condition_id, [yes, no])
        elif intent.action is OutcomeInventoryAction.SPLIT:
            if self._usdc.functions.balanceOf(self._account.address).call() < amount:
                raise ValueError("Insufficient Base USDC balance for Limitless split")
            allowance = self._usdc.functions.allowance(
                self._account.address,
                self._ctf_address,
            ).call()
            if allowance < amount:
                raise ValueError(
                    "Insufficient Limitless CTF USDC allowance; call setup_approvals()",
                )
            function = self._ctf.functions.splitPosition(
                self._usdc_address,
                _ROOT_COLLECTION,
                condition_id,
                _BINARY_PARTITION,
                amount,
            )
        elif intent.action is OutcomeInventoryAction.MERGE:
            self._require_complete_sets(market, amount)
            function = self._ctf.functions.mergePositions(
                self._usdc_address,
                _ROOT_COLLECTION,
                condition_id,
                _BINARY_PARTITION,
                amount,
            )
        else:
            self._require_settled(condition_id)
            yes, no = self._outcome_balances(market)
            if yes == 0 and no == 0:
                raise ValueError("No Limitless outcome inventory is available to redeem")
            function = self._ctf.functions.redeemPositions(
                self._usdc_address,
                _ROOT_COLLECTION,
                condition_id,
                _BINARY_PARTITION,
            )

        raw, transaction_hash, nonce = self._sign(function)
        request = json.dumps(
            {
                "schema": 1,
                "operation_id": str(intent.operation_id),
                "action": intent.action.value,
                "market_id": str(intent.market_id),
                "transaction_hash": transaction_hash,
                "raw_transaction": "0x" + raw.hex(),
            },
            separators=(",", ":"),
            sort_keys=True,
        ).encode()
        reference = InventoryOperationReference(
            venue_id=LIMITLESS_VENUE_ID,
            operation_id=intent.operation_id,
            recovery_data=json.dumps(
                {
                    "schema": 1,
                    "transaction_hash": transaction_hash,
                    "nonce": nonce,
                },
                separators=(",", ":"),
                sort_keys=True,
            ).encode(),
            quantity=intent.quantity,
            action=intent.action,
            portfolio_id=intent.portfolio_id,
            balance_before=balance_before,
        )
        return PreparedInventoryOperation(intent, reference, request)

    def discard_prepared(self, operation: PreparedInventoryOperation) -> None:
        """Release the nonce of a transaction the caller has never broadcast."""
        data = self._request(operation)
        with self._nonce_lock:
            self._reserved_nonces.pop(data["transaction_hash"], None)

    def submit(self, operation: PreparedInventoryOperation) -> InventorySubmissionResult:
        """Broadcast a persisted signed Base transaction without rebuilding it.

        Parameters
        ----------
        operation
            Exact transaction returned by :meth:`prepare`.

        Returns
        -------
        InventorySubmissionResult
            Accepted, rejected, or uncertain broadcast state.
        """
        _require_venue(operation.reference.venue_id)
        data = self._request(operation)
        raw = bytes.fromhex(data["raw_transaction"].removeprefix("0x"))
        try:
            returned_hash = _hash_hex(self._web3.eth.send_raw_transaction(raw))
        except ValueError as error:
            reason = _rpc_error(error)
            if "already known" in reason.lower():
                return self._accepted(operation.reference, data["transaction_hash"])
            reconciled = self.reconcile(operation.reference)
            if reconciled.status is InventoryReconciliationStatus.FOUND:
                return InventorySubmissionResult(
                    InventorySubmissionStatus.ACCEPTED,
                    operation.reference,
                    reconciled.snapshot,
                )
            rejected = any(
                marker in reason.lower()
                for marker in (
                    "insufficient funds",
                    "intrinsic gas too low",
                    "invalid sender",
                    "execution reverted",
                )
            )
            if rejected:
                with self._nonce_lock:
                    self._reserved_nonces.pop(data["transaction_hash"], None)
            return InventorySubmissionResult(
                InventorySubmissionStatus.REJECTED
                if rejected
                else InventorySubmissionStatus.UNKNOWN,
                operation.reference,
                reason=reason,
            )
        except (Web3Exception, OSError, TimeoutError):
            return InventorySubmissionResult(
                InventorySubmissionStatus.UNKNOWN,
                operation.reference,
            )

        if returned_hash != data["transaction_hash"]:
            return InventorySubmissionResult(
                InventorySubmissionStatus.UNKNOWN,
                operation.reference,
                reason="Base RPC returned a different transaction hash",
            )
        return self._accepted(operation.reference, returned_hash)

    def reconcile(
        self,
        reference: InventoryOperationReference,
    ) -> InventoryReconciliationResult:
        """Resolve a persisted Limitless inventory transaction on Base.

        Parameters
        ----------
        reference
            Durable reference containing the deterministic transaction hash.

        Returns
        -------
        InventoryReconciliationResult
            Mined state, pending state, absence, or RPC uncertainty.
        """
        _require_venue(reference.venue_id)
        transaction_hash = _recovery_hash(reference)
        try:
            receipt = self._web3.eth.get_transaction_receipt(transaction_hash)
        except TransactionNotFound:
            try:
                self._web3.eth.get_transaction(transaction_hash)
            except TransactionNotFound:
                nonce = _recovery_nonce(reference)
                if nonce is not None:
                    try:
                        confirmed_nonce = self._web3.eth.get_transaction_count(
                            self._account.address,
                            "latest",
                        )
                    except (Web3Exception, OSError, TimeoutError, ValueError):
                        return InventoryReconciliationResult(
                            InventoryReconciliationStatus.UNKNOWN,
                            reference,
                        )
                    if confirmed_nonce > nonce:
                        return InventoryReconciliationResult(
                            InventoryReconciliationStatus.FOUND,
                            reference,
                            InventoryOperationSnapshot(
                                reference=reference,
                                status=InventoryOperationStatus.FAILED,
                                updated_at=Timestamp.now(),
                                transaction_id=transaction_hash,
                                quantity=reference.quantity,
                                reason="Base transaction nonce was consumed by another transaction",
                            ),
                        )
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
                "Base transaction reverted"
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
        """Close SDK resources owned by this adapter."""
        if self._closed:
            return
        if self._owns_http_client and self._http_client is not None:
            self._run(self._http_client.close())
        self._runner.close()
        self._closed = True

    def _market_data(self, market_id: MarketID) -> dict[str, Any]:
        """Load and validate binary CTF or NegRisk market metadata."""
        slug = str(market_id)
        market = self._markets.get(slug)
        if market is None:
            market = self._run(self._market_fetcher.get_market(slug))
            self._markets[slug] = market
        is_neg_risk = bool(
            _value(market, "neg_risk_request_id", "negRiskRequestId")
            or _value(
                market,
                "neg_risk_market_id",
                "negRiskMarketId",
            )
        )
        if is_neg_risk:
            venue = _value(market, "venue")
            adapter = _value(venue, "adapter")
            if not adapter:
                raise ValueError(f"Limitless NegRisk market {slug} has no venue adapter")
            adapter_address = self._web3.to_checksum_address(str(adapter))
        else:
            adapter_address = None

        condition_id = _bytes32(_value(market, "condition_id", "conditionId"))
        collateral = _value(market, "collateral_token", "collateralToken")
        collateral_address = self._web3.to_checksum_address(
            str(_value(collateral, "address") or ""),
        )
        if collateral_address != self._usdc_address:
            raise NotImplementedError("Limitless inventory supports native Base USDC only")
        decimals = int(_value(collateral, "decimals"))
        if decimals < 0 or decimals > 18:
            raise ValueError("Invalid Limitless collateral decimals")
        symbol = str(_value(collateral, "symbol") or "USDC")

        position_ids = _value(market, "position_ids", "positionIds")
        if isinstance(position_ids, (list, tuple)) and len(position_ids) >= 2:
            yes_token, no_token = position_ids[:2]
        else:
            tokens = _value(market, "tokens")
            yes_token = _value(tokens, "yes")
            no_token = _value(tokens, "no")
        if yes_token is None or no_token is None:
            raise ValueError(f"Limitless market {slug} has no binary position ids")
        return {
            "adapter": adapter_address,
            "condition_id": condition_id,
            "decimals": decimals,
            "symbol": symbol,
            "yes_token": int(yes_token),
            "no_token": int(no_token),
        }

    def _outcome_balances(self, market: dict[str, Any]) -> tuple[int, int]:
        """Read the current YES and NO token balances for one market."""
        return (
            self._ctf.functions.balanceOf(
                self._account.address,
                market["yes_token"],
            ).call(),
            self._ctf.functions.balanceOf(
                self._account.address,
                market["no_token"],
            ).call(),
        )

    def _require_complete_sets(self, market: dict[str, Any], amount: int) -> None:
        """Reject a merge that exceeds the smaller outcome balance."""
        yes, no = self._outcome_balances(market)
        if min(yes, no) < amount:
            raise ValueError("Insufficient complete YES/NO sets for Limitless merge")

    def _require_settled(self, condition_id: bytes) -> None:
        """Reject redemption before the CTF payout vector is available."""
        if self._ctf.functions.payoutDenominator(condition_id).call() == 0:
            raise ValueError("Limitless payout is not settled on-chain yet")

    def _require_neg_risk_operator(self, adapter_address: str) -> None:
        """Require the wallet to approve the market adapter for CTF transfers."""
        if not self._ctf.functions.isApprovedForAll(
            self._account.address,
            adapter_address,
        ).call():
            raise ValueError(
                "Limitless NegRisk adapter is not approved for Conditional Tokens; "
                "approve CTF to market venue.adapter",
            )

    def _sign(self, function: Any) -> tuple[bytes, str, int]:
        """Reserve one nonce and sign the exact contract invocation."""
        with self._nonce_lock:
            chain_id = int(self._web3.eth.chain_id)
            if chain_id != _CHAIN_ID:
                raise ValueError(f"Limitless inventory requires Base Mainnet, got {chain_id}")
            pending_nonce = self._web3.eth.get_transaction_count(
                self._account.address,
                "pending",
            )
            # ponytail: one adapter owns one signer; add a shared nonce manager only
            # when multiple adapter instances must sign concurrently with the same key.
            self._reserved_nonces = {
                key: value
                for key, value in self._reserved_nonces.items()
                if value >= pending_nonce
            }
            reserved = set(self._reserved_nonces.values())
            nonce = pending_nonce
            while nonce in reserved:
                nonce += 1
            transaction = function.build_transaction(
                {
                    "chainId": _CHAIN_ID,
                    "from": self._account.address,
                    "nonce": nonce,
                },
            )
            signed = self._account.sign_transaction(transaction)
            self._reserved_nonces[_hash_hex(signed.hash)] = nonce
        raw = bytes(signed.raw_transaction)
        return raw, _hash_hex(signed.hash), nonce

    def _request(self, operation: PreparedInventoryOperation) -> dict[str, Any]:
        """Validate persisted request identity and signed-transaction integrity."""
        try:
            data = json.loads(operation.request)
        except (json.JSONDecodeError, UnicodeDecodeError) as error:
            raise ValueError("Invalid persisted Limitless inventory request") from error
        recovery_hash = _recovery_hash(operation.reference)
        if (
            data.get("schema") != 1
            or data.get("operation_id") != str(operation.intent.operation_id)
            or data.get("action") != operation.intent.action.value
            or data.get("market_id") != str(operation.intent.market_id)
            or data.get("transaction_hash") != recovery_hash
            or not isinstance(data.get("raw_transaction"), str)
        ):
            raise ValueError("Limitless inventory request identity does not match")
        try:
            raw = bytes.fromhex(data["raw_transaction"].removeprefix("0x"))
        except ValueError as error:
            raise ValueError("Invalid signed Limitless inventory transaction") from error
        if _hash_hex(self._web3.keccak(raw)) != recovery_hash:
            raise ValueError("Signed Limitless inventory transaction hash does not match")
        return data

    def _accepted(
        self,
        reference: InventoryOperationReference,
        transaction_hash: str,
    ) -> InventorySubmissionResult:
        """Build the normalized pending result for an accepted Base transaction."""
        snapshot = InventoryOperationSnapshot(
            reference=reference,
            status=InventoryOperationStatus.PENDING,
            updated_at=Timestamp.now(),
            transaction_id=transaction_hash,
            quantity=reference.quantity,
        )
        return InventorySubmissionResult(
            InventorySubmissionStatus.ACCEPTED,
            reference,
            snapshot,
        )

    def _run(self, awaitable: Awaitable[_T]) -> _T:
        """Run one official SDK coroutine on the adapter-owned event loop."""
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            pass
        else:
            if hasattr(awaitable, "close"):
                awaitable.close()
            raise RuntimeError("Call the synchronous Limitless adapter outside an event loop")
        with self._runner_lock:
            if self._closed:
                if hasattr(awaitable, "close"):
                    awaitable.close()
                raise RuntimeError("Limitless inventory adapter is closed")
            return self._runner.run(awaitable)


def _to_base_units(quantity: Decimal, decimals: int) -> int:
    raw = quantity * (Decimal(10) ** decimals)
    if raw != raw.to_integral_value():
        raise ValueError(f"Limitless quantities support at most {decimals} decimal places")
    return int(raw)


def _bytes32(value: Any) -> bytes:
    text = str(value or "")
    if not text.startswith("0x") or len(text) != 66:
        raise ValueError("Limitless market has no valid 32-byte condition id")
    try:
        return bytes.fromhex(text[2:])
    except ValueError as error:
        raise ValueError("Limitless condition id must be hexadecimal") from error


def _hash_hex(value: Any) -> str:
    return "0x" + bytes(value).hex()


def _recovery_hash(reference: InventoryOperationReference) -> str:
    try:
        data = json.loads(reference.recovery_data)
    except (json.JSONDecodeError, UnicodeDecodeError) as error:
        raise ValueError("Invalid Limitless inventory recovery data") from error
    transaction_hash = data.get("transaction_hash")
    if (
        data.get("schema") != 1
        or not isinstance(transaction_hash, str)
        or not transaction_hash.startswith("0x")
        or len(transaction_hash) != 66
    ):
        raise ValueError("Limitless recovery data has no valid transaction hash")
    return transaction_hash


def _recovery_nonce(reference: InventoryOperationReference) -> int | None:
    """Return the signed nonce when newer recovery data contains it."""
    try:
        data = json.loads(reference.recovery_data)
    except (json.JSONDecodeError, UnicodeDecodeError):
        return None
    nonce = data.get("nonce")
    return nonce if isinstance(nonce, int) and nonce >= 0 else None


def _rpc_error(error: ValueError) -> str:
    payload = error.args[0] if error.args else error
    if isinstance(payload, dict):
        return str(payload.get("message") or payload)
    return str(payload)


def _gas_fee(receipt: Any) -> Money | None:
    """Return the exact native Base fee reported by a mined receipt."""
    gas_used = _value(receipt, "gasUsed", "gas_used")
    gas_price = _value(receipt, "effectiveGasPrice", "effective_gas_price")
    if gas_used is None or gas_price is None:
        return None
    return Money(
        Decimal(str(gas_used)) * Decimal(str(gas_price)) / Decimal(10**18),
        Currency("ETH"),
    )


def _receipt_timestamp(web3: Any, receipt: Any) -> Timestamp | None:
    """Return the mined block timestamp when the RPC exposes it."""
    block_number = _value(receipt, "blockNumber", "block_number")
    if block_number is None:
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


def _value(value: Any, *names: str) -> Any:
    for name in names:
        if isinstance(value, dict) and name in value:
            return value[name]
        if hasattr(value, name):
            return getattr(value, name)
    return None


def _require_venue(venue_id: Any) -> None:
    if venue_id != LIMITLESS_VENUE_ID:
        raise ValueError(f"Expected venue {LIMITLESS_VENUE_ID}, received {venue_id}")
