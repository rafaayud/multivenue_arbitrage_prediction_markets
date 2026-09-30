"""Execute recoverable Predict.fun orders through its REST API.

Responsibilities
----------------
- Sign orders before submission and serialize the exact request.
- Reconcile orders from their deterministic order hash.
- Normalize venue lifecycle, fills, and fees into domain snapshots.
"""

import base64
import binascii
import json
import logging
import os
import time
from dataclasses import replace
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from threading import Lock
from typing import Any
from uuid import uuid4

import httpx
from requests.exceptions import RequestException, Timeout as RequestsTimeout
from predict_sdk import (
    ApprovalScope,
    BuildOrderInput,
    ChainId,
    LimitHelperInput,
    OrderBuilder,
    OrderBuilderOptions,
    Side as PredictSide,
)

from prediction_markets.domain.ports.execution import ExecutionPort
from prediction_markets.domain.shared.value_objects import (
    ClientOrderID,
    ContractID,
    Currency,
    Money,
    OrderID,
    Price,
    Quantity,
    Timestamp,
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
from prediction_markets.domain.trading.value_objects import (
    OrderReference,
    PreparedOrder,
    ReconciliationResult,
    SubmissionResult,
    TradingFee,
)
from prediction_markets.infrastructure.http_client import instrumented_client
from prediction_markets.infrastructure.observability.execution_timing import (
    measure_phase,
    record_phase_elapsed,
)
from prediction_markets.infrastructure.observability.predict_fill_study import observe_submission
from prediction_markets.infrastructure.metrics import (
    ORDER_AUTH_READY,
    ORDER_AUTH_REFRESHES,
    ORDER_AUTH_TOKEN_TTL,
    ORDER_LATENCY,
    ORDER_CANCEL_ATTEMPTS,
)
from prediction_markets.infrastructure.venues.predict.config import (
    TRANSACTION_LOCK,
    predict_account_address,
    predict_api_key,
    predict_headers,
    predict_privy_private_key,
)
from prediction_markets.infrastructure.venues.predict.mappers import (
    PREDICT_NO_OUTCOME,
    PREDICT_VENUE_ID,
    parse_predict_contract_id,
)
from prediction_markets.infrastructure.venues.predict.taker_fees import predict_taker_fee

_PRECISION = 10**18
_JWT_REFRESH_LEEWAY_SECONDS = 30
_HTTP_KEEPALIVE_SECONDS = 60
_CHAIN_ANCHOR_REFRESH_SECONDS = 30
_CHAIN_ANCHOR_MAX_AGE_SECONDS = 60
_AUTH_REFRESH_REASONS = {"startup", "preload", "proactive", "manual"}


class PredictExecutionAdapter(ExecutionPort):
    """Execute price-protected LIMIT orders recoverable by order hash."""

    venue_id = PREDICT_VENUE_ID
    cancellation_transaction_lock = TRANSACTION_LOCK

    def __init__(
        self,
        privy_private_key: str | None = None,
        *,
        account_address: str | None = None,
        api_key: str | None = None,
        base_url: str = "https://api.predict.fun",
        timeout_seconds: float = 10.0,
        client: httpx.Client | None = None,
        order_builder: OrderBuilder | Any | None = None,
        limit_fok_enabled: bool | None = None,
        resting_window_ms: int | None = None,
        onchain_cancel_enabled: bool | None = None,
    ) -> None:
        """Configure signing and authenticated Predict REST access.

        Parameters
        ----------
        privy_private_key
            Privy signer private key. Defaults to ``PREDICT_PRIVY_PRIVATE_KEY``.
        account_address
            Predict Smart Wallet maker address. Defaults to
            ``PREDICT_ACCOUNT_ADDRESS``.
        api_key
            Mainnet API key. Testnet does not require one.
        base_url
            Predict API origin.
        timeout_seconds
            Positive HTTP timeout in seconds.
        client
            Optional caller-owned synchronous HTTP client.
        order_builder
            Optional Predict SDK builder, primarily for deterministic tests.
        limit_fok_enabled
            Opt-in for fill-or-kill semantics on every Predict LIMIT. Defaults
            to the disabled ``PREDICT_LIMIT_FOK_ENABLED`` rollout switch; when
            disabled, IOC intents use application-timed cancellation.
        resting_window_ms
            Positive application-owned LIMIT lifetime in milliseconds. Defaults
            to ``PREDICT_RESTING_WINDOW_MS`` or 1000.
        onchain_cancel_enabled
            Enable journaled on-chain cancellation for uncertain removed orders.
            Defaults to the disabled ``PREDICT_ONCHAIN_CANCEL_ENABLED`` switch.
        """
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        self._api_key = predict_api_key(api_key)
        key = predict_privy_private_key(privy_private_key)
        self._account_address = predict_account_address(account_address)
        if not key or not self._account_address:
            raise ValueError(
                "Set PREDICT_PRIVY_PRIVATE_KEY and PREDICT_ACCOUNT_ADDRESS "
                "to sign Predict Smart Wallet orders"
            )
        if not self._api_key and "api-testnet.predict.fun" not in base_url:
            raise ValueError("Predict mainnet execution requires PREDICT_API_KEY")

        self._base_url = base_url.rstrip("/")
        testnet = "api-testnet.predict.fun" in base_url
        self._order_builder = order_builder or OrderBuilder.make(
            ChainId.BNB_TESTNET if testnet else ChainId.BNB_MAINNET,
            key,
            OrderBuilderOptions(predict_account=self._account_address),
        )
        self._owns_client = client is None
        self._client = client or instrumented_client(
            str(self.venue_id).lower(),
            timeout=timeout_seconds,
            limits=httpx.Limits(keepalive_expiry=_HTTP_KEEPALIVE_SECONDS),
        )
        self._jwt: str | None = None
        self._jwt_expires_at: float | None = None
        self._auth_lock = Lock()
        self._markets: dict[str, dict[str, Any]] = {}
        self._limit_fok_enabled = (
            os.getenv("PREDICT_LIMIT_FOK_ENABLED", "0") == "1"
            if limit_fok_enabled is None
            else limit_fok_enabled
        )
        window_ms = (
            int(os.getenv("PREDICT_RESTING_WINDOW_MS", "1000"))
            if resting_window_ms is None
            else resting_window_ms
        )
        if window_ms <= 0:
            raise ValueError("Predict resting window must be positive")
        self._cancellation_window_seconds = window_ms / 1_000
        self._chain_cancellation = None
        self._chain_anchor_block: int | None = None
        self._chain_anchor_time = 0.0
        self._chain_preflight_ready = False
        if (os.getenv("PREDICT_ONCHAIN_CANCEL_ENABLED", "0") == "1"
                if onchain_cancel_enabled is None else onchain_cancel_enabled):
            from prediction_markets.infrastructure.venues.predict.cancellation import PredictOnChainCancellation

            self._chain_cancellation = PredictOnChainCancellation(
                self._order_builder,
                Decimal(os.getenv("PREDICT_ONCHAIN_CANCEL_MAX_FEE_BNB", "0.0001")),
            )
        self._set_auth_metrics()

    @property
    def supports_definitive_cancellation(self) -> bool:
        return self._chain_cancellation is not None

    def prepare_cancellation(self, order: PreparedOrder) -> bytes | None:
        """Sign a recovery-only cancellation for journaling before broadcast."""
        if self._chain_cancellation is None:
            return None
        return self._chain_cancellation.prepare(order)

    def submit_cancellation(
        self, order: PreparedOrder, request: bytes,
    ) -> ReconciliationResult:
        """Broadcast the journaled transaction and check finalized fill evidence."""
        if self._chain_cancellation is None:
            return ReconciliationResult(ReconciliationStatus.UNKNOWN, order.reference)
        try:
            self._chain_cancellation.broadcast(order.reference, request)
        except Exception as error:
            # An RPC timeout may follow broadcast. Reconcile; never sign a new nonce.
            ORDER_CANCEL_ATTEMPTS.labels("predict", "chain_broadcast_error").inc()
            status = getattr(getattr(error, "response", None), "status_code", None)
            status = status if type(status) is int and 100 <= status <= 599 else None
            if status is not None:
                reason = "http_403" if status == 403 else "http_error"
            elif isinstance(error, (TimeoutError, RequestsTimeout, httpx.TimeoutException)):
                reason = "timeout"
            else:
                reason = "rpc_error"
            ORDER_CANCEL_ATTEMPTS.labels("predict", f"chain_broadcast_{reason}").inc()
            logging.getLogger(__name__).warning(
                "Predict cancellation broadcast failed (%s, HTTP status=%s); "
                "reconciling the persisted transaction", type(error).__name__, status,
            )
        recovery = _recovery_data(order.reference)
        try:
            snapshot = self._chain_cancellation.reconcile(order.reference,
                _snapshot_from_reference(order.reference, order_id=OrderID(recovery["order_hash"])))
        except Exception:
            ORDER_CANCEL_ATTEMPTS.labels("predict", "chain_proof_error").inc()
            return ReconciliationResult(ReconciliationStatus.UNKNOWN, order.reference)
        if snapshot.settlement_finalized_block is None:
            return ReconciliationResult(ReconciliationStatus.UNKNOWN, order.reference)
        return ReconciliationResult(ReconciliationStatus.FOUND, order.reference, snapshot)

    def cancellation_window_seconds(self, intent: OrderIntent) -> float | None:
        """Return the lifetime of Predict's non-FOK resting LIMIT.

        Parameters
        ----------
        intent
            Prepared IOC or FOK intent.

        Returns
        -------
        float | None
            Configured lifetime, or ``None`` when fill-or-kill is guaranteed.
        """
        if self._limit_fok_enabled or intent.time_in_force is TimeInForce.FOK:
            return None
        return self._cancellation_window_seconds

    def close(self) -> None:
        """Close the owned HTTP client; injected clients remain caller-owned."""
        if self._owns_client:
            self._client.close()
            self._owns_client = False
        with self._auth_lock:
            self._jwt = None
            self._jwt_expires_at = None
            self._set_auth_metrics_locked()

    def preload(self, contract_ids: tuple[ContractID, ...]) -> None:
        """Load signing metadata and warm authentication off the hot path.

        Parameters
        ----------
        contract_ids
            Predict contracts whose market metadata is required for signing.
        """
        for market_id in dict.fromkeys(
            parse_predict_contract_id(contract_id)[0] for contract_id in contract_ids
        ):
            self._market(market_id)
        self.refresh_auth(reason="preload")
        self.refresh_cancellation_anchor()

    def refresh_cancellation_anchor(self) -> None:
        """Refresh the bounded chain scan anchor outside order preparation."""
        if self._chain_cancellation is not None:
            with self.cancellation_transaction_lock:
                if (
                    self._chain_anchor_block is not None
                    and time.monotonic() - self._chain_anchor_time
                    < _CHAIN_ANCHOR_REFRESH_SECONDS
                ):
                    return

                def refresh() -> None:
                    if not self._chain_preflight_ready:
                        self._chain_cancellation.preflight()
                        self._chain_preflight_ready = True
                    self._chain_anchor_block = self._chain_cancellation.anchor()
                    self._chain_anchor_time = time.monotonic()

                try:
                    refresh()
                except RequestException:
                    self._chain_cancellation.reconnect()
                    self._chain_preflight_ready = False
                    refresh()

    @ORDER_LATENCY.labels("predict", "prepare").time()
    def prepare(self, intent: OrderIntent) -> PreparedOrder:
        """Build and serialize one signed price-protected arbitrage order.

        Parameters
        ----------
        intent
            LIMIT IOC or FOK intent. IOC remains a non-post-only venue LIMIT
            until the application cancels it after its short resting window.

        Returns
        -------
        PreparedOrder
            Exact request and deterministic order-hash recovery reference.

        Raises
        ------
        NotImplementedError
            If the intent is not a LIMIT IOC or FOK request.
        RuntimeError
            If market metadata or a ready cached JWT is unavailable.

        Notes
        -----
        - Market discovery is restricted to :meth:`preload`; this method signs
          from the in-memory cache and performs no venue request.
        - A missing or expiring JWT fails closed instead of authenticating here.
        - Recovery references retain the signed share quantity after truncation.
        """
        if intent.order_type is not OrderType.LIMIT:
            raise NotImplementedError("Predict arbitrage execution requires LIMIT orders")
        if intent.time_in_force not in {TimeInForce.IOC, TimeInForce.FOK}:
            raise NotImplementedError("Predict arbitrage execution requires IOC or FOK")

        market_id, outcome = parse_predict_contract_id(intent.contract_id)
        market = self._markets.get(market_id)
        if market is None:
            raise RuntimeError(
                f"Predict market {market_id} was not preloaded before order preparation",
            )
        self._ready_token()
        signed_order, price_wei, order_hash = self._signed_order(
            intent,
            market,
            _token_id(market, outcome),
        )
        signed_order["hash"] = order_hash
        client_order_id = intent.client_order_id or ClientOrderID(uuid4().hex)
        recovery = {
            "schema": 1,
            "order_hash": order_hash,
            "contract_id": str(intent.contract_id),
            "side": intent.side.value,
            "quantity": str(
                Decimal(signed_order["takerAmount" if intent.side is OrderSide.BUY else "makerAmount"])
                / _PRECISION
            ),
            "limit_price": str(intent.limit_price.value),
            "created_at": (
                intent.created_at.value.isoformat() if intent.created_at else None
            ),
        }
        if self._chain_cancellation is not None:
            if (
                self._chain_anchor_block is None
                or time.monotonic() - self._chain_anchor_time
                > _CHAIN_ANCHOR_MAX_AGE_SECONDS
            ):
                raise RuntimeError("Predict cancellation metadata was not preloaded")
            recovery["chain"] = {
                "from_block": self._chain_anchor_block,
                "is_neg_risk": bool(market.get("isNegRisk")),
                "is_yield_bearing": bool(market.get("isYieldBearing")),
                "order": signed_order,
            }
        request = {
            "data": {
                "order": signed_order,
                "pricePerShare": str(price_wei),
                "strategy": "LIMIT",
                "isFillOrKill": (
                    True
                    if self._limit_fok_enabled
                    else intent.time_in_force is TimeInForce.FOK
                ),
                "isPostOnly": False,
            }
        }
        with measure_phase("predict_payload_serialization"):
            return PreparedOrder(
                reference=OrderReference(
                    venue_id=PREDICT_VENUE_ID,
                    client_order_id=client_order_id,
                    recovery_data=json.dumps(recovery, separators=(",", ":")).encode(),
                ),
                request=json.dumps(request, separators=(",", ":")).encode(),
            )

    def get_available_collateral(
        self,
        contract_ids: tuple[ContractID, ...],
    ) -> Decimal | None:
        """Read Predict USDT constrained by current BUY approval scopes.

        Parameters
        ----------
        contract_ids
            Current Predict contracts whose market approval spenders must be
            covered.

        Returns
        -------
        Decimal | None
            Spendable USDT, or ``None`` when no Predict contract is active.

        Raises
        ------
        RuntimeError
            If SDK contract access or a required BUY allowance is unavailable.
        """
        market_ids = tuple(
            dict.fromkeys(
                parse_predict_contract_id(contract_id)[0]
                for contract_id in contract_ids
            ),
        )
        if not market_ids:
            return None
        contracts = self._order_builder.contracts
        if contracts is None:
            raise RuntimeError("Predict USDT contract access is unavailable")
        spenders = []
        for market_id in market_ids:
            market = self._market(market_id)
            steps = self._order_builder.get_approval_steps(
                ApprovalScope(
                    operation="TRADE",
                    is_neg_risk=bool(market.get("isNegRisk")),
                    is_yield_bearing=bool(market.get("isYieldBearing")),
                    side=PredictSide.BUY,
                ),
            )
            allowance_step = next(
                (step for step in steps if step.type == "ERC20_ALLOWANCE"),
                None,
            )
            if allowance_step is None:
                raise RuntimeError("Predict SDK returned no BUY collateral allowance")
            spenders.append(allowance_step.spender)
        balance = int(
            self._order_builder.balance_of(address=self._account_address),
        )
        allowances = (
            int(
                contracts.usdt.functions.allowance(
                    self._account_address,
                    spender,
                ).call(),
            )
            for spender in dict.fromkeys(spenders)
        )
        return Decimal(min(balance, *allowances)) / _PRECISION

    @ORDER_LATENCY.labels("predict", "submit").time()
    def submit(self, order: PreparedOrder) -> SubmissionResult:
        """Submit the persisted signed payload without rebuilding it.

        Parameters
        ----------
        order
            Prepared request previously persisted by the caller.

        Returns
        -------
        SubmissionResult
            Submission certainty and initial snapshot when accepted.
        """
        observe_submission(order.reference)
        recovery = _recovery_data(order.reference)
        payload = json.loads(order.request)
        try:
            response = self._request("POST", "/v1/orders", json=payload)
            data = _response_data(response.json(), dict)
        except httpx.HTTPStatusError as error:
            status_code = error.response.status_code
            if status_code >= 500 or status_code in {408, 429}:
                found = self.reconcile(order.reference)
                if found.status is ReconciliationStatus.FOUND:
                    return SubmissionResult(
                        SubmissionStatus.ACCEPTED,
                        order.reference,
                        found.snapshot,
                    )
            return SubmissionResult(
                status=(
                    SubmissionStatus.REJECTED
                    if 400 <= status_code < 500 and status_code not in {408, 429}
                    else SubmissionStatus.UNKNOWN
                ),
                reference=order.reference,
                reason=_error_reason(error.response),
            )
        except (httpx.RequestError, OSError):
            return SubmissionResult(SubmissionStatus.UNKNOWN, order.reference)
        except (TypeError, RuntimeError) as error:
            return SubmissionResult(
                SubmissionStatus.REJECTED,
                order.reference,
                reason=str(error),
            )

        venue_hash = str(data.get("orderHash") or recovery["order_hash"])
        if venue_hash.lower() != recovery["order_hash"].lower():
            return SubmissionResult(
                SubmissionStatus.UNKNOWN,
                order.reference,
                reason="Predict returned a different order hash",
            )
        return SubmissionResult(
            SubmissionStatus.ACCEPTED,
            order.reference,
            _snapshot_from_reference(order.reference, order_id=OrderID(venue_hash)),
        )

    @ORDER_LATENCY.labels("predict", "get").time()
    def reconcile(self, reference: OrderReference) -> ReconciliationResult:
        """Resolve an order using only its persisted deterministic hash.

        Parameters
        ----------
        reference
            Durable reference produced by :meth:`prepare`.

        Returns
        -------
        ReconciliationResult
            Authoritative venue state, proven absence, or uncertainty.

        Notes
        -----
        - A terminal REST snapshot remains observable when optional on-chain
          finality is unavailable. Its ``may_receive_more_fills`` value keeps
          settlement uncertainty explicit until chain reconciliation succeeds.
        """
        recovery = _recovery_data(reference)
        try:
            response = self._request("GET", f"/v1/orders/{recovery['order_hash']}")
            raw = _response_data(response.json(), dict)
            snapshot = self._enrich_fills(_snapshot_from_order(reference, raw), raw)
        except Exception:
            snapshot = None
        if (self._chain_cancellation is not None and recovery.get("chain")
                and (snapshot is None or snapshot.is_terminal())):
            try:
                candidate = self._chain_cancellation.reconcile(
                    reference, snapshot or _snapshot_from_reference(
                        reference, order_id=OrderID(recovery["order_hash"]),
                    ),
                )
                if candidate.settlement_finalized_block is not None:
                    snapshot = candidate
            except Exception:
                ORDER_CANCEL_ATTEMPTS.labels("predict", "chain_proof_error").inc()
                if snapshot is None:
                    return ReconciliationResult(ReconciliationStatus.UNKNOWN, reference)
        return ReconciliationResult(
            (
                ReconciliationStatus.FOUND
                if snapshot is not None
                else ReconciliationStatus.UNKNOWN
            ),
            reference,
            snapshot,
        )

    @ORDER_LATENCY.labels("predict", "cancel").time()
    def cancel(self, reference: OrderReference) -> ReconciliationResult:
        """Remove an arbitrage order from Predict's off-chain order book.

        Parameters
        ----------
        reference
            Durable reference produced by :meth:`prepare`.

        Returns
        -------
        ReconciliationResult
            Confirmed removed state, current terminal state, or uncertainty.

        Notes
        -----
        - The adapter re-reads both the order and its matches after removal so a
          concurrent fill cannot be overwritten by a synthetic cancellation.
        """
        recovery = _recovery_data(reference)
        try:
            response = self._request("GET", f"/v1/orders/{recovery['order_hash']}")
            raw = _response_data(response.json(), dict)
            snapshot = self._enrich_fills(_snapshot_from_order(reference, raw), raw)
        except httpx.HTTPStatusError:
            return ReconciliationResult(ReconciliationStatus.UNKNOWN, reference)
        except (httpx.RequestError, OSError, TypeError, RuntimeError, ValueError, InvalidOperation):
            return ReconciliationResult(ReconciliationStatus.UNKNOWN, reference)
        if snapshot is None:
            return ReconciliationResult(ReconciliationStatus.UNKNOWN, reference)
        if snapshot.is_terminal():
            return ReconciliationResult(ReconciliationStatus.FOUND, reference, snapshot)
        api_order_id = str(raw.get("id") or "")
        if not api_order_id:
            return ReconciliationResult(ReconciliationStatus.UNKNOWN, reference)
        try:
            payload = self._request(
                "POST",
                "/v1/orders/remove",
                json={"data": {"ids": [api_order_id]}},
            ).json()
        except (httpx.HTTPError, OSError):
            return ReconciliationResult(ReconciliationStatus.UNKNOWN, reference)
        if not isinstance(payload, dict) or payload.get("success") is not True:
            return ReconciliationResult(ReconciliationStatus.UNKNOWN, reference)
        removed = {str(value) for value in payload.get("removed", ())}
        noop = {str(value) for value in payload.get("noop", ())}
        if api_order_id in removed or api_order_id in noop:
            return self.reconcile(reference)
        return ReconciliationResult(ReconciliationStatus.UNKNOWN, reference)

    def auth_token(self) -> str:
        """Return the prewarmed wallet JWT for a private WebSocket connection.

        Returns
        -------
        str
            Bearer token used by the private wallet-event topic.
        """
        return self._ready_token()

    def refresh_auth(
        self,
        *,
        reason: str = "manual",
        force: bool = False,
    ) -> str:
        """Warm or proactively renew the cached Predict wallet JWT.

        Parameters
        ----------
        reason
            Bounded lifecycle reason used by authentication metrics.
        force
            Whether to replace a token that is still outside its refresh window.

        Returns
        -------
        str
            Cached or newly issued JWT.

        Notes
        -----
        - Callers must invoke this from startup, preload, or a background task;
          order preparation and submission never call it.
        """
        metric_reason = reason if reason in _AUTH_REFRESH_REASONS else "manual"
        with self._auth_lock:
            if not force and self._token_ready_locked(time.time()):
                self._set_auth_metrics_locked()
                assert self._jwt is not None
                return self._jwt
            try:
                headers = predict_headers(self._api_key)
                response = self._client.get(
                    f"{self._base_url}/v1/auth/message",
                    headers=headers,
                )
                response.raise_for_status()
                message = str(_response_data(response.json(), dict)["message"])
                response = self._client.post(
                    f"{self._base_url}/v1/auth",
                    headers=headers,
                    json={
                        "signer": self._account_address,
                        "signature": self._order_builder.sign_predict_account_message(
                            message
                        ),
                        "message": message,
                    },
                )
                response.raise_for_status()
                self._jwt = str(_response_data(response.json(), dict)["token"])
                self._jwt_expires_at = _jwt_expiry(self._jwt)
            except Exception as error:
                ORDER_AUTH_REFRESHES.labels(
                    str(self.venue_id).lower(),
                    metric_reason,
                    _auth_failure_kind(error),
                ).inc()
                self._set_auth_metrics_locked()
                raise
            ORDER_AUTH_REFRESHES.labels(
                str(self.venue_id).lower(),
                metric_reason,
                "success",
            ).inc()
            self._set_auth_metrics_locked()
            assert self._jwt is not None
            return self._jwt

    def _enrich_fills(
        self,
        snapshot: OrderSnapshot,
        raw_order: dict[str, Any],
    ) -> OrderSnapshot | None:
        """Reconcile non-regressing fills against Predict's matches endpoint.

        Notes
        -----
        - Empty or unavailable matches do not prove zero execution. Cancellation
          can precede settlement visibility; only confirmed full quantity is
          final here. Explicit no-match rejection is handled by the private feed.
        """
        if snapshot.order_id is None:
            return snapshot
        try:
            response = self._request(
                "GET",
                "/v1/orders/matches",
                params={
                    "first": 100,
                    "marketId": str(raw_order.get("marketId") or ""),
                    "signerAddress": self._account_address,
                },
            )
            matches = _response_data(response.json(), list)
        except (httpx.HTTPError, OSError, TypeError, RuntimeError):
            return (
                None
                if snapshot.is_terminal() and snapshot.filled_quantity.value <= 0
                else snapshot
            )

        fills: list[tuple[Decimal, Decimal, Decimal, str]] = []
        seen_matches: set[str] = set()
        for match in matches:
            if not isinstance(match, dict):
                continue
            identity = json.dumps(match, sort_keys=True)
            if identity in seen_matches:
                continue
            seen_matches.add(identity)
            participants = (match.get("taker"), *(match.get("makers") or ()))
            own_fill = next(
                (
                    fill
                    for fill in participants
                    if isinstance(fill, dict)
                    and str(fill.get("hash") or "").lower()
                    == str(snapshot.order_id).lower()
                ),
                None,
            )
            if own_fill is None:
                continue
            try:
                is_taker = own_fill is match.get("taker")
                quantity = _external_decimal(
                    match.get("amountFilled") if is_taker else own_fill.get("amount"), wei=True,
                )
                price = _external_decimal(
                    match.get("priceExecuted") if is_taker else own_fill.get("price"), price=True,
                )
                fee = own_fill.get("fee")
                fee_amount = (
                    _external_decimal(fee.get("amount"), wei=True)
                    if isinstance(fee, dict)
                    else Decimal("0")
                )
            except (InvalidOperation, TypeError, ValueError):
                continue
            fee_type = str(fee.get("type") or "") if isinstance(fee, dict) else ""
            if quantity > 0 and Decimal("0") <= price <= Decimal("1"):
                fills.append((quantity, price, fee_amount, fee_type))
        matched_filled = sum(
            (quantity for quantity, _, _, _ in fills),
            Decimal("0"),
        )
        if matched_filled < snapshot.filled_quantity.value or matched_filled <= 0:
            return snapshot
        average = Price(
            sum((quantity * price for quantity, price, _, _ in fills), Decimal("0"))
            / matched_filled
        )
        fee_amount = sum((amount for _, _, amount, _ in fills), Decimal("0"))
        fee_types = {fee_type for _, _, _, fee_type in fills if fee_type}
        fee = snapshot.fee
        if len(fee_types) == 1:
            fee_currency = Currency(
                "OUTCOME_TOKEN" if fee_types.pop() == "SHARES" else "USDT"
            )
            fee = TradingFee(
                charged=Money(fee_amount, fee_currency),
                settlement_cost=Money(fee_amount, Currency("USD")),
            )
        filled = Quantity(min(matched_filled, snapshot.quantity.value))
        status = _order_status(
            str(raw_order.get("status") or ""),
            snapshot.quantity,
            filled,
        )
        return replace(
            snapshot,
            status=status,
            filled_quantity=filled,
            average_price=average,
            fee=fee,
            may_receive_more_fills=filled.value < snapshot.quantity.value,
        )

    def _signed_order(
        self,
        intent: OrderIntent,
        market: dict[str, Any],
        token_id: str,
    ) -> tuple[dict[str, Any], int, str]:
        """Build and sign the Predict venue order payload."""
        price_wei = _retain_significant_digits(_to_wei(intent.limit_price.value), 3)
        quantity_wei = _retain_significant_digits(_to_wei(intent.quantity.value), 5)
        if quantity_wei < 10**16:
            raise ValueError("Predict orders require at least 0.01 shares")
        side = PredictSide.BUY if intent.side is OrderSide.BUY else PredictSide.SELL
        with measure_phase("predict_sdk_build"):
            amounts = self._order_builder.get_limit_order_amounts(
                LimitHelperInput(
                    side=side,
                    price_per_share_wei=price_wei,
                    quantity_wei=quantity_wei,
                )
            )
            expires_at = (
                datetime.fromtimestamp(intent.expires_at.value.timestamp(), timezone.utc)
                if intent.expires_at
                else None
            )
            order = self._order_builder.build_order(
                "LIMIT",
                BuildOrderInput(
                    side=side,
                    token_id=token_id,
                    maker_amount=amounts.maker_amount,
                    taker_amount=amounts.taker_amount,
                    fee_rate_bps=int(market.get("feeRateBps") or 0),
                    expires_at=expires_at,
                ),
            )
            typed_data = self._order_builder.build_typed_data(
                order,
                is_neg_risk=bool(market.get("isNegRisk")),
                is_yield_bearing=bool(market.get("isYieldBearing")),
            )
        with measure_phase("predict_sdk_sign"):
            signed = self._order_builder.sign_typed_data_order(typed_data)
        with measure_phase("predict_payload_serialization"):
            payload = _signed_order_payload(signed)
        price_per_share = amounts.price_per_share
        with measure_phase("predict_sdk_hash"):
            order_hash = self._order_builder.build_typed_data_hash(typed_data)
        return payload, price_per_share, order_hash

    def _market(self, market_id: str) -> dict[str, Any]:
        """Fetch and cache one market's signing metadata."""
        if market_id not in self._markets:
            response = self._client.get(
                f"{self._base_url}/v1/markets/{market_id}",
                headers=predict_headers(self._api_key),
            )
            response.raise_for_status()
            self._markets[market_id] = _response_data(response.json(), dict)
        return self._markets[market_id]

    def _request(self, method: str, path: str, **kwargs: Any) -> httpx.Response:
        """Send one request with a prewarmed JWT and never authenticate inline."""
        token = self._ready_token()
        response = self._client.request(
            method,
            f"{self._base_url}{path}",
            headers={
                **predict_headers(self._api_key),
                "Authorization": f"Bearer {token}",
            },
            **kwargs,
        )
        if response.status_code == 401:
            with self._auth_lock:
                if self._jwt == token:
                    self._jwt = None
                    self._jwt_expires_at = None
                    self._set_auth_metrics_locked()
        response.raise_for_status()
        return response

    def _ready_token(self) -> str:
        """Return an unexpired cached JWT without performing external I/O."""
        lock_started_at_ns = time.monotonic_ns()
        with self._auth_lock:
            record_phase_elapsed("predict_cached_jwt_lock_wait", lock_started_at_ns)
            with measure_phase("predict_cached_jwt_check"):
                if not self._token_ready_locked(time.time()):
                    self._set_auth_metrics_locked()
                    raise RuntimeError("Predict authentication is not prewarmed")
                self._set_auth_metrics_locked()
                assert self._jwt is not None
                return self._jwt

    def _token_ready_locked(self, now: float) -> bool:
        return self._jwt is not None and (
            self._jwt_expires_at is None
            or now + _JWT_REFRESH_LEEWAY_SECONDS < self._jwt_expires_at
        )

    def _set_auth_metrics(self) -> None:
        with self._auth_lock:
            self._set_auth_metrics_locked()

    def _set_auth_metrics_locked(self) -> None:
        now = time.time()
        venue = str(self.venue_id).lower()
        ORDER_AUTH_READY.labels(venue).set(
            1 if self._token_ready_locked(now) else 0
        )
        ORDER_AUTH_TOKEN_TTL.labels(venue).set(
            max(0.0, self._jwt_expires_at - now)
            if self._jwt_expires_at is not None
            else -1
        )


def _jwt_expiry(token: str) -> float | None:
    """Read unverified JWT expiry only to schedule preventive refresh."""
    try:
        payload = token.split(".")[1]
        payload += "=" * (-len(payload) % 4)
        expires_at = json.loads(base64.urlsafe_b64decode(payload)).get("exp")
        return float(expires_at) if expires_at is not None else None
    except (
        AttributeError,
        binascii.Error,
        IndexError,
        TypeError,
        UnicodeDecodeError,
        ValueError,
    ):
        return None


def _auth_failure_kind(error: Exception) -> str:
    """Map authentication failures to bounded metric labels."""
    if isinstance(error, httpx.HTTPStatusError):
        return "http_status"
    if isinstance(error, (httpx.RequestError, OSError)):
        return "transport"
    return "invalid_response"


def _recovery_data(reference: OrderReference) -> dict[str, Any]:
    if reference.venue_id != PREDICT_VENUE_ID:
        raise ValueError(f"Predict cannot handle venue {reference.venue_id}")
    data = json.loads(reference.recovery_data)
    required = {"order_hash", "contract_id", "side", "quantity", "limit_price"}
    if data.get("schema") != 1 or not required <= data.keys():
        raise ValueError("Invalid Predict recovery data")
    return data


def _snapshot_from_reference(
    reference: OrderReference,
    *,
    order_id: OrderID,
) -> OrderSnapshot:
    data = _recovery_data(reference)
    return OrderSnapshot(
        status=OrderStatus.SUBMITTED,
        contract_id=ContractID(data["contract_id"]),
        side=OrderSide(data["side"]),
        quantity=Quantity(Decimal(data["quantity"])),
        order_type=OrderType.LIMIT,
        client_order_id=reference.client_order_id,
        order_id=order_id,
        limit_price=Price(Decimal(data["limit_price"])),
        created_at=(
            Timestamp.from_iso(data["created_at"]) if data.get("created_at") else None
        ),
        updated_at=Timestamp.now(),
        may_receive_more_fills=True,
    )


def _snapshot_from_order(
    reference: OrderReference,
    raw: dict[str, Any],
) -> OrderSnapshot:
    """Normalize an order response without treating removal as fill finality."""
    recovery = _recovery_data(reference)
    order = raw.get("order")
    if not isinstance(order, dict):
        raise TypeError("Predict order response has no order")
    if str(order.get("hash") or "").lower() != recovery["order_hash"].lower():
        raise ValueError("Predict order response belongs to a different hash")
    side = OrderSide.BUY if int(order.get("side") or 0) == 0 else OrderSide.SELL
    shares_wei = int(
        order.get("takerAmount") if side is OrderSide.BUY else order.get("makerAmount")
    )
    collateral_wei = int(
        order.get("makerAmount") if side is OrderSide.BUY else order.get("takerAmount")
    )
    if shares_wei <= 0:
        raise ValueError("Predict order has no share quantity")
    quantity = Quantity(Decimal(shares_wei) / _PRECISION)
    price = Price(Decimal(collateral_wei) / shares_wei)
    filled = Quantity(Decimal(str(raw.get("amountFilled") or 0)) / _PRECISION)
    fee = (
        predict_taker_fee(int(order.get("feeRateBps") or 0), price, filled, side)
        if filled.value > 0
        else None
    )
    status = _order_status(str(raw.get("status") or ""), quantity, filled)
    return OrderSnapshot(
        status=status,
        contract_id=ContractID(recovery["contract_id"]),
        side=side,
        quantity=quantity,
        order_type=OrderType.LIMIT,
        client_order_id=reference.client_order_id,
        order_id=OrderID(str(order.get("hash") or recovery["order_hash"])),
        limit_price=price,
        filled_quantity=filled,
        average_price=price if filled.value > 0 else None,
        fee=fee,
        created_at=(
            Timestamp.from_iso(recovery["created_at"])
            if recovery.get("created_at")
            else None
        ),
        updated_at=Timestamp.now(),
        may_receive_more_fills=filled.value < quantity.value,
    )


def _response_data(payload: Any, expected: type) -> Any:
    """Validate a Predict response envelope and return its typed data field."""
    if not isinstance(payload, dict) or payload.get("success") is not True:
        raise RuntimeError(f"Predict rejected request: {payload}")
    data = payload.get("data")
    if not isinstance(data, expected):
        raise TypeError(
            f"Unexpected Predict data: expected {expected.__name__}, "
            f"got {type(data).__name__}"
        )
    return data


def _error_reason(response: httpx.Response) -> str:
    try:
        payload = response.json()
    except ValueError:
        return f"HTTP {response.status_code}"
    return f"HTTP {response.status_code}: {payload}"


def _external_decimal(
    value: Any,
    *,
    wei: bool = False,
    price: bool = False,
) -> Decimal:
    """Normalize decimal API fields while tolerating legacy Wei payloads."""
    parsed = Decimal(str(value or 0))
    if not parsed.is_finite():
        raise ValueError("Predict numeric fields must be finite")
    if (wei or price) and abs(parsed) >= Decimal("1000000000000"):
        parsed /= Decimal(_PRECISION)
    return parsed


def _to_wei(value: Decimal) -> int:
    scaled = value * _PRECISION
    if scaled != scaled.to_integral_value():
        raise ValueError("Predict amounts support at most 18 decimal places")
    return int(scaled)


def _retain_significant_digits(value: int, digits: int) -> int:
    excess = len(str(abs(value))) - digits
    if excess <= 0:
        return value
    divisor = 10**excess
    return value // divisor * divisor


def _signed_order_payload(order: Any) -> dict[str, Any]:
    return {
        "salt": str(order.salt),
        "maker": order.maker,
        "signer": order.signer,
        "taker": order.taker,
        "tokenId": str(order.token_id),
        "makerAmount": str(order.maker_amount),
        "takerAmount": str(order.taker_amount),
        "expiration": str(order.expiration),
        "nonce": str(order.nonce),
        "feeRateBps": str(order.fee_rate_bps),
        "side": order.side.value,
        "signatureType": order.signature_type.value,
        "signature": order.signature,
    }


def _token_id(market: dict[str, Any], outcome: str) -> str:
    """Resolve the venue token identifier across supported payload shapes."""
    index_set = 2 if outcome == PREDICT_NO_OUTCOME else 1
    for raw in market.get("outcomes") or ():
        if isinstance(raw, dict) and raw.get("indexSet") == index_set:
            token_id = str(raw.get("onChainId") or "")
            if token_id:
                return token_id
    raise RuntimeError(f"Predict market has no {outcome.upper()} token")


def _order_status(
    raw_status: str,
    quantity: Quantity,
    filled: Quantity,
) -> OrderStatus:
    """Map Predict lifecycle state without regressing known fill quantity."""
    if filled.value >= quantity.value:
        return OrderStatus.FILLED
    status = {
        "OPEN": OrderStatus.ACCEPTED,
        "PENDING": OrderStatus.SUBMITTED,
        "FILLED": OrderStatus.SUBMITTED,
        "MATCHED": OrderStatus.SUBMITTED,
        "CANCELLED": OrderStatus.CANCELLED,
        "CANCELED": OrderStatus.CANCELLED,
        "REMOVED": OrderStatus.CANCELLED,
        "EXPIRED": OrderStatus.EXPIRED,
        "REJECTED": OrderStatus.REJECTED,
        "INVALID": OrderStatus.REJECTED,
    }.get(raw_status.upper(), OrderStatus.SUBMITTED)
    if status in {
        OrderStatus.FILLED,
        OrderStatus.CANCELLED,
        OrderStatus.REJECTED,
        OrderStatus.EXPIRED,
    }:
        return status
    if filled.value > 0:
        return OrderStatus.PARTIALLY_FILLED
    return status
