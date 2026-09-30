"""Execute recoverable Limitless orders through the official SDK transport.

Responsibilities
----------------
- Build and sign exact venue payloads before submission.
- Correlate submission, reconciliation, and cancellation by client order id.
- Normalize Limitless responses into domain snapshots.
- Enrich snapshots with fill totals, average price, and fees from execution data.
"""

import asyncio
import json
import os
from collections.abc import Awaitable, Callable
from dataclasses import replace
from decimal import Decimal
from threading import Lock
from typing import Any, TypeVar
from uuid import uuid4

import aiohttp
from eth_account import Account
from limitless_sdk.api import HttpClient
from limitless_sdk.api.errors import APIError
from limitless_sdk.markets import MarketFetcher
from limitless_sdk.orders import OrderClient
from limitless_sdk.types import HMACCredentials, OrderSigningConfig, SignedOrder
from limitless_sdk.types import OrderType as LimitlessOrderType
from limitless_sdk.types import Side as LimitlessSide
from web3 import Web3

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
from prediction_markets.infrastructure.venues.limitless.mappers import (
    LIMITLESS_VENUE_ID,
    parse_limitless_contract_id,
)
from prediction_markets.infrastructure.metrics import ORDER_LATENCY

_T = TypeVar("_T")
_TIME_IN_FORCE = {
    TimeInForce.GTC: LimitlessOrderType.GTC,
    TimeInForce.GTD: LimitlessOrderType.GTC,
    TimeInForce.IOC: LimitlessOrderType.FAK,
}
_USDC_ADDRESS = "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913"
_USDC_READ_ABI = [
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
]


def _base_usdc_reader(
    rpc_url: str | None,
) -> Callable[[str, str], tuple[int, int]] | None:
    """Build a lazy Base USDC balance and allowance reader.

    Parameters
    ----------
    rpc_url
        Base JSON-RPC endpoint, or ``None`` when collateral cannot be verified.

    Returns
    -------
    Callable[[str, str], tuple[int, int]] | None
        Reader accepting owner and exchange addresses and returning six-decimal
        balance and allowance base units, or ``None`` without an endpoint.
    """
    if not rpc_url:
        return None
    web3 = Web3(Web3.HTTPProvider(rpc_url, request_kwargs={"timeout": 10}))
    usdc = web3.eth.contract(
        address=web3.to_checksum_address(_USDC_ADDRESS),
        abi=_USDC_READ_ABI,
    )

    def read(owner: str, spender: str) -> tuple[int, int]:
        return (
            int(usdc.functions.balanceOf(owner).call()),
            int(usdc.functions.allowance(owner, spender).call()),
        )

    return read


async def _sign_order(signer: Any, order: Any, config: OrderSigningConfig) -> str:
    """
    Sign one Limitless order without logging its sensitive result.

    Parameters
    ----------
    signer
        SDK signer owning the private signing material.
    order
        Unsigned venue order.
    config
        Chain and exchange contract used for the signature.

    Returns
    -------
    str
        Native order signature.
    """
    return await signer.sign_order(order, config)


class LimitlessFillEnricher:
    """Attach fill totals, average price, and fees from Limitless execution data."""

    def enrich(self, snapshot: OrderSnapshot, data: Any) -> OrderSnapshot:
        """
        Merge Limitless order and execution states into one normalized snapshot.

        Parameters
        ----------
        snapshot
            Base snapshot built from the durable order reference.
        data
            Venue create or status payload containing ``order`` and ``execution``.

        Returns
        -------
        OrderSnapshot
            Snapshot with status, filled quantity, average price, and fee updated
            from the venue payload when present.
        """
        if not isinstance(data, dict):
            return snapshot
        envelope = data.get("order") if isinstance(data.get("order"), dict) else data
        order = (
            envelope.get("order")
            if isinstance(envelope, dict) and isinstance(envelope.get("order"), dict)
            else envelope
        )
        execution = data.get("execution") or (
            envelope.get("execution") if isinstance(envelope, dict) else None
        )
        execution = execution if isinstance(execution, dict) else {}
        filled_quantity, average_price = self._execution_totals(execution, snapshot)
        status = _order_status(order.get("status") if isinstance(order, dict) else None)
        settlement = str(execution.get("settlementStatus") or "").upper()
        if settlement in {"CANCELED", "CANCELLED", "KILLED"}:
            status = OrderStatus.CANCELLED
        elif settlement in {"UNMATCHED", "FAILED"}:
            status = OrderStatus.CANCELLED if filled_quantity.value > 0 else OrderStatus.REJECTED
        elif filled_quantity.value >= snapshot.quantity.value:
            status = OrderStatus.FILLED
        elif filled_quantity.value > 0:
            status = OrderStatus.PARTIALLY_FILLED
        elif execution.get("matched") is True or status is OrderStatus.FILLED:
            status = OrderStatus.FILLED
            filled_quantity = snapshot.quantity
            average_price = snapshot.limit_price

        return replace(
            snapshot,
            status=status,
            order_id=_response_order_id(data, required=False) or snapshot.order_id,
            filled_quantity=filled_quantity,
            average_price=average_price,
            fee=self._execution_fee(execution, snapshot.side) or snapshot.fee,
            reason=(
                _limitless_response_reason(data) or snapshot.reason
                if status in {OrderStatus.CANCELLED, OrderStatus.REJECTED}
                else None
            ),
            updated_at=Timestamp.now(),
        )

    def _execution_totals(
        self,
        execution: dict[str, Any],
        snapshot: OrderSnapshot,
    ) -> tuple[Quantity, Price | None]:
        """Derive filled quantity and average price from ``totalsRaw``."""
        totals = execution.get("totalsRaw")
        if not isinstance(totals, dict):
            return snapshot.filled_quantity, snapshot.average_price
        contracts = Decimal(str(totals.get("contractsGross") or 0)) / Decimal("1000000")
        contracts = min(contracts, snapshot.quantity.value)
        if contracts <= 0:
            return Quantity(Decimal("0")), None
        usd = Decimal(str(totals.get("usdGross") or 0)) / Decimal("1000000")
        return Quantity(contracts), Price(usd / contracts) if usd > 0 else snapshot.limit_price

    def _execution_fee(
        self,
        execution: dict[str, Any],
        side: OrderSide,
    ) -> TradingFee | None:
        """Derive the taker fee from ``totalsRaw`` for the given side."""
        totals = execution.get("totalsRaw")
        if not isinstance(totals, dict):
            return None
        key = "contractsFee" if side is OrderSide.BUY else "usdFee"
        raw = totals.get(key)
        if raw is None:
            return None
        amount = Decimal(str(raw)) / Decimal("1000000")
        return TradingFee(
            charged=Money(
                amount,
                Currency("OUTCOME_TOKEN" if side is OrderSide.BUY else "USDC"),
            ),
            settlement_cost=Money(amount, Currency("USD")),
        )


class LimitlessExecutionAdapter(ExecutionPort):
    """Execute signed orders recoverable by Limitless client order id."""

    def __init__(
        self,
        private_key: str | None = None,
        *,
        api_key: str | None = None,
        api_secret: str | None = None,
        rpc_url: str | None = None,
        post_only: bool = False,
        http_client: Any | None = None,
        market_fetcher: Any | None = None,
        order_client: Any | None = None,
        fill_enricher: LimitlessFillEnricher | None = None,
        collateral_reader: Callable[[str, str], tuple[int, int]] | None = None,
    ) -> None:
        """Configure signing, transport, and Base USDC collateral reads.

        Parameters
        ----------
        private_key
            Base EOA private key. Defaults to ``LIMITLESS_PRIVATE_KEY``.
        api_key
            Optional Limitless API key.
        api_secret
            Optional HMAC secret paired with ``api_key``.
        rpc_url
            Base JSON-RPC endpoint used only for pre-submission collateral
            checks. Defaults to ``LIMITLESS_RPC_URL``.
        post_only
            Whether persistent GTC limit orders must be post-only.
        http_client
            Optional official SDK HTTP client.
        market_fetcher
            Optional official SDK market metadata client.
        order_client
            Optional official SDK order client.
        fill_enricher
            Optional snapshot fill normalizer.
        collateral_reader
            Optional deterministic ``(balance, allowance)`` reader in six-decimal
            USDC base units, primarily for tests.
        """
        owns_http_client = http_client is None
        maker_address = getattr(
            getattr(order_client, "wallet", None),
            "address",
            None,
        )
        if order_client is not None:
            if http_client is None or market_fetcher is None:
                raise ValueError("Injected order_client requires http_client and market_fetcher")
        else:
            key = private_key or os.getenv("LIMITLESS_PRIVATE_KEY")
            if not key:
                raise ValueError(
                    "Set LIMITLESS_PRIVATE_KEY or pass private_key to sign Limitless orders",
                )
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
            market_fetcher = market_fetcher or MarketFetcher(http_client)
            account = Account.from_key(key)
            maker_address = account.address
            order_client = OrderClient(
                http_client=http_client,
                wallet=account,
                market_fetcher=market_fetcher,
            )

        self._http_client = http_client
        self._market_fetcher = market_fetcher
        self._order_client = order_client
        self._maker_address = str(maker_address) if maker_address else None
        self._owns_http_client = owns_http_client
        self._post_only = post_only
        self._fill_enricher = fill_enricher or LimitlessFillEnricher()
        self._runner = asyncio.Runner()
        self._runner_lock = Lock()
        self._closed = False
        self._markets: dict[str, Any] = {}
        self._profile_preloaded = False
        self._collateral_reader = collateral_reader or _base_usdc_reader(
            rpc_url or os.getenv("LIMITLESS_RPC_URL"),
        )

    def preload(self, contract_ids: tuple[ContractID, ...]) -> None:
        """
        Cache market signing data before the contracts become actionable.

        Parameters
        ----------
        contract_ids
            Limitless contracts whose market and profile metadata should be loaded.
        """
        slugs = tuple(
            dict.fromkeys(
                parse_limitless_contract_id(contract_id)[0]
                for contract_id in contract_ids
            ),
        )
        self._run(self._preload_metadata(slugs))

    async def _preload_metadata(self, slugs: tuple[str, ...]) -> None:
        """Load the lazy SDK profile and full market objects once."""
        self._markets = {
            slug: market for slug, market in self._markets.items() if slug in slugs
        }
        if not self._profile_preloaded:
            ensure_user_data = getattr(self._order_client, "_ensure_user_data", None)
            if ensure_user_data is not None:
                await ensure_user_data()
            self._profile_preloaded = True

        missing = tuple(slug for slug in slugs if slug not in self._markets)
        if not missing:
            return
        markets = await asyncio.gather(
            *(self._market_fetcher.get_market(slug) for slug in missing),
        )
        self._markets.update(zip(missing, markets, strict=True))

    def prepare(self, intent: OrderIntent) -> PreparedOrder:
        """
        Build, sign, and serialize a Limitless order without posting it.

        Parameters
        ----------
        intent
            LIMIT or MARKET intent. IOC is translated to Limitless FAK.

        Returns
        -------
        PreparedOrder
            Exact signed payload keyed by a caller-controlled client order id.
        """
        if intent.order_type not in {OrderType.LIMIT, OrderType.MARKET}:
            raise NotImplementedError("Limitless supports LIMIT and MARKET intents")
        if intent.order_type is OrderType.LIMIT and intent.time_in_force is TimeInForce.FOK:
            raise NotImplementedError(
                "Limitless FOK is a market order without a limit price; use IOC",
            )
        client_order_id = intent.client_order_id or ClientOrderID(uuid4().hex)
        if len(str(client_order_id)) > 128:
            raise ValueError("Limitless client order ids cannot exceed 128 characters")
        payload = self._run_prepare(self._prepare_payload(intent, client_order_id))
        recovery = {
            "schema": 1,
            "client_order_id": str(client_order_id),
            "intent": _intent_data(intent),
        }
        return PreparedOrder(
            reference=OrderReference(
                venue_id=LIMITLESS_VENUE_ID,
                client_order_id=client_order_id,
                recovery_data=json.dumps(recovery, separators=(",", ":")).encode(),
            ),
            request=json.dumps(payload, separators=(",", ":")).encode(),
        )

    def get_available_collateral(
        self,
        contract_ids: tuple[ContractID, ...],
    ) -> Decimal | None:
        """Read Base USDC constrained by current market exchanges.

        Parameters
        ----------
        contract_ids
            Current Limitless contracts whose exchange allowances must be
            included.

        Returns
        -------
        Decimal | None
            Spendable USDC, or ``None`` when no Limitless contract is active.

        Raises
        ------
        RuntimeError
            If account, RPC, market, or exchange data is unavailable.
        """
        slugs = tuple(
            dict.fromkeys(
                parse_limitless_contract_id(contract_id)[0]
                for contract_id in contract_ids
            ),
        )
        if not slugs:
            return None
        if self._collateral_reader is None:
            raise RuntimeError("LIMITLESS_RPC_URL is unavailable for collateral")
        if self._maker_address is None:
            raise RuntimeError("Limitless maker address is unavailable")
        self._run(self._preload_metadata(slugs))
        exchanges = []
        for slug in slugs:
            market = self._markets.get(slug)
            venue = _value(market, "venue") if market is not None else None
            exchange = _value(venue, "exchange")
            if not exchange:
                raise RuntimeError(f"Limitless market {slug} has no exchange")
            exchanges.append(str(exchange))
        readings = (
            self._collateral_reader(self._maker_address, exchange)
            for exchange in dict.fromkeys(exchanges)
        )
        available = min(
            min(int(balance), int(allowance))
            for balance, allowance in readings
        )
        return Decimal(available) / Decimal("1000000")

    async def _prepare_payload(
        self,
        intent: OrderIntent,
        client_order_id: ClientOrderID,
    ) -> dict[str, Any]:
        """Build the signed API payload from metadata loaded before the hot path.

        Parameters
        ----------
        intent
            Order values to encode and sign.
        client_order_id
            Stable identifier included in the venue request.

        Returns
        -------
        dict[str, Any]
            Signed Limitless request payload.

        Raises
        ------
        RuntimeError
            If the market or account profile was not successfully preloaded.

        Notes
        -----
        - This method performs local order construction and signing only. Venue
          metadata and profile I/O belong to :meth:`preload`.
        """
        slug, outcome = parse_limitless_contract_id(intent.contract_id)
        if (
            not self._profile_preloaded
            or getattr(self._order_client, "owner_id", None) is None
        ):
            raise RuntimeError(
                "Limitless account profile was not preloaded before order preparation",
            )
        market = self._markets.get(slug)
        if market is None:
            raise RuntimeError(
                f"Limitless market {slug} was not preloaded before order preparation",
            )
        token_id = _value(_value(market, "tokens"), outcome)
        if not token_id:
            raise RuntimeError(f"Limitless market {slug} has no {outcome.upper()} token")
        price = (
            Decimal("0.999") if intent.side is OrderSide.BUY else Decimal("0.001")
        ) if intent.order_type is OrderType.MARKET else intent.limit_price.value
        unsigned = await self._order_client.build_unsigned_order(
            token_id=str(token_id),
            side=LimitlessSide.BUY if intent.side is OrderSide.BUY else LimitlessSide.SELL,
            price=float(price),
            size=float(intent.quantity.value),
            expiration=(
                int(intent.expires_at.value.timestamp()) if intent.expires_at else None
            ),
        )
        venue = _value(market, "venue")
        exchange = _value(venue, "exchange")
        if not exchange:
            raise RuntimeError(f"Limitless market {slug} has no signing venue")
        signing_config = OrderSigningConfig(
            chain_id=self._order_client._signing_config.chain_id,
            contract_address=str(exchange),
        )
        signature = await _sign_order(
            self._order_client._signer,
            unsigned,
            signing_config,
        )
        signed = SignedOrder(**unsigned.model_dump(), signature=signature)
        order_type = (
            LimitlessOrderType.FAK
            if intent.order_type is OrderType.MARKET
            else _TIME_IN_FORCE[intent.time_in_force]
        )
        return {
            "order": signed.model_dump(by_alias=True, exclude_none=True),
            "ownerId": self._order_client.owner_id,
            "orderType": order_type.value,
            "marketSlug": slug,
            "postOnly": self._post_only if order_type is LimitlessOrderType.GTC else False,
            "clientOrderId": str(client_order_id),
        }

    @ORDER_LATENCY.labels("limitless", "submit").time()
    def submit(self, order: PreparedOrder) -> SubmissionResult:
        """
        Submit the persisted signed payload without rebuilding or signing it.

        Parameters
        ----------
        order
            Prepared request previously persisted by the caller.

        Returns
        -------
        SubmissionResult
            Submission certainty and the initial normalized snapshot when accepted.
        """
        recovery = _recovery_data(order.reference)
        payload = json.loads(order.request)
        if payload.get("clientOrderId") != recovery["client_order_id"]:
            raise ValueError("Limitless request and recovery client ids differ")
        try:
            response = self._run(self._http_client.post("/orders", payload))
        except APIError as error:
            reconciled = self.reconcile(order.reference)
            if reconciled.status is ReconciliationStatus.FOUND:
                return SubmissionResult(
                    status=SubmissionStatus.ACCEPTED,
                    reference=order.reference,
                    snapshot=reconciled.snapshot,
                )
            status = (
                SubmissionStatus.REJECTED
                if error.status_code is not None and 400 <= error.status_code < 500
                else SubmissionStatus.UNKNOWN
            )
            reason = (
                f"HTTP {error.status_code}: {error.message}"
                if error.status_code is not None
                else error.message
            )
            return SubmissionResult(
                status=status,
                reference=order.reference,
                reason=reason,
            )
        except (aiohttp.ClientError, TimeoutError, OSError):
            return SubmissionResult(
                status=SubmissionStatus.UNKNOWN,
                reference=order.reference,
            )

        venue_order_id = _response_order_id(response)
        snapshot = self._fill_enricher.enrich(
            _snapshot_from_reference(order.reference, venue_order_id),
            response,
        )
        return SubmissionResult(
            status=SubmissionStatus.ACCEPTED,
            reference=order.reference,
            snapshot=snapshot,
            reason=snapshot.reason,
        )

    @ORDER_LATENCY.labels("limitless", "get").time()
    def reconcile(self, reference: OrderReference) -> ReconciliationResult:
        """
        Resolve an order by the client id persisted before submission.

        Parameters
        ----------
        reference
            Durable reference produced by :meth:`prepare`.

        Returns
        -------
        ReconciliationResult
            Authoritative venue state, proven absence, or uncertainty.
        """
        recovery = _recovery_data(reference)
        try:
            response = self._run(
                self._http_client.post(
                    "/orders/status/batch",
                    {"items": [{"clientOrderId": recovery["client_order_id"]}]},
                ),
            )
        except (APIError, aiohttp.ClientError, TimeoutError, OSError):
            return ReconciliationResult(
                status=ReconciliationStatus.UNKNOWN,
                reference=reference,
            )
        results = response.get("results") if isinstance(response, dict) else None
        if not results or not isinstance(results[0], dict):
            return ReconciliationResult(
                status=ReconciliationStatus.UNKNOWN,
                reference=reference,
            )
        result = results[0]
        if result.get("status") != "found":
            status = (
                ReconciliationStatus.NOT_FOUND
                if str(result.get("status")).lower() in {"not_found", "not-found"}
                else ReconciliationStatus.UNKNOWN
            )
            return ReconciliationResult(status=status, reference=reference)
        data = result.get("data")
        order_id = _response_order_id(data, required=False)
        snapshot = self._fill_enricher.enrich(
            _snapshot_from_reference(reference, order_id),
            data,
        )
        return ReconciliationResult(
            status=ReconciliationStatus.FOUND,
            reference=reference,
            snapshot=snapshot,
        )

    @ORDER_LATENCY.labels("limitless", "cancel").time()
    def cancel(self, reference: OrderReference) -> ReconciliationResult:
        """
        Cancel and reconcile an order by its durable client order id.

        Parameters
        ----------
        reference
            Durable reference produced by :meth:`prepare`.

        Returns
        -------
        ReconciliationResult
            State observed after the cancellation request.
        """
        recovery = _recovery_data(reference)
        try:
            self._run(
                self._http_client.post(
                    "/orders/cancel",
                    {"clientOrderId": recovery["client_order_id"]},
                ),
            )
        except (APIError, aiohttp.ClientError, TimeoutError, OSError):
            return ReconciliationResult(
                status=ReconciliationStatus.UNKNOWN,
                reference=reference,
            )
        return self.reconcile(reference)

    def close(self) -> None:
        """
        Close owned SDK resources.

        Notes
        -----
        - Repeated calls are harmless and injected clients remain caller-owned.
        """
        if self._closed:
            return
        if self._owns_http_client:
            self._run(self._http_client.close())
            self._owns_http_client = False
        self._runner.close()
        self._closed = True

    def _run(self, awaitable: Awaitable[_T]) -> _T:
        """Run one SDK coroutine on the adapter-owned event loop."""
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            pass
        else:
            if hasattr(awaitable, "close"):
                awaitable.close()
            raise RuntimeError("Call the synchronous execution adapter outside an event loop")
        with self._runner_lock:
            if self._closed:
                if hasattr(awaitable, "close"):
                    awaitable.close()
                raise RuntimeError("Limitless execution adapter is closed")
            return self._runner.run(awaitable)

    def _run_prepare(self, awaitable: Awaitable[_T]) -> _T:
        """Run cache-only preparation without waiting for the shared SDK runner.

        Parameters
        ----------
        awaitable
            Local order-building and signing coroutine created for one intent.

        Returns
        -------
        _T
            Value returned by the preparation coroutine.

        Raises
        ------
        RuntimeError
            If called from an event-loop thread or after the adapter is closed.

        Notes
        -----
        - Network-bound SDK operations remain on the adapter-owned runner. Each
          worker-thread preparation gets a private loop so independent pairs do
          not serialize behind those operations or another signature.
        """
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            pass
        else:
            if hasattr(awaitable, "close"):
                awaitable.close()
            raise RuntimeError(
                "Call the synchronous execution adapter outside an event loop",
            )
        if self._closed:
            if hasattr(awaitable, "close"):
                awaitable.close()
            raise RuntimeError("Limitless execution adapter is closed")
        return asyncio.run(awaitable)


def _recovery_data(reference: OrderReference) -> dict[str, Any]:
    if reference.venue_id != LIMITLESS_VENUE_ID:
        raise ValueError(f"Limitless cannot handle venue {reference.venue_id}")
    data = json.loads(reference.recovery_data)
    if data.get("schema") != 1 or data.get("client_order_id") != str(reference.client_order_id):
        raise ValueError("Invalid Limitless recovery data")
    return data


def _intent_data(intent: OrderIntent) -> dict[str, Any]:
    return {
        "contract_id": str(intent.contract_id),
        "side": intent.side.value,
        "quantity": str(intent.quantity.value),
        "order_type": intent.order_type.value,
        "limit_price": str(intent.limit_price.value) if intent.limit_price else None,
        "created_at": intent.created_at.value.isoformat() if intent.created_at else None,
    }


def _snapshot_from_reference(
    reference: OrderReference,
    order_id: OrderID | None,
) -> OrderSnapshot:
    intent = _recovery_data(reference)["intent"]
    now = Timestamp.now()
    return OrderSnapshot(
        status=OrderStatus.SUBMITTED,
        contract_id=ContractID(intent["contract_id"]),
        side=OrderSide(intent["side"]),
        quantity=Quantity(Decimal(intent["quantity"])),
        order_type=OrderType(intent["order_type"]),
        client_order_id=reference.client_order_id,
        order_id=order_id,
        limit_price=Price(Decimal(intent["limit_price"])) if intent["limit_price"] else None,
        created_at=Timestamp.from_iso(intent["created_at"]) if intent["created_at"] else now,
        updated_at=now,
    )


def _response_order_id(response: Any, *, required: bool = True) -> OrderID | None:
    envelope = _value(response, "order")
    order = _value(envelope, "order") or envelope
    order_id = _value(order, "id")
    if not order_id and required:
        raise RuntimeError("Limitless accepted the request without an order ID")
    return OrderID(str(order_id)) if order_id else None


def _limitless_response_reason(response: Any) -> str | None:
    """Extract the terminal reason or state exposed by a Limitless response.

    Parameters
    ----------
    response
        Create-order, status, or private order-event payload returned by Limitless.

    Returns
    -------
    str | None
        Explicit venue reason, or terminal order and settlement states when the
        response does not include one.
    """
    envelope = _value(response, "order")
    order = _value(envelope, "order") or envelope or response
    execution = _value(response, "execution") or _value(envelope, "execution")
    for data in (response, envelope, order, execution):
        if not isinstance(data, dict):
            continue
        for key in ("reason", "failureReason", "error", "message"):
            if value := data.get(key):
                return (
                    json.dumps(value, separators=(",", ":"), default=str)
                    if isinstance(value, (dict, list))
                    else str(value)
                )
    states = []
    if status := _value(order, "status"):
        states.append(f"order status {status}")
    if settlement := _value(execution, "settlementStatus"):
        states.append(f"settlement status {settlement}")
    return "; ".join(states) or None


def _order_status(value: Any) -> OrderStatus:
    return {
        "LIVE": OrderStatus.ACCEPTED,
        "MATCHED": OrderStatus.FILLED,
        "CANCELED": OrderStatus.CANCELLED,
        "CANCELLED": OrderStatus.CANCELLED,
        "UNMATCHED": OrderStatus.REJECTED,
        "INVALID": OrderStatus.REJECTED,
    }.get(str(value or "").upper(), OrderStatus.SUBMITTED)


def _value(value: Any, name: str) -> Any:
    return value.get(name) if isinstance(value, dict) else getattr(value, name, None)
