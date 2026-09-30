"""Execute recoverable Polymarket orders through the CLOB client.

Responsibilities
----------------
- Sign orders before submission and serialize the exact signed request.
- Reconcile and cancel orders from their deterministic CLOB order hash.
- Normalize venue responses into domain snapshots.
- Enrich filled snapshots with trade average price and taker fees.
"""

import json
from collections.abc import Mapping
from dataclasses import asdict, replace
from datetime import datetime, timezone
from decimal import ROUND_DOWN, Decimal
from typing import Any
from uuid import uuid4

import httpx
from nautilus_trader.adapters.polymarket.common.constants import (
    POLYMARKET_NAUTILUS_BUILDER_CODE,
)
from nautilus_trader.adapters.polymarket.factories import get_polymarket_http_client
from py_clob_client_v2.client import ClobClient
from py_clob_client_v2.clob_types import (
    AssetType,
    BalanceAllowanceParams,
    OrderArgsV2,
    OrderPayload,
    PartialCreateOrderOptions,
    TradeParams,
)
from py_clob_client_v2.clob_types import OrderType as PolymarketOrderType
from py_clob_client_v2.config import get_contract_config
from py_clob_client_v2.exceptions import PolyApiException, PolyException
from py_clob_client_v2.http_helpers import helpers as clob_http_helpers
from py_clob_client_v2.order_utils import ExchangeOrderBuilderV1, ExchangeOrderBuilderV2
from py_clob_client_v2.order_utils.model.order_data_v1 import SignedOrderV1
from py_clob_client_v2.order_utils.model.order_data_v2 import SignedOrderV2

from prediction_markets.domain.contracts.value_objects import TickSize
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
from prediction_markets.infrastructure.metrics import ORDER_LATENCY
from prediction_markets.infrastructure.venues.polymarket.mappers import (
    POLYMARKET_VENUE_ID,
    parse_polymarket_contract_id,
    polymarket_contract_id,
)

_TIME_IN_FORCE = {
    TimeInForce.GTC: PolymarketOrderType.GTC,
    TimeInForce.GTD: PolymarketOrderType.GTD,
    TimeInForce.FOK: PolymarketOrderType.FOK,
    TimeInForce.IOC: PolymarketOrderType.FAK,
}
_FEE_QUANTUM = Decimal("0.00001")


def _validate_polymarket_price(price: Decimal, tick_size: Decimal) -> None:
    """
    Reject prices that the Polymarket CLOB cannot accept.

    Parameters
    ----------
    price
        Order price in the inclusive ``[0, 1]`` probability range.
    tick_size
        CLOB-provided minimum tick for the outcome token.

    Raises
    ------
    ValueError
        If the price is outside the CLOB range or is not aligned to its tick.
    """
    if (
        tick_size <= 0
        or price < tick_size
        or price > Decimal("1") - tick_size
        or price % tick_size != 0
    ):
        raise ValueError(
            f"invalid Polymarket price ({price}), min: {tick_size} - "
            f"max: {Decimal('1') - tick_size}"
        )


def _create_signed_order(
    client: ClobClient,
    args: OrderArgsV2,
    *,
    tick_size: Decimal,
    neg_risk: bool,
) -> Any:
    """
    Create one signed CLOB order without logging its sensitive payload.

    Parameters
    ----------
    client
        Authenticated CLOB client performing the signature.
    args
        Venue order arguments to sign.
    tick_size
        Preloaded market tick used to avoid metadata discovery during signing.
    neg_risk
        Preloaded negative-risk flag for the outcome token.

    Returns
    -------
    Any
        Native signed order returned by the CLOB client.
    """
    return client.create_order(
        args,
        PartialCreateOrderOptions(
            tick_size=str(tick_size),
            neg_risk=neg_risk,
        ),
    )


class PolymarketFillEnricher:
    """Attach average fill price and taker fees from CLOB trade history."""

    def __init__(self, client: ClobClient) -> None:
        """
        Parameters
        ----------
        client
            Authenticated CLOB client used to fetch trades and market fee data.
        """
        self._client = client
        self._fee_schedules: dict[str, tuple[Decimal, Decimal]] = {}

    def enrich(self, snapshot: OrderSnapshot) -> OrderSnapshot:
        """
        Enrich a reconciled snapshot with trade-level average price and fees.

        Parameters
        ----------
        snapshot
            Order state from ``get_order`` before trade enrichment.

        Returns
        -------
        OrderSnapshot
            Unchanged when there is no fill or trades cannot be loaded; otherwise
            average price and fee derived from matching taker fills.
        """
        if snapshot.filled_quantity.value <= 0 or snapshot.order_id is None:
            return snapshot
        condition_id, token_id = parse_polymarket_contract_id(snapshot.contract_id)
        try:
            trades = self._client.get_trades(
                TradeParams(market=condition_id, asset_id=token_id),
                only_first_page=True,
            )
        except (AttributeError, PolyApiException):
            return snapshot
        matching = [
            trade
            for trade in trades or ()
            if _trade_matches_order(trade, snapshot.order_id)
        ]
        taker_fills = [
            trade
            for trade in matching
            if str(trade.get("taker_order_id")) == str(snapshot.order_id)
        ]
        filled = sum(
            (Decimal(str(trade["size"])) for trade in taker_fills),
            Decimal("0"),
        )
        average = (
            Price(
                sum(
                    Decimal(str(trade["size"])) * Decimal(str(trade["price"]))
                    for trade in taker_fills
                )
                / filled,
            )
            if filled > 0
            else snapshot.average_price
        )
        return replace(
            snapshot,
            average_price=average,
            fee=_polymarket_fee(matching, self.fee_schedule(condition_id)),
        )

    def fee_schedule(self, condition_id: str) -> tuple[Decimal, Decimal] | None:
        """
        Return the cached ``(rate, exponent)`` fee schedule for a market.

        Parameters
        ----------
        condition_id
            Polymarket condition id used by the CLOB market info endpoint.

        Returns
        -------
        tuple[Decimal, Decimal] | None
            Fee curve parameters when available; otherwise ``None``.
        """
        cached = self._fee_schedules.get(condition_id)
        if cached is not None:
            return cached
        try:
            market = self._client.get_clob_market_info(condition_id)
        except (AttributeError, PolyException):
            return None
        fee_data = market.get("fd") if isinstance(market, dict) else None
        if not isinstance(fee_data, dict) or fee_data.get("r") is None:
            return None
        schedule = (Decimal(str(fee_data["r"])), Decimal(str(fee_data.get("e", 1))))
        self._fee_schedules[condition_id] = schedule
        return schedule


class PolymarketExecutionAdapter(ExecutionPort):
    """Execute signed CLOB orders recoverable by deterministic order hash."""

    def __init__(
        self,
        client: ClobClient | None = None,
        *,
        signature_type: int = 0,
        post_only: bool = False,
        fill_enricher: PolymarketFillEnricher | None = None,
    ) -> None:
        """
        Parameters
        ----------
        client
            Optional CLOB client; defaults to the shared Nautilus factory client.
        signature_type : int, default=0
            Polymarket signature type forwarded to the factory client.
        post_only : bool, default=False
            Whether GTC prepares are posted as post-only.
        fill_enricher : PolymarketFillEnricher, optional
            Collaborator that attaches trade average price and fees on reconcile.
        """
        clob_http_helpers._http_client.timeout = httpx.Timeout(30.0)
        self._client = client or get_polymarket_http_client(signature_type=signature_type)
        self._post_only = post_only
        self._fill_enricher = fill_enricher or PolymarketFillEnricher(self._client)
        self._preloaded_conditions: set[str] = set()
        self._neg_risk_by_token: dict[str, bool] = {}
        self._tick_size_by_token: dict[str, Decimal] = {}
        self._version_preloaded = False
        self._order_version: int | None = None

    def preload(
        self,
        contract_ids: tuple[ContractID, ...],
    ) -> Mapping[ContractID, TickSize]:
        """
        Cache CLOB signing metadata before the contracts become actionable.

        Parameters
        ----------
        contract_ids
            Polymarket contracts whose condition metadata should be loaded.

        Returns
        -------
        Mapping[ContractID, TickSize]
            Current CLOB tick for each preloaded outcome token.

        Notes
        -----
        - One market-info request warms CLOB metadata for both outcome tokens.
        - Negative-risk state is cached explicitly by token for order preparation.
        - Tick sizes are cached explicitly by token before order preparation.
        - The global CLOB order version is resolved only once per adapter.
        - Legacy V1 fee rates are loaded before the token becomes actionable.
        """
        if not self._version_preloaded:
            # py-clob-client caches the public version endpoint only through this resolver.
            resolve_version = getattr(
                self._client,
                "_ClobClient__resolve_version",
                getattr(self._client, "get_version", None),
            )
            if resolve_version is None:
                raise RuntimeError("Polymarket order version resolver is unavailable")
            self._order_version = int(resolve_version())
            self._version_preloaded = True

        contracts = tuple(
            parse_polymarket_contract_id(contract_id)
            for contract_id in contract_ids
        )
        condition_ids = tuple(
            dict.fromkeys(
                condition_id for condition_id, _ in contracts
            ),
        )
        for condition_id in condition_ids:
            if condition_id in self._preloaded_conditions:
                continue
            self._client.get_clob_market_info(condition_id)
            self._preloaded_conditions.add(condition_id)
        for _, token_id in contracts:
            if token_id not in self._neg_risk_by_token:
                self._neg_risk_by_token[token_id] = bool(
                    self._client.get_neg_risk(token_id),
                )
            if token_id not in self._tick_size_by_token:
                self._tick_size_by_token[token_id] = Decimal(
                    str(self._client.get_tick_size(token_id)),
                )
            if self._order_version == 1:
                self._client.get_fee_rate_bps(token_id)
        return {
            contract_id: TickSize(self._tick_size_by_token[token_id])
            for contract_id, (_, token_id) in zip(
                contract_ids,
                contracts,
                strict=True,
            )
        }

    def prepare(self, intent: OrderIntent) -> PreparedOrder:
        """
        Sign and serialize a CLOB order without posting it.

        Parameters
        ----------
        intent
            LIMIT or MARKET intent. MARKET is encoded as a marketable FAK order.

        Returns
        -------
        PreparedOrder
            Exact signed request and its deterministic CLOB hash.

        Raises
        ------
        RuntimeError
            If signing metadata was not successfully preloaded.
        ValueError
            If the requested price is outside the preloaded tick constraints.

        Notes
        -----
        - Signing metadata must be loaded by :meth:`preload`; preparation never
          repairs a cache miss with venue I/O.
        - MARKET intents still read the current venue order book and are outside
          the monitored LIMIT-order hot path.
        """
        if intent.order_type not in {OrderType.LIMIT, OrderType.MARKET}:
            raise NotImplementedError("Polymarket supports LIMIT and MARKET intents")

        _, token_id = parse_polymarket_contract_id(intent.contract_id)
        if not self._version_preloaded:
            raise RuntimeError("Polymarket order version was not preloaded")
        tick_size = self._tick_size_by_token.get(token_id)
        if tick_size is None:
            raise RuntimeError(
                f"Polymarket token {token_id} tick size was not preloaded",
            )
        if token_id not in self._neg_risk_by_token:
            raise RuntimeError(
                f"Polymarket token {token_id} negative-risk state was not preloaded",
            )
        price = (
            _marketable_price(self._client.get_order_book(token_id), intent.side)
            if intent.order_type is OrderType.MARKET
            else intent.limit_price.value
        )
        _validate_polymarket_price(price, tick_size)
        order_type = (
            PolymarketOrderType.FAK
            if intent.order_type is OrderType.MARKET
            else _TIME_IN_FORCE[intent.time_in_force]
        )
        neg_risk = self._neg_risk_by_token[token_id]
        signed_order = _create_signed_order(
            self._client,
            OrderArgsV2(
                token_id=token_id,
                price=float(price),
                size=float(intent.quantity.value),
                side=intent.side.value.upper(),
                expiration=(
                    int(intent.expires_at.value.timestamp()) if intent.expires_at else 0
                ),
                builder_code=POLYMARKET_NAUTILUS_BUILDER_CODE,
            ),
            tick_size=tick_size,
            neg_risk=neg_risk,
        )
        order_id = _expected_order_id(self._client, signed_order, neg_risk)
        client_order_id = intent.client_order_id or ClientOrderID(uuid4().hex)
        request = {
            "schema": 1,
            "version": 2 if hasattr(signed_order, "timestamp") else 1,
            "order": asdict(signed_order),
            "order_type": str(order_type),
            "post_only": self._post_only if order_type == PolymarketOrderType.GTC else False,
            "intent": _intent_data(intent),
        }
        return PreparedOrder(
            reference=OrderReference(
                venue_id=POLYMARKET_VENUE_ID,
                client_order_id=client_order_id,
                recovery_data=str(order_id).encode(),
            ),
            request=json.dumps(request, separators=(",", ":")).encode(),
        )

    def update_tick_size(
        self,
        contract_id: ContractID,
        tick_size: TickSize,
    ) -> None:
        """Apply a tick-size change received from the public market stream.

        Parameters
        ----------
        contract_id
            Polymarket outcome token whose price increment changed.
        tick_size
            New authoritative CLOB price increment.

        Notes
        -----
        - This is an in-memory update; order preparation remains free of venue I/O.
        - The SDK cache is updated with the same authoritative stream value so
          its signing validation cannot disagree with the adapter cache.
        """
        _, token_id = parse_polymarket_contract_id(contract_id)
        self._tick_size_by_token[token_id] = tick_size.value
        client_tick_sizes = getattr(self._client, "_ClobClient__tick_sizes", None)
        if isinstance(client_tick_sizes, dict):
            client_tick_sizes[token_id] = str(tick_size.value)

    def get_available_collateral(
        self,
        contract_ids: tuple[ContractID, ...],
    ) -> Decimal:
        """Read Polymarket USDC constrained by every CLOB allowance.

        Parameters
        ----------
        contract_ids
            Current Polymarket contracts. The CLOB collateral allowance is
            account-wide, so the identifiers require no additional lookups.

        Returns
        -------
        Decimal
            Spendable USDC after the most restrictive returned allowance.

        Raises
        ------
        RuntimeError
            If Polymarket omits collateral allowances.
        """
        raw = self._client.get_balance_allowance(
            BalanceAllowanceParams(asset_type=AssetType.COLLATERAL),
        )
        balance = int(raw.get("balance") or 0)
        allowances = raw.get("allowances")
        if not isinstance(allowances, Mapping) or not allowances:
            raise RuntimeError("Polymarket collateral allowances were not returned")
        allowance = min(int(value) for value in allowances.values())
        return Decimal(min(balance, allowance)) / Decimal("1000000")

    @ORDER_LATENCY.labels("polymarket", "submit").time()
    def submit(self, order: PreparedOrder) -> SubmissionResult:
        """
        Submit the persisted signed CLOB request without rebuilding it.

        Parameters
        ----------
        order
            Prepared request previously persisted by the caller.

        Returns
        -------
        SubmissionResult
            Submission certainty and the initial normalized snapshot when accepted.
        """
        data = _request_data(order)
        try:
            response = self._client.post_order(
                _signed_order(data),
                order_type=data["order_type"],
                post_only=bool(data["post_only"]),
            )
        except PolyApiException as error:
            reason = _polymarket_error_reason(error)
            if order_id := _error_order_id(error):
                reconciled = self._reconcile_order_id(order.reference, order_id)
                if reconciled.status is ReconciliationStatus.FOUND:
                    return SubmissionResult(
                        status=SubmissionStatus.ACCEPTED,
                        reference=order.reference,
                        snapshot=reconciled.snapshot,
                    )
                if reconciled.status is ReconciliationStatus.UNKNOWN:
                    return SubmissionResult(
                        status=SubmissionStatus.UNKNOWN,
                        reference=order.reference,
                        reason=reason,
                    )
            status = (
                SubmissionStatus.REJECTED
                if error.status_code is not None and 400 <= error.status_code < 500
                else SubmissionStatus.UNKNOWN
            )
            return SubmissionResult(
                status=status,
                reference=order.reference,
                reason=reason,
            )
        except httpx.TransportError:
            return SubmissionResult(
                status=SubmissionStatus.UNKNOWN,
                reference=order.reference,
            )

        if not isinstance(response, dict) or response.get("success") is not True:
            return SubmissionResult(
                status=SubmissionStatus.REJECTED,
                reference=order.reference,
                reason=_polymarket_response_reason(response),
            )
        venue_order_id = response.get("orderID")
        if not venue_order_id:
            return SubmissionResult(
                status=SubmissionStatus.UNKNOWN,
                reference=order.reference,
            )
        snapshot = _prepared_snapshot(
            data,
            order.reference,
            OrderID(str(venue_order_id)),
        )
        return SubmissionResult(
            status=SubmissionStatus.ACCEPTED,
            reference=order.reference,
            snapshot=snapshot,
        )

    @ORDER_LATENCY.labels("polymarket", "get").time()
    def reconcile(self, reference: OrderReference) -> ReconciliationResult:
        """
        Fetch an order by the deterministic hash stored before submission.

        Parameters
        ----------
        reference
            Durable reference produced by :meth:`prepare`.

        Returns
        -------
        ReconciliationResult
            Authoritative venue state, proven absence, or uncertainty.
        """
        _require_venue(reference)
        return self._reconcile_order_id(reference, reference.recovery_data.decode())

    @ORDER_LATENCY.labels("polymarket", "cancel").time()
    def cancel(self, reference: OrderReference) -> ReconciliationResult:
        """
        Cancel an order by its persisted CLOB hash and reconcile the result.

        Parameters
        ----------
        reference
            Durable reference produced by :meth:`prepare`.

        Returns
        -------
        ReconciliationResult
            State observed after the cancellation request.
        """
        _require_venue(reference)
        order_id = reference.recovery_data.decode()
        try:
            response = self._client.cancel_order(OrderPayload(order_id))
        except (PolyApiException, httpx.TransportError):
            return ReconciliationResult(
                status=ReconciliationStatus.UNKNOWN,
                reference=reference,
            )
        if not isinstance(response, dict):
            return ReconciliationResult(
                status=ReconciliationStatus.UNKNOWN,
                reference=reference,
            )
        return self._reconcile_order_id(reference, order_id)

    def _reconcile_order_id(
        self,
        reference: OrderReference,
        order_id: str,
    ) -> ReconciliationResult:
        try:
            raw = self._client.get_order(order_id)
        except PolyApiException as error:
            status = (
                ReconciliationStatus.NOT_FOUND
                if error.status_code == 404
                else ReconciliationStatus.UNKNOWN
            )
            return ReconciliationResult(status=status, reference=reference)
        except httpx.TransportError:
            return ReconciliationResult(
                status=ReconciliationStatus.UNKNOWN,
                reference=reference,
            )
        if not raw:
            return ReconciliationResult(
                status=ReconciliationStatus.NOT_FOUND,
                reference=reference,
            )
        snapshot = self._fill_enricher.enrich(_order_snapshot(raw, reference.client_order_id))
        return ReconciliationResult(
            status=ReconciliationStatus.FOUND,
            reference=reference,
            snapshot=snapshot,
        )


def _expected_order_id(client: Any, signed_order: Any, neg_risk: bool) -> OrderID:
    """Derive the native CLOB hash from an already signed order."""
    explicit = getattr(signed_order, "order_id", None) or getattr(signed_order, "id", None)
    if explicit:
        return OrderID(str(explicit))
    signer = getattr(client, "signer", None)
    if signer is None:
        raise RuntimeError("Polymarket client does not expose the signer needed for recovery")
    chain_id = signer.get_chain_id()
    config = get_contract_config(chain_id)
    if hasattr(signed_order, "timestamp"):
        exchange = config.neg_risk_exchange_v2 if neg_risk else config.exchange_v2
        builder = ExchangeOrderBuilderV2(exchange, chain_id, signer)
    else:
        exchange = config.neg_risk_exchange if neg_risk else config.exchange
        builder = ExchangeOrderBuilderV1(exchange, chain_id, signer)
    return OrderID(builder.build_order_hash(builder.build_order_typed_data(signed_order)))


def _request_data(order: PreparedOrder) -> dict[str, Any]:
    _require_venue(order.reference)
    data = json.loads(order.request)
    if data.get("schema") != 1:
        raise ValueError("Unsupported Polymarket prepared-order schema")
    return data


def _require_venue(reference: OrderReference) -> None:
    if reference.venue_id != POLYMARKET_VENUE_ID:
        raise ValueError(f"Polymarket cannot handle venue {reference.venue_id}")


def _signed_order(data: dict[str, Any]) -> SignedOrderV1 | SignedOrderV2:
    cls = SignedOrderV2 if data["version"] == 2 else SignedOrderV1
    return cls(**data["order"])


def _intent_data(intent: OrderIntent) -> dict[str, Any]:
    return {
        "contract_id": str(intent.contract_id),
        "side": intent.side.value,
        "quantity": str(intent.quantity.value),
        "order_type": intent.order_type.value,
        "limit_price": str(intent.limit_price.value) if intent.limit_price else None,
        "created_at": intent.created_at.value.isoformat() if intent.created_at else None,
    }


def _prepared_snapshot(
    data: dict[str, Any],
    reference: OrderReference,
    order_id: OrderID,
) -> OrderSnapshot:
    intent = data["intent"]
    now = Timestamp.now()
    return OrderSnapshot(
        status=OrderStatus.SUBMITTED,
        contract_id=polymarket_contract_id(*parse_polymarket_contract_id(intent["contract_id"])),
        side=OrderSide(intent["side"]),
        quantity=Quantity(Decimal(intent["quantity"])),
        order_type=OrderType(intent["order_type"]),
        client_order_id=reference.client_order_id,
        order_id=order_id,
        limit_price=Price(Decimal(intent["limit_price"])) if intent["limit_price"] else None,
        created_at=Timestamp.from_iso(intent["created_at"]) if intent["created_at"] else now,
        updated_at=now,
    )


def _error_order_id(error: PolyApiException) -> str | None:
    message = error.error_msg
    return str(message["orderID"]) if isinstance(message, dict) and message.get("orderID") else None


def _polymarket_error_reason(error: PolyApiException) -> str:
    reason = _polymarket_response_reason(error.error_msg)
    return (
        f"HTTP {error.status_code}: {reason}"
        if error.status_code is not None
        else reason
    )


def _polymarket_response_reason(response: Any) -> str:
    if isinstance(response, dict):
        for key in ("errorMsg", "error", "message", "msg"):
            if response.get(key):
                return str(response[key])
        return json.dumps(response, separators=(",", ":"), default=str)
    return str(response)


def _marketable_price(raw_book: Any, side: OrderSide) -> Decimal:
    """Select a marketable price from the executable side of a CLOB book."""
    name = "asks" if side is OrderSide.BUY else "bids"
    levels = (
        raw_book.get(name) if isinstance(raw_book, dict) else getattr(raw_book, name, None)
    ) or ()
    prices = [
        Decimal(str(level.get("price") if isinstance(level, dict) else level.price))
        for level in levels
    ]
    prices = [price for price in prices if Decimal("0") < price < Decimal("1")]
    if not prices:
        raise RuntimeError(f"Polymarket has no executable {name}")
    return max(prices) if side is OrderSide.BUY else min(prices)


def _order_snapshot(raw: dict[str, Any], client_order_id: ClientOrderID) -> OrderSnapshot:
    """Normalize a CLOB order payload into a domain snapshot."""
    quantity = Quantity(Decimal(str(raw["original_size"])))
    filled = Quantity(Decimal(str(raw.get("size_matched") or 0)))
    price = Price(Decimal(str(raw["price"])))
    return OrderSnapshot(
        status=_order_status(str(raw.get("status") or ""), quantity, filled),
        contract_id=polymarket_contract_id(str(raw["market"]), str(raw["asset_id"])),
        side=OrderSide(str(raw["side"]).lower()),
        quantity=quantity,
        order_type=OrderType.LIMIT,
        client_order_id=client_order_id,
        order_id=OrderID(str(raw["id"])),
        limit_price=price,
        filled_quantity=filled,
        average_price=price if filled.value > 0 else None,
        created_at=_timestamp(raw.get("created_at")),
        updated_at=Timestamp.now(),
    )


def _polymarket_fee(
    trades: list[dict[str, Any]],
    schedule: tuple[Decimal, Decimal] | None,
) -> TradingFee | None:
    """Compute the fee attributed to fills matching one Polymarket order."""
    if not trades:
        return None
    total = Decimal("0")
    for trade in trades:
        if str(trade.get("trader_side") or "").upper() == "MAKER":
            continue
        if schedule is None:
            raw_rate = trade.get("fee_rate_bps")
            if raw_rate is None or Decimal(str(raw_rate)) <= 0:
                return None
            rate, exponent = Decimal(str(raw_rate)) / Decimal("10000"), Decimal("1")
        else:
            rate, exponent = schedule
        price = Decimal(str(trade["price"]))
        fee = Decimal(str(trade["size"])) * rate * (
            price * (Decimal("1") - price)
        ) ** exponent
        total += fee.quantize(_FEE_QUANTUM, rounding=ROUND_DOWN)
    return TradingFee(
        charged=Money(total, Currency("USDC")),
        settlement_cost=Money(total, Currency("USD")),
    )


def _trade_matches_order(trade: dict[str, Any], order_id: OrderID) -> bool:
    if str(trade.get("taker_order_id")) == str(order_id):
        return True
    return any(
        str(maker.get("order_id") or maker.get("orderId")) == str(order_id)
        for maker in trade.get("maker_orders") or ()
        if isinstance(maker, dict)
    )


def _order_status(
    raw_status: str,
    quantity: Quantity,
    filled_quantity: Quantity,
) -> OrderStatus:
    if filled_quantity.value >= quantity.value:
        return OrderStatus.FILLED
    if filled_quantity.value > 0:
        return OrderStatus.PARTIALLY_FILLED
    return {
        "LIVE": OrderStatus.ACCEPTED,
        "DELAYED": OrderStatus.SUBMITTED,
        "MATCHED": OrderStatus.FILLED,
        "CANCELED": OrderStatus.CANCELLED,
        "CANCELED_MARKET_RESOLVED": OrderStatus.CANCELLED,
        "INVALID": OrderStatus.REJECTED,
        "UNMATCHED": OrderStatus.REJECTED,
    }.get(raw_status.upper(), OrderStatus.SUBMITTED)


def _timestamp(value: Any) -> Timestamp | None:
    if value is None:
        return None
    seconds = Decimal(str(value))
    if seconds > Decimal("10000000000"):
        seconds /= Decimal("1000")
    return Timestamp(datetime.fromtimestamp(float(seconds), tz=timezone.utc))
