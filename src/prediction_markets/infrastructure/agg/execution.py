"""Integrate AGG paper execution with the recoverable execution port.

Responsibilities
----------------
- Build and persist exact AGG paper requests before submission.
- Translate AGG order responses into domain snapshots.
- Reconcile persisted orders after process loss.
"""

import json
import os
from decimal import Decimal
from threading import RLock
from typing import Any
from uuid import uuid4

import httpx
from dotenv import load_dotenv

from prediction_markets.domain.ports.execution import ExecutionPort
from prediction_markets.domain.shared.value_objects import (
    ClientOrderID,
    ContractID,
    OrderID,
    Price,
    Quantity,
    Timestamp,
    VenueID,
)
from prediction_markets.domain.trading.entities import OrderIntent, OrderSnapshot
from prediction_markets.domain.trading.enums import (
    OrderSide,
    OrderStatus,
    OrderType,
    ReconciliationStatus,
    SubmissionStatus,
)
from prediction_markets.domain.trading.value_objects import (
    OrderReference,
    PreparedOrder,
    ReconciliationResult,
    SubmissionResult,
)
from prediction_markets.infrastructure.metrics import ORDER_LATENCY

AGG_VENUE_ID = VenueID("AGG")


class AggPaperExecutionAdapter(ExecutionPort):
    """Execute recoverable orders through an AGG paper account.

    Notes
    -----
    - AGG paper orders are currently market-like and use the optional
      ``slippageBps`` request field.
    - The adapter is synchronous because the output dispatcher invokes venue
      I/O outside the main event loop.
    """

    def __init__(
        self,
        account_id: str | None = None,
        *,
        app_id: str | None = None,
        api_key: str | None = None,
        admin_key: str | None = None,
        base_url: str = "https://api.agg.market",
        slippage_bps: int | None = None,
        timeout_seconds: float = 10.0,
        client: httpx.Client | None = None,
    ) -> None:
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        if slippage_bps is not None and slippage_bps < 0:
            raise ValueError("slippage_bps must be non-negative")

        load_dotenv()
        self._account_id = (account_id or os.getenv("AGG_PAPER_ACCOUNT_ID") or "").strip()
        self._app_id = (app_id or os.getenv("AGG_APP_ID") or "").strip()
        self._api_key = (api_key or os.getenv("AGG_APP_API_KEY") or "").strip()
        self._admin_key = (admin_key or os.getenv("AGG_ADMIN_KEY") or "").strip()
        if not self._account_id:
            raise ValueError("AGG paper execution requires account_id or AGG_PAPER_ACCOUNT_ID")
        if not self._app_id:
            raise ValueError("AGG paper execution requires app_id or AGG_APP_ID")
        if not self._api_key and not self._admin_key:
            raise ValueError("AGG paper execution requires AGG_APP_API_KEY or AGG_ADMIN_KEY")

        self._base_url = base_url.rstrip("/")
        self._slippage_bps = slippage_bps
        self._owns_client = client is None
        self._client = client or httpx.Client(timeout=timeout_seconds)
        self._orders: dict[ClientOrderID, OrderSnapshot] = {}
        self._venue_ids: dict[ClientOrderID, OrderID] = {}
        self._client_ids: dict[OrderID, ClientOrderID] = {}
        self._lock = RLock()

    def close(self) -> None:
        """Release the network client owned by this adapter.

        Notes
        -----
        - A caller-supplied client remains under caller ownership.
        """
        if self._owns_client:
            self._client.close()
            self._owns_client = False

    def preload(self, contract_ids: tuple[ContractID, ...]) -> None:
        """Warm AGG metadata for contracts that may execute soon.

        Parameters
        ----------
        contract_ids
            AGG contract identifiers monitored by the runtime.

        Notes
        -----
        - The current paper endpoint requires no metadata request before order
          submission, so this remains intentionally empty.
        """

    def prepare(self, intent: OrderIntent) -> PreparedOrder:
        """Build an exact, recoverable AGG request without submitting it.

        Parameters
        ----------
        intent
            Venue-independent order parameters to translate into AGG fields.

        Returns
        -------
        PreparedOrder
            Serialized request and opaque recovery data persisted by the
            application journal before submission.
        """
        client_order_id = intent.client_order_id or ClientOrderID(uuid4().hex)
        payload: dict[str, Any] = {
            "venueMarketOutcomeId": _agg_outcome_id(intent.contract_id),
            "side": intent.side.value,
            "shares": float(intent.quantity.value),
            "clientOrderId": str(client_order_id),
        }
        if self._slippage_bps is not None:
            payload["slippageBps"] = self._slippage_bps
        recovery = {
            "schema": 1,
            "client_order_id": str(client_order_id),
            "intent": _intent_data(intent),
        }
        return PreparedOrder(
            reference=OrderReference(
                venue_id=AGG_VENUE_ID,
                client_order_id=client_order_id,
                recovery_data=json.dumps(recovery, separators=(",", ":")).encode(),
            ),
            request=json.dumps(payload, separators=(",", ":")).encode(),
        )

    @ORDER_LATENCY.labels("agg", "submit").time()
    def submit(self, order: PreparedOrder) -> SubmissionResult:
        """Submit the exact prepared request to AGG.

        Parameters
        ----------
        order
            Prepared request previously persisted by the journal.

        Returns
        -------
        SubmissionResult
            Submission certainty and the first normalized paper snapshot.
        """
        recovery = _recovery_data(order.reference)
        payload = json.loads(order.request)
        if payload.get("clientOrderId") != recovery["client_order_id"]:
            raise ValueError("AGG request and recovery client ids differ")
        intent = _intent_from_data(
            recovery["intent"],
            order.reference.client_order_id,
        )
        try:
            raw = self._request("POST", self._orders_path, payload)
        except httpx.HTTPStatusError as error:
            status = (
                SubmissionStatus.REJECTED
                if 400 <= error.response.status_code < 500
                else SubmissionStatus.UNKNOWN
            )
            return SubmissionResult(
                status=status,
                reference=order.reference,
                reason=_response_reason(error.response),
            )
        except httpx.TransportError:
            return SubmissionResult(
                status=SubmissionStatus.UNKNOWN,
                reference=order.reference,
            )

        if not isinstance(raw, dict) or not raw.get("id"):
            return SubmissionResult(
                status=SubmissionStatus.UNKNOWN,
                reference=order.reference,
                reason="AGG accepted the request without an order id",
            )
        snapshot = _order_snapshot(raw, intent, order.reference.client_order_id)
        self._remember(snapshot)
        return SubmissionResult(
            status=SubmissionStatus.ACCEPTED,
            reference=order.reference,
            snapshot=snapshot,
            reason=_order_reason(raw) if snapshot.is_terminal() else None,
        )

    @ORDER_LATENCY.labels("agg", "get").time()
    def reconcile(self, reference: OrderReference) -> ReconciliationResult:
        """Resolve a persisted AGG paper order by client identifier.

        Parameters
        ----------
        reference
            Recovery reference created by :meth:`prepare`.

        Returns
        -------
        ReconciliationResult
            Found, proven absent, or uncertain paper-order state.
        """
        recovery = _recovery_data(reference)
        try:
            payload = self._request(
                "GET",
                self._orders_path,
                query={
                    "limit": "100",
                    "clientOrderId": recovery["client_order_id"],
                },
            )
        except httpx.HTTPStatusError as error:
            status = (
                ReconciliationStatus.NOT_FOUND
                if error.response.status_code == 404
                else ReconciliationStatus.UNKNOWN
            )
            return ReconciliationResult(status=status, reference=reference)
        except httpx.TransportError:
            return ReconciliationResult(
                status=ReconciliationStatus.UNKNOWN,
                reference=reference,
            )

        intent = _intent_from_data(
            recovery["intent"],
            reference.client_order_id,
        )
        for raw in _order_rows(payload):
            if _raw_client_order_id(raw) != reference.client_order_id:
                continue
            snapshot = _order_snapshot(raw, intent, reference.client_order_id)
            self._remember(snapshot)
            return ReconciliationResult(
                status=ReconciliationStatus.FOUND,
                reference=reference,
                snapshot=snapshot,
            )
        return ReconciliationResult(
            status=ReconciliationStatus.NOT_FOUND,
            reference=reference,
        )

    @ORDER_LATENCY.labels("agg", "cancel").time()
    def cancel(self, reference: OrderReference) -> ReconciliationResult:
        """Return the current state of an AGG paper order.

        Parameters
        ----------
        reference
            Persisted order reference.

        Returns
        -------
        ReconciliationResult
            Terminal state when known, or ``UNKNOWN`` when cancellation cannot
            be proven through the current AGG paper contract.
        """
        current = self.reconcile(reference)
        if current.status is not ReconciliationStatus.FOUND:
            return current
        if current.snapshot is not None and current.snapshot.is_terminal():
            return current
        return ReconciliationResult(
            status=ReconciliationStatus.UNKNOWN,
            reference=reference,
        )

    def submit_order(self, intent: OrderIntent) -> ClientOrderID:
        """Submit an order through the recoverable path.

        This compatibility wrapper is retained for callers using the former
        direct adapter API.
        """
        result = self.submit(self.prepare(intent))
        if result.status is not SubmissionStatus.ACCEPTED:
            raise RuntimeError(result.reason or f"AGG submission {result.status.value}")
        return result.reference.client_order_id

    @ORDER_LATENCY.labels("agg", "cancel").time()
    def cancel_order(self, client_order_id: ClientOrderID) -> ClientOrderID:
        """Retain the former cancellation API for settled paper orders."""
        with self._lock:
            snapshot = self._orders.get(client_order_id)
        if snapshot is None:
            raise KeyError(f"Unknown client order ID: {client_order_id}")
        if not snapshot.is_terminal():
            raise NotImplementedError(
                "AGG paper orders are settled immediately and cannot be canceled",
            )
        return client_order_id

    def get_order(self, client_order_id: ClientOrderID) -> OrderSnapshot | None:
        """Refresh and return one cached order by client identifier."""
        self._refresh()
        with self._lock:
            return self._orders.get(client_order_id)

    def get_order_by_venue_id(self, order_id: OrderID) -> OrderSnapshot | None:
        """Refresh and return one cached order by AGG order identifier."""
        self._refresh()
        with self._lock:
            client_order_id = self._client_ids.get(order_id)
            return self._orders.get(client_order_id) if client_order_id else None

    def list_open_orders(self) -> tuple[OrderSnapshot, ...]:
        """Refresh and return cached paper orders that remain open."""
        self._refresh()
        with self._lock:
            return tuple(snapshot for snapshot in self._orders.values() if snapshot.is_open())

    @property
    def _orders_path(self) -> str:
        return f"/apps/{self._app_id}/paper-trading/accounts/{self._account_id}/orders"

    def _refresh(self) -> None:
        """Refresh cached order state from the paper account."""
        payload = self._request("GET", self._orders_path, query={"limit": "100"})
        for row in _order_rows(payload):
            client_order_id = _raw_client_order_id(row)
            if client_order_id is None:
                continue
            with self._lock:
                existing = self._orders.get(client_order_id)
            if existing is not None:
                self._remember(
                    _order_snapshot(row, _intent_from_snapshot(existing), client_order_id),
                )

    def _request(
        self,
        method: str,
        path: str,
        body: dict[str, Any] | None = None,
        *,
        query: dict[str, str] | None = None,
    ) -> Any:
        """Send an authenticated AGG request and unwrap object responses."""
        headers = {"x-app-id": self._app_id}
        if self._api_key:
            headers["x-app-api-key"] = self._api_key
        if self._admin_key:
            headers["x-admin-key"] = self._admin_key
        response = self._client.request(
            method,
            f"{self._base_url}{path}",
            headers=headers,
            json=body,
            params=query,
        )
        response.raise_for_status()
        payload = response.json()
        if isinstance(payload, dict) and isinstance(payload.get("data"), dict):
            return payload["data"]
        return payload

    def _remember(self, snapshot: OrderSnapshot) -> None:
        """Index the latest snapshot by every available identifier."""
        with self._lock:
            if snapshot.client_order_id is not None:
                self._orders[snapshot.client_order_id] = snapshot
            if snapshot.client_order_id is not None and snapshot.order_id is not None:
                self._venue_ids[snapshot.client_order_id] = snapshot.order_id
                self._client_ids[snapshot.order_id] = snapshot.client_order_id


def _agg_outcome_id(contract_id: ContractID) -> str:
    """Extract the AGG venue outcome identifier from a normalized contract id."""
    value = str(contract_id)
    if value.startswith("agg:"):
        value = value.removeprefix("agg:")
    if not value:
        raise ValueError("AGG contract ID must contain a venue market outcome ID")
    return value


def _recovery_data(reference: OrderReference) -> dict[str, Any]:
    """Decode and validate AGG recovery data."""
    if reference.venue_id != AGG_VENUE_ID:
        raise ValueError(f"AGG cannot handle venue {reference.venue_id}")
    data = json.loads(reference.recovery_data)
    if data.get("schema") != 1 or data.get("client_order_id") != str(reference.client_order_id):
        raise ValueError("Invalid AGG recovery data")
    return data


def _intent_data(intent: OrderIntent) -> dict[str, Any]:
    """Serialize the intent fields needed to normalize recovery responses."""
    return {
        "contract_id": str(intent.contract_id),
        "side": intent.side.value,
        "quantity": str(intent.quantity.value),
        "order_type": intent.order_type.value,
        "limit_price": str(intent.limit_price.value) if intent.limit_price else None,
        "created_at": intent.created_at.value.isoformat() if intent.created_at else None,
    }


def _intent_from_data(
    data: dict[str, Any],
    client_order_id: ClientOrderID,
) -> OrderIntent:
    """Restore an order intent from prepared recovery data."""
    return OrderIntent(
        contract_id=ContractID(data["contract_id"]),
        side=OrderSide(data["side"]),
        quantity=Quantity(Decimal(data["quantity"])),
        order_type=OrderType(data["order_type"]),
        client_order_id=client_order_id,
        limit_price=Price(Decimal(data["limit_price"])) if data.get("limit_price") else None,
        created_at=Timestamp.from_iso(data["created_at"]) if data.get("created_at") else None,
    )


def _order_rows(payload: Any) -> tuple[dict[str, Any], ...]:
    """Normalize AGG list and single-order response shapes."""
    if isinstance(payload, list):
        return tuple(row for row in payload if isinstance(row, dict))
    if not isinstance(payload, dict):
        return ()
    rows = payload.get("data")
    if isinstance(rows, list):
        return tuple(row for row in rows if isinstance(row, dict))
    return (payload,)


def _order_snapshot(
    raw: dict[str, Any],
    intent: OrderIntent,
    client_order_id: ClientOrderID,
) -> OrderSnapshot:
    """Normalize one AGG order payload into a domain snapshot."""
    order_id = OrderID(str(raw["id"])) if raw.get("id") else None
    filled = Quantity(Decimal(str(raw.get("filledShares") or 0)))
    average = _price(raw.get("avgPrice"))
    status = {
        "filled": OrderStatus.FILLED,
        "partially_filled": OrderStatus.PARTIALLY_FILLED,
        "partial": OrderStatus.PARTIALLY_FILLED,
        "accepted": OrderStatus.ACCEPTED,
        "rejected": OrderStatus.REJECTED,
        "cancelled": OrderStatus.CANCELLED,
        "canceled": OrderStatus.CANCELLED,
        "expired": OrderStatus.EXPIRED,
    }.get(str(raw.get("status") or "").lower(), OrderStatus.SUBMITTED)
    if filled.value > 0 and average is None:
        average = intent.limit_price or Price(Decimal("0"))
    return OrderSnapshot(
        status=status,
        contract_id=intent.contract_id,
        side=intent.side,
        quantity=intent.quantity,
        order_type=intent.order_type,
        client_order_id=client_order_id,
        order_id=order_id,
        limit_price=intent.limit_price,
        filled_quantity=filled,
        average_price=average,
        created_at=_timestamp(raw.get("createdAt")) or intent.created_at or Timestamp.now(),
        updated_at=(
            _timestamp(raw.get("updatedAt"))
            or _timestamp(raw.get("createdAt"))
            or Timestamp.now()
        ),
    )


def _raw_client_order_id(raw: dict[str, Any]) -> ClientOrderID | None:
    """Extract a client order identifier without failing on malformed rows."""
    value = raw.get("clientOrderId")
    if value:
        return ClientOrderID(str(value))
    if raw.get("id"):
        return ClientOrderID(f"agg:{raw['id']}")
    return None


def _client_order_id(raw: dict[str, Any]) -> ClientOrderID:
    """Extract an AGG client identifier from a valid order row."""
    value = _raw_client_order_id(raw)
    if value is None:
        raise ValueError("AGG order payload has no order identifier")
    return value


def _intent_from_snapshot(snapshot: OrderSnapshot) -> OrderIntent:
    """Rebuild an intent for refreshing a cached order."""
    return OrderIntent(
        contract_id=snapshot.contract_id,
        side=snapshot.side,
        quantity=snapshot.quantity,
        order_type=snapshot.order_type,
        client_order_id=snapshot.client_order_id,
        limit_price=snapshot.limit_price,
    )


def _price(value: Any) -> Price | None:
    """Normalize an optional AGG price."""
    return Price(Decimal(str(value))) if value is not None else None


def _timestamp(value: Any) -> Timestamp | None:
    """Normalize an optional AGG timestamp."""
    if value is None:
        return None
    return Timestamp.from_iso(str(value))


def _order_reason(raw: dict[str, Any]) -> str | None:
    """Extract a terminal reason exposed by AGG."""
    for key in ("reason", "failureReason", "error", "message"):
        value = raw.get(key)
        if value:
            return str(value)
    return None


def _response_reason(response: httpx.Response) -> str:
    """Extract a compact reason from an unsuccessful HTTP response."""
    try:
        payload = response.json()
    except ValueError:
        return f"HTTP {response.status_code}: {response.text}".strip()
    if isinstance(payload, dict):
        for key in ("reason", "error", "message", "detail"):
            if payload.get(key):
                return str(payload[key])
        data = payload.get("data")
        if isinstance(data, dict):
            for key in ("reason", "error", "message", "detail"):
                if data.get(key):
                    return str(data[key])
    return f"HTTP {response.status_code}"
