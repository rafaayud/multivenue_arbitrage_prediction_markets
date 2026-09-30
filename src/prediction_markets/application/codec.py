"""Encode typed application events into a stable local journal representation.

Responsibilities
----------------
- Serialize approved domain and application dataclasses without executable payloads.
- Reject unknown type tags while reading a journal.

Notes
-----
- The codec is intentionally explicit: journal data never controls Python imports.
"""

import base64
import json
from dataclasses import fields, is_dataclass
from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Any

from prediction_markets.application import events
from prediction_markets.application.markets.models import MarketCycle, MarketFamily
from prediction_markets.domain.arbitrage.services import ArbitrageLegPlan, ArbitragePlan
from prediction_markets.domain.arbitrage.value_objects import ArbitrageOpportunity
from prediction_markets.domain.contracts.entities import BinaryContract
from prediction_markets.domain.contracts.value_objects import LotSize, Payout, TickSize
from prediction_markets.domain.market_matching.value_objects import (
    MatchedContractPair,
    RegularCandidate,
    Underlying,
)
from prediction_markets.domain.markets.entities import Market, MarketSide
from prediction_markets.domain.markets.enums import BinaryOutcome, MarketStatus
from prediction_markets.domain.markets.value_objects import MarketResolution, MarketState
from prediction_markets.domain.orderbook.entities import OrderBook
from prediction_markets.domain.orderbook.value_objects import OrderBookLevel
from prediction_markets.domain.outcome_inventory import (
    InventoryOperationID,
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
from prediction_markets.domain.shared.value_objects import (
    ClientOrderID,
    ContractID,
    Currency,
    EventID,
    MarketID,
    Money,
    OrderID,
    OutcomeID,
    PortfolioID,
    PositionID,
    Price,
    Quantity,
    StrategyID,
    Timestamp,
    TradeID,
    VenueID,
)
from prediction_markets.domain.trading.entities import (
    AccountingCorrection,
    CashMovement,
    OrderIntent,
    OrderSnapshot,
    Position,
    Trade,
)
from prediction_markets.domain.trading.enums import (
    ArbitrageExecutionStatus,
    CashMovementKind,
    OrderSide,
    OrderStatus,
    OrderType,
    PositionSide,
    ReconciliationStatus,
    RecoveryRoute,
    RecoveryStatus,
    SubmissionStatus,
    TimeInForce,
)
from prediction_markets.domain.trading.entities import (
    ArbitrageExecutionJournal,
    ExposureRecovery,
)
from prediction_markets.domain.trading.value_objects import (
    OrderBookDecisionSnapshot,
    OrderReference,
    PreparedOrder,
    ReconciliationResult,
    SubmissionResult,
    TradingFee,
)

_SCHEMA = 1
_APP_TYPES = (
    MarketCycle,
    events.MarketMatchesUpdated,
    events.OrderBookUpdated,
    events.ArbitrageOpportunityFound,
    events.ArbitragePlanned,
    events.SubmitOrder,
    events.OrderPrepared,
    events.OrderCancellationPrepared,
    events.PreparedExecutionBatch,
    events.SubmissionReceived,
    events.OrderSnapshotUpdated,
    events.TradeRecorded,
    events.PositionUpdated,
    events.ExecutionUpdated,
    events.RecoveryPlanned,
    events.RecoveryUpdated,
    events.InventoryOperationRecorded,
    events.MarketSettlementRecorded,
    events.CashMovementRecorded,
    events.AccountingCorrectionRecorded,
    events.TradingSafetyStop,
)
_DOMAIN_TYPES = (
    ArbitrageLegPlan,
    ArbitragePlan,
    events.OpportunityValidationRef,
    ArbitrageOpportunity,
    ArbitrageExecutionJournal,
    OrderBookDecisionSnapshot,
    ExposureRecovery,
    BinaryContract,
    LotSize,
    MatchedContractPair,
    RegularCandidate,
    Market,
    MarketSide,
    MarketResolution,
    MarketState,
    Payout,
    TickSize,
    Underlying,
    OrderBook,
    OrderBookLevel,
    ClientOrderID,
    ContractID,
    Currency,
    EventID,
    MarketID,
    Money,
    OrderID,
    OutcomeID,
    PortfolioID,
    PositionID,
    Price,
    Quantity,
    StrategyID,
    Timestamp,
    TradeID,
    VenueID,
    OrderIntent,
    OrderSnapshot,
    Position,
    Trade,
    CashMovement,
    AccountingCorrection,
    OrderReference,
    PreparedOrder,
    ReconciliationResult,
    SubmissionResult,
    TradingFee,
    InventoryOperationID,
    InventoryOperationReference,
    InventoryOperationSnapshot,
    InventoryReconciliationResult,
    InventorySubmissionResult,
    OutcomeInventoryBalance,
    OutcomeInventoryIntent,
    OutcomeInventorySettlement,
    PreparedInventoryOperation,
)
_ENUM_TYPES = (
    ArbitrageExecutionStatus,
    BinaryOutcome,
    MarketFamily,
    MarketStatus,
    OrderSide,
    OrderStatus,
    OrderType,
    PositionSide,
    ReconciliationStatus,
    RecoveryRoute,
    RecoveryStatus,
    SubmissionStatus,
    TimeInForce,
    InventoryOperationStatus,
    InventoryReconciliationStatus,
    InventorySubmissionStatus,
    OutcomeInventoryAction,
    CashMovementKind,
)
_TYPE_BY_NAME = {
    f"{type_.__module__}.{type_.__qualname__}": type_
    for type_ in _APP_TYPES + _DOMAIN_TYPES + _ENUM_TYPES
}
# Journals written before MatchedContractPair moved into market_matching.
_TYPE_BY_NAME[
    "prediction_markets.application.models.MatchedContractPair"
] = MatchedContractPair
# Journals written before application modules were grouped by responsibility.
_TYPE_BY_NAME["prediction_markets.application.models.MarketCycle"] = MarketCycle


def encode_event(event: events.ApplicationEvent) -> bytes:
    """Serialize one approved application event as compact UTF-8 JSON.

    Parameters
    ----------
    event
        Typed event or command accepted by the journal.

    Returns
    -------
    bytes
        Deterministic compact JSON containing the codec schema and event value.
    """
    if type(event) not in _APP_TYPES:
        raise TypeError(f"Unsupported journal event: {type(event).__name__}")
    return json.dumps(
        {"schema": _SCHEMA, "event": _encode(event)},
        separators=(",", ":"),
        sort_keys=True,
    ).encode()


def decode_event(payload: bytes) -> events.ApplicationEvent:
    """Deserialize one event while rejecting unknown schemas and type tags.

    Parameters
    ----------
    payload
        JSON bytes previously produced by :func:`encode_event`.

    Returns
    -------
    ApplicationEvent
        Reconstructed immutable event.

    Raises
    ------
    ValueError
        If the schema or encoded value is unsupported.
    """
    data = json.loads(payload)
    if not isinstance(data, dict) or data.get("schema") != _SCHEMA:
        raise ValueError("Unsupported journal event schema")
    value = _decode(data.get("event"))
    if type(value) not in _APP_TYPES:
        raise ValueError("Journal payload is not an application event")
    return value


def event_kind(event: events.ApplicationEvent) -> str:
    """Return the stable short name used by metrics and SQL projections."""
    return type(event).__name__


def _type_name(value: object) -> str:
    type_ = type(value)
    return f"{type_.__module__}.{type_.__qualname__}"


def _encode(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, Decimal):
        return {"$decimal": str(value)}
    if isinstance(value, datetime):
        return {"$datetime": value.isoformat()}
    if isinstance(value, bytes):
        return {"$bytes": base64.b64encode(value).decode()}
    if isinstance(value, Enum):
        return {"$enum": _type_name(value), "value": value.value}
    if isinstance(value, tuple):
        return {"$tuple": [_encode(item) for item in value]}
    if is_dataclass(value):
        name = _type_name(value)
        if name not in _TYPE_BY_NAME:
            raise TypeError(f"Unsupported journal type: {name}")
        return {
            "$type": name,
            "fields": {field.name: _encode(getattr(value, field.name)) for field in fields(value)},
        }
    raise TypeError(f"Unsupported journal value: {type(value).__name__}")


def _decode(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if not isinstance(value, dict):
        raise ValueError("Invalid journal value")
    if "$decimal" in value:
        return Decimal(value["$decimal"])
    if "$datetime" in value:
        return datetime.fromisoformat(value["$datetime"])
    if "$bytes" in value:
        return base64.b64decode(value["$bytes"], validate=True)
    if "$tuple" in value:
        return tuple(_decode(item) for item in value["$tuple"])
    if "$enum" in value:
        type_ = _TYPE_BY_NAME.get(value["$enum"])
        if type_ is None or not issubclass(type_, Enum):
            raise ValueError(f"Unsupported journal enum: {value['$enum']}")
        return type_(value["value"])
    if "$type" in value:
        type_ = _TYPE_BY_NAME.get(value["$type"])
        if type_ is None or not is_dataclass(type_):
            raise ValueError(f"Unsupported journal type: {value['$type']}")
        raw_fields = value.get("fields")
        if not isinstance(raw_fields, dict):
            raise ValueError("Journal dataclass fields must be an object")
        return type_(**{name: _decode(item) for name, item in raw_fields.items()})
    raise ValueError("Unknown journal value tag")
