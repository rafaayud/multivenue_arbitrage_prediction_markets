"""Define typed inputs, internal events, and commands for the trading pipeline.

Responsibilities
----------------
- Carry normalized data between adapters, the engine, the journal, and projections.
- Keep venue-specific payloads opaque after order preparation.
"""

from dataclasses import dataclass
from typing import Literal, TypeAlias

from prediction_markets.application.markets.models import MonitoredMarket
from prediction_markets.domain.arbitrage.services import ArbitragePlan
from prediction_markets.domain.arbitrage.value_objects import ArbitrageOpportunity
from prediction_markets.domain.market_matching.value_objects import MatchedContractPair
from prediction_markets.domain.outcome_inventory import (
    InventoryReconciliationResult,
    InventorySubmissionResult,
    OutcomeInventorySettlement,
    PreparedInventoryOperation,
)
from prediction_markets.domain.orderbook.entities import OrderBook
from prediction_markets.domain.shared.value_objects import (
    ClientOrderID,
    ContractID,
    Timestamp,
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
from prediction_markets.domain.trading.entities import ArbitrageExecutionJournal
from prediction_markets.domain.trading.entities import ExposureRecovery
from prediction_markets.domain.trading.value_objects import (
    OrderReference,
    PreparedOrder,
    SubmissionResult,
)


@dataclass(frozen=True, slots=True)
class OpportunityValidationRef:
    """Identify the worker snapshot that authorized central admission."""

    intent_id: str
    partition: str
    process_generation: str
    sequence: int
    left_book_generation: str
    right_book_generation: str


@dataclass(frozen=True, slots=True)
class MarketMatchesUpdated:
    """Replace matched contracts for one monitored market selection."""

    # Keep the historical field name for journal compatibility.
    cycle: MonitoredMarket
    pairs: tuple[MatchedContractPair, ...]


@dataclass(frozen=True, slots=True)
class OrderBookUpdated:
    """Carry the latest executable book for one venue contract."""

    venue_id: VenueID
    contract_id: ContractID
    order_book: OrderBook


@dataclass(frozen=True, slots=True)
class ArbitrageOpportunityFound:
    """Record a fee-aware opportunity produced from a matched pair."""

    id: str
    cycle: MonitoredMarket
    pair: MatchedContractPair
    opportunity: ArbitrageOpportunity
    validation_ref: OpportunityValidationRef | None = None


@dataclass(frozen=True, slots=True)
class OrderBookPairUpdated:
    """Carry one worker-detected opportunity and its atomic book pair."""

    detected: ArbitrageOpportunityFound
    left_order_book: OrderBook
    right_order_book: OrderBook


@dataclass(frozen=True, slots=True)
class ArbitragePlanned:
    """Record the domain plan and its recoverable execution state."""

    opportunity_id: str
    cycle: MonitoredMarket
    pair: MatchedContractPair
    plan: ArbitragePlan
    execution: ArbitrageExecutionJournal


@dataclass(frozen=True, slots=True)
class SubmitOrder:
    """Request preparation and submission of one arbitrage leg."""

    execution_id: str
    role: Literal["primary", "hedge", "recovery"]
    venue_id: VenueID
    intent: OrderIntent


@dataclass(frozen=True, slots=True)
class OrderPrepared:
    """Persist the exact opaque venue request before network submission."""

    command: SubmitOrder
    prepared: PreparedOrder


@dataclass(frozen=True, slots=True)
class OrderCancellationPrepared:
    """Persist an exact cancellation transaction before its first broadcast."""

    command: SubmitOrder
    request: bytes


@dataclass(frozen=True, slots=True)
class ExecutionPreparationRequested:
    """Carry one admitted execution to asynchronous request preparation."""

    opportunity: ArbitrageOpportunityFound
    planned: ArbitragePlanned
    commands: tuple[SubmitOrder, SubmitOrder]
    deadline_at_ns: int
    deadline_wall_at_ns: int


@dataclass(frozen=True, slots=True)
class PreparedExecutionBatch:
    """Persist a complete pre-submission decision in one journal frame."""

    opportunity: ArbitrageOpportunityFound
    planned: ArbitragePlanned
    commands: tuple[SubmitOrder, SubmitOrder]
    prepared: tuple[OrderPrepared, ...]
    deadline_wall_at_ns: int
    rejection_reason: str | None = None


def prepared_execution_events(
    batch: PreparedExecutionBatch,
) -> tuple[
    ArbitrageOpportunityFound | ArbitragePlanned | SubmitOrder | OrderPrepared,
    ...,
]:
    """Expand one durable batch into its ordered state transitions."""
    return (
        batch.opportunity,
        batch.planned,
        *batch.commands,
        *batch.prepared,
    )


@dataclass(frozen=True, slots=True)
class SubmissionReceived:
    """Carry the normalized certainty returned by an execution adapter."""

    command: SubmitOrder
    result: SubmissionResult


@dataclass(frozen=True, slots=True)
class OrderSnapshotUpdated:
    """Carry a newer private-stream or reconciliation order snapshot."""

    execution_id: str
    role: Literal["primary", "hedge", "recovery"]
    reference: OrderReference
    snapshot: OrderSnapshot
    source: Literal["ws", "get"]


@dataclass(frozen=True, slots=True)
class TradeRecorded:
    """Record the incremental fill derived from a cumulative order snapshot."""

    trade: Trade


@dataclass(frozen=True, slots=True)
class PositionUpdated:
    """Record the position resulting from an incremental fill."""

    position: Position


@dataclass(frozen=True, slots=True)
class ExecutionUpdated:
    """Replace recoverable execution state and optionally persist its trace.

    Attributes
    ----------
    execution
        Latest durable state of the two-leg execution.
    latency_trace_json
        Terminal process-local latency snapshot serialized as JSON, when both
        legs have settled.
    """

    execution: ArbitrageExecutionJournal
    latency_trace_json: str | None = None


@dataclass(frozen=True, slots=True)
class RecoveryPlanningRequested:
    """Request current books before choosing a recovery route, without journaling I/O.

    Attributes
    ----------
    deadline_at_ns
        Absolute process-local monotonic deadline shared by quote-wait retries.
        ``None`` preserves callers without an enclosing quote-wait budget.
    not_before_ns
        Earliest monotonic time to start the next serial lookup, or ``None``
        for an immediate initial lookup. These clocks are not persisted.
    """

    request_id: str
    execution: ArbitrageExecutionJournal
    deadline_at_ns: int | None = None
    not_before_ns: int | None = None


@dataclass(frozen=True, slots=True)
class RecoveryBooksReceived:
    """Return an ephemeral recovery pair in execution-leg order with original clocks.

    Attributes
    ----------
    fresh_contract_ids
        Contracts passing dispatcher freshness checks at receipt. This routing
        hint never replaces the final submission guard; ``None`` omits ranking.
    """

    request: RecoveryPlanningRequested
    books: tuple[OrderBook, OrderBook] | None = None
    error: str | None = None
    fresh_contract_ids: frozenset[ContractID] | None = None


@dataclass(frozen=True, slots=True)
class RecoveryPlanned:
    """Record the selected bounded recovery order before dispatch."""

    recovery: ExposureRecovery


@dataclass(frozen=True, slots=True)
class RecoveryUpdated:
    """Replace the durable state and realized economics of one recovery."""

    recovery: ExposureRecovery


@dataclass(frozen=True, slots=True)
class InventoryOperationRecorded:
    """Persist one prepared, submitted, or reconciled inventory operation."""

    record: (
        PreparedInventoryOperation
        | InventorySubmissionResult
        | InventoryReconciliationResult
    )


@dataclass(frozen=True, slots=True)
class MarketSettlementRecorded:
    """Persist one venue-verified final binary payout vector."""

    settlement: OutcomeInventorySettlement


@dataclass(frozen=True, slots=True)
class CashMovementRecorded:
    """Persist one deposit, withdrawal, or inter-portfolio transfer."""

    movement: CashMovement


@dataclass(frozen=True, slots=True)
class AccountingCorrectionRecorded:
    """Persist an explicit replacement for one trade and derived position."""

    correction: AccountingCorrection


@dataclass(frozen=True, slots=True)
class TradingSafetyStop:
    """Halt live trading and optionally identify an order requiring review.

    Notes
    -----
    - Missing execution and client identifiers preserve venue-wide legacy stops.
    """

    venue_id: VenueID
    reason: str
    detected_at: Timestamp
    execution_id: str | None = None
    client_order_id: ClientOrderID | None = None


ApplicationEvent: TypeAlias = (
    MarketMatchesUpdated
    | OrderBookUpdated
    | OrderBookPairUpdated
    | ArbitrageOpportunityFound
    | ArbitragePlanned
    | SubmitOrder
    | OrderPrepared
    | OrderCancellationPrepared
    | PreparedExecutionBatch
    | SubmissionReceived
    | OrderSnapshotUpdated
    | TradeRecorded
    | PositionUpdated
    | ExecutionUpdated
    | RecoveryPlanningRequested
    | RecoveryBooksReceived
    | RecoveryPlanned
    | RecoveryUpdated
    | InventoryOperationRecorded
    | MarketSettlementRecorded
    | CashMovementRecorded
    | AccountingCorrectionRecorded
    | TradingSafetyStop
)
