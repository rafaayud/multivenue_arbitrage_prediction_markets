"""Translate durable trading events into alerting directives.

Responsibilities
----------------
- Define immutable commands for incident lifecycle changes.
- Map supported application events without I/O or mutable policy state.
"""

from dataclasses import dataclass
from typing import TypeAlias

from prediction_markets.application.events import (
    ApplicationEvent,
    ExecutionUpdated,
    InventoryOperationRecorded,
    SubmissionReceived,
    TradingSafetyStop,
)
from prediction_markets.domain.alerting.enums import Severity
from prediction_markets.domain.alerting.value_objects import (
    AlertSource,
    Fingerprint,
    IncidentID,
)
from prediction_markets.domain.outcome_inventory import (
    InventoryOperationStatus,
    InventoryReconciliationResult,
    InventoryReconciliationStatus,
    InventorySubmissionResult,
    InventorySubmissionStatus,
)
from prediction_markets.domain.shared.value_objects import Timestamp
from prediction_markets.domain.trading.enums import (
    ArbitrageExecutionStatus,
    SubmissionStatus,
)
from prediction_markets.domain.trading.entities import ArbitrageExecutionJournal

_EXECUTION_ALERT_PREFIX = "execution-risk"
_INVENTORY_ALERT_PREFIX = "inventory-operation"
_TRADING_SAFETY_ALERT_PREFIX = "trading-safety"


@dataclass(frozen=True, slots=True)
class OpenIncident:
    """Request creation or correlation of an active incident.

    Attributes
    ----------
    incident_id
        Deterministic incident identifier.
    fingerprint
        Stable correlation key shared by later directives.
    severity
        Initial incident severity.
    source
        Component that produced the condition.
    description
        Human-readable explanation of the condition.
    opened_at
        Instant when the condition entered the alerting lifecycle.
    title
        Optional concise incident title.
    """

    incident_id: IncidentID
    fingerprint: Fingerprint
    severity: Severity
    source: AlertSource
    description: str
    opened_at: Timestamp
    title: str | None = None


@dataclass(frozen=True, slots=True)
class AcknowledgeIncident:
    """Request acknowledgement of an active incident.

    Attributes
    ----------
    incident_id
        Identifier of the incident to acknowledge.
    fingerprint
        Stable incident correlation key.
    acknowledged_at
        Instant recorded for the acknowledgement.
    """

    incident_id: IncidentID
    fingerprint: Fingerprint
    acknowledged_at: Timestamp


@dataclass(frozen=True, slots=True)
class MarkIncidentInProgress:
    """Request transition of an acknowledged incident into active investigation.

    Attributes
    ----------
    incident_id
        Identifier of the incident under investigation.
    fingerprint
        Stable incident correlation key.
    in_progress_at
        Instant when active investigation started.
    """

    incident_id: IncidentID
    fingerprint: Fingerprint
    in_progress_at: Timestamp


@dataclass(frozen=True, slots=True)
class ResolveIncident:
    """Request resolution of an active incident.

    Attributes
    ----------
    incident_id
        Identifier of the incident to resolve.
    fingerprint
        Stable incident correlation key.
    resolved_at
        Instant when the triggering condition cleared.
    """

    incident_id: IncidentID
    fingerprint: Fingerprint
    resolved_at: Timestamp


@dataclass(frozen=True, slots=True)
class NotifyIncident:
    """Request a one-shot operator notification without opening an incident."""

    fingerprint: Fingerprint
    severity: Severity
    source: AlertSource
    description: str
    notified_at: Timestamp
    title: str | None = None


@dataclass(frozen=True, slots=True)
class ChangeIncidentSeverity:
    """Request replacement of an active incident severity.

    Attributes
    ----------
    incident_id
        Identifier of the incident whose severity changes.
    fingerprint
        Stable incident correlation key.
    severity
        New severity to apply.
    changed_at
        Instant when the severity decision was made.
    """

    incident_id: IncidentID
    fingerprint: Fingerprint
    severity: Severity
    changed_at: Timestamp


AlertingDirective: TypeAlias = (
    OpenIncident
    | AcknowledgeIncident
    | MarkIncidentInProgress
    | ResolveIncident
    | ChangeIncidentSeverity
    | NotifyIncident
)


def directives_for(
    event: ApplicationEvent,
    *,
    recorded_at: Timestamp | None = None,
) -> tuple[AlertingDirective, ...]:
    """Return alerting directives implied by one durable application event.

    Parameters
    ----------
    event
        Trading event read from the durable journal.
    recorded_at
        Durable journal timestamp, required for conditions whose source event
        carries no domain timestamp.

    Returns
    -------
    tuple[AlertingDirective, ...]
        Deterministic incident changes, or an empty tuple when the event does
        not affect alerting.

    Notes
    -----
    - Resolution directives may have no matching open incident when an
      execution completed normally. Projectors must treat that case as an
      idempotent no-op.
    """
    if isinstance(event, SubmissionReceived):
        if event.result.status is not SubmissionStatus.UNKNOWN:
            return ()
        at = recorded_at or event.command.intent.created_at
        if at is None:
            raise ValueError("recorded_at is required for uncertain submissions")
        reason = event.result.reason or "venue outcome is unknown"
        return _open_execution_incident(
            event.command.execution_id,
            severity=Severity.UNKNOWN,
            description=f"{event.command.role} submission uncertain: {reason}",
            opened_at=at,
            title="Order submission outcome is unknown",
            component="trading-submission",
        )

    if isinstance(event, InventoryOperationRecorded):
        return _inventory_directives(event.record, recorded_at)

    if isinstance(event, TradingSafetyStop):
        key = f"{_TRADING_SAFETY_ALERT_PREFIX}:{event.venue_id}"
        return (
            OpenIncident(
                incident_id=IncidentID(key),
                fingerprint=Fingerprint(key),
                severity=Severity.CRITICAL,
                source=AlertSource(
                    component="trading-safety-circuit",
                    service=str(event.venue_id),
                ),
                description=event.reason,
                opened_at=event.detected_at,
                title="Live trading stopped by venue safety circuit",
            ),
        )

    if not isinstance(event, ExecutionUpdated):
        return ()

    execution = event.execution

    if execution.status in {
        ArbitrageExecutionStatus.RECOVERY_PENDING,
        ArbitrageExecutionStatus.UNWIND_PENDING,
    }:
        return _open_execution_incident(
            execution.id,
            severity=Severity.MAJOR,
            description=(
                execution.last_error
                or "Execution has residual exposure under automatic recovery"
            ),
            opened_at=execution.updated_at,
            title="Automatic exposure recovery is pending",
        )

    if execution.status is ArbitrageExecutionStatus.NEEDS_REVIEW:
        return _open_execution_incident(
            execution.id,
            severity=Severity.CRITICAL,
            description=(
                execution.last_error or "Execution requires manual review"
            ),
            opened_at=execution.updated_at,
            title="Execution requires manual review",
        )

    if execution.status is ArbitrageExecutionStatus.REJECTED:
        incident_id, fingerprint = _execution_identity(execution.id)
        return (
            ResolveIncident(
                incident_id=incident_id,
                fingerprint=fingerprint,
                resolved_at=execution.updated_at,
            ),
        )

    if execution.status is ArbitrageExecutionStatus.RECOVERED:
        return _terminal_trade_incident(
            execution,
            title="Trade recovered",
            key_prefix="trade-recovered",
            description=f"Execution {execution.id} recovered",
        )
    if execution.status is ArbitrageExecutionStatus.COMPLETED:
        if execution.resolution_method is not None:
            return _terminal_trade_incident(
                execution,
                title="Trade manually resolved",
                key_prefix="trade-manually-resolved",
                description=(
                    f"Execution {execution.id} manually resolved "
                    f"via {execution.resolution_method}"
                ),
            )
        if execution.leg1_filled_quantity != execution.leg2_filled_quantity:
            return _terminal_trade_incident(
                execution,
                title="Trade closed",
                key_prefix="trade-closed",
                description=f"Execution {execution.id} closed with unequal initial fills",
            )
        return _terminal_trade_incident(
            execution,
            title="Trade completed",
            key_prefix="trade-completed",
            description=f"Execution {execution.id} completed",
        )
    return ()


def _terminal_trade_incident(
    execution: ArbitrageExecutionJournal,
    *,
    title: str,
    key_prefix: str,
    description: str,
) -> tuple[AlertingDirective, ...]:
    """Resolve execution-risk and enqueue one informational trade notification.

    Parameters
    ----------
    execution
        Terminal execution whose risk incident can close.
    title
        Alertmanager ``alertname`` for the informational notification.
    key_prefix
        Fingerprint prefix that distinguishes terminal execution outcomes.
    description
        Human-readable body copied into the alert annotation.
    """
    incident_id, fingerprint = _execution_identity(execution.id)
    trade_key = f"{key_prefix}:{execution.id}"
    return (
        ResolveIncident(
            incident_id=incident_id,
            fingerprint=fingerprint,
            resolved_at=execution.updated_at,
        ),
        NotifyIncident(
            fingerprint=Fingerprint(trade_key),
            severity=Severity.INFORMATIONAL,
            source=AlertSource(component="trading-execution"),
            description=description,
            notified_at=execution.updated_at,
            title=title,
        ),
    )


def _open_execution_incident(
    execution_id: str,
    *,
    severity: Severity,
    description: str,
    opened_at: Timestamp,
    title: str,
    component: str = "trading-execution",
) -> tuple[OpenIncident, ...]:
    """Build one correlated execution incident directive."""
    incident_id, fingerprint = _execution_identity(execution_id)
    return (
        OpenIncident(
            incident_id=incident_id,
            fingerprint=fingerprint,
            severity=severity,
            source=AlertSource(component=component),
            description=description,
            opened_at=opened_at,
            title=title,
        ),
    )


def _execution_identity(execution_id: str) -> tuple[IncidentID, Fingerprint]:
    """Return the stable alert identity shared by one execution lifecycle."""
    key = f"{_EXECUTION_ALERT_PREFIX}:{execution_id}"
    return IncidentID(key), Fingerprint(key)


def _inventory_directives(
    record: object,
    recorded_at: Timestamp | None,
) -> tuple[AlertingDirective, ...]:
    """Translate terminal or uncertain inventory results into directives."""
    if not isinstance(
        record,
        (InventorySubmissionResult, InventoryReconciliationResult),
    ):
        return ()
    reference = record.reference
    key = f"{_INVENTORY_ALERT_PREFIX}:{reference.operation_id}"
    incident_id = IncidentID(key)
    fingerprint = Fingerprint(key)
    snapshot = record.snapshot
    at = snapshot.updated_at if snapshot is not None else recorded_at
    if snapshot is not None and snapshot.status is InventoryOperationStatus.CONFIRMED:
        return (
            ResolveIncident(
                incident_id=incident_id,
                fingerprint=fingerprint,
                resolved_at=snapshot.updated_at,
            ),
        )

    severity: Severity | None = None
    description: str | None = None
    if snapshot is not None and snapshot.status is InventoryOperationStatus.FAILED:
        severity = Severity.MINOR
        description = snapshot.reason or "Inventory operation failed"
    elif (
        isinstance(record, InventorySubmissionResult)
        and record.status is InventorySubmissionStatus.REJECTED
    ):
        severity = Severity.MINOR
        description = record.reason or "Inventory operation was rejected"
    elif (
        isinstance(record, InventorySubmissionResult)
        and record.status is InventorySubmissionStatus.UNKNOWN
    ) or (
        isinstance(record, InventoryReconciliationResult)
        and record.status is InventoryReconciliationStatus.UNKNOWN
    ):
        severity = Severity.UNKNOWN
        description = "Inventory operation outcome is unknown"

    if severity is None:
        return ()
    if at is None:
        raise ValueError("recorded_at is required for inventory alerts")
    return (
        OpenIncident(
            incident_id=incident_id,
            fingerprint=fingerprint,
            severity=severity,
            source=AlertSource(
                component="outcome-inventory",
                service=str(reference.venue_id),
            ),
            description=description or "Inventory operation requires attention",
            opened_at=at,
            title="Outcome inventory operation requires attention",
        ),
    )
