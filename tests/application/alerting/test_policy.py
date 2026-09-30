"""Verify deterministic translation from trading events to alert directives."""

from dataclasses import replace
from datetime import datetime, timezone
import json
from decimal import Decimal

import pytest

from prediction_markets.application.codec import decode_event, encode_event
from prediction_markets.application.alerting.policy import (
    NotifyIncident,
    OpenIncident,
    ResolveIncident,
    directives_for,
)
from prediction_markets.application.events import (
    ExecutionUpdated,
    InventoryOperationRecorded,
    SubmissionReceived,
    SubmitOrder,
    TradingSafetyStop,
)
from prediction_markets.domain.outcome_inventory import (
    InventoryOperationID,
    InventoryOperationReference,
    InventorySubmissionResult,
    InventorySubmissionStatus,
)
from prediction_markets.domain.trading.entities import OrderIntent
from prediction_markets.domain.alerting.enums import Severity
from prediction_markets.domain.alerting.value_objects import (
    AlertSource,
    Fingerprint,
    IncidentID,
)
from prediction_markets.domain.shared.value_objects import (
    ClientOrderID,
    ContractID,
    Price,
    Quantity,
    Timestamp,
    VenueID,
)
from prediction_markets.domain.trading.enums import (
    ArbitrageExecutionStatus,
    OrderSide,
    OrderType,
    SubmissionStatus,
)
from prediction_markets.domain.trading.entities import ArbitrageExecutionJournal
from prediction_markets.domain.trading.value_objects import (
    OrderReference,
    SubmissionResult,
)

NOW = Timestamp(datetime(2026, 8, 17, tzinfo=timezone.utc))
INCIDENT_KEY = "execution-risk:execution-1"


def _execution(
    status: ArbitrageExecutionStatus,
    *,
    last_error: str | None = None,
) -> ArbitrageExecutionJournal:
    """Build the smallest valid execution for alert policy tests."""
    return ArbitrageExecutionJournal(
        id="execution-1",
        status=status,
        leg1_venue_id=VenueID("venue-1"),
        leg1_contract_id=ContractID("contract-1"),
        leg1_side=OrderSide.BUY,
        leg1_quantity=Quantity(Decimal("1")),
        leg1_limit_price=Price(Decimal("0.4")),
        leg1_client_order_id=ClientOrderID("client-1"),
        leg2_venue_id=VenueID("venue-2"),
        leg2_contract_id=ContractID("contract-2"),
        leg2_side=OrderSide.BUY,
        leg2_quantity=Quantity(Decimal("1")),
        leg2_limit_price=Price(Decimal("0.5")),
        leg2_client_order_id=ClientOrderID("client-2"),
        created_at=NOW,
        updated_at=NOW,
        last_error=last_error,
    )


def test_directives_for_opens_needs_review_incident() -> None:
    """Open one critical incident with stable execution-derived identity."""
    event = ExecutionUpdated(
        _execution(
            ArbitrageExecutionStatus.NEEDS_REVIEW,
            last_error="Residual exposure requires review",
        ),
    )

    assert directives_for(event) == (
        OpenIncident(
            incident_id=IncidentID(INCIDENT_KEY),
            fingerprint=Fingerprint(INCIDENT_KEY),
            severity=Severity.CRITICAL,
            source=AlertSource(component="trading-execution"),
            description="Residual exposure requires review",
            opened_at=NOW,
            title="Execution requires manual review",
        ),
    )


@pytest.mark.parametrize(
    "status",
    [
        ArbitrageExecutionStatus.RECOVERY_PENDING,
        ArbitrageExecutionStatus.UNWIND_PENDING,
    ],
)
def test_directives_for_opens_major_incident_during_automatic_recovery(
    status: ArbitrageExecutionStatus,
) -> None:
    """Notify once execution exposure enters an automatic recovery state."""
    event = ExecutionUpdated(_execution(status, last_error="Residual exposure"))

    directive = directives_for(event)[0]

    assert isinstance(directive, OpenIncident)
    assert directive.fingerprint == Fingerprint(INCIDENT_KEY)
    assert directive.severity is Severity.MAJOR
    assert directive.description == "Residual exposure"


@pytest.mark.parametrize(
    ("status", "outcome"),
    [(ArbitrageExecutionStatus.COMPLETED, "completed"),
     (ArbitrageExecutionStatus.RECOVERED, "recovered")],
)
def test_directives_for_resolves_terminal_execution(
    status: ArbitrageExecutionStatus,
    outcome: str,
) -> None:
    """Distinguish automatic recovery from normal completion."""
    event = ExecutionUpdated(_execution(status))
    trade_key = f"trade-{outcome}:execution-1"

    assert directives_for(event) == (
        ResolveIncident(
            incident_id=IncidentID(INCIDENT_KEY),
            fingerprint=Fingerprint(INCIDENT_KEY),
            resolved_at=NOW,
        ),
        NotifyIncident(
            fingerprint=Fingerprint(trade_key),
            severity=Severity.INFORMATIONAL,
            source=AlertSource(component="trading-execution"),
            description=f"Execution execution-1 {outcome}",
            notified_at=NOW,
            title=f"Trade {outcome}",
        ),
    )


@pytest.mark.parametrize("method", ["manual_sale", "settlement"])
def test_manual_resolution_survives_journal_replay_and_has_its_own_alert(method) -> None:
    """Keep operator closure explicit without inferring it for old records."""
    event = ExecutionUpdated(replace(
        _execution(ArbitrageExecutionStatus.COMPLETED),
        resolution_method=method,
        leg1_filled_quantity=Quantity(Decimal("1")),
    ))
    replayed = decode_event(encode_event(event))
    assert replayed == event
    notification = directives_for(replayed)[1]
    assert notification.title == "Trade manually resolved"
    assert notification.fingerprint == Fingerprint("trade-manually-resolved:execution-1")
    assert method in notification.description

    legacy = json.loads(encode_event(event))
    del legacy["event"]["fields"]["execution"]["fields"]["resolution_method"]
    old_event = decode_event(json.dumps(legacy).encode())
    assert old_event.execution.resolution_method is None
    assert directives_for(old_event)[1].title == "Trade closed"


def test_directives_for_rejected_execution_only_resolves_risk() -> None:
    """Close execution-risk without notifying a rejected trade."""
    last_error = (
        "primary submission rejected: pre-submission guard found "
        "polymarket:contract-1 book age 205.317 ms above 200.000 ms"
    )
    event = ExecutionUpdated(
        _execution(ArbitrageExecutionStatus.REJECTED, last_error=last_error),
    )

    assert directives_for(event) == (
        ResolveIncident(
            incident_id=IncidentID(INCIDENT_KEY),
            fingerprint=Fingerprint(INCIDENT_KEY),
            resolved_at=NOW,
        ),
    )


def test_directives_for_ignores_non_alerting_execution_state() -> None:
    """Return no directives while an execution is progressing normally."""
    event = ExecutionUpdated(_execution(ArbitrageExecutionStatus.HEDGE_PENDING))

    assert directives_for(event) == ()


def test_directives_for_opens_critical_trading_safety_incident() -> None:
    """Alert operators when a venue safety circuit halts live trading."""
    event = TradingSafetyStop(
        venue_id=VenueID("POLYMARKET"),
        reason="Trading halted by venue safety circuit: HTTP 401 Unauthorized",
        detected_at=NOW,
    )

    assert directives_for(event) == (
        OpenIncident(
            incident_id=IncidentID("trading-safety:POLYMARKET"),
            fingerprint=Fingerprint("trading-safety:POLYMARKET"),
            severity=Severity.CRITICAL,
            source=AlertSource(
                component="trading-safety-circuit",
                service="POLYMARKET",
            ),
            description=event.reason,
            opened_at=NOW,
            title="Live trading stopped by venue safety circuit",
        ),
    )
    assert decode_event(encode_event(event)) == event


def test_directives_for_marks_uncertain_submission_unknown() -> None:
    """Use UNKNOWN until reconciliation establishes the submission impact."""
    reference = OrderReference(
        venue_id=VenueID("venue-1"),
        client_order_id=ClientOrderID("client-1"),
        recovery_data=b"opaque",
    )
    command = SubmitOrder(
        execution_id="execution-1",
        role="hedge",
        venue_id=VenueID("venue-1"),
        intent=OrderIntent(
            contract_id=ContractID("contract-1"),
            side=OrderSide.BUY,
            quantity=Quantity(Decimal("1")),
            order_type=OrderType.MARKET,
            client_order_id=ClientOrderID("client-1"),
            created_at=NOW,
        ),
    )
    event = SubmissionReceived(
        command,
        SubmissionResult(
            status=SubmissionStatus.UNKNOWN,
            reference=reference,
            reason="request timed out",
        ),
    )

    directive = directives_for(event)[0]

    assert isinstance(directive, OpenIncident)
    assert directive.severity is Severity.UNKNOWN
    assert directive.fingerprint == Fingerprint(INCIDENT_KEY)


def test_directives_for_marks_rejected_inventory_operation_minor() -> None:
    """Classify a bounded collateral operation rejection as MINOR."""
    reference = InventoryOperationReference(
        venue_id=VenueID("venue-1"),
        operation_id=InventoryOperationID("inventory-1"),
        recovery_data=b"opaque",
    )
    event = InventoryOperationRecorded(
        InventorySubmissionResult(
            status=InventorySubmissionStatus.REJECTED,
            reference=reference,
            reason="insufficient collateral",
        ),
    )

    directive = directives_for(event, recorded_at=NOW)[0]

    assert isinstance(directive, OpenIncident)
    assert directive.severity is Severity.MINOR
    assert directive.fingerprint == Fingerprint(
        "inventory-operation:inventory-1",
    )
