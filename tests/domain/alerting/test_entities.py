"""Verify alerting incident lifecycle invariants."""

from datetime import datetime, timezone

import pytest

from prediction_markets.domain.alerting.entities import (
    Incident,
    NotificationSnapshot,
)
from prediction_markets.domain.alerting.enums import (
    IncidentStatus,
    NotificationState,
    Severity,
)
from prediction_markets.domain.alerting.value_objects import (
    AlertSource,
    Fingerprint,
    IncidentID,
)
from prediction_markets.domain.shared.value_objects import Timestamp


def _closed_incident(*, resolved_at: Timestamp | None = None) -> Incident:
    """Build a closed incident for lifecycle validation tests."""
    return Incident(
        id=IncidentID("incident-1"),
        fingerprint=Fingerprint("fingerprint-1"),
        status=IncidentStatus.CLOSED,
        severity=Severity.CRITICAL,
        source=AlertSource(component="execution"),
        description="Execution requires review",
        opened_at=Timestamp(datetime(2026, 8, 17, tzinfo=timezone.utc)),
        resolved_at=resolved_at,
    )


def test_closed_incident_requires_resolved_at() -> None:
    """Reject a closed incident that has not reached the resolved state."""
    with pytest.raises(ValueError, match="Closed incident requires resolved_at"):
        _closed_incident()


def test_closed_incident_accepts_resolved_at() -> None:
    """Allow a closed incident once its resolution time is recorded."""
    incident = _closed_incident(
        resolved_at=Timestamp(datetime(2026, 8, 17, 1, tzinfo=timezone.utc)),
    )

    assert incident.status is IncidentStatus.CLOSED


def test_notification_snapshot_pins_severity_and_resolution_state() -> None:
    """Keep queued delivery content independent from later incident changes."""
    starts_at = Timestamp(datetime(2026, 8, 17, tzinfo=timezone.utc))
    ends_at = Timestamp(datetime(2026, 8, 17, 1, tzinfo=timezone.utc))

    snapshot = NotificationSnapshot(
        fingerprint=Fingerprint("fingerprint-1"),
        state=NotificationState.RESOLVED,
        severity=Severity.MAJOR,
        source=AlertSource(component="execution"),
        description="Automatic recovery completed",
        starts_at=starts_at,
        ends_at=ends_at,
    )

    assert snapshot.severity is Severity.MAJOR
    assert snapshot.ends_at == ends_at


def test_resolved_notification_snapshot_requires_end_time() -> None:
    """Reject a resolved Alertmanager payload without its resolution instant."""
    with pytest.raises(ValueError, match="requires ends_at"):
        NotificationSnapshot(
            fingerprint=Fingerprint("fingerprint-1"),
            state=NotificationState.RESOLVED,
            severity=Severity.MAJOR,
            source=AlertSource(component="execution"),
            description="Automatic recovery completed",
            starts_at=Timestamp(datetime(2026, 8, 17, tzinfo=timezone.utc)),
        )


def test_firing_notification_allows_one_shot_end_time() -> None:
    """Allow a firing payload that closes immediately for ephemeral alerts."""
    at = Timestamp(datetime(2026, 8, 17, tzinfo=timezone.utc))

    snapshot = NotificationSnapshot(
        fingerprint=Fingerprint("trade-completed:execution-1"),
        state=NotificationState.FIRING,
        severity=Severity.INFORMATIONAL,
        source=AlertSource(component="trading-execution"),
        description="Execution execution-1 completed",
        starts_at=at,
        ends_at=at,
    )

    assert snapshot.ends_at == at
