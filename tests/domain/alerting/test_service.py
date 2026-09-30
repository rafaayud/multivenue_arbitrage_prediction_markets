"""Verify alerting notification and escalation policies."""

from datetime import datetime, timedelta, timezone

import pytest

from prediction_markets.domain.alerting.entities import Incident
from prediction_markets.domain.alerting.enums import Channel, IncidentStatus, Severity
from prediction_markets.domain.alerting.service import EscalationPolicy, NotificationPolicy
from prediction_markets.domain.alerting.value_objects import (
    AlertSource,
    Fingerprint,
    IncidentID,
    Recipient,
)
from prediction_markets.domain.shared.value_objects import Timestamp

OPENED_AT = Timestamp(datetime(2026, 8, 17, tzinfo=timezone.utc))


def _incident(
    *,
    status: IncidentStatus = IncidentStatus.OPEN,
    severity: Severity = Severity.MAJOR,
    opened_at: Timestamp = OPENED_AT,
) -> Incident:
    """Build an incident whose timestamps satisfy the given status."""
    acknowledged_at = None
    in_progress_at = None
    resolved_at = None
    if status in {IncidentStatus.ACKNOWLEDGED, IncidentStatus.IN_PROGRESS}:
        acknowledged_at = Timestamp(opened_at.value + timedelta(minutes=1))
    if status is IncidentStatus.IN_PROGRESS:
        in_progress_at = Timestamp(opened_at.value + timedelta(minutes=2))
    if status in {IncidentStatus.RESOLVED, IncidentStatus.CLOSED}:
        resolved_at = Timestamp(opened_at.value + timedelta(hours=1))
    return Incident(
        id=IncidentID("incident-1"),
        fingerprint=Fingerprint("fingerprint-1"),
        status=status,
        severity=severity,
        source=AlertSource(component="execution"),
        description="Execution requires review",
        opened_at=opened_at,
        acknowledged_at=acknowledged_at,
        in_progress_at=in_progress_at,
        resolved_at=resolved_at,
    )


def _recipient(channel: Channel) -> Recipient:
    """Build one recipient per channel, with a stable identity."""
    return Recipient(
        id=channel.value,
        channel=channel,
        address=f"{channel.value}@alerts",
    )


def test_notification_policy_stores_recipients_as_frozenset() -> None:
    """Accept any iterable and collapse duplicates into an immutable set."""
    push = _recipient(Channel.PUSH)
    policy = NotificationPolicy(recipient for recipient in (push, push))

    assert policy.recipients == frozenset({push})


@pytest.mark.parametrize(
    ("severity", "expected_channels"),
    [
        (Severity.CRITICAL, {Channel.SMS, Channel.EMAIL, Channel.PUSH}),
        (Severity.MAJOR, {Channel.EMAIL, Channel.PUSH}),
        (Severity.MINOR, {Channel.EMAIL, Channel.PUSH}),
        (Severity.WARNING, {Channel.EMAIL, Channel.PUSH}),
        (Severity.INFORMATIONAL, {Channel.EMAIL}),
        (Severity.UNKNOWN, {Channel.EMAIL, Channel.PUSH}),
    ],
)
def test_targets_for_selects_recipients_on_enabled_channels(
    severity: Severity,
    expected_channels: set[Channel],
) -> None:
    """Notify only recipients whose channel is enabled for the severity."""
    recipients = [_recipient(channel) for channel in Channel]
    policy = NotificationPolicy(recipients)

    targets = policy.targets_for(_incident(severity=severity))

    assert {target.channel for target in targets} == expected_channels
    assert Channel.OTHER not in {target.channel for target in targets}


@pytest.mark.parametrize(
    ("severity", "expected_channels"),
    [
        (Severity.CRITICAL, {Channel.SMS, Channel.EMAIL, Channel.PUSH}),
        (Severity.MAJOR, {Channel.EMAIL, Channel.PUSH}),
        (Severity.INFORMATIONAL, {Channel.EMAIL}),
    ],
)
def test_targets_for_notification_matches_incident_routing(
    severity: Severity,
    expected_channels: set[Channel],
) -> None:
    """Route ephemeral notifications through the same severity channel map."""
    recipients = [_recipient(channel) for channel in Channel]
    policy = NotificationPolicy(recipients)

    targets = policy.targets_for_notification(severity)

    assert {target.channel for target in targets} == expected_channels


def test_targets_for_returns_empty_when_no_recipient_matches() -> None:
    """Return no targets when registered destinations use disabled channels."""
    policy = NotificationPolicy([_recipient(Channel.OTHER)])

    assert policy.targets_for(_incident(severity=Severity.CRITICAL)) == set()


def test_targets_for_does_not_mutate_the_incident() -> None:
    """Leave incident state unchanged while selecting recipients."""
    incident = _incident(severity=Severity.MAJOR)
    policy = NotificationPolicy([_recipient(Channel.PUSH)])

    policy.targets_for(incident)

    assert incident.severity is Severity.MAJOR
    assert incident.status is IncidentStatus.OPEN


def test_severity_after_timeout_returns_next_severity_without_mutating() -> None:
    """Escalate by return value; leave the incident unchanged until applied."""
    incident = _incident(severity=Severity.MAJOR)
    at = Timestamp(OPENED_AT.value + timedelta(hours=5))

    result = EscalationPolicy().severity_after_timeout(incident, at)

    assert result is Severity.CRITICAL
    assert incident.severity is Severity.MAJOR

    incident.change_severity(result)

    assert incident.severity is Severity.CRITICAL


@pytest.mark.parametrize(
    ("status", "severity", "elapsed", "expected"),
    [
        (
            IncidentStatus.OPEN,
            Severity.MAJOR,
            timedelta(hours=4, seconds=1),
            Severity.CRITICAL,
        ),
        (
            IncidentStatus.OPEN,
            Severity.MAJOR,
            timedelta(hours=4),
            Severity.MAJOR,
        ),
        (
            IncidentStatus.OPEN,
            Severity.MINOR,
            timedelta(days=1, seconds=1),
            Severity.MAJOR,
        ),
        (
            IncidentStatus.ACKNOWLEDGED,
            Severity.MAJOR,
            timedelta(hours=4, seconds=1),
            Severity.CRITICAL,
        ),
        (
            IncidentStatus.ACKNOWLEDGED,
            Severity.MINOR,
            timedelta(days=1, seconds=1),
            Severity.MAJOR,
        ),
        (
            IncidentStatus.IN_PROGRESS,
            Severity.MAJOR,
            timedelta(hours=2, seconds=1),
            Severity.CRITICAL,
        ),
        (
            IncidentStatus.IN_PROGRESS,
            Severity.MINOR,
            timedelta(hours=12, seconds=1),
            Severity.MAJOR,
        ),
        (
            IncidentStatus.IN_PROGRESS,
            Severity.MAJOR,
            timedelta(hours=2),
            Severity.MAJOR,
        ),
        (
            IncidentStatus.RESOLVED,
            Severity.MAJOR,
            timedelta(hours=10),
            Severity.MAJOR,
        ),
        (
            IncidentStatus.CLOSED,
            Severity.MINOR,
            timedelta(days=10),
            Severity.MINOR,
        ),
        (
            IncidentStatus.OPEN,
            Severity.CRITICAL,
            timedelta(hours=10),
            Severity.CRITICAL,
        ),
        (
            IncidentStatus.OPEN,
            Severity.WARNING,
            timedelta(days=10),
            Severity.WARNING,
        ),
        (
            IncidentStatus.OPEN,
            Severity.INFORMATIONAL,
            timedelta(days=10),
            Severity.INFORMATIONAL,
        ),
        (
            IncidentStatus.OPEN,
            Severity.UNKNOWN,
            timedelta(days=10),
            Severity.UNKNOWN,
        ),
    ],
)
def test_severity_after_timeout_follows_status_and_severity_table(
    status: IncidentStatus,
    severity: Severity,
    elapsed: timedelta,
    expected: Severity,
) -> None:
    """Escalate only when a table rule exists and its timeout has elapsed."""
    incident = _incident(status=status, severity=severity)
    at = Timestamp(OPENED_AT.value + elapsed)

    result = EscalationPolicy().severity_after_timeout(incident, at)

    assert result is expected
    assert incident.severity is severity


def test_in_progress_major_escalates_before_open_major() -> None:
    """Use the shorter IN_PROGRESS timeout while OPEN still holds MAJOR."""
    at = Timestamp(OPENED_AT.value + timedelta(hours=3))
    policy = EscalationPolicy()

    open_result = policy.severity_after_timeout(
        _incident(status=IncidentStatus.OPEN, severity=Severity.MAJOR),
        at,
    )
    in_progress_result = policy.severity_after_timeout(
        _incident(status=IncidentStatus.IN_PROGRESS, severity=Severity.MAJOR),
        at,
    )

    assert open_result is Severity.MAJOR
    assert in_progress_result is Severity.CRITICAL
