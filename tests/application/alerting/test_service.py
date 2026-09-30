"""Verify alerting use cases coordinate domain state through repository ports."""

from datetime import datetime, timedelta, timezone
from unittest.mock import Mock

from prediction_markets.application.alerting.service import (
    AlertingApplicationService,
)
from prediction_markets.domain.alerting.entities import (
    NotificationDelivery,
    NotificationSnapshot,
)
from prediction_markets.domain.alerting.enums import (
    Channel,
    DeliveryStatus,
    NotificationState,
    Severity,
)
from prediction_markets.domain.alerting.ports import (
    IncidentRepositoryPort,
    NotificationDeliveryRepositoryPort,
)
from prediction_markets.domain.alerting.service import NotificationPolicy
from prediction_markets.domain.alerting.value_objects import (
    AlertSource,
    DeliveryID,
    Fingerprint,
    IncidentID,
    Recipient,
)
from prediction_markets.domain.shared.value_objects import Timestamp


def test_report_escalate_and_resolve_capture_each_outbound_state() -> None:
    """Keep lifecycle mutation and immutable outbox snapshots coordinated."""
    incidents = Mock(spec=IncidentRepositoryPort)
    deliveries = Mock(spec=NotificationDeliveryRepositoryPort)
    stored_incidents = {}
    stored_deliveries = []
    incidents.get_active_by_fingerprint.side_effect = (
        lambda fingerprint: next(
            (
                incident
                for incident in stored_incidents.values()
                if incident.fingerprint == fingerprint
                and incident.resolved_at is None
            ),
            None,
        )
    )
    incidents.get_incident.side_effect = stored_incidents.get
    incidents.add_incident.side_effect = (
        lambda incident: stored_incidents.__setitem__(incident.id, incident)
    )
    deliveries.add_notification_delivery.side_effect = stored_deliveries.append
    service = AlertingApplicationService(
        incidents,
        deliveries,
        NotificationPolicy(
            [Recipient("operator", Channel.EMAIL, "alertmanager:operator")],
        ),
    )
    opened_at = Timestamp(datetime(2026, 8, 19, tzinfo=timezone.utc))
    escalated_at = Timestamp(opened_at.value + timedelta(minutes=5))
    resolved_at = Timestamp(opened_at.value + timedelta(minutes=10))
    incident_id = IncidentID("incident-1")
    fingerprint = Fingerprint("execution-risk:execution-1")
    source = AlertSource(component="trading-execution")

    service.report_incident(
        incident_id,
        fingerprint,
        Severity.MAJOR,
        source,
        "Recovery pending",
        opened_at,
    )
    service.report_incident(
        incident_id,
        fingerprint,
        Severity.MAJOR,
        source,
        "Recovery still pending",
        escalated_at,
    )
    service.report_incident(
        incident_id,
        fingerprint,
        Severity.CRITICAL,
        source,
        "Manual review required",
        escalated_at,
    )
    service.report_incident(
        incident_id,
        fingerprint,
        Severity.MAJOR,
        source,
        "Stale recovery update",
        escalated_at,
    )
    service.resolve_incident(incident_id, resolved_at)

    assert [delivery.snapshot.state for delivery in stored_deliveries] == [
        NotificationState.FIRING,
        NotificationState.RESOLVED,
        NotificationState.FIRING,
        NotificationState.RESOLVED,
    ]
    assert [delivery.snapshot.severity for delivery in stored_deliveries] == [
        Severity.MAJOR,
        Severity.MAJOR,
        Severity.CRITICAL,
        Severity.CRITICAL,
    ]


def test_retry_requeues_failed_delivery_at_command_time() -> None:
    """Use the manual retry timestamp as the new outbox ordering time."""
    incidents = Mock(spec=IncidentRepositoryPort)
    deliveries = Mock(spec=NotificationDeliveryRepositoryPort)
    requested_at = Timestamp(datetime(2026, 8, 19, tzinfo=timezone.utc))
    started_at = Timestamp(requested_at.value + timedelta(minutes=1))
    failed_at = Timestamp(requested_at.value + timedelta(minutes=2))
    retried_at = Timestamp(requested_at.value + timedelta(minutes=3))
    delivery = NotificationDelivery(
        id=DeliveryID("delivery-1"),
        incident_id=IncidentID("incident-1"),
        recipient=Recipient("operator", Channel.EMAIL, "alertmanager:operator"),
        snapshot=NotificationSnapshot(
            fingerprint=Fingerprint("execution-risk:execution-1"),
            state=NotificationState.FIRING,
            severity=Severity.CRITICAL,
            source=AlertSource(component="trading-execution"),
            description="Manual review required",
            starts_at=requested_at,
        ),
        status=DeliveryStatus.FAILED,
        requested_at=requested_at,
        attempt_count=1,
        started_at=started_at,
        failed_at=failed_at,
        last_error="Alertmanager unavailable",
    )
    deliveries.get_notification_delivery.return_value = delivery
    service = AlertingApplicationService(
        incidents,
        deliveries,
        NotificationPolicy(()),
    )

    service.retry_notification(delivery.id, retried_at)

    assert delivery.status is DeliveryStatus.PENDING
    assert delivery.requested_at == retried_at
    deliveries.update_notification_delivery.assert_called_once_with(delivery)


def test_request_notifications_without_incident_enqueues_ephemeral_delivery() -> None:
    """Route informational snapshots without creating an incident row."""
    incidents = Mock(spec=IncidentRepositoryPort)
    deliveries = Mock(spec=NotificationDeliveryRepositoryPort)
    stored_deliveries: list[NotificationDelivery] = []
    deliveries.add_notification_delivery.side_effect = stored_deliveries.append
    service = AlertingApplicationService(
        incidents,
        deliveries,
        NotificationPolicy(
            [Recipient("operator", Channel.EMAIL, "alertmanager:operator")],
        ),
    )
    notified_at = Timestamp(datetime(2026, 8, 19, tzinfo=timezone.utc))

    delivery_ids = service.request_notifications(
        None,
        NotificationSnapshot(
            fingerprint=Fingerprint("trade-completed:execution-1"),
            state=NotificationState.FIRING,
            severity=Severity.INFORMATIONAL,
            source=AlertSource(component="trading-execution"),
            description="Execution execution-1 completed",
            starts_at=notified_at,
            ends_at=notified_at,
            title="Trade completed",
        ),
        None,
        notified_at,
    )

    assert len(delivery_ids) == 1
    assert stored_deliveries[0].incident_id is None
    assert stored_deliveries[0].snapshot.ends_at == notified_at
    incidents.get_incident.assert_not_called()
