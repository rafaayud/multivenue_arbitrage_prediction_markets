"""Verify alert delivery worker lifecycle persistence."""

import asyncio
from contextlib import nullcontext
from datetime import datetime, timedelta, timezone

from prediction_markets.domain.alerting.entities import (
    Incident,
    NotificationDelivery,
    NotificationSnapshot,
)
from prediction_markets.domain.alerting.enums import (
    Channel,
    DeliveryStatus,
    IncidentStatus,
    NotificationState,
    Severity,
)
from prediction_markets.domain.alerting.value_objects import (
    AlertSource,
    DeliveryID,
    Fingerprint,
    IncidentID,
    Recipient,
)
from prediction_markets.domain.shared.value_objects import Timestamp
from prediction_markets.infrastructure.alerting.worker import (
    AlertDeliveryWorker,
    AlertEscalationWorker,
)


def _claimed_delivery() -> NotificationDelivery:
    """Build one delivery already leased by the worker."""
    now = Timestamp(datetime(2026, 8, 19, tzinfo=timezone.utc))
    return NotificationDelivery(
        id=DeliveryID("delivery-1"),
        incident_id=IncidentID("incident-1"),
        recipient=Recipient("on-call", Channel.SMS, "alertmanager:on-call"),
        snapshot=NotificationSnapshot(
            fingerprint=Fingerprint("execution-risk:1"),
            state=NotificationState.FIRING,
            severity=Severity.CRITICAL,
            source=AlertSource(component="trading-execution"),
            description="Residual exposure",
            starts_at=now,
        ),
        status=DeliveryStatus.SENDING,
        requested_at=now,
        attempt_count=1,
        started_at=now,
    )


class _Repository:
    """Return one claimed delivery and capture its terminal update."""

    def __init__(self, delivery: NotificationDelivery) -> None:
        self.delivery = delivery
        self.updated: NotificationDelivery | None = None

    def claim_pending(self, limit: int, at: Timestamp):
        return (self.delivery,)

    def update_notification_delivery(self, delivery: NotificationDelivery) -> None:
        self.updated = delivery


class _Sender:
    """Accept one immutable delivery and return an adapter reference."""

    def __init__(self, error: Exception | None = None) -> None:
        self.error = error
        self.received: NotificationDelivery | None = None

    async def send_notification(self, delivery: NotificationDelivery) -> str | None:
        self.received = delivery
        if self.error is not None:
            raise self.error
        return "accepted"


def test_worker_persists_success_and_failure_outcomes() -> None:
    """Finish every claimed generation even when Alertmanager rejects one."""

    async def run_test() -> None:
        successful = _claimed_delivery()
        success_repository = _Repository(successful)
        success_sender = _Sender()
        success_worker = AlertDeliveryWorker(success_repository, success_sender)

        assert await success_worker.run_once() == 1
        assert success_repository.updated is successful
        assert successful.status is DeliveryStatus.DELIVERED
        assert successful.provider_reference == "accepted"
        assert success_sender.received.snapshot.severity is Severity.CRITICAL

        failed = _claimed_delivery()
        failure_repository = _Repository(failed)
        failure_worker = AlertDeliveryWorker(
            failure_repository,
            _Sender(RuntimeError("alertmanager unavailable")),
        )

        assert await failure_worker.run_once() == 1
        assert failure_repository.updated is failed
        assert failed.status is DeliveryStatus.FAILED
        assert failed.last_error == "alertmanager unavailable"

    asyncio.run(run_test())


def test_escalation_worker_applies_due_policy_change() -> None:
    """Route due severity changes through the inbound alerting port."""
    opened_at = Timestamp(datetime(2026, 8, 19, tzinfo=timezone.utc))
    incident = Incident(
        id=IncidentID("incident-1"),
        fingerprint=Fingerprint("execution-risk:1"),
        status=IncidentStatus.OPEN,
        severity=Severity.MAJOR,
        source=AlertSource(component="trading-execution"),
        description="Recovery pending",
        opened_at=opened_at,
    )

    class _Incidents:
        def list_active(self):
            return (incident,)

    class _Alerting:
        def __init__(self) -> None:
            self.change = None

        def change_incident_severity(self, incident_id, severity, at) -> None:
            self.change = (incident_id, severity, at)

    alerting = _Alerting()
    worker = AlertEscalationWorker(
        _Incidents(),
        alerting,
        nullcontext,
    )
    at = Timestamp(opened_at.value + timedelta(hours=5))

    assert asyncio.run(worker.run_once(at)) == 1
    assert alerting.change == (incident.id, Severity.CRITICAL, at)
