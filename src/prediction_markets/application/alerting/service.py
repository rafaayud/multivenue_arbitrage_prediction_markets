"""Implement alerting use cases over domain repository ports.

Responsibilities
----------------
- Correlate incident lifecycle changes by fingerprint.
- Capture immutable firing and resolved notification snapshots.
- Persist one delivery per recipient selected by notification policy.

Notes
-----
- Transaction ownership belongs to the caller's repository adapters.
"""

from uuid import uuid4

from prediction_markets.domain.alerting.entities import (
    Incident,
    NotificationDelivery,
    NotificationSnapshot,
)
from prediction_markets.domain.alerting.enums import (
    DeliveryStatus,
    IncidentStatus,
    NotificationState,
    Severity,
)
from prediction_markets.domain.alerting.ports import (
    AlertingPort,
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

_SEVERITY_RANK = {
    Severity.UNKNOWN: 0,
    Severity.INFORMATIONAL: 1,
    Severity.WARNING: 2,
    Severity.MINOR: 3,
    Severity.MAJOR: 4,
    Severity.CRITICAL: 5,
}


class AlertingApplicationService(AlertingPort):
    """Coordinate incident state and immutable notification deliveries.

    Parameters
    ----------
    incidents
        Incident persistence adapter participating in the caller's transaction.
    deliveries
        Delivery persistence adapter participating in the caller's transaction.
    notification_policy
        Recipient routing policy applied to lifecycle notifications.

    Notes
    -----
    - Opening, escalating, and resolving an incident enqueue notifications in
      the same transaction as the incident mutation.
    """

    def __init__(
        self,
        incidents: IncidentRepositoryPort,
        deliveries: NotificationDeliveryRepositoryPort,
        notification_policy: NotificationPolicy,
    ) -> None:
        self._incidents = incidents
        self._deliveries = deliveries
        self._notification_policy = notification_policy

    def report_incident(
        self,
        incident_id: IncidentID,
        fingerprint: Fingerprint,
        severity: Severity,
        source: AlertSource,
        description: str,
        opened_at: Timestamp,
        title: str | None = None,
    ) -> IncidentID:
        """Open, correlate, or escalate an incident and enqueue notifications.

        Notes
        -----
        - Repeated automatic reports can raise but not lower the severity of an
          active incident. Explicit severity changes may move in either direction.
        """
        incident = self._incidents.get_active_by_fingerprint(fingerprint)
        if incident is None:
            incident = Incident(
                id=incident_id,
                fingerprint=fingerprint,
                status=IncidentStatus.OPEN,
                severity=severity,
                source=source,
                description=description,
                opened_at=opened_at,
                title=title,
            )
            self._incidents.add_incident(incident)
            self._notify(
                incident,
                self._snapshot(
                    incident,
                    NotificationState.FIRING,
                    starts_at=opened_at,
                ),
                opened_at,
            )
            return incident.id

        severity_changed = (
            _SEVERITY_RANK[severity] > _SEVERITY_RANK[incident.severity]
        )
        if severity_changed:
            self._notify(
                incident,
                self._snapshot(
                    incident,
                    NotificationState.RESOLVED,
                    ends_at=opened_at,
                ),
                opened_at,
            )
            incident.change_severity(severity)
        incident.source = source
        incident.description = description
        incident.title = title
        self._incidents.update_incident(incident)
        if severity_changed:
            self._notify(
                incident,
                self._snapshot(
                    incident,
                    NotificationState.FIRING,
                    starts_at=opened_at,
                ),
                opened_at,
            )
        return incident.id

    def acknowledge_incident(self, incident_id: IncidentID, at: Timestamp) -> None:
        """Acknowledge an existing open incident."""
        incident = self._require_incident(incident_id)
        if incident.status is IncidentStatus.ACKNOWLEDGED:
            return
        incident.acknowledge(at)
        self._incidents.update_incident(incident)

    def mark_incident_in_progress(
        self,
        incident_id: IncidentID,
        at: Timestamp,
    ) -> None:
        """Mark an acknowledged incident as actively investigated."""
        incident = self._require_incident(incident_id)
        if incident.status is IncidentStatus.IN_PROGRESS:
            return
        incident.mark_in_progress(at)
        self._incidents.update_incident(incident)

    def resolve_incident(self, incident_id: IncidentID, at: Timestamp) -> None:
        """Resolve an active incident and enqueue matching resolved snapshots."""
        incident = self._incidents.get_incident(incident_id)
        if incident is None or incident.status in {
            IncidentStatus.RESOLVED,
            IncidentStatus.CLOSED,
        }:
            return
        incident.resolve(at)
        self._incidents.update_incident(incident)
        self._notify(
            incident,
            self._snapshot(
                incident,
                NotificationState.RESOLVED,
                ends_at=at,
            ),
            at,
        )

    def change_incident_severity(
        self,
        incident_id: IncidentID,
        severity: Severity,
        at: Timestamp,
    ) -> None:
        """Replace severity while resolving the previous Alertmanager labels."""
        incident = self._require_incident(incident_id)
        if incident.severity is severity:
            return
        self._notify(
            incident,
            self._snapshot(
                incident,
                NotificationState.RESOLVED,
                ends_at=at,
            ),
            at,
        )
        incident.change_severity(severity)
        self._incidents.update_incident(incident)
        self._notify(
            incident,
            self._snapshot(
                incident,
                NotificationState.FIRING,
                starts_at=at,
            ),
            at,
        )

    def request_notifications(
        self,
        incident_id: IncidentID | None,
        snapshot: NotificationSnapshot,
        recipients: list[Recipient] | None,
        requested_at: Timestamp,
    ) -> list[DeliveryID]:
        """Persist one delivery per distinct recipient for a captured snapshot."""
        if incident_id is not None:
            incident = self._require_incident(incident_id)
            if snapshot.fingerprint != incident.fingerprint:
                raise ValueError(
                    "Notification fingerprint must match its incident",
                )
            if snapshot.severity is not incident.severity:
                raise ValueError(
                    "Notification severity must match its incident",
                )
            targets = (
                recipients
                if recipients is not None
                else list(self._notification_policy.targets_for(incident))
            )
            delivery_incident_id = incident.id
        else:
            targets = (
                recipients
                if recipients is not None
                else list(
                    self._notification_policy.targets_for_notification(
                        snapshot.severity,
                    ),
                )
            )
            delivery_incident_id = None

        delivery_ids: list[DeliveryID] = []
        for recipient in sorted(
            set(targets),
            key=lambda value: (value.channel.value, value.id, value.address),
        ):
            delivery_id = DeliveryID(uuid4().hex)
            self._deliveries.add_notification_delivery(
                NotificationDelivery(
                    id=delivery_id,
                    incident_id=delivery_incident_id,
                    recipient=recipient,
                    snapshot=snapshot,
                    status=DeliveryStatus.PENDING,
                    requested_at=requested_at,
                ),
            )
            delivery_ids.append(delivery_id)
        return delivery_ids

    def retry_notification(self, delivery_id: DeliveryID, at: Timestamp) -> None:
        """Return a failed delivery to the pending outbox state."""
        delivery = self._deliveries.get_notification_delivery(delivery_id)
        if delivery is None:
            raise KeyError(f"Unknown notification delivery: {delivery_id.value}")
        delivery.retry(at)
        self._deliveries.update_notification_delivery(delivery)

    def _notify(
        self,
        incident: Incident,
        snapshot: NotificationSnapshot,
        requested_at: Timestamp,
    ) -> None:
        """Request deliveries for recipients selected by current severity."""
        self.request_notifications(
            incident.id,
            snapshot,
            None,
            requested_at,
        )

    @staticmethod
    def _snapshot(
        incident: Incident,
        state: NotificationState,
        *,
        starts_at: Timestamp | None = None,
        ends_at: Timestamp | None = None,
    ) -> NotificationSnapshot:
        """Capture the current incident content for one Alertmanager state."""
        return NotificationSnapshot(
            fingerprint=incident.fingerprint,
            state=state,
            severity=incident.severity,
            source=incident.source,
            description=incident.description,
            starts_at=starts_at or incident.opened_at,
            ends_at=ends_at,
            title=incident.title,
            summary=incident.summary,
        )

    def _require_incident(self, incident_id: IncidentID) -> Incident:
        """Load an incident or fail the requested use case."""
        incident = self._incidents.get_incident(incident_id)
        if incident is None:
            raise KeyError(f"Unknown incident: {incident_id.value}")
        return incident
