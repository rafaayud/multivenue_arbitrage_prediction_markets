"""Define alerting boundaries required by the domain.

Responsibilities
----------------
- Expose inbound use cases for incident lifecycle and notification delivery.
- Specify infrastructure-neutral contracts for persistence and external notification.
"""

from abc import ABC, abstractmethod

from prediction_markets.domain.alerting.entities import (
    Incident,
    NotificationDelivery,
    NotificationSnapshot,
)
from prediction_markets.domain.alerting.enums import Severity
from prediction_markets.domain.alerting.value_objects import (
    AlertSource,
    DeliveryID,
    Fingerprint,
    IncidentID,
    Recipient,
)
from prediction_markets.domain.shared.value_objects import Timestamp


# ---------------------------------------------------------------------------
# Inbound port
# ---------------------------------------------------------------------------


class AlertingPort(ABC):
    """Expose alerting use cases to the rest of the application."""

    @abstractmethod
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
        """Report a new incident or correlate it with an existing active one.

        Parameters
        ----------
        incident_id
            Stable identifier assigned to a newly created incident.
        fingerprint
            Stable incident fingerprint used for correlation.
        severity
            Severity assigned to the reported incident.
        source
            Originating component of the alert.
        description
            Non-empty description of the incident.
        opened_at
            Instant recorded when the incident is opened.
        title
            Optional human-readable title.

        Returns
        -------
        IncidentID
            Identifier of the created or correlated active incident.
        """
        pass

    @abstractmethod
    def acknowledge_incident(
        self,
        incident_id: IncidentID,
        at: Timestamp,
    ) -> None:
        """Acknowledge an open incident.

        Parameters
        ----------
        incident_id
            Identifier of the incident to acknowledge.
        at
            Instant recorded as acknowledgement time.
        """
        pass

    @abstractmethod
    def mark_incident_in_progress(
        self,
        incident_id: IncidentID,
        at: Timestamp,
    ) -> None:
        """Mark an acknowledged incident as being actively handled.

        Parameters
        ----------
        incident_id
            Identifier of the incident to mark in progress.
        at
            Instant recorded as investigation start time.
        """
        pass

    @abstractmethod
    def resolve_incident(
        self,
        incident_id: IncidentID,
        at: Timestamp,
    ) -> None:
        """Resolve an active incident.

        Parameters
        ----------
        incident_id
            Identifier of the incident to resolve.
        at
            Instant recorded as resolution time.
        """
        pass

    @abstractmethod
    def change_incident_severity(
        self,
        incident_id: IncidentID,
        severity: Severity,
        at: Timestamp,
    ) -> None:
        """Change the severity of an active incident.

        Parameters
        ----------
        incident_id
            Identifier of the incident whose severity changes.
        severity
            New severity level.
        at
            Instant when the new severity becomes effective.
        """
        pass

    @abstractmethod
    def request_notifications(
        self,
        incident_id: IncidentID | None,
        snapshot: NotificationSnapshot,
        recipients: list[Recipient] | None,
        requested_at: Timestamp,
    ) -> list[DeliveryID]:
        """Create one notification delivery per recipient.

        Parameters
        ----------
        incident_id
            Identifier of the incident being notified, or ``None`` for
            ephemeral operator notifications that do not open an incident.
        snapshot
            Immutable outbound content shared by the requested deliveries.
        recipients
            Destinations that should receive a notification. When omitted,
            routing is derived from ``snapshot.severity``.
        requested_at
            Instant recorded when the deliveries are requested.

        Returns
        -------
        list[DeliveryID]
            Identifiers of the created notification deliveries, one per recipient.
        """
        pass

    @abstractmethod
    def retry_notification(
        self,
        delivery_id: DeliveryID,
        at: Timestamp,
    ) -> None:
        """Retry a failed notification delivery.

        Parameters
        ----------
        delivery_id
            Identifier of the failed delivery to retry.
        at
            Instant associated with the retry attempt.
        """
        pass


# ---------------------------------------------------------------------------
# Outbound repository ports
# ---------------------------------------------------------------------------


class IncidentRepositoryPort(ABC):
    """Persist and retrieve incidents without exposing storage details.

    Notes
    -----
    - Adapters own the persistence mechanism and mapping to storage.
    """

    @abstractmethod
    def get_incident(
        self,
        incident_id: IncidentID,
    ) -> Incident | None:
        """Load one incident by identifier.

        Parameters
        ----------
        incident_id
            Identifier of the incident to load.

        Returns
        -------
        Incident | None
            The incident when found, otherwise ``None``.
        """
        pass

    @abstractmethod
    def get_active_by_fingerprint(
        self,
        fingerprint: Fingerprint,
    ) -> Incident | None:
        """Load the active incident that matches a fingerprint.

        Parameters
        ----------
        fingerprint
            Incident fingerprint used for correlation.

        Returns
        -------
        Incident | None
            The matching active incident when found, otherwise ``None``.
        """
        pass

    @abstractmethod
    def list_active(self) -> tuple[Incident, ...]:
        """List incidents eligible for lifecycle processing.

        Returns
        -------
        tuple[Incident, ...]
            Non-resolved and non-closed incidents ordered by opening time.
        """
        pass

    @abstractmethod
    def add_incident(
        self,
        incident: Incident,
    ) -> None:
        """Persist a newly created incident.

        Parameters
        ----------
        incident
            Incident aggregate to store.
        """
        pass

    @abstractmethod
    def update_incident(
        self,
        incident: Incident,
    ) -> None:
        """Persist changes to an existing incident.

        Parameters
        ----------
        incident
            Incident aggregate with updated state.
        """
        pass


class NotificationDeliveryRepositoryPort(ABC):
    """Persist and retrieve notification deliveries without exposing storage details.

    Notes
    -----
    - Adapters own the persistence mechanism and mapping to storage.
    """

    @abstractmethod
    def get_notification_delivery(
        self,
        delivery_id: DeliveryID,
    ) -> NotificationDelivery | None:
        """Load one notification delivery by identifier.

        Parameters
        ----------
        delivery_id
            Identifier of the delivery to load.

        Returns
        -------
        NotificationDelivery | None
            The delivery when found, otherwise ``None``.
        """
        pass

    @abstractmethod
    def claim_pending(
        self,
        limit: int,
        at: Timestamp,
    ) -> tuple[NotificationDelivery, ...]:
        """Atomically claim notification deliveries that are ready to send.

        Parameters
        ----------
        limit
            Maximum number of deliveries to claim. Must be positive.
        at
            Instant assigned to ``started_at`` for the claimed attempt.

        Returns
        -------
        tuple[NotificationDelivery, ...]
            Claimed deliveries transitioned to ``SENDING``, with their attempt
            count incremented and ``started_at`` set to ``at``.

        Raises
        ------
        ValueError
            If ``limit`` is not positive.

        Notes
        -----
        - Implementations must claim rows atomically so concurrent workers do
          not receive the same attempt.
        - Pending rows and abandoned ``SENDING`` rows whose adapter-owned lease
          expired are eligible. Claims are ordered by ``requested_at`` and ID.
        - ``attempt_count`` and ``started_at`` identify the claimed generation;
          completion updates must reject stale generations after a reclaim.
        """
        pass

    @abstractmethod
    def add_notification_delivery(
        self,
        notification_delivery: NotificationDelivery,
    ) -> None:
        """Persist a newly created notification delivery.

        Parameters
        ----------
        notification_delivery
            Delivery aggregate to store.
        """
        pass

    @abstractmethod
    def update_notification_delivery(
        self,
        notification_delivery: NotificationDelivery,
    ) -> None:
        """Persist changes to an existing notification delivery.

        Parameters
        ----------
        notification_delivery
            Delivery aggregate with updated state.

        Notes
        -----
        - Updates that finish a sending attempt must compare ``attempt_count``
          and ``started_at`` atomically so a worker cannot overwrite a newer
          reclaimed attempt.
        """
        pass


# ---------------------------------------------------------------------------
# Outbound notification port
# ---------------------------------------------------------------------------


class NotificationSenderPort(ABC):
    """Send a notification through external infrastructure.

    Notes
    -----
    - Adapters own provider-specific transport, credentials, and response mapping.
    """

    @abstractmethod
    async def send_notification(
        self,
        notification_delivery: NotificationDelivery,
    ) -> str | None:
        """Send the immutable payload captured by a delivery.

        Parameters
        ----------
        notification_delivery
            Delivery containing the intended recipient and immutable alert
            content.

        Returns
        -------
        str | None
            Provider-side reference when one is available.

        Raises
        ------
        Exception
            If the external delivery operation fails.
        """
        pass
