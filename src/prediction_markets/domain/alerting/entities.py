"""Define the core alerting domain entities.

Responsibilities
----------------
- Model identity, state, and behavior independent of infrastructure.
"""

from dataclasses import dataclass

from prediction_markets.domain.alerting.enums import (
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


@dataclass(slots=True)
class Incident:
    """Represent a mutable incident and its lifecycle.

    Attributes
    ----------
    id
        Immutable incident identifier.
    fingerprint
        Immutable incident fingerprint.
    status
        Current lifecycle state.
    severity
        Current severity level.
    source
        Originating component of the alert.
    description
        Non-empty description of the incident.
    opened_at
        Instant when the incident was opened.
    title
        Optional title.
    summary
        Optional summary.
    acknowledged_at
        Instant when the incident was acknowledged, if any.
    in_progress_at
        Instant when investigation started, if any.
    resolved_at
        Instant when the incident was resolved, if any.

    Invariants
    ----------
    - `description` contains at least one non-whitespace character.
    - `OPEN` has no `acknowledged_at`, `in_progress_at`, or `resolved_at`.
    - `ACKNOWLEDGED` requires `acknowledged_at` and forbids `in_progress_at` and `resolved_at`.
    - `IN_PROGRESS` requires `acknowledged_at` and `in_progress_at`, and forbids `resolved_at`.
    - `RESOLVED` requires `resolved_at`.
    - `CLOSED` requires `resolved_at`.
    """

    id: IncidentID
    fingerprint: Fingerprint
    status: IncidentStatus
    severity: Severity
    source: AlertSource
    description: str
    opened_at: Timestamp
    title: str | None = None
    summary: str | None = None
    acknowledged_at: Timestamp | None = None
    in_progress_at: Timestamp | None = None
    resolved_at: Timestamp | None = None

    def __post_init__(self) -> None:
        if not self.description.strip():
            raise ValueError("Description cannot be empty")
        self._validate_state()

    def acknowledge(self, at: Timestamp) -> None:
        """Transition an open incident to acknowledged.

        Parameters
        ----------
        at
            Instant recorded as `acknowledged_at`.

        Raises
        ------
        ValueError
            If the incident is not open.
        """
        if self.status != IncidentStatus.OPEN:
            raise ValueError("Only an open incident can be acknowledged")

        self.status = IncidentStatus.ACKNOWLEDGED
        self.acknowledged_at = at

    def mark_in_progress(self, at: Timestamp) -> None:
        """Transition an acknowledged incident to in progress.

        Parameters
        ----------
        at
            Instant recorded as `in_progress_at`.

        Raises
        ------
        ValueError
            If the incident is not acknowledged.
        """
        if self.status != IncidentStatus.ACKNOWLEDGED:
            raise ValueError(
                "Only an acknowledged incident can be marked as in progress"
            )

        self.status = IncidentStatus.IN_PROGRESS
        self.in_progress_at = at

    def resolve(self, at: Timestamp) -> None:
        """Mark the incident as resolved.

        Parameters
        ----------
        at
            Instant recorded as `resolved_at`.

        Raises
        ------
        ValueError
            If the incident is already resolved.

        Notes
        -----
        - Allowed from any status other than `RESOLVED`.
        """
        if self.status == IncidentStatus.RESOLVED:
            raise ValueError("Incident is already resolved")

        self.status = IncidentStatus.RESOLVED
        self.resolved_at = at

    def change_severity(self, severity: Severity) -> None:
        """Replace the incident severity.

        Parameters
        ----------
        severity
            New severity level.

        Raises
        ------
        ValueError
            If the incident is resolved.
        """
        if self.status == IncidentStatus.RESOLVED:
            raise ValueError("Cannot change severity of a resolved incident")

        self.severity = severity

    def _validate_state(self) -> None:
        """Enforce the lifecycle timestamp invariants for the current status.

        Raises
        ------
        ValueError
            If required timestamps are missing or forbidden timestamps are present.

        Notes
        -----
        - Called at construction; transition methods do not re-run this check.
        """
        if self.status == IncidentStatus.OPEN:
            if any(
                timestamp is not None
                for timestamp in (
                    self.acknowledged_at,
                    self.in_progress_at,
                    self.resolved_at,
                )
            ):
                raise ValueError(
                    "Open incident cannot have lifecycle timestamps"
                )

        elif self.status == IncidentStatus.ACKNOWLEDGED:
            if self.acknowledged_at is None:
                raise ValueError(
                    "Acknowledged incident requires acknowledged_at"
                )

            if self.in_progress_at is not None:
                raise ValueError(
                    "Acknowledged incident cannot have in_progress_at"
                )

            if self.resolved_at is not None:
                raise ValueError(
                    "Acknowledged incident cannot have resolved_at"
                )

        elif self.status == IncidentStatus.IN_PROGRESS:
            if self.acknowledged_at is None:
                raise ValueError(
                    "In-progress incident requires acknowledged_at"
                )

            if self.in_progress_at is None:
                raise ValueError(
                    "In-progress incident requires in_progress_at"
                )

            if self.resolved_at is not None:
                raise ValueError(
                    "In-progress incident cannot have resolved_at"
                )

        elif self.status == IncidentStatus.RESOLVED:
            if self.resolved_at is None:
                raise ValueError(
                    "Resolved incident requires resolved_at"
                )

        elif self.status == IncidentStatus.CLOSED:
            if self.resolved_at is None:
                raise ValueError(
                    "Closed incident requires resolved_at"
                )


@dataclass(frozen=True, slots=True)
class NotificationSnapshot:
    """Capture the immutable Alertmanager payload for one delivery.

    Attributes
    ----------
    fingerprint
        Stable correlation label used by Alertmanager.
    state
        Whether the payload opens or resolves the alert.
    severity
        Severity at the time the delivery was requested.
    source
        Component identity at the time the delivery was requested.
    description
        Non-empty human-readable alert description.
    starts_at
        Instant when this alert condition started.
    ends_at
        Resolution instant, required only for resolved payloads.
    title
        Optional concise alert name.
    summary
        Optional human-readable summary.

    Invariants
    ----------
    - The description is non-empty.
    - Firing payloads have no end time.
    - Resolved payloads have an end time.
    """

    fingerprint: Fingerprint
    state: NotificationState
    severity: Severity
    source: AlertSource
    description: str
    starts_at: Timestamp
    ends_at: Timestamp | None = None
    title: str | None = None
    summary: str | None = None

    def __post_init__(self) -> None:
        if not self.description.strip():
            raise ValueError("Notification description cannot be empty")
        if self.state is NotificationState.FIRING and self.ends_at is not None:
            if self.ends_at != self.starts_at:
                raise ValueError("Firing notification cannot have ends_at")
        if self.state is NotificationState.RESOLVED and self.ends_at is None:
            raise ValueError("Resolved notification requires ends_at")


@dataclass(slots=True)
class NotificationDelivery:
    """Represent a mutable notification delivery and its lifecycle.

    Attributes
    ----------
    id
        Immutable delivery identifier.
    incident_id
        Identifier of the incident being notified, when one exists.
    recipient
        Destination of the notification.
    snapshot
        Immutable outbound content captured when delivery was requested.
    status
        Current delivery lifecycle state.
    requested_at
        Instant when delivery was requested.
    attempt_count
        Number of send attempts so far.
    provider_reference
        Optional provider-side reference for the delivery.
    last_error
        Last recorded failure message, if any.
    started_at
        Instant when the current send attempt started, if any.
    delivered_at
        Instant when the notification was delivered, if any.
    failed_at
        Instant when the current attempt failed, if any.

    Invariants
    ----------
    - `attempt_count` is greater than or equal to zero.
    - `PENDING` has no lifecycle timestamps, error, or provider reference.
    - `SENDING` requires `started_at` and at least one attempt, and forbids
      terminal fields, errors, and provider references.
    - `DELIVERED` requires `started_at`, `delivered_at`, and at least one attempt, and forbids `failed_at` and `last_error`.
    - `FAILED` requires `started_at`, `failed_at`, `last_error`, and at least one attempt, and forbids `delivered_at`.
    """

    id: DeliveryID
    incident_id: IncidentID | None
    recipient: Recipient
    snapshot: NotificationSnapshot
    status: DeliveryStatus
    requested_at: Timestamp
    attempt_count: int = 0
    provider_reference: str | None = None
    last_error: str | None = None
    started_at: Timestamp | None = None
    delivered_at: Timestamp | None = None
    failed_at: Timestamp | None = None

    def __post_init__(self) -> None:
        if self.attempt_count < 0:
            raise ValueError("Attempt count cannot be negative")
        self._validate_state()

    def start(self, at: Timestamp) -> None:
        """Transition a pending delivery to sending.

        Parameters
        ----------
        at
            Instant recorded as `started_at`.

        Raises
        ------
        ValueError
            If the delivery is not pending.

        Notes
        -----
        - Increments `attempt_count` by one.
        """
        if self.status != DeliveryStatus.PENDING:
            raise ValueError("Only a pending delivery can be started")

        self.status = DeliveryStatus.SENDING
        self.started_at = at
        self.attempt_count += 1

    def mark_delivered(
        self,
        at: Timestamp,
        provider_reference: str | None = None,
    ) -> None:
        """Mark a sending delivery as delivered.

        Parameters
        ----------
        at
            Instant recorded as `delivered_at`.
        provider_reference
            Optional provider-side reference to store.

        Raises
        ------
        ValueError
            If the delivery is not sending.
        """
        if self.status != DeliveryStatus.SENDING:
            raise ValueError("Only a sending delivery can be marked as delivered")

        self.status = DeliveryStatus.DELIVERED
        self.delivered_at = at
        self.provider_reference = provider_reference

    def mark_failed(self, at: Timestamp, error: str) -> None:
        """Mark a sending delivery as failed.

        Parameters
        ----------
        at
            Instant recorded as `failed_at`.
        error
            Failure message stored as `last_error`.

        Raises
        ------
        ValueError
            If the delivery is not sending, or if `error` contains only whitespace.
        """
        if self.status != DeliveryStatus.SENDING:
            raise ValueError("Only a sending delivery can be marked as failed")

        if not error.strip():
            raise ValueError("Delivery error cannot be empty")

        self.status = DeliveryStatus.FAILED
        self.failed_at = at
        self.last_error = error

    def retry(self, at: Timestamp) -> None:
        """Return a failed delivery to pending for another attempt.

        Parameters
        ----------
        at
            Instant recorded as the new outbox request time.

        Raises
        ------
        ValueError
            If the delivery is not failed.

        Notes
        -----
        - Replaces `requested_at` so the retry rejoins outbox ordering.
        - Clears `started_at`, `failed_at`, `last_error`, and `provider_reference`.
        - Leaves `attempt_count` unchanged.
        """
        if self.status != DeliveryStatus.FAILED:
            raise ValueError("Only a failed delivery can be retried")

        self.status = DeliveryStatus.PENDING
        self.requested_at = at
        self.started_at = None
        self.failed_at = None
        self.last_error = None
        self.provider_reference = None

    def _validate_state(self) -> None:
        """Enforce the lifecycle timestamp invariants for the current status.

        Raises
        ------
        ValueError
            If required timestamps are missing, forbidden timestamps are present,
            or a sending, delivered, or failed delivery has no attempts.

        Notes
        -----
        - Called at construction; transition methods do not re-run this check.
        """
        if self.status == DeliveryStatus.PENDING:
            if any(
                value is not None
                for value in (
                    self.started_at,
                    self.delivered_at,
                    self.failed_at,
                    self.last_error,
                    self.provider_reference,
                )
            ):
                raise ValueError(
                    "Pending delivery cannot have lifecycle timestamps or last_error"
                )

        elif self.status == DeliveryStatus.SENDING:
            if self.started_at is None:
                raise ValueError(
                    "Sending delivery requires started_at"
                )

            if self.attempt_count == 0:
                raise ValueError(
                    "Sending delivery requires at least one attempt"
                )

            if any(
                value is not None
                for value in (
                    self.delivered_at,
                    self.failed_at,
                    self.last_error,
                    self.provider_reference,
                )
            ):
                raise ValueError(
                    "Sending delivery cannot have delivered_at, failed_at, or last_error"
                )

        elif self.status == DeliveryStatus.DELIVERED:
            if self.started_at is None:
                raise ValueError(
                    "Delivered delivery requires started_at"
                )

            if self.delivered_at is None:
                raise ValueError(
                    "Delivered delivery requires delivered_at"
                )

            if self.attempt_count == 0:
                raise ValueError(
                    "Delivered delivery requires at least one attempt"
                )

            if any(
                value is not None
                for value in (
                    self.failed_at,
                    self.last_error,
                )
            ):
                raise ValueError(
                    "Delivered delivery cannot have failed_at or last_error"
                )

        elif self.status == DeliveryStatus.FAILED:
            if self.started_at is None:
                raise ValueError(
                    "Failed delivery requires started_at"
                )

            if self.failed_at is None:
                raise ValueError(
                    "Failed delivery requires failed_at"
                )

            if self.last_error is None:
                raise ValueError(
                    "Failed delivery requires last_error"
                )

            if self.attempt_count == 0:
                raise ValueError(
                    "Failed delivery requires at least one attempt"
                )

            if self.delivered_at is not None:
                raise ValueError(
                    "Failed delivery cannot have delivered_at"
                )
