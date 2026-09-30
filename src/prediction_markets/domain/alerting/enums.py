"""Define the finite states used by the alerting domain.

Responsibilities
----------------
- Provide stable symbolic values for domain decisions.
"""

from enum import StrEnum


class IncidentStatus(StrEnum):
    """Enumerate incident lifecycle states.

    Notes
    -----
    - `OPEN` is the initial state.
    - `ACKNOWLEDGED` is the state after the incident has been acknowledged.
    - `IN_PROGRESS` is the state after the incident has been acknowledged and is being investigated.
    - `RESOLVED` is the state after the incident has been resolved.
    - `CLOSED` is the state after the incident has been closed.
    """

    OPEN = "open"
    ACKNOWLEDGED = "acknowledged"
    IN_PROGRESS = "in_progress"
    RESOLVED = "resolved"
    CLOSED = "closed"


class DeliveryStatus(StrEnum):
    """Enumerate notification delivery lifecycle states.

    Notes
    -----
    - `PENDING` is the initial state.
    - `SENDING` is the state after the notification has been sent.
    - `DELIVERED` is the state after the notification has been delivered.
    - `FAILED` is the state after the notification has failed to be delivered.
    """

    PENDING = "pending"
    SENDING = "sending"
    DELIVERED = "delivered"
    FAILED = "failed"


class NotificationState(StrEnum):
    """Describe whether an outbound alert opens or resolves a condition."""

    FIRING = "firing"
    RESOLVED = "resolved"


class Severity(StrEnum):
    """Represent the operational impact of an incident.

    Notes
    -----
    - CRITICAL: immediate action required; severe operational or financial risk.
    - MAJOR: significant degradation or risk requiring prompt attention.
    - MINOR: limited impact that does not threaten core operation.
    - WARNING: abnormal condition that may develop into an incident.
    - INFORMATIONAL: noteworthy condition requiring no immediate action.
    - UNKNOWN: severity could not be determined.
    """

    CRITICAL = "critical"
    MAJOR = "major"
    MINOR = "minor"
    WARNING = "warning"
    INFORMATIONAL = "informational"
    UNKNOWN = "unknown"


class Channel(StrEnum):
    """Enumerate supported notification channels.
    
    Notes
    -----
    - `EMAIL` is the channel for email notifications.
    - `VOICE` is the channel for voice notifications.
    - `PUSH` is the channel for push notifications.
    - `SMS` is the channel for SMS notifications.
    - `OTHER` is the channel for other notifications.
    """

    EMAIL = "email"
    VOICE = "voice"
    PUSH = "push"
    SMS = "sms"
    OTHER = "other"
