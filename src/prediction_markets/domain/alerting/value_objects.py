"""Define validated value objects for the alerting domain.

Responsibilities
----------------
- Enforce domain invariants at construction time.
"""

from dataclasses import dataclass

from prediction_markets.domain.alerting.enums import Channel


@dataclass(frozen=True, slots=True)
class IncidentID:
    """Represent an immutable non-empty incident identifier.

    Invariants
    ----------
    - `value` is a non-empty string.

    Notes
    -----
    - Equality and hashing are value-based.
    """

    value: str

    def __post_init__(self):
        if not self.value:
            raise ValueError("Incident ID cannot be empty")


@dataclass(frozen=True, slots=True)
class DeliveryID:
    """Represent an immutable non-empty delivery identifier.

    Invariants
    ----------
    - `value` is a non-empty string.

    Notes
    -----
    - Equality and hashing are value-based.
    """

    value: str

    def __post_init__(self):
        if not self.value:
            raise ValueError("Delivery ID cannot be empty")


@dataclass(frozen=True, slots=True)
class Fingerprint:
    """Represent an immutable non-empty incident fingerprint.

    Invariants
    ----------
    - `value` is a non-empty string.

    Notes
    -----
    - Equality and hashing are value-based.
    """

    value: str

    def __post_init__(self):
        if not self.value:
            raise ValueError("Fingerprint cannot be empty")


@dataclass(frozen=True, slots=True)
class AlertSource:
    """Identify the originating component of an alert.

    Attributes
    ----------
    component
        Name of the originating component.
    service
        Optional service that emitted the alert.
    instance
        Optional instance identifier within the service.

    Notes
    -----
    - Equality and hashing are value-based.
    """

    component: str
    service: str | None = None
    instance: str | None = None


@dataclass(frozen=True, slots=True)
class Recipient:
    """Identify a notification destination.

    Attributes
    ----------
    id
        Unique identifier for the recipient.
    channel
        Delivery channel used to reach the recipient.
    address
        Channel-specific destination address.

    Invariants
    ----------
    - `channel` is a non-empty `Channel`.
    - `address` is a non-empty string.

    Notes
    -----
    - Equality and hashing are value-based.
    """

    id: str
    channel: Channel
    address: str

    def __post_init__(self):
        if not self.id.strip():
            raise ValueError("Recipient ID cannot be empty")
        if not self.channel:
            raise ValueError("Channel cannot be empty")
        if not self.address.strip():
            raise ValueError("Address cannot be empty")
