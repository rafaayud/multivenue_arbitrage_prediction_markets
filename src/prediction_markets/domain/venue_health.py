"""Define normalized venue-health observations.

Responsibilities
----------------
- Represent one venue's current trading availability independently of transport.
"""

from dataclasses import dataclass
from enum import StrEnum

from prediction_markets.domain.shared.value_objects import Timestamp, VenueID


class VenueHealthStatus(StrEnum):
    """Classify whether a venue is usable by the trading application."""

    OPERATIONAL = "operational"
    DEGRADED = "degraded"
    UNAVAILABLE = "unavailable"


@dataclass(frozen=True, slots=True)
class VenueHealthSnapshot:
    """Capture one bounded health check against a venue.

    Attributes
    ----------
    venue_id
        Venue represented by the observation.
    status
        Normalized trading availability.
    checked_at
        Time at which the check completed.
    latency_ms
        End-to-end adapter latency in milliseconds, including failed checks.
    source
        External resource used by the adapter.
    message
        Human-readable result or failure detail.
    error_type
        Stable exception or venue-outage classification when unhealthy.
    http_status
        Upstream HTTP response status, including successful health responses.
    retryable
        Whether a later health check may reasonably succeed without reconfiguration.
    """

    venue_id: VenueID
    status: VenueHealthStatus
    checked_at: Timestamp
    latency_ms: float | None
    source: str
    message: str
    error_type: str | None = None
    http_status: int | None = None
    retryable: bool = False

    def __post_init__(self) -> None:
        if self.latency_ms is not None and self.latency_ms < 0:
            raise ValueError("Venue health latency cannot be negative")
        if self.http_status is not None and not 100 <= self.http_status <= 599:
            raise ValueError("Venue health HTTP status must be between 100 and 599")
