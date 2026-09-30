"""Define validated value objects for the markets domain.

Responsibilities
----------------
- Enforce domain invariants at construction time.
"""

from dataclasses import dataclass
from prediction_markets.domain.shared.value_objects import OutcomeID, Timestamp
from prediction_markets.domain.markets.enums import MarketStatus


@dataclass(frozen = True, slots = True)
class MarketState:
    """Capture the immutable lifecycle state and optional timestamps of a market."""
    status: MarketStatus
    start_time: Timestamp | None = None
    close_time: Timestamp | None = None
    resolved_time: Timestamp | None = None

    def is_open(self) -> bool:
        return self.status == MarketStatus.ACTIVE

    def is_suspended(self) -> bool:
        return self.status == MarketStatus.SUSPENDED

    def is_closed(self) -> bool:
        return self.status == MarketStatus.CLOSED

    def is_resolved(self) -> bool:
        return self.status == MarketStatus.RESOLVED

    def is_cancelled(self) -> bool:
        return self.status == MarketStatus.CANCELLED

    def is_unknown(self) -> bool:
        return self.status == MarketStatus.UNKNOWN

    def is_terminal(self) -> bool:
        return self.status in {
            MarketStatus.RESOLVED,
            MarketStatus.CANCELLED,
        }

    def __repr__(self):
        return (
            "MarketState("
            f"status={self.status.value}, "
            f"start_time={self.start_time}, "
            f"close_time={self.close_time}, "
            f"resolved_time={self.resolved_time}"
            ")"
        )

    def __str__(self):
        return self.status.value


@dataclass(frozen=True, slots=True)
class MarketResolution:
    """Capture immutable resolution metadata and the resolved outcome when known."""
    rules: str | None = None
    source: str | None = None
    resolved_outcome_id: OutcomeID | None = None
    resolved_at: Timestamp | None = None

    def is_resolved(self) -> bool:
        return self.resolved_outcome_id is not None

    def __repr__(self):
        return (
            "MarketResolution("
            f"resolved_outcome_id={self.resolved_outcome_id}, "
            f"resolved_at={self.resolved_at}, "
            f"source={self.source!r}"
            ")"
        )

    def __str__(self):
        if self.resolved_outcome_id is None:
            return "unresolved"
        return f"resolved={self.resolved_outcome_id}"

