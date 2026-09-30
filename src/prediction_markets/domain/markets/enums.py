"""Define the finite states used by the markets domain.

Responsibilities
----------------
- Provide stable symbolic values for domain decisions.
"""

from enum import Enum


class MarketStatus(Enum):
    """Enumerate normalized market lifecycle states."""
    ACTIVE = "active"
    SUSPENDED = "suspended"
    CLOSED = "closed"
    RESOLVED = "resolved"
    CANCELLED = "cancelled"
    UNKNOWN = "unknown"


class BinaryOutcome(Enum):
    """Enumerate the two outcomes of a binary market."""
    YES = "yes"
    NO = "no"

    def is_yes(self) -> bool:
        return self == BinaryOutcome.YES

    def is_no(self) -> bool:
        return self == BinaryOutcome.NO
