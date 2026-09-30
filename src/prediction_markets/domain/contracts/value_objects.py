"""Define validated value objects for the contracts domain.

Responsibilities
----------------
- Enforce domain invariants at construction time.
"""

from dataclasses import dataclass
from decimal import Decimal


@dataclass(frozen=True, slots=True, order=True)
class Payout:
    """Represent an immutable non-negative contract payout.

    Invariants
    ----------
    - `value` is greater than or equal to zero.
    """
    value: Decimal

    def __post_init__(self):
        if self.value < 0:
            raise ValueError("Payout must be non-negative")

    def __repr__(self):
        return f"Payout({self.value})"

    def __str__(self):
        return f"{self.value:.2f}"


@dataclass(frozen=True, slots=True, order=True)
class TickSize:
    """Represent the immutable minimum price increment for a contract.

    Invariants
    ----------
    - `value` is strictly positive.
    """
    value: Decimal

    def __post_init__(self):
        if self.value <= 0:
            raise ValueError("TickSize must be positive")

    def __repr__(self):
        return f"TickSize({self.value})"

    def __str__(self):
        return str(self.value)


@dataclass(frozen=True, slots=True, order=True)
class LotSize:
    """Represent the immutable minimum quantity increment for a contract.

    Invariants
    ----------
    - `value` is strictly positive.
    """
    value: Decimal

    def __post_init__(self):
        if self.value <= 0:
            raise ValueError("LotSize must be positive")

    def __repr__(self):
        return f"LotSize({self.value})"

    def __str__(self):
        return str(self.value)
