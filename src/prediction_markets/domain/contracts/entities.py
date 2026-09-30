"""Define the core contracts domain entities.

Responsibilities
----------------
- Model identity, state, and behavior independent of infrastructure.
"""

from dataclasses import dataclass
from decimal import Decimal

from prediction_markets.domain.contracts.value_objects import LotSize, Payout, TickSize
from prediction_markets.domain.shared.value_objects import (
    ContractID,
    Currency,
    MarketID,
    OutcomeID,
    Quantity,
    VenueID)


@dataclass(frozen=True, slots=True)
class BinaryContract:
    """Represent an immutable venue contract identified by `id`.

    Attributes
    ----------
    lot_size
        Smallest supported quantity increment.
    minimum_order_size
        Smallest quantity accepted for one order, when reported by the venue.

    Invariants
    ----------
    - The true payout is greater than the false payout.
    - A provided symbol is not blank.
    - A provided minimum order size is strictly positive.
    """
    id: ContractID
    market_id: MarketID
    outcome_id: OutcomeID
    venue_id: VenueID
    payout_currency: Currency
    payout_if_true: Payout = Payout(Decimal("1"))
    payout_if_false: Payout = Payout(Decimal("0"))
    symbol: str | None = None
    tick_size: TickSize | None = None
    lot_size: LotSize | None = None
    minimum_order_size: Quantity | None = None

    def __post_init__(self):
        if self.payout_if_true.value <= self.payout_if_false.value:
            raise ValueError("BinaryContract true payout must be greater than false payout")

        if self.symbol is not None and not self.symbol.strip():
            raise ValueError("BinaryContract symbol cannot be blank if provided")

        if self.minimum_order_size is not None and self.minimum_order_size.value <= 0:
            raise ValueError("BinaryContract minimum order size must be positive")

    def max_payout(self) -> Payout:
        return self.payout_if_true

    def min_payout(self) -> Payout:
        return self.payout_if_false

    def is_standard_binary(self) -> bool:
        return (
            self.payout_if_true.value == Decimal("1")
            and self.payout_if_false.value == Decimal("0")
        )

    def __repr__(self):
        return (
            f"BinaryContract("
            f"id={self.id}, "
            f"market_id={self.market_id}, "
            f"outcome_id={self.outcome_id}, "
            f"venue_id={self.venue_id}"
            f")"
        )

    def __str__(self):
        if self.symbol:
            return self.symbol
        return f"{self.market_id}:{self.outcome_id}"
