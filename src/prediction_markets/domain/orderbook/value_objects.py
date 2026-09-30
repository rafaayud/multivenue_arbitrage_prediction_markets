"""Define validated value objects for the orderbook domain.

Responsibilities
----------------
- Enforce domain invariants at construction time.
"""

from dataclasses import dataclass
from prediction_markets.domain.shared.value_objects import Price, Quantity

@dataclass(frozen = True, slots = True)
class OrderBookLevel:
    """Represent an immutable price and quantity level.

    Invariants
    ----------
    - Price and quantity are validated value objects.
    """
    price: Price
    quantity: Quantity

    def __post_init__(self):
        if not self.price:
            raise ValueError("OrderBookLevel must have a price")
        if not self.quantity:
            raise ValueError("OrderBookLevel must have a quantity")

    def __repr__(self):
        return f"OrderBookLevel(price={self.price}, quantity={self.quantity})"

    def __str__(self):
        return f"{self.price} @ {self.quantity}"