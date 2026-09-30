"""Define validated value objects for the shared domain.

Responsibilities
----------------
- Enforce domain invariants at construction time.
"""

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal


def _require_non_empty(value: str, field_name: str) -> None:
    if not value or not value.strip():
        raise ValueError(f"{field_name} must be non-empty")


@dataclass(frozen = True, slots = True)
class MarketID:
    """Represent an immutable non-empty market identifier.

    Invariants
    ----------
    - `value` contains at least one non-whitespace character.

    Notes
    -----
    - Equality and hashing are value-based.
    """
    value: str

    def __post_init__(self):
        _require_non_empty(self.value, "MarketID")

    def __repr__(self):
        return f"MarketID({self.value})"

    def __str__(self):
        return self.value


@dataclass(frozen = True, slots = True)
class EventID:
    """Represent an immutable non-empty event identifier.

    Invariants
    ----------
    - `value` contains at least one non-whitespace character.

    Notes
    -----
    - Equality and hashing are value-based.
    """
    value: str

    def __post_init__(self):
        _require_non_empty(self.value, "EventID")

    def __repr__(self):
        return f"EventID({self.value})"

    def __str__(self):
        return self.value


@dataclass(frozen = True, slots = True)
class VenueID:
    """Represent an immutable non-empty venue identifier.

    Invariants
    ----------
    - `value` contains at least one non-whitespace character.

    Notes
    -----
    - Equality and hashing are value-based.
    """
    value: str

    def __post_init__(self):
        _require_non_empty(self.value, "VenueID")

    def __repr__(self):
        return f"VenueID({self.value})"

    def __str__(self):
        return self.value


@dataclass(frozen = True, slots = True)
class OutcomeID:
    """Represent an immutable non-empty outcome identifier.

    Invariants
    ----------
    - `value` contains at least one non-whitespace character.

    Notes
    -----
    - Equality and hashing are value-based.
    """
    value: str

    def __post_init__(self):
        _require_non_empty(self.value, "OutcomeID")

    def __repr__(self):
        return f"OutcomeID({self.value})"

    def __str__(self):
        return self.value


@dataclass(frozen = True, slots = True)
class ContractID:
    """Represent an immutable non-empty contract identifier.

    Invariants
    ----------
    - `value` contains at least one non-whitespace character.

    Notes
    -----
    - Equality and hashing are value-based.
    """
    value: str

    def __post_init__(self):
        _require_non_empty(self.value, "ContractID")

    def __repr__(self):
        return f"ContractID({self.value})"

    def __str__(self):
        return self.value


@dataclass(frozen = True, slots = True)
class OrderID:
    """Represent an immutable non-empty venue order identifier.

    Invariants
    ----------
    - `value` contains at least one non-whitespace character.

    Notes
    -----
    - Equality and hashing are value-based.
    """
    value: str

    def __post_init__(self):
        _require_non_empty(self.value, "OrderID")

    def __repr__(self):
        return f"OrderID({self.value})"

    def __str__(self):
        return self.value


@dataclass(frozen = True, slots = True)
class ClientOrderID:
    """Represent an immutable non-empty client order identifier.

    Invariants
    ----------
    - `value` contains at least one non-whitespace character.

    Notes
    -----
    - Equality and hashing are value-based.
    """
    value: str

    def __post_init__(self):
        _require_non_empty(self.value, "ClientOrderID")

    def __repr__(self):
        return f"ClientOrderID({self.value})"

    def __str__(self):
        return self.value


@dataclass(frozen = True, slots = True)
class TradeID:
    """Represent an immutable non-empty trade identifier.

    Invariants
    ----------
    - `value` contains at least one non-whitespace character.

    Notes
    -----
    - Equality and hashing are value-based.
    """
    value: str

    def __post_init__(self):
        _require_non_empty(self.value, "TradeID")

    def __repr__(self):
        return f"TradeID({self.value})"

    def __str__(self):
        return self.value


@dataclass(frozen = True, slots = True)
class PositionID:
    """Represent an immutable non-empty position identifier.

    Invariants
    ----------
    - `value` contains at least one non-whitespace character.

    Notes
    -----
    - Equality and hashing are value-based.
    """
    value: str

    def __post_init__(self):
        _require_non_empty(self.value, "PositionID")

    def __repr__(self):
        return f"PositionID({self.value})"

    def __str__(self):
        return self.value


@dataclass(frozen = True, slots = True)
class StrategyID:
    """Represent an immutable non-empty strategy identifier.

    Invariants
    ----------
    - `value` contains at least one non-whitespace character.

    Notes
    -----
    - Equality and hashing are value-based.
    """
    value: str

    def __post_init__(self):
        _require_non_empty(self.value, "StrategyID")

    def __repr__(self):
        return f"StrategyID({self.value})"

    def __str__(self):
        return self.value


@dataclass(frozen = True, slots = True)
class PortfolioID:
    """Represent an immutable non-empty portfolio identifier.

    Invariants
    ----------
    - `value` contains at least one non-whitespace character.

    Notes
    -----
    - Equality and hashing are value-based.
    """
    value: str

    def __post_init__(self):
        _require_non_empty(self.value, "PortfolioID")

    def __repr__(self):
        return f"PortfolioID({self.value})"

    def __str__(self):
        return self.value


@dataclass(frozen = True, slots = True)
class Currency:
    """Represent an immutable normalized currency code.

    Invariants
    ----------
    - The code is non-empty, uppercase, and alphanumeric apart from underscores.
    """
    code: str

    def __post_init__(self):
        _require_non_empty(self.code, "Currency code")
        normalized = self.code.upper()
        if not normalized.replace("_", "").isalnum():
            raise ValueError("Currency code must be alphanumeric")
        object.__setattr__(self, "code", normalized)

    def __repr__(self):
        return f"Currency({self.code})"

    def __str__(self):
        return self.code


@dataclass(frozen = True, slots = True)
class Money:
    """Represent an immutable non-negative amount in one currency.

    Invariants
    ----------
    - `amount` is greater than or equal to zero.
    """
    amount: Decimal
    currency: Currency

    def __post_init__(self):
        if self.amount < 0:
            raise ValueError("Money amount must be non-negative")

    def __repr__(self):
        return f"Money({self.amount}, {self.currency})"

    def __str__(self):
        return f"{self.amount:.2f} {self.currency}"


@dataclass(frozen = True, slots = True, order = True)
class Price:
    """Represent an immutable prediction-market price as a decimal fraction.

    Invariants
    ----------
    - `value` lies between 0 and 1 inclusive.
    """
    value: Decimal

    def __post_init__(self):
        if self.value < 0 or self.value > 1:
            raise ValueError("Price must be between 0 and 1")

    def __repr__(self):
        return f"Price({self.value})"

    def __str__(self):
        return f"{self.value:.2f}"


@dataclass(frozen = True, slots = True, order = True)
class Quantity:
    """Represent an immutable non-negative contract quantity.

    Invariants
    ----------
    - `value` is greater than or equal to zero.
    """
    value: Decimal

    def __post_init__(self):
        if self.value < 0:
            raise ValueError("Quantity must be non-negative")

    def __repr__(self):
        return f"Quantity({self.value})"

    def __str__(self):
        return f"{self.value:.2f}"


@dataclass(frozen = True, slots = True, order = True)
class Probability:
    """Represent an immutable probability as a decimal fraction.

    Invariants
    ----------
    - `value` lies between 0 and 1 inclusive.
    """
    value: Decimal

    def __post_init__(self):
        if self.value < 0 or self.value > 1:
            raise ValueError("Probability must be between 0 and 1")

    def __repr__(self):
        return f"Probability({self.value})"

    def __str__(self):
        return f"{self.value:.2f}"

    def complement(self):
        return Probability(Decimal("1") - self.value)

    def to_percentage(self):
        return self.value * Decimal('100')


@dataclass(frozen = True, slots = True)
class Timestamp:
    """Represent an immutable timezone-aware instant.

    Invariants
    ----------
    - `value` includes timezone information.
    """
    value: datetime

    def __post_init__(self):
        if self.value.tzinfo is None:
            raise ValueError("Timestamp must be timezone-aware")

    @classmethod
    def now(cls) -> "Timestamp":
        return cls(datetime.now(timezone.utc))

    @classmethod
    def from_iso(cls, iso: str) -> "Timestamp":
        """Parse an ISO timestamp, interpreting a missing timezone as UTC.

        Returns
        -------
        Timestamp
            A timezone-aware immutable timestamp.
        """
        dt = datetime.fromisoformat(iso)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo = timezone.utc)
        return cls(dt)

    def to_unix_ms(self) -> int:
        return int(self.value.timestamp() * 1000)

    def __repr__(self):
        return f"Timestamp({self.value.isoformat()})"

    def __str__(self):
        return self.value.isoformat()
    
    def __add__(self, other: timedelta) -> "Timestamp":
        if isinstance(other, timedelta):
            return Timestamp(self.value + other)
    
    def __sub__(self, other: timedelta) -> "Timestamp":
        if isinstance(other, timedelta):
            return Timestamp(self.value - other)
    
    def __lt__(self, other: "Timestamp") -> bool:
        if not isinstance(other, Timestamp):
            raise TypeError("Cannot compare Timestamp and non-Timestamp")
        return self.value < other.value
    
    def __le__(self, other: "Timestamp") -> bool:
        if not isinstance(other, Timestamp):
            raise TypeError("Cannot compare Timestamp and non-Timestamp")
        return self.value <= other.value
    
    def __gt__(self, other: "Timestamp") -> bool:
        if not isinstance(other, Timestamp):
            raise TypeError("Cannot compare Timestamp and non-Timestamp")
        return self.value > other.value
    
    def __ge__(self, other: "Timestamp") -> bool:
        if not isinstance(other, Timestamp):
            raise TypeError("Cannot compare Timestamp and non-Timestamp")
        return self.value >= other.value
    
    def __eq__(self, other: "Timestamp") -> bool:
        if not isinstance(other, Timestamp):
            raise TypeError("Cannot compare Timestamp and non-Timestamp")
        return self.value == other.value