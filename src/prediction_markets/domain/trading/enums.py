"""Define the finite states used by the trading domain.

Responsibilities
----------------
- Provide stable symbolic values for domain decisions.
"""

from enum import Enum


class OrderSide(Enum):
    """Enumerate buy and sell order directions."""
    BUY = "buy"
    SELL = "sell"


class OrderType(Enum):
    """Enumerate supported order execution types."""
    MARKET = "market"
    LIMIT = "limit"


class TimeInForce(Enum):
    """Enumerate supported order lifetime policies."""
    GTC = "gtc"
    IOC = "ioc"
    FOK = "fok"
    GTD = "gtd"


class OrderStatus(Enum):
    """Enumerate normalized order lifecycle states."""
    CREATED = "created"
    SUBMITTED = "submitted"
    ACCEPTED = "accepted"
    PARTIALLY_FILLED = "partially_filled"
    FILLED = "filled"
    CANCELLED = "cancelled"
    REJECTED = "rejected"
    EXPIRED = "expired"


class SubmissionStatus(Enum):
    """Describe what is known immediately after a submission attempt."""

    ACCEPTED = "accepted"
    REJECTED = "rejected"
    UNKNOWN = "unknown"


class ReconciliationStatus(Enum):
    """Describe whether a persisted order can be resolved at its venue."""

    FOUND = "found"
    NOT_FOUND = "not_found"
    UNKNOWN = "unknown"


class SignalDirection(Enum):
    """Enumerate actionable and neutral trading directions."""
    BUY = "buy"
    SELL = "sell"
    HOLD = "hold"


class PositionSide(Enum):
    """Enumerate long, short, and flat position states."""
    LONG = "long"
    SHORT = "short"
    FLAT = "flat"


class CashMovementKind(Enum):
    """Enumerate cash entering, leaving, or moving between portfolios."""

    DEPOSIT = "deposit"
    WITHDRAWAL = "withdrawal"
    TRANSFER = "transfer"


class RecoveryStatus(Enum):
    """Enumerate exposure-recovery lifecycle states."""
    PENDING = "pending"
    ATTEMPTING = "attempting"
    RESOLVED = "resolved"
    NEEDS_REVIEW = "needs_review"


class RecoveryRoute(Enum):
    """Enumerate the two ways to neutralize unmatched arbitrage exposure."""

    COMPLETE_MISSING_LEG = "complete_missing_leg"
    UNWIND_EXCESS = "unwind_excess"


class ArbitrageExecutionStatus(Enum):
    """Enumerate durable two-leg execution states.

    Notes
    -----
    - ``REJECTED`` is terminal with no residual exposure and no fills.
    """
    PLANNED = "planned"
    PRIMARY_PENDING = "primary_pending"
    HEDGE_PENDING = "hedge_pending"
    RECOVERY_PENDING = "recovery_pending"
    UNWIND_PENDING = "unwind_pending"
    ACCOUNTING_PENDING = "accounting_pending"
    COMPLETED = "completed"
    RECOVERED = "recovered"
    NEEDS_REVIEW = "needs_review"
    REJECTED = "rejected"
