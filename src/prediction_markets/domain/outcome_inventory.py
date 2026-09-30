"""Model venue outcome inventory and recoverable collateral operations.

Responsibilities
----------------
- Represent split, merge, and redeem intents without venue protocol details.
- Track available YES, NO, and collateral balances.
- Carry opaque prepared requests and reconciliation references.
"""

from dataclasses import dataclass, replace
from decimal import Decimal
from enum import Enum

from prediction_markets.domain.shared.value_objects import (
    ContractID,
    MarketID,
    Money,
    PortfolioID,
    Price,
    Quantity,
    Timestamp,
    VenueID,
)
from prediction_markets.domain.trading.portfolio import STRATEGY_PORTFOLIO_ID


class OutcomeInventoryAction(Enum):
    """Enumerate supported outcome-token inventory operations."""

    SPLIT = "split"
    MERGE = "merge"
    REDEEM = "redeem"


class InventorySubmissionStatus(Enum):
    """Describe what is known immediately after submitting an operation."""

    ACCEPTED = "accepted"
    REJECTED = "rejected"
    UNKNOWN = "unknown"


class InventoryReconciliationStatus(Enum):
    """Describe whether a persisted operation can be resolved at its venue."""

    FOUND = "found"
    NOT_FOUND = "not_found"
    UNKNOWN = "unknown"


class InventoryOperationStatus(Enum):
    """Enumerate normalized venue operation states."""

    PENDING = "pending"
    CONFIRMED = "confirmed"
    FAILED = "failed"


@dataclass(frozen=True, slots=True)
class InventoryOperationID:
    """Identify one outcome-inventory operation inside the application.

    Invariants
    ----------
    - `value` contains at least one non-whitespace character.
    """

    value: str

    def __post_init__(self) -> None:
        if not self.value.strip():
            raise ValueError("InventoryOperationID must be non-empty")

    def __str__(self) -> str:
        return self.value


@dataclass(frozen=True, slots=True)
class OutcomeInventoryIntent:
    """Request one venue-neutral split, merge, or redeem operation.

    Attributes
    ----------
    operation_id
        Application-owned identifier used for durable correlation.
    venue_id
        Venue that owns the market inventory.
    market_id
        Venue market whose binary outcome inventory is affected.
    action
        Split, merge, or redeem operation to perform.
    quantity
        Number of complete sets to split or merge. Redemption operates on all
        redeemable inventory and therefore has no quantity.

    Invariants
    ----------
    - Split and merge quantities are strictly positive.
    - Redeem intents do not carry a quantity.
    """

    operation_id: InventoryOperationID
    venue_id: VenueID
    market_id: MarketID
    action: OutcomeInventoryAction
    quantity: Quantity | None = None
    portfolio_id: PortfolioID = STRATEGY_PORTFOLIO_ID

    def __post_init__(self) -> None:
        if self.action in {
            OutcomeInventoryAction.SPLIT,
            OutcomeInventoryAction.MERGE,
        }:
            if self.quantity is None or self.quantity.value <= 0:
                raise ValueError("Split and merge quantities must be positive")
        elif self.quantity is not None:
            raise ValueError("Redeem intents cannot specify a quantity")


@dataclass(frozen=True, slots=True)
class OutcomeInventoryBalance:
    """Capture available binary-outcome and collateral balances at one venue.

    Attributes
    ----------
    venue_id
        Venue reporting the balances.
    market_id
        Market whose YES and NO balances are reported.
    yes
        Available YES contracts.
    no
        Available NO contracts.
    collateral
        Available collateral that may be split into complete sets.
    observed_at
        Time at which the balances were observed.

    Notes
    -----
    - Only equal YES and NO quantities from the same venue market are mergeable.
    """

    venue_id: VenueID
    market_id: MarketID
    yes: Quantity
    no: Quantity
    collateral: Money
    observed_at: Timestamp
    yes_contract_id: ContractID | None = None
    no_contract_id: ContractID | None = None

    @property
    def mergeable_quantity(self) -> Quantity:
        """Return the complete-set quantity currently available for merge."""
        return Quantity(min(self.yes.value, self.no.value))


@dataclass(frozen=True, slots=True)
class OutcomeInventorySettlement:
    """Capture the final binary payout vector reported by one venue.

    Invariants
    ----------
    - YES and NO payouts form one complete collateral unit.
    - Both outcome contract identifiers belong to the reported venue market.
    """

    venue_id: VenueID
    market_id: MarketID
    yes_contract_id: ContractID
    no_contract_id: ContractID
    yes_payout: Price
    no_payout: Price
    observed_at: Timestamp

    def __post_init__(self) -> None:
        if self.yes_payout.value + self.no_payout.value != Decimal("1"):
            raise ValueError("Binary settlement payouts must sum to one")


@dataclass(frozen=True, slots=True)
class InventoryOperationReference:
    """Identify an operation using application and opaque venue recovery data.

    Attributes
    ----------
    venue_id
        Venue responsible for submitting and reconciling the operation.
    operation_id
        Application-owned correlation identifier.
    recovery_data
        Adapter-owned bytes sufficient to recover after process loss.
    quantity
        Planned complete-set or outcome-token quantity, when known.

    Invariants
    ----------
    - Recovery data is non-empty and interpreted only by the matching adapter.
    """

    venue_id: VenueID
    operation_id: InventoryOperationID
    recovery_data: bytes
    quantity: Quantity | None = None
    action: OutcomeInventoryAction | None = None
    portfolio_id: PortfolioID = STRATEGY_PORTFOLIO_ID
    balance_before: OutcomeInventoryBalance | None = None

    def __post_init__(self) -> None:
        if not self.recovery_data:
            raise ValueError("Inventory operation recovery data must be non-empty")
        if self.quantity is not None and self.quantity.value < 0:
            raise ValueError("Inventory operation quantity cannot be negative")


@dataclass(frozen=True, slots=True)
class PreparedInventoryOperation:
    """Carry an exact opaque inventory request prepared for durable submission.

    Attributes
    ----------
    intent
        Venue-neutral operation requested by the application.
    reference
        Stable identity used for reconciliation after a restart.
    request
        Adapter-owned serialized request submitted without rebuilding it.

    Invariants
    ----------
    - Intent and reference identify the same operation and venue.
    - The serialized request is non-empty.
    """

    intent: OutcomeInventoryIntent
    reference: InventoryOperationReference
    request: bytes

    def __post_init__(self) -> None:
        if self.intent.operation_id != self.reference.operation_id:
            raise ValueError("Prepared inventory operation IDs must match")
        if self.intent.venue_id != self.reference.venue_id:
            raise ValueError("Prepared inventory operation venues must match")
        if not self.request:
            raise ValueError("Prepared inventory operation request must be non-empty")


@dataclass(frozen=True, slots=True)
class InventoryOperationSnapshot:
    """Describe the latest normalized state of an inventory operation.

    Attributes
    ----------
    reference
        Stable operation identity.
    status
        Pending, confirmed, or failed venue state.
    updated_at
        Time at which the state was observed.
    transaction_id
        Optional venue transaction or operation identifier.
    quantity
        Confirmed outcome-token or complete-set quantity when reported.
    collateral
        Collateral debited or credited by the operation, when reported.
    payout
        Settlement payout produced by redemption, when reported.
    fee
        Fee charged in the venue's native denomination, when reported.
    fee_settlement_cost
        Fee converted to the portfolio settlement currency, when reported.
    fee_observed_at
        Economic timestamp used to convert the native fee.
    reason
        Optional venue explanation for failure.
    """

    reference: InventoryOperationReference
    status: InventoryOperationStatus
    updated_at: Timestamp
    transaction_id: str | None = None
    quantity: Quantity | None = None
    yes_quantity: Quantity | None = None
    no_quantity: Quantity | None = None
    collateral: Money | None = None
    payout: Money | None = None
    yes_payout: Price | None = None
    no_payout: Price | None = None
    fee: Money | None = None
    fee_settlement_cost: Money | None = None
    fee_observed_at: Timestamp | None = None
    quality_flags: tuple[str, ...] = ()
    reason: str | None = None

    def __post_init__(self) -> None:
        if self.transaction_id is not None and not self.transaction_id.strip():
            raise ValueError("Inventory transaction ID cannot be blank")
        if self.quantity is not None and self.quantity.value < 0:
            raise ValueError("Inventory operation quantity cannot be negative")
        if self.yes_quantity is not None and self.yes_quantity.value < 0:
            raise ValueError("YES inventory quantity cannot be negative")
        if self.no_quantity is not None and self.no_quantity.value < 0:
            raise ValueError("NO inventory quantity cannot be negative")
        if self.reason is not None and not self.reason.strip():
            raise ValueError("Inventory operation reason cannot be blank")

    def with_balance_change(
        self,
        balance_after: OutcomeInventoryBalance,
    ) -> "InventoryOperationSnapshot":
        """Attach exact token and collateral deltas observed after confirmation.

        Parameters
        ----------
        balance_after
            Venue balances read after the operation confirmed.

        Returns
        -------
        InventoryOperationSnapshot
            Snapshot enriched with affected quantities and cash economics.

        Raises
        ------
        ValueError
            If the reference lacks its pre-operation balance or identities and
            currencies differ.
        """
        if self.status is not InventoryOperationStatus.CONFIRMED:
            raise ValueError("Only confirmed inventory operations have economics")
        before = self.reference.balance_before
        action = self.reference.action
        if before is None or action is None:
            raise ValueError("Inventory reference lacks accounting context")
        if (
            before.venue_id != balance_after.venue_id
            or before.market_id != balance_after.market_id
            or before.collateral.currency != balance_after.collateral.currency
        ):
            raise ValueError("Inventory balance identities or currencies differ")

        yes_delta = balance_after.yes.value - before.yes.value
        no_delta = balance_after.no.value - before.no.value
        cash_delta = balance_after.collateral.amount - before.collateral.amount
        currency = before.collateral.currency
        flags = list(self.quality_flags)
        if action is OutcomeInventoryAction.SPLIT:
            yes_quantity = Quantity(max(yes_delta, Decimal("0")))
            no_quantity = Quantity(max(no_delta, Decimal("0")))
            quantity = Quantity(min(yes_quantity.value, no_quantity.value))
            if yes_quantity != no_quantity:
                flags.append("INCONSISTENT_INVENTORY_DELTA")
            return replace(
                self,
                quantity=quantity,
                yes_quantity=yes_quantity,
                no_quantity=no_quantity,
                collateral=Money(max(-cash_delta, Decimal("0")), currency),
                quality_flags=tuple(flags),
            )
        if action is OutcomeInventoryAction.MERGE:
            yes_quantity = Quantity(max(-yes_delta, Decimal("0")))
            no_quantity = Quantity(max(-no_delta, Decimal("0")))
            quantity = Quantity(min(yes_quantity.value, no_quantity.value))
            if yes_quantity != no_quantity:
                flags.append("INCONSISTENT_INVENTORY_DELTA")
            return replace(
                self,
                quantity=quantity,
                yes_quantity=yes_quantity,
                no_quantity=no_quantity,
                collateral=Money(max(cash_delta, Decimal("0")), currency),
                quality_flags=tuple(flags),
            )

        yes_quantity = Quantity(max(-yes_delta, Decimal("0")))
        no_quantity = Quantity(max(-no_delta, Decimal("0")))
        payout = Money(max(cash_delta, Decimal("0")), currency)
        yes_payout, no_payout, allocation_flag = _payout_prices(
            yes_quantity.value,
            no_quantity.value,
            payout.amount,
        )
        if allocation_flag is not None and allocation_flag not in flags:
            flags.append(allocation_flag)
        return replace(
            self,
            quantity=Quantity(yes_quantity.value + no_quantity.value),
            yes_quantity=yes_quantity,
            no_quantity=no_quantity,
            payout=payout,
            yes_payout=yes_payout,
            no_payout=no_payout,
            quality_flags=tuple(flags),
        )


@dataclass(frozen=True, slots=True)
class InventorySubmissionResult:
    """Report immediate submission certainty and any initial operation state."""

    status: InventorySubmissionStatus
    reference: InventoryOperationReference
    snapshot: InventoryOperationSnapshot | None = None
    reason: str | None = None

    def __post_init__(self) -> None:
        if self.snapshot is not None and self.snapshot.reference != self.reference:
            raise ValueError("Inventory submission snapshot reference must match")
        if self.reason is not None and not self.reason.strip():
            raise ValueError("Inventory submission reason cannot be blank")


@dataclass(frozen=True, slots=True)
class InventoryReconciliationResult:
    """Report the authoritative state found for a persisted operation.

    Invariants
    ----------
    - A found operation includes its normalized snapshot.
    - An absent or uncertain operation does not include a snapshot.
    """

    status: InventoryReconciliationStatus
    reference: InventoryOperationReference
    snapshot: InventoryOperationSnapshot | None = None

    def __post_init__(self) -> None:
        if (self.status is InventoryReconciliationStatus.FOUND) != (
            self.snapshot is not None
        ):
            raise ValueError("Only found inventory reconciliations must include a snapshot")
        if self.snapshot is not None and self.snapshot.reference != self.reference:
            raise ValueError("Inventory reconciliation snapshot reference must match")


def _payout_prices(
    yes_quantity: Decimal,
    no_quantity: Decimal,
    payout: Decimal,
) -> tuple[Price | None, Price | None, str | None]:
    """Allocate a confirmed aggregate redemption payout to binary outcomes.

    Notes
    -----
    - Exact winner and tie payouts are retained when observable from quantities.
    - Ambiguous aggregate payouts use one equal per-token price so portfolio-level
      realized PnL remains exact; the allocation is explicitly flagged.
    """
    total = yes_quantity + no_quantity
    if total == 0:
        return None, None, "MISSING_REDEEMED_QUANTITY"
    if payout < 0 or payout > total:
        return None, None, "INCONSISTENT_PAYOUT"
    if no_quantity == 0:
        return Price(payout / yes_quantity), None, None
    if yes_quantity == 0:
        return None, Price(payout / no_quantity), None
    if yes_quantity != no_quantity and payout == yes_quantity:
        return Price(Decimal("1")), Price(Decimal("0")), None
    if yes_quantity != no_quantity and payout == no_quantity:
        return Price(Decimal("0")), Price(Decimal("1")), None
    if payout * 2 == total:
        half = Price(Decimal("0.5"))
        return (
            half,
            half,
            "AGGREGATE_PAYOUT_ALLOCATION"
            if yes_quantity == no_quantity
            else None,
        )
    allocated = Price(payout / total)
    return allocated, allocated, "AGGREGATE_PAYOUT_ALLOCATION"
