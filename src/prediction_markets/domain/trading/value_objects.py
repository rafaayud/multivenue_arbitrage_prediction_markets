"""Define validated value objects for the trading domain.

Responsibilities
----------------
- Enforce domain invariants at construction time.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import TYPE_CHECKING

from prediction_markets.domain.orderbook.value_objects import OrderBookLevel
from prediction_markets.domain.shared.value_objects import (
    ClientOrderID,
    ContractID,
    Money,
    Price,
    Quantity,
    Timestamp,
    VenueID,
)
from prediction_markets.domain.trading.enums import (
    OrderSide,
    ReconciliationStatus,
    RecoveryRoute,
    SubmissionStatus,
)

if TYPE_CHECKING:
    from prediction_markets.domain.trading.entities import OrderSnapshot


@dataclass(frozen=True, slots=True)
class OrderBookDecisionSnapshot:
    """Capture the executable depth used to price one order leg.

    Attributes
    ----------
    venue_id : VenueID
        Venue that published the source book.
    contract_id : ContractID
        Contract submitted for this leg.
    side : OrderSide
        Side used to select executable asks or bids.
    limit_price : Price
        Submitted limit used for the leg.
    requested_quantity : Quantity
        Quantity requested by the order command.
    levels : tuple[OrderBookLevel, ...]
        Best-to-worst levels executable at the submitted limit.
    book_timestamp : Timestamp, optional
        Venue timestamp when one was supplied.
    book_age_ns : int, optional
        Local monotonic age at capture time, in nanoseconds.
    source_hash : str, optional
        Venue order-book hash when supplied by the feed.
    captured_at : Timestamp
        Local wall-clock capture time.

    Invariants
    ----------
    - Requested quantity is positive.
    - Every retained level is executable at the submitted limit.
    - Book age is non-negative when available.
    """

    venue_id: VenueID
    contract_id: ContractID
    side: OrderSide
    limit_price: Price
    requested_quantity: Quantity
    levels: tuple[OrderBookLevel, ...]
    book_timestamp: Timestamp | None
    book_age_ns: int | None
    source_hash: str | None
    captured_at: Timestamp

    def __post_init__(self) -> None:
        if self.requested_quantity.value <= 0:
            raise ValueError("Decision snapshot quantity must be positive")
        if self.book_age_ns is not None and self.book_age_ns < 0:
            raise ValueError("Decision snapshot book age must be non-negative")
        if any(
            level.price.value > self.limit_price.value
            if self.side is OrderSide.BUY
            else level.price.value < self.limit_price.value
            for level in self.levels
        ):
            raise ValueError("Decision snapshot contains a non-executable level")

    def available_quantity(self) -> Quantity:
        """Return cumulative quantity executable at the submitted limit."""
        return Quantity(
            sum((level.quantity.value for level in self.levels), Decimal("0")),
        )

    def shortfall_quantity(self) -> Quantity:
        """Return requested quantity missing from the captured executable depth."""
        return Quantity(
            max(
                Decimal("0"),
                self.requested_quantity.value - self.available_quantity().value,
            ),
        )

    def vwap(self) -> Price | None:
        """Calculate VWAP for the executable portion of the requested quantity."""
        remaining = self.requested_quantity.value
        filled = Decimal("0")
        notional = Decimal("0")
        for level in self.levels:
            taken = min(remaining, level.quantity.value)
            filled += taken
            notional += taken * level.price.value
            remaining -= taken
            if remaining <= 0:
                break
        return Price(notional / filled) if filled > 0 else None


@dataclass(frozen=True, slots=True, order=True)
class Edge:
    """Represent an immutable signed trading edge as a decimal fraction.

    Invariants
    ----------
    - `value` lies between -1 and 1 inclusive.
    """
    value: Decimal

    def __post_init__(self):
        if self.value < Decimal("-1") or self.value > Decimal("1"):
            raise ValueError("Edge must be between -1 and 1")

    def is_positive(self) -> bool:
        return self.value > 0

    def __repr__(self):
        return f"Edge({self.value})"

    def __str__(self):
        return f"{self.value:.4f}"


@dataclass(frozen=True, slots=True, order=True)
class Confidence:
    """Represent immutable signal confidence as a decimal fraction.

    Invariants
    ----------
    - `value` lies between 0 and 1 inclusive.
    """
    value: Decimal

    def __post_init__(self):
        if self.value < 0 or self.value > 1:
            raise ValueError("Confidence must be between 0 and 1")

    def __repr__(self):
        return f"Confidence({self.value})"

    def __str__(self):
        return f"{self.value:.2f}"


@dataclass(frozen=True, slots=True, order=True)
class FillRatio:
    """Represent an immutable cumulative fill ratio.

    Invariants
    ----------
    - `value` lies between 0 and 1 inclusive.
    """
    value: Decimal

    def __post_init__(self):
        if self.value < 0 or self.value > 1:
            raise ValueError("FillRatio must be between 0 and 1")

    def is_complete(self) -> bool:
        return self.value == Decimal("1")

    def __repr__(self):
        return f"FillRatio({self.value})"

    def __str__(self):
        return f"{self.value:.2f}"


@dataclass(frozen=True, slots=True)
class TradingFee:
    """Represent a venue fee and its comparable settlement cost.

    Attributes
    ----------
    charged
        Amount and asset actually charged by the venue.
    settlement_cost
        Conservative value in the strategy's common settlement unit.

    Notes
    -----
    - Outcome-token fees use their maximum payout value for settlement cost so
      arbitrage profitability remains guaranteed for either resolution.
    """

    charged: Money
    settlement_cost: Money


@dataclass(frozen=True, slots=True)
class RecoveryEconomics:
    """Describe the result of neutralizing one residual exposure quantity.

    Attributes
    ----------
    route : RecoveryRoute
        Whether recovery completes the missing complementary leg or unwinds
        the excess fill on its original contract.
    recovery_side : OrderSide
        Side required for the recovery order.
    quantity : Quantity
        Residual contract quantity evaluated.
    gross_result : Decimal
        Signed collateral result before fees. Positive values are profitable.
    total_fees : Money, optional
        Combined proportional source fee and recovery fee in their common
        settlement currency, or ``None`` while either fee is unknown.
    net_result : Decimal, optional
        Signed result after fees, or ``None`` while fees are unknown.
    """

    route: RecoveryRoute
    recovery_side: OrderSide
    quantity: Quantity
    gross_result: Decimal
    total_fees: Money | None
    net_result: Decimal | None


@dataclass(frozen=True, slots=True)
class RecoveryDecision:
    """Describe one executable recovery route selected from current depth.

    Attributes
    ----------
    venue_id : VenueID
        Venue receiving the future recovery order.
    contract_id : ContractID
        Contract bought or sold by the recovery order.
    limit_price : Price
        Worst consumed book price and protective execution limit.
    estimated_vwap : Price
        Quantity-weighted price across the quoted book levels.
    estimated_fee : TradingFee
        Recovery taker fee estimated level by level.
    economics : RecoveryEconomics
        Complete fee-aware result including the allocated source fee.
    """

    venue_id: VenueID
    contract_id: ContractID
    limit_price: Price
    estimated_vwap: Price
    estimated_fee: TradingFee
    economics: RecoveryEconomics

    @property
    def route(self) -> RecoveryRoute:
        """Return the selected economic route."""
        return self.economics.route

    @property
    def side(self) -> OrderSide:
        """Return the side required by the selected route."""
        return self.economics.recovery_side

    @property
    def quantity(self) -> Quantity:
        """Return the quoted executable quantity."""
        return self.economics.quantity


def recovery_order_side(
    source_side: OrderSide,
    route: RecoveryRoute,
) -> OrderSide:
    """Return the order side required by one recovery route.

    Parameters
    ----------
    source_side : OrderSide
        Side of the original unmatched fill.
    route : RecoveryRoute
        Candidate recovery route.

    Returns
    -------
    OrderSide
        Original side when completing the missing leg, otherwise its opposite.
    """
    if route is RecoveryRoute.COMPLETE_MISSING_LEG:
        return source_side
    return OrderSide.SELL if source_side is OrderSide.BUY else OrderSide.BUY


def evaluate_recovery_economics(
    source_side: OrderSide,
    source_price: Price,
    quantity: Quantity,
    route: RecoveryRoute,
    recovery_price: Price,
    source_fee: Money | None,
    recovery_fee: Money | None,
) -> RecoveryEconomics:
    """Calculate complete-leg or unwind economics for residual exposure.

    Parameters
    ----------
    source_side : OrderSide
        Side of the original unmatched fill.
    source_price : Price
        Actual average price of the unmatched fill.
    quantity : Quantity
        Positive residual quantity being neutralized.
    route : RecoveryRoute
        Candidate recovery route.
    recovery_price : Price
        Estimated or actual average recovery price.
    source_fee : Money, optional
        Original fill fee allocated proportionally to the residual quantity in
        the common settlement currency, or ``None`` when unknown.
    recovery_fee : Money, optional
        Estimated or actual recovery fee in the common settlement currency, or
        ``None`` when unknown.

    Returns
    -------
    RecoveryEconomics
        Gross and fee-aware result together with the required recovery side.

    Raises
    ------
    ValueError
        If quantity is zero or fees use different settlement currencies.

    Notes
    -----
    - Buys are negative cashflows and sells are positive cashflows.
    - Completing complementary buys adds one unit of payout per contract.
    - Completing complementary sells consumes one unit of collateral inventory
      per contract. Unwinds add no complete-pair adjustment.
    - Missing fees remain unknown and never default to zero.
    """
    if quantity.value <= 0:
        raise ValueError("Recovery quantity must be positive")

    recovery_side = recovery_order_side(source_side, route)
    if route is RecoveryRoute.COMPLETE_MISSING_LEG:
        pair_value = (
            quantity.value
            if source_side is OrderSide.BUY
            else -quantity.value
        )
    else:
        pair_value = Decimal("0")
    source_cashflow = (
        source_price.value * quantity.value
        if source_side is OrderSide.SELL
        else -source_price.value * quantity.value
    )
    recovery_cashflow = (
        recovery_price.value * quantity.value
        if recovery_side is OrderSide.SELL
        else -recovery_price.value * quantity.value
    )
    gross_result = source_cashflow + recovery_cashflow + pair_value

    total_fees = None
    net_result = None
    if source_fee is not None and recovery_fee is not None:
        if source_fee.currency != recovery_fee.currency:
            raise ValueError("Recovery fees must share one settlement currency")
        total_fees = Money(
            source_fee.amount + recovery_fee.amount,
            source_fee.currency,
        )
        net_result = gross_result - total_fees.amount

    return RecoveryEconomics(
        route=route,
        recovery_side=recovery_side,
        quantity=quantity,
        gross_result=gross_result,
        total_fees=total_fees,
        net_result=net_result,
    )


@dataclass(frozen=True, slots=True)
class OrderReference:
    """Identify an order using application and opaque venue recovery data.

    Attributes
    ----------
    venue_id
        Platform responsible for executing and reconciling the order.
    client_order_id
        Application-generated identifier used for internal correlation.
    recovery_data
        Adapter-owned bytes sufficient to recover the order without prior process memory.

    Invariants
    ----------
    - Recovery data is non-empty and interpreted only by the matching venue adapter.
    """

    venue_id: VenueID
    client_order_id: ClientOrderID
    recovery_data: bytes

    def __post_init__(self) -> None:
        if not self.recovery_data:
            raise ValueError("Order recovery data must be non-empty")


@dataclass(frozen=True, slots=True)
class PreparedOrder:
    """Carry the exact opaque request that must be persisted before submission.

    Attributes
    ----------
    reference
        Stable reference used to reconcile the order after a restart.
    request
        Adapter-owned serialized request submitted without rebuilding the order.

    Invariants
    ----------
    - The serialized request is non-empty and immutable.
    """

    reference: OrderReference
    request: bytes

    def __post_init__(self) -> None:
        if not self.request:
            raise ValueError("Prepared order request must be non-empty")


@dataclass(frozen=True, slots=True)
class SubmissionResult:
    """Report submission certainty, venue reason, and any normalized snapshot.

    Attributes
    ----------
    status
        Immediate certainty reported for the submission attempt.
    reference
        Stable order identity used for later reconciliation.
    snapshot
        Initial normalized venue state when the order was accepted.
    reason
        Venue or transport explanation for a rejected submission or terminal
        initial order state.
    """

    status: SubmissionStatus
    reference: OrderReference
    snapshot: OrderSnapshot | None = None
    reason: str | None = None


@dataclass(frozen=True, slots=True)
class ReconciliationResult:
    """Report the authoritative state discovered during reconciliation.

    Invariants
    ----------
    - A found order includes its normalized snapshot.
    - An absent or uncertain order does not include a snapshot.
    """

    status: ReconciliationStatus
    reference: OrderReference
    snapshot: OrderSnapshot | None = None

    def __post_init__(self) -> None:
        if (self.status is ReconciliationStatus.FOUND) != (self.snapshot is not None):
            raise ValueError("Only found reconciliations must include a snapshot")
