"""Define the core trading domain entities.

Responsibilities
----------------
- Model identity, state, and behavior independent of infrastructure.
"""

from dataclasses import dataclass, replace
from decimal import Decimal

from prediction_markets.domain.shared.value_objects import (
    ClientOrderID,
    ContractID,
    Currency,
    Money,
    OrderID,
    PortfolioID,
    PositionID,
    Price,
    Probability,
    Quantity,
    StrategyID,
    Timestamp,
    TradeID,
    VenueID,
)

from prediction_markets.domain.trading.enums import (
    ArbitrageExecutionStatus,
    CashMovementKind,
    OrderSide,
    OrderStatus,
    OrderType,
    PositionSide,
    RecoveryRoute,
    RecoveryStatus,
    SignalDirection,
    TimeInForce,
)

from prediction_markets.domain.trading.portfolio import STRATEGY_PORTFOLIO_ID
from prediction_markets.domain.trading.value_objects import (
    Confidence,
    Edge,
    FillRatio,
    OrderBookDecisionSnapshot,
    TradingFee,
)


@dataclass(frozen=True, slots=True)
class Signal:
    """A signal for a trading opportunity."""

    contract_id: ContractID
    direction: SignalDirection
    fair_probability: Probability
    confidence: Confidence
    generated_at: Timestamp
    strategy_id: StrategyID | None = None
    edge: Edge | None = None
    reason: str | None = None
    venue_id: VenueID | None = None
    quantity: Quantity | None = None
    limit_price: Price | None = None

    def __post_init__(self):
        if self.reason is not None and not self.reason.strip():
            raise ValueError("Signal reason cannot be blank if provided")

    def is_actionable(self) -> bool:
        return self.direction != SignalDirection.HOLD


@dataclass(frozen=True, slots=True)
class OrderIntent:
    """Describe one venue-independent order request and its execution constraints."""

    contract_id: ContractID
    side: OrderSide
    quantity: Quantity
    order_type: OrderType
    client_order_id: ClientOrderID | None = None
    limit_price: Price | None = None
    time_in_force: TimeInForce = TimeInForce.GTC
    strategy_id: StrategyID | None = None
    portfolio_id: PortfolioID | None = None
    created_at: Timestamp | None = None
    expires_at: Timestamp | None = None
    reason: str | None = None

    def __post_init__(self):
        if self.quantity.value <= 0:
            raise ValueError("OrderIntent quantity must be positive")

        if self.order_type == OrderType.LIMIT and self.limit_price is None:
            raise ValueError("Limit orders require a limit_price")

        if self.order_type == OrderType.MARKET and self.limit_price is not None:
            raise ValueError("Market orders cannot have a limit_price")

        if self.time_in_force == TimeInForce.GTD and self.expires_at is None:
            raise ValueError("GTD orders require expires_at")

        if self.reason is not None and not self.reason.strip():
            raise ValueError("OrderIntent reason cannot be blank if provided")

    def is_buy(self) -> bool:
        return self.side == OrderSide.BUY

    def is_sell(self) -> bool:
        return self.side == OrderSide.SELL


@dataclass(frozen=True, slots=True)
class ExecutionEvent:
    """Represent a normalized venue update, including cumulative and last-fill quantities."""

    status: OrderStatus
    client_order_id: ClientOrderID | None = None
    order_id: OrderID | None = None
    filled_quantity: Quantity = Quantity(Decimal("0"))
    last_fill_quantity: Quantity = Quantity(Decimal("0"))
    last_fill_price: Price | None = None
    average_price: Price | None = None
    reported_at: Timestamp | None = None
    message: str | None = None

    def __post_init__(self):
        if self.filled_quantity.value > 0 and self.average_price is None:
            raise ValueError("ExecutionEvent average_price is required when filled quantity is positive")

        if self.last_fill_quantity.value > 0 and self.last_fill_price is None:
            raise ValueError("ExecutionEvent last_fill_price is required when last fill quantity is positive")

        if self.last_fill_quantity.value > self.filled_quantity.value:
            raise ValueError("ExecutionEvent last fill quantity cannot exceed total filled quantity")

        if self.message is not None and not self.message.strip():
            raise ValueError("ExecutionEvent message cannot be blank if provided")

        if self.client_order_id is None and self.order_id is None:
            raise ValueError("ExecutionEvent requires either client_order_id or order_id")

    def fill_ratio(self, requested_quantity: Quantity) -> FillRatio:
        """
        Compute cumulative fill ratio against the requested quantity.

        Parameters
        ----------
        requested_quantity
            Original quantity used to normalize the fill.

        Returns
        -------
        FillRatio
            Exact Decimal-backed ratio; raises ``ValueError`` for non-positive input.
        """
        if requested_quantity.value <= 0:
            raise ValueError("Requested quantity must be positive")
        return FillRatio(self.filled_quantity.value / requested_quantity.value)

    def is_terminal(self) -> bool:
        return self.status in {
            OrderStatus.FILLED,
            OrderStatus.CANCELLED,
            OrderStatus.REJECTED,
            OrderStatus.EXPIRED,
        }


@dataclass(frozen=True, slots=True)
class OrderSnapshot:
    """Capture the latest normalized order state used by settlement and accounting.

    Attributes
    ----------
    may_receive_more_fills
        Adapter-provided fill finality, including cancelled orders with pending
        settlement. ``None`` keeps legacy inference for older journal records.
    reason
        Venue explanation for a terminal rejection or cancellation, when supplied.
    settlement_finalized_block
        Finalized chain block through which the adapter proved the complete fill
        quantity and invalidation of the remaining order. Absent for legacy data.
    """

    status: OrderStatus
    contract_id: ContractID
    side: OrderSide
    quantity: Quantity
    order_type: OrderType
    client_order_id: ClientOrderID | None = None
    order_id: OrderID | None = None
    limit_price: Price | None = None
    filled_quantity: Quantity = Quantity(Decimal("0"))
    average_price: Price | None = None
    fee: TradingFee | None = None
    created_at: Timestamp | None = None
    updated_at: Timestamp | None = None
    may_receive_more_fills: bool | None = None
    reason: str | None = None
    settlement_finalized_block: int | None = None

    def __post_init__(self):
        if self.settlement_finalized_block is not None and (
            type(self.settlement_finalized_block) is not int
            or self.settlement_finalized_block <= 0
            or not self.is_terminal()
            or self.may_receive_more_fills is not False
        ):
            raise ValueError("Finalized settlement requires a terminal snapshot with no pending fills")
        if self.quantity.value <= 0:
            raise ValueError("OrderSnapshot quantity must be positive")

        if self.filled_quantity.value > 0 and self.average_price is None:
            raise ValueError("OrderSnapshot average_price is required when filled quantity is positive")

        if self.client_order_id is None and self.order_id is None:
            raise ValueError("OrderSnapshot requires either client_order_id or order_id")

    def is_open(self) -> bool:
        return self.status in {
            OrderStatus.SUBMITTED,
            OrderStatus.ACCEPTED,
            OrderStatus.PARTIALLY_FILLED,
        }

    def is_terminal(self) -> bool:
        return self.status in {
            OrderStatus.FILLED,
            OrderStatus.CANCELLED,
            OrderStatus.REJECTED,
            OrderStatus.EXPIRED,
        }


@dataclass(frozen=True, slots=True)
class Trade:
    """Represent an immutable executed trade identified by `id`.

    Attributes
    ----------
    fee : Money, optional
        Amount charged by the venue in its native fee denomination.
    fee_settlement_cost : Money, optional
        Equivalent cost used for PnL accounting in settlement currency.
    journal_sequence : int, optional
        Durable application-journal sequence used to replay fills in the exact
        order in which they were accepted.

    Invariants
    ----------
    - Executed quantity is strictly positive.
    """
    id: TradeID
    contract_id: ContractID
    venue_id: VenueID
    side: OrderSide
    quantity: Quantity
    price: Price
    executed_at: Timestamp
    order_id: OrderID | None = None
    client_order_id: ClientOrderID | None = None
    portfolio_id: PortfolioID | None = None
    strategy_id: StrategyID | None = None
    fee: Money | None = None
    fee_settlement_cost: Money | None = None
    journal_sequence: int | None = None

    def __post_init__(self):
        if self.quantity.value <= 0:
            raise ValueError("Trade quantity must be positive")
        if self.journal_sequence is not None and self.journal_sequence <= 0:
            raise ValueError("Trade journal_sequence must be positive")


@dataclass(frozen=True, slots=True)
class Position:
    """Represent current exposure for one contract inside one portfolio.

    Attributes
    ----------
    id : PositionID
        Stable identity of the position.
    contract_id : ContractID
        Contract held by the position.
    portfolio_id : PortfolioID
        Portfolio that owns the exposure.
    quantity : Quantity
        Absolute size of the open exposure; zero when ``side`` is flat.
    side : PositionSide
        Long, short, or flat orientation of the exposure.
    venue_id : VenueID
        Venue where the exposure is held.
    average_price : Price, optional
        Volume-weighted entry price; required for open positions.
    current_price : Price, optional
        Latest mark price used for unrealized PnL, when available.
    opened_at : Timestamp, optional
        Wall-clock time when the position was opened.
    updated_at : Timestamp, optional
        Wall-clock time of the latest state change.

    Invariants
    ----------
    - Flat positions have zero quantity.
    - Open positions have strictly positive quantity.
    - Open positions require ``average_price``.
    - ``current_price`` is optional because fills do not carry a mark.
    - ``realized_pnl`` is cumulative gross PnL in the portfolio settlement
      currency; fees are kept separately.
    """

    id: PositionID
    contract_id: ContractID
    portfolio_id: PortfolioID
    quantity: Quantity
    side: PositionSide
    venue_id: VenueID
    average_price: Price | None = None
    current_price: Price | None = None
    realized_pnl: Decimal = Decimal("0")
    fees: Money | None = None
    quality_flags: tuple[str, ...] = ()
    opened_at: Timestamp | None = None
    updated_at: Timestamp | None = None

    def __post_init__(self):
        if self.side == PositionSide.FLAT and self.quantity.value != 0:
            raise ValueError("Flat positions must have zero quantity")
        if self.side != PositionSide.FLAT and self.quantity.value <= 0:
            raise ValueError("Open positions must have positive quantity")
        if self.side != PositionSide.FLAT and self.average_price is None:
            raise ValueError("Open positions require average_price")

    @property
    def average_entry_price(self) -> Price | None:
        """Return the WAC entry price under the legacy read-model name."""
        return self.average_price

    @property
    def signed_quantity(self) -> Decimal:
        """Return quantity signed positive for long and negative for short."""
        if self.side is PositionSide.SHORT:
            return -self.quantity.value
        return self.quantity.value

    @property
    def current_value(self) -> Decimal | None:
        """
        Return marked notional value, signed for short exposure.

        Returns
        -------
        Decimal or None
            Current marked notional, or ``None`` when no mark is available.
        """
        if self.current_price is None:
            return None
        return self.signed_quantity * self.current_price.value

    @property
    def unrealized_pnl(self) -> Decimal | None:
        """
        Return mark-to-market PnL against the average entry price.

        Returns
        -------
        Decimal or None
            Mark-to-market PnL, or ``None`` when no mark is available.
        """
        if self.side is PositionSide.FLAT:
            return Decimal("0")
        if self.current_price is None or self.average_price is None:
            return None
        return self.signed_quantity * (
            self.current_price.value - self.average_price.value
        )

    def is_open(self) -> bool:
        return self.side != PositionSide.FLAT

    def with_mark(self, price: Price, observed_at: Timestamp) -> "Position":
        """Return this immutable position with a newer market mark."""
        return replace(self, current_price=price, updated_at=observed_at)

    def notional_cost(self) -> Decimal:
        """Return the absolute WAC cost of the open exposure."""
        if self.average_price is None:
            return Decimal("0")
        return self.quantity.value * self.average_price.value


@dataclass(frozen=True, slots=True)
class AccountingCorrection:
    """Replace one recorded trade and its derived position with an audit trail."""

    id: str
    target_trade_id: TradeID
    original_trade: Trade
    replacement_trade: Trade
    resulting_position: Position
    reason: str
    recorded_at: Timestamp

    def __post_init__(self) -> None:
        if not self.id.strip():
            raise ValueError("Accounting correction id must be non-empty")
        if not self.reason.strip():
            raise ValueError("Accounting correction reason must be non-empty")
        if (
            self.original_trade.id != self.target_trade_id
            or self.replacement_trade.id != self.target_trade_id
        ):
            raise ValueError("Correction trades must retain the target trade id")
        original_identity = (
            self.original_trade.venue_id,
            self.original_trade.portfolio_id or STRATEGY_PORTFOLIO_ID,
            self.original_trade.contract_id,
        )
        replacement_identity = (
            self.replacement_trade.venue_id,
            self.replacement_trade.portfolio_id or STRATEGY_PORTFOLIO_ID,
            self.replacement_trade.contract_id,
        )
        position_identity = (
            self.resulting_position.venue_id,
            self.resulting_position.portfolio_id,
            self.resulting_position.contract_id,
        )
        if (
            original_identity != replacement_identity
            or replacement_identity != position_identity
        ):
            raise ValueError("Accounting corrections cannot move trades between positions")


@dataclass(frozen=True, slots=True)
class CashMovement:
    """Record cash entering, leaving, or moving between venue portfolios.

    Attributes
    ----------
    id
        Stable replay identity.
    kind
        Deposit, withdrawal, or internal transfer.
    amount
        Strictly positive amount moved, excluding fees.
    occurred_at
        Economic timestamp reported by the source.
    fee
        Optional settlement-currency movement fee.

    Invariants
    ----------
    - Deposits have only a destination and withdrawals only a source.
    - Transfers have distinct source and destination identities.
    - Cash flows never become trading PnL.
    """

    id: str
    kind: CashMovementKind
    amount: Money
    occurred_at: Timestamp
    source_venue_id: VenueID | None = None
    source_portfolio_id: PortfolioID | None = None
    destination_venue_id: VenueID | None = None
    destination_portfolio_id: PortfolioID | None = None
    fee: Money | None = None
    external_reference: str | None = None

    def __post_init__(self) -> None:
        if not self.id.strip():
            raise ValueError("Cash movement id must be non-empty")
        if self.amount.amount <= 0:
            raise ValueError("Cash movement amount must be positive")
        source = (self.source_venue_id, self.source_portfolio_id)
        destination = (self.destination_venue_id, self.destination_portfolio_id)
        has_source = all(value is not None for value in source)
        has_destination = all(value is not None for value in destination)
        if any(value is not None for value in source) != has_source or any(
            value is not None for value in destination
        ) != has_destination:
            raise ValueError("Cash movement venue and portfolio identities are paired")
        if self.kind is CashMovementKind.DEPOSIT and (has_source or not has_destination):
            raise ValueError("Deposits require only a destination")
        if self.kind is CashMovementKind.WITHDRAWAL and (
            not has_source or has_destination
        ):
            raise ValueError("Withdrawals require only a source")
        if self.kind is CashMovementKind.TRANSFER and (
            not has_source or not has_destination or source == destination
        ):
            raise ValueError("Transfers require distinct source and destination")
        if self.fee is not None and self.fee.amount < 0:
            raise ValueError("Cash movement fee cannot be negative")
        if self.external_reference is not None and not self.external_reference.strip():
            raise ValueError("Cash movement reference cannot be blank")

    def amount_for(self, venue_id: VenueID, portfolio_id: PortfolioID) -> Decimal:
        """Return the signed principal cash flow for one portfolio identity."""
        identity = (venue_id, portfolio_id)
        value = Decimal("0")
        if (self.destination_venue_id, self.destination_portfolio_id) == identity:
            value += self.amount.amount
        if (self.source_venue_id, self.source_portfolio_id) == identity:
            value -= self.amount.amount
        return value


@dataclass(frozen=True, slots=True)
class Portfolio:
    """Aggregate positions and available cash for one venue portfolio.

    Attributes
    ----------
    venue_id : VenueID
        Venue that hosts the portfolio balances.
    portfolio_id : PortfolioID
        Stable portfolio identity.
    positions : tuple of Position
        Current exposures owned by the portfolio.
    available_funds : Money, optional
        Spendable cash balance in the settlement currency.
    realized_pnl : Decimal
        Sum of cumulative gross realized PnL from the positions.
    fees : Money, optional
        Sum of known settlement-cost fees. ``None`` means at least one fee is
        unknown or currencies could not be combined.
    quality_flags : tuple of str
        Data-quality limitations propagated from positions.

    Notes
    -----
    - A portfolio is scoped to exactly one venue and portfolio identifier.
    - It is a derived aggregate; trades and inventory operations remain the
      source of truth.
    """

    venue_id: VenueID
    portfolio_id: PortfolioID
    positions: tuple[Position, ...] = ()
    cash_movements: tuple[CashMovement, ...] = ()
    available_funds: Money | None = None
    realized_pnl: Decimal = Decimal("0")
    fees: Money | None = None
    quality_flags: tuple[str, ...] = ()

    @classmethod
    def from_positions(
        cls,
        positions: tuple[Position, ...] | list[Position],
        *,
        available_funds: Money | None = None,
    ) -> "Portfolio":
        """Build one venue-scoped portfolio from derived positions.

        Parameters
        ----------
        positions : tuple of Position or list of Position
            Positions belonging to one venue and portfolio.
        available_funds : Money, optional
            Latest available cash balance, when known.

        Returns
        -------
        Portfolio
            Aggregate portfolio snapshot.

        Raises
        ------
        ValueError
            If positions span multiple venue or portfolio identities.
        """
        values = tuple(positions)
        if not values:
            raise ValueError("Portfolio requires at least one position")
        identity = (values[0].venue_id, values[0].portfolio_id)
        if any((value.venue_id, value.portfolio_id) != identity for value in values):
            raise ValueError("Portfolio positions must share venue and portfolio")
        flags: list[str] = []
        for value in values:
            for flag in value.quality_flags:
                if flag not in flags:
                    flags.append(flag)
            if value.is_open() and value.current_price is None and "MISSING_MARK" not in flags:
                flags.append("MISSING_MARK")
        fees = (
            None
            if "MISSING_FEES" in flags
            else _sum_fees(tuple(value.fees for value in values), flags)
        )
        return cls(
            venue_id=identity[0],
            portfolio_id=identity[1],
            positions=values,
            available_funds=available_funds,
            realized_pnl=sum((value.realized_pnl for value in values), Decimal("0")),
            fees=fees,
            quality_flags=tuple(flags),
        )

    @classmethod
    def group_by_venue(
        cls,
        positions: tuple[Position, ...] | list[Position],
        cash_movements: tuple[CashMovement, ...] | list[CashMovement] = (),
    ) -> dict[tuple[VenueID, PortfolioID], "Portfolio"]:
        """Build one portfolio for each venue and portfolio identity."""
        grouped: dict[tuple[VenueID, PortfolioID], list[Position]] = {}
        for position in positions:
            grouped.setdefault((position.venue_id, position.portfolio_id), []).append(
                position,
            )
        movements_by_identity: dict[
            tuple[VenueID, PortfolioID], list[CashMovement]
        ] = {}
        for movement in cash_movements:
            for identity in (
                (movement.source_venue_id, movement.source_portfolio_id),
                (movement.destination_venue_id, movement.destination_portfolio_id),
            ):
                if identity[0] is not None and identity[1] is not None:
                    movements_by_identity.setdefault(identity, []).append(movement)
        identities = grouped.keys() | movements_by_identity.keys()
        portfolios: dict[tuple[VenueID, PortfolioID], Portfolio] = {}
        for identity in identities:
            values = tuple(grouped.get(identity, ()))
            movements = tuple(movements_by_identity.get(identity, ()))
            if values:
                portfolio = cls.from_positions(values)
                flags = list(portfolio.quality_flags)
                fees = _sum_fees(
                    tuple(position.fees for position in values)
                    + tuple(
                        movement.fee
                        for movement in movements
                        if (
                            movement.source_venue_id,
                            movement.source_portfolio_id,
                        ) == identity
                        or (
                            movement.source_venue_id is None
                            and (
                                movement.destination_venue_id,
                                movement.destination_portfolio_id,
                            ) == identity
                        )
                    ),
                    flags,
                )
                if "MISSING_FEES" in flags:
                    fees = None
                portfolios[identity] = replace(
                    portfolio,
                    cash_movements=movements,
                    fees=fees,
                    quality_flags=tuple(flags),
                )
            else:
                flags: list[str] = []
                fees = _sum_fees(
                    tuple(
                        movement.fee
                        for movement in movements
                        if (
                            movement.source_venue_id,
                            movement.source_portfolio_id,
                        ) == identity
                        or (
                            movement.source_venue_id is None
                            and (
                                movement.destination_venue_id,
                                movement.destination_portfolio_id,
                            ) == identity
                        )
                    ),
                    flags,
                )
                if "MISSING_FEES" in flags:
                    fees = None
                portfolios[identity] = cls(
                    venue_id=identity[0],
                    portfolio_id=identity[1],
                    cash_movements=movements,
                    fees=fees,
                    quality_flags=tuple(flags),
                )
        return portfolios

    @property
    def net_cash_flow(self) -> Decimal | None:
        """Return signed deposits, withdrawals, and transfers for this portfolio."""
        relevant = tuple(
            movement
            for movement in self.cash_movements
            if movement.amount_for(self.venue_id, self.portfolio_id) != 0
        )
        if not relevant:
            return None
        currency = relevant[0].amount.currency
        if any(movement.amount.currency != currency for movement in relevant):
            return None
        return sum(
            (
                movement.amount_for(self.venue_id, self.portfolio_id)
                for movement in relevant
            ),
            Decimal("0"),
        )

    @property
    def cash_flow_currency(self) -> Currency | None:
        """Return the common cash-flow currency, or ``None`` when mixed."""
        currencies = {
            movement.amount.currency
            for movement in self.cash_movements
            if movement.amount_for(self.venue_id, self.portfolio_id) != 0
        }
        return next(iter(currencies)) if len(currencies) == 1 else None

    @property
    def unrealized_pnl(self) -> Decimal | None:
        """
        Return the sum of unrealized PnL across all positions.

        Returns
        -------
        Decimal or None
            Aggregate, or ``None`` when any open position lacks a mark.
        """
        values = tuple(position.unrealized_pnl for position in self.positions)
        if any(value is None for value in values):
            return None
        return sum(values, Decimal("0"))

    @property
    def total_pnl(self) -> Decimal | None:
        """Return realized plus marked unrealized PnL less known fees."""
        unrealized = self.unrealized_pnl
        if unrealized is None or self.fees is None or "MISSING_FEES" in self.quality_flags:
            return None
        return self.realized_pnl + unrealized - self.fees.amount

    @property
    def total_value(self) -> Decimal | None:
        """
        Return available funds plus the sum of position current values.

        Returns
        -------
        Decimal or None
            Cash balance plus marked position values, when all are known.
        """
        if self.available_funds is None:
            return None
        values = tuple(position.current_value for position in self.positions)
        if any(value is None for value in values):
            return None
        return self.available_funds.amount + sum(values, Decimal("0"))


def _sum_fees(values: tuple[Money | None, ...], flags: list[str]) -> Money | None:
    """Sum settlement fees without treating unknown values as zero."""
    known = tuple(value for value in values if value is not None)
    if len(known) != len(values):
        if "MISSING_FEES" not in flags:
            flags.append("MISSING_FEES")
    if not known:
        return None
    currency = known[0].currency
    if any(value.currency != currency for value in known):
        if "INCONSISTENT_FEE_CURRENCY" not in flags:
            flags.append("INCONSISTENT_FEE_CURRENCY")
        return None
    return Money(sum((value.amount for value in known), Decimal("0")), currency)


@dataclass(frozen=True, slots=True)
class ArbitrageExecutionJournal:
    """Represent durable safety state for one two-leg execution.

    Attributes
    ----------
    id : str
        Stable execution identity shared by all derived events and commands.
    leg1_decision : OrderBookDecisionSnapshot, optional
        Executable depth observed when the primary command was created.
    leg2_decision : OrderBookDecisionSnapshot, optional
        Executable depth observed when the hedge command was created.
    resolution_method : str, optional
        Explicit operator closure: ``manual_sale`` or ``settlement``. Older
        records and automatic executions leave this unset.

    Invariants
    ----------
    - The identifier is non-empty.
    - Both requested leg quantities are positive.

    Notes
    -----
    - Instances are immutable; state transitions replace the complete record.
    """

    id: str
    status: ArbitrageExecutionStatus
    leg1_venue_id: VenueID
    leg1_contract_id: ContractID
    leg1_side: OrderSide
    leg1_quantity: Quantity
    leg1_limit_price: Price
    leg1_client_order_id: ClientOrderID
    leg2_venue_id: VenueID
    leg2_contract_id: ContractID
    leg2_side: OrderSide
    leg2_quantity: Quantity
    leg2_limit_price: Price
    leg2_client_order_id: ClientOrderID
    created_at: Timestamp
    updated_at: Timestamp
    portfolio_id: PortfolioID | None = None
    strategy_id: StrategyID | None = None
    leg1_order_id: OrderID | None = None
    leg1_filled_quantity: Quantity = Quantity(Decimal("0"))
    leg2_order_id: OrderID | None = None
    leg2_filled_quantity: Quantity = Quantity(Decimal("0"))
    residual_quantity: Quantity = Quantity(Decimal("0"))
    last_error: str | None = None
    leg1_decision: OrderBookDecisionSnapshot | None = None
    leg2_decision: OrderBookDecisionSnapshot | None = None
    resolution_method: str | None = None

    def __post_init__(self) -> None:
        if not self.id.strip():
            raise ValueError("ArbitrageExecutionJournal id must be non-empty")
        if self.leg1_quantity.value <= 0 or self.leg2_quantity.value <= 0:
            raise ValueError("Arbitrage execution quantities must be positive")
        if self.resolution_method not in {None, "manual_sale", "settlement"}:
            raise ValueError("Unsupported execution resolution method")


@dataclass(frozen=True, slots=True)
class ExposureRecovery:
    """Persist one bounded order intended to neutralize residual exposure.

    Attributes
    ----------
    id : str
        Stable recovery identifier shared with the parent execution across its
        bounded order attempts.
    execution_id : str, optional
        Parent arbitrage execution. ``None`` identifies a legacy manual record.
    route : RecoveryRoute, optional
        Complete-leg or unwind route chosen from current executable depth.
    source_fee : Money, optional
        Original fill fee allocated to the residual target quantity.
    estimated_recovery_fee : Money, optional
        Fee estimated for the complete requested recovery quantity.
    actual_net_result : Decimal, optional
        Realized result after known source and recovery fees.
    attempts : int
        Monotonically increasing planned-order count used for unique client IDs.
    local_rejections : int
        Plans definitively rejected before submission. Journal replay preserves
        this counter separately from the bounded venue-attempt budget.

    Invariants
    ----------
    - Actionable recoveries have positive requested quantity.
    - Filled quantity and attempt count are non-negative.
    - Local rejections cannot exceed the planned-order count.
    - Automatic recoveries contain their source and estimated economics.
    """

    id: str
    venue_id: VenueID
    contract_id: ContractID
    side: OrderSide
    quantity: Quantity
    limit_price: Price
    status: RecoveryStatus
    attempts: int
    created_at: Timestamp
    updated_at: Timestamp
    portfolio_id: PortfolioID | None = None
    strategy_id: StrategyID | None = None
    client_order_id: ClientOrderID | None = None
    order_id: OrderID | None = None
    last_error: str | None = None
    execution_id: str | None = None
    route: RecoveryRoute | None = None
    source_contract_id: ContractID | None = None
    source_side: OrderSide | None = None
    source_price: Price | None = None
    source_fee: Money | None = None
    estimated_vwap: Price | None = None
    estimated_recovery_fee: Money | None = None
    estimated_gross_result: Decimal | None = None
    estimated_net_result: Decimal | None = None
    filled_quantity: Quantity = Quantity(Decimal("0"))
    average_price: Price | None = None
    recovery_fee: Money | None = None
    actual_gross_result: Decimal | None = None
    actual_net_result: Decimal | None = None
    local_rejections: int = 0

    def __post_init__(self) -> None:
        if not self.id.strip():
            raise ValueError("ExposureRecovery id must be non-empty")
        if (
            self.quantity.value <= 0
            and self.status is not RecoveryStatus.NEEDS_REVIEW
        ):
            raise ValueError("Actionable ExposureRecovery quantity must be positive")
        if self.attempts < 0:
            raise ValueError("ExposureRecovery attempts must be non-negative")
        if not 0 <= self.local_rejections <= self.attempts:
            raise ValueError("ExposureRecovery local rejections must be between zero and attempts")
        if self.filled_quantity.value < 0:
            raise ValueError("ExposureRecovery filled quantity must be non-negative")
        if self.execution_id is not None and any(
            value is None
            for value in (
                self.route,
                self.source_contract_id,
                self.source_side,
                self.source_price,
                self.source_fee,
                self.estimated_vwap,
                self.estimated_recovery_fee,
                self.estimated_gross_result,
                self.estimated_net_result,
                self.client_order_id,
            )
        ):
            raise ValueError(
                "Automatic ExposureRecovery requires source and estimated economics",
            )

    def remaining_quantity(self) -> Quantity:
        """Return requested quantity not filled by the recovery order."""
        return Quantity(
            max(Decimal("0"), self.quantity.value - self.filled_quantity.value),
        )
