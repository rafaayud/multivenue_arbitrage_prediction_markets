"""Implement stateless domain decisions for arbitrage.

Responsibilities
----------------
- Apply business rules to domain values and entities.
"""
from abc import ABC, abstractmethod
from dataclasses import dataclass
from decimal import ROUND_CEILING, ROUND_FLOOR, Decimal
import logging
from typing import Literal

from prediction_markets.domain.arbitrage.value_objects import ArbitrageOpportunity
from prediction_markets.domain.contracts.value_objects import TickSize
from prediction_markets.domain.orderbook.entities import OrderBook
from prediction_markets.domain.orderbook.value_objects import OrderBookLevel
from prediction_markets.domain.ports.taker_fees import TakerFeeCalculatorPort
from prediction_markets.domain.shared.value_objects import (
    ContractID,
    Price,
    Quantity,
    Timestamp,
)
from prediction_markets.domain.trading.enums import OrderSide
from prediction_markets.domain.trading.value_objects import TradingFee

from prediction_markets.utils.decorators.logger import logged


class ArbitrageDetectionService(ABC):
    """Abstract base class for arbitrage detection """

    def __init__(
        self,
        *,
        max_skew_ms: int = 80,
        cost_buffer: Decimal = Decimal("0"),
        min_edge: Decimal = Decimal("0"),
        left_taker_fees: TakerFeeCalculatorPort | None = None,
        right_taker_fees: TakerFeeCalculatorPort | None = None) -> None:
        """
        Initialize the arbitrage detection service.

        Parameters
        ----------
        max_skew_ms
            The maximum skew between the order books in milliseconds.
        cost_buffer
            The cost buffer in decimal.
        min_edge
            The minimum edge in decimal.
        """
        if max_skew_ms < 0:
            raise ValueError("max_skew_ms must be non-negative")
        if cost_buffer < 0:
            raise ValueError("cost_buffer must be non-negative")
        if min_edge < 0:
            raise ValueError("min_edge must be non-negative")

        self._max_skew_ns = max_skew_ms * 1_000_000
        self._cost_buffer = cost_buffer
        self._min_edge = min_edge
        self._left_taker_fees = left_taker_fees
        self._right_taker_fees = right_taker_fees

    @abstractmethod
    def detect(
        self,
        left_contract_id: ContractID,
        right_contract_id: ContractID,
        left: OrderBook,
        right: OrderBook,
        left_tick_size: TickSize | None = None,
        right_tick_size: TickSize | None = None,
    ) -> ArbitrageOpportunity | None:
        """
        Detect arbitrage opportunities between two order books.

        Parameters
        ----------
        left_contract_id
            The ID of the left contract.
        right_contract_id
            The ID of the right contract.
        left
            The left order book.
        right
            The right order book.
        left_tick_size
            Current executable tick for the left contract, when known.
        right_tick_size
            Current executable tick for the right contract, when known.

        Returns
        -------
        ArbitrageOpportunity | None
            The arbitrage opportunity if it exists, otherwise None.
        """


    def _comparable_skew(self, left: OrderBook, right: OrderBook) -> int | None:
        """Return inter-book skew when the configured freshness policy passes."""
        if left.received_at_ns is None or right.received_at_ns is None:
            return None

        skew_ns = abs(left.received_at_ns - right.received_at_ns)
        return skew_ns if skew_ns <= self._max_skew_ns else None

    def _opportunity(
        self,
        left_contract_id: ContractID,
        right_contract_id: ContractID,
        side: OrderSide,
        left_level: OrderBookLevel,
        right_level: OrderBookLevel,
        gross_edge: Decimal,
        skew_ns: int) -> ArbitrageOpportunity | None:

        """Apply liquidity, fee, buffer, and minimum-edge rules to a candidate pair."""
        quantity = Quantity(
            min(left_level.quantity.value, right_level.quantity.value)
        )
        if quantity.value == 0:
            return None

        fees = tuple(
            calculator.calculate(contract_id, level.price, quantity, side)
            for calculator, contract_id, level in (
                (self._left_taker_fees, left_contract_id, left_level),
                (self._right_taker_fees, right_contract_id, right_level),
            )
            if calculator is not None
        )
        _require_same_settlement_currency(fees)
        total_fees = sum(
            (fee.settlement_cost.amount for fee in fees),
            Decimal("0"),
        )
        fee_per_contract = total_fees / quantity.value
        net_edge = gross_edge - fee_per_contract - self._cost_buffer
        if net_edge <= self._min_edge:
            return None

        return ArbitrageOpportunity(
            left_contract_id=left_contract_id,
            right_contract_id=right_contract_id,
            side=side,
            left_level=left_level,
            right_level=right_level,
            quantity=quantity,
            gross_edge=gross_edge,
            net_edge=net_edge,
            skew_ns=skew_ns,
            detected_at=Timestamp.now(),
            fee_per_contract=fee_per_contract,
            total_fees=total_fees,
        )


class LongArbitrageDetectionService(ArbitrageDetectionService):
    """Detect buying complementary contracts for less than their unit payout."""

    @logged(level=logging.DEBUG)
    def detect(
        self,
        left_contract_id: ContractID,
        right_contract_id: ContractID,
        left: OrderBook,
        right: OrderBook,
        left_tick_size: TickSize | None = None,
        right_tick_size: TickSize | None = None,
    ) -> ArbitrageOpportunity | None:
        """Detect a long arbitrage from the best asks of complementary contracts.

        Returns
        -------
        ArbitrageOpportunity | None
            An opportunity when ``1 - left_ask - right_ask`` exceeds fees,
            buffer, and the minimum edge; otherwise `None`.

        Notes
        -----
        - Both books must be fresh, within the skew limit, and have ask liquidity.
        """
        skew_ns = self._comparable_skew(left, right)
        if skew_ns is None:
            return None

        left_ask = left.best_ask()
        right_ask = right.best_ask()
        if left_ask is None or right_ask is None:
            return None
        left_ask = _tick_aligned_level(left_ask, left_tick_size, OrderSide.BUY)
        right_ask = _tick_aligned_level(right_ask, right_tick_size, OrderSide.BUY)
        if left_ask is None or right_ask is None:
            return None

        gross_edge = Decimal("1") - left_ask.price.value - right_ask.price.value
        return self._opportunity(
            left_contract_id,
            right_contract_id,
            OrderSide.BUY,
            left_ask,
            right_ask,
            gross_edge,
            skew_ns,
        )


class ShortArbitrageDetectionService(ArbitrageDetectionService):
    """Detect selling complementary contracts for more than their unit payout."""

    @logged(level=logging.DEBUG)
    def detect(
        self,
        left_contract_id: ContractID,
        right_contract_id: ContractID,
        left: OrderBook,
        right: OrderBook,
        left_tick_size: TickSize | None = None,
        right_tick_size: TickSize | None = None,
    ) -> ArbitrageOpportunity | None:
        """Detect a short arbitrage from the best bids of complementary contracts.

        Returns
        -------
        ArbitrageOpportunity | None
            An opportunity when ``left_bid + right_bid - 1`` exceeds fees,
            buffer, and the minimum edge; otherwise `None`.

        Notes
        -----
        - Both books must be fresh, within the skew limit, and have bid liquidity.
        """
        skew_ns = self._comparable_skew(left, right)
        if skew_ns is None:
            return None

        left_bid = left.best_bid()
        right_bid = right.best_bid()
        if left_bid is None or right_bid is None:
            return None
        left_bid = _tick_aligned_level(left_bid, left_tick_size, OrderSide.SELL)
        right_bid = _tick_aligned_level(right_bid, right_tick_size, OrderSide.SELL)
        if left_bid is None or right_bid is None:
            return None

        gross_edge = left_bid.price.value + right_bid.price.value - Decimal("1")
        return self._opportunity(
            left_contract_id,
            right_contract_id,
            OrderSide.SELL,
            left_bid,
            right_bid,
            gross_edge,
            skew_ns,
        )

class NestedStrikeArbitrageDetector(ArbitrageDetectionService):
    """
    Detect arbitrage opportunities between logically nested strike markets.

    For two markets where the harder condition implies the easier condition
    (A ⇒ B), the strategy constructs the position BUY NO(A) + BUY YES(B).

    Because the state A=True and B=False is logically impossible, this
    portfolio has a guaranteed minimum payout of one unit per matched share
    pair at resolution.

    The left contract must represent NO(A), where A is the harder strike, and
    the right contract must represent YES(B), where B is the easier strike.

    An arbitrage opportunity exists when the all-in acquisition cost of both
    legs is lower than their guaranteed minimum payout, after accounting for
    fees, execution buffers, and the configured minimum edge.

    This detector assumes that the logical implication A ⇒ B has already been
    validated by the caller. It is responsible only for detecting and valuing
    the price discrepancy; it does not infer strike relationships or execute
    orders.
    """

    @logged(level=logging.DEBUG)
    def detect(
        self,
        left_contract_id: ContractID,
        right_contract_id: ContractID,
        left: OrderBook,
        right: OrderBook,
        left_tick_size: TickSize | None = None,
        right_tick_size: TickSize | None = None,
    ) -> ArbitrageOpportunity | None:
        """
        Detect nested-strike arbitrage from the best asks of both legs.

        Parameters
        ----------
        left_contract_id
            Contract ID of NO(A), where A is the harder condition.
        right_contract_id
            Contract ID of YES(B), where B is the easier condition.
        left
            Order book for NO(A).
        right
            Order book for YES(B).
        left_tick_size
            Optional tick size for the NO(A) contract.
        right_tick_size
            Optional tick size for the YES(B) contract.

        Returns
        -------
        ArbitrageOpportunity | None
            An opportunity when::

                1 - ask_NO(A) - ask_YES(B)

            exceeds fees, execution buffer, and the configured minimum edge;
            otherwise ``None``.

        Notes
        -----
        - The caller must guarantee the logical implication ``A ⇒ B``.
        - Both legs are BUY orders.
        - Both books must be fresh and within the configured skew limit.
        - Both books must contain executable ask liquidity.
        - Equal quantities must ultimately be acquired on both legs for the
          guaranteed minimum payout to apply.
        - This method detects the top-of-book opportunity. Executable sizing
          must account for order-book depth/VWAP for the target quantity.
        """
        skew_ns = self._comparable_skew(left, right)
        if skew_ns is None:
            return None

        harder_no_ask = left.best_ask()
        easier_yes_ask = right.best_ask()

        if harder_no_ask is None or easier_yes_ask is None:
            return None

        harder_no_ask = _tick_aligned_level(
            harder_no_ask,
            left_tick_size,
            OrderSide.BUY,
        )
        easier_yes_ask = _tick_aligned_level(
            easier_yes_ask,
            right_tick_size,
            OrderSide.BUY,
        )

        if harder_no_ask is None or easier_yes_ask is None:
            return None

        gross_edge = (
            Decimal("1")
            - harder_no_ask.price.value
            - easier_yes_ask.price.value
        )

        return self._opportunity(
            left_contract_id,
            right_contract_id,
            OrderSide.BUY,
            harder_no_ask,
            easier_yes_ask,
            gross_edge,
            skew_ns,
        )



@dataclass(frozen=True, slots=True)
class ArbitrageLegPlan:
    """Describe one planned leg before venue submission.

    Attributes
    ----------
    contract_id : ContractID
        Contract to trade on this leg.
    side : OrderSide
        Buy for long arbitrage, sell for short arbitrage.
    quantity : Quantity
        Target size for this leg.
    limit_price : Price
        Limit derived from the executable book used for planning.
    role : Literal["primary", "hedge"]
        Stable leg label; both live legs are submitted concurrently by the
        execution pipeline.
    """

    contract_id: ContractID
    side: OrderSide
    quantity: Quantity
    limit_price: Price
    role: Literal["primary", "hedge"]


@dataclass(frozen=True, slots=True)
class ArbitragePlan:
    """Capture the domain decision for one two-leg arbitrage.

    Attributes
    ----------
    side : OrderSide
        Shared direction of both legs.
    quantity : Quantity
        Intended size before the primary fill is known.
    legs : tuple[ArbitrageLegPlan, ArbitrageLegPlan]
        Ordered as (primary, hedge).
    net_edge : Decimal
        Detected net edge per contract after fees and buffers at detection time.
    fee_per_contract : Decimal
        Combined taker fee per contract observed during detection.
    """

    side: OrderSide
    quantity: Quantity
    legs: tuple[ArbitrageLegPlan, ArbitrageLegPlan]
    net_edge: Decimal
    fee_per_contract: Decimal


class ArbitragePlanningService:
    """Decide the shared size and stable leg labels without submitting orders.

    Notes
    -----
    - Primary leg is the less liquid book level.
    - Both limits come from the fee-aware opportunity at detection time.
    """

    @logged(level=logging.DEBUG)
    def plan(
        self,
        opportunity: ArbitrageOpportunity,
    ) -> ArbitragePlan | None:
        """
        Build an entry plan from a detected opportunity.

        Parameters
        ----------
        opportunity : ArbitrageOpportunity
            Detected two-leg edge with book levels and fee estimates.
        Returns
        -------
        ArbitragePlan | None
            Plan ordered as primary then hedge using observed book prices, or
            `None` when the executable size is zero.
        """
        qty = opportunity.quantity.value
        if qty <= 0:
            return None
        quantity = Quantity(qty)

        side = opportunity.side
        left_first = (
            opportunity.left_level.quantity <= opportunity.right_level.quantity
        )
        first_level, second_level = (
            (opportunity.left_level, opportunity.right_level)
            if left_first
            else (opportunity.right_level, opportunity.left_level)
        )
        first_id, second_id = (
            (opportunity.left_contract_id, opportunity.right_contract_id)
            if left_first
            else (opportunity.right_contract_id, opportunity.left_contract_id)
        )

        return ArbitragePlan(
            side=side,
            quantity=quantity,
            net_edge=opportunity.net_edge,
            fee_per_contract=opportunity.fee_per_contract,
            legs=(
                ArbitrageLegPlan(
                    first_id,
                    side,
                    quantity,
                    first_level.price,
                    "primary",
                ),
                ArbitrageLegPlan(
                    second_id,
                    side,
                    quantity,
                    second_level.price,
                    "hedge",
                ),
            ),
        )


def _require_same_settlement_currency(fees: tuple[TradingFee, ...]) -> None:
    """Reject fee arithmetic across different settlement units."""
    if len({fee.settlement_cost.currency for fee in fees}) > 1:
        raise ValueError("Fees must use the same settlement currency")


def _tick_aligned_level(
    level: OrderBookLevel,
    tick_size: TickSize | None,
    side: OrderSide,
) -> OrderBookLevel | None:
    """
    Round one executable level to its side-aware venue tick.

    Parameters
    ----------
    level
        Best executable book level.
    tick_size
        Current venue tick, or ``None`` when the venue does not report one.
    side
        BUY rounds up and SELL rounds down.

    Returns
    -------
    OrderBookLevel | None
        Original or rounded level, or ``None`` outside the tradable tick range.
    """
    if tick_size is None:
        return level
    tick = tick_size.value
    price = level.price.value
    if price % tick == 0 and tick <= price <= Decimal("1") - tick:
        return level
    rounded = (price / tick).to_integral_value(
        rounding=ROUND_CEILING if side is OrderSide.BUY else ROUND_FLOOR,
    ) * tick
    if rounded < tick or rounded > Decimal("1") - tick:
        return None
    return OrderBookLevel(Price(rounded), level.quantity)
