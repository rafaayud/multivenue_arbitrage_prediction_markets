"""Choose a fee-aware order for neutralizing residual arbitrage exposure.

Responsibilities
----------------
- Quote complete-leg and unwind routes from current normalized order books.
- Select one bounded venue-neutral recovery decision for the trading engine.

Notes
-----
- The service performs no I/O, persistence, or order submission.
"""

from collections.abc import Mapping
from decimal import ROUND_FLOOR, Decimal

from prediction_markets.domain.contracts.entities import BinaryContract
from prediction_markets.domain.orderbook.entities import OrderBook
from prediction_markets.domain.orderbook.value_objects import OrderBookLevel
from prediction_markets.domain.ports.taker_fees import TakerFeeCalculatorPort
from prediction_markets.domain.shared.value_objects import (
    ContractID,
    Money,
    Price,
    Quantity,
    VenueID,
)
from prediction_markets.domain.trading.enums import OrderSide, RecoveryRoute
from prediction_markets.domain.trading.value_objects import (
    RecoveryDecision,
    TradingFee,
    evaluate_recovery_economics,
    recovery_order_side,
)


class RecoveryDecisionService:
    """Select the safest best-priced recovery candidate without side effects.

    Notes
    -----
    - Fresh executable routes are prioritized before quantity and net result.
    - A candidate whose total net result breaches ``max_loss`` is rejected.
    - Order-book freshness remains an engine admission responsibility.
    """

    def __init__(
        self,
        fees_by_venue: Mapping[VenueID, TakerFeeCalculatorPort]) -> None:
        """
        Parameters
        ----------
        fees_by_venue : Mapping[VenueID, TakerFeeCalculatorPort]
            Prepared synchronous taker-fee calculators keyed by venue.
        """
        self._fees = dict(fees_by_venue)

    def choose(
        self,
        *,
        source_side: OrderSide,
        source_price: Price,
        source_fee: Money,
        residual_quantity: Quantity,
        excess_contract: BinaryContract,
        excess_book: OrderBook,
        missing_contract: BinaryContract,
        missing_book: OrderBook,
        max_loss: Decimal,
        min_buy_notional: Decimal = Decimal("0"),
        fresh_contract_ids: frozenset[ContractID] | None = None,
    ) -> RecoveryDecision | None:
        """Choose between completing the missing leg and unwinding the excess.

        Parameters
        ----------
        source_side : OrderSide
            Side of the original unmatched fill.
        source_price : Price
            Actual average price of the unmatched fill.
        source_fee : Money
            Actual or conservatively estimated source fee allocated to the full
            residual quantity in settlement currency.
        residual_quantity : Quantity
            Positive unmatched quantity requiring neutralization.
        excess_contract : BinaryContract
            Contract holding the unmatched fill.
        excess_book : OrderBook
            Current book used to quote an opposite-side unwind.
        missing_contract : BinaryContract
            Complementary contract whose original order underfilled.
        missing_book : OrderBook
            Current book used to quote completion on the original side.
        max_loss : Decimal
            Maximum accepted negative net result in settlement currency.
        min_buy_notional : Decimal, default=0
            Minimum limit-price-times-quantity accepted for a BUY candidate.
            Expected VWAP remains the basis for fees and economic loss limits.
        fresh_contract_ids
            Contracts passing the caller's current freshness guards. Prefer
            those routes when admissible. If none qualify, retain the best
            bounded quote for the dispatcher's existing fresh-book wait.
            ``None`` preserves price-and-quantity selection without a preference.

        Returns
        -------
        RecoveryDecision | None
            Best fresh candidate by quantity and net result, or the best
            bounded quote when neither route is fresh. ``None`` means neither
            route satisfies liquidity and loss limits.

        Raises
        ------
        ValueError
            If quantity is zero or a supplied risk limit is negative.
        """
        if residual_quantity.value <= 0:
            raise ValueError("Recovery residual quantity must be positive")
        if max_loss < 0 or min_buy_notional < 0:
            raise ValueError("Recovery risk limits must be non-negative")

        candidates = tuple(
            candidate
            for route, contract, book in (
                (
                    RecoveryRoute.COMPLETE_MISSING_LEG,
                    missing_contract,
                    missing_book,
                ),
                (
                    RecoveryRoute.UNWIND_EXCESS,
                    excess_contract,
                    excess_book,
                ),
            )
            if (
                candidate := self._candidate(
                    source_side=source_side,
                    source_price=source_price,
                    source_fee=source_fee,
                    residual_quantity=residual_quantity,
                    route=route,
                    contract=contract,
                    book=book,
                    min_buy_notional=min_buy_notional,
                )
            )
            is not None
            and candidate.economics.net_result is not None
            and candidate.economics.net_result >= -max_loss
        )
        return max(
            candidates,
            key=lambda candidate: (
                fresh_contract_ids is not None and candidate.contract_id in fresh_contract_ids,
                candidate.quantity.value,
                candidate.economics.net_result,
            ),
            default=None,
        )

    def _candidate(
        self,
        *,
        source_side: OrderSide,
        source_price: Price,
        source_fee: Money,
        residual_quantity: Quantity,
        route: RecoveryRoute,
        contract: BinaryContract,
        book: OrderBook,
        min_buy_notional: Decimal) -> RecoveryDecision | None:
        """Build one route from executable depth and level-specific fees.

        Parameters
        ----------
        source_side : OrderSide
            Side of the unmatched source fill.
        source_price : Price
            Average execution price of the unmatched source fill.
        source_fee : Money
            Fee allocated to the complete residual before route sizing.
        residual_quantity : Quantity
            Maximum quantity available for neutralization.
        route : RecoveryRoute
            Economic route being quoted.
        contract : BinaryContract
            Contract traded by the recovery route.
        book : OrderBook
            Current normalized depth for the recovery contract.
        min_buy_notional : Decimal
            Minimum BUY limit-price notional in settlement units.

        Returns
        -------
        RecoveryDecision | None
            Executable candidate, or ``None`` when depth or venue constraints
            prevent the route.
        """
        calculator = self._fees.get(contract.venue_id)
        if calculator is None:
            return None
        side = recovery_order_side(source_side, route)
        quote = self._quote(
            contract,
            book,
            side,
            residual_quantity,
            calculator,
        )
        if quote is None:
            return None
        quantity, vwap, limit_price, recovery_fee = quote
        if side is OrderSide.BUY and limit_price.value * quantity.value < min_buy_notional:
            return None
        allocated_source_fee = Money(
            source_fee.amount * quantity.value / residual_quantity.value,
            source_fee.currency,
        )
        economics = evaluate_recovery_economics(
            source_side=source_side,
            source_price=source_price,
            quantity=quantity,
            route=route,
            recovery_price=vwap,
            source_fee=allocated_source_fee,
            recovery_fee=recovery_fee.settlement_cost,
        )
        return RecoveryDecision(
            venue_id=contract.venue_id,
            contract_id=contract.id,
            limit_price=limit_price,
            estimated_vwap=vwap,
            estimated_fee=recovery_fee,
            economics=economics,
        )

    def _quote(
        self,
        contract: BinaryContract,
        book: OrderBook,
        side: OrderSide,
        residual_quantity: Quantity,
        calculator: TakerFeeCalculatorPort) -> tuple[Quantity, Price, Price, TradingFee] | None:
        """Quote maximal executable quantity and aggregate level fees.

        Parameters
        ----------
        contract : BinaryContract
            Contract providing lot and minimum-order constraints.
        book : OrderBook
            Normalized depth to consume at taker prices.
        side : OrderSide
            Recovery order direction.
        residual_quantity : Quantity
            Maximum quantity to quote.
        calculator : TakerFeeCalculatorPort
            Prepared venue fee calculator.

        Returns
        -------
        tuple[Quantity, Price, Price, TradingFee] | None
            Executable quantity, VWAP, worst IOC limit, and aggregate fee, or
            ``None`` when no valid quantity can be submitted.
        """
        levels = sorted(
            book.asks if side is OrderSide.BUY else book.bids,
            key=lambda level: level.price.value,
            reverse=side is OrderSide.SELL,
        )
        available = min(
            residual_quantity.value,
            sum(
                (
                    level.quantity.value
                    for level in levels
                    if level.quantity.value > 0
                ),
                Decimal("0"),
            ),
        )
        if contract.lot_size is not None:
            step = contract.lot_size.value
            available = (
                available / step
            ).to_integral_value(rounding=ROUND_FLOOR) * step
        if available <= 0 or (
            contract.minimum_order_size is not None
            and available < contract.minimum_order_size.value
        ):
            return None

        quantity = Quantity(available)
        remaining = available
        taken_levels: list[OrderBookLevel] = []
        fees: list[TradingFee] = []
        notional = Decimal("0")
        for level in levels:
            taken = min(remaining, level.quantity.value)
            if taken <= 0:
                continue
            level_quantity = Quantity(taken)
            taken_levels.append(OrderBookLevel(level.price, level_quantity))
            notional += level.price.value * taken
            fees.append(
                calculator.calculate(
                    contract.id,
                    level.price,
                    level_quantity,
                    side,
                ),
            )
            remaining -= taken
            if remaining <= 0:
                break
        if remaining > 0 or not taken_levels:
            return None
        return (
            quantity,
            Price(notional / available),
            taken_levels[-1].price,
            _sum_fees(tuple(fees)),
        )


def _sum_fees(fees: tuple[TradingFee, ...]) -> TradingFee:
    """Aggregate fees whose native and settlement currencies agree.

    Parameters
    ----------
    fees : tuple[TradingFee, ...]
        Non-empty fees calculated for the consumed depth levels.

    Returns
    -------
    TradingFee
        Sum in the shared native and settlement currencies.

    Raises
    ------
    ValueError
        If native or settlement currencies differ between levels.
    """
    charged_currencies = {fee.charged.currency for fee in fees}
    settlement_currencies = {fee.settlement_cost.currency for fee in fees}
    if len(charged_currencies) != 1 or len(settlement_currencies) != 1:
        raise ValueError("Recovery quote fees must use consistent currencies")
    return TradingFee(
        charged=Money(
            sum((fee.charged.amount for fee in fees), Decimal("0")),
            fees[0].charged.currency,
        ),
        settlement_cost=Money(
            sum((fee.settlement_cost.amount for fee in fees), Decimal("0")),
            fees[0].settlement_cost.currency,
        ),
    )
