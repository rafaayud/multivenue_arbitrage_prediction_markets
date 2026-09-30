"""Verify application-level recovery quoting and route selection."""

from decimal import Decimal

import pytest

from prediction_markets.application.execution.recovery_decision import (
    RecoveryDecisionService,
)
from prediction_markets.domain.contracts.entities import BinaryContract
from prediction_markets.domain.contracts.value_objects import LotSize, TickSize
from prediction_markets.domain.orderbook.entities import OrderBook
from prediction_markets.domain.orderbook.value_objects import OrderBookLevel
from prediction_markets.domain.ports.taker_fees import TakerFeeCalculatorPort
from prediction_markets.domain.shared.value_objects import (
    ContractID,
    Currency,
    MarketID,
    Money,
    OutcomeID,
    Price,
    Quantity,
    VenueID,
)
from prediction_markets.domain.trading.enums import OrderSide, RecoveryRoute
from prediction_markets.domain.trading.value_objects import TradingFee


class _Fees(TakerFeeCalculatorPort):
    """Charge a fixed settlement amount per executed contract."""

    def __init__(self, per_contract: str = "0") -> None:
        self._per_contract = Decimal(per_contract)

    async def prepare(self, contract_ids: tuple[ContractID, ...]) -> None:
        pass

    def calculate(
        self,
        contract_id: ContractID,
        price: Price,
        quantity: Quantity,
        side: OrderSide,
    ) -> TradingFee:
        amount = self._per_contract * quantity.value
        usd = Currency("USD")
        return TradingFee(Money(amount, usd), Money(amount, usd))


def _contract(name: str, venue: str) -> BinaryContract:
    return BinaryContract(
        id=ContractID(name),
        market_id=MarketID(f"market-{name}"),
        outcome_id=OutcomeID(f"outcome-{name}"),
        venue_id=VenueID(venue),
        payout_currency=Currency("USD"),
        tick_size=TickSize(Decimal("0.01")),
        lot_size=LotSize(Decimal("1")),
        minimum_order_size=Quantity(Decimal("1")),
    )


def _book(
    contract: BinaryContract,
    *,
    bids: tuple[tuple[str, str], ...] = (),
    asks: tuple[tuple[str, str], ...] = (),
) -> OrderBook:
    def levels(values: tuple[tuple[str, str], ...]) -> tuple[OrderBookLevel, ...]:
        return tuple(
            OrderBookLevel(Price(Decimal(price)), Quantity(Decimal(quantity)))
            for price, quantity in values
        )

    return OrderBook(
        market_id=contract.market_id,
        outcome_id=contract.outcome_id,
        bids=levels(bids),
        asks=levels(asks),
    )


def test_choose_completes_long_using_depth_vwap_and_aggregated_fees() -> None:
    """Prefer a profitable complete leg and preserve its worst IOC limit."""
    excess = _contract("excess", "LEFT")
    missing = _contract("missing", "RIGHT")
    service = RecoveryDecisionService(
        {VenueID("LEFT"): _Fees("0.01"), VenueID("RIGHT"): _Fees("0.01")},
    )

    decision = service.choose(
        source_side=OrderSide.BUY,
        source_price=Price(Decimal("0.30")),
        source_fee=Money(Decimal("0.05"), Currency("USD")),
        residual_quantity=Quantity(Decimal("5")),
        excess_contract=excess,
        excess_book=_book(excess, bids=(("0.28", "5"),)),
        missing_contract=missing,
        missing_book=_book(missing, asks=(("0.60", "2"), ("0.70", "3"))),
        max_loss=Decimal("1"),
    )

    assert decision is not None
    assert decision.route is RecoveryRoute.COMPLETE_MISSING_LEG
    assert decision.side is OrderSide.BUY
    assert decision.quantity == Quantity(Decimal("5"))
    assert decision.estimated_vwap == Price(Decimal("0.66"))
    assert decision.limit_price == Price(Decimal("0.70"))
    assert decision.estimated_fee.settlement_cost.amount == Decimal("0.05")
    assert decision.economics.net_result == Decimal("0.10")


def test_choose_unwinds_short_when_repurchase_is_cheaper() -> None:
    """Use the opposite side when closing the excess beats another short sale."""
    excess = _contract("excess", "LEFT")
    missing = _contract("missing", "RIGHT")
    service = RecoveryDecisionService(
        {VenueID("LEFT"): _Fees(), VenueID("RIGHT"): _Fees()},
    )

    decision = service.choose(
        source_side=OrderSide.SELL,
        source_price=Price(Decimal("0.80")),
        source_fee=Money(Decimal("0"), Currency("USD")),
        residual_quantity=Quantity(Decimal("5")),
        excess_contract=excess,
        excess_book=_book(excess, asks=(("0.79", "5"),)),
        missing_contract=missing,
        missing_book=_book(missing, bids=(("0.15", "5"),)),
        max_loss=Decimal("1"),
    )

    assert decision is not None
    assert decision.route is RecoveryRoute.UNWIND_EXCESS
    assert decision.side is OrderSide.BUY
    assert decision.economics.net_result == Decimal("0.05")


def test_choose_prioritizes_full_neutralization_within_loss_limit() -> None:
    """Prefer closing all exposure over a more profitable partial completion."""
    excess = _contract("excess", "LEFT")
    missing = _contract("missing", "RIGHT")
    service = RecoveryDecisionService(
        {VenueID("LEFT"): _Fees(), VenueID("RIGHT"): _Fees()},
    )

    decision = service.choose(
        source_side=OrderSide.BUY,
        source_price=Price(Decimal("0.30")),
        source_fee=Money(Decimal("0"), Currency("USD")),
        residual_quantity=Quantity(Decimal("5")),
        excess_contract=excess,
        excess_book=_book(excess, bids=(("0.29", "5"),)),
        missing_contract=missing,
        missing_book=_book(missing, asks=(("0.65", "2"),)),
        max_loss=Decimal("0.10"),
        fresh_contract_ids=frozenset((excess.id, missing.id)),
    )

    assert decision is not None
    assert decision.route is RecoveryRoute.UNWIND_EXCESS
    assert decision.quantity == Quantity(Decimal("5"))
    assert decision.economics.net_result == Decimal("-0.05")


def test_choose_returns_none_when_every_route_breaches_loss_limit() -> None:
    """Reject automatic recovery when both maximal quotes exceed the cap."""
    excess = _contract("excess", "LEFT")
    missing = _contract("missing", "RIGHT")
    service = RecoveryDecisionService(
        {VenueID("LEFT"): _Fees(), VenueID("RIGHT"): _Fees()},
    )

    decision = service.choose(
        source_side=OrderSide.BUY,
        source_price=Price(Decimal("0.30")),
        source_fee=Money(Decimal("0"), Currency("USD")),
        residual_quantity=Quantity(Decimal("5")),
        excess_contract=excess,
        excess_book=_book(excess, bids=(("0.10", "5"),)),
        missing_contract=missing,
        missing_book=_book(missing, asks=(("0.90", "5"),)),
        max_loss=Decimal("0.10"),
    )

    assert decision is None


@pytest.mark.parametrize("fresh_names,expected_route", [
    (("excess",), RecoveryRoute.UNWIND_EXCESS),
    (("missing",), RecoveryRoute.COMPLETE_MISSING_LEG),
    (("excess", "missing"), RecoveryRoute.COMPLETE_MISSING_LEG),
    ((), RecoveryRoute.COMPLETE_MISSING_LEG),
    (None, RecoveryRoute.COMPLETE_MISSING_LEG),
])
def test_fresh_unwind_beats_stale_profitable_completion(fresh_names, expected_route):
    """Do not spend local retries waiting on stale Predict when an unwind is fresh."""
    excess = _contract("excess", "POLYMARKET")
    missing = _contract("missing", "PREDICT")
    service = RecoveryDecisionService({excess.venue_id: _Fees(), missing.venue_id: _Fees()})
    decision = service.choose(
        source_side=OrderSide.SELL,
        source_price=Price(Decimal("0.30")),
        source_fee=Money(Decimal("0"), Currency("USD")),
        residual_quantity=Quantity(Decimal("5")),
        excess_contract=excess,
        excess_book=_book(excess, asks=(("0.32", "5"),)),
        missing_contract=missing,
        missing_book=_book(missing, bids=(("0.75", "5"),)),
        max_loss=Decimal("1"),
        fresh_contract_ids=(
            frozenset(ContractID(name) for name in fresh_names)
            if fresh_names is not None else None
        ),
    )
    assert decision is not None
    assert decision.route is expected_route
    assert decision.quantity == Quantity(Decimal("5"))


@pytest.mark.parametrize("fresh_asks,max_loss,min_buy_notional", [
    ((("0.90", "5"),), "1", "0"),
    ((("0.32", "5"),), "1", "2"),
    ((("0.10", "1.42"), ("0.19", "45.43")), "1", "1"),
])
def test_freshness_preference_cannot_bypass_recovery_risk_limits(
    fresh_asks, max_loss, min_buy_notional,
):
    """Keep waiting on the missing leg if the fresh unwind fails risk admission."""
    excess = _contract("excess", "POLYMARKET")
    missing = _contract("missing", "PREDICT")
    service = RecoveryDecisionService({excess.venue_id: _Fees(), missing.venue_id: _Fees()})
    decision = service.choose(
        source_side=OrderSide.SELL,
        source_price=Price(Decimal("0.30")),
        source_fee=Money(Decimal("0"), Currency("USD")),
        residual_quantity=Quantity(Decimal("5")),
        excess_contract=excess,
        excess_book=_book(excess, asks=fresh_asks),
        missing_contract=missing,
        missing_book=_book(missing, bids=(("0.75", "5"),)),
        max_loss=Decimal(max_loss),
        min_buy_notional=Decimal(min_buy_notional),
        fresh_contract_ids=frozenset((excess.id,)),
    )
    assert decision is not None
    assert decision.route is RecoveryRoute.COMPLETE_MISSING_LEG


def test_fresh_unwind_minimum_uses_limit_not_discounted_depth_vwap():
    """Admit the captured five-token order signed at one dollar despite price improvement."""
    from py_clob_client_v2.order_builder.builder import OrderBuilder, ROUNDING_CONFIG

    excess = _contract("excess", "POLYMARKET")
    missing = _contract("missing", "PREDICT")
    service = RecoveryDecisionService({excess.venue_id: _Fees(), missing.venue_id: _Fees()})
    decision = service.choose(
        source_side=OrderSide.SELL,
        source_price=Price(Decimal("0.12")),
        source_fee=Money(Decimal("0"), Currency("USD")),
        residual_quantity=Quantity(Decimal("5")),
        excess_contract=excess,
        excess_book=_book(excess, asks=(("0.10", "1.42"), ("0.20", "45.43"))),
        missing_contract=missing,
        missing_book=_book(missing, bids=(("0.85", "10"),)),
        max_loss=Decimal("1"),
        min_buy_notional=Decimal("1"),
        fresh_contract_ids=frozenset((excess.id,)),
    )
    assert decision is not None
    assert decision.route is RecoveryRoute.UNWIND_EXCESS
    assert decision.quantity.value == Decimal("5")
    assert decision.limit_price.value == Decimal("0.20")
    assert decision.limit_price.value * decision.quantity.value == Decimal("1")
    assert decision.estimated_vwap.value == Decimal("0.1716")
    assert decision.economics.net_result == Decimal("-0.258")
    _, maker_amount, taker_amount = OrderBuilder(signer=None).get_order_amounts(
        "BUY", float(decision.quantity.value), float(decision.limit_price.value),
        ROUNDING_CONFIG["0.01"],
    )
    assert maker_amount == 1_000_000
    assert taker_amount == 5_000_000
