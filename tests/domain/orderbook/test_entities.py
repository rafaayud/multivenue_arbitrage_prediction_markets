"""Verify normalized order-book best-level access."""

from decimal import Decimal

from prediction_markets.domain.orderbook.entities import OrderBook
from prediction_markets.domain.orderbook.value_objects import OrderBookLevel
from prediction_markets.domain.shared.value_objects import (
    MarketID,
    OutcomeID,
    Price,
    Quantity,
)


def test_best_levels_are_first_in_a_normalized_book() -> None:
    bid = OrderBookLevel(Price(Decimal("0.45")), Quantity(Decimal("2")))
    lower_bid = OrderBookLevel(Price(Decimal("0.40")), Quantity(Decimal("3")))
    ask = OrderBookLevel(Price(Decimal("0.55")), Quantity(Decimal("4")))
    higher_ask = OrderBookLevel(Price(Decimal("0.60")), Quantity(Decimal("5")))
    book = OrderBook(
        MarketID("market"),
        OutcomeID("yes"),
        (bid, lower_bid),
        (ask, higher_ask),
    )

    assert book.best_bid() is bid
    assert book.best_ask() is ask


def test_empty_book_has_no_best_levels() -> None:
    book = OrderBook(MarketID("market"), OutcomeID("yes"), (), ())

    assert book.best_bid() is None
    assert book.best_ask() is None
