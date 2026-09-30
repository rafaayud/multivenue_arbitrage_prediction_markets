"""Exercise services behavior in the domain market matching layer.

Responsibilities
----------------
- Verify services contracts, edge cases, and failure handling.
"""

from decimal import Decimal
from itertools import permutations

import pytest

from prediction_markets.domain.market_matching.services import MarketMatchingService
from prediction_markets.domain.market_matching.value_objects import (
    ComparisonOperator,
    ObservationMethod,
    Underlying,
    UpDownMarketKey,
    UpDownOutcome,
    UpDownResolutionRule,
)
from prediction_markets.domain.markets.entities import Market, MarketSide
from prediction_markets.domain.markets.enums import BinaryOutcome, MarketStatus
from prediction_markets.domain.markets.value_objects import MarketState
from prediction_markets.domain.shared.value_objects import (
    Currency,
    MarketID,
    Money,
    OutcomeID,
    Timestamp,
    VenueID,
)


def _key(
    reference_price: str = "67000",
    start: str = "2026-12-31T23:45:00+00:00",
    source: str | None = None,
) -> UpDownMarketKey:
    currency = Currency("USD")
    return UpDownMarketKey(
        underlying=Underlying("BTC"),
        currency=currency,
        start=Timestamp.from_iso(start),
        end=Timestamp.from_iso("2026-12-31T23:59:59+00:00"),
        reference_price=Money(Decimal(reference_price), currency),
        resolution_rule=UpDownResolutionRule(
            observation=ObservationMethod.LAST,
            observation_window_seconds=0,
            comparison=ComparisonOperator.GREATER_THAN_OR_EQUAL,
            tie_outcome=UpDownOutcome.UP,
            source=source,
        ),
    )


def _market(market_id: str, venue_id: str) -> Market:
    return Market(
        id=MarketID(market_id),
        venue_id=VenueID(venue_id),
        title="Will BTC be above $100,000?",
        state=MarketState(MarketStatus.ACTIVE),
        yes_side=MarketSide(
            id=OutcomeID(f"{market_id}-yes"),
            side=BinaryOutcome.YES,
        ),
        no_side=MarketSide(
            id=OutcomeID(f"{market_id}-no"),
            side=BinaryOutcome.NO,
        ),
    )


def test_match_groups_equal_keys_from_different_venues():
    key = _key()
    polymarket = _market("poly-btc", "polymarket")
    kalshi = _market("kalshi-btc", "kalshi")

    matches = MarketMatchingService().match(
        ((key, polymarket), (key, kalshi))
    )

    assert len(matches) == 1
    assert set(matches[0].markets) == {polymarket, kalshi}


def test_match_excludes_key_available_in_only_one_venue():
    key = _key()

    matches = MarketMatchingService().match(
        ((key, _market("poly-btc", "polymarket")),)
    )

    assert matches == ()


def test_match_does_not_combine_different_canonical_keys():
    polymarket_key = _key("67000")
    kalshi_key = _key("68000")

    matches = MarketMatchingService().match(
        (
            (polymarket_key, _market("poly-btc", "polymarket")),
            (kalshi_key, _market("kalshi-btc", "kalshi")),
        )
    )

    assert matches == ()


def test_match_allows_strikes_within_tolerance():
    limitless_key = _key("67000")
    polymarket_key = _key("67000.20")
    limitless = _market("limitless-btc", "limitless")
    polymarket = _market("poly-btc", "polymarket")

    matches = MarketMatchingService().match(
        ((limitless_key, limitless), (polymarket_key, polymarket)),
        strike_tolerance=Decimal("0.20"),
    )

    assert len(matches) == 1
    assert matches[0].markets == (limitless, polymarket)


def test_match_rejects_different_windows_with_same_maturity():
    fifteen_minute_key = _key(start="2026-12-31T23:45:00+00:00")
    five_minute_key = _key(start="2026-12-31T23:55:00+00:00")
    polymarket = _market("poly-btc-15m", "polymarket")
    limitless = _market("limitless-btc-5m", "limitless")

    matches = MarketMatchingService().match(
        ((fifteen_minute_key, polymarket), (five_minute_key, limitless)),
    )

    assert matches == ()


def test_match_rejects_different_resolution_sources():
    polymarket_key = _key(source="BINANCE")
    limitless_key = _key(source="PYTH")

    matches = MarketMatchingService().match(
        (
            (polymarket_key, _market("poly-btc", "polymarket")),
            (limitless_key, _market("limitless-btc", "limitless")),
        ),
    )

    assert matches == ()


def test_match_rejects_negative_strike_tolerance():
    with pytest.raises(ValueError, match="must be non-negative"):
        MarketMatchingService().match((), strike_tolerance=Decimal("-0.01"))


def test_match_requires_distinct_venues():
    key = _key()

    matches = MarketMatchingService().match(
        (
            (key, _market("poly-btc-1", "polymarket")),
            (key, _market("poly-btc-2", "polymarket")),
        )
    )

    assert matches == ()


def test_match_is_order_independent_without_transitive_grouping():
    candidates = (
        (_key("100"), _market("a", "A")),
        (_key("100.15"), _market("b", "B")),
        (_key("100.30"), _market("c", "C")),
    )

    results = []
    for ordered in permutations(candidates):
        matches = MarketMatchingService().match(
            ordered,
            strike_tolerance=Decimal("0.20"),
        )
        results.append(
            {
                frozenset((str(match.left_market.id), str(match.right_market.id)))
                for match in matches
            }
        )

    expected = {frozenset(("a", "b")), frozenset(("b", "c"))}
    assert all(result == expected for result in results)
