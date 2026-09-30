"""Exercise value objects behavior in the domain market matching layer.

Responsibilities
----------------
- Verify value objects contracts, edge cases, and failure handling.
"""

from decimal import Decimal
import pytest

from prediction_markets.domain.market_matching.value_objects import (
    ComparisonOperator,
    ObservationMethod,
    RegularCandidate,
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


def _resolution_rule(
    observation: ObservationMethod = ObservationMethod.LAST,
) -> UpDownResolutionRule:
    return UpDownResolutionRule(
        observation=observation,
        observation_window_seconds=0,
        comparison=ComparisonOperator.GREATER_THAN_OR_EQUAL,
        tie_outcome=UpDownOutcome.UP,
    )


def _market_key(
    reference_price: str = "67000",
    resolution_rule: UpDownResolutionRule | None = None,
) -> UpDownMarketKey:
    currency = Currency("USD")
    return UpDownMarketKey(
        underlying=Underlying("BTC"),
        currency=currency,
        start=Timestamp.from_iso("2026-12-31T23:45:00+00:00"),
        end=Timestamp.from_iso("2026-12-31T23:59:59+00:00"),
        reference_price=Money(Decimal(reference_price), currency),
        resolution_rule=resolution_rule or _resolution_rule(),
    )


def _market(market_id: str, venue_id: str) -> Market:
    return Market(
        id=MarketID(market_id),
        venue_id=VenueID(venue_id),
        title="Will the proposition resolve YES?",
        state=MarketState(MarketStatus.ACTIVE),
        yes_side=MarketSide(OutcomeID(f"{market_id}-yes"), BinaryOutcome.YES),
        no_side=MarketSide(OutcomeID(f"{market_id}-no"), BinaryOutcome.NO),
    )


def test_regular_candidate_groups_normalized_cross_venue_markets():
    polymarket = _market("condition-1", "POLYMARKET")
    limitless = _market("market-slug", "LIMITLESS")

    candidate = RegularCandidate((polymarket, limitless))

    assert candidate.markets == (polymarket, limitless)
    assert candidate.key == (
        ("LIMITLESS", "market-slug"),
        ("POLYMARKET", "condition-1"),
    )
    with pytest.raises(ValueError, match="at least two venues"):
        RegularCandidate((polymarket, _market("condition-2", "POLYMARKET")))


def test_up_down_market_keys_with_same_values_are_equal():
    assert _market_key() == _market_key()


def test_reference_price_distinguishes_up_down_market_keys():
    assert _market_key("67000") != _market_key("68000")


def test_resolution_rule_distinguishes_up_down_market_keys():
    assert _market_key() != _market_key(
        resolution_rule=UpDownResolutionRule(
            observation=ObservationMethod.MEAN,
            observation_window_seconds=60,
            comparison=ComparisonOperator.GREATER_THAN,
            tie_outcome=UpDownOutcome.DOWN,
        ),
    )


def test_resolution_rule_rejects_negative_observation_window():
    with pytest.raises(ValueError, match="must not be negative"):
        UpDownResolutionRule(
            observation=ObservationMethod.MEAN,
            observation_window_seconds=-1,
            comparison=ComparisonOperator.GREATER_THAN,
            tie_outcome=UpDownOutcome.DOWN,
        )


def test_reference_price_currency_must_match_market_currency():
    with pytest.raises(ValueError, match="currency must match"):
        UpDownMarketKey(
            underlying=Underlying("BTC"),
            currency=Currency("USD"),
            start=Timestamp.from_iso("2026-12-31T23:45:00+00:00"),
            end=Timestamp.from_iso("2026-12-31T23:59:59+00:00"),
            reference_price=Money(Decimal("67000"), Currency("EUR")),
            resolution_rule=_resolution_rule(),
        )
