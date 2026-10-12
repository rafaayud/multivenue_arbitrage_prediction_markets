"""Exercise value objects behavior in the domain market matching layer.

Responsibilities
----------------
- Verify value objects contracts, edge cases, and failure handling.
"""

import pytest

from prediction_markets.domain.market_matching.value_objects import RegularCandidate
from prediction_markets.domain.markets.entities import Market, MarketSide
from prediction_markets.domain.markets.enums import BinaryOutcome, MarketStatus
from prediction_markets.domain.markets.value_objects import MarketState
from prediction_markets.domain.shared.value_objects import MarketID, OutcomeID, VenueID


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
