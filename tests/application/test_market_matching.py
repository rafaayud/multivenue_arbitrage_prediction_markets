"""Exercise manually selected application market matching."""

import asyncio
from datetime import timedelta
from decimal import Decimal

import pytest

from prediction_markets.application.markets.matching import MarketMatcher
from prediction_markets.application.codec import decode_event, encode_event
from prediction_markets.application.events import MarketMatchesUpdated
from prediction_markets.application.markets.models import RegularMarketSelection
from prediction_markets.domain.contracts.entities import BinaryContract
from prediction_markets.domain.contracts.value_objects import Payout
from prediction_markets.domain.markets.entities import Market, MarketSide
from prediction_markets.domain.markets.enums import BinaryOutcome, MarketStatus
from prediction_markets.domain.markets.value_objects import MarketState
from prediction_markets.domain.ports.instrument_discovery import (
    InstrumentDiscoveryPort,
    InstrumentDiscoveryQuery,
    InstrumentDiscoveryResult,
)
from prediction_markets.domain.shared.value_objects import (
    ContractID,
    Currency,
    MarketID,
    OutcomeID,
    Timestamp,
    VenueID,
)


class _Discovery(InstrumentDiscoveryPort):
    def __init__(self, market: Market, contracts: tuple[BinaryContract, ...]) -> None:
        self.market = market
        self.contracts = contracts
        self.queries: list[InstrumentDiscoveryQuery] = []
        self.batches: list[tuple[InstrumentDiscoveryQuery, ...]] = []

    async def discover(
        self,
        query: InstrumentDiscoveryQuery,
    ) -> InstrumentDiscoveryResult:
        self.queries.append(query)
        return InstrumentDiscoveryResult((self.market,), self.contracts)

    async def discover_many(
        self,
        queries: tuple[InstrumentDiscoveryQuery, ...],
    ) -> tuple[InstrumentDiscoveryResult, ...]:
        self.batches.append(queries)
        return await super().discover_many(queries)


def _market(venue: str, market_id: str, closes_at: str | None) -> Market:
    return Market(
        id=MarketID(market_id),
        venue_id=VenueID(venue),
        title="Will the proposition resolve YES?",
        state=MarketState(
            MarketStatus.ACTIVE,
            close_time=(Timestamp.from_iso(closes_at) if closes_at else None),
        ),
        yes_side=MarketSide(OutcomeID(f"{market_id}:yes"), BinaryOutcome.YES),
        no_side=MarketSide(OutcomeID(f"{market_id}:no"), BinaryOutcome.NO),
    )


def _contracts(market: Market) -> tuple[BinaryContract, BinaryContract]:
    return tuple(
        BinaryContract(
            id=ContractID(f"{market.venue_id}:{side.id}"),
            market_id=market.id,
            outcome_id=side.id,
            venue_id=market.venue_id,
            payout_currency=Currency("USD"),
            payout_if_true=Payout(Decimal("1")),
            payout_if_false=Payout(Decimal("0")),
        )
        for side in market.sides
    )


def test_resolve_regular_builds_native_complementary_pairs() -> None:
    polymarket = _market("POLYMARKET", "condition-1", "2026-08-07T12:00:00Z")
    limitless = _market("LIMITLESS", "market-slug", "2026-08-07T12:01:00Z")
    discoveries = {
        polymarket.venue_id: _Discovery(polymarket, _contracts(polymarket)),
        limitless.venue_id: _Discovery(limitless, _contracts(limitless)),
    }
    matcher = MarketMatcher(discoveries)

    candidate, pairs = asyncio.run(
        matcher.resolve_regular(
            (
                RegularMarketSelection(polymarket.venue_id, "condition-1"),
                RegularMarketSelection(limitless.venue_id, "market-slug"),
            ),
        ),
    )

    assert candidate.markets == (polymarket, limitless)
    assert {(pair.left.outcome_id, pair.right.outcome_id) for pair in pairs} == {
        (polymarket.yes_side.id, limitless.no_side.id),
        (polymarket.no_side.id, limitless.yes_side.id),
    }
    assert all(
        pair.ends_at == Timestamp.from_iso("2026-08-07T12:00:00Z")
        for pair in pairs
    )
    assert {
        query.venue_market_id
        for discovery in discoveries.values()
        for query in discovery.queries
    } == {"condition-1", "market-slug"}
    assert all(len(discovery.queries) == 1 for discovery in discoveries.values())
    event = MarketMatchesUpdated(candidate, pairs)
    assert decode_event(encode_event(event)) == event


def test_resolve_regular_uses_known_close_when_predict_omits_it() -> None:
    """Resolve an esports candidate when only one venue reports its window."""
    polymarket = _market("POLYMARKET", "condition-1", "2026-08-14T20:00:00Z")
    predict = _market("PREDICT", "1360403", None)
    discoveries = {
        polymarket.venue_id: _Discovery(polymarket, _contracts(polymarket)),
        predict.venue_id: _Discovery(predict, _contracts(predict)),
    }
    matcher = MarketMatcher(discoveries)

    _, pairs = asyncio.run(
        matcher.resolve_regular(
            (
                RegularMarketSelection(polymarket.venue_id, "condition-1"),
                RegularMarketSelection(predict.venue_id, "1360403"),
            ),
        ),
    )

    assert len(pairs) == 2
    assert all(
        pair.ends_at == Timestamp.from_iso("2026-08-14T20:00:00Z")
        for pair in pairs
    )


def test_resolve_regular_uses_latest_close_during_live_sports_window() -> None:
    """Keep trading when one venue reports kickoff and another reports resolution."""
    now = Timestamp.now()
    resolution = now + timedelta(hours=1)
    polymarket = _market("POLYMARKET", "condition-1", str(resolution))
    limitless = _market("LIMITLESS", "villarreal", str(now - timedelta(hours=1)))
    discoveries = {
        polymarket.venue_id: _Discovery(polymarket, _contracts(polymarket)),
        limitless.venue_id: _Discovery(limitless, _contracts(limitless)),
    }
    matcher = MarketMatcher(discoveries)

    _, pairs = asyncio.run(
        matcher.resolve_regular(
            (
                RegularMarketSelection(polymarket.venue_id, "condition-1"),
                RegularMarketSelection(limitless.venue_id, "villarreal"),
            ),
        ),
    )

    assert len(pairs) == 2
    assert all(pair.ends_at == resolution for pair in pairs)


def test_resolve_regular_rejects_when_every_close_is_unknown() -> None:
    """Reject candidates without any conservative execution deadline."""
    polymarket = _market("POLYMARKET", "condition-1", None)
    predict = _market("PREDICT", "1360403", None)
    discoveries = {
        polymarket.venue_id: _Discovery(polymarket, _contracts(polymarket)),
        predict.venue_id: _Discovery(predict, _contracts(predict)),
    }
    matcher = MarketMatcher(discoveries)

    with pytest.raises(ValueError, match="known closing time"):
        asyncio.run(
            matcher.resolve_regular(
                (
                    RegularMarketSelection(polymarket.venue_id, "condition-1"),
                    RegularMarketSelection(predict.venue_id, "1360403"),
                ),
            ),
        )
