"""Verify live ticks survive discovery refresh and concurrent metadata preload."""

import asyncio
from decimal import Decimal
from types import SimpleNamespace
from prediction_markets.api.runtime.feeds import _MarketFeedCoordinator
from prediction_markets.application.events import MarketMatchesUpdated
from prediction_markets.application.markets.models import monitored_market_key
from prediction_markets.application.state import TradingState
from prediction_markets.domain.contracts.value_objects import TickSize
from prediction_markets.domain.market_matching.value_objects import MatchedContractPair
from prediction_markets.domain.shared.value_objects import Timestamp, VenueID
from prediction_markets.infrastructure.venues.polymarket.execution import (
    PolymarketExecutionAdapter,
)
from tests.application.test_dispatch_preparation import _contract
from tests.api.test_regular_market_runtime import _Fees, _Pipeline, _Stream
from tests.infrastructure.venues.polymarket.test_execution import _Client
from tests.api.test_regular_market_runtime import _market
from prediction_markets.domain.market_matching.value_objects import RegularCandidate

def _matches() -> MarketMatchesUpdated:
    """Reproduce a selected event route with a coarse initial discovery tick."""
    left = _contract("polymarket:condition-1:token-1", VenueID("POLYMARKET"), "yes")
    right = _contract("predict:42:no", VenueID("PREDICT"), "no")
    return MarketMatchesUpdated(
        RegularCandidate(tuple(_market(c.venue_id, c.market_id, c.outcome_id) for c in (left, right))),
        (MatchedContractPair(left, right, Timestamp.from_iso("2099-01-01T00:00:00Z")),),
    )


def _feed(event, *, pipeline=None):
    venues = (VenueID("POLYMARKET"), VenueID("PREDICT"))
    return _MarketFeedCoordinator(
        SimpleNamespace(), {venue: _Stream() for venue in venues},
        {venue: _Fees() for venue in venues}, pipeline or _Pipeline(),
        TradingState(matches={event.cycle: event.pairs}),
        refresh_seconds=15,
    )


def test_late_preload_result_cannot_replace_a_concurrent_stream_tick():
    """Restore SDK and adapter metadata after an older background preload completes."""
    event = _matches()
    contract = event.pairs[0].left
    feed = _feed(event)
    client = _Client()
    adapter = PolymarketExecutionAdapter(client=client)
    adapter.preload((contract.id,))

    async def run():
        await feed.configure_execution({contract.venue_id: adapter}, {})
        loop = asyncio.get_running_loop()

        def stale_preload(_ids):
            changed = asyncio.run_coroutine_threadsafe(feed._handle_tick_size_change(
                contract.venue_id, contract.id, TickSize(Decimal("0.001")),
            ), loop)
            changed.result(timeout=1)
            adapter._tick_size_by_token["token-1"] = Decimal("0.01")
            client._ClobClient__tick_sizes["token-1"] = "0.01"
            return {contract.id: TickSize(Decimal("0.01"))}

        adapter.preload = stale_preload
        await feed._preload_execution(contract.venue_id, (contract.id,))

    asyncio.run(run())
    assert adapter._tick_size_by_token["token-1"] == Decimal("0.001")
    assert client._ClobClient__tick_sizes["token-1"] == "0.001"
    assert feed._pipeline.sink.events[-1].pairs[0].left.tick_size.value == Decimal("0.001")


def test_refresh_preserves_live_ticks_and_accepts_later_increases():
    """Stale discovery metadata cannot overwrite a newer stream tick."""
    event = _matches()
    contract = event.pairs[0].left
    feed = _feed(event)

    async def resolve_regular(_selections):
        return event.cycle, event.pairs

    async def run():
        feed._matcher = SimpleNamespace(resolve_regular=resolve_regular)
        await feed._handle_tick_size_change(contract.venue_id, contract.id, TickSize(Decimal("0.001")))
        await feed._refresh_regular_matches()
        assert feed._matches[monitored_market_key(event.cycle)].pairs[0].left.tick_size.value == Decimal("0.001")
        await feed._handle_tick_size_change(contract.venue_id, contract.id, TickSize(Decimal("0.01")))
        assert feed._matches[monitored_market_key(event.cycle)].pairs[0].left.tick_size.value == Decimal("0.01")

    asyncio.run(run())
