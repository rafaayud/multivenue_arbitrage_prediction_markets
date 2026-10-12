"""Keep historical journals readable while admitting only selected events live."""

import asyncio
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from prediction_markets.api.runtime.facade import ArbitrageRuntime

from prediction_markets.api.trading.runner import LiveArbitrageConfig
from prediction_markets.application.codec import decode_event, encode_event
from prediction_markets.application.engine import TradingEngine
from prediction_markets.application.events import (
    MarketMatchesUpdated,
    OpportunityValidationRef,
    OrderBookUpdated,
    SubmitOrder,
)
from prediction_markets.application.state import EventDispatcher, TradingState
from prediction_markets.domain.market_matching.value_objects import RegularCandidate
from tests.api.test_regular_market_runtime import _market
from tests.application.test_dispatch_preparation import _detected, _pair, _ZeroFees
from tests.application.test_engine import _process


def test_legacy_opportunity_round_trips_without_authorizing_live_orders():
    """A replayed cycle and worker provenance remain data, never live admission."""
    cycle, pair, left, right = _pair()
    detected = replace(_detected(cycle, pair, left, right), validation_ref=OpportunityValidationRef(
        "old-intent", "old-partition", "old-generation", 1, "left-book", "right-book",
    ))
    restored = decode_event(encode_event(detected))
    assert restored == detected
    state = TradingState()
    engine = TradingEngine(EventDispatcher(state), {c.venue_id: _ZeroFees() for c in (pair.left, pair.right)})
    engine.enable(LiveArbitrageConfig().engine_config())
    engine.process(MarketMatchesUpdated(cycle, (pair,)), replay=True)
    engine.process(restored, replay=True)
    assert list(state.opportunities) == [restored]
    assert engine.process(restored) == ()
    assert not state.executions


def test_live_config_still_admits_a_selected_event():
    """Removing cycle admission must preserve the manual two-leg trading path."""
    _, pair, left, right = _pair()
    candidate = RegularCandidate(tuple(_market(c.venue_id, c.market_id, c.outcome_id) for c in (pair.left, pair.right)))
    state = TradingState()
    engine = TradingEngine(EventDispatcher(state), {c.venue_id: _ZeroFees() for c in (pair.left, pair.right)})
    engine.enable(LiveArbitrageConfig().engine_config())
    _process(engine, MarketMatchesUpdated(candidate, (pair,)))
    _process(engine, OrderBookUpdated(pair.left.venue_id, pair.left.id, left))
    output = _process(engine, OrderBookUpdated(pair.right.venue_id, pair.right.id, right))
    assert len([event for event in output if isinstance(event, SubmitOrder)]) == 2


@pytest.mark.parametrize("market", [None, _pair()[0]])
def test_short_inventory_rejects_unselected_or_legacy_keys_before_preparation(monkeypatch, market):
    """Reject obsolete selections before any inventory split can be requested."""
    runtime = ArbitrageRuntime(enabled=False)
    runtime.engine = object()
    runtime.pipeline = object()
    runtime._safety = object()
    prepare = AsyncMock()
    runtime._inventory = SimpleNamespace(prepare=prepare)
    runtime._feed = SimpleNamespace(monitored_market=lambda _key: market)
    monkeypatch.setattr(runtime, "start", AsyncMock())
    monkeypatch.setattr(runtime, "_ensure_execution", AsyncMock())
    with pytest.raises(RuntimeError, match="explicitly monitored events"):
        asyncio.run(runtime._run_execution(
            LiveArbitrageConfig(short_market_keys=("removed-selection",)), asyncio.Event(),
        ))
    prepare.assert_not_awaited()
