"""Exercise websocket behavior in the api layer.

Responsibilities
----------------
- Verify websocket contracts, edge cases, and failure handling.
"""

import asyncio
from datetime import datetime, timezone
from decimal import Decimal
from threading import Event
from urllib.parse import urlencode

import pytest
from starlette.websockets import WebSocketDisconnect
from fastapi.testclient import TestClient

from prediction_markets.api.trading_main import app
from prediction_markets.application.markets.models import MarketCycle, monitored_market_key
from prediction_markets.domain.market_matching.value_objects import RegularCandidate
from prediction_markets.domain.markets.entities import Market, MarketSide
from prediction_markets.domain.markets.enums import BinaryOutcome, MarketStatus
from prediction_markets.domain.markets.value_objects import MarketState
from prediction_markets.domain.shared.value_objects import (
    ContractID,
    MarketID,
    OutcomeID,
    Price,
    Probability,
    Quantity,
    StrategyID,
    Timestamp,
    VenueID,
)
from prediction_markets.domain.trading.entities import Signal
from prediction_markets.domain.trading.enums import SignalDirection
from prediction_markets.domain.trading.value_objects import Confidence, Edge


class _MarketWorker:
    """Provide deterministic market worker behavior for this test module."""
    def __init__(
        self,
        signals: tuple[Signal, Signal],
        markets: dict[str, MarketCycle | RegularCandidate] | None = None,
    ) -> None:
        self.signals = signals
        self.markets = markets or {}
        self.calls: list[MarketCycle | RegularCandidate] = []

    def monitored_market(self, monitor_key: str):
        return self.markets.get(monitor_key)

    async def stream_signal_pairs(self, market):
        self.calls.append(market)
        yield self.signals


class _WaitingWorker:
    """Provide deterministic waiting worker behavior for this test module."""
    def __init__(self) -> None:
        self.started = Event()
        self.stopped = Event()

    def monitored_market(self, _monitor_key):
        return _regular_candidate()

    async def stream_signal_pairs(self, *args, **kwargs):
        self.started.set()
        try:
            await asyncio.Event().wait()
            yield
        finally:
            self.stopped.set()


def _signal(contract: str, venue: str, price: str) -> Signal:
    return Signal(
        contract_id=ContractID(contract),
        venue_id=VenueID(venue),
        direction=SignalDirection.BUY,
        quantity=Quantity(Decimal("2")),
        limit_price=Price(Decimal(price)),
        fair_probability=Probability(Decimal("0.55")),
        edge=Edge(Decimal("0.09")),
        confidence=Confidence(Decimal("1")),
        strategy_id=StrategyID("long-arbitrage"),
        generated_at=Timestamp(datetime(2026, 7, 23, 14, 30, tzinfo=timezone.utc)),
    )


def _regular_candidate() -> RegularCandidate:
    """Build one cross-venue regular candidate for WebSocket tests."""
    markets = tuple(
        Market(
            id=MarketID(market_id),
            venue_id=VenueID(venue),
            title=title,
            state=MarketState(MarketStatus.ACTIVE),
            yes_side=MarketSide(OutcomeID(f"{market_id}:yes"), BinaryOutcome.YES),
            no_side=MarketSide(OutcomeID(f"{market_id}:no"), BinaryOutcome.NO),
        )
        for venue, market_id, title in (
            ("POLYMARKET", "condition-1", "Will the regular event happen?"),
            ("LIMITLESS", "market-1", "Regular event"),
        )
    )
    return RegularCandidate(markets)


def test_streams_regular_candidate_by_monitor_key() -> None:
    """Resolve a regular candidate without cycle-only transport fields."""
    candidate = _regular_candidate()
    key = monitored_market_key(candidate)
    market_worker = _MarketWorker(
        (
            _signal("polymarket:condition:token", "polymarket", "0.40"),
            _signal("limitless:market:no", "limitless", "0.50"),
        ),
        {key: candidate},
    )

    with TestClient(app) as client:
        app.state.market_worker = market_worker
        with client.websocket_connect(
            f"/ws/arbitrage-signals?{urlencode({'monitor_key': key})}",
        ) as websocket:
            message = websocket.receive_json()

    assert message["monitor_type"] == "regular"
    assert message["monitor_key"] == key
    assert message["market_label"] == "Will the regular event happen?"
    assert message["underlying"] is None
    assert message["interval_seconds"] is None
    assert market_worker.calls == [candidate]


def test_stops_stream_when_client_disconnects() -> None:
    market_worker = _WaitingWorker()

    with TestClient(app) as client:
        app.state.market_worker = market_worker
        with client.websocket_connect(
            "/ws/arbitrage-signals?monitor_key=regular-test"
        ):
            assert market_worker.started.wait(timeout=1)

        assert market_worker.stopped.wait(timeout=1)


@pytest.mark.parametrize("query", ["underlying=btc&interval_seconds=3600", "monitor_key=cycle:BTC:3600", "monitor_key=unknown", ""])
def test_rejects_unselected_or_legacy_markets(query):
    """Only an explicitly monitored regular event may open a signal stream."""
    with TestClient(app) as client:
        app.state.market_worker = _MarketWorker(())
        with pytest.raises(WebSocketDisconnect) as error:
            with client.websocket_connect(f"/ws/arbitrage-signals?{query}"):
                pass
        assert error.value.code == 1008
