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

from fastapi.testclient import TestClient

from prediction_markets.api.runtime import MONITORED_MARKET_CYCLES
from prediction_markets.api.trading_main import app
from prediction_markets.application.markets.models import (
    MarketCycle,
    MarketFamily,
    monitored_market_key,
)
from prediction_markets.domain.market_matching.value_objects import (
    RegularCandidate,
    Underlying,
)
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


def test_streams_arbitrage_signal_pair_as_json() -> None:
    market_worker = _MarketWorker(
        (
            _signal("polymarket:condition:token", "polymarket", "0.40"),
            _signal("limitless:market:no", "limitless", "0.50"),
        )
    )

    with TestClient(app) as client:
        app.state.market_worker = market_worker
        with client.websocket_connect(
            "/ws/arbitrage-signals?underlying=btc&interval_seconds=3600"
        ) as websocket:
            message = websocket.receive_json()

    assert message == {
        "type": "arbitrage_signal_pair",
        "monitor_type": "cycle",
        "monitor_key": "cycle:BTC:3600",
        "market_label": "BTC",
        "underlying": "BTC",
        "interval_seconds": 3600,
        "signals": [
            {
                "contract_id": "polymarket:condition:token",
                "venue_id": "polymarket",
                "direction": "buy",
                "quantity": "2",
                "limit_price": "0.40",
                "fair_probability": "0.55",
                "edge": "0.09",
                "strategy_id": "long-arbitrage",
                "generated_at": "2026-07-23T14:30:00Z",
            },
            {
                "contract_id": "limitless:market:no",
                "venue_id": "limitless",
                "direction": "buy",
                "quantity": "2",
                "limit_price": "0.50",
                "fair_probability": "0.55",
                "edge": "0.09",
                "strategy_id": "long-arbitrage",
                "generated_at": "2026-07-23T14:30:00Z",
            },
        ],
    }
    assert market_worker.calls == [MarketCycle(Underlying("BTC"), 3600)]


def test_cycle_selector_preserves_registered_family(monkeypatch) -> None:
    cycle = MarketCycle(Underlying("NVDA"), 86400, MarketFamily.FINANCE)
    monkeypatch.setitem(
        MONITORED_MARKET_CYCLES,
        (cycle.underlying, cycle.interval_seconds),
        cycle,
    )
    market_worker = _MarketWorker(
        (
            _signal("polymarket:nvda:yes", "polymarket", "0.40"),
            _signal("limitless:nvda:no", "limitless", "0.50"),
        ),
    )

    with TestClient(app) as client:
        app.state.market_worker = market_worker
        with client.websocket_connect(
            "/ws/arbitrage-signals?underlying=nvda&interval_seconds=86400",
        ) as websocket:
            websocket.receive_json()

    assert market_worker.calls == [cycle]


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
            "/ws/arbitrage-signals?underlying=btc&interval_seconds=3600"
        ):
            assert market_worker.started.wait(timeout=1)

        assert market_worker.stopped.wait(timeout=1)
