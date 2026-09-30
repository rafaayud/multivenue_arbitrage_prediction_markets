"""Exercise orderbook stream behavior in the infrastructure agg layer.

Responsibilities
----------------
- Verify orderbook stream contracts, edge cases, and failure handling.
"""

import asyncio
import json
from decimal import Decimal

import prediction_markets.infrastructure.agg.orderbook_stream as orderbook_stream
from prediction_markets.domain.contracts.entities import BinaryContract
from prediction_markets.domain.shared.value_objects import (
    ContractID,
    Currency,
    MarketID,
    OutcomeID,
    VenueID,
)
from prediction_markets.infrastructure.agg.orderbook_stream import (
    AggOrderBookStreamAdapter,
)


class _WebSocket:
    """Provide a scripted WebSocket test double for transport scenarios."""
    def __init__(self, messages, *, block_when_empty=False):
        self._messages = iter(messages)
        self._block_when_empty = block_when_empty
        self.sent: list[str] = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        return None

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            return next(self._messages)
        except StopIteration:
            if self._block_when_empty:
                await asyncio.Future()
            raise StopAsyncIteration from None

    async def send(self, message: str):
        self.sent.append(message)


def _contract() -> BinaryContract:
    return BinaryContract(
        id=ContractID("agg:outcome_1"),
        market_id=MarketID("market_1"),
        outcome_id=OutcomeID("outcome_1"),
        venue_id=VenueID("polymarket"),
        payout_currency=Currency("USD"),
        symbol="YES",
    )


def test_stream_applies_delta_and_resnapshots_on_sequence_gap(monkeypatch):
    contract = _contract()
    websocket = _WebSocket(
        (
            json.dumps(
                {
                    "type": "orderbook_snapshot",
                    "outcomeId": "outcome_1",
                    "seq": 10,
                    "bids": [
                        [0.4, 5, {"polymarket": 5}],
                        [0.45, 3, {"polymarket": 3}],
                    ],
                    "asks": [
                        [0.6, 4, {"polymarket": 4}],
                        [0.55, 2, {"polymarket": 2}],
                    ],
                    "timestamp": 1710000000000,
                }
            ),
            json.dumps(
                {
                    "type": "orderbook_delta",
                    "outcomeId": "outcome_1",
                    "seq": 11,
                    "bidChanges": [[0.45, 7, {"polymarket": 7}]],
                    "askChanges": [[0.6, 0, {}], [0.55, 0, {}]],
                    "timestamp": 1710000000100,
                }
            ),
            json.dumps(
                {
                    "type": "orderbook_delta",
                    "outcomeId": "outcome_1",
                    "seq": 13,
                    "bidChanges": [],
                    "askChanges": [],
                    "timestamp": 1710000000200,
                }
            ),
        ),
        block_when_empty=True,
    )

    def fake_connect(_url, *, origin):
        assert origin == "http://localhost:3000"
        return websocket

    monkeypatch.setattr(orderbook_stream, "connect", fake_connect)

    async def receive():
        stream = AggOrderBookStreamAdapter(
            contracts=(contract,),
            app_id="app_id",
            origin="http://localhost:3000",
        ).stream_order_books((contract.id,))
        snapshot = await anext(stream)
        delta = await anext(stream)
        pending = asyncio.create_task(anext(stream))
        while len(websocket.sent) < 2:
            await asyncio.sleep(0)
        pending.cancel()
        try:
            await pending
        except asyncio.CancelledError:
            pass
        await stream.aclose()
        return snapshot, delta

    snapshot, delta = asyncio.run(receive())

    assert tuple(level.price.value for level in snapshot[1].bids) == (
        Decimal("0.45"),
        Decimal("0.4"),
    )
    assert tuple(level.price.value for level in snapshot[1].asks) == (
        Decimal("0.55"),
        Decimal("0.6"),
    )
    assert snapshot[1].best_bid().quantity.value == Decimal("3")
    assert delta[1].best_bid().quantity.value == Decimal("7")
    assert delta[1].best_ask() is None
    assert json.loads(websocket.sent[0]) == {
        "action": "subscribe",
        "channel": "orderbook",
        "outcomeIds": ["outcome_1"],
    }
    assert json.loads(websocket.sent[1]) == {
        "action": "resnapshot",
        "channel": "orderbook",
        "outcomeIds": ["outcome_1"],
    }
