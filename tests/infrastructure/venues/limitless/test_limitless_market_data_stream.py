"""Exercise limitless market data stream behavior in the infrastructure limitless layer.

Responsibilities
----------------
- Verify limitless market data stream contracts, edge cases, and failure handling.
"""

import asyncio
import json
import logging

import prediction_markets.infrastructure.venues.limitless.market_data_stream as market_data_stream
from prediction_markets.infrastructure.venues.limitless.market_data_stream import LimitlessMarketDataStreamAdapter
from prediction_markets.infrastructure.venues.limitless.market_data_stream import _is_namespace_connected
from prediction_markets.infrastructure.venues.limitless.market_data_stream import _namespace_connect_message
from prediction_markets.infrastructure.venues.limitless.market_data_stream import _socketio_event
from prediction_markets.infrastructure.venues.limitless.market_data_stream import _subscription_message
from prediction_markets.infrastructure.venues.limitless.mappers import limitless_market_to_contracts


class _WebSocket:
    """Provide a scripted WebSocket test double for transport scenarios."""
    def __init__(self, messages=()):
        self.messages = iter(messages)
        self.sent: list[str] = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        return None

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            return next(self.messages)
        except StopIteration:
            raise StopAsyncIteration from None

    async def send(self, message: str):
        self.sent.append(message)


def test_subscription_message_targets_clob_market_slug():
    message = _subscription_message("btc-up-or-down-hourly-123")

    assert message == (
        '42/markets,["subscribe_market_prices", {"marketSlugs": '
        '["btc-up-or-down-hourly-123"]}]'
    )


def test_stream_order_books_multiplexes_yes_and_no_over_one_connection(
    monkeypatch,
    caplog,
    limitless_market_payload,
    limitless_orderbook_payload,
):
    caplog.set_level(logging.INFO, logger="prediction_markets.events.limitless")
    slug = limitless_market_payload["slug"]
    second_slug = "eth-up-or-down-hourly-456"
    second_market = {
        **limitless_market_payload,
        "slug": second_slug,
        "tokens": {"yes": "333", "no": "444"},
    }
    contracts = (
        *limitless_market_to_contracts(limitless_market_payload),
        *limitless_market_to_contracts(second_market),
    )
    events = []
    for market_slug in (slug, second_slug):
        payload = [
            "orderbookUpdate",
            {"marketSlug": market_slug, "orderbook": limitless_orderbook_payload},
        ]
        events.append(f"42/markets,{json.dumps(payload)}")
    websocket = _WebSocket(("0{}", "40/markets,", *events))
    connections = 0

    def fake_connect(_url, **_kwargs):
        nonlocal connections
        connections += 1
        return websocket

    monkeypatch.setattr(market_data_stream, "connect", fake_connect)

    async def receive_books():
        stream = LimitlessMarketDataStreamAdapter(
            contracts=contracts,
        ).stream_order_books(tuple(contract.id for contract in contracts))
        updates = tuple([await anext(stream) for _ in contracts])
        await stream.aclose()
        return updates

    updates = asyncio.run(receive_books())

    assert connections == 1
    assert {contract_id for contract_id, _ in updates} == {
        contract.id for contract in contracts
    }
    assert all(book.arrival_wall_at_ns is not None for _, book in updates)
    assert all(book.arrival_at_ns == book.received_at_ns for _, book in updates)
    assert all(book.source_timestamp_kind == "venue_update" for _, book in updates)
    assert websocket.sent == [
        _namespace_connect_message(),
        _subscription_message((slug, second_slug)),
    ]
    assert "WS Limitless · conectado · 2 mercados" in caplog.messages


def test_namespace_connect_message_is_recognized():
    assert _namespace_connect_message() == "40/markets,"
    assert _is_namespace_connected("40/markets,")
    assert _is_namespace_connected("40/markets")
    assert _is_namespace_connected('40/markets,{"sid":"server-session"}')
    assert not _is_namespace_connected("40/")


def test_socketio_event_decodes_orderbook_update():
    event = _socketio_event(
        '42/markets,["orderbookUpdate", {"marketSlug": "btc-up-or-down-hourly-123", '
        '"orderbook": {"bids": [], "asks": []}}]',
    )

    assert event is not None
    event_name, payload = event
    assert event_name == "orderbookUpdate"
    assert payload["marketSlug"] == "btc-up-or-down-hourly-123"


def test_socketio_event_rejects_malformed_payload():
    assert _socketio_event("42/markets,not-json") is None
