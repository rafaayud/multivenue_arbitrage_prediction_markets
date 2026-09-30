"""Exercise polynode market data stream behavior in the infrastructure polynode layer.

Responsibilities
----------------
- Verify polynode market data stream contracts, edge cases, and failure handling.
"""

import asyncio
import json
from decimal import Decimal

import prediction_markets.infrastructure.venues.polynode.market_data_stream as market_data_stream
from prediction_markets.infrastructure.venues.polymarket.mappers import gamma_market_to_contracts
from prediction_markets.infrastructure.venues.polynode.mappers import polynode_orderbook_to_order_book
from prediction_markets.infrastructure.venues.polynode.market_data_stream import PolynodeMarketDataStreamAdapter
from prediction_markets.infrastructure.venues.polynode.market_data_stream import _snapshot_for_token
from prediction_markets.infrastructure.venues.polynode.market_data_stream import _websocket_url


GAMMA_MARKET = {
    "conditionId": "0xabc",
    "clobTokenIds": '["123", "456"]',
    "outcomes": '["Yes", "No"]',
}
CLOB_BOOK = {
    "asset_id": "123",
    "timestamp": "1710000000000",
    "bids": [{"price": "0.45", "size": "3"}],
    "asks": [{"price": "0.55", "size": "2"}],
}


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


def test_stream_order_books_multiplexes_tokens_over_one_connection(monkeypatch):
    contracts = gamma_market_to_contracts(GAMMA_MARKET)
    websocket = _WebSocket(
        (
            json.dumps(
                {
                    "type": "snapshot_batch",
                    "snapshots": [
                        CLOB_BOOK,
                        {**CLOB_BOOK, "asset_id": "456"},
                    ],
                }
            ),
        )
    )
    connections = 0

    def fake_connect(_url):
        nonlocal connections
        connections += 1
        return websocket

    monkeypatch.setattr(market_data_stream, "connect", fake_connect)

    async def receive_books():
        stream = PolynodeMarketDataStreamAdapter(
            contracts=contracts,
            api_key="test-key",
        ).stream_order_books(tuple(contract.id for contract in contracts))
        updates = (await anext(stream), await anext(stream))
        await stream.aclose()
        return updates

    updates = asyncio.run(receive_books())

    assert connections == 1
    assert {contract_id for contract_id, _ in updates} == {
        contract.id for contract in contracts
    }
    assert json.loads(websocket.sent[0]) == {
        "action": "subscribe",
        "markets": ["123", "456"],
    }


def test_snapshot_batch_selects_the_requested_token():
    snapshot = _snapshot_for_token(
        {
            "type": "snapshot_batch",
            "snapshots": [
                {"asset_id": "999", "bids": [], "asks": []},
                CLOB_BOOK,
            ],
        },
        "123",
    )

    assert snapshot == CLOB_BOOK


def test_price_change_updates_the_book():
    contract = gamma_market_to_contracts(GAMMA_MARKET)[0]
    current_book = polynode_orderbook_to_order_book(CLOB_BOOK, contract=contract)

    updated_book = PolynodeMarketDataStreamAdapter._apply_update(
        current_book=current_book,
        update={
            "type": "price_change",
            "assets": [{"asset_id": "123", "price": "0.45", "size": "7", "side": "BUY"}],
        },
        token_id="123",
    )

    assert updated_book is not None
    assert updated_book.best_bid().price.value == Decimal("0.45")
    assert updated_book.best_bid().quantity.value == Decimal("7")


def test_websocket_url_encodes_the_api_key():
    assert _websocket_url("wss://ob.polynode.dev/ws", "key with space") == (
        "wss://ob.polynode.dev/ws?key=key+with+space"
    )


def test_api_key_can_be_injected_or_read_from_poly_node_api(monkeypatch):
    monkeypatch.setenv("POLY_NODE_API", "env-key")

    assert PolynodeMarketDataStreamAdapter()._api_key == "pn_live_env-key"
    assert PolynodeMarketDataStreamAdapter(api_key="injected-key")._api_key == "injected-key"

    monkeypatch.delenv("POLY_NODE_API")
    monkeypatch.setenv("POLYNODE_API_KEY", "alias-key")

    assert PolynodeMarketDataStreamAdapter()._api_key == "pn_live_alias-key"
