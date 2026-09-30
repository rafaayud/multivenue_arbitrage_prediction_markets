"""Exercise kalshi market data stream behavior in the infrastructure kalshi layer.

Responsibilities
----------------
- Verify kalshi market data stream contracts, edge cases, and failure handling.
"""

import asyncio
import json
from decimal import Decimal

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
import prediction_markets.infrastructure.venues.kalshi.market_data_stream as market_data_stream
from prediction_markets.infrastructure.venues.kalshi.mappers import kalshi_market_to_contracts
from prediction_markets.infrastructure.venues.kalshi.mappers import kalshi_orderbook_to_order_book
from prediction_markets.infrastructure.venues.kalshi.market_data_stream import KalshiMarketDataStreamAdapter
from prediction_markets.infrastructure.venues.kalshi.market_data_stream import _apply_orderbook_delta
from prediction_markets.infrastructure.venues.kalshi.market_data_stream import _decode_messages
from prediction_markets.infrastructure.venues.kalshi.market_data_stream import _subscription_message


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


def test_subscription_message_targets_orderbook_channels():
    message = _subscription_message("KXBTC-26JAN01-B100000")

    assert message["cmd"] == "subscribe"
    assert message["params"]["channels"] == ["orderbook_delta"]
    assert message["params"]["market_tickers"] == ["KXBTC-26JAN01-B100000"]


def test_stream_order_books_multiplexes_markets_over_one_connection(
    monkeypatch,
    kalshi_market_payload,
    kalshi_orderbook_payload,
):
    second_market = {
        **kalshi_market_payload,
        "ticker": "KXETH-26JAN01-B5000",
    }
    contracts = (
        *kalshi_market_to_contracts(kalshi_market_payload),
        *kalshi_market_to_contracts(second_market),
    )
    tickers = (kalshi_market_payload["ticker"], second_market["ticker"])
    websocket = _WebSocket(
        json.dumps(
            {
                "type": "orderbook_snapshot",
                "msg": {
                    **kalshi_orderbook_payload,
                    "market_ticker": ticker,
                },
            }
        )
        for ticker in tickers
    )
    connections = 0

    def fake_connect(_url, **_kwargs):
        nonlocal connections
        connections += 1
        return websocket

    monkeypatch.setattr(market_data_stream, "connect", fake_connect)
    monkeypatch.setattr(
        KalshiMarketDataStreamAdapter,
        "_authentication_headers",
        lambda self: {},
    )

    async def receive_books():
        stream = KalshiMarketDataStreamAdapter(
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
    assert json.loads(websocket.sent[0]) == _subscription_message(tickers)


def test_authentication_headers_sign_websocket_handshake(tmp_path):
    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    private_key_path = tmp_path / "kalshi_private.pem"
    private_key_path.write_bytes(
        private_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        ),
    )
    adapter = KalshiMarketDataStreamAdapter(
        api_key_id="test-key-id",
        private_key_path=private_key_path,
    )

    headers = adapter._authentication_headers()

    assert headers["KALSHI-ACCESS-KEY"] == "test-key-id"
    assert headers["KALSHI-ACCESS-SIGNATURE"]
    assert headers["KALSHI-ACCESS-TIMESTAMP"].isdigit()


def test_decode_messages_accepts_single_message():
    messages = _decode_messages(
        '{"type": "orderbook_snapshot", "msg": {"market_ticker": "KXBTC"}}',
    )

    assert len(messages) == 1
    assert messages[0]["type"] == "orderbook_snapshot"


def test_decode_messages_accepts_message_list():
    messages = _decode_messages(
        '[{"type": "orderbook_snapshot"}, {"type": "orderbook_delta"}]',
    )

    assert len(messages) == 2
    assert messages[1]["type"] == "orderbook_delta"


def test_apply_orderbook_delta_updates_same_outcome_bid(
    kalshi_market_payload,
    kalshi_orderbook_payload,
):
    contract = kalshi_market_to_contracts(kalshi_market_payload)[0]
    current_book = kalshi_orderbook_to_order_book(kalshi_orderbook_payload, contract=contract)
    message = {
        "market_ticker": "KXBTC-26JAN01-B100000",
        "side": "yes",
        "price": 45,
        "delta": 4,
    }

    updated_book = _apply_orderbook_delta(
        current_book=current_book,
        message=message,
        ticker="KXBTC-26JAN01-B100000",
        outcome="yes",
    )

    assert updated_book.best_bid().price.value == Decimal("0.45")
    assert updated_book.best_bid().quantity.value == Decimal("7")


def test_apply_orderbook_delta_accepts_fixed_point_delta(
    kalshi_market_payload,
    kalshi_orderbook_payload,
):
    contract = kalshi_market_to_contracts(kalshi_market_payload)[0]
    current_book = kalshi_orderbook_to_order_book(kalshi_orderbook_payload, contract=contract)
    message = {
        "market_ticker": "KXBTC-26JAN01-B100000",
        "side": "yes",
        "price_dollars": "0.45",
        "delta_fp": "4.00",
    }

    updated_book = _apply_orderbook_delta(
        current_book=current_book,
        message=message,
        ticker="KXBTC-26JAN01-B100000",
        outcome="yes",
    )

    assert updated_book.best_bid().price.value == Decimal("0.45")
    assert updated_book.best_bid().quantity.value == Decimal("7.00")


def test_apply_orderbook_delta_removes_depleted_bid(
    kalshi_market_payload,
    kalshi_orderbook_payload,
):
    contract = kalshi_market_to_contracts(kalshi_market_payload)[0]
    current_book = kalshi_orderbook_to_order_book(kalshi_orderbook_payload, contract=contract)
    message = {
        "market_ticker": "KXBTC-26JAN01-B100000",
        "side": "yes",
        "price": 45,
        "delta": -3,
    }

    updated_book = _apply_orderbook_delta(
        current_book=current_book,
        message=message,
        ticker="KXBTC-26JAN01-B100000",
        outcome="yes",
    )

    assert updated_book.best_bid().price.value == Decimal("0.44")


def test_apply_orderbook_delta_updates_opposite_outcome_as_ask(
    kalshi_market_payload,
    kalshi_orderbook_payload,
):
    contract = kalshi_market_to_contracts(kalshi_market_payload)[0]
    current_book = kalshi_orderbook_to_order_book(kalshi_orderbook_payload, contract=contract)
    message = {
        "market_ticker": "KXBTC-26JAN01-B100000",
        "side": "no",
        "price": 40,
        "delta": 5,
    }

    updated_book = _apply_orderbook_delta(
        current_book=current_book,
        message=message,
        ticker="KXBTC-26JAN01-B100000",
        outcome="yes",
    )

    assert updated_book.best_ask().price.value == Decimal("0.58")
    assert updated_book.best_ask().quantity.value == Decimal("4")
    assert [level.quantity.value for level in updated_book.asks if level.price.value == Decimal("0.60")] == [
        Decimal("7"),
    ]


def test_apply_orderbook_delta_ignores_other_ticker(
    kalshi_market_payload,
    kalshi_orderbook_payload,
):
    contract = kalshi_market_to_contracts(kalshi_market_payload)[0]
    current_book = kalshi_orderbook_to_order_book(kalshi_orderbook_payload, contract=contract)
    message = {
        "market_ticker": "OTHER-TICKER",
        "side": "yes",
        "price": 45,
        "delta": 4,
    }

    updated_book = _apply_orderbook_delta(
        current_book=current_book,
        message=message,
        ticker="KXBTC-26JAN01-B100000",
        outcome="yes",
    )

    assert updated_book is None
