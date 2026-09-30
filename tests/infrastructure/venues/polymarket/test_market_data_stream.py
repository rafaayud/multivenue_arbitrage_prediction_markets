"""Exercise market data stream behavior in the infrastructure polymarket layer.

Responsibilities
----------------
- Verify market data stream contracts, edge cases, and failure handling.
"""

import asyncio
from dataclasses import replace
import json
from decimal import Decimal
import logging
import threading
import time

import pytest
from prometheus_client import REGISTRY
from websockets.asyncio.messages import Assembler
from websockets.frames import Frame, OP_TEXT

import prediction_markets.infrastructure.venues.polymarket.market_data_stream as market_data_stream
from prediction_markets.domain.contracts.value_objects import TickSize
from prediction_markets.infrastructure.venues.polymarket.market_data_stream import PolymarketMarketDataStreamAdapter
from prediction_markets.infrastructure.venues.polymarket.market_data_stream import _decode_messages
from prediction_markets.infrastructure.venues.polymarket.mappers import clob_book_to_order_book
from prediction_markets.infrastructure.venues.polymarket.mappers import gamma_market_to_contracts


class _WebSocket:
    """Provide a scripted WebSocket test double for transport scenarios."""
    def __init__(
        self,
        messages=(),
        error: Exception | None = None,
        queue_depths=(),
        block_when_exhausted: bool = False,
        receive_gates: dict[int, threading.Event] | None = None,
    ):
        self.messages = iter(messages)
        self.error = error
        self.queue_depths = iter(queue_depths)
        self.block_when_exhausted = block_when_exhausted
        self.receive_gates = receive_gates or {}
        self.receive_index = 0
        self.sent: list[str] = []
        self.closed = False
        self.recv_messages = type(
            "ReceiveState",
            (),
            {"frames": (), "paused": False},
        )()

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        self.closed = True
        return None

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self.error is not None:
            error, self.error = self.error, None
            raise error
        try:
            return next(self.messages)
        except StopIteration:
            if self.block_when_exhausted:
                await asyncio.Event().wait()
            raise StopAsyncIteration from None

    async def recv(self):
        """Receive one scripted frame through the websockets 15 interface."""
        gate = self.receive_gates.get(self.receive_index)
        self.receive_index += 1
        if gate is not None:
            deadline = asyncio.get_running_loop().time() + 1
            while not gate.is_set():
                if asyncio.get_running_loop().time() >= deadline:
                    raise TimeoutError(
                        "Scripted WebSocket receive gate was not released"
                    )
                await asyncio.sleep(0.001)
        message = await self.__anext__()
        depth = next(self.queue_depths, 0)
        self.recv_messages.frames = (None,) * depth
        self.recv_messages.paused = depth >= market_data_stream._WS_QUEUE_CAPACITY
        return message

    async def send(self, message: str):
        self.sent.append(message)


def test_apply_price_change_updates_existing_bid(gamma_market_payload, clob_book_payload):
    contract = gamma_market_to_contracts(gamma_market_payload)[0]
    current_book = clob_book_to_order_book(clob_book_payload, contract=contract)
    message = {
        "event_type": "price_change",
        "price_changes": [
            {
                "asset_id": "123",
                "price": "0.45",
                "size": "7",
                "side": "BUY",
            },
        ],
    }

    updated_book = PolymarketMarketDataStreamAdapter._apply_price_change(
        current_book=current_book,
        message=message,
        token_id="123",
    )

    assert updated_book.best_bid().price.value == Decimal("0.45")
    assert updated_book.best_bid().quantity.value == Decimal("7")


def test_mutable_book_profiles_transformations_without_changing_depth(
    gamma_market_payload,
    clob_book_payload,
):
    """Measure sampled work while preserving price and quantity updates."""
    contract = gamma_market_to_contracts(gamma_market_payload)[0]
    current_book = clob_book_to_order_book(clob_book_payload, contract=contract)
    mutable = market_data_stream._MutableOrderBook.from_order_book(current_book)
    profile: dict[str, int] = {}

    applied = mutable.apply_price_change(
        (
            {
                "asset_id": "123",
                "price": "0.45",
                "size": "7",
                "side": "BUY",
                "best_bid": "0.45",
                "best_ask": "0.55",
            },
        ),
        "123",
        None,
        profile=profile,
    )
    updated_book = mutable.snapshot(profile)

    assert applied
    assert updated_book.best_bid().price.value == Decimal("0.45")
    assert updated_book.best_bid().quantity.value == Decimal("7")
    assert profile["apply_deltas_items"] == 1
    assert profile["validate_top_items"] == 0
    assert profile["materialize_snapshot_items"] == (
        len(mutable.bids) + len(mutable.asks)
    )
    assert all(value >= 0 for key, value in profile.items() if key.endswith("_ns"))


def test_apply_price_change_removes_zero_size_level(gamma_market_payload, clob_book_payload):
    contract = gamma_market_to_contracts(gamma_market_payload)[0]
    current_book = clob_book_to_order_book(clob_book_payload, contract=contract)
    message = {
        "event_type": "price_change",
        "price_changes": [
            {
                "asset_id": "123",
                "price": "0.45",
                "size": "0",
                "side": "BUY",
            },
        ],
    }

    updated_book = PolymarketMarketDataStreamAdapter._apply_price_change(
        current_book=current_book,
        message=message,
        token_id="123",
    )

    assert updated_book.best_bid().price.value == Decimal("0.44")


def test_apply_price_change_ignores_other_token(gamma_market_payload, clob_book_payload):
    contract = gamma_market_to_contracts(gamma_market_payload)[0]
    current_book = clob_book_to_order_book(clob_book_payload, contract=contract)
    message = {
        "event_type": "price_change",
        "price_changes": [
            {
                "asset_id": "999",
                "price": "0.45",
                "size": "7",
                "side": "BUY",
            },
        ],
    }

    updated_book = PolymarketMarketDataStreamAdapter._apply_price_change(
        current_book=current_book,
        message=message,
        token_id="123",
    )

    assert updated_book is None


def test_decode_messages_accepts_single_message():
    messages = _decode_messages(
        '{"event_type": "book", "asset_id": "123", "bids": [], "asks": []}',
    )

    assert len(messages) == 1
    assert messages[0]["event_type"] == "book"


def test_decode_messages_accepts_message_list():
    messages = _decode_messages(
        '[{"event_type": "book", "asset_id": "123"}, {"event_type": "price_change"}]',
    )

    assert len(messages) == 2
    assert messages[1]["event_type"] == "price_change"


def test_decode_messages_ignores_application_heartbeat():
    """Do not parse the server heartbeat reply as JSON market data."""
    assert _decode_messages("PONG") == ()


def test_invalid_venue_timestamp_restarts_only_its_market_socket():
    """Treat malformed venue time as unsafe queued state."""
    adapter = PolymarketMarketDataStreamAdapter()

    with pytest.raises(market_data_stream._RestartSocket) as raised:
        adapter._reject_stale_message(
            "condition",
            {"timestamp": "not-an-epoch"},
        )

    assert raised.value.reason == "message_timestamp"


def test_message_queue_timing_observes_assembler_wait(monkeypatch):
    """Measure wait between websockets assembler enqueue and message dequeue."""
    condition_id = "condition"
    timestamps = iter((1_000_000_000, 1_250_000_000))
    monkeypatch.setattr(time, "monotonic_ns", lambda: next(timestamps))
    labels = {"condition_id": condition_id}
    before = REGISTRY.get_sample_value(
        "polymarket_ws_message_queue_wait_seconds_sum",
        labels,
    ) or 0

    async def receive_message():
        assembler = Assembler()
        websocket = type("WebSocket", (), {"recv_messages": assembler})()
        assert market_data_stream._instrument_message_queue(websocket, condition_id)
        assembler.put(Frame(OP_TEXT, b"message"))
        return await assembler.get()

    assert asyncio.run(receive_message()) == "message"

    after = REGISTRY.get_sample_value(
        "polymarket_ws_message_queue_wait_seconds_sum",
        labels,
    )
    assert after == before + 0.25


def test_worker_bridge_replaces_an_unread_contract_snapshot(
    gamma_market_payload,
    clob_book_payload,
):
    """Keep the latest fully reconstructed book at the thread boundary."""
    contract = gamma_market_to_contracts(gamma_market_payload)[0]
    first = clob_book_to_order_book(clob_book_payload, contract=contract)
    latest = replace(first, source_hash="latest")
    before = REGISTRY.get_sample_value(
        "polymarket_bridge_replacements_total",
    ) or 0

    async def receive_latest():
        bridge = market_data_stream._ThreadsafeLatestBookBridge(
            asyncio.get_running_loop(),
        )
        bridge.publish_book((contract.id, first))
        bridge.publish_book((contract.id, latest))
        await asyncio.sleep(0)
        return await bridge.get()

    item = asyncio.run(receive_latest())

    assert isinstance(item, tuple)
    assert item == (contract.id, latest)
    assert REGISTRY.get_sample_value(
        "polymarket_bridge_replacements_total",
    ) == before + 1
    assert REGISTRY.get_sample_value("polymarket_bridge_pending_books") == 0


def test_price_change_rejects_a_top_of_book_mismatch(
    gamma_market_payload,
    clob_book_payload,
):
    """Force a fresh snapshot when local depth disagrees with Polymarket."""
    contract = gamma_market_to_contracts(gamma_market_payload)[0]
    current_book = clob_book_to_order_book(clob_book_payload, contract=contract)
    message = {
        "event_type": "price_change",
        "price_changes": [
            {
                "asset_id": "123",
                "price": "0.45",
                "size": "7",
                "side": "BUY",
                "best_bid": "0.46",
                "best_ask": "0.55",
            },
        ],
    }

    with pytest.raises(market_data_stream._OrderBookOutOfSync, match="out of sync"):
        PolymarketMarketDataStreamAdapter._apply_price_change(
            current_book=current_book,
            message=message,
            token_id="123",
        )


def test_price_change_prunes_stale_levels_beyond_advertised_top(
    gamma_market_payload,
    clob_book_payload,
):
    """Remove stale aggressive levels without requesting another snapshot."""
    contract = gamma_market_to_contracts(gamma_market_payload)[0]
    current_book = clob_book_to_order_book(clob_book_payload, contract=contract)
    message = {
        "event_type": "price_change",
        "price_changes": [
            {
                "asset_id": "123",
                "price": "0.43",
                "size": "7",
                "side": "BUY",
                "best_bid": "0.44",
                "best_ask": "0.56",
            },
        ],
    }

    updated_book = PolymarketMarketDataStreamAdapter._apply_price_change(
        current_book=current_book,
        message=message,
        token_id="123",
    )

    assert updated_book is not None
    assert updated_book.best_bid().price.value == Decimal("0.44")
    assert updated_book.best_ask().price.value == Decimal("0.56")


def test_terminal_boundary_top_matches_empty_book_sides(
    gamma_market_payload,
    clob_book_payload,
) -> None:
    """Accept Polymarket terminal sentinels without snapshot resubscription."""
    contract = gamma_market_to_contracts(gamma_market_payload)[0]
    current_book = clob_book_to_order_book(clob_book_payload, contract=contract)
    terminal_book = replace(current_book, bids=(), asks=())

    market_data_stream._require_matching_top(
        terminal_book,
        {"best_bid": "0", "best_ask": "1"},
        "123",
    )


def test_stream_order_books_multiplexes_contracts_over_one_connection(
    monkeypatch,
    caplog,
    gamma_market_payload,
    clob_book_payload,
):
    caplog.set_level(logging.INFO, logger="prediction_markets.events.polymarket")
    contracts = gamma_market_to_contracts(gamma_market_payload)
    snapshots = (
        json.dumps({**clob_book_payload, "event_type": "book"}),
        json.dumps(
            {
                **clob_book_payload,
                "asset_id": "456",
                "event_type": "book",
            }
        ),
    )
    websocket = _WebSocket(snapshots)
    connections = 0

    def fake_connect(_url, **_kwargs):
        nonlocal connections
        connections += 1
        return websocket

    monkeypatch.setattr(market_data_stream, "connect", fake_connect)

    async def receive_books():
        stream = PolymarketMarketDataStreamAdapter(
            contracts=contracts,
            max_event_age_seconds=None,
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
        "type": "market",
        "assets_ids": ["123", "456"],
        "custom_feature_enabled": True,
    }
    assert "WS Polymarket | connected | market 0xabc | 2 books" in caplog.messages


def test_stream_uses_one_socket_for_each_binary_market(
    monkeypatch,
    gamma_market_payload,
    clob_book_payload,
):
    """Isolate four active conditions while retaining both tokens on each socket."""
    market_contracts = []
    sockets = []
    expected_tokens = []
    for index in range(4):
        condition_id = f"0xmarket{index}"
        token_ids = (f"{index}01", f"{index}02")
        payload = {
            **gamma_market_payload,
            "conditionId": condition_id,
            "clobTokenIds": json.dumps(token_ids),
        }
        contracts = gamma_market_to_contracts(payload)
        market_contracts.extend(contracts)
        expected_tokens.append(set(token_ids))
        sockets.append(
            _WebSocket(
                tuple(
                    json.dumps(
                        {
                            **clob_book_payload,
                            "market": condition_id,
                            "asset_id": token_id,
                            "event_type": "book",
                        }
                    )
                    for token_id in token_ids
                ),
                block_when_exhausted=True,
            )
        )

    remaining_sockets = iter(sockets)
    connect_kwargs = []
    connected_at = []
    connected_threads = []

    def fake_connect(_url, **kwargs):
        connect_kwargs.append(kwargs)
        connected_at.append(asyncio.get_running_loop().time())
        connected_threads.append(threading.get_ident())
        return next(remaining_sockets)

    monkeypatch.setattr(market_data_stream, "connect", fake_connect)

    async def receive_all_books():
        connections_before = REGISTRY.get_sample_value(
            "polymarket_ws_connections_total",
        ) or 0
        frames_before = REGISTRY.get_sample_value(
            "polymarket_ws_frames_received_total",
        ) or 0
        books_before = REGISTRY.get_sample_value(
            "polymarket_books_emitted_total",
        ) or 0
        stream = PolymarketMarketDataStreamAdapter(
            contracts=tuple(market_contracts),
            max_event_age_seconds=None,
        ).stream_order_books(tuple(contract.id for contract in market_contracts))
        updates = tuple([await anext(stream) for _ in market_contracts])
        assert REGISTRY.get_sample_value("polymarket_ws_expected_sockets") == 4
        assert REGISTRY.get_sample_value("polymarket_ws_active_sockets") == 4
        assert REGISTRY.get_sample_value(
            "polymarket_ws_connections_total",
        ) == connections_before + 4
        assert REGISTRY.get_sample_value(
            "polymarket_ws_frames_received_total",
        ) == frames_before + 8
        assert REGISTRY.get_sample_value(
            "polymarket_books_emitted_total",
        ) == books_before + 8
        await stream.aclose()
        assert REGISTRY.get_sample_value("polymarket_ws_expected_sockets") == 0
        assert REGISTRY.get_sample_value("polymarket_ws_active_sockets") == 0
        return updates

    updates = asyncio.run(receive_all_books())

    assert len(connect_kwargs) == 4
    assert all(
        kwargs["max_queue"] == market_data_stream._WS_QUEUE_CAPACITY
        for kwargs in connect_kwargs
    )
    assert connected_at[-1] - connected_at[0] >= (
        2 * market_data_stream._SOCKET_START_STAGGER_SECONDS
    )
    assert len(set(connected_threads)) == 4
    assert threading.get_ident() not in connected_threads
    assert {contract_id for contract_id, _ in updates} == {
        contract.id for contract in market_contracts
    }
    assert [set(json.loads(socket.sent[0])["assets_ids"]) for socket in sockets] == (
        expected_tokens
    )


def test_cold_stream_groups_conditions_into_a_fixed_loop_pool(
    monkeypatch,
    gamma_market_payload,
):
    """Share cold event loops without combining condition sockets."""
    contracts = tuple(
        contract
        for index in range(4)
        for contract in gamma_market_to_contracts(
            {
                **gamma_market_payload,
                "conditionId": f"0xmarket{index}",
                "clobTokenIds": json.dumps((f"{index}01", f"{index}02")),
            }
        )
    )
    adapter = PolymarketMarketDataStreamAdapter(
        contracts=contracts,
        max_event_age_seconds=None,
        loop_thread_count=2,
    )
    worker_threads: list[int] = []
    worker_conditions: list[set[str]] = []

    async def fake_worker(contract_ids, *, start_delay_seconds=0):
        del start_delay_seconds
        worker_threads.append(threading.get_ident())
        worker_conditions.append(
            {
                market_data_stream.parse_polymarket_contract_id(contract_id)[0]
                for contract_id in contract_ids
            }
        )
        for contract_id in contract_ids:
            contract = adapter._contracts[contract_id]
            yield contract_id, market_data_stream.OrderBook(
                contract.market_id,
                contract.outcome_id,
                (),
                (),
            )
        await asyncio.Event().wait()

    monkeypatch.setattr(adapter, "_stream_order_books_on_worker", fake_worker)

    async def receive_all():
        stream = adapter.stream_order_books(tuple(contract.id for contract in contracts))
        updates = tuple([await anext(stream) for _ in contracts])
        await stream.aclose()
        return updates

    updates = asyncio.run(receive_all())

    assert len(set(worker_threads)) == 2
    assert sorted(map(len, worker_conditions)) == [2, 2]
    assert {contract_id for contract_id, _ in updates} == {
        contract.id for contract in contracts
    }


def test_stream_withholds_books_until_backlog_is_drained(
    monkeypatch,
    gamma_market_payload,
    clob_book_payload,
):
    """Apply bounded slices but publish only after their queue catches up."""
    monkeypatch.setattr(market_data_stream, "_MAX_BATCH_FRAMES", 2)
    contract = gamma_market_to_contracts(gamma_market_payload)[0]
    snapshot = json.dumps({**clob_book_payload, "event_type": "book"})
    first_delta = json.dumps(
        {
            "event_type": "price_change",
            "price_changes": [
                {
                    "asset_id": "123",
                    "price": "0.46",
                    "size": "7",
                    "side": "BUY",
                    "best_bid": "0.46",
                    "best_ask": "0.55",
                }
            ],
        }
    )
    final_delta = json.dumps(
        {
            "event_type": "price_change",
            "price_changes": [
                {
                    "asset_id": "123",
                    "price": "0.46",
                    "size": "9",
                    "side": "BUY",
                    "best_bid": "0.46",
                    "best_ask": "0.55",
                }
            ],
        }
    )
    websocket = _WebSocket(
        (snapshot, first_delta, final_delta),
        queue_depths=(40, 39, 0),
        block_when_exhausted=True,
    )
    monkeypatch.setattr(
        market_data_stream,
        "connect",
        lambda _url, **_kwargs: websocket,
    )
    snapshots = 0
    original_snapshot = market_data_stream._MutableOrderBook.snapshot

    def count_snapshot(book):
        nonlocal snapshots
        snapshots += 1
        return original_snapshot(book)

    monkeypatch.setattr(
        market_data_stream._MutableOrderBook,
        "snapshot",
        count_snapshot,
    )

    async def receive_burst():
        stream = PolymarketMarketDataStreamAdapter(
            contracts=(contract,),
            max_event_age_seconds=None,
        ).stream_order_book(contract.id)
        book = await anext(stream)
        await stream.aclose()
        return book

    final_book = asyncio.run(receive_burst())

    assert snapshots == 1
    assert final_book.best_bid().price.value == Decimal("0.46")
    assert final_book.best_bid().quantity.value == Decimal("9")


def test_stream_coalesces_an_idle_micro_burst(
    monkeypatch,
    gamma_market_payload,
    clob_book_payload,
):
    """Wait briefly for a second frame and publish only its final token state."""
    contract = gamma_market_to_contracts(gamma_market_payload)[0]
    snapshot = json.dumps({**clob_book_payload, "event_type": "book"})
    first_delta = json.dumps(
        {
            "event_type": "price_change",
            "price_changes": [
                {
                    "asset_id": "123",
                    "price": "0.46",
                    "size": "7",
                    "side": "BUY",
                    "best_bid": "0.46",
                    "best_ask": "0.55",
                }
            ],
        }
    )
    final_delta = json.dumps(
        {
            "event_type": "price_change",
            "price_changes": [
                {
                    "asset_id": "123",
                    "price": "0.46",
                    "size": "9",
                    "side": "BUY",
                    "best_bid": "0.46",
                    "best_ask": "0.55",
                }
            ],
        }
    )
    websocket = _WebSocket(
        (snapshot, first_delta, final_delta),
        queue_depths=(0, 0, 0),
        block_when_exhausted=True,
    )
    original_recv = websocket.recv
    received_frames = 0

    async def receive_and_enqueue_follow_up():
        nonlocal received_frames
        message = await original_recv()
        received_frames += 1
        if received_frames == 2:
            asyncio.get_running_loop().call_soon(
                setattr,
                websocket.recv_messages,
                "frames",
                (None,),
            )
        return message

    websocket.recv = receive_and_enqueue_follow_up
    monkeypatch.setattr(
        market_data_stream,
        "connect",
        lambda _url, **_kwargs: websocket,
    )
    snapshots = 0
    original_snapshot = market_data_stream._MutableOrderBook.snapshot

    def count_snapshot(book):
        nonlocal snapshots
        snapshots += 1
        return original_snapshot(book)

    monkeypatch.setattr(
        market_data_stream._MutableOrderBook,
        "snapshot",
        count_snapshot,
    )

    async def receive_burst():
        stream = PolymarketMarketDataStreamAdapter(
            contracts=(contract,),
            max_event_age_seconds=None,
        ).stream_order_book(contract.id)
        initial = await anext(stream)
        book = await anext(stream)
        await stream.aclose()
        return initial, book

    initial, book = asyncio.run(receive_burst())

    assert received_frames == 3
    assert snapshots == 2
    assert initial.best_bid().quantity.value == Decimal("3")
    assert book.best_bid().quantity.value == Decimal("9")


def test_stream_accepts_an_old_initial_snapshot_without_reconnecting(
    monkeypatch,
    gamma_market_payload,
    clob_book_payload,
):
    """Treat snapshot time as last book mutation rather than transport delay."""
    contract = gamma_market_to_contracts(gamma_market_payload)[0]
    snapshot = json.dumps(
        {
            **clob_book_payload,
            "timestamp": str(int((time.time() - 30) * 1000)),
            "event_type": "book",
        }
    )
    websocket = _WebSocket((snapshot,), block_when_exhausted=True)
    connections = 0

    def fake_connect(_url, **_kwargs):
        nonlocal connections
        connections += 1
        return websocket

    monkeypatch.setattr(market_data_stream, "connect", fake_connect)

    async def receive_snapshot():
        stream = PolymarketMarketDataStreamAdapter(
            contracts=(contract,),
        ).stream_order_book(contract.id)
        book = await anext(stream)
        assert connections == 1
        await stream.aclose()
        return book

    book = asyncio.run(receive_snapshot())

    assert not book.is_empty()
    assert book.arrival_wall_at_ns is not None
    assert book.arrival_at_ns == book.received_at_ns
    assert book.source_timestamp_kind == "snapshot_state"


def test_stream_reconnects_and_invalidates_books_on_stale_venue_timestamp(
    monkeypatch,
    gamma_market_payload,
    clob_book_payload,
):
    """Discard an affected socket rather than publishing an old queued delta."""
    contract = gamma_market_to_contracts(gamma_market_payload)[0]
    current_timestamp = str(int(time.time() * 1000))
    snapshot = json.dumps(
        {
            **clob_book_payload,
            "timestamp": current_timestamp,
            "event_type": "book",
        }
    )
    stale_delta = json.dumps(
        {
            "event_type": "price_change",
            "timestamp": str(int((time.time() - 3) * 1000)),
            "price_changes": [
                {
                    "asset_id": "123",
                    "price": "0.46",
                    "size": "7",
                    "side": "BUY",
                    "best_bid": "0.46",
                    "best_ask": "0.55",
                }
            ],
        }
    )
    release_stale = threading.Event()
    first_socket = _WebSocket(
        (snapshot, stale_delta),
        receive_gates={1: release_stale},
    )
    second_socket = _WebSocket((snapshot,), block_when_exhausted=True)
    sockets = iter((first_socket, second_socket))
    monkeypatch.setattr(
        market_data_stream,
        "connect",
        lambda _url, **_kwargs: next(sockets),
    )

    async def receive_recovery_sequence():
        stream = PolymarketMarketDataStreamAdapter(
            contracts=(contract,),
        ).stream_order_book(contract.id)
        first = await anext(stream)
        release_stale.set()
        replacement = await anext(stream)
        recovered = await anext(stream) if replacement.is_empty() else replacement
        await stream.aclose()
        return first, recovered

    first, recovered = asyncio.run(receive_recovery_sequence())

    assert not first.is_empty()
    assert not recovered.is_empty()
    assert first_socket.closed is True


def test_stream_drains_a_fresh_queue_spike_before_reconnecting(
    monkeypatch,
    gamma_market_payload,
    clob_book_payload,
):
    """Keep a socket whose fresh backlog shrinks across bounded batches."""
    drained_before = REGISTRY.get_sample_value(
        "polymarket_ws_queue_overloads_total",
        {"outcome": "drained"},
    ) or 0
    monkeypatch.setattr(market_data_stream, "_MAX_BATCH_FRAMES", 2)
    monkeypatch.setattr(market_data_stream, "_WS_RESTART_QUEUE_DEPTH", 3)
    contract = gamma_market_to_contracts(gamma_market_payload)[0]
    snapshot = json.dumps({**clob_book_payload, "event_type": "book"})
    final_delta = json.dumps(
        {
            "event_type": "price_change",
            "price_changes": [
                {
                    "asset_id": "123",
                    "price": "0.45",
                    "size": "9",
                    "side": "BUY",
                    "best_bid": "0.45",
                    "best_ask": "0.55",
                }
            ],
        }
    )
    release_backlog = threading.Event()
    websocket = _WebSocket(
        (snapshot, "PONG", "PONG", final_delta),
        queue_depths=(0, 4, 2, 0),
        block_when_exhausted=True,
        receive_gates={1: release_backlog},
    )
    connect_calls = 0

    def connect_once(_url, **_kwargs):
        nonlocal connect_calls
        connect_calls += 1
        return websocket

    monkeypatch.setattr(market_data_stream, "connect", connect_once)

    async def receive_drained_update():
        stream = PolymarketMarketDataStreamAdapter(
            contracts=(contract,),
            max_event_age_seconds=None,
        ).stream_order_book(contract.id)
        initial = await anext(stream)
        release_backlog.set()
        updated = await anext(stream)
        await stream.aclose()
        return initial, updated

    initial, updated = asyncio.run(receive_drained_update())

    assert initial.best_bid().quantity.value == Decimal("3")
    assert updated.best_bid().quantity.value == Decimal("9")
    assert connect_calls == 1
    assert REGISTRY.get_sample_value(
        "polymarket_ws_queue_overloads_total",
        {"outcome": "drained"},
    ) == drained_before + 1


def test_stream_reconnects_when_an_overloaded_queue_does_not_drain(
    monkeypatch,
    gamma_market_payload,
    clob_book_payload,
):
    """Close one market socket after repeated overloaded batches make no progress."""
    restarted_before = REGISTRY.get_sample_value(
        "polymarket_ws_queue_overloads_total",
        {"outcome": "restarted"},
    ) or 0
    monkeypatch.setattr(market_data_stream, "_MAX_BATCH_FRAMES", 2)
    monkeypatch.setattr(market_data_stream, "_WS_RESTART_QUEUE_DEPTH", 3)
    monkeypatch.setattr(market_data_stream, "_WS_STALLED_BATCH_LIMIT", 2)
    contract = gamma_market_to_contracts(gamma_market_payload)[0]
    current_timestamp = str(int(time.time() * 1000))
    snapshot = json.dumps(
        {
            **clob_book_payload,
            "timestamp": current_timestamp,
            "event_type": "book",
        }
    )
    release_backlog = threading.Event()
    first_socket = _WebSocket(
        (snapshot, "PONG", "PONG", "PONG", "PONG"),
        queue_depths=(0, 4, 4, 4, 4),
        receive_gates={1: release_backlog},
    )
    second_socket = _WebSocket((snapshot,), block_when_exhausted=True)
    sockets = iter((first_socket, second_socket))
    monkeypatch.setattr(
        market_data_stream,
        "connect",
        lambda _url, **_kwargs: next(sockets),
    )

    async def receive_recovery_sequence():
        stream = PolymarketMarketDataStreamAdapter(
            contracts=(contract,),
        ).stream_order_book(contract.id)
        first = await anext(stream)
        release_backlog.set()
        replacement = await anext(stream)
        recovered = await anext(stream) if replacement.is_empty() else replacement
        await stream.aclose()
        return first, recovered

    first, recovered = asyncio.run(receive_recovery_sequence())

    assert not first.is_empty()
    assert not recovered.is_empty()
    assert first_socket.closed is True
    assert REGISTRY.get_sample_value(
        "polymarket_ws_queue_overloads_total",
        {"outcome": "restarted"},
    ) == restarted_before + 1


def test_stream_ignores_racing_best_bid_ask_event(
    monkeypatch,
    gamma_market_payload,
    clob_book_payload,
):
    """Keep the cached book when a standalone top event races with depth updates."""
    contract = gamma_market_to_contracts(gamma_market_payload)[0]
    snapshot = json.dumps({**clob_book_payload, "event_type": "book"})
    racing_top = json.dumps(
        {
            "event_type": "best_bid_ask",
            "asset_id": "123",
            "best_bid": "0.44",
            "best_ask": "0.56",
        }
    )
    price_change = json.dumps(
        {
            "event_type": "price_change",
            "price_changes": [
                {
                    "asset_id": "123",
                    "price": "0.45",
                    "size": "7",
                    "side": "BUY",
                    "best_bid": "0.45",
                    "best_ask": "0.55",
                }
            ],
        }
    )
    release_change = threading.Event()
    websocket = _WebSocket(
        (snapshot, racing_top, price_change),
        block_when_exhausted=True,
        receive_gates={2: release_change},
    )
    monkeypatch.setattr(market_data_stream, "connect", lambda _url, **_kwargs: websocket)

    async def receive_after_racing_top():
        stream = PolymarketMarketDataStreamAdapter(
            contracts=(contract,),
            max_event_age_seconds=None,
        ).stream_order_book(contract.id)
        await anext(stream)
        release_change.set()
        updated = await anext(stream)
        await stream.aclose()
        return updated

    updated = asyncio.run(receive_after_racing_top())

    assert updated.best_bid().quantity.value == Decimal("7")
    assert len(websocket.sent) == 1


def test_stream_applies_tick_size_change_before_next_book(
    monkeypatch,
    gamma_market_payload,
    clob_book_payload,
):
    """Forward Polymarket tick changes before later books become actionable."""
    contract = gamma_market_to_contracts(gamma_market_payload)[0]
    tick_change = json.dumps(
        {
            "event_type": "tick_size_change",
            "asset_id": "123",
            "old_tick_size": "0.01",
            "new_tick_size": "0.001",
        }
    )
    snapshot = json.dumps({**clob_book_payload, "event_type": "book"})
    websocket = _WebSocket((tick_change, snapshot))
    monkeypatch.setattr(
        market_data_stream,
        "connect",
        lambda _url, **_kwargs: websocket,
    )
    changes = []

    async def receive_book():
        adapter = PolymarketMarketDataStreamAdapter(
            contracts=(contract,),
            max_event_age_seconds=None,
        )

        async def record_tick(contract_id, tick_size):
            changes.append((contract_id, tick_size))

        adapter.set_tick_size_handler(record_tick)
        stream = adapter.stream_order_book(contract.id)
        book = await anext(stream)
        await stream.aclose()
        return adapter, book

    adapter, _ = asyncio.run(receive_book())

    assert changes == [(contract.id, TickSize(Decimal("0.001")))]
    assert adapter._contracts[contract.id].tick_size == TickSize(Decimal("0.001"))


def test_stream_resynchronizes_one_book_without_reconnecting(
    monkeypatch,
    caplog,
    gamma_market_payload,
    clob_book_payload,
):
    """Invalidate one mismatched book until its replacement snapshot arrives."""
    caplog.set_level(logging.WARNING, logger="prediction_markets.events.polymarket")
    contract = gamma_market_to_contracts(gamma_market_payload)[0]
    snapshot = json.dumps({**clob_book_payload, "event_type": "book"})
    mismatch = json.dumps(
        {
            "event_type": "price_change",
            "timestamp": clob_book_payload["timestamp"],
            "price_changes": [
                {
                    "asset_id": "123",
                    "price": "0.45",
                    "size": "7",
                    "side": "BUY",
                    "best_bid": "0.46",
                    "best_ask": "0.55",
                }
            ],
        }
    )
    websocket = _WebSocket(
        (snapshot, mismatch, snapshot),
        block_when_exhausted=True,
    )
    connections = 0

    def fake_connect(_url, **_kwargs):
        nonlocal connections
        connections += 1
        return websocket

    monkeypatch.setattr(market_data_stream, "connect", fake_connect)

    async def receive_resynchronized_book():
        stream = PolymarketMarketDataStreamAdapter(
            contracts=(contract,),
            max_event_age_seconds=None,
        ).stream_order_book(contract.id)
        latest = await asyncio.wait_for(anext(stream), timeout=1)
        deadline = asyncio.get_running_loop().time() + 1
        while len(websocket.sent) < 3:
            if asyncio.get_running_loop().time() >= deadline:
                raise TimeoutError("Polymarket token resubscribe was not sent")
            await asyncio.sleep(0.001)
        await asyncio.wait_for(stream.aclose(), timeout=1)
        return latest

    latest = asyncio.run(receive_resynchronized_book())

    assert connections == 1
    assert latest.best_bid() is not None
    assert latest.best_bid().price.value == Decimal("0.45")
    assert latest.best_ask() is not None
    assert latest.best_ask().price.value == Decimal("0.55")
    assert json.loads(websocket.sent[1]) == {
        "operation": "unsubscribe",
        "assets_ids": ["123"],
    }
    assert json.loads(websocket.sent[2]) == {
        "operation": "subscribe",
        "assets_ids": ["123"],
        "initial_dump": True,
    }
    assert any(
        "desync" in message and "token 123" in message
        for message in caplog.messages
    )
    diagnostic_message = next(
        message
        for message in caplog.messages
        if "desync diagnostic" in message
    )
    diagnostic = json.loads(diagnostic_message.rsplit(" | ", 1)[-1])
    condition_id, _ = market_data_stream.parse_polymarket_contract_id(contract.id)
    assert diagnostic["condition_id"] == condition_id
    assert diagnostic["token_id"] == "123"
    assert diagnostic["queue_depth"] == 0
    assert diagnostic["change_count"] == 1
    assert diagnostic["advertised"] == {
        "best_ask": "0.55",
        "best_ask_present": True,
        "best_bid": "0.46",
        "best_bid_present": False,
    }
    assert diagnostic["local_before"]["best_bid_price"] == "0.45"
    assert diagnostic["local_after"]["best_bid"] == {
        "present": False,
        "price": "0.46",
        "quantity": None,
    }


def test_stream_reconnects_after_socket_error(
    monkeypatch,
    caplog,
    gamma_market_payload,
    clob_book_payload,
):
    caplog.set_level(logging.INFO, logger="prediction_markets.events.polymarket")
    contract = gamma_market_to_contracts(gamma_market_payload)[0]
    snapshot = json.dumps({**clob_book_payload, "event_type": "book"})
    sockets = iter(
        (
            _WebSocket(error=OSError("lost")),
            _WebSocket((snapshot,), block_when_exhausted=True),
        )
    )
    connections = 0

    def fake_connect(_url, **_kwargs):
        nonlocal connections
        connections += 1
        return next(sockets)

    monkeypatch.setattr(market_data_stream, "connect", fake_connect)
    monkeypatch.setattr(market_data_stream, "_RECONNECT_BASE_SECONDS", 0)

    async def receive_after_reconnect():
        stream = PolymarketMarketDataStreamAdapter(
            contracts=(contract,),
            max_event_age_seconds=None,
        ).stream_order_book(contract.id)
        book = await anext(stream)
        await stream.aclose()
        return book

    book = asyncio.run(receive_after_reconnect())

    assert connections == 2
    assert book.best_bid().price.value == Decimal("0.45")
    assert caplog.messages.count("WS Polymarket | connected | market 0xabc | 1 books") == 1
    assert (
        caplog.messages.count(
            "WS Polymarket | reconnected | market 0xabc | 1 books"
        )
        == 1
    )
