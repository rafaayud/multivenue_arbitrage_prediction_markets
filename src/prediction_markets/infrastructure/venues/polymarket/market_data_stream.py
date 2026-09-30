"""Integrate Polymarket market data with the domain stream port.

Responsibilities
----------------
- Isolate each binary market on its own WebSocket connection.
- Reconstruct full depth from snapshots and incremental price changes.
- Detect receive-queue pressure and stale venue events before publication.
- Log structured state only when reconstructed depth becomes inconsistent.
- Coalesce bounded transport slices into the latest normalized book per token.
"""

import asyncio
import json
import logging
import threading
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import aclosing, asynccontextmanager
from dataclasses import dataclass, replace
from decimal import Decimal, InvalidOperation
from typing import Any, Literal

from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed

from prediction_markets.domain.contracts.entities import BinaryContract
from prediction_markets.domain.contracts.value_objects import TickSize
from prediction_markets.domain.orderbook.entities import OrderBook
from prediction_markets.domain.orderbook.value_objects import OrderBookLevel
from prediction_markets.domain.ports.market_data_stream import MarketDataStreamPort
from prediction_markets.domain.shared.value_objects import (
    ContractID,
    Price,
    Quantity,
    Timestamp,
)
from prediction_markets.infrastructure.operational_metrics import (
    MARKET_FEED_WS_MESSAGE_QUEUE_WAIT,
    MARKET_FEED_WS_PAUSED,
    MARKET_FEED_WS_QUEUE_DEPTH,
    MARKET_FEED_WS_QUEUE_HIGH_WATERMARK,
    POLYMARKET_BOOKS_EMITTED,
    POLYMARKET_BRIDGE_PENDING,
    POLYMARKET_BRIDGE_REPLACEMENTS,
    POLYMARKET_WS_ACTIVE_SOCKETS,
    POLYMARKET_WS_CONNECTIONS,
    POLYMARKET_WS_DEQUEUE_TO_EMIT,
    POLYMARKET_WS_EXPECTED_SOCKETS,
    POLYMARKET_WS_FRAMES_RECEIVED,
    POLYMARKET_WS_MESSAGE_AGE,
    POLYMARKET_WS_MESSAGE_QUEUE_WAIT,
    POLYMARKET_WS_PAUSED_SOCKETS,
    POLYMARKET_WS_PROCESSING,
    POLYMARKET_WS_PROCESSING_ITEMS,
    POLYMARKET_WS_QUEUE_OVERLOADS,
    POLYMARKET_WS_QUEUE_DEPTH,
    POLYMARKET_WS_QUEUE_HIGH_WATERMARK,
    POLYMARKET_WS_RESYNCS,
    POLYMARKET_WS_VENUE_TIMESTAMP_DELTA,
)
from prediction_markets.infrastructure.venues.polymarket.mappers import (
    POLYMARKET_VENUE_ID,
    _timestamp_from_epoch,
    clob_book_to_order_book,
    parse_polymarket_contract_id,
)
from prediction_markets.infrastructure.websocket_transport import (
    SocketArrival,
    TimestampedClientConnection,
    WebSocketQueueObserver,
    WebSocketQueueSample,
    WebSocketTransition,
    WebSocketTransitionObserver,
    instrument_message_queue as instrument_transport_queue,
    socket_arrival,
    stamp_order_book,
)
from prediction_markets.infrastructure.websocket_transport import (
    transport_state,
)

_events = logging.getLogger("prediction_markets.events.polymarket")

_WS_QUEUE_CAPACITY = 256
_WS_RESTART_QUEUE_DEPTH = 192
_WS_STALLED_BATCH_LIMIT = 3
_WS_PUBLISH_QUEUE_DEPTH = 32
_MAX_BATCH_FRAMES = 32
_COALESCE_WINDOW_SECONDS = 0.005
_SOCKET_START_STAGGER_SECONDS = 0.05
_METRIC_SAMPLE_EVERY = 16
_PROCESSING_SAMPLE_EVERY = 64
_RECONNECT_BASE_SECONDS = 1.0
_RECONNECT_MAX_SECONDS = 15.0


class _OrderBookOutOfSync(Exception):
    """Signal that one token requires a fresh order-book snapshot."""


class _RestartSocket(Exception):
    """Signal that one market socket must discard its queue and reconnect."""

    def __init__(self, reason: str, detail: str) -> None:
        super().__init__(detail)
        self.reason = reason


@dataclass(frozen=True, slots=True)
class _StreamFailure:
    """Carry a worker-loop failure back to the owning event loop."""

    error: BaseException


class _StreamStopped:
    """Mark an unexpected end of the isolated worker stream."""


_STREAM_STOPPED = _StreamStopped()


@dataclass(slots=True)
class _MutableOrderBook:
    """Apply deltas without sorting and rebuilding an immutable book each time.

    Notes
    -----
    - Best prices are retained as pointers and normally updated from the top values
      included in Polymarket ``price_change`` events.
    - Full depth is sorted only when a transport burst is emitted downstream.
    """

    template: OrderBook
    bids: dict[Decimal, OrderBookLevel]
    asks: dict[Decimal, OrderBookLevel]
    best_bid_price: Decimal | None
    best_ask_price: Decimal | None

    @classmethod
    def from_order_book(cls, book: OrderBook) -> "_MutableOrderBook":
        """Create mutable depth from one complete venue snapshot."""
        best_bid = book.best_bid()
        best_ask = book.best_ask()
        return cls(
            template=book,
            bids={level.price.value: level for level in book.bids},
            asks={level.price.value: level for level in book.asks},
            best_bid_price=best_bid.price.value if best_bid is not None else None,
            best_ask_price=best_ask.price.value if best_ask is not None else None,
        )

    def apply_price_change(
        self,
        changes: tuple[dict[str, Any], ...],
        token_id: str,
        event_timestamp: Timestamp | None,
        received_at_ns: int | None = None,
        profile: dict[str, int] | None = None,
        arrival_wall_at_ns: int | None = None,
    ) -> bool:
        """Apply every matching delta and retain venue top-of-book pointers.

        Parameters
        ----------
        changes
            Deltas already grouped for the target token.
        token_id
            Token whose deltas must be applied.
        event_timestamp
            Venue timestamp parsed once for the containing event.
        received_at_ns
            Local monotonic time when the containing frame was dequeued.
        profile
            Optional sampled counters populated with transformation durations
            and item counts. Normal processing passes ``None``.

        Returns
        -------
        bool
            Whether the message contained at least one matching depth update.

        Raises
        ------
        _OrderBookOutOfSync
            If a delta is malformed or reconstructed depth disagrees with the
            venue's advertised best prices.
        """
        started_ns = time.perf_counter_ns() if profile is not None else 0
        for change in changes:
            try:
                price = Decimal(str(change["price"]))
                size = Decimal(str(change["size"]))
            except (InvalidOperation, KeyError, ValueError) as error:
                raise _OrderBookOutOfSync(
                    f"Invalid Polymarket depth change for token {token_id}",
                ) from error

            side = str(change.get("side", "")).upper()
            if side not in {"BUY", "SELL"}:
                raise _OrderBookOutOfSync(
                    f"Unknown Polymarket order-book side for token {token_id}: {side}",
                )
            levels = self.bids if side == "BUY" else self.asks

            if size <= 0:
                levels.pop(price, None)
            else:
                levels[price] = OrderBookLevel(
                    price=Price(price),
                    quantity=Quantity(size),
                )

            if side == "BUY":
                if size > 0 and (
                    self.best_bid_price is None or price > self.best_bid_price
                ):
                    self.best_bid_price = price
                elif size <= 0 and price == self.best_bid_price:
                    self.best_bid_price = None
            else:
                if size > 0 and (
                    self.best_ask_price is None or price < self.best_ask_price
                ):
                    self.best_ask_price = price
                elif size <= 0 and price == self.best_ask_price:
                    self.best_ask_price = None

        if not changes:
            return False

        if profile is not None:
            profile["apply_deltas_ns"] = profile.get("apply_deltas_ns", 0) + (
                time.perf_counter_ns() - started_ns
            )
            profile["apply_deltas_items"] = profile.get(
                "apply_deltas_items",
                0,
            ) + len(changes)

        last_change = changes[-1]
        started_ns = time.perf_counter_ns() if profile is not None else 0
        self._apply_advertised_top(last_change, token_id, profile)
        if profile is not None:
            profile["validate_top_ns"] = profile.get("validate_top_ns", 0) + (
                time.perf_counter_ns() - started_ns
            )

        started_ns = time.perf_counter_ns() if profile is not None else 0
        self.template = replace(
            self.template,
            timestamp=event_timestamp or self.template.timestamp,
            source_at_ns=(
                _timestamp_ns(event_timestamp)
                if event_timestamp is not None
                else self.template.source_at_ns
            ),
            arrival_wall_at_ns=(
                arrival_wall_at_ns
                if arrival_wall_at_ns is not None
                else self.template.arrival_wall_at_ns
            ),
            arrival_at_ns=(
                received_at_ns
                if received_at_ns is not None
                else self.template.arrival_at_ns
            ),
            source_timestamp_kind="venue_update",
            received_at_ns=(
                received_at_ns
                if received_at_ns is not None
                else self.template.received_at_ns
            ),
            source_hash=(
                str(last_change["hash"])
                if last_change.get("hash")
                else self.template.source_hash
            ),
        )
        if profile is not None:
            profile["update_metadata_ns"] = profile.get(
                "update_metadata_ns",
                0,
            ) + (time.perf_counter_ns() - started_ns)
        return True

    def snapshot(self, profile: dict[str, int] | None = None) -> OrderBook:
        """Materialize one immutable, sorted snapshot from current mutable depth.

        Parameters
        ----------
        profile
            Optional sampled counters populated with materialization duration
            and traversed depth levels.
        """
        started_ns = time.perf_counter_ns() if profile is not None else 0
        snapshot = replace(
            self.template,
            bids=tuple(
                sorted(
                    self.bids.values(),
                    key=lambda level: level.price.value,
                    reverse=True,
                )
            ),
            asks=tuple(
                sorted(self.asks.values(), key=lambda level: level.price.value)
            ),
        )
        if profile is not None:
            profile["materialize_snapshot_ns"] = time.perf_counter_ns() - started_ns
            profile["materialize_snapshot_items"] = len(self.bids) + len(self.asks)
        return snapshot

    def invalidated_snapshot(self) -> OrderBook:
        """Build an empty snapshot that immediately replaces unsafe cached depth."""
        return replace(
            self.template,
            bids=(),
            asks=(),
            arrival_wall_at_ns=None,
            arrival_at_ns=None,
            processed_at_ns=None,
            received_at_ns=None,
        )

    def _apply_advertised_top(
        self,
        change: dict[str, Any],
        token_id: str,
        profile: dict[str, int] | None = None,
    ) -> None:
        """Prune stale aggressive levels and validate venue top pointers.

        Notes
        -----
        - A delta that inserts or removes the current top updates the retained
          pointer first. Therefore, an unchanged advertised top can skip the
          full depth scan without skipping quantity updates.
        """
        if profile is not None:
            profile.setdefault("validate_top_items", 0)
        if "best_bid" in change:
            expected_bid = _top_price(change.get("best_bid"), empty_sentinel="0")
            if expected_bid is None or expected_bid != self.best_bid_price:
                if profile is not None:
                    profile["validate_top_items"] += len(self.bids)
                for price in tuple(self.bids):
                    if expected_bid is None or price > expected_bid:
                        del self.bids[price]
            self.best_bid_price = expected_bid
        elif self.best_bid_price not in self.bids:
            if profile is not None:
                profile["validate_top_items"] = profile.get(
                    "validate_top_items",
                    0,
                ) + len(self.bids)
            self.best_bid_price = max(self.bids, default=None)

        if "best_ask" in change:
            expected_ask = _top_price(change.get("best_ask"), empty_sentinel="1")
            if expected_ask is None or expected_ask != self.best_ask_price:
                if profile is not None:
                    profile["validate_top_items"] += len(self.asks)
                for price in tuple(self.asks):
                    if expected_ask is None or price < expected_ask:
                        del self.asks[price]
            self.best_ask_price = expected_ask
        elif self.best_ask_price not in self.asks:
            if profile is not None:
                profile["validate_top_items"] = profile.get(
                    "validate_top_items",
                    0,
                ) + len(self.asks)
            self.best_ask_price = min(self.asks, default=None)

        if self.best_bid_price is not None and self.best_bid_price not in self.bids:
            raise _OrderBookOutOfSync(
                f"Polymarket order book out of sync for token {token_id}: "
                f"missing best_bid={self.best_bid_price}",
            )
        if self.best_ask_price is not None and self.best_ask_price not in self.asks:
            raise _OrderBookOutOfSync(
                f"Polymarket order book out of sync for token {token_id}: "
                f"missing best_ask={self.best_ask_price}",
            )


class _LatestBookBuffer:
    """Retain at most one unpublished immutable book per contract."""

    def __init__(self) -> None:
        self._latest: dict[ContractID, OrderBook] = {}
        self._scheduled: set[ContractID] = set()
        self._ready: asyncio.Queue[ContractID] = asyncio.Queue()

    def publish(self, contract_id: ContractID, book: OrderBook) -> None:
        """Replace any unpublished book and schedule the contract once."""
        self._latest[contract_id] = book
        if contract_id not in self._scheduled:
            self._scheduled.add(contract_id)
            self._ready.put_nowait(contract_id)

    async def get(self) -> tuple[ContractID, OrderBook]:
        """Wait for and remove the latest book for one scheduled contract."""
        contract_id = await self._ready.get()
        self._scheduled.remove(contract_id)
        return contract_id, self._latest.pop(contract_id)


class _ThreadsafeLatestBookBridge:
    """Bridge worker threads while retaining one pending book per contract.

    Notes
    -----
    - Workers apply every venue delta before publishing here.
    - A fresh full snapshot may replace an unread invalidation because it already
      restores authoritative depth. Without recovery, the empty book is retained.
    """

    def __init__(self, owner_loop: asyncio.AbstractEventLoop) -> None:
        self._owner_loop = owner_loop
        self._latest: dict[ContractID, OrderBook] = {}
        self._scheduled: set[ContractID] = set()
        self._ready: asyncio.Queue[
            ContractID | _StreamFailure | _StreamStopped
        ] = asyncio.Queue()
        self._lock = threading.Lock()

    def publish_book(self, item: tuple[ContractID, OrderBook]) -> None:
        """Replace a pending snapshot and wake the owner at most once."""
        contract_id, book = item
        with self._lock:
            self._latest[contract_id] = book
            if contract_id in self._scheduled:
                POLYMARKET_BRIDGE_REPLACEMENTS.inc()
                return
            self._scheduled.add(contract_id)
            POLYMARKET_BRIDGE_PENDING.set(len(self._scheduled))
        self._owner_loop.call_soon_threadsafe(self._ready.put_nowait, contract_id)

    def publish_control(self, item: _StreamFailure | _StreamStopped) -> None:
        """Forward one worker lifecycle event without coalescing it."""
        self._owner_loop.call_soon_threadsafe(self._ready.put_nowait, item)

    async def get(
        self,
    ) -> tuple[ContractID, OrderBook] | _StreamFailure | _StreamStopped:
        """Return one control event or the latest pending contract snapshot."""
        item = await self._ready.get()
        if isinstance(item, (_StreamFailure, _StreamStopped)):
            return item
        with self._lock:
            self._scheduled.remove(item)
            book = self._latest.pop(item)
            POLYMARKET_BRIDGE_PENDING.set(len(self._scheduled))
            return item, book


class PolymarketMarketDataStreamAdapter(MarketDataStreamPort):
    """Stream CLOB depth over one independently recoverable socket per market.

    Parameters
    ----------
    contracts
        Known contracts used to preserve domain identity and trading metadata.
    websocket_url
        Polymarket CLOB market-channel endpoint.
    max_event_age_seconds
        Maximum wall-clock age accepted from ``message.timestamp``. ``None``
        disables age-based restarts for controlled tests.
    loop_thread_count
        Fixed number of worker loops. ``None`` keeps one loop per condition.

    Notes
    -----
    - One binary condition owns one socket and normally two complementary tokens.
    - Each socket is read and reconstructed on its own worker thread and event loop.
    - A socket waits up to 5 ms for an idle micro-burst, then processes at most
      32 queued frames before publishing and yielding to its worker event loop.
    - Books remain unpublished above 32 queued frames while mutable depth catches
      up. A stale event or three non-draining batches above 192 frames close only
      the affected socket.
    - Subscription generations stagger socket starts by 50 ms so refreshes do not
      deliver every market's initial snapshot burst at once.
    """

    venue_id = POLYMARKET_VENUE_ID

    def __init__(
        self,
        contracts: tuple[BinaryContract, ...] = (),
        websocket_url: str = "wss://ws-subscriptions-clob.polymarket.com/ws/market",
        *,
        max_event_age_seconds: float | None = 2.0,
        loop_thread_count: int | None = None,
    ) -> None:
        if max_event_age_seconds is not None and max_event_age_seconds <= 0:
            raise ValueError("max_event_age_seconds must be positive")
        if loop_thread_count is not None and loop_thread_count <= 0:
            raise ValueError("loop_thread_count must be positive")
        self._contracts = {contract.id: contract for contract in contracts}
        self._websocket_url = websocket_url
        self._max_event_age_seconds = max_event_age_seconds
        self._loop_thread_count = loop_thread_count
        self._metrics_lock = threading.Lock()
        self._active_socket_count = 0
        self._queue_depths: dict[str, int] = {}
        self._queue_high_watermark = 0
        self._paused_sockets: set[str] = set()
        self._transport_metric_sample_index = 0
        self._message_metric_event_types: dict[str, set[str]] = {}
        self._processing_metric_stages: dict[str, set[str]] = {}
        self._owner_loop: asyncio.AbstractEventLoop | None = None
        self._queue_observer: WebSocketQueueObserver | None = None
        self._transition_observer: WebSocketTransitionObserver | None = None
        self._tick_size_handler: (
            Callable[[ContractID, TickSize], Awaitable[None]] | None
        ) = None

    def set_transport_observers(
        self,
        queue_observer: WebSocketQueueObserver,
        transition_observer: WebSocketTransitionObserver,
    ) -> None:
        """Attach process-local aggregation hooks used by market workers.

        Parameters
        ----------
        queue_observer
            Thread-safe, non-blocking callback for sampled queue pressure.
        transition_observer
            Thread-safe, non-blocking callback for low-volume state changes.
        """
        self._queue_observer = queue_observer
        self._transition_observer = transition_observer

    def set_tick_size_handler(
        self,
        handler: Callable[[ContractID, TickSize], Awaitable[None]],
    ) -> None:
        """Register the runtime callback for live CLOB tick changes.

        Parameters
        ----------
        handler
            Async callback applied before later books become actionable.
        """
        self._tick_size_handler = handler

    async def stream_order_books(
        self,
        contract_ids: tuple[ContractID, ...],
    ) -> AsyncIterator[tuple[ContractID, OrderBook]]:
        """Bridge isolated Polymarket ingestion into the owning event loop.

        Parameters
        ----------
        contract_ids
            Polymarket contracts grouped by condition before subscription.

        Yields
        ------
        tuple[ContractID, OrderBook]
            Latest book for a token after applying one queued transport burst.

        Notes
        -----
        - The default gives each binary market a dedicated worker thread. A fixed
          pool may group low-rate conditions while retaining one socket per market.
        - Closing this generator cancels and joins every worker before returning.
        """
        if not contract_ids:
            return

        unique_ids = tuple(dict.fromkeys(contract_ids))
        market_contract_ids: dict[str, list[ContractID]] = {}
        for contract_id in unique_ids:
            condition_id, _ = parse_polymarket_contract_id(contract_id)
            market_contract_ids.setdefault(condition_id, []).append(contract_id)

        owner_loop = asyncio.get_running_loop()
        bridge = _ThreadsafeLatestBookBridge(owner_loop)
        workers: list[
            tuple[threading.Thread, threading.Event, dict[str, object]]
        ] = []
        self._owner_loop = owner_loop
        POLYMARKET_WS_EXPECTED_SOCKETS.set(len(market_contract_ids))

        async def pump_worker(
            worker_contract_ids: tuple[ContractID, ...],
            start_delay_seconds: float,
        ) -> None:
            try:
                async with aclosing(
                    self._stream_order_books_on_worker(
                        worker_contract_ids,
                        start_delay_seconds=start_delay_seconds,
                    ),
                ) as order_books:
                    async for item in order_books:
                        bridge.publish_book(item)
            except asyncio.CancelledError:
                raise
            except BaseException as error:
                bridge.publish_control(_StreamFailure(error))
            finally:
                bridge.publish_control(_STREAM_STOPPED)

        def run_worker(
            worker_contract_ids: tuple[ContractID, ...],
            start_delay_seconds: float,
            worker_ready: threading.Event,
            worker_state: dict[str, object],
        ) -> None:
            worker_loop = asyncio.new_event_loop()
            asyncio.set_event_loop(worker_loop)
            worker_task = worker_loop.create_task(
                pump_worker(worker_contract_ids, start_delay_seconds),
            )
            worker_state["loop"] = worker_loop
            worker_state["task"] = worker_task
            worker_ready.set()
            try:
                worker_loop.run_until_complete(worker_task)
            except asyncio.CancelledError:
                pass
            finally:
                worker_loop.run_until_complete(worker_loop.shutdown_asyncgens())
                worker_loop.run_until_complete(
                    worker_loop.shutdown_default_executor(),
                )
                worker_loop.close()

        try:
            conditions = tuple(market_contract_ids.items())
            thread_count = min(
                self._loop_thread_count or len(conditions),
                len(conditions),
            )
            grouped_conditions: list[list[tuple[str, list[ContractID]]]] = [
                [] for _ in range(thread_count)
            ]
            for index, condition in enumerate(conditions):
                grouped_conditions[index % thread_count].append(condition)
            for index, condition_group in enumerate(grouped_conditions):
                worker_contract_ids = tuple(
                    contract_id
                    for _, grouped_ids in condition_group
                    for contract_id in grouped_ids
                )
                worker_name = "+".join(
                    condition_id for condition_id, _ in condition_group
                )
                worker_ready = threading.Event()
                worker_state: dict[str, object] = {}
                worker = threading.Thread(
                    target=run_worker,
                    args=(
                        worker_contract_ids,
                        index * _SOCKET_START_STAGGER_SECONDS,
                        worker_ready,
                        worker_state,
                    ),
                    name=f"polymarket-market:{worker_name}",
                )
                worker.start()
                workers.append((worker, worker_ready, worker_state))

            while not all(worker_ready.is_set() for _, worker_ready, _ in workers):
                await asyncio.sleep(0)

            while True:
                item = await bridge.get()
                if isinstance(item, _StreamFailure):
                    raise item.error
                if isinstance(item, _StreamStopped):
                    raise ConnectionError("Polymarket worker stream stopped")
                yield item
        finally:
            for _, _, worker_state in workers:
                worker_loop = worker_state.get("loop")
                worker_task = worker_state.get("task")
                if (
                    isinstance(worker_loop, asyncio.AbstractEventLoop)
                    and isinstance(worker_task, asyncio.Task)
                    and not worker_task.done()
                    and not worker_loop.is_closed()
                ):
                    worker_loop.call_soon_threadsafe(worker_task.cancel)
            await asyncio.gather(
                *(asyncio.to_thread(worker.join) for worker, _, _ in workers),
            )
            if self._owner_loop is owner_loop:
                self._owner_loop = None
            POLYMARKET_WS_EXPECTED_SOCKETS.set(0)

    async def _stream_order_books_on_worker(
        self,
        contract_ids: tuple[ContractID, ...],
        *,
        start_delay_seconds: float = 0.0,
    ) -> AsyncIterator[tuple[ContractID, OrderBook]]:
        """Stream normalized books entirely on one isolated worker loop.

        Parameters
        ----------
        contract_ids
            Complementary token contracts belonging to one binary market.
        start_delay_seconds
            Delay before opening the socket, used to stagger generation refreshes.

        Yields
        ------
        tuple[ContractID, OrderBook]
            Latest reconstructed book for a changed token.
        """
        unique_ids = tuple(dict.fromkeys(contract_ids))
        if not unique_ids:
            return

        markets: dict[str, dict[str, ContractID]] = {}
        for contract_id in unique_ids:
            condition_id, token_id = parse_polymarket_contract_id(contract_id)
            markets.setdefault(condition_id, {})[token_id] = contract_id
        output = _LatestBookBuffer()
        failure: asyncio.Future[BaseException] = (
            asyncio.get_running_loop().create_future()
        )
        tasks = tuple(
            asyncio.create_task(
                self._stream_market(
                    condition_id,
                    token_to_contract_id,
                    output,
                    start_delay_seconds=(
                        start_delay_seconds
                        + index * _SOCKET_START_STAGGER_SECONDS
                    ),
                ),
                name=f"polymarket-market:{condition_id}",
            )
            for index, (condition_id, token_to_contract_id) in enumerate(
                markets.items(),
            )
        )

        def record_failure(task: asyncio.Task[None]) -> None:
            if task.cancelled() or failure.done():
                return
            failure.set_result(
                task.exception()
                or RuntimeError(f"Polymarket socket task {task.get_name()} stopped")
            )

        for task in tasks:
            task.add_done_callback(record_failure)

        pending_book: asyncio.Task[tuple[ContractID, OrderBook]] | None = None
        try:
            while True:
                pending_book = asyncio.create_task(output.get())
                done, _ = await asyncio.wait(
                    (pending_book, failure),
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if failure in done:
                    pending_book.cancel()
                    await asyncio.gather(pending_book, return_exceptions=True)
                    pending_book = None
                    raise failure.result()
                contract_id, book = pending_book.result()
                pending_book = None
                if book.received_at_ns is not None:
                    POLYMARKET_WS_DEQUEUE_TO_EMIT.observe(
                        max(0, time.monotonic_ns() - book.received_at_ns)
                        / 1_000_000_000
                    )
                POLYMARKET_BOOKS_EMITTED.inc()
                yield contract_id, book
        finally:
            if pending_book is not None:
                pending_book.cancel()
                await asyncio.gather(pending_book, return_exceptions=True)
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            if not failure.done():
                failure.cancel()
            for condition_id in markets:
                self._clear_transport_metrics(condition_id)
                self._remove_condition_metrics(condition_id)

    async def _stream_market(
        self,
        condition_id: str,
        token_to_contract_id: dict[str, ContractID],
        output: _LatestBookBuffer,
        *,
        start_delay_seconds: float = 0.0,
    ) -> None:
        """Keep one market socket alive and restart it without affecting peers.

        Parameters
        ----------
        condition_id
            Binary market assigned to the socket.
        token_to_contract_id
            Complementary venue tokens translated to domain contracts.
        output
            Latest-book buffer shared by this subscription generation.
        start_delay_seconds
            Initial delay used to spread simultaneous subscription bursts.
        """
        if start_delay_seconds > 0:
            await asyncio.sleep(start_delay_seconds)
        reconnecting = False
        reconnect_delay = _RECONNECT_BASE_SECONDS
        processing_sample_index = 0
        snapshot_sample_index = 0

        while True:
            current_books: dict[ContractID, _MutableOrderBook] = {}
            try:
                async with self._connect() as websocket:
                    if not _instrument_message_queue(
                        websocket,
                        condition_id,
                        self._queue_observer,
                    ):
                        _events.warning(
                            "WS Polymarket | queue timing unavailable | market %s",
                            condition_id,
                        )
                    await websocket.send(
                        json.dumps(
                            {
                                "type": "market",
                                "assets_ids": list(token_to_contract_id),
                                "custom_feature_enabled": True,
                            }
                        )
                    )
                    heartbeat = asyncio.create_task(
                        _send_heartbeats(websocket),
                        name=f"polymarket-heartbeat:{condition_id}",
                    )
                    _events.log(
                        logging.WARNING if reconnecting else logging.INFO,
                        "WS Polymarket | %s | market %s | %s books",
                        "reconnected" if reconnecting else "connected",
                        condition_id,
                        len(token_to_contract_id),
                    )
                    reconnecting = True
                    reconnect_delay = _RECONNECT_BASE_SECONDS
                    pending_contracts: set[ContractID] = set()
                    stalled_overload_batches = 0
                    overload_active = False

                    try:
                        while True:
                            coalescible_change = False
                            batch_initial_queue_depth: int | None = None
                            for frame_index in range(_MAX_BATCH_FRAMES):
                                raw_message = await _receive_frame(websocket)
                                POLYMARKET_WS_FRAMES_RECEIVED.inc()
                                if heartbeat.done():
                                    heartbeat.result()
                                arrival = socket_arrival(websocket)
                                received_at_ns = arrival.monotonic_ns
                                queue_depth, paused = self._observe_transport(
                                    condition_id,
                                    websocket,
                                )
                                if batch_initial_queue_depth is None:
                                    batch_initial_queue_depth = queue_depth
                                queue_overloaded = (
                                    queue_depth >= _WS_RESTART_QUEUE_DEPTH
                                )
                                if queue_overloaded and not overload_active:
                                    self._notify_transport_transition(
                                        condition_id,
                                        "overload",
                                        "started",
                                    )
                                overload_active = overload_active or queue_overloaded

                                processing_sample_index += 1
                                profile = (
                                    {}
                                    if processing_sample_index
                                    % _PROCESSING_SAMPLE_EVERY
                                    == 0
                                    else None
                                )
                                frame_started_ns = (
                                    time.perf_counter_ns()
                                    if profile is not None
                                    else 0
                                )
                                started_ns = frame_started_ns
                                messages = _decode_messages(raw_message)
                                if profile is not None:
                                    profile["decode_json_ns"] = (
                                        time.perf_counter_ns() - started_ns
                                    )
                                    profile["decode_json_items"] = len(messages)

                                for message in messages:
                                    event_timestamp = self._reject_stale_message(
                                        condition_id,
                                        message,
                                        arrival.wall_ns,
                                    )
                                    message_changes = await self._apply_message(
                                        websocket,
                                        condition_id,
                                        token_to_contract_id,
                                        current_books,
                                        output,
                                        message,
                                        event_timestamp,
                                        received_at_ns,
                                        profile,
                                        arrival.wall_ns,
                                    )
                                    coalescible_change = coalescible_change or (
                                        message.get("event_type") == "price_change"
                                        and bool(message_changes)
                                    )
                                    pending_contracts.update(message_changes)
                                if profile is not None:
                                    profile["process_frame_total_ns"] = (
                                        time.perf_counter_ns() - frame_started_ns
                                    )
                                    profile["process_frame_total_items"] = len(messages)
                                    self._observe_processing_profile(
                                        condition_id,
                                        profile,
                                    )
                                if (
                                    frame_index == 0
                                    and coalescible_change
                                    and queue_depth == 0
                                    and not paused
                                ):
                                    await asyncio.sleep(_COALESCE_WINDOW_SECONDS)
                                    queue_depth, paused = self._observe_transport(
                                        condition_id,
                                        websocket,
                                    )
                                    queue_overloaded = (
                                        queue_depth >= _WS_RESTART_QUEUE_DEPTH
                                    )
                                    if queue_overloaded and not overload_active:
                                        self._notify_transport_transition(
                                            condition_id,
                                            "overload",
                                            "started",
                                        )
                                    overload_active = (
                                        overload_active or queue_overloaded
                                    )
                                if queue_depth == 0 and not paused:
                                    break

                            if queue_depth >= _WS_RESTART_QUEUE_DEPTH:
                                if (
                                    batch_initial_queue_depth is not None
                                    and queue_depth >= batch_initial_queue_depth
                                ):
                                    stalled_overload_batches += 1
                                else:
                                    stalled_overload_batches = 0
                            else:
                                stalled_overload_batches = 0
                            if stalled_overload_batches >= _WS_STALLED_BATCH_LIMIT:
                                POLYMARKET_WS_QUEUE_OVERLOADS.labels(
                                    "restarted"
                                ).inc()
                                self._notify_transport_transition(
                                    condition_id,
                                    "overload",
                                    "restarted",
                                )
                                raise _RestartSocket(
                                    "queue_depth",
                                    "receive queue failed to drain for "
                                    f"{stalled_overload_batches} batches at "
                                    f"{queue_depth} frames",
                                )
                            if (
                                overload_active
                                and queue_depth < _WS_RESTART_QUEUE_DEPTH
                            ):
                                POLYMARKET_WS_QUEUE_OVERLOADS.labels("drained").inc()
                                self._notify_transport_transition(
                                    condition_id,
                                    "overload",
                                    "cleared",
                                )
                                overload_active = False

                            if (
                                queue_depth <= _WS_PUBLISH_QUEUE_DEPTH
                                and not paused
                            ):
                                for contract_id in pending_contracts:
                                    current = current_books.get(contract_id)
                                    if current is not None:
                                        snapshot_sample_index += 1
                                        snapshot_profile = (
                                            {}
                                            if snapshot_sample_index
                                            % _PROCESSING_SAMPLE_EVERY
                                            == 0
                                            else None
                                        )
                                        snapshot = (
                                            current.snapshot(snapshot_profile)
                                            if snapshot_profile is not None
                                            else current.snapshot()
                                        )
                                        output.publish(contract_id, snapshot)
                                        if snapshot_profile is not None:
                                            self._observe_processing_profile(
                                                condition_id,
                                                snapshot_profile,
                                            )
                                pending_contracts.clear()
                            await asyncio.sleep(0)
                    finally:
                        heartbeat.cancel()
                        await asyncio.gather(heartbeat, return_exceptions=True)

                raise ConnectionError("Polymarket WebSocket closed")
            except _RestartSocket as error:
                POLYMARKET_WS_RESYNCS.labels(error.reason).inc()
                self._notify_transport_transition(
                    condition_id,
                    "resync",
                    error.reason,
                )
                self._invalidate_books(current_books, output)
                self._clear_transport_metrics(condition_id)
                _events.warning(
                    "WS Polymarket | lag | market %s | reconnecting: %s",
                    condition_id,
                    error,
                )
                await asyncio.sleep(0)
            except (ConnectionClosed, OSError) as error:
                self._invalidate_books(current_books, output)
                self._clear_transport_metrics(condition_id)
                _events.warning(
                    "WS Polymarket | disconnected | market %s: %s",
                    condition_id,
                    error,
                )
                await asyncio.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 2, _RECONNECT_MAX_SECONDS)

    @asynccontextmanager
    async def _connect(self) -> AsyncIterator[Any]:
        """Track every live public socket until its context closes."""
        async with connect(
            self._websocket_url,
            ping_interval=10,
            ping_timeout=10,
            max_queue=_WS_QUEUE_CAPACITY,
            create_connection=TimestampedClientConnection,
        ) as websocket:
            with self._metrics_lock:
                self._active_socket_count += 1
                POLYMARKET_WS_ACTIVE_SOCKETS.set(self._active_socket_count)
            POLYMARKET_WS_CONNECTIONS.inc()
            try:
                yield websocket
            finally:
                with self._metrics_lock:
                    self._active_socket_count -= 1
                    POLYMARKET_WS_ACTIVE_SOCKETS.set(self._active_socket_count)

    async def _apply_message(
        self,
        websocket: Any,
        condition_id: str,
        token_to_contract_id: dict[str, ContractID],
        current_books: dict[ContractID, _MutableOrderBook],
        output: _LatestBookBuffer,
        message: dict[str, Any],
        event_timestamp: Timestamp | None,
        received_at_ns: int,
        profile: dict[str, int] | None = None,
        arrival_wall_at_ns: int | None = None,
    ) -> set[ContractID]:
        """Apply one decoded message and return contracts changed by its depth."""
        event_type = message.get("event_type")
        changed: set[ContractID] = set()

        if event_type == "book":
            token_id = str(message.get("asset_id"))
            contract_id = token_to_contract_id.get(token_id)
            if contract_id is None:
                return changed
            started_ns = time.perf_counter_ns() if profile is not None else 0
            book = stamp_order_book(
                clob_book_to_order_book(
                    message,
                    contract=self._contracts.get(contract_id),
                    contract_id=contract_id,
                ),
                SocketArrival(
                    arrival_wall_at_ns or time.time_ns(),
                    received_at_ns,
                ),
                source_timestamp_kind="snapshot_state",
            )
            if profile is not None:
                profile["map_snapshot_ns"] = profile.get("map_snapshot_ns", 0) + (
                    time.perf_counter_ns() - started_ns
                )
                profile["map_snapshot_items"] = profile.get(
                    "map_snapshot_items",
                    0,
                ) + len(book.bids) + len(book.asks)
            current_books[contract_id] = _MutableOrderBook.from_order_book(book)
            changed.add(contract_id)
            return changed

        if event_type == "price_change":
            started_ns = time.perf_counter_ns() if profile is not None else 0
            changes_by_token: dict[str, list[dict[str, Any]]] = {}
            for change in message.get("price_changes", []):
                if not isinstance(change, dict):
                    continue
                token_id = str(change.get("asset_id"))
                if token_id in token_to_contract_id:
                    changes_by_token.setdefault(token_id, []).append(change)
            if profile is not None:
                profile["group_changes_ns"] = profile.get("group_changes_ns", 0) + (
                    time.perf_counter_ns() - started_ns
                )
                profile["group_changes_items"] = profile.get(
                    "group_changes_items",
                    0,
                ) + sum(len(changes) for changes in changes_by_token.values())
            for token_id, token_changes in changes_by_token.items():
                contract_id = token_to_contract_id.get(token_id)
                if contract_id is None:
                    continue
                current = current_books.get(contract_id)
                if current is None:
                    continue
                before_best_bid = current.best_bid_price
                before_best_ask = current.best_ask_price
                try:
                    applied = current.apply_price_change(
                        tuple(token_changes),
                        token_id,
                        event_timestamp,
                        received_at_ns,
                        profile,
                        arrival_wall_at_ns,
                    )
                except _OrderBookOutOfSync as error:
                    queue_depth, paused = _websocket_transport_state(websocket)
                    _events.warning(
                        "WS Polymarket | desync diagnostic | %s",
                        json.dumps(
                            _desync_diagnostic(
                                condition_id=condition_id,
                                token_id=token_id,
                                message=message,
                                token_changes=token_changes,
                                current=current,
                                before_best_bid=before_best_bid,
                                before_best_ask=before_best_ask,
                                event_timestamp=event_timestamp,
                                received_at_ns=received_at_ns,
                                queue_depth=queue_depth,
                                paused=paused,
                                error=error,
                            ),
                            separators=(",", ":"),
                            sort_keys=True,
                        ),
                    )
                    output.publish(contract_id, current.invalidated_snapshot())
                    current_books.pop(contract_id, None)
                    POLYMARKET_WS_RESYNCS.labels("desync").inc()
                    await _resubscribe_token(websocket, token_id, error)
                    continue
                if applied:
                    changed.add(contract_id)
            return changed

        if event_type == "tick_size_change":
            await self._apply_tick_size_change(message, token_to_contract_id)
        return changed

    async def _apply_tick_size_change(
        self,
        message: dict[str, Any],
        token_to_contract_id: dict[str, ContractID],
    ) -> None:
        """Update known contract metadata before subsequent books are emitted."""
        token_id = str(message.get("asset_id"))
        contract_id = token_to_contract_id.get(token_id)
        if contract_id is None:
            return
        try:
            tick_size = TickSize(Decimal(str(message["new_tick_size"])))
        except (InvalidOperation, KeyError, ValueError):
            _events.warning(
                "Invalid Polymarket tick change for token %s: %r",
                token_id,
                message.get("new_tick_size"),
            )
            return
        contract = self._contracts.get(contract_id)
        if contract is not None:
            self._contracts[contract_id] = replace(contract, tick_size=tick_size)
        if self._tick_size_handler is not None:
            owner_loop = self._owner_loop
            if owner_loop is None or owner_loop is asyncio.get_running_loop():
                await self._tick_size_handler(contract_id, tick_size)
            else:
                callback = asyncio.run_coroutine_threadsafe(
                    self._tick_size_handler(contract_id, tick_size),
                    owner_loop,
                )
                await asyncio.wrap_future(callback)

    def _reject_stale_message(
        self,
        condition_id: str,
        message: dict[str, Any],
        arrival_wall_at_ns: int | None = None,
    ) -> Timestamp | None:
        """Measure venue age and reject delayed incremental depth changes.

        Parameters
        ----------
        condition_id
            Binary market owning the socket that delivered the message.
        message
            Decoded Polymarket event carrying an optional venue timestamp.

        Returns
        -------
        Timestamp | None
            Parsed venue timestamp reused by order-book reconstruction.

        Notes
        -----
        - A full ``book`` timestamp identifies its last venue mutation and may be
          old even when the subscription snapshot arrived immediately.
        """
        try:
            timestamp = _timestamp_from_epoch(message.get("timestamp"))
        except (InvalidOperation, OSError, OverflowError, ValueError) as error:
            raise _RestartSocket(
                "message_timestamp",
                f"invalid venue timestamp {message.get('timestamp')!r}",
            ) from error
        if timestamp is None:
            return None
        event_type = str(message.get("event_type") or "unknown")
        observed_wall_ns = arrival_wall_at_ns or time.time_ns()
        timestamp_delta = (
            observed_wall_ns - _timestamp_ns(timestamp)
        ) / 1_000_000_000
        age_seconds = max(0.0, timestamp_delta)
        stale = (
            message.get("event_type") == "price_change"
            and self._max_event_age_seconds is not None
            and age_seconds > self._max_event_age_seconds
        )
        POLYMARKET_WS_VENUE_TIMESTAMP_DELTA.labels(event_type).set(
            timestamp_delta,
        )
        POLYMARKET_WS_MESSAGE_AGE.labels(condition_id, event_type).observe(
            age_seconds,
        )
        self._message_metric_event_types.setdefault(condition_id, set()).add(
            event_type,
        )
        if stale:
            raise _RestartSocket(
                "message_age",
                f"venue event was {age_seconds:.3f}s old",
            )
        return timestamp

    def _observe_transport(self, condition_id: str, websocket: Any) -> tuple[int, bool]:
        """Record current queue depth and paused receive transports."""
        queue_depth, paused = _websocket_transport_state(websocket)
        sample: WebSocketQueueSample | None = None
        pause_transition: WebSocketTransition | None = None
        with self._metrics_lock:
            was_paused = condition_id in self._paused_sockets
            self._queue_depths[condition_id] = queue_depth
            self._queue_high_watermark = max(self._queue_high_watermark, queue_depth)
            if paused:
                self._paused_sockets.add(condition_id)
            else:
                self._paused_sockets.discard(condition_id)
            self._transport_metric_sample_index += 1
            if (
                self._transport_metric_sample_index % _METRIC_SAMPLE_EVERY == 0
                or queue_depth >= _WS_RESTART_QUEUE_DEPTH
                or paused
            ):
                self._publish_transport_metrics_locked()
                sample = WebSocketQueueSample(
                    str(self.venue_id),
                    condition_id,
                    queue_depth,
                    self._queue_high_watermark,
                    paused,
                )
            if paused != was_paused:
                pause_transition = WebSocketTransition(
                    str(self.venue_id),
                    condition_id,
                    "pause",
                    "started" if paused else "cleared",
                )
        if sample is not None and self._queue_observer is not None:
            self._queue_observer(sample)
        if pause_transition is not None and self._transition_observer is not None:
            self._transition_observer(pause_transition)
        return queue_depth, paused

    def _notify_transport_transition(
        self,
        stream_id: str,
        kind: Literal["overload", "resync"],
        state: str,
    ) -> None:
        """Forward a low-volume transport transition immediately."""
        if self._transition_observer is not None:
            self._transition_observer(
                WebSocketTransition(
                    str(self.venue_id),
                    stream_id,
                    kind,
                    state,
                )
            )

    def _publish_transport_metrics(self) -> None:
        """Publish sampled aggregate queue state without burdening every frame."""
        with self._metrics_lock:
            self._publish_transport_metrics_locked()

    def _publish_transport_metrics_locked(self) -> None:
        """Publish aggregate queue state while the metrics lock is held."""
        queue_depth = max(self._queue_depths.values(), default=0)
        paused_sockets = len(self._paused_sockets)
        POLYMARKET_WS_QUEUE_DEPTH.set(queue_depth)
        POLYMARKET_WS_QUEUE_HIGH_WATERMARK.set(self._queue_high_watermark)
        POLYMARKET_WS_PAUSED_SOCKETS.set(paused_sockets)
        venue = str(self.venue_id)
        MARKET_FEED_WS_QUEUE_DEPTH.labels(venue).set(queue_depth)
        MARKET_FEED_WS_QUEUE_HIGH_WATERMARK.labels(venue).set(
            self._queue_high_watermark,
        )
        MARKET_FEED_WS_PAUSED.labels(venue).set(1 if paused_sockets else 0)

    def _clear_transport_metrics(self, condition_id: str) -> None:
        """Remove a disconnected market from current transport gauges."""
        with self._metrics_lock:
            self._queue_depths.pop(condition_id, None)
            self._paused_sockets.discard(condition_id)
            self._publish_transport_metrics_locked()
            high_watermark = self._queue_high_watermark
        if self._queue_observer is not None:
            self._queue_observer(
                WebSocketQueueSample(
                    str(self.venue_id),
                    condition_id,
                    0,
                    high_watermark,
                    False,
                    removed=True,
                )
            )

    def _remove_condition_metrics(self, condition_id: str) -> None:
        """Remove completed rotating-market labels from process metrics."""
        for event_type in self._message_metric_event_types.pop(condition_id, set()):
            POLYMARKET_WS_MESSAGE_AGE.remove(condition_id, event_type)
        POLYMARKET_WS_MESSAGE_QUEUE_WAIT.remove(condition_id)
        for stage in self._processing_metric_stages.pop(condition_id, set()):
            POLYMARKET_WS_PROCESSING.remove(condition_id, stage)
            POLYMARKET_WS_PROCESSING_ITEMS.remove(condition_id, stage)

    def _observe_processing_profile(
        self,
        condition_id: str,
        profile: dict[str, int],
    ) -> None:
        """Publish one sampled worker profile outside unsampled hot-path work."""
        stages = self._processing_metric_stages.setdefault(condition_id, set())
        for key, value in profile.items():
            if key.endswith("_ns"):
                stage = key.removesuffix("_ns")
                POLYMARKET_WS_PROCESSING.labels(condition_id, stage).observe(
                    value / 1_000_000_000,
                )
            elif key.endswith("_items"):
                stage = key.removesuffix("_items")
                POLYMARKET_WS_PROCESSING_ITEMS.labels(condition_id, stage).observe(
                    value,
                )
            else:
                continue
            stages.add(stage)

    @staticmethod
    def _invalidate_books(
        current_books: dict[ContractID, _MutableOrderBook],
        output: _LatestBookBuffer,
    ) -> None:
        """Replace cached market depth with empty books before reconnecting."""
        for contract_id, current in current_books.items():
            output.publish(contract_id, current.invalidated_snapshot())
        current_books.clear()

    def add_contracts(self, contracts: tuple[BinaryContract, ...]) -> None:
        """Register contracts used to translate subsequent venue updates."""
        for contract in contracts:
            self._contracts[contract.id] = contract

    @staticmethod
    def _apply_price_change(
        current_book: OrderBook | None,
        message: dict[str, Any],
        token_id: str,
    ) -> OrderBook | None:
        """Apply a delta batch using the mutable runtime reconstruction logic.

        Returns
        -------
        OrderBook | None
            Updated immutable book, or ``None`` without matching token deltas.

        Raises
        ------
        _OrderBookOutOfSync
            If reconstructed depth disagrees with advertised venue top values.
        """
        if current_book is None:
            return None
        changes = tuple(
            change
            for change in message.get("price_changes", [])
            if isinstance(change, dict) and str(change.get("asset_id")) == token_id
        )
        mutable = _MutableOrderBook.from_order_book(current_book)
        return (
            mutable.snapshot()
            if mutable.apply_price_change(
                changes,
                token_id,
                _timestamp_from_epoch(message.get("timestamp")),
            )
            else None
        )


def _decode_messages(raw_message: str | bytes) -> tuple[dict[str, Any], ...]:
    """Decode one transport frame into valid JSON message objects."""
    if raw_message in {"PONG", b"PONG"}:
        return ()
    data = json.loads(raw_message)
    if isinstance(data, list):
        return tuple(item for item in data if isinstance(item, dict))
    if isinstance(data, dict):
        return (data,)
    return ()


async def _receive_frame(websocket: Any) -> str | bytes:
    """Receive one frame and normalize scripted iterator exhaustion to a close."""
    try:
        return await websocket.recv()
    except StopAsyncIteration as error:
        raise ConnectionError("Polymarket WebSocket closed") from error


def _timestamp_ns(timestamp: Timestamp) -> int:
    """Convert one venue wall-clock timestamp to integer nanoseconds."""
    return int(timestamp.value.timestamp() * 1_000_000) * 1_000


def _instrument_message_queue(
    websocket: Any,
    condition_id: str,
    queue_observer: WebSocketQueueObserver | None = None,
) -> bool:
    """Measure message wait inside websockets' local receive assembler.

    Parameters
    ----------
    websocket
        Live websockets 15 client connection.
    condition_id
        Binary market used to identify the independently queued socket.
    queue_observer
        Optional non-blocking callback for worker-local aggregation.

    Returns
    -------
    bool
        Whether the installed websockets internals support the timing hook.

    Notes
    -----
    - The hook delegates queue behavior to websockets and records only monotonic
      timestamps. It doesn't measure time spent in the kernel or on the network.
    - Every frame is timed; source age is observed after the adapter decodes it.
    """
    def observe(queue_wait: float) -> None:
        POLYMARKET_WS_MESSAGE_QUEUE_WAIT.labels(condition_id).observe(queue_wait)
        MARKET_FEED_WS_MESSAGE_QUEUE_WAIT.labels(
            str(POLYMARKET_VENUE_ID),
        ).observe(queue_wait)
        if queue_observer is not None:
            queue_observer(
                WebSocketQueueSample(
                    str(POLYMARKET_VENUE_ID),
                    condition_id,
                    queue_wait_seconds=queue_wait,
                )
            )

    return instrument_transport_queue(
        websocket,
        observe,
    )


def _websocket_transport_state(websocket: Any) -> tuple[int, bool]:
    """Read websockets' bounded receive queue without depending on it for correctness."""
    return transport_state(websocket)


def _top_price(value: Any, *, empty_sentinel: str) -> Decimal | None:
    """Normalize one advertised top price, including terminal sentinels."""
    if value in {None, ""}:
        return None
    try:
        price = Decimal(str(value))
    except (InvalidOperation, ValueError) as error:
        raise _OrderBookOutOfSync(f"Invalid Polymarket top price: {value!r}") from error
    return None if price == Decimal(empty_sentinel) else price


def _mutable_book_diagnostic(book: _MutableOrderBook) -> dict[str, Any]:
    """Capture compact mutable depth state for a desynchronization report."""

    def top_state(
        levels: dict[Decimal, OrderBookLevel],
        price: Decimal | None,
    ) -> dict[str, Any]:
        level = levels.get(price) if price is not None else None
        return {
            "price": str(price) if price is not None else None,
            "quantity": str(level.quantity.value) if level is not None else None,
            "present": price is None or level is not None,
        }

    return {
        "best_bid": top_state(book.bids, book.best_bid_price),
        "best_ask": top_state(book.asks, book.best_ask_price),
        "bid_levels": len(book.bids),
        "ask_levels": len(book.asks),
        "book_timestamp": (
            book.template.timestamp.value.isoformat()
            if book.template.timestamp is not None
            else None
        ),
        "source_hash": book.template.source_hash,
    }


def _advertised_level_present(
    levels: dict[Decimal, OrderBookLevel],
    value: Any,
    *,
    empty_sentinel: str,
) -> bool | None:
    """Check diagnostic depth presence without masking the original error."""
    if value in {None, ""}:
        return None
    try:
        price = _top_price(value, empty_sentinel=empty_sentinel)
    except _OrderBookOutOfSync:
        return None
    return not levels if price is None else price in levels


def _desync_diagnostic(
    *,
    condition_id: str,
    token_id: str,
    message: dict[str, Any],
    token_changes: list[dict[str, Any]],
    current: _MutableOrderBook,
    before_best_bid: Decimal | None,
    before_best_ask: Decimal | None,
    event_timestamp: Timestamp | None,
    received_at_ns: int,
    queue_depth: int,
    paused: bool,
    error: _OrderBookOutOfSync,
) -> dict[str, Any]:
    """Build a bounded JSON-safe report after depth reconstruction fails.

    Notes
    -----
    - The report is created only on the desynchronization path.
    - At most eight token changes are retained to bound container log size.
    """
    last_change = token_changes[-1] if token_changes else {}
    fields = ("price", "size", "side", "best_bid", "best_ask", "hash")
    changes = [
        {field: change[field] for field in fields if field in change}
        for change in token_changes[-8:]
    ]
    event_age_ms = (
        max(0.0, time.time() - event_timestamp.value.timestamp()) * 1_000
        if event_timestamp is not None
        else None
    )
    return {
        "condition_id": condition_id,
        "token_id": token_id,
        "error": str(error),
        "event_type": message.get("event_type"),
        "event_timestamp": message.get("timestamp"),
        "event_age_ms": round(event_age_ms, 3) if event_age_ms is not None else None,
        "adapter_elapsed_ms": round(
            max(0, time.monotonic_ns() - received_at_ns) / 1_000_000,
            3,
        ),
        "queue_depth": queue_depth,
        "paused": paused,
        "change_count": len(token_changes),
        "changes": changes,
        "advertised": {
            "best_bid": last_change.get("best_bid"),
            "best_ask": last_change.get("best_ask"),
            "best_bid_present": _advertised_level_present(
                current.bids,
                last_change.get("best_bid"),
                empty_sentinel="0",
            ),
            "best_ask_present": _advertised_level_present(
                current.asks,
                last_change.get("best_ask"),
                empty_sentinel="1",
            ),
        },
        "local_before": {
            "best_bid_price": (
                str(before_best_bid) if before_best_bid is not None else None
            ),
            "best_ask_price": (
                str(before_best_ask) if before_best_ask is not None else None
            ),
        },
        "local_after": _mutable_book_diagnostic(current),
    }


async def _send_heartbeats(websocket: Any) -> None:
    """Send the application-level heartbeat required by Polymarket."""
    while True:
        await asyncio.sleep(10)
        await websocket.send("PING")


async def _resubscribe_token(
    websocket: Any,
    token_id: str,
    error: _OrderBookOutOfSync,
) -> None:
    """Request a fresh snapshot for one token without reconnecting its socket."""
    _events.warning(
        "WS Polymarket | desync | token %s | resynchronizing: %s",
        token_id,
        error,
    )
    await websocket.send(
        json.dumps({"operation": "unsubscribe", "assets_ids": [token_id]})
    )
    await websocket.send(
        json.dumps(
            {
                "operation": "subscribe",
                "assets_ids": [token_id],
                "initial_dump": True,
            }
        )
    )


def _require_matching_top(
    order_book: OrderBook,
    message: dict[str, Any],
    token_id: str,
) -> None:
    """Reject local depth when it disagrees with venue top-of-book values.

    Raises
    ------
    _OrderBookOutOfSync
        If an advertised best bid or ask differs from reconstructed depth.
    """
    actual = {
        "best_bid": (
            order_book.best_bid().price.value if order_book.best_bid() else None
        ),
        "best_ask": (
            order_book.best_ask().price.value if order_book.best_ask() else None
        ),
    }
    sentinels = {"best_bid": "0", "best_ask": "1"}
    for field, local_value in actual.items():
        if field not in message:
            continue
        expected = _top_price(message.get(field), empty_sentinel=sentinels[field])
        if expected != local_value:
            raise _OrderBookOutOfSync(
                f"Polymarket order book out of sync for token {token_id}: "
                f"{field}={local_value}, venue={expected}",
            )
