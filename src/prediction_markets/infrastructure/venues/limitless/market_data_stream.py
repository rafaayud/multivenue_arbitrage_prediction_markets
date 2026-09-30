"""Integrate limitless market data stream with domain ports.

Responsibilities
----------------
- Perform venue I/O and translate external data into domain models.
"""

import json
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

from prediction_markets.domain.contracts.entities import BinaryContract
from prediction_markets.domain.orderbook.entities import OrderBook
from prediction_markets.domain.ports.market_data_stream import MarketDataStreamPort
from prediction_markets.domain.shared.value_objects import ContractID
from prediction_markets.infrastructure.operational_metrics import (
    MARKET_FEED_WS_MESSAGE_QUEUE_WAIT,
    MARKET_FEED_WS_PAUSED,
    MARKET_FEED_WS_QUEUE_DEPTH,
    MARKET_FEED_WS_QUEUE_HIGH_WATERMARK,
)
from tenacity import AsyncRetrying, retry_if_exception_type, wait_exponential
from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed

from prediction_markets.infrastructure.venues.limitless.mappers import (
    LIMITLESS_VENUE_ID,
    limitless_orderbook_to_order_book,
    parse_limitless_contract_id,
)
from prediction_markets.infrastructure.websocket_transport import (
    TimestampedClientConnection,
    WebSocketQueueObserver,
    WebSocketQueueSample,
    WebSocketTransition,
    WebSocketTransitionObserver,
    instrument_message_queue,
    socket_arrival,
    stamp_order_book,
    transport_state,
)

_NAMESPACE = "/markets"
_METRIC_SAMPLE_EVERY = 16
_events = logging.getLogger("prediction_markets.events.limitless")


class LimitlessMarketDataStreamAdapter(MarketDataStreamPort):
    """Public Limitless CLOB stream using its Socket.IO WebSocket transport."""

    venue_id = LIMITLESS_VENUE_ID

    def __init__(
        self,
        contracts: tuple[BinaryContract, ...] = (),
        websocket_url: str = (
            "wss://ws.limitless.exchange/socket.io/?EIO=4&transport=websocket"
        ),
    ) -> None:
        self._contracts = {contract.id: contract for contract in contracts}
        self._websocket_url = websocket_url
        self._queue_high_watermark = 0
        self._transport_metric_sample_index = 0
        self._queue_observer: WebSocketQueueObserver | None = None
        self._transition_observer: WebSocketTransitionObserver | None = None
        self._last_paused = False

    def set_transport_observers(
        self,
        queue_observer: WebSocketQueueObserver,
        transition_observer: WebSocketTransitionObserver,
    ) -> None:
        """Attach process-local aggregation hooks used by market workers.

        Parameters
        ----------
        queue_observer
            Non-blocking callback for sampled queue pressure and wait time.
        transition_observer
            Non-blocking callback for low-volume transport state changes.
        """
        self._queue_observer = queue_observer
        self._transition_observer = transition_observer

    async def stream_order_books(
        self,
        contract_ids: tuple[ContractID, ...],
    ) -> AsyncIterator[tuple[ContractID, OrderBook]]:
        """Stream normalized order books from limitless's live transport.

        Yields
        ------
        OrderBook
            Valid snapshots for the requested contracts.
        """
        contract_ids = tuple(dict.fromkeys(contract_ids))
        if not contract_ids:
            return

        slug_to_contract_ids: dict[str, list[ContractID]] = {}
        for contract_id in contract_ids:
            slug, _ = parse_limitless_contract_id(contract_id)
            slug_to_contract_ids.setdefault(slug, []).append(contract_id)

        async for attempt in AsyncRetrying(
            retry=retry_if_exception_type((ConnectionClosed, OSError)),
            wait=wait_exponential(multiplier=1, min=4, max=15),
            reraise=True,
        ):
            with attempt:
                namespace_requested = False
                subscribed = False
                async with self._tracked_connection() as websocket:
                    async for raw_message in websocket:
                        arrival = socket_arrival(websocket)
                        self._observe_transport(websocket)
                        message = _as_text(raw_message)
                        if message.startswith("0") and not namespace_requested:
                            await websocket.send(_namespace_connect_message())
                            namespace_requested = True
                            continue
                        if message == "2":
                            await websocket.send("3")
                            continue

                        if _is_namespace_connected(message) and not subscribed:
                            await websocket.send(
                                _subscription_message(tuple(slug_to_contract_ids))
                            )
                            subscribed = True
                            reconnecting = attempt.retry_state.attempt_number > 1
                            _events.log(
                                logging.WARNING if reconnecting else logging.INFO,
                                "WS Limitless · %s · %s mercados",
                                "reconectado" if reconnecting else "conectado",
                                len(slug_to_contract_ids),
                            )
                            continue

                        event = _socketio_event(message)
                        if event is None:
                            continue
                        event_name, payload = event
                        if event_name != "orderbookUpdate" or not isinstance(payload, dict):
                            continue
                        slug = str(payload.get("marketSlug") or "")
                        streamed_contract_ids = slug_to_contract_ids.get(slug)
                        if streamed_contract_ids is None:
                            continue

                        raw_book = payload.get("orderbook")
                        if not isinstance(raw_book, dict):
                            continue
                        if payload.get("timestamp") is not None:
                            raw_book = {**raw_book, "timestamp": payload["timestamp"]}
                        for contract_id in streamed_contract_ids:
                            yield contract_id, stamp_order_book(
                                limitless_orderbook_to_order_book(
                                    raw_book,
                                    contract=self._contracts.get(contract_id),
                                    contract_id=contract_id,
                                ),
                                arrival,
                                source_timestamp_kind="venue_update",
                            )

                    raise ConnectionError("Limitless WebSocket closed")

    def add_contracts(self, contracts: tuple[BinaryContract, ...]) -> None:
        """Register contracts used to translate subsequent venue order-book updates."""
        for contract in contracts:
            self._contracts[contract.id] = contract

    def _observe_transport(self, websocket: Any) -> None:
        """Sample current Socket.IO receive pressure without changing delivery."""
        queue_depth, paused = transport_state(websocket)
        self._queue_high_watermark = max(self._queue_high_watermark, queue_depth)
        self._transport_metric_sample_index += 1
        if (
            self._transport_metric_sample_index % _METRIC_SAMPLE_EVERY == 0
            or paused
        ):
            venue = str(self.venue_id)
            MARKET_FEED_WS_QUEUE_DEPTH.labels(venue).set(queue_depth)
            MARKET_FEED_WS_QUEUE_HIGH_WATERMARK.labels(venue).set(
                self._queue_high_watermark,
            )
            MARKET_FEED_WS_PAUSED.labels(venue).set(1 if paused else 0)
            if self._queue_observer is not None:
                self._queue_observer(
                    WebSocketQueueSample(
                        venue,
                        "public",
                        queue_depth,
                        self._queue_high_watermark,
                        paused,
                    )
                )
        if paused != self._last_paused:
            if self._transition_observer is not None:
                self._transition_observer(
                    WebSocketTransition(
                        str(self.venue_id),
                        "public",
                        "pause",
                        "started" if paused else "cleared",
                    )
                )
            self._last_paused = paused

    @asynccontextmanager
    async def _tracked_connection(self) -> AsyncIterator[Any]:
        """Open one connection and reset its instantaneous metrics on close.

        Yields
        ------
        Any
            Connected Limitless WebSocket instrumented for queue timing.
        """
        venue = str(self.venue_id)
        async with connect(
            self._websocket_url,
            max_queue=256,
            create_connection=TimestampedClientConnection,
        ) as websocket:
            MARKET_FEED_WS_QUEUE_DEPTH.labels(venue).set(0)
            MARKET_FEED_WS_QUEUE_HIGH_WATERMARK.labels(venue).set(
                self._queue_high_watermark,
            )
            MARKET_FEED_WS_PAUSED.labels(venue).set(0)
            def observe_wait(queue_wait: float) -> None:
                MARKET_FEED_WS_MESSAGE_QUEUE_WAIT.labels(venue).observe(queue_wait)
                if self._queue_observer is not None:
                    self._queue_observer(
                        WebSocketQueueSample(
                            venue,
                            "public",
                            queue_wait_seconds=queue_wait,
                        )
                    )

            instrument_message_queue(
                websocket,
                observe_wait,
            )
            try:
                yield websocket
            finally:
                MARKET_FEED_WS_QUEUE_DEPTH.labels(venue).set(0)
                MARKET_FEED_WS_PAUSED.labels(venue).set(0)
                if self._queue_observer is not None:
                    self._queue_observer(
                        WebSocketQueueSample(
                            venue,
                            "public",
                            0,
                            self._queue_high_watermark,
                            False,
                        )
                    )
                self._last_paused = False


def _subscription_message(slugs: str | tuple[str, ...]) -> str:
    if isinstance(slugs, str):
        slugs = (slugs,)
    return f"42{_NAMESPACE},{json.dumps(['subscribe_market_prices', {'marketSlugs': list(slugs)}])}"


def _namespace_connect_message() -> str:
    return f"40{_NAMESPACE},"


def _is_namespace_connected(message: str) -> bool:
    return message == f"40{_NAMESPACE}" or message.startswith(f"40{_NAMESPACE},")


def _socketio_event(message: str) -> tuple[str, Any] | None:
    """Decode a Socket.IO frame into an event name and payload."""
    prefix = f"42{_NAMESPACE},"
    if not message.startswith(prefix):
        return None
    try:
        event = json.loads(message.removeprefix(prefix))
    except json.JSONDecodeError:
        return None
    if not isinstance(event, list) or len(event) != 2 or not isinstance(event[0], str):
        return None
    return event[0], event[1]


def _as_text(raw_message: str | bytes) -> str:
    return (
        raw_message.decode("utf-8") if isinstance(raw_message, bytes) else raw_message
    )
