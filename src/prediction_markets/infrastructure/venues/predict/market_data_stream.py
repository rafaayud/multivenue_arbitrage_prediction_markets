"""Integrate predict market data stream with domain ports.

Responsibilities
----------------
- Perform venue I/O and translate external data into domain models.
"""

import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

from prediction_markets.infrastructure.observability.predict_fill_study import observe_predict_payload

from tenacity import AsyncRetrying, retry_if_exception_type, wait_exponential
from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed

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
from prediction_markets.infrastructure.venues.predict.config import predict_api_key
from prediction_markets.infrastructure.venues.predict.mappers import (
    PREDICT_VENUE_ID,
    parse_predict_contract_id,
    predict_orderbook_to_order_book,
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


_METRIC_SAMPLE_EVERY = 16


class PredictMarketDataStreamAdapter(MarketDataStreamPort):
    """Stream full Predict.fun order-book snapshots over one WebSocket."""

    venue_id = PREDICT_VENUE_ID

    def __init__(
        self,
        contracts: tuple[BinaryContract, ...] = (),
        *,
        api_key: str | None = None,
        websocket_url: str = "wss://ws.predict.fun/ws",
    ) -> None:
        self._contracts = {contract.id: contract for contract in contracts}
        self._api_key = predict_api_key(api_key)
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
        """Stream normalized order books from predict's live transport.

        Yields
        ------
        OrderBook
            Valid snapshots for the requested contracts.
        """
        if not self._api_key:
            raise ValueError(
                "Predict WebSocket requires api_key or PREDICT_API_KEY"
            )

        contract_ids = tuple(dict.fromkeys(contract_ids))
        if not contract_ids:
            return

        by_market: dict[str, list[ContractID]] = {}
        for contract_id in contract_ids:
            market_id, _ = parse_predict_contract_id(contract_id)
            by_market.setdefault(market_id, []).append(contract_id)

        async for attempt in AsyncRetrying(
            retry=retry_if_exception_type((ConnectionClosed, OSError)),
            wait=wait_exponential(multiplier=1, min=1, max=30),
            reraise=True,
        ):
            with attempt:
                async with self._tracked_connection() as websocket:
                    for request_id, market_id in enumerate(by_market, start=1):
                        await websocket.send(
                            _subscription_message(market_id, request_id)
                        )

                    async for raw_message in websocket:
                        arrival = socket_arrival(websocket)
                        self._observe_transport(websocket)
                        message = _decode_message(raw_message)
                        if message is None:
                            continue
                        if _is_heartbeat(message):
                            await websocket.send(
                                json.dumps(
                                    {
                                        "method": "heartbeat",
                                        "data": message["data"],
                                    }
                                )
                            )
                            continue
                        if message.get("type") == "R":
                            if message.get("success") is not True:
                                raise RuntimeError(
                                    f"Predict subscription failed: "
                                    f"{message.get('error')}"
                                )
                            continue
                        if message.get("type") != "M":
                            continue

                        market_id = _topic_market_id(message.get("topic"))
                        payload = message.get("data")
                        if market_id is None or not isinstance(payload, dict):
                            continue
                        observe_predict_payload(payload, arrival.wall_ns)
                        for contract_id in by_market.get(market_id, ()):
                            yield contract_id, stamp_order_book(
                                predict_orderbook_to_order_book(
                                    payload,
                                    contract=self._contracts.get(contract_id),
                                    contract_id=contract_id,
                                ),
                                arrival,
                                source_timestamp_kind="venue_update",
                            )

                    raise ConnectionError("Predict WebSocket closed")

    def add_contracts(self, contracts: tuple[BinaryContract, ...]) -> None:
        """Register contracts used to translate subsequent venue order-book updates."""
        for contract in contracts:
            self._contracts[contract.id] = contract

    def _observe_transport(self, websocket: Any) -> None:
        """Sample current Predict receive pressure without changing delivery."""
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
            Connected Predict WebSocket instrumented for queue timing.
        """
        venue = str(self.venue_id)
        async with connect(
            self._websocket_url,
            additional_headers={"x-api-key": self._api_key},
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


def _subscription_message(market_id: str, request_id: int) -> str:
    return json.dumps(
        {
            "method": "subscribe",
            "requestId": request_id,
            "params": [f"predictOrderbook/{market_id}"],
        }
    )


def _decode_message(raw_message: str | bytes) -> dict[str, Any] | None:
    try:
        message = json.loads(raw_message)
    except (json.JSONDecodeError, UnicodeDecodeError):
        return None
    return message if isinstance(message, dict) else None


def _is_heartbeat(message: dict[str, Any]) -> bool:
    return (
        message.get("type") == "M"
        and message.get("topic") == "heartbeat"
        and message.get("data") is not None
    )


def _topic_market_id(topic: Any) -> str | None:
    prefix = "predictOrderbook/"
    if not isinstance(topic, str) or not topic.startswith(prefix):
        return None
    market_id = topic.removeprefix(prefix)
    return market_id or None
