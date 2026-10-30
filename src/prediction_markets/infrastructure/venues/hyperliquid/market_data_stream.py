"""Stream full Hyperliquid outcome books with bounded receive queues."""

import asyncio
import json
from collections.abc import AsyncIterator
from contextlib import suppress
from time import monotonic

from websockets.asyncio.client import ClientConnection, connect
from websockets.exceptions import ConnectionClosed, InvalidHandshake

from prediction_markets.domain.orderbook.entities import OrderBook
from prediction_markets.domain.ports.market_data_stream import MarketDataStreamPort
from prediction_markets.domain.shared.value_objects import ContractID
from prediction_markets.infrastructure.operational_metrics import (
    MARKET_FEED_WS_MESSAGE_QUEUE_WAIT, MARKET_FEED_WS_PAUSED,
    MARKET_FEED_WS_QUEUE_DEPTH, MARKET_FEED_WS_QUEUE_HIGH_WATERMARK,
)
from prediction_markets.infrastructure.venues.hyperliquid.config import HyperliquidSettings
from prediction_markets.infrastructure.venues.hyperliquid.mappers import (
    HYPERLIQUID_VENUE_ID, hyperliquid_orderbook_to_order_book, outcome_coin,
)
from prediction_markets.infrastructure.websocket_transport import (
    TimestampedClientConnection, instrument_message_queue, socket_arrival,
    stamp_order_book, transport_state,
)


class HyperliquidMarketDataStreamAdapter(MarketDataStreamPort):
    """Subscribe to native YES/NO snapshots on one reconnecting public socket.

    Notes
    -----
    - Every frame is a full book, so reconnects need no delta replay.
    - Disconnects invalidate all subscribed books before retrying.
    - Application pings keep idle subscriptions alive without changing book ages.
    """

    venue_id = HYPERLIQUID_VENUE_ID

    def __init__(self, *, settings: HyperliquidSettings | None = None,
                 websocket_url: str | None = None) -> None:
        self._url = websocket_url or (settings or HyperliquidSettings.from_env()).websocket_url
        self._high_watermark = 0

    async def stream_order_books(
        self, contract_ids: tuple[ContractID, ...],
    ) -> AsyncIterator[tuple[ContractID, OrderBook]]:
        """Yield stamped books, rejecting malformed frames and subscription errors."""
        by_coin = {outcome_coin(contract): contract for contract in contract_ids}
        if not by_coin:
            return
        if len(by_coin) > 1000:
            raise ValueError("Hyperliquid permits at most 1000 subscriptions per IP")
        venue = str(self.venue_id)
        delay = 1.0
        try:
            while True:
                try:
                    async with connect(self._url, max_queue=256,
                                       create_connection=TimestampedClientConnection) as socket:
                        instrument_message_queue(
                            socket, MARKET_FEED_WS_MESSAGE_QUEUE_WAIT.labels(venue).observe,
                        )
                        for coin in by_coin:
                            await socket.send(json.dumps({"method": "subscribe", "subscription": {"type": "l2Book", "coin": coin}}))
                        seen: set[ContractID] = set()
                        frame_count = 0
                        connected_at = monotonic()
                        heartbeat = asyncio.create_task(_heartbeat(socket))
                        try:
                            async for raw in socket:
                                arrival = socket_arrival(socket)
                                depth, paused = transport_state(socket)
                                self._high_watermark = max(self._high_watermark, depth)
                                if frame_count % 16 == 0:
                                    MARKET_FEED_WS_QUEUE_DEPTH.labels(venue).set(depth)
                                    MARKET_FEED_WS_QUEUE_HIGH_WATERMARK.labels(venue).set(self._high_watermark)
                                    MARKET_FEED_WS_PAUSED.labels(venue).set(int(paused))
                                frame_count += 1
                                message = json.loads(raw)
                                if not isinstance(message, dict):
                                    raise ValueError("Hyperliquid returned an invalid WebSocket message")
                                channel = message.get("channel")
                                if channel == "error":
                                    raise ValueError("Hyperliquid rejected a book subscription")
                                if channel != "l2Book":
                                    continue
                                payload = message.get("data")
                                if not isinstance(payload, dict):
                                    raise ValueError("Hyperliquid returned an invalid book frame")
                                contract = by_coin.get(payload.get("coin"))
                                if contract is None:
                                    continue
                                book = hyperliquid_orderbook_to_order_book(payload, contract)
                                if monotonic() - connected_at >= 30:
                                    delay = 1.0
                                kind = "venue_update" if contract in seen else "snapshot_state"
                                seen.add(contract)
                                yield contract, stamp_order_book(book, arrival, source_timestamp_kind=kind)
                            raise ConnectionError("Hyperliquid book socket closed")
                        finally:
                            heartbeat.cancel()
                            with suppress(asyncio.CancelledError, ConnectionClosed):
                                await heartbeat
                except (ConnectionClosed, InvalidHandshake, OSError, TimeoutError):
                    MARKET_FEED_WS_QUEUE_DEPTH.labels(venue).set(0)
                    MARKET_FEED_WS_PAUSED.labels(venue).set(0)
                    for coin, contract in by_coin.items():
                        yield contract, hyperliquid_orderbook_to_order_book(
                            {"coin": coin, "time": 0, "levels": [[], []]}, contract,
                        )
                    await asyncio.sleep(delay)
                    delay = min(delay * 2, 30.0)
        finally:
            MARKET_FEED_WS_QUEUE_DEPTH.labels(venue).set(0)
            MARKET_FEED_WS_PAUSED.labels(venue).set(0)


async def _heartbeat(socket: ClientConnection) -> None:
    """Send protocol-level application pings while subscriptions remain idle."""
    while True:
        await asyncio.sleep(30)
        await socket.send('{"method":"ping"}')
