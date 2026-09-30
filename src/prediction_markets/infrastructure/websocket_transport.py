"""Inspect WebSocket receive queues without changing their delivery semantics.

Responsibilities
----------------
- Read supported ``websockets`` assembler pressure state.
- Measure time spent between frame assembly and adapter receipt.

Notes
-----
- Instrumentation is best effort because the inspected assembler is an internal
  detail of the installed ``websockets`` version.
"""

import time
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass, replace
from typing import Any, Literal, TypeAlias

from websockets.asyncio.client import ClientConnection
from websockets.frames import Frame, Opcode

from prediction_markets.domain.orderbook.entities import OrderBook


@dataclass(frozen=True, slots=True)
class SocketArrival:
    """Pair local wall and monotonic clocks at one transport callback."""

    wall_ns: int
    monotonic_ns: int


@dataclass(frozen=True, slots=True)
class WebSocketQueueSample:
    """Describe one locally aggregated WebSocket receive-queue sample.

    Attributes
    ----------
    venue
        Stable venue label.
    stream_id
        Socket or subscription identifier within the venue adapter.
    depth
        Current queued frame count when sampled.
    high_watermark
        Highest frame count observed by the adapter.
    paused
        Whether the receive transport is currently paused.
    queue_wait_seconds
        Assembler enqueue-to-dequeue wait.
    removed
        Whether a rotating socket should be forgotten by the aggregator.
    """

    venue: str
    stream_id: str
    depth: int | None = None
    high_watermark: int | None = None
    paused: bool | None = None
    queue_wait_seconds: float | None = None
    removed: bool = False


@dataclass(frozen=True, slots=True)
class WebSocketTransition:
    """Describe a low-volume queue, overload, or resynchronization transition.

    Attributes
    ----------
    venue
        Stable venue label.
    stream_id
        Socket or subscription identifier within the venue adapter.
    kind
        Bounded transition category.
    state
        Bounded state emitted by the adapter.
    """

    venue: str
    stream_id: str
    kind: Literal["pause", "overload", "resync"]
    state: str


WebSocketQueueObserver: TypeAlias = Callable[[WebSocketQueueSample], None]
WebSocketTransitionObserver: TypeAlias = Callable[[WebSocketTransition], None]


class TimestampedClientConnection(ClientConnection):
    """Attach the first transport-read time to each complete data message."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._current_arrival: SocketArrival | None = None
        self._message_arrivals: deque[SocketArrival] = deque()
        self.last_arrival: SocketArrival | None = None

    def data_received(self, data: bytes) -> None:
        """Timestamp bytes when asyncio hands them to the WebSocket protocol."""
        self._current_arrival = SocketArrival(time.time_ns(), time.monotonic_ns())
        try:
            super().data_received(data)
        finally:
            self._current_arrival = None

    def process_event(self, event: Any) -> None:
        """Associate each data message with its first frame's arrival time."""
        if (
            self.response is not None
            and isinstance(event, Frame)
            and event.opcode in {Opcode.TEXT, Opcode.BINARY}
        ):
            self._message_arrivals.append(
                self._current_arrival
                or SocketArrival(time.time_ns(), time.monotonic_ns())
            )
        super().process_event(event)

    async def recv(self, decode: bool | None = None) -> str | bytes:
        """Return one message and expose its matched transport arrival time."""
        message = await super().recv(decode)
        self.last_arrival = (
            self._message_arrivals.popleft()
            if self._message_arrivals
            else SocketArrival(time.time_ns(), time.monotonic_ns())
        )
        return message


def socket_arrival(websocket: Any) -> SocketArrival:
    """Return a transport timestamp, falling back for simple test doubles."""
    arrival = getattr(websocket, "last_arrival", None)
    return (
        arrival
        if isinstance(arrival, SocketArrival)
        else SocketArrival(time.time_ns(), time.monotonic_ns())
    )


def stamp_order_book(
    book: OrderBook,
    arrival: SocketArrival,
    *,
    source_timestamp_kind: Literal["venue_update", "snapshot_state"],
) -> OrderBook:
    """Attach venue source time and local transport clocks to one book.

    Parameters
    ----------
    book
        Normalized venue book carrying an optional wall-clock timestamp.
    arrival
        Local clocks sampled before WebSocket library-level message queuing.
    source_timestamp_kind
        ``venue_update`` for live updates or ``snapshot_state`` when the
        timestamp may describe the last mutation represented by a snapshot.

    Returns
    -------
    OrderBook
        Book carrying comparable wall and monotonic timing fields.

    Raises
    ------
    ValueError
        If ``source_timestamp_kind`` is unsupported.
    """
    if source_timestamp_kind not in {"venue_update", "snapshot_state"}:
        raise ValueError("Unsupported source timestamp kind")
    source_at_ns = (
        int(book.timestamp.value.timestamp() * 1_000_000) * 1_000
        if book.timestamp is not None
        else None
    )
    return replace(
        book,
        source_at_ns=source_at_ns,
        arrival_wall_at_ns=arrival.wall_ns,
        arrival_at_ns=arrival.monotonic_ns,
        source_timestamp_kind=source_timestamp_kind,
        received_at_ns=arrival.monotonic_ns,
    )


def instrument_message_queue(
    websocket: Any,
    observe_wait: Callable[[float], None],
) -> bool:
    """Measure how long complete messages wait in a WebSocket receive queue.

    Parameters
    ----------
    websocket
        Connected ``websockets`` client exposing its receive assembler.
    observe_wait
        Callback receiving queue wait in seconds.
    Returns
    -------
    bool
        Whether the installed client exposed the required assembler hooks.

    Notes
    -----
    - The hook delegates queue behavior to ``websockets`` and records monotonic
      timestamps only. It does not measure kernel or network transit time.
    """
    assembler = getattr(websocket, "recv_messages", None)
    frames = getattr(assembler, "frames", None)
    original_put = getattr(frames, "put", None)
    original_frame_get = getattr(frames, "get", None)
    original_message_get = getattr(assembler, "get", None)
    if not all(
        callable(value)
        for value in (original_put, original_frame_get, original_message_get)
    ):
        return False

    enqueued_at_ns: dict[int, int] = {}
    current_waits: list[float] = []

    def timed_put(frame: Any) -> None:
        enqueued_at_ns[id(frame)] = time.monotonic_ns()
        original_put(frame)

    async def timed_frame_get(block: bool = True) -> Any:
        frame = await original_frame_get(block)
        queued_at_ns = enqueued_at_ns.pop(id(frame), None)
        if queued_at_ns is not None:
            current_waits.append(
                max(0, time.monotonic_ns() - queued_at_ns) / 1_000_000_000,
            )
        return frame

    async def timed_message_get(decode: bool | None = None) -> Any:
        current_waits.clear()
        message = await original_message_get(decode)
        if current_waits:
            observe_wait(max(current_waits))
        return message

    frames.put = timed_put
    frames.get = timed_frame_get
    assembler.get = timed_message_get
    return True


def transport_state(websocket: Any) -> tuple[int, bool]:
    """Return queued frame count and whether the receive transport is paused.

    Parameters
    ----------
    websocket
        Connected ``websockets`` client exposing an optional receive assembler.

    Returns
    -------
    tuple[int, bool]
        Current queued frame count and paused state. Unsupported clients report
        ``(0, False)``.
    """
    assembler = getattr(websocket, "recv_messages", None)
    frames = getattr(assembler, "frames", ())
    try:
        queue_depth = len(frames)
    except TypeError:
        queue_depth = 0
    return queue_depth, bool(getattr(assembler, "paused", False))
