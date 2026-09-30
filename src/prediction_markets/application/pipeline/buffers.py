"""Provide the bounded buffers and ordered event loop for the pipeline.

Responsibilities
----------------
- Buffer normalized inputs and venue-neutral output commands.
- Drop only disposable order-book updates when the input ring is saturated.
- Journal durable events before the engine processes or dispatches them.
- Process the input ring without interleaving state mutations.
"""

import asyncio
import time
from collections import deque
from collections.abc import Callable
from dataclasses import replace
from typing import Generic, Protocol, TypeVar, cast

from prediction_markets.application.engine import TradingEngine
from prediction_markets.application.events import (
    ApplicationEvent,
    ArbitrageOpportunityFound,
    ExecutionPreparationRequested,
    OrderBookPairUpdated,
    OrderBookUpdated,
    PreparedExecutionBatch,
    RecoveryBooksReceived,
    RecoveryPlanningRequested,
    SubmitOrder,
    TradeRecorded,
)
from prediction_markets.application.freshness import SUBMISSION_BOOK_MAX_AGE_MS

_EVENT_LOOP_YIELD_BUDGET_SECONDS = 0.002

T = TypeVar("T")
_EMPTY = object()
PipelineOutput = SubmitOrder | ExecutionPreparationRequested | RecoveryPlanningRequested


class JournalRecord(Protocol):
    """Expose the typed event required for state replay."""

    event: ApplicationEvent


class JournalPort(Protocol):
    """Append typed events before the next pipeline action."""

    def append(self, event: ApplicationEvent) -> JournalRecord: ...


class RingBufferFull(RuntimeError):
    """Report saturation without suspending the producer."""


class RingBuffer(Generic[T]):
    """Provide a preallocated non-blocking circular FIFO.

    Notes
    -----
    - Publishing never waits: saturation is reported by ``try_publish``.
    - The buffer is confined to one asyncio event loop, so cursor updates need no lock.
    - Consumers suspend only while the buffer is empty.
    """

    def __init__(self, capacity: int) -> None:
        if capacity <= 0:
            raise ValueError("RingBuffer capacity must be positive")
        self._capacity = capacity
        self._slots: list[object] = [_EMPTY] * capacity
        self._read_sequence = 0
        self._write_sequence = 0
        self._size = 0
        self._high_watermark = 0
        self._unfinished = 0
        self._not_empty = asyncio.Event()
        self._drained = asyncio.Event()
        self._drained.set()

    def try_publish(self, value: T) -> bool:
        """Publish immediately, returning ``False`` when every slot is occupied."""
        if self._size == self._capacity:
            return False
        self._slots[self._write_sequence % self._capacity] = value
        self._write_sequence += 1
        self._size += 1
        self._high_watermark = max(self._high_watermark, self._size)
        self._unfinished += 1
        self._drained.clear()
        self._not_empty.set()
        return True

    async def get(self) -> T:
        """Remove the oldest value, waiting when the buffer is empty."""
        while self._size == 0:
            self._not_empty.clear()
            if self._size == 0:
                await self._not_empty.wait()
        index = self._read_sequence % self._capacity
        value = self._slots[index]
        self._slots[index] = _EMPTY
        self._read_sequence += 1
        self._size -= 1
        return cast(T, value)

    def task_done(self) -> None:
        """Mark one retrieved value as completely processed."""
        if self._unfinished <= 0:
            raise ValueError("RingBuffer task_done called too many times")
        self._unfinished -= 1
        if self._unfinished == 0:
            self._drained.set()

    async def join(self) -> None:
        """Wait until every published value has been processed."""
        await self._drained.wait()

    @property
    def has_capacity(self) -> bool:
        """Report whether one value can be published immediately."""
        return self._size < self._capacity

    @property
    def size(self) -> int:
        """Return the current number of buffered values."""
        return self._size

    @property
    def capacity(self) -> int:
        """Return the maximum number of buffered values."""
        return self._capacity

    @property
    def high_watermark(self) -> int:
        """Return the largest occupancy reached by this buffer."""
        return self._high_watermark


class EventSink:
    """Accept normalized adapter events without owning their transports."""

    def __init__(
        self,
        inputs: RingBuffer[ApplicationEvent],
        on_order_book_drop: Callable[[], None] | None = None,
    ) -> None:
        self._inputs = inputs
        self._on_order_book_drop = on_order_book_drop
        self.dropped_order_books = 0

    async def publish(self, event: ApplicationEvent) -> bool:
        """Publish immediately, dropping only disposable public book updates.

        Returns
        -------
        bool
            ``False`` only when a saturated buffer discards an ``OrderBookUpdated``.

        Raises
        ------
        RingBufferFull
            If saturation would discard a financial or lifecycle event.
        """
        if self._inputs.try_publish(event):
            return True
        if isinstance(event, OrderBookUpdated):
            self.dropped_order_books += 1
            if self._on_order_book_drop is not None:
                self._on_order_book_drop()
            return False
        raise RingBufferFull(f"Input ring buffer is full for {type(event).__name__}")


class EventLoop:
    """Process inputs and journal durable outputs in causal order.

    Notes
    -----
    - This loop is the sole output-ring producer and does not suspend between
      checking capacity, appending a command, and publishing it.
    """

    def __init__(
        self,
        inputs: RingBuffer[ApplicationEvent],
        outputs: RingBuffer[PipelineOutput],
        journal: JournalPort,
        engine: TradingEngine,
        on_processing: Callable[[ApplicationEvent], None] | None = None,
    ) -> None:
        self._inputs = inputs
        self._outputs = outputs
        self._journal = journal
        self._engine = engine
        self._on_processing = on_processing
        self._processing = asyncio.Lock()

    async def run(self) -> None:
        """Consume inputs until cancelled while yielding between root events.

        Notes
        -----
        - Derived events from one input remain a single causal batch.
        - A short processing-time budget prevents a sustained input burst from
          starving feed, dispatcher, and monitoring tasks.
        """
        budget_used = 0.0
        loop = asyncio.get_running_loop()
        while True:
            event = await self._inputs.get()
            if isinstance(event, OrderBookUpdated):
                event = replace(
                    event,
                    order_book=replace(
                        event.order_book,
                        processed_at_ns=time.monotonic_ns(),
                    ),
                )
            elif isinstance(event, OrderBookPairUpdated):
                processed_at_ns = time.monotonic_ns()
                event = replace(
                    event,
                    left_order_book=replace(
                        event.left_order_book,
                        processed_at_ns=processed_at_ns,
                    ),
                    right_order_book=replace(
                        event.right_order_book,
                        processed_at_ns=processed_at_ns,
                    ),
                )
            started = loop.time()
            try:
                if self._on_processing is not None:
                    self._on_processing(event)
                await self.process(event)
            finally:
                self._inputs.task_done()
            budget_used += loop.time() - started
            if budget_used >= _EVENT_LOOP_YIELD_BUDGET_SECONDS:
                budget_used = 0.0
                await asyncio.sleep(0)

    def replay(self, entries: tuple[JournalRecord, ...]) -> None:
        """Rebuild state without appending or regenerating external effects."""
        for entry in entries:
            event = entry.event
            if isinstance(event, PreparedExecutionBatch):
                self._engine.commit_prepared_execution(event, replay=True)
                continue
            sequence = getattr(entry, "sequence", None)
            if isinstance(event, TradeRecorded) and isinstance(sequence, int):
                event = TradeRecorded(
                    replace(event.trade, journal_sequence=sequence),
                )
            self._engine.process(event, replay=True)

    async def process(
        self,
        event: ApplicationEvent,
        *,
        enqueue_commands: bool = True,
    ) -> None:
        """Journal and process one event without interleaving state mutations."""
        async with self._processing:
            await self._process(event, enqueue_commands=enqueue_commands)

    async def _process(
        self,
        first: ApplicationEvent,
        *,
        enqueue_commands: bool,
    ) -> None:
        pending: deque[ApplicationEvent] = deque((first,))
        while pending:
            event = pending.popleft()
            if isinstance(event, ArbitrageOpportunityFound) and enqueue_commands:
                books = tuple(
                    self._engine.state.books.get(contract_id)
                    for contract_id in (
                        event.opportunity.left_contract_id,
                        event.opportunity.right_contract_id,
                    )
                )
                received_at_ns = tuple(
                    book.received_at_ns
                    for book in books
                    if book is not None and book.received_at_ns is not None
                )
                deadline_at_ns = (
                    min(received_at_ns)
                    + SUBMISSION_BOOK_MAX_AGE_MS * 1_000_000
                    if len(received_at_ns) == 2
                    else time.monotonic_ns()
                )
                now_ns = time.monotonic_ns()
                if now_ns >= deadline_at_ns:
                    self._journal.append(event)
                    self._engine.commit(event)
                    continue
                staged = self._engine.stage_opportunity(event)
                if staged:
                    planned, primary, hedge = staged
                    request = ExecutionPreparationRequested(
                        event,
                        planned,
                        (primary, hedge),
                        deadline_at_ns,
                        time.time_ns() + deadline_at_ns - now_ns,
                    )
                    if not self._outputs.try_publish(request):
                        self._engine.abort_staged_execution(planned.execution)
                        raise RingBufferFull("Output ring buffer is full")
                    continue
                self._journal.append(event)
                self._engine.commit(event)
                continue
            if isinstance(event, PreparedExecutionBatch):
                self._journal.append(event)
                self._engine.commit_prepared_execution(event)
                continue
            if isinstance(event, RecoveryPlanningRequested):
                if not enqueue_commands:
                    pending.append(RecoveryBooksReceived(
                        event, error="recovery requires fresh authorization after journal reconciliation",
                    ))
                elif not self._outputs.try_publish(event):
                    raise RingBufferFull("Output ring buffer is full")
                continue
            enqueue = enqueue_commands and isinstance(event, SubmitOrder)
            if enqueue and not self._outputs.has_capacity:
                raise RingBufferFull("Output ring buffer is full")
            if not isinstance(event, (OrderBookUpdated, OrderBookPairUpdated, RecoveryBooksReceived)):
                self._journal.append(event)
            pending.extend(self._engine.process(event))
            if enqueue and not self._outputs.try_publish(cast(SubmitOrder, event)):
                raise RuntimeError("Output ring capacity invariant violated")
