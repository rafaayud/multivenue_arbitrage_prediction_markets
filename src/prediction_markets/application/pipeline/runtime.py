"""Compose the bounded event loop and parallel order-dispatch stages.

Responsibilities
----------------
- Keep the public application-pipeline import surface stable.
- Own the lifecycle of the event loop and output dispatcher tasks.
- Coordinate replay, derived-event recovery, draining, startup, and shutdown.

Notes
-----
- Buffer mechanics live in :mod:`prediction_markets.application.pipeline.buffers`.
- Parallel preparation, submission, monitoring, and recovery live in
  :mod:`prediction_markets.application.pipeline.order_dispatch`.
"""

import asyncio
from collections.abc import Callable

from prediction_markets.application.engine import TradingEngine
from prediction_markets.application.events import ApplicationEvent
from prediction_markets.application.pipeline.order_dispatch import (
    OutputDispatcher,
)
from prediction_markets.application.pipeline.buffers import (
    EventLoop,
    EventSink,
    JournalPort,
    JournalRecord,
    PipelineOutput,
    RingBuffer,
)
from prediction_markets.infrastructure.operational_metrics import (
    EVENT_LOOP_LAG,
    PIPELINE_BUFFER_CAPACITY,
    PIPELINE_BUFFER_HIGH_WATERMARK,
    PIPELINE_BUFFER_SIZE,
    PIPELINE_ORDER_BOOK_DROPS,
    scheduled_lags,
    update_predict_fill_capture_metrics,
    update_clock_metrics,
)
from prediction_markets.infrastructure.clock_health import ClockMonitor
from prediction_markets.infrastructure.observability.predict_fill_study import capture_status

__all__ = [
    "EventLoop",
    "EventSink",
    "JournalPort",
    "JournalRecord",
    "OutputDispatcher",
    "RingBuffer",
    "TradingPipeline",
]


class TradingPipeline:
    """Own the two buffers and the long-lived event/output tasks.

    Parameters
    ----------
    journal
        Single ordered journal shared by every pipeline stage.
    engine
        Pure application processor.
    input_capacity : int, default=8192
        Maximum unprocessed normalized inputs.
    output_capacity : int, default=1024
        Maximum prepared-to-dispatch order commands.
    enforce_source_age
        Whether venue wall-clock age can reject dispatch. ``None`` uses the
        process-level rollout setting.
    """

    def __init__(
        self,
        journal: JournalPort,
        engine: TradingEngine,
        *,
        input_capacity: int = 8_192,
        output_capacity: int = 1_024,
        on_processing: Callable[[ApplicationEvent], None] | None = None,
        enforce_source_age: bool | None = None,
    ) -> None:
        self.inputs = RingBuffer[ApplicationEvent](input_capacity)
        self.outputs = RingBuffer[PipelineOutput](output_capacity)
        self.sink = EventSink(self.inputs, PIPELINE_ORDER_BOOK_DROPS.inc)
        self.event_loop = EventLoop(
            self.inputs,
            self.outputs,
            journal,
            engine,
            on_processing,
        )
        self._engine = engine
        self._tasks: tuple[asyncio.Task[None], ...] = ()
        self._error: BaseException | None = None
        self._failed = asyncio.Event()
        self.output_dispatcher = OutputDispatcher(
            self.outputs,
            self.sink,
            journal,
            engine.state,
            self._fail,
            commit_prepared_execution=engine.commit_prepared_execution,
            abort_staged_execution=engine.abort_staged_execution,
            reprice_staged_execution=engine.reprice_staged_execution,
            guard_recovery=engine.guard_recovery,
            enforce_source_age=enforce_source_age,
        )
        for name, buffer in (("input", self.inputs), ("output", self.outputs)):
            PIPELINE_BUFFER_CAPACITY.labels(name).set(buffer.capacity)
            PIPELINE_BUFFER_SIZE.labels(name).set(0)
            PIPELINE_BUFFER_HIGH_WATERMARK.labels(name).set(0)

    @property
    def running(self) -> bool:
        """Report whether both pipeline consumers are alive."""
        return bool(self._tasks) and all(not task.done() for task in self._tasks)

    @property
    def error(self) -> BaseException | None:
        """Return the first unexpected consumer failure."""
        return self._error

    async def wait_until_failed(self) -> None:
        """Wait until either pipeline consumer exits with an exception."""
        await self._failed.wait()

    def replay(self, entries: tuple[JournalRecord, ...]) -> None:
        """Rebuild engine state before live sources are connected."""
        self.event_loop.replay(entries)

    async def recover_derived(self) -> None:
        """Journal missing deterministic outputs without dispatching them twice."""
        for event in self._engine.recovery_outputs():
            await self.event_loop.process(event, enqueue_commands=False)

    async def drain_inputs(self, timeout: float = 5.0) -> None:
        """Wait for queued recovery responses before enabling new orders."""
        await asyncio.wait_for(self.inputs.join(), timeout=timeout)

    async def start(self) -> None:
        """Start the event loop and output dispatcher idempotently."""
        if self.running:
            return
        self._error = None
        self._failed.clear()
        self._tasks = (
            asyncio.create_task(self.event_loop.run(), name="trading-event-loop"),
            asyncio.create_task(
                self.output_dispatcher.run(),
                name="trading-output-dispatcher",
            ),
            asyncio.create_task(
                self._observe_operational_metrics(),
                name="trading-operational-metrics",
            ),
        )
        for task in self._tasks:
            task.add_done_callback(self._task_done)

    async def stop(self) -> None:
        """Stop consumers and order watchers without closing shared adapters."""
        for task in self._tasks:
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks = ()
        await self.output_dispatcher.close()

    def _task_done(self, task: asyncio.Task[None]) -> None:
        if not task.cancelled() and (error := task.exception()) is not None:
            self._fail(error)

    def _fail(self, error: BaseException) -> None:
        if self._error is None:
            self._error = error
            self._failed.set()

    async def _observe_operational_metrics(self) -> None:
        """Sample buffer pressure and scheduling delay once per second.

        Notes
        -----
        - Sampling avoids adding Prometheus operations to every order-book update.
        - Lag measures delay on the same asyncio loop that owns the pipeline.
        """
        loop = asyncio.get_running_loop()
        clock = ClockMonitor()
        clock.sample()
        deadline = loop.time() + 1.0
        while True:
            await asyncio.sleep(max(0.0, deadline - loop.time()))
            now = loop.time()
            lags, deadline = scheduled_lags(now, deadline, 1.0)
            for lag in lags:
                EVENT_LOOP_LAG.observe(lag)
            capture = capture_status()
            update_clock_metrics(str(capture.get("producer", "parent")), clock.sample())
            update_predict_fill_capture_metrics(
                str(capture.get("producer", "parent")), capture,
            )
            for name, buffer in (
                ("input", self.inputs),
                ("output", self.outputs),
            ):
                PIPELINE_BUFFER_SIZE.labels(name).set(buffer.size)
                PIPELINE_BUFFER_HIGH_WATERMARK.labels(name).set(
                    buffer.high_watermark,
                )
