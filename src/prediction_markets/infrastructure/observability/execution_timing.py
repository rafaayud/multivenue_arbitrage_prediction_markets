"""Measure execution thread calls and bounded local adapter phases.

Notes
-----
- Collectors belong to one call and travel through ``asyncio.to_thread`` using
  a context variable. They do not emit metrics, perform I/O, or retain payloads.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from typing import Any, Literal

Phase = Literal[
    "predict_cached_jwt_lock_wait",
    "predict_cached_jwt_check",
    "predict_sdk_build",
    "predict_sdk_sign",
    "predict_sdk_hash",
    "predict_payload_serialization",
]


@dataclass(slots=True)
class ThreadCallTiming:
    """Hold monotonic boundaries and thread CPU usage for one executor call.

    Attributes
    ----------
    queued_at_ns, started_at_ns, finished_at_ns, resumed_at_ns
        Scheduling, worker entry, worker exit, and event-loop resumption marks.
    cpu_ns
        CPU time consumed by the worker thread while running the call.
    phases_ns
        Cumulative wall time of bounded adapter phases, in nanoseconds.

    Notes
    -----
    - Phase durations are subsets of worker wall time and may include waits.
    - SDK phases time calls, not isolated cryptographic operations. Predict's
      signing call includes internal hashing; ``predict_sdk_hash`` measures the
      additional explicit hash call. These wall times are not signing CPU time
      and must not be added to the total worker wall or CPU intervals.
    - Unfinished boundaries remain absent after early cancellation.
    """

    queued_at_ns: int | None = None
    started_at_ns: int | None = None
    finished_at_ns: int | None = None
    resumed_at_ns: int | None = None
    cpu_ns: int | None = None
    phases_ns: dict[Phase, int] = field(default_factory=dict)

    def snapshot(self) -> dict[str, object]:
        """Return completed scheduling and adapter intervals in milliseconds."""
        return {
            "queue_ms": _milliseconds(self.queued_at_ns, self.started_at_ns),
            "wall_ms": _milliseconds(self.started_at_ns, self.finished_at_ns),
            "cpu_ms": self.cpu_ns / 1_000_000 if self.cpu_ns is not None else None,
            "resume_ms": _milliseconds(self.finished_at_ns, self.resumed_at_ns),
            "phases_ms": {
                name: elapsed / 1_000_000 for name, elapsed in self.phases_ns.items()
            },
        }


_CURRENT: ContextVar[ThreadCallTiming | None] = ContextVar(
    "execution_thread_timing", default=None,
)


async def timed_to_thread[T](
    timing: ThreadCallTiming | None,
    call: Callable[..., T],
    /,
    *args: Any,
    **kwargs: Any,
) -> T:
    """Run a synchronous call with optional isolated timing collection.

    Parameters
    ----------
    timing
        Fresh collector, or ``None`` to disable collection for this call.
    call, args, kwargs
        Callable and arguments forwarded unchanged to the default executor.

    Returns
    -------
    T
        The callable's result; its exceptions and cancellation propagate.

    Notes
    -----
    - CPU time uses the worker's thread clock, excluding other threads.
    - Queue time includes executor dispatch; resume time includes delivering
      the completed result back to the event loop. Neither is network timing.
    """
    def invoke() -> T:
        assert timing is not None
        timing.started_at_ns = time.monotonic_ns()
        cpu_started = time.thread_time_ns()
        try:
            return call(*args, **kwargs)
        finally:
            timing.cpu_ns = max(0, time.thread_time_ns() - cpu_started)
            timing.finished_at_ns = time.monotonic_ns()

    token = _CURRENT.set(timing)
    try:
        if timing is None:
            return await asyncio.to_thread(call, *args, **kwargs)
        timing.queued_at_ns = time.monotonic_ns()
        return await asyncio.to_thread(invoke)
    finally:
        if timing is not None and timing.finished_at_ns is not None:
            timing.resumed_at_ns = time.monotonic_ns()
        _CURRENT.reset(token)


def record_phase_elapsed(phase: Phase, started_at_ns: int) -> None:
    """Accumulate a local phase only when this thread has an active collector."""
    timing = _CURRENT.get()
    if timing is not None:
        elapsed = max(0, time.monotonic_ns() - started_at_ns)
        timing.phases_ns[phase] = timing.phases_ns.get(phase, 0) + elapsed


@contextmanager
def measure_phase(phase: Phase) -> Iterator[None]:
    """Record a bounded phase, including failed calls, when collection is active."""
    if _CURRENT.get() is None:
        yield
        return
    started_at_ns = time.monotonic_ns()
    try:
        yield
    finally:
        record_phase_elapsed(phase, started_at_ns)


def _milliseconds(start_ns: int | None, end_ns: int | None) -> float | None:
    if start_ns is None or end_ns is None:
        return None
    return max(0, end_ns - start_ns) / 1_000_000
