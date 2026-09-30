"""Verify isolated thread timing without network or timing-dependent sleeps."""

import asyncio
from threading import Barrier
from unittest.mock import patch

from prediction_markets.infrastructure.observability.execution_timing import (
    ThreadCallTiming,
    measure_phase,
    timed_to_thread,
)


def test_thread_timing_separates_queue_work_cpu_and_resume() -> None:
    """Use independent clock boundaries to expose scheduling and worker time."""
    timing = ThreadCallTiming()
    result = object()
    with (
        patch(
            "prediction_markets.infrastructure.observability.execution_timing.time.monotonic_ns",
            side_effect=[10_000_000, 30_000_000, 80_000_000, 100_000_000],
        ),
        patch(
            "prediction_markets.infrastructure.observability.execution_timing.time.thread_time_ns",
            side_effect=[1_000_000, 21_000_000],
        ),
    ):
        assert asyncio.run(timed_to_thread(timing, lambda: result)) is result

    assert timing.snapshot() == {
        "queue_ms": 20,
        "wall_ms": 50,
        "cpu_ms": 20,
        "resume_ms": 20,
        "phases_ms": {},
    }


def test_parallel_calls_keep_phases_isolated_after_failure() -> None:
    """Keep a failed call's phases out of its peer and subsequent untraced work."""
    failed, succeeded = ThreadCallTiming(), ThreadCallTiming()
    barrier = Barrier(2, timeout=5)
    error = RuntimeError("signing failed")

    def fail():
        with measure_phase("predict_sdk_sign"):
            barrier.wait()
            raise error

    def succeed():
        with measure_phase("predict_sdk_build"):
            barrier.wait()
            return 42

    def untraced():
        with measure_phase("predict_sdk_hash"):
            return 7

    async def run():
        results = await asyncio.gather(
            timed_to_thread(failed, fail),
            timed_to_thread(succeeded, succeed),
            return_exceptions=True,
        )
        assert results == [error, 42]
        assert untraced() == 7
        assert await timed_to_thread(None, untraced) == 7

    asyncio.run(run())

    assert set(failed.phases_ns) == {"predict_sdk_sign"}
    assert set(succeeded.phases_ns) == {"predict_sdk_build"}
    for timing in (failed, succeeded):
        assert timing.started_at_ns is not None
        assert timing.finished_at_ns is not None
        assert timing.resumed_at_ns is not None
        assert timing.queued_at_ns <= timing.started_at_ns <= timing.finished_at_ns
        assert timing.finished_at_ns <= timing.resumed_at_ns
