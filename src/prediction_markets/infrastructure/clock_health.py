"""Observe local clock discontinuities without calibrating venue timestamps.

Notes
-----
- Reads local clocks only. No network I/O, system-clock adjustment, or trading
  decision is made here. Stable clocks are not proof of UTC synchronization.
"""

import time


CLOCK_STEP_TOLERANCE_NS = 20_000_000


def sample_clocks() -> dict[str, int | None]:
    """Bracket wall time with monotonic reads and optionally sample Linux raw time.

    Returns
    -------
    dict
        Nanosecond wall/monotonic midpoint, read span, and optional raw monotonic
        clock. The span bounds scheduling uncertainty between the paired reads.
    """
    before = time.monotonic_ns()
    wall = time.time_ns()
    raw_clock = getattr(time, "CLOCK_MONOTONIC_RAW", None)
    raw = time.clock_gettime_ns(raw_clock) if raw_clock is not None else None
    after = time.monotonic_ns()
    return {"wall_ns": wall, "mono_ns": (before + after) // 2,
            "read_span_ns": after - before,
            "raw_ns": raw}


class ClockMonitor:
    """Retain bounded local clock diagnostics for a single sampling task.

    Notes
    -----
    - A 20 ms offset-change tolerance excludes small sampling noise. Measured
      read uncertainty is also excluded before counting a discontinuity.
    - The Linux raw-rate comparison measures local clock discipline, not UTC
      accuracy. It is unavailable on platforms without a raw monotonic clock.
    """

    def __init__(self) -> None:
        self._previous: dict[str, int | None] | None = None
        self._steps = 0
        self._maximum_step_ns = 0

    def sample(self) -> dict[str, int | float | None]:
        """Return one clock anchor with cumulative discontinuity diagnostics."""
        current = sample_clocks()
        previous = self._previous
        self._previous = current
        step = None
        rate = None
        if previous is not None:
            elapsed = current["mono_ns"] - previous["mono_ns"]
            step = current["wall_ns"] - previous["wall_ns"] - elapsed
            uncertainty = (current["read_span_ns"] + previous["read_span_ns"]) // 2
            lower_bound = max(0, abs(step) - uncertainty)
            self._maximum_step_ns = max(self._maximum_step_ns, lower_bound)
            if lower_bound > CLOCK_STEP_TOLERANCE_NS:
                self._steps += 1
            if current["raw_ns"] is not None and previous["raw_ns"] is not None:
                raw_elapsed = current["raw_ns"] - previous["raw_ns"]
                if raw_elapsed > 0 and elapsed > 0 and uncertainty <= CLOCK_STEP_TOLERANCE_NS:
                    rate = (elapsed / raw_elapsed - 1) * 1_000_000
        return {**current, "wall_step_ns": step, "discontinuities": self._steps,
                "maximum_step_ns": self._maximum_step_ns, "monotonic_raw_rate_ppm": rate}
