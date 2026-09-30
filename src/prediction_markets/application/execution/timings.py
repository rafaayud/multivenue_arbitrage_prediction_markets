"""Record process-local timing marks for parallel arbitrage execution.

Responsibilities
----------------
- Hold monotonic marks for both independently submitted legs.
- Observe per-leg and cross-leg Prometheus stage histograms.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

from prediction_markets.infrastructure.metrics import ARBITRAGE_STAGE_LATENCY
from prediction_markets.infrastructure.observability.execution_timing import ThreadCallTiming

Leg = Literal["primary", "hedge"]

_CROSS_VENUE = "cross-venue"
_PARALLEL_LEG = "parallel"


@dataclass(slots=True)
class ExecutionTimings:
    """Hold monotonic timestamps for one parallel two-leg execution.

    Attributes
    ----------
    opportunity_at_ns
        When the execution was admitted and planned.
    venue_ids
        Venue identifier for each execution leg.
    book_received_at_ns
        Local receive time of each book used to plan the execution.
    command_received_at_ns
        Dispatcher receive time by leg.
    prepare_started_at_ns
        Request preparation start time by leg.
    watch_finished_at_ns
        Private order-stream readiness completion time by leg.
    adapter_prepared_at_ns
        Venue payload build and signature completion time by leg.
    journaled_at_ns
        Dispatcher completion mark after append and grouped state commit,
        without an fsync guarantee.
    prepare_calls, submit_calls, journal_calls
        Initial per-leg executor calls. A shared batch uses the same journal
        collector for both legs; bounded reprices retain the first preparation.
    prepared_at_ns
        Request preparation completion time by leg.
    guard_book_received_at_ns
        Local receive time of each book checked by the live guard.
    guard_checked_at_ns
        Instant used to calculate both guard book ages.
    guard_finished_at_ns
        Guard validation completion time.
    guard_error
        Local guard rejection reason, when validation failed.
    submit_at_ns
        Submit-call start by leg.
    ack_at_ns
        Initial venue response by leg.
    first_fill_at_ns
        First positive fill recognized by leg.
    terminal_at_ns
        Terminal order result recognized by leg.
    worker_validation_ns
        Cumulative local time awaiting worker validation, including failed checks.
    worker_validation_count
        Number of worker checks made for this execution.
    worker_reprices
        Number of bounded in-memory replans before the durable batch.
    worker_reprepare_ns
        Additional signing time after a worker requested a new price.
    entry_pricing
        Initial and bounded reprice decisions, including mode, limits, edge,
        and local computation time. These intervals overlap existing stages.

    Notes
    -----
    - Values use ``time.monotonic_ns()``, not wall-clock time.
    - Duplicate marks are ignored so replayed or repeated snapshots do not
      duplicate histogram observations.
    - Worker checks and replacement signing are subsets of elapsed pre-submission
      time, not additive stages. Original preparation marks remain available.
    """

    opportunity_at_ns: int | None = None
    venue_ids: dict[Leg, str] = field(default_factory=dict)
    book_received_at_ns: dict[Leg, int] = field(default_factory=dict)
    command_received_at_ns: dict[Leg, int] = field(default_factory=dict)
    prepare_started_at_ns: dict[Leg, int] = field(default_factory=dict)
    watch_finished_at_ns: dict[Leg, int] = field(default_factory=dict)
    adapter_prepared_at_ns: dict[Leg, int] = field(default_factory=dict)
    journaled_at_ns: dict[Leg, int] = field(default_factory=dict)
    prepare_calls: dict[Leg, ThreadCallTiming] = field(default_factory=dict)
    submit_calls: dict[Leg, ThreadCallTiming] = field(default_factory=dict)
    journal_calls: dict[Leg, ThreadCallTiming] = field(default_factory=dict)
    prepared_at_ns: dict[Leg, int] = field(default_factory=dict)
    guard_book_received_at_ns: dict[Leg, int] = field(default_factory=dict)
    guard_checked_at_ns: int | None = None
    guard_finished_at_ns: int | None = None
    guard_error: str | None = None
    submit_at_ns: dict[Leg, int] = field(default_factory=dict)
    ack_at_ns: dict[Leg, int] = field(default_factory=dict)
    first_fill_at_ns: dict[Leg, int] = field(default_factory=dict)
    terminal_at_ns: dict[Leg, int] = field(default_factory=dict)
    worker_validation_ns: int = 0
    worker_validation_count: int = 0
    worker_reprices: int = 0
    worker_reprepare_ns: int = 0
    entry_pricing: tuple[dict[str, object], ...] = ()

    def mark_entry_pricing(
        self, decision: dict[str, object], started_ns: int, finished_ns: int,
    ) -> None:
        """Record fee-aware sizing and price selection before order preparation.

        Parameters
        ----------
        decision
            JSON-compatible pricing mode, limits, quantity and edge evidence.
        started_ns, finished_ns
            Host monotonic timestamps around fee-aware sizing and limit selection.

        Notes
        -----
        - Elapsed time includes baseline sizing and any edge-budget search.
          It is part of book-to-plan time, not an additional pipeline stage.
        - Mode labels are bounded by the engine to fixed_ticks and edge_budget.
        """
        self.entry_pricing += ({
            **decision,
            "risk_pricing_ms": (finished_ns - started_ns) / 1_000_000,
        },)
        role = next(role for role, venue in self.venue_ids.items() if venue == "PREDICT")
        self._observe(
            "PREDICT", role, f"entry_pricing_{decision['mode']}", started_ns, finished_ns,
        )

    def mark_command_received(
        self,
        leg: Leg,
        venue_id: str,
        now_ns: int,
    ) -> None:
        """Record when one command reaches the output dispatcher."""
        self.venue_ids.setdefault(leg, venue_id)
        self.command_received_at_ns.setdefault(leg, now_ns)

    def mark_prepare_started(self, leg: Leg, now_ns: int) -> None:
        """Record when venue request preparation starts for one leg."""
        self.prepare_started_at_ns.setdefault(leg, now_ns)

    def mark_watch_finished(self, leg: Leg, now_ns: int) -> None:
        """Record private order-stream readiness for one leg."""
        if leg in self.watch_finished_at_ns:
            return
        self.watch_finished_at_ns[leg] = now_ns
        self._observe_leg(
            leg,
            "watch",
            self.prepare_started_at_ns.get(leg),
            now_ns,
        )

    def mark_adapter_prepared(self, leg: Leg, now_ns: int) -> None:
        """Record venue payload construction and signature completion."""
        if leg in self.adapter_prepared_at_ns:
            return
        self.adapter_prepared_at_ns[leg] = now_ns
        self._observe_leg(
            leg,
            "adapter_prepare",
            self.watch_finished_at_ns.get(leg),
            now_ns,
        )

    def mark_journaled(self, leg: Leg, now_ns: int) -> None:
        """Record completion while preserving the historical composite metric."""
        if leg in self.journaled_at_ns:
            return
        self.journaled_at_ns[leg] = now_ns
        self._observe_leg(
            leg,
            "journal_append",
            self.adapter_prepared_at_ns.get(leg),
            now_ns,
        )

    def mark_prepare_finished(self, leg: Leg, now_ns: int) -> None:
        """Record when venue request preparation ends for one leg."""
        if leg in self.prepared_at_ns:
            return
        self.prepared_at_ns[leg] = now_ns
        self._observe_leg(
            leg,
            "prepare_total",
            self.prepare_started_at_ns.get(leg),
            now_ns,
        )

    def mark_guard(
        self,
        book_received_at_ns: dict[Leg, int | None],
        checked_at_ns: int,
        finished_at_ns: int,
        error: str | None,
    ) -> None:
        """Record the exact books and interval used by the local guard."""
        self.guard_book_received_at_ns = {
            leg: received_at_ns
            for leg, received_at_ns in book_received_at_ns.items()
            if received_at_ns is not None
        }
        self.guard_checked_at_ns = checked_at_ns
        self.guard_finished_at_ns = finished_at_ns
        self.guard_error = error

    def mark_submit(self, leg: Leg, venue_id: str, now_ns: int) -> None:
        """Record one submit start and parallel dispatch timing."""
        if leg in self.submit_at_ns:
            return
        self.submit_at_ns[leg] = now_ns
        self._observe(
            venue_id,
            leg,
            "opportunity_to_submit",
            self.opportunity_at_ns,
            now_ns,
        )
        if len(self.submit_at_ns) == 2:
            first, last = min(self.submit_at_ns.values()), max(
                self.submit_at_ns.values(),
            )
            self._observe_parallel("submit_start_skew", first, last)
            self._observe_parallel(
                "opportunity_to_both_submits",
                self.opportunity_at_ns,
                last,
            )

    def mark_ack(self, leg: Leg, venue_id: str, now_ns: int) -> None:
        """Record one initial venue response and parallel acknowledgment timing."""
        if leg in self.ack_at_ns:
            return
        self.ack_at_ns[leg] = now_ns
        self._observe(
            venue_id,
            leg,
            "submit_to_ack",
            self.submit_at_ns.get(leg),
            now_ns,
        )
        if len(self.ack_at_ns) == 2:
            self._observe_parallel(
                "opportunity_to_both_acks",
                self.opportunity_at_ns,
                max(self.ack_at_ns.values()),
            )

    def mark_fill_recognized(self, leg: Leg, venue_id: str, now_ns: int) -> None:
        """Record the first positive fill recognized for one leg."""
        if leg in self.first_fill_at_ns:
            return
        self.first_fill_at_ns[leg] = now_ns
        self._observe(
            venue_id,
            leg,
            "ack_to_first_fill",
            self.ack_at_ns.get(leg),
            now_ns,
        )
        if len(self.first_fill_at_ns) == 2:
            self._observe_parallel(
                "opportunity_to_both_fills",
                self.opportunity_at_ns,
                max(self.first_fill_at_ns.values()),
            )

    def mark_terminal(self, leg: Leg, venue_id: str, now_ns: int) -> None:
        """Record one terminal result and close the parallel execution cycle."""
        if leg in self.terminal_at_ns:
            return
        self.terminal_at_ns[leg] = now_ns
        self._observe(
            venue_id,
            leg,
            "ack_to_terminal",
            self.ack_at_ns.get(leg),
            now_ns,
        )
        if len(self.terminal_at_ns) == 2:
            first, last = min(self.terminal_at_ns.values()), max(
                self.terminal_at_ns.values(),
            )
            self._observe_parallel("terminal_skew", first, last)
            self._observe_parallel(
                "opportunity_to_both_terminal",
                self.opportunity_at_ns,
                last,
            )

    def snapshot(self, execution_id: str) -> dict[str, object]:
        """Build a JSON-compatible latency breakdown for one execution.

        Parameters
        ----------
        execution_id
            Identifier used to correlate all marks in the attempt.

        Returns
        -------
        dict[str, object]
            Exact completed intervals in milliseconds and per-leg book ages.

        Notes
        -----
        - ``book_arrival_skew_ms`` covers venue, network, and adapter arrival
          differences; all stages after the newest local book are process-local.
        - The phase sum explains guard age only when the guard checked the same
          books that were used to build the plan.
        - Historical ``journal_append_ms`` includes peer and validation waits,
          scheduling, and grouped state commit. ``journal_thread.wall_ms``
          measures only the append call, excluding any later background fsync.
        - Thread details subdivide existing intervals; CPU and adapter phases
          are subsets of thread wall time, not additional stages.
        """
        book_times = tuple(self.book_received_at_ns.values())
        latest_book_at_ns = max(book_times) if len(book_times) == 2 else None
        command_pair_at_ns = self._pair_last(self.command_received_at_ns)
        prepared_pair_at_ns = self._pair_last(self.prepared_at_ns)
        older_book_role = None
        if len(book_times) == 2 and min(book_times) != max(book_times):
            older_book_role = min(
                self.book_received_at_ns,
                key=self.book_received_at_ns.__getitem__,
            )

        if self.guard_error is not None:
            outcome = "guard_rejected"
        elif len(self.terminal_at_ns) == 2:
            outcome = "terminal"
        elif len(self.ack_at_ns) == 2:
            outcome = "acknowledged"
        elif len(self.submit_at_ns) == 2:
            outcome = "submitted"
        elif self.guard_finished_at_ns is not None:
            outcome = "guard_passed"
        else:
            outcome = "pending"

        return {
            "execution_id": execution_id,
            **({"entry_pricing": list(self.entry_pricing)} if self.entry_pricing else {}),
            "outcome": outcome,
            "error": self.guard_error,
            "older_book_role": older_book_role,
            "stages": {
                **({
                    "worker_validation_ms": self.worker_validation_ns / 1_000_000,
                    "worker_validation_count": self.worker_validation_count,
                    "worker_reprices": self.worker_reprices,
                    "worker_reprepare_ms": self.worker_reprepare_ns / 1_000_000,
                } if self.worker_validation_count else {}),
                "book_arrival_skew_ms": (
                    self._milliseconds(min(book_times), max(book_times))
                    if len(book_times) == 2
                    else None
                ),
                "newest_book_to_plan_ms": self._milliseconds(
                    latest_book_at_ns,
                    self.opportunity_at_ns,
                ),
                "plan_to_dispatcher_ms": self._milliseconds(
                    self.opportunity_at_ns,
                    command_pair_at_ns,
                ),
                "dispatcher_prepare_ms": self._milliseconds(
                    command_pair_at_ns,
                    prepared_pair_at_ns,
                ),
                "prepare_to_guard_ms": self._milliseconds(
                    prepared_pair_at_ns,
                    self.guard_checked_at_ns,
                ),
                "guard_ms": self._milliseconds(
                    self.guard_checked_at_ns,
                    self.guard_finished_at_ns,
                ),
                "guard_to_both_submits_ms": self._milliseconds(
                    self.guard_finished_at_ns,
                    self._pair_last(self.submit_at_ns),
                ),
            },
            "legs": [
                {
                    "role": leg,
                    "venue": self.venue_ids.get(leg),
                    "book_age_at_plan_ms": self._milliseconds(
                        self.book_received_at_ns.get(leg),
                        self.opportunity_at_ns,
                    ),
                    "book_age_at_guard_ms": self._milliseconds(
                        self.guard_book_received_at_ns.get(leg),
                        self.guard_checked_at_ns,
                    ),
                    "book_replaced_before_guard": (
                        self.book_received_at_ns.get(leg) is not None
                        and self.guard_book_received_at_ns.get(leg) is not None
                        and self.book_received_at_ns[leg]
                        != self.guard_book_received_at_ns[leg]
                    ),
                    "prepare_ms": self._milliseconds(
                        self.prepare_started_at_ns.get(leg),
                        self.prepared_at_ns.get(leg),
                    ),
                    "watch_ms": self._milliseconds(
                        self.prepare_started_at_ns.get(leg),
                        self.watch_finished_at_ns.get(leg),
                    ),
                    "adapter_prepare_ms": self._milliseconds(
                        self.watch_finished_at_ns.get(leg),
                        self.adapter_prepared_at_ns.get(leg),
                    ),
                    "journal_append_ms": self._milliseconds(
                        self.adapter_prepared_at_ns.get(leg),
                        self.journaled_at_ns.get(leg),
                    ),
                    "prepare_peer_wait_ms": self._milliseconds(
                        self.adapter_prepared_at_ns.get(leg),
                        self._pair_last(self.adapter_prepared_at_ns),
                    ),
                    **{
                        name: calls[leg].snapshot()
                        for name, calls in (
                            ("prepare_thread", self.prepare_calls),
                            ("submit_thread", self.submit_calls),
                            ("journal_thread", self.journal_calls),
                        )
                        if leg in calls
                    },
                    "guard_to_submit_ms": self._milliseconds(
                        self.guard_finished_at_ns,
                        self.submit_at_ns.get(leg),
                    ),
                    "submit_to_ack_ms": self._milliseconds(
                        self.submit_at_ns.get(leg),
                        self.ack_at_ns.get(leg),
                    ),
                    "ack_to_first_fill_ms": self._milliseconds(
                        self.ack_at_ns.get(leg),
                        self.first_fill_at_ns.get(leg),
                    ),
                    "ack_to_terminal_ms": self._milliseconds(
                        self.ack_at_ns.get(leg),
                        self.terminal_at_ns.get(leg),
                    ),
                }
                for leg in ("primary", "hedge")
            ],
        }

    @staticmethod
    def _pair_last(marks: dict[Leg, int]) -> int | None:
        """Return the later mark only after both legs have completed a phase."""
        return max(marks.values()) if len(marks) == 2 else None

    @staticmethod
    def _milliseconds(start_ns: int | None, end_ns: int | None) -> float | None:
        """Return a non-negative elapsed interval in milliseconds."""
        if start_ns is None or end_ns is None:
            return None
        return max(0, end_ns - start_ns) / 1_000_000

    @staticmethod
    def _observe(
        venue_id: str,
        leg: str,
        stage: str,
        start_ns: int | None,
        end_ns: int,
    ) -> None:
        """Observe one completed interval when its start mark exists."""
        if start_ns is None:
            return
        ARBITRAGE_STAGE_LATENCY.labels(venue_id, leg, stage).observe(
            (end_ns - start_ns) / 1_000_000_000,
        )

    def _observe_leg(
        self,
        leg: Leg,
        stage: str,
        start_ns: int | None,
        end_ns: int,
    ) -> None:
        """Observe one local preparation interval for a known leg venue."""
        venue_id = self.venue_ids.get(leg)
        if venue_id is not None:
            self._observe(venue_id, leg, stage, start_ns, end_ns)

    @classmethod
    def _observe_parallel(
        cls,
        stage: str,
        start_ns: int | None,
        end_ns: int,
    ) -> None:
        """Observe one cross-venue interval under stable aggregate labels."""
        cls._observe(_CROSS_VENUE, _PARALLEL_LEG, stage, start_ns, end_ns)
