"""Verify timing metrics for concurrently submitted arbitrage legs."""

from unittest.mock import call, patch

import pytest

from prediction_markets.application.execution.timings import ExecutionTimings
from prediction_markets.infrastructure.observability.execution_timing import ThreadCallTiming


def test_entry_pricing_records_mode_limits_and_a_non_additive_duration() -> None:
    """Expose the experiment and its computation time without replacing stage metrics."""
    timings = ExecutionTimings(venue_ids={"primary": "PREDICT", "hedge": "POLYMARKET"})
    decision = {"mode": "edge_budget", "detected_limit": "0.5", "planned_limit": "0.58"}
    with patch("prediction_markets.application.execution.timings.ARBITRAGE_STAGE_LATENCY") as metric:
        timings.mark_entry_pricing(decision, 1_000_000, 1_200_000)
    assert timings.snapshot("execution")["entry_pricing"] == [{**decision, "risk_pricing_ms": 0.2}]
    metric.labels.assert_called_once_with("PREDICT", "primary", "entry_pricing_edge_budget")
    metric.labels.return_value.observe.assert_called_once_with(0.0002)


def test_parallel_timings_observe_leg_and_cross_leg_intervals() -> None:
    """Close aggregate stages only after both unique leg marks exist."""
    with patch(
        "prediction_markets.application.execution.timings.ARBITRAGE_STAGE_LATENCY",
    ) as metric:
        timings = ExecutionTimings(opportunity_at_ns=1_000_000_000)
        timings.mark_submit("primary", "venue-a", 2_000_000_000)
        timings.mark_submit("hedge", "venue-b", 2_100_000_000)
        timings.mark_ack("primary", "venue-a", 3_000_000_000)
        timings.mark_ack("hedge", "venue-b", 4_000_000_000)
        timings.mark_fill_recognized("primary", "venue-a", 5_000_000_000)
        timings.mark_fill_recognized("hedge", "venue-b", 6_000_000_000)
        timings.mark_terminal("primary", "venue-a", 7_000_000_000)
        timings.mark_terminal("hedge", "venue-b", 8_000_000_000)
        timings.mark_terminal("hedge", "venue-b", 9_000_000_000)

    assert metric.labels.call_args_list == [
        call("venue-a", "primary", "opportunity_to_submit"),
        call("venue-b", "hedge", "opportunity_to_submit"),
        call("cross-venue", "parallel", "submit_start_skew"),
        call("cross-venue", "parallel", "opportunity_to_both_submits"),
        call("venue-a", "primary", "submit_to_ack"),
        call("venue-b", "hedge", "submit_to_ack"),
        call("cross-venue", "parallel", "opportunity_to_both_acks"),
        call("venue-a", "primary", "ack_to_first_fill"),
        call("venue-b", "hedge", "ack_to_first_fill"),
        call("cross-venue", "parallel", "opportunity_to_both_fills"),
        call("venue-a", "primary", "ack_to_terminal"),
        call("venue-b", "hedge", "ack_to_terminal"),
        call("cross-venue", "parallel", "terminal_skew"),
        call("cross-venue", "parallel", "opportunity_to_both_terminal"),
    ]
    durations = [
        observed.args[0]
        for observed in metric.labels.return_value.observe.call_args_list
    ]
    assert durations == pytest.approx(
        [1, 1.1, 0.1, 1.1, 1, 1.9, 3, 2, 2, 5, 4, 4, 1, 7],
    )


def test_execution_snapshot_explains_guard_book_age() -> None:
    """Split one stale guard decision into correlated local phases."""
    timings = ExecutionTimings(
        opportunity_at_ns=60_000_000,
        venue_ids={"primary": "LIMITLESS", "hedge": "POLYMARKET"},
        book_received_at_ns={"primary": 0, "hedge": 50_000_000},
    )
    timings.mark_command_received("primary", "LIMITLESS", 70_000_000)
    timings.mark_command_received("hedge", "POLYMARKET", 72_000_000)
    timings.mark_prepare_started("primary", 73_000_000)
    timings.mark_prepare_started("hedge", 73_000_000)
    timings.mark_watch_finished("primary", 76_000_000)
    timings.mark_watch_finished("hedge", 75_000_000)
    timings.mark_adapter_prepared("primary", 95_000_000)
    timings.mark_adapter_prepared("hedge", 105_000_000)
    timings.mark_journaled("primary", 99_000_000)
    timings.mark_journaled("hedge", 111_000_000)
    timings.mark_prepare_finished("primary", 100_000_000)
    timings.mark_prepare_finished("hedge", 112_000_000)
    timings.mark_guard(
        {"primary": 0, "hedge": 50_000_000},
        113_000_000,
        114_000_000,
        "stale LIMITLESS book",
    )

    snapshot = timings.snapshot("execution-1")

    assert snapshot["outcome"] == "guard_rejected"
    assert snapshot["older_book_role"] == "primary"
    assert snapshot["stages"] == {
        "book_arrival_skew_ms": 50,
        "newest_book_to_plan_ms": 10,
        "plan_to_dispatcher_ms": 12,
        "dispatcher_prepare_ms": 40,
        "prepare_to_guard_ms": 1,
        "guard_ms": 1,
        "guard_to_both_submits_ms": None,
    }
    primary, hedge = snapshot["legs"]
    assert primary["book_age_at_guard_ms"] == 113
    assert hedge["book_age_at_guard_ms"] == 63
    assert primary["prepare_ms"] == 27
    assert hedge["prepare_ms"] == 39
    assert primary["watch_ms"] == 3
    assert primary["adapter_prepare_ms"] == 19
    assert primary["journal_append_ms"] == 4
    assert hedge["watch_ms"] == 2
    assert hedge["adapter_prepare_ms"] == 30
    assert hedge["journal_append_ms"] == 6


def test_shared_journal_append_excludes_peer_validation_and_scheduling() -> None:
    """Keep the legacy composite metric alongside the actual shared append."""
    journal = ThreadCallTiming(
        queued_at_ns=130_000_000,
        started_at_ns=140_000_000,
        finished_at_ns=145_000_000,
        resumed_at_ns=150_000_000,
        cpu_ns=2_000_000,
    )
    timings = ExecutionTimings(
        venue_ids={"primary": "PREDICT", "hedge": "POLYMARKET"},
        adapter_prepared_at_ns={"primary": 80_000_000, "hedge": 100_000_000},
        journal_calls={"primary": journal, "hedge": journal},
    )
    with patch(
        "prediction_markets.application.execution.timings.ARBITRAGE_STAGE_LATENCY",
    ) as metric:
        timings.mark_journaled("primary", 155_000_000)
        timings.mark_journaled("hedge", 155_000_000)
        timings.mark_journaled("primary", 200_000_000)

    primary, hedge = timings.snapshot("execution-1")["legs"]
    assert primary["prepare_peer_wait_ms"] == 20
    assert hedge["prepare_peer_wait_ms"] == 0
    assert primary["journal_append_ms"] == 75
    assert hedge["journal_append_ms"] == 55
    assert primary["journal_thread"] == hedge["journal_thread"] == {
        "queue_ms": 10,
        "wall_ms": 5,
        "cpu_ms": 2,
        "resume_ms": 5,
        "phases_ms": {},
    }
    assert metric.labels.call_args_list == [
        call("PREDICT", "primary", "journal_append"),
        call("POLYMARKET", "hedge", "journal_append"),
    ]
    assert metric.labels.return_value.observe.call_args_list == [call(0.075), call(0.055)]
