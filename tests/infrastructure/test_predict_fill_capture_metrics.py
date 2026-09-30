"""Verify bounded capture metrics and generation-aware worker summary forwarding."""

import asyncio
import math
import queue
from dataclasses import replace
from decimal import Decimal
from types import SimpleNamespace

import pytest
from prometheus_client import REGISTRY

from prediction_markets.api.runtime import market_workers as workers
from prediction_markets.infrastructure import operational_metrics as metrics


def _capture(state="running"):
    """Return one process-local diagnostic snapshot with cumulative loss and pressure."""
    return {"status": state, "counts": {"dropped": 7, "offers_after_stop": 3},
        "bytes": 12345, "queue_depth": 4, "queue_capacity": 512,
        "active_windows": 2, "writer_alive": True, "writer_progress_age_seconds": 0.125,
        "run_id": "never-a-label", "producer": "worker:crypto-slow:unique-generation"}


def test_capture_metrics_keep_current_state_and_clear_old_run_values():
    """Publish observable capture exhaustion and distinguish a fresh unknown generation."""
    metrics.update_predict_fill_capture_metrics("parent", _capture())
    metrics.update_predict_fill_capture_metrics("parent", _capture("disk_limit"))
    assert REGISTRY.get_sample_value("predict_fill_capture_state", {"producer": "parent", "state": "disk_limit"}) == 1
    assert REGISTRY.get_sample_value("predict_fill_capture_state", {"producer": "parent", "state": "running"}) == 0
    for name, expected in (("dropped_samples", 7), ("offers_after_stop", 3), ("bytes", 12345),
                          ("queue_depth", 4), ("queue_capacity", 512), ("active_windows", 2),
                          ("writer_alive", 1), ("writer_progress_age_seconds", 0.125)):
        assert REGISTRY.get_sample_value(f"predict_fill_capture_{name}", {"producer": "parent"}) == expected
    metrics.update_predict_fill_capture_metrics("parent", None)
    assert REGISTRY.get_sample_value("predict_fill_capture_state", {"producer": "parent", "state": "unknown"}) == 1
    assert math.isnan(REGISTRY.get_sample_value("predict_fill_capture_bytes", {"producer": "parent"}))
    metrics.update_predict_fill_capture_metrics("parent", {**_capture(), "counts": {}, "bytes": 0})
    assert REGISTRY.get_sample_value("predict_fill_capture_dropped_samples", {"producer": "parent"}) == 0


def test_capture_metrics_do_not_create_labels_from_run_or_unknown_producer_ids():
    """Collapse unknown producer and status text into fixed labels and reject invalid values."""
    metrics.update_predict_fill_capture_metrics("worker:crypto-slow:unique-generation", _capture())
    assert REGISTRY.get_sample_value("predict_fill_capture_state", {"producer": "crypto-slow", "state": "running"}) == 1
    for index in range(30):
        metrics.update_predict_fill_capture_metrics(f"worker-{index}:unique-run", {
            **_capture(), "status": f"invalid-{index}", "bytes": "malformed",
            "queue_depth": -1, "writer_progress_age_seconds": float("inf")})
    samples = metrics.PREDICT_FILL_CAPTURE_STATE.collect()[0].samples
    assert all(sample.labels["producer"] in metrics._CAPTURE_PRODUCERS for sample in samples)
    assert all(sample.labels["state"] in metrics._CAPTURE_STATES for sample in samples)
    assert sum(sample.value for sample in samples if sample.labels["producer"] == "unknown") == 1
    for name in ("bytes", "queue_depth", "writer_progress_age_seconds"):
        assert math.isnan(REGISTRY.get_sample_value(f"predict_fill_capture_{name}", {"producer": "unknown"}))


def test_worker_capture_observation_occurs_once_per_summary_outside_telemetry_lock(monkeypatch):
    """Keep capture sampling off book callbacks and reuse the existing aggregate IPC path."""
    output = queue.Queue()
    telemetry = workers._WorkerTelemetry("crypto-slow", "generation", output)
    calls = []

    def capture():
        assert telemetry._lock.acquire(blocking=False)
        telemetry._lock.release()
        calls.append(1)
        return _capture()

    monkeypatch.setattr(workers, "capture_status", capture)
    for _ in range(100):
        telemetry.record_drop("events")
    assert calls == []
    assert output.empty()
    summary = telemetry.snapshot((0.01,), queue.Queue())
    assert calls == [1]
    assert summary.capture == _capture()
    assert summary.enqueue_drops == (("events", "full", 100),)
    assert output.empty()
    assert workers.WorkerRuntimeSummary("crypto-slow", "old", (), 0, None, None, (), (), ()).capture is None


def test_parent_forwards_capture_only_from_current_worker_generation(monkeypatch):
    """Reject stale summaries before updating capture metrics or runtime status."""
    async def scenario():
        partition = workers.market_worker_partitions(enabled_names=("crypto-slow",))[0]
        supervisor = workers.MarketWorkerSupervisor(SimpleNamespace(), min_net_edge=Decimal("0"),
            cost_buffer=Decimal("0"), mode="active", partitions=(partition,))
        supervisor._metrics_queue = object()
        supervisor._process_generations[partition.name] = "current"
        summary = workers.WorkerRuntimeSummary(partition.name, "current", (), 0, None, None,
            (), (), (), _capture("disk_limit"))
        pending = [replace(summary, process_generation="old", capture=_capture()), summary,
            replace(summary, process_generation="old", capture=_capture("io_error"))]
        observations = []

        def dequeue(source):
            if pending:
                return pending.pop(0)
            raise asyncio.CancelledError

        monkeypatch.setattr(workers, "_queue_get", dequeue)
        monkeypatch.setattr(workers, "update_predict_fill_capture_metrics",
            lambda producer, capture: observations.append((producer, capture)))
        with pytest.raises(asyncio.CancelledError):
            await supervisor._consume_metrics()
        assert observations == [(partition.name, summary.capture)]
        assert supervisor.status()[0]["capture"] == summary.capture

    asyncio.run(scenario())
