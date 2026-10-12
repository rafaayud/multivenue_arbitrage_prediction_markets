"""Verify bounded capture metrics and producer labels."""

import math

from prometheus_client import REGISTRY

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
