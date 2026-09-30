"""Verify that clock diagnostics retain uncertainty and never calibrate trading."""

import pytest

from prediction_markets.infrastructure import clock_health
from prediction_markets.infrastructure.operational_metrics import LOCAL_CLOCK, update_clock_metrics


def test_clock_monitor_detects_steps_and_linux_rate_without_claiming_utc_accuracy(monkeypatch):
    """Forward/backward wall steps survive aggregation; raw rate is independent."""
    samples = iter([
        {"wall_ns": 100_000_000_000, "mono_ns": 10_000_000_000, "raw_ns": 20_000_000_000, "read_span_ns": 100},
        {"wall_ns": 101_500_000_000, "mono_ns": 11_000_000_000, "raw_ns": 21_005_000_000, "read_span_ns": 100},
        {"wall_ns": 101_900_000_000, "mono_ns": 12_000_000_000, "raw_ns": 22_010_000_000, "read_span_ns": 100},
    ])
    monkeypatch.setattr(clock_health, "sample_clocks", lambda: next(samples))
    monitor = clock_health.ClockMonitor()
    assert monitor.sample()["wall_step_ns"] is None
    forward = monitor.sample()
    assert forward["wall_step_ns"] == 500_000_000
    assert forward["monotonic_raw_rate_ppm"] == pytest.approx(-4975.124378)
    backward = monitor.sample()
    assert backward["wall_step_ns"] == -600_000_000
    assert backward["discontinuities"] == 2
    update_clock_metrics("clock-test", backward)
    assert LOCAL_CLOCK.labels("clock-test", "discontinuities")._value.get() == 2


def test_clock_read_scheduling_uncertainty_is_not_reported_as_a_proven_step(monkeypatch):
    """A delayed read pair must not manufacture a wall-clock discontinuity."""
    samples = iter([
        {"wall_ns": 100_000_000_000, "mono_ns": 10_000_000_000, "raw_ns": None, "read_span_ns": 0},
        {"wall_ns": 101_100_000_000, "mono_ns": 11_000_000_000, "raw_ns": None, "read_span_ns": 200_000_000},
    ])
    monkeypatch.setattr(clock_health, "sample_clocks", lambda: next(samples))
    monitor = clock_health.ClockMonitor()
    monitor.sample()
    result = monitor.sample()
    assert result["discontinuities"] == 0
    assert result["monotonic_raw_rate_ppm"] is None


def test_sampling_supports_platforms_without_linux_raw_clock(monkeypatch):
    """Windows retains paired clocks without a raw-clock syscall or network I/O."""
    monkeypatch.delattr(clock_health.time, "CLOCK_MONOTONIC_RAW", raising=False)
    sample = clock_health.sample_clocks()
    assert sample["raw_ns"] is None
    assert sample["read_span_ns"] >= 0


def test_negative_source_age_stays_raw_and_is_not_classified_as_fresh(tmp_path, monkeypatch):
    """Unsynchronized clocks must not improve the study's source-freshness bucket."""
    pytest.importorskip("repo_tools.predict_fill_analysis")
    from tests.infrastructure.test_predict_fill_analysis_features import _capture, _row, _report

    rows = _capture()
    rows[2]["data"]["source_at_ns"] = _row("clock", {}, 1000)["wall_ns"]
    report = _report(tmp_path, monkeypatch, rows)
    feature = report["orders"][0]["features"]
    assert feature["predict_source_age_ms"] < 0
    assert feature["predict_source_clock_status"] == "negative_raw_delta"
    assert report["comparisons"][0]["source_age_bucket"] == "unknown"


@pytest.mark.parametrize("step", [-500_000_000, 500_000_000])
def test_clock_step_censors_only_crossing_intervals(tmp_path, monkeypatch, step):
    """Keep earlier evidence while censoring survival through an uncertain clock jump."""
    pytest.importorskip("repo_tools.predict_fill_analysis")
    from tests.infrastructure.test_predict_fill_analysis_features import _capture, _book, _row, _report

    rows = _capture() + [_book(200)]
    for row in rows:
        if row["mono_ns"] >= _row("clock", {}, 200)["mono_ns"]:
            row["wall_ns"] += step
    report = _report(tmp_path, monkeypatch, rows)
    horizons = report["signal_samples"][0]["horizons"]
    assert horizons[0]["displayed_executable"] is True
    assert horizons[1]["reason"] == "clock_discontinuity"
    assert horizons[1]["displayed_executable"] is None


def test_ntp_invalid_response_is_not_an_offset_estimate(monkeypatch):
    """An unmatched UDP response cannot certify clock quality."""
    diagnose_clocks = pytest.importorskip("repo_tools.diagnose_clocks")

    class Connection:
        """Return malformed diagnostic bytes without accessing a socket."""

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def settimeout(self, _timeout):
            pass

        def connect(self, _address):
            pass

        def send(self, _packet):
            pass

        def recv(self, _size):
            return bytes(48)

    monkeypatch.setattr(diagnose_clocks.socket, "socket", lambda *_args: Connection())
    result = diagnose_clocks.probe("example.invalid", 1)
    assert result["status"] == "unavailable"
    assert "offset_ms" not in result


@pytest.mark.parametrize("wall_step", [0, -500_000_000, 500_000_000])
def test_ntp_estimate_bounds_and_wall_step_rejection(monkeypatch, wall_step):
    """Keep asymmetric transit bounds and reject an offset across a wall step."""
    import struct
    diagnose_clocks = pytest.importorskip("repo_tools.diagnose_clocks")

    start = 1_788_854_000_000_000_000
    samples = iter([
        {"wall_ns": start, "mono_ns": 100_000_000_000},
        {"wall_ns": start + 31_000_000 + wall_step, "mono_ns": 100_031_000_000},
    ])
    monkeypatch.setattr(diagnose_clocks, "local_sample", lambda: next(samples))

    class Connection:
        """Echo a matched NTP reply with known transit and processing times."""

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def settimeout(self, _timeout):
            pass

        def connect(self, _address):
            pass

        def send(self, packet):
            self.packet = packet

        def recv(self, _size):
            response = bytearray(48)
            response[0], response[1] = 0x24, 1
            response[24:32] = self.packet[40:48]
            for offset, at in ((32, start + 90_000_000), (40, start + 91_000_000)):
                seconds, fraction = divmod(at, 1_000_000_000)
                response[offset:offset + 8] = struct.pack(
                    "!II", seconds + diagnose_clocks._NTP_EPOCH, (fraction << 32) // 1_000_000_000,
                )
            return bytes(response)

    monkeypatch.setattr(diagnose_clocks.socket, "socket", lambda *_args: Connection())
    result = diagnose_clocks.probe("example.invalid", 1)
    assert result["round_trip_ms"] == 31
    if wall_step:
        assert result["status"] == "clock_discontinuity"
        assert result["offset_ms"] is None
        assert result["network_only_offset_interval_ms"] is None
    else:
        assert result["status"] == "estimate"
        assert result["offset_ms"] == pytest.approx(75)
        assert result["network_only_offset_interval_ms"] == pytest.approx([60, 90])
