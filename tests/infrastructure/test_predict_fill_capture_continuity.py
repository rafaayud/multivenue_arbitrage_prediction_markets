"""Verify selective, bounded fill evidence survives segment rotation and overload."""

import json
import threading
import time
from decimal import Decimal
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from prediction_markets.application import events
from prediction_markets.domain.orderbook.entities import OrderBook
from prediction_markets.domain.orderbook.value_objects import OrderBookLevel
from prediction_markets.domain.shared.value_objects import MarketID, OutcomeID, Price, Quantity, Timestamp, VenueID
from prediction_markets.domain.trading.enums import OrderSide
from prediction_markets.domain.trading.value_objects import OrderBookDecisionSnapshot
from prediction_markets.infrastructure.observability import predict_fill_study as study
from repo_tools.predict_fill_analysis import signal_samples


def _book(contract="predict:42:yes", market="42", venue="PREDICT"):
    """Build an immutable normalized book for one selected or unrelated contract."""
    level = OrderBookLevel(Price(Decimal("0.4")), Quantity(Decimal("12")))
    book = OrderBook(MarketID(market), OutcomeID("yes"), (), (level,),
        arrival_at_ns=time.monotonic_ns(), arrival_wall_at_ns=time.time_ns())
    return events.OrderBookUpdated(VenueID(venue), contract, book)


def _signal():
    """Build detector metadata for a Predict and Polymarket contract pair."""
    level = OrderBookLevel(Price(Decimal("0.4")), Quantity(Decimal("5")))
    pair = SimpleNamespace(left=SimpleNamespace(id="predict:42:yes", venue_id="PREDICT"),
        right=SimpleNamespace(id="other:no", venue_id="POLYMARKET"))
    opportunity = SimpleNamespace(side=OrderSide.BUY, quantity=Quantity(Decimal("5")),
        left_level=level, right_level=level, fee_per_contract=Decimal("0.01"),
        net_edge=Decimal("0.19"), detected_at=Timestamp.now())
    return events.ArbitrageOpportunityFound("signal", None, pair, opportunity)


def _rows(root):
    """Read all retained event segments in their reserved slot order."""
    return [json.loads(line) for path in sorted(root.glob("slot-*/events.jsonl"))
        for line in path.read_text().splitlines()]


def test_runtime_defaults_to_trade_windows_without_detector_disk_capture(tmp_path, monkeypatch):
    """Repeated detector signals retain bounded history without writing unrelated books."""
    monkeypatch.setenv("PREDICT_FILL_STUDY", "1")
    monkeypatch.setenv("JOURNAL_PATH", str(tmp_path / "trading.log"))
    monkeypatch.setattr(study, "_recorder", None)
    study.start_study("test")
    recorder = study._recorder
    try:
        for _ in range(5):
            study.observe_event(_book())
            study.observe_event(_signal())
        recorder._queue.join()
        assert study.capture_status()["capture_mode"] == "trades"
        assert study.capture_status()["active_windows"] == 0
    finally:
        study.stop_study()
    assert {row["kind"] for row in _rows(recorder.root)} == {"segment_start"}


def test_pair_pre_post_windows_preserve_signal_and_pending_depth_only_for_selected_trade(tmp_path, monkeypatch):
    """Capture selected prehistory and later books, with finite non-extending deadlines."""
    clock = [10_000_000_000]
    monkeypatch.setattr(study.time, "monotonic_ns", lambda: clock[0])
    recorder = study.FillStudyRecorder(tmp_path, "test", capture_mode="trades")
    monkeypatch.setattr(study, "_recorder", recorder)
    try:
        for event in (_book(), _book("other:no", venue="POLYMARKET"), _book("predict:99:yes", "99")):
            study.observe_event(event)
        study.observe_predict_payload({"marketId": 42}, time.time_ns())
        study.observe_predict_payload({"marketId": 99}, time.time_ns())
        study.observe_event(_signal())
        recorder._queue.join()
        clock[0] += 1_000_000_000
        study.observe_execution_window("execution", ("predict:42:yes", "other:no"), signal_id="signal")
        recorder._queue.join()
        clock[0] += 1_000_000_000
        study.observe_execution_window("execution", ("predict:42:yes", "other:no"))
        study.observe_event(_book())
        study.observe_event(_book("predict:99:yes", "99"))
        recorder._queue.join()
        clock[0] = 27_000_000_000
        study.observe_event(_book())
        recorder._queue.join()
    finally:
        recorder.close()
    rows = _rows(tmp_path)
    books = [row for row in rows if row["kind"] == "book"]
    assert len(books) == 3
    assert {row["data"]["contract"] for row in books} == {"predict:42:yes", "other:no"}
    assert [row["data"]["market_id"] for row in rows if row["kind"] == "predict_payload"] == [42]
    assert [row["data"]["signal_id"] for row in rows if row["kind"] == "signal"] == ["signal"]
    windows = [row["data"] for row in rows if row["kind"] == "execution_window"]
    assert len(windows) == 1
    assert windows[0]["window_end_mono_ns"] == 26_000_000_000
    assert windows[0]["prehistory_truncated"] is True
    assert windows[0]["pre_start_mono_ns"] == 10_000_000_000
    assert any(row["kind"] == "execution_window_end" and row["data"]["reason"] == "elapsed" for row in rows)
    assert recorder.status()["counts"]["repeated_window_triggers"] == 1


def test_recovery_renews_window_and_window_capacity_is_visible(tmp_path, monkeypatch):
    """Only actual recovery renews the deadline, and excess windows cannot grow memory."""
    clock = [10_000_000_000]
    monkeypatch.setattr(study.time, "monotonic_ns", lambda: clock[0])
    monkeypatch.setattr(study, "MAX_ACTIVE_WINDOWS", 1)
    recorder = study.FillStudyRecorder(tmp_path, "test", capture_mode="trades")
    monkeypatch.setattr(study, "_recorder", recorder)
    try:
        study.observe_execution_window("execution", ("predict:42:yes",))
        recorder._queue.join()
        clock[0] += 10_000_000_000
        study.observe_execution_window("execution", ("predict:42:yes",), reason="recovery")
        study.observe_execution_window("excess", ("predict:99:yes",))
        recorder._queue.join()
        assert recorder.status()["active_windows"] == 1
        assert recorder.status()["counts"]["window_capacity"] == 1
    finally:
        recorder.close()
    windows = [row["data"] for row in _rows(tmp_path) if row["kind"] == "execution_window"]
    assert [window["window_end_mono_ns"] for window in windows] == [25_000_000_000, 35_000_000_000]
    assert windows[1]["window_start_mono_ns"] == windows[0]["window_start_mono_ns"]
    assert any(row["kind"] == "execution_window_rejected" for row in _rows(tmp_path))


def test_ring_eviction_reports_unknown_precoverage(tmp_path, monkeypatch):
    """A ring too small for the requested prewindow records the retained lower bound."""
    clock = [10_000_000_000]
    monkeypatch.setattr(study.time, "monotonic_ns", lambda: clock[0])
    monkeypatch.setattr(study, "HISTORY_CAPACITY", 2)
    recorder = study.FillStudyRecorder(tmp_path, "test", capture_mode="trades")
    monkeypatch.setattr(study, "_recorder", recorder)
    try:
        clock[0] += 3_000_000_000
        for _ in range(4):
            clock[0] += 1_000_000
            study.observe_event(_book())
            recorder._queue.join()
        study.observe_execution_window("execution", ("predict:42:yes",))
    finally:
        recorder.close()
    window = next(row["data"] for row in _rows(tmp_path) if row["kind"] == "execution_window")
    assert window["prehistory_truncated"] is True
    assert window["pre_start_mono_ns"] > 13_001_000_000
    assert len([row for row in _rows(tmp_path) if row["kind"] == "book"]) <= 2


@pytest.mark.parametrize("delay_ms, idle, complete", [(14, False, True), (14, True, True),
    (900, True, True), (1014, True, False)])
def test_two_second_analysis_uses_retained_baseline_before_delayed_trigger(
        tmp_path, monkeypatch, delay_ms, idle, complete):
    """Retain two seconds before detection, without shifting idle-book timestamps."""
    clock = [10_000_000_000]
    wall_offset = 1_700_000_000_000_000_000
    monkeypatch.setattr(study.time, "monotonic_ns", lambda: clock[0])
    monkeypatch.setattr(study.time, "time_ns", lambda: wall_offset + clock[0])
    recorder = study.FillStudyRecorder(tmp_path, "test", capture_mode="trades")
    monkeypatch.setattr(study, "_recorder", recorder)
    try:
        study.observe_event(_book())
        study.observe_event(_book("other:no", venue="POLYMARKET"))
        recorder._queue.join()
        if not idle:
            clock[0] = 12_500_000_000
            study.observe_event(_book())
            recorder._queue.join()
        clock[0] = 15_000_000_000
        signal = _signal()
        signal.opportunity.detected_at = Timestamp(datetime.fromtimestamp(
            (wall_offset + clock[0]) / 1_000_000_000, tz=timezone.utc))
        study.observe_event(signal)
        recorder._queue.join()
        clock[0] += delay_ms * 1_000_000
        study.observe_execution_window("signal", ("predict:42:yes", "other:no"), signal_id="signal")
        recorder._queue.join()
    finally:
        recorder.close()
    rows = _rows(tmp_path)
    window = next(row["data"] for row in rows if row["kind"] == "execution_window")
    assert window["pre_start_mono_ns"] == clock[0] - 3_000_000_000
    baseline = next(row for row in rows if row["kind"] == "book" and row["data"]["contract"] == "other:no")
    assert baseline["mono_ns"] == baseline["data"]["arrival_at_ns"] == 10_000_000_000
    assert baseline["wall_ns"] == baseline["data"]["arrival_wall_at_ns"] == wall_offset + 10_000_000_000
    assert window["prehistory_boundary_books"]["other:no"] == {
        key: baseline[key] for key in ("run_id", "seq", "mono_ns", "loss_epoch")}
    sample = signal_samples(rows, horizons_ms=(100,))[0]
    pre = next(value for value in sample["pretrade_movement"] if value["ms"] == 2000)
    assert pre["complete"] is complete
    assert pre["reason"] == (None if complete else "outside_capture_window")
    if idle and complete:
        assert pre["observed_span_ms"] == 0
        window["prehistory_boundary_books"]["predict:42:yes"]["seq"] += 1000
        corrupted = signal_samples(rows, horizons_ms=(100,))[0]["pretrade_movement"]
        assert next(value for value in corrupted if value["ms"] == 2000)["reason"] == "missing_book"
    manifest = json.loads((recorder.path / "manifest.json").read_text())
    assert manifest["pre_window_seconds"] == 2
    assert manifest["history_retention_seconds"] == 3
    assert manifest["trigger_grace_seconds"] == 1


def test_lost_predecessor_interval_cannot_be_certified_by_idle_book(tmp_path, monkeypatch):
    """A genuine lost sample prevents an old idle baseline proving continuity."""
    clock = [10_000_000_000]
    monkeypatch.setattr(study.time, "monotonic_ns", lambda: clock[0])
    recorder = study.FillStudyRecorder(tmp_path, "test", capture_mode="trades")
    monkeypatch.setattr(study, "_recorder", recorder)
    try:
        study.observe_event(_book())
        recorder._queue.join()
        recorder.record_error("queue_full")
        clock[0] = 15_000_000_000
        study.observe_execution_window("execution", ("predict:42:yes",))
    finally:
        recorder.close()
    rows = _rows(tmp_path)
    window = next(row["data"] for row in rows if row["kind"] == "execution_window")
    assert window["prehistory_loss_epoch"] == 1
    assert window["prehistory_boundary_books"] == {}
    assert not any(row["kind"] == "book" for row in rows)
    assert recorder.status()["counts"]["dropped"] == 1


def test_idle_predecessors_have_fixed_capacity_and_do_not_survive_eviction(tmp_path, monkeypatch):
    """Contract churn cannot grow idle baselines or fabricate a retired baseline."""
    clock = [10_000_000_000]
    monkeypatch.setattr(study.time, "monotonic_ns", lambda: clock[0])
    monkeypatch.setattr(study, "BOUNDARY_CAPACITY", 2)
    recorder = study.FillStudyRecorder(tmp_path, "test", capture_mode="trades")
    monkeypatch.setattr(study, "_recorder", recorder)
    try:
        for index in range(4):
            study.observe_event(_book(f"predict:{index}:yes", str(index)))
        recorder._queue.join()
        clock[0] = 15_000_000_000
        study.observe_execution_window("execution", ("predict:0:yes", "predict:3:yes"))
    finally:
        recorder.close()
    rows = _rows(tmp_path)
    window = next(row["data"] for row in rows if row["kind"] == "execution_window")
    assert set(window["prehistory_boundary_books"]) == {"predict:3:yes"}
    assert [row["data"]["contract"] for row in rows if row["kind"] == "book"] == ["predict:3:yes"]
    assert recorder.status()["counts"]["boundary_high_watermark"] == 2
    assert recorder.status()["counts"]["boundary_capacity_evictions"] == 2


def test_capacity_loss_after_idle_baseline_remains_a_capture_barrier(tmp_path, monkeypatch):
    """Keeping an old predecessor must not conceal discarded intermediate updates."""
    clock = [10_000_000_000]
    monkeypatch.setattr(study.time, "monotonic_ns", lambda: clock[0])
    monkeypatch.setattr(study, "HISTORY_CAPACITY", 2)
    recorder = study.FillStudyRecorder(tmp_path, "test", capture_mode="trades")
    monkeypatch.setattr(study, "_recorder", recorder)
    try:
        study.observe_event(_book())
        recorder._queue.join()
        clock[0] = 14_000_000_000
        recorder.offer("test", {})
        recorder._queue.join()
        for index in range(3):
            clock[0] += 1_000_000
            study.observe_event(_book(f"predict:{index}:yes", str(index)))
            recorder._queue.join()
        study.observe_execution_window("execution", ("predict:42:yes",))
    finally:
        recorder.close()
    rows = _rows(tmp_path)
    window = next(row["data"] for row in rows if row["kind"] == "execution_window")
    assert window["prehistory_truncated"] is True
    assert window["pre_start_mono_ns"] > 14_001_000_000
    assert window["prehistory_boundary_books"] == {}
    assert not any(row["kind"] == "book" for row in rows)
    assert recorder.status()["counts"]["history_capacity_evictions"] == 1


def test_rotation_preserves_history_links_segments_and_exposes_final_exhaustion(tmp_path, monkeypatch):
    """Fill only unused slots, retain every prefix, and stop explicitly at total capacity."""
    monkeypatch.setattr(study, "SLOTS", 3)
    historical = tmp_path / "slot-00"
    historical.mkdir()
    evidence = historical / "historical.json"
    evidence.write_bytes(b"retained evidence")
    recorder = study.FillStudyRecorder(tmp_path, "test", byte_limit=2500, capture_mode="trades")
    try:
        for index in range(30):
            recorder.offer("test", {"index": index, "value": "x" * 300})
    finally:
        recorder.close()
    assert evidence.read_bytes() == b"retained evidence"
    assert len(list(tmp_path.glob("slot-*"))) == 3
    assert all(path.stat().st_size <= 2500 for path in tmp_path.glob("slot-*/events.jsonl"))
    first = json.loads((tmp_path / "slot-01" / "summary.json").read_text())
    last = json.loads((tmp_path / "slot-02" / "summary.json").read_text())
    manifest = json.loads((tmp_path / "slot-02" / "manifest.json").read_text())
    assert first["status"] == "rotated"
    assert first["next_slot"] == "slot-02"
    assert manifest["previous_slot"] == "slot-01"
    assert manifest["run_id"] == first["run_id"] == last["run_id"]
    assert manifest["segment_index"] == 1
    assert last["status"] == recorder.status()["status"] == "disk_limit"
    assert recorder.status()["counts"]["slot_exhausted"] == 1
    records = [row for row in _rows(tmp_path) if row["kind"] == "test"]
    assert [row["data"]["index"] for row in records] == list(range(len(records)))
    assert {row["segment_index"] for row in records} == {0, 1}
    recorder.offer("test", {})
    assert recorder.status()["counts"]["offers_after_stop"] == 1


def test_stalled_writer_keeps_callbacks_fast_and_lost_window_observable(tmp_path, monkeypatch):
    """A blocked serializer cannot occupy producer threads or hide a dropped trigger."""
    entered, release = threading.Event(), threading.Event()
    serialize = study._serialize

    def stalled(kind, value):
        entered.set()
        release.wait(3)
        return serialize(kind, value)

    monkeypatch.setattr(study, "_serialize", stalled)
    recorder = study.FillStudyRecorder(tmp_path, "test", capacity=1, capture_mode="trades")
    monkeypatch.setattr(study, "_recorder", recorder)
    try:
        study.observe_event(_book())
        assert entered.wait(1)
        study.observe_event(_book())
        started = time.perf_counter()
        for _ in range(200):
            study.observe_execution_window("execution", ("predict:42:yes",))
            study.capture_status()
        assert time.perf_counter() - started < 0.5
        status = study.capture_status()
        assert status["queue_depth"] == 1
        assert status["counts"]["queue_full"] == 200
        assert status["active_windows"] == 0
    finally:
        release.set()
        recorder.close()
    summary = json.loads((recorder.path / "summary.json").read_text())
    assert summary["counts"]["dropped"] == 200
    assert summary["pending_at_stop"] == 0


def test_prepared_capture_keeps_exact_native_size_without_signed_private_data():
    """Keep native rounded sizing distinct from application intent and redact signatures."""
    intent = SimpleNamespace(contract_id="predict:42:yes", side=OrderSide.BUY,
        quantity=Quantity(Decimal("5.123456")), limit_price=Price(Decimal("0.4")),
        created_at=Timestamp.now())
    native = {"data": {"strategy": "LIMIT", "pricePerShare": "400000000000000000",
        "order": {"hash": "0xabc", "makerAmount": "2049360000000000000",
            "takerAmount": "5123400000000000000", "side": 0,
            "signature": "SECRET", "maker": "PRIVATE"}}}
    prepared = SimpleNamespace(command=SimpleNamespace(execution_id="execution", role="primary",
        venue_id="PREDICT", intent=intent), prepared=SimpleNamespace(
        reference=SimpleNamespace(client_order_id="client"), request=json.dumps(native)))
    data = study._serialize("prepared", prepared)
    assert data["quantity"] == "5.123456"
    assert data["native"]["takerAmount"] == "5123400000000000000"
    assert data["intent_created_wall_ns"] > 0
    assert "SECRET" not in json.dumps(data)
    assert "PRIVATE" not in json.dumps(data)


def test_preparation_observation_captures_bounded_decision_revisions(tmp_path, monkeypatch):
    """Retain immutable decision identities and actual sizes before and after preparation."""
    level = OrderBookLevel(Price(Decimal("0.4")), Quantity(Decimal("2")))
    snapshot = OrderBookDecisionSnapshot(VenueID("PREDICT"), "predict:42:yes", OrderSide.BUY,
        Price(Decimal("0.4")), Quantity(Decimal("5")), (level,) * 40,
        None, 1_000_000, None, Timestamp.now())
    execution = SimpleNamespace(id="execution", leg1_decision=snapshot, leg2_decision=None,
        leg1_client_order_id="client", leg2_client_order_id="other-client")
    planned = SimpleNamespace(execution=execution)
    command = SimpleNamespace(execution_id="execution")
    opportunity = _signal()
    requested = events.ExecutionPreparationRequested(opportunity, planned, (command,), 0, 0)
    batch = events.PreparedExecutionBatch(opportunity, planned, (command,), (), 0)
    recorder = study.FillStudyRecorder(tmp_path, "test", capture_mode="trades")
    monkeypatch.setattr(study, "_recorder", recorder)
    try:
        study.observe_event(requested)
        study.observe_event(batch)
    finally:
        recorder.close()
    decisions = [row["data"] for row in _rows(tmp_path) if row["kind"] == "decision"]
    assert [decision["phase"] for decision in decisions] == ["preparation_requested", "prepared_batch"]
    assert len(snapshot.levels) == 40
    assert all(len(decision["levels"]) == 20 and decision["levels_truncated"] for decision in decisions)
    assert all(decision["source_hash"] is None and decision["book_timestamp_wall_ns"] is None for decision in decisions)
    assert all(decision["client_id"] == "client" and decision["quantity"] == "5" for decision in decisions)
    assert recorder.status()["counts"]["windows_opened"] == 1


def test_rotation_io_failure_reports_error_without_touching_old_evidence(tmp_path, monkeypatch):
    """Distinguish an unavailable filesystem from exhaustion of reserved slot capacity."""
    recorder = study.FillStudyRecorder(tmp_path, "test", byte_limit=1500)

    def unavailable():
        raise OSError("read-only filesystem")

    monkeypatch.setattr(recorder, "_reserve_slot", unavailable)
    try:
        for index in range(10):
            recorder.offer("test", {"index": index, "value": "x" * 300})
    finally:
        recorder.close()
    status = recorder.status()
    assert status["status"] == "io_error"
    assert status["counts"]["writer_io_errors"] == 1
    assert status["counts"].get("slot_exhausted", 0) == 0
    assert (recorder.path / "events.jsonl").stat().st_size <= 1500
    assert json.loads((recorder.path / "summary.json").read_text())["status"] == "io_error"
