"""Verify bounded, disposable Predict experiments without live venue access."""

import json
import multiprocessing
import os
import threading
import time
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest

from prediction_markets.application import events
from prediction_markets.domain.orderbook.entities import OrderBook
from prediction_markets.domain.orderbook.value_objects import OrderBookLevel
from prediction_markets.domain.shared.value_objects import MarketID, OutcomeID, Price, Quantity, Timestamp, VenueID
from prediction_markets.domain.trading.enums import OrderSide
from prediction_markets.infrastructure.observability import predict_fill_study as study


@pytest.fixture
def analysis():
    """Load the optional local analyzer without skipping runtime-only tests."""
    return pytest.importorskip("repo_tools.predict_fill_analysis")


def _signal():
    """Build a minimal typed opportunity for exercising diagnostic serialization."""
    level = OrderBookLevel(Price(Decimal("0.4")), Quantity(Decimal(5)))
    pair = SimpleNamespace(left=SimpleNamespace(id="predict:42:yes", venue_id="PREDICT"),
        right=SimpleNamespace(id="other:no", venue_id="POLYMARKET"))
    opportunity = SimpleNamespace(side=OrderSide.BUY, quantity=Quantity(Decimal(5)),
        left_level=level, right_level=level, fee_per_contract=Decimal("0.01"),
        net_edge=Decimal("0.19"), detected_at=Timestamp.now())
    return events.ArbitrageOpportunityFound("signal", None, pair, opportunity)


def _book():
    """Return a deeply immutable book with transport and source timestamps."""
    mono, wall = time.monotonic_ns(), time.time_ns()
    level = OrderBookLevel(Price(Decimal("0.4")), Quantity(Decimal(5)))
    return OrderBook(MarketID("42"), OutcomeID("yes"), (), (level,),
        source_at_ns=wall - 300_000_000, arrival_wall_at_ns=wall,
        arrival_at_ns=mono, received_at_ns=mono, source_timestamp_kind="venue_update")


def test_capture_owns_only_diagnostic_files_and_redacts_native_fields(tmp_path, monkeypatch, analysis):
    """Capture books and native order outcomes without credentials or order signatures."""
    root = tmp_path / "predict-fill-study"
    recorder = study.FillStudyRecorder(root, "test")
    monkeypatch.setattr(study, "_recorder", recorder)
    try:
        study.observe_event(events.OrderBookUpdated(VenueID("PREDICT"), "predict:42:yes", _book()))
        study.observe_predict_payload({"marketId": 42, "settlementsPending": {"asks": None, "bids": "bad"}}, time.time_ns())
        study.observe_event(_signal())
        study.observe_native({"type": "orderCancelled", "orderHash": "0xabc", "jwt": "SECRET"})
        study.observe_native({"type": "orderTransactionSuccess", "orderHash": "0xabc",
            "fill": {"executedSizeWei": "5", "signature": "SECRET"}, "signature": "SECRET"})
    finally:
        recorder.close()
    items = analysis.inventory(root)
    assert len(items) == 1
    assert items[0]["manifest"]["code_sha256"]
    assert {entry["path"].rsplit("\\", 1)[-1].rsplit("/", 1)[-1] for entry in items[0]["files"]} == set(study.OWNED_FILES)
    rows, incomplete = analysis.read_capture(recorder.path)
    assert not incomplete
    assert "SECRET" not in json.dumps(rows)
    assert {row["kind"] for row in rows} >= {"book", "signal", "private", "predict_payload"}
    assert len([r for r in rows if r["kind"] == "private"]) == 2
    assert json.loads((recorder.path / "summary.json").read_text())["counts"].get("observer_errors", 0) == 0
    assert analysis.analyze(root)["signals"] == 1


def test_full_queue_does_not_block_producers_and_capture_is_bounded(tmp_path, monkeypatch):
    """Make the writer stall while the bounded producer queue drops with evidence."""
    gate, entered = threading.Event(), threading.Event()
    serialize = study._serialize
    def blocked(kind, value):
        entered.set()
        gate.wait(2)
        return serialize(kind, value)
    monkeypatch.setattr(study, "_serialize", blocked)
    recorder = study.FillStudyRecorder(tmp_path / "study", "test", capacity=1, byte_limit=1024)
    try:
        recorder.offer("test", {"value": 1})
        assert entered.wait(1)
        started = time.perf_counter()
        for _ in range(100):
            recorder.offer("test", {"value": 1})
        assert time.perf_counter() - started < 0.5
        assert recorder._queue.qsize() <= 1
        assert recorder._counts["dropped"] >= 99
    finally:
        gate.set()
        recorder.close()
    assert (recorder.path / "events.jsonl").stat().st_size <= 1024
    assert json.loads((recorder.path / "summary.json").read_text())["counts"]["dropped"] >= 99


def test_observer_failures_do_not_escape_and_disabled_mode_creates_nothing(tmp_path, monkeypatch):
    """A malformed sample is disposable, not an exception in market-data processing."""
    monkeypatch.delenv("PREDICT_FILL_STUDY", raising=False)
    monkeypatch.setenv("JOURNAL_PATH", str(tmp_path / "trading.log"))
    monkeypatch.setattr(study, "_recorder", None)
    study.start_study("test")
    assert not (tmp_path / "predict-fill-study").exists()
    recorder = study.FillStudyRecorder(tmp_path / "study", "test")
    monkeypatch.setattr(study, "_recorder", recorder)
    try:
        study.observe_event(events.OrderBookUpdated(VenueID("PREDICT"), "contract", None))
        assert recorder._counts["observer_errors"] == 1
    finally:
        recorder.close()


def test_clean_requires_a_closed_owned_run_and_preserves_journal(tmp_path, analysis):
    """Only explicitly confirmed, manifest-owned diagnostic artifacts are removable."""
    journal = tmp_path / "trading.log"
    journal.write_bytes(b"financial evidence")
    root = tmp_path / "study"
    recorder = study.FillStudyRecorder(root, "test")
    try:
        with pytest.raises(ValueError, match="not closed"):
            analysis.clean(root, recorder.run_id)
    finally:
        recorder.close()
    with pytest.raises(ValueError, match="Unknown"):
        analysis.clean(root, "wrong-id")
    analysis.clean(root, recorder.run_id)
    assert analysis.inventory(root) == []
    assert journal.read_bytes() == b"financial evidence"


def _row(kind, data, offset=0, loss=0):
    """Build synthetic process-local evidence on independent wall and monotonic clocks."""
    return {"schema": 1, "run_id": "run", "kind": kind, "data": data,
        "mono_ns": 1_000_000_000 + offset * 1_000_000,
        "wall_ns": 1_800_000_000_000_000_000 + offset * 1_000_000, "loss_epoch": loss}


def _rows():
    """Show visible depth disappearing after 150 ms without claiming an actual fill."""
    rows = []
    for contract, venue, offset in (("other:no", "POLYMARKET", -20), ("predict:42:yes", "PREDICT", -10)):
        book = {"contract": contract, "venue": venue, "asks": [["0.4", "10"]], "bids": [],
            "asks_truncated": False, "bids_truncated": False, "source_at_ns": 1_799_999_999_700_000_000,
            "arrival_wall_at_ns": 1_800_000_000_000_000_000 + offset * 1_000_000,
            "arrival_at_ns": 1_000_000_000 + offset * 1_000_000}
        rows.append(_row("book", book, offset))
    rows.append(_row("signal", {"signal_id": "signal", "pair": ["predict:42:yes", "other:no"],
        "side": "buy", "quantity": "5", "limits": ["0.4", "0.4"], "fee_per_contract": "0.01"}))
    rows.append(_row("book", {**rows[1]["data"], "asks": [["0.7", "10"]],
        "arrival_wall_at_ns": 1_800_000_000_150_000_000, "arrival_at_ns": 1_150_000_000}, 150))
    rows.append(_row("heartbeat", {"queue_depth": 0}, 600))
    return rows


def test_horizon_analysis_separates_source_age_local_age_and_new_confirmation(analysis):
    """Keep raw source delay separate and never treat a cached book as a new update."""
    sample = analysis.signal_samples(_rows())[0]
    assert sample["predict_source_age_ms"] == 300
    assert sample["predict_local_age_ms"] == 10
    assert sample["predict_depth_ratio"] == "2"
    assert [p["displayed_executable"] for p in sample["horizons"]] == [True, False, False]
    assert sample["horizons"][0]["new_predict_confirmation"] is False
    assert sample["horizons"][1]["new_predict_confirmation"] is True
    rows = _rows()
    rows[-1]["loss_epoch"] = 1
    assert all(p["reason"] == "capture_loss" for p in analysis.signal_samples(rows)[0]["horizons"])
    assert all(p["displayed_executable"] is None for p in analysis.signal_samples(rows)[0]["horizons"])


def test_outcomes_use_unique_successes_not_cancelled_application_status(analysis):
    """A cancelled order can be filled; a cancelled zero-fill snapshot stays unknown."""
    order = {"venue": "PREDICT", "client_id": "client", "quantity": "5", "native": {"hash": "0xABC"}}
    success = {"type": "orderTransactionSuccess", "orderHash": "0xabc", "settlementId": "one",
        "fill": {"executedSizeWei": str(5 * 10**18)}}
    rows = [_row("prepared", order), _row("private", success), _row("private", success),
        _row("private", {"type": "orderCancelled", "orderHash": "0xabc"})]
    assert analysis.order_samples(rows)[0]["confirmed_filled"] == "5"
    assert analysis.order_samples(rows)[0]["outcome"] == "filled"
    assert analysis.order_samples([rows[0], rows[-1]])[0]["outcome"] == "unresolved"


@pytest.mark.parametrize("evidence,state", (
    ([], "unknown"),
    ([("submission", {"client_id": "client", "status": "rejected"})], "unknown"),
    ([("submit_started", {"client_id": "client"})], "attempt_observed"),
    ([("submission", {"client_id": "client", "status": "accepted"})], "venue_observed"),
    ([("venue_verification", {"hash": "0xabc", "status": "UNKNOWN", "filled": None})], "unknown"),
    ([("venue_verification", {"hash": "0xabc", "status": "cancelled", "filled": "0"})], "venue_observed"),
))
def test_order_analysis_separates_preparation_attempt_and_venue_evidence(evidence, state, analysis):
    """Missing capture and application rejection cannot establish a venue no-fill."""
    order = {"venue": "PREDICT", "client_id": "client", "quantity": "5", "native": {"hash": "0xabc"}}
    result = analysis.order_samples([_row("prepared", order), *(_row(kind, data) for kind, data in evidence)])[0]
    assert result["submission_state"] == state
    assert result["outcome"] == ("submission_unknown" if state == "unknown" else "unresolved")
    assert result["confirmed_filled"] == "0"


def test_venue_comparison_excludes_prepared_orders_without_submission_evidence(tmp_path, monkeypatch, analysis):
    """Four locally rejected preparations must not become four venue execution outcomes."""
    rows = []
    for index in range(5):
        client = f"client-{index}"
        rows.append(_row("prepared", {"venue": "PREDICT", "client_id": client,
            "execution_id": f"execution-{index}", "role": "hedge", "quantity": "5",
            "native": {"hash": f"0x{index}", "strategy": "LIMIT", "isFillOrKill": False}}))
        rows.append(_row("submission", {"client_id": client, "status": "rejected" if index else "accepted"}))
    rows.extend((_row("submit_started", {"client_id": "client-0"}),
        _row("private", {"orderHash": "0x0", "type": "orderCancelled"})))
    monkeypatch.setattr(analysis, "inventory", lambda root: [{"slot": str(tmp_path),
        "manifest": {"run_id": "run", "producer": "test"}}])
    monkeypatch.setattr(analysis, "read_capture", lambda slot: (rows, False))
    report = analysis.analyze(tmp_path)
    assert len(report["orders"]) == 5
    assert report["outcomes"] == {"unresolved": 1, "submission_unknown": 4}
    assert report["submission_states"] == {"venue_observed": 1, "unknown": 4}
    assert sum(group["count"] for group in report["comparisons"]) == 1
    assert report["comparisons"][0]["submission_state"] == "venue_observed"

    rows.extend((_row("prepared", {"venue": "PREDICT", "client_id": "attempt-only",
        "execution_id": "attempt-execution", "role": "hedge", "quantity": "5",
        "native": {"hash": "0xattempt", "strategy": "LIMIT", "isFillOrKill": False}}),
        _row("submit_started", {"client_id": "attempt-only"})))
    comparisons = analysis.analyze(tmp_path)["comparisons"]
    assert {group["submission_state"]: group["count"] for group in comparisons} == {
        "venue_observed": 1, "attempt_observed": 1,
    }


def test_horizons_accept_later_writer_drain_without_using_future_books(analysis):
    """A transient writer backlog is not data loss once its FIFO drains intact."""
    rows = _rows()
    rows.extend(_row("heartbeat", {"queue_depth": 1}, ms) for ms in (110, 260, 510))
    horizons = analysis.signal_samples(rows)[0]["horizons"]
    assert [p["reason"] for p in horizons] == [None, None, None]
    assert [p["displayed_executable"] for p in horizons] == [True, False, False]

    rows[4]["loss_epoch"] = 1
    assert all(p["reason"] == "capture_loss" for p in analysis.signal_samples(rows)[0]["horizons"])

    rows[4]["data"]["queue_depth"] = 1
    rows[4]["loss_epoch"] = 0
    assert all(p["reason"] == "writer_backlog" for p in analysis.signal_samples(rows)[0]["horizons"])


def test_disk_and_slot_limits_stop_only_optional_capture(tmp_path, monkeypatch):
    """Stop optional recording at its caps without deleting existing evidence."""
    monkeypatch.setattr(study, "SLOTS", 1)
    recorder = study.FillStudyRecorder(tmp_path / "study", "test", byte_limit=64)
    recorder.offer("test", {"value": "x" * 512})
    recorder.close()
    assert (recorder.path / "events.jsonl").stat().st_size <= 64
    assert json.loads((recorder.path / "summary.json").read_text())["status"] == "disk_limit"
    with pytest.raises(OSError, match="slots are full"):
        study.FillStudyRecorder(tmp_path / "study", "test")


def _closed_run(root, slots, run_id, started):
    """Create a complete synthetic run with deliberately independent slot order."""
    root.mkdir(exist_ok=True)
    for segment, index in enumerate(slots):
        slot = root / f"slot-{index:02d}"
        slot.mkdir()
        (slot / "manifest.json").write_text(json.dumps({"schema": study.SCHEMA,
            "owner": "predict-fill-study", "files": list(study.OWNED_FILES), "run_id": run_id,
            "producer": "fixture", "pid": os.getpid(), "started_wall_ns": started,
            "segment_index": segment, "event_byte_limit": study.FILE_LIMIT,
            "previous_slot": f"slot-{slots[segment - 1]:02d}" if segment else None}), encoding="utf-8")
        last = segment + 1 == len(slots)
        (slot / "summary.json").write_text(json.dumps({"schema": study.SCHEMA, "run_id": run_id,
            "segment_index": segment, "status": "closed" if last else "rotated",
            "next_slot": None if last else f"slot-{slots[segment + 1]:02d}",
            "bytes": 3, "total_bytes": 3 * (segment + 1), "pending_at_stop": 0,
            "active_windows": [], "counts": {}, "ended_wall_ns": started + 1}), encoding="utf-8")
        (slot / "events.jsonl").write_bytes(b"{}\n")


def test_full_storage_reuses_oldest_whole_run_and_reports_retention(tmp_path):
    """Reuse the oldest run by its start time, preserving newer slots byte for byte."""
    _closed_run(tmp_path, (27, 31), "oldest", 1)
    for index in range(study.SLOTS):
        if index not in (27, 31):
            _closed_run(tmp_path, (index,), f"newer-{index}", index + 2)
    before = {path: path.read_bytes() for path in tmp_path.glob("slot-*/*")
        if path.parent.name not in {"slot-27", "slot-31"}}
    recorder = study.FillStudyRecorder(tmp_path, "replacement")
    try:
        assert recorder.path.name == "slot-27"
        assert not (tmp_path / "slot-31").exists()
        status = recorder.status()
        assert status["slot_limit"] == 32
        assert status["queue_capacity"] == 512
        assert status["counts"]["retention_runs_removed"] == 1
        assert status["counts"]["retention_slots_removed"] == 2
        assert status["counts"]["retention_bytes_removed"] == 6
    finally:
        recorder.close()
    assert all(path.read_bytes() == value for path, value in before.items())
    assert len(list(tmp_path.glob("slot-*"))) == 31
    assert json.loads((recorder.path / "summary.json").read_text())["counts"]["retention_runs_removed"] == 1


@pytest.mark.parametrize("damage", (
    "foreign_file", "missing_manifest", "unfinished_summary", "summary_schema", "run_id",
    "index", "previous_slot", "next_slot", "bytes", "total_bytes", "pending", "window",
    "disk_limit", "io_error", "oversized_metadata", "current_run",
))
def test_retention_protects_every_segment_of_uncertain_or_current_runs(tmp_path, monkeypatch, damage):
    """Reject a whole run before removing any file when one segment is uncertain."""
    monkeypatch.setattr(study, "SLOTS", 2)
    _closed_run(tmp_path, (0, 1), "retained", 1)
    slot = tmp_path / "slot-01"
    summary_path = slot / "summary.json"
    summary = json.loads(summary_path.read_text())
    if damage == "foreign_file":
        (slot / "unrelated.log").write_bytes(b"foreign evidence")
    elif damage == "missing_manifest":
        (slot / "manifest.json").unlink()
    elif damage == "unfinished_summary":
        summary_path.write_bytes(b'{"status":"closed"')
    elif damage == "oversized_metadata":
        summary_path.write_bytes(b" " * (study.METADATA_LIMIT + 1))
    elif damage == "previous_slot":
        path = slot / "manifest.json"
        manifest = json.loads(path.read_text())
        path.write_text(json.dumps({**manifest, "previous_slot": "../journal"}))
    elif damage == "current_run":
        monkeypatch.setattr(study.uuid, "uuid4", lambda: SimpleNamespace(hex="retained"))
    else:
        key, value = {"summary_schema": ("schema", 999), "run_id": ("run_id", "other"),
            "index": ("segment_index", 2), "next_slot": ("next_slot", "slot-00"),
            "bytes": ("bytes", 0), "total_bytes": ("total_bytes", 3),
            "pending": ("pending_at_stop", 1), "window": ("active_windows", [{}]),
            "disk_limit": ("status", "disk_limit"), "io_error": ("status", "io_error")}[damage]
        summary_path.write_text(json.dumps({**summary, key: value}))
    before = {path: path.read_bytes() for path in tmp_path.glob("slot-*/*")}
    with pytest.raises(FileExistsError, match="no completely closed run"):
        study.FillStudyRecorder(tmp_path, "new")
    assert {path: path.read_bytes() for path in tmp_path.glob("slot-*/*")} == before


@pytest.mark.parametrize("name", study.OWNED_FILES)
def test_retention_refuses_hard_linked_foreign_files(tmp_path, monkeypatch, name):
    """Keep shared filesystem objects outside the retention ownership boundary."""
    monkeypatch.setattr(study, "SLOTS", 1)
    root = tmp_path / "study"
    _closed_run(root, (0,), "retained", 1)
    target = root / "slot-00" / name
    foreign = tmp_path / "foreign.json"
    evidence = target.read_bytes()
    foreign.write_bytes(evidence)
    target.unlink()
    os.link(foreign, target)
    with pytest.raises(FileExistsError, match="no completely closed run"):
        study.FillStudyRecorder(root, "new")
    assert target.read_bytes() == foreign.read_bytes() == evidence


def test_retention_protects_active_writer_and_publishes_only_after_stream_close(tmp_path, monkeypatch):
    """An active slot becomes reusable only after its writer closes the event stream."""
    monkeypatch.setattr(study, "SLOTS", 1)
    recorder = study.FillStudyRecorder(tmp_path, "active")
    write_metadata = recorder._write_metadata
    observed = []

    def checked(name, value):
        if name == "summary.json":
            observed.append(recorder._stream.closed)
        write_metadata(name, value)

    monkeypatch.setattr(recorder, "_write_metadata", checked)
    try:
        with pytest.raises(FileExistsError, match="no completely closed run"):
            study.FillStudyRecorder(tmp_path, "new")
        assert recorder.status()["status"] == "running"
    finally:
        recorder.close()
    assert observed == [True]
    replacement = study.FillStudyRecorder(tmp_path, "replacement")
    replacement.close()
    assert replacement.status()["counts"]["retention_runs_removed"] == 1


def test_slow_retention_stays_off_producers_and_preserves_current_run_prefix(tmp_path, monkeypatch):
    """A blocked writer retention scan leaves queue pressure bounded and callbacks fast."""
    monkeypatch.setattr(study, "SLOTS", 2)
    _closed_run(tmp_path, (0,), "old", 1)
    recorder = study.FillStudyRecorder(tmp_path, "active", capacity=1, byte_limit=900)
    entered, release = threading.Event(), threading.Event()
    closed_runs = study._closed_runs

    def delayed(root, current_run_id):
        entered.set()
        release.wait(3)
        return closed_runs(root, current_run_id)

    monkeypatch.setattr(study, "_closed_runs", delayed)
    try:
        recorder.offer("test", {"value": "x" * 650})
        assert entered.wait(2)
        started = time.perf_counter()
        for _ in range(100):
            recorder.offer("test", {"value": "x" * 650})
            recorder.status()
        assert time.perf_counter() - started < 0.5
        assert recorder.status()["queue_depth"] == 1
        assert recorder.status()["counts"]["queue_full"] == 99
    finally:
        release.set()
        recorder.close()
    status = recorder.status()
    assert status["counts"]["retention_runs_removed"] == 1
    assert status["counts"]["slot_exhausted"] == 1
    assert status["status"] == "disk_limit"
    manifests = [json.loads(path.read_text()) for path in tmp_path.glob("slot-*/manifest.json")]
    assert {manifest["run_id"] for manifest in manifests} == {recorder.run_id}
    assert {manifest["segment_index"] for manifest in manifests} == {0, 1}
    assert all(path.stat().st_size <= 900 for path in tmp_path.glob("slot-*/events.jsonl"))


def _spawn_capture(root, ready, release):
    """Keep a real spawned recorder active until its parent inspects all reservations."""
    recorder = study.FillStudyRecorder(Path(root), "spawned")
    try:
        ready.put((recorder.run_id, recorder.path.name))
        if not release.wait(10):
            raise TimeoutError("Parent did not release the spawned capture")
        recorder.offer("test", {"pid": os.getpid()})
    finally:
        recorder.close()


def test_concurrent_spawned_recorders_reuse_slots_without_collisions_or_orphans(tmp_path, analysis):
    """Several real processes reserve distinct slots while deleting old runs as groups."""
    _closed_run(tmp_path, (0, 1), "oldest", 1)
    for index in range(2, study.SLOTS):
        _closed_run(tmp_path, (index,), f"old-{index}", index)
    context = multiprocessing.get_context("spawn")
    ready, release = context.Queue(), context.Event()
    workers = [context.Process(target=_spawn_capture, args=(str(tmp_path), ready, release)) for _ in range(4)]
    try:
        for worker in workers:
            worker.start()
        captures = [ready.get(timeout=15) for _ in workers]
        assert len({run_id for run_id, slot in captures}) == len(workers)
        assert {slot for run_id, slot in captures} == {"slot-00", "slot-01", "slot-02", "slot-03"}
        assert all(not (tmp_path / slot / "summary.json").exists() for run_id, slot in captures)
    finally:
        release.set()
        for worker in workers:
            worker.join(timeout=15)
            if worker.is_alive():
                worker.terminate()
                worker.join(timeout=5)
        ready.close()
        ready.join_thread()
    assert all(worker.exitcode == 0 for worker in workers)
    items = analysis.inventory(tmp_path)
    assert len(items) == study.SLOTS
    assert all({Path(entry["path"]).name for entry in item["files"]} == set(study.OWNED_FILES) for item in items)
    assert {item["manifest"]["run_id"] for item in items} >= {run_id for run_id, slot in captures}
    assert all((tmp_path / slot / "events.jsonl").stat().st_size <= study.FILE_LIMIT for run_id, slot in captures)


def test_journal_observer_failure_cannot_invalidate_financial_append(tmp_path):
    """A failing diagnostic callback leaves the already appended event recoverable."""
    from prediction_markets.infrastructure.binary_journal import BinaryJournal
    def broken(event):
        raise RuntimeError("diagnostic only")
    event = events.TradingSafetyStop(VenueID("PREDICT"), "test", Timestamp.now())
    journal = BinaryJournal(tmp_path / "trading.log", on_append=broken)
    try:
        assert journal.append(event).sequence == 1
        assert journal.entries()[0].event == event
    finally:
        journal.close()


def test_local_producer_microbenchmark_reports_overhead_and_loss(tmp_path, monkeypatch):
    """Measure only synthetic producer cost, never live venue/container latency."""
    event = events.OrderBookUpdated(VenueID("PREDICT"), "predict:42:yes", _book())
    monkeypatch.setattr(study, "_recorder", None)
    results = {}
    count = 5000
    started = time.perf_counter_ns()
    for _ in range(count):
        study.observe_event(event)
    results["disabled_us_per_offer"] = (time.perf_counter_ns() - started) / count / 1000
    recorder = study.FillStudyRecorder(tmp_path / "study", "microbenchmark")
    monkeypatch.setattr(study, "_recorder", recorder)
    try:
        started = time.perf_counter_ns()
        for _ in range(count):
            study.observe_event(event)
        results["enabled_us_per_offer"] = (time.perf_counter_ns() - started) / count / 1000
    finally:
        recorder.close()
    results["dropped"] = recorder._counts["dropped"]
    results["queue_high_watermark"] = recorder._counts["queue_high_watermark"]
    assert results["queue_high_watermark"] <= 512
    assert recorder._counts["enqueued"] + results["dropped"] == count
    print(json.dumps(results, sort_keys=True))
