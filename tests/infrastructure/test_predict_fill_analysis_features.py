"""Check causal ordering and explicit uncertainty in offline fill-study features."""

import json

import pytest

from repo_tools import predict_fill_analysis as analysis


def _row(kind, data, ms=0, *, loss=0):
    """Use a stable host clock with room for a complete two-second pre-window."""
    return {"schema": 1, "run_id": "run", "kind": kind, "loss_epoch": loss,
        "mono_ns": 10_000_000_000 + ms * 1_000_000,
        "wall_ns": 1_800_000_000_000_000_000 + ms * 1_000_000, "data": data}


def _book(ms, price="0.4", size="17", contract="predict:42:yes", *, truncated=False):
    """Record source identity independently from local observation time."""
    row = _row("book", {"contract": contract, "venue": "PREDICT" if contract.startswith("predict:") else "POLYMARKET",
        "asks": [[price, size]], "bids": [["0.3", size]], "asks_truncated": truncated,
        "bids_truncated": False, "source_hash": f"{contract}:{ms}"}, ms)
    row["data"].update(source_at_ns=row["wall_ns"] - 30_000_000,
        arrival_at_ns=row["mono_ns"], arrival_wall_at_ns=row["wall_ns"])
    return row


def _capture():
    """Keep an executable pair visible through a complete post-submission horizon."""
    return [_book(-2000), _book(-2000, "0.5", contract="other:no"), _book(-500),
        _row("signal", {"signal_id": "execution", "pair": ["predict:42:yes", "other:no"],
            "quantity": "17", "side": "buy", "limits": ["0.4", "0.5"], "fee_per_contract": "0.01"}),
        _row("prepared", {"execution_id": "execution", "role": "hedge", "venue": "PREDICT",
            "client_id": "client", "contract": "predict:42:yes", "side": "buy", "quantity": "5",
            "limit": "0.42", "native": {"hash": "0xabc", "strategy": "LIMIT"}}, 20),
        _row("prepared", {"execution_id": "execution", "role": "primary", "venue": "POLYMARKET",
            "client_id": "peer", "contract": "other:no", "side": "buy", "quantity": "5",
            "limit": "0.5", "native": {}}, 20),
        _row("submit_started", {"client_id": "client"}, 100),
        _row("heartbeat", {"queue_depth": 0}, 1800)]


def _report(tmp_path, monkeypatch, rows):
    """Analyze synthetic input without creating a recorder or accessing a venue."""
    monkeypatch.setattr(analysis, "inventory", lambda root: [{"slot": str(tmp_path),
        "manifest": {"run_id": "run", "producer": "test"}}])
    monkeypatch.setattr(analysis, "read_capture", lambda slot: (rows, False))
    return analysis.analyze(tmp_path)


def test_actual_order_size_and_limit_replace_signal_parameters(tmp_path, monkeypatch):
    """A size-17 worker signal must not turn a real size-5 order's cushion into 1x."""
    rows = _capture()
    rows[2] = _book(-500, "0.42")
    report = _report(tmp_path, monkeypatch, rows)
    order = report["orders"][0]
    feature = order["features"]
    assert feature["quantity"] == "5"
    assert feature["signal_quantity"] == "17"
    assert feature["limits"] == ["0.42", "0.5"]
    assert feature["predict_depth_ratio"] == "3.4"
    assert feature["initial_visibility"]["displayed_executable"] is True
    assert feature["post_send"]["horizons"][-1]["ms"] == 1500
    assert feature["post_send"]["horizons"][-1]["observed_continuous_executable"] is True
    assert report["signal_samples"][0]["initial_visibility"]["displayed_executable"] is False
    assert report["comparisons"][0]["quantity"] == "5"


def test_predecision_movement_cannot_use_future_books_or_venue_backdating(tmp_path, monkeypatch):
    """A post-decision jump with an earlier venue timestamp cannot enter pre-features."""
    rows = _capture()
    rows[2] = _book(-500, "0.38", "20")
    rows.extend([_book(-100, "0.4", "17"), _book(1, "0.99", "1")])
    rows[-1]["data"]["source_at_ns"] = _row("test", {}, -10000)["wall_ns"]
    feature = _report(tmp_path, monkeypatch, rows)["orders"][0]["features"]
    movement = feature["pretrade_movement"][0]
    assert movement["complete"] is True
    assert movement["observation_count"] == 2
    assert movement["quote_range"] == "0.02"
    assert movement["max_abs_quote_jump"] == "0.02"
    assert movement["visible_quantity_depletion"] == "3"
    assert movement["book_updates_per_second"] == 2
    assert feature["quantity_persistence"]["first_observed_loss_ms"] == 1


def test_returning_opportunity_is_not_continuous_survival():
    """An executable endpoint cannot hide the observed interruption before it."""
    rows = _capture() + [_book(150, "0.9"), _book(225)]
    sample = analysis.signal_samples(rows)[0]
    assert [point["displayed_executable"] for point in sample["horizons"]] == [True, True, True]
    assert [point["observed_continuous_executable"] for point in sample["horizons"]] == [True, False, False]
    lifetime = sample["quantity_persistence"]
    assert lifetime["first_observed_loss_ms"] == 150
    assert lifetime["first_loss_right_censored"] is False
    assert lifetime["reappearance_count"] == 1
    assert lifetime["episodes"][0]["end_ms"] == 150
    assert lifetime["episodes"][1]["start_ms"] == 225
    assert lifetime["episodes"][1]["end_right_censored"] is True
    rows.append(_row("signal", {**rows[3]["data"], "signal_id": "returned"}, 300))
    assert [point["episode_id"] for point in analysis.signal_samples(rows)] == ["execution", "returned"]


def test_complete_window_before_later_disk_stop_is_preserved(tmp_path):
    """A file-level disk stop only censors windows lacking a prior successful drain."""
    rows = _capture()
    (tmp_path / "events.jsonl").write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")
    (tmp_path / "summary.json").write_text(json.dumps({"status": "disk_limit", "pending_at_stop": 3}), encoding="utf-8")
    loaded, incomplete = analysis.read_capture(tmp_path)
    assert incomplete is True
    sample = analysis.signal_samples(loaded, incomplete=incomplete, horizons_ms=(500, 1500, 2500))[0]
    assert [point["displayed_executable"] for point in sample["horizons"]] == [True, True, None]
    assert sample["horizons"][-1]["observed_continuous_executable"] is None
    assert sample["quantity_persistence"]["first_observed_loss_ms"] is None
    assert sample["quantity_persistence"]["first_loss_right_censored"] is True


def test_truncated_missing_and_damaged_evidence_never_become_no_fill(tmp_path):
    """Unknown book size or capture loss is distinct from observed insufficient size."""
    rows = _capture() + [_book(50, size="1", truncated=True)]
    assert all(point["reason"] == "truncated_depth" for point in analysis.signal_samples(rows)[0]["horizons"])
    assert all(point["displayed_executable"] is None for point in analysis.signal_samples(rows)[0]["horizons"])
    rows = [row for row in _capture() if row["kind"] != "book" or row["data"]["contract"] != "other:no"]
    assert all(point["reason"] == "missing_book" for point in analysis.signal_samples(rows)[0]["horizons"])
    rows = _capture()
    content = "".join(json.dumps(row) + "\n" for row in rows[:-1]) + "{damaged}\n" + json.dumps(rows[-1]) + "\n"
    (tmp_path / "events.jsonl").write_text(content, encoding="utf-8")
    loaded, incomplete = analysis.read_capture(tmp_path)
    assert incomplete is True
    assert all(point["reason"] == "capture_gap" for point in analysis.signal_samples(loaded)[0]["horizons"])
    assert analysis.order_samples(loaded)[0]["outcome_group"] == "unknown"


def test_finalized_chain_zero_differs_from_cancelled_zero_and_cannot_erase_fill():
    """Only terminal chain proof establishes zero; successful native fills never regress."""
    rows = _capture()
    cancelled = _row("snapshot", {"client_id": "client", "status": "cancelled", "filled": "0",
        "source": "get", "may_receive_more_fills": False}, 200)
    assert analysis.order_samples(rows + [cancelled])[0]["outcome"] == "unresolved"
    cancelled["data"]["settlement_finalized_block"] = 1234
    assert analysis.order_samples(rows + [cancelled])[0]["outcome_group"] == "proven_zero"
    success = _row("private", {"orderHash": "0xabc", "type": "orderTransactionSuccess", "settlementId": "settlement",
        "fill": {"executedSizeWei": str(5 * 10**18)}}, 150)
    smaller_duplicate = _row("private", {**success["data"], "fill": {"executedSizeWei": str(2 * 10**18)}}, 190)
    filled = analysis.order_samples(rows + [success, smaller_duplicate, cancelled])[0]
    assert filled["outcome_group"] == "filled"
    assert filled["confirmed_filled"] == "5"
    partial = analysis.order_samples(rows + [smaller_duplicate, cancelled])[0]
    assert partial["outcome_group"] == "partial"


def test_recovery_uses_its_own_side_size_and_decision_time(tmp_path, monkeypatch):
    """Original execution identity cannot attach sell features to a recovery buy."""
    rows = _capture()
    rows[3]["data"]["side"] = "sell"
    rows[4]["data"]["side"] = "sell"
    recovery = {**rows[4]["data"], "client_id": "recovery", "role": "recovery", "side": "buy", "quantity": "2",
        "limit": "0.45", "native": {"hash": "0xrecovery"},
        "intent_created_wall_ns": _row("test", {}, 600)["wall_ns"]}
    rows.extend([_row("prepared", recovery, 650), _row("submit_started", {"client_id": "recovery"}, 700)])
    orders = _report(tmp_path, monkeypatch, rows)["orders"]
    feature = orders[1]["features"]
    assert feature["role"] == "recovery"
    assert feature["side"] == "buy"
    assert feature["quantity"] == "2"
    assert feature["limits"] == ["0.45"]
    assert feature["mono_ns"] == _row("test", {}, 600)["mono_ns"]
    assert feature["anchor_kind"] == "intent_creation_wall_clock_mapped"
    assert feature["initial_visibility"]["net_edge_estimate_frozen_fees"] is None


def test_explicit_window_end_prevents_survival_across_inactive_capture():
    """A later drained heartbeat from another window cannot extend an ended one."""
    rows = _capture()
    window = {"execution_id": "execution", "contracts": ["predict:42:yes", "other:no"],
        "pre_start_mono_ns": _row("test", {}, -2000)["mono_ns"],
        "window_start_mono_ns": _row("test", {})["mono_ns"],
        "window_end_mono_ns": _row("test", {}, 15000)["mono_ns"]}
    rows += [_row("execution_window", window), _row("execution_window_end", {
        **window, "end_mono_ns": _row("test", {}, 200)["mono_ns"], "reason": "recorder_closed"}, 200)]
    sample = analysis.signal_samples(rows)[0]
    assert sample["horizons"][0]["displayed_executable"] is True
    assert [point["reason"] for point in sample["horizons"]][1:] == ["unfinished_window", "unfinished_window"]
    assert sample["horizons"][-1]["observed_continuous_executable"] is None


def test_decision_revision_matches_actual_parameters_and_book_identity(tmp_path, monkeypatch):
    """Use the final matching decision while retaining its original revision context."""
    rows = _capture()
    old = {"execution_id": "execution", "client_id": "client", "role": "hedge", "contract": "predict:42:yes",
        "side": "buy", "quantity": "5", "limit": "0.4", "source_hash": "predict:42:yes:-500",
        "captured_wall_ns": _row("test", {}, -10)["wall_ns"]}
    new = {**old, "limit": "0.42", "source_hash": "predict:42:yes:5",
        "captured_wall_ns": _row("test", {}, 10)["wall_ns"]}
    rows += [_row("decision", old), _book(5, "0.42"), _row("decision", new, 15), _book(16, "0.99")]
    feature = _report(tmp_path, monkeypatch, rows)["orders"][0]["features"]
    assert feature["anchor_kind"] == "decision_snapshot"
    assert feature["decision_book_identity_match"] is True
    assert feature["book_identities"][0]["source_hash"] == "predict:42:yes:5"
    assert feature["predict_depth_ratio"] == "3.4"
    assert feature["mono_ns"] == _row("test", {}, 10)["mono_ns"]
    assert len(feature["decision_revisions"]) == 2
    assert feature["post_send"]["initial_visibility"]["displayed_executable"] is False


def test_analyze_never_creates_artifacts_or_a_verification_recorder(tmp_path, monkeypatch):
    """The reporting command must remain read-only even when capture is enabled."""
    monkeypatch.setenv("PREDICT_FILL_STUDY", "1")
    monkeypatch.setattr(analysis, "FillStudyRecorder", lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("write")))
    before = list(tmp_path.iterdir())
    assert _report(tmp_path, monkeypatch, _capture())["orders"]
    assert list(tmp_path.iterdir()) == before


def test_final_checkpoint_after_preparation_anchors_exact_books_without_lookahead(tmp_path, monkeypatch):
    """Use the guard book, retain prior phases, and ignore post-submit evidence."""
    rows = _capture()
    identity = {"execution_id": "execution", "client_id": "client", "role": "hedge",
        "contract": "predict:42:yes", "side": "buy", "quantity": "5", "limit": "0.42"}
    for ms, phase, price in ((30, "validation_preparation", "0.41"), (90, "guard", "0.42"),
                              (110, "guard", "0.99")):
        book = _book(ms - 5, price, "25")
        point = _row("book_checkpoint", {**identity, "phase": phase,
            "captured_mono_ns": _row("clock", {}, ms)["mono_ns"],
            "captured_wall_ns": _row("clock", {}, ms)["wall_ns"], "book": book["data"]}, ms)
        rows += [book, point]
    order = _report(tmp_path, monkeypatch, rows)["orders"][0]
    feature = order["features"]
    assert feature["decision_snapshot"]["phase"] == "guard"
    assert feature["mono_ns"] == _row("clock", {}, 90)["mono_ns"]
    assert feature["predict_source_age_ms"] == 35
    assert feature["predict_local_age_ms"] == 5
    assert feature["predict_depth_ratio"] == "5"
    assert feature["decision_to_submit_ms"] == 10
    assert len(feature["decision_revisions"]) == 2
    assert len(order["book_checkpoints"]) == 3


def test_sparse_parent_checkpoints_do_not_prove_liquidity_survival():
    """A healthy parent heartbeat is not evidence of all worker market updates."""
    rows = _capture()
    for row in rows:
        row["producer"] = "parent"
    rows[3]["data"]["intent_id"] = "worker-generation:1"
    sample = analysis.signal_samples(rows)[0]
    assert all(point["reason"] == "sparse_parent_capture" for point in sample["horizons"])
    assert all(point["displayed_executable"] is None for point in sample["horizons"])
    rows[3]["data"].pop("intent_id")
    single_process = analysis.signal_samples(rows)[0]
    assert all(point["reason"] is None for point in single_process["horizons"])


def test_native_rounding_controls_fill_denominator_and_visible_size(tmp_path, monkeypatch):
    """A fully executed rounded request is not a partial fill of its earlier intent."""
    rows = _capture()
    rows[4]["data"]["quantity"] = "5.00009"
    rows[4]["data"]["native"].update(side=0, makerAmount=str(21 * 10**17),
        takerAmount=str(5 * 10**18), pricePerShare=str(42 * 10**16))
    rows.append(_row("private", {"orderHash": "0xabc", "type": "orderTransactionSuccess", "settlementId": "fill",
        "fill": {"executedSizeWei": str(5 * 10**18)}}, 150))
    order = _report(tmp_path, monkeypatch, rows)["orders"][0]
    assert order["outcome_group"] == "filled"
    assert order["quantity"] == "5"
    assert order["requested_quantity"] == "5.00009"
    assert order["parameter_source"] == "signed_request"
    assert order["features"]["predict_depth_ratio"] == "3.4"


def test_missing_rotation_segment_preserves_only_pre_gap_windows(tmp_path, monkeypatch):
    """A continuation heartbeat cannot conceal a missing segment between books."""
    early = _capture()[:-1] + [_row("heartbeat", {"queue_depth": 0}, 110)]
    late = [_row("heartbeat", {"queue_depth": 0}, 1800)]
    monkeypatch.setattr(analysis, "inventory", lambda root: [
        {"slot": str(tmp_path / "slot-00"), "manifest": {"run_id": "run", "producer": "test", "segment_index": 0}},
        {"slot": str(tmp_path / "slot-02"), "manifest": {"run_id": "run", "producer": "test", "segment_index": 2,
            "previous_slot": "slot-01"}}])
    monkeypatch.setattr(analysis, "read_capture", lambda slot: (early if slot.name == "slot-00" else late, False))
    sample = analysis.analyze(tmp_path)["signal_samples"][0]
    assert sample["horizons"][0]["displayed_executable"] is True
    assert all(point["reason"] == "capture_gap" for point in sample["horizons"][1:])


def test_unfinished_tail_does_not_rewrite_an_earlier_drained_window(tmp_path):
    """A concurrent partial JSON record only invalidates windows reaching its boundary."""
    (tmp_path / "events.jsonl").write_text(
        "".join(json.dumps(row) + "\n" for row in _capture()) + '{"schema":1', encoding="utf-8")
    loaded, incomplete = analysis.read_capture(tmp_path)
    assert incomplete is True
    assert all(point["displayed_executable"] is True
        for point in analysis.signal_samples(loaded, incomplete=incomplete)[0]["horizons"])


def test_later_unproven_verification_cannot_replace_terminal_chain_proof():
    """Retain finalized evidence when a later REST cancellation still reports zero."""
    verified = {"hash": "0xabc", "status": "cancelled", "filled": "0",
        "may_receive_more_fills": False, "settlement_finalized_block": 1234}
    rows = _capture() + [_row("venue_verification", verified, 500),
        _row("venue_verification", {"hash": "0xabc", "status": "cancelled", "filled": "0"}, 600)]
    assert analysis.order_samples(rows)[0]["outcome_group"] == "proven_zero"


def test_delayed_signal_copy_uses_detection_time_for_predecision_features(tmp_path, monkeypatch):
    """The parent's later signal copy cannot admit books observed after detection."""
    rows = _capture()
    rows[3] = _row("signal", {**rows[3]["data"], "detected_wall_ns": _row("test", {})["wall_ns"]}, 300)
    rows.append(_book(50, "0.01"))
    feature = _report(tmp_path, monkeypatch, rows)["orders"][0]["features"]
    assert feature["mono_ns"] == _row("test", {})["mono_ns"]
    assert feature["pretrade_movement"][0]["quote_range"] == "0.0"
    assert feature["book_identities"][0]["source_hash"] == "predict:42:yes:-500"


def _owned_slot(root, index, run_id, segment, status, *, previous=None, following=None):
    """Create a test-only owned diagnostic slot without running a writer."""
    slot = root / f"slot-{index:02d}"
    slot.mkdir()
    (slot / "manifest.json").write_text(json.dumps({"schema": analysis.SCHEMA,
        "owner": "predict-fill-study", "files": list(analysis.OWNED_FILES), "run_id": run_id,
        "producer": "test", "segment_index": segment, "previous_slot": previous}), encoding="utf-8")
    (slot / "summary.json").write_text(json.dumps({"schema": analysis.SCHEMA, "run_id": run_id,
        "segment_index": segment, "status": status, "next_slot": following}), encoding="utf-8")
    (slot / "events.jsonl").write_text("test evidence\n", encoding="utf-8")
    return slot


@pytest.mark.parametrize("status,previous", [("running", "slot-00"), ("closed", "slot-15")])
def test_cleanup_validates_all_segments_before_deleting_any(tmp_path, status, previous):
    """An active or mislinked later segment prevents partial deletion of its run."""
    _owned_slot(tmp_path, 0, "run", 0, "rotated", following="slot-01")
    _owned_slot(tmp_path, 1, "run", 1, status, previous=previous)
    _owned_slot(tmp_path, 2, "unrelated", 0, "closed")
    with analysis.study_storage_lock(tmp_path):
        pass
    before = {path: path.read_bytes() for path in tmp_path.rglob("*") if path.is_file()}
    with pytest.raises(ValueError):
        analysis.clean(tmp_path, "run")
    assert {path: path.read_bytes() for path in tmp_path.rglob("*") if path.is_file()} == before


def test_cleanup_removes_all_closed_rotated_segments_and_preserves_other_runs(tmp_path):
    """Remove only the complete explicitly named run, including every continuation."""
    first = _owned_slot(tmp_path, 0, "run", 0, "rotated", following="slot-01")
    last = _owned_slot(tmp_path, 1, "run", 1, "disk_limit", previous="slot-00")
    other = _owned_slot(tmp_path, 2, "unrelated", 0, "closed")
    before = {path: path.read_bytes() for path in other.iterdir()}
    analysis.clean(tmp_path, "run")
    assert not first.exists()
    assert not last.exists()
    assert {path: path.read_bytes() for path in other.iterdir()} == before
