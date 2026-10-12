"""Verify real dispatch checkpoints survive bounded capture and offline analysis."""

import asyncio
from dataclasses import replace

import pytest

from prediction_markets.domain.shared.value_objects import ClientOrderID, ContractID, VenueID
from prediction_markets.infrastructure.observability import predict_fill_study as study
from tests.application.test_dispatch_preparation import _book, _pair
from tests.application import test_dispatch_preparation as dispatch_fixtures
from tests.application.test_dispatch_preparation import _SignedExecution

analysis = pytest.importorskip("repo_tools.predict_fill_analysis")


class _CapturedExecution(_SignedExecution):
    """Emit the adapter's normal submit marker without a venue or authentication client."""

    def prepare(self, intent):
        """Retain the adapter's JSON envelope without a signature or network request."""
        prepared = replace(super().prepare(intent), request=b'{"data":{"strategy":"LIMIT"}}')
        self.prepared_orders[-1] = prepared
        return prepared

    def submit(self, order):
        study.observe_submission(order.reference)
        return super().submit(order)


class _Snapshot:
    """Count only the recovery snapshot request already required by dispatch."""

    def __init__(self, book):
        self.book = book
        self.calls = 0

    async def get_order_book(self, _contract):
        self.calls += 1
        return self.book


def _stage_predict(tmp_path, monkeypatch):
    """Reuse the real staged-engine fixture with a Predict complementary contract."""
    cycle, pair, left, _ = _pair()
    right = replace(pair.right, id=ContractID("predict:42:no"), venue_id=VenueID("PREDICT"))
    pair = replace(pair, right=right)
    monkeypatch.setattr(dispatch_fixtures, "_pair", lambda: (cycle, pair, left, _book(right, "0.5")))
    engine, request, journal, inputs, dispatcher, _ = dispatch_fixtures._staged(tmp_path)
    monkeypatch.setattr(journal, "_on_append", study.observe_event)
    adapters = {contract.venue_id: _CapturedExecution(journal, contract.venue_id)
        for contract in (pair.left, pair.right)}
    dispatcher.configure(adapters, market_data={venue: dispatch_fixtures._ForbiddenSnapshots()
        for venue in adapters})
    return engine, request, journal, inputs, dispatcher, adapters


def _assert_checkpoint_ages(checkpoint, book):
    """Check ages against retained raw clocks, not recorder enqueue or write time."""
    captured = checkpoint["book"]
    assert captured["source_at_ns"] == book.source_at_ns
    assert captured["arrival_at_ns"] == book.arrival_at_ns
    assert captured["arrival_wall_at_ns"] == book.arrival_wall_at_ns
    if book.source_at_ns is None:
        assert checkpoint["source_age_ms"] is None
        assert checkpoint["source_age_unavailable_reason"] == "missing_source_timestamp"
    else:
        assert checkpoint["source_age_ms"] == pytest.approx(
            (checkpoint["captured_wall_ns"] - book.source_at_ns) / 1_000_000)
        assert checkpoint["source_age_unavailable_reason"] is None
    local = book.arrival_at_ns if book.arrival_at_ns is not None else captured["received_at_ns"]
    assert checkpoint["local_age_ms"] == pytest.approx(
        (checkpoint["captured_mono_ns"] - local) / 1_000_000)


@pytest.mark.parametrize("origin,known_source", [("in_memory", True), ("rest_snapshot", True), ("rest_snapshot", False)])
def test_recovery_capture_preserves_age_or_explicit_unknown_without_extra_io(
    tmp_path, monkeypatch, origin, known_source,
):
    """Recovery checkpoints preserve source clocks and add no snapshot or authentication work."""
    async def run():
        root = tmp_path / "capture"
        recorder = study.FillStudyRecorder(root, "parent", capture_mode="trades")
        monkeypatch.setattr(study, "_recorder", recorder)
        engine, staged, journal, _, dispatcher, adapters = _stage_predict(tmp_path, monkeypatch)
        command = next(command for command in staged.commands if str(command.venue_id) == "PREDICT")
        command = replace(command, role="recovery", intent=replace(command.intent,
            client_order_id=ClientOrderID("opportunity-recovery-1")))
        book = replace(_book(staged.opportunity.pair.right, "0.48"), source_hash="recovery-book")
        if not known_source:
            book = replace(book, source_at_ns=None, arrival_at_ns=None, arrival_wall_at_ns=None,
                source_timestamp_kind=None)
        if origin == "in_memory":
            engine.state.books[command.intent.contract_id] = book
        snapshot = _Snapshot(book)
        dispatcher.configure(adapters, market_data={command.venue_id: snapshot} if origin == "rest_snapshot" else {})
        try:
            await dispatcher._submit_single(command)
            assert snapshot.calls == (1 if origin == "rest_snapshot" else 0)
            assert len(adapters[command.venue_id].submitted) == 1
        finally:
            await dispatcher.close()
            journal.close()
            recorder.close()

        report = analysis.analyze(root)
        order = next(order for order in report["orders"] if order["role"] == "recovery")
        feature = order["features"]
        checkpoints = {item["phase"]: item for item in feature["book_checkpoints"]}
        checkpoint = checkpoints["recovery_preparation"]
        assert checkpoint["book_origin"] == origin
        _assert_checkpoint_ages(checkpoint, book)
        assert feature["decision_snapshot"]["phase"] == "recovery_preparation"
        assert feature["predict_source_age_ms"] == checkpoint["source_age_ms"]
        assert feature["predict_local_age_ms"] == checkpoint["local_age_ms"]
        assert checkpoints["submission"]["book"]["source_hash"] == "recovery-book"
        assert checkpoints["submission"]["book_origin"] == "recovery_selected"
        if origin == "rest_snapshot":
            assert checkpoints["recovery_snapshot_received"]["book"]["received_at_ns"] == book.received_at_ns
        if not known_source:
            assert checkpoint["local_clock_basis"] == "adapter_received"
        assert recorder.status()["counts"].get("observer_errors", 0) == 0

    asyncio.run(run())


def test_guard_capture_reads_a_local_update_received_during_signing(tmp_path, monkeypatch):
    """Retain the latest local book and its clocks at guard and submission."""
    async def run():
        root = tmp_path / "capture"
        recorder = study.FillStudyRecorder(root, "parent", capture_mode="trades")
        monkeypatch.setattr(study, "_recorder", recorder)
        engine, staged, journal, _, dispatcher, adapters = _stage_predict(tmp_path, monkeypatch)
        contract = staged.opportunity.pair.right
        latest = replace(_book(contract, "0.48"), source_hash="during-signing")
        adapter = adapters[contract.venue_id]
        prepare = adapter.prepare

        def prepare_and_update(intent):
            result = prepare(intent)
            engine.state.books[contract.id] = latest
            return result

        monkeypatch.setattr(adapter, "prepare", prepare_and_update)
        try:
            await dispatcher._dispatch_prepared_execution(staged)
            assert all(len(value.submitted) == 1 for value in adapters.values())
        finally:
            await dispatcher.close()
            journal.close()
            recorder.close()
        order = next(value for value in analysis.analyze(root)["orders"] if value["venue"] == "PREDICT")
        checkpoints = {value["phase"]: value for value in order["features"]["book_checkpoints"]}
        for phase in ("guard", "submission"):
            assert checkpoints[phase]["book"]["source_hash"] == "during-signing"
            _assert_checkpoint_ages(checkpoints[phase], latest)
        assert order["features"]["decision_snapshot"]["phase"] == "guard"

    asyncio.run(run())
