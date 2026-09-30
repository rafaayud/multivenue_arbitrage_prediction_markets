"""Verify real dispatch checkpoints survive bounded capture and offline analysis."""

import asyncio
import time
from dataclasses import replace

import pytest

from prediction_markets.domain.shared.value_objects import ClientOrderID, ContractID, VenueID
from prediction_markets.infrastructure.observability import predict_fill_study as study
from tests.api.test_market_workers import _book, _pair
from tests.application import test_worker_dispatch_validation as dispatch_fixtures
from tests.application.test_worker_dispatch_validation import Reason, _reply, _SignedExecution

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


def test_final_worker_book_reaches_guard_report_after_preparation_mutation(tmp_path, monkeypatch):
    """A real two-check dispatch reports the last validated books, not its signed-plan books."""
    async def run():
        root = tmp_path / "capture"
        recorder = study.FillStudyRecorder(root, "parent", capture_mode="trades")
        monkeypatch.setattr(study, "_recorder", recorder)
        engine, staged, journal, _, dispatcher, adapters = _stage_predict(tmp_path, monkeypatch)
        responses = []

        async def validate(request):
            index = len(responses) + 1
            response = _reply(request, Reason.ACCEPTED,
                right_price="0.48" if index == 1 else "0.46",
                left_price="0.39" if index == 1 else "0.38")
            mono, wall = time.monotonic_ns(), time.time_ns()
            response = replace(response,
                left_order_book=replace(response.left_order_book,
                    source_hash=f"left-check-{index}"),
                right_order_book=replace(response.right_order_book,
                    source_hash=f"predict-check-{index}", source_at_ns=wall - 200_000_000,
                    arrival_wall_at_ns=wall, arrival_at_ns=mono, received_at_ns=mono),
                right_book_generation=f"predict-generation-{index}")
            responses.append(response)
            return response

        dispatcher.set_worker_validation(validate)
        try:
            await dispatcher._dispatch_prepared_execution(staged)
            assert len(responses) == 2
            assert all(len(adapter.submitted) == 1 for adapter in adapters.values())
        finally:
            await dispatcher.close()
            journal.close()
            recorder.close()

        rows, incomplete = analysis.read_capture(recorder.path)
        assert incomplete is False
        assert recorder.status()["counts"].get("dropped", 0) == 0
        order = next(order for order in analysis.analyze(root)["orders"] if order["venue"] == "PREDICT")
        checkpoints = {item["phase"]: item for item in order["features"]["book_checkpoints"]}
        assert set(checkpoints) >= {"validation_preparation", "validation_final", "guard", "submission"}
        for index, phase in enumerate(("validation_preparation", "validation_final")):
            checkpoint = checkpoints[phase]
            assert checkpoint["request_id"] == responses[index].request.request_id
            assert checkpoint["book_generation"] == f"predict-generation-{index + 1}"
            assert checkpoint["book"]["source_hash"] == f"predict-check-{index + 1}"
            _assert_checkpoint_ages(checkpoint, responses[index].right_order_book)
        for phase in ("guard", "submission"):
            assert checkpoints[phase]["book"]["asks"][0][0] == "0.46"
            _assert_checkpoint_ages(checkpoints[phase], responses[-1].right_order_book)
        feature = order["features"]
        assert feature["decision_snapshot"]["phase"] == "guard"
        assert feature["decision_snapshot"]["book"]["source_hash"] == "predict-check-2"
        assert feature["predict_source_age_ms"] == pytest.approx(checkpoints["guard"]["source_age_ms"])
        assert feature["predict_local_age_ms"] == pytest.approx(checkpoints["guard"]["local_age_ms"])
        initial = next(row for row in rows if row["kind"] == "decision"
            and row["data"]["venue"] == "PREDICT")
        assert initial["data"]["levels"][0][0] == "0.5"
        assert engine.state.books[staged.opportunity.pair.right.id].source_hash == "predict-check-2"

    asyncio.run(run())


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
