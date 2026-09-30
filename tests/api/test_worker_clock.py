"""Keep same-host IPC ages independent of adjustable wall-clock timestamps."""

import asyncio
import queue
from dataclasses import replace

import pytest

from prediction_markets.api.runtime.market_workers import _validate_worker_opportunity, _WorkerTelemetry
from prediction_markets.application.events import OrderBookPairUpdated, OrderBookUpdated
from prediction_markets.application.worker_validation import WorkerValidationReason as Reason
from tests.api.test_worker_opportunity_validation import _setup, _supervisor


@pytest.mark.parametrize("wall_step", [-2_000_000_000, 2_000_000_000])
def test_wall_steps_do_not_rewrite_local_books_or_reject_fresh_ipc(wall_step):
    """Wall steps affect diagnostics, not local request ordering or arrival ages."""
    async def check():
        engine, journal, request = _setup()
        request = replace(request, sent_wall_at_ns=request.sent_wall_at_ns + wall_step)
        for key, book in tuple(engine.state.books.items()):
            engine.state.books[key] = replace(book, arrival_wall_at_ns=book.arrival_wall_at_ns + wall_step)
        supervisor = _supervisor(request, timeout=0.1)
        task = asyncio.create_task(supervisor.validate_opportunity(request))
        await asyncio.sleep(0)
        reply = _validate_worker_opportunity(request, engine, journal)
        assert reply.reason is Reason.ACCEPTED
        supervisor._consume_validation(replace(reply, responded_wall_at_ns=request.sent_wall_at_ns - wall_step * 2))
        result = await task
        assert result.reason is Reason.ACCEPTED
        assert result.left_order_book.received_at_ns == engine.state.books[request.pair.left.id].received_at_ns
        assert result.left_order_book.arrival_at_ns == engine.state.books[request.pair.left.id].arrival_at_ns
        original = next(iter(journal._validation_intents.values()))
        original = replace(original, sent_wall_at_ns=original.sent_wall_at_ns + wall_step)
        await supervisor._consume_message(original)
        event = supervisor._pipeline.sink.events[-1]
        assert isinstance(event, OrderBookPairUpdated)
        assert event.left_order_book.received_at_ns == original.left_order_book.received_at_ns
    asyncio.run(check())


def test_worker_summary_keeps_signed_source_age_and_existing_metric():
    """Preserve a negative raw age without renaming the legacy clamped series."""
    engine, _, request = _setup()
    book = engine.state.books[request.pair.left.id]
    book = replace(book, source_at_ns=book.arrival_wall_at_ns + 250_000_000)
    telemetry = _WorkerTelemetry("clock-test", "generation")
    telemetry.observe_processing(OrderBookUpdated(request.pair.left.venue_id, request.pair.left.id, book))
    summary = telemetry.snapshot(0.001, queue.Queue())
    timing = summary.timings[0]
    assert timing.source_to_transport_max_seconds == 0
    assert timing.source_to_transport_raw_min_seconds == -0.25
    assert timing.source_to_transport_raw_max_seconds == -0.25
    assert timing.future_source_samples == 1
    assert summary.clock["discontinuities"] == 0
