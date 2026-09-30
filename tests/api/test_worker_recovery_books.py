"""Verify read-only recovery book RPC identity, bounds, and process lifecycle."""

import asyncio
import queue
import time
from dataclasses import replace

import pytest

from prediction_markets.api.runtime import market_workers
from prediction_markets.api.runtime.market_workers import (
    _CONTROL_QUEUE_CAPACITY,
    _apply_worker_controls,
    _read_worker_recovery_books,
    _wait_for_process_stop,
)
from prediction_markets.application.events import MarketMatchesUpdated, OrderBookPairUpdated
from prediction_markets.application.recovery_books import (
    RECOVERY_BOOKS_TIMEOUT_NS,
    WorkerRecoveryBooksRequest,
    WorkerRecoveryBooksResult,
)
from prediction_markets.application.worker_validation import WorkerValidationReason as Reason
from prediction_markets.domain.shared.value_objects import ContractID
from tests.api.test_market_workers import _book, _detected
from tests.api.test_worker_opportunity_validation import _setup, _supervisor


def _recovery_setup():
    """Reuse the real validation fixture with approved parent partition matches."""
    engine, journal, template = _setup()
    supervisor = _supervisor(template)
    supervisor._worker_matches[template.cycle] = MarketMatchesUpdated(template.cycle, (template.pair,))
    contracts = template.pair.left.id, template.pair.right.id
    return engine, journal, template, supervisor, contracts


async def _start_request(supervisor, template, contracts):
    task = asyncio.create_task(supervisor.recovery_books(template.execution_id, template.validation_ref, contracts))
    await asyncio.sleep(0)
    return task, supervisor._controls[template.validation_ref.partition].get_nowait()


def test_latest_unprofitable_books_survive_evicted_intent_and_keep_original_clocks():
    """Recovery returns changed books in requested order despite lost arbitrage edge."""
    async def check():
        engine, journal, template, supervisor, contracts = _recovery_setup()
        latest = replace(_book(template.pair.right, "0.8"), source_hash="changed")
        engine.state.books[contracts[1]] = latest
        journal._validation_intents.clear()
        task, request = await _start_request(supervisor, template, contracts[::-1])
        reply = _read_worker_recovery_books(request, journal)
        supervisor._consume_recovery_books(reply)
        books = await task
        assert reply.reason is Reason.ACCEPTED
        assert books == (latest, engine.state.books[contracts[0]])
        assert books[0] is latest
        assert books[0].arrival_at_ns == latest.arrival_at_ns
        assert books[0].received_at_ns == latest.received_at_ns
        assert books[0].source_at_ns == latest.source_at_ns
        assert books[0].arrival_wall_at_ns == latest.arrival_wall_at_ns
        assert not supervisor._pending_recovery_books
    asyncio.run(check())


@pytest.mark.parametrize("change,expected", [
    ("partition", Reason.GENERATION), ("generation", Reason.GENERATION),
    ("old_request", Reason.TIMEOUT), ("future_request", Reason.TIMEOUT),
    ("blank_execution", Reason.IDENTITY), ("blank_request", Reason.IDENTITY),
    ("different_pair", Reason.IDENTITY), ("repeated_contract", Reason.IDENTITY),
    ("wrong_book", Reason.IDENTITY), ("missing_book", Reason.MISSING_BOOK),
])
def test_worker_recovery_rejects_invalid_request_and_book_identity(change, expected):
    """Worker ownership and approved book identities remain mandatory for recovery."""
    engine, journal, template, _, _ = _recovery_setup()
    request = WorkerRecoveryBooksRequest(
        "request", template.execution_id, template.validation_ref, (template.pair.left, template.pair.right),
    )
    if change in ("partition", "generation"):
        field = "partition" if change == "partition" else "process_generation"
        request = replace(request, validation_ref=replace(request.validation_ref, **{field: "old"}))
    elif change in ("old_request", "future_request"):
        offset = -2 * RECOVERY_BOOKS_TIMEOUT_NS if change == "old_request" else RECOVERY_BOOKS_TIMEOUT_NS
        request = replace(request, sent_monotonic_at_ns=time.monotonic_ns() + offset)
    elif change == "blank_execution":
        request = replace(request, execution_id="")
    elif change == "blank_request":
        request = replace(request, request_id="")
    elif change == "different_pair":
        engine.state.matches.clear()
    elif change == "repeated_contract":
        request = replace(request, contracts=(template.pair.left, template.pair.left))
    elif change == "wrong_book":
        engine.state.books[template.pair.left.id] = engine.state.books[template.pair.right.id]
    else:
        engine.state.books.pop(template.pair.left.id)
    reply = _read_worker_recovery_books(request, journal)
    assert reply.reason is expected
    assert reply.books is None
    assert reply.partition == journal._partition
    assert reply.process_generation == journal._process_generation


@pytest.mark.parametrize("action,expected", [
    ("timeout", "timeout"), ("restart", "generation"), ("shutdown", "shutdown"),
    ("failure", "unavailable"), ("unhealthy", "unavailable"),
    ("wrong_execution", "identity"), ("wrong_contract_order", "identity"),
    ("wrong_responder", "generation"), ("wrong_partition", "generation"),
    ("wrong_books", "identity"), ("missing_books", "missing_book"),
    ("before_request", "timeout"), ("future_response", "timeout"),
])
def test_supervisor_recovery_waiters_fail_closed_and_cleanup(action, expected, monkeypatch):
    """Malformed, stale, unavailable, and late replies cannot supply recovery quotes."""
    async def check():
        _, journal, template, supervisor, contracts = _recovery_setup()
        if action == "timeout":
            monkeypatch.setattr(market_workers, "RECOVERY_BOOKS_TIMEOUT_NS", 5_000_000)
        task, request = await _start_request(supervisor, template, contracts)
        reply = _read_worker_recovery_books(request, journal)
        if action == "restart":
            supervisor._process_generations["btc-5m"] = "new"
        elif action == "shutdown":
            supervisor._stopping = True
            supervisor._resolve_recovery_books(Reason.SHUTDOWN)
        elif action == "failure":
            supervisor._fail(RuntimeError("worker failed"))
        elif action == "unhealthy":
            supervisor._worker_started_at["btc-5m"] -= 60
        elif action == "wrong_execution":
            reply = replace(reply, request=replace(request, execution_id="another"))
        elif action == "wrong_contract_order":
            reply = replace(reply, request=replace(request, contracts=request.contracts[::-1]))
        elif action == "wrong_responder":
            reply = replace(reply, process_generation="old")
        elif action == "wrong_partition":
            reply = replace(reply, partition="other")
        elif action == "wrong_books":
            reply = replace(reply, books=reply.books[::-1])
        elif action == "missing_books":
            reply = replace(reply, books=None)
        elif action == "before_request":
            reply = replace(reply, responded_monotonic_at_ns=request.sent_monotonic_at_ns - 1)
        elif action == "future_response":
            reply = replace(reply, responded_monotonic_at_ns=time.monotonic_ns() + RECOVERY_BOOKS_TIMEOUT_NS)
        if action != "timeout":
            supervisor._consume_recovery_books(reply)
        error_type = TimeoutError if expected == "timeout" else RuntimeError
        with pytest.raises(error_type, match=f": {expected}$"):
            await task
        assert not supervisor._pending_recovery_books
        supervisor._consume_recovery_books(reply)
        assert not supervisor._pending_recovery_books
    asyncio.run(check())


@pytest.mark.parametrize("action,expected", [
    ("restart", "generation"), ("shutdown", "shutdown"), ("failure", "unavailable"),
])
def test_recovery_success_is_rechecked_before_returning_to_caller(action, expected):
    """A resolved success becomes unusable if the worker changes before task resume."""
    async def check():
        _, journal, template, supervisor, contracts = _recovery_setup()
        task, request = await _start_request(supervisor, template, contracts)
        supervisor._consume_recovery_books(_read_worker_recovery_books(request, journal))
        if action == "restart":
            supervisor._process_generations["btc-5m"] = "new"
        elif action == "shutdown":
            supervisor._stopping = True
        else:
            supervisor._fail(RuntimeError("failed after reply"))
        with pytest.raises(RuntimeError, match=f": {expected}$"):
            await task
        assert not supervisor._pending_recovery_books
    asyncio.run(check())


def test_supervisor_rejects_unknown_contracts_and_pre_request_generation_changes():
    """The authoritative partition matches constrain every requested contract pair."""
    async def check():
        _, _, template, supervisor, contracts = _recovery_setup()
        with pytest.raises(RuntimeError, match=": identity$"):
            await supervisor.recovery_books(template.execution_id, template.validation_ref, (contracts[0], ContractID("unknown")))
        with pytest.raises(RuntimeError, match=": identity$"):
            await supervisor.recovery_books(template.execution_id, template.validation_ref, (contracts[0], contracts[0]))
        with pytest.raises(RuntimeError, match=": generation$"):
            await supervisor.recovery_books(template.execution_id, replace(template.validation_ref, process_generation="old"), contracts)
        assert supervisor._controls["btc-5m"].empty()
        assert not supervisor._pending_recovery_books
    asyncio.run(check())


def test_recovery_cancellation_late_reply_and_full_control_queue():
    """Cancelled waiters disappear; replies cannot bind to a fresh unique request."""
    async def check():
        _, journal, template, supervisor, contracts = _recovery_setup()
        task, request = await _start_request(supervisor, template, contracts)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert not supervisor._pending_recovery_books
        next_task, next_request = await _start_request(supervisor, template, contracts)
        assert next_request.request_id != request.request_id
        supervisor._consume_recovery_books(_read_worker_recovery_books(request, journal))
        assert not next_task.done()
        supervisor._consume_recovery_books(_read_worker_recovery_books(next_request, journal))
        await next_task
        control = supervisor._controls["btc-5m"]
        while not control.full():
            control.put_nowait(object())
        with pytest.raises(RuntimeError, match=": queue_full$"):
            await supervisor.recovery_books(template.execution_id, template.validation_ref, contracts)
        assert not supervisor._pending_recovery_books
    asyncio.run(check())


def test_recovery_pending_capacity_stays_bounded_when_control_queue_drains(monkeypatch):
    """A slow worker cannot accumulate unbounded waiters after accepting requests."""
    async def check():
        _, _, template, supervisor, contracts = _recovery_setup()
        monkeypatch.setattr(market_workers, "RECOVERY_BOOKS_TIMEOUT_NS", 5_000_000_000)
        tasks = []
        try:
            for _ in range(_CONTROL_QUEUE_CAPACITY):
                task, _ = await _start_request(supervisor, template, contracts)
                tasks.append(task)
            assert len(supervisor._pending_recovery_books) == _CONTROL_QUEUE_CAPACITY
            with pytest.raises(RuntimeError, match=": queue_full$"):
                await supervisor.recovery_books(template.execution_id, template.validation_ref, contracts)
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
        assert not supervisor._pending_recovery_books
    asyncio.run(check())


@pytest.mark.parametrize("full", [False, True])
def test_real_control_handler_returns_books_or_drops_full_reply_queue(full):
    """The actual worker loop serves recovery without blocking when replies saturate."""
    async def check():
        replies = queue.Queue(maxsize=1)
        if full:
            replies.put_nowait(object())
        engine, journal, template = _setup(metrics=replies)
        journal._validation_intents.clear()
        request = WorkerRecoveryBooksRequest(
            "request", template.execution_id, template.validation_ref, (template.pair.left, template.pair.right),
        )
        controls = queue.Queue(maxsize=1)
        controls.put_nowait(request)
        task = asyncio.create_task(_apply_worker_controls(controls, engine, journal))
        try:
            deadline = time.monotonic() + 1
            while not (journal._telemetry._drops.get(("recovery_reply", "full")) if full else not replies.empty()):
                assert time.monotonic() < deadline
                await asyncio.sleep(0.002)
            assert not task.done()
            if full:
                assert journal._telemetry._drops[("recovery_reply", "full")] == 1
            else:
                reply = replies.get_nowait()
                assert isinstance(reply, WorkerRecoveryBooksResult)
                assert reply.reason is Reason.ACCEPTED
                assert reply.books == (engine.state.books[template.pair.left.id], engine.state.books[template.pair.right.id])
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
    asyncio.run(check())


def _recovering_worker(_partition, generation, _config, control, events, metrics, stop):
    """Serve changed public books after removing the profitable intent from memory."""
    async def run():
        engine, journal, template = _setup(generation=generation, output=queue.Queue(), metrics=metrics)
        journal._output = events
        journal.append(MarketMatchesUpdated(template.cycle, (template.pair,)))
        journal.clear_intent_dedup()
        journal.append(_detected(template.cycle, template.pair, engine.state.books[template.pair.left.id], engine.state.books[template.pair.right.id]))
        engine.state.books[template.pair.right.id] = replace(_book(template.pair.right, "0.8"), source_hash="latest recovery")
        journal._validation_intents.clear()
        task = asyncio.create_task(_apply_worker_controls(control, engine, journal))
        try:
            await _wait_for_process_stop(stop)
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
    asyncio.run(run())


def test_spawn_recovery_roundtrip_latest_books_and_shutdown_cleanup():
    """Real spawn queues carry latest books unchanged and shutdown removes all tasks."""
    async def check():
        _, _, template, supervisor, contracts = _recovery_setup()
        supervisor._processes.clear()
        supervisor._controls.clear()
        supervisor._worker_target = _recovering_worker
        try:
            await supervisor.start()
            deadline = time.monotonic() + 15
            while not any(isinstance(event, OrderBookPairUpdated) for event in supervisor._pipeline.sink.events):
                assert time.monotonic() < deadline
                await asyncio.sleep(0.005)
            event = next(event for event in supervisor._pipeline.sink.events if isinstance(event, OrderBookPairUpdated))
            books = await supervisor.recovery_books(template.execution_id, event.detected.validation_ref, contracts[::-1])
            assert books[0].source_hash == "latest recovery"
            assert books[0].best_ask().price.value > event.right_order_book.best_ask().price.value
            assert books[1] == event.left_order_book
            assert books[0].received_at_ns == books[0].arrival_at_ns
            assert books[0].arrival_at_ns <= time.monotonic_ns()
            processes = tuple(supervisor._processes.values())
        finally:
            await supervisor.stop()
        assert all(not process.is_alive() for process in processes)
        assert not supervisor._pending_recovery_books
        assert not supervisor._tasks
    asyncio.run(check())
