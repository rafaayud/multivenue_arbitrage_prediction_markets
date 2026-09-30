"""Separate bounded local requotes from confirmed venue recovery attempts."""

from dataclasses import replace
from decimal import Decimal
import json
import time

import pytest

from prediction_markets.application.codec import decode_event, encode_event
from prediction_markets.application.engine import EngineConfig, TradingEngine
from prediction_markets.application.events import (
    ExecutionUpdated, RecoveryBooksReceived, RecoveryPlanningRequested,
    RecoveryUpdated, SubmissionReceived, SubmitOrder, TradeRecorded,
)
from prediction_markets.application.execution.accounting import ExecutionAccounting
from prediction_markets.application.execution.recovery_lifecycle import RecoveryLifecycle
from prediction_markets.application.state import EventDispatcher, TradingState
from prediction_markets.domain.orderbook.value_objects import OrderBookLevel
from prediction_markets.domain.shared.value_objects import (
    ClientOrderID, Currency, Money, OrderID, Price, Quantity, Timestamp, VenueID,
)
from prediction_markets.domain.trading.entities import ArbitrageExecutionJournal, OrderSnapshot
from prediction_markets.domain.trading.enums import (
    ArbitrageExecutionStatus, OrderSide, OrderStatus, OrderType, RecoveryStatus,
    SubmissionStatus,
)
from prediction_markets.domain.trading.value_objects import (
    OrderReference, SubmissionResult, TradingFee,
)
from prediction_markets.infrastructure.recovery_snapshots import (
    RecoverySnapshot, RecoverySnapshotStore,
)
from tests.application.test_engine import _book, _contract, _ZeroFees


@pytest.fixture
def recovery_case():
    """Create one known eight-contract fill with a liquid missing hedge leg."""
    left = _contract("left-yes", VenueID("left"), "yes")
    right = _contract("right-no", VenueID("right"), "no")
    now = Timestamp.now()
    execution = ArbitrageExecutionJournal(
        id="retry-budget",
        status=ArbitrageExecutionStatus.RECOVERY_PENDING,
        leg1_venue_id=left.venue_id,
        leg1_contract_id=left.id,
        leg1_side=OrderSide.BUY,
        leg1_quantity=Quantity(Decimal("8")),
        leg1_limit_price=Price(Decimal("0.52")),
        leg1_client_order_id=ClientOrderID("source"),
        leg2_venue_id=right.venue_id,
        leg2_contract_id=right.id,
        leg2_side=OrderSide.BUY,
        leg2_quantity=Quantity(Decimal("8")),
        leg2_limit_price=Price(Decimal("0.42")),
        leg2_client_order_id=ClientOrderID("missing"),
        leg1_filled_quantity=Quantity(Decimal("8")),
        residual_quantity=Quantity(Decimal("8")),
        created_at=now,
        updated_at=now,
    )
    source = OrderSnapshot(
        status=OrderStatus.FILLED,
        contract_id=left.id,
        side=OrderSide.BUY,
        quantity=execution.leg1_quantity,
        order_type=OrderType.LIMIT,
        client_order_id=execution.leg1_client_order_id,
        filled_quantity=execution.leg1_filled_quantity,
        average_price=execution.leg1_limit_price,
    )
    state = TradingState(
        contracts={left.id: left, right.id: right},
        books={left.id: _book(left, "0.39", "8", bid=True), right.id: _book(right, "0.42", "8")},
        executions={execution.id: execution},
        orders={execution.leg1_client_order_id: source},
    )
    lifecycle = RecoveryLifecycle(
        state,
        {left.venue_id: _ZeroFees(), right.venue_id: _ZeroFees()},
        ExecutionAccounting(state),
    )
    return state, lifecycle


def _plan(state, lifecycle) -> SubmitOrder | None:
    outputs = lifecycle.plan(
        state.executions["retry-budget"],
        max_loss=Decimal("1"),
        min_buy_notional=Decimal("0"),
    )
    for event in outputs:
        state.apply(event)
    return next((event for event in outputs if isinstance(event, SubmitOrder)), None)


def _rejected(command, *, local=False, snapshot=False):
    return SubmissionReceived(
        command,
        SubmissionResult(
            SubmissionStatus.ACCEPTED if snapshot else SubmissionStatus.REJECTED,
            OrderReference(
                command.venue_id,
                command.intent.client_order_id,
                b"local-pre-submission-guard" if local else b"venue-no-match",
            ),
            snapshot=OrderSnapshot(
                status=OrderStatus.CANCELLED,
                contract_id=command.intent.contract_id,
                side=command.intent.side,
                quantity=command.intent.quantity,
                order_type=command.intent.order_type,
                client_order_id=command.intent.client_order_id,
                filled_quantity=Quantity(Decimal("0")),
                may_receive_more_fills=False,
            ) if snapshot else None,
            reason="price changed" if local else "no match",
        ),
    )


def _respond(state, lifecycle, event):
    previous = state.orders.get(event.command.intent.client_order_id)
    state.apply(event)
    outputs = lifecycle.handle_order_event(event, previous)
    for output in outputs:
        state.apply(output)
    return outputs


def test_two_local_rejections_do_not_exhaust_three_venue_attempts(recovery_case):
    """The incident's two unsent plans must leave room after one zero fill."""
    state, lifecycle = recovery_case
    identifiers = []
    for local in (True, True, False):
        command = _plan(state, lifecycle)
        identifiers.append(command.intent.client_order_id)
        _respond(state, lifecycle, _rejected(command, local=local, snapshot=not local))
    command = _plan(state, lifecycle)
    identifiers.append(command.intent.client_order_id)
    assert [str(identifier) for identifier in identifiers] == [
        f"retry-budget-recovery-{index}" for index in range(1, 5)
    ]
    recovery = state.recoveries[command.execution_id]
    assert recovery.attempts == 4
    assert recovery.local_rejections == 2
    assert recovery.status is RecoveryStatus.PENDING


@pytest.mark.parametrize("snapshot", [False, True])
@pytest.mark.parametrize("local_prefix", [0, 2])
def test_three_real_attempts_still_stop_recovery(recovery_case, snapshot, local_prefix):
    """Only three definite venue attempts are allowed, with or without snapshots."""
    state, lifecycle = recovery_case
    for _ in range(local_prefix):
        command = _plan(state, lifecycle)
        _respond(state, lifecycle, _rejected(command, local=True))
    for _ in range(3):
        command = _plan(state, lifecycle)
        assert command is not None
        _respond(state, lifecycle, _rejected(command, snapshot=snapshot))
    assert _plan(state, lifecycle) is None
    recovery = state.recoveries["retry-budget"]
    assert recovery.attempts == 3 + local_prefix
    assert recovery.local_rejections == local_prefix
    assert recovery.status is RecoveryStatus.NEEDS_REVIEW


def test_local_requotes_have_a_separate_finite_budget(recovery_case):
    """Continually moving books cannot produce an unbounded local retry loop."""
    state, lifecycle = recovery_case
    for _ in range(3):
        command = _plan(state, lifecycle)
        _respond(state, lifecycle, _rejected(command, local=True))
    assert _plan(state, lifecycle) is None
    recovery = state.recoveries["retry-budget"]
    assert recovery.attempts == recovery.local_rejections == 3
    assert recovery.status is RecoveryStatus.NEEDS_REVIEW
    assert "local recovery requote budget exhausted" in recovery.last_error


def test_recovery_lifecycle_passes_freshness_into_route_selection(recovery_case):
    """Choose an executable unwind before a better-priced stale missing leg."""
    state, lifecycle = recovery_case
    execution = state.executions["retry-budget"]
    outputs = lifecycle.plan(
        execution,
        max_loss=Decimal("2"),
        min_buy_notional=Decimal("0"),
        fresh_contract_ids=frozenset((execution.leg1_contract_id,)),
    )
    command = next(event for event in outputs if isinstance(event, SubmitOrder))
    assert command.intent.contract_id == execution.leg1_contract_id
    assert command.intent.side is OrderSide.SELL
    assert command.intent.limit_price.value == Decimal("0.39")


def test_recovery_overfill_records_actual_trade_but_pairs_economics(recovery_case):
    """Keep an authoritative overfill while matching economics to the request."""
    state, lifecycle = recovery_case
    command = _plan(state, lifecycle)
    recovery = state.recoveries[command.execution_id]
    source_fee = Money(Decimal("0.80"), Currency("USD"))
    state.orders[state.executions[command.execution_id].leg1_client_order_id] = replace(
        state.orders[state.executions[command.execution_id].leg1_client_order_id],
        fee=TradingFee(source_fee, source_fee),
    )
    state.recoveries[command.execution_id] = replace(
        recovery,
        source_fee=source_fee,
    )
    actual = Quantity(Decimal("8.2"))
    recovery_fee = Money(Decimal("0.82"), Currency("USD"))
    snapshot = OrderSnapshot(
        status=OrderStatus.FILLED,
        contract_id=command.intent.contract_id,
        side=command.intent.side,
        quantity=command.intent.quantity,
        order_type=command.intent.order_type,
        client_order_id=command.intent.client_order_id,
        order_id=OrderID("recovery-overfill"),
        filled_quantity=actual,
        average_price=command.intent.limit_price,
        fee=None,
    )
    event = SubmissionReceived(
        command,
        SubmissionResult(
            SubmissionStatus.ACCEPTED,
            OrderReference(command.venue_id, command.intent.client_order_id, b"overfill"),
            snapshot=snapshot,
        ),
    )
    outputs = _respond(state, lifecycle, event)

    updated = state.recoveries[command.execution_id]
    assert updated.filled_quantity == actual
    assert updated.actual_gross_result is not None
    assert updated.actual_net_result is None
    assert updated.status is RecoveryStatus.RESOLVED
    assert state.executions[command.execution_id].residual_quantity.value == 0
    assert len([item for item in outputs if isinstance(item, TradeRecorded)]) == 1
    assert next(item for item in state.trades.values() if item.order_id == snapshot.order_id).quantity == actual

    late_snapshot = replace(snapshot, fee=TradingFee(recovery_fee, recovery_fee))
    state.orders[command.intent.client_order_id] = late_snapshot
    late_outputs = lifecycle.recovery_outputs()
    assert not any(isinstance(item, TradeRecorded) for item in late_outputs)
    assert any(isinstance(item, RecoveryUpdated) for item in late_outputs)
    for item in late_outputs:
        state.apply(item)
    assert state.recoveries[command.execution_id].actual_net_result == Decimal("-1.12")
    assert not lifecycle.recovery_outputs()


def test_duplicate_local_rejection_cannot_consume_budget_twice(recovery_case):
    """Deduplicate both before fresh planning and after the command is superseded."""
    state, lifecycle = recovery_case
    command = _plan(state, lifecycle)
    event = _rejected(command, local=True)
    _respond(state, lifecycle, event)
    assert not _respond(state, lifecycle, event)
    assert state.recoveries[command.execution_id].local_rejections == 1
    replacement = _plan(state, lifecycle)
    assert replacement.intent.client_order_id != command.intent.client_order_id
    assert not _respond(state, lifecycle, event)
    assert state.recoveries[command.execution_id].local_rejections == 1


def test_uncertain_cancel_cannot_grant_an_additional_attempt(recovery_case):
    """A terminal-looking snapshot cannot unlock a replacement before finality."""
    state, lifecycle = recovery_case
    command = _plan(state, lifecycle)
    event = _rejected(command, snapshot=True)
    event = replace(event, result=replace(
        event.result,
        snapshot=replace(event.result.snapshot, may_receive_more_fills=True),
    ))
    _respond(state, lifecycle, event)
    assert _plan(state, lifecycle) is None
    assert state.recoveries[command.execution_id].local_rejections == 0


def test_local_sentinel_cannot_override_a_previous_acceptance(recovery_case):
    """Contradictory local evidence fails closed instead of releasing a live order."""
    state, lifecycle = recovery_case
    command = _plan(state, lifecycle)
    event = _rejected(command)
    _respond(state, lifecycle, replace(
        event,
        result=replace(event.result, status=SubmissionStatus.ACCEPTED),
    ))
    _respond(state, lifecycle, _rejected(command, local=True))
    assert _plan(state, lifecycle) is None
    recovery = state.recoveries[command.execution_id]
    assert recovery.status is RecoveryStatus.NEEDS_REVIEW
    assert recovery.local_rejections == 0


def test_requote_counter_survives_journal_and_snapshot_replay(recovery_case, tmp_path):
    """Persist the operational budget in the authoritative recovery journal."""
    state, lifecycle = recovery_case
    command = _plan(state, lifecycle)
    _respond(state, lifecycle, _rejected(command, local=True))
    event = RecoveryUpdated(state.recoveries[command.execution_id])
    assert decode_event(encode_event(event)) == event
    snapshot = RecoverySnapshot(
        journal_sequence=10,
        created_at=Timestamp.now().value,
        application_version="test",
        events=(event,),
        unresolved=(),
    )
    store = RecoverySnapshotStore(tmp_path)
    store.save(snapshot)
    loaded = store.load_latest(through_sequence=10)
    replayed = TradingState()
    for record in loaded.records():
        replayed.apply(record.event)
    assert replayed.recoveries[command.execution_id].local_rejections == 1


def test_old_journals_default_to_conservative_venue_attempt_count(recovery_case):
    """Old recovery records have no proof that an attempt was rejected locally."""
    state, lifecycle = recovery_case
    command = _plan(state, lifecycle)
    event = RecoveryUpdated(state.recoveries[command.execution_id])
    encoded = json.loads(encode_event(event))
    del encoded["event"]["fields"]["recovery"]["fields"]["local_rejections"]
    decoded = decode_event(json.dumps(encoded).encode())
    assert decoded.recovery.local_rejections == 0
    assert decoded.recovery.attempts == 1


@pytest.mark.parametrize("local_rejections", [-1, 2])
def test_local_rejection_count_cannot_exceed_planned_orders(recovery_case, local_rejections):
    """Malformed recovery state cannot grant additional venue attempts."""
    state, lifecycle = recovery_case
    command = _plan(state, lifecycle)
    with pytest.raises(ValueError, match="local rejections"):
        replace(state.recoveries[command.execution_id], local_rejections=local_rejections)


def test_final_recovery_vwap_can_breach_loss_inside_the_signed_limit(recovery_case):
    """The same 0.49 limit does not make a 0.40 to 0.49 VWAP move harmless."""
    state, lifecycle = recovery_case
    execution = state.executions["retry-budget"]
    quantity = Quantity(Decimal("10"))
    state.executions[execution.id] = replace(
        execution,
        leg1_quantity=quantity,
        leg2_quantity=quantity,
        leg1_filled_quantity=quantity,
        residual_quantity=quantity,
    )
    state.orders[execution.leg1_client_order_id] = replace(
        state.orders[execution.leg1_client_order_id],
        quantity=quantity,
        filled_quantity=quantity,
        average_price=Price(Decimal("0.60")),
    )
    book = state.books[execution.leg2_contract_id]
    book = replace(book, asks=(
        OrderBookLevel(Price(Decimal("0.39")), Quantity(Decimal("9"))),
        OrderBookLevel(Price(Decimal("0.49")), Quantity(Decimal("1"))),
    ))
    state.books[execution.leg2_contract_id] = book
    command = _plan(state, lifecycle)
    assert command.intent.limit_price == Price(Decimal("0.49"))
    assert state.recoveries[execution.id].estimated_vwap == Price(Decimal("0.40"))
    assert lifecycle.guard_command(
        command, book, max_loss=Decimal("0.50"), min_buy_notional=Decimal("0"),
    ) is None
    latest = replace(book, asks=(OrderBookLevel(Price(Decimal("0.49")), quantity),))
    assert lifecycle.guard_command(
        command, latest, max_loss=Decimal("0.50"), min_buy_notional=Decimal("0"),
    ) == "recovery loss limit exceeded"


def test_final_recovery_guard_allocates_fees_to_a_partial_depth_plan(recovery_case):
    """A four-contract recovery carries half of an eight-contract source fee."""
    state, lifecycle = recovery_case
    execution = state.executions["retry-budget"]
    fee = Money(Decimal("0.80"), Currency("USD"))
    state.orders[execution.leg1_client_order_id] = replace(
        state.orders[execution.leg1_client_order_id], fee=TradingFee(fee, fee),
    )
    left = state.contracts[execution.leg1_contract_id]
    right = state.contracts[execution.leg2_contract_id]
    state.books[left.id] = _book(left, "0.10", "8", bid=True)
    state.books[right.id] = _book(right, "0.42", "4")
    command = _plan(state, lifecycle)
    assert command.intent.quantity.value == Decimal("4")
    assert lifecycle.guard_command(
        command, state.books[right.id], max_loss=Decimal("0.50"), min_buy_notional=Decimal("0"),
    ) is None


@pytest.mark.parametrize("latest_price", ["0.10", "0.20"])
def test_final_recovery_minimum_uses_unchanged_signed_notional(recovery_case, latest_price):
    """Price improvement below one dollar cannot invalidate the signed one-dollar BUY."""
    state, lifecycle = recovery_case
    execution = state.executions["retry-budget"]
    quantity = Quantity(Decimal("5"))
    execution = replace(
        execution, leg1_quantity=quantity, leg2_quantity=quantity,
        leg1_filled_quantity=quantity, residual_quantity=quantity,
    )
    state.executions[execution.id] = execution
    book = replace(state.books[execution.leg2_contract_id], asks=(
        OrderBookLevel(Price(Decimal("0.10")), Quantity(Decimal("1.42"))),
        OrderBookLevel(Price(Decimal("0.20")), Quantity(Decimal("45.43"))),
    ))
    state.books[execution.leg2_contract_id] = book
    outputs = lifecycle.plan(execution, max_loss=Decimal("1"), min_buy_notional=Decimal("1"))
    for event in outputs:
        state.apply(event)
    command = next(event for event in outputs if isinstance(event, SubmitOrder))
    assert command.intent.side is OrderSide.BUY
    assert command.intent.limit_price.value * command.intent.quantity.value == Decimal("1")
    latest = replace(book, asks=(OrderBookLevel(Price(Decimal(latest_price)), quantity),))
    assert lifecycle.guard_command(
        command, latest, max_loss=Decimal("1"), min_buy_notional=Decimal("1"),
    ) is None
    assert lifecycle.guard_command(
        command, latest, max_loss=Decimal("1"), min_buy_notional=Decimal("1.01"),
    ) == "recovery minimum buy notional unavailable"


@pytest.mark.parametrize("price, quantity, reason", [
    ("0.41", "7", "recovery liquidity unavailable"),
    ("0.43", "8", "recovery limit price changed"),
])
def test_final_recovery_guard_requires_the_signed_quantity_and_limit(
    recovery_case, price, quantity, reason,
):
    """Replanning must happen before modifying a signed order's price or size."""
    state, lifecycle = recovery_case
    command = _plan(state, lifecycle)
    contract = state.contracts[command.intent.contract_id]
    assert lifecycle.guard_command(
        command, _book(contract, price, quantity),
        max_loss=Decimal("1"), min_buy_notional=Decimal("0"),
    ) == reason


def _waiting_engine(state):
    """Configure a parent engine before restoring the fixture's residual execution."""
    execution = state.executions.pop("retry-budget")
    fees = {contract.venue_id: _ZeroFees() for contract in state.contracts.values()}
    engine = TradingEngine(EventDispatcher(state), fees)
    engine.enable(EngineConfig(
        max_notional_by_venue={venue: Decimal("20") for venue in fees},
        max_recovery_loss=Decimal("1"),
    ))
    state.executions[execution.id] = execution
    return engine, engine.process(ExecutionUpdated(execution))[0]


def test_repeated_stale_quotes_do_not_create_or_consume_recovery_orders(recovery_case, monkeypatch):
    """A hundred identical unusable replies remain one bounded data wait."""
    state, _ = recovery_case
    now_ns = [time.monotonic_ns()]
    monkeypatch.setattr("prediction_markets.application.engine.time.monotonic_ns", lambda: now_ns[0])
    engine, request = _waiting_engine(state)
    deadline_at_ns = request.deadline_at_ns
    assert deadline_at_ns == now_ns[0] + 10_000_000_000
    books = tuple(state.books[contract_id] for contract_id in (
        request.execution.leg1_contract_id, request.execution.leg2_contract_id,
    ))
    for _ in range(100):
        previous_request = request
        reply = RecoveryBooksReceived(request, books, fresh_contract_ids=frozenset())
        outputs = engine.process(reply)
        assert len(outputs) == 1
        request = outputs[0]
        assert isinstance(request, RecoveryPlanningRequested)
        assert request.request_id != previous_request.request_id
        assert request.deadline_at_ns == deadline_at_ns
        assert request.not_before_ns == now_ns[0] + 20_000_000
        assert not state.recoveries and not state.commands
        assert engine.process(reply) == ()
        assert engine._pending_recovery_books[request.execution.id] == request
        now_ns[0] = request.not_before_ns

    missing = state.contracts[request.execution.leg2_contract_id]
    latest = _book(missing, "0.49", "8")
    outputs = engine.process(RecoveryBooksReceived(
        request, (books[0], latest), fresh_contract_ids=frozenset((missing.id,)),
    ))
    for output in outputs:
        state.apply(output)
    command = next(output for output in outputs if isinstance(output, SubmitOrder))
    assert command.intent.limit_price.value == Decimal("0.49")
    assert str(command.intent.client_order_id) == "retry-budget-recovery-1"
    assert state.recoveries[command.execution_id].attempts == 1
    assert state.recoveries[command.execution_id].local_rejections == 0
    assert not engine._pending_recovery_books


@pytest.mark.parametrize("quote_unavailable", ["stale", "loss"])
def test_recovery_quote_wait_expires_once_without_unsent_attempts(
    recovery_case, monkeypatch, quote_unavailable,
):
    """Freshness or economic ineligibility exhausts one absolute data deadline."""
    state, _ = recovery_case
    if quote_unavailable == "loss":
        for contract_id, book in tuple(state.books.items()):
            state.books[contract_id] = replace(book,
                bids=(OrderBookLevel(Price(Decimal("0.01")), Quantity(Decimal("8"))),),
                asks=(OrderBookLevel(Price(Decimal("0.99")), Quantity(Decimal("8"))),),
            )
    now_ns = [time.monotonic_ns()]
    monkeypatch.setattr("prediction_markets.application.engine.time.monotonic_ns", lambda: now_ns[0])
    engine, request = _waiting_engine(state)
    books = tuple(state.books.values())
    fresh = frozenset(state.contracts) if quote_unavailable == "loss" else frozenset()
    retry, = engine.process(RecoveryBooksReceived(request, books, fresh_contract_ids=fresh))
    assert isinstance(retry, RecoveryPlanningRequested)
    assert retry.deadline_at_ns == request.deadline_at_ns
    now_ns[0] = request.deadline_at_ns
    reply = RecoveryBooksReceived(retry, books, fresh_contract_ids=fresh)
    outputs = engine.process(reply)
    for output in outputs:
        engine.process(output)
    execution = state.executions[request.execution.id]
    assert execution.status is ArbitrageExecutionStatus.NEEDS_REVIEW
    assert execution.last_error == (
        "recovery quote wait expired: no fresh route satisfied freshness, liquidity and loss limits"
    )
    assert not state.recoveries and not state.commands
    assert engine.process(reply) == ()
    assert not engine._pending_recovery_books


def test_uncertain_recovery_does_not_become_a_quote_wait(recovery_case):
    """Only a missing quote returns the deferral sentinel; unknown fills stay blocked."""
    state, lifecycle = recovery_case
    command = _plan(state, lifecycle)
    event = _rejected(command, snapshot=True)
    event = replace(event, result=replace(
        event.result, snapshot=replace(event.result.snapshot, may_receive_more_fills=True),
    ))
    _respond(state, lifecycle, event)
    assert lifecycle.plan(
        state.executions[command.execution_id], max_loss=Decimal("1"),
        min_buy_notional=Decimal("1"), fresh_contract_ids=frozenset(), wait_for_quote=True,
    ) == ()
