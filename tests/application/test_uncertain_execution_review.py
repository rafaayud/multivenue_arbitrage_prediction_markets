"""Verify durable review and continued accounting for uncertain order outcomes."""

from dataclasses import replace
from decimal import Decimal
import json

import pytest

from prediction_markets.application.codec import decode_event, encode_event
from prediction_markets.application.engine import EngineConfig, TradingEngine
from prediction_markets.application.events import (
    ApplicationEvent,
    ExecutionUpdated,
    MarketMatchesUpdated,
    OrderBookUpdated,
    OrderSnapshotUpdated,
    PositionUpdated,
    RecoveryUpdated,
    SubmitOrder,
    TradeRecorded,
    TradingSafetyStop,
)
from prediction_markets.application.markets.models import MarketCycle
from prediction_markets.application.state import EventDispatcher, TradingState
from prediction_markets.domain.market_matching.value_objects import (
    MatchedContractPair,
    Underlying,
)
from prediction_markets.domain.shared.value_objects import (
    ClientOrderID,
    Price,
    Quantity,
    Timestamp,
    VenueID,
)
from prediction_markets.domain.trading.enums import (
    ArbitrageExecutionStatus,
    OrderStatus,
    RecoveryStatus,
)
from prediction_markets.infrastructure.recovery_snapshots import _RecoveryAccumulator
from tests.application.test_engine import _ZeroFees, _book, _contract, _fill, _process


def _observation(
    command: SubmitOrder,
    quantity: str,
    status: OrderStatus,
    may_receive_more_fills: bool,
) -> OrderSnapshotUpdated:
    """Build a private order observation with an explicit settlement certainty."""
    fill = _fill(command, command.intent.limit_price, Quantity(Decimal(quantity)))
    return OrderSnapshotUpdated(
        command.execution_id,
        command.role,
        fill.result.reference,
        replace(
            fill.result.snapshot,
            status=status,
            average_price=command.intent.limit_price if Decimal(quantity) > 0 else None,
            may_receive_more_fills=may_receive_more_fills,
        ),
        "get",
    )


def _uncertain_short() -> tuple[TradingEngine, SubmitOrder, SubmitOrder, list[ApplicationEvent]]:
    """Accept both short legs, fill Polymarket, and observe an uncertain Predict cancel."""
    predict = _contract("predict-up", VenueID("PREDICT"), "UP")
    polymarket = _contract("polymarket-down", VenueID("POLYMARKET"), "DOWN")
    pair = MatchedContractPair(predict, polymarket, Timestamp.now())
    cycle = MarketCycle(Underlying("ETH"), 3600)
    engine = TradingEngine(
        EventDispatcher(TradingState()),
        {predict.venue_id: _ZeroFees(), polymarket.venue_id: _ZeroFees()},
    )
    engine.enable(EngineConfig(
        max_notional_by_venue={predict.venue_id: Decimal("10"), polymarket.venue_id: Decimal("10")},
        max_recovery_loss=Decimal("1"),
        execute_long=False,
        execute_short=True,
        predict_limit_slippage_ticks=1,
        short_market_keys=frozenset({"cycle:ETH:3600"}),
        allowed_underlyings=("ETH",),
        allowed_intervals_seconds=(3600,),
        short_pair_keys=frozenset({pair.key}),
        short_inventory_by_contract={predict.id: Quantity(Decimal("5")), polymarket.id: Quantity(Decimal("5"))},
    ))
    history = _process(engine, MarketMatchesUpdated(cycle, (pair,)))
    history.extend(_process(engine, OrderBookUpdated(
        predict.venue_id, predict.id, _book(predict, "0.79", "10", bid=True),
    )))
    history.extend(_process(engine, OrderBookUpdated(
        polymarket.venue_id, polymarket.id, _book(polymarket, "0.25", "5", bid=True),
    )))
    primary, hedge = (
        next(event for event in history if isinstance(event, SubmitOrder) and event.role == role)
        for role in ("primary", "hedge")
    )
    assert primary.venue_id == polymarket.venue_id
    assert hedge.venue_id == predict.venue_id
    assert hedge.intent.limit_price == Price(Decimal("0.78"))
    for command in (primary, hedge):
        accepted = _fill(command, command.intent.limit_price, Quantity(Decimal("0")))
        history.extend(_process(engine, replace(
            accepted,
            result=replace(accepted.result, snapshot=replace(
                accepted.result.snapshot,
                status=OrderStatus.ACCEPTED,
                average_price=None,
                may_receive_more_fills=True,
            )),
        )))
    history.extend(_process(engine, _observation(primary, "5", OrderStatus.FILLED, False)))
    history.extend(_process(engine, _observation(hedge, "0", OrderStatus.CANCELLED, True)))
    assert engine.state.executions[primary.execution_id].status is ArbitrageExecutionStatus.HEDGE_PENDING
    return engine, primary, hedge, history


def _stop(command: SubmitOrder) -> TradingSafetyStop:
    """Identify exactly the accepted order whose outcome remains uncertain."""
    return TradingSafetyStop(
        command.venue_id,
        "Order remains unsettled after cancellation; manual review required",
        Timestamp.now(),
        execution_id=command.execution_id,
        client_order_id=command.intent.client_order_id,
    )


def test_scoped_stop_completes_after_matching_direct_fills_settle() -> None:
    """Complete a reviewed pair when late final evidence proves matching fills."""
    engine, primary, hedge, _ = _uncertain_short()
    state = engine.state
    unaffected = replace(state.executions[primary.execution_id], id="unrelated")
    state.executions[unaffected.id] = unaffected
    positions_before = dict(state.positions)
    trades_before = dict(state.trades)
    halted = _process(engine, _stop(hedge))
    execution = state.executions[primary.execution_id]
    assert any(isinstance(event, ExecutionUpdated) for event in halted)
    assert execution.status is ArbitrageExecutionStatus.NEEDS_REVIEW
    assert execution.leg1_filled_quantity.value == 5
    assert execution.leg2_filled_quantity.value == 0
    assert execution.residual_quantity.value == 5
    assert execution.leg2_order_id == state.orders[hedge.intent.client_order_id].order_id
    assert state.executions[unaffected.id] == unaffected
    assert state.positions == positions_before
    assert state.trades == trades_before
    assert state.safety_halted and not state.trading_enabled

    for quantity, status, uncertain in (
        ("2", OrderStatus.CANCELLED, True),
        ("5", OrderStatus.FILLED, False),
    ):
        event = _observation(hedge, quantity, status, uncertain)
        observed = _process(engine, event)
        duplicate = _process(engine, event)
        assert not any(isinstance(value, SubmitOrder) for value in observed + duplicate)
        assert not any(isinstance(value, TradeRecorded) for value in duplicate)
        execution = state.executions[primary.execution_id]
        assert execution.status is (
            ArbitrageExecutionStatus.NEEDS_REVIEW
            if uncertain
            else ArbitrageExecutionStatus.COMPLETED
        )
        assert execution.leg2_filled_quantity.value == Decimal(quantity)
        assert execution.residual_quantity.value == 5 - Decimal(quantity)
    assert sorted(
        trade.quantity.value for trade in state.trades.values()
        if trade.client_order_id == hedge.intent.client_order_id
    ) == [Decimal("2"), Decimal("3")]
    assert sorted(position.signed_quantity for position in state.positions.values()) == [
        Decimal("-5"), Decimal("-5"),
    ]
    assert not state.recoveries
    assert not state.trading_enabled
    assert state.safety_halted


@pytest.mark.parametrize("invalid_scope", ["legacy", "execution", "client", "venue"])
def test_unscoped_or_mismatched_stops_do_not_mark_executions(invalid_scope: str) -> None:
    """Preserve global halting without guessing which execution needs review."""
    engine, primary, hedge, _ = _uncertain_short()
    original = engine.state.executions[primary.execution_id]
    changes = {
        "legacy": {"execution_id": None, "client_order_id": None},
        "execution": {"execution_id": "another-execution"},
        "client": {"client_order_id": ClientOrderID("another-order")},
        "venue": {"venue_id": primary.venue_id},
    }[invalid_scope]
    observed = _process(engine, replace(_stop(hedge), **changes))
    assert not any(isinstance(event, ExecutionUpdated) for event in observed)
    assert engine.state.executions[primary.execution_id] == original
    assert not engine.state.execution_safety_stops
    assert engine.state.safety_halted


@pytest.mark.parametrize("compact", [False, True])
@pytest.mark.parametrize("interrupted_late_fill", [False, True])
def test_replay_restores_scoped_review_and_missing_fill_accounting(
    compact: bool,
    interrupted_late_fill: bool,
) -> None:
    """Recover a crash before review or late-fill accounting, including compact snapshots."""
    engine, primary, hedge, history = _uncertain_short()
    stop = _stop(hedge)
    if interrupted_late_fill:
        history.extend(_process(engine, stop))
        history.extend(_process(engine, _observation(hedge, "2", OrderStatus.CANCELLED, True)))
        history.append(_observation(hedge, "5", OrderStatus.FILLED, False))
    else:
        history.append(stop)
    if compact:
        accumulator = _RecoveryAccumulator(None)
        for sequence, event in enumerate(history, start=1):
            accumulator.apply(event, sequence=sequence, assign_trade_sequence=False)
        history = list(accumulator.compact_events())
        assert stop in history
    restored = TradingEngine(EventDispatcher(TradingState()), {})
    for event in history:
        assert not restored.process(decode_event(encode_event(event)), replay=True)
    assert restored.state.execution_safety_stops[primary.execution_id] == stop
    outputs = restored.recovery_outputs()
    assert not any(isinstance(event, SubmitOrder) for event in outputs)
    for event in outputs:
        _process(restored, event)
    execution = restored.state.executions[primary.execution_id]
    assert execution.status is (
        ArbitrageExecutionStatus.COMPLETED
        if interrupted_late_fill
        else ArbitrageExecutionStatus.NEEDS_REVIEW
    )
    assert execution.residual_quantity.value == (0 if interrupted_late_fill else 5)
    assert execution.leg2_order_id is not None
    assert not restored.recovery_outputs()
    assert sum(
        trade.quantity.value for trade in restored.state.trades.values()
        if trade.client_order_id == hedge.intent.client_order_id
    ) == (5 if interrupted_late_fill else 0)


def test_legacy_safety_stop_codec_defaults_remain_readable() -> None:
    """Read old journal payloads that do not contain execution or order scope."""
    event = TradingSafetyStop(VenueID("PREDICT"), "venue halted", Timestamp.now())
    payload = json.loads(encode_event(event))
    del payload["event"]["fields"]["execution_id"]
    del payload["event"]["fields"]["client_order_id"]
    assert decode_event(json.dumps(payload).encode()) == event


@pytest.mark.parametrize("manual", [False, True])
def test_delayed_stop_and_fills_do_not_reopen_completed_execution(manual: bool) -> None:
    """Preserve a prior final or operator resolution while still accounting venue fills."""
    engine, primary, hedge, _ = _uncertain_short()
    _process(engine, _stop(hedge))
    completed = replace(
        engine.state.executions[primary.execution_id],
        status=ArbitrageExecutionStatus.RECOVERED if manual else ArbitrageExecutionStatus.COMPLETED,
        resolution_method="manual_sale" if manual else None,
        residual_quantity=Quantity(Decimal("0")),
        last_error=None,
    )
    _process(engine, ExecutionUpdated(completed))
    observed = _process(engine, _stop(hedge))
    observed.extend(_process(engine, _observation(hedge, "5", OrderStatus.FILLED, False)))
    assert engine.state.executions[primary.execution_id] == completed
    assert not any(isinstance(event, (ExecutionUpdated, SubmitOrder)) for event in observed)
    assert any(isinstance(event, TradeRecorded) for event in observed)
    assert not engine.recovery_outputs()


@pytest.mark.parametrize("already_filled", ["0", "1"])
def test_uncertain_recovery_keeps_review_after_final_fill(already_filled: str) -> None:
    """Preserve recovery identity and economics while forbidding requotes or auto-resolution."""
    engine, primary, hedge, _ = _uncertain_short()
    finalized = _process(engine, _observation(hedge, "0", OrderStatus.CANCELLED, False))
    command = next(event for event in finalized if isinstance(event, SubmitOrder) and event.role == "recovery")
    _process(engine, _observation(command, already_filled, OrderStatus.CANCELLED, True))
    recovery_before = engine.state.recoveries[primary.execution_id]
    halted = _process(engine, _stop(command))
    recovery = engine.state.recoveries[primary.execution_id]
    assert recovery == replace(
        recovery_before,
        status=RecoveryStatus.NEEDS_REVIEW,
        filled_quantity=Quantity(Decimal(already_filled)),
        last_error=_stop(command).reason,
        updated_at=recovery.updated_at,
    )
    assert engine.state.executions[primary.execution_id].residual_quantity.value == 5 - Decimal(already_filled)
    assert not any(isinstance(event, SubmitOrder) for event in halted)
    for quantity, uncertain in (("2", True), ("5", False)):
        event = _observation(command, quantity, OrderStatus.CANCELLED if uncertain else OrderStatus.FILLED, uncertain)
        observed = _process(engine, event)
        duplicate = _process(engine, event)
        recovery = engine.state.recoveries[primary.execution_id]
        assert recovery.status is RecoveryStatus.NEEDS_REVIEW
        assert recovery.filled_quantity.value == Decimal(quantity)
        assert recovery.order_id == event.snapshot.order_id
        assert recovery.actual_gross_result is not None
        assert engine.state.executions[primary.execution_id].status is ArbitrageExecutionStatus.NEEDS_REVIEW
        assert engine.state.executions[primary.execution_id].residual_quantity.value == 5 - Decimal(quantity)
        assert not any(isinstance(value, SubmitOrder) for value in observed + duplicate)
        assert not any(isinstance(value, (TradeRecorded, RecoveryUpdated)) for value in duplicate)
    assert not engine.recovery_outputs()
    reviewed = engine.state.executions[primary.execution_id]
    stale_pending = _process(engine, ExecutionUpdated(replace(
        reviewed,
        status=ArbitrageExecutionStatus.RECOVERY_PENDING,
    )))
    assert not any(isinstance(event, SubmitOrder) for event in stale_pending)
    assert engine.state.executions[primary.execution_id].status is ArbitrageExecutionStatus.NEEDS_REVIEW


@pytest.mark.parametrize("compact", [False, True])
@pytest.mark.parametrize("accounting_recorded", [False, True])
def test_replay_accounts_prior_recovery_fill_before_allowing_current_attempt(
    compact: bool,
    accounting_recorded: bool,
) -> None:
    """Retain a late prior-attempt fill when a crash precedes its scoped stop."""
    engine, primary, hedge, history = _uncertain_short()
    finalized = _process(engine, _observation(hedge, "0", OrderStatus.CANCELLED, False))
    history.extend(finalized)
    first = next(event for event in finalized if isinstance(event, SubmitOrder) and event.role == "recovery")
    retried = _process(engine, _observation(first, "0", OrderStatus.CANCELLED, False))
    history.extend(retried)
    current = next(event for event in retried if isinstance(event, SubmitOrder))
    assert current.intent.client_order_id != first.intent.client_order_id
    late = _observation(first, "5", OrderStatus.FILLED, False)
    history.append(late)
    if accounting_recorded:
        for event in engine.process(late):
            if isinstance(event, (TradeRecorded, PositionUpdated)):
                history.extend(_process(engine, event))
    if compact:
        accumulator = _RecoveryAccumulator(None)
        for sequence, event in enumerate(history, start=1):
            accumulator.apply(event, sequence=sequence, assign_trade_sequence=False)
        history = list(accumulator.compact_events())
    restored = TradingEngine(EventDispatcher(TradingState()), {})
    for event in history:
        restored.process(decode_event(encode_event(event)), replay=True)
    outputs = restored.recovery_outputs()
    stop = next(event for event in outputs if isinstance(event, TradingSafetyStop))
    assert stop.execution_id == primary.execution_id
    assert stop.client_order_id == first.intent.client_order_id
    assert not any(isinstance(event, SubmitOrder) for event in outputs)
    assert not any(
        isinstance(event, ExecutionUpdated)
        and event.execution.status is ArbitrageExecutionStatus.RECOVERED
        for event in outputs
    )
    for event in outputs:
        _process(restored, event)
    assert restored.state.executions[primary.execution_id].status is ArbitrageExecutionStatus.NEEDS_REVIEW
    assert restored.state.recoveries[primary.execution_id].status is RecoveryStatus.NEEDS_REVIEW
    assert sum(
        trade.quantity.value for trade in restored.state.trades.values()
        if trade.client_order_id == first.intent.client_order_id
    ) == 5
    assert not restored.recovery_outputs()
