"""Plan and settle one bounded automatic residual-exposure recovery.

Responsibilities
----------------
- Turn a mismatched parallel execution into one journaled recovery command.
- Reuse common order accounting and record estimated versus realized economics.
- Requote terminal zero-fill orders through a bounded sequence of fresh attempts.
- Keep definite pre-submission rejections outside the venue-attempt budget.
"""

from collections.abc import Mapping
from dataclasses import replace
from decimal import Decimal

from prediction_markets.application.events import (
    ApplicationEvent,
    ExecutionUpdated,
    OrderSnapshotUpdated,
    RecoveryPlanned,
    RecoveryUpdated,
    SubmissionReceived,
    SubmitOrder,
    TradingSafetyStop,
)
from prediction_markets.application.execution.accounting import (
    ExecutionAccounting,
    is_settled_order,
)
from prediction_markets.application.execution.recovery_decision import (
    RecoveryDecisionService,
)
from prediction_markets.application.state import TradingState
from prediction_markets.domain.orderbook.entities import OrderBook
from prediction_markets.domain.ports.taker_fees import TakerFeeCalculatorPort
from prediction_markets.domain.shared.value_objects import (
    ClientOrderID,
    ContractID,
    Money,
    Quantity,
    Timestamp,
    VenueID,
)
from prediction_markets.domain.trading.entities import OrderIntent, OrderSnapshot
from prediction_markets.domain.trading.enums import (
    ArbitrageExecutionStatus,
    OrderSide,
    OrderType,
    RecoveryStatus,
    SubmissionStatus,
    TimeInForce,
)
from prediction_markets.domain.trading.entities import ArbitrageExecutionJournal
from prediction_markets.domain.trading.entities import ExposureRecovery
from prediction_markets.domain.trading.value_objects import evaluate_recovery_economics

_RECOVERY_ORDER_ATTEMPT_LIMIT = 3
_RECOVERY_LOCAL_REQUOTE_LIMIT = 3


def _can_requote(recovery: ExposureRecovery) -> bool:
    """Bound venue attempts and definite local rejections independently."""
    return (
        recovery.attempts - recovery.local_rejections < _RECOVERY_ORDER_ATTEMPT_LIMIT
        and recovery.local_rejections < _RECOVERY_LOCAL_REQUOTE_LIMIT
    )


class RecoveryLifecycle:
    """Coordinate recovery decisions and terminal order outcomes without I/O."""

    def __init__(
        self,
        state: TradingState,
        fees_by_venue: Mapping[VenueID, TakerFeeCalculatorPort],
        accounting: ExecutionAccounting,
    ) -> None:
        self._state = state
        self._fees = dict(fees_by_venue)
        self._decisions = RecoveryDecisionService(fees_by_venue)
        self._accounting = accounting

    def plan(
        self,
        execution: ArbitrageExecutionJournal,
        *,
        max_loss: Decimal,
        min_buy_notional: Decimal,
        fresh_contract_ids: frozenset[ContractID] | None = None,
        wait_for_quote: bool = False,
    ) -> tuple[ApplicationEvent, ...] | None:
        """Choose and journal the next recovery order for a residual execution.

        Parameters
        ----------
        execution : ArbitrageExecutionJournal
            Execution in ``RECOVERY_PENDING`` with unequal terminal fills.
        max_loss : Decimal
            Maximum accepted estimated recovery loss in settlement currency.
        min_buy_notional : Decimal
            Minimum limit-price-times-quantity accepted for a BUY recovery.
        fresh_contract_ids
            Contracts passing current submission freshness guards. An admissible
            fresh route takes priority over a stale, apparently better quote.
        wait_for_quote : bool, default=False
            Defer an unavailable or stale quote without allocating an order
            attempt. The caller owns the bounded data-wait deadline.

        Returns
        -------
        tuple[ApplicationEvent, ...] or None
            Recovery plan and command, a fail-closed execution update, or an
            empty tuple when authority or lifecycle prevents planning. Only
            quote deferral returns ``None``, and only in ``wait_for_quote`` mode.
        """
        if execution.status is not ArbitrageExecutionStatus.RECOVERY_PENDING:
            return ()
        previous_recovery = self._state.recoveries.get(execution.id)
        stop = self._state.execution_safety_stops.get(execution.id)
        if stop is not None:
            return self._review_plan(previous_recovery, execution, stop.reason)
        if previous_recovery is not None:
            if (
                previous_recovery.status is not RecoveryStatus.PENDING
                or previous_recovery.client_order_id is None
                or not _can_requote(previous_recovery)
            ):
                return ()
            previous_snapshot = self._state.orders.get(
                previous_recovery.client_order_id,
            )
            if (
                previous_snapshot is None
                and not (previous_recovery.last_error or "").endswith("; requoting")
            ) or (
                previous_snapshot is not None
                and (
                    not previous_snapshot.is_terminal()
                    or previous_snapshot.may_receive_more_fills is True
                    or previous_snapshot.filled_quantity.value > 0
                )
            ):
                return ()

        first_excess = (
            execution.leg1_filled_quantity.value
            > execution.leg2_filled_quantity.value
        )
        if first_excess:
            source_contract_id = execution.leg1_contract_id
            source_side = execution.leg1_side
            source_client_order_id = execution.leg1_client_order_id
            missing_contract_id = execution.leg2_contract_id
            source_fill = execution.leg1_filled_quantity
        else:
            source_contract_id = execution.leg2_contract_id
            source_side = execution.leg2_side
            source_client_order_id = execution.leg2_client_order_id
            missing_contract_id = execution.leg1_contract_id
            source_fill = execution.leg2_filled_quantity

        residual = execution.residual_quantity
        source_snapshot = self._state.orders.get(source_client_order_id)
        source_contract = self._state.contracts.get(source_contract_id)
        missing_contract = self._state.contracts.get(missing_contract_id)
        source_book = self._state.books.get(source_contract_id)
        missing_book = self._state.books.get(missing_contract_id)
        if (
            residual.value <= 0
            or source_snapshot is None
            or source_snapshot.average_price is None
            or source_contract is None
            or missing_contract is None
            or source_book is None
            or missing_book is None
        ):
            return self._review_plan(
                previous_recovery,
                execution,
                "recovery could not reconstruct source fill or current books",
            )

        calculator = self._fees.get(source_contract.venue_id)
        if calculator is None:
            return self._review_plan(
                previous_recovery,
                execution,
                f"no recovery fee calculator for {source_contract.venue_id}",
            )
        try:
            source_fee = (
                Money(
                    source_snapshot.fee.settlement_cost.amount
                    * residual.value
                    / source_fill.value,
                    source_snapshot.fee.settlement_cost.currency,
                )
                if source_snapshot.fee is not None
                else calculator.calculate(
                    source_contract.id,
                    source_snapshot.average_price,
                    residual,
                    source_side,
                ).settlement_cost
            )
            decision = self._decisions.choose(
                source_side=source_side,
                source_price=source_snapshot.average_price,
                source_fee=source_fee,
                residual_quantity=residual,
                excess_contract=source_contract,
                excess_book=source_book,
                missing_contract=missing_contract,
                missing_book=missing_book,
                max_loss=max_loss,
                min_buy_notional=min_buy_notional,
                fresh_contract_ids=fresh_contract_ids,
            )
        except (KeyError, LookupError, RuntimeError, ValueError) as error:
            return self._review_plan(
                previous_recovery,
                execution,
                f"recovery quote failed: {error}",
            )
        if wait_for_quote and (
            decision is None
            or (fresh_contract_ids is not None and decision.contract_id not in fresh_contract_ids)
        ):
            return None
        if (
            decision is None
            or decision.economics.net_result is None
            or decision.economics.total_fees is None
        ):
            return self._review_plan(
                previous_recovery,
                execution,
                "no recovery route satisfied liquidity and loss limits",
            )

        now = Timestamp.now()
        attempt = 1 if previous_recovery is None else previous_recovery.attempts + 1
        client_order_id = ClientOrderID(f"{execution.id}-recovery-{attempt}")
        recovery = ExposureRecovery(
            id=execution.id,
            execution_id=execution.id,
            route=decision.route,
            source_contract_id=source_contract.id,
            source_side=source_side,
            source_price=source_snapshot.average_price,
            source_fee=source_fee,
            venue_id=decision.venue_id,
            contract_id=decision.contract_id,
            side=decision.side,
            quantity=decision.quantity,
            limit_price=decision.limit_price,
            estimated_vwap=decision.estimated_vwap,
            estimated_recovery_fee=decision.estimated_fee.settlement_cost,
            estimated_gross_result=decision.economics.gross_result,
            estimated_net_result=decision.economics.net_result,
            status=RecoveryStatus.PENDING,
            attempts=attempt,
            local_rejections=(
                previous_recovery.local_rejections if previous_recovery is not None else 0
            ),
            client_order_id=client_order_id,
            portfolio_id=execution.portfolio_id,
            strategy_id=execution.strategy_id,
            created_at=(
                previous_recovery.created_at
                if previous_recovery is not None
                else now
            ),
            updated_at=now,
        )
        planned = (
            RecoveryPlanned(recovery)
            if previous_recovery is None
            else RecoveryUpdated(recovery)
        )
        return planned, recovery_command(recovery)

    def guard_command(
        self,
        command: SubmitOrder,
        book: OrderBook,
        *,
        max_loss: Decimal,
        min_buy_notional: Decimal,
    ) -> str | None:
        """Recheck the signed recovery quantity and economics against current depth.

        Parameters
        ----------
        command : SubmitOrder
            Current recovery command whose signed limit must remain unchanged.
        book : OrderBook
            Latest in-memory snapshot for the recovery contract.
        max_loss : Decimal
            Maximum accepted estimated net loss in settlement currency.
        min_buy_notional : Decimal
            Minimum signed-limit-price-times-quantity accepted for a BUY recovery.

        Returns
        -------
        str or None
            Bounded rejection reason, or ``None`` when quantity, limit, and
            fee-adjusted loss remain acceptable. No I/O is performed.

        Notes
        -----
        - Depth can worsen inside the signed limit while increasing VWAP enough
          to breach the loss bound. Limit-price checks alone do not cover this.
        - Freshness and execution lifecycle checks remain the caller's responsibility.
        """
        recovery = self._state.recoveries.get(command.execution_id)
        execution = self._state.executions.get(command.execution_id)
        contract = self._state.contracts.get(command.intent.contract_id)
        if recovery is None or execution is None or contract is None:
            return "recovery state unavailable"
        if (
            command.role != "recovery"
            or command.intent.client_order_id != recovery.client_order_id
            or command.intent.contract_id != recovery.contract_id
            or command.venue_id != recovery.venue_id
            or command.intent.side != recovery.side
            or command.intent.quantity != recovery.quantity
            or command.intent.limit_price != recovery.limit_price
        ):
            return "recovery command mismatch"
        if book.market_id != contract.market_id or book.outcome_id != contract.outcome_id:
            return "recovery book identity mismatch"
        if (
            recovery.source_side is None
            or recovery.source_price is None
            or recovery.source_fee is None
            or recovery.route is None
            or execution.residual_quantity.value <= 0
        ):
            return "recovery economics unavailable"
        if max_loss < 0 or min_buy_notional < 0:
            return "recovery risk limits invalid"
        if command.intent.side is OrderSide.BUY and (
            command.intent.limit_price is None
            or command.intent.limit_price.value * command.intent.quantity.value < min_buy_notional
        ):
            return "recovery minimum buy notional unavailable"
        try:
            # Plans retain the source fee allocated to the full residual, even
            # when available depth sizes this individual recovery order smaller.
            source_fee = Money(
                recovery.source_fee.amount
                * command.intent.quantity.value
                / execution.residual_quantity.value,
                recovery.source_fee.currency,
            )
            candidate = self._decisions._candidate(
                source_side=recovery.source_side,
                source_price=recovery.source_price,
                source_fee=source_fee,
                residual_quantity=command.intent.quantity,
                route=recovery.route,
                contract=contract,
                book=book,
                # Price improvement changes VWAP, not the already signed notional.
                min_buy_notional=Decimal("0"),
            )
        except (LookupError, RuntimeError, ValueError):
            return "recovery quote failed"
        if candidate is None or candidate.quantity != command.intent.quantity:
            return "recovery liquidity unavailable"
        if (
            candidate.venue_id != command.venue_id
            or candidate.contract_id != command.intent.contract_id
            or candidate.side != command.intent.side
        ):
            return "recovery command mismatch"
        limit = command.intent.limit_price
        if limit is None or (
            candidate.limit_price.value > limit.value
            if command.intent.side is OrderSide.BUY
            else candidate.limit_price.value < limit.value
        ):
            return "recovery limit price changed"
        if candidate.economics.net_result is None:
            return "recovery economics unavailable"
        if candidate.economics.net_result < -max_loss:
            return "recovery loss limit exceeded"
        return None

    def handle_order_event(
        self,
        event: SubmissionReceived | OrderSnapshotUpdated,
        previous: OrderSnapshot | None,
    ) -> tuple[ApplicationEvent, ...]:
        """Account recovery fills and settle only the current, confirmed attempt.

        Notes
        -----
        - A new fill on a previous attempt is still an accounting fact. It also
          stops new trading and requires review of all overlapping attempts.
        """
        command = (
            event.command
            if isinstance(event, SubmissionReceived)
            else self._state.commands.get(event.reference.client_order_id)
        )
        if command is None or command.role != "recovery":
            return ()
        recovery = self._state.recoveries.get(command.execution_id)
        execution = self._state.executions.get(command.execution_id)
        if recovery is None or execution is None:
            return ()
        snapshot = (
            event.result.snapshot
            if isinstance(event, SubmissionReceived)
            else event.snapshot
        )
        outputs: list[ApplicationEvent] = []
        if snapshot is not None:
            outputs.extend(
                self._accounting.record(command, previous, snapshot, execution),
            )
        if execution.resolution_method is not None or execution.status in {
            ArbitrageExecutionStatus.COMPLETED,
            ArbitrageExecutionStatus.RECOVERED,
            ArbitrageExecutionStatus.REJECTED,
        }:
            return tuple(outputs)
        if recovery.client_order_id != command.intent.client_order_id:
            if snapshot is not None and snapshot.filled_quantity.value > (
                previous.filled_quantity.value if previous is not None else 0
            ):
                message = "Late fill on an earlier recovery attempt; reconcile combined exposure"
                outputs.extend(self._needs_review(recovery, execution, message))
                outputs.append(TradingSafetyStop(
                    command.venue_id,
                    message,
                    Timestamp.now(),
                    execution_id=command.execution_id,
                    client_order_id=command.intent.client_order_id,
                ))
            return tuple(outputs)
        if execution.id in self._state.execution_safety_stops:
            if snapshot is not None:
                outputs.extend(self._settle(recovery, execution, snapshot))
            return tuple(outputs)
        if snapshot is None:
            if (
                isinstance(event, SubmissionReceived)
                and event.result.status is SubmissionStatus.ACCEPTED
                and recovery.status is RecoveryStatus.PENDING
            ):
                outputs.append(
                    RecoveryUpdated(
                        replace(
                            recovery,
                            status=RecoveryStatus.ATTEMPTING,
                            updated_at=Timestamp.now(),
                        ),
                    ),
                )
                return tuple(outputs)
            if isinstance(event, SubmissionReceived):
                reason = event.result.reason or event.result.status.value
                message = f"recovery submission failed: {reason}"
                local_rejection = (
                    event.result.status is SubmissionStatus.REJECTED
                    and event.result.reference.recovery_data == b"local-pre-submission-guard"
                )
                if local_rejection:
                    if recovery.status in {
                        RecoveryStatus.NEEDS_REVIEW,
                        RecoveryStatus.RESOLVED,
                    } or (recovery.last_error or "").endswith("; requoting"):
                        return tuple(outputs)
                    if recovery.status is not RecoveryStatus.PENDING:
                        outputs.extend(self._needs_review(
                            recovery,
                            execution,
                            "local rejection conflicts with an already submitted recovery",
                        ))
                        return tuple(outputs)
                    # Only this dispatcher sentinel proves no order was sent.
                    recovery = replace(
                        recovery,
                        local_rejections=recovery.local_rejections + 1,
                    )
                    if recovery.local_rejections >= _RECOVERY_LOCAL_REQUOTE_LIMIT:
                        message = f"{message}; local recovery requote budget exhausted"
                if (
                    event.result.status is SubmissionStatus.REJECTED
                    and _can_requote(recovery)
                ):
                    now = Timestamp.now()
                    outputs.extend(
                        (
                            RecoveryUpdated(
                                replace(
                                    recovery,
                                    status=RecoveryStatus.PENDING,
                                    last_error=f"{message}; requoting",
                                    updated_at=now,
                                ),
                            ),
                            ExecutionUpdated(
                                replace(
                                    execution,
                                    status=ArbitrageExecutionStatus.RECOVERY_PENDING,
                                    last_error=f"{message}; requoting",
                                    updated_at=now,
                                ),
                            ),
                        ),
                    )
                else:
                    outputs.extend(self._needs_review(recovery, execution, message))
            return tuple(outputs)
        if not is_settled_order(command, snapshot):
            if recovery.status is RecoveryStatus.PENDING:
                outputs.append(
                    RecoveryUpdated(
                        replace(
                            recovery,
                            status=RecoveryStatus.ATTEMPTING,
                            order_id=snapshot.order_id,
                            updated_at=Timestamp.now(),
                        ),
                    ),
                )
            return tuple(outputs)
        outputs.extend(self._settle(recovery, execution, snapshot))
        return tuple(outputs)

    def recovery_outputs(self) -> tuple[ApplicationEvent, ...]:
        """Recreate recovery outputs, including late fills on prior attempts.

        Notes
        -----
        - An isolated accounting copy combines fills sharing a position without
          applying unjournaled events to live state.
        """
        outputs: list[ApplicationEvent] = []
        accounting_state = replace(
            self._state,
            trades=dict(self._state.trades),
            positions=dict(self._state.positions),
            accounting_corrections=dict(self._state.accounting_corrections),
        )
        accounting = ExecutionAccounting(accounting_state)
        for recovery in tuple(self._state.recoveries.values()):
            if recovery.client_order_id is None:
                continue
            execution = self._state.executions.get(recovery.execution_id or "")
            if execution is None:
                continue
            terminal = execution.resolution_method is not None or execution.status in {
                ArbitrageExecutionStatus.COMPLETED,
                ArbitrageExecutionStatus.RECOVERED,
                ArbitrageExecutionStatus.REJECTED,
            }
            previous_fill = None
            for previous_command in self._state.commands.values():
                if (
                    previous_command.execution_id != execution.id
                    or previous_command.role != "recovery"
                    or previous_command.intent.client_order_id == recovery.client_order_id
                ):
                    continue
                previous_snapshot = self._state.orders.get(previous_command.intent.client_order_id)
                if previous_snapshot is None or previous_snapshot.filled_quantity.value <= 0:
                    continue
                recorded = accounting.record(previous_command, None, previous_snapshot, execution)
                outputs.extend(recorded)
                for event in recorded:
                    accounting_state.apply(event)
                previous_fill = previous_command
            if previous_fill is not None and not terminal and execution.id not in self._state.execution_safety_stops:
                outputs.append(TradingSafetyStop(
                    previous_fill.venue_id,
                    "Late fill on an earlier recovery attempt; reconcile combined exposure",
                    Timestamp.now(),
                    execution_id=execution.id,
                    client_order_id=previous_fill.intent.client_order_id,
                ))
            command = self._state.commands.get(recovery.client_order_id)
            review_locked = recovery.execution_id in self._state.execution_safety_stops
            if (
                command is None
                and recovery.status is RecoveryStatus.PENDING
                and not review_locked
                and previous_fill is None
                and not terminal
            ):
                outputs.append(recovery_command(recovery))
                continue
            snapshot = self._state.orders.get(recovery.client_order_id)
            if (
                command is None
                or snapshot is None
                or (previous_fill is None and not review_locked and not is_settled_order(command, snapshot))
            ):
                continue
            recorded = accounting.record(command, None, snapshot, execution)
            outputs.extend(recorded)
            for event in recorded:
                accounting_state.apply(event)
            if terminal or previous_fill is not None:
                if (
                    terminal
                    and not previous_fill
                    and not review_locked
                    and execution.status is ArbitrageExecutionStatus.RECOVERED
                    and execution.resolution_method is None
                    and recovery.status is RecoveryStatus.RESOLVED
                ):
                    outputs.extend(self._settle(recovery, execution, snapshot))
                continue
            if review_locked or recovery.status in {
                RecoveryStatus.PENDING,
                RecoveryStatus.ATTEMPTING,
            } or (
                snapshot.filled_quantity.value >= recovery.quantity.value
                and execution.status is not ArbitrageExecutionStatus.RECOVERED
            ):
                outputs.extend(self._settle(recovery, execution, snapshot))
        return tuple(outputs)

    def _settle(
        self,
        recovery: ExposureRecovery,
        execution: ArbitrageExecutionJournal,
        snapshot: OrderSnapshot,
    ) -> tuple[ApplicationEvent, ...]:
        """Update observed economics without clearing an order-scoped safety review."""
        review_locked = execution.id in self._state.execution_safety_stops
        filled = snapshot.filled_quantity
        if review_locked and filled.value < recovery.filled_quantity.value:
            return ()
        economics = None
        if filled.value > 0 and snapshot.average_price is not None:
            paired_value = min(filled.value, recovery.quantity.value)
            paired_ratio = paired_value / filled.value
            paired_quantity = Quantity(paired_value)
            source_fee = (
                Money(
                    recovery.source_fee.amount * paired_value / recovery.quantity.value,
                    recovery.source_fee.currency,
                )
                if recovery.source_fee is not None
                else None
            )
            recovery_fee = (
                Money(
                    snapshot.fee.settlement_cost.amount * paired_ratio,
                    snapshot.fee.settlement_cost.currency,
                )
                if snapshot.fee is not None
                else None
            )
            economics = evaluate_recovery_economics(
                source_side=recovery.source_side,
                source_price=recovery.source_price,
                quantity=paired_quantity,
                route=recovery.route,
                recovery_price=snapshot.average_price,
                source_fee=source_fee,
                recovery_fee=recovery_fee,
            )
        resolved = filled.value >= recovery.quantity.value
        terminal_before = recovery.status in {
            RecoveryStatus.RESOLVED,
            RecoveryStatus.NEEDS_REVIEW,
        }
        if (
            filled.value <= 0
            and _can_requote(recovery)
            and not terminal_before
            and not review_locked
        ):
            reason = f": {snapshot.reason}" if snapshot.reason else ""
            error = f"recovery order did not fill{reason}; requoting"
            now = Timestamp.now()
            return (
                RecoveryUpdated(
                    replace(
                        recovery,
                        status=RecoveryStatus.PENDING,
                        order_id=snapshot.order_id,
                        filled_quantity=filled,
                        average_price=snapshot.average_price,
                        recovery_fee=None,
                        actual_gross_result=None,
                        actual_net_result=None,
                        last_error=error,
                        updated_at=now,
                    ),
                ),
                ExecutionUpdated(
                    replace(
                        execution,
                        status=ArbitrageExecutionStatus.RECOVERY_PENDING,
                        last_error=error,
                        updated_at=now,
                    ),
                ),
            )
        error = None
        if not resolved:
            error = (
                "recovery order did not fill"
                if filled.value <= 0
                else "recovery order left residual exposure"
            )
            if snapshot.reason:
                error = f"{error}: {snapshot.reason}"
        if review_locked:
            error = self._state.execution_safety_stops[execution.id].reason
        updated = replace(
            recovery,
            status=(
                RecoveryStatus.RESOLVED
                if resolved and not review_locked
                else RecoveryStatus.NEEDS_REVIEW
            ),
            order_id=snapshot.order_id,
            filled_quantity=filled,
            average_price=snapshot.average_price,
            recovery_fee=(
                snapshot.fee.settlement_cost if snapshot.fee is not None else None
            ),
            actual_gross_result=(
                economics.gross_result if economics is not None else None
            ),
            actual_net_result=(
                economics.net_result if economics is not None else None
            ),
            last_error=error,
        )
        outputs: list[ApplicationEvent] = []
        if updated != recovery:
            outputs.append(RecoveryUpdated(replace(updated, updated_at=Timestamp.now())))
        execution_status = (
            ArbitrageExecutionStatus.RECOVERED
            if resolved and not review_locked
            else ArbitrageExecutionStatus.NEEDS_REVIEW
        )
        residual = updated.remaining_quantity()
        if (
            execution.status is not execution_status
            or execution.residual_quantity != residual
            or execution.last_error != error
        ):
            outputs.append(
                ExecutionUpdated(
                    replace(
                        execution,
                        status=execution_status,
                        residual_quantity=residual,
                        last_error=error,
                        updated_at=Timestamp.now(),
                    ),
                ),
            )
        return tuple(outputs)

    def _review_plan(
        self,
        recovery: ExposureRecovery | None,
        execution: ArbitrageExecutionJournal,
        reason: str,
    ) -> tuple[ApplicationEvent, ...]:
        """Fail closed when an initial or repeated quote cannot be submitted."""
        if recovery is None:
            return self.review_execution(execution, reason)
        return self._needs_review(recovery, execution, reason)

    def _needs_review(
        self,
        recovery: ExposureRecovery,
        execution: ArbitrageExecutionJournal,
        message: str,
    ) -> tuple[ApplicationEvent, ...]:
        """Persist a terminal recovery failure and stop the parent execution."""
        now = Timestamp.now()
        return (
            RecoveryUpdated(
                replace(
                    recovery,
                    status=RecoveryStatus.NEEDS_REVIEW,
                    last_error=message,
                    updated_at=now,
                ),
            ),
            ExecutionUpdated(
                replace(
                    execution,
                    status=ArbitrageExecutionStatus.NEEDS_REVIEW,
                    last_error=message,
                    updated_at=now,
                ),
            ),
        )

    @staticmethod
    def review_execution(
        execution: ArbitrageExecutionJournal,
        reason: str,
    ) -> tuple[ApplicationEvent, ...]:
        return (
            ExecutionUpdated(
                replace(
                    execution,
                    status=ArbitrageExecutionStatus.NEEDS_REVIEW,
                    last_error=reason,
                    updated_at=Timestamp.now(),
                ),
            ),
        )


def recovery_command(recovery: ExposureRecovery) -> SubmitOrder:
    """Rebuild the exact venue-neutral command from a durable recovery plan."""
    if recovery.execution_id is None or recovery.client_order_id is None:
        raise ValueError("Automatic recovery requires execution and client identifiers")
    return SubmitOrder(
        execution_id=recovery.execution_id,
        role="recovery",
        venue_id=recovery.venue_id,
        intent=OrderIntent(
            contract_id=recovery.contract_id,
            side=recovery.side,
            quantity=recovery.quantity,
            order_type=OrderType.LIMIT,
            client_order_id=recovery.client_order_id,
            limit_price=recovery.limit_price,
            time_in_force=TimeInForce.IOC,
            strategy_id=recovery.strategy_id,
            portfolio_id=recovery.portfolio_id,
            created_at=recovery.updated_at,
            reason=f"automatic exposure recovery for {recovery.execution_id}",
        ),
    )
