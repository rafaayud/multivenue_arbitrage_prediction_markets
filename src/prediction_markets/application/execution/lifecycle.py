"""Advance paired execution state after orders leave the submission boundary.

Responsibilities
----------------
- Reconcile ordered pair outcomes with replayable execution state.
- Record incremental fills, fee corrections, and position changes.
- Finalize paired legs and derive restart-recovery outputs.
- Build execution commands and immutable order-book decision snapshots.

Notes
-----
- This module is not called by order-book detection or opportunity admission.
- Its operations run after an order submission or during recovery.
"""

import json
import time
from dataclasses import replace
from decimal import Decimal

from prediction_markets.application.execution.accounting import (
    ExecutionAccounting,
    is_settled_order,
)
from prediction_markets.application.events import (
    ApplicationEvent,
    ExecutionUpdated,
    OrderSnapshotUpdated,
    RecoveryUpdated,
    SubmissionReceived,
    SubmitOrder,
    TradingSafetyStop,
)
from prediction_markets.application.state import TradingState
from prediction_markets.domain.orderbook.entities import OrderBook
from prediction_markets.domain.shared.value_objects import (
    ContractID,
    Price,
    Quantity,
    Timestamp,
    VenueID,
)
from prediction_markets.domain.trading.entities import (
    OrderIntent,
    OrderSnapshot,
)
from prediction_markets.domain.trading.enums import (
    ArbitrageExecutionStatus,
    OrderSide,
    OrderType,
    RecoveryStatus,
    SubmissionStatus,
    TimeInForce,
)
from prediction_markets.domain.trading.entities import ArbitrageExecutionJournal
from prediction_markets.domain.trading.value_objects import OrderBookDecisionSnapshot


class _ExecutionLifecycle:
    """Derive accounting and parallel transitions from order outcomes.

    Attributes
    ----------
    state
        Replayable trading state read by lifecycle decisions.

    Notes
    -----
    - This collaborator is used only for order outcomes and restart recovery.
    - Order-book detection and opportunity admission never call it.
    """

    def __init__(
        self,
        state: TradingState,
        accounting: ExecutionAccounting,
    ) -> None:
        self.state = state
        self._accounting = accounting

    def recovery_outputs(self) -> tuple[ApplicationEvent, ...]:
        """Resume journaled executions interrupted between derived events.

        Returns
        -------
        tuple[ApplicationEvent, ...]
            Missing accounting, scoped safety reviews, or order transitions
            derived from already journaled observations.
        """
        outputs: list[ApplicationEvent] = []
        for execution in tuple(self.state.executions.values()):
            stop = self.state.execution_safety_stops.get(execution.id)
            if execution.resolution_method is not None or execution.status in {
                ArbitrageExecutionStatus.COMPLETED,
                ArbitrageExecutionStatus.RECOVERED,
                ArbitrageExecutionStatus.REJECTED,
            }:
                continue
            if stop is not None or execution.status is ArbitrageExecutionStatus.NEEDS_REVIEW:
                for client_order_id in (
                    execution.leg1_client_order_id,
                    execution.leg2_client_order_id,
                ):
                    command = self.state.commands.get(client_order_id)
                    snapshot = self.state.orders.get(client_order_id)
                    if command is not None and snapshot is not None:
                        outputs.extend(
                            self._accounting.record(command, None, snapshot, execution),
                        )
                outputs.extend(self.review_execution(
                    execution,
                    stop.reason if stop is not None else execution.last_error,
                ))
                continue
            roles = {
                ArbitrageExecutionStatus.PLANNED: ("primary", "hedge"),
                ArbitrageExecutionStatus.PRIMARY_PENDING: ("primary",),
                ArbitrageExecutionStatus.HEDGE_PENDING: ("hedge",),
            }.get(execution.status, ())
            current = execution
            for role in roles:
                client_order_id = (
                    current.leg1_client_order_id
                    if role == "primary"
                    else current.leg2_client_order_id
                )
                command = self.state.commands.get(client_order_id)
                if command is None:
                    command = _execution_command(current, role)
                    outputs.append(command)
                snapshot = self.state.orders.get(client_order_id)
                if snapshot is None or not is_settled_order(command, snapshot):
                    continue
                outputs.extend(
                    self._accounting.record(command, None, snapshot, current),
                )
                updates = self._after_parallel_leg(current, role, snapshot)
                outputs.extend(updates)
                if updates:
                    current = updates[-1].execution
        return tuple(outputs)

    def handle_safety_stop(
        self,
        event: TradingSafetyStop,
    ) -> tuple[ApplicationEvent, ...]:
        """Persist review only for a validated, still-active order scope."""
        if self.state.execution_safety_stops.get(event.execution_id) != event:
            return ()
        execution = self.state.executions.get(event.execution_id)
        if execution is None or execution.resolution_method is not None or execution.status in {
            ArbitrageExecutionStatus.COMPLETED,
            ArbitrageExecutionStatus.RECOVERED,
            ArbitrageExecutionStatus.REJECTED,
        }:
            return ()
        return self.review_execution(execution, event.reason)

    def review_execution(
        self,
        execution: ArbitrageExecutionJournal,
        reason: str | None,
    ) -> tuple[ApplicationEvent, ...]:
        """Reconcile a reviewed execution from the latest final order evidence.

        Notes
        -----
        - Cumulative leg fills never regress and no replacement is authorized.
        - Matching settled direct legs complete automatically after a transient
          cancellation uncertainty; the global safety halt remains operator-controlled.
        - Existing recovery economics are preserved for its own lifecycle to update.
        """
        changes = {}
        fills = []
        settled = []
        for leg in (1, 2):
            client_order_id = getattr(execution, f"leg{leg}_client_order_id")
            snapshot = self.state.orders.get(client_order_id)
            command = self.state.commands.get(client_order_id)
            previous_fill = getattr(execution, f"leg{leg}_filled_quantity")
            filled = max(
                previous_fill.value,
                snapshot.filled_quantity.value if snapshot is not None else Decimal("0"),
            )
            fills.append(filled)
            settled.append(
                command is not None
                and snapshot is not None
                and is_settled_order(command, snapshot)
            )
            changes[f"leg{leg}_filled_quantity"] = Quantity(filled)
            if snapshot is not None and snapshot.order_id is not None:
                changes[f"leg{leg}_order_id"] = snapshot.order_id
        recovery = self.state.recoveries.get(execution.id)
        outputs: list[ApplicationEvent] = []
        residual = Quantity(abs(fills[0] - fills[1]))
        if recovery is not None:
            residual = execution.residual_quantity
            if execution.id in self.state.execution_safety_stops:
                snapshot = self.state.orders.get(recovery.client_order_id)
                filled = Quantity(max(
                    recovery.filled_quantity.value,
                    snapshot.filled_quantity.value if snapshot is not None else Decimal("0"),
                ))
                residual = Quantity(max(Decimal("0"), recovery.quantity.value - filled.value))
                reviewed = replace(
                    recovery,
                    status=RecoveryStatus.NEEDS_REVIEW,
                    filled_quantity=filled,
                    order_id=(snapshot.order_id or recovery.order_id) if snapshot is not None else recovery.order_id,
                    last_error=reason,
                )
                if reviewed != recovery:
                    outputs.append(RecoveryUpdated(replace(reviewed, updated_at=Timestamp.now())))
        completed = (
            recovery is None
            and all(settled)
            and fills[0] == fills[1]
            and fills[0] > 0
        )
        updated = replace(
            execution,
            status=(
                ArbitrageExecutionStatus.COMPLETED
                if completed
                else ArbitrageExecutionStatus.NEEDS_REVIEW
            ),
            residual_quantity=residual,
            last_error=None if completed else reason,
            **changes,
        )
        if updated != execution:
            outputs.append(ExecutionUpdated(replace(updated, updated_at=Timestamp.now())))
        return tuple(outputs)

    def handle_order_event(
        self,
        event: SubmissionReceived | OrderSnapshotUpdated,
        previous: OrderSnapshot | None,
    ) -> tuple[ApplicationEvent, ...]:
        """Derive accounting and lifecycle events from one order update."""
        command = (
            event.command
            if isinstance(event, SubmissionReceived)
            else self.state.commands.get(event.reference.client_order_id)
        )
        if command is None:
            return ()
        snapshot = (
            event.result.snapshot
            if isinstance(event, SubmissionReceived)
            else event.snapshot
        )
        outputs: list[ApplicationEvent] = []
        if snapshot is not None:
            outputs.extend(
                self._accounting.record(
                    command,
                    previous,
                    snapshot,
                    self.state.executions.get(command.execution_id),
                ),
            )
        outputs.extend(self._advance_execution(command, event, snapshot))
        return tuple(outputs)

    def _advance_execution(
        self,
        command: SubmitOrder,
        event: SubmissionReceived | OrderSnapshotUpdated,
        snapshot: OrderSnapshot | None,
    ) -> tuple[ApplicationEvent, ...]:
        execution = self.state.executions.get(command.execution_id)
        if execution is None:
            return ()
        if execution.resolution_method is not None or execution.status in {
            ArbitrageExecutionStatus.COMPLETED,
            ArbitrageExecutionStatus.RECOVERED,
            ArbitrageExecutionStatus.REJECTED,
        }:
            return ()
        stop = self.state.execution_safety_stops.get(execution.id)
        if stop is not None or execution.status is ArbitrageExecutionStatus.NEEDS_REVIEW:
            return self.review_execution(
                execution,
                stop.reason if stop is not None else execution.last_error,
            )
        settled = snapshot is not None and is_settled_order(command, snapshot)
        rejected = (
            isinstance(event, SubmissionReceived)
            and snapshot is None
            and event.result.status is SubmissionStatus.REJECTED
        )
        if settled or rejected:
            timings = self.state.timings.get(command.execution_id)
            if timings is not None:
                timings.mark_terminal(
                    command.role,
                    str(command.venue_id),
                    time.monotonic_ns(),
                )
        if isinstance(event, SubmissionReceived) and snapshot is None:
            if event.result.status is SubmissionStatus.ACCEPTED:
                return ()
            message = f"{command.role} submission {event.result.status.value}"
            if event.result.reason:
                message = f"{message}: {event.result.reason}"
            if event.result.status is SubmissionStatus.UNKNOWN:
                self.state.trading_enabled = False
                self.state.last_error = message
                return ()
            if _requires_review_rejection(command, event.result.reason):
                return (
                    ExecutionUpdated(
                        replace(
                            execution,
                            status=ArbitrageExecutionStatus.NEEDS_REVIEW,
                            residual_quantity=Quantity(Decimal("0")),
                            last_error=message,
                            updated_at=Timestamp.now(),
                        ),
                    ),
                )
            return self._after_parallel_leg(
                execution,
                command.role,
                None,
                message,
            )
        if snapshot is None or not settled:
            return ()
        reason = snapshot.reason
        if isinstance(event, SubmissionReceived):
            reason = event.result.reason or reason
        error = None
        if snapshot.filled_quantity.value <= 0:
            error = f"{command.role} leg did not fill"
            if reason:
                error = f"{error}: {reason}"
        return self._after_parallel_leg(
            execution,
            command.role,
            snapshot,
            error,
        )

    def _after_parallel_leg(
        self,
        execution: ArbitrageExecutionJournal,
        role: str,
        snapshot: OrderSnapshot | None,
        error: str | None = None,
    ) -> tuple[ExecutionUpdated, ...]:
        """Record one terminal leg and finalize after its peer settles.

        Parameters
        ----------
        execution
            Latest durable state for the two-leg execution.
        role
            Leg that reached a terminal result: ``primary`` or ``hedge``.
        snapshot
            Terminal venue snapshot, or ``None`` for a rejected submission.
        error
            Normalized rejection or no-fill reason, when available.

        Returns
        -------
        tuple[ExecutionUpdated, ...]
            One durable state transition, or an empty tuple for a duplicate result.
        """
        expected_statuses = (
            {
                ArbitrageExecutionStatus.PLANNED,
                ArbitrageExecutionStatus.PRIMARY_PENDING,
            }
            if role == "primary"
            else {
                ArbitrageExecutionStatus.PLANNED,
                ArbitrageExecutionStatus.HEDGE_PENDING,
            }
        )
        if execution.status not in expected_statuses:
            return ()
        quantity = (
            snapshot.filled_quantity
            if snapshot is not None
            else Quantity(Decimal("0"))
        )
        changes = {
            "leg1_order_id" if role == "primary" else "leg2_order_id": (
                snapshot.order_id if snapshot is not None else None
            ),
            (
                "leg1_filled_quantity"
                if role == "primary"
                else "leg2_filled_quantity"
            ): quantity,
        }
        waiting = execution.status is ArbitrageExecutionStatus.PLANNED
        if waiting:
            updated = replace(
                execution,
                status=(
                    ArbitrageExecutionStatus.HEDGE_PENDING
                    if role == "primary"
                    else ArbitrageExecutionStatus.PRIMARY_PENDING
                ),
                residual_quantity=quantity,
                last_error=error,
                updated_at=Timestamp.now(),
                **changes,
            )
            return (ExecutionUpdated(updated),)

        primary_fill = (
            quantity if role == "primary" else execution.leg1_filled_quantity
        )
        hedge_fill = quantity if role == "hedge" else execution.leg2_filled_quantity
        residual = Quantity(abs(primary_fill.value - hedge_fill.value))
        matched = residual.value == 0
        filled = primary_fill.value > 0
        detail = execution.last_error or error
        if matched and filled:
            last_error = None
            status = ArbitrageExecutionStatus.COMPLETED
        elif matched:
            last_error = detail or "parallel legs did not fill"
            status = ArbitrageExecutionStatus.REJECTED
        else:
            last_error = "parallel legs left residual exposure"
            if detail:
                last_error = f"{last_error}: {detail}"
            status = ArbitrageExecutionStatus.RECOVERY_PENDING
        updated = replace(
            execution,
            status=status,
            residual_quantity=residual,
            last_error=last_error,
            updated_at=Timestamp.now(),
            **changes,
        )
        timings = self.state.timings.get(execution.id)
        latency_trace_json = (
            json.dumps(timings.snapshot(execution.id), separators=(",", ":"))
            if timings is not None
            else None
        )
        return (ExecutionUpdated(updated, latency_trace_json),)


def _requires_review_rejection(command: SubmitOrder, reason: str | None) -> bool:
    """Identify a live venue-funding rejection that must stop trading.

    Parameters
    ----------
    command
        Live or recovery command associated with the rejection.
    reason
        Normalized local or venue rejection detail.

    Returns
    -------
    bool
        Whether the execution requires review and the run must stop.
    """
    if command.role == "recovery" or reason is None:
        return False
    normalized = reason.lower()
    return "pre-submission funds guard" in normalized or any(
        marker in normalized
        for marker in (
            "not enough balance / allowance",
            "balance is not enough",
            "insufficient balance",
            "insufficient collateral",
        )
    )


def _decision_snapshot(
    venue_id: VenueID,
    contract_id: ContractID,
    side: OrderSide,
    limit_price: Price,
    quantity: Quantity,
    order_book: OrderBook | None,
) -> OrderBookDecisionSnapshot | None:
    """Capture executable depth without adding I/O or a journal append."""
    if order_book is None:
        return None
    levels = (
        tuple(
            level
            for level in order_book.asks
            if level.price.value <= limit_price.value
        )
        if side is OrderSide.BUY
        else tuple(
            level
            for level in order_book.bids
            if level.price.value >= limit_price.value
        )
    )
    return OrderBookDecisionSnapshot(
        venue_id=venue_id,
        contract_id=contract_id,
        side=side,
        limit_price=limit_price,
        requested_quantity=quantity,
        levels=levels,
        book_timestamp=order_book.timestamp,
        book_age_ns=(
            max(0, time.monotonic_ns() - order_book.received_at_ns)
            if order_book.received_at_ns is not None
            else None
        ),
        source_hash=order_book.source_hash,
        captured_at=Timestamp.now(),
    )


def _execution_command(
    execution: ArbitrageExecutionJournal,
    role: str,
) -> SubmitOrder:
    """Rebuild one venue-neutral command from durable execution state."""
    primary = role == "primary"
    created_at = execution.created_at if primary else execution.updated_at
    return SubmitOrder(
        execution_id=execution.id,
        role="primary" if primary else "hedge",
        venue_id=(execution.leg1_venue_id if primary else execution.leg2_venue_id),
        intent=OrderIntent(
            contract_id=(
                execution.leg1_contract_id
                if primary
                else execution.leg2_contract_id
            ),
            side=execution.leg1_side if primary else execution.leg2_side,
            quantity=(execution.leg1_quantity if primary else execution.leg2_quantity),
            order_type=OrderType.LIMIT,
            client_order_id=(
                execution.leg1_client_order_id
                if primary
                else execution.leg2_client_order_id
            ),
            limit_price=(
                execution.leg1_limit_price
                if primary
                else execution.leg2_limit_price
            ),
            time_in_force=TimeInForce.IOC,
            strategy_id=execution.strategy_id,
            portfolio_id=execution.portfolio_id,
            created_at=created_at,
            reason=f"{role} leg for arbitrage {execution.id}",
        ),
    )
