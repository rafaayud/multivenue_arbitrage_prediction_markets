"""Record explicit runtime accounting adjustments.

Responsibilities
----------------
- Append manual accounting changes to the durable journal.
- Record externally resolved exposure through the normal trade and position ledger.
- Rebuild corrected positions from retained journal economics.
- Apply each durable event to the in-memory dispatcher after append.
"""

import asyncio
from collections.abc import Callable, Mapping
from dataclasses import replace
from decimal import Decimal, ROUND_DOWN
from uuid import uuid4

from prediction_markets.application.events import (
    AccountingCorrectionRecorded,
    CashMovementRecorded,
    ExecutionUpdated,
    OrderSnapshotUpdated,
    PositionUpdated,
    RecoveryUpdated,
    TradeRecorded,
)
from prediction_markets.application.execution.accounting import (
    apply_trade,
    is_settled_order,
    manual_resolution_client_order_id,
    manual_resolution_metadata,
    manual_resolution_order_id,
    manual_resolution_trade_id,
    rebuild_corrected_position,
)
from prediction_markets.application.pipeline import JournalRecord, TradingPipeline
from prediction_markets.application.state import EventDispatcher, TradingState
from prediction_markets.domain.shared.value_objects import (
    Currency,
    Money,
    Price,
    Quantity,
    Timestamp,
    VenueID,
)
from prediction_markets.domain.ports.execution import ExecutionPort
from prediction_markets.domain.trading.entities import (
    AccountingCorrection,
    ArbitrageExecutionJournal,
    CashMovement,
    Trade,
)
from prediction_markets.domain.trading.enums import (
    ArbitrageExecutionStatus,
    OrderSide,
    ReconciliationStatus,
    RecoveryStatus,
)
from prediction_markets.domain.trading.portfolio import STRATEGY_PORTFOLIO_ID
from prediction_markets.infrastructure.binary_journal import BinaryJournal


class RuntimeAccounting:
    """Own explicit durable adjustments outside automatic execution."""

    def __init__(
        self,
        journal: BinaryJournal,
        state: TradingState,
        dispatcher: EventDispatcher,
        pipeline: TradingPipeline,
        recovery_records: Callable[[], tuple[JournalRecord, ...]],
        execution: Mapping[VenueID, ExecutionPort],
    ) -> None:
        self.journal = journal
        self.state = state
        self.dispatcher = dispatcher
        self.pipeline = pipeline
        self._recovery_records = recovery_records
        self._execution = dict(execution)

    async def reconcile_execution(
        self,
        execution_id: str,
    ) -> ArbitrageExecutionJournal:
        """Reconcile venue fills without recording a manual resolution.

        Parameters
        ----------
        execution_id : str
            Execution whose submitted orders must be checked.

        Returns
        -------
        ArbitrageExecutionJournal
            Current durable execution after reconciliation.

        Raises
        ------
        KeyError
            If the execution does not exist.
        RuntimeError
            If live trading is enabled or venue reconciliation is unavailable.

        Notes
        -----
        - This operation can record proven venue fills, but never fabricates an
          operator trade or settlement.
        """
        if self.state.trading_enabled:
            raise RuntimeError("Disable live trading before reconciling an execution")
        if execution_id not in self.state.executions:
            raise KeyError(execution_id)
        await self._reconcile_execution_orders(execution_id)
        return self.state.executions[execution_id]

    async def complete_execution(
        self,
        execution_id: str,
        *,
        method: str,
        price: Price,
        fee_amount_usd: Decimal,
        executed_at: Timestamp,
        external_reference: str | None = None,
    ) -> tuple[ArbitrageExecutionJournal, Trade]:
        """Record an externally resolved exposure and its accounting trade.

        Parameters
        ----------
        execution_id : str
            Execution whose unmatched position was closed externally.
        method : str
            Operator action: ``manual_sale`` or ``settlement``.
        price : Price
            Actual exit price or binary settlement payout per contract.
        fee_amount_usd : Decimal
            Total externally observed fee in USD.
        executed_at : Timestamp
            Economic timestamp of the external action.
        external_reference : str, optional
            Venue transaction, claim, or operator reference.

        Returns
        -------
        tuple[ArbitrageExecutionJournal, Trade]
            Durable completed execution and its inferred closing trade.

        Raises
        ------
        KeyError
            If the execution does not exist.
        ValueError
            If the execution, operator data, recovery source, or position cannot
            be closed safely.
        RuntimeError
            If live trading is enabled or venue reconciliation is unavailable.

        Notes
        -----
        - The recorded trade is an accounting fact and never enters venue order
          preparation or submission.
        - If an inventory-backed SELL flattened its source position, close the
          remaining execution leg, not the venue of the last failed recovery.
        """
        if self.state.trading_enabled:
            raise RuntimeError("Disable live trading before completing an execution")
        execution = self.state.executions.get(execution_id)
        if execution is None:
            raise KeyError(execution_id)
        if method not in {"manual_sale", "settlement"}:
            raise ValueError("Unsupported manual resolution method")
        reference = external_reference.strip() if external_reference else None
        trade_id = manual_resolution_trade_id(execution_id)
        existing_trade = self.state.trades.get(trade_id)
        if execution.status is ArbitrageExecutionStatus.COMPLETED:
            metadata = (
                manual_resolution_metadata(existing_trade)
                if existing_trade is not None
                else None
            )
            if (
                existing_trade is None
                or metadata != (method, reference)
                or existing_trade.price != price
                or existing_trade.fee_settlement_cost
                != Money(fee_amount_usd, Currency("USD"))
                or existing_trade.executed_at != executed_at
            ):
                raise ValueError("Execution was already completed with different data")
            return execution, existing_trade
        if execution.status is not ArbitrageExecutionStatus.NEEDS_REVIEW:
            raise ValueError("Execution is not waiting for manual review")
        await self._reconcile_execution_orders(execution_id)
        execution = self.state.executions[execution_id]
        if execution.status is not ArbitrageExecutionStatus.NEEDS_REVIEW:
            raise ValueError(
                "Venue reconciliation resolved the execution; refresh activity",
            )
        if execution.residual_quantity.value <= 0:
            raise ValueError("Execution has no residual exposure")

        recovery = self.state.recoveries.get(execution_id)
        source_contract_id = recovery.source_contract_id if recovery else None
        if source_contract_id is None:
            source_contract_id = (
                execution.leg1_contract_id
                if execution.leg1_filled_quantity.value
                > execution.leg2_filled_quantity.value
                else execution.leg2_contract_id
            )
        if source_contract_id == execution.leg1_contract_id:
            source_venue_id = execution.leg1_venue_id
            source_side = execution.leg1_side
        elif source_contract_id == execution.leg2_contract_id:
            source_venue_id = execution.leg2_venue_id
            source_side = execution.leg2_side
        else:
            raise ValueError("Recovery source does not match an execution leg")
        if (
            recovery is not None
            and recovery.source_side is not None
            and recovery.source_side is not source_side
        ):
            raise ValueError("Recovery source side does not match the execution leg")

        portfolio_id = execution.portfolio_id or STRATEGY_PORTFOLIO_ID
        closing_contract_id = source_contract_id
        closing_venue_id = source_venue_id
        trade_side = (
            OrderSide.SELL if source_side is OrderSide.BUY else OrderSide.BUY
        )
        current_position = next(
            (
                position
                for position in self.state.positions.values()
                if (
                    position.venue_id,
                    position.portfolio_id,
                    position.contract_id,
                )
                == (source_venue_id, portfolio_id, source_contract_id)
            ),
            None,
        )
        expected_direction = (
            Decimal("1") if source_side is OrderSide.BUY else Decimal("-1")
        )
        source_covers = (
            current_position is not None
            and current_position.signed_quantity * expected_direction > 0
            and current_position.quantity.value >= execution.residual_quantity.value
        )
        if not source_covers:
            if source_side is not OrderSide.SELL:
                raise ValueError(
                    "Recorded position cannot safely cover the residual exposure",
                )
            if source_contract_id == execution.leg1_contract_id:
                closing_contract_id = execution.leg2_contract_id
                closing_venue_id = execution.leg2_venue_id
                trade_side = execution.leg2_side
            else:
                closing_contract_id = execution.leg1_contract_id
                closing_venue_id = execution.leg1_venue_id
                trade_side = execution.leg1_side
            if trade_side is not OrderSide.SELL:
                raise ValueError("Remaining inventory leg must be a SELL")
            current_position = next(
                (
                    position
                    for position in self.state.positions.values()
                    if (
                        position.venue_id,
                        position.portfolio_id,
                        position.contract_id,
                    )
                    == (closing_venue_id, portfolio_id, closing_contract_id)
                ),
                None,
            )
            if (
                current_position is None
                or current_position.signed_quantity <= 0
                or current_position.quantity.value
                < execution.residual_quantity.value
            ):
                raise ValueError(
                    "Recorded position cannot safely cover the residual exposure",
                )

        fee = Money(fee_amount_usd, Currency("USD"))
        trade = Trade(
            id=trade_id,
            contract_id=closing_contract_id,
            venue_id=closing_venue_id,
            side=trade_side,
            quantity=execution.residual_quantity,
            price=price,
            executed_at=executed_at,
            order_id=manual_resolution_order_id(method, reference),
            client_order_id=manual_resolution_client_order_id(execution_id),
            portfolio_id=portfolio_id,
            strategy_id=execution.strategy_id,
            fee=fee,
            fee_settlement_cost=fee,
        )
        if existing_trade is not None and replace(
            existing_trade,
            journal_sequence=None,
        ) != trade:
            raise ValueError("Execution already has different manual resolution data")
        if existing_trade is None:
            await self.pipeline.event_loop.process(
                TradeRecorded(trade),
                enqueue_commands=False,
            )
        if (
            existing_trade is None
            or current_position.updated_at is None
            or current_position.updated_at < executed_at
        ):
            await self.pipeline.event_loop.process(
                PositionUpdated(apply_trade(current_position, trade).position),
                enqueue_commands=False,
            )
        if recovery is not None and recovery.status is not RecoveryStatus.RESOLVED:
            await self.pipeline.event_loop.process(
                RecoveryUpdated(
                    replace(
                        recovery,
                        status=RecoveryStatus.RESOLVED,
                        last_error=None,
                        updated_at=Timestamp.now(),
                    ),
                ),
                enqueue_commands=False,
            )
        completed = replace(
            execution,
            status=ArbitrageExecutionStatus.COMPLETED,
            resolution_method=method,
            residual_quantity=Quantity(Decimal("0")),
            last_error=None,
            updated_at=Timestamp.now(),
        )
        await self.pipeline.event_loop.process(
            ExecutionUpdated(completed),
            enqueue_commands=False,
        )
        return completed, trade

    async def _reconcile_execution_orders(self, execution_id: str) -> None:
        """Persist authoritative late fills before manual accounting.

        Parameters
        ----------
        execution_id
            Execution whose original and recovery orders must be checked.

        Raises
        ------
        RuntimeError
            If a prepared order cannot be authoritatively reconciled or its fill
            quantity regresses.

        Notes
        -----
        - Cancelled orders with unresolved settlement still require reconciliation.
        - Settled fills without a fee are re-read so accounting can persist the
          venue fee as a correction without duplicating the trade.
        """
        for command in tuple(self.state.commands.values()):
            if command.execution_id != execution_id:
                continue
            client_order_id = command.intent.client_order_id
            if client_order_id is None:
                continue
            prepared = self.state.prepared.get(client_order_id)
            if prepared is None:
                continue
            current = self.state.orders.get(client_order_id)
            if (
                current is not None
                and is_settled_order(command, current)
                and (current.filled_quantity.value <= 0 or current.fee is not None)
            ):
                continue
            adapter = self._execution.get(command.venue_id)
            if adapter is None:
                raise RuntimeError(
                    f"Execution adapter unavailable for {command.venue_id}",
                )
            reconciled = await asyncio.to_thread(
                adapter.reconcile,
                prepared.reference,
            )
            if (
                reconciled.status is not ReconciliationStatus.FOUND
                or reconciled.snapshot is None
            ):
                raise RuntimeError(
                    f"Could not verify {command.role} order before manual completion",
                )
            current = self.state.orders.get(client_order_id)
            if (
                current is not None
                and reconciled.snapshot.filled_quantity.value
                < current.filled_quantity.value
            ):
                raise RuntimeError(
                    f"{command.role} reconciliation regressed its filled quantity",
                )
            if reconciled.snapshot != current:
                await self.pipeline.event_loop.process(
                    OrderSnapshotUpdated(
                        execution_id,
                        command.role,
                        prepared.reference,
                        reconciled.snapshot,
                        "get",
                    ),
                    enqueue_commands=False,
                )
        await self._resolve_predict_rounding_review(execution_id)

    async def _resolve_predict_rounding_review(self, execution_id: str) -> None:
        """Close a verified single-attempt Predict review while retaining signing dust.

        Notes
        -----
        - Only explicit reconciliation with trading disabled can use this path.
        - All orders must be settled, the recovery must have a finalized chain
          proof, and the missing quantity must exactly equal the SDK's truncation.
        - Actual fills, fees and positions remain unchanged. Residual dust stays
          visible, so terminal neutral-exposure PnL remains unavailable.
        - The original safety stop remains an audit record and trading stays off.
        """
        execution = self.state.executions.get(execution_id)
        recovery = self.state.recoveries.get(execution_id)
        stop = self.state.execution_safety_stops.get(execution_id)
        if (
            self.state.trading_enabled
            or execution is None or recovery is None or stop is None
            or execution.status is not ArbitrageExecutionStatus.NEEDS_REVIEW
            or recovery.status is not RecoveryStatus.NEEDS_REVIEW
            or execution.resolution_method is not None
            or str(recovery.venue_id) != "PREDICT"
            or recovery.attempts != 1
            or stop.execution_id != execution_id
            or stop.venue_id != recovery.venue_id
            or stop.client_order_id != recovery.client_order_id
            or not stop.reason.startswith("Cancellation could not prove the order terminal:")
        ):
            return
        commands = tuple(
            command for command in self.state.commands.values()
            if command.execution_id == execution_id
        )
        recoveries = tuple(command for command in commands if command.role == "recovery")
        if len(recoveries) != 1:
            return
        command = recoveries[0]
        client_id = recovery.client_order_id
        if (
            command.intent.client_order_id != client_id
            or command.venue_id != recovery.venue_id
            or command.intent.contract_id != recovery.contract_id
            or command.intent.side is not recovery.side
            or command.intent.quantity != recovery.quantity
            or {value.intent.client_order_id for value in commands}
            != {execution.leg1_client_order_id, execution.leg2_client_order_id, client_id}
        ):
            return
        for value in commands:
            current = self.state.orders.get(value.intent.client_order_id)
            if (
                value.intent.client_order_id not in self.state.prepared
                or current is None or not is_settled_order(value, current)
            ):
                return
        snapshot = self.state.orders[client_id]
        requested = recovery.quantity.value
        step = Decimal(1).scaleb(requested.adjusted() - 4)
        signed = requested.quantize(step, rounding=ROUND_DOWN)
        dust = requested - signed
        if (
            not Decimal("0") < dust < step
            or snapshot.may_receive_more_fills is not False
            or snapshot.settlement_finalized_block is None
            or snapshot.order_id is None or snapshot.order_id != recovery.order_id
            or snapshot.client_order_id != client_id
            or snapshot.contract_id != recovery.contract_id or snapshot.side is not recovery.side
            or snapshot.quantity.value != signed or snapshot.filled_quantity.value != signed
            or recovery.filled_quantity != snapshot.filled_quantity
            or execution.residual_quantity.value != dust
            or abs(execution.leg1_filled_quantity.value - execution.leg2_filled_quantity.value) != requested
            or snapshot.fee is None or recovery.recovery_fee != snapshot.fee.settlement_cost
        ):
            return
        fills = tuple(trade for trade in self.state.trades.values() if trade.client_order_id == client_id)
        if (
            sum((trade.quantity.value for trade in fills), Decimal("0")) != signed
            or any(trade.fee_settlement_cost is None for trade in fills)
            or sum((trade.fee_settlement_cost.amount for trade in fills), Decimal("0"))
            != snapshot.fee.settlement_cost.amount
        ):
            return
        note = f"Finalized Predict recovery reconciled; retained signing dust: {dust} shares"
        now = Timestamp.now()
        for event in (
            RecoveryUpdated(replace(recovery, status=RecoveryStatus.RESOLVED, last_error=note, updated_at=now)),
            ExecutionUpdated(replace(execution, status=ArbitrageExecutionStatus.RECOVERED, last_error=note, updated_at=now)),
        ):
            await self.pipeline.event_loop.process(event, enqueue_commands=False)


    def record_cash_movement(self, movement: CashMovement) -> None:
        """Persist and apply one externally observed cash movement idempotently."""
        if self.journal is None:
            raise RuntimeError("Arbitrage journal is not initialized")
        event = CashMovementRecorded(movement)
        self.journal.append(event)
        if self.dispatcher is not None:
            self.dispatcher.dispatch(event)

    def record_accounting_correction(
        self,
        replacement: Trade,
        reason: str,
        *,
        correction_id: str | None = None,
    ) -> AccountingCorrection:
        """Replace one trade and rebuild its position from durable economics."""
        if self.journal is None or self.state is None:
            raise RuntimeError("Arbitrage journal is not initialized")
        original = self.state.trades.get(replacement.id)
        if original is None:
            raise KeyError(str(replacement.id))
        if (
            original.venue_id,
            original.portfolio_id,
            original.contract_id,
        ) != (
            replacement.venue_id,
            replacement.portfolio_id,
            replacement.contract_id,
        ):
            raise ValueError("Accounting corrections cannot move trades")
        position = rebuild_corrected_position(
            tuple(record.event for record in self._recovery_records()),
            replacement,
        )
        current = self.state.positions.get(position.id)
        if current is not None and current.current_price is not None:
            position = replace(
                position,
                current_price=current.current_price,
                updated_at=max(
                    value
                    for value in (position.updated_at, current.updated_at)
                    if value is not None
                ),
            )
        correction = AccountingCorrection(
            id=correction_id or f"correction:{replacement.id}:{uuid4().hex}",
            target_trade_id=replacement.id,
            original_trade=original,
            replacement_trade=replacement,
            resulting_position=position,
            reason=reason,
            recorded_at=Timestamp.now(),
        )
        event = AccountingCorrectionRecorded(correction)
        self.journal.append(event)
        if self.dispatcher is not None:
            self.dispatcher.dispatch(event)
        return correction
