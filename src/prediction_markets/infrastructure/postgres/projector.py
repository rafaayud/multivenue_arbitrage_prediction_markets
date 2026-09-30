"""Project durable journal events into PostgreSQL read models on a separate thread.

Responsibilities
----------------
- Read only through the journal's completed durability cursor.
- Materialize matches, opportunities, commands, orders, fills, positions, and executions.
- Advance the SQL checkpoint in the same transaction as each projected batch.
- Keep signed prepared-order payloads out of PostgreSQL.
"""

import json
import threading
import time
from collections.abc import Callable, Iterable
from dataclasses import replace
from decimal import Decimal
from itertools import islice
from pathlib import Path
from typing import Any

import psycopg

from prediction_markets.application.alerting.policy import (
    AcknowledgeIncident,
    ChangeIncidentSeverity,
    MarkIncidentInProgress,
    NotifyIncident,
    OpenIncident,
    ResolveIncident,
    directives_for,
)
from prediction_markets.application.alerting.service import (
    AlertingApplicationService,
)
from prediction_markets.application.codec import event_kind
from prediction_markets.application.events import (
    AccountingCorrectionRecorded,
    ArbitrageOpportunityFound,
    ArbitragePlanned,
    CashMovementRecorded,
    ExecutionUpdated,
    InventoryOperationRecorded,
    MarketSettlementRecorded,
    MarketMatchesUpdated,
    OrderPrepared,
    OrderBookUpdated,
    OrderSnapshotUpdated,
    PreparedExecutionBatch,
    PositionUpdated,
    RecoveryPlanned,
    RecoveryUpdated,
    SubmissionReceived,
    SubmitOrder,
    TradeRecorded,
    TradingSafetyStop,
    prepared_execution_events,
)
from prediction_markets.application.markets.models import (
    MarketCycle,
    MonitoredMarket,
    monitored_market_key,
)
from prediction_markets.domain.alerting.entities import NotificationSnapshot
from prediction_markets.domain.alerting.enums import NotificationState
from prediction_markets.domain.alerting.ports import AlertingPort
from prediction_markets.domain.alerting.service import NotificationPolicy
from prediction_markets.domain.alerting.value_objects import Recipient
from prediction_markets.domain.trading.entities import (
    AccountingCorrection,
    CashMovement,
    OrderSnapshot,
    Position,
    Trade,
)
from prediction_markets.domain.trading.entities import ArbitrageExecutionJournal
from prediction_markets.domain.trading.portfolio import STRATEGY_PORTFOLIO_ID
from prediction_markets.domain.trading.value_objects import OrderBookDecisionSnapshot
from prediction_markets.domain.trading.entities import ExposureRecovery
from prediction_markets.infrastructure.binary_journal import BinaryJournal, JournalEntry
from prediction_markets.infrastructure.postgres.repositories import (
    PostgresIncidentRepository,
    PostgresNotificationDeliveryRepository,
)

_PROJECTOR_NAME = "trading_read_model_v1"


def apply_migrations(dsn: str, directory: str | Path = "migrations") -> None:
    """Apply the repository's idempotent SQL migrations before projection starts.

    Parameters
    ----------
    dsn
        PostgreSQL connection string.
    directory : str | Path, default="migrations"
        Directory containing lexically ordered ``.sql`` migration files.

    Raises
    ------
    FileNotFoundError
        If the configured migration directory is unavailable.
    """
    path = Path(directory)
    if not path.is_dir():
        raise FileNotFoundError(f"Migration directory does not exist: {path}")
    with psycopg.connect(dsn, connect_timeout=5, autocommit=True) as connection:
        for migration in sorted(path.glob("*.sql")):
            connection.execute(migration.read_text(encoding="utf-8"))


class PostgresProjector:
    """Maintain PostgreSQL read models behind the journal durability cursor."""

    def __init__(
        self,
        dsn: str,
        journal: BinaryJournal,
        *,
        batch_size: int = 256,
        retry_seconds: float = 1.0,
        recipients: Iterable[Recipient] = (),
        connect: Callable[..., Any] = psycopg.connect,
    ) -> None:
        """
        Parameters
        ----------
        dsn
            PostgreSQL connection string used only by the projector thread.
        journal
            Source of validated entries and the durability watermark.
        batch_size : int, default=256
            Maximum records committed in one SQL transaction.
        retry_seconds : float, default=1.0
            Delay after a database failure.
        recipients
            Logical destinations eligible for notification routing.
        connect
            Injectable connection factory used by tests.
        """
        if batch_size <= 0 or retry_seconds <= 0:
            raise ValueError("Projector batch and retry settings must be positive")
        self._dsn = dsn
        self._journal = journal
        self._batch_size = batch_size
        self._retry_seconds = retry_seconds
        self._notification_policy = NotificationPolicy(recipients)
        self._connect = connect
        self._stop = threading.Event()
        self._condition = threading.Condition()
        self._thread: threading.Thread | None = None
        self._projected_sequence = 0
        self._error: BaseException | None = None

    @property
    def projected_sequence(self) -> int:
        """Return the last sequence committed by this process."""
        with self._condition:
            return self._projected_sequence

    @property
    def error(self) -> BaseException | None:
        """Return the latest projector failure, cleared after a successful batch."""
        with self._condition:
            return self._error

    def start(self) -> None:
        """Start the sole projection thread idempotently."""
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._run,
            name="postgres-journal-projector",
            daemon=True,
        )
        self._thread.start()

    def validate_checkpoint(self) -> None:
        """Reject a PostgreSQL cursor incompatible with the current journal.

        Raises
        ------
        RuntimeError
            If PostgreSQL references unavailable journal history.
        """
        with self._connect(self._dsn, connect_timeout=5) as connection:
            _validate_checkpoint(_load_checkpoint(connection), self._journal)

    def drain(self, through_sequence: int, timeout: float = 5.0) -> bool:
        """Wait until SQL reaches a requested durable sequence.

        Returns
        -------
        bool
            ``True`` when the checkpoint reached the target before timeout.
        """
        deadline = time.monotonic() + timeout
        with self._condition:
            while self._projected_sequence < through_sequence:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                self._condition.wait(remaining)
            return True

    def close(self) -> None:
        """Stop and join the projection thread."""
        self._stop.set()
        thread = self._thread
        if thread is not None:
            thread.join()
        self._thread = None

    def _run(self) -> None:
        checkpoint = 0
        while not self._stop.is_set():
            try:
                with self._connect(self._dsn, connect_timeout=5) as connection:
                    checkpoint = _load_checkpoint(connection)
                    _validate_checkpoint(checkpoint, self._journal)
                    self._advance(checkpoint)
                    while not self._stop.is_set():
                        durable = self._journal.wait_for_durable(checkpoint, 0.25)
                        entries = tuple(
                            islice(
                                self._journal.iter_entries(
                                    after_sequence=checkpoint,
                                    through_sequence=durable,
                                ),
                                self._batch_size,
                            ),
                        )
                        if not entries:
                            continue
                        _project_batch(
                            connection,
                            entries,
                            self._notification_policy,
                        )
                        checkpoint = entries[-1].sequence
                        self._advance(checkpoint)
            except BaseException as error:
                with self._condition:
                    self._error = error
                    self._condition.notify_all()
                self._stop.wait(self._retry_seconds)

    def _advance(self, sequence: int) -> None:
        with self._condition:
            self._projected_sequence = sequence
            self._error = None
            self._condition.notify_all()


def _load_checkpoint(connection: Any) -> int:
    row = connection.execute(
        "SELECT last_sequence FROM journal_projection_checkpoints "
        "WHERE projector_name = %s",
        (_PROJECTOR_NAME,),
    ).fetchone()
    return int(row[0]) if row else 0


def _validate_checkpoint(checkpoint: int, journal: BinaryJournal) -> None:
    """Ensure a SQL checkpoint can continue from retained journal history."""
    if checkpoint > journal.last_sequence:
        raise RuntimeError(
            "PostgreSQL projection checkpoint is ahead of the journal",
        )
    if checkpoint < journal.retained_through_sequence:
        raise RuntimeError(
            "PostgreSQL projection checkpoint is behind retained journal history",
        )


def _project_batch(
    connection: Any,
    entries: tuple[JournalEntry, ...],
    notification_policy: NotificationPolicy | None = None,
) -> None:
    cursor = connection.cursor()
    alerting: AlertingPort | None = None
    if notification_policy is not None:
        alerting = AlertingApplicationService(
            PostgresIncidentRepository(
                connection,
                manage_transactions=False,
            ),
            PostgresNotificationDeliveryRepository(
                connection,
                manage_transactions=False,
            ),
            notification_policy,
        )
    try:
        for entry in entries:
            _project_entry(cursor, entry, alerting)
        cursor.execute(
            """
            INSERT INTO journal_projection_checkpoints (
                projector_name, last_sequence, updated_at
            ) VALUES (%s, %s, NOW())
            ON CONFLICT (projector_name) DO UPDATE SET
                last_sequence = EXCLUDED.last_sequence,
                updated_at = EXCLUDED.updated_at
            """,
            (_PROJECTOR_NAME, entries[-1].sequence),
        )
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        cursor.close()


def _project_entry(
    cursor: Any,
    entry: JournalEntry,
    alerting: AlertingPort | None = None,
    *,
    record_frame: bool = True,
) -> None:
    event = entry.event
    if record_frame:
        cursor.execute(
            """
            INSERT INTO projected_events (
                journal_sequence, event_type, recorded_at, correlation_id, summary
            ) VALUES (%s, %s, %s, %s, %s::jsonb)
            ON CONFLICT (journal_sequence) DO NOTHING
            """,
            (
                entry.sequence,
                event_kind(event),
                entry.recorded_at.value,
                _correlation_id(event),
                json.dumps(_summary(event), separators=(",", ":")),
            ),
        )
    if isinstance(event, PreparedExecutionBatch):
        for nested in prepared_execution_events(event):
            _project_entry(
                cursor,
                replace(entry, event=nested),
                alerting,
                record_frame=False,
            )
        return
    if isinstance(event, MarketMatchesUpdated):
        _project_matches(cursor, entry.sequence, event)
    elif isinstance(event, OrderBookUpdated):
        mark = event.order_book.mid_price()
        if mark is not None:
            cursor.execute(
                "UPDATE positions SET current_price = %s, updated_at = %s "
                "WHERE venue_id = %s AND contract_id = %s AND side != 'flat' "
                "AND current_price IS DISTINCT FROM %s",
                (
                    mark.value,
                    (
                        event.order_book.timestamp.value
                        if event.order_book.timestamp is not None
                        else entry.recorded_at.value
                    ),
                    str(event.venue_id),
                    str(event.contract_id),
                    mark.value,
                ),
            )
            if cursor.rowcount:
                _project_bot_pnl(
                    cursor,
                    entry,
                    point_id=(
                        "bot_ledger:mark:"
                        f"{int(entry.recorded_at.value.timestamp() // 60)}"
                    ),
                )
    elif isinstance(event, ArbitrageOpportunityFound):
        _project_opportunity(cursor, entry.sequence, event)
    elif isinstance(event, SubmitOrder):
        _project_command(cursor, entry.sequence, entry.recorded_at.value, event)
    elif isinstance(event, OrderPrepared):
        cursor.execute(
            """
            UPDATE execution_order_commands
            SET status = 'prepared', prepared_sequence = %s, updated_at = %s
            WHERE client_order_id = %s
            """,
            (
                entry.sequence,
                entry.recorded_at.value,
                str(event.prepared.reference.client_order_id),
            ),
        )
    elif isinstance(event, SubmissionReceived):
        status = (
            event.result.snapshot.status.value
            if event.result.snapshot is not None
            else event.result.status.value
        )
        cursor.execute(
            """
            UPDATE execution_order_commands
            SET status = %s, submitted_sequence = %s, updated_at = %s
            WHERE client_order_id = %s
            """,
            (
                status,
                entry.sequence,
                entry.recorded_at.value,
                str(event.result.reference.client_order_id),
            ),
        )
        if event.result.snapshot is not None:
            _project_order(cursor, event.result.snapshot)
    elif isinstance(event, OrderSnapshotUpdated):
        cursor.execute(
            "UPDATE execution_order_commands SET status = %s, updated_at = %s "
            "WHERE client_order_id = %s",
            (
                event.snapshot.status.value,
                entry.recorded_at.value,
                str(event.reference.client_order_id),
            ),
        )
        _project_order(cursor, event.snapshot)
    elif isinstance(event, TradeRecorded):
        _project_trade(cursor, event.trade, journal_sequence=entry.sequence)
    elif isinstance(event, PositionUpdated):
        _project_position(cursor, event.position)
        _project_bot_pnl(cursor, entry)
    elif isinstance(event, CashMovementRecorded):
        _project_cash_movement(cursor, event.movement)
        _project_bot_pnl(cursor, entry)
    elif isinstance(event, AccountingCorrectionRecorded):
        _project_accounting_correction(cursor, event.correction)
        _project_trade(
            cursor,
            event.correction.replacement_trade,
            journal_sequence=(
                event.correction.replacement_trade.journal_sequence
                or entry.sequence
            ),
        )
        _project_position(cursor, event.correction.resulting_position)
        _project_bot_pnl(cursor, entry)
    elif isinstance(event, ArbitragePlanned):
        _project_execution(cursor, event.execution)
    elif isinstance(event, ExecutionUpdated):
        _project_execution(cursor, event.execution)
        if event.latency_trace_json is not None:
            cursor.execute(
                "UPDATE arbitrage_execution_journals "
                "SET latency_trace = %s::jsonb WHERE execution_id = %s",
                (event.latency_trace_json, event.execution.id),
            )
        if event.execution.status.value in {"completed", "recovered"}:
            _project_bot_pnl(cursor, entry)
    elif isinstance(event, (RecoveryPlanned, RecoveryUpdated)):
        _project_recovery(cursor, event.recovery)
        if event.recovery.status.value == "resolved":
            _project_bot_pnl(cursor, entry)
    if alerting is not None:
        _project_alerting(entry, alerting)


def _project_alerting(
    entry: JournalEntry,
    alerting: AlertingPort,
) -> None:
    """Dispatch journal-derived directives through the inbound alerting port."""
    for directive in directives_for(entry.event, recorded_at=entry.recorded_at):
        if isinstance(directive, OpenIncident):
            alerting.report_incident(
                directive.incident_id,
                directive.fingerprint,
                directive.severity,
                directive.source,
                directive.description,
                directive.opened_at,
                directive.title,
            )
        elif isinstance(directive, AcknowledgeIncident):
            alerting.acknowledge_incident(
                directive.incident_id,
                directive.acknowledged_at,
            )
        elif isinstance(directive, MarkIncidentInProgress):
            alerting.mark_incident_in_progress(
                directive.incident_id,
                directive.in_progress_at,
            )
        elif isinstance(directive, ResolveIncident):
            alerting.resolve_incident(
                directive.incident_id,
                directive.resolved_at,
            )
        elif isinstance(directive, ChangeIncidentSeverity):
            alerting.change_incident_severity(
                directive.incident_id,
                directive.severity,
                directive.changed_at,
            )
        elif isinstance(directive, NotifyIncident):
            alerting.request_notifications(
                None,
                NotificationSnapshot(
                    fingerprint=directive.fingerprint,
                    state=NotificationState.FIRING,
                    severity=directive.severity,
                    source=directive.source,
                    description=directive.description,
                    starts_at=directive.notified_at,
                    ends_at=directive.notified_at,
                    title=directive.title,
                ),
                None,
                directive.notified_at,
            )
        else:
            raise TypeError(
                f"Unsupported alerting directive: {type(directive).__name__}",
            )


def _project_matches(cursor: Any, sequence: int, event: MarketMatchesUpdated) -> None:
    monitor_type, monitor_key, underlying, interval_seconds = _monitor_fields(
        event.cycle,
    )
    cursor.execute(
        "DELETE FROM matched_contracts WHERE monitor_key = %s",
        (monitor_key,),
    )
    for pair in event.pairs:
        cursor.execute(
            """
            INSERT INTO matched_contracts (
                monitor_type, monitor_key, underlying, interval_seconds,
                left_contract_id, left_market_id, left_outcome_id,
                left_venue_id, left_symbol,
                right_contract_id, right_market_id, right_outcome_id,
                right_venue_id, right_symbol, ends_at, updated_sequence
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s
            )
            """,
            (
                monitor_type,
                monitor_key,
                underlying,
                interval_seconds,
                str(pair.left.id),
                str(pair.left.market_id),
                str(pair.left.outcome_id),
                str(pair.left.venue_id),
                pair.left.symbol,
                str(pair.right.id),
                str(pair.right.market_id),
                str(pair.right.outcome_id),
                str(pair.right.venue_id),
                pair.right.symbol,
                pair.ends_at.value,
                sequence,
            ),
        )
    contracts_by_market: dict[tuple[str, str], set[str]] = {}
    for pair in event.pairs:
        for contract in (pair.left, pair.right):
            key = str(contract.venue_id), str(contract.market_id)
            contracts_by_market.setdefault(key, set()).add(str(contract.id))
    for (venue_id, market_id), contract_ids in contracts_by_market.items():
        if len(contract_ids) != 2:
            continue
        first, second = sorted(contract_ids)
        for contract_id, complement_id in ((first, second), (second, first)):
            cursor.execute(
                """
                INSERT INTO binary_contract_complements (
                    venue_id, market_id, contract_id, complement_contract_id
                ) VALUES (%s, %s, %s, %s)
                ON CONFLICT (venue_id, contract_id) DO UPDATE SET
                    market_id = EXCLUDED.market_id,
                    complement_contract_id = EXCLUDED.complement_contract_id
                """,
                (venue_id, market_id, contract_id, complement_id),
            )


def _project_opportunity(
    cursor: Any,
    sequence: int,
    event: ArbitrageOpportunityFound,
) -> None:
    value = event.opportunity
    monitor_type, monitor_key, underlying, interval_seconds = _monitor_fields(
        event.cycle,
    )
    cursor.execute(
        """
        INSERT INTO arbitrage_opportunities (
            opportunity_id, journal_sequence, monitor_type, monitor_key,
            underlying, interval_seconds, side, left_contract_id,
            right_contract_id, left_price, right_price, quantity, gross_edge,
            net_edge, fee_per_contract, total_fees, skew_ns, detected_at
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT (opportunity_id) DO NOTHING
        """,
        (
            event.id,
            sequence,
            monitor_type,
            monitor_key,
            underlying,
            interval_seconds,
            value.side.value,
            str(value.left_contract_id),
            str(value.right_contract_id),
            value.left_level.price.value,
            value.right_level.price.value,
            value.quantity.value,
            value.gross_edge,
            value.net_edge,
            value.fee_per_contract,
            value.total_fees,
            value.skew_ns,
            value.detected_at.value,
        ),
    )


def _project_command(
    cursor: Any,
    sequence: int,
    recorded_at: object,
    event: SubmitOrder,
) -> None:
    intent = event.intent
    cursor.execute(
        """
        INSERT INTO execution_order_commands (
            client_order_id, execution_id, role, venue_id, contract_id, side,
            quantity, limit_price, status, command_sequence, updated_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'commanded', %s, %s)
        ON CONFLICT (client_order_id) DO NOTHING
        """,
        (
            str(intent.client_order_id),
            event.execution_id,
            event.role,
            str(event.venue_id),
            str(intent.contract_id),
            intent.side.value,
            intent.quantity.value,
            intent.limit_price.value if intent.limit_price else None,
            sequence,
            intent.created_at.value if intent.created_at else recorded_at,
        ),
    )


def _project_order(cursor: Any, order: OrderSnapshot) -> None:
    key = order.client_order_id or order.order_id
    cursor.execute(
        """
        INSERT INTO orders (
            order_key, client_order_id, order_id, contract_id, status, side,
            quantity, order_type, limit_price, filled_quantity, average_price,
            created_at, updated_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (order_key) DO UPDATE SET
            order_id = EXCLUDED.order_id,
            status = EXCLUDED.status,
            filled_quantity = EXCLUDED.filled_quantity,
            average_price = EXCLUDED.average_price,
            updated_at = EXCLUDED.updated_at
        """,
        (
            str(key),
            str(order.client_order_id) if order.client_order_id else None,
            str(order.order_id) if order.order_id else None,
            str(order.contract_id),
            order.status.value,
            order.side.value,
            order.quantity.value,
            order.order_type.value,
            order.limit_price.value if order.limit_price else None,
            order.filled_quantity.value,
            order.average_price.value if order.average_price else None,
            order.created_at.value if order.created_at else None,
            order.updated_at.value if order.updated_at else None,
        ),
    )


def _project_trade(
    cursor: Any,
    trade: Trade,
    *,
    journal_sequence: int | None = None,
) -> None:
    """Project one fill and retain its durable replay sequence.

    Parameters
    ----------
    cursor
        PostgreSQL-compatible cursor used by the current projection batch.
    trade
        Fill to insert or update with late fee information.
    journal_sequence : int, optional
        Sequence assigned by the append-only journal. Direct repository
        backfills may omit it and remain ordered after journaled fills.
    """
    journal_sequence = journal_sequence or trade.journal_sequence
    cursor.execute(
        """
        INSERT INTO trades (
            trade_id, order_id, client_order_id, contract_id, venue_id, side,
            quantity, price, executed_at, portfolio_id, strategy_id, journal_sequence,
            fee_amount, fee_currency,
            fee_settlement_amount, fee_settlement_currency
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (trade_id) DO UPDATE SET
            venue_id = COALESCE(trades.venue_id, EXCLUDED.venue_id),
            side = EXCLUDED.side,
            quantity = EXCLUDED.quantity,
            price = EXCLUDED.price,
            executed_at = EXCLUDED.executed_at,
            fee_amount = EXCLUDED.fee_amount,
            fee_currency = EXCLUDED.fee_currency,
            fee_settlement_amount = EXCLUDED.fee_settlement_amount,
            fee_settlement_currency = EXCLUDED.fee_settlement_currency,
            journal_sequence = COALESCE(
                trades.journal_sequence,
                EXCLUDED.journal_sequence
            )
        """,
        (
            str(trade.id),
            str(trade.order_id) if trade.order_id else None,
            str(trade.client_order_id) if trade.client_order_id else None,
            str(trade.contract_id),
            str(trade.venue_id),
            trade.side.value,
            trade.quantity.value,
            trade.price.value,
            trade.executed_at.value,
            str(trade.portfolio_id) if trade.portfolio_id else None,
            str(trade.strategy_id) if trade.strategy_id else None,
            journal_sequence,
            trade.fee.amount if trade.fee else None,
            str(trade.fee.currency) if trade.fee else None,
            (
                trade.fee_settlement_cost.amount
                if trade.fee_settlement_cost
                else None
            ),
            (
                str(trade.fee_settlement_cost.currency)
                if trade.fee_settlement_cost
                else None
            ),
        ),
    )


def _project_position(cursor: Any, position: Position) -> None:
    cursor.execute(
        """
        INSERT INTO positions (
            position_id, contract_id, venue_id, side, quantity, average_entry_price,
            current_price, realized_pnl, fee_settlement_amount,
            fee_settlement_currency, quality_flags, portfolio_id, opened_at, updated_at
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s, %s
        )
        ON CONFLICT (position_id) DO UPDATE SET
            venue_id = EXCLUDED.venue_id,
            side = EXCLUDED.side,
            quantity = EXCLUDED.quantity,
            average_entry_price = EXCLUDED.average_entry_price,
            current_price = CASE
                WHEN EXCLUDED.side = 'flat' THEN NULL
                WHEN EXCLUDED.current_price IS NOT NULL THEN EXCLUDED.current_price
                WHEN EXCLUDED.side = positions.side THEN positions.current_price
                ELSE NULL
            END,
            realized_pnl = EXCLUDED.realized_pnl,
            fee_settlement_amount = EXCLUDED.fee_settlement_amount,
            fee_settlement_currency = EXCLUDED.fee_settlement_currency,
            quality_flags = EXCLUDED.quality_flags,
            updated_at = EXCLUDED.updated_at
        """,
        (
            str(position.id),
            str(position.contract_id),
            str(position.venue_id),
            position.side.value,
            position.quantity.value,
            position.average_price.value if position.average_price else None,
            position.current_price.value if position.current_price else None,
            position.realized_pnl,
            position.fees.amount if position.fees else None,
            str(position.fees.currency) if position.fees else None,
            json.dumps(position.quality_flags, separators=(",", ":")),
            str(position.portfolio_id) if position.portfolio_id else None,
            position.opened_at.value if position.opened_at else None,
            position.updated_at.value if position.updated_at else None,
        ),
    )


def _project_cash_movement(cursor: Any, movement: CashMovement) -> None:
    """Project one idempotent deposit, withdrawal, or transfer."""
    cursor.execute(
        """
        INSERT INTO cash_movements (
            movement_id, kind, amount, currency, occurred_at,
            source_venue_id, source_portfolio_id,
            destination_venue_id, destination_portfolio_id,
            fee_amount, fee_currency, external_reference
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (movement_id) DO UPDATE SET
            kind = EXCLUDED.kind,
            amount = EXCLUDED.amount,
            currency = EXCLUDED.currency,
            occurred_at = EXCLUDED.occurred_at,
            source_venue_id = EXCLUDED.source_venue_id,
            source_portfolio_id = EXCLUDED.source_portfolio_id,
            destination_venue_id = EXCLUDED.destination_venue_id,
            destination_portfolio_id = EXCLUDED.destination_portfolio_id,
            fee_amount = EXCLUDED.fee_amount,
            fee_currency = EXCLUDED.fee_currency,
            external_reference = EXCLUDED.external_reference
        """,
        (
            movement.id,
            movement.kind.value,
            movement.amount.amount,
            str(movement.amount.currency),
            movement.occurred_at.value,
            str(movement.source_venue_id) if movement.source_venue_id else None,
            str(movement.source_portfolio_id) if movement.source_portfolio_id else None,
            (
                str(movement.destination_venue_id)
                if movement.destination_venue_id
                else None
            ),
            (
                str(movement.destination_portfolio_id)
                if movement.destination_portfolio_id
                else None
            ),
            movement.fee.amount if movement.fee else None,
            str(movement.fee.currency) if movement.fee else None,
            movement.external_reference,
        ),
    )


def _project_accounting_correction(
    cursor: Any,
    correction: AccountingCorrection,
) -> None:
    """Store the immutable before/after values for one ledger correction."""
    cursor.execute(
        """
        INSERT INTO accounting_corrections (
            correction_id, target_trade_id, reason, recorded_at,
            original_trade, replacement_trade, resulting_position_id
        ) VALUES (%s, %s, %s, %s, %s::jsonb, %s::jsonb, %s)
        ON CONFLICT (correction_id) DO NOTHING
        """,
        (
            correction.id,
            str(correction.target_trade_id),
            correction.reason,
            correction.recorded_at.value,
            json.dumps(_trade_audit(correction.original_trade), separators=(",", ":")),
            json.dumps(
                _trade_audit(correction.replacement_trade),
                separators=(",", ":"),
            ),
            str(correction.resulting_position.id),
        ),
    )


def _trade_audit(trade: Trade) -> dict[str, object]:
    """Return the economic trade fields needed to audit a replacement."""
    return {
        "trade_id": str(trade.id),
        "venue_id": str(trade.venue_id),
        "portfolio_id": str(trade.portfolio_id) if trade.portfolio_id else None,
        "contract_id": str(trade.contract_id),
        "side": trade.side.value,
        "quantity": str(trade.quantity.value),
        "price": str(trade.price.value),
        "executed_at": trade.executed_at.value.isoformat(),
        "fee_amount": str(trade.fee.amount) if trade.fee else None,
        "fee_currency": str(trade.fee.currency) if trade.fee else None,
        "fee_settlement_amount": (
            str(trade.fee_settlement_cost.amount)
            if trade.fee_settlement_cost
            else None
        ),
        "fee_settlement_currency": (
            str(trade.fee_settlement_cost.currency)
            if trade.fee_settlement_cost
            else None
        ),
    }


def _project_bot_pnl(
    cursor: Any,
    entry: JournalEntry,
    *,
    point_id: str | None = None,
) -> None:
    """Persist one global bot-ledger point after a durable economic change.

    Notes
    -----
    - Unrealized and fees sum only known USD components so the series stays
      plottable while ``partial`` / quality flags expose missing marks or fees.
    """
    position_row = cursor.execute(
        """
        SELECT
            COALESCE(SUM(realized_pnl), 0),
            COALESCE(SUM(
                CASE
                    WHEN side = 'long'
                         AND current_price IS NOT NULL
                         AND average_entry_price IS NOT NULL
                    THEN quantity * (current_price - average_entry_price)
                    WHEN side = 'short'
                         AND current_price IS NOT NULL
                         AND average_entry_price IS NOT NULL
                    THEN quantity * (average_entry_price - current_price)
                    ELSE 0
                END
            ), 0),
            COALESCE(SUM(
                CASE
                    WHEN fee_settlement_amount IS NOT NULL
                         AND fee_settlement_currency = 'USD'
                    THEN fee_settlement_amount
                    ELSE 0
                END
            ), 0),
            COALESCE((
                SELECT SUM(trade.fee_settlement_amount)
                FROM trades AS trade
                WHERE trade.portfolio_id = %s
                  AND trade.fee_settlement_currency = 'USD'
            ), 0),
            COALESCE(BOOL_OR(side != 'flat' AND current_price IS NULL), FALSE),
            COALESCE(BOOL_OR(quality_flags ? 'MISSING_FEES'), FALSE),
            COALESCE(BOOL_OR(
                quality_flags ? 'INCONSISTENT_FEE_CURRENCY'
                OR (
                    fee_settlement_amount IS NOT NULL
                    AND fee_settlement_currency IS DISTINCT FROM 'USD'
                )
            ), FALSE)
        FROM positions
        WHERE portfolio_id = %s
        """,
        (str(STRATEGY_PORTFOLIO_ID), str(STRATEGY_PORTFOLIO_ID)),
    ).fetchone()
    locked_adjustment = cursor.execute(
        """
        WITH strategy_scope AS (
            SELECT %s::text AS portfolio_id
        ), local_pairs AS (
            SELECT
                position.portfolio_id,
                position.venue_id,
                position.contract_id,
                complement.contract_id AS complement_contract_id,
                LEAST(position.quantity, complement.quantity) AS locked_quantity,
                position.average_entry_price AS leg1_entry_price,
                position.current_price AS leg1_current_price,
                complement.average_entry_price AS leg2_entry_price,
                complement.current_price AS leg2_current_price
            FROM positions AS position
            JOIN strategy_scope AS scope
              ON scope.portfolio_id = position.portfolio_id
            JOIN binary_contract_complements AS mapping
              ON mapping.venue_id = position.venue_id
             AND mapping.contract_id = position.contract_id
            JOIN positions AS complement
              ON complement.portfolio_id = position.portfolio_id
             AND complement.venue_id = position.venue_id
             AND complement.contract_id = mapping.complement_contract_id
             AND complement.side = 'long'
            WHERE position.side = 'long'
              AND position.contract_id < complement.contract_id
        ), local_locked AS (
            SELECT
                portfolio_id,
                venue_id,
                contract_id,
                locked_quantity
            FROM local_pairs
            UNION ALL
            SELECT
                portfolio_id,
                venue_id,
                complement_contract_id,
                locked_quantity
            FROM local_pairs
        ), position_capacity AS (
            SELECT
                position.*,
                GREATEST(
                    position.quantity - COALESCE(local.locked_quantity, 0),
                    0
                ) AS remaining_quantity
            FROM positions AS position
            JOIN strategy_scope AS scope
              ON scope.portfolio_id = position.portfolio_id
            LEFT JOIN local_locked AS local
              ON local.portfolio_id = position.portfolio_id
             AND local.venue_id = position.venue_id
             AND local.contract_id = position.contract_id
        ), terminal_executions AS (
            SELECT
                execution.portfolio_id,
                execution.leg1_venue_id,
                execution.leg1_contract_id,
                execution.leg1_side,
                execution.leg2_venue_id,
                execution.leg2_contract_id,
                execution.leg2_side,
                CASE
                    WHEN execution.status = 'completed'
                    THEN LEAST(
                        execution.leg1_filled_quantity,
                        execution.leg2_filled_quantity
                    )
                    WHEN execution.status = 'recovered'
                         AND recovery.status = 'resolved'
                         AND recovery.route = 'complete_missing_leg'
                    THEN GREATEST(
                        execution.leg1_filled_quantity,
                        execution.leg2_filled_quantity
                    )
                    ELSE 0
                END AS paired_quantity
            FROM arbitrage_execution_journals AS execution
            LEFT JOIN exposure_recoveries AS recovery
                ON recovery.execution_id = execution.execution_id
            JOIN strategy_scope AS scope
              ON scope.portfolio_id = execution.portfolio_id
        ), terminal_pairs AS (
            SELECT
                portfolio_id,
                leg1_venue_id,
                leg1_contract_id,
                leg2_venue_id,
                leg2_contract_id,
                paired_quantity
            FROM terminal_executions
            WHERE leg1_side = 'buy' AND leg2_side = 'buy'
            UNION ALL
            SELECT
                execution.portfolio_id,
                execution.leg1_venue_id,
                leg1.complement_contract_id,
                execution.leg2_venue_id,
                leg2.complement_contract_id,
                execution.paired_quantity
            FROM terminal_executions AS execution
            JOIN binary_contract_complements AS leg1
              ON leg1.venue_id = execution.leg1_venue_id
             AND leg1.contract_id = execution.leg1_contract_id
            JOIN binary_contract_complements AS leg2
              ON leg2.venue_id = execution.leg2_venue_id
             AND leg2.contract_id = execution.leg2_contract_id
            WHERE execution.leg1_side = 'sell'
              AND execution.leg2_side = 'sell'
        ), locked_pairs AS (
            SELECT
                portfolio_id,
                leg1_venue_id,
                leg1_contract_id,
                leg2_venue_id,
                leg2_contract_id,
                SUM(paired_quantity) AS paired_quantity
            FROM terminal_pairs
            GROUP BY
                portfolio_id,
                leg1_venue_id,
                leg1_contract_id,
                leg2_venue_id,
                leg2_contract_id
        )
        SELECT
            COALESCE((
                SELECT SUM(
                    local.locked_quantity
                    * (
                        1 - local.leg1_entry_price - local.leg2_entry_price
                        - COALESCE(
                            local.leg1_current_price - local.leg1_entry_price,
                            0
                        )
                        - COALESCE(
                            local.leg2_current_price - local.leg2_entry_price,
                            0
                        )
                    )
                )
                FROM local_pairs AS local
            ), 0)
            + COALESCE(SUM(
            LEAST(
                pairs.paired_quantity,
                leg1.remaining_quantity,
                leg2.remaining_quantity
            )
            * (
                1 - leg1.average_entry_price - leg2.average_entry_price
                - COALESCE(leg1.current_price - leg1.average_entry_price, 0)
                - COALESCE(leg2.current_price - leg2.average_entry_price, 0)
            )
        ), 0)
        FROM locked_pairs AS pairs
        JOIN position_capacity AS leg1
          ON leg1.portfolio_id = pairs.portfolio_id
         AND leg1.venue_id = pairs.leg1_venue_id
         AND leg1.contract_id = pairs.leg1_contract_id
         AND leg1.side = 'long'
        JOIN position_capacity AS leg2
          ON leg2.portfolio_id = pairs.portfolio_id
         AND leg2.venue_id = pairs.leg2_venue_id
         AND leg2.contract_id = pairs.leg2_contract_id
         AND leg2.side = 'long'
        """,
        (str(STRATEGY_PORTFOLIO_ID),),
    ).fetchone()[0]
    movement_row = cursor.execute(
        """
        SELECT
            COUNT(*),
            COALESCE(SUM(fee_amount) FILTER (WHERE fee_currency = 'USD'), 0),
            COALESCE(BOOL_OR(fee_amount IS NULL), FALSE),
            COALESCE(BOOL_OR(fee_amount IS NOT NULL AND fee_currency != 'USD'), FALSE)
        FROM cash_movements
        WHERE source_portfolio_id = %s OR destination_portfolio_id = %s
        """,
        (str(STRATEGY_PORTFOLIO_ID), str(STRATEGY_PORTFOLIO_ID)),
    ).fetchone()
    (
        realized,
        unrealized,
        position_fees,
        trading_fees,
        missing_mark,
        missing_fees,
        mixed_fees,
    ) = position_row
    unrealized += locked_adjustment
    movement_count, movement_fees, missing_movement_fees, mixed_movement_fees = (
        movement_row
    )
    fees = position_fees + (movement_fees if movement_count else Decimal("0"))
    gas = max(position_fees - trading_fees, Decimal("0"))
    flags = [
        flag
        for flag, present in (
            ("MISSING_MARK", missing_mark),
            ("MISSING_FEES", missing_fees or missing_movement_fees),
            ("INCONSISTENT_FEE_CURRENCY", mixed_fees or mixed_movement_fees),
        )
        if present
    ]
    total = realized + unrealized - fees
    cursor.execute(
        """
        INSERT INTO pnl_performance_points (
            point_id, source, venue_id, observed_at, realized_pnl_usd,
            unrealized_pnl_usd, fees_usd, trading_fees_usd, gas_usd,
            total_pnl_usd, scope, partial, quality_flags
        ) VALUES (%s, 'bot_ledger', NULL, %s, %s, %s, %s, %s, %s, %s,
                  'journal_all_time', %s, %s::jsonb)
        ON CONFLICT (point_id) DO UPDATE SET
            observed_at = EXCLUDED.observed_at,
            realized_pnl_usd = EXCLUDED.realized_pnl_usd,
            unrealized_pnl_usd = EXCLUDED.unrealized_pnl_usd,
            fees_usd = EXCLUDED.fees_usd,
            trading_fees_usd = EXCLUDED.trading_fees_usd,
            gas_usd = EXCLUDED.gas_usd,
            total_pnl_usd = EXCLUDED.total_pnl_usd,
            partial = EXCLUDED.partial,
            quality_flags = EXCLUDED.quality_flags
        """,
        (
            point_id or f"bot_ledger:{entry.sequence}",
            entry.recorded_at.value,
            realized,
            unrealized,
            fees,
            trading_fees,
            gas,
            total,
            bool(flags),
            json.dumps(flags, separators=(",", ":")),
        ),
    )


def _project_recovery(cursor: Any, value: ExposureRecovery) -> None:
    """Project the latest automatic recovery state and economics."""
    cursor.execute(
        """
        INSERT INTO exposure_recoveries (
            recovery_id, venue_id, contract_id, side, quantity, limit_price,
            portfolio_id, strategy_id, status, attempts, client_order_id,
            order_id, last_error, created_at, updated_at, execution_id, route,
            source_contract_id, source_side, source_price, source_fee_amount,
            source_fee_currency, estimated_vwap,
            estimated_recovery_fee_amount, estimated_recovery_fee_currency,
            estimated_gross_result, estimated_net_result, filled_quantity,
            average_price, recovery_fee_amount, recovery_fee_currency,
            actual_gross_result, actual_net_result
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT (recovery_id) DO UPDATE SET
            venue_id = EXCLUDED.venue_id,
            contract_id = EXCLUDED.contract_id,
            side = EXCLUDED.side,
            quantity = EXCLUDED.quantity,
            limit_price = EXCLUDED.limit_price,
            portfolio_id = EXCLUDED.portfolio_id,
            strategy_id = EXCLUDED.strategy_id,
            status = EXCLUDED.status,
            attempts = EXCLUDED.attempts,
            client_order_id = EXCLUDED.client_order_id,
            order_id = EXCLUDED.order_id,
            last_error = EXCLUDED.last_error,
            updated_at = EXCLUDED.updated_at,
            execution_id = EXCLUDED.execution_id,
            route = EXCLUDED.route,
            source_contract_id = EXCLUDED.source_contract_id,
            source_side = EXCLUDED.source_side,
            source_price = EXCLUDED.source_price,
            source_fee_amount = EXCLUDED.source_fee_amount,
            source_fee_currency = EXCLUDED.source_fee_currency,
            estimated_vwap = EXCLUDED.estimated_vwap,
            estimated_recovery_fee_amount = EXCLUDED.estimated_recovery_fee_amount,
            estimated_recovery_fee_currency = EXCLUDED.estimated_recovery_fee_currency,
            estimated_gross_result = EXCLUDED.estimated_gross_result,
            estimated_net_result = EXCLUDED.estimated_net_result,
            filled_quantity = EXCLUDED.filled_quantity,
            average_price = EXCLUDED.average_price,
            recovery_fee_amount = EXCLUDED.recovery_fee_amount,
            recovery_fee_currency = EXCLUDED.recovery_fee_currency,
            actual_gross_result = EXCLUDED.actual_gross_result,
            actual_net_result = EXCLUDED.actual_net_result
        """,
        (
            value.id,
            str(value.venue_id),
            str(value.contract_id),
            value.side.value,
            value.quantity.value,
            value.limit_price.value,
            str(value.portfolio_id) if value.portfolio_id else None,
            str(value.strategy_id) if value.strategy_id else None,
            value.status.value,
            value.attempts,
            str(value.client_order_id) if value.client_order_id else None,
            str(value.order_id) if value.order_id else None,
            value.last_error,
            value.created_at.value,
            value.updated_at.value,
            value.execution_id,
            value.route.value if value.route else None,
            str(value.source_contract_id) if value.source_contract_id else None,
            value.source_side.value if value.source_side else None,
            value.source_price.value if value.source_price else None,
            value.source_fee.amount if value.source_fee else None,
            str(value.source_fee.currency) if value.source_fee else None,
            value.estimated_vwap.value if value.estimated_vwap else None,
            (
                value.estimated_recovery_fee.amount
                if value.estimated_recovery_fee
                else None
            ),
            (
                str(value.estimated_recovery_fee.currency)
                if value.estimated_recovery_fee
                else None
            ),
            value.estimated_gross_result,
            value.estimated_net_result,
            value.filled_quantity.value,
            value.average_price.value if value.average_price else None,
            value.recovery_fee.amount if value.recovery_fee else None,
            str(value.recovery_fee.currency) if value.recovery_fee else None,
            value.actual_gross_result,
            value.actual_net_result,
        ),
    )


def _project_execution(cursor: Any, value: ArbitrageExecutionJournal) -> None:
    cursor.execute(
        """
        INSERT INTO arbitrage_execution_journals (
            execution_id, status,
            leg1_venue_id, leg1_contract_id, leg1_side, leg1_quantity,
            leg1_limit_price, leg1_client_order_id, leg1_order_id,
            leg1_filled_quantity,
            leg2_venue_id, leg2_contract_id, leg2_side, leg2_quantity,
            leg2_limit_price, leg2_client_order_id, leg2_order_id,
            leg2_filled_quantity, residual_quantity, portfolio_id, strategy_id,
            last_error, created_at, updated_at
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT (execution_id) DO UPDATE SET
            status = EXCLUDED.status,
            leg1_order_id = EXCLUDED.leg1_order_id,
            leg1_filled_quantity = EXCLUDED.leg1_filled_quantity,
            leg2_quantity = EXCLUDED.leg2_quantity,
            leg2_limit_price = EXCLUDED.leg2_limit_price,
            leg2_order_id = EXCLUDED.leg2_order_id,
            leg2_filled_quantity = EXCLUDED.leg2_filled_quantity,
            residual_quantity = EXCLUDED.residual_quantity,
            last_error = EXCLUDED.last_error,
            updated_at = EXCLUDED.updated_at
        """,
        (
            value.id,
            value.status.value,
            str(value.leg1_venue_id),
            str(value.leg1_contract_id),
            value.leg1_side.value,
            value.leg1_quantity.value,
            value.leg1_limit_price.value,
            str(value.leg1_client_order_id),
            str(value.leg1_order_id) if value.leg1_order_id else None,
            value.leg1_filled_quantity.value,
            str(value.leg2_venue_id),
            str(value.leg2_contract_id),
            value.leg2_side.value,
            value.leg2_quantity.value,
            value.leg2_limit_price.value,
            str(value.leg2_client_order_id),
            str(value.leg2_order_id) if value.leg2_order_id else None,
            value.leg2_filled_quantity.value,
            value.residual_quantity.value,
            str(value.portfolio_id) if value.portfolio_id else None,
            str(value.strategy_id) if value.strategy_id else None,
            value.last_error,
            value.created_at.value,
            value.updated_at.value,
        ),
    )


def _correlation_id(event: object) -> str | None:
    if isinstance(event, PreparedExecutionBatch):
        return event.planned.execution.id
    if isinstance(event, AccountingCorrectionRecorded):
        return event.correction.id
    if hasattr(event, "execution_id"):
        return str(getattr(event, "execution_id"))
    if hasattr(event, "execution"):
        return str(getattr(event, "execution").id)
    if hasattr(event, "recovery"):
        return str(getattr(event, "recovery").id)
    if hasattr(event, "id"):
        return str(getattr(event, "id"))
    return None


def _monitor_fields(
    market: MonitoredMarket,
) -> tuple[str, str, str | None, int | None]:
    """Map a monitored market to its SQL identity and cycle metadata."""
    if isinstance(market, MarketCycle):
        return (
            "cycle",
            monitored_market_key(market),
            market.underlying.symbol,
            market.interval_seconds,
        )
    return "regular", monitored_market_key(market), None, None


def _summary(event: object) -> dict[str, object]:
    """Return non-sensitive event metadata for operational tracing."""
    if isinstance(event, PreparedExecutionBatch):
        return {
            "execution_id": event.planned.execution.id,
            "prepared_legs": len(event.prepared),
            "rejection_reason": event.rejection_reason,
        }
    if isinstance(event, MarketMatchesUpdated):
        monitor_type, monitor_key, underlying, interval_seconds = _monitor_fields(
            event.cycle,
        )
        return {
            "monitor_type": monitor_type,
            "monitor_key": monitor_key,
            "underlying": underlying,
            "interval_seconds": interval_seconds,
            "pairs": len(event.pairs),
        }
    if isinstance(event, ArbitrageOpportunityFound):
        return {
            "opportunity_id": event.id,
            "side": event.opportunity.side.value,
            "net_edge": str(event.opportunity.net_edge),
        }
    if isinstance(event, (SubmitOrder, OrderPrepared)):
        command = event.command if isinstance(event, OrderPrepared) else event
        return {
            "execution_id": command.execution_id,
            "role": command.role,
            "venue_id": str(command.venue_id),
            "client_order_id": str(command.intent.client_order_id),
        }
    if isinstance(event, SubmissionReceived):
        return {
            "execution_id": event.command.execution_id,
            "role": event.command.role,
            "status": event.result.status.value,
            "reason": event.result.reason or (
                event.result.snapshot.reason if event.result.snapshot is not None else None
            ),
        }
    if isinstance(event, TradingSafetyStop):
        return {
            "venue_id": str(event.venue_id),
            "reason": event.reason,
        }
    if isinstance(event, InventoryOperationRecorded):
        return _inventory_operation_summary(event.record)
    if isinstance(event, MarketSettlementRecorded):
        return {
            "venue_id": str(event.settlement.venue_id),
            "market_id": str(event.settlement.market_id),
            "yes_payout": str(event.settlement.yes_payout.value),
            "no_payout": str(event.settlement.no_payout.value),
        }
    if isinstance(event, CashMovementRecorded):
        return {
            "movement_id": event.movement.id,
            "kind": event.movement.kind.value,
            "amount": str(event.movement.amount.amount),
            "currency": str(event.movement.amount.currency),
        }
    if isinstance(event, AccountingCorrectionRecorded):
        return {
            "correction_id": event.correction.id,
            "target_trade_id": str(event.correction.target_trade_id),
            "reason": event.correction.reason,
        }
    if isinstance(event, OrderSnapshotUpdated):
        return {
            "execution_id": event.execution_id,
            "role": event.role,
            "status": event.snapshot.status.value,
            "filled_quantity": str(event.snapshot.filled_quantity.value),
            "reason": event.snapshot.reason,
        }
    if isinstance(event, (ArbitragePlanned, ExecutionUpdated)):
        return {
            "execution_id": event.execution.id,
            "status": event.execution.status.value,
            "resolution_method": event.execution.resolution_method,
            "leg1_decision": _decision_summary(event.execution.leg1_decision),
            "leg2_decision": _decision_summary(event.execution.leg2_decision),
        }
    if isinstance(event, (RecoveryPlanned, RecoveryUpdated)):
        return {
            "recovery_id": event.recovery.id,
            "execution_id": event.recovery.execution_id,
            "status": event.recovery.status.value,
            "route": event.recovery.route.value if event.recovery.route else None,
            "estimated_net_result": (
                str(event.recovery.estimated_net_result)
                if event.recovery.estimated_net_result is not None
                else None
            ),
            "actual_net_result": (
                str(event.recovery.actual_net_result)
                if event.recovery.actual_net_result is not None
                else None
            ),
        }
    return {}


def _inventory_operation_summary(record: object) -> dict[str, object]:
    """Expose non-sensitive inventory economics in the SQL event audit."""
    intent = getattr(record, "intent", None)
    snapshot = getattr(record, "snapshot", None)
    reference = getattr(record, "reference", None)
    if snapshot is not None:
        reference = snapshot.reference
    quantity = getattr(snapshot, "quantity", None) or getattr(intent, "quantity", None)

    def money(value: object) -> dict[str, str] | None:
        if value is None:
            return None
        return {"amount": str(value.amount), "currency": str(value.currency)}

    return {
        "operation_id": (
            str(getattr(reference, "operation_id", ""))
            if reference is not None
            else None
        ),
        "venue_id": (
            str(getattr(reference, "venue_id", ""))
            if reference is not None
            else None
        ),
        "action": (
            getattr(intent, "action", None).value
            if getattr(intent, "action", None) is not None
            else None
        ),
        "market_id": (
            str(getattr(intent, "market_id", ""))
            if intent is not None
            else None
        ),
        "quantity": str(quantity.value) if quantity is not None else None,
        "collateral": money(getattr(snapshot, "collateral", None)),
        "payout": money(getattr(snapshot, "payout", None)),
        "fee": money(getattr(snapshot, "fee", None)),
        "fee_settlement_cost": money(
            getattr(snapshot, "fee_settlement_cost", None),
        ),
    }


def _decision_summary(
    value: OrderBookDecisionSnapshot | None,
) -> dict[str, object] | None:
    """Return JSON-safe execution-depth diagnostics for SQL event tracing."""
    if value is None:
        return None
    vwap = value.vwap()
    return {
        "venue_id": str(value.venue_id),
        "contract_id": str(value.contract_id),
        "side": value.side.value,
        "limit_price": str(value.limit_price.value),
        "requested_quantity": str(value.requested_quantity.value),
        "available_quantity": str(value.available_quantity().value),
        "shortfall_quantity": str(value.shortfall_quantity().value),
        "vwap": str(vwap.value) if vwap is not None else None,
        "book_timestamp": (
            value.book_timestamp.value.isoformat()
            if value.book_timestamp is not None
            else None
        ),
        "book_age_ns": value.book_age_ns,
        "source_hash": value.source_hash,
        "captured_at": value.captured_at.value.isoformat(),
        "levels": [
            {
                "price": str(level.price.value),
                "quantity": str(level.quantity.value),
            }
            for level in value.levels
        ],
    }
