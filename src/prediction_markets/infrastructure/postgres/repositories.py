"""Persist trading state through PostgreSQL repositories.

Responsibilities
----------------
- Implement repository ports with transactional database I/O.
"""

import json
from datetime import timedelta
from decimal import Decimal
from typing import Any, Protocol

from prediction_markets.domain.alerting.entities import (
    Incident,
    NotificationDelivery,
    NotificationSnapshot,
)
from prediction_markets.domain.alerting.enums import (
    Channel,
    DeliveryStatus,
    IncidentStatus,
    NotificationState,
    Severity,
)
from prediction_markets.domain.alerting.ports import (
    IncidentRepositoryPort,
    NotificationDeliveryRepositoryPort,
)
from prediction_markets.domain.alerting.value_objects import (
    AlertSource,
    DeliveryID,
    Fingerprint,
    IncidentID,
    Recipient,
)
from prediction_markets.domain.ports.repositories import (
    ArbitrageExecutionJournalRepository,
    ExposureRecoveryRepository,
    OrderRepository,
    PositionRepository,
    TradeRepository,
)
from prediction_markets.domain.shared.value_objects import (
    ClientOrderID,
    ContractID,
    Currency,
    Money,
    OrderID,
    PortfolioID,
    PositionID,
    Price,
    Quantity,
    StrategyID,
    Timestamp,
    TradeID,
    VenueID,
)
from prediction_markets.domain.trading.entities import OrderSnapshot, Position, Trade
from prediction_markets.domain.trading.enums import (
    ArbitrageExecutionStatus,
    OrderSide,
    OrderStatus,
    OrderType,
    PositionSide,
    RecoveryRoute,
    RecoveryStatus,
)
from prediction_markets.domain.trading.entities import ArbitrageExecutionJournal
from prediction_markets.domain.trading.entities import ExposureRecovery
from prediction_markets.domain.trading.portfolio import STRATEGY_PORTFOLIO_ID


class _Cursor(Protocol):
    """Describe the synchronous database cursor operations used by repositories."""

    def execute(self, query: str, params: tuple[Any, ...] = ()) -> Any:
        """Execute one parameterized SQL statement."""
        ...

    def fetchone(self) -> tuple[Any, ...] | None:
        """Return the next result row, or `None` when exhausted."""
        ...

    def fetchall(self) -> list[tuple[Any, ...]]:
        """Return all remaining result rows."""
        ...

    def close(self) -> None:
        """Release cursor resources."""
        ...

    @property
    def rowcount(self) -> int:
        """Return the number of rows affected by the last statement."""
        ...


class _Connection(Protocol):
    """Describe the transactional database connection used by repositories."""

    def cursor(self) -> _Cursor:
        """Create a cursor for one repository operation."""
        ...

    def commit(self) -> None:
        """Commit the current database transaction."""
        ...

    def rollback(self) -> None:
        """Roll back the current database transaction."""
        ...


class _PostgresRepository:
    """Provide shared transactional query helpers for PostgreSQL repositories.

    Notes
    -----
    - Repositories commit and roll back by default.
    - Callers may retain transaction ownership to compose several repository
      operations into one atomic unit.
    """

    def __init__(
        self,
        connection: _Connection,
        *,
        manage_transactions: bool = True,
    ) -> None:
        self._connection = connection
        self._manage_transactions = manage_transactions

    def _execute(self, query: str, params: tuple[Any, ...]) -> None:
        cursor = self._connection.cursor()
        try:
            cursor.execute(query, params)
            if self._manage_transactions:
                self._connection.commit()
        except Exception:
            if self._manage_transactions:
                self._connection.rollback()
            raise
        finally:
            cursor.close()

    def _fetchone(self, query: str, params: tuple[Any, ...]) -> tuple[Any, ...] | None:
        cursor = self._connection.cursor()
        try:
            cursor.execute(query, params)
            return cursor.fetchone()
        except Exception:
            if self._manage_transactions:
                self._connection.rollback()
            raise
        finally:
            cursor.close()

    def _fetchall(self, query: str, params: tuple[Any, ...]) -> list[tuple[Any, ...]]:
        cursor = self._connection.cursor()
        try:
            cursor.execute(query, params)
            return cursor.fetchall()
        except Exception:
            if self._manage_transactions:
                self._connection.rollback()
            raise
        finally:
            cursor.close()

    def _execute_returning(
        self,
        query: str,
        params: tuple[Any, ...],
    ) -> list[tuple[Any, ...]]:
        """Execute a mutating statement and return its committed rows."""
        cursor = self._connection.cursor()
        try:
            cursor.execute(query, params)
            rows = cursor.fetchall()
            if self._manage_transactions:
                self._connection.commit()
            return rows
        except Exception:
            if self._manage_transactions:
                self._connection.rollback()
            raise
        finally:
            cursor.close()


class PostgresOrderRepository(_PostgresRepository, OrderRepository):
    """Persist normalized order snapshots in PostgreSQL."""
    def get_by_client_order_id(
        self,
        client_order_id: ClientOrderID,
    ) -> OrderSnapshot | None:
        row = self._fetchone(
            f"{_ORDER_SELECT} WHERE client_order_id = %s",
            (str(client_order_id),),
        )
        return _order_from_row(row) if row else None

    def get_by_venue_order_id(self, order_id: OrderID) -> OrderSnapshot | None:
        row = self._fetchone(
            f"{_ORDER_SELECT} WHERE order_id = %s",
            (str(order_id),),
        )
        return _order_from_row(row) if row else None

    def save(self, order: OrderSnapshot) -> None:
        self._execute(
            """
            INSERT INTO orders (
                order_key, client_order_id, order_id, contract_id, status, side,
                quantity, order_type, limit_price, filled_quantity, average_price,
                created_at, updated_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (order_key) DO UPDATE SET
                client_order_id = EXCLUDED.client_order_id,
                order_id = EXCLUDED.order_id,
                contract_id = EXCLUDED.contract_id,
                status = EXCLUDED.status,
                side = EXCLUDED.side,
                quantity = EXCLUDED.quantity,
                order_type = EXCLUDED.order_type,
                limit_price = EXCLUDED.limit_price,
                filled_quantity = EXCLUDED.filled_quantity,
                average_price = EXCLUDED.average_price,
                created_at = EXCLUDED.created_at,
                updated_at = EXCLUDED.updated_at
            """,
            _order_params(order),
        )

    def list_open(self) -> tuple[OrderSnapshot, ...]:
        """Return persisted order snapshots that are not terminal."""
        statuses = (
            OrderStatus.SUBMITTED.value,
            OrderStatus.ACCEPTED.value,
            OrderStatus.PARTIALLY_FILLED.value,
        )
        return tuple(
            _order_from_row(row)
            for row in self._fetchall(
                f"{_ORDER_SELECT} WHERE status IN (%s, %s, %s) "
                "ORDER BY updated_at DESC NULLS LAST, order_key",
                statuses,
            )
        )

    def list_recent(
        self,
        *,
        limit: int,
        status: OrderStatus | None = None,
        contract_id: ContractID | None = None,
    ) -> tuple[OrderSnapshot, ...]:
        conditions: list[str] = []
        params: list[Any] = []
        if status is not None:
            conditions.append("status = %s")
            params.append(status.value)
        if contract_id is not None:
            conditions.append("contract_id = %s")
            params.append(str(contract_id))
        where = f" WHERE {' AND '.join(conditions)}" if conditions else ""
        params.append(limit)
        return tuple(
            _order_from_row(row)
            for row in self._fetchall(
                f"{_ORDER_SELECT}{where} "
                "ORDER BY updated_at DESC NULLS LAST, created_at DESC NULLS LAST "
                "LIMIT %s",
                tuple(params),
            )
        )


class PostgresTradeRepository(_PostgresRepository, TradeRepository):

    """Persist normalized trades in PostgreSQL."""
    def get(self, trade_id: TradeID) -> Trade | None:
        """Return the trade for an identifier, or `None` when absent."""
        row = self._fetchone(
            """
            SELECT trade_id, order_id, client_order_id, contract_id, side, quantity,
                   price, executed_at, portfolio_id, strategy_id, fee_amount, fee_currency,
                   fee_settlement_amount, fee_settlement_currency, journal_sequence,
                   venue_id
            FROM trades
            WHERE trade_id = %s
            """,
            (str(trade_id),),
        )
        return _trade_from_row(row) if row else None

    def save(self, trade: Trade) -> None:
        self._execute(
            """
            INSERT INTO trades (
                trade_id, order_id, client_order_id, contract_id, side, quantity,
                price, executed_at, portfolio_id, strategy_id, venue_id, journal_sequence,
                fee_amount, fee_currency,
                fee_settlement_amount, fee_settlement_currency
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (trade_id) DO UPDATE SET
                order_id = EXCLUDED.order_id,
                client_order_id = EXCLUDED.client_order_id,
                contract_id = EXCLUDED.contract_id,
                side = EXCLUDED.side,
                quantity = EXCLUDED.quantity,
                price = EXCLUDED.price,
                executed_at = EXCLUDED.executed_at,
                portfolio_id = EXCLUDED.portfolio_id,
                strategy_id = EXCLUDED.strategy_id,
                fee_amount = EXCLUDED.fee_amount,
                fee_currency = EXCLUDED.fee_currency,
                fee_settlement_amount = EXCLUDED.fee_settlement_amount,
                fee_settlement_currency = EXCLUDED.fee_settlement_currency,
                venue_id = COALESCE(trades.venue_id, EXCLUDED.venue_id),
                journal_sequence = COALESCE(
                    trades.journal_sequence,
                    EXCLUDED.journal_sequence
                )
            """,
            _trade_params(trade),
        )

    def list_by_order(self, order_id: OrderID) -> tuple[Trade, ...]:
        return tuple(
            _trade_from_row(row)
            for row in self._fetchall(
                """
                SELECT trade_id, order_id, client_order_id, contract_id, side,
                       quantity, price, executed_at, portfolio_id, strategy_id,
                       fee_amount, fee_currency, fee_settlement_amount,
                       fee_settlement_currency, journal_sequence, venue_id
                FROM trades
                WHERE order_id = %s
                ORDER BY journal_sequence NULLS LAST, executed_at, trade_id
                """,
                (str(order_id),),
            )
        )

    def list_by_contract(self, contract_id: ContractID) -> tuple[Trade, ...]:
        return tuple(
            _trade_from_row(row)
            for row in self._fetchall(
                """
                SELECT trade_id, order_id, client_order_id, contract_id, side,
                       quantity, price, executed_at, portfolio_id, strategy_id,
                       fee_amount, fee_currency, fee_settlement_amount,
                       fee_settlement_currency, journal_sequence, venue_id
                FROM trades
                WHERE contract_id = %s
                ORDER BY journal_sequence NULLS LAST, executed_at, trade_id
                """,
                (str(contract_id),),
            )
        )

    def list_recent(
        self,
        *,
        limit: int,
        order_id: OrderID | None = None,
        contract_id: ContractID | None = None,
    ) -> tuple[Trade, ...]:
        conditions: list[str] = []
        params: list[Any] = []
        if order_id is not None:
            conditions.append("order_id = %s")
            params.append(str(order_id))
        if contract_id is not None:
            conditions.append("contract_id = %s")
            params.append(str(contract_id))
        where = f" WHERE {' AND '.join(conditions)}" if conditions else ""
        params.append(limit)
        return tuple(
            _trade_from_row(row)
            for row in self._fetchall(
                f"{_TRADE_SELECT}{where} ORDER BY journal_sequence DESC NULLS LAST, "
                "executed_at DESC, trade_id LIMIT %s",
                tuple(params),
            )
        )

    def list_by_client_orders(
        self,
        client_order_ids: tuple[ClientOrderID, ...],
    ) -> tuple[Trade, ...]:
        """Return trades for a set of client order identifiers.

        Parameters
        ----------
        client_order_ids
            Distinct durable client order identifiers to include.

        Returns
        -------
        tuple[Trade, ...]
            Matching trades ordered by execution time and trade identifier.
        """
        if not client_order_ids:
            return ()
        return tuple(
            _trade_from_row(row)
            for row in self._fetchall(
                f"{_TRADE_SELECT} WHERE client_order_id = ANY(%s) "
                "ORDER BY journal_sequence NULLS LAST, executed_at, trade_id",
                ([str(value) for value in client_order_ids],),
            )
        )


class PostgresPositionRepository(_PostgresRepository, PositionRepository):

    """Persist current contract positions in PostgreSQL."""
    def get(self, position_id: PositionID) -> Position | None:
        """Return the position for an identifier, or `None` when absent."""
        row = self._fetchone(
            """
            SELECT position_id, contract_id, venue_id, side, quantity,
                   average_entry_price, current_price, realized_pnl,
                   fee_settlement_amount, fee_settlement_currency, quality_flags,
                   portfolio_id, opened_at, updated_at
            FROM positions
            WHERE position_id = %s
            """,
            (str(position_id),),
        )
        return _position_from_row(row) if row else None

    def save(self, position: Position) -> None:
        self._execute(
            """
            INSERT INTO positions (
                position_id, contract_id, venue_id, side, quantity, average_entry_price,
                current_price, realized_pnl, fee_settlement_amount,
                fee_settlement_currency, quality_flags, portfolio_id, opened_at, updated_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s, %s)
            ON CONFLICT (position_id) DO UPDATE SET
                venue_id = EXCLUDED.venue_id,
                contract_id = EXCLUDED.contract_id,
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
                portfolio_id = EXCLUDED.portfolio_id,
                opened_at = EXCLUDED.opened_at,
                updated_at = EXCLUDED.updated_at
            """,
            _position_params(position),
        )

    def list_open(self) -> tuple[Position, ...]:
        return tuple(
            _position_from_row(row)
            for row in self._fetchall(
                """
                SELECT position_id, contract_id, venue_id, side, quantity,
                       average_entry_price, current_price, realized_pnl,
                       fee_settlement_amount, fee_settlement_currency, quality_flags,
                       portfolio_id, opened_at, updated_at
                FROM positions
                WHERE side != %s
                ORDER BY updated_at DESC NULLS LAST, position_id
                """,
                (PositionSide.FLAT.value,),
            )
        )

    def list_recent(
        self,
        *,
        limit: int,
        open_only: bool = False,
    ) -> tuple[Position, ...]:
        where = " WHERE side != %s" if open_only else ""
        params: tuple[Any, ...] = (
            (PositionSide.FLAT.value, limit) if open_only else (limit,)
        )
        return tuple(
            _position_from_row(row)
            for row in self._fetchall(
                f"{_POSITION_SELECT}{where} "
                "ORDER BY updated_at DESC NULLS LAST, opened_at DESC NULLS LAST "
                "LIMIT %s",
                params,
            )
        )


class PostgresExposureRecoveryRepository(
    _PostgresRepository,
    ExposureRecoveryRepository,
):

    """Persist unresolved and completed exposure recoveries in PostgreSQL."""
    def get(self, recovery_id: str) -> ExposureRecovery | None:
        """Return the exposure recovery for an identifier, or `None` when absent."""
        row = self._fetchone(
            f"{_RECOVERY_SELECT} WHERE recovery_id = %s",
            (recovery_id,),
        )
        return _recovery_from_row(row) if row else None

    def save(self, recovery: ExposureRecovery) -> None:
        self._execute(
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
            )
            VALUES (
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
                actual_net_result = EXCLUDED.actual_net_result,
                updated_at = EXCLUDED.updated_at
            """,
            _recovery_params(recovery),
        )

    def list_unresolved(self) -> tuple[ExposureRecovery, ...]:
        return tuple(
            _recovery_from_row(row)
            for row in self._fetchall(
                f"{_RECOVERY_SELECT} WHERE status != %s ORDER BY created_at, recovery_id",
                (RecoveryStatus.RESOLVED.value,),
            )
        )

    def list_recent(
        self,
        *,
        limit: int,
        status: RecoveryStatus | None = None,
    ) -> tuple[ExposureRecovery, ...]:
        where = " WHERE status = %s" if status is not None else ""
        params: tuple[Any, ...] = (
            (status.value, limit) if status is not None else (limit,)
        )
        return tuple(
            _recovery_from_row(row)
            for row in self._fetchall(
                f"{_RECOVERY_SELECT}{where} ORDER BY updated_at DESC LIMIT %s",
                params,
            )
        )


class PostgresArbitrageExecutionJournalRepository(
    _PostgresRepository,
    ArbitrageExecutionJournalRepository,
):
    """Persist durable two-leg arbitrage execution journals in PostgreSQL."""

    def claim(self, journal: ArbitrageExecutionJournal) -> bool:
        """Atomically claim an opportunity before its first order is submitted."""
        cursor = self._connection.cursor()
        try:
            cursor.execute(
                f"{_JOURNAL_INSERT} "
                "ON CONFLICT (execution_id) DO NOTHING "
                "RETURNING execution_id",
                _journal_params(journal),
            )
            claimed = cursor.fetchone() is not None
            self._connection.commit()
            return claimed
        except Exception:
            self._connection.rollback()
            raise
        finally:
            cursor.close()

    def get(self, execution_id: str) -> ArbitrageExecutionJournal | None:
        """Return the execution journal for an identifier, or `None` when absent."""
        row = self._fetchone(
            f"{_JOURNAL_SELECT} WHERE execution_id = %s",
            (execution_id,),
        )
        return _journal_from_row(row) if row else None

    def save(self, journal: ArbitrageExecutionJournal) -> None:
        self._execute(
            f"""
            {_JOURNAL_INSERT}
            ON CONFLICT (execution_id) DO UPDATE SET
                status = EXCLUDED.status,
                leg1_order_id = EXCLUDED.leg1_order_id,
                leg1_filled_quantity = EXCLUDED.leg1_filled_quantity,
                leg2_order_id = EXCLUDED.leg2_order_id,
                leg2_filled_quantity = EXCLUDED.leg2_filled_quantity,
                residual_quantity = EXCLUDED.residual_quantity,
                last_error = EXCLUDED.last_error,
                updated_at = EXCLUDED.updated_at
            """,
            _journal_params(journal),
        )

    def list_active(self) -> tuple[ArbitrageExecutionJournal, ...]:
        return tuple(
            _journal_from_row(row)
            for row in self._fetchall(
                f"{_JOURNAL_SELECT} WHERE status NOT IN (%s, %s, %s) "
                "ORDER BY created_at, execution_id",
                (
                    ArbitrageExecutionStatus.COMPLETED.value,
                    ArbitrageExecutionStatus.RECOVERED.value,
                    ArbitrageExecutionStatus.REJECTED.value,
                ),
            )
        )

    def list_recent(
        self,
        *,
        limit: int,
        status: ArbitrageExecutionStatus | None = None,
    ) -> tuple[ArbitrageExecutionJournal, ...]:
        where = " WHERE status = %s" if status is not None else ""
        params: tuple[Any, ...] = (
            (status.value, limit) if status is not None else (limit,)
        )
        return tuple(
            _journal_from_row(row)
            for row in self._fetchall(
                f"{_JOURNAL_SELECT}{where} ORDER BY updated_at DESC LIMIT %s",
                params,
            )
        )


class PostgresIncidentRepository(_PostgresRepository, IncidentRepositoryPort):
    """Persist alerting incidents in PostgreSQL."""

    def get_incident(self, incident_id: IncidentID) -> Incident | None:
        """Lock and return one incident by identifier when it exists."""
        row = self._fetchone(
            f"{_INCIDENT_SELECT} WHERE incident_id = %s FOR UPDATE",
            (incident_id.value,),
        )
        return _incident_from_row(row) if row else None

    def get_active_by_fingerprint(
        self,
        fingerprint: Fingerprint,
    ) -> Incident | None:
        """Lock and return the active incident correlated by a fingerprint."""
        row = self._fetchone(
            f"{_INCIDENT_SELECT} WHERE fingerprint = %s "
            "AND status NOT IN ('resolved', 'closed') FOR UPDATE",
            (fingerprint.value,),
        )
        return _incident_from_row(row) if row else None

    def list_active(self) -> tuple[Incident, ...]:
        """Return incidents eligible for timeout escalation."""
        return tuple(
            _incident_from_row(row)
            for row in self._fetchall(
                f"{_INCIDENT_SELECT} "
                "WHERE status NOT IN ('resolved', 'closed') "
                "ORDER BY opened_at, incident_id",
                (),
            )
        )

    def add_incident(self, incident: Incident) -> None:
        """Insert a newly opened incident."""
        self._execute(
            """
            INSERT INTO incidents (
                incident_id, fingerprint, status, severity, source_component,
                source_service, source_instance, title, summary, description,
                opened_at, acknowledged_at, in_progress_at, resolved_at
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            """,
            _incident_params(incident),
        )

    def update_incident(self, incident: Incident) -> None:
        """Persist the current lifecycle state of an incident."""
        self._execute(
            """
            UPDATE incidents SET
                fingerprint = %s, status = %s, severity = %s,
                source_component = %s, source_service = %s,
                source_instance = %s, title = %s, summary = %s,
                description = %s, opened_at = %s, acknowledged_at = %s,
                in_progress_at = %s, resolved_at = %s
            WHERE incident_id = %s
            """,
            _incident_params(incident)[1:] + (incident.id.value,),
        )


class PostgresNotificationDeliveryRepository(
    _PostgresRepository,
    NotificationDeliveryRepositoryPort,
):
    """Persist and atomically lease Alertmanager delivery outbox rows.

    Parameters
    ----------
    connection
        Dedicated synchronous PostgreSQL connection.
    lease_seconds
        Time after which an abandoned sending attempt may be reclaimed.
    """

    def __init__(
        self,
        connection: _Connection,
        *,
        lease_seconds: float = 300,
        manage_transactions: bool = True,
    ) -> None:
        if lease_seconds <= 0:
            raise ValueError("Delivery lease must be positive")
        super().__init__(
            connection,
            manage_transactions=manage_transactions,
        )
        self._lease = timedelta(seconds=lease_seconds)

    def get_notification_delivery(
        self,
        delivery_id: DeliveryID,
    ) -> NotificationDelivery | None:
        """Lock and return one notification delivery by identifier when it exists."""
        row = self._fetchone(
            f"{_DELIVERY_SELECT} WHERE delivery_id = %s FOR UPDATE",
            (delivery_id.value,),
        )
        return _delivery_from_row(row) if row else None

    def claim_pending(
        self,
        limit: int,
        at: Timestamp,
    ) -> tuple[NotificationDelivery, ...]:
        """Claim pending and expired sending rows without worker contention."""
        if limit <= 0:
            raise ValueError("Delivery claim limit must be positive")
        rows = self._execute_returning(
            f"""
            WITH candidates AS (
                SELECT delivery_id
                FROM notification_deliveries
                WHERE status = 'pending'
                   OR (status = 'sending' AND started_at <= %s)
                ORDER BY requested_at, delivery_id
                FOR UPDATE SKIP LOCKED
                LIMIT %s
            ), updated AS (
                UPDATE notification_deliveries AS delivery
                SET status = 'sending', started_at = %s,
                    attempt_count = delivery.attempt_count + 1,
                    provider_reference = NULL, last_error = NULL,
                    delivered_at = NULL, failed_at = NULL
                FROM candidates
                WHERE delivery.delivery_id = candidates.delivery_id
                RETURNING delivery.*
            )
            SELECT {_DELIVERY_COLUMNS}
            FROM updated
            ORDER BY requested_at, delivery_id
            """,
            (at.value - self._lease, limit, at.value),
        )
        return tuple(_delivery_from_row(row) for row in rows)

    def add_notification_delivery(
        self,
        notification_delivery: NotificationDelivery,
    ) -> None:
        """Insert one immutable notification request."""
        self._execute(
            f"""
            INSERT INTO notification_deliveries ({_DELIVERY_COLUMNS})
            VALUES ({', '.join(['%s'] * 24)})
            """,
            _delivery_params(notification_delivery),
        )

    def update_notification_delivery(
        self,
        notification_delivery: NotificationDelivery,
    ) -> None:
        """Update one lifecycle state with stale-attempt protection."""
        guarded = notification_delivery.status in {
            DeliveryStatus.DELIVERED,
            DeliveryStatus.FAILED,
        }
        where = "delivery_id = %s"
        params: tuple[Any, ...] = (
            notification_delivery.status.value,
            notification_delivery.attempt_count,
            notification_delivery.provider_reference,
            notification_delivery.last_error,
            (
                notification_delivery.started_at.value
                if notification_delivery.started_at
                else None
            ),
            (
                notification_delivery.delivered_at.value
                if notification_delivery.delivered_at
                else None
            ),
            (
                notification_delivery.failed_at.value
                if notification_delivery.failed_at
                else None
            ),
            notification_delivery.requested_at.value,
            notification_delivery.id.value,
        )
        if guarded:
            where += " AND attempt_count = %s AND started_at = %s"
            params += (
                notification_delivery.attempt_count,
                notification_delivery.started_at.value,
            )
        cursor = self._connection.cursor()
        try:
            cursor.execute(
                f"""
                UPDATE notification_deliveries SET
                    status = %s, attempt_count = %s, provider_reference = %s,
                    last_error = %s, started_at = %s, delivered_at = %s,
                    failed_at = %s, requested_at = %s
                WHERE {where}
                """,
                params,
            )
            if cursor.rowcount != 1:
                raise RuntimeError("Stale notification delivery generation")
            if self._manage_transactions:
                self._connection.commit()
        except Exception:
            if self._manage_transactions:
                self._connection.rollback()
            raise
        finally:
            cursor.close()


def _trade_params(trade: Trade) -> tuple[Any, ...]:
    return (
        str(trade.id),
        str(trade.order_id) if trade.order_id else None,
        str(trade.client_order_id) if trade.client_order_id else None,
        str(trade.contract_id),
        trade.side.value,
        trade.quantity.value,
        trade.price.value,
        trade.executed_at.value,
        str(trade.portfolio_id) if trade.portfolio_id else None,
        str(trade.strategy_id) if trade.strategy_id else None,
        str(trade.venue_id),
        trade.journal_sequence,
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
    )


_ORDER_SELECT = """
    SELECT status, contract_id, side, quantity, order_type, client_order_id,
           order_id, limit_price, filled_quantity, average_price, created_at,
           updated_at
    FROM orders
"""

_TRADE_SELECT = """
    SELECT trade_id, order_id, client_order_id, contract_id, side, quantity,
           price, executed_at, portfolio_id, strategy_id, fee_amount, fee_currency,
           fee_settlement_amount, fee_settlement_currency, journal_sequence, venue_id
    FROM trades
"""

_POSITION_SELECT = """
    SELECT position_id, contract_id, venue_id, side, quantity, average_entry_price,
           current_price, realized_pnl, fee_settlement_amount,
           fee_settlement_currency, quality_flags, portfolio_id, opened_at, updated_at
    FROM positions
"""

_RECOVERY_SELECT = """
    SELECT recovery_id, venue_id, contract_id, side, quantity, limit_price,
           portfolio_id, strategy_id, status, attempts, client_order_id, order_id,
           last_error, created_at, updated_at, execution_id, route,
           source_contract_id, source_side, source_price, source_fee_amount,
           source_fee_currency, estimated_vwap, estimated_recovery_fee_amount,
           estimated_recovery_fee_currency, estimated_gross_result,
           estimated_net_result, filled_quantity, average_price,
           recovery_fee_amount, recovery_fee_currency, actual_gross_result,
           actual_net_result
    FROM exposure_recoveries
"""

_JOURNAL_INSERT = """
    INSERT INTO arbitrage_execution_journals (
        execution_id, status, leg1_venue_id, leg1_contract_id, leg1_side,
        leg1_quantity, leg1_limit_price, leg1_client_order_id, leg1_order_id,
        leg1_filled_quantity, leg2_venue_id, leg2_contract_id, leg2_side,
        leg2_quantity, leg2_limit_price, leg2_client_order_id, leg2_order_id,
        leg2_filled_quantity, residual_quantity, portfolio_id, strategy_id,
        last_error, created_at, updated_at
    )
    VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
    )
"""

_JOURNAL_SELECT = """
    SELECT execution_id, status, leg1_venue_id, leg1_contract_id, leg1_side,
           leg1_quantity, leg1_limit_price, leg1_client_order_id, leg1_order_id,
           leg1_filled_quantity, leg2_venue_id, leg2_contract_id, leg2_side,
           leg2_quantity, leg2_limit_price, leg2_client_order_id, leg2_order_id,
           leg2_filled_quantity, residual_quantity, portfolio_id, strategy_id,
           last_error, created_at, updated_at
    FROM arbitrage_execution_journals
"""

_INCIDENT_SELECT = """
    SELECT incident_id, fingerprint, status, severity, source_component,
           source_service, source_instance, title, summary, description,
           opened_at, acknowledged_at, in_progress_at, resolved_at
    FROM incidents
"""

_DELIVERY_COLUMNS = """
    delivery_id, incident_id, recipient_id, recipient_channel,
    recipient_address, alert_fingerprint, alert_state, alert_severity,
    source_component, source_service, source_instance, alert_title,
    alert_summary, alert_description, alert_starts_at, alert_ends_at,
    status, requested_at, attempt_count, provider_reference, last_error,
    started_at, delivered_at, failed_at
""".strip()

_DELIVERY_SELECT = f"""
    SELECT {_DELIVERY_COLUMNS}
    FROM notification_deliveries
"""


def _incident_params(incident: Incident) -> tuple[Any, ...]:
    """Serialize a validated incident into PostgreSQL parameters."""
    return (
        incident.id.value,
        incident.fingerprint.value,
        incident.status.value,
        incident.severity.value,
        incident.source.component,
        incident.source.service,
        incident.source.instance,
        incident.title,
        incident.summary,
        incident.description,
        incident.opened_at.value,
        incident.acknowledged_at.value if incident.acknowledged_at else None,
        incident.in_progress_at.value if incident.in_progress_at else None,
        incident.resolved_at.value if incident.resolved_at else None,
    )


def _delivery_params(delivery: NotificationDelivery) -> tuple[Any, ...]:
    """Serialize a delivery and its immutable snapshot."""
    snapshot = delivery.snapshot
    return (
        delivery.id.value,
        delivery.incident_id.value if delivery.incident_id is not None else None,
        delivery.recipient.id,
        delivery.recipient.channel.value,
        delivery.recipient.address,
        snapshot.fingerprint.value,
        snapshot.state.value,
        snapshot.severity.value,
        snapshot.source.component,
        snapshot.source.service,
        snapshot.source.instance,
        snapshot.title,
        snapshot.summary,
        snapshot.description,
        snapshot.starts_at.value,
        snapshot.ends_at.value if snapshot.ends_at else None,
        delivery.status.value,
        delivery.requested_at.value,
        delivery.attempt_count,
        delivery.provider_reference,
        delivery.last_error,
        delivery.started_at.value if delivery.started_at else None,
        delivery.delivered_at.value if delivery.delivered_at else None,
        delivery.failed_at.value if delivery.failed_at else None,
    )


def _order_params(order: OrderSnapshot) -> tuple[Any, ...]:
    """Serialize a normalized order snapshot into PostgreSQL parameters."""
    key = (
        f"client:{order.client_order_id}"
        if order.client_order_id
        else f"venue:{order.order_id}"
    )
    return (
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
    )


def _position_params(position: Position) -> tuple[Any, ...]:
    return (
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
    )


def _recovery_params(recovery: ExposureRecovery) -> tuple[Any, ...]:
    return (
        recovery.id,
        str(recovery.venue_id),
        str(recovery.contract_id),
        recovery.side.value,
        recovery.quantity.value,
        recovery.limit_price.value,
        str(recovery.portfolio_id) if recovery.portfolio_id else None,
        str(recovery.strategy_id) if recovery.strategy_id else None,
        recovery.status.value,
        recovery.attempts,
        str(recovery.client_order_id) if recovery.client_order_id else None,
        str(recovery.order_id) if recovery.order_id else None,
        recovery.last_error,
        recovery.created_at.value,
        recovery.updated_at.value,
        recovery.execution_id,
        recovery.route.value if recovery.route else None,
        str(recovery.source_contract_id) if recovery.source_contract_id else None,
        recovery.source_side.value if recovery.source_side else None,
        recovery.source_price.value if recovery.source_price else None,
        recovery.source_fee.amount if recovery.source_fee else None,
        str(recovery.source_fee.currency) if recovery.source_fee else None,
        recovery.estimated_vwap.value if recovery.estimated_vwap else None,
        (
            recovery.estimated_recovery_fee.amount
            if recovery.estimated_recovery_fee
            else None
        ),
        (
            str(recovery.estimated_recovery_fee.currency)
            if recovery.estimated_recovery_fee
            else None
        ),
        recovery.estimated_gross_result,
        recovery.estimated_net_result,
        recovery.filled_quantity.value,
        recovery.average_price.value if recovery.average_price else None,
        recovery.recovery_fee.amount if recovery.recovery_fee else None,
        str(recovery.recovery_fee.currency) if recovery.recovery_fee else None,
        recovery.actual_gross_result,
        recovery.actual_net_result,
    )


def _journal_params(journal: ArbitrageExecutionJournal) -> tuple[Any, ...]:
    """Serialize an execution journal into PostgreSQL parameters."""
    return (
        journal.id,
        journal.status.value,
        str(journal.leg1_venue_id),
        str(journal.leg1_contract_id),
        journal.leg1_side.value,
        journal.leg1_quantity.value,
        journal.leg1_limit_price.value,
        str(journal.leg1_client_order_id),
        str(journal.leg1_order_id) if journal.leg1_order_id else None,
        journal.leg1_filled_quantity.value,
        str(journal.leg2_venue_id),
        str(journal.leg2_contract_id),
        journal.leg2_side.value,
        journal.leg2_quantity.value,
        journal.leg2_limit_price.value,
        str(journal.leg2_client_order_id),
        str(journal.leg2_order_id) if journal.leg2_order_id else None,
        journal.leg2_filled_quantity.value,
        journal.residual_quantity.value,
        str(journal.portfolio_id) if journal.portfolio_id else None,
        str(journal.strategy_id) if journal.strategy_id else None,
        journal.last_error,
        journal.created_at.value,
        journal.updated_at.value,
    )


def _incident_from_row(row: tuple[Any, ...]) -> Incident:
    """Reconstruct a validated incident from a database row."""
    return Incident(
        id=IncidentID(str(row[0])),
        fingerprint=Fingerprint(str(row[1])),
        status=IncidentStatus(str(row[2])),
        severity=Severity(str(row[3])),
        source=AlertSource(
            component=str(row[4]),
            service=str(row[5]) if row[5] is not None else None,
            instance=str(row[6]) if row[6] is not None else None,
        ),
        title=str(row[7]) if row[7] is not None else None,
        summary=str(row[8]) if row[8] is not None else None,
        description=str(row[9]),
        opened_at=Timestamp(row[10]),
        acknowledged_at=Timestamp(row[11]) if row[11] is not None else None,
        in_progress_at=Timestamp(row[12]) if row[12] is not None else None,
        resolved_at=Timestamp(row[13]) if row[13] is not None else None,
    )


def _delivery_from_row(row: tuple[Any, ...]) -> NotificationDelivery:
    """Reconstruct a validated delivery and immutable snapshot."""
    return NotificationDelivery(
        id=DeliveryID(str(row[0])),
        incident_id=IncidentID(str(row[1])) if row[1] is not None else None,
        recipient=Recipient(
            id=str(row[2]),
            channel=Channel(str(row[3])),
            address=str(row[4]),
        ),
        snapshot=NotificationSnapshot(
            fingerprint=Fingerprint(str(row[5])),
            state=NotificationState(str(row[6])),
            severity=Severity(str(row[7])),
            source=AlertSource(
                component=str(row[8]),
                service=str(row[9]) if row[9] is not None else None,
                instance=str(row[10]) if row[10] is not None else None,
            ),
            title=str(row[11]) if row[11] is not None else None,
            summary=str(row[12]) if row[12] is not None else None,
            description=str(row[13]),
            starts_at=Timestamp(row[14]),
            ends_at=Timestamp(row[15]) if row[15] is not None else None,
        ),
        status=DeliveryStatus(str(row[16])),
        requested_at=Timestamp(row[17]),
        attempt_count=int(row[18]),
        provider_reference=str(row[19]) if row[19] is not None else None,
        last_error=str(row[20]) if row[20] is not None else None,
        started_at=Timestamp(row[21]) if row[21] is not None else None,
        delivered_at=Timestamp(row[22]) if row[22] is not None else None,
        failed_at=Timestamp(row[23]) if row[23] is not None else None,
    )


def _order_from_row(row: tuple[Any, ...]) -> OrderSnapshot:
    return OrderSnapshot(
        status=OrderStatus(str(row[0])),
        contract_id=ContractID(str(row[1])),
        side=OrderSide(str(row[2])),
        quantity=Quantity(Decimal(str(row[3]))),
        order_type=OrderType(str(row[4])),
        client_order_id=(ClientOrderID(str(row[5])) if row[5] is not None else None),
        order_id=OrderID(str(row[6])) if row[6] is not None else None,
        limit_price=Price(Decimal(str(row[7]))) if row[7] is not None else None,
        filled_quantity=Quantity(Decimal(str(row[8]))),
        average_price=Price(Decimal(str(row[9]))) if row[9] is not None else None,
        created_at=Timestamp(row[10]) if row[10] is not None else None,
        updated_at=Timestamp(row[11]) if row[11] is not None else None,
    )


def _trade_from_row(row: tuple[Any, ...]) -> Trade:
    return Trade(
        id=TradeID(str(row[0])),
        order_id=OrderID(str(row[1])) if row[1] is not None else None,
        client_order_id=ClientOrderID(str(row[2])) if row[2] is not None else None,
        contract_id=ContractID(str(row[3])),
        venue_id=(
            VenueID(str(row[15]))
            if len(row) > 15 and row[15] is not None
            else VenueID("legacy")
        ),
        side=OrderSide(str(row[4])),
        quantity=Quantity(Decimal(str(row[5]))),
        price=Price(Decimal(str(row[6]))),
        executed_at=Timestamp(row[7]),
        portfolio_id=PortfolioID(str(row[8])) if row[8] is not None else None,
        strategy_id=StrategyID(str(row[9])) if row[9] is not None else None,
        fee=(
            Money(Decimal(str(row[10])), Currency(str(row[11])))
            if len(row) > 11 and row[10] is not None and row[11] is not None
            else None
        ),
        fee_settlement_cost=(
            Money(Decimal(str(row[12])), Currency(str(row[13])))
            if len(row) > 13 and row[12] is not None and row[13] is not None
            else None
        ),
        journal_sequence=(
            int(row[14])
            if len(row) > 14 and row[14] is not None
            else None
        ),
    )


def _position_from_row(row: tuple[Any, ...]) -> Position:
    legacy = len(row) <= 8
    if legacy:
        venue_id = VenueID("legacy")
        side_index = 2
        quantity_index = 3
        average_index = 4
        current_price = None
        realized_pnl = Decimal("0")
        fees = None
        quality_flags = ("LEGACY_POSITION",)
        portfolio_index = 5
        opened_index = 6
        updated_index = 7
    else:
        venue_id = VenueID(str(row[2])) if row[2] is not None else VenueID("legacy")
        side_index = 3
        quantity_index = 4
        average_index = 5
        current_price = Price(Decimal(str(row[6]))) if row[6] is not None else None
        realized_pnl = Decimal(str(row[7])) if row[7] is not None else Decimal("0")
        fees = (
            Money(Decimal(str(row[8])), Currency(str(row[9])))
            if row[8] is not None and row[9] is not None
            else None
        )
        raw_flags = row[10]
        if isinstance(raw_flags, str):
            quality_flags = tuple(json.loads(raw_flags))
        else:
            quality_flags = tuple(raw_flags or ())
        portfolio_index = 11
        opened_index = 12
        updated_index = 13
    return Position(
        id=PositionID(str(row[0])),
        contract_id=ContractID(str(row[1])),
        side=PositionSide(str(row[side_index])),
        quantity=Quantity(Decimal(str(row[quantity_index]))),
        average_price=Price(Decimal(str(row[average_index]))) if row[average_index] is not None else None,
        current_price=current_price,
        realized_pnl=realized_pnl,
        fees=fees,
        quality_flags=quality_flags,
        portfolio_id=PortfolioID(str(row[portfolio_index])) if row[portfolio_index] is not None else STRATEGY_PORTFOLIO_ID,
        venue_id=venue_id,
        opened_at=Timestamp(row[opened_index]) if row[opened_index] is not None else None,
        updated_at=Timestamp(row[updated_index]) if row[updated_index] is not None else None,
    )


def _recovery_from_row(row: tuple[Any, ...]) -> ExposureRecovery:
    return ExposureRecovery(
        id=str(row[0]),
        venue_id=VenueID(str(row[1])),
        contract_id=ContractID(str(row[2])),
        side=OrderSide(str(row[3])),
        quantity=Quantity(Decimal(str(row[4]))),
        limit_price=Price(Decimal(str(row[5]))),
        portfolio_id=PortfolioID(str(row[6])) if row[6] is not None else None,
        strategy_id=StrategyID(str(row[7])) if row[7] is not None else None,
        status=RecoveryStatus(str(row[8])),
        attempts=int(row[9]),
        client_order_id=ClientOrderID(str(row[10])) if row[10] is not None else None,
        order_id=OrderID(str(row[11])) if row[11] is not None else None,
        last_error=str(row[12]) if row[12] is not None else None,
        created_at=Timestamp(row[13]),
        updated_at=Timestamp(row[14]),
        execution_id=str(row[15]) if row[15] is not None else None,
        route=RecoveryRoute(str(row[16])) if row[16] is not None else None,
        source_contract_id=(
            ContractID(str(row[17])) if row[17] is not None else None
        ),
        source_side=OrderSide(str(row[18])) if row[18] is not None else None,
        source_price=(
            Price(Decimal(str(row[19]))) if row[19] is not None else None
        ),
        source_fee=(
            Money(Decimal(str(row[20])), Currency(str(row[21])))
            if row[20] is not None and row[21] is not None
            else None
        ),
        estimated_vwap=(
            Price(Decimal(str(row[22]))) if row[22] is not None else None
        ),
        estimated_recovery_fee=(
            Money(Decimal(str(row[23])), Currency(str(row[24])))
            if row[23] is not None and row[24] is not None
            else None
        ),
        estimated_gross_result=(
            Decimal(str(row[25])) if row[25] is not None else None
        ),
        estimated_net_result=(
            Decimal(str(row[26])) if row[26] is not None else None
        ),
        filled_quantity=Quantity(Decimal(str(row[27]))),
        average_price=(
            Price(Decimal(str(row[28]))) if row[28] is not None else None
        ),
        recovery_fee=(
            Money(Decimal(str(row[29])), Currency(str(row[30])))
            if row[29] is not None and row[30] is not None
            else None
        ),
        actual_gross_result=(
            Decimal(str(row[31])) if row[31] is not None else None
        ),
        actual_net_result=(
            Decimal(str(row[32])) if row[32] is not None else None
        ),
    )


def _journal_from_row(row: tuple[Any, ...]) -> ArbitrageExecutionJournal:
    """Reconstruct a validated execution journal from a database row."""
    return ArbitrageExecutionJournal(
        id=str(row[0]),
        status=ArbitrageExecutionStatus(str(row[1])),
        leg1_venue_id=VenueID(str(row[2])),
        leg1_contract_id=ContractID(str(row[3])),
        leg1_side=OrderSide(str(row[4])),
        leg1_quantity=Quantity(Decimal(str(row[5]))),
        leg1_limit_price=Price(Decimal(str(row[6]))),
        leg1_client_order_id=ClientOrderID(str(row[7])),
        leg1_order_id=OrderID(str(row[8])) if row[8] is not None else None,
        leg1_filled_quantity=Quantity(Decimal(str(row[9]))),
        leg2_venue_id=VenueID(str(row[10])),
        leg2_contract_id=ContractID(str(row[11])),
        leg2_side=OrderSide(str(row[12])),
        leg2_quantity=Quantity(Decimal(str(row[13]))),
        leg2_limit_price=Price(Decimal(str(row[14]))),
        leg2_client_order_id=ClientOrderID(str(row[15])),
        leg2_order_id=OrderID(str(row[16])) if row[16] is not None else None,
        leg2_filled_quantity=Quantity(Decimal(str(row[17]))),
        residual_quantity=Quantity(Decimal(str(row[18]))),
        portfolio_id=PortfolioID(str(row[19])) if row[19] is not None else None,
        strategy_id=StrategyID(str(row[20])) if row[20] is not None else None,
        last_error=str(row[21]) if row[21] is not None else None,
        created_at=Timestamp(row[22]),
        updated_at=Timestamp(row[23]),
    )
