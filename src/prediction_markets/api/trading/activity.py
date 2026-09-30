"""Build API read models from PostgreSQL journal projections.

Responsibilities
----------------
- Query durable market, opportunity, order, trade, position, and execution views.
- Translate persistence entities into transport models.
"""

from collections import defaultdict
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import json
from threading import Lock
from time import monotonic

import psycopg

from prediction_markets.api.models import (
    ArbitrageOpportunityOut,
    ContractOut,
    ExecutionActivityOut,
    ExecutionJournalOut,
    ExecutionLegOut,
    ExposureRecoveryOut,
    InternalPnlSummaryOut,
    InternalVenuePnlOut,
    InternalVenuePnlV2Out,
    ManualExecutionResolutionOut,
    OrderOut,
    PositionOut,
    MatchPairOut,
    TradeOut,
    PnlPointOut,
    PerformanceViewOut,
    PnlComparabilityOut,
    PnlPerformanceSummaryOut,
)
from prediction_markets.application.execution.accounting import (
    manual_resolution_client_order_id,
    manual_resolution_metadata,
)
from prediction_markets.application.pnl import ConsolidatedPnl
from prediction_markets.domain.shared.value_objects import (
    ClientOrderID,
    ContractID,
    Money,
    OrderID,
)
from prediction_markets.domain.trading.entities import (
    OrderSnapshot,
    Portfolio,
    Position,
    Trade,
)
from prediction_markets.domain.trading.enums import (
    ArbitrageExecutionStatus,
    OrderSide,
    OrderStatus,
    RecoveryStatus,
)
from prediction_markets.domain.trading.entities import ArbitrageExecutionJournal
from prediction_markets.domain.trading.entities import ExposureRecovery
from prediction_markets.infrastructure.postgres.repositories import (
    PostgresArbitrageExecutionJournalRepository,
    PostgresExposureRecoveryRepository,
    PostgresOrderRepository,
    PostgresPositionRepository,
    PostgresTradeRepository,
)

_INTERNAL_PNL_CACHE_SECONDS = 30.0


class TradingActivityReader:
    """Build API read models from the latest persisted execution state."""

    def __init__(self, dsn: str) -> None:
        self._dsn = dsn
        self._internal_pnl_cache: tuple[float, InternalPnlSummaryOut] | None = None
        self._internal_pnl_lock = Lock()

    def orders(
        self,
        *,
        limit: int,
        status: OrderStatus | None = None,
        contract_id: ContractID | None = None,
    ) -> list[OrderOut]:
        with psycopg.connect(self._dsn, connect_timeout=5) as connection:
            values = PostgresOrderRepository(connection).list_recent(
                limit=limit,
                status=status,
                contract_id=contract_id,
            )
        return [_order_out(value) for value in values]

    def market_matches(
        self,
        *,
        underlying: str,
        interval_seconds: int,
    ) -> list[MatchPairOut]:
        """Return the latest SQL-projected pairs for one monitored cycle."""
        with psycopg.connect(self._dsn, connect_timeout=5) as connection:
            rows = connection.execute(
                """
                SELECT
                    left_contract_id, left_market_id, left_outcome_id,
                    left_venue_id, left_symbol,
                    right_contract_id, right_market_id, right_outcome_id,
                    right_venue_id, right_symbol
                FROM matched_contracts
                WHERE underlying = %s AND interval_seconds = %s
                ORDER BY left_venue_id, left_contract_id, right_venue_id, right_contract_id
                """,
                (underlying, interval_seconds),
            ).fetchall()
        return [_match_pair_out(row) for row in rows]

    def market_matches_by_cycle(self) -> dict[tuple[str, int], list[MatchPairOut]]:
        """Return every latest cycle projection in one database read.

        Returns
        -------
        dict[tuple[str, int], list[MatchPairOut]]
            Pairs grouped by underlying and interval in seconds. Cycles without
            persisted matches are absent.
        """
        with psycopg.connect(self._dsn, connect_timeout=5) as connection:
            rows = connection.execute(
                """
                SELECT
                    underlying, interval_seconds,
                    left_contract_id, left_market_id, left_outcome_id,
                    left_venue_id, left_symbol,
                    right_contract_id, right_market_id, right_outcome_id,
                    right_venue_id, right_symbol
                FROM matched_contracts
                ORDER BY underlying, interval_seconds,
                         left_venue_id, left_contract_id,
                         right_venue_id, right_contract_id
                """
            ).fetchall()
        matches: dict[tuple[str, int], list[MatchPairOut]] = {}
        for row in rows:
            matches.setdefault((row[0], row[1]), []).append(_match_pair_out(row[2:]))
        return matches

    def opportunities(
        self,
        *,
        limit: int,
        underlying: str | None = None,
        interval_seconds: int | None = None,
    ) -> list[ArbitrageOpportunityOut]:
        """Return recent SQL-projected opportunities with optional cycle filters."""
        conditions: list[str] = []
        params: list[object] = []
        if underlying is not None:
            conditions.append("underlying = %s")
            params.append(underlying)
        if interval_seconds is not None:
            conditions.append("interval_seconds = %s")
            params.append(interval_seconds)
        where = f" WHERE {' AND '.join(conditions)}" if conditions else ""
        params.append(limit)
        with psycopg.connect(self._dsn, connect_timeout=5) as connection:
            rows = connection.execute(
                """
                SELECT opportunity_id, journal_sequence, monitor_type, monitor_key,
                       underlying, interval_seconds, side, left_contract_id,
                       right_contract_id, left_price, right_price, quantity,
                       gross_edge, net_edge, fee_per_contract, total_fees, skew_ns,
                       detected_at
                FROM arbitrage_opportunities
                """
                + where
                + " ORDER BY journal_sequence DESC LIMIT %s",
                tuple(params),
            ).fetchall()
        return [
            ArbitrageOpportunityOut.model_validate(
                dict(
                    zip(
                        ArbitrageOpportunityOut.model_fields,
                        row,
                        strict=True,
                    ),
                ),
            )
            for row in rows
        ]

    def trades(
        self,
        *,
        limit: int,
        order_id: OrderID | None = None,
        contract_id: ContractID | None = None,
    ) -> list[TradeOut]:
        with psycopg.connect(self._dsn, connect_timeout=5) as connection:
            values = PostgresTradeRepository(connection).list_recent(
                limit=limit,
                order_id=order_id,
                contract_id=contract_id,
            )
        return [_trade_out(value) for value in values]

    def positions(self, *, limit: int, open_only: bool = False) -> list[PositionOut]:
        with psycopg.connect(self._dsn, connect_timeout=5) as connection:
            values = PostgresPositionRepository(connection).list_recent(
                limit=limit,
                open_only=open_only,
            )
        return [_position_out(value) for value in values]

    def journals(
        self,
        *,
        limit: int,
        status: ArbitrageExecutionStatus | None = None,
    ) -> list[ExecutionJournalOut]:
        with psycopg.connect(self._dsn, connect_timeout=5) as connection:
            values = PostgresArbitrageExecutionJournalRepository(
                connection
            ).list_recent(limit=limit, status=status)
            markets = _journal_markets(connection, values)
            latency_traces = _journal_latency_traces(connection, values)
            recoveries = PostgresExposureRecoveryRepository(connection).list_recent(
                limit=limit,
            )
            recovery_by_execution = {
                recovery.execution_id: recovery
                for recovery in recoveries
                if recovery.execution_id is not None
            }
            trades = PostgresTradeRepository(connection).list_by_client_orders(
                _journal_client_order_ids(values, recoveries),
            )
        return [
            _journal_out(
                value,
                markets.get(value.id),
                trades,
                latency_traces.get(value.id),
                recovery_by_execution.get(value.id),
            )
            for value in values
        ]

    def recoveries(
        self,
        *,
        limit: int,
        status: RecoveryStatus | None = None,
    ) -> list[ExposureRecoveryOut]:
        with psycopg.connect(self._dsn, connect_timeout=5) as connection:
            values = PostgresExposureRecoveryRepository(connection).list_recent(
                limit=limit,
                status=status,
            )
        return [_recovery_out(value) for value in values]

    def snapshot(self, *, limit: int) -> ExecutionActivityOut:
        with psycopg.connect(self._dsn, connect_timeout=5) as connection:
            orders = PostgresOrderRepository(connection).list_recent(limit=limit)
            trade_repository = PostgresTradeRepository(connection)
            trades = trade_repository.list_recent(limit=limit)
            positions = PostgresPositionRepository(connection).list_recent(limit=limit)
            journals = PostgresArbitrageExecutionJournalRepository(
                connection
            ).list_recent(limit=limit)
            recoveries = PostgresExposureRecoveryRepository(connection).list_recent(
                limit=limit
            )
            recovery_by_execution = {
                recovery.execution_id: recovery
                for recovery in recoveries
                if recovery.execution_id is not None
            }
            journal_markets = _journal_markets(connection, journals)
            journal_latency_traces = _journal_latency_traces(connection, journals)
            journal_trades = trade_repository.list_by_client_orders(
                _journal_client_order_ids(journals, recoveries),
            )
            trading_fees, gas = self._latest_activity_fees(connection)
        return ExecutionActivityOut(
            generated_at=datetime.now(timezone.utc),
            trading_fees_usd=trading_fees,
            gas_usd=gas,
            orders=[_order_out(value) for value in orders],
            trades=[_trade_out(value) for value in trades],
            positions=[_position_out(value) for value in positions],
            journals=[
                _journal_out(
                    value,
                    journal_markets.get(value.id),
                    journal_trades,
                    journal_latency_traces.get(value.id),
                    recovery_by_execution.get(value.id),
                )
                for value in journals
            ],
            recoveries=[_recovery_out(value) for value in recoveries],
        )

    def _latest_activity_fees(
        self,
        connection: psycopg.Connection,
    ) -> tuple[Decimal, Decimal]:
        row = connection.execute(
            """
            SELECT trading_fees_usd, gas_usd
            FROM pnl_performance_points
            WHERE source = 'bot_ledger' AND venue_id IS NULL
            ORDER BY observed_at DESC, point_id DESC
            LIMIT 1
            """
        ).fetchone()
        if row is None:
            return Decimal("0"), Decimal("0")
        return row[0] or Decimal("0"), row[1] or Decimal("0")

    def internal_pnl(self) -> InternalPnlSummaryOut:
        """Return cached all-time PnL from terminal executions.

        Returns
        -------
        InternalPnlSummaryOut
            Gross PnL, normalized settlement fees, net PnL, cumulative series,
            and venue contributions for terminal bot executions.

        Notes
        -----
        - The result is cached for 30 seconds across API requests.
        - One lock deduplicates concurrent PostgreSQL refreshes.
        """
        cached = self._internal_pnl_cache
        if (
            cached is not None
            and monotonic() - cached[0] < _INTERNAL_PNL_CACHE_SECONDS
        ):
            return cached[1]
        with self._internal_pnl_lock:
            cached = self._internal_pnl_cache
            if (
                cached is not None
                and monotonic() - cached[0] < _INTERNAL_PNL_CACHE_SECONDS
            ):
                return cached[1]
            result = self._load_internal_pnl()
            self._internal_pnl_cache = (monotonic(), result)
            return result

    def record_venue_pnl(self, pnl: ConsolidatedPnl) -> None:
        """Persist venue observations and a global point only when comparable."""
        snapshots = tuple(
            result.snapshot for result in pnl.venues if result.snapshot is not None
        )
        scopes = {snapshot.scope for snapshot in snapshots}
        comparable = (
            not pnl.partial
            and len(snapshots) == len(pnl.venues)
            and None not in scopes
            and len(scopes) == 1
        )
        with psycopg.connect(self._dsn, connect_timeout=5) as connection:
            for result in pnl.venues:
                snapshot = result.snapshot
                if snapshot is None:
                    continue
                flags = [
                    flag
                    for flag, present in (
                        ("STALE", result.stale),
                        ("MISSING_FEES", snapshot.fees_usd is None),
                    )
                    if present
                ]
                _insert_pnl_point(
                    connection,
                    point_id=(
                        f"venue_account:{result.venue_id}:"
                        f"{int(snapshot.observed_at.value.timestamp() * 1_000_000)}"
                    ),
                    source="venue_account",
                    venue_id=str(result.venue_id),
                    observed_at=snapshot.observed_at.value,
                    realized=snapshot.realized_pnl_usd,
                    unrealized=snapshot.unrealized_pnl_usd,
                    fees=snapshot.fees_usd,
                    total=snapshot.total_pnl_usd,
                    scope=snapshot.scope or "unknown",
                    partial=result.stale or result.error is not None,
                    quality_flags=flags,
                )
            if comparable:
                global_flags = (
                    ["MISSING_FEES"] if pnl.fees_usd is None else []
                )
                observed_at = max(
                    snapshot.observed_at.value for snapshot in snapshots
                )
                _insert_pnl_point(
                    connection,
                    point_id=(
                        "venue_account:global:"
                        f"{int(observed_at.timestamp() * 1_000_000)}"
                    ),
                    source="venue_account",
                    venue_id=None,
                    observed_at=observed_at,
                    realized=pnl.realized_pnl_usd,
                    unrealized=pnl.unrealized_pnl_usd,
                    fees=pnl.fees_usd,
                    total=pnl.total_pnl_usd,
                    scope=next(iter(scopes)),
                    partial=bool(global_flags),
                    quality_flags=global_flags,
                )

    def performance_view(
        self,
        *,
        source: str,
        requested_range: str,
        comparable: bool,
        notes: list[str],
        partial: bool,
        range_supported: bool = True,
    ) -> PerformanceViewOut:
        """Read persisted global points and apply one explicit time horizon."""
        effective_range = requested_range if range_supported else "ALL"
        cutoff = _range_cutoff(effective_range)
        with psycopg.connect(self._dsn, connect_timeout=5) as connection:
            rows = connection.execute(
                """
                SELECT observed_at, realized_pnl_usd, unrealized_pnl_usd,
                       fees_usd, gas_usd, total_pnl_usd, partial, quality_flags
                FROM pnl_performance_points
                WHERE source = %s AND venue_id IS NULL
                  AND (%s::timestamptz IS NULL OR observed_at >= %s)
                ORDER BY observed_at, point_id
                """,
                (source, cutoff, cutoff),
            ).fetchall()
            baseline = None
            if cutoff is not None:
                baseline = connection.execute(
                    """
                    SELECT observed_at, realized_pnl_usd, unrealized_pnl_usd,
                           fees_usd, gas_usd, total_pnl_usd, partial, quality_flags
                    FROM pnl_performance_points
                    WHERE source = %s AND venue_id IS NULL AND observed_at < %s
                    ORDER BY observed_at DESC, point_id DESC LIMIT 1
                    """,
                    (source, cutoff),
                ).fetchone()
        if source == "venue_account" and not comparable:
            rows = []
            baseline = None
        latest = rows[-1] if rows else baseline
        flags = list(latest[7] if latest and latest[7] else ())
        if partial and "PARTIAL" not in flags:
            flags.append("PARTIAL")
        if not comparable and "INCONSISTENT_SCOPE" not in flags:
            flags.append("INCONSISTENT_SCOPE")
        baseline_values = (
            baseline[1:6] if baseline is not None else (0, 0, 0, 0, 0)
        )
        series = [
            PnlPointOut(
                observed_at=row[0],
                net_pnl_usd=(
                    row[5] - baseline_values[4]
                    if cutoff is not None
                    and row[5] is not None
                    and baseline_values[4] is not None
                    else row[5]
                ),
            )
            for row in rows
            if row[5] is not None
        ]
        if latest is None:
            summary = PnlPerformanceSummaryOut(
                realized=None,
                unrealized=None,
                total=None,
                fees=None,
                gas=None,
            )
            last_updated = datetime.now(timezone.utc)
        else:
            current = latest[1:6]
            values = tuple(
                (
                    value - baseline_value
                    if cutoff is not None
                    and value is not None
                    and baseline_value is not None
                    else value
                )
                for value, baseline_value in zip(
                    current,
                    baseline_values,
                    strict=True,
                )
            )
            summary = PnlPerformanceSummaryOut(
                realized=values[0],
                unrealized=values[1],
                fees=(
                    values[2] - values[3]
                    if values[2] is not None and values[3] is not None
                    else values[2]
                ),
                gas=values[3],
                total=values[4],
            )
            last_updated = latest[0]
        methodology_note = None
        if not range_supported:
            methodology_note = (
                f"{requested_range} is not supported by the venue scopes; "
                "showing persisted all-time account observations."
            )
        elif cutoff is not None and baseline is None:
            methodology_note = "History begins at the first persisted observation."
        return PerformanceViewOut(
            scope_label=(
                "Bot ledger, journal ordered"
                if source == "bot_ledger"
                else "Venue account snapshots"
            ),
            methodology=(
                "WAC realized PnL plus marked or locked-payout unrealized PnL, "
                "less USD trading fees and gas."
                if source == "bot_ledger"
                else "Values reported by venue account APIs; never merged across scopes."
            ),
            requested_range=requested_range,
            effective_range=effective_range,
            range_supported=range_supported,
            methodology_note=methodology_note,
            summary=summary,
            series=series,
            comparability=PnlComparabilityOut(
                comparable=comparable,
                confidence="high" if comparable and source == "bot_ledger" else "low",
                notes=notes,
            ),
            partial=partial or latest is None or bool(latest[5]),
            last_updated=last_updated,
            quality_flags=flags,
        )

    def ledger_venues(self) -> list[InternalVenuePnlV2Out]:
        """Return current WAC portfolio totals grouped by venue."""
        with psycopg.connect(self._dsn, connect_timeout=5) as connection:
            positions = PostgresPositionRepository(connection).list_recent(limit=10_000)
        grouped: dict[str, list[Portfolio]] = defaultdict(list)
        for portfolio in Portfolio.group_by_venue(positions).values():
            grouped[str(portfolio.venue_id)].append(portfolio)
        rows: list[InternalVenuePnlV2Out] = []
        for venue_id, portfolios in sorted(grouped.items()):
            unrealized_values = tuple(value.unrealized_pnl for value in portfolios)
            fee_values = tuple(value.fees for value in portfolios)
            unrealized = (
                None
                if any(value is None for value in unrealized_values)
                else sum((value for value in unrealized_values if value is not None), Decimal("0"))
            )
            fees = (
                None
                if any(value is None for value in fee_values)
                or len({value.currency for value in fee_values if value is not None}) > 1
                else sum((value.amount for value in fee_values if value is not None), Decimal("0"))
            )
            realized = sum((value.realized_pnl for value in portfolios), Decimal("0"))
            total = (
                realized + unrealized - fees
                if unrealized is not None and fees is not None
                else None
            )
            rows.append(
                InternalVenuePnlV2Out(
                    venue_id=venue_id,
                    realized_pnl_usd=realized,
                    unrealized_pnl_usd=unrealized,
                    fees_usd=fees,
                    total_pnl_usd=total,
                    partial=total is None,
                )
            )
        return rows

    def _load_internal_pnl(self) -> InternalPnlSummaryOut:
        """Calculate all-time PnL from PostgreSQL terminal executions.

        Returns
        -------
        InternalPnlSummaryOut
            Fresh internal PnL derived from normalized persisted fills.

        Notes
        -----
        - Trade identity and fee corrections are already idempotent in the
          persisted ledger.
        - Terminal executions with missing fills or normalized fees are
          reported but excluded from priced totals.
        """
        with psycopg.connect(self._dsn, connect_timeout=5) as connection:
            counts = dict(
                connection.execute(
                    "SELECT status, count(*) FROM arbitrage_execution_journals "
                    "WHERE status IN ('completed', 'recovered') GROUP BY status"
                ).fetchall()
            )
            repository = PostgresArbitrageExecutionJournalRepository(connection)
            completed = repository.list_recent(
                limit=max(int(counts.get("completed", 0)), 1),
                status=ArbitrageExecutionStatus.COMPLETED,
            )
            recovered = repository.list_recent(
                limit=max(int(counts.get("recovered", 0)), 1),
                status=ArbitrageExecutionStatus.RECOVERED,
            )
            journals = completed + recovered
            resolved_count = connection.execute(
                "SELECT count(*) FROM exposure_recoveries "
                "WHERE status = 'resolved'"
            ).fetchone()[0]
            recoveries = PostgresExposureRecoveryRepository(
                connection
            ).list_recent(
                limit=max(int(resolved_count), 1),
                status=RecoveryStatus.RESOLVED,
            )
            recovered_ids = {journal.id for journal in recovered}
            recoveries = tuple(
                recovery
                for recovery in recoveries
                if recovery.execution_id in recovered_ids
            )
            recovery_by_execution = {
                recovery.execution_id: recovery for recovery in recoveries
            }
            trades = PostgresTradeRepository(connection).list_by_client_orders(
                _journal_client_order_ids(journals, recoveries),
            )
        outputs = [
            _journal_out(
                journal,
                None,
                trades,
                None,
                recovery_by_execution.get(journal.id),
            )
            for journal in journals
        ]
        recovery_outputs = [
            _recovery_out(recovery)
            for recovery in recoveries
            if recovery.execution_id in recovered_ids
        ]
        return _internal_pnl_out(
            outputs,
            recovery_outputs,
            terminal_execution_count=len(completed) + len(recovered),
        )


def _insert_pnl_point(
    connection: psycopg.Connection,
    *,
    point_id: str,
    source: str,
    venue_id: str | None,
    observed_at: datetime,
    realized: Decimal | None,
    unrealized: Decimal | None,
    fees: Decimal | None,
    total: Decimal | None,
    scope: str,
    partial: bool,
    quality_flags: list[str],
) -> None:
    """Insert one idempotent historical performance observation."""
    connection.execute(
        """
        INSERT INTO pnl_performance_points (
            point_id, source, venue_id, observed_at, realized_pnl_usd,
            unrealized_pnl_usd, fees_usd, total_pnl_usd, scope,
            partial, quality_flags
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
        ON CONFLICT (point_id) DO UPDATE SET
            realized_pnl_usd = EXCLUDED.realized_pnl_usd,
            unrealized_pnl_usd = EXCLUDED.unrealized_pnl_usd,
            fees_usd = EXCLUDED.fees_usd,
            total_pnl_usd = EXCLUDED.total_pnl_usd,
            scope = EXCLUDED.scope,
            partial = EXCLUDED.partial,
            quality_flags = EXCLUDED.quality_flags
        """,
        (
            point_id,
            source,
            venue_id,
            observed_at,
            realized,
            unrealized,
            fees,
            total,
            scope,
            partial,
            json.dumps(quality_flags, separators=(",", ":")),
        ),
    )


def _range_cutoff(value: str) -> datetime | None:
    """Return the UTC lower bound for a dashboard range."""
    now = datetime.now(timezone.utc)
    if value == "ALL":
        return None
    if value == "YTD":
        return datetime(now.year, 1, 1, tzinfo=timezone.utc)
    days = {"1D": 1, "1W": 7, "1M": 30, "3M": 90, "1Y": 365}
    try:
        return now - timedelta(days=days[value])
    except KeyError as error:
        raise ValueError(f"Unsupported PnL range: {value}") from error


def _order_out(order: OrderSnapshot) -> OrderOut:
    return OrderOut(
        status=order.status.value,
        contract_id=str(order.contract_id),
        side=order.side.value,
        quantity=order.quantity.value,
        order_type=order.order_type.value,
        client_order_id=str(order.client_order_id) if order.client_order_id else None,
        order_id=str(order.order_id) if order.order_id else None,
        limit_price=order.limit_price.value if order.limit_price else None,
        filled_quantity=order.filled_quantity.value,
        average_price=order.average_price.value if order.average_price else None,
        created_at=order.created_at.value if order.created_at else None,
        updated_at=order.updated_at.value if order.updated_at else None,
    )


def _trade_out(trade: Trade) -> TradeOut:
    return TradeOut(
        trade_id=str(trade.id),
        order_id=str(trade.order_id) if trade.order_id else None,
        client_order_id=str(trade.client_order_id) if trade.client_order_id else None,
        contract_id=str(trade.contract_id),
        side=trade.side.value,
        quantity=trade.quantity.value,
        price=trade.price.value,
        executed_at=trade.executed_at.value,
        portfolio_id=str(trade.portfolio_id) if trade.portfolio_id else None,
        strategy_id=str(trade.strategy_id) if trade.strategy_id else None,
        fee_amount=trade.fee.amount if trade.fee else None,
        fee_currency=str(trade.fee.currency) if trade.fee else None,
        fee_settlement_amount=(
            trade.fee_settlement_cost.amount
            if trade.fee_settlement_cost
            else None
        ),
        fee_settlement_currency=(
            str(trade.fee_settlement_cost.currency)
            if trade.fee_settlement_cost
            else None
        ),
        venue_id=str(trade.venue_id),
    )


def _position_out(position: Position) -> PositionOut:
    return PositionOut(
        position_id=str(position.id),
        contract_id=str(position.contract_id),
        side=position.side.value,
        quantity=position.quantity.value,
        average_entry_price=(
            position.average_entry_price.value if position.average_entry_price else None
        ),
        portfolio_id=str(position.portfolio_id) if position.portfolio_id else None,
        opened_at=position.opened_at.value if position.opened_at else None,
        updated_at=position.updated_at.value if position.updated_at else None,
        venue_id=str(position.venue_id),
        current_price=position.current_price.value if position.current_price else None,
        realized_pnl=position.realized_pnl,
        fee_settlement_amount=position.fees.amount if position.fees else None,
        fee_settlement_currency=str(position.fees.currency) if position.fees else None,
        quality_flags=position.quality_flags,
    )


def _recovery_out(recovery: ExposureRecovery) -> ExposureRecoveryOut:
    return ExposureRecoveryOut(
        recovery_id=recovery.id,
        execution_id=recovery.execution_id,
        route=recovery.route.value if recovery.route else None,
        venue_id=str(recovery.venue_id),
        contract_id=str(recovery.contract_id),
        side=recovery.side.value,
        quantity=recovery.quantity.value,
        filled_quantity=recovery.filled_quantity.value,
        limit_price=recovery.limit_price.value,
        average_price=(
            recovery.average_price.value if recovery.average_price else None
        ),
        source_contract_id=(
            str(recovery.source_contract_id) if recovery.source_contract_id else None
        ),
        source_side=recovery.source_side.value if recovery.source_side else None,
        source_price=(recovery.source_price.value if recovery.source_price else None),
        source_fee_amount=(
            recovery.source_fee.amount if recovery.source_fee else None
        ),
        source_fee_currency=(
            str(recovery.source_fee.currency) if recovery.source_fee else None
        ),
        estimated_vwap=(
            recovery.estimated_vwap.value if recovery.estimated_vwap else None
        ),
        estimated_recovery_fee_amount=(
            recovery.estimated_recovery_fee.amount
            if recovery.estimated_recovery_fee
            else None
        ),
        estimated_recovery_fee_currency=(
            str(recovery.estimated_recovery_fee.currency)
            if recovery.estimated_recovery_fee
            else None
        ),
        estimated_gross_result=recovery.estimated_gross_result,
        estimated_net_result=recovery.estimated_net_result,
        recovery_fee_amount=(
            recovery.recovery_fee.amount if recovery.recovery_fee else None
        ),
        recovery_fee_currency=(
            str(recovery.recovery_fee.currency) if recovery.recovery_fee else None
        ),
        actual_gross_result=recovery.actual_gross_result,
        actual_net_result=recovery.actual_net_result,
        portfolio_id=str(recovery.portfolio_id) if recovery.portfolio_id else None,
        strategy_id=str(recovery.strategy_id) if recovery.strategy_id else None,
        status=recovery.status.value,
        attempts=recovery.attempts,
        client_order_id=(
            str(recovery.client_order_id) if recovery.client_order_id else None
        ),
        order_id=str(recovery.order_id) if recovery.order_id else None,
        last_error=recovery.last_error,
        created_at=recovery.created_at.value,
        updated_at=recovery.updated_at.value,
    )


def _journal_markets(
    connection: psycopg.Connection,
    journals: tuple[ArbitrageExecutionJournal, ...],
) -> dict[str, tuple[str, str, str | None, int | None]]:
    """Load projected market identities for execution journals in one query.

    Parameters
    ----------
    connection
        Open PostgreSQL connection used by the surrounding read operation.
    journals
        Execution journals whose identifiers also identify their opportunities.

    Returns
    -------
    dict[str, tuple[str, str, str | None, int | None]]
        Market type, stable key, optional underlying, and optional interval by
        execution identifier.
    """
    if not journals:
        return {}
    rows = connection.execute(
        """
        SELECT opportunity_id, monitor_type, monitor_key,
               underlying, interval_seconds
        FROM arbitrage_opportunities
        WHERE opportunity_id = ANY(%s)
        """,
        ([journal.id for journal in journals],),
    ).fetchall()
    return {row[0]: (row[1], row[2], row[3], row[4]) for row in rows}


def _journal_latency_traces(
    connection: psycopg.Connection,
    journals: tuple[ArbitrageExecutionJournal, ...],
) -> dict[str, dict[str, object]]:
    """Load terminal latency traces for execution journals in one query.

    Parameters
    ----------
    connection
        Open PostgreSQL connection used by the surrounding read operation.
    journals
        Execution journals whose persisted traces should be loaded.

    Returns
    -------
    dict[str, dict[str, object]]
        JSON latency trace indexed by execution identifier.
    """
    if not journals:
        return {}
    rows = connection.execute(
        "SELECT execution_id, latency_trace "
        "FROM arbitrage_execution_journals WHERE execution_id = ANY(%s)",
        ([journal.id for journal in journals],),
    ).fetchall()
    return {
        execution_id: trace
        for execution_id, trace in rows
        if isinstance(trace, dict)
    }


def _journal_out(
    journal: ArbitrageExecutionJournal,
    market: tuple[str, str, str | None, int | None] | None = None,
    trades: tuple[Trade, ...] = (),
    latency_trace: dict[str, object] | None = None,
    recovery: ExposureRecovery | None = None,
) -> ExecutionJournalOut:
    """Map one journal and calculate its terminal end-to-end PnL.

    Parameters
    ----------
    journal
        Durable two-leg execution state.
    market
        Optional projected monitor identity associated with the execution.
    trades
        Actual normalized fills and fees available for the journal legs.
    latency_trace
        Persisted terminal latency breakdown for this execution, when present.
    recovery
        Resolved recovery associated with the execution, when applicable.

    Returns
    -------
    ExecutionJournalOut
        API view with per-leg actuals and full terminal gross, fees, and net PnL.
    """
    leg1_average, leg1_fee, leg1_settlement = _leg_financials(
        journal.leg1_client_order_id,
        trades,
    )
    leg2_average, leg2_fee, leg2_settlement = _leg_financials(
        journal.leg2_client_order_id,
        trades,
    )
    terminal_ids = {
        journal.leg1_client_order_id,
        journal.leg2_client_order_id,
        manual_resolution_client_order_id(journal.id),
    }
    if recovery is not None and recovery.client_order_id is not None:
        terminal_ids.add(recovery.client_order_id)
    terminal_trades = tuple(
        trade for trade in trades if trade.client_order_id in terminal_ids
    )
    gross_pnl, total_fee, net_pnl = _terminal_execution_financials(
        journal,
        recovery,
        terminal_trades,
    )
    return ExecutionJournalOut(
        execution_id=journal.id,
        monitor_type=market[0] if market else None,
        monitor_key=market[1] if market else None,
        underlying=market[2] if market else None,
        interval_seconds=market[3] if market else None,
        status=journal.status.value,
        leg1=ExecutionLegOut(
            venue_id=str(journal.leg1_venue_id),
            contract_id=str(journal.leg1_contract_id),
            side=journal.leg1_side.value,
            quantity=journal.leg1_quantity.value,
            limit_price=journal.leg1_limit_price.value,
            client_order_id=str(journal.leg1_client_order_id),
            order_id=str(journal.leg1_order_id) if journal.leg1_order_id else None,
            filled_quantity=journal.leg1_filled_quantity.value,
            average_fill_price=leg1_average,
            fee_amount=leg1_fee.amount if leg1_fee else None,
            fee_currency=str(leg1_fee.currency) if leg1_fee else None,
            fee_settlement_amount=(
                leg1_settlement.amount if leg1_settlement else None
            ),
            fee_settlement_currency=(
                str(leg1_settlement.currency) if leg1_settlement else None
            ),
        ),
        leg2=ExecutionLegOut(
            venue_id=str(journal.leg2_venue_id),
            contract_id=str(journal.leg2_contract_id),
            side=journal.leg2_side.value,
            quantity=journal.leg2_quantity.value,
            limit_price=journal.leg2_limit_price.value,
            client_order_id=str(journal.leg2_client_order_id),
            order_id=str(journal.leg2_order_id) if journal.leg2_order_id else None,
            filled_quantity=journal.leg2_filled_quantity.value,
            average_fill_price=leg2_average,
            fee_amount=leg2_fee.amount if leg2_fee else None,
            fee_currency=str(leg2_fee.currency) if leg2_fee else None,
            fee_settlement_amount=(
                leg2_settlement.amount if leg2_settlement else None
            ),
            fee_settlement_currency=(
                str(leg2_settlement.currency) if leg2_settlement else None
            ),
        ),
        residual_quantity=journal.residual_quantity.value,
        gross_locked_pnl_usd=gross_pnl,
        total_fee_settlement_cost_usd=total_fee,
        net_locked_pnl_usd=net_pnl,
        manual_resolution=_manual_resolution_out(journal.id, trades),
        latency_trace=latency_trace,
        portfolio_id=str(journal.portfolio_id) if journal.portfolio_id else None,
        strategy_id=str(journal.strategy_id) if journal.strategy_id else None,
        last_error=journal.last_error,
        created_at=journal.created_at.value,
        updated_at=journal.updated_at.value,
    )


def _manual_resolution_out(
    execution_id: str,
    trades: tuple[Trade, ...],
) -> ManualExecutionResolutionOut | None:
    """Map the deterministic operator trade attached to one execution.

    Parameters
    ----------
    execution_id : str
        Parent execution identity used by the manual client-order identifier.
    trades : tuple[Trade, ...]
        Fills loaded for the execution lifecycle.

    Returns
    -------
    ManualExecutionResolutionOut | None
        Operator-entered economics, or ``None`` when no valid manual trade is
        attached to the execution.
    """
    client_order_id = manual_resolution_client_order_id(execution_id)
    trade = next(
        (value for value in trades if value.client_order_id == client_order_id),
        None,
    )
    metadata = manual_resolution_metadata(trade) if trade is not None else None
    if trade is None or metadata is None:
        return None
    method, external_reference = metadata
    fee = trade.fee_settlement_cost
    return ManualExecutionResolutionOut(
        execution_id=execution_id,
        method=method,
        venue_id=str(trade.venue_id),
        contract_id=str(trade.contract_id),
        side=trade.side.value,
        quantity=trade.quantity.value,
        price=trade.price.value,
        fee_amount_usd=(
            fee.amount
            if fee is not None and str(fee.currency) == "USD"
            else Decimal("0")
        ),
        executed_at=trade.executed_at.value,
        external_reference=external_reference,
    )


def _match_pair_out(row: tuple[object, ...]) -> MatchPairOut:
    """Map one SQL projection row to a venue contract pair."""
    return MatchPairOut(
        left=ContractOut(
            id=row[0],
            market_id=row[1],
            outcome_id=row[2],
            venue_id=row[3],
            symbol=row[4],
        ),
        right=ContractOut(
            id=row[5],
            market_id=row[6],
            outcome_id=row[7],
            venue_id=row[8],
            symbol=row[9],
        ),
    )


def _internal_pnl_out(
    journals: list[ExecutionJournalOut],
    recoveries: list[ExposureRecoveryOut] | tuple[ExposureRecoveryOut, ...] = (),
    *,
    terminal_execution_count: int | None = None,
) -> InternalPnlSummaryOut:
    """Aggregate fee-normalized terminal execution PnL.

    Parameters
    ----------
    journals
        Completed and recovered journal views enriched with every lifecycle fill.
    recoveries
        Resolved recovery views for executions that did not complete normally.
    terminal_execution_count
        Total completed and recovered execution count, including records whose
        financial details are incomplete.

    Returns
    -------
    InternalPnlSummaryOut
        All-time priced totals, venue contributions, and cumulative series.
    """
    priced = [
        journal
        for journal in journals
        if journal.status
        in {
            ArbitrageExecutionStatus.COMPLETED.value,
            ArbitrageExecutionStatus.RECOVERED.value,
        }
        and journal.gross_locked_pnl_usd is not None
        and journal.total_fee_settlement_cost_usd is not None
        and journal.net_locked_pnl_usd is not None
    ]
    recovery_by_execution = {
        recovery.execution_id: recovery
        for recovery in recoveries
        if recovery.execution_id is not None
    }
    gross_by_venue: defaultdict[str, Decimal] = defaultdict(lambda: Decimal("0"))
    fees_by_venue: defaultdict[str, Decimal] = defaultdict(lambda: Decimal("0"))
    executions_by_venue: defaultdict[str, int] = defaultdict(int)
    pnl_events = [
        (journal.updated_at, journal.execution_id, journal.net_locked_pnl_usd)
        for journal in priced
    ]
    for journal in priced:
        recovery = recovery_by_execution.get(journal.execution_id)
        for venue_id in {journal.leg1.venue_id, journal.leg2.venue_id}:
            executions_by_venue[venue_id] += 1
        for leg in (journal.leg1, journal.leg2):
            if leg.filled_quantity == 0:
                continue
            source_recovery = (
                recovery
                if recovery is not None
                and recovery.source_contract_id == leg.contract_id
                else None
            )
            average_fill_price = (
                leg.average_fill_price
                if leg.average_fill_price is not None
                else (
                    source_recovery.source_price
                    if source_recovery is not None
                    else None
                )
            )
            fee = (
                leg.fee_settlement_amount
                if leg.fee_settlement_amount is not None
                else (
                    source_recovery.source_fee_amount
                    if source_recovery is not None
                    else None
                )
            )
            assert average_fill_price is not None
            assert fee is not None
            direction = (
                Decimal("1")
                if leg.side == OrderSide.BUY.value
                else Decimal("-1")
            )
            gross_by_venue[leg.venue_id] -= (
                direction * leg.filled_quantity * average_fill_price
            )
            fees_by_venue[leg.venue_id] += fee
        signed_pair_quantity = (
            journal.leg1.filled_quantity
            if journal.leg1.side == OrderSide.BUY.value
            else -journal.leg1.filled_quantity
        )
        if recovery is not None:
            assert recovery.average_price is not None
            assert recovery.recovery_fee_amount is not None
            recovery_direction = (
                Decimal("1")
                if recovery.side == OrderSide.BUY.value
                else Decimal("-1")
            )
            if recovery.contract_id == journal.leg1.contract_id:
                signed_pair_quantity += recovery_direction * recovery.filled_quantity
            gross_by_venue[recovery.venue_id] -= (
                recovery_direction
                * recovery.filled_quantity
                * recovery.average_price
            )
            fees_by_venue[recovery.venue_id] += recovery.recovery_fee_amount
        pair_share = signed_pair_quantity / 2
        gross_by_venue[journal.leg1.venue_id] += pair_share
        gross_by_venue[journal.leg2.venue_id] += pair_share

    cumulative = Decimal("0")
    series: list[PnlPointOut] = []
    for observed_at, _event_id, net_pnl in sorted(pnl_events):
        assert net_pnl is not None
        cumulative += net_pnl
        series.append(
            PnlPointOut(
                observed_at=observed_at,
                net_pnl_usd=cumulative,
            )
        )
    venues = [
        InternalVenuePnlOut(
            venue_id=venue_id,
            gross_contribution_usd=gross_by_venue[venue_id],
            fees_usd=fees_by_venue[venue_id],
            net_contribution_usd=(
                gross_by_venue[venue_id] - fees_by_venue[venue_id]
            ),
            execution_count=executions_by_venue[venue_id],
        )
        for venue_id in sorted(executions_by_venue)
    ]
    gross = sum(gross_by_venue.values(), Decimal("0"))
    fees = sum(fees_by_venue.values(), Decimal("0"))
    terminal_count = terminal_execution_count
    if terminal_count is None:
        terminal_count = len(journals) + len(recoveries)
    return InternalPnlSummaryOut(
        gross_pnl_usd=gross,
        fees_usd=fees,
        net_pnl_usd=gross - fees,
        priced_terminal_executions=len(priced),
        unpriced_terminal_executions=max(
            terminal_count - len(priced),
            0,
        ),
        venues=venues,
        series=series,
    )


def _journal_client_order_ids(
    journals: tuple[ArbitrageExecutionJournal, ...],
    recoveries: tuple[ExposureRecovery, ...] = (),
) -> tuple[ClientOrderID, ...]:
    """Return distinct execution and recovery identifiers for trade lookup.

    Parameters
    ----------
    journals
        Execution journals whose two legs should be queried.
    recoveries
        Recovery orders whose fills complete a terminal execution.

    Returns
    -------
    tuple[ClientOrderID, ...]
        Stable identifiers with duplicates removed in journal order.
    """
    return tuple(
        dict.fromkeys(
            client_order_id
            for journal in journals
            for client_order_id in (
                journal.leg1_client_order_id,
                journal.leg2_client_order_id,
                manual_resolution_client_order_id(journal.id),
            )
        )
        | dict.fromkeys(
            recovery.client_order_id
            for recovery in recoveries
            if recovery.client_order_id is not None
        ),
    )


def _terminal_execution_financials(
    journal: ArbitrageExecutionJournal,
    recovery: ExposureRecovery | None,
    trades: tuple[Trade, ...],
) -> tuple[Decimal | None, Decimal | None, Decimal | None]:
    """Calculate one terminal execution from every initial and recovery fill.

    Parameters
    ----------
    journal
        Terminal two-leg execution state.
    recovery
        Resolved recovery needed by recovered executions.
    trades
        All normalized fills belonging to the execution lifecycle.

    Returns
    -------
    tuple[Decimal | None, Decimal | None, Decimal | None]
        Gross locked result, normalized USD fees, and net result. Values are
        unavailable when the terminal fills do not form neutral exposure or
        when a fee lacks a USD settlement cost.
    """
    if journal.status not in {
        ArbitrageExecutionStatus.COMPLETED,
        ArbitrageExecutionStatus.RECOVERED,
    } or journal.residual_quantity.value != 0:
        return None, None, None
    if journal.status is ArbitrageExecutionStatus.RECOVERED and (
        recovery is None or recovery.status is not RecoveryStatus.RESOLVED
    ):
        return None, None, None
    recovery_fallback: tuple[Decimal, Decimal, Decimal] | None = None
    if (
        journal.status is ArbitrageExecutionStatus.RECOVERED
        and recovery is not None
        and min(
            journal.leg1_filled_quantity.value,
            journal.leg2_filled_quantity.value,
        )
        == 0
        and recovery.actual_gross_result is not None
        and recovery.actual_net_result is not None
        and recovery.source_fee is not None
        and recovery.recovery_fee is not None
        and str(recovery.source_fee.currency) == "USD"
        and str(recovery.recovery_fee.currency) == "USD"
    ):
        recovery_fallback = (
            recovery.actual_gross_result,
            recovery.actual_gross_result - recovery.actual_net_result,
            recovery.actual_net_result,
        )
    quantities: defaultdict[ContractID, Decimal] = defaultdict(lambda: Decimal("0"))
    cashflow = Decimal("0")
    for trade in trades:
        direction = Decimal("1") if trade.side is OrderSide.BUY else Decimal("-1")
        quantities[trade.contract_id] += direction * trade.quantity.value
        cashflow -= direction * trade.quantity.value * trade.price.value
    leg1_quantity = quantities[journal.leg1_contract_id]
    leg2_quantity = quantities[journal.leg2_contract_id]
    if not trades or leg1_quantity != leg2_quantity:
        return recovery_fallback or (None, None, None)
    gross = cashflow + leg1_quantity
    settlement_fees = tuple(trade.fee_settlement_cost for trade in trades)
    if any(
        fee is None or str(fee.currency) != "USD" for fee in settlement_fees
    ):
        return recovery_fallback or (gross, None, None)
    fees = sum(
        (fee.amount for fee in settlement_fees if fee is not None),
        Decimal("0"),
    )
    return gross, fees, gross - fees


def _leg_financials(
    client_order_id: ClientOrderID,
    trades: tuple[Trade, ...],
) -> tuple[Decimal | None, Money | None, Money | None]:
    """Aggregate actual fill price and venue fee for one execution leg.

    Parameters
    ----------
    client_order_id
        Durable identifier shared by the command and its fills.
    trades
        Candidate normalized trades from the journal batch.

    Returns
    -------
    tuple[Decimal | None, Money | None, Money | None]
        Volume-weighted fill price, charged venue fee, and normalized settlement
        cost. Each unavailable or mixed-currency value is ``None``.
    """
    matching = tuple(
        trade for trade in trades if trade.client_order_id == client_order_id
    )
    quantity = sum((trade.quantity.value for trade in matching), Decimal("0"))
    average = (
        sum(
            (trade.quantity.value * trade.price.value for trade in matching),
            Decimal("0"),
        )
        / quantity
        if quantity > 0
        else None
    )
    return (
        average,
        _sum_money(tuple(trade.fee for trade in matching if trade.fee is not None)),
        _sum_money(
            tuple(
                trade.fee_settlement_cost
                for trade in matching
                if trade.fee_settlement_cost is not None
            ),
        ),
    )


def _sum_money(values: tuple[Money, ...]) -> Money | None:
    """Sum money values only when they share one currency.

    Parameters
    ----------
    values
        Money values to combine.

    Returns
    -------
    Money | None
        Combined amount, or ``None`` for empty or mixed-currency input.
    """
    currencies = {value.currency for value in values}
    if len(currencies) != 1:
        return None
    return Money(
        sum((value.amount for value in values), Decimal("0")),
        values[0].currency,
    )
