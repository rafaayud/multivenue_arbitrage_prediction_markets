"""Verify PostgreSQL projection remains behind journal durability."""

from decimal import Decimal
import time
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest

from prediction_markets.application.events import (
    ExecutionUpdated,
    MarketMatchesUpdated,
    TradeRecorded,
)
from prediction_markets.application.markets.models import MarketCycle
from prediction_markets.domain.alerting.entities import NotificationSnapshot
from prediction_markets.domain.alerting.enums import NotificationState, Severity
from prediction_markets.domain.alerting.ports import AlertingPort
from prediction_markets.domain.alerting.value_objects import Fingerprint
from prediction_markets.domain.market_matching.value_objects import (
    RegularCandidate,
    Underlying,
)
from prediction_markets.domain.markets.entities import Market, MarketSide
from prediction_markets.domain.markets.enums import BinaryOutcome, MarketStatus
from prediction_markets.domain.markets.value_objects import MarketState
from prediction_markets.domain.shared.value_objects import (
    ContractID,
    ClientOrderID,
    Currency,
    MarketID,
    Money,
    OutcomeID,
    Price,
    Quantity,
    Timestamp,
    TradeID,
    VenueID,
)
from prediction_markets.domain.trading.entities import Trade
from prediction_markets.domain.trading.enums import (
    ArbitrageExecutionStatus,
    OrderSide,
    RecoveryRoute,
    RecoveryStatus,
)
from prediction_markets.domain.trading.entities import ExposureRecovery
from prediction_markets.infrastructure.binary_journal import BinaryJournal, JournalEntry
from prediction_markets.infrastructure.postgres.projector import (
    PostgresProjector,
    _project_entry,
    _project_bot_pnl,
    _project_matches,
    _project_recovery,
    _project_trade,
    _summary,
    _validate_checkpoint,
)


class _Result:
    def fetchone(self):
        return None


class _Cursor:
    def __init__(
        self,
        statements: list[str],
        executions: list[tuple[str, object]],
    ) -> None:
        self.statements = statements
        self.executions = executions

    def execute(self, statement, params=None) -> None:
        self.statements.append(statement)
        self.executions.append((statement, params))

    def close(self) -> None:
        pass


class _Connection:
    def __init__(self) -> None:
        self.statements: list[str] = []
        self.executions: list[tuple[str, object]] = []

    def __enter__(self):
        return self

    def __exit__(self, *exc_info) -> None:
        pass

    def execute(self, statement, params=None) -> _Result:
        return _Result()

    def cursor(self) -> _Cursor:
        return _Cursor(self.statements, self.executions)

    def commit(self) -> None:
        pass

    def rollback(self) -> None:
        pass


class _PnlCursor:
    def __init__(self, locked_adjustment: Decimal = Decimal("0")) -> None:
        self.executions: list[tuple[str, object]] = []
        self.rows = [
            (
                Decimal("2"),
                Decimal("1"),
                Decimal("0.5"),
                Decimal("0.4"),
                False,
                False,
                False,
            ),
            (locked_adjustment,),
            (1, Decimal("0.1"), False, False),
        ]

    def execute(self, statement, params=None):
        self.executions.append((statement, params))
        return self

    def fetchone(self):
        return self.rows.pop(0)


def test_projector_rejects_checkpoint_ahead_of_journal(tmp_path) -> None:
    """Do not serve read models produced from a different journal."""
    journal = BinaryJournal(tmp_path / "events.log")
    try:
        with pytest.raises(RuntimeError, match="ahead of the journal"):
            _validate_checkpoint(1, journal)
    finally:
        journal.close()


def test_terminal_execution_projects_latency_trace() -> None:
    """Store the terminal structured trace beside the execution journal."""
    cursor = _Cursor([], [])
    execution = SimpleNamespace(
        id="execution-1",
        status=SimpleNamespace(value="completed"),
        resolution_method=None,
        leg1_decision=None,
        leg2_decision=None,
    )
    event = ExecutionUpdated(
        execution,  # type: ignore[arg-type]
        '{"execution_id":"execution-1","outcome":"terminal"}',
    )
    entry = JournalEntry(1, Timestamp.now(), event, b"")

    with patch(
        "prediction_markets.infrastructure.postgres.projector._project_execution",
    ), patch(
        "prediction_markets.infrastructure.postgres.projector._project_bot_pnl",
    ):
        _project_entry(cursor, entry)

    statement, params = cursor.executions[-1]
    assert "SET latency_trace = %s::jsonb" in statement
    assert params == (event.latency_trace_json, "execution-1")


def test_execution_alert_is_dispatched_through_inbound_port() -> None:
    """Send journal-derived alerting work through the inbound port."""
    cursor = _Cursor([], [])
    now = Timestamp.now()
    execution = SimpleNamespace(
        id="execution-1",
        status=ArbitrageExecutionStatus.NEEDS_REVIEW,
        resolution_method=None,
        last_error="Residual exposure",
        updated_at=now,
        leg1_decision=None,
        leg2_decision=None,
    )
    entry = JournalEntry(7, now, ExecutionUpdated(execution), b"")
    alerting = Mock(spec=AlertingPort)

    with patch(
        "prediction_markets.infrastructure.postgres.projector._project_execution",
    ):
        _project_entry(cursor, entry, alerting)

    args = alerting.report_incident.call_args.args
    assert args[0].value == "execution-risk:execution-1"
    assert args[1].value == "execution-risk:execution-1"
    assert args[2] is Severity.CRITICAL


def test_completed_execution_requests_ephemeral_notification() -> None:
    """Send a one-shot trade-completed notification without opening an incident."""
    cursor = _Cursor([], [])
    now = Timestamp.now()
    execution = SimpleNamespace(
        id="execution-1",
        status=ArbitrageExecutionStatus.COMPLETED,
        resolution_method=None,
        leg1_filled_quantity=Quantity(Decimal("1")),
        leg2_filled_quantity=Quantity(Decimal("1")),
        last_error=None,
        updated_at=now,
        leg1_decision=None,
        leg2_decision=None,
    )
    entry = JournalEntry(8, now, ExecutionUpdated(execution), b"")
    alerting = Mock(spec=AlertingPort)

    with patch(
        "prediction_markets.infrastructure.postgres.projector._project_execution",
    ), patch(
        "prediction_markets.infrastructure.postgres.projector._project_bot_pnl",
    ):
        _project_entry(cursor, entry, alerting)

    alerting.resolve_incident.assert_called_once()
    alerting.request_notifications.assert_called_once()
    alerting.report_incident.assert_not_called()

    incident_id, snapshot, recipients, requested_at = (
        alerting.request_notifications.call_args.args
    )
    assert incident_id is None
    assert recipients is None
    assert requested_at is now
    assert isinstance(snapshot, NotificationSnapshot)
    assert snapshot.fingerprint == Fingerprint("trade-completed:execution-1")
    assert snapshot.state is NotificationState.FIRING
    assert snapshot.severity is Severity.INFORMATIONAL
    assert snapshot.starts_at is now
    assert snapshot.ends_at is now
    assert snapshot.title == "Trade completed"


def test_rejected_execution_does_not_notify() -> None:
    """Close execution-risk without enqueueing a trade-rejected notification."""
    cursor = _Cursor([], [])
    now = Timestamp.now()
    execution = SimpleNamespace(
        id="execution-1",
        status=ArbitrageExecutionStatus.REJECTED,
        resolution_method=None,
        last_error="pre-submission guard rejected the order",
        updated_at=now,
        leg1_decision=None,
        leg2_decision=None,
    )
    entry = JournalEntry(9, now, ExecutionUpdated(execution), b"")
    alerting = Mock(spec=AlertingPort)

    with patch(
        "prediction_markets.infrastructure.postgres.projector._project_execution",
    ):
        _project_entry(cursor, entry, alerting)

    alerting.resolve_incident.assert_called_once()
    alerting.request_notifications.assert_not_called()
    alerting.report_incident.assert_not_called()


def test_projector_waits_for_fdatasync_cursor(tmp_path) -> None:
    """Do not expose page-cache-only events through PostgreSQL."""
    journal = BinaryJournal(tmp_path / "events.log")
    journal.append(
        MarketMatchesUpdated(MarketCycle(Underlying("BTC"), 300), ()),
    )
    connection = _Connection()
    projector = PostgresProjector(
        "postgresql://unused",
        journal,
        retry_seconds=0.01,
        connect=lambda *args, **kwargs: connection,
    )
    projector.start()

    time.sleep(0.03)
    assert projector.projected_sequence == 0

    journal.sync()
    assert projector.drain(1, timeout=1)
    assert projector.projected_sequence == 1
    assert any("projected_events" in statement for statement in connection.statements)

    projector.close()
    journal.close()


def test_regular_candidate_projects_without_cycle_metadata() -> None:
    """Use the candidate key when no underlying or interval exists."""
    closes_at = Timestamp.from_iso("2026-08-07T12:00:00Z")
    candidate = RegularCandidate(
        tuple(
            Market(
                id=MarketID(market_id),
                venue_id=VenueID(venue_id),
                title="Regular market",
                state=MarketState(MarketStatus.ACTIVE, close_time=closes_at),
                yes_side=MarketSide(OutcomeID(f"{market_id}:yes"), BinaryOutcome.YES),
                no_side=MarketSide(OutcomeID(f"{market_id}:no"), BinaryOutcome.NO),
            )
            for venue_id, market_id in (
                ("POLYMARKET", "condition-1"),
                ("LIMITLESS", "market-1"),
            )
        ),
    )
    event = MarketMatchesUpdated(candidate, ())
    statements: list[str] = []
    executions: list[tuple[str, object]] = []

    _project_matches(_Cursor(statements, executions), 7, event)

    assert executions[0][1] == (
        "regular:(('LIMITLESS', 'market-1'), ('POLYMARKET', 'condition-1'))",
    )
    assert _summary(event) == {
        "monitor_type": "regular",
        "monitor_key": (
            "regular:(('LIMITLESS', 'market-1'), "
            "('POLYMARKET', 'condition-1'))"
        ),
        "underlying": None,
        "interval_seconds": None,
        "pairs": 0,
    }


def test_match_projection_retains_binary_contract_complements() -> None:
    """Keep both venue complements after the current match is replaced."""
    now = Timestamp.now()
    left_no = SimpleNamespace(
        id=ContractID("left:no"),
        market_id=MarketID("left-market"),
        outcome_id=OutcomeID("no"),
        venue_id=VenueID("LEFT"),
        symbol=None,
    )
    left_yes = SimpleNamespace(
        id=ContractID("left:yes"),
        market_id=MarketID("left-market"),
        outcome_id=OutcomeID("yes"),
        venue_id=VenueID("LEFT"),
        symbol=None,
    )
    right_no = SimpleNamespace(
        id=ContractID("right:no"),
        market_id=MarketID("right-market"),
        outcome_id=OutcomeID("no"),
        venue_id=VenueID("RIGHT"),
        symbol=None,
    )
    right_yes = SimpleNamespace(
        id=ContractID("right:yes"),
        market_id=MarketID("right-market"),
        outcome_id=OutcomeID("yes"),
        venue_id=VenueID("RIGHT"),
        symbol=None,
    )
    event = MarketMatchesUpdated(
        MarketCycle(Underlying("BTC"), 3600),
        (
            SimpleNamespace(left=left_no, right=right_yes, ends_at=now),
            SimpleNamespace(left=left_yes, right=right_no, ends_at=now),
        ),  # type: ignore[arg-type]
    )
    cursor = _Cursor([], [])

    _project_matches(cursor, 7, event)

    complements = {
        params
        for statement, params in cursor.executions
        if "INSERT INTO binary_contract_complements" in statement
    }
    assert complements == {
        ("LEFT", "left-market", "left:no", "left:yes"),
        ("LEFT", "left-market", "left:yes", "left:no"),
        ("RIGHT", "right-market", "right:no", "right:yes"),
        ("RIGHT", "right-market", "right:yes", "right:no"),
    }


def test_trade_projection_upserts_late_fee_corrections() -> None:
    """Replace missing fee fields without recording a second fill."""
    cursor = _Cursor([], [])
    trade = Trade(
        id=TradeID("trade-fee"),
        contract_id=ContractID("contract"),
        venue_id=VenueID("venue-1"),
        side=OrderSide.BUY,
        quantity=Quantity(Decimal("6")),
        price=Price(Decimal("0.17")),
        executed_at=Timestamp.now(),
        fee=Money(Decimal("0.05926"), Currency("USDC")),
        fee_settlement_cost=Money(Decimal("0.05926"), Currency("USD")),
    )

    _project_trade(cursor, trade)

    statement, params = cursor.executions[0]
    assert "ON CONFLICT (trade_id) DO UPDATE" in statement
    assert params[-4:] == (
        Decimal("0.05926"),
        "USDC",
        Decimal("0.05926"),
        "USD",
    )


def test_trade_projection_retains_durable_journal_sequence() -> None:
    """Persist journal order so equal execution timestamps replay identically."""
    cursor = _Cursor([], [])
    trade = Trade(
        id=TradeID("trade-sequenced"),
        contract_id=ContractID("contract"),
        venue_id=VenueID("venue"),
        side=OrderSide.BUY,
        quantity=Quantity(Decimal("1")),
        price=Price(Decimal("0.17")),
        executed_at=Timestamp.now(),
    )

    _project_entry(
        cursor,
        JournalEntry(42, Timestamp.now(), TradeRecorded(trade), b""),
    )

    statement, params = cursor.executions[-1]
    assert "journal_sequence" in statement
    assert params[11] == 42


def test_bot_pnl_point_persists_position_and_cash_fees() -> None:
    cursor = _PnlCursor()
    event = MarketMatchesUpdated(MarketCycle(Underlying("BTC"), 300), ())
    entry = JournalEntry(9, Timestamp.now(), event, b"")

    _project_bot_pnl(cursor, entry)

    statement, params = cursor.executions[-1]
    assert "INSERT INTO pnl_performance_points" in statement
    assert "trading_fees_usd" in statement
    assert params[0] == "bot_ledger:9"
    assert params[2:8] == (
        Decimal("2"),
        Decimal("1"),
        Decimal("0.6"),
        Decimal("0.4"),
        Decimal("0.1"),
        Decimal("2.4"),
    )


def test_bot_pnl_point_values_complementary_inventory_at_locked_payout() -> None:
    """Value prepared collateral and executed pairs without double counting."""
    cursor = _PnlCursor(locked_adjustment=Decimal("0.8"))
    event = MarketMatchesUpdated(MarketCycle(Underlying("ETH"), 86400), ())
    entry = JournalEntry(10, Timestamp.now(), event, b"")

    _project_bot_pnl(cursor, entry)

    _statement, params = cursor.executions[-1]
    assert params[2:8] == (
        Decimal("2"),
        Decimal("1.8"),
        Decimal("0.6"),
        Decimal("0.4"),
        Decimal("0.1"),
        Decimal("3.2"),
    )
    position_query, position_params = cursor.executions[0]
    locked_query, locked_params = cursor.executions[1]
    movement_query, movement_params = cursor.executions[2]
    assert "WHERE portfolio_id = %s" in position_query
    assert position_params == (
        "cross-venue-arbitrage",
        "cross-venue-arbitrage",
    )
    assert "execution.leg1_side = 'sell'" in locked_query
    assert "binary_contract_complements" in locked_query
    assert "local_pairs AS" in locked_query
    assert "position.contract_id < complement.contract_id" in locked_query
    assert "leg1.remaining_quantity" in locked_query
    assert locked_params == ("cross-venue-arbitrage",)
    assert "source_portfolio_id = %s" in movement_query
    assert movement_params == (
        "cross-venue-arbitrage",
        "cross-venue-arbitrage",
    )


def test_recovery_projection_keeps_estimated_and_actual_economics() -> None:
    """Project the durable recovery plan and its realized cost fields."""
    cursor = _Cursor([], [])
    now = Timestamp.now()
    usd = Currency("USD")
    recovery = ExposureRecovery(
        id="execution-1",
        execution_id="execution-1",
        route=RecoveryRoute.UNWIND_EXCESS,
        source_contract_id=ContractID("source"),
        source_side=OrderSide.BUY,
        source_price=Price(Decimal("0.40")),
        source_fee=Money(Decimal("0.01"), usd),
        venue_id=VenueID("venue"),
        contract_id=ContractID("source"),
        side=OrderSide.SELL,
        quantity=Quantity(Decimal("2")),
        limit_price=Price(Decimal("0.38")),
        estimated_vwap=Price(Decimal("0.38")),
        estimated_recovery_fee=Money(Decimal("0.01"), usd),
        estimated_gross_result=Decimal("-0.04"),
        estimated_net_result=Decimal("-0.06"),
        filled_quantity=Quantity(Decimal("2")),
        average_price=Price(Decimal("0.39")),
        recovery_fee=Money(Decimal("0.01"), usd),
        actual_gross_result=Decimal("-0.02"),
        actual_net_result=Decimal("-0.04"),
        status=RecoveryStatus.RESOLVED,
        attempts=1,
        client_order_id=ClientOrderID("recovery-1"),
        created_at=now,
        updated_at=now,
    )

    _project_recovery(cursor, recovery)

    statement, params = cursor.executions[0]
    assert "ON CONFLICT (recovery_id) DO UPDATE" in statement
    assert "limit_price = EXCLUDED.limit_price" in statement
    assert "attempts = EXCLUDED.attempts" in statement
    assert "client_order_id = EXCLUDED.client_order_id" in statement
    assert "estimated_vwap = EXCLUDED.estimated_vwap" in statement
    assert len(params) == 33
    assert params[-6:] == (
        Decimal("2"),
        Decimal("0.39"),
        Decimal("0.01"),
        "USD",
        Decimal("-0.02"),
        Decimal("-0.04"),
    )
