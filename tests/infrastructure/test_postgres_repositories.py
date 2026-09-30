"""Exercise postgres repositories behavior in the infrastructure layer.

Responsibilities
----------------
- Verify postgres repositories contracts, edge cases, and failure handling.
"""

from datetime import datetime, timezone
from decimal import Decimal

import pytest

from prediction_markets.domain.alerting.entities import (
    NotificationDelivery,
    NotificationSnapshot,
)
from prediction_markets.domain.alerting.enums import (
    Channel,
    DeliveryStatus,
    NotificationState,
    Severity,
)
from prediction_markets.domain.alerting.value_objects import (
    AlertSource,
    DeliveryID,
    Fingerprint,
    IncidentID,
    Recipient,
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
from prediction_markets.infrastructure.postgres.repositories import (
    PostgresArbitrageExecutionJournalRepository,
    PostgresExposureRecoveryRepository,
    PostgresIncidentRepository,
    PostgresOrderRepository,
    PostgresPositionRepository,
    PostgresNotificationDeliveryRepository,
    PostgresTradeRepository,
)


class _Cursor:
    """Record PostgreSQL cursor behavior without a database."""
    def __init__(self, rows, rowcount=1):
        self.rows = rows
        self.calls = []
        self.rowcount = rowcount

    def execute(self, query, params=()):
        self.calls.append((query, params))

    def fetchone(self):
        return self.rows[0] if self.rows else None

    def fetchall(self):
        return self.rows

    def close(self):
        pass


class _Connection:
    """Record PostgreSQL connection behavior without a database."""
    def __init__(self, rows=(), rowcount=1):
        self.cursor_obj = _Cursor(list(rows), rowcount)
        self.commits = 0
        self.rollbacks = 0

    def cursor(self):
        return self.cursor_obj

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1


def _notification_delivery(
    *,
    status: DeliveryStatus = DeliveryStatus.SENDING,
) -> NotificationDelivery:
    """Build one claimed alert delivery for repository tests."""
    now = Timestamp(datetime(2026, 8, 19, tzinfo=timezone.utc))
    return NotificationDelivery(
        id=DeliveryID("delivery-1"),
        incident_id=IncidentID("incident-1"),
        recipient=Recipient("on-call", Channel.SMS, "alertmanager:on-call"),
        snapshot=NotificationSnapshot(
            fingerprint=Fingerprint("execution-risk:1"),
            state=NotificationState.FIRING,
            severity=Severity.CRITICAL,
            source=AlertSource(component="trading-execution"),
            description="Residual exposure",
            starts_at=now,
        ),
        status=status,
        requested_at=now,
        attempt_count=1 if status is DeliveryStatus.SENDING else 0,
        started_at=now if status is DeliveryStatus.SENDING else None,
    )


def test_notification_repository_claims_pending_rows_with_a_lease() -> None:
    """Use SKIP LOCKED and reconstruct the immutable claimed generation."""
    now = datetime(2026, 8, 19, tzinfo=timezone.utc)
    row = (
        "delivery-1",
        "incident-1",
        "on-call",
        "sms",
        "alertmanager:on-call",
        "execution-risk:1",
        "firing",
        "critical",
        "trading-execution",
        None,
        None,
        "Execution requires manual review",
        None,
        "Residual exposure",
        now,
        None,
        "sending",
        now,
        1,
        None,
        None,
        now,
        None,
        None,
    )
    connection = _Connection([row])
    repository = PostgresNotificationDeliveryRepository(
        connection,
        lease_seconds=60,
    )

    claimed = repository.claim_pending(5, Timestamp(now))

    assert claimed[0].snapshot.severity is Severity.CRITICAL
    assert claimed[0].status is DeliveryStatus.SENDING
    statement, params = connection.cursor_obj.calls[0]
    assert "FOR UPDATE SKIP LOCKED" in statement
    assert params[1:] == (5, now)
    assert connection.commits == 1


def test_notification_repository_rejects_stale_completion() -> None:
    """Prevent an expired worker from completing a newer claimed generation."""
    connection = _Connection(rowcount=0)
    repository = PostgresNotificationDeliveryRepository(connection)
    delivery = _notification_delivery()
    delivery.mark_delivered(Timestamp(datetime(2026, 8, 19, 0, 1, tzinfo=timezone.utc)))

    with pytest.raises(RuntimeError, match="Stale notification delivery generation"):
        repository.update_notification_delivery(delivery)

    assert connection.commits == 0
    assert connection.rollbacks == 1


def test_notification_repository_can_join_an_external_transaction() -> None:
    """Leave commit ownership to the projector transaction when requested."""
    connection = _Connection()
    repository = PostgresNotificationDeliveryRepository(
        connection,
        manage_transactions=False,
    )

    repository.add_notification_delivery(
        _notification_delivery(status=DeliveryStatus.PENDING),
    )

    assert connection.commits == 0
    assert connection.rollbacks == 0


def test_notification_repository_persists_null_incident_id() -> None:
    """Store ephemeral one-shot deliveries without an incident foreign key."""
    connection = _Connection()
    repository = PostgresNotificationDeliveryRepository(connection)
    now = Timestamp(datetime(2026, 8, 19, tzinfo=timezone.utc))
    delivery = NotificationDelivery(
        id=DeliveryID("delivery-ephemeral"),
        incident_id=None,
        recipient=Recipient("on-call", Channel.EMAIL, "alertmanager:on-call"),
        snapshot=NotificationSnapshot(
            fingerprint=Fingerprint("trade-completed:execution-1"),
            state=NotificationState.FIRING,
            severity=Severity.INFORMATIONAL,
            source=AlertSource(component="trading-execution"),
            description="Execution execution-1 completed",
            starts_at=now,
            ends_at=now,
            title="Trade completed",
        ),
        status=DeliveryStatus.PENDING,
        requested_at=now,
    )

    repository.add_notification_delivery(delivery)

    _, params = connection.cursor_obj.calls[0]
    assert params[1] is None


def test_notification_repository_reconstructs_null_incident_id() -> None:
    """Round-trip claimed rows whose incident_id is NULL."""
    now = datetime(2026, 8, 19, tzinfo=timezone.utc)
    row = (
        "delivery-ephemeral",
        None,
        "on-call",
        "email",
        "alertmanager:on-call",
        "trade-completed:execution-1",
        "firing",
        "informational",
        "trading-execution",
        None,
        None,
        "Trade completed",
        None,
        "Execution execution-1 completed",
        now,
        now,
        "pending",
        now,
        0,
        None,
        None,
        None,
        None,
        None,
    )
    connection = _Connection([row])
    repository = PostgresNotificationDeliveryRepository(connection, lease_seconds=60)

    claimed = repository.claim_pending(1, Timestamp(now))

    assert claimed[0].incident_id is None
    assert claimed[0].snapshot.ends_at == Timestamp(now)


def test_incident_repository_locks_mutations_and_lists_active_rows() -> None:
    """Serialize incident mutations while keeping escalation scans read-only."""
    connection = _Connection()
    repository = PostgresIncidentRepository(connection)

    assert repository.get_incident(IncidentID("incident-1")) is None
    assert repository.list_active() == ()

    assert "FOR UPDATE" in connection.cursor_obj.calls[0][0]
    assert "status NOT IN ('resolved', 'closed')" in connection.cursor_obj.calls[1][0]


def test_order_repository_saves_and_reads_order():
    created_at = datetime(2026, 7, 19, tzinfo=timezone.utc)
    connection = _Connection(
        [
            (
                "filled",
                "contract-1",
                "buy",
                Decimal("2"),
                "limit",
                "client-1",
                "order-1",
                Decimal("0.42"),
                Decimal("2"),
                Decimal("0.42"),
                created_at,
                created_at,
            )
        ],
    )
    repository = PostgresOrderRepository(connection)
    order = OrderSnapshot(
        status=OrderStatus.FILLED,
        contract_id=ContractID("contract-1"),
        side=OrderSide.BUY,
        quantity=Quantity(Decimal("2")),
        order_type=OrderType.LIMIT,
        client_order_id=ClientOrderID("client-1"),
        order_id=OrderID("order-1"),
        limit_price=Price(Decimal("0.42")),
        filled_quantity=Quantity(Decimal("2")),
        average_price=Price(Decimal("0.42")),
        created_at=Timestamp(created_at),
        updated_at=Timestamp(created_at),
    )

    repository.save(order)

    assert connection.commits == 1
    assert repository.get_by_client_order_id(ClientOrderID("client-1")) == order


def test_trade_repository_saves_and_reads_trade():
    executed_at = datetime(2026, 7, 19, tzinfo=timezone.utc)
    connection = _Connection(
        [
            (
                "trade-1",
                "order-1",
                "client-1",
                "contract-1",
                "buy",
                Decimal("2"),
                Decimal("0.42"),
                executed_at,
                "portfolio-1",
                "strategy-1",
                None,
                None,
                None,
                None,
                None,
                "venue-1",
            )
        ],
    )
    repository = PostgresTradeRepository(connection)

    repository.save(
            Trade(
                id=TradeID("trade-1"),
            order_id=OrderID("order-1"),
            client_order_id=ClientOrderID("client-1"),
                contract_id=ContractID("contract-1"),
                venue_id=VenueID("venue-1"),
            side=OrderSide.BUY,
            quantity=Quantity(Decimal("2")),
            price=Price(Decimal("0.42")),
            executed_at=Timestamp(executed_at),
            portfolio_id=PortfolioID("portfolio-1"),
            strategy_id=StrategyID("strategy-1"),
        ),
    )
    trade = repository.get(TradeID("trade-1"))

    assert connection.commits == 1
    assert trade.id == TradeID("trade-1")
    assert trade.order_id == OrderID("order-1")
    assert trade.price == Price(Decimal("0.42"))


def test_trade_repository_round_trips_fee():
    executed_at = datetime(2026, 7, 19, tzinfo=timezone.utc)
    connection = _Connection(
        [
            (
                "trade-fee",
                "order-1",
                "client-1",
                "contract-1",
                "buy",
                Decimal("2"),
                Decimal("0.42"),
                executed_at,
                None,
                None,
                Decimal("0.01234"),
                "USDC",
                Decimal("0.01234"),
                "USD",
                None,
                "venue-1",
            )
        ],
    )
    repository = PostgresTradeRepository(connection)
    repository.save(
            Trade(
                id=TradeID("trade-fee"),
                contract_id=ContractID("contract-1"),
                venue_id=VenueID("venue-1"),
            side=OrderSide.BUY,
            quantity=Quantity(Decimal("2")),
            price=Price(Decimal("0.42")),
            executed_at=Timestamp(executed_at),
            fee=Money(Decimal("0.01234"), Currency("USDC")),
            fee_settlement_cost=Money(Decimal("0.01234"), Currency("USD")),
        ),
    )

    assert repository.get(TradeID("trade-fee")).fee == Money(
        Decimal("0.01234"), Currency("USDC")
    )
    assert repository.get(TradeID("trade-fee")).fee_settlement_cost == Money(
        Decimal("0.01234"), Currency("USD")
    )


def test_position_repository_saves_and_lists_open_positions():
    updated_at = datetime(2026, 7, 19, tzinfo=timezone.utc)
    connection = _Connection(
        [
            (
                "position-1",
                "contract-1",
                "venue-1",
                "long",
                Decimal("3"),
                Decimal("0.40"),
                Decimal("0.45"),
                Decimal("0"),
                Decimal("0.01"),
                "USD",
                [],
                "portfolio-1",
                updated_at,
                updated_at,
            )
        ],
    )
    repository = PostgresPositionRepository(connection)

    repository.save(
        Position(
            id=PositionID("position-1"),
            contract_id=ContractID("contract-1"),
            venue_id=VenueID("venue-1"),
            side=PositionSide.LONG,
            quantity=Quantity(Decimal("3")),
            average_price=Price(Decimal("0.40")),
            current_price=Price(Decimal("0.45")),
            fees=Money(Decimal("0.01"), Currency("USD")),
            portfolio_id=PortfolioID("portfolio-1"),
            opened_at=Timestamp(updated_at),
            updated_at=Timestamp(updated_at),
        ),
    )
    positions = repository.list_open()

    assert connection.commits == 1
    assert positions == (
        Position(
            id=PositionID("position-1"),
            contract_id=ContractID("contract-1"),
            venue_id=VenueID("venue-1"),
            side=PositionSide.LONG,
            quantity=Quantity(Decimal("3")),
            average_price=Price(Decimal("0.40")),
            current_price=Price(Decimal("0.45")),
            fees=Money(Decimal("0.01"), Currency("USD")),
            portfolio_id=PortfolioID("portfolio-1"),
            opened_at=Timestamp(updated_at),
            updated_at=Timestamp(updated_at),
        ),
    )


def test_exposure_recovery_repository_saves_and_lists_unresolved_work():
    updated_at = datetime(2026, 7, 19, tzinfo=timezone.utc)
    row = (
        "recovery-1",
        "venue-1",
        "contract-1",
        "sell",
        Decimal("2"),
        Decimal("0.42"),
        "portfolio-1",
        "strategy-1",
        "pending",
        1,
        "client-1",
        "order-1",
        None,
        updated_at,
        updated_at,
        "execution-1",
        "unwind_excess",
        "contract-1",
        "buy",
        Decimal("0.40"),
        Decimal("0.01"),
        "USD",
        Decimal("0.38"),
        Decimal("0.01"),
        "USD",
        Decimal("-0.04"),
        Decimal("-0.06"),
        Decimal("0"),
        None,
        None,
        None,
        None,
        None,
    )
    connection = _Connection([row])
    repository = PostgresExposureRecoveryRepository(connection)
    recovery = ExposureRecovery(
        id="recovery-1",
        venue_id=VenueID("venue-1"),
        contract_id=ContractID("contract-1"),
        side=OrderSide.SELL,
        quantity=Quantity(Decimal("2")),
        limit_price=Price(Decimal("0.42")),
        portfolio_id=PortfolioID("portfolio-1"),
        strategy_id=StrategyID("strategy-1"),
        status=RecoveryStatus.PENDING,
        attempts=1,
        client_order_id=ClientOrderID("client-1"),
        order_id=OrderID("order-1"),
        execution_id="execution-1",
        route=RecoveryRoute.UNWIND_EXCESS,
        source_contract_id=ContractID("contract-1"),
        source_side=OrderSide.BUY,
        source_price=Price(Decimal("0.40")),
        source_fee=Money(Decimal("0.01"), Currency("USD")),
        estimated_vwap=Price(Decimal("0.38")),
        estimated_recovery_fee=Money(Decimal("0.01"), Currency("USD")),
        estimated_gross_result=Decimal("-0.04"),
        estimated_net_result=Decimal("-0.06"),
        created_at=Timestamp(updated_at),
        updated_at=Timestamp(updated_at),
    )

    repository.save(recovery)

    assert connection.commits == 1
    assert repository.get("recovery-1") == recovery
    assert repository.list_unresolved() == (recovery,)


def test_arbitrage_journal_repository_saves_and_lists_active_work():
    updated_at = datetime(2026, 7, 19, tzinfo=timezone.utc)
    row = (
        "execution-1",
        "hedge_pending",
        "left",
        "left-contract",
        "buy",
        Decimal("2"),
        Decimal("0.40"),
        "leg1-client",
        "leg1-order",
        Decimal("2"),
        "right",
        "right-contract",
        "buy",
        Decimal("2"),
        Decimal("0.45"),
        "leg2-client",
        None,
        Decimal("0"),
        Decimal("2"),
        "portfolio-1",
        "strategy-1",
        None,
        updated_at,
        updated_at,
    )
    connection = _Connection([row])
    repository = PostgresArbitrageExecutionJournalRepository(connection)
    journal = ArbitrageExecutionJournal(
        id="execution-1",
        status=ArbitrageExecutionStatus.HEDGE_PENDING,
        leg1_venue_id=VenueID("left"),
        leg1_contract_id=ContractID("left-contract"),
        leg1_side=OrderSide.BUY,
        leg1_quantity=Quantity(Decimal("2")),
        leg1_limit_price=Price(Decimal("0.40")),
        leg1_client_order_id=ClientOrderID("leg1-client"),
        leg1_order_id=OrderID("leg1-order"),
        leg1_filled_quantity=Quantity(Decimal("2")),
        leg2_venue_id=VenueID("right"),
        leg2_contract_id=ContractID("right-contract"),
        leg2_side=OrderSide.BUY,
        leg2_quantity=Quantity(Decimal("2")),
        leg2_limit_price=Price(Decimal("0.45")),
        leg2_client_order_id=ClientOrderID("leg2-client"),
        residual_quantity=Quantity(Decimal("2")),
        portfolio_id=PortfolioID("portfolio-1"),
        strategy_id=StrategyID("strategy-1"),
        created_at=Timestamp(updated_at),
        updated_at=Timestamp(updated_at),
    )

    repository.save(journal)

    assert connection.commits == 1
    assert repository.get("execution-1") == journal
    assert repository.list_active() == (journal,)


def test_arbitrage_journal_claim_is_atomic():
    timestamp = Timestamp(datetime(2026, 7, 29, tzinfo=timezone.utc))
    journal = ArbitrageExecutionJournal(
        id="opportunity:btc-5m",
        status=ArbitrageExecutionStatus.PLANNED,
        leg1_venue_id=VenueID("left"),
        leg1_contract_id=ContractID("left-contract"),
        leg1_side=OrderSide.BUY,
        leg1_quantity=Quantity(Decimal("2")),
        leg1_limit_price=Price(Decimal("0.40")),
        leg1_client_order_id=ClientOrderID("leg1-client"),
        leg2_venue_id=VenueID("right"),
        leg2_contract_id=ContractID("right-contract"),
        leg2_side=OrderSide.BUY,
        leg2_quantity=Quantity(Decimal("2")),
        leg2_limit_price=Price(Decimal("0.45")),
        leg2_client_order_id=ClientOrderID("leg2-client"),
        created_at=timestamp,
        updated_at=timestamp,
    )
    claimed_connection = _Connection([(journal.id,)])
    duplicate_connection = _Connection()

    assert PostgresArbitrageExecutionJournalRepository(
        claimed_connection
    ).claim(journal) is True
    assert PostgresArbitrageExecutionJournalRepository(
        duplicate_connection
    ).claim(journal) is False
    assert "ON CONFLICT (execution_id) DO NOTHING" in (
        claimed_connection.cursor_obj.calls[0][0]
    )


@pytest.mark.parametrize(
    "repository_type",
    (
        PostgresOrderRepository,
        PostgresTradeRepository,
        PostgresPositionRepository,
        PostgresExposureRecoveryRepository,
        PostgresArbitrageExecutionJournalRepository,
    ),
)
@pytest.mark.parametrize("operation", ("_execute", "_fetchone", "_fetchall"))
def test_postgres_repositories_roll_back_failed_operations(
    repository_type,
    operation,
):
    connection = _Connection()

    def fail(*_args, **_kwargs):
        raise RuntimeError("constraint failure")

    connection.cursor_obj.execute = fail
    repository = repository_type(connection)

    with pytest.raises(RuntimeError, match="constraint failure"):
        getattr(repository, operation)("SELECT 1", ())

    assert connection.rollbacks == 1
    assert connection.commits == 0
