from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from decimal import Decimal
from threading import Event
from types import SimpleNamespace
from unittest.mock import Mock

from fastapi.testclient import TestClient
import pytest
from starlette.websockets import WebSocketDisconnect

from prediction_markets.api.dependencies import (
    get_arbitrage_runtime,
    get_pnl_service,
    get_trading_activity_reader,
    get_venue_health_service,
)
from prediction_markets.api.control_main import app as control_app
from prediction_markets.api.models import (
    ExecutionActivityOut,
    ExecutionJournalOut,
    ExecutionLegOut,
    ExposureRecoveryOut,
    InternalPnlSummaryOut,
    InternalVenuePnlOut,
    InternalVenuePnlV2Out,
    OrderOut,
    PositionOut,
    PerformanceViewOut,
    PnlComparabilityOut,
    PnlPerformanceSummaryOut,
    PnlPointOut,
    TradeOut,
)
from prediction_markets.api.trading_main import app as trading_app
from prediction_markets.api.trading.activity import (
    TradingActivityReader,
    _internal_pnl_out,
    _journal_out,
    _recovery_out,
)
from prediction_markets.application.execution.accounting import (
    apply_trade,
    manual_resolution_client_order_id,
    manual_resolution_order_id,
    manual_resolution_trade_id,
)
from prediction_markets.application.pnl import ConsolidatedPnl, PnlPoint, VenuePnlResult
from prediction_markets.application.venue_health import VenueHealthReport
from prediction_markets.domain.ports.pnl import VenuePnlPosition, VenuePnlSnapshot
from prediction_markets.domain.shared.value_objects import (
    ClientOrderID,
    ContractID,
    Currency,
    Money,
    Price,
    Quantity,
    Timestamp,
    TradeID,
    VenueID,
)
from prediction_markets.domain.trading.entities import (
    AccountingCorrection,
    ExposureRecovery,
    Trade,
)
from prediction_markets.domain.venue_health import (
    VenueHealthSnapshot,
    VenueHealthStatus,
)
from prediction_markets.domain.trading.enums import (
    ArbitrageExecutionStatus,
    OrderSide,
    RecoveryRoute,
    RecoveryStatus,
)
from prediction_markets.domain.trading.entities import ArbitrageExecutionJournal


_NOW = datetime(2026, 7, 29, 18, 30, tzinfo=timezone.utc)
_ORDER = OrderOut(
    status="filled",
    contract_id="polymarket:condition:token",
    side="buy",
    quantity=Decimal("2"),
    order_type="limit",
    client_order_id="client-1",
    order_id="order-1",
    limit_price=Decimal("0.40"),
    filled_quantity=Decimal("2"),
    average_price=Decimal("0.39"),
    created_at=_NOW,
    updated_at=_NOW,
)
_TRADE = TradeOut(
    trade_id="trade-1",
    order_id="order-1",
    client_order_id="client-1",
    contract_id="polymarket:condition:token",
    side="buy",
    quantity=Decimal("2"),
    price=Decimal("0.39"),
    executed_at=_NOW,
    portfolio_id="portfolio-1",
    strategy_id="long-arbitrage",
    fee_amount=Decimal("0.01"),
    fee_currency="USDC",
)
_POSITION = PositionOut(
    position_id="position-1",
    contract_id="polymarket:condition:token",
    side="long",
    quantity=Decimal("2"),
    average_entry_price=Decimal("0.39"),
    portfolio_id="portfolio-1",
    opened_at=_NOW,
    updated_at=_NOW,
)
_LEG = ExecutionLegOut(
    venue_id="POLYMARKET",
    contract_id="polymarket:condition:token",
    side="buy",
    quantity=Decimal("2"),
    limit_price=Decimal("0.40"),
    client_order_id="client-1",
    order_id="order-1",
    filled_quantity=Decimal("2"),
)
_JOURNAL = ExecutionJournalOut(
    execution_id="execution-1",
    monitor_type="cycle",
    monitor_key="cycle:BTC:300",
    underlying="BTC",
    interval_seconds=300,
    status="completed",
    leg1=_LEG,
    leg2=_LEG.model_copy(
        update={
            "venue_id": "LIMITLESS",
            "contract_id": "limitless:market:no",
        }
    ),
    residual_quantity=Decimal("0"),
    portfolio_id="portfolio-1",
    strategy_id="long-arbitrage",
    last_error=None,
    created_at=_NOW,
    updated_at=_NOW,
)
_RECOVERY = ExposureRecoveryOut(
    recovery_id="recovery-1",
    venue_id="POLYMARKET",
    contract_id="polymarket:condition:token",
    side="sell",
    quantity=Decimal("2"),
    limit_price=Decimal("0.38"),
    portfolio_id="portfolio-1",
    strategy_id="long-arbitrage",
    status="resolved",
    attempts=1,
    client_order_id="recovery-client",
    order_id="recovery-order",
    last_error=None,
    created_at=_NOW,
    updated_at=_NOW,
)
_VENUE_PNL = VenuePnlSnapshot(
    venue_id=VenueID("POLYMARKET"),
    realized_pnl_usd=Decimal("1.25"),
    unrealized_pnl_usd=Decimal("0.75"),
    total_pnl_usd=Decimal("2"),
    fees_usd=None,
    observed_at=Timestamp(_NOW),
    source="Polymarket Data API",
    scope="all_time + current_positions",
    positions=(
        VenuePnlPosition(
            venue_id=VenueID("POLYMARKET"),
            position_id="condition:token",
            contract_id=ContractID("polymarket:condition:token"),
            market_id="condition",
            title="Test position",
            outcome="Yes",
            quantity=Decimal("2"),
            average_entry_price=Decimal("0.4"),
            current_price=Decimal("0.5"),
            current_value_usd=Decimal("1"),
            realized_pnl_usd=Decimal("0"),
            unrealized_pnl_usd=Decimal("0.2"),
            total_pnl_usd=Decimal("0.2"),
            fees_usd=Decimal("0.01"),
            resolved=False,
        ),
    ),
)


class _PnlService:
    async def get(self) -> ConsolidatedPnl:
        return ConsolidatedPnl(
            generated_at=Timestamp(_NOW),
            realized_pnl_usd=Decimal("1.25"),
            unrealized_pnl_usd=Decimal("0.75"),
            gross_pnl_usd=None,
            total_pnl_usd=Decimal("2"),
            fees_usd=None,
            partial=False,
            venues=(
                VenuePnlResult(
                    venue_id=VenueID("POLYMARKET"),
                    snapshot=_VENUE_PNL,
                ),
            ),
            series=(
                PnlPoint(
                    observed_at=Timestamp(_NOW),
                    total_pnl_usd=Decimal("2"),
                ),
            ),
        )


class _Reader:
    def orders(self, **_filters):
        return [_ORDER]

    def trades(self, **_filters):
        return [_TRADE]

    def positions(self, **_filters):
        return [_POSITION]

    def journals(self, **_filters):
        return [_JOURNAL]

    def recoveries(self, **_filters):
        return [_RECOVERY]

    def snapshot(self, **_filters):
        return ExecutionActivityOut(
            generated_at=_NOW,
            trading_fees_usd=Decimal("0.1"),
            gas_usd=Decimal("0.02"),
            orders=[_ORDER],
            trades=[_TRADE],
            positions=[_POSITION],
            journals=[_JOURNAL],
            recoveries=[_RECOVERY],
        )

    def internal_pnl(self) -> InternalPnlSummaryOut:
        return InternalPnlSummaryOut(
            gross_pnl_usd=Decimal("1.1"),
            fees_usd=Decimal("0.1"),
            net_pnl_usd=Decimal("1"),
            priced_terminal_executions=1,
            unpriced_terminal_executions=0,
            venues=[
                InternalVenuePnlOut(
                    venue_id="POLYMARKET",
                    gross_contribution_usd=Decimal("1.1"),
                    fees_usd=Decimal("0.1"),
                    net_contribution_usd=Decimal("1"),
                    execution_count=1,
                )
            ],
            series=[],
        )

    def record_venue_pnl(self, _pnl: ConsolidatedPnl) -> None:
        pass

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
        total = Decimal("1") if source == "bot_ledger" else Decimal("2")
        return PerformanceViewOut(
            scope_label=source,
            methodology="test",
            requested_range=requested_range,
            effective_range=requested_range if range_supported else "ALL",
            range_supported=range_supported,
            methodology_note=None,
            summary=PnlPerformanceSummaryOut(
                realized=total,
                unrealized=Decimal("0"),
                fees=Decimal("0"),
                gas=Decimal("0"),
                total=total,
            ),
            series=[PnlPointOut(observed_at=_NOW, net_pnl_usd=total)],
            comparability=PnlComparabilityOut(
                comparable=comparable,
                confidence="high" if comparable else "low",
                notes=notes,
            ),
            partial=partial,
            last_updated=_NOW,
        )

    def ledger_venues(self) -> list[InternalVenuePnlV2Out]:
        return [
            InternalVenuePnlV2Out(
                venue_id="POLYMARKET",
                realized_pnl_usd=Decimal("1"),
                unrealized_pnl_usd=Decimal("0"),
                fees_usd=Decimal("0"),
                total_pnl_usd=Decimal("1"),
                partial=False,
            )
        ]


class _HealthService:
    async def get(self) -> VenueHealthReport:
        return VenueHealthReport(
            generated_at=Timestamp(_NOW),
            overall_status=VenueHealthStatus.OPERATIONAL,
            venues=(
                VenueHealthSnapshot(
                    venue_id=VenueID("POLYMARKET"),
                    status=VenueHealthStatus.OPERATIONAL,
                    checked_at=Timestamp(_NOW),
                    latency_ms=10,
                    source="test",
                    message="ok",
                ),
            ),
        )


@pytest.fixture(autouse=True)
def trading_activity(monkeypatch):
    monkeypatch.setenv("TRADING_API_KEY", "test-trading-key")
    control_app.dependency_overrides[get_trading_activity_reader] = _Reader
    control_app.dependency_overrides[get_pnl_service] = _PnlService
    control_app.dependency_overrides[get_venue_health_service] = _HealthService
    yield
    control_app.dependency_overrides.pop(get_trading_activity_reader, None)
    control_app.dependency_overrides.pop(get_pnl_service, None)
    control_app.dependency_overrides.pop(get_venue_health_service, None)


def test_lists_persisted_trading_activity() -> None:
    expected = {
        "/orders?status=filled&limit=5": ("contract_id", _ORDER.contract_id),
        "/trades?order_id=order-1": ("trade_id", _TRADE.trade_id),
        "/positions?open_only=true": ("position_id", _POSITION.position_id),
        "/execution-journals?status=completed": (
            "execution_id",
            _JOURNAL.execution_id,
        ),
        "/exposure-recoveries?status=resolved": (
            "recovery_id",
            _RECOVERY.recovery_id,
        ),
    }
    with TestClient(control_app) as client:
        for path, (field, value) in expected.items():
            response = client.get(
                path,
                headers={"X-Trading-Key": "test-trading-key"},
            )
            assert response.status_code == 200
            assert response.json()[0][field] == value


def test_activity_endpoints_require_trading_authentication() -> None:
    with TestClient(control_app) as client:
        assert client.get("/orders").status_code == 401


def test_reads_consolidated_venue_pnl() -> None:
    with TestClient(control_app) as client:
        response = client.get(
            "/pnl",
            headers={"X-Trading-Key": "test-trading-key"},
        )

    assert response.status_code == 200
    payload = response.json()
    performance = payload["portfolio_performance"]
    assert performance["default_view"] == "bot_ledger"
    assert performance["selected_range"] == "1M"
    assert performance["bot_ledger"]["summary"]["total"] == "1"
    assert performance["venue_account"]["range_supported"] is False
    assert payload["reconciliation"]["difference_usd"] == "1"
    assert payload["venue_health"][0]["scope"] == "all_time + current_positions"
    assert payload["venue_health"][0]["missing_fees"] is True
    assert payload["positions"][0]["position_id"] == "position-1"


def test_records_explicit_accounting_correction() -> None:
    original = Trade(
        id=TradeID("trade-correction"),
        contract_id=ContractID("contract"),
        venue_id=VenueID("POLYMARKET"),
        side=OrderSide.BUY,
        quantity=Quantity(Decimal("2")),
        price=Price(Decimal("0.4")),
        executed_at=Timestamp(_NOW),
    )

    class Runtime:
        state = type("State", (), {"trades": {original.id: original}})()

        def record_accounting_correction(
            self,
            replacement: Trade,
            reason: str,
        ) -> AccountingCorrection:
            return AccountingCorrection(
                id="correction-1",
                target_trade_id=original.id,
                original_trade=original,
                replacement_trade=replacement,
                resulting_position=apply_trade(None, replacement).position,
                reason=reason,
                recorded_at=Timestamp(_NOW),
            )

    trading_app.dependency_overrides[get_arbitrage_runtime] = Runtime
    try:
        with TestClient(trading_app) as client:
            response = client.post(
                "/accounting-corrections",
                headers={"X-Trading-Key": "test-trading-key"},
                json={
                    "trade_id": "trade-correction",
                    "side": "buy",
                    "quantity": "2",
                    "price": "0.45",
                    "executed_at": _NOW.isoformat(),
                    "reason": "Correct venue price",
                    "fee_amount": "0.01",
                    "fee_currency": "USDC",
                    "fee_settlement_amount": "0.01",
                    "fee_settlement_currency": "USD",
                },
            )
    finally:
        trading_app.dependency_overrides.pop(get_arbitrage_runtime, None)

    assert response.status_code == 200
    assert response.json() == {
        "correction_id": "correction-1",
        "trade_id": "trade-correction",
        "position_id": "POLYMARKET:cross-venue-arbitrage:contract",
        "recorded_at": _NOW.isoformat().replace("+00:00", "Z"),
    }


def test_records_manual_execution_resolution_economics() -> None:
    """Pass only operator economics and return the runtime-inferred trade."""
    class Runtime:
        async def complete_execution(self, execution_id: str, **values):
            trade = Trade(
                id=manual_resolution_trade_id(execution_id),
                contract_id=ContractID("limitless:btc:up"),
                venue_id=VenueID("LIMITLESS"),
                side=OrderSide.SELL,
                quantity=Quantity(Decimal("20.42")),
                price=values["price"],
                executed_at=values["executed_at"],
                client_order_id=manual_resolution_client_order_id(execution_id),
                order_id=manual_resolution_order_id(
                    values["method"],
                    values["external_reference"],
                ),
                fee_settlement_cost=Money(
                    values["fee_amount_usd"],
                    Currency("USD"),
                ),
            )
            return Mock(), trade

    trading_app.dependency_overrides[get_arbitrage_runtime] = Runtime
    try:
        with TestClient(trading_app) as client:
            response = client.post(
                "/execution-journals/execution-manual/complete",
                headers={"X-Trading-Key": "test-trading-key"},
                json={
                    "method": "settlement",
                    "price": "1",
                    "fee_amount_usd": "0",
                    "executed_at": _NOW.isoformat(),
                    "external_reference": "claim-0x123",
                },
            )
    finally:
        trading_app.dependency_overrides.pop(get_arbitrage_runtime, None)

    assert response.status_code == 200
    assert response.json() == {
        "execution_id": "execution-manual",
        "method": "settlement",
        "venue_id": "LIMITLESS",
        "contract_id": "limitless:btc:up",
        "side": "sell",
        "quantity": "20.42",
        "price": "1",
        "fee_amount_usd": "0",
        "executed_at": _NOW.isoformat().replace("+00:00", "Z"),
        "external_reference": "claim-0x123",
    }


def test_reconciles_execution_without_manual_resolution_data() -> None:
    """Expose venue reconciliation without accepting fabricated economics."""
    class Runtime:
        async def reconcile_execution(self, execution_id: str):
            assert execution_id == "execution-review"
            return SimpleNamespace(status=ArbitrageExecutionStatus.RECOVERED)

    trading_app.dependency_overrides[get_arbitrage_runtime] = Runtime
    try:
        with TestClient(trading_app) as client:
            response = client.post(
                "/execution-journals/execution-review/reconcile",
                headers={"X-Trading-Key": "test-trading-key"},
            )
    finally:
        trading_app.dependency_overrides.pop(get_arbitrage_runtime, None)

    assert response.status_code == 200
    assert response.json() == {
        "execution_id": "execution-review",
        "status": "recovered",
    }


def test_reconciles_pending_predict_inventory_by_transaction_hash() -> None:
    """Pass an operator-proven hash to the journal-owning runtime."""
    transaction_hash = "0x" + "ab" * 32

    class Runtime:
        async def reconcile_predict_inventory(self, operation_id: str, tx_hash: str):
            assert operation_id == "split-1"
            assert tx_hash == transaction_hash
            return SimpleNamespace(
                status=SimpleNamespace(value="confirmed"),
                transaction_id=tx_hash,
                quantity=Quantity(Decimal("5")),
                fee=Money(Decimal("0.0000369"), Currency("BNB")),
            )

    trading_app.dependency_overrides[get_arbitrage_runtime] = Runtime
    try:
        with TestClient(trading_app) as client:
            response = client.post(
                "/inventory-operations/split-1/reconcile",
                params={"transaction_hash": transaction_hash},
                headers={"X-Trading-Key": "test-trading-key"},
            )
    finally:
        trading_app.dependency_overrides.pop(get_arbitrage_runtime, None)

    assert response.status_code == 200
    assert response.json() == {
        "operation_id": "split-1",
        "status": "confirmed",
        "transaction_id": transaction_hash,
        "quantity": "5",
        "fee": "0.0000369",
        "fee_currency": "BNB",
    }


def test_internal_pnl_cache_deduplicates_concurrent_database_reads(
    monkeypatch,
) -> None:
    """Share one fresh internal PnL calculation across dashboard requests."""
    reader = TradingActivityReader("unused")
    expected = _Reader().internal_pnl()
    started = Event()
    release = Event()
    calls = 0

    def load() -> InternalPnlSummaryOut:
        nonlocal calls
        calls += 1
        started.set()
        assert release.wait(timeout=1)
        return expected

    monkeypatch.setattr(reader, "_load_internal_pnl", load)
    with ThreadPoolExecutor(max_workers=2) as executor:
        first = executor.submit(reader.internal_pnl)
        assert started.wait(timeout=1)
        second = executor.submit(reader.internal_pnl)
        release.set()

    assert first.result() is expected
    assert second.result() is expected
    assert calls == 1


def test_activity_fees_read_the_latest_projected_bot_point() -> None:
    reader = TradingActivityReader("unused")
    connection = Mock()
    connection.execute.return_value.fetchone.return_value = (
        Decimal("0.4"),
        Decimal("0.1"),
    )

    assert reader._latest_activity_fees(connection) == (
        Decimal("0.4"),
        Decimal("0.1"),
    )
    statement = connection.execute.call_args.args[0]
    assert "FROM pnl_performance_points" in statement
    assert "FROM trades" not in statement
    assert "FROM positions" not in statement


def test_activity_fees_default_to_zero_without_a_projected_point() -> None:
    reader = TradingActivityReader("unused")
    connection = Mock()
    connection.execute.return_value.fetchone.return_value = None

    assert reader._latest_activity_fees(connection) == (
        Decimal("0"),
        Decimal("0"),
    )


def test_streams_persisted_execution_activity_with_browser_session() -> None:
    with TestClient(trading_app) as trading_client:
        session = trading_client.post(
            "/trading-runs/session",
            json={"trading_key": "test-trading-key"},
        )
        assert session.status_code == 200

        # The browser receives this cookie through the same reverse-proxy origin.
        with TestClient(control_app, cookies=trading_client.cookies) as client:
            with client.websocket_connect("/ws/execution-events") as websocket:
                message = websocket.receive_json()

    assert message["type"] == "execution_activity_snapshot"
    assert message["orders"][0]["order_id"] == "order-1"
    assert message["trades"][0]["fee_amount"] == "0.01"
    assert message["journals"][0]["leg2"]["venue_id"] == "LIMITLESS"
    assert message["journals"][0]["monitor_key"] == "cycle:BTC:300"


def test_execution_stream_rejects_unauthenticated_clients() -> None:
    with TestClient(control_app) as client:
        with pytest.raises(WebSocketDisconnect) as error:
            with client.websocket_connect("/ws/execution-events"):
                pass

    assert error.value.code == 1008


def test_completed_execution_reports_actual_fees_and_locked_pnl() -> None:
    """Calculate post-trade PnL from actual fills and normalized venue fees."""
    journal = ArbitrageExecutionJournal(
        id="execution-fees",
        status=ArbitrageExecutionStatus.COMPLETED,
        leg1_venue_id=VenueID("POLYMARKET"),
        leg1_contract_id=ContractID("polymarket:yes"),
        leg1_side=OrderSide.BUY,
        leg1_quantity=Quantity(Decimal("6")),
        leg1_limit_price=Price(Decimal("0.17")),
        leg1_client_order_id=ClientOrderID("poly-client"),
        leg2_venue_id=VenueID("LIMITLESS"),
        leg2_contract_id=ContractID("limitless:no"),
        leg2_side=OrderSide.BUY,
        leg2_quantity=Quantity(Decimal("6")),
        leg2_limit_price=Price(Decimal("0.81")),
        leg2_client_order_id=ClientOrderID("limitless-client"),
        leg1_filled_quantity=Quantity(Decimal("6")),
        leg2_filled_quantity=Quantity(Decimal("6")),
        residual_quantity=Quantity(Decimal("0")),
        created_at=Timestamp(_NOW),
        updated_at=Timestamp(_NOW),
    )
    trades = (
        Trade(
            id=TradeID("poly-trade"),
            client_order_id=journal.leg1_client_order_id,
            contract_id=journal.leg1_contract_id,
            venue_id=journal.leg1_venue_id,
            side=OrderSide.BUY,
            quantity=Quantity(Decimal("6")),
            price=Price(Decimal("0.17")),
            executed_at=Timestamp(_NOW),
            fee=Money(Decimal("0.05926"), Currency("USDC")),
            fee_settlement_cost=Money(Decimal("0.05926"), Currency("USD")),
        ),
        Trade(
            id=TradeID("limitless-trade"),
            client_order_id=journal.leg2_client_order_id,
            contract_id=journal.leg2_contract_id,
            venue_id=journal.leg2_venue_id,
            side=OrderSide.BUY,
            quantity=Quantity(Decimal("6")),
            price=Price(Decimal("0.81")),
            executed_at=Timestamp(_NOW),
            fee=Money(Decimal("0.06"), Currency("OUTCOME_TOKEN")),
            fee_settlement_cost=Money(Decimal("0.06"), Currency("USD")),
        ),
    )

    latency_trace = {"execution_id": journal.id, "outcome": "terminal"}
    result = _journal_out(journal, trades=trades, latency_trace=latency_trace)

    assert result.leg1.fee_amount == Decimal("0.05926")
    assert result.leg2.fee_amount == Decimal("0.06")
    assert result.gross_locked_pnl_usd == Decimal("0.12")
    assert result.total_fee_settlement_cost_usd == Decimal("0.11926")
    assert result.net_locked_pnl_usd == Decimal("0.00074")
    assert result.latency_trace == latency_trace

    internal = _internal_pnl_out([result])
    assert internal.gross_pnl_usd == Decimal("0.12")
    assert internal.fees_usd == Decimal("0.11926")
    assert internal.net_pnl_usd == Decimal("0.00074")
    assert sum(
        (venue.net_contribution_usd for venue in internal.venues),
        Decimal("0"),
    ) == internal.net_pnl_usd

    recovery = ExposureRecoveryOut(
        recovery_id="recovery-priced",
        execution_id="execution-recovered",
        route="complete_missing_leg",
        venue_id="PREDICT",
        contract_id="predict:42:no",
        side="buy",
        quantity=Decimal("2"),
        filled_quantity=Decimal("2"),
        limit_price=Decimal("0.5"),
        average_price=Decimal("0.5"),
        source_contract_id="polymarket:condition:token",
        source_side="buy",
        source_price=Decimal("0.4"),
        source_fee_amount=Decimal("0.01"),
        source_fee_currency="USD",
        recovery_fee_amount=Decimal("0.02"),
        recovery_fee_currency="USD",
        actual_gross_result=Decimal("0.2"),
        actual_net_result=Decimal("0.17"),
        portfolio_id="portfolio-1",
        strategy_id="long-arbitrage",
        status="resolved",
        attempts=1,
        client_order_id="recovery-client",
        order_id="recovery-order",
        last_error=None,
        created_at=_NOW,
        updated_at=_NOW,
    )
    recovered_journal = ArbitrageExecutionJournal(
        id="execution-recovered",
        status=ArbitrageExecutionStatus.RECOVERED,
        leg1_venue_id=VenueID("POLYMARKET"),
        leg1_contract_id=ContractID("polymarket:condition:token"),
        leg1_side=OrderSide.BUY,
        leg1_quantity=Quantity(Decimal("2")),
        leg1_limit_price=Price(Decimal("0.4")),
        leg1_client_order_id=ClientOrderID("recovered-primary"),
        leg2_venue_id=VenueID("PREDICT"),
        leg2_contract_id=ContractID("predict:42:no"),
        leg2_side=OrderSide.BUY,
        leg2_quantity=Quantity(Decimal("2")),
        leg2_limit_price=Price(Decimal("0.5")),
        leg2_client_order_id=ClientOrderID("recovered-hedge"),
        leg1_filled_quantity=Quantity(Decimal("2")),
        leg2_filled_quantity=Quantity(Decimal("0")),
        residual_quantity=Quantity(Decimal("0")),
        created_at=Timestamp(_NOW),
        updated_at=Timestamp(_NOW),
    )
    recovered = ExposureRecovery(
        id="recovery-priced",
        execution_id="execution-recovered",
        route=RecoveryRoute.COMPLETE_MISSING_LEG,
        venue_id=VenueID("PREDICT"),
        contract_id=ContractID("predict:42:no"),
        side=OrderSide.BUY,
        quantity=Quantity(Decimal("2")),
        filled_quantity=Quantity(Decimal("2")),
        limit_price=Price(Decimal("0.5")),
        average_price=Price(Decimal("0.5")),
        source_contract_id=ContractID("polymarket:condition:token"),
        source_side=OrderSide.BUY,
        source_price=Price(Decimal("0.4")),
        source_fee=Money(Decimal("0.01"), Currency("USD")),
        estimated_vwap=Price(Decimal("0.5")),
        estimated_recovery_fee=Money(Decimal("0.02"), Currency("USD")),
        estimated_gross_result=Decimal("0.2"),
        estimated_net_result=Decimal("0.17"),
        recovery_fee=Money(Decimal("0.02"), Currency("USD")),
        actual_gross_result=Decimal("0.2"),
        actual_net_result=Decimal("0.17"),
        status=RecoveryStatus.RESOLVED,
        attempts=1,
        client_order_id=ClientOrderID("recovery-client"),
        created_at=Timestamp(_NOW),
        updated_at=Timestamp(_NOW),
    )
    recovered_result = _journal_out(
        recovered_journal,
        trades=(
            Trade(
                id=TradeID("recovered-primary-trade"),
                client_order_id=ClientOrderID("recovered-primary"),
                contract_id=ContractID("polymarket:condition:token"),
                venue_id=VenueID("POLYMARKET"),
                side=OrderSide.BUY,
                quantity=Quantity(Decimal("2")),
                price=Price(Decimal("0.4")),
                executed_at=Timestamp(_NOW),
                fee=Money(Decimal("0.01"), Currency("USDC")),
                fee_settlement_cost=Money(Decimal("0.01"), Currency("USD")),
            ),
            Trade(
                id=TradeID("recovery-trade"),
                client_order_id=ClientOrderID("recovery-client"),
                contract_id=ContractID("predict:42:no"),
                venue_id=VenueID("PREDICT"),
                side=OrderSide.BUY,
                quantity=Quantity(Decimal("2")),
                price=Price(Decimal("0.5")),
                executed_at=Timestamp(_NOW),
                fee=Money(Decimal("0.02"), Currency("OUTCOME_TOKEN")),
                fee_settlement_cost=Money(Decimal("0.02"), Currency("USD")),
            ),
        ),
        recovery=recovered,
    )
    assert recovered_result.gross_locked_pnl_usd == Decimal("0.2")
    assert recovered_result.total_fee_settlement_cost_usd == Decimal("0.03")
    assert recovered_result.net_locked_pnl_usd == Decimal("0.17")
    with_recovery = _internal_pnl_out(
        [result, recovered_result],
        [recovery],
        terminal_execution_count=2,
    )
    assert with_recovery.gross_pnl_usd == Decimal("0.32")
    assert with_recovery.fees_usd == Decimal("0.14926")
    assert with_recovery.net_pnl_usd == Decimal("0.17074")
    assert with_recovery.priced_terminal_executions == 2
    assert with_recovery.unpriced_terminal_executions == 0
    assert sum(
        (venue.net_contribution_usd for venue in with_recovery.venues),
        Decimal("0"),
    ) == with_recovery.net_pnl_usd


def test_manually_settled_residual_reports_actual_pnl_and_operator_data() -> None:
    """Treat a claim at one as an auditable closing trade, not a venue order."""
    journal = ArbitrageExecutionJournal(
        id="execution-manual",
        status=ArbitrageExecutionStatus.COMPLETED,
        leg1_venue_id=VenueID("LIMITLESS"),
        leg1_contract_id=ContractID("limitless:btc:up"),
        leg1_side=OrderSide.BUY,
        leg1_quantity=Quantity(Decimal("20.42")),
        leg1_limit_price=Price(Decimal("0.378")),
        leg1_client_order_id=ClientOrderID("limitless-buy"),
        leg2_venue_id=VenueID("POLYMARKET"),
        leg2_contract_id=ContractID("polymarket:btc:down"),
        leg2_side=OrderSide.BUY,
        leg2_quantity=Quantity(Decimal("20.42")),
        leg2_limit_price=Price(Decimal("0.61")),
        leg2_client_order_id=ClientOrderID("polymarket-buy"),
        leg1_filled_quantity=Quantity(Decimal("20.42")),
        residual_quantity=Quantity(Decimal("0")),
        created_at=Timestamp(_NOW),
        updated_at=Timestamp(_NOW),
    )
    source = Trade(
        id=TradeID("limitless-fill"),
        client_order_id=journal.leg1_client_order_id,
        contract_id=journal.leg1_contract_id,
        venue_id=journal.leg1_venue_id,
        side=OrderSide.BUY,
        quantity=Quantity(Decimal("20.42")),
        price=Price(Decimal("0.378")),
        executed_at=Timestamp(_NOW),
        fee=Money(Decimal("0.01"), Currency("USD")),
        fee_settlement_cost=Money(Decimal("0.01"), Currency("USD")),
    )
    settlement = Trade(
        id=manual_resolution_trade_id(journal.id),
        client_order_id=manual_resolution_client_order_id(journal.id),
        order_id=manual_resolution_order_id("settlement", "claim-0x123"),
        contract_id=journal.leg1_contract_id,
        venue_id=journal.leg1_venue_id,
        side=OrderSide.SELL,
        quantity=Quantity(Decimal("20.42")),
        price=Price(Decimal("1")),
        executed_at=Timestamp(_NOW),
        fee=Money(Decimal("0"), Currency("USD")),
        fee_settlement_cost=Money(Decimal("0"), Currency("USD")),
    )

    result = _journal_out(journal, trades=(source, settlement))

    assert result.gross_locked_pnl_usd == Decimal("12.70124")
    assert result.total_fee_settlement_cost_usd == Decimal("0.01")
    assert result.net_locked_pnl_usd == Decimal("12.69124")
    assert result.manual_resolution is not None
    assert result.manual_resolution.method == "settlement"
    assert result.manual_resolution.price == Decimal("1")
    assert result.manual_resolution.external_reference == "claim-0x123"


def test_zero_fill_recovery_uses_recorded_actual_net_when_trade_is_missing() -> None:
    """Price a full recovery from its durable actual economics."""
    journal = ArbitrageExecutionJournal(
        id="execution-zero-fill-recovery",
        status=ArbitrageExecutionStatus.RECOVERED,
        leg1_venue_id=VenueID("PREDICT"),
        leg1_contract_id=ContractID("predict:no"),
        leg1_side=OrderSide.SELL,
        leg1_quantity=Quantity(Decimal("5")),
        leg1_limit_price=Price(Decimal("0.08")),
        leg1_client_order_id=ClientOrderID("failed-primary"),
        leg2_venue_id=VenueID("POLYMARKET"),
        leg2_contract_id=ContractID("polymarket:yes"),
        leg2_side=OrderSide.SELL,
        leg2_quantity=Quantity(Decimal("5")),
        leg2_limit_price=Price(Decimal("0.93")),
        leg2_client_order_id=ClientOrderID("filled-hedge"),
        leg1_filled_quantity=Quantity(Decimal("0")),
        leg2_filled_quantity=Quantity(Decimal("5")),
        residual_quantity=Quantity(Decimal("0")),
        created_at=Timestamp(_NOW),
        updated_at=Timestamp(_NOW),
    )
    recovery = ExposureRecovery(
        id=journal.id,
        execution_id=journal.id,
        route=RecoveryRoute.COMPLETE_MISSING_LEG,
        source_contract_id=journal.leg2_contract_id,
        source_side=OrderSide.SELL,
        source_price=Price(Decimal("0.93")),
        source_fee=Money(Decimal("0.0228"), Currency("USD")),
        venue_id=journal.leg1_venue_id,
        contract_id=journal.leg1_contract_id,
        side=OrderSide.SELL,
        quantity=Quantity(Decimal("5")),
        filled_quantity=Quantity(Decimal("5")),
        limit_price=Price(Decimal("0.08")),
        average_price=Price(Decimal("0.08")),
        estimated_vwap=Price(Decimal("0.08")),
        estimated_recovery_fee=Money(Decimal("0.008"), Currency("USD")),
        estimated_gross_result=Decimal("0.05"),
        estimated_net_result=Decimal("0.0192"),
        recovery_fee=Money(Decimal("0.008"), Currency("USD")),
        actual_gross_result=Decimal("0.05"),
        actual_net_result=Decimal("0.0192"),
        status=RecoveryStatus.RESOLVED,
        attempts=1,
        client_order_id=ClientOrderID("recovery-order"),
        created_at=Timestamp(_NOW),
        updated_at=Timestamp(_NOW),
    )
    source_trade = Trade(
        id=TradeID("source-fill"),
        client_order_id=journal.leg2_client_order_id,
        contract_id=journal.leg2_contract_id,
        venue_id=journal.leg2_venue_id,
        side=OrderSide.SELL,
        quantity=Quantity(Decimal("5")),
        price=Price(Decimal("0.93")),
        executed_at=Timestamp(_NOW),
    )

    result = _journal_out(journal, trades=(source_trade,), recovery=recovery)

    assert result.gross_locked_pnl_usd == Decimal("0.05")
    assert result.total_fee_settlement_cost_usd == Decimal("0.0308")
    assert result.net_locked_pnl_usd == Decimal("0.0192")
    aggregate = _internal_pnl_out([result], [_recovery_out(recovery)])
    assert aggregate.gross_pnl_usd == Decimal("0.05")
    assert aggregate.fees_usd == Decimal("0.0308")
    assert aggregate.net_pnl_usd == Decimal("0.0192")
