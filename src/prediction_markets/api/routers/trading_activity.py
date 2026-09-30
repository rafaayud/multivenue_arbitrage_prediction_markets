"""Expose PostgreSQL-projected trading activity through HTTP and WebSocket APIs.

Responsibilities
----------------
- Serve durable opportunities, orders, trades, positions, and recovery state.
- Keep read-model polling outside the trading hot path.
"""

import asyncio
from dataclasses import replace
from typing import Annotated, Literal

from fastapi import APIRouter, Depends, HTTPException, Query, WebSocket
from fastapi.websockets import WebSocketDisconnect

from prediction_markets.api.auth import require_trading_key
from prediction_markets.api.dependencies import (
    get_arbitrage_runtime,
    get_pnl_service,
    get_trading_activity_reader,
    get_venue_health_service,
)
from prediction_markets.api.models import (
    AccountingCorrectionIn,
    AccountingCorrectionOut,
    ArbitrageOpportunityOut,
    ExecutionJournalOut,
    ExposureRecoveryOut,
    ManualExecutionResolutionIn,
    ManualExecutionResolutionOut,
    OrderOut,
    PnlDashboardOut,
    PnlReconciliationOut,
    PnlVenueHealthRowOut,
    PortfolioPerformanceOut,
    PositionOut,
    TradeOut,
)
from prediction_markets.application.pnl import PnlService
from prediction_markets.application.venue_health import VenueHealthService
from prediction_markets.api.runtime import ArbitrageRuntime
from prediction_markets.api.trading.activity import TradingActivityReader
from prediction_markets.domain.shared.value_objects import (
    ContractID,
    Currency,
    Money,
    OrderID,
    Price,
    Quantity,
    Timestamp,
    TradeID,
)
from prediction_markets.domain.trading.enums import (
    ArbitrageExecutionStatus,
    OrderSide,
    OrderStatus,
    RecoveryStatus,
)


query_router = APIRouter(tags=["trading-activity"])
runtime_router = APIRouter(tags=["trading-activity"])
router = APIRouter()
_PROTECTED = [Depends(require_trading_key)]


@query_router.get(
    "/arbitrage-opportunities",
    response_model=list[ArbitrageOpportunityOut],
)
def list_arbitrage_opportunities(
    reader: Annotated[TradingActivityReader, Depends(get_trading_activity_reader)],
    limit: int = Query(default=100, ge=1, le=500),
    underlying: str | None = None,
    interval_seconds: int | None = Query(default=None, gt=0),
) -> list[ArbitrageOpportunityOut]:
    """Return recent durable opportunities for REST dashboards."""
    return reader.opportunities(
        limit=limit,
        underlying=underlying.upper() if underlying else None,
        interval_seconds=interval_seconds,
    )


@query_router.get(
    "/orders",
    response_model=list[OrderOut],
    dependencies=_PROTECTED,
)
def list_orders(
    reader: Annotated[TradingActivityReader, Depends(get_trading_activity_reader)],
    limit: int = Query(default=100, ge=1, le=500),
    status: OrderStatus | None = None,
    contract_id: str | None = None,
) -> list[OrderOut]:
    return reader.orders(
        limit=limit,
        status=status,
        contract_id=ContractID(contract_id) if contract_id else None,
    )


@query_router.get(
    "/trades",
    response_model=list[TradeOut],
    dependencies=_PROTECTED,
)
def list_trades(
    reader: Annotated[TradingActivityReader, Depends(get_trading_activity_reader)],
    limit: int = Query(default=100, ge=1, le=500),
    order_id: str | None = None,
    contract_id: str | None = None,
) -> list[TradeOut]:
    return reader.trades(
        limit=limit,
        order_id=OrderID(order_id) if order_id else None,
        contract_id=ContractID(contract_id) if contract_id else None,
    )


@query_router.get(
    "/positions",
    response_model=list[PositionOut],
    dependencies=_PROTECTED,
)
def list_positions(
    reader: Annotated[TradingActivityReader, Depends(get_trading_activity_reader)],
    limit: int = Query(default=100, ge=1, le=500),
    open_only: bool = False,
) -> list[PositionOut]:
    return reader.positions(limit=limit, open_only=open_only)


@query_router.get(
    "/execution-journals",
    response_model=list[ExecutionJournalOut],
    dependencies=_PROTECTED,
)
def list_execution_journals(
    reader: Annotated[TradingActivityReader, Depends(get_trading_activity_reader)],
    limit: int = Query(default=100, ge=1, le=500),
    status: ArbitrageExecutionStatus | None = None,
) -> list[ExecutionJournalOut]:
    return reader.journals(limit=limit, status=status)


@query_router.get(
    "/pnl",
    response_model=PnlDashboardOut,
    dependencies=_PROTECTED,
)
async def get_pnl(
    service: Annotated[PnlService, Depends(get_pnl_service)],
    reader: Annotated[TradingActivityReader, Depends(get_trading_activity_reader)],
    health_service: Annotated[
        VenueHealthService,
        Depends(get_venue_health_service),
    ],
    view: Literal["venue_account", "bot_ledger"] = Query(default="bot_ledger"),
    range_: Literal["1D", "1W", "1M", "3M", "YTD", "1Y", "ALL"] = Query(
        default="1M",
        alias="range",
    ),
) -> PnlDashboardOut:
    """Return range-aware ledger performance with venue diagnostics."""
    venue_pnl, health = await asyncio.gather(
        service.get(),
        health_service.get(),
        return_exceptions=True,
    )
    if isinstance(venue_pnl, BaseException):
        raise HTTPException(status_code=503, detail="Venue PnL is unavailable")
    snapshots = tuple(
        result.snapshot for result in venue_pnl.venues if result.snapshot is not None
    )
    scopes = {snapshot.scope for snapshot in snapshots}
    venue_comparable = (
        not venue_pnl.partial
        and len(snapshots) == len(venue_pnl.venues)
        and None not in scopes
        and len(scopes) == 1
    )
    venue_notes = []
    if not venue_comparable:
        venue_notes.append(
            "Venue values are not summed because scopes or freshness differ."
        )
    bot_notes = ["Trades and confirmed economic operations are the source of truth."]
    try:
        await asyncio.to_thread(reader.record_venue_pnl, venue_pnl)
        (
            bot_view,
            venue_view,
            terminal_executions,
            positions,
            internal_venues,
        ) = await asyncio.gather(
            asyncio.to_thread(
                reader.performance_view,
                source="bot_ledger",
                requested_range=range_,
                comparable=True,
                notes=bot_notes,
                partial=False,
            ),
            asyncio.to_thread(
                reader.performance_view,
                source="venue_account",
                requested_range=range_,
                comparable=venue_comparable,
                notes=venue_notes,
                partial=venue_pnl.partial,
                range_supported=range_ == "ALL",
            ),
            asyncio.to_thread(reader.internal_pnl),
            asyncio.to_thread(reader.positions, limit=500),
            asyncio.to_thread(reader.ledger_venues),
        )
    except Exception as error:
        raise HTTPException(
            status_code=503,
            detail="Persisted PnL ledger is unavailable",
        ) from error
    health_by_venue = (
        {str(value.venue_id): value for value in health.venues}
        if not isinstance(health, BaseException)
        else {}
    )
    health_rows = []
    for result in venue_pnl.venues:
        snapshot = result.snapshot
        venue_health = health_by_venue.get(str(result.venue_id))
        notes = []
        if result.error:
            notes.append(result.error)
        if snapshot is not None and snapshot.fees_usd is None:
            notes.append("Fees are missing from the venue response.")
        if venue_health is not None and venue_health.status.value != "operational":
            notes.append(venue_health.message)
        health_rows.append(
            PnlVenueHealthRowOut(
                venue_id=str(result.venue_id),
                observed_at=snapshot.observed_at.value if snapshot else None,
                stale=result.stale,
                scope=snapshot.scope if snapshot else None,
                missing_fees=snapshot is None or snapshot.fees_usd is None,
                status=(
                    venue_health.status.value
                    if venue_health is not None
                    else "unavailable"
                ),
                notes=notes,
            )
        )
    internal_total = bot_view.summary.total
    venue_total = venue_view.summary.total if venue_comparable else None
    return PnlDashboardOut(
        generated_at=venue_pnl.generated_at.value,
        portfolio_performance=PortfolioPerformanceOut(
            selected_view=view,
            selected_range=range_,
            available_ranges=["1D", "1W", "1M", "3M", "YTD", "1Y", "ALL"],
            venue_account=venue_view,
            bot_ledger=bot_view,
        ),
        terminal_executions=terminal_executions,
        reconciliation=PnlReconciliationOut(
            internal_net_pnl_usd=internal_total,
            venue_reported_net_pnl_usd=venue_total,
            difference_usd=(
                venue_total - internal_total
                if venue_total is not None and internal_total is not None
                else None
            ),
            notes=venue_notes,
            internal_venues=internal_venues,
        ),
        venue_health=health_rows,
        positions=positions,
    )


@runtime_router.post(
    "/accounting-corrections",
    response_model=AccountingCorrectionOut,
    dependencies=_PROTECTED,
)
async def record_accounting_correction(
    value: AccountingCorrectionIn,
    runtime: Annotated[ArbitrageRuntime, Depends(get_arbitrage_runtime)],
) -> AccountingCorrectionOut:
    """Durably replace one trade while retaining its venue identity."""
    if runtime.state is None:
        raise HTTPException(status_code=503, detail="Trading runtime is unavailable")
    original = runtime.state.trades.get(TradeID(value.trade_id))
    if original is None:
        raise HTTPException(status_code=404, detail="Trade not found")
    if (value.fee_amount is None) != (value.fee_currency is None) or (
        value.fee_settlement_amount is None
    ) != (value.fee_settlement_currency is None):
        raise HTTPException(
            status_code=422,
            detail="Each fee amount requires its currency",
        )
    replacement = replace(
        original,
        side=OrderSide(value.side),
        quantity=Quantity(value.quantity),
        price=Price(value.price),
        executed_at=Timestamp(value.executed_at),
        fee=(
            Money(value.fee_amount, Currency(value.fee_currency))
            if value.fee_amount is not None and value.fee_currency is not None
            else None
        ),
        fee_settlement_cost=(
            Money(
                value.fee_settlement_amount,
                Currency(value.fee_settlement_currency),
            )
            if value.fee_settlement_amount is not None
            and value.fee_settlement_currency is not None
            else None
        ),
    )
    try:
        correction = runtime.record_accounting_correction(
            replacement,
            value.reason,
        )
    except (RuntimeError, ValueError) as error:
        raise HTTPException(status_code=409, detail=str(error)) from error
    return AccountingCorrectionOut(
        correction_id=correction.id,
        trade_id=str(correction.target_trade_id),
        position_id=str(correction.resulting_position.id),
        recorded_at=correction.recorded_at.value,
    )


@runtime_router.post(
    "/execution-journals/{execution_id}/reconcile",
    dependencies=_PROTECTED,
)
async def reconcile_execution(
    execution_id: str,
    runtime: Annotated[ArbitrageRuntime, Depends(get_arbitrage_runtime)],
) -> dict[str, str]:
    """Reconcile proven venue fills without creating a manual trade.

    Parameters
    ----------
    execution_id : str
        Execution whose venue orders must be reconciled.
    runtime : ArbitrageRuntime
        Active journal-backed trading runtime.

    Returns
    -------
    dict[str, str]
        Execution identity and durable status after reconciliation.

    Raises
    ------
    HTTPException
        HTTP 404 when the execution is unknown, or HTTP 409 when reconciliation
        cannot run safely.
    """
    try:
        execution = await runtime.reconcile_execution(execution_id)
    except KeyError as error:
        raise HTTPException(status_code=404, detail="Execution not found") from error
    except (RuntimeError, ValueError) as error:
        raise HTTPException(status_code=409, detail=str(error)) from error
    return {"execution_id": execution_id, "status": execution.status.value}


@runtime_router.post(
    "/execution-journals/{execution_id}/complete",
    response_model=ManualExecutionResolutionOut,
    dependencies=_PROTECTED,
)
async def complete_execution(
    execution_id: str,
    value: ManualExecutionResolutionIn,
    runtime: Annotated[ArbitrageRuntime, Depends(get_arbitrage_runtime)],
) -> ManualExecutionResolutionOut:
    """Persist the actual economics of an operator-resolved exposure.

    Parameters
    ----------
    execution_id : str
        Execution waiting for manual review.
    value : ManualExecutionResolutionIn
        Actual external exit or settlement data entered by the operator.
    runtime : ArbitrageRuntime
        Active journal-backed trading runtime.

    Returns
    -------
    ManualExecutionResolutionOut
        Inferred closing trade recorded in the accounting journal.

    Raises
    ------
    HTTPException
        HTTP 404 when the execution is unknown, or HTTP 409 when its state cannot
        be completed safely.
    """
    try:
        _, trade = await runtime.complete_execution(
            execution_id,
            method=value.method,
            price=Price(value.price),
            fee_amount_usd=value.fee_amount_usd,
            executed_at=Timestamp(value.executed_at),
            external_reference=value.external_reference,
        )
    except KeyError as error:
        raise HTTPException(status_code=404, detail="Execution not found") from error
    except (RuntimeError, ValueError) as error:
        raise HTTPException(status_code=409, detail=str(error)) from error
    return ManualExecutionResolutionOut(
        execution_id=execution_id,
        method=value.method,
        venue_id=str(trade.venue_id),
        contract_id=str(trade.contract_id),
        side=trade.side.value,
        quantity=trade.quantity.value,
        price=trade.price.value,
        fee_amount_usd=value.fee_amount_usd,
        executed_at=trade.executed_at.value,
        external_reference=(value.external_reference.strip() or None)
        if value.external_reference is not None
        else None,
    )


@runtime_router.post(
    "/inventory-operations/{operation_id}/reconcile",
    dependencies=_PROTECTED,
)
async def reconcile_predict_inventory(
    operation_id: str,
    runtime: Annotated[ArbitrageRuntime, Depends(get_arbitrage_runtime)],
    transaction_hash: str = Query(min_length=66, max_length=66),
) -> dict[str, object]:
    """Attach a proven BNB transaction hash to a pending Predict operation.

    Parameters
    ----------
    operation_id
        Pending inventory operation recorded before its external broadcast.
    runtime
        Authoritative journal-owning trading runtime.
    transaction_hash
        Exact BNB Chain transaction hash to verify and reconcile.

    Returns
    -------
    dict[str, object]
        Terminal status, transaction identity, quantity, and fee.

    Raises
    ------
    HTTPException
        HTTP 404 when the operation is not pending or HTTP 409 when identity,
        runtime state, or on-chain finality prevents reconciliation.
    """
    try:
        snapshot = await runtime.reconcile_predict_inventory(
            operation_id,
            transaction_hash,
        )
    except KeyError as error:
        raise HTTPException(status_code=404, detail="Inventory operation not found") from error
    except (RuntimeError, ValueError) as error:
        raise HTTPException(status_code=409, detail=str(error)) from error
    return {
        "operation_id": operation_id,
        "status": snapshot.status.value,
        "transaction_id": snapshot.transaction_id,
        "quantity": snapshot.quantity.value if snapshot.quantity is not None else None,
        "fee": snapshot.fee.amount if snapshot.fee is not None else None,
        "fee_currency": str(snapshot.fee.currency) if snapshot.fee is not None else None,
    }


@query_router.get(
    "/exposure-recoveries",
    response_model=list[ExposureRecoveryOut],
    dependencies=_PROTECTED,
)
def list_exposure_recoveries(
    reader: Annotated[TradingActivityReader, Depends(get_trading_activity_reader)],
    limit: int = Query(default=100, ge=1, le=500),
    status: RecoveryStatus | None = None,
) -> list[ExposureRecoveryOut]:
    return reader.recoveries(limit=limit, status=status)


@query_router.websocket("/ws/execution-events")
async def stream_execution_events(
    websocket: WebSocket,
    reader: Annotated[TradingActivityReader, Depends(get_trading_activity_reader)],
    limit: int = Query(default=100, ge=1, le=500),
) -> None:
    try:
        require_trading_key(
            websocket,
            websocket.headers.get("X-Trading-Key"),
        )
    except HTTPException:
        await websocket.close(code=1008)
        return

    await websocket.accept()
    previous = ""
    try:
        while True:
            snapshot = await asyncio.to_thread(reader.snapshot, limit=limit)
            fingerprint = snapshot.model_dump_json(exclude={"generated_at"})
            if fingerprint != previous:
                await websocket.send_json(snapshot.model_dump(mode="json"))
                previous = fingerprint

            # ponytail: polling serves one dashboard; use LISTEN/NOTIFY if fan-out grows.
            try:
                message = await asyncio.wait_for(websocket.receive(), timeout=1)
            except TimeoutError:
                continue
            if message["type"] == "websocket.disconnect":
                return
    except WebSocketDisconnect:
        return


router.include_router(query_router)
router.include_router(runtime_router)
