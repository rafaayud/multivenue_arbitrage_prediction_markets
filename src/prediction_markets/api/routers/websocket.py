"""Expose the websocket HTTP and WebSocket endpoints.

Responsibilities
----------------
- Validate transport inputs, invoke application services, and map results to API responses.
"""

import asyncio
from typing import Annotated

from fastapi import APIRouter, Depends, WebSocket, WebSocketDisconnect

from prediction_markets.api.dependencies import get_websocket_market_worker
from prediction_markets.api.models import SignalData, SignalResponse
from prediction_markets.api.runtime import ArbitrageRuntime
from prediction_markets.application.markets.models import monitored_market_key
from prediction_markets.domain.market_matching.value_objects import RegularCandidate
from prediction_markets.domain.trading.entities import Signal


router = APIRouter(prefix="/ws", tags=["websocket"])


def _signal_data(signal: Signal) -> SignalData:
    return SignalData(
        contract_id=str(signal.contract_id),
        venue_id=str(signal.venue_id),
        direction=signal.direction.value,
        quantity=signal.quantity.value,
        limit_price=signal.limit_price.value,
        fair_probability=signal.fair_probability.value,
        edge=signal.edge.value,
        strategy_id=str(signal.strategy_id),
        generated_at=signal.generated_at.value,
    )


async def _wait_for_disconnect(websocket: WebSocket) -> None:
    while (await websocket.receive())["type"] != "websocket.disconnect":
        pass


def _signal_response(
    market: RegularCandidate,
    left: Signal,
    right: Signal,
) -> SignalResponse:
    """Map a selected event signal pair to the dashboard wire model."""
    return SignalResponse(
        type="arbitrage_signal_pair",
        monitor_type="regular",
        monitor_key=monitored_market_key(market),
        market_label=max((value.title for value in market.markets), key=len),
        signals=[_signal_data(left), _signal_data(right)],
    )


@router.websocket("/arbitrage-signals")
async def stream_arbitrage_signals(
    websocket: WebSocket,
    market_worker: Annotated[
        ArbitrageRuntime,
        Depends(get_websocket_market_worker),
    ],
    monitor_key: str | None = None,
    underlying: str | None = None,
    interval_seconds: int | None = None,
):
    """Stream signals for an explicitly monitored event.

    Parameters
    ----------
    monitor_key
        Stable key returned when selecting an event from the catalog.
    underlying
        Retired selector, rejected with close code 1008 when present.
    interval_seconds
        Retired selector, rejected with close code 1008 when present.

    Notes
    -----
    - Unsupported, missing, and legacy selectors are rejected before acceptance.
    """
    market = market_worker.monitored_market(monitor_key) if monitor_key else None
    if (underlying is not None or interval_seconds is not None
            or not isinstance(market, RegularCandidate)):
        await websocket.close(code=1008)
        return

    await websocket.accept()

    async def send_signals() -> None:
        try:
            async for left, right in market_worker.stream_signal_pairs(market):
                response = _signal_response(market, left, right)
                await websocket.send_json(response.model_dump(mode="json"))
        except WebSocketDisconnect:
            pass

    async with asyncio.TaskGroup() as tasks:
        stream_task = tasks.create_task(send_signals())
        await _wait_for_disconnect(websocket)
        stream_task.cancel()
