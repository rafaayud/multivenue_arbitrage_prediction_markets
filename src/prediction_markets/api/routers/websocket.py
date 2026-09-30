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
from prediction_markets.api.runtime import ArbitrageRuntime, MONITORED_MARKET_CYCLES
from prediction_markets.application.markets.models import (
    MarketCycle,
    MonitoredMarket,
    monitored_market_key,
)
from prediction_markets.domain.market_matching.value_objects import (
    RegularCandidate,
    Underlying,
)
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


def _market_label(market: MonitoredMarket) -> str:
    """Return a concise user-facing label for a signal stream."""
    if isinstance(market, MarketCycle):
        return market.underlying.symbol
    return max((value.title for value in market.markets), key=len)


def _signal_response(
    market: MonitoredMarket,
    left: Signal,
    right: Signal,
) -> SignalResponse:
    """Map a cycle or regular candidate signal pair to the shared wire model."""
    cycle = market if isinstance(market, MarketCycle) else None
    return SignalResponse(
        type="arbitrage_signal_pair",
        monitor_type="regular" if isinstance(market, RegularCandidate) else "cycle",
        monitor_key=monitored_market_key(market),
        market_label=_market_label(market),
        underlying=cycle.underlying.symbol if cycle else None,
        interval_seconds=cycle.interval_seconds if cycle else None,
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
    """
    Stream validated signal pairs until the client disconnects or the worker stops.

    Parameters
    ----------
    websocket
        Accepted WebSocket connection used for JSON signal delivery.
    monitor_key
        Stable key identifying either a recurring cycle or a regular candidate.
    underlying
        Legacy cycle symbol accepted together with ``interval_seconds``.
    interval_seconds
        Legacy cycle interval accepted together with ``underlying``.

    Notes
    -----
    - The endpoint closes with policy-violation code ``1008`` before accepting
      unsupported or ambiguous selectors.
    - Existing cycle query parameters remain supported for compatibility.
    """
    try:
        if monitor_key is not None:
            if underlying is not None or interval_seconds is not None:
                raise ValueError("Use monitor_key or cycle fields, not both")
            market = market_worker.monitored_market(monitor_key)
            if market is None:
                raise ValueError("Market is not monitored")
        else:
            if underlying is None or interval_seconds is None:
                raise ValueError("A market selector is required")
            parsed_underlying = Underlying(underlying)
            if (parsed_underlying, interval_seconds) not in MONITORED_MARKET_CYCLES:
                raise ValueError("Cycle is not monitored")
            market = MONITORED_MARKET_CYCLES[(parsed_underlying, interval_seconds)]
    except (TypeError, ValueError):
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
