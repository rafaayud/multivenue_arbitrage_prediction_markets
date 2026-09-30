"""Read account PnL from Polymarket portfolio APIs.

Responsibilities
----------------
- Combine current positions with the all-time account PnL reported by Polymarket.
- Normalize monetary values to decimal USD without estimating missing fees.
"""

import asyncio
import os
from decimal import Decimal, InvalidOperation
from typing import Any

import httpx

from prediction_markets.domain.ports.pnl import (
    PnlPort,
    VenuePnlPosition,
    VenuePnlSnapshot,
)
from prediction_markets.domain.shared.value_objects import Timestamp
from prediction_markets.infrastructure.http_client import instrumented_async_client
from prediction_markets.infrastructure.venues.polymarket.mappers import (
    POLYMARKET_VENUE_ID,
    polymarket_contract_id,
)

_PAGE_SIZE = 500


class PolymarketPnlAdapter(PnlPort):
    """Read wallet PnL from the public Polymarket Data API.

    Parameters
    ----------
    wallet
        Account wallet. Defaults to ``POLYMARKET_FUNDER``.
    base_url
        Polymarket Data API base URL.
    timeout_seconds
        Per-request timeout in seconds.
    client
        Optional compatible async HTTP client owned by the caller.

    Notes
    -----
    - Explicit fees are omitted because current-position entry fees are not a
      complete account-wide fee total.
    """

    def __init__(
        self,
        *,
        wallet: str | None = None,
        base_url: str = "https://data-api.polymarket.com",
        timeout_seconds: float = 10.0,
        client: Any | None = None,
    ) -> None:
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        self._wallet = (wallet or os.getenv("POLYMARKET_FUNDER") or "").strip()
        self._owns_client = client is None
        self._client = client or instrumented_async_client(
            "polymarket",
            base_url=base_url.rstrip("/"),
            timeout=timeout_seconds,
        )

    async def fetch(self) -> VenuePnlSnapshot:
        """Fetch all-time realized and current unrealized Polymarket PnL.

        Returns
        -------
        VenuePnlSnapshot
            Account-wide PnL reported by the Data API.

        Raises
        ------
        RuntimeError
            If no wallet is configured.
        TypeError
            If Polymarket returns an unexpected payload.
        """
        if not self._wallet:
            raise RuntimeError("POLYMARKET_FUNDER is not configured")
        positions, leaderboard = await asyncio.gather(
            self._positions(),
            self._get(
                "/v1/leaderboard",
                params={
                    "user": self._wallet,
                    "timePeriod": "ALL",
                    "orderBy": "PNL",
                    "limit": 1,
                },
            ),
        )
        unrealized = sum(
            (_decimal(position.get("cashPnl")) for position in positions),
            Decimal("0"),
        )
        current_realized = sum(
            (_decimal(position.get("realizedPnl")) for position in positions),
            Decimal("0"),
        )
        rows = _items(leaderboard)
        total = _decimal(rows[0].get("pnl")) if rows else unrealized + current_realized
        return VenuePnlSnapshot(
            venue_id=POLYMARKET_VENUE_ID,
            realized_pnl_usd=total - unrealized,
            unrealized_pnl_usd=unrealized,
            total_pnl_usd=total,
            fees_usd=None,
            observed_at=Timestamp.now(),
            source="Polymarket Data API",
            scope="all_time + current_positions",
            positions=tuple(_position(position) for position in positions),
        )

    async def close(self) -> None:
        """Close the HTTP client when this adapter created it."""
        if self._owns_client:
            await self._client.aclose()

    async def _positions(self) -> list[dict[str, Any]]:
        """Read every current position page for the configured wallet."""
        positions: list[dict[str, Any]] = []
        offset = 0
        while True:
            page = _items(
                await self._get(
                    "/positions",
                    params={
                        "user": self._wallet,
                        "limit": _PAGE_SIZE,
                        "offset": offset,
                        "includeArchived": "true",
                    },
                )
            )
            positions.extend(page)
            if len(page) < _PAGE_SIZE:
                return positions
            offset += _PAGE_SIZE

    async def _get(self, path: str, *, params: dict[str, Any]) -> Any:
        """Return decoded JSON from an HTTPX response or compatible SDK fake."""
        response = await self._client.get(path, params=params)
        if isinstance(response, httpx.Response):
            response.raise_for_status()
            return response.json()
        return response


def _items(payload: Any) -> list[dict[str, Any]]:
    """Validate a Polymarket collection response.

    Parameters
    ----------
    payload
        Decoded Data API response.

    Returns
    -------
    list[dict[str, Any]]
        Validated response rows.

    Raises
    ------
    TypeError
        If the response is not a list of objects.
    """
    if isinstance(payload, dict):
        payload = payload.get("data")
    if not isinstance(payload, list) or not all(
        isinstance(item, dict) for item in payload
    ):
        raise TypeError("Unexpected Polymarket PnL response")
    return payload


def _decimal(value: Any) -> Decimal:
    """Parse one required API monetary value as decimal USD."""
    if value is None:
        raise TypeError("Polymarket PnL value is missing")
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError) as error:
        raise TypeError("Invalid Polymarket PnL value") from error


def _position(payload: dict[str, Any]) -> VenuePnlPosition:
    """Normalize one current Polymarket position."""
    condition_id = str(payload.get("conditionId") or "")
    asset = str(payload.get("asset") or "")
    if not condition_id or not asset:
        raise TypeError("Polymarket position has no contract identity")
    realized = _decimal(payload.get("realizedPnl"))
    unrealized = _decimal(payload.get("cashPnl"))
    return VenuePnlPosition(
        venue_id=POLYMARKET_VENUE_ID,
        position_id=f"{condition_id}:{asset}",
        contract_id=polymarket_contract_id(condition_id, asset),
        market_id=condition_id,
        title=str(payload.get("title") or "") or None,
        outcome=str(payload.get("outcome") or "") or None,
        quantity=_decimal(payload.get("size")),
        average_entry_price=_optional_decimal(payload.get("avgPrice")),
        current_price=_optional_decimal(payload.get("curPrice")),
        current_value_usd=_decimal(payload.get("currentValue")),
        realized_pnl_usd=realized,
        unrealized_pnl_usd=unrealized,
        total_pnl_usd=realized + unrealized,
        fees_usd=_optional_decimal(payload.get("entryFeesUsdc")),
        resolved=bool(payload.get("redeemable")),
    )


def _optional_decimal(value: Any) -> Decimal | None:
    """Parse an optional Polymarket monetary value."""
    return _decimal(value) if value is not None else None
