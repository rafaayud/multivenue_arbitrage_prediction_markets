"""Read account PnL from Predict portfolio APIs.

Responsibilities
----------------
- Read resolved and unresolved positions for one account address.
- Normalize Predict position PnL to decimal USD.
"""

import asyncio
from decimal import Decimal, InvalidOperation
from typing import Any
from urllib.parse import quote

import httpx

from prediction_markets.domain.ports.pnl import (
    PnlPort,
    VenuePnlPosition,
    VenuePnlSnapshot,
)
from prediction_markets.domain.shared.value_objects import Timestamp
from prediction_markets.infrastructure.http_client import instrumented_async_client
from prediction_markets.infrastructure.venues.predict.config import (
    predict_account_address,
    predict_api_key,
    predict_headers,
)
from prediction_markets.infrastructure.venues.predict.mappers import (
    PREDICT_VENUE_ID,
    predict_contract_id,
)

_PAGE_SIZE = 100


class PredictPnlAdapter(PnlPort):
    """Read address-level PnL from the public-by-address Predict endpoint.

    Parameters
    ----------
    account_address
        Predict account address. Defaults to ``PREDICT_ACCOUNT_ADDRESS``.
    api_key
        Predict API key. Defaults to ``PREDICT_API_KEY``.
    base_url
        Predict API base URL.
    timeout_seconds
        Per-request timeout in seconds.
    client
        Optional compatible async HTTP client owned by the caller.

    Notes
    -----
    - Explicit fees are omitted because position responses do not expose a
      complete account-wide fee total.
    """

    def __init__(
        self,
        *,
        account_address: str | None = None,
        api_key: str | None = None,
        base_url: str = "https://api.predict.fun",
        timeout_seconds: float = 10.0,
        client: Any | None = None,
    ) -> None:
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        self._account_address = predict_account_address(account_address)
        self._api_key = predict_api_key(api_key)
        self._base_url = base_url.rstrip("/")
        self._owns_client = client is None
        self._client = client or instrumented_async_client(
            "predict",
            timeout=timeout_seconds,
        )

    async def fetch(self) -> VenuePnlSnapshot:
        """Fetch resolved and unresolved Predict position PnL.

        Returns
        -------
        VenuePnlSnapshot
            Normalized account PnL in USD.

        Raises
        ------
        RuntimeError
            If the account address or API key is missing.
        TypeError
            If Predict returns an unexpected payload.
        """
        if not self._account_address:
            raise RuntimeError("PREDICT_ACCOUNT_ADDRESS is not configured")
        if not self._api_key:
            raise RuntimeError("PREDICT_API_KEY is not configured")
        unresolved, resolved = await asyncio.gather(
            self._positions(is_resolved=False),
            self._positions(is_resolved=True),
        )
        unrealized = sum(
            (_decimal(position.get("pnlUsd")) for position in unresolved),
            Decimal("0"),
        )
        realized = sum(
            (_decimal(position.get("pnlUsd")) for position in resolved),
            Decimal("0"),
        )
        return VenuePnlSnapshot(
            venue_id=PREDICT_VENUE_ID,
            realized_pnl_usd=realized,
            unrealized_pnl_usd=unrealized,
            total_pnl_usd=realized + unrealized,
            fees_usd=None,
            observed_at=Timestamp.now(),
            source="Predict Positions API",
            scope="resolved + unresolved_positions",
            positions=tuple(
                _position(position, resolved=False) for position in unresolved
            )
            + tuple(_position(position, resolved=True) for position in resolved),
        )

    async def close(self) -> None:
        """Close the HTTP client when this adapter created it."""
        if self._owns_client:
            await self._client.aclose()

    async def _positions(self, *, is_resolved: bool) -> list[dict[str, Any]]:
        """Read every Predict position page for one resolution state."""
        positions: list[dict[str, Any]] = []
        cursor: str | None = None
        seen: set[str] = set()
        while True:
            params: dict[str, Any] = {
                "first": _PAGE_SIZE,
                "isResolved": str(is_resolved).lower(),
            }
            if cursor:
                params["after"] = cursor
            response = await self._client.get(
                f"{self._base_url}/v1/positions/"
                f"{quote(self._account_address or '', safe='')}",
                headers=predict_headers(self._api_key),
                params=params,
            )
            if isinstance(response, httpx.Response):
                response.raise_for_status()
                response = response.json()
            if not isinstance(response, dict) or response.get("success") is not True:
                raise TypeError("Unexpected Predict PnL response")
            page = response.get("data")
            if not isinstance(page, list) or not all(
                isinstance(item, dict) for item in page
            ):
                raise TypeError("Unexpected Predict positions data")
            positions.extend(page)
            next_cursor = response.get("cursor")
            if not next_cursor:
                return positions
            cursor = str(next_cursor)
            if cursor in seen:
                raise RuntimeError("Predict positions returned a repeated cursor")
            seen.add(cursor)


def _decimal(value: Any) -> Decimal:
    """Parse one required Predict monetary value as decimal USD."""
    if value is None:
        raise TypeError("Predict PnL value is missing")
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError) as error:
        raise TypeError("Invalid Predict PnL value") from error


def _position(payload: dict[str, Any], *, resolved: bool) -> VenuePnlPosition:
    """Normalize one Predict position and its binary outcome identity."""
    market = payload.get("market")
    outcome = payload.get("outcome")
    if not isinstance(market, dict) or not isinstance(outcome, dict):
        raise TypeError("Predict position has no market or outcome")
    market_id = str(market.get("id") or "")
    index_set = outcome.get("indexSet")
    if not market_id or index_set not in {1, 2}:
        raise TypeError("Predict position has no binary contract identity")
    quantity = _decimal(payload.get("amount")) / Decimal("1e18")
    current_value = _decimal(payload.get("valueUsd"))
    pnl = _decimal(payload.get("pnlUsd"))
    return VenuePnlPosition(
        venue_id=PREDICT_VENUE_ID,
        position_id=str(payload.get("id") or f"{market_id}:{index_set}"),
        contract_id=predict_contract_id(
            market_id,
            "yes" if index_set == 1 else "no",
        ),
        market_id=market_id,
        title=str(market.get("title") or "") or None,
        outcome=str(outcome.get("name") or "") or None,
        quantity=quantity,
        average_entry_price=_optional_decimal(payload.get("averageBuyPriceUsd")),
        current_price=current_value / quantity if quantity > 0 else None,
        current_value_usd=current_value,
        realized_pnl_usd=pnl if resolved else None,
        unrealized_pnl_usd=None if resolved else pnl,
        total_pnl_usd=pnl,
        fees_usd=None,
        resolved=resolved,
    )


def _optional_decimal(value: Any) -> Decimal | None:
    """Parse an optional Predict monetary value."""
    return _decimal(value) if value is not None else None
