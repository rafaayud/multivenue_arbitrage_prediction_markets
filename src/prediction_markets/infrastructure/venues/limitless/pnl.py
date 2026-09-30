"""Read account PnL from Limitless portfolio APIs.

Responsibilities
----------------
- Combine all-time realized PnL with open-position unrealized PnL.
- Normalize Limitless six-decimal monetary integers to decimal USD.
"""

import asyncio
import os
from decimal import Decimal, InvalidOperation
from typing import Any

from limitless_sdk.api import HttpClient
from limitless_sdk.types import HMACCredentials

from prediction_markets.domain.ports.pnl import (
    PnlPort,
    VenuePnlPosition,
    VenuePnlSnapshot,
)
from prediction_markets.domain.shared.value_objects import Timestamp
from prediction_markets.infrastructure.venues.limitless.mappers import (
    LIMITLESS_VENUE_ID,
    limitless_contract_id,
)

_BASE_UNITS = Decimal("1000000")


class LimitlessPnlAdapter(PnlPort):
    """Read PnL through the authenticated Limitless portfolio endpoints.

    Parameters
    ----------
    api_key
        API key. Defaults to ``LIMITLESS_API_KEY``.
    api_secret
        Optional HMAC secret. Defaults to ``LIMITLESS_API_SECRET``.
    base_url
        Limitless API base URL.
    timeout_seconds
        Per-request timeout in seconds.
    client
        Optional compatible async Limitless client owned by the caller.

    Notes
    -----
    - Explicit fees are omitted because the portfolio resources do not expose
      a complete account-wide fee total.
    """

    def __init__(
        self,
        *,
        api_key: str | None = None,
        api_secret: str | None = None,
        base_url: str = "https://api.limitless.exchange",
        timeout_seconds: float = 10.0,
        client: Any | None = None,
    ) -> None:
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        resolved_key = (api_key or os.getenv("LIMITLESS_API_KEY") or "").strip()
        resolved_secret = (
            api_secret or os.getenv("LIMITLESS_API_SECRET") or ""
        ).strip()
        self._configured = client is not None or bool(resolved_key)
        self._owns_client = client is None
        if client is not None:
            self._client = client
        elif resolved_secret and resolved_key:
            self._client = HttpClient(
                base_url=base_url.rstrip("/"),
                timeout=int(timeout_seconds),
                hmac_credentials=HMACCredentials(
                    tokenId=resolved_key,
                    secret=resolved_secret,
                ),
            )
        else:
            self._client = HttpClient(
                base_url=base_url.rstrip("/"),
                timeout=int(timeout_seconds),
                api_key=resolved_key or None,
            )

    async def fetch(self) -> VenuePnlSnapshot:
        """Fetch realized and unrealized Limitless account PnL.

        Returns
        -------
        VenuePnlSnapshot
            Normalized account PnL in USD.

        Raises
        ------
        RuntimeError
            If no Limitless API key is configured.
        TypeError
            If a portfolio payload has an unexpected shape.
        """
        if not self._configured:
            raise RuntimeError("LIMITLESS_API_KEY is not configured")
        positions, chart = await asyncio.gather(
            self._client.get("/portfolio/positions"),
            self._client.get(
                "/portfolio/pnl-chart",
                params={"timeframe": "all"},
            ),
        )
        if not isinstance(positions, dict) or not isinstance(chart, dict):
            raise TypeError("Unexpected Limitless PnL response")
        realized = _decimal(chart.get("currentValue"))
        normalized_positions = _positions(positions)
        unrealized = sum(
            (
                position.unrealized_pnl_usd or Decimal("0")
                for position in normalized_positions
            ),
            Decimal("0"),
        )
        return VenuePnlSnapshot(
            venue_id=LIMITLESS_VENUE_ID,
            realized_pnl_usd=realized,
            unrealized_pnl_usd=unrealized,
            total_pnl_usd=realized + unrealized,
            fees_usd=None,
            observed_at=Timestamp.now(),
            source="Limitless Portfolio API",
            scope="all_time + current_positions",
            positions=normalized_positions,
        )

    async def close(self) -> None:
        """Close the Limitless client when this adapter created it."""
        if self._owns_client:
            await self._client.close()


def _positions(payload: dict[str, Any]) -> tuple[VenuePnlPosition, ...]:
    """Normalize active Limitless CLOB positions.

    Parameters
    ----------
    payload
        Decoded ``/portfolio/positions`` response.

    Returns
    -------
    tuple[VenuePnlPosition, ...]
        Non-empty YES and NO positions with six-decimal units normalized.

    Raises
    ------
    TypeError
        If a documented portfolio container has an unexpected shape.
    """
    clob = payload.get("clob") or []
    if not isinstance(clob, list):
        raise TypeError("Unexpected Limitless CLOB positions")
    normalized: list[VenuePnlPosition] = []
    for item in clob:
        positions = item.get("positions") if isinstance(item, dict) else None
        market = item.get("market") if isinstance(item, dict) else None
        if not isinstance(market, dict) or not isinstance(positions, dict) or not all(
            isinstance(value, dict) for value in positions.values()
        ):
            raise TypeError("Unexpected Limitless CLOB position")
        market_key = str(market.get("slug") or market.get("address") or "")
        if not market_key:
            raise TypeError("Limitless position has no market identity")
        title = str(market.get("title") or "") or None
        balances = item.get("tokensBalance")
        for outcome, value in positions.items():
            cost = _usd(value.get("cost"))
            average = _usd(value.get("fillPrice"))
            current_value = _usd(value.get("marketValue"))
            realized = _usd(value.get("realisedPnl"))
            unrealized = _usd(value.get("unrealizedPnl"))
            quantity = (
                _usd(balances.get(outcome))
                if isinstance(balances, dict) and balances.get(outcome) is not None
                else cost / average
                if average > 0
                else Decimal("0")
            )
            if not any((quantity, current_value, realized, unrealized)):
                continue
            normalized.append(
                VenuePnlPosition(
                    venue_id=LIMITLESS_VENUE_ID,
                    position_id=f"{market_key}:{outcome}",
                    contract_id=limitless_contract_id(market_key, outcome),
                    market_id=market_key,
                    title=title,
                    outcome=outcome,
                    quantity=quantity,
                    average_entry_price=average,
                    current_price=_binary_price(current_value, quantity),
                    current_value_usd=current_value,
                    realized_pnl_usd=realized,
                    unrealized_pnl_usd=unrealized,
                    total_pnl_usd=realized + unrealized,
                    fees_usd=None,
                    resolved=str(market.get("status") or "").upper() == "RESOLVED",
                )
            )
    return tuple(normalized)


def _usd(value: Any) -> Decimal:
    """Convert one six-decimal Limitless monetary integer to USD."""
    return _decimal(value) / _BASE_UNITS


def _binary_price(value: Decimal, quantity: Decimal) -> Decimal | None:
    """Return a valid binary mark or omit inconsistent venue data."""
    if quantity <= 0:
        return None
    price = value / quantity
    return price if price <= 1 else None


def _decimal(value: Any) -> Decimal:
    """Parse one Limitless chart value already denominated in USD."""
    if value is None:
        raise TypeError("Limitless PnL value is missing")
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError) as error:
        raise TypeError("Invalid Limitless PnL value") from error
