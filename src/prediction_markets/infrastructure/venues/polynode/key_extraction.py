"""Integrate polynode key extraction with domain ports.

Responsibilities
----------------
- Perform venue I/O and translate external data into domain models.
"""

from datetime import timedelta
from decimal import Decimal, InvalidOperation

import httpx

from prediction_markets.domain.market_matching.enums import (
    ComparisonOperator,
    ObservationMethod,
    UpDownOutcome,
)
from prediction_markets.domain.market_matching.value_objects import (
    Underlying,
    UpDownMarketKey,
    UpDownResolutionRule,
)
from prediction_markets.domain.markets.entities import Market
from prediction_markets.domain.ports.port_key_extraction import KeyExtractionPort
from prediction_markets.domain.shared.value_objects import Currency, Money
from prediction_markets.infrastructure.http_client import instrumented_async_client

from prediction_markets.infrastructure.venues.polymarket.mappers import POLYMARKET_VENUE_ID
from prediction_markets.infrastructure.venues.polynode.config import polynode_api_key


SUPPORTED_INTERVALS = {
    timedelta(minutes=5): "5m",
    timedelta(minutes=15): "15m",
}


class PolynodeKeyExtractionAdapter(KeyExtractionPort):
    """Builds Polymarket short-form crypto keys from Polynode open prices."""

    def __init__(
        self,
        *,
        api_key: str | None = None,
        base_url: str = "https://api.polynode.dev",
        timeout_seconds: float = 10.0,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")

        self._base_url = base_url.rstrip("/")
        self._owns_client = client is None
        self._client = client or instrumented_async_client(
            "polynode",
            timeout=timeout_seconds,
        )
        self._api_key = _normalize_api_key(api_key) if api_key else polynode_api_key()
        self._open_prices: dict[tuple[str, int, str], Decimal] = {}

    async def close(self) -> None:
        """Release network resources owned by the adapter.

        Notes
        -----
        - A caller-supplied client remains under caller ownership.
        """
        if self._owns_client:
            await self._client.aclose()

    async def extract_key(
        self,
        markets: tuple[Market, ...],
        *,
        underlying: Underlying,
    ) -> tuple[tuple[Market, UpDownMarketKey], ...]:
        """Derive comparable up-or-down keys from polynode resolution metadata.

        Returns
        -------
        tuple[tuple[Market, UpDownMarketKey], ...]
            Supported markets paired with validated semantic keys.
        """
        keyed: list[tuple[Market, UpDownMarketKey]] = []

        for market in markets:
            window = _short_form_window(market)
            if market.venue_id != POLYMARKET_VENUE_ID or window is None:
                continue

            try:
                open_price = await self._open_price(underlying, *window)
            except (TypeError, ValueError):
                continue

            keyed.append((market, _market_key(market, underlying, open_price)))

        return tuple(keyed)

    async def _open_price(
        self,
        underlying: Underlying,
        window: int,
        interval: str,
    ) -> Decimal:
        """Fetch the opening reference price from Polynode for one market window."""
        cache_key = (underlying.symbol, window, interval)
        if cache_key in self._open_prices:
            return self._open_prices[cache_key]

        response = await self._client.get(
            f"{self._base_url}/v1/crypto/price",
            params={"symbol": underlying.symbol, "window": window, "interval": interval},
            headers={"x-api-key": self._api_key} if self._api_key else None,
        )
        response.raise_for_status()
        data = response.json()
        if not isinstance(data, dict):
            raise TypeError("Unexpected Polynode price response")

        value = data.get("openPrice")
        if value is None:
            raise ValueError("Polynode response is missing openPrice")
        try:
            price = Decimal(str(value))
        except (InvalidOperation, TypeError, ValueError) as exc:
            raise ValueError("Polynode openPrice must be numeric") from exc
        if price <= 0:
            raise ValueError("Polynode openPrice must be positive")

        self._open_prices[cache_key] = price
        return price


def _short_form_window(market: Market) -> tuple[int, str] | None:
    start = market.state.start_time
    end = market.state.close_time
    if start is None or end is None:
        return None
    interval = SUPPORTED_INTERVALS.get(end.value - start.value)
    return (int(start.value.timestamp()), interval) if interval else None


def _normalize_api_key(value: str) -> str:
    value = value.strip().strip('"').strip("'")
    return value if value.startswith("pn_") else f"pn_live_{value}"


def _market_key(
    market: Market,
    underlying: Underlying,
    open_price: Decimal,
) -> UpDownMarketKey:
    """Build a validated market key from external reference-price metadata."""
    start = market.state.start_time
    end = market.state.close_time
    assert start is not None and end is not None

    currency = Currency("USD")
    return UpDownMarketKey(
        underlying=underlying,
        currency=currency,
        start=start,
        end=end,
        reference_price=Money(open_price, currency),
        resolution_rule=UpDownResolutionRule(
            observation=ObservationMethod.LAST,
            observation_window_seconds=0,
            comparison=ComparisonOperator.GREATER_THAN_OR_EQUAL,
            tie_outcome=UpDownOutcome.UP,
        ),
    )
