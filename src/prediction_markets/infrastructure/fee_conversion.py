"""Convert native chain fees into the common USD accounting unit."""

import os
from decimal import Decimal, InvalidOperation
from typing import Any

import httpx

from prediction_markets.domain.shared.value_objects import (
    Currency,
    Money,
    Timestamp,
)

_COIN_IDS = {"ETH": "ethereum", "BNB": "binancecoin"}
_USD = Currency("USD")


class CoinGeckoFeeConverter:
    """Value ETH and BNB fees at the closest historical USD observation."""

    def __init__(
        self,
        *,
        timeout_seconds: float = 10.0,
        client: Any | None = None,
    ) -> None:
        if timeout_seconds <= 0:
            raise ValueError("Fee conversion timeout must be positive")
        pro_key = (os.getenv("COINGECKO_PRO_API_KEY") or "").strip()
        demo_key = (os.getenv("COINGECKO_API_KEY") or "").strip()
        headers = (
            {"x-cg-pro-api-key": pro_key}
            if pro_key
            else {"x-cg-demo-api-key": demo_key}
            if demo_key
            else {}
        )
        self._owns_client = client is None
        self._client = client or httpx.Client(
            base_url=(
                "https://pro-api.coingecko.com/api/v3"
                if pro_key
                else "https://api.coingecko.com/api/v3"
            ),
            headers=headers,
            timeout=timeout_seconds,
        )
        self._prices: dict[tuple[str, int], Decimal] = {}

    def convert(self, fee: Money, occurred_at: Timestamp) -> Money:
        """Return a native fee valued in USD at ``occurred_at``."""
        code = str(fee.currency)
        coin_id = _COIN_IDS.get(code)
        if coin_id is None:
            raise ValueError(f"Unsupported native fee currency: {code}")
        timestamp = int(occurred_at.value.timestamp())
        key = (code, timestamp // 300)
        price = self._prices.get(key)
        if price is None:
            response = self._client.get(
                f"/coins/{coin_id}/market_chart/range",
                params={
                    "vs_currency": "usd",
                    "from": timestamp - 3600,
                    "to": timestamp + 3600,
                    "precision": "full",
                },
            )
            if isinstance(response, httpx.Response):
                response.raise_for_status()
                payload = response.json()
            else:
                payload = response
            price = _closest_price(payload, timestamp)
            self._prices[key] = price
        return Money(fee.amount * price, _USD)

    def close(self) -> None:
        """Close the HTTP client when this converter created it."""
        if self._owns_client:
            self._client.close()


def _closest_price(payload: Any, timestamp: int) -> Decimal:
    """Return the closest valid CoinGecko price within the requested window."""
    prices = payload.get("prices") if isinstance(payload, dict) else None
    if not isinstance(prices, list) or not prices:
        raise ValueError("CoinGecko returned no historical fee price")
    valid: list[tuple[int, Decimal]] = []
    for item in prices:
        if not isinstance(item, list) or len(item) < 2:
            continue
        try:
            observed_ms = int(item[0])
            price = Decimal(str(item[1]))
        except (InvalidOperation, TypeError, ValueError):
            continue
        if price.is_finite() and price > 0:
            valid.append((observed_ms, price))
    if not valid:
        raise ValueError("CoinGecko returned no valid historical fee price")
    return min(valid, key=lambda item: abs(item[0] - timestamp * 1000))[1]
