"""Integrate predict key extraction with domain ports.

Responsibilities
----------------
- Perform venue I/O and translate external data into domain models.
"""

from decimal import Decimal, InvalidOperation
from typing import Any

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
from prediction_markets.infrastructure.venues.predict.catalog import PredictMarketCatalog
from prediction_markets.infrastructure.venues.predict.mappers import (
    PREDICT_VENUE_ID,
    predict_market_window,
)


class PredictKeyExtractionAdapter(KeyExtractionPort):
    """Extract canonical keys from Predict crypto up/down markets."""

    def __init__(
        self,
        *,
        api_key: str | None = None,
        base_url: str = "https://api.predict.fun",
        timeout_seconds: float = 10.0,
        cache_seconds: float = 60.0,
        client: httpx.AsyncClient | None = None,
        catalog: PredictMarketCatalog | None = None,
        raw_markets: tuple[dict[str, Any], ...] = (),
    ) -> None:
        self._owns_catalog = catalog is None
        self._catalog = catalog or PredictMarketCatalog(
            api_key=api_key,
            base_url=base_url,
            timeout_seconds=timeout_seconds,
            cache_seconds=cache_seconds,
            client=client,
            raw_markets=raw_markets,
        )
        if catalog is not None:
            catalog.remember(raw_markets)

    async def close(self) -> None:
        """Release network resources owned by the adapter.

        Notes
        -----
        - A caller-supplied client remains under caller ownership.
        """
        if self._owns_catalog:
            await self._catalog.close()

    async def extract_key(
        self,
        markets: tuple[Market, ...],
        *,
        underlying: Underlying,
    ) -> tuple[tuple[Market, UpDownMarketKey], ...]:
        """Derive comparable up-or-down keys from predict resolution metadata.

        Returns
        -------
        tuple[tuple[Market, UpDownMarketKey], ...]
            Supported markets paired with validated semantic keys.
        """
        keyed: list[tuple[Market, UpDownMarketKey]] = []
        for market in markets:
            if market.venue_id != PREDICT_VENUE_ID:
                continue

            raw_market = await self._catalog.get_market(str(market.id))
            if raw_market is None:
                continue

            try:
                key = _predict_payload_to_up_down_key(
                    raw_market,
                    underlying=underlying,
                )
            except (TypeError, ValueError):
                continue
            keyed.append((market, key))
        return tuple(keyed)


def _predict_payload_to_up_down_key(
    market: dict[str, Any],
    *,
    underlying: Underlying,
) -> UpDownMarketKey:
    """Translate Predict resolution metadata into a validated market key."""
    variant = market.get("variantData")
    if not isinstance(variant, dict) or (
        market.get("marketVariant") != "CRYPTO_UP_DOWN"
        and variant.get("type") != "CRYPTO_UP_DOWN"
    ):
        raise ValueError("Predict market is not CRYPTO_UP_DOWN")

    symbol = str(variant.get("priceFeedSymbol") or "").upper()
    normalized = symbol.replace("_", "").replace("/", "").replace("-", "")
    if not normalized.startswith(underlying.symbol):
        raise ValueError("Predict price feed does not match the requested underlying")
    quote = normalized.removeprefix(underlying.symbol)
    if not quote:
        raise ValueError("Predict price feed has no quote currency")

    start, end = predict_market_window(market)
    if start is None or end is None:
        raise ValueError("Predict up/down market window is missing")

    currency = Currency(quote)
    interval_seconds = int((end.value - start.value).total_seconds())
    hourly = abs(interval_seconds - 3600) <= 1
    try:
        reference_price = Decimal(str(variant.get("startPrice")))
    except InvalidOperation as error:
        raise ValueError("Predict start price is missing or invalid") from error
    if not reference_price.is_finite() or reference_price <= 0:
        raise ValueError("Predict start price must be positive and finite")
    return UpDownMarketKey(
        underlying=underlying,
        currency=currency,
        start=start,
        end=end,
        reference_price=Money(reference_price, currency),
        resolution_rule=UpDownResolutionRule(
            observation=ObservationMethod.LAST,
            observation_window_seconds=0,
            comparison=(
                ComparisonOperator.GREATER_THAN_OR_EQUAL
                if hourly
                else ComparisonOperator.GREATER_THAN
            ),
            tie_outcome=UpDownOutcome.UP if hourly else UpDownOutcome.SPLIT,
            source=str(variant.get("priceFeedProvider") or "") or None,
        ),
    )
