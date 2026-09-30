"""Integrate polymarket key extraction with domain ports.

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


_SHORT_FORM_INTERVALS = {timedelta(minutes=5), timedelta(minutes=15)}
_BINANCE_INTERVALS = {
    timedelta(hours=1): ("1h", 1),
    timedelta(days=1): ("1m", 4),
}
_PYTH_UNDERLYINGS = frozenset(("NVDA", "AMZN", "META", "TSLA", "SPY", "SPCX"))
_FINANCE_CATEGORIES = frozenset(("finance", "financial"))


class PolymarketKeyExtractionAdapter(KeyExtractionPort):
    """Extract Polymarket keys using the resolution source for each interval."""

    def __init__(
        self,
        *,
        polynode_key_extractor: KeyExtractionPort,
        binance_base_url: str = "https://api.binance.com",
        binance_futures_base_url: str = "https://fapi.binance.com",
        timeout_seconds: float = 10.0,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        self._polynode_key_extractor = polynode_key_extractor
        self._binance_base_url = binance_base_url.rstrip("/")
        self._binance_futures_base_url = binance_futures_base_url.rstrip("/")
        self._owns_client = client is None
        self._client = client or instrumented_async_client(
            "polymarket",
            timeout=timeout_seconds,
        )

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
        """Derive comparable up-or-down keys from polymarket resolution metadata.

        Returns
        -------
        tuple[tuple[Market, UpDownMarketKey], ...]
            Supported markets paired with validated semantic keys.
        """
        keyed: list[tuple[Market, UpDownMarketKey]] = []
        for market in markets:
            interval = _market_interval(market)
            if (
                market.venue_id == POLYMARKET_VENUE_ID
                and interval in _SHORT_FORM_INTERVALS
            ):
                keyed.append((market, _short_form_market_key(market, underlying)))
                continue
            if (
                market.venue_id == POLYMARKET_VENUE_ID
                and _is_finance_daily_market(market, underlying)
                and _resolution_source(market) == "PYTH"
            ):
                keyed.append((market, _pyth_market_key(market, underlying)))
                continue
            if market.venue_id != POLYMARKET_VENUE_ID or interval not in _BINANCE_INTERVALS:
                continue
            price = await self._binance_reference_price(underlying, market, interval)
            keyed.append((market, _binance_market_key(market, underlying, price, interval)))

        return tuple(keyed)

    async def _binance_reference_price(
        self,
        underlying: Underlying,
        market: Market,
        interval: timedelta,
    ) -> Decimal:
        """Fetch the Binance opening kline price required by a Polymarket resolution key."""
        start = market.state.start_time
        assert start is not None
        candle_interval, value_index = _BINANCE_INTERVALS[interval]
        start_ms = start.to_unix_ms()
        futures = bool(
            market.resolution
            and market.resolution.source
            and "/futures/" in market.resolution.source.lower()
        )
        base_url = self._binance_futures_base_url if futures else self._binance_base_url
        path = "/fapi/v1/klines" if futures else "/api/v3/klines"
        response = await self._client.get(
            f"{base_url}{path}",
            params={
                "symbol": f"{underlying.symbol}USDT",
                "interval": candle_interval,
                "startTime": start_ms,
                "limit": 1,
            },
        )
        response.raise_for_status()
        data = response.json()
        if (
            not isinstance(data, list)
            or not data
            or not isinstance(data[0], list)
            or len(data[0]) <= value_index
            or data[0][0] != start_ms
        ):
            raise TypeError("Unexpected Binance candle response")
        try:
            price = Decimal(str(data[0][value_index]))
        except (InvalidOperation, TypeError, ValueError) as exc:
            raise ValueError("Binance reference price must be numeric") from exc
        if price <= 0:
            raise ValueError("Binance reference price must be positive")
        return price


def _market_interval(market: Market) -> timedelta | None:
    start = market.state.start_time
    end = market.state.close_time
    return end.value - start.value if start is not None and end is not None else None


def _resolution_source(market: Market) -> str | None:
    """Normalize the external resolution price source when recognized."""
    resolution = market.resolution
    text = " ".join(
        part
        for part in (
            market.description,
            resolution.rules if resolution else None,
            resolution.source if resolution else None,
        )
        if part
    ).lower()
    if "chainlink" in text or "chain.link" in text:
        return "CHAINLINK"
    if "pyth" in text:
        return "PYTH"
    if "binance" in text:
        return "BINANCE"
    return None


def _is_finance_daily_market(market: Market, underlying: Underlying) -> bool:
    """Identify Pyth finance cycles without relying on calendar-day duration.

    Parameters
    ----------
    market
        Normalized Polymarket market metadata.
    underlying
        Requested normalized underlying symbol.

    Returns
    -------
    bool
        Whether the market belongs to the supported daily finance family.

    Notes
    -----
    - Trading-day windows span weekends and holidays, so their elapsed time
      may exceed 24 hours.
    """
    category = (market.category or "").strip().lower()
    return category in _FINANCE_CATEGORIES or underlying.symbol in _PYTH_UNDERLYINGS


def _short_form_market_key(
    market: Market,
    underlying: Underlying,
) -> UpDownMarketKey:
    """Build a 5m/15m key from the market window without an external strike.

    Notes
    -----
    - Cross-venue matching for short crypto cycles ignores the reference price
      and normalizes tie handling in application compatibility rules.
    """
    start = market.state.start_time
    end = market.state.close_time
    assert start is not None and end is not None
    currency = Currency("USD")
    return UpDownMarketKey(
        underlying=underlying,
        currency=currency,
        start=start,
        end=end,
        reference_price=None,
        resolution_rule=UpDownResolutionRule(
            observation=ObservationMethod.LAST,
            observation_window_seconds=0,
            comparison=ComparisonOperator.GREATER_THAN_OR_EQUAL,
            tie_outcome=UpDownOutcome.UP,
            source=_resolution_source(market),
        ),
    )


def _binance_market_key(
    market: Market,
    underlying: Underlying,
    price: Decimal,
    interval: timedelta,
) -> UpDownMarketKey:
    """Build a validated Polymarket key from a Binance reference price."""
    start = market.state.start_time
    end = market.state.close_time
    assert start is not None and end is not None
    currency = Currency("USDT")
    daily = interval == timedelta(days=1)
    return UpDownMarketKey(
        underlying=underlying,
        currency=currency,
        start=start,
        end=end,
        reference_price=Money(price, currency),
        resolution_rule=UpDownResolutionRule(
            observation=ObservationMethod.LAST,
            observation_window_seconds=0,
            comparison=(
                ComparisonOperator.GREATER_THAN
                if daily
                else ComparisonOperator.GREATER_THAN_OR_EQUAL
            ),
            tie_outcome=UpDownOutcome.SPLIT if daily else UpDownOutcome.UP,
            source="BINANCE",
        ),
    )


def _pyth_market_key(
    market: Market,
    underlying: Underlying,
) -> UpDownMarketKey:
    """Build a finance daily key whose Pyth reference is not in Gamma data."""
    start = market.state.start_time
    end = market.state.close_time
    assert start is not None and end is not None
    currency = Currency("USD")
    return UpDownMarketKey(
        underlying=underlying,
        currency=currency,
        start=start,
        end=end,
        reference_price=None,
        resolution_rule=UpDownResolutionRule(
            observation=ObservationMethod.LAST,
            observation_window_seconds=0,
            comparison=ComparisonOperator.GREATER_THAN,
            tie_outcome=UpDownOutcome.SPLIT,
            source="PYTH",
        ),
    )
