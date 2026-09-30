"""Integrate kalshi key extraction with domain ports.

Responsibilities
----------------
- Perform venue I/O and translate external data into domain models.
"""

import re
from datetime import datetime, timezone
from decimal import Decimal
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
from prediction_markets.domain.shared.value_objects import Currency, Money, Timestamp
from prediction_markets.infrastructure.http_client import instrumented_async_client

from prediction_markets.infrastructure.venues.kalshi.mappers import KALSHI_VENUE_ID


class KalshiKeyExtractionAdapter(KeyExtractionPort):
    """Extracts comparable up/down keys from Kalshi market payloads."""

    def __init__(
        self,
        *,
        base_url: str = "https://external-api.kalshi.com/trade-api/v2",
        timeout_seconds: float = 10.0,
        client: httpx.AsyncClient | None = None,
        raw_markets: tuple[dict[str, Any], ...] = ()) -> None:

        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")

        self._base_url = base_url.rstrip("/")
        self._owns_client = client is None
        self._client = client or instrumented_async_client(
            "kalshi",
            timeout=timeout_seconds,
        )
        self._raw_markets = _index_raw_markets(raw_markets)

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
        underlying: Underlying) -> tuple[tuple[Market, UpDownMarketKey], ...]:

        """Derive comparable up-or-down keys from kalshi resolution metadata.

        Returns
        -------
        tuple[tuple[Market, UpDownMarketKey], ...]
            Supported markets paired with validated semantic keys.
        """
        keyed: list[tuple[Market, UpDownMarketKey]] = []

        for market in markets:
            if market.venue_id != KALSHI_VENUE_ID:
                continue

            raw_market = self._raw_markets.get(str(market.id))
            if raw_market is None:
                raw_market = await self._fetch_market(str(market.id))
            if raw_market is None:
                continue

            try:
                key = _kalshi_payload_to_up_down_key(raw_market, underlying=underlying)
            except (TypeError, ValueError):
                continue

            keyed.append((market, key))

        return tuple(keyed)

    async def _fetch_market(self, ticker: str) -> dict[str, Any] | None:
        """Fetch one venue market and validate its response shape."""
        response = await self._client.get(f"{self._base_url}/markets/{ticker}")
        if response.status_code == 404:
            return None

        response.raise_for_status()
        data = response.json()
        if not isinstance(data, dict):
            raise TypeError(
                f"Unexpected Kalshi market response for ticker {ticker}: "
                f"expected dict, got {type(data).__name__}"
            )

        market = data.get("market")
        if not isinstance(market, dict):
            raise TypeError(
                f"Unexpected Kalshi market response for ticker {ticker}: "
                "missing or invalid 'market' field"
            )

        self._raw_markets[ticker] = market
        return market


def _index_raw_markets(
    raw_markets: tuple[dict[str, Any], ...],
) -> dict[str, dict[str, Any]]:
    """Index valid raw market payloads by their external identifier."""
    indexed: dict[str, dict[str, Any]] = {}
    for market in raw_markets:
        ticker = market.get("ticker") or market.get("market_ticker")
        if ticker:
            indexed[str(ticker)] = market
    return indexed


def _kalshi_payload_to_up_down_key(
    market: dict[str, Any],
    *,
    underlying: Underlying) -> UpDownMarketKey:
    """
    Build a canonical key from a Kalshi /markets/{ticker} payload.

    Notes
    -----
    - Relevant fields: - open_time / close_time: observation window bounds - floor_strike: reference price to compare against - strike_type + rules_primary/rules_secondary: resolution semantics - quote_currency / currency: payout currency (defaults to USD)
    """
    currency = _up_down_currency(market)
    return UpDownMarketKey(
        underlying=underlying,
        currency=currency,
        start=_required_market_timestamp(market, "open_time", "Up/down market start"),
        end=_required_market_timestamp(market, "close_time", "Up/down market end"),
        reference_price=_up_down_reference_price(market, currency),
        resolution_rule=_up_down_resolution_rule(market),
    )


def _up_down_currency(market: dict[str, Any]) -> Currency:
    value = market.get("quote_currency") or market.get("currency") or "USD"
    return Currency(str(value))


def _up_down_resolution_rule(market: dict[str, Any]) -> UpDownResolutionRule:
    """Derive a normalized up-or-down resolution rule from venue metadata."""
    rules = " ".join(
        str(market[field]).strip()
        for field in ("rules_primary", "rules_secondary")
        if market.get(field) and str(market[field]).strip()
    ).lower()
    if not rules:
        raise ValueError("Up/down resolution rules are missing")

    observation = _observation_method(rules)
    comparison = _comparison_operator(market, rules)
    return UpDownResolutionRule(
        observation=observation,
        observation_window_seconds=_observation_window_seconds(rules, observation),
        comparison=comparison,
        tie_outcome=_tie_outcome(rules, comparison),
    )


def _observation_method(rules: str) -> ObservationMethod:
    """Infer the supported observation method from venue resolution text."""
    if "time-weighted average" in rules or "time weighted average" in rules or "twap" in rules:
        return ObservationMethod.TWAP
    if "average" in rules or "mean" in rules:
        return ObservationMethod.MEAN
    if "median" in rules:
        return ObservationMethod.MEDIAN
    if "minimum" in rules:
        return ObservationMethod.MINIMUM
    if "maximum" in rules:
        return ObservationMethod.MAXIMUM

    last_price_terms = (
        "last price",
        "ending price",
        "end price",
        "closing price",
        "final price",
    )
    if any(term in rules for term in last_price_terms):
        return ObservationMethod.LAST

    first_price_terms = ("first price", "opening price", "start price", "initial price")
    if any(term in rules for term in first_price_terms):
        return ObservationMethod.FIRST

    raise ValueError("Cannot determine up/down observation method from market rules")


def _observation_window_seconds(
    rules: str,
    observation: ObservationMethod) -> int:

    """Extract the non-negative observation window from resolution text."""
    if observation in {ObservationMethod.FIRST, ObservationMethod.LAST}:
        return 0

    match = re.search(r"(\d+)\s*(second|minute|hour)s?", rules)
    if match is None:
        raise ValueError("Cannot determine up/down observation window from market rules")

    value = int(match.group(1))
    multiplier = {"second": 1, "minute": 60, "hour": 3600}[match.group(2)]
    return value * multiplier


def _comparison_operator(
    market: dict[str, Any],
    rules: str,
) -> ComparisonOperator:
    """Infer the supported comparison operator from resolution text."""
    if "greater than or equal" in rules or "at least" in rules:
        return ComparisonOperator.GREATER_THAN_OR_EQUAL
    if "less than or equal" in rules or "at most" in rules:
        return ComparisonOperator.LESS_THAN_OR_EQUAL

    strike_type = str(market.get("strike_type") or "").strip().lower()
    if strike_type in {"greater", "greater_than", "above"} or "greater than" in rules:
        return ComparisonOperator.GREATER_THAN
    if strike_type in {"less", "less_than", "below"} or "less than" in rules:
        return ComparisonOperator.LESS_THAN

    raise ValueError("Cannot determine up/down comparison operator from market")


def _tie_outcome(
    rules: str,
    comparison: ComparisonOperator,
) -> UpDownOutcome:
    """Infer which outcome wins when the observed price equals the reference."""
    if "tie" in rules and ("void" in rules or "cancel" in rules):
        return UpDownOutcome.VOID
    if comparison in {
        ComparisonOperator.GREATER_THAN_OR_EQUAL,
        ComparisonOperator.LESS_THAN_OR_EQUAL,
    }:
        return UpDownOutcome.UP
    return UpDownOutcome.DOWN


def _up_down_reference_price(
    market: dict[str, Any],
    currency: Currency) -> Money:

    """Extract and validate the up-or-down market reference price."""
    value = market.get("floor_strike")
    if value is None:
        raise ValueError("Up/down reference price is missing")

    try:
        return Money(Decimal(str(value)), currency)
    except (ValueError, TypeError) as exc:
        raise ValueError("Invalid up/down reference price") from exc


def _required_market_timestamp(
    market: dict[str, Any],
    field: str,
    field_name: str,
) -> Timestamp:
    timestamp = _timestamp_from_any(market.get(field))
    if timestamp is None:
        raise ValueError(f"{field_name} is missing")
    return timestamp


def _timestamp_from_any(value: Any) -> Timestamp | None:
    """Normalize numeric, text, and datetime timestamp variants."""
    if value is None:
        return None

    if isinstance(value, str) and not value.replace(".", "", 1).isdigit():
        normalized = value.replace("Z", "+00:00")
        return Timestamp(datetime.fromisoformat(normalized))

    epoch = Decimal(str(value))
    if epoch <= 0:
        return None

    if epoch > Decimal("1000000000000000"):
        seconds = epoch / Decimal("1000000000")
    elif epoch > Decimal("10000000000"):
        seconds = epoch / Decimal("1000")
    else:
        seconds = epoch

    return Timestamp(datetime.fromtimestamp(float(seconds), tz=timezone.utc))
