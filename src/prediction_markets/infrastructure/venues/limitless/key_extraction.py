"""Integrate limitless key extraction with domain ports.

Responsibilities
----------------
- Perform venue I/O and translate external data into domain models.
"""

import re
from datetime import datetime, timezone
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
from prediction_markets.domain.shared.value_objects import Currency, Money, Timestamp
from prediction_markets.infrastructure.venues.limitless.catalog import (
    LimitlessMarketCatalog,
)
from prediction_markets.infrastructure.venues.limitless.mappers import LIMITLESS_VENUE_ID


class LimitlessKeyExtractionAdapter(KeyExtractionPort):
    """Extract canonical up/down keys from public Limitless CLOB market payloads."""

    def __init__(
        self,
        *,
        base_url: str = "https://api.limitless.exchange",
        timeout_seconds: float = 10.0,
        client: httpx.AsyncClient | None = None,
        catalog: LimitlessMarketCatalog | None = None,
        raw_markets: tuple[dict[str, Any], ...] = (),
    ) -> None:
        self._owns_catalog = catalog is None
        self._catalog = catalog or LimitlessMarketCatalog(
            base_url=base_url,
            timeout_seconds=timeout_seconds,
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
        """Derive comparable up-or-down keys from limitless resolution metadata.

        Returns
        -------
        tuple[tuple[Market, UpDownMarketKey], ...]
            Supported markets paired with validated semantic keys.
        """
        keyed: list[tuple[Market, UpDownMarketKey]] = []

        for market in markets:
            if market.venue_id != LIMITLESS_VENUE_ID:
                continue

            slug = str(market.id)
            raw_market = await self._fetch_market(slug)
            if raw_market is None:
                continue

            try:
                key = _limitless_payload_to_up_down_key(
                    raw_market,
                    underlying=underlying,
                )
            except (InvalidOperation, TypeError, ValueError):
                await self._catalog.invalidate_market(slug)
                continue

            keyed.append((market, key))

        return tuple(keyed)

    async def _fetch_market(self, slug: str) -> dict[str, Any] | None:
        """Fetch one venue market and validate its response shape."""
        return await self._catalog.get_market(slug, missing_ok=True)


def _index_raw_markets(
    raw_markets: tuple[dict[str, Any], ...],
) -> dict[str, dict[str, Any]]:
    """Index valid raw market payloads by their external identifier."""
    indexed: dict[str, dict[str, Any]] = {}
    for market in raw_markets:
        slug = market.get("slug")
        if slug:
            indexed[str(slug)] = market
    return indexed


def _limitless_payload_to_up_down_key(
    market: dict[str, Any],
    *,
    underlying: Underlying,
) -> UpDownMarketKey:
    """
    Build a key for exact start-to-end Limitless up/down resolutions.

    Notes
    -----
    - Limitless's public CLOB payload exposes the start observation in ``metadata.openPrice`` and the market close in ``expirationTimestamp``. The text rules determine whether equality resolves Up, Down, or void.
    """
    currency = _reference_currency(market)
    return UpDownMarketKey(
        underlying=underlying,
        currency=currency,
        start=_required_timestamp(market, "startAt", "Up/down market start"),
        end=_required_timestamp(
            market,
            "expirationTimestamp",
            "Up/down market end",
        ),
        reference_price=_reference_price(market, currency),
        resolution_rule=_resolution_rule(market),
    )


def _reference_currency(market: dict[str, Any]) -> Currency:
    metadata = market.get("priceOracleMetadata")
    symbol = str(metadata.get("symbol") if isinstance(metadata, dict) else "")
    if "/USD" in symbol.upper():
        return Currency("USD")

    # Limitless's published up/down markets express the reference price in USD.
    return Currency("USD")


def _reference_price(market: dict[str, Any], currency: Currency) -> Money:
    """Extract and validate the venue's reference price."""
    metadata = market.get("metadata")
    value = metadata.get("openPrice") if isinstance(metadata, dict) else None
    if value is None:
        value = market.get("openPrice")
    if value is None:
        raise ValueError("Up/down reference price is missing")

    try:
        return Money(Decimal(str(value)), currency)
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError("Invalid up/down reference price") from exc


def _resolution_rule(market: dict[str, Any]) -> UpDownResolutionRule:
    rules = _plain_text(str(market.get("description") or "")).lower()
    if not rules:
        raise ValueError("Up/down resolution rules are missing")

    comparison = _comparison_operator(rules)
    return UpDownResolutionRule(
        observation=ObservationMethod.LAST,
        observation_window_seconds=0,
        comparison=comparison,
        tie_outcome=_tie_outcome(rules, comparison),
        source=_resolution_source(rules),
    )


def _resolution_source(rules: str) -> str | None:
    """Normalize the external resolution price source when recognized."""
    if "chainlink" in rules:
        return "CHAINLINK"
    if "pyth" in rules:
        return "PYTH"
    return None


def _comparison_operator(rules: str) -> ComparisonOperator:
    """Infer the supported comparison operator from resolution text."""
    if "greater than or equal" in rules or "at least" in rules:
        return ComparisonOperator.GREATER_THAN_OR_EQUAL
    if "less than or equal" in rules or "at most" in rules:
        return ComparisonOperator.LESS_THAN_OR_EQUAL
    if "strictly higher" in rules or "greater than" in rules or "above" in rules:
        return ComparisonOperator.GREATER_THAN
    if "strictly lower" in rules or "less than" in rules or "below" in rules:
        return ComparisonOperator.LESS_THAN
    raise ValueError("Cannot determine up/down comparison operator from market rules")


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


def _required_timestamp(
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
        return Timestamp(datetime.fromisoformat(value.replace("Z", "+00:00")))

    epoch = Decimal(str(value))
    if epoch <= 0:
        return None
    if epoch > Decimal("1000000000000000"):
        epoch /= Decimal("1000000000")
    elif epoch > Decimal("10000000000"):
        epoch /= Decimal("1000")
    return Timestamp(datetime.fromtimestamp(float(epoch), tz=timezone.utc))


def _plain_text(value: str) -> str:
    return re.sub(r"<[^>]+>", " ", value)
