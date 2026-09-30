"""Integrate predict instrument discovery with domain ports.

Responsibilities
----------------
- Perform venue I/O and translate external data into domain models.
"""

from decimal import Decimal
from typing import Any

import httpx

from prediction_markets.domain.ports.instrument_discovery import (
    InstrumentDiscoveryPort,
    InstrumentDiscoveryQuery,
    InstrumentDiscoveryResult,
)
from prediction_markets.infrastructure.venues.predict.catalog import PredictMarketCatalog
from prediction_markets.infrastructure.venues.predict.mappers import (
    PREDICT_VENUE_ID,
    predict_market_is_active,
    predict_market_window,
    predict_market_to_contracts,
    predict_market_to_market,
)

_PAGE_SIZE = 100


class PredictInstrumentDiscoveryAdapter(InstrumentDiscoveryPort):
    """Discover Predict.fun markets through its REST API."""

    def __init__(
        self,
        *,
        api_key: str | None = None,
        base_url: str = "https://api.predict.fun",
        timeout_seconds: float = 10.0,
        cache_seconds: float = 60.0,
        client: httpx.AsyncClient | None = None,
        catalog: PredictMarketCatalog | None = None,
    ) -> None:
        self._owns_catalog = catalog is None
        self._catalog = catalog or PredictMarketCatalog(
            api_key=api_key,
            base_url=base_url,
            timeout_seconds=timeout_seconds,
            cache_seconds=cache_seconds,
            client=client,
        )

    async def close(self) -> None:
        """Release network resources owned by the adapter.

        Notes
        -----
        - A caller-supplied client remains under caller ownership.
        """
        if self._owns_catalog:
            await self._catalog.close()

    async def discover(
        self,
        query: InstrumentDiscoveryQuery,
    ) -> InstrumentDiscoveryResult:
        """Discover markets and contracts from one filtered Predict payload set."""
        payloads = await self._discover_payloads(query)
        markets = tuple(
            market
            for payload in payloads
            if (market := predict_market_to_market(payload)) is not None
        )
        contracts = [
            contract
            for payload in payloads
            for contract in predict_market_to_contracts(payload)
        ]
        if query.venue_token_id is not None:
            contracts = [
                contract
                for contract in contracts
                if contract.symbol == query.venue_token_id
            ]
        return InstrumentDiscoveryResult(markets, tuple(contracts))

    async def _discover_payloads(
        self,
        query: InstrumentDiscoveryQuery,
    ) -> tuple[dict[str, Any], ...]:
        """Select the venue discovery route and apply local query filtering."""
        if query.venue_id is not None and query.venue_id != PREDICT_VENUE_ID:
            return ()

        if query.venue_market_id and query.venue_market_id.isdigit():
            market = await self._catalog.get_market(query.venue_market_id)
            markets = [market] if market is not None else []
        else:
            markets = await self._list_markets(query)
        return tuple(self._filter_markets(markets, query)[: query.limit])

    async def _list_markets(
        self,
        query: InstrumentDiscoveryQuery,
    ) -> list[dict[str, Any]]:
        """Fetch paginated venue markets and validate each response page."""
        markets: list[dict[str, Any]] = []
        cursor: str | None = None
        while len(self._filter_markets(markets, query)) < query.limit:
            params: dict[str, str | int] = {
                "first": _PAGE_SIZE,
            }
            if query.active_only:
                params["status"] = "OPEN"
            if query.underlying is not None and query.interval_seconds is not None:
                params["marketVariant"] = "CRYPTO_UP_DOWN"
            if cursor is not None:
                params["after"] = cursor

            batch, next_cursor = await self._catalog.list_page(
                params,
                rollover_seconds=query.interval_seconds,
            )
            markets.extend(batch)
            if (
                not batch
                or not next_cursor
                or next_cursor == cursor
            ):
                break
            cursor = next_cursor
        return markets

    @staticmethod
    def _filter_markets(
        markets: list[dict[str, Any]],
        query: InstrumentDiscoveryQuery,
    ) -> list[dict[str, Any]]:
        """Apply local market filters not guaranteed by the venue endpoint."""
        filtered: list[dict[str, Any]] = []
        for market in markets:
            market_id = str(market.get("id") or "")
            if query.active_only and not predict_market_is_active(market):
                continue
            if query.market_slug and market.get("categorySlug") != query.market_slug:
                continue
            if query.venue_market_id and query.venue_market_id not in {
                market_id,
                str(market.get("conditionId") or ""),
            }:
                continue
            if query.min_volume is not None and _stat(
                market, "volumeTotalUsd"
            ) < query.min_volume:
                continue
            if query.min_liquidity is not None and _stat(
                market, "totalLiquidityUsd"
            ) < query.min_liquidity:
                continue
            if not _matches_crypto_query(market, query):
                continue
            filtered.append(market)
        return filtered


def _stat(market: dict[str, Any], name: str) -> Decimal:
    stats = market.get("stats")
    value = stats.get(name) if isinstance(stats, dict) else 0
    return Decimal(str(value or 0))


def _matches_crypto_query(
    market: dict[str, Any],
    query: InstrumentDiscoveryQuery,
) -> bool:
    """Apply crypto variant, underlying, and interval query constraints."""
    if query.underlying is None and query.interval_seconds is None:
        return True

    variant = market.get("variantData")
    if not isinstance(variant, dict) or (
        market.get("marketVariant") != "CRYPTO_UP_DOWN"
        and variant.get("type") != "CRYPTO_UP_DOWN"
    ):
        return False

    if query.underlying is not None:
        symbol = str(variant.get("priceFeedSymbol") or "").upper()
        normalized = symbol.replace("_", "").replace("/", "").replace("-", "")
        if not normalized.startswith(query.underlying.symbol):
            return False

    if query.interval_seconds is not None:
        start, end = predict_market_window(market)
        if start is None or end is None:
            return False
        seconds = (end.value - start.value).total_seconds()
        if abs(seconds - query.interval_seconds) > 1:
            return False
    return True
