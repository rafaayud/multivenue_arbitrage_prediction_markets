"""Integrate limitless instrument discovery with domain ports.

Responsibilities
----------------
- Perform venue I/O and translate external data into domain models.
"""

from decimal import Decimal
from time import time
from typing import Any

import httpx

from prediction_markets.domain.contracts.entities import BinaryContract
from prediction_markets.domain.ports.instrument_discovery import (
    InstrumentDiscoveryPort,
    InstrumentDiscoveryQuery,
    InstrumentDiscoveryResult,
)
from prediction_markets.infrastructure.venues.limitless.mappers import (
    LIMITLESS_VENUE_ID,
    limitless_market_to_contracts,
    limitless_market_to_market,
    parse_limitless_contract_id,
)
from prediction_markets.infrastructure.venues.limitless.catalog import (
    LimitlessMarketCatalog,
)


_PAGE_SIZE = 25


class LimitlessInstrumentDiscoveryAdapter(InstrumentDiscoveryPort):
    """Discovers public, active Limitless CLOB markets and their binary contracts."""

    def __init__(
        self,
        *,
        base_url: str = "https://api.limitless.exchange",
        timeout_seconds: float = 10.0,
        client: httpx.AsyncClient | None = None,
        catalog: LimitlessMarketCatalog | None = None,
    ) -> None:
        self._owns_catalog = catalog is None
        self._catalog = catalog or LimitlessMarketCatalog(
            base_url=base_url,
            timeout_seconds=timeout_seconds,
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
        """Discover normalized markets and contracts from one Limitless payload set.

        Returns
        -------
        InstrumentDiscoveryResult
            Markets and contracts accepted by venue and query filters.
        """
        if query.venue_id is not None and query.venue_id != LIMITLESS_VENUE_ID:
            return InstrumentDiscoveryResult((), ())

        payloads = await self._discover_payloads(query)
        markets = tuple(
            market
            for payload in payloads
            if (market := limitless_market_to_market(payload)) is not None
        )
        contracts = [
            contract
            for payload in payloads
            for contract in limitless_market_to_contracts(payload)
        ]
        return InstrumentDiscoveryResult(
            markets,
            tuple(_filter_contracts(contracts, query)),
        )

    async def _discover_payloads(
        self,
        query: InstrumentDiscoveryQuery,
    ) -> tuple[dict[str, Any], ...]:
        """Select the venue discovery route and apply local query filtering."""
        slug = query.market_slug or query.venue_market_id
        if slug:
            if query.venue_market_id and query.venue_market_id.isdigit():
                markets = [
                    await self._search_market_by_id(
                        query.venue_market_id,
                        query.search_text,
                    ),
                ]
            else:
                markets = [await self._get_market_by_slug(slug)]
        elif query.underlying is not None and query.interval_seconds is not None:
            markets = await self._list_short_form_markets(query)
        else:
            markets = await self._list_active_markets(query)
        return tuple(self._filter_markets(markets, query))

    async def _list_short_form_markets(self, query: InstrumentDiscoveryQuery) -> list[dict[str, Any]]:
        """Resolve active short-form market slugs and fetch their payloads."""
        label = {300: "5-min", 900: "15-min", 3600: "hourly", 86400: "daily"}.get(
            query.interval_seconds,
        )
        if label is None:
            return []
        if query.interval_seconds in {300, 900}:
            window = int(time() // query.interval_seconds * query.interval_seconds)
            slug = (
                f"{query.underlying.symbol.lower()}-up-or-down-{label}-{window}"
            )
            try:
                return [await self._get_market_by_slug(slug)]
            except httpx.HTTPStatusError as exc:
                if exc.response.status_code == 404:
                    return []
                raise

        data = await self._get_active_slugs()

        slugs = [
            str(item["slug"])
            for item in data
            if isinstance(item, dict)
            and str(item.get("ticker") or "").upper() == query.underlying.symbol
            and f"-{label}-" in str(item.get("slug") or "")
        ][: query.limit]
        # ponytail: Limitless has no bulk-detail endpoint; avoid an unbounded gather.
        markets = []
        for slug in slugs:
            markets.append(await self._get_market_by_slug(slug))
        return markets

    async def _get_active_slugs(self) -> tuple[dict[str, Any], ...]:
        """Return the cached active-slug catalog."""
        return await self._catalog.get_active_slugs()

    async def _get_market_by_slug(self, slug: str) -> dict[str, Any]:
        data = await self._catalog.get_market(slug)
        assert data is not None
        return data

    async def _search_market_by_id(
        self,
        market_id: str,
        search_text: str | None,
    ) -> dict[str, Any]:
        """Resolve AGG's numeric Limitless identifier through a fresh catalog.

        Parameters
        ----------
        market_id
            Numeric identifier supplied by AGG.
        search_text
            Event or market text used to narrow the public Limitless catalog.

        Returns
        -------
        dict[str, Any]
            Native Limitless market payload matching ``market_id``.

        Raises
        ------
        ValueError
            If no search text is available.
        LookupError
            If the identifier is absent from the refreshed catalog.

        Notes
        -----
        - The first miss invalidates the exact-text entry and retries once.
        - A small token fallback handles catalog wording differences while
          still requiring an exact numeric ID match.
        """
        if search_text is None:
            raise ValueError("Limitless numeric market IDs require search text")

        queries = _search_queries(search_text)
        for index, query in enumerate(queries):
            markets = await self._get_search_catalog(query)
            match = _market_with_id(markets, market_id)
            if match is not None:
                return match
            if index == 0:
                await self._invalidate_search_catalog(query)
                markets = await self._get_search_catalog(query)
                match = _market_with_id(markets, market_id)
                if match is not None:
                    return match
        raise LookupError(f"Limitless market ID not found: {market_id}")

    async def _get_search_catalog(
        self,
        query: str,
    ) -> tuple[dict[str, Any], ...]:
        """Return one short-lived, single-flight Limitless search catalog."""
        return await self._catalog.search(query)

    async def _invalidate_search_catalog(self, query: str) -> None:
        """Remove one search result so a stale catalog can be refreshed."""
        await self._catalog.invalidate_search(query)

    async def _list_active_markets(
        self,
        query: InstrumentDiscoveryQuery,
    ) -> list[dict[str, Any]]:
        """Fetch active venue markets across pages up to the query limit."""
        markets: list[dict[str, Any]] = []
        page = 1
        while len(markets) < query.limit:
            limit = min(_PAGE_SIZE, query.limit - len(markets))
            batch = await self._catalog.list_active_markets(limit=limit, page=page)
            markets.extend(batch)
            if len(batch) < limit:
                break
            page += 1
        return markets

    @staticmethod
    def _filter_markets(
        markets: list[dict[str, Any]],
        query: InstrumentDiscoveryQuery,
    ) -> list[dict[str, Any]]:
        """Apply local market filters not guaranteed by the venue endpoint."""
        filtered: list[dict[str, Any]] = []
        for market in markets:
            if str(market.get("tradeType") or "").lower() != "clob":
                continue

            slug = str(market.get("slug") or "")
            condition_id = str(market.get("conditionId") or "")
            market_ids = {slug, condition_id, str(market.get("id") or "")}
            if query.venue_market_id and query.venue_market_id not in market_ids:
                continue
            if query.min_volume is not None and _decimal_field(market, "volume") < query.min_volume:
                continue
            if (
                query.min_liquidity is not None
                and _decimal_field(market, "liquidity") < query.min_liquidity
            ):
                continue
            filtered.append(market)
        return filtered


def _search_queries(search_text: str) -> tuple[str, ...]:
    """Build exact and conservative token queries for one catalog lookup."""
    normalized = " ".join(search_text.split())
    tokens = tuple(
        dict.fromkeys(
            token
            for token in normalized.split()
            if len(token) >= 4
        ),
    )
    return tuple(dict.fromkeys((normalized, *tokens[:3])))


def _market_with_id(
    markets: tuple[dict[str, Any], ...],
    market_id: str,
) -> dict[str, Any] | None:
    """Return the exact numeric market match from a normalized catalog."""
    return next(
        (market for market in markets if str(market.get("id")) == market_id),
        None,
    )


def _decimal_field(market: dict[str, Any], field: str) -> Decimal:
    value = market.get(f"{field}Formatted") or market.get(field) or 0
    return Decimal(str(value))


def _filter_contracts(
    contracts: list[BinaryContract],
    query: InstrumentDiscoveryQuery,
) -> list[BinaryContract]:
    """Apply contract-level query constraints not guaranteed upstream."""
    if query.venue_token_id is None:
        return contracts
    return [
        contract
        for contract in contracts
        if parse_limitless_contract_id(contract.id)[1] == query.venue_token_id.lower()
    ]
