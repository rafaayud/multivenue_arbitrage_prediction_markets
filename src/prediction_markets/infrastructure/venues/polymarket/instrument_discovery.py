"""Integrate polymarket instrument discovery with domain ports.

Responsibilities
----------------
- Perform venue I/O and translate external data into domain models.
"""

from datetime import datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

import httpx
from prediction_markets.domain.contracts.entities import BinaryContract
from prediction_markets.domain.ports.instrument_discovery import (
    InstrumentDiscoveryPort,
    InstrumentDiscoveryQuery,
    InstrumentDiscoveryResult,
)
from prediction_markets.infrastructure.http_client import instrumented_async_client

from prediction_markets.infrastructure.venues.polymarket.mappers import gamma_market_to_contracts, gamma_market_to_market
from prediction_markets.infrastructure.venues.polymarket.mappers import parse_polymarket_contract_id


_CRYPTO_SLUG_NAMES = {
    "BTC": "bitcoin",
    "ETH": "ethereum",
    "SOL": "solana",
    "BNB": "bnb",
    "XRP": "xrp",
    "DOGE": "dogecoin",
    "HYPE": "hype",
}
_FINANCE_DAILY_SERIES = {
    "NVDA": "nvda-daily-up-down",
    "AMZN": "amzn-daily-up-down",
    "META": "meta-daily-up-down",
    "TSLA": "tsla-daily-up-down",
    "SPY": "spy-daily-up-or-down",
    "SPCX": "spcx-daily-up-or-down",
}
_NEW_YORK = ZoneInfo("America/New_York")


class PolymarketInstrumentDiscoveryAdapter(InstrumentDiscoveryPort):
    """Async Polymarket instrument discovery adapter using Gamma directly."""

    def __init__(
        self,
        gamma_base_url: str = "https://gamma-api.polymarket.com",
        timeout_seconds: float = 10.0,
        client: httpx.AsyncClient | None = None) -> None:

        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")

        self._gamma_base_url = gamma_base_url.rstrip("/")
        self._own_client = client is None
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
        if self._own_client:
            await self._client.aclose()


    async def discover(
        self,
        query: InstrumentDiscoveryQuery,
    ) -> InstrumentDiscoveryResult:
        """Discover normalized markets and contracts from one Gamma payload set.

        Returns
        -------
        InstrumentDiscoveryResult
            Markets and contracts accepted by venue and query filters.
        """
        payloads = await self._discover_payloads(query)
        return self._normalize_payloads(payloads, query)

    async def discover_many(
        self,
        queries: tuple[InstrumentDiscoveryQuery, ...],
    ) -> tuple[InstrumentDiscoveryResult, ...]:
        """Discover recurring markets with one series and one events request.

        Parameters
        ----------
        queries
            Discovery queries to execute in input order.

        Returns
        -------
        tuple[InstrumentDiscoveryResult, ...]
            One normalized result per query.

        Notes
        -----
        - Mixed or direct-selection queries use the default independent requests.
        """
        if not queries:
            return ()
        if not all(_is_recurring_query(query) for query in queries):
            return await super().discover_many(queries)

        payload_groups = await self._discover_recurring_payloads(queries)
        return tuple(
            self._normalize_payloads(payloads, query)
            for query, payloads in zip(queries, payload_groups, strict=True)
        )

    @staticmethod
    def _normalize_payloads(
        payloads: tuple[dict[str, Any], ...],
        query: InstrumentDiscoveryQuery,
    ) -> InstrumentDiscoveryResult:
        """Normalize one payload group and apply contract filters."""
        markets = tuple(
            market
            for payload in payloads
            if (market := gamma_market_to_market(payload)) is not None
        )
        contracts = tuple(
            contract
            for payload in payloads
            for contract in gamma_market_to_contracts(payload)
        )
        return InstrumentDiscoveryResult(
            markets,
            tuple(_filter_contracts(contracts, query)),
        )

    async def _discover_recurring_payloads(
        self,
        queries: tuple[InstrumentDiscoveryQuery, ...],
    ) -> tuple[tuple[dict[str, Any], ...], ...]:
        """Resolve recurring query payloads through Gamma bulk endpoints."""
        event_slugs: dict[int, str] = {}
        finance_series: dict[int, str] = {}
        previous_closes: dict[int, str] = {}

        for index, query in enumerate(queries):
            assert query.underlying is not None and query.interval_seconds is not None
            series_slug = (
                _FINANCE_DAILY_SERIES.get(query.underlying.symbol)
                if query.interval_seconds == 86400
                else None
            )
            if series_slug:
                finance_series[index] = series_slug
            else:
                event_slugs[index] = _short_form_event_slug(query)

        if finance_series:
            series_by_slug = await self._get_series_by_slugs(
                tuple(dict.fromkeys(finance_series.values())),
            )
            for index, series_slug in finance_series.items():
                series = series_by_slug.get(series_slug)
                window = _active_series_window(series) if series is not None else None
                if window is not None:
                    event_slugs[index], previous_closes[index] = window

        events_by_slug = await self._get_events_by_slugs(
            tuple(dict.fromkeys(event_slugs.values())),
        )
        payload_groups: list[tuple[dict[str, Any], ...]] = []
        for index, query in enumerate(queries):
            event = events_by_slug.get(event_slugs.get(index, ""))
            if event is None:
                payload_groups.append(())
                continue
            markets = event.get("markets")
            if not isinstance(markets, list):
                raise TypeError(
                    f"Unexpected Gamma event response for slug {event_slugs[index]}",
                )
            if index in previous_closes:
                markets = [
                    {**market, "eventStartTime": previous_closes[index]}
                    for market in markets
                ]
            payload_groups.append(tuple(self._filter_markets(markets, query)))
        return tuple(payload_groups)

    async def _get_series_by_slugs(
        self,
        series_slugs: tuple[str, ...],
    ) -> dict[str, dict[str, Any]]:
        """Fetch several Gamma series in one request and index them by slug."""
        response = await self._client.get(
            f"{self._gamma_base_url}/series",
            params=[("slug", slug) for slug in series_slugs],
        )
        response.raise_for_status()
        data = response.json()
        if not isinstance(data, list):
            raise TypeError("Unexpected Gamma bulk series response")
        return {
            str(series["slug"]): series
            for series in data
            if isinstance(series, dict) and series.get("slug")
        }

    async def _get_events_by_slugs(
        self,
        event_slugs: tuple[str, ...],
    ) -> dict[str, dict[str, Any]]:
        """Fetch several Gamma events in one request and index them by slug."""
        if not event_slugs:
            return {}
        response = await self._client.get(
            f"{self._gamma_base_url}/events",
            params=[("slug", slug) for slug in event_slugs],
        )
        response.raise_for_status()
        data = response.json()
        if not isinstance(data, list):
            raise TypeError("Unexpected Gamma bulk events response")
        return {
            str(event["slug"]): event
            for event in data
            if isinstance(event, dict) and event.get("slug")
        }

    async def _discover_payloads(self, query: InstrumentDiscoveryQuery) -> tuple[dict[str, Any], ...]:
        """Select the venue discovery route and apply local query filtering."""
        if query.underlying is not None and query.interval_seconds is not None:
            series_slug = (
                _FINANCE_DAILY_SERIES.get(query.underlying.symbol)
                if query.interval_seconds == 86400
                else None
            )
            markets = list(
                await (
                    self._get_finance_daily_markets(series_slug)
                    if series_slug
                    else self._get_event_markets(_short_form_event_slug(query))
                ),
            )
        elif query.market_slug:
            markets = [await self._get_market_by_slug(query.market_slug)]
        elif query.venue_market_id and query.venue_market_id.isdigit():
            market = await self._get_market_by_id(query.venue_market_id)
            markets = [market] if market is not None else []
        else:
            markets = await self._list_markets(query)
        return tuple(self._filter_markets(markets, query))

    async def _get_event_markets(self, event_slug: str) -> tuple[dict[str, Any], ...]:
        """Fetch all markets nested under one Polymarket event slug."""
        response = await self._client.get(f"{self._gamma_base_url}/events/slug/{event_slug}")
        if response.status_code == 404:
            return ()
        response.raise_for_status()
        data = response.json()
        if not isinstance(data, dict) or not isinstance(data.get("markets"), list):
            raise TypeError(f"Unexpected Gamma event response for slug {event_slug}")
        return tuple(data["markets"])

    async def _get_finance_daily_markets(
        self,
        series_slug: str,
    ) -> tuple[dict[str, Any], ...]:
        """Fetch the next active finance event from a stable Gamma series."""
        response = await self._client.get(
            f"{self._gamma_base_url}/series",
            params={"slug": series_slug},
        )
        response.raise_for_status()
        data = response.json()
        series = data[0] if isinstance(data, list) and data else data
        if not isinstance(series, dict):
            raise TypeError(f"Unexpected Gamma series response for {series_slug}")
        window = _active_series_window(series)
        if window is None:
            return ()
        event_slug, previous_close = window
        return tuple(
            {**market, "eventStartTime": previous_close}
            for market in await self._get_event_markets(event_slug)
        )


    async def _get_market_by_slug(self, slug: str) -> dict[str, Any]:
        """Fetch one venue market by slug and validate the response shape."""
        response = await self._client.get(f"{self._gamma_base_url}/markets/slug/{slug}")
        response.raise_for_status()
        data = response.json()
        if isinstance(data, list):
            if not data:
                raise ValueError(f"Polymarket market slug not found: {slug}")
            return data[0]
        if not isinstance(data, dict):
            raise TypeError(f"Unexpected Gamma response for slug {slug}: {type(data).__name__}")
        return data

    async def _get_market_by_id(self, market_id: str) -> dict[str, Any] | None:
        """Fetch one venue market by identifier, returning `None` on 404."""
        response = await self._client.get(f"{self._gamma_base_url}/markets/{market_id}")
        if response.status_code == 404:
            return None
        response.raise_for_status()
        data = response.json()
        if not isinstance(data, dict):
            raise TypeError(f"Unexpected Gamma response for market {market_id}")
        return data


    async def _list_markets(self, query: InstrumentDiscoveryQuery) -> list[dict[str, Any]]:
        params: dict[str, Any] = {
            "limit": query.limit,
            "active": str(query.active_only).lower(),
            "closed": str(not query.active_only).lower(),
            "archived": "false"}
        if query.venue_market_id:
            params["condition_ids"] = query.venue_market_id

        response = await self._client.get(f"{self._gamma_base_url}/markets", params=params)
        response.raise_for_status()
        data = response.json()
        if not isinstance(data, list):
            raise TypeError(f"Unexpected Gamma markets response: {type(data).__name__}")
        return data

    @staticmethod
    def _filter_markets(
        markets: list[dict[str, Any]],
        query: InstrumentDiscoveryQuery) -> list[dict[str, Any]]:

        """Apply local market filters not guaranteed by the venue endpoint."""
        filtered: list[dict[str, Any]] = []
        for market in markets:
            condition_id = market.get("conditionId") or market.get("condition_id")
            market_id = str(market.get("id") or "")
            if query.venue_market_id and query.venue_market_id not in {
                market_id,
                condition_id,
            }:
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


def _is_recurring_query(query: InstrumentDiscoveryQuery) -> bool:
    """Return whether a query can use recurring Gamma bulk discovery."""
    return (
        query.underlying is not None
        and query.interval_seconds is not None
        and query.market_slug is None
        and query.venue_market_id is None
    )


def _decimal_field(market: dict[str, Any], field: str):
    from decimal import Decimal

    value = market.get(field) or market.get(f"{field}Num") or 0
    return Decimal(str(value))


def _short_form_event_slug(
    query: InstrumentDiscoveryQuery,
    now: datetime | None = None,
) -> str:
    """Build the Polymarket event slug for a supported interval and window."""
    assert query.underlying is not None and query.interval_seconds is not None
    current = now or datetime.now(timezone.utc)
    if query.interval_seconds in {300, 900}:
        window = int(current.timestamp() // query.interval_seconds * query.interval_seconds)
        return (
            f"{query.underlying.symbol.lower()}-updown-"
            f"{query.interval_seconds // 60}m-{window}"
        )

    name = _CRYPTO_SLUG_NAMES.get(query.underlying.symbol)
    if name is None:
        raise ValueError(f"Unsupported Polymarket crypto symbol: {query.underlying}")
    local = current.astimezone(_NEW_YORK)

    if query.interval_seconds == 3600:
        hour = local.replace(minute=0, second=0, microsecond=0)
        suffix = (
            f"{hour.strftime('%B').lower()}-{hour.day}-{hour.year}-"
            f"{hour.strftime('%I').lstrip('0')}{hour.strftime('%p').lower()}-et"
        )
        return f"{name}-up-or-down-{suffix}"

    if query.interval_seconds == 86400:
        end_date = local.date() + timedelta(days=local.hour >= 12)
        suffix = f"{end_date.strftime('%B').lower()}-{end_date.day}-{end_date.year}"
        return f"{name}-up-or-down-on-{suffix}"

    raise ValueError(f"Unsupported Polymarket interval: {query.interval_seconds} seconds")


def _active_series_window(
    series: dict[str, Any],
    *,
    now: datetime | None = None,
) -> tuple[str, str] | None:
    """Return the next event slug and its previous trading-day close."""
    current = now or datetime.now(timezone.utc)
    events = series.get("events")
    if not isinstance(events, list):
        return None
    dated = tuple(
        (event, end)
        for event in events
        if isinstance(event, dict)
        and (end := _event_end(event)) is not None
    )
    upcoming = tuple(
        (event, end)
        for event, end in dated
        if event.get("active") is True
        and not event.get("closed")
        and end > current
        and event.get("slug")
    )
    if not upcoming:
        return None
    event, end = min(upcoming, key=lambda item: item[1])
    previous = max((value for _, value in dated if value < end), default=None)
    if previous is None:
        return None
    return str(event["slug"]), previous.isoformat().replace("+00:00", "Z")


def _event_end(event: dict[str, Any]) -> datetime | None:
    """Parse one Gamma event end timestamp when valid."""
    value = event.get("endDate")
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _filter_contracts(
    contracts: tuple[BinaryContract, ...] | list[BinaryContract],
    query: InstrumentDiscoveryQuery) -> list[BinaryContract]:

    """Apply contract-level query constraints not guaranteed upstream."""
    if query.venue_token_id is None:
        return list(contracts)

    filtered: list[BinaryContract] = []
    for contract in contracts:
        _, token_id = parse_polymarket_contract_id(contract.id)
        if token_id == query.venue_token_id:
            filtered.append(contract)
    return filtered
