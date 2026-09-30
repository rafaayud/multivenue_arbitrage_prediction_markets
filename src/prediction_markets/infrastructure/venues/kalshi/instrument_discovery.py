"""Integrate kalshi instrument discovery with domain ports.

Responsibilities
----------------
- Perform venue I/O and translate external data into domain models.
"""

from typing import Any

import httpx
from prediction_markets.domain.contracts.entities import BinaryContract
from prediction_markets.domain.ports.instrument_discovery import (
    InstrumentDiscoveryPort,
    InstrumentDiscoveryQuery,
    InstrumentDiscoveryResult,
)
from prediction_markets.infrastructure.http_client import instrumented_async_client

from prediction_markets.infrastructure.venues.kalshi.mappers import (
    kalshi_market_to_contracts,
    kalshi_market_to_market,
    parse_kalshi_contract_id,
)


class KalshiMarketDiscoveryAdapter(InstrumentDiscoveryPort):
    """Async Kalshi market discovery adapter using Kalshi API directly."""
    def __init__(
        self,
        base_url: str = "https://external-api.kalshi.com/trade-api/v2",
        timeout_seconds: float = 10.0,
        client: httpx.AsyncClient | None = None) -> None:

        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")

        self._base_url = base_url.rstrip("/")
        self._own_client = client is None
        self._client = client or instrumented_async_client(
            "kalshi",
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
        """Discover normalized markets and contracts from one Kalshi payload set.

        Returns
        -------
        InstrumentDiscoveryResult
            Markets and contracts accepted by venue and query filters.
        """
        payloads = await self._discover_payloads(query)
        markets = tuple(
            market
            for payload in payloads
            if (market := kalshi_market_to_market(payload)) is not None
        )
        contracts = tuple(
            contract
            for payload in payloads
            for contract in kalshi_market_to_contracts(payload)
        )
        return InstrumentDiscoveryResult(
            markets,
            tuple(_filter_contracts(contracts, query)),
        )

    async def _discover_payloads(self, query: InstrumentDiscoveryQuery) -> tuple[dict[str, Any], ...]:
        ticker = query.market_slug or query.venue_market_id
        if ticker:
            market = await self._get_market_by_ticker(ticker)
            markets = [market]
        else:
            markets = await self._list_markets(query)

        return tuple(self._filter_markets(markets, query))

    async def _get_market_by_ticker(self, ticker: str) -> dict[str, Any]:
        """Fetch one Kalshi market and validate the response envelope."""
        response = await self._client.get(f"{self._base_url}/markets/{ticker}")
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

        return market

    async def _list_markets(self, query: InstrumentDiscoveryQuery) -> list[dict[str, Any]]:
        """Fetch paginated venue markets and validate each response page."""
        params: dict[str, Any] = {"limit": query.limit}
        if query.active_only:
            params["status"] = "open"

        response = await self._client.get(f"{self._base_url}/markets", params=params)
        response.raise_for_status()
        data = response.json()

        if not isinstance(data, dict):
            raise TypeError(f"Unexpected Kalshi markets response: {type(data).__name__}")

        markets = data.get("markets")
        if not isinstance(markets, list):
            raise TypeError("Unexpected Kalshi markets response: missing or invalid 'markets' field")
        return markets


    @staticmethod
    def _filter_markets(
        markets: list[dict[str, Any]],
        query: InstrumentDiscoveryQuery) -> list[dict[str, Any]]:

        """Apply local market filters not guaranteed by the venue endpoint."""
        filtered: list[dict[str, Any]] = []
        for market in markets:
            ticker = market.get("ticker") or market.get("market_ticker")
            if query.venue_market_id and ticker != query.venue_market_id:
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


def _decimal_field(market: dict[str, Any], field: str):
    from decimal import Decimal

    value = (
        market.get(field)
        or market.get(f"{field}_fp")
        or market.get(f"{field}_dollars")
        or market.get(f"{field}Num")
        or 0
    )
    return Decimal(str(value))


def _filter_contracts(
    contracts: tuple[BinaryContract, ...] | list[BinaryContract],
    query: InstrumentDiscoveryQuery) -> list[BinaryContract]:

    """Apply contract-level query constraints not guaranteed upstream."""
    if query.venue_token_id is None:
        return list(contracts)

    filtered: list[BinaryContract] = []
    for contract in contracts:
        _, token_id = parse_kalshi_contract_id(contract.id)
        if token_id == query.venue_token_id:
            filtered.append(contract)
    return filtered
