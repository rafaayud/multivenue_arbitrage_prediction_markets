"""Discover explicitly selected Hyperliquid binary outcome contracts."""

from prediction_markets.domain.ports.instrument_discovery import (
    InstrumentDiscoveryPort, InstrumentDiscoveryQuery, InstrumentDiscoveryResult,
)
from prediction_markets.domain.shared.value_objects import ContractID
from prediction_markets.infrastructure.venues.hyperliquid.catalog import HyperliquidMarketCatalog
from prediction_markets.infrastructure.venues.hyperliquid.mappers import (
    HYPERLIQUID_VENUE_ID, parse_hyperliquid_contract_id,
)


class HyperliquidInstrumentDiscoveryAdapter(InstrumentDiscoveryPort):
    """Resolve native outcome ids without automatic cross-venue matching."""

    def __init__(self, *, catalog: HyperliquidMarketCatalog | None = None) -> None:
        self._owns_catalog = catalog is None
        self._catalog = catalog or HyperliquidMarketCatalog()

    async def close(self) -> None:
        if self._owns_catalog:
            await self._catalog.close()

    async def discover(self, query: InstrumentDiscoveryQuery) -> InstrumentDiscoveryResult:
        """Filter one atomic public catalog, excluding unsupported query families."""
        if query.venue_id not in (None, HYPERLIQUID_VENUE_ID):
            return InstrumentDiscoveryResult((), ())
        if query.underlying is not None or query.interval_seconds is not None:
            return InstrumentDiscoveryResult((), ())
        if query.min_volume is not None or query.min_liquidity is not None:
            raise ValueError("Hyperliquid outcome metadata does not supply volume or liquidity")
        identifier = query.venue_market_id or query.market_slug
        if identifier and identifier.startswith("outcome:"):
            identifier = identifier.removeprefix("outcome:")
        if identifier and identifier.startswith("hyperliquid:"):
            identifier = str(parse_hyperliquid_contract_id(ContractID(identifier))[0])
        selected = []
        for market, contracts in await self._catalog.instruments():
            if identifier is not None and str(market.id) != identifier:
                continue
            if query.active_only and not market.is_active():
                continue
            if identifier is None and query.search_text and query.search_text.casefold() not in market.title.casefold():
                continue
            if query.venue_token_id and not any(contract.symbol == query.venue_token_id for contract in contracts):
                continue
            selected.append((market, contracts))
            if len(selected) == query.limit:
                break
        return InstrumentDiscoveryResult(
            tuple(market for market, _ in selected),
            tuple(contract for _, contracts in selected for contract in contracts),
        )
