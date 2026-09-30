"""Define the instrument discovery boundary required by the domain.

Responsibilities
----------------
- Specify infrastructure-neutral contracts for external effects.
"""

import asyncio
from abc import ABC, abstractmethod
from dataclasses import dataclass
from decimal import Decimal

from prediction_markets.domain.contracts.entities import BinaryContract
from prediction_markets.domain.market_matching.value_objects import Underlying
from prediction_markets.domain.markets.entities import Market
from prediction_markets.domain.shared.value_objects import VenueID


@dataclass(frozen=True, slots=True)
class InstrumentDiscoveryQuery:
    """Describes which tradable contracts should be discovered from a venue."""

    venue_id: VenueID | None = None
    active_only: bool = True
    limit: int = 100
    market_slug: str | None = None
    venue_market_id: str | None = None
    search_text: str | None = None
    venue_token_id: str | None = None
    min_volume: Decimal | None = None
    min_liquidity: Decimal | None = None
    underlying: Underlying | None = None
    interval_seconds: int | None = None

    def __post_init__(self):
        if self.limit <= 0:
            raise ValueError("InstrumentDiscoveryQuery limit must be positive")

        if self.market_slug is not None and not self.market_slug.strip():
            raise ValueError("InstrumentDiscoveryQuery market_slug cannot be blank")

        if self.venue_market_id is not None and not self.venue_market_id.strip():
            raise ValueError("InstrumentDiscoveryQuery venue_market_id cannot be blank")

        if self.search_text is not None and not self.search_text.strip():
            raise ValueError("InstrumentDiscoveryQuery search_text cannot be blank")

        if self.venue_token_id is not None and not self.venue_token_id.strip():
            raise ValueError("InstrumentDiscoveryQuery venue_token_id cannot be blank")

        if self.min_volume is not None and self.min_volume < 0:
            raise ValueError("InstrumentDiscoveryQuery min_volume must be non-negative")

        if self.min_liquidity is not None and self.min_liquidity < 0:
            raise ValueError("InstrumentDiscoveryQuery min_liquidity must be non-negative")

        if self.interval_seconds is not None and self.interval_seconds <= 0:
            raise ValueError("InstrumentDiscoveryQuery interval_seconds must be positive")


@dataclass(frozen=True, slots=True)
class InstrumentDiscoveryResult:
    """Contain one atomic normalized discovery result.

    Attributes
    ----------
    markets
        Venue markets accepted by the discovery query.
    contracts
        Tradable contracts derived from the same external payloads.

    Invariants
    ----------
    - Markets and contracts originate from one adapter operation.
    """

    markets: tuple[Market, ...]
    contracts: tuple[BinaryContract, ...]


class InstrumentDiscoveryPort(ABC):
    """Port for discovering domain markets and contracts without venue API details."""

    @abstractmethod
    async def discover(
        self,
        query: InstrumentDiscoveryQuery,
    ) -> InstrumentDiscoveryResult:
        """Return markets and contracts derived from one venue operation."""
        ...

    async def discover_many(
        self,
        queries: tuple[InstrumentDiscoveryQuery, ...],
    ) -> tuple[InstrumentDiscoveryResult, ...]:
        """Discover several queries while preserving their input order.

        Parameters
        ----------
        queries
            Venue queries to execute.

        Returns
        -------
        tuple[InstrumentDiscoveryResult, ...]
            One result per query in the same order.

        Notes
        -----
        - Adapters with a native bulk endpoint should override this method.
        """
        return tuple(await asyncio.gather(*(self.discover(query) for query in queries)))

    async def discover_markets(
        self,
        query: InstrumentDiscoveryQuery,
    ) -> tuple[Market, ...]:
        """Return only markets for compatibility with direct adapter consumers."""
        return (await self.discover(query)).markets

    async def discover_contracts(
        self,
        query: InstrumentDiscoveryQuery,
    ) -> tuple[BinaryContract, ...]:
        """Return only contracts for compatibility with direct adapter consumers."""
        return (await self.discover(query)).contracts
