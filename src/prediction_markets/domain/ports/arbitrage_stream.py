"""Define the arbitrage stream boundary required by the domain.

Responsibilities
----------------
- Specify infrastructure-neutral contracts for external effects.
"""

from abc import ABC, abstractmethod
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import timedelta
from decimal import Decimal
from typing import Literal

from prediction_markets.domain.shared.value_objects import (
    EventID,
    MarketID,
    OutcomeID,
    Timestamp,
    VenueID,
)


@dataclass(frozen=True, slots=True)
class ArbitrageVenueMarket:
    """One selectable venue market inside an external arbitrage candidate."""

    venue_id: VenueID
    market_id: MarketID
    external_market_id: str
    yes_outcome_id: OutcomeID
    no_outcome_id: OutcomeID
    title: str | None = None
    volume_usd: Decimal | None = None

    def __post_init__(self) -> None:
        if not self.external_market_id.strip():
            raise ValueError("ArbitrageVenueMarket external_market_id must be non-empty")
        if self.yes_outcome_id == self.no_outcome_id:
            raise ValueError("ArbitrageVenueMarket outcomes must be unique")
        if self.title is not None and not self.title.strip():
            raise ValueError("ArbitrageVenueMarket title cannot be blank")
        if self.volume_usd is not None and self.volume_usd < 0:
            raise ValueError("ArbitrageVenueMarket volume_usd must be non-negative")


@dataclass(frozen=True, slots=True)
class ArbitrageReturn:
    """Represent one normalized external arbitrage candidate or return update.

    Invariants
    ----------
    - Reported USD liquidity is non-negative when present.
    - Event timestamps are timezone-aware when supplied by the provider.
    """
    market_id: MarketID
    venue_event_id: EventID | None
    return_rate: Decimal
    observed_at: Timestamp
    event_title: str | None = None
    starts_at: Timestamp | None = None
    ends_at: Timestamp | None = None
    volume_usd: Decimal | None = None
    liquidity_usd: Decimal | None = None
    liquidity_tier: Literal["deep", "shallow"] | None = None
    markets: tuple[ArbitrageVenueMarket, ...] = ()

    def __post_init__(self) -> None:
        if self.event_title is not None and not self.event_title.strip():
            raise ValueError("ArbitrageReturn event_title cannot be blank")
        if self.volume_usd is not None and self.volume_usd < 0:
            raise ValueError("ArbitrageReturn volume_usd must be non-negative")
        if self.liquidity_usd is not None and self.liquidity_usd < 0:
            raise ValueError("ArbitrageReturn liquidity_usd must be non-negative")

    @property
    def key(self) -> tuple[tuple[str, str], ...]:
        """Return a stable key for one matched cross-venue market group."""
        return tuple(
            sorted(
                (str(market.venue_id), str(market.market_id))
                for market in self.markets
            ),
        )

    @property
    def title(self) -> str:
        """Return the selected venue market title with an identifier fallback."""
        selected = next(
            (market for market in self.markets if market.market_id == self.market_id),
            None,
        )
        return selected.title if selected is not None and selected.title else str(self.market_id)


class ArbitrageStreamPort(ABC):
    """Expose normalized external arbitrage candidates and live return updates."""

    @abstractmethod
    async def list_candidates(
        self,
        *,
        min_return: Decimal = Decimal("0"),
        limit: int = 100,
        topics: tuple[str, ...] = (),
        search_text: str | None = None,
        live_only: bool = False,
        min_time_to_close: timedelta | None = None,
        max_time_to_close: timedelta | None = None,
        match_statuses: tuple[Literal["matched", "verified"], ...] = (
            "matched",
            "verified",
        ),
    ) -> tuple[ArbitrageReturn, ...]:
        """
        Return candidates meeting filters without exposing the provider payload shape.

        Parameters
        ----------
        min_return
            Minimum decimal return rate accepted.
        limit
            Maximum number of candidates to return.
        topics
            Optional provider topics used to narrow the search.
        search_text
            Optional case-insensitive text required in the provider event payload.
        live_only
            Whether to require the event to have started and not yet ended.
        min_time_to_close
            Lower bound for remaining market lifetime.
        max_time_to_close
            Upper bound for remaining market lifetime.
        match_statuses
            Provider match states considered eligible.

        Returns
        -------
        tuple[ArbitrageReturn, ...]
            Normalized candidate records ordered by the adapter's venue semantics.
        """
        ...

    @abstractmethod
    async def stream_returns(self) -> AsyncIterator[ArbitrageReturn]:
        """
        Stream normalized returns until the upstream transport closes or fails.

        Yields
        ------
        ArbitrageReturn
            Return observations with venue event ids, liquidity metadata, and timestamps.
        """
        ...
