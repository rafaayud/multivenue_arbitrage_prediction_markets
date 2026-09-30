"""Define immutable application values shared by the event pipeline.

Responsibilities
----------------
- Identify monitored market cycles for discovery and signal subscription.
- Carry provider-neutral regular market selections into native discovery.
"""

from dataclasses import dataclass
from enum import StrEnum
from typing import TypeAlias

from prediction_markets.domain.market_matching.value_objects import (
    RegularCandidate,
    Underlying,
)
from prediction_markets.domain.shared.value_objects import VenueID


class MarketFamily(StrEnum):
    """Classify recurring markets whose venue compatibility differs."""

    CRYPTO = "crypto"
    FINANCE = "finance"


@dataclass(frozen=True, slots=True)
class RegularMarketSelection:
    """Select one arbitrary venue market by its native external identifier.

    Attributes
    ----------
    venue_id
        Venue whose discovery adapter owns the identifier.
    external_market_id
        Native condition id, slug, or equivalent market identifier.

    Invariants
    ----------
    - The external market identifier is non-empty and trimmed.
    """

    venue_id: VenueID
    external_market_id: str
    search_text: str | None = None

    def __post_init__(self) -> None:
        market_id = self.external_market_id.strip()
        if not market_id:
            raise ValueError("Regular market selection ID must be non-empty")
        object.__setattr__(self, "external_market_id", market_id)
        if self.search_text is not None:
            search_text = self.search_text.strip()
            if not search_text:
                raise ValueError("Regular market selection search text cannot be blank")
            object.__setattr__(self, "search_text", search_text)


@dataclass(frozen=True, slots=True)
class MarketCycle:
    """Identify one underlying, prediction-window duration, and market family.

    Attributes
    ----------
    underlying
        Normalized asset symbol.
    interval_seconds
        Positive prediction-window duration in seconds.
    family
        Compatibility family used to select approved venue routes.

    Invariants
    ----------
    - ``interval_seconds`` is strictly positive.
    """

    underlying: Underlying
    interval_seconds: int
    family: MarketFamily = MarketFamily.CRYPTO

    def __post_init__(self) -> None:
        if self.interval_seconds <= 0:
            raise ValueError("MarketCycle interval_seconds must be positive")


MonitoredMarket: TypeAlias = MarketCycle | RegularCandidate


def market_expiry_guard_seconds(
    market: MonitoredMarket,
    default_seconds: int,
) -> int:
    """Return the submission safety window for one recurring market.

    Parameters
    ----------
    market
        Recurring cycle or explicitly selected regular candidate.
    default_seconds
        Fallback window used outside the 5- and 15-minute cycles.

    Returns
    -------
    int
        Thirty seconds for 5-minute cycles, sixty seconds for 15-minute
        cycles, and the supplied fallback otherwise.
    """
    if isinstance(market, MarketCycle):
        return {300: 30, 900: 60}.get(market.interval_seconds, default_seconds)
    return default_seconds


def monitored_market_label(market: MonitoredMarket) -> str:
    """Return a deterministic label for opportunity identity.

    Parameters
    ----------
    market
        Recurring cycle or explicitly selected regular candidate.

    Returns
    -------
    str
        Existing underlying symbol for cycles, or the regular candidate key.
    """
    if isinstance(market, MarketCycle):
        return market.underlying.symbol
    return f"regular:{market.key!r}"


def monitored_market_key(market: MonitoredMarket) -> str:
    """Return the stable projection key for one monitored market.

    Parameters
    ----------
    market
        Recurring cycle or explicitly selected regular candidate.

    Returns
    -------
    str
        Key that distinguishes cycle intervals and regular candidates.
    """
    if isinstance(market, MarketCycle):
        return f"cycle:{market.underlying.symbol}:{market.interval_seconds}"
    return monitored_market_label(market)
