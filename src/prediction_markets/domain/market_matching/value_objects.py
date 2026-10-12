"""Define validated value objects for the market matching domain.

Responsibilities
----------------
- Enforce domain invariants at construction time.
"""

from dataclasses import dataclass

from prediction_markets.domain.contracts.entities import BinaryContract
from prediction_markets.domain.markets.entities import Market
from prediction_markets.domain.shared.value_objects import Timestamp

_UNDERLYING_ALIASES = {
    "BTC": "BTC",
    "XBT": "BTC",
    "BITCOIN": "BTC",
    "ETH": "ETH",
    "ETHER": "ETH",
    "ETHEREUM": "ETH",
    "SOL": "SOL",
    "SOLANA": "SOL",
    "SPACEX": "SPCX",
}


@dataclass(frozen=True, slots=True)
class Underlying:
    """Represent an immutable normalized underlying symbol.

    Invariants
    ----------
    - The symbol is non-empty, uppercase, and normalized through known aliases.
    """
    symbol: str

    def __post_init__(self):
        object.__setattr__(self, "symbol", self._normalize(self.symbol))

    def _normalize(cls, value: str) -> str:
        """Normalize aliases and delimited symbols to an uppercase base asset."""
        raw = value.strip().upper()
        if not raw:
            raise ValueError("Underlying must be non-empty")

        normalized = raw.replace("-", "_").replace("/", "_")
        base = normalized.split("_", maxsplit=1)[0]

        alias = _UNDERLYING_ALIASES.get(base)
        if alias is not None:
            return alias

        if not base.isalnum():
            raise ValueError("Underlying symbol must be alphanumeric")

        return base

    def __str__(self):
        return self.symbol


@dataclass(frozen=True, slots=True)
class RegularCandidate:
    """Group venue markets asserted to represent one binary proposition.

    Attributes
    ----------
    markets
        Normalized markets supplied manually or by a matching provider.

    Invariants
    ----------
    - At least two uniquely identified markets are present.
    - The markets span at least two venues.

    Notes
    -----
    - Construction asserts semantic equivalence; discovery or explicit manual
      confirmation must establish it before creating the candidate.
    - Tradable contracts are resolved after candidate selection.
    """

    markets: tuple[Market, ...]

    def __post_init__(self) -> None:
        markets = tuple(self.markets)
        identities = tuple((market.venue_id, market.id) for market in markets)
        if len(markets) < 2:
            raise ValueError("RegularCandidate requires at least two markets")
        if len(set(identities)) != len(identities):
            raise ValueError("RegularCandidate markets must be unique")
        if len({market.venue_id for market in markets}) < 2:
            raise ValueError("RegularCandidate requires at least two venues")
        object.__setattr__(self, "markets", markets)

    @property
    def key(self) -> tuple[tuple[str, str], ...]:
        """Return a stable orientation-independent candidate key."""
        return tuple(
            sorted(
                (str(market.venue_id), str(market.id)) for market in self.markets
            ),
        )


@dataclass(frozen=True, slots=True)
class MatchedContractPair:
    """Pair complementary cross-venue contracts for one market cycle.

    Invariants
    ----------
    - The contracts belong to different venues.
    """

    left: BinaryContract
    right: BinaryContract
    ends_at: Timestamp

    def __post_init__(self) -> None:
        if self.left.venue_id == self.right.venue_id:
            raise ValueError("Matched contracts must belong to different venues")

    @property
    def key(self) -> tuple[str, str]:
        """Return a stable orientation-independent pair key."""
        return tuple(sorted((str(self.left.id), str(self.right.id))))
