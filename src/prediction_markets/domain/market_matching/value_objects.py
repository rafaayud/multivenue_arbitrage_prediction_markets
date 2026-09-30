"""Define validated value objects for the market matching domain.

Responsibilities
----------------
- Enforce domain invariants at construction time.
"""

from dataclasses import dataclass
from decimal import Decimal

from prediction_markets.domain.contracts.entities import BinaryContract
from prediction_markets.domain.market_matching.enums import (
    ComparisonOperator,
    ObservationMethod,
    UpDownOutcome,
)
from prediction_markets.domain.markets.entities import Market
from prediction_markets.domain.shared.value_objects import Currency, Money, Timestamp

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


@dataclass(frozen=True, slots=True, order=True)
class Strike:
    """Represent an immutable positive strike value.

    Invariants
    ----------
    - `value` is strictly positive.
    """
    value: Decimal

    def __post_init__(self):
        if self.value <= 0:
            raise ValueError("Strike must be positive")

    def __str__(self):
        return f"{self.value:.2f}"


@dataclass(frozen=True, slots=True)
class UpDownResolutionRule:
    """Describe how an immutable up-or-down market determines its outcome.

    Invariants
    ----------
    - The observation window is non-negative.
    - A provided source is normalized to non-empty uppercase text.
    """
    observation: ObservationMethod
    observation_window_seconds: int
    comparison: ComparisonOperator
    tie_outcome: UpDownOutcome
    source: str | None = None

    def __post_init__(self):
        if self.observation_window_seconds < 0:
            raise ValueError("Observation window must not be negative")
        if self.source is not None:
            source = self.source.strip().upper()
            if not source:
                raise ValueError("Resolution source cannot be blank")
            object.__setattr__(self, "source", source)


@dataclass(frozen=True, slots=True)
class UpDownMarketKey:
    """Identify immutable up-or-down market semantics across venues.

    Invariants
    ----------
    - The start precedes the end.
    - A published reference price is positive and uses the key currency.

    Notes
    -----
    - ``None`` records that a venue does not publish the reference observation.
      Application compatibility rules decide whether such keys may be compared.
    """
    underlying: Underlying
    currency: Currency
    start: Timestamp
    end: Timestamp
    reference_price: Money | None
    resolution_rule: UpDownResolutionRule

    def __post_init__(self):
        if self.start >= self.end:
            raise ValueError("Start must be before end")
        if self.reference_price is not None:
            if self.reference_price.amount <= 0:
                raise ValueError("Reference price must be positive")
            if self.reference_price.currency != self.currency:
                raise ValueError("Reference price currency must match market currency")


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
