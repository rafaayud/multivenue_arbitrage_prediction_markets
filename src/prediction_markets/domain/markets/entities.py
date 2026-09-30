"""Define the core markets domain entities.

Responsibilities
----------------
- Model identity, state, and behavior independent of infrastructure.
"""

from dataclasses import dataclass
from prediction_markets.domain.shared.value_objects import VenueID, EventID, MarketID, OutcomeID
from prediction_markets.domain.markets.value_objects import MarketState, MarketResolution
from prediction_markets.domain.markets.enums import BinaryOutcome


@dataclass(frozen=True, slots=True)
class MarketSide:
    """Represent one immutable binary outcome identified by its outcome id.

    Invariants
    ----------
    - `side` is a `BinaryOutcome`.
    """
    id: OutcomeID
    side: BinaryOutcome

    def __post_init__(self):
        if not isinstance(self.side, BinaryOutcome):
            raise TypeError("MarketSide side must be a BinaryOutcome")

    def is_yes(self) -> bool:
        return self.side.is_yes()

    def is_no(self) -> bool:
        return self.side.is_no()

    def binary_outcome(self) -> BinaryOutcome:
        return self.side

    def __repr__(self):
        return f"MarketSide(id={self.id}, side={self.side.value})"

    def __str__(self):
        return f"{self.side.value.upper()}:{self.id}"


@dataclass(frozen=True, slots=True)
class Market:
    # Universal identifiers
    """Represent an immutable venue market identified by market and venue ids.

    Invariants
    ----------
    - The title is non-empty and optional text is not blank.
    - YES and NO sides have distinct outcome ids and the expected polarity.
    """
    id: MarketID
    venue_id: VenueID

    # Descriptive attributes
    title: str
    state: MarketState

    # Binary market sides
    yes_side: MarketSide
    no_side: MarketSide

    # Optional metadata
    event_id: EventID | None = None
    description: str | None = None
    resolution: MarketResolution | None = None
    category: str | None = None

    def __post_init__(self):
        if not self.title.strip():
            raise ValueError("Market title must be non-empty")

        if self.description is not None and not self.description.strip():
            raise ValueError("Market description cannot be blank if provided")

        if not self.yes_side.is_yes():
            raise ValueError("Market yes_side must be a YES side")

        if not self.no_side.is_no():
            raise ValueError("Market no_side must be a NO side")

        if self.yes_side.id == self.no_side.id:
            raise ValueError("Market sides must have unique IDs")

    @property
    def sides(self) -> tuple[MarketSide, MarketSide]:
        return (self.yes_side, self.no_side)

    def is_active(self) -> bool:
        return self.state.is_open()

    def is_suspended(self) -> bool:
        return self.state.is_suspended()

    def is_closed(self) -> bool:
        return self.state.is_closed()

    def is_resolved(self) -> bool:
        return self.state.is_resolved()

    def is_cancelled(self) -> bool:
        return self.state.is_cancelled()

    def get_side(self, outcome_id: OutcomeID) -> MarketSide | None:
        for side in self.sides:
            if side.id == outcome_id:
                return side
        return None

    def has_side(self, outcome_id: OutcomeID) -> bool:
        return self.get_side(outcome_id) is not None

    def get_yes_side(self) -> MarketSide:
        return self.yes_side

    def get_no_side(self) -> MarketSide:
        return self.no_side

    def __repr__(self):
        return (
            f"Market("
            f"id={self.id}, "
            f"venue_id={self.venue_id}, "
            f"title={self.title!r}, "
            f"state={self.state.status.value}, "
            f"sides=2"
            f")"
        )

    def __str__(self):
        return f"{self.title} [{self.venue_id} {self.state.status.value}]"


@dataclass(frozen = True, slots = True)
class Venue:
    """Represent an immutable execution venue identified by `id`.

    Invariants
    ----------
    - The venue id and name are present.
    """
    id: VenueID
    name: str
    description: str | None = None

    def __post_init__(self):
        if not self.id:
            raise ValueError("Venue must have an ID")
        if not self.name:
            raise ValueError("Venue must have a name")

    def __repr__(self):
        return f"Venue({self.id}, {self.name}, {self.description})"

    def __str__(self):
        return f"{self.name} ({self.id})"
