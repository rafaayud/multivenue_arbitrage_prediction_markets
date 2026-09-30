"""Define the core orderbook domain entities.

Responsibilities
----------------
- Model identity, state, and behavior independent of infrastructure.
"""

from dataclasses import dataclass
from typing import Literal
from prediction_markets.domain.shared.value_objects import MarketID, OutcomeID, Timestamp, Price
from prediction_markets.domain.orderbook.value_objects import OrderBookLevel
from decimal import Decimal


@dataclass(frozen=True, slots=True)
class OrderBook:
    """Represent an immutable order-book snapshot for one market outcome.

    Attributes
    ----------
    source_at_ns : int, optional
        Venue event time on the Unix wall clock, in nanoseconds.
    arrival_wall_at_ns : int, optional
        Local Unix wall time captured at the transport callback.
    arrival_at_ns : int, optional
        Local monotonic time captured at the same transport callback.
    processed_at_ns : int, optional
        Local monotonic time immediately before pipeline processing.
    source_timestamp_kind : {"venue_update", "snapshot_state"}, optional
        Whether the venue timestamp describes a live update or the last mutation
        represented by a subscription snapshot.
    received_at_ns : int, optional
        Compatibility alias for ``arrival_at_ns`` used by freshness guards.
    source_hash : str, optional
        Venue hash identifying the source book state when available.

    Invariants
    ----------
    - Bid and ask collections are present.
    - Bids are ordered by descending price, with the best bid first.
    - Asks are ordered by ascending price, with the best ask first.
    - Provided timestamps are non-negative nanosecond values.
    - Wall-clock values are never directly compared with monotonic values.
    """
    market_id: MarketID
    outcome_id: OutcomeID
    bids: tuple[OrderBookLevel, ...]
    asks: tuple[OrderBookLevel, ...]
    timestamp: Timestamp | None = None
    source_at_ns: int | None = None
    arrival_wall_at_ns: int | None = None
    arrival_at_ns: int | None = None
    processed_at_ns: int | None = None
    source_timestamp_kind: Literal["venue_update", "snapshot_state"] | None = None
    received_at_ns: int | None = None
    source_hash: str | None = None

    def __post_init__(self):
        if not self.market_id:
            raise ValueError("OrderBook must have a market ID")

        if not self.outcome_id:
            raise ValueError("OrderBook must have an outcome ID")

        if self.bids is None:
            raise ValueError("OrderBook bids cannot be None")

        if self.asks is None:
            raise ValueError("OrderBook asks cannot be None")

        for name in (
            "source_at_ns",
            "arrival_wall_at_ns",
            "arrival_at_ns",
            "processed_at_ns",
            "received_at_ns",
        ):
            value = getattr(self, name)
            if value is not None and value < 0:
                raise ValueError(f"OrderBook {name} must be non-negative")

    def best_bid(self) -> OrderBookLevel | None:
        """Return the highest bid level, or `None` when no bids exist."""
        return self.bids[0] if self.bids else None

    def best_ask(self) -> OrderBookLevel | None:
        """Return the lowest ask level, or `None` when no asks exist."""
        return self.asks[0] if self.asks else None

    def spread(self):
        """Calculate best ask minus best bid.

        Returns
        -------
        Decimal | None
            The spread, or `None` when either side is empty.
        """
        best_bid = self.best_bid()
        best_ask = self.best_ask()

        if best_bid is None or best_ask is None:
            return None

        return best_ask.price.value - best_bid.price.value

    def mid_price(self):
        """Calculate the midpoint between the best bid and ask.

        Returns
        -------
        Price | None
            The midpoint, or `None` when either side is empty.
        """
        best_bid = self.best_bid()
        best_ask = self.best_ask()

        if best_bid is None or best_ask is None:
            return None

        return Price((best_bid.price.value + best_ask.price.value) / Decimal("2"))

    def is_empty(self) -> bool:
        return not self.bids and not self.asks

    def is_crossed(self) -> bool:
        best_bid = self.best_bid()
        best_ask = self.best_ask()

        if best_bid is None or best_ask is None:
            return False

        return best_bid.price.value >= best_ask.price.value

    def __repr__(self):
        return (
            "OrderBook("
            f"market_id={self.market_id}, "
            f"outcome_id={self.outcome_id}, "
            f"bids={len(self.bids)}, "
            f"asks={len(self.asks)}, "
            f"best_bid={self.best_bid()}, "
            f"best_ask={self.best_ask()}, "
            f"spread={self.spread()}, "
            f"is_crossed={self.is_crossed()}"
            ")"
        )

    def __str__(self):
        return (
            f"OrderBook {self.outcome_id} "
            f"bid={self.best_bid() or '-'} "
            f"ask={self.best_ask() or '-'} "
            f"spread={self.spread() if self.spread() is not None else '-'} "
            f"levels={len(self.bids)}x{len(self.asks)}"
        )
