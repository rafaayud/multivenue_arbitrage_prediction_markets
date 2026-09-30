"""Carry bounded, immutable final-validation messages between parent and worker.

Notes
-----
- Parent and spawned workers share host monotonic time for local ages and IPC
  ordering. Wall clocks remain unchanged diagnostic and source-age evidence.
- Admission provenance stays fixed across repricing; returned generations identify
  the latest complete books independently.
"""

import time
from dataclasses import dataclass, field
from enum import StrEnum

from prediction_markets.application.events import OpportunityValidationRef
from prediction_markets.application.markets.models import MonitoredMarket
from prediction_markets.domain.market_matching.value_objects import MatchedContractPair
from prediction_markets.domain.orderbook.entities import OrderBook
from prediction_markets.domain.shared.value_objects import Price, Quantity
from prediction_markets.domain.trading.enums import OrderSide


class WorkerValidationReason(StrEnum):
    """Bound validation outcomes for execution decisions and metrics labels."""

    ACCEPTED = "accepted"
    REPRICE = "reprice"
    TIMEOUT = "timeout"
    GENERATION = "generation"
    IDENTITY = "identity"
    STALE_REQUEST = "stale_request"
    STALE_LOCAL = "stale_local"
    STALE_SOURCE = "stale_source"
    MISSING_BOOK = "missing_book"
    INSUFFICIENT_DEPTH = "insufficient_depth"
    EDGE_LOST = "edge_lost"
    UNAVAILABLE = "unavailable"
    SHUTDOWN = "shutdown"
    QUEUE_FULL = "queue_full"
    ERROR = "error"


@dataclass(frozen=True, slots=True)
class WorkerOpportunityValidationRequest:
    """Check prepared leg limits and actual quantities against current worker books.

    Attributes
    ----------
    request_id
        Unique request identity, never reused after timeout or cancellation.
    validation_ref
        Original admission identity, including worker generation and book digests.
    sent_wall_at_ns
        Parent Unix request time in nanoseconds, retained for diagnostics only.
    sent_monotonic_at_ns
        Request creation time on the shared host monotonic clock. Spawned
        processes share this clock on supported Python platforms.
    max_book_age_ns
        Maximum local arrival age; source age uses the shared freshness policy.
    enforce_source_age
        Apply source age to live venue updates; snapshot-state timestamps describe
        historical mutations and are not treated as transport freshness.
    validation_deadline_at_ns
        Host monotonic deadline shared by the dispatcher and supervisor.
    """

    request_id: str
    execution_id: str
    validation_ref: OpportunityValidationRef
    cycle: MonitoredMarket
    pair: MatchedContractPair
    side: OrderSide
    left_quantity: Quantity
    right_quantity: Quantity
    left_limit_price: Price
    right_limit_price: Price
    sent_wall_at_ns: int
    max_book_age_ns: int = 100_000_000
    enforce_source_age: bool = True
    validation_deadline_at_ns: int | None = None
    sent_monotonic_at_ns: int = field(default_factory=time.monotonic_ns)


@dataclass(frozen=True, slots=True)
class WorkerOpportunityValidationResult:
    """Return a bounded outcome and complete books while echoing request identity.

    Notes
    -----
    - ``accepted`` permits the prepared limits; ``reprice`` requires the parent
      to repeat planning, risk checks, and preparation using the returned books.
    - Source, arrival and host monotonic timestamps remain unchanged across IPC.
    - Response ordering uses ``responded_monotonic_at_ns``, never wall time.
    """

    request: WorkerOpportunityValidationRequest
    reason: WorkerValidationReason
    responded_wall_at_ns: int
    left_order_book: OrderBook | None = None
    right_order_book: OrderBook | None = None
    left_book_generation: str = ""
    right_book_generation: str = ""
    responded_monotonic_at_ns: int = field(default_factory=time.monotonic_ns)
