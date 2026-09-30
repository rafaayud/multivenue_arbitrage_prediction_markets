"""Carry immutable, bounded worker book lookups for exposure recovery.

Notes
-----
- Recovery reads current books without checking arbitrage profitability or the
  original intent cache. The parent owns execution authorization and loss limits.
- Request and response times use the shared host monotonic clock; book timestamps
  are preserved unchanged across IPC.
"""

import time
from dataclasses import dataclass, field

from prediction_markets.application.events import OpportunityValidationRef
from prediction_markets.application.worker_validation import WorkerValidationReason
from prediction_markets.domain.contracts.entities import BinaryContract
from prediction_markets.domain.orderbook.entities import OrderBook


RECOVERY_BOOKS_TIMEOUT_NS = 100_000_000


@dataclass(frozen=True, slots=True)
class WorkerRecoveryBooksRequest:
    """Read two approved contracts in the parent's requested recovery order.

    Attributes
    ----------
    request_id
        Unique waiter identity, never reused after timeout or cancellation.
    execution_id
        Parent-authorized execution identity echoed unchanged in the response.
    validation_ref
        Admission provenance identifying the required worker and generation.
    contracts
        Complete approved contract identities in requested return order.
    sent_monotonic_at_ns
        Request creation time in host monotonic nanoseconds. Replies are bounded
        by ``RECOVERY_BOOKS_TIMEOUT_NS`` from this timestamp.
    """

    request_id: str
    execution_id: str
    validation_ref: OpportunityValidationRef
    contracts: tuple[BinaryContract, BinaryContract]
    sent_monotonic_at_ns: int = field(default_factory=time.monotonic_ns)


@dataclass(frozen=True, slots=True)
class WorkerRecoveryBooksResult:
    """Return current immutable books and the actual responding worker identity.

    Notes
    -----
    - Books follow request order and retain all source and local timestamps.
    - Only ``accepted`` returns books; other reasons describe bounded failures.
    """

    request: WorkerRecoveryBooksRequest
    partition: str
    process_generation: str
    reason: WorkerValidationReason
    books: tuple[OrderBook, OrderBook] | None = None
    responded_monotonic_at_ns: int = field(default_factory=time.monotonic_ns)
