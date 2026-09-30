"""Define process-wide Prometheus metrics for the trading runtime.

Responsibilities
----------------
- Expose counters, gauges, and histograms updated by application services.
"""

import time

from prometheus_client import Counter, Gauge, Histogram

from prediction_markets.domain.market_matching.value_objects import (
    MatchedContractPair,
)
from prediction_markets.domain.orderbook.entities import OrderBook


LATENCY_BUCKETS = (
    0.001,
    0.0025,
    0.005,
    0.01,
    0.025,
    0.05,
    0.1,
    0.25,
    0.5,
    1,
    2,
    5,
)


ORDER_LATENCY = Histogram(
    "order_operation_seconds",
    "Order execution adapter latency",
    ("venue", "operation"),
    buckets=LATENCY_BUCKETS,
)
ORDER_AUTH_REFRESHES = Counter(
    "order_auth_refreshes_total",
    "Authentication refresh attempts by execution adapter and bounded outcome",
    ("venue", "reason", "result"),
)
ORDER_AUTH_READY = Gauge(
    "order_auth_ready",
    "Whether an execution adapter has a cached token outside its refresh window",
    ("venue",),
)
ORDER_AUTH_TOKEN_TTL = Gauge(
    "order_auth_token_ttl_seconds",
    "Remaining execution authentication token lifetime, or -1 when unavailable",
    ("venue",),
)
ACTIVE_RESTING_ORDERS = Gauge(
    "active_resting_orders",
    "Application-timed orders awaiting a terminal fill or cancellation",
    ("venue",),
)
ORDER_RESTING_DURATION = Histogram(
    "order_resting_seconds",
    "Time from submission monitoring to a terminal application-timed order",
    ("venue",),
    buckets=LATENCY_BUCKETS,
)
ORDER_CANCEL_ATTEMPTS = Counter(
    "order_cancel_attempts_total",
    "Application-timed cancellation attempts by venue and bounded result",
    ("venue", "result"),
)
ARBITRAGE_ORDERBOOK_AGE = Histogram(
    "arbitrage_orderbook_age_seconds",
    "Age of each order book when an arbitrage pair is evaluated",
    ("venue",),
    buckets=LATENCY_BUCKETS,
)
ARBITRAGE_ORDERBOOK_SKEW = Histogram(
    "arbitrage_orderbook_skew_seconds",
    "Receive-time skew between order books evaluated for arbitrage",
    buckets=LATENCY_BUCKETS,
)
ARBITRAGE_STAGE_LATENCY = Histogram(
    "arbitrage_stage_seconds",
    "Arbitrage execution stage latency",
    ("venue", "leg", "stage"),
    buckets=LATENCY_BUCKETS,
)
ARBITRAGE_FILL_RATIO = Histogram(
    "arbitrage_order_fill_ratio",
    "Filled quantity divided by requested quantity",
    ("venue", "leg"),
    buckets=(0, 0.25, 0.5, 0.75, 0.9, 0.99, 1),
)
ARBITRAGE_ORDER_OUTCOMES = Counter(
    "arbitrage_order_outcomes_total",
    "Terminal outcome of each arbitrage order",
    ("venue", "leg", "outcome"),
)
ARBITRAGE_EDGE_SURVIVAL = Counter(
    "arbitrage_edge_survival_observations_total",
    "Executable edge observations grouped by initial edge and local book age",
    ("initial_edge_bps", "book_age_ms", "survived"),
)


def observe_arbitrage_orderbooks(
    pair: MatchedContractPair,
    left: OrderBook,
    right: OrderBook,
) -> None:
    """Record receive age and inter-book skew for one pair evaluation.

    Parameters
    ----------
    pair
        Matched contracts whose venue labels identify each book.
    left
        Latest normalized book for the left contract.
    right
        Latest normalized book for the right contract.

    Notes
    -----
    - Books without a process-local receive timestamp are omitted.
    """
    now_ns = time.monotonic_ns()
    for contract, book in ((pair.left, left), (pair.right, right)):
        if book.received_at_ns is not None:
            ARBITRAGE_ORDERBOOK_AGE.labels(str(contract.venue_id)).observe(
                max(0, now_ns - book.received_at_ns) / 1_000_000_000,
            )
    if left.received_at_ns is not None and right.received_at_ns is not None:
        ARBITRAGE_ORDERBOOK_SKEW.observe(
            abs(left.received_at_ns - right.received_at_ns) / 1_000_000_000,
        )
