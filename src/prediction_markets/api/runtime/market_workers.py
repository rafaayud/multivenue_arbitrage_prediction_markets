"""Run public market-data and detection partitions in isolated processes.

Responsibilities
----------------
- Derive worker partitions from enabled recurring cycles and approved venue routes.
- Own spawn-safe child lifecycle, bounded IPC, ordering, and process generations.
- Forward atomic opportunity book pairs to the authoritative parent runtime.
- Aggregate child timing and resource samples before crossing IPC.

Notes
-----
- Workers never create execution adapters, private order streams, or durable journals.
- The parent process remains the sole execution, risk, recovery, and accounting owner.
- CPU affinity is optional and disabled unless explicitly configured.
- Intent admission and the parent submission guard bound stale execution.
"""

import asyncio
import hashlib
import logging
import multiprocessing
import os
import queue
import threading
import time
import uuid
from collections.abc import Awaitable, Callable, Iterable
from contextlib import AsyncExitStack
from dataclasses import dataclass, field, replace
from decimal import ROUND_CEILING, ROUND_FLOOR, Decimal
from enum import StrEnum
from multiprocessing.context import BaseContext
from multiprocessing.process import BaseProcess
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

from prediction_markets.infrastructure.observability.predict_fill_study import (
    capture_status,
    observe_event as observe_fill_study,
    observe_execution_window,
    start_study,
    stop_study,
)

from prediction_markets.api.runtime.feeds import (
    MONITORED_MARKET_CYCLES,
    _MarketFeedCoordinator,
)
from prediction_markets.api.trading.runner import LiveArbitrageConfig
from prediction_markets.application.engine import TradingEngine
from prediction_markets.application.freshness import SOURCE_BOOK_MAX_AGE_MS
from prediction_markets.application.events import (
    ApplicationEvent,
    ArbitrageOpportunityFound,
    MarketMatchesUpdated,
    OpportunityValidationRef,
    OrderBookPairUpdated,
    OrderBookUpdated,
)
from prediction_markets.application.markets.matching import MarketMatcher
from prediction_markets.application.markets.models import (
    MarketCycle,
    MarketFamily,
    MonitoredMarket,
)
from prediction_markets.application.pipeline import TradingPipeline
from prediction_markets.application.recovery_books import (
    RECOVERY_BOOKS_TIMEOUT_NS,
    WorkerRecoveryBooksRequest,
    WorkerRecoveryBooksResult,
)
from prediction_markets.application.state import EventDispatcher, TradingState
from prediction_markets.application.worker_validation import (
    WorkerOpportunityValidationRequest,
    WorkerOpportunityValidationResult,
    WorkerValidationReason,
)
from prediction_markets.domain.contracts.value_objects import TickSize
from prediction_markets.domain.market_matching.value_objects import (
    MatchedContractPair,
    Underlying,
)
from prediction_markets.domain.orderbook.entities import OrderBook
from prediction_markets.domain.shared.value_objects import ContractID, Price, Quantity, VenueID
from prediction_markets.domain.trading.enums import OrderSide
from prediction_markets.infrastructure.clock_health import ClockMonitor
from prediction_markets.infrastructure.operational_metrics import (
    MARKET_WORKER_BOOK_AGE,
    MARKET_WORKER_CPU_SECONDS,
    MARKET_WORKER_EVENT_LOOP_LAG,
    MARKET_WORKER_EXITS,
    MARKET_WORKER_GENERATION,
    MARKET_WORKER_FUTURE_SOURCE,
    MARKET_WORKER_HEARTBEAT_AGE,
    MARKET_WORKER_INTENT_LATENCY,
    MARKET_WORKER_INTENTS_REJECTED,
    MARKET_WORKER_IPC_ENQUEUE_FAILURES,
    MARKET_WORKER_IPC_QUEUE_CAPACITY,
    MARKET_WORKER_IPC_QUEUE_DEPTH,
    MARKET_WORKER_IPC_QUEUE_HIGH_WATERMARK,
    MARKET_WORKER_MEMORY_BYTES,
    MARKET_WORKER_PID,
    MARKET_WORKER_RESTARTS,
    MARKET_WORKER_SHADOW_DETECTIONS,
    MARKET_WORKER_STARTS,
    MARKET_WORKER_TIMING,
    MARKET_WORKER_UP,
    MARKET_WORKER_VALIDATION_LATENCY,
    MARKET_WORKER_VALIDATION_QUEUE_CAPACITY,
    MARKET_WORKER_VALIDATION_QUEUE_DEPTH,
    MARKET_WORKER_VALIDATION_TIMEOUTS,
    MARKET_WORKER_VALIDATION_TOTAL,
    MARKET_WORKER_TRANSITIONS,
    MARKET_WORKER_WS_PAUSED,
    MARKET_WORKER_WS_QUEUE_DEPTH,
    MARKET_WORKER_WS_QUEUE_HIGH_WATERMARK,
    MARKET_WORKER_WS_QUEUE_WAIT_MAX,
    scheduled_lags,
    update_predict_fill_capture_metrics,
    update_clock_metrics,
)
from prediction_markets.infrastructure.venues.limitless.catalog import (
    LimitlessMarketCatalog,
)
from prediction_markets.infrastructure.venues.limitless.instrument_discovery import (
    LimitlessInstrumentDiscoveryAdapter,
)
from prediction_markets.infrastructure.venues.limitless.key_extraction import (
    LimitlessKeyExtractionAdapter,
)
from prediction_markets.infrastructure.venues.limitless.mappers import (
    LIMITLESS_VENUE_ID,
)
from prediction_markets.infrastructure.venues.limitless.market_data_stream import (
    LimitlessMarketDataStreamAdapter,
)
from prediction_markets.infrastructure.venues.limitless.taker_fees import (
    LimitlessTakerFeeCalculator,
)
from prediction_markets.infrastructure.venues.polymarket.instrument_discovery import (
    PolymarketInstrumentDiscoveryAdapter,
)
from prediction_markets.infrastructure.venues.polymarket.key_extraction import (
    PolymarketKeyExtractionAdapter,
)
from prediction_markets.infrastructure.venues.polymarket.mappers import (
    POLYMARKET_VENUE_ID,
)
from prediction_markets.infrastructure.venues.polymarket.market_data_stream import (
    PolymarketMarketDataStreamAdapter,
)
from prediction_markets.infrastructure.venues.polymarket.taker_fees import (
    PolymarketTakerFeeCalculator,
)
from prediction_markets.infrastructure.venues.polynode.key_extraction import (
    PolynodeKeyExtractionAdapter,
)
from prediction_markets.infrastructure.venues.predict.catalog import PredictMarketCatalog
from prediction_markets.infrastructure.venues.predict.instrument_discovery import (
    PredictInstrumentDiscoveryAdapter,
)
from prediction_markets.infrastructure.venues.predict.key_extraction import (
    PredictKeyExtractionAdapter,
)
from prediction_markets.infrastructure.venues.predict.mappers import PREDICT_VENUE_ID
from prediction_markets.infrastructure.venues.predict.market_data_stream import (
    PredictMarketDataStreamAdapter,
)
from prediction_markets.infrastructure.venues.predict.taker_fees import (
    PredictTakerFeeCalculator,
)
from prediction_markets.infrastructure.websocket_transport import (
    WebSocketQueueSample,
    WebSocketTransition,
)

_events = logging.getLogger("prediction_markets.events.market_workers")

_EVENT_QUEUE_CAPACITY = 1_024
_EVENT_BATCH_CAPACITY = 64
_METRICS_QUEUE_CAPACITY = 128
_CONTROL_QUEUE_CAPACITY = 64
_INTENT_MAX_AGE_NS = 25 * 1_000_000
_SUMMARY_INTERVAL_SECONDS = 1.0
_WORKER_HEARTBEAT_TIMEOUT_SECONDS = 5.0
_WORKER_STARTUP_TIMEOUT_SECONDS = 30.0
_VALIDATION_TIMEOUT_SECONDS = 0.025
_VALIDATION_INTENT_CAPACITY = 128


class MarketWorkerMode(StrEnum):
    """Select whether workers are disabled, observational, or authoritative."""

    DISABLED = "disabled"
    SHADOW = "shadow"
    ACTIVE = "active"


@dataclass(frozen=True, slots=True)
class MarketWorkerPartition:
    """Assign recurring cycles and approved venue routes to one process.

    Attributes
    ----------
    name
        Stable metrics and process label.
    cycles
        Non-overlapping recurring markets owned by the process.
    venues
        Exact public venue adapters composed inside the process.
    cpu_index
        Optional validated CPU affinity index.

    Invariants
    ----------
    - Names and cycle sets are non-empty.
    - At least two unique venues are present.
    """

    name: str
    cycles: tuple[MarketCycle, ...]
    venues: tuple[VenueID, ...]
    cpu_index: int | None = None

    def __post_init__(self) -> None:
        if not self.name.strip():
            raise ValueError("Market worker partition name must be non-empty")
        if not self.cycles:
            raise ValueError("Market worker partition cycles must be non-empty")
        if len(set(self.cycles)) != len(self.cycles):
            raise ValueError("Market worker partition cycles must be unique")
        if len(set(self.venues)) < 2:
            raise ValueError("Market worker partition requires two unique venues")
        if self.cpu_index is not None and self.cpu_index < 0:
            raise ValueError("Market worker CPU index must be non-negative")


@dataclass(frozen=True, slots=True)
class WorkerConfig:
    """Carry signal policy from the parent to a worker."""

    min_net_edge: Decimal
    cost_buffer: Decimal


@dataclass(frozen=True, slots=True)
class WorkerResumeHandoff:
    """Signal a worker to clear opportunity-intent deduplication."""


@dataclass(frozen=True, slots=True)
class WorkerCaptureExecution:
    """Open a bounded diagnostic window for an authoritative execution."""

    execution_id: str
    validation_ref: OpportunityValidationRef
    contracts: tuple[str, ...]
    reason: str = "execution"


@dataclass(frozen=True, slots=True)
class WorkerEvent:
    """Carry one ordered control-plane event across the process boundary."""

    partition: str
    process_generation: str
    sequence: int
    event: MarketMatchesUpdated


@dataclass(frozen=True, slots=True)
class WorkerOpportunityIntent:
    """Carry versioned books with host-monotonic IPC timing.

    Notes
    -----
    - Spawned processes share a host clock; wall time remains diagnostic data
      and is not used to reconstruct or reset a book's local arrival time.
    """

    partition: str
    process_generation: str
    sequence: int
    intent_id: str
    sent_wall_at_ns: int
    detected: ArbitrageOpportunityFound
    left_order_book: OrderBook
    right_order_book: OrderBook
    left_book_generation: str
    right_book_generation: str
    sent_monotonic_at_ns: int = field(default_factory=time.monotonic_ns)


@dataclass(frozen=True, slots=True)
class WorkerTickSizeChange:
    """Carry an authoritative stream tick in the ordered worker control flow."""

    partition: str
    process_generation: str
    sequence: int
    venue_id: VenueID
    contract_id: ContractID
    tick_size: TickSize


@dataclass(frozen=True, slots=True)
class WorkerVenueTimingSummary:
    """Aggregate one second of worker book timing for one venue."""

    venue_id: str
    samples: int
    source_to_transport_max_seconds: float | None
    arrival_to_processing_max_seconds: float | None
    arrival_to_sink_max_seconds: float | None
    sink_publish_max_seconds: float | None
    source_to_transport_raw_min_seconds: float | None = None
    source_to_transport_raw_max_seconds: float | None = None
    future_source_samples: int = 0


@dataclass(frozen=True, slots=True)
class WorkerQueueTimingSummary:
    """Carry one-second receive-queue pressure for one worker venue."""

    venue_id: str
    depth: int
    high_watermark: int
    paused_streams: int
    queue_wait_samples: int
    queue_wait_max_seconds: float | None


@dataclass(frozen=True, slots=True)
class WorkerMetricTransition:
    """Carry one immediate low-volume transport state change to the parent."""

    partition: str
    process_generation: str
    transition: WebSocketTransition


@dataclass(frozen=True, slots=True)
class WorkerRuntimeSummary:
    """Carry one bounded per-second child health and timing summary."""

    partition: str
    process_generation: str
    event_loop_lags_seconds: tuple[float, ...]
    cpu_seconds: float
    resident_memory_bytes: int | None
    event_queue_depth: int | None
    timings: tuple[WorkerVenueTimingSummary, ...]
    queues: tuple[WorkerQueueTimingSummary, ...]
    enqueue_drops: tuple[tuple[str, str, int], ...]
    capture: dict[str, object] | None = None
    clock: dict[str, int | float | None] | None = None


@dataclass(frozen=True, slots=True)
class WorkerFailure:
    """Report an unrecoverable child-process failure immediately."""

    partition: str
    process_generation: str
    error: str


def configured_worker_mode(value: str | None = None) -> MarketWorkerMode:
    """Parse the disabled-by-default market-worker rollout mode.

    Parameters
    ----------
    value
        Explicit mode, or ``None`` to read ``MARKET_DATA_WORKERS_MODE``.

    Returns
    -------
    MarketWorkerMode
        Normalized disabled, shadow, or active mode.

    Raises
    ------
    ValueError
        If the configured mode is unsupported.
    """
    raw = value if value is not None else os.getenv(
        "MARKET_DATA_WORKERS_MODE",
        MarketWorkerMode.DISABLED.value,
    )
    try:
        return MarketWorkerMode(raw.strip().lower())
    except ValueError as error:
        raise ValueError(
            "MARKET_DATA_WORKERS_MODE must be disabled, shadow, or active"
        ) from error


def configured_worker_affinity(value: str | None = None) -> dict[str, int]:
    """Parse optional ``partition=cpu`` assignments and verify visible CPUs.

    Parameters
    ----------
    value
        Comma-separated assignments. An empty value leaves OS scheduling intact.

    Returns
    -------
    dict[str, int]
        Explicit partition-to-CPU assignments.

    Raises
    ------
    ValueError
        If an assignment is malformed or names a CPU unavailable to the process.
    """
    raw = value if value is not None else os.getenv("MARKET_DATA_WORKER_AFFINITY", "")
    if not raw.strip():
        return {}
    affinity: dict[str, int] = {}
    try:
        for item in raw.split(","):
            name, cpu = item.split("=", maxsplit=1)
            normalized = name.strip()
            if not normalized or normalized in affinity:
                raise ValueError
            affinity[normalized] = int(cpu.strip())
    except ValueError as error:
        raise ValueError(
            "MARKET_DATA_WORKER_AFFINITY must contain unique partition=cpu entries"
        ) from error
    available = (
        set(os.sched_getaffinity(0))
        if hasattr(os, "sched_getaffinity")
        else set(range(os.cpu_count() or 1))
    )
    if any(cpu not in available for cpu in affinity.values()):
        raise ValueError("Market worker affinity contains a CPU unavailable at runtime")
    return affinity


def market_worker_partitions(
    cycles: Iterable[MarketCycle] | None = None,
    *,
    enabled_names: Iterable[str] | None = None,
    affinity: dict[str, int] | None = None,
) -> tuple[MarketWorkerPartition, ...]:
    """Build generic fast-asset and shared slow-crypto partitions.

    Parameters
    ----------
    cycles
        Enabled recurring cycles. Defaults to the runtime's monitored set.
    enabled_names
        Optional subset of partition names selected for rollout.
    affinity
        Optional validated CPU assignments.

    Returns
    -------
    tuple[MarketWorkerPartition, ...]
        Non-empty partitions in deterministic deployment order.

    Raises
    ------
    ValueError
        If a cycle is assigned twice or an unknown partition is requested.
    """
    selected_cycles = tuple(
        MONITORED_MARKET_CYCLES.values() if cycles is None else cycles
    )
    by_key = {
        (cycle.underlying.symbol, cycle.interval_seconds): cycle
        for cycle in selected_cycles
    }
    fast_venues = (POLYMARKET_VENUE_ID, LIMITLESS_VENUE_ID)
    slow_venues = (POLYMARKET_VENUE_ID, PREDICT_VENUE_ID)
    specs = (
        ("btc-5m", (("BTC", 300),), fast_venues),
        ("btc-15m", (("BTC", 900),), fast_venues),
        ("eth-5m", (("ETH", 300),), fast_venues),
        ("eth-15m", (("ETH", 900),), fast_venues),
        (
            "crypto-slow",
            tuple(
                (symbol, interval)
                for symbol in ("BTC", "ETH", "BNB")
                for interval in (3_600, 86_400)
            ),
            slow_venues,
        ),
    )
    requested = set(
        (name for name, _, _ in specs)
        if enabled_names is None
        else enabled_names
    )
    known = {name for name, _, _ in specs}
    unknown = requested - known
    if unknown:
        raise ValueError(f"Unknown market worker partitions: {', '.join(sorted(unknown))}")
    affinity = affinity or {}
    unknown_affinity = set(affinity) - known
    if unknown_affinity:
        raise ValueError(
            "Unknown market worker affinity partitions: "
            + ", ".join(sorted(unknown_affinity))
        )
    partitions = tuple(
        MarketWorkerPartition(
            name,
            tuple(by_key[key] for key in keys if key in by_key),
            venues,
            affinity.get(name),
        )
        for name, keys, venues in specs
        if name in requested and any(key in by_key for key in keys)
    )
    assigned = tuple(cycle for partition in partitions for cycle in partition.cycles)
    if len(set(assigned)) != len(assigned):
        raise ValueError("A recurring cycle cannot belong to multiple workers")
    return partitions


def configured_worker_partitions() -> tuple[MarketWorkerPartition, ...]:
    """Build partitions selected by environment without requiring CPU affinity.

    Returns
    -------
    tuple[MarketWorkerPartition, ...]
        Enabled partitions with any optional validated affinity assignments.
    """
    raw_names = os.getenv("MARKET_DATA_WORKER_PARTITIONS", "")
    names = tuple(name.strip() for name in raw_names.split(",") if name.strip())
    return market_worker_partitions(
        enabled_names=names or None,
        affinity=configured_worker_affinity(),
    )


def order_book_generation(book: OrderBook) -> str:
    """Return a deterministic identity for one complete source book state.

    Parameters
    ----------
    book
        Immutable book including source and local arrival identity.

    Returns
    -------
    str
        BLAKE2 digest used to verify the transferred snapshot identity.
    """
    digest = hashlib.blake2b(digest_size=16)
    values: tuple[object, ...] = (
        book.market_id,
        book.outcome_id,
        book.source_hash,
        book.source_at_ns,
        book.arrival_wall_at_ns,
        book.arrival_at_ns,
        book.source_timestamp_kind,
        *((level.price.value, level.quantity.value) for level in book.bids),
        "asks",
        *((level.price.value, level.quantity.value) for level in book.asks),
    )
    for value in values:
        digest.update(str(value).encode())
        digest.update(b"\0")
    return digest.hexdigest()


def source_book_generation(book: OrderBook) -> str:
    """Return a transport-independent source-state identity for shadow checks.

    Parameters
    ----------
    book
        Immutable book whose local arrival clocks are intentionally ignored.

    Returns
    -------
    str
        BLAKE2 digest used to compare independently received source states.
    """
    digest = hashlib.blake2b(digest_size=16)
    values: tuple[object, ...] = (
        book.market_id,
        book.outcome_id,
        book.source_hash,
        book.source_at_ns,
        book.source_timestamp_kind,
        *((level.price.value, level.quantity.value) for level in book.bids),
        "asks",
        *((level.price.value, level.quantity.value) for level in book.asks),
    )
    for value in values:
        digest.update(str(value).encode())
        digest.update(b"\0")
    return digest.hexdigest()


class _WorkerTelemetry:
    """Accumulate hot-path samples locally and snapshot them once per second."""

    def __init__(
        self,
        partition: str,
        process_generation: str,
        metrics_output: Any | None = None,
    ) -> None:
        self._partition = partition
        self._process_generation = process_generation
        self._metrics_output = metrics_output
        self._timings: dict[str, list[float | int | None]] = {}
        self._queue_states: dict[tuple[str, str], list[int | bool]] = {}
        self._known_queue_venues: set[str] = set()
        self._queue_high_watermarks: dict[str, int] = {}
        self._queue_waits: dict[str, list[float | int | None]] = {}
        self._drops: dict[tuple[str, str], int] = {}
        self._clock = ClockMonitor()
        self._clock.sample()
        self._lock = threading.Lock()

    def observe_processing(self, event: ApplicationEvent) -> None:
        """Aggregate timing from one normalized book without performing IPC."""
        observe_fill_study(event)
        if not isinstance(event, OrderBookUpdated):
            return
        book = event.order_book
        venue = str(event.venue_id)
        with self._lock:
            values = self._timings.setdefault(venue, [0, None, None, None, None, None, None, 0])
            values[0] = int(values[0]) + 1
            if book.source_at_ns is not None and book.arrival_wall_at_ns is not None:
                raw_age = (book.arrival_wall_at_ns - book.source_at_ns) / 1_000_000_000
                values[5] = raw_age if values[5] is None else min(float(values[5]), raw_age)
                values[6] = raw_age if values[6] is None else max(float(values[6]), raw_age)
                values[7] = int(values[7]) + int(raw_age < 0)
                values[1] = max(
                    float(values[1] or 0),
                    max(0, book.arrival_wall_at_ns - book.source_at_ns)
                    / 1_000_000_000,
                )
            if book.arrival_at_ns is not None and book.processed_at_ns is not None:
                values[2] = max(
                    float(values[2] or 0),
                    max(0, book.processed_at_ns - book.arrival_at_ns)
                    / 1_000_000_000,
                )

    def observe_sink(
        self,
        venue_id: VenueID,
        book: OrderBook,
        publish_started_ns: int,
        published_at_ns: int,
    ) -> None:
        """Aggregate adapter-to-sink and sink-publication timings locally."""
        with self._lock:
            values = self._timings.setdefault(
                str(venue_id),
                [0, None, None, None, None, None, None, 0],
            )
            if book.arrival_at_ns is not None:
                values[3] = max(
                    float(values[3] or 0),
                    max(0, published_at_ns - book.arrival_at_ns) / 1_000_000_000,
                )
            values[4] = max(
                float(values[4] or 0),
                max(0, published_at_ns - publish_started_ns) / 1_000_000_000,
            )

    def observe_queue(self, sample: WebSocketQueueSample) -> None:
        """Merge one adapter queue sample without crossing process IPC."""
        with self._lock:
            self._known_queue_venues.add(sample.venue)
            if sample.high_watermark is not None:
                self._queue_high_watermarks[sample.venue] = max(
                    self._queue_high_watermarks.get(sample.venue, 0),
                    sample.high_watermark,
                )
            if sample.removed:
                self._queue_states.pop((sample.venue, sample.stream_id), None)
                return
            if sample.depth is not None:
                key = sample.venue, sample.stream_id
                state = self._queue_states.setdefault(key, [0, 0, False])
                state[0] = sample.depth
                state[1] = max(int(state[1]), sample.high_watermark or sample.depth)
                if sample.paused is not None:
                    state[2] = sample.paused
            if sample.queue_wait_seconds is not None:
                wait = self._queue_waits.setdefault(sample.venue, [0, None])
                wait[0] = int(wait[0]) + 1
                wait[1] = max(
                    float(wait[1] or 0),
                    sample.queue_wait_seconds,
                )

    def observe_transition(self, transition: WebSocketTransition) -> None:
        """Forward one rare transport transition immediately and non-blockingly."""
        if self._metrics_output is None:
            return
        try:
            self._metrics_output.put_nowait(
                WorkerMetricTransition(
                    self._partition,
                    self._process_generation,
                    transition,
                )
            )
        except queue.Full:
            self.record_drop("metrics_transition")

    def record_drop(self, queue_name: str, reason: str = "full") -> None:
        """Count one bounded-queue enqueue failure for the next summary."""
        key = queue_name, reason
        with self._lock:
            self._drops[key] = self._drops.get(key, 0) + 1

    def snapshot(
        self,
        loop_lag_seconds: float | tuple[float, ...],
        events: Any,
    ) -> WorkerRuntimeSummary:
        """Return and reset one interval of aggregated child telemetry."""
        with self._lock:
            timings = tuple(
                WorkerVenueTimingSummary(
                    venue,
                    int(values[0]),
                    float(values[1]) if values[1] is not None else None,
                    float(values[2]) if values[2] is not None else None,
                    float(values[3]) if values[3] is not None else None,
                    float(values[4]) if values[4] is not None else None,
                    float(values[5]) if values[5] is not None else None,
                    float(values[6]) if values[6] is not None else None,
                    int(values[7]),
                )
                for venue, values in self._timings.items()
            )
            venues = self._known_queue_venues | set(self._queue_waits)
            queues = tuple(
                WorkerQueueTimingSummary(
                    venue,
                    max(
                        (
                            int(state[0])
                            for (candidate, _), state in self._queue_states.items()
                            if candidate == venue
                        ),
                        default=0,
                    ),
                    max(
                        self._queue_high_watermarks.get(venue, 0),
                        max(
                            (
                                int(state[1])
                                for (candidate, _), state in self._queue_states.items()
                                if candidate == venue
                            ),
                            default=0,
                        ),
                    ),
                    sum(
                        bool(state[2])
                        for (candidate, _), state in self._queue_states.items()
                        if candidate == venue
                    ),
                    int(self._queue_waits.get(venue, [0, None])[0]),
                    (
                        float(self._queue_waits[venue][1])
                        if venue in self._queue_waits
                        and self._queue_waits[venue][1] is not None
                        else None
                    ),
                )
                for venue in sorted(venues)
            )
            drops = tuple(
                (name, reason, count)
                for (name, reason), count in self._drops.items()
            )
            self._timings.clear()
            self._queue_waits.clear()
            self._drops.clear()
        summary = WorkerRuntimeSummary(
            self._partition,
            self._process_generation,
            (
                loop_lag_seconds
                if isinstance(loop_lag_seconds, tuple)
                else (loop_lag_seconds,)
            ),
            time.process_time(),
            _resident_memory_bytes(),
            _queue_size(events),
            timings,
            queues,
            drops,
            capture_status(),
            self._clock.sample(),
        )
        return summary


class _WorkerJournal:
    """Forward matches and bounded executable intents without durable writes."""

    def __init__(
        self,
        partition: str,
        output: Any,
        state: TradingState,
        process_generation: str,
        telemetry: _WorkerTelemetry | None = None,
    ) -> None:
        self._partition = partition
        self._output = output
        self._state = state
        self._process_generation = process_generation
        self._telemetry = telemetry
        self._sequence = 0
        self._last_intent_snapshot: tuple[object, ...] | None = None
        self._validation_intents: dict[str, WorkerOpportunityIntent] = {}

    def clear_intent_dedup(self) -> None:
        """Allow the next unchanged book snapshot to cross IPC again."""
        self._last_intent_snapshot = None

    def publish_tick_size(
        self, venue_id: VenueID, contract_id: ContractID, tick_size: TickSize,
    ) -> None:
        """Enqueue a rare metadata change before subsequent worker book intents.

        Raises
        ------
        RuntimeError
            If bounded IPC is full; the feed must stop instead of losing a tick.
        """
        self._sequence += 1
        message = WorkerTickSizeChange(
            self._partition, self._process_generation, self._sequence,
            venue_id, contract_id, tick_size,
        )
        try:
            self._output.put_nowait(message)
        except queue.Full:
            if self._telemetry is not None:
                self._telemetry.record_drop("events")
            raise RuntimeError("Worker tick-size event queue is full") from None

    def append(self, event: ApplicationEvent) -> None:
        """Forward matches or one atomic book-pair intent without blocking."""
        observe_fill_study(event)
        if isinstance(event, ArbitrageOpportunityFound):
            left = self._state.books.get(event.pair.left.id)
            right = self._state.books.get(event.pair.right.id)
            if left is None or right is None:
                raise RuntimeError("Worker opportunity has no complete book pair")
            left_generation = order_book_generation(left)
            right_generation = order_book_generation(right)
            snapshot = (
                event.cycle,
                event.pair,
                event.opportunity.side,
                left_generation,
                right_generation,
            )
            if snapshot == self._last_intent_snapshot:
                return
            self._sequence += 1
            intent_id = f"{self._process_generation}:{self._sequence}"
            message: WorkerEvent | WorkerOpportunityIntent = WorkerOpportunityIntent(
                self._partition,
                self._process_generation,
                self._sequence,
                intent_id,
                time.time_ns(),
                event,
                left,
                right,
                left_generation,
                right_generation,
            )
        elif isinstance(event, MarketMatchesUpdated):
            self._sequence += 1
            message = WorkerEvent(
                self._partition,
                self._process_generation,
                self._sequence,
                event,
            )
        else:
            return
        try:
            self._output.put_nowait(message)
        except queue.Full:
            if self._telemetry is not None:
                self._telemetry.record_drop("events")
            if isinstance(message, WorkerEvent):
                raise RuntimeError("Worker match event queue is full") from None
            return
        if isinstance(message, WorkerOpportunityIntent):
            self._last_intent_snapshot = snapshot
            self._validation_intents[message.intent_id] = message
            if len(self._validation_intents) > _VALIDATION_INTENT_CAPACITY:
                self._validation_intents.pop(next(iter(self._validation_intents)))


def _validate_worker_opportunity(
    request: WorkerOpportunityValidationRequest,
    engine: TradingEngine,
    journal: _WorkerJournal,
) -> WorkerOpportunityValidationResult:
    """Check current local books, complete leg depth, fees, and configured edge.

    Notes
    -----
    - The original emitted intent authenticates the pair and direction, while
      book generations may change without invalidating executable prices.
    - Each leg uses its actual quantity and its worst executable tick price.
      BUY payout is the smaller leg; SELL liability is the larger leg, so an
      unmatched quantity never receives an assumed profitable valuation.
    - A better Polymarket BUY price requests a bounded reprice so its FAK
      notional cannot turn the improvement into avoidable excess shares.
    - No I/O or await occurs while reading and evaluating the two local books.
    """
    reason = WorkerValidationReason
    reference = request.validation_ref
    left = engine.state.books.get(request.pair.left.id)
    right = engine.state.books.get(request.pair.right.id)

    def result(outcome: WorkerValidationReason) -> WorkerOpportunityValidationResult:
        return WorkerOpportunityValidationResult(
            request, outcome, time.time_ns(), left, right,
            order_book_generation(left) if left is not None else "",
            order_book_generation(right) if right is not None else "",
        )

    if (
        reference.partition != journal._partition
        or reference.process_generation != journal._process_generation
    ):
        return result(reason.GENERATION)
    original = journal._validation_intents.get(reference.intent_id)
    if (
        original is None
        or request.execution_id != original.detected.id
        or reference.sequence != original.sequence
        or reference.left_book_generation != original.left_book_generation
        or reference.right_book_generation != original.right_book_generation
        or request.cycle != original.detected.cycle
        or request.pair != original.detected.pair
        or request.side is not original.detected.opportunity.side
        or request.pair not in engine.state.matches.get(request.cycle, ())
        or not request.request_id or not request.execution_id
        or request.max_book_age_ns <= 0
        or request.left_quantity.value <= 0 or request.right_quantity.value <= 0
    ):
        return result(reason.IDENTITY)
    now_wall = time.time_ns()
    now_local = time.monotonic_ns()
    if not 0 <= now_local - request.sent_monotonic_at_ns <= _INTENT_MAX_AGE_NS:
        return result(reason.STALE_REQUEST)
    if left is None or right is None:
        return result(reason.MISSING_BOOK)
    config = engine._config
    if config is None:
        return result(reason.UNAVAILABLE)
    for contract, book in ((request.pair.left, left), (request.pair.right, right)):
        if contract.market_id != book.market_id or contract.outcome_id != book.outcome_id:
            return result(reason.IDENTITY)
        if (
            book.arrival_wall_at_ns is None
            or book.received_at_ns is None
            or not 0 <= now_local - book.received_at_ns <= request.max_book_age_ns
        ):
            return result(reason.STALE_LOCAL)
        if request.enforce_source_age and book.source_timestamp_kind == "venue_update":
            if (
                book.source_at_ns is None
                or now_wall - book.source_at_ns > SOURCE_BOOK_MAX_AGE_MS * 1_000_000
            ):
                return result(reason.STALE_SOURCE)
    assert left.received_at_ns is not None and right.received_at_ns is not None
    if abs(left.received_at_ns - right.received_at_ns) > config.max_skew_ms * 1_000_000:
        return result(reason.STALE_LOCAL)
    current_prices: list[Price] = []
    for contract, book, quantity in (
        (request.pair.left, left, request.left_quantity),
        (request.pair.right, right, request.right_quantity),
    ):
        remaining = quantity.value
        levels = book.asks if request.side is OrderSide.BUY else book.bids
        for level in levels:
            remaining -= level.quantity.value
            if remaining <= 0:
                value = level.price.value
                if contract.tick_size is not None:
                    tick = contract.tick_size.value
                    value = (value / tick).to_integral_value(
                        rounding=ROUND_CEILING if request.side is OrderSide.BUY else ROUND_FLOOR,
                    ) * tick
                if not 0 < value < 1:
                    return result(reason.INSUFFICIENT_DEPTH)
                current_prices.append(Price(value))
                break
        else:
            return result(reason.INSUFFICIENT_DEPTH)

    def profitable(prices: tuple[Price, Price] | list[Price]) -> bool:
        """Apply settlement fees at each actual quantity and worst leg limits."""
        quantities = (request.left_quantity, request.right_quantity)
        fees = tuple(
            engine._fees[contract.venue_id].calculate(contract.id, price, quantity, request.side)
            for contract, price, quantity in zip(
                (request.pair.left, request.pair.right), prices, quantities, strict=True,
            )
        )
        if len({fee.settlement_cost.currency for fee in fees}) != 1:
            return False
        total = sum(
            (price.value * quantity.value for price, quantity in zip(prices, quantities, strict=True)),
            Decimal("0"),
        )
        hedged = min(quantity.value for quantity in quantities)
        gross = hedged - total if request.side is OrderSide.BUY else total - max(quantity.value for quantity in quantities)
        net = (gross - sum((fee.settlement_cost.amount for fee in fees), Decimal("0"))) / hedged
        return net - config.cost_buffer > config.min_net_edge

    prepared_prices = (request.left_limit_price, request.right_limit_price)
    marketable = all(
        current.value <= prepared.value if request.side is OrderSide.BUY else current.value >= prepared.value
        for current, prepared in zip(current_prices, prepared_prices, strict=True)
    )
    polymarket_buy_improved = request.side is OrderSide.BUY and any(
        contract.venue_id == POLYMARKET_VENUE_ID and current.value < prepared.value
        for contract, current, prepared in zip(
            (request.pair.left, request.pair.right), current_prices, prepared_prices, strict=True,
        )
    )
    if marketable and profitable(prepared_prices) and not polymarket_buy_improved:
        return result(reason.ACCEPTED)
    return result(reason.REPRICE if profitable(current_prices) else reason.EDGE_LOST)


def _read_worker_recovery_books(
    request: WorkerRecoveryBooksRequest,
    journal: _WorkerJournal,
) -> WorkerRecoveryBooksResult:
    """Read an approved pair atomically without requiring a profitable intent.

    Notes
    -----
    - The parent authorizes the execution; this read-only handler checks worker
      ownership, current match identities, and the bounded host-monotonic age.
    - No I/O or await occurs between the two in-memory book reads.
    """
    reason = WorkerValidationReason
    reference = request.validation_ref

    def result(
        outcome: WorkerValidationReason,
        books: tuple[OrderBook, OrderBook] | None = None,
    ) -> WorkerRecoveryBooksResult:
        return WorkerRecoveryBooksResult(
            request, journal._partition, journal._process_generation, outcome, books,
        )

    if (reference.partition != journal._partition
            or reference.process_generation != journal._process_generation):
        return result(reason.GENERATION)
    if not 0 <= time.monotonic_ns() - request.sent_monotonic_at_ns <= RECOVERY_BOOKS_TIMEOUT_NS:
        return result(reason.TIMEOUT)
    if (not request.request_id or not request.execution_id
            or len(request.contracts) != 2
            or request.contracts[0].id == request.contracts[1].id
            or not any(
                request.contracts in ((pair.left, pair.right), (pair.right, pair.left))
                for pairs in journal._state.matches.values() for pair in pairs
            )):
        return result(reason.IDENTITY)
    left, right = (journal._state.books.get(contract.id) for contract in request.contracts)
    if left is None or right is None:
        return result(reason.MISSING_BOOK)
    if any(
        book.market_id != contract.market_id or book.outcome_id != contract.outcome_id
        for contract, book in zip(request.contracts, (left, right), strict=True)
    ):
        return result(reason.IDENTITY)
    return result(reason.ACCEPTED, (left, right))


class MarketWorkerSupervisor:
    """Own generic market-data workers and their parent-side IPC lifecycle."""

    def __init__(
        self,
        pipeline: TradingPipeline,
        *,
        min_net_edge: Decimal,
        cost_buffer: Decimal,
        mode: MarketWorkerMode | str | None = None,
        partitions: tuple[MarketWorkerPartition, ...] | None = None,
        context: BaseContext | None = None,
        central_engine: TradingEngine | None = None,
        on_cycle_matches: Callable[[MarketMatchesUpdated], Awaitable[None]]
        | None = None,
        on_tick_size_change: Callable[[VenueID, ContractID, TickSize], None]
        | None = None,
        worker_target: Callable[..., None] | None = None,
        heartbeat_timeout_seconds: float = _WORKER_HEARTBEAT_TIMEOUT_SECONDS,
        startup_timeout_seconds: float = _WORKER_STARTUP_TIMEOUT_SECONDS,
        validation_timeout_seconds: float = _VALIDATION_TIMEOUT_SECONDS,
    ) -> None:
        """Configure disabled-by-default worker supervision.

        Parameters
        ----------
        pipeline
            Authoritative parent pipeline receiving validated atomic pairs.
        min_net_edge
            Worker detection threshold per contract.
        cost_buffer
            Worker detection cost allowance per contract.
        mode
            Disabled, shadow, or active rollout behavior.
        partitions
            Explicit partitions, primarily for staged rollout and tests.
        context
            Multiprocessing context; production defaults to ``spawn``.
        central_engine
            Authoritative detector used only to compare shadow snapshots.
        on_cycle_matches
            Parent callback that warms authoritative execution state.
        on_tick_size_change
            Synchronous in-memory callback applied before later IPC intents.
        worker_target
            Spawn target override used by process-lifecycle tests.
        heartbeat_timeout_seconds
            Maximum time without a runtime summary before a live process is
            considered wedged and restarted.
        startup_timeout_seconds
            Maximum process-start time before the first runtime summary. This
            accounts for ``spawn`` imports and adapter initialization.
        validation_timeout_seconds
            Bounded request round-trip deadline; late replies never authorize an order.

        Raises
        ------
        ValueError
            If either timeout is not positive.
        """
        if heartbeat_timeout_seconds <= 0:
            raise ValueError("heartbeat_timeout_seconds must be positive")
        if startup_timeout_seconds <= 0:
            raise ValueError("startup_timeout_seconds must be positive")
        if validation_timeout_seconds <= 0:
            raise ValueError("validation_timeout_seconds must be positive")
        self._pipeline = pipeline
        self.mode = MarketWorkerMode(mode) if mode is not None else configured_worker_mode()
        self._partitions = (
            configured_worker_partitions() if partitions is None else partitions
        )
        self._context = context or multiprocessing.get_context("spawn")
        self._config = WorkerConfig(
            min_net_edge,
            cost_buffer,
        )
        self._central_engine = central_engine
        self._on_cycle_matches = on_cycle_matches
        self._on_tick_size_change = on_tick_size_change
        self._worker_matches: dict[MonitoredMarket, MarketMatchesUpdated] = {}
        self._worker_target = worker_target or _market_worker_entry
        self._heartbeat_timeout_seconds = heartbeat_timeout_seconds
        self._startup_timeout_seconds = startup_timeout_seconds
        self._validation_timeout_seconds = validation_timeout_seconds
        self._pending_validations: dict[
            str, tuple[WorkerOpportunityValidationRequest, asyncio.Future[WorkerOpportunityValidationResult]]
        ] = {}
        self._pending_recovery_books: dict[
            str, tuple[WorkerRecoveryBooksRequest, asyncio.Future[WorkerRecoveryBooksResult]]
        ] = {}
        self._processes: dict[str, BaseProcess] = {}
        self._process_generations: dict[str, str] = {}
        self._generation_numbers: dict[str, int] = {}
        self._stops: dict[str, Any] = {}
        self._controls: dict[str, Any] = {}
        self._events_queue: Any | None = None
        self._metrics_queue: Any | None = None
        self._tasks: tuple[asyncio.Task[None], ...] = ()
        self._pending_match_preparations: dict[
            MonitoredMarket, MarketMatchesUpdated
        ] = {}
        self._match_preparation_wakeup = asyncio.Event()
        self._message_sequences: dict[tuple[str, str], int] = {}
        self._ipc_high_watermark = 0
        self._worker_started_at: dict[str, float] = {}
        self._last_summary_at: dict[str, float] = {}
        self._stopping = False
        self._failed = asyncio.Event()
        self.error: BaseException | None = None
        self.latest_summaries: dict[str, WorkerRuntimeSummary] = {}
        for partition in self._partitions:
            MARKET_WORKER_VALIDATION_QUEUE_CAPACITY.labels(partition.name).set(_CONTROL_QUEUE_CAPACITY)
            MARKET_WORKER_VALIDATION_QUEUE_DEPTH.labels(partition.name).set(0)

    @property
    def enabled(self) -> bool:
        """Return whether worker processes should run."""
        return self.mode is not MarketWorkerMode.DISABLED

    @property
    def cycles(self) -> tuple[MarketCycle, ...]:
        """Return cycles removed from the parent feed in active mode."""
        if self.mode is not MarketWorkerMode.ACTIVE:
            return ()
        return tuple(cycle for partition in self._partitions for cycle in partition.cycles)

    @property
    def monitored_cycles(self) -> tuple[MarketCycle, ...]:
        """Return all recurring cycles observed by enabled child processes."""
        if not self.enabled:
            return ()
        return tuple(cycle for partition in self._partitions for cycle in partition.cycles)

    @property
    def running(self) -> bool:
        """Report whether every enabled worker process is responsive."""
        return not self.enabled or (
            bool(self._processes)
            and all(
                self._worker_healthy(partition.name)
                for partition in self._partitions
            )
        )

    async def start(self) -> None:
        """Spawn workers and start parent-side IPC consumers idempotently."""
        if not self.enabled or self._processes:
            return
        self.error = None
        self._failed.clear()
        self._stopping = False
        self._pending_match_preparations.clear()
        self._match_preparation_wakeup.clear()
        self._events_queue = self._context.Queue(maxsize=_EVENT_QUEUE_CAPACITY)
        self._metrics_queue = self._context.Queue(maxsize=_METRICS_QUEUE_CAPACITY)
        MARKET_WORKER_IPC_QUEUE_CAPACITY.set(_EVENT_QUEUE_CAPACITY)
        for partition in self._partitions:
            self._spawn_partition(partition)
        preparation_tasks = (
            (
                asyncio.create_task(
                    self._prepare_match_updates(),
                    name="market-worker-match-preparation",
                ),
            )
            if self._on_cycle_matches is not None
            else ()
        )
        self._tasks = (
            asyncio.create_task(self._consume_events(), name="market-worker-events"),
            asyncio.create_task(self._consume_metrics(), name="market-worker-metrics"),
            asyncio.create_task(self._watch_processes(), name="market-worker-watch"),
            *preparation_tasks,
        )
        for task in self._tasks:
            task.add_done_callback(self._task_done)

    async def stop(self) -> None:
        """Stop and join every child without touching central execution state."""
        self._stopping = True
        self._resolve_validations(WorkerValidationReason.SHUTDOWN)
        self._resolve_recovery_books(WorkerValidationReason.SHUTDOWN)
        for stop in self._stops.values():
            stop.set()
        for task in self._tasks:
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks = ()
        self._pending_match_preparations.clear()
        self._match_preparation_wakeup.clear()
        for name, process in tuple(self._processes.items()):
            await asyncio.to_thread(process.join, 5)
            if process.is_alive():
                process.terminate()
                await asyncio.to_thread(process.join, 2)
            MARKET_WORKER_UP.labels(name).set(0)
            MARKET_WORKER_HEARTBEAT_AGE.labels(name).set(0)
            MARKET_WORKER_PID.labels(name).set(0)
            generation = self._process_generations.get(name)
            if generation is not None:
                MARKET_WORKER_GENERATION.labels(name).set(0)
        self._processes.clear()
        self._process_generations.clear()
        self._worker_started_at.clear()
        self._last_summary_at.clear()
        self._stops.clear()
        for control in self._controls.values():
            control.cancel_join_thread()
            control.close()
        self._controls.clear()
        for output in (
            self._events_queue,
            self._metrics_queue,
        ):
            if output is not None:
                output.close()
        self._events_queue = None
        self._metrics_queue = None
        self._message_sequences.clear()
        MARKET_WORKER_IPC_QUEUE_DEPTH.set(0)

    def configure(self, min_net_edge: Decimal, cost_buffer: Decimal) -> None:
        """Apply signal thresholds to every running worker."""
        self._config = WorkerConfig(
            min_net_edge,
            cost_buffer,
        )
        for name, control in self._controls.items():
            try:
                control.put_nowait(self._config)
            except queue.Full:
                MARKET_WORKER_IPC_ENQUEUE_FAILURES.labels(
                    name, "control", "full"
                ).inc()
                self._fail(RuntimeError(f"Market worker {name} control queue is full"))

    def resume_execution_handoff(self) -> None:
        """Clear parent and child intent deduplication after trading enables."""
        self._message_sequences.clear()
        for name, control in self._controls.items():
            try:
                control.put_nowait(WorkerResumeHandoff())
            except queue.Full:
                MARKET_WORKER_IPC_ENQUEUE_FAILURES.labels(
                    name, "control", "full"
                ).inc()
                self._fail(RuntimeError(f"Market worker {name} control queue is full"))

    def capture_execution(
        self,
        execution_id: str,
        validation_ref: OpportunityValidationRef,
        contracts: Iterable[str],
        *,
        reason: str = "execution",
    ) -> None:
        """Request a pair-scoped diagnostic window without delaying execution.

        Notes
        -----
        - Saturation drops this diagnostic request; it cannot invalidate execution.
        """
        control = self._controls.get(validation_ref.partition)
        if control is None or self._process_generations.get(validation_ref.partition) != validation_ref.process_generation:
            return
        try:
            control.put_nowait(WorkerCaptureExecution(execution_id, validation_ref, tuple(contracts), reason))
        except (queue.Full, OSError, ValueError):
            MARKET_WORKER_IPC_ENQUEUE_FAILURES.labels(validation_ref.partition, "capture", "full").inc()

    async def recovery_books(
        self,
        execution_id: str,
        validation_ref: OpportunityValidationRef,
        contracts: tuple[ContractID, ContractID],
    ) -> tuple[OrderBook, OrderBook]:
        """Fetch current approved worker books for parent-owned exposure recovery.

        Parameters
        ----------
        execution_id
            Execution whose recovery is authorized by the parent.
        validation_ref
            Original admission identity fixing the owning worker generation.
        contracts
            Two approved contract IDs in the exact desired return order.

        Returns
        -------
        tuple[OrderBook, OrderBook]
            Current worker books with unchanged source and monotonic timestamps.
            The caller must apply recovery freshness, depth, and loss limits.

        Raises
        ------
        RuntimeError
            If the worker, request identity, books, or bounded IPC is unavailable.
        TimeoutError
            If the request or reply exceeds its per-request IPC deadline.

        Notes
        -----
        - Requests and outstanding futures are bounded; cancellation removes the
          waiter and late replies cannot satisfy a later request.
        """
        reason = self._recovery_worker_reason(validation_ref)
        if reason is not None:
            raise RuntimeError(f"Worker recovery books: {reason}")
        partition = self._partition(validation_ref.partition)
        if (not execution_id or len(contracts) != 2
                or not all(isinstance(contract, ContractID) for contract in contracts)
                or contracts[0] == contracts[1] or partition is None):
            raise RuntimeError("Worker recovery books: identity")
        approved = next((
            (pair.left, pair.right) if pair.left.id == contracts[0] else (pair.right, pair.left)
            for cycle in partition.cycles
            for pair in self._worker_matches.get(cycle, MarketMatchesUpdated(cycle, ())).pairs
            if {pair.left.id, pair.right.id} == set(contracts)
            and {pair.left.venue_id, pair.right.venue_id} == set(partition.venues)
        ), None)
        if approved is None:
            raise RuntimeError("Worker recovery books: identity")
        if len(self._pending_recovery_books) >= _CONTROL_QUEUE_CAPACITY:
            raise RuntimeError("Worker recovery books: queue_full")
        control = self._controls.get(validation_ref.partition)
        if control is None:
            raise RuntimeError("Worker recovery books: unavailable")
        request = WorkerRecoveryBooksRequest(uuid.uuid4().hex, execution_id, validation_ref, approved)
        future: asyncio.Future[WorkerRecoveryBooksResult] = asyncio.get_running_loop().create_future()
        self._pending_recovery_books[request.request_id] = request, future
        try:
            try:
                control.put_nowait(request)
            except queue.Full:
                MARKET_WORKER_IPC_ENQUEUE_FAILURES.labels(validation_ref.partition, "recovery", "full").inc()
                raise RuntimeError("Worker recovery books: queue_full") from None
            except (OSError, ValueError):
                raise RuntimeError("Worker recovery books: unavailable") from None
            try:
                reply = await asyncio.wait_for(
                    future,
                    max(0, request.sent_monotonic_at_ns + RECOVERY_BOOKS_TIMEOUT_NS - time.monotonic_ns()) / 1_000_000_000,
                )
            except TimeoutError:
                raise TimeoutError("Worker recovery books: timeout") from None
            reason = self._recovery_worker_reason(validation_ref) or reply.reason
            if time.monotonic_ns() - request.sent_monotonic_at_ns > RECOVERY_BOOKS_TIMEOUT_NS:
                reason = WorkerValidationReason.TIMEOUT
            if reason is WorkerValidationReason.TIMEOUT:
                raise TimeoutError("Worker recovery books: timeout")
            if reason is not WorkerValidationReason.ACCEPTED:
                raise RuntimeError(f"Worker recovery books: {reason}")
            if reply.books is None:
                raise RuntimeError("Worker recovery books: missing_book")
            return reply.books
        finally:
            self._pending_recovery_books.pop(request.request_id, None)
            if not future.done():
                future.cancel()

    def _recovery_worker_reason(
        self, reference: OpportunityValidationRef,
    ) -> WorkerValidationReason | None:
        """Recheck worker lifecycle both before IPC and immediately before use."""
        if self._stopping:
            return WorkerValidationReason.SHUTDOWN
        if self._process_generations.get(reference.partition) != reference.process_generation:
            return WorkerValidationReason.GENERATION
        if self.mode is not MarketWorkerMode.ACTIVE or self.error is not None or not self._worker_healthy(reference.partition):
            return WorkerValidationReason.UNAVAILABLE
        return None

    def _resolve_recovery_books(
        self, reason: WorkerValidationReason, partition: str | None = None,
    ) -> None:
        """Resolve recovery waiters immediately when their worker becomes unusable."""
        for request, future in tuple(self._pending_recovery_books.values()):
            reference = request.validation_ref
            if not future.done() and (partition is None or reference.partition == partition):
                future.set_result(WorkerRecoveryBooksResult(
                    request, reference.partition, reference.process_generation, reason,
                ))

    def _consume_recovery_books(self, reply: WorkerRecoveryBooksResult) -> None:
        """Check the echoed request, actual responder, book identities, and IPC age."""
        pending = self._pending_recovery_books.get(reply.request.request_id)
        if pending is None or pending[1].done():
            return
        request, future = pending
        reference = request.validation_ref
        now_ns = time.monotonic_ns()
        reason = self._recovery_worker_reason(reference) or reply.reason
        if reply.request != request or not isinstance(reply.reason, WorkerValidationReason):
            reason = WorkerValidationReason.IDENTITY
        elif reply.partition != reference.partition or reply.process_generation != reference.process_generation:
            reason = WorkerValidationReason.GENERATION
        elif (not request.sent_monotonic_at_ns <= reply.responded_monotonic_at_ns <= now_ns
                or now_ns - request.sent_monotonic_at_ns > RECOVERY_BOOKS_TIMEOUT_NS):
            reason = WorkerValidationReason.TIMEOUT
        elif reason is WorkerValidationReason.ACCEPTED:
            if reply.books is None or len(reply.books) != 2:
                reason = WorkerValidationReason.MISSING_BOOK
            elif any(
                not isinstance(book, OrderBook)
                or book.market_id != contract.market_id or book.outcome_id != contract.outcome_id
                for contract, book in zip(request.contracts, reply.books, strict=True)
            ):
                reason = WorkerValidationReason.IDENTITY
        future.set_result(replace(reply, request=request, reason=reason))

    async def validate_opportunity(
        self, request: WorkerOpportunityValidationRequest,
    ) -> WorkerOpportunityValidationResult:
        """Revalidate one prepared pair with bounded queueing and round-trip time.

        Returns
        -------
        WorkerOpportunityValidationResult
            Current books retaining their host monotonic timestamps, or a bounded
            rejection. Cancellation removes its waiter and propagates normally.

        Notes
        -----
        - The parent's monotonic deadline is shared with its outer timeout. An
          expired outer wait counts as a timeout; earlier cancellation does not.
        """
        started_at_ns = time.monotonic_ns()
        started = started_at_ns / 1_000_000_000
        deadline_at_ns = started_at_ns + int(self._validation_timeout_seconds * 1_000_000_000)
        if request.validation_deadline_at_ns is not None:
            deadline_at_ns = min(deadline_at_ns, request.validation_deadline_at_ns)
        partition = request.validation_ref.partition
        outcome = WorkerValidationReason.ERROR

        def rejected(reason: WorkerValidationReason) -> WorkerOpportunityValidationResult:
            return WorkerOpportunityValidationResult(request, reason, time.time_ns())

        future: asyncio.Future[WorkerOpportunityValidationResult] | None = None
        try:
            if self._stopping:
                reply = rejected(WorkerValidationReason.SHUTDOWN)
            elif self._process_generations.get(partition) != request.validation_ref.process_generation:
                reply = rejected(WorkerValidationReason.GENERATION)
            elif self.mode is not MarketWorkerMode.ACTIVE or self.error is not None or not self._worker_healthy(partition):
                reply = rejected(WorkerValidationReason.UNAVAILABLE)
            elif time.monotonic_ns() >= deadline_at_ns:
                reply = rejected(WorkerValidationReason.TIMEOUT)
            elif request.request_id in self._pending_validations:
                reply = rejected(WorkerValidationReason.IDENTITY)
            elif len(self._pending_validations) >= _CONTROL_QUEUE_CAPACITY:
                reply = rejected(WorkerValidationReason.QUEUE_FULL)
            else:
                control = self._controls.get(partition)
                if control is None:
                    reply = rejected(WorkerValidationReason.UNAVAILABLE)
                else:
                    future = asyncio.get_running_loop().create_future()
                    self._pending_validations[request.request_id] = request, future
                    self._observe_validation_queue(partition)
                    try:
                        control.put_nowait(request)
                    except queue.Full:
                        MARKET_WORKER_IPC_ENQUEUE_FAILURES.labels(partition, "validation", "full").inc()
                        reply = rejected(WorkerValidationReason.QUEUE_FULL)
                    except (OSError, ValueError):
                        reply = rejected(WorkerValidationReason.UNAVAILABLE)
                    else:
                        try:
                            reply = await asyncio.wait_for(
                                future, max(0, deadline_at_ns - time.monotonic_ns()) / 1_000_000_000,
                            )
                        except TimeoutError:
                            reply = rejected(WorkerValidationReason.TIMEOUT)
            if reply.reason in (WorkerValidationReason.ACCEPTED, WorkerValidationReason.REPRICE):
                if self._stopping:
                    reply = rejected(WorkerValidationReason.SHUTDOWN)
                elif self._process_generations.get(partition) != request.validation_ref.process_generation:
                    reply = rejected(WorkerValidationReason.GENERATION)
                elif self.error is not None or not self._worker_healthy(partition):
                    reply = rejected(WorkerValidationReason.UNAVAILABLE)
                elif time.monotonic_ns() >= deadline_at_ns:
                    reply = rejected(WorkerValidationReason.TIMEOUT)
            outcome = reply.reason
            return reply
        except asyncio.CancelledError:
            outcome = (
                WorkerValidationReason.TIMEOUT
                if time.monotonic_ns() >= deadline_at_ns else "cancelled"
            )
            raise
        finally:
            if future is not None:
                self._pending_validations.pop(request.request_id, None)
                if not future.done():
                    future.cancel()
                self._observe_validation_queue(partition)
            if outcome is WorkerValidationReason.TIMEOUT:
                MARKET_WORKER_VALIDATION_TIMEOUTS.labels(partition).inc()
            MARKET_WORKER_VALIDATION_TOTAL.labels(partition, str(outcome)).inc()
            MARKET_WORKER_VALIDATION_LATENCY.labels(partition).observe(time.monotonic() - started)

    def _observe_validation_queue(self, partition: str) -> None:
        """Expose the bounded number of outstanding requests for one partition."""
        MARKET_WORKER_VALIDATION_QUEUE_DEPTH.labels(partition).set(sum(
            request.validation_ref.partition == partition
            for request, _ in self._pending_validations.values()
        ))

    def _resolve_validations(
        self, reason: WorkerValidationReason, partition: str | None = None,
    ) -> None:
        """Fail pending requests immediately on worker replacement or shutdown."""
        for request, future in tuple(self._pending_validations.values()):
            if not future.done() and (partition is None or request.validation_ref.partition == partition):
                future.set_result(WorkerOpportunityValidationResult(request, reason, time.time_ns()))

    def _consume_validation(self, reply: WorkerOpportunityValidationResult) -> None:
        """Resolve the matching request without reconstructing arrival from wall time."""
        pending = self._pending_validations.get(reply.request.request_id)
        if pending is None or pending[1].done():
            return
        request, future = pending
        reason = reply.reason
        now_wall = time.time_ns()
        now_local = time.monotonic_ns()
        if reply.request != request or not isinstance(reason, WorkerValidationReason):
            reason = WorkerValidationReason.IDENTITY
        elif self._process_generations.get(request.validation_ref.partition) != request.validation_ref.process_generation:
            reason = WorkerValidationReason.GENERATION
        elif self._stopping:
            reason = WorkerValidationReason.SHUTDOWN
        elif not self._worker_healthy(request.validation_ref.partition):
            reason = WorkerValidationReason.UNAVAILABLE
        elif (
            not request.sent_monotonic_at_ns <= reply.responded_monotonic_at_ns <= now_local
            or now_local - request.sent_monotonic_at_ns > self._validation_timeout_seconds * 1_000_000_000
        ):
            reason = WorkerValidationReason.TIMEOUT
        if reason in (WorkerValidationReason.ACCEPTED, WorkerValidationReason.REPRICE):
            translated: list[OrderBook] = []
            for contract, book, generation in (
                (request.pair.left, reply.left_order_book, reply.left_book_generation),
                (request.pair.right, reply.right_order_book, reply.right_book_generation),
            ):
                if book is None:
                    reason = WorkerValidationReason.MISSING_BOOK
                    break
                if (contract.market_id != book.market_id or contract.outcome_id != book.outcome_id
                        or order_book_generation(book) != generation):
                    reason = WorkerValidationReason.IDENTITY
                    break
                age = now_local - book.received_at_ns if book.received_at_ns is not None else -1
                source_kind = book.source_timestamp_kind or "unknown"
                if age >= 0:
                    MARKET_WORKER_BOOK_AGE.labels(
                        request.validation_ref.partition, str(contract.venue_id),
                        "validation", "local", source_kind,
                    ).observe(age / 1_000_000_000)
                if book.source_at_ns is not None:
                    MARKET_WORKER_BOOK_AGE.labels(
                        request.validation_ref.partition, str(contract.venue_id),
                        "validation", "source", source_kind,
                    ).observe(max(0, now_wall - book.source_at_ns) / 1_000_000_000)
                if not 0 <= age <= min(request.max_book_age_ns, now_local):
                    reason = WorkerValidationReason.STALE_LOCAL
                    break
                if request.enforce_source_age and book.source_timestamp_kind == "venue_update" and (
                    book.source_at_ns is None or now_wall - book.source_at_ns > SOURCE_BOOK_MAX_AGE_MS * 1_000_000
                ):
                    reason = WorkerValidationReason.STALE_SOURCE
                    break
                translated.append(replace(book, processed_at_ns=None))
            if len(translated) == 2:
                reply = replace(reply, left_order_book=translated[0], right_order_book=translated[1])
        future.set_result(replace(reply, request=request, reason=reason))

    async def wait_until_failed(self) -> None:
        """Wait until a child exits unexpectedly or IPC processing fails."""
        await self._failed.wait()

    def observe_parent_book_age(
        self,
        execution_id: str,
        venue_id: VenueID,
        contract_id: ContractID,
        stage: str,
        monotonic_at_ns: int,
        wall_at_ns: int,
    ) -> None:
        """Record one worker-routed parent book age at guard or submission.

        Parameters
        ----------
        execution_id
            Authoritative execution owning the worker provenance reference.
        venue_id
            Venue whose leg is being measured.
        contract_id
            Contract used to resolve the current authoritative book.
        stage
            Bounded pipeline stage such as ``guard`` or ``submission``.
        monotonic_at_ns
            Parent monotonic observation time in nanoseconds.
        wall_at_ns
            Parent wall-clock observation time in nanoseconds.
        """
        engine = self._central_engine
        if engine is None:
            return
        reference = engine.state.execution_worker_refs.get(execution_id)
        book = engine.state.books.get(contract_id)
        if reference is None or book is None:
            return
        source_kind = book.source_timestamp_kind or "unknown"
        if book.received_at_ns is not None:
            MARKET_WORKER_BOOK_AGE.labels(
                reference.partition,
                str(venue_id),
                stage,
                "local",
                source_kind,
            ).observe(
                max(0, monotonic_at_ns - book.received_at_ns) / 1_000_000_000
            )
        if book.source_at_ns is not None:
            MARKET_WORKER_BOOK_AGE.labels(
                reference.partition,
                str(venue_id),
                stage,
                "source",
                source_kind,
            ).observe(max(0, wall_at_ns - book.source_at_ns) / 1_000_000_000)

    def monitored_market(self, monitor_key: str) -> MarketCycle | None:
        """Resolve a child-observed cycle from its stable monitor key."""
        from prediction_markets.application.markets.models import monitored_market_key

        return next(
            (
                cycle
                for cycle in self.monitored_cycles
                if monitored_market_key(cycle) == monitor_key
            ),
            None,
        )

    def status(self) -> tuple[dict[str, object], ...]:
        """Return process, partition, route, and latest health state."""
        return tuple(
            {
                "partition": partition.name,
                "cycles": tuple(
                    (cycle.underlying.symbol, cycle.interval_seconds)
                    for cycle in partition.cycles
                ),
                "venues": tuple(str(venue) for venue in partition.venues),
                "cpu_index": partition.cpu_index,
                "generation": self._process_generations.get(partition.name),
                "pid": (
                    self._processes[partition.name].pid
                    if partition.name in self._processes
                    else None
                ),
                "alive": (
                    self._worker_healthy(partition.name)
                ),
                "heartbeat_age_seconds": self._heartbeat_age(partition.name),
                "summary": self.latest_summaries.get(partition.name),
                "capture": (
                    self.latest_summaries[partition.name].capture
                    if partition.name in self.latest_summaries else None
                ),
            }
            for partition in self._partitions
        )

    def _spawn_partition(
        self,
        partition: MarketWorkerPartition,
        *,
        restarting: bool = False,
    ) -> None:
        assert self._events_queue is not None
        assert self._metrics_queue is not None
        self._resolve_validations(WorkerValidationReason.GENERATION, partition.name)
        self._resolve_recovery_books(WorkerValidationReason.GENERATION, partition.name)
        stop = self._context.Event()
        control = self._context.Queue(maxsize=_CONTROL_QUEUE_CAPACITY)
        generation_number = self._generation_numbers.get(partition.name, 0) + 1
        self._generation_numbers[partition.name] = generation_number
        process_generation = f"{generation_number}-{uuid.uuid4().hex}"
        process = self._context.Process(
            target=self._worker_target,
            args=(
                partition,
                process_generation,
                self._config,
                control,
                self._events_queue,
                self._metrics_queue,
                stop,
            ),
            name=f"market-worker:{partition.name}",
        )
        process.start()
        self._processes[partition.name] = process
        self._process_generations[partition.name] = process_generation
        for cycle in partition.cycles:
            self._worker_matches.pop(cycle, None)
        self._stops[partition.name] = stop
        self._worker_started_at[partition.name] = time.monotonic()
        self._last_summary_at.pop(partition.name, None)
        self.latest_summaries.pop(partition.name, None)
        update_predict_fill_capture_metrics(partition.name, None)
        previous_control = self._controls.get(partition.name)
        if previous_control is not None:
            previous_control.cancel_join_thread()
            previous_control.close()
        self._controls[partition.name] = control
        MARKET_WORKER_UP.labels(partition.name).set(1)
        MARKET_WORKER_HEARTBEAT_AGE.labels(partition.name).set(0)
        MARKET_WORKER_PID.labels(partition.name).set(process.pid or 0)
        MARKET_WORKER_GENERATION.labels(partition.name).set(generation_number)
        MARKET_WORKER_STARTS.labels(partition.name).inc()
        if restarting:
            MARKET_WORKER_RESTARTS.labels(partition.name).inc()

    async def _consume_events(self) -> None:
        assert self._events_queue is not None
        while True:
            messages, queued_depth = await asyncio.to_thread(_queue_get_batch, self._events_queue)
            depth = _queue_size(self._events_queue)
            if depth is not None:
                self._ipc_high_watermark = max(
                    self._ipc_high_watermark, depth, queued_depth or 0,
                )
                MARKET_WORKER_IPC_QUEUE_DEPTH.set(depth)
                MARKET_WORKER_IPC_QUEUE_HIGH_WATERMARK.set(self._ipc_high_watermark)
            await self._consume_event_batch(messages)

    async def _consume_event_batch(self, messages: tuple[object, ...]) -> None:
        """Keep the latest pending pair while preserving sequence and metadata barriers.

        Notes
        -----
        - Only already dequeued opportunities are replaceable. Discovery, tick,
          failure, duplicate and reordered messages retain their original position.
        - Surviving messages pass the unchanged admission and freshness checks.
        """
        retained: list[object | None] = list(messages)
        latest: dict[tuple[object, ...], int] = {}
        sequences = dict(self._message_sequences)
        for index, message in enumerate(messages):
            if not isinstance(message, (WorkerEvent, WorkerOpportunityIntent, WorkerTickSizeChange)):
                latest.clear()
                continue
            sequence_key = message.partition, message.process_generation
            if (
                self._partition(message.partition) is None
                or self._process_generations.get(message.partition) != message.process_generation
                or message.sequence <= sequences.get(sequence_key, 0)
            ):
                latest.clear()
                continue
            sequences[sequence_key] = message.sequence
            if not isinstance(message, WorkerOpportunityIntent):
                latest.clear()
                continue
            key = (
                message.partition, message.process_generation, message.detected.cycle,
                message.detected.pair.key, message.detected.opportunity.side,
            )
            previous = latest.get(key)
            if previous is not None:
                retained[previous] = None
                self._reject_intent(message.partition, "coalesced")
            latest[key] = index
        for message in retained:
            if message is not None:
                await self._consume_message(message)

    async def _consume_message(self, message: object) -> None:
        """Apply one worker message in process-generation and sequence order."""
        if isinstance(message, WorkerFailure):
            if self._process_generations.get(message.partition) == message.process_generation:
                self._fail(RuntimeError(f"{message.partition}: {message.error}"))
            return
        if not isinstance(message, (WorkerEvent, WorkerOpportunityIntent, WorkerTickSizeChange)):
            return
        partition = self._partition(message.partition)
        if partition is None:
            self._reject_intent(message.partition, "partition")
            return
        if self._process_generations.get(message.partition) != message.process_generation:
            self._reject_intent(message.partition, "generation")
            return
        sequence_key = message.partition, message.process_generation
        if message.sequence <= self._message_sequences.get(sequence_key, 0):
            self._reject_intent(message.partition, "order")
            return
        self._message_sequences[sequence_key] = message.sequence
        if isinstance(message, WorkerTickSizeChange):
            if self.mode is not MarketWorkerMode.ACTIVE:
                return
            if message.venue_id not in partition.venues:
                self._reject_intent(message.partition, "route")
                return
            known = any(
                contract.id == message.contract_id and contract.venue_id == message.venue_id
                for cycle in partition.cycles
                for pair in self._worker_matches.get(cycle, MarketMatchesUpdated(cycle, ())).pairs
                for contract in (pair.left, pair.right)
            )
            if not known:
                self._reject_intent(message.partition, "identity")
                return
            if self._on_tick_size_change is None:
                self._fail(RuntimeError("Worker tick-size callback is unavailable"))
                return
            self._on_tick_size_change(message.venue_id, message.contract_id, message.tick_size)
            return
        if isinstance(message, WorkerEvent):
            if message.event.cycle not in partition.cycles:
                self._reject_intent(message.partition, "cycle")
                return
            if any(frozenset((pair.left.venue_id, pair.right.venue_id)) != frozenset(partition.venues)
                   for pair in message.event.pairs):
                self._reject_intent(message.partition, "route")
                return
            self._worker_matches[message.event.cycle] = message.event
            if self.mode is MarketWorkerMode.SHADOW:
                return
            await self._pipeline.sink.publish(message.event)
            if self._on_cycle_matches is not None:
                self._pending_match_preparations[message.event.cycle] = message.event
                self._match_preparation_wakeup.set()
            return
        await self._consume_intent(message, partition)

    async def _consume_intent(
        self,
        intent: WorkerOpportunityIntent,
        partition: MarketWorkerPartition,
    ) -> None:
        """Reject unsafe intents and publish one current atomic pair snapshot."""
        detected = intent.detected
        pair = detected.pair
        if detected.cycle not in partition.cycles:
            self._reject_intent(intent.partition, "cycle")
            return
        if frozenset((pair.left.venue_id, pair.right.venue_id)) != frozenset(
            partition.venues
        ):
            self._reject_intent(intent.partition, "route")
            return
        received_wall_at_ns = time.time_ns()
        received_at_ns = time.monotonic_ns()
        intent_age_ns = received_at_ns - intent.sent_monotonic_at_ns
        MARKET_WORKER_INTENT_LATENCY.labels(intent.partition).observe(
            max(0, intent_age_ns) / 1_000_000_000
        )
        if intent_age_ns < 0 or intent_age_ns > _INTENT_MAX_AGE_NS:
            self._reject_intent(intent.partition, "stale_age")
            return
        expected_generations = (
            intent.left_book_generation,
            intent.right_book_generation,
        )
        translated_books: list[OrderBook] = []
        for index, (contract, book) in enumerate(
            (
                (pair.left, intent.left_order_book),
                (pair.right, intent.right_order_book),
            )
        ):
            if contract.market_id != book.market_id or contract.outcome_id != book.outcome_id:
                self._reject_intent(intent.partition, "identity")
                return
            if order_book_generation(book) != expected_generations[index]:
                self._reject_intent(intent.partition, "identity")
                return
            if book.arrival_wall_at_ns is None or book.received_at_ns is None:
                self._reject_intent(intent.partition, "missing_arrival")
                return
            detection_local_age_ns = intent.sent_monotonic_at_ns - book.received_at_ns
            if detection_local_age_ns >= 0:
                MARKET_WORKER_BOOK_AGE.labels(
                    intent.partition,
                    str(contract.venue_id),
                    "worker_detection",
                    "local",
                    book.source_timestamp_kind or "unknown",
                ).observe(detection_local_age_ns / 1_000_000_000)
            if book.source_at_ns is not None:
                detection_source_age_ns = intent.sent_wall_at_ns - book.source_at_ns
                if detection_source_age_ns >= 0:
                    MARKET_WORKER_BOOK_AGE.labels(
                        intent.partition,
                        str(contract.venue_id),
                        "worker_detection",
                        "source",
                        book.source_timestamp_kind or "unknown",
                    ).observe(detection_source_age_ns / 1_000_000_000)
            local_age_ns = received_at_ns - book.received_at_ns
            if local_age_ns < 0:
                self._reject_intent(intent.partition, "invalid_arrival")
                return
            if (
                book.source_timestamp_kind == "venue_update"
                and book.source_at_ns is not None
            ):
                source_age_ns = max(0, received_wall_at_ns - book.source_at_ns)
                MARKET_WORKER_BOOK_AGE.labels(
                    intent.partition,
                    str(contract.venue_id),
                    "parent_receipt",
                    "source",
                    book.source_timestamp_kind or "unknown",
                ).observe(source_age_ns / 1_000_000_000)
            MARKET_WORKER_BOOK_AGE.labels(
                intent.partition,
                str(contract.venue_id),
                "parent_receipt",
                "local",
                book.source_timestamp_kind or "unknown",
            ).observe(local_age_ns / 1_000_000_000)
            translated_books.append(
                replace(book, processed_at_ns=None)
            )
        reference = OpportunityValidationRef(
            intent.intent_id,
            intent.partition,
            intent.process_generation,
            intent.sequence,
            intent.left_book_generation,
            intent.right_book_generation,
        )
        if (
            detected.opportunity.left_contract_id != pair.left.id
            or detected.opportunity.right_contract_id != pair.right.id
        ):
            self._reject_intent(intent.partition, "identity")
            return
        if self.mode is MarketWorkerMode.SHADOW:
            self._observe_shadow_detection(intent)
            return
        await self._pipeline.sink.publish(
            OrderBookPairUpdated(
                replace(detected, validation_ref=reference),
                translated_books[0],
                translated_books[1],
            )
        )

    def _observe_shadow_detection(self, intent: WorkerOpportunityIntent) -> None:
        """Compare a worker detection only when parent books share source state."""
        engine = self._central_engine
        detected = intent.detected
        pair_key = detected.pair.key
        if engine is None:
            outcome = "central_unavailable"
        else:
            pair = next(
                (
                    candidate
                    for candidate in engine.state.matches.get(detected.cycle, ())
                    if candidate.key == pair_key
                ),
                None,
            )
            books = (
                engine.state.books.get(pair.left.id) if pair is not None else None,
                engine.state.books.get(pair.right.id) if pair is not None else None,
            )
            if pair is None or any(book is None for book in books):
                outcome = "central_missing"
            elif (
                source_book_generation(books[0])
                != source_book_generation(intent.left_order_book)
                or source_book_generation(books[1])
                != source_book_generation(intent.right_order_book)
            ):
                outcome = "generation_mismatch"
            else:
                assert books[0] is not None and books[1] is not None
                try:
                    central_opportunity = engine.detect_shadow_opportunity(
                        pair,
                        books[0],
                        books[1],
                        detected.opportunity.side,
                    )
                except (KeyError, RuntimeError, ValueError):
                    central_opportunity = None
                outcome = "agree" if central_opportunity is not None else "disagree"
        MARKET_WORKER_SHADOW_DETECTIONS.labels(intent.partition, outcome).inc()

    async def _consume_metrics(self) -> None:
        assert self._metrics_queue is not None
        while True:
            summary = await asyncio.to_thread(_queue_get, self._metrics_queue)
            if isinstance(summary, WorkerRecoveryBooksResult):
                self._consume_recovery_books(summary)
                continue
            if isinstance(summary, WorkerOpportunityValidationResult):
                self._consume_validation(summary)
                continue
            if isinstance(summary, WorkerMetricTransition):
                if (
                    self._process_generations.get(summary.partition)
                    == summary.process_generation
                ):
                    transition = summary.transition
                    MARKET_WORKER_TRANSITIONS.labels(
                        summary.partition,
                        transition.venue,
                        transition.kind,
                        transition.state,
                    ).inc()
                continue
            if not isinstance(summary, WorkerRuntimeSummary):
                continue
            if self._process_generations.get(summary.partition) != summary.process_generation:
                continue
            self._last_summary_at[summary.partition] = time.monotonic()
            MARKET_WORKER_HEARTBEAT_AGE.labels(summary.partition).set(0)
            self.latest_summaries[summary.partition] = summary
            update_predict_fill_capture_metrics(summary.partition, summary.capture)
            if summary.clock is not None:
                update_clock_metrics(summary.partition, summary.clock)
            if summary.event_queue_depth is not None:
                self._ipc_high_watermark = max(
                    self._ipc_high_watermark,
                    summary.event_queue_depth,
                )
                MARKET_WORKER_IPC_QUEUE_DEPTH.set(summary.event_queue_depth)
                MARKET_WORKER_IPC_QUEUE_HIGH_WATERMARK.set(
                    self._ipc_high_watermark
                )
            for lag in summary.event_loop_lags_seconds:
                MARKET_WORKER_EVENT_LOOP_LAG.labels(summary.partition).observe(lag)
            MARKET_WORKER_CPU_SECONDS.labels(summary.partition).set(summary.cpu_seconds)
            if summary.resident_memory_bytes is not None:
                MARKET_WORKER_MEMORY_BYTES.labels(summary.partition).set(
                    summary.resident_memory_bytes
                )
            for timing in summary.timings:
                for stage, value in (
                    ("source_to_transport_raw_min", timing.source_to_transport_raw_min_seconds),
                    ("source_to_transport_raw_max", timing.source_to_transport_raw_max_seconds),
                ):
                    if value is not None:
                        MARKET_WORKER_TIMING.labels(summary.partition, timing.venue_id, stage).set(value)
                if timing.future_source_samples:
                    MARKET_WORKER_FUTURE_SOURCE.labels(summary.partition, timing.venue_id).inc(timing.future_source_samples)
                if timing.source_to_transport_max_seconds is not None:
                    MARKET_WORKER_TIMING.labels(
                        summary.partition,
                        timing.venue_id,
                        "source_to_transport_max",
                    ).set(timing.source_to_transport_max_seconds)
                if timing.arrival_to_processing_max_seconds is not None:
                    MARKET_WORKER_TIMING.labels(
                        summary.partition,
                        timing.venue_id,
                        "arrival_to_processing_max",
                    ).set(timing.arrival_to_processing_max_seconds)
                if timing.arrival_to_sink_max_seconds is not None:
                    MARKET_WORKER_TIMING.labels(
                        summary.partition,
                        timing.venue_id,
                        "transport_to_sink_max",
                    ).set(timing.arrival_to_sink_max_seconds)
                if timing.sink_publish_max_seconds is not None:
                    MARKET_WORKER_TIMING.labels(
                        summary.partition,
                        timing.venue_id,
                        "sink_publish_max",
                    ).set(timing.sink_publish_max_seconds)
            for pressure in summary.queues:
                MARKET_WORKER_WS_QUEUE_DEPTH.labels(
                    summary.partition,
                    pressure.venue_id,
                ).set(pressure.depth)
                MARKET_WORKER_WS_QUEUE_HIGH_WATERMARK.labels(
                    summary.partition,
                    pressure.venue_id,
                ).set(pressure.high_watermark)
                MARKET_WORKER_WS_PAUSED.labels(
                    summary.partition,
                    pressure.venue_id,
                ).set(pressure.paused_streams)
                MARKET_WORKER_WS_QUEUE_WAIT_MAX.labels(
                    summary.partition,
                    pressure.venue_id,
                ).set(
                    pressure.queue_wait_max_seconds
                    if pressure.queue_wait_max_seconds is not None
                    else float("nan")
                )
            for queue_name, reason, count in summary.enqueue_drops:
                MARKET_WORKER_IPC_ENQUEUE_FAILURES.labels(
                    summary.partition,
                    queue_name,
                    reason,
                ).inc(count)

    async def _prepare_match_updates(self) -> None:
        """Warm parent adapters without delaying the worker event consumer.

        Notes
        -----
        - At most one pending update is retained per cycle. A newer discovery
          snapshot replaces an older one while slow external warm-up is active.
        - Failures trip the authoritative safety signal but do not stop IPC
          draining, which lets child failures and queue pressure stay visible.
        """
        assert self._on_cycle_matches is not None
        while True:
            await self._match_preparation_wakeup.wait()
            self._match_preparation_wakeup.clear()
            while self._pending_match_preparations:
                cycle = next(iter(self._pending_match_preparations))
                event = self._pending_match_preparations.pop(cycle)
                try:
                    await self._on_cycle_matches(event)
                except asyncio.CancelledError:
                    raise
                except Exception as error:
                    self._fail(
                        RuntimeError(
                            f"Worker match preparation failed for {cycle}: {error}"
                        )
                    )

    async def _watch_processes(self) -> None:
        while True:
            await asyncio.sleep(0.25)
            for partition in self._partitions:
                process = self._processes.get(partition.name)
                if process is None:
                    continue
                heartbeat_age = self._heartbeat_age(partition.name)
                if heartbeat_age is not None:
                    MARKET_WORKER_HEARTBEAT_AGE.labels(partition.name).set(
                        heartbeat_age
                    )
                heartbeat_timeout = (
                    process.exitcode is None
                    and heartbeat_age is not None
                    and heartbeat_age > self._heartbeat_limit(partition.name)
                )
                if process.exitcode is None and not heartbeat_timeout:
                    continue
                generation = self._process_generations.get(partition.name, "")
                MARKET_WORKER_UP.labels(partition.name).set(0)
                MARKET_WORKER_PID.labels(partition.name).set(0)
                MARKET_WORKER_GENERATION.labels(partition.name).set(0)
                exit_reason = (
                    "heartbeat_timeout"
                    if heartbeat_timeout
                    else str(process.exitcode)
                )
                MARKET_WORKER_EXITS.labels(
                    partition.name,
                    exit_reason,
                ).inc()
                if heartbeat_timeout:
                    error = RuntimeError(
                        f"Market worker {partition.name} missed its heartbeat for "
                        f"{heartbeat_age:.3f}s"
                    )
                    self._stops[partition.name].set()
                    if process.is_alive():
                        process.terminate()
                else:
                    error = RuntimeError(
                        f"Market worker {partition.name} exited with {process.exitcode}"
                    )
                self._fail(error)
                await asyncio.to_thread(process.join)
                if not self._stopping:
                    self._spawn_partition(partition, restarting=True)

    def _heartbeat_age(self, partition: str) -> float | None:
        """Return seconds since the latest summary or current process start."""
        baseline = self._last_summary_at.get(
            partition,
            self._worker_started_at.get(partition),
        )
        if baseline is None:
            return None
        return max(0.0, time.monotonic() - baseline)

    def _worker_healthy(self, partition: str) -> bool:
        """Return whether one process is alive and inside its heartbeat limit."""
        process = self._processes.get(partition)
        heartbeat_age = self._heartbeat_age(partition)
        return (
            process is not None
            and process.is_alive()
            and heartbeat_age is not None
            and heartbeat_age <= self._heartbeat_limit(partition)
        )

    def _heartbeat_limit(self, partition: str) -> float:
        """Allow spawn initialization more time than an established worker."""
        if partition not in self._last_summary_at:
            return self._startup_timeout_seconds
        return self._heartbeat_timeout_seconds

    def _partition(self, name: str) -> MarketWorkerPartition | None:
        return next((partition for partition in self._partitions if partition.name == name), None)

    def _reject_intent(self, partition: str, reason: str) -> None:
        MARKET_WORKER_INTENTS_REJECTED.labels(partition or "unknown", reason).inc()

    def _fail(self, error: BaseException) -> None:
        self._resolve_validations(WorkerValidationReason.UNAVAILABLE)
        self._resolve_recovery_books(WorkerValidationReason.UNAVAILABLE)
        if self.error is None:
            self.error = error
            self._failed.set()

    def _task_done(self, task: asyncio.Task[None]) -> None:
        if self._stopping or task.cancelled():
            return
        error = task.exception()
        self._fail(
            error
            if error is not None
            else RuntimeError(f"Parent worker task {task.get_name()} stopped")
        )


def _queue_get(source: Any) -> Any | None:
    try:
        return source.get(timeout=0.25)
    except queue.Empty:
        return None


def _queue_get_batch(source: Any) -> tuple[tuple[object, ...], int | None]:
    """Read up to 64 events and sample backlog before nonblocking draining.

    Returns
    -------
    tuple
        FIFO messages and remaining depth after the first read, or ``None``
        for depth when the platform cannot report queue size.

    Notes
    -----
    - Only the first read waits, using the existing bounded shutdown timeout.
    - Queue depth is sampled before draining so a burst remains observable.
    """
    first = _queue_get(source)
    depth = _queue_size(source)
    if first is None:
        return (), depth
    messages = [first]
    while len(messages) < _EVENT_BATCH_CAPACITY:
        try:
            messages.append(source.get_nowait())
        except queue.Empty:
            break
    return tuple(messages), depth


def _queue_size(source: Any) -> int | None:
    try:
        return max(0, int(source.qsize()))
    except (AttributeError, NotImplementedError, OSError):
        return None


def _resident_memory_bytes() -> int | None:
    """Return current Linux resident memory using only the standard library."""
    try:
        resident_pages = int(Path("/proc/self/statm").read_text().split()[1])
        return resident_pages * int(os.sysconf("SC_PAGE_SIZE"))
    except (AttributeError, IndexError, OSError, ValueError):
        return None


def _market_worker_entry(
    partition: MarketWorkerPartition,
    process_generation: str,
    config: WorkerConfig,
    control: Any,
    events: Any,
    metrics: Any,
    stop: Any,
) -> None:
    """Start one spawn-safe worker process and report terminal failure."""
    load_dotenv()
    if partition.cpu_index is not None and hasattr(os, "sched_setaffinity"):
        os.sched_setaffinity(0, {partition.cpu_index})
    try:
        asyncio.run(
            _run_market_worker(
                partition,
                process_generation,
                config,
                control,
                events,
                metrics,
                stop,
            )
        )
    except BaseException as error:
        try:
            events.put_nowait(
                WorkerFailure(partition.name, process_generation, repr(error))
            )
        except queue.Full:
            pass
        raise


async def _run_market_worker(
    partition: MarketWorkerPartition,
    process_generation: str,
    config: WorkerConfig,
    control: Any,
    events: Any,
    metrics: Any,
    stop: Any,
) -> None:
    """Compose and run one worker-owned discovery, feed, and detection pipeline."""
    telemetry = _WorkerTelemetry(partition.name, process_generation, metrics)
    async with AsyncExitStack() as stack:
        start_study(f"worker:{partition.name}:{process_generation}")
        stack.push_async_callback(asyncio.to_thread, stop_study)
        matcher, streams, fees = await _worker_resources(
            partition,
            stack,
            telemetry,
        )
        state = TradingState()
        engine = TradingEngine(EventDispatcher(state), fees)
        engine.configure(
            LiveArbitrageConfig(
                min_net_edge=config.min_net_edge,
                cost_buffer=config.cost_buffer,
            ).engine_config()
        )
        journal = _WorkerJournal(
            partition.name,
            events,
            state,
            process_generation,
            telemetry,
        )
        pipeline = TradingPipeline(
            journal,
            engine,
            on_processing=telemetry.observe_processing,
        )
        feed = _MarketFeedCoordinator(
            matcher,
            streams,
            fees,
            pipeline,
            state,
            refresh_seconds=float(os.getenv("MARKET_REFRESH_SECONDS", "15")),
            cycles=partition.cycles,
            on_book_published=telemetry.observe_sink,
            on_tick_size_change=journal.publish_tick_size,
        )
        await pipeline.start()
        await feed.start()
        control_task = asyncio.create_task(
            _apply_worker_controls(
                control,
                engine,
                journal,
            ),
            name=f"worker-control:{partition.name}",
        )
        metrics_task = asyncio.create_task(
            _publish_worker_summaries(
                metrics,
                events,
                telemetry,
            ),
            name=f"worker-metrics:{partition.name}",
        )
        stop_task = asyncio.create_task(
            _wait_for_process_stop(stop),
            name=f"worker-stop:{partition.name}",
        )
        feed_failed = asyncio.create_task(feed.wait_until_failed())
        pipeline_failed = asyncio.create_task(pipeline.wait_until_failed())
        done, pending = await asyncio.wait(
            (stop_task, feed_failed, pipeline_failed, control_task, metrics_task),
            return_when=asyncio.FIRST_COMPLETED,
        )
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)
        await feed.stop()
        await pipeline.stop()
        if feed_failed in done and feed.error is not None:
            raise RuntimeError(str(feed.error))
        if pipeline_failed in done and pipeline.error is not None:
            raise RuntimeError(str(pipeline.error))
        for task in (control_task, metrics_task):
            if task in done and not task.cancelled() and task.exception() is not None:
                raise RuntimeError(str(task.exception())) from task.exception()


async def _wait_for_process_stop(stop: Any) -> None:
    """Poll a process-shared stop event without occupying an executor thread.

    Parameters
    ----------
    stop
        Multiprocessing event set by the authoritative parent.

    Notes
    -----
    - Async polling is cancellation-safe. A cancelled worker task cannot leave
      ``asyncio.run`` waiting forever for a blocked default-executor thread.
    """
    while not stop.is_set():
        await asyncio.sleep(0.1)


async def _publish_worker_summaries(
    metrics: Any,
    events: Any,
    telemetry: _WorkerTelemetry,
) -> None:
    """Send one aggregate summary per second without blocking market data."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + _SUMMARY_INTERVAL_SECONDS
    while True:
        await asyncio.sleep(max(0.0, deadline - loop.time()))
        now = loop.time()
        lags, deadline = scheduled_lags(now, deadline, _SUMMARY_INTERVAL_SECONDS)
        summary = telemetry.snapshot(lags, events)
        try:
            metrics.put_nowait(summary)
        except queue.Full:
            telemetry.record_drop("metrics")


async def _apply_worker_controls(
    control: Any,
    engine: TradingEngine,
    journal: _WorkerJournal,
) -> None:
    """Apply controls and reply to book requests without waiting on the event sink."""
    while True:
        message = await asyncio.to_thread(_queue_get, control)
        if isinstance(message, WorkerConfig):
            engine.configure(
                LiveArbitrageConfig(
                    min_net_edge=message.min_net_edge,
                    cost_buffer=message.cost_buffer,
                ).engine_config()
            )
        elif isinstance(message, WorkerResumeHandoff):
            journal.clear_intent_dedup()
        elif isinstance(message, WorkerCaptureExecution):
            if (message.validation_ref.partition == journal._partition
                    and message.validation_ref.process_generation == journal._process_generation):
                observe_execution_window(
                    message.execution_id, message.contracts,
                    intent_id=message.validation_ref.intent_id, reason=message.reason,
                )
        elif isinstance(message, WorkerRecoveryBooksRequest):
            try:
                reply = _read_worker_recovery_books(message, journal)
            except Exception:
                _events.exception("Worker recovery book lookup failed")
                reply = WorkerRecoveryBooksResult(
                    message, journal._partition, journal._process_generation, WorkerValidationReason.ERROR,
                )
            telemetry = journal._telemetry
            if telemetry is not None and telemetry._metrics_output is not None:
                try:
                    telemetry._metrics_output.put_nowait(reply)
                except queue.Full:
                    telemetry.record_drop("recovery_reply")
        elif isinstance(message, WorkerOpportunityValidationRequest):
            if (message.validation_ref.partition == journal._partition
                    and message.validation_ref.process_generation == journal._process_generation):
                observe_execution_window(
                    message.execution_id, (str(message.pair.left.id), str(message.pair.right.id)),
                    intent_id=message.validation_ref.intent_id,
                )
            try:
                reply = _validate_worker_opportunity(message, engine, journal)
            except Exception:
                _events.exception("Worker opportunity validation failed")
                reply = WorkerOpportunityValidationResult(message, WorkerValidationReason.ERROR, time.time_ns())
            telemetry = journal._telemetry
            if telemetry is not None and telemetry._metrics_output is not None:
                try:
                    telemetry._metrics_output.put_nowait(reply)
                except queue.Full:
                    telemetry.record_drop("validation_reply")


async def _worker_resources(
    partition: MarketWorkerPartition,
    stack: AsyncExitStack,
    telemetry: _WorkerTelemetry | None = None,
) -> tuple[MarketMatcher, dict[VenueID, Any], dict[VenueID, Any]]:
    """Compose only public adapters required by one approved partition route."""
    discovery: dict[VenueID, Any] = {}
    keys: dict[VenueID, Any] = {}
    fees: dict[VenueID, Any] = {}
    streams: dict[VenueID, Any] = {}
    resources: list[Any] = []
    if POLYMARKET_VENUE_ID in partition.venues:
        polymarket_discovery = PolymarketInstrumentDiscoveryAdapter()
        polynode_keys = PolynodeKeyExtractionAdapter()
        polymarket_keys = PolymarketKeyExtractionAdapter(
            polynode_key_extractor=polynode_keys
        )
        polymarket_fees = PolymarketTakerFeeCalculator()
        discovery[POLYMARKET_VENUE_ID] = polymarket_discovery
        keys[POLYMARKET_VENUE_ID] = polymarket_keys
        fees[POLYMARKET_VENUE_ID] = polymarket_fees
        streams[POLYMARKET_VENUE_ID] = PolymarketMarketDataStreamAdapter(
            loop_thread_count=2 if partition.name == "crypto-slow" else None,
        )
        resources.extend(
            (polymarket_discovery, polynode_keys, polymarket_keys, polymarket_fees)
        )
    if LIMITLESS_VENUE_ID in partition.venues:
        catalog = LimitlessMarketCatalog(
            cache_seconds=float(os.getenv("LIMITLESS_DISCOVERY_CACHE_SECONDS", "30"))
        )
        limitless_discovery = LimitlessInstrumentDiscoveryAdapter(catalog=catalog)
        limitless_keys = LimitlessKeyExtractionAdapter(catalog=catalog)
        limitless_fees = LimitlessTakerFeeCalculator(
            api_key=os.getenv("LIMITLESS_API_KEY"),
            api_secret=os.getenv("LIMITLESS_API_SECRET"),
        )
        discovery[LIMITLESS_VENUE_ID] = limitless_discovery
        keys[LIMITLESS_VENUE_ID] = limitless_keys
        fees[LIMITLESS_VENUE_ID] = limitless_fees
        streams[LIMITLESS_VENUE_ID] = LimitlessMarketDataStreamAdapter()
        resources.extend(
            (catalog, limitless_discovery, limitless_keys, limitless_fees)
        )
    if PREDICT_VENUE_ID in partition.venues:
        catalog = PredictMarketCatalog(
            cache_seconds=float(os.getenv("PREDICT_DISCOVERY_CACHE_SECONDS", "60")),
            requests_per_second=float(
                os.getenv("PREDICT_READ_REQUESTS_PER_SECOND", "3")
            ),
        )
        predict_discovery = PredictInstrumentDiscoveryAdapter(catalog=catalog)
        predict_keys = PredictKeyExtractionAdapter(catalog=catalog)
        predict_fees = PredictTakerFeeCalculator(catalog=catalog)
        discovery[PREDICT_VENUE_ID] = predict_discovery
        keys[PREDICT_VENUE_ID] = predict_keys
        fees[PREDICT_VENUE_ID] = predict_fees
        streams[PREDICT_VENUE_ID] = PredictMarketDataStreamAdapter()
        resources.extend((catalog, predict_discovery, predict_keys, predict_fees))
    for resource in resources:
        stack.push_async_callback(resource.close)
    if telemetry is not None:
        for stream in streams.values():
            set_observers = getattr(stream, "set_transport_observers", None)
            if set_observers is not None:
                set_observers(
                    telemetry.observe_queue,
                    telemetry.observe_transition,
                )
    return MarketMatcher(discovery, keys), streams, fees
