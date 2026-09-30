"""Expose process-wide operational metrics for control and data planes.

Responsibilities
----------------
- Measure outbound venue HTTP traffic and discovery refreshes.
- Report pipeline pressure, order-book drops, and event-loop lag.
- Report venue WebSocket pressure, transport lag, and resynchronizations.
- Track the currently monitored contract and pair cardinality.
"""

from prometheus_client import Counter, Gauge, Histogram


_SECONDS_BUCKETS = (
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
    10,
    30,
)

_PROCESSING_SECONDS_BUCKETS = (
    0.00001,
    0.000025,
    0.00005,
    0.0001,
    0.00025,
    0.0005,
    *_SECONDS_BUCKETS,
)

_PROCESSING_ITEMS_BUCKETS = (1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024)

HTTP_REQUESTS = Counter(
    "venue_http_requests_total",
    "Outbound HTTP responses received from venue services",
    ("venue", "host", "method", "endpoint", "status"),
)
HTTP_REQUEST_DURATION = Histogram(
    "venue_http_request_seconds",
    "Time until an outbound venue HTTP response or transport failure",
    ("venue", "host", "method", "endpoint"),
    buckets=_SECONDS_BUCKETS,
)
HTTP_SCHEDULER_WAIT = Histogram(
    "venue_http_scheduler_wait_seconds",
    "Time spent waiting for an outbound venue request slot",
    ("venue", "host", "reason"),
    buckets=_SECONDS_BUCKETS,
)
HTTP_SCHEDULER_PENDING = Gauge(
    "venue_http_scheduler_pending",
    "Outbound venue requests waiting or active in a shared scheduler",
    ("venue", "host"),
)
HTTP_RETRIES = Counter(
    "venue_http_retries_total",
    "Outbound venue requests resumed after a shared cooldown",
    ("venue", "host", "reason"),
)
DISCOVERY_REFRESHES = Counter(
    "market_discovery_refreshes_total",
    "Completed market discovery refreshes",
    ("outcome",),
)
DISCOVERY_REFRESH_DURATION = Histogram(
    "market_discovery_refresh_seconds",
    "Duration of one refresh across all configured market cycles",
    buckets=_SECONDS_BUCKETS,
)
PIPELINE_BUFFER_SIZE = Gauge(
    "trading_pipeline_buffer_size",
    "Current number of queued pipeline events",
    ("buffer",),
)
PIPELINE_BUFFER_CAPACITY = Gauge(
    "trading_pipeline_buffer_capacity",
    "Configured pipeline buffer capacity",
    ("buffer",),
)
PIPELINE_BUFFER_HIGH_WATERMARK = Gauge(
    "trading_pipeline_buffer_high_watermark",
    "Largest observed pipeline buffer occupancy",
    ("buffer",),
)
PIPELINE_ORDER_BOOK_DROPS = Counter(
    "trading_pipeline_order_book_drops_total",
    "Order-book updates discarded because the input buffer was full",
)
EVENT_LOOP_LAG = Histogram(
    "trading_event_loop_lag_seconds",
    "Delay beyond the pipeline metrics task's scheduled wake-up",
    buckets=_SECONDS_BUCKETS,
)
LOCAL_CLOCK = Gauge(
    "local_clock_observation",
    "Uncalibrated local clock diagnostics; units are identified by the measurement label",
    ("producer", "measurement"),
)


def update_clock_metrics(producer: str, sample: dict[str, object]) -> None:
    """Publish bounded clock diagnostics, never an assertion of UTC accuracy."""
    for measurement, key, divisor in (
        ("wall_step_seconds", "wall_step_ns", 1_000_000_000),
        ("maximum_step_seconds", "maximum_step_ns", 1_000_000_000),
        ("read_span_seconds", "read_span_ns", 1_000_000_000),
        ("discontinuities", "discontinuities", 1),
        ("monotonic_raw_rate_ppm", "monotonic_raw_rate_ppm", 1),
    ):
        value = sample.get(key)
        LOCAL_CLOCK.labels(producer, measurement).set(
            float(value) / divisor if value is not None else float("nan"),
        )


HTTP_TRANSPORT_FAILURES = Counter(
    "venue_http_transport_failures_total",
    "Outbound venue HTTP requests that failed before receiving a response",
    ("venue", "host", "method", "endpoint", "reason"),
)
MARKET_WORKER_UP = Gauge(
    "market_worker_up",
    "Whether a configured market-data worker process is alive and responsive",
    ("partition",),
)
MARKET_WORKER_HEARTBEAT_AGE = Gauge(
    "market_worker_heartbeat_age_seconds",
    "Elapsed parent monotonic time since the latest worker runtime summary",
    ("partition",),
)
MARKET_WORKER_GENERATION = Gauge(
    "market_worker_generation",
    "Monotonically increasing process generation for one market-data partition",
    ("partition",),
)
MARKET_WORKER_PID = Gauge(
    "market_worker_pid",
    "Operating-system process identifier for the current worker generation",
    ("partition",),
)
MARKET_WORKER_STARTS = Counter(
    "market_worker_starts_total",
    "Market-data worker process starts",
    ("partition",),
)
MARKET_WORKER_EXITS = Counter(
    "market_worker_unexpected_exits_total",
    "Unexpected market-data worker process exits",
    ("partition", "exit_code"),
)
MARKET_WORKER_RESTARTS = Counter(
    "market_worker_restarts_total",
    "Market-data worker process restarts after an unexpected exit",
    ("partition",),
)
MARKET_WORKER_EVENT_LOOP_LAG = Histogram(
    "market_worker_event_loop_lag_seconds",
    "Scheduling delay on each worker-owned market-data event loop",
    ("partition",),
    buckets=_SECONDS_BUCKETS,
)
MARKET_WORKER_CPU_SECONDS = Gauge(
    "market_worker_cpu_seconds",
    "Cumulative CPU time consumed by one market-data worker process",
    ("partition",),
)
MARKET_WORKER_MEMORY_BYTES = Gauge(
    "market_worker_memory_bytes",
    "Resident memory reported by one market-data worker process",
    ("partition",),
)
MARKET_WORKER_TIMING = Gauge(
    "market_worker_timing_seconds",
    "Latest one-second worker timing aggregate",
    ("partition", "venue", "stage"),
)
MARKET_WORKER_IPC_QUEUE_DEPTH = Gauge(
    "market_worker_ipc_queue_depth",
    "Current worker-to-parent event queue occupancy",
)
MARKET_WORKER_IPC_QUEUE_CAPACITY = Gauge(
    "market_worker_ipc_queue_capacity",
    "Configured worker-to-parent event queue capacity",
)
MARKET_WORKER_IPC_QUEUE_HIGH_WATERMARK = Gauge(
    "market_worker_ipc_queue_high_watermark",
    "Largest sampled worker-to-parent event queue occupancy",
)
MARKET_WORKER_IPC_ENQUEUE_FAILURES = Counter(
    "market_worker_ipc_enqueue_failures_total",
    "Messages not enqueued on a bounded multiprocessing queue",
    ("partition", "queue", "reason"),
)
MARKET_WORKER_INTENT_LATENCY = Histogram(
    "market_worker_intent_ipc_seconds",
    "Worker intent creation to authoritative parent dequeue latency",
    ("partition",),
    buckets=_SECONDS_BUCKETS,
)
MARKET_WORKER_INTENTS_REJECTED = Counter(
    "market_worker_intents_rejected_total",
    "Worker opportunity intents rejected before parent admission",
    ("partition", "reason"),
)
MARKET_WORKER_FUTURE_SOURCE = Counter(
    "market_worker_future_source_samples_total",
    "Worker books whose source timestamp is ahead of local transport wall time; not proof of venue delay",
    ("partition", "venue"),
)
MARKET_WORKER_VALIDATION_TOTAL = Counter(
    "market_worker_validation_results_total",
    "Final in-memory worker validation outcomes, including bounded repricing requests",
    ("partition", "outcome"),
)
MARKET_WORKER_VALIDATION_LATENCY = Histogram(
    "market_worker_validation_round_trip_seconds",
    "Parent request to final worker validation result, including IPC and scheduling",
    ("partition",),
    buckets=_SECONDS_BUCKETS,
)
MARKET_WORKER_VALIDATION_QUEUE_DEPTH = Gauge(
    "market_worker_validation_queue_depth",
    "Pending parent validation requests waiting for one worker result",
    ("partition",),
)
MARKET_WORKER_VALIDATION_QUEUE_CAPACITY = Gauge(
    "market_worker_validation_queue_capacity",
    "Bound on simultaneous parent validation requests",
    ("partition",),
)
MARKET_WORKER_VALIDATION_TIMEOUTS = Counter(
    "market_worker_validation_timeouts_total",
    "Worker validation requests that exceeded their bounded reply deadline",
    ("partition",),
)
MARKET_WORKER_BOOK_AGE = Histogram(
    "market_worker_book_age_seconds",
    "Worker-routed order-book age at each execution checkpoint",
    ("partition", "venue", "stage", "clock", "source_kind"),
    buckets=_SECONDS_BUCKETS,
)
MARKET_WORKER_WS_QUEUE_DEPTH = Gauge(
    "market_worker_ws_queue_depth_frames",
    "Largest current WebSocket receive-queue occupancy in one worker venue",
    ("partition", "venue"),
)
MARKET_WORKER_WS_QUEUE_HIGH_WATERMARK = Gauge(
    "market_worker_ws_queue_high_watermark_frames",
    "Largest WebSocket receive-queue occupancy observed by one worker venue",
    ("partition", "venue"),
)
MARKET_WORKER_WS_PAUSED = Gauge(
    "market_worker_ws_paused_streams",
    "Current paused WebSocket receive transports in one worker venue",
    ("partition", "venue"),
)
MARKET_WORKER_WS_QUEUE_WAIT_MAX = Gauge(
    "market_worker_ws_queue_wait_max_seconds",
    "Maximum WebSocket assembler enqueue-to-adapter-dequeue wait in the latest worker interval",
    ("partition", "venue"),
)
MARKET_WORKER_TRANSITIONS = Counter(
    "market_worker_transport_transitions_total",
    "Immediate worker WebSocket pause, overload, and resynchronization transitions",
    ("partition", "venue", "kind", "state"),
)
MARKET_WORKER_SHADOW_DETECTIONS = Counter(
    "market_worker_shadow_detections_total",
    "Shadow worker detections compared with the authoritative parent state",
    ("partition", "outcome"),
)
MONITORED_CONTRACTS = Gauge(
    "trading_monitored_contracts",
    "Contracts currently subscribed for market data",
    ("venue",),
)
MONITORED_PAIRS = Gauge(
    "trading_monitored_pairs",
    "Matched contract pairs currently monitored",
)
MARKET_FEED_ACTIVE_PUMPS = Gauge(
    "market_feed_active_pumps",
    "Live public market-data pumps owned by each venue feed",
    ("venue",),
)
MARKET_FEED_RECEIVE_TO_SINK = Histogram(
    "market_feed_receive_to_sink_seconds",
    "Time from local venue-frame receipt until the pipeline sink publish completes",
    ("venue",),
    buckets=_SECONDS_BUCKETS,
)
MARKET_FEED_SINK_PUBLISH = Histogram(
    "market_feed_sink_publish_seconds",
    "Time spent publishing one normalized order-book update to the pipeline sink",
    ("venue",),
    buckets=_SECONDS_BUCKETS,
)
MARKET_FEED_VENUE_TIMESTAMP_DELTA = Gauge(
    "market_feed_venue_timestamp_delta_seconds",
    "Signed local wall-clock minus venue order-book timestamp at sink handoff",
    ("venue",),
)
MARKET_FEED_VENUE_AGE = Histogram(
    "market_feed_venue_age_seconds",
    "Wall-clock age of venue order-book timestamps at pipeline sink handoff",
    ("venue",),
    buckets=_SECONDS_BUCKETS,
)
MARKET_FEED_SOURCE_TO_TRANSPORT_DELTA = Gauge(
    "market_feed_source_to_transport_delta_seconds",
    "Signed local transport wall clock minus the venue order-book timestamp",
    ("venue", "source_kind"),
)
MARKET_FEED_SOURCE_TO_TRANSPORT_AGE = Histogram(
    "market_feed_source_to_transport_age_seconds",
    "Non-negative venue timestamp age at the local transport callback",
    ("venue", "source_kind"),
    buckets=_SECONDS_BUCKETS,
)
MARKET_FEED_WS_QUEUE_DEPTH = Gauge(
    "market_feed_ws_queue_depth_frames",
    "Sampled current WebSocket receive queue depth",
    ("venue",),
)
MARKET_FEED_WS_QUEUE_HIGH_WATERMARK = Gauge(
    "market_feed_ws_queue_high_watermark_frames",
    "Largest observed WebSocket receive queue depth since adapter startup",
    ("venue",),
)
MARKET_FEED_WS_PAUSED = Gauge(
    "market_feed_ws_paused",
    "Whether a venue WebSocket receive transport is currently paused",
    ("venue",),
)
MARKET_FEED_WS_MESSAGE_QUEUE_WAIT = Histogram(
    "market_feed_ws_message_queue_wait_seconds",
    "Time a complete message waited in a venue WebSocket receive queue",
    ("venue",),
    buckets=_SECONDS_BUCKETS,
)
POLYMARKET_WS_EXPECTED_SOCKETS = Gauge(
    "polymarket_ws_expected_sockets",
    "Polymarket market sockets expected from the current condition set",
)
POLYMARKET_WS_ACTIVE_SOCKETS = Gauge(
    "polymarket_ws_active_sockets",
    "Currently connected Polymarket public market sockets",
)
POLYMARKET_WS_CONNECTIONS = Counter(
    "polymarket_ws_connections_total",
    "Successful Polymarket public market socket connections",
)
POLYMARKET_WS_FRAMES_RECEIVED = Counter(
    "polymarket_ws_frames_received_total",
    "Transport frames received from Polymarket public market sockets",
)
POLYMARKET_BOOKS_EMITTED = Counter(
    "polymarket_books_emitted_total",
    "Normalized Polymarket books emitted after burst coalescing",
)
POLYMARKET_BRIDGE_REPLACEMENTS = Counter(
    "polymarket_bridge_replacements_total",
    "Unread Polymarket snapshots replaced by a newer book for the same contract",
)
POLYMARKET_BRIDGE_PENDING = Gauge(
    "polymarket_bridge_pending_books",
    "Polymarket contract snapshots currently pending in the worker bridge",
)
POLYMARKET_WS_QUEUE_DEPTH = Gauge(
    "polymarket_ws_queue_depth_frames",
    "Sampled largest current websockets receive queue across Polymarket sockets",
)
POLYMARKET_WS_QUEUE_HIGH_WATERMARK = Gauge(
    "polymarket_ws_queue_high_watermark_frames",
    "Largest Polymarket websockets receive queue observed since adapter startup",
)
POLYMARKET_WS_PAUSED_SOCKETS = Gauge(
    "polymarket_ws_paused_sockets",
    "Polymarket market sockets whose websockets receive transport is paused",
)
POLYMARKET_WS_MESSAGE_AGE = Histogram(
    "polymarket_ws_message_age_seconds",
    "Wall-clock age of Polymarket venue timestamps when dequeued",
    ("condition_id", "event_type"),
    buckets=_SECONDS_BUCKETS,
)
POLYMARKET_WS_MESSAGE_QUEUE_WAIT = Histogram(
    "polymarket_ws_message_queue_wait_seconds",
    "Time a Polymarket message waited in websockets' receive queue",
    ("condition_id",),
    buckets=_SECONDS_BUCKETS,
)
POLYMARKET_WS_VENUE_TIMESTAMP_DELTA = Gauge(
    "polymarket_ws_venue_timestamp_delta_seconds",
    "Signed local wall-clock minus Polymarket message timestamp when dequeued",
    ("event_type",),
)
POLYMARKET_WS_DEQUEUE_TO_EMIT = Histogram(
    "polymarket_ws_dequeue_to_emit_seconds",
    "Time from dequeuing a Polymarket frame to emitting its latest normalized book",
    buckets=_SECONDS_BUCKETS,
)
POLYMARKET_WS_PROCESSING = Histogram(
    "polymarket_ws_processing_seconds",
    "Sampled Polymarket worker processing time by market and transformation stage",
    ("condition_id", "stage"),
    buckets=_PROCESSING_SECONDS_BUCKETS,
)
POLYMARKET_WS_PROCESSING_ITEMS = Histogram(
    "polymarket_ws_processing_items",
    "Items handled by sampled Polymarket worker transformation stages",
    ("condition_id", "stage"),
    buckets=_PROCESSING_ITEMS_BUCKETS,
)
POLYMARKET_WS_RESYNCS = Counter(
    "polymarket_ws_resyncs_total",
    "Polymarket order-book resynchronizations",
    ("reason",),
)
POLYMARKET_WS_QUEUE_OVERLOADS = Counter(
    "polymarket_ws_queue_overloads_total",
    "Polymarket receive-queue overloads by terminal outcome",
    ("outcome",),
)


def scheduled_lags(
    now: float,
    deadline: float,
    interval: float,
) -> tuple[tuple[float, ...], float]:
    """Return every overdue scheduled-probe delay and the next deadline.

    Parameters
    ----------
    now
        Current monotonic time.
    deadline
        Monotonic time of the next scheduled probe.
    interval
        Positive fixed interval between probes.

    Returns
    -------
    tuple[tuple[float, ...], float]
        Delays for all probes due by ``now`` and the first future deadline.

    Raises
    ------
    ValueError
        If ``interval`` is not positive.

    Notes
    -----
    - Advancing from the prior deadline preserves probes missed during an event-loop
      stall instead of coordinating the monitor with the stalled system.
    """
    if interval <= 0:
        raise ValueError("interval must be positive")
    due = max(0, int((now - deadline) // interval) + 1)
    return (
        tuple(max(0.0, now - (deadline + index * interval)) for index in range(due)),
        deadline + due * interval,
    )


_CAPTURE_PRODUCERS = frozenset((
    "parent", "btc-5m", "btc-15m", "eth-5m", "eth-15m", "crypto-slow", "unknown",
))
_CAPTURE_STATES = (
    "disabled", "running", "closed", "disk_limit", "io_error", "start_failed", "unknown",
)
PREDICT_FILL_CAPTURE_STATE = Gauge(
    "predict_fill_capture_state",
    "One-hot diagnostic capture state reported by the parent or current worker generation",
    ("producer", "state"),
)
PREDICT_FILL_CAPTURE_DROPPED = Gauge(
    "predict_fill_capture_dropped_samples",
    "Cumulative lost diagnostic samples during the current recorder run; resets on restart",
    ("producer",),
)
PREDICT_FILL_CAPTURE_OFFERS_AFTER_STOP = Gauge(
    "predict_fill_capture_offers_after_stop",
    "Samples offered after the current optional recorder stopped or exhausted capacity",
    ("producer",),
)
PREDICT_FILL_CAPTURE_BYTES = Gauge(
    "predict_fill_capture_bytes",
    "Bytes written across the current recorder run's segments, excluding historical runs",
    ("producer",),
)
PREDICT_FILL_CAPTURE_QUEUE_DEPTH = Gauge(
    "predict_fill_capture_queue_depth",
    "Current samples pending in the bounded diagnostic writer queue",
    ("producer",),
)
PREDICT_FILL_CAPTURE_QUEUE_CAPACITY = Gauge(
    "predict_fill_capture_queue_capacity",
    "Maximum pending samples in the diagnostic writer queue",
    ("producer",),
)
PREDICT_FILL_CAPTURE_WINDOWS = Gauge(
    "predict_fill_capture_active_windows",
    "Current bounded execution windows tracked by the optional recorder",
    ("producer",),
)
PREDICT_FILL_CAPTURE_WRITER_ALIVE = Gauge(
    "predict_fill_capture_writer_alive",
    "Whether the optional writer thread was alive at its latest status observation",
    ("producer",),
)
PREDICT_FILL_CAPTURE_WRITER_AGE = Gauge(
    "predict_fill_capture_writer_progress_age_seconds",
    "Time since the optional writer last advanced its loop, measured by its process",
    ("producer",),
)


def _capture_number(value: object) -> float:
    """Represent unavailable or malformed diagnostic values as unknown in metrics."""
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return float("nan")
    return number if 0 <= number < float("inf") else float("nan")


def update_predict_fill_capture_metrics(producer: str, capture: dict[str, object] | None) -> None:
    """Publish one in-memory capture snapshot with bounded producer and state labels.

    Parameters
    ----------
    producer
        ``parent``, a configured partition, or ``worker:partition:generation``.
        Unknown names share one fallback label; process, run, execution and
        market IDs are excluded.
    capture
        Process-local recorder status or the latest worker summary. ``None``
        clears stale generation values to unknown until a new summary arrives.

    Notes
    -----
    - Cumulative loss and bytes use gauges because each recorder run starts at
      zero. Worker callers update these once per summary, never per book.
    - No filesystem reads, venue requests or metric labels derived from captures
      are performed here. Worker heartbeat age separately identifies old samples.
    """
    if producer.startswith("worker:"):
        producer = producer.split(":", 2)[1]
    producer = producer if producer in _CAPTURE_PRODUCERS else "unknown"
    values = capture if isinstance(capture, dict) else {}
    state = values.get("status", "unknown")
    if state not in _CAPTURE_STATES:
        state = "unknown"
    for candidate in _CAPTURE_STATES:
        PREDICT_FILL_CAPTURE_STATE.labels(producer, candidate).set(int(candidate == state))
    counts = values.get("counts")
    counts = counts if isinstance(counts, dict) else {}
    for metric, value in (
        (PREDICT_FILL_CAPTURE_DROPPED, counts.get("dropped", 0) if values else None),
        (PREDICT_FILL_CAPTURE_OFFERS_AFTER_STOP, counts.get("offers_after_stop", 0) if values else None),
        (PREDICT_FILL_CAPTURE_BYTES, values.get("bytes")),
        (PREDICT_FILL_CAPTURE_QUEUE_DEPTH, values.get("queue_depth")),
        (PREDICT_FILL_CAPTURE_QUEUE_CAPACITY, values.get("queue_capacity")),
        (PREDICT_FILL_CAPTURE_WINDOWS, values.get("active_windows")),
        (PREDICT_FILL_CAPTURE_WRITER_ALIVE, values.get("writer_alive")),
        (PREDICT_FILL_CAPTURE_WRITER_AGE, values.get("writer_progress_age_seconds")),
    ):
        metric.labels(producer).set(_capture_number(value))
