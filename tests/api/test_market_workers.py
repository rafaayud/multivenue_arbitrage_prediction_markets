"""Verify generic market-worker partitioning, IPC, and lifecycle."""

import asyncio
import queue
import time
from dataclasses import replace
from decimal import Decimal
from types import SimpleNamespace

import pytest

from prediction_markets.api.runtime.market_workers import (
    MarketWorkerMode,
    MarketWorkerPartition,
    MarketWorkerSupervisor,
    WorkerEvent,
    WorkerFailure,
    WorkerMetricTransition,
    WorkerOpportunityIntent,
    WorkerTickSizeChange,
    _EVENT_BATCH_CAPACITY,
    _INTENT_MAX_AGE_NS,
    _WorkerJournal,
    _WorkerTelemetry,
    _queue_get_batch,
    _wait_for_process_stop,
    market_worker_partitions,
    order_book_generation,
)
from prediction_markets.application.events import (
    ArbitrageOpportunityFound,
    MarketMatchesUpdated,
    OrderBookPairUpdated,
)
from prediction_markets.application.markets.models import MarketCycle
from prediction_markets.application.state import TradingState
from prediction_markets.domain.arbitrage.value_objects import ArbitrageOpportunity
from prediction_markets.domain.contracts.entities import BinaryContract
from prediction_markets.domain.contracts.value_objects import LotSize, TickSize
from prediction_markets.domain.market_matching.value_objects import (
    MatchedContractPair,
    Underlying,
)
from prediction_markets.domain.orderbook.entities import OrderBook
from prediction_markets.domain.orderbook.value_objects import OrderBookLevel
from prediction_markets.domain.shared.value_objects import (
    ContractID,
    Currency,
    MarketID,
    OutcomeID,
    Price,
    Quantity,
    Timestamp,
    VenueID,
)
from prediction_markets.domain.trading.enums import OrderSide
from prediction_markets.infrastructure.operational_metrics import MARKET_WORKER_INTENTS_REJECTED
from prediction_markets.infrastructure.venues.limitless.mappers import (
    LIMITLESS_VENUE_ID,
)
from prediction_markets.infrastructure.venues.polymarket.mappers import (
    POLYMARKET_VENUE_ID,
)
from prediction_markets.infrastructure.venues.predict.mappers import PREDICT_VENUE_ID
from prediction_markets.infrastructure.websocket_transport import (
    WebSocketQueueSample,
    WebSocketTransition,
)


class _Sink:
    def __init__(self) -> None:
        self.events: list[object] = []

    async def publish(self, event: object) -> None:
        self.events.append(event)


def _idle_worker(
    _partition: object,
    _generation: str,
    _config: object,
    _control: object,
    _events: object,
    _metrics: object,
    stop: object,
) -> None:
    """Remain alive until the supervisor sets the process-shared stop event."""
    stop.wait()


def _failing_worker(
    _partition: object,
    _generation: str,
    _config: object,
    _control: object,
    _events: object,
    _metrics: object,
    _stop: object,
) -> None:
    """Exit abnormally so the real supervisor watcher can fail closed."""
    raise RuntimeError("worker failed")


def _contract(name: str, venue: VenueID, outcome: str) -> BinaryContract:
    return BinaryContract(
        id=ContractID(name),
        market_id=MarketID(f"{name}-market"),
        outcome_id=OutcomeID(outcome),
        venue_id=venue,
        payout_currency=Currency("USD"),
        tick_size=TickSize(Decimal("0.01")),
        lot_size=LotSize(Decimal("1")),
    )


def _book(
    contract: BinaryContract,
    price: str,
    *,
    bid: bool = False,
    source_kind: str = "snapshot_state",
    source_age_ms: int = 0,
) -> OrderBook:
    now_ns = time.monotonic_ns()
    now_wall_ns = time.time_ns()
    level = OrderBookLevel(Price(Decimal(price)), Quantity(Decimal("10")))
    return OrderBook(
        contract.market_id,
        contract.outcome_id,
        (level,) if bid else (),
        () if bid else (level,),
        timestamp=Timestamp.now(),
        source_at_ns=now_wall_ns - source_age_ms * 1_000_000,
        arrival_wall_at_ns=now_wall_ns,
        arrival_at_ns=now_ns,
        source_timestamp_kind=source_kind,
        received_at_ns=now_ns,
    )


def _pair() -> tuple[MarketCycle, MatchedContractPair, OrderBook, OrderBook]:
    cycle = MarketCycle(Underlying("BTC"), 300)
    left = _contract("left", POLYMARKET_VENUE_ID, "yes")
    right = _contract("right", LIMITLESS_VENUE_ID, "no")
    pair = MatchedContractPair(
        left,
        right,
        Timestamp.from_iso("2099-01-01T00:00:00Z"),
    )
    return cycle, pair, _book(left, "0.40"), _book(right, "0.50")


def _detected(
    cycle: MarketCycle,
    pair: MatchedContractPair,
    left: OrderBook,
    right: OrderBook,
) -> ArbitrageOpportunityFound:
    return ArbitrageOpportunityFound(
        "opportunity",
        cycle,
        pair,
        ArbitrageOpportunity(
            pair.left.id,
            pair.right.id,
            OrderSide.BUY,
            left.best_ask(),
            right.best_ask(),
            Quantity(Decimal("5")),
            Decimal("0.1"),
            Decimal("0.1"),
            0,
            Timestamp.now(),
        ),
    )


def test_default_partitions_assign_every_enabled_cycle_once() -> None:
    """Start only the slow worker for the enabled hourly and daily crypto cycles."""
    partitions = market_worker_partitions()

    assert tuple(partition.name for partition in partitions) == ("crypto-slow",)
    assigned = tuple(cycle for partition in partitions for cycle in partition.cycles)
    assert len(assigned) == len(set(assigned)) == 6
    assert {cycle.interval_seconds for cycle in assigned} == {3600, 86400}
    assert {cycle.underlying.symbol for cycle in assigned} == {"BTC", "ETH", "BNB"}
    assert partitions[0].venues == (POLYMARKET_VENUE_ID, PREDICT_VENUE_ID)
    assert all(partition.cpu_index is None for partition in partitions)


def test_fast_partition_support_requires_explicit_cycles() -> None:
    """Keep partition mechanics testable without enabling fast cycles by default."""
    partitions = market_worker_partitions(
        tuple(MarketCycle(Underlying(symbol), interval)
              for symbol in ("BTC", "ETH") for interval in (300, 900)),
    )
    assert tuple(partition.name for partition in partitions) == (
        "btc-5m",
        "btc-15m",
        "eth-5m",
        "eth-15m",
    )
    assigned = tuple(cycle for partition in partitions for cycle in partition.cycles)
    assert len(assigned) == len(set(assigned)) == 4
    assert partitions[0].venues == (POLYMARKET_VENUE_ID, LIMITLESS_VENUE_ID)
    assert all(partition.cpu_index is None for partition in partitions)


def test_worker_journal_forwards_an_atomic_versioned_intent() -> None:
    cycle, pair, left, right = _pair()
    output: queue.Queue[object] = queue.Queue(maxsize=2)
    state = TradingState(books={pair.left.id: left, pair.right.id: right})
    opportunity = ArbitrageOpportunity(
        pair.left.id,
        pair.right.id,
        OrderSide.BUY,
        left.best_ask(),
        right.best_ask(),
        Quantity(Decimal("10")),
        Decimal("0.1"),
        Decimal("0.1"),
        0,
        Timestamp.now(),
    )

    detected = ArbitrageOpportunityFound("opportunity", cycle, pair, opportunity)
    _WorkerJournal("btc-5m", output, state, "generation").append(detected)

    intent = output.get_nowait()
    assert isinstance(intent, WorkerOpportunityIntent)
    assert intent.intent_id == "generation:1"
    assert intent.left_book_generation == order_book_generation(left)
    assert intent.right_book_generation == order_book_generation(right)
    assert intent.detected == detected


def test_supervisor_rejects_an_intent_after_25_ms() -> None:
    cycle, pair, left, right = _pair()
    partition = MarketWorkerPartition(
        "btc-5m",
        (cycle,),
        (POLYMARKET_VENUE_ID, LIMITLESS_VENUE_ID),
    )
    sink = _Sink()
    supervisor = MarketWorkerSupervisor(
        SimpleNamespace(sink=sink),
        min_net_edge=Decimal("0"),
        cost_buffer=Decimal("0"),
        mode=MarketWorkerMode.ACTIVE,
        partitions=(partition,),
    )
    supervisor._process_generations[partition.name] = "generation"
    intent = WorkerOpportunityIntent(
        partition.name,
        "generation",
        1,
        "generation:1",
        time.time_ns(),
        _detected(cycle, pair, left, right),
        left,
        right,
        order_book_generation(left),
        order_book_generation(right),
    )

    asyncio.run(supervisor._consume_message(replace(
        intent, sent_monotonic_at_ns=time.monotonic_ns() - 26 * 1_000_000,
    )))

    assert _INTENT_MAX_AGE_NS == 25 * 1_000_000
    assert sink.events == []


def test_supervisor_rejects_reordered_and_old_generation_intents() -> None:
    cycle, pair, left, right = _pair()
    partition = MarketWorkerPartition(
        "btc-5m",
        (cycle,),
        (POLYMARKET_VENUE_ID, LIMITLESS_VENUE_ID),
    )
    sink = _Sink()
    supervisor = MarketWorkerSupervisor(
        SimpleNamespace(sink=sink),
        min_net_edge=Decimal("0"),
        cost_buffer=Decimal("0"),
        mode=MarketWorkerMode.ACTIVE,
        partitions=(partition,),
    )
    supervisor._process_generations[partition.name] = "generation"
    match = WorkerEvent(
        partition.name,
        "generation",
        1,
        MarketMatchesUpdated(cycle, (pair,)),
    )
    intent = WorkerOpportunityIntent(
        partition.name,
        "generation",
        2,
        "generation:2",
        time.time_ns(),
        _detected(cycle, pair, left, right),
        left,
        right,
        order_book_generation(left),
        order_book_generation(right),
    )

    async def consume() -> None:
        await supervisor._consume_message(match)
        await supervisor._consume_message(intent)
        await supervisor._consume_message(intent)
        await supervisor._consume_message(
            replace(intent, process_generation="old", sequence=3)
        )

    asyncio.run(consume())

    assert len(sink.events) == 2
    assert isinstance(sink.events[0], MarketMatchesUpdated)
    assert isinstance(sink.events[1], OrderBookPairUpdated)


def _batch_subject():
    """Create a current worker intent and real admission consumer for burst checks."""
    cycle, pair, left, right = _pair()
    partition = MarketWorkerPartition(
        "btc-5m", (cycle,), (POLYMARKET_VENUE_ID, LIMITLESS_VENUE_ID),
    )
    sink = _Sink()
    supervisor = MarketWorkerSupervisor(
        SimpleNamespace(sink=sink), min_net_edge=Decimal("0"), cost_buffer=Decimal("0"),
        mode=MarketWorkerMode.ACTIVE, partitions=(partition,),
    )
    supervisor._process_generations[partition.name] = "generation"
    intent = WorkerOpportunityIntent(
        partition.name, "generation", 1, "generation:1", time.time_ns(),
        _detected(cycle, pair, left, right), left, right,
        order_book_generation(left), order_book_generation(right),
    )
    return supervisor, sink, intent


@pytest.mark.parametrize("other_key", ("pair", "side"))
def test_batch_keeps_latest_pair_in_surviving_sequence_order(other_key):
    """A1, B2, A3 publishes only B2, A3, without merging routes or directions."""
    supervisor, sink, first = _batch_subject()
    other = replace(first, sequence=2, intent_id="generation:2")
    if other_key == "side":
        other = replace(other, detected=replace(other.detected,
            opportunity=replace(other.detected.opportunity, side=OrderSide.SELL)))
    else:
        original = other.detected.pair.left
        contract = replace(original, id=ContractID("other-left"), market_id=MarketID("other-market"))
        pair = replace(other.detected.pair, left=contract)
        book = replace(other.left_order_book, market_id=contract.market_id)
        other = replace(other,
            detected=replace(other.detected, pair=pair,
                opportunity=replace(other.detected.opportunity, left_contract_id=contract.id)),
            left_order_book=book, left_book_generation=order_book_generation(book))
    latest = replace(first, sequence=3, intent_id="generation:3")
    counter = MARKET_WORKER_INTENTS_REJECTED.labels(first.partition, "coalesced")
    before = counter._value.get()

    asyncio.run(supervisor._consume_event_batch((first, other, latest)))

    assert [event.detected.validation_ref.sequence for event in sink.events] == [2, 3]
    assert counter._value.get() == before + 1
    assert supervisor._message_sequences[(first.partition, first.process_generation)] == 3


@pytest.mark.parametrize("barrier_kind", ("matches", "tick", "failure"))
def test_batch_does_not_coalesce_across_control_or_failure_barriers(barrier_kind):
    """Metadata and failures retain causal order between opportunity generations."""
    supervisor, sink, first = _batch_subject()
    pair, cycle = first.detected.pair, first.detected.cycle
    matches = MarketMatchesUpdated(cycle, (pair,))
    supervisor._worker_matches[cycle] = matches
    observed = []
    if barrier_kind == "matches":
        barrier = WorkerEvent(first.partition, first.process_generation, 2, matches)
    elif barrier_kind == "tick":
        barrier = WorkerTickSizeChange(first.partition, first.process_generation, 2,
            pair.left.venue_id, pair.left.id, TickSize(Decimal("0.001")))
        supervisor._on_tick_size_change = lambda *_: observed.append(len(sink.events))
    else:
        barrier = WorkerFailure(first.partition, first.process_generation, "worker stopped")
    latest = replace(first, sequence=3, intent_id="generation:3")

    asyncio.run(supervisor._consume_event_batch((first, barrier, latest)))

    assert [event.detected.validation_ref.sequence for event in sink.events
            if isinstance(event, OrderBookPairUpdated)] == [1, 3]
    if barrier_kind == "matches":
        assert sink.events[1] == matches
    elif barrier_kind == "tick":
        assert observed == [1]
    else:
        assert supervisor.error is not None and "worker stopped" in str(supervisor.error)


def test_batch_preserves_duplicate_generation_and_stale_rejections():
    """Invalid later messages cannot replace a current intent or bypass admission."""
    supervisor, sink, first = _batch_subject()
    supervisor._message_sequences[(first.partition, first.process_generation)] = 1
    current = replace(first, sequence=3, intent_id="generation:3")
    reasons = []
    supervisor._reject_intent = lambda _, reason: reasons.append(reason)
    old = replace(first, sequence=4, process_generation="old")
    stale = replace(first, sequence=4, intent_id="generation:4",
        sent_monotonic_at_ns=time.monotonic_ns() - 26_000_000)

    asyncio.run(supervisor._consume_event_batch((first, current, current, old, stale)))

    assert [event.detected.validation_ref.sequence for event in sink.events] == [3]
    assert reasons == ["order", "order", "generation", "stale_age"]


def test_event_batch_drain_is_bounded_and_does_not_wait_for_more_messages():
    """Drain available events immediately and retain the sampled pre-drain pressure."""
    source = queue.Queue()
    for sequence in range(70):
        source.put_nowait(sequence)
    messages, depth = _queue_get_batch(source)
    assert _EVENT_BATCH_CAPACITY == 64
    assert messages == tuple(range(64)) and depth == 69
    messages, depth = _queue_get_batch(source)
    assert messages == tuple(range(64, 70)) and depth == 5
    assert source.empty()


def test_batched_event_consumer_keeps_shutdown_cancellation_bounded():
    """An empty IPC queue retains the existing short get timeout during shutdown."""
    async def run():
        supervisor, _, _ = _batch_subject()
        supervisor._events_queue = queue.Queue()
        task = asyncio.create_task(supervisor._consume_events())
        await asyncio.sleep(0)
        task.cancel()
        result = await asyncio.wait_for(asyncio.gather(task, return_exceptions=True), 0.5)
        assert isinstance(result[0], asyncio.CancelledError)

    asyncio.run(run())


def test_supervisor_defers_source_age_to_the_parent_submission_guard() -> None:
    cycle, pair, _, right = _pair()
    left = _book(
        pair.left,
        "0.40",
        source_kind="venue_update",
        source_age_ms=401,
    )
    partition = MarketWorkerPartition(
        "btc-5m",
        (cycle,),
        (POLYMARKET_VENUE_ID, LIMITLESS_VENUE_ID),
    )
    sink = _Sink()
    supervisor = MarketWorkerSupervisor(
        SimpleNamespace(sink=sink),
        min_net_edge=Decimal("0"),
        cost_buffer=Decimal("0"),
        mode=MarketWorkerMode.ACTIVE,
        partitions=(partition,),
    )
    supervisor._process_generations[partition.name] = "generation"
    intent = WorkerOpportunityIntent(
        partition.name,
        "generation",
        1,
        "generation:1",
        time.time_ns(),
        _detected(cycle, pair, left, right),
        left,
        right,
        order_book_generation(left),
        order_book_generation(right),
    )

    asyncio.run(supervisor._consume_message(intent))

    assert len(sink.events) == 1
    assert isinstance(sink.events[0], OrderBookPairUpdated)


def test_supervisor_admits_old_local_books_from_a_fresh_intent() -> None:
    """Do not confuse a quiet book with a delayed IPC intent."""
    cycle, pair, _, right = _pair()
    now_ns = time.monotonic_ns()
    now_wall_ns = time.time_ns()
    left = _book(
        pair.left,
        "0.40",
        source_kind="venue_update",
        source_age_ms=200,
    )
    left = replace(
        left,
        arrival_wall_at_ns=now_wall_ns - 500 * 1_000_000,
        arrival_at_ns=now_ns - 500 * 1_000_000,
        received_at_ns=now_ns - 500 * 1_000_000,
    )
    partition = MarketWorkerPartition(
        "btc-5m",
        (cycle,),
        (POLYMARKET_VENUE_ID, LIMITLESS_VENUE_ID),
    )
    sink = _Sink()
    supervisor = MarketWorkerSupervisor(
        SimpleNamespace(sink=sink),
        min_net_edge=Decimal("0"),
        cost_buffer=Decimal("0"),
        mode=MarketWorkerMode.ACTIVE,
        partitions=(partition,),
    )
    supervisor._process_generations[partition.name] = "generation"
    intent = WorkerOpportunityIntent(
        partition.name,
        "generation",
        1,
        "generation:1",
        time.time_ns(),
        _detected(cycle, pair, left, right),
        left,
        right,
        order_book_generation(left),
        order_book_generation(right),
    )

    asyncio.run(supervisor._consume_message(intent))

    assert len(sink.events) == 1
    assert isinstance(sink.events[0], OrderBookPairUpdated)


def test_supervisor_observes_external_age_without_rejecting_the_intent() -> None:
    """Admit a locally fresh intent while retaining its old source timestamp."""
    cycle, pair, _, right = _pair()
    left = _book(
        pair.left,
        "0.40",
        source_kind="venue_update",
        source_age_ms=500,
    )
    partition = MarketWorkerPartition(
        "btc-5m",
        (cycle,),
        (POLYMARKET_VENUE_ID, LIMITLESS_VENUE_ID),
    )
    sink = _Sink()
    supervisor = MarketWorkerSupervisor(
        SimpleNamespace(sink=sink),
        min_net_edge=Decimal("0"),
        cost_buffer=Decimal("0"),
        mode=MarketWorkerMode.ACTIVE,
        partitions=(partition,),
    )
    supervisor._process_generations[partition.name] = "generation"
    intent = WorkerOpportunityIntent(
        partition.name,
        "generation",
        1,
        "generation:1",
        time.time_ns(),
        _detected(cycle, pair, left, right),
        left,
        right,
        order_book_generation(left),
        order_book_generation(right),
    )

    asyncio.run(supervisor._consume_message(intent))

    assert len(sink.events) == 1
    assert isinstance(sink.events[0], OrderBookPairUpdated)


def test_metrics_queue_saturation_never_blocks_worker_journal() -> None:
    cycle, pair, left, right = _pair()
    output: queue.Queue[object] = queue.Queue(maxsize=1)
    output.put_nowait(object())
    telemetry = _WorkerTelemetry("btc-5m", "generation")
    state = TradingState(books={pair.left.id: left, pair.right.id: right})
    opportunity = ArbitrageOpportunity(
        pair.left.id,
        pair.right.id,
        OrderSide.BUY,
        left.best_ask(),
        right.best_ask(),
        Quantity(Decimal("10")),
        Decimal("0.1"),
        Decimal("0.1"),
        0,
        Timestamp.now(),
    )

    _WorkerJournal("btc-5m", output, state, "generation", telemetry).append(
        ArbitrageOpportunityFound("opportunity", cycle, pair, opportunity)
    )
    summary = telemetry.snapshot(0, output)

    assert summary.enqueue_drops == (("events", "full", 1),)


def test_worker_telemetry_aggregates_queue_samples_and_sends_transitions() -> None:
    metrics: queue.Queue[object] = queue.Queue(maxsize=2)
    telemetry = _WorkerTelemetry("btc-5m", "generation", metrics)
    telemetry.observe_queue(
        WebSocketQueueSample("POLYMARKET", "condition", 12, 20, True)
    )
    telemetry.observe_queue(
        WebSocketQueueSample(
            "POLYMARKET",
            "condition",
            queue_wait_seconds=0.04,
        )
    )
    telemetry.observe_transition(
        WebSocketTransition("POLYMARKET", "condition", "pause", "started")
    )

    transition = metrics.get_nowait()
    summary = telemetry.snapshot(0.01, queue.Queue())

    assert isinstance(transition, WorkerMetricTransition)
    assert transition.transition.state == "started"
    assert summary.queues[0].depth == 12
    assert summary.queues[0].high_watermark == 20
    assert summary.queues[0].paused_streams == 1
    assert summary.queues[0].queue_wait_max_seconds == 0.04


def test_full_metrics_queue_is_counted_without_blocking_market_data() -> None:
    """Drop immediate transitions into the next bounded worker summary."""
    metrics: queue.Queue[object] = queue.Queue(maxsize=1)
    metrics.put_nowait(object())
    telemetry = _WorkerTelemetry("btc-5m", "generation", metrics)

    telemetry.observe_transition(
        WebSocketTransition("POLYMARKET", "condition", "overload", "started")
    )
    summary = telemetry.snapshot(0, queue.Queue())

    assert summary.enqueue_drops == (("metrics_transition", "full", 1),)


def test_worker_resume_handoff_clears_intent_deduplication() -> None:
    cycle, pair, left, right = _pair()
    output: queue.Queue[object] = queue.Queue(maxsize=3)
    state = TradingState(books={pair.left.id: left, pair.right.id: right})
    opportunity = ArbitrageOpportunity(
        pair.left.id,
        pair.right.id,
        OrderSide.BUY,
        left.best_ask(),
        right.best_ask(),
        Quantity(Decimal("10")),
        Decimal("0.1"),
        Decimal("0.1"),
        0,
        Timestamp.now(),
    )
    event = ArbitrageOpportunityFound("opportunity", cycle, pair, opportunity)
    journal = _WorkerJournal("btc-5m", output, state, "generation")

    journal.append(event)
    journal.append(event)
    journal.clear_intent_dedup()
    journal.append(event)

    intents = (output.get_nowait(), output.get_nowait())
    assert [intent.sequence for intent in intents] == [1, 2]


def test_shadow_worker_events_do_not_update_authoritative_state() -> None:
    cycle, pair, left, right = _pair()
    partition = MarketWorkerPartition(
        "btc-5m",
        (cycle,),
        (POLYMARKET_VENUE_ID, LIMITLESS_VENUE_ID),
    )
    sink = _Sink()
    supervisor = MarketWorkerSupervisor(
        SimpleNamespace(sink=sink),
        min_net_edge=Decimal("0"),
        cost_buffer=Decimal("0"),
        mode=MarketWorkerMode.SHADOW,
        partitions=(partition,),
    )
    supervisor._process_generations[partition.name] = "generation"
    messages = (
        WorkerEvent(
            partition.name,
            "generation",
            1,
            MarketMatchesUpdated(cycle, (pair,)),
        ),
        WorkerOpportunityIntent(
            partition.name,
            "generation",
            2,
            "generation:2",
            time.time_ns(),
            _detected(cycle, pair, left, right),
            left,
            right,
            order_book_generation(left),
            order_book_generation(right),
        ),
    )

    async def consume() -> None:
        for message in messages:
            await supervisor._consume_message(message)

    asyncio.run(consume())

    assert sink.events == []


def test_slow_match_preparation_does_not_block_or_backlog_ipc() -> None:
    """Drain intents while retaining only the newest pending match per cycle."""
    cycle, pair, left, right = _pair()
    partition = MarketWorkerPartition(
        "btc-5m",
        (cycle,),
        (POLYMARKET_VENUE_ID, LIMITLESS_VENUE_ID),
    )
    sink = _Sink()
    preparation_started = asyncio.Event()
    release_preparation = asyncio.Event()
    second_preparation_done = asyncio.Event()
    prepared: list[MarketMatchesUpdated] = []

    async def prepare(event: MarketMatchesUpdated) -> None:
        prepared.append(event)
        if len(prepared) == 1:
            preparation_started.set()
            await release_preparation.wait()
        else:
            second_preparation_done.set()

    supervisor = MarketWorkerSupervisor(
        SimpleNamespace(sink=sink),
        min_net_edge=Decimal("0"),
        cost_buffer=Decimal("0"),
        mode=MarketWorkerMode.ACTIVE,
        partitions=(partition,),
        on_cycle_matches=prepare,
    )
    supervisor._process_generations[partition.name] = "generation"
    first_match = WorkerEvent(
        partition.name,
        "generation",
        1,
        MarketMatchesUpdated(cycle, (pair,)),
    )
    intent = WorkerOpportunityIntent(
        partition.name,
        "generation",
        2,
        "generation:2",
        time.time_ns(),
        _detected(cycle, pair, left, right),
        left,
        right,
        order_book_generation(left),
        order_book_generation(right),
    )
    superseded_match = WorkerEvent(
        partition.name,
        "generation",
        3,
        MarketMatchesUpdated(cycle, (pair,)),
    )
    latest_match = WorkerEvent(
        partition.name,
        "generation",
        4,
        MarketMatchesUpdated(cycle, ()),
    )

    async def run() -> None:
        preparation_task = asyncio.create_task(
            supervisor._prepare_match_updates()
        )
        try:
            await supervisor._consume_message(first_match)
            await asyncio.wait_for(preparation_started.wait(), timeout=0.5)
            await asyncio.wait_for(
                supervisor._consume_message(intent),
                timeout=0.1,
            )
            await supervisor._consume_message(superseded_match)
            await supervisor._consume_message(latest_match)
            release_preparation.set()
            await asyncio.wait_for(second_preparation_done.wait(), timeout=0.5)
        finally:
            preparation_task.cancel()
            await asyncio.gather(preparation_task, return_exceptions=True)

    asyncio.run(run())

    assert isinstance(sink.events[1], OrderBookPairUpdated)
    assert prepared == [first_match.event, latest_match.event]


def test_process_stop_wait_is_cancellation_safe() -> None:
    """Cancel process-stop polling without leaving a blocked helper thread."""

    async def run() -> None:
        stop = SimpleNamespace(is_set=lambda: False)
        task = asyncio.create_task(_wait_for_process_stop(stop))
        await asyncio.sleep(0)
        task.cancel()
        await asyncio.wait_for(
            asyncio.gather(task, return_exceptions=True),
            timeout=0.5,
        )

    asyncio.run(run())


def test_supervisor_starts_and_stops_a_real_spawned_child() -> None:
    cycle, _, _, _ = _pair()
    partition = MarketWorkerPartition(
        "lifecycle-test",
        (cycle,),
        (POLYMARKET_VENUE_ID, LIMITLESS_VENUE_ID),
    )
    supervisor = MarketWorkerSupervisor(
        SimpleNamespace(sink=_Sink()),
        min_net_edge=Decimal("0"),
        cost_buffer=Decimal("0"),
        mode=MarketWorkerMode.SHADOW,
        partitions=(partition,),
        worker_target=_idle_worker,
    )

    async def run() -> tuple[object, ...]:
        await supervisor.start()
        processes = tuple(supervisor._processes.values())
        assert processes and all(process.is_alive() for process in processes)
        await supervisor.stop()
        return processes

    processes = asyncio.run(run())

    assert all(not process.is_alive() for process in processes)


def test_real_spawned_child_failure_trips_supervisor_safety() -> None:
    """Surface a child crash through the fail-closed parent signal."""
    cycle, _, _, _ = _pair()
    partition = MarketWorkerPartition(
        "failure-test",
        (cycle,),
        (POLYMARKET_VENUE_ID, LIMITLESS_VENUE_ID),
    )
    supervisor = MarketWorkerSupervisor(
        SimpleNamespace(sink=_Sink()),
        min_net_edge=Decimal("0"),
        cost_buffer=Decimal("0"),
        mode=MarketWorkerMode.SHADOW,
        partitions=(partition,),
        worker_target=_failing_worker,
    )

    async def run() -> tuple[object, ...]:
        try:
            await supervisor.start()
            processes = tuple(supervisor._processes.values())
            await asyncio.wait_for(supervisor.wait_until_failed(), timeout=15)
            assert supervisor.error is not None
            return processes
        finally:
            await supervisor.stop()

    processes = asyncio.run(run())

    assert all(not process.is_alive() for process in processes)


def test_stale_startup_heartbeat_restarts_a_real_spawned_child() -> None:
    """Replace a live PID that never emits its first runtime summary."""
    cycle, _, _, _ = _pair()
    partition = MarketWorkerPartition(
        "heartbeat-test",
        (cycle,),
        (POLYMARKET_VENUE_ID, LIMITLESS_VENUE_ID),
    )
    supervisor = MarketWorkerSupervisor(
        SimpleNamespace(sink=_Sink()),
        min_net_edge=Decimal("0"),
        cost_buffer=Decimal("0"),
        mode=MarketWorkerMode.SHADOW,
        partitions=(partition,),
        worker_target=_idle_worker,
        heartbeat_timeout_seconds=0.5,
        startup_timeout_seconds=0.5,
    )

    async def run() -> tuple[object, ...]:
        await supervisor.start()
        original = supervisor._processes[partition.name]
        await asyncio.wait_for(supervisor.wait_until_failed(), timeout=5)
        deadline = asyncio.get_running_loop().time() + 5
        while supervisor._generation_numbers[partition.name] < 2:
            if asyncio.get_running_loop().time() >= deadline:
                raise TimeoutError("Worker generation did not restart")
            await asyncio.sleep(0.05)
        restarted = supervisor._processes[partition.name]
        assert supervisor.error is not None
        assert "missed its heartbeat" in str(supervisor.error)
        assert restarted is not original
        await supervisor.stop()
        return original, restarted

    processes = asyncio.run(run())

    assert all(not process.is_alive() for process in processes)
