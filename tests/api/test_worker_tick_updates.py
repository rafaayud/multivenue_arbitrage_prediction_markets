"""Verify live Polymarket ticks reach parent signing across ordered worker IPC."""

import asyncio
import pickle
import queue
from dataclasses import replace
from decimal import Decimal
from types import SimpleNamespace

import pytest

from prediction_markets.api.runtime.facade import ArbitrageRuntime
from prediction_markets.api.runtime.feeds import _MarketFeedCoordinator
from prediction_markets.api.runtime.market_workers import (
    MarketWorkerMode, MarketWorkerPartition, MarketWorkerSupervisor,
    WorkerEvent, WorkerTickSizeChange, _WorkerJournal, _WorkerTelemetry,
)
from prediction_markets.application.events import MarketMatchesUpdated
from prediction_markets.application.markets.models import MarketCycle, monitored_market_key
from prediction_markets.application.state import TradingState
from prediction_markets.domain.contracts.value_objects import TickSize
from prediction_markets.domain.market_matching.value_objects import MatchedContractPair, Underlying
from prediction_markets.domain.shared.value_objects import ContractID, Price, Timestamp, VenueID
from prediction_markets.domain.trading.enums import OrderSide
from prediction_markets.infrastructure.venues.polymarket.execution import PolymarketExecutionAdapter
from tests.api.test_market_workers import _contract
from tests.api.test_regular_market_runtime import _Fees, _Pipeline, _Stream
from tests.infrastructure.venues.polymarket.test_execution import _Client, _intent


def _matches() -> MarketMatchesUpdated:
    """Reproduce the hourly ETH route with a coarse initial discovery tick."""
    left = _contract("polymarket:condition-1:token-1", VenueID("POLYMARKET"), "yes")
    right = _contract("predict:42:no", VenueID("PREDICT"), "no")
    return MarketMatchesUpdated(
        MarketCycle(Underlying("ETH"), 3600),
        (MatchedContractPair(left, right, Timestamp.from_iso("2099-01-01T00:00:00Z")),),
    )


def _feed(event, *, pipeline=None, callback=None, cycles=None):
    venues = (VenueID("POLYMARKET"), VenueID("PREDICT"))
    return _MarketFeedCoordinator(
        SimpleNamespace(), {venue: _Stream() for venue in venues},
        {venue: _Fees() for venue in venues}, pipeline or _Pipeline(),
        TradingState(matches={event.cycle: event.pairs}),
        refresh_seconds=15, cycles=cycles,
        on_tick_size_change=callback,
    )


@pytest.mark.parametrize("attach_before_tick", (False, True))
def test_worker_tick_updates_both_parent_caches_before_signing(attach_before_tick):
    """Sign SELL 0.992 after IPC, including a tick received before execution attach."""
    event = _matches()
    contract = event.pairs[0].left
    output = queue.Queue(maxsize=8)
    journal = _WorkerJournal("crypto-slow", output, TradingState(), "generation")

    class Sink:
        async def publish(self, value):
            journal.append(value)

    worker = _feed(event, pipeline=SimpleNamespace(sink=Sink()), callback=journal.publish_tick_size)
    parent = _feed(event, cycles=())
    client = _Client()
    adapter = PolymarketExecutionAdapter(client=client)
    partition = MarketWorkerPartition(
        "crypto-slow", (event.cycle,), (contract.venue_id, event.pairs[0].right.venue_id),
    )
    runtime = SimpleNamespace(_feed=parent)
    supervisor = MarketWorkerSupervisor(
        _Pipeline(), min_net_edge=Decimal("0"), cost_buffer=Decimal("0"),
        mode=MarketWorkerMode.ACTIVE, partitions=(partition,),
        on_tick_size_change=lambda *args: ArbitrageRuntime._apply_worker_tick_size(runtime, *args),
    )
    supervisor._process_generations[partition.name] = "generation"

    async def attach():
        await parent.configure_execution({contract.venue_id: adapter}, {})
        # Reproduce stale SDK market metadata overwriting a stream tick in preload.
        original = client.get_clob_market_info

        def stale_market_info(condition):
            result = original(condition)
            client._ClobClient__tick_sizes["token-1"] = "0.01"
            return result

        client.get_clob_market_info = stale_market_info
        await parent.prepare_matches(event)

    async def run():
        journal.append(event)
        await supervisor._consume_message(output.get_nowait())
        if attach_before_tick:
            await attach()
        await worker._handle_tick_size_change(contract.venue_id, contract.id, TickSize(Decimal("0.001")))
        tick = pickle.loads(pickle.dumps(output.get_nowait()))
        matches = output.get_nowait()
        assert isinstance(tick, WorkerTickSizeChange)
        assert isinstance(matches, WorkerEvent)
        assert tick.sequence < matches.sequence
        await supervisor._consume_message(tick)
        await supervisor._consume_message(matches)
        if not attach_before_tick:
            await attach()
        assert matches.event.pairs[0].left.tick_size.value == Decimal("0.001")

    asyncio.run(run())
    assert adapter._tick_size_by_token["token-1"] == Decimal("0.001")
    assert client._ClobClient__tick_sizes["token-1"] == "0.001"

    def no_http(*_args, **_kwargs):
        raise AssertionError("Metadata HTTP entered preparation")

    for name in ("get_clob_market_info", "get_tick_size", "get_neg_risk", "get_order_book"):
        setattr(client, name, no_http)
    adapter.prepare(_intent(side=OrderSide.SELL, limit_price=Price(Decimal("0.992"))))
    assert client.create_options[-1].tick_size == "0.001"
    assert client.created[-1].price == 0.992
    assert client.posted == []


def test_discovery_cannot_overwrite_live_ticks_but_explicit_increase_can():
    """Preserve a live tick across stale recurring refreshes without keeping the minimum forever."""
    event = _matches()
    contract = event.pairs[0].left
    feed = _feed(event)

    async def run():
        await feed._handle_tick_size_change(contract.venue_id, contract.id, TickSize(Decimal("0.001")))
        refreshed = asyncio.Event()
        observed = []

        async def match_cycles(_cycles):
            return tuple(event if cycle == event.cycle else MarketMatchesUpdated(cycle, ())
                         for cycle in _cycles)

        async def on_matches(value):
            if value.cycle == event.cycle:
                observed.append(value)
                refreshed.set()

        feed._matcher = SimpleNamespace(match_cycles=match_cycles)
        feed._on_cycle_matches = on_matches
        task = asyncio.create_task(feed._discovery_loop())
        try:
            await asyncio.wait_for(refreshed.wait(), 1)
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        assert observed[0].pairs[0].left.tick_size.value == Decimal("0.001")
        await feed._handle_tick_size_change(contract.venue_id, contract.id, TickSize(Decimal("0.01")))
        assert feed._matches[monitored_market_key(event.cycle)].pairs[0].left.tick_size.value == Decimal("0.01")

    asyncio.run(run())


def test_late_preload_result_cannot_replace_a_concurrent_stream_tick():
    """Restore SDK and adapter metadata after an older background preload completes."""
    event = _matches()
    contract = event.pairs[0].left
    feed = _feed(event)
    client = _Client()
    adapter = PolymarketExecutionAdapter(client=client)
    adapter.preload((contract.id,))

    async def run():
        await feed.configure_execution({contract.venue_id: adapter}, {})
        loop = asyncio.get_running_loop()

        def stale_preload(_ids):
            changed = asyncio.run_coroutine_threadsafe(feed._handle_tick_size_change(
                contract.venue_id, contract.id, TickSize(Decimal("0.001")),
            ), loop)
            changed.result(timeout=1)
            adapter._tick_size_by_token["token-1"] = Decimal("0.01")
            client._ClobClient__tick_sizes["token-1"] = "0.01"
            return {contract.id: TickSize(Decimal("0.01"))}

        adapter.preload = stale_preload
        await feed._preload_execution(contract.venue_id, (contract.id,))

    asyncio.run(run())
    assert adapter._tick_size_by_token["token-1"] == Decimal("0.001")
    assert client._ClobClient__tick_sizes["token-1"] == "0.001"
    assert feed._pipeline.sink.events[-1].pairs[0].left.tick_size.value == Decimal("0.001")


def test_old_unknown_reordered_and_shadow_ticks_cannot_change_execution():
    """Validate tick ownership and process ordering before invoking the parent callback."""
    event = _matches()
    contract = event.pairs[0].left
    applied = []
    partition = MarketWorkerPartition("crypto-slow", (event.cycle,),
        (contract.venue_id, event.pairs[0].right.venue_id))
    supervisor = MarketWorkerSupervisor(_Pipeline(), min_net_edge=Decimal("0"),
        cost_buffer=Decimal("0"), mode=MarketWorkerMode.ACTIVE, partitions=(partition,),
        on_tick_size_change=lambda *args: applied.append(args))
    supervisor._process_generations[partition.name] = "new"
    tick = WorkerTickSizeChange(partition.name, "new", 2, contract.venue_id,
        contract.id, TickSize(Decimal("0.001")))

    async def run():
        await supervisor._consume_message(WorkerEvent(partition.name, "new", 1, event))
        await supervisor._consume_message(replace(tick, process_generation="old"))
        await supervisor._consume_message(tick)
        await supervisor._consume_message(replace(tick, tick_size=TickSize(Decimal("0.01"))))
        await supervisor._consume_message(replace(tick, sequence=3, contract_id=ContractID("unrelated")))
        await supervisor._consume_message(replace(tick, sequence=4, venue_id=VenueID("LIMITLESS")))
        supervisor.mode = MarketWorkerMode.SHADOW
        await supervisor._consume_message(replace(tick, sequence=5))

    asyncio.run(run())
    assert applied == [(contract.venue_id, contract.id, TickSize(Decimal("0.001")))]


def test_full_tick_queue_is_observable_and_fails_the_feed():
    """A lost tick must stop its worker feed rather than merely reconnect a socket."""
    event = _matches()
    output = queue.Queue(maxsize=1)
    output.put_nowait(object())
    telemetry = _WorkerTelemetry("crypto-slow", "generation")
    journal = _WorkerJournal("crypto-slow", output, TradingState(), "generation", telemetry)
    feed = _feed(event, callback=journal.publish_tick_size)
    contract = event.pairs[0].left

    async def run():
        with pytest.raises(RuntimeError, match="tick-size event queue is full"):
            await feed._handle_tick_size_change(contract.venue_id, contract.id, TickSize(Decimal("0.001")))
        await asyncio.wait_for(feed.wait_until_failed(), 0.1)

    asyncio.run(run())
    assert output.qsize() == 1
    assert feed.error is not None
    assert telemetry._drops
