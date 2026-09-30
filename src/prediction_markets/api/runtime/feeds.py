"""Own market discovery, subscription refresh, and public order-book feeds.

Responsibilities
----------------
- Refresh recurring and explicitly monitored market matches.
- Normalize subscription changes before publishing order books to the pipeline.
- Warm execution metadata outside the order-submission hot path.
"""

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable, Mapping
from contextlib import aclosing
from dataclasses import replace
from functools import partial

from prediction_markets.application.events import MarketMatchesUpdated, OrderBookUpdated
from prediction_markets.application.markets.matching import MarketMatcher
from prediction_markets.application.markets.models import (
    MarketCycle,
    MarketFamily,
    MonitoredMarket,
    RegularCandidate,
    RegularMarketSelection,
    monitored_market_key,
)
from prediction_markets.application.pipeline import TradingPipeline
from prediction_markets.application.state import TradingState
from prediction_markets.domain.contracts.entities import BinaryContract
from prediction_markets.domain.contracts.value_objects import TickSize
from prediction_markets.domain.market_matching.value_objects import Underlying
from prediction_markets.domain.ports.execution import ExecutionPort, OrderUpdatePort
from prediction_markets.domain.ports.market_data_stream import MarketDataStreamPort
from prediction_markets.domain.ports.taker_fees import TakerFeeCalculatorPort
from prediction_markets.domain.orderbook.entities import OrderBook
from prediction_markets.domain.shared.value_objects import ContractID, Timestamp, VenueID
from prediction_markets.infrastructure.operational_metrics import (
    DISCOVERY_REFRESH_DURATION,
    DISCOVERY_REFRESHES,
    MARKET_FEED_ACTIVE_PUMPS,
    MARKET_FEED_RECEIVE_TO_SINK,
    MARKET_FEED_SINK_PUBLISH,
    MARKET_FEED_SOURCE_TO_TRANSPORT_AGE,
    MARKET_FEED_SOURCE_TO_TRANSPORT_DELTA,
    MARKET_FEED_VENUE_AGE,
    MARKET_FEED_VENUE_TIMESTAMP_DELTA,
    MONITORED_CONTRACTS,
    MONITORED_PAIRS,
)

_events = logging.getLogger("prediction_markets.events.runtime")

MONITORED_MARKET_CYCLES = {
    (cycle.underlying, cycle.interval_seconds): cycle
    for cycle in (
        MarketCycle(Underlying("BTC"), 3600, MarketFamily.CRYPTO),
        MarketCycle(Underlying("BTC"), 86400, MarketFamily.CRYPTO),
        MarketCycle(Underlying("ETH"), 3600, MarketFamily.CRYPTO),
        MarketCycle(Underlying("ETH"), 86400, MarketFamily.CRYPTO),
        MarketCycle(Underlying("BNB"), 3600, MarketFamily.CRYPTO),
        MarketCycle(Underlying("BNB"), 86400, MarketFamily.CRYPTO),
    )
}


class _MarketFeedCoordinator:
    """Refresh matches and keep one public stream alive per configured venue.

    Notes
    -----
    - Venue discovery and subscription refreshes are time-bounded so stalled
      external I/O is cancelled and retried by the next refresh iteration.
    - Journal recovery cannot restore recurring cycles removed from
      ``MONITORED_MARKET_CYCLES``.
    - Cycle callbacks finish before subscriptions expose newly matched contracts.
    """

    def __init__(
        self,
        matcher: MarketMatcher,
        streams: dict[VenueID, MarketDataStreamPort],
        fees: dict[VenueID, TakerFeeCalculatorPort],
        pipeline: TradingPipeline,
        state: TradingState,
        *,
        refresh_seconds: float,
        refresh_timeout_seconds: float = 45.0,
        cycles: tuple[MarketCycle, ...] | None = None,
        on_cycle_matches: Callable[[MarketMatchesUpdated], Awaitable[None]]
        | None = None,
        on_book_published: Callable[
            [VenueID, OrderBook, int, int],
            None,
        ]
        | None = None,
        on_tick_size_change: Callable[[VenueID, ContractID, TickSize], None]
        | None = None,
    ) -> None:
        """Configure discovery, feed ownership, and publication callbacks.

        Parameters
        ----------
        matcher
            Discovery and route matcher for monitored markets.
        streams
            Public market-data adapters keyed by venue.
        fees
            Fee calculators warmed when matches change.
        pipeline
            Event pipeline that receives normalized books and match updates.
        state
            In-memory state used to restore currently valid subscriptions.
        refresh_seconds
            Delay between discovery refreshes, in seconds.
        refresh_timeout_seconds
            Maximum discovery refresh duration, in seconds.
        cycles
            Recurring cycles owned by this coordinator. ``None`` selects all
            monitored cycles; an empty tuple intentionally selects none.
        on_cycle_matches
            Optional async callback completed before matches become actionable.
        on_book_published
            Optional non-blocking timing callback invoked after sink publication.
        on_tick_size_change
            Optional non-blocking control callback forwarding authoritative ticks.

        Raises
        ------
        ValueError
            If the refresh timeout is not positive.
        """
        if refresh_timeout_seconds <= 0:
            raise ValueError("refresh_timeout_seconds must be positive")

        self._matcher = matcher
        self._streams = streams
        self._fees = fees
        self._pipeline = pipeline
        self._state = state
        self._cycles = (
            tuple(MONITORED_MARKET_CYCLES.values())
            if cycles is None
            else cycles
        )
        self._refresh_seconds = refresh_seconds
        self._refresh_timeout_seconds = refresh_timeout_seconds
        self._on_cycle_matches = on_cycle_matches
        self._on_book_published = on_book_published
        self._on_tick_size_change = on_tick_size_change
        self._live_tick_sizes: dict[tuple[VenueID, ContractID], TickSize] = {}
        now = Timestamp.now()
        self._matches = {
            monitored_market_key(market): MarketMatchesUpdated(market, pairs)
            for market, pairs in state.matches.items()
            if pairs
            and (
                isinstance(market, RegularCandidate)
                or market in self._cycles
            )
            and (
                isinstance(market, RegularCandidate)
                or any(pair.ends_at > now for pair in pairs)
            )
        }
        self._desired: dict[VenueID, tuple[ContractID, ...]] = {
            venue_id: () for venue_id in streams
        }
        self._execution: dict[VenueID, ExecutionPort] = {}
        self._updates: dict[VenueID, OrderUpdatePort] = {}
        self._preload_lock = asyncio.Lock()
        self._refresh_lock = asyncio.Lock()
        self._changed = {venue_id: asyncio.Event() for venue_id in streams}
        self._tasks: tuple[asyncio.Task[None], ...] = ()
        self._failed = asyncio.Event()
        self.error: BaseException | None = None
        for venue_id, stream in self._streams.items():
            set_tick_size_handler = getattr(stream, "set_tick_size_handler", None)
            if set_tick_size_handler is not None:
                set_tick_size_handler(
                    partial(self._handle_tick_size_change, venue_id),
                )

    @property
    def running(self) -> bool:
        return bool(self._tasks) and all(not task.done() for task in self._tasks)

    @property
    def supported_venues(self) -> frozenset[VenueID]:
        """Return venues backed by both discovery and market-data adapters."""
        return frozenset(self._streams)

    @property
    def regular_markets(self) -> tuple[dict[str, object], ...]:
        """Return the regular candidates currently subscribed to live feeds."""
        return tuple(
            {
                "monitor_key": key,
                "pair_count": len(event.pairs),
                "markets": tuple(
                    {
                        "venue_id": str(market.venue_id),
                        "title": market.title,
                    }
                    for market in event.cycle.markets
                ),
                "pairs": tuple(
                    {
                        side: {
                            "id": str(contract.id),
                            "market_id": str(contract.market_id),
                            "outcome_id": str(contract.outcome_id),
                            "venue_id": str(contract.venue_id),
                            "symbol": contract.symbol,
                        }
                        for side, contract in (
                            ("left", pair.left),
                            ("right", pair.right),
                        )
                    }
                    for pair in event.pairs
                ),
            }
            for key, event in self._matches.items()
            if isinstance(event.cycle, RegularCandidate)
        )

    def monitored_market(self, monitor_key: str) -> MonitoredMarket | None:
        """Return the cycle or regular candidate identified by a stable key."""
        event = self._matches.get(monitor_key)
        if event is not None:
            return event.cycle
        return next(
            (
                cycle
                for cycle in self._cycles
                if monitored_market_key(cycle) == monitor_key
            ),
            None,
        )

    async def configure_execution(
        self,
        execution: dict[VenueID, ExecutionPort],
        updates: dict[VenueID, OrderUpdatePort],
    ) -> None:
        """
        Attach live execution adapters and warm already monitored contracts.

        Parameters
        ----------
        execution
            Venue adapters created after live trading is enabled.
        updates
            Private order-update adapters to connect and subscribe before an
            order becomes actionable.
        """
        self._execution = dict(execution)
        self._updates = dict(updates)
        for (venue_id, contract_id), tick_size in self._live_tick_sizes.items():
            self.apply_tick_size(venue_id, contract_id, tick_size)
        for venue_id, contract_ids in self._desired.items():
            await self._preload_execution(venue_id, contract_ids)

    async def prepare_matches(self, event: MarketMatchesUpdated) -> None:
        """Warm parent fee and execution adapters for externally owned matches.

        Parameters
        ----------
        event
            Worker-owned pairs that may become executable in the parent.
        """
        contracts: dict[VenueID, dict[ContractID, None]] = {}
        for pair in event.pairs:
            contracts.setdefault(pair.left.venue_id, {})[pair.left.id] = None
            contracts.setdefault(pair.right.venue_id, {})[pair.right.id] = None
        await asyncio.gather(
            *(
                self._prepare_contracts(venue_id, tuple(contract_ids))
                for venue_id, contract_ids in contracts.items()
            ),
        )

    async def start(self) -> None:
        """Start discovery and one dynamically resubscribed feed per venue."""
        if self.running:
            return
        if self._tasks:
            await self.stop()
        self.error = None
        self._failed.clear()
        self._tasks = (
            asyncio.create_task(self._discovery_loop(), name="market-discovery"),
            *(
                asyncio.create_task(
                    self._feed_loop(venue_id, stream),
                    name=f"market-feed:{venue_id}",
                )
                for venue_id, stream in self._streams.items()
            ),
        )
        for task in self._tasks:
            task.add_done_callback(self._task_done)

    async def stop(self) -> None:
        """Cancel discovery and public streams cooperatively."""
        for task in self._tasks:
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks = ()

    async def wait_until_failed(self) -> None:
        """Wait until a long-lived discovery or feed task exits unexpectedly."""
        await self._failed.wait()

    async def monitor_regular(
        self,
        selections: tuple[RegularMarketSelection, ...],
    ) -> MarketMatchesUpdated:
        """Resolve and subscribe one explicit cross-venue market candidate.

        Parameters
        ----------
        selections
            Native venue market identifiers selected by a user or provider.

        Returns
        -------
        MarketMatchesUpdated
            Resolved candidate and complementary contract pairs now monitored.

        Notes
        -----
        - Native discovery and subscription changes run on this control path,
          outside order-book processing and order submission.
        """
        candidate, pairs = await self._matcher.resolve_regular(selections)
        event = self._with_tick_sizes(MarketMatchesUpdated(candidate, pairs))
        async with self._refresh_lock:
            key = monitored_market_key(candidate)
            current = self._matches.get(key)
            if current is None or current.pairs != event.pairs:
                await self._pipeline.sink.publish(event)
            self._matches[key] = event
            await self._refresh_subscriptions()
        return event

    async def unmonitor_regular(self, monitor_key: str) -> bool:
        """Stop one regular candidate and remove its live subscriptions.

        Notes
        -----
        - Once the empty match event is published, removal is authoritative.
          Subscription refresh failures are retained for the background retry
          without turning a completed deletion into an HTTP error.
        """
        async with self._refresh_lock:
            event = self._matches.get(monitor_key)
            if event is None or not isinstance(event.cycle, RegularCandidate):
                return False
            if self._state.has_active_execution(event.cycle):
                raise ValueError("Cannot stop monitoring an active execution")
            await self._pipeline.sink.publish(MarketMatchesUpdated(event.cycle, ()))
            self._matches.pop(monitor_key)
            try:
                await self._refresh_subscriptions()
            except Exception as error:
                self.error = error
                _events.warning(
                    "Market subscription refresh after removing %s failed: %s",
                    monitor_key,
                    error,
                )
            else:
                self.error = None
        return True

    async def _discovery_loop(self) -> None:
        while True:
            loop = asyncio.get_running_loop()
            started_at = loop.time()
            try:
                results = await self._match_cycles(self._cycles)
            except Exception as error:
                results = tuple(error for _ in self._cycles)
            failed = any(isinstance(result, BaseException) for result in results)
            DISCOVERY_REFRESH_DURATION.observe(loop.time() - started_at)
            DISCOVERY_REFRESHES.labels(
                "partial_failure" if failed else "success",
            ).inc()
            async with self._refresh_lock:
                for cycle, result in zip(self._cycles, results, strict=True):
                    if isinstance(result, BaseException):
                        self.error = result
                        _events.warning(
                            "Market discovery failed for %s: %s",
                            cycle,
                            result,
                        )
                        continue
                    result = self._with_tick_sizes(result)
                    key = monitored_market_key(cycle)
                    current = self._matches.get(key)
                    if current is None or current.pairs != result.pairs:
                        await self._pipeline.sink.publish(result)
                    self._matches[key] = result
                    if self._on_cycle_matches is not None:
                        await self._on_cycle_matches(result)
                await self._refresh_regular_matches()
                try:
                    await asyncio.wait_for(
                        self._refresh_subscriptions(),
                        timeout=self._refresh_timeout_seconds,
                    )
                except Exception as error:
                    self.error = error
                    _events.warning("Market subscription refresh failed: %s", error)
            await asyncio.sleep(self._refresh_seconds)

    async def _refresh_regular_matches(self) -> None:
        """Re-resolve explicit regular candidates so pair deadlines stay current."""
        for key, event in tuple(self._matches.items()):
            if not isinstance(event.cycle, RegularCandidate):
                continue
            selections = tuple(
                RegularMarketSelection(market.venue_id, str(market.id))
                for market in event.cycle.markets
            )
            try:
                candidate, pairs = await self._matcher.resolve_regular(selections)
            except Exception as error:
                _events.warning(
                    "Regular market refresh failed for %s: %s",
                    key,
                    error,
                )
                continue
            refreshed = self._with_tick_sizes(MarketMatchesUpdated(candidate, pairs))
            current = self._matches.get(key)
            if current is None or current.pairs != refreshed.pairs:
                await self._pipeline.sink.publish(refreshed)
            self._matches[key] = refreshed

    async def _match_cycles(
        self,
        cycles: tuple[MarketCycle, ...],
    ) -> tuple[MarketMatchesUpdated, ...]:
        """Bound one batched discovery attempt for all recurring cycles.

        Parameters
        ----------
        cycles
            Recurring market windows to discover and match.

        Returns
        -------
        tuple[MarketMatchesUpdated, ...]
            Current complementary pairs in cycle order.

        Raises
        ------
        TimeoutError
            If venue discovery does not finish before the refresh timeout.
        """
        return await asyncio.wait_for(
            self._matcher.match_cycles(cycles),
            timeout=self._refresh_timeout_seconds,
        )

    async def _refresh_subscriptions(self) -> None:
        contracts: dict[VenueID, dict[ContractID, BinaryContract]] = {
            venue_id: {} for venue_id in self._streams
        }
        for event in self._matches.values():
            for pair in event.pairs:
                contracts[pair.left.venue_id][pair.left.id] = pair.left
                contracts[pair.right.venue_id][pair.right.id] = pair.right
        for venue_id, values in contracts.items():
            contract_values = tuple(values.values())
            contract_ids = tuple(values)
            MONITORED_CONTRACTS.labels(str(venue_id)).set(len(contract_ids))
            if contract_ids == self._desired[venue_id]:
                continue
            if contract_ids:
                await self._prepare_contracts(venue_id, contract_ids)
            register = getattr(self._streams[venue_id], "add_contracts", None)
            if register is not None:
                register(contract_values)
            self._desired[venue_id] = contract_ids
            self._changed[venue_id].set()
        MONITORED_PAIRS.set(sum(len(event.pairs) for event in self._matches.values()))

    async def _prepare_contracts(
        self,
        venue_id: VenueID,
        contract_ids: tuple[ContractID, ...],
    ) -> None:
        """Warm fee and execution metadata before books become actionable."""
        await asyncio.gather(
            self._fees[venue_id].prepare(contract_ids),
            self._preload_execution(venue_id, contract_ids),
        )

    async def _preload_execution(
        self,
        venue_id: VenueID,
        contract_ids: tuple[ContractID, ...],
    ) -> None:
        """Warm execution metadata and private updates outside order submission.

        Notes
        -----
        - Authoritative ticks are republished with matched contracts before they
          become actionable.
        """
        adapter = self._execution.get(venue_id)
        updates = self._updates.get(venue_id)
        if (adapter is None and updates is None) or not contract_ids:
            return
        async with self._preload_lock:
            tick_sizes = None
            if adapter is not None:
                try:
                    tick_sizes = await asyncio.to_thread(
                        adapter.preload,
                        contract_ids,
                    )
                except Exception as error:
                    _events.warning(
                        "Execution metadata preload failed for %s: %s",
                        venue_id,
                        error,
                    )
            # A stream update may arrive while metadata preload is awaiting I/O.
            # Reapply it to both adapter and SDK caches before using that result.
            for contract_id in contract_ids:
                live_tick = self._live_tick_sizes.get((venue_id, contract_id))
                if live_tick is not None:
                    self.apply_tick_size(venue_id, contract_id, live_tick)
            if tick_sizes:
                await self._publish_tick_sizes(tick_sizes)
            watch = getattr(updates, "watch", None)
            if watch is not None:
                for contract_id in contract_ids:
                    try:
                        await watch(contract_id)
                    except Exception as error:
                        _events.warning(
                            "Private order updates preload failed for %s: %s",
                            venue_id,
                            error,
                        )

    def apply_tick_size(
        self,
        venue_id: VenueID,
        contract_id: ContractID,
        tick_size: TickSize,
    ) -> None:
        """Retain a live tick and update execution caches without venue I/O.

        Notes
        -----
        - Explicit stream changes may increase or decrease the tick. Discovery
          snapshots cannot override them for the same contract.
        - Values received before execution initialization are applied on attach.
        """
        self._live_tick_sizes[venue_id, contract_id] = tick_size
        adapter = self._execution.get(venue_id)
        update_tick_size = getattr(adapter, "update_tick_size", None)
        if update_tick_size is not None:
            update_tick_size(contract_id, tick_size)

    async def _handle_tick_size_change(
        self,
        venue_id: VenueID,
        contract_id: ContractID,
        tick_size: TickSize,
    ) -> None:
        """Synchronize one live venue tick before processing later book events.

        Parameters
        ----------
        venue_id
            Venue that emitted the metadata update.
        contract_id
            Outcome contract affected by the update.
        tick_size
            New authoritative price increment.

        Notes
        -----
        - Execution metadata is changed locally; no HTTP enters order preparation.
        - Publishing refreshed matches keeps detection rounding consistent with
          the execution adapter.
        """
        self.apply_tick_size(venue_id, contract_id, tick_size)
        if self._on_tick_size_change is not None:
            try:
                self._on_tick_size_change(venue_id, contract_id, tick_size)
            except Exception as error:
                # Socket reconnect alone cannot repair a lost metadata message.
                self.error = error
                self._failed.set()
                raise
        await self._publish_tick_sizes({contract_id: tick_size})

    def _with_tick_sizes(
        self,
        event: MarketMatchesUpdated,
        tick_sizes: Mapping[ContractID, TickSize] | None = None,
    ) -> MarketMatchesUpdated:
        """Overlay live stream ticks on discovery or preload metadata."""
        fallback = tick_sizes or {}

        def updated(contract: BinaryContract) -> BinaryContract:
            tick = self._live_tick_sizes.get(
                (contract.venue_id, contract.id), fallback.get(contract.id),
            )
            return replace(contract, tick_size=tick) if tick is not None else contract

        pairs = tuple(replace(pair, left=updated(pair.left), right=updated(pair.right))
                      for pair in event.pairs)
        return replace(event, pairs=pairs) if pairs != event.pairs else event

    async def _publish_tick_sizes(
        self,
        tick_sizes: Mapping[ContractID, TickSize],
    ) -> None:
        """Publish authoritative venue ticks into every affected matched pair."""
        for key, event in tuple(self._matches.items()):
            updated = self._with_tick_sizes(event, tick_sizes)
            if updated != event:
                self._matches[key] = updated
                await self._pipeline.sink.publish(updated)

    async def _feed_loop(
        self,
        venue_id: VenueID,
        stream: MarketDataStreamPort,
    ) -> None:
        changed = self._changed[venue_id]
        while True:
            contract_ids = self._desired[venue_id]
            if not contract_ids:
                await changed.wait()
                changed.clear()
                continue
            changed.clear()
            pump = asyncio.create_task(
                self._pump(venue_id, stream, contract_ids),
                name=f"market-pump:{venue_id}",
            )
            resubscribe = asyncio.create_task(changed.wait())
            try:
                done, _ = await asyncio.wait(
                    (pump, resubscribe),
                    return_when=asyncio.FIRST_COMPLETED,
                )
            finally:
                for task in (pump, resubscribe):
                    if not task.done():
                        task.cancel()
                await asyncio.gather(pump, resubscribe, return_exceptions=True)
            if pump in done:
                try:
                    pump.result()
                except asyncio.CancelledError:
                    raise
                except Exception as error:
                    self.error = error
                    _events.warning("Market feed failed for %s: %s", venue_id, error)
                    await asyncio.sleep(1)

    async def _pump(
        self,
        venue_id: VenueID,
        stream: MarketDataStreamPort,
        contract_ids: tuple[ContractID, ...],
    ) -> None:
        """Publish one venue stream while measuring its local sink handoff.

        Parameters
        ----------
        venue_id
            Venue owning the public order-book stream.
        stream
            Adapter yielding normalized books with local receive timestamps.
        contract_ids
            Contracts included in the current subscription generation.

        Notes
        -----
        - These measurements remain active when order submission is disabled.
        - Venue timestamp deltas are signed because venue and local wall clocks
          may not be synchronized.
        """
        active_pumps = MARKET_FEED_ACTIVE_PUMPS.labels(str(venue_id))
        active_pumps.inc()
        try:
            async with aclosing(stream.stream_order_books(contract_ids)) as order_books:
                async for contract_id, order_book in order_books:
                    venue = str(venue_id)
                    if (
                        order_book.source_at_ns is not None
                        and order_book.arrival_wall_at_ns is not None
                    ):
                        source_kind = order_book.source_timestamp_kind or "unknown"
                        source_delta = (
                            order_book.arrival_wall_at_ns - order_book.source_at_ns
                        ) / 1_000_000_000
                        MARKET_FEED_SOURCE_TO_TRANSPORT_DELTA.labels(
                            venue,
                            source_kind,
                        ).set(source_delta)
                        MARKET_FEED_SOURCE_TO_TRANSPORT_AGE.labels(
                            venue,
                            source_kind,
                        ).observe(max(0.0, source_delta))
                    if order_book.timestamp is not None:
                        timestamp_delta = (
                            time.time() - order_book.timestamp.value.timestamp()
                        )
                        MARKET_FEED_VENUE_TIMESTAMP_DELTA.labels(venue).set(
                            timestamp_delta,
                        )
                        MARKET_FEED_VENUE_AGE.labels(venue).observe(
                            max(0.0, timestamp_delta),
                        )
                    publish_started_ns = time.monotonic_ns()
                    try:
                        await self._pipeline.sink.publish(
                            OrderBookUpdated(
                                venue_id,
                                contract_id,
                                order_book,
                            ),
                        )
                    finally:
                        published_at_ns = time.monotonic_ns()
                        MARKET_FEED_SINK_PUBLISH.labels(venue).observe(
                            (published_at_ns - publish_started_ns) / 1_000_000_000,
                        )
                        if order_book.received_at_ns is not None:
                            MARKET_FEED_RECEIVE_TO_SINK.labels(venue).observe(
                                max(0, published_at_ns - order_book.received_at_ns)
                                / 1_000_000_000,
                            )
                        if self._on_book_published is not None:
                            self._on_book_published(
                                venue_id,
                                order_book,
                                publish_started_ns,
                                published_at_ns,
                            )
        finally:
            active_pumps.dec()

    def _task_done(self, task: asyncio.Task[None]) -> None:
        if not task.cancelled() and (error := task.exception()) is not None:
            self.error = error
            self._failed.set()

