"""Compose and expose the event-driven arbitrage runtime.

Responsibilities
----------------
- Compose venue adapters, the application pipeline, journal, and SQL projector.
- Refresh market matches and public feeds without coupling the engine to platforms.
- Enable execution only after preflight and restart reconciliation succeed.
- Build the operational API representation of runtime and trading state.
"""

import asyncio
import logging
import os
from contextlib import AsyncExitStack
from dataclasses import replace
from decimal import Decimal
from pathlib import Path

from prediction_markets.infrastructure.observability.predict_fill_study import (
    capture_status,
    observe_event as observe_fill_study,
    start_study,
    stop_study,
)

from prediction_markets.api.runtime.accounting import RuntimeAccounting
from prediction_markets.api.runtime.feeds import (
    MONITORED_MARKET_CYCLES,
    _MarketFeedCoordinator,
)
from prediction_markets.api.runtime.inventory import (
    ShortInventoryCoordinator,
    _legacy_short_position_repairs,
)
from prediction_markets.api.runtime.market_workers import MarketWorkerSupervisor
from prediction_markets.api.runtime.persistence import RuntimePersistence
from prediction_markets.api.runtime.safety import (
    RuntimeSafety,
    _CollateralRefreshError,
    _DEFAULT_RUNTIME_HEALTH_CHECK_SECONDS,
    _VenueSafetyStop,
    _auth_failure,
)
from prediction_markets.api.trading.run_manager import ExecutionRunManager
from prediction_markets.api.trading.runner import LiveArbitrageConfig, preflight
from prediction_markets.application.engine import TradingEngine
from prediction_markets.application.events import (
    MarketMatchesUpdated,
)
from prediction_markets.application.freshness import source_age_guard_enabled
from prediction_markets.application.markets.matching import MarketMatcher
from prediction_markets.application.markets.models import (
    MonitoredMarket,
    RegularCandidate,
    RegularMarketSelection,
)
from prediction_markets.application.pipeline import RecoveryCoordinator, TradingPipeline
from prediction_markets.application.state import EventDispatcher, TradingState
from prediction_markets.application.venue_health import VenueHealthService
from prediction_markets.domain.contracts.entities import BinaryContract
from prediction_markets.domain.contracts.value_objects import TickSize
from prediction_markets.domain.market_matching.value_objects import Underlying
from prediction_markets.domain.outcome_inventory import InventoryOperationSnapshot
from prediction_markets.domain.ports.execution import ExecutionPort, OrderUpdatePort
from prediction_markets.domain.ports.market_data import MarketDataPort
from prediction_markets.domain.shared.value_objects import (
    ContractID,
    Price,
    Quantity,
    Timestamp,
    VenueID,
)
from prediction_markets.domain.trading.entities import (
    AccountingCorrection,
    ArbitrageExecutionJournal,
    CashMovement,
    Trade,
)
from prediction_markets.domain.trading.enums import ArbitrageExecutionStatus
from prediction_markets.infrastructure.binary_journal import BinaryJournal
from prediction_markets.infrastructure.metrics import observe_arbitrage_orderbooks
from prediction_markets.infrastructure.venues.limitless.catalog import LimitlessMarketCatalog
from prediction_markets.infrastructure.venues.limitless.execution import LimitlessExecutionAdapter
from prediction_markets.infrastructure.venues.limitless.instrument_discovery import LimitlessInstrumentDiscoveryAdapter
from prediction_markets.infrastructure.venues.limitless.key_extraction import LimitlessKeyExtractionAdapter
from prediction_markets.infrastructure.venues.limitless.mappers import LIMITLESS_VENUE_ID
from prediction_markets.infrastructure.venues.limitless.market_data import LimitlessMarketDataAdapter
from prediction_markets.infrastructure.venues.limitless.market_data_stream import LimitlessMarketDataStreamAdapter
from prediction_markets.infrastructure.venues.limitless.order_updates import LimitlessOrderUpdateAdapter
from prediction_markets.infrastructure.venues.limitless.taker_fees import LimitlessTakerFeeCalculator
from prediction_markets.infrastructure.venues.polymarket.execution import PolymarketExecutionAdapter
from prediction_markets.infrastructure.venues.polymarket.instrument_discovery import PolymarketInstrumentDiscoveryAdapter
from prediction_markets.infrastructure.venues.polymarket.key_extraction import PolymarketKeyExtractionAdapter
from prediction_markets.infrastructure.venues.polymarket.mappers import POLYMARKET_VENUE_ID
from prediction_markets.infrastructure.venues.polymarket.market_data import PolymarketMarketDataAdapter
from prediction_markets.infrastructure.venues.polymarket.market_data_stream import PolymarketMarketDataStreamAdapter
from prediction_markets.infrastructure.venues.polymarket.order_updates import PolymarketOrderUpdateAdapter
from prediction_markets.infrastructure.venues.polymarket.taker_fees import PolymarketTakerFeeCalculator
from prediction_markets.infrastructure.venues.polynode.key_extraction import PolynodeKeyExtractionAdapter
from prediction_markets.infrastructure.venues.predict.catalog import PredictMarketCatalog
from prediction_markets.infrastructure.venues.predict.execution import PredictExecutionAdapter
from prediction_markets.infrastructure.venues.predict.instrument_discovery import PredictInstrumentDiscoveryAdapter
from prediction_markets.infrastructure.venues.predict.key_extraction import PredictKeyExtractionAdapter
from prediction_markets.infrastructure.venues.predict.mappers import PREDICT_VENUE_ID
from prediction_markets.infrastructure.venues.predict.market_data import PredictMarketDataAdapter
from prediction_markets.infrastructure.venues.predict.market_data_stream import PredictMarketDataStreamAdapter
from prediction_markets.infrastructure.venues.predict.order_updates import PredictOrderUpdateAdapter
from prediction_markets.infrastructure.venues.predict.taker_fees import PredictTakerFeeCalculator


_LIMITLESS_REDEEM_INTERVAL_SECONDS = 300.0


def _trading_state_status(state: TradingState) -> dict[str, object]:
    """Build the operational API view of replayable trading state.

    Parameters
    ----------
    state
        Current in-memory projection.

    Returns
    -------
    dict[str, object]
        JSON-compatible counters and latest execution timing trace.
    """
    latest_execution_id = next(reversed(state.timings), None)
    return {
        "trading_enabled": state.trading_enabled,
        "safety_halted": state.safety_halted,
        "matched_cycles": len(state.matches),
        "matched_pairs": sum(len(pairs) for pairs in state.matches.values()),
        "books": len(state.books),
        "orders": len(state.orders),
        "active_executions": sum(
            execution.status
            not in {
                ArbitrageExecutionStatus.COMPLETED,
                ArbitrageExecutionStatus.RECOVERED,
                ArbitrageExecutionStatus.REJECTED,
            }
            for execution in state.executions.values()
        ),
        "last_error": state.last_error,
        "last_execution_latency": (
            state.timings[latest_execution_id].snapshot(latest_execution_id)
            if latest_execution_id is not None
            else None
        ),
    }


class ArbitrageRuntime:
    """Compose runtime collaborators and preserve the public API facade."""

    def __init__(
        self,
        *,
        enabled: bool | None = None,
        venue_health_service: VenueHealthService | None = None,
    ) -> None:
        """Initialize runtime ownership and the optional active-run health monitor.

        Parameters
        ----------
        enabled
            Whether signal monitoring starts automatically after composition.
        venue_health_service
            Shared lifecycle-owned health service used to stop live trading
            when an active venue becomes unavailable.
        """
        self._enabled = (
            os.getenv("MARKET_WORKER_ENABLED", "1") != "0"
            if enabled is None
            else enabled
        )
        self._venue_health_service = venue_health_service
        self._source_age_guard_enabled = source_age_guard_enabled()
        self._stack: AsyncExitStack | None = None
        self._feed: _MarketFeedCoordinator | None = None
        self._market_workers: MarketWorkerSupervisor | None = None
        self._persistence: RuntimePersistence | None = None
        self._safety: RuntimeSafety | None = None
        self._inventory: ShortInventoryCoordinator | None = None
        self._accounting: RuntimeAccounting | None = None
        self._execution: dict[VenueID, ExecutionPort] = {}
        self._updates: dict[VenueID, OrderUpdatePort] = {}
        self._market_data: dict[VenueID, MarketDataPort] = {}
        self._execution_resources: list[object] = []
        self._recovered = False
        self.journal: BinaryJournal | None = None
        self.state: TradingState | None = None
        self.dispatcher: EventDispatcher | None = None
        self.engine: TradingEngine | None = None
        self.pipeline: TradingPipeline | None = None
        self.market_matcher: MarketMatcher | None = None
        self.execution_runs: ExecutionRunManager | None = None
        self._signal_min_net_edge = Decimal("0")
        self._signal_cost_buffer = Decimal("0")
        self._limitless_redeem_task: asyncio.Task[None] | None = None
        self._limitless_redeem_stop_event: asyncio.Event | None = None
        self._predict_auth_task: asyncio.Task[None] | None = None
        self._predict_auth_stop_event: asyncio.Event | None = None

    async def __aenter__(self) -> "ArbitrageRuntime":
        stack = AsyncExitStack()
        self._stack = stack
        try:
            polymarket_discovery = PolymarketInstrumentDiscoveryAdapter()
            limitless_catalog = LimitlessMarketCatalog(
                cache_seconds=float(
                    os.getenv("LIMITLESS_DISCOVERY_CACHE_SECONDS", "30"),
                ),
            )
            limitless_discovery = LimitlessInstrumentDiscoveryAdapter(
                catalog=limitless_catalog,
            )
            predict_catalog = PredictMarketCatalog(
                cache_seconds=float(
                    os.getenv("PREDICT_DISCOVERY_CACHE_SECONDS", "60"),
                ),
                requests_per_second=float(
                    os.getenv("PREDICT_READ_REQUESTS_PER_SECOND", "3"),
                ),
            )
            predict_discovery = PredictInstrumentDiscoveryAdapter(
                catalog=predict_catalog,
            )
            polynode_keys = PolynodeKeyExtractionAdapter()
            polymarket_keys = PolymarketKeyExtractionAdapter(
                polynode_key_extractor=polynode_keys,
            )
            limitless_keys = LimitlessKeyExtractionAdapter(catalog=limitless_catalog)
            predict_keys = PredictKeyExtractionAdapter(catalog=predict_catalog)
            polymarket_fees = PolymarketTakerFeeCalculator()
            limitless_fees = LimitlessTakerFeeCalculator(
                api_key=os.getenv("LIMITLESS_API_KEY"),
                api_secret=os.getenv("LIMITLESS_API_SECRET"),
            )
            predict_fees = PredictTakerFeeCalculator(catalog=predict_catalog)
            for resource in (
                predict_catalog,
                limitless_catalog,
                polymarket_discovery,
                limitless_discovery,
                predict_discovery,
                polynode_keys,
                polymarket_keys,
                limitless_keys,
                predict_keys,
                polymarket_fees,
                limitless_fees,
                predict_fees,
            ):
                stack.push_async_callback(resource.close)

            discovery = {
                POLYMARKET_VENUE_ID: polymarket_discovery,
                LIMITLESS_VENUE_ID: limitless_discovery,
                PREDICT_VENUE_ID: predict_discovery,
            }
            keys = {
                POLYMARKET_VENUE_ID: polymarket_keys,
                LIMITLESS_VENUE_ID: limitless_keys,
                PREDICT_VENUE_ID: predict_keys,
            }
            fees = {
                POLYMARKET_VENUE_ID: polymarket_fees,
                LIMITLESS_VENUE_ID: limitless_fees,
                PREDICT_VENUE_ID: predict_fees,
            }
            streams = {
                POLYMARKET_VENUE_ID: PolymarketMarketDataStreamAdapter(),
                LIMITLESS_VENUE_ID: LimitlessMarketDataStreamAdapter(),
                PREDICT_VENUE_ID: PredictMarketDataStreamAdapter(),
            }
            self.market_matcher = MarketMatcher(discovery, keys)
            sync_interval = Decimal(
                os.getenv("JOURNAL_SYNC_INTERVAL_MS", "10"),
            ) / Decimal("1000")
            self.journal = BinaryJournal(
                Path(os.getenv("JOURNAL_PATH", "data/trading.log")),
                sync_interval_seconds=float(sync_interval),
                on_append=observe_fill_study,
                segment_size_bytes=(
                    int(os.getenv("JOURNAL_SEGMENT_SIZE_MB", "64"))
                    * 1024
                    * 1024
                ),
            )
            self.journal.start_sync_worker()
            start_study("parent")
            stack.push_async_callback(asyncio.to_thread, stop_study)
            self._persistence = RuntimePersistence(self.journal)
            self.state = TradingState()
            self.dispatcher = EventDispatcher(self.state)
            self.engine = TradingEngine(
                self.dispatcher,
                fees,
                observe_orderbooks=observe_arbitrage_orderbooks,
            )
            self.engine.configure(
                LiveArbitrageConfig(
                    min_net_edge=self._signal_min_net_edge,
                    cost_buffer=self._signal_cost_buffer,
                ).engine_config(),
            )
            self.pipeline = TradingPipeline(
                self.journal,
                self.engine,
                enforce_source_age=self._source_age_guard_enabled,
                on_processing=observe_fill_study,
            )
            self._safety = RuntimeSafety(
                self._venue_health_service,
                self.state,
                self.engine,
                self.pipeline,
                self._execution,
            )
            self._inventory = ShortInventoryCoordinator(
                self.state,
                self.engine,
                self.journal,
                self.dispatcher,
                self._execution_resources,
                self._safety.read_available_collateral,
            )
            self._market_workers = MarketWorkerSupervisor(
                self.pipeline,
                min_net_edge=self._signal_min_net_edge,
                cost_buffer=self._signal_cost_buffer,
                central_engine=self.engine,
                on_cycle_matches=self._prepare_worker_matches,
                on_tick_size_change=self._apply_worker_tick_size,
            )
            self.pipeline.output_dispatcher.set_worker_book_age_observer(
                self._market_workers.observe_parent_book_age,
            )
            self.pipeline.output_dispatcher.set_worker_validation(
                self._market_workers.validate_opportunity,
                capture=self._market_workers.capture_execution,
                recovery_books=self._market_workers.recovery_books,
            )
            self._accounting = RuntimeAccounting(
                self.journal,
                self.state,
                self.dispatcher,
                self.pipeline,
                self._persistence.recovery_records,
                self._execution,
            )
            entries = self._persistence.recovery_records()
            self.pipeline.replay(entries)
            for event in _legacy_short_position_repairs(
                self.state,
                self._inventory.portfolio_id(),
            ):
                self.journal.append(event)
                self.dispatcher.dispatch(event)
            self._feed = _MarketFeedCoordinator(
                self.market_matcher,
                streams,
                fees,
                self.pipeline,
                self.state,
                refresh_seconds=float(os.getenv("MARKET_REFRESH_SECONDS", "15")),
                cycles=(
                    tuple(
                        cycle
                        for cycle in MONITORED_MARKET_CYCLES.values()
                        if cycle not in self._market_workers.cycles
                    )
                    if os.getenv("MARKET_RECURRING_MARKETS_ENABLED", "1") != "0"
                    else ()
                ),
                on_cycle_matches=self._inventory.refresh,
            )
            self.execution_runs = ExecutionRunManager(
                self._run_execution,
                self.preflight,
            )
            await self._persistence.start()
            self._persistence.start_maintenance()
            if self._enabled:
                await self.start()
            return self
        except BaseException:
            await self._close_owned_resources()
            await stack.aclose()
            self._stack = None
            raise

    async def __aexit__(self, *exc_info: object) -> None:
        if self.execution_runs is not None:
            await self.execution_runs.close()
        await self.stop()
        await self._close_owned_resources()
        if self._stack is not None:
            await self._stack.aclose()
        self._stack = None

    async def start(self) -> None:
        """Start feeds, journal consumers, and automatic Limitless redemption."""
        if self.pipeline is None or self._feed is None:
            raise RuntimeError("Arbitrage runtime is not initialized")
        await self.pipeline.start()
        if self._market_workers is not None:
            await self._market_workers.start()
        await self._feed.start()
        self._start_limitless_redemption()
        if self.state is not None and any(
            execution.status
            not in {
                ArbitrageExecutionStatus.COMPLETED,
                ArbitrageExecutionStatus.RECOVERED,
                ArbitrageExecutionStatus.REJECTED,
            }
            for execution in self.state.executions.values()
        ):
            await self._ensure_execution()

    async def stop(self) -> None:
        """Stop feeds, redemption, and command consumers without closing storage."""
        if self.engine is not None:
            self.engine.disable()
        await self._stop_limitless_redemption()
        if self._market_workers is not None:
            await self._market_workers.stop()
        if self._feed is not None:
            await self._feed.stop()
        if self.pipeline is not None:
            await self.pipeline.stop()

    def _start_limitless_redemption(self) -> None:
        """Start the process-local automatic Limitless redemption loop."""
        if self._inventory is None:
            raise RuntimeError("Arbitrage runtime is not initialized")
        if self._limitless_redeem_task is not None:
            if not self._limitless_redeem_task.done():
                return
            self._limitless_redeem_task = None
        stop_event = asyncio.Event()
        self._limitless_redeem_stop_event = stop_event
        self._limitless_redeem_task = asyncio.create_task(
            self._inventory.redeem_limitless_periodically(
                _LIMITLESS_REDEEM_INTERVAL_SECONDS,
                stop_event,
            ),
            name="limitless-redemption",
        )

    async def _stop_limitless_redemption(self) -> None:
        """Finish or stop the automatic Limitless redemption loop."""
        task = self._limitless_redeem_task
        if task is None:
            return
        if self._limitless_redeem_stop_event is not None:
            self._limitless_redeem_stop_event.set()
        await asyncio.gather(task, return_exceptions=True)
        self._limitless_redeem_task = None
        self._limitless_redeem_stop_event = None

    def current_pairs(
        self,
        underlying: Underlying,
        interval_seconds: int,
    ) -> tuple[tuple[BinaryContract, BinaryContract], ...]:
        """Return the in-memory pair snapshot for a monitored cycle."""
        if self.state is None:
            return ()
        cycle = MONITORED_MARKET_CYCLES.get((underlying, interval_seconds))
        return self.state.current_pairs(cycle) if cycle is not None else ()

    async def monitor_regular(
        self,
        selections: tuple[RegularMarketSelection, ...],
    ) -> tuple[RegularCandidate, int]:
        """Add one explicit candidate to the existing discovery and feed runtime."""
        if self._feed is None:
            raise RuntimeError("Arbitrage runtime is not initialized")
        supported = tuple(
            selection
            for selection in selections
            if selection.venue_id in self._feed.supported_venues
        )
        if len({selection.venue_id for selection in supported}) < 2:
            raise ValueError(
                "Candidate requires at least two supported venues: "
                + ", ".join(map(str, sorted(self._feed.supported_venues, key=str))),
            )
        event = await self._feed.monitor_regular(supported)
        candidate = event.cycle
        if not isinstance(candidate, RegularCandidate):
            raise RuntimeError("Regular market resolution returned a cycle")
        return candidate, len(event.pairs)

    async def unmonitor_regular(self, monitor_key: str) -> bool:
        """Remove one regular candidate from detection and market-data feeds."""
        if self._feed is None:
            raise RuntimeError("Arbitrage runtime is not initialized")
        return await self._feed.unmonitor_regular(monitor_key)

    async def complete_execution(
        self,
        execution_id: str,
        *,
        method: str,
        price: Price,
        fee_amount_usd: Decimal,
        executed_at: Timestamp,
        external_reference: str | None = None,
    ) -> tuple[ArbitrageExecutionJournal, Trade]:
        """Record an externally resolved exposure and its accounting trade.

        Parameters
        ----------
        execution_id : str
            Recoverable execution whose residual position was closed externally.
        method : str
            Operator action: ``manual_sale`` or ``settlement``.
        price : Price
            Actual exit price or binary settlement payout per contract.
        fee_amount_usd : Decimal
            Total externally observed fee in USD.
        executed_at : Timestamp
            Economic timestamp of the external action.
        external_reference : str, optional
            Venue transaction, claim, or operator reference.

        Returns
        -------
        tuple[ArbitrageExecutionJournal, Trade]
            Durable completed execution and its inferred closing trade.

        Raises
        ------
        KeyError
            If the execution does not exist.
        ValueError
            If the execution is not waiting for manual review.
        RuntimeError
            If the runtime is unavailable or live trading is enabled.
        """
        if self._accounting is None:
            raise RuntimeError("Arbitrage runtime is not initialized")
        if self.pipeline is not None and self.journal is not None:
            await self._ensure_execution()
            self._accounting._execution = dict(self._execution)
        return await self._accounting.complete_execution(
            execution_id,
            method=method,
            price=price,
            fee_amount_usd=fee_amount_usd,
            executed_at=executed_at,
            external_reference=external_reference,
        )

    async def reconcile_execution(
        self,
        execution_id: str,
    ) -> ArbitrageExecutionJournal:
        """Reconcile submitted fills without recording a manual resolution.

        Parameters
        ----------
        execution_id : str
            Execution whose venue state must be reconciled.

        Returns
        -------
        ArbitrageExecutionJournal
            Current durable execution after reconciliation.

        Raises
        ------
        KeyError
            If the execution does not exist.
        RuntimeError
            If the runtime is unavailable or live trading is enabled.
        """
        if self._accounting is None:
            raise RuntimeError("Arbitrage runtime is not initialized")
        if self.pipeline is not None and self.journal is not None:
            await self._ensure_execution()
            self._accounting._execution = dict(self._execution)
        return await self._accounting.reconcile_execution(execution_id)

    async def reconcile_predict_inventory(
        self,
        operation_id: str,
        transaction_hash: str,
    ) -> InventoryOperationSnapshot:
        """Record the terminal state of a previously broadcast Predict operation.

        Parameters
        ----------
        operation_id
            Pending inventory operation to repair.
        transaction_hash
            Exact BNB Chain transaction hash to verify.

        Returns
        -------
        InventoryOperationSnapshot
            Terminal snapshot appended to the authoritative journal.

        Raises
        ------
        RuntimeError
            If the runtime is unavailable or the operation remains uncertain.
        """
        if self._inventory is None:
            raise RuntimeError("Arbitrage runtime is not initialized")
        return await self._inventory.reconcile_predict_transaction(
            operation_id,
            transaction_hash,
        )

    def monitored_market(self, monitor_key: str) -> MonitoredMarket | None:
        """Resolve a stable signal-stream key to its monitored market."""
        if self._feed is None:
            return None
        market = self._feed.monitored_market(monitor_key)
        if market is not None:
            return market
        return (
            self._market_workers.monitored_market(monitor_key)
            if self._market_workers is not None
            else None
        )

    def stream_signal_pairs(self, market: MonitoredMarket):
        """Stream opportunity signals for one monitored market."""
        if self.dispatcher is None:
            raise RuntimeError("Arbitrage runtime is not initialized")
        return self.dispatcher.stream_signal_pairs(market)

    def record_cash_movement(self, movement: CashMovement) -> None:
        """Delegate an explicit cash movement to durable accounting."""
        if self._accounting is None:
            raise RuntimeError("Arbitrage runtime is not initialized")
        self._accounting.record_cash_movement(movement)

    def record_accounting_correction(
        self,
        replacement: Trade,
        reason: str,
        *,
        correction_id: str | None = None,
    ) -> AccountingCorrection:
        """Delegate a trade correction to durable accounting."""
        if self._accounting is None:
            raise RuntimeError("Arbitrage runtime is not initialized")
        return self._accounting.record_accounting_correction(
            replacement,
            reason,
            correction_id=correction_id,
        )

    def status(self) -> dict[str, object]:
        """Return lifecycle, journal, projection, and engine state."""
        journal = self.journal
        pipeline = self.pipeline
        state = self.state
        persistence = self._persistence
        inventory = self._inventory
        return {
            "running": bool(
                pipeline
                and pipeline.running
                and self._feed
                and self._feed.running
                and (
                    self._market_workers is None
                    or self._market_workers.running
                )
            ),
            "journal_sequence": journal.last_sequence if journal else 0,
            "durable_sequence": journal.durable_sequence if journal else 0,
            "projection_sequence": (
                persistence.projected_sequence if persistence else None
            ),
            "snapshot_sequence": persistence.snapshot_sequence if persistence else 0,
            "retention_safe_sequence": (
                persistence.safe_delete_sequence if persistence else 0
            ),
            "retention_eligible_segments": (
                persistence.eligible_segment_count if persistence else 0
            ),
            "input_buffer_size": pipeline.inputs.size if pipeline else 0,
            "output_buffer_size": pipeline.outputs.size if pipeline else 0,
            "dropped_order_books": (
                pipeline.sink.dropped_order_books if pipeline else 0
            ),
            "error": str(
                (pipeline.error if pipeline else None)
                or (self._feed.error if self._feed else None)
                or (self._market_workers.error if self._market_workers else None)
                or (persistence.error if persistence else None)
                or "",
            ) or None,
            "signal_settings": {
                "min_net_edge": float(self._signal_min_net_edge),
                "cost_buffer": float(self._signal_cost_buffer),
            },
            "source_age_guard_enabled": self._source_age_guard_enabled,
            "short_inventory_preparing": inventory.preparing if inventory else False,
            "prepared_short_market_keys": (
                inventory.prepared_market_keys if inventory else ()
            ),
            "regular_markets": self._feed.regular_markets if self._feed else (),
            "market_worker_mode": (
                self._market_workers.mode.value
                if self._market_workers is not None
                else "disabled"
            ),
            "market_workers": (
                self._market_workers.status() if self._market_workers else ()
            ),
            "predict_fill_capture": capture_status(),
            **(_trading_state_status(state) if state else {}),
        }

    def configure_signals(
        self,
        min_net_edge: Decimal,
        cost_buffer: Decimal,
    ) -> dict[str, object]:
        """Apply fee-aware thresholds to monitoring while trading is disabled.

        Parameters
        ----------
        min_net_edge
            Minimum profit per contract after fees and the cost buffer.
        cost_buffer
            Additional cost allowance deducted per contract.

        Returns
        -------
        dict[str, object]
            Updated runtime status.

        Raises
        ------
        RuntimeError
            If the runtime is unavailable or live trading is enabled.
        """
        if self.engine is None:
            raise RuntimeError("Arbitrage runtime is not initialized")
        if self.state is not None and self.state.trading_enabled:
            raise RuntimeError("Disable live trading before changing signal settings")
        self._signal_min_net_edge = min_net_edge
        self._signal_cost_buffer = cost_buffer
        self.engine.configure(
            LiveArbitrageConfig(
                min_net_edge=min_net_edge,
                cost_buffer=cost_buffer,
            ).engine_config(),
        )
        if self._market_workers is not None:
            self._market_workers.configure(min_net_edge, cost_buffer)
        return self.status()

    def preflight(self) -> dict[str, object]:
        """Combine environment checks with current journal recovery state."""
        report = preflight()
        if self.pipeline is not None and self.pipeline.error is not None:
            report["ready"] = False
        if self._market_workers is not None and self._market_workers.error is not None:
            report["ready"] = False
        if self._persistence is not None and self._persistence.error is not None:
            report["ready"] = False
            report["database_ready"] = False
        if self.state is not None and any(
            value.status.value == "needs_review" for value in self.state.executions.values()
        ):
            report["ready"] = False
            report["active_journals"] = sum(
                value.status.value == "needs_review"
                for value in self.state.executions.values()
            )
        return report

    async def _run_execution(
        self,
        config: LiveArbitrageConfig,
        stop_event: asyncio.Event,
    ) -> None:
        """Reconcile outstanding orders before enabling a new trading run."""
        await self.start()
        await self._ensure_execution()
        assert self.engine is not None
        assert self.pipeline is not None
        if self._inventory is None or self._safety is None:
            raise RuntimeError("Arbitrage runtime is not initialized")
        self._signal_min_net_edge = config.min_net_edge
        self._signal_cost_buffer = config.cost_buffer
        if self._market_workers is not None:
            self._market_workers.configure(config.min_net_edge, config.cost_buffer)
        pair_keys: frozenset[tuple[str, str]] = frozenset()
        short_inventory_by_contract: dict[ContractID, Quantity] = {}
        if config.short_market_keys:
            pair_keys, short_inventory_by_contract = (
                await self._inventory.prepare(
                    config.short_market_keys,
                    minimum_time_remaining_seconds=(
                        config.min_market_time_remaining_seconds
                    ),
                )
            )
            if stop_event.is_set():
                return
            if self.execution_runs is not None:
                self.execution_runs.mark_running()
        else:
            self._inventory.clear_prepared()
        try:
            collateral_by_venue = await self._safety.read_available_collateral()
        except _CollateralRefreshError as error:
            if _auth_failure(error, error.http_status):
                safety_stop = _VenueSafetyStop(error.venue_id, str(error))
                await self._safety.record_safety_stop(safety_stop)
                raise safety_stop from error
            raise
        collateral_refresh_seconds = float(
            os.getenv("COLLATERAL_REFRESH_SECONDS", "30"),
        )
        if collateral_refresh_seconds <= 0:
            raise ValueError("collateral refresh interval must be positive")
        health_check_seconds = None
        if self._venue_health_service is not None:
            health_check_seconds = float(
                os.getenv(
                    "VENUE_HEALTH_RUNTIME_CHECK_SECONDS",
                    str(_DEFAULT_RUNTIME_HEALTH_CHECK_SECONDS),
                ),
            )
            if health_check_seconds <= 0:
                raise ValueError("venue health check interval must be positive")
        if stop_event.is_set():
            return
        if self.state is not None and any(
            execution.status not in {
                ArbitrageExecutionStatus.COMPLETED,
                ArbitrageExecutionStatus.RECOVERED,
                ArbitrageExecutionStatus.REJECTED,
            }
            for execution in self.state.executions.values()
        ):
            raise RuntimeError("Unresolved executions prevent enabling trading after reconciliation")
        self.engine.enable(
            replace(
                config.engine_config(),
                execute_short=bool(pair_keys),
                short_pair_keys=pair_keys,
                short_inventory_by_contract=short_inventory_by_contract,
                collateral_by_venue=collateral_by_venue,
            ),
        )
        if self._market_workers is not None:
            self._market_workers.resume_execution_handoff()
        collateral_refresh = asyncio.create_task(
            self._safety.refresh_collateral_periodically(
                collateral_refresh_seconds,
            ),
            name="collateral-refresh",
        )
        health_monitor = None
        if health_check_seconds is not None:
            health_monitor = asyncio.create_task(
                self._safety.monitor_active_venue_health(health_check_seconds),
                name="venue-health-monitor",
            )
        stop = asyncio.create_task(stop_event.wait())
        completed = asyncio.create_task(self.engine.wait_until_done())
        failed = asyncio.create_task(self.pipeline.wait_until_failed())
        assert self._feed is not None
        feed_failed = asyncio.create_task(self._feed.wait_until_failed())
        waiters = [stop, completed, failed, feed_failed, collateral_refresh]
        workers_failed = None
        if self._market_workers is not None and self._market_workers.enabled:
            workers_failed = asyncio.create_task(
                self._market_workers.wait_until_failed(),
            )
            waiters.append(workers_failed)
        if health_monitor is not None:
            waiters.append(health_monitor)
        done, pending = await asyncio.wait(
            waiters,
            return_when=asyncio.FIRST_COMPLETED,
        )
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)
        safety_stop = None
        background_error = None
        for task in (collateral_refresh, health_monitor):
            if task is None or task not in done or task.cancelled():
                continue
            error = task.exception()
            if isinstance(error, _VenueSafetyStop):
                safety_stop = error
                break
            if error is not None:
                background_error = error
        if safety_stop is not None:
            await self._safety.record_safety_stop(safety_stop)
        self.engine.disable()
        if safety_stop is not None:
            raise safety_stop
        if background_error is not None:
            raise RuntimeError(str(background_error)) from background_error
        if failed in done and self.pipeline.error is not None:
            raise RuntimeError(str(self.pipeline.error))
        if feed_failed in done and self._feed.error is not None:
            raise RuntimeError(str(self._feed.error))
        if (
            workers_failed is not None
            and workers_failed in done
            and self._market_workers is not None
            and self._market_workers.error is not None
        ):
            raise RuntimeError(str(self._market_workers.error))
        if self.state is not None and self.state.last_error:
            raise RuntimeError(self.state.last_error)

    def _apply_worker_tick_size(
        self, venue_id: VenueID, contract_id: ContractID, tick_size: TickSize,
    ) -> None:
        """Synchronize worker stream metadata locally before later IPC intents."""
        if self._feed is None:
            raise RuntimeError("Arbitrage runtime is not initialized")
        self._feed.apply_tick_size(venue_id, contract_id, tick_size)

    async def _prepare_worker_matches(self, event: MarketMatchesUpdated) -> None:
        """Warm authoritative adapters outside worker IPC consumption."""
        if self._feed is None or self._inventory is None:
            raise RuntimeError("Arbitrage runtime is not initialized")
        await self._feed.prepare_matches(event)
        await self._inventory.refresh(event)

    async def _ensure_execution(self) -> None:
        assert self.pipeline is not None
        assert self.journal is not None
        if not self._execution:
            polymarket_execution = await asyncio.to_thread(
                PolymarketExecutionAdapter,
                signature_type=int(os.getenv("POLYMARKET_SIGNATURE_TYPE", "3")),
            )
            limitless_execution = await asyncio.to_thread(
                LimitlessExecutionAdapter,
                api_key=os.getenv("LIMITLESS_API_KEY"),
                api_secret=os.getenv("LIMITLESS_API_SECRET"),
            )
            predict_execution = await asyncio.to_thread(PredictExecutionAdapter)
            try:
                await asyncio.to_thread(
                    predict_execution.refresh_auth,
                    reason="startup",
                )
            except Exception:
                await asyncio.to_thread(predict_execution.close)
                raise
            polymarket_updates = PolymarketOrderUpdateAdapter()
            limitless_updates = LimitlessOrderUpdateAdapter(
                api_key=os.getenv("LIMITLESS_API_KEY"),
            )
            predict_updates = PredictOrderUpdateAdapter(
                predict_execution.auth_token,
                api_key=os.getenv("PREDICT_API_KEY"),
            )
            self._market_data.update({
                POLYMARKET_VENUE_ID: PolymarketMarketDataAdapter(),
                LIMITLESS_VENUE_ID: LimitlessMarketDataAdapter(),
                PREDICT_VENUE_ID: PredictMarketDataAdapter(),
            })
            assert self._stack is not None
            for market_data in self._market_data.values():
                self._stack.push_async_callback(market_data.close)
            self._execution.update({
                POLYMARKET_VENUE_ID: polymarket_execution,
                LIMITLESS_VENUE_ID: limitless_execution,
                PREDICT_VENUE_ID: predict_execution,
            })
            self._updates.update({
                POLYMARKET_VENUE_ID: polymarket_updates,
                LIMITLESS_VENUE_ID: limitless_updates,
                PREDICT_VENUE_ID: predict_updates,
            })
            self._execution_resources.extend([
                polymarket_updates,
                limitless_updates,
                predict_updates,
                limitless_execution,
                predict_execution,
            ])
            self._start_predict_auth_refresh(predict_execution)
            self.pipeline.output_dispatcher.configure(
                self._execution,
                self._updates,
                self._market_data,
            )
            assert self._feed is not None
            await self._feed.configure_execution(self._execution, self._updates)
            if self._market_workers is not None and self.state is not None:
                for cycle in self._market_workers.monitored_cycles:
                    pairs = self.state.matches.get(cycle, ())
                    if pairs:
                        await self._feed.prepare_matches(
                            MarketMatchesUpdated(cycle, pairs),
                        )
        if not self._recovered:
            if self._persistence is None:
                raise RuntimeError("Arbitrage runtime is not initialized")
            await self.pipeline.recover_derived()
            await RecoveryCoordinator(
                self._persistence.recovery_records(),
                self.pipeline.output_dispatcher,
                self._execution,
            ).recover()
            await self.pipeline.drain_inputs()
            self._recovered = True

    async def _close_owned_resources(self) -> None:
        """Close execution resources before persistence-owned consumers."""
        await self._stop_predict_auth_refresh()
        for resource in self._execution_resources:
            close = getattr(resource, "close", None)
            if close is None:
                continue
            if asyncio.iscoroutinefunction(close):
                await close()
            else:
                await asyncio.to_thread(close)
        self._execution_resources.clear()
        if self._persistence is not None:
            await self._persistence.close()
            self._persistence = None

    def _start_predict_auth_refresh(
        self,
        adapter: PredictExecutionAdapter,
    ) -> None:
        """Start preventive Predict JWT refresh outside order dispatch."""
        if self._predict_auth_task is not None and not self._predict_auth_task.done():
            return
        interval_seconds = float(
            os.getenv("PREDICT_AUTH_REFRESH_SECONDS", "10")
        )
        if interval_seconds <= 0:
            raise ValueError("Predict auth refresh interval must be positive")
        stop_event = asyncio.Event()
        self._predict_auth_stop_event = stop_event
        self._predict_auth_task = asyncio.create_task(
            self._refresh_predict_auth_periodically(
                adapter,
                interval_seconds,
                stop_event,
            ),
            name="predict-auth-refresh",
        )

    async def _stop_predict_auth_refresh(self) -> None:
        """Stop the lifecycle-owned Predict authentication refresh task."""
        task = self._predict_auth_task
        if task is None:
            return
        if self._predict_auth_stop_event is not None:
            self._predict_auth_stop_event.set()
        await asyncio.gather(task, return_exceptions=True)
        self._predict_auth_task = None
        self._predict_auth_stop_event = None

    @staticmethod
    async def _refresh_predict_auth_periodically(
        adapter: PredictExecutionAdapter,
        interval_seconds: float,
        stop_event: asyncio.Event,
    ) -> None:
        """Refresh Predict authentication and chain anchors outside submission."""
        while not stop_event.is_set():
            try:
                await asyncio.wait_for(
                    stop_event.wait(),
                    timeout=interval_seconds,
                )
            except TimeoutError:
                try:
                    await asyncio.to_thread(
                        adapter.refresh_auth,
                        reason="proactive",
                    )
                    await asyncio.to_thread(adapter.refresh_cancellation_anchor)
                except Exception as error:
                    logging.getLogger(__name__).warning(
                        "Predict background refresh failed: %s", type(error).__name__,
                    )
