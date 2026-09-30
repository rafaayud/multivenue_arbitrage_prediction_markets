"""Process journaled events through matching, arbitrage, risk, and accounting.

Responsibilities
----------------
- Invoke stateless domain services after state has received an ordered event.
- Produce journalable internal events and venue-neutral order commands.
- Never perform venue, filesystem, or database I/O.
"""

import asyncio
from collections.abc import Callable
from datetime import timedelta
import hashlib
import math
import time
from dataclasses import dataclass, field, fields, replace
from decimal import ROUND_CEILING, ROUND_FLOOR, Decimal
from typing import Mapping
from uuid import uuid4

from prediction_markets.application.events import (
    ApplicationEvent,
    ArbitrageOpportunityFound,
    ArbitragePlanned,
    ExecutionPreparationRequested,
    ExecutionUpdated,
    OrderBookPairUpdated,
    OrderBookUpdated,
    OrderSnapshotUpdated,
    PreparedExecutionBatch,
    RecoveryBooksReceived,
    RecoveryPlanningRequested,
    SubmissionReceived,
    SubmitOrder,
    TradingSafetyStop,
    prepared_execution_events,
)
from prediction_markets.application.execution.accounting import ExecutionAccounting
from prediction_markets.application.execution.lifecycle import (
    _ExecutionLifecycle,
    _decision_snapshot,
    _execution_command,
)
from prediction_markets.application.execution.recovery_lifecycle import RecoveryLifecycle
from prediction_markets.application.markets.models import (
    MarketCycle,
    MonitoredMarket,
    market_expiry_guard_seconds,
    monitored_market_key,
    monitored_market_label,
)
from prediction_markets.application.execution.timings import ExecutionTimings
from prediction_markets.application.state import EventDispatcher
from prediction_markets.domain.arbitrage.services import (
    ArbitragePlanningService,
    LongArbitrageDetectionService,
    ShortArbitrageDetectionService,
)
from prediction_markets.domain.arbitrage.value_objects import ArbitrageOpportunity
from prediction_markets.domain.contracts.entities import BinaryContract
from prediction_markets.domain.market_matching.value_objects import (
    MatchedContractPair,
    RegularCandidate,
)
from prediction_markets.domain.orderbook.entities import OrderBook
from prediction_markets.domain.ports.taker_fees import TakerFeeCalculatorPort
from prediction_markets.domain.shared.value_objects import (
    ClientOrderID,
    ContractID,
    PortfolioID,
    Price,
    Quantity,
    StrategyID,
    Timestamp,
    VenueID,
)
from prediction_markets.domain.trading.entities import OrderSnapshot
from prediction_markets.domain.trading.enums import (
    ArbitrageExecutionStatus,
    OrderSide,
)
from prediction_markets.domain.trading.entities import ArbitrageExecutionJournal
from prediction_markets.domain.trading.portfolio import STRATEGY_PORTFOLIO_ID


_PREDICT_VENUE_ID = VenueID("PREDICT")
_COLLATERAL_SAFETY_BUFFER = Decimal("0.01")
_RECOVERY_QUOTE_WAIT_NS = 10_000_000_000
_RECOVERY_QUOTE_POLL_NS = 20_000_000
_EconomicRoute = tuple[OrderSide, tuple[tuple[str, str, str], ...]]


@dataclass(frozen=True, slots=True)
class EngineConfig:
    """Hold strategy and risk limits for one live trading run.

    Attributes
    ----------
    max_notional_by_venue
        Maximum settlement cost permitted for each venue leg.
    market_buy_notional_steps
        Minimum settlement-amount increment required by each venue for
        immediate-or-cancel buy orders.
    max_arbitrages
        Number of completed or recovered executions allowed in this run.
        Rejected attempts are not counted.
    max_concurrent_arbitrages
        Maximum independent executions admitted at the same time.
    min_notional_per_venue
        Minimum price-times-quantity required for every submitted BUY leg.
    min_net_edge
        Minimum fee-adjusted payout edge per contract.
    large_order_contract_threshold
        Detected quantity at which liquidity headroom starts to apply.
    large_order_liquidity_safety_factor
        Fraction of detected liquidity retained for large orders.
    predict_limit_slippage_ticks
        Predict-only execution-limit headroom in price ticks. Zero preserves
        the detected top-of-book limit.
    predict_use_edge_budget
        Replace fixed Predict ticks with a bounded fee-aware limit search.
        Preserve the baseline quantity, budgets, minimum edge and cost buffer.
    collateral_by_venue
        Latest spendable collateral observed before or during this run. ``None``
        disables local collateral admission for isolated engine users.
    max_skew_ms
        Maximum receive-time difference between the two books used for one
        detection.
    short_market_keys
        Monitored market keys permitted to execute short opportunities when
        short execution is enabled.
    short_pair_keys
        Exact prepared contract pairs permitted to execute short opportunities.
    short_inventory_by_contract
        Confirmed covered quantity available to reserve for each short contract.
    min_market_time_remaining_seconds
        Default minimum lifetime required before planning a recurring-market
        execution. Live 5-minute cycles use 30 seconds, 15-minute cycles use
        60 seconds, and other cycles use this value, which defaults to 120
        seconds. Regular candidates rely on live order-book freshness because
        venue event timestamps are not authoritative trading deadlines.
        ``None`` disables the guard for isolated engine users.
    """

    max_notional_by_venue: Mapping[VenueID, Decimal]
    market_buy_notional_steps: Mapping[VenueID, Decimal] = field(
        default_factory=dict,
    )
    max_arbitrages: int = 1
    max_concurrent_arbitrages: int = 2
    min_notional_per_venue: Decimal = Decimal("1")
    min_net_edge: Decimal = Decimal("0")
    cost_buffer: Decimal = Decimal("0")
    large_order_contract_threshold: int = 10
    large_order_liquidity_safety_factor: Decimal = Decimal("0.7")
    predict_limit_slippage_ticks: int = 2
    predict_use_edge_budget: bool = False
    collateral_by_venue: Mapping[VenueID, Decimal] | None = None
    max_recovery_loss: Decimal | None = None
    max_skew_ms: int = 80
    monitor_long: bool = True
    monitor_short: bool = True
    execute_long: bool = True
    execute_short: bool = False
    short_market_keys: frozenset[str] = frozenset()
    short_pair_keys: frozenset[tuple[str, str]] = frozenset()
    short_inventory_by_contract: Mapping[ContractID, Quantity] = field(
        default_factory=dict,
    )
    allowed_underlyings: tuple[str, ...] = ("BTC", "ETH")
    allowed_intervals_seconds: tuple[int, ...] = (300, 900)
    min_market_time_remaining_seconds: int | None = None
    portfolio_id: PortfolioID = STRATEGY_PORTFOLIO_ID

    def __post_init__(self) -> None:
        if self.max_arbitrages <= 0:
            raise ValueError("max_arbitrages must be positive")
        if self.max_concurrent_arbitrages <= 0:
            raise ValueError("max_concurrent_arbitrages must be positive")
        if self.min_notional_per_venue < 0:
            raise ValueError("min_notional_per_venue must be non-negative")
        if self.min_net_edge < 0 or self.cost_buffer < 0:
            raise ValueError("edge and cost buffer must be non-negative")
        if self.large_order_contract_threshold <= 0:
            raise ValueError("large order contract threshold must be positive")
        if not (
            Decimal("0")
            < self.large_order_liquidity_safety_factor
            <= Decimal("1")
        ):
            raise ValueError("large order liquidity safety factor must be in (0, 1]")
        if self.predict_limit_slippage_ticks < 0:
            raise ValueError("Predict limit slippage ticks must be non-negative")
        if self.collateral_by_venue is not None and any(
            amount < 0 for amount in self.collateral_by_venue.values()
        ):
            raise ValueError("venue collateral balances must be non-negative")
        if (
            self.min_market_time_remaining_seconds is not None
            and self.min_market_time_remaining_seconds < 0
        ):
            raise ValueError("minimum market time remaining must be non-negative")
        if self.max_recovery_loss is not None and self.max_recovery_loss < 0:
            raise ValueError("maximum recovery loss must be non-negative")
        if any(limit <= 0 for limit in self.max_notional_by_venue.values()):
            raise ValueError("venue notional limits must be positive")
        if any(step <= 0 for step in self.market_buy_notional_steps.values()):
            raise ValueError("market buy notional steps must be positive")
        if any(
            quantity.value <= 0
            for quantity in self.short_inventory_by_contract.values()
        ):
            raise ValueError("short inventory quantities must be positive")


class TradingEngine:
    """Turn ordered market and execution events into deterministic outputs.

    Notes
    -----
    - The event loop journals every returned event before processing or dispatching it.
    - Replay applies state only; it never regenerates commands.
    """

    def __init__(
        self,
        dispatcher: EventDispatcher,
        fees_by_venue: Mapping[VenueID, TakerFeeCalculatorPort],
        *,
        observe_orderbooks: Callable[
            [MatchedContractPair, OrderBook, OrderBook],
            None,
        ]
        | None = None,
    ) -> None:
        """
        Parameters
        ----------
        dispatcher
            Ordered state updater and live signal broadcaster.
        fees_by_venue
            Prepared taker-fee calculators used synchronously in the hot path.
        observe_orderbooks
            Optional live observer called once for each complete pair evaluation.
        """
        self.dispatcher = dispatcher
        self.state = dispatcher.state
        self._fees = dict(fees_by_venue)
        self._planning = ArbitragePlanningService()
        self._accounting = ExecutionAccounting(self.state)
        self._execution_lifecycle = _ExecutionLifecycle(
            self.state,
            self._accounting,
        )
        self._recovery_lifecycle = RecoveryLifecycle(
            self.state,
            self._fees,
            self._accounting,
        )
        self._observe_orderbooks = observe_orderbooks
        self._config: EngineConfig | None = None
        self._pending_recovery_books: dict[str, RecoveryPlanningRequested] = {}
        self._detectors: dict[
            tuple[VenueID, VenueID],
            tuple[LongArbitrageDetectionService, ShortArbitrageDetectionService],
        ] = {}
        self._last_signal: dict[
            tuple[tuple[str, str], OrderSide],
            tuple[object, ...],
        ] = {}
        self._short_inventory_remaining: dict[ContractID, Decimal] = {}
        self._reserved_short_execution_ids: set[str] = set()
        self._reserved_notional_by_venue: dict[VenueID, Decimal] = {}
        self._notional_reservations: dict[str, dict[VenueID, Decimal]] = {}
        self._collateral_balance_by_venue: dict[VenueID, Decimal] | None = None
        self._collateral_reserved_by_venue: dict[VenueID, Decimal] = {}
        self._collateral_reservations: dict[str, dict[VenueID, Decimal]] = {}
        self._admission_routes: dict[str, _EconomicRoute] = {}
        self._completed_at_start = 0
        self._run_done = asyncio.Event()

    def configure(self, config: EngineConfig) -> None:
        """Configure monitoring and risk without changing execution mode.

        Raises
        ------
        RuntimeError
            If a previous run still owns an active execution lane.
        """
        if self.active_execution_count():
            raise RuntimeError("Cannot configure trading while executions are active")
        self._config = config
        self._detectors.clear()
        self._last_signal.clear()
        self._short_inventory_remaining = {
            contract_id: quantity.value
            for contract_id, quantity in config.short_inventory_by_contract.items()
        }
        self._reserved_short_execution_ids.clear()
        self._reserved_notional_by_venue.clear()
        self._notional_reservations.clear()
        self._collateral_balance_by_venue = (
            None
            if config.collateral_by_venue is None
            else dict(config.collateral_by_venue)
        )
        self._collateral_reserved_by_venue.clear()
        self._collateral_reservations.clear()
        self._admission_routes.clear()

    def replace_short_inventory(
        self,
        previous_pair_keys: frozenset[tuple[str, str]],
        pair_keys: frozenset[tuple[str, str]],
        inventory_by_contract: Mapping[ContractID, Quantity],
    ) -> None:
        """Replace one rolled market's covered-short inventory.

        Parameters
        ----------
        previous_pair_keys
            Contract pairs prepared for the expired market window.
        pair_keys
            Contract pairs prepared for the new market window.
        inventory_by_contract
            Confirmed complete-set quantity for each new contract.

        Raises
        ------
        RuntimeError
            If the engine is unconfigured or an execution is still active.

        Notes
        -----
        - Inventory for other selected cycles retains its consumed quantity.
        """
        config = self._config
        if config is None:
            raise RuntimeError("Trading engine is not configured")
        if any(
            execution.status
            not in {
                ArbitrageExecutionStatus.COMPLETED,
                ArbitrageExecutionStatus.RECOVERED,
                ArbitrageExecutionStatus.REJECTED,
            }
            for execution in self.state.executions.values()
        ):
            raise RuntimeError(
                "Cannot replace short inventory during an active execution",
            )

        previous_contract_ids = {
            ContractID(contract_id)
            for pair_key in previous_pair_keys
            for contract_id in pair_key
        }
        configured_inventory = {
            contract_id: quantity
            for contract_id, quantity in config.short_inventory_by_contract.items()
            if contract_id not in previous_contract_ids
        }
        configured_inventory.update(inventory_by_contract)
        self._config = replace(
            config,
            short_pair_keys=(config.short_pair_keys - previous_pair_keys) | pair_keys,
            short_inventory_by_contract=configured_inventory,
        )
        for contract_id in previous_contract_ids:
            self._short_inventory_remaining.pop(contract_id, None)
        self._short_inventory_remaining.update(
            {
                contract_id: quantity.value
                for contract_id, quantity in inventory_by_contract.items()
            },
        )

    def enable(self, config: EngineConfig) -> None:
        """Enable command generation using a new run's risk limits."""
        self.configure(config)
        self._completed_at_start = self.state.completed_executions()
        self._run_done.clear()
        self.state.last_error = None
        self.state.safety_halted = False
        self.state.trading_enabled = True

    def disable(self) -> None:
        """Stop creating new order commands while continuing state processing."""
        self.state.trading_enabled = False

    def fail_run(self, reason: str) -> None:
        """Halt the active run after a definitive venue safety failure.

        Parameters
        ----------
        reason
            Failure detail exposed through runtime status and run history.

        Notes
        -----
        - A later explicit :meth:`enable` clears the safety halt.
        - The run completion event is released so the runtime can stop its
          background execution task without waiting for a successful trade.
        """
        self.state.trading_enabled = False
        self.state.safety_halted = True
        self.state.last_error = reason
        self._run_done.set()

    def refresh_collateral(
        self,
        collateral_by_venue: Mapping[VenueID, Decimal],
    ) -> bool:
        """Replace venue collateral while no BUY execution owns a reservation.

        Parameters
        ----------
        collateral_by_venue
            Latest spendable balances after venue allowance constraints.

        Returns
        -------
        bool
            ``True`` when applied, or ``False`` while an admitted BUY is active.

        Raises
        ------
        ValueError
            If any observed collateral amount is negative.
        """
        if any(amount < 0 for amount in collateral_by_venue.values()):
            raise ValueError("venue collateral balances must be non-negative")
        if self._collateral_reservations:
            return False
        self._collateral_balance_by_venue = dict(collateral_by_venue)
        return True

    async def wait_until_done(self) -> None:
        """Wait until the configured successful-execution limit is reached."""
        await self._run_done.wait()

    def active_execution_count(self) -> int:
        """Return active and planned executions that own admission capacity."""
        active_ids = {
            execution_id
            for execution_id, execution in self.state.executions.items()
            if execution.status
            not in {
                ArbitrageExecutionStatus.COMPLETED,
                ArbitrageExecutionStatus.RECOVERED,
                ArbitrageExecutionStatus.REJECTED,
            }
        }
        return len(active_ids | set(self._admission_routes))

    def recovery_outputs(self) -> tuple[ApplicationEvent, ...]:
        """Resume journaled executions interrupted between derived events.

        Returns
        -------
        tuple[ApplicationEvent, ...]
            Missing accounting, scoped safety reviews, or order transitions
            derived from already journaled observations.
        """
        return (
            *self._execution_lifecycle.recovery_outputs(),
            *self._recovery_lifecycle.recovery_outputs(),
        )

    def process(
        self,
        event: ApplicationEvent,
        *,
        replay: bool = False,
    ) -> tuple[ApplicationEvent, ...]:
        """Apply one event and return new internal events in causal order.

        Parameters
        ----------
        event
            Journaled durable event or an ephemeral market-data/recovery reply.
        replay : bool, default=False
            When true, suppress all newly derived events and external commands.

        Returns
        -------
        tuple[ApplicationEvent, ...]
            Durable events to journal or ephemeral requests dispatched without persistence.
        """
        if isinstance(event, OrderBookPairUpdated):
            detected = event.detected
            pair = next(
                (
                    candidate
                    for candidate in self.state.matches.get(detected.cycle, ())
                    if candidate == detected.pair
                ),
                None,
            )
            if pair is None:
                return ()
            event = replace(event, detected=replace(detected, pair=pair))
        if isinstance(event, RecoveryBooksReceived):
            return () if replay else self._plan_recovery_from_books(event)
        previous = self._previous_snapshot(event)
        self.dispatcher.dispatch(event, replay=replay)
        if replay:
            return ()
        if isinstance(event, OrderBookUpdated):
            return self._detect(event)
        if isinstance(event, OrderBookPairUpdated):
            if self._observe_orderbooks is not None:
                self._observe_orderbooks(
                    event.detected.pair,
                    event.left_order_book,
                    event.right_order_book,
                )
            return (event.detected,)
        if isinstance(event, ArbitrageOpportunityFound):
            return self._plan(event)
        if isinstance(event, TradingSafetyStop):
            self._run_done.set()
            return self._execution_lifecycle.handle_safety_stop(event)
        if isinstance(event, (SubmissionReceived, OrderSnapshotUpdated)):
            command = (
                event.command
                if isinstance(event, SubmissionReceived)
                else self.state.commands.get(event.reference.client_order_id)
            )
            if command is not None and command.role == "recovery":
                return self._recovery_lifecycle.handle_order_event(event, previous)
            return self._execution_lifecycle.handle_order_event(event, previous)
        if isinstance(event, ExecutionUpdated):
            if event.execution.status is not ArbitrageExecutionStatus.RECOVERY_PENDING:
                self._pending_recovery_books.pop(event.execution.id, None)
            self._release_unfilled_short_inventory(event.execution)
            self._release_execution_notional(event.execution)
            self._settle_buy_collateral(event.execution)
            self._release_admission_route(event.execution)
            outputs: tuple[ApplicationEvent, ...] = ()
            if event.execution.status is ArbitrageExecutionStatus.RECOVERY_PENDING:
                config = self._config
                if config is None or config.max_recovery_loss is None:
                    outputs = self._recovery_lifecycle.review_execution(
                        event.execution,
                        "automatic recovery is disabled",
                    )
                else:
                    pending = self._pending_recovery_books.get(event.execution.id)
                    if pending is None or pending.execution != event.execution:
                        request = RecoveryPlanningRequested(
                            uuid4().hex, event.execution,
                            deadline_at_ns=time.monotonic_ns() + _RECOVERY_QUOTE_WAIT_NS,
                        )
                        self._pending_recovery_books[event.execution.id] = request
                        outputs = (request,)
            if event.execution.status is ArbitrageExecutionStatus.NEEDS_REVIEW:
                self.state.trading_enabled = False
                self.state.last_error = event.execution.last_error or "execution needs review"
                self._run_done.set()
            self._finish_run_if_needed()
            return outputs
        return ()

    def _plan_recovery_from_books(
        self, event: RecoveryBooksReceived,
    ) -> tuple[ApplicationEvent, ...]:
        """Apply a current pair atomically, then choose a centrally bounded recovery.

        Notes
        -----
        - Delayed or duplicate replies cannot resurrect a settled/replaced execution.
        - No await separates pair installation from the recovery decision.
        """
        request = event.request
        execution = self.state.executions.get(request.execution.id)
        if self._pending_recovery_books.get(request.execution.id) != request:
            return ()
        self._pending_recovery_books.pop(request.execution.id)
        if execution != request.execution or execution.status is not ArbitrageExecutionStatus.RECOVERY_PENDING:
            return ()
        error = event.error
        if (error is None and request.deadline_at_ns is not None
                and time.monotonic_ns() >= request.deadline_at_ns):
            error = "recovery quote wait expired: no fresh route satisfied freshness, liquidity and loss limits"
        if error is None and event.books is None:
            error = "recovery books unavailable"
        contracts = (execution.leg1_contract_id, execution.leg2_contract_id)
        if (error is None and event.fresh_contract_ids is not None
                and not event.fresh_contract_ids.issubset(contracts)):
            error = "recovery freshness identity mismatch"
        if error is None:
            for contract_id, book in zip(contracts, event.books, strict=True):
                contract = self.state.contracts.get(contract_id)
                if (contract is None or book.market_id != contract.market_id
                        or book.outcome_id != contract.outcome_id
                        or book.received_at_ns is None
                        or book.received_at_ns > time.monotonic_ns()):
                    error = "recovery book identity or clock mismatch"
                    break
        config = self._config
        if error is None and (config is None or config.max_recovery_loss is None):
            error = "automatic recovery is disabled"
        if error is not None:
            return self._recovery_lifecycle._review_plan(
                self.state.recoveries.get(execution.id), execution, error,
            )
        assert event.books is not None and config is not None and config.max_recovery_loss is not None
        for contract_id, book in zip(contracts, event.books, strict=True):
            current = self.state.books.get(contract_id)
            if current is None or (current.received_at_ns or 0) <= book.received_at_ns:
                contract = self.state.contracts[contract_id]
                self.state.apply(OrderBookUpdated(contract.venue_id, contract_id, book))
        outputs = self._recovery_lifecycle.plan(
            execution, max_loss=config.max_recovery_loss,
            min_buy_notional=config.min_notional_per_venue,
            fresh_contract_ids=event.fresh_contract_ids,
            wait_for_quote=event.fresh_contract_ids is not None,
        )
        if outputs is None:
            now_ns = time.monotonic_ns()
            deadline_at_ns = (
                request.deadline_at_ns if request.deadline_at_ns is not None
                else now_ns + _RECOVERY_QUOTE_WAIT_NS
            )
            if now_ns >= deadline_at_ns:
                return self._recovery_lifecycle._review_plan(
                    self.state.recoveries.get(execution.id), execution,
                    "recovery quote wait expired: no fresh route satisfied freshness, liquidity and loss limits",
                )
            retry = RecoveryPlanningRequested(
                uuid4().hex, execution, deadline_at_ns=deadline_at_ns,
                not_before_ns=min(now_ns + _RECOVERY_QUOTE_POLL_NS, deadline_at_ns),
            )
            self._pending_recovery_books[execution.id] = retry
            return (retry,)
        return outputs

    def guard_recovery(self, command: SubmitOrder, book: OrderBook) -> str | None:
        """Recheck the signed recovery's current economics using central loss limits."""
        config = self._config
        if config is None or config.max_recovery_loss is None:
            return "automatic recovery is disabled"
        return self._recovery_lifecycle.guard_command(
            command, book, max_loss=config.max_recovery_loss,
            min_buy_notional=config.min_notional_per_venue,
        )

    def stage_opportunity(
        self,
        event: ArbitrageOpportunityFound,
    ) -> tuple[ArbitragePlanned, SubmitOrder, SubmitOrder] | tuple[()]:
        """Plan and reserve one opportunity without exposing unjournaled state."""
        outputs = self._plan(event)
        if not outputs:
            return ()
        planned, primary, hedge = outputs
        if not (
            isinstance(planned, ArbitragePlanned)
            and isinstance(primary, SubmitOrder)
            and isinstance(hedge, SubmitOrder)
        ):
            raise RuntimeError("Opportunity planning returned an invalid execution batch")
        return planned, primary, hedge

    def commit(self, event: ApplicationEvent, *, replay: bool = False) -> None:
        """Apply an event that has already crossed its durability boundary."""
        self.dispatcher.dispatch(event, replay=replay)

    def commit_prepared_execution(
        self,
        batch: PreparedExecutionBatch,
        *,
        replay: bool = False,
    ) -> None:
        """Apply the logical events stored in one durable execution frame."""
        for event in prepared_execution_events(batch):
            self.dispatcher.dispatch(event, replay=replay)

    def abort_staged_execution(self, execution: ArbitrageExecutionJournal) -> None:
        """Release reservations when a staged execution cannot be journaled."""
        rejected = replace(execution, status=ArbitrageExecutionStatus.REJECTED)
        self._release_unfilled_short_inventory(rejected)
        self._release_execution_notional(rejected)
        self._settle_buy_collateral(rejected)
        self._release_admission_route(rejected)
        self.state.timings.pop(execution.id, None)

    def reprice_staged_execution(
        self,
        request: ExecutionPreparationRequested,
        left: OrderBook,
        right: OrderBook,
    ) -> ExecutionPreparationRequested | None:
        """Replan an unjournaled pair using current books and normal risk limits.

        Parameters
        ----------
        request
            Staged, never-submitted execution whose deadline must not move.
        left, right
            Atomic worker snapshots translated to the parent clock.

        Returns
        -------
        ExecutionPreparationRequested | None
            Replacement commands with the original identity and deadline, or
            ``None`` when the current opportunity cannot pass admission and risk.

        Notes
        -----
        - No await or external I/O separates releasing and reserving exposure.
        - Committed executions and any observed submission cannot be repriced.
        """
        execution_id = request.planned.execution.id
        timings = self.state.timings.get(execution_id)
        if (
            execution_id in self.state.executions
            or execution_id not in self._admission_routes
            or self.state.safety_halted
            or time.monotonic_ns() >= request.deadline_at_ns
            or (timings is not None and timings.submit_at_ns)
        ):
            return None
        pair = request.opportunity.pair
        if pair not in self.state.matches.get(request.opportunity.cycle, ()):
            return None
        for contract, book in ((pair.left, left), (pair.right, right)):
            if (
                book.market_id != contract.market_id
                or book.outcome_id != contract.outcome_id
            ):
                return None
        opportunity = self.detect_shadow_opportunity(
            pair, left, right, request.opportunity.opportunity.side,
        )
        if opportunity is None:
            return None
        detected = replace(request.opportunity, opportunity=opportunity)
        self.state.apply(OrderBookPairUpdated(detected, left, right))
        self.abort_staged_execution(request.planned.execution)
        try:
            staged = self.stage_opportunity(detected)
        finally:
            if timings is not None:
                revised_timings = self.state.timings.get(execution_id)
                if revised_timings is not None and revised_timings is not timings:
                    timings.entry_pricing += revised_timings.entry_pricing
                self.state.timings[execution_id] = timings
        if not staged:
            return None
        planned, primary, hedge = staged
        if timings is not None:
            old_roles = {
                command.intent.contract_id: command.role
                for command in request.commands
            }
            new_roles = {
                command.role: old_roles[command.intent.contract_id]
                for command in (primary, hedge)
            }
            for item in fields(timings):
                marks = getattr(timings, item.name)
                if isinstance(marks, dict):
                    setattr(timings, item.name, {
                        role: marks[old] for role, old in new_roles.items()
                        if old in marks
                    })
            timings.worker_reprices += 1
        return replace(
            request, opportunity=detected, planned=planned,
            commands=(primary, hedge),
        )

    def _detect(self, event: OrderBookUpdated) -> tuple[ApplicationEvent, ...]:
        config = self._config
        if config is None:
            return ()
        found: list[ApplicationEvent] = []
        for cycle, pair in self.state.affected_pairs(event.contract_id):
            left = self.state.books.get(pair.left.id)
            right = self.state.books.get(pair.right.id)
            if left is None or right is None:
                continue
            found.extend(self._detect_pair(cycle, pair, left, right))
        return tuple(found)

    def _detect_pair(
        self,
        cycle: MonitoredMarket,
        pair: MatchedContractPair,
        left: OrderBook,
        right: OrderBook,
        *,
        deduplicate: bool = True,
    ) -> tuple[ApplicationEvent, ...]:
        """Detect opportunities from one locally authoritative book pair."""
        config = self._config
        if config is None:
            return ()
        if self._observe_orderbooks is not None:
            self._observe_orderbooks(pair, left, right)
        found: list[ApplicationEvent] = []
        detectors = self._detectors_for(pair, config)
        enabled = (config.monitor_long, config.monitor_short)
        for detector, allowed in zip(detectors, enabled, strict=True):
            if not allowed:
                continue
            side = OrderSide.BUY if detector is detectors[0] else OrderSide.SELL
            signal_key = (pair.key, side)
            try:
                opportunity = detector.detect(
                    pair.left.id,
                    pair.right.id,
                    left,
                    right,
                    pair.left.tick_size,
                    pair.right.tick_size,
                )
            except (KeyError, RuntimeError, ValueError) as error:
                self.state.last_error = str(error)
                continue
            if opportunity is None:
                self._last_signal.pop(signal_key, None)
                continue
            fingerprint = _opportunity_fingerprint(opportunity)
            if deduplicate and self._last_signal.get(signal_key) == fingerprint:
                continue
            if deduplicate:
                self._last_signal[signal_key] = fingerprint
            found.append(
                ArbitrageOpportunityFound(
                    id=_opportunity_id(
                        monitored_market_label(cycle),
                        opportunity,
                        left,
                        right,
                    ),
                    cycle=cycle,
                    pair=pair,
                    opportunity=opportunity,
                ),
            )
        return tuple(found)

    def _detectors_for(
        self,
        pair: MatchedContractPair,
        config: EngineConfig,
    ) -> tuple[LongArbitrageDetectionService, ShortArbitrageDetectionService]:
        key = pair.left.venue_id, pair.right.venue_id
        cached = self._detectors.get(key)
        if cached is not None:
            return cached
        options = {
            "max_skew_ms": config.max_skew_ms,
            "cost_buffer": config.cost_buffer,
            "min_edge": config.min_net_edge,
            "left_taker_fees": self._fees.get(pair.left.venue_id),
            "right_taker_fees": self._fees.get(pair.right.venue_id),
        }
        detectors = (
            LongArbitrageDetectionService(**options),
            ShortArbitrageDetectionService(**options),
        )
        self._detectors[key] = detectors
        return detectors

    def detect_shadow_opportunity(
        self,
        pair: MatchedContractPair,
        left: OrderBook,
        right: OrderBook,
        side: OrderSide,
    ) -> ArbitrageOpportunity | None:
        """Detect one requested side for worker shadow-mode comparison.

        Parameters
        ----------
        pair
            Matched contracts whose fees and ticks define the route.
        left
            Latest left-contract book.
        right
            Latest right-contract book.
        side
            Long ``BUY`` or short ``SELL`` opportunity to verify.

        Returns
        -------
        ArbitrageOpportunity | None
            Current fee-aware opportunity, or ``None`` when it no longer exists.
        """
        config = self._config
        if config is None:
            return None
        detector = self._detectors_for(pair, config)[
            0 if side is OrderSide.BUY else 1
        ]
        return detector.detect(
            pair.left.id,
            pair.right.id,
            left,
            right,
            pair.left.tick_size,
            pair.right.tick_size,
        )

    def _has_equivalent_route(
        self,
        pair: MatchedContractPair,
        side: OrderSide,
    ) -> bool:
        """Report whether an admitted execution already consumes this route.

        Notes
        -----
        - Opposite-side plans over complementary contracts of the same two venue
          markets are equivalent: BUY NO consumes the same economic route as
          SELL YES, and vice versa.
        - Reservations are checked before their ``ArbitragePlanned`` event is
          applied, closing admission races inside one event-loop batch.
        """
        route = _economic_route((pair.left, pair.right), side)
        if any(
            _routes_are_equivalent(route, admitted)
            for admitted in self._admission_routes.values()
        ):
            return True
        for execution_id, execution in self.state.executions.items():
            if (
                execution_id in self._admission_routes
                or execution.status
                in {
                    ArbitrageExecutionStatus.COMPLETED,
                    ArbitrageExecutionStatus.RECOVERED,
                    ArbitrageExecutionStatus.REJECTED,
                }
            ):
                continue
            leg1_contract = self.state.contracts.get(execution.leg1_contract_id)
            leg2_contract = self.state.contracts.get(execution.leg2_contract_id)
            if leg1_contract is None or leg2_contract is None:
                continue
            admitted = _economic_route(
                (leg1_contract, leg2_contract),
                execution.leg1_side,
            )
            if _routes_are_equivalent(route, admitted):
                return True
        return False

    def _release_admission_route(
        self,
        execution: ArbitrageExecutionJournal,
    ) -> None:
        """Release route capacity after an execution becomes terminal."""
        if execution.status in {
            ArbitrageExecutionStatus.COMPLETED,
            ArbitrageExecutionStatus.RECOVERED,
            ArbitrageExecutionStatus.REJECTED,
        }:
            self._admission_routes.pop(execution.id, None)

    def _has_unresolved_exposure(self) -> bool:
        """Block new admission while an execution requires hedging or review."""
        return any(
            execution.status
            in {
                ArbitrageExecutionStatus.RECOVERY_PENDING,
                ArbitrageExecutionStatus.UNWIND_PENDING,
                ArbitrageExecutionStatus.NEEDS_REVIEW,
            }
            for execution in self.state.executions.values()
        )

    def _plan(
        self,
        event: ArbitrageOpportunityFound,
    ) -> tuple[ApplicationEvent, ...]:
        config = self._config
        active_executions = self.active_execution_count()
        if (
            config is None
            or not self.state.trading_enabled
            or event.id in self.state.executions
            or event.id in self._admission_routes
            or self._has_unresolved_exposure()
            or self._has_equivalent_route(event.pair, event.opportunity.side)
            or self.state.is_pair_active(event.pair)
            or not _market_allowed(event.cycle, config)
            or not _market_has_time_remaining(event.cycle, event.pair, config)
            or event.opportunity.side is OrderSide.BUY and not config.execute_long
            or event.opportunity.side is OrderSide.SELL
            and (
                not config.execute_short
                or monitored_market_key(event.cycle) not in config.short_market_keys
                or event.pair.key not in config.short_pair_keys
            )
            or active_executions >= config.max_concurrent_arbitrages
            or self.state.completed_executions()
            - self._completed_at_start
            + active_executions
            >= config.max_arbitrages
        ):
            return ()
        pricing: dict[str, object] = {}
        pricing_started_ns = time.monotonic_ns()
        sized = self._risk_adjusted_opportunity(
            event.opportunity, event.pair, config, pricing=pricing,
        )
        pricing_finished_ns = time.monotonic_ns()
        if sized is None:
            return ()
        plan = self._planning.plan(sized)
        if plan is None:
            return ()

        primary, hedge = plan.legs
        execution_id = event.id

        primary_contract = self.state.contracts[primary.contract_id]
        hedge_contract = self.state.contracts[hedge.contract_id]
        primary_book = self.state.books.get(primary.contract_id)
        hedge_book = self.state.books.get(hedge.contract_id)
        self.state.timings[execution_id] = ExecutionTimings(
            opportunity_at_ns=time.monotonic_ns(),
            venue_ids={
                "primary": str(primary_contract.venue_id),
                "hedge": str(hedge_contract.venue_id),
            },
            book_received_at_ns={
                role: book.received_at_ns
                for role, book in (
                    ("primary", primary_book),
                    ("hedge", hedge_book),
                )
                if book is not None and book.received_at_ns is not None
            },
        )
        for contract, before, after in zip(
            (event.pair.left, event.pair.right),
            (event.opportunity.left_level, event.opportunity.right_level),
            (sized.left_level, sized.right_level),
            strict=True,
        ):
            if contract.venue_id == _PREDICT_VENUE_ID:
                pricing.update({
                    "mode": "edge_budget" if config.predict_use_edge_budget else "fixed_ticks",
                    "contract_id": str(contract.id),
                    "side": sized.side.value,
                    "quantity": str(sized.quantity.value),
                    "detected_limit": str(before.price.value),
                    "planned_limit": str(after.price.value),
                    "detected_net_edge": str(event.opportunity.net_edge),
                    "planned_net_edge": str(sized.net_edge),
                    "min_net_edge": str(config.min_net_edge),
                    "cost_buffer": str(config.cost_buffer),
                })
                self.state.timings[execution_id].mark_entry_pricing(
                    pricing, pricing_started_ns, pricing_finished_ns,
                )
        primary_client_id = ClientOrderID(f"{execution_id}-primary")
        hedge_client_id = ClientOrderID(f"{execution_id}-hedge")
        now = Timestamp.now()
        strategy_id = StrategyID(
            "long-arbitrage" if plan.side is OrderSide.BUY else "short-arbitrage",
        )
        execution = ArbitrageExecutionJournal(
            id=execution_id,
            status=ArbitrageExecutionStatus.PLANNED,
            leg1_venue_id=primary_contract.venue_id,
            leg1_contract_id=primary.contract_id,
            leg1_side=primary.side,
            leg1_quantity=primary.quantity,
            leg1_limit_price=primary.limit_price,
            leg1_client_order_id=primary_client_id,
            leg2_venue_id=hedge_contract.venue_id,
            leg2_contract_id=hedge.contract_id,
            leg2_side=hedge.side,
            leg2_quantity=hedge.quantity,
            leg2_limit_price=hedge.limit_price,
            leg2_client_order_id=hedge_client_id,
            portfolio_id=config.portfolio_id,
            strategy_id=strategy_id,
            created_at=now,
            updated_at=now,
            leg1_decision=_decision_snapshot(
                primary_contract.venue_id,
                primary.contract_id,
                primary.side,
                primary.limit_price,
                primary.quantity,
                primary_book,
            ),
            leg2_decision=_decision_snapshot(
                hedge_contract.venue_id,
                hedge.contract_id,
                hedge.side,
                hedge.limit_price,
                hedge.quantity,
                hedge_book,
            ),
        )
        if not self._reserve_execution_notional(execution_id, event.pair, sized):
            return ()
        if plan.side is OrderSide.BUY:
            if not self._reserve_buy_collateral(execution_id, event.pair, sized):
                self._release_notional_reservation(execution_id)
                return ()
        else:
            try:
                self._reserve_short_inventory(execution_id, event.pair, sized.quantity)
            except RuntimeError:
                self._release_notional_reservation(execution_id)
                raise
        self._admission_routes[execution_id] = _economic_route(
            (event.pair.left, event.pair.right),
            plan.side,
        )
        return (
            ArbitragePlanned(
                opportunity_id=event.id,
                cycle=event.cycle,
                pair=event.pair,
                plan=plan,
                execution=execution,
            ),
            _execution_command(execution, "primary"),
            _execution_command(execution, "hedge"),
        )

    def _risk_adjusted_opportunity(
        self,
        opportunity: ArbitrageOpportunity,
        pair: MatchedContractPair,
        config: EngineConfig,
        *,
        pricing: dict[str, object] | None = None,
    ) -> ArbitrageOpportunity | None:
        """Size both legs to one fee-aware quantity within venue constraints.

        Parameters
        ----------
        opportunity
            Detected quotes and available quantity before execution adjustments.
        pair
            Exact contracts supplying venue ticks, lots and minimum sizes.
        config
            Current entry, collateral and inventory policy.
        pricing
            Optional caller-owned diagnostic fields for the bounded limit search.

        Returns
        -------
        ArbitrageOpportunity | None
            Resized opportunity, or `None` when budgets, liquidity, minimum
            quantities, or net edge prevent a valid two-leg order.

        Notes
        -----
        - Predict BUY limits add the configured tick headroom and round up;
          SELL limits subtract it and round down. Both stay within
          ``[tick, 1 - tick]``.
        - Gross edge, fees, and venue notionals use the adjusted limits.
        - Edge-budget mode sizes at the original quotes, then spends only the
          affordable margin on Predict without resizing either leg.
        - Detected quantities at or above the configured threshold are
          multiplied by the liquidity safety factor before lot rounding.
        - A market-buy amount step adds ``amount_step / tick_size`` to the
          shared quantity grid, keeping price-times-quantity venue-valid at
          every possible tick price.
        """
        contracts = (pair.left, pair.right)
        levels = [opportunity.left_level, opportunity.right_level]
        if config.predict_limit_slippage_ticks and not config.predict_use_edge_budget:
            for index, (contract, level) in enumerate(
                zip(contracts, levels, strict=True),
            ):
                if contract.venue_id != _PREDICT_VENUE_ID:
                    continue
                if contract.tick_size is None:
                    return None
                tick = contract.tick_size.value
                slippage = tick * config.predict_limit_slippage_ticks
                unrounded = (
                    level.price.value + slippage
                    if opportunity.side is OrderSide.BUY
                    else level.price.value - slippage
                )
                rounded = (unrounded / tick).to_integral_value(
                    rounding=(
                        ROUND_CEILING
                        if opportunity.side is OrderSide.BUY
                        else ROUND_FLOOR
                    ),
                ) * tick
                limit = (
                    min(Decimal("1") - tick, rounded)
                    if opportunity.side is OrderSide.BUY
                    else max(tick, rounded)
                )
                levels[index] = replace(level, price=Price(limit))
        adjusted_levels = (levels[0], levels[1])
        gross_edge = (
            Decimal("1")
            - adjusted_levels[0].price.value
            - adjusted_levels[1].price.value
            if opportunity.side is OrderSide.BUY
            else adjusted_levels[0].price.value
            + adjusted_levels[1].price.value
            - Decimal("1")
        )
        steps = [
            contract.lot_size.value if contract.lot_size is not None else Decimal("1")
            for contract in contracts
        ]
        if opportunity.side is OrderSide.BUY:
            for contract in contracts:
                amount_step = config.market_buy_notional_steps.get(contract.venue_id)
                if amount_step is None:
                    continue
                if contract.tick_size is None:
                    return None
                steps.append(amount_step / contract.tick_size.value)
        step = _common_step(tuple(steps))
        quantity_value = opportunity.quantity.value
        if quantity_value >= config.large_order_contract_threshold:
            quantity_value *= config.large_order_liquidity_safety_factor
        quantity_value = _round_down(quantity_value, step)
        if opportunity.side is OrderSide.SELL:
            quantity_value = min(
                quantity_value,
                *(
                    _round_down(
                        self._short_inventory_remaining.get(contract.id, Decimal("0")),
                        step,
                    )
                    for contract in contracts
                ),
            )
            if quantity_value <= 0:
                return None
        for contract, level in zip(contracts, adjusted_levels, strict=True):
            budget = config.max_notional_by_venue.get(contract.venue_id)
            calculator = self._fees.get(contract.venue_id)
            if budget is None or calculator is None:
                return None
            budget = self._risk_budget(
                contract.venue_id,
                opportunity.side,
                config,
            )
            unit_fee = calculator.calculate(
                contract.id,
                level.price,
                Quantity(Decimal("1")),
                opportunity.side,
            ).settlement_cost.amount
            unit_cost = (
                level.price.value + unit_fee
                if opportunity.side is OrderSide.BUY
                else Decimal("1") - level.price.value + unit_fee
            )
            quantity_value = min(
                quantity_value,
                _round_down(budget / unit_cost, step),
            )
        while quantity_value > 0:
            quantity = Quantity(quantity_value)
            fees = tuple(
                self._fees[contract.venue_id].calculate(
                    contract.id,
                    level.price,
                    quantity,
                    opportunity.side,
                )
                for contract, level in zip(contracts, adjusted_levels, strict=True)
            )
            if len({fee.settlement_cost.currency for fee in fees}) != 1:
                raise ValueError("Arbitrage fees must share a settlement currency")
            within_budget = all(
                (
                    level.price.value * quantity_value
                    if opportunity.side is OrderSide.BUY
                    else (Decimal("1") - level.price.value) * quantity_value
                )
                + fee.settlement_cost.amount
                <= self._risk_budget(
                    contract.venue_id,
                    opportunity.side,
                    config,
                )
                for contract, level, fee in zip(
                    contracts,
                    adjusted_levels,
                    fees,
                    strict=True,
                )
            )
            if within_budget:
                break
            quantity_value -= step
        if quantity_value <= 0 or any(
            contract.minimum_order_size is not None
            and quantity_value < contract.minimum_order_size.value
            for contract in contracts
        ) or (
            opportunity.side is OrderSide.BUY
            and any(
                level.price.value * quantity_value < config.min_notional_per_venue
                for level in adjusted_levels
            )
        ):
            return None
        total_fees = sum(
            (fee.settlement_cost.amount for fee in fees),
            Decimal("0"),
        )
        fee_per_contract = total_fees / quantity_value
        net_edge = gross_edge - fee_per_contract - config.cost_buffer
        if net_edge <= config.min_net_edge:
            return None
        sized = replace(
            opportunity,
            left_level=adjusted_levels[0],
            right_level=adjusted_levels[1],
            quantity=Quantity(quantity_value),
            gross_edge=gross_edge,
            total_fees=total_fees,
            fee_per_contract=fee_per_contract,
            net_edge=net_edge,
        )
        if config.predict_use_edge_budget:
            return self._predict_edge_budget(sized, pair, config, pricing=pricing)
        return sized

    def _predict_edge_budget(
        self,
        opportunity: ArbitrageOpportunity,
        pair: MatchedContractPair,
        config: EngineConfig,
        *,
        pricing: dict[str, object] | None = None,
    ) -> ArbitrageOpportunity | None:
        """Spend available entry edge on a validated Predict limit in memory.

        Parameters
        ----------
        opportunity
            Already validated baseline with execution quantity and settlement fees.
        pair
            Exact contracts; only a single Predict leg may change.
        config
            Minimum edge, cost allowance and authoritative remaining budgets.
        pricing
            Optional diagnostic output for elapsed milliseconds and edge spent.

        Returns
        -------
        ArbitrageOpportunity | None
            Best validated candidate, unchanged baseline when no tick fits, or
            ``None`` when the Predict identity or price grid cannot be validated.

        Notes
        -----
        - Start from an already sized, fee-aware opportunity. Neither the other
          leg nor the quantity changes. This method is never used for recovery.
        - Each candidate recomputes Predict fees for the actual quantity and
          respects the current risk budget and strict net-edge floor.
        - Bisection makes at most 32 candidate checks, including fine tick grids.
          Only tested feasible prices are retained. Non-monotone fee schedules
          or finer grids can leave unused edge, never authorize an unchecked limit.
        - Timing covers this search only; it is a subset of entry pricing time.
        """
        contracts = (pair.left, pair.right)
        indices = [
            i for i, contract in enumerate(contracts)
            if contract.venue_id == _PREDICT_VENUE_ID
        ]
        if not indices:
            return opportunity
        if len(indices) != 1:
            return None
        index = indices[0]
        contract = contracts[index]
        if contract.tick_size is None:
            return None
        levels = (opportunity.left_level, opportunity.right_level)
        level = levels[index]
        tick = contract.tick_size.value
        original = level.price.value
        if original % tick or not tick <= original <= Decimal("1") - tick:
            return None
        started_ns = time.monotonic_ns()
        quantity = opportunity.quantity
        calculator = self._fees[contract.venue_id]
        original_fee = calculator.calculate(
            contract.id, level.price, quantity, opportunity.side,
        )
        other_fee = opportunity.total_fees - original_fee.settlement_cost.amount
        budget = self._risk_budget(contract.venue_id, opportunity.side, config)
        direction = 1 if opportunity.side is OrderSide.BUY else -1
        distance = Decimal("1") - tick - original if direction == 1 else original - tick
        low, high = 1, int(distance / tick)
        best = opportunity
        checks = 0
        while low <= high and checks < 32:
            steps = (low + high) // 2
            price = Price(original + direction * steps * tick)
            fee = calculator.calculate(contract.id, price, quantity, opportunity.side)
            checks += 1
            if fee.settlement_cost.currency != original_fee.settlement_cost.currency:
                raise ValueError("Arbitrage fees must share a settlement currency")
            total_fees = other_fee + fee.settlement_cost.amount
            gross = opportunity.gross_edge - steps * tick
            net = gross - total_fees / quantity.value - config.cost_buffer
            unit_cost = price.value if direction == 1 else Decimal("1") - price.value
            if net > config.min_net_edge and (
                unit_cost * quantity.value + fee.settlement_cost.amount <= budget
            ):
                adjusted = replace(level, price=price)
                best = replace(
                    opportunity,
                    left_level=adjusted if index == 0 else levels[0],
                    right_level=adjusted if index == 1 else levels[1],
                    gross_edge=gross,
                    total_fees=total_fees,
                    fee_per_contract=total_fees / quantity.value,
                    net_edge=net,
                )
                low = steps + 1
            else:
                high = steps - 1
        if pricing is not None:
            pricing.update({
                "edge_budget_search_ms": (time.monotonic_ns() - started_ns) / 1_000_000,
                "candidate_checks": checks,
                "baseline_net_edge": str(opportunity.net_edge),
                "edge_spent": str(opportunity.net_edge - best.net_edge),
            })
        return best

    def _risk_budget(
        self,
        venue_id: VenueID,
        side: OrderSide,
        config: EngineConfig,
    ) -> Decimal:
        """Return unreserved configured notional constrained by BUY cash."""
        budget = max(
            Decimal("0"),
            config.max_notional_by_venue[venue_id]
            - self._reserved_notional_by_venue.get(venue_id, Decimal("0")),
        )
        if side is not OrderSide.BUY:
            return budget
        available = self._available_collateral(venue_id)
        if available is None:
            return budget
        return min(
            budget,
            max(Decimal("0"), available - _COLLATERAL_SAFETY_BUFFER),
        )

    def _reserve_execution_notional(
        self,
        execution_id: str,
        pair: MatchedContractPair,
        opportunity: ArbitrageOpportunity,
    ) -> bool:
        """Reserve aggregate venue settlement budget for an admitted plan.

        Returns
        -------
        bool
            Whether every venue had enough unreserved configured budget.
        """
        reservations: dict[VenueID, Decimal] = {}
        for contract, level in zip(
            (pair.left, pair.right),
            (opportunity.left_level, opportunity.right_level),
            strict=True,
        ):
            calculator = self._fees.get(contract.venue_id)
            if calculator is None:
                return False
            amount = _settlement_notional(
                calculator,
                contract.id,
                level.price,
                opportunity.quantity,
                opportunity.side,
            )
            reservations[contract.venue_id] = (
                reservations.get(contract.venue_id, Decimal("0")) + amount
            )
        config = self._config
        if config is None or any(
            amount > self._risk_budget(venue_id, opportunity.side, config)
            for venue_id, amount in reservations.items()
        ):
            return False
        self._notional_reservations[execution_id] = reservations
        for venue_id, amount in reservations.items():
            self._reserved_notional_by_venue[venue_id] = (
                self._reserved_notional_by_venue.get(venue_id, Decimal("0"))
                + amount
            )
        return True

    def _release_execution_notional(
        self,
        execution: ArbitrageExecutionJournal,
    ) -> None:
        """Release configured budget after one execution becomes terminal."""
        if execution.status not in {
            ArbitrageExecutionStatus.COMPLETED,
            ArbitrageExecutionStatus.RECOVERED,
            ArbitrageExecutionStatus.REJECTED,
        }:
            return
        self._release_notional_reservation(execution.id)

    def _release_notional_reservation(self, execution_id: str) -> None:
        """Release one reservation idempotently."""
        reservations = self._notional_reservations.pop(execution_id, None)
        if reservations is None:
            return
        for venue_id, amount in reservations.items():
            remaining = self._reserved_notional_by_venue[venue_id] - amount
            if remaining > 0:
                self._reserved_notional_by_venue[venue_id] = remaining
            else:
                self._reserved_notional_by_venue.pop(venue_id, None)

    def _available_collateral(self, venue_id: VenueID) -> Decimal | None:
        """Return locally spendable collateral after active reservations."""
        if self._collateral_balance_by_venue is None:
            return None
        return max(
            Decimal("0"),
            self._collateral_balance_by_venue.get(venue_id, Decimal("0"))
            - self._collateral_reserved_by_venue.get(venue_id, Decimal("0")),
        )

    def _reserve_buy_collateral(
        self,
        execution_id: str,
        pair: MatchedContractPair,
        opportunity: ArbitrageOpportunity,
    ) -> bool:
        """Reserve worst-case BUY settlement cost before emitting commands."""
        if self._collateral_balance_by_venue is None:
            return True
        reservations: dict[VenueID, Decimal] = {}
        for contract, level in zip(
            (pair.left, pair.right),
            (opportunity.left_level, opportunity.right_level),
            strict=True,
        ):
            fee = self._fees[contract.venue_id].calculate(
                contract.id,
                level.price,
                opportunity.quantity,
                OrderSide.BUY,
            ).settlement_cost.amount
            amount = (
                level.price.value * opportunity.quantity.value
                + fee
                + _COLLATERAL_SAFETY_BUFFER
            )
            reservations[contract.venue_id] = (
                reservations.get(contract.venue_id, Decimal("0")) + amount
            )
        if any(
            amount > (self._available_collateral(venue_id) or Decimal("0"))
            for venue_id, amount in reservations.items()
        ):
            return False
        self._collateral_reservations[execution_id] = reservations
        for venue_id, amount in reservations.items():
            self._collateral_reserved_by_venue[venue_id] = (
                self._collateral_reserved_by_venue.get(venue_id, Decimal("0"))
                + amount
            )
        return True

    def _settle_buy_collateral(
        self,
        execution: ArbitrageExecutionJournal,
    ) -> None:
        """Release a zero-fill reject or consume a terminal BUY reservation."""
        if execution.status not in {
            ArbitrageExecutionStatus.COMPLETED,
            ArbitrageExecutionStatus.RECOVERED,
            ArbitrageExecutionStatus.REJECTED,
        }:
            return
        reservations = self._collateral_reservations.pop(execution.id, None)
        if reservations is None:
            return
        consumed = execution.status is not ArbitrageExecutionStatus.REJECTED
        for venue_id, amount in reservations.items():
            remaining = self._collateral_reserved_by_venue[venue_id] - amount
            if remaining > 0:
                self._collateral_reserved_by_venue[venue_id] = remaining
            else:
                self._collateral_reserved_by_venue.pop(venue_id, None)
            if consumed and self._collateral_balance_by_venue is not None:
                self._collateral_balance_by_venue[venue_id] = max(
                    Decimal("0"),
                    self._collateral_balance_by_venue.get(venue_id, Decimal("0"))
                    - amount,
                )

    def _reserve_short_inventory(
        self,
        execution_id: str,
        pair: MatchedContractPair,
        quantity: Quantity,
    ) -> None:
        """Reserve covered tokens when a SELL plan creates its two commands."""
        for contract in (pair.left, pair.right):
            remaining = self._short_inventory_remaining.get(contract.id, Decimal("0"))
            if remaining < quantity.value:
                raise RuntimeError(f"Insufficient prepared short inventory for {contract.id}")
        for contract in (pair.left, pair.right):
            self._short_inventory_remaining[contract.id] -= quantity.value
        self._reserved_short_execution_ids.add(execution_id)

    def _release_unfilled_short_inventory(
        self,
        execution: ArbitrageExecutionJournal,
    ) -> None:
        """Return unused covered tokens after a short execution completes or is rejected.

        Parameters
        ----------
        execution
            Terminal execution containing requested and filled leg quantities.

        Notes
        -----
        - The reservation identifier makes repeated terminal events idempotent.
        - Recovery states retain their reservation because the run stops before
          same-run inventory can be reused safely.
        """
        if (
            execution.status
            not in {
                ArbitrageExecutionStatus.COMPLETED,
                ArbitrageExecutionStatus.REJECTED,
            }
            or execution.id not in self._reserved_short_execution_ids
        ):
            return
        self._reserved_short_execution_ids.remove(execution.id)
        for contract_id, requested, filled in (
            (
                execution.leg1_contract_id,
                execution.leg1_quantity,
                execution.leg1_filled_quantity,
            ),
            (
                execution.leg2_contract_id,
                execution.leg2_quantity,
                execution.leg2_filled_quantity,
            ),
        ):
            self._short_inventory_remaining[contract_id] += max(
                Decimal("0"),
                requested.value - filled.value,
            )

    def _previous_snapshot(
        self,
        event: ApplicationEvent,
    ) -> OrderSnapshot | None:
        if isinstance(event, SubmissionReceived):
            return self.state.orders.get(event.result.reference.client_order_id)
        if isinstance(event, OrderSnapshotUpdated):
            return self.state.orders.get(event.reference.client_order_id)
        return None

    def _finish_run_if_needed(self) -> None:
        config = self._config
        if config is None:
            return
        if (
            self.state.completed_executions() - self._completed_at_start
            >= config.max_arbitrages
        ):
            self.state.trading_enabled = False
            self._run_done.set()


def _settlement_notional(
    calculator: TakerFeeCalculatorPort,
    contract_id: ContractID,
    price: Price,
    quantity: Quantity,
    side: OrderSide,
) -> Decimal:
    """Return one leg's worst-case settlement cost for budget reservation."""
    fee = calculator.calculate(
        contract_id,
        price,
        quantity,
        side,
    ).settlement_cost.amount
    if side is OrderSide.BUY:
        return price.value * quantity.value + fee
    return (Decimal("1") - price.value) * quantity.value + fee


def _economic_route(
    contracts: tuple[BinaryContract, BinaryContract],
    side: OrderSide,
) -> _EconomicRoute:
    """Describe one two-venue route without relying on venue outcome labels."""
    return side, tuple(
        sorted(
            (
                str(contract.venue_id),
                str(contract.market_id),
                str(contract.id),
            )
            for contract in contracts
        ),
    )


def _routes_are_equivalent(
    first: _EconomicRoute,
    second: _EconomicRoute,
) -> bool:
    """Return whether two plans share a pair or complementary economic route."""
    first_side, first_legs = first
    second_side, second_legs = second
    if tuple(leg[:2] for leg in first_legs) != tuple(
        leg[:2] for leg in second_legs
    ):
        return False
    first_contracts = tuple(leg[2] for leg in first_legs)
    second_contracts = tuple(leg[2] for leg in second_legs)
    if first_contracts == second_contracts:
        return True
    return first_side is not second_side and all(
        left != right
        for left, right in zip(first_contracts, second_contracts, strict=True)
    )


def _market_allowed(market: MonitoredMarket, config: EngineConfig) -> bool:
    """Apply recurring-cycle allowlists without restricting explicit candidates."""
    return not isinstance(market, MarketCycle) or (
        market.underlying.symbol in config.allowed_underlyings
        and market.interval_seconds in config.allowed_intervals_seconds
    )


def _market_has_time_remaining(
    market: MonitoredMarket,
    pair: MatchedContractPair,
    config: EngineConfig,
) -> bool:
    """Return whether a scheduled expiry can safely admit a new plan.

    Notes
    -----
    - Regular candidates use fresh executable books as the authoritative
      tradability signal because sports timestamps can represent kickoff rather
      than venue close.
    """
    if isinstance(market, RegularCandidate):
        return True
    minimum = config.min_market_time_remaining_seconds
    if minimum is not None:
        minimum = market_expiry_guard_seconds(market, minimum)
    return minimum is None or pair.ends_at > Timestamp.now() + timedelta(
        seconds=minimum,
    )


def _opportunity_fingerprint(opportunity: ArbitrageOpportunity) -> tuple[object, ...]:
    return (
        opportunity.side,
        opportunity.left_level.price.value,
        opportunity.left_level.quantity.value,
        opportunity.right_level.price.value,
        opportunity.right_level.quantity.value,
        opportunity.net_edge,
    )


def _opportunity_id(
    underlying: str,
    opportunity: ArbitrageOpportunity,
    left_book: OrderBook,
    right_book: OrderBook,
) -> str:
    """Identify an economic opportunity at one pair of source timestamps."""
    value = "|".join(
        (
            underlying,
            str(opportunity.left_contract_id),
            str(opportunity.right_contract_id),
            opportunity.side.value,
            str(opportunity.left_level.price.value.normalize()),
            str(opportunity.left_level.quantity.value.normalize()),
            str(opportunity.right_level.price.value.normalize()),
            str(opportunity.right_level.quantity.value.normalize()),
            str(opportunity.net_edge.normalize()),
            str(left_book.timestamp),
            str(right_book.timestamp),
        ),
    )
    return hashlib.sha256(value.encode()).hexdigest()[:32]


def _common_step(steps: tuple[Decimal, ...]) -> Decimal:
    """Return the smallest decimal grid divisible by every lot step."""
    places = max(max(0, -step.as_tuple().exponent) for step in steps)
    scale = 10**places
    units = [int(step * scale) for step in steps]
    return Decimal(math.lcm(*units)) / Decimal(scale)


def _round_down(value: Decimal, step: Decimal) -> Decimal:
    return (value / step).to_integral_value(rounding=ROUND_FLOOR) * step
