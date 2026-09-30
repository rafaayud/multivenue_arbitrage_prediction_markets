"""Dispatch paired orders concurrently and recover requests.

Responsibilities
----------------
- Prepare venue requests and preflight their funding concurrently.
- Guard paired legs and submit both live legs concurrently.
- Fetch current in-memory recovery books before central route selection.
- Reject recurring-market submissions too close to matched-market expiry.
- Retry transient Polymarket recovery submissions while fresh inventory settles.
- Monitor accepted or uncertain orders and reconcile terminal fees.
- Recover unfinished commands from the durable journal.

Notes
-----
- The paired preparation and submission body is moved unchanged from
  :mod:`pipeline`.
"""

import asyncio
import time
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import replace
from datetime import timedelta
from uuid import uuid4

from prediction_markets.application.events import (
    ExecutionPreparationRequested,
    ArbitrageOpportunityFound,
    OpportunityValidationRef,
    ExecutionUpdated,
    OrderBookUpdated,
    OrderPrepared,
    OrderCancellationPrepared,
    OrderSnapshotUpdated,
    PreparedExecutionBatch,
    RecoveryBooksReceived,
    RecoveryPlanningRequested,
    SubmissionReceived,
    SubmitOrder,
    TradingSafetyStop,
    prepared_execution_events,
)
from prediction_markets.application.freshness import (
    SOURCE_BOOK_MAX_AGE_MS,
    SUBMISSION_BOOK_MAX_AGE_MS,
    source_age_guard_enabled,
)
from prediction_markets.application.execution.accounting import is_settled_order
from prediction_markets.infrastructure.observability.predict_fill_study import (
    observe_cancel,
    observe_book_checkpoint,
    observe_event as observe_fill_study,
    observe_execution_window,
)
from prediction_markets.infrastructure.observability.execution_timing import (
    ThreadCallTiming, timed_to_thread,
)
from prediction_markets.application.worker_validation import (
    WorkerOpportunityValidationRequest,
    WorkerOpportunityValidationResult,
    WorkerValidationReason,
)
from prediction_markets.application.recovery_books import RECOVERY_BOOKS_TIMEOUT_NS
from prediction_markets.application.markets.models import market_expiry_guard_seconds
from prediction_markets.application.pipeline.buffers import (
    EventSink,
    JournalPort,
    JournalRecord,
    PipelineOutput,
    RingBuffer,
)
from prediction_markets.application.state import TradingState
from prediction_markets.domain.market_matching.value_objects import RegularCandidate
from prediction_markets.domain.orderbook.entities import OrderBook
from prediction_markets.domain.ports.execution import ExecutionPort, OrderUpdatePort
from prediction_markets.domain.ports.market_data import MarketDataPort
from prediction_markets.domain.shared.value_objects import (
    ClientOrderID,
    ContractID,
    Timestamp,
    VenueID,
)
from prediction_markets.domain.trading.entities import (
    ArbitrageExecutionJournal,
    OrderSnapshot,
)
from prediction_markets.domain.trading.enums import (
    ArbitrageExecutionStatus,
    OrderSide,
    ReconciliationStatus,
    RecoveryStatus,
    SubmissionStatus,
    TimeInForce,
)
from prediction_markets.domain.trading.value_objects import (
    OrderReference,
    PreparedOrder,
    ReconciliationResult,
    SubmissionResult,
)
from prediction_markets.infrastructure.metrics import (
    ACTIVE_RESTING_ORDERS,
    ARBITRAGE_EDGE_SURVIVAL,
    ORDER_CANCEL_ATTEMPTS,
    ORDER_LATENCY,
    ORDER_RESTING_DURATION,
)

_RECOVERY_SUBMISSION_ATTEMPT_LIMIT = 15
_RECOVERY_SUBMISSION_RETRY_SECONDS = 2
_RECOVERY_FRESH_BOOK_WAIT_SECONDS = 1.0
_RECOVERY_BOOK_POLL_SECONDS = 0.02
_SETTLEMENT_CONFIRMATION_SECONDS = 3.0
_CHAIN_CANCELLATION_CONFIRMATION_SECONDS = 10.0
_MIN_MARKET_TIME_REMAINING_SECONDS = 120
_SOURCE_BOOK_MAX_AGE_NS = SOURCE_BOOK_MAX_AGE_MS * 1_000_000
_WORKER_VALIDATION_TIMEOUT_SECONDS = 0.025


def _local_polymarket_buy_reprice_books(
    request: ExecutionPreparationRequested,
    state: TradingState,
) -> tuple[OrderBook, OrderBook] | None:
    """Return current books when a local Polymarket BUY fully improved.

    Parameters
    ----------
    request
        Staged single-process execution that has not been signed or journaled.
    state
        Parent state containing the latest normalized books.

    Returns
    -------
    tuple[OrderBook, OrderBook] | None
        Pair books for central replanning, or ``None`` when no full-depth
        Polymarket price improvement exists or worker validation owns the pair.
    """
    if (
        request.opportunity.validation_ref is not None
        or request.opportunity.opportunity.side is not OrderSide.BUY
    ):
        return None
    pair = request.opportunity.pair
    left = state.books.get(pair.left.id)
    right = state.books.get(pair.right.id)
    if left is None or right is None:
        return None
    books = (left, right)
    by_contract = {command.intent.contract_id: command for command in request.commands}
    for contract, book in zip((pair.left, pair.right), books, strict=True):
        command = by_contract.get(contract.id)
        if (
            command is None
            or str(command.venue_id).lower() != "polymarket"
            or command.intent.limit_price is None
        ):
            continue
        remaining = command.intent.quantity.value
        for level in book.asks:
            remaining -= level.quantity.value
            if remaining <= 0:
                if level.price.value < command.intent.limit_price.value:
                    return books
                break
    return None


class OutputDispatcher:
    """Prepare, journal, submit, and observe venue orders outside the engine.

    Notes
    -----
    - Live venue submissions for one pair are concurrent after the shared guard.
    - If one leg fails, an open peer is cancelled and reconciled.
    - Optional adapter funding checks finish before either venue is submitted.
    - Live command pairs pass one shared in-memory orderbook guard after both
      requests are prepared and before either is submitted.
    - A rejected or terminal zero-fill hedge cancels any persistent peer,
      including adapter-declared application-timed orders.
    - Recurring-market submissions use interval-specific expiry windows before
      close. Regular candidates instead rely on the shared fresh-book guard
      because venue event timestamps may represent kickoff.
    - The prepared venue request remains journaled before its network submission.
    """

    def __init__(
        self,
        outputs: RingBuffer[PipelineOutput],
        sink: EventSink,
        journal: JournalPort,
        state: TradingState,
        on_error: Callable[[BaseException], None] | None = None,
        *,
        commit_prepared_execution: Callable[[PreparedExecutionBatch], None]
        | None = None,
        abort_staged_execution: Callable[[ArbitrageExecutionJournal], None]
        | None = None,
        reprice_staged_execution: Callable[
            [ExecutionPreparationRequested, OrderBook, OrderBook],
            ExecutionPreparationRequested | None,
        ] | None = None,
        guard_recovery: Callable[[SubmitOrder, OrderBook], str | None] | None = None,
        max_book_age_ms: int = SUBMISSION_BOOK_MAX_AGE_MS,
        enforce_source_age: bool | None = None,
    ) -> None:
        """
        Parameters
        ----------
        outputs
            Live order commands produced in primary-and-hedge pairs.
        sink
            Input sink receiving normalized submission outcomes.
        journal
            Durable journal written before external submission.
        state
            Current in-memory books and execution state.
        on_error
            Optional callback for asynchronous submission failures.
        max_book_age_ms : int, default=500
            Maximum age accepted for both books immediately before dispatch.
        commit_prepared_execution
            Applies a journaled execution batch to the owning engine.
        abort_staged_execution
            Releases engine reservations if uncommitted preparation is cancelled
            or the batch cannot be journaled.
        reprice_staged_execution
            Repeats central admission and risk for an unjournaled replacement
            using worker books, without extending its deadline.
        guard_recovery
            Rechecks recovery depth economics against central loss limits after signing.
        enforce_source_age
            Whether venue wall-clock age can reject submission. ``None`` uses
            the process-level rollout setting.

        Raises
        ------
        ValueError
            If the maximum book age is not positive.
        """
        if max_book_age_ms <= 0:
            raise ValueError("book age must be positive")
        self._outputs = outputs
        self._sink = sink
        self._journal = journal
        self._state = state
        self._on_error = on_error
        self._commit_prepared_execution = commit_prepared_execution
        self._abort_staged_execution = abort_staged_execution
        self._reprice_staged_execution = reprice_staged_execution
        self._guard_recovery = guard_recovery
        self._started_recovery_commands: set[ClientOrderID] = set()
        self._staged_requests: dict[str, ExecutionPreparationRequested] = {}
        self._worker_validator: Callable[
            [WorkerOpportunityValidationRequest], Awaitable[WorkerOpportunityValidationResult]
        ] | None = None
        self._worker_capture: Callable[..., None] | None = None
        self._worker_recovery_books: Callable[
            [str, OpportunityValidationRef, tuple[ContractID, ContractID]],
            Awaitable[tuple[OrderBook, OrderBook]],
        ] | None = None
        self._max_book_age_ns = max_book_age_ms * 1_000_000
        self._enforce_source_age = (
            source_age_guard_enabled()
            if enforce_source_age is None
            else enforce_source_age
        )
        self._worker_book_age_observer: (
            Callable[[str, VenueID, ContractID, str, int, int], None] | None
        ) = None
        self._execution: dict[VenueID, ExecutionPort] = {}
        self._updates: dict[VenueID, OrderUpdatePort] = {}
        self._market_data: dict[VenueID, MarketDataPort] = {}
        self._submissions: set[asyncio.Task[None]] = set()
        self._watchers: set[asyncio.Task[None]] = set()
        self._cancellation_locks: dict[VenueID, asyncio.Lock] = {}

    def set_worker_book_age_observer(
        self,
        observer: Callable[
            [str, VenueID, ContractID, str, int, int],
            None,
        ]
        | None,
    ) -> None:
        """Attach parent guard and submission age telemetry for worker books."""
        self._worker_book_age_observer = observer

    def set_worker_validation(
        self,
        validator: Callable[
            [WorkerOpportunityValidationRequest], Awaitable[WorkerOpportunityValidationResult]
        ] | None,
        *,
        capture: Callable[..., None] | None = None,
        recovery_books: Callable[
            [str, OpportunityValidationRef, tuple[ContractID, ContractID]],
            Awaitable[tuple[OrderBook, OrderBook]],
        ] | None = None,
    ) -> None:
        """Attach bounded worker IPC checks and optional non-blocking diagnostics.

        Notes
        -----
        - Worker-originated pairs fail closed when the validator is unavailable.
        - Single-process pairs without worker provenance keep their local guard.
        """
        self._worker_validator = validator
        self._worker_capture = capture
        self._worker_recovery_books = recovery_books

    async def _current_recovery_books(
        self, execution: ArbitrageExecutionJournal,
    ) -> tuple[OrderBook, OrderBook]:
        """Read the owning worker or local memory without refreshing clocks or using REST.

        Raises
        ------
        RuntimeError
            If the execution changed, or the worker or identified books are unavailable.
        TimeoutError
            If serial worker lookups exhaust the one-second retry budget.

        Notes
        -----
        - Only IPC timeouts retry; each request retains its 100 ms deadline.
          Retries neither submit orders nor consume recovery order attempts.
        - The fresh-quote caller's enclosing deadline still bounds nested reads.
        """
        contracts = (execution.leg1_contract_id, execution.leg2_contract_id)
        reference = self._state.execution_worker_refs.get(execution.id)
        if reference is not None:
            if self._worker_recovery_books is None:
                raise RuntimeError("worker recovery books unavailable")
            deadline = asyncio.timeout(_RECOVERY_FRESH_BOOK_WAIT_SECONDS)
            try:
                async with deadline:
                    while True:
                        if (self._state.executions.get(execution.id) != execution
                                or execution.id in self._state.execution_safety_stops
                                or self._state.execution_worker_refs.get(execution.id) != reference):
                            raise RuntimeError("recovery changed during book lookup")
                        try:
                            books = await asyncio.wait_for(
                                self._worker_recovery_books(execution.id, reference, contracts),
                                RECOVERY_BOOKS_TIMEOUT_NS / 1_000_000_000,
                            )
                        except TimeoutError:
                            await asyncio.sleep(_RECOVERY_BOOK_POLL_SECONDS)
                            continue
                        break
            except TimeoutError as cause:
                if not deadline.expired():
                    raise
                raise TimeoutError("recovery book lookup wait expired") from cause
            if (self._state.executions.get(execution.id) != execution
                    or execution.id in self._state.execution_safety_stops
                    or self._state.execution_worker_refs.get(execution.id) != reference):
                raise RuntimeError("recovery changed during book lookup")
        else:
            books = tuple(self._state.books.get(contract_id) for contract_id in contracts)
        if len(books) != 2:
            raise RuntimeError("recovery book pair unavailable")
        for contract_id, book in zip(contracts, books, strict=True):
            contract = self._state.contracts.get(contract_id)
            if (contract is None or book is None or book.market_id != contract.market_id
                    or book.outcome_id != contract.outcome_id or book.received_at_ns is None
                    or book.received_at_ns > time.monotonic_ns()):
                raise RuntimeError("recovery book identity or clock mismatch")
        return books

    async def _request_recovery_plan(self, request: RecoveryPlanningRequested) -> None:
        """Poll current books within the engine's unchanged quote-wait deadline.

        Notes
        -----
        - Delayed polls do not allocate order attempts or trigger REST requests.
        - One serial read checks both routes. Cancellation and changed execution
          authority stop the wait before another read can authorize a plan.
        """
        execution = request.execution
        if self._state.executions.get(execution.id) != execution:
            return
        contracts = (execution.leg1_contract_id, execution.leg2_contract_id)
        reference = self._state.execution_worker_refs.get(execution.id)
        if request.not_before_ns is None:
            observe_execution_window(execution.id, tuple(map(str, contracts)), reason="recovery")
            if reference is not None and self._worker_capture is not None:
                try:
                    self._worker_capture(execution.id, reference, contracts, reason="recovery")
                except Exception:
                    pass  # Diagnostic capture cannot decide whether recovery is allowed.
        wait = asyncio.timeout(
            None if request.deadline_at_ns is None
            else max(0, request.deadline_at_ns - time.monotonic_ns()) / 1_000_000_000,
        )
        try:
            async with wait:
                if request.not_before_ns is not None:
                    await asyncio.sleep(max(0, request.not_before_ns - time.monotonic_ns()) / 1_000_000_000)
                if (self._state.executions.get(execution.id) != execution
                        or execution.id in self._state.execution_safety_stops
                        or self._state.execution_worker_refs.get(execution.id) != reference):
                    raise RuntimeError("recovery changed during quote wait")
                books = await self._current_recovery_books(execution)
        except Exception as error:
            reason = (
                "recovery quote wait expired: no fresh route satisfied freshness, liquidity and loss limits"
                if wait.expired()
                else f"recovery book refresh failed: {type(error).__name__}: {error}"
            )
            await self._sink.publish(RecoveryBooksReceived(request, error=reason))
            return
        now_ns, now_wall_ns = time.monotonic_ns(), time.time_ns()
        fresh_contract_ids = set()
        for client_order_id, book in zip(
            (execution.leg1_client_order_id, execution.leg2_client_order_id), books, strict=True,
        ):
            command = self._state.commands.get(client_order_id)
            if command is not None:
                observe_book_checkpoint(command, book, phase="recovery_planning", book_origin="in_memory")
                if _book_freshness_error(
                    command, book, self._max_book_age_ns, now_ns, now_wall_ns,
                    enforce_source_age=self._enforce_source_age,
                ) is None:
                    fresh_contract_ids.add(command.intent.contract_id)
        await self._sink.publish(RecoveryBooksReceived(
            request, books, fresh_contract_ids=frozenset(fresh_contract_ids),
        ))

    async def _fresh_recovery_book(
        self, command: SubmitOrder, execution: ArbitrageExecutionJournal,
        book: OrderBook | None,
    ) -> OrderBook:
        """Wait up to one second for a usable quote without consuming a requote.

        Notes
        -----
        - Only stale recovery quotes wait; fresh quotes return immediately.
        - Serial, bounded IPC reads preserve original book timestamps. There is
          no REST refresh, age-limit relaxation, or order submission here.
        - Cancellation, worker failure, or changed execution stops the wait.
        """
        error = None
        deadline = asyncio.timeout(_RECOVERY_FRESH_BOOK_WAIT_SECONDS)
        try:
            async with deadline:
                while True:
                    recovery = self._state.recoveries.get(execution.id)
                    if (self._state.executions.get(execution.id) != execution
                            or execution.id in self._state.execution_safety_stops
                            or recovery is None or recovery.status is not RecoveryStatus.PENDING
                            or recovery.client_order_id != command.intent.client_order_id):
                        raise RuntimeError("recovery changed while waiting for a fresh book")
                    error = _book_freshness_error(
                        command, book, self._max_book_age_ns, time.monotonic_ns(),
                        enforce_source_age=self._enforce_source_age,
                    )
                    if error is None:
                        assert book is not None
                        return book
                    await asyncio.sleep(_RECOVERY_BOOK_POLL_SECONDS)
                    books = await self._current_recovery_books(execution)
                    for contract_id, latest in zip(
                        (execution.leg1_contract_id, execution.leg2_contract_id), books, strict=True,
                    ):
                        cached = self._state.books.get(contract_id)
                        if cached is None or (cached.received_at_ns or 0) <= latest.received_at_ns:
                            contract = self._state.contracts[contract_id]
                            self._state.apply(OrderBookUpdated(contract.venue_id, contract_id, latest))
                    book = self._state.books.get(command.intent.contract_id)
        except TimeoutError as cause:
            if not deadline.expired():
                raise
            raise TimeoutError(f"recovery fresh-book wait expired: {error}") from cause

    def _capture_commands(
        self,
        commands: tuple[SubmitOrder, ...],
        reference: OpportunityValidationRef | None = None,
    ) -> None:
        """Request selective diagnostic windows without waiting for persistence."""
        execution_id = commands[0].execution_id
        contracts = tuple(command.intent.contract_id for command in commands)
        reason = "recovery" if commands[0].role == "recovery" else "execution"
        observe_execution_window(execution_id, tuple(map(str, contracts)), reason=reason)
        reference = reference or self._state.execution_worker_refs.get(execution_id)
        if reference is not None and self._worker_capture is not None:
            try:
                self._worker_capture(execution_id, reference, contracts, reason=reason)
            except Exception:
                # Diagnostic failures must not authorize or reject a venue command.
                return

    async def _validate_worker_pair(
        self,
        commands: tuple[SubmitOrder, SubmitOrder],
        *,
        opportunity: ArbitrageOpportunityFound | None = None,
        deadline_at_ns: int | None = None,
        phase: str = "validation_preparation",
    ) -> WorkerOpportunityValidationResult | None:
        """Fetch current worker books, preserving identity, clocks, and deadlines.

        Returns
        -------
        WorkerOpportunityValidationResult | None
            Accepted/reprice evidence, or ``None`` for a single-process pair.

        Raises
        ------
        RuntimeError
            If worker evidence is unavailable, invalid, stale, or late.
        """
        execution_id = commands[0].execution_id
        reference = (opportunity.validation_ref if opportunity is not None else
                     self._state.execution_worker_refs.get(execution_id))
        if reference is None:
            return None
        if self._worker_validator is None:
            raise RuntimeError("worker validation unavailable")
        cycle = opportunity.cycle if opportunity is not None else self._state.execution_cycles.get(execution_id)
        pair = opportunity.pair if opportunity is not None else next(
            (candidate for candidate in self._state.matches.get(cycle, ())
             if candidate.key == self._state.execution_pairs.get(execution_id)), None,
        )
        by_contract = {command.intent.contract_id: command for command in commands}
        if pair is None or set(by_contract) != {pair.left.id, pair.right.id}:
            raise RuntimeError("worker validation identity mismatch")
        left, right = by_contract[pair.left.id], by_contract[pair.right.id]
        if (left.intent.side is not right.intent.side or left.intent.limit_price is None
                or right.intent.limit_price is None or right.execution_id != execution_id):
            raise RuntimeError("worker validation identity mismatch")
        started = time.monotonic_ns()
        validation_deadline = started + int(_WORKER_VALIDATION_TIMEOUT_SECONDS * 1_000_000_000)
        if deadline_at_ns is not None:
            validation_deadline = min(validation_deadline, deadline_at_ns)
        if validation_deadline <= started:
            raise RuntimeError("pre-submission deadline expired before worker validation")
        request = WorkerOpportunityValidationRequest(
            uuid4().hex, execution_id, reference, cycle, pair, left.intent.side,
            left.intent.quantity, right.intent.quantity,
            left.intent.limit_price, right.intent.limit_price, time.time_ns(),
            self._max_book_age_ns, self._enforce_source_age,
            validation_deadline_at_ns=validation_deadline,
        )
        try:
            result = await asyncio.wait_for(
                self._worker_validator(request),
                max(0, validation_deadline - time.monotonic_ns()) / 1_000_000_000,
            )
        except TimeoutError as error:
            raise RuntimeError("worker validation timeout") from error
        finally:
            timings = self._state.timings.get(execution_id)
            if timings is not None:
                timings.worker_validation_count += 1
                timings.worker_validation_ns += time.monotonic_ns() - started
        if not isinstance(result, WorkerOpportunityValidationResult) or result.request != request:
            raise RuntimeError("worker validation identity mismatch")
        captured_mono_ns, captured_wall_ns = time.monotonic_ns(), time.time_ns()
        for command, book, generation in (
            (left, result.left_order_book, result.left_book_generation),
            (right, result.right_order_book, result.right_book_generation),
        ):
            observe_book_checkpoint(command, book, phase=phase,
                validation=result, book_generation=generation,
                monotonic_at_ns=captured_mono_ns, wall_at_ns=captured_wall_ns)
        if result.reason not in {WorkerValidationReason.ACCEPTED, WorkerValidationReason.REPRICE}:
            raise RuntimeError(f"worker validation {result.reason.value}")
        if (not request.sent_monotonic_at_ns <= result.responded_monotonic_at_ns <= time.monotonic_ns()
                or (deadline_at_ns is not None and time.monotonic_ns() > deadline_at_ns)):
            raise RuntimeError("worker validation stale response")
        for contract, book in ((pair.left, result.left_order_book), (pair.right, result.right_order_book)):
            if (book is None or book.market_id != contract.market_id
                    or book.outcome_id != contract.outcome_id or book.received_at_ns is None):
                raise RuntimeError("worker validation book identity mismatch")
        # No await separates these updates; the parent sees one complete pair.
        for contract, book in ((pair.left, result.left_order_book), (pair.right, result.right_order_book)):
            current = self._state.books.get(contract.id)
            if current is None or (current.received_at_ns or 0) <= book.received_at_ns:
                self._state.apply(OrderBookUpdated(contract.venue_id, contract.id, book))
        return result

    def configure(
        self,
        execution: Mapping[VenueID, ExecutionPort],
        updates: Mapping[VenueID, OrderUpdatePort] | None = None,
        market_data: Mapping[VenueID, MarketDataPort] | None = None,
    ) -> None:
        """Replace venue adapters used by subsequent commands.

        Parameters
        ----------
        execution
            Order preparation, submission, and reconciliation adapters.
        updates
            Optional private order-update adapters.
        market_data
            Optional snapshot adapters used to refresh recovery order books.
        """
        self._execution = dict(execution)
        self._updates = dict(updates or {})
        self._market_data = dict(market_data or {})

    async def run(self) -> None:
        """Validate and schedule paired live commands until cancelled."""
        pending: dict[str, SubmitOrder] = {}
        while True:
            output = await self._outputs.get()
            if isinstance(output, RecoveryPlanningRequested):
                task = asyncio.create_task(
                    self._request_recovery_plan(output),
                    name=f"recovery-books:{output.execution.id}",
                )
                self._submissions.add(task)
                task.add_done_callback(self._single_submission_done)
                continue
            if isinstance(output, ExecutionPreparationRequested):
                for command in output.commands:
                    self._mark_command_received(command)
                task = asyncio.create_task(
                    self._dispatch_prepared_execution(output),
                    name=f"order-dispatch:{output.planned.execution.id}",
                )
                self._submissions.add(task)
                task.add_done_callback(self._batched_submission_done)
                continue
            command = output
            self._mark_command_received(command)
            if command.role == "recovery":
                task = asyncio.create_task(
                    self._submit_single(command),
                    name=f"recovery-submit:{command.execution_id}",
                )
                self._submissions.add(task)
                task.add_done_callback(self._single_submission_done)
                continue
            peer = pending.pop(command.execution_id, None)
            if peer is None:
                pending[command.execution_id] = command
                continue
            commands = (peer, command)
            task = asyncio.create_task(
                self._dispatch_pair(commands),
                name=f"order-dispatch:{commands[0].execution_id}",
            )
            self._submissions.add(task)
            task.add_done_callback(self._pair_submission_done)

    def _mark_command_received(self, command: SubmitOrder) -> None:
        """Record one output-ring handoff without branching the run loop."""
        command_received_ns = time.monotonic_ns()
        timings = self._state.timings.get(command.execution_id)
        if timings is not None and command.role in {"primary", "hedge"}:
            timings.mark_command_received(
                command.role,
                str(command.venue_id),
                command_received_ns,
            )

    async def _dispatch_prepared_execution(
        self,
        request: ExecutionPreparationRequested,
    ) -> None:
        """Release uncommitted admission reservations if preparation is cancelled."""
        execution_id = request.planned.execution.id
        self._staged_requests[execution_id] = request
        try:
            await self._prepare_and_dispatch_execution(request)
        except BaseException:
            latest = self._staged_requests[execution_id]
            if (
                execution_id not in self._state.executions
                and self._abort_staged_execution is not None
            ):
                self._abort_staged_execution(latest.planned.execution)
            raise
        finally:
            self._staged_requests.pop(execution_id, None)

    async def _prepare_and_dispatch_execution(
        self,
        request: ExecutionPreparationRequested,
    ) -> None:
        """Prepare and persist one complete execution before any submission.

        Notes
        -----
        - Single-process Polymarket BUY improvements are replanned before
          signing so stale FAK notional cannot buy avoidable excess shares.
        """
        commands = request.commands
        observe_fill_study(request)
        prepared_pair: list[tuple[ExecutionPort, PreparedOrder]] = []
        prepared_events: tuple[OrderPrepared, ...] = ()
        error: BaseException | None = None
        rejection_reason: str | None = None
        if self._state.safety_halted:
            rejection_reason = (
                self._state.last_error or "live trading halted by venue safety circuit"
            )
        elif time.monotonic_ns() > request.deadline_at_ns:
            rejection_reason = "pre-submission deadline expired before preparation"
        else:
            local_books = _local_polymarket_buy_reprice_books(request, self._state)
            if local_books is not None:
                revised = (
                    self._reprice_staged_execution(request, *local_books)
                    if self._reprice_staged_execution is not None
                    else None
                )
                if revised is None:
                    rejection_reason = "local Polymarket reprice rejected by central admission or risk"
                else:
                    request, commands = revised, revised.commands
                    self._staged_requests[request.planned.execution.id] = request
                    observe_fill_study(request)
            self._capture_commands(commands, request.opportunity.validation_ref)
            if rejection_reason is None:
                results = await asyncio.gather(
                    *(self._prepare(command, journal_prepared=False) for command in commands),
                    return_exceptions=True,
                )
                collected_events: list[OrderPrepared] = []
                for command, result in zip(commands, results, strict=True):
                    if isinstance(result, BaseException):
                        error = error or result
                        continue
                    adapter, prepared = result
                    prepared_pair.append((adapter, prepared))
                    collected_events.append(OrderPrepared(command, prepared))
                prepared_events = tuple(collected_events)
                if error is not None:
                    rejection_reason = f"pre-submission preparation failed: {error}"
                elif self._state.safety_halted:
                    rejection_reason = (
                        self._state.last_error
                        or "live trading halted by venue safety circuit"
                    )
                elif time.monotonic_ns() > request.deadline_at_ns:
                    rejection_reason = "pre-submission deadline expired after preparation"

        if rejection_reason is None:
            try:
                validation = await self._validate_worker_pair(
                    commands, opportunity=request.opportunity, deadline_at_ns=request.deadline_at_ns,
                )
                if validation is not None and validation.reason is WorkerValidationReason.REPRICE:
                    revised = (self._reprice_staged_execution(
                        request, validation.left_order_book, validation.right_order_book,
                    ) if self._reprice_staged_execution is not None else None)
                    if revised is None:
                        raise RuntimeError("worker reprice rejected by central admission or risk")
                    request, commands = revised, revised.commands
                    self._staged_requests[request.planned.execution.id] = request
                    observe_fill_study(request)
                    reprepare_started = time.monotonic_ns()
                    try:
                        results = await asyncio.gather(
                            *(self._prepare(command, journal_prepared=False) for command in commands),
                            return_exceptions=True,
                        )
                    finally:
                        if (timings := self._state.timings.get(commands[0].execution_id)) is not None:
                            timings.worker_reprepare_ns += time.monotonic_ns() - reprepare_started
                    prepared_pair, collected_events = [], []
                    for command, result in zip(commands, results, strict=True):
                        if isinstance(result, BaseException):
                            error = error or result
                        else:
                            adapter, prepared = result
                            prepared_pair.append((adapter, prepared))
                            collected_events.append(OrderPrepared(command, prepared))
                    prepared_events = tuple(collected_events)
                    if error is not None:
                        raise RuntimeError(f"worker reprice preparation failed: {error}")
                if time.monotonic_ns() > request.deadline_at_ns:
                    raise RuntimeError("pre-submission deadline expired after worker validation")
            except Exception as validation_error:
                rejection_reason = str(validation_error)

        batch = PreparedExecutionBatch(
            request.opportunity,
            request.planned,
            commands,
            prepared_events,
            request.deadline_wall_at_ns,
            rejection_reason,
        )
        timings = self._state.timings.get(commands[0].execution_id)
        journal_call = ThreadCallTiming() if timings is not None else None
        if timings is not None:
            for prepared in prepared_events:
                timings.journal_calls.setdefault(prepared.command.role, journal_call)
        await timed_to_thread(journal_call, self._journal.append, batch)
        if self._commit_prepared_execution is None:
            for event in prepared_execution_events(batch):
                self._state.apply(event)
        else:
            self._commit_prepared_execution(batch)
        journaled_at_ns = time.monotonic_ns()
        timings = self._state.timings.get(commands[0].execution_id)
        if timings is not None:
            for prepared in prepared_events:
                timings.mark_journaled(prepared.command.role, journaled_at_ns)

        if rejection_reason is not None:
            await self.reject_pair(commands, rejection_reason)
            return
        if len(prepared_pair) != 2:
            raise RuntimeError("Prepared execution batch lost one order payload")
        await self._guard_and_submit(
            commands,
            prepared_pair,
            deadline_at_ns=request.deadline_at_ns,
        )

    async def _dispatch_pair(
        self,
        commands: tuple[SubmitOrder, SubmitOrder],
    ) -> None:
        """Prepare, guard, and submit one engine-admitted live pair.

        Notes
        -----
        - Different pairs may prepare concurrently.
        - Both legs are submitted concurrently after the parent book guard.
        """
        self._capture_commands(commands)
        if self._state.safety_halted:
            await self.reject_pair(
                commands,
                self._state.last_error or "live trading halted by venue safety circuit",
            )
            return
        try:
            prepared_pair = await self._prepare_pair(commands)
        except Exception as error:
            reason = f"pre-submission preparation failed: {error}"
            await self.reject_pair(commands, reason)
            return
        await self._guard_and_submit(commands, prepared_pair)

    async def _guard_and_submit(
        self,
        commands: tuple[SubmitOrder, SubmitOrder],
        prepared_pair: list[tuple[ExecutionPort, PreparedOrder]],
        *,
        deadline_at_ns: int | None = None,
    ) -> None:
        """Validate worker state after durability, then guard and submit the pair."""
        try:
            validation = await self._validate_worker_pair(commands, deadline_at_ns=deadline_at_ns,
                phase="validation_final")
            if validation is not None and validation.reason is WorkerValidationReason.REPRICE:
                raise RuntimeError("worker reprice limit reached before submission")
        except Exception as validation_error:
            await self.reject_pair(commands, str(validation_error))
            return
        timings = self._state.timings.get(commands[0].execution_id)
        guard_checked_ns = time.monotonic_ns()
        guard_wall_at_ns = time.time_ns()
        for queued in commands:
            observe_book_checkpoint(queued, self._state.books.get(queued.intent.contract_id),
                phase="guard", monotonic_at_ns=guard_checked_ns, wall_at_ns=guard_wall_at_ns)
            self._observe_worker_book_age(
                queued,
                "guard",
                guard_checked_ns,
                guard_wall_at_ns,
            )
        guard_error = (
            "pre-submission deadline expired"
            if deadline_at_ns is not None and guard_checked_ns > deadline_at_ns
            else _submission_guard_error(
                commands,
                self._state,
                self._max_book_age_ns,
                guard_checked_ns,
                self._enforce_source_age,
            )
        )
        _observe_edge_survival(
            self._state,
            commands[0].execution_id,
            guard_checked_ns
            - min(
                timings.book_received_at_ns.values(),
                default=guard_checked_ns,
            )
            if timings is not None
            else 0,
            guard_error is None,
        )
        if guard_error is None:
            guard_error = _market_expiry_guard_error(
                commands,
                self._state,
                _MIN_MARKET_TIME_REMAINING_SECONDS,
            )
        guard_finished_ns = time.monotonic_ns()
        if timings is not None:
            timings.mark_guard(
                {
                    queued.role: (
                        book.received_at_ns
                        if (book := self._state.books.get(
                            queued.intent.contract_id,
                        ))
                        else None
                    )
                    for queued in commands
                },
                guard_checked_ns,
                guard_finished_ns,
                guard_error,
            )
        if guard_error is not None:
            await self.reject_pair(commands, guard_error)
            return
        if self._state.safety_halted:
            await self.reject_pair(
                commands,
                self._state.last_error or "live trading halted by venue safety circuit",
            )
            return
        await self._submit_pair(commands, prepared_pair)

    async def reject_pair(
        self,
        commands: tuple[SubmitOrder, SubmitOrder],
        reason: str,
    ) -> None:
        """
        Publish terminal local rejections for both unsubmitted legs.

        Parameters
        ----------
        commands
            Paired primary and hedge commands that were not submitted.
        reason
            Terminal local rejection reason persisted in the event stream.
        """
        for command in commands:
            await self._sink.publish(
                SubmissionReceived(
                    command,
                    SubmissionResult(
                        SubmissionStatus.REJECTED,
                        OrderReference(
                            command.venue_id,
                            command.intent.client_order_id,
                            b"local-pre-submission-rejection",
                        ),
                        reason=reason,
                    ),
                ),
            )

    async def _submit_single(self, command: SubmitOrder) -> None:
        """Prepare, guard, and submit one independent recovery command.

        Notes
        -----
        - Central recovery planning already used current in-memory books. Worker
          books are checked again after signing without any venue REST request.
        - Legacy commands without a tracked execution retain snapshot refresh.
        - Tracked orders return definitive venue rejections to central replanning;
          only legacy commands retain balance-settlement retries of the same payload.
        - Uncertain responses never authorize a replacement order.
        - Stale quotes wait for a bounded in-memory update before rejection;
          reading the same old worker snapshot does not exhaust all requotes.
        """
        execution = self._state.executions.get(command.execution_id)
        if execution is not None:
            recovery = self._state.recoveries.get(execution.id)
            if (execution.status is not ArbitrageExecutionStatus.RECOVERY_PENDING
                    or execution.id in self._state.execution_safety_stops
                    or recovery is None or recovery.status is not RecoveryStatus.PENDING
                    or recovery.client_order_id != command.intent.client_order_id):
                return
        if command.intent.client_order_id in self._started_recovery_commands:
            return
        self._started_recovery_commands.add(command.intent.client_order_id)
        self._capture_commands((command,))
        book = self._state.books.get(command.intent.contract_id)
        market_data = self._market_data.get(command.venue_id) if execution is None else None
        if market_data is not None:
            try:
                book = await market_data.get_order_book(command.intent.contract_id)
            except Exception as error:
                await self._reject_recovery(
                    command,
                    f"recovery book refresh failed: {error}",
                )
                return
            if book is None:
                await self._reject_recovery(
                    command,
                    f"recovery book refresh found no book for {command.intent.contract_id}",
                )
                return
            observe_book_checkpoint(command, book, phase="recovery_snapshot_received",
                book_origin="rest_snapshot")
            book = replace(book, received_at_ns=time.monotonic_ns())
            await self._sink.publish(
                OrderBookUpdated(command.venue_id, command.intent.contract_id, book),
            )

        if execution is not None:
            try:
                book = await self._fresh_recovery_book(command, execution, book)
            except Exception as error:
                await self._reject_recovery(command, f"recovery fresh book unavailable: {error}")
                return
        observe_book_checkpoint(command, book, phase="recovery_preparation",
            book_origin="rest_snapshot" if market_data is not None else "in_memory")
        guard_error = _book_guard_error(
            command,
            book,
            self._max_book_age_ns,
            time.monotonic_ns(),
            enforce_source_age=self._enforce_source_age,
        )
        if guard_error is not None:
            await self._reject_recovery(command, guard_error)
            return
        adapter, prepared = await self._prepare(command)
        if execution is not None:
            try:
                books = await self._current_recovery_books(execution)
            except Exception as error:
                await self._reject_recovery(command, f"recovery final books unavailable: {error}")
                return
            current = self._state.executions.get(execution.id)
            recovery = self._state.recoveries.get(execution.id)
            if (current != execution or execution.id in self._state.execution_safety_stops
                    or recovery is None or recovery.status is not RecoveryStatus.PENDING
                    or recovery.client_order_id != command.intent.client_order_id):
                await self._reject_recovery(command, "recovery changed during preparation")
                return
            for contract_id, latest in zip(
                (execution.leg1_contract_id, execution.leg2_contract_id), books, strict=True,
            ):
                cached = self._state.books.get(contract_id)
                if cached is None or (cached.received_at_ns or 0) <= latest.received_at_ns:
                    contract = self._state.contracts[contract_id]
                    self._state.apply(OrderBookUpdated(contract.venue_id, contract_id, latest))
            book = self._state.books.get(command.intent.contract_id)
            try:
                book = await self._fresh_recovery_book(command, execution, book)
            except Exception as error:
                await self._reject_recovery(command, f"recovery final fresh book unavailable: {error}")
                return
            observe_book_checkpoint(command, book, phase="recovery_final_validation", book_origin="in_memory")
            guard_error = _book_guard_error(
                command, book, self._max_book_age_ns, time.monotonic_ns(),
                enforce_source_age=self._enforce_source_age,
            )
            if guard_error is not None:
                await self._reject_recovery(command, guard_error)
                return
            guard_error = (
                "central recovery risk guard unavailable" if self._guard_recovery is None
                else self._guard_recovery(command, book)
            )
            if guard_error is not None:
                await self._reject_recovery(command, guard_error)
                return
        for attempt in range(_RECOVERY_SUBMISSION_ATTEMPT_LIMIT):
            result = await self._submit_once(command, adapter, prepared, decision_book=book)
            if execution is not None or not _retryable_recovery_rejection(command, result):
                break
            if attempt + 1 < _RECOVERY_SUBMISSION_ATTEMPT_LIMIT:
                await asyncio.sleep(_RECOVERY_SUBMISSION_RETRY_SECONDS)
        await self._sink.publish(SubmissionReceived(command, result))
        self._start_monitor(command, prepared, result)

    async def _reject_recovery(self, command: SubmitOrder, reason: str) -> None:
        """Publish a definitive local zero-fill recovery result."""
        await self._sink.publish(
            SubmissionReceived(
                command,
                SubmissionResult(
                    SubmissionStatus.REJECTED,
                    OrderReference(
                        command.venue_id,
                        command.intent.client_order_id,
                        b"local-pre-submission-guard",
                    ),
                    reason=reason,
                ),
            ),
        )

    async def _submit_pair(
        self,
        commands: tuple[SubmitOrder, SubmitOrder],
        prepared_pair: list[tuple[ExecutionPort, PreparedOrder]],
    ) -> None:
        """Submit both legs concurrently and cancel an open peer after failure.

        Parameters
        ----------
        commands
            The two commands from one admitted execution.
        prepared_pair
            Venue adapters and persisted requests in command order.
        """
        results = list(
            await asyncio.gather(
                *(
                    self._submit_once(command, adapter, prepared)
                    for command, (adapter, prepared) in zip(
                        commands,
                        prepared_pair,
                        strict=True,
                    )
                ),
            ),
        )
        for command, result in zip(commands, results, strict=True):
            await self._sink.publish(SubmissionReceived(command, result))

        failed = [
            index
            for index, (command, result) in enumerate(
                zip(commands, results, strict=True),
            )
            if _submission_failed(command, result)
        ]
        cancel_immediately: set[int] = set()
        if len(failed) == 1:
            peer = 1 - failed[0]
            command = commands[peer]
            adapter, prepared = prepared_pair[peer]
            if _submission_may_be_open(command, adapter, results[peer]):
                cancel_immediately.add(peer)
                results[peer] = await self._cancel_submission(
                    command,
                    adapter,
                    prepared,
                    results[peer],
                )

        for index, (command, (_, prepared), result) in enumerate(
            zip(commands, prepared_pair, results, strict=True),
        ):
            self._start_monitor(
                command,
                prepared,
                result,
                cancel_immediately=index in cancel_immediately,
            )

    async def _prepare_pair(
        self,
        commands: tuple[SubmitOrder, SubmitOrder],
    ) -> list[tuple[ExecutionPort, PreparedOrder]]:
        """Prepare both requests concurrently before the shared book guard.

        Parameters
        ----------
        commands
            Primary and hedge commands belonging to one execution.

        Returns
        -------
        list[tuple[ExecutionPort, PreparedOrder]]
            Prepared adapters and durable requests in command order.

        Notes
        -----
        - Preparation signs and journals requests but does not submit them.
        - The engine reserves locally refreshed BUY collateral before commands
          reach this boundary, leaving no funding await before the age guard.
        """
        adapters = tuple(self._execution.get(command.venue_id) for command in commands)
        if any(adapter is None for adapter in adapters):
            missing = next(
                command.venue_id
                for command, adapter in zip(commands, adapters, strict=True)
                if adapter is None
            )
            raise RuntimeError(f"No execution adapter for {missing}")

        prepared = list(
            await asyncio.gather(*(self._prepare(command) for command in commands)),
        )
        return prepared

    async def execute(
        self,
        command: SubmitOrder,
        prepared: PreparedOrder | None = None,
    ) -> None:
        """Execute one command, optionally resuming an already prepared request.

        Parameters
        ----------
        command
            Journaled venue-neutral order command.
        prepared
            Exact recovered payload; when omitted the adapter prepares a new one.
        """
        adapter, prepared = await self._prepare(command, prepared)
        await self._submit(command, adapter, prepared)

    async def _prepare(
        self,
        command: SubmitOrder,
        prepared: PreparedOrder | None = None,
        *,
        journal_prepared: bool = True,
    ) -> tuple[ExecutionPort, PreparedOrder]:
        """Prepare and persist one request without submitting it.

        Parameters
        ----------
        command
            Journaled venue-neutral order command.
        prepared
            Exact recovered payload to reuse when already available.
        journal_prepared
            Persist ``OrderPrepared`` separately. The paired hot path disables
            this because both payloads share one batch frame.

        Returns
        -------
        tuple[ExecutionPort, PreparedOrder]
            Resolved adapter and durable request ready for submission.
        """
        timings = (
            self._state.timings.get(command.execution_id)
            if command.role in {"primary", "hedge"}
            else None
        )
        prepare_started_ns = time.monotonic_ns()
        if timings is not None:
            timings.mark_prepare_started(command.role, prepare_started_ns)
        try:
            adapter = self._execution.get(command.venue_id)
            if adapter is None:
                raise RuntimeError(f"No execution adapter for {command.venue_id}")
            updates = self._updates.get(command.venue_id)
            if updates is not None:
                watch = getattr(updates, "watch", None)
                if watch is not None:
                    await watch(command.intent.contract_id)
            if timings is not None:
                timings.mark_watch_finished(command.role, time.monotonic_ns())
            if prepared is None:
                prepare_call = ThreadCallTiming() if timings is not None else None
                if timings is not None:
                    timings.prepare_calls.setdefault(command.role, prepare_call)
                prepared = await timed_to_thread(prepare_call, adapter.prepare, command.intent)
                if timings is not None:
                    timings.mark_adapter_prepared(command.role, time.monotonic_ns())
                if journal_prepared:
                    journal_call = ThreadCallTiming() if timings is not None else None
                    if timings is not None:
                        timings.journal_calls.setdefault(command.role, journal_call)
                    await timed_to_thread(
                        journal_call,
                        self._journal.append,
                        OrderPrepared(command, prepared),
                    )
                    if timings is not None:
                        timings.mark_journaled(command.role, time.monotonic_ns())
            return adapter, prepared
        finally:
            if timings is not None:
                timings.mark_prepare_finished(command.role, time.monotonic_ns())

    async def _submit(
        self,
        command: SubmitOrder,
        adapter: ExecutionPort,
        prepared: PreparedOrder,
    ) -> None:
        """Submit one already prepared request and publish its initial result."""
        result = await self._submit_once(command, adapter, prepared)
        await self._sink.publish(SubmissionReceived(command, result))
        self._start_monitor(command, prepared, result)

    async def _submit_once(
        self,
        command: SubmitOrder,
        adapter: ExecutionPort,
        prepared: PreparedOrder,
        *,
        decision_book: OrderBook | None = None,
    ) -> SubmissionResult:
        """Submit one prepared request without starting its update monitor.

        Parameters
        ----------
        command
            Journaled order command whose timings are recorded.
        adapter
            Venue execution boundary used for submission.
        prepared
            Exact request already persisted in the journal.
        decision_book
            Exact recovery snapshot used for its guard, which may not yet have
            reached the asynchronously updated parent cache. Diagnostic only.

        Returns
        -------
        SubmissionResult
            Initial normalized venue response.
        """
        submit_started_ns = time.monotonic_ns()
        observe_book_checkpoint(command, decision_book if decision_book is not None else
            self._state.books.get(command.intent.contract_id), phase="submission",
            monotonic_at_ns=submit_started_ns,
            book_origin="recovery_selected" if decision_book is not None else "in_memory")
        updates = self._updates.get(command.venue_id)
        timings = (
            self._state.timings.get(command.execution_id)
            if command.role in {"primary", "hedge"}
            else None
        )
        if timings is not None:
            timings.mark_submit(
                command.role,
                str(command.venue_id),
                submit_started_ns,
            )

        self._observe_worker_book_age(
            command,
            "submission",
            submit_started_ns,
            time.time_ns(),
        )

        submit_call = ThreadCallTiming() if timings is not None else None
        if timings is not None:
            timings.submit_calls.setdefault(command.role, submit_call)
        result = await timed_to_thread(submit_call, adapter.submit, prepared)

        if timings is not None:
            timings.mark_ack(command.role, str(command.venue_id), time.monotonic_ns())

        return self._record_initial(updates, result)

    def _observe_worker_book_age(
        self,
        command: SubmitOrder,
        stage: str,
        monotonic_at_ns: int,
        wall_at_ns: int,
    ) -> None:
        """Report one checkpoint without coupling dispatch to worker metrics."""
        if self._worker_book_age_observer is not None:
            self._worker_book_age_observer(
                command.execution_id,
                command.venue_id,
                command.intent.contract_id,
                stage,
                monotonic_at_ns,
                wall_at_ns,
            )

    async def _cancel_submission(
        self,
        command: SubmitOrder,
        adapter: ExecutionPort,
        prepared: PreparedOrder,
        initial: SubmissionResult,
    ) -> SubmissionResult:
        """Cancel a possibly open order and publish any authoritative snapshot.

        Parameters
        ----------
        command
            Peer command whose order may remain active.
        adapter
            Venue execution boundary used for cancellation.
        prepared
            Persisted request carrying the recoverable order reference.
        initial
            Initial submission result retained when cancellation is uncertain.

        Returns
        -------
        SubmissionResult
            Initial result updated with a cancellation snapshot when confirmed.
        """
        try:
            observe_cancel(prepared.reference, "peer_failure_or_recovery")
            cancelled = await asyncio.to_thread(adapter.cancel, prepared.reference)
        except Exception:
            return initial
        snapshot = cancelled.snapshot
        if cancelled.status is not ReconciliationStatus.FOUND or snapshot is None:
            return initial
        updates = self._updates.get(command.venue_id)
        if updates is not None:
            snapshot = updates.record_snapshot(prepared.reference, snapshot, "get")
        await self._sink.publish(
            OrderSnapshotUpdated(
                command.execution_id,
                command.role,
                prepared.reference,
                snapshot,
                "get",
            ),
        )
        return SubmissionResult(
            SubmissionStatus.ACCEPTED,
            prepared.reference,
            snapshot,
            initial.reason,
        )

    def _start_monitor(
        self,
        command: SubmitOrder,
        prepared: PreparedOrder,
        result: SubmissionResult,
        *,
        cancel_immediately: bool = False,
    ) -> None:
        """Start venue updates for one accepted or uncertain submission.

        Parameters
        ----------
        command
            Order command associated with subsequent snapshots.
        prepared
            Persisted request carrying the recovery reference.
        result
            Latest submission or cancellation result used as monitor state.
        cancel_immediately
            Skip the adapter's resting window after recovery or an uncertain peer
            cancellation.
        """
        if result.status in {SubmissionStatus.ACCEPTED, SubmissionStatus.UNKNOWN}:
            task = asyncio.create_task(
                self._monitor(
                    command,
                    prepared,
                    result,
                    cancel_immediately=cancel_immediately,
                ),
                name=f"order-updates:{prepared.reference.client_order_id}",
            )
            self._watchers.add(task)
            task.add_done_callback(self._watcher_done)

    async def adopt(
        self,
        command: SubmitOrder,
        prepared: PreparedOrder,
        snapshot: OrderSnapshot,
    ) -> None:
        """Publish and monitor an order found during restart reconciliation."""
        result = SubmissionResult(
            status=SubmissionStatus.ACCEPTED,
            reference=prepared.reference,
            snapshot=snapshot,
        )
        updates = self._updates.get(command.venue_id)
        result = self._record_initial(updates, result, source="get")
        await self._sink.publish(SubmissionReceived(command, result))
        task = asyncio.create_task(
            self._monitor(command, prepared, result, cancel_immediately=True),
            name=f"recovered-order:{prepared.reference.client_order_id}",
        )
        self._watchers.add(task)
        task.add_done_callback(self._watcher_done)

    async def close(self) -> None:
        """Cancel submissions and waiters while leaving adapters open."""
        tasks = tuple(self._submissions | self._watchers)
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._submissions.clear()
        self._watchers.clear()

    def _record_initial(
        self,
        updates: OrderUpdatePort | None,
        result: SubmissionResult,
        *,
        source: str = "submit",
    ) -> SubmissionResult:
        if updates is None or result.snapshot is None:
            return result
        snapshot = updates.record_snapshot(result.reference, result.snapshot, source)
        return SubmissionResult(
            result.status,
            result.reference,
            snapshot,
            result.reason,
        )

    def _pair_submission_done(self, task: asyncio.Task[None]) -> None:
        self._submissions.discard(task)
        self._outputs.task_done()
        self._outputs.task_done()
        self._report_error(task)

    def _batched_submission_done(self, task: asyncio.Task[None]) -> None:
        self._submissions.discard(task)
        self._outputs.task_done()
        self._report_error(task)

    def _single_submission_done(self, task: asyncio.Task[None]) -> None:
        self._submissions.discard(task)
        self._outputs.task_done()
        self._report_error(task)

    def _watcher_done(self, task: asyncio.Task[None]) -> None:
        self._watchers.discard(task)
        self._report_error(task)

    def _report_error(self, task: asyncio.Task[None]) -> None:
        if (
            not task.cancelled()
            and (error := task.exception()) is not None
            and self._on_error is not None
        ):
            self._on_error(error)

    async def _definitive_cancel(
        self, command: SubmitOrder, prepared: PreparedOrder,
    ) -> ReconciliationResult:
        """Journal a cancellation before broadcast and reuse it across retries.

        Notes
        -----
        - Serialize nonce selection through initial broadcast per venue. Chain
          polling runs off the event loop and never generates a new transaction
          after an uncertain broadcast.
        """
        adapter = self._execution[command.venue_id]
        lock = self._cancellation_locks.setdefault(command.venue_id, asyncio.Lock())
        async with lock:
            transaction_lock = adapter.cancellation_transaction_lock
            if transaction_lock is not None:
                while not transaction_lock.acquire(blocking=False):
                    await asyncio.sleep(0.01)
            async def persist_and_broadcast() -> ReconciliationResult:
                client_id = prepared.reference.client_order_id
                request = self._state.cancellations.get(client_id)
                if request is None:
                    request = await asyncio.to_thread(adapter.prepare_cancellation, prepared)
                    if request is None:
                        return ReconciliationResult(ReconciliationStatus.UNKNOWN, prepared.reference)
                    event = OrderCancellationPrepared(command, request)
                    await asyncio.to_thread(self._journal.append, event)
                    self._state.apply(event)
                    ORDER_CANCEL_ATTEMPTS.labels(str(command.venue_id).lower(), "chain_prepared").inc()
                sync = getattr(self._journal, "sync", None)
                if sync is None:
                    raise RuntimeError("Definitive cancellation requires a durable journal")
                await asyncio.to_thread(sync)
                return await asyncio.to_thread(adapter.submit_cancellation, prepared, request)

            job = asyncio.create_task(persist_and_broadcast())
            cancelled = False
            try:
                # A cancelled await cannot stop an RPC thread. Keep nonce ownership
                # until signing/journaling/broadcast has actually finished.
                while not job.done():
                    try:
                        await asyncio.shield(job)
                    except asyncio.CancelledError:
                        cancelled = True
                result = job.result()
                if cancelled:
                    raise asyncio.CancelledError
                return result
            finally:
                if transaction_lock is not None:
                    transaction_lock.release()

    async def _monitor(
        self,
        command: SubmitOrder,
        prepared: PreparedOrder,
        initial: SubmissionResult,
        *,
        cancel_immediately: bool = False,
    ) -> None:
        """Reconcile an order and enforce adapter-declared cancellation.

        Parameters
        ----------
        command
            Journaled order whose cumulative snapshots are published.
        prepared
            Persisted request carrying the recoverable venue reference.
        initial
            Initial submission or cancellation state.
        cancel_immediately
            Skip a declared resting window after restart or peer failure.

        Notes
        -----
        - An unconfirmed timed cancellation halts new trading and keeps retrying.
          Confirmed removal with uncertain fills allows three seconds for
          settlement before halting, without permitting replacement meanwhile.
        - Optional definitive cancellation starts immediately after uncertain
          removal. Its ten-second escalation deadline halts new trading but never
          interprets a timeout as zero fills; the same transaction stays tracked.
        - Cached private updates remain consumable after cancellation; REST
          errors must not hide an already confirmed fill.
        """
        adapter = self._execution[command.venue_id]
        updates = self._updates.get(command.venue_id)
        current = initial.snapshot
        not_found = 0
        cancellation_window = adapter.cancellation_window_seconds(command.intent)
        application_timed_order = cancellation_window is not None
        venue = str(command.venue_id).lower()
        loop = asyncio.get_running_loop()
        started_at = loop.time()
        cancel_at = started_at + (
            0.0 if cancel_immediately else cancellation_window or 0.0
        )
        active_timed_order = application_timed_order and (
            current is None or not is_settled_order(command, current)
        )
        safety_stop_published = False
        settlement_wait_started: float | None = None
        chain_task: asyncio.Task[ReconciliationResult] | None = None
        chain_started: float | None = None
        if active_timed_order:
            ACTIVE_RESTING_ORDERS.labels(venue).inc()
        try:
            while current is None or not is_settled_order(command, current):
                execution = self._state.executions.get(command.execution_id)
                if (execution is not None and execution.status is ArbitrageExecutionStatus.COMPLETED
                        and execution.resolution_method is not None):
                    return
                if updates is not None and current is not None:
                    changed = await updates.wait_for_update(
                        prepared.reference, current, timeout=0,
                    )
                    if changed is not None:
                        current = changed
                        source = getattr(updates, "source", lambda _: None)(changed)
                        await self._sink.publish(OrderSnapshotUpdated(
                            command.execution_id, command.role, prepared.reference,
                            changed, "get" if source == "get" else "ws",
                        ))
                        if is_settled_order(command, changed):
                            continue
                now = loop.time()
                removed_unsettled = (
                    current is not None and current.is_terminal()
                    and current.may_receive_more_fills is True
                )
                if removed_unsettled and adapter.supports_definitive_cancellation:
                    if chain_started is None:
                        chain_started = now
                    if chain_task is None:
                        chain_task = asyncio.create_task(self._definitive_cancel(command, prepared))
                    if chain_task.done():
                        try:
                            reconciled = chain_task.result()
                        except Exception:
                            ORDER_CANCEL_ATTEMPTS.labels(venue, "chain_error").inc()
                        else:
                            snapshot = reconciled.snapshot
                            if reconciled.status is ReconciliationStatus.FOUND and snapshot is not None:
                                if updates is not None:
                                    snapshot = updates.record_snapshot(prepared.reference, snapshot, "get")
                                if snapshot != current:
                                    current = snapshot
                                    await self._sink.publish(OrderSnapshotUpdated(
                                        command.execution_id, command.role, prepared.reference, snapshot, "get",
                                    ))
                                if is_settled_order(command, snapshot):
                                    ORDER_CANCEL_ATTEMPTS.labels(venue, "chain_finalized").inc()
                                    ORDER_LATENCY.labels(venue, "cancel_chain_total").observe(loop.time() - chain_started)
                                    continue
                        chain_task = None
                    if now - chain_started >= _CHAIN_CANCELLATION_CONFIRMATION_SECONDS and not safety_stop_published:
                        safety_stop_published = True
                        ORDER_CANCEL_ATTEMPTS.labels(venue, "chain_timeout").inc()
                        await self._sink.publish(TradingSafetyStop(
                            command.venue_id,
                            f"On-chain cancellation remains uncertain; replacement blocked: {prepared.reference.client_order_id}",
                            Timestamp.now(), execution_id=command.execution_id,
                            client_order_id=prepared.reference.client_order_id,
                        ))
                    await asyncio.sleep(0.1 if chain_task is not None else 0.25)
                    continue
                if removed_unsettled:
                    if settlement_wait_started is None:
                        settlement_wait_started = now
                    if now - settlement_wait_started >= _SETTLEMENT_CONFIRMATION_SECONDS and not safety_stop_published:
                        safety_stop_published = True
                        await self._sink.publish(TradingSafetyStop(
                            command.venue_id,
                            "Order removed but settlement remains uncertain; "
                            f"replacement blocked: {prepared.reference.client_order_id}",
                            Timestamp.now(),
                            execution_id=command.execution_id,
                            client_order_id=prepared.reference.client_order_id,
                        ))
                elif venue == "predict" and not safety_stop_published and (
                    now - started_at >= (cancellation_window or 0) + _SETTLEMENT_CONFIRMATION_SECONDS
                ):
                    safety_stop_published = True
                    await self._sink.publish(TradingSafetyStop(
                        command.venue_id,
                        "Predict order outcome remains uncertain; replacement blocked: "
                        f"{prepared.reference.client_order_id}", Timestamp.now(),
                        execution_id=command.execution_id,
                        client_order_id=prepared.reference.client_order_id,
                    ))
                if application_timed_order and now >= cancel_at:
                    try:
                        if not removed_unsettled:
                            observe_cancel(prepared.reference, "peer_failure" if cancel_immediately else "resting_deadline")
                        cancelled = await asyncio.to_thread(
                            adapter.reconcile if removed_unsettled else adapter.cancel,
                            prepared.reference,
                        )
                    except Exception:
                        cancelled = ReconciliationResult(
                            ReconciliationStatus.UNKNOWN,
                            prepared.reference,
                        )
                        cancel_result = "error"
                    else:
                        if cancelled.status is ReconciliationStatus.FOUND:
                            cancel_result = (
                                "found_terminal"
                                if cancelled.snapshot is not None
                                and is_settled_order(command, cancelled.snapshot)
                                else "found_open"
                            )
                        else:
                            cancel_result = cancelled.status.value.lower()
                    if not removed_unsettled:
                        ORDER_CANCEL_ATTEMPTS.labels(venue, cancel_result).inc()
                    if (
                        cancelled.status is ReconciliationStatus.FOUND
                        and cancelled.snapshot is not None
                    ):
                        not_found = 0
                        snapshot = cancelled.snapshot
                        if updates is not None:
                            snapshot = updates.record_snapshot(
                                prepared.reference,
                                snapshot,
                                "get",
                            )
                        if snapshot != current:
                            current = snapshot
                            await self._sink.publish(
                                OrderSnapshotUpdated(
                                    command.execution_id,
                                    command.role,
                                    prepared.reference,
                                    snapshot,
                                    "get",
                                ),
                            )
                        if is_settled_order(command, snapshot):
                            continue
                    if not safety_stop_published and not (
                        current is not None and current.is_terminal()
                        and current.may_receive_more_fills is True
                    ):
                        safety_stop_published = True
                        await self._sink.publish(
                            TradingSafetyStop(
                                command.venue_id,
                                "Cancellation could not prove the order terminal: "
                                f"{prepared.reference.client_order_id} "
                                f"({cancel_result})",
                                Timestamp.now(),
                                execution_id=command.execution_id,
                                client_order_id=prepared.reference.client_order_id,
                            ),
                        )
                    await asyncio.sleep(0.25)
                    continue

                wait_timeout = 0.5
                if application_timed_order:
                    wait_timeout = min(wait_timeout, max(0.0, cancel_at - now))
                if updates is not None and current is not None and wait_timeout > 0:
                    changed = await updates.wait_for_update(
                        prepared.reference,
                        current,
                        timeout=wait_timeout,
                    )
                    if changed is not None:
                        current = changed
                        source = (
                            getattr(updates, "source", lambda _: None)(changed) or "ws"
                        )
                        await self._sink.publish(
                            OrderSnapshotUpdated(
                                command.execution_id,
                                command.role,
                                prepared.reference,
                                changed,
                                "get" if source == "get" else "ws",
                            ),
                        )
                        continue
                if application_timed_order:
                    if wait_timeout > 0 and (updates is None or current is None):
                        await asyncio.sleep(wait_timeout)
                    continue

                reconciled = await asyncio.to_thread(
                    adapter.reconcile,
                    prepared.reference,
                )
                if reconciled.status is ReconciliationStatus.FOUND:
                    not_found = 0
                    snapshot = reconciled.snapshot
                    if updates is not None:
                        snapshot = updates.record_snapshot(
                            prepared.reference,
                            snapshot,
                            "get",
                        )
                    if snapshot != current:
                        current = snapshot
                        await self._sink.publish(
                            OrderSnapshotUpdated(
                                command.execution_id,
                                command.role,
                                prepared.reference,
                                snapshot,
                                "get",
                            ),
                        )
                    continue
                if reconciled.status is ReconciliationStatus.NOT_FOUND:
                    not_found += 1
                    if not_found >= 2:
                        await self._sink.publish(
                            SubmissionReceived(
                                command,
                                SubmissionResult(
                                    (
                                        SubmissionStatus.REJECTED
                                        if initial.status is SubmissionStatus.UNKNOWN
                                        else SubmissionStatus.UNKNOWN
                                    ),
                                    prepared.reference,
                                    reason=(
                                        "order not found after two reconciliation "
                                        "attempts"
                                    ),
                                ),
                            ),
                        )
                        return
                await asyncio.sleep(0.25)
        except asyncio.CancelledError:
            if application_timed_order and (
                current is None or not is_settled_order(command, current)
            ):
                try:
                    observe_cancel(prepared.reference, "shutdown")
                    shutdown_cancel = await asyncio.to_thread(
                        adapter.cancel,
                        prepared.reference,
                    )
                except Exception:
                    shutdown_result = "shutdown_error"
                else:
                    shutdown_result = f"shutdown_{shutdown_cancel.status.value.lower()}"
                ORDER_CANCEL_ATTEMPTS.labels(venue, shutdown_result).inc()
            raise
        finally:
            if chain_task is not None:
                chain_task.cancel()
                await asyncio.gather(chain_task, return_exceptions=True)
            if active_timed_order:
                ACTIVE_RESTING_ORDERS.labels(venue).dec()
                if current is not None and is_settled_order(command, current):
                    ORDER_RESTING_DURATION.labels(venue).observe(
                        max(0.0, loop.time() - started_at),
                    )

        if current.filled_quantity.value > 0 and current.fee is None:
            for attempt in range(5):
                reconciled = await asyncio.to_thread(
                    adapter.reconcile,
                    prepared.reference,
                )
                snapshot = reconciled.snapshot
                if (
                    reconciled.status is ReconciliationStatus.FOUND
                    and snapshot is not None
                ):
                    if updates is not None:
                        snapshot = updates.record_snapshot(
                            prepared.reference,
                            snapshot,
                            "get",
                        )
                    # Actual fills must reach accounting even if fees are not
                    # available yet. Fee retries must not hide excess shares.
                    if (
                        snapshot.filled_quantity != current.filled_quantity
                        or snapshot.average_price != current.average_price
                        or snapshot.fee != current.fee
                    ):
                        await self._sink.publish(
                            OrderSnapshotUpdated(
                                command.execution_id,
                                command.role,
                                prepared.reference,
                                snapshot,
                                "get",
                            ),
                        )
                        current = snapshot
                    if snapshot.fee is not None:
                        return
                if attempt < 4:
                    await asyncio.sleep(0.5)


class RecoveryError(RuntimeError):
    """Block live trading when a journaled order cannot be reconciled safely."""


class RecoveryCoordinator:
    """Reconcile every unfinished command reconstructed from the durable journal."""

    def __init__(
        self,
        entries: tuple[JournalRecord, ...],
        dispatcher: OutputDispatcher,
        execution: Mapping[VenueID, ExecutionPort],
    ) -> None:
        self._entries = entries
        self._dispatcher = dispatcher
        self._execution = execution

    async def recover(self) -> None:
        """Adopt, resubmit, or prepare every command lacking a terminal outcome.

        Notes
        -----
        - Operator-completed executions never re-enter order dispatch.
        - A persisted cancellation awaiting finality resumes monitoring with
          trading halted; its original order is never submitted again.

        Raises
        ------
        RecoveryError
            If a venue cannot authoritatively reconcile a persisted reference.
        """
        commands: dict[ClientOrderID, SubmitOrder] = {}
        prepared: dict[ClientOrderID, PreparedOrder] = {}
        terminal: set[ClientOrderID] = set()
        previously_submitted: set[ClientOrderID] = set()
        rejected_batches: list[PreparedExecutionBatch] = []
        batch_deadlines: dict[str, int] = {}
        executions = dict(self._dispatcher._state.executions)
        snapshots = dict(self._dispatcher._state.orders)
        for entry in self._entries:
            if isinstance(entry.event, PreparedExecutionBatch):
                batch_deadlines[entry.event.planned.execution.id] = (
                    entry.event.deadline_wall_at_ns
                )
                if entry.event.rejection_reason is not None:
                    rejected_batches.append(entry.event)
            events = (
                prepared_execution_events(entry.event)
                if isinstance(entry.event, PreparedExecutionBatch)
                else (entry.event,)
            )
            for event in events:
                if isinstance(event, ExecutionUpdated):
                    executions[event.execution.id] = event.execution
                elif isinstance(event, SubmitOrder):
                    if event.intent.client_order_id is not None:
                        commands[event.intent.client_order_id] = event
                elif isinstance(event, OrderPrepared):
                    prepared[event.prepared.reference.client_order_id] = event.prepared
                elif isinstance(event, OrderCancellationPrepared):
                    self._dispatcher._state.apply(event)
                    previously_submitted.add(event.command.intent.client_order_id)
                elif isinstance(event, SubmissionReceived):
                    if event.result.snapshot is not None:
                        snapshots[event.result.reference.client_order_id] = event.result.snapshot
                    if event.result.status is not SubmissionStatus.REJECTED:
                        previously_submitted.add(
                            event.result.reference.client_order_id,
                        )
                    if event.result.status is SubmissionStatus.REJECTED or (
                        event.result.snapshot is not None
                        and is_settled_order(event.command, event.result.snapshot)
                    ):
                        terminal.add(event.result.reference.client_order_id)
                elif isinstance(event, OrderSnapshotUpdated):
                    snapshots[event.reference.client_order_id] = event.snapshot
                    previously_submitted.add(event.reference.client_order_id)
                    command = commands.get(event.reference.client_order_id)
                    if command is not None and is_settled_order(
                        command,
                        event.snapshot,
                    ):
                        terminal.add(event.reference.client_order_id)

        manually_completed = {
            execution_id for execution_id, execution in executions.items()
            if execution.status is ArbitrageExecutionStatus.COMPLETED
            and execution.resolution_method is not None
        }
        commands = {
            client_id: command for client_id, command in commands.items()
            if command.execution_id not in manually_completed
        }
        commands_by_execution: dict[str, list[SubmitOrder]] = {}
        for command in commands.values():
            commands_by_execution.setdefault(command.execution_id, []).append(command)
        rejected_executions: set[str] = set()
        for batch in rejected_batches:
            if batch.planned.execution.id in manually_completed:
                continue
            client_order_ids = {
                command.intent.client_order_id for command in batch.commands
            }
            if not client_order_ids <= terminal:
                await self._dispatcher.reject_pair(
                    batch.commands,
                    batch.rejection_reason or "pre-submission rejection",
                )
            rejected_executions.add(batch.planned.execution.id)
        for execution_commands in commands_by_execution.values():
            if any(command.intent.client_order_id in self._dispatcher._state.cancellations
                   for command in execution_commands):
                # Cancellation proves this batch reached the venue; it is not
                # an expired, never-submitted preparation to discard or resend.
                continue
            if {command.role for command in execution_commands} != {"primary", "hedge"}:
                continue
            prepared_commands = [
                (command, prepared.get(command.intent.client_order_id))
                for command in execution_commands
            ]
            if not any(payload is not None for _, payload in prepared_commands):
                continue
            if all(payload is not None for _, payload in prepared_commands):
                deadline = batch_deadlines.get(execution_commands[0].execution_id)
                if deadline is not None and time.time_ns() >= deadline:
                    results: list[ReconciliationResult] = []
                    for command, payload in prepared_commands:
                        assert payload is not None
                        adapter = self._execution.get(command.venue_id)
                        if adapter is None:
                            raise RecoveryError(
                                f"No recovery adapter for {command.venue_id}"
                            )
                        results.append(
                            await asyncio.to_thread(
                                adapter.reconcile,
                                payload.reference,
                            )
                        )
                    if any(
                        result.status is ReconciliationStatus.UNKNOWN
                        for result in results
                    ):
                        raise RecoveryError(
                            "Venue returned UNKNOWN for an expired prepared batch"
                        )
                    if all(
                        result.status is ReconciliationStatus.NOT_FOUND
                        for result in results
                    ):
                        await self._dispatcher.reject_pair(
                            tuple(execution_commands),
                            "recovery discarded expired unsubmitted execution",
                        )
                        rejected_executions.add(execution_commands[0].execution_id)
                continue
            command, payload = next(
                (item for item in prepared_commands if item[1] is not None),
            )
            adapter = self._execution.get(command.venue_id)
            if adapter is None:
                raise RecoveryError(f"No recovery adapter for {command.venue_id}")
            result = await asyncio.to_thread(adapter.reconcile, payload.reference)
            if result.status is ReconciliationStatus.UNKNOWN:
                raise RecoveryError(
                    f"Venue {command.venue_id} returned UNKNOWN for "
                    f"{payload.reference.client_order_id}",
                )
            if result.status is ReconciliationStatus.NOT_FOUND:
                await self._dispatcher.reject_pair(
                    tuple(execution_commands),
                    "recovery aborted pair after an unsubmitted prepared leg",
                )
                rejected_executions.add(command.execution_id)

        for client_order_id, command in commands.items():
            if command.execution_id in rejected_executions:
                continue
            payload = prepared.get(client_order_id)
            client_order_id = (
                payload.reference.client_order_id
                if payload is not None
                else client_order_id
            )
            if client_order_id is not None and client_order_id in terminal:
                continue
            adapter = self._execution.get(command.venue_id)
            if adapter is None:
                raise RecoveryError(f"No recovery adapter for {command.venue_id}")
            if payload is None:
                await self._dispatcher.execute(command)
                continue
            if client_order_id in self._dispatcher._state.cancellations and adapter.supports_definitive_cancellation:
                result = await self._dispatcher._definitive_cancel(command, payload)
                if result.status is ReconciliationStatus.UNKNOWN:
                    snapshot = snapshots.get(client_order_id)
                    if (snapshot is not None and snapshot.may_receive_more_fills is True
                            and not is_settled_order(command, snapshot)):
                        if not self._dispatcher._state.safety_halted:
                            await self._dispatcher._sink.publish(TradingSafetyStop(
                                command.venue_id,
                                "Persisted cancellation awaits finality after restart; "
                                f"trading halted: {client_order_id}", Timestamp.now(),
                            ))
                        await self._dispatcher.adopt(command, payload, snapshot)
                        continue
            else:
                result = await asyncio.to_thread(adapter.reconcile, payload.reference)
            if result.status is ReconciliationStatus.FOUND:
                await self._dispatcher.adopt(command, payload, result.snapshot)
            elif result.status is ReconciliationStatus.NOT_FOUND:
                if client_order_id in previously_submitted:
                    raise RecoveryError(
                        f"Venue {command.venue_id} lost a previously observed order "
                        f"{payload.reference.client_order_id}",
                    )
                await self._dispatcher.execute(command, payload)
            else:
                raise RecoveryError(
                    f"Venue {command.venue_id} returned UNKNOWN for "
                    f"{payload.reference.client_order_id}",
                )


def _submission_failed(
    command: SubmitOrder,
    result: SubmissionResult,
) -> bool:
    """Return whether an initial submission cannot provide any hedge fill.

    Parameters
    ----------
    command
        Command used to interpret IOC and FOK partial terminal states.
    result
        Initial normalized venue response.

    Returns
    -------
    bool
        ``True`` for a rejection or a terminal zero-fill snapshot.
    """
    return result.status is SubmissionStatus.REJECTED or (
        result.snapshot is not None
        and is_settled_order(command, result.snapshot)
        and result.snapshot.filled_quantity.value == 0
    )


def _observe_edge_survival(
    state: TradingState,
    execution_id: str,
    age_ns: int,
    survived: bool,
) -> None:
    """Record one bounded edge-survival sample without scheduling probes."""
    opportunity = next(
        (
            event.opportunity
            for event in reversed(state.opportunities)
            if event.id == execution_id
        ),
        None,
    )
    if opportunity is None:
        return
    edge_bps = float(opportunity.net_edge * 10_000)
    age_ms = max(0.0, age_ns / 1_000_000)
    ARBITRAGE_EDGE_SURVIVAL.labels(
        _upper_bucket(edge_bps, (25, 50, 100, 150, 250, 500)),
        _upper_bucket(age_ms, (2, 5, 10, 25, 50, 100)),
        str(survived).lower(),
    ).inc()


def _upper_bucket(value: float, bounds: tuple[int, ...]) -> str:
    """Return one stable Prometheus label for a bounded observation."""
    return next((f"le_{bound}" for bound in bounds if value <= bound), "gt_max")


def _retryable_recovery_rejection(
    command: SubmitOrder,
    result: SubmissionResult,
) -> bool:
    """Identify a definitive Polymarket inventory-settlement race.

    Parameters
    ----------
    command
        Recovery command associated with the submission response.
    result
        Definitive initial response returned by the venue adapter.

    Returns
    -------
    bool
        Whether resubmitting the same persisted IOC request is safe and useful.
    """
    if (
        command.role != "recovery"
        or str(command.venue_id).upper() != "POLYMARKET"
        or result.status is not SubmissionStatus.REJECTED
        or result.reason is None
    ):
        return False
    reason = result.reason.lower()
    return any(
        marker in reason
        for marker in (
            "not enough balance / allowance",
            "balance is not enough",
            "insufficient balance",
            "insufficient collateral allowance",
        )
    )


def _submission_may_be_open(
    command: SubmitOrder,
    adapter: ExecutionPort,
    result: SubmissionResult,
) -> bool:
    """Return whether cancellation can still prevent a peer fill.

    Parameters
    ----------
    command
        Peer command used to interpret terminal order states.
    adapter
        Venue boundary declaring translated lifecycle behavior.
    result
        Initial normalized venue response.

    Returns
    -------
    bool
        ``True`` when the venue may still match the peer order.
    """
    potentially_persistent = (
        adapter.cancellation_window_seconds(command.intent) is not None
        or command.intent.time_in_force not in {TimeInForce.IOC, TimeInForce.FOK}
    )
    return potentially_persistent and (
        result.status in {SubmissionStatus.ACCEPTED, SubmissionStatus.UNKNOWN}
    ) and (
        result.snapshot is None or not is_settled_order(command, result.snapshot)
    )


def _submission_guard_error(
    commands: tuple[SubmitOrder, SubmitOrder],
    state: TradingState,
    max_book_age_ns: int,
    now_ns: int,
    enforce_source_age: bool = True,
) -> str | None:
    """Validate both live legs against one current in-memory snapshot instant.

    Parameters
    ----------
    commands
        Primary and hedge commands belonging to one execution.
    state
        Current normalized orderbooks keyed by contract.
    max_book_age_ns
        Maximum accepted local receive age in nanoseconds.
    now_ns
        Shared monotonic validation time for both books.
    enforce_source_age
        Whether venue wall-clock age can reject the pair.

    Returns
    -------
    str | None
        Rejection reason, or ``None`` when both legs remain fresh and marketable.
    """
    if {command.role for command in commands} != {"primary", "hedge"}:
        return "pre-submission guard requires one primary and one hedge command"
    now_wall_ns = time.time_ns()
    for command in commands:
        error = _command_guard_error(
            command,
            state,
            max_book_age_ns,
            now_ns,
            now_wall_ns,
            enforce_source_age,
        )
        if error is not None:
            return error
    return None


def _market_expiry_guard_error(
    commands: tuple[SubmitOrder, SubmitOrder],
    state: TradingState,
    minimum_seconds: int,
) -> str | None:
    """Reject a recurring submission whose market is too close to expiry."""
    cycle = state.execution_cycles.get(commands[0].execution_id)
    if isinstance(cycle, RegularCandidate):
        return None
    pair_key = state.execution_pairs.get(commands[0].execution_id)
    if cycle is None or pair_key is None:
        return None
    minimum_seconds = market_expiry_guard_seconds(cycle, minimum_seconds)
    pair = next(
        (
            candidate
            for candidate in state.matches.get(cycle, ())
            if candidate.key == pair_key
        ),
        None,
    )
    if pair is None or pair.ends_at > Timestamp.now() + timedelta(
        seconds=minimum_seconds,
    ):
        return None
    return (
        "market expiry guard: paired submission skipped with less than "
        f"{minimum_seconds} seconds remaining"
    )


def _command_guard_error(
    command: SubmitOrder,
    state: TradingState,
    max_book_age_ns: int | None,
    now_ns: int,
    now_wall_ns: int | None = None,
    enforce_source_age: bool = True,
) -> str | None:
    """Validate one command against its latest executable book."""
    return _book_guard_error(
        command,
        state.books.get(command.intent.contract_id),
        max_book_age_ns,
        now_ns,
        now_wall_ns,
        enforce_source_age,
    )


def _book_freshness_error(
    command: SubmitOrder,
    book: OrderBook | None,
    max_book_age_ns: int | None,
    now_ns: int,
    now_wall_ns: int | None = None,
    enforce_source_age: bool = True,
) -> str | None:
    """Share unchanged freshness rules between submission and recovery waits."""
    if book is None:
        return f"pre-submission guard found no book for {command.intent.contract_id}"
    if max_book_age_ns is None:
        return None
    if book.received_at_ns is None:
        return (
            "pre-submission guard found no receive timestamp for "
            f"{command.intent.contract_id}"
        )
    age_ns = max(0, now_ns - book.received_at_ns)
    if age_ns > max_book_age_ns:
        return (
            f"pre-submission guard found {command.intent.contract_id} book "
            f"age {age_ns / 1_000_000:.3f} ms above "
            f"{max_book_age_ns / 1_000_000:.3f} ms"
        )
    if enforce_source_age and book.source_timestamp_kind == "venue_update":
        if book.source_at_ns is None:
            return f"pre-submission guard found no source timestamp for {command.intent.contract_id}"
        source_age_ns = max(
            0, (now_wall_ns if now_wall_ns is not None else time.time_ns()) - book.source_at_ns,
        )
        if source_age_ns > _SOURCE_BOOK_MAX_AGE_NS:
            return (
                f"pre-submission guard found {command.intent.contract_id} "
                f"source age {source_age_ns / 1_000_000:.3f} ms above "
                f"{SOURCE_BOOK_MAX_AGE_MS:.3f} ms"
            )
    return None


def _book_guard_error(
    command: SubmitOrder,
    book: OrderBook | None,
    max_book_age_ns: int | None,
    now_ns: int,
    now_wall_ns: int | None = None,
    enforce_source_age: bool = True,
) -> str | None:
    """Validate price, freshness, and full requested depth for one command.

    Parameters
    ----------
    command
        Venue-neutral order whose protected limit and quantity are checked.
    book
        Latest normalized book, or ``None`` when no snapshot is available.
    max_book_age_ns
        Maximum receive age, or ``None`` to omit the age check.
    now_ns
        Monotonic validation instant.
    now_wall_ns
        Wall-clock validation instant used only for venue source timestamps.
    enforce_source_age
        Whether venue wall-clock age can reject the book.

    Returns
    -------
    str | None
        Guard rejection reason, or ``None`` when the full order is executable.
    """
    error = _book_freshness_error(
        command, book, max_book_age_ns, now_ns, now_wall_ns, enforce_source_age,
    )
    if error is not None:
        return error
    assert book is not None
    limit = command.intent.limit_price
    if limit is None:
        return f"pre-submission guard requires a limit for {command.intent.contract_id}"
    level = book.best_ask() if command.intent.side is OrderSide.BUY else book.best_bid()
    if level is None:
        return (
            f"pre-submission guard found no executable "
            f"{command.intent.side.value} level for {command.intent.contract_id}"
        )
    marketable = (
        level.price.value <= limit.value
        if command.intent.side is OrderSide.BUY
        else level.price.value >= limit.value
    )
    if marketable:
        levels = book.asks if command.intent.side is OrderSide.BUY else book.bids
        available = sum(
            (
                candidate.quantity.value
                for candidate in levels
                if (
                    candidate.price.value <= limit.value
                    if command.intent.side is OrderSide.BUY
                    else candidate.price.value >= limit.value
                )
            ),
            0,
        )
        if available >= command.intent.quantity.value:
            return None
        return (
            f"pre-submission guard found only {available} executable shares for "
            f"{command.intent.contract_id}; requested {command.intent.quantity.value}"
        )
    level_name = "ask" if command.intent.side is OrderSide.BUY else "bid"
    return (
        f"pre-submission guard found best {level_name} {level.price.value} "
        f"outside {command.intent.side.value} limit {limit.value} for "
        f"{command.intent.contract_id}"
    )
