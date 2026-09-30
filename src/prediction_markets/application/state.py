"""Hold replayable in-memory trading state and publish read-only signal streams.

Responsibilities
----------------
- Apply journaled events to the current market, order, execution, and position state.
- Fan opportunity signals to WebSocket consumers without putting them in the hot path.
"""

import asyncio
from collections import deque
from collections.abc import AsyncIterator
from dataclasses import dataclass, field, replace
from decimal import Decimal

from prediction_markets.application.events import (
    AccountingCorrectionRecorded,
    ApplicationEvent,
    ArbitrageOpportunityFound,
    ArbitragePlanned,
    CashMovementRecorded,
    ExecutionUpdated,
    InventoryOperationRecorded,
    MarketSettlementRecorded,
    MarketMatchesUpdated,
    OpportunityValidationRef,
    OrderBookPairUpdated,
    OrderBookUpdated,
    OrderPrepared,
    OrderCancellationPrepared,
    OrderSnapshotUpdated,
    PositionUpdated,
    RecoveryPlanned,
    RecoveryUpdated,
    SubmissionReceived,
    SubmitOrder,
    TradeRecorded,
    TradingSafetyStop,
)
from prediction_markets.application.execution.timings import ExecutionTimings
from prediction_markets.application.markets.models import (
    MonitoredMarket,
    monitored_market_key,
)
from prediction_markets.domain.contracts.entities import BinaryContract
from prediction_markets.domain.outcome_inventory import (
    InventoryOperationID,
    InventoryOperationReference,
    InventoryOperationSnapshot,
    InventoryOperationStatus,
    InventoryReconciliationResult,
    InventorySubmissionResult,
    InventorySubmissionStatus,
    OutcomeInventoryAction,
    OutcomeInventorySettlement,
)
from prediction_markets.domain.market_matching.value_objects import MatchedContractPair
from prediction_markets.domain.orderbook.entities import OrderBook
from prediction_markets.domain.shared.value_objects import (
    ClientOrderID,
    ContractID,
    MarketID,
    PortfolioID,
    PositionID,
    Probability,
    StrategyID,
    TradeID,
    VenueID,
)
from prediction_markets.domain.trading.entities import (
    AccountingCorrection,
    CashMovement,
    OrderSnapshot,
    Portfolio,
    Position,
    Signal,
    Trade,
)
from prediction_markets.domain.trading.enums import (
    ArbitrageExecutionStatus,
    OrderSide,
    SignalDirection,
)
from prediction_markets.domain.trading.entities import ArbitrageExecutionJournal
from prediction_markets.domain.trading.entities import ExposureRecovery
from prediction_markets.domain.trading.value_objects import Confidence, Edge, PreparedOrder


@dataclass(slots=True)
class TradingState:
    """Store state derivable from the ordered journal.

    Notes
    -----
    - Mutations are confined to the application event loop.
    - API readers receive immutable domain values or copied collections.
    - ``_opportunity_ids`` mirrors ids currently stored in the bounded
      ``opportunities`` deque for O(1) idempotent inserts.
    """

    matches: dict[MonitoredMarket, tuple[MatchedContractPair, ...]] = field(
        default_factory=dict,
    )
    contracts: dict[ContractID, BinaryContract] = field(default_factory=dict)
    books: dict[ContractID, OrderBook] = field(default_factory=dict)
    opportunities: deque[ArbitrageOpportunityFound] = field(
        default_factory=lambda: deque(maxlen=1_000),
    )
    _opportunity_ids: set[str] = field(default_factory=set, repr=False)
    _pairs_by_contract: dict[
        ContractID,
        tuple[tuple[MonitoredMarket, MatchedContractPair], ...],
    ] = field(default_factory=dict, init=False, repr=False)
    executions: dict[str, ArbitrageExecutionJournal] = field(default_factory=dict)
    recoveries: dict[str, ExposureRecovery] = field(default_factory=dict)
    execution_pairs: dict[str, tuple[str, str]] = field(default_factory=dict)
    execution_cycles: dict[str, MonitoredMarket] = field(default_factory=dict)
    opportunity_worker_refs: dict[str, OpportunityValidationRef] = field(
        default_factory=dict,
    )
    execution_worker_refs: dict[str, OpportunityValidationRef] = field(
        default_factory=dict,
    )
    timings: dict[str, ExecutionTimings] = field(default_factory=dict)
    commands: dict[ClientOrderID, SubmitOrder] = field(default_factory=dict)
    prepared: dict[ClientOrderID, PreparedOrder] = field(default_factory=dict)
    cancellations: dict[ClientOrderID, bytes] = field(default_factory=dict)
    orders: dict[ClientOrderID, OrderSnapshot] = field(default_factory=dict)
    trades: dict[TradeID, Trade] = field(default_factory=dict)
    positions: dict[PositionID, Position] = field(default_factory=dict)
    inventory_operations: dict[InventoryOperationID, InventoryOperationSnapshot] = field(
        default_factory=dict,
    )
    pending_inventory_operations: dict[
        InventoryOperationID, InventoryOperationReference
    ] = field(default_factory=dict)
    applied_inventory_operations: set[InventoryOperationID] = field(
        default_factory=set,
    )
    settlements: dict[tuple[VenueID, MarketID], OutcomeInventorySettlement] = field(
        default_factory=dict,
    )
    cash_movements: dict[str, CashMovement] = field(default_factory=dict)
    accounting_corrections: dict[str, AccountingCorrection] = field(
        default_factory=dict,
    )
    trading_enabled: bool = False
    last_error: str | None = None
    safety_halted: bool = False
    execution_safety_stops: dict[str, TradingSafetyStop] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Build lookup indexes for state supplied during construction."""
        self._rebuild_pair_index()

    @property
    def portfolios(self) -> dict[tuple[VenueID, PortfolioID], Portfolio]:
        """Return venue-scoped portfolio views derived from current positions."""
        return Portfolio.group_by_venue(
            tuple(self.positions.values()),
            tuple(self.cash_movements.values()),
        )

    def apply(self, event: ApplicationEvent) -> None:
        """Apply one already-journaled event idempotently.

        Parameters
        ----------
        event
            Event in global journal order.

        Notes
        -----
        - Match snapshots replace older candidate values sharing the same stable
          monitor key, including candidates whose venue state or timestamps changed.
        """
        if isinstance(event, MarketMatchesUpdated):
            monitor_key = monitored_market_key(event.cycle)
            for market in tuple(self.matches):
                if monitored_market_key(market) == monitor_key:
                    self.matches.pop(market)
            self.matches[event.cycle] = event.pairs
            for pair in event.pairs:
                self.contracts[pair.left.id] = pair.left
                self.contracts[pair.right.id] = pair.right
            self._rebuild_pair_index()
            return
        if isinstance(event, OrderBookUpdated):
            self.books[event.contract_id] = event.order_book
            mark = event.order_book.mid_price()
            observed_at = event.order_book.timestamp
            if mark is not None and observed_at is not None:
                for position_id, position in tuple(self.positions.items()):
                    if (
                        position.venue_id == event.venue_id
                        and position.contract_id == event.contract_id
                        and position.is_open()
                    ):
                        self.positions[position_id] = position.with_mark(
                            mark,
                            observed_at,
                        )
            return
        if isinstance(event, OrderBookPairUpdated):
            pair = event.detected.pair
            self.contracts[pair.left.id] = pair.left
            self.contracts[pair.right.id] = pair.right
            self.apply(
                OrderBookUpdated(
                    pair.left.venue_id,
                    pair.left.id,
                    event.left_order_book,
                ),
            )
            self.apply(
                OrderBookUpdated(
                    pair.right.venue_id,
                    pair.right.id,
                    event.right_order_book,
                ),
            )
            return
        if isinstance(event, ArbitrageOpportunityFound):
            if event.id in self._opportunity_ids:
                if event.validation_ref is not None:
                    self.opportunity_worker_refs[event.id] = event.validation_ref
                return
            if (
                self.opportunities.maxlen is not None
                and len(self.opportunities) == self.opportunities.maxlen
            ):
                expired_id = self.opportunities[0].id
                self._opportunity_ids.discard(expired_id)
                self.opportunity_worker_refs.pop(expired_id, None)
            self.opportunities.append(event)
            self._opportunity_ids.add(event.id)
            if event.validation_ref is not None:
                self.opportunity_worker_refs[event.id] = event.validation_ref
            return
        if isinstance(event, ArbitragePlanned):
            self.executions[event.execution.id] = event.execution
            self.execution_pairs[event.execution.id] = event.pair.key
            self.execution_cycles[event.execution.id] = event.cycle
            worker_ref = self.opportunity_worker_refs.get(event.opportunity_id)
            if worker_ref is not None:
                self.execution_worker_refs[event.execution.id] = worker_ref
            return
        if isinstance(event, SubmitOrder):
            client_order_id = event.intent.client_order_id
            if client_order_id is not None:
                self.commands[client_order_id] = event
            return
        if isinstance(event, OrderPrepared):
            self.prepared[event.prepared.reference.client_order_id] = event.prepared
            return
        if isinstance(event, OrderCancellationPrepared):
            self.cancellations[event.command.intent.client_order_id] = event.request
            return
        if isinstance(event, SubmissionReceived):
            if event.result.snapshot is not None:
                self.orders[event.result.reference.client_order_id] = event.result.snapshot
            return
        if isinstance(event, OrderSnapshotUpdated):
            self.orders[event.reference.client_order_id] = event.snapshot
            return
        if isinstance(event, TradeRecorded):
            self.trades[event.trade.id] = event.trade
            return
        if isinstance(event, PositionUpdated):
            self.positions[event.position.id] = _marked_position(
                event.position,
                self.books,
            )
            return
        if isinstance(event, InventoryOperationRecorded):
            snapshot = _inventory_snapshot(event.record)
            reference = event.record.reference
            terminal = (
                snapshot is not None
                and snapshot.status is not InventoryOperationStatus.PENDING
            ) or (
                isinstance(event.record, InventorySubmissionResult)
                and event.record.status is InventorySubmissionStatus.REJECTED
            )
            if terminal:
                self.pending_inventory_operations.pop(reference.operation_id, None)
            else:
                self.pending_inventory_operations[reference.operation_id] = reference
            if snapshot is None:
                return
            self.inventory_operations[snapshot.reference.operation_id] = snapshot
            if snapshot.status is not InventoryOperationStatus.CONFIRMED:
                return
            if snapshot.reference.operation_id in self.applied_inventory_operations:
                return
            before = snapshot.reference.balance_before
            if (
                before is not None
                and snapshot.reference.action is OutcomeInventoryAction.REDEEM
            ):
                settlement = self.settlements.get(
                    (snapshot.reference.venue_id, before.market_id),
                )
                if settlement is not None:
                    snapshot = replace(
                        snapshot,
                        yes_payout=settlement.yes_payout,
                        no_payout=settlement.no_payout,
                        quality_flags=tuple(
                            flag
                            for flag in snapshot.quality_flags
                            if flag != "AGGREGATE_PAYOUT_ALLOCATION"
                        ),
                    )
                    self.inventory_operations[
                        snapshot.reference.operation_id
                    ] = snapshot
            # Local import avoids coupling the domain state module to the
            # execution coordinator at import time.
            from prediction_markets.application.execution.accounting import (
                apply_inventory_operation,
            )

            try:
                result = apply_inventory_operation(self.positions, snapshot)
            except ValueError:
                # Journals written before inventory economics were introduced
                # remain replayable but cannot reconstruct their missing basis.
                return
            for position in result.positions:
                self.positions[position.id] = _marked_position(position, self.books)
            self.applied_inventory_operations.add(snapshot.reference.operation_id)
            return
        if isinstance(event, MarketSettlementRecorded):
            settlement = event.settlement
            self.settlements[(settlement.venue_id, settlement.market_id)] = settlement
            for contract_id, price in (
                (settlement.yes_contract_id, settlement.yes_payout),
                (settlement.no_contract_id, settlement.no_payout),
            ):
                for position_id, position in tuple(self.positions.items()):
                    if (
                        position.venue_id == settlement.venue_id
                        and position.contract_id == contract_id
                        and position.is_open()
                    ):
                        self.positions[position_id] = position.with_mark(
                            price,
                            settlement.observed_at,
                        )
            return
        if isinstance(event, CashMovementRecorded):
            self.cash_movements[event.movement.id] = event.movement
            return
        if isinstance(event, AccountingCorrectionRecorded):
            correction = event.correction
            if correction.id in self.accounting_corrections:
                return
            self.accounting_corrections[correction.id] = correction
            self.trades[correction.target_trade_id] = correction.replacement_trade
            self.positions[correction.resulting_position.id] = _marked_position(
                correction.resulting_position,
                self.books,
            )
            return
        if isinstance(event, ExecutionUpdated):
            self.executions[event.execution.id] = event.execution
            return
        if isinstance(event, (RecoveryPlanned, RecoveryUpdated)):
            self.recoveries[event.recovery.id] = event.recovery
            return
        if isinstance(event, TradingSafetyStop):
            self.trading_enabled = False
            self.safety_halted = True
            self.last_error = event.reason
            command = self.commands.get(event.client_order_id)
            execution = self.executions.get(event.execution_id)
            if (
                command is not None
                and execution is not None
                and command.execution_id == execution.id
                and command.venue_id == event.venue_id
                and execution.resolution_method is None
                and execution.status not in {
                    ArbitrageExecutionStatus.COMPLETED,
                    ArbitrageExecutionStatus.RECOVERED,
                    ArbitrageExecutionStatus.REJECTED,
                }
                and (
                    command.role == "recovery"
                    or event.client_order_id in {
                        execution.leg1_client_order_id,
                        execution.leg2_client_order_id,
                    }
                )
            ):
                # Retain the cause if replay stops before its derived review event.
                self.execution_safety_stops[execution.id] = event

    def current_pairs(
        self,
        cycle: MonitoredMarket,
    ) -> tuple[tuple[BinaryContract, BinaryContract], ...]:
        """Return current complementary pairs for one monitored selection."""
        return tuple((pair.left, pair.right) for pair in self.matches.get(cycle, ()))

    def affected_pairs(
        self,
        contract_id: ContractID,
    ) -> tuple[tuple[MonitoredMarket, MatchedContractPair], ...]:
        """Return matched pairs containing a changed contract."""
        return self._pairs_by_contract.get(contract_id, ())

    def _rebuild_pair_index(self) -> None:
        """Index pairs by contract while preserving match iteration order."""
        pairs_by_contract: dict[
            ContractID,
            list[tuple[MonitoredMarket, MatchedContractPair]],
        ] = {}
        for market, pairs in self.matches.items():
            for pair in pairs:
                for contract_id in (pair.left.id, pair.right.id):
                    pairs_by_contract.setdefault(contract_id, []).append(
                        (market, pair),
                    )
        self._pairs_by_contract = {
            contract_id: tuple(pairs)
            for contract_id, pairs in pairs_by_contract.items()
        }

    def is_pair_active(self, pair: MatchedContractPair) -> bool:
        """Report whether an unfinished execution already owns this pair."""
        return any(
            self.execution_pairs.get(execution_id) == pair.key
            and execution.status
            not in {
                ArbitrageExecutionStatus.COMPLETED,
                ArbitrageExecutionStatus.RECOVERED,
                ArbitrageExecutionStatus.REJECTED,
            }
            for execution_id, execution in self.executions.items()
        )

    def has_active_execution(self, market: MonitoredMarket) -> bool:
        """Report whether an unfinished execution belongs to one monitored market."""
        return any(
            self.execution_cycles.get(execution_id) == market
            and execution.status
            not in {
                ArbitrageExecutionStatus.COMPLETED,
                ArbitrageExecutionStatus.RECOVERED,
                ArbitrageExecutionStatus.REJECTED,
            }
            for execution_id, execution in self.executions.items()
        )

    def completed_executions(self) -> int:
        """Count run-quota executions: clean two-leg completes and recoveries.

        Returns
        -------
        int
            ``COMPLETED`` executions with both legs filled and no error, plus
            ``RECOVERED`` executions whose residual is already neutralized.
        """
        return sum(
            (
                execution.status is ArbitrageExecutionStatus.COMPLETED
                and execution.last_error is None
                and execution.leg2_filled_quantity.value > 0
            )
            or (
                execution.status is ArbitrageExecutionStatus.RECOVERED
                and execution.residual_quantity.value == 0
            )
            for execution in self.executions.values()
        )

def _inventory_snapshot(record: object) -> InventoryOperationSnapshot | None:
    """Return the normalized snapshot carried by an inventory record."""
    if isinstance(record, InventoryOperationSnapshot):
        return record
    if isinstance(record, (InventorySubmissionResult, InventoryReconciliationResult)):
        return record.snapshot
    return None


def _marked_position(
    position: Position,
    books: dict[ContractID, OrderBook],
) -> Position:
    """Apply the latest replayable midpoint to one open position when available."""
    book = books.get(position.contract_id)
    if not position.is_open() or book is None or book.timestamp is None:
        return position
    mark = book.mid_price()
    return position.with_mark(mark, book.timestamp) if mark is not None else position


class EventDispatcher:
    """Apply events and broadcast opportunity signals after state changes."""

    def __init__(self, state: TradingState, *, subscriber_capacity: int = 128) -> None:
        """
        Parameters
        ----------
        state
            Replayable state updated in journal order.
        subscriber_capacity : int, default=128
            Per-client pending signal-pair limit.
        """
        self.state = state
        self._subscriber_capacity = subscriber_capacity
        self._subscribers: dict[
            MonitoredMarket,
            set[asyncio.Queue[tuple[Signal, Signal]]],
        ] = {}

    def dispatch(self, event: ApplicationEvent, *, replay: bool = False) -> None:
        """Apply an event and notify live subscribers when appropriate."""
        self.state.apply(event)
        if not replay and isinstance(event, ArbitrageOpportunityFound):
            signals = opportunity_to_signals(event)
            for queue in tuple(self._subscribers.get(event.cycle, ())):
                if queue.full():
                    queue.get_nowait()
                queue.put_nowait(signals)

    async def stream_signal_pairs(
        self,
        cycle: MonitoredMarket,
    ) -> AsyncIterator[tuple[Signal, Signal]]:
        """Yield signal pairs for one selection until the consumer closes."""
        queue: asyncio.Queue[tuple[Signal, Signal]] = asyncio.Queue(
            self._subscriber_capacity,
        )
        subscribers = self._subscribers.setdefault(cycle, set())
        subscribers.add(queue)
        try:
            while True:
                yield await queue.get()
        finally:
            subscribers.discard(queue)
            if not subscribers:
                self._subscribers.pop(cycle, None)


def opportunity_to_signals(
    event: ArbitrageOpportunityFound,
) -> tuple[Signal, Signal]:
    """Convert one opportunity into two liquidity-ordered API signals.

    Returns
    -------
    tuple[Signal, Signal]
        Primary (less liquid) signal followed by the hedge signal.
    """
    opportunity = event.opportunity
    edge = Edge(opportunity.net_edge)
    direction = SignalDirection(opportunity.side.value)
    kind = "long" if opportunity.side is OrderSide.BUY else "short"
    reason = (
        f"{kind} arbitrage quantity={opportunity.quantity.value} "
        f"fee_per_contract={opportunity.fee_per_contract} "
        f"skew_ns={opportunity.skew_ns}"
    )
    common = {
        "direction": direction,
        "confidence": Confidence(Decimal("1")),
        "generated_at": opportunity.detected_at,
        "strategy_id": StrategyID(f"{kind}-arbitrage"),
        "edge": edge,
        "reason": reason,
        "quantity": opportunity.quantity,
    }
    left = Signal(
        contract_id=opportunity.left_contract_id,
        fair_probability=Probability(Decimal("1") - opportunity.right_level.price.value),
        venue_id=event.pair.left.venue_id,
        limit_price=opportunity.left_level.price,
        **common,
    )
    right = Signal(
        contract_id=opportunity.right_contract_id,
        fair_probability=Probability(Decimal("1") - opportunity.left_level.price.value),
        venue_id=event.pair.right.venue_id,
        limit_price=opportunity.right_level.price,
        **common,
    )
    if opportunity.right_level.quantity < opportunity.left_level.quantity:
        return right, left
    return left, right
