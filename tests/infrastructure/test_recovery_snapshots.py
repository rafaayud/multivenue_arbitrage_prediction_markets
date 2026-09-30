"""Verify durable-prefix snapshots, fallback, and gated segment retention."""

from dataclasses import replace
from decimal import Decimal

from prediction_markets.application.events import (
    ArbitragePlanned,
    InventoryOperationRecorded,
    MarketMatchesUpdated,
    OrderPrepared,
    SubmitOrder,
)
from prediction_markets.application.markets.models import MarketCycle
from prediction_markets.application.state import TradingState
from prediction_markets.domain.arbitrage.services import ArbitrageLegPlan, ArbitragePlan
from prediction_markets.domain.contracts.entities import BinaryContract
from prediction_markets.domain.outcome_inventory import (
    InventoryOperationID,
    InventoryOperationReference,
    InventorySubmissionResult,
    InventorySubmissionStatus,
    OutcomeInventoryAction,
    OutcomeInventoryIntent,
    PreparedInventoryOperation,
)
from prediction_markets.domain.market_matching.value_objects import (
    MatchedContractPair,
    Underlying,
)
from prediction_markets.domain.shared.value_objects import (
    ClientOrderID,
    ContractID,
    Currency,
    MarketID,
    OutcomeID,
    Price,
    Quantity,
    Timestamp,
    VenueID,
)
from prediction_markets.domain.trading.entities import OrderIntent
from prediction_markets.domain.trading.enums import (
    ArbitrageExecutionStatus,
    OrderSide,
    OrderType,
    TimeInForce,
)
from prediction_markets.domain.trading.entities import ArbitrageExecutionJournal
from prediction_markets.domain.trading.value_objects import OrderReference, PreparedOrder
from prediction_markets.infrastructure.binary_journal import BinaryJournal
from prediction_markets.infrastructure.recovery_snapshots import (
    JournalMaintenanceWorker,
    RecoverySnapshotStore,
    build_recovery_snapshot,
)


def test_snapshot_rebuilds_active_execution_from_exact_durable_prefix(tmp_path) -> None:
    """Keep recovery-critical state while excluding a newer non-durable event."""
    journal = BinaryJournal(tmp_path / "events.log")
    cycle, pair, planned, command, prepared = _active_execution()
    for event in (
        MarketMatchesUpdated(cycle, (pair,)),
        planned,
        command,
        prepared,
        MarketMatchesUpdated(cycle, ()),
    ):
        journal.append(event)
    journal.sync()
    durable = journal.durable_sequence
    journal.append(MarketMatchesUpdated(cycle, (pair,)))

    snapshot = build_recovery_snapshot(
        journal,
        previous=None,
        through_sequence=durable,
    )
    full_replay = TradingState()
    for entry in journal.entries(through_sequence=durable):
        full_replay.apply(entry.event)
    restored = TradingState()
    for record in snapshot.records():
        restored.apply(record.event)

    assert restored.matches == full_replay.matches
    assert restored.executions == full_replay.executions
    assert restored.commands == full_replay.commands
    assert restored.prepared == full_replay.prepared
    assert pair.left.id in restored.contracts
    assert snapshot.unresolved[0].first_required_sequence == 2
    journal.close()


def test_snapshot_preserves_latest_pending_inventory_reference(tmp_path) -> None:
    """Retain uncertain inventory recovery after compaction and clear rejections."""
    journal = BinaryJournal(tmp_path / "inventory.log")
    intent = OutcomeInventoryIntent(
        InventoryOperationID("split-1"), VenueID("LIMITLESS"), MarketID("market"),
        OutcomeInventoryAction.SPLIT, Quantity(Decimal("5")),
    )
    prepared_ref = InventoryOperationReference(intent.venue_id, intent.operation_id, b"prepared")
    latest_ref = replace(prepared_ref, recovery_data=b"latest")
    journal.append(InventoryOperationRecorded(PreparedInventoryOperation(intent, prepared_ref, b"request")))
    journal.append(InventoryOperationRecorded(InventorySubmissionResult(
        InventorySubmissionStatus.UNKNOWN, latest_ref,
    )))
    journal.sync()
    snapshot = build_recovery_snapshot(
        journal, previous=None, through_sequence=journal.durable_sequence,
    )
    state = TradingState()
    for record in snapshot.records():
        state.apply(record.event)
    assert state.pending_inventory_operations == {intent.operation_id: latest_ref}
    journal.append(InventoryOperationRecorded(InventorySubmissionResult(
        InventorySubmissionStatus.REJECTED, latest_ref, reason="rejected",
    )))
    journal.sync()
    snapshot = build_recovery_snapshot(
        journal, previous=snapshot, through_sequence=journal.durable_sequence,
    )
    restored = TradingState()
    for record in snapshot.records():
        restored.apply(record.event)
    assert not restored.pending_inventory_operations
    journal.close()


def test_snapshot_store_falls_back_when_the_newest_checksum_is_invalid(tmp_path) -> None:
    """Select the previous valid snapshot after an incomplete or corrupt write."""
    journal = BinaryJournal(tmp_path / "events.log")
    cycle = MarketCycle(Underlying("BTC"), 300)
    journal.append(MarketMatchesUpdated(cycle, ()))
    journal.sync()
    first = build_recovery_snapshot(journal, previous=None, through_sequence=1)
    store = RecoverySnapshotStore(tmp_path / "snapshots")
    store.save(first)
    second = replace(first, journal_sequence=2)
    second_path = store.save(second)
    second_path.write_bytes(second_path.read_bytes() + b"corrupt")

    recovered = store.load_latest(through_sequence=2)

    assert recovered == first
    journal.close()


def test_retention_defaults_to_dry_run_and_deletes_only_closed_safe_segments(
    tmp_path,
) -> None:
    """Require two snapshots before deleting a fully covered closed segment."""
    path = tmp_path / "events.log"
    journal = BinaryJournal(path, segment_size_bytes=1)
    store = RecoverySnapshotStore(tmp_path / "snapshots")
    cycle = MarketCycle(Underlying("BTC"), 300)
    event = MarketMatchesUpdated(cycle, ())
    dry_run = JournalMaintenanceWorker(journal, store)

    journal.append(event)
    journal.sync()
    dry_run.run_once()
    journal.append(event)
    journal.sync()
    dry_run.run_once()

    assert dry_run.safe_delete_sequence == 1
    assert dry_run.eligible_segments == (path,)
    assert path.exists()

    delete = JournalMaintenanceWorker(journal, store, retention_mode="delete")
    delete.run_once()
    assert not path.exists()
    journal.close()


def test_unresolved_execution_keeps_its_first_required_segment(tmp_path) -> None:
    """Stop retention immediately before the first unfinished execution record."""
    path = tmp_path / "events.log"
    journal = BinaryJournal(path, segment_size_bytes=1)
    store = RecoverySnapshotStore(tmp_path / "snapshots")
    worker = JournalMaintenanceWorker(journal, store)
    cycle, pair, planned, command, prepared = _active_execution()

    journal.append(MarketMatchesUpdated(cycle, (pair,)))
    journal.sync()
    worker.run_once()
    for event in (planned, command, prepared):
        journal.append(event)
        journal.sync()
    worker.run_once()
    journal.append(MarketMatchesUpdated(cycle, ()))
    journal.sync()
    worker.run_once()

    assert worker.safe_delete_sequence == 1
    assert all(
        segment.last_sequence <= 1
        for segment in journal.segments()
        if segment.path in worker.eligible_segments
    )
    journal.close()


def _active_execution():
    left_venue = VenueID("left")
    right_venue = VenueID("right")
    left = _contract("left", left_venue, "yes")
    right = _contract("right", right_venue, "no")
    pair = MatchedContractPair(left, right, Timestamp.now())
    cycle = MarketCycle(Underlying("BTC"), 300)
    quantity = Quantity(Decimal("5"))
    left_price = Price(Decimal("0.4"))
    right_price = Price(Decimal("0.5"))
    primary_id = ClientOrderID("primary-client")
    hedge_id = ClientOrderID("hedge-client")
    plan = ArbitragePlan(
        side=OrderSide.BUY,
        quantity=quantity,
        legs=(
            ArbitrageLegPlan(left.id, OrderSide.BUY, quantity, left_price, "primary"),
            ArbitrageLegPlan(right.id, OrderSide.BUY, quantity, right_price, "hedge"),
        ),
        net_edge=Decimal("0.1"),
        fee_per_contract=Decimal("0"),
    )
    now = Timestamp.now()
    execution = ArbitrageExecutionJournal(
        id="execution-1",
        status=ArbitrageExecutionStatus.PLANNED,
        leg1_venue_id=left_venue,
        leg1_contract_id=left.id,
        leg1_side=OrderSide.BUY,
        leg1_quantity=quantity,
        leg1_limit_price=left_price,
        leg1_client_order_id=primary_id,
        leg2_venue_id=right_venue,
        leg2_contract_id=right.id,
        leg2_side=OrderSide.BUY,
        leg2_quantity=quantity,
        leg2_limit_price=right_price,
        leg2_client_order_id=hedge_id,
        created_at=now,
        updated_at=now,
    )
    planned = ArbitragePlanned("opportunity-1", cycle, pair, plan, execution)
    command = SubmitOrder(
        execution.id,
        "primary",
        left_venue,
        OrderIntent(
            contract_id=left.id,
            side=OrderSide.BUY,
            quantity=quantity,
            order_type=OrderType.LIMIT,
            client_order_id=primary_id,
            limit_price=left_price,
            time_in_force=TimeInForce.IOC,
        ),
    )
    value = PreparedOrder(
        OrderReference(left_venue, primary_id, b"recovery"),
        b"signed-request",
    )
    return cycle, pair, planned, command, OrderPrepared(command, value)


def _contract(name: str, venue_id: VenueID, outcome: str) -> BinaryContract:
    return BinaryContract(
        id=ContractID(name),
        market_id=MarketID(f"{name}-market"),
        outcome_id=OutcomeID(outcome),
        venue_id=venue_id,
        payout_currency=Currency("USD"),
    )
