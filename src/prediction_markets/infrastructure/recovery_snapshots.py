"""Create compact recovery snapshots outside the trading hot path.

Responsibilities
----------------
- Replay only a durable journal prefix into isolated recovery state.
- Persist checksummed snapshots with atomic replacement.
- Retain raw segments until snapshots, SQL projection, and unresolved orders allow deletion.

Notes
-----
- Snapshots contain opaque prepared-order bytes and therefore use mode ``0600``.
- Order books and historical opportunities are disposable and are not snapshotted.
"""

import base64
import hashlib
import json
import os
import struct
import threading
from collections.abc import Callable
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path

from prediction_markets.application.codec import decode_event, encode_event
from prediction_markets.application.events import (
    AccountingCorrectionRecorded,
    ApplicationEvent,
    ArbitragePlanned,
    CashMovementRecorded,
    ExecutionUpdated,
    InventoryOperationRecorded,
    MarketSettlementRecorded,
    MarketMatchesUpdated,
    OrderPrepared,
    OrderCancellationPrepared,
    OrderSnapshotUpdated,
    PreparedExecutionBatch,
    PositionUpdated,
    RecoveryPlanned,
    RecoveryUpdated,
    SubmissionReceived,
    SubmitOrder,
    TradeRecorded,
    prepared_execution_events,
)
from prediction_markets.application.pipeline import JournalRecord
from prediction_markets.application.state import TradingState
from prediction_markets.domain.shared.value_objects import ClientOrderID, PositionID, TradeID
from prediction_markets.domain.trading.enums import (
    ArbitrageExecutionStatus,
    OrderStatus,
    SubmissionStatus,
    TimeInForce,
)
from prediction_markets.infrastructure.binary_journal import BinaryJournal

_MAGIC = b"PMS1"
_HEADER = struct.Struct(">4sQI32s")
_SCHEMA_VERSION = 1
_JOURNAL_FORMAT_VERSION = "PMJ1"
_MAX_SNAPSHOT_BYTES = 256 * 1024 * 1024
_SNAPSHOT_NAME = "snapshot-{sequence:020d}.pms"


class SnapshotCorruptionError(RuntimeError):
    """Report a recovery snapshot that cannot be trusted."""


@dataclass(frozen=True, slots=True)
class UnresolvedRecord:
    """Describe the earliest raw record required by an unfinished execution."""

    aggregate_id: str
    first_required_sequence: int
    status: str


@dataclass(frozen=True, slots=True)
class SnapshotRecord:
    """Expose a compact snapshot event through the journal replay protocol."""

    event: ApplicationEvent


@dataclass(frozen=True, slots=True)
class RecoverySnapshot:
    """Capture compact recovery state at one inclusive durable sequence.

    Attributes
    ----------
    journal_sequence
        Highest journal sequence represented by the snapshot.
    created_at
        UTC creation timestamp.
    application_version
        Informational package version used to create the snapshot.
    events
        Minimal event set needed to rebuild live recovery state.
    unresolved
        Raw-history retention gates for unfinished executions.
    """

    journal_sequence: int
    created_at: datetime
    application_version: str
    events: tuple[ApplicationEvent, ...]
    unresolved: tuple[UnresolvedRecord, ...]

    def records(self) -> tuple[JournalRecord, ...]:
        """Return compact events in the shape consumed by pipeline replay."""
        return tuple(SnapshotRecord(event) for event in self.events)

    @property
    def min_unresolved_sequence(self) -> int | None:
        """Return the earliest raw sequence required by unfinished work."""
        return min(
            (record.first_required_sequence for record in self.unresolved),
            default=None,
        )


class RecoverySnapshotStore:
    """Read and atomically write local checksummed recovery snapshots."""

    def __init__(self, directory: str | Path) -> None:
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, mode=0o700, exist_ok=True)

    def load_latest(self, *, through_sequence: int) -> RecoverySnapshot | None:
        """Load the newest valid snapshot no later than a journal boundary."""
        for path in reversed(self._paths(through_sequence)):
            try:
                return self._load(path)
            except (KeyError, OSError, SnapshotCorruptionError, TypeError, ValueError):
                continue
        return None

    def valid_snapshots(
        self,
        *,
        through_sequence: int,
    ) -> tuple[RecoverySnapshot, ...]:
        """Return valid snapshots ordered by increasing journal sequence."""
        snapshots: list[RecoverySnapshot] = []
        for path in self._paths(through_sequence):
            try:
                snapshots.append(self._load(path))
            except (KeyError, OSError, SnapshotCorruptionError, TypeError, ValueError):
                continue
        return tuple(snapshots)

    def save(self, snapshot: RecoverySnapshot) -> Path:
        """Persist one snapshot through fsync and atomic replacement."""
        payload = _encode_snapshot(snapshot)
        if len(payload) > _MAX_SNAPSHOT_BYTES:
            raise ValueError("Recovery snapshot exceeds the safety limit")
        checksum = hashlib.sha256(payload).digest()
        data = _HEADER.pack(
            _MAGIC,
            snapshot.journal_sequence,
            len(payload),
            checksum,
        ) + payload
        destination = self.directory / _SNAPSHOT_NAME.format(
            sequence=snapshot.journal_sequence,
        )
        temporary = destination.with_suffix(".tmp")
        flags = os.O_WRONLY | os.O_CREAT | os.O_TRUNC
        if hasattr(os, "O_CLOEXEC"):
            flags |= os.O_CLOEXEC
        if hasattr(os, "O_BINARY"):
            flags |= os.O_BINARY
        fd = os.open(temporary, flags, 0o600)
        try:
            _write_all(fd, data)
            os.fsync(fd)
        finally:
            os.close(fd)
        os.replace(temporary, destination)
        os.chmod(destination, 0o600)
        _fsync_directory(self.directory)
        return destination

    def prune(self, *, keep: int = 2) -> tuple[Path, ...]:
        """Keep only the newest generated snapshot files."""
        if keep < 2:
            raise ValueError("At least two snapshots are required for fallback")
        valid_paths = []
        for path in self._paths(None):
            try:
                self._load(path)
            except (KeyError, OSError, SnapshotCorruptionError, TypeError, ValueError):
                continue
            valid_paths.append(path)
        removed = tuple(valid_paths[:-keep])
        for path in removed:
            path.unlink()
        if removed:
            _fsync_directory(self.directory)
        return removed

    def _load(self, path: Path) -> RecoverySnapshot:
        data = path.read_bytes()
        if len(data) < _HEADER.size:
            raise SnapshotCorruptionError("Incomplete recovery snapshot header")
        magic, sequence, payload_size, checksum = _HEADER.unpack_from(data)
        payload = data[_HEADER.size :]
        if magic != _MAGIC or payload_size != len(payload):
            raise SnapshotCorruptionError("Invalid recovery snapshot framing")
        if payload_size > _MAX_SNAPSHOT_BYTES:
            raise SnapshotCorruptionError("Recovery snapshot exceeds the safety limit")
        if hashlib.sha256(payload).digest() != checksum:
            raise SnapshotCorruptionError("Recovery snapshot checksum mismatch")
        snapshot = _decode_snapshot(payload)
        if snapshot.journal_sequence != sequence or _path_sequence(path) != sequence:
            raise SnapshotCorruptionError("Recovery snapshot sequence mismatch")
        return snapshot

    def _paths(self, through_sequence: int | None) -> list[Path]:
        values = [
            path
            for path in self.directory.glob("snapshot-*.pms")
            if _path_sequence(path) is not None
            and (through_sequence is None or _path_sequence(path) <= through_sequence)
        ]
        return sorted(values, key=lambda path: _path_sequence(path) or 0)


class JournalMaintenanceWorker:
    """Create snapshots and evaluate segment retention in one background thread.

    Notes
    -----
    - ``retention_mode='dry-run'`` is the default and never removes journal segments.
    - Real deletion is limited to closed segments behind every configured gate.
    """

    def __init__(
        self,
        journal: BinaryJournal,
        store: RecoverySnapshotStore,
        *,
        interval_seconds: float = 30.0,
        retention_mode: str = "dry-run",
        projected_sequence: Callable[[], int | None] | None = None,
    ) -> None:
        if interval_seconds <= 0:
            raise ValueError("Snapshot interval must be positive")
        if retention_mode not in {"dry-run", "delete"}:
            raise ValueError("Retention mode must be 'dry-run' or 'delete'")
        self._journal = journal
        self._store = store
        self._interval_seconds = interval_seconds
        self._retention_mode = retention_mode
        self._projected_sequence = projected_sequence
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._run_lock = threading.Lock()
        self.error: BaseException | None = None
        self.last_snapshot_sequence = 0
        self.safe_delete_sequence = 0
        self.eligible_segments: tuple[Path, ...] = ()

    def start(self) -> None:
        """Start background maintenance idempotently."""
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._run,
            name="journal-snapshot-worker",
            daemon=True,
        )
        self._thread.start()

    def close(self) -> None:
        """Stop and join the maintenance thread."""
        self._stop.set()
        thread = self._thread
        if thread is not None:
            thread.join()
        self._thread = None

    def run_once(self) -> RecoverySnapshot | None:
        """Snapshot the current durable prefix and evaluate retention once."""
        with self._run_lock:
            through_sequence = self._journal.durable_sequence
            if through_sequence <= 0:
                return None
            previous = self._store.load_latest(through_sequence=through_sequence)
            if previous is None or previous.journal_sequence < through_sequence:
                snapshot = build_recovery_snapshot(
                    self._journal,
                    previous=previous,
                    through_sequence=through_sequence,
                )
                self._store.save(snapshot)
                self._store.prune(keep=2)
            else:
                snapshot = previous
            self.last_snapshot_sequence = snapshot.journal_sequence
            self._apply_retention(snapshot)
            self.error = None
            return snapshot

    def _run(self) -> None:
        while not self._stop.wait(self._interval_seconds):
            try:
                self.run_once()
            except Exception as error:
                self.error = error

    def _apply_retention(self, latest: RecoverySnapshot) -> None:
        snapshots = self._store.valid_snapshots(
            through_sequence=latest.journal_sequence,
        )
        if len(snapshots) < 2:
            self.safe_delete_sequence = 0
            self.eligible_segments = ()
            return
        gates = [snapshots[-2].journal_sequence]
        if self._projected_sequence is not None:
            projected = self._projected_sequence()
            if projected is None or projected <= 0:
                self.safe_delete_sequence = 0
                self.eligible_segments = ()
                return
            gates.append(projected)
        if latest.min_unresolved_sequence is not None:
            gates.append(max(0, latest.min_unresolved_sequence - 1))
        safe = min(gates)
        self.safe_delete_sequence = safe
        self.eligible_segments = self._journal.delete_closed_segments_through(
            safe,
            dry_run=self._retention_mode == "dry-run",
        )


def build_recovery_snapshot(
    journal: BinaryJournal,
    *,
    previous: RecoverySnapshot | None,
    through_sequence: int,
) -> RecoverySnapshot:
    """Build compact state from a prior snapshot and an exact durable journal tail.

    Parameters
    ----------
    journal
        Segmented journal supplying validated records.
    previous
        Latest valid compact base, if one exists.
    through_sequence
        Inclusive durability boundary. Records above it are never read.

    Returns
    -------
    RecoverySnapshot
        Compact state representing exactly records at or below the boundary.
    """
    if through_sequence < 0:
        raise ValueError("Snapshot sequence must be non-negative")
    after_sequence = previous.journal_sequence if previous is not None else 0
    if after_sequence > through_sequence:
        raise ValueError("Previous snapshot is ahead of the requested boundary")
    if through_sequence > journal.durable_sequence:
        raise ValueError("Recovery snapshots may cover only durable journal records")
    if journal.retained_through_sequence > after_sequence:
        raise RuntimeError("No valid snapshot covers the retained journal prefix")
    accumulator = _RecoveryAccumulator(previous)
    for entry in journal.iter_entries(
        after_sequence=after_sequence,
        through_sequence=through_sequence,
    ):
        events = (
            prepared_execution_events(entry.event)
            if isinstance(entry.event, PreparedExecutionBatch)
            else (entry.event,)
        )
        for event in events:
            accumulator.apply(event, sequence=entry.sequence)
    return RecoverySnapshot(
        journal_sequence=through_sequence,
        created_at=datetime.now(timezone.utc),
        application_version=_application_version(),
        events=accumulator.compact_events(),
        unresolved=accumulator.unresolved_records(),
    )


class _RecoveryAccumulator:
    """Reduce a prior compact state and journal tail to current recovery facts."""

    def __init__(self, previous: RecoverySnapshot | None) -> None:
        self.state = TradingState()
        self.plans: dict[str, ArbitragePlanned] = {}
        self.commands: dict[ClientOrderID, SubmitOrder] = {}
        self.prepared: dict[ClientOrderID, OrderPrepared] = {}
        self.cancellations: dict[ClientOrderID, OrderCancellationPrepared] = {}
        self.order_events: dict[
            ClientOrderID,
            SubmissionReceived | OrderSnapshotUpdated,
        ] = {}
        self.trades: dict[TradeID, TradeRecorded] = {}
        self.positions: dict[PositionID, PositionUpdated] = {}
        self.inventory_operations: dict[str, InventoryOperationRecorded] = {}
        self.settlements: dict[tuple[str, str], MarketSettlementRecorded] = {}
        self.cash_movements: dict[str, CashMovementRecorded] = {}
        self.accounting_corrections: dict[str, AccountingCorrectionRecorded] = {}
        self.recoveries: dict[str, RecoveryPlanned | RecoveryUpdated] = {}
        self.terminal: set[ClientOrderID] = set()
        self.first_required: dict[str, int] = {
            record.aggregate_id: record.first_required_sequence
            for record in previous.unresolved
        } if previous is not None else {}
        if previous is not None:
            for event in previous.events:
                self.apply(
                    event,
                    sequence=previous.journal_sequence,
                    assign_trade_sequence=False,
                )

    def apply(
        self,
        event: ApplicationEvent,
        *,
        sequence: int,
        assign_trade_sequence: bool = True,
    ) -> None:
        """Apply one event and retain its latest recovery-relevant representation."""
        if (
            isinstance(event, TradeRecorded)
            and assign_trade_sequence
            and event.trade.journal_sequence is None
        ):
            event = TradeRecorded(
                replace(event.trade, journal_sequence=sequence),
            )
        self.state.apply(event)
        if isinstance(event, ArbitragePlanned):
            execution_id = event.execution.id
            self.plans[execution_id] = event
            self.first_required.setdefault(execution_id, sequence)
        elif isinstance(event, SubmitOrder):
            client_order_id = event.intent.client_order_id
            if client_order_id is not None:
                self.commands[client_order_id] = event
                self.first_required.setdefault(event.execution_id, sequence)
        elif isinstance(event, OrderPrepared):
            client_order_id = event.prepared.reference.client_order_id
            self.prepared[client_order_id] = event
        elif isinstance(event, OrderCancellationPrepared):
            self.cancellations[event.command.intent.client_order_id] = event
        elif isinstance(event, SubmissionReceived):
            client_order_id = event.result.reference.client_order_id
            self.order_events[client_order_id] = event
            if event.result.status is SubmissionStatus.REJECTED or (
                event.result.snapshot is not None
                and _settled(event.command, event.result.snapshot.status)
            ):
                self.terminal.add(client_order_id)
        elif isinstance(event, OrderSnapshotUpdated):
            client_order_id = event.reference.client_order_id
            self.order_events[client_order_id] = event
            if (
                event.snapshot.is_terminal()
                or event.snapshot.status is OrderStatus.PARTIALLY_FILLED
            ):
                self.terminal.add(client_order_id)
        elif isinstance(event, TradeRecorded):
            self.trades[event.trade.id] = event
        elif isinstance(event, PositionUpdated):
            self.positions[event.position.id] = event
        elif isinstance(event, InventoryOperationRecorded):
            self.inventory_operations[str(event.record.reference.operation_id)] = event
        elif isinstance(event, MarketSettlementRecorded):
            self.settlements[
                (str(event.settlement.venue_id), str(event.settlement.market_id))
            ] = event
        elif isinstance(event, CashMovementRecorded):
            self.cash_movements[event.movement.id] = event
        elif isinstance(event, AccountingCorrectionRecorded):
            self.accounting_corrections[event.correction.id] = event
        elif isinstance(event, (RecoveryPlanned, RecoveryUpdated)):
            recovery_id = event.recovery.id
            self.recoveries[recovery_id] = event
            self.first_required.setdefault(recovery_id, sequence)

    def compact_events(self) -> tuple[ApplicationEvent, ...]:
        """Return events sufficient to reconstruct current live recovery state."""
        aggregate_ids = self._aggregate_ids()
        active_plans = [
            event for execution_id, event in self.plans.items()
            if execution_id in aggregate_ids
        ]
        events: list[ApplicationEvent] = []
        active_cycles = []
        for plan in active_plans:
            events.append(MarketMatchesUpdated(plan.cycle, (plan.pair,)))
            if plan.cycle not in active_cycles:
                active_cycles.append(plan.cycle)
        final_cycles = set()
        for cycle in (*active_cycles, *self.state.matches):
            if cycle in final_cycles:
                continue
            events.append(MarketMatchesUpdated(cycle, self.state.matches.get(cycle, ())))
            final_cycles.add(cycle)
        events.extend(active_plans)
        for execution_id in aggregate_ids:
            execution = self.state.executions.get(execution_id)
            if execution is not None:
                events.append(ExecutionUpdated(execution))
            recovery = self.recoveries.get(execution_id)
            if recovery is not None:
                events.append(recovery)
        commands = [
            command for command in self.commands.values()
            if command.execution_id in aggregate_ids
        ]
        events.extend(commands)
        events.extend(
            event for event in self.cancellations.values()
            if event.command.execution_id in aggregate_ids
        )
        command_ids = {
            command.intent.client_order_id
            for command in commands
            if command.intent.client_order_id is not None
        }
        events.extend(
            event for client_order_id, event in self.prepared.items()
            if client_order_id in command_ids
        )
        events.extend(
            event for client_order_id, event in self.order_events.items()
            if client_order_id in command_ids
        )
        events.extend(
            stop for execution_id, stop in self.state.execution_safety_stops.items()
            if execution_id in aggregate_ids
        )
        # Trades are the accounting source of truth and must survive compaction
        # even after their execution aggregate becomes terminal.
        events.extend(self.trades.values())
        events.extend(self.settlements.values())
        events.extend(self.inventory_operations.values())
        events.extend(self.cash_movements.values())
        events.extend(self.accounting_corrections.values())
        events.extend(
            PositionUpdated(position)
            for position in self.state.positions.values()
        )
        return tuple(events)

    def unresolved_records(self) -> tuple[UnresolvedRecord, ...]:
        """Return raw-history gates for active or unreconciled executions."""
        records = []
        for execution_id in self._aggregate_ids():
            execution = self.state.executions.get(execution_id)
            records.append(
                UnresolvedRecord(
                    aggregate_id=execution_id,
                    first_required_sequence=self.first_required[execution_id],
                    status=(
                        execution.status.value
                        if execution is not None
                        else "pending_reconciliation"
                    ),
                ),
            )
        return tuple(records)

    def _aggregate_ids(self) -> tuple[str, ...]:
        """Return executions that are active or still have a non-terminal command."""
        active = {
            execution_id
            for execution_id, execution in self.state.executions.items()
            if execution.status
            not in {
                ArbitrageExecutionStatus.COMPLETED,
                ArbitrageExecutionStatus.RECOVERED,
                ArbitrageExecutionStatus.REJECTED,
            }
        }
        active.update(
            command.execution_id
            for client_order_id, command in self.commands.items()
            if client_order_id not in self.terminal
        )
        return tuple(
            execution_id
            for execution_id in self.first_required
            if execution_id in active
        )


def _settled(command: SubmitOrder, status: OrderStatus) -> bool:
    """Apply the same terminal-submission rule used by restart reconciliation."""
    return status in {
        OrderStatus.FILLED,
        OrderStatus.CANCELLED,
        OrderStatus.REJECTED,
        OrderStatus.EXPIRED,
    } or (
        command.intent.time_in_force in {TimeInForce.IOC, TimeInForce.FOK}
        and status is OrderStatus.PARTIALLY_FILLED
    )


def _encode_snapshot(snapshot: RecoverySnapshot) -> bytes:
    """Serialize approved snapshot fields without executable Python payloads."""
    payload = {
        "schema_version": _SCHEMA_VERSION,
        "journal_format_version": _JOURNAL_FORMAT_VERSION,
        "journal_sequence": snapshot.journal_sequence,
        "created_at": snapshot.created_at.isoformat(),
        "application_version": snapshot.application_version,
        "events": [
            base64.b64encode(encode_event(event)).decode("ascii")
            for event in snapshot.events
        ],
        "unresolved": [
            {
                "aggregate_id": record.aggregate_id,
                "first_required_sequence": record.first_required_sequence,
                "status": record.status,
            }
            for record in snapshot.unresolved
        ],
    }
    return json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()


def _decode_snapshot(payload: bytes) -> RecoverySnapshot:
    """Decode and validate one checksummed snapshot payload."""
    data = json.loads(payload)
    if not isinstance(data, dict) or data.get("schema_version") != _SCHEMA_VERSION:
        raise SnapshotCorruptionError("Unsupported recovery snapshot schema")
    if data.get("journal_format_version") != _JOURNAL_FORMAT_VERSION:
        raise SnapshotCorruptionError("Unsupported journal format version")
    sequence = data.get("journal_sequence")
    if type(sequence) is not int or sequence < 0:
        raise SnapshotCorruptionError("Invalid recovery snapshot sequence")
    raw_events = data.get("events")
    raw_unresolved = data.get("unresolved")
    if not isinstance(raw_events, list) or not all(
        isinstance(value, str) for value in raw_events
    ):
        raise SnapshotCorruptionError("Invalid recovery snapshot events")
    if not isinstance(raw_unresolved, list) or not all(
        isinstance(value, dict)
        and isinstance(value.get("aggregate_id"), str)
        and type(value.get("first_required_sequence")) is int
        and isinstance(value.get("status"), str)
        for value in raw_unresolved
    ):
        raise SnapshotCorruptionError("Invalid unresolved recovery records")
    events = tuple(
        decode_event(base64.b64decode(value, validate=True))
        for value in raw_events
    )
    unresolved = tuple(UnresolvedRecord(**value) for value in raw_unresolved)
    if any(
        not record.aggregate_id
        or record.first_required_sequence <= 0
        or record.first_required_sequence > sequence
        for record in unresolved
    ):
        raise SnapshotCorruptionError("Invalid unresolved recovery record")
    created_at = datetime.fromisoformat(data["created_at"])
    if created_at.tzinfo is None:
        raise SnapshotCorruptionError("Recovery snapshot timestamp must be timezone-aware")
    return RecoverySnapshot(
        journal_sequence=sequence,
        created_at=created_at,
        application_version=str(data["application_version"]),
        events=events,
        unresolved=unresolved,
    )


def _path_sequence(path: Path) -> int | None:
    try:
        return int(path.stem.removeprefix("snapshot-"))
    except ValueError:
        return None


def _application_version() -> str:
    try:
        return version("prediction-markets")
    except PackageNotFoundError:
        return "development"


def _write_all(fd: int, data: bytes) -> None:
    view = memoryview(data)
    while view:
        written = os.write(fd, view)
        if written <= 0:
            raise OSError("Snapshot write made no progress")
        view = view[written:]


def _fsync_directory(path: Path) -> None:
    if os.name == "nt" or not hasattr(os, "O_DIRECTORY"):
        return
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
