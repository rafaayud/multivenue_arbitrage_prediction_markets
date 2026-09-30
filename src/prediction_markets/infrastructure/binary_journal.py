"""Persist the ordered hot-path event stream in append-only binary segments.

Responsibilities
----------------
- Frame typed events with a global sequence, wall-clock timestamp, length, and CRC.
- Append to the operating-system page cache without waiting for disk synchronization.
- Rotate only fully durable, quiet segments from the background sync path.
- Read retained segments as one continuous sequence and repair only the active tail.
"""

from __future__ import annotations

import os
import re
import struct
import threading
import time
import zlib
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from prediction_markets.application.codec import decode_event, encode_event
from prediction_markets.application.events import ApplicationEvent
from prediction_markets.domain.shared.value_objects import Timestamp

_MAGIC = b"PMJ1"
_HEADER = struct.Struct(">4sQQII")
_MAX_PAYLOAD_BYTES = 16 * 1024 * 1024
_DEFAULT_SEGMENT_SIZE_BYTES = 64 * 1024 * 1024
_SEGMENT_NAME = re.compile(r"segment-(\d{8})\.pmj$")


class JournalCorruptionError(RuntimeError):
    """Report a complete journal frame or segment sequence that cannot be trusted."""


@dataclass(frozen=True, slots=True)
class JournalEntry:
    """Represent one validated journal frame.

    Attributes
    ----------
    sequence
        Monotonic global sequence shared by input, output, and prepared events.
    recorded_at
        UTC wall-clock time captured immediately before append.
    event
        Reconstructed typed application event.
    payload
        Exact encoded event bytes protected by the frame CRC.
    """

    sequence: int
    recorded_at: Timestamp
    event: ApplicationEvent
    payload: bytes


@dataclass(frozen=True, slots=True)
class JournalSegment:
    """Describe one retained journal segment and its inclusive sequence range."""

    path: Path
    first_sequence: int
    last_sequence: int
    size_bytes: int
    is_closed: bool


@dataclass(slots=True)
class _SegmentState:
    path: Path
    first_sequence: int
    last_sequence: int


class Sequencer:
    """Assign strictly increasing process-local journal sequences."""

    def __init__(self, last_sequence: int = 0) -> None:
        if last_sequence < 0:
            raise ValueError("last_sequence must be non-negative")
        self._value = last_sequence

    def next(self) -> int:
        """Return the next sequence.

        Notes
        -----
        - Callers must serialize this method with the corresponding append.
        """
        self._value += 1
        return self._value


class BinaryJournal:
    """Append events while segment maintenance remains outside the hot path.

    Notes
    -----
    - ``append`` waits for one page-cache write, never for ``fdatasync``.
    - The existing append lock remains the sole writer-serialization mechanism.
    - Rotation occurs after background synchronization only when no newer append
      raced with that synchronization.
    - The legacy configured file remains the first segment for compatibility.
    - Journal files use mode ``0600`` because prepared orders contain signatures.
    """

    def __init__(
        self,
        path: str | Path,
        *,
        sync_interval_seconds: float = 0.01,
        segment_size_bytes: int = _DEFAULT_SEGMENT_SIZE_BYTES,
        on_append: Callable[[ApplicationEvent], None] | None = None,
    ) -> None:
        """
        Parameters
        ----------
        path
            Legacy first-segment path and anchor for the segment directory.
        sync_interval_seconds : float, default=0.01
            Maximum normal interval between background durability attempts.
        segment_size_bytes : int, default=67108864
            Target closed-segment size. Rotation may overshoot until one sync
            observes a quiet, fully durable segment.
        on_append
            Optional non-blocking diagnostic observer. Its failures cannot
            invalidate an event already appended to the financial journal.
        """
        if sync_interval_seconds <= 0:
            raise ValueError("sync_interval_seconds must be positive")
        if segment_size_bytes <= 0:
            raise ValueError("segment_size_bytes must be positive")

        self.path = Path(path)
        self._on_append = on_append
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._segment_directory = self.path.parent / f"{self.path.name}.segments"
        self._segment_directory.mkdir(mode=0o700, exist_ok=True)
        self._sync_interval_seconds = sync_interval_seconds
        self._segment_size_bytes = segment_size_bytes
        self._lock = threading.Lock()
        self._condition = threading.Condition(self._lock)
        self._maintenance_lock = threading.RLock()
        self._sync_lock = threading.Lock()
        self._stop = threading.Event()
        self._sync_thread: threading.Thread | None = None
        self._closed = False

        segment_paths = self._discover_segments()
        if not segment_paths:
            self.path.touch(mode=0o600, exist_ok=True)
            segment_paths = [self.path]

        self._segments, last_sequence = _scan_segments(segment_paths)
        self._next_segment_index = _next_segment_index(segment_paths)
        self._active_path = self._segments[-1].path
        flags = os.O_WRONLY | os.O_APPEND | os.O_CREAT
        if hasattr(os, "O_CLOEXEC"):
            flags |= os.O_CLOEXEC
        if hasattr(os, "O_BINARY"):
            flags |= os.O_BINARY
        self._fd = os.open(self._active_path, flags, 0o600)
        os.chmod(self._active_path, 0o600)
        self._sequencer = Sequencer(last_sequence)
        self._last_sequence = last_sequence
        self._durable_sequence = last_sequence

    @property
    def last_sequence(self) -> int:
        """Return the most recently page-cache-appended sequence."""
        with self._lock:
            return self._last_sequence

    @property
    def durable_sequence(self) -> int:
        """Return the highest sequence covered by a completed sync."""
        with self._lock:
            return self._durable_sequence

    @property
    def first_sequence(self) -> int:
        """Return the first retained sequence, or the next sequence when empty."""
        with self._lock:
            return self._segments[0].first_sequence

    @property
    def retained_through_sequence(self) -> int:
        """Return the highest sequence intentionally removed from local segments."""
        return self.first_sequence - 1

    def append(self, event: ApplicationEvent) -> JournalEntry:
        """Append one event to page cache and return its assigned sequence.

        Parameters
        ----------
        event
            Input, internal event, or command to record before further processing.

        Returns
        -------
        JournalEntry
            The sequence and exact encoded value accepted by the journal.

        Raises
        ------
        RuntimeError
            If the journal has already been closed.
        """
        payload = encode_event(event)
        if len(payload) > _MAX_PAYLOAD_BYTES:
            raise ValueError("Journal event exceeds the maximum frame size")
        recorded_at_ns = time.time_ns()
        crc = zlib.crc32(payload)
        with self._lock:
            if self._closed:
                raise RuntimeError("BinaryJournal is closed")
            sequence = self._sequencer.next()
            frame = _HEADER.pack(
                _MAGIC,
                sequence,
                recorded_at_ns,
                len(payload),
                crc,
            ) + payload
            _write_all(self._fd, frame)
            self._last_sequence = sequence
        if self._on_append is not None:
            try:
                self._on_append(event)
            except Exception:
                # Diagnostic evidence is disposable; financial events are not.
                pass
        return JournalEntry(
            sequence=sequence,
            recorded_at=Timestamp(
                datetime.fromtimestamp(recorded_at_ns / 1_000_000_000, timezone.utc),
            ),
            event=event,
            payload=payload,
        )

    def entries(
        self,
        *,
        after_sequence: int = 0,
        through_sequence: int | None = None,
    ) -> tuple[JournalEntry, ...]:
        """Read validated retained entries inside inclusive sequence boundaries."""
        return tuple(
            self.iter_entries(
                after_sequence=after_sequence,
                through_sequence=through_sequence,
            ),
        )

    def iter_entries(
        self,
        *,
        after_sequence: int = 0,
        through_sequence: int | None = None,
    ) -> Iterator[JournalEntry]:
        """
        Yield validated entries without materializing the complete retained journal.

        Parameters
        ----------
        after_sequence : int, default=0
            Exclude entries at or below this checkpoint.
        through_sequence : int, optional
            Exclude entries above this inclusive durability boundary.

        Yields
        ------
        JournalEntry
            Entries in strict global sequence order.
        """
        if after_sequence < 0:
            raise ValueError("after_sequence must be non-negative")
        if through_sequence is not None and through_sequence < 0:
            raise ValueError("through_sequence must be non-negative")

        with self._maintenance_lock:
            snapshots = self._segment_snapshots(after_sequence)
            expected: int | None = None
            for path, file_size in snapshots:
                for entry in _iter_file_entries(path, file_size):
                    if expected is not None and entry.sequence != expected:
                        raise JournalCorruptionError(
                            f"Journal sequence gap at {entry.sequence}; expected {expected}",
                        )
                    expected = entry.sequence + 1
                    if entry.sequence <= after_sequence:
                        continue
                    if through_sequence is not None and entry.sequence > through_sequence:
                        return
                    yield entry

    def segments(self) -> tuple[JournalSegment, ...]:
        """Return a stable description of retained closed and active segments."""
        with self._maintenance_lock:
            with self._lock:
                active = self._active_path
                values = tuple(
                    (
                        segment.path,
                        segment.first_sequence,
                        self._last_sequence
                        if segment.path == active
                        else segment.last_sequence,
                    )
                    for segment in self._segments
                )
                active_size = os.fstat(self._fd).st_size
            return tuple(
                JournalSegment(
                    path=path,
                    first_sequence=first,
                    last_sequence=last,
                    size_bytes=(active_size if path == active else path.stat().st_size),
                    is_closed=path != active,
                )
                for path, first, last in values
            )

    def delete_closed_segments_through(
        self,
        sequence: int,
        *,
        dry_run: bool = True,
    ) -> tuple[Path, ...]:
        """
        Delete only closed segments fully covered by a safe checkpoint.

        Parameters
        ----------
        sequence
            Inclusive safe deletion boundary.
        dry_run : bool, default=True
            When true, return eligible paths without changing the filesystem.

        Returns
        -------
        tuple[Path, ...]
            Eligible or deleted segment paths.
        """
        if sequence < 0:
            raise ValueError("Retention sequence must be non-negative")
        with self._maintenance_lock:
            with self._lock:
                candidates = tuple(
                    segment.path
                    for segment in self._segments[:-1]
                    if segment.last_sequence <= sequence
                )
            if dry_run or not candidates:
                return candidates
            for path in candidates:
                path.unlink()
            with self._lock:
                candidate_set = set(candidates)
                self._segments = [
                    segment
                    for segment in self._segments
                    if segment.path not in candidate_set
                ]
            _fsync_directory(self._segment_directory)
            _fsync_directory(self.path.parent)
            return candidates

    def start_sync_worker(self) -> None:
        """Start the sole periodic durability and rotation worker idempotently."""
        with self._lock:
            if self._closed:
                raise RuntimeError("BinaryJournal is closed")
            if self._sync_thread is not None and self._sync_thread.is_alive():
                return
            self._sync_thread = threading.Thread(
                target=self._sync_loop,
                name="binary-journal-fdatasync",
                daemon=True,
            )
            self._sync_thread.start()

    def sync(self) -> int:
        """Synchronize current pages, advance durability, and rotate if safe."""
        with self._sync_lock:
            with self._lock:
                if self._closed:
                    return self._durable_sequence
                target = self._last_sequence
                fd = self._fd
                already_durable = target <= self._durable_sequence
            if not already_durable:
                _sync_fd(fd)
            segment_size = os.fstat(fd).st_size
            with self._condition:
                if not already_durable:
                    self._durable_sequence = max(self._durable_sequence, target)
                should_rotate = (
                    not self._stop.is_set()
                    and fd == self._fd
                    and target == self._last_sequence
                    and segment_size >= self._segment_size_bytes
                    and self._last_sequence >= self._segments[-1].first_sequence
                )
                self._condition.notify_all()
                durable = self._durable_sequence
            rotated = should_rotate and self._rotate_if_quiet(fd, target)
        if rotated:
            _fsync_directory(self._segment_directory)
        return durable

    def wait_for_durable(self, after_sequence: int, timeout: float) -> int:
        """Wait for the durability cursor to advance beyond a checkpoint."""
        deadline = time.monotonic() + timeout
        with self._condition:
            while (
                self._durable_sequence <= after_sequence
                and not self._closed
                and (remaining := deadline - time.monotonic()) > 0
            ):
                self._condition.wait(remaining)
            return self._durable_sequence

    def close(self) -> None:
        """Stop workers, durably flush the remaining tail, and close the active file."""
        with self._lock:
            if self._closed:
                return
        self._stop.set()
        thread = self._sync_thread
        if thread is not None:
            thread.join()
        self.sync()
        with self._condition:
            self._closed = True
            os.close(self._fd)
            self._condition.notify_all()

    def _discover_segments(self) -> list[Path]:
        paths = [self.path] if self.path.exists() else []
        paths.extend(
            sorted(
                path
                for path in self._segment_directory.iterdir()
                if path.is_file() and _SEGMENT_NAME.fullmatch(path.name)
            ),
        )
        return paths

    def _segment_snapshots(self, after_sequence: int) -> tuple[tuple[Path, int], ...]:
        with self._lock:
            active = self._active_path
            segments = tuple(
                segment
                for segment in self._segments
                if (
                    self._last_sequence
                    if segment.path == active
                    else segment.last_sequence
                ) > after_sequence
            )
            active_size = os.fstat(self._fd).st_size
        return tuple(
            (
                segment.path,
                active_size if segment.path == active else segment.path.stat().st_size,
            )
            for segment in segments
        )

    def _rotate_if_quiet(self, expected_fd: int, expected_sequence: int) -> bool:
        path = self._segment_directory / f"segment-{self._next_segment_index:08d}.pmj"
        self._next_segment_index += 1
        flags = os.O_WRONLY | os.O_APPEND | os.O_CREAT | os.O_EXCL
        if hasattr(os, "O_CLOEXEC"):
            flags |= os.O_CLOEXEC
        if hasattr(os, "O_BINARY"):
            flags |= os.O_BINARY
        new_fd = os.open(path, flags, 0o600)
        with self._lock:
            if (
                self._stop.is_set()
                or self._fd != expected_fd
                or self._last_sequence != expected_sequence
            ):
                rotated = False
                old_fd = None
            else:
                rotated = True
                old_fd = self._fd
                self._segments[-1].last_sequence = self._last_sequence
                self._fd = new_fd
                self._active_path = path
                self._segments.append(
                    _SegmentState(
                        path=path,
                        first_sequence=self._last_sequence + 1,
                        last_sequence=self._last_sequence,
                    ),
                )
        if not rotated:
            os.close(new_fd)
            path.unlink()
            return False
        assert old_fd is not None
        os.close(old_fd)
        return True

    def _sync_loop(self) -> None:
        while not self._stop.wait(self._sync_interval_seconds):
            self.sync()


def _write_all(fd: int, data: bytes) -> None:
    view = memoryview(data)
    while view:
        written = os.write(fd, view)
        if written <= 0:
            raise OSError("Journal append made no progress")
        view = view[written:]


def _sync_fd(fd: int) -> None:
    if hasattr(os, "fdatasync"):
        os.fdatasync(fd)
    else:
        os.fsync(fd)


def _scan_segments(paths: list[Path]) -> tuple[list[_SegmentState], int]:
    segments: list[_SegmentState] = []
    expected: int | None = None
    last_sequence = 0
    for index, path in enumerate(paths):
        os.chmod(path, 0o600)
        first, last, valid_size = _scan_segment(
            path,
            expected_sequence=expected,
            repair_tail=index == len(paths) - 1,
        )
        if path.stat().st_size != valid_size:
            raise JournalCorruptionError("Journal recovery left an invalid segment size")
        if first is None:
            if index != len(paths) - 1:
                raise JournalCorruptionError("Only the active journal segment may be empty")
            first = last_sequence + 1
            last = last_sequence
        else:
            last_sequence = last
            expected = last + 1
        segments.append(_SegmentState(path, first, last))
    return segments, last_sequence


def _scan_segment(
    path: Path,
    *,
    expected_sequence: int | None,
    repair_tail: bool,
) -> tuple[int | None, int, int]:
    file_size = path.stat().st_size
    first_sequence: int | None = None
    last_sequence = (expected_sequence - 1) if expected_sequence is not None else 0
    valid_size = 0
    with path.open("rb") as stream:
        while valid_size < file_size:
            start = valid_size
            raw_header = stream.read(_HEADER.size)
            if len(raw_header) < _HEADER.size:
                if repair_tail:
                    os.truncate(path, start)
                    return first_sequence, last_sequence, start
                raise JournalCorruptionError("Incomplete frame in a closed journal segment")
            magic, sequence, _, payload_size, expected_crc = _HEADER.unpack(raw_header)
            expected = (
                expected_sequence
                if first_sequence is None and expected_sequence is not None
                else sequence if first_sequence is None else last_sequence + 1
            )
            if magic != _MAGIC:
                raise JournalCorruptionError(f"Invalid journal magic at {path}:{start}")
            if sequence <= 0 or sequence != expected:
                raise JournalCorruptionError(
                    f"Journal sequence gap at {sequence}; expected {expected}",
                )
            if payload_size > _MAX_PAYLOAD_BYTES:
                raise JournalCorruptionError("Journal frame length exceeds safety limit")
            payload = stream.read(payload_size)
            if len(payload) < payload_size:
                if repair_tail:
                    os.truncate(path, start)
                    return first_sequence, last_sequence, start
                raise JournalCorruptionError("Incomplete frame in a closed journal segment")
            if zlib.crc32(payload) != expected_crc:
                raise JournalCorruptionError(f"Journal CRC mismatch at sequence {sequence}")
            try:
                decode_event(payload)
            except Exception as error:
                raise JournalCorruptionError(
                    f"Invalid event payload at sequence {sequence}",
                ) from error
            first_sequence = first_sequence or sequence
            last_sequence = sequence
            valid_size = start + _HEADER.size + payload_size
    return first_sequence, last_sequence, valid_size


def _iter_file_entries(path: Path, file_size: int) -> Iterator[JournalEntry]:
    offset = 0
    with path.open("rb") as stream:
        while offset < file_size:
            raw_header = stream.read(_HEADER.size)
            if len(raw_header) != _HEADER.size:
                raise JournalCorruptionError("Journal changed during a stable read")
            magic, sequence, recorded_at_ns, payload_size, expected_crc = _HEADER.unpack(
                raw_header,
            )
            payload = stream.read(payload_size)
            if magic != _MAGIC or len(payload) != payload_size:
                raise JournalCorruptionError("Invalid journal frame during read")
            if zlib.crc32(payload) != expected_crc:
                raise JournalCorruptionError(f"Journal CRC mismatch at sequence {sequence}")
            yield JournalEntry(
                sequence=sequence,
                recorded_at=Timestamp(
                    datetime.fromtimestamp(
                        recorded_at_ns / 1_000_000_000,
                        timezone.utc,
                    ),
                ),
                event=decode_event(payload),
                payload=payload,
            )
            offset += _HEADER.size + payload_size


def _next_segment_index(paths: list[Path]) -> int:
    indexes = [
        int(match.group(1))
        for path in paths
        if (match := _SEGMENT_NAME.fullmatch(path.name)) is not None
    ]
    return max(indexes, default=0) + 1


def _fsync_directory(path: Path) -> None:
    if os.name == "nt" or not hasattr(os, "O_DIRECTORY"):
        return
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
