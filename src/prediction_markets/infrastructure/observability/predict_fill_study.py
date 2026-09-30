"""Capture bounded Predict fill-study evidence without controlling trading.

Notes
-----
- Disabled unless ``PREDICT_FILL_STUDY=1``. No venue requests are made here.
- Producers enqueue bounded, shallow samples; one daemon writes JSON off-loop.
- Thirty-two exclusively reserved slots cap retained event files at 1 GiB.
- Full storage reuses the oldest completely closed run; active or uncertain
  runs remain protected. Retention runs only during startup or on the writer.
- Diagnostics never replace the financial journal or authorize recovery.
"""

from __future__ import annotations

import json
import hashlib
import logging
import os
import queue
import stat
import threading
import time
import uuid
from collections import Counter, deque
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from dataclasses import replace
from functools import wraps
from itertools import islice
from pathlib import Path
from typing import Any

from prediction_markets.application import events
from prediction_markets.infrastructure.clock_health import ClockMonitor

SCHEMA = 1
SLOTS = 32
FILE_LIMIT = 32 * 1024 * 1024
LEVEL_LIMIT = 20
HISTORY_CAPACITY = 2048
BOUNDARY_CAPACITY = 128
MAX_ACTIVE_WINDOWS = 8
PRE_WINDOW_NS = 2_000_000_000
TRIGGER_GRACE_NS = 1_000_000_000
POST_WINDOW_NS = 15_000_000_000
OWNED_FILES = ("manifest.json", "events.jsonl", "summary.json")
STORAGE_LOCK_FILE = ".retention.lock"
METADATA_LIMIT = 64 * 1024
_log = logging.getLogger(__name__)
_recorder: FillStudyRecorder | None = None
_startup_status = "disabled"


def study_root() -> Path:
    """Return the dedicated diagnostics directory beside the configured journal."""
    return Path(os.getenv("JOURNAL_PATH", "data/trading.log")).parent / "predict-fill-study"


@contextmanager
def study_storage_lock(root: Path) -> Iterator[None]:
    """Serialize slot reservation, retention, and explicit cleanup across processes.

    Parameters
    ----------
    root
        Existing dedicated capture directory. Its persistent lock is never
        removed, so concurrent processes always lock the same file.

    Raises
    ------
    OSError
        If the lock is unsafe, inaccessible, or unavailable for five seconds.

    Notes
    -----
    - Call only during startup or offline/writer work, never from producers.
    """
    path = root.resolve() / STORAGE_LOCK_FILE
    if path.is_symlink() or path.is_junction() or path.exists() and not path.is_file():
        raise OSError("Unsafe fill-study storage lock")
    descriptor = os.open(path, os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0), 0o600)
    locked = False
    try:
        info = os.fstat(descriptor)
        if (not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_size > 1
                or path.is_symlink() or path.stat().st_ino != info.st_ino):
            raise OSError("Unsafe fill-study storage lock")
        if os.name == "nt":
            import msvcrt
        else:
            import fcntl
        deadline = time.monotonic() + 5
        while True:
            try:
                if os.name == "nt":
                    msvcrt.locking(descriptor, msvcrt.LK_NBLCK, 1)
                else:
                    fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
                locked = True
                break
            except OSError:
                if time.monotonic() >= deadline:
                    raise OSError("Fill-study storage lock is busy") from None
                time.sleep(0.01)
        yield
    finally:
        if locked:
            if os.name == "nt":
                msvcrt.locking(descriptor, msvcrt.LK_UNLCK, 1)
            else:
                fcntl.flock(descriptor, fcntl.LOCK_UN)
        os.close(descriptor)


def _owned_metadata(path: Path) -> dict[str, Any]:
    """Read bounded regular metadata without following links or sharing hard links."""
    info = path.lstat()
    if (path.is_symlink() or not stat.S_ISREG(info.st_mode)
            or info.st_nlink != 1 or info.st_size > METADATA_LIMIT):
        raise ValueError("Unsafe diagnostic metadata")
    with path.open("rb") as stream:
        payload = stream.read(METADATA_LIMIT + 1)
    if len(payload) > METADATA_LIMIT:
        raise ValueError("Oversized diagnostic metadata")
    value = json.loads(payload)
    if not isinstance(value, dict):
        raise ValueError("Invalid diagnostic metadata")
    return value


def _closed_runs(root: Path, current_run_id: str) -> list[tuple[int, str, tuple[Path, ...], int]]:
    """Find complete closed runs using at most 32 slots and bounded metadata reads.

    Notes
    -----
    - Call with the storage lock held. Unknown files, unsafe paths, failed runs,
      unfinished summaries, and any broken continuation protect the whole run.
    - Event contents are never loaded; their exact recorded byte counts must
      agree with the closed summaries and the original segment cap.
    """
    groups: dict[str, list[tuple[Path, dict[str, Any], dict[str, Any]]]] = {}
    invalid: set[str] = {current_run_id}
    for index in range(SLOTS):
        slot = root / f"slot-{index:02d}"
        run_id = None
        try:
            if slot.is_symlink() or slot.is_junction() or slot.resolve().parent != root:
                continue
            manifest = _owned_metadata(slot / "manifest.json")
            run_id = manifest.get("run_id")
            if not isinstance(run_id, str) or not 0 < len(run_id) <= 128:
                continue
            children = list(islice(slot.iterdir(), len(OWNED_FILES) + 1))
            if (manifest.get("schema") != SCHEMA or manifest.get("owner") != "predict-fill-study"
                    or manifest.get("files") != list(OWNED_FILES)
                    or "previous_slot" not in manifest
                    or type(manifest.get("pid")) is not int or manifest["pid"] <= 0
                    or not isinstance(manifest.get("producer"), str) or not manifest["producer"]
                    or {child.name for child in children} != set(OWNED_FILES)):
                raise ValueError("Unrecognized diagnostic slot")
            for child in children:
                info = child.lstat()
                if (child.is_symlink() or not stat.S_ISREG(info.st_mode)
                        or info.st_nlink != 1):
                    raise ValueError("Unsafe diagnostic file")
            summary = _owned_metadata(slot / "summary.json")
            size, cap = (slot / "events.jsonl").stat().st_size, manifest.get("event_byte_limit")
            if (type(cap) is not int or not 0 < cap <= FILE_LIMIT or size > cap
                    or type(summary.get("bytes")) is not int or summary["bytes"] != size
                    or summary.get("schema") != SCHEMA or summary.get("run_id") != run_id
                    or type(manifest.get("started_wall_ns")) is not int or manifest["started_wall_ns"] <= 0
                    or type(summary.get("ended_wall_ns")) is not int or summary["ended_wall_ns"] <= 0
                    or type(manifest.get("segment_index")) is not int
                    or type(summary.get("segment_index")) is not int or "next_slot" not in summary
                    or type(summary.get("total_bytes")) is not int
                    or type(summary.get("pending_at_stop")) is not int or summary["pending_at_stop"] < 0
                    or not isinstance(summary.get("counts"), dict)
                    or not isinstance(summary.get("active_windows"), list)):
                raise ValueError("Uncertain diagnostic summary")
            groups.setdefault(run_id, []).append((slot, manifest, summary))
        except (OSError, ValueError, RecursionError):
            if isinstance(run_id, str):
                invalid.add(run_id)
    result = []
    for run_id, segments in groups.items():
        if run_id in invalid:
            continue
        segments.sort(key=lambda item: item[1]["segment_index"])
        first = segments[0][1]
        total_bytes = 0
        for index, (slot, manifest, summary) in enumerate(segments):
            last = index + 1 == len(segments)
            total_bytes += summary["bytes"]
            if (manifest["segment_index"] != index or summary["segment_index"] != index
                    or summary["total_bytes"] != total_bytes
                    or manifest.get("previous_slot") != (segments[index - 1][0].name if index else None)
                    or summary.get("next_slot") != (None if last else segments[index + 1][0].name)
                    or summary.get("status") != ("closed" if last else "rotated")
                    or any(manifest.get(key) != first.get(key) for key in
                        ("started_wall_ns", "pid", "producer", "event_byte_limit"))
                    or last and (summary.get("pending_at_stop") != 0 or summary.get("active_windows") != [])):
                break
        else:
            result.append((first["started_wall_ns"], run_id, tuple(item[0] for item in segments),
                sum(item[2]["bytes"] for item in segments)))
    return result


class FillStudyRecorder:
    """Own bounded recording segments and a non-blocking producer queue.

    Parameters
    ----------
    root
        Dedicated study directory, never the journal directory itself.
    producer
        Parent or worker partition label.
    capacity
        Maximum pending samples. Overflow increments an observable loss counter.
    byte_limit
        Per-segment event cap, at most 32 MiB. Full storage releases only the
        oldest completely closed run before reserving a slot.
    capture_mode
        ``trades`` opens windows only for explicit execution observations. The
        ``signals`` default retains the standalone recorder's historical API.

    Notes
    -----
    - The ring retains at most 2048 samples, including one second of trigger
      grace before the two-second analysis window in trade mode. Up to 128
      predecessor books preserve idle baselines with their original timestamps.
      Eight concurrent windows record only selected contracts, for fifteen
      seconds after selection.
    - Repeated triggers for an active execution do not extend its deadline.
      Truncation and loss cannot prove insufficient liquidity.
    """

    def __init__(self, root: Path, producer: str, *, capacity: int = 512,
                 byte_limit: int = FILE_LIMIT, capture_mode: str = "signals") -> None:
        if capacity <= 0 or not 0 < byte_limit <= FILE_LIMIT:
            raise ValueError("Invalid diagnostic capacity or byte limit")
        if capture_mode not in {"trades", "signals"}:
            raise ValueError("Unknown diagnostic capture mode")
        self.root = root.resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        self.run_id = uuid.uuid4().hex
        self._counts: Counter[str] = Counter()
        self.path = self._reserve_slot()
        self.producer = producer
        self._queue: queue.Queue[dict[str, Any]] = queue.Queue(capacity)
        self._stop = threading.Event()
        self._disabled = threading.Event()
        self._lock = threading.Lock()
        self._sequence = 0
        self._byte_limit = byte_limit
        self._bytes = 0
        self._total_bytes = 0
        self._segment_index = 0
        self._status = "running"
        self._capture_mode = capture_mode
        self._clock = ClockMonitor()
        self._history_retention_ns = PRE_WINDOW_NS + (TRIGGER_GRACE_NS if capture_mode == "trades" else 0)
        self._started_mono_ns = time.monotonic_ns()
        self._last_persisted_mono_ns = 0
        self._last_persisted_seq = 0
        self._last_progress_mono_ns = self._started_mono_ns
        self._windows: dict[str, dict[str, Any]] = {}
        self._stream: Any = None
        self._manifest = {
            "schema": SCHEMA, "owner": "predict-fill-study", "run_id": self.run_id,
            "producer": producer, "pid": os.getpid(), "started_wall_ns": time.time_ns(),
            "files": list(OWNED_FILES), "event_byte_limit": byte_limit,
            "queue_capacity": capacity, "level_limit": LEVEL_LIMIT,
            "layer": "normalized_pipeline_books", "window_seconds": 2,
            "capture_mode": capture_mode, "capture_started_mono_ns": self._started_mono_ns,
            "pre_window_seconds": 2, "post_window_seconds": 15,
            "history_retention_seconds": self._history_retention_ns / 1_000_000_000,
            "trigger_grace_seconds": (self._history_retention_ns - PRE_WINDOW_NS) / 1_000_000_000,
            "boundary_capacity": BOUNDARY_CAPACITY,
            "history_capacity": HISTORY_CAPACITY, "max_active_windows": MAX_ACTIVE_WINDOWS,
            "segment_index": 0, "previous_slot": None,
            "retention_policy": "oldest_closed_run",
            "code_sha256": {
                name: hashlib.sha256((Path(__file__) if name == "predict_fill_study.py"
                    else Path(__file__).parents[1] / name).read_bytes()).hexdigest()
                for name in ("predict_fill_study.py", "venues/predict/execution.py",
                    "venues/predict/order_updates.py", "../application/pipeline/order_dispatch.py")
            },
            "configuration": {name: os.getenv(name, default) for name, default in (
                ("PREDICT_LIMIT_FOK_ENABLED", "0"), ("PREDICT_RESTING_WINDOW_MS", "1000"),
                ("MARKET_DATA_ENFORCE_SOURCE_AGE", "true"))},
            "clock_basis": "same_host_monotonic_intervals_uncalibrated_wall_source_age",
        }
        self._write_metadata("manifest.json", self._manifest)
        self._thread = threading.Thread(target=self._run, name="predict-fill-study", daemon=True)
        self._thread.start()

    def _reserve_slot(self) -> Path:
        """Reserve a slot, releasing one complete old run only when storage is full."""
        with study_storage_lock(self.root):
            for index in range(SLOTS):
                path = self.root / f"slot-{index:02d}"
                try:
                    path.mkdir(mode=0o700)
                except FileExistsError:
                    continue
                return path
            candidates = _closed_runs(self.root, self.run_id)
            if not candidates:
                raise FileExistsError("Fill-study slots are full; no completely closed run can be reused")
            oldest = min(candidates)
            if oldest not in _closed_runs(self.root, self.run_id):
                raise FileExistsError("Fill-study slots are full; retained evidence changed")
            _, run_id, slots, size = oldest
            for slot in slots:
                for name in OWNED_FILES:
                    (slot / name).unlink()
                slot.rmdir()
            self._counts["retention_runs_removed"] += 1
            self._counts["retention_slots_removed"] += len(slots)
            self._counts["retention_bytes_removed"] += size
            _log.warning("Fill-study retention removed closed run %s: %d slots, %d event bytes",
                run_id, len(slots), size)
            path = min(slots)
            path.mkdir(mode=0o700)
            return path

    def _write_metadata(self, name: str, value: dict[str, Any]) -> None:
        """Create owned metadata exclusively so existing evidence is never replaced."""
        with (self.path / name).open("x", encoding="utf-8") as stream:
            json.dump(value, stream, separators=(",", ":"))

    def offer(self, kind: str, data: Any) -> None:
        """Enqueue evidence without waiting for the writer or exposing its errors."""
        with self._lock:
            if self._disabled.is_set() or self._stop.is_set():
                self._counts["offers_after_stop"] += 1
                return
            self._sequence += 1
            row = {"schema": SCHEMA, "run_id": self.run_id, "producer": self.producer,
                   "seq": self._sequence, "wall_ns": time.time_ns(),
                   "mono_ns": time.monotonic_ns(), "kind": kind,
                   "loss_epoch": self._counts["dropped"], "data": data}
            try:
                self._queue.put_nowait(row)
                self._counts["enqueued"] += 1
                self._counts["queue_high_watermark"] = max(
                    self._counts["queue_high_watermark"], self._queue.qsize())
            except queue.Full:
                self._counts["dropped"] += 1
                self._counts["queue_full"] += 1

    def status(self) -> dict[str, Any]:
        """Return a bounded in-memory snapshot without filesystem work.

        Returns
        -------
        dict
            Recorder state, queue pressure, cumulative loss and writer progress.
            Bytes count all segments in this process's run, not historical runs.
        """
        with self._lock:
            return {"status": self._status, "run_id": self.run_id,
                "producer": self.producer, "capture_mode": self._capture_mode,
                "slot": self.path.name, "segment_index": self._segment_index,
                "bytes": self._total_bytes, "segment_bytes": self._bytes,
                "segment_byte_limit": self._byte_limit, "slot_limit": SLOTS,
                "queue_depth": self._queue.qsize(), "queue_capacity": self._queue.maxsize,
                "active_windows": len(self._windows), "counts": dict(self._counts),
                "writer_alive": self._thread.is_alive(),
                "writer_progress_age_seconds": max(0, time.monotonic_ns() -
                    self._last_progress_mono_ns) / 1_000_000_000}

    def close(self) -> None:
        """Request a bounded drain; a blocked filesystem cannot block shutdown forever."""
        self._stop.set()
        self._thread.join(timeout=2)
        if self._thread.is_alive():
            _log.warning("Fill-study writer still draining: %s", self.path)

    def record_error(self, reason: str) -> None:
        """Make discarded diagnostic observations explicit without raising."""
        with self._lock:
            self._counts["dropped"] += 1
            self._counts[reason] += 1

    def _run(self) -> None:
        """Filter a fixed ring into contract windows and rotate within bounded storage."""
        history: deque[dict[str, Any]] = deque()
        boundary_books: dict[str, dict[str, Any]] = {}
        written: set[int] = set()
        last_evicted_mono_ns = 0
        heartbeat_at = 0
        try:
            self._stream = (self.path / "events.jsonl").open("xb")
            self._segment_marker("segment_start", {"previous_slot": None})
            while not self._stop.is_set() or not self._queue.empty():
                try:
                    item = self._queue.get(timeout=0.05)
                except queue.Empty:
                    item = None
                now = time.monotonic_ns()
                through = item["mono_ns"] if item is not None else now
                self._expire_windows(through)
                retains_history = item is not None and (item["kind"] in {"book", "predict_payload"}
                    or item["kind"] == "signal" and self._capture_mode == "trades")
                while history and (retains_history and len(history) >= HISTORY_CAPACITY or
                        history[0]["mono_ns"] < through - self._history_retention_ns):
                    old = history.popleft()
                    if old["mono_ns"] < through - self._history_retention_ns and old["kind"] == "book":
                        previous = boundary_books.pop(old["data"]["contract"], None)
                        if previous is not None:
                            written.discard(previous["seq"])
                        boundary_books[old["data"]["contract"]] = old
                        if len(boundary_books) > BOUNDARY_CAPACITY:
                            retired = boundary_books.pop(next(iter(boundary_books)))
                            written.discard(retired["seq"])
                            self._counts["boundary_capacity_evictions"] += 1
                        self._counts["boundary_high_watermark"] = max(
                            self._counts["boundary_high_watermark"], len(boundary_books))
                    else:
                        written.discard(old["seq"])
                    if old["mono_ns"] >= through - self._history_retention_ns:
                        last_evicted_mono_ns = old["mono_ns"]
                        self._counts["history_capacity_evictions"] += 1
                if item is not None:
                    try:
                        row = {**item, "data": _serialize(item["kind"], item["data"])}
                        kind, data = row["kind"], row["data"]
                        if kind in {"book", "predict_payload"} or (
                                kind == "signal" and self._capture_mode == "trades"):
                            history.append(row)
                            if any(_matches_window(row, window) for window in self._windows.values()):
                                if self._write(self._stream, row):
                                    written.add(row["seq"])
                        else:
                            trigger = data if kind == "execution_window" else None
                            duration = POST_WINDOW_NS
                            if kind == "signal" and self._capture_mode == "signals":
                                trigger = {"execution_id": f"signal:{data['signal_id']}",
                                    "contracts": data["pair"], "signal_id": data["signal_id"],
                                    "intent_id": data.get("intent_id")}
                                duration = PRE_WINDOW_NS
                            if trigger is not None:
                                window = self._open_window(row, trigger, duration,
                                    last_evicted_mono_ns, boundary_books)
                                if window is not None:
                                    for contract, provenance in window["prehistory_boundary_books"].items():
                                        old = boundary_books.get(contract)
                                        if old is not None and old["seq"] == provenance["seq"] and old["seq"] not in written:
                                            if self._write(self._stream, old):
                                                written.add(old["seq"])
                                    for old in history:
                                        if old["seq"] not in written and _matches_window(old, window):
                                            if self._write(self._stream, old):
                                                written.add(old["seq"])
                            if kind != "execution_window":
                                self._write(self._stream, row)
                    except (ValueError, TypeError, AttributeError, KeyError):
                        self.record_error("serialization_errors")
                    finally:
                        self._queue.task_done()
                self._last_progress_mono_ns = now
                if now - heartbeat_at >= 100_000_000:
                    if self._windows:
                        self._heartbeat(now)
                    self._stream.flush()
                    heartbeat_at = now
                if self._disabled.is_set():
                    break
            if self._status == "running":
                self._heartbeat(time.monotonic_ns())
                self._expire_windows(time.monotonic_ns(), "recorder_closed")
                if self._status == "running":
                    self._status = "closed"
        except OSError:
            self._status = "io_error"
            self._disabled.set()
            self.record_error("writer_io_errors")
            _log.exception("Fill-study capture disabled; trading is unaffected")
        finally:
            self._disabled.set()
            try:
                if self._stream is not None:
                    self._stream.close()
                self._finish_segment(self._status)
            except OSError:
                _log.warning("Cannot write fill-study summary: %s", self.path)

    def _open_window(self, row: dict[str, Any], trigger: dict[str, Any], duration: int,
                     evicted_mono_ns: int,
                     boundary_books: dict[str, dict[str, Any]]) -> dict[str, Any] | None:
        """Open bounded coverage without backdating books or bridging a lost interval.

        Notes
        -----
        - Trade windows include a one-second trigger grace period. Longer IPC
          delays and capacity loss remain explicitly incomplete in analysis.
        - A predecessor retains its original identity and loss epoch. It may
          seed an idle interval only when no capacity or queue loss intervened.
        """
        execution_id = trigger["execution_id"]
        if execution_id in self._windows:
            if trigger.get("reason") == "recovery":
                window = {**self._windows[execution_id], "reason": "recovery",
                    "window_end_mono_ns": row["mono_ns"] + duration}
                self._windows[execution_id] = window
                self._write(self._stream, {**row, "kind": "execution_window", "data": window})
                self._counts["recovery_window_extensions"] += 1
                return window
            self._counts["repeated_window_triggers"] += 1
            return None
        if len(self._windows) >= MAX_ACTIVE_WINDOWS:
            self.record_error("window_capacity")
            self._write(self._stream, self._row("execution_window_rejected", {
                **trigger, "reason": "window_capacity"}))
            return None
        requested_pre = row["mono_ns"] - self._history_retention_ns
        pre_start = max(requested_pre, self._started_mono_ns, evicted_mono_ns + 1)
        predecessors = {contract: {key: book[key] for key in ("run_id", "seq", "mono_ns", "loss_epoch")}
            for contract in trigger["contracts"] if (book := boundary_books.get(contract)) is not None
            and evicted_mono_ns < book["mono_ns"] <= pre_start
            and book["loss_epoch"] == row["loss_epoch"]}
        window = {**trigger, "window_start_mono_ns": row["mono_ns"],
            "window_end_mono_ns": row["mono_ns"] + duration,
            "pre_start_mono_ns": pre_start, "capture_started_mono_ns": self._started_mono_ns,
            "prehistory_truncated": pre_start > requested_pre,
            "prehistory_boundary_books": predecessors,
            "prehistory_loss_epoch": row["loss_epoch"]}
        self._windows[execution_id] = window
        self._counts["windows_opened"] += 1
        self._write(self._stream, {**row, "kind": "execution_window", "data": window})
        return window

    def _expire_windows(self, through: int, reason: str = "elapsed") -> None:
        """Persist the actual covered end before retiring each bounded window."""
        for execution_id, window in tuple(self._windows.items()):
            if reason == "elapsed" and through <= window["window_end_mono_ns"]:
                continue
            if not self._write(self._stream, self._row("execution_window_end", {**window,
                    "reason": reason, "end_mono_ns": min(through, window["window_end_mono_ns"])})):
                break
            del self._windows[execution_id]

    def _row(self, kind: str, data: dict[str, Any]) -> dict[str, Any]:
        return {"schema": SCHEMA, "run_id": self.run_id, "producer": self.producer,
            "kind": kind, "wall_ns": time.time_ns(), "mono_ns": time.monotonic_ns(),
            "loss_epoch": self._counts["dropped"], "data": data}

    def _heartbeat(self, now: int) -> None:
        """Identify selected windows and how far the producer queue has drained."""
        if self._windows:
            depth = self._queue.qsize()
            self._write(self._stream, self._row("heartbeat", {"queue_depth": depth,
                "observed_through_mono_ns": now if depth == 0 else self._last_persisted_mono_ns,
                "clock": self._clock.sample(),
                "windows": list(self._windows.values())}))

    def _finish_segment(self, status: str, next_slot: str | None = None) -> None:
        """Publish a summary after closing its stream, under the storage lock."""
        if self._stream is not None and not self._stream.closed:
            raise OSError("Cannot publish a summary for an open diagnostic stream")
        with study_storage_lock(self.root):
            self._write_metadata("summary.json", {
                "schema": SCHEMA, "run_id": self.run_id, "status": status,
                "segment_index": self._segment_index, "next_slot": next_slot,
                "bytes": self._bytes, "total_bytes": self._total_bytes,
                "counts": dict(self._counts), "pending_at_stop": self._queue.qsize(),
                "last_persisted_mono_ns": self._last_persisted_mono_ns,
                "last_persisted_seq": self._last_persisted_seq,
                "active_windows": list(self._windows.values()), "ended_wall_ns": time.time_ns(),
            })

    def _segment_marker(self, kind: str, data: dict[str, Any], reserve_bytes: int = 0) -> None:
        """Write optional links when space allows; manifests and summaries always link."""
        row = {**self._row(kind, data), "segment_index": self._segment_index}
        encoded = (json.dumps(row, separators=(",", ":")) + "\n").encode()
        if self._bytes + len(encoded) + reserve_bytes <= self._byte_limit:
            self._stream.write(encoded)
            self._bytes += len(encoded)
            self._total_bytes += len(encoded)

    def _write(self, stream: Any, row: dict[str, Any]) -> bool:
        """Append or reserve a segment while protecting every segment of this run."""
        if self._disabled.is_set():
            return False
        row = {**row, "segment_index": self._segment_index}
        data = (json.dumps(row, separators=(",", ":"), allow_nan=False) + "\n").encode()
        if len(data) > self._byte_limit:
            self.record_error("oversized_rows")
            self._status = "disk_limit"
            self._disabled.set()
            _log.warning("Fill-study row exceeds segment cap; capture stopped: %s", self.path)
            return False
        if self._bytes + len(data) > self._byte_limit:
            try:
                next_path = self._reserve_slot()
            except FileExistsError as error:
                self.record_error("slot_exhausted")
                self._status = "disk_limit"
                self._disabled.set()
                _log.warning("Fill-study capture exhausted; evidence preserved: %s", error)
                return False
            self._segment_marker("segment_end", {"next_slot": next_path.name})
            self._stream.close()
            self._finish_segment("rotated", next_path.name)
            previous_slot = self.path.name
            self.path = next_path
            self._segment_index += 1
            self._bytes = 0
            self._counts["rotations"] += 1
            self._manifest = {**self._manifest, "segment_index": self._segment_index,
                "previous_slot": previous_slot}
            self._write_metadata("manifest.json", self._manifest)
            self._stream = (self.path / "events.jsonl").open("xb")
            row["segment_index"] = self._segment_index
            data = (json.dumps(row, separators=(",", ":"), allow_nan=False) + "\n").encode()
            if len(data) > self._byte_limit:
                self.record_error("oversized_rows")
                return False
            # ponytail: metadata links cover segments too small for a start marker.
            self._segment_marker("segment_start", {"previous_slot": previous_slot}, len(data))
        self._stream.write(data)
        self._bytes += len(data)
        self._total_bytes += len(data)
        self._last_persisted_mono_ns = max(self._last_persisted_mono_ns, row["mono_ns"])
        self._last_persisted_seq = max(self._last_persisted_seq, row.get("seq", 0))
        self._counts["written"] += 1
        return True


def start_study(producer: str) -> None:
    """Enable a process-local recorder at runtime startup, failing open for trading."""
    global _recorder, _startup_status
    if os.getenv("PREDICT_FILL_STUDY", "0") != "1" or _recorder is not None:
        return
    try:
        _recorder = FillStudyRecorder(study_root(), producer, capture_mode="trades")
        _startup_status = "running"
    except (OSError, ValueError):
        _startup_status = "start_failed"
        _log.exception("Cannot start optional Predict fill study")


def stop_study() -> None:
    """Close only this process's disposable recorder."""
    global _recorder, _startup_status
    recorder, _recorder = _recorder, None
    if recorder is not None:
        recorder.close()
        _startup_status = recorder.status()["status"]


def capture_status() -> dict[str, Any]:
    """Expose process-local diagnostic state without disk reads or venue requests."""
    recorder = _recorder
    return recorder.status() if recorder is not None else {
        "status": _startup_status, "counts": {}, "bytes": 0,
        "queue_depth": 0, "queue_capacity": 0, "active_windows": 0,
        "writer_alive": False, "writer_progress_age_seconds": 0,
    }


def _diagnostic(callback):
    """Isolate optional producer failures from adapters and execution control."""
    @wraps(callback)
    def observe(*args, **kwargs):
        recorder = _recorder
        if recorder is None:
            return
        try:
            callback(*args, **kwargs)
        except Exception:
            recorder.record_error("observer_errors")
    return observe


@_diagnostic
def observe_execution_window(execution_id: str, contracts: Iterable[str], *,
                             signal_id: str | None = None, intent_id: str | None = None,
                             reason: str = "execution") -> None:
    """Request a bounded pair capture when an execution is selected or recovered.

    Parameters
    ----------
    execution_id
        Stable identity shared by parent and worker diagnostic streams.
    contracts
        One to four selected contract identifiers, normally the execution pair.
    signal_id, intent_id
        Optional joins to detector and worker-validation evidence.
    reason
        ``execution`` opens a fixed fifteen-second window. ``recovery`` renews
        that deadline for an actual recovery action, never for detector repeats.

    Notes
    -----
    - The observer only offers bounded metadata. Queue overflow is visible in
      capture status and never delays or changes execution decisions.
    """
    selected = tuple(dict.fromkeys(str(value)[:256] for value in islice(contracts, 5)))
    if not execution_id or not 1 <= len(selected) <= 4 or reason not in {"execution", "recovery"}:
        raise ValueError("Invalid diagnostic execution window")
    assert _recorder is not None
    _recorder.offer("execution_window", {"execution_id": str(execution_id)[:256],
        "contracts": selected, "signal_id": _scalar(signal_id),
        "intent_id": _scalar(intent_id), "reason": reason})


@_diagnostic
def observe_event(event: Any) -> None:
    """Capture selected events without altering their financial state or identity."""
    recorder = _recorder
    if recorder is None:
        return
    if isinstance(event, events.OrderBookUpdated):
        book = event.order_book
        recorder.offer("book", (str(event.venue_id), str(event.contract_id),
            replace(book, bids=book.bids[:LEVEL_LIMIT], asks=book.asks[:LEVEL_LIMIT]),
            len(book.bids) > LEVEL_LIMIT, len(book.asks) > LEVEL_LIMIT))
    elif isinstance(event, events.ArbitrageOpportunityFound):
        if "PREDICT" in {str(event.pair.left.venue_id), str(event.pair.right.venue_id)}:
            recorder.offer("signal", event)
    elif isinstance(event, (events.ExecutionPreparationRequested, events.PreparedExecutionBatch)):
        if event.commands and "PREDICT" in {
                str(event.opportunity.pair.left.venue_id), str(event.opportunity.pair.right.venue_id)}:
            observe_execution_window(event.commands[0].execution_id,
                (str(event.opportunity.pair.left.id), str(event.opportunity.pair.right.id)),
                signal_id=event.opportunity.id,
                intent_id=event.opportunity.validation_ref.intent_id
                if event.opportunity.validation_ref else None)
        observe_event(event.opportunity)
        execution = event.planned.execution
        for index, role in ((1, "primary"), (2, "hedge")):
            snapshot = getattr(execution, f"leg{index}_decision", None)
            if snapshot is not None:
                recorder.offer("decision", ({"execution_id": execution.id,
                    "signal_id": event.opportunity.id,
                    "intent_id": event.opportunity.validation_ref.intent_id
                    if event.opportunity.validation_ref else None,
                    "client_id": str(getattr(execution, f"leg{index}_client_order_id")),
                    "role": role, "phase": "prepared_batch"
                    if isinstance(event, events.PreparedExecutionBatch) else "preparation_requested"},
                    replace(snapshot, levels=snapshot.levels[:LEVEL_LIMIT]),
                    len(snapshot.levels) > LEVEL_LIMIT))
        if isinstance(event, events.PreparedExecutionBatch):
            for prepared in event.prepared:
                observe_event(prepared)
    elif isinstance(event, events.OrderPrepared):
        recorder.offer("prepared", event)
    elif isinstance(event, events.OrderCancellationPrepared):
        cancellation = json.loads(event.request)
        recorder.offer("chain_cancel_prepared", {
            "execution_id": event.command.execution_id,
            "client_id": str(event.command.intent.client_order_id),
            **{key: _scalar(cancellation.get(key)) for key in (
                "transaction_hash", "nonce", "max_fee_wei")},
        })
    elif isinstance(event, events.SubmissionReceived):
        recorder.offer("submission", event)
    elif isinstance(event, events.OrderSnapshotUpdated):
        recorder.offer("snapshot", event)


@_diagnostic
def observe_book_checkpoint(command: Any, book: Any, *, phase: str,
                            monotonic_at_ns: int | None = None,
                            wall_at_ns: int | None = None,
                            validation: Any = None, book_generation: str | None = None,
                            book_origin: str = "in_memory") -> None:
    """Retain the exact bounded book used at a dispatch checkpoint without I/O.

    Parameters
    ----------
    phase
        Preparation validation, final validation, guard, recovery preparation,
        or submission checkpoint; these observations never replace each other.
    monotonic_at_ns, wall_at_ns
        Checkpoint clocks in nanoseconds. Missing values are sampled locally.
    validation
        Optional worker response carrying request identity and outcome.
    book_origin
        Distinguish in-memory books from an existing REST snapshot request.

    Notes
    -----
    - Immutable levels are clipped before enqueue. Serialization and age
      calculations run in the existing recorder thread, never the hot path.
    - Missing venue or transport timestamps remain explicitly unknown.
    """
    if _recorder is None:
        return
    identity = {"execution_id": command.execution_id,
        "client_id": str(command.intent.client_order_id), "role": command.role,
        "venue": str(command.venue_id), "contract": str(command.intent.contract_id),
        "side": command.intent.side.value, "quantity": _number(command.intent.quantity),
        "limit": _number(command.intent.limit_price), "phase": phase,
        "captured_mono_ns": monotonic_at_ns if monotonic_at_ns is not None else time.monotonic_ns(),
        "captured_wall_ns": wall_at_ns if wall_at_ns is not None else time.time_ns(),
        "book_origin": book_origin, "book_generation": book_generation}
    if validation is not None:
        identity.update(request_id=validation.request.request_id,
            intent_id=validation.request.validation_ref.intent_id,
            validation_reason=validation.reason.value,
            worker_responded_wall_ns=validation.responded_wall_at_ns)
    clipped = None if book is None else (
        str(command.venue_id), str(command.intent.contract_id),
        replace(book, bids=book.bids[:LEVEL_LIMIT], asks=book.asks[:LEVEL_LIMIT]),
        len(book.bids) > LEVEL_LIMIT, len(book.asks) > LEVEL_LIMIT)
    _recorder.offer("book_checkpoint", (identity, clipped))


@_diagnostic
def observe_native(payload: dict[str, Any]) -> None:
    """Preserve whitelisted private event fields, including pending and failed matches."""
    if _recorder is not None:
        data = {k: _scalar(payload.get(k)) for k in (
            "type", "orderHash", "timestamp", "reason", "settlementId")}
        fill = payload.get("fill")
        data["fill"] = {k: _scalar(fill.get(k)) for k in (
            "executedSizeWei", "executedPriceWei", "executedValueWei")} if isinstance(fill, dict) else None
        _recorder.offer("private", data)


@_diagnostic
def observe_predict_payload(payload: dict[str, Any], arrival_wall_ns: int) -> None:
    """Capture bounded pending-depth metadata without changing executable book sizes."""
    if _recorder is None:
        return
    pending = payload.get("settlementsPending")
    _recorder.offer("predict_payload", {
        "market_id": _scalar(payload.get("marketId")), "source_ms": _scalar(payload.get("updateTimestampMs")),
        "arrival_wall_ns": arrival_wall_ns,
        "pending": {k: _levels(pending.get(k)) for k in ("asks", "bids")}
        if isinstance(pending, dict) else None,
    })


@_diagnostic
def observe_cancel(reference: Any, reason: str) -> None:
    """Record the application's cancellation decision, not a claim of zero fills."""
    if _recorder is not None:
        _recorder.offer("cancel_requested", {"client_id": str(reference.client_order_id),
            "venue": str(reference.venue_id), "reason": reason})


@_diagnostic
def observe_submission(reference: Any) -> None:
    """Timestamp entry into Predict submission without inspecting the signed request."""
    if _recorder is not None:
        _recorder.offer("submit_started", {"client_id": str(reference.client_order_id),
            "venue": str(reference.venue_id)})


def _number(value: Any) -> str | None:
    return str(value.value) if value is not None else None


def _scalar(value: Any) -> Any:
    """Bound externally supplied scalar text and exclude nested private data."""
    if isinstance(value, str):
        return value[:256]
    return value if value is None or isinstance(value, (int, float, bool)) else None


def _levels(value: Any) -> list[list[Any]] | None:
    """Copy only bounded price/value pairs; malformed pending depth stays unknown."""
    if not isinstance(value, (list, tuple)):
        return None
    return [[_scalar(x[0]), _scalar(x[1])] for x in value[:LEVEL_LIMIT]
        if isinstance(x, (list, tuple)) and len(x) == 2]


def _matches_window(row: dict[str, Any], window: dict[str, Any]) -> bool:
    """Match only selected contracts or their Predict pending-depth market."""
    if not window["pre_start_mono_ns"] <= row["mono_ns"] <= window["window_end_mono_ns"]:
        return False
    data, contracts = row["data"], window["contracts"]
    if row["kind"] == "book":
        return data["contract"] in contracts
    if row["kind"] == "signal":
        return set(data["pair"]).issubset(contracts)
    if row["kind"] == "predict_payload":
        return any(contract.lower().startswith("predict:") and
            contract.split(":")[1] == str(data["market_id"]) for contract in contracts)
    return False


def _serialize(kind: str, value: Any) -> dict[str, Any]:
    """Whitelist persisted data; never serialize credentials or signed requests wholesale."""
    if kind == "book_checkpoint":
        identity, clipped = value
        book = _serialize("book", clipped) if clipped is not None else None
        source = book.get("source_at_ns") if book else None
        arrival = book.get("arrival_at_ns") if book else None
        received = book.get("received_at_ns") if book else None
        local = arrival if arrival is not None else received
        return {**identity, "book": book,
            "source_age_ms": (identity["captured_wall_ns"] - source) / 1e6 if source is not None else None,
            "local_age_ms": (identity["captured_mono_ns"] - local) / 1e6 if local is not None else None,
            "local_clock_basis": "transport_callback" if arrival is not None else
                "adapter_received" if received is not None else "unavailable",
            "source_age_unavailable_reason": None if source is not None else
                "missing_book" if book is None else "missing_source_timestamp"}
    if kind == "book":
        venue, contract, book, bids_truncated, asks_truncated = value
        return {"venue": venue, "contract": contract, "market_id": str(book.market_id),
            "bids": [[_number(x.price), _number(x.quantity)] for x in book.bids],
            "asks": [[_number(x.price), _number(x.quantity)] for x in book.asks],
            "bids_truncated": bids_truncated, "asks_truncated": asks_truncated,
            **{k: getattr(book, k) for k in ("source_at_ns", "arrival_wall_at_ns",
                "arrival_at_ns", "received_at_ns", "processed_at_ns", "source_timestamp_kind", "source_hash")}}
    if kind == "signal":
        opportunity = value.opportunity
        return {"signal_id": value.id, "pair": [str(value.pair.left.id), str(value.pair.right.id)],
            "side": opportunity.side.value, "quantity": _number(opportunity.quantity),
            "limits": [_number(opportunity.left_level.price), _number(opportunity.right_level.price)],
            "fee_per_contract": str(opportunity.fee_per_contract),
            "net_edge": str(opportunity.net_edge), "detected_wall_ns": int(
                opportunity.detected_at.value.timestamp() * 1_000_000_000),
            "intent_id": value.validation_ref.intent_id if value.validation_ref else None,
            "book_generations": [value.validation_ref.left_book_generation,
                value.validation_ref.right_book_generation] if value.validation_ref else None}
    if kind == "decision":
        identity, snapshot, truncated = value
        return {**identity, "venue": str(snapshot.venue_id), "contract": str(snapshot.contract_id),
            "side": snapshot.side.value, "quantity": _number(snapshot.requested_quantity),
            "limit": _number(snapshot.limit_price),
            "levels": [[_number(level.price), _number(level.quantity)] for level in snapshot.levels],
            "levels_truncated": truncated, "book_age_ns": snapshot.book_age_ns,
            "source_hash": _scalar(snapshot.source_hash),
            "book_timestamp_wall_ns": int(snapshot.book_timestamp.value.timestamp() * 1_000_000_000)
            if snapshot.book_timestamp is not None else None,
            "captured_wall_ns": int(snapshot.captured_at.value.timestamp() * 1_000_000_000)}
    if kind == "prepared":
        command, prepared = value.command, value.prepared
        native: dict[str, Any] = {}
        if str(command.venue_id) == "PREDICT":
            request = json.loads(prepared.request).get("data", {})
            native = {k: _scalar(request.get(k)) for k in (
                "strategy", "isFillOrKill", "isPostOnly", "pricePerShare")}
            order = request.get("order", {})
            native.update({k: _scalar(order.get(k)) for k in (
                "hash", "makerAmount", "takerAmount", "side", "tokenId", "feeRateBps")})
        return {"execution_id": command.execution_id, "role": command.role,
            "venue": str(command.venue_id), "client_id": str(prepared.reference.client_order_id),
            "contract": str(command.intent.contract_id), "side": command.intent.side.value,
            "quantity": _number(command.intent.quantity), "limit": _number(command.intent.limit_price),
            "intent_created_wall_ns": int(command.intent.created_at.value.timestamp() * 1_000_000_000)
            if command.intent.created_at is not None else None,
            "native": native}
    if kind in {"submission", "snapshot"}:
        snapshot = value.result.snapshot if kind == "submission" else value.snapshot
        command = value.command if kind == "submission" else None
        reference = value.result.reference if command else value.reference
        return {"execution_id": command.execution_id if command else value.execution_id,
            "role": command.role if command else value.role, "venue": str(reference.venue_id),
            "client_id": str(reference.client_order_id),
            "status": value.result.status.value if command else snapshot.status.value,
            "filled": _number(snapshot.filled_quantity) if snapshot else None,
            "order_id": str(snapshot.order_id) if snapshot and snapshot.order_id else None,
            "average_price": _number(snapshot.average_price) if snapshot else None,
            "source": "submission" if command else value.source,
            "may_receive_more_fills": snapshot.may_receive_more_fills if snapshot else None,
            "settlement_finalized_block": snapshot.settlement_finalized_block if snapshot else None}
    return value
