"""Analyze disposable fill-study captures offline and list their owned artifacts.

Notes
-----
- Offline replay measures displayed liquidity, not counterfactual fills.
- Fees are frozen at detection in horizon estimates, never recomputed as a
  live trading decision. No filter produced here is enabled automatically.
- ``verify`` performs explicit read-only venue queries outside the trading
  process and writes a new diagnostic slot. It never reconciles financial state
  or submits/cancels an order. ``analyze`` and ``inventory`` do not write files.
"""

from __future__ import annotations

import argparse
import json
import time
from bisect import bisect_left, bisect_right
from collections import Counter, defaultdict
from decimal import Decimal
from pathlib import Path
from statistics import median
from typing import Any

from prediction_markets.infrastructure.clock_health import CLOCK_STEP_TOLERANCE_NS
from prediction_markets.infrastructure.observability.predict_fill_study import (
    FILE_LIMIT, OWNED_FILES, SCHEMA, SLOTS, FillStudyRecorder, study_root, study_storage_lock,
)


def inventory(root: Path) -> list[dict[str, Any]]:
    """Validate ownership and enumerate only known diagnostic files.

    Raises
    ------
    ValueError
        If a slot is a link, has foreign contents, or lacks a valid manifest.
    """
    root = root.resolve()
    result = []
    for index in range(SLOTS):
        slot = root / f"slot-{index:02d}"
        if not slot.exists():
            continue
        if slot.is_symlink() or slot.is_junction() or slot.resolve().parent != root:
            raise ValueError(f"Unsafe diagnostic slot: {slot}")
        children = list(slot.iterdir())
        if any(p.is_symlink() or not p.is_file() or p.name not in OWNED_FILES for p in children):
            raise ValueError(f"Foreign or linked file in diagnostic slot: {slot}")
        manifest = json.loads((slot / "manifest.json").read_text(encoding="utf-8"))
        if (manifest.get("schema") != SCHEMA or manifest.get("owner") != "predict-fill-study"
                or manifest.get("files") != list(OWNED_FILES)):
            raise ValueError(f"Unrecognized diagnostic manifest: {slot}")
        result.append({"slot": str(slot), "manifest": manifest,
            "files": [{"path": str(p), "bytes": p.stat().st_size} for p in children]})
    return result


def read_capture(slot: Path) -> tuple[list[dict[str, Any]], bool]:
    """Read bounded evidence and retain local barriers for damaged records.

    Returns
    -------
    tuple
        Rows and a file-level incomplete flag. Earlier drained windows can remain
        complete when a later tail is damaged or capture reaches its disk cap.
    """
    path = slot / "events.jsonl"
    if not path.exists():
        return [], True
    if path.stat().st_size > FILE_LIMIT:
        raise ValueError(f"Oversized diagnostic file: {path}")
    rows = []
    incomplete = False
    with path.open("rb") as stream:
        for line in stream:
            try:
                if not line.endswith(b"\n"):
                    raise ValueError("Unfinished diagnostic record")
                row = json.loads(line)
                if (row.get("schema") != SCHEMA or not isinstance(row.get("data"), dict)
                        or not all(key in row for key in ("mono_ns", "wall_ns", "run_id", "kind"))):
                    raise ValueError("Unknown diagnostic schema")
                rows.append(row)
            except (ValueError, TypeError, AttributeError):
                incomplete = True
                if rows:
                    rows.append({**rows[-1], "kind": "capture_gap", "data": {"reason": "damaged_record"}})
    summary_path = slot / "summary.json"
    if summary_path.exists():
        try:
            summary = json.loads(summary_path.read_text(encoding="utf-8"))
            incomplete |= summary.get("status") not in {"closed", "rotated"} or summary.get("pending_at_stop", 0) > 0
        except (ValueError, TypeError, AttributeError):
            incomplete = True
    else:
        incomplete = True
    return rows, incomplete


def _depth(book: dict[str, Any], side: str, limit: Decimal,
           quantity: Decimal) -> tuple[Decimal, Decimal | None, bool]:
    """Return visible size, executable VWAP and whether truncated depth is sufficient."""
    key = "asks" if side == "buy" else "bids"
    available = Decimal(0)
    remaining = quantity
    notional = Decimal(0)
    exhausted_limit = False
    for price, size in book[key]:
        price, size = Decimal(price), Decimal(size)
        if (side == "buy" and price > limit) or (side == "sell" and price < limit):
            exhausted_limit = True
            break
        available += size
        take = min(remaining, size)
        remaining -= take
        notional += take * price
    complete = remaining <= 0 or exhausted_limit or not book[f"{key}_truncated"]
    return available, notional / quantity if remaining <= 0 else None, complete


def _index(rows: list[dict[str, Any]]) -> dict[str, Any]:
    """Index one recorder's local observation clock without backdating books."""
    ordered = sorted(rows, key=lambda row: row["mono_ns"])
    clock_rows = [row for row in ordered if row["kind"] != "capture_gap"]
    clock_changes = [(left["mono_ns"], right["mono_ns"]) for left, right in zip(clock_rows, clock_rows[1:])
        if abs((right["wall_ns"] - right["mono_ns"]) - (left["wall_ns"] - left["mono_ns"]))
        > CLOCK_STEP_TOLERANCE_NS]
    books: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in ordered:
        if row["kind"] == "book":
            books[row["data"]["contract"]].append(row)
        elif row["kind"] == "book_checkpoint" and row["data"].get("book"):
            books[row["data"]["contract"]].append({**row, "kind": "book",
                "mono_ns": row["data"]["captured_mono_ns"],
                "wall_ns": row["data"]["captured_wall_ns"], "data": row["data"]["book"]})
    for values in books.values():
        values.sort(key=lambda row: row["mono_ns"])
    heartbeats = [row for row in ordered if row["kind"] == "heartbeat"]
    drained = [row for row in heartbeats if row["data"].get("queue_depth") == 0]
    worker_executions = {row["data"].get("execution_id") or row["data"].get("signal_id")
        for row in ordered if (row["kind"] == "signal" and row["data"].get("intent_id")) or
        (row["kind"] == "book_checkpoint" and row["data"].get("request_id"))}
    return {"rows": ordered, "books": books,
        "producer": next((row.get("producer") for row in ordered if row.get("producer")), None),
        "worker_execution_ids": worker_executions,
        "times": {contract: [row["mono_ns"] for row in values] for contract, values in books.items()},
        "heartbeats": heartbeats, "heartbeat_times": [row["mono_ns"] for row in heartbeats],
        "drained": drained, "drained_times": [row["mono_ns"] for row in drained],
        "windows": [row["data"] for row in ordered if row["kind"] == "execution_window"],
        "ends": [row["data"] for row in ordered if row["kind"] == "execution_window_end"],
        "gaps": [row["mono_ns"] for row in ordered if row["kind"] == "capture_gap"],
        "clock_changes": clock_changes}


def _latest(tape: dict[str, Any], contract: str, at: int) -> dict[str, Any] | None:
    index = bisect_right(tape["times"].get(contract, []), at) - 1
    return tape["books"][contract][index] if index >= 0 else None


def _identity(row: dict[str, Any] | None) -> dict[str, Any] | None:
    return {key: row["data"].get(key) for key in (
        "source_hash", "source_at_ns", "arrival_at_ns", "source_timestamp_kind") } if row else None


def _signal_anchor(row: dict[str, Any]) -> dict[str, Any]:
    """Map recorded detection time before a delayed worker or parent signal copy."""
    detected = row["data"].get("detected_wall_ns")
    if detected is None or detected > row["wall_ns"]:
        return row
    return {**row, "mono_ns": row["mono_ns"] - (row["wall_ns"] - detected), "wall_ns": detected,
        "legacy_window_end_ns": row["mono_ns"] + 2_000_000_000}


def _coverage(tape: dict[str, Any], anchor: dict[str, Any], contracts: list[str],
              start: int, end: int) -> str | None:
    """Require a drained, loss-free bounded interval and all baseline books.

    Notes
    -----
    - A later file-level stop cannot invalidate an already drained interval.
    - Explicit execution windows prevent carrying cached books across inactive
      capture periods. Legacy signal recordings have a two-second post-window.
    """
    if end > start and any(before < end and after > start
                           for before, after in tape.get("clock_changes", ())):
        return "clock_discontinuity"
    execution_id = anchor["data"].get("execution_id") or anchor["data"].get("signal_id")
    if (tape.get("producer") == "parent" and execution_id in tape.get("worker_execution_ids", ())
            and end > start):
        return "sparse_parent_capture"
    if tape["windows"]:
        matching = [window for window in tape["windows"]
            if window.get("execution_id") == execution_id
            and set(contracts) <= set(window["contracts"])
            and window["pre_start_mono_ns"] <= start
            and window["window_end_mono_ns"] >= end]
        if not matching:
            return "outside_capture_window"
        if not any(not any(stop.get("execution_id") == execution_id
                and stop.get("window_start_mono_ns") == window["window_start_mono_ns"]
                and stop["end_mono_ns"] < end for stop in tape["ends"])
                for window in matching):
            return "unfinished_window"
    elif end > anchor.get("legacy_window_end_ns", anchor["mono_ns"] + 2_000_000_000):
        return "outside_capture_window"
    baseline = [_latest(tape, contract, start) for contract in contracts]
    if not all(baseline):
        return "missing_book"
    if any(min(row["mono_ns"] for row in baseline) <= gap <= end for gap in tape["gaps"]):
        return "capture_gap"
    if tape["windows"] and not any(all(
            row["mono_ns"] >= window["pre_start_mono_ns"] or (
                window.get("prehistory_boundary_books", {}).get(row["data"]["contract"]) ==
                    {key: row.get(key) for key in ("run_id", "seq", "mono_ns", "loss_epoch")}
                and row.get("loss_epoch", 0) == window.get("prehistory_loss_epoch"))
            for row in baseline) for window in matching):
        return "missing_book"
    heartbeat_index = bisect_left(tape["heartbeat_times"], end)
    drained_index = bisect_left(tape["drained_times"], end)
    heartbeat = tape["heartbeats"][heartbeat_index] if heartbeat_index < len(tape["heartbeats"]) else None
    for index in range(drained_index, len(tape["drained"])):
        candidate = tape["drained"][index]
        if candidate["data"].get("observed_through_mono_ns", candidate["mono_ns"]) >= end:
            heartbeat = candidate
            break
    if heartbeat is None:
        return "incomplete_window"
    if heartbeat.get("loss_epoch", 0) != anchor.get("loss_epoch", 0) or any(
            row.get("loss_epoch", 0) != anchor.get("loss_epoch", 0)
            for row in baseline):
        return "capture_loss"
    if (heartbeat["data"].get("queue_depth", 0) > 0
            or heartbeat["data"].get("observed_through_mono_ns", heartbeat["mono_ns"]) < end):
        return "writer_backlog"
    return None


def _visible(tape: dict[str, Any], anchor: dict[str, Any], at: int) -> dict[str, Any]:
    """Evaluate displayed size and a fee-frozen paired edge at one observation cutoff."""
    data = anchor["data"]
    current = [_latest(tape, contract, at) for contract in data["pair"]]
    if at == anchor["mono_ns"]:
        current = [anchor.get("baseline_books", {}).get(contract, row)
            for contract, row in zip(data["pair"], current, strict=True)]
    reason = _coverage(tape, anchor, data["pair"], anchor["mono_ns"], at)
    if not all(current):
        reason = "missing_decision_book" if anchor.get("baseline_books") else "missing_book"
    point: dict[str, Any] = {"ms": (at - anchor["mono_ns"]) / 1e6, "reason": reason,
        "displayed_executable": None, "net_edge_estimate_frozen_fees": None,
        "positive_edge_frozen_fees": None, "predict_displayed_executable": None,
        "predict_best_quote": None, "predict_visible_quantity": None}
    if reason is not None:
        return point
    depths = [_depth(row["data"], data["side"], Decimal(limit), Decimal(data["quantity"]))
        for row, limit in zip(current, data["limits"], strict=True)]
    predict_index = next(i for i, contract in enumerate(data["pair"]) if contract.startswith("predict:"))
    predict_depth = depths[predict_index]
    quotes = current[predict_index]["data"]["asks" if data["side"] == "buy" else "bids"]
    point.update({"predict_displayed_executable": predict_depth[1] is not None if predict_depth[2] else None,
        "predict_best_quote": quotes[0][0] if quotes else None, "predict_visible_quantity": str(predict_depth[0])})
    if not all(depth[2] for depth in depths):
        point["reason"] = "truncated_depth"
        return point
    point["displayed_executable"] = all(depth[1] is not None for depth in depths)
    if len(depths) == 2 and data.get("fee_per_contract") is not None:
        if point["displayed_executable"]:
            prices = sum((depth[1] for depth in depths), Decimal(0))
            gross = 1 - prices if data["side"] == "buy" else prices - 1
            edge = gross - Decimal(data["fee_per_contract"])
            point["net_edge_estimate_frozen_fees"] = str(edge)
            point["positive_edge_frozen_fees"] = edge > 0
        else:
            point["positive_edge_frozen_fees"] = False
    original = anchor.get("baseline_books", {}).get(data["pair"][predict_index],
        _latest(tape, data["pair"][predict_index], anchor["mono_ns"]))
    arrival = current[predict_index]["data"].get("arrival_at_ns")
    previous_arrival = original["data"].get("arrival_at_ns") if original else None
    point["new_predict_confirmation"] = arrival != previous_arrival if arrival is not None and previous_arrival is not None else None
    point["book_local_ages_ms"] = [(at - row["data"]["arrival_at_ns"]) / 1e6
        if row["data"].get("arrival_at_ns") is not None else None for row in current]
    return point


def _persistence(points: list[dict[str, Any]], field: str) -> dict[str, Any]:
    """Describe observed losses and separate episodes, censoring missing intervals."""
    episodes = []
    active = None
    first_loss = None
    first_loss_interval = None
    last_state = None
    previous_ms = None
    continuous_through = None
    interrupted = False
    for point in points:
        state = point[field]
        if state is True and active is None:
            active = {"start_ms": point["ms"], "end_ms": None,
                "start_left_censored": last_state is None, "end_right_censored": True}
            episodes.append(active)
        elif state is not True and active is not None:
            active["end_ms"] = point["ms"]
            active["end_right_censored"] = state is None
            active = None
        if state is False and first_loss is None:
            first_loss = point["ms"]
            first_loss_interval = [previous_ms if last_state is True else None, point["ms"]]
        if state is None:
            interrupted = True
        elif not interrupted:
            continuous_through = point["ms"]
        last_state = state
        previous_ms = point["ms"]
    last_observed = next((point["ms"] for point in reversed(points) if point[field] is not None), None)
    return {"first_observed_loss_ms": first_loss,
        "first_loss_interval_ms": first_loss_interval,
        "first_loss_right_censored": first_loss is None,
        "observed_through_ms": continuous_through, "last_observation_ms": last_observed,
        "reappearance_count": sum(not episode["start_left_censored"] for episode in episodes[1:]),
        "episodes": episodes}


def _pretrade(tape: dict[str, Any], anchor: dict[str, Any], contract: str,
              limit: Decimal, quantity: Decimal) -> list[dict[str, Any]]:
    """Measure earlier observed quote and depth changes; never use a later book.

    Notes
    -----
    - Price movements describe locally observed quotes, not pure venue volatility.
    - Incomplete pre-windows retain descriptive observations but no full-window
      update rate. Truncated depth is a lower bound, not proven depletion.
    """
    at, side = anchor["mono_ns"], anchor["data"]["side"]
    result = []
    for ms in (500, 1000, 2000):
        start = at - ms * 1_000_000
        baseline = _latest(tape, contract, start)
        values = ([baseline] if baseline else []) + [row for row in tape["books"].get(contract, [])
            if start < row["mono_ns"] < at]
        distinct = []
        for row in values:
            if (not distinct or _identity(row) != _identity(distinct[-1])
                    or any(row["data"].get(key) != distinct[-1]["data"].get(key) for key in ("asks", "bids"))):
                distinct.append(row)
        reason = _coverage(tape, anchor, [contract], start, at - 1)
        depths = [_depth(row["data"], side, limit, quantity) for row in distinct]
        if reason is None and not all(depth[2] for depth in depths):
            reason = "truncated_depth"
        quotes = [Decimal(row["data"]["asks" if side == "buy" else "bids"][0][0])
            for row in distinct if row["data"]["asks" if side == "buy" else "bids"]]
        spreads = [Decimal(row["data"]["asks"][0][0]) - Decimal(row["data"]["bids"][0][0])
            for row in distinct if row["data"]["asks"] and row["data"]["bids"]]
        sizes = [depth[0] for depth in depths]
        lower_bound = any(row["data"].get("asks_truncated" if side == "buy" else "bids_truncated") for row in distinct)
        jumps = [abs(right - left) for left, right in zip(quotes, quotes[1:])]
        updates = sum(row["mono_ns"] > start for row in distinct)
        result.append({"ms": ms, "reason": reason, "complete": reason is None,
            "observation_count": len(distinct), "book_update_count": updates,
            "book_updates_per_second": updates * 1000 / ms if reason is None else None,
            "observed_span_ms": max(0, distinct[-1]["mono_ns"] - max(start, distinct[0]["mono_ns"])) / 1e6 if distinct else None,
            "quote_range": str(max(quotes) - min(quotes)) if quotes else None,
            "net_quote_move": str(quotes[-1] - quotes[0]) if len(quotes) > 1 else None,
            "max_abs_quote_jump": str(max(jumps)) if jumps else None,
            "sum_abs_quote_changes": str(sum(jumps, Decimal(0))) if jumps else None,
            "visible_quantity_start": str(sizes[0]) if sizes else None,
            "visible_quantity_end": str(sizes[-1]) if sizes else None,
            "visible_quantity_min": str(min(sizes)) if sizes else None,
            "visible_quantity_depletion": str(sizes[0] - sizes[-1]) if len(sizes) > 1 and not lower_bound else None,
            "depth_is_lower_bound": lower_bound,
            "minimum_volume_cushion": str(min(sizes) / quantity) if sizes else None,
            "spread_start": str(spreads[0]) if spreads else None,
            "spread_end": str(spreads[-1]) if spreads else None,
            "spread_range": str(max(spreads) - min(spreads)) if spreads else None})
    return result


def _sample(tape: dict[str, Any], anchor: dict[str, Any], horizons_ms: tuple[int, ...]) -> dict[str, Any]:
    """Build signal or actual-order features using a single recorder's observation clock."""
    data, at = anchor["data"], anchor["mono_ns"]
    baseline = [_latest(tape, contract, at) for contract in data["pair"]]
    baseline = [anchor.get("baseline_books", {}).get(contract, row)
        for contract, row in zip(data["pair"], baseline, strict=True)]
    index = next(i for i, contract in enumerate(data["pair"]) if contract.startswith("predict:"))
    predict = baseline[index]
    sample = {**data, "run_id": anchor["run_id"], "wall_ns": anchor["wall_ns"],
        "mono_ns": at, "book_identities": [_identity(row) for row in baseline],
        "trigger_venue": max((row for row in baseline if row),
            key=lambda row: row["mono_ns"], default={"data": {}})["data"].get("venue"),
        "predict_source_age_ms": None, "predict_local_age_ms": None,
        "predict_depth_ratio": None, "pretrade_movement": [],
        "predict_source_clock_status": "missing_source_timestamp"}
    if predict:
        book = predict["data"]
        quantity, limit = Decimal(data["quantity"]), Decimal(data["limits"][index])
        available, _, _ = _depth(book, data["side"], limit, quantity)
        sample.update({"predict_source_age_ms": (anchor["wall_ns"] - book["source_at_ns"]) / 1e6
                if book.get("source_at_ns") is not None else None,
            "predict_local_age_ms": (at - book["arrival_at_ns"]) / 1e6
                if book.get("arrival_at_ns") is not None else
                    (at - book["received_at_ns"]) / 1e6 if book.get("received_at_ns") is not None else None,
            "predict_local_clock_basis": "transport_callback" if book.get("arrival_at_ns") is not None else
                "adapter_received" if book.get("received_at_ns") is not None else "unavailable",
            "predict_source_age_unavailable_reason": None if book.get("source_at_ns") is not None else
                "missing_source_timestamp",
            "predict_visible_quantity": str(available), "predict_depth_ratio": str(available / quantity),
            "predict_depth_is_lower_bound": bool(book.get("asks_truncated" if data["side"] == "buy" else "bids_truncated")),
            "pretrade_movement": _pretrade(tape, anchor, data["pair"][index], limit, quantity)})
        if book.get("source_at_ns") is not None:
            source_delta = book.get("arrival_wall_at_ns", anchor["wall_ns"])
            source_delta = (source_delta if source_delta is not None else anchor["wall_ns"]) - book["source_at_ns"]
            sample["predict_source_clock_status"] = (
                "negative_raw_delta" if source_delta < 0 or sample["predict_source_age_ms"] < 0 else
                "clock_discontinuity" if any(before < at and after > predict["mono_ns"]
                    for before, after in tape.get("clock_changes", ())) else "uncalibrated")
        market = data["pair"][index].split(":")[1]
        pending = [row for row in tape["rows"] if row["kind"] == "predict_payload"
            and str(row["data"].get("market_id")) == market and row["mono_ns"] <= at]
        sample["settlements_pending"] = pending[-1]["data"].get("pending") if pending else None
    cutoffs = {at, *(at + ms * 1_000_000 for ms in horizons_ms)}
    cutoffs.update(row["mono_ns"] for contract in data["pair"] for row in tape["books"].get(contract, [])
        if at < row["mono_ns"] <= at + max(horizons_ms) * 1_000_000)
    points = [_visible(tape, anchor, cutoff) for cutoff in sorted(cutoffs)]
    horizons = []
    for point in points:
        if point["ms"] not in horizons_ms:
            continue
        earlier = [previous for previous in points if previous["ms"] <= point["ms"]]
        for field, name in (("displayed_executable", "observed_continuous_executable"),
                ("positive_edge_frozen_fees", "observed_continuous_positive_edge")):
            states = [previous[field] for previous in earlier]
            point[name] = False if False in states else None if None in states else True
        horizons.append(point)
    sample.update({"horizons": horizons, "initial_visibility": points[0],
        "quantity_persistence": _persistence(points, "displayed_executable"),
        "predict_quantity_persistence": _persistence(points, "predict_displayed_executable"),
        "edge_persistence_frozen_fees": _persistence(points, "positive_edge_frozen_fees")})
    first_quote = points[0]["predict_best_quote"]
    moves = [Decimal(point["predict_best_quote"]) - Decimal(first_quote) for point in points
        if first_quote is not None and point["predict_best_quote"] is not None]
    sample["observed_adverse_predict_quote_move"] = (any(move > 0 if data["side"] == "buy" else move < 0 for move in moves)
        if moves else None)
    return sample


def signal_samples(rows: list[dict[str, Any]], *, incomplete: bool = False,
                   horizons_ms: tuple[int, ...] = (100, 250, 500)) -> list[dict[str, Any]]:
    """Measure observed signal episodes without inferring survival from absent data.

    Parameters
    ----------
    rows
        Capture rows from one recorder, including any retained continuation slots.
    incomplete
        File-level diagnostic flag retained for API compatibility. Coverage is
        evaluated per window so valid evidence before a later stop remains usable.
    horizons_ms
        Positive local-observation horizons; legacy callers retain three points.

    Notes
    -----
    - Repeated signals within 500 ms share an episode unless an observed loss or
      missing coverage separates them. Quote survival never guarantees a fill.
    """
    if not horizons_ms or min(horizons_ms) <= 0:
        raise ValueError("Visibility horizons must be positive")
    tape = _index(rows)
    episodes = {}
    seen = set()
    result = []
    for signal in (row for row in tape["rows"] if row["kind"] == "signal"):
        signal = _signal_anchor(signal)
        data, at = signal["data"], signal["mono_ns"]
        if data["signal_id"] in seen:
            continue
        seen.add(data["signal_id"])
        if not any(contract.startswith("predict:") for contract in data["pair"]):
            continue
        sample = _sample(tape, signal, horizons_ms)
        key = (*data["pair"], data["side"])
        previous = episodes.get(key)
        share = previous is not None and at - previous[0]["mono_ns"] <= 500_000_000
        if share:
            cutoffs = {at, *(row["mono_ns"] for contract in data["pair"]
                for row in tape["books"].get(contract, []) if previous[0]["mono_ns"] <= row["mono_ns"] <= at)}
            share = all(_visible(tape, previous[0], cutoff)["positive_edge_frozen_fees"] is True for cutoff in cutoffs)
        sample["episode_id"] = previous[1] if share else data["signal_id"]
        episodes[key] = (signal, sample["episode_id"])
        result.append(sample)
    return result


def order_samples(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Join order hashes, cancellations and non-regressing native fill evidence.

    Notes
    -----
    - Application snapshots require finalized chain proof before establishing
      a terminal quantity. Ordinary application status is not venue proof.
    - A cancelled zero-fill REST snapshot alone remains unresolved because
      settlement may become visible after cancellation.
    - Preparation is not submission. Missing submission capture remains unknown;
      entry into the adapter establishes an attempt, not venue receipt.
    """
    prepared = {}
    prepared_rows = {}
    attempts = {}
    finalized = {}
    private = defaultdict(list)
    cancellations = defaultdict(list)
    verified = {}
    submission_attempts = set()
    accepted_submissions = set()
    timelines = defaultdict(list)
    native_timelines = defaultdict(list)
    for row in rows:
        data = row["data"]
        point = {"event": row["kind"], "run_id": row["run_id"], "wall_ns": row["wall_ns"],
            "mono_ns": row["mono_ns"], "status": data.get("status"), "filled": data.get("filled")}
        if data.get("client_id"):
            timelines[data["client_id"]].append(point)
        if row["kind"] == "prepared" and data["venue"] == "PREDICT":
            prepared[data["client_id"]] = data
            prepared_rows[data["client_id"]] = row
        elif row["kind"] == "submit_started":
            submission_attempts.add(data["client_id"])
            attempts.setdefault(data["client_id"], row)
        elif row["kind"] == "submission" and str(data.get("status")).lower() == "accepted":
            accepted_submissions.add(data["client_id"])
        elif row["kind"] == "private":
            private[str(data.get("orderHash") or "").lower()].append(data)
            native_timelines[str(data.get("orderHash") or "").lower()].append({
                **point, "event": data.get("type"), "venue_timestamp": data.get("timestamp")})
        elif row["kind"] == "cancel_requested":
            cancellations[data["client_id"]].append(data["reason"])
        elif row["kind"] == "venue_verification":
            key = data["hash"].lower()
            if (Decimal(str(data.get("filled") or 0)), _finalized(data)) >= (
                    Decimal(str(verified.get(key, {}).get("filled") or 0)), _finalized(verified.get(key, {}))):
                verified[key] = data
        if row["kind"] in {"snapshot", "submission"} and _finalized(data):
            client = data.get("client_id")
            if Decimal(str(data["filled"])) >= Decimal(str(finalized.get(client, {}).get("filled") or 0)):
                finalized[client] = data
    result = []
    for client, order in prepared.items():
        order = _submitted_parameters(order)
        order_hash = str(order["native"].get("hash") or "").lower()
        native_events = private[order_hash] if order_hash else []
        fills = {}
        for event in native_events:
            if event.get("type") == "orderTransactionSuccess" and event.get("settlementId"):
                fill = event.get("fill") or {}
                fills[event["settlementId"]] = max(fills.get(event["settlementId"], Decimal(0)),
                    Decimal(str(fill.get("executedSizeWei") or 0)) / Decimal(10**18))
        quantity = sum(fills.values(), Decimal(0))
        verification = verified.get(order_hash, {})
        if verification.get("filled") is not None:
            quantity = max(quantity, Decimal(str(verification.get("filled") or 0)))
        chain = finalized.get(client, {})
        if chain:
            quantity = max(quantity, Decimal(str(chain["filled"])))
        submission_state = (
            "venue_observed" if native_events or client in accepted_submissions
            or verification.get("filled") is not None or chain else
            "attempt_observed" if client in submission_attempts else "unknown"
        )
        outcome = "submission_unknown" if submission_state == "unknown" else "unresolved"
        if quantity > 0:
            outcome = "filled" if quantity >= Decimal(order["quantity"]) else "partial"
        elif chain or _finalized(verification):
            outcome = "proven_zero"
        elif any(e.get("reason") == "noMarketMatch" for e in native_events):
            outcome = "no_match_reported"
        elif any(e.get("type") == "orderTransactionSubmitted" for e in native_events):
            submitted = {e.get("settlementId") for e in native_events if e.get("type") == "orderTransactionSubmitted"}
            failed = {e.get("settlementId") for e in native_events if e.get("type") == "orderTransactionFailed"}
            outcome = "settlement_pending" if submitted - failed else "settlement_failed"
        result.append({**order, "submission_state": submission_state,
            "outcome": outcome, "confirmed_filled": str(quantity),
            "outcome_group": outcome if outcome in {"filled", "partial", "proven_zero"} else "unknown",
            "capture_run_id": prepared_rows[client]["run_id"],
            "prepared_mono_ns": prepared_rows[client]["mono_ns"],
            "prepared_wall_ns": prepared_rows[client]["wall_ns"],
            "submit_started_mono_ns": attempts.get(client, {}).get("mono_ns"),
            "submit_started_wall_ns": attempts.get(client, {}).get("wall_ns"),
            "finalized_chain_evidence": chain or (verification if _finalized(verification) else None),
            "cancel_reasons": sorted(set(cancellations[client])),
            "native_events": sorted({e.get("type", "unknown") for e in native_events}),
            "verification": verification,
            "timeline": sorted(timelines[client] + native_timelines[order_hash], key=lambda p: p["wall_ns"])})
    return result


def _submitted_parameters(order: dict[str, Any]) -> dict[str, Any]:
    """Prefer captured signed share quantity and explicit price over pre-rounding intent."""
    result = {**order, "requested_quantity": order["quantity"], "requested_limit": order.get("limit"),
        "parameter_source": "prepared_intent"}
    native = order.get("native", {})
    side = native.get("side")
    if str(side) not in {"0", "1"} or order.get("side") != ("buy" if str(side) == "0" else "sell"):
        return result
    shares = native.get("takerAmount" if str(side) == "0" else "makerAmount")
    price = native.get("pricePerShare")
    if shares is not None and price is not None and int(shares) > 0 and int(price) > 0:
        result.update(quantity=str(Decimal(shares) / Decimal(10**18)),
            limit=str(Decimal(price) / Decimal(10**18)), parameter_source="signed_request")
    return result


def _finalized(data: dict[str, Any]) -> bool:
    """Require explicit terminal chain proof; cancelled REST zero is insufficient."""
    block = data.get("settlement_finalized_block")
    return (data.get("may_receive_more_fills") is False and data.get("filled") is not None
        and str(data.get("status")).lower() in {"cancelled", "filled"}
        and isinstance(block, int) and not isinstance(block, bool) and block > 0)


def _order_features(order: dict[str, Any], tapes: dict[str, dict[str, Any]],
                    all_rows: list[dict[str, Any]]) -> dict[str, Any] | None:
    """Join actual order parameters to decision and submission observations.

    Notes
    -----
    - Entry orders join by execution ID, contract and side. Original and recovery
      orders never share an anchor merely because their execution ID matches.
    - Monotonic timestamps come from the same captured host. Venue timestamps
      are retained only for source age and identity, never observation ordering.
    - Paired edge requires equal actual leg quantities and a captured fee estimate.
    """
    contract = order.get("contract")
    if not contract or not order.get("limit") or not order.get("side"):
        return None
    original = order.get("role") != "recovery"
    cutoff = order.get("submit_started_wall_ns") or order["prepared_wall_ns"]
    decisions = [row for row in all_rows if row["kind"] in {"decision", "book_checkpoint"}
        and row["data"].get("client_id") == order["client_id"]
        and row["data"].get("contract") == contract and row["data"].get("side") == order["side"]
        and row["data"].get("captured_wall_ns") is not None
        and row["data"]["captured_wall_ns"] <= cutoff
        and row["data"].get("phase") != "submission"
        and row["data"].get("validation_reason", "accepted") == "accepted"]
    matching_decisions = [row for row in decisions
        if Decimal(row["data"]["quantity"]) == Decimal(order["requested_quantity"])
        and Decimal(row["data"]["limit"]) == Decimal(order["requested_limit"])]
    decision = max(matching_decisions, key=lambda row: row["data"]["captured_wall_ns"], default=None)
    candidates = []
    for tape in tapes.values():
        for row in tape["rows"]:
            if (row["kind"] == "signal" and row["data"]["signal_id"] == order["execution_id"]
                    and contract in row["data"]["pair"] and row["data"]["side"] == order["side"] and original):
                candidates.append((tape, row))
    if not candidates:
        eligible = [(run_id, tape) for run_id, tape in tapes.items()
            if run_id == order["capture_run_id"] or any(
                window.get("execution_id") == order["execution_id"]
                and contract in window.get("contracts", ()) for window in tape["windows"])]
        if not eligible:
            return None
        at, wall = order["prepared_mono_ns"], order["prepared_wall_ns"]
        created = order.get("intent_created_wall_ns")
        if created is not None and created <= wall:
            at -= wall - created
            wall = created
        for run_id, tape in eligible:
            previous = [row for row in tape["rows"] if row["mono_ns"] <= at]
            candidates.append((tape, {"kind": "order_anchor", "run_id": run_id, "mono_ns": at, "wall_ns": wall,
                "loss_epoch": previous[-1].get("loss_epoch", 0) if previous else 0,
                "data": {"pair": [contract], "limits": [order["limit"]], "side": order["side"]}}))
    samples = []
    for tape, row in candidates:
        data = {**row["data"], "execution_id": order["execution_id"], "quantity": order["quantity"]}
        data["limits"] = list(data["limits"])
        data["limits"][data["pair"].index(contract)] = order["limit"]
        peers = [candidate["data"] for candidate in all_rows if candidate["kind"] == "prepared"
            and candidate["data"].get("execution_id") == order["execution_id"]
            and candidate["data"].get("role") != "recovery"
            and candidate["data"].get("contract") in data["pair"]
            and candidate["data"].get("contract") != contract]
        peer = peers[-1] if original and peers else None
        for index, pair_contract in enumerate(data["pair"]):
            if peer and pair_contract == peer["contract"]:
                data["limits"][index] = peer["limit"]
        if len(data["pair"]) == 2 and (peer is None or Decimal(peer["quantity"]) != Decimal(order["quantity"])
                or peer["side"] != order["side"]):
            data["fee_per_contract"] = None
        anchor = {**(_signal_anchor(row) if row["kind"] == "signal" else row), "data": data}
        identity_match = None
        if decision:
            captured = decision["data"]["captured_wall_ns"]
            anchor.update({"mono_ns": decision["data"].get("captured_mono_ns",
                decision["mono_ns"] - (decision["wall_ns"] - captured)),
                "wall_ns": captured})
            source_hash = decision["data"].get("source_hash")
            if source_hash is not None:
                matching_books = [book for book in tape["books"].get(contract, [])
                    if book["mono_ns"] <= anchor["mono_ns"] and book["data"].get("source_hash") == source_hash]
                identity_match = bool(matching_books)
                anchor["baseline_books"] = {contract: matching_books[-1] if matching_books else None}
            if decision["data"].get("book") is not None:
                checkpoint_book = {**decision, "kind": "book", "mono_ns": anchor["mono_ns"],
                    "wall_ns": captured, "data": decision["data"]["book"]}
                anchor["baseline_books"] = {contract: checkpoint_book}
                observed = _latest(tape, contract, anchor["mono_ns"])
                raw_book = checkpoint_book["data"]
                identity_match = (all(observed["data"].get(key) == raw_book.get(key) for key in
                    ("source_at_ns", "source_hash", "source_timestamp_kind", "asks", "bids"))
                    if observed is not None and tape.get("producer") != "parent"
                    and (raw_book.get("source_at_ns") is not None or raw_book.get("source_hash") is not None)
                    else None)
                # Capture both validated legs, not a mix of the exact Predict
                # checkpoint and an unrelated cached complementary book.
                for peer_decision in all_rows:
                    other = peer_decision["data"]
                    if (peer_decision["kind"] == "book_checkpoint" and other.get("book") is not None
                            and other.get("execution_id") == order["execution_id"]
                            and other.get("contract") in data["pair"] and other.get("contract") != contract
                            and other.get("phase") == decision["data"].get("phase")
                            and other.get("captured_wall_ns") == captured):
                        anchor["baseline_books"][other["contract"]] = {**peer_decision, "kind": "book",
                            "mono_ns": anchor["mono_ns"], "wall_ns": captured, "data": other["book"]}
        sample = _sample(tape, anchor, (100, 250, 500, 1000, 1500))
        sample.update({"basis": "actual_submitted_order" if order["submit_started_mono_ns"] is not None else "prepared_order",
            "anchor_kind": "decision_snapshot" if decision else "signal_decision" if row["kind"] == "signal" else
                "intent_creation_wall_clock_mapped" if order.get("intent_created_wall_ns") is not None else "preparation_observation",
            "signal_quantity": row["data"].get("quantity"), "signal_limits": row["data"].get("limits"),
            "client_id": order["client_id"], "role": order.get("role"),
            "paired_actual_parameters_complete": peer is not None and data.get("fee_per_contract") is not None,
            "join": "client_order_id_actual_parameters_source_hash" if decision else
                "legacy_execution_id_contract_side" if row["kind"] == "signal" else "client_order_id",
            "decision_book_identity_match": identity_match,
            "decision_snapshot": decision["data"] if decision else None,
            "book_checkpoints": [item["data"] for item in all_rows
                if item["kind"] == "book_checkpoint" and item["data"].get("client_id") == order["client_id"]],
            "decision_revisions": list({(item["data"]["captured_wall_ns"], item["data"]["quantity"], item["data"]["limit"]):
                item["data"] for item in decisions}.values()),
            "recorded_signal_book_generations": row["data"].get("book_generations")})
        if sample["anchor_kind"] == "preparation_observation":
            sample["preparation_movement"] = sample["pretrade_movement"]
            sample["pretrade_movement"] = []
        submitted = order["submit_started_mono_ns"]
        sample["post_send"] = None
        if submitted is not None and submitted >= anchor["mono_ns"]:
            send = {**anchor, "mono_ns": submitted, "wall_ns": order["submit_started_wall_ns"],
                "legacy_window_end_ns": row["mono_ns"] + 2_000_000_000}
            send.pop("baseline_books", None)
            sample["post_send"] = _sample(tape, send, (100, 250, 500, 1000, 1500))
            sample["decision_to_submit_ms"] = (submitted - anchor["mono_ns"]) / 1e6
        samples.append(sample)
    return max(samples, key=lambda sample: (
        sum(point["reason"] is None for point in (sample.get("post_send") or sample)["horizons"]),
        sum(point["complete"] for point in sample["pretrade_movement"]),
        sample["decision_book_identity_match"] is True, -sample["mono_ns"]))


def analyze(root: Path) -> dict[str, Any]:
    """Produce an offline report without writing files or changing trading settings."""
    samples = []
    all_rows = []
    runs = []
    captures: dict[str, list[dict[str, Any]]] = defaultdict(list)
    previous_segments = {}
    items = sorted(inventory(root), key=lambda item: (
        item["manifest"]["run_id"], item["manifest"].get("segment_index", 0)))
    for item in items:
        rows, incomplete = read_capture(Path(item["slot"]))
        manifest = item["manifest"]
        run_id, segment = manifest["run_id"], manifest.get("segment_index", 0)
        runs.append({"run_id": run_id, "slot": item["slot"], "producer": manifest["producer"],
            "segment_index": segment, "incomplete": incomplete})
        previous = previous_segments.get(run_id)
        broken_link = previous and manifest.get("previous_slot", Path(previous[1]).name) != Path(previous[1]).name
        if ((rows or captures[run_id]) and ((previous is None and segment != 0)
                or (previous and segment != previous[0] + 1) or broken_link or (not rows and incomplete))):
            gap = {**(rows[0] if rows else captures[run_id][-1]),
                "kind": "capture_gap", "data": {"reason": "missing_segment"}}
            if captures[run_id]:
                gap["mono_ns"] = max(row["mono_ns"] for row in captures[run_id]) + 1
            captures[run_id].append(gap)
        captures[run_id].extend(rows)
        previous_segments[run_id] = (segment, item["slot"])
    for rows in captures.values():
        samples.extend(signal_samples(rows, horizons_ms=(100, 250, 500, 1000, 1500)))
        all_rows.extend(rows)
    orders = order_samples(all_rows)
    producers = {item["run_id"]: item["producer"] for item in runs}
    tapes = {run_id: {**_index(rows), "producer": producers[run_id]} for run_id, rows in captures.items()}
    # Prefer complete worker observations over the parent's sparse atomic handoffs.
    features = {s["signal_id"]: s for s in sorted(samples,
        key=lambda s: sum(p["reason"] is None for p in s["horizons"]))}
    for order in orders:
        order["book_checkpoints"] = [row["data"] for row in all_rows
            if row["kind"] == "book_checkpoint" and row["data"].get("client_id") == order["client_id"]]
        order["features"] = _order_features(order, tapes, all_rows)
        feature = order["features"] or {}
        post = feature.get("post_send") or {}
        persistence = post.get("quantity_persistence", {})
        initial = post.get("initial_visibility", {})
        if post.get("observed_adverse_predict_quote_move") is True:
            order["observed_book_evidence"] = "adverse_observed_predict_quote_move"
        elif (initial.get("predict_displayed_executable") is True
                and post.get("predict_quantity_persistence", {}).get("first_observed_loss_ms") is not None):
            order["observed_book_evidence"] = "displayed_predict_liquidity_removed"
        elif initial.get("displayed_executable") is True and persistence.get("first_observed_loss_ms") is not None:
            order["observed_book_evidence"] = "displayed_pair_liquidity_removed"
        elif (order["outcome_group"] in {"unknown", "proven_zero"} and post.get("horizons")
                and all(point.get("observed_continuous_executable") is True for point in post["horizons"])):
            order["observed_book_evidence"] = "remained_displayed_without_confirmed_match"
        else:
            order["observed_book_evidence"] = "unknown"
    episodes = {}
    for sample in features.values():
        if sample["signal_id"] == sample["episode_id"]:
            episodes[sample["episode_id"]] = sample
    survival = {str(ms): dict(Counter(
        p["reason"] or ("observed_continuous" if p["observed_continuous_executable"] is True else
            "observed_interruption" if p["observed_continuous_executable"] is False else "unknown")
        for sample in episodes.values() for p in sample["horizons"] if p["ms"] == ms))
        for ms in (100, 250, 500, 1000, 1500)}
    comparisons = Counter()
    for order in orders:
        if order["submission_state"] == "unknown":
            continue
        feature = order["features"] or {}
        age = feature.get("predict_source_age_ms")
        if feature.get("predict_source_clock_status") in {"negative_raw_delta", "clock_discontinuity"}:
            age = None
        local_age = feature.get("predict_local_age_ms")
        depth = feature.get("predict_depth_ratio")
        movement = next((point for point in feature.get("pretrade_movement", [])
            if point["ms"] == 500 and point["complete"]), {})
        quote_range = movement.get("quote_range")
        comparisons[(order["role"], order["native"].get("strategy"),
            order["native"].get("isFillOrKill"), feature.get("trigger_venue"),
            "unknown" if age is None else "under_300ms" if age < 300 else "300ms_or_more",
            "unknown" if depth is None else "under_2x" if Decimal(depth) < 2 else "2x_or_more",
            order["outcome"], order["submission_state"], order.get("side"), order.get("contract"),
            order["quantity"], order["outcome_group"],
            "unknown" if local_age is None else "under_100ms" if local_age < 100 else "100ms_or_more",
            "unknown" if quote_range is None else "unchanged_observed_quote" if Decimal(quote_range) == 0 else "observed_quote_changed")] += 1
    return {"schema": SCHEMA, "runs": runs, "signals": len(features),
        "episodes": len(episodes), "survival": survival, "orders": orders,
        "signal_samples": list(features.values()),
        "comparisons": [{"role": k[0], "strategy": k[1], "fok": k[2], "trigger_venue": k[3],
            "source_age_bucket": k[4], "depth_bucket": k[5], "outcome": k[6],
            "submission_state": k[7], "side": k[8], "contract": k[9], "quantity": k[10],
            "outcome_group": k[11], "local_age_bucket": k[12], "pretrade_quote_bucket": k[13], "count": count}
            for k, count in comparisons.items()],
        "outcome_feature_summary": _feature_summary(orders),
        "outcomes": dict(Counter(o["outcome"] for o in orders if o["role"] != "recovery")),
        "submission_states": dict(Counter(o["submission_state"] for o in orders if o["role"] != "recovery")),
        "limitations": ["Displayed depth is not a fill guarantee.",
            "Source ages are uncalibrated wall-clock differences; negative deltas are never classified as fresh.",
            "Intervals crossing observed wall/monotonic discontinuities are censored, not treated as liquidity failures.",
            "Horizon edge estimates freeze per-contract fees at detection; size/price changes can change actual fees.",
            "Cancelled zero-fill observations are unresolved, not proven failures.",
            "Prepared orders without submission or venue evidence are excluded from comparisons.",
            "An observed submission attempt does not prove venue receipt; missing capture remains unknown.",
            "Source ages use uncalibrated wall clocks; local ages remain separate.",
            "Price movement is observed quote movement, not pure venue volatility or a causal explanation for fills.",
            "Source timestamps never backdate books; incomplete or interrupted windows are censored.",
            "Side, market and quantity can confound comparisons; no filter is automatically enabled.",
            "First observed losses bracket changes between observations and are not exact venue lifetimes.",
            "Recovery intent wall-clock mapping is approximate; legacy preparation observations omit predecision movement."]}


def _feature_summary(orders: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Compare observed outcomes with counts and descriptive ranges, without fitted filters."""
    groups = defaultdict(list)
    for order in orders:
        if order["submission_state"] != "unknown":
            groups[("recovery" if order.get("role") == "recovery" else "initial",
                order.get("side"), order.get("contract"), order["outcome_group"])].append(order)
    result = []
    for (role, side, contract, outcome), values in groups.items():
        metrics = defaultdict(list)
        for order in values:
            feature = order["features"] or {}
            raw = {"quantity": order["quantity"], **{key: feature.get(key) for key in (
                "predict_source_age_ms", "predict_local_age_ms", "predict_depth_ratio")}}
            for point in feature.get("pretrade_movement", []):
                if point["complete"]:
                    raw.update({f"pre_{point['ms']}ms_{key}": point.get(key) for key in (
                        "quote_range", "max_abs_quote_jump", "book_updates_per_second",
                        "visible_quantity_depletion", "spread_end")})
            for key, value in raw.items():
                if value is not None:
                    metrics[key].append(Decimal(str(value)))
        result.append({"role": role, "side": side, "contract": contract, "outcome_group": outcome,
            "orders": len(values), "metrics": {key: {"observations": len(numbers),
                "min": str(min(numbers)), "median": str(median(numbers)), "max": str(max(numbers))}
                for key, numbers in metrics.items()}})
    return result


def verify(root: Path, limit: int = 50) -> None:
    """Read at most ``limit`` recorded orders and matches into a new diagnostic slot.

    Notes
    -----
    - Authentication is explicitly warmed here, never in the execution hot path.
    - Each order uses the existing read-only reconciler and its bounded matches
      page. Missing evidence stays unresolved; no financial projection is changed.
    """
    from prediction_markets.infrastructure.venues.predict.execution import PredictExecutionAdapter
    from prediction_markets.domain.shared.value_objects import ClientOrderID, VenueID
    from prediction_markets.domain.trading.value_objects import OrderReference

    if not 1 <= limit <= 100:
        raise ValueError("Verification limit must be between 1 and 100")
    orders = analyze(root)["orders"]
    orders = list({o["native"]["hash"]: o for o in orders if o["native"].get("hash")}.values())[-limit:]
    if not orders:
        return
    recorder = FillStudyRecorder(root, "offline-verification")
    adapter = None
    try:
        adapter = PredictExecutionAdapter(timeout_seconds=5)
        adapter.refresh_auth(reason="manual")
        for order in orders:
            order_hash = order["native"]["hash"]
            try:
                reference = OrderReference(VenueID("PREDICT"), ClientOrderID(order["client_id"]),
                    json.dumps({"schema": 1, "order_hash": order_hash, "contract_id": order["contract"],
                        "side": order["side"], "quantity": order["quantity"], "limit_price": order["limit"]}).encode())
                result = adapter.reconcile(reference)
                snapshot = result.snapshot
                recorder.offer("venue_verification", {"hash": order_hash,
                    "status": snapshot.status.value if snapshot else "UNKNOWN",
                    "filled": str(snapshot.filled_quantity.value) if snapshot else None,
                    "may_receive_more_fills": snapshot.may_receive_more_fills if snapshot else True,
                    "settlement_finalized_block": snapshot.settlement_finalized_block if snapshot else None})
            except Exception as error:
                recorder.offer("venue_verification", {"hash": order_hash,
                    "status": "UNKNOWN", "error_type": type(error).__name__})
            time.sleep(0.2)
    finally:
        if adapter is not None:
            adapter.close()
        recorder.close()


def clean(root: Path, run_id: str) -> None:
    """Validate every segment before deleting one explicitly confirmed closed run.

    Raises
    ------
    ValueError
        If ownership or continuation links are uncertain, any segment is not
        closed, or the run ID is unknown. Validation precedes every deletion.
    OSError
        If the storage lock cannot be acquired or an owned file cannot be removed.

    Notes
    -----
    - Serializes validation and removal with runtime slot reservation and
      retention so a newly reused slot cannot be mistaken for the old run.
    """
    with study_storage_lock(root):
        _clean_locked(root, run_id)


def _clean_locked(root: Path, run_id: str) -> None:
    """Validate and remove one complete run while its storage lock is held."""
    targets = sorted((item for item in inventory(root) if item["manifest"]["run_id"] == run_id),
        key=lambda item: item["manifest"].get("segment_index", 0))
    if not targets:
        raise ValueError("Unknown diagnostic run ID")
    for index, item in enumerate(targets):
        slot = Path(item["slot"]).resolve()
        if slot.parent != root.resolve() or item["manifest"].get("segment_index", 0) != index:
            raise ValueError("Diagnostic run has missing or unsafe segments")
        summary = slot / "summary.json"
        if not summary.exists():
            raise ValueError("Recording is not closed; stop capture before cleaning")
        metadata = json.loads(summary.read_text(encoding="utf-8"))
        allowed = {"rotated"} if index + 1 < len(targets) else {"closed", "disk_limit", "io_error"}
        if (metadata.get("run_id") != run_id or metadata.get("status") not in allowed
                or metadata.get("segment_index", 0) != index):
            raise ValueError("Recording summary does not prove a closed owned run")
        previous_slot = Path(targets[index - 1]["slot"]).name if index else None
        next_slot = Path(targets[index + 1]["slot"]).name if index + 1 < len(targets) else None
        if item["manifest"].get("previous_slot") != previous_slot or metadata.get("next_slot") != next_slot:
            raise ValueError("Diagnostic continuation links do not match the complete run")
        for entry in item["files"]:
            path = Path(entry["path"])
            if path.is_symlink() or not path.is_file() or path.resolve().parent != slot:
                raise ValueError("Refusing a linked or out-of-slot diagnostic file")
    for item in targets:
        for entry in item["files"]:
            Path(entry["path"]).unlink()
        Path(item["slot"]).rmdir()


def main() -> None:
    """Expose read-only reports, explicit venue-verification capture and owned cleanup."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("inventory", "analyze", "verify", "clean"))
    parser.add_argument("--root", type=Path, default=study_root())
    parser.add_argument("--limit", type=int, default=50)
    parser.add_argument("--confirm-run-id")
    args = parser.parse_args()
    if args.command == "verify":
        verify(args.root, args.limit)
    elif args.command == "clean":
        if not args.confirm_run_id:
            parser.error("clean requires --confirm-run-id from inventory")
        clean(args.root, args.confirm_run_id)
    else:
        print(json.dumps(inventory(args.root) if args.command == "inventory" else analyze(args.root), indent=2))


if __name__ == "__main__":
    main()
