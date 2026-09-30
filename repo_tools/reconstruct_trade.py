"""Reconstruct one arbitrage trade from the journal and PostgreSQL.

The command combines the causal event stream with SQL ledger state and writes a
stable Markdown report. Opaque prepared requests and recovery bytes are redacted.

Responsibilities
----------------
- Correlate execution, order, fill, latency, and book evidence.
- Preserve millisecond wall-clock deltas and monotonic phase durations.
- Detect hard consistency problems without inventing latency thresholds.
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from collections import defaultdict
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
import psycopg
from psycopg.rows import dict_row

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from prediction_markets.infrastructure.binary_journal import _iter_file_entries
from prediction_markets.application.execution.accounting import manual_resolution_client_order_id


TICK = chr(96)
PHASES = {
    "ArbitrageOpportunityFound": "Oportunidad detectada",
    "ArbitragePlanned": "Plan creado",
    "SubmitOrder": "Comando emitido",
    "OrderPrepared": "Orden preparada",
    "SubmissionReceived": "Respuesta inicial",
    "OrderSnapshotUpdated": "Estado de orden",
    "TradeRecorded": "Fill contabilizado",
    "ExecutionUpdated": "Estado de ejecución",
}
STAGES = {
    "book_arrival_skew_ms": "Desfase de llegada de books",
    "newest_book_to_plan_ms": "Book más nuevo → plan",
    "plan_to_dispatcher_ms": "Plan → dispatcher",
    "dispatcher_prepare_ms": "Preparación paralela",
    "prepare_to_guard_ms": "Preparación → guard",
    "guard_ms": "Validación del guard",
    "guard_to_both_submits_ms": "Guard → ambos submits",
}


def _args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Reconstruct one trade into a detailed Markdown report.",
    )
    parser.add_argument(
        "execution_id",
        nargs="?",
        help="Execution identifier; omit it to use the latest execution.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Markdown path; defaults to reports/trades/<execution-id>.md.",
    )
    parser.add_argument(
        "--journal",
        type=Path,
        help="Journal anchor; defaults to JOURNAL_PATH or data/trading.log.",
    )
    return parser.parse_args()


def _load_database(execution_id: str | None) -> tuple[str, dict[str, Any]]:
    """Load SQL evidence in one read-only transaction.

    Parameters
    ----------
    execution_id
        Requested execution, or None to select the latest persisted row.

    Returns
    -------
    tuple[str, dict[str, Any]]
        Resolved identifier and all SQL evidence needed by the report.

    Raises
    ------
    SystemExit
        If the database is not configured or the execution does not exist.
    """
    load_dotenv(ROOT / ".env")
    dsn = os.getenv("DATABASE_URL") or os.getenv("POSTGRES_DSN")
    if not dsn:
        raise SystemExit("DATABASE_URL or POSTGRES_DSN is not configured")

    with psycopg.connect(
        dsn,
        connect_timeout=5,
        row_factory=dict_row,
    ) as connection:
        connection.execute("SET TRANSACTION READ ONLY")
        if execution_id is None:
            row = connection.execute(
                "SELECT execution_id FROM arbitrage_execution_journals "
                "ORDER BY updated_at DESC LIMIT 1",
            ).fetchone()
            if row is None:
                raise SystemExit("No execution journals were found")
            execution_id = row["execution_id"]

        journal = connection.execute(
            "SELECT * FROM arbitrage_execution_journals WHERE execution_id = %s",
            (execution_id,),
        ).fetchone()
        if journal is None:
            raise SystemExit(f"Execution not found: {execution_id}")

        commands = connection.execute(
            "SELECT * FROM execution_order_commands WHERE execution_id = %s "
            "ORDER BY role",
            (execution_id,),
        ).fetchall()
        client_ids = [row["client_order_id"] for row in commands]
        client_ids.append(str(manual_resolution_client_order_id(execution_id)))
        orders = connection.execute(
            "SELECT * FROM orders WHERE client_order_id = ANY(%s) "
            "ORDER BY client_order_id",
            (client_ids,),
        ).fetchall()
        trades = connection.execute(
            "SELECT * FROM trades WHERE client_order_id = ANY(%s) "
            "ORDER BY executed_at, trade_id",
            (client_ids,),
        ).fetchall()
        projected = connection.execute(
            "SELECT * FROM projected_events WHERE correlation_id = %s "
            "ORDER BY journal_sequence",
            (execution_id,),
        ).fetchall()
        opportunity = connection.execute(
            "SELECT * FROM arbitrage_opportunities WHERE opportunity_id = %s",
            (execution_id,),
        ).fetchone()
        checkpoint = connection.execute(
            "SELECT last_sequence, updated_at FROM journal_projection_checkpoints "
            "WHERE projector_name = 'trading_read_model_v1'",
        ).fetchone()

    return execution_id, {
        "journal": journal,
        "commands": commands,
        "orders": orders,
        "trades": trades,
        "projected": projected,
        "opportunity": opportunity,
        "checkpoint": checkpoint,
    }


def _journal_paths(anchor: Path) -> list[Path]:
    paths = [anchor] if anchor.is_file() else []
    segment_dir = anchor.parent / f"{anchor.name}.segments"
    if segment_dir.is_dir():
        paths.extend(sorted(segment_dir.glob("segment-*.pmj")))
    return paths


def _belongs(event: object, execution_id: str, clients: set[str]) -> bool:
    if str(getattr(event, "execution_id", "")) == execution_id:
        return True
    if str(getattr(getattr(event, "execution", None), "id", "")) == execution_id:
        return True
    if str(getattr(event, "id", "")) == execution_id:
        return True
    command = getattr(event, "command", None)
    if str(getattr(command, "execution_id", "")) == execution_id:
        return True
    trade = getattr(event, "trade", None)
    return str(getattr(trade, "client_order_id", "")) in clients


def _read_journal(
    anchor: Path,
    execution_id: str,
    clients: set[str],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Read a stable journal prefix and retain execution-related events.

    Parameters
    ----------
    anchor
        Legacy file used to locate all retained journal segments.
    execution_id
        Stable identifier shared by execution events.
    clients
        Client order identifiers used to correlate fill events.

    Returns
    -------
    tuple[list[dict[str, Any]], dict[str, Any]]
        Safe event summaries and retained-sequence coverage.

    Notes
    -----
    - The project reader validates frame magic, length, and CRC.
    - Prepared request and recovery bytes are never returned.
    """
    paths = _journal_paths(anchor)
    matches: list[dict[str, Any]] = []
    first_sequence = last_sequence = None
    error = None
    try:
        for path in paths:
            stable_size = path.stat().st_size
            for entry in _iter_file_entries(path, stable_size):
                first_sequence = first_sequence or entry.sequence
                last_sequence = entry.sequence
                if _belongs(entry.event, execution_id, clients):
                    matches.append(
                        {
                            "sequence": entry.sequence,
                            "recorded_at": entry.recorded_at.value,
                            "kind": type(entry.event).__name__,
                            "role": _role(entry.event),
                            "detail": _detail(entry.event),
                        },
                    )
    except (OSError, RuntimeError, ValueError) as exc:
        error = f"{type(exc).__name__}: {exc}"
    return matches, {
        "anchor": str(anchor),
        "segments": len(paths),
        "first_sequence": first_sequence,
        "last_sequence": last_sequence,
        "error": error,
    }

def _role(event: object) -> str:
    role = getattr(event, "role", None)
    if role is None:
        role = getattr(getattr(event, "command", None), "role", None)
    if role is None:
        client_id = str(
            getattr(getattr(event, "trade", None), "client_order_id", ""),
        )
        role = "primary" if client_id.endswith("-primary") else None
        role = "hedge" if client_id.endswith("-hedge") else role
    return str(role or "—")


def _detail(event: object) -> str:
    kind = type(event).__name__
    if kind == "ArbitrageOpportunityFound":
        value = event.opportunity
        return (
            f"{value.side.value}; qty={value.quantity.value}; "
            f"net_edge={value.net_edge}"
        )
    if kind == "ArbitragePlanned":
        value = event.execution
        return (
            f"status={value.status.value}; "
            f"{value.leg1_venue_id} {value.leg1_quantity.value}@"
            f"{value.leg1_limit_price.value}; "
            f"{value.leg2_venue_id} {value.leg2_quantity.value}@"
            f"{value.leg2_limit_price.value}"
        )
    if kind == "SubmitOrder":
        intent = event.intent
        return (
            f"{event.venue_id}; {intent.side.value} {intent.quantity.value} @ "
            f"{getattr(intent.limit_price, 'value', None)}"
        )
    if kind == "OrderPrepared":
        return (
            f"{event.command.venue_id}; request={len(event.prepared.request)} bytes; "
            f"recovery={len(event.prepared.reference.recovery_data)} bytes; "
            "contenido oculto"
        )
    if kind == "SubmissionReceived":
        snapshot = event.result.snapshot
        detail = f"status={event.result.status.value}"
        if snapshot is not None:
            detail += (
                f"; order={snapshot.order_id}; venue_status={snapshot.status.value}"
            )
        reason = event.result.reason or (snapshot.reason if snapshot is not None else None)
        return f"{detail}; reason={reason}" if reason else detail
    if kind == "OrderSnapshotUpdated":
        value = event.snapshot
        detail = (
            f"source={event.source}; status={value.status.value}; "
            f"filled={value.filled_quantity.value}/{value.quantity.value}; "
            f"avg={getattr(value.average_price, 'value', None)}"
        )
        return f"{detail}; reason={value.reason}" if value.reason else detail
    if kind == "TradeRecorded":
        value = event.trade
        fee = (
            f"{value.fee.amount} {value.fee.currency}"
            if value.fee is not None
            else "desconocida"
        )
        return (
            f"{value.quantity.value} @ {value.price.value}; "
            f"trade={value.id}; fee={fee}"
        )
    if kind == "ExecutionUpdated":
        value = event.execution
        detail = (
            f"status={value.status.value}; residual={value.residual_quantity.value}"
        )
        return (
            f"{detail}; error={value.last_error}"
            if value.last_error
            else detail
        )
    return "evento correlacionado"


def _decimal(value: Any) -> Decimal:
    return value if isinstance(value, Decimal) else Decimal(str(value or 0))


def _trade_totals(trades: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for trade in trades:
        grouped[trade["client_order_id"]].append(trade)
    result: dict[str, dict[str, Any]] = {}
    for client_id, values in grouped.items():
        quantity = sum((_decimal(row["quantity"]) for row in values), Decimal())
        notional = sum(
            (
                _decimal(row["quantity"]) * _decimal(row["price"])
                for row in values
            ),
            Decimal(),
        )
        fees = [
            _decimal(row["fee_settlement_amount"])
            for row in values
            if row["fee_settlement_amount"] is not None
            and row["fee_settlement_currency"] == "USD"
        ]
        result[client_id] = {
            "quantity": quantity,
            "average": notional / quantity if quantity else None,
            "fee": sum(fees, Decimal()),
            "fees_known": len(fees) == len(values),
        }
    return result


def _financials(data: dict[str, Any]) -> dict[str, Any]:
    journal = data["journal"]
    totals = _trade_totals(data["trades"])
    primary = totals.get(journal["leg1_client_order_id"], {})
    hedge = totals.get(journal["leg2_client_order_id"], {})
    gross = fees = net = None
    if (
        primary.get("average") is not None
        and hedge.get("average") is not None
        and primary.get("quantity") == hedge.get("quantity")
        and primary.get("quantity", Decimal()) > 0
        and journal["leg1_side"] == journal["leg2_side"]
    ):
        quantity = primary["quantity"]
        price_sum = primary["average"] + hedge["average"]
        gross = quantity * (
            Decimal(1) - price_sum
            if journal["leg1_side"] == "buy"
            else price_sum - Decimal(1)
        )
        if primary["fees_known"] and hedge["fees_known"]:
            fees = primary["fee"] + hedge["fee"]
            net = gross - fees
    return {
        "primary": primary,
        "hedge": hedge,
        "gross": gross,
        "fees": fees,
        "net": net,
    }


def _remaining_exposure(data: dict[str, Any]) -> Decimal:
    """Calculate unmatched quantity after recorded recovery and manual trades.

    Notes
    -----
    Initial fills already belong to the execution journal. Other fills add to
    their original leg on the same side, or subtract when unwinding that leg.
    Match both venue and contract; unrelated fills cannot neutralize exposure.

    Raises
    ------
    ValueError
        If a corrective fill cannot be assigned to exactly one original leg.
    """
    journal = data["journal"]
    prefixes = ("leg1", "leg2")
    quantities = [_decimal(journal[f"{leg}_filled_quantity"]) for leg in prefixes]
    initial_clients = {journal[f"{leg}_client_order_id"] for leg in prefixes}
    for trade in data["trades"]:
        if trade["client_order_id"] in initial_clients:
            continue
        matches = [
            index for index, leg in enumerate(prefixes)
            if trade["venue_id"] == journal[f"{leg}_venue_id"]
            and trade["contract_id"] == journal[f"{leg}_contract_id"]
        ]
        if len(matches) != 1 or trade["side"] not in {"buy", "sell"}:
            raise ValueError(f"Cannot assign corrective trade {trade['trade_id']} to a leg")
        index = matches[0]
        direction = 1 if trade["side"] == journal[f"{prefixes[index]}_side"] else -1
        quantities[index] += direction * _decimal(trade["quantity"])
    return abs(quantities[0] - quantities[1])


def _checks(
    data: dict[str, Any],
    raw: list[dict[str, Any]],
    raw_meta: dict[str, Any],
) -> list[tuple[str, str, str]]:
    journal = data["journal"]
    issues: list[tuple[str, str, str]] = []
    fill1 = _decimal(journal["leg1_filled_quantity"])
    fill2 = _decimal(journal["leg2_filled_quantity"])
    residual = _decimal(journal["residual_quantity"])
    try:
        expected_residual = _remaining_exposure(data)
    except ValueError as error:
        expected_residual = None
        issues.append(("warning", "Residual not verifiable", str(error)))
    if journal["status"] in {"completed", "recovered"} and expected_residual:
        issues.append(
            (
                "critical",
                "Terminal state has residual exposure",
                f"status={journal['status']}, remaining={expected_residual}",
            ),
        )
    elif journal["status"] == "completed" and fill1 == fill2 == 0:
        issues.append(
            (
                "info" if journal.get("last_error") else "warning",
                "Ejecución terminada sin fills",
                str(journal.get("last_error") or "No existe razón persistida"),
            ),
        )
    if expected_residual is not None and residual != expected_residual:
        issues.append(
            (
                "critical",
                "Residual inconsistente",
                f"persistido={residual}, calculado={expected_residual}",
            ),
        )
    if str(journal.get("last_error") or "").strip():
        issues.append(("warning", "Error persistido", str(journal["last_error"])))
    trace = journal.get("latency_trace")
    if not isinstance(trace, dict):
        issues.append(
            ("warning", "Sin traza de latencia", "No existe latency_trace JSONB"),
        )
    elif trace.get("error"):
        issues.append(
            ("warning", "Guard rechazó la ejecución", str(trace["error"])),
        )

    totals = _trade_totals(data["trades"])
    for role, prefix in (("primary", "leg1"), ("hedge", "leg2")):
        client_id = journal[f"{prefix}_client_order_id"]
        persisted = _decimal(journal[f"{prefix}_filled_quantity"])
        recorded = totals.get(client_id, {}).get("quantity", Decimal())
        if recorded != persisted:
            issues.append(
                (
                    "critical",
                    f"Fills SQL no cuadran en {role}",
                    f"journal={persisted}, trades={recorded}",
                ),
            )
        if persisted > 0 and not totals.get(client_id, {}).get(
            "fees_known",
            False,
        ):
            issues.append(
                ("warning", f"Fees incompletas en {role}", client_id),
            )

    for command in data["commands"]:
        role = command["role"]
        prepared = command["prepared_sequence"]
        submitted = command["submitted_sequence"]
        if prepared is None:
            issues.append(
                ("warning", f"Falta OrderPrepared en {role}", command["client_order_id"]),
            )
        if submitted is None:
            issues.append(
                (
                    "warning",
                    f"Falta SubmissionReceived en {role}",
                    command["client_order_id"],
                ),
            )
        if prepared is not None and submitted is not None and prepared >= submitted:
            issues.append(
                (
                    "critical",
                    f"Orden causal inválido en {role}",
                    "prepared_sequence >= submitted_sequence",
                ),
            )

    target_last = max(
        [row["sequence"] for row in raw]
        + [row["journal_sequence"] for row in data["projected"]],
        default=0,
    )
    checkpoint = (data["checkpoint"] or {}).get("last_sequence", 0)
    if checkpoint < target_last:
        issues.append(
            (
                "warning",
                "Proyección PostgreSQL atrasada",
                f"checkpoint={checkpoint}, trade_hasta={target_last}",
            ),
        )
    if raw_meta["error"]:
        issues.append(
            ("warning", "Lectura del journal incompleta", raw_meta["error"]),
        )
    elif not raw:
        issues.append(
            (
                "warning",
                "Eventos crudos no retenidos",
                f"journal disponible desde #{raw_meta['first_sequence']}",
            ),
        )

    planned = next(
        (
            row["summary"]
            for row in data["projected"]
            if row["event_type"] == "ArbitragePlanned"
        ),
        {},
    )
    for role, key in (
        ("primary", "leg1_decision"),
        ("hedge", "leg2_decision"),
    ):
        decision = planned.get(key) if isinstance(planned, dict) else None
        if decision and _decimal(decision.get("shortfall_quantity")) > 0:
            issues.append(
                (
                    "critical",
                    f"Profundidad insuficiente en {role}",
                    f"shortfall={decision['shortfall_quantity']}",
                ),
            )
    if isinstance(trace, dict):
        for leg in trace.get("legs", []):
            if leg.get("book_replaced_before_guard"):
                issues.append(
                    (
                        "info",
                        f"Book reemplazado antes del guard ({leg.get('role')})",
                        "El guard validó una actualización más reciente",
                    ),
                )
    if not issues:
        issues.append(
            ("ok", "Sin inconsistencias detectadas", "Journal y ledger cuadran"),
        )
    return issues

def _escape(value: Any) -> str:
    return (
        str(value if value is not None else "—")
        .replace("|", "\\|")
        .replace("\n", "<br>")
    )


def _iso_ms(value: datetime | None) -> str:
    if value is None:
        return "—"
    return (
        value.astimezone(timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


def _fmt_ms(value: Any) -> str:
    return "—" if value is None else f"{float(value):.3f}"


def _timeline(
    raw: list[dict[str, Any]],
    projected: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    if raw:
        return raw
    return [
        {
            "sequence": row["journal_sequence"],
            "recorded_at": row["recorded_at"],
            "kind": row["event_type"],
            "role": (row["summary"] or {}).get("role", "—"),
            "detail": ", ".join(
                f"{key}={value}"
                for key, value in (row["summary"] or {}).items()
                if key not in {"leg1_decision", "leg2_decision"}
            ),
        }
        for row in projected
    ]


def _mermaid(timeline: list[dict[str, Any]], data: dict[str, Any]) -> str:
    journal = data["journal"]
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in timeline:
        grouped[(row["kind"], row["role"])].append(row)

    def first(kind: str, role: str = "—") -> dict[str, Any] | None:
        values = grouped.get((kind, role), [])
        return values[0] if values else None

    def last(kind: str, role: str) -> dict[str, Any] | None:
        values = grouped.get((kind, role), [])
        return values[-1] if values else None

    lines = [
        "sequenceDiagram",
        "    participant DET as Detector",
        "    participant ENG as Engine",
        "    participant JRN as Journal",
        f"    participant PRI as {_mermaid_text(journal['leg1_venue_id'])} primary",
        f"    participant HED as {_mermaid_text(journal['leg2_venue_id'])} hedge",
        "    participant SQL as PostgreSQL",
    ]
    opportunity = first("ArbitrageOpportunityFound")
    planned = first("ArbitragePlanned")
    if opportunity:
        lines.append(f"    DET->>JRN: #{opportunity['sequence']} oportunidad")
    if planned:
        lines.append(f"    ENG->>JRN: #{planned['sequence']} plan")
    lines.append("    par primary")
    lines.extend(
        _mermaid_leg(
            "primary",
            "PRI",
            first,
            last,
            journal.get("latency_trace"),
        ),
    )
    lines.append("    and hedge")
    lines.extend(
        _mermaid_leg(
            "hedge",
            "HED",
            first,
            last,
            journal.get("latency_trace"),
        ),
    )
    lines.append("    end")
    final = next(
        (
            row
            for row in reversed(timeline)
            if row["kind"] == "ExecutionUpdated"
        ),
        None,
    )
    if final:
        lines.append(
            f"    ENG->>JRN: #{final['sequence']} {journal['status']}",
        )
    checkpoint = (data["checkpoint"] or {}).get("last_sequence", "?")
    lines.append(f"    JRN-->>SQL: proyección confirmada hasta #{checkpoint}")
    return "\n".join(lines)


def _mermaid_leg(
    role: str,
    participant: str,
    first: Any,
    last: Any,
    trace: Any,
) -> list[str]:
    command = first("SubmitOrder", role)
    prepared = first("OrderPrepared", role)
    response = first("SubmissionReceived", role)
    terminal = last("OrderSnapshotUpdated", role)
    latency = next(
        (
            leg
            for leg in (trace or {}).get("legs", [])
            if leg.get("role") == role
        ),
        {},
    )
    values: list[str] = []
    if command:
        values.append(
            f"        ENG->>JRN: #{command['sequence']} comando",
        )
    if prepared:
        values.append(
            f"        JRN->>{participant}: #{prepared['sequence']} preparado",
        )
    if response:
        values.append(
            f"        {participant}-->>JRN: #{response['sequence']} ACK "
            f"({_fmt_ms(latency.get('submit_to_ack_ms'))} ms)",
        )
    if terminal:
        values.append(
            f"        {participant}-->>JRN: #{terminal['sequence']} terminal "
            f"({_fmt_ms(latency.get('ack_to_terminal_ms'))} ms)",
        )
    return values or [
        f"        Note over JRN,{participant}: sin eventos crudos retenidos",
    ]


def _mermaid_text(value: Any) -> str:
    return re.sub(r"[^A-Za-z0-9 _.-]", "", str(value)) or "Venue"

def _decision_rows(data: dict[str, Any]) -> list[str]:
    planned = next(
        (
            row["summary"]
            for row in data["projected"]
            if row["event_type"] == "ArbitragePlanned"
        ),
        {},
    )
    rows = [
        "",
        "### Order books usados para decidir",
        "",
        "| Leg | Capturado UTC | Edad | Disponible | Solicitado | Shortfall | VWAP | Niveles |",
        "|---|---|---:|---:|---:|---:|---:|---|",
    ]
    for role, key in (
        ("primary", "leg1_decision"),
        ("hedge", "leg2_decision"),
    ):
        decision = planned.get(key) if isinstance(planned, dict) else None
        if not decision:
            rows.append(
                f"| {role} | — | — | — | — | — | — | no disponible |",
            )
            continue
        levels = ", ".join(
            f"{level['quantity']}@{level['price']}"
            for level in decision.get("levels", [])
        )
        age_ms = _decimal(decision.get("book_age_ns")) / Decimal(1_000_000)
        rows.append(
            "| {role} | {captured} | {age} ms | {available} | {requested} | "
            "{shortfall} | {vwap} | {levels} |".format(
                role=role,
                captured=_escape(decision.get("captured_at")),
                age=_fmt_ms(age_ms),
                available=decision.get("available_quantity", "—"),
                requested=decision.get("requested_quantity", "—"),
                shortfall=decision.get("shortfall_quantity", "—"),
                vwap=decision.get("vwap", "—"),
                levels=_escape(levels or "—"),
            ),
        )
    return rows


def _execution_rows(
    data: dict[str, Any],
    financials: dict[str, Any],
) -> list[str]:
    journal = data["journal"]
    orders = {row["client_order_id"]: row for row in data["orders"]}
    rows = [
        "",
        "## Decisión, órdenes y fills",
        "",
        "| Leg | Venue | Side | Plan qty@limit | Fill SQL qty@avg | Order ID | Fees settlement |",
        "|---|---|---|---|---|---|---:|",
    ]
    for role, prefix in (("primary", "leg1"), ("hedge", "leg2")):
        client_id = journal[f"{prefix}_client_order_id"]
        actual = financials[role]
        order = orders.get(client_id, {})
        rows.append(
            "| {role} | {venue} | {side} | {qty}@{limit} | "
            "{filled}@{average} | {tick}{order}{tick} | {fee} |".format(
                role=role,
                venue=_escape(journal[f"{prefix}_venue_id"]),
                side=journal[f"{prefix}_side"],
                qty=journal[f"{prefix}_quantity"],
                limit=journal[f"{prefix}_limit_price"],
                filled=actual.get("quantity", 0),
                average=actual.get("average", "—"),
                tick=TICK,
                order=_escape(
                    order.get("order_id") or journal[f"{prefix}_order_id"],
                ),
                fee=_money(actual.get("fee") if actual else None),
            ),
        )
    initial_clients = {journal[f"{leg}_client_order_id"] for leg in ("leg1", "leg2")}
    corrective_trades = [
        trade for trade in data["trades"] if trade["client_order_id"] not in initial_clients
    ]
    if corrective_trades:
        rows.extend([
            "", "### Recovery and manual fills used to calculate residual", "",
            "| Client order | Venue | Contract | Side | Quantity | Price |",
            "|---|---|---|---|---:|---:|",
        ])
        for trade in corrective_trades:
            rows.append("| " + " | ".join(
                _escape(trade[key]) for key in (
                    "client_order_id", "venue_id", "contract_id", "side", "quantity", "price",
                )
            ) + " |")
    rows.extend(_decision_rows(data))
    return rows


def _market(opportunity: dict[str, Any] | None) -> str:
    if not opportunity:
        return "no proyectado"
    if opportunity.get("monitor_key"):
        return str(opportunity["monitor_key"])
    return (
        f"{opportunity.get('underlying')} · "
        f"{opportunity.get('interval_seconds')}s"
    )


def _money(value: Any) -> str:
    return (
        "—"
        if value is None
        else f"{TICK}{_decimal(value):.8f} USD{TICK}"
    )


def _largest_interval(trace: dict[str, Any]) -> str:
    candidates = [
        (STAGES.get(key, key), value)
        for key, value in trace.get("stages", {}).items()
        if value is not None
    ]
    for leg in trace.get("legs", []):
        for key in (
            "prepare_ms",
            "journal_append_ms",
            "submit_to_ack_ms",
            "ack_to_first_fill_ms",
            "ack_to_terminal_ms",
        ):
            if leg.get(key) is not None:
                candidates.append(
                    (f"{leg.get('role')} · {key}", leg[key]),
                )
    if not candidates:
        return "—"
    name, value = max(candidates, key=lambda item: float(item[1]))
    return f"{name} · {_fmt_ms(value)} ms"

def _render(
    execution_id: str,
    data: dict[str, Any],
    raw: list[dict[str, Any]],
    raw_meta: dict[str, Any],
) -> str:
    journal = data["journal"]
    timeline = _timeline(raw, data["projected"])
    financials = _financials(data)
    issues = _checks(data, raw, raw_meta)
    first_at = (
        timeline[0]["recorded_at"] if timeline else journal["created_at"]
    )
    last_at = (
        timeline[-1]["recorded_at"] if timeline else journal["updated_at"]
    )
    duration_ms = (last_at - first_at).total_seconds() * 1000
    worst = (
        "critical"
        if any(row[0] == "critical" for row in issues)
        else "warning"
        if any(row[0] == "warning" for row in issues)
        else "ok"
    )
    headline = {"critical": "❌", "warning": "⚠️", "ok": "✅"}[worst]
    trace = journal.get("latency_trace") or {}
    largest_interval = _largest_interval(trace)

    lines = [
        f"# Reconstrucción de trade {TICK}{execution_id}{TICK}",
        "",
        f"> {headline} **{journal['status']}** · generado "
        f"{_iso_ms(datetime.now(timezone.utc))} · fuentes en modo solo lectura",
        "",
        "## Resumen ejecutivo",
        "",
        "| Campo | Valor |",
        "|---|---|",
        f"| Execution ID | {TICK}{execution_id}{TICK} |",
        f"| Mercado | {_escape(_market(data.get('opportunity')))} |",
        f"| Ventana causal observada | {_fmt_ms(duration_ms)} ms |",
        f"| Fills | primary {TICK}{journal['leg1_filled_quantity']}{TICK} · "
        f"hedge {TICK}{journal['leg2_filled_quantity']}{TICK} |",
        f"| Exposición residual | {TICK}{journal['residual_quantity']}{TICK} |",
        f"| PnL locked bruto | {_money(financials['gross'])} |",
        f"| Fees de settlement | {_money(financials['fees'])} |",
        f"| PnL locked neto | {_money(financials['net'])} |",
        f"| Resultado de latencia | "
        f"{TICK}{trace.get('outcome', 'sin traza')}{TICK} |",
        f"| Mayor intervalo observado | {_escape(largest_interval)} |",
        "",
        "## Secuencia causal",
        "",
        f"{TICK * 3}mermaid",
        _mermaid(timeline, data),
        TICK * 3,
        "",
        "### Timeline del journal",
        "",
        "| Seq | UTC | Δ previo (ms) | Δ inicio (ms) | Fase | Leg | Evidencia |",
        "|---:|---|---:|---:|---|---|---|",
    ]
    previous = first_at
    for row in timeline:
        at = row["recorded_at"]
        lines.append(
            "| {seq} | {time} | {delta} | {elapsed} | {phase} | {role} | "
            "{detail} |".format(
                seq=row["sequence"],
                time=_iso_ms(at),
                delta=_fmt_ms((at - previous).total_seconds() * 1000),
                elapsed=_fmt_ms((at - first_at).total_seconds() * 1000),
                phase=PHASES.get(row["kind"], row["kind"]),
                role=_escape(row["role"]),
                detail=_escape(row["detail"]),
            ),
        )
        previous = at

    lines.extend(
        [
            "",
            "## Latencias exactas",
            "",
            f"> Duraciones de {TICK}time.monotonic_ns(){TICK}; "
            "no sufren ajustes del reloj.",
            "",
            "### Fases globales",
            "",
            "| Fase | ms |",
            "|---|---:|",
        ],
    )
    for key, value in trace.get("stages", {}).items():
        lines.append(f"| {STAGES.get(key, key)} | {_fmt_ms(value)} |")
    lines.extend(
        [
            "",
            "### Por pierna",
            "",
            "| Leg | Venue | Book plan | Book guard | Prepare | Journal | "
            "Submit→ACK | ACK→fill | ACK→terminal |",
            "|---|---|---:|---:|---:|---:|---:|---:|---:|",
        ],
    )
    for leg in trace.get("legs", []):
        lines.append(
            "| {role} | {venue} | {plan} | {guard} | {prepare} | {journal} | "
            "{ack} | {fill} | {terminal} |".format(
                role=_escape(leg.get("role")),
                venue=_escape(leg.get("venue")),
                plan=_fmt_ms(leg.get("book_age_at_plan_ms")),
                guard=_fmt_ms(leg.get("book_age_at_guard_ms")),
                prepare=_fmt_ms(leg.get("prepare_ms")),
                journal=_fmt_ms(leg.get("journal_append_ms")),
                ack=_fmt_ms(leg.get("submit_to_ack_ms")),
                fill=_fmt_ms(leg.get("ack_to_first_fill_ms")),
                terminal=_fmt_ms(leg.get("ack_to_terminal_ms")),
            ),
        )
    lines.extend(_execution_rows(data, financials))
    icons = {
        "critical": "❌",
        "warning": "⚠️",
        "info": "ℹ️",
        "ok": "✅",
    }
    lines.extend(
        [
            "",
            "## Problemas y observaciones",
            "",
            "| Nivel | Hallazgo | Evidencia |",
            "|---|---|---|",
        ],
    )
    for severity, title, evidence in issues:
        lines.append(
            f"| {icons[severity]} | {_escape(title)} | {_escape(evidence)} |",
        )

    checkpoint = data["checkpoint"] or {}
    lines.extend(
        [
            "",
            "## Procedencia y límites",
            "",
            f"- Journal: {TICK}{raw_meta['anchor']}{TICK}; "
            f"{raw_meta['segments']} segmento(s); cobertura "
            f"{TICK}#{raw_meta['first_sequence']}–"
            f"#{raw_meta['last_sequence']}{TICK}.",
            f"- PostgreSQL: transacción {TICK}READ ONLY{TICK}; projector "
            f"{TICK}#{checkpoint.get('last_sequence', '—')}{TICK} a "
            f"{_iso_ms(checkpoint.get('updated_at'))}.",
            "- Requests preparados y recovery_data: solo se muestran tamaños.",
            "- La tabla causal usa UTC del journal; las subfases internas solo "
            "tienen duración monotónica y no reciben timestamps inventados.",
            "",
        ],
    )
    return "\n".join(lines)


def main() -> int:
    """Generate a reconstruction and print its absolute report path.

    Returns
    -------
    int
        Zero after the Markdown report has been written.
    """
    args = _args()
    execution_id, data = _load_database(args.execution_id)
    journal_path = args.journal or Path(
        os.getenv("JOURNAL_PATH", "data/trading.log"),
    )
    if not journal_path.is_absolute():
        journal_path = ROOT / journal_path
    clients = {row["client_order_id"] for row in data["commands"]}
    clients.add(str(manual_resolution_client_order_id(execution_id)))
    raw, raw_meta = _read_journal(journal_path, execution_id, clients)
    output = (
        args.output
        or ROOT / "reports" / "trades" / f"{execution_id}.md"
    )
    if not output.is_absolute():
        output = ROOT / output
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        _render(execution_id, data, raw, raw_meta),
        encoding="utf-8",
    )
    print(output.resolve())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
