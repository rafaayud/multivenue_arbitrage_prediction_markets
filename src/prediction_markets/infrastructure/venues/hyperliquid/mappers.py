"""Normalize Hyperliquid YES/NO identities, metadata and full L2 snapshots.

Notes
-----
- Outcome encoding is ``10 * outcome + side``, with YES=0 and NO=1.
- Coins use ``#encoding``, balance tokens ``+encoding`` and order assets
  ``100_000_000 + encoding``. These identifiers are not ordinary spot indexes.
"""

import re
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

from prediction_markets.domain.contracts.entities import BinaryContract
from prediction_markets.domain.contracts.value_objects import LotSize, TickSize
from prediction_markets.domain.markets.entities import Market, MarketSide
from prediction_markets.domain.markets.enums import BinaryOutcome, MarketStatus
from prediction_markets.domain.markets.value_objects import MarketResolution, MarketState
from prediction_markets.domain.orderbook.entities import OrderBook
from prediction_markets.domain.orderbook.value_objects import OrderBookLevel
from prediction_markets.domain.shared.value_objects import (
    ContractID, Currency, EventID, MarketID, OutcomeID, Price, Quantity, Timestamp, VenueID,
)

HYPERLIQUID_VENUE_ID = VenueID("HYPERLIQUID")
OUTCOME_TICK = TickSize(Decimal("0.00001"))
OUTCOME_LOT = LotSize(Decimal("1"))


def hyperliquid_contract_id(outcome: int, side: int) -> ContractID:
    """Return a stable binary identity, validating the native outcome and side."""
    if type(outcome) is not int or outcome < 0 or type(side) is not int or side not in (0, 1):
        raise ValueError("Invalid Hyperliquid outcome or side")
    return ContractID(f"hyperliquid:{outcome}:{'yes' if side == 0 else 'no'}")


def parse_hyperliquid_contract_id(contract_id: ContractID) -> tuple[int, int]:
    match = re.fullmatch(r"hyperliquid:(0|[1-9][0-9]*):(yes|no)", str(contract_id))
    if match is None:
        raise ValueError(f"Invalid Hyperliquid outcome contract: {contract_id}")
    return int(match[1]), 0 if match[2] == "yes" else 1


def outcome_coin(contract_id: ContractID) -> str:
    outcome, side = parse_hyperliquid_contract_id(contract_id)
    return f"#{10 * outcome + side}"


def description_fields(description: str) -> dict[str, str]:
    """Read the venue's pipe-separated fields without changing their values."""
    return dict(part.split(":", 1) for part in description.split("|") if ":" in part)


def _render(template: str, fields: dict[str, str]) -> str:
    return re.sub(r"\{([^{}]+)\}", lambda match: fields.get(match[1], match[0]), template)


def outcome_to_market(
    payload: dict[str, Any],
    templates: dict[str, dict[str, Any]],
    parent: dict[str, Any] | None = None,
) -> tuple[Market, tuple[BinaryContract, ...]] | None:
    """Normalize listed YES/NO outcomes and their explicit resolution metadata.

    Notes
    -----
    - Non-YES/NO side labels and unrenderable templates are excluded.
    - Named outcomes inherit their parent question's parameters and deadline.
    - Missing deadlines remain unknown; no artificial expiry is introduced.
    """
    outcome = payload.get("outcome")
    if type(outcome) is not int or outcome < 0:
        raise ValueError("Hyperliquid outcome metadata lacks a numeric identity")
    sides = payload.get("sideSpecs", [])
    if not isinstance(sides, list) or any(not isinstance(side, dict) or not isinstance(side.get("name"), str) for side in sides):
        raise ValueError("Hyperliquid outcome has invalid side labels")
    if [side.get("name", "").removeprefix("template:").casefold() for side in sides] != ["yes", "no"]:
        return None
    name = str(payload.get("name", ""))
    if "fallback" in name.casefold():
        return None
    fields = description_fields(str((parent or {}).get("description", "")))
    fields.update(description_fields(str(payload.get("description", ""))))
    rules = str(payload.get("description", ""))
    if name.startswith("template:"):
        template = templates.get(name.removeprefix("template:"))
        if template is None:
            return None
        title = _render(str(template["name"]), fields)
        rules = _render(str(template.get("description", rules)), fields)
        if re.search(r"\{[^{}]+\}", title):
            return None
    elif name == "Recurring" and fields.get("class") == "priceBinary":
        title = f"{fields.get('underlying', '')} above {fields.get('targetPrice', '')} at {fields.get('expiry', '')}?"
    else:
        title = name
    if not title.strip():
        return None
    if parent:
        parent_name = str(parent.get("name", ""))
        parent_template = templates.get(parent_name.removeprefix("template:"), {})
        parent_title = _render(str(parent_template.get("name", parent_name)), fields)
        parent_rules = _render(str(parent_template.get("description", parent.get("description", ""))), fields)
        if parent_name.startswith("template:") and (not parent_template or re.search(r"\{[^{}]+\}", parent_title)):
            return None
        title = f"{parent_title}: {title}"
        rules = f"{parent_rules}\n\n{rules}"
    deadline = None
    for key in ("expiry", "time", "resolutionDeadline", "decisionDeadline", "scheduledDecision", "ipoDeadline"):
        raw = fields.get(key)
        if raw:
            try:
                deadline = Timestamp(datetime.strptime(raw, "%Y%m%d-%H%M").replace(tzinfo=timezone.utc))
            except ValueError:
                try:
                    deadline = Timestamp.from_iso(raw)
                except ValueError:
                    continue
            break
    currency = Currency(str(payload.get("quoteToken") or "USDC"))
    if str(currency) != "USDC":
        return None
    contracts = tuple(
        BinaryContract(
            id=hyperliquid_contract_id(outcome, side), market_id=MarketID(str(outcome)),
            outcome_id=OutcomeID(f"{outcome}:{'yes' if side == 0 else 'no'}"),
            venue_id=HYPERLIQUID_VENUE_ID, payout_currency=currency,
            symbol=f"#{10 * outcome + side}", tick_size=OUTCOME_TICK, lot_size=OUTCOME_LOT,
        )
        for side in (0, 1)
    )
    status = MarketStatus.ACTIVE
    if deadline is None:
        status = MarketStatus.UNKNOWN
    elif deadline <= Timestamp.now():
        status = MarketStatus.CLOSED
    return Market(
        id=MarketID(str(outcome)), venue_id=HYPERLIQUID_VENUE_ID, title=title,
        state=MarketState(status, close_time=deadline),
        yes_side=MarketSide(contracts[0].outcome_id, BinaryOutcome.YES),
        no_side=MarketSide(contracts[1].outcome_id, BinaryOutcome.NO),
        event_id=EventID(str(parent["question"])) if parent else None,
        description=rules or None,
        resolution=MarketResolution(rules=rules or None, source=fields.get("officialSource")),
    ), contracts


def hyperliquid_orderbook_to_order_book(
    payload: dict[str, Any], contract_id: ContractID,
) -> OrderBook:
    """Validate and normalize one full, independently quoted outcome book.

    Raises
    ------
    ValueError
        If identity, time, prices, quantities or book structure are invalid.
    """
    outcome, side = parse_hyperliquid_contract_id(contract_id)
    if payload.get("coin") != outcome_coin(contract_id):
        raise ValueError("Hyperliquid book coin differs from requested outcome")
    timestamp = payload.get("time")
    if type(timestamp) is not int or timestamp < 0:
        raise ValueError("Hyperliquid book has no valid millisecond timestamp")
    raw_levels = payload.get("levels")
    if not isinstance(raw_levels, list) or len(raw_levels) != 2:
        raise ValueError("Hyperliquid book must contain bids and asks")
    levels = []
    for index, raw_side in enumerate(raw_levels):
        if not isinstance(raw_side, list):
            raise ValueError("Hyperliquid book side must be a list")
        parsed = []
        for level in raw_side:
            if not isinstance(level, dict) or not all(isinstance(level.get(key), str) for key in ("px", "sz")):
                raise ValueError("Hyperliquid book levels require decimal strings")
            try:
                price, size = Decimal(level["px"]), Decimal(level["sz"])
            except (KeyError, TypeError, InvalidOperation) as error:
                raise ValueError("Invalid Hyperliquid book level") from error
            if not price.is_finite() or not size.is_finite() or not 0 <= price <= 1 or size < 0:
                raise ValueError("Invalid Hyperliquid book price or quantity")
            if size:
                parsed.append(OrderBookLevel(Price(price), Quantity(size)))
        levels.append(tuple(sorted(parsed, key=lambda level: level.price.value, reverse=index == 0)))
    return OrderBook(
        market_id=MarketID(str(outcome)),
        outcome_id=OutcomeID(f"{outcome}:{'yes' if side == 0 else 'no'}"),
        bids=levels[0], asks=levels[1],
        timestamp=Timestamp(datetime.fromtimestamp(timestamp / 1000, tz=timezone.utc)),
        source_at_ns=timestamp * 1_000_000,
    )
