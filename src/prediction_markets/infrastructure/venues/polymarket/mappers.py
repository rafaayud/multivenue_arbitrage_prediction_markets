"""Translate polymarket payloads into domain models.

Responsibilities
----------------
- Normalize external identifiers, prices, states, and outcomes.
"""

import json
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from prediction_markets.domain.contracts.entities import BinaryContract
from prediction_markets.domain.contracts.value_objects import LotSize, Payout, TickSize
from prediction_markets.domain.markets.entities import Market
from prediction_markets.domain.markets.entities import MarketSide
from prediction_markets.domain.markets.enums import BinaryOutcome
from prediction_markets.domain.markets.enums import MarketStatus
from prediction_markets.domain.markets.value_objects import MarketResolution
from prediction_markets.domain.markets.value_objects import MarketState
from prediction_markets.domain.orderbook.entities import OrderBook
from prediction_markets.domain.orderbook.value_objects import OrderBookLevel
from prediction_markets.domain.shared.value_objects import (
    ContractID,
    Currency,
    MarketID,
    OutcomeID,
    Price,
    Quantity,
    Timestamp,
    VenueID,
)

POLYMARKET_VENUE_ID = VenueID("POLYMARKET")
_QUANTITY_STEP = LotSize(Decimal("0.01"))


def polymarket_contract_id(condition_id: str, token_id: str) -> ContractID:
    """
    Build the stable domain id from a Polymarket condition and token id.

    Parameters
    ----------
    condition_id
        Polymarket condition identifier.
    token_id
        CLOB asset/token identifier.

    Returns
    -------
    ContractID
        Domain contract id retaining both protocol identifiers.
    """
    return ContractID(f"polymarket:{condition_id}:{token_id}")


def parse_polymarket_contract_id(contract_id: ContractID) -> tuple[str, str]:
    """
    Parse current or legacy Polymarket ids into condition and token identifiers.

    Parameters
    ----------
    contract_id
        Current ``polymarket:...`` id or supported legacy symbol.

    Returns
    -------
    tuple[str, str]
        ``(condition_id, token_id)`` used by CLOB requests.
    """
    value = str(contract_id)
    if value.startswith("polymarket:"):
        _, condition_id, token_id = value.split(":", maxsplit=2)
        return condition_id, token_id

    # Backward compatibility with earlier Nautilus-style IDs:
    # "{condition_id}-{token_id}.POLYMARKET".
    symbol = value.removesuffix(".POLYMARKET")
    if "-" in symbol:
        condition_id, token_id = symbol.split("-", maxsplit=1)
        return condition_id, token_id

    raise ValueError(f"Invalid Polymarket contract ID: {value}")


def polymarket_outcome_id(condition_id: str, outcome: str, token_id: str) -> OutcomeID:
    """
    Build an outcome id that preserves the CLOB token used for I/O.

    Parameters
    ----------
    condition_id
        Polymarket condition identifier.
    outcome
        Human-readable outcome label.
    token_id
        CLOB token identifier.

    Returns
    -------
    OutcomeID
        Domain outcome id with a normalized outcome label.
    """
    return OutcomeID(f"{condition_id}:{outcome.lower()}:{token_id}")


def gamma_market_to_contracts(market: dict[str, Any]) -> tuple[BinaryContract, ...]:
    """
    Map Gamma payload variants to contracts and their trading constraints.

    Parameters
    ----------
    market
        Raw Gamma market payload; token and outcome fields may be JSON strings.

    Returns
    -------
    tuple[BinaryContract, ...]
        Tuple of normalized contracts, empty when no condition or token ids are usable.
    """
    condition_id = str(
        market.get("conditionId")
        or market.get("condition_id")
        or market.get("condition_id".lower())
        or "",
    )
    if not condition_id:
        return ()

    token_ids = _coerce_list(
        market.get("clobTokenIds")
        or market.get("clob_token_ids")
        or market.get("tokenIds")
        or market.get("tokens"),
    )
    outcomes = _coerce_list(market.get("outcomes") or market.get("shortOutcomes"))

    contracts: list[BinaryContract] = []
    for index, token in enumerate(token_ids):
        token_id = _token_id(token)
        if not token_id:
            continue

        outcome = _outcome_name(token, outcomes, index)
        contracts.append(
            BinaryContract(
                id=polymarket_contract_id(condition_id, token_id),
                market_id=MarketID(condition_id),
                outcome_id=polymarket_outcome_id(condition_id, outcome, token_id),
                venue_id=POLYMARKET_VENUE_ID,
                payout_currency=Currency("pUSD"),
                payout_if_true=Payout(Decimal("1")),
                payout_if_false=Payout(Decimal("0")),
                symbol=token_id,
                tick_size=_optional_tick_size(market),
                lot_size=_QUANTITY_STEP,
                minimum_order_size=_optional_minimum_order_size(market),
            ),
        )

    return tuple(contracts)


def gamma_market_to_market(market: dict[str, Any]) -> Market | None:
    """
    Map a Gamma market payload, returning ``None`` when it lacks two outcomes.

    Parameters
    ----------
    market
        Raw Gamma market payload.

    Returns
    -------
    Market | None
        Normalized market with state and resolution metadata, or ``None`` if incomplete.
    """
    condition_id = _condition_id(market)
    if not condition_id:
        return None

    contracts = gamma_market_to_contracts(market)
    if len(contracts) < 2:
        return None

    yes_contract = _find_contract_by_outcome(contracts, "yes") or contracts[0]
    no_contract = _find_contract_by_outcome(contracts, "no") or contracts[1]

    return Market(
        id=MarketID(condition_id),
        venue_id=POLYMARKET_VENUE_ID,
        title=str(market.get("question") or market.get("title") or condition_id),
        state=_market_state(market),
        yes_side=MarketSide(
            id=yes_contract.outcome_id,
            side=BinaryOutcome.YES,
        ),
        no_side=MarketSide(
            id=no_contract.outcome_id,
            side=BinaryOutcome.NO,
        ),
        description=market.get("description") or None,
        resolution=_market_resolution(market),
        category=market.get("category") or None,
    )


def clob_book_to_order_book(
    raw_book: dict[str, Any],
    *,
    contract: BinaryContract | None = None,
    contract_id: ContractID | None = None) -> OrderBook:
    """
    Map a CLOB book while preserving Decimal prices and venue timestamps.

    Parameters
    ----------
    raw_book
        CLOB payload containing bid/ask levels and an optional epoch timestamp.
    contract
        Known domain contract, preferred when available.
    contract_id
        Contract id used when the full contract entity is unavailable.

    Returns
    -------
    OrderBook
        Normalized order book with sorted bids and asks.
    """

    if contract is None and contract_id is None:
        raise ValueError("Either contract or contract_id must be provided")

    effective_contract_id = contract.id if contract else contract_id
    condition_id, token_id = parse_polymarket_contract_id(effective_contract_id)

    bids = tuple(
        sorted(
            (_order_book_level(level) for level in raw_book.get("bids", [])),
            key=lambda level: level.price.value,
            reverse=True,
        ),
    )
    asks = tuple(
        sorted(
            (_order_book_level(level) for level in raw_book.get("asks", [])),
            key=lambda level: level.price.value,
        ),
    )

    return OrderBook(
        market_id=contract.market_id if contract else MarketID(condition_id),
        outcome_id=contract.outcome_id if contract else OutcomeID(f"{condition_id}:{token_id}"),
        bids=bids,
        asks=asks,
        timestamp=_timestamp_from_epoch(raw_book.get("timestamp")),
        source_hash=(str(raw_book["hash"]) if raw_book.get("hash") else None),
    )


def _coerce_list(value: Any) -> list[Any]:
    """Normalize list-like JSON fields encoded as arrays or JSON strings."""
    if value is None:
        return []
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return [value]
        return parsed if isinstance(parsed, list) else [parsed]
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    return [value]


def _condition_id(market: dict[str, Any]) -> str:
    return str(market.get("conditionId") or market.get("condition_id") or "")


def _token_id(token: Any) -> str | None:
    if isinstance(token, dict):
        value = token.get("token_id") or token.get("tokenId") or token.get("id")
        return str(value) if value else None
    return str(token) if token else None


def _outcome_name(token: Any, outcomes: list[Any], index: int) -> str:
    """Resolve an outcome label across alternate external field names."""
    if isinstance(token, dict):
        outcome = token.get("outcome")
        if outcome:
            return str(outcome)

    if index < len(outcomes):
        outcome = outcomes[index]
        if isinstance(outcome, dict):
            return str(outcome.get("name") or outcome.get("outcome") or index)
        return str(outcome)

    return str(index)


def _find_contract_by_outcome(
    contracts: tuple[BinaryContract, ...],
    outcome: str,
) -> BinaryContract | None:
    """Find the contract matching a normalized outcome label."""
    needle = f":{outcome.lower()}:"
    for contract in contracts:
        if needle in str(contract.outcome_id).lower():
            return contract
    return None


def _market_state(market: dict[str, Any]) -> MarketState:
    """Derive normalized lifecycle state and timestamps from venue flags."""
    if _bool_field(market, "closed") and _resolved_outcome(market) is not None:
        status = MarketStatus.RESOLVED
    elif _bool_field(market, "closed"):
        status = MarketStatus.CLOSED
    elif _bool_field(market, "active"):
        status = MarketStatus.ACTIVE
    elif _bool_field(market, "archived"):
        status = MarketStatus.CLOSED
    else:
        status = MarketStatus.UNKNOWN

    return MarketState(
        status=status,
        start_time=_timestamp_from_iso(
            market.get("eventStartTime")
            or market.get("startDate")
            or market.get("start_date")
        ),
        close_time=_timestamp_from_iso(market.get("endDate") or market.get("endDateIso")),
        resolved_time=_timestamp_from_iso(market.get("closedTime")),
    )


def _market_resolution(market: dict[str, Any]) -> MarketResolution | None:
    """Build resolution metadata only when the payload provides it."""
    resolved = _resolved_outcome(market)
    rules = market.get("description") or market.get("rules") or None
    source = market.get("resolutionSource") or None
    if resolved is None and rules is None and source is None:
        return None

    condition_id = _condition_id(market)
    winning_contract = (
        _find_contract_by_outcome(gamma_market_to_contracts(market), resolved)
        if resolved is not None
        else None
    )
    return MarketResolution(
        rules=rules,
        source=source,
        resolved_outcome_id=(
            winning_contract.outcome_id
            if winning_contract
            else (
                polymarket_outcome_id(condition_id, resolved, resolved)
                if resolved is not None
                else None
            )
        ),
        resolved_at=_timestamp_from_iso(market.get("closedTime")),
    )


def _resolved_outcome(market: dict[str, Any]) -> str | None:
    """Resolve the winning outcome id from venue resolution fields."""
    outcome = market.get("outcome") or market.get("resolution")
    if outcome:
        return str(outcome)

    tokens = _coerce_list(market.get("tokens"))
    for token in tokens:
        if isinstance(token, dict) and token.get("winner") is True:
            token_outcome = token.get("outcome")
            return str(token_outcome) if token_outcome else None
    return None


def _bool_field(market: dict[str, Any], field: str) -> bool:
    value = market.get(field)
    if isinstance(value, str):
        return value.lower() == "true"
    return bool(value)


def _timestamp_from_iso(value: Any) -> Timestamp | None:
    """Parse an optional ISO timestamp into a domain timestamp."""
    if not value:
        return None
    try:
        raw = str(value).replace("Z", "+00:00")
        parsed = datetime.fromisoformat(raw)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return Timestamp(parsed)


def _optional_tick_size(market: dict[str, Any]) -> TickSize | None:
    value = (
        market.get("orderPriceMinTickSize")
        or market.get("minimum_tick_size")
        or market.get("minimumTickSize")
        or market.get("min_tick_size")
    )
    return TickSize(Decimal(str(value))) if value is not None else None


def _optional_minimum_order_size(market: dict[str, Any]) -> Quantity | None:
    value = market.get("order_min_size") or market.get("orderMinSize") or market.get("min_size")
    return Quantity(Decimal(str(value))) if value is not None else None


def _order_book_level(raw: dict[str, Any]) -> OrderBookLevel:
    return OrderBookLevel(
        price=Price(Decimal(str(raw["price"]))),
        quantity=Quantity(Decimal(str(raw["size"]))),
    )


def _timestamp_from_epoch(value: Any) -> Timestamp | None:
    """Normalize second or millisecond epoch values into timestamps."""
    if value is None:
        return None

    epoch = Decimal(str(value))
    if epoch <= 0:
        return None

    if epoch > Decimal("1000000000000000"):
        seconds = epoch / Decimal("1000000000")
    elif epoch > Decimal("10000000000"):
        seconds = epoch / Decimal("1000")
    else:
        seconds = epoch

    return Timestamp(datetime.fromtimestamp(float(seconds), tz=timezone.utc))
