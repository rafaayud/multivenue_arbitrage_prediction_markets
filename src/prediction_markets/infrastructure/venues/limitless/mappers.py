"""Translate limitless payloads into domain models.

Responsibilities
----------------
- Normalize external identifiers, prices, states, and outcomes.
"""

from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from prediction_markets.domain.contracts.entities import BinaryContract
from prediction_markets.domain.contracts.value_objects import LotSize, Payout, TickSize
from prediction_markets.domain.markets.entities import Market, MarketSide
from prediction_markets.domain.markets.enums import BinaryOutcome, MarketStatus
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

LIMITLESS_VENUE_ID = VenueID("LIMITLESS")
LIMITLESS_YES_OUTCOME = "yes"
LIMITLESS_NO_OUTCOME = "no"
_SIZE_SCALE = Decimal("1000000")
_PRICE_TICK = TickSize(Decimal("0.001"))
_QUANTITY_STEP = LotSize(Decimal("0.000001"))


def limitless_contract_id(slug: str, outcome: str) -> ContractID:
    """
    Build the stable domain id used to address one Limitless outcome.

    Parameters
    ----------
    slug
        Limitless market slug.
    outcome
        ``yes`` or ``no`` outcome label.

    Returns
    -------
    ContractID
        Domain contract id with a normalized lowercase outcome.
    """
    return ContractID(f"limitless:{slug}:{_normalize_outcome(outcome)}")


def parse_limitless_contract_id(contract_id: ContractID) -> tuple[str, str]:
    """
    Parse a domain id into the Limitless market slug and normalized outcome.

    Parameters
    ----------
    contract_id
        Domain id produced by ``limitless_contract_id``.

    Returns
    -------
    tuple[str, str]
        ``(slug, outcome)`` for SDK market and token lookup.
    """
    value = str(contract_id)
    if not value.startswith("limitless:"):
        raise ValueError(f"Invalid Limitless contract ID: {value}")

    _, slug, outcome = value.split(":", maxsplit=2)
    if not slug:
        raise ValueError(f"Invalid Limitless contract ID: {value}")
    return slug, _normalize_outcome(outcome)


def limitless_outcome_id(slug: str, outcome: str, token_id: str) -> OutcomeID:
    """
    Build an outcome id retaining the venue token id for protocol translation.

    Parameters
    ----------
    slug
        Limitless market slug.
    outcome
        Binary outcome label.
    token_id
        Venue token identifier.

    Returns
    -------
    OutcomeID
        Domain outcome id preserving all protocol addressing information.
    """
    return OutcomeID(f"{slug}:{_normalize_outcome(outcome)}:{token_id}")


def limitless_market_to_contracts(market: dict[str, Any]) -> tuple[BinaryContract, ...]:
    """Map a public Limitless CLOB market payload to its YES and NO contracts.

    Notes
    -----
    - ``metadata.minSize`` controls LP reward eligibility, not order validity.
    """
    slug = str(market.get("slug") or "")
    if not slug:
        return ()

    tokens = market.get("tokens")
    if not isinstance(tokens, dict):
        return ()

    yes_token = _token_id(tokens.get(LIMITLESS_YES_OUTCOME))
    no_token = _token_id(tokens.get(LIMITLESS_NO_OUTCOME))
    if yes_token is None or no_token is None:
        return ()

    return (
        _limitless_contract(
            slug=slug,
            outcome=LIMITLESS_YES_OUTCOME,
            token_id=yes_token,
        ),
        _limitless_contract(
            slug=slug,
            outcome=LIMITLESS_NO_OUTCOME,
            token_id=no_token,
        ),
    )


def limitless_market_to_market(market: dict[str, Any]) -> Market | None:
    """Map a Limitless CLOB market payload, including its trading window."""
    slug = str(market.get("slug") or "")
    contracts = limitless_market_to_contracts(market)
    if not slug or len(contracts) != 2:
        return None

    yes_contract = next(contract for contract in contracts if ":yes:" in str(contract.outcome_id))
    no_contract = next(contract for contract in contracts if ":no:" in str(contract.outcome_id))
    return Market(
        id=MarketID(slug),
        venue_id=LIMITLESS_VENUE_ID,
        title=str(market.get("title") or slug),
        state=MarketState(
            status=_market_status(market),
            start_time=_timestamp_from_any(market.get("startAt")),
            close_time=_timestamp_from_any(market.get("expirationTimestamp")),
        ),
        yes_side=MarketSide(yes_contract.outcome_id, BinaryOutcome.YES),
        no_side=MarketSide(no_contract.outcome_id, BinaryOutcome.NO),
        description=market.get("description") or None,
        category=_category(market),
    )


def limitless_orderbook_to_order_book(
    raw_book: dict[str, Any],
    *,
    contract: BinaryContract | None = None,
    contract_id: ContractID | None = None,
) -> OrderBook:
    """
    Map Limitless's market-level CLOB book to a single outcome order book.

    Notes
    -----
    - Limitless publishes the YES order book for a binary market. The NO book is its complementary view: a YES ask at p is a NO bid at 1 - p, and vice versa.
    """
    if contract is None and contract_id is None:
        raise ValueError("Either contract or contract_id must be provided")

    effective_contract_id = contract.id if contract else contract_id
    slug, outcome = parse_limitless_contract_id(effective_contract_id)
    book = _book_payload(raw_book)
    yes_bids = _levels_from_book(book, "bids")
    yes_asks = _levels_from_book(book, "asks")

    if outcome == LIMITLESS_YES_OUTCOME:
        bids = yes_bids
        asks = yes_asks
    else:
        bids = _opposite_levels_to_bids(yes_asks)
        asks = _opposite_levels_to_asks(yes_bids)

    return OrderBook(
        market_id=contract.market_id if contract else MarketID(slug),
        outcome_id=(
            contract.outcome_id
            if contract
            else OutcomeID(f"{slug}:{outcome}")
        ),
        bids=tuple(sorted(bids, key=lambda level: level.price.value, reverse=True)),
        asks=tuple(sorted(asks, key=lambda level: level.price.value)),
        timestamp=_timestamp_from_any(book.get("timestamp") or raw_book.get("timestamp")),
    )


def _limitless_contract(
    *,
    slug: str,
    outcome: str,
    token_id: str,
) -> BinaryContract:
    return BinaryContract(
        id=limitless_contract_id(slug, outcome),
        market_id=MarketID(slug),
        outcome_id=limitless_outcome_id(slug, outcome, token_id),
        venue_id=LIMITLESS_VENUE_ID,
        payout_currency=Currency("USDC"),
        payout_if_true=Payout(Decimal("1")),
        payout_if_false=Payout(Decimal("0")),
        symbol=token_id,
        tick_size=_PRICE_TICK,
        lot_size=_QUANTITY_STEP,
    )


def _normalize_outcome(outcome: str) -> str:
    normalized = outcome.strip().lower()
    if normalized not in {LIMITLESS_YES_OUTCOME, LIMITLESS_NO_OUTCOME}:
        raise ValueError("Limitless outcome must be 'yes' or 'no'")
    return normalized


def _token_id(value: Any) -> str | None:
    if value is None:
        return None
    token_id = str(value).strip()
    return token_id or None


def _market_status(market: dict[str, Any]) -> MarketStatus:
    """Map venue-specific lifecycle fields to a normalized market status."""
    if market.get("expired"):
        return MarketStatus.CLOSED
    if str(market.get("status") or "").upper() in {"ACTIVE", "FUNDED", "OPEN"}:
        return MarketStatus.ACTIVE
    return MarketStatus.UNKNOWN


def _category(market: dict[str, Any]) -> str | None:
    categories = market.get("categories")
    return str(categories[0]) if isinstance(categories, list) and categories else None


def _book_payload(raw_book: dict[str, Any]) -> dict[str, Any]:
    payload = raw_book.get("orderbook") or raw_book.get("book") or raw_book
    if not isinstance(payload, dict):
        raise TypeError(f"Unexpected Limitless orderbook payload: {type(payload).__name__}")
    return payload


def _levels_from_book(book: dict[str, Any], side: str) -> tuple[OrderBookLevel, ...]:
    """Normalize Kalshi price levels, including complementary YES/NO prices."""
    raw_levels = book.get(side) or []
    if not isinstance(raw_levels, list):
        raise TypeError(f"Limitless orderbook {side} must be a list")
    return tuple(_order_book_level(level) for level in raw_levels)


def _order_book_level(raw: Any) -> OrderBookLevel:
    if isinstance(raw, dict):
        price = raw.get("price")
        size = raw.get("size")
    else:
        price, size = raw

    return OrderBookLevel(
        price=Price(Decimal(str(price))),
        quantity=Quantity(Decimal(str(size)) / _SIZE_SCALE),
    )


def _opposite_levels_to_bids(levels: tuple[OrderBookLevel, ...]) -> tuple[OrderBookLevel, ...]:
    return tuple(
        OrderBookLevel(
            price=Price(Decimal("1") - level.price.value),
            quantity=level.quantity,
        )
        for level in levels
    )


def _opposite_levels_to_asks(levels: tuple[OrderBookLevel, ...]) -> tuple[OrderBookLevel, ...]:
    return _opposite_levels_to_bids(levels)


def _timestamp_from_any(value: Any) -> Timestamp | None:
    """Normalize numeric, text, and datetime timestamp variants."""
    if value is None:
        return None

    if isinstance(value, str) and not value.replace(".", "", 1).isdigit():
        return Timestamp(datetime.fromisoformat(value.replace("Z", "+00:00")))

    epoch = Decimal(str(value))
    if epoch <= 0:
        return None
    if epoch > Decimal("1000000000000000"):
        epoch /= Decimal("1000000000")
    elif epoch > Decimal("10000000000"):
        epoch /= Decimal("1000")
    return Timestamp(datetime.fromtimestamp(float(epoch), tz=timezone.utc))
