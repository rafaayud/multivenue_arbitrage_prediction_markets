"""Translate kalshi payloads into domain models.

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

KALSHI_VENUE_ID = VenueID("KALSHI")
KALSHI_YES_OUTCOME = "yes"
KALSHI_NO_OUTCOME = "no"
_QUANTITY_STEP = LotSize(Decimal("1"))


def kalshi_contract_id(ticker: str, outcome: str) -> ContractID:
    return ContractID(f"kalshi:{ticker}:{_normalize_outcome(outcome)}")


def parse_kalshi_contract_id(contract_id: ContractID) -> tuple[str, str]:
    """Translate parse kalshi contract id data between Kalshi and domain formats."""
    value = str(contract_id)
    if value.startswith("kalshi:"):
        _, ticker, outcome = value.split(":", maxsplit=2)
        return ticker, _normalize_outcome(outcome)

    # Backward compatibility with simple Nautilus-style IDs:
    # "{ticker}-{outcome}.KALSHI".
    symbol = value.removesuffix(".KALSHI")
    if "-" in symbol:
        ticker, outcome = symbol.rsplit("-", maxsplit=1)
        return ticker, _normalize_outcome(outcome)

    raise ValueError(f"Invalid Kalshi contract ID: {value}")


def kalshi_outcome_id(ticker: str, outcome: str) -> OutcomeID:
    return OutcomeID(f"{ticker}:{_normalize_outcome(outcome)}")


def kalshi_market_to_contracts(market: dict[str, Any]) -> tuple[BinaryContract, ...]:
    """Translate kalshi market to contracts data between Kalshi and domain formats."""
    ticker = str(market.get("ticker") or market.get("market_ticker") or "")
    if not ticker:
        return ()

    tick_size = _optional_tick_size(market)
    lot_size = _optional_lot_size(market)
    minimum_order_size = _optional_minimum_order_size(market)

    return (
        _kalshi_contract(
            ticker=ticker,
            outcome=KALSHI_YES_OUTCOME,
            symbol=ticker,
            tick_size=tick_size,
            lot_size=lot_size,
            minimum_order_size=minimum_order_size,
        ),
        _kalshi_contract(
            ticker=ticker,
            outcome=KALSHI_NO_OUTCOME,
            symbol=f"{ticker}-NO",
            tick_size=tick_size,
            lot_size=lot_size,
            minimum_order_size=minimum_order_size,
        ),
    )


def kalshi_market_to_market(market: dict[str, Any]) -> Market | None:
    """Translate kalshi market to market data between Kalshi and domain formats."""
    ticker = str(market.get("ticker") or market.get("market_ticker") or "")
    contracts = kalshi_market_to_contracts(market)
    if not ticker or len(contracts) != 2:
        return None

    return Market(
        id=MarketID(ticker),
        venue_id=KALSHI_VENUE_ID,
        title=str(market.get("title") or market.get("subtitle") or ticker),
        state=MarketState(
            status=_market_status(market),
            start_time=_timestamp_from_any(market.get("open_time")),
            close_time=_timestamp_from_any(market.get("close_time")),
        ),
        yes_side=MarketSide(contracts[0].outcome_id, BinaryOutcome.YES),
        no_side=MarketSide(contracts[1].outcome_id, BinaryOutcome.NO),
        description=market.get("rules_primary") or None,
    )


def kalshi_orderbook_to_order_book(
    raw_book: dict[str, Any],
    *,
    contract: BinaryContract | None = None,
    contract_id: ContractID | None = None,
) -> OrderBook:
    """Translate kalshi orderbook to order book data between Kalshi and domain formats."""
    if contract is None and contract_id is None:
        raise ValueError("Either contract or contract_id must be provided")

    effective_contract_id = contract.id if contract else contract_id
    ticker, outcome = parse_kalshi_contract_id(effective_contract_id)

    book = _book_payload(raw_book)
    yes_bids = _levels_from_book(book, "yes")
    no_bids = _levels_from_book(book, "no")

    if outcome == KALSHI_YES_OUTCOME:
        bids = yes_bids
        asks = _opposite_bids_to_asks(no_bids)
    elif outcome == KALSHI_NO_OUTCOME:
        bids = no_bids
        asks = _opposite_bids_to_asks(yes_bids)
    else:
        raise ValueError(f"Unsupported Kalshi outcome: {outcome}")

    return OrderBook(
        market_id=contract.market_id if contract else MarketID(ticker),
        outcome_id=contract.outcome_id if contract else kalshi_outcome_id(ticker, outcome),
        bids=tuple(sorted(bids, key=lambda level: level.price.value, reverse=True)),
        asks=tuple(sorted(asks, key=lambda level: level.price.value)),
        timestamp=_timestamp_from_any(raw_book.get("timestamp") or raw_book.get("ts")),
    )


def _kalshi_contract(
    *,
    ticker: str,
    outcome: str,
    symbol: str,
    tick_size: TickSize | None,
    lot_size: LotSize | None,
    minimum_order_size: Quantity | None,
) -> BinaryContract:
    """Build one validated Kalshi binary contract from a market payload."""
    return BinaryContract(
        id=kalshi_contract_id(ticker, outcome),
        market_id=MarketID(ticker),
        outcome_id=kalshi_outcome_id(ticker, outcome),
        venue_id=KALSHI_VENUE_ID,
        payout_currency=Currency("USD"),
        payout_if_true=Payout(Decimal("1")),
        payout_if_false=Payout(Decimal("0")),
        symbol=symbol,
        tick_size=tick_size,
        lot_size=lot_size,
        minimum_order_size=minimum_order_size,
    )


def _normalize_outcome(outcome: str) -> str:
    normalized = outcome.strip().lower()
    if normalized not in {KALSHI_YES_OUTCOME, KALSHI_NO_OUTCOME}:
        raise ValueError("Kalshi outcome must be 'yes' or 'no'")
    return normalized


def _book_payload(raw_book: dict[str, Any]) -> dict[str, Any]:
    """Select the nested Kalshi order-book payload shape."""
    if "yes_dollars_fp" in raw_book or "no_dollars_fp" in raw_book:
        return raw_book

    payload = (
        raw_book.get("orderbook_fp")
        or raw_book.get("orderbook")
        or raw_book.get("book")
        or raw_book
    )
    if not isinstance(payload, dict):
        raise TypeError(f"Unexpected Kalshi orderbook payload: {type(payload).__name__}")
    return payload


def _levels_from_book(book: dict[str, Any], side: str) -> tuple[OrderBookLevel, ...]:
    """Normalize Kalshi price levels, including complementary YES/NO prices."""
    dollar_levels = book.get(f"{side}_dollars") or book.get(f"{side}_dollars_fp")
    if dollar_levels is not None:
        return tuple(_order_book_level(level, price_scale=Decimal("1")) for level in dollar_levels)

    cent_levels = book.get(side)
    if cent_levels is not None:
        return tuple(_order_book_level(level, price_scale=Decimal("100")) for level in cent_levels)

    return ()


def _opposite_bids_to_asks(levels: tuple[OrderBookLevel, ...]) -> tuple[OrderBookLevel, ...]:
    return tuple(
        OrderBookLevel(
            price=Price(Decimal("1") - level.price.value),
            quantity=level.quantity,
        )
        for level in levels
    )


def _order_book_level(raw: Any, *, price_scale: Decimal) -> OrderBookLevel:
    if isinstance(raw, dict):
        raw_price = raw.get("price") or raw.get("price_dollars")
        raw_size = raw.get("size") or raw.get("quantity") or raw.get("count")
    else:
        raw_price = raw[0]
        raw_size = raw[1]

    return OrderBookLevel(
        price=Price(Decimal(str(raw_price)) / price_scale),
        quantity=Quantity(Decimal(str(raw_size))),
    )


def _optional_tick_size(market: dict[str, Any]) -> TickSize | None:
    """Parse a positive optional venue tick size."""
    value = market.get("tick_size") or market.get("minimum_tick_size")
    if value is not None:
        return TickSize(_decimal_price(value))

    price_ranges = market.get("price_ranges")
    if isinstance(price_ranges, list) and price_ranges:
        step = price_ranges[0].get("step")
        if step is not None:
            return TickSize(_decimal_price(step))

    return None


def _optional_lot_size(market: dict[str, Any]) -> LotSize | None:
    value = market.get("lot_size")
    return LotSize(Decimal(str(value))) if value is not None else _QUANTITY_STEP


def _optional_minimum_order_size(market: dict[str, Any]) -> Quantity | None:
    value = market.get("min_order_size")
    return Quantity(Decimal(str(value))) if value is not None else None


def _decimal_price(value: Any) -> Decimal:
    decimal_value = Decimal(str(value))
    if decimal_value > 1:
        return decimal_value / Decimal("100")
    return decimal_value


def _timestamp_from_any(value: Any) -> Timestamp | None:
    """Normalize numeric, text, and datetime timestamp variants."""
    if value is None:
        return None

    if isinstance(value, str) and not value.replace(".", "", 1).isdigit():
        normalized = value.replace("Z", "+00:00")
        return Timestamp(datetime.fromisoformat(normalized))

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


def _market_status(market: dict[str, Any]) -> MarketStatus:
    """Map venue-specific lifecycle fields to a normalized market status."""
    status = str(market.get("status") or "").lower()
    if status == "open":
        return MarketStatus.ACTIVE
    if status == "paused":
        return MarketStatus.SUSPENDED
    if status == "settled":
        return MarketStatus.RESOLVED
    if status == "closed":
        return MarketStatus.CLOSED
    return MarketStatus.UNKNOWN
