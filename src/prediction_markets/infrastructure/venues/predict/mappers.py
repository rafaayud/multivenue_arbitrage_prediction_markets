"""Translate predict payloads into domain models.

Responsibilities
----------------
- Normalize external identifiers, prices, states, and outcomes.
"""

from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from prediction_markets.domain.contracts.entities import BinaryContract
from prediction_markets.domain.contracts.value_objects import Payout, TickSize
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

PREDICT_VENUE_ID = VenueID("PREDICT")
PREDICT_YES_OUTCOME = "yes"
PREDICT_NO_OUTCOME = "no"


def predict_contract_id(market_id: str | int, outcome: str) -> ContractID:
    """
    Build the stable domain id for one Predict market outcome.

    Parameters
    ----------
    market_id
        Predict market identifier.
    outcome
        ``yes`` or ``no`` label.

    Returns
    -------
    ContractID
        Domain contract id with a normalized outcome.
    """
    return ContractID(f"predict:{market_id}:{_normalize_outcome(outcome)}")


def parse_predict_contract_id(contract_id: ContractID) -> tuple[str, str]:
    """
    Parse a Predict domain id into market id and normalized outcome.

    Parameters
    ----------
    contract_id
        Domain id produced by ``predict_contract_id``.

    Returns
    -------
    tuple[str, str]
        ``(market_id, outcome)`` used by Predict REST requests.
    """
    value = str(contract_id)
    if not value.startswith("predict:"):
        raise ValueError(f"Invalid Predict contract ID: {value}")

    _, market_id, outcome = value.split(":", maxsplit=2)
    if not market_id:
        raise ValueError(f"Invalid Predict contract ID: {value}")
    return market_id, _normalize_outcome(outcome)


def predict_market_to_contracts(
    market: dict[str, Any],
) -> tuple[BinaryContract, ...]:
    """
    Map Predict outcome index sets 1/2 to YES/NO contracts and derive tick size.

    Parameters
    ----------
    market
        Raw Predict market payload containing outcome index sets and precision.

    Returns
    -------
    tuple[BinaryContract, ...]
        Two normalized contracts, or an empty tuple when required outcomes are absent.
    """
    market_id = _market_id(market)
    outcomes = market.get("outcomes")
    if market_id is None or not isinstance(outcomes, list):
        return ()

    by_index = {
        outcome.get("indexSet"): outcome
        for outcome in outcomes
        if isinstance(outcome, dict)
    }
    yes = by_index.get(1)
    no = by_index.get(2)
    if not isinstance(yes, dict) or not isinstance(no, dict):
        return ()
    if not str(yes.get("onChainId") or "").strip() or not str(
        no.get("onChainId") or ""
    ).strip():
        return ()

    precision = int(market.get("decimalPrecision") or 2)
    tick_size = TickSize(Decimal(1).scaleb(-precision))
    return (
        _predict_contract(market_id, PREDICT_YES_OUTCOME, yes, tick_size),
        _predict_contract(market_id, PREDICT_NO_OUTCOME, no, tick_size),
    )


def predict_market_is_active(market: dict[str, Any]) -> bool:
    """Return whether Predict currently reports a market as tradable.

    Parameters
    ----------
    market
        Raw Predict market payload containing lifecycle and trading status.

    Returns
    -------
    bool
        ``True`` only when the normalized market lifecycle is active.

    Notes
    -----
    - A terminal lifecycle overrides a stale ``tradingStatus=OPEN`` value.
    """
    return _market_status(market) is MarketStatus.ACTIVE


def predict_market_to_market(market: dict[str, Any]) -> Market | None:
    """
    Map a Predict market and its CRYPTO_UP_DOWN trading window when present.

    Parameters
    ----------
    market
        Raw Predict market payload.

    Returns
    -------
    Market | None
        Normalized market, or ``None`` when its id or binary outcomes are incomplete.
    """
    market_id = _market_id(market)
    contracts = predict_market_to_contracts(market)
    if market_id is None or len(contracts) != 2:
        return None

    start_time, close_time = predict_market_window(market)

    return Market(
        id=MarketID(market_id),
        venue_id=PREDICT_VENUE_ID,
        title=str(market.get("question") or market.get("title") or market_id),
        state=MarketState(
            status=_market_status(market),
            start_time=start_time,
            close_time=close_time,
        ),
        yes_side=MarketSide(contracts[0].outcome_id, BinaryOutcome.YES),
        no_side=MarketSide(contracts[1].outcome_id, BinaryOutcome.NO),
        description=market.get("description") or None,
        category=market.get("categorySlug") or None,
    )


def predict_orderbook_to_order_book(
    raw_book: dict[str, Any],
    *,
    contract: BinaryContract | None = None,
    contract_id: ContractID | None = None,
) -> OrderBook:
    """
    Map Predict levels, complementing the YES book to represent the NO outcome.

    Parameters
    ----------
    raw_book
        Predict payload containing ``bids``, ``asks``, and update timestamp.
    contract
        Known contract used for exact market and outcome identifiers.
    contract_id
        Contract id used when the full contract entity is unavailable.

    Returns
    -------
    OrderBook
        Normalized sorted order book for the requested outcome.
    """
    if contract is None and contract_id is None:
        raise ValueError("Either contract or contract_id must be provided")

    effective_contract_id = contract.id if contract else contract_id
    market_id, outcome = parse_predict_contract_id(effective_contract_id)
    book = raw_book.get("data") or raw_book
    if not isinstance(book, dict):
        raise TypeError(f"Unexpected Predict orderbook payload: {type(book).__name__}")

    yes_bids = _levels(book.get("bids"))
    yes_asks = _levels(book.get("asks"))
    if outcome == PREDICT_YES_OUTCOME:
        bids, asks = yes_bids, yes_asks
    else:
        bids = _complement(yes_asks)
        asks = _complement(yes_bids)

    return OrderBook(
        market_id=contract.market_id if contract else MarketID(market_id),
        outcome_id=(
            contract.outcome_id
            if contract
            else OutcomeID(f"{market_id}:{outcome}")
        ),
        bids=tuple(sorted(bids, key=lambda level: level.price.value, reverse=True)),
        asks=tuple(sorted(asks, key=lambda level: level.price.value)),
        timestamp=predict_timestamp(book.get("updateTimestampMs")),
    )


def _predict_contract(
    market_id: str,
    outcome: str,
    raw_outcome: dict[str, Any],
    tick_size: TickSize,
) -> BinaryContract:
    """Build one validated Predict contract from a token payload."""
    token_id = str(raw_outcome.get("onChainId") or "").strip()
    if not token_id:
        raise ValueError(f"Predict market {market_id} has no {outcome} token ID")
    return BinaryContract(
        id=predict_contract_id(market_id, outcome),
        market_id=MarketID(market_id),
        outcome_id=OutcomeID(f"{market_id}:{outcome}:{token_id}"),
        venue_id=PREDICT_VENUE_ID,
        payout_currency=Currency("USDT"),
        payout_if_true=Payout(Decimal("1")),
        payout_if_false=Payout(Decimal("0")),
        symbol=token_id,
        tick_size=tick_size,
    )


def _market_id(market: dict[str, Any]) -> str | None:
    value = market.get("id")
    return str(value) if value is not None and str(value).strip() else None


def _normalize_outcome(outcome: str) -> str:
    normalized = outcome.strip().lower()
    if normalized not in {PREDICT_YES_OUTCOME, PREDICT_NO_OUTCOME}:
        raise ValueError("Predict outcome must be 'yes' or 'no'")
    return normalized


def _market_status(market: dict[str, Any]) -> MarketStatus:
    """Map venue-specific lifecycle fields to a normalized market status."""
    lifecycle = str(market.get("status") or "").upper()
    trading = str(market.get("tradingStatus") or "").upper()
    if lifecycle == "RESOLVED":
        return MarketStatus.RESOLVED
    if lifecycle in {"CANCELLED", "REMOVED"}:
        return MarketStatus.CANCELLED
    if trading == "OPEN":
        return MarketStatus.ACTIVE
    if trading in {"MATCHING_NOT_ENABLED", "CANCEL_ONLY"}:
        return MarketStatus.SUSPENDED
    if trading == "CLOSED":
        return MarketStatus.CLOSED
    return MarketStatus.UNKNOWN


def _levels(raw_levels: Any) -> tuple[OrderBookLevel, ...]:
    """Normalize external price levels and discard malformed entries."""
    levels: list[OrderBookLevel] = []
    for raw in raw_levels if isinstance(raw_levels, list) else ():
        if not isinstance(raw, (list, tuple)) or len(raw) < 2:
            continue
        quantity = Quantity(Decimal(str(raw[1])))
        if quantity.value <= 0:
            continue
        levels.append(
            OrderBookLevel(
                price=Price(Decimal(str(raw[0]))),
                quantity=quantity,
            )
        )
    return tuple(levels)


def _complement(
    levels: tuple[OrderBookLevel, ...],
) -> tuple[OrderBookLevel, ...]:
    return tuple(
        OrderBookLevel(
            price=Price(Decimal("1") - level.price.value),
            quantity=level.quantity,
        )
        for level in levels
    )


def predict_timestamp(value: Any) -> Timestamp | None:
    """
    Normalize ISO or millisecond epoch timestamps into timezone-aware domain values.

    Parameters
    ----------
    value
        ISO string, positive epoch value, or ``None``.

    Returns
    -------
    Timestamp | None
        UTC-aware domain timestamp, or ``None`` for absent/non-positive values.
    """
    if value is None:
        return None
    if isinstance(value, str) and not value.replace(".", "", 1).isdigit():
        return Timestamp(datetime.fromisoformat(value.replace("Z", "+00:00")))

    epoch = Decimal(str(value))
    if epoch <= 0:
        return None
    if epoch > Decimal("10000000000"):
        epoch /= Decimal("1000")
    return Timestamp(datetime.fromtimestamp(float(epoch), tz=timezone.utc))


def predict_market_window(
    market: dict[str, Any],
) -> tuple[Timestamp | None, Timestamp | None]:
    """Return the crypto comparison window across Predict payload versions.

    Parameters
    ----------
    market
        Raw Predict market payload.

    Returns
    -------
    tuple[Timestamp | None, Timestamp | None]
        Normalized start and end timestamps when present.
    """
    return (
        predict_timestamp(market.get("boostStartsAt") or market.get("startsAt")),
        predict_timestamp(market.get("boostEndsAt") or market.get("endsAt")),
    )
