"""Translate polynode payloads into domain models.

Responsibilities
----------------
- Normalize external identifiers, prices, states, and outcomes.
"""

from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from prediction_markets.domain.contracts.entities import BinaryContract
from prediction_markets.domain.orderbook.entities import OrderBook
from prediction_markets.domain.orderbook.value_objects import OrderBookLevel
from prediction_markets.domain.shared.value_objects import ContractID, MarketID, OutcomeID, Price, Quantity, Timestamp


def polynode_orderbook_to_order_book(
    snapshot: dict[str, Any],
    *,
    contract: BinaryContract | None = None,
    contract_id: ContractID | None = None,
) -> OrderBook:
    """Map Polynode's snapshot payload, not Polymarket CLOB's payload."""
    if contract is None and contract_id is None:
        raise ValueError("Either contract or contract_id must be provided")

    token_id = str(snapshot.get("asset_id") or "")
    market_id = contract.market_id if contract else MarketID(token_id)
    outcome_id = contract.outcome_id if contract else OutcomeID(token_id)
    return OrderBook(
        market_id=market_id,
        outcome_id=outcome_id,
        bids=_levels(snapshot, "bids", reverse=True),
        asks=_levels(snapshot, "asks"),
        timestamp=_timestamp(snapshot.get("ts") or snapshot.get("timestamp")),
    )


def _levels(snapshot: dict[str, Any], side: str, *, reverse: bool = False) -> tuple[OrderBookLevel, ...]:
    """Normalize external price levels and discard malformed entries."""
    raw_levels = snapshot.get(side, [])
    if not isinstance(raw_levels, list):
        raise TypeError(f"Polynode snapshot {side} must be a list")
    return tuple(
        sorted(
            (_level(level) for level in raw_levels),
            key=lambda level: level.price.value,
            reverse=reverse,
        ),
    )


def _level(raw: Any) -> OrderBookLevel:
    if not isinstance(raw, dict):
        raise TypeError("Polynode orderbook level must be an object")
    return OrderBookLevel(
        price=Price(Decimal(str(raw["price"]))),
        quantity=Quantity(Decimal(str(raw["size"]))),
    )


def _timestamp(value: Any) -> Timestamp | None:
    """Normalize a supported external timestamp into a domain timestamp."""
    if value is None:
        return None
    epoch = Decimal(str(value))
    if epoch <= 0:
        return None
    if epoch > Decimal("1000000000000000"):
        epoch /= Decimal("1000000000")
    elif epoch > Decimal("10000000000"):
        epoch /= Decimal("1000")
    return Timestamp(datetime.fromtimestamp(float(epoch), tz=timezone.utc))
