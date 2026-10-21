"""Read recorded inventory transaction costs independently of position balances.

Notes
-----
- Repeated observations of the same venue operation count once.
- Legacy audit rows lack status and portfolio fields; only rows with recorded
  economics qualify, and their portfolio defaults to the bot strategy.
"""

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any

from prediction_markets.domain.trading.portfolio import STRATEGY_PORTFOLIO_ID


@dataclass(frozen=True, slots=True)
class InventoryCost:
    """Represent an immutable recorded cost identified by venue and operation.

    Attributes
    ----------
    observed_at
        Time of the first recorded economic observation.
    amount_usd
        Latest known USD charge, or ``None`` while conversion is unavailable.
    """

    operation_id: str
    venue_id: str
    observed_at: datetime
    amount_usd: Decimal | None


def read_inventory_costs(connection: Any) -> tuple[InventoryCost, ...]:
    """Read each bot inventory operation's latest economic observation.

    Parameters
    ----------
    connection
        Open PostgreSQL connection or cursor holding the projected event audit.

    Returns
    -------
    tuple[InventoryCost, ...]
        Costs in first-observed order. Unknown USD costs remain ``None``.

    Notes
    -----
    Reads the event audit without writing. Prepared and pending operations are
    excluded; failed operations with recorded fees still incur a cost. A later
    observation without a USD conversion cannot erase a recorded USD charge.
    """
    rows = connection.execute(
        """
        SELECT recorded_at, summary
        FROM projected_events
        WHERE event_type = 'InventoryOperationRecorded'
          AND COALESCE(summary->>'portfolio_id', %s) = %s
        ORDER BY journal_sequence
        """,
        (str(STRATEGY_PORTFOLIO_ID), str(STRATEGY_PORTFOLIO_ID)),
    ).fetchall()
    costs: dict[tuple[str, str], InventoryCost] = {}
    for observed_at, summary in rows:
        status = summary.get("snapshot_status")
        if status is not None and status not in {"confirmed", "failed"}:
            continue
        if not any(
            summary.get(key) is not None
            for key in ("fee", "fee_settlement_cost", "collateral", "payout")
        ) and status != "confirmed":
            continue
        operation_id = summary.get("operation_id")
        venue_id = summary.get("venue_id")
        if not operation_id or not venue_id:
            continue
        key = (venue_id, operation_id)
        fee = summary.get("fee_settlement_cost")
        previous = costs.get(key)
        costs[key] = InventoryCost(
            operation_id=operation_id,
            venue_id=venue_id,
            observed_at=previous.observed_at if previous else observed_at,
            amount_usd=(
                Decimal(fee["amount"])
                if fee is not None and fee.get("currency") == "USD"
                else previous.amount_usd if previous else None
            ),
        )
    return tuple(costs.values())
