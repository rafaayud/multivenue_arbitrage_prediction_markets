"""Verify recorded transaction costs survive repeated inventory observations."""

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from unittest.mock import Mock

from prediction_markets.infrastructure.postgres.inventory_costs import read_inventory_costs


def test_inventory_costs_deduplicate_charges_and_preserve_unknown_conversions() -> None:
    """Count each operation once, including failed transactions that paid gas."""
    now = datetime(2026, 10, 21, tzinfo=timezone.utc)
    known = {
        "operation_id": "split-1", "venue_id": "LIMITLESS",
        "snapshot_status": "confirmed",
        "fee_settlement_cost": {"amount": "0.10", "currency": "USD"},
    }
    connection = Mock()
    connection.execute.return_value.fetchall.return_value = [
        (now, {"operation_id": "prepared", "venue_id": "LIMITLESS"}),
        (now, {**known, "snapshot_status": "pending"}),
        (now, known),
        (now + timedelta(seconds=1), known),
        (now + timedelta(seconds=2), {
            **known, "fee_settlement_cost": {"amount": "0.12", "currency": "USD"},
        }),
        (now + timedelta(seconds=3), {**known, "fee_settlement_cost": None}),
        (now, {
            **known, "operation_id": "failed", "snapshot_status": "failed",
            "fee_settlement_cost": {"amount": "0.01", "currency": "USD"},
        }),
        (now, {
            **known, "operation_id": "unknown",
            "fee_settlement_cost": {"amount": "0.2", "currency": "BNB"},
        }),
        (now, {
            **known, "operation_id": "zero",
            "fee_settlement_cost": {"amount": "0", "currency": "USD"},
        }),
        (now, {**known, "operation_id": "missing", "fee_settlement_cost": None}),
        (now, {
            "operation_id": "legacy", "venue_id": "PREDICT",
            "fee_settlement_cost": {"amount": "0.02", "currency": "USD"},
        }),
        (now, {**known, "venue_id": "PREDICT"}),
    ]

    costs = read_inventory_costs(connection)

    assert len(costs) == 7
    assert costs[0].observed_at == now
    assert costs[0].amount_usd == Decimal("0.12")
    assert sum((cost.amount_usd for cost in costs if cost.amount_usd is not None), Decimal("0")) == Decimal("0.25")
    assert sum(cost.amount_usd is None for cost in costs) == 2
    statement, params = connection.execute.call_args.args
    assert "COALESCE(summary->>'portfolio_id', %s) = %s" in statement
    assert params == ("cross-venue-arbitrage", "cross-venue-arbitrage")
