"""Verify shared long and short exposure-recovery economics."""

from decimal import Decimal

import pytest

from prediction_markets.domain.shared.value_objects import (
    Currency,
    Money,
    Price,
    Quantity,
)
from prediction_markets.domain.trading.enums import OrderSide, RecoveryRoute
from prediction_markets.domain.trading.value_objects import evaluate_recovery_economics


@pytest.mark.parametrize(
    ("source_side", "route", "recovery_price", "recovery_side", "gross"),
    (
        (
            OrderSide.BUY,
            RecoveryRoute.COMPLETE_MISSING_LEG,
            "0.65",
            OrderSide.BUY,
            "0.10",
        ),
        (
            OrderSide.BUY,
            RecoveryRoute.UNWIND_EXCESS,
            "0.28",
            OrderSide.SELL,
            "-0.04",
        ),
        (
            OrderSide.SELL,
            RecoveryRoute.COMPLETE_MISSING_LEG,
            "0.75",
            OrderSide.SELL,
            "0.10",
        ),
        (
            OrderSide.SELL,
            RecoveryRoute.UNWIND_EXCESS,
            "0.32",
            OrderSide.BUY,
            "-0.04",
        ),
    ),
)
def test_recovery_economics_cover_long_and_short_routes(
    source_side: OrderSide,
    route: RecoveryRoute,
    recovery_price: str,
    recovery_side: OrderSide,
    gross: str,
) -> None:
    """Use one cashflow rule for completing and unwinding long and short fills."""
    usd = Currency("USD")

    result = evaluate_recovery_economics(
        source_side=source_side,
        source_price=Price(Decimal("0.30")),
        quantity=Quantity(Decimal("2")),
        route=route,
        recovery_price=Price(Decimal(recovery_price)),
        source_fee=Money(Decimal("0.01"), usd),
        recovery_fee=Money(Decimal("0.02"), usd),
    )

    assert result.recovery_side is recovery_side
    assert result.gross_result == Decimal(gross)
    assert result.total_fees == Money(Decimal("0.03"), usd)
    assert result.net_result == Decimal(gross) - Decimal("0.03")


def test_recovery_economics_do_not_invent_missing_fees() -> None:
    """Keep the net result unknown until both settlement fees are available."""
    result = evaluate_recovery_economics(
        source_side=OrderSide.BUY,
        source_price=Price(Decimal("0.30")),
        quantity=Quantity(Decimal("2")),
        route=RecoveryRoute.UNWIND_EXCESS,
        recovery_price=Price(Decimal("0.28")),
        source_fee=None,
        recovery_fee=Money(Decimal("0.02"), Currency("USD")),
    )

    assert result.gross_result == Decimal("-0.04")
    assert result.total_fees is None
    assert result.net_result is None
