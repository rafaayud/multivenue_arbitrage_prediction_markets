"""Exercise limitless taker fees behavior in the infrastructure limitless layer.

Responsibilities
----------------
- Verify limitless taker fees contracts, edge cases, and failure handling.
"""

import asyncio
from decimal import Decimal

from prediction_markets.domain.shared.value_objects import (
    ContractID,
    Currency,
    Money,
    Price,
    Quantity,
)
from prediction_markets.domain.trading.enums import OrderSide
from prediction_markets.infrastructure.venues.limitless.taker_fees import (
    LimitlessTakerFeeCalculator,
)


class _ProfileClient:
    """Provide a controllable profile client test double."""
    async def get(self, path):
        assert path == "/profiles/0xabc"
        return {"rank": {"feeRateBps": 300}}


def test_loads_profile_rate_and_applies_limitless_buy_curve():
    async def run_test():
        calculator = LimitlessTakerFeeCalculator(
            wallet_address="0xabc",
            http_client=_ProfileClient(),
        )
        contract_id = ContractID("limitless:btc-up-or-down:no")
        await calculator.prepare((contract_id,))

        low_price = calculator.calculate(
            contract_id,
            Price(Decimal("0.40")),
            Quantity(Decimal("100")),
            OrderSide.BUY,
        )
        high_price = calculator.calculate(
            contract_id,
            Price(Decimal("0.60")),
            Quantity(Decimal("100")),
            OrderSide.BUY,
        )
        sell = calculator.calculate(
            contract_id,
            Price(Decimal("0.50")),
            Quantity(Decimal("100")),
            OrderSide.SELL,
        )

        assert low_price.charged == Money(
            Decimal("3.000000"),
            Currency("OUTCOME_TOKEN"),
        )
        assert low_price.settlement_cost == Money(
            Decimal("3.000000"),
            Currency("USD"),
        )
        assert high_price.settlement_cost.amount == Decimal("2.133333")
        assert sell.charged.currency == Currency("USDC")
        assert sell.settlement_cost.currency == Currency("USD")

    asyncio.run(run_test())
