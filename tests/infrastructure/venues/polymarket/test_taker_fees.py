"""Exercise taker fees behavior in the infrastructure polymarket layer.

Responsibilities
----------------
- Verify taker fees contracts, edge cases, and failure handling.
"""

import asyncio
from decimal import Decimal

import httpx

from prediction_markets.domain.shared.value_objects import ContractID, Price, Quantity
from prediction_markets.domain.trading.enums import OrderSide
from prediction_markets.infrastructure.venues.polymarket.taker_fees import (
    PolymarketTakerFeeCalculator,
)


def test_loads_market_fee_schedule_and_calculates_taker_fee(
    gamma_market_payload,
    clob_book_payload,
    mock_async_client,
):
    async def run_test():
        market = {
            **gamma_market_payload,
            "feeSchedule": {"rate": 0.07},
        }
        client = mock_async_client(market, clob_book_payload)
        calculator = PolymarketTakerFeeCalculator(
            gamma_base_url="https://example.test",
            client=client,
        )
        contract_id = ContractID("polymarket:0xabc:123")

        try:
            await calculator.prepare((contract_id,))
            fee = calculator.calculate(
                contract_id,
                Price(Decimal("0.50")),
                Quantity(Decimal("2")),
                OrderSide.BUY,
            )
        finally:
            await client.aclose()

        assert fee.charged.amount == Decimal("0.035")
        assert fee.charged.currency.code == "USDC"
        assert fee.settlement_cost.amount == Decimal("0.035")
        assert fee.settlement_cost.currency.code == "USD"

    asyncio.run(run_test())


def test_uses_the_fee_schedule_for_each_market(
    gamma_market_payload,
):
    async def run_test():
        markets = (
            {
                **gamma_market_payload,
                "conditionId": "0xcrypto",
                "feeSchedule": {"rate": "0.07"},
            },
            {
                **gamma_market_payload,
                "conditionId": "0xfinance",
                "feeSchedule": {"rate": "0.04"},
            },
        )

        async def handler(request: httpx.Request) -> httpx.Response:
            assert request.url.path == "/markets"
            assert set(request.url.params.get_list("condition_ids")) == {
                "0xcrypto",
                "0xfinance",
            }
            return httpx.Response(200, json=list(markets))

        client = httpx.AsyncClient(
            transport=httpx.MockTransport(handler),
            base_url="https://example.test",
        )
        calculator = PolymarketTakerFeeCalculator(client=client)
        try:
            await calculator.prepare(
                (
                    ContractID("polymarket:0xcrypto:123"),
                    ContractID("polymarket:0xfinance:456"),
                ),
            )
            crypto = calculator.calculate(
                ContractID("polymarket:0xcrypto:123"),
                Price(Decimal("0.50")),
                Quantity(Decimal("100")),
                OrderSide.BUY,
            )
            finance = calculator.calculate(
                ContractID("polymarket:0xfinance:456"),
                Price(Decimal("0.50")),
                Quantity(Decimal("100")),
                OrderSide.BUY,
            )
        finally:
            await client.aclose()

        assert crypto.settlement_cost.amount == Decimal("1.75")
        assert finance.settlement_cost.amount == Decimal("1.00")

    asyncio.run(run_test())


def test_applies_market_fee_curve_exponent(
    gamma_market_payload,
    clob_book_payload,
    mock_async_client,
):
    async def run_test():
        market = {
            **gamma_market_payload,
            "feeSchedule": {"rate": 0.25, "exponent": 2},
        }
        client = mock_async_client(market, clob_book_payload)
        calculator = PolymarketTakerFeeCalculator(client=client)
        contract_id = ContractID("polymarket:0xabc:123")

        try:
            await calculator.prepare((contract_id,))
            fee = calculator.calculate(
                contract_id,
                Price(Decimal("0.50")),
                Quantity(Decimal("100")),
                OrderSide.BUY,
            )
        finally:
            await client.aclose()

        assert fee.settlement_cost.amount == Decimal("1.5625")

    asyncio.run(run_test())
