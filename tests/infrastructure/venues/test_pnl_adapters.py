import asyncio
from decimal import Decimal

from prediction_markets.infrastructure.venues.limitless.pnl import LimitlessPnlAdapter
from prediction_markets.infrastructure.venues.polymarket.pnl import PolymarketPnlAdapter
from prediction_markets.infrastructure.venues.predict.pnl import PredictPnlAdapter


class _PolymarketClient:
    async def get(self, path, **_kwargs):
        if path == "/positions":
            return [
                {
                    "asset": "token-1",
                    "conditionId": "condition-1",
                    "size": "10",
                    "avgPrice": "0.4",
                    "curPrice": "0.65",
                    "currentValue": "6.5",
                    "cashPnl": "2.5",
                    "realizedPnl": "0.5",
                    "entryFeesUsdc": "0.1",
                    "title": "Test market",
                    "outcome": "Yes",
                }
            ]
        return [{"pnl": "7.25"}]


class _LimitlessClient:
    def __init__(self):
        self.chart_params = None

    async def get(self, path, **kwargs):
        if path == "/portfolio/positions":
            return {
                "clob": [
                    {
                        "market": {
                            "slug": "test-market",
                            "title": "Test market",
                            "status": "RESOLVED",
                        },
                        "tokensBalance": {
                            "yes": "100000000",
                            "no": "100000000",
                        },
                        "positions": {
                            "yes": {
                                "cost": "75000000",
                                "fillPrice": "750000",
                                "realisedPnl": "0",
                                "unrealizedPnl": "25000000",
                                "marketValue": "100000000",
                            },
                            "no": {
                                "cost": "25000000",
                                "fillPrice": "250000",
                                "realisedPnl": "0",
                                "unrealizedPnl": "-5000000",
                                "marketValue": "200000000",
                            },
                        }
                    }
                ],
                "amm": [],
                "group": [],
            }
        self.chart_params = kwargs.get("params")
        return {"currentValue": "7.5"}


class _PredictClient:
    async def get(self, _path, **kwargs):
        pnl = "4.5" if kwargs["params"]["isResolved"] == "true" else "-1.25"
        return {
            "success": True,
            "cursor": None,
            "data": [
                {
                    "id": f"position-{kwargs['params']['isResolved']}",
                    "market": {"id": 42, "title": "Test market"},
                    "outcome": {"indexSet": 1, "name": "Yes"},
                    "amount": "10000000000000000000",
                    "valueUsd": "6.5",
                    "averageBuyPriceUsd": "0.6",
                    "pnlUsd": pnl,
                }
            ],
        }


def test_venue_pnl_adapters_normalize_official_payloads() -> None:
    async def run() -> None:
        limitless_client = _LimitlessClient()
        polymarket, limitless, predict = await asyncio.gather(
            PolymarketPnlAdapter(
                wallet="0x0000000000000000000000000000000000000001",
                client=_PolymarketClient(),
            ).fetch(),
            LimitlessPnlAdapter(client=limitless_client).fetch(),
            PredictPnlAdapter(
                account_address="0x0000000000000000000000000000000000000001",
                api_key="key",
                client=_PredictClient(),
            ).fetch(),
        )

        assert polymarket.total_pnl_usd == Decimal("7.25")
        assert polymarket.realized_pnl_usd == Decimal("4.75")
        assert limitless.realized_pnl_usd == Decimal("7.5")
        assert limitless.unrealized_pnl_usd == Decimal("20")
        assert limitless.total_pnl_usd == Decimal("27.5")
        assert limitless.scope == "all_time + current_positions"
        assert limitless_client.chart_params == {"timeframe": "all"}
        assert predict.realized_pnl_usd == Decimal("4.5")
        assert predict.unrealized_pnl_usd == Decimal("-1.25")
        assert predict.total_pnl_usd == Decimal("3.25")
        assert polymarket.fees_usd is limitless.fees_usd is predict.fees_usd is None
        assert polymarket.positions[0].fees_usd == Decimal("0.1")
        assert polymarket.positions[0].contract_id is not None
        assert len(limitless.positions) == 2
        assert limitless.positions[0].resolved is True
        assert limitless.positions[1].current_price is None
        assert predict.positions[0].quantity == Decimal("10")
        assert predict.positions[1].resolved is True

    asyncio.run(run())
