"""Exercise key extraction behavior in the infrastructure polymarket layer.

Responsibilities
----------------
- Verify key extraction contracts, edge cases, and failure handling.
"""

import asyncio
from decimal import Decimal

import httpx
import pytest

from prediction_markets.domain.market_matching.enums import ComparisonOperator, UpDownOutcome
from prediction_markets.domain.market_matching.value_objects import Underlying
from prediction_markets.infrastructure.venues.polymarket.key_extraction import PolymarketKeyExtractionAdapter
from prediction_markets.infrastructure.venues.polymarket.mappers import gamma_market_to_market
from prediction_markets.infrastructure.venues.polynode.key_extraction import PolynodeKeyExtractionAdapter


class _EmptyKeyExtractor:
    """Provide deterministic empty key extractor behavior for this test module."""
    async def extract_key(self, markets, *, underlying):
        return ()


@pytest.mark.parametrize(
    "end_date",
    [
        "2026-12-31T23:50:00Z",
        "2027-01-01T00:00:00Z",
    ],
)
def test_polymarket_short_form_key_uses_window_without_reference_price(
    gamma_market_payload,
    end_date,
):
    async def run_test():
        adapter = PolymarketKeyExtractionAdapter(
            polynode_key_extractor=_EmptyKeyExtractor(),
        )
        market = gamma_market_to_market(
            {**gamma_market_payload, "endDate": end_date},
        )
        assert market is not None

        keyed = await adapter.extract_key((market,), underlying=Underlying("BTC"))

        key = keyed[0][1]
        assert key.reference_price is None
        assert key.start == market.state.start_time
        assert key.end == market.state.close_time
        assert key.resolution_rule.comparison == ComparisonOperator.GREATER_THAN_OR_EQUAL
        assert key.resolution_rule.tie_outcome == UpDownOutcome.UP
        assert key.resolution_rule.source == "CHAINLINK"

        await adapter.close()

    asyncio.run(run_test())


@pytest.mark.parametrize(
    (
        "symbol",
        "end_date",
        "resolution_source",
        "candle_interval",
        "expected_path",
        "expected_price",
        "comparison",
        "tie_outcome",
    ),
    [
        (
            "BTC",
            "2026-07-14T22:00:00Z",
            "https://www.binance.com/en/trade/BTC_USDT",
            "1h",
            "/api/v3/klines",
            Decimal("64500"),
            ComparisonOperator.GREATER_THAN_OR_EQUAL,
            UpDownOutcome.UP,
        ),
        (
            "BTC",
            "2026-07-15T21:00:00Z",
            "https://www.binance.com/en/trade/BTC_USDT",
            "1m",
            "/api/v3/klines",
            Decimal("64600"),
            ComparisonOperator.GREATER_THAN,
            UpDownOutcome.SPLIT,
        ),
        (
            "HYPE",
            "2026-07-14T22:00:00Z",
            "https://www.binance.com/en/futures/HYPEUSDT",
            "1h",
            "/fapi/v1/klines",
            Decimal("64500"),
            ComparisonOperator.GREATER_THAN_OR_EQUAL,
            UpDownOutcome.UP,
        ),
    ],
)
def test_polymarket_long_interval_key_uses_binance_candle(
    gamma_market_payload,
    symbol,
    end_date,
    resolution_source,
    candle_interval,
    expected_path,
    expected_price,
    comparison,
    tie_outcome,
):
    async def run_test():
        requests: list[httpx.Request] = []

        async def handler(request: httpx.Request) -> httpx.Response:
            requests.append(request)
            start_ms = int(request.url.params["startTime"])
            return httpx.Response(
                200,
                json=[[start_ms, "64500", "64700", "64400", "64600"]],
            )

        client = httpx.AsyncClient(
            transport=httpx.MockTransport(handler),
            base_url="https://example.test",
        )
        adapter = PolymarketKeyExtractionAdapter(
            polynode_key_extractor=_EmptyKeyExtractor(),
            binance_base_url="https://example.test",
            binance_futures_base_url="https://example.test",
            client=client,
        )
        market = gamma_market_to_market(
            {
                **gamma_market_payload,
                "eventStartTime": "2026-07-14T21:00:00Z",
                "endDate": end_date,
                "resolutionSource": resolution_source,
                "description": "Resolves using Binance BTC/USDT prices.",
            },
        )
        assert market is not None

        keyed = await adapter.extract_key((market,), underlying=Underlying(symbol))

        key = keyed[0][1]
        assert key.reference_price.amount == expected_price
        assert str(key.currency) == "USDT"
        assert key.resolution_rule.comparison == comparison
        assert key.resolution_rule.tie_outcome == tie_outcome
        assert key.resolution_rule.source == "BINANCE"
        assert requests[0].url.path == expected_path
        assert requests[0].url.params["interval"] == candle_interval
        await client.aclose()

    asyncio.run(run_test())


@pytest.mark.parametrize("symbol", ("NVDA", "AMZN", "META", "TSLA", "SPY", "SPCX"))
def test_finance_daily_key_records_unpublished_pyth_reference(
    gamma_market_payload,
    symbol,
):
    async def run_test():
        requests: list[httpx.Request] = []

        async def handler(request: httpx.Request) -> httpx.Response:
            requests.append(request)
            return httpx.Response(500)

        client = httpx.AsyncClient(
            transport=httpx.MockTransport(handler),
            base_url="https://example.test",
        )
        adapter = PolymarketKeyExtractionAdapter(
            polynode_key_extractor=_EmptyKeyExtractor(),
            client=client,
        )
        market = gamma_market_to_market(
            {
                **gamma_market_payload,
                "eventStartTime": "2026-08-14T20:00:00Z",
                "endDate": "2026-08-17T20:00:00Z",
                "resolutionSource": f"https://pythdata.app/Equity.US.{symbol}/USD",
                "description": f"{symbol} resolves from Pyth closing prices.",
            },
        )
        assert market is not None

        keyed = await adapter.extract_key((market,), underlying=Underlying(symbol))

        key = keyed[0][1]
        assert key.reference_price is None
        assert str(key.currency) == "USD"
        assert key.resolution_rule.comparison == ComparisonOperator.GREATER_THAN
        assert key.resolution_rule.tie_outcome == UpDownOutcome.SPLIT
        assert key.resolution_rule.source == "PYTH"
        assert requests == []
        await client.aclose()

    asyncio.run(run_test())


def test_finance_daily_key_accepts_trading_day_window_longer_than_24_hours(
    gamma_market_payload,
):
    async def run_test():
        client = httpx.AsyncClient(
            transport=httpx.MockTransport(lambda _: httpx.Response(500)),
            base_url="https://example.test",
        )
        adapter = PolymarketKeyExtractionAdapter(
            polynode_key_extractor=_EmptyKeyExtractor(),
            client=client,
        )
        market = gamma_market_to_market(
            {
                **gamma_market_payload,
                "category": "finance",
                "eventStartTime": "2026-08-14T20:00:00Z",
                "endDate": "2026-08-17T20:00:00Z",
                "resolutionSource": "https://pythdata.app/Equity.US.XYZ/USD",
                "description": "XYZ resolves from Pyth closing prices.",
            },
        )
        assert market is not None

        keyed = await adapter.extract_key((market,), underlying=Underlying("XYZ"))

        assert len(keyed) == 1
        assert keyed[0][1].reference_price is None
        await client.aclose()

    asyncio.run(run_test())


def test_polynode_key_extraction_reads_poly_node_api(monkeypatch):
    async def run_test():
        monkeypatch.setenv("POLY_NODE_API", "env-key")
        adapter = PolynodeKeyExtractionAdapter()

        assert adapter._api_key == "pn_live_env-key"
        await adapter.close()

    asyncio.run(run_test())
