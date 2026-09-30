"""Exercise kalshi key extraction behavior in the infrastructure kalshi layer.

Responsibilities
----------------
- Verify kalshi key extraction contracts, edge cases, and failure handling.
"""

import asyncio
from decimal import Decimal

import httpx
import pytest

from prediction_markets.domain.market_matching.enums import (
    ComparisonOperator,
    ObservationMethod,
    UpDownOutcome,
)
from prediction_markets.domain.market_matching.value_objects import Underlying
from prediction_markets.domain.markets.entities import Market, MarketSide
from prediction_markets.domain.markets.enums import BinaryOutcome, MarketStatus
from prediction_markets.domain.markets.value_objects import MarketState
from prediction_markets.domain.shared.value_objects import MarketID, OutcomeID, VenueID
from prediction_markets.infrastructure.venues.kalshi.key_extraction import (
    KalshiKeyExtractionAdapter,
    _index_raw_markets,
    _kalshi_payload_to_up_down_key,
)


def _kalshi_market(
    market_id: str = "KXBTC-26JAN01-B100000",
    *,
    title: str = "Bitcoin above $100,000 on Jan 1, 2026?",
) -> Market:
    return Market(
        id=MarketID(market_id),
        venue_id=VenueID("KALSHI"),
        title=title,
        state=MarketState(MarketStatus.ACTIVE),
        yes_side=MarketSide(
            id=OutcomeID(f"{market_id}:yes"),
            side=BinaryOutcome.YES,
        ),
        no_side=MarketSide(
            id=OutcomeID(f"{market_id}:no"),
            side=BinaryOutcome.NO,
        ),
    )


def test_init_rejects_non_positive_timeout():
    with pytest.raises(ValueError, match="timeout_seconds must be positive"):
        KalshiKeyExtractionAdapter(timeout_seconds=0)


def test_index_raw_markets_supports_market_ticker_field():
    indexed = _index_raw_markets(
        (
            {
                "market_ticker": "KXBTC-26JAN01-B90000",
                "floor_strike": "90000",
            },
        ),
    )

    assert "KXBTC-26JAN01-B90000" in indexed


def test_kalshi_payload_to_up_down_key_parses_fixture_fields(kalshi_market_payload):
    key = _kalshi_payload_to_up_down_key(
        kalshi_market_payload,
        underlying=Underlying("BTC"),
    )

    assert str(key.underlying) == "BTC"
    assert str(key.currency) == "USD"
    assert str(key.start) == "2026-01-01T14:45:00+00:00"
    assert str(key.end) == "2026-01-01T15:00:00+00:00"
    assert key.reference_price.amount == Decimal("100000.25")
    assert key.resolution_rule.observation == ObservationMethod.MEAN
    assert key.resolution_rule.observation_window_seconds == 60
    assert key.resolution_rule.comparison == ComparisonOperator.GREATER_THAN
    assert key.resolution_rule.tie_outcome == UpDownOutcome.DOWN


def test_kalshi_payload_to_up_down_key_requires_floor_strike(kalshi_market_payload):
    payload = {**kalshi_market_payload}
    payload.pop("floor_strike")

    with pytest.raises(ValueError, match="reference price is missing"):
        _kalshi_payload_to_up_down_key(payload, underlying=Underlying("BTC"))


def test_kalshi_payload_to_up_down_key_rejects_unknown_resolution_rule(
    kalshi_market_payload,
):
    payload = {
        **kalshi_market_payload,
        "rules_primary": "The market follows the official value.",
        "rules_secondary": "",
    }

    with pytest.raises(ValueError, match="observation method"):
        _kalshi_payload_to_up_down_key(payload, underlying=Underlying("BTC"))


def test_kalshi_payload_to_up_down_key_parses_last_price_and_gte(kalshi_market_payload):
    payload = {
        **kalshi_market_payload,
        "rules_primary": "Resolves Yes if the last price is greater than or equal to the strike.",
        "rules_secondary": "",
    }

    key = _kalshi_payload_to_up_down_key(payload, underlying=Underlying("BTC"))

    assert key.resolution_rule.observation == ObservationMethod.LAST
    assert key.resolution_rule.observation_window_seconds == 0
    assert key.resolution_rule.comparison == ComparisonOperator.GREATER_THAN_OR_EQUAL
    assert key.resolution_rule.tie_outcome == UpDownOutcome.UP


def test_extract_key_uses_cached_raw_market_payload(kalshi_market_payload):
    async def run_test():
        adapter = KalshiKeyExtractionAdapter(raw_markets=(kalshi_market_payload,))

        keyed = await adapter.extract_key(
            (_kalshi_market(),),
            underlying=Underlying("BTC"),
        )

        assert len(keyed) == 1
        market, key = keyed[0]
        assert market.id.value == "KXBTC-26JAN01-B100000"
        assert key.reference_price.amount == Decimal("100000.25")

    asyncio.run(run_test())


def test_extract_key_fetches_market_when_not_cached(
    kalshi_market_payload,
    mock_kalshi_async_client,
):
    async def run_test():
        client, requests = mock_kalshi_async_client(kalshi_market_payload)
        adapter = KalshiKeyExtractionAdapter(
            base_url="https://example.test",
            client=client,
        )

        keyed = await adapter.extract_key(
            (_kalshi_market(),),
            underlying=Underlying("BTC"),
        )

        assert len(keyed) == 1
        assert len(requests) == 1
        assert requests[0].url.path == "/markets/KXBTC-26JAN01-B100000"
        await client.aclose()

    asyncio.run(run_test())


def test_extract_key_reuses_fetched_market_without_extra_requests(
    kalshi_market_payload,
    mock_kalshi_async_client,
):
    async def run_test():
        client, requests = mock_kalshi_async_client(kalshi_market_payload)
        adapter = KalshiKeyExtractionAdapter(
            base_url="https://example.test",
            client=client,
        )
        market = _kalshi_market()

        first = await adapter.extract_key((market,), underlying=Underlying("BTC"))
        second = await adapter.extract_key((market,), underlying=Underlying("BTC"))

        assert len(first) == 1
        assert first == second
        assert len(requests) == 1
        await client.aclose()

    asyncio.run(run_test())


def test_extract_key_skips_market_when_fetch_returns_404(
    mock_kalshi_async_client,
):
    async def run_test():
        client, requests = mock_kalshi_async_client(
            {"ticker": "KXBTC-26JAN01-B100000"},
        )
        adapter = KalshiKeyExtractionAdapter(
            base_url="https://example.test",
            client=client,
        )

        keyed = await adapter.extract_key(
            (_kalshi_market(market_id="KXBTC-UNKNOWN"),),
            underlying=Underlying("BTC"),
        )

        assert keyed == ()
        assert len(requests) == 1
        await client.aclose()

    asyncio.run(run_test())


def test_extract_key_skips_markets_without_reference_strike(kalshi_market_payload):
    async def run_test():
        incomplete_payload = {**kalshi_market_payload}
        incomplete_payload.pop("floor_strike")
        adapter = KalshiKeyExtractionAdapter(raw_markets=(incomplete_payload,))

        keyed = await adapter.extract_key(
            (_kalshi_market(),),
            underlying=Underlying("BTC"),
        )

        assert keyed == ()

    asyncio.run(run_test())


def test_extract_key_skips_non_kalshi_markets(kalshi_market_payload):
    async def run_test():
        adapter = KalshiKeyExtractionAdapter(raw_markets=(kalshi_market_payload,))
        polymarket = Market(
            id=MarketID("0xabc"),
            venue_id=VenueID("POLYMARKET"),
            title="Will BTC go up?",
            state=MarketState(MarketStatus.ACTIVE),
            yes_side=MarketSide(
                id=OutcomeID("0xabc:yes:123"),
                side=BinaryOutcome.YES,
            ),
            no_side=MarketSide(
                id=OutcomeID("0xabc:no:456"),
                side=BinaryOutcome.NO,
            ),
        )

        keyed = await adapter.extract_key(
            (polymarket,),
            underlying=Underlying("BTC"),
        )

        assert keyed == ()

    asyncio.run(run_test())


def test_extract_key_returns_only_keyed_markets_from_batch(kalshi_market_payload):
    async def run_test():
        valid = {**kalshi_market_payload}
        invalid = {
            **kalshi_market_payload,
            "ticker": "KXBTC-26JAN01-B90000",
            "market_ticker": "KXBTC-26JAN01-B90000",
        }
        invalid.pop("floor_strike")

        adapter = KalshiKeyExtractionAdapter(raw_markets=(valid, invalid))

        keyed = await adapter.extract_key(
            (
                _kalshi_market("KXBTC-26JAN01-B100000"),
                _kalshi_market("KXBTC-26JAN01-B90000"),
            ),
            underlying=Underlying("BTC"),
        )

        assert len(keyed) == 1
        assert keyed[0][0].id.value == "KXBTC-26JAN01-B100000"

    asyncio.run(run_test())


def test_extract_key_mixed_venues_returns_only_kalshi(kalshi_market_payload):
    async def run_test():
        adapter = KalshiKeyExtractionAdapter(raw_markets=(kalshi_market_payload,))
        polymarket = Market(
            id=MarketID("0xabc"),
            venue_id=VenueID("POLYMARKET"),
            title="Will BTC go up?",
            state=MarketState(MarketStatus.ACTIVE),
            yes_side=MarketSide(
                id=OutcomeID("0xabc:yes:123"),
                side=BinaryOutcome.YES,
            ),
            no_side=MarketSide(
                id=OutcomeID("0xabc:no:456"),
                side=BinaryOutcome.NO,
            ),
        )

        keyed = await adapter.extract_key(
            (polymarket, _kalshi_market()),
            underlying=Underlying("BTC"),
        )

        assert len(keyed) == 1
        assert keyed[0][0].venue_id.value == "KALSHI"

    asyncio.run(run_test())


def test_close_closes_owned_client():
    async def run_test():
        adapter = KalshiKeyExtractionAdapter()
        assert not adapter._client.is_closed

        await adapter.close()

        assert adapter._client.is_closed

    asyncio.run(run_test())


def test_close_does_not_close_injected_client():
    async def run_test():
        client = httpx.AsyncClient()
        adapter = KalshiKeyExtractionAdapter(client=client)

        await adapter.close()

        assert not client.is_closed
        await client.aclose()

    asyncio.run(run_test())


def test_fetch_market_raises_on_invalid_response_shape():
    async def run_test():
        async def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json=["not", "a", "dict"])

        client = httpx.AsyncClient(
            transport=httpx.MockTransport(handler),
            base_url="https://example.test",
        )
        adapter = KalshiKeyExtractionAdapter(
            base_url="https://example.test",
            client=client,
        )

        with pytest.raises(TypeError, match="expected dict"):
            await adapter.extract_key(
                (_kalshi_market(),),
                underlying=Underlying("BTC"),
            )

        await client.aclose()

    asyncio.run(run_test())
