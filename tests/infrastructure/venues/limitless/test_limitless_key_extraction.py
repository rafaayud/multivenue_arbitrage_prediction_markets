"""Exercise limitless key extraction behavior in the infrastructure limitless layer.

Responsibilities
----------------
- Verify limitless key extraction contracts, edge cases, and failure handling.
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
from prediction_markets.domain.ports.instrument_discovery import InstrumentDiscoveryQuery
from prediction_markets.domain.markets.value_objects import MarketState
from prediction_markets.domain.shared.value_objects import MarketID, OutcomeID, VenueID
from prediction_markets.infrastructure.venues.limitless.catalog import LimitlessMarketCatalog
from prediction_markets.infrastructure.venues.limitless.instrument_discovery import (
    LimitlessInstrumentDiscoveryAdapter,
)
from prediction_markets.infrastructure.venues.limitless.key_extraction import (
    LimitlessKeyExtractionAdapter,
    _index_raw_markets,
    _limitless_payload_to_up_down_key,
)


def _limitless_market(
    market_id: str = "btc-up-or-down-hourly-123",
) -> Market:
    return Market(
        id=MarketID(market_id),
        venue_id=VenueID("LIMITLESS"),
        title="BTC Up or Down - Hourly",
        state=MarketState(MarketStatus.ACTIVE),
        yes_side=MarketSide(
            id=OutcomeID(f"{market_id}:yes:111"),
            side=BinaryOutcome.YES,
        ),
        no_side=MarketSide(
            id=OutcomeID(f"{market_id}:no:222"),
            side=BinaryOutcome.NO,
        ),
    )


def test_init_rejects_non_positive_timeout():
    with pytest.raises(ValueError, match="timeout_seconds must be positive"):
        LimitlessKeyExtractionAdapter(timeout_seconds=0)


def test_index_raw_markets_uses_slug(limitless_market_payload):
    indexed = _index_raw_markets((limitless_market_payload,))

    assert "btc-up-or-down-hourly-123" in indexed


def test_payload_to_up_down_key_parses_public_limitless_fields(limitless_market_payload):
    key = _limitless_payload_to_up_down_key(
        limitless_market_payload,
        underlying=Underlying("BTC"),
    )

    assert str(key.underlying) == "BTC"
    assert str(key.currency) == "USD"
    assert str(key.start) == "2026-07-14T16:00:00+00:00"
    assert str(key.end) == "2026-07-14T17:00:00+00:00"
    assert key.reference_price.amount == Decimal("64700.27740085")
    assert key.resolution_rule.observation == ObservationMethod.LAST
    assert key.resolution_rule.observation_window_seconds == 0
    assert key.resolution_rule.comparison == ComparisonOperator.GREATER_THAN_OR_EQUAL
    assert key.resolution_rule.tie_outcome == UpDownOutcome.UP
    assert key.resolution_rule.source == "CHAINLINK"


def test_payload_to_up_down_key_supports_strict_comparison(limitless_market_payload):
    payload = {
        **limitless_market_payload,
        "description": (
            "Resolves Up if the final Pyth price is strictly higher than the open price."
        ),
    }

    key = _limitless_payload_to_up_down_key(payload, underlying=Underlying("BTC"))

    assert key.resolution_rule.comparison == ComparisonOperator.GREATER_THAN
    assert key.resolution_rule.tie_outcome == UpDownOutcome.DOWN
    assert key.resolution_rule.source == "PYTH"


def test_payload_to_up_down_key_requires_open_price(limitless_market_payload):
    payload = {**limitless_market_payload, "metadata": {"minSize": "1000000"}}

    with pytest.raises(ValueError, match="reference price is missing"):
        _limitless_payload_to_up_down_key(payload, underlying=Underlying("BTC"))


def test_payload_to_up_down_key_rejects_unknown_rules(limitless_market_payload):
    payload = {**limitless_market_payload, "description": "Official result decides the market."}

    with pytest.raises(ValueError, match="comparison operator"):
        _limitless_payload_to_up_down_key(payload, underlying=Underlying("BTC"))


def test_extract_key_uses_cached_market(limitless_market_payload):
    async def run_test():
        adapter = LimitlessKeyExtractionAdapter(raw_markets=(limitless_market_payload,))

        keyed = await adapter.extract_key(
            (_limitless_market(),),
            underlying=Underlying("BTC"),
        )

        assert len(keyed) == 1
        assert keyed[0][0].id.value == "btc-up-or-down-hourly-123"
        assert keyed[0][1].reference_price.amount == Decimal("64700.27740085")
        await adapter.close()

    asyncio.run(run_test())


def test_discovery_and_key_extraction_share_one_market_request(
    limitless_market_payload,
    limitless_orderbook_payload,
    mock_limitless_async_client,
):
    """Reuse discovery metadata when key extraction handles the same market."""
    async def run_test():
        client, requests = mock_limitless_async_client(
            limitless_market_payload,
            limitless_orderbook_payload,
        )
        catalog = LimitlessMarketCatalog(
            base_url="https://example.test",
            client=client,
        )
        discovery = LimitlessInstrumentDiscoveryAdapter(catalog=catalog)
        keys = LimitlessKeyExtractionAdapter(catalog=catalog)

        result = await discovery.discover(
            InstrumentDiscoveryQuery(
                market_slug="btc-up-or-down-hourly-123",
            ),
        )
        keyed = await keys.extract_key(result.markets, underlying=Underlying("BTC"))

        assert len(keyed) == 1
        assert [request.url.path for request in requests] == [
            "/markets/btc-up-or-down-hourly-123",
        ]
        await catalog.close()
        await client.aclose()

    asyncio.run(run_test())


def test_extract_key_fetches_and_caches_market(
    limitless_market_payload,
    limitless_orderbook_payload,
    mock_limitless_async_client,
):
    async def run_test():
        client, requests = mock_limitless_async_client(
            limitless_market_payload,
            limitless_orderbook_payload,
        )
        adapter = LimitlessKeyExtractionAdapter(
            base_url="https://example.test",
            client=client,
        )
        market = _limitless_market()

        first = await adapter.extract_key((market,), underlying=Underlying("BTC"))
        second = await adapter.extract_key((market,), underlying=Underlying("BTC"))

        assert len(first) == 1
        assert first == second
        assert [request.url.path for request in requests] == [
            "/markets/btc-up-or-down-hourly-123",
        ]
        await client.aclose()

    asyncio.run(run_test())


def test_extract_key_refetches_transient_invalid_open_price(
    limitless_market_payload,
):
    async def run_test():
        requests: list[httpx.Request] = []
        invalid = {
            **limitless_market_payload,
            "metadata": {
                **limitless_market_payload["metadata"],
                "openPrice": "-",
            },
        }

        async def handler(request: httpx.Request) -> httpx.Response:
            requests.append(request)
            payload = invalid if len(requests) == 1 else limitless_market_payload
            return httpx.Response(200, json=payload)

        client = httpx.AsyncClient(
            transport=httpx.MockTransport(handler),
            base_url="https://example.test",
        )
        adapter = LimitlessKeyExtractionAdapter(client=client)
        market = _limitless_market()

        first = await adapter.extract_key((market,), underlying=Underlying("BTC"))
        second = await adapter.extract_key((market,), underlying=Underlying("BTC"))

        assert first == ()
        assert len(second) == 1
        assert len(requests) == 2
        await client.aclose()

    asyncio.run(run_test())


def test_extract_key_skips_missing_or_wrong_venue_market(limitless_market_payload):
    async def run_test():
        incomplete = {
            **limitless_market_payload,
            "slug": "not-found",
            "metadata": {"minSize": "1000000"},
        }
        adapter = LimitlessKeyExtractionAdapter(
            raw_markets=(limitless_market_payload, incomplete),
        )
        other_venue = Market(
            id=MarketID("other"),
            venue_id=VenueID("KALSHI"),
            title="Other venue market",
            state=MarketState(MarketStatus.ACTIVE),
            yes_side=MarketSide(OutcomeID("other:yes"), BinaryOutcome.YES),
            no_side=MarketSide(OutcomeID("other:no"), BinaryOutcome.NO),
        )

        keyed = await adapter.extract_key(
            (other_venue, _limitless_market("not-found")),
            underlying=Underlying("BTC"),
        )

        assert keyed == ()
        await adapter.close()

    asyncio.run(run_test())


def test_close_does_not_close_injected_client():
    async def run_test():
        client = httpx.AsyncClient()
        adapter = LimitlessKeyExtractionAdapter(client=client)

        await adapter.close()

        assert not client.is_closed
        await client.aclose()

    asyncio.run(run_test())
