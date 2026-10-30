"""Exercise outcome identities, native books, metadata caching and stream recovery."""

import asyncio
import json
from contextlib import asynccontextmanager
from decimal import Decimal

import httpx
import pytest

from prediction_markets.domain.ports.instrument_discovery import InstrumentDiscoveryQuery
from prediction_markets.domain.shared.value_objects import ContractID, MarketID
from prediction_markets.domain.venue_health import VenueHealthStatus
from prediction_markets.infrastructure.venues.hyperliquid.catalog import HyperliquidMarketCatalog
from prediction_markets.infrastructure.venues.hyperliquid.config import HyperliquidSettings, hyperliquid_enabled
from prediction_markets.infrastructure.venues.hyperliquid.health import HyperliquidHealthAdapter
from prediction_markets.infrastructure.venues.hyperliquid.instrument_discovery import HyperliquidInstrumentDiscoveryAdapter
from prediction_markets.infrastructure.venues.hyperliquid.market_data import HyperliquidMarketDataAdapter
from prediction_markets.infrastructure.venues.hyperliquid import market_data_stream as streams
from prediction_markets.infrastructure.venues.hyperliquid.mappers import (
    hyperliquid_contract_id, hyperliquid_orderbook_to_order_book, outcome_coin,
    outcome_to_market, parse_hyperliquid_contract_id,
)

YES, NO = hyperliquid_contract_id(19990, 0), hyperliquid_contract_id(19990, 1)
OUTCOME = {
    "outcome": 19990, "name": "template:increase", "description": "",
    "sideSpecs": [{"name": "Yes"}, {"name": "No"}], "quoteToken": "USDC",
}
QUESTION = {
    "question": 42, "name": "template:decision",
    "description": "institution:FOMC|decision:December 2099|expiry:20991215-1900|officialSource:Federal Reserve",
    "namedOutcomes": [19990], "fallbackOutcome": 19991,
}
TEMPLATES = [
    {"id": "decision", "name": "{institution} {decision} rate decision", "description": "Published by {officialSource}."},
    {"id": "increase", "name": "Increase", "description": "Higher than the prior rate under the parent rules."},
]


def book(coin: str = "#199900", price: str = "0.4") -> dict:
    return {"coin": coin, "time": 1793376000123, "levels": [
        [{"px": price, "sz": "25"}, {"px": "0.3", "sz": "1"}],
        [{"px": "0.7", "sz": "30"}, {"px": "0.6", "sz": "0"}],
    ]}


def test_network_is_explicit_and_disabled_by_default(monkeypatch):
    """Keep public access opt-in and reject unknown network names."""
    monkeypatch.delenv("HYPERLIQUID_ENABLED", raising=False)
    assert not hyperliquid_enabled()
    monkeypatch.setenv("HYPERLIQUID_ENABLED", "true")
    assert hyperliquid_enabled()
    assert HyperliquidSettings("testnet").websocket_url == "wss://api.hyperliquid-testnet.xyz/ws"
    with pytest.raises(ValueError):
        HyperliquidSettings("other")


def test_ids_and_parent_context_do_not_conflate_binary_outcomes():
    """Retain native side identities, parent rules and actual metadata constraints."""
    assert outcome_coin(YES) == "#199900"
    assert outcome_coin(NO) == "#199901"
    assert parse_hyperliquid_contract_id(NO) == (19990, 1)
    for invalid in ("hyperliquid:19990:maybe", "hyperliquid:-1:yes", "hyperliquid:01:yes", "predict:19990:yes"):
        with pytest.raises(ValueError):
            parse_hyperliquid_contract_id(ContractID(invalid))
    with pytest.raises(ValueError):
        hyperliquid_contract_id(1, True)
    normalized = outcome_to_market(OUTCOME, {t["id"]: t for t in TEMPLATES}, QUESTION)
    assert normalized is not None
    market, contracts = normalized
    assert market.title == "FOMC December 2099 rate decision: Increase"
    assert "Published by Federal Reserve" in market.resolution.rules
    assert market.state.close_time.value.year == 2099
    assert market.is_active()
    assert [contract.id for contract in contracts] == [YES, NO]
    assert contracts[0].lot_size.value == 1
    assert contracts[0].tick_size.value == Decimal("0.00001")
    assert outcome_to_market({**OUTCOME, "sideSpecs": [{"name": "A"}, {"name": "B"}]}, {}) is None
    assert outcome_to_market(OUTCOME, {}) is None
    assert outcome_to_market({**OUTCOME, "name": "Other", "quoteToken": "USDH"}, {}) is None


def test_native_no_book_is_not_synthesized_and_invalid_frames_fail():
    """Validate numeric depth and identity before returning an immutable book."""
    result = hyperliquid_orderbook_to_order_book(book("#199901", "0.15"), NO)
    assert result.best_bid().price.value == Decimal("0.3")
    assert result.best_ask().price.value == Decimal("0.7")
    assert len(result.asks) == 1
    assert result.source_at_ns == 1793376000123000000
    for payload in (
        book("#199900"), {**book("#199901"), "time": True},
        {**book("#199901"), "levels": [None, []]},
        book("#199901", "NaN"), book("#199901", "1.2"),
        {**book("#199901"), "levels": [[{"px": "0.1", "sz": "-1"}], []]},
        {**book("#199901"), "levels": [[{}], []]},
        {**book("#199901"), "levels": [[{"px": True, "sz": "1"}], []]},
    ):
        with pytest.raises(ValueError):
            hyperliquid_orderbook_to_order_book(payload, NO)


def test_shared_catalog_discovery_rest_books_and_health():
    """Share metadata reads while keeping health uncached and injected clients open."""
    async def run():
        requests = []

        def respond(request):
            body = json.loads(request.content)
            requests.append(body)
            payload = {
                "outcomeMeta": {"outcomes": [OUTCOME], "questions": [QUESTION]},
                "outcomeTemplates": TEMPLATES,
                "l2Book": book(body.get("coin", "#199900")),
            }[body["type"]]
            return httpx.Response(200, json=payload)

        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
            catalog = HyperliquidMarketCatalog(client=client)
            discovery = HyperliquidInstrumentDiscoveryAdapter(catalog=catalog)
            data = HyperliquidMarketDataAdapter(catalog=catalog)
            first, second = await asyncio.gather(
                discovery.discover(InstrumentDiscoveryQuery(search_text="FOMC")),
                data.list_contracts(MarketID("19990")),
            )
            assert len(first.markets) == 1 and len(second) == 2
            assert len(requests) == 2
            exact = await discovery.discover(InstrumentDiscoveryQuery(venue_market_id="19990", search_text="irrelevant AGG title"))
            assert exact.markets == first.markets
            agg_id = await discovery.discover(InstrumentDiscoveryQuery(venue_market_id="outcome:19990", search_text="irrelevant AGG title"))
            assert agg_id == exact
            assert not (await discovery.discover(InstrumentDiscoveryQuery(search_text="unknown"))).markets
            no_book = await data.get_order_book(NO)
            assert str(no_book.outcome_id) == "19990:no"
            assert requests[-1] == {"type": "l2Book", "coin": "#199901"}
            health = await HyperliquidHealthAdapter(catalog=catalog).check()
            assert health.status is VenueHealthStatus.OPERATIONAL
            assert requests[-1] == {"type": "outcomeMeta"}
            await data.close()
            await discovery.close()
            await catalog.close()
            assert not client.is_closed

    asyncio.run(run())


def test_stream_subscribes_both_sides_and_invalidates_before_reconnect(monkeypatch):
    """Invalidate disconnected snapshots and replace them after resubscription."""
    async def run():
        sent = []
        connections = []

        class Socket:
            async def send(self, message):
                sent.append(json.loads(message))

            async def __aiter__(self):
                for payload in (book(), book("#199901", "0.17"), book()):
                    yield json.dumps({"channel": "l2Book", "data": payload})
                raise OSError("connection lost")

        @asynccontextmanager
        async def connect(_url, **kwargs):
            connections.append(kwargs)
            yield Socket()

        monkeypatch.setattr(streams, "connect", connect)
        stream = streams.HyperliquidMarketDataStreamAdapter().stream_order_books((YES, NO, YES))
        try:
            contract, initial = await anext(stream)
            assert contract == YES and initial.source_timestamp_kind == "snapshot_state"
            assert initial.arrival_at_ns > 0 and initial.arrival_wall_at_ns > 0
            contract, other = await anext(stream)
            assert contract == NO and other.best_bid().price.value == Decimal("0.3")
            assert (await anext(stream))[1].source_timestamp_kind == "venue_update"
            for expected in (YES, NO):
                contract, invalidated = await anext(stream)
                assert contract == expected and not invalidated.bids and not invalidated.asks
            assert (await anext(stream))[1].source_timestamp_kind == "snapshot_state"
            assert len(connections) == 2 and connections[0]["max_queue"] == 256
            assert [entry["subscription"]["coin"] for entry in sent if entry["method"] == "subscribe"] == ["#199900", "#199901"] * 2
        finally:
            await stream.aclose()

    asyncio.run(run())
