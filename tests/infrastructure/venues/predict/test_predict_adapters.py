"""Exercise predict adapters behavior in the infrastructure predict layer.

Responsibilities
----------------
- Verify predict adapters contracts, edge cases, and failure handling.
"""

import asyncio
import json
from decimal import Decimal

import httpx
import pytest

import prediction_markets.infrastructure.venues.predict.market_data_stream as stream_module
from prediction_markets.domain.market_matching.value_objects import Underlying
from prediction_markets.domain.market_matching.enums import (
    ComparisonOperator,
    UpDownOutcome,
)
from prediction_markets.domain.ports.instrument_discovery import (
    InstrumentDiscoveryQuery,
)
from prediction_markets.infrastructure.venues.predict.instrument_discovery import (
    PredictInstrumentDiscoveryAdapter,
)
from prediction_markets.infrastructure.venues.predict.key_extraction import (
    PredictKeyExtractionAdapter,
    _predict_payload_to_up_down_key,
)
from prediction_markets.infrastructure.venues.predict.market_data import (
    PredictMarketDataAdapter,
)
from prediction_markets.infrastructure.venues.predict.market_data_stream import (
    PredictMarketDataStreamAdapter,
    _subscription_message,
)
from prediction_markets.infrastructure.venues.predict.mappers import (
    PREDICT_VENUE_ID,
    parse_predict_contract_id,
    predict_market_to_contracts,
    predict_market_to_market,
    predict_orderbook_to_order_book,
)


def _market(market_id: int = 29076) -> dict:
    return {
        "id": market_id,
        "title": "BTC/USD Up or Down",
        "question": "BTC/USD Up or Down - 9:30-9:45AM ET",
        "description": "Up if the Pyth BTC/USD end price is higher; tie is 50-50.",
        "categorySlug": "btc-usd-up-down-15-minutes",
        "decimalPrecision": 2,
        "status": "REGISTERED",
        "tradingStatus": "OPEN",
        "marketVariant": "CRYPTO_UP_DOWN",
        "boostStartsAt": "2026-07-23T13:30:00Z",
        "boostEndsAt": "2026-07-23T13:44:59Z",
        "outcomes": [
            {"name": "Up", "indexSet": 1, "onChainId": f"{market_id}1"},
            {"name": "Down", "indexSet": 2, "onChainId": f"{market_id}2"},
        ],
        "stats": {
            "volumeTotalUsd": 125,
            "totalLiquidityUsd": 25,
        },
        "variantData": {
            "type": "CRYPTO_UP_DOWN",
            "priceFeedProvider": "PYTH",
            "priceFeedSymbol": "BTC_USD",
            "startPrice": 64700.25,
            "endPrice": None,
        },
    }


def _book(market_id: int = 29076) -> dict:
    return {
        "marketId": market_id,
        "updateTimestampMs": 1784813400000,
        "bids": [[0.45, 3], [0.44, 1]],
        "asks": [[0.55, 4], [0.56, 2]],
    }


def test_mappers_build_binary_market_and_complementary_books():
    market = _market()
    contracts = predict_market_to_contracts(market)
    domain_market = predict_market_to_market(market)

    assert len(contracts) == 2
    assert str(contracts[0].id) == "predict:29076:yes"
    assert contracts[0].symbol == "290761"
    assert str(contracts[0].payout_currency) == "USDT"
    assert contracts[0].tick_size.value == Decimal("0.01")
    assert parse_predict_contract_id(contracts[1].id) == ("29076", "no")
    assert domain_market is not None
    assert domain_market.venue_id == PREDICT_VENUE_ID
    assert str(domain_market.state.start_time) == "2026-07-23T13:30:00+00:00"
    assert str(domain_market.state.close_time) == "2026-07-23T13:44:59+00:00"

    yes_book = predict_orderbook_to_order_book(_book(), contract=contracts[0])
    no_book = predict_orderbook_to_order_book(_book(), contract=contracts[1])
    assert yes_book.best_bid().price.value == Decimal("0.45")
    assert yes_book.best_ask().price.value == Decimal("0.55")
    assert no_book.best_bid().price.value == Decimal("0.45")
    assert no_book.best_ask().price.value == Decimal("0.55")
    assert no_book.best_bid().quantity.value == Decimal("4")
    assert no_book.best_ask().quantity.value == Decimal("3")


@pytest.mark.parametrize("source_ms", [1784813400000, None])
def test_rest_snapshot_preserves_source_and_local_receipt_without_fake_transport(source_ms):
    """One existing HTTP fetch provides snapshot clocks, never invented WS arrival."""
    requests = []
    async def handler(request):
        requests.append(request)
        return httpx.Response(200, json={"success": True,
            "data": {**_book(), "updateTimestampMs": source_ms}})
    async def fetch():
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            adapter = PredictMarketDataAdapter(api_key="test", client=client)
            return await adapter.get_order_book(predict_market_to_contracts(_market())[0].id)
    book = asyncio.run(fetch())
    assert len(requests) == 1
    assert book.source_at_ns == (source_ms * 1_000_000 if source_ms is not None else None)
    assert book.received_at_ns > 0
    assert book.source_timestamp_kind == "snapshot_state"
    assert book.arrival_at_ns is None
    assert book.arrival_wall_at_ns is None


def test_discovery_paginates_filters_and_sends_api_key():
    requests: list[httpx.Request] = []
    markets = [_market(1), _market(2), _market(3)]

    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        after = request.url.params.get("after")
        if after is None:
            return httpx.Response(
                200,
                json={"success": True, "cursor": "next", "data": markets[:2]},
            )
        return httpx.Response(
            200,
            json={"success": True, "cursor": "", "data": markets[2:]},
        )

    async def discover():
        client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        adapter = PredictInstrumentDiscoveryAdapter(
            api_key="secret",
            client=client,
            base_url="https://example.test",
        )
        try:
            return await adapter.discover_contracts(
                InstrumentDiscoveryQuery(
                    venue_id=PREDICT_VENUE_ID,
                    limit=3,
                    underlying=Underlying("BTC"),
                    interval_seconds=900,
                    min_liquidity=Decimal("20"),
                )
            )
        finally:
            await client.aclose()

    contracts = asyncio.run(discover())

    assert len(contracts) == 6
    assert len(requests) == 2
    assert requests[0].headers["x-api-key"] == "secret"
    assert requests[0].url.params["status"] == "OPEN"
    assert requests[0].url.params["marketVariant"] == "CRYPTO_UP_DOWN"
    assert requests[1].url.params["after"] == "next"


def test_discovery_excludes_resolved_market_with_stale_open_status():
    """Let terminal lifecycle win over a stale Predict trading status."""
    market = {**_market(), "status": "RESOLVED", "tradingStatus": "OPEN"}

    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={"success": True, "cursor": "", "data": [market]},
        )

    async def discover():
        client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        adapter = PredictInstrumentDiscoveryAdapter(
            api_key="secret",
            client=client,
            base_url="https://example.test",
        )
        try:
            return await adapter.discover(
                InstrumentDiscoveryQuery(
                    venue_id=PREDICT_VENUE_ID,
                    active_only=True,
                    limit=1,
                    underlying=Underlying("BTC"),
                    interval_seconds=900,
                ),
            )
        finally:
            await client.aclose()

    result = asyncio.run(discover())

    assert result.markets == ()
    assert result.contracts == ()


def test_discovery_coalesces_concurrent_identical_market_pages():
    requests: list[httpx.Request] = []
    market = _market()

    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            200,
            json={"success": True, "cursor": "", "data": [market]},
        )

    async def discover():
        client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        adapter = PredictInstrumentDiscoveryAdapter(
            api_key="secret",
            client=client,
            base_url="https://example.test",
        )
        query = InstrumentDiscoveryQuery(
            venue_id=PREDICT_VENUE_ID,
            limit=1,
            underlying=Underlying("BTC"),
            interval_seconds=900,
        )
        try:
            markets, contracts = await asyncio.gather(
                adapter.discover_markets(query),
                adapter.discover_contracts(query),
            )
            return markets, contracts
        finally:
            await client.aclose()

    markets, contracts = asyncio.run(discover())

    assert len(markets) == 1
    assert len(contracts) == 2
    assert len(requests) == 1


def test_discovery_cools_down_after_predict_rate_limit():
    requests: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            429,
            headers={"Retry-After": "30"},
            request=request,
        )

    async def discover():
        client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        adapter = PredictInstrumentDiscoveryAdapter(
            api_key="secret",
            client=client,
            base_url="https://example.test",
        )
        query = InstrumentDiscoveryQuery(
            venue_id=PREDICT_VENUE_ID,
            limit=1,
            underlying=Underlying("BTC"),
            interval_seconds=900,
        )
        try:
            return await asyncio.gather(
                adapter.discover_contracts(query),
                adapter.discover_contracts(query),
                return_exceptions=True,
            )
        finally:
            await client.aclose()

    results = asyncio.run(discover())

    assert all(isinstance(result, httpx.HTTPStatusError) for result in results)
    assert len(requests) == 1


def test_rest_snapshot_and_key_extraction():
    market = _market()
    contracts = predict_market_to_contracts(market)

    async def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/orderbook"):
            return httpx.Response(200, json={"success": True, "data": _book()})
        return httpx.Response(200, json={"success": True, "data": market})

    async def exercise():
        client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        data = PredictMarketDataAdapter(
            api_key="secret",
            base_url="https://example.test",
            client=client,
            contracts=contracts,
        )
        keys = PredictKeyExtractionAdapter(
            api_key="secret",
            base_url="https://example.test",
            client=client,
            raw_markets=(market,),
        )
        try:
            book = await data.get_order_book(contracts[0].id)
            domain_market = predict_market_to_market(market)
            assert domain_market is not None
            extracted = await keys.extract_key(
                (domain_market,),
                underlying=Underlying("BTC"),
            )
            return book, extracted
        finally:
            await client.aclose()

    book, extracted = asyncio.run(exercise())

    assert book is not None
    assert book.best_bid().price.value == Decimal("0.45")
    assert len(extracted) == 1
    key = extracted[0][1]
    assert key.reference_price.amount == Decimal("64700.25")
    assert str(key.currency) == "USD"
    assert key.resolution_rule.source == "PYTH"
    assert key.resolution_rule.tie_outcome == "split"


def test_hourly_and_daily_keys_follow_binance_rules():
    hourly = _market()
    hourly.update(
        boostStartsAt="2026-07-23T13:00:00Z",
        boostEndsAt="2026-07-23T14:00:00Z",
    )
    hourly["variantData"].update(
        priceFeedProvider="BINANCE",
        priceFeedSymbol="BTCUSDT",
    )
    daily = {
        **hourly,
        "boostStartsAt": "2026-07-23T16:00:00Z",
        "boostEndsAt": "2026-07-24T16:00:00Z",
        "variantData": {**hourly["variantData"]},
    }

    hourly_key = _predict_payload_to_up_down_key(
        hourly,
        underlying=Underlying("BTC"),
    )
    daily_key = _predict_payload_to_up_down_key(
        daily,
        underlying=Underlying("BTC"),
    )

    assert str(hourly_key.currency) == "USDT"
    assert hourly_key.resolution_rule.comparison is ComparisonOperator.GREATER_THAN_OR_EQUAL
    assert hourly_key.resolution_rule.tie_outcome is UpDownOutcome.UP
    assert daily_key.resolution_rule.comparison is ComparisonOperator.GREATER_THAN
    assert daily_key.resolution_rule.tie_outcome is UpDownOutcome.SPLIT


def test_predict_key_rejects_a_market_that_has_not_started() -> None:
    market = _market()
    market["variantData"]["startPrice"] = None

    with pytest.raises(ValueError, match="start price is missing"):
        _predict_payload_to_up_down_key(
            market,
            underlying=Underlying("BTC"),
        )


def test_key_extraction_refetches_start_price_after_market_opens() -> None:
    """Retry incomplete detail metadata once per cache TTL."""
    pending = _market()
    pending["variantData"]["startPrice"] = None
    active = _market()
    requests = 0

    async def handler(_: httpx.Request) -> httpx.Response:
        nonlocal requests
        requests += 1
        return httpx.Response(
            200,
            json={"success": True, "data": pending if requests == 1 else active},
        )

    async def exercise():
        client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        adapter = PredictKeyExtractionAdapter(
            api_key="secret",
            base_url="https://example.test",
            cache_seconds=0.001,
            client=client,
        )
        market = predict_market_to_market(pending)
        assert market is not None
        try:
            first = await adapter.extract_key((market,), underlying=Underlying("BTC"))
            cached = await adapter.extract_key(
                (market,),
                underlying=Underlying("BTC"),
            )
            await asyncio.sleep(0.01)
            second = await adapter.extract_key((market,), underlying=Underlying("BTC"))
            return first, cached, second
        finally:
            await client.aclose()

    first, cached, second = asyncio.run(exercise())

    assert first == cached == ()
    assert len(second) == 1
    assert requests == 2


class _WebSocket:
    """Provide a scripted WebSocket test double for transport scenarios."""
    def __init__(self, messages: tuple[str, ...]):
        self._messages = iter(messages)
        self.sent: list[str] = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        return None

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            return next(self._messages)
        except StopIteration:
            raise StopAsyncIteration from None

    async def send(self, message: str):
        self.sent.append(message)


def test_websocket_multiplexes_markets_and_outcomes_and_answers_heartbeat(monkeypatch):
    contracts = (
        *predict_market_to_contracts(_market()),
        *predict_market_to_contracts(_market(29077)),
    )
    websocket = _WebSocket(
        (
            json.dumps({"type": "M", "topic": "heartbeat", "data": 123}),
            json.dumps({"type": "R", "requestId": 1, "success": True}),
            json.dumps({"type": "R", "requestId": 2, "success": True}),
            json.dumps(
                {
                    "type": "M",
                    "topic": "predictOrderbook/29076",
                    "data": _book(),
                }
            ),
            json.dumps(
                {
                    "type": "M",
                    "topic": "predictOrderbook/29077",
                    "data": _book(29077),
                }
            ),
        )
    )
    connect_args = {}
    connections = 0

    def fake_connect(url, **kwargs):
        nonlocal connections
        connections += 1
        connect_args.update(url=url, **kwargs)
        return websocket

    monkeypatch.setattr(stream_module, "connect", fake_connect)

    async def receive():
        stream = PredictMarketDataStreamAdapter(
            contracts,
            api_key="secret",
        ).stream_order_books(tuple(contract.id for contract in contracts))
        updates = tuple([await anext(stream) for _ in contracts])
        await stream.aclose()
        return updates

    updates = asyncio.run(receive())

    assert connections == 1
    assert {contract_id for contract_id, _ in updates} == {
        contract.id for contract in contracts
    }
    assert all(book.arrival_wall_at_ns is not None for _, book in updates)
    assert all(book.arrival_at_ns == book.received_at_ns for _, book in updates)
    assert all(book.source_timestamp_kind == "venue_update" for _, book in updates)
    assert websocket.sent == [
        _subscription_message("29076", 1),
        _subscription_message("29077", 2),
        json.dumps({"method": "heartbeat", "data": 123}),
    ]
    assert connect_args["additional_headers"] == {"x-api-key": "secret"}
