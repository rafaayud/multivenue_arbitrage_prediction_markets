"""Exercise arbitrage stream behavior in the infrastructure agg layer.

Responsibilities
----------------
- Verify arbitrage stream contracts, edge cases, and failure handling.
"""

import asyncio
import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import httpx
import prediction_markets.infrastructure.agg.arbitrage_stream as arbitrage_stream
from prediction_markets.infrastructure.agg.arbitrage_stream import (
    AggArbitrageStreamAdapter,
)


class _WebSocket:
    """Provide a scripted WebSocket test double for transport scenarios."""
    def __init__(self, messages):
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


def test_adapter_loads_config_from_dotenv(monkeypatch):
    monkeypatch.delenv("AGG_APP_ID", raising=False)
    monkeypatch.delenv("AGG_ORIGIN", raising=False)

    def fake_load_dotenv():
        monkeypatch.setenv("AGG_APP_ID", "from-dotenv")
        monkeypatch.setenv("AGG_ORIGIN", "http://localhost:3000")

    monkeypatch.setattr(arbitrage_stream, "load_dotenv", fake_load_dotenv)

    adapter = AggArbitrageStreamAdapter()

    assert adapter._app_id == "from-dotenv"
    assert adapter._origin == "http://localhost:3000"


def test_list_candidates_flattens_matches_filters_and_paginates():
    requests: list[httpx.Request] = []
    now = datetime.now(timezone.utc)
    valid_start = (now - timedelta(hours=1)).isoformat()
    valid_end = (now + timedelta(hours=1)).isoformat()
    late_end = (now + timedelta(days=2)).isoformat()

    def handler(request: httpx.Request):
        requests.append(request)
        cursor = request.url.params.get("cursor")
        if cursor is None:
            payload = {
                "data": [
                    {
                        "id": "ve_1",
                        "title": "Bitcoin milestone",
                        "gameStartTime": valid_start,
                        "endDate": valid_end,
                        "categories": [{"category": {"name": "crypto"}}],
                        "venueMarkets": [
                            {
                                "id": "vm_source",
                                "venue": "polymarket",
                                "question": "Will BTC close above $100k?",
                                "volume": 100,
                                "externalIdentifier": "poly-native",
                                "venueMarketOutcomes": [
                                    {"id": "poly-yes", "label": "Yes"},
                                    {"id": "poly-no", "label": "No"},
                                ],
                                "arbReturn": 0,
                                "matchedVenueMarkets": [
                                    {
                                        "id": "vm_1",
                                        "venue": "limitless",
                                        "question": "Will BTC close above $100k?",
                                        "volume": 250,
                                        "externalIdentifier": "limitless-native",
                                        "venueMarketOutcomes": [
                                            {"id": "limitless-yes", "label": "Yes"},
                                            {"id": "limitless-no", "label": "No"},
                                        ],
                                        "arbReturn": 0.015,
                                        "venueEvent": {"id": "ve_2"},
                                    },
                                    {"id": "vm_low", "arbReturn": 0.005},
                                ],
                            }
                        ],
                    },
                    {
                        "id": "ve_wrong_topic",
                        "endDate": valid_end,
                        "categories": [{"category": {"name": "sports"}}],
                        "venueMarkets": [{"id": "vm_sports", "arbReturn": 0.5}],
                    },
                    {
                        "id": "ve_too_late",
                        "endDate": late_end,
                        "categories": [{"category": {"name": "crypto"}}],
                        "venueMarkets": [{"id": "vm_late", "arbReturn": 0.5}],
                    },
                ],
                "hasMore": True,
                "nextCursor": "next",
            }
        else:
            payload = {
                "data": [
                    {
                        "id": "ve_3",
                        "title": "Ethereum milestone",
                        "gameStartTime": valid_start,
                        "endDate": valid_end,
                        "categories": [{"category": {"name": "crypto"}}],
                        "venueMarkets": [
                            {
                                "id": "vm_2-source",
                                "venue": "polymarket",
                                "question": "Will ETH close above $10k?",
                                "externalIdentifier": "poly-eth",
                                "venueMarketOutcomes": [
                                    {"id": "poly-eth-yes", "label": "Yes"},
                                    {"id": "poly-eth-no", "label": "No"},
                                ],
                                "arbReturn": 0,
                                "matchedVenueMarkets": [
                                    {
                                        "id": "vm_2",
                                        "venue": "limitless",
                                        "question": "Will ETH close above $10k?",
                                        "externalIdentifier": "limitless-eth",
                                        "venueMarketOutcomes": [
                                            {"id": "limitless-eth-yes", "label": "Yes"},
                                            {"id": "limitless-eth-no", "label": "No"},
                                        ],
                                        "arbReturn": 0.02,
                                    },
                                ],
                            },
                        ],
                    }
                ],
                "hasMore": False,
                "nextCursor": None,
            }
        return httpx.Response(200, json=payload)

    async def list_candidates():
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            return await AggArbitrageStreamAdapter(
                app_id="app-id",
                origin="http://localhost:3000",
                client=client,
            ).list_candidates(
                min_return=Decimal("0.01"),
                limit=2,
                topics=(" Crypto ",),
                search_text="MILESTONE",
                live_only=True,
                min_time_to_close=timedelta(minutes=10),
                max_time_to_close=timedelta(hours=24),
                match_statuses=("verified",),
            )

    candidates = asyncio.run(list_candidates())

    assert [str(candidate.market_id) for candidate in candidates] == ["vm_2", "vm_1"]
    assert candidates[1].venue_event_id is not None
    assert str(candidates[1].venue_event_id) == "ve_2"
    assert {
        (str(market.venue_id), market.external_market_id)
        for market in candidates[1].markets
    } == {
        ("POLYMARKET", "poly-native"),
        ("LIMITLESS", "limitless-native"),
    }
    assert candidates[1].title == "Will BTC close above $100k?"
    assert candidates[1].event_title == "Bitcoin milestone"
    assert candidates[1].starts_at is not None
    assert candidates[1].starts_at.value == datetime.fromisoformat(valid_start)
    assert candidates[1].ends_at is not None
    assert candidates[1].ends_at.value == datetime.fromisoformat(valid_end)
    assert candidates[1].volume_usd == Decimal("350")
    assert requests[0].headers["x-app-id"] == "app-id"
    assert requests[0].url.params.get_list("matchStatus") == ["verified"]
    assert requests[0].url.params["search"] == "milestone"
    assert requests[1].url.params["cursor"] == "next"


def test_live_filter_rejects_events_that_have_not_started() -> None:
    """Require an AGG event window that contains the observation time."""
    observed_at = arbitrage_stream.Timestamp.from_iso("2026-08-14T12:00:00Z")
    event = {
        "title": "Dota 2: Liquid vs Spirit",
        "gameStartTime": "2026-08-14T13:00:00Z",
        "endDate": "2026-08-14T16:00:00Z",
        "categories": [{"category": {"name": "esports"}}],
    }

    assert not arbitrage_stream._event_matches_filters(
        event,
        observed_at,
        ("esports",),
        "dota",
        True,
        None,
        None,
    )
    event["gameStartTime"] = "2026-08-14T11:00:00Z"
    assert arbitrage_stream._event_matches_filters(
        event,
        observed_at,
        ("esports",),
        "dota",
        True,
        None,
        None,
    )


def test_search_filter_matches_all_terms_in_an_event() -> None:
    """Match an event when search terms are separated by other text."""
    event = {"title": "Alpha Club vs Beta United"}
    observed_at = arbitrage_stream.Timestamp.from_iso("2026-08-14T12:00:00Z")

    assert arbitrage_stream._event_matches_filters(
        event,
        observed_at,
        (),
        "Alpha Beta",
        False,
        None,
        None,
    )


def test_sports_filter_excludes_esports_events() -> None:
    """Keep the broad sports scope limited to traditional sports."""
    observed_at = arbitrage_stream.Timestamp.from_iso("2026-08-14T12:00:00Z")
    event = {
        "categories": [
            {"category": {"name": "sports"}},
            {"category": {"name": "esports"}},
        ],
    }

    assert not arbitrage_stream._event_matches_filters(
        event,
        observed_at,
        ("sports",),
        None,
        False,
        None,
        None,
    )
    event["categories"] = [
        {"category": {"name": "sports"}},
        {"category": {"name": "soccer"}},
    ]
    assert arbitrage_stream._event_matches_filters(
        event,
        observed_at,
        ("sports",),
        None,
        False,
        None,
        None,
    )


def test_stream_returns_subscribes_once_and_maps_feed_entries(monkeypatch):
    websocket = _WebSocket(
        (
            json.dumps({"type": "connected", "appId": "app_demo"}),
            json.dumps(
                {
                    "type": "arb_feed_batch",
                    "feed": "arb",
                    "activeVenuesOnly": True,
                    "entries": [
                        {
                            "marketId": "vm_1",
                            "venueEventId": "ve_1",
                            "arbReturn": 0.012,
                            "ts": 1710000000000,
                            "liquidityUsd": 2500,
                            "liquidityTier": "deep",
                        },
                        {
                            "marketId": "vm_2",
                            "venueEventId": None,
                            "arbReturn": 0.004,
                            "ts": 1710000000300,
                        },
                    ],
                    "flushTs": 1710000000300,
                    "chunk": 0,
                    "chunkCount": 1,
                }
            ),
        )
    )
    connections: list[tuple[str, str | None]] = []

    def fake_connect(url, *, origin=None):
        connections.append((url, origin))
        return websocket

    monkeypatch.setattr(arbitrage_stream, "connect", fake_connect)

    async def receive_updates():
        stream = AggArbitrageStreamAdapter(
            app_id="app id",
            origin="http://localhost:3000",
        ).stream_returns()
        updates = (await anext(stream), await anext(stream))
        await stream.aclose()
        return updates

    first, second = asyncio.run(receive_updates())

    assert connections == [
        ("wss://ws.agg.market/ws?appId=app+id", "http://localhost:3000")
    ]
    assert json.loads(websocket.sent[0]) == {
        "action": "subscribe",
        "channel": "arb-feed",
    }
    assert str(first.market_id) == "vm_1"
    assert first.return_rate == Decimal("0.012")
    assert first.observed_at.to_unix_ms() == 1710000000000
    assert first.liquidity_usd == Decimal("2500")
    assert first.liquidity_tier == "deep"
    assert str(second.market_id) == "vm_2"
    assert second.venue_event_id is None
