"""Exercise AGG market catalog discovery.

Responsibilities
----------------
- Verify that market browsing does not depend on positive arbitrage returns.
"""

import asyncio
from datetime import datetime, timedelta, timezone

import httpx

from prediction_markets.infrastructure.agg.market_explorer import (
    AggMarketExplorerAdapter,
)


def test_lists_zero_return_and_unmatched_markets() -> None:
    """Keep live binary groups even when AGG reports no opportunity or match."""
    requests: list[httpx.Request] = []
    now = datetime.now(timezone.utc)

    def venue_market(
        market_id: str,
        venue: str,
        external_id: str,
    ) -> dict[str, object]:
        return {
            "id": market_id,
            "venue": venue,
            "question": "Match Winner",
            "externalIdentifier": external_id,
            "arbReturn": 0,
            "venueMarketOutcomes": [
                {"id": f"{market_id}-yes", "label": "Yes"},
                {"id": f"{market_id}-no", "label": "No"},
            ],
        }

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        unmatched = venue_market("poly-map-1", "polymarket", "poly-map-1")
        matched = venue_market("poly-winner", "polymarket", "poly-winner")
        matched["matchedVenueMarkets"] = [
            venue_market("predict-winner", "predict", "predict-winner"),
        ]
        return httpx.Response(
            200,
            json={
                "data": [
                    {
                        "id": "valorant-event",
                        "title": "Valorant: Alpha vs Bravo",
                        "gameStartTime": (now - timedelta(hours=1)).isoformat(),
                        "endDate": (now + timedelta(hours=1)).isoformat(),
                        "categories": [{"category": {"name": "esports"}}],
                        "venueMarkets": [matched, unmatched],
                    },
                ],
                "hasMore": False,
                "nextCursor": None,
            },
        )

    async def list_markets():
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            return await AggMarketExplorerAdapter(
                app_id="app-id",
                client=client,
            ).list_markets(
                topics=("esports",),
                search_text="valorant",
                live_only=True,
            )

    markets = asyncio.run(list_markets())

    assert len(markets) == 2
    assert {market.return_rate for market in markets} == {0}
    assert sorted(len(market.markets) for market in markets) == [1, 2]
    assert requests[0].url.params["search"] == "valorant"
    assert requests[0].url.params.get_list("matchStatus") == []


def test_pairs_unmatched_markets_with_agg_key_or_limitless_title() -> None:
    """Pair Limitless by participants when AGG omits its canonical key."""
    requests: list[httpx.Request] = []
    now = datetime.now(timezone.utc)
    event = {
        "id": "agg-event-dota",
        "title": "Dota 2: Inner Circle vs Nemiga Gaming (BO3)",
        "gameStartTime": (now - timedelta(minutes=5)).isoformat(),
        "endDate": (now + timedelta(hours=1)).isoformat(),
        "categories": [
            {"category": {"name": "sports"}},
            {"category": {"name": "esports"}},
        ],
    }

    def venue_market(
        market_id: str,
        venue: str,
        external_id: str,
    ) -> dict[str, object]:
        return {
            "id": market_id,
            "venue": venue,
            "question": "Match Winner",
            "externalIdentifier": external_id,
            "status": "open",
            "arbReturn": 0,
            "aggKey": "agg_dota2_innercircle_nemigagaming_260824",
            "sportsMarketType": "moneyline",
            "period": "full",
            "marketCategory": "game_lines",
            "marketGroup": "game_lines",
            "marketSubtype": "moneyline",
            "lineValue": None,
            "startDate": event["gameStartTime"],
            "endDate": event["endDate"],
            "venueEventId": f"{venue}-event",
            "venueEvent": {
                "id": f"{venue}-event",
                "title": event["title"],
                "startDate": event["gameStartTime"],
                "endDate": event["endDate"],
                "categories": event["categories"],
            },
            "venueMarketOutcomes": [
                {"id": f"{market_id}-yes", "label": "Yes"},
                {"id": f"{market_id}-no", "label": "No"},
            ],
            "matchedVenueMarkets": [],
        }

    polymarket = venue_market("poly-match", "polymarket", "3831357")
    predict = venue_market("predict-match", "predict", "1672983")
    limitless = venue_market("limitless-match", "limitless", "370096")
    limitless["aggKey"] = None
    limitless["question"] = "Nemiga Gaming vs Inner Circle"
    limitless_event = limitless["venueEvent"]
    assert isinstance(limitless_event, dict)
    limitless_event["title"] = "Nemiga Gaming vs Inner Circle"

    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.path == "/venue-events":
            return httpx.Response(
                200,
                json={
                    "data": [{**event, "venueMarkets": [polymarket]}],
                    "hasMore": False,
                    "nextCursor": None,
                },
            )
        assert request.url.path == "/venue-markets"
        if request.url.params.get("venue") == "limitless":
            data = [limitless]
        elif request.url.params.get("search") == "nemiga":
            data = [polymarket, predict, limitless]
        else:
            data = [polymarket, predict]
        return httpx.Response(
            200,
            json={
                "data": data,
                "hasMore": True,
                "nextCursor": "another-market-page",
            },
        )

    async def list_markets():
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            return await AggMarketExplorerAdapter(
                app_id="app-id",
                client=client,
            ).list_markets(
                topics=("esports",),
                live_only=True,
            )

    markets = asyncio.run(list_markets())
    paired = next(
        market
        for market in markets
        if {str(item.venue_id) for item in market.markets}
        == {"POLYMARKET", "PREDICT", "LIMITLESS"}
    )

    assert len(paired.markets) == 3
    assert {item.external_market_id for item in paired.markets} == {
        "3831357",
        "1672983",
        "370096",
    }
    assert markets[0] == paired
    assert not any(
        len(market.markets) == 1
        and market.markets[0].external_market_id == "3831357"
        for market in markets
    )
    assert [request.url.path for request in requests].count("/venue-markets") == 3
    assert requests[1].url.params["context"] == "detail"
    assert requests[1].url.params["search"] == "esports"
    assert any(request.url.params.get("venue") == "limitless" for request in requests)
    assert any(request.url.params.get("search") == "nemiga" for request in requests)
