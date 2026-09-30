"""Exercise instrument discovery behavior in the infrastructure agg layer.

Responsibilities
----------------
- Verify instrument discovery contracts, edge cases, and failure handling.
"""

import asyncio
from typing import Any

import httpx

from prediction_markets.domain.ports.instrument_discovery import InstrumentDiscoveryQuery
from prediction_markets.domain.shared.value_objects import VenueID
from prediction_markets.infrastructure.agg.instrument_discovery import (
    AggInstrumentDiscoveryAdapter,
)


def test_discovery_uses_agg_outcome_ids_and_maps_binary_sides():
    async def run_test():
        requests: list[httpx.Request] = []
        payload: dict[str, Any] = {
            "id": "agg-market-1",
            "venue": "polymarket",
            "question": "Will BTC be up?",
            "externalIdentifier": "btc-up-1",
            "status": "open",
            "venueEventId": "agg-event-1",
            "startDate": "2026-07-28T12:00:00Z",
            "endDate": "2026-07-28T12:15:00Z",
            "venueMarketOutcomes": [
                {"id": "agg-no", "label": "Down", "venueMarketId": "agg-market-1"},
                {"id": "agg-yes", "label": "Up", "venueMarketId": "agg-market-1"},
            ],
        }

        async def handler(request: httpx.Request) -> httpx.Response:
            requests.append(request)
            return httpx.Response(
                200,
                json={"data": [payload], "hasMore": False, "nextCursor": None},
            )

        client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        adapter = AggInstrumentDiscoveryAdapter(
            app_id="app-1",
            api_key="key-1",
            base_url="https://example.test",
            client=client,
        )

        markets = await adapter.discover_markets(
            InstrumentDiscoveryQuery(venue_id=VenueID("POLYMARKET"), interval_seconds=900),
        )
        contracts = await adapter.discover_contracts(
            InstrumentDiscoveryQuery(venue_id=VenueID("POLYMARKET"), interval_seconds=900),
        )

        assert str(markets[0].id) == "agg-market-1"
        assert str(markets[0].yes_side.id) == "agg-yes"
        assert str(markets[0].no_side.id) == "agg-no"
        assert [str(contract.id) for contract in contracts] == ["agg:agg-yes", "agg:agg-no"]
        assert [str(contract.outcome_id) for contract in contracts] == ["agg-yes", "agg-no"]
        assert requests[0].url.path == "/venue-markets"
        assert requests[0].url.params["venue"] == "polymarket"
        assert requests[0].url.params["status"] == "open"
        assert requests[0].headers["x-app-id"] == "app-1"
        assert requests[0].headers["x-app-api-key"] == "key-1"
        await adapter.close()

    asyncio.run(run_test())


def test_discovery_flattens_matched_markets_when_no_venue_filter():
    async def run_test():
        primary = {
            "id": "agg-poly-1",
            "venue": "polymarket",
            "question": "Will it happen?",
            "externalIdentifier": "poly-1",
            "status": "open",
            "venueMarketOutcomes": [
                {"id": "poly-yes", "label": "Yes"},
                {"id": "poly-no", "label": "No"},
            ],
            "matchedVenueMarkets": [
                {
                    "id": "agg-limitless-1",
                    "venue": "limitless",
                    "question": "Will it happen?",
                    "externalIdentifier": "limitless-1",
                    "status": "open",
                    "venueMarketOutcomes": [
                        {"id": "limitless-yes", "label": "Yes"},
                        {"id": "limitless-no", "label": "No"},
                    ],
                },
            ],
        }

        async def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json={"data": [primary], "hasMore": False})

        client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        adapter = AggInstrumentDiscoveryAdapter(
            app_id="app-1",
            api_key="key-1",
            client=client,
        )

        markets = await adapter.discover_markets(InstrumentDiscoveryQuery(limit=10))

        assert {str(market.id) for market in markets} == {"agg-poly-1", "agg-limitless-1"}
        await adapter.close()

    asyncio.run(run_test())
