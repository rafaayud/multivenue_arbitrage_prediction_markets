"""Verify the public outcome explorer is read-only and handles upstream failures."""

import json

import httpx
from fastapi import FastAPI
from fastapi.testclient import TestClient

from prediction_markets.api.routers.hyperliquid import router
from prediction_markets.infrastructure.venues.hyperliquid.catalog import HyperliquidMarketCatalog


def test_disabled_catalog_and_no_mutation_routes():
    """Expose disabled state without creating any mutation endpoint."""
    app = FastAPI()
    app.include_router(router)
    with TestClient(app) as client:
        assert client.get("/hyperliquid/markets").json() == {
            "enabled": False, "network": None, "trading_enabled": False, "markets": [],
        }
        assert client.get("/hyperliquid/markets/1/books").status_code == 503
        assert client.get("/hyperliquid/markets?search_text=%20").status_code == 422
        assert client.post("/hyperliquid/markets").status_code == 405


def test_catalog_and_books_use_only_public_info_requests():
    """Preserve native book identities and reject unlisted outcome ids."""
    requests = []

    def respond(request):
        assert request.url.path == "/info"
        body = json.loads(request.content)
        requests.append(body)
        payload = {
            "outcomeMeta": {"outcomes": [{
                "outcome": 3, "name": "Example binary", "description": "expiry:20991231-0000",
                "sideSpecs": [{"name": "Yes"}, {"name": "No"}],
            }], "questions": []},
            "outcomeTemplates": [],
            "l2Book": {"coin": body.get("coin"), "time": 1793376000123,
                       "levels": [[{"px": "0.11" if body.get("coin") == "#30" else "0.25", "sz": "50"}], []]},
        }[body["type"]]
        return httpx.Response(200, json=payload)

    upstream = httpx.AsyncClient(transport=httpx.MockTransport(respond))
    app = FastAPI()
    app.state.hyperliquid_catalog = HyperliquidMarketCatalog(client=upstream)
    app.include_router(router)
    with TestClient(app) as client:
        response = client.get("/hyperliquid/markets?search_text=Example")
        assert response.status_code == 200
        assert response.json()["markets"][0]["no_contract_id"] == "hyperliquid:3:no"
        assert response.json()["trading_enabled"] is False
        books = client.get("/hyperliquid/markets/3/books").json()
        assert books["books"][0]["bids"] == [["0.11", "50"]]
        assert books["books"][1]["bids"] == [["0.25", "50"]]
        assert books["trading_enabled"] is False
        assert client.get("/hyperliquid/markets/999/books").status_code == 404
        assert client.get("/hyperliquid/markets/-1/books").status_code == 422
        assert len(requests) == 4
        client.portal.call(upstream.aclose)


def test_upstream_failure_is_not_an_empty_successful_catalog():
    """Report source unavailability instead of hiding it behind empty results."""
    upstream = httpx.AsyncClient(transport=httpx.MockTransport(lambda request: httpx.Response(503)))
    app = FastAPI()
    app.state.hyperliquid_catalog = HyperliquidMarketCatalog(client=upstream)
    app.include_router(router)
    with TestClient(app) as client:
        assert client.get("/hyperliquid/markets").status_code == 502
        client.portal.call(upstream.aclose)


def test_enabled_data_does_not_admit_hyperliquid_to_trading(monkeypatch):
    """Reject Hyperliquid monitoring even when its public explorer is enabled."""
    from prediction_markets.api.auth import require_trading_key
    from prediction_markets.api.control_main import app as control_app
    from prediction_markets.api.trading_main import app as trading_app

    monkeypatch.setenv("HYPERLIQUID_ENABLED", "true")
    monkeypatch.setenv("HYPERLIQUID_NETWORK", "testnet")
    with TestClient(control_app):
        assert control_app.state.hyperliquid_catalog.settings.network == "testnet"
    trading_app.dependency_overrides[require_trading_key] = lambda: None
    try:
        with TestClient(trading_app) as client:
            result = client.post("/arbitrage-candidates/monitor", json={"markets": [
                {"venue_id": "HYPERLIQUID", "external_market_id": "19990"},
                {"venue_id": "POLYMARKET", "external_market_id": "1"},
            ]})
            assert result.status_code == 422
            assert "supported" in result.json()["detail"]
    finally:
        trading_app.dependency_overrides.pop(require_trading_key, None)
