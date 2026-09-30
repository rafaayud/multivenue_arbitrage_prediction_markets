"""Exercise the AGG candidate listing endpoint."""

from datetime import datetime, timezone
from decimal import Decimal

from fastapi.testclient import TestClient

from prediction_markets.api.dependencies import (
    get_agg_market_explorer,
    get_arbitrage_candidate_service,
    get_arbitrage_runtime,
)
from prediction_markets.api.control_main import app as control_app
from prediction_markets.api.trading_main import app as trading_app
from prediction_markets.application.markets.arbitrage_candidates import (
    ArbitrageCandidateQuery,
)
from prediction_markets.domain.ports.arbitrage_stream import (
    ArbitrageReturn,
    ArbitrageVenueMarket,
)
from prediction_markets.domain.shared.value_objects import (
    EventID,
    MarketID,
    OutcomeID,
    Timestamp,
    VenueID,
)


class _Service:
    """Return a fixed candidate and record the application query."""

    def __init__(self) -> None:
        self.query: ArbitrageCandidateQuery | None = None

    async def execute(self, query: ArbitrageCandidateQuery):
        self.query = query
        return (
            ArbitrageReturn(
                market_id=MarketID("poly-market-1"),
                venue_event_id=EventID("agg-event-1"),
                return_rate=Decimal("0.025"),
                observed_at=Timestamp(
                    datetime(2026, 8, 6, 12, 0, tzinfo=timezone.utc),
                ),
                event_title="Bitcoin milestone",
                starts_at=Timestamp(
                    datetime(2026, 8, 6, 11, 0, tzinfo=timezone.utc),
                ),
                ends_at=Timestamp(
                    datetime(2026, 8, 6, 13, 0, tzinfo=timezone.utc),
                ),
                volume_usd=Decimal("1000"),
                liquidity_usd=Decimal("250"),
                liquidity_tier="deep",
                markets=(
                    ArbitrageVenueMarket(
                        venue_id=VenueID("POLYMARKET"),
                        market_id=MarketID("poly-market-1"),
                        external_market_id="condition-1",
                        yes_outcome_id=OutcomeID("poly-yes"),
                        no_outcome_id=OutcomeID("poly-no"),
                        title="Will BTC close above $100k?",
                        volume_usd=Decimal("1000"),
                    ),
                ),
            ),
        )


def test_lists_agg_candidates_as_json() -> None:
    service = _Service()
    control_app.dependency_overrides[get_arbitrage_candidate_service] = lambda: service

    try:
        with TestClient(control_app) as client:
            response = client.get(
                "/arbitrage-candidates",
                params=[
                    ("min_return", "0.01"),
                    ("limit", "10"),
                    ("topics", "crypto"),
                    ("search_text", "bitcoin"),
                    ("live_only", "true"),
                    ("min_time_to_close_seconds", "300"),
                ],
            )
    finally:
        control_app.dependency_overrides.pop(get_arbitrage_candidate_service, None)

    assert response.status_code == 200
    assert response.json() == [
        {
            "market_id": "poly-market-1",
            "title": "Will BTC close above $100k?",
            "event_title": "Bitcoin milestone",
            "venue_event_id": "agg-event-1",
            "return_rate": "0.025",
            "observed_at": "2026-08-06T12:00:00Z",
            "starts_at": "2026-08-06T11:00:00Z",
            "ends_at": "2026-08-06T13:00:00Z",
            "volume_usd": "1000",
            "liquidity_usd": "250",
            "liquidity_tier": "deep",
            "markets": [
                {
                    "venue_id": "POLYMARKET",
                    "market_id": "poly-market-1",
                    "external_market_id": "condition-1",
                    "yes_outcome_id": "poly-yes",
                    "no_outcome_id": "poly-no",
                    "title": "Will BTC close above $100k?",
                    "volume_usd": "1000",
                },
            ],
        },
    ]
    assert service.query is not None
    assert service.query.min_return == Decimal("0.01")
    assert service.query.limit == 10
    assert service.query.topics == ("crypto",)
    assert service.query.search_text == "bitcoin"
    assert service.query.live_only is True


class _Runtime:
    """Record regular market selections accepted by the monitor endpoint."""

    def __init__(self) -> None:
        self.selections = ()

    async def monitor_regular(self, selections):
        self.selections = selections
        candidate = type(
            "Candidate",
            (),
            {
                "key": (("LIMITLESS", "limitless-1"), ("POLYMARKET", "poly-1")),
                "markets": (
                    type("Market", (), {"venue_id": VenueID("POLYMARKET")})(),
                    type("Market", (), {"venue_id": VenueID("LIMITLESS")})(),
                ),
            },
        )()
        return candidate, 2


class _StaleRuntime:
    async def monitor_regular(self, _selections):
        raise LookupError("Limitless market ID not found: 361542")


class _Explorer:
    """Return the fixed AGG record without applying opportunity thresholds."""

    def __init__(self) -> None:
        self.query: dict[str, object] | None = None

    async def list_markets(self, **kwargs):
        self.query = kwargs
        return await _Service().execute(ArbitrageCandidateQuery())


def test_lists_market_catalog_without_minimum_return() -> None:
    """Expose market groups through the catalog endpoint and its own filters."""
    explorer = _Explorer()
    control_app.dependency_overrides[get_agg_market_explorer] = lambda: explorer

    try:
        with TestClient(control_app) as client:
            response = client.get(
                "/market-catalog",
                params={
                    "limit": 25,
                    "topics": "esports",
                    "search_text": "valorant",
                    "live_only": True,
                },
            )
    finally:
        control_app.dependency_overrides.pop(get_agg_market_explorer, None)

    assert response.status_code == 200
    assert response.json()[0]["market_id"] == "poly-market-1"
    assert explorer.query == {
        "limit": 25,
        "topics": ("esports",),
        "search_text": "valorant",
        "live_only": True,
    }


def test_monitors_selected_agg_candidate(monkeypatch) -> None:
    """Resolve candidate selections through the protected runtime action."""
    runtime = _Runtime()
    monkeypatch.setenv("TRADING_API_KEY", "test-key")
    trading_app.dependency_overrides[get_arbitrage_runtime] = lambda: runtime

    try:
        with TestClient(trading_app) as client:
            response = client.post(
                "/arbitrage-candidates/monitor",
                headers={"X-Trading-Key": "test-key"},
                json={
                    "markets": [
                        {
                            "venue_id": "POLYMARKET",
                            "external_market_id": "poly-1",
                            "search_text": "Bitcoin milestone",
                        },
                        {
                            "venue_id": "LIMITLESS",
                            "external_market_id": "limitless-1",
                            "search_text": "Bitcoin milestone",
                        },
                    ],
                },
            )
    finally:
        trading_app.dependency_overrides.pop(get_arbitrage_runtime, None)

    assert response.status_code == 200
    assert response.json() == {
        "monitor_key": (
            "regular:(('LIMITLESS', 'limitless-1'), "
            "('POLYMARKET', 'poly-1'))"
        ),
        "pair_count": 2,
        "venue_ids": ["POLYMARKET", "LIMITLESS"],
    }
    assert [str(selection.venue_id) for selection in runtime.selections] == [
        "POLYMARKET",
        "LIMITLESS",
    ]
    assert {selection.search_text for selection in runtime.selections} == {
        "Bitcoin milestone",
    }


def test_monitor_reports_stale_market_as_client_error(monkeypatch) -> None:
    """Translate a vanished native market into a retryable client response."""
    monkeypatch.setenv("TRADING_API_KEY", "test-key")
    trading_app.dependency_overrides[get_arbitrage_runtime] = lambda: _StaleRuntime()

    try:
        with TestClient(trading_app) as client:
            response = client.post(
                "/arbitrage-candidates/monitor",
                headers={"X-Trading-Key": "test-key"},
                json={
                    "markets": [
                        {
                            "venue_id": "POLYMARKET",
                            "external_market_id": "poly-1",
                        },
                        {
                            "venue_id": "LIMITLESS",
                            "external_market_id": "361542",
                        },
                    ],
                },
            )
    finally:
        trading_app.dependency_overrides.pop(get_arbitrage_runtime, None)

    assert response.status_code == 422
    assert response.json()["detail"] == (
        "Selected market is no longer available: "
        "Limitless market ID not found: 361542"
    )
