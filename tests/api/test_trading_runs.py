"""Exercise trading runs behavior in the api layer.

Responsibilities
----------------
- Verify trading runs contracts, edge cases, and failure handling.
"""

import time
from decimal import Decimal
from threading import Event

from fastapi.testclient import TestClient
import pytest

from prediction_markets.api.dependencies import get_venue_health_service
from prediction_markets.api.models import TradingRunStart
from prediction_markets.api.trading.runner import LiveArbitrageConfig
from prediction_markets.api.trading_main import app
from prediction_markets.api.trading.run_manager import ExecutionRunManager
from prediction_markets.application.venue_health import VenueHealthReport
from prediction_markets.domain.shared.value_objects import Timestamp, VenueID
from prediction_markets.domain.venue_health import (
    VenueHealthSnapshot,
    VenueHealthStatus,
)


@pytest.fixture(autouse=True)
def trading_auth(monkeypatch):
    monkeypatch.setenv("TRADING_API_KEY", "test-trading-key")


def _headers() -> dict[str, str]:
    return {"X-Trading-Key": "test-trading-key"}


def _preflight(*, ready: bool = True) -> dict[str, object]:
    return {
        "ready": ready,
        "missing_credentials": [] if ready else ["LIMITLESS_API_KEY"],
        "database_ready": True,
        "active_journals": 0,
        "unresolved_recoveries": 0,
    }


def _body() -> dict[str, object]:
    return {
        "live": True,
        "confirmation": "LIVE",
        "underlyings": ["BTC", "ETH", "BNB"],
        "intervals_seconds": [3600, 86400],
    }


def test_default_trading_intervals_match_hourly_and_daily_monitoring() -> None:
    """Keep API defaults and runtime admission on the same enabled intervals."""
    request = TradingRunStart(live=True, confirmation="LIVE")
    assert request.intervals_seconds == (3600, 86400)
    assert LiveArbitrageConfig().engine_config().allowed_intervals_seconds == (3600, 86400)


@pytest.mark.parametrize("interval", [300, 900])
def test_rejects_removed_fast_trading_intervals(interval) -> None:
    """Reject stale clients requesting disabled fast cycles before starting a run."""
    with TestClient(app) as client:
        response = client.post(
            "/trading-runs",
            json={**_body(), "intervals_seconds": [interval]},
            headers=_headers(),
        )
    assert response.status_code == 422


def test_start_status_and_graceful_stop() -> None:
    started = Event()

    async def runner(config, stop_event):
        started.set()
        await stop_event.wait()

    manager = ExecutionRunManager(runner, _preflight)
    with TestClient(app) as client:
        app.state.execution_runs = manager

        response = client.post(
            "/trading-runs",
            json=_body(),
            headers=_headers(),
        )

        assert response.status_code == 202
        run_id = response.json()["id"]
        assert response.json()["status"] == "running"
        assert started.wait(timeout=1)
        assert client.get(
            f"/trading-runs/{run_id}",
            headers=_headers(),
        ).json()["status"] == "running"

        stopped = client.post(
            f"/trading-runs/{run_id}/stop",
            headers=_headers(),
        )

        assert stopped.status_code == 200
        assert stopped.json()["status"] == "stopping"
        for _ in range(20):
            current = client.get(
                f"/trading-runs/{run_id}",
                headers=_headers(),
            ).json()
            if current["status"] == "stopped":
                break
            time.sleep(0.01)
        assert current["status"] == "stopped"
        assert current["finished_at"] is not None


def test_rejects_start_when_preflight_fails() -> None:
    async def runner(config, stop_event):
        raise AssertionError("Runner must not start")

    manager = ExecutionRunManager(
        runner,
        lambda: _preflight(ready=False),
    )
    with TestClient(app) as client:
        app.state.execution_runs = manager

        response = client.post(
            "/trading-runs",
            json=_body(),
            headers=_headers(),
        )

    assert response.status_code == 409
    assert response.json()["detail"]["missing_credentials"] == [
        "LIMITLESS_API_KEY"
    ]


def test_rejects_start_when_a_venue_is_degraded() -> None:
    """Keep unhealthy venue checks outside the execution task."""
    started = Event()

    async def runner(config, stop_event):
        started.set()

    class _DegradedVenueService:
        async def get(self, *, force_refresh: bool = False) -> VenueHealthReport:
            now = Timestamp.now()
            return VenueHealthReport(
                generated_at=now,
                overall_status=VenueHealthStatus.DEGRADED,
                venues=(
                    VenueHealthSnapshot(
                        venue_id=VenueID("PREDICT"),
                        status=VenueHealthStatus.DEGRADED,
                        checked_at=now,
                        latency_ms=100,
                        source="test",
                        message="Rate limited",
                        error_type="RateLimitError",
                        http_status=429,
                        retryable=True,
                    ),
                ),
            )

    manager = ExecutionRunManager(runner, _preflight)
    app.dependency_overrides[get_venue_health_service] = _DegradedVenueService
    with TestClient(app) as client:
        app.state.execution_runs = manager
        preflight = client.post(
            "/trading-runs/preflight",
            headers=_headers(),
        )
        response = client.post(
            "/trading-runs",
            json=_body(),
            headers=_headers(),
        )
        override = client.post(
            "/trading-runs",
            json={**_body(), "allow_degraded_venues": True},
            headers=_headers(),
        )

    assert preflight.json()["ready"] is False
    assert preflight.json()["venue_health_status"] == "degraded"
    assert response.status_code == 409
    assert response.json()["detail"]["venue_health_status"] == "degraded"
    assert "RateLimitError, HTTP 429, retryable" in response.json()["detail"][
        "venue_health_issues"
    ][0]
    assert override.status_code == 202
    assert started.is_set()


def test_degraded_override_does_not_allow_an_unavailable_venue() -> None:
    """Keep complete venue outages non-overridable."""

    class _UnavailableVenueService:
        async def get(self, *, force_refresh: bool = False) -> VenueHealthReport:
            now = Timestamp.now()
            return VenueHealthReport(
                generated_at=now,
                overall_status=VenueHealthStatus.DEGRADED,
                venues=(
                    VenueHealthSnapshot(
                        venue_id=VenueID("POLYMARKET"),
                        status=VenueHealthStatus.UNAVAILABLE,
                        checked_at=now,
                        latency_ms=None,
                        source="test",
                        message="Connection timed out",
                        error_type="ConnectTimeout",
                        retryable=True,
                    ),
                    VenueHealthSnapshot(
                        venue_id=VenueID("PREDICT"),
                        status=VenueHealthStatus.OPERATIONAL,
                        checked_at=now,
                        latency_ms=50,
                        source="test",
                        message="reachable",
                        http_status=200,
                    ),
                ),
            )

    async def runner(config, stop_event):
        raise AssertionError("Runner must not start")

    manager = ExecutionRunManager(runner, _preflight)
    app.dependency_overrides[get_venue_health_service] = _UnavailableVenueService
    with TestClient(app) as client:
        app.state.execution_runs = manager
        response = client.post(
            "/trading-runs",
            json={**_body(), "allow_degraded_venues": True},
            headers=_headers(),
        )

    assert response.status_code == 409
    assert "unavailable" in response.json()["detail"]["message"]


def test_rejects_two_simultaneous_live_runs() -> None:
    async def runner(config, stop_event):
        await stop_event.wait()

    manager = ExecutionRunManager(runner, _preflight)
    with TestClient(app) as client:
        app.state.execution_runs = manager
        first = client.post(
            "/trading-runs",
            json=_body(),
            headers=_headers(),
        )

        second = client.post(
            "/trading-runs",
            json=_body(),
            headers=_headers(),
        )

        assert first.status_code == 202
        assert second.status_code == 409
        client.post(
            f"/trading-runs/{first.json()['id']}/stop",
            headers=_headers(),
        )


def test_short_run_stays_preparing_until_runtime_marks_it_ready() -> None:
    """Expose collateral preparation before the engine can execute."""

    async def runner(config, stop_event):
        await stop_event.wait()

    manager = ExecutionRunManager(runner, _preflight)
    with TestClient(app) as client:
        app.state.execution_runs = manager
        started = client.post(
            "/trading-runs",
            json={**_body(), "short_market_keys": ["cycle:BTC:3600"]},
            headers=_headers(),
        )

        assert started.json()["status"] == "preparing"
        manager.mark_running()
        assert client.get("/trading-runs/current").json()["status"] == "running"
        client.post(
            f"/trading-runs/{started.json()['id']}/stop",
            headers=_headers(),
        )


def test_requires_explicit_live_confirmation() -> None:
    with TestClient(app) as client:
        response = client.post(
            "/trading-runs",
            json={**_body(), "confirmation": "yes"},
            headers=_headers(),
        )

    assert response.status_code == 422


def test_start_passes_execution_count_and_venue_budgets_to_runner() -> None:
    started = Event()
    received = {}

    async def runner(config, _stop_event):
        received["config"] = config
        started.set()

    manager = ExecutionRunManager(runner, _preflight)
    with TestClient(app) as client:
        app.state.execution_runs = manager
        response = client.post(
            "/trading-runs",
            json={
                **_body(),
                "max_arbitrages": 3,
                "max_concurrent_arbitrages": 2,
                "polymarket_max_notional": 10,
                "limitless_max_notional": 10,
                "predict_max_notional": 10,
                "predict_limit_slippage_ticks": 0,
                "predict_use_edge_budget": True,
                "short_market_keys": ["cycle:BTC:3600"],
            },
            headers=_headers(),
        )

        assert response.status_code == 202
        assert started.wait(timeout=1)
        config = received["config"]
        assert config.max_arbitrages == 3
        assert config.max_concurrent_arbitrages == 2
        assert config.polymarket_max_notional == Decimal("10")
        assert config.limitless_max_notional == Decimal("10")
        assert config.predict_max_notional == Decimal("10")
        assert config.predict_limit_slippage_ticks == 0
        assert config.predict_use_edge_budget is True
        assert config.short_market_keys == ("cycle:BTC:3600",)
        assert response.json()["short_market_keys"] == ["cycle:BTC:3600"]


def test_preflight_endpoint_is_read_only() -> None:
    async def runner(config, stop_event):
        raise AssertionError("Read-only preflight must not start the runner")

    manager = ExecutionRunManager(
        runner,
        preflight_check=lambda: _preflight(ready=False),
    )
    with TestClient(app) as client:
        app.state.execution_runs = manager

        response = client.post(
            "/trading-runs/preflight",
            headers=_headers(),
        )

    assert response.status_code == 200
    assert response.json()["ready"] is False


def test_current_run_restores_trading_toggle_state() -> None:
    async def runner(config, stop_event):
        await stop_event.wait()

    manager = ExecutionRunManager(runner, _preflight)
    with TestClient(app) as client:
        app.state.execution_runs = manager
        assert client.get("/trading-runs/current").json() is None

        started = client.post(
            "/trading-runs",
            json=_body(),
            headers=_headers(),
        )
        current = client.get("/trading-runs/current")

        assert current.status_code == 200
        assert current.json()["id"] == started.json()["id"]
        assert current.json()["status"] == "running"
        client.post(
            f"/trading-runs/{started.json()['id']}/stop",
            headers=_headers(),
        )


def test_rejects_missing_trading_key() -> None:
    with TestClient(app) as client:
        response = client.post("/trading-runs/preflight")

    assert response.status_code == 401


def test_browser_session_authenticates_trading_without_exposing_key() -> None:
    async def runner(config, stop_event):
        await stop_event.wait()

    manager = ExecutionRunManager(runner, _preflight)
    with TestClient(app) as client:
        app.state.execution_runs = manager
        session = client.post(
            "/trading-runs/session",
            json={"trading_key": "test-trading-key"},
        )

        assert session.status_code == 200
        cookie = session.cookies["prediction_markets_trading"]
        assert cookie != "test-trading-key"
        assert "HttpOnly" in session.headers["set-cookie"]
        assert client.get("/trading-runs/session").json() == {
            "authenticated": True,
        }

        started = client.post("/trading-runs", json=_body())
        assert started.status_code == 202
        client.post(f"/trading-runs/{started.json()['id']}/stop")


def test_browser_session_rejects_invalid_key() -> None:
    with TestClient(app) as client:
        response = client.post(
            "/trading-runs/session",
            json={"trading_key": "wrong"},
        )

    assert response.status_code == 401


def test_trading_session_check_rejects_missing_cookie() -> None:
    with TestClient(app) as client:
        response = client.get("/trading-runs/session")

    assert response.status_code == 401
