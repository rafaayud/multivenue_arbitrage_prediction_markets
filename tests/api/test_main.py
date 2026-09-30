"""Exercise main behavior in the api layer.

Responsibilities
----------------
- Verify main contracts, edge cases, and failure handling.
"""

from fastapi.testclient import TestClient

from prediction_markets.api.control_main import app as control_app
from prediction_markets.api.runtime import ArbitrageRuntime
from prediction_markets.api.trading_main import app as trading_app
from prediction_markets.application.markets.matching import MarketMatcher
from prediction_markets.application.pipeline import TradingPipeline
from prediction_markets.application.venue_health import VenueHealthService


def _route_paths(router) -> set[str]:
    """Collect paths from direct and lazily included FastAPI routers."""
    paths: set[str] = set()
    for route in router.routes:
        if hasattr(route, "path"):
            paths.add(route.path)
        elif included := getattr(route, "original_router", None):
            paths.update(_route_paths(included))
    return paths


def test_trading_lifespan_builds_event_driven_runtime() -> None:
    with TestClient(trading_app):
        runtime = trading_app.state.arbitrage_runtime

        assert isinstance(runtime, ArbitrageRuntime)
        assert isinstance(runtime.market_matcher, MarketMatcher)
        assert isinstance(runtime.pipeline, TradingPipeline)
        assert trading_app.state.market_worker is runtime
        assert trading_app.state.execution_runs is runtime.execution_runs
        assert isinstance(trading_app.state.venue_health_service, VenueHealthService)


def test_control_and_trading_apps_partition_process_owned_routes() -> None:
    """Keep queries outside trading and mutations beside their runtime owner."""
    control_paths = _route_paths(control_app)
    trading_paths = _route_paths(trading_app)

    assert {
        "/arbitrage-candidates",
        "/market-catalog",
        "/market-matches",
        "/arbitrage-opportunities",
        "/orders",
        "/pnl",
        "/ws/execution-events",
    } <= control_paths
    assert {
        "/runtime/status",
        "/runtime/start",
        "/arbitrage-candidates/monitor",
        "/execution-journals/{execution_id}/complete",
        "/trading-runs",
        "/ws/arbitrage-signals",
    } <= trading_paths

    assert "/runtime/status" not in control_paths
    assert "/trading-runs" not in control_paths
    assert "/arbitrage-candidates" not in trading_paths
    assert "/orders" not in trading_paths
