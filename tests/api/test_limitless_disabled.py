"""Verify that disabling Limitless preserves other venue checks."""

from unittest.mock import Mock

from prediction_markets.api.trading import runner
from prediction_markets.api.runtime import inventory


def test_disabled_limitless_does_not_require_credentials(monkeypatch):
    """Keep active venue requirements and fail closed without a database."""
    for name in ("POLYMARKET_PK", "PREDICT_API_KEY", "LIMITLESS_PRIVATE_KEY", "LIMITLESS_API_KEY"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("LIMITLESS_ENABLED", "false")
    monkeypatch.setattr(runner, "database_url", Mock(side_effect=RuntimeError("No database")))
    report = runner.preflight()
    assert report["ready"] is False
    assert "POLYMARKET_PK" in report["missing_credentials"]
    assert "PREDICT_API_KEY" in report["missing_credentials"]
    assert not any(k.startswith("LIMITLESS_") for k in report["missing_credentials"])
    monkeypatch.setenv("LIMITLESS_ENABLED", "true")
    assert "LIMITLESS_PRIVATE_KEY" in runner.preflight()["missing_credentials"]


def test_disabled_limitless_never_constructs_inventory_adapter(monkeypatch):
    """Avoid constructing a signer for the disabled venue."""
    monkeypatch.setenv("LIMITLESS_ENABLED", "0")
    poly, predict = Mock(), Mock()
    monkeypatch.setattr(inventory, "PolymarketOutcomeInventoryAdapter", lambda: poly)
    monkeypatch.setattr(inventory, "PredictOutcomeInventoryAdapter", lambda: predict)
    forbidden = Mock(side_effect=AssertionError("Disabled adapter constructed"))
    monkeypatch.setattr(inventory, "LimitlessOutcomeInventoryAdapter", forbidden)
    assert set(map(str, inventory._build_short_inventory_adapters())) == {"POLYMARKET", "PREDICT"}
    forbidden.assert_not_called()
