"""Configure isolated process resources for the test suite."""

import pytest


@pytest.fixture(autouse=True)
def isolated_runtime(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    """Keep API lifespans offline and give every test an isolated journal."""
    monkeypatch.setenv("MARKET_WORKER_ENABLED", "0")
    monkeypatch.setenv("POSTGRES_PROJECTOR_ENABLED", "0")
    monkeypatch.setenv("PREDICT_ONCHAIN_CANCEL_ENABLED", "0")
    monkeypatch.setenv("JOURNAL_PATH", str(tmp_path / "trading.log"))
