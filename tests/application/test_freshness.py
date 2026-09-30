"""Verify the source-age rollout boundary."""

import pytest

from prediction_markets.application.freshness import source_age_guard_enabled


def test_source_age_guard_environment_is_explicit(monkeypatch) -> None:
    """Accept boolean modes and reject ambiguous safety configuration."""
    monkeypatch.setenv("MARKET_DATA_ENFORCE_SOURCE_AGE", "false")
    assert source_age_guard_enabled() is False

    monkeypatch.setenv("MARKET_DATA_ENFORCE_SOURCE_AGE", "sometimes")
    with pytest.raises(ValueError, match="must be a boolean"):
        source_age_guard_enabled()
