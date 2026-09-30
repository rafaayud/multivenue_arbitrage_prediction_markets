"""Verify Predict adapter configuration parsing."""

import pytest

from prediction_markets.infrastructure.venues.predict.config import (
    predict_transaction_gas_price_wei,
)


def test_transaction_gas_price_uses_floor_and_retains_higher_suggestion() -> None:
    """Apply the configured floor without lowering the RPC recommendation."""
    assert predict_transaction_gas_price_wei(10_000_000, "0.05") == 50_000_000
    assert predict_transaction_gas_price_wei(300_000_000, "0.05") == 300_000_000


@pytest.mark.parametrize("value", ("0", "-1", "NaN", "invalid", "0.0000000001"))
def test_transaction_gas_price_rejects_invalid_floor(value: str) -> None:
    """Reject unusable or sub-wei gas-price configuration."""
    with pytest.raises(ValueError):
        predict_transaction_gas_price_wei(value=value)
