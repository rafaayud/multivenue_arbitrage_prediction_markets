"""Public API for the segregated arbitrage runtime package."""

from prediction_markets.api.runtime.facade import ArbitrageRuntime
from prediction_markets.api.runtime.persistence import _configured_alert_recipients

__all__ = ["ArbitrageRuntime"]
