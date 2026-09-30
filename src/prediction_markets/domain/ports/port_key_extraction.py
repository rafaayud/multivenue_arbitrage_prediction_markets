"""Define the port key extraction boundary required by the domain.

Responsibilities
----------------
- Specify infrastructure-neutral contracts for external effects.
"""

from abc import ABC, abstractmethod

from prediction_markets.domain.market_matching.value_objects import Underlying, UpDownMarketKey
from prediction_markets.domain.markets.entities import Market

class KeyExtractionPort(ABC):
    """Define the asynchronous contract for deriving comparable market keys."""

    @abstractmethod
    async def extract_key(
        self,
        markets: tuple[Market, ...],
        *,
        underlying: Underlying) -> tuple[tuple[Market, UpDownMarketKey], ...]:
        """Return keyed markets only; skip unsupported or incomplete markets."""
        ...
