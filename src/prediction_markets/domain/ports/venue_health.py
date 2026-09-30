"""Define the boundary for checking one venue's trading availability."""

from abc import ABC, abstractmethod

from prediction_markets.domain.venue_health import VenueHealthSnapshot


class VenueHealthPort(ABC):
    """Check one external venue without interacting with the trading hotpath."""

    @abstractmethod
    async def check(self) -> VenueHealthSnapshot:
        """Return the venue's current normalized health observation."""
        ...

    async def close(self) -> None:
        """Release adapter-owned resources when present."""
