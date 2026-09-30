"""Define the market data boundary required by the domain.

Responsibilities
----------------
- Specify infrastructure-neutral contracts for external effects.
"""

from abc import ABC, abstractmethod

from prediction_markets.domain.contracts.entities import BinaryContract
from prediction_markets.domain.markets.entities import Market
from prediction_markets.domain.orderbook.entities import OrderBook
from prediction_markets.domain.shared.value_objects import ContractID, MarketID, VenueID


class MarketDataPort(ABC):
    """Expose normalized market and order-book reads without venue-specific payloads."""

    @abstractmethod
    async def list_markets(self, venue_id: VenueID | None = None) -> tuple[Market, ...]:
        """
        List normalized markets exposed by the venue.

        Parameters
        ----------
        venue_id
            Optional domain venue filter; ``None`` leaves filtering to the adapter.

        Returns
        -------
        tuple[Market, ...]
            A tuple of domain markets with venue-specific payload details removed.
        """
        ...

    @abstractmethod
    async def get_market(self, market_id: MarketID) -> Market | None:
        """
        Read one normalized market without leaking the venue payload shape.

        Parameters
        ----------
        market_id
            Domain market identifier to look up.

        Returns
        -------
        Market | None
            The market when found, otherwise ``None``.
        """
        ...

    @abstractmethod
    async def list_contracts(self, market_id: MarketID) -> tuple[BinaryContract, ...]:
        """
        List normalized outcome contracts belonging to one market.

        Parameters
        ----------
        market_id
            Domain market whose tradable outcomes are requested.

        Returns
        -------
        tuple[BinaryContract, ...]
            A tuple of binary contracts, possibly empty when the market is unavailable.
        """
        ...

    @abstractmethod
    async def get_order_book(self, contract_id: ContractID) -> OrderBook | None:
        """
        Read the latest normalized order book for one contract.

        Parameters
        ----------
        contract_id
            Domain contract identifier whose book is requested.

        Returns
        -------
        OrderBook | None
            The latest book, or ``None`` when the venue has no usable snapshot.
        """
        ...
