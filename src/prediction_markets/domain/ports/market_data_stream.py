"""Define the market data stream boundary required by the domain.

Responsibilities
----------------
- Specify infrastructure-neutral contracts for external effects.
"""

from abc import ABC, abstractmethod
from collections.abc import AsyncIterator

from prediction_markets.domain.orderbook.entities import OrderBook
from prediction_markets.domain.shared.value_objects import ContractID, Price, VenueID


class MarketDataStreamPort(ABC):
    """Stream normalized books while preserving the contract id attached to each update."""

    venue_id: VenueID | None = None

    async def stream_price(self, contract_id: ContractID) -> AsyncIterator[Price]:
        """
        Yield mid-prices derived from streamed books for one contract.

        Parameters
        ----------
        contract_id
            Contract whose book updates should be projected to prices.

        Yields
        ------
        Price
            A price only when the corresponding book has a calculable midpoint.
        """
        async for order_book in self.stream_order_book(contract_id):
            if (price := order_book.mid_price()) is not None:
                yield price

    async def stream_order_book(
        self,
        contract_id: ContractID,
    ) -> AsyncIterator[OrderBook]:
        """
        Filter the multi-contract stream down to one contract.

        Parameters
        ----------
        contract_id
            Contract identifier to retain from the underlying stream.

        Yields
        ------
        OrderBook
            Normalized books tagged with the requested contract id.
        """
        async for streamed_contract_id, order_book in self.stream_order_books(
            (contract_id,)
        ):
            if streamed_contract_id == contract_id:
                yield order_book

    @abstractmethod
    async def stream_order_books(
        self,
        contract_ids: tuple[ContractID, ...],
    ) -> AsyncIterator[tuple[ContractID, OrderBook]]:
        """
        Stream normalized updates for a set of contracts.

        Parameters
        ----------
        contract_ids
            Contracts to subscribe to; implementations may deduplicate them.

        Yields
        ------
        tuple[ContractID
            ``(contract_id, order_book)`` pairs until the transport closes or raises.
        """
        ...
