"""Define the taker fees boundary required by the domain.

Responsibilities
----------------
- Specify infrastructure-neutral contracts for external effects.
"""

from abc import ABC, abstractmethod
from prediction_markets.domain.shared.value_objects import ContractID, Price, Quantity
from prediction_markets.domain.trading.enums import OrderSide
from prediction_markets.domain.trading.value_objects import TradingFee


class TakerFeeCalculatorPort(ABC):
    """Calculate the fee charged for an immediately executed order."""

    @abstractmethod
    async def prepare(self, contract_ids: tuple[ContractID, ...]) -> None:
        """
        Load or refresh fee schedules before fee calculations.

        Parameters
        ----------
        contract_ids
            Contracts whose venue fee schedules must be available locally.
        """
        ...

    @abstractmethod
    def calculate(
        self,
        contract_id: ContractID,
        price: Price,
        quantity: Quantity,
        side: OrderSide,
    ) -> TradingFee:
        """
        Return the exact fee amount for one immediately executed order.

        Parameters
        ----------
        contract_id
            Contract whose venue fee schedule applies.
        price
            Decimal execution price.
        quantity
            Decimal executed quantity.
        side
            Buy or sell side affecting the fee rule.

        Returns
        -------
        TradingFee
            Native charge and its conservative value in the common settlement unit.
        """
        ...
