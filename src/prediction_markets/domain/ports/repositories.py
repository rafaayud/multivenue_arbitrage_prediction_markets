"""Define the repositories boundary required by the domain.

Responsibilities
----------------
- Specify infrastructure-neutral contracts for external effects.
"""

from abc import ABC, abstractmethod

from prediction_markets.domain.shared.value_objects import (
    ClientOrderID,
    ContractID,
    OrderID,
    PositionID,
    TradeID,
)
from prediction_markets.domain.trading.entities import OrderSnapshot, Position, Trade
from prediction_markets.domain.trading.entities import ArbitrageExecutionJournal
from prediction_markets.domain.trading.entities import ExposureRecovery


class PositionRepository(ABC):
    """Persist derived portfolio positions and expose unresolved open positions."""

    @abstractmethod
    def get(self, position_id: PositionID) -> Position | None:
        """Return the persisted position for an identifier, or `None` when absent."""
        ...

    @abstractmethod
    def save(self, position: Position) -> None:
        """Insert or replace the supplied position."""
        ...

    @abstractmethod
    def list_open(self) -> tuple[Position, ...]:
        """Return persisted open position records."""
        ...


class OrderRepository(ABC):
    """Persist order snapshots for idempotent client- and venue-id reconciliation."""

    @abstractmethod
    def get_by_client_order_id(
        self,
        client_order_id: ClientOrderID,
    ) -> OrderSnapshot | None:
        """Return an order by client identifier, or `None` when absent."""
        ...

    @abstractmethod
    def get_by_venue_order_id(self, order_id: OrderID) -> OrderSnapshot | None:
        """Return an order by venue identifier, or `None` when absent."""
        ...

    @abstractmethod
    def save(self, order: OrderSnapshot) -> None:
        """Insert or replace the supplied order."""
        ...

    @abstractmethod
    def list_open(self) -> tuple[OrderSnapshot, ...]:
        """Return persisted open order records."""
        ...


class TradeRepository(ABC):
    """Persist executed fills independently from their originating order snapshots."""

    @abstractmethod
    def get(self, trade_id: TradeID) -> Trade | None:
        """Return the persisted trade for an identifier, or `None` when absent."""
        ...

    @abstractmethod
    def save(self, trade: Trade) -> None:
        """Insert or replace the supplied trade."""
        ...

    @abstractmethod
    def list_by_order(self, order_id: OrderID) -> tuple[Trade, ...]:
        """Return trades associated with the supplied order."""
        ...

    @abstractmethod
    def list_by_contract(self, contract_id: ContractID) -> tuple[Trade, ...]:
        """Return trades associated with the supplied contract."""
        ...


class ExposureRecoveryRepository(ABC):
    """Persist unmatched exposure so recovery survives process restarts."""

    @abstractmethod
    def get(self, recovery_id: str) -> ExposureRecovery | None:
        """Return the persisted exposurerecovery for an identifier, or `None` when absent."""
        ...

    @abstractmethod
    def save(self, recovery: ExposureRecovery) -> None:
        """Insert or replace the supplied exposurerecovery."""
        ...

    @abstractmethod
    def list_unresolved(self) -> tuple[ExposureRecovery, ...]:
        """Return exposure recoveries that still require resolution."""
        ...


class ArbitrageExecutionJournalRepository(ABC):
    """Persist two-leg execution state used to fail closed across restarts."""

    @abstractmethod
    def claim(self, journal: ArbitrageExecutionJournal) -> bool:
        """Atomically claim an opportunity; return false when it was already claimed."""
        ...

    @abstractmethod
    def get(self, execution_id: str) -> ArbitrageExecutionJournal | None:
        """Return the persisted arbitrageexecutionjournal for an identifier, or `None` when absent."""
        ...

    @abstractmethod
    def save(self, journal: ArbitrageExecutionJournal) -> None:
        """Insert or replace the supplied arbitrageexecutionjournal."""
        ...

    @abstractmethod
    def list_active(self) -> tuple[ArbitrageExecutionJournal, ...]:
        """Return execution journals that have not reached a terminal state."""
        ...
