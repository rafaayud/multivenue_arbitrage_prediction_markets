"""Define the boundary for reading diagnostic account PnL from one venue.

Responsibilities
----------------
- Normalize external account snapshots used only for reconciliation.
- Keep venue-reported values separate from the trade-derived accounting domain.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from decimal import Decimal

from prediction_markets.domain.shared.value_objects import (
    ContractID,
    MarketID,
    PositionID,
    Price,
    Timestamp,
    VenueID,
)


@dataclass(frozen=True, slots=True)
class VenuePnlPosition:
    """Represent one position reported by a venue account API.

    Notes
    -----
    This is an external reconciliation record, not the accounting
    :class:`~prediction_markets.domain.trading.entities.Position`.
    """

    venue_id: VenueID
    position_id: PositionID
    contract_id: ContractID | None
    market_id: MarketID | None
    title: str | None
    outcome: str | None
    quantity: Decimal
    average_entry_price: Price | None
    current_price: Price | None
    current_value_usd: Decimal
    realized_pnl_usd: Decimal | None
    unrealized_pnl_usd: Decimal | None
    total_pnl_usd: Decimal
    fees_usd: Decimal | None
    resolved: bool = False

    def __post_init__(self) -> None:
        values = (
            self.quantity,
            self.average_entry_price,
            self.current_price,
            self.current_value_usd,
            self.realized_pnl_usd,
            self.unrealized_pnl_usd,
            self.total_pnl_usd,
            self.fees_usd,
        )
        if any(value is not None and not value.is_finite() for value in values):
            raise ValueError("Position PnL values must be finite")
        if not str(self.position_id).strip():
            raise ValueError("Position id must be non-empty")
        if self.quantity < 0 or self.current_value_usd < 0:
            raise ValueError("Position quantity and current value cannot be negative")
        if self.fees_usd is not None and self.fees_usd < 0:
            raise ValueError("Position fees cannot be negative")
        for price in (self.average_entry_price, self.current_price):
            if price is not None and not Decimal("0") <= price <= Decimal("1"):
                raise ValueError("Binary position prices must be between zero and one")


@dataclass(frozen=True, slots=True)
class VenuePnlSnapshot:
    """Capture one external venue's current account PnL in USD."""

    venue_id: VenueID
    realized_pnl_usd: Decimal | None
    unrealized_pnl_usd: Decimal | None
    total_pnl_usd: Decimal
    fees_usd: Decimal | None
    observed_at: Timestamp
    source: str
    scope: str
    positions: tuple[VenuePnlPosition, ...] = ()

    def __post_init__(self) -> None:
        values = (
            self.realized_pnl_usd,
            self.unrealized_pnl_usd,
            self.total_pnl_usd,
            self.fees_usd,
        )
        if any(value is not None and not value.is_finite() for value in values):
            raise ValueError("PnL values must be finite")
        if self.fees_usd is not None and self.fees_usd < 0:
            raise ValueError("PnL fees cannot be negative")
        if not self.source.strip():
            raise ValueError("PnL source must be non-empty")
        if not self.scope.strip():
            raise ValueError("PnL scope must be non-empty")
        if any(position.venue_id != self.venue_id for position in self.positions):
            raise ValueError("PnL positions must belong to the snapshot venue")


class PnlPort(ABC):
    """Read venue-reported PnL for diagnostics and reconciliation only."""

    @abstractmethod
    async def fetch(self) -> VenuePnlSnapshot:
        """Fetch and normalize the latest venue account PnL.

        Returns
        -------
        VenuePnlSnapshot
            Immutable USD snapshot from the venue portfolio API.
        """
        ...

    async def close(self) -> None:
        """Release adapter-owned resources when present."""
