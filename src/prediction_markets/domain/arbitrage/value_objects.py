"""Define validated value objects for the arbitrage domain.

Responsibilities
----------------
- Enforce domain invariants at construction time.
"""

from dataclasses import dataclass
from decimal import Decimal

from prediction_markets.domain.orderbook.value_objects import OrderBookLevel
from prediction_markets.domain.shared.value_objects import ContractID, Quantity, Timestamp
from prediction_markets.domain.trading.enums import OrderSide


@dataclass(frozen=True, slots=True)
class ArbitrageOpportunity:
    """Capture an immutable two-contract arbitrage observation.

    Notes
    -----
    - Prices and quantities come from the observed order-book levels; `skew_ns` is measured in nanoseconds.
    - Fee amounts are conservative values in the calculators' common settlement unit.
    """
    left_contract_id: ContractID
    right_contract_id: ContractID
    side: OrderSide
    left_level: OrderBookLevel
    right_level: OrderBookLevel
    quantity: Quantity
    gross_edge: Decimal
    net_edge: Decimal
    skew_ns: int
    detected_at: Timestamp
    fee_per_contract: Decimal = Decimal("0")
    total_fees: Decimal = Decimal("0")
