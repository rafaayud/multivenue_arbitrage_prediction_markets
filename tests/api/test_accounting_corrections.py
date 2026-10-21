"""Verify accounting corrections use chronological economics, not recovery order."""

from decimal import Decimal
from types import SimpleNamespace

import pytest

from prediction_markets.api.runtime.accounting import RuntimeAccounting
from prediction_markets.application.events import InventoryOperationRecorded, TradeRecorded
from prediction_markets.application.state import EventDispatcher, TradingState
from prediction_markets.domain.outcome_inventory import (
    InventoryOperationID, InventoryOperationReference, InventoryOperationSnapshot,
    InventoryOperationStatus, OutcomeInventoryAction, OutcomeInventoryBalance,
)
from prediction_markets.domain.shared.value_objects import (
    ContractID, Currency, MarketID, Money, PortfolioID, Price, Quantity, Timestamp,
    TradeID, VenueID,
)
from prediction_markets.domain.trading.entities import Trade
from prediction_markets.domain.trading.enums import OrderSide
from prediction_markets.infrastructure.binary_journal import BinaryJournal


def test_correction_preserves_split_and_trade_order(tmp_path) -> None:
    """Interleaved splits and fills must retain their historical WAC and profit."""
    venue, portfolio = VenueID("PREDICT"), PortfolioID("bot")
    now = Timestamp.now()
    zero = Money(Decimal("0"), Currency("USD"))
    balance = OutcomeInventoryBalance(
        venue, MarketID("market"), Quantity(Decimal("0")), Quantity(Decimal("0")),
        Money(Decimal("20"), Currency("USD")), now, ContractID("yes"), ContractID("no"),
    )

    def split(identity):
        """Create a confirmed ten-dollar split with explicit economic quantities."""
        return InventoryOperationRecorded(InventoryOperationSnapshot(
            InventoryOperationReference(
                venue, InventoryOperationID(identity), b"split",
                quantity=Quantity(Decimal("10")), action=OutcomeInventoryAction.SPLIT,
                portfolio_id=portfolio, balance_before=balance,
            ),
            InventoryOperationStatus.CONFIRMED, now,
            quantity=Quantity(Decimal("10")), yes_quantity=Quantity(Decimal("10")),
            no_quantity=Quantity(Decimal("10")), collateral=Money(Decimal("10"), Currency("USD")),
            fee=zero, fee_settlement_cost=zero,
        ))

    buy = Trade(
        TradeID("buy"), ContractID("yes"), venue, OrderSide.BUY,
        Quantity(Decimal("10")), Price(Decimal("0.8")), now,
        portfolio_id=portfolio, fee_settlement_cost=zero,
    )
    sell = Trade(
        TradeID("sell"), ContractID("yes"), venue, OrderSide.SELL,
        Quantity(Decimal("10")), Price(Decimal("0.9")), now,
        portfolio_id=portfolio, fee_settlement_cost=zero,
    )
    state = TradingState()
    journal = BinaryJournal(tmp_path / "accounting.log")
    try:
        for event in (split("first"), TradeRecorded(buy), TradeRecorded(sell), split("second")):
            journal.append(event)
            state.apply(event)
        accounting = RuntimeAccounting(journal, state, EventDispatcher(state), None, {})
        correction = accounting.record_accounting_correction(buy, "Rebuild from original sequence")
        position = correction.resulting_position
        assert position.quantity.value == 20
        assert position.average_price.value == Decimal("0.575")
        assert position.realized_pnl == Decimal("2.5")
        assert state.positions[position.id] == position
    finally:
        journal.close()


def test_correction_rejects_pruned_history_before_writing() -> None:
    """A compact recovery snapshot cannot prove original cost allocation."""
    journal = SimpleNamespace(retained_through_sequence=1)
    state = TradingState()
    accounting = RuntimeAccounting(journal, state, EventDispatcher(state), None, {})
    with pytest.raises(RuntimeError, match="complete chronological journal"):
        accounting.record_accounting_correction(None, "Must not mutate accounting")
    assert not state.accounting_corrections
