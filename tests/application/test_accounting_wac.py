"""Verify venue-scoped weighted-average position accounting."""

from datetime import datetime, timezone
from dataclasses import replace
from decimal import Decimal

from prediction_markets.application.execution.accounting import (
    _correction_event,
    apply_inventory_operation,
    apply_trade,
    rebuild_corrected_position,
)
from prediction_markets.application.events import (
    CashMovementRecorded,
    InventoryOperationRecorded,
    MarketSettlementRecorded,
    PositionUpdated,
    TradeRecorded,
)
from prediction_markets.application.state import TradingState
from prediction_markets.domain.outcome_inventory import (
    InventoryOperationID,
    InventoryOperationReference,
    InventoryOperationSnapshot,
    InventoryOperationStatus,
    InventorySubmissionResult,
    InventorySubmissionStatus,
    OutcomeInventoryAction,
    OutcomeInventoryBalance,
    OutcomeInventorySettlement,
)
from prediction_markets.domain.shared.value_objects import (
    ContractID,
    Currency,
    MarketID,
    Money,
    PortfolioID,
    PositionID,
    Price,
    Quantity,
    Timestamp,
    TradeID,
    VenueID,
)
from prediction_markets.domain.trading.entities import CashMovement, Portfolio, Trade
from prediction_markets.domain.trading.enums import (
    CashMovementKind,
    OrderSide,
    PositionSide,
)


_NOW = Timestamp(datetime(2026, 8, 26, tzinfo=timezone.utc))
_USD = Currency("USD")


def _trade(
    trade_id: str,
    side: OrderSide,
    quantity: str,
    price: str,
    *,
    venue: str = "POLYMARKET",
    portfolio: str = "bot",
    fee: str | None = "0",
) -> Trade:
    return Trade(
        id=TradeID(trade_id),
        contract_id=ContractID("contract-1"),
        venue_id=VenueID(venue),
        side=side,
        quantity=Quantity(Decimal(quantity)),
        price=Price(Decimal(price)),
        executed_at=_NOW,
        portfolio_id=PortfolioID(portfolio),
        fee_settlement_cost=(Money(Decimal(fee), _USD) if fee is not None else None),
    )


def test_wac_realizes_partial_close_and_carries_fee_total() -> None:
    opened = apply_trade(None, _trade("buy-1", OrderSide.BUY, "10", "0.40", fee="0.10"))
    closed = apply_trade(
        opened.position,
        _trade("buy-2", OrderSide.BUY, "10", "0.60", fee="0.20"),
    )
    result = apply_trade(
        closed.position,
        _trade("sell-1", OrderSide.SELL, "4", "0.80", fee="0.05"),
    )

    assert result.realized_pnl == Decimal("1.2")
    assert result.position.quantity == Quantity(Decimal("16"))
    assert result.position.side is PositionSide.LONG
    assert result.position.average_price == Price(Decimal("0.50"))
    assert result.position.realized_pnl == Decimal("1.2")
    assert result.position.fees == Money(Decimal("0.35"), _USD)


def test_wac_exact_close_and_long_to_short_flip() -> None:
    opened = apply_trade(None, _trade("buy-1", OrderSide.BUY, "10", "0.40"))
    flipped = apply_trade(
        opened.position,
        _trade("sell-1", OrderSide.SELL, "15", "0.60"),
    )

    assert flipped.realized_pnl == Decimal("2.0")
    assert flipped.position.side is PositionSide.SHORT
    assert flipped.position.quantity == Quantity(Decimal("5"))
    assert flipped.position.average_price == Price(Decimal("0.60"))

    closed = apply_trade(
        flipped.position,
        _trade("buy-2", OrderSide.BUY, "5", "0.50"),
    )
    assert closed.position.side is PositionSide.FLAT
    assert closed.position.quantity == Quantity(Decimal("0"))
    assert closed.realized_pnl == Decimal("0.5")


def test_accounting_correction_replays_later_partial_close() -> None:
    opened = replace(
        _trade("buy-1", OrderSide.BUY, "10", "0.40"),
        journal_sequence=1,
    )
    closed = replace(
        _trade("sell-1", OrderSide.SELL, "4", "0.80"),
        journal_sequence=2,
    )
    corrected = replace(opened, price=Price(Decimal("0.50")))

    position = rebuild_corrected_position(
        (TradeRecorded(opened), TradeRecorded(closed)),
        corrected,
    )

    assert position.quantity == Quantity(Decimal("6"))
    assert position.average_price == Price(Decimal("0.50"))
    assert position.realized_pnl == Decimal("1.20")


def test_short_unrealized_pnl_is_signed_and_portfolios_are_venue_scoped() -> None:
    short = apply_trade(
        None,
        _trade("sell-1", OrderSide.SELL, "6", "0.70", venue="PREDICT"),
    )
    marked = replace(short.position, current_price=Price(Decimal("0.40")))
    assert marked.unrealized_pnl == Decimal("1.8")

    other = apply_trade(
        None,
        _trade("buy-1", OrderSide.BUY, "2", "0.20", venue="POLYMARKET"),
    )
    state = TradingState()
    state.apply(PositionUpdated(marked))
    state.apply(PositionUpdated(other.position))

    assert set(state.portfolios) == {
        (VenueID("PREDICT"), PortfolioID("bot")),
        (VenueID("POLYMARKET"), PortfolioID("bot")),
    }
    assert state.portfolios[(VenueID("PREDICT"), PortfolioID("bot"))].positions == (marked,)


def test_missing_fee_is_flagged_in_result_and_portfolio() -> None:
    result = apply_trade(
        None,
        _trade("buy-unknown-fee", OrderSide.BUY, "1", "0.25", fee=None),
    )

    assert result.fees is None
    assert result.quality_flags == ("MISSING_FEES",)
    portfolio = Portfolio.from_positions((result.position,))
    assert portfolio.fees is None
    assert "MISSING_FEES" in portfolio.quality_flags


def _inventory_balance(yes: str, no: str, cash: str) -> OutcomeInventoryBalance:
    return OutcomeInventoryBalance(
        venue_id=VenueID("POLYMARKET"),
        market_id=MarketID("market-1"),
        yes=Quantity(Decimal(yes)),
        no=Quantity(Decimal(no)),
        collateral=Money(Decimal(cash), _USD),
        observed_at=_NOW,
        yes_contract_id=ContractID("yes"),
        no_contract_id=ContractID("no"),
    )


def _inventory_snapshot(
    operation_id: str,
    action: OutcomeInventoryAction,
    before: OutcomeInventoryBalance,
    after: OutcomeInventoryBalance,
) -> InventoryOperationSnapshot:
    reference = InventoryOperationReference(
        venue_id=before.venue_id,
        operation_id=InventoryOperationID(operation_id),
        recovery_data=operation_id.encode(),
        action=action,
        portfolio_id=PortfolioID("bot"),
        balance_before=before,
    )
    return InventoryOperationSnapshot(
        reference=reference,
        status=InventoryOperationStatus.CONFIRMED,
        updated_at=_NOW,
        fee_settlement_cost=Money(Decimal("0"), _USD),
    ).with_balance_change(after)


def test_inventory_operations_preserve_wac_and_realize_only_on_consumption() -> None:
    split = apply_inventory_operation(
        {},
        _inventory_snapshot(
            "split-1",
            OutcomeInventoryAction.SPLIT,
            _inventory_balance("0", "0", "10"),
            _inventory_balance("5", "5", "5"),
        ),
    )
    positions = {position.id: position for position in split.positions}
    assert {position.average_price for position in split.positions} == {
        Price(Decimal("0.5")),
    }
    assert split.realized_pnl == 0

    merged = apply_inventory_operation(
        positions,
        _inventory_snapshot(
            "merge-1",
            OutcomeInventoryAction.MERGE,
            _inventory_balance("5", "5", "5"),
            _inventory_balance("2", "2", "8"),
        ),
    )
    assert merged.realized_pnl == 0
    assert {position.quantity.value for position in merged.positions} == {
        Decimal("2"),
    }

    current = {position.id: position for position in merged.positions}
    redeemed = apply_inventory_operation(
        current,
        _inventory_snapshot(
            "redeem-1",
            OutcomeInventoryAction.REDEEM,
            _inventory_balance("2", "2", "8"),
            _inventory_balance("0", "0", "10"),
        ),
    )
    assert all(position.side is PositionSide.FLAT for position in redeemed.positions)
    assert redeemed.realized_pnl == 0
    assert "AGGREGATE_PAYOUT_ALLOCATION" in redeemed.quality_flags


def test_confirmed_split_uses_request_when_post_balance_is_stale() -> None:
    """Account the persisted request when a confirmed split balance lags."""
    snapshot = _inventory_snapshot(
        "split-stale-balance",
        OutcomeInventoryAction.SPLIT,
        _inventory_balance("0", "0", "10"),
        _inventory_balance("0", "0", "10"),
    )
    snapshot = replace(
        snapshot,
        reference=replace(snapshot.reference, quantity=Quantity(Decimal("5"))),
    )

    split = apply_inventory_operation({}, snapshot)

    assert {position.quantity for position in split.positions} == {
        Quantity(Decimal("5")),
    }
    assert {position.average_price for position in split.positions} == {
        Price(Decimal("0.5")),
    }


def test_covered_short_fee_correction_preserves_split_wac() -> None:
    """Keep a late fee update from rebuilding a covered SELL as a naked short."""
    split = apply_inventory_operation(
        {},
        _inventory_snapshot(
            "split-covered",
            OutcomeInventoryAction.SPLIT,
            _inventory_balance("0", "0", "10"),
            _inventory_balance("5", "5", "5"),
        ),
    )
    positions = {position.contract_id: position for position in split.positions}
    yes_sell = replace(
        _trade("sell-yes", OrderSide.SELL, "5", "0.10", fee=None),
        contract_id=ContractID("yes"),
    )
    no_sell = replace(
        _trade("sell-no", OrderSide.SELL, "5", "0.93", fee=None),
        contract_id=ContractID("no"),
    )
    yes = apply_trade(positions[ContractID("yes")], yes_sell)
    no = apply_trade(positions[ContractID("no")], no_sell)

    assert yes.position.side is no.position.side is PositionSide.FLAT
    assert yes.realized_pnl + no.realized_pnl == Decimal("0.15")

    state = TradingState()
    state.apply(TradeRecorded(no_sell))
    state.apply(PositionUpdated(no.position))
    corrected = replace(
        no_sell,
        fee_settlement_cost=Money(Decimal("0.02"), _USD),
    )
    correction = _correction_event(state, no_sell, corrected).correction

    assert correction.resulting_position.side is PositionSide.FLAT
    assert correction.resulting_position.realized_pnl == Decimal("2.15")
    assert correction.resulting_position.fees == Money(Decimal("0.02"), _USD)


def test_cash_movements_are_venue_scoped_and_do_not_change_pnl() -> None:
    state = TradingState()
    movement = CashMovement(
        id="transfer-1",
        kind=CashMovementKind.TRANSFER,
        amount=Money(Decimal("3"), _USD),
        occurred_at=_NOW,
        source_venue_id=VenueID("POLYMARKET"),
        source_portfolio_id=PortfolioID("bot"),
        destination_venue_id=VenueID("PREDICT"),
        destination_portfolio_id=PortfolioID("bot"),
        fee=Money(Decimal("0.1"), _USD),
    )
    state.apply(CashMovementRecorded(movement))

    source = state.portfolios[(VenueID("POLYMARKET"), PortfolioID("bot"))]
    destination = state.portfolios[(VenueID("PREDICT"), PortfolioID("bot"))]
    assert source.net_cash_flow == Decimal("-3")
    assert destination.net_cash_flow == Decimal("3")
    assert source.cash_flow_currency == destination.cash_flow_currency == _USD
    assert source.realized_pnl == destination.realized_pnl == 0


def test_resolution_marks_positions_before_redeem_realizes_them() -> None:
    state = TradingState()
    split = _inventory_snapshot(
        "split-state",
        OutcomeInventoryAction.SPLIT,
        _inventory_balance("0", "0", "10"),
        _inventory_balance("2", "2", "8"),
    )
    state.apply(
        InventoryOperationRecorded(
            InventorySubmissionResult(
                InventorySubmissionStatus.ACCEPTED,
                split.reference,
                split,
            ),
        ),
    )
    settlement = OutcomeInventorySettlement(
        venue_id=VenueID("POLYMARKET"),
        market_id=MarketID("market-1"),
        yes_contract_id=ContractID("yes"),
        no_contract_id=ContractID("no"),
        yes_payout=Price(Decimal("1")),
        no_payout=Price(Decimal("0")),
        observed_at=_NOW,
    )
    state.apply(MarketSettlementRecorded(settlement))
    assert state.positions[PositionID("POLYMARKET:bot:yes")].current_price.value == 1
    assert state.positions[PositionID("POLYMARKET:bot:no")].current_price.value == 0

    redeem = _inventory_snapshot(
        "redeem-state",
        OutcomeInventoryAction.REDEEM,
        _inventory_balance("2", "2", "8"),
        _inventory_balance("0", "0", "10"),
    )
    event = InventoryOperationRecorded(
        InventorySubmissionResult(
            InventorySubmissionStatus.ACCEPTED,
            redeem.reference,
            redeem,
        ),
    )
    state.apply(event)
    realized = sum(position.realized_pnl for position in state.positions.values())
    state.apply(event)

    assert realized == 0
    assert all(position.side is PositionSide.FLAT for position in state.positions.values())
    assert sum(position.realized_pnl for position in state.positions.values()) == realized
    MarketID,
