"""Verify outcome-inventory invariants and its recoverable venue boundary."""

from datetime import datetime, timezone
from decimal import Decimal

import pytest

from prediction_markets.domain.outcome_inventory import (
    InventoryOperationID,
    InventoryOperationReference,
    InventoryOperationSnapshot,
    InventoryOperationStatus,
    InventoryReconciliationResult,
    InventoryReconciliationStatus,
    OutcomeInventoryAction,
    OutcomeInventoryBalance,
    OutcomeInventoryIntent,
    PreparedInventoryOperation,
)
from prediction_markets.domain.ports.outcome_inventory import OutcomeInventoryPort
from prediction_markets.domain.shared.value_objects import (
    Currency,
    MarketID,
    Money,
    Quantity,
    Timestamp,
    VenueID,
)


def _intent(
    action: OutcomeInventoryAction,
    quantity: Quantity | None = None,
) -> OutcomeInventoryIntent:
    """Build a deterministic inventory intent for domain checks."""
    return OutcomeInventoryIntent(
        operation_id=InventoryOperationID("inventory-operation"),
        venue_id=VenueID("venue"),
        market_id=MarketID("market"),
        action=action,
        quantity=quantity,
    )


def _reference() -> InventoryOperationReference:
    """Build the stable reference matching :func:`_intent`."""
    return InventoryOperationReference(
        venue_id=VenueID("venue"),
        operation_id=InventoryOperationID("inventory-operation"),
        recovery_data=b"venue-transaction-reference",
    )


def test_split_merge_and_redeem_quantity_invariants() -> None:
    """Require positive complete sets and quantity-free redemption."""
    split = _intent(OutcomeInventoryAction.SPLIT, Quantity(Decimal("2")))
    merge = _intent(OutcomeInventoryAction.MERGE, Quantity(Decimal("1")))
    redeem = _intent(OutcomeInventoryAction.REDEEM)

    assert split.quantity == Quantity(Decimal("2"))
    assert merge.quantity == Quantity(Decimal("1"))
    assert redeem.quantity is None

    with pytest.raises(ValueError, match="must be positive"):
        _intent(OutcomeInventoryAction.SPLIT, Quantity(Decimal("0")))
    with pytest.raises(ValueError, match="cannot specify"):
        _intent(OutcomeInventoryAction.REDEEM, Quantity(Decimal("1")))


def test_balance_exposes_only_same_venue_complete_sets_as_mergeable() -> None:
    """Limit merge capacity to the smaller local outcome balance."""
    balance = OutcomeInventoryBalance(
        venue_id=VenueID("venue"),
        market_id=MarketID("market"),
        yes=Quantity(Decimal("4")),
        no=Quantity(Decimal("2.5")),
        collateral=Money(Decimal("10"), Currency("USDC")),
        observed_at=Timestamp(datetime(2026, 8, 6, tzinfo=timezone.utc)),
    )

    assert balance.mergeable_quantity == Quantity(Decimal("2.5"))


def test_port_requires_durable_preparation_and_reconciliation() -> None:
    """Keep financial operations recoverable without venue process memory."""
    intent = _intent(OutcomeInventoryAction.SPLIT, Quantity(Decimal("1")))
    reference = _reference()
    prepared = PreparedInventoryOperation(intent, reference, b"exact-request")

    assert prepared.reference == reference
    assert OutcomeInventoryPort.__abstractmethods__ == {
        "get_balance",
        "get_settlement",
        "prepare",
        "submit",
        "reconcile",
    }

    with pytest.raises(ValueError, match="Only found"):
        InventoryReconciliationResult(
            InventoryReconciliationStatus.FOUND,
            reference,
        )

    snapshot = InventoryOperationSnapshot(
        reference=reference,
        status=InventoryOperationStatus.CONFIRMED,
        updated_at=Timestamp(datetime(2026, 8, 6, tzinfo=timezone.utc)),
        transaction_id="transaction-id",
    )
    assert InventoryReconciliationResult(
        InventoryReconciliationStatus.FOUND,
        reference,
        snapshot,
    ).snapshot == snapshot


def test_inventory_snapshot_carries_economic_facts_without_faking_unknowns() -> None:
    """Keep confirmed quantity, cash movement, payout, and fee metadata together."""
    reference = InventoryOperationReference(
        venue_id=VenueID("venue"),
        operation_id=InventoryOperationID("inventory-operation"),
        recovery_data=b"venue-transaction-reference",
        quantity=Quantity(Decimal("2")),
    )
    snapshot = InventoryOperationSnapshot(
        reference=reference,
        status=InventoryOperationStatus.CONFIRMED,
        updated_at=Timestamp(datetime(2026, 8, 6, tzinfo=timezone.utc)),
        transaction_id="transaction-id",
        quantity=Quantity(Decimal("2")),
        collateral=Money(Decimal("2"), Currency("USDC")),
        payout=Money(Decimal("1.8"), Currency("USDC")),
        fee=Money(Decimal("0.01"), Currency("USDC")),
        fee_settlement_cost=Money(Decimal("0.01"), Currency("USD")),
    )

    assert snapshot.quantity == reference.quantity
    assert snapshot.collateral == Money(Decimal("2"), Currency("USDC"))
    assert snapshot.payout == Money(Decimal("1.8"), Currency("USDC"))
    assert snapshot.fee_settlement_cost == Money(Decimal("0.01"), Currency("USD"))
