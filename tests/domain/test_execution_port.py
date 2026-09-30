"""Verify the venue-neutral recoverable execution contract."""

import pytest

from prediction_markets.domain.ports.execution import (
    ExecutionPort,
    OrderUpdatePort,
)
from prediction_markets.domain.shared.value_objects import ClientOrderID, VenueID
from prediction_markets.domain.trading.enums import ReconciliationStatus
from prediction_markets.domain.trading.value_objects import (
    OrderReference,
    PreparedOrder,
    ReconciliationResult,
)


def test_execution_port_requires_preparation_and_unambiguous_reconciliation() -> None:
    """Keep recovery information durable and distinguish absence from uncertainty."""
    reference = OrderReference(
        venue_id=VenueID("prediction-venue"),
        client_order_id=ClientOrderID("client-order"),
        recovery_data=b"venue-recovery-key",
    )

    assert PreparedOrder(reference, b"exact-request").reference == reference
    assert ExecutionPort.__abstractmethods__ == {
        "prepare",
        "submit",
        "reconcile",
        "cancel",
    }
    assert OrderUpdatePort.__abstractmethods__ == {
        "record_snapshot",
        "wait_for_update",
    }
    assert ReconciliationResult(
        ReconciliationStatus.UNKNOWN,
        reference,
    ).snapshot is None

    with pytest.raises(ValueError, match="Only found"):
        ReconciliationResult(ReconciliationStatus.FOUND, reference)
