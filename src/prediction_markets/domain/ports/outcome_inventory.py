"""Define the recoverable boundary for venue outcome inventory.

Responsibilities
----------------
- Read venue balances needed for covered short arbitrage.
- Prepare, submit, and reconcile split, merge, and redeem operations.
"""

from abc import ABC, abstractmethod

from prediction_markets.domain.outcome_inventory import (
    InventoryOperationReference,
    InventoryReconciliationResult,
    InventorySubmissionResult,
    OutcomeInventoryBalance,
    OutcomeInventoryIntent,
    OutcomeInventorySettlement,
    PreparedInventoryOperation,
)
from prediction_markets.domain.shared.value_objects import MarketID


class OutcomeInventoryPort(ABC):
    """Manage one venue's binary-outcome inventory recoverably.

    Notes
    -----
    - Adapters must prepare an exact request before submission so the
      application can persist it and reconcile after process loss.
    - Implementations own all venue-specific contracts, APIs, approvals, and
      transaction identifiers.
    """

    @abstractmethod
    def get_balance(self, market_id: MarketID) -> OutcomeInventoryBalance:
        """Return available collateral, YES, and NO balances for one market.

        Parameters
        ----------
        market_id
            Venue market whose currently available inventory is required.

        Returns
        -------
        OutcomeInventoryBalance
            Venue snapshot used for split, merge, and sell-capacity decisions.
        """
        ...

    @abstractmethod
    def get_settlement(self, market_id: MarketID) -> OutcomeInventorySettlement:
        """Return the venue's final binary payout vector.

        Raises
        ------
        ValueError
            If the market has not settled or its payout cannot be verified.
        """
        ...

    def list_redeemable_markets(self) -> tuple[MarketID, ...]:
        """Return account markets that may contain resolved outcome tokens.

        Returns
        -------
        tuple[MarketID, ...]
            Market identifiers discovered from an account-level venue API.
            Venues without account-wide discovery return an empty tuple.

        Notes
        -----
        - Implementations may return resolved markets with zero on-chain
          balance; callers always verify the balance before preparing a redeem.
        """
        return ()

    @abstractmethod
    def prepare(self, intent: OutcomeInventoryIntent) -> PreparedInventoryOperation:
        """Build an exact recoverable request without submitting it.

        Parameters
        ----------
        intent
            Venue-neutral split, merge, or redeem request.

        Returns
        -------
        PreparedInventoryOperation
            Opaque request and stable recovery reference that must be persisted.
        """
        ...

    @abstractmethod
    def submit(self, operation: PreparedInventoryOperation) -> InventorySubmissionResult:
        """Submit a previously prepared inventory request without rebuilding it.

        Parameters
        ----------
        operation
            Exact persisted request returned by :meth:`prepare`.

        Returns
        -------
        InventorySubmissionResult
            Accepted, rejected, or uncertain submission state.
        """
        ...

    def discard_prepared(self, operation: PreparedInventoryOperation) -> None:
        """Release local reservations for a request that was never submitted.

        Notes
        -----
        - Callers must not discard submitted or uncertain requests.
        - Adapters without local reservations need no cleanup.
        """

    @abstractmethod
    def reconcile(
        self,
        reference: InventoryOperationReference,
    ) -> InventoryReconciliationResult:
        """Resolve a persisted operation using only its stable reference.

        Parameters
        ----------
        reference
            Adapter-owned recovery data persisted before submission.

        Returns
        -------
        InventoryReconciliationResult
            Found, absent, or uncertain venue state.
        """
        ...
