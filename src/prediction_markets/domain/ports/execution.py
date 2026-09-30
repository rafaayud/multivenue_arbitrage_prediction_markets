"""Define recoverable execution boundaries required by the application.

Responsibilities
----------------
- Specify venue-neutral contracts for preparing, submitting, and reconciling orders.
- Carry opaque recovery data without exposing venue-specific protocols.
"""

from abc import ABC, abstractmethod
from collections.abc import Mapping
from decimal import Decimal

from prediction_markets.domain.contracts.value_objects import TickSize
from prediction_markets.domain.shared.value_objects import ContractID
from prediction_markets.domain.trading.entities import OrderIntent, OrderSnapshot
from prediction_markets.domain.trading.value_objects import (
    OrderReference,
    PreparedOrder,
    ReconciliationResult,
    SubmissionResult,
)


class ExecutionPort(ABC):
    """Prepare and execute orders that remain recoverable after process loss."""

    supports_definitive_cancellation = False
    cancellation_transaction_lock = None

    def prepare_cancellation(self, order: PreparedOrder) -> bytes | None:
        """Prepare optional definitive cancellation without broadcasting.

        Returns
        -------
        bytes | None
            Opaque transaction to journal before broadcast, or ``None`` when the
            adapter does not support this recovery-only escalation.
        """
        return None

    def submit_cancellation(
        self, order: PreparedOrder, request: bytes,
    ) -> ReconciliationResult:
        """Broadcast the exact journaled cancellation and reconcile its outcome.

        Notes
        -----
        - Retries must reuse the transaction identity; uncertainty never permits
          a replacement order.

        Raises
        ------
        NotImplementedError
            If definitive cancellation is unsupported by the adapter.
        """
        raise NotImplementedError("Definitive cancellation is unavailable")

    def cancellation_window_seconds(self, intent: OrderIntent) -> float | None:
        """Return the application-owned lifetime of a potentially open order.

        Parameters
        ----------
        intent
            Venue-independent order whose translated venue semantics are queried.

        Returns
        -------
        float | None
            Positive seconds before cancellation, or ``None`` when the venue
            implements the requested time-in-force itself.

        Notes
        -----
        - Adapters that translate an immediate intent into a resting order must
          override this method. The dispatcher then owns cancellation uniformly.
        """
        del intent
        return None

    def preload(
        self,
        contract_ids: tuple[ContractID, ...],
    ) -> Mapping[ContractID, TickSize] | None:
        """Warm optional venue metadata for contracts that may execute soon.

        Parameters
        ----------
        contract_ids
            Newly monitored contracts. Adapters without lazy venue metadata may
            keep the default no-op implementation.

        Returns
        -------
        Mapping[ContractID, TickSize] | None
            Authoritative execution ticks learned during preload, when available.
        """

    def get_available_collateral(
        self,
        contract_ids: tuple[ContractID, ...],
    ) -> Decimal | None:
        """Read collateral constrained by venue allowances outside submission.

        Parameters
        ----------
        contract_ids
            Currently executable contracts whose approval scopes must be
            included in the result.

        Returns
        -------
        Decimal | None
            Spendable collateral in settlement units, or ``None`` when the
            adapter does not support collateral-backed BUY orders.

        Notes
        -----
        - Runtime composition calls this before enabling a run and periodically,
          never between order preparation and the submission guard.
        """
        return None

    @abstractmethod
    def prepare(self, intent: OrderIntent) -> PreparedOrder:
        """
        Build an exact recoverable request without submitting it.

        Parameters
        ----------
        intent
            Venue-independent order parameters to translate and sign when required.

        Returns
        -------
        PreparedOrder
            Opaque request and stable reference that must be persisted before submission.
        """
        ...

    @abstractmethod
    def submit(self, order: PreparedOrder) -> SubmissionResult:
        """
        Submit the exact request previously returned by :meth:`prepare`.

        Parameters
        ----------
        order
            Persisted prepared order; adapters must not rebuild its venue request.

        Returns
        -------
        SubmissionResult
            Accepted, rejected, or uncertain result with any available snapshot.
        """
        ...

    @abstractmethod
    def reconcile(self, reference: OrderReference) -> ReconciliationResult:
        """
        Resolve an order using only its persisted reference.

        Parameters
        ----------
        reference
            Adapter-owned recovery data and application correlation identifiers.

        Returns
        -------
        ReconciliationResult
            Found, absent, or uncertain outcome. ``UNKNOWN`` must never be treated as absent.
        """
        ...

    @abstractmethod
    def cancel(self, reference: OrderReference) -> ReconciliationResult:
        """
        Request cancellation and reconcile the resulting venue state.

        Parameters
        ----------
        reference
            Persisted reference for the order to cancel.

        Returns
        -------
        ReconciliationResult
            Current authoritative state, or ``UNKNOWN`` when cancellation cannot be proven.
        """
        ...


class OrderUpdatePort(ABC):
    """Expose normalized private order changes without owning stream lifecycle."""

    @abstractmethod
    def record_snapshot(
        self,
        reference: OrderReference,
        snapshot: OrderSnapshot,
        source: str,
    ) -> OrderSnapshot:
        """
        Register a submitted or reconciled snapshot under its durable reference.

        Parameters
        ----------
        reference
            Persisted order identity used after process loss.
        snapshot
            Normalized state returned by submission or reconciliation.
        source
            ``"submit"`` for an initial response or ``"get"`` for REST reconciliation.

        Returns
        -------
        OrderSnapshot
            Latest state after merging any private updates received earlier.
        """
        ...

    @abstractmethod
    async def wait_for_update(
        self,
        reference: OrderReference,
        after: OrderSnapshot,
        timeout: float,
    ) -> OrderSnapshot | None:
        """
        Wait for normalized state newer than the caller's last snapshot.

        Parameters
        ----------
        reference
            Persisted identity of the tracked order.
        after
            Last state already observed by the caller.
        timeout
            Maximum wait in seconds.

        Returns
        -------
        OrderSnapshot | None
            New state, or ``None`` when no change arrives before the timeout.
        """
        ...
