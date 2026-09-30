"""Coordinate outcome inventory required by covered short arbitrage.

Responsibilities
----------------
- Ensure each venue market holds a configured number of complete sets.
- Merge complete YES/NO sets left after short orders settle.
- Redeem remaining outcome inventory after market resolution.
- Persist every prepared request before venue submission.
- Recover pending requests from journaled references without resubmitting.
"""

import asyncio
import time
from collections.abc import Callable, Mapping
from dataclasses import replace
from decimal import Decimal
from typing import Literal, TypeAlias
from uuid import uuid4

from prediction_markets.domain.outcome_inventory import (
    InventoryOperationID,
    InventoryOperationReference,
    InventoryOperationSnapshot,
    InventoryOperationStatus,
    InventoryReconciliationResult,
    InventorySubmissionResult,
    InventorySubmissionStatus,
    OutcomeInventoryAction,
    OutcomeInventoryBalance,
    OutcomeInventoryIntent,
    OutcomeInventorySettlement,
    PreparedInventoryOperation,
)
from prediction_markets.domain.ports.outcome_inventory import OutcomeInventoryPort
from prediction_markets.domain.shared.value_objects import (
    MarketID,
    Money,
    PortfolioID,
    Quantity,
    Timestamp,
    VenueID,
)

InventoryOperationRecord: TypeAlias = (
    PreparedInventoryOperation
    | InventorySubmissionResult
    | InventoryReconciliationResult
    | OutcomeInventorySettlement
)
ShortInventoryCleanup: TypeAlias = Literal["merge", "redeem"]


class ShortInventoryError(RuntimeError):
    """Report a rejected, failed, or unconfirmed inventory operation."""


class ShortInventoryService:
    """Manage collateralized inventory around cross-venue short execution.

    Invariants
    ----------
    - Every prepared operation is recorded before it is submitted.
    - Venue operations use only the adapter registered for that venue.
    - Merge quantities never exceed the current same-market complete sets.

    Notes
    -----
    - Call :meth:`ensure_short_inventory` when short trading is activated for
      a market pair.
    - Use ``merge`` cleanup only after both short orders are terminal.
    - Use ``redeem`` cleanup only after each market is settled on-chain.
    """

    def __init__(
        self,
        inventory_by_venue: Mapping[VenueID, OutcomeInventoryPort],
        record: Callable[[InventoryOperationRecord], None],
        *,
        portfolio_id: PortfolioID,
        target_quantity: Quantity = Quantity(Decimal("5")),
        reconciliation_timeout_seconds: float = 120.0,
        poll_interval_seconds: float = 0.25,
        convert_fee: Callable[[Money, Timestamp], Money] | None = None,
    ) -> None:
        """
        Parameters
        ----------
        inventory_by_venue
            Inventory adapters keyed by their venue identifiers.
        record
            Durable recorder invoked for prepared requests and venue results.
        target_quantity
            Complete-set inventory required per venue market before shorts.
        portfolio_id
            Portfolio that owns the resulting inventory and economics; must match
            the trading engine portfolio so splits and fills net into one WAC.
        reconciliation_timeout_seconds
            Maximum time to wait for each submitted operation to become terminal.
        poll_interval_seconds
            Delay between venue reconciliation attempts.

        Raises
        ------
        ValueError
            If adapters are absent or timing and quantity settings are invalid.
        """
        if not inventory_by_venue:
            raise ValueError("At least one inventory adapter is required")
        if target_quantity.value <= 0:
            raise ValueError("Short inventory target must be positive")
        if reconciliation_timeout_seconds <= 0 or poll_interval_seconds <= 0:
            raise ValueError("Inventory reconciliation timings must be positive")
        self._inventory = dict(inventory_by_venue)
        self._record = record
        self._target = target_quantity
        self._portfolio_id = portfolio_id
        self._timeout = reconciliation_timeout_seconds
        self._poll_interval = poll_interval_seconds
        self._convert_fee = convert_fee

    @property
    def target_quantity(self) -> Quantity:
        return self._target

    async def ensure_short_inventory(
        self,
        markets: Mapping[VenueID, MarketID],
    ) -> tuple[OutcomeInventoryBalance, ...]:
        """Ensure every venue market has the configured complete-set quantity.

        Parameters
        ----------
        markets
            One market identifier per venue participating in the short pair.

        Returns
        -------
        tuple[OutcomeInventoryBalance, ...]
            Authoritative venue balances after any required splits complete.

        Raises
        ------
        ShortInventoryError
            If an adapter is missing, split collateral is insufficient, or an
            operation does not confirm.

        Notes
        -----
        - Only the deficit below the target is split, making repeated activation
          safe against duplicate full-size splits.
        - Check all venue deficits before any signing or submission.
        """
        balances = await self._balances(markets)
        for balance in balances:
            deficit = max(
                self._target.value - balance.mergeable_quantity.value, Decimal("0"),
            )
            if deficit > 0 and balance.collateral.amount < deficit:
                raise ShortInventoryError(
                    f"Insufficient {balance.collateral.currency} balance for "
                    f"{balance.venue_id} split on {balance.market_id}: "
                    f"required {deficit}, available {balance.collateral.amount}",
                )
        intents = tuple(
            self._intent(
                balance.venue_id,
                balance.market_id,
                OutcomeInventoryAction.SPLIT,
                Quantity(self._target.value - balance.mergeable_quantity.value),
            )
            for balance in balances
            if balance.mergeable_quantity < self._target
        )
        if not intents:
            return balances
        await self._execute(intents)
        return await self._balances(markets)

    async def cleanup(
        self,
        markets: Mapping[VenueID, MarketID],
        mode: ShortInventoryCleanup,
    ) -> tuple[InventoryOperationSnapshot, ...]:
        """Apply the configured post-short inventory policy.

        Parameters
        ----------
        markets
            Venue markets whose inventory should be finalized.
        mode
            ``"merge"`` for immediate complete-set recovery after terminal
            orders, or ``"redeem"`` after short-duration markets settle.

        Returns
        -------
        tuple[InventoryOperationSnapshot, ...]
            Confirmed merge or redemption operations.

        Raises
        ------
        ValueError
            If the cleanup mode is unsupported.
        """
        if mode == "merge":
            return await self.merge_remaining(markets)
        if mode == "redeem":
            return await self.redeem_resolved(markets)
        raise ValueError(f"Unsupported short inventory cleanup mode: {mode}")

    async def merge_remaining(
        self,
        markets: Mapping[VenueID, MarketID],
    ) -> tuple[InventoryOperationSnapshot, ...]:
        """Merge every complete YES/NO set remaining after short execution.

        Parameters
        ----------
        markets
            Venue markets whose short orders have reached terminal states.

        Returns
        -------
        tuple[InventoryOperationSnapshot, ...]
            Confirmed merge operations. Venues without complete sets are omitted.
        """
        balances = await self._balances(markets)
        intents = tuple(
            self._intent(
                balance.venue_id,
                balance.market_id,
                OutcomeInventoryAction.MERGE,
                balance.mergeable_quantity,
            )
            for balance in balances
            if balance.mergeable_quantity.value > 0
        )
        return await self._execute(intents)

    async def redeem_resolved(
        self,
        markets: Mapping[VenueID, MarketID],
    ) -> tuple[InventoryOperationSnapshot, ...]:
        """Redeem non-empty outcome inventory for resolved venue markets.

        Parameters
        ----------
        markets
            Resolved venue markets whose remaining outcome tokens may be claimed.

        Returns
        -------
        tuple[InventoryOperationSnapshot, ...]
            Confirmed redemption operations. Empty inventories are omitted.

        Raises
        ------
        ShortInventoryError
            If a venue rejects redemption or does not confirm it in time.
        """
        settlements = await self._settlements(markets)
        for settlement in settlements:
            self._record(settlement)
        balances = await self._balances(markets)
        intents = tuple(
            self._intent(
                balance.venue_id,
                balance.market_id,
                OutcomeInventoryAction.REDEEM,
            )
            for balance in balances
            if balance.yes.value > 0 or balance.no.value > 0
        )
        return await self._execute(intents)

    async def redeem_available(
        self,
        venue_id: VenueID,
    ) -> tuple[InventoryOperationSnapshot, ...]:
        """Redeem all resolved markets currently exposed by one venue account.

        Parameters
        ----------
        venue_id
            Venue whose account-level market list should be checked.

        Returns
        -------
        tuple[InventoryOperationSnapshot, ...]
            Confirmed redemption operations for markets with non-empty
            on-chain outcome balances.

        Raises
        ------
        ShortInventoryError
            If market discovery, settlement, balance reads, or redemption
            confirmation fails.
        """
        adapter = self._adapter(venue_id)
        markets = await asyncio.to_thread(adapter.list_redeemable_markets)
        redeemed: list[InventoryOperationSnapshot] = []
        errors: list[ShortInventoryError] = []
        for market_id in markets:
            try:
                redeemed.extend(
                    await self.redeem_resolved({venue_id: market_id}),
                )
            except ShortInventoryError as error:
                errors.append(error)
        if errors:
            raise ShortInventoryError("; ".join(str(error) for error in errors))
        return tuple(redeemed)

    async def _settlements(
        self,
        markets: Mapping[VenueID, MarketID],
    ) -> tuple[OutcomeInventorySettlement, ...]:
        """Read final payout vectors before recording any redemption."""
        requested = tuple(markets.items())
        try:
            settlements = await asyncio.gather(
                *(
                    asyncio.to_thread(
                        self._adapter(venue_id).get_settlement,
                        market_id,
                    )
                    for venue_id, market_id in requested
                ),
            )
        except Exception as error:
            raise ShortInventoryError(str(error)) from error
        for settlement, (venue_id, market_id) in zip(
            settlements,
            requested,
            strict=True,
        ):
            if settlement.venue_id != venue_id or settlement.market_id != market_id:
                raise ShortInventoryError(
                    "Inventory adapter returned settlement for another market",
                )
        return tuple(settlements)

    async def _balances(
        self,
        markets: Mapping[VenueID, MarketID],
    ) -> tuple[OutcomeInventoryBalance, ...]:
        """Read and validate current balances for every requested venue market.

        Notes
        -----
        - One transient transport failure is retried because balance reads have
          no external side effects.
        """
        if not markets:
            raise ShortInventoryError("Short inventory requires at least one market")
        requested = tuple(markets.items())
        adapters = tuple(self._adapter(venue_id) for venue_id, _ in requested)
        for attempt in range(2):
            try:
                balances = await asyncio.gather(
                    *(
                        asyncio.to_thread(adapter.get_balance, market_id)
                        for adapter, (_, market_id) in zip(
                            adapters,
                            requested,
                            strict=True,
                        )
                    ),
                )
                break
            except OSError as error:
                if attempt:
                    raise ShortInventoryError(str(error)) from error
                await asyncio.sleep(0.25)
            except Exception as error:
                raise ShortInventoryError(str(error)) from error
        for balance, (venue_id, market_id) in zip(balances, requested, strict=True):
            if balance.venue_id != venue_id or balance.market_id != market_id:
                raise ShortInventoryError(
                    f"Inventory adapter returned {balance.venue_id}/{balance.market_id} "
                    f"for requested {venue_id}/{market_id}",
                )
        return tuple(balances)

    async def _execute(
        self,
        intents: tuple[OutcomeInventoryIntent, ...],
    ) -> tuple[InventoryOperationSnapshot, ...]:
        """Prepare all requests before submitting and confirming them concurrently."""
        if not intents:
            return ()
        adapters = tuple(self._adapter(intent.venue_id) for intent in intents)
        preparation = asyncio.gather(
            *(
                asyncio.to_thread(adapter.prepare, intent)
                for adapter, intent in zip(adapters, intents, strict=True)
            ),
            return_exceptions=True,
        )
        try:
            prepared = await asyncio.shield(preparation)
        except asyncio.CancelledError:
            # Threaded signing must finish before its unsubmitted reservations
            # can be released; cancelling the await does not stop the signer.
            prepared = await preparation
            await self._discard_prepared(adapters, prepared)
            raise
        errors = [result for result in prepared if isinstance(result, BaseException)]
        if errors:
            discarded = await self._discard_prepared(adapters, prepared)
            errors.extend(result for result in discarded if isinstance(result, BaseException))
            raise ShortInventoryError("; ".join(str(error) for error in errors))
        for operation in prepared:
            self._record(operation)
        results = await asyncio.gather(
            *(
                self._submit_and_confirm(adapter, operation)
                for adapter, operation in zip(adapters, prepared, strict=True)
            ),
            return_exceptions=True,
        )
        errors = tuple(result for result in results if isinstance(result, BaseException))
        if errors:
            raise ShortInventoryError("; ".join(str(error) for error in errors))
        return tuple(
            result
            for result in results
            if isinstance(result, InventoryOperationSnapshot)
        )

    async def _discard_prepared(
        self,
        adapters: tuple[OutcomeInventoryPort, ...],
        prepared: list[PreparedInventoryOperation | BaseException],
    ) -> list[None | BaseException]:
        """Release every successful preparation in an unsubmitted batch."""
        return await asyncio.gather(
            *(
                asyncio.to_thread(adapter.discard_prepared, operation)
                for adapter, operation in zip(adapters, prepared, strict=True)
                if isinstance(operation, PreparedInventoryOperation)
            ),
            return_exceptions=True,
        )

    async def reconcile_pending(
        self,
        references: tuple[InventoryOperationReference, ...],
    ) -> tuple[InventoryOperationSnapshot, ...]:
        """Resolve journaled operations before preparing another transaction.

        Parameters
        ----------
        references
            Latest durable references for operations without a terminal result.

        Returns
        -------
        tuple[InventoryOperationSnapshot, ...]
            Confirmed operations recorded during recovery.

        Raises
        ------
        ShortInventoryError
            If any operation fails or remains uncertain. Absence from a venue
            response does not prove that an earlier broadcast cannot confirm.

        Notes
        -----
        - Reuses durable references without submitting or rebuilding requests.
        - Records successful peers even if another venue fails.
        """
        results = await asyncio.gather(
            *(self._confirm(self._adapter(ref.venue_id), ref) for ref in references),
            return_exceptions=True,
        )
        errors = [result for result in results if isinstance(result, BaseException)]
        if errors:
            raise ShortInventoryError("; ".join(str(error) for error in errors))
        return tuple(
            result for result in results if isinstance(result, InventoryOperationSnapshot)
        )

    async def _submit_and_confirm(
        self,
        adapter: OutcomeInventoryPort,
        operation: PreparedInventoryOperation,
    ) -> InventoryOperationSnapshot:
        """Submit one durable request and reconcile its latest recovery reference."""
        submitted = await asyncio.to_thread(adapter.submit, operation)
        if submitted.status is InventorySubmissionStatus.REJECTED:
            self._record(submitted)
            raise ShortInventoryError(
                submitted.reason
                or f"{operation.intent.action.value} rejected by {operation.intent.venue_id}",
            )
        terminal = await self._record_result(submitted)
        if terminal is not None:
            return terminal
        return await self._confirm(adapter, submitted.reference)

    async def _confirm(
        self,
        adapter: OutcomeInventoryPort,
        reference: InventoryOperationReference,
    ) -> InventoryOperationSnapshot:
        """Poll the latest durable reference without repeating a submission."""
        deadline = time.monotonic() + self._timeout
        while time.monotonic() < deadline:
            try:
                reconciled = await asyncio.to_thread(adapter.reconcile, reference)
            except OSError:
                await asyncio.sleep(self._poll_interval)
                continue
            if (
                reconciled.snapshot is not None
                and reconciled.snapshot.status is not InventoryOperationStatus.PENDING
            ) or reconciled.reference != reference:
                terminal = await self._record_result(reconciled)
                if terminal is not None:
                    return terminal
            reference = reconciled.reference
            await asyncio.sleep(self._poll_interval)
        market_id = reference.balance_before.market_id if reference.balance_before else "unknown"
        action = reference.action.value if reference.action is not None else "inventory operation"
        raise ShortInventoryError(
            f"{action} did not confirm for {reference.venue_id}/{market_id} "
            f"(operation {reference.operation_id}); reconcile before retrying",
        )

    async def _record_result(
        self,
        result: InventorySubmissionResult | InventoryReconciliationResult,
    ) -> InventoryOperationSnapshot | None:
        """Persist terminal failures as well as confirmations before returning."""
        snapshot = result.snapshot
        if snapshot is not None and snapshot.status is InventoryOperationStatus.CONFIRMED:
            result = replace(result, snapshot=await self._with_settlement_fee(snapshot))
        self._record(result)
        return self._terminal(result.snapshot)

    async def _with_settlement_fee(
        self,
        snapshot: InventoryOperationSnapshot,
    ) -> InventoryOperationSnapshot:
        """Attach USD cost to a native gas fee without hiding quote failures."""
        if snapshot.fee is None or snapshot.fee_settlement_cost is not None:
            return snapshot
        flags = tuple(
            flag
            for flag in snapshot.quality_flags
            if flag != "MISSING_FEE_CONVERSION"
        )
        if self._convert_fee is None:
            return replace(
                snapshot,
                quality_flags=(*flags, "MISSING_FEE_CONVERSION"),
            )
        try:
            converted = await asyncio.to_thread(
                self._convert_fee,
                snapshot.fee,
                snapshot.fee_observed_at or snapshot.updated_at,
            )
        except Exception:
            return replace(
                snapshot,
                quality_flags=(*flags, "MISSING_FEE_CONVERSION"),
            )
        return replace(
            snapshot,
            fee_settlement_cost=converted,
            quality_flags=flags,
        )

    @staticmethod
    def _terminal(
        snapshot: InventoryOperationSnapshot | None,
    ) -> InventoryOperationSnapshot | None:
        """Return confirmed snapshots and raise for authoritative failures."""
        if snapshot is None or snapshot.status is InventoryOperationStatus.PENDING:
            return None
        if snapshot.status is InventoryOperationStatus.FAILED:
            raise ShortInventoryError(snapshot.reason or "Inventory operation failed")
        return snapshot

    def _adapter(self, venue_id: VenueID) -> OutcomeInventoryPort:
        """Return the adapter for one venue or fail before preparing requests."""
        adapter = self._inventory.get(venue_id)
        if adapter is None:
            raise ShortInventoryError(f"No inventory adapter for {venue_id}")
        return adapter

    def _intent(
        self,
        venue_id: VenueID,
        market_id: MarketID,
        action: OutcomeInventoryAction,
        quantity: Quantity | None = None,
    ) -> OutcomeInventoryIntent:
        """Create one uniquely recoverable inventory intent."""
        return OutcomeInventoryIntent(
            operation_id=InventoryOperationID(
                f"short-{action.value}-{venue_id}-{market_id}-{uuid4().hex}",
            ),
            venue_id=venue_id,
            market_id=market_id,
            action=action,
            quantity=quantity,
            portfolio_id=self._portfolio_id,
        )
