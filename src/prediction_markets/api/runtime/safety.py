"""Enforce venue health and collateral safety outside order submission.

Responsibilities
----------------
- Observe active venue health and collateral on periodic control tasks.
- Convert sustained or authentication failures into fail-closed safety stops.
- Persist the safety-stop event through the existing trading pipeline.
"""

import asyncio
import logging
from decimal import Decimal

from prediction_markets.application.engine import TradingEngine
from prediction_markets.application.events import TradingSafetyStop
from prediction_markets.application.pipeline import TradingPipeline
from prediction_markets.application.state import TradingState
from prediction_markets.application.venue_health import VenueHealthService
from prediction_markets.domain.market_matching.value_objects import MatchedContractPair
from prediction_markets.domain.ports.execution import ExecutionPort
from prediction_markets.domain.shared.value_objects import ContractID, Timestamp, VenueID
from prediction_markets.domain.venue_health import VenueHealthSnapshot, VenueHealthStatus

_events = logging.getLogger("prediction_markets.events.runtime")
_DEFAULT_RUNTIME_HEALTH_CHECK_SECONDS = 15.0
_VENUE_5XX_FAILURE_THRESHOLD = 3


class _VenueSafetyStop(RuntimeError):
    """Carry the venue and reason for a fail-closed trading stop."""

    def __init__(self, venue_id: VenueID, reason: str) -> None:
        super().__init__(reason)
        self.venue_id = venue_id
        self.reason = reason


class _CollateralRefreshError(RuntimeError):
    """Carry the venue identity and HTTP status of a collateral-read failure."""

    def __init__(
        self,
        venue_id: VenueID,
        reason: str,
        *,
        http_status: int | None = None,
    ) -> None:
        super().__init__(reason)
        self.venue_id = venue_id
        self.http_status = http_status


def _http_status(error: BaseException) -> int | None:
    """Extract a valid HTTP status from common SDK exception shapes."""
    candidates = (error, getattr(error, "response", None))
    for candidate in candidates:
        value = getattr(candidate, "status_code", None)
        if isinstance(value, int) and 100 <= value <= 599:
            return value
    return None


def _auth_failure(value: object, http_status: int | None = None) -> bool:
    """Identify authentication failures even when an SDK hides status codes."""
    if http_status == 401:
        return True
    message = str(value).lower()
    return any(
        marker in message
        for marker in (
            "401",
            "unauthorized",
            "authentication failed",
            "invalid token",
            "not authorized",
        )
    )


def _transient_health_failure(snapshot: VenueHealthSnapshot) -> bool:
    """Identify retryable transport failures that require confirmation."""
    http_status = snapshot.http_status
    return (
        snapshot.status is not VenueHealthStatus.OPERATIONAL
        and snapshot.retryable
        and (
            http_status is None
            or http_status == 429
            or http_status >= 500
        )
    )


class RuntimeSafety:
    """Own active-run health, collateral refresh, and fail-closed recording."""

    def __init__(
        self,
        venue_health_service: VenueHealthService | None,
        state: TradingState,
        engine: TradingEngine,
        pipeline: TradingPipeline,
        execution: dict[VenueID, ExecutionPort],
    ) -> None:
        self._venue_health_service = venue_health_service
        self.state = state
        self.engine = engine
        self.pipeline = pipeline
        self._execution = execution

    def _active_venue_ids(self) -> frozenset[VenueID]:
        """Return venues participating in the current monitored pairs."""
        if self.state is None:
            return frozenset()
        return frozenset(
            contract.venue_id
            for pairs in self.state.matches.values()
            for pair in pairs
            for contract in (pair.left, pair.right)
        )

    async def monitor_active_venue_health(self, interval_seconds: float) -> None:
        """Stop the active run when an active venue is not operational.

        Parameters
        ----------
        interval_seconds
            Delay between forced health observations. The monitor runs outside
            order detection and submission tasks.

        Raises
        ------
        _VenueSafetyStop
            If an active venue is degraded, unavailable, unauthorized, or
            returns enough consecutive server errors to be considered unsafe.
        """
        if self._venue_health_service is None:
            raise RuntimeError("Venue health service is not configured")
        transient_failures: dict[VenueID, int] = {}
        while True:
            await asyncio.sleep(interval_seconds)
            active_venues = self._active_venue_ids()
            if not active_venues:
                continue
            try:
                report = await self._venue_health_service.get(force_refresh=True)
            except Exception as error:
                raise _VenueSafetyStop(
                    VenueID("runtime"),
                    f"venue health monitor failed: {error}",
                ) from error
            snapshots = {venue.venue_id: venue for venue in report.venues}
            for venue_id in active_venues:
                snapshot = snapshots.get(venue_id)
                if snapshot is None:
                    raise _VenueSafetyStop(
                        venue_id,
                        "active venue is missing from the health report",
                    )
                http_status = snapshot.http_status
                if _auth_failure(snapshot.message, http_status):
                    raise _VenueSafetyStop(
                        venue_id,
                        f"{venue_id} health authentication failure: {snapshot.message}",
                    )
                if _transient_health_failure(snapshot):
                    failures = transient_failures.get(venue_id, 0) + 1
                    transient_failures[venue_id] = failures
                    if failures >= _VENUE_5XX_FAILURE_THRESHOLD:
                        raise _VenueSafetyStop(
                            venue_id,
                            f"{venue_id} health had {failures} consecutive "
                            f"transient failures: {snapshot.message}",
                        )
                    _events.warning(
                        "Venue %s health check had a transient failure (%s/%s): %s",
                        venue_id,
                        failures,
                        _VENUE_5XX_FAILURE_THRESHOLD,
                        snapshot.message,
                    )
                    continue
                transient_failures.pop(venue_id, None)
                if snapshot.status is not VenueHealthStatus.OPERATIONAL:
                    raise _VenueSafetyStop(
                        venue_id,
                        f"{venue_id} health is {snapshot.status.value}: "
                        f"{snapshot.message}",
                    )

    async def record_safety_stop(self, failure: _VenueSafetyStop) -> None:
        """Disable live trading and persist one alertable safety-stop event."""
        reason = f"Trading halted by venue safety circuit: {failure.reason}"
        if self.engine is not None:
            fail_run = getattr(self.engine, "fail_run", None)
            if fail_run is not None:
                fail_run(reason)
            else:
                self.engine.disable()
                if self.state is not None:
                    self.state.last_error = reason
        if self.pipeline is None:
            return
        event_loop = getattr(self.pipeline, "event_loop", None)
        if event_loop is None:
            return
        try:
            await event_loop.process(
                TradingSafetyStop(
                    venue_id=failure.venue_id,
                    reason=reason,
                    detected_at=Timestamp.now(),
                ),
                enqueue_commands=False,
            )
        except Exception:
            _events.exception("Could not persist venue safety stop")

    async def read_available_collateral(
        self,
        additional_pairs: tuple[MatchedContractPair, ...] = (),
    ) -> dict[VenueID, Decimal]:
        """Read spendable BUY collateral for every currently matched venue.

        Parameters
        ----------
        additional_pairs
            Newly discovered pairs not yet guaranteed to be visible in runtime
            state, such as a cycle rollover being prepared.

        Returns
        -------
        dict[VenueID, Decimal]
            Venue balances constrained by the approval scopes of current
            executable contracts.

        Raises
        ------
        RuntimeError
            If an active venue cannot provide a collateral observation.

        Notes
        -----
        - All venue I/O runs before admission or in the periodic control task,
          never between order preparation and its age guard.
        """
        if self.state is None:
            raise RuntimeError("Arbitrage runtime is not initialized")
        contracts_by_venue: dict[VenueID, dict[ContractID, None]] = {}
        for pairs in self.state.matches.values():
            for pair in pairs:
                for contract in (pair.left, pair.right):
                    contracts_by_venue.setdefault(contract.venue_id, {})[
                        contract.id
                    ] = None
        for pair in additional_pairs:
            for contract in (pair.left, pair.right):
                contracts_by_venue.setdefault(contract.venue_id, {})[
                    contract.id
                ] = None

        async def read_one(
            venue_id: VenueID,
            contract_ids: tuple[ContractID, ...],
        ) -> tuple[VenueID, Decimal]:
            adapter = self._execution.get(venue_id)
            if adapter is None:
                raise RuntimeError(f"No execution adapter for {venue_id}")
            try:
                available = await asyncio.to_thread(
                    adapter.get_available_collateral,
                    contract_ids,
                )
            except Exception as error:
                raise _CollateralRefreshError(
                    venue_id,
                    f"{venue_id} collateral refresh failed: {error}",
                    http_status=_http_status(error),
                ) from error
            if available is None:
                raise RuntimeError(
                    f"{venue_id} does not expose collateral for active contracts",
                )
            return venue_id, available

        observed = await asyncio.gather(
            *(
                read_one(venue_id, tuple(contract_ids))
                for venue_id, contract_ids in contracts_by_venue.items()
            ),
        )
        return dict(observed)

    async def refresh_collateral_periodically(self, interval_seconds: float) -> None:
        """Reconcile the local BUY ledger outside the order hot path.

        Parameters
        ----------
        interval_seconds
            Positive delay between venue observations.

        Raises
        ------
        ValueError
            If the refresh interval is not positive.

        Notes
        -----
        - Authentication failures stop the active run immediately.
        - Server errors preserve the previous ledger while transient, then stop
          the run after repeated failures.
        - The engine postpones observations while a BUY reservation is active.
        """
        if interval_seconds <= 0:
            raise ValueError("collateral refresh interval must be positive")
        five_xx_failures: dict[VenueID, int] = {}
        while True:
            await asyncio.sleep(interval_seconds)
            try:
                observed = await self.read_available_collateral()
            except _CollateralRefreshError as error:
                if _auth_failure(error, error.http_status):
                    raise _VenueSafetyStop(error.venue_id, str(error)) from error
                if error.http_status is not None and error.http_status >= 500:
                    failures = five_xx_failures.get(error.venue_id, 0) + 1
                    five_xx_failures[error.venue_id] = failures
                    if failures >= _VENUE_5XX_FAILURE_THRESHOLD:
                        raise _VenueSafetyStop(
                            error.venue_id,
                            f"{error} ({failures} consecutive server errors)",
                        ) from error
                    _events.warning(
                        "Collateral refresh failed for %s (%s/%s): %s",
                        error.venue_id,
                        failures,
                        _VENUE_5XX_FAILURE_THRESHOLD,
                        error,
                    )
                    continue
                _events.warning("Collateral refresh failed: %s", error)
                continue
            except Exception as error:
                _events.warning("Collateral refresh failed: %s", error)
                continue
            five_xx_failures.clear()
            if self.engine is not None:
                self.engine.refresh_collateral(observed)
