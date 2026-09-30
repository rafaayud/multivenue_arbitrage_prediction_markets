"""Run alert delivery and escalation outside the trading hot path.

Responsibilities
----------------
- Atomically claim pending or abandoned notification deliveries.
- Send immutable snapshots through the configured notification adapter.
- Persist accepted and failed attempt outcomes.
- Apply policy-driven severity escalation to active incidents.
"""

import asyncio
from collections.abc import Callable
from contextlib import AbstractContextManager

from prediction_markets.domain.alerting.ports import (
    AlertingPort,
    IncidentRepositoryPort,
    NotificationDeliveryRepositoryPort,
    NotificationSenderPort,
)
from prediction_markets.domain.alerting.service import EscalationPolicy
from prediction_markets.domain.shared.value_objects import Timestamp


class AlertDeliveryWorker:
    """Poll and deliver the alerting outbox without blocking journal projection.

    Parameters
    ----------
    repository
        Delivery repository with atomic claim semantics.
    sender
        External notification adapter.
    batch_size
        Maximum rows claimed per poll.
    poll_seconds
        Delay after an empty batch or repository failure.

    Notes
    -----
    - A provider failure leaves the row in ``FAILED`` for explicit retry.
    - A process crash leaves ``SENDING`` rows reclaimable after the repository
      adapter's lease expires.
    """

    def __init__(
        self,
        repository: NotificationDeliveryRepositoryPort,
        sender: NotificationSenderPort,
        *,
        batch_size: int = 32,
        poll_seconds: float = 1.0,
    ) -> None:
        if batch_size <= 0 or poll_seconds <= 0:
            raise ValueError("Alert worker batch and poll settings must be positive")
        self._repository = repository
        self._sender = sender
        self._batch_size = batch_size
        self._poll_seconds = poll_seconds
        self._stop = asyncio.Event()
        self._task: asyncio.Task[None] | None = None
        self._error: BaseException | None = None

    @property
    def error(self) -> BaseException | None:
        """Return the latest claim or delivery failure."""
        return self._error

    def start(self) -> None:
        """Start the worker task idempotently."""
        if self._task is not None and not self._task.done():
            return
        self._stop.clear()
        self._task = asyncio.create_task(
            self._run(),
            name="alert-delivery-worker",
        )

    async def run_once(self) -> int:
        """Claim and process one batch.

        Returns
        -------
        int
            Number of claimed deliveries processed.
        """
        deliveries = await asyncio.to_thread(
            self._repository.claim_pending,
            self._batch_size,
            Timestamp.now(),
        )
        last_error: BaseException | None = None
        for delivery in deliveries:
            try:
                provider_reference = await self._sender.send_notification(delivery)
                delivery.mark_delivered(Timestamp.now(), provider_reference)
            except Exception as error:
                last_error = error
                delivery.mark_failed(
                    Timestamp.now(),
                    str(error) or type(error).__name__,
                )
            await asyncio.to_thread(
                self._repository.update_notification_delivery,
                delivery,
            )
        self._error = last_error
        return len(deliveries)

    async def close(self) -> None:
        """Stop the worker after its current delivery finishes."""
        self._stop.set()
        task = self._task
        if task is not None:
            await task
        self._task = None

    async def _run(self) -> None:
        """Drain available batches and back off while the outbox is idle."""
        while not self._stop.is_set():
            try:
                processed = await self.run_once()
            except Exception as error:
                self._error = error
                processed = 0
            if processed:
                continue
            try:
                await asyncio.wait_for(
                    self._stop.wait(),
                    timeout=self._poll_seconds,
                )
            except TimeoutError:
                pass


class AlertEscalationWorker:
    """Apply timeout escalation periodically outside the trading hot path.

    Parameters
    ----------
    incidents
        Repository used to list active incidents.
    alerting
        Inbound use cases used to apply severity changes and enqueue deliveries.
    transaction
        Context factory enclosing each complete escalation scan atomically.
    poll_seconds
        Delay between scans.

    Notes
    -----
    - The runtime gives this worker a dedicated PostgreSQL connection.
    """

    def __init__(
        self,
        incidents: IncidentRepositoryPort,
        alerting: AlertingPort,
        transaction: Callable[[], AbstractContextManager[object]],
        *,
        poll_seconds: float = 60.0,
        policy: EscalationPolicy | None = None,
    ) -> None:
        if poll_seconds <= 0:
            raise ValueError("Alert escalation poll setting must be positive")
        self._incidents = incidents
        self._alerting = alerting
        self._transaction = transaction
        self._poll_seconds = poll_seconds
        self._policy = policy or EscalationPolicy()
        self._stop = asyncio.Event()
        self._task: asyncio.Task[None] | None = None
        self._error: BaseException | None = None

    @property
    def error(self) -> BaseException | None:
        """Return the latest escalation scan failure."""
        return self._error

    def start(self) -> None:
        """Start the worker task idempotently."""
        if self._task is not None and not self._task.done():
            return
        self._stop.clear()
        self._task = asyncio.create_task(
            self._run(),
            name="alert-escalation-worker",
        )

    async def run_once(self, at: Timestamp | None = None) -> int:
        """Apply every escalation due at one instant.

        Parameters
        ----------
        at
            Evaluation instant, defaulting to the current time.

        Returns
        -------
        int
            Number of incidents whose severity changed.
        """
        return await asyncio.to_thread(
            self._run_once,
            at or Timestamp.now(),
        )

    async def close(self) -> None:
        """Stop the worker after its current scan finishes."""
        self._stop.set()
        task = self._task
        if task is not None:
            await task
        self._task = None

    def _run_once(self, at: Timestamp) -> int:
        """Evaluate and persist one scan inside its caller-owned transaction."""
        escalated = 0
        with self._transaction():
            # ponytail: scan all active incidents; add due-at indexing if volume
            # makes this periodic query measurable.
            for incident in self._incidents.list_active():
                severity = self._policy.severity_after_timeout(incident, at)
                if severity is incident.severity:
                    continue
                self._alerting.change_incident_severity(
                    incident.id,
                    severity,
                    at,
                )
                escalated += 1
        return escalated

    async def _run(self) -> None:
        """Scan on the configured interval until shutdown."""
        while not self._stop.is_set():
            try:
                await self.run_once()
                self._error = None
            except Exception as error:
                self._error = error
            try:
                await asyncio.wait_for(
                    self._stop.wait(),
                    timeout=self._poll_seconds,
                )
            except TimeoutError:
                pass
