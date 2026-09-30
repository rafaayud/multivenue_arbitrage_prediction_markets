"""Own durable projection, alert workers, snapshots, and journal shutdown.

Responsibilities
----------------
- Start PostgreSQL projection and alert delivery workers.
- Recover retained journal history from the latest valid snapshot.
- Coordinate durable shutdown ordering for journal-owned resources.
"""

import asyncio
import json
import logging
import os
from pathlib import Path
from typing import Any

import psycopg

from prediction_markets.application.alerting.service import AlertingApplicationService
from prediction_markets.application.pipeline import JournalRecord
from prediction_markets.domain.alerting.enums import Channel
from prediction_markets.domain.alerting.service import EscalationPolicy, NotificationPolicy
from prediction_markets.domain.alerting.value_objects import Recipient
from prediction_markets.infrastructure.alerting.notifications.alertmanager.alertmanager_adapter import (
    AlertManagerAdapter,
)
from prediction_markets.infrastructure.alerting.worker import (
    AlertDeliveryWorker,
    AlertEscalationWorker,
)
from prediction_markets.infrastructure.binary_journal import BinaryJournal
from prediction_markets.infrastructure.postgres.projector import (
    PostgresProjector,
    apply_migrations,
)
from prediction_markets.infrastructure.postgres.repositories import (
    PostgresIncidentRepository,
    PostgresNotificationDeliveryRepository,
)
from prediction_markets.infrastructure.recovery_snapshots import (
    JournalMaintenanceWorker,
    RecoverySnapshotStore,
)

_SUPPORTED_ALERT_CHANNELS = frozenset({Channel.SMS, Channel.EMAIL, Channel.PUSH})
_events = logging.getLogger("prediction_markets.events.runtime")


def _configured_alert_recipients(raw: str) -> tuple[Recipient, ...]:
    """Parse logical Alertmanager recipients from a JSON environment value.

    Parameters
    ----------
    raw
        JSON array containing ``id``, ``channel``, and ``address`` strings.

    Returns
    -------
    tuple[Recipient, ...]
        Validated recipients in configuration order.

    Raises
    ------
    ValueError
        If the JSON shape, channel, or recipient identity is invalid.
    """
    if not raw.strip():
        return ()
    try:
        values = json.loads(raw)
    except json.JSONDecodeError as error:
        raise ValueError("ALERT_RECIPIENTS_JSON must contain valid JSON") from error
    if not isinstance(values, list):
        raise ValueError("ALERT_RECIPIENTS_JSON must be a JSON array")
    recipients: list[Recipient] = []
    identities: set[tuple[str, Channel]] = set()
    for value in values:
        if not isinstance(value, dict):
            raise ValueError("Each alert recipient must be a JSON object")
        if any(
            not isinstance(value.get(field), str)
            for field in ("id", "channel", "address")
        ):
            raise ValueError("Alert recipient fields must be strings")
        try:
            recipient = Recipient(
                id=str(value["id"]),
                channel=Channel(str(value["channel"])),
                address=str(value["address"]),
            )
        except (KeyError, ValueError) as error:
            raise ValueError(f"Invalid alert recipient: {value!r}") from error
        if recipient.channel not in _SUPPORTED_ALERT_CHANNELS:
            raise ValueError(
                f"Unsupported alert channel: {recipient.channel.value}",
            )
        identity = (recipient.id, recipient.channel)
        if identity in identities:
            raise ValueError(
                f"Duplicate alert recipient/channel: {recipient.id}/{recipient.channel.value}",
            )
        identities.add(identity)
        recipients.append(recipient)
    return tuple(recipients)


class RuntimePersistence:
    """Own persistent runtime consumers and their ordered lifecycle."""

    def __init__(self, journal: BinaryJournal) -> None:
        self.journal = journal
        self._snapshot_store = RecoverySnapshotStore(
            Path(os.getenv("JOURNAL_SNAPSHOT_DIR", f"{journal.path}.snapshots")),
        )
        self._projector: PostgresProjector | None = None
        self._alert_delivery_worker: AlertDeliveryWorker | None = None
        self._alert_escalation_worker: AlertEscalationWorker | None = None
        self._alert_sender: AlertManagerAdapter | None = None
        self._alert_connection: Any | None = None
        self._alerting_connection: Any | None = None
        self._journal_maintenance: JournalMaintenanceWorker | None = None

    @property
    def projected_sequence(self) -> int | None:
        return self._projector.projected_sequence if self._projector else None

    @property
    def snapshot_sequence(self) -> int:
        return self._journal_maintenance.last_snapshot_sequence if self._journal_maintenance else 0

    @property
    def safe_delete_sequence(self) -> int:
        return self._journal_maintenance.safe_delete_sequence if self._journal_maintenance else 0

    @property
    def eligible_segment_count(self) -> int:
        return len(self._journal_maintenance.eligible_segments) if self._journal_maintenance else 0

    @property
    def error(self) -> BaseException | None:
        for resource in (
            self._projector,
            self._alert_delivery_worker,
            self._alert_escalation_worker,
            self._journal_maintenance,
        ):
            if resource is not None and getattr(resource, "error", None) is not None:
                return resource.error
        return None

    async def start(self) -> None:
        if os.getenv("POSTGRES_PROJECTOR_ENABLED", "1") == "0":
            return
        dsn = os.getenv("DATABASE_URL") or os.getenv("POSTGRES_DSN")
        if not dsn or self.journal is None:
            return
        recipients = _configured_alert_recipients(
            os.getenv("ALERT_RECIPIENTS_JSON", ""),
        )
        await asyncio.to_thread(
            apply_migrations,
            dsn,
            os.getenv("MIGRATIONS_DIR", "migrations"),
        )
        self._projector = PostgresProjector(
            dsn,
            self.journal,
            recipients=recipients,
        )
        await asyncio.to_thread(self._projector.validate_checkpoint)
        self._projector.start()
        self._alerting_connection = await asyncio.to_thread(
            psycopg.connect,
            dsn,
            connect_timeout=5,
        )
        incidents = PostgresIncidentRepository(
            self._alerting_connection,
            manage_transactions=False,
        )
        alerting = AlertingApplicationService(
            incidents,
            PostgresNotificationDeliveryRepository(
                self._alerting_connection,
                manage_transactions=False,
            ),
            NotificationPolicy(recipients),
        )
        self._alert_escalation_worker = AlertEscalationWorker(
            incidents,
            alerting,
            self._alerting_connection.transaction,
            poll_seconds=float(
                os.getenv("ALERT_ESCALATION_POLL_SECONDS", "60"),
            ),
            policy=EscalationPolicy(),
        )
        self._alert_escalation_worker.start()
        alertmanager_url = os.getenv("ALERTMANAGER_URL", "").strip()
        if not alertmanager_url:
            return
        self._alert_connection = await asyncio.to_thread(
            psycopg.connect,
            dsn,
            connect_timeout=5,
        )
        repository = PostgresNotificationDeliveryRepository(
            self._alert_connection,
            lease_seconds=float(os.getenv("ALERT_DELIVERY_LEASE_SECONDS", "300")),
        )
        self._alert_sender = AlertManagerAdapter(
            alertmanager_url,
            timeout_seconds=float(os.getenv("ALERTMANAGER_TIMEOUT_SECONDS", "10")),
        )
        self._alert_delivery_worker = AlertDeliveryWorker(
            repository,
            self._alert_sender,
            batch_size=int(os.getenv("ALERT_DELIVERY_BATCH_SIZE", "32")),
            poll_seconds=float(os.getenv("ALERT_DELIVERY_POLL_SECONDS", "1")),
        )
        self._alert_delivery_worker.start()

    def start_maintenance(self) -> None:
        assert self.journal is not None
        assert self._snapshot_store is not None
        projected_sequence = (
            (lambda: self._projector.projected_sequence)
            if self._projector is not None
            else None
        )
        self._journal_maintenance = JournalMaintenanceWorker(
            self.journal,
            self._snapshot_store,
            interval_seconds=float(os.getenv("JOURNAL_SNAPSHOT_INTERVAL_SECONDS", "30")),
            retention_mode=os.getenv("JOURNAL_RETENTION_MODE", "delete"),
            projected_sequence=projected_sequence,
        )
        self._journal_maintenance.start()

    def recovery_records(self) -> tuple[JournalRecord, ...]:
        """Return the newest valid snapshot followed by its retained journal tail."""
        assert self.journal is not None
        assert self._snapshot_store is not None
        snapshot = self._snapshot_store.load_latest(
            through_sequence=self.journal.last_sequence,
        )
        if snapshot is None:
            if self.journal.retained_through_sequence > 0:
                raise RuntimeError(
                    "Journal history was retained without a valid recovery snapshot",
                )
            return self.journal.entries()
        if self.journal.retained_through_sequence > snapshot.journal_sequence:
            raise RuntimeError("Recovery snapshot is older than retained journal history")
        return snapshot.records() + self.journal.entries(
            after_sequence=snapshot.journal_sequence,
        )

    async def close(self) -> None:
        if self._alert_escalation_worker is not None:
            await self._alert_escalation_worker.close()
            self._alert_escalation_worker = None
        if self._journal_maintenance is not None:
            await asyncio.to_thread(self._journal_maintenance.close)
        if self.journal is not None:
            await asyncio.to_thread(self.journal.sync)
        if self._projector is not None:
            if self.journal is not None:
                await asyncio.to_thread(
                    self._projector.drain,
                    self.journal.durable_sequence,
                )
        if self._journal_maintenance is not None:
            try:
                await asyncio.to_thread(self._journal_maintenance.run_once)
            except Exception as error:
                _events.warning("Final journal snapshot failed: %s", error)
        if self._projector is not None:
            await asyncio.to_thread(self._projector.close)
            self._projector = None
        if self._alert_delivery_worker is not None:
            await self._alert_delivery_worker.close()
            self._alert_delivery_worker = None
        if self._alert_sender is not None:
            await self._alert_sender.close()
            self._alert_sender = None
        if self._alert_connection is not None:
            await asyncio.to_thread(self._alert_connection.close)
            self._alert_connection = None
        if self._alerting_connection is not None:
            await asyncio.to_thread(self._alerting_connection.close)
            self._alerting_connection = None
        self._journal_maintenance = None
        if self.journal is not None:
            await asyncio.to_thread(self.journal.close)
