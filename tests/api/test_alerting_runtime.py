"""Verify runtime alert recipient configuration."""

import asyncio
from pathlib import Path

import pytest

import prediction_markets.api.runtime.persistence as persistence_module
from prediction_markets.api.runtime.persistence import (
    RuntimePersistence,
    _configured_alert_recipients,
)
from prediction_markets.domain.alerting.enums import Channel
from prediction_markets.infrastructure.binary_journal import BinaryJournal


def test_configured_alert_recipients_accepts_supported_channels() -> None:
    """Preserve logical routing identities and audit addresses."""
    recipients = _configured_alert_recipients(
        '[{"id":"on-call","channel":"sms","address":"alertmanager:on-call"},'
        '{"id":"mobile","channel":"push","address":"mobile-app"}]',
    )

    assert [(value.id, value.channel) for value in recipients] == [
        ("on-call", Channel.SMS),
        ("mobile", Channel.PUSH),
    ]


def test_configured_alert_recipients_rejects_unsupported_voice() -> None:
    """Keep the partial implementation limited to SMS, email, and push."""
    with pytest.raises(ValueError, match="Unsupported alert channel"):
        _configured_alert_recipients(
            '[{"id":"voice","channel":"voice","address":"not-enabled"}]',
        )


def test_runtime_starts_escalation_without_alertmanager(
    monkeypatch,
    tmp_path: Path,
) -> None:
    """Keep timeout escalation active even when external delivery is disabled."""

    class _Connection:
        def transaction(self):
            raise AssertionError("The fake worker must not run a scan")

        def close(self) -> None:
            pass

    class _Projector:
        def __init__(self, *args, **kwargs) -> None:
            self.started = False
            self.validated = False

        def validate_checkpoint(self) -> None:
            self.validated = True

        def start(self) -> None:
            self.started = True

        def drain(self, _sequence: int) -> None:
            pass

        def close(self) -> None:
            pass

    class _EscalationWorker:
        def __init__(self, *args, **kwargs) -> None:
            self.started = False

        def start(self) -> None:
            self.started = True

        async def close(self) -> None:
            pass

    connection = _Connection()
    monkeypatch.setenv("POSTGRES_DSN", "postgresql://unused")
    monkeypatch.setenv("POSTGRES_PROJECTOR_ENABLED", "1")
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.delenv("ALERTMANAGER_URL", raising=False)
    monkeypatch.setattr(persistence_module, "apply_migrations", lambda *args: None)
    monkeypatch.setattr(
        persistence_module.psycopg,
        "connect",
        lambda *args, **kwargs: connection,
    )
    monkeypatch.setattr(persistence_module, "PostgresProjector", _Projector)
    monkeypatch.setattr(
        persistence_module,
        "AlertEscalationWorker",
        _EscalationWorker,
    )
    persistence = RuntimePersistence(BinaryJournal(tmp_path / "alerts.log"))

    asyncio.run(persistence.start())

    assert persistence._projector.validated
    assert persistence._projector.started
    assert persistence._alert_escalation_worker.started
    assert persistence._alert_delivery_worker is None
    asyncio.run(persistence.close())
