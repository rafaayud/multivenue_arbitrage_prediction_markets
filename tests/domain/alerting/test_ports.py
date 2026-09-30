"""Verify the alerting domain port contracts.

Responsibilities
----------------
- Pin the abstract surface that infrastructure adapters must implement.
"""

import inspect

from prediction_markets.domain.alerting.ports import (
    AlertingPort,
    IncidentRepositoryPort,
    NotificationDeliveryRepositoryPort,
    NotificationSenderPort,
)


def test_alerting_port_declares_incident_and_notification_use_cases() -> None:
    """Keep the inbound alerting surface stable for application services."""
    assert AlertingPort.__abstractmethods__ == {
        "report_incident",
        "acknowledge_incident",
        "mark_incident_in_progress",
        "resolve_incident",
        "change_incident_severity",
        "request_notifications",
        "retry_notification",
    }
    assert tuple(inspect.signature(AlertingPort.report_incident).parameters) == (
        "self",
        "incident_id",
        "fingerprint",
        "severity",
        "source",
        "description",
        "opened_at",
        "title",
    )


def test_incident_repository_port_declares_persistence_surface() -> None:
    """Keep the incident persistence contract stable for adapters."""
    assert IncidentRepositoryPort.__abstractmethods__ == {
        "get_incident",
        "get_active_by_fingerprint",
        "list_active",
        "add_incident",
        "update_incident",
    }


def test_notification_delivery_repository_port_declares_persistence_surface() -> None:
    """Keep the delivery persistence contract stable for adapters."""
    assert NotificationDeliveryRepositoryPort.__abstractmethods__ == {
        "get_notification_delivery",
        "claim_pending",
        "add_notification_delivery",
        "update_notification_delivery",
    }
    assert tuple(
        inspect.signature(NotificationDeliveryRepositoryPort.claim_pending).parameters
    ) == ("self", "limit", "at")


def test_notification_sender_port_is_async_and_single_method() -> None:
    """Keep the sender contract awaitable for async runtimes."""
    assert NotificationSenderPort.__abstractmethods__ == {"send_notification"}
    assert inspect.iscoroutinefunction(NotificationSenderPort.send_notification)
