"""Verify authenticated manual alerting lifecycle actions."""

from fastapi import FastAPI
from fastapi.testclient import TestClient

from prediction_markets.api.dependencies import get_alerting_port
from prediction_markets.api.routers.alerting import router


class _Alerting:
    """Capture manual commands received through the HTTP adapter."""

    def __init__(self) -> None:
        self.calls = []

    def acknowledge_incident(self, incident_id, at) -> None:
        self.calls.append(("acknowledge", incident_id.value))

    def mark_incident_in_progress(self, incident_id, at) -> None:
        self.calls.append(("in_progress", incident_id.value))

    def retry_notification(self, delivery_id, at) -> None:
        self.calls.append(("retry", delivery_id.value))


def test_manual_alerting_actions_require_auth_and_use_inbound_port(monkeypatch) -> None:
    """Protect operational mutations and dispatch them through AlertingPort."""
    monkeypatch.setenv("TRADING_API_KEY", "test-key")
    alerting = _Alerting()
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_alerting_port] = lambda: alerting

    with TestClient(app) as client:
        assert (
            client.post("/alerts/incidents/incident-1/acknowledge").status_code
            == 401
        )
        headers = {"X-Trading-Key": "test-key"}
        acknowledged = client.post(
            "/alerts/incidents/incident-1/acknowledge",
            headers=headers,
        )
        in_progress = client.post(
            "/alerts/incidents/incident-1/in-progress",
            headers=headers,
        )
        retried = client.post(
            "/alerts/deliveries/delivery-1/retry",
            headers=headers,
        )

    assert acknowledged.json() == {"status": "acknowledged"}
    assert in_progress.json() == {"status": "in_progress"}
    assert retried.json() == {"status": "pending"}
    assert alerting.calls == [
        ("acknowledge", "incident-1"),
        ("in_progress", "incident-1"),
        ("retry", "delivery-1"),
    ]
