"""Exercise Alertmanager notification sender behavior in the infrastructure alerting layer.

Responsibilities
----------------
- Verify payload mapping, request targeting, and failure handling.
"""

import asyncio
from datetime import datetime, timezone
import json

import httpx
import pytest

from prediction_markets.domain.alerting.entities import (
    NotificationDelivery,
    NotificationSnapshot,
)
from prediction_markets.domain.alerting.enums import (
    Channel,
    DeliveryStatus,
    NotificationState,
    Severity,
)
from prediction_markets.domain.alerting.value_objects import (
    AlertSource,
    DeliveryID,
    Fingerprint,
    IncidentID,
    Recipient,
)
from prediction_markets.domain.shared.value_objects import Timestamp
from prediction_markets.infrastructure.alerting.notifications.alertmanager.alertmanager_adapter import (
    AlertManagerAdapter,
)

OPENED_AT = Timestamp(datetime(2026, 8, 16, 20, 0, 0, tzinfo=timezone.utc))


def _delivery(**overrides) -> NotificationDelivery:
    defaults = dict(
        fingerprint=Fingerprint("hedge:polymarket:0xabc"),
        state=NotificationState.FIRING,
        severity=Severity.CRITICAL,
        source=AlertSource(
            component="execution",
            service="trading-runtime",
            instance="pod-1",
        ),
        description="Leg 2 rejected by the risk guard",
        starts_at=OPENED_AT,
        title="HedgeExecutionFailed",
        summary="Hedge execution failed",
    )
    return NotificationDelivery(
        id=DeliveryID("delivery-1"),
        incident_id=IncidentID("inc-1"),
        recipient=Recipient(
            id="trading-on-call",
            channel=Channel.SMS,
            address="configured-in-alertmanager",
        ),
        snapshot=NotificationSnapshot(**(defaults | overrides)),
        status=DeliveryStatus.PENDING,
        requested_at=OPENED_AT,
    )


def test_send_notification_posts_mapped_alert_array() -> None:
    """Map the incident into labels/annotations and target the v2 alerts endpoint."""

    async def run_test() -> None:
        requests: list[httpx.Request] = []

        async def handler(request: httpx.Request) -> httpx.Response:
            requests.append(request)
            return httpx.Response(200)

        client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        adapter = AlertManagerAdapter(
            base_url="https://am.example.test/",
            client=client,
        )

        result = await adapter.send_notification(_delivery())

        assert result is None
        assert len(requests) == 1

        request = requests[0]
        assert request.url == "https://am.example.test/api/v2/alerts"
        assert request.headers["Content-Type"] == "application/json"

        body = json.loads(request.content)
        assert isinstance(body, list)
        assert len(body) == 1

        alert = body[0]
        assert alert["labels"] == {
            "alertname": "HedgeExecutionFailed",
            "severity": "critical",
            "fingerprint": "hedge:polymarket:0xabc",
            "component": "execution",
            "recipient": "trading-on-call",
            "channel": "sms",
            "service": "trading-runtime",
            "instance": "pod-1",
        }
        assert alert["annotations"] == {
            "summary": "Hedge execution failed",
            "description": "Leg 2 rejected by the risk guard",
        }
        assert alert["startsAt"] == "2026-08-16T20:00:00+00:00"
        assert "endsAt" not in alert

    asyncio.run(run_test())


def test_payload_omits_optional_fields_when_absent() -> None:
    """Fall back to the component name and skip null labels or annotations."""

    async def run_test() -> None:
        requests: list[httpx.Request] = []

        async def handler(request: httpx.Request) -> httpx.Response:
            requests.append(request)
            return httpx.Response(200)

        client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        adapter = AlertManagerAdapter(
            base_url="https://am.example.test",
            client=client,
        )
        delivery = _delivery(
            title=None,
            summary=None,
            source=AlertSource(component="execution"),
        )

        await adapter.send_notification(delivery)

        alert = json.loads(requests[0].content)[0]
        assert alert["labels"] == {
            "alertname": "execution",
            "severity": "critical",
            "fingerprint": "hedge:polymarket:0xabc",
            "component": "execution",
            "recipient": "trading-on-call",
            "channel": "sms",
        }
        assert alert["annotations"] == {
            "description": "Leg 2 rejected by the risk guard",
        }

    asyncio.run(run_test())


def test_resolved_snapshot_sets_alertmanager_end_time() -> None:
    """Resolve the exact captured label set instead of reloading an incident."""

    async def run_test() -> None:
        requests: list[httpx.Request] = []

        async def handler(request: httpx.Request) -> httpx.Response:
            requests.append(request)
            return httpx.Response(200)

        client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        adapter = AlertManagerAdapter("https://am.example.test", client=client)
        ends_at = Timestamp(
            datetime(2026, 8, 16, 20, 5, tzinfo=timezone.utc),
        )

        await adapter.send_notification(
            _delivery(state=NotificationState.RESOLVED, ends_at=ends_at),
        )

        assert json.loads(requests[0].content)[0]["endsAt"] == (
            "2026-08-16T20:05:00+00:00"
        )

    asyncio.run(run_test())


def test_one_shot_firing_snapshot_sets_equal_start_and_end_time() -> None:
    """Send endsAt for firing one-shot alerts so Alertmanager does not repeat them."""

    async def run_test() -> None:
        requests: list[httpx.Request] = []

        async def handler(request: httpx.Request) -> httpx.Response:
            requests.append(request)
            return httpx.Response(200)

        client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        adapter = AlertManagerAdapter("https://am.example.test", client=client)

        await adapter.send_notification(
            _delivery(
                state=NotificationState.FIRING,
                fingerprint=Fingerprint("trade-completed:execution-1"),
                severity=Severity.INFORMATIONAL,
                title="Trade completed",
                description="Execution execution-1 completed",
                ends_at=OPENED_AT,
            ),
        )

        alert = json.loads(requests[0].content)[0]
        assert alert["startsAt"] == "2026-08-16T20:00:00+00:00"
        assert alert["endsAt"] == "2026-08-16T20:00:00+00:00"

    asyncio.run(run_test())


def test_send_notification_raises_on_error_status() -> None:
    """Propagate Alertmanager rejections so deliveries can be marked failed."""

    async def run_test() -> None:
        async def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(500, json={"error": "boom"})

        client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        adapter = AlertManagerAdapter(
            base_url="https://am.example.test",
            client=client,
        )

        with pytest.raises(httpx.HTTPStatusError):
            await adapter.send_notification(_delivery())

    asyncio.run(run_test())


def test_send_notification_raises_on_transport_error() -> None:
    """Propagate connectivity failures instead of reporting success."""

    async def run_test() -> None:
        async def handler(request: httpx.Request) -> httpx.Response:
            raise httpx.ConnectError("unreachable")

        client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        adapter = AlertManagerAdapter(
            base_url="https://am.example.test",
            client=client,
        )

        with pytest.raises(httpx.TransportError):
            await adapter.send_notification(_delivery())

    asyncio.run(run_test())


def test_init_rejects_non_positive_timeout() -> None:
    """Reject non-positive timeouts at construction."""
    with pytest.raises(ValueError, match="timeout_seconds must be positive"):
        AlertManagerAdapter(base_url="https://am.example.test", timeout_seconds=0)


def test_close_only_closes_owned_clients() -> None:
    """Keep caller-supplied clients open while releasing owned ones."""

    async def run_test() -> None:
        injected = httpx.AsyncClient()
        adapter = AlertManagerAdapter(
            base_url="https://am.example.test",
            client=injected,
        )
        await adapter.close()
        assert not injected.is_closed
        await injected.aclose()

        owned = AlertManagerAdapter(base_url="https://am.example.test")
        await owned.close()
        assert owned.client.is_closed

    asyncio.run(run_test())
