"""Deliver immutable alert snapshots to Alertmanager over its HTTP API v2.

Responsibilities
----------------
- Translate notification deliveries into Alertmanager alert payloads and push them.

Notes
-----
- Alertmanager deduplicates alerts by their full label set, so re-sending the
  same captured alert updates it instead of creating a new one.
"""

from typing import Any

import httpx

from prediction_markets.domain.alerting.entities import NotificationDelivery
from prediction_markets.domain.alerting.ports import NotificationSenderPort


class AlertManagerAdapter(NotificationSenderPort):
    """Push captured alerts to Alertmanager via ``POST /api/v2/alerts``.

    Parameters
    ----------
    base_url : str
        Alertmanager base URL; the adapter appends ``/api/v2/alerts``.
    timeout_seconds : float, default=10.0
        HTTP timeout applied to each delivery request; must be positive.
    client : httpx.AsyncClient | None, default=None
        Injected HTTP client, mainly for tests. When omitted, the adapter
        creates and owns one.

    Notes
    -----
    - Implements ``NotificationSenderPort`` against Alertmanager API v2.
    - Alertmanager routes notifications by matching labels against its own
      configuration.
    """

    def __init__(
        self,
        base_url: str,
        timeout_seconds: float = 10.0,
        client: httpx.AsyncClient | None = None,
    ) -> None:

        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")

        self._base_url = base_url.rstrip("/")
        self._own_client = client is None
        self.client = client or httpx.AsyncClient(timeout=timeout_seconds)

    async def close(self) -> None:
        """Release network resources owned by the adapter.

        Notes
        -----
        - A caller-supplied client remains under caller ownership.
        """
        if self._own_client:
            await self.client.aclose()

    async def send_notification(
        self,
        notification_delivery: NotificationDelivery,
    ) -> str | None:
        """Push one captured delivery to Alertmanager.

        Parameters
        ----------
        notification_delivery
            Delivery whose immutable snapshot and intended recipient are mapped
            into Alertmanager labels and annotations.

        Returns
        -------
        str | None
            Always ``None``: Alertmanager answers 200 with an empty body and
            exposes no provider-side reference.

        Raises
        ------
        httpx.HTTPStatusError
            If Alertmanager rejects the request with a 4xx or 5xx response.
        httpx.TransportError
            If Alertmanager is unreachable.

        Notes
        -----
        - Firing snapshots omit ``endsAt``; resolved snapshots include their
          captured resolution time.
        """
        notification_payload = self._build_notification_payload(
            notification_delivery,
        )
        response = await self.client.post(
            f"{self._base_url}/api/v2/alerts",
            json=notification_payload,
        )
        response.raise_for_status()
        return None

    def _build_notification_payload(
        self,
        notification_delivery: NotificationDelivery,
    ) -> list[dict[str, Any]]:
        """Translate a delivery into the Alertmanager v2 alert array.

        Parameters
        ----------
        notification_delivery
            Captured delivery to convert.

        Returns
        -------
        list[dict[str, Any]]
            One-element alert array as expected by ``POST /api/v2/alerts``.
        """
        snapshot = notification_delivery.snapshot
        recipient = notification_delivery.recipient
        labels = {
            "alertname": snapshot.title or snapshot.source.component,
            "severity": snapshot.severity.value,
            "fingerprint": snapshot.fingerprint.value,
            "component": snapshot.source.component,
            "recipient": recipient.id,
            "channel": recipient.channel.value,
        }

        if snapshot.source.service:
            labels["service"] = snapshot.source.service

        if snapshot.source.instance:
            labels["instance"] = snapshot.source.instance

        annotations: dict[str, str] = {
            "description": snapshot.description,
        }

        if snapshot.summary:
            annotations["summary"] = snapshot.summary

        alert: dict[str, Any] = {
            "labels": labels,
            "annotations": annotations,
            "startsAt": snapshot.starts_at.value.isoformat(),
        }

        if snapshot.ends_at is not None:
            alert["endsAt"] = snapshot.ends_at.value.isoformat()

        return [alert]
