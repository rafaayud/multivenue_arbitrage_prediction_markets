"""Verify authenticated Alertmanager webhook delivery through FCM."""

import base64
from collections.abc import AsyncIterator
import json
from urllib.parse import parse_qs

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from fastapi import FastAPI
from fastapi.testclient import TestClient
import httpx

from prediction_markets.api.routers.alertmanager_webhook import (
    get_fcm_push_client,
    router,
)
from prediction_markets.infrastructure.alerting.notifications.fcm import FcmPushClient


def _payload() -> dict[str, object]:
    """Return one standard Alertmanager webhook v4 payload."""
    labels = {
        "alertname": "HedgeExecutionFailed",
        "severity": "critical",
        "component": "execution",
        "recipient": "mobile-app",
        "channel": "push",
    }
    return {
        "version": "4",
        "groupKey": "{}:{fingerprint=\"hedge-1\"}",
        "truncatedAlerts": 0,
        "status": "firing",
        "receiver": "mobile-push",
        "groupLabels": {"fingerprint": "hedge-1"},
        "commonLabels": labels,
        "commonAnnotations": {"summary": "Hedge failed"},
        "externalURL": "http://alertmanager:9093",
        "alerts": [
            {
                "status": "firing",
                "labels": labels,
                "annotations": {
                    "summary": "Hedge failed",
                    "description": "The recovery order was rejected",
                },
                "startsAt": "2026-08-20T10:00:00Z",
                "endsAt": "0001-01-01T00:00:00Z",
                "generatorURL": "",
                "fingerprint": "hedge-1",
            }
        ],
    }


def test_webhook_authenticates_maps_and_preserves_at_least_once(
    monkeypatch,
    tmp_path,
) -> None:
    """Authenticate separately and allow repeated deliveries to reach FCM."""
    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    private_key_pem = private_key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    ).decode()
    credential_file = tmp_path / "firebase-service-account.json"
    credential_file.write_text(
        json.dumps(
            {
                "type": "service_account",
                "project_id": "prediction-test",
                "private_key_id": "key-1",
                "private_key": private_key_pem,
                "client_email": "firebase@test.example",
                "token_uri": "https://oauth.example.test/token",
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("ALERTMANAGER_WEBHOOK_TOKEN", "alertmanager-secret")
    monkeypatch.setenv(
        "ALERT_PUSH_RECIPIENT_TOKENS_JSON",
        '{"mobile-app":"device-token-1"}',
    )
    provider_requests: list[httpx.Request] = []

    async def provider(request: httpx.Request) -> httpx.Response:
        provider_requests.append(request)
        if request.url == "https://oauth.example.test/token":
            form = parse_qs(request.content.decode())
            assertion = form["assertion"][0]
            encoded_claims = assertion.split(".")[1]
            encoded_claims += "=" * (-len(encoded_claims) % 4)
            claims = json.loads(base64.urlsafe_b64decode(encoded_claims))
            assert form["grant_type"] == [
                "urn:ietf:params:oauth:grant-type:jwt-bearer"
            ]
            assert claims["iss"] == "firebase@test.example"
            assert claims["scope"] == (
                "https://www.googleapis.com/auth/firebase.messaging"
            )
            return httpx.Response(
                200,
                json={"access_token": "oauth-token", "expires_in": 3600},
            )
        return httpx.Response(
            200,
            json={"name": "projects/prediction-test/messages/message-1"},
        )

    async def fcm_override() -> AsyncIterator[FcmPushClient]:
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(provider)
        ) as client:
            yield FcmPushClient.from_service_account_file(
                credential_file,
                timeout_seconds=4,
                client=client,
            )

    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_fcm_push_client] = fcm_override
    headers = {"Authorization": "Bearer alertmanager-secret"}
    with TestClient(app) as client:
        unauthenticated = client.post(
            "/webhooks/alertmanager/push",
            json=_payload(),
        )
        assert unauthenticated.status_code == 401
        assert (
            client.post(
                "/webhooks/alertmanager/push",
                json=_payload(),
                headers=headers,
            ).status_code
            == 204
        )
        assert (
            client.post(
                "/webhooks/alertmanager/push",
                json=_payload(),
                headers=headers,
            ).status_code
            == 204
        )

    fcm_requests = [
        request
        for request in provider_requests
        if request.url.host == "fcm.googleapis.com"
    ]
    assert len(fcm_requests) == 2
    assert fcm_requests[0].headers["Authorization"] == "Bearer oauth-token"
    message = json.loads(fcm_requests[0].content)["message"]
    assert message["token"] == "device-token-1"
    assert message["notification"] == {
        "title": "Hedge failed",
        "body": "The recovery order was rejected",
    }
    assert message["data"]["deduplication_key"] == "hedge-1:firing"


def test_provider_transport_failure_returns_retryable_status(monkeypatch) -> None:
    """Return 503 so Alertmanager retries an ambiguous provider outcome."""

    class UnavailableFcm:
        timeout_seconds = 4.0

        async def send(self, **_values) -> str:
            raise httpx.ReadError("response lost")

    async def fcm_override() -> AsyncIterator[UnavailableFcm]:
        yield UnavailableFcm()

    monkeypatch.setenv("ALERTMANAGER_WEBHOOK_TOKEN", "alertmanager-secret")
    monkeypatch.setenv(
        "ALERT_PUSH_RECIPIENT_TOKENS_JSON",
        '{"mobile-app":"device-token-1"}',
    )
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_fcm_push_client] = fcm_override

    with TestClient(app) as client:
        response = client.post(
            "/webhooks/alertmanager/push",
            json=_payload(),
            headers={"Authorization": "Bearer alertmanager-secret"},
        )

    assert response.status_code == 503
