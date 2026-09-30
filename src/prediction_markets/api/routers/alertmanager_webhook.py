"""Receive authenticated Alertmanager webhooks and deliver mobile pushes.

Responsibilities
----------------
- Authenticate Alertmanager independently from trading controls.
- Validate bounded Alertmanager webhook payloads and map recipients to FCM tokens.
- Return retryable failures until every push in the webhook is accepted by FCM.

Notes
-----
- Delivery is at-least-once. If FCM accepts a push but its response is lost,
  Alertmanager retries the webhook and the device can receive a duplicate.
"""

import asyncio
from collections.abc import AsyncIterator
import json
import os
import secrets
from typing import Annotated

from fastapi import APIRouter, Depends, Header, HTTPException, Request, Response
import httpx
from pydantic import ValidationError

from prediction_markets.api.models import (
    AlertmanagerWebhook,
    AlertmanagerWebhookAlert,
)
from prediction_markets.infrastructure.alerting.notifications.fcm import FcmPushClient

_MAX_WEBHOOK_BYTES = 64 * 1024


def _require_alertmanager_token(
    authorization: Annotated[str | None, Header()] = None,
) -> None:
    """Require the dedicated Alertmanager bearer token.

    Raises
    ------
    HTTPException
        With status 503 when unconfigured or 401 when authentication fails.
    """
    expected = os.getenv("ALERTMANAGER_WEBHOOK_TOKEN")
    if not expected:
        raise HTTPException(
            status_code=503,
            detail="ALERTMANAGER_WEBHOOK_TOKEN is not configured",
        )
    scheme, separator, supplied = (authorization or "").partition(" ")
    valid = (
        separator == " "
        and scheme.lower() == "bearer"
        and bool(supplied)
        and secrets.compare_digest(supplied, expected)
    )
    if not valid:
        raise HTTPException(
            status_code=401,
            detail="Invalid Alertmanager token",
            headers={"WWW-Authenticate": "Bearer"},
        )


async def get_fcm_push_client() -> AsyncIterator[FcmPushClient]:
    """Compose one request-scoped FCM client from environment configuration.

    Yields
    ------
    FcmPushClient
        Concrete FCM HTTP v1 client for the webhook request.

    Raises
    ------
    HTTPException
        With status 503 when credentials or timeout configuration is invalid.
    """
    credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if not credentials_path:
        raise HTTPException(
            status_code=503,
            detail="GOOGLE_APPLICATION_CREDENTIALS is not configured",
        )
    try:
        timeout_seconds = float(os.getenv("ALERT_PUSH_TIMEOUT_SECONDS", "4"))
        client = FcmPushClient.from_service_account_file(
            credentials_path,
            timeout_seconds=timeout_seconds,
        )
    except (OSError, ValueError) as error:
        raise HTTPException(
            status_code=503,
            detail="FCM configuration is invalid",
        ) from error
    try:
        yield client
    finally:
        await client.close()


router = APIRouter(
    prefix="/webhooks/alertmanager",
    tags=["alertmanager-webhook"],
    dependencies=[Depends(_require_alertmanager_token)],
)


async def _parse_webhook(request: Request) -> AlertmanagerWebhook:
    """Read and validate one size-bounded Alertmanager webhook body.

    Parameters
    ----------
    request
        Incoming request whose body is limited before JSON parsing.

    Returns
    -------
    AlertmanagerWebhook
        Validated webhook v4 payload.

    Raises
    ------
    HTTPException
        With status 413 when the body is too large or 422 when invalid.
    """
    try:
        declared_size = int(request.headers.get("content-length", "0"))
    except ValueError as error:
        raise HTTPException(status_code=422, detail="Invalid Content-Length") from error
    if declared_size > _MAX_WEBHOOK_BYTES:
        raise HTTPException(status_code=413, detail="Alertmanager webhook is too large")

    body = bytearray()
    async for chunk in request.stream():
        if len(body) + len(chunk) > _MAX_WEBHOOK_BYTES:
            raise HTTPException(
                status_code=413,
                detail="Alertmanager webhook is too large",
            )
        body.extend(chunk)
    try:
        return AlertmanagerWebhook.model_validate_json(body)
    except ValidationError as error:
        raise HTTPException(
            status_code=422,
            detail="Invalid Alertmanager webhook payload",
        ) from error


def _recipient_tokens() -> dict[str, str]:
    """Load the static recipient ID to FCM token mapping.

    Raises
    ------
    HTTPException
        With status 503 when the mapping is absent or malformed.
    """
    try:
        mapping = json.loads(os.getenv("ALERT_PUSH_RECIPIENT_TOKENS_JSON", ""))
    except json.JSONDecodeError as error:
        raise HTTPException(
            status_code=503,
            detail="ALERT_PUSH_RECIPIENT_TOKENS_JSON is invalid",
        ) from error
    if not isinstance(mapping, dict) or not mapping or not all(
        isinstance(recipient, str)
        and recipient
        and isinstance(token, str)
        and token
        for recipient, token in mapping.items()
    ):
        raise HTTPException(
            status_code=503,
            detail="ALERT_PUSH_RECIPIENT_TOKENS_JSON is invalid",
        )
    return mapping


def _push_content(alert: AlertmanagerWebhookAlert) -> tuple[str, str, dict[str, str]]:
    """Map one Alertmanager alert to a bounded FCM notification payload."""
    title = alert.annotations.get("summary") or alert.labels.get("alertname")
    title = title or "Prediction markets alert"
    if alert.status == "resolved":
        title = f"Resolved: {title}"
    body = alert.annotations.get("description") or title
    data = {
        "fingerprint": alert.fingerprint,
        "status": alert.status,
        "deduplication_key": f"{alert.fingerprint}:{alert.status}",
    }
    for label in ("severity", "component", "recipient"):
        if value := alert.labels.get(label):
            data[label] = value
    return title[:120], body[:1024], data


@router.post("/push", status_code=204)
async def receive_push_webhook(
    request: Request,
    fcm: Annotated[FcmPushClient, Depends(get_fcm_push_client)],
) -> Response:
    """Deliver every alert in an authenticated Alertmanager webhook to FCM.

    Parameters
    ----------
    request
        Authenticated HTTP request containing the Alertmanager v4 payload.
    fcm
        Request-scoped FCM client.

    Returns
    -------
    Response
        Empty 204 response after FCM accepts every push.

    Raises
    ------
    HTTPException
        With 422 for an invalid route or permanent provider rejection,
        or 503 for failures that Alertmanager should retry.

    Notes
    -----
    - Partial success is intentionally not persisted. Retrying the complete
      webhook can duplicate pushes already accepted by FCM.
    """
    webhook = await _parse_webhook(request)
    tokens = _recipient_tokens()
    try:
        async with asyncio.timeout(fcm.timeout_seconds):
            for alert in webhook.alerts:
                if alert.labels.get("channel") != "push":
                    raise HTTPException(
                        status_code=422,
                        detail="Alert is not routed to the push channel",
                    )
                recipient = alert.labels.get("recipient", "")
                token = tokens.get(recipient)
                if token is None:
                    raise HTTPException(
                        status_code=503,
                        detail=f"No push token configured for recipient {recipient!r}",
                    )
                title, body, data = _push_content(alert)
                await fcm.send(token=token, title=title, body=body, data=data)
    except HTTPException:
        raise
    except (TimeoutError, httpx.TransportError) as error:
        raise HTTPException(
            status_code=503,
            detail="Push provider is temporarily unavailable",
        ) from error
    except httpx.HTTPStatusError as error:
        status_code = error.response.status_code
        retryable = status_code == 429 or status_code >= 500
        raise HTTPException(
            status_code=503 if retryable else 422,
            detail="Push provider rejected the notification",
        ) from error
    except ValueError as error:
        raise HTTPException(
            status_code=502,
            detail="Push provider returned an invalid response",
        ) from error
    return Response(status_code=204)
