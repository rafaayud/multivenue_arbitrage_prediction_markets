"""Verify low-cardinality metrics emitted by owned HTTP clients."""

import asyncio
from unittest.mock import Mock

import httpx
import pytest

from prediction_markets.infrastructure import http_client


@pytest.mark.parametrize(
    ("path", "expected"),
    (
        ("/v1/markets/1518434", "/v1/markets/{id}"),
        ("/events/slug/btc-up-or-down", "/events/slug/{id}"),
        ("/markets/active/slugs", "/markets/active/slugs"),
        ("/markets/btc-hourly/orderbook", "/markets/{id}/orderbook"),
    ),
)
def test_endpoint_normalization_bounds_dynamic_labels(
    path: str,
    expected: str,
) -> None:
    """Keep resource IDs and slugs out of Prometheus label values."""
    assert http_client._normalized_endpoint(path) == expected


def test_instrumented_client_records_normalized_429(monkeypatch) -> None:
    """Count a Predict rate limit without exposing a market ID label."""
    requests = Mock()
    duration = Mock()
    monkeypatch.setattr(http_client, "HTTP_REQUESTS", requests)
    monkeypatch.setattr(http_client, "HTTP_REQUEST_DURATION", duration)

    async def run() -> None:
        client = http_client.instrumented_async_client(
            "predict",
            transport=httpx.MockTransport(lambda _request: httpx.Response(429)),
        )
        try:
            await client.get("https://api.predict.fun/v1/markets/1518434")
        finally:
            await client.aclose()

    asyncio.run(run())

    labels = {
        "venue": "predict",
        "host": "api.predict.fun",
        "method": "GET",
        "endpoint": "/v1/markets/{id}",
    }
    requests.labels.assert_called_once_with(**labels, status="429")
    requests.labels.return_value.inc.assert_called_once_with()
    duration.labels.assert_called_once_with(**labels)
    duration.labels.return_value.observe.assert_called_once()


def test_instrumented_sync_client_records_response_and_transport_failure(
    monkeypatch,
) -> None:
    """Instrument Predict's synchronous keep-alive client on both outcomes."""
    requests = Mock()
    duration = Mock()
    failures = Mock()
    monkeypatch.setattr(http_client, "HTTP_REQUESTS", requests)
    monkeypatch.setattr(http_client, "HTTP_REQUEST_DURATION", duration)
    monkeypatch.setattr(http_client, "HTTP_TRANSPORT_FAILURES", failures)

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/broken"):
            raise httpx.ConnectError("offline", request=request)
        return httpx.Response(200)

    with http_client.instrumented_client(
        "predict",
        transport=httpx.MockTransport(handler),
    ) as client:
        client.get("https://api.predict.fun/v1/markets/1518434")
        with pytest.raises(httpx.ConnectError):
            client.get("https://api.predict.fun/v1/orders/broken")

    labels = {
        "venue": "predict",
        "host": "api.predict.fun",
        "method": "GET",
        "endpoint": "/v1/markets/{id}",
    }
    requests.labels.assert_called_once_with(**labels, status="200")
    assert duration.labels.call_count == 2
    duration.labels.assert_any_call(**labels)
    duration.labels.assert_any_call(
        "predict",
        "api.predict.fun",
        "GET",
        "/v1/orders/{id}",
    )
    assert duration.labels.return_value.observe.call_count == 2
    failures.labels.assert_called_once_with(
        "predict",
        "api.predict.fun",
        "GET",
        "/v1/orders/{id}",
        "connect",
    )
