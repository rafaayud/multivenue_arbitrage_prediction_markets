"""Create HTTP clients instrumented with low-cardinality venue metrics.

Responsibilities
----------------
- Observe responses from application-owned ``httpx`` clients.
- Normalize dynamic resource identifiers before using paths as metric labels.
"""

import time
from collections.abc import Mapping, Sequence
from typing import Any

import httpx

from prediction_markets.infrastructure.operational_metrics import (
    HTTP_REQUEST_DURATION,
    HTTP_REQUESTS,
    HTTP_TRANSPORT_FAILURES,
)


_STARTED_AT = "prediction_markets.started_at"
_STATIC_RESOURCE_NAMES = {
    "active",
    "book",
    "events",
    "fees",
    "health",
    "klines",
    "markets",
    "orderbook",
    "positions",
    "search",
    "series",
    "slug",
    "slugs",
    "status",
    "time",
    "trades",
}
_RESOURCE_PARENTS = {"contracts", "events", "markets", "orders", "slug"}


class _InstrumentedAsyncClient(httpx.AsyncClient):
    """Record transport failures in addition to normal response hooks."""

    def __init__(self, venue: str, **kwargs: Any) -> None:
        self._metrics_venue = venue
        configured = kwargs.pop("event_hooks", None)
        event_hooks = _copied_event_hooks(configured)

        async def record_start(request: httpx.Request) -> None:
            _record_start(request)

        async def record_response(response: httpx.Response) -> None:
            _record_response(venue, response)

        event_hooks.setdefault("request", []).append(record_start)
        event_hooks.setdefault("response", []).append(record_response)
        super().__init__(event_hooks=event_hooks, **kwargs)

    async def request(
        self,
        method: str,
        url: httpx.URL | str,
        **kwargs: Any,
    ) -> httpx.Response:
        """Send one request and count failures without an HTTP response."""
        started_at = time.monotonic()
        try:
            return await super().request(method, url, **kwargs)
        except (httpx.RequestError, OSError) as error:
            _record_transport_failure(
                self._metrics_venue,
                method,
                url,
                error,
                started_at,
            )
            raise


class _InstrumentedClient(httpx.Client):
    """Record synchronous responses, durations, and transport failures."""

    def __init__(self, venue: str, **kwargs: Any) -> None:
        self._metrics_venue = venue
        configured = kwargs.pop("event_hooks", None)
        event_hooks = _copied_event_hooks(configured)
        event_hooks.setdefault("request", []).append(_record_start)
        event_hooks.setdefault("response", []).append(
            lambda response: _record_response(venue, response),
        )
        super().__init__(event_hooks=event_hooks, **kwargs)

    def request(
        self,
        method: str,
        url: httpx.URL | str,
        **kwargs: Any,
    ) -> httpx.Response:
        """Send one request and count failures without an HTTP response."""
        started_at = time.monotonic()
        try:
            return super().request(method, url, **kwargs)
        except (httpx.RequestError, OSError) as error:
            _record_transport_failure(
                self._metrics_venue,
                method,
                url,
                error,
                started_at,
            )
            raise


def instrumented_async_client(
    venue: str,
    **kwargs: Any,
) -> httpx.AsyncClient:
    """Create an ``httpx`` client that records response counts and latency.

    Parameters
    ----------
    venue
        Stable venue label used by Prometheus.
    **kwargs
        Arguments forwarded to ``httpx.AsyncClient``.

    Returns
    -------
    httpx.AsyncClient
        Application-owned client with request and response hooks installed.

    Notes
    -----
    - Caller-provided event hooks are preserved and run before metric hooks.
    - Transport failures are recorded separately from HTTP responses.
    """

    return _InstrumentedAsyncClient(venue, **kwargs)


def instrumented_client(venue: str, **kwargs: Any) -> httpx.Client:
    """Create a synchronous client with response and transport metrics.

    Parameters
    ----------
    venue
        Stable venue label used by Prometheus.
    **kwargs
        Arguments forwarded to ``httpx.Client``.

    Returns
    -------
    httpx.Client
        Application-owned keep-alive-capable synchronous client.
    """
    return _InstrumentedClient(venue, **kwargs)


def _record_start(request: httpx.Request) -> None:
    request.extensions[_STARTED_AT] = time.monotonic()


def _record_response(venue: str, response: httpx.Response) -> None:
    request = response.request
    labels = {
        "venue": venue,
        "host": request.url.host or "unknown",
        "method": request.method,
        "endpoint": _normalized_endpoint(request.url.path),
    }
    HTTP_REQUESTS.labels(**labels, status=str(response.status_code)).inc()
    started_at = request.extensions.get(_STARTED_AT)
    if isinstance(started_at, (int, float)):
        HTTP_REQUEST_DURATION.labels(**labels).observe(
            max(0.0, time.monotonic() - started_at),
        )


def _record_transport_failure(
    venue: str,
    method: str,
    url: httpx.URL | str,
    error: BaseException,
    started_at: float,
) -> None:
    """Count and time one request that failed before an HTTP response existed."""
    parsed = url if isinstance(url, httpx.URL) else httpx.URL(url)
    labels = (
        venue,
        parsed.host or "unknown",
        method.upper(),
        _normalized_endpoint(parsed.path),
    )
    HTTP_TRANSPORT_FAILURES.labels(
        *labels,
        _transport_failure_kind(error),
    ).inc()
    HTTP_REQUEST_DURATION.labels(*labels).observe(
        max(0.0, time.monotonic() - started_at),
    )


def _transport_failure_kind(error: BaseException) -> str:
    """Map transport exceptions to bounded Prometheus label values."""
    if isinstance(
        error,
        (
            httpx.ConnectTimeout,
            httpx.ReadTimeout,
            httpx.WriteTimeout,
            httpx.PoolTimeout,
        ),
    ):
        return "timeout"
    if isinstance(error, httpx.ConnectError):
        return "connect"
    if isinstance(error, httpx.RequestError):
        return "request"
    return "os"


def _copied_event_hooks(
    configured: Mapping[str, Sequence[Any]] | None,
) -> dict[str, list[Any]]:
    """Copy caller hooks so adding metric callbacks cannot mutate input state."""
    return {
        name: list(callbacks)
        for name, callbacks in (configured or {}).items()
    }


def _normalized_endpoint(path: str) -> str:
    """Replace dynamic resource path segments with a bounded ``{id}`` label."""
    parts = [part for part in path.split("/") if part]
    for index, part in enumerate(parts):
        if (
            index > 0
            and parts[index - 1] in _RESOURCE_PARENTS
            and part not in _STATIC_RESOURCE_NAMES
        ):
            parts[index] = "{id}"
    return "/" + "/".join(parts)
