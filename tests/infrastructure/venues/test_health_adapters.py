"""Verify venue-specific health normalization."""

import asyncio

import httpx

from prediction_markets.domain.venue_health import VenueHealthStatus
from prediction_markets.infrastructure.venues.limitless.health import (
    LimitlessHealthAdapter,
)
from prediction_markets.infrastructure.venues.polymarket.health import (
    PolymarketHealthAdapter,
)
from prediction_markets.infrastructure.venues.predict.health import (
    PredictHealthAdapter,
)


class _Client:
    def __init__(
        self,
        responses: dict[str, tuple[int, object] | Exception],
    ) -> None:
        self.responses = responses

    async def get(self, url: str, **_kwargs) -> httpx.Response:
        response = self.responses[url]
        if isinstance(response, Exception):
            raise response
        status, payload = response
        return httpx.Response(
            status,
            json=payload,
            request=httpx.Request("GET", url),
        )


def test_health_adapters_normalize_outages_and_rate_limits() -> None:
    """Distinguish operational, official outage, and rate-limited venues."""

    async def run() -> None:
        polymarket = PolymarketHealthAdapter(
            client=_Client(
                {
                    "https://clob.test/time": (200, 123),
                    "https://status.test/components": (
                        200,
                        {
                            "components": [
                                {
                                    "name": "Trading API (CLOB)",
                                    "status": "MAJOROUTAGE",
                                }
                            ]
                        },
                    ),
                }
            ),
            clob_url="https://clob.test",
            components_url="https://status.test/components",
        )
        predict = PredictHealthAdapter(
            api_key="key",
            base_url="https://predict.test",
            client=_Client({"https://predict.test/v1/markets": (429, {})}),
        )
        limitless = LimitlessHealthAdapter(
            base_url="https://limitless.test",
            client=_Client(
                {"https://limitless.test/markets/active/slugs": (200, [])}
            ),
        )

        poly, pred, limit = await asyncio.gather(
            polymarket.check(),
            predict.check(),
            limitless.check(),
        )

        assert poly.status is VenueHealthStatus.UNAVAILABLE
        assert poly.message == "Trading API (CLOB): MAJOROUTAGE"
        assert poly.error_type == "OfficialMajorOutage"
        assert poly.http_status == 200
        assert poly.retryable is True
        assert pred.status is VenueHealthStatus.DEGRADED
        assert pred.message == "Rate limited (HTTP 429)"
        assert pred.error_type == "RateLimitError"
        assert pred.http_status == 429
        assert pred.retryable is True
        assert limit.status is VenueHealthStatus.OPERATIONAL
        assert limit.error_type is None
        assert limit.http_status == 200

    asyncio.run(run())


def test_polymarket_ignores_unrelated_component_degradation() -> None:
    """Keep prediction trading operational when only perpetuals are affected."""

    async def run() -> None:
        polymarket = PolymarketHealthAdapter(
            client=_Client(
                {
                    "https://clob.test/time": (200, 123),
                    "https://status.test/components": (
                        200,
                        {
                            "components": [
                                {
                                    "name": "  Trading API (CLOB)",
                                    "status": "OPERATIONAL",
                                },
                                {
                                    "name": "Perpetuals trading",
                                    "status": "DEGRADEDPERFORMANCE",
                                },
                            ]
                        },
                    ),
                }
            ),
            clob_url="https://clob.test",
            components_url="https://status.test/components",
        )

        snapshot = await polymarket.check()

        assert snapshot.status is VenueHealthStatus.OPERATIONAL
        assert snapshot.message == "Trading API (CLOB): OPERATIONAL"
        assert snapshot.error_type is None
        assert snapshot.http_status == 200
        assert snapshot.retryable is False

    asyncio.run(run())


def test_polymarket_keeps_clob_operational_when_status_component_times_out() -> None:
    """Treat the official status page as advisory after the CLOB responds."""

    async def run() -> None:
        polymarket = PolymarketHealthAdapter(
            client=_Client(
                {
                    "https://clob.test/time": (200, 123),
                    "https://status.test/components": TimeoutError(),
                }
            ),
            clob_url="https://clob.test",
            components_url="https://status.test/components",
        )

        snapshot = await polymarket.check()

        assert snapshot.status is VenueHealthStatus.OPERATIONAL
        assert snapshot.message.endswith("TimeoutError")
        assert snapshot.http_status == 200

    asyncio.run(run())
