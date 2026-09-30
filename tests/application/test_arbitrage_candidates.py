"""Exercise the arbitrage candidate application query."""

import asyncio
from datetime import timedelta
from decimal import Decimal

from prediction_markets.application.markets.arbitrage_candidates import (
    ArbitrageCandidateQuery,
    ListArbitrageCandidates,
)
from prediction_markets.domain.ports.arbitrage_stream import (
    ArbitrageReturn,
    ArbitrageStreamPort,
)


class _Source(ArbitrageStreamPort):
    """Record the query passed by the application service."""

    def __init__(self) -> None:
        self.query: dict[str, object] | None = None

    async def list_candidates(self, **kwargs) -> tuple[ArbitrageReturn, ...]:
        self.query = kwargs
        return ()

    async def stream_returns(self):
        if False:
            yield ArbitrageReturn  # pragma: no cover


def test_list_arbitrage_candidates_delegates_provider_neutral_filters() -> None:
    source = _Source()
    service = ListArbitrageCandidates(source)
    query = ArbitrageCandidateQuery(
        min_return=Decimal("0.02"),
        limit=12,
        topics=("crypto",),
        search_text="bitcoin",
        live_only=True,
        min_time_to_close=timedelta(minutes=5),
        max_time_to_close=timedelta(hours=2),
        match_statuses=("verified",),
    )

    assert asyncio.run(service.execute(query)) == ()
    assert source.query == {
        "min_return": Decimal("0.02"),
        "limit": 12,
        "topics": ("crypto",),
        "search_text": "bitcoin",
        "live_only": True,
        "min_time_to_close": timedelta(minutes=5),
        "max_time_to_close": timedelta(hours=2),
        "match_statuses": ("verified",),
    }
