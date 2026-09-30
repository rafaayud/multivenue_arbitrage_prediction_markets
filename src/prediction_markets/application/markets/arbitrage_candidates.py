"""List normalized arbitrage candidates through an application use case.

Responsibilities
----------------
- Accept provider-neutral candidate filters.
- Delegate candidate retrieval to the arbitrage stream port.

Notes
-----
- This use case is read-only and does not publish pipeline inputs, journal
  events, or execution commands.
"""

from dataclasses import dataclass
from datetime import timedelta
from decimal import Decimal
from typing import Literal

from prediction_markets.domain.ports.arbitrage_stream import (
    ArbitrageReturn,
    ArbitrageStreamPort,
)

CandidateMatchStatus = Literal["matched", "verified"]


@dataclass(frozen=True, slots=True)
class ArbitrageCandidateQuery:
    """Define provider-neutral filters for one candidate listing request.

    Attributes
    ----------
    min_return
        Minimum decimal return rate accepted by the source.
    limit
        Maximum number of candidates returned.
    topics
        Optional normalized topic filters.
    search_text
        Optional case-insensitive event search text.
    live_only
        Whether candidates must be inside their reported event window.
    min_time_to_close
        Optional minimum remaining market lifetime.
    max_time_to_close
        Optional maximum remaining market lifetime.
    match_statuses
        Source match states eligible for the result.
    """

    min_return: Decimal = Decimal("0")
    limit: int = 100
    topics: tuple[str, ...] = ()
    search_text: str | None = None
    live_only: bool = False
    min_time_to_close: timedelta | None = None
    max_time_to_close: timedelta | None = None
    match_statuses: tuple[CandidateMatchStatus, ...] = ("matched", "verified")


class ListArbitrageCandidates:
    """List normalized arbitrage candidates from an injected source."""

    def __init__(self, source: ArbitrageStreamPort) -> None:
        """
        Parameters
        ----------
        source
            Port used to retrieve normalized candidates from an external source.
        """
        self._source = source

    async def execute(
        self,
        query: ArbitrageCandidateQuery = ArbitrageCandidateQuery(),
    ) -> tuple[ArbitrageReturn, ...]:
        """Return candidates matching the requested filters.

        Parameters
        ----------
        query
            Provider-neutral candidate filters.

        Returns
        -------
        tuple[ArbitrageReturn, ...]
            Normalized candidates returned by the injected source.
        """
        return await self._source.list_candidates(
            min_return=query.min_return,
            limit=query.limit,
            topics=query.topics,
            search_text=query.search_text,
            live_only=query.live_only,
            min_time_to_close=query.min_time_to_close,
            max_time_to_close=query.max_time_to_close,
            match_statuses=query.match_statuses,
        )
