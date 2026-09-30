"""Discover open AGG markets independently of current arbitrage returns.

Responsibilities
----------------
- Fetch AGG venue events without opportunity or match-status filters.
- Normalize every selectable binary market group for catalog browsing.
"""

import asyncio
import json
import re
from decimal import Decimal, InvalidOperation
from typing import Any

import httpx

from prediction_markets.domain.ports.arbitrage_stream import ArbitrageReturn
from prediction_markets.domain.shared.value_objects import EventID, Timestamp
from prediction_markets.infrastructure.http_client import instrumented_async_client
from prediction_markets.infrastructure.agg.arbitrage_stream import (
    AggArbitrageStreamAdapter,
    _event_candidates,
    _event_matches_filters,
    _event_topics_match,
    _optional_timestamp,
    _venue_market,
)


class AggMarketExplorerAdapter(AggArbitrageStreamAdapter):
    """List AGG markets without requiring a positive arbitrage opportunity.

    Notes
    -----
    - The adapter includes unmatched and zero-return market groups.
    - AGG event timestamps determine whether an esports event is live.
    """

    async def list_markets(
        self,
        *,
        limit: int = 500,
        topics: tuple[str, ...] = (),
        search_text: str | None = None,
        live_only: bool = False,
    ) -> tuple[ArbitrageReturn, ...]:
        """Return open market groups matching catalog filters.

        Parameters
        ----------
        limit
            Maximum number of normalized market groups returned.
        topics
            Optional AGG event categories matched case-insensitively.
        search_text
            Optional case-insensitive terms, all required in the event payload.
        live_only
            Whether the event window must contain the observation time.

        Returns
        -------
        tuple[ArbitrageReturn, ...]
            Selectable groups, including entries with no current return.
        """
        if not self._app_id:
            raise ValueError("AGG market explorer requires app_id or AGG_APP_ID")
        if limit <= 0:
            raise ValueError("limit must be positive")
        normalized_topics = tuple(topic.strip().lower() for topic in topics)
        if any(not topic for topic in normalized_topics):
            raise ValueError("topics cannot contain blank values")
        normalized_search = search_text.strip().casefold() if search_text else None
        if search_text is not None and not normalized_search:
            raise ValueError("search_text cannot be blank")

        if self._client is not None:
            return await self._list_markets(
                self._client,
                limit=limit,
                topics=normalized_topics,
                search_text=normalized_search,
                live_only=live_only,
            )

        async with instrumented_async_client(
            "agg",
            timeout=self._timeout_seconds,
        ) as client:
            return await self._list_markets(
                client,
                limit=limit,
                topics=normalized_topics,
                search_text=normalized_search,
                live_only=live_only,
            )

    async def _list_markets(
        self,
        client: httpx.AsyncClient,
        *,
        limit: int,
        topics: tuple[str, ...],
        search_text: str | None,
        live_only: bool,
    ) -> tuple[ArbitrageReturn, ...]:
        """Scan filtered AGG event pages and retain unique market groups."""
        markets: dict[tuple[tuple[str, str], ...], ArbitrageReturn] = {}
        cursor: str | None = None
        headers = {"x-app-id": self._app_id}
        if self._origin:
            headers["Origin"] = self._origin
        search_terms = tuple(search_text.split()) if search_text else ()
        upstream_search = (
            search_terms[-1] if search_terms else (topics[0] if len(topics) == 1 else None)
        )

        while len(markets) < limit:
            params: list[tuple[str, str | int]] = [
                ("status", "open"),
                ("limit", 100),
            ]
            if upstream_search:
                params.append(("search", upstream_search))
            if cursor is not None:
                params.append(("cursor", cursor))

            response = await client.get(
                f"{self._rest_url}/venue-events",
                headers=headers,
                params=params,
            )
            response.raise_for_status()
            payload = response.json()
            if not isinstance(payload, dict) or not isinstance(
                payload.get("data"), list
            ):
                raise TypeError("Unexpected AGG venue-events response")

            observed_at = Timestamp.now()
            for event in payload["data"]:
                if not _event_matches_filters(
                    event,
                    observed_at,
                    topics,
                    search_text,
                    live_only,
                    None,
                    None,
                ):
                    continue
                for market in _event_candidates(
                    event,
                    observed_at,
                    min_return=None,
                    min_venues=1,
                ):
                    markets.setdefault(market.key, market)
                    if len(markets) >= limit:
                        break
                if len(markets) >= limit:
                    break

            cursor = payload.get("nextCursor")
            if search_text:
                # Targeted searches must stay within the gateway response budget.
                break
            if not payload.get("hasMore") or not isinstance(cursor, str) or not cursor:
                break

        if search_text or topics:
            fallback_markets = await self._fallback_markets(
                client,
                limit=limit,
                topics=topics,
                search_text=search_text,
                live_only=live_only,
            )
            matched_market_ids = {
                (str(item.venue_id), item.external_market_id)
                for market in fallback_markets
                if len(market.markets) > 1
                for item in market.markets
            }
            for key, market in tuple(markets.items()):
                if len(market.markets) == 1 and (
                    str(market.markets[0].venue_id),
                    market.markets[0].external_market_id,
                ) in matched_market_ids:
                    del markets[key]
            for market in fallback_markets:
                current = markets.get(market.key)
                if current is None or market.return_rate > current.return_rate:
                    markets[market.key] = market

        return tuple(
            sorted(markets.values(), key=lambda market: len(market.markets), reverse=True)
        )[:limit]

    async def _fallback_markets(
        self,
        client: httpx.AsyncClient,
        *,
        limit: int,
        topics: tuple[str, ...],
        search_text: str | None,
        live_only: bool,
    ) -> tuple[ArbitrageReturn, ...]:
        """Pair searchable venue markets using AGG metadata and event names.

        Parameters
        ----------
        client
            HTTP client used for the AGG request.
        limit
            Maximum number of normalized groups to return.
        topics
            Event categories that fallback markets must belong to.
        search_text
            Optional case-insensitive terms required in each market payload.
        live_only
            Whether the fallback event window must contain the observation time.

        Returns
        -------
        tuple[ArbitrageReturn, ...]
            Cross-venue groups that AGG exposed as separate venue markets.

        Notes
        -----
        - Topic filters fetch Limitless directly and resolve at most 20 unique
          matchups concurrently, keeping gateway latency bounded.
        - ``aggKey`` remains authoritative when participant names are unavailable.
        """
        headers = {"x-app-id": self._app_id}
        if self._origin:
            headers["Origin"] = self._origin
        search_term = search_text.split()[-1] if search_text else topics[0]
        raw_by_id: dict[str, dict[str, Any]] = {}
        limitless_by_id: dict[str, dict[str, Any]] = {}
        observed_at = Timestamp.now()

        async def fetch_page(
            term: str,
            venue: str | None = None,
        ) -> tuple[dict[str, Any], ...]:
            params: list[tuple[str, str | int]] = [
                ("status", "open"),
                ("limit", 100),
                ("context", "detail"),
                ("search", term),
            ]
            if venue:
                params.append(("venue", venue))
            response = await client.get(
                f"{self._rest_url}/venue-markets",
                headers=headers,
                params=params,
            )
            response.raise_for_status()
            payload = response.json()
            if not isinstance(payload, dict) or not isinstance(
                payload.get("data"), list
            ):
                raise TypeError("Unexpected AGG venue-markets response")
            return tuple(raw for raw in payload["data"] if isinstance(raw, dict))

        def ingest(rows: tuple[dict[str, Any], ...]) -> None:
            for raw in rows:
                market_id = str(raw.get("id") or "").strip()
                if market_id and str(raw.get("venue") or "").casefold() == "limitless":
                    limitless_by_id[market_id] = raw
                for market in (raw, *(raw.get("matchedVenueMarkets") or ())):
                    if not isinstance(market, dict):
                        continue
                    market_id = str(market.get("id") or "").strip()
                    if market_id:
                        raw_by_id[market_id] = market

        initial_pages = await asyncio.gather(
            fetch_page(search_term),
            *(
                (fetch_page(search_term, "limitless"),)
                if search_text is None
                else ()
            ),
        )
        for page in initial_pages:
            ingest(page)

        if search_text is None:
            search_terms = sorted(
                {
                    term
                    for raw in limitless_by_id.values()
                    if _fallback_matches(raw, topics, None, live_only, observed_at)
                    and (term := _fallback_search_term(raw)) is not None
                },
            )[:20]
            # ponytail: 20 concurrent exact searches bound latency and request volume.
            for page in await asyncio.gather(
                *(fetch_page(term) for term in search_terms),
            ):
                ingest(page)

        groups: dict[tuple[str, ...], dict[str, dict[str, Any]]] = {}
        for raw in raw_by_id.values():
            if not _fallback_matches(raw, topics, search_text, live_only, observed_at):
                continue
            market = _venue_market(raw)
            signature = _fallback_signature(raw)
            if market is None or signature is None:
                continue
            venue = str(market.venue_id)
            current = groups.setdefault(signature, {}).get(venue)
            if current is None or _raw_return(raw) > _raw_return(current):
                groups[signature][venue] = raw

        candidates: list[ArbitrageReturn] = []
        for group in groups.values():
            if len(group) < 2:
                continue
            candidates.append(_fallback_candidate(tuple(group.values()), observed_at))
            if len(candidates) >= limit:
                break
        return tuple(candidates)


def _fallback_matches(
    raw: dict[str, Any],
    topics: tuple[str, ...],
    search_text: str | None,
    live_only: bool,
    observed_at: Timestamp,
) -> bool:
    """Apply catalog filters to one detailed AGG venue-market payload."""
    event = raw.get("venueEvent")
    event = event if isinstance(event, dict) else raw
    if topics and not _event_topics_match(event, topics):
        return False
    if search_text:
        payload_text = json.dumps(raw, ensure_ascii=False).casefold()
        if any(term.casefold() not in payload_text for term in search_text.split()):
            return False
    if not live_only:
        return True
    starts_at = _optional_timestamp(event.get("startDate") or raw.get("startDate"))
    ends_at = _optional_timestamp(event.get("endDate") or raw.get("endDate"))
    return (
        starts_at is not None
        and ends_at is not None
        and starts_at <= observed_at < ends_at
    )


def _fallback_signature(raw: dict[str, Any]) -> tuple[str, ...] | None:
    """Build a stable identity for one AGG sports market line."""
    venue = str(raw.get("venue") or "").casefold()
    competitors = _fallback_competitors(raw)
    question = str(raw.get("question") or "")
    period_match = re.search(r"\b(?:map|game)\s*(\d+)\b", question, re.IGNORECASE)
    period = (
        f"game_{period_match.group(1)}"
        if period_match
        else str(raw.get("period") or "full").casefold()
    )
    event = raw.get("venueEvent")
    event = event if isinstance(event, dict) else raw
    event_title = str(event.get("title") or "")
    is_moneyline = (
        str(raw.get("sportsMarketType") or "").casefold() == "moneyline"
        or "winner" in question.casefold()
        or (
            question.casefold() == event_title.casefold()
            and ":" not in event_title
        )
    )
    if (
        venue in {"limitless", "polymarket", "predict"}
        and competitors is not None
        and is_moneyline
    ):
        return ("participants", *sorted(_normalized_name(team) for team in competitors), period)

    agg_key = str(raw.get("aggKey") or "").strip()
    if not agg_key:
        return None
    return (
        agg_key,
        str(raw.get("sportsMarketType") or ""),
        str(raw.get("period") or ""),
        str(raw.get("marketCategory") or ""),
        str(raw.get("marketGroup") or ""),
        str(raw.get("marketSubtype") or ""),
        str(raw.get("lineValue") or ""),
    )


def _fallback_competitors(raw: dict[str, Any]) -> tuple[str, str] | None:
    """Extract two competitors from one AGG event title in either order."""
    event = raw.get("venueEvent")
    event = event if isinstance(event, dict) else raw
    title = str(event.get("title") or raw.get("question") or "").strip()
    title = re.sub(
        r":\s*(?:map|game)\s*\d+\s*winner.*$",
        "",
        title,
        flags=re.IGNORECASE,
    )
    parts = re.split(r"\s+vs\.?\s+", title, maxsplit=1, flags=re.IGNORECASE)
    if len(parts) != 2:
        return None
    left, right = parts
    if ":" in left:
        left = left.rsplit(":", 1)[1]
    right = re.sub(r"\s+\([^)]*\).*$", "", right)
    right = re.split(r"\s+-\s+", right, maxsplit=1)[0]
    competitors = left.strip(), right.strip()
    return competitors if all(_normalized_name(team) for team in competitors) else None


def _fallback_search_term(raw: dict[str, Any]) -> str | None:
    """Choose one distinctive AGG search token from a Limitless matchup."""
    competitors = _fallback_competitors(raw)
    if competitors is None:
        return None
    ignored = {"club", "esports", "gaming", "global", "team", "youth"}
    tokens = [
        token.casefold()
        for competitor in competitors
        for token in re.findall(r"[^\W_]+", competitor)
        if token.casefold() not in ignored
    ]
    return max(tokens, key=lambda token: (len(token), token), default=None)


def _normalized_name(value: str) -> str:
    """Normalize one competitor name for order-independent comparison."""
    return "".join(character for character in value.casefold() if character.isalnum())


def _raw_return(raw: dict[str, Any]) -> Decimal:
    """Read one AGG return value without rejecting malformed catalog data."""
    try:
        return Decimal(str(raw.get("arbReturn") or 0))
    except (InvalidOperation, TypeError, ValueError):
        return Decimal("0")


def _fallback_candidate(
    raw_markets: tuple[dict[str, Any], ...],
    observed_at: Timestamp,
) -> ArbitrageReturn:
    """Normalize one locally paired AGG market group."""
    normalized = tuple(
        market
        for raw in raw_markets
        if (market := _venue_market(raw)) is not None
    )
    selected_raw = max(raw_markets, key=_raw_return)
    selected = next(
        market for market in normalized if str(market.market_id) == str(selected_raw["id"])
    )
    event = selected_raw.get("venueEvent")
    event = event if isinstance(event, dict) else selected_raw
    event_id = event.get("id") or selected_raw.get("venueEventId")
    volume = sum(
        (market.volume_usd for market in normalized if market.volume_usd is not None),
        Decimal("0"),
    )
    return ArbitrageReturn(
        market_id=selected.market_id,
        venue_event_id=EventID(str(event_id)) if event_id else None,
        return_rate=_raw_return(selected_raw),
        observed_at=observed_at,
        event_title=str(event.get("title") or "").strip() or None,
        starts_at=_optional_timestamp(event.get("startDate")),
        ends_at=_optional_timestamp(event.get("endDate")),
        volume_usd=volume or None,
        markets=normalized,
    )
