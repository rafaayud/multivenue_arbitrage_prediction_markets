"""Integrate agg arbitrage stream with domain ports.

Responsibilities
----------------
- Perform venue I/O and translate external data into domain models.
"""

import json
import os
from collections.abc import AsyncIterator
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Literal
from urllib.parse import urlencode

import httpx
from dotenv import load_dotenv
from tenacity import AsyncRetrying, retry_if_exception_type, wait_exponential
from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed

from prediction_markets.domain.ports.arbitrage_stream import (
    ArbitrageReturn,
    ArbitrageStreamPort,
    ArbitrageVenueMarket,
)
from prediction_markets.domain.shared.value_objects import (
    EventID,
    MarketID,
    OutcomeID,
    Timestamp,
    VenueID,
)
from prediction_markets.infrastructure.http_client import instrumented_async_client
from prediction_markets.infrastructure.agg.instrument_discovery import binary_outcomes


class AggArbitrageStreamAdapter(ArbitrageStreamPort):
    """Expose AGG cross-venue candidates through the arbitrage stream port.

    Notes
    -----
    - Candidate listing performs paginated REST I/O against AGG venue events.
    - Return streaming reconnects AGG's WebSocket feed after transport failures.
    - Provider payloads are normalized before crossing the domain port.
    """

    def __init__(
        self,
        app_id: str | None = None,
        origin: str | None = None,
        rest_url: str = "https://api.agg.market",
        websocket_url: str = "wss://ws.agg.market/ws",
        timeout_seconds: float = 10,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")

        load_dotenv()
        self._app_id = (app_id or os.getenv("AGG_APP_ID") or "").strip()
        self._origin = (origin or os.getenv("AGG_ORIGIN") or "").strip()
        self._rest_url = rest_url.rstrip("/")
        self._websocket_url = websocket_url
        self._timeout_seconds = timeout_seconds
        self._client = client

    async def list_candidates(
        self,
        *,
        min_return: Decimal = Decimal("0"),
        limit: int = 100,
        topics: tuple[str, ...] = (),
        search_text: str | None = None,
        live_only: bool = False,
        min_time_to_close: timedelta | None = None,
        max_time_to_close: timedelta | None = None,
        match_statuses: tuple[Literal["matched", "verified"], ...] = (
            "matched",
            "verified",
        ),
    ) -> tuple[ArbitrageReturn, ...]:
        """Fetch and rank normalized open arbitrage candidates from AGG REST.

        Parameters
        ----------
        min_return
            Minimum decimal return rate accepted.
        limit
            Maximum number of normalized candidates returned.
        topics
            Optional AGG event categories matched case-insensitively.
        search_text
            Optional case-insensitive terms, all required in the event payload.
        live_only
            Whether ``gameStartTime <= observed_at < endDate`` is required.
        min_time_to_close
            Optional lower bound for remaining event lifetime.
        max_time_to_close
            Optional upper bound for remaining event lifetime.
        match_statuses
            AGG semantic match states included in the REST request.

        Returns
        -------
        tuple[ArbitrageReturn, ...]
            Candidates passing return, topic, lifetime, and match-state filters.

        Notes
        -----
        - Performs paginated HTTP I/O and keeps the best return per AGG market.
        """
        if not self._app_id:
            raise ValueError("AGG REST API requires app_id or AGG_APP_ID")
        if min_return < 0:
            raise ValueError("min_return must be non-negative")
        if limit <= 0:
            raise ValueError("limit must be positive")
        normalized_topics = tuple(topic.strip().lower() for topic in topics)
        if any(not topic for topic in normalized_topics):
            raise ValueError("topics cannot contain blank values")
        normalized_search = search_text.strip().casefold() if search_text else None
        if search_text is not None and not normalized_search:
            raise ValueError("search_text cannot be blank")
        if not match_statuses or any(
            status not in {"matched", "verified"} for status in match_statuses
        ):
            raise ValueError("match_statuses must contain matched or verified")
        if min_time_to_close is not None and min_time_to_close < timedelta(0):
            raise ValueError("min_time_to_close must be non-negative")
        if max_time_to_close is not None and max_time_to_close <= timedelta(0):
            raise ValueError("max_time_to_close must be positive")
        if (
            min_time_to_close is not None
            and max_time_to_close is not None
            and min_time_to_close > max_time_to_close
        ):
            raise ValueError("min_time_to_close cannot exceed max_time_to_close")

        if self._client is not None:
            return await self._list_candidates(
                self._client,
                min_return=min_return,
                limit=limit,
                topics=normalized_topics,
                search_text=normalized_search,
                live_only=live_only,
                min_time_to_close=min_time_to_close,
                max_time_to_close=max_time_to_close,
                match_statuses=match_statuses,
            )

        async with instrumented_async_client(
            "agg",
            timeout=self._timeout_seconds,
        ) as client:
            return await self._list_candidates(
                client,
                min_return=min_return,
                limit=limit,
                topics=normalized_topics,
                search_text=normalized_search,
                live_only=live_only,
                min_time_to_close=min_time_to_close,
                max_time_to_close=max_time_to_close,
                match_statuses=match_statuses,
            )

    async def _list_candidates(
        self,
        client: httpx.AsyncClient,
        *,
        min_return: Decimal,
        limit: int,
        topics: tuple[str, ...],
        search_text: str | None,
        live_only: bool,
        min_time_to_close: timedelta | None,
        max_time_to_close: timedelta | None,
        match_statuses: tuple[Literal["matched", "verified"], ...],
    ) -> tuple[ArbitrageReturn, ...]:
        """Scan AGG venue-event pages and retain the best filtered return per market."""
        candidates: dict[tuple[tuple[str, str], ...], ArbitrageReturn] = {}
        cursor: str | None = None
        headers = {"x-app-id": self._app_id}
        if self._origin:
            headers["Origin"] = self._origin
        search_terms = tuple(search_text.split()) if search_text else ()
        upstream_search = (
            search_terms[0] if search_terms else (topics[0] if len(topics) == 1 else None)
        )

        # ponytail: stop at limit; scan every page only if global ranking is needed.
        while len(candidates) < limit:
            params: list[tuple[str, str | int]] = [
                ("status", "open"),
                ("limit", 100),
            ]
            params.extend(("matchStatus", status) for status in match_statuses)
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
                    min_time_to_close,
                    max_time_to_close,
                ):
                    continue
                for candidate in _event_candidates(event, observed_at, min_return):
                    current = candidates.get(candidate.key)
                    if current is None or candidate.return_rate > current.return_rate:
                        candidates[candidate.key] = candidate

            cursor = payload.get("nextCursor")
            if not payload.get("hasMore") or not isinstance(cursor, str) or not cursor:
                break

        return tuple(
            sorted(
                candidates.values(),
                key=lambda candidate: candidate.return_rate,
                reverse=True,
            )[:limit]
        )

    async def stream_returns(self) -> AsyncIterator[ArbitrageReturn]:
        """Subscribe to AGG's arbitrage feed and normalize live return updates.

        Yields
        ------
        ArbitrageReturn
            Valid active-venue return observations.

        Notes
        -----
        - Reconnects with exponential backoff after transport failures.
        """
        if not self._app_id:
            raise ValueError("AGG WebSocket requires app_id or AGG_APP_ID")
        if not self._origin:
            raise ValueError("AGG WebSocket requires origin or AGG_ORIGIN")

        async for attempt in AsyncRetrying(
            retry=retry_if_exception_type((ConnectionClosed, OSError)),
            wait=wait_exponential(multiplier=1, min=1, max=30),
            reraise=True,
        ):
            with attempt:
                url = _websocket_url(self._websocket_url, self._app_id)
                async with connect(url, origin=self._origin) as websocket:
                    await websocket.send(
                        json.dumps({"action": "subscribe", "channel": "arb-feed"})
                    )

                    async for raw_message in websocket:
                        message = _decode_message(raw_message)
                        if message is None:
                            continue
                        if message.get("type") == "error":
                            detail = message.get("message", "unknown error")
                            raise RuntimeError(
                                f"AGG WebSocket error: {detail}"
                            )
                        if (
                            message.get("type") != "arb_feed_batch"
                            or message.get("feed") != "arb"
                            or message.get("activeVenuesOnly") is not True
                        ):
                            continue

                        for entry in message.get("entries", ()):
                            update = _entry_to_arbitrage_return(entry)
                            if update is not None:
                                yield update

                    raise ConnectionError("AGG WebSocket closed")


def _websocket_url(base_url: str, app_id: str) -> str:
    separator = "&" if "?" in base_url else "?"
    return f"{base_url}{separator}{urlencode({'appId': app_id})}"


def _decode_message(raw_message: str | bytes) -> dict[str, Any] | None:
    try:
        message = json.loads(raw_message)
    except (json.JSONDecodeError, UnicodeDecodeError):
        return None
    return message if isinstance(message, dict) else None


def _entry_to_arbitrage_return(entry: Any) -> ArbitrageReturn | None:
    """Normalize one AGG feed entry, skipping malformed observations."""
    if not isinstance(entry, dict):
        return None

    try:
        venue_event_id = entry.get("venueEventId")
        raw_liquidity = entry.get("liquidityUsd")
        raw_tier = entry.get("liquidityTier")
        liquidity_tier: Literal["deep", "shallow"] | None = (
            raw_tier if raw_tier in {"deep", "shallow"} else None
        )
        return ArbitrageReturn(
            market_id=MarketID(str(entry["marketId"])),
            venue_event_id=(
                EventID(str(venue_event_id)) if venue_event_id is not None else None
            ),
            return_rate=Decimal(str(entry["arbReturn"])),
            observed_at=Timestamp(
                datetime.fromtimestamp(float(entry["ts"]) / 1000, tz=timezone.utc)
            ),
            liquidity_usd=(
                Decimal(str(raw_liquidity)) if raw_liquidity is not None else None
            ),
            liquidity_tier=liquidity_tier,
        )
    except (KeyError, TypeError, ValueError, InvalidOperation, OverflowError):
        return None


def _event_candidates(
    event: Any,
    observed_at: Timestamp,
    min_return: Decimal | None,
    min_venues: int = 2,
) -> tuple[ArbitrageReturn, ...]:
    """Build normalized market groups from one AGG event.

    Parameters
    ----------
    event
        Raw AGG venue event.
    observed_at
        Timestamp attached to normalized groups.
    min_return
        Exclusive return threshold, or ``None`` to include every return.
    min_venues
        Minimum number of distinct venues required in a group.

    Returns
    -------
    tuple[ArbitrageReturn, ...]
        Valid binary market groups accepted by the supplied thresholds.
    """
    if not isinstance(event, dict):
        return ()

    event_id = event.get("id")
    if event_id is None:
        return ()

    raw_markets = event.get("venueMarkets")
    if not isinstance(raw_markets, list):
        return ()

    candidates: list[ArbitrageReturn] = []
    for market in raw_markets:
        if not isinstance(market, dict):
            continue
        matched_markets = market.get("matchedVenueMarkets")
        if not isinstance(matched_markets, list):
            matched_markets = []
        group = (market, *(item for item in matched_markets if isinstance(item, dict)))
        selectable_markets = tuple(
            normalized
            for raw_market in group
            if (normalized := _venue_market(raw_market)) is not None
        )
        if len({market.venue_id for market in selectable_markets}) < min_venues:
            continue
        ranked: list[tuple[Decimal, dict[str, Any]]] = []
        for raw_market in group:
            try:
                ranked.append(
                    (Decimal(str(raw_market.get("arbReturn") or 0)), raw_market),
                )
            except (AttributeError, TypeError, ValueError, InvalidOperation):
                continue
        if not ranked:
            continue
        return_rate, selected = max(ranked, key=lambda value: value[0])
        if min_return is not None and return_rate <= min_return:
            continue
        raw_event = selected.get("venueEvent")
        venue_event_id = (
            raw_event.get("id")
            if isinstance(raw_event, dict) and raw_event.get("id") is not None
            else event_id
        )
        event_title = str(event.get("title") or "").strip() or None
        starts_at = _optional_timestamp(event.get("gameStartTime"))
        ends_at = _optional_timestamp(event.get("endDate"))
        selected_id = str(selected.get("id") or "").strip()
        if not selected_id:
            continue
        volume_usd = sum(
            (
                market.volume_usd
                for market in selectable_markets
                if market.volume_usd is not None
            ),
            Decimal("0"),
        )
        candidates.append(
            ArbitrageReturn(
                market_id=MarketID(selected_id),
                venue_event_id=EventID(str(venue_event_id)),
                return_rate=return_rate,
                observed_at=observed_at,
                event_title=event_title,
                starts_at=starts_at,
                ends_at=ends_at,
                volume_usd=volume_usd or None,
                markets=selectable_markets,
            )
        )
    return tuple(candidates)


def _venue_market(raw: dict[str, Any]) -> ArbitrageVenueMarket | None:
    """Normalize one AGG venue market and its binary outcome identifiers."""
    market_id = str(raw.get("id") or "").strip()
    venue = str(raw.get("venue") or "").strip().upper()
    external_market_id = str(
        raw.get("externalIdentifier") or raw.get("conditionId") or ""
    ).strip()
    outcomes = tuple(
        outcome
        for outcome in raw.get("venueMarketOutcomes") or ()
        if isinstance(outcome, dict)
    )
    sides = binary_outcomes(outcomes)
    if not market_id or not venue or not external_market_id or sides is None:
        return None
    yes, no = sides
    return ArbitrageVenueMarket(
        venue_id=VenueID(venue),
        market_id=MarketID(market_id),
        external_market_id=external_market_id,
        yes_outcome_id=OutcomeID(str(yes["id"])),
        no_outcome_id=OutcomeID(str(no["id"])),
        title=(str(raw["question"]).strip() if raw.get("question") else None),
        volume_usd=_optional_decimal(raw.get("volume")),
    )


def _optional_decimal(value: Any) -> Decimal | None:
    """Return one non-negative decimal when the provider supplied it."""
    if value is None:
        return None
    try:
        parsed = Decimal(str(value))
    except (TypeError, ValueError, InvalidOperation):
        return None
    return parsed if parsed >= 0 else None


def _optional_timestamp(value: Any) -> Timestamp | None:
    """Parse one optional provider timestamp."""
    if value is None:
        return None
    try:
        return Timestamp.from_iso(str(value))
    except ValueError:
        return None


def _event_matches_filters(
    event: Any,
    observed_at: Timestamp,
    topics: tuple[str, ...],
    search_text: str | None,
    live_only: bool,
    min_time_to_close: timedelta | None,
    max_time_to_close: timedelta | None,
) -> bool:
    """Apply topic, text, live-window, and lifetime filters to an AGG event."""
    if not isinstance(event, dict):
        return False

    if topics and not _event_topics_match(event, topics):
        return False

    if search_text:
        event_text = json.dumps(event).casefold()
        if any(term.casefold() not in event_text for term in search_text.split()):
            return False

    starts_at = _optional_timestamp(event.get("gameStartTime"))
    closes_at = _optional_timestamp(event.get("endDate"))
    if live_only and (
        starts_at is None
        or closes_at is None
        or observed_at < starts_at
        or observed_at >= closes_at
    ):
        return False

    if min_time_to_close is None and max_time_to_close is None:
        return True

    # ponytail: use the event endDate; inspect per-venue settlement before execution.
    if closes_at is None:
        return False
    time_to_close = closes_at.value - observed_at.value
    if min_time_to_close is not None and time_to_close < min_time_to_close:
        return False
    return max_time_to_close is None or time_to_close <= max_time_to_close


def _event_topics_match(
    event: dict[str, Any],
    topics: tuple[str, ...],
) -> bool:
    """Match AGG categories while treating ``sports`` as non-esports."""
    raw_categories = event.get("categories")
    if not isinstance(raw_categories, list):
        return False
    categories = {
        str(category.get("name") or category.get("displayName") or "").lower()
        for item in raw_categories
        if isinstance(item, dict)
        and isinstance((category := item.get("category")), dict)
    }
    return not categories.isdisjoint(topics) and not (
        set(topics) == {"sports"} and "esports" in categories
    )
