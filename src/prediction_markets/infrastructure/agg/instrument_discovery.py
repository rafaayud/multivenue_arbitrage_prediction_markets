"""Integrate agg instrument discovery with domain ports.

Responsibilities
----------------
- Perform venue I/O and translate external data into domain models.
"""

import os
import re
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

import httpx
from dotenv import load_dotenv

from prediction_markets.domain.contracts.entities import BinaryContract
from prediction_markets.domain.contracts.value_objects import Payout
from prediction_markets.domain.markets.entities import Market, MarketSide
from prediction_markets.domain.markets.enums import BinaryOutcome, MarketStatus
from prediction_markets.domain.markets.value_objects import MarketResolution, MarketState
from prediction_markets.domain.ports.instrument_discovery import (
    InstrumentDiscoveryPort,
    InstrumentDiscoveryQuery,
    InstrumentDiscoveryResult,
)
from prediction_markets.domain.shared.value_objects import (
    ContractID,
    Currency,
    EventID,
    MarketID,
    OutcomeID,
    Timestamp,
    VenueID,
)
from prediction_markets.infrastructure.http_client import instrumented_async_client


_PAGE_SIZE = 100
_YES_LABELS = {"yes", "up", "higher", "above", "true", "1"}
_NO_LABELS = {"no", "down", "lower", "below", "false", "0"}


class AggInstrumentDiscoveryAdapter(InstrumentDiscoveryPort):
    """Discover AGG-canonical venue markets and outcome IDs."""

    def __init__(
        self,
        *,
        app_id: str | None = None,
        api_key: str | None = None,
        admin_key: str | None = None,
        base_url: str = "https://api.agg.market",
        timeout_seconds: float = 10.0,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")

        load_dotenv()
        self._app_id = (app_id or _env("AGG_APP_ID")).strip()
        self._api_key = (api_key or _env("AGG_APP_API_KEY")).strip()
        self._admin_key = (admin_key or _env("AGG_ADMIN_KEY")).strip()
        if not self._app_id:
            raise ValueError("AGG discovery requires app_id or AGG_APP_ID")
        if not self._api_key and not self._admin_key:
            raise ValueError("AGG discovery requires AGG_APP_API_KEY or AGG_ADMIN_KEY")

        self._base_url = base_url.rstrip("/")
        self._owns_client = client is None
        self._client = client or instrumented_async_client(
            "agg",
            timeout=timeout_seconds,
        )

    async def close(self) -> None:
        """Release network resources owned by the adapter.

        Notes
        -----
        - A caller-supplied client remains under caller ownership.
        """
        if self._owns_client:
            await self._client.aclose()
            self._owns_client = False

    async def discover(
        self,
        query: InstrumentDiscoveryQuery,
    ) -> InstrumentDiscoveryResult:
        """Discover normalized markets and contracts from one AGG payload set.

        Returns
        -------
        InstrumentDiscoveryResult
            Markets and contracts accepted by venue and query filters.
        """
        payloads = await self._discover_payloads(query)
        markets: list[Market] = []
        contracts: list[BinaryContract] = []
        for payload in payloads:
            market = _to_market(payload)
            if market is not None:
                markets.append(market)
            contracts.extend(_to_contracts(payload))
        if query.venue_token_id is not None:
            needle = query.venue_token_id
            contracts = [
                contract
                for contract in contracts
                if str(contract.outcome_id) == needle
            ]
        return InstrumentDiscoveryResult(tuple(markets), tuple(contracts))

    async def _discover_payloads(
        self,
        query: InstrumentDiscoveryQuery,
    ) -> tuple[dict[str, Any], ...]:
        """Fetch AGG venue events and flatten selected venue markets under query filters."""
        params: dict[str, str | int] = {
            "limit": min(query.limit, _PAGE_SIZE),
            "context": "detail",
        }
        if query.venue_id is not None:
            params["venue"] = str(query.venue_id).lower()
        if query.active_only:
            params["status"] = "open"
        search = query.market_slug or (
            query.underlying.symbol if query.underlying is not None else None
        )
        if search:
            params["search"] = search

        markets: dict[str, dict[str, Any]] = {}
        cursor: str | None = None
        while len(markets) < query.limit:
            if cursor:
                params["cursor"] = cursor
            response = await self._client.get(
                f"{self._base_url}/venue-markets",
                headers=self._headers,
                params=params,
            )
            response.raise_for_status()
            payload = response.json()
            if not isinstance(payload, dict) or not isinstance(payload.get("data"), list):
                raise TypeError("Unexpected AGG venue-markets response")

            for raw in payload["data"]:
                for market in _flatten_markets(raw):
                    if not isinstance(market, dict):
                        continue
                    market_id = str(market.get("id") or "").strip()
                    if market_id and _matches(market, query):
                        markets[market_id] = market
                        if len(markets) >= query.limit:
                            break
                if len(markets) >= query.limit:
                    break

            cursor = payload.get("nextCursor")
            if not payload.get("hasMore") or not isinstance(cursor, str) or not cursor:
                break
        return tuple(markets.values())

    @property
    def _headers(self) -> dict[str, str]:
        """Build the authenticated headers required for the upstream request."""
        headers = {"x-app-id": self._app_id}
        if self._api_key:
            headers["x-app-api-key"] = self._api_key
        if self._admin_key:
            headers["x-admin-key"] = self._admin_key
        return headers


def _env(name: str) -> str:
    return os.getenv(name, "")


def _flatten_markets(raw: Any) -> tuple[dict[str, Any], ...]:
    """Flatten nested venue-event markets while ignoring malformed entries."""
    if not isinstance(raw, dict):
        return ()
    matched = raw.get("matchedVenueMarkets") or ()
    return (raw, *(market for market in matched if isinstance(market, dict)))


def _matches(market: dict[str, Any], query: InstrumentDiscoveryQuery) -> bool:
    """Apply query filters that the upstream endpoint does not guarantee."""
    if query.venue_id is not None and str(market.get("venue") or "").upper() != str(query.venue_id):
        return False
    if query.venue_market_id is not None and query.venue_market_id not in _market_ids(market):
        return False
    if query.market_slug is not None and query.market_slug not in _market_ids(market):
        return False
    if query.venue_token_id is not None:
        outcome_ids = {
            str(outcome.get("id") or "")
            for outcome in market.get("venueMarketOutcomes") or ()
            if isinstance(outcome, dict)
        }
        if query.venue_token_id not in outcome_ids:
            return False
    if query.min_volume is not None and _decimal(market.get("volume")) < query.min_volume:
        return False
    if query.min_liquidity is not None and _decimal(market.get("liquidity")) < query.min_liquidity:
        return False
    if query.underlying is not None and query.underlying.symbol.upper() not in str(
        market.get("question") or market.get("externalIdentifier") or "",
    ).upper():
        return False
    if query.interval_seconds is not None and _duration_seconds(market) != query.interval_seconds:
        return False
    return not query.active_only or _status(market.get("status")) is MarketStatus.ACTIVE


def _market_ids(market: dict[str, Any]) -> set[str]:
    return {
        str(market.get(field) or "")
        for field in ("id", "externalIdentifier", "marketId", "conditionId")
        if market.get(field)
    }


def _to_contracts(market: dict[str, Any]) -> tuple[BinaryContract, ...]:
    """Translate one external market into validated binary contracts."""
    market_id = str(market.get("id") or "").strip()
    venue = str(market.get("venue") or "").strip().upper()
    outcomes = tuple(
        outcome
        for outcome in market.get("venueMarketOutcomes") or ()
        if isinstance(outcome, dict) and str(outcome.get("id") or "").strip()
    )
    sides = binary_outcomes(outcomes)
    if not market_id or not venue or sides is None:
        return ()
    return tuple(
        BinaryContract(
            id=ContractID(f"agg:{outcome['id']}"),
            market_id=MarketID(market_id),
            outcome_id=OutcomeID(str(outcome["id"])),
            venue_id=VenueID(venue),
            payout_currency=Currency("USD"),
            payout_if_true=Payout(Decimal("1")),
            payout_if_false=Payout(Decimal("0")),
            symbol=str(outcome.get("label") or outcome["id"]),
        )
        for outcome in sides
    )


def _to_market(raw: dict[str, Any]) -> Market | None:
    """Translate one external market into a normalized domain market."""
    contracts = _to_contracts(raw)
    if len(contracts) != 2:
        return None
    yes_contract, no_contract = contracts
    return Market(
        id=MarketID(str(raw["id"])),
        venue_id=VenueID(str(raw["venue"]).upper()),
        title=str(raw.get("question") or raw["id"]),
        state=MarketState(
            status=_status(raw.get("status")),
            start_time=_timestamp(raw.get("startDate")),
            close_time=_timestamp(raw.get("endDate")),
            resolved_time=_timestamp(raw.get("resolutionDate")),
        ),
        yes_side=MarketSide(OutcomeID(str(yes_contract.outcome_id)), BinaryOutcome.YES),
        no_side=MarketSide(OutcomeID(str(no_contract.outcome_id)), BinaryOutcome.NO),
        event_id=EventID(str(raw["venueEventId"])) if raw.get("venueEventId") else None,
        description=raw.get("description") or raw.get("rulesPrimary") or None,
        resolution=_resolution(raw, contracts),
    )


def binary_outcomes(
    outcomes: tuple[dict[str, Any], ...],
) -> tuple[dict[str, Any], dict[str, Any]] | None:
    """Resolve unique YES and NO outcome identifiers from an external market payload.

    Returns
    -------
    tuple[OutcomeID, OutcomeID]
        The YES identifier followed by the NO identifier.

    Raises
    ------
    ValueError
        If outcomes are missing, duplicated, or not binary.
    """
    if len(outcomes) != 2:
        return None
    yes = next((outcome for outcome in outcomes if _label(outcome) in _YES_LABELS), None)
    no = next((outcome for outcome in outcomes if _label(outcome) in _NO_LABELS), None)
    return (yes, no) if yes is not None and no is not None else None


def _label(outcome: dict[str, Any]) -> str:
    return re.sub(r"[^a-z0-9]", "", str(outcome.get("label") or "").lower())


def _status(value: Any) -> MarketStatus:
    return {
        "open": MarketStatus.ACTIVE,
        "unopened": MarketStatus.ACTIVE,
        "paused": MarketStatus.SUSPENDED,
        "closed": MarketStatus.CLOSED,
        "resolved": MarketStatus.RESOLVED,
    }.get(str(value or "").lower(), MarketStatus.UNKNOWN)


def _timestamp(value: Any) -> Timestamp | None:
    """Normalize a supported external timestamp into a domain timestamp."""
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return Timestamp(parsed)


def _duration_seconds(market: dict[str, Any]) -> int | None:
    start = _timestamp(market.get("startDate"))
    end = _timestamp(market.get("endDate"))
    if start is None or end is None:
        return None
    return int((end.value - start.value).total_seconds())


def _decimal(value: Any) -> Decimal:
    try:
        return Decimal(str(value or 0))
    except (InvalidOperation, ValueError):
        return Decimal("0")


def _resolution(
    raw: dict[str, Any], contracts: tuple[BinaryContract, ...]
) -> MarketResolution | None:
    """Normalize external market resolution metadata when available."""
    contract_by_id = {str(contract.outcome_id): contract for contract in contracts}
    winner = next(
        (
            contract_by_id[str(outcome.get("id"))].outcome_id
            for outcome in raw.get("venueMarketOutcomes") or ()
            if isinstance(outcome, dict) and outcome.get("winner") is True
            and str(outcome.get("id")) in contract_by_id
        ),
        None,
    )
    rules = raw.get("rulesPrimary") or raw.get("description")
    resolved_at = _timestamp(raw.get("resolutionDate"))
    if winner is None and rules is None and resolved_at is None:
        return None
    return MarketResolution(
        rules=rules,
        source="AGG",
        resolved_outcome_id=winner,
        resolved_at=resolved_at,
    )
