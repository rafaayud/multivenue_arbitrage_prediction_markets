"""Expose public Hyperliquid outcome metadata and books without trading controls."""

import asyncio
from datetime import datetime
from typing import Annotated, Literal

import httpx
from fastapi import APIRouter, Depends, HTTPException, Path, Query, Request
from pydantic import BaseModel

from prediction_markets.domain.ports.instrument_discovery import InstrumentDiscoveryQuery
from prediction_markets.domain.shared.value_objects import MarketID, Timestamp
from prediction_markets.infrastructure.venues.hyperliquid.catalog import HyperliquidMarketCatalog
from prediction_markets.infrastructure.venues.hyperliquid.instrument_discovery import HyperliquidInstrumentDiscoveryAdapter
from prediction_markets.infrastructure.venues.hyperliquid.market_data import HyperliquidMarketDataAdapter

router = APIRouter(tags=["hyperliquid"])


class OutcomeMarketOut(BaseModel):
    """Present one native YES/NO question with its resolution context."""

    market_id: str
    title: str
    status: str
    closes_at: datetime | None
    rules: str | None
    source: str | None
    yes_contract_id: str
    no_contract_id: str


class OutcomeCatalogOut(BaseModel):
    """Make data availability and the trading prohibition explicit to clients."""

    enabled: bool
    network: str | None
    trading_enabled: Literal[False] = False
    markets: list[OutcomeMarketOut]


class OutcomeBookOut(BaseModel):
    """Preserve decimal prices and quantities as strings in one native book."""

    contract_id: str
    outcome: Literal["YES", "NO"]
    source_at: datetime | None
    bids: list[tuple[str, str]]
    asks: list[tuple[str, str]]


class OutcomeBooksOut(BaseModel):
    """Pair native YES/NO snapshots without implying simultaneous venue updates."""

    network: str
    observed_at: datetime
    trading_enabled: Literal[False] = False
    books: list[OutcomeBookOut]


def _catalog(request: Request) -> HyperliquidMarketCatalog | None:
    return getattr(request.app.state, "hyperliquid_catalog", None)


@router.get("/hyperliquid/markets", response_model=OutcomeCatalogOut)
async def list_outcomes(
    catalog: Annotated[HyperliquidMarketCatalog | None, Depends(_catalog)],
    search_text: Annotated[str | None, Query(min_length=1, max_length=200, pattern=r"\S")] = None,
    limit: Annotated[int, Query(ge=1, le=500)] = 100,
    active_only: bool = True,
) -> OutcomeCatalogOut:
    """List supported YES/NO outcomes from the public venue catalog.

    Parameters
    ----------
    search_text
        Optional case-insensitive title filter, including the parent question.
    limit
        Maximum number of returned outcomes, between 1 and 500.
    active_only
        Exclude expired outcomes and those with no known deadline by default.

    Returns
    -------
    OutcomeCatalogOut
        Native identifiers and rules, or an empty disabled catalog.

    Raises
    ------
    HTTPException
        With status 502 when upstream metadata cannot be read or normalized.
    """
    if catalog is None:
        return OutcomeCatalogOut(enabled=False, network=None, markets=[])
    try:
        result = await HyperliquidInstrumentDiscoveryAdapter(catalog=catalog).discover(
            InstrumentDiscoveryQuery(search_text=search_text, limit=limit, active_only=active_only),
        )
        contracts = {str(contract.market_id): [] for contract in result.contracts}
        for contract in result.contracts:
            contracts[str(contract.market_id)].append(str(contract.id))
        rows = [OutcomeMarketOut(
            market_id=str(market.id), title=market.title, status=market.state.status.value,
            closes_at=market.state.close_time.value if market.state.close_time else None,
            rules=market.resolution.rules if market.resolution else market.description,
            source=market.resolution.source if market.resolution else None,
            yes_contract_id=contracts[str(market.id)][0], no_contract_id=contracts[str(market.id)][1],
        ) for market in result.markets]
    except (httpx.HTTPError, ValueError, TypeError, KeyError) as error:
        raise HTTPException(502, "Hyperliquid outcome metadata is unavailable") from error
    return OutcomeCatalogOut(enabled=True, network=catalog.settings.network, markets=rows)


@router.get("/hyperliquid/markets/{market_id}/books", response_model=OutcomeBooksOut)
async def outcome_books(
    market_id: Annotated[str, Path(pattern=r"^(0|[1-9][0-9]*)$", max_length=12)],
    catalog: Annotated[HyperliquidMarketCatalog | None, Depends(_catalog)],
) -> OutcomeBooksOut:
    """Read up to 20 levels from each side of a supported listed outcome.

    Raises
    ------
    HTTPException
        With status 503 when disabled, 404 for an unsupported or unlisted market,
        or 502 when the public venue books cannot be read or normalized.
    """
    if catalog is None:
        raise HTTPException(503, "Hyperliquid market data is disabled")
    data = HyperliquidMarketDataAdapter(catalog=catalog)
    try:
        contracts = await data.list_contracts(MarketID(market_id))
        if not contracts:
            raise HTTPException(404, "Hyperliquid YES/NO outcome is not listed")
        books = await asyncio.gather(*(data.get_order_book(contract.id) for contract in contracts))
        rows = [OutcomeBookOut(
            contract_id=str(contract.id), outcome="YES" if index == 0 else "NO",
            source_at=book.timestamp.value if book else None,
            bids=[(str(level.price.value), str(level.quantity.value)) for level in book.bids[:20]] if book else [],
            asks=[(str(level.price.value), str(level.quantity.value)) for level in book.asks[:20]] if book else [],
        ) for index, (contract, book) in enumerate(zip(contracts, books, strict=True))]
    except (httpx.HTTPError, ValueError, TypeError, KeyError) as error:
        raise HTTPException(502, "Hyperliquid outcome books are unavailable") from error
    return OutcomeBooksOut(network=catalog.settings.network, observed_at=Timestamp.now().value, books=rows)
