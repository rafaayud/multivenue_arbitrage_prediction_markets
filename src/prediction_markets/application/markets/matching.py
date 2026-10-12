"""Resolve explicitly selected venue markets into complementary contract pairs."""

import asyncio
from itertools import combinations
from typing import Mapping

from prediction_markets.application.markets.models import RegularMarketSelection
from prediction_markets.domain.contracts.entities import BinaryContract
from prediction_markets.domain.market_matching.value_objects import (
    MatchedContractPair,
    RegularCandidate,
)
from prediction_markets.domain.markets.entities import Market
from prediction_markets.domain.ports.instrument_discovery import (
    InstrumentDiscoveryPort,
    InstrumentDiscoveryQuery,
)
from prediction_markets.domain.shared.value_objects import Timestamp, VenueID


class MarketMatcher:
    """Resolve native contracts for markets explicitly selected by the user."""

    def __init__(self, discovery_by_venue: Mapping[VenueID, InstrumentDiscoveryPort]) -> None:
        """Register the native discovery adapters available for event selection."""
        self._discovery = dict(discovery_by_venue)


    async def resolve_regular(
        self,
        selections: tuple[RegularMarketSelection, ...],
    ) -> tuple[RegularCandidate, tuple[MatchedContractPair, ...]]:
        """Resolve arbitrary native markets into complementary contract pairs.

        Parameters
        ----------
        selections
            Markets selected manually or through a provider before native discovery.

        Returns
        -------
        tuple[RegularCandidate, tuple[MatchedContractPair, ...]]
            Normalized candidate and every cross-venue YES/NO contract pairing.

        Raises
        ------
        ValueError
            If a venue is unavailable, discovery is ambiguous, closing times are
            missing from every market, or a selected market lacks binary contracts.

        Notes
        -----
        - All venue I/O runs during candidate selection, outside the trading hot path.
        """
        discovered = await asyncio.gather(
            *(self._discover_regular(selection) for selection in selections),
        )
        markets = tuple(market for market, _ in discovered)
        candidate = RegularCandidate(markets)
        contracts = tuple(
            contract
            for _, venue_contracts in discovered
            for contract in venue_contracts
        )
        contract_by_outcome = {
            (contract.venue_id, contract.market_id, contract.outcome_id): contract
            for contract in contracts
        }
        ends_at = _regular_ends_at(markets)
        pairs: list[MatchedContractPair] = []
        for left, right in combinations(markets, 2):
            if left.venue_id == right.venue_id:
                continue
            resolved = _complementary_pairs(
                left,
                right,
                ends_at,
                contract_by_outcome,
            )
            if len(resolved) != 2:
                raise ValueError("Regular candidate markets require YES and NO contracts")
            pairs.extend(resolved)
        return candidate, tuple(dict.fromkeys(pairs))

    async def _discover_regular(
        self,
        selection: RegularMarketSelection) -> tuple[Market, tuple[BinaryContract, ...]]:
        """Resolve one external selection through its native venue adapter."""
        discovery = self._discovery.get(selection.venue_id)
        if discovery is None:
            raise ValueError(f"Unsupported regular candidate venue: {selection.venue_id}")
        query = InstrumentDiscoveryQuery(
            venue_id=selection.venue_id,
            venue_market_id=selection.external_market_id,
            search_text=selection.search_text,
        )
        result = await discovery.discover(query)
        markets = result.markets
        if len(markets) != 1:
            raise ValueError(
                f"Expected one {selection.venue_id} market for "
                f"{selection.external_market_id}, found {len(markets)}",
            )
        market = markets[0]
        market_contracts = tuple(
            contract
            for contract in result.contracts
            if contract.venue_id == market.venue_id
            and contract.market_id == market.id
        )
        return market, market_contracts


def _regular_ends_at(markets: tuple[Market, ...]) -> Timestamp:
    """Return a conservative tradable-until timestamp for one regular candidate.

    Parameters
    ----------
    markets
        Venue markets that must remain open for both legs of a cross-venue pair.

    Returns
    -------
    Timestamp
        Latest moment when planning should still treat the candidate as open.

    Raises
    ------
    ValueError
        If no venue reports a closing time.

    Notes
    -----
    - Sports venues often disagree: one reports kickoff, another resolution.
      When the earliest close is already past but a later close is still future,
      keep trading until that later deadline instead of treating the event as
      expired at kickoff.
    """
    close_times = tuple(
        market.state.close_time
        for market in markets
        if market.state.close_time is not None
    )
    if not close_times:
        raise ValueError("Regular candidate markets require a known closing time")
    earliest = min(close_times)
    latest = max(close_times)
    now = Timestamp.now()
    if earliest <= now < latest:
        return latest
    return earliest


def _complementary_pairs(
    left: Market,
    right: Market,
    ends_at: Timestamp,
    contracts: dict[tuple[object, object, object], BinaryContract],
) -> tuple[MatchedContractPair, ...]:
    left_yes = contracts.get((left.venue_id, left.id, left.yes_side.id))
    left_no = contracts.get((left.venue_id, left.id, left.no_side.id))
    right_yes = contracts.get((right.venue_id, right.id, right.yes_side.id))
    right_no = contracts.get((right.venue_id, right.id, right.no_side.id))
    values = (
        (left_yes, right_no),
        (left_no, right_yes),
    )
    return tuple(
        MatchedContractPair(first, second, ends_at)
        for first, second in values
        if first is not None and second is not None
    )
