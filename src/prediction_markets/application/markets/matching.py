"""Discover and match complementary contracts across configured venues.

Responsibilities
----------------
- Run venue discovery and semantic-key extraction concurrently.
- Enforce the approved venue, family, and duration routes.
- Delegate equivalence rules to the domain matching service.
- Produce venue-neutral complementary contract pairs for the event pipeline.
"""

import asyncio
from dataclasses import replace
from decimal import Decimal
from enum import StrEnum
from itertools import combinations
from typing import Mapping

from prediction_markets.application.events import MarketMatchesUpdated
from prediction_markets.application.markets.models import (
    MarketCycle,
    MarketFamily,
    RegularMarketSelection,
)
from prediction_markets.domain.contracts.entities import BinaryContract
from prediction_markets.domain.market_matching.enums import UpDownOutcome
from prediction_markets.domain.market_matching.services import MarketMatchingService
from prediction_markets.domain.market_matching.value_objects import (
    MatchedContractPair,
    RegularCandidate,
    UpDownMarketKey,
)
from prediction_markets.domain.markets.entities import Market
from prediction_markets.domain.ports.instrument_discovery import (
    InstrumentDiscoveryPort,
    InstrumentDiscoveryQuery,
)
from prediction_markets.domain.ports.port_key_extraction import KeyExtractionPort
from prediction_markets.domain.shared.value_objects import Timestamp, VenueID


class CompatibilityMode(StrEnum):
    """Select the semantic comparison used by an approved venue route."""

    EXACT = "exact"
    IGNORE_TIE_OUTCOME = "ignore_tie_outcome"
    IGNORE_TIE_AND_REFERENCE_PRICE = "ignore_tie_and_reference_price"


VENUE_COMPATIBILITY = {
    (
        frozenset((VenueID("POLYMARKET"), VenueID("PREDICT"))),
        MarketFamily.CRYPTO,
        3600,
    ): CompatibilityMode.EXACT,
    (
        frozenset((VenueID("POLYMARKET"), VenueID("PREDICT"))),
        MarketFamily.CRYPTO,
        86400,
    ): CompatibilityMode.EXACT,
    (
        frozenset((VenueID("POLYMARKET"), VenueID("LIMITLESS"))),
        MarketFamily.CRYPTO,
        900,
    ): CompatibilityMode.IGNORE_TIE_AND_REFERENCE_PRICE,
    (
        frozenset((VenueID("POLYMARKET"), VenueID("LIMITLESS"))),
        MarketFamily.CRYPTO,
        300,
    ): CompatibilityMode.IGNORE_TIE_AND_REFERENCE_PRICE,
    (
        frozenset((VenueID("POLYMARKET"), VenueID("LIMITLESS"))),
        MarketFamily.FINANCE,
        86400,
    ): CompatibilityMode.IGNORE_TIE_AND_REFERENCE_PRICE,
}

_DiscoveredVenue = tuple[
    VenueID,
    tuple[tuple[Market, UpDownMarketKey], ...],
    tuple[BinaryContract, ...],
]


class MarketMatcher:
    """Match only venue pairs approved for a recurring market profile."""

    def __init__(
        self,
        discovery_by_venue: Mapping[VenueID, InstrumentDiscoveryPort],
        keys_by_venue: Mapping[VenueID, KeyExtractionPort],
        *,
        matching: MarketMatchingService | None = None,
    ) -> None:
        """
        Parameters
        ----------
        discovery_by_venue
            Market and contract discovery adapter per platform.
        keys_by_venue
            Semantic key extractor for every discovery adapter.
        matching
            Optional domain equivalence service.
        """
        if set(discovery_by_venue) != set(keys_by_venue):
            raise ValueError("Discovery and key extraction venues must match")
        self._discovery = dict(discovery_by_venue)
        self._keys = dict(keys_by_venue)
        self._matching = matching or MarketMatchingService()

    async def match_cycle(
        self,
        cycle: MarketCycle,
        *,
        strike_tolerance: Decimal = Decimal("0.001"),
        limit: int = 100,
    ) -> MarketMatchesUpdated:
        """Discover all venues and return complementary pairs for one cycle.

        Parameters
        ----------
        cycle
            Underlying and interval requested from every venue.
        strike_tolerance : Decimal, default=0.0001
            Maximum reference-price difference accepted by domain matching.
        limit : int, default=100
            Maximum markets requested from each venue.

        Returns
        -------
        MarketMatchesUpdated
            Complete replacement snapshot for the cycle.
        """
        return (
            await self.match_cycles(
                (cycle,),
                strike_tolerance=strike_tolerance,
                limit=limit,
            )
        )[0]

    async def match_cycles(
        self,
        cycles: tuple[MarketCycle, ...],
        *,
        strike_tolerance: Decimal = Decimal("0.0001"),
        limit: int = 100,
    ) -> tuple[MarketMatchesUpdated, ...]:
        """Discover and match several recurring cycles as venue batches.

        Parameters
        ----------
        cycles
            Recurring cycles to refresh in input order.
        strike_tolerance : Decimal, default=0.0001
            Maximum reference-price difference accepted by domain matching.
        limit : int, default=100
            Maximum markets requested from each venue and cycle.

        Returns
        -------
        tuple[MarketMatchesUpdated, ...]
            Complete replacement snapshots in cycle order.
        """
        if not cycles:
            return ()

        queries = tuple(
            InstrumentDiscoveryQuery(
                active_only=True,
                limit=limit,
                underlying=cycle.underlying,
                interval_seconds=cycle.interval_seconds,
            )
            for cycle in cycles
        )
        configured = frozenset(self._discovery)
        batches = []
        for venue_id in self._discovery:
            indexed_queries = tuple(
                (index, cycle, queries[index])
                for index, cycle in enumerate(cycles)
                if venue_id in _eligible_venues(cycle, configured)
            )
            if indexed_queries:
                batches.append(
                    self._discover_venue_cycles(venue_id, indexed_queries),
                )

        discovered_by_cycle: list[list[_DiscoveredVenue]] = [[] for _ in cycles]
        for batch in await asyncio.gather(*batches):
            for index, discovered in batch:
                discovered_by_cycle[index].append(discovered)

        return tuple(
            self._match_discovered(
                cycle,
                tuple(discovered_by_cycle[index]),
                strike_tolerance,
            )
            for index, cycle in enumerate(cycles)
        )

    def _match_discovered(
        self,
        cycle: MarketCycle,
        discovered: tuple[_DiscoveredVenue, ...],
        strike_tolerance: Decimal,
    ) -> MarketMatchesUpdated:
        """Match one cycle after venue I/O has completed."""
        contracts = tuple(
            contract
            for _, _, venue_contracts in discovered
            for contract in venue_contracts
        )
        contract_by_outcome = {
            (contract.venue_id, contract.market_id, contract.outcome_id): contract
            for contract in contracts
        }
        pairs: list[MatchedContractPair] = []
        for left, right in combinations(discovered, 2):
            mode = VENUE_COMPATIBILITY.get(
                (
                    frozenset((left[0], right[0])),
                    cycle.family,
                    cycle.interval_seconds,
                ),
            )
            if mode is None:
                continue
            keyed = tuple(
                (_matching_key(key, mode), market)
                for _, venue_keyed, _ in (left, right)
                for market, key in venue_keyed
            )
            for match in self._matching.match(
                keyed,
                strike_tolerance=strike_tolerance,
            ):
                pairs.extend(
                    _complementary_pairs(
                        match.left_market,
                        match.right_market,
                        min(match.left_key.end, match.right_key.end),
                        contract_by_outcome,
                    ),
                )
        return MarketMatchesUpdated(cycle, tuple(dict.fromkeys(pairs)))

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

    async def _discover_venue_cycles(
        self,
        venue_id: VenueID,
        indexed_queries: tuple[
            tuple[int, MarketCycle, InstrumentDiscoveryQuery],
            ...,
        ],
    ) -> tuple[tuple[int, _DiscoveredVenue], ...]:
        """Discover one venue batch and extract keys for every cycle."""
        discovery = self._discovery[venue_id]
        results = await discovery.discover_many(
            tuple(query for _, _, query in indexed_queries),
        )
        discovered: list[tuple[int, _DiscoveredVenue]] = []
        for (index, cycle, _), result in zip(indexed_queries, results, strict=True):
            keyed = await self._keys[venue_id].extract_key(
                result.markets,
                underlying=cycle.underlying,
            )
            discovered.append(
                (index, (venue_id, keyed, result.contracts)),
            )
        return tuple(discovered)

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


def _eligible_venues(
    cycle: MarketCycle,
    configured: frozenset[VenueID],
) -> frozenset[VenueID]:
    """Return configured venues approved for one recurring cycle."""
    return frozenset(
        venue_id
        for (venues, family, interval), _ in VENUE_COMPATIBILITY.items()
        if family is cycle.family
        and interval == cycle.interval_seconds
        and venues <= configured
        for venue_id in venues
    )


def _matching_key(
    key: UpDownMarketKey,
    mode: CompatibilityMode,
) -> UpDownMarketKey:
    """Normalize only mismatches accepted by an explicit compatibility route."""
    if mode is CompatibilityMode.EXACT:
        return key
    return replace(
        key,
        reference_price=(
            None
            if mode is CompatibilityMode.IGNORE_TIE_AND_REFERENCE_PRICE
            else key.reference_price
        ),
        resolution_rule=replace(key.resolution_rule, tie_outcome=UpDownOutcome.SPLIT),
    )


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
