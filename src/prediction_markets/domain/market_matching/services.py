"""Implement stateless domain decisions for market matching.

Responsibilities
----------------
- Apply business rules to domain values and entities.
"""

from dataclasses import dataclass
from decimal import Decimal

import logging

from prediction_markets.domain.market_matching.value_objects import UpDownMarketKey
from prediction_markets.domain.markets.entities import Market
from prediction_markets.utils.decorators.logger import logged


@dataclass(frozen=True, slots=True)
class MarketMatch:
    """Pair two immutable markets whose normalized resolution keys match."""
    left_key: UpDownMarketKey
    left_market: Market
    right_key: UpDownMarketKey
    right_market: Market

    @property
    def markets(self) -> tuple[Market, Market]:
        return self.left_market, self.right_market


class MarketMatchingService:
    """Match every valid cross-venue market pair."""

    @logged(level=logging.INFO)
    def match(
        self,
        candidates: tuple[tuple[UpDownMarketKey, Market], ...],
        *,
        strike_tolerance: Decimal = Decimal("0"),
    ) -> tuple[MarketMatch, ...]:
        """Return pairwise matches without transitive strike grouping."""
        if strike_tolerance < 0:
            raise ValueError("strike_tolerance must be non-negative")

        ordered = sorted(
            candidates,
            key=lambda candidate: (
                str(candidate[1].venue_id),
                str(candidate[1].id),
                (
                    candidate[0].reference_price.amount
                    if candidate[0].reference_price is not None
                    else Decimal("-1")
                ),
            ),
        )
        return tuple(
            MarketMatch(left_key, left_market, right_key, right_market)
            for index, (left_key, left_market) in enumerate(ordered)
            for right_key, right_market in ordered[index + 1 :]
            if left_market.venue_id != right_market.venue_id
            and self._keys_match(
                left_key,
                right_key,
                strike_tolerance,
            )
        )

    @staticmethod
    @logged(level=logging.DEBUG)
    def _keys_match(
        left: UpDownMarketKey,
        right: UpDownMarketKey,
        strike_tolerance: Decimal,
    ) -> bool:
        return (
            left.underlying == right.underlying
            and left.currency == right.currency
            and left.start == right.start
            and left.end == right.end
            and left.resolution_rule == right.resolution_rule
            and _reference_prices_match(left, right, strike_tolerance)
        )


def _reference_prices_match(
    left: UpDownMarketKey,
    right: UpDownMarketKey,
    tolerance: Decimal,
) -> bool:
    """Compare published references while preserving explicit missing values."""
    if left.reference_price is None or right.reference_price is None:
        return left.reference_price is right.reference_price
    return abs(left.reference_price.amount - right.reference_price.amount) <= tolerance
