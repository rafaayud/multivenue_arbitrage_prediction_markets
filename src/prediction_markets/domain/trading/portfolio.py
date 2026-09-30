"""Identify the bot strategy portfolio shared across venues.

Notes
-----
- Option A: one strategy ``portfolio_id`` is reused on every venue.
- WAC books stay venue-scoped because position identity is
  ``venue_id:portfolio_id:contract_id``.
- Inventory splits/merges and fills must use this same portfolio label so
  they net into one book per venue contract.
"""

from prediction_markets.domain.shared.value_objects import PortfolioID

STRATEGY_PORTFOLIO_ID = PortfolioID("cross-venue-arbitrage")
