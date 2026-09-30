"""Verify live arbitrage metric observations."""

from unittest.mock import Mock

from prediction_markets.domain.contracts.entities import BinaryContract
from prediction_markets.domain.market_matching.value_objects import (
    MatchedContractPair,
)
from prediction_markets.domain.orderbook.entities import OrderBook
from prediction_markets.domain.shared.value_objects import (
    ContractID,
    Currency,
    MarketID,
    OutcomeID,
    Timestamp,
    VenueID,
)
from prediction_markets.infrastructure import metrics
from prediction_markets.infrastructure.operational_metrics import scheduled_lags


def _contract(name: str, venue: str) -> BinaryContract:
    return BinaryContract(
        id=ContractID(name),
        market_id=MarketID(f"{name}-market"),
        outcome_id=OutcomeID(name),
        venue_id=VenueID(venue),
        payout_currency=Currency("USD"),
    )


def test_observe_arbitrage_orderbooks_records_age_and_skew(monkeypatch) -> None:
    """Expose one age per venue and one receive-time skew per pair."""
    left_contract = _contract("yes", "left")
    right_contract = _contract("no", "right")
    pair = MatchedContractPair(left_contract, right_contract, Timestamp.now())
    left = OrderBook(
        left_contract.market_id,
        left_contract.outcome_id,
        (),
        (),
        received_at_ns=9_700_000_000,
    )
    right = OrderBook(
        right_contract.market_id,
        right_contract.outcome_id,
        (),
        (),
        received_at_ns=9_900_000_000,
    )
    observations = {"left": Mock(), "right": Mock()}
    age = Mock()
    age.labels.side_effect = lambda venue: observations[venue]
    skew = Mock()
    monkeypatch.setattr(metrics.time, "monotonic_ns", lambda: 10_000_000_000)
    monkeypatch.setattr(metrics, "ARBITRAGE_ORDERBOOK_AGE", age)
    monkeypatch.setattr(metrics, "ARBITRAGE_ORDERBOOK_SKEW", skew)

    metrics.observe_arbitrage_orderbooks(pair, left, right)

    observations["left"].observe.assert_called_once_with(0.3)
    observations["right"].observe.assert_called_once_with(0.1)
    skew.observe.assert_called_once_with(0.2)


def test_scheduled_lags_preserve_probes_missed_during_a_stall() -> None:
    """Expose the full response-time tail after a paused event loop."""
    lags, next_deadline = scheduled_lags(now=13.5, deadline=11.0, interval=1.0)

    assert lags == (2.5, 1.5, 0.5)
    assert next_deadline == 14.0
