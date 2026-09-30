"""Exercise matches behavior in the api layer.

Responsibilities
----------------
- Verify matches contracts, edge cases, and failure handling.
"""

from fastapi.testclient import TestClient
import pytest

from prediction_markets.api.control_main import app
from prediction_markets.api.dependencies import get_trading_activity_reader
from prediction_markets.api.models import ContractOut, MatchPairOut


class _Reader:
    """Return a fixed SQL projection without opening PostgreSQL."""

    def __init__(self, pairs: list[MatchPairOut]) -> None:
        self.pairs = pairs
        self.calls: list[tuple[str, int]] = []

    def market_matches(self, *, underlying: str, interval_seconds: int):
        self.calls.append((underlying, interval_seconds))
        return self.pairs

    def market_matches_by_cycle(self):
        self.calls.append(("*", 0))
        return {
            ("BTC", 3600): self.pairs,
            # Old SQL projections must not re-enable removed fast subscriptions.
            ("BTC", 900): self.pairs,
        }


def _pair() -> MatchPairOut:
    return MatchPairOut(
        left=ContractOut(
            id="left-contract",
            market_id="left-market",
            outcome_id="yes",
            venue_id="polymarket",
            symbol="YES",
        ),
        right=ContractOut(
            id="right-contract",
            market_id="right-market",
            outcome_id="no",
            venue_id="limitless",
            symbol="NO",
        ),
    )


def test_returns_matched_contract_pairs() -> None:
    reader = _Reader([_pair()])
    app.dependency_overrides[get_trading_activity_reader] = lambda: reader

    try:
        with TestClient(app) as client:
            response = client.get(
                "/market-matches",
                params={
                    "underlying": "btc",
                    "interval_seconds": 3600,
                },
            )
    finally:
        app.dependency_overrides.pop(get_trading_activity_reader, None)

    assert response.status_code == 200
    assert response.json() == {
        "underlying": "BTC",
        "interval_seconds": 3600,
        "pairs": [
            {
                "left": {
                    "id": "left-contract",
                    "market_id": "left-market",
                    "outcome_id": "yes",
                    "venue_id": "polymarket",
                    "symbol": "YES",
                },
                "right": {
                    "id": "right-contract",
                    "market_id": "right-market",
                    "outcome_id": "no",
                    "venue_id": "limitless",
                    "symbol": "NO",
                },
            },
        ],
    }
    assert reader.calls == [("BTC", 3600)]


@pytest.mark.parametrize("underlying", ["BTC", "ETH", "BNB"])
@pytest.mark.parametrize("interval", [300, 900])
def test_rejects_unmonitored_short_cycle(underlying, interval) -> None:
    reader = _Reader([])
    app.dependency_overrides[get_trading_activity_reader] = lambda: reader

    try:
        with TestClient(app) as client:
            response = client.get(
                "/market-matches",
                params={"underlying": underlying, "interval_seconds": interval},
            )
    finally:
        app.dependency_overrides.pop(get_trading_activity_reader, None)

    assert response.status_code == 422
    assert reader.calls == []


def test_returns_all_monitored_cycles() -> None:
    reader = _Reader([_pair()])
    app.dependency_overrides[get_trading_activity_reader] = lambda: reader

    try:
        with TestClient(app) as client:
            response = client.get("/market-matches/all")
    finally:
        app.dependency_overrides.pop(get_trading_activity_reader, None)

    assert response.status_code == 200
    cycles = response.json()
    assert {cycle["monitor_key"] for cycle in cycles} == {
        "cycle:BTC:3600",
        "cycle:BTC:86400",
        "cycle:ETH:3600",
        "cycle:ETH:86400",
        "cycle:BNB:3600",
        "cycle:BNB:86400",
    }
    assert {cycle["family"] for cycle in cycles} == {"crypto"}
    pairs_by_key = {cycle["monitor_key"]: cycle["pairs"] for cycle in cycles}
    assert len(pairs_by_key["cycle:BTC:3600"]) == 1
    assert all(
        pairs == []
        for key, pairs in pairs_by_key.items()
        if key != "cycle:BTC:3600"
    )
    assert reader.calls == [("*", 0)]
