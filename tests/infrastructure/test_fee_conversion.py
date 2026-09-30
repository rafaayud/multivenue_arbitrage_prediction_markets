"""Verify historical native-chain fee conversion."""

from datetime import datetime, timezone
from decimal import Decimal

from prediction_markets.domain.shared.value_objects import Currency, Money, Timestamp
from prediction_markets.infrastructure.fee_conversion import CoinGeckoFeeConverter


class _Client:
    def __init__(self) -> None:
        self.calls = 0

    def get(self, _path: str, *, params: dict[str, object]):
        self.calls += 1
        timestamp = int(params["from"]) + 3600
        return {"prices": [[(timestamp - 30) * 1000, 3000], [timestamp * 1000, 3100]]}


def test_converts_eth_fee_at_closest_historical_price_and_caches_bucket() -> None:
    client = _Client()
    converter = CoinGeckoFeeConverter(client=client)
    observed_at = Timestamp(datetime(2026, 8, 26, tzinfo=timezone.utc))

    first = converter.convert(Money(Decimal("0.002"), Currency("ETH")), observed_at)
    second = converter.convert(Money(Decimal("0.001"), Currency("ETH")), observed_at)

    assert first == Money(Decimal("6.200"), Currency("USD"))
    assert second == Money(Decimal("3.100"), Currency("USD"))
    assert client.calls == 1
