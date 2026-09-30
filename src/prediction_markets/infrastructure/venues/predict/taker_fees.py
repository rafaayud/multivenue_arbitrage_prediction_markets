"""Calculate Predict.fun taker fees from per-market fee metadata.

Responsibilities
----------------
- Cache ``feeRateBps`` before markets become actionable.
- Normalize collateral and outcome-token charges into settlement cost.
"""

from decimal import ROUND_DOWN, Decimal
import httpx

from prediction_markets.domain.ports.taker_fees import TakerFeeCalculatorPort
from prediction_markets.domain.shared.value_objects import (
    ContractID,
    Currency,
    Money,
    Price,
    Quantity,
)
from prediction_markets.domain.trading.enums import OrderSide
from prediction_markets.domain.trading.value_objects import TradingFee
from prediction_markets.infrastructure.venues.predict.catalog import PredictMarketCatalog
from prediction_markets.infrastructure.venues.predict.mappers import parse_predict_contract_id

_BPS = Decimal("10000")
_TOKEN_QUANTUM = Decimal("0.000000000000000001")


class PredictTakerFeeCalculator(TakerFeeCalculatorPort):
    """Predict CTF taker fees using each market's current basis-point rate."""

    def __init__(
        self,
        *,
        fee_rates_bps: dict[str, int] | None = None,
        api_key: str | None = None,
        base_url: str = "https://api.predict.fun",
        timeout_seconds: float = 10.0,
        cache_seconds: float = 60.0,
        client: httpx.AsyncClient | None = None,
        catalog: PredictMarketCatalog | None = None,
    ) -> None:
        """Configure fee metadata access.

        Parameters
        ----------
        fee_rates_bps
            Optional preloaded rates keyed by Predict market id.
        api_key
            API key used by the market endpoint.
        base_url
            Predict API origin.
        timeout_seconds
            Positive HTTP timeout in seconds.
        cache_seconds
            Positive maximum age for shared Predict metadata, in seconds.
        client
            Optional caller-owned asynchronous HTTP client.
        catalog
            Optional shared Predict metadata catalog.
        """
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        self._rates = {
            str(market_id): _validate_rate(rate)
            for market_id, rate in (fee_rates_bps or {}).items()
        }
        self._owns_catalog = catalog is None
        self._catalog = catalog or PredictMarketCatalog(
            api_key=api_key,
            base_url=base_url,
            timeout_seconds=timeout_seconds,
            cache_seconds=cache_seconds,
            client=client,
        )

    async def close(self) -> None:
        """Close the owned HTTP client; injected clients remain caller-owned."""
        if self._owns_catalog:
            await self._catalog.close()

    async def prepare(self, contract_ids: tuple[ContractID, ...]) -> None:
        """Load fee rates for the supplied Predict contracts.

        Parameters
        ----------
        contract_ids
            Contracts whose market fee metadata is required.
        """
        market_ids = tuple(
            dict.fromkeys(
                parse_predict_contract_id(contract_id)[0]
                for contract_id in contract_ids
            )
        )
        for market_id in market_ids:
            if market_id in self._rates:
                continue
            data = await self._catalog.get_market(market_id)
            if data is None:
                raise LookupError(f"Predict market not found: {market_id}")
            self._rates[market_id] = _validate_rate(int(data.get("feeRateBps") or 0))

    def calculate(
        self,
        contract_id: ContractID,
        price: Price,
        quantity: Quantity,
        side: OrderSide,
    ) -> TradingFee:
        """Calculate the native fee and conservative USD settlement cost.

        Parameters
        ----------
        contract_id
            Predict contract whose cached market rate applies.
        price
            Execution probability between zero and one.
        quantity
            Positive share quantity.
        side
            Buy or sell direction, which determines the charged asset.

        Returns
        -------
        TradingFee
            Shares for buys or USDT for sells, valued at maximum payout in USD.

        Notes
        -----
        - The collateral-equivalent curve is
          ``quantity * rate * min(price, 1 - price)``. Buy fees are converted
          to shares by dividing by price.
        """
        market_id, _ = parse_predict_contract_id(contract_id)
        try:
            rate = self._rates[market_id]
        except KeyError as error:
            raise LookupError(
                f"Call prepare() before calculating fees for {market_id}"
            ) from error
        return predict_taker_fee(rate, price, quantity, side)


def predict_taker_fee(
    fee_rate_bps: int,
    price: Price,
    quantity: Quantity,
    side: OrderSide,
) -> TradingFee:
    """Calculate Predict's CTF fee curve for normalized order values.

    Parameters
    ----------
    fee_rate_bps
        Market fee rate in basis points between zero and 10,000.
    price
        Execution probability between zero and one.
    quantity
        Share quantity charged by the curve.
    side
        Direction determining whether Predict charges shares or USDT.

    Returns
    -------
    TradingFee
        Native fee and its conservative maximum-payout USD value.
    """
    rate = Decimal(_validate_rate(fee_rate_bps)) / _BPS
    collateral = (
        rate
        * min(price.value, Decimal("1") - price.value)
        * quantity.value
    )
    amount = (
        collateral / price.value
        if side is OrderSide.BUY and price.value > 0
        else collateral
    ).quantize(_TOKEN_QUANTUM, rounding=ROUND_DOWN)
    charged_currency = Currency(
        "OUTCOME_TOKEN" if side is OrderSide.BUY else "USDT"
    )
    return TradingFee(
        charged=Money(amount, charged_currency),
        settlement_cost=Money(amount, Currency("USD")),
    )


def _validate_rate(value: int) -> int:
    if not 0 <= value <= 10_000:
        raise ValueError("fee_rate_bps must be between 0 and 10000")
    return value
