"""Integrate polymarket taker fees with domain ports.

Responsibilities
----------------
- Perform venue I/O and translate external data into domain models.
"""

from collections.abc import Mapping
from decimal import Decimal
from typing import Any

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
from prediction_markets.infrastructure.http_client import instrumented_async_client
from prediction_markets.infrastructure.venues.polymarket.mappers import (
    parse_polymarket_contract_id,
)


class PolymarketTakerFeeCalculator(TakerFeeCalculatorPort):
    """Polymarket taker fees using each market's Gamma fee schedule."""

    def __init__(
        self,
        *,
        fee_rates: Mapping[str, Decimal] | None = None,
        gamma_base_url: str = "https://gamma-api.polymarket.com",
        timeout_seconds: float = 10.0,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")

        self._fee_schedules = {
            condition_id: (rate, Decimal("1"))
            for condition_id, rate in (fee_rates or {}).items()
        }
        self._gamma_base_url = gamma_base_url.rstrip("/")
        self._own_client = client is None
        self._client = client or instrumented_async_client(
            "polymarket",
            timeout=timeout_seconds,
        )

    async def close(self) -> None:
        """Release network resources owned by the adapter.

        Notes
        -----
        - A caller-supplied client remains under caller ownership.
        """
        if self._own_client:
            await self._client.aclose()

    async def prepare(self, contract_ids: tuple[ContractID, ...]) -> None:
        """Load venue fee metadata required for the supplied contracts."""
        condition_ids = tuple(
            dict.fromkeys(
                parse_polymarket_contract_id(contract_id)[0]
                for contract_id in contract_ids
            )
        )
        missing = [value for value in condition_ids if value not in self._fee_schedules]

        for start in range(0, len(missing), 100):
            batch = missing[start : start + 100]
            response = await self._client.get(
                f"{self._gamma_base_url}/markets",
                params={"condition_ids": batch, "limit": len(batch)},
            )
            response.raise_for_status()
            markets = response.json()
            if not isinstance(markets, list):
                raise TypeError(
                    f"Unexpected Gamma markets response: {type(markets).__name__}"
                )
            self._store_schedules(markets)

        unresolved = [value for value in missing if value not in self._fee_schedules]
        if unresolved:
            raise LookupError(
                f"Polymarket fee schedule unavailable for: {', '.join(unresolved)}"
            )

    def calculate(
        self,
        contract_id: ContractID,
        price: Price,
        quantity: Quantity,
        side: OrderSide,
    ) -> TradingFee:
        """Calculate the taker fee for one order in settlement currency.

        Returns
        -------
        TradingFee
            Native USDC charge and its USD settlement cost.
        """
        condition_id, _ = parse_polymarket_contract_id(contract_id)
        try:
            fee_rate, exponent = self._fee_schedules[condition_id]
        except KeyError as error:
            raise LookupError(
                f"Call prepare() before calculating fees for {condition_id}"
            ) from error

        curve = price.value * (Decimal("1") - price.value)
        fee = round(
            float(quantity.value * fee_rate * curve**exponent),
            5,
        )
        amount = Decimal(str(fee))
        return TradingFee(
            charged=Money(amount, Currency("USDC")),
            settlement_cost=Money(amount, Currency("USD")),
        )

    def _store_schedules(self, markets: list[dict[str, Any]]) -> None:
        """Validate and cache Polymarket fee schedules by condition identifier."""
        for market in markets:
            condition_id = market.get("conditionId") or market.get("condition_id")
            if not condition_id:
                continue

            schedule = market.get("feeSchedule")
            raw_rate = schedule.get("rate", 0) if isinstance(schedule, dict) else 0
            raw_exponent = (
                schedule.get("exponent", 1) if isinstance(schedule, dict) else 1
            )
            rate = Decimal(str(raw_rate))
            exponent = Decimal(str(raw_exponent))
            if rate < 0 or rate > 1:
                raise ValueError(
                    f"Invalid Polymarket taker fee rate for {condition_id}: {rate}"
                )
            if exponent < 0:
                raise ValueError(
                    "Invalid Polymarket fee exponent "
                    f"for {condition_id}: {exponent}"
                )
            self._fee_schedules[str(condition_id)] = (rate, exponent)
