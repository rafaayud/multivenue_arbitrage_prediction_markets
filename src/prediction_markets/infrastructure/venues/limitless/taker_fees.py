"""Integrate limitless taker fees with domain ports.

Responsibilities
----------------
- Perform venue I/O and translate external data into domain models.
"""

import logging
import os
from decimal import ROUND_DOWN, Decimal
from typing import Any
from urllib.parse import quote

import httpx
from eth_account import Account
from limitless_sdk.api import HttpClient
from limitless_sdk.types import HMACCredentials

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
from prediction_markets.infrastructure.venues.limitless.mappers import (
    parse_limitless_contract_id,
)


_BPS = Decimal("10000")
_MIN_RATE = Decimal("0.004")
_TOKEN_QUANTUM = Decimal("0.000001")
_DEFAULT_FEE_RATE_BPS = 300
_LOG = logging.getLogger(__name__)


class LimitlessTakerFeeCalculator(TakerFeeCalculatorPort):
    """Limitless CLOB taker fees using the authenticated profile rate."""

    def __init__(
        self,
        *,
        fee_rate_bps: int | None = None,
        api_key: str | None = None,
        api_secret: str | None = None,
        private_key: str | None = None,
        wallet_address: str | None = None,
        base_url: str = "https://api.limitless.exchange",
        timeout_seconds: float = 10.0,
        http_client: Any | None = None,
    ) -> None:
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")

        self._fee_rate_bps = _validate_rate(fee_rate_bps)
        key = private_key or os.getenv("LIMITLESS_PRIVATE_KEY")
        self._wallet_address = wallet_address or (
            Account.from_key(key).address if key else None
        )
        self._own_client = http_client is None
        self._hmac_client = False
        if http_client is not None:
            self._client = http_client
        else:
            resolved_api_key = api_key or os.getenv("LIMITLESS_API_KEY")
            resolved_api_secret = api_secret or os.getenv("LIMITLESS_API_SECRET")
            if resolved_api_secret:
                if not resolved_api_key:
                    raise ValueError(
                        "LIMITLESS_API_KEY is required when LIMITLESS_API_SECRET is set",
                    )
                self._client = HttpClient(
                    base_url=base_url.rstrip("/"),
                    timeout=int(timeout_seconds),
                    hmac_credentials=HMACCredentials(
                        tokenId=resolved_api_key,
                        secret=resolved_api_secret,
                    ),
                )
                self._hmac_client = True
            else:
                headers = {"X-API-Key": resolved_api_key} if resolved_api_key else {}
                self._client = instrumented_async_client(
                    "limitless",
                    base_url=base_url.rstrip("/"),
                    timeout=timeout_seconds,
                    headers=headers,
                )

    async def close(self) -> None:
        """Release network resources owned by the adapter.

        Notes
        -----
        - A caller-supplied client remains under caller ownership.
        """
        if self._own_client:
            if self._hmac_client:
                await self._client.close()
            else:
                await self._client.aclose()

    async def prepare(self, contract_ids: tuple[ContractID, ...]) -> None:
        """Load venue fee metadata required for the supplied contracts."""
        for contract_id in contract_ids:
            parse_limitless_contract_id(contract_id)

        if self._fee_rate_bps is None:
            path = (
                f"/profiles/{quote(self._wallet_address, safe='')}"
                if self._wallet_address
                else "/profiles/me"
            )
            try:
                response = await self._client.get(path)
                if isinstance(response, httpx.Response):
                    response.raise_for_status()
                    profile = response.json()
                else:
                    profile = response
            except httpx.HTTPStatusError as error:
                if error.response.status_code not in {401, 403}:
                    raise
                _LOG.warning(
                    "Limitless profile authentication failed; using conservative %s bps",
                    _DEFAULT_FEE_RATE_BPS,
                )
                self._fee_rate_bps = _DEFAULT_FEE_RATE_BPS
                return
            rank = profile.get("rank") if isinstance(profile, dict) else None
            raw_rate = rank.get("feeRateBps") if isinstance(rank, dict) else None
            if raw_rate is None:
                raise LookupError("Limitless profile has no rank.feeRateBps")
            self._fee_rate_bps = _validate_rate(int(raw_rate))

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
            Native venue charge and conservative USD settlement cost.
        """
        parse_limitless_contract_id(contract_id)
        if self._fee_rate_bps is None:
            raise LookupError("Call prepare() before calculating Limitless fees")

        maximum_rate = Decimal(self._fee_rate_bps) / _BPS
        floor = min(_MIN_RATE, maximum_rate)
        p = price.value

        if side is OrderSide.BUY:
            rate = (
                maximum_rate
                if p <= Decimal("0.5")
                else floor + (maximum_rate - floor) * (Decimal("1") - p) / p
            )
        else:
            peak = maximum_rate / Decimal("2")
            rate = floor + (peak - floor) * Decimal("4") * p * (Decimal("1") - p)

        amount = (quantity.value * rate).quantize(
            _TOKEN_QUANTUM,
            rounding=ROUND_DOWN,
        )
        return TradingFee(
            charged=Money(
                amount,
                Currency("OUTCOME_TOKEN" if side is OrderSide.BUY else "USDC"),
            ),
            settlement_cost=Money(amount, Currency("USD")),
        )


def _validate_rate(value: int | None) -> int | None:
    if value is not None and not 0 <= value <= 10_000:
        raise ValueError("fee_rate_bps must be between 0 and 10000")
    return value
