"""Load and validate predict adapter configuration.

Responsibilities
----------------
- Build authenticated request settings from process configuration.
"""

import os
from decimal import Decimal, InvalidOperation
from threading import Lock

from dotenv import load_dotenv

# One authoritative process signs BNB transactions for this account. Inventory
# and cancellation share nonce allocation; ordinary signed orders do not use it.
TRANSACTION_LOCK = Lock()
_DEFAULT_TRANSACTION_GAS_PRICE_GWEI = Decimal("0.05")
_WEI_PER_GWEI = Decimal("1000000000")


def predict_api_key(value: str | None = None) -> str | None:
    """Load and validate the api key used by Predict."""
    load_dotenv()
    key = (value or os.getenv("PREDICT_API_KEY") or "").strip()
    return key or None


def predict_account_address(value: str | None = None) -> str | None:
    """Load and validate the account address used by Predict."""
    load_dotenv()
    address = (value or os.getenv("PREDICT_ACCOUNT_ADDRESS") or "").strip()
    return address or None


def predict_privy_private_key(value: str | None = None) -> str | None:
    """Load and validate the privy private key used by Predict."""
    load_dotenv()
    key = (value or os.getenv("PREDICT_PRIVY_PRIVATE_KEY") or "").strip()
    return key or None


def predict_transaction_gas_price_wei(
    suggested_wei: int = 0,
    value: str | None = None,
) -> int:
    """Return a BNB transaction gas price no lower than the configured floor.

    Parameters
    ----------
    suggested_wei
        Current RPC suggestion in wei. Higher network suggestions are retained.
    value
        Optional gas-price floor in gwei. Defaults to
        ``PREDICT_TRANSACTION_GAS_PRICE_GWEI`` or 0.05 gwei.

    Returns
    -------
    int
        Gas price in wei for Predict inventory and cancellation transactions.

    Raises
    ------
    ValueError
        If the configured floor is not a positive, finite gwei amount.

    Notes
    -----
    - This setting affects BNB Chain transactions only. Signed venue orders do
      not use gas and remain unchanged.
    """
    load_dotenv()
    raw = value if value is not None else os.getenv(
        "PREDICT_TRANSACTION_GAS_PRICE_GWEI",
        str(_DEFAULT_TRANSACTION_GAS_PRICE_GWEI),
    )
    try:
        floor_gwei = Decimal(raw)
    except (InvalidOperation, TypeError) as error:
        raise ValueError("Predict transaction gas price must be numeric") from error
    if not floor_gwei.is_finite() or floor_gwei <= 0:
        raise ValueError("Predict transaction gas price must be positive and finite")
    floor_wei = floor_gwei * _WEI_PER_GWEI
    if floor_wei != floor_wei.to_integral_value():
        raise ValueError("Predict transaction gas price has sub-wei precision")
    return max(int(suggested_wei), int(floor_wei))


def predict_headers(api_key: str | None) -> dict[str, str]:
    return {"x-api-key": api_key} if api_key else {}
