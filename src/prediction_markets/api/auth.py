"""Authenticate trading control requests shared by API processes.

Responsibilities
----------------
- Validate the configured trading key or its derived session cookie.
- Keep authentication behavior identical across control and trading services.
"""

import hashlib
import hmac
import os
import secrets
from typing import Annotated

from fastapi import Header, HTTPException
from starlette.requests import HTTPConnection


TRADING_COOKIE = "prediction_markets_trading"
_SESSION_PURPOSE = b"prediction-markets-trading-session"


def configured_trading_key() -> str:
    """Return the configured trading key.

    Returns
    -------
    str
        Secret used by both trading HTTP services.

    Raises
    ------
    HTTPException
        With status 503 when the key is not configured.
    """
    expected = os.getenv("TRADING_API_KEY")
    if not expected:
        raise HTTPException(
            status_code=503,
            detail="TRADING_API_KEY is not configured",
        )
    return expected


def trading_session_token(trading_key: str) -> str:
    """Derive the HttpOnly trading-session token from the configured key."""
    return hmac.new(
        trading_key.encode(),
        _SESSION_PURPOSE,
        hashlib.sha256,
    ).hexdigest()


def require_trading_key(
    request: HTTPConnection,
    supplied: Annotated[str | None, Header(alias="X-Trading-Key")] = None,
) -> None:
    """Reject requests without a valid trading header or session cookie.

    Raises
    ------
    HTTPException
        With status 503 when unconfigured or 401 when authentication fails.
    """
    expected = configured_trading_key()
    valid_header = supplied is not None and secrets.compare_digest(supplied, expected)
    session = request.cookies.get(TRADING_COOKIE)
    valid_session = session is not None and secrets.compare_digest(
        session,
        trading_session_token(expected),
    )
    if not valid_header and not valid_session:
        raise HTTPException(status_code=401, detail="Invalid trading API key")
