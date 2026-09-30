"""Configure whether external venue clocks can block final submission.

Responsibilities
----------------
- Parse the process-level final source-age rollout switch.

Notes
-----
- Source timestamps remain observable in every mode.
- Local-only mode is for controlled operation while external clocks or
  timestamp semantics are not calibrated.
"""

import os


_ENV_NAME = "MARKET_DATA_ENFORCE_SOURCE_AGE"
SOURCE_BOOK_MAX_AGE_MS = 500
SUBMISSION_BOOK_MAX_AGE_MS = 500


def source_age_guard_enabled() -> bool:
    """Return whether venue source age participates in freshness guards.

    Returns
    -------
    bool
        ``True`` by default. False-like environment values select local-only
        freshness while retaining source-age telemetry.

    Raises
    ------
    ValueError
        If the configured value is not a recognized boolean.
    """
    value = os.getenv(_ENV_NAME, "true").strip().lower()
    if value in {"1", "true", "yes", "on"}:
        return True
    if value in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"{_ENV_NAME} must be a boolean")
