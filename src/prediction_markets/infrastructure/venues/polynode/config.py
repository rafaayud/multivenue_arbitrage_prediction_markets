"""Configuration shared by Polynode adapters."""

import os
from pathlib import Path


def polynode_api_key() -> str | None:
    """Return the Polynode API key from the process or this project's ``.env``."""
    value = os.getenv("POLY_NODE_API") or os.getenv("POLYNODE_API_KEY") or _env_value()
    if not value:
        return None

    value = value.strip().strip('"').strip("'")
    return value if value.startswith("pn_") else f"pn_live_{value}"


def _env_value() -> str | None:
    """Resolve an explicit value or the first non-empty environment fallback."""
    env_path = Path(__file__).resolve().parents[4] / ".env"
    if not env_path.exists():
        return None

    for line in env_path.read_text(encoding="utf-8").splitlines():
        key, separator, value = line.partition("=")
        if separator and key.strip() in {"POLY_NODE_API", "POLYNODE_API_KEY"} and value.strip():
            return value
    return None
