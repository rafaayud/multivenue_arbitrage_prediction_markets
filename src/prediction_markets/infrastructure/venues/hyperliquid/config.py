"""Resolve the opt-in public Hyperliquid outcome data network."""

import os
from dataclasses import dataclass


def hyperliquid_enabled() -> bool:
    """Return whether Hyperliquid is explicitly enabled for this process."""
    return os.getenv("HYPERLIQUID_ENABLED", "false").strip().lower() in {"1", "true"}


@dataclass(frozen=True, slots=True)
class HyperliquidSettings:
    """Pin public HTTP and WebSocket access to mainnet or testnet."""

    network: str = "mainnet"

    def __post_init__(self) -> None:
        if self.network not in {"mainnet", "testnet"}:
            raise ValueError("HYPERLIQUID_NETWORK must be mainnet or testnet")

    @classmethod
    def from_env(cls) -> "HyperliquidSettings":
        return cls(
            network=os.getenv("HYPERLIQUID_NETWORK", "mainnet").strip().lower(),
        )

    @property
    def base_url(self) -> str:
        return (
            "https://api.hyperliquid.xyz" if self.network == "mainnet"
            else "https://api.hyperliquid-testnet.xyz"
        )

    @property
    def websocket_url(self) -> str:
        return self.base_url.replace("https://", "wss://", 1) + "/ws"
