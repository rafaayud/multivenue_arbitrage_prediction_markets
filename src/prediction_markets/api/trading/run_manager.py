"""Coordinate run manager for API-managed live trading.

Responsibilities
----------------
- Manage execution lifecycle and translate runtime state for API consumers.
"""

import asyncio
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from typing import Callable
from uuid import uuid4

from prediction_markets.api.trading.runner import (
    LiveArbitrageConfig,
    preflight,
)


@dataclass(frozen=True, slots=True)
class ExecutionRun:
    """Capture immutable process-local state for one live execution run."""
    id: str
    status: str
    config: LiveArbitrageConfig
    started_at: datetime
    finished_at: datetime | None = None
    error: str | None = None


class PreflightError(RuntimeError):
    """Carry the failed live-trading preflight report to the API boundary."""
    def __init__(self, report: dict[str, object]) -> None:
        super().__init__("Live trading preflight failed")
        self.report = report


class ExecutionRunManager:
    """Own at most one live arbitrage task inside this API process."""

    def __init__(
        self,
        runner: Callable,
        preflight_check: Callable = preflight,
    ) -> None:
        self._runner = runner
        self._preflight_check = preflight_check
        self._lock = asyncio.Lock()
        self._task: asyncio.Task | None = None
        self._stop_event: asyncio.Event | None = None
        self._run: ExecutionRun | None = None

    async def preflight(self) -> dict[str, object]:
        return await asyncio.to_thread(self._preflight_check)

    async def start(self, config: LiveArbitrageConfig) -> ExecutionRun:
        """Start the sole live execution task after a successful preflight.

        Parameters
        ----------
        config
            Market selection and risk limits for the run.

        Returns
        -------
        ExecutionRun
            Newly created running state.

        Raises
        ------
        PreflightError
            If credentials, persistence, or recovery state is not ready.
        RuntimeError
            If another execution task is active.
        """
        report = await self.preflight()
        if not report["ready"]:
            raise PreflightError(report)
        async with self._lock:
            if self._task is not None and not self._task.done():
                raise RuntimeError("A live execution run is already active")
            stop_event = asyncio.Event()
            current = ExecutionRun(
                id=uuid4().hex,
                status="preparing" if config.short_market_keys else "running",
                config=config,
                started_at=datetime.now(timezone.utc),
            )
            self._stop_event = stop_event
            self._run = current
            self._task = asyncio.create_task(
                self._execute(current.id, config, stop_event),
                name=f"live-arbitrage:{current.id}",
            )
            return current

    def status(self, run_id: str) -> ExecutionRun:
        """Return the current run when its identifier matches.

        Raises
        ------
        KeyError
            If no run exists with the supplied identifier.
        """
        if self._run is None or self._run.id != run_id:
            raise KeyError(run_id)
        return self._run

    def current(self) -> ExecutionRun | None:
        """Return the latest execution run so clients can restore toggle state."""
        return self._run

    def mark_running(self) -> None:
        """Mark a collateral-preparing run ready for live execution."""
        if self._run is not None and self._run.status == "preparing":
            self._run = replace(self._run, status="running")

    async def stop(self, run_id: str) -> ExecutionRun:
        """Signal cooperative shutdown without cancelling venue operations mid-flight.

        Returns
        -------
        ExecutionRun
            The stopping or already terminal run state.
        """
        async with self._lock:
            current = self.status(run_id)
            if current.status not in {"preparing", "running", "stopping"}:
                return current
            self._run = replace(current, status="stopping")
            if self._stop_event is not None:
                self._stop_event.set()
            return self._run

    async def close(self) -> None:
        """Stop and await the active execution task during API shutdown."""
        if self._run is not None and self._run.status in {
            "preparing",
            "running",
            "stopping",
        }:
            await self.stop(self._run.id)
        if self._task is not None:
            await self._task

    async def _execute(
        self,
        run_id: str,
        config: LiveArbitrageConfig,
        stop_event: asyncio.Event,
    ) -> None:
        try:
            await self._runner(config, stop_event)
        except Exception as error:
            self._finish(run_id, "failed", str(error))
        else:
            self._finish(
                run_id,
                "stopped" if stop_event.is_set() else "completed",
            )

    def _finish(
        self,
        run_id: str,
        status: str,
        error: str | None = None,
    ) -> None:
        if self._run is None or self._run.id != run_id:
            return
        self._run = replace(
            self._run,
            status=status,
            finished_at=datetime.now(timezone.utc),
            error=error,
        )
