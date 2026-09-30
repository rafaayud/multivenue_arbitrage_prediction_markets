import asyncio
from decimal import Decimal

from prediction_markets.application.pnl import PnlService
from prediction_markets.domain.ports.pnl import PnlPort, VenuePnlSnapshot
from prediction_markets.domain.shared.value_objects import Timestamp, VenueID


class _Adapter(PnlPort):
    def __init__(self, venue_id: VenueID, gate: asyncio.Event, started: list[VenueID]):
        self.venue_id = venue_id
        self.gate = gate
        self.started = started
        self.calls = 0

    async def fetch(self) -> VenuePnlSnapshot:
        self.calls += 1
        self.started.append(self.venue_id)
        if len(set(self.started)) == 2:
            self.gate.set()
        await asyncio.wait_for(self.gate.wait(), timeout=0.2)
        return VenuePnlSnapshot(
            venue_id=self.venue_id,
            realized_pnl_usd=Decimal("1"),
            unrealized_pnl_usd=Decimal("2"),
            total_pnl_usd=Decimal("3"),
            fees_usd=Decimal("0.5"),
            observed_at=Timestamp.now(),
            source="test",
            scope="test",
        )


class _FailingAdapter(PnlPort):
    def __init__(self) -> None:
        self.calls = 0

    async def fetch(self) -> VenuePnlSnapshot:
        self.calls += 1
        raise RuntimeError("venue unavailable")


def test_pnl_service_refreshes_venues_concurrently_and_locks_per_venue() -> None:
    async def run() -> None:
        gate = asyncio.Event()
        started: list[VenueID] = []
        first = _Adapter(VenueID("FIRST"), gate, started)
        second = _Adapter(VenueID("SECOND"), gate, started)
        service = PnlService(
            {first.venue_id: first, second.venue_id: second},
            cache_ttl_seconds=30,
        )

        snapshots = await asyncio.gather(service.get(), service.get())

        assert first.calls == second.calls == 1
        assert snapshots[0].total_pnl_usd == Decimal("6")
        assert snapshots[0].fees_usd == Decimal("1")
        assert snapshots[0].gross_pnl_usd == Decimal("7")
        assert snapshots[0].partial is False
        assert len(snapshots[0].series) == 1

    asyncio.run(run())


def test_pnl_service_caches_partial_venue_failures() -> None:
    async def run() -> None:
        venue_id = VenueID("FAIL")
        adapter = _FailingAdapter()
        service = PnlService({venue_id: adapter}, cache_ttl_seconds=30)

        first, second = await asyncio.gather(service.get(), service.get())

        assert adapter.calls == 1
        assert first.total_pnl_usd is second.total_pnl_usd is None
        assert first.partial is second.partial is True
        assert first.venues[0].error == "venue unavailable"

    asyncio.run(run())
