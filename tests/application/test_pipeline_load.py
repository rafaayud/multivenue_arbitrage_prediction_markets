"""Exercise bounded pipeline ingestion at target contract cardinalities."""

import asyncio

import pytest

from prediction_markets.application.events import ApplicationEvent, OrderBookUpdated
from prediction_markets.application.pipeline import EventLoop, EventSink, RingBuffer
from prediction_markets.domain.orderbook.entities import OrderBook
from prediction_markets.domain.shared.value_objects import (
    ContractID,
    MarketID,
    OutcomeID,
    VenueID,
)


class _CountingEngine:
    """Count public updates without producing commands."""

    def __init__(self) -> None:
        self.processed = 0

    def process(self, _event: ApplicationEvent) -> tuple[ApplicationEvent, ...]:
        self.processed += 1
        return ()


class _NoopJournal:
    """Reject accidental durable writes in a public-book workload."""

    def append(self, _event: ApplicationEvent) -> None:
        raise AssertionError("Order-book updates must not be journaled")


class _CascadeEngine:
    """Return derived books from the first root book for scheduling tests."""

    def __init__(self, observed: list[tuple[str, ...]]) -> None:
        self.processed: list[str] = []
        self._observed = observed
        self._first: OrderBookUpdated | None = None

    def process(self, event: ApplicationEvent) -> tuple[ApplicationEvent, ...]:
        assert isinstance(event, OrderBookUpdated)
        name = str(event.contract_id)
        self.processed.append(name)
        if self._first is None:
            self._first = event

            async def observe_after_yield() -> None:
                self._observed.append(tuple(self.processed))

            asyncio.create_task(observe_after_yield())
            return (
                _book_update("derived-1"),
                _book_update("derived-2"),
            )
        return ()


def _book_update(contract: str) -> OrderBookUpdated:
    """Build a minimal public-book event for pipeline scheduling tests."""
    return OrderBookUpdated(
        VenueID("load-test"),
        ContractID(contract),
        OrderBook(
            MarketID(f"market-{contract}"),
            OutcomeID("yes"),
            (),
            (),
        ),
    )


@pytest.mark.parametrize("contract_count", (50, 200, 500))
def test_pipeline_processes_target_contract_load_without_drops(
    contract_count: int,
) -> None:
    """Process a cooperative fake-feed burst without losing books."""

    async def run() -> None:
        inputs = RingBuffer[ApplicationEvent](8_192)
        sink = EventSink(inputs)
        engine = _CountingEngine()
        event_loop = EventLoop(inputs, RingBuffer(1), _NoopJournal(), engine)
        consumer = asyncio.create_task(event_loop.run())
        total = contract_count * 4
        try:
            for index in range(total):
                contract = index % contract_count
                await sink.publish(
                    OrderBookUpdated(
                        VenueID("load-test"),
                        ContractID(f"contract-{contract}"),
                        OrderBook(
                            MarketID(f"market-{contract}"),
                            OutcomeID("yes"),
                            (),
                            (),
                        ),
                    ),
                )
                if index % 128 == 0:
                    await asyncio.sleep(0)
            await inputs.join()
        finally:
            consumer.cancel()
            await asyncio.gather(consumer, return_exceptions=True)

        assert engine.processed == total
        assert sink.dropped_order_books == 0
        assert inputs.high_watermark <= inputs.capacity

    asyncio.run(run())


def test_event_loop_yields_only_after_a_root_event_cascade(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Yield after derived events, before processing the next root input."""

    async def run() -> None:
        inputs = RingBuffer[ApplicationEvent](8)
        observations: list[tuple[str, ...]] = []
        engine = _CascadeEngine(observations)
        event_loop = EventLoop(inputs, RingBuffer(1), _NoopJournal(), engine)
        consumer = asyncio.create_task(event_loop.run())
        try:
            assert inputs.try_publish(_book_update("root"))
            assert inputs.try_publish(_book_update("next-root"))
            await inputs.join()
            await asyncio.sleep(0)
        finally:
            consumer.cancel()
            await asyncio.gather(consumer, return_exceptions=True)

        assert observations == [("root", "derived-1", "derived-2")]
        assert engine.processed == [
            "root",
            "derived-1",
            "derived-2",
            "next-root",
        ]

    monkeypatch.setattr(
        "prediction_markets.application.pipeline.buffers._EVENT_LOOP_YIELD_BUDGET_SECONDS",
        0.0,
    )
    asyncio.run(run())
