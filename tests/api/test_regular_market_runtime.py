"""Verify explicit regular candidates join the existing market-data runtime."""

import asyncio
import time
from dataclasses import replace
from decimal import Decimal
from types import SimpleNamespace

import pytest
from prometheus_client import REGISTRY

from prediction_markets.api.runtime import ArbitrageRuntime
from prediction_markets.api.runtime.accounting import RuntimeAccounting
from prediction_markets.api.trading.activity import _terminal_execution_financials
from prediction_markets.api.runtime.feeds import _MarketFeedCoordinator
from prediction_markets.application.engine import TradingEngine
from prediction_markets.application.execution.timings import ExecutionTimings
from prediction_markets.application.events import (
    ExecutionUpdated,
    MarketMatchesUpdated,
    OrderSnapshotUpdated,
    PositionUpdated,
    RecoveryUpdated,
    SubmitOrder,
    TradeRecorded,
    TradingSafetyStop,
)
from prediction_markets.application.execution.accounting import apply_trade
from prediction_markets.application.markets.models import (
    MarketCycle,
    RegularMarketSelection,
    monitored_market_key,
)
from prediction_markets.application.state import EventDispatcher, TradingState
from prediction_markets.domain.contracts.entities import BinaryContract
from prediction_markets.domain.contracts.value_objects import Payout, TickSize
from prediction_markets.domain.market_matching.value_objects import (
    MatchedContractPair,
    RegularCandidate,
    Underlying,
)
from prediction_markets.domain.markets.entities import Market, MarketSide
from prediction_markets.domain.markets.enums import BinaryOutcome, MarketStatus
from prediction_markets.domain.markets.value_objects import MarketState
from prediction_markets.domain.orderbook.entities import OrderBook
from prediction_markets.domain.shared.value_objects import (
    ClientOrderID,
    ContractID,
    Currency,
    MarketID,
    Money,
    OutcomeID,
    OrderID,
    Price,
    Quantity,
    Timestamp,
    TradeID,
    VenueID,
)
from prediction_markets.domain.trading.enums import (
    ArbitrageExecutionStatus,
    OrderSide,
    OrderStatus,
    OrderType,
    ReconciliationStatus,
    RecoveryStatus,
    RecoveryRoute,
    TimeInForce,
)
from prediction_markets.domain.trading.entities import (
    ArbitrageExecutionJournal,
    ExposureRecovery,
    OrderIntent,
    OrderSnapshot,
    Trade,
)
from prediction_markets.domain.trading.value_objects import (
    OrderReference,
    PreparedOrder,
    ReconciliationResult,
    TradingFee,
)


class _Matcher:
    def __init__(self, result) -> None:
        self.result = result
        self.selections = ()

    async def resolve_regular(self, selections):
        self.selections = selections
        return self.result


class _RetryMatcher:
    def __init__(self) -> None:
        self.calls = 0
        self.retried = asyncio.Event()

    async def match_cycles(self, cycles):
        self.calls += 1
        if self.calls == 1:
            await asyncio.Event().wait()
        self.retried.set()
        return tuple(MarketMatchesUpdated(cycle, ()) for cycle in cycles)


class _Sink:
    def __init__(self) -> None:
        self.events = []

    async def publish(self, event) -> None:
        self.events.append(event)


class _Pipeline:
    def __init__(self) -> None:
        self.sink = _Sink()


class _EventLoop:
    def __init__(self, state: TradingState) -> None:
        self.state = state
        self.events = []

    async def process(self, event, **_kwargs) -> None:
        self.events.append(event)
        self.state.apply(event)


class _Fees:
    def __init__(self, failure: Exception | None = None) -> None:
        self.prepared = ()
        self.failure = failure

    async def prepare(self, contract_ids) -> None:
        self.prepared = contract_ids
        if self.failure is not None:
            raise self.failure


class _Stream:
    def __init__(self) -> None:
        self.contracts = ()
        self.tick_size_handler = None

    def add_contracts(self, contracts) -> None:
        self.contracts = contracts

    def set_tick_size_handler(self, handler) -> None:
        self.tick_size_handler = handler


class _BlockingStream(_Stream):
    def __init__(self) -> None:
        super().__init__()
        self.active = 0
        self.starts = asyncio.Queue()
        self.closes = asyncio.Queue()

    async def stream_order_books(self, _contract_ids):
        self.active += 1
        await self.starts.put(None)
        try:
            await asyncio.Event().wait()
            if False:
                yield
        finally:
            self.active -= 1
            await self.closes.put(None)


class _Execution:
    def __init__(self, tick_sizes=None) -> None:
        self.preloaded = ()
        self.tick_sizes = tick_sizes
        self.tick_updates = []

    def preload(self, contract_ids):
        self.preloaded = contract_ids
        return self.tick_sizes

    def update_tick_size(self, contract_id, tick_size) -> None:
        self.tick_updates.append((contract_id, tick_size))


class _Updates:
    def __init__(self) -> None:
        self.watched = []

    async def watch(self, contract_id) -> None:
        if contract_id not in self.watched:
            self.watched.append(contract_id)


def _market(venue: VenueID, market_id: MarketID, outcome: OutcomeID) -> Market:
    return Market(
        id=market_id,
        venue_id=venue,
        title=f"{venue} regular market",
        state=MarketState(
            MarketStatus.ACTIVE,
            close_time=Timestamp.from_iso("2099-01-01T00:00:00Z"),
        ),
        yes_side=MarketSide(outcome, BinaryOutcome.YES),
        no_side=MarketSide(OutcomeID(f"{market_id}:no"), BinaryOutcome.NO),
    )


def _contract(market: Market) -> BinaryContract:
    return BinaryContract(
        id=ContractID(f"{market.venue_id}:{market.yes_side.id}"),
        market_id=market.id,
        outcome_id=market.yes_side.id,
        venue_id=market.venue_id,
        payout_currency=Currency("USD"),
        payout_if_true=Payout(Decimal("1")),
        payout_if_false=Payout(Decimal("0")),
    )


def test_monitor_regular_adds_contracts_to_dynamic_subscriptions() -> None:
    """Publish matches and warm both existing venue streams."""
    left_market = _market(VenueID("POLYMARKET"), MarketID("poly-1"), OutcomeID("yes"))
    right_market = _market(
        VenueID("LIMITLESS"),
        MarketID("limitless-1"),
        OutcomeID("yes"),
    )
    left, right = _contract(left_market), _contract(right_market)
    candidate = RegularCandidate((left_market, right_market))
    pair = MatchedContractPair(
        left,
        right,
        Timestamp.from_iso("2099-01-01T00:00:00Z"),
    )
    pipeline = _Pipeline()
    streams = {venue: _Stream() for venue in (left.venue_id, right.venue_id)}
    fees = {venue: _Fees() for venue in streams}
    matcher = _Matcher((candidate, (pair,)))
    coordinator = _MarketFeedCoordinator(
        matcher,
        streams,
        fees,
        pipeline,
        TradingState(),
        refresh_seconds=15,
    )
    executions = {venue: _Execution() for venue in streams}
    updates = {venue: _Updates() for venue in streams}
    asyncio.run(coordinator.configure_execution(executions, updates))

    runtime = ArbitrageRuntime(enabled=False)
    runtime._feed = coordinator
    resolved, pair_count = asyncio.run(
        runtime.monitor_regular(
            (
                RegularMarketSelection(left.venue_id, "poly-1"),
                RegularMarketSelection(right.venue_id, "limitless-1"),
                RegularMarketSelection(VenueID("KALSHI"), "kalshi-1"),
            ),
        ),
    )

    event = pipeline.sink.events[0]
    assert resolved == candidate
    assert pair_count == 1
    assert tuple(selection.venue_id for selection in matcher.selections) == (
        left.venue_id,
        right.venue_id,
    )
    assert pipeline.sink.events == [event]
    assert streams[left.venue_id].contracts == (left,)
    assert streams[right.venue_id].contracts == (right,)
    assert fees[left.venue_id].prepared == (left.id,)
    assert fees[right.venue_id].prepared == (right.id,)
    assert updates[left.venue_id].watched == [left.id]
    assert updates[right.venue_id].watched == [right.id]

    monitored = coordinator.regular_markets
    assert monitored[0]["monitor_key"] == monitored_market_key(candidate)
    assert monitored[0]["pair_count"] == 1
    assert monitored[0]["pairs"][0]["left"]["id"] == str(left.id)
    assert monitored[0]["pairs"][0]["right"]["id"] == str(right.id)

    removed = asyncio.run(runtime.unmonitor_regular(monitored_market_key(candidate)))

    assert removed is True
    assert pipeline.sink.events[-1].pairs == ()
    assert coordinator.regular_markets == ()
    assert streams[left.venue_id].contracts == ()
    assert streams[right.venue_id].contracts == ()


def test_unmonitor_succeeds_while_remaining_stale_subscription_fails() -> None:
    """Persist removal even when another stale market cannot refresh."""
    left_venue = VenueID("POLYMARKET")
    right_venue = VenueID("LIMITLESS")
    first_markets = (
        _market(left_venue, MarketID("poly-1"), OutcomeID("yes-1")),
        _market(right_venue, MarketID("limitless-1"), OutcomeID("yes-1")),
    )
    second_markets = (
        _market(left_venue, MarketID("poly-2"), OutcomeID("yes-2")),
        _market(right_venue, MarketID("limitless-2"), OutcomeID("yes-2")),
    )
    first = RegularCandidate(first_markets)
    second = RegularCandidate(second_markets)
    first_pair = MatchedContractPair(
        _contract(first_markets[0]),
        _contract(first_markets[1]),
        Timestamp.from_iso("2099-01-01T00:00:00Z"),
    )
    second_pair = MatchedContractPair(
        _contract(second_markets[0]),
        _contract(second_markets[1]),
        Timestamp.from_iso("2099-01-01T00:00:00Z"),
    )
    state = TradingState(matches={first: (first_pair,), second: (second_pair,)})
    pipeline = _Pipeline()
    failing_fees = _Fees(LookupError("stale fee schedule"))
    coordinator = _MarketFeedCoordinator(
        _Matcher((second, (second_pair,))),
        {left_venue: _Stream(), right_venue: _Stream()},
        {left_venue: failing_fees, right_venue: _Fees()},
        pipeline,
        state,
        refresh_seconds=15,
    )
    runtime = ArbitrageRuntime(enabled=False)
    runtime._feed = coordinator

    removed = asyncio.run(runtime.unmonitor_regular(monitored_market_key(first)))

    assert removed is True
    assert pipeline.sink.events[-1] == MarketMatchesUpdated(first, ())
    assert tuple(
        market["monitor_key"] for market in coordinator.regular_markets
    ) == (monitored_market_key(second),)
    assert isinstance(coordinator.error, LookupError)

    failing_fees.failure = None
    removed = asyncio.run(runtime.unmonitor_regular(monitored_market_key(second)))

    assert removed is True
    assert coordinator.regular_markets == ()
    assert coordinator.error is None


def test_recovery_keeps_regular_subscriptions_past_scheduled_event_time() -> None:
    """Restore regular feeds because fresh books decide live tradability."""
    left_market = _market(VenueID("POLYMARKET"), MarketID("poly-1"), OutcomeID("yes"))
    right_market = _market(
        VenueID("LIMITLESS"),
        MarketID("limitless-1"),
        OutcomeID("yes"),
    )
    candidate = RegularCandidate((left_market, right_market))
    expired = MatchedContractPair(
        _contract(left_market),
        _contract(right_market),
        Timestamp.from_iso("2020-01-01T00:00:00Z"),
    )
    state = TradingState(matches={candidate: (expired,)})
    streams = {venue: _Stream() for venue in (expired.left.venue_id, expired.right.venue_id)}

    coordinator = _MarketFeedCoordinator(
        _Matcher((candidate, (expired,))),
        streams,
        {venue: _Fees() for venue in streams},
        _Pipeline(),
        state,
        refresh_seconds=15,
    )

    assert coordinator._matches == {
        monitored_market_key(candidate): MarketMatchesUpdated(candidate, (expired,)),
    }


def test_recovery_does_not_restore_unmonitored_regular_candidate() -> None:
    """Let the latest empty snapshot remove every historical candidate version."""
    left_market = _market(
        VenueID("POLYMARKET"),
        MarketID("poly-1"),
        OutcomeID("yes"),
    )
    right_market = _market(
        VenueID("LIMITLESS"),
        MarketID("limitless-1"),
        OutcomeID("yes"),
    )
    candidate = RegularCandidate((left_market, right_market))
    pair = MatchedContractPair(
        _contract(left_market),
        _contract(right_market),
        Timestamp.from_iso("2020-01-01T00:00:00Z"),
    )
    refreshed = RegularCandidate(
        (
            replace(
                left_market,
                state=MarketState(
                    MarketStatus.ACTIVE,
                    close_time=Timestamp.from_iso("2100-01-01T00:00:00Z"),
                ),
            ),
            right_market,
        ),
    )
    state = TradingState()
    state.apply(MarketMatchesUpdated(candidate, (pair,)))
    state.apply(MarketMatchesUpdated(refreshed, ()))
    streams = {
        venue: _Stream()
        for venue in (pair.left.venue_id, pair.right.venue_id)
    }

    coordinator = _MarketFeedCoordinator(
        _Matcher((refreshed, ())),
        streams,
        {venue: _Fees() for venue in streams},
        _Pipeline(),
        state,
        refresh_seconds=15,
    )

    assert state.matches == {refreshed: ()}
    assert coordinator._matches == {}


def test_recovery_does_not_resubscribe_a_removed_recurring_cycle() -> None:
    """Keep contracts from disabled cycles out of every venue adapter."""
    left_market = _market(
        VenueID("POLYMARKET"),
        MarketID("removed-poly"),
        OutcomeID("yes"),
    )
    right_market = _market(
        VenueID("LIMITLESS"),
        MarketID("removed-limitless"),
        OutcomeID("yes"),
    )
    pair = MatchedContractPair(
        _contract(left_market),
        _contract(right_market),
        Timestamp.from_iso("2099-01-01T00:00:00Z"),
    )
    removed_cycle = MarketCycle(Underlying("BTC"), 3600)
    state = TradingState(matches={removed_cycle: (pair,)})
    streams = {
        venue: _Stream()
        for venue in (pair.left.venue_id, pair.right.venue_id)
    }
    coordinator = _MarketFeedCoordinator(
        _Matcher(None),
        streams,
        {venue: _Fees() for venue in streams},
        _Pipeline(),
        state,
        refresh_seconds=15,
        cycles=(),
    )

    asyncio.run(coordinator._refresh_subscriptions())

    assert coordinator._matches == {}
    assert all(stream.contracts == () for stream in streams.values())
    assert all(contract_ids == () for contract_ids in coordinator._desired.values())


def test_execution_configuration_warms_private_market_subscriptions() -> None:
    """Connect private updates for current contracts before order submission."""
    left_market = _market(
        VenueID("POLYMARKET"),
        MarketID("poly-1"),
        OutcomeID("yes"),
    )
    right_market = _market(
        VenueID("LIMITLESS"),
        MarketID("limitless-1"),
        OutcomeID("yes"),
    )
    left, right = _contract(left_market), _contract(right_market)
    candidate = RegularCandidate((left_market, right_market))
    pair = MatchedContractPair(
        left,
        right,
        Timestamp.from_iso("2099-01-01T00:00:00Z"),
    )
    streams = {venue: _Stream() for venue in (left.venue_id, right.venue_id)}
    pipeline = _Pipeline()
    coordinator = _MarketFeedCoordinator(
        _Matcher((candidate, (pair,))),
        streams,
        {venue: _Fees() for venue in streams},
        pipeline,
        TradingState(matches={candidate: (pair,)}),
        refresh_seconds=15,
    )
    executions = {
        left.venue_id: _Execution({left.id: TickSize(Decimal("0.01"))}),
        right.venue_id: _Execution(),
    }
    updates = {venue: _Updates() for venue in streams}

    async def configure() -> None:
        await coordinator._refresh_subscriptions()
        await coordinator.configure_execution(executions, updates)
        await streams[left.venue_id].tick_size_handler(
            left.id,
            TickSize(Decimal("0.001")),
        )

    asyncio.run(configure())

    assert executions[left.venue_id].preloaded == (left.id,)
    assert executions[right.venue_id].preloaded == (right.id,)
    assert updates[left.venue_id].watched == [left.id]
    assert updates[right.venue_id].watched == [right.id]
    assert pipeline.sink.events[-1].pairs[0].left.tick_size == TickSize(
        Decimal("0.001"),
    )
    assert executions[left.venue_id].tick_updates == [
        (left.id, TickSize(Decimal("0.001"))),
    ]


def test_discovery_timeout_retries_a_stalled_rollover() -> None:
    """Cancel stalled venue I/O and retry the recurring cycle."""

    async def run_test() -> int:
        matcher = _RetryMatcher()
        coordinator = _MarketFeedCoordinator(
            matcher,
            {},
            {},
            _Pipeline(),
            TradingState(),
            refresh_seconds=0.001,
            refresh_timeout_seconds=0.01,
        )
        coordinator._cycles = (MarketCycle(Underlying("BTC"), 3600),)
        await coordinator.start()
        try:
            await asyncio.wait_for(matcher.retried.wait(), timeout=1)
        finally:
            await coordinator.stop()
        return matcher.calls

    assert asyncio.run(run_test()) >= 2


def test_stop_closes_market_pump_before_restart() -> None:
    """Keep exactly one stream generation alive across repeated restarts."""

    async def run_test() -> None:
        venue = VenueID("POLYMARKET")
        contract_id = ContractID("polymarket:condition:token")
        stream = _BlockingStream()
        coordinator = _MarketFeedCoordinator(
            _Matcher(None),
            {venue: stream},
            {venue: _Fees()},
            _Pipeline(),
            TradingState(),
            refresh_seconds=15,
        )
        coordinator._desired[venue] = (contract_id,)

        for _ in range(3):
            pumps_before = REGISTRY.get_sample_value(
                "market_feed_active_pumps",
                {"venue": "POLYMARKET"},
            ) or 0
            feed = asyncio.create_task(coordinator._feed_loop(venue, stream))
            coordinator._tasks = (feed,)
            await asyncio.wait_for(stream.starts.get(), timeout=1)
            assert stream.active == 1
            assert REGISTRY.get_sample_value(
                "market_feed_active_pumps",
                {"venue": "POLYMARKET"},
            ) == pumps_before + 1

            await coordinator.stop()

            await asyncio.wait_for(stream.closes.get(), timeout=1)
            assert stream.active == 0
            assert REGISTRY.get_sample_value(
                "market_feed_active_pumps",
                {"venue": "POLYMARKET"},
            ) == pumps_before

    asyncio.run(run_test())


def test_predict_auth_refresh_runs_outside_order_dispatch() -> None:
    """Exercise the lifecycle-owned proactive JWT refresh loop."""

    class _PredictExecution:
        def __init__(self) -> None:
            self.reasons: list[str] = []

        def refresh_auth(self, *, reason: str) -> None:
            self.reasons.append(reason)

        def refresh_cancellation_anchor(self) -> None:
            self.reasons.append("chain_anchor")

    async def run_test() -> list[str]:
        adapter = _PredictExecution()
        stop_event = asyncio.Event()
        task = asyncio.create_task(
            ArbitrageRuntime._refresh_predict_auth_periodically(
                adapter,
                0.001,
                stop_event,
            )
        )
        await asyncio.sleep(0.01)
        stop_event.set()
        await task
        return adapter.reasons

    reasons = asyncio.run(run_test())
    assert "proactive" in reasons
    assert "chain_anchor" in reasons


def test_market_feed_measures_receive_to_sink_while_trading_is_disabled() -> None:
    """Measure public-feed handoff independently of live order submission."""

    async def run_test() -> None:
        venue = VenueID("POLYMARKET")
        contract_id = ContractID("polymarket:condition:token")
        pipeline = _Pipeline()
        state = TradingState()
        assert state.trading_enabled is False
        book = OrderBook(
            market_id=MarketID("market-1"),
            outcome_id=OutcomeID("yes"),
            bids=(),
            asks=(),
            timestamp=Timestamp.now(),
            received_at_ns=time.monotonic_ns(),
        )

        class _OneBookStream:
            async def stream_order_books(self, _contract_ids):
                yield contract_id, book

        coordinator = _MarketFeedCoordinator(
            _Matcher(None),
            {venue: _OneBookStream()},
            {},
            pipeline,
            state,
            refresh_seconds=15,
        )
        receive_count_before = REGISTRY.get_sample_value(
            "market_feed_receive_to_sink_seconds_count",
            {"venue": "POLYMARKET"},
        ) or 0
        sink_count_before = REGISTRY.get_sample_value(
            "market_feed_sink_publish_seconds_count",
            {"venue": "POLYMARKET"},
        ) or 0

        await coordinator._pump(venue, _OneBookStream(), (contract_id,))

        assert len(pipeline.sink.events) == 1
        assert REGISTRY.get_sample_value(
            "market_feed_receive_to_sink_seconds_count",
            {"venue": "POLYMARKET"},
        ) == receive_count_before + 1
        assert REGISTRY.get_sample_value(
            "market_feed_sink_publish_seconds_count",
            {"venue": "POLYMARKET"},
        ) == sink_count_before + 1
        assert REGISTRY.get_sample_value(
            "market_feed_venue_timestamp_delta_seconds",
            {"venue": "POLYMARKET"},
        ) is not None

    asyncio.run(run_test())


def test_signal_settings_reconfigure_detection_only_while_trading_is_disabled() -> None:
    """Apply monitoring thresholds and reject live reconfiguration."""
    runtime = ArbitrageRuntime(enabled=False)
    runtime.state = TradingState()
    runtime.engine = TradingEngine(EventDispatcher(runtime.state), {})

    status = runtime.configure_signals(Decimal("0.015"), Decimal("0.005"))

    assert status["signal_settings"] == {
        "min_net_edge": 0.015,
        "cost_buffer": 0.005,
    }
    assert runtime.engine._config.min_net_edge == Decimal("0.015")
    assert runtime.engine._config.cost_buffer == Decimal("0.005")

    runtime.state.trading_enabled = True
    with pytest.raises(RuntimeError, match="Disable live trading"):
        runtime.configure_signals(Decimal("0"), Decimal("0"))


def test_runtime_status_builds_the_api_view_from_trading_state() -> None:
    """Keep transport formatting out of the replayable state projection."""
    runtime = ArbitrageRuntime(enabled=False)
    runtime.state = TradingState()
    runtime.state.trading_enabled = True
    runtime.state.last_error = "test error"
    runtime.state.timings["execution-1"] = ExecutionTimings(
        opportunity_at_ns=1,
        venue_ids={"primary": "left", "hedge": "right"},
    )

    status = runtime.status()

    assert status["trading_enabled"] is True
    assert status["matched_cycles"] == 0
    assert status["matched_pairs"] == 0
    assert status["books"] == 0
    assert status["orders"] == 0
    assert status["active_executions"] == 0
    assert status["last_error"] == "test error"
    assert status["last_execution_latency"]["execution_id"] == "execution-1"


def test_manual_completion_clears_residual_exposure_durably() -> None:
    async def run_test():
        runtime = ArbitrageRuntime(enabled=False)
        runtime.state = TradingState()
        execution = ArbitrageExecutionJournal(
            id="execution-1",
            status=ArbitrageExecutionStatus.NEEDS_REVIEW,
            leg1_venue_id=VenueID("PREDICT"),
            leg1_contract_id=ContractID("predict:market:no"),
            leg1_side=OrderSide.BUY,
            leg1_quantity=Quantity(Decimal("6")),
            leg1_limit_price=Price(Decimal("0.75")),
            leg1_client_order_id=ClientOrderID("predict-client"),
            leg2_venue_id=VenueID("POLYMARKET"),
            leg2_contract_id=ContractID("polymarket:market:yes"),
            leg2_side=OrderSide.BUY,
            leg2_quantity=Quantity(Decimal("6")),
            leg2_limit_price=Price(Decimal("0.23")),
            leg2_client_order_id=ClientOrderID("polymarket-client"),
            leg1_filled_quantity=Quantity(Decimal("6")),
            residual_quantity=Quantity(Decimal("6")),
            last_error="Polymarket rejected the order",
            created_at=Timestamp.now(),
            updated_at=Timestamp.now(),
        )
        runtime.state.executions[execution.id] = execution
        recovery = ExposureRecovery(
            id=execution.id,
            venue_id=VenueID("PREDICT"),
            contract_id=ContractID("predict:market:no"),
            side=OrderSide.BUY,
            quantity=Quantity(Decimal("6")),
            limit_price=Price(Decimal("0.75")),
            status=RecoveryStatus.NEEDS_REVIEW,
            attempts=1,
            created_at=Timestamp.now(),
            updated_at=Timestamp.now(),
        )
        runtime.state.recoveries[execution.id] = recovery
        source_trade = Trade(
            id=TradeID("predict-fill"),
            contract_id=execution.leg1_contract_id,
            venue_id=execution.leg1_venue_id,
            side=OrderSide.BUY,
            quantity=Quantity(Decimal("6")),
            price=Price(Decimal("0.75")),
            executed_at=execution.created_at,
            client_order_id=execution.leg1_client_order_id,
            fee=Money(Decimal("0.03"), Currency("USD")),
            fee_settlement_cost=Money(Decimal("0.03"), Currency("USD")),
        )
        runtime.state.trades[source_trade.id] = source_trade
        source_position = apply_trade(None, source_trade).position
        runtime.state.positions[source_position.id] = source_position
        event_loop = _EventLoop(runtime.state)
        runtime.pipeline = type("Pipeline", (), {"event_loop": event_loop})()
        runtime._accounting = RuntimeAccounting(
            None,
            runtime.state,
            EventDispatcher(runtime.state),
            runtime.pipeline,
            lambda: (),
            {},
        )
        resolved_at = Timestamp.now()
        runtime.state.positions[source_position.id] = replace(
            source_position,
            quantity=Quantity(Decimal("5")),
        )
        with pytest.raises(
            ValueError,
            match="cannot safely cover the residual exposure",
        ):
            await runtime.complete_execution(
                execution.id,
                method="settlement",
                price=Price(Decimal("1")),
                fee_amount_usd=Decimal("0"),
                executed_at=resolved_at,
            )
        assert event_loop.events == []
        runtime.state.positions[source_position.id] = source_position

        completed, manual_trade = await runtime.complete_execution(
            execution.id,
            method="settlement",
            price=Price(Decimal("1")),
            fee_amount_usd=Decimal("0"),
            executed_at=resolved_at,
            external_reference="claim-0x123",
        )

        assert completed.status is ArbitrageExecutionStatus.COMPLETED
        assert completed.resolution_method == "settlement"
        assert completed.residual_quantity == Quantity(Decimal("0"))
        assert completed.last_error is None
        assert event_loop.events[0] == TradeRecorded(manual_trade)
        assert isinstance(event_loop.events[1], PositionUpdated)
        assert event_loop.events[1].position.quantity == Quantity(Decimal("0"))
        assert event_loop.events[1].position.realized_pnl == Decimal("1.5")
        assert event_loop.events[2:] == [
            RecoveryUpdated(runtime.state.recoveries[execution.id]),
            ExecutionUpdated(completed),
        ]
        assert manual_trade.side is OrderSide.SELL
        assert manual_trade.quantity == Quantity(Decimal("6"))
        assert manual_trade.price == Price(Decimal("1"))
        assert runtime.state.recoveries[execution.id].status is RecoveryStatus.RESOLVED

        event_count = len(event_loop.events)
        repeated = await runtime.complete_execution(
            execution.id,
            method="settlement",
            price=Price(Decimal("1")),
            fee_amount_usd=Decimal("0"),
            executed_at=resolved_at,
            external_reference="claim-0x123",
        )
        assert repeated == (completed, manual_trade)
        assert len(event_loop.events) == event_count

    asyncio.run(run_test())


@pytest.mark.parametrize("reverse_legs", [False, True])
def test_manual_inventory_sale_ignores_failed_unwind_venue(reverse_legs) -> None:
    """Close the remaining Limitless inventory after a failed Polymarket unwind."""
    async def run_test():
        state = TradingState()
        quantity = Quantity(Decimal("5"))
        now = Timestamp.now()
        execution = ArbitrageExecutionJournal(
            id="manual-short", status=ArbitrageExecutionStatus.NEEDS_REVIEW,
            leg1_venue_id=VenueID("POLYMARKET"),
            leg1_contract_id=ContractID("polymarket:market:no"),
            leg1_side=OrderSide.SELL, leg1_quantity=quantity,
            leg1_limit_price=Price(Decimal("0.60")),
            leg1_client_order_id=ClientOrderID("primary"),
            leg1_filled_quantity=quantity,
            leg2_venue_id=VenueID("LIMITLESS"),
            leg2_contract_id=ContractID("limitless:market:yes"),
            leg2_side=OrderSide.SELL, leg2_quantity=quantity,
            leg2_limit_price=Price(Decimal("0.47")),
            leg2_client_order_id=ClientOrderID("hedge"),
            residual_quantity=quantity, created_at=now, updated_at=now,
        )
        source_contract = execution.leg1_contract_id
        source_venue = execution.leg1_venue_id
        remaining_contract = execution.leg2_contract_id
        if reverse_legs:
            execution = replace(
                execution,
                leg1_venue_id=execution.leg2_venue_id,
                leg1_contract_id=execution.leg2_contract_id,
                leg1_filled_quantity=Quantity(Decimal("0")),
                leg2_venue_id=source_venue, leg2_contract_id=source_contract,
                leg2_filled_quantity=quantity,
            )
        state.executions[execution.id] = execution
        state.recoveries[execution.id] = ExposureRecovery(
            id=execution.id, venue_id=source_venue, contract_id=source_contract,
            side=OrderSide.BUY, quantity=quantity, limit_price=Price(Decimal("0.73")),
            source_contract_id=source_contract, source_side=OrderSide.SELL,
            status=RecoveryStatus.NEEDS_REVIEW, attempts=3, created_at=now, updated_at=now,
        )
        inventory = Trade(
            TradeID("limitless-inventory"), remaining_contract, VenueID("LIMITLESS"),
            OrderSide.BUY, quantity, Price(Decimal("0.5")), now,
        )
        position = apply_trade(None, inventory).position
        event_loop = _EventLoop(state)
        accounting = RuntimeAccounting(
            None, state, EventDispatcher(state),
            SimpleNamespace(event_loop=event_loop), lambda: (), {},
        )
        # An unrelated or undersized position must not authorize completion.
        state.positions[position.id] = replace(position, quantity=Quantity(Decimal("4")))
        with pytest.raises(ValueError, match="cannot safely cover"):
            await accounting.complete_execution(
                execution.id, method="manual_sale", price=Price(Decimal("0.05")),
                fee_amount_usd=Decimal("0"), executed_at=now,
            )
        assert event_loop.events == []
        state.positions[position.id] = position
        completed, trade = await accounting.complete_execution(
            execution.id, method="manual_sale", price=Price(Decimal("0.05")),
            fee_amount_usd=Decimal("0"), executed_at=now,
        )
        assert trade.venue_id == VenueID("LIMITLESS")
        assert trade.contract_id == remaining_contract
        assert trade.side is OrderSide.SELL and trade.quantity == quantity
        assert state.positions[position.id].quantity.value == 0
        assert state.positions[position.id].realized_pnl == Decimal("-2.25")
        assert completed.status is ArbitrageExecutionStatus.COMPLETED
        assert completed.residual_quantity.value == 0
        assert state.recoveries[execution.id].status is RecoveryStatus.RESOLVED

    asyncio.run(run_test())


@pytest.mark.parametrize("cancelled_unsettled", (False, True))
def test_manual_completion_stops_when_late_fill_resolves_execution(cancelled_unsettled) -> None:
    """Persist a late venue fill instead of adding a duplicate manual trade."""

    async def run_test() -> None:
        state = TradingState()
        venue_id = VenueID("PREDICT")
        client_order_id = ClientOrderID("predict-late-fill")
        execution = ArbitrageExecutionJournal(
            id="execution-late-fill",
            status=ArbitrageExecutionStatus.NEEDS_REVIEW,
            leg1_venue_id=venue_id,
            leg1_contract_id=ContractID("predict:market:yes"),
            leg1_side=OrderSide.BUY,
            leg1_quantity=Quantity(Decimal("5")),
            leg1_limit_price=Price(Decimal("0.4")),
            leg1_client_order_id=client_order_id,
            leg2_venue_id=VenueID("POLYMARKET"),
            leg2_contract_id=ContractID("polymarket:market:no"),
            leg2_side=OrderSide.BUY,
            leg2_quantity=Quantity(Decimal("5")),
            leg2_limit_price=Price(Decimal("0.5")),
            leg2_client_order_id=ClientOrderID("polymarket-fill"),
            leg2_filled_quantity=Quantity(Decimal("5")),
            residual_quantity=Quantity(Decimal("5")),
            created_at=Timestamp.now(),
            updated_at=Timestamp.now(),
        )
        state.executions[execution.id] = execution
        intent = OrderIntent(
            contract_id=execution.leg1_contract_id,
            side=execution.leg1_side,
            quantity=execution.leg1_quantity,
            order_type=OrderType.LIMIT,
            client_order_id=client_order_id,
            limit_price=execution.leg1_limit_price,
            time_in_force=TimeInForce.IOC,
        )
        command = SubmitOrder(execution.id, "primary", venue_id, intent)
        reference = OrderReference(venue_id, client_order_id, b"predict-order")
        state.commands[client_order_id] = command
        state.prepared[client_order_id] = PreparedOrder(reference, b"{}")
        late_fill = OrderSnapshot(
            status=OrderStatus.FILLED,
            contract_id=intent.contract_id,
            side=intent.side,
            quantity=intent.quantity,
            order_type=intent.order_type,
            client_order_id=client_order_id,
            filled_quantity=intent.quantity,
            average_price=intent.limit_price,
            limit_price=intent.limit_price,
        )
        if cancelled_unsettled:
            state.orders[client_order_id] = replace(
                late_fill, status=OrderStatus.CANCELLED,
                filled_quantity=Quantity(Decimal("0")), average_price=None,
                may_receive_more_fills=True,
            )

        class _ResolvingEventLoop(_EventLoop):
            async def process(self, event, **kwargs) -> None:
                await super().process(event, **kwargs)
                if isinstance(event, OrderSnapshotUpdated):
                    state.executions[execution.id] = replace(
                        execution,
                        status=ArbitrageExecutionStatus.COMPLETED,
                        leg1_filled_quantity=intent.quantity,
                        residual_quantity=Quantity(Decimal("0")),
                    )

        event_loop = _ResolvingEventLoop(state)
        accounting = RuntimeAccounting(
            None,
            state,
            EventDispatcher(state),
            SimpleNamespace(event_loop=event_loop),
            lambda: (),
            {
                venue_id: SimpleNamespace(
                    reconcile=lambda received: ReconciliationResult(
                        ReconciliationStatus.FOUND,
                        received,
                        late_fill,
                    ),
                ),
            },
        )

        with pytest.raises(ValueError, match="reconciliation resolved"):
            await accounting.complete_execution(
                execution.id,
                method="settlement",
                price=Price(Decimal("1")),
                fee_amount_usd=Decimal("0"),
                executed_at=Timestamp.now(),
            )

        assert event_loop.events == [
            OrderSnapshotUpdated(
                execution.id,
                "primary",
                reference,
                late_fill,
                "get",
            ),
        ]

    asyncio.run(run_test())


def test_reconciliation_refreshes_a_settled_fill_with_missing_fees() -> None:
    """Re-read a final fill so its late venue fee reaches accounting."""

    async def run_test() -> None:
        state = TradingState()
        venue_id = VenueID("PREDICT")
        client_order_id = ClientOrderID("predict-missing-fee")
        execution = ArbitrageExecutionJournal(
            id="execution-missing-fee",
            status=ArbitrageExecutionStatus.COMPLETED,
            leg1_venue_id=venue_id,
            leg1_contract_id=ContractID("predict:market:yes"),
            leg1_side=OrderSide.BUY,
            leg1_quantity=Quantity(Decimal("5")),
            leg1_limit_price=Price(Decimal("0.4")),
            leg1_client_order_id=client_order_id,
            leg1_filled_quantity=Quantity(Decimal("5")),
            leg2_venue_id=VenueID("POLYMARKET"),
            leg2_contract_id=ContractID("polymarket:market:no"),
            leg2_side=OrderSide.BUY,
            leg2_quantity=Quantity(Decimal("5")),
            leg2_limit_price=Price(Decimal("0.5")),
            leg2_client_order_id=ClientOrderID("polymarket-filled"),
            leg2_filled_quantity=Quantity(Decimal("5")),
            created_at=Timestamp.now(),
            updated_at=Timestamp.now(),
        )
        intent = OrderIntent(
            contract_id=execution.leg1_contract_id,
            side=execution.leg1_side,
            quantity=execution.leg1_quantity,
            order_type=OrderType.LIMIT,
            client_order_id=client_order_id,
            limit_price=execution.leg1_limit_price,
            time_in_force=TimeInForce.IOC,
        )
        command = SubmitOrder(execution.id, "primary", venue_id, intent)
        reference = OrderReference(venue_id, client_order_id, b"predict-order")
        snapshot = OrderSnapshot(
            status=OrderStatus.FILLED,
            contract_id=intent.contract_id,
            side=intent.side,
            quantity=intent.quantity,
            order_type=intent.order_type,
            client_order_id=client_order_id,
            order_id=OrderID("predict-order"),
            limit_price=intent.limit_price,
            filled_quantity=intent.quantity,
            average_price=intent.limit_price,
            may_receive_more_fills=False,
        )
        fee = TradingFee(
            Money(Decimal("0.02"), Currency("USDT")),
            Money(Decimal("0.02"), Currency("USD")),
        )
        enriched = replace(snapshot, fee=fee)
        state.executions[execution.id] = execution
        state.commands[client_order_id] = command
        state.prepared[client_order_id] = PreparedOrder(reference, b"{}")
        state.orders[client_order_id] = snapshot
        event_loop = _EventLoop(state)
        accounting = RuntimeAccounting(
            None,
            state,
            EventDispatcher(state),
            SimpleNamespace(event_loop=event_loop),
            lambda: (),
            {
                venue_id: SimpleNamespace(
                    reconcile=lambda received: ReconciliationResult(
                        ReconciliationStatus.FOUND,
                        received,
                        enriched,
                    ),
                ),
            },
        )

        assert await accounting.reconcile_execution(execution.id) == execution
        assert event_loop.events == [
            OrderSnapshotUpdated(
                execution.id,
                "primary",
                reference,
                enriched,
                "get",
            ),
        ]

    asyncio.run(run_test())


def _predict_rounding_review():
    """Construct the fractional recovery whose finalized fill left signing dust."""
    state = TradingState()
    now = Timestamp.now()
    requested = Decimal("2.296296296296296298")
    signed = Decimal("2.2962")
    predict = VenueID("PREDICT")
    execution = ArbitrageExecutionJournal(
        id="rounding-review", status=ArbitrageExecutionStatus.NEEDS_REVIEW,
        leg1_venue_id=predict, leg1_contract_id=ContractID("predict:2108502:no"),
        leg1_side=OrderSide.BUY, leg1_quantity=Quantity(Decimal(6)),
        leg1_limit_price=Price(Decimal("0.74")), leg1_client_order_id=ClientOrderID("primary"),
        leg1_filled_quantity=Quantity(Decimal(6) - requested),
        leg2_venue_id=VenueID("POLYMARKET"), leg2_contract_id=ContractID("polymarket:btc:yes"),
        leg2_side=OrderSide.BUY, leg2_quantity=Quantity(Decimal(6)),
        leg2_limit_price=Price(Decimal("0.23")), leg2_client_order_id=ClientOrderID("hedge"),
        leg2_filled_quantity=Quantity(Decimal(6)), residual_quantity=Quantity(requested - signed),
        created_at=now, updated_at=now,
    )
    state.executions[execution.id] = execution
    recovery = ExposureRecovery(
        id=execution.id, execution_id=execution.id, venue_id=predict,
        contract_id=execution.leg1_contract_id, side=OrderSide.BUY,
        quantity=Quantity(requested), limit_price=Price(Decimal("0.75")),
        status=RecoveryStatus.NEEDS_REVIEW, attempts=1, created_at=now, updated_at=now,
        client_order_id=ClientOrderID("recovery"), route=RecoveryRoute.COMPLETE_MISSING_LEG,
        source_contract_id=execution.leg2_contract_id, source_side=OrderSide.BUY,
        source_price=execution.leg2_limit_price, source_fee=Money(Decimal("0.02"), Currency("USD")),
        estimated_vwap=Price(Decimal("0.75")),
        estimated_recovery_fee=Money(Decimal("0.015308"), Currency("USD")),
        estimated_gross_result=Decimal("0.04"), estimated_net_result=Decimal("0.004"),
        filled_quantity=Quantity(signed), average_price=Price(Decimal("0.75")),
        recovery_fee=Money(Decimal("0.015308"), Currency("USD")),
    )
    for role, venue, contract, client, quantity, filled, price in (
        ("primary", predict, execution.leg1_contract_id, execution.leg1_client_order_id,
         execution.leg1_quantity, execution.leg1_filled_quantity, execution.leg1_limit_price),
        ("hedge", execution.leg2_venue_id, execution.leg2_contract_id, execution.leg2_client_order_id,
         execution.leg2_quantity, execution.leg2_filled_quantity, execution.leg2_limit_price),
        ("recovery", predict, recovery.contract_id, recovery.client_order_id,
         recovery.quantity, recovery.filled_quantity, recovery.limit_price),
    ):
        command = SubmitOrder(execution.id, role, venue, OrderIntent(
            contract, OrderSide.BUY, quantity, OrderType.LIMIT,
            client_order_id=client, limit_price=price, time_in_force=TimeInForce.IOC,
        ))
        state.commands[client] = command
        reference = OrderReference(venue, client, b"signed-reference")
        state.prepared[client] = PreparedOrder(reference, b"signed-request")
        state.orders[client] = OrderSnapshot(
            OrderStatus.FILLED if role != "primary" else OrderStatus.CANCELLED,
            contract, OrderSide.BUY, filled if role == "recovery" else quantity,
            OrderType.LIMIT, client_order_id=client, filled_quantity=filled,
            average_price=price, limit_price=price, may_receive_more_fills=False,
            settlement_finalized_block=110, order_id=OrderID(str(client)),
            fee=TradingFee(Money(Decimal("0.015308"), Currency("OUTCOME_TOKEN")), recovery.recovery_fee),
        )
    recovery = replace(recovery, order_id=state.orders[recovery.client_order_id].order_id)
    state.recoveries[execution.id] = recovery
    fill = Trade(TradeID("verified-recovery"), recovery.contract_id, predict, recovery.side,
        recovery.filled_quantity, recovery.average_price, now, order_id=recovery.order_id,
        client_order_id=recovery.client_order_id,
        fee=state.orders[recovery.client_order_id].fee.charged,
        fee_settlement_cost=recovery.recovery_fee)
    state.trades[fill.id] = fill
    stop = TradingSafetyStop(predict, "Cancellation could not prove the order terminal: recovery (unknown)",
        now, execution_id=execution.id, client_order_id=recovery.client_order_id)
    state.apply(stop)
    engine = TradingEngine(EventDispatcher(state), {})

    class _ReconciliationLoop:
        """Apply ordinary accounting outputs without submitting any commands."""
        def __init__(self):
            self.events = []

        async def process(self, event, **kwargs):
            assert kwargs.get("enqueue_commands") is False
            pending = [event]
            while pending:
                current = pending.pop(0)
                assert not isinstance(current, SubmitOrder)
                self.events.append(current)
                pending.extend(engine.process(current))

    loop = _ReconciliationLoop()
    accounting = RuntimeAccounting(None, state, EventDispatcher(state),
        SimpleNamespace(event_loop=loop), lambda: (), {})
    return accounting, execution, recovery, loop


def test_explicit_reconciliation_closes_finalized_predict_rounding_review():
    """Record the actual late fill and fee, retain dust, and never invent a trade."""
    async def run_test():
        accounting, execution, recovery, loop = _predict_rounding_review()
        state = accounting.state
        state.trades.clear()
        final = state.orders[recovery.client_order_id]
        state.orders[recovery.client_order_id] = replace(final,
            status=OrderStatus.CANCELLED, filled_quantity=Quantity(Decimal(0)),
            average_price=None, fee=None, may_receive_more_fills=True, settlement_finalized_block=None)
        state.recoveries[execution.id] = replace(recovery, filled_quantity=Quantity(Decimal(0)), recovery_fee=None)
        state.executions[execution.id] = replace(execution, residual_quantity=recovery.quantity)
        accounting._execution = {recovery.venue_id: SimpleNamespace(
            reconcile=lambda reference: ReconciliationResult(ReconciliationStatus.FOUND, reference, final))}
        reconciled = await accounting.reconcile_execution(execution.id)
        assert reconciled is state.executions[execution.id]
        assert state.executions[execution.id].status is ArbitrageExecutionStatus.RECOVERED
        assert state.executions[execution.id].residual_quantity.value == Decimal("0.000096296296296298")
        assert "retained signing dust" in state.executions[execution.id].last_error
        assert state.recoveries[execution.id].status is RecoveryStatus.RESOLVED
        assert len(state.trades) == 1
        trade = next(iter(state.trades.values()))
        assert trade.client_order_id == recovery.client_order_id
        assert trade.quantity.value == Decimal("2.2962")
        assert trade.price.value == Decimal("0.75")
        assert trade.fee_settlement_cost.amount == Decimal("0.015308")
        assert state.safety_halted and not state.trading_enabled
        assert execution.id in state.execution_safety_stops
        assert TradingEngine(EventDispatcher(state), {}).recovery_outputs() == ()
        assert _terminal_execution_financials(
            state.executions[execution.id], state.recoveries[execution.id], tuple(state.trades.values()),
        ) == (None, None, None)
        before = tuple(loop.events)
        await accounting._reconcile_execution_orders(execution.id)
        assert tuple(loop.events) == before
    asyncio.run(run_test())


@pytest.mark.parametrize("failure", (
    "trading", "stop_client", "stop_reason", "not_final", "no_chain_finality",
    "wrong_client", "wrong_order", "partial", "one_step", "different_dust",
    "attempts", "overlap", "original_unsettled", "missing_fee", "missing_trade",
))
def test_predict_dust_review_rejects_unproven_or_overlapping_recovery(failure):
    """Only the exact finalized truncation incident can close an explicit review."""
    async def run_test():
        accounting, execution, recovery, loop = _predict_rounding_review()
        state = accounting.state
        client = recovery.client_order_id
        snapshot = state.orders[client]
        if failure == "trading":
            state.trading_enabled = True
        elif failure == "missing_trade":
            state.trades.clear()
        elif failure.startswith("stop_"):
            state.execution_safety_stops[execution.id] = replace(
                state.execution_safety_stops[execution.id],
                **({"client_order_id": ClientOrderID("another")} if failure == "stop_client"
                   else {"reason": "Late fill on an earlier recovery attempt"}))
        elif failure in {"attempts", "missing_fee"}:
            state.recoveries[execution.id] = replace(recovery,
                **({"attempts": 2} if failure == "attempts" else {"recovery_fee": None}))
        elif failure == "overlap":
            command = state.commands[client]
            other = ClientOrderID("overlap")
            state.commands[other] = replace(command, intent=replace(command.intent, client_order_id=other))
        elif failure == "original_unsettled":
            state.orders[execution.leg1_client_order_id] = replace(
                state.orders[execution.leg1_client_order_id], may_receive_more_fills=True,
                settlement_finalized_block=None)
        elif failure == "different_dust":
            state.executions[execution.id] = replace(execution, residual_quantity=Quantity(Decimal("0.00001")))
        else:
            changes = {
                "not_final": {"may_receive_more_fills": True, "settlement_finalized_block": None},
                "no_chain_finality": {"settlement_finalized_block": None},
                "wrong_client": {"client_order_id": ClientOrderID("another")},
                "wrong_order": {"order_id": OrderID("another")},
                "partial": {"filled_quantity": Quantity(Decimal("2.2"))},
                "one_step": {"quantity": Quantity(Decimal("2.2961")), "filled_quantity": Quantity(Decimal("2.2961"))},
            }[failure]
            state.orders[client] = replace(snapshot, **changes)
        await accounting._resolve_predict_rounding_review(execution.id)
        assert state.executions[execution.id].status is ArbitrageExecutionStatus.NEEDS_REVIEW
        assert loop.events == []
    asyncio.run(run_test())
