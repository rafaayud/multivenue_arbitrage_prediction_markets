"""Verify covered-short inventory preparation and cleanup."""

import asyncio
from dataclasses import replace
from decimal import Decimal

import pytest

from prediction_markets.application.events import InventoryOperationRecorded
from prediction_markets.application.state import TradingState
from prediction_markets.application.execution.short_inventory import (
    InventoryOperationRecord,
    ShortInventoryError,
    ShortInventoryService,
)
from prediction_markets.domain.outcome_inventory import (
    InventoryOperationReference,
    InventoryOperationSnapshot,
    InventoryOperationStatus,
    InventoryReconciliationResult,
    InventoryReconciliationStatus,
    InventorySubmissionResult,
    InventorySubmissionStatus,
    OutcomeInventoryAction,
    OutcomeInventoryBalance,
    OutcomeInventoryIntent,
    OutcomeInventorySettlement,
    PreparedInventoryOperation,
)
from prediction_markets.domain.ports.outcome_inventory import OutcomeInventoryPort
from prediction_markets.domain.trading.portfolio import STRATEGY_PORTFOLIO_ID
from prediction_markets.domain.shared.value_objects import (
    Currency,
    ContractID,
    MarketID,
    Money,
    Price,
    Quantity,
    Timestamp,
    VenueID,
)


class _Inventory(OutcomeInventoryPort):
    """Apply deterministic balance changes and confirm through reconciliation."""

    def __init__(
        self,
        venue_id: VenueID,
        market_id: MarketID,
        *,
        yes: str,
        no: str,
    ) -> None:
        self.venue_id = venue_id
        self.market_id = market_id
        self.yes = Decimal(yes)
        self.no = Decimal(no)
        self.collateral = Decimal("20")
        self.prepared: dict[str, OutcomeInventoryIntent] = {}
        self.submitted: list[OutcomeInventoryIntent] = []
        self.reconciled: list[InventoryOperationReference] = []

    def list_redeemable_markets(self) -> tuple[MarketID, ...]:
        """Expose this deterministic market as the account's redeemable set."""
        return (self.market_id,)

    def get_balance(self, market_id: MarketID) -> OutcomeInventoryBalance:
        assert market_id == self.market_id
        return OutcomeInventoryBalance(
            venue_id=self.venue_id,
            market_id=market_id,
            yes=Quantity(self.yes),
            no=Quantity(self.no),
            collateral=Money(self.collateral, Currency("USD")),
            observed_at=Timestamp.now(),
            yes_contract_id=ContractID(f"{self.venue_id}:{market_id}:yes"),
            no_contract_id=ContractID(f"{self.venue_id}:{market_id}:no"),
        )

    def get_settlement(self, market_id: MarketID) -> OutcomeInventorySettlement:
        balance = self.get_balance(market_id)
        return OutcomeInventorySettlement(
            venue_id=self.venue_id,
            market_id=market_id,
            yes_contract_id=balance.yes_contract_id,
            no_contract_id=balance.no_contract_id,
            yes_payout=Price(Decimal("1")),
            no_payout=Price(Decimal("0")),
            observed_at=Timestamp.now(),
        )

    def prepare(self, intent: OutcomeInventoryIntent) -> PreparedInventoryOperation:
        reference = InventoryOperationReference(
            intent.venue_id,
            intent.operation_id,
            f"reference:{intent.operation_id}".encode(),
            action=intent.action,
            quantity=intent.quantity,
            portfolio_id=intent.portfolio_id,
            balance_before=self.get_balance(intent.market_id),
        )
        self.prepared[str(intent.operation_id)] = intent
        return PreparedInventoryOperation(intent, reference, b"prepared-request")

    def submit(
        self,
        operation: PreparedInventoryOperation,
    ) -> InventorySubmissionResult:
        intent = operation.intent
        self.submitted.append(intent)
        quantity = intent.quantity.value if intent.quantity is not None else None
        if intent.action is OutcomeInventoryAction.SPLIT:
            self.collateral -= quantity
            self.yes += quantity
            self.no += quantity
        elif intent.action is OutcomeInventoryAction.MERGE:
            self.yes -= quantity
            self.no -= quantity
            self.collateral += quantity
        else:
            self.yes = Decimal("0")
            self.no = Decimal("0")
        submitted_reference = replace(
            operation.reference,
            recovery_data=f"submitted-reference:{intent.operation_id}".encode(),
        )
        return InventorySubmissionResult(
            InventorySubmissionStatus.ACCEPTED,
            submitted_reference,
            InventoryOperationSnapshot(
                submitted_reference,
                InventoryOperationStatus.PENDING,
                Timestamp.now(),
                transaction_id=f"tx:{intent.operation_id}",
            ),
        )

    def reconcile(
        self,
        reference: InventoryOperationReference,
    ) -> InventoryReconciliationResult:
        self.reconciled.append(reference)
        return InventoryReconciliationResult(
            InventoryReconciliationStatus.FOUND,
            reference,
            InventoryOperationSnapshot(
                reference,
                InventoryOperationStatus.CONFIRMED,
                Timestamp.now(),
                transaction_id=f"tx:{reference.operation_id}",
            ),
        )


class _TransientBalanceFailure(_Inventory):
    """Fail the first balance read with a retryable transport error."""

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.balance_reads = 0

    def get_balance(self, market_id: MarketID) -> OutcomeInventoryBalance:
        self.balance_reads += 1
        if self.balance_reads == 1:
            raise ConnectionError("remote closed connection")
        return super().get_balance(market_id)


class _NativeFeeInventory(_Inventory):
    def reconcile(
        self,
        reference: InventoryOperationReference,
    ) -> InventoryReconciliationResult:
        result = super().reconcile(reference)
        assert result.snapshot is not None
        return replace(
            result,
            snapshot=replace(
                result.snapshot,
                fee=Money(Decimal("0.002"), Currency("ETH")),
                fee_observed_at=result.snapshot.updated_at,
            ),
        )


def test_ensures_five_complete_sets_per_venue_without_duplicate_splits() -> None:
    """Split only each venue's deficit and record preparation before submission."""

    async def run() -> None:
        left_venue, right_venue = VenueID("left"), VenueID("right")
        left_market, right_market = MarketID("left-market"), MarketID("right-market")
        left = _Inventory(left_venue, left_market, yes="0", no="0")
        right = _Inventory(right_venue, right_market, yes="2", no="2")
        records: list[InventoryOperationRecord] = []
        service = ShortInventoryService(
            {left_venue: left, right_venue: right},
            records.append,
            portfolio_id=STRATEGY_PORTFOLIO_ID,
            poll_interval_seconds=0.001,
        )
        markets = {left_venue: left_market, right_venue: right_market}

        balances = await service.ensure_short_inventory(markets)
        repeated = await service.ensure_short_inventory(markets)

        assert tuple(balance.mergeable_quantity.value for balance in balances) == (
            Decimal("5"),
            Decimal("5"),
        )
        assert tuple(balance.mergeable_quantity.value for balance in repeated) == (
            Decimal("5"),
            Decimal("5"),
        )
        assert [intent.quantity.value for intent in left.submitted] == [Decimal("5")]
        assert [intent.quantity.value for intent in right.submitted] == [Decimal("3")]
        assert left.yes == left.no == right.yes == right.no == Decimal("5")
        assert all(
            reference.recovery_data.startswith(b"submitted-reference:")
            for reference in (*left.reconciled, *right.reconciled)
        )
        first_submission = next(
            index
            for index, record in enumerate(records)
            if isinstance(record, InventorySubmissionResult)
        )
        assert all(
            isinstance(record, PreparedInventoryOperation)
            for record in records[:first_submission]
        )
        assert first_submission == 2

    asyncio.run(run())


def test_retries_one_transient_balance_read() -> None:
    """Retry one idempotent balance read after a remote disconnect."""

    async def run() -> None:
        venue, market = VenueID("venue"), MarketID("market")
        inventory = _TransientBalanceFailure(
            venue,
            market,
            yes="0",
            no="0",
        )
        service = ShortInventoryService(
            {venue: inventory},
            lambda _: None,
            portfolio_id=STRATEGY_PORTFOLIO_ID,
            poll_interval_seconds=0.001,
        )

        balances = await service.ensure_short_inventory({venue: market})

        assert balances[0].mergeable_quantity == Quantity(Decimal("5"))
        assert inventory.balance_reads == 4

    asyncio.run(run())


def test_records_confirmed_inventory_with_converted_native_fee() -> None:
    async def run() -> None:
        venue, market = VenueID("venue"), MarketID("market")
        records: list[InventoryOperationRecord] = []
        service = ShortInventoryService(
            {venue: _NativeFeeInventory(venue, market, yes="0", no="0")},
            records.append,
            portfolio_id=STRATEGY_PORTFOLIO_ID,
            convert_fee=lambda fee, _at: Money(
                fee.amount * Decimal("3000"),
                Currency("USD"),
            ),
            poll_interval_seconds=0.001,
        )

        await service.ensure_short_inventory({venue: market})

        confirmed = next(
            record.snapshot
            for record in records
            if isinstance(record, InventoryReconciliationResult)
        )
        assert confirmed is not None
        assert confirmed.fee == Money(Decimal("0.002"), Currency("ETH"))
        assert confirmed.fee_settlement_cost == Money(
            Decimal("6.000"),
            Currency("USD"),
        )

    asyncio.run(run())


def test_merges_complete_sets_then_redeems_one_sided_inventory() -> None:
    """Merge only local pairs and leave one-sided tokens for later redemption."""

    async def run() -> None:
        left_venue, right_venue = VenueID("left"), VenueID("right")
        left_market, right_market = MarketID("left-market"), MarketID("right-market")
        left = _Inventory(left_venue, left_market, yes="2", no="5")
        right = _Inventory(right_venue, right_market, yes="5", no="1")
        service = ShortInventoryService(
            {left_venue: left, right_venue: right},
            lambda _: None,
            portfolio_id=STRATEGY_PORTFOLIO_ID,
            poll_interval_seconds=0.001,
        )
        markets = {left_venue: left_market, right_venue: right_market}

        merged = await service.cleanup(markets, "merge")

        assert len(merged) == 2
        assert (left.yes, left.no) == (Decimal("0"), Decimal("3"))
        assert (right.yes, right.no) == (Decimal("4"), Decimal("0"))
        assert left.submitted[0].quantity == Quantity(Decimal("2"))
        assert right.submitted[0].quantity == Quantity(Decimal("1"))

        redeemed = await service.cleanup(markets, "redeem")

        assert len(redeemed) == 2
        assert left.yes == left.no == right.yes == right.no == Decimal("0")

    asyncio.run(run())


def test_redeems_all_markets_discovered_from_a_venue_account() -> None:
    """Check account-discovered markets before preparing redemption."""

    async def run() -> None:
        venue, market = VenueID("venue"), MarketID("market")
        inventory = _Inventory(venue, market, yes="2", no="0")
        service = ShortInventoryService(
            {venue: inventory},
            lambda _: None,
            portfolio_id=STRATEGY_PORTFOLIO_ID,
            poll_interval_seconds=0.001,
        )

        redeemed = await service.redeem_available(venue)

        assert len(redeemed) == 1
        assert inventory.submitted[0].action is OutcomeInventoryAction.REDEEM
        assert inventory.yes == inventory.no == Decimal("0")

    asyncio.run(run())


@pytest.mark.parametrize("fail_during_submit", [False, True])
def test_persists_terminal_failure_before_raising(monkeypatch, fail_during_submit) -> None:
    """Replay the authoritative failure instead of leaving a pending operation."""
    venue, market = VenueID("venue"), MarketID("market")
    inventory = _Inventory(venue, market, yes="0", no="0")
    records = []
    failed = lambda ref: InventoryOperationSnapshot(
        ref, InventoryOperationStatus.FAILED, Timestamp.now(), reason="reverted",
    )
    if fail_during_submit:
        monkeypatch.setattr(inventory, "submit", lambda operation: InventorySubmissionResult(
            InventorySubmissionStatus.ACCEPTED, operation.reference, failed(operation.reference),
        ))
    else:
        monkeypatch.setattr(inventory, "reconcile", lambda ref: InventoryReconciliationResult(
            InventoryReconciliationStatus.FOUND, ref, failed(ref),
        ))
    service = ShortInventoryService(
        {venue: inventory}, records.append, portfolio_id=STRATEGY_PORTFOLIO_ID,
    )
    with pytest.raises(ShortInventoryError, match="reverted"):
        asyncio.run(service.ensure_short_inventory({venue: market}))
    state = TradingState()
    for record in records:
        state.apply(InventoryOperationRecorded(record))
    assert not state.pending_inventory_operations
    assert next(iter(state.inventory_operations.values())).status is InventoryOperationStatus.FAILED


def test_recovers_partial_split_after_restart_without_resubmitting(monkeypatch) -> None:
    """Recover the latest durable reference and apply each split's WAC once."""
    left = _Inventory(VenueID("left"), MarketID("left-market"), yes="0", no="0")
    right = _Inventory(VenueID("right"), MarketID("right-market"), yes="0", no="0")
    adapters = {left.venue_id: left, right.venue_id: right}
    markets = {adapter.venue_id: adapter.market_id for adapter in adapters.values()}
    records = []
    original = right.reconcile
    monkeypatch.setattr(right, "reconcile", lambda ref: InventoryReconciliationResult(
        InventoryReconciliationStatus.UNKNOWN, ref,
    ))
    service = ShortInventoryService(
        adapters, records.append, portfolio_id=STRATEGY_PORTFOLIO_ID,
        reconciliation_timeout_seconds=0.01, poll_interval_seconds=0.001,
    )
    with pytest.raises(ShortInventoryError, match="did not confirm"):
        asyncio.run(service.ensure_short_inventory(markets))
    state = TradingState()
    for record in records:
        state.apply(InventoryOperationRecorded(record))
    assert len(state.pending_inventory_operations) == 1
    reference = next(iter(state.pending_inventory_operations.values()))
    assert reference.recovery_data.startswith(b"submitted-reference:")
    assert len(state.positions) == 2

    def record_result(record):
        records.append(record)
        state.apply(InventoryOperationRecorded(record))

    restarted = ShortInventoryService(
        adapters, record_result, portfolio_id=STRATEGY_PORTFOLIO_ID,
        reconciliation_timeout_seconds=0.01, poll_interval_seconds=0.001,
    )
    with pytest.raises(ShortInventoryError, match="reconcile before retrying"):
        asyncio.run(restarted.reconcile_pending((reference,)))
    monkeypatch.setattr(right, "reconcile", original)
    asyncio.run(restarted.reconcile_pending((reference,)))
    asyncio.run(restarted.reconcile_pending((reference,)))
    asyncio.run(restarted.ensure_short_inventory(markets))
    assert not state.pending_inventory_operations
    assert len(left.submitted) == len(right.submitted) == 1
    assert len(state.positions) == 4
    assert all(position.quantity.value == 5 for position in state.positions.values())
    assert all(position.average_price.value == Decimal("0.5") for position in state.positions.values())


def test_discards_all_prepared_peers_when_preparation_fails(monkeypatch) -> None:
    """Release successful preparations even when another venue failed first."""
    from threading import Event

    failed = Event()
    left = _Inventory(VenueID("left"), MarketID("left-market"), yes="0", no="0")
    right = _Inventory(VenueID("right"), MarketID("right-market"), yes="0", no="0")
    prepare = left.prepare
    discarded = []
    records = []

    def delayed_prepare(intent):
        assert failed.wait(1)
        return prepare(intent)

    def reject_prepare(intent):
        failed.set()
        raise ValueError("insufficient collateral")

    monkeypatch.setattr(left, "prepare", delayed_prepare)
    monkeypatch.setattr(left, "discard_prepared", discarded.append)
    monkeypatch.setattr(right, "prepare", reject_prepare)
    service = ShortInventoryService(
        {left.venue_id: left, right.venue_id: right}, records.append,
        portfolio_id=STRATEGY_PORTFOLIO_ID,
    )
    with pytest.raises(ShortInventoryError, match="insufficient collateral"):
        asyncio.run(service.ensure_short_inventory({
            left.venue_id: left.market_id, right.venue_id: right.market_id,
        }))
    assert len(discarded) == 1
    assert discarded[0].intent.venue_id == left.venue_id
    assert not left.submitted and not right.submitted and not records


def test_cancelled_preparation_waits_for_signing_and_discards_reservation(monkeypatch) -> None:
    """Cancellation must not strand a nonce in a still-running signing thread."""
    from threading import Event

    async def run():
        inventory = _Inventory(VenueID("venue"), MarketID("market"), yes="0", no="0")
        started, finish = Event(), Event()
        prepare = inventory.prepare
        discarded, records = [], []

        def slow_prepare(intent):
            started.set()
            assert finish.wait(2)
            return prepare(intent)

        monkeypatch.setattr(inventory, "prepare", slow_prepare)
        monkeypatch.setattr(inventory, "discard_prepared", discarded.append)
        service = ShortInventoryService(
            {inventory.venue_id: inventory}, records.append,
            portfolio_id=STRATEGY_PORTFOLIO_ID,
        )
        task = asyncio.create_task(service.ensure_short_inventory({inventory.venue_id: inventory.market_id}))
        assert await asyncio.to_thread(started.wait, 1)
        task.cancel()
        finish.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert len(discarded) == 1
        assert not inventory.submitted and not records

    asyncio.run(run())


def test_insufficient_collateral_stops_all_venues_before_preparation() -> None:
    """Check every split deficit before a funded peer reserves or spends funds."""
    left = _Inventory(VenueID("left"), MarketID("left-market"), yes="0", no="0")
    right = _Inventory(VenueID("right"), MarketID("right-market"), yes="2", no="2")
    right.collateral = Decimal("2.99")
    service = ShortInventoryService(
        {left.venue_id: left, right.venue_id: right}, lambda _: None,
        portfolio_id=STRATEGY_PORTFOLIO_ID,
    )
    with pytest.raises(ShortInventoryError, match="required 3, available 2.99"):
        asyncio.run(service.ensure_short_inventory({
            left.venue_id: left.market_id, right.venue_id: right.market_id,
        }))
    assert not left.prepared and not right.prepared
    assert not left.submitted and not right.submitted
