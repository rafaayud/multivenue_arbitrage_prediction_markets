"""Prepare and reconcile covered short inventory for selected markets.

Responsibilities
----------------
- Resolve monitored markets to exact venue inventory operations.
- Prepare and roll covered inventory when recurring contracts change.
- Journal inventory operations before applying their derived positions.
"""

import asyncio
import json
import logging
import os
from dataclasses import replace
from decimal import Decimal
from uuid import uuid4

from prediction_markets.api.trading.runner import LiveArbitrageConfig
from prediction_markets.application.engine import TradingEngine
from prediction_markets.application.events import (
    InventoryOperationRecorded,
    MarketSettlementRecorded,
    PositionUpdated,
)
from prediction_markets.application.execution.short_inventory import (
    InventoryOperationRecord,
    ShortInventoryService,
)
from prediction_markets.application.markets.models import (
    MonitoredMarket,
    market_expiry_guard_seconds,
    monitored_market_key,
)
from prediction_markets.application.state import EventDispatcher, TradingState
from prediction_markets.domain.contracts.entities import BinaryContract
from prediction_markets.domain.market_matching.value_objects import (
    MatchedContractPair,
    RegularCandidate,
)
from prediction_markets.domain.outcome_inventory import (
    InventoryOperationID,
    InventoryOperationReference,
    InventoryOperationSnapshot,
    InventoryOperationStatus,
    InventoryReconciliationResult,
    InventoryReconciliationStatus,
    OutcomeInventoryAction,
    OutcomeInventoryBalance,
    OutcomeInventorySettlement,
)
from prediction_markets.domain.ports.outcome_inventory import OutcomeInventoryPort
from prediction_markets.domain.shared.value_objects import (
    ContractID,
    Currency,
    MarketID,
    Money,
    PortfolioID,
    PositionID,
    Quantity,
    Timestamp,
    VenueID,
)
from prediction_markets.domain.trading.enums import PositionSide
from prediction_markets.infrastructure.binary_journal import BinaryJournal
from prediction_markets.infrastructure.fee_conversion import CoinGeckoFeeConverter
from prediction_markets.infrastructure.venues.limitless.inventory import (
    LimitlessOutcomeInventoryAdapter,
)
from prediction_markets.infrastructure.venues.limitless.mappers import LIMITLESS_VENUE_ID
from prediction_markets.infrastructure.venues.polymarket.inventory import (
    PolymarketOutcomeInventoryAdapter,
)
from prediction_markets.infrastructure.venues.polymarket.mappers import POLYMARKET_VENUE_ID
from prediction_markets.infrastructure.venues.predict.inventory import (
    PredictOutcomeInventoryAdapter,
)
from prediction_markets.infrastructure.venues.predict.mappers import PREDICT_VENUE_ID


_events = logging.getLogger("prediction_markets.events.runtime")


def _build_short_inventory_adapters(
    limitless_adapter: OutcomeInventoryPort | None = None,
) -> dict[VenueID, OutcomeInventoryPort]:
    """Construct the live inventory adapters and close partial composition.

    Parameters
    ----------
    limitless_adapter
        Existing Limitless adapter to reuse when automatic redemption already
        initialized it.
    """
    adapters: dict[VenueID, OutcomeInventoryPort] = {}
    try:
        adapters[POLYMARKET_VENUE_ID] = PolymarketOutcomeInventoryAdapter()
        adapters[LIMITLESS_VENUE_ID] = (
            limitless_adapter
            or LimitlessOutcomeInventoryAdapter(
                api_key=os.getenv("LIMITLESS_API_KEY"),
                api_secret=os.getenv("LIMITLESS_API_SECRET"),
            )
        )
        adapters[PREDICT_VENUE_ID] = PredictOutcomeInventoryAdapter()
    except BaseException:
        for adapter in adapters.values():
            if adapter is limitless_adapter:
                continue
            adapter.close()
        raise
    return adapters


def _short_inventory_selections(
    state: TradingState,
    market_keys: tuple[str, ...],
    *,
    minimum_time_remaining_seconds: int | None = None,
) -> tuple[
    tuple[str, dict[VenueID, MarketID], tuple[MatchedContractPair, ...]],
    ...,
]:
    """Resolve selected monitor keys to exact venue markets and contract pairs.

    Raises
    ------
    RuntimeError
        If a selection has no current pair or resolves ambiguously at a venue.
    """
    current = {
        monitored_market_key(market): (market, pairs)
        for market, pairs in state.matches.items()
    }
    selections = []
    for market_key in dict.fromkeys(market_keys):
        current_market = current.get(market_key)
        if current_market is None or not current_market[1]:
            raise RuntimeError(f"Short market has no current matched pairs: {market_key}")
        market, pairs = current_market
        if minimum_time_remaining_seconds is not None:
            error = _short_inventory_expiry_error(
                market_key,
                market,
                pairs,
                minimum_time_remaining_seconds,
            )
            if error is not None:
                raise RuntimeError(error)
        selections.append(_short_inventory_selection(market_key, pairs))
    return tuple(selections)


def _short_inventory_expiry_error(
    market_key: str,
    market: MonitoredMarket,
    pairs: tuple[MatchedContractPair, ...],
    default_seconds: int,
) -> str | None:
    """Reject cycle inventory preparation inside the submission expiry guard.

    Parameters
    ----------
    market_key
        Stable monitored-market key used in diagnostics.
    market
        Cycle or regular market whose configured guard applies.
    pairs
        Current matched pairs considered for inventory preparation.
    default_seconds
        Fallback minimum remaining lifetime in seconds.

    Returns
    -------
    str | None
        Bounded rejection reason, or ``None`` when the selection may prepare
        inventory.

    Notes
    -----
    - Regular candidates rely on fresh executable books for tradability because
      their timestamps may represent the event date rather than venue closure.
    """
    if isinstance(market, RegularCandidate):
        return None
    minimum = market_expiry_guard_seconds(market, default_seconds)
    remaining = min(
        (pair.ends_at.value - Timestamp.now().value).total_seconds()
        for pair in pairs
    )
    if remaining >= minimum:
        return None
    return (
        f"Short market {market_key} is inside its expiry guard: "
        f"{max(remaining, 0):.3f}s remaining, {minimum}s required"
    )


def _short_inventory_selection(
    market_key: str,
    pairs: tuple[MatchedContractPair, ...],
) -> tuple[str, dict[VenueID, MarketID], tuple[MatchedContractPair, ...]]:
    """Resolve one selected cycle to exact venue markets and contract pairs.

    Raises
    ------
    RuntimeError
        If one venue resolves to multiple markets in the same cycle.
    """
    markets: dict[VenueID, MarketID] = {}
    for pair in pairs:
        for contract in (pair.left, pair.right):
            previous = markets.get(contract.venue_id)
            if previous is not None and previous != contract.market_id:
                raise RuntimeError(
                    f"Short market {market_key} resolves multiple markets for "
                    f"{contract.venue_id}",
                )
            markets[contract.venue_id] = contract.market_id
    return market_key, markets, pairs


def _accounted_short_quantity(
    state: TradingState,
    portfolio_id: PortfolioID,
    contract: BinaryContract,
    balance: OutcomeInventoryBalance,
) -> Quantity:
    """Cap physical short inventory to the matching strategy WAC position."""
    contract_ids = {balance.yes_contract_id, balance.no_contract_id}
    if contract_ids == {None}:
        return balance.mergeable_quantity
    if contract.id not in contract_ids:
        raise RuntimeError(
            f"Inventory contract mismatch for {contract.venue_id}/{contract.id}",
        )
    position = state.positions.get(
        PositionID(f"{contract.venue_id}:{portfolio_id}:{contract.id}"),
    )
    accounted = max(position.signed_quantity if position is not None else 0, 0)
    if accounted == 0 and balance.mergeable_quantity.value > 0:
        raise RuntimeError(
            f"Covered inventory for {contract.venue_id}/{contract.id} is not "
            f"accounted in portfolio {portfolio_id}. If you split manually without "
            "paying gas, use Register manual splits in the trading dialog first.",
        )
    return Quantity(min(balance.mergeable_quantity.value, accounted))


def _manual_split_snapshot(
    state: TradingState,
    balance: OutcomeInventoryBalance,
    portfolio_id: PortfolioID,
) -> InventoryOperationSnapshot | None:
    """Value explicitly declared, unaccounted complete sets at one collateral unit.

    Parameters
    ----------
    state
        Existing inventory across all portfolios sharing the venue wallet.
    balance
        Current holdings for one selected binary market.
    portfolio_id
        Portfolio receiving only the unaccounted complete sets.

    Returns
    -------
    InventoryOperationSnapshot | None
        Opening split accounting, or no record when holdings already reconcile.

    Raises
    ------
    RuntimeError
        If contract identities or tracked holdings cannot support a split import.

    Notes
    -----
    - The operator attests that excess tokens came from fee-free manual splits.
      Balances alone cannot prove acquisition cost or sponsorship.
    - Count all portfolios to avoid adopting inventory already owned elsewhere.
      Unequal deltas or short positions require separate reconciliation.
    - This records opening inventory now, not a historical transaction. The
      pre-import balance is reconstructed solely for split accounting.
    """
    contracts = (balance.yes_contract_id, balance.no_contract_id)
    if None in contracts or contracts[0] == contracts[1]:
        raise RuntimeError("Manual split import requires distinct YES and NO contracts")
    unaccounted = []
    for contract_id, available in zip(contracts, (balance.yes, balance.no), strict=True):
        positions = [
            position for position in state.positions.values()
            if position.venue_id == balance.venue_id and position.contract_id == contract_id
        ]
        if any(
            position.signed_quantity < 0
            or (position.quantity.value > 0 and position.average_price is None)
            for position in positions
        ):
            raise RuntimeError(f"Reconcile existing position basis before importing {contract_id}")
        accounted = sum((position.signed_quantity for position in positions), Decimal("0"))
        unaccounted.append(available.value - accounted)
    if min(unaccounted) < 0 or unaccounted[0] != unaccounted[1]:
        raise RuntimeError(
            f"Cannot import manual splits for {balance.venue_id}/{balance.market_id}: "
            f"unaccounted YES={unaccounted[0]}, NO={unaccounted[1]}. "
            "Refresh balances or reconcile external trades/transfers first.",
        )
    if unaccounted[0] == 0:
        return None
    quantity = Quantity(unaccounted[0])
    reference = InventoryOperationReference(
        venue_id=balance.venue_id,
        operation_id=InventoryOperationID(f"manual-split:{uuid4().hex}"),
        recovery_data=b'{"schema":1,"source":"operator_declared_fee_free_split"}',
        quantity=quantity,
        action=OutcomeInventoryAction.SPLIT,
        portfolio_id=portfolio_id,
        balance_before=replace(
            balance,
            yes=Quantity(balance.yes.value - quantity.value),
            no=Quantity(balance.no.value - quantity.value),
            collateral=Money(
                balance.collateral.amount + quantity.value, balance.collateral.currency,
            ),
        ),
    )
    zero_fee = Money(Decimal("0"), Currency("USD"))
    return InventoryOperationSnapshot(
        reference=reference,
        status=InventoryOperationStatus.CONFIRMED,
        updated_at=balance.observed_at,
        quantity=quantity,
        yes_quantity=quantity,
        no_quantity=quantity,
        collateral=Money(quantity.value, balance.collateral.currency),
        fee=zero_fee,
        fee_settlement_cost=zero_fee,
        fee_observed_at=balance.observed_at,
        reason="Operator-declared fee-free manual split; opening inventory at import time",
    )


def _legacy_short_position_repairs(
    state: TradingState,
    portfolio_id: PortfolioID,
) -> tuple[PositionUpdated, ...]:
    """Net exact legacy LONG and strategy SHORT books after historical splits."""
    repairs: list[PositionUpdated] = []
    positions = tuple(state.positions.values())
    for legacy in positions:
        if (
            legacy.portfolio_id != PortfolioID("default")
            or legacy.side is not PositionSide.LONG
            or legacy.average_price is None
        ):
            continue
        strategy = next(
            (
                position
                for position in positions
                if position.venue_id == legacy.venue_id
                and position.contract_id == legacy.contract_id
                and position.portfolio_id == portfolio_id
                and position.side is PositionSide.SHORT
                and position.quantity == legacy.quantity
                and position.average_price is not None
            ),
            None,
        )
        if strategy is None or strategy.average_price is None:
            continue
        realized = (
            legacy.realized_pnl
            + strategy.realized_pnl
            + legacy.quantity.value
            * (strategy.average_price.value - legacy.average_price.value)
        )
        repairs.extend(
            (
                PositionUpdated(
                    replace(
                        legacy,
                        side=PositionSide.FLAT,
                        quantity=Quantity(Decimal("0")),
                        average_price=None,
                        current_price=None,
                        realized_pnl=Decimal("0"),
                    ),
                ),
                PositionUpdated(
                    replace(
                        strategy,
                        side=PositionSide.FLAT,
                        quantity=Quantity(Decimal("0")),
                        average_price=None,
                        current_price=None,
                        realized_pnl=realized,
                    ),
                ),
            ),
        )
    return tuple(repairs)


class ShortInventoryCoordinator:
    """Own covered-short preparation and Limitless redemption.

    Notes
    -----
    - Inventory operations are serialized so balance deltas and signer nonces
      cannot overlap between activation and automatic redemption.
    """

    def __init__(
        self,
        state: TradingState,
        engine: TradingEngine,
        journal: BinaryJournal,
        dispatcher: EventDispatcher,
        execution_resources: list[object],
    ) -> None:
        self.state = state
        self.engine = engine
        self.journal = journal
        self.dispatcher = dispatcher
        self._execution_resources = execution_resources
        self._service_lock = asyncio.Lock()
        # ponytail: serialize inventory for these shared wallets; use per-signer
        # locks if independent wallets need concurrent inventory operations.
        self._operation_lock = asyncio.Lock()
        self._service: ShortInventoryService | None = None
        self._limitless_service: ShortInventoryService | None = None
        self._limitless_adapter: LimitlessOutcomeInventoryAdapter | None = None
        self._fee_converter: CoinGeckoFeeConverter | None = None
        self._short_inventory_preparing = False
        self._prepared_short_market_keys: tuple[str, ...] = ()

    @property
    def preparing(self) -> bool:
        return self._short_inventory_preparing

    @property
    def prepared_market_keys(self) -> tuple[str, ...]:
        return self._prepared_short_market_keys

    def clear_prepared(self) -> None:
        self._prepared_short_market_keys = ()

    async def prepare(
        self,
        market_keys: tuple[str, ...],
        *,
        minimum_time_remaining_seconds: int | None = None,
    ) -> tuple[
        frozenset[tuple[str, str]],
        dict[ContractID, Quantity],
    ]:
        """Prepare complete sets for selected current markets before trading.

        Parameters
        ----------
        market_keys
            Stable monitored-market keys selected for covered short trading.
        minimum_time_remaining_seconds
            Default expiry guard applied before any inventory transaction.

        Returns
        -------
        tuple[frozenset[tuple[str, str]], dict[ContractID, Quantity]]
            Exact pair keys and conservative per-contract covered quantities.
        """
        async with self._operation_lock:
            if self.state is None:
                raise RuntimeError("Arbitrage runtime is not initialized")
            self._prepared_short_market_keys = ()
            selections = _short_inventory_selections(
                self.state,
                market_keys,
                minimum_time_remaining_seconds=minimum_time_remaining_seconds,
            )
            self._short_inventory_preparing = True
            inventory_by_contract: dict[ContractID, Quantity] = {}
            try:
                service = await self._ensure_service()
                await service.reconcile_pending(
                    tuple(self.state.pending_inventory_operations.values()),
                )
                for _, markets, pairs in selections:
                    balances = await service.ensure_short_inventory(markets)
                    balance_by_venue = {balance.venue_id: balance for balance in balances}
                    for pair in pairs:
                        for contract in (pair.left, pair.right):
                            inventory_by_contract.setdefault(
                                contract.id,
                                _accounted_short_quantity(
                                    self.state,
                                    self.portfolio_id(),
                                    contract,
                                    balance_by_venue[contract.venue_id],
                                ),
                            )
            finally:
                self._short_inventory_preparing = False
            self._prepared_short_market_keys = tuple(
                market_key for market_key, _, _ in selections
            )
            return (
                frozenset(
                    pair.key
                    for _, _, pairs in selections
                    for pair in pairs
                ),
                inventory_by_contract,
            )

    async def import_manual_splits(
        self,
        market_keys: tuple[str, ...],
    ) -> tuple[InventoryOperationSnapshot, ...]:
        """Journal unaccounted complete sets explicitly declared as fee-free splits.

        Parameters
        ----------
        market_keys
            Selected SHORT events whose current venue balances may be adopted.

        Returns
        -------
        tuple[InventoryOperationSnapshot, ...]
            Newly imported sets; empty when every token is already accounted for.

        Raises
        ------
        RuntimeError
            If trading, executions or pending operations can change balances,
            or the surplus does not consist of equal YES and NO quantities.

        Notes
        -----
        - Reads venues but never submits a split, merge or order. The caller must
          obtain the operator's explicit cost-basis and zero-fee declaration.
        - Validate every selected balance before writing. Journal replay and
          serialized balance checks prevent repeated imports of the same tokens.
        """
        async with self._operation_lock:
            self._require_idle_for_import()
            if not market_keys:
                raise RuntimeError("Select at least one SHORT event to import manual splits")
            selections = _short_inventory_selections(self.state, market_keys)
            service = await self._ensure_service()
            balances = {}
            for _, markets, _ in selections:
                for balance in await service.get_balances(markets):
                    balances[(balance.venue_id, balance.market_id)] = balance
            self._require_idle_for_import()
            snapshots = []
            for balance in balances.values():
                snapshot = _manual_split_snapshot(self.state, balance, self.portfolio_id())
                if snapshot is not None:
                    snapshots.append(snapshot)
            for snapshot in snapshots:
                self._record(InventoryReconciliationResult(
                    status=InventoryReconciliationStatus.FOUND,
                    reference=snapshot.reference,
                    snapshot=snapshot,
                ))
            return tuple(snapshots)

    def _require_idle_for_import(self) -> None:
        """Prevent importing balances while executions or inventory can move them."""
        if (
            self.state.trading_enabled
            or self.engine.active_execution_count()
            or self.state.pending_inventory_operations
        ):
            raise RuntimeError("Stop trading and resolve pending operations before importing splits")

    async def reconcile_predict_transaction(
        self,
        operation_id: str,
        transaction_hash: str,
    ) -> InventoryOperationSnapshot:
        """Reattach and reconcile a proven Predict transaction hash.

        Parameters
        ----------
        operation_id
            Pending durable inventory operation to repair.
        transaction_hash
            Exact 32-byte BNB Chain transaction hash returned after broadcast.

        Returns
        -------
        InventoryOperationSnapshot
            Terminal operation recorded in the authoritative journal.

        Raises
        ------
        KeyError
            If the operation is not currently pending.
        ValueError
            If the operation is not Predict or the hash is malformed/conflicting.
        RuntimeError
            If trading is enabled or the transaction remains uncertain.
        """
        if self.state.trading_enabled:
            raise RuntimeError("Disable trading before repairing inventory")
        if (
            len(transaction_hash) != 66
            or not transaction_hash.startswith("0x")
            or any(
                character not in "0123456789abcdefABCDEF"
                for character in transaction_hash[2:]
            )
        ):
            raise ValueError("Transaction hash must be a 32-byte hexadecimal value")
        operation = InventoryOperationID(operation_id)
        reference = self.state.pending_inventory_operations.get(operation)
        if reference is None:
            raise KeyError(operation_id)
        if reference.venue_id != PREDICT_VENUE_ID:
            raise ValueError("Only Predict inventory transactions can be repaired by hash")
        data = json.loads(reference.recovery_data)
        existing = data.get("transaction_hash")
        if existing is not None and existing.lower() != transaction_hash.lower():
            raise ValueError("Inventory operation already references another transaction")
        repaired = replace(
            reference,
            recovery_data=json.dumps(
                {"schema": 1, "transaction_hash": transaction_hash},
                separators=(",", ":"),
                sort_keys=True,
            ).encode(),
        )
        async with self._operation_lock:
            service = await self._ensure_service()
            snapshots = await service.reconcile_pending((repaired,))
        if not snapshots:
            raise RuntimeError("Predict inventory transaction remains uncertain")
        return snapshots[0]


    async def redeem_limitless_available(
        self,
    ) -> tuple[InventoryOperationSnapshot, ...]:
        """Redeem resolved Limitless outcome tokens currently in the account.

        Returns
        -------
        tuple[InventoryOperationSnapshot, ...]
            Confirmed automatic redemption operations.
        """
        async with self._operation_lock:
            service = await self._ensure_limitless_service()
            await service.reconcile_pending(tuple(
                reference
                for reference in self.state.pending_inventory_operations.values()
                if reference.venue_id == LIMITLESS_VENUE_ID
            ))
            return await service.redeem_available(LIMITLESS_VENUE_ID)

    async def redeem_limitless_periodically(
        self,
        interval_seconds: float = 600.0,
        stop_event: asyncio.Event | None = None,
    ) -> None:
        """Redeem available Limitless tokens repeatedly until stopped.

        Parameters
        ----------
        interval_seconds : float, default=600.0
            Delay between redemption checks.
        stop_event
            Optional cooperative stop signal used by the runtime lifecycle.

        Raises
        ------
        ValueError
            If the interval is not positive.

        Notes
        -----
        - Venue errors are logged and retried on the next interval so a
          temporary Limitless or Base failure does not stop live trading.
        """
        if interval_seconds <= 0:
            raise ValueError("Limitless redemption interval must be positive")
        while True:
            if stop_event is None:
                await asyncio.sleep(interval_seconds)
            else:
                try:
                    await asyncio.wait_for(
                        stop_event.wait(),
                        timeout=interval_seconds,
                    )
                except TimeoutError:
                    pass
                else:
                    return
            try:
                redeemed = await self.redeem_limitless_available()
            except Exception as error:
                _events.warning("Automatic Limitless redemption failed: %s", error)
            else:
                if redeemed:
                    _events.info(
                        "Automatically redeemed %d Limitless inventory operations",
                        len(redeemed),
                    )

    async def _ensure_service(self) -> ShortInventoryService:
        """Compose live inventory adapters once and attach journal recording."""
        if self._service is not None:
            return self._service
        async with self._service_lock:
            if self._service is not None:
                return self._service
            if self.journal is None:
                raise RuntimeError("Arbitrage journal is not initialized")
            adapters = await asyncio.to_thread(
                _build_short_inventory_adapters,
                self._limitless_adapter,
            )
            self._execution_resources.extend(
                adapter
                for adapter in adapters.values()
                if adapter is not self._limitless_adapter
            )
            fee_converter = self._ensure_fee_converter()
            self._service = ShortInventoryService(
                adapters,
                self._record,
                portfolio_id=self.portfolio_id(),
                convert_fee=fee_converter.convert,
            )
            self._limitless_service = self._service
            return self._service

    async def _ensure_limitless_service(self) -> ShortInventoryService:
        """Compose the Limitless-only service used by automatic redemption."""
        if self._service is not None:
            return self._service
        async with self._service_lock:
            if self._service is not None:
                return self._service
            if self._limitless_service is not None:
                return self._limitless_service
            if self.journal is None:
                raise RuntimeError("Arbitrage journal is not initialized")
            adapter = await asyncio.to_thread(
                LimitlessOutcomeInventoryAdapter,
                api_key=os.getenv("LIMITLESS_API_KEY"),
                api_secret=os.getenv("LIMITLESS_API_SECRET"),
            )
            self._limitless_adapter = adapter
            self._execution_resources.append(adapter)
            fee_converter = self._ensure_fee_converter()
            self._limitless_service = ShortInventoryService(
                {LIMITLESS_VENUE_ID: adapter},
                self._record,
                portfolio_id=self.portfolio_id(),
                convert_fee=fee_converter.convert,
            )
            return self._limitless_service

    def _ensure_fee_converter(self) -> CoinGeckoFeeConverter:
        """Create the shared native-fee converter once."""
        if self._fee_converter is None:
            self._fee_converter = CoinGeckoFeeConverter()
            self._execution_resources.append(self._fee_converter)
        return self._fee_converter

    def portfolio_id(self) -> PortfolioID:
        """Return the live-run portfolio used for fills and inventory WAC."""
        config = getattr(self.engine, "_config", None) if self.engine is not None else None
        if config is not None:
            return config.portfolio_id
        return LiveArbitrageConfig().engine_config().portfolio_id

    def _record(self, record: InventoryOperationRecord) -> None:
        """Append an inventory operation before its next external side effect."""
        if self.journal is None:
            raise RuntimeError("Arbitrage journal is not initialized")
        event = (
            MarketSettlementRecorded(record)
            if isinstance(record, OutcomeInventorySettlement)
            else InventoryOperationRecorded(record)
        )
        positions_before = dict(self.state.positions) if self.state is not None else {}
        self.journal.append(event)
        if self.dispatcher is not None:
            self.dispatcher.dispatch(event)
        if self.state is not None:
            for position_id, position in self.state.positions.items():
                if positions_before.get(position_id) == position:
                    continue
                updated = PositionUpdated(position)
                self.journal.append(updated)
                if self.dispatcher is not None:
                    self.dispatcher.dispatch(updated)
