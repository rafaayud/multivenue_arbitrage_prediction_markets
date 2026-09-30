"""Verify recoverable Limitless execution behavior."""

from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from decimal import Decimal
from threading import Barrier
from types import SimpleNamespace

import aiohttp
from limitless_sdk.api.errors import APIError
from limitless_sdk.types import UnsignedOrder

from prediction_markets.domain.shared.value_objects import (
    ClientOrderID,
    ContractID,
    OrderID,
    Price,
    Quantity,
)
from prediction_markets.domain.trading.entities import OrderIntent, OrderSnapshot
from prediction_markets.domain.trading.enums import (
    OrderSide,
    OrderStatus,
    OrderType,
    ReconciliationStatus,
    SubmissionStatus,
    TimeInForce,
)
from prediction_markets.infrastructure.venues.limitless.execution import (
    LimitlessExecutionAdapter,
    LimitlessFillEnricher,
)


class _Signer:
    """Return a deterministic signature while retaining the signing venue."""

    def __init__(self) -> None:
        self.configs = []

    async def sign_order(self, order, config):
        self.configs.append(config)
        return "0xsigned"


class _OrderClient:
    """Build unsigned orders without performing venue I/O."""

    def __init__(self) -> None:
        self._signing_config = SimpleNamespace(chain_id=8453)
        self._signer = _Signer()
        self.wallet = SimpleNamespace(
            address="0x0000000000000000000000000000000000000001",
        )
        self.owner_id = 7
        self.built = []
        self.profile_loads = 0

    async def _ensure_user_data(self):
        self.profile_loads += 1

    async def build_unsigned_order(self, **values):
        self.built.append(values)
        return UnsignedOrder(
            salt=1,
            maker="0x0000000000000000000000000000000000000001",
            signer="0x0000000000000000000000000000000000000001",
            taker="0x0000000000000000000000000000000000000000",
            tokenId=values["token_id"],
            makerAmount=2100000,
            takerAmount=5000000,
            expiration=values["expiration"],
            nonce=0,
            feeRateBps=0,
            side=0 if values["side"].name == "BUY" else 1,
            signatureType=0,
            price=values["price"],
        )


class _MarketFetcher:
    """Return one cached market and venue."""

    def __init__(self) -> None:
        self.calls = []

    async def get_market(self, slug):
        assert slug == "btc-up"
        self.calls.append(slug)
        return SimpleNamespace(
            tokens=SimpleNamespace(yes="yes-token", no="no-token"),
            venue=SimpleNamespace(exchange="0x0000000000000000000000000000000000000002"),
        )


class _HttpClient:
    """Model create, status, and combined cancellation endpoints."""

    def __init__(self) -> None:
        self.requests = []
        self.order = None
        self.execution = {}
        self.close_calls = 0

    async def post(self, path, data):
        self.requests.append((path, data))
        if path == "/orders":
            self.order = {
                "id": "venue-1",
                "status": "LIVE",
            }
            return {"order": self.order, "execution": self.execution}
        if path == "/orders/status/batch":
            assert data == {"items": [{"clientOrderId": "client-1"}]}
            return {
                "results": [
                    {
                        "status": "found" if self.order else "not_found",
                        "data": {"order": {"order": self.order}, "execution": self.execution},
                    },
                ],
            }
        if path == "/orders/cancel":
            assert data == {"clientOrderId": "client-1"}
            self.order["status"] = "CANCELED"
            return {"message": "Order canceled successfully"}
        raise AssertionError(path)

    async def close(self):
        self.close_calls += 1


class _CollateralReader:
    """Return controllable Base USDC balance and exchange allowance values."""

    def __init__(self) -> None:
        self.balance = 10_000_000
        self.allowance = 10_000_000
        self.calls = []

    def __call__(self, owner: str, spender: str) -> tuple[int, int]:
        self.calls.append((owner, spender))
        return self.balance, self.allowance


def _adapter(*, collateral_reader=None):
    http = _HttpClient()
    orders = _OrderClient()
    adapter = LimitlessExecutionAdapter(
        http_client=http,
        market_fetcher=_MarketFetcher(),
        order_client=orders,
        collateral_reader=collateral_reader,
    )
    adapter.preload((ContractID("limitless:btc-up:yes"),))
    return adapter, http, orders


def _intent(**changes):
    values = {
        "contract_id": ContractID("limitless:btc-up:yes"),
        "side": OrderSide.BUY,
        "quantity": Quantity(Decimal("5")),
        "order_type": OrderType.LIMIT,
        "client_order_id": ClientOrderID("client-1"),
        "limit_price": Price(Decimal("0.42")),
    }
    values.update(changes)
    return OrderIntent(**values)


def test_prepares_signed_payload_and_reconciles_after_restart():
    adapter, http, orders = _adapter()
    prepared = adapter.prepare(_intent())

    assert len(orders.built) == 1
    assert orders._signer.configs[0].contract_address.endswith("0002")

    restarted = LimitlessExecutionAdapter(
        http_client=http,
        market_fetcher=_MarketFetcher(),
        order_client=orders,
    )
    assert restarted.reconcile(prepared.reference).status is ReconciliationStatus.NOT_FOUND
    submitted = restarted.submit(prepared)
    reconciled = restarted.reconcile(prepared.reference)

    assert len(orders.built) == 1
    assert submitted.status is SubmissionStatus.ACCEPTED
    submitted_payload = next(data for path, data in http.requests if path == "/orders")
    assert submitted_payload["clientOrderId"] == "client-1"
    assert submitted_payload["order"]["signature"] == "0xsigned"
    assert reconciled.status is ReconciliationStatus.FOUND
    assert reconciled.snapshot.order_id.value == "venue-1"
    assert reconciled.snapshot.status is OrderStatus.ACCEPTED


def test_reads_limitless_collateral_constrained_by_balance_and_allowance():
    """Expose the most restrictive Base USDC account value."""
    collateral = _CollateralReader()
    adapter, http, _ = _adapter(collateral_reader=collateral)
    contracts = (_intent().contract_id,)

    collateral.balance = 973_763
    balance_limited = adapter.get_available_collateral(contracts)
    collateral.balance = 10_000_000
    collateral.allowance = 2_100_000
    allowance_limited = adapter.get_available_collateral(contracts)

    assert balance_limited == Decimal("0.973763")
    assert allowance_limited == Decimal("2.1")
    assert http.requests == []


def test_limitless_collateral_read_reuses_one_market_exchange():
    """Read one allowance for both outcomes sharing a Limitless exchange."""
    collateral = _CollateralReader()
    adapter, _, _ = _adapter(collateral_reader=collateral)

    assert adapter.get_available_collateral(
        (
            ContractID("limitless:btc-up:yes"),
            ContractID("limitless:btc-up:no"),
        ),
    ) == Decimal("10")
    assert adapter.get_available_collateral(()) is None
    assert collateral.calls == [
        (
            "0x0000000000000000000000000000000000000001",
            "0x0000000000000000000000000000000000000002",
        ),
    ]


def test_preloads_and_reuses_market_and_profile_metadata():
    http = _HttpClient()
    markets = _MarketFetcher()
    orders = _OrderClient()
    adapter = LimitlessExecutionAdapter(
        http_client=http,
        market_fetcher=markets,
        order_client=orders,
    )
    contracts = (
        ContractID("limitless:btc-up:yes"),
        ContractID("limitless:btc-up:no"),
    )

    adapter.preload(contracts)
    adapter.preload(contracts)
    adapter.prepare(_intent())

    assert markets.calls == ["btc-up"]
    assert orders.profile_loads == 1


def test_prepare_without_preload_fails_without_loading_metadata():
    """Reject cold preparation without fetching a market or account profile."""
    http = _HttpClient()
    markets = _MarketFetcher()
    orders = _OrderClient()
    adapter = LimitlessExecutionAdapter(
        http_client=http,
        market_fetcher=markets,
        order_client=orders,
    )

    try:
        adapter.prepare(_intent())
    except RuntimeError as error:
        assert "profile was not preloaded" in str(error)
    else:
        raise AssertionError("cold Limitless preparation was accepted")

    assert markets.calls == []
    assert orders.profile_loads == 0
    assert orders.built == []


def test_concurrent_prepares_do_not_wait_for_the_shared_runner_lock():
    """Allow independent cache-only signatures to overlap in worker threads."""

    class _ConcurrentOrderClient(_OrderClient):
        def __init__(self) -> None:
            super().__init__()
            self.barrier = Barrier(2)

        async def build_unsigned_order(self, **values):
            self.barrier.wait(timeout=2)
            return await super().build_unsigned_order(**values)

    http = _HttpClient()
    markets = _MarketFetcher()
    orders = _ConcurrentOrderClient()
    adapter = LimitlessExecutionAdapter(
        http_client=http,
        market_fetcher=markets,
        order_client=orders,
    )
    adapter.preload((ContractID("limitless:btc-up:yes"),))
    intents = (
        _intent(client_order_id=ClientOrderID("client-1")),
        _intent(client_order_id=ClientOrderID("client-2")),
    )

    with ThreadPoolExecutor(max_workers=2) as executor:
        prepared = tuple(executor.map(adapter.prepare, intents))

    assert len(prepared) == 2
    assert len(orders.built) == 2


def test_reconciles_partial_fill_and_exact_buy_fee_from_client_id():
    adapter, http, _ = _adapter()
    prepared = adapter.prepare(_intent())
    adapter.submit(prepared)
    http.execution = {
        "totalsRaw": {
            "contractsGross": "2000000",
            "usdGross": "840000",
            "contractsFee": "16628",
            "usdFee": "12000",
        },
    }

    result = adapter.reconcile(prepared.reference)

    assert result.snapshot.status is OrderStatus.PARTIALLY_FILLED
    assert result.snapshot.filled_quantity == Quantity(Decimal("2"))
    assert result.snapshot.average_price == Price(Decimal("0.42"))
    assert result.snapshot.fee.charged.amount == Decimal("0.016628")
    assert result.snapshot.fee.charged.currency.code == "OUTCOME_TOKEN"
    assert result.snapshot.fee.settlement_cost.amount == Decimal("0.016628")
    assert result.snapshot.fee.settlement_cost.currency.code == "USD"


def test_cancels_using_only_the_persisted_client_id():
    adapter, _, _ = _adapter()
    prepared = adapter.prepare(_intent())
    adapter.submit(prepared)

    result = adapter.cancel(prepared.reference)

    assert result.status is ReconciliationStatus.FOUND
    assert result.snapshot.status is OrderStatus.CANCELLED


def test_market_order_is_prepared_as_price_protected_fak():
    adapter, http, orders = _adapter()
    prepared = adapter.prepare(
        _intent(
            contract_id=ContractID("limitless:btc-up:no"),
            side=OrderSide.SELL,
            order_type=OrderType.MARKET,
            limit_price=None,
            time_in_force=TimeInForce.IOC,
        ),
    )
    adapter.submit(prepared)

    assert orders.built[0]["token_id"] == "no-token"
    assert orders.built[0]["price"] == 0.001
    assert http.requests[0][1]["orderType"] == "FAK"
    assert http.requests[0][1]["postOnly"] is False


def test_transport_timeout_keeps_submission_uncertain():
    adapter, http, _ = _adapter()
    prepared = adapter.prepare(_intent())

    async def fail(path, data):
        raise aiohttp.ClientConnectionError("timeout")

    http.post = fail

    assert adapter.submit(prepared).status is SubmissionStatus.UNKNOWN


def test_rejection_keeps_venue_reason():
    adapter, http, _ = _adapter()
    prepared = adapter.prepare(_intent())

    async def reject(path, data):
        if path == "/orders":
            raise APIError("insufficient balance", status_code=400)
        return await _HttpClient.post(http, path, data)

    http.post = reject
    result = adapter.submit(prepared)

    assert result.status is SubmissionStatus.REJECTED
    assert result.reason == "HTTP 400: insufficient balance"


def test_accepted_request_keeps_terminal_execution_reason():
    adapter, http, _ = _adapter()
    prepared = adapter.prepare(_intent())

    async def unmatched(path, data):
        if path == "/orders":
            return {
                "order": {"id": "venue-1"},
                "execution": {"settlementStatus": "UNMATCHED"},
            }
        return await _HttpClient.post(http, path, data)

    http.post = unmatched
    result = adapter.submit(prepared)

    assert result.status is SubmissionStatus.ACCEPTED
    assert result.snapshot.status is OrderStatus.REJECTED
    assert result.reason == "settlement status UNMATCHED"
    assert result.snapshot.reason == result.reason


def test_reconcile_keeps_reason_after_initial_acceptance():
    """Preserve a late settlement failure, including its explicit venue reason."""
    adapter, http, _ = _adapter()
    prepared = adapter.prepare(_intent())
    accepted = adapter.submit(prepared)
    assert accepted.snapshot.reason is None

    http.execution = {
        "settlementStatus": "FAILED",
        "failureReason": "insufficient balance at settlement",
    }
    rejected = adapter.reconcile(prepared.reference)
    assert rejected.snapshot.status is OrderStatus.REJECTED
    assert rejected.snapshot.reason == "insufficient balance at settlement"


def test_fill_enricher_attaches_partial_fill_and_buy_fee():
    snapshot = OrderSnapshot(
        status=OrderStatus.SUBMITTED,
        contract_id=ContractID("limitless:btc-up:yes"),
        side=OrderSide.BUY,
        quantity=Quantity(Decimal("5")),
        order_type=OrderType.LIMIT,
        client_order_id=ClientOrderID("client-1"),
        order_id=OrderID("venue-1"),
        limit_price=Price(Decimal("0.42")),
    )
    data = {
        "order": {"id": "venue-1", "status": "LIVE"},
        "execution": {
            "totalsRaw": {
                "contractsGross": "2000000",
                "usdGross": "840000",
                "contractsFee": "16628",
                "usdFee": "12000",
            },
        },
    }

    enriched = LimitlessFillEnricher().enrich(snapshot, data)

    assert enriched.status is OrderStatus.PARTIALLY_FILLED
    assert enriched.filled_quantity == Quantity(Decimal("2"))
    assert enriched.average_price == Price(Decimal("0.42"))
    assert enriched.fee.charged.amount == Decimal("0.016628")
    assert enriched.fee.charged.currency.code == "OUTCOME_TOKEN"
    assert enriched.fee.settlement_cost.amount == Decimal("0.016628")

    sell = LimitlessFillEnricher().enrich(
        replace(snapshot, side=OrderSide.SELL),
        data,
    )
    assert sell.fee.charged.amount == Decimal("0.012")
    assert sell.fee.charged.currency.code == "USDC"
    assert sell.fee.settlement_cost.amount == Decimal("0.012")
    assert sell.fee.settlement_cost.currency.code == "USD"
