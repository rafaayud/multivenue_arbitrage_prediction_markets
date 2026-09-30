"""Exercise definitive cancellation without broadcasting real transactions."""

import asyncio
import json
import time
from dataclasses import replace
from decimal import Decimal
from types import SimpleNamespace
from threading import Event, Lock
from unittest.mock import AsyncMock, Mock

import pytest
import httpx
from requests import Response
from requests.exceptions import HTTPError, ReadTimeout
from web3 import Web3
from web3.exceptions import TransactionNotFound

from prediction_markets.application.codec import decode_event, encode_event
from prediction_markets.application.events import OrderCancellationPrepared, OrderSnapshotUpdated, TradingSafetyStop
from prediction_markets.application.pipeline import EventSink, OutputDispatcher, RingBuffer
from prediction_markets.application.state import TradingState
from prediction_markets.domain.trading.enums import OrderStatus, ReconciliationStatus, SubmissionStatus, OrderSide
from prediction_markets.domain.trading.value_objects import PreparedOrder, ReconciliationResult, SubmissionResult
from prediction_markets.infrastructure.binary_journal import BinaryJournal
from prediction_markets.infrastructure.venues.predict.cancellation import PredictOnChainCancellation
from prediction_markets.infrastructure.venues.predict.order_updates import PredictOrderUpdateAdapter
from tests.infrastructure.venues.predict.test_predict_settlement import _order, _event

_HASH = "0x" + "ab" * 32
_ACCOUNT = "0x" + "12" * 20
_EXCHANGE = "0x" + "34" * 20


def _chain(side=OrderSide.SELL):
    """Build a fake chain with controllable finality, logs and transaction I/O."""
    command, reference, initial = _order()
    command = replace(command, intent=replace(command.intent, side=side))
    raw = dict(salt="1", maker=_ACCOUNT, signer=_ACCOUNT, taker="0x" + "00" * 20,
               tokenId="123", makerAmount=str(5 * 10**18), takerAmount=str(10**18),
               expiration="4102444800", nonce="0", feeRateBps="200", side=1,
               signatureType=0)
    if side is OrderSide.BUY:
        raw.update(side=0, makerAmount=str(10**18), takerAmount=str(5 * 10**18))
    data = json.loads(reference.recovery_data)
    data.update(order_hash=_HASH, side=side.value, chain={"from_block": 100,
        "is_neg_risk": False, "is_yield_bearing": False, "order": raw})
    reference = replace(reference, recovery_data=json.dumps(data).encode())
    initial = replace(initial, order_id=type(initial.order_id)(_HASH), side=side,
                      status=OrderStatus.CANCELLED)
    exchange = SimpleNamespace(address=_EXCHANGE, abi=[])
    exchange.functions = Mock()
    exchange.functions.getOrderStatus.return_value.call.return_value = (False, 0)
    exchange.events = Mock()
    exchange.events.OrderFilled.return_value.get_logs.return_value = []
    exchange.encode_abi = Mock(return_value="0x1234")
    method = Mock()
    method.estimate_gas.return_value = 100000
    method.build_transaction.side_effect = lambda transaction: transaction
    kernel = SimpleNamespace(functions=Mock())
    kernel.functions.execute.return_value = method
    eth = Mock()
    eth.block_number = 100
    eth.gas_price = 100000000
    eth.chain_id = 56
    eth.get_block.return_value = {"number": 110, "hash": b"b" * 32}
    eth.get_transaction_count.return_value = 7
    eth.get_balance.return_value = 10**18
    eth.get_transaction_receipt.side_effect = TransactionNotFound("not mined")
    eth.contract.side_effect = lambda address, abi: exchange if address == _EXCHANGE else kernel
    builder = SimpleNamespace(_predict_account=_ACCOUNT,
        _signer=SimpleNamespace(address=_ACCOUNT, sign_transaction=Mock(
            return_value=SimpleNamespace(raw_transaction=b"signed-cancel"))),
        contracts=SimpleNamespace(ctf_exchange=exchange), _execution_mode=b"mode",
        _encode_execution_calldata=Mock(return_value=b"calldata"),
        build_typed_data=Mock(side_effect=lambda order, **options: order),
        build_typed_data_hash=Mock(return_value=_HASH))
    builder._chain_id = 56
    helper = PredictOnChainCancellation(builder, Decimal("0.0001"), web3=SimpleNamespace(eth=eth))
    return helper, command, PreparedOrder(reference, b"original-order"), initial, exchange, eth


def _fill(quantity, *, buy=False):
    """Return one exact-order on-chain fill at price 0.2."""
    return {"address": _EXCHANGE, "transactionHash": b"f" * 32, "logIndex": 0,
        "blockNumber": 105, "removed": False,
        "args": {"orderHash": bytes.fromhex(_HASH[2:]), "maker": _ACCOUNT,
            "makerAssetId": 0 if buy else 123, "takerAssetId": 123 if buy else 0,
            "makerAmountFilled": quantity * 10**18 // 5 if buy else quantity * 10**18,
            "takerAmountFilled": quantity * 10**18 if buy else quantity * 10**18 // 5,
            "fee": 20000000000000000}}


@pytest.mark.parametrize("quantity", (0, 2, 5))
@pytest.mark.parametrize("side", (OrderSide.SELL, OrderSide.BUY))
def test_finalized_scan_proves_zero_partial_and_full_fills(quantity, side):
    """Only finalized invalidation plus complete logs unlock the exact remainder."""
    helper, _, prepared, initial, exchange, _ = _chain(side)
    exchange.functions.getOrderStatus.return_value.call.return_value = (True, 0)
    logs = [_fill(quantity, buy=side is OrderSide.BUY)] * 2 if quantity else []
    exchange.events.OrderFilled.return_value.get_logs.return_value = logs
    result = helper.reconcile(prepared.reference, initial)
    assert result.filled_quantity.value == quantity
    assert result.may_receive_more_fills is False
    assert result.settlement_finalized_block == 110
    assert result.status is (OrderStatus.FILLED if quantity == 5 else OrderStatus.CANCELLED)
    if quantity:
        assert result.average_price.value == Decimal("0.2")
        assert result.fee.charged.amount == Decimal("0.02")
    exchange.functions.getOrderStatus.return_value.call.assert_called_with(block_identifier=110)


def test_unfinalized_removed_order_never_authorizes_replacement():
    """API cancellation and a latest block are not finalized evidence."""
    helper, _, prepared, initial, exchange, _ = _chain()
    assert helper.reconcile(prepared.reference, initial) == initial
    exchange.events.OrderFilled.return_value.get_logs.assert_not_called()


@pytest.mark.parametrize("side", (OrderSide.BUY, OrderSide.SELL))
@pytest.mark.parametrize("stored_quantity", ("2.296296296296296298", "2.2962"))
def test_rounded_recovery_reference_accounts_for_late_finalized_fill(side, stored_quantity):
    """Reconcile rounded legacy and new orders after a zero-fill cancellation."""
    helper, _, prepared, initial, exchange, _ = _chain(side)
    data = json.loads(prepared.reference.recovery_data)
    data["quantity"] = stored_quantity
    data["chain"]["order"]["takerAmount" if side is OrderSide.BUY else "makerAmount"] = "2296200000000000000"
    reference = replace(prepared.reference, recovery_data=json.dumps(data).encode())
    initial = replace(initial, quantity=type(initial.quantity)(Decimal(stored_quantity)))
    updates = PredictOrderUpdateAdapter(lambda: "jwt", api_key="test")
    updates.record_snapshot(reference, initial, "submit")
    assert helper.reconcile(reference, initial) == initial
    exchange.functions.getOrderStatus.return_value.call.return_value = (True, 0)
    log = _fill(1, buy=side is OrderSide.BUY)
    log["args"]["takerAmountFilled" if side is OrderSide.BUY else "makerAmountFilled"] = 2296200000000000000
    log["args"]["makerAmountFilled" if side is OrderSide.BUY else "takerAmountFilled"] = 1722150000000000000
    log["args"]["fee"] = 15308000000000000
    exchange.events.OrderFilled.return_value.get_logs.return_value = [log]
    final = helper.reconcile(reference, initial)
    assert final.filled_quantity.value == Decimal("2.2962")
    assert final.average_price.value == Decimal("0.75")
    assert final.fee.charged.amount == Decimal("0.015308")
    assert final.may_receive_more_fills is False
    assert final.settlement_finalized_block == 110
    assert updates.record_snapshot(reference, final, "get") == final
    assert updates.record_snapshot(reference, initial, "get") == final


@pytest.mark.parametrize("signed_quantity", ("2296100000000000000", "2296300000000000000", "2296296296296296298"))
def test_legacy_quantity_accepts_only_exact_signing_truncation(signed_quantity):
    """Do not replace identity checks with a generic quantity tolerance."""
    helper, _, prepared, initial, _, _ = _chain(OrderSide.BUY)
    data = json.loads(prepared.reference.recovery_data)
    data["quantity"] = "2.296296296296296298"
    data["chain"]["order"]["takerAmount"] = signed_quantity
    reference = replace(prepared.reference, recovery_data=json.dumps(data).encode())
    with pytest.raises(ValueError, match="Cancellation quantity mismatch"):
        helper.reconcile(reference, initial)


@pytest.mark.parametrize("failure", ("logs", "finality", "range", "hash", "token", "regression"))
def test_chain_proof_fails_closed(failure):
    """Missing history, identity conflicts and regressing fills remain uncertain."""
    helper, _, prepared, initial, exchange, eth = _chain()
    exchange.functions.getOrderStatus.return_value.call.return_value = (True, 0)
    if failure == "logs":
        exchange.events.OrderFilled.return_value.get_logs.side_effect = TimeoutError()
    elif failure == "finality":
        eth.get_block.side_effect = TimeoutError()
    elif failure == "range":
        eth.get_block.return_value = {"number": 50000}
    elif failure == "hash":
        helper.builder.build_typed_data_hash.return_value = "0xwrong"
    elif failure == "token":
        log = _fill(2)
        log["args"]["makerAssetId"] = 999
        exchange.events.OrderFilled.return_value.get_logs.return_value = [log]
    else:
        initial = replace(initial, filled_quantity=type(initial.quantity)(Decimal(2)),
                          average_price=initial.limit_price)
    with pytest.raises((ValueError, TimeoutError)):
        helper.reconcile(prepared.reference, initial)


def test_cancellation_signs_once_and_rebroadcasts_identical_transaction():
    """Preparation is side-effect free; retries never allocate another nonce."""
    helper, _, prepared, _, _, eth = _chain()
    request = helper.prepare(prepared)
    eth.send_raw_transaction.assert_not_called()
    for _ in range(2):
        helper.broadcast(prepared.reference, request)
    assert eth.send_raw_transaction.call_count == 2
    assert all(call.args == (b"signed-cancel",) for call in eth.send_raw_transaction.call_args_list)
    assert helper.builder._signer.sign_transaction.call_count == 1
    signed_transaction = helper.builder._signer.sign_transaction.call_args.args[0]
    assert signed_transaction["gasPrice"] == 100_000_000
    assert json.loads(request)["transaction_hash"] == Web3.to_hex(Web3.keccak(b"signed-cancel"))


def test_first_broadcast_does_not_require_unknown_transaction_receipt():
    """An RPC's archive-token HTTP 403 cannot prevent the initial broadcast."""
    helper, _, prepared, initial, _, eth = _chain()
    request = helper.prepare(prepared)
    response = Response()
    response.status_code = 403
    eth.get_transaction_receipt.side_effect = HTTPError(
        "Archive requests require a personal token", response=response,
    )
    helper.broadcast(prepared.reference, request)
    eth.get_transaction_receipt.assert_not_called()
    eth.send_raw_transaction.assert_called_once_with(b"signed-cancel")
    # A successful send is not proof of invalidation or absence of late fills.
    assert helper.reconcile(prepared.reference, initial) == initial


@pytest.mark.parametrize("error", (ReadTimeout("ambiguous send"), ValueError("already known")))
def test_broadcast_error_replays_persisted_bytes_without_resigning(error):
    """Uncertain sends and duplicate RPC responses never allocate another nonce."""
    helper, _, prepared, _, exchange, eth = _chain()
    request = helper.prepare(prepared)
    nonce_reads = eth.get_transaction_count.call_count
    eth.send_raw_transaction.side_effect = (error, b"accepted")
    with pytest.raises(type(error)):
        helper.broadcast(prepared.reference, request)
    # A fresh helper models the loss of in-memory state after a restart.
    restarted = PredictOnChainCancellation(
        helper.builder, Decimal("0.0001"), web3=helper.web3,
    )
    restarted.broadcast(prepared.reference, request)
    assert all(call.args == (b"signed-cancel",) for call in eth.send_raw_transaction.call_args_list)
    assert eth.send_raw_transaction.call_count == 2
    assert helper.builder._signer.sign_transaction.call_count == 1
    assert eth.get_transaction_count.call_count == nonce_reads
    exchange.functions.getOrderStatus.return_value.call.return_value = (True, 0)
    restarted.broadcast(prepared.reference, request)
    assert eth.send_raw_transaction.call_count == 2


@pytest.mark.parametrize("field", ("order_hash", "transaction_hash"))
def test_broadcast_rejects_changed_persisted_identity(field):
    """Removing receipt polling does not weaken the persisted transaction checks."""
    helper, _, prepared, _, _, eth = _chain()
    request = json.loads(helper.prepare(prepared))
    request[field] = "0x" + "00" * 32
    with pytest.raises(ValueError):
        helper.broadcast(prepared.reference, json.dumps(request).encode())
    eth.send_raw_transaction.assert_not_called()


@pytest.mark.parametrize("failure", ("gas", "balance", "pending_nonce"))
def test_cancellation_budget_and_pending_signer_block_signing(failure):
    """Do not sign an unaffordable cancellation or race an outstanding nonce."""
    helper, _, prepared, _, _, eth = _chain()
    if failure == "gas":
        eth.gas_price = 10**12
    elif failure == "balance":
        eth.get_balance.return_value = 0
    else:
        eth.get_transaction_count.side_effect = (8, 7)
    with pytest.raises((ValueError, RuntimeError)):
        helper.prepare(prepared)
    helper.builder._signer.sign_transaction.assert_not_called()


def test_finalized_snapshot_survives_late_wallet_duplicates_and_stale_rest():
    """Finalized partial fill totals cannot be double-counted by delayed events."""
    helper, _, prepared, initial, exchange, _ = _chain()
    updates = PredictOrderUpdateAdapter(lambda: "jwt", api_key="test")
    updates.record_snapshot(prepared.reference, initial, "submit")
    exchange.functions.getOrderStatus.return_value.call.return_value = (True, 0)
    exchange.events.OrderFilled.return_value.get_logs.return_value = [_fill(2)]
    final = helper.reconcile(prepared.reference, initial)
    assert updates.record_snapshot(prepared.reference, final, "get") == final
    updates._handle({**_event("orderTransactionSuccess", quantity="2"), "orderHash": _HASH})
    assert updates.record_snapshot(prepared.reference, initial, "get") == final
    assert decode_event(encode_event(OrderSnapshotUpdated("execution", "recovery", prepared.reference, final, "get"))).snapshot == final


@pytest.fixture
def anyio_backend():
    """Use the application's asyncio loop for coroutine tests."""
    return "asyncio"


@pytest.mark.anyio
async def test_dispatcher_journals_before_broadcast_and_reuses_after_restart(tmp_path):
    """Replay retains the exact signed cancellation, including ambiguous sends."""
    _, command, prepared, initial, _, _ = _chain()
    state = TradingState()
    journal = BinaryJournal(tmp_path / "journal")
    adapter = Mock(supports_definitive_cancellation=True, cancellation_transaction_lock=None)
    adapter.prepare_cancellation.return_value = b"one-signed-transaction"
    def submit(order, request):
        assert isinstance(journal.entries()[-1].event, OrderCancellationPrepared)
        assert journal.durable_sequence == journal.entries()[-1].sequence
        assert request == b"one-signed-transaction"
        return ReconciliationResult(ReconciliationStatus.FOUND, order.reference, initial)
    adapter.submit_cancellation.side_effect = submit
    dispatcher = OutputDispatcher(RingBuffer(8), EventSink(RingBuffer(8)), journal, state)
    dispatcher.configure({command.venue_id: adapter})
    await dispatcher._definitive_cancel(command, prepared)
    replayed = TradingState()
    for entry in journal.entries():
        replayed.apply(decode_event(encode_event(entry.event)))
    dispatcher._state = replayed
    await dispatcher._definitive_cancel(command, prepared)
    assert adapter.prepare_cancellation.call_count == 1
    assert len(journal.entries()) == 1
    journal.close()


@pytest.mark.anyio
@pytest.mark.parametrize("failure", ("append", "sync"))
async def test_journal_failure_prevents_cancellation_broadcast(failure):
    """A transaction must not leave the process before its durable append."""
    _, command, prepared, _, _, _ = _chain()
    journal = Mock()
    getattr(journal, failure).side_effect = OSError("disk unavailable")
    adapter = Mock(cancellation_transaction_lock=None)
    adapter.prepare_cancellation.return_value = b"signed"
    dispatcher = OutputDispatcher(RingBuffer(8), EventSink(RingBuffer(8)), journal, TradingState())
    dispatcher.configure({command.venue_id: adapter})
    with pytest.raises(OSError):
        await dispatcher._definitive_cancel(command, prepared)
    adapter.submit_cancellation.assert_not_called()


@pytest.mark.anyio
async def test_monitor_escalates_immediately_and_emits_settled_snapshot():
    """The existing recovery engine can act without the old three-second wait."""
    _, command, prepared, initial, _, _ = _chain()
    adapter = Mock(supports_definitive_cancellation=True)
    adapter.cancellation_window_seconds.return_value = 1.0
    sink = SimpleNamespace(publish=Mock())
    events = []
    async def publish(event):
        events.append(event)
    sink.publish = publish
    dispatcher = OutputDispatcher(RingBuffer(8), sink, Mock(), TradingState())
    dispatcher.configure({command.venue_id: adapter})
    final = replace(initial, may_receive_more_fills=False, settlement_finalized_block=110)
    async def cancel(*args):
        return ReconciliationResult(ReconciliationStatus.FOUND, prepared.reference, final)
    dispatcher._definitive_cancel = cancel
    await asyncio.wait_for(dispatcher._monitor(command, prepared,
        SubmissionResult(SubmissionStatus.ACCEPTED, prepared.reference, initial)), 0.8)
    assert any(isinstance(event, OrderSnapshotUpdated) and event.snapshot == final for event in events)
    assert not any(isinstance(event, TradingSafetyStop) for event in events)


@pytest.mark.anyio
async def test_shutdown_retains_nonce_lock_until_broadcast_thread_finishes():
    """Cancellation of the monitor cannot release an in-flight signer's nonce."""
    _, command, prepared, initial, _, _ = _chain()
    entered, release, signer_lock = Event(), Event(), Lock()
    adapter = Mock(cancellation_transaction_lock=signer_lock)
    adapter.prepare_cancellation.return_value = b"signed"
    def submit(*args):
        entered.set()
        assert release.wait(3)
        return ReconciliationResult(ReconciliationStatus.FOUND, prepared.reference, initial)
    adapter.submit_cancellation.side_effect = submit
    dispatcher = OutputDispatcher(RingBuffer(8), Mock(), Mock(), TradingState())
    dispatcher.configure({command.venue_id: adapter})
    task = asyncio.create_task(dispatcher._definitive_cancel(command, prepared))
    try:
        assert await asyncio.to_thread(entered.wait, 2)
        task.cancel()
        await asyncio.sleep(0.02)
        assert signer_lock.locked()
        assert not task.done()
    finally:
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
    assert not signer_lock.locked()


def test_compaction_preserves_signed_cancellation_and_uncertain_order(tmp_path):
    """A compact recovery snapshot retains both the order and its cancellation."""
    from prediction_markets.application.events import OrderPrepared
    from prediction_markets.infrastructure.recovery_snapshots import build_recovery_snapshot
    _, command, prepared, initial, _, _ = _chain()
    journal = BinaryJournal(tmp_path / "compact")
    for event in (command, OrderPrepared(command, prepared),
            OrderCancellationPrepared(command, b"same-signed-nonce")):
        journal.append(event)
    journal.sync()
    compact = build_recovery_snapshot(journal, previous=None, through_sequence=journal.durable_sequence)
    state = TradingState()
    for record in compact.records():
        state.apply(decode_event(encode_event(record.event)))
    assert state.cancellations[command.intent.client_order_id] == b"same-signed-nonce"
    assert state.prepared[command.intent.client_order_id] == prepared
    journal.close()


def test_preload_then_prepare_and_first_submit_do_not_add_auth_or_chain_io():
    """Only the original orders POST remains after preflight, including the first submit."""
    from tests.infrastructure.venues.predict.test_predict_execution import _collateral_adapter
    adapter, _, client, intent, requests = _collateral_adapter()
    chain = Mock()
    chain.anchor.return_value = 100
    adapter._chain_cancellation = chain
    adapter._jwt, adapter._jwt_expires_at = "ready-jwt", time.time() + 3600
    adapter.preload((intent.contract_id,))
    def accepted(request):
        requests.append(request)
        return httpx.Response(200, json={"success": True, "data": {"orderHash": "0xabc"}})
    client._transport.handler = accepted
    chain.reset_mock()
    requests.clear()
    prepared = adapter.prepare(intent)
    assert not requests
    assert not chain.mock_calls
    assert json.loads(prepared.reference.recovery_data)["chain"]["from_block"] == 100
    adapter.submit(prepared)
    assert [(x.method, x.url.path) for x in requests] == [("POST", "/v1/orders")]
    assert not chain.mock_calls
    adapter._chain_anchor_time -= 61
    with pytest.raises(RuntimeError, match="cancellation metadata"):
        adapter.prepare(intent)
    client.close()


def test_cancellation_anchor_reuses_fresh_value_and_reconnects_after_http_error():
    """Keep RPC refresh off the hot path and replace a failed HTTP session once."""
    from tests.infrastructure.venues.predict.test_predict_execution import _collateral_adapter
    adapter, _, client, intent, _ = _collateral_adapter()
    response = Response()
    response.status_code = 403
    chain = Mock()
    chain.preflight.side_effect = (
        HTTPError("temporary RPC rejection", response=response),
        None,
    )
    chain.anchor.return_value = 123
    adapter._chain_cancellation = chain
    adapter._jwt, adapter._jwt_expires_at = "ready-jwt", time.time() + 3600

    adapter.preload((intent.contract_id,))
    adapter.preload((intent.contract_id,))

    assert chain.preflight.call_count == 2
    chain.reconnect.assert_called_once_with()
    chain.anchor.assert_called_once_with()
    assert adapter._chain_anchor_block == 123
    client.close()


def test_rpc_preflight_rejects_wrong_chain_or_missing_logs():
    """Unsupported RPCs fail before the first original order can be prepared."""
    helper, _, _, _, _, eth = _chain()
    helper.preflight()
    eth.chain_id = 1
    with pytest.raises(ValueError, match="wrong chain"):
        helper.preflight()
    eth.chain_id = 56
    eth.get_logs.side_effect = RuntimeError("logs disabled")
    with pytest.raises(RuntimeError, match="logs disabled"):
        helper.preflight()


@pytest.mark.parametrize("known_fill", (2, 5))
def test_chain_proof_compares_buffered_fills_without_double_counting(known_fill):
    """Buffered fills are included once, and conflicts with chain evidence block."""
    helper, _, prepared, initial, exchange, _ = _chain()
    updates = PredictOrderUpdateAdapter(lambda: "jwt", api_key="test")
    updates._handle({**_event("orderTransactionSuccess", quantity=str(known_fill)), "orderHash": _HASH})
    exchange.functions.getOrderStatus.return_value.call.return_value = (True, 0)
    exchange.events.OrderFilled.return_value.get_logs.return_value = [_fill(2)]
    final = helper.reconcile(prepared.reference, initial)
    if known_fill > 2:
        with pytest.raises(ValueError, match="buffered fills"):
            updates.record_snapshot(prepared.reference, final, "get")
    else:
        assert updates.record_snapshot(prepared.reference, final, "get") == final


def test_cancellation_proof_does_not_depend_on_predict_rest():
    """An unavailable indexer cannot delay a valid finalized cancellation proof."""
    from tests.infrastructure.venues.predict.test_predict_execution import _collateral_adapter
    adapter, _, client, _, _ = _collateral_adapter()
    _, _, prepared, initial, _, _ = _chain()
    final = replace(initial, may_receive_more_fills=False, settlement_finalized_block=110)
    adapter._chain_cancellation = Mock()
    adapter._chain_cancellation.reconcile.return_value = final
    adapter._chain_cancellation.broadcast.side_effect = TimeoutError("ambiguous send")
    adapter._request = Mock(side_effect=TimeoutError("indexer offline"))
    assert adapter.submit_cancellation(prepared, b"signed").snapshot == final
    adapter._request.assert_not_called()
    assert adapter.reconcile(prepared.reference).snapshot == final
    adapter._chain_cancellation.reconcile.side_effect = ValueError("conflicting evidence")
    assert adapter.reconcile(prepared.reference).status is ReconciliationStatus.UNKNOWN
    client.close()


def test_failed_chain_proof_preserves_terminal_rest_snapshot(monkeypatch):
    """Keep known venue state while chain finality remains unavailable."""
    from prediction_markets.infrastructure.venues.predict import execution
    from tests.infrastructure.venues.predict.test_predict_execution import (
        _collateral_adapter,
    )
    adapter, _, client, _, _ = _collateral_adapter()
    _, _, prepared, snapshot, _, _ = _chain()
    adapter._chain_cancellation = Mock()
    adapter._chain_cancellation.reconcile.side_effect = TimeoutError("RPC unavailable")
    adapter._request = Mock(return_value=Mock(json=Mock(return_value={
        "success": True, "data": {},
    })))
    monkeypatch.setattr(execution, "_snapshot_from_order", lambda *_: snapshot)
    adapter._enrich_fills = Mock(return_value=snapshot)

    result = adapter.reconcile(prepared.reference)

    assert result.status is ReconciliationStatus.FOUND
    assert result.snapshot == snapshot
    assert result.snapshot.is_terminal()
    assert result.snapshot.may_receive_more_fills is True
    assert result.snapshot.settlement_finalized_block is None
    client.close()


@pytest.mark.parametrize("failure, reason", (("forbidden", "http_403"), ("timeout", "timeout")))
def test_cancellation_broadcast_errors_are_observable_and_remain_uncertain(failure, reason, caplog):
    """Bounded labels identify transport failures without leaking request data."""
    from prediction_markets.infrastructure.metrics import ORDER_CANCEL_ATTEMPTS
    from tests.infrastructure.venues.predict.test_predict_execution import _collateral_adapter
    adapter, _, client, _, _ = _collateral_adapter()
    _, _, prepared, initial, _, _ = _chain()
    response = Response()
    response.status_code = 403
    error = (HTTPError("secret-url-and-signed-payload", response=response)
             if failure == "forbidden" else ReadTimeout("secret-url-and-signed-payload"))
    adapter._chain_cancellation = Mock()
    adapter._chain_cancellation.broadcast.side_effect = error
    adapter._chain_cancellation.reconcile.return_value = initial
    counter = ORDER_CANCEL_ATTEMPTS.labels("predict", f"chain_broadcast_{reason}")
    before = counter._value.get()
    result = adapter.submit_cancellation(prepared, b"signed")
    assert result.status is ReconciliationStatus.UNKNOWN
    adapter._chain_cancellation.reconcile.assert_called_once()
    assert counter._value.get() == before + 1
    assert "reconciling the persisted transaction" in caplog.text
    assert "secret-url-and-signed-payload" not in caplog.text
    client.close()


@pytest.mark.anyio
async def test_restart_resumes_cancellation_not_original_order(tmp_path):
    """Recovered cancellation bytes cannot fall through to another venue order."""
    from prediction_markets.application.events import OrderPrepared
    from prediction_markets.application.pipeline.order_dispatch import RecoveryCoordinator
    _, command, prepared, initial, _, _ = _chain()
    journal = BinaryJournal(tmp_path / "restart")
    for event in (command, OrderPrepared(command, prepared),
                  OrderCancellationPrepared(command, b"persisted-cancel")):
        journal.append(event)
    journal.sync()
    adapter = Mock(supports_definitive_cancellation=True, cancellation_transaction_lock=None)
    adapter.submit_cancellation.return_value = ReconciliationResult(
        ReconciliationStatus.FOUND, prepared.reference,
        replace(initial, may_receive_more_fills=False, settlement_finalized_block=110))
    dispatcher = OutputDispatcher(RingBuffer(8), Mock(), journal, TradingState())
    dispatcher.configure({command.venue_id: adapter})
    dispatcher.adopt = AsyncMock()
    dispatcher.execute = AsyncMock()
    await RecoveryCoordinator(journal.entries(), dispatcher, {command.venue_id: adapter}).recover()
    adapter.prepare_cancellation.assert_not_called()
    adapter.reconcile.assert_not_called()
    adapter.submit_cancellation.assert_called_once_with(prepared, b"persisted-cancel")
    dispatcher.execute.assert_not_called()
    dispatcher.adopt.assert_awaited_once()
    journal.close()


@pytest.mark.anyio
@pytest.mark.parametrize("already_halted", (False, True))
async def test_restart_monitors_pending_finality_without_resubmitting(tmp_path, already_halted):
    """Normal finality delay starts a monitor instead of aborting application startup."""
    from prediction_markets.application.events import OrderPrepared
    from prediction_markets.application.pipeline.order_dispatch import RecoveryCoordinator
    _, command, prepared, initial, _, _ = _chain()
    journal = BinaryJournal(tmp_path / "pending-finality")
    state = TradingState()
    for event in (command, OrderPrepared(command, prepared),
            OrderSnapshotUpdated(command.execution_id, command.role, prepared.reference, initial, "get"),
            OrderCancellationPrepared(command, b"persisted-cancel")):
        journal.append(event)
        state.apply(event)
    journal.sync()
    state.trading_enabled = not already_halted
    state.safety_halted = already_halted
    state.last_error = "previous review cause" if already_halted else None
    events = []
    async def publish(event):
        events.append(event)
        state.apply(event)
    adapter = Mock(supports_definitive_cancellation=True, cancellation_transaction_lock=None)
    adapter.cancellation_window_seconds.return_value = 1.0
    final = replace(initial, may_receive_more_fills=False, settlement_finalized_block=110)
    adapter.submit_cancellation.side_effect = (
        ReconciliationResult(ReconciliationStatus.UNKNOWN, prepared.reference),
        ReconciliationResult(ReconciliationStatus.FOUND, prepared.reference, final),
    )
    dispatcher = OutputDispatcher(RingBuffer(8), SimpleNamespace(publish=publish), journal, state)
    dispatcher.configure({command.venue_id: adapter})
    dispatcher.execute = AsyncMock()
    try:
        await RecoveryCoordinator(journal.entries(), dispatcher, {command.venue_id: adapter}).recover()
        assert state.safety_halted and not state.trading_enabled
        if already_halted:
            assert state.last_error == "previous review cause"
            assert not any(isinstance(event, TradingSafetyStop) for event in events)
        else:
            assert isinstance(events[0], TradingSafetyStop)
        await asyncio.wait_for(asyncio.gather(*tuple(dispatcher._watchers)), 1)
        assert state.orders[command.intent.client_order_id] == final
        adapter.prepare_cancellation.assert_not_called()
        adapter.reconcile.assert_not_called()
        adapter.cancel.assert_not_called()
        dispatcher.execute.assert_not_awaited()
        assert adapter.submit_cancellation.call_count == 2
        assert all(call.args == (prepared, b"persisted-cancel")
                   for call in adapter.submit_cancellation.call_args_list)
    finally:
        await dispatcher.close()
        journal.close()


@pytest.mark.anyio
@pytest.mark.parametrize("compact", (False, True))
@pytest.mark.parametrize("kind", ("cancel", "expired_batch", "rejected_batch"))
async def test_restart_does_not_dispatch_manually_completed_execution(tmp_path, compact, kind):
    """Durable manual closure survives both raw replay and compact recovery records."""
    from prediction_markets.application.events import (
        ArbitrageOpportunityFound, ArbitragePlanned, ExecutionUpdated, OrderPrepared,
        PreparedExecutionBatch,
    )
    from prediction_markets.application.pipeline.order_dispatch import RecoveryCoordinator
    from prediction_markets.domain.trading.enums import ArbitrageExecutionStatus
    from prediction_markets.infrastructure.recovery_snapshots import build_recovery_snapshot
    from tests.application.test_uncertain_execution_review import _uncertain_short, _observation
    engine, primary, hedge, history = _uncertain_short()
    planned = next(event for event in history if isinstance(event, ArbitragePlanned))
    opportunity = next(event for event in history if isinstance(event, ArbitrageOpportunityFound))
    payloads = tuple(OrderPrepared(command, PreparedOrder(
        _observation(command, "0", OrderStatus.CANCELLED, True).reference, b"original",
    )) for command in (primary, hedge))
    if kind == "cancel":
        history.extend(payloads)
        history.append(OrderCancellationPrepared(hedge, b"persisted-cancel"))
    else:
        history.append(PreparedExecutionBatch(
            opportunity, planned, (primary, hedge), payloads, 0,
            "expired" if kind == "rejected_batch" else None,
        ))
    completed = replace(engine.state.executions[primary.execution_id],
        status=ArbitrageExecutionStatus.COMPLETED, resolution_method="manual_sale",
        residual_quantity=type(primary.intent.quantity)(Decimal(0)))
    history.append(ExecutionUpdated(completed))
    journal = BinaryJournal(tmp_path / "manual-completion")
    for event in history:
        journal.append(event)
    journal.sync()
    entries = journal.entries()
    if compact:
        entries = build_recovery_snapshot(
            journal, previous=None, through_sequence=journal.durable_sequence,
        ).records()
    # The coordinator must honor the durable event even without a prepopulated state.
    adapter = Mock(supports_definitive_cancellation=True, cancellation_transaction_lock=None)
    dispatcher = OutputDispatcher(RingBuffer(8), Mock(), journal, TradingState())
    dispatcher.configure({primary.venue_id: adapter, hedge.venue_id: adapter})
    dispatcher.execute, dispatcher.adopt, dispatcher.reject_pair = AsyncMock(), AsyncMock(), AsyncMock()
    await RecoveryCoordinator(entries, dispatcher,
        {primary.venue_id: adapter, hedge.venue_id: adapter}).recover()
    assert not adapter.mock_calls
    dispatcher.execute.assert_not_awaited()
    dispatcher.adopt.assert_not_awaited()
    dispatcher.reject_pair.assert_not_awaited()
    journal.close()


@pytest.mark.anyio
@pytest.mark.parametrize("status", ("hedge_pending", "needs_review", "completed", "recovered", "rejected"))
async def test_trading_enable_rechecks_executions_after_awaited_preflight(status):
    """A review created during startup cannot be cleared by the same enable call."""
    from prediction_markets.api.runtime.facade import ArbitrageRuntime
    from prediction_markets.api.trading.runner import LiveArbitrageConfig
    from prediction_markets.domain.trading.enums import ArbitrageExecutionStatus
    from tests.infrastructure.test_recovery_snapshots import _active_execution
    _, _, planned, _, _ = _active_execution()
    state = TradingState()
    state.safety_halted = True
    async def collateral():
        state.executions[planned.execution.id] = replace(
            planned.execution, status=ArbitrageExecutionStatus(status),
        )
        return {}
    engine = Mock()
    engine.enable.side_effect = RuntimeError("reached enable")
    runtime = SimpleNamespace(
        start=AsyncMock(), _ensure_execution=AsyncMock(), engine=engine, state=state,
        pipeline=Mock(), _inventory=Mock(), _market_workers=None,
        _safety=SimpleNamespace(read_available_collateral=collateral),
        _venue_health_service=None,
    )
    terminal = status in {"completed", "recovered", "rejected"}
    with pytest.raises(RuntimeError, match="reached enable" if terminal else "Unresolved executions"):
        await ArbitrageRuntime._run_execution(runtime, LiveArbitrageConfig(), asyncio.Event())
    assert engine.enable.call_count == int(terminal)
    assert state.safety_halted and not state.trading_enabled


def test_rollout_flag_selects_definitive_cancellation(monkeypatch):
    """Opt-in construction enables the recovery path without changing order type."""
    from prediction_markets.infrastructure.venues.predict import cancellation
    from tests.infrastructure.venues.predict.test_predict_execution import _collateral_adapter
    helper = Mock()
    monkeypatch.setattr(cancellation, "PredictOnChainCancellation", Mock(return_value=helper))
    monkeypatch.setenv("PREDICT_ONCHAIN_CANCEL_ENABLED", "1")
    adapter, _, client, intent, _ = _collateral_adapter()
    assert adapter.supports_definitive_cancellation
    assert adapter.cancellation_window_seconds(intent) == 1
    client.close()


@pytest.mark.parametrize("quantity", ("0", "2"))
def test_finalized_cancellation_starts_recovery_for_actual_remainder(quantity):
    """Proven cancellation unblocks central recovery once, for only the missing size."""
    from tests.application.test_uncertain_execution_review import _uncertain_short, _observation, _process
    from prediction_markets.application.events import SubmitOrder
    engine, _, hedge, _ = _uncertain_short()
    final = _observation(hedge, quantity, OrderStatus.CANCELLED, False)
    final = replace(final, snapshot=replace(final.snapshot, settlement_finalized_block=110))
    events = _process(engine, final)
    commands = [event for event in events if isinstance(event, SubmitOrder)]
    assert len(commands) == 1
    assert commands[0].role == "recovery"
    assert commands[0].intent.quantity.value == 5 - Decimal(quantity)
    assert not any(isinstance(event, SubmitOrder) for event in _process(engine, final))
