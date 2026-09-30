"""Invalidate uncertain Predict orders and prove their finalized fills.

Notes
-----
- The dispatcher journals signed cancellation bytes before broadcasting them.
- Chain reads and signing happen only during preload or cancellation, never
  while preparing the original venue order.
- Fill scans start at a block observed before the order was signed. Incomplete
  scans, unavailable finality and conflicting quantities fail closed.
"""

import json
import os
from dataclasses import replace
from decimal import Decimal, ROUND_DOWN
from typing import Any

from predict_sdk import Order, Side, SignatureType
from predict_sdk._internal.contracts import get_exchange_contract
from predict_sdk.abis import KERNEL_ABI
from web3 import HTTPProvider, Web3
from web3.middleware import ExtraDataToPOAMiddleware

from prediction_markets.domain.shared.value_objects import Currency, Money, Price, Quantity, Timestamp
from prediction_markets.domain.trading.entities import OrderSnapshot
from prediction_markets.domain.trading.enums import OrderSide, OrderStatus
from prediction_markets.domain.trading.value_objects import OrderReference, PreparedOrder, TradingFee
from prediction_markets.infrastructure.metrics import ORDER_CANCEL_ATTEMPTS, ORDER_LATENCY
from prediction_markets.infrastructure.venues.predict.config import (
    predict_transaction_gas_price_wei,
)

_WEI = Decimal(10**18)
_LOG_BLOCKS = 1000
_MAX_SCAN_BLOCKS = 32000


class PredictOnChainCancellation:
    """Use the configured SDK signer for order-specific cancellation only.

    Notes
    -----
    - A private RPC client bounds each request to five seconds. No method waits
      synchronously for a transaction receipt or changes account-wide nonces.
    - The caller serializes prepare/journal/broadcast per venue.
    """

    def __init__(self, builder: Any, max_fee_bnb: Decimal, *, web3: Any = None) -> None:
        self.builder = builder
        self.max_fee_wei = int(max_fee_bnb * _WEI)
        if self.max_fee_wei <= 0:
            raise ValueError("Cancellation fee cap must be positive")
        self._endpoint: str | None = None
        if web3 is None:
            provider = builder._web3.provider
            endpoint = os.getenv("PREDICT_CANCELLATION_RPC_URL") or provider.endpoint_uri
            self._endpoint = str(endpoint)
            web3 = self._connect()
        self.web3 = web3

    def _connect(self) -> Web3:
        """Create a fresh bounded RPC connection for owned clients."""
        if self._endpoint is None:
            raise RuntimeError("Cannot reconnect an injected cancellation RPC")
        web3 = Web3(HTTPProvider(self._endpoint, request_kwargs={"timeout": 5},
                                exception_retry_configuration=None))
        web3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
        return web3

    def reconnect(self) -> None:
        """Replace a failed owned HTTP session without changing the endpoint."""
        if self._endpoint is not None:
            self.web3 = self._connect()

    def anchor(self) -> int:
        """Read a pre-submission scan boundary outside order preparation."""
        return int(self.web3.eth.block_number)

    def preflight(self) -> None:
        """Require the correct chain, finalized state and working log queries."""
        if int(self.web3.eth.chain_id) != int(self.builder._chain_id):
            raise ValueError("Cancellation RPC uses the wrong chain")
        block = self.web3.eth.get_block("finalized")
        exchange = self.builder.contracts.ctf_exchange
        self.web3.eth.get_logs({"address": exchange.address,
            "fromBlock": int(block["number"]), "toBlock": int(block["number"]),
            "topics": [Web3.to_hex(Web3.keccak(text="OrderFilled(bytes32,address,address,uint256,uint256,uint256,uint256,uint256)")),
                       "0x" + "00" * 32]})

    def _context(self, reference: OrderReference) -> tuple[dict, Any]:
        """Validate persisted chain identity and select the exact exchange."""
        data = json.loads(reference.recovery_data)
        chain = data.get("chain")
        if not isinstance(chain, dict) or type(chain.get("from_block")) is not int:
            raise ValueError("Order has no pre-submission chain scan boundary")
        if chain["from_block"] < 0:
            raise ValueError("Invalid chain scan boundary")
        raw = chain["order"]
        order = Order(**{
            "salt": raw["salt"], "maker": raw["maker"], "signer": raw["signer"],
            "taker": raw["taker"], "token_id": raw["tokenId"],
            "maker_amount": raw["makerAmount"], "taker_amount": raw["takerAmount"],
            "expiration": raw["expiration"], "nonce": raw["nonce"],
            "fee_rate_bps": raw["feeRateBps"], "side": Side(raw["side"]),
            "signature_type": SignatureType(raw["signatureType"]),
        })
        options = {"is_neg_risk": chain["is_neg_risk"],
                   "is_yield_bearing": chain["is_yield_bearing"]}
        if any(type(value) is not bool for value in options.values()):
            raise ValueError("Invalid exchange selection")
        expected = self.builder.build_typed_data_hash(self.builder.build_typed_data(order, **options))
        if expected.lower() != data["order_hash"].lower():
            raise ValueError("Cancellation reference does not match signed order")
        if raw["maker"].lower() != self.builder._predict_account.lower():
            raise ValueError("Cancellation belongs to another account")
        buy = data["side"] == OrderSide.BUY.value
        if int(raw["side"]) != (0 if buy else 1):
            raise ValueError("Cancellation side mismatch")
        quantity = Decimal(raw["takerAmount" if buy else "makerAmount"]) / _WEI
        requested = Decimal(data["quantity"])
        # Legacy references persisted the unrounded intent. Accept only the exact
        # five-significant-digit truncation used when signing those orders.
        canonical = requested.quantize(
            Decimal(1).scaleb(requested.adjusted() - 4), rounding=ROUND_DOWN,
        )
        if quantity != canonical:
            raise ValueError("Cancellation quantity mismatch")
        selected = get_exchange_contract(self.builder.contracts, **options)
        exchange = self.web3.eth.contract(address=selected.address, abi=selected.abi)
        return data, exchange

    @ORDER_LATENCY.labels("predict", "cancel_chain_prepare").time()
    def prepare(self, prepared: PreparedOrder) -> bytes:
        """Sign one bounded-fee cancellation without submitting it.

        Raises
        ------
        ValueError
            If order identity, scan boundary or fee budget is invalid.
        RuntimeError
            If another signer transaction is pending or funding is insufficient.
        """
        data, exchange = self._context(prepared.reference)
        raw = data["chain"]["order"]
        status = exchange.functions.getOrderStatus(data["order_hash"]).call()
        result = {"schema": 1, "order_hash": data["order_hash"], "raw_transaction": None}
        if status[0]:
            return json.dumps(result).encode()
        signer = self.builder._signer
        nonce = self.web3.eth.get_transaction_count(signer.address, "pending")
        if nonce != self.web3.eth.get_transaction_count(signer.address, "latest"):
            raise RuntimeError("Signer has a pending transaction; cancellation not signed")
        values = [int(raw["salt"]), raw["maker"], raw["signer"], raw["taker"],
                  int(raw["tokenId"]), int(raw["makerAmount"]), int(raw["takerAmount"]),
                  int(raw["expiration"]), int(raw["nonce"]), int(raw["feeRateBps"]),
                  int(raw["side"]), int(raw["signatureType"]), b""]
        encoded = exchange.encode_abi("cancelOrders", args=[[tuple(values)]])
        calldata = self.builder._encode_execution_calldata(exchange.address, encoded, value=0)
        kernel = self.web3.eth.contract(address=self.builder._predict_account, abi=KERNEL_ABI)
        method = kernel.functions.execute(self.builder._execution_mode, calldata)
        gas = (int(method.estimate_gas({"from": signer.address})) * 125) // 100
        gas_price = predict_transaction_gas_price_wei(int(self.web3.eth.gas_price))
        if gas * gas_price > self.max_fee_wei:
            raise ValueError("Cancellation exceeds configured BNB fee cap")
        if self.web3.eth.get_balance(signer.address) < gas * gas_price:
            raise RuntimeError("Signer has insufficient BNB for cancellation")
        transaction = method.build_transaction({"from": signer.address, "nonce": nonce,
            "gas": gas, "gasPrice": gas_price, "chainId": self.web3.eth.chain_id, "value": 0})
        signed = signer.sign_transaction(transaction)
        result.update(raw_transaction=Web3.to_hex(signed.raw_transaction),
                      transaction_hash=Web3.to_hex(Web3.keccak(signed.raw_transaction)),
                      nonce=nonce, max_fee_wei=gas * gas_price)
        return json.dumps(result, separators=(",", ":")).encode()

    @ORDER_LATENCY.labels("predict", "cancel_chain_broadcast").time()
    def broadcast(self, reference: OrderReference, request: bytes) -> None:
        """Rebroadcast only the exact journaled transaction, never a replacement.

        Notes
        -----
        - Receipt availability is not a prerequisite for sending: some public
          RPCs reject unknown transaction receipts with HTTP 403. Replaying the
          same signed bytes cannot create a second transaction. Only finalized
          order state and fill logs can authorize replacement orders.
        """
        data, exchange = self._context(reference)
        transaction = json.loads(request)
        if transaction.get("schema") != 1 or transaction["order_hash"] != data["order_hash"]:
            raise ValueError("Cancellation transaction belongs to another order")
        raw = transaction["raw_transaction"]
        if raw is None:
            return
        raw_bytes = bytes.fromhex(raw.removeprefix("0x"))
        if Web3.to_hex(Web3.keccak(raw_bytes)) != transaction["transaction_hash"]:
            raise ValueError("Cancellation transaction hash mismatch")
        # A mined/latest cancellation needs finality, not another broadcast.
        if exchange.functions.getOrderStatus(data["order_hash"]).call()[0]:
            return
        self.web3.eth.send_raw_transaction(raw_bytes)
        ORDER_CANCEL_ATTEMPTS.labels("predict", "chain_broadcast_sent").inc()

    @ORDER_LATENCY.labels("predict", "cancel_chain_reconcile").time()
    def reconcile(self, reference: OrderReference, snapshot: OrderSnapshot) -> OrderSnapshot:
        """Prove invalidation and all fills through the same finalized block.

        Notes
        -----
        - A complete bounded OrderFilled log scan is required even when REST says
          zero. The on-chain status conflates full execution and cancellation.
        - BUY fees are outcome-token units; SELL fees are collateral units. The
          existing ledger values outcome-token fees at maximum payout.
        """
        data, exchange = self._context(reference)
        block = self.web3.eth.get_block("finalized")
        end = int(block["number"])
        start = data["chain"]["from_block"]
        if end < start or end - start > _MAX_SCAN_BLOCKS:
            raise ValueError("Finalized fill scan exceeds its bounded window")
        if not exchange.functions.getOrderStatus(data["order_hash"]).call(block_identifier=end)[0]:
            return snapshot
        raw = data["chain"]["order"]
        quantity = collateral = fee = 0
        seen = set()
        for first in range(start, end + 1, _LOG_BLOCKS):
            logs = exchange.events.OrderFilled().get_logs(
                from_block=first, to_block=min(end, first + _LOG_BLOCKS - 1),
                argument_filters={"orderHash": data["order_hash"]},
            )
            for log in logs:
                identity = (Web3.to_hex(log["transactionHash"]), int(log["logIndex"]))
                if identity in seen:
                    continue
                seen.add(identity)
                values = log["args"]
                if (log.get("removed", False) or not first <= int(log["blockNumber"]) <= min(end, first + _LOG_BLOCKS - 1)
                        or log["address"].lower() != exchange.address.lower()
                        or Web3.to_hex(values["orderHash"]).lower() != data["order_hash"].lower()
                        or values["maker"].lower() != raw["maker"].lower()):
                    raise ValueError("Fill log identity mismatch")
                buy = snapshot.side is OrderSide.BUY
                asset = "takerAssetId" if buy else "makerAssetId"
                if (int(values[asset]) != int(raw["tokenId"])
                        or int(values["makerAssetId" if buy else "takerAssetId"]) != 0):
                    raise ValueError("Fill token mismatch")
                quantity += int(values["takerAmountFilled" if buy else "makerAmountFilled"])
                collateral += int(values["makerAmountFilled" if buy else "takerAmountFilled"])
                fee += int(values["fee"])
        filled = Decimal(quantity) / _WEI
        if filled < snapshot.filled_quantity.value or filled > snapshot.quantity.value:
            raise ValueError("Finalized fills conflict with observed order quantity")
        average = Price(Decimal(collateral) / quantity) if quantity else None
        charged = Money(Decimal(fee) / _WEI,
                        Currency("OUTCOME_TOKEN" if snapshot.side is OrderSide.BUY else "USDT"))
        return replace(snapshot,
            status=OrderStatus.FILLED if filled == snapshot.quantity.value else OrderStatus.CANCELLED,
            filled_quantity=Quantity(filled), average_price=average,
            fee=TradingFee(charged, Money(charged.amount, Currency("USD"))),
            may_receive_more_fills=False, settlement_finalized_block=end,
            reason="onchain_finalized", updated_at=Timestamp.now())
