"""Exercise limitless mappers behavior in the infrastructure limitless layer.

Responsibilities
----------------
- Verify limitless mappers contracts, edge cases, and failure handling.
"""

from decimal import Decimal

import pytest

from prediction_markets.domain.shared.value_objects import ContractID
from prediction_markets.infrastructure.venues.limitless.mappers import limitless_market_to_contracts
from prediction_markets.infrastructure.venues.limitless.mappers import limitless_market_to_market
from prediction_markets.infrastructure.venues.limitless.mappers import limitless_orderbook_to_order_book
from prediction_markets.infrastructure.venues.limitless.mappers import parse_limitless_contract_id


def test_limitless_market_to_contracts_maps_yes_and_no(limitless_market_payload):
    contracts = limitless_market_to_contracts(limitless_market_payload)

    assert len(contracts) == 2
    assert str(contracts[0].id) == "limitless:btc-up-or-down-hourly-123:yes"
    assert str(contracts[0].market_id) == "btc-up-or-down-hourly-123"
    assert str(contracts[0].outcome_id) == "btc-up-or-down-hourly-123:yes:111"
    assert str(contracts[0].venue_id) == "LIMITLESS"
    assert str(contracts[0].payout_currency) == "USDC"
    assert str(contracts[0].tick_size) == "0.001"
    assert str(contracts[0].lot_size) == "0.000001"
    assert contracts[0].minimum_order_size is None
    assert str(contracts[1].id) == "limitless:btc-up-or-down-hourly-123:no"
    assert contracts[1].symbol == "222"


def test_limitless_market_to_contracts_skips_payload_without_complete_tokens(
    limitless_market_payload,
):
    market = {**limitless_market_payload, "tokens": {"yes": "111"}}

    assert limitless_market_to_contracts(market) == ()


def test_limitless_market_to_market_maps_maturity(limitless_market_payload):
    market = limitless_market_to_market(limitless_market_payload)

    assert market is not None
    assert str(market.state.start_time) == "2026-07-14T16:00:00+00:00"
    assert str(market.state.close_time) == "2026-07-14T17:00:00+00:00"


def test_limitless_orderbook_to_order_book_maps_yes_side(
    limitless_market_payload,
    limitless_orderbook_payload,
):
    contract = limitless_market_to_contracts(limitless_market_payload)[0]

    order_book = limitless_orderbook_to_order_book(
        limitless_orderbook_payload,
        contract=contract,
    )

    assert str(order_book.market_id) == "btc-up-or-down-hourly-123"
    assert str(order_book.outcome_id) == "btc-up-or-down-hourly-123:yes:111"
    assert order_book.best_bid().price.value == Decimal("0.45")
    assert order_book.best_bid().quantity.value == Decimal("3")
    assert order_book.best_ask().price.value == Decimal("0.55")
    assert order_book.best_ask().quantity.value == Decimal("4")


def test_limitless_orderbook_to_order_book_maps_no_side_as_complement(
    limitless_market_payload,
    limitless_orderbook_payload,
):
    contract = limitless_market_to_contracts(limitless_market_payload)[1]

    order_book = limitless_orderbook_to_order_book(
        limitless_orderbook_payload,
        contract=contract,
    )

    assert str(order_book.outcome_id) == "btc-up-or-down-hourly-123:no:222"
    assert order_book.best_bid().price.value == Decimal("0.45")
    assert order_book.best_bid().quantity.value == Decimal("4")
    assert order_book.best_ask().price.value == Decimal("0.55")
    assert order_book.best_ask().quantity.value == Decimal("3")


def test_parse_limitless_contract_id_rejects_invalid_format():
    with pytest.raises(ValueError, match="Invalid Limitless contract ID"):
        parse_limitless_contract_id(ContractID("btc-up-or-down-hourly-123"))
