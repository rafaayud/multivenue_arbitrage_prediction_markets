"""Exercise kalshi mappers behavior in the infrastructure kalshi layer.

Responsibilities
----------------
- Verify kalshi mappers contracts, edge cases, and failure handling.
"""

from decimal import Decimal

import pytest

from prediction_markets.domain.shared.value_objects import ContractID
from prediction_markets.infrastructure.venues.kalshi.mappers import kalshi_market_to_contracts
from prediction_markets.infrastructure.venues.kalshi.mappers import kalshi_orderbook_to_order_book
from prediction_markets.infrastructure.venues.kalshi.mappers import parse_kalshi_contract_id


def test_kalshi_market_to_contracts_maps_yes_and_no(kalshi_market_payload):
    contracts = kalshi_market_to_contracts(kalshi_market_payload)

    assert len(contracts) == 2
    assert str(contracts[0].id) == "kalshi:KXBTC-26JAN01-B100000:yes"
    assert str(contracts[0].market_id) == "KXBTC-26JAN01-B100000"
    assert str(contracts[0].outcome_id) == "KXBTC-26JAN01-B100000:yes"
    assert str(contracts[0].venue_id) == "KALSHI"
    assert str(contracts[0].payout_currency) == "USD"
    assert str(contracts[0].tick_size) == "0.01"
    assert str(contracts[0].lot_size) == "1"
    assert str(contracts[0].minimum_order_size) == "1.00"
    assert str(contracts[1].id) == "kalshi:KXBTC-26JAN01-B100000:no"
    assert contracts[1].symbol == "KXBTC-26JAN01-B100000-NO"


def test_kalshi_orderbook_to_order_book_maps_yes_side(
    kalshi_market_payload,
    kalshi_orderbook_payload,
):
    contract = kalshi_market_to_contracts(kalshi_market_payload)[0]

    order_book = kalshi_orderbook_to_order_book(kalshi_orderbook_payload, contract=contract)

    assert str(order_book.market_id) == "KXBTC-26JAN01-B100000"
    assert str(order_book.outcome_id) == "KXBTC-26JAN01-B100000:yes"
    assert order_book.best_bid().price.value == Decimal("0.45")
    assert order_book.best_bid().quantity.value == Decimal("3")
    assert order_book.best_ask().price.value == Decimal("0.58")
    assert order_book.best_ask().quantity.value == Decimal("4")


def test_kalshi_orderbook_to_order_book_maps_no_side(
    kalshi_market_payload,
    kalshi_orderbook_payload,
):
    contract = kalshi_market_to_contracts(kalshi_market_payload)[1]

    order_book = kalshi_orderbook_to_order_book(kalshi_orderbook_payload, contract=contract)

    assert str(order_book.outcome_id) == "KXBTC-26JAN01-B100000:no"
    assert order_book.best_bid().price.value == Decimal("0.42")
    assert order_book.best_bid().quantity.value == Decimal("4")
    assert order_book.best_ask().price.value == Decimal("0.55")
    assert order_book.best_ask().quantity.value == Decimal("3")


def test_kalshi_orderbook_to_order_book_maps_websocket_snapshot(kalshi_market_payload):
    contract = kalshi_market_to_contracts(kalshi_market_payload)[0]
    snapshot = {
        "market_ticker": "KXBTC-26JAN01-B100000",
        "yes_dollars_fp": [["0.4400", "1.00"], ["0.4500", "3.00"]],
        "no_dollars_fp": [["0.4000", "2.00"], ["0.4200", "4.00"]],
    }

    order_book = kalshi_orderbook_to_order_book(snapshot, contract=contract)

    assert order_book.best_bid().price.value == Decimal("0.4500")
    assert order_book.best_bid().quantity.value == Decimal("3.00")
    assert order_book.best_ask().price.value == Decimal("0.5800")
    assert order_book.best_ask().quantity.value == Decimal("4.00")


def test_parse_kalshi_contract_id_supports_current_format(kalshi_market_payload):
    condition_id, token_id = parse_kalshi_contract_id(
        kalshi_market_to_contracts(kalshi_market_payload)[0].id,
    )

    assert condition_id == "KXBTC-26JAN01-B100000"
    assert token_id == "yes"


def test_parse_kalshi_contract_id_supports_legacy_format():
    ticker, outcome = parse_kalshi_contract_id(
        ContractID("KXBTC-26JAN01-B100000-YES.KALSHI"),
    )

    assert ticker == "KXBTC-26JAN01-B100000"
    assert outcome == "yes"


def test_parse_kalshi_contract_id_rejects_invalid_format():
    with pytest.raises(ValueError, match="Invalid Kalshi contract ID"):
        parse_kalshi_contract_id(ContractID("KXBTC"))
