"""Exercise mappers behavior in the infrastructure polymarket layer.

Responsibilities
----------------
- Verify mappers contracts, edge cases, and failure handling.
"""

from decimal import Decimal

from prediction_markets.domain.markets.enums import BinaryOutcome
from prediction_markets.infrastructure.venues.polymarket.mappers import clob_book_to_order_book
from prediction_markets.infrastructure.venues.polymarket.mappers import gamma_market_to_contracts
from prediction_markets.infrastructure.venues.polymarket.mappers import gamma_market_to_market
from prediction_markets.infrastructure.venues.polymarket.mappers import parse_polymarket_contract_id


def test_gamma_market_to_contracts_maps_yes_and_no(gamma_market_payload):
    contracts = gamma_market_to_contracts(gamma_market_payload)

    assert len(contracts) == 2
    assert str(contracts[0].id) == "polymarket:0xabc:123"
    assert str(contracts[0].market_id) == "0xabc"
    assert str(contracts[0].outcome_id) == "0xabc:yes:123"
    assert str(contracts[0].venue_id) == "POLYMARKET"
    assert str(contracts[0].tick_size) == "0.01"
    assert str(contracts[0].lot_size) == "0.01"
    assert str(contracts[0].minimum_order_size) == "1.00"


def test_gamma_market_to_market_maps_binary_sides(gamma_market_payload):
    market = gamma_market_to_market(gamma_market_payload)

    assert market is not None
    assert str(market.id) == "0xabc"
    assert market.title == "Will BTC go up?"
    assert market.yes_side.side == BinaryOutcome.YES
    assert market.no_side.side == BinaryOutcome.NO
    assert market.is_active()


def test_clob_book_to_order_book_sorts_levels(gamma_market_payload, clob_book_payload):
    contract = gamma_market_to_contracts(gamma_market_payload)[0]

    order_book = clob_book_to_order_book(clob_book_payload, contract=contract)

    assert order_book.best_bid().price.value == Decimal("0.45")
    assert order_book.best_bid().quantity.value == Decimal("3")
    assert order_book.best_ask().price.value == Decimal("0.55")
    assert order_book.best_ask().quantity.value == Decimal("4")
    assert order_book.mid_price().value == Decimal("0.50")


def test_parse_polymarket_contract_id_supports_current_format():
    condition_id, token_id = parse_polymarket_contract_id(
        gamma_market_to_contracts(
            {
                "conditionId": "0xabc",
                "clobTokenIds": '["123"]',
                "outcomes": '["Yes"]',
            },
        )[0].id,
    )

    assert condition_id == "0xabc"
    assert token_id == "123"
