"""Verify residual reconstruction against actual corrective fills."""

from decimal import Decimal

import pytest

from repo_tools.reconstruct_trade import _checks, _remaining_exposure


def _evidence(side, fills, corrections, residual="0", status="recovered"):
    journal = {"status": status, "residual_quantity": residual}
    trades = []
    for index, quantity in enumerate(fills, 1):
        leg = f"leg{index}"
        journal.update({
            f"{leg}_venue_id": f"venue-{index}",
            f"{leg}_contract_id": f"contract-{index}",
            f"{leg}_client_order_id": leg,
            f"{leg}_filled_quantity": quantity,
            f"{leg}_side": side,
        })
        if Decimal(quantity):
            trades.append(_trade(leg, index, side, quantity))
    trades.extend(_trade(*correction) for correction in corrections)
    return {
        "journal": journal, "trades": trades, "commands": [],
        "projected": [], "checkpoint": None,
    }


def _trade(client, leg, side, quantity):
    return {
        "trade_id": f"fill-{client}", "client_order_id": client,
        "venue_id": f"venue-{leg}", "contract_id": f"contract-{leg}",
        "side": side, "quantity": quantity, "price": "0.5",
        "fee_settlement_amount": "0", "fee_settlement_currency": "USD",
    }


@pytest.mark.parametrize(
    ("side", "fills", "corrections", "remaining", "status"),
    [
        ("buy", ("10", "0"), [("recovery-1", 2, "buy", "10")], "0", "recovered"),
        ("sell", ("5", "0"), [("recovery-1", 1, "buy", "5")], "0", "recovered"),
        ("buy", ("0", "9"), [("manual-resolution:x", 2, "sell", "9")], "0", "completed"),
        ("sell", ("0", "5"), [("manual-resolution:x", 2, "buy", "5")], "0", "completed"),
        ("buy", ("10", "3"), [("recovery-1", 2, "buy", "2"),
                                 ("manual-resolution:x", 1, "sell", "5")], "0", "completed"),
        ("buy", ("10", "0"), [("recovery-1", 2, "buy", "4")], "6", "needs_review"),
        ("buy", ("10", "0"), [("recovery-1", 1, "sell", "12")], "2", "needs_review"),
        ("buy", ("5", "5"), [], "0", "completed"),
    ],
)
def test_residual_includes_only_actual_corrective_fills(side, fills, corrections, remaining, status):
    """Cover completion, unwind, manual closure, partial recovery and overshoot."""
    data = _evidence(side, fills, corrections, remaining, status)
    assert _remaining_exposure(data) == Decimal(remaining)
    issues = _checks(data, [], {"error": None, "first_sequence": None})
    assert not [issue for issue in issues if issue[0] == "critical"]


def test_terminal_label_does_not_hide_an_unresolved_or_unverifiable_residual():
    """Continue detecting false closure and never count an unrelated contract."""
    data = _evidence("buy", ("10", "0"), [("recovery-1", 2, "buy", "4")])
    issues = _checks(data, [], {"error": None, "first_sequence": None})
    assert any(title == "Residual inconsistente" for _, title, _ in issues)
    data["trades"][-1]["contract_id"] = "unrelated-contract"
    issues = _checks(data, [], {"error": None, "first_sequence": None})
    assert any(title == "Residual not verifiable" for _, title, _ in issues)
    assert not any(title == "Residual inconsistente" for _, title, _ in issues)
