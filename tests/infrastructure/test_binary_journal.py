"""Verify ordered journal framing, durability cursors, and tail recovery."""

from decimal import Decimal

from prediction_markets.application.events import (
    AccountingCorrectionRecorded,
    InventoryOperationRecorded,
    MarketMatchesUpdated,
)
from prediction_markets.application.codec import decode_event, encode_event
from prediction_markets.application.markets.models import MarketCycle
from prediction_markets.domain.outcome_inventory import (
    InventoryOperationID,
    InventoryOperationReference,
    OutcomeInventoryAction,
    OutcomeInventoryBalance,
    OutcomeInventoryIntent,
    PreparedInventoryOperation,
)
from prediction_markets.domain.market_matching.value_objects import Underlying
from prediction_markets.application.execution.accounting import apply_trade
from prediction_markets.domain.shared.value_objects import (
    ContractID,
    Currency,
    MarketID,
    Money,
    PortfolioID,
    Price,
    Quantity,
    Timestamp,
    TradeID,
    VenueID,
)
from prediction_markets.domain.trading.entities import AccountingCorrection, Trade
from prediction_markets.domain.trading.enums import OrderSide
from prediction_markets.infrastructure.binary_journal import BinaryJournal


def test_journal_recovers_only_an_incomplete_tail(tmp_path) -> None:
    """Keep complete typed frames and discard bytes after the last full frame."""
    path = tmp_path / "events.log"
    event = MarketMatchesUpdated(MarketCycle(Underlying("BTC"), 300), ())
    journal = BinaryJournal(path)

    entry = journal.append(event)
    assert entry.sequence == 1
    assert journal.durable_sequence == 0
    assert journal.sync() == 1
    journal.close()

    with path.open("ab") as stream:
        stream.write(b"partial-frame")

    recovered = BinaryJournal(path)
    assert recovered.entries() == (entry,)
    assert recovered.last_sequence == 1
    recovered.close()


def test_journal_round_trips_prepared_inventory_operation(tmp_path) -> None:
    """Keep the exact split request recoverable before venue submission."""
    operation_id = InventoryOperationID("split-1")
    intent = OutcomeInventoryIntent(
        operation_id,
        VenueID("venue"),
        MarketID("market"),
        OutcomeInventoryAction.SPLIT,
        Quantity(Decimal("1")),
    )
    reference = InventoryOperationReference(
        intent.venue_id,
        operation_id,
        b"recovery",
        intent.quantity,
        balance_before=OutcomeInventoryBalance(
            intent.venue_id,
            intent.market_id,
            Quantity(Decimal("0")),
            Quantity(Decimal("0")),
            Money(Decimal("10"), Currency("USD")),
            Timestamp.now(),
        ),
    )
    event = InventoryOperationRecorded(
        PreparedInventoryOperation(intent, reference, b"request"),
    )
    path = tmp_path / "inventory.log"
    journal = BinaryJournal(path)

    journal.append(event)
    journal.close()

    recovered = BinaryJournal(path)
    assert recovered.entries()[0].event == event
    recovered.close()


def test_codec_round_trips_accounting_correction() -> None:
    original = Trade(
        id=TradeID("trade-1"),
        contract_id=ContractID("contract-1"),
        venue_id=VenueID("venue-1"),
        side=OrderSide.BUY,
        quantity=Quantity(Decimal("2")),
        price=Price(Decimal("0.4")),
        executed_at=Timestamp.now(),
        portfolio_id=PortfolioID("bot"),
    )
    replacement = Trade(
        id=original.id,
        contract_id=original.contract_id,
        venue_id=original.venue_id,
        side=original.side,
        quantity=original.quantity,
        price=Price(Decimal("0.45")),
        executed_at=original.executed_at,
        portfolio_id=original.portfolio_id,
    )
    correction = AccountingCorrection(
        id="correction-1",
        target_trade_id=original.id,
        original_trade=original,
        replacement_trade=replacement,
        resulting_position=apply_trade(None, replacement).position,
        reason="Correct venue price",
        recorded_at=Timestamp.now(),
    )
    event = AccountingCorrectionRecorded(correction)

    assert decode_event(encode_event(event)) == event


def test_journal_preserves_frames_after_sequence_nine(tmp_path) -> None:
    """Keep Windows text translation from corrupting sequence ten."""
    path = tmp_path / "events.log"
    event = MarketMatchesUpdated(MarketCycle(Underlying("BTC"), 300), ())
    journal = BinaryJournal(path)

    for _ in range(12):
        journal.append(event)
    journal.close()

    recovered = BinaryJournal(path)
    assert [entry.sequence for entry in recovered.entries()] == list(range(1, 13))
    recovered.close()


def test_codec_reads_market_cycles_written_before_application_reorganization() -> None:
    """Keep existing binary journals valid after moving application models."""
    event = MarketMatchesUpdated(MarketCycle(Underlying("BTC"), 300), ())
    payload = encode_event(event).replace(
        b"prediction_markets.application.markets.models.MarketCycle",
        b"prediction_markets.application.models.MarketCycle",
    )

    assert decode_event(payload) == event


def test_journal_rotates_durable_segments_and_reopens_after_retention(tmp_path) -> None:
    """Treat retained segments as one stream even when sequence one was deleted."""
    path = tmp_path / "events.log"
    event = MarketMatchesUpdated(MarketCycle(Underlying("BTC"), 300), ())
    journal = BinaryJournal(path, segment_size_bytes=1)

    journal.append(event)
    journal.sync()
    journal.append(event)

    eligible = journal.delete_closed_segments_through(1)
    assert eligible == (path,)
    assert path.exists()
    assert journal.delete_closed_segments_through(1, dry_run=False) == (path,)
    assert not path.exists()
    journal.close()

    recovered = BinaryJournal(path, segment_size_bytes=1)
    assert recovered.first_sequence == 2
    assert [entry.sequence for entry in recovered.entries()] == [2]
    recovered.close()
