"""Verify Timestamp arithmetic and ordering."""

import operator
from datetime import datetime, timedelta, timezone

import pytest

from prediction_markets.domain.shared.value_objects import Timestamp

INSTANT = datetime(2026, 8, 17, 12, 0, tzinfo=timezone.utc)


def _ts(hour: int = 12) -> Timestamp:
    """Build a timezone-aware timestamp on 2026-08-17."""
    return Timestamp(datetime(2026, 8, 17, hour, tzinfo=timezone.utc))


def test_add_timedelta_returns_later_timestamp_without_mutating() -> None:
    """Shift forward by a duration and leave the original instant unchanged."""
    timestamp = Timestamp(INSTANT)

    result = timestamp + timedelta(hours=4)

    assert result == Timestamp(INSTANT + timedelta(hours=4))
    assert timestamp == Timestamp(INSTANT)


def test_sub_timedelta_returns_earlier_timestamp_without_mutating() -> None:
    """Shift backward by a duration and leave the original instant unchanged."""
    timestamp = Timestamp(INSTANT)

    result = timestamp - timedelta(hours=4)

    assert result == Timestamp(INSTANT - timedelta(hours=4))
    assert timestamp == Timestamp(INSTANT)


def test_ordering_compares_underlying_instants() -> None:
    """Order timestamps by their timezone-aware values."""
    earlier = _ts(11)
    later = _ts(13)
    same = _ts(11)

    assert earlier < later
    assert earlier <= later
    assert earlier <= same
    assert later > earlier
    assert later >= earlier
    assert later >= Timestamp(later.value)
    assert earlier == same
    assert earlier != later


@pytest.mark.parametrize(
    "compare",
    [operator.lt, operator.le, operator.gt, operator.ge, operator.eq],
)
def test_ordering_rejects_non_timestamp(compare) -> None:
    """Refuse mixed-type comparisons instead of falling back to identity."""
    with pytest.raises(TypeError, match="Cannot compare Timestamp and non-Timestamp"):
        compare(_ts(), INSTANT)
