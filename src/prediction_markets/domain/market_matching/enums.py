"""Define the finite states used by the market matching domain.

Responsibilities
----------------
- Provide stable symbolic values for domain decisions.
"""

from enum import StrEnum


class ObservationMethod(StrEnum):
    """Enumerate supported price observation methods."""
    FIRST = "first"
    LAST = "last"
    MEAN = "mean"
    MEDIAN = "median"
    MINIMUM = "minimum"
    MAXIMUM = "maximum"
    TWAP = "twap"


class ComparisonOperator(StrEnum):
    """Enumerate supported reference-price comparisons."""
    GREATER_THAN = "gt"
    GREATER_THAN_OR_EQUAL = "gte"
    LESS_THAN = "lt"
    LESS_THAN_OR_EQUAL = "lte"


class UpDownOutcome(StrEnum):
    """Enumerate the outcomes of an up-or-down market."""
    UP = "up"
    DOWN = "down"
    VOID = "void"
    SPLIT = "split"
