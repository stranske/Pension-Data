"""Shared normalization rules for ratio and percentage values.

Different source formats express the same measurement as either a fraction or a
percentage.  Keeping the thresholds here makes those conventions explicit and
prevents ingestion paths from silently applying different unit conversions.
"""

from __future__ import annotations

from pension_data.finite_guards import require_finite

RATIO_PERCENT_THRESHOLD = 1.0
"""Values above this generic threshold are percentage-form ratios."""

PPD_FUNDED_RATIO_PERCENT_THRESHOLD = 3.0
"""PPD funded-ratio values above this threshold are expressed as percentages."""

ALLOCATION_FRACTION_MAX = 1.0
"""PPD allocation values through this threshold are expressed as fractions."""


def to_ratio(
    value: float | None,
    *,
    percent_threshold: float = RATIO_PERCENT_THRESHOLD,
    explicitly_percent: bool = False,
) -> float | None:
    """Return a finite value in ratio units using a threshold or explicit unit marker."""
    if value is None:
        return None
    value = require_finite(value, field="ratio")
    if explicitly_percent or value > percent_threshold:
        return round(value / 100.0, 9)
    return round(value, 9)


def to_percent(value: float | None) -> float | None:
    """Return a finite non-negative PPD allocation value in percent units."""
    if value is None:
        return None
    value = require_finite(value, field="allocation")
    if value < 0.0:
        return None
    if value <= ALLOCATION_FRACTION_MAX:
        return round(value * 100.0, 9)
    return round(value, 9)


def ppd_funded_ratio_fraction(value: float | None) -> float | None:
    """Normalize PPD's documented funded-ratio convention into a fraction."""
    return to_ratio(value, percent_threshold=PPD_FUNDED_RATIO_PERCENT_THRESHOLD)
