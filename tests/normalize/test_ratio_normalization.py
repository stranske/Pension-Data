"""Cross-path contracts for ratio and percent normalization."""

from __future__ import annotations

import pytest

from pension_data.normalize.investment_normalization import normalize_rate_to_ratio
from pension_data.normalize.ratio_normalization import (
    ALLOCATION_FRACTION_MAX,
    PPD_FUNDED_RATIO_PERCENT_THRESHOLD,
    ppd_funded_ratio_fraction,
    to_percent,
    to_ratio,
)
from pension_data.sources.ppd.mapping import _allocation_percent, _as_fraction


@pytest.mark.parametrize(
    ("value", "expected"),
    [(0.75, 0.75), (1.0, 1.0), (1.5, 0.015), (75.0, 0.75)],
)
def test_rate_normalizer_uses_the_shared_ratio_contract(value: float, expected: float) -> None:
    assert normalize_rate_to_ratio(value) == expected
    assert to_ratio(value) == expected


def test_ppd_funded_ratio_and_rate_normalizer_share_documented_thresholds() -> None:
    assert PPD_FUNDED_RATIO_PERCENT_THRESHOLD == 3.0
    assert _as_fraction(2.5) == ppd_funded_ratio_fraction(2.5) == 2.5
    assert _as_fraction(75.0) == ppd_funded_ratio_fraction(75.0) == 0.75
    assert normalize_rate_to_ratio(2.5) == 0.025


def test_ppd_allocation_percent_preserves_documented_fraction_boundary() -> None:
    assert ALLOCATION_FRACTION_MAX == 1.0
    assert _allocation_percent(1.0) == to_percent(1.0) == 100.0
    assert _allocation_percent(1.0001) == to_percent(1.0001) == 1.0001


@pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf")])
def test_shared_helpers_reject_non_finite_values(value: float) -> None:
    with pytest.raises(ValueError):
        to_ratio(value)
    with pytest.raises(ValueError):
        to_percent(value)


def test_shared_helpers_keep_optional_values_optional() -> None:
    assert to_ratio(None) is None
    assert to_percent(None) is None


def test_explicit_negative_percent_is_converted_to_ratio_units() -> None:
    assert to_ratio(-6.8, explicitly_percent=True) == -0.068
