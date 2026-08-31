"""Coercing raw Public Plans Database cells into numbers a model can use.

Every function here answers "what number is this, if any" for a cell typed by a human into a
public pension filing. None of them raise: a wrong answer becomes a plan's asset allocation, its
funded ratio, or its classification, and the analysis downstream cannot tell.

The `bool` rejection in `_coerce_float` is the one worth reading twice. `isinstance(True, int)` is
True in Python, so without an explicit guard a boolean cell silently becomes 1.0 — a plan with a
flag where a number belongs would report a one-dollar liability.
"""

from __future__ import annotations

import pytest

from pension_data.sources.ppd.mapping import (
    _PLAN_TYPE_LABELS,
    _allocation_percent,
    _coerce_float,
    derive_plan_type,
    variable_name,
)

# ---------------------------------------------------------------------------------------------
# Numeric coercion.
# ---------------------------------------------------------------------------------------------


@pytest.mark.parametrize("value, expected", [(3, 3.0), (3.5, 3.5), (-2, -2.0), (0, 0.0)])
def test_real_numbers_pass_through_as_floats(value, expected):
    assert _coerce_float(value) == expected


def test_a_boolean_is_not_a_number():
    """`isinstance(True, int)` is True in Python.

    Without the explicit guard a boolean cell becomes 1.0, and a plan with a flag where a dollar
    amount belongs reports a one-dollar liability rather than a missing one.
    """
    assert _coerce_float(True) is None
    assert _coerce_float(False) is None
    # WHERE THE GUARD ACTUALLY LIVES, established by break demo rather than by reading. Removing
    # the `isinstance(value, bool)` line in `_coerce_float` changes nothing, because
    # `finite_guards.is_finite_number` excludes bool too — the local check is defence in depth,
    # not the load-bearing one. Only removing BOTH makes this assertion fail. Recorded so the
    # next reader does not conclude the local guard is dead code and delete it: it would then be
    # the sole remaining guard's job, with no test naming that dependency.


@pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf")])
def test_non_finite_numbers_are_refused(value):
    """NaN propagates through every later arithmetic step and poisons an entire aggregate; inf
    does the same to any ratio. Neither is a measurement."""
    assert _coerce_float(value) is None


def test_thousands_separators_are_stripped():
    """Public filings are typed by people. "1,234.5" is a number in every one of them."""
    assert _coerce_float("1,234.5") == 1234.5


def test_surrounding_whitespace_is_ignored():
    assert _coerce_float("  42  ") == 42.0


@pytest.mark.parametrize("sentinel", ["na", "NA", "n/a", "N/A", "null", "none", ".", ""])
def test_every_missing_data_sentinel_becomes_none(sentinel):
    """These are how the PPD writes "we do not have this".

    Coercing any of them to 0.0 would report a plan holding nothing, which is a very different
    claim from not knowing what it holds — and it averages into peer statistics as a real zero.
    """
    assert _coerce_float(sentinel) is None


@pytest.mark.parametrize("value", ["abc", [1], {"a": 1}, object()])
def test_values_that_are_not_numbers_are_none(value):
    assert _coerce_float(value) is None


def test_none_stays_none():
    assert _coerce_float(None) is None


# ---------------------------------------------------------------------------------------------
# Allocation weights: fractions and percentages in the same column.
# ---------------------------------------------------------------------------------------------


def test_a_fraction_is_scaled_to_a_percentage():
    """Filings mix 0.35 and 35 for the same thing; both must land on 35%."""
    assert _allocation_percent(0.35) == pytest.approx(35.0)


def test_a_percentage_is_left_alone():
    assert _allocation_percent(42) == pytest.approx(42.0)


def test_a_negative_allocation_is_refused():
    """A negative weight is not a short position here, it is a bad cell — and clamping it to zero
    would report a real allocation of nothing."""
    assert _allocation_percent(-1) is None


def test_zero_is_a_real_allocation_and_survives():
    """Zero means the plan holds none of this asset class, which is a measurement. Returning None
    would make it indistinguishable from a class the filing never mentioned."""
    assert _allocation_percent(0) == 0.0


def test_the_boundary_at_one_is_read_as_a_fraction():
    """DOCUMENTED AMBIGUITY, pinned so a change to it is deliberate.

    `<= 1.0` scales up, so a cell of exactly `1` becomes 100%, not 1%. That is the right call for
    a column where fractions dominate — but it means a genuine one-percent allocation written as
    `1` is misread, and anyone revisiting this boundary should know the test is asserting a
    trade-off rather than an obvious truth.
    """
    assert _allocation_percent(1) == pytest.approx(100.0)
    assert _allocation_percent(1.0001) == pytest.approx(1.0001)


def test_none_allocation_stays_none():
    assert _allocation_percent(None) is None


# ---------------------------------------------------------------------------------------------
# Plan-type classification, from a coded number or free text.
# ---------------------------------------------------------------------------------------------


def _record(**fields):
    return {variable_name(slug): value for slug, value in fields.items()}


@pytest.mark.parametrize("code, expected", sorted(_PLAN_TYPE_LABELS.items()))
def test_every_coded_plan_type_maps_to_its_label(code, expected):
    """The code table is the contract. A code that falls through to "unknown" silently drops a
    whole class of plans out of every peer comparison."""
    assert derive_plan_type(_record(plan_type=int(code))) == expected


def test_an_unmapped_code_is_unknown_rather_than_a_guess():
    assert derive_plan_type(_record(plan_type=99)) == "unknown"


@pytest.mark.parametrize(
    "text, expected",
    [
        ("Teachers Retirement System", "teacher"),
        ("TEACHER", "teacher"),
        ("Police and Fire", "safety"),
        ("Firefighters Pension", "safety"),
        ("Public Safety Officers", "safety"),
        ("General Employees", "general"),
        ("State Employees Retirement", "general"),
        ("Something Else Entirely", "unknown"),
    ],
)
def test_free_text_is_classified_when_there_is_no_code(text, expected):
    """Not every filing carries the coded variable, and the text form is what remains."""
    assert derive_plan_type(_record(plan_type=text)) == expected


def test_a_record_with_no_plan_type_is_unknown():
    assert derive_plan_type({}) == "unknown"


def test_a_non_integer_code_falls_through_to_the_text_rules():
    """A code of 2.5 is not a category. Rounding it would assign the plan to a real class on the
    strength of a malformed cell."""
    assert derive_plan_type(_record(plan_type=2.5)) == "unknown"
