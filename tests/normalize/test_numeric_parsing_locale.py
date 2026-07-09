"""#637: locale-aware numeric parsing + sentence-boundary truncation."""

from __future__ import annotations

import pytest

from pension_data.normalize.numeric_parsing import (
    is_year_like_token,
    parse_numeric_token,
    truncate_at_sentence_boundary,
)


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        # US convention
        ("1,234.56", 1234.56),
        ("78.4%", 78.4),
        ("1,000,000", 1_000_000.0),
        # European convention (the #637 cases)
        ("1.234,56 million", 1234.56),
        ("81,2%", 81.2),
        ("325.000", 325_000.0),
        ("1.234.567", 1_234_567.0),
        # plain
        ("42", 42.0),
        ("-3.5", -3.5),
    ],
)
def test_parse_numeric_token_handles_us_and_european(text: str, expected: float) -> None:
    assert parse_numeric_token(text) == pytest.approx(expected)


def test_three_decimal_value_is_not_treated_as_thousands() -> None:
    # Guard: only a trailing ".000" group collapses to thousands; a real 3-decimal stays.
    assert parse_numeric_token("7.125") == pytest.approx(7.125)


def test_year_like_tokens_are_skipped() -> None:
    assert parse_numeric_token("FY 2024 funded ratio 81.2") == pytest.approx(81.2)
    assert is_year_like_token("2024") is True
    assert is_year_like_token("325") is False


def test_truncate_at_sentence_boundary() -> None:
    assert truncate_at_sentence_boundary(" not disclosed. AAL was $410.5 million.") == (
        " not disclosed"
    )
    # a period inside a number is not a boundary
    assert truncate_at_sentence_boundary(" was 78.4% this year") == " was 78.4% this year"
    # trailing terminator at end-of-string is a boundary
    assert truncate_at_sentence_boundary(" 0.812.") == " 0.812"
