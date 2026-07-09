"""#637: table-row extraction selects the most-recent period, never a year token."""

from __future__ import annotations

from pension_data.parser.pdf_pipeline import _extract_table_rows


def _value(line: str) -> str:
    rows = _extract_table_rows(page_number=1, lines=[line])
    assert rows, f"no row extracted from {line!r}"
    return rows[0]["value"]


def test_two_value_columns_select_most_recent() -> None:
    # prior|current year values, no header row -> take the rightmost (most recent).
    assert _value("Funded Ratio | 78.4% | 81.2%") == "81.2%"


def test_year_header_columns_are_not_returned_as_values() -> None:
    assert _value("Funded Ratio | 2023 | 2024 | 78.4% | 81.2%") == "81.2%"


def test_single_value_column_is_unchanged() -> None:
    assert _value("Funded Ratio | 78.4%") == "78.4%"
