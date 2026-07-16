"""#637: table-row extraction selects the most-recent period, never a year token."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from pension_data.parser.pdf_pipeline import _extract_table_rows

FIXTURE_PATH = Path(__file__).parent / "fixtures" / "table_period_selection_golden.json"


def _value(line: str) -> str:
    rows = _extract_table_rows(page_number=1, lines=[line])
    assert rows, f"no row extracted from {line!r}"
    return rows[0]["value"]


def _cases() -> list[tuple[str, str]]:
    with FIXTURE_PATH.open(encoding="utf-8") as handle:
        fixtures = json.load(handle)
    return [(case["line"], case["expected_value"]) for case in fixtures.values()]


@pytest.mark.parametrize(("line", "expected_value"), _cases())
def test_period_selection_golden_cases(line: str, expected_value: str) -> None:
    assert _value(line) == expected_value
