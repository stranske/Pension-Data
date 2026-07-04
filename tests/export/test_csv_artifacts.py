"""Tests for shared CSV artifact writing."""

from __future__ import annotations

import csv
from pathlib import Path

from pension_data.export.csv_artifacts import write_csv_artifact


def _read_rows(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def test_write_csv_artifact_serializes_complex_cells(tmp_path: Path) -> None:
    output = tmp_path / "serialized.csv"

    write_csv_artifact(
        output,
        rows=[{"plan_id": "p1", "metadata": {"b": 2, "a": 1}, "notes": None}],
        fieldnames=("plan_id", "metadata", "notes"),
        serialize_complex_cells=True,
    )

    assert _read_rows(output) == [{"plan_id": "p1", "metadata": '{"a": 1, "b": 2}', "notes": ""}]


def test_write_csv_artifact_preserves_complex_cells_verbatim(tmp_path: Path) -> None:
    output = tmp_path / "verbatim.csv"

    write_csv_artifact(
        output,
        rows=[{"plan_id": "p1", "metadata": {"b": 2, "a": 1}}],
        fieldnames=("plan_id", "metadata", "missing"),
        serialize_complex_cells=False,
    )

    assert _read_rows(output) == [{"plan_id": "p1", "metadata": "{'b': 2, 'a': 1}", "missing": ""}]
