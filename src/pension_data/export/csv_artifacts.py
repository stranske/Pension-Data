"""Shared CSV artifact writing helpers."""

from __future__ import annotations

import csv
import json
from pathlib import Path


def write_csv_artifact(
    path: Path,
    *,
    rows: list[dict[str, object]],
    fieldnames: tuple[str, ...],
    serialize_complex_cells: bool,
) -> None:
    """Write CSV artifacts with explicit complex-cell handling.

    Complex cells are deterministic when serialized as JSON; verbatim mode preserves
    the caller's legacy stringification behavior.
    """
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            serialized_row: dict[str, object] = {}
            for field in fieldnames:
                value = row.get(field)
                if value is None:
                    serialized_row[field] = ""
                elif serialize_complex_cells and isinstance(value, (dict, list)):
                    serialized_row[field] = json.dumps(value, sort_keys=True)
                else:
                    serialized_row[field] = value
            writer.writerow(serialized_row)
