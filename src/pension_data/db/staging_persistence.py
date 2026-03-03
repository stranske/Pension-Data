"""DB persistence helpers for staged extraction facts."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from typing import Any

from pension_data.db.strategy import DatabaseDialect

_STAGING_CORE_METRIC_COLUMNS: tuple[str, ...] = (
    "fact_id",
    "plan_id",
    "plan_period",
    "metric_family",
    "metric_name",
    "as_reported_value",
    "normalized_value",
    "as_reported_unit",
    "normalized_unit",
    "manager_name",
    "fund_name",
    "vehicle_name",
    "relationship_completeness",
    "confidence",
    "evidence_refs",
    "effective_date",
    "ingestion_date",
    "benchmark_version",
    "source_document_id",
)


def _serialize_evidence_refs(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    if isinstance(value, Sequence) and not isinstance(value, (bytes, bytearray)):
        refs = [str(item) for item in value]
        return json.dumps(refs, sort_keys=True)
    return json.dumps(value, sort_keys=True)


def _row_values(row: Mapping[str, object]) -> tuple[object, ...]:
    values: list[object] = []
    for column in _STAGING_CORE_METRIC_COLUMNS:
        raw_value = row.get(column)
        if column == "evidence_refs":
            values.append(_serialize_evidence_refs(raw_value))
        else:
            values.append(raw_value)
    return tuple(values)


def persist_staging_core_metrics(
    connection: Any,
    *,
    dialect: DatabaseDialect,
    rows: Sequence[Mapping[str, object]],
) -> int:
    """Persist staged core metrics rows idempotently."""
    if not rows:
        return 0

    placeholders = ", ".join(
        "%s" if dialect == "postgresql" else "?" for _ in _STAGING_CORE_METRIC_COLUMNS
    )
    columns = ", ".join(_STAGING_CORE_METRIC_COLUMNS)
    update_columns = [column for column in _STAGING_CORE_METRIC_COLUMNS if column != "fact_id"]
    update_clause = ", ".join(f"{column}=excluded.{column}" for column in update_columns)
    sql = (
        f"INSERT INTO staging_core_metrics ({columns}) VALUES ({placeholders}) "
        f"ON CONFLICT (fact_id) DO UPDATE SET {update_clause}"
    )

    for row in rows:
        connection.execute(sql, _row_values(row))
    connection.commit()
    return len(rows)
