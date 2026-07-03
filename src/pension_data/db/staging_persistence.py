"""DB persistence helpers for staged extraction facts."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from hashlib import sha1
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
    "valid_from",
    "valid_to",
    "asserted_at",
    "superseded_at",
    "restated",
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
        elif column == "valid_from":
            values.append(raw_value or row.get("effective_date"))
        elif column == "asserted_at":
            values.append(raw_value or row.get("ingestion_date"))
        elif column == "restated":
            values.append(bool(raw_value or row.get("superseded_at")))
        else:
            values.append(raw_value)
    return tuple(values)


def _placeholder(dialect: DatabaseDialect) -> str:
    return "%s" if dialect == "postgresql" else "?"


def _replacement_fact_id(row: Mapping[str, object]) -> str:
    asserted_at = row.get("asserted_at") or row.get("ingestion_date") or ""
    digest_payload = json.dumps(
        {key: row.get(key) for key in _STAGING_CORE_METRIC_COLUMNS if key != "fact_id"},
        default=str,
        sort_keys=True,
    )
    digest = sha1(digest_payload.encode("utf-8")).hexdigest()[:12]
    return f"{row['fact_id']}@{asserted_at}:{digest}"


def _same_assertion(existing: Any, candidate_values: tuple[object, ...]) -> bool:
    return all(existing[index] == candidate for index, candidate in enumerate(candidate_values))


def _same_assertion_source(existing: Any, candidate_values: tuple[object, ...]) -> bool:
    asserted_at_index = _STAGING_CORE_METRIC_COLUMNS.index("asserted_at")
    source_document_id_index = _STAGING_CORE_METRIC_COLUMNS.index("source_document_id")
    return bool(
        existing[asserted_at_index] == candidate_values[asserted_at_index]
        and existing[source_document_id_index] == candidate_values[source_document_id_index]
    )


def persist_staging_core_metrics(
    connection: Any,
    *,
    dialect: DatabaseDialect,
    rows: Sequence[Mapping[str, object]],
) -> int:
    """Persist staged core metrics rows idempotently."""
    if not rows:
        return 0

    placeholder = _placeholder(dialect)
    placeholders = ", ".join(placeholder for _ in _STAGING_CORE_METRIC_COLUMNS)
    columns = ", ".join(_STAGING_CORE_METRIC_COLUMNS)
    sql = (
        f"INSERT INTO staging_core_metrics ({columns}) VALUES ({placeholders}) "
        "ON CONFLICT (fact_id) DO NOTHING"
    )
    select_sql = f"SELECT {columns} FROM staging_core_metrics WHERE fact_id = {placeholder}"
    update_sql = (
        "UPDATE staging_core_metrics "
        f"SET superseded_at = {placeholder}, restated = {placeholder} "
        f"WHERE fact_id = {placeholder} AND (superseded_at IS NULL OR TRIM(CAST(superseded_at AS TEXT)) = '')"
    )

    inserted = 0
    for row in rows:
        candidate_values = _row_values(row)
        existing = connection.execute(select_sql, (row["fact_id"],)).fetchone()
        if existing is not None:
            if _same_assertion(existing, candidate_values) or _same_assertion_source(
                existing,
                candidate_values,
            ):
                continue

            replacement = dict(row)
            replacement["fact_id"] = _replacement_fact_id(row)
            replacement["restated"] = True
            replacement_values = _row_values(replacement)
            existing_replacement = connection.execute(
                select_sql,
                (replacement["fact_id"],),
            ).fetchone()
            if existing_replacement is not None:
                if _same_assertion(existing_replacement, replacement_values):
                    continue
                raise ValueError(f"conflicting assertion already exists for {row['fact_id']!r}")

            asserted_at_index = _STAGING_CORE_METRIC_COLUMNS.index("asserted_at")
            superseded_at = candidate_values[asserted_at_index]
            connection.execute(update_sql, (superseded_at, True, row["fact_id"]))
            replacement_cursor = connection.execute(sql, replacement_values)
            inserted += max(replacement_cursor.rowcount, 0)
            continue

        cursor = connection.execute(sql, candidate_values)
        rowcount = max(cursor.rowcount, 0)
        inserted += rowcount
    connection.commit()
    return inserted
