"""DB persistence helpers for staged extraction facts."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime
from decimal import Decimal
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

_TEMPORAL_COLUMNS: frozenset[str] = frozenset(
    {
        "effective_date",
        "ingestion_date",
        "valid_from",
        "valid_to",
        "asserted_at",
        "superseded_at",
    }
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


def _format_canonical_instant(value: object) -> str:
    canonical = _canonical_instant(value)
    if isinstance(canonical, datetime):
        text = canonical.isoformat()
        return f"{text[:-6]}Z" if text.endswith("+00:00") else text
    return "" if canonical is None else str(canonical)


def _canonical_key_value(value: object) -> object:
    # Match Python numeric equality (40 == 40.0) inside nested evidence
    # without rounding distinct large integers through float conversion.
    # Booleans are not floats here, so they retain JSON boolean identity.
    if isinstance(value, Decimal) and value.is_finite():
        if value == value.to_integral_value():
            return int(value)
        # Strip trailing zeros without context-precision rounding, so
        # Decimal("0.790") and Decimal("0.79") share one key while distinct
        # non-integral decimals (and float spellings) remain distinct.
        sign, digits, raw_exponent = value.as_tuple()
        # Finite decimals always have an int exponent; the Literal
        # exponents ('n'/'N'/'F') only occur for NaN/Infinity, excluded above.
        if not isinstance(raw_exponent, int):
            return value
        exponent = raw_exponent
        trimmed = list(digits)
        while exponent < 0 and trimmed and trimmed[-1] == 0:
            trimmed.pop()
            exponent += 1
        return Decimal((sign, tuple(trimmed), exponent)) if trimmed else 0
    if isinstance(value, float) and value.is_integer():
        return int(value)
    if isinstance(value, Mapping):
        return {key: _canonical_key_value(item) for key, item in value.items()}
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [_canonical_key_value(item) for item in value]
    return value


def _replacement_fact_id(row: Mapping[str, object]) -> str:
    # Replacement identity uses the same canonical contract as _same_assertion:
    # decoded JSON and serialized JSON, Z/offset/naive instants, and omitted
    # valid_from/asserted_at defaults must derive one key, while genuine
    # content changes still derive a different key.
    candidate_values = _row_values(row)
    canonical_values: dict[str, object] = {}
    for column, value in zip(_STAGING_CORE_METRIC_COLUMNS, candidate_values, strict=True):
        if column == "fact_id":
            continue
        canonical = _canonical_column_value(column, value)
        if isinstance(canonical, datetime):
            canonical = _format_canonical_instant(canonical)
        else:
            canonical = _canonical_key_value(canonical)
        canonical_values[column] = canonical
    asserted_at = _format_canonical_instant(
        candidate_values[_STAGING_CORE_METRIC_COLUMNS.index("asserted_at")]
    )
    digest_payload = json.dumps(canonical_values, default=str, sort_keys=True)
    digest = sha1(digest_payload.encode("utf-8")).hexdigest()[:12]
    return f"{row['fact_id']}@{asserted_at}:{digest}"


def _canonical_evidence(value: object) -> object:
    if value is None:
        return None
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return value
        return _canonical_evidence(parsed)
    if isinstance(value, Sequence) and not isinstance(value, (bytes, bytearray)):
        return [str(item) for item in value]
    return value


def _canonical_instant(value: object) -> object:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None or value.tzinfo.utcoffset(value) is None:
            return value.replace(tzinfo=UTC)
        return value.astimezone(UTC)
    if isinstance(value, date):
        return datetime(value.year, value.month, value.day, tzinfo=UTC)
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return None
        # Accept both Z and +00:00 so candidate ISO text matches TIMESTAMPTZ fetches.
        if stripped.endswith(("Z", "z")):
            stripped = f"{stripped[:-1]}+00:00"
        try:
            parsed = datetime.fromisoformat(stripped)
        except ValueError:
            return value.strip()
        return _canonical_instant(parsed)
    return value


def _canonical_column_value(column: str, value: object) -> object:
    if column == "evidence_refs":
        return _canonical_evidence(value)
    if column in _TEMPORAL_COLUMNS:
        return _canonical_instant(value)
    return value


def _same_assertion(existing: Any, candidate_values: tuple[object, ...]) -> bool:
    return all(
        _canonical_column_value(_STAGING_CORE_METRIC_COLUMNS[index], existing[index])
        == _canonical_column_value(_STAGING_CORE_METRIC_COLUMNS[index], candidate)
        for index, candidate in enumerate(candidate_values)
    )


def _same_assertion_source(existing: Any, candidate_values: tuple[object, ...]) -> bool:
    asserted_at_index = _STAGING_CORE_METRIC_COLUMNS.index("asserted_at")
    source_document_id_index = _STAGING_CORE_METRIC_COLUMNS.index("source_document_id")
    return bool(
        _canonical_instant(existing[asserted_at_index])
        == _canonical_instant(candidate_values[asserted_at_index])
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
    active_clause = "(superseded_at IS NULL OR TRIM(CAST(superseded_at AS TEXT)) = '')"
    select_sql = f"SELECT {columns} FROM staging_core_metrics WHERE fact_id = {placeholder}"
    select_active_sql = (
        f"SELECT {columns} FROM staging_core_metrics "
        f"WHERE (fact_id = {placeholder} OR fact_id LIKE {placeholder}) AND {active_clause} "
        f"ORDER BY CASE WHEN fact_id = {placeholder} THEN 0 ELSE 1 END, asserted_at DESC, fact_id DESC "
        "LIMIT 1"
    )
    update_sql = (
        "UPDATE staging_core_metrics "
        f"SET superseded_at = {placeholder}, restated = {placeholder} "
        f"WHERE (fact_id = {placeholder} OR fact_id LIKE {placeholder}) AND {active_clause}"
    )

    inserted = 0
    try:
        for row in rows:
            candidate_values = _row_values(row)
            fact_id = str(row["fact_id"])
            fact_id_pattern = f"{fact_id}@%"
            existing = connection.execute(
                select_active_sql,
                (fact_id, fact_id_pattern, fact_id),
            ).fetchone()
            if existing is not None:
                replacement = dict(row)
                replacement["fact_id"] = _replacement_fact_id(row)
                replacement["restated"] = True
                replacement_values = _row_values(replacement)
                if _same_assertion(existing, candidate_values) or _same_assertion(
                    existing,
                    replacement_values,
                ):
                    continue
                if _same_assertion_source(existing, candidate_values):
                    raise ValueError(f"conflicting assertion source for {fact_id!r}")

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
                connection.execute(update_sql, (superseded_at, True, fact_id, fact_id_pattern))
                replacement_cursor = connection.execute(sql, replacement_values)
                inserted += max(replacement_cursor.rowcount, 0)
                continue

            cursor = connection.execute(sql, candidate_values)
            rowcount = max(cursor.rowcount, 0)
            inserted += rowcount
    except Exception:
        connection.rollback()
        raise
    connection.commit()
    return inserted
