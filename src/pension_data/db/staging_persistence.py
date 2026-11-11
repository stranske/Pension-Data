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
    return _evidence_json(value, reject_nonfinite_decimal=True)


def _evidence_json(value: object, *, reject_nonfinite_decimal: bool = False) -> str:
    """Encode evidence without rounding JSON numbers through binary floats."""
    if reject_nonfinite_decimal and isinstance(value, Decimal) and not value.is_finite():
        raise ValueError(f"non-finite Decimal in evidence_refs: {value}")
    if isinstance(value, int) and not isinstance(value, bool):
        value = Decimal(value)
    elif isinstance(value, float):
        value = Decimal(str(value))
    if isinstance(value, Decimal):
        if not value.is_finite():
            return json.dumps(float(value))
        if not value:
            return "0"
        sign, digits, exponent = value.as_tuple()
        assert isinstance(exponent, int)  # Finite decimals have integer exponents.
        end = len(digits)
        while end > 1 and digits[end - 1] == 0:
            end -= 1
            exponent += 1
        return str(Decimal((sign, digits[:end], exponent)))
    if isinstance(value, Mapping):
        items = []
        for key, item in sorted(value.items()):
            if not isinstance(key, str):
                if key is not None and not isinstance(key, (int, float)):
                    raise TypeError(f"invalid JSON object key: {key!r}")
                key = json.dumps(key)
            encoded = _evidence_json(item, reject_nonfinite_decimal=reject_nonfinite_decimal)
            items.append(f"{json.dumps(key)}: {encoded}")
        return "{" + ", ".join(items) + "}"
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        encoded_items = (
            _evidence_json(item, reject_nonfinite_decimal=reject_nonfinite_decimal)
            for item in value
        )
        return "[" + ", ".join(encoded_items) + "]"
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


def _replacement_id_pattern(fact_id: str) -> str:
    escaped = fact_id.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
    return f"{escaped}@%"


def _replacement_fact_id(row: Mapping[str, object]) -> str:
    canonical_values: dict[str, object] = {}
    for column, value in zip(_STAGING_CORE_METRIC_COLUMNS, _row_values(row), strict=True):
        if column == "fact_id":
            continue
        canonical = _canonical_column_value(column, value)
        if isinstance(canonical, datetime):
            canonical = canonical.isoformat().removesuffix("+00:00") + "Z"
        elif isinstance(canonical, float) and canonical.is_integer():
            canonical = int(canonical)
        canonical_values[column] = canonical
    asserted_at = canonical_values["asserted_at"] or ""
    digest_payload = json.dumps(canonical_values, default=str, sort_keys=True)
    digest = sha1(digest_payload.encode("utf-8")).hexdigest()[:12]
    return f"{row['fact_id']}@{asserted_at}:{digest}"


def _canonical_evidence(value: object) -> object:
    if value is None:
        return None
    if isinstance(value, str):
        try:
            # Top-level arrays are reference strings. Preserve their existing
            # coercion and decode numbers in evidence objects exactly.
            parsed = (
                json.loads(value)
                if value.lstrip().startswith("[")
                else json.loads(value, parse_float=Decimal)
            )
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
        return _evidence_json(_canonical_evidence(value))
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
    # psycopg's default JSON decoder rounds non-integral JSONB numbers to floats.
    evidence_column = "CAST(evidence_refs AS TEXT) AS evidence_refs"
    select_columns = ", ".join(
        evidence_column if column == "evidence_refs" else column
        for column in _STAGING_CORE_METRIC_COLUMNS
    )
    replacement_clause = f"fact_id LIKE {placeholder} ESCAPE '\\'"
    fact_clause = f"(fact_id = {placeholder} OR {replacement_clause})"
    select_replacements_sql = (
        f"SELECT {select_columns} FROM staging_core_metrics WHERE {replacement_clause}"
    )
    select_active_sql = (
        f"SELECT {select_columns} FROM staging_core_metrics WHERE {fact_clause} AND {active_clause} "
        f"ORDER BY CASE WHEN fact_id = {placeholder} THEN 0 ELSE 1 END, asserted_at DESC, fact_id DESC "
        "LIMIT 1"
    )
    update_sql = (
        "UPDATE staging_core_metrics "
        f"SET superseded_at = {placeholder}, restated = {placeholder} "
        f"WHERE {fact_clause} AND {active_clause}"
    )

    inserted = 0
    try:
        for row in rows:
            candidate_values = _row_values(row)
            fact_id = str(row["fact_id"])
            fact_id_pattern = _replacement_id_pattern(fact_id)
            existing = connection.execute(
                select_active_sql,
                (fact_id, fact_id_pattern, fact_id),
            ).fetchone()
            replacement_values = _row_values({**row, "restated": True})
            if existing is not None:
                if _same_assertion(existing, candidate_values):
                    continue
                if existing[0].startswith(f"{fact_id}@") and _same_assertion(
                    existing, (existing[0], *replacement_values[1:])
                ):
                    continue
                if _same_assertion_source(existing, candidate_values):
                    raise ValueError(f"conflicting assertion source for {fact_id!r}")

            replacement_id = _replacement_fact_id(row) if existing is not None else None
            # Check history by assertion source as well as key so the upgrade cannot
            # reinsert an old assertion under a new ID and supersede the current row.
            stored_replacement = next(
                (
                    stored
                    for stored in connection.execute(select_replacements_sql, (fact_id_pattern,))
                    if stored[0].startswith(f"{fact_id}@")
                    and (
                        stored[0] == replacement_id
                        or _same_assertion_source(stored, candidate_values)
                    )
                ),
                None,
            )
            if stored_replacement is not None:
                if _same_assertion(
                    stored_replacement, (stored_replacement[0], *replacement_values[1:])
                ):
                    continue
                raise ValueError(f"conflicting assertion already exists for {fact_id!r}")

            if existing is not None:
                asserted_at_index = _STAGING_CORE_METRIC_COLUMNS.index("asserted_at")
                superseded_at = candidate_values[asserted_at_index]
                connection.execute(update_sql, (superseded_at, True, fact_id, fact_id_pattern))
                replacement_cursor = connection.execute(
                    sql, (replacement_id, *replacement_values[1:])
                )
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
