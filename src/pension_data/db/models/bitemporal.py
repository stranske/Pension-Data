"""Bitemporal assertion helpers for additive metric and holdings history."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Sequence
from copy import copy
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol

from pension_data.db.models.core_facts import _parse_iso_temporal


class BitemporalRow(Protocol):
    """Row shape needed for valid-time + assertion-time filtering."""

    valid_from: str
    valid_to: str | None
    asserted_at: str
    superseded_at: str | None


@dataclass(frozen=True, slots=True)
class BitemporalAssertion:
    """Generic bitemporal value assertion for staged metrics or facts."""

    entity_id: str
    fact_name: str
    value: float | str | None
    valid_from: str
    valid_to: str | None
    asserted_at: str
    source_document_id: str
    superseded_at: str | None = None

    @property
    def is_restated(self) -> bool:
        """Whether a later assertion superseded this row."""
        return self.superseded_at is not None


def _within_valid_window(row: BitemporalRow, valid_at: datetime) -> bool:
    valid_from = _parse_iso_temporal(row.valid_from, field_name="valid_from")
    if valid_from > valid_at:
        return False
    if row.valid_to is None:
        return True
    valid_to = _parse_iso_temporal(row.valid_to, field_name="valid_to")
    return valid_at < valid_to


def _known_at(row: BitemporalRow, known_at: datetime) -> bool:
    asserted_at = _parse_iso_temporal(row.asserted_at, field_name="asserted_at")
    if asserted_at > known_at:
        return False
    if row.superseded_at is None:
        return True
    superseded_at = _parse_iso_temporal(row.superseded_at, field_name="superseded_at")
    return known_at < superseded_at


def _copy_with_superseded_at[T: BitemporalRow](row: T, superseded_at: str) -> T:
    updated = copy(row)
    object.__setattr__(updated, "superseded_at", superseded_at)
    return updated


def query_as_known_at[T: BitemporalRow, K: object](
    rows: Sequence[T],
    *,
    valid_at: str,
    known_at: str,
    key: Callable[[T], K],
) -> list[T]:
    """Return the latest active assertion per key for an as-of/as-known-at view."""
    valid_dt = _parse_iso_temporal(valid_at, field_name="valid_at")
    known_dt = _parse_iso_temporal(known_at, field_name="known_at")

    latest_by_key: dict[K, T] = {}
    for row in rows:
        if not _within_valid_window(row, valid_dt) or not _known_at(row, known_dt):
            continue
        row_key = key(row)
        current = latest_by_key.get(row_key)
        if current is None or _parse_iso_temporal(
            row.asserted_at,
            field_name="asserted_at",
        ) > _parse_iso_temporal(current.asserted_at, field_name="asserted_at"):
            latest_by_key[row_key] = row

    return sorted(latest_by_key.values(), key=lambda row: (str(key(row)), row.asserted_at))


def assert_no_active_valid_overlaps[T: BitemporalRow, K: object](
    rows: Sequence[T],
    *,
    key: Callable[[T], K],
) -> None:
    """Raise when two active rows overlap for the same valid-time key."""
    active_rows = [row for row in rows if row.superseded_at is None]
    rows_by_key: dict[K, list[T]] = {}
    for row in active_rows:
        rows_by_key.setdefault(key(row), []).append(row)

    for row_key, keyed_rows in rows_by_key.items():
        ordered = sorted(
            keyed_rows,
            key=lambda row: _parse_iso_temporal(row.valid_from, field_name="valid_from"),
        )
        previous: T | None = None
        for row in ordered:
            if previous is None:
                previous = row
                continue
            previous_end = (
                _parse_iso_temporal(previous.valid_to, field_name="valid_to")
                if previous.valid_to is not None
                else None
            )
            row_start = _parse_iso_temporal(row.valid_from, field_name="valid_from")
            if previous_end is None or row_start < previous_end:
                raise ValueError(f"active valid-time overlap for {row_key!r}")
            previous = row


def supersede_assertions[T: BitemporalRow, K: object](
    existing_rows: Sequence[T],
    replacement_rows: Iterable[T],
    *,
    key: Callable[[T], K],
    superseded_at: str,
) -> list[T]:
    """Return additive rows where matching active assertions are closed, not overwritten."""
    replacement_list = list(replacement_rows)
    replacement_keys = {key(row) for row in replacement_list}
    updated_existing: list[T] = []
    for row in existing_rows:
        if key(row) in replacement_keys and row.superseded_at is None:
            updated_existing.append(_copy_with_superseded_at(row, superseded_at))
        else:
            updated_existing.append(row)
    return [*updated_existing, *replacement_list]
