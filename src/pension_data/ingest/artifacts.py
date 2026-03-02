"""Raw-file ingestion with checksum dedupe and supersession lineage."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, replace
from datetime import UTC, datetime

from pension_data.db.models.artifacts import IngestionRunMetrics, RawArtifactRecord


@dataclass(frozen=True, slots=True)
class RawArtifactIngestionInput:
    """Raw file payload and provenance metadata for one ingestion candidate."""

    plan_id: str
    plan_period: str
    source_url: str
    fetched_at: str
    mime_type: str
    content_bytes: bytes


def _artifact_key(*, plan_id: str, plan_period: str, source_url: str) -> tuple[str, str, str]:
    return (plan_id.strip(), plan_period.strip(), source_url.strip())


def _checksum_sha256(content_bytes: bytes) -> str:
    return hashlib.sha256(content_bytes).hexdigest()


def _artifact_id(*, artifact_key: tuple[str, str, str], checksum: str, fetched_at: str) -> str:
    key_payload = "|".join((*artifact_key, checksum, fetched_at))
    digest = hashlib.sha256(key_payload.encode("utf-8")).hexdigest()[:24]
    return f"artifact:{digest}"


def _normalize_utc_timestamp(value: str) -> str | None:
    stripped = value.strip()
    if not stripped:
        return None
    iso_candidate = f"{stripped[:-1]}+00:00" if stripped.endswith("Z") else stripped
    try:
        parsed = datetime.fromisoformat(iso_candidate)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return None
    return parsed.astimezone(UTC).isoformat().replace("+00:00", "Z")


def ingest_raw_artifacts(
    *,
    existing_records: list[RawArtifactRecord],
    inputs: list[RawArtifactIngestionInput],
) -> tuple[list[RawArtifactRecord], IngestionRunMetrics]:
    """Ingest raw artifacts with deterministic dedupe and supersession behavior."""
    records_by_id = {row.artifact_id: row for row in existing_records}
    active_by_key: dict[tuple[str, str, str], str] = {}
    for row in sorted(
        existing_records, key=lambda current: (current.first_seen_at, current.artifact_id)
    ):
        if row.is_active:
            key = _artifact_key(
                plan_id=row.plan_id,
                plan_period=row.plan_period,
                source_url=row.source_url,
            )
            existing_active = active_by_key.get(key)
            if existing_active is not None and existing_active != row.artifact_id:
                msg = (
                    "existing_records contains multiple active artifacts for key "
                    f"{key[0]}|{key[1]}|{key[2]}"
                )
                raise ValueError(msg)
            active_by_key[key] = row.artifact_id

    new_count = 0
    unchanged_count = 0
    superseded_count = 0
    failed_count = 0

    for item in sorted(
        inputs,
        key=lambda current: (
            current.plan_id,
            current.plan_period,
            current.source_url,
            current.fetched_at,
        ),
    ):
        artifact_key = _artifact_key(
            plan_id=item.plan_id,
            plan_period=item.plan_period,
            source_url=item.source_url,
        )
        fetched_at = _normalize_utc_timestamp(item.fetched_at)
        if not all(artifact_key) or not item.mime_type.strip() or fetched_at is None:
            failed_count += 1
            continue

        checksum = _checksum_sha256(item.content_bytes)
        active_artifact_id = active_by_key.get(artifact_key)
        if active_artifact_id is None:
            artifact_id = _artifact_id(
                artifact_key=artifact_key, checksum=checksum, fetched_at=fetched_at
            )
            records_by_id[artifact_id] = RawArtifactRecord(
                artifact_id=artifact_id,
                plan_id=item.plan_id.strip(),
                plan_period=item.plan_period.strip(),
                source_url=item.source_url.strip(),
                fetched_at=fetched_at,
                mime_type=item.mime_type.strip(),
                byte_size=len(item.content_bytes),
                checksum_sha256=checksum,
                is_active=True,
                supersedes_artifact_id=None,
                superseded_by_artifact_id=None,
                first_seen_at=fetched_at,
                last_seen_at=fetched_at,
            )
            active_by_key[artifact_key] = artifact_id
            new_count += 1
            continue

        active_row = records_by_id[active_artifact_id]
        if active_row.checksum_sha256 == checksum:
            records_by_id[active_artifact_id] = replace(
                active_row,
                last_seen_at=fetched_at,
            )
            unchanged_count += 1
            continue

        replacement_id = _artifact_id(
            artifact_key=artifact_key, checksum=checksum, fetched_at=fetched_at
        )
        records_by_id[active_artifact_id] = replace(
            active_row,
            is_active=False,
            superseded_by_artifact_id=replacement_id,
            last_seen_at=fetched_at,
        )
        records_by_id[replacement_id] = RawArtifactRecord(
            artifact_id=replacement_id,
            plan_id=item.plan_id.strip(),
            plan_period=item.plan_period.strip(),
            source_url=item.source_url.strip(),
            fetched_at=fetched_at,
            mime_type=item.mime_type.strip(),
            byte_size=len(item.content_bytes),
            checksum_sha256=checksum,
            is_active=True,
            supersedes_artifact_id=active_artifact_id,
            superseded_by_artifact_id=None,
            first_seen_at=fetched_at,
            last_seen_at=fetched_at,
        )
        active_by_key[artifact_key] = replacement_id
        superseded_count += 1

    records = sorted(
        records_by_id.values(),
        key=lambda row: (
            row.plan_id,
            row.plan_period,
            row.source_url,
            row.first_seen_at,
            row.artifact_id,
        ),
    )
    return (
        records,
        IngestionRunMetrics(
            new_count=new_count,
            unchanged_count=unchanged_count,
            superseded_count=superseded_count,
            failed_count=failed_count,
        ),
    )


def lineage_for_artifact(
    *, records: list[RawArtifactRecord], artifact_id: str
) -> list[RawArtifactRecord]:
    """Return full supersession lineage chain for one artifact id."""
    records_by_id = {row.artifact_id: row for row in records}
    current = records_by_id.get(artifact_id)
    if current is None:
        return []

    # Walk to root artifact, guarding against cycles.
    visited_up: set[str] = {current.artifact_id}
    while current.supersedes_artifact_id is not None:
        parent = records_by_id.get(current.supersedes_artifact_id)
        if parent is None:
            break
        if parent.artifact_id in visited_up:
            break
        visited_up.add(parent.artifact_id)
        current = parent

    lineage: list[RawArtifactRecord] = [current]
    visited_down: set[str] = {current.artifact_id}
    while lineage[-1].superseded_by_artifact_id is not None:
        child_id = lineage[-1].superseded_by_artifact_id or ""
        if child_id in visited_down:
            break
        child = records_by_id.get(child_id)
        if child is None:
            break
        visited_down.add(child.artifact_id)
        lineage.append(child)
    return lineage
