"""Immutable raw artifact models with supersession lineage metadata."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class RawArtifactRecord:
    """Immutable ingested artifact row with supersession linkage."""

    artifact_id: str
    plan_id: str
    plan_period: str
    source_url: str
    fetched_at: str
    mime_type: str
    byte_size: int
    checksum_sha256: str
    is_active: bool
    supersedes_artifact_id: str | None
    superseded_by_artifact_id: str | None
    first_seen_at: str
    last_seen_at: str


@dataclass(frozen=True, slots=True)
class IngestionRunMetrics:
    """Per-run ingestion outcome counters."""

    new_count: int
    unchanged_count: int
    superseded_count: int
    failed_count: int
