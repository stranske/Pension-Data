"""Tests for immutable raw artifact ingestion and supersession lineage."""

from __future__ import annotations

import pytest

from pension_data.db.models.artifacts import RawArtifactRecord
from pension_data.ingest.artifacts import (
    RawArtifactIngestionInput,
    ingest_raw_artifacts,
    lineage_for_artifact,
)


def _input(
    *, fetched_at: str, content: bytes, source_url: str = "https://example.org/ca-2024.pdf"
) -> RawArtifactIngestionInput:
    return RawArtifactIngestionInput(
        plan_id="CA-PERS",
        plan_period="FY2024",
        source_url=source_url,
        fetched_at=fetched_at,
        mime_type="application/pdf",
        content_bytes=content,
    )


def test_reingesting_same_content_does_not_create_duplicate_active_artifacts() -> None:
    first_records, first_metrics = ingest_raw_artifacts(
        existing_records=[],
        inputs=[_input(fetched_at="2026-03-02T00:00:00Z", content=b"same-bytes")],
    )
    assert first_metrics.new_count == 1
    assert len(first_records) == 1
    assert first_records[0].is_active

    second_records, second_metrics = ingest_raw_artifacts(
        existing_records=first_records,
        inputs=[_input(fetched_at="2026-03-03T00:00:00Z", content=b"same-bytes")],
    )
    assert second_metrics.new_count == 0
    assert second_metrics.unchanged_count == 1
    assert len(second_records) == 1
    assert second_records[0].is_active
    assert second_records[0].last_seen_at == "2026-03-03T00:00:00Z"


def test_revised_content_creates_supersession_linked_artifact() -> None:
    baseline, _metrics = ingest_raw_artifacts(
        existing_records=[],
        inputs=[_input(fetched_at="2026-03-02T00:00:00Z", content=b"v1")],
    )
    revised_records, revised_metrics = ingest_raw_artifacts(
        existing_records=baseline,
        inputs=[_input(fetched_at="2026-03-05T00:00:00Z", content=b"v2")],
    )
    assert revised_metrics.superseded_count == 1
    assert len(revised_records) == 2

    inactive = [row for row in revised_records if not row.is_active][0]
    active = [row for row in revised_records if row.is_active][0]
    assert active.supersedes_artifact_id == inactive.artifact_id
    assert inactive.superseded_by_artifact_id == active.artifact_id
    assert inactive.byte_size == 2
    assert active.byte_size == 2


def test_ingestion_records_keep_required_provenance_metadata() -> None:
    records, _metrics = ingest_raw_artifacts(
        existing_records=[],
        inputs=[_input(fetched_at="2026-03-02T00:00:00Z", content=b"payload")],
    )
    row = records[0]
    assert row.source_url == "https://example.org/ca-2024.pdf"
    assert row.fetched_at == "2026-03-02T00:00:00Z"
    assert row.mime_type == "application/pdf"
    assert row.byte_size == len(b"payload")
    assert len(row.checksum_sha256) == 64


def test_failed_inputs_are_counted_and_skipped() -> None:
    records, metrics = ingest_raw_artifacts(
        existing_records=[],
        inputs=[
            _input(fetched_at="2026-03-02T00:00:00Z", content=b"ok"),
            RawArtifactIngestionInput(
                plan_id="CA-PERS",
                plan_period="FY2024",
                source_url="",
                fetched_at="2026-03-02T00:00:01Z",
                mime_type="application/pdf",
                content_bytes=b"bad",
            ),
            RawArtifactIngestionInput(
                plan_id="CA-PERS",
                plan_period="FY2024",
                source_url="https://example.org/ca-2024.pdf",
                fetched_at="   ",
                mime_type="application/pdf",
                content_bytes=b"bad",
            ),
        ],
    )
    assert metrics.new_count == 1
    assert metrics.failed_count == 2
    assert len(records) == 1


def test_invalid_iso8601_timestamp_inputs_are_counted_and_skipped() -> None:
    records, metrics = ingest_raw_artifacts(
        existing_records=[],
        inputs=[
            _input(fetched_at="2026-03-02T00:00:00Z", content=b"ok"),
            _input(fetched_at="2026-03-02T00:00:00", content=b"naive"),
            _input(fetched_at="not-a-timestamp", content=b"invalid"),
        ],
    )
    assert metrics.new_count == 1
    assert metrics.failed_count == 2
    assert len(records) == 1


def test_existing_records_with_multiple_active_rows_for_same_key_raise() -> None:
    first_records, _ = ingest_raw_artifacts(
        existing_records=[],
        inputs=[_input(fetched_at="2026-03-02T00:00:00Z", content=b"v1")],
    )
    duplicate_active = RawArtifactRecord(
        artifact_id="artifact:duplicate",
        plan_id="CA-PERS",
        plan_period="FY2024",
        source_url="https://example.org/ca-2024.pdf",
        fetched_at="2026-03-03T00:00:00Z",
        mime_type="application/pdf",
        byte_size=2,
        checksum_sha256="f" * 64,
        is_active=True,
        supersedes_artifact_id=None,
        superseded_by_artifact_id=None,
        first_seen_at="2026-03-03T00:00:00Z",
        last_seen_at="2026-03-03T00:00:00Z",
    )
    with pytest.raises(
        ValueError,
        match="multiple active artifacts for key CA-PERS\\|FY2024\\|https://example.org/ca-2024.pdf",
    ):
        ingest_raw_artifacts(
            existing_records=[first_records[0], duplicate_active],
            inputs=[_input(fetched_at="2026-03-04T00:00:00Z", content=b"v2")],
        )


def test_lineage_query_returns_supersession_chain_for_audit() -> None:
    first_records, _ = ingest_raw_artifacts(
        existing_records=[],
        inputs=[_input(fetched_at="2026-03-02T00:00:00Z", content=b"v1")],
    )
    second_records, _ = ingest_raw_artifacts(
        existing_records=first_records,
        inputs=[_input(fetched_at="2026-03-03T00:00:00Z", content=b"v2")],
    )
    active_artifact_id = [row.artifact_id for row in second_records if row.is_active][0]
    lineage = lineage_for_artifact(records=second_records, artifact_id=active_artifact_id)

    assert len(lineage) == 2
    assert lineage[0].is_active is False
    assert lineage[1].is_active is True
    assert lineage[0].superseded_by_artifact_id == lineage[1].artifact_id


def test_lineage_query_stops_when_supersession_cycle_is_present() -> None:
    first = RawArtifactRecord(
        artifact_id="artifact:a",
        plan_id="CA-PERS",
        plan_period="FY2024",
        source_url="https://example.org/ca.pdf",
        fetched_at="2026-03-01T00:00:00Z",
        mime_type="application/pdf",
        byte_size=1,
        checksum_sha256="a" * 64,
        is_active=False,
        supersedes_artifact_id="artifact:b",
        superseded_by_artifact_id="artifact:b",
        first_seen_at="2026-03-01T00:00:00Z",
        last_seen_at="2026-03-01T00:00:00Z",
    )
    second = RawArtifactRecord(
        artifact_id="artifact:b",
        plan_id="CA-PERS",
        plan_period="FY2024",
        source_url="https://example.org/ca.pdf",
        fetched_at="2026-03-02T00:00:00Z",
        mime_type="application/pdf",
        byte_size=1,
        checksum_sha256="b" * 64,
        is_active=True,
        supersedes_artifact_id="artifact:a",
        superseded_by_artifact_id="artifact:a",
        first_seen_at="2026-03-02T00:00:00Z",
        last_seen_at="2026-03-02T00:00:00Z",
    )

    lineage = lineage_for_artifact(records=[first, second], artifact_id="artifact:a")
    assert [row.artifact_id for row in lineage] == ["artifact:b", "artifact:a"]
