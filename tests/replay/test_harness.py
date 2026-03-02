"""Tests for deterministic replay snapshots and diff classification."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from tools.replay.harness import (
    REPLAY_BASELINE_ARTIFACT_TYPE,
    SUPPORTED_ARTIFACT_SCHEMA_VERSION,
    SUPPORTED_BASELINE_VERSION,
    CorpusDocument,
    FieldExtraction,
    ReplayResult,
    build_snapshot,
    diff_snapshots,
    load_snapshot,
    run_replay,
    write_snapshot,
)


def _parser(document: CorpusDocument) -> dict[str, FieldExtraction]:
    if document.document_id == "doc-a":
        return {
            "manager_count": FieldExtraction(value=23, confidence=0.90, evidence="p2"),
            "funded_ratio": FieldExtraction(value=0.80, confidence=0.95, evidence="p1"),
        }
    return {
        "manager_count": FieldExtraction(value=17, confidence=0.88, evidence="p3"),
        "funded_ratio": FieldExtraction(value=0.72, confidence=0.91, evidence="p4"),
    }


def test_run_replay_is_deterministic_for_ordering() -> None:
    corpus = [
        CorpusDocument(document_id="doc-b", content="beta"),
        CorpusDocument(document_id="doc-a", content="alpha"),
    ]
    first = run_replay(corpus, _parser)
    second = run_replay(list(reversed(corpus)), _parser)

    assert [item.document_id for item in first] == ["doc-a", "doc-b"]
    assert [item.document_id for item in second] == ["doc-a", "doc-b"]
    assert list(first[0].fields.keys()) == ["funded_ratio", "manager_count"]
    assert first == second


def test_snapshot_write_requires_explicit_overwrite(tmp_path: Path) -> None:
    output_path = tmp_path / "baseline_v1.json"
    replay_results = run_replay([CorpusDocument(document_id="doc-a", content="alpha")], _parser)
    snapshot = build_snapshot(
        replay_results,
        baseline_version=SUPPORTED_BASELINE_VERSION,
        generated_at=datetime(2026, 3, 2, 0, 0, tzinfo=UTC),
    )

    write_snapshot(output_path, snapshot)
    with pytest.raises(FileExistsError):
        write_snapshot(output_path, snapshot)

    write_snapshot(output_path, snapshot, overwrite=True)
    loaded = load_snapshot(output_path)
    assert loaded == snapshot


def test_snapshot_includes_versioned_artifact_metadata() -> None:
    replay_results = run_replay([CorpusDocument(document_id="doc-a", content="alpha")], _parser)
    snapshot = build_snapshot(
        replay_results,
        parser_id="tests.replay.fixtures_parser:parser",
        generated_at=datetime(2026, 3, 2, 0, 0, tzinfo=UTC),
    )

    assert snapshot["artifact_type"] == REPLAY_BASELINE_ARTIFACT_TYPE
    assert snapshot["schema_version"] == SUPPORTED_ARTIFACT_SCHEMA_VERSION
    assert snapshot["baseline_version"] == SUPPORTED_BASELINE_VERSION
    assert snapshot["parser_id"] == "tests.replay.fixtures_parser:parser"


def test_diff_classifies_expected_and_unexpected_drift() -> None:
    baseline_results = run_replay([CorpusDocument(document_id="doc-a", content="alpha")], _parser)
    current_results = [
        ReplayResult(
            document_id="doc-a",
            fields={
                "funded_ratio": FieldExtraction(value=0.81, confidence=0.95, evidence="p1"),
                "manager_count": FieldExtraction(value=21, confidence=0.90, evidence="p2"),
            },
        )
    ]
    baseline = build_snapshot(
        baseline_results,
        generated_at=datetime(2026, 3, 2, 0, 0, tzinfo=UTC),
    )
    current = build_snapshot(
        current_results,
        generated_at=datetime(2026, 3, 2, 0, 1, tzinfo=UTC),
    )

    report = diff_snapshots(
        baseline=baseline,
        current=current,
        expected_change_fields={("doc-a", "funded_ratio")},
    )

    assert report["total_changes"] == 2
    assert report["unexpected_changes"] == 1
    assert {change["classification"] for change in report["changes"]} == {
        "expected_change",
        "unexpected_drift",
    }
    assert any(
        change["field"] == "funded_ratio" and change["classification"] == "expected_change"
        for change in report["changes"]
    )
    assert any(
        change["field"] == "manager_count" and change["classification"] == "unexpected_drift"
        for change in report["changes"]
    )


def test_diff_reports_field_presence_changes() -> None:
    baseline_results = run_replay([CorpusDocument(document_id="doc-a", content="alpha")], _parser)
    baseline = build_snapshot(baseline_results, generated_at=datetime(2026, 3, 2, 0, 0, tzinfo=UTC))
    current = build_snapshot(
        [
            ReplayResult(
                document_id="doc-a",
                fields={
                    "funded_ratio": FieldExtraction(value=0.80, confidence=0.95, evidence="p1")
                },
            )
        ],
        generated_at=datetime(2026, 3, 2, 0, 1, tzinfo=UTC),
    )

    report = diff_snapshots(baseline=baseline, current=current)
    assert report["total_changes"] == 1
    change = report["changes"][0]
    assert change["attribute"] == "field_presence"
    assert change["field"] == "manager_count"


def test_load_snapshot_rejects_duplicate_document_ids(tmp_path: Path) -> None:
    snapshot_path = tmp_path / "duplicate_ids.json"
    snapshot_path.write_text(
        json.dumps(
            {
                "artifact_type": REPLAY_BASELINE_ARTIFACT_TYPE,
                "schema_version": SUPPORTED_ARTIFACT_SCHEMA_VERSION,
                "baseline_version": SUPPORTED_BASELINE_VERSION,
                "parser_id": "tests.replay.fixtures_parser:parser",
                "generated_at": "2026-03-02T00:00:00+00:00",
                "documents": [
                    {
                        "document_id": "doc-a",
                        "fields": {
                            "funded_ratio": {"value": 0.8, "confidence": 0.95, "evidence": "p1"}
                        },
                    },
                    {
                        "document_id": "doc-a",
                        "fields": {
                            "funded_ratio": {"value": 0.79, "confidence": 0.95, "evidence": "p2"}
                        },
                    },
                ],
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="duplicate document_id"):
        load_snapshot(snapshot_path)


def test_load_snapshot_rejects_unsupported_schema_version(tmp_path: Path) -> None:
    snapshot_path = tmp_path / "schema_v2.json"
    snapshot_path.write_text(
        json.dumps(
            {
                "artifact_type": REPLAY_BASELINE_ARTIFACT_TYPE,
                "schema_version": 2,
                "baseline_version": SUPPORTED_BASELINE_VERSION,
                "parser_id": "tests.replay.fixtures_parser:parser",
                "generated_at": "2026-03-02T00:00:00+00:00",
                "documents": [],
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="schema_version"):
        load_snapshot(snapshot_path)
