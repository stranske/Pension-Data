"""Fail closed on malformed persisted facts and preserve run review metadata."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from tests.api.test_artifact_data import _metric_row, _write_run

from pension_data.api.artifact_data import QueryArtifactError, load_query_data_snapshot


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("fact_id", "   ", "requires non-empty fact_id"),
        ("normalized_unit", "", "normalized_unit must be non-empty or null"),
        ("confidence", None, "confidence must be between 0 and 1"),
        ("confidence", -0.1, "confidence must be between 0 and 1"),
        ("confidence", 1.1, "confidence must be between 0 and 1"),
        ("effective_date", "yesterday", "effective_date must be ISO-8601"),
        ("valid_to", "yesterday", "valid_to must be ISO-8601"),
        ("superseded_at", "yesterday", "superseded_at must be ISO-8601"),
        ("restated", 1, "restated must be boolean"),
        ("evidence_refs", [], "requires non-empty evidence_refs"),
        ("evidence_refs", "p.4", "requires non-empty evidence_refs"),
        ("evidence_refs", [None], "evidence_refs must contain strings"),
        ("evidence_refs", ["   "], "evidence_refs must be non-empty"),
        ("normalized_unit", "percent", "funded_ratio must use normalized unit 'ratio'"),
    ],
)
def test_snapshot_rejects_invalid_metric_fields(
    tmp_path: Path, field: str, value: object, message: str
) -> None:
    row = {**_metric_row(), field: value}
    _write_run(tmp_path, "run-a", [row])

    with pytest.raises(QueryArtifactError, match=message):
        load_query_data_snapshot(tmp_path)


def test_snapshot_requires_at_least_one_reported_or_normalized_value(tmp_path: Path) -> None:
    row = {**_metric_row(), "normalized_value": None, "as_reported_value": None}
    _write_run(tmp_path, "run-a", [row])

    with pytest.raises(QueryArtifactError, match="metric row requires a numeric value"):
        load_query_data_snapshot(tmp_path)


def test_snapshot_preserves_temporal_bounds_and_deduplicates_evidence(tmp_path: Path) -> None:
    row = {
        **_metric_row(),
        "valid_from": "2024-07-01",
        "valid_to": "2025-07-01",
        "asserted_at": "2026-02-01T00:00:00Z",
        "superseded_at": "2026-03-01T00:00:00Z",
        "restated": True,
        "evidence_refs": ["p.4#table", "p.4#table"],
    }
    _write_run(tmp_path, "run-a", [row])

    metric = load_query_data_snapshot(tmp_path).metric_history_rows[0]

    assert metric.valid_from == row["valid_from"]
    assert metric.valid_to == row["valid_to"]
    assert metric.asserted_at == row["asserted_at"]
    assert metric.superseded_at == row["superseded_at"]
    assert metric.is_restated is True
    assert len(metric.provenance_refs) == 1
    assert metric.provenance_refs[0].raw_ref == "p.4#table"


@pytest.mark.parametrize("payload", [b"{broken", b"\xff", b""])
def test_snapshot_wraps_unreadable_manifest(tmp_path: Path, payload: bytes) -> None:
    run = _write_run(tmp_path, "run-a", [_metric_row()])
    (run / "run_manifest.json").write_bytes(payload)

    with pytest.raises(QueryArtifactError, match="invalid query artifact: run_manifest.json"):
        load_query_data_snapshot(tmp_path)


@pytest.mark.parametrize(
    ("change", "message"),
    [
        ({"ledger_status": "failed"}, "query run manifest is not successful"),
        ({"input": {"plan_id": " "}}, "input requires non-empty plan_id"),
        ({"review_context": {}}, "review_context missing required fields"),
        ({"input": {"source_authority_tier": "untrusted"}}, "source_authority_tier is invalid"),
    ],
)
def test_snapshot_rejects_invalid_manifest(
    tmp_path: Path, change: dict[str, object], message: str
) -> None:
    run = _write_run(tmp_path, "run-a", [_metric_row()])
    path = run / "run_manifest.json"
    manifest = json.loads(path.read_text())
    manifest.update(change)
    path.write_text(json.dumps(manifest))

    with pytest.raises(QueryArtifactError, match=message):
        load_query_data_snapshot(tmp_path)


@pytest.mark.parametrize(
    ("payload", "message"),
    [
        ({}, "staging_core_metrics.json must contain a list"),
        ([None], "staging_core_metrics row must be an object"),
        ([], "completed pilot runs contain no query metric rows"),
    ],
)
def test_snapshot_rejects_invalid_staging_inventory(
    tmp_path: Path, payload: object, message: str
) -> None:
    run = _write_run(tmp_path, "run-a", [_metric_row()])
    (run / "extraction_persistence" / "staging_core_metrics.json").write_text(json.dumps(payload))

    with pytest.raises(QueryArtifactError, match=message):
        load_query_data_snapshot(tmp_path)


@pytest.mark.parametrize("layout", ["missing-root", "file-root", "file-pilot", "no-runs"])
def test_snapshot_rejects_unusable_artifact_roots(tmp_path: Path, layout: str) -> None:
    root = tmp_path / "artifacts"
    if layout == "missing-root":
        message = "configured query artifact root does not exist"
    elif layout == "file-root":
        root.write_text("file")
        message = "configured query artifact root must be a directory"
    else:
        root.mkdir()
        pilot = root / "one_pdf_pilot"
        if layout == "file-pilot":
            pilot.write_text("file")
            message = "query artifact one_pdf_pilot must be a directory"
        else:
            (pilot / "in-progress").mkdir(parents=True)
            message = "no completed one-PDF pilot runs found"

    with pytest.raises(QueryArtifactError, match=message):
        load_query_data_snapshot(root)


def _review_context(escalation: bool | None, tier: str | None) -> dict[str, object]:
    return {
        "actionable_flags": [],
        "escalation_required": escalation,
        "source_authority_tier": tier,
        "source_authority_status": "declared" if tier else "unknown",
    }


@pytest.mark.parametrize("reverse", [False, True])
@pytest.mark.parametrize("second_escalation", [False, None])
@pytest.mark.parametrize("second_tier", ["official-mirror", "official"])
def test_snapshot_duplicate_review_merge_preserves_uncertainty(
    tmp_path: Path, reverse: bool, second_escalation: bool | None, second_tier: str
) -> None:
    first = _review_context(False, "official-mirror")
    first["actionable_flags"] = ["z-review", "shared"]
    second = _review_context(second_escalation, second_tier)
    second["actionable_flags"] = ["shared", "a-review"]
    contexts = [first, second]
    if reverse:
        contexts.reverse()
    for index, context in enumerate(contexts):
        _write_run(tmp_path, f"run-{index}", [_metric_row()], review_context=context)

    snapshot = load_query_data_snapshot(tmp_path)

    assert len(snapshot.metric_history_rows) == 1
    expected = _review_context(
        second_escalation, "official-mirror" if second_tier == "official-mirror" else None
    )
    expected["actionable_flags"] = ["a-review", "shared", "z-review"]
    assert snapshot.metric_history_rows[0].review_context.to_dict() == expected


@pytest.mark.parametrize("row_context", [[], {}, _review_context(True, None)])
def test_snapshot_rejects_row_review_context_conflicting_with_manifest(
    tmp_path: Path, row_context: object
) -> None:
    row = {**_metric_row(), "review_context": row_context}
    _write_run(tmp_path, "run-a", [row], review_context=_review_context(False, None))

    with pytest.raises(QueryArtifactError, match="review_context"):
        load_query_data_snapshot(tmp_path)


@pytest.mark.parametrize("explicit_context", [False, True])
def test_snapshot_preserves_declared_manifest_authority(
    tmp_path: Path, explicit_context: bool
) -> None:
    context = _review_context(False, "official-mirror") if explicit_context else None
    run = _write_run(tmp_path, "run-a", [_metric_row()], review_context=context)
    path = run / "run_manifest.json"
    manifest = json.loads(path.read_text())
    manifest["input"]["source_authority_tier"] = " official-mirror "
    path.write_text(json.dumps(manifest))

    metric = load_query_data_snapshot(tmp_path).metric_history_rows[0]

    assert metric.review_context.source_authority_tier == "official-mirror"
    assert metric.review_context.source_authority_status == "declared"


def test_snapshot_rejects_manifest_authority_disagreement(tmp_path: Path) -> None:
    run = _write_run(
        tmp_path, "run-a", [_metric_row()], review_context=_review_context(False, None)
    )
    path = run / "run_manifest.json"
    manifest = json.loads(path.read_text())
    manifest["input"]["source_authority_tier"] = "official-mirror"
    path.write_text(json.dumps(manifest))

    with pytest.raises(QueryArtifactError, match="source_authority_tier conflicts with input"):
        load_query_data_snapshot(tmp_path)
