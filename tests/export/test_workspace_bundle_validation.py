"""Validate persisted workspace contracts before replacing an operator's bundle."""

from __future__ import annotations

from pathlib import Path

import pytest
from tests.export.test_workspace_bundle import _row

from pension_data.export.workspace_bundle import (
    WorkspaceBundleError,
    build_workspace_bundle,
    map_staging_row_to_ui_row,
    normalize_review_context,
    write_workspace_bundle,
)


@pytest.mark.parametrize(
    ("location", "field", "value", "message"),
    [
        ("bundle", "datasets", None, "dataset inventory must be a non-empty list"),
        ("bundle", "datasets", [], "dataset inventory must be a non-empty list"),
        ("bundle", "datasets", [None], "dataset 0 must be an object"),
        ("bundle", "contractVersion", "unsupported", "workspace contractVersion must be"),
        ("dataset", "rows", [], "dataset 0 rows must be non-empty"),
        ("dataset", "rows", [None], "dataset 0 row 0 must be an object"),
        ("row", "provenance", None, "row 0 provenance must be an object"),
        ("provenance", "source_document", " ", "provenance.source_document is required"),
        ("provenance", "source_document", 123, "provenance.source_document is required"),
        ("provenance", "evidence_refs", "p.4", "evidence_refs must be a list of strings"),
        ("provenance", "evidence_refs", [None], "evidence_refs must be a list of strings"),
        ("provenance", "evidence_refs", [" "], "evidence_refs must be a list of strings"),
        ("provenance", "source_locator", "", "source_locator must be a non-empty relative path"),
        ("provenance", "source_locator", 123, "source_locator must be a non-empty relative path"),
        ("provenance", "unexpected", "extra", "provenance has unsupported fields: unexpected"),
        ("row", "value", float("nan"), "workspace bundle is not strict JSON"),
        ("row", "value", {1, 2}, "workspace bundle is not strict JSON"),
    ],
)
def test_writer_rejects_malformed_bundle_without_replacing_existing_file(
    tmp_path: Path, location: str, field: str, value: object, message: str
) -> None:
    payload = build_workspace_bundle([_row()], run_id="run-validation", last_updated="2026-01-01")
    output = write_workspace_bundle(tmp_path / "workspace.json", payload)
    before = output.read_bytes()
    dataset = payload["datasets"][0]
    row = dataset["rows"][0]
    targets = {"bundle": payload, "dataset": dataset, "row": row, "provenance": row["provenance"]}
    targets[location][field] = value

    with pytest.raises(WorkspaceBundleError, match=message):
        write_workspace_bundle(output, payload)

    assert output.read_bytes() == before


@pytest.mark.parametrize(
    ("location", "field"),
    [("bundle", "datasets"), ("row", "value"), ("provenance", "evidence_refs")],
)
def test_writer_rejects_missing_contract_fields(tmp_path: Path, location: str, field: str) -> None:
    payload = build_workspace_bundle([_row()], run_id="run-validation", last_updated="2026-01-01")
    row = payload["datasets"][0]["rows"][0]
    targets = {"bundle": payload, "row": row, "provenance": row["provenance"]}
    del targets[location][field]

    with pytest.raises(WorkspaceBundleError, match=f"missing required fields: {field}"):
        write_workspace_bundle(tmp_path / "workspace.json", payload)

    assert not (tmp_path / "workspace.json").exists()


@pytest.mark.parametrize(
    ("context", "strict", "message"),
    [
        (None, True, "review_context must be an object"),
        ([], False, "review_context must be an object"),
        ({"escalation_required": 1}, False, "escalation_required must be boolean or null"),
        ({"actionable_flags": None}, True, "actionable_flags must be a list of strings"),
        ({"actionable_flags": ("review",)}, True, "actionable_flags must be a list of strings"),
        ({"actionable_flags": "review"}, False, "actionable_flags must be a list of strings"),
        ({"actionable_flags": [None]}, False, "actionable_flags must contain non-empty strings"),
        ({"actionable_flags": [" "]}, False, "actionable_flags must contain non-empty strings"),
        ({"source_authority_tier": "untrusted"}, False, "source_authority_tier must be one of"),
        ({"source_authority_status": None}, True, "source_authority_status must be"),
        ({"source_authority_status": "trusted"}, False, "source_authority_status must be"),
        ({"source_authority_status": "declared"}, False, "conflicts with source_authority_tier"),
    ],
)
def test_review_context_refuses_ambiguous_metadata(
    context: object, strict: bool, message: str
) -> None:
    if isinstance(context, dict):
        context = {
            "actionable_flags": [],
            "escalation_required": None,
            "source_authority_tier": None,
            "source_authority_status": "unknown",
            **context,
        }

    with pytest.raises(WorkspaceBundleError, match=message):
        normalize_review_context(context, require_all_fields=strict)


def test_review_context_normalizes_legacy_flags_and_infers_declared_authority() -> None:
    normalized = normalize_review_context(
        {"actionable_flags": (" review ", "review"), "source_authority_tier": " official "}
    )

    assert normalized == {
        "actionable_flags": ["review"],
        "escalation_required": None,
        "source_authority_tier": "official",
        "source_authority_status": "declared",
    }


@pytest.mark.parametrize(
    ("evidence", "expected"),
    [
        (None, []),
        (" ", []),
        (" p.4#table ", ["p.4#table"]),
        ('["p.4#table", " ", null]', ["p.4#table"]),
        (42, ["42"]),
    ],
)
@pytest.mark.parametrize("confidence", [None, "not-a-number"])
def test_staging_mapper_handles_legacy_evidence_and_missing_confidence(
    evidence: object, expected: list[str], confidence: object
) -> None:
    row = {
        **_row(),
        "evidence_refs": evidence,
        "confidence": confidence,
        "normalized_value": None,
        "as_reported_value": 81.0,
    }

    mapped = map_staging_row_to_ui_row(row)

    assert mapped["provenance"]["evidence_refs"] == expected
    assert mapped["confidence"] == 0.0
    assert mapped["value"] == 81.0


@pytest.mark.parametrize("use_document_map", [False, True])
def test_staging_mapper_preserves_source_locator_fallbacks(use_document_map: bool) -> None:
    row = _row()
    mapped = map_staging_row_to_ui_row(
        row,
        source_locators={"calpers-fy2024": "sources/document.pdf"} if use_document_map else None,
        source_locator="sources/fallback.pdf",
    )

    assert mapped["provenance"]["source_locator"] == (
        "sources/document.pdf" if use_document_map else "sources/fallback.pdf"
    )


def test_builder_rejects_conflicting_row_and_run_review_context() -> None:
    row = {**_row(), "review_context": {"escalation_required": True}}

    with pytest.raises(WorkspaceBundleError, match="staging row review_context conflicts"):
        build_workspace_bundle(
            [row], run_id="run-review", last_updated="2026-01-01", review_context={}
        )


@pytest.mark.parametrize(
    ("rows", "run_id", "message"),
    [([], "run-empty", "requires at least one staging row"), ([_row()], " ", "run_id is required")],
)
def test_builder_requires_nonempty_run_identity_and_rows(
    rows: list[dict[str, object]], run_id: str, message: str
) -> None:
    with pytest.raises(WorkspaceBundleError, match=message):
        build_workspace_bundle(rows, run_id=run_id, last_updated="2026-01-01")


@pytest.mark.parametrize("value", [float("nan"), float("inf"), -float("inf")])
def test_builder_rejects_nonfinite_metric_value(value: float) -> None:
    with pytest.raises(WorkspaceBundleError, match="workspace metric value must be finite"):
        build_workspace_bundle(
            [{**_row(), "normalized_value": value}],
            run_id="run-nonfinite",
            last_updated="2026-01-01",
        )
