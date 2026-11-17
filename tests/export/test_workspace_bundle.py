"""Contract tests for generated extraction-run workspace bundles."""

from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest

from pension_data.export.workspace_bundle import (
    WORKSPACE_CONTRACT_VERSION,
    WorkspaceBundleError,
    build_workspace_bundle,
    validate_generated_workspace_bundle,
    write_workspace_bundle,
)
from pension_data.sources.schema import SOURCE_AUTHORITY_TIERS

ROOT = Path(__file__).resolve().parents[2]
RUNTIME_CONTRACT = ROOT / "apps" / "contracts" / "runtime-contract.json"


def _row(*, metric: str = "funded_ratio", confidence: float = 0.92) -> dict[str, object]:
    return {
        "confidence": confidence,
        "evidence_refs": ["p12:t4:r2"],
        "metric_family": "funded_status",
        "metric_name": metric,
        "normalized_value": 0.81,
        "plan_id": "ca-pers",
        "plan_period": "FY2024",
        "source_document_id": "calpers-fy2024",
    }


def test_bundle_matches_runtime_contract(tmp_path: Path) -> None:
    contract = json.loads(RUNTIME_CONTRACT.read_text(encoding="utf-8"))
    payload = build_workspace_bundle(
        [_row()],
        run_id="run-123",
        last_updated="2024-06-30",
    )
    output = write_workspace_bundle(tmp_path / "workspace-bundle.json", payload)

    persisted = json.loads(output.read_text(encoding="utf-8"))
    descriptor = contract["workspaceBundle"]
    assert persisted["contractVersion"] == contract["version"] == WORKSPACE_CONTRACT_VERSION
    assert persisted["data_origin"] == "generated"
    assert set(descriptor["requiredTopLevelFields"]) <= set(persisted)
    assert set(descriptor["datasetRequiredFields"]) <= set(persisted["datasets"][0])
    assert persisted["datasets"][0]["rows"]


@pytest.mark.parametrize("invalid_origin", ["fixture", "live", "invalid"])
def test_writer_rejects_non_generated_origin_before_creating_file(
    tmp_path: Path, invalid_origin: str
) -> None:
    payload = build_workspace_bundle([_row()], run_id="run-123", last_updated="2024-06-30")
    broken = copy.deepcopy(payload)
    broken["data_origin"] = invalid_origin
    output = tmp_path / "workspace-bundle.json"

    with pytest.raises(WorkspaceBundleError, match="data_origin='generated'"):
        write_workspace_bundle(output, broken)

    assert not output.exists()


def test_bundle_bytes_are_stable_across_input_order(tmp_path: Path) -> None:
    first = build_workspace_bundle(
        [_row(metric="funded_ratio"), _row(metric="aal_usd")],
        run_id="run-123",
        last_updated="2024-06-30",
    )
    second = build_workspace_bundle(
        [_row(metric="aal_usd"), _row(metric="funded_ratio")],
        run_id="run-123",
        last_updated="2024-06-30",
    )

    first_path = write_workspace_bundle(tmp_path / "first.json", first)
    second_path = write_workspace_bundle(tmp_path / "second.json", second)
    assert first_path.read_bytes() == second_path.read_bytes()


def test_nonfinite_values_are_rejected() -> None:
    with pytest.raises(WorkspaceBundleError, match="confidence must be finite"):
        build_workspace_bundle(
            [_row(confidence=float("nan"))],
            run_id="run-123",
            last_updated="2024-06-30",
        )


def test_validator_rejects_missing_dataset_contract_field() -> None:
    payload = build_workspace_bundle([_row()], run_id="run-123", last_updated="2024-06-30")
    del payload["datasets"][0]["freshness"]

    with pytest.raises(WorkspaceBundleError, match="missing required fields: freshness"):
        validate_generated_workspace_bundle(payload)


@pytest.mark.parametrize("authority_tier", [None, *SOURCE_AUTHORITY_TIERS])
@pytest.mark.parametrize("escalation_required", [None, False, True])
def test_bundle_carries_and_validates_review_context(
    authority_tier: str | None, escalation_required: bool | None
) -> None:
    review_context = {
        "actionable_flags": ["doc_lineage_unreadable_pages:1"],
        "escalation_required": escalation_required,
        "source_authority_status": "declared" if authority_tier is not None else "unknown",
        "source_authority_tier": authority_tier,
    }
    payload = build_workspace_bundle(
        [_row(), _row(metric="aal_usd")],
        run_id="run-review",
        last_updated="2024-06-30",
        review_context=review_context,
    )
    assert all(row["review_context"] == review_context for row in payload["datasets"][0]["rows"])

    broken = copy.deepcopy(payload)
    broken["datasets"][0]["rows"][0]["review_context"]["source_authority_tier"] = "invalid-tier"
    with pytest.raises(WorkspaceBundleError, match="source_authority_tier"):
        validate_generated_workspace_bundle(broken)


def test_legacy_bundle_rows_have_explicit_unknown_review_context() -> None:
    payload = build_workspace_bundle([_row()], run_id="legacy", last_updated="2024-06-30")
    assert payload["datasets"][0]["rows"][0]["review_context"] == {
        "actionable_flags": [],
        "escalation_required": None,
        "source_authority_status": "unknown",
        "source_authority_tier": None,
    }


def test_runtime_contract_authority_tiers_match_source_schema() -> None:
    contract = json.loads(RUNTIME_CONTRACT.read_text(encoding="utf-8"))
    assert tuple(contract["workspaceBundle"]["sourceAuthorityTiers"]) == SOURCE_AUTHORITY_TIERS


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("actionable_flags", ["manual_review_needed", "manual_review_needed"]),
        ("actionable_flags", [" manual_review_needed "]),
        ("source_authority_status", " declared "),
        ("source_authority_tier", " official "),
    ],
)
def test_writer_rejects_noncanonical_review_context_without_replacing_file(
    tmp_path: Path, field: str, value: object
) -> None:
    payload = build_workspace_bundle(
        [_row()],
        run_id="run-review",
        last_updated="2024-06-30",
        review_context={
            "actionable_flags": ["manual_review_needed"],
            "escalation_required": True,
            "source_authority_status": "declared",
            "source_authority_tier": "official",
        },
    )
    output = write_workspace_bundle(tmp_path / "workspace-bundle.json", payload)
    before = output.read_bytes()
    payload["datasets"][0]["rows"][0]["review_context"][field] = value

    with pytest.raises(WorkspaceBundleError, match="review_context must use canonical values"):
        write_workspace_bundle(output, payload)

    assert output.read_bytes() == before


@pytest.mark.parametrize(
    "field",
    ["actionable_flags", "escalation_required", "source_authority_status", "source_authority_tier"],
)
def test_producer_rejects_missing_review_context_fields(field: str) -> None:
    payload = build_workspace_bundle([_row()], run_id="run-review", last_updated="2024-06-30")
    del payload["datasets"][0]["rows"][0]["review_context"][field]

    with pytest.raises(WorkspaceBundleError, match=f"missing required fields: {field}"):
        validate_generated_workspace_bundle(payload)
