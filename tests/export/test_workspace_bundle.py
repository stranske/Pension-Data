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
