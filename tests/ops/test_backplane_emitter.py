"""Tests for the Pension-Data research-backplane reference emitter."""

from __future__ import annotations

import copy
import json
import subprocess
import sys
from pathlib import Path

from pension_data.ops.backplane_emitter import build_backplane_reference_run
from pension_data.ops.one_pdf_pilot import OnePdfPilotInput, run_one_pdf_pilot

_REPO_ROOT = Path(__file__).resolve().parents[2]
_FIXTURE = _REPO_ROOT / "tests" / "parser" / "fixtures" / "calpers_fy2024_excerpt.pdf"
_REGISTRY = _REPO_ROOT / "config" / "backplane_participants.json"
_SCHEMA_DIR = _REPO_ROOT / "docs" / "contracts" / "schemas"
_VALIDATOR = _REPO_ROOT / "scripts" / "validate_run_contract.py"


def valid_reference_run(tmp_path: Path) -> dict[str, Path]:
    result = run_one_pdf_pilot(
        pilot_input=OnePdfPilotInput(
            pdf_path=_FIXTURE,
            plan_id="CA-PERS",
            plan_period="FY2024",
            effective_date="2024-06-30",
            ingestion_date="2026-01-01",
        ),
        output_root=tmp_path / "reference",
        run_id="pension-data-backplane-test",
    )
    return build_backplane_reference_run(
        pilot_manifest_path=Path(result["run_manifest_json"]),
        output_dir=tmp_path / "reference",
        wall_ms=123.4,
        recorded_at="2026-01-01T00:00:00+00:00",
        repo_root=_REPO_ROOT,
    )


def _load(path: Path) -> dict[str, object]:
    return json.loads(path.read_text(encoding="utf-8"))


def _validate(run_json: Path, manifest: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            sys.executable,
            str(_VALIDATOR),
            str(run_json),
            "--manifest",
            str(manifest),
            "--registry",
            str(_REGISTRY),
            "--schema-dir",
            str(_SCHEMA_DIR),
            "--repo",
            "stranske/Pension-Data",
        ],
        cwd=_REPO_ROOT,
        text=True,
        capture_output=True,
    )


def _validation_error_points_to(
    completed: subprocess.CompletedProcess[str], *path_parts: str
) -> bool:
    stderr = completed.stderr.lower()
    return all(part.lower() in stderr for part in path_parts)


def test_reference_run_validates_strictly(tmp_path: Path) -> None:
    paths = valid_reference_run(tmp_path)
    completed = _validate(paths["run_json"], paths["manifest"])
    assert completed.returncode == 0, completed.stderr

    run_payload = _load(paths["run_json"])
    manifest = _load(paths["manifest"])
    assert run_payload["schema_version"] == "run-contract/v1"
    assert run_payload["repo"] == "stranske/Pension-Data"
    assert run_payload["tool"] == "one-pdf-pilot"
    assert run_payload["cost"] == {"input_tokens": 0, "output_tokens": 0, "usd": 0}
    assert run_payload["latency"]["wall_ms"] == 123.4
    assert run_payload["evidence_refs"]
    assert run_payload["identity_refs"]
    assert run_payload["data_quality"]["overall_status"] == "ok"
    assert "rows" not in run_payload
    assert "rows" not in run_payload["data_quality"]

    manifest_ids = {artifact["artifact_id"] for artifact in manifest["artifacts"]}
    assert set(run_payload["outputs"]["artifact_ids"]) == manifest_ids
    for artifact in manifest["artifacts"]:
        assert len(artifact["sha256"]) == 64


def test_run_id_is_deterministic_for_same_pilot_input(tmp_path: Path) -> None:
    first = valid_reference_run(tmp_path / "first")
    second = valid_reference_run(tmp_path / "second")
    assert _load(first["run_json"])["run_id"] == _load(second["run_json"])["run_id"]


def test_negative_cost_is_rejected(tmp_path: Path) -> None:
    paths = valid_reference_run(tmp_path)
    run_payload = _load(paths["run_json"])
    run_payload["cost"]["usd"] = -0.01
    paths["run_json"].write_text(json.dumps(run_payload, indent=2), encoding="utf-8")

    completed = _validate(paths["run_json"], paths["manifest"])
    assert completed.returncode != 0
    assert _validation_error_points_to(completed, "cost", "usd"), completed.stderr


def test_negative_latency_is_rejected(tmp_path: Path) -> None:
    paths = valid_reference_run(tmp_path)
    run_payload = _load(paths["run_json"])
    run_payload["latency"]["wall_ms"] = -1
    paths["run_json"].write_text(json.dumps(run_payload, indent=2), encoding="utf-8")

    completed = _validate(paths["run_json"], paths["manifest"])
    assert completed.returncode != 0
    assert _validation_error_points_to(completed, "latency", "wall_ms"), completed.stderr


def test_raw_payload_fields_are_rejected(tmp_path: Path) -> None:
    paths = valid_reference_run(tmp_path)
    run_payload = _load(paths["run_json"])
    run_payload["outputs"]["summary"]["rows"] = [{"funded_ratio": 0.784}]
    paths["run_json"].write_text(json.dumps(run_payload, indent=2), encoding="utf-8")

    completed = _validate(paths["run_json"], paths["manifest"])
    assert completed.returncode != 0
    assert "unsafe raw payload field 'rows'" in completed.stderr


def test_required_sections_are_enforced(tmp_path: Path) -> None:
    paths = valid_reference_run(tmp_path)
    run_payload = _load(paths["run_json"])
    del run_payload["data_quality"]
    paths["run_json"].write_text(json.dumps(run_payload, indent=2), encoding="utf-8")

    completed = _validate(paths["run_json"], paths["manifest"])
    assert completed.returncode != 0
    assert "registry requires section 'data_quality'" in completed.stderr


def test_manifest_must_cover_all_artifacts(tmp_path: Path) -> None:
    paths = valid_reference_run(tmp_path)
    run_payload = _load(paths["run_json"])
    manifest = _load(paths["manifest"])
    broken = copy.deepcopy(manifest)
    missing_id = run_payload["outputs"]["artifact_ids"][0]
    broken["artifacts"] = [
        artifact for artifact in broken["artifacts"] if artifact["artifact_id"] != missing_id
    ]
    paths["manifest"].write_text(json.dumps(broken, indent=2), encoding="utf-8")

    manifest_ids = {artifact["artifact_id"] for artifact in broken["artifacts"]}
    assert missing_id in set(run_payload["outputs"]["artifact_ids"])
    assert missing_id not in manifest_ids, "artifact missing from manifest"

    completed = _validate(paths["run_json"], paths["manifest"])
    assert completed.returncode != 0
    assert f"artifact_id '{missing_id}' not in manifest" in completed.stderr
