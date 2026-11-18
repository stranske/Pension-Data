"""Unit tests for backplane emitter guardrails and helper edge cases."""

from __future__ import annotations

import json
from importlib import metadata
from pathlib import Path
from unittest.mock import patch

import pytest

from pension_data.ops import backplane_emitter as emitter_module
from pension_data.ops.backplane_emitter import (
    _git_sha,
    _relative_artifact_path,
    _tool_version,
    _warning_summary,
    build_backplane_reference_run,
)
from pension_data.ops.one_pdf_pilot import OnePdfPilotInput, run_one_pdf_pilot

_REPO_ROOT = Path(__file__).resolve().parents[2]
_FIXTURE = _REPO_ROOT / "tests" / "parser" / "fixtures" / "calpers_fy2024_excerpt.pdf"


def _pilot_manifest_path(tmp_path: Path, *, run_root: Path | None = None) -> Path:
    output_root = run_root or tmp_path / "reference"
    result = run_one_pdf_pilot(
        pilot_input=OnePdfPilotInput(
            pdf_path=_FIXTURE,
            plan_id="CA-PERS",
            plan_period="FY2024",
            effective_date="2024-06-30",
            ingestion_date="2026-01-01",
        ),
        output_root=output_root,
        run_id="backplane-guard-test",
    )
    return Path(result["run_manifest_json"])


def test_git_sha_returns_none_when_git_unavailable(tmp_path: Path) -> None:
    assert _git_sha(repo_root=tmp_path / "not-a-git-repo") is None


def test_tool_version_falls_back_when_package_metadata_missing() -> None:
    with patch.object(metadata, "version", side_effect=metadata.PackageNotFoundError):
        assert _tool_version() == "0.0+local"


def test_relative_artifact_path_rejects_paths_outside_run_root(tmp_path: Path) -> None:
    root = tmp_path / "root"
    root.mkdir()
    outside = tmp_path / "outside.json"
    outside.write_text("{}", encoding="utf-8")
    with pytest.raises(ValueError, match="outside run root"):
        _relative_artifact_path(outside, root)


def test_warning_summary_wraps_non_list_payload() -> None:
    path = Path(__file__)
    with patch.object(emitter_module, "_read_json", return_value={"not": "a-list"}):
        warnings = _warning_summary(path)
    assert len(warnings) == 1
    assert warnings[0]["code"] == "warning-artifact-malformed"


def test_warning_summary_is_empty_for_empty_list(tmp_path: Path) -> None:
    warning_path = tmp_path / "warnings.json"
    warning_path.write_text("[]", encoding="utf-8")
    assert _warning_summary(warning_path) == []


@pytest.mark.parametrize(
    ("mutator", "message"),
    [
        (lambda manifest: manifest.update({"artifact_files": "not-a-dict"}), "artifact_files"),
        (lambda manifest: manifest.pop("artifact_files", None), "artifact_files"),
        (lambda manifest: manifest.update({"artifact_files": {}}), "artifact_files"),
    ],
)
def test_build_backplane_reference_run_rejects_invalid_manifest(
    tmp_path: Path, mutator, message: str
) -> None:
    reference_root = tmp_path / "invalid-manifest"
    manifest_path = _pilot_manifest_path(tmp_path, run_root=reference_root)
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    mutator(manifest)
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    with pytest.raises(ValueError, match=message):
        build_backplane_reference_run(
            pilot_manifest_path=manifest_path,
            output_dir=reference_root,
            wall_ms=1.0,
        )


def test_build_backplane_reference_run_rejects_missing_pilot_artifact(tmp_path: Path) -> None:
    reference_root = tmp_path / "missing-artifact"
    manifest_path = _pilot_manifest_path(tmp_path, run_root=reference_root)
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    artifact_key = next(iter(manifest["artifact_files"]))
    artifact_path = Path(manifest["artifact_files"][artifact_key])
    artifact_path.unlink()
    with pytest.raises(FileNotFoundError, match="pilot artifact missing"):
        build_backplane_reference_run(
            pilot_manifest_path=manifest_path,
            output_dir=reference_root,
            wall_ms=1.0,
        )


def test_build_backplane_reference_run_rejects_non_object_manifest(tmp_path: Path) -> None:
    reference_root = tmp_path / "non-object-manifest"
    manifest_path = reference_root / "run_manifest.json"
    reference_root.mkdir(parents=True)
    manifest_path.write_text(json.dumps(["not", "a", "dict"]), encoding="utf-8")
    with pytest.raises(ValueError, match="must be a JSON object"):
        build_backplane_reference_run(
            pilot_manifest_path=manifest_path,
            output_dir=reference_root,
            wall_ms=1.0,
        )


def test_build_backplane_reference_run_rejects_invalid_relationship_rows(tmp_path: Path) -> None:
    reference_root = tmp_path / "bad-relationships"
    manifest_path = _pilot_manifest_path(tmp_path, run_root=reference_root)
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    relationship_path = Path(
        manifest["artifact_files"]["staging_manager_fund_vehicle_relationships_json"]
    )
    relationship_path.write_text(json.dumps(["not-an-object"]), encoding="utf-8")
    with pytest.raises(ValueError, match="staging_manager_fund_vehicle_relationships_json"):
        build_backplane_reference_run(
            pilot_manifest_path=manifest_path,
            output_dir=reference_root,
            wall_ms=1.0,
        )


def test_build_backplane_reference_run_rejects_missing_source_evidence(tmp_path: Path) -> None:
    reference_root = tmp_path / "missing-evidence"
    manifest_path = _pilot_manifest_path(tmp_path, run_root=reference_root)
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    parser_path = Path(manifest["artifact_files"]["parser_result_json"])
    parser_payload = json.loads(parser_path.read_text(encoding="utf-8"))
    parser_payload.pop("source_evidence", None)
    parser_path.write_text(json.dumps(parser_payload), encoding="utf-8")
    with pytest.raises(ValueError, match="source_evidence"):
        build_backplane_reference_run(
            pilot_manifest_path=manifest_path,
            output_dir=reference_root,
            wall_ms=1.0,
        )


def test_build_backplane_reference_run_rejects_non_object_staging_rows(tmp_path: Path) -> None:
    reference_root = tmp_path / "bad-staging"
    manifest_path = _pilot_manifest_path(tmp_path, run_root=reference_root)
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    core_path = Path(manifest["artifact_files"]["staging_core_metrics_json"])
    core_path.write_text(json.dumps(["not-an-object"]), encoding="utf-8")
    with pytest.raises(ValueError, match="staging_core_metrics_json"):
        build_backplane_reference_run(
            pilot_manifest_path=manifest_path,
            output_dir=reference_root,
            wall_ms=1.0,
        )


def test_backplane_emitter_main_writes_paths(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    output_dir = tmp_path / "cli-out"
    manifest_path = _pilot_manifest_path(tmp_path, run_root=output_dir)

    def fake_pilot(**_kwargs: object) -> dict[str, str]:
        return {"run_manifest_json": str(manifest_path)}

    monkeypatch.setattr(
        "pension_data.ops.one_pdf_pilot.run_one_pdf_pilot",
        fake_pilot,
    )
    from pension_data.ops.backplane_emitter import main

    argv = [
        "--out",
        str(output_dir),
        "--fixture",
        str(_FIXTURE),
        "--run-id",
        "backplane-cli-guard",
    ]
    assert main(argv) == 0
    assert (output_dir / "run.json").is_file()
    assert (output_dir / "manifest.json").is_file()
