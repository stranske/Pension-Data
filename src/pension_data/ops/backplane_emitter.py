"""Project one-PDF pilot artifacts into the research backplane run contract."""

from __future__ import annotations

import hashlib
import json
import platform
import subprocess
import sys
from collections.abc import Mapping
from datetime import UTC, datetime
from importlib import metadata
from pathlib import Path
from typing import Any

from pension_data.emit.evidence_object import build_evidence_objects
from pension_data.emit.identity_map import build_identity_maps, identity_refs_from_maps

RUN_SCHEMA_VERSION = "run-contract/v1"
MANIFEST_SCHEMA_VERSION = "artifact-manifest/v1"
REPO = "stranske/Pension-Data"
TOOL = "one-pdf-pilot"
SOURCE_ISSUE = "stranske/Pension-Data#703"


def _read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _artifact_entry(
    *, artifact_id: str, name: str, kind: str, path: Path, root: Path
) -> dict[str, object]:
    return {
        "artifact_id": artifact_id,
        "name": name,
        "kind": kind,
        "path": _relative_artifact_path(path, root),
        "sha256": _sha256(path),
        "bytes": path.stat().st_size,
        "media_type": "application/json",
    }


def _git_sha(repo_root: Path | None = None) -> str | None:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=repo_root,
            check=True,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
        )
    except (OSError, subprocess.CalledProcessError):
        return None
    return result.stdout.strip() or None


def _tool_version() -> str:
    try:
        return metadata.version("pension-data")
    except metadata.PackageNotFoundError:
        return "0.0+local"


def _relative_artifact_path(path: Path, root: Path) -> str:
    resolved_path = path.resolve()
    resolved_root = root.resolve()
    try:
        return resolved_path.relative_to(resolved_root).as_posix()
    except ValueError as exc:
        raise ValueError(f"artifact path is outside run root: {path}") from exc


def _warning_summary(warning_path: Path) -> list[dict[str, object]]:
    warnings_payload = _read_json(warning_path)
    if not isinstance(warnings_payload, list):
        return [
            {
                "code": "warning-artifact-malformed",
                "severity": "warning",
                "message": "warning artifact was not a list",
            }
        ]
    return (
        [
            {
                "code": "extraction-warnings-present",
                "severity": "warning",
                "message": "one-pdf pilot emitted extraction warnings",
                "context": {"count": len(warnings_payload)},
            }
        ]
        if warnings_payload
        else []
    )


def build_backplane_reference_run(
    *,
    pilot_manifest_path: Path,
    output_dir: Path,
    wall_ms: float,
    recorded_at: str | None = None,
    repo_root: Path | None = None,
) -> dict[str, Path]:
    """Write ``run.json`` and ``manifest.json`` for a completed one-PDF pilot run."""
    pilot_manifest = _read_json(pilot_manifest_path)
    if not isinstance(pilot_manifest, dict):
        raise ValueError("pilot run manifest must be a JSON object")
    artifact_files = pilot_manifest.get("artifact_files")
    if not isinstance(artifact_files, dict) or not artifact_files:
        raise ValueError("pilot run manifest missing artifact_files object")

    output_dir.mkdir(parents=True, exist_ok=True)
    run_id = str(pilot_manifest["run_id"])
    artifact_entries: list[dict[str, object]] = []
    artifact_ids: list[str] = []
    for name, value in sorted(artifact_files.items()):
        path = Path(str(value))
        if not path.exists():
            raise FileNotFoundError(f"pilot artifact missing: {path}")
        artifact_id = f"one-pdf-pilot:{name}"
        artifact_ids.append(artifact_id)
        artifact_entries.append(
            _artifact_entry(
                artifact_id=artifact_id,
                name=name,
                kind="data",
                path=path,
                root=output_dir,
            )
        )

    source_pdf = Path(str(pilot_manifest["input"]["pdf_path"]))
    parser_result = _read_json(Path(str(artifact_files["parser_result_json"])))
    core_rows = _read_json(Path(str(artifact_files["staging_core_metrics_json"])))
    relationship_rows = _read_json(
        Path(str(artifact_files["staging_manager_fund_vehicle_relationships_json"]))
    )
    if not isinstance(core_rows, list) or not all(isinstance(row, dict) for row in core_rows):
        raise ValueError("staging_core_metrics_json must contain a list of objects")
    if not isinstance(relationship_rows, list) or not all(
        isinstance(row, dict) for row in relationship_rows
    ):
        raise ValueError("staging_manager_fund_vehicle_relationships_json must contain a list")
    coverage = _read_json(Path(str(artifact_files["coverage_summary_json"])))
    component_report = _read_json(Path(str(artifact_files["component_coverage_report_json"])))
    warning_path = Path(str(artifact_files["extraction_warnings_json"]))
    recorded = recorded_at or datetime.now(UTC).replace(microsecond=0).isoformat()
    git_sha = _git_sha(repo_root)

    identity_maps = build_identity_maps(
        pilot_input=pilot_manifest["input"],
        core_rows=core_rows,
        relationship_rows=relationship_rows,
        published_at=recorded,
    )
    identity_refs = identity_refs_from_maps(identity_maps)
    pension_refs = [ref for ref in identity_refs if ref.startswith("pension:")]
    if len(pension_refs) != 1:
        raise ValueError("one-PDF run must publish exactly one pension identity")
    source_evidence = parser_result.get("source_evidence")
    if not isinstance(source_evidence, dict):
        raise ValueError("parser result missing source_evidence object")
    evidence_objects = build_evidence_objects(
        run_id=run_id,
        core_rows=core_rows,
        source_evidence=source_evidence,
        pension_entity_ref=pension_refs[0],
    )
    evidence_refs = [str(item["evidence_id"]) for item in evidence_objects]

    evidence_dir = output_dir / "evidence"
    for evidence_object in evidence_objects:
        evidence_id = str(evidence_object["evidence_id"])
        evidence_path = evidence_dir / f"{evidence_id.removeprefix('evidence:')}.json"
        _write_json(evidence_path, evidence_object)
        artifact_ids.append(evidence_id)
        artifact_entries.append(
            _artifact_entry(
                artifact_id=evidence_id,
                name=f"evidence-{evidence_object['fact_ref']}",
                kind="evidence",
                path=evidence_path,
                root=output_dir,
            )
        )

    identity_dir = output_dir / "identity"
    for identity_map in identity_maps:
        entity_type = str(identity_map["entity_type"])
        artifact_id = f"identity-map:{entity_type}"
        identity_path = identity_dir / f"identity-map-{entity_type}.json"
        _write_json(identity_path, identity_map)
        artifact_ids.append(artifact_id)
        artifact_entries.append(
            _artifact_entry(
                artifact_id=artifact_id,
                name=f"identity-map-{entity_type}",
                kind="data",
                path=identity_path,
                root=output_dir,
            )
        )

    manifest = {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "run_id": run_id,
        "tool": TOOL,
        "git_sha": git_sha,
        "created_at": recorded,
        "artifacts": artifact_entries,
    }
    manifest_path = output_dir / "manifest.json"
    _write_json(manifest_path, manifest)

    envelope = {
        "schema_version": RUN_SCHEMA_VERSION,
        "repo": REPO,
        "tool": TOOL,
        "run_id": run_id,
        "status": "success" if pilot_manifest.get("ledger_status") == "success" else "error",
        "github_issue": SOURCE_ISSUE,
        "actor": {
            "kind": "ci",
            "id": "backplane-reference-run",
            "intent": "emit Pension-Data one-PDF pilot reference envelope",
        },
        "inputs": {
            "validated": True,
            "refs": [f"sha256:{_sha256(source_pdf)}"],
            "summary": {
                "plan_id": pilot_manifest["input"].get("plan_id"),
                "plan_period": pilot_manifest["input"].get("plan_period"),
                "effective_date": pilot_manifest["input"].get("effective_date"),
                "source_document_id": pilot_manifest["input"].get("source_document_id"),
                "source_pdf_sha256": _sha256(source_pdf),
            },
        },
        "outputs": {
            "manifest_ref": "artifact:manifest.json",
            "artifact_ids": artifact_ids,
            "summary": {
                "ledger_status": pilot_manifest.get("ledger_status"),
                "document_outcome_count": pilot_manifest.get("document_outcome_count"),
                "staging_core_metric_count": coverage.get("staging_core_metric_count"),
                "relationship_row_count": coverage.get("relationship_row_count"),
                "component_expected_count": component_report.get("expected_component_count"),
            },
        },
        "provenance": {
            "tool_version": _tool_version(),
            "git_sha": git_sha,
            "python_version": platform.python_version(),
            "platform": platform.platform(),
            "code_version": git_sha or "unknown",
        },
        "cost": {"usd": 0, "input_tokens": 0, "output_tokens": 0},
        "latency": {"wall_ms": wall_ms},
        "warnings": _warning_summary(warning_path),
        "evidence_refs": evidence_refs,
        "identity_refs": identity_refs,
        "data_quality": {
            "overall_status": "ok" if coverage.get("has_required_funded_metrics") else "fail",
            "dropped_rows": 0,
            "coerced_rows": 0,
            "conflict": bool(parser_result.get("escalation_required")),
            "findings": [
                {
                    "code": "missing-required-metrics",
                    "count": len(coverage.get("missing_required_metrics", [])),
                },
                {
                    "code": "component-coverage",
                    "valid": bool(component_report.get("is_valid")),
                },
            ],
        },
        "recorded_at": recorded,
    }
    run_path = output_dir / "run.json"
    _write_json(run_path, envelope)
    return {"run_json": run_path, "manifest": manifest_path}


def main(argv: list[str] | None = None) -> int:
    import argparse
    import time

    from pension_data.ops.one_pdf_pilot import OnePdfPilotInput, run_one_pdf_pilot

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path, default=Path("artifacts/reference"))
    parser.add_argument(
        "--fixture",
        type=Path,
        default=Path("tests/parser/fixtures/calpers_fy2024_excerpt.pdf"),
    )
    parser.add_argument("--run-id", default="pension-data-one-pdf-reference")
    args = parser.parse_args(argv)

    start = time.perf_counter()
    result = run_one_pdf_pilot(
        pilot_input=OnePdfPilotInput(
            pdf_path=args.fixture,
            plan_id="CA-PERS",
            plan_period="FY2024",
            effective_date="2024-06-30",
            ingestion_date="2026-01-01",
        ),
        output_root=args.out,
        run_id=args.run_id,
    )
    elapsed_ms = (time.perf_counter() - start) * 1000
    paths = build_backplane_reference_run(
        pilot_manifest_path=Path(result["run_manifest_json"]),
        output_dir=args.out,
        wall_ms=elapsed_ms,
        repo_root=Path.cwd(),
    )
    print(json.dumps({key: str(path) for key, path in paths.items()}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
