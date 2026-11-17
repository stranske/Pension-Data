#!/usr/bin/env python3
"""Build a generated web workspace bundle from one-pdf-pilot artifacts."""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Mapping
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from workspace_contract import (  # noqa: E402
    allowed_data_origins,
    load_runtime_contract,
    validate_workspace_bundle,
)

from pension_data.export.workspace_bundle import (  # noqa: E402
    build_workspace_bundle as build_generated_workspace_bundle,
)
from pension_data.export.workspace_bundle import (  # noqa: E402
    default_review_context,
    normalize_review_context,
    write_workspace_bundle,
)
from pension_data.sources.schema import SOURCE_AUTHORITY_TIERS  # noqa: E402

CONTRACT_PATH = ROOT / "apps" / "contracts" / "runtime-contract.json"


def _load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _artifact_path(pilot_run_dir: Path, manifest: Mapping[str, Any], key: str) -> Path:
    """Resolve a manifest artifact path and require it to stay inside the pilot run.

    Deliberate-break anchor: ``origin/main`` returned absolute/existing paths without
    a run-directory containment check; restoring that early return lets foreign-run
    staging with matching identity bypass this guard.
    """
    artifact_files = manifest.get("artifact_files")
    if not isinstance(artifact_files, Mapping):
        raise ValueError("run_manifest.json missing artifact_files object")
    raw_path = artifact_files.get(key)
    if not isinstance(raw_path, str) or not raw_path.strip():
        raise ValueError(f"run_manifest.json missing artifact_files.{key}")
    resolved_run = pilot_run_dir.resolve()
    candidate = Path(raw_path.strip())
    if not candidate.is_absolute():
        run_relative = resolved_run / candidate
        if run_relative.exists():
            candidate = run_relative
        else:
            cwd_candidate = Path.cwd() / candidate
            if cwd_candidate.exists():
                candidate = cwd_candidate
            else:
                root_candidate = ROOT / candidate
                candidate = root_candidate if root_candidate.exists() else run_relative
    try:
        resolved = candidate.resolve(strict=True)
    except OSError as exc:
        raise ValueError(f"artifact_files.{key} cannot be resolved: {exc}") from exc
    if not resolved.is_relative_to(resolved_run) or not resolved.is_file():
        raise ValueError(f"artifact_files.{key} must resolve to a file inside the pilot run")
    return resolved


def _as_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _load_staging_rows(path: Path) -> list[Mapping[str, Any]]:
    payload = _load_json(path)
    rows: Any
    if isinstance(payload, list):
        rows = payload
    elif isinstance(payload, Mapping):
        rows = payload.get("rows") or payload.get("staging_core_metrics_rows")
    else:
        rows = None
    if not isinstance(rows, list) or not rows:
        raise ValueError(f"staging core metrics file has no rows: {path}")
    normalized: list[Mapping[str, Any]] = []
    for index, row in enumerate(rows):
        if not isinstance(row, Mapping):
            raise ValueError(f"staging row {index} is not an object in {path}")
        normalized.append(row)
    return normalized


def _manifest_input_identity(manifest: Mapping[str, Any]) -> tuple[str, str, str]:
    input_payload = manifest.get("input")
    if not isinstance(input_payload, Mapping):
        raise ValueError("run_manifest.json missing input object")
    plan_id = _as_text(input_payload.get("plan_id"))
    plan_period = _as_text(input_payload.get("plan_period"))
    source_document_id = _as_text(input_payload.get("source_document_id"))
    if not plan_id or not plan_period or not source_document_id:
        raise ValueError("run manifest input requires plan_id, plan_period, and source_document_id")
    return plan_id, plan_period, source_document_id


def _validate_staging_rows_match_manifest(
    rows: list[Mapping[str, Any]],
    manifest: Mapping[str, Any],
) -> None:
    plan_id, plan_period, source_document_id = _manifest_input_identity(manifest)
    for index, row in enumerate(rows):
        if _as_text(row.get("plan_id")) != plan_id:
            raise ValueError(f"staging row {index} plan_id differs from run manifest input")
        if _as_text(row.get("plan_period")) != plan_period:
            raise ValueError(f"staging row {index} plan_period differs from run manifest input")
        if _as_text(row.get("source_document_id")) != source_document_id:
            raise ValueError(
                f"staging row {index} source_document_id differs from run manifest input"
            )


def _last_updated(manifest: Mapping[str, Any]) -> str:
    input_payload = manifest.get("input")
    if isinstance(input_payload, Mapping):
        for key in ("effective_date", "ingestion_date", "fetched_at"):
            value = _as_text(input_payload.get(key))
            if value:
                return value
    return ""


def _manifest_review_context(manifest: Mapping[str, Any]) -> dict[str, Any]:
    input_payload = manifest.get("input")
    if not isinstance(input_payload, Mapping):
        raise ValueError("run_manifest.json missing input object")
    raw_input_tier = input_payload.get("source_authority_tier")
    if raw_input_tier is None:
        input_tier: str | None = None
    elif isinstance(raw_input_tier, str) and raw_input_tier.strip() in SOURCE_AUTHORITY_TIERS:
        input_tier = raw_input_tier.strip()
    else:
        raise ValueError("run manifest input source_authority_tier is invalid")

    if "review_context" not in manifest:
        legacy = default_review_context()
        if input_tier is not None:
            legacy["source_authority_tier"] = input_tier
            legacy["source_authority_status"] = "declared"
        return legacy

    raw_review_context = manifest.get("review_context")
    if not isinstance(raw_review_context, Mapping):
        raise ValueError("run_manifest.json review_context must be an object")
    normalized = normalize_review_context(raw_review_context, require_all_fields=True)
    if input_tier is not None and normalized["source_authority_tier"] != input_tier:
        raise ValueError(
            "run_manifest.json review_context source_authority_tier differs from input"
        )
    return normalized


def _relative_locator_from_file(pilot_run_dir: Path, artifact_file: Path, *, label: str) -> str:
    resolved_run = pilot_run_dir.resolve()
    try:
        resolved = artifact_file.resolve(strict=True)
    except OSError as exc:
        raise ValueError(f"{label} cannot be resolved: {exc}") from exc
    if not resolved.is_relative_to(resolved_run) or not resolved.is_file():
        raise ValueError(f"{label} must resolve to a file inside the pilot run")
    return resolved.relative_to(resolved_run).as_posix()


def _resolve_declared_locator(pilot_run_dir: Path, raw_locator: Any, *, label: str) -> str:
    locator = _as_text(raw_locator)
    if not locator:
        raise ValueError(f"{label} must be a non-empty string")
    if (
        locator.startswith("/")
        or locator.endswith("/")
        or "\\" in locator
        or "\x00" in locator
        or "://" in locator
    ):
        raise ValueError(f"{label} must be a safe relative path inside the pilot run")
    segments = locator.split("/")
    if any(segment in {"", ".", ".."} or ":" in segment for segment in segments):
        raise ValueError(f"{label} must be a safe relative path inside the pilot run")
    resolved_locator = _relative_locator_from_file(
        pilot_run_dir, pilot_run_dir.resolve() / locator, label=label
    )
    if resolved_locator != locator:
        raise ValueError(f"{label} must not traverse symlinks inside the pilot run")
    return resolved_locator


def _manifest_source_locators(
    pilot_run_dir: Path,
    manifest: Mapping[str, Any],
) -> dict[str, str]:
    _, _, source_document_id = _manifest_input_identity(manifest)
    locators: dict[str, str] = {}

    raw_locators = manifest.get("source_locators")
    if raw_locators is not None:
        if not isinstance(raw_locators, Mapping):
            raise ValueError("run_manifest.json source_locators must be an object")
        for raw_doc_id, raw_locator in raw_locators.items():
            doc_id = _as_text(raw_doc_id)
            if not doc_id:
                raise ValueError("run_manifest.json source_locators keys must be non-empty")
            locators[doc_id] = _resolve_declared_locator(
                pilot_run_dir,
                raw_locator,
                label=f"source_locators.{doc_id}",
            )

    input_payload = manifest.get("input")
    if isinstance(input_payload, Mapping) and "source_locator" in input_payload:
        input_locator = _resolve_declared_locator(
            pilot_run_dir,
            input_payload.get("source_locator"),
            label="input.source_locator",
        )
        existing = locators.get(source_document_id)
        if existing is not None and existing != input_locator:
            raise ValueError("run manifest input.source_locator conflicts with source_locators")
        locators[source_document_id] = input_locator

    artifact_files = manifest.get("artifact_files")
    if isinstance(artifact_files, Mapping) and "source_pdf" in artifact_files:
        source_pdf_path = _artifact_path(pilot_run_dir, manifest, "source_pdf")
        pdf_locator = _relative_locator_from_file(
            pilot_run_dir,
            source_pdf_path,
            label="artifact_files.source_pdf",
        )
        existing = locators.get(source_document_id)
        if existing is not None and existing != pdf_locator:
            raise ValueError("run manifest source_pdf conflicts with declared source_locator")
        locators[source_document_id] = pdf_locator

    return locators


def build_workspace_bundle(pilot_run_dir: Path) -> dict[str, Any]:
    pilot_run_dir = pilot_run_dir.resolve()
    manifest_path = pilot_run_dir / "run_manifest.json"
    if not manifest_path.is_file():
        raise ValueError(f"run manifest not found: {manifest_path}")
    manifest = _load_json(manifest_path)
    if not isinstance(manifest, Mapping):
        raise ValueError(f"run manifest must be an object: {manifest_path}")
    if _as_text(manifest.get("ledger_status")) != "success":
        raise ValueError("completed pilot ledger_status=success required")

    staging_rows = _load_staging_rows(
        _artifact_path(pilot_run_dir, manifest, "staging_core_metrics_json")
    )
    _validate_staging_rows_match_manifest(staging_rows, manifest)
    review_context = _manifest_review_context(manifest)
    source_locators = _manifest_source_locators(pilot_run_dir, manifest)
    run_id = _as_text(manifest.get("run_id")) or pilot_run_dir.name
    contract = load_runtime_contract(CONTRACT_PATH)
    bundle = build_generated_workspace_bundle(
        staging_rows,
        run_id=run_id,
        last_updated=_last_updated(manifest),
        review_context=review_context,
        source_locators=source_locators,
    )
    # This builder emits real generated data, so "fixture" is not an acceptable
    # origin for its output even though the runtime contract permits it elsewhere.
    validate_workspace_bundle(
        bundle,
        path_label="generated workspace bundle",
        accepted_origins=allowed_data_origins(contract) - {"fixture"},
        contract=contract,
    )
    return bundle


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--pilot-run-dir",
        type=Path,
        required=True,
        help="Directory containing run_manifest.json from one-pdf-pilot.",
    )
    parser.add_argument(
        "--out",
        type=Path,
        required=True,
        help="Path for the generated workspace bundle JSON.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    try:
        bundle = build_workspace_bundle(args.pilot_run_dir)
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc
    write_workspace_bundle(args.out, bundle)
    print(f"wrote generated workspace bundle: {args.out}")


if __name__ == "__main__":
    main()
