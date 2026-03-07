"""Component completeness coverage gating for one-PDF extraction artifacts."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Literal

ComponentDatasetStatus = Literal["present", "partial", "not_disclosed"]

ALLOWED_COMPONENT_STATUSES: tuple[ComponentDatasetStatus, ...] = (
    "present",
    "partial",
    "not_disclosed",
)

CORE_SCHEMA_COMPONENTS: tuple[str, ...] = (
    "pension_plan",
    "source_document",
    "document_version",
    "plan_period",
    "metric_observation",
    "evidence_reference",
    "investment_exposure",
    "manager_entity",
    "fund_vehicle_entity",
    "plan_manager_fund_position",
    "manager_lifecycle_event",
    "benchmark_definition",
    "benchmark_version",
    "performance_observation",
    "fee_observation",
    "risk_exposure_observation",
    "consultant_entity",
    "plan_consultant_engagement",
    "consultant_recommendation",
)


def _normalized_refs_from_rows(rows: Sequence[Mapping[str, object]]) -> tuple[str, ...]:
    refs: list[str] = []
    for row in rows:
        values = row.get("evidence_refs")
        if not isinstance(values, list):
            continue
        for value in values:
            if not isinstance(value, str):
                continue
            normalized = value.strip()
            if not normalized or normalized in refs:
                continue
            refs.append(normalized)
    return tuple(sorted(refs))


def _component_row(
    *,
    component_name: str,
    status: ComponentDatasetStatus,
    row_count: int,
    plan_id: str | None,
    plan_period: str | None,
    effective_date: str | None,
    ingestion_date: str | None,
    source_document_id: str | None,
    evidence_refs: tuple[str, ...],
    notes: str,
) -> dict[str, object]:
    confidence: float | None
    if status == "present":
        confidence = 1.0
    elif status == "partial":
        confidence = 0.5
    else:
        confidence = None
    return {
        "component_name": component_name,
        "status": status,
        "row_count": row_count,
        "plan_id": plan_id,
        "plan_period": plan_period,
        "effective_date": effective_date,
        "ingestion_date": ingestion_date,
        "source_document_id": source_document_id,
        "confidence": confidence,
        "evidence_refs": list(evidence_refs),
        "notes": notes,
    }


def build_component_datasets(
    *,
    persisted_core_metrics: Sequence[Mapping[str, object]],
    relationship_rows: Sequence[Mapping[str, object]],
    warning_rows: Sequence[Mapping[str, object]],
    plan_id: str,
    plan_period: str,
    effective_date: str,
    ingestion_date: str,
    source_document_id: str,
) -> dict[str, list[dict[str, object]]]:
    """Build deterministic one-row coverage datasets for each core schema component."""
    all_rows = [*persisted_core_metrics, *relationship_rows, *warning_rows]
    shared_refs = _normalized_refs_from_rows(all_rows)
    has_payload = bool(persisted_core_metrics or relationship_rows or warning_rows)

    metric_families = {
        str(row.get("metric_family"))
        for row in persisted_core_metrics
        if isinstance(row.get("metric_family"), str)
    }
    unique_managers = {
        str(row.get("manager_name")).strip()
        for row in relationship_rows
        if isinstance(row.get("manager_name"), str) and str(row.get("manager_name")).strip()
    }
    unique_funds = {
        str(row.get("fund_name")).strip()
        for row in relationship_rows
        if isinstance(row.get("fund_name"), str) and str(row.get("fund_name")).strip()
    }

    component_rows: dict[str, list[dict[str, object]]] = {}

    for component in CORE_SCHEMA_COMPONENTS:
        status: ComponentDatasetStatus = "not_disclosed"
        row_count = 0
        notes = "no rows for component in current one-pdf artifact set"

        if component in {"pension_plan", "source_document", "document_version", "plan_period"}:
            if has_payload:
                status = "present"
                row_count = 1
                notes = "document-level metadata available from run artifacts"
        elif component == "metric_observation":
            row_count = len(persisted_core_metrics)
            if row_count > 0:
                status = "present"
                notes = "metric rows emitted"
        elif component == "evidence_reference":
            row_count = len(shared_refs)
            if row_count > 0:
                status = "present"
                notes = "evidence references emitted"
        elif component == "investment_exposure":
            row_count = len(relationship_rows)
            if row_count > 0:
                status = "present"
                notes = "investment relationship rows emitted"
        elif component == "manager_entity":
            row_count = len(unique_managers)
            if row_count > 0:
                status = "present"
                notes = "manager entities inferred from relationship rows"
        elif component == "fund_vehicle_entity":
            row_count = len(unique_funds)
            if row_count > 0:
                status = "present"
                notes = "fund/vehicle entities inferred from relationship rows"
        elif component == "plan_manager_fund_position":
            row_count = len(relationship_rows)
            if row_count > 0:
                status = "present"
                notes = "plan-manager-fund position rows emitted"
        elif component == "manager_lifecycle_event":
            if relationship_rows:
                status = "partial"
                row_count = len(relationship_rows)
                notes = "manager relationships available; lifecycle extraction pending"
        elif component in {"benchmark_definition", "benchmark_version"}:
            if persisted_core_metrics:
                status = "partial"
                row_count = 1
                notes = "benchmark metadata present on metric rows"
        elif component == "performance_observation":
            row_count = sum(
                1 for row in persisted_core_metrics if row.get("metric_family") == "performance"
            )
            if row_count > 0:
                status = "present"
                notes = "performance rows emitted"
            elif persisted_core_metrics:
                status = "partial"
                notes = "metric payload present; dedicated performance extraction pending"
        elif component == "fee_observation":
            row_count = sum(
                1 for row in persisted_core_metrics if row.get("metric_family") == "fee"
            )
            if row_count > 0:
                status = "present"
                notes = "fee rows emitted"
        elif component == "risk_exposure_observation":
            row_count = sum(
                1 for row in persisted_core_metrics if row.get("metric_family") == "risk"
            )
            if row_count > 0:
                status = "present"
                notes = "risk exposure rows emitted"
        elif component in {
            "consultant_entity",
            "plan_consultant_engagement",
            "consultant_recommendation",
        } and metric_families.intersection({"consultant", "governance"}):
            status = "partial"
            row_count = 1
            notes = "governance metrics present; dedicated consultant datasets pending"

        refs = shared_refs if status in {"present", "partial"} else ()
        component_rows[component] = [
            _component_row(
                component_name=component,
                status=status,
                row_count=row_count,
                plan_id=plan_id,
                plan_period=plan_period,
                effective_date=effective_date,
                ingestion_date=ingestion_date,
                source_document_id=source_document_id,
                evidence_refs=refs,
                notes=notes,
            )
        ]

    return component_rows


def validate_component_coverage(
    *,
    component_datasets: Mapping[str, Sequence[Mapping[str, object]]],
    expected_components: Sequence[str] = CORE_SCHEMA_COMPONENTS,
) -> dict[str, object]:
    """Validate component datasets and return a deterministic machine-readable report."""
    expected = tuple(expected_components)
    observed_components = sorted(component_datasets.keys())
    unexpected_components = sorted(
        component for component in observed_components if component not in expected
    )
    missing_components = sorted(
        component for component in expected if component not in component_datasets
    )

    invalid_state_rows: list[dict[str, object]] = []
    metadata_violations: list[dict[str, object]] = []
    component_status: dict[str, str] = {}
    status_counts = {"present": 0, "partial": 0, "not_disclosed": 0}

    for component in expected:
        rows = component_datasets.get(component)
        if rows is None or len(rows) == 0:
            component_status[component] = "missing"
            continue

        statuses: list[str] = []
        for index, row in enumerate(rows):
            status = row.get("status")
            if status not in ALLOWED_COMPONENT_STATUSES:
                invalid_state_rows.append(
                    {
                        "component": component,
                        "row_index": index,
                        "status": status,
                        "message": "status must be present, partial, or not_disclosed",
                    }
                )
                continue

            status_token = str(status)
            statuses.append(status_token)
            status_counts[status_token] += 1

            evidence_refs = row.get("evidence_refs")
            if status_token in {"present", "partial"}:
                for key in ("plan_id", "plan_period", "source_document_id"):
                    value = row.get(key)
                    if not isinstance(value, str) or not value.strip():
                        metadata_violations.append(
                            {
                                "component": component,
                                "row_index": index,
                                "field": key,
                                "message": "required metadata is missing",
                            }
                        )
                if not isinstance(evidence_refs, list) or not any(
                    isinstance(ref, str) and ref.strip() for ref in evidence_refs
                ):
                    metadata_violations.append(
                        {
                            "component": component,
                            "row_index": index,
                            "field": "evidence_refs",
                            "message": "present/partial rows require non-empty evidence_refs",
                        }
                    )

        if not statuses:
            component_status[component] = "missing"
        elif len(set(statuses)) == 1:
            component_status[component] = statuses[0]
        else:
            component_status[component] = "mixed"
            invalid_state_rows.append(
                {
                    "component": component,
                    "row_index": None,
                    "status": statuses,
                    "message": "component rows must share a single status value",
                }
            )

    is_valid = (
        not missing_components
        and not unexpected_components
        and not invalid_state_rows
        and not metadata_violations
    )

    return {
        "is_valid": is_valid,
        "expected_component_count": len(expected),
        "expected_components": list(expected),
        "observed_component_count": len(observed_components),
        "observed_components": observed_components,
        "missing_components": missing_components,
        "unexpected_components": unexpected_components,
        "invalid_state_rows": invalid_state_rows,
        "metadata_violations": metadata_violations,
        "status_counts": status_counts,
        "component_status": component_status,
    }


def _read_json_object(path: Path) -> dict[str, object]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{path} must contain a JSON object")
    return payload


def _resolve_artifact_path(*, root: Path, value: object) -> Path:
    if not isinstance(value, str) or not value.strip():
        raise ValueError("artifact path value must be a non-empty string")
    path = Path(value)
    if not path.is_absolute():
        path = root / path
    return path


def load_component_datasets_from_manifest(
    *,
    component_manifest_path: Path,
) -> dict[str, list[dict[str, object]]]:
    """Load component dataset JSON payloads from an extraction-persistence manifest."""
    manifest = _read_json_object(component_manifest_path)
    datasets: dict[str, list[dict[str, object]]] = {}
    for component_name in sorted(manifest.keys()):
        if not isinstance(component_name, str) or not component_name.strip():
            raise ValueError("component manifest keys must be non-empty strings")
        component_path = _resolve_artifact_path(
            root=component_manifest_path.parent,
            value=manifest[component_name],
        )
        payload = json.loads(component_path.read_text(encoding="utf-8"))
        if not isinstance(payload, list):
            raise ValueError(f"{component_path} must contain a JSON array")
        rows: list[dict[str, object]] = []
        for index, row in enumerate(payload):
            if not isinstance(row, dict):
                raise ValueError(f"{component_path} row {index} must be a JSON object")
            rows.append(row)
        datasets[component_name] = rows
    return datasets


def build_component_coverage_report_from_manifest(
    *,
    component_manifest_path: Path,
    run_id: str | None = None,
) -> dict[str, object]:
    """Build deterministic core-component coverage report from one-PDF artifact files."""
    all_datasets = load_component_datasets_from_manifest(
        component_manifest_path=component_manifest_path
    )
    datasets = {
        component_name: rows
        for component_name, rows in all_datasets.items()
        if component_name in CORE_SCHEMA_COMPONENTS
    }
    additional_components = sorted(
        component_name
        for component_name in all_datasets
        if component_name not in CORE_SCHEMA_COMPONENTS
    )
    validation_report = validate_component_coverage(
        component_datasets=datasets,
        expected_components=CORE_SCHEMA_COMPONENTS,
    )
    component_status_map = validation_report["component_status"]
    assert isinstance(component_status_map, dict)

    per_component: list[dict[str, object]] = []
    for component_name in CORE_SCHEMA_COMPONENTS:
        rows = datasets.get(component_name, [])
        row_count = 0
        for row in rows:
            value = row.get("row_count")
            if isinstance(value, bool):
                continue
            if isinstance(value, int):
                row_count += value
        per_component.append(
            {
                "component_name": component_name,
                "status": component_status_map.get(component_name, "missing"),
                "row_count": row_count,
            }
        )

    report = {
        **validation_report,
        "run_id": run_id,
        "component_manifest_path": str(component_manifest_path),
        "additional_components": additional_components,
        "per_component": per_component,
    }
    return report
