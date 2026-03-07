"""One-PDF pilot harness for deterministic parser/orchestration artifact generation."""

from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from types import MappingProxyType
from typing import cast

from pension_data.coverage.component_completeness import (
    build_component_coverage_report_from_manifest,
)
from pension_data.db.models.artifacts import RawArtifactRecord
from pension_data.extract.actuarial.metrics import RawFundedActuarialInput
from pension_data.extract.persistence import (
    build_schema_component_datasets,
    extraction_persistence_contract,
    write_extraction_persistence_artifacts,
)
from pension_data.normalize.financial_units import UnitScale
from pension_data.ops.document_orchestration import (
    DocumentOrchestrationState,
    SourceDocumentJobItem,
    run_document_orchestration,
)
from pension_data.parser.pdf_pipeline import PDFParserInput, parse_pdf_to_funded_input

_ALLOWED_UNIT_SCALES: tuple[UnitScale, ...] = ("usd", "thousand_usd", "million_usd", "billion_usd")

ONE_PDF_PILOT_REQUIRED_INPUT_FIELDS: tuple[str, ...] = (
    "pdf_path",
    "plan_id",
    "plan_period",
    "effective_date",
    "ingestion_date",
)
ONE_PDF_PILOT_PATH_FIELDS: tuple[str, ...] = ("pdf_path",)
ONE_PDF_PILOT_OPTIONAL_METADATA_FIELDS: tuple[str, ...] = (
    "source_url",
    "source_document_id",
    "fetched_at",
    "mime_type",
    "default_money_unit_scale",
)
ONE_PDF_PILOT_OPTIONAL_RUNTIME_FIELDS: tuple[str, ...] = ("output_root", "run_id")

ONE_PDF_PILOT_ENV_VAR_BY_FIELD: Mapping[str, str] = MappingProxyType(
    {
        "pdf_path": "ONE_PDF_PILOT_PDF_PATH",
        "plan_id": "ONE_PDF_PILOT_PLAN_ID",
        "plan_period": "ONE_PDF_PILOT_PLAN_PERIOD",
        "effective_date": "ONE_PDF_PILOT_EFFECTIVE_DATE",
        "ingestion_date": "ONE_PDF_PILOT_INGESTION_DATE",
        "source_url": "ONE_PDF_PILOT_SOURCE_URL",
        "source_document_id": "ONE_PDF_PILOT_SOURCE_DOCUMENT_ID",
        "fetched_at": "ONE_PDF_PILOT_FETCHED_AT",
        "mime_type": "ONE_PDF_PILOT_MIME_TYPE",
        "default_money_unit_scale": "ONE_PDF_PILOT_DEFAULT_MONEY_UNIT_SCALE",
        "output_root": "ONE_PDF_PILOT_OUTPUT_ROOT",
        "run_id": "ONE_PDF_PILOT_RUN_ID",
    }
)


def one_pdf_pilot_input_contract() -> dict[str, object]:
    """Canonical one-PDF pilot input contract (flags/env vars/metadata)."""
    return {
        "required_input_fields": ONE_PDF_PILOT_REQUIRED_INPUT_FIELDS,
        "path_fields": ONE_PDF_PILOT_PATH_FIELDS,
        "optional_metadata_fields": ONE_PDF_PILOT_OPTIONAL_METADATA_FIELDS,
        "optional_runtime_fields": ONE_PDF_PILOT_OPTIONAL_RUNTIME_FIELDS,
        "env_var_by_field": dict(ONE_PDF_PILOT_ENV_VAR_BY_FIELD),
        "defaults": {
            "mime_type": "application/pdf",
            "default_money_unit_scale": "million_usd",
            "output_root": "outputs",
        },
    }


def _resolve_str_arg(
    *,
    field: str,
    value: str | None,
    env: Mapping[str, str],
) -> str | None:
    if value is not None and value.strip():
        return value.strip()
    env_var = ONE_PDF_PILOT_ENV_VAR_BY_FIELD[field]
    from_env = env.get(env_var)
    if from_env is None or not from_env.strip():
        return None
    return from_env.strip()


def resolve_one_pdf_pilot_runtime_options(
    *,
    output_root: str | Path | None,
    run_id: str | None,
    env: Mapping[str, str] | None = None,
) -> tuple[Path, str | None]:
    """Resolve optional runtime fields with CLI-first and env fallback precedence."""
    resolved_env = env if env is not None else os.environ
    output_root_value = (
        str(output_root).strip() if output_root is not None else None
    ) or resolved_env.get(ONE_PDF_PILOT_ENV_VAR_BY_FIELD["output_root"], "outputs").strip()
    run_id_value = _resolve_str_arg(field="run_id", value=run_id, env=resolved_env)
    return Path(output_root_value), run_id_value


def resolve_one_pdf_pilot_input(
    *,
    pdf_path: str | Path | None,
    plan_id: str | None,
    plan_period: str | None,
    effective_date: str | None,
    ingestion_date: str | None,
    source_url: str | None = None,
    source_document_id: str | None = None,
    fetched_at: str | None = None,
    mime_type: str | None = None,
    default_money_unit_scale: str | UnitScale | None = None,
    env: Mapping[str, str] | None = None,
) -> OnePdfPilotInput:
    """Resolve the canonical one-PDF contract from CLI args with env var fallback."""
    resolved_env = env if env is not None else os.environ
    resolved: dict[str, str] = {}
    missing_fields: list[str] = []

    raw_pdf_path = str(pdf_path) if pdf_path is not None else None
    for field, value in (
        ("pdf_path", raw_pdf_path),
        ("plan_id", plan_id),
        ("plan_period", plan_period),
        ("effective_date", effective_date),
        ("ingestion_date", ingestion_date),
    ):
        resolved_value = _resolve_str_arg(field=field, value=value, env=resolved_env)
        if resolved_value is None:
            missing_fields.append(field)
            continue
        resolved[field] = resolved_value

    if missing_fields:
        missing_env = ", ".join(ONE_PDF_PILOT_ENV_VAR_BY_FIELD[field] for field in missing_fields)
        missing_names = ", ".join(missing_fields)
        raise ValueError(
            f"Missing required one-pdf pilot input fields: {missing_names}. "
            f"Set CLI args or env vars: {missing_env}."
        )

    resolved_source_url = _resolve_str_arg(field="source_url", value=source_url, env=resolved_env)
    resolved_source_document_id = _resolve_str_arg(
        field="source_document_id",
        value=source_document_id,
        env=resolved_env,
    )
    resolved_fetched_at = _resolve_str_arg(field="fetched_at", value=fetched_at, env=resolved_env)
    resolved_mime_type = _resolve_str_arg(field="mime_type", value=mime_type, env=resolved_env)
    if resolved_mime_type is None:
        resolved_mime_type = "application/pdf"

    unit_scale_value = _resolve_str_arg(
        field="default_money_unit_scale",
        value=str(default_money_unit_scale) if default_money_unit_scale is not None else None,
        env=resolved_env,
    )
    if unit_scale_value is None:
        unit_scale_value = "million_usd"
    if unit_scale_value not in _ALLOWED_UNIT_SCALES:
        allowed = ", ".join(_ALLOWED_UNIT_SCALES)
        raise ValueError(
            f"default_money_unit_scale must be one of [{allowed}], got: {unit_scale_value}"
        )

    return OnePdfPilotInput(
        pdf_path=Path(resolved["pdf_path"]),
        plan_id=resolved["plan_id"],
        plan_period=resolved["plan_period"],
        effective_date=resolved["effective_date"],
        ingestion_date=resolved["ingestion_date"],
        default_money_unit_scale=cast(UnitScale, unit_scale_value),
        source_url=resolved_source_url,
        source_document_id=resolved_source_document_id,
        fetched_at=resolved_fetched_at,
        mime_type=resolved_mime_type,
    )


@dataclass(frozen=True, slots=True)
class OnePdfPilotInput:
    """Input contract for one deterministic pilot run."""

    pdf_path: Path
    plan_id: str
    plan_period: str
    effective_date: str
    ingestion_date: str
    default_money_unit_scale: UnitScale = "million_usd"
    source_url: str | None = None
    source_document_id: str | None = None
    fetched_at: str | None = None
    mime_type: str = "application/pdf"


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _default_source_document_id(
    *,
    plan_id: str,
    plan_period: str,
    pdf_bytes: bytes,
) -> str:
    fingerprint = hashlib.sha256(pdf_bytes).hexdigest()[:16]
    return f"pilot:{plan_id}:{plan_period}:{fingerprint}"


def _slugify_segment(value: str) -> str:
    cleaned = "".join(ch.lower() if ch.isalnum() else "-" for ch in value.strip())
    compact = "-".join(part for part in cleaned.split("-") if part)
    return compact or "unknown"


def _default_run_id(
    *, plan_id: str, plan_period: str, effective_date: str, pdf_bytes: bytes
) -> str:
    fingerprint = hashlib.sha256(pdf_bytes).hexdigest()[:12]
    return (
        "one-pdf-"
        f"{_slugify_segment(plan_id)}-"
        f"{_slugify_segment(plan_period)}-"
        f"{_slugify_segment(effective_date)}-"
        f"{fingerprint}"
    )


def _coverage_summary(
    *,
    parser_result: Mapping[str, object],
    orchestration_artifacts: Mapping[str, object],
) -> dict[str, object]:
    published_rows = cast(list[dict[str, object]], orchestration_artifacts["published_rows"])
    review_queue_rows = cast(list[dict[str, object]], orchestration_artifacts["review_queue_rows"])
    core_rows = cast(
        list[dict[str, object]],
        orchestration_artifacts.get("staging_core_metrics_rows", published_rows),
    )
    relationship_rows = cast(
        list[dict[str, object]],
        orchestration_artifacts.get(
            "staging_manager_fund_vehicle_relationship_rows",
            orchestration_artifacts.get("manager_relationship_rows", []),
        ),
    )
    warning_rows = cast(list[dict[str, object]], orchestration_artifacts["extraction_warning_rows"])
    missing_metrics = cast(list[str], parser_result["missing_metrics"])

    return {
        "missing_required_metrics": missing_metrics,
        "has_required_funded_metrics": len(missing_metrics) == 0,
        "escalation_required": bool(parser_result["escalation_required"]),
        "published_row_count": len(published_rows),
        "review_queue_row_count": len(review_queue_rows),
        "staging_core_metric_count": len(core_rows),
        "relationship_row_count": len(relationship_rows),
        "warning_row_count": len(warning_rows),
    }


def run_one_pdf_pilot(
    *,
    pilot_input: OnePdfPilotInput,
    output_root: Path,
    run_id: str | None = None,
    state: DocumentOrchestrationState | None = None,
) -> dict[str, str]:
    """Run parser + orchestration for one PDF and persist a deterministic artifact contract."""
    if not pilot_input.plan_id.strip():
        raise ValueError("plan_id is required")
    if not pilot_input.plan_period.strip():
        raise ValueError("plan_period is required")
    if not pilot_input.effective_date.strip():
        raise ValueError("effective_date is required")
    if not pilot_input.ingestion_date.strip():
        raise ValueError("ingestion_date is required")
    if not pilot_input.mime_type.strip():
        raise ValueError("mime_type is required")

    pdf_path = pilot_input.pdf_path.expanduser().resolve()
    if not pdf_path.exists():
        raise FileNotFoundError(f"PDF not found: {pdf_path}")
    pdf_bytes = pdf_path.read_bytes()

    source_url = pilot_input.source_url or pdf_path.as_uri()
    source_document_id = pilot_input.source_document_id or _default_source_document_id(
        plan_id=pilot_input.plan_id,
        plan_period=pilot_input.plan_period,
        pdf_bytes=pdf_bytes,
    )
    fetched_at = pilot_input.fetched_at or _utc_now_iso()
    effective_run_id = run_id or _default_run_id(
        plan_id=pilot_input.plan_id,
        plan_period=pilot_input.plan_period,
        effective_date=pilot_input.effective_date,
        pdf_bytes=pdf_bytes,
    )

    parser_result = parse_pdf_to_funded_input(
        PDFParserInput(
            source_document_id=source_document_id,
            source_url=source_url,
            effective_date=pilot_input.effective_date,
            ingestion_date=pilot_input.ingestion_date,
            default_money_unit_scale=pilot_input.default_money_unit_scale,
            pdf_bytes=pdf_bytes,
        )
    )
    if parser_result.raw is None or parser_result.missing_metrics:
        missing = ", ".join(parser_result.missing_metrics) or "unknown"
        raise ValueError(
            f"Unable to parse required funded metrics from PDF. Missing metrics: {missing}. "
            "If this PDF is image-only, OCR fallback must be configured."
        )

    parser_payload = {
        "stage_name": parser_result.stage_name,
        "stage_confidence": parser_result.stage_confidence,
        "missing_metrics": list(parser_result.missing_metrics),
        "escalation_required": parser_result.escalation_required,
        "actionable_flags": list(parser_result.actionable_flags),
        "provenance_refs": list(parser_result.provenance_refs),
    }

    run_root = output_root / "one_pdf_pilot" / effective_run_id
    run_root.mkdir(parents=True, exist_ok=True)
    parser_json = run_root / "parser_result.json"
    _write_json(parser_json, parser_payload)

    raw_for_orchestration = parser_result.raw

    def _parser(_: SourceDocumentJobItem, __: RawArtifactRecord) -> RawFundedActuarialInput:
        return raw_for_orchestration

    job = SourceDocumentJobItem(
        plan_id=pilot_input.plan_id,
        plan_period=pilot_input.plan_period,
        source_url=source_url,
        fetched_at=fetched_at,
        mime_type=pilot_input.mime_type,
        content_bytes=pdf_bytes,
        source_document_id=source_document_id,
        effective_date=pilot_input.effective_date,
        ingestion_date=pilot_input.ingestion_date,
        default_money_unit_scale=pilot_input.default_money_unit_scale,
    )
    ledger, next_state, orchestration_artifacts = run_document_orchestration(
        documents=[job],
        parser=_parser,
        state=state or DocumentOrchestrationState(),
        run_id=effective_run_id,
        max_retries_per_stage=1,
        output_root=run_root,
    )

    core_rows = cast(
        list[dict[str, object]],
        orchestration_artifacts.get(
            "staging_core_metrics_rows",
            orchestration_artifacts["published_rows"],
        ),
    )
    relationship_rows = cast(
        list[dict[str, object]],
        orchestration_artifacts.get(
            "staging_manager_fund_vehicle_relationship_rows",
            orchestration_artifacts.get("manager_relationship_rows", []),
        ),
    )
    warning_rows = cast(
        list[dict[str, object]],
        orchestration_artifacts["extraction_warning_rows"],
    )

    schema_component_datasets = build_schema_component_datasets(
        persisted_core_metrics=core_rows,
        relationship_rows=relationship_rows,
        warning_rows=warning_rows,
    )

    persistence_paths = write_extraction_persistence_artifacts(
        {
            "persistence_contract": orchestration_artifacts.get(
                "persistence_contract",
                extraction_persistence_contract(),
            ),
            "staging_core_metrics_rows": core_rows,
            "staging_manager_fund_vehicle_relationship_rows": relationship_rows,
            "extraction_warning_rows": warning_rows,
            "schema_component_datasets": schema_component_datasets,
        },
        output_root=run_root,
    )

    coverage_summary = _coverage_summary(
        parser_result=parser_payload,
        orchestration_artifacts=orchestration_artifacts,
    )
    coverage_json = run_root / "coverage" / "component_coverage_summary.json"
    component_coverage_report = build_component_coverage_report_from_manifest(
        component_manifest_path=Path(persistence_paths["schema_component_datasets_manifest_json"]),
        run_id=effective_run_id,
    )
    if not component_coverage_report["is_valid"]:
        raise ValueError("Schema component coverage validation failed for one-pdf run artifacts")
    component_coverage_report_json = run_root / "coverage" / "component_coverage_report.json"
    _write_json(coverage_json, coverage_summary)
    _write_json(component_coverage_report_json, component_coverage_report)

    manifest_json = run_root / "run_manifest.json"
    manifest = {
        "run_id": effective_run_id,
        "input": {
            "pdf_path": str(pdf_path),
            "plan_id": pilot_input.plan_id,
            "plan_period": pilot_input.plan_period,
            "effective_date": pilot_input.effective_date,
            "ingestion_date": pilot_input.ingestion_date,
            "source_document_id": source_document_id,
            "source_url": source_url,
            "fetched_at": fetched_at,
            "mime_type": pilot_input.mime_type,
            "default_money_unit_scale": pilot_input.default_money_unit_scale,
        },
        "ledger_status": ledger.status,
        "document_outcome_count": len(ledger.document_outcomes),
        "state": asdict(next_state),
        "artifact_files": {
            "parser_result_json": str(parser_json),
            "coverage_summary_json": str(coverage_json),
            "component_coverage_report_json": str(component_coverage_report_json),
            "persistence_contract_json": persistence_paths["persistence_contract_json"],
            "staging_core_metrics_json": persistence_paths["staging_core_metrics_json"],
            "staging_manager_fund_vehicle_relationships_json": persistence_paths[
                "staging_manager_fund_vehicle_relationships_json"
            ],
            "extraction_warnings_json": persistence_paths["extraction_warnings_json"],
            "schema_component_datasets_manifest_json": persistence_paths[
                "schema_component_datasets_manifest_json"
            ],
            "orchestration_ledger_json": str(
                run_root / "document_orchestration" / effective_run_id / "ledger.json"
            ),
            "orchestration_published_rows_json": str(
                run_root / "document_orchestration" / effective_run_id / "published_rows.json"
            ),
            "orchestration_review_queue_rows_json": str(
                run_root / "document_orchestration" / effective_run_id / "review_queue_rows.json"
            ),
            "orchestration_state_json": str(
                run_root / "document_orchestration" / effective_run_id / "state.json"
            ),
        },
    }
    _write_json(manifest_json, manifest)

    return {
        "run_id": effective_run_id,
        "run_manifest_json": str(manifest_json),
        "parser_result_json": str(parser_json),
        "coverage_summary_json": str(coverage_json),
        "component_coverage_report_json": str(component_coverage_report_json),
        "persistence_contract_json": persistence_paths["persistence_contract_json"],
        "staging_core_metrics_json": persistence_paths["staging_core_metrics_json"],
        "staging_manager_fund_vehicle_relationships_json": persistence_paths[
            "staging_manager_fund_vehicle_relationships_json"
        ],
        "extraction_warnings_json": persistence_paths["extraction_warnings_json"],
        "schema_component_datasets_manifest_json": persistence_paths[
            "schema_component_datasets_manifest_json"
        ],
        "orchestration_ledger_json": str(
            run_root / "document_orchestration" / effective_run_id / "ledger.json"
        ),
        "orchestration_published_rows_json": str(
            run_root / "document_orchestration" / effective_run_id / "published_rows.json"
        ),
        "orchestration_review_queue_rows_json": str(
            run_root / "document_orchestration" / effective_run_id / "review_queue_rows.json"
        ),
        "orchestration_state_json": str(
            run_root / "document_orchestration" / effective_run_id / "state.json"
        ),
    }
