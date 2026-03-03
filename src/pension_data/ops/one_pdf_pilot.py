"""One-PDF pilot harness for deterministic parser/orchestration artifact generation."""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import cast

from pension_data.db.models.artifacts import RawArtifactRecord
from pension_data.extract.actuarial.metrics import RawFundedActuarialInput
from pension_data.extract.persistence import write_extraction_persistence_artifacts
from pension_data.normalize.financial_units import UnitScale
from pension_data.ops.document_orchestration import (
    DocumentOrchestrationState,
    SourceDocumentJobItem,
    run_document_orchestration,
)
from pension_data.parser.pdf_pipeline import PDFParserInput, parse_pdf_to_funded_input


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
    pdf_path: Path,
) -> str:
    fingerprint = hashlib.sha256(
        f"{plan_id}|{plan_period}|{pdf_path.resolve()}".encode()
    ).hexdigest()[:16]
    return f"pilot:{plan_id}:{plan_period}:{fingerprint}"


def _coverage_summary(
    *,
    parser_result: dict[str, object],
    orchestration_artifacts: dict[str, object],
) -> dict[str, object]:
    published_rows = cast(list[dict[str, object]], orchestration_artifacts["published_rows"])
    review_queue_rows = cast(list[dict[str, object]], orchestration_artifacts["review_queue_rows"])
    core_rows = cast(list[dict[str, object]], orchestration_artifacts["staging_core_metrics_rows"])
    relationship_rows = cast(
        list[dict[str, object]],
        orchestration_artifacts["staging_manager_fund_vehicle_relationship_rows"],
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
        pdf_path=pdf_path,
    )
    fetched_at = pilot_input.fetched_at or _utc_now_iso()
    effective_run_id = run_id or f"one-pdf-{datetime.now(UTC).strftime('%Y%m%d%H%M%S')}"

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

    persistence_paths = write_extraction_persistence_artifacts(
        {
            "persistence_contract": orchestration_artifacts["persistence_contract"],
            "staging_core_metrics_rows": orchestration_artifacts["staging_core_metrics_rows"],
            "staging_manager_fund_vehicle_relationship_rows": orchestration_artifacts[
                "staging_manager_fund_vehicle_relationship_rows"
            ],
            "extraction_warning_rows": orchestration_artifacts["extraction_warning_rows"],
        },
        output_root=run_root,
    )

    coverage_summary = _coverage_summary(
        parser_result=parser_payload,
        orchestration_artifacts=orchestration_artifacts,
    )
    coverage_json = run_root / "coverage" / "component_coverage_summary.json"
    _write_json(coverage_json, coverage_summary)

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
            "persistence_contract_json": persistence_paths["persistence_contract_json"],
            "staging_core_metrics_json": persistence_paths["staging_core_metrics_json"],
            "staging_manager_fund_vehicle_relationships_json": persistence_paths[
                "staging_manager_fund_vehicle_relationships_json"
            ],
            "extraction_warnings_json": persistence_paths["extraction_warnings_json"],
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
        "persistence_contract_json": persistence_paths["persistence_contract_json"],
        "staging_core_metrics_json": persistence_paths["staging_core_metrics_json"],
        "staging_manager_fund_vehicle_relationships_json": persistence_paths[
            "staging_manager_fund_vehicle_relationships_json"
        ],
        "extraction_warnings_json": persistence_paths["extraction_warnings_json"],
    }
