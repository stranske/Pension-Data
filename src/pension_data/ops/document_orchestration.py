"""End-to-end document ingestion orchestration with run ledger and retries."""

from __future__ import annotations

import json
from collections.abc import Callable, Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from functools import partial
from pathlib import Path
from typing import Literal, TypeVar

from pension_data.db.models.artifacts import RawArtifactRecord
from pension_data.db.models.funded_actuarial import (
    FundedActuarialStagingFact,
)
from pension_data.extract.actuarial.metrics import (
    RawFundedActuarialInput,
    extract_funded_and_actuarial_metrics,
)
from pension_data.extract.persistence import (
    WarningPersistenceContext,
    build_extraction_persistence_artifacts,
)
from pension_data.ingest.artifacts import RawArtifactIngestionInput, ingest_raw_artifacts
from pension_data.normalize.financial_units import UnitScale
from pension_data.quality.confidence import ExtractionConfidenceInput, route_confidence_rows
from pension_data.review_queue.extraction import build_extraction_review_queue

OrchestrationStage = Literal["discovery", "ingestion", "parse_extract", "validation", "publish"]
StageStatus = Literal["ok", "error", "skipped"]
RunStatus = Literal["success", "failed"]
DocumentStatus = Literal["processed", "skipped", "failed"]


@dataclass(frozen=True, slots=True)
class SourceDocumentJobItem:
    """Source-to-parser contract for one discovered pension document."""

    plan_id: str
    plan_period: str
    source_url: str
    fetched_at: str
    mime_type: str
    content_bytes: bytes
    source_document_id: str
    effective_date: str
    ingestion_date: str
    default_money_unit_scale: UnitScale


@dataclass(frozen=True, slots=True)
class OrchestrationStageMetric:
    """Per-stage run metric emitted in orchestration ledger."""

    stage: OrchestrationStage
    status: StageStatus
    record_count: int
    error_count: int
    attempt_count: int
    notes: str


@dataclass(frozen=True, slots=True)
class OrchestrationFailure:
    """Structured stage-level failure for actionable diagnostics."""

    stage: OrchestrationStage
    document_key: str | None
    attempts: int
    message: str


@dataclass(frozen=True, slots=True)
class DocumentOutcome:
    """One-document orchestration outcome with lineage + promotion stats."""

    plan_id: str
    plan_period: str
    source_url: str
    artifact_id: str | None
    supersedes_artifact_id: str | None
    status: DocumentStatus
    promoted_fact_count: int
    review_queue_count: int
    notes: str


@dataclass(frozen=True, slots=True)
class DocumentOrchestrationLedger:
    """Top-level run ledger for document orchestration executions."""

    run_id: str
    started_at: str
    completed_at: str
    status: RunStatus
    stage_metrics: tuple[OrchestrationStageMetric, ...]
    failures: tuple[OrchestrationFailure, ...]
    document_outcomes: tuple[DocumentOutcome, ...]


@dataclass(frozen=True, slots=True)
class DocumentOrchestrationState:
    """Idempotent orchestration state across runs."""

    artifact_records: tuple[RawArtifactRecord, ...] = ()
    processed_artifact_ids: tuple[str, ...] = ()
    published_fact_ids: tuple[str, ...] = ()


ParserCallable = Callable[[SourceDocumentJobItem, RawArtifactRecord], RawFundedActuarialInput]
T = TypeVar("T")


def _utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _sorted_unique(values: Sequence[str]) -> tuple[str, ...]:
    return tuple(sorted({value for value in values if value}))


def _document_key(item: SourceDocumentJobItem) -> str:
    return f"{item.plan_id}|{item.plan_period}|{item.source_url}"


def _normalize_state(state: DocumentOrchestrationState | None) -> DocumentOrchestrationState:
    return state or DocumentOrchestrationState()


def _retry(func: Callable[[], T], *, max_retries: int) -> tuple[T, int]:
    attempts = 0
    while True:
        attempts += 1
        try:
            return func(), attempts
        except Exception:  # noqa: BLE001
            if attempts > max_retries:
                raise


def _validate_job_items(items: Sequence[SourceDocumentJobItem]) -> list[SourceDocumentJobItem]:
    validated: list[SourceDocumentJobItem] = []
    for item in items:
        if not item.plan_id.strip():
            raise ValueError("plan_id is required")
        if not item.plan_period.strip():
            raise ValueError("plan_period is required")
        if not item.source_url.strip():
            raise ValueError("source_url is required")
        if not item.source_document_id.strip():
            raise ValueError("source_document_id is required")
        if not item.effective_date.strip():
            raise ValueError("effective_date is required")
        if not item.ingestion_date.strip():
            raise ValueError("ingestion_date is required")
        if not item.mime_type.strip():
            raise ValueError("mime_type is required")
        if not item.fetched_at.strip():
            raise ValueError("fetched_at is required")
        validated.append(item)
    return sorted(
        validated,
        key=lambda row: (row.plan_id, row.plan_period, row.source_url, row.fetched_at),
    )


def _active_artifact_by_key(
    records: Sequence[RawArtifactRecord],
) -> dict[tuple[str, str, str], RawArtifactRecord]:
    active: dict[tuple[str, str, str], RawArtifactRecord] = {}
    for row in records:
        if row.is_active:
            active[(row.plan_id, row.plan_period, row.source_url)] = row
    return active


def _confidence_inputs(
    rows: Sequence[FundedActuarialStagingFact],
) -> list[ExtractionConfidenceInput]:
    return [
        ExtractionConfidenceInput(
            row_id=f"{row.source_document_id}:{row.metric_name}",
            plan_id=row.plan_id,
            plan_period=row.plan_period,
            metric_name=row.metric_name,
            confidence=row.confidence,
            evidence_refs=row.evidence_refs,
        )
        for row in rows
    ]


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def run_document_orchestration(
    *,
    documents: Sequence[SourceDocumentJobItem],
    parser: ParserCallable,
    state: DocumentOrchestrationState | None = None,
    run_id: str | None = None,
    max_retries_per_stage: int = 1,
    output_root: Path | None = None,
) -> tuple[DocumentOrchestrationLedger, DocumentOrchestrationState, dict[str, object]]:
    """Run discovery->ingestion->parse/extract->validation->publish with idempotency."""
    run_started = _utc_now_iso()
    effective_run_id = run_id or f"doc-orch-{datetime.now(UTC).strftime('%Y%m%d%H%M%S')}"
    current_state = _normalize_state(state)
    failures: list[OrchestrationFailure] = []
    stage_metrics: list[OrchestrationStageMetric] = []
    document_outcomes: list[DocumentOutcome] = []
    published_rows: list[dict[str, object]] = []
    review_queue_rows: list[dict[str, object]] = []
    total_attempts = 0

    try:
        discovered = _validate_job_items(documents)
        stage_metrics.append(
            OrchestrationStageMetric(
                stage="discovery",
                status="ok",
                record_count=len(discovered),
                error_count=0,
                attempt_count=1,
                notes="validated source document job contract",
            )
        )
    except Exception as exc:  # noqa: BLE001
        failures.append(
            OrchestrationFailure(
                stage="discovery",
                document_key=None,
                attempts=1,
                message=f"{type(exc).__name__}: {exc}",
            )
        )
        stage_metrics.append(
            OrchestrationStageMetric(
                stage="discovery",
                status="error",
                record_count=0,
                error_count=1,
                attempt_count=1,
                notes="job contract validation failed",
            )
        )
        completed_at = _utc_now_iso()
        ledger = DocumentOrchestrationLedger(
            run_id=effective_run_id,
            started_at=run_started,
            completed_at=completed_at,
            status="failed",
            stage_metrics=tuple(stage_metrics),
            failures=tuple(failures),
            document_outcomes=(),
        )
        return ledger, current_state, {}

    ingestion_inputs = [
        RawArtifactIngestionInput(
            plan_id=item.plan_id,
            plan_period=item.plan_period,
            source_url=item.source_url,
            fetched_at=item.fetched_at,
            mime_type=item.mime_type,
            content_bytes=item.content_bytes,
        )
        for item in discovered
    ]
    ingested_records, ingestion_metrics = ingest_raw_artifacts(
        existing_records=list(current_state.artifact_records),
        inputs=ingestion_inputs,
    )
    stage_metrics.append(
        OrchestrationStageMetric(
            stage="ingestion",
            status="ok" if ingestion_metrics.failed_count == 0 else "error",
            record_count=len(ingested_records),
            error_count=ingestion_metrics.failed_count,
            attempt_count=1,
            notes=(
                f"new={ingestion_metrics.new_count} unchanged={ingestion_metrics.unchanged_count} "
                f"superseded={ingestion_metrics.superseded_count}"
            ),
        )
    )
    if ingestion_metrics.failed_count > 0:
        failures.append(
            OrchestrationFailure(
                stage="ingestion",
                document_key=None,
                attempts=1,
                message=f"failed_count={ingestion_metrics.failed_count}",
            )
        )

    active_artifacts = _active_artifact_by_key(ingested_records)
    processed_ids = set(current_state.processed_artifact_ids)
    published_ids = set(current_state.published_fact_ids)
    parse_successes = 0
    parse_failures = 0
    validation_warnings = 0
    publish_successes = 0
    publish_failures = 0

    for item in discovered:
        key = (item.plan_id, item.plan_period, item.source_url)
        artifact = active_artifacts.get(key)
        if artifact is None:
            parse_failures += 1
            failures.append(
                OrchestrationFailure(
                    stage="parse_extract",
                    document_key=_document_key(item),
                    attempts=1,
                    message="active artifact not found after ingestion",
                )
            )
            document_outcomes.append(
                DocumentOutcome(
                    plan_id=item.plan_id,
                    plan_period=item.plan_period,
                    source_url=item.source_url,
                    artifact_id=None,
                    supersedes_artifact_id=None,
                    status="failed",
                    promoted_fact_count=0,
                    review_queue_count=0,
                    notes="ingestion did not produce active artifact",
                )
            )
            continue

        if artifact.artifact_id in processed_ids:
            document_outcomes.append(
                DocumentOutcome(
                    plan_id=item.plan_id,
                    plan_period=item.plan_period,
                    source_url=item.source_url,
                    artifact_id=artifact.artifact_id,
                    supersedes_artifact_id=artifact.supersedes_artifact_id,
                    status="skipped",
                    promoted_fact_count=0,
                    review_queue_count=0,
                    notes="artifact already processed in prior run",
                )
            )
            continue

        try:
            raw_input, attempts = _retry(
                partial(parser, item, artifact),
                max_retries=max_retries_per_stage,
            )
            total_attempts += attempts
            funded_rows, funded_diagnostics = extract_funded_and_actuarial_metrics(
                plan_id=item.plan_id,
                plan_period=item.plan_period,
                raw=raw_input,
            )
            parse_successes += 1
        except Exception as exc:  # noqa: BLE001
            parse_failures += 1
            failures.append(
                OrchestrationFailure(
                    stage="parse_extract",
                    document_key=_document_key(item),
                    attempts=max_retries_per_stage + 1,
                    message=f"{type(exc).__name__}: {exc}",
                )
            )
            document_outcomes.append(
                DocumentOutcome(
                    plan_id=item.plan_id,
                    plan_period=item.plan_period,
                    source_url=item.source_url,
                    artifact_id=artifact.artifact_id,
                    supersedes_artifact_id=artifact.supersedes_artifact_id,
                    status="failed",
                    promoted_fact_count=0,
                    review_queue_count=0,
                    notes="parser/extractor failed after retries",
                )
            )
            continue

        confidence_decisions = route_confidence_rows(_confidence_inputs(funded_rows))
        queue_rows = build_extraction_review_queue(confidence_decisions)
        validation_warnings += len(queue_rows)
        publish_blocked = any(item.code == "missing_metric" for item in funded_diagnostics)

        if publish_blocked:
            publish_failures += 1
            failures.append(
                OrchestrationFailure(
                    stage="validation",
                    document_key=_document_key(item),
                    attempts=1,
                    message="missing required funded metrics; publish blocked",
                )
            )
            document_outcomes.append(
                DocumentOutcome(
                    plan_id=item.plan_id,
                    plan_period=item.plan_period,
                    source_url=item.source_url,
                    artifact_id=artifact.artifact_id,
                    supersedes_artifact_id=artifact.supersedes_artifact_id,
                    status="failed",
                    promoted_fact_count=0,
                    review_queue_count=len(queue_rows),
                    notes="validation blocked promotion",
                )
            )
            continue

        artifacts = build_extraction_persistence_artifacts(
            funded_actuarial_rows=funded_rows,
            funded_actuarial_diagnostics=funded_diagnostics,
            funded_warning_context=WarningPersistenceContext(
                plan_id=item.plan_id,
                plan_period=item.plan_period,
                effective_date=item.effective_date,
                ingestion_date=item.ingestion_date,
                source_document_id=item.source_document_id,
                source_url=item.source_url,
            ),
            benchmark_version="v1",
        )
        core_rows = artifacts["staging_core_metrics_rows"]
        assert isinstance(core_rows, list)

        new_rows = [row for row in core_rows if str(row.get("fact_id")) not in published_ids]
        for row in new_rows:
            published_ids.add(str(row["fact_id"]))
            published_rows.append(row)
        review_queue_rows.extend(
            {
                "queue_id": row.queue_id,
                "row_id": row.row_id,
                "plan_id": row.plan_id,
                "plan_period": row.plan_period,
                "metric_name": row.metric_name,
                "confidence": row.confidence,
                "priority": row.priority,
                "state": row.state,
            }
            for row in queue_rows
        )

        processed_ids.add(artifact.artifact_id)
        publish_successes += 1
        document_outcomes.append(
            DocumentOutcome(
                plan_id=item.plan_id,
                plan_period=item.plan_period,
                source_url=item.source_url,
                artifact_id=artifact.artifact_id,
                supersedes_artifact_id=artifact.supersedes_artifact_id,
                status="processed",
                promoted_fact_count=len(new_rows),
                review_queue_count=len(queue_rows),
                notes=(
                    "reprocessed revised artifact"
                    if artifact.supersedes_artifact_id is not None
                    else "processed active artifact"
                ),
            )
        )

    stage_metrics.append(
        OrchestrationStageMetric(
            stage="parse_extract",
            status="ok" if parse_failures == 0 else "error",
            record_count=parse_successes,
            error_count=parse_failures,
            attempt_count=max(total_attempts, 1),
            notes="parser + funded extraction per active artifact",
        )
    )
    stage_metrics.append(
        OrchestrationStageMetric(
            stage="validation",
            status="ok" if validation_warnings == 0 and publish_failures == 0 else "error",
            record_count=validation_warnings,
            error_count=publish_failures,
            attempt_count=1,
            notes="confidence review queue routing + required metric gate",
        )
    )
    stage_metrics.append(
        OrchestrationStageMetric(
            stage="publish",
            status="ok" if publish_failures == 0 else "error",
            record_count=publish_successes,
            error_count=publish_failures,
            attempt_count=1,
            notes=f"promoted {len(published_rows)} non-duplicate fact rows",
        )
    )

    next_state = DocumentOrchestrationState(
        artifact_records=tuple(ingested_records),
        processed_artifact_ids=_sorted_unique(tuple(processed_ids)),
        published_fact_ids=_sorted_unique(tuple(published_ids)),
    )
    completed_at = _utc_now_iso()
    run_status: RunStatus = "failed" if failures else "success"
    ledger = DocumentOrchestrationLedger(
        run_id=effective_run_id,
        started_at=run_started,
        completed_at=completed_at,
        status=run_status,
        stage_metrics=tuple(stage_metrics),
        failures=tuple(failures),
        document_outcomes=tuple(document_outcomes),
    )

    artifacts_payload: dict[str, object] = {
        "ledger": asdict(ledger),
        "published_rows": published_rows,
        "review_queue_rows": review_queue_rows,
        "state": asdict(next_state),
    }
    if output_root is not None:
        run_dir = output_root / "document_orchestration" / effective_run_id
        _write_json(run_dir / "ledger.json", artifacts_payload["ledger"])
        _write_json(run_dir / "published_rows.json", published_rows)
        _write_json(run_dir / "review_queue_rows.json", review_queue_rows)
        _write_json(run_dir / "state.json", artifacts_payload["state"])
        artifacts_payload["output_dir"] = str(run_dir)

    return ledger, next_state, artifacts_payload
