"""End-to-end document ingestion orchestration with run ledger and retries."""

from __future__ import annotations

import json
import re
from collections.abc import Callable, Mapping, Sequence
from dataclasses import asdict, dataclass, fields, is_dataclass
from datetime import UTC, datetime
from functools import partial
from pathlib import Path
from typing import Literal, TypeVar, cast

from pension_data.coverage.component_completeness import (
    build_component_datasets,
    validate_component_coverage,
)
from pension_data.db.models.artifacts import RawArtifactRecord
from pension_data.db.models.consultant_attribution import ConsultantAttributionObservation
from pension_data.db.models.consultants import (
    ConsultantEntity,
    ConsultantRecommendation,
    PlanConsultantEngagement,
)
from pension_data.db.models.financial_flows import PlanFinancialFlow
from pension_data.db.models.funded_actuarial import (
    ExtractionDiagnostic,
    FundedActuarialStagingFact,
)
from pension_data.db.models.investment_allocations_fees import (
    AssetAllocationObservation,
    InvestmentExtractionWarning,
    ManagerFeeObservation,
)
from pension_data.db.models.investment_positions import PlanManagerFundPosition
from pension_data.db.models.manager_lifecycle import ManagerLifecycleEvent
from pension_data.db.models.risk_exposures import RiskExposureObservation
from pension_data.extract.actuarial.metrics import (
    RawFundedActuarialInput,
    extract_funded_and_actuarial_metrics,
)
from pension_data.extract.funded.financial_flows import (
    FinancialFlowWarning,
    RawFinancialFlowInput,
    extract_plan_financial_flow,
)
from pension_data.extract.governance.consultants import (
    AttributionMention,
    ConsultantExtractionWarning,
    ConsultantMention,
    RecommendationMention,
    extract_consultant_records,
)
from pension_data.extract.investment.allocation_fees import (
    AllocationDisclosureInput,
    FeeDisclosureInput,
    extract_asset_allocations,
    extract_fee_observations,
)
from pension_data.extract.investment.lifecycle import infer_lifecycle_events
from pension_data.extract.investment.manager_positions import (
    ExtractionWarning as ManagerPositionWarning,
)
from pension_data.extract.investment.manager_positions import (
    ManagerFundDisclosureInput,
    build_manager_fund_positions,
)
from pension_data.extract.investment.risk_disclosures import (
    DerivativesDisclosureInput,
    RiskExtractionDiagnostic,
    SecuritiesLendingDisclosureInput,
    extract_risk_exposure_observations,
)
from pension_data.extract.persistence import (
    PositionPersistenceContext,
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
    domain: str | None = None


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

_NUMBER_PATTERN = re.compile(r"[-+]?\d[\d,]*(?:\.\d+)?")
_DOMAIN_ORDER: tuple[str, ...] = (
    "funded_actuarial",
    "financial_flow",
    "allocation_fee",
    "risk_exposure",
    "consultant",
    "manager_position",
)


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


@dataclass(slots=True)
class _DomainStageAccumulator:
    parse_records: int = 0
    parse_errors: int = 0
    parse_attempts: int = 0
    validation_records: int = 0
    validation_errors: int = 0
    validation_attempts: int = 0
    publish_records: int = 0
    publish_errors: int = 0
    publish_attempts: int = 0


def _canonical_label(value: str) -> str:
    return " ".join(value.strip().lower().split())


def _parse_numeric_token(text: str) -> float | None:
    for match in _NUMBER_PATTERN.finditer(text):
        token = match.group(0).replace(",", "")
        if token.isdigit() and len(token) == 4 and token.startswith(("19", "20")):
            continue
        return float(token)
    return None


def _detect_money_scale(text: str, *, fallback: UnitScale) -> UnitScale:
    lowered = text.lower()
    if "billion" in lowered or " bn" in lowered:
        return "billion_usd"
    if "million" in lowered or " mm" in lowered:
        return "million_usd"
    if "thousand" in lowered or " k" in lowered:
        return "thousand_usd"
    return fallback


def _table_entries(raw: RawFundedActuarialInput) -> list[tuple[str, str, str]]:
    entries: list[tuple[str, str, str]] = []
    for row in raw.table_rows:
        label = str(row.get("label", "")).strip()
        value = str(row.get("value", "")).strip()
        evidence_ref = str(row.get("evidence_ref", "")).strip()
        if not label and not value:
            continue
        entries.append((label, value, evidence_ref))
    return entries


def _first_matching_entry(
    entries: Sequence[tuple[str, str, str]],
    aliases: Sequence[str],
) -> tuple[str, str, str] | None:
    lowered_aliases = tuple(_canonical_label(alias) for alias in aliases)
    for label, value, evidence_ref in entries:
        normalized_label = _canonical_label(label)
        if any(alias in normalized_label for alias in lowered_aliases):
            return label, value, evidence_ref
    return None


def _build_financial_flow_input(
    *,
    raw: RawFundedActuarialInput,
) -> RawFinancialFlowInput:
    entries = _table_entries(raw)
    field_aliases: dict[str, tuple[str, ...]] = {
        "beginning_aum": ("beginning aum", "beginning assets", "opening aum"),
        "ending_aum": ("ending aum", "ending assets", "closing aum"),
        "employer_contributions": ("employer contribution", "employer contributions"),
        "employee_contributions": ("employee contribution", "employee contributions"),
        "benefit_payments": ("benefit payment", "benefit payments"),
        "refunds": ("refund", "refunds"),
    }
    parsed_values: dict[str, float | None] = {}
    parsed_evidence: list[str] = []
    inferred_scale = raw.default_money_unit_scale

    for field_name, aliases in field_aliases.items():
        match = _first_matching_entry(entries, aliases)
        if match is None:
            parsed_values[field_name] = None
            continue
        label, value, evidence_ref = match
        parsed_values[field_name] = _parse_numeric_token(value)
        if evidence_ref:
            parsed_evidence.append(evidence_ref)
        inferred_scale = _detect_money_scale(f"{label} {value}", fallback=inferred_scale)

    outflow_candidates = (
        parsed_values["benefit_payments"],
        parsed_values["refunds"],
    )
    outflows_negative = any((value is not None and value < 0.0) for value in outflow_candidates)

    return RawFinancialFlowInput(
        source_document_id=raw.source_document_id,
        source_url=raw.source_url,
        effective_period=raw.effective_date,
        reported_at=raw.ingestion_date,
        unit_scale=inferred_scale,
        outflows_reported_as_negative=outflows_negative,
        beginning_aum=parsed_values["beginning_aum"],
        ending_aum=parsed_values["ending_aum"],
        employer_contributions=parsed_values["employer_contributions"],
        employee_contributions=parsed_values["employee_contributions"],
        benefit_payments=parsed_values["benefit_payments"],
        refunds=parsed_values["refunds"],
        evidence_refs=tuple(dict.fromkeys(parsed_evidence)),
    )


def _build_allocation_inputs(raw: RawFundedActuarialInput) -> list[AllocationDisclosureInput]:
    rows: list[AllocationDisclosureInput] = []
    for label, value, evidence_ref in _table_entries(raw):
        canonical = _canonical_label(label)
        if not canonical.startswith("allocation:"):
            continue
        category = label.split(":", maxsplit=1)[1].strip() or "other"
        numeric = _parse_numeric_token(value)
        if numeric is None:
            continue
        if "%" in value or "percent" in _canonical_label(value):
            rows.append(
                AllocationDisclosureInput(
                    category_label=category,
                    percent_value=numeric,
                    amount_value=None,
                    evidence_refs=(evidence_ref,) if evidence_ref else (),
                )
            )
            continue
        rows.append(
            AllocationDisclosureInput(
                category_label=category,
                percent_value=None,
                amount_value=numeric,
                amount_unit=_detect_money_scale(value, fallback=raw.default_money_unit_scale),
                evidence_refs=(evidence_ref,) if evidence_ref else (),
            )
        )
    return rows


def _fee_type_from_token(token: str) -> Literal["management_fee", "performance_fee", "other_fee"]:
    lowered = _canonical_label(token)
    if lowered in ("management_fee", "management fee"):
        return "management_fee"
    if lowered in ("performance_fee", "performance fee"):
        return "performance_fee"
    return "other_fee"


def _build_fee_inputs(raw: RawFundedActuarialInput) -> list[FeeDisclosureInput]:
    rows: list[FeeDisclosureInput] = []
    for label, value, evidence_ref in _table_entries(raw):
        canonical = _canonical_label(label)
        if not canonical.startswith("fee:"):
            continue
        tokens = [token.strip() for token in label.split(":")]
        manager_name = tokens[1] if len(tokens) > 1 and tokens[1].strip() else None
        fee_type = _fee_type_from_token(tokens[2] if len(tokens) > 2 else "other_fee")
        field = _canonical_label(tokens[3] if len(tokens) > 3 else "")
        numeric = _parse_numeric_token(value)
        rate_value = numeric if field in ("rate", "rate_pct", "percent") else None
        amount_value = numeric if field in ("amount", "amount_usd", "usd") else None
        explicit_not_disclosed = "not disclosed" in _canonical_label(value)
        rows.append(
            FeeDisclosureInput(
                manager_name=manager_name,
                fee_type=fee_type,
                rate_value=rate_value,
                amount_value=amount_value,
                amount_unit=_detect_money_scale(value, fallback=raw.default_money_unit_scale),
                explicit_not_disclosed=explicit_not_disclosed,
                evidence_refs=(evidence_ref,) if evidence_ref else (),
            )
        )
    return rows


def _build_manager_disclosure_inputs(
    *,
    raw: RawFundedActuarialInput,
    plan_id: str,
    plan_period: str,
) -> list[ManagerFundDisclosureInput]:
    grouped: dict[tuple[str | None, str | None], dict[str, object]] = {}
    for label, value, evidence_ref in _table_entries(raw):
        canonical = _canonical_label(label)
        if not canonical.startswith("position:"):
            continue
        tokens = [token.strip() for token in label.split(":")]
        manager_name = tokens[1] if len(tokens) > 1 and tokens[1].strip() else None
        fund_name = tokens[2] if len(tokens) > 2 and tokens[2].strip() else None
        field = _canonical_label(tokens[3] if len(tokens) > 3 else "")
        key = (manager_name, fund_name)
        row = grouped.setdefault(
            key,
            {
                "commitment": None,
                "unfunded": None,
                "market_value": None,
                "explicit_not_disclosed": False,
                "known_not_invested": False,
                "evidence_refs": [],
            },
        )
        if evidence_ref:
            cast(list[str], row["evidence_refs"]).append(evidence_ref)
        if field in ("commitment", "unfunded", "market_value"):
            row[field] = _parse_numeric_token(value)
            continue
        if field == "not_disclosed":
            row["explicit_not_disclosed"] = True
            continue
        if field == "known_not_invested":
            row["known_not_invested"] = True

    disclosures: list[ManagerFundDisclosureInput] = []
    for (manager_name, fund_name), payload in sorted(
        grouped.items(),
        key=lambda item: (_canonical_label(item[0][0] or ""), _canonical_label(item[0][1] or "")),
    ):
        evidence_refs = tuple(dict.fromkeys(cast(list[str], payload["evidence_refs"])))
        disclosures.append(
            ManagerFundDisclosureInput(
                plan_id=plan_id,
                plan_period=plan_period,
                manager_name=manager_name,
                fund_name=fund_name,
                commitment=cast(float | None, payload["commitment"]),
                unfunded=cast(float | None, payload["unfunded"]),
                market_value=cast(float | None, payload["market_value"]),
                explicit_not_disclosed=cast(bool, payload["explicit_not_disclosed"]),
                known_not_invested=cast(bool, payload["known_not_invested"]),
                evidence_refs=evidence_refs,
            )
        )
    return disclosures


def _risk_kind_from_tokens(
    tokens: Sequence[str],
) -> Literal["derivatives", "securities_lending"] | None:
    if len(tokens) < 2:
        return None
    normalized = _canonical_label(tokens[1]).replace(" ", "_")
    if normalized in ("derivatives", "securities_lending"):
        return cast(Literal["derivatives", "securities_lending"], normalized)
    return None


def _build_risk_inputs(
    raw: RawFundedActuarialInput,
) -> tuple[list[DerivativesDisclosureInput], list[SecuritiesLendingDisclosureInput]]:
    derivatives: dict[str, dict[str, object]] = {}
    securities: dict[str, dict[str, object]] = {}
    for label, value, evidence_ref in _table_entries(raw):
        canonical = _canonical_label(label)
        if not canonical.startswith("risk:"):
            continue
        tokens = [token.strip() for token in label.split(":")]
        risk_kind = _risk_kind_from_tokens(tokens)
        if risk_kind is None:
            continue
        topic = tokens[2].strip() if len(tokens) > 2 and tokens[2].strip() else "not_specified"
        field = _canonical_label(tokens[3] if len(tokens) > 3 else "")
        bucket = derivatives if risk_kind == "derivatives" else securities
        row = bucket.setdefault(
            topic,
            {
                "policy_limit_value": None,
                "realized_exposure_value": None,
                "collateral_value": None,
                "value_unit": _detect_money_scale(value, fallback=raw.default_money_unit_scale),
                "as_reported_text": value,
                "source_kind": "table",
                "confidence": 0.84,
                "evidence_refs": [],
            },
        )
        if evidence_ref:
            cast(list[str], row["evidence_refs"]).append(evidence_ref)
        if field in ("policy_limit", "realized_exposure", "collateral"):
            parsed = _parse_numeric_token(value)
            if field == "policy_limit":
                row["policy_limit_value"] = parsed
            elif field == "realized_exposure":
                row["realized_exposure_value"] = parsed
            else:
                row["collateral_value"] = parsed

    derivative_rows = [
        DerivativesDisclosureInput(
            usage_type=topic,
            policy_limit_value=cast(float | None, payload["policy_limit_value"]),
            realized_exposure_value=cast(float | None, payload["realized_exposure_value"]),
            value_unit=cast(
                Literal["usd", "thousand_usd", "million_usd", "billion_usd", "ratio"],
                payload["value_unit"],
            ),
            as_reported_text=cast(str, payload["as_reported_text"]),
            source_kind="table",
            confidence=cast(float, payload["confidence"]),
            evidence_refs=tuple(dict.fromkeys(cast(list[str], payload["evidence_refs"]))),
            source_url=raw.source_url,
        )
        for topic, payload in sorted(
            derivatives.items(), key=lambda item: _canonical_label(item[0])
        )
    ]
    securities_rows = [
        SecuritiesLendingDisclosureInput(
            program_name=topic,
            policy_limit_value=cast(float | None, payload["policy_limit_value"]),
            realized_exposure_value=cast(float | None, payload["realized_exposure_value"]),
            collateral_value=cast(float | None, payload["collateral_value"]),
            value_unit=cast(
                Literal["usd", "thousand_usd", "million_usd", "billion_usd", "ratio"],
                payload["value_unit"],
            ),
            as_reported_text=cast(str, payload["as_reported_text"]),
            source_kind="table",
            confidence=cast(float, payload["confidence"]),
            evidence_refs=tuple(dict.fromkeys(cast(list[str], payload["evidence_refs"]))),
            source_url=raw.source_url,
        )
        for topic, payload in sorted(securities.items(), key=lambda item: _canonical_label(item[0]))
    ]
    return derivative_rows, securities_rows


def _build_consultant_mentions(
    raw: RawFundedActuarialInput,
) -> tuple[list[ConsultantMention], list[RecommendationMention], list[AttributionMention]]:
    consultant_mentions: list[ConsultantMention] = []
    recommendation_mentions: list[RecommendationMention] = []
    attribution_mentions: list[AttributionMention] = []

    for block in raw.text_blocks:
        line = block.strip()
        lowered = _canonical_label(line)
        if lowered.startswith("consultant:"):
            payload = line.split(":", maxsplit=1)[1]
            tokens = [token.strip() for token in payload.split("|")]
            consultant_mentions.append(
                ConsultantMention(
                    consultant_name=tokens[0] if tokens and tokens[0] else None,
                    role_description=tokens[1] if len(tokens) > 1 and tokens[1] else None,
                    confidence=0.86,
                    evidence_refs=(),
                    source_url=raw.source_url,
                )
            )
            continue
        if lowered.startswith("recommendation:"):
            payload = line.split(":", maxsplit=1)[1]
            tokens = [token.strip() for token in payload.split("|")]
            recommendation_mentions.append(
                RecommendationMention(
                    consultant_name=tokens[0] if tokens and tokens[0] else None,
                    topic=tokens[1] if len(tokens) > 1 and tokens[1] else None,
                    recommendation_text=tokens[2] if len(tokens) > 2 and tokens[2] else None,
                    board_decision_status=tokens[3] if len(tokens) > 3 and tokens[3] else None,
                    confidence=0.8,
                    evidence_refs=(),
                    source_url=raw.source_url,
                )
            )
            continue
        if lowered.startswith("attribution:"):
            payload = line.split(":", maxsplit=1)[1]
            tokens = [token.strip() for token in payload.split("|")]
            attribution_mentions.append(
                AttributionMention(
                    consultant_name=tokens[0] if tokens and tokens[0] else None,
                    topic=tokens[1] if len(tokens) > 1 and tokens[1] else None,
                    observed_outcome=tokens[2] if len(tokens) > 2 and tokens[2] else None,
                    strength=tokens[3] if len(tokens) > 3 and tokens[3] else None,
                    confidence=0.76,
                    evidence_refs=(),
                    source_url=raw.source_url,
                )
            )

    return consultant_mentions, recommendation_mentions, attribution_mentions


def _warning_confidence_input(
    *,
    row_id: str,
    plan_id: str,
    plan_period: str,
    metric_name: str,
    confidence: float,
    evidence_refs: tuple[str, ...] = (),
) -> ExtractionConfidenceInput:
    return ExtractionConfidenceInput(
        row_id=row_id,
        plan_id=plan_id,
        plan_period=plan_period,
        metric_name=metric_name,
        confidence=confidence,
        evidence_refs=evidence_refs,
    )


def _jsonable(value: object) -> object:
    if is_dataclass(value):
        return {field.name: _jsonable(getattr(value, field.name)) for field in fields(value)}
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, dict):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_jsonable(item) for item in value]
    if isinstance(value, list):
        return [_jsonable(item) for item in value]
    return value


def _serialize_dataclass_rows(rows: Sequence[object]) -> list[dict[str, object]]:
    serialized: list[dict[str, object]] = []
    for row in rows:
        payload = _jsonable(row)
        assert isinstance(payload, dict)
        serialized.append(cast(dict[str, object], payload))
    return sorted(serialized, key=lambda item: json.dumps(item, sort_keys=True))


def _append_domain_metrics(
    *,
    stage_metrics: list[OrchestrationStageMetric],
    stage: OrchestrationStage,
    accumulators: dict[str, _DomainStageAccumulator],
) -> None:
    for domain in _DOMAIN_ORDER:
        counters = accumulators[domain]
        if stage == "parse_extract":
            record_count = counters.parse_records
            error_count = counters.parse_errors
            attempt_count = counters.parse_attempts
            notes = "domain extractor execution"
        elif stage == "validation":
            record_count = counters.validation_records
            error_count = counters.validation_errors
            attempt_count = counters.validation_attempts
            notes = "domain warning routing + quality checks"
        else:
            record_count = counters.publish_records
            error_count = counters.publish_errors
            attempt_count = counters.publish_attempts
            notes = "domain publish row aggregation"
        stage_metrics.append(
            OrchestrationStageMetric(
                stage=stage,
                status="ok" if error_count == 0 else "error",
                record_count=record_count,
                error_count=error_count,
                attempt_count=attempt_count,
                notes=notes,
                domain=domain,
            )
        )


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
    component_coverage_reports: list[dict[str, object]] = []
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
    domain_accumulators = {domain: _DomainStageAccumulator() for domain in _DOMAIN_ORDER}
    financial_flow_rows: list[dict[str, object]] = []
    risk_exposure_rows: list[dict[str, object]] = []
    consultant_entity_rows: list[dict[str, object]] = []
    consultant_engagement_rows: list[dict[str, object]] = []
    consultant_recommendation_rows: list[dict[str, object]] = []
    consultant_attribution_rows: list[dict[str, object]] = []
    lifecycle_event_rows: list[dict[str, object]] = []
    manager_relationship_rows: list[dict[str, object]] = []
    extraction_warning_rows: list[dict[str, object]] = []

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

        funded_rows: list[FundedActuarialStagingFact] = []
        funded_diagnostics: list[ExtractionDiagnostic] = []
        funded_rows_for_publish: list[FundedActuarialStagingFact] = []
        financial_flow_row: PlanFinancialFlow | None = None
        financial_flow_warnings: list[FinancialFlowWarning] = []
        allocation_rows: list[AssetAllocationObservation] = []
        fee_rows: list[ManagerFeeObservation] = []
        investment_warnings: list[InvestmentExtractionWarning] = []
        risk_rows: list[RiskExposureObservation] = []
        risk_diagnostics: list[RiskExtractionDiagnostic] = []
        manager_position_rows: list[PlanManagerFundPosition] = []
        manager_position_warnings: list[ManagerPositionWarning] = []
        lifecycle_rows: list[ManagerLifecycleEvent] = []
        lifecycle_warnings: list[ManagerPositionWarning] = []
        consultant_entities: list[ConsultantEntity] = []
        consultant_engagements: list[PlanConsultantEngagement] = []
        consultant_recommendations: list[ConsultantRecommendation] = []
        consultant_attributions: list[ConsultantAttributionObservation] = []
        consultant_warnings: list[ConsultantExtractionWarning] = []
        warning_inputs: list[ExtractionConfidenceInput] = []
        domain_failures: list[str] = []

        funded_acc = domain_accumulators["funded_actuarial"]
        funded_acc.parse_attempts += 1
        try:
            funded_rows, funded_diagnostics = extract_funded_and_actuarial_metrics(
                plan_id=item.plan_id,
                plan_period=item.plan_period,
                raw=raw_input,
            )
            funded_rows_for_publish = list(funded_rows)
            funded_acc.parse_records += len(funded_rows)
        except Exception as exc:  # noqa: BLE001
            parse_failures += 1
            funded_acc.parse_errors += 1
            domain_failures.append("funded_actuarial")
            failures.append(
                OrchestrationFailure(
                    stage="parse_extract",
                    document_key=_document_key(item),
                    attempts=1,
                    message=f"domain=funded_actuarial {type(exc).__name__}: {exc}",
                )
            )

        funded_acc.validation_attempts += 1
        funded_acc.validation_records += len(funded_diagnostics)
        for diagnostic in funded_diagnostics:
            warning_inputs.append(
                _warning_confidence_input(
                    row_id=(
                        f"{raw_input.source_document_id}:funded:{diagnostic.metric_name}:{diagnostic.code}"
                    ),
                    plan_id=item.plan_id,
                    plan_period=item.plan_period,
                    metric_name=f"funded_actuarial:{diagnostic.metric_name}:{diagnostic.code}",
                    confidence=0.65 if diagnostic.code == "missing_metric" else 0.72,
                    evidence_refs=diagnostic.evidence_refs,
                )
            )

        funded_publish_blocked = any(diag.code == "missing_metric" for diag in funded_diagnostics)
        funded_acc.publish_attempts += 1
        if funded_publish_blocked:
            funded_acc.validation_errors += 1
            funded_acc.publish_errors += 1
            publish_failures += 1
            funded_rows_for_publish = []
            failures.append(
                OrchestrationFailure(
                    stage="validation",
                    document_key=_document_key(item),
                    attempts=1,
                    message="domain=funded_actuarial missing required metrics; publish blocked",
                )
            )

        flow_acc = domain_accumulators["financial_flow"]
        flow_acc.parse_attempts += 1
        try:
            flow_input = _build_financial_flow_input(raw=raw_input)
            financial_flow_row, financial_flow_warnings = extract_plan_financial_flow(
                plan_id=item.plan_id,
                plan_period=item.plan_period,
                raw=flow_input,
            )
            flow_acc.parse_records += 1
            flow_acc.validation_attempts += 1
            flow_acc.validation_records += len(financial_flow_warnings)
            flow_acc.publish_attempts += 1
            flow_acc.publish_records += 1
        except Exception as exc:  # noqa: BLE001
            parse_failures += 1
            flow_acc.parse_errors += 1
            flow_acc.publish_errors += 1
            domain_failures.append("financial_flow")
            failures.append(
                OrchestrationFailure(
                    stage="parse_extract",
                    document_key=_document_key(item),
                    attempts=1,
                    message=f"domain=financial_flow {type(exc).__name__}: {exc}",
                )
            )

        for warning in financial_flow_warnings:
            warning_inputs.append(
                _warning_confidence_input(
                    row_id=(
                        f"{raw_input.source_document_id}:flow:{warning.code}:{warning.plan_id}:{warning.plan_period}"
                    ),
                    plan_id=item.plan_id,
                    plan_period=item.plan_period,
                    metric_name=f"financial_flow:{warning.code}",
                    confidence=0.68 if warning.code == "not_disclosed" else 0.74,
                    evidence_refs=warning.evidence_refs,
                )
            )

        allocation_acc = domain_accumulators["allocation_fee"]
        allocation_acc.parse_attempts += 1
        try:
            allocation_inputs = _build_allocation_inputs(raw_input)
            fee_inputs = _build_fee_inputs(raw_input)
            allocation_rows = extract_asset_allocations(
                plan_id=item.plan_id,
                plan_period=item.plan_period,
                effective_date=item.effective_date,
                ingestion_date=item.ingestion_date,
                source_document_id=item.source_document_id,
                rows=allocation_inputs,
            )
            fee_rows, investment_warnings = extract_fee_observations(
                plan_id=item.plan_id,
                plan_period=item.plan_period,
                effective_date=item.effective_date,
                ingestion_date=item.ingestion_date,
                source_document_id=item.source_document_id,
                rows=fee_inputs,
            )
            allocation_acc.parse_records += len(allocation_rows) + len(fee_rows)
            allocation_acc.validation_attempts += 1
            allocation_acc.validation_records += len(investment_warnings)
            allocation_acc.publish_attempts += 1
            allocation_acc.publish_records += len(allocation_rows) + len(fee_rows)
        except Exception as exc:  # noqa: BLE001
            parse_failures += 1
            allocation_acc.parse_errors += 1
            allocation_acc.publish_errors += 1
            domain_failures.append("allocation_fee")
            failures.append(
                OrchestrationFailure(
                    stage="parse_extract",
                    document_key=_document_key(item),
                    attempts=1,
                    message=f"domain=allocation_fee {type(exc).__name__}: {exc}",
                )
            )

        for investment_warning in investment_warnings:
            warning_inputs.append(
                _warning_confidence_input(
                    row_id=(
                        f"{raw_input.source_document_id}:fee:{investment_warning.code}:{investment_warning.manager_name or 'not_disclosed'}"
                    ),
                    plan_id=item.plan_id,
                    plan_period=item.plan_period,
                    metric_name=f"allocation_fee:{investment_warning.code}",
                    confidence=0.71,
                    evidence_refs=investment_warning.evidence_refs,
                )
            )

        risk_acc = domain_accumulators["risk_exposure"]
        risk_acc.parse_attempts += 1
        try:
            derivative_inputs, lending_inputs = _build_risk_inputs(raw_input)
            risk_rows, risk_diagnostics = extract_risk_exposure_observations(
                plan_id=item.plan_id,
                plan_period=item.plan_period,
                derivatives_disclosures=derivative_inputs,
                securities_lending_disclosures=lending_inputs,
            )
            risk_acc.parse_records += len(risk_rows)
            risk_acc.validation_attempts += 1
            risk_acc.validation_records += len(risk_diagnostics)
            risk_acc.publish_attempts += 1
            risk_acc.publish_records += len(risk_rows)
        except Exception as exc:  # noqa: BLE001
            parse_failures += 1
            risk_acc.parse_errors += 1
            risk_acc.publish_errors += 1
            domain_failures.append("risk_exposure")
            failures.append(
                OrchestrationFailure(
                    stage="parse_extract",
                    document_key=_document_key(item),
                    attempts=1,
                    message=f"domain=risk_exposure {type(exc).__name__}: {exc}",
                )
            )

        for risk_diagnostic in risk_diagnostics:
            warning_inputs.append(
                _warning_confidence_input(
                    row_id=(
                        f"{raw_input.source_document_id}:risk:{risk_diagnostic.metric_name}:{risk_diagnostic.code}"
                    ),
                    plan_id=item.plan_id,
                    plan_period=item.plan_period,
                    metric_name=f"risk_exposure:{risk_diagnostic.metric_name}:{risk_diagnostic.code}",
                    confidence=0.69,
                    evidence_refs=risk_diagnostic.evidence_refs,
                )
            )

        consultant_acc = domain_accumulators["consultant"]
        consultant_acc.parse_attempts += 1
        try:
            consultant_mentions, recommendation_mentions, attribution_mentions = (
                _build_consultant_mentions(raw_input)
            )
            if consultant_mentions or recommendation_mentions or attribution_mentions:
                consultant_payload = extract_consultant_records(
                    plan_id=item.plan_id,
                    plan_period=item.plan_period,
                    consultant_mentions=consultant_mentions,
                    recommendation_mentions=recommendation_mentions,
                    attribution_mentions=attribution_mentions,
                )
                consultant_entities = cast(
                    list[ConsultantEntity], consultant_payload["consultant_entities"]
                )
                consultant_engagements = cast(
                    list[PlanConsultantEngagement],
                    consultant_payload["plan_consultant_engagements"],
                )
                consultant_recommendations = cast(
                    list[ConsultantRecommendation], consultant_payload["consultant_recommendations"]
                )
                consultant_attributions = cast(
                    list[ConsultantAttributionObservation],
                    consultant_payload["consultant_attribution_observations"],
                )
                consultant_warnings = cast(
                    list[ConsultantExtractionWarning], consultant_payload["warnings"]
                )
            consultant_acc.parse_records += (
                len(consultant_entities)
                + len(consultant_engagements)
                + len(consultant_recommendations)
                + len(consultant_attributions)
            )
            consultant_acc.validation_attempts += 1
            consultant_acc.validation_records += len(consultant_warnings)
            consultant_acc.publish_attempts += 1
            consultant_acc.publish_records += (
                len(consultant_entities)
                + len(consultant_engagements)
                + len(consultant_recommendations)
                + len(consultant_attributions)
            )
        except Exception as exc:  # noqa: BLE001
            parse_failures += 1
            consultant_acc.parse_errors += 1
            consultant_acc.publish_errors += 1
            domain_failures.append("consultant")
            failures.append(
                OrchestrationFailure(
                    stage="parse_extract",
                    document_key=_document_key(item),
                    attempts=1,
                    message=f"domain=consultant {type(exc).__name__}: {exc}",
                )
            )

        for consultant_warning in consultant_warnings:
            warning_inputs.append(
                _warning_confidence_input(
                    row_id=f"{raw_input.source_document_id}:consultant:{consultant_warning.code}",
                    plan_id=item.plan_id,
                    plan_period=item.plan_period,
                    metric_name=f"consultant:{consultant_warning.code}",
                    confidence=0.7,
                    evidence_refs=consultant_warning.evidence_refs,
                )
            )

        manager_acc = domain_accumulators["manager_position"]
        manager_acc.parse_attempts += 1
        try:
            manager_inputs = _build_manager_disclosure_inputs(
                raw=raw_input,
                plan_id=item.plan_id,
                plan_period=item.plan_period,
            )
            manager_position_rows, manager_position_warnings = build_manager_fund_positions(
                manager_inputs
            )
            lifecycle_rows, lifecycle_warnings = infer_lifecycle_events(
                [],
                manager_position_rows,
            )
            manager_acc.parse_records += len(manager_position_rows) + len(lifecycle_rows)
            manager_acc.validation_attempts += 1
            manager_acc.validation_records += len(manager_position_warnings) + len(
                lifecycle_warnings
            )
            manager_acc.publish_attempts += 1
            manager_acc.publish_records += len(manager_position_rows) + len(lifecycle_rows)
        except Exception as exc:  # noqa: BLE001
            parse_failures += 1
            manager_acc.parse_errors += 1
            manager_acc.publish_errors += 1
            domain_failures.append("manager_position")
            failures.append(
                OrchestrationFailure(
                    stage="parse_extract",
                    document_key=_document_key(item),
                    attempts=1,
                    message=f"domain=manager_position {type(exc).__name__}: {exc}",
                )
            )

        for position_warning in (*manager_position_warnings, *lifecycle_warnings):
            warning_inputs.append(
                _warning_confidence_input(
                    row_id=(
                        f"{raw_input.source_document_id}:position:{position_warning.code}:{position_warning.manager_name or 'not_disclosed'}:{position_warning.fund_name or 'none'}"
                    ),
                    plan_id=item.plan_id,
                    plan_period=item.plan_period,
                    metric_name=f"manager_position:{position_warning.code}",
                    confidence=0.7,
                    evidence_refs=position_warning.evidence_refs,
                )
            )

        confidence_inputs = [*_confidence_inputs(funded_rows), *warning_inputs]
        confidence_decisions = route_confidence_rows(confidence_inputs)
        queue_rows = build_extraction_review_queue(confidence_decisions)
        validation_warnings += len(queue_rows)

        artifacts = build_extraction_persistence_artifacts(
            funded_actuarial_rows=funded_rows_for_publish,
            funded_actuarial_diagnostics=funded_diagnostics,
            funded_warning_context=WarningPersistenceContext(
                plan_id=item.plan_id,
                plan_period=item.plan_period,
                effective_date=item.effective_date,
                ingestion_date=item.ingestion_date,
                source_document_id=item.source_document_id,
                source_url=item.source_url,
            ),
            allocation_rows=allocation_rows,
            fee_rows=fee_rows,
            investment_warnings=investment_warnings,
            manager_position_rows=manager_position_rows,
            manager_position_warnings=[*manager_position_warnings, *lifecycle_warnings],
            manager_position_context=PositionPersistenceContext(
                effective_date=item.effective_date,
                ingestion_date=item.ingestion_date,
                source_document_id=item.source_document_id,
                source_url=item.source_url,
            ),
            benchmark_version="v1",
        )
        core_rows = artifacts["staging_core_metrics_rows"]
        assert isinstance(core_rows, list)
        relationship_rows = artifacts["staging_manager_fund_vehicle_relationship_rows"]
        assert isinstance(relationship_rows, list)
        warning_rows = artifacts["extraction_warning_rows"]
        assert isinstance(warning_rows, list)

        component_datasets = build_component_datasets(
            persisted_core_metrics=core_rows,
            relationship_rows=relationship_rows,
            warning_rows=warning_rows,
            plan_id=item.plan_id,
            plan_period=item.plan_period,
            effective_date=item.effective_date,
            ingestion_date=item.ingestion_date,
            source_document_id=item.source_document_id,
        )
        coverage_report = validate_component_coverage(component_datasets=component_datasets)
        component_coverage_reports.append(
            {
                "document_key": _document_key(item),
                "component_coverage_report": coverage_report,
            }
        )
        if not coverage_report["is_valid"]:
            missing_components = coverage_report.get("missing_components")
            missing_names: list[str] = []
            if isinstance(missing_components, list):
                missing_names = [
                    item.strip()
                    for item in missing_components
                    if isinstance(item, str) and item.strip()
                ]
            missing_count = len(missing_names)
            missing_sample = ",".join(sorted(missing_names)[:5])
            message = (
                f"schema component coverage validation failed (missing_components={missing_count}"
            )
            if missing_sample:
                message += f"; sample={missing_sample}"
            message += ")"
            publish_failures += 1
            missing_components = coverage_report.get("missing_components", [])
            invalid_state_rows = coverage_report.get("invalid_state_rows", [])
            metadata_violations = coverage_report.get("metadata_violations", [])
            details: list[str] = []
            if isinstance(missing_components, list) and missing_components:
                details.append(f"missing_components={len(missing_components)}")
            if isinstance(invalid_state_rows, list) and invalid_state_rows:
                details.append(f"invalid_state_rows={len(invalid_state_rows)}")
            if isinstance(metadata_violations, list) and metadata_violations:
                details.append(f"metadata_violations={len(metadata_violations)}")
            detail_suffix = ", ".join(details) if details else "unknown_details"
            failures.append(
                OrchestrationFailure(
                    stage="validation",
                    document_key=_document_key(item),
                    attempts=1,
                    message=f"{message}; {detail_suffix}",
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
                    notes="validation blocked promotion: schema component coverage report failed",
                )
            )
            continue

        new_rows = [row for row in core_rows if str(row.get("fact_id")) not in published_ids]
        for row in new_rows:
            published_ids.add(str(row["fact_id"]))
            published_rows.append(row)
        manager_relationship_rows.extend(relationship_rows)
        extraction_warning_rows.extend(warning_rows)
        if financial_flow_row is not None:
            financial_flow_rows.extend(_serialize_dataclass_rows([financial_flow_row]))
        risk_exposure_rows.extend(_serialize_dataclass_rows(risk_rows))
        consultant_entity_rows.extend(_serialize_dataclass_rows(consultant_entities))
        consultant_engagement_rows.extend(_serialize_dataclass_rows(consultant_engagements))
        consultant_recommendation_rows.extend(_serialize_dataclass_rows(consultant_recommendations))
        consultant_attribution_rows.extend(_serialize_dataclass_rows(consultant_attributions))
        lifecycle_event_rows.extend(_serialize_dataclass_rows(lifecycle_rows))
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

        published_count = (
            len(new_rows)
            + (1 if financial_flow_row is not None else 0)
            + len(risk_rows)
            + len(consultant_entities)
            + len(consultant_engagements)
            + len(consultant_recommendations)
            + len(consultant_attributions)
            + len(lifecycle_rows)
        )
        if published_count > 0:
            publish_successes += 1
        else:
            publish_failures += 1

        if funded_rows_for_publish:
            funded_acc.publish_records += len(funded_rows_for_publish)

        document_status: DocumentStatus
        if published_count == 0:
            document_status = "failed"
            outcome_note = "no publishable domain output"
        elif domain_failures:
            document_status = "processed"
            outcome_note = (
                f"processed with domain failures: {', '.join(sorted(set(domain_failures)))}"
            )
        else:
            document_status = "processed"
            outcome_note = (
                "reprocessed revised artifact"
                if artifact.supersedes_artifact_id is not None
                else "processed active artifact"
            )

        if document_status == "processed" and not domain_failures:
            processed_ids.add(artifact.artifact_id)

        document_outcomes.append(
            DocumentOutcome(
                plan_id=item.plan_id,
                plan_period=item.plan_period,
                source_url=item.source_url,
                artifact_id=artifact.artifact_id,
                supersedes_artifact_id=artifact.supersedes_artifact_id,
                status=document_status,
                promoted_fact_count=published_count,
                review_queue_count=len(queue_rows),
                notes=outcome_note,
            )
        )

    stage_metrics.append(
        OrchestrationStageMetric(
            stage="parse_extract",
            status="ok" if parse_failures == 0 else "error",
            record_count=parse_successes,
            error_count=parse_failures,
            attempt_count=max(total_attempts, 1),
            notes="combined parse and domain extraction stage (see domain metrics for per-domain errors)",
        )
    )
    _append_domain_metrics(
        stage_metrics=stage_metrics,
        stage="parse_extract",
        accumulators=domain_accumulators,
    )
    stage_metrics.append(
        OrchestrationStageMetric(
            stage="validation",
            status="ok" if validation_warnings == 0 and publish_failures == 0 else "error",
            record_count=validation_warnings,
            error_count=publish_failures,
            attempt_count=1,
            notes="confidence review queue routing",
        )
    )
    _append_domain_metrics(
        stage_metrics=stage_metrics,
        stage="validation",
        accumulators=domain_accumulators,
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
    _append_domain_metrics(
        stage_metrics=stage_metrics,
        stage="publish",
        accumulators=domain_accumulators,
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
        "financial_flow_rows": financial_flow_rows,
        "risk_exposure_rows": risk_exposure_rows,
        "consultant_entity_rows": consultant_entity_rows,
        "consultant_engagement_rows": consultant_engagement_rows,
        "consultant_recommendation_rows": consultant_recommendation_rows,
        "consultant_attribution_rows": consultant_attribution_rows,
        "lifecycle_event_rows": lifecycle_event_rows,
        "manager_relationship_rows": manager_relationship_rows,
        "extraction_warning_rows": extraction_warning_rows,
        "component_coverage_reports": component_coverage_reports,
        "state": asdict(next_state),
    }
    if output_root is not None:
        run_dir = output_root / "document_orchestration" / effective_run_id
        _write_json(run_dir / "ledger.json", artifacts_payload["ledger"])
        _write_json(run_dir / "published_rows.json", published_rows)
        _write_json(run_dir / "review_queue_rows.json", review_queue_rows)
        _write_json(run_dir / "financial_flow_rows.json", financial_flow_rows)
        _write_json(run_dir / "risk_exposure_rows.json", risk_exposure_rows)
        _write_json(run_dir / "consultant_entity_rows.json", consultant_entity_rows)
        _write_json(run_dir / "consultant_engagement_rows.json", consultant_engagement_rows)
        _write_json(
            run_dir / "consultant_recommendation_rows.json",
            consultant_recommendation_rows,
        )
        _write_json(run_dir / "consultant_attribution_rows.json", consultant_attribution_rows)
        _write_json(run_dir / "lifecycle_event_rows.json", lifecycle_event_rows)
        _write_json(run_dir / "manager_relationship_rows.json", manager_relationship_rows)
        _write_json(run_dir / "extraction_warning_rows.json", extraction_warning_rows)
        _write_json(run_dir / "component_coverage_reports.json", component_coverage_reports)
        _write_json(run_dir / "state.json", artifacts_payload["state"])
        artifacts_payload["output_dir"] = str(run_dir)

    return ledger, next_state, artifacts_payload
