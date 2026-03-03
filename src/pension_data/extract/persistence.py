"""Extraction-to-persistence adapters for funded and investment staging artifacts."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path

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
from pension_data.extract.investment.manager_positions import (
    ExtractionWarning as PositionExtractionWarning,
)

NON_DISCLOSED_MANAGER_NAME = "[not_disclosed]"
UNKNOWN_MANAGER_NAME = "[unknown_manager]"

STAGING_CORE_METRICS_COLUMNS: tuple[str, ...] = (
    "fact_id",
    "plan_id",
    "plan_period",
    "metric_family",
    "metric_name",
    "as_reported_value",
    "normalized_value",
    "as_reported_unit",
    "normalized_unit",
    "manager_name",
    "fund_name",
    "vehicle_name",
    "relationship_completeness",
    "confidence",
    "evidence_refs",
    "effective_date",
    "ingestion_date",
    "benchmark_version",
    "source_document_id",
    "source_url",
    "parser_version",
    "extraction_method",
)

STAGING_MANAGER_RELATIONSHIP_COLUMNS: tuple[str, ...] = (
    "relationship_id",
    "plan_id",
    "plan_period",
    "manager_name",
    "fund_name",
    "vehicle_name",
    "relationship_completeness",
    "known_not_invested",
    "evidence_refs",
    "effective_date",
    "ingestion_date",
    "benchmark_version",
    "source_document_id",
    "source_url",
)

EXTRACTION_WARNING_COLUMNS: tuple[str, ...] = (
    "warning_id",
    "warning_domain",
    "code",
    "severity",
    "plan_id",
    "plan_period",
    "manager_name",
    "fund_name",
    "metric_name",
    "message",
    "evidence_refs",
    "effective_date",
    "ingestion_date",
    "source_document_id",
    "source_url",
)

_FUNDED_METRIC_NAMES: frozenset[str] = frozenset({"funded_ratio", "aal_usd", "ava_usd"})


@dataclass(frozen=True, slots=True)
class PositionPersistenceContext:
    """Shared temporal/provenance context for manager-position persistence rows."""

    effective_date: str
    ingestion_date: str
    source_document_id: str
    source_url: str
    benchmark_version: str = "v1"


@dataclass(frozen=True, slots=True)
class WarningPersistenceContext:
    """Temporal/provenance context for diagnostics that do not carry row-level dates."""

    plan_id: str
    plan_period: str
    effective_date: str
    ingestion_date: str
    source_document_id: str
    source_url: str


def extraction_persistence_contract() -> dict[str, tuple[str, ...]]:
    """Return target persistence artifacts and required columns for #88 write adapters."""
    return {
        "staging_core_metrics": STAGING_CORE_METRICS_COLUMNS,
        "staging_manager_fund_vehicle_relationships": STAGING_MANAGER_RELATIONSHIP_COLUMNS,
        "extraction_warnings": EXTRACTION_WARNING_COLUMNS,
    }


def _write_json(path: Path, payload: object) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _normalize_refs(evidence_refs: Sequence[str]) -> tuple[str, ...]:
    normalized: list[str] = []
    for ref in evidence_refs:
        token = ref.strip()
        if not token or token in normalized:
            continue
        normalized.append(token)
    return tuple(normalized)


def _stable_id(prefix: str, *parts: object) -> str:
    encoded_parts = [json.dumps(part, sort_keys=True) for part in parts]
    digest = hashlib.sha256("|".join(encoded_parts).encode("utf-8")).hexdigest()[:20]
    return f"{prefix}:{digest}"


def _metric_family_for_funded(metric_name: str) -> str:
    return "funded" if metric_name in _FUNDED_METRIC_NAMES else "actuarial"


def _manager_name_for_relationship(row: PlanManagerFundPosition) -> str:
    if row.manager_name and row.manager_name.strip():
        return row.manager_name
    if row.completeness == "not_disclosed":
        return NON_DISCLOSED_MANAGER_NAME
    return UNKNOWN_MANAGER_NAME


def persist_funded_actuarial_metrics(
    rows: Sequence[FundedActuarialStagingFact],
    *,
    benchmark_version: str | None = None,
) -> list[dict[str, object]]:
    """Map funded/actuarial extraction rows into staging-core metric persistence rows."""
    persisted: list[dict[str, object]] = []
    for row in rows:
        evidence_refs = _normalize_refs(row.evidence_refs)
        current_benchmark = benchmark_version or row.parser_version
        record: dict[str, object] = {
            "fact_id": _stable_id(
                "fact",
                row.plan_id,
                row.plan_period,
                _metric_family_for_funded(row.metric_name),
                row.metric_name,
                row.effective_date,
                row.ingestion_date,
                current_benchmark,
                row.source_document_id,
                row.evidence_refs,
            ),
            "plan_id": row.plan_id,
            "plan_period": row.plan_period,
            "metric_family": _metric_family_for_funded(row.metric_name),
            "metric_name": row.metric_name,
            "as_reported_value": row.as_reported_value,
            "normalized_value": row.normalized_value,
            "as_reported_unit": row.as_reported_unit,
            "normalized_unit": row.normalized_unit,
            "manager_name": None,
            "fund_name": None,
            "vehicle_name": None,
            "relationship_completeness": None,
            "confidence": row.confidence,
            "evidence_refs": list(evidence_refs),
            "effective_date": row.effective_date,
            "ingestion_date": row.ingestion_date,
            "benchmark_version": current_benchmark,
            "source_document_id": row.source_document_id,
            "source_url": row.source_url,
            "parser_version": row.parser_version,
            "extraction_method": row.extraction_method,
        }
        persisted.append(record)

    return sorted(
        persisted,
        key=lambda item: (
            str(item["plan_id"]),
            str(item["plan_period"]),
            str(item["metric_family"]),
            str(item["metric_name"]),
            str(item["effective_date"]),
            str(item["ingestion_date"]),
            str(item["source_document_id"]),
        ),
    )


def _allocation_metric_rows(
    row: AssetAllocationObservation, *, benchmark_version: str
) -> list[dict[str, object]]:
    evidence_refs = _normalize_refs(row.evidence_refs)
    records: list[dict[str, object]] = []
    weight_name = f"{row.category}_weight"
    if row.as_reported_percent is not None or row.normalized_weight is not None:
        records.append(
            {
                "fact_id": _stable_id(
                    "fact",
                    row.plan_id,
                    row.plan_period,
                    "allocation",
                    weight_name,
                    row.effective_date,
                    row.ingestion_date,
                    benchmark_version,
                    row.source_document_id,
                    row.evidence_refs,
                ),
                "plan_id": row.plan_id,
                "plan_period": row.plan_period,
                "metric_family": "allocation",
                "metric_name": weight_name,
                "as_reported_value": row.as_reported_percent,
                "normalized_value": row.normalized_weight,
                "as_reported_unit": "percent" if row.as_reported_percent is not None else None,
                "normalized_unit": "ratio" if row.normalized_weight is not None else None,
                "manager_name": None,
                "fund_name": None,
                "vehicle_name": None,
                "relationship_completeness": None,
                "confidence": 1.0,
                "evidence_refs": list(evidence_refs),
                "effective_date": row.effective_date,
                "ingestion_date": row.ingestion_date,
                "benchmark_version": benchmark_version,
                "source_document_id": row.source_document_id,
                "source_url": None,
                "parser_version": "allocation_fee_v1",
                "extraction_method": "table_lookup",
            }
        )

    amount_name = f"{row.category}_amount_usd"
    if row.as_reported_amount is not None or row.normalized_amount_usd is not None:
        records.append(
            {
                "fact_id": _stable_id(
                    "fact",
                    row.plan_id,
                    row.plan_period,
                    "allocation",
                    amount_name,
                    row.effective_date,
                    row.ingestion_date,
                    benchmark_version,
                    row.source_document_id,
                    row.evidence_refs,
                ),
                "plan_id": row.plan_id,
                "plan_period": row.plan_period,
                "metric_family": "allocation",
                "metric_name": amount_name,
                "as_reported_value": row.as_reported_amount,
                "normalized_value": row.normalized_amount_usd,
                "as_reported_unit": "usd" if row.as_reported_amount is not None else None,
                "normalized_unit": "usd" if row.normalized_amount_usd is not None else None,
                "manager_name": None,
                "fund_name": None,
                "vehicle_name": None,
                "relationship_completeness": None,
                "confidence": 1.0,
                "evidence_refs": list(evidence_refs),
                "effective_date": row.effective_date,
                "ingestion_date": row.ingestion_date,
                "benchmark_version": benchmark_version,
                "source_document_id": row.source_document_id,
                "source_url": None,
                "parser_version": "allocation_fee_v1",
                "extraction_method": "table_lookup",
            }
        )
    return records


def persist_asset_allocations(
    rows: Sequence[AssetAllocationObservation],
    *,
    benchmark_version: str = "v1",
) -> list[dict[str, object]]:
    """Map allocation observations into staging-core metric persistence rows."""
    persisted = [
        record
        for row in rows
        for record in _allocation_metric_rows(row, benchmark_version=benchmark_version)
    ]
    return sorted(
        persisted,
        key=lambda item: (
            str(item["plan_id"]),
            str(item["plan_period"]),
            str(item["metric_name"]),
            str(item["effective_date"]),
            str(item["source_document_id"]),
        ),
    )


def _fee_metric_rows(
    row: ManagerFeeObservation, *, benchmark_version: str
) -> list[dict[str, object]]:
    evidence_refs = _normalize_refs(row.evidence_refs)
    records: list[dict[str, object]] = []
    rate_metric_name = f"{row.fee_type}_rate"
    if row.as_reported_rate_pct is not None or row.normalized_rate is not None:
        records.append(
            {
                "fact_id": _stable_id(
                    "fact",
                    row.plan_id,
                    row.plan_period,
                    "fee",
                    rate_metric_name,
                    row.manager_name,
                    row.effective_date,
                    row.ingestion_date,
                    benchmark_version,
                    row.source_document_id,
                    row.evidence_refs,
                ),
                "plan_id": row.plan_id,
                "plan_period": row.plan_period,
                "metric_family": "fee",
                "metric_name": rate_metric_name,
                "as_reported_value": row.as_reported_rate_pct,
                "normalized_value": row.normalized_rate,
                "as_reported_unit": "percent" if row.as_reported_rate_pct is not None else None,
                "normalized_unit": "ratio" if row.normalized_rate is not None else None,
                "manager_name": row.manager_name,
                "fund_name": None,
                "vehicle_name": None,
                "relationship_completeness": row.completeness,
                "confidence": 1.0,
                "evidence_refs": list(evidence_refs),
                "effective_date": row.effective_date,
                "ingestion_date": row.ingestion_date,
                "benchmark_version": benchmark_version,
                "source_document_id": row.source_document_id,
                "source_url": None,
                "parser_version": "allocation_fee_v1",
                "extraction_method": "table_lookup",
            }
        )

    amount_metric_name = f"{row.fee_type}_amount_usd"
    if row.as_reported_amount is not None or row.normalized_amount_usd is not None:
        records.append(
            {
                "fact_id": _stable_id(
                    "fact",
                    row.plan_id,
                    row.plan_period,
                    "fee",
                    amount_metric_name,
                    row.manager_name,
                    row.effective_date,
                    row.ingestion_date,
                    benchmark_version,
                    row.source_document_id,
                    row.evidence_refs,
                ),
                "plan_id": row.plan_id,
                "plan_period": row.plan_period,
                "metric_family": "fee",
                "metric_name": amount_metric_name,
                "as_reported_value": row.as_reported_amount,
                "normalized_value": row.normalized_amount_usd,
                "as_reported_unit": "usd" if row.as_reported_amount is not None else None,
                "normalized_unit": "usd" if row.normalized_amount_usd is not None else None,
                "manager_name": row.manager_name,
                "fund_name": None,
                "vehicle_name": None,
                "relationship_completeness": row.completeness,
                "confidence": 1.0,
                "evidence_refs": list(evidence_refs),
                "effective_date": row.effective_date,
                "ingestion_date": row.ingestion_date,
                "benchmark_version": benchmark_version,
                "source_document_id": row.source_document_id,
                "source_url": None,
                "parser_version": "allocation_fee_v1",
                "extraction_method": "table_lookup",
            }
        )
    return records


def persist_fee_observations(
    rows: Sequence[ManagerFeeObservation],
    *,
    benchmark_version: str = "v1",
) -> list[dict[str, object]]:
    """Map fee observations into staging-core metric persistence rows."""
    persisted = [
        record
        for row in rows
        for record in _fee_metric_rows(row, benchmark_version=benchmark_version)
    ]
    return sorted(
        persisted,
        key=lambda item: (
            str(item["plan_id"]),
            str(item["plan_period"]),
            str(item["manager_name"] or ""),
            str(item["metric_name"]),
            str(item["effective_date"]),
            str(item["source_document_id"]),
        ),
    )


def persist_manager_positions(
    rows: Sequence[PlanManagerFundPosition],
    *,
    context: PositionPersistenceContext,
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    """Map manager/fund holdings into core metrics and relationship staging rows."""
    metric_rows: list[dict[str, object]] = []
    relationship_rows: list[dict[str, object]] = []

    for row in rows:
        manager_name = _manager_name_for_relationship(row)
        evidence_refs = _normalize_refs(row.evidence_refs)
        relationship_row: dict[str, object] = {
            "relationship_id": _stable_id(
                "rel",
                row.plan_id,
                row.plan_period,
                manager_name,
                row.fund_name,
                row.completeness,
                context.effective_date,
                context.ingestion_date,
                context.benchmark_version,
                context.source_document_id,
                row.evidence_refs,
            ),
            "plan_id": row.plan_id,
            "plan_period": row.plan_period,
            "manager_name": manager_name,
            "fund_name": row.fund_name,
            "vehicle_name": None,
            "relationship_completeness": row.completeness,
            "known_not_invested": 1 if row.known_not_invested else 0,
            "evidence_refs": list(evidence_refs),
            "effective_date": context.effective_date,
            "ingestion_date": context.ingestion_date,
            "benchmark_version": context.benchmark_version,
            "source_document_id": context.source_document_id,
            "source_url": context.source_url,
        }
        relationship_rows.append(relationship_row)

        for metric_name, metric_value in (
            ("commitment", row.commitment),
            ("unfunded", row.unfunded),
            ("market_value", row.market_value),
        ):
            if metric_value is None:
                continue
            metric_rows.append(
                {
                    "fact_id": _stable_id(
                        "fact",
                        row.plan_id,
                        row.plan_period,
                        "holding",
                        metric_name,
                        manager_name,
                        row.fund_name,
                        context.effective_date,
                        context.ingestion_date,
                        context.benchmark_version,
                        context.source_document_id,
                        row.evidence_refs,
                    ),
                    "plan_id": row.plan_id,
                    "plan_period": row.plan_period,
                    "metric_family": "holding",
                    "metric_name": metric_name,
                    "as_reported_value": metric_value,
                    "normalized_value": metric_value,
                    "as_reported_unit": "usd",
                    "normalized_unit": "usd",
                    "manager_name": manager_name,
                    "fund_name": row.fund_name,
                    "vehicle_name": None,
                    "relationship_completeness": row.completeness,
                    "confidence": row.confidence,
                    "evidence_refs": list(evidence_refs),
                    "effective_date": context.effective_date,
                    "ingestion_date": context.ingestion_date,
                    "benchmark_version": context.benchmark_version,
                    "source_document_id": context.source_document_id,
                    "source_url": context.source_url,
                    "parser_version": "manager_positions_v1",
                    "extraction_method": "table_lookup",
                }
            )

    sorted_metric_rows = sorted(
        metric_rows,
        key=lambda item: (
            str(item["plan_id"]),
            str(item["plan_period"]),
            str(item["manager_name"]),
            str(item["fund_name"] or ""),
            str(item["metric_name"]),
            str(item["effective_date"]),
            str(item["source_document_id"]),
        ),
    )
    sorted_relationship_rows = sorted(
        relationship_rows,
        key=lambda item: (
            str(item["plan_id"]),
            str(item["plan_period"]),
            str(item["manager_name"]),
            str(item["fund_name"] or ""),
            str(item["effective_date"]),
            str(item["source_document_id"]),
        ),
    )
    return sorted_metric_rows, sorted_relationship_rows


def persist_extraction_warnings(
    *,
    funded_diagnostics: Sequence[ExtractionDiagnostic] = (),
    funded_context: WarningPersistenceContext | None = None,
    investment_warnings: Sequence[InvestmentExtractionWarning] = (),
    manager_position_warnings: Sequence[PositionExtractionWarning] = (),
    manager_position_context: PositionPersistenceContext | None = None,
) -> list[dict[str, object]]:
    """Map extraction diagnostics and warnings into a dedicated persisted warning artifact."""
    warning_rows: list[dict[str, object]] = []

    for diagnostic in funded_diagnostics:
        context = funded_context
        plan_id = context.plan_id if context is not None else ""
        plan_period = context.plan_period if context is not None else ""
        effective_date = context.effective_date if context is not None else ""
        ingestion_date = context.ingestion_date if context is not None else ""
        source_document_id = context.source_document_id if context is not None else ""
        source_url = context.source_url if context is not None else ""
        evidence_refs = _normalize_refs(diagnostic.evidence_refs)
        warning_rows.append(
            {
                "warning_id": _stable_id(
                    "warn",
                    "funded_actuarial",
                    diagnostic.code,
                    diagnostic.metric_name,
                    diagnostic.message,
                    plan_id,
                    plan_period,
                    evidence_refs,
                ),
                "warning_domain": "funded_actuarial",
                "code": diagnostic.code,
                "severity": diagnostic.severity,
                "plan_id": plan_id,
                "plan_period": plan_period,
                "manager_name": None,
                "fund_name": None,
                "metric_name": diagnostic.metric_name,
                "message": diagnostic.message,
                "evidence_refs": list(evidence_refs),
                "effective_date": effective_date,
                "ingestion_date": ingestion_date,
                "source_document_id": source_document_id,
                "source_url": source_url,
            }
        )

    for investment_warning in investment_warnings:
        evidence_refs = _normalize_refs(investment_warning.evidence_refs)
        warning_rows.append(
            {
                "warning_id": _stable_id(
                    "warn",
                    "investment_fee",
                    investment_warning.code,
                    investment_warning.plan_id,
                    investment_warning.plan_period,
                    investment_warning.manager_name,
                    investment_warning.message,
                    evidence_refs,
                ),
                "warning_domain": "investment_fee",
                "code": investment_warning.code,
                "severity": "warning",
                "plan_id": investment_warning.plan_id,
                "plan_period": investment_warning.plan_period,
                "manager_name": investment_warning.manager_name,
                "fund_name": None,
                "metric_name": None,
                "message": investment_warning.message,
                "evidence_refs": list(evidence_refs),
                "effective_date": "",
                "ingestion_date": "",
                "source_document_id": "",
                "source_url": "",
            }
        )

    for position_warning in manager_position_warnings:
        position_context = manager_position_context
        effective_date = position_context.effective_date if position_context is not None else ""
        ingestion_date = position_context.ingestion_date if position_context is not None else ""
        source_document_id = (
            position_context.source_document_id if position_context is not None else ""
        )
        source_url = position_context.source_url if position_context is not None else ""
        evidence_refs = _normalize_refs(position_warning.evidence_refs)
        warning_rows.append(
            {
                "warning_id": _stable_id(
                    "warn",
                    "manager_position",
                    position_warning.code,
                    position_warning.plan_id,
                    position_warning.plan_period,
                    position_warning.manager_name,
                    position_warning.fund_name,
                    position_warning.message,
                    evidence_refs,
                ),
                "warning_domain": "manager_position",
                "code": position_warning.code,
                "severity": "warning",
                "plan_id": position_warning.plan_id,
                "plan_period": position_warning.plan_period,
                "manager_name": position_warning.manager_name,
                "fund_name": position_warning.fund_name,
                "metric_name": None,
                "message": position_warning.message,
                "evidence_refs": list(evidence_refs),
                "effective_date": effective_date,
                "ingestion_date": ingestion_date,
                "source_document_id": source_document_id,
                "source_url": source_url,
            }
        )

    return sorted(
        warning_rows,
        key=lambda item: (
            str(item["warning_domain"]),
            str(item["plan_id"]),
            str(item["plan_period"]),
            str(item["manager_name"] or ""),
            str(item["fund_name"] or ""),
            str(item["metric_name"] or ""),
            str(item["code"]),
            str(item["message"]),
        ),
    )


def build_extraction_persistence_artifacts(
    *,
    funded_actuarial_rows: Sequence[FundedActuarialStagingFact] = (),
    funded_actuarial_diagnostics: Sequence[ExtractionDiagnostic] = (),
    funded_warning_context: WarningPersistenceContext | None = None,
    allocation_rows: Sequence[AssetAllocationObservation] = (),
    fee_rows: Sequence[ManagerFeeObservation] = (),
    investment_warnings: Sequence[InvestmentExtractionWarning] = (),
    manager_position_rows: Sequence[PlanManagerFundPosition] = (),
    manager_position_warnings: Sequence[PositionExtractionWarning] = (),
    manager_position_context: PositionPersistenceContext | None = None,
    benchmark_version: str = "v1",
) -> dict[str, object]:
    """Build deterministic staging artifacts for funded and investment extraction output."""
    persisted_core_metrics = [
        *persist_funded_actuarial_metrics(
            funded_actuarial_rows,
            benchmark_version=benchmark_version,
        ),
        *persist_asset_allocations(allocation_rows, benchmark_version=benchmark_version),
        *persist_fee_observations(fee_rows, benchmark_version=benchmark_version),
    ]

    manager_metric_rows: list[dict[str, object]] = []
    relationship_rows: list[dict[str, object]] = []
    if manager_position_rows:
        if manager_position_context is None:
            raise ValueError(
                "manager_position_context is required when manager_position_rows are provided"
            )
        manager_metric_rows, relationship_rows = persist_manager_positions(
            manager_position_rows,
            context=manager_position_context,
        )
        persisted_core_metrics.extend(manager_metric_rows)

    persisted_core_metrics = sorted(
        persisted_core_metrics,
        key=lambda item: (
            str(item["plan_id"]),
            str(item["plan_period"]),
            str(item["metric_family"]),
            str(item["metric_name"]),
            str(item["manager_name"] or ""),
            str(item["fund_name"] or ""),
            str(item["effective_date"]),
            str(item["ingestion_date"]),
            str(item["source_document_id"]),
        ),
    )

    warning_rows = persist_extraction_warnings(
        funded_diagnostics=funded_actuarial_diagnostics,
        funded_context=funded_warning_context,
        investment_warnings=investment_warnings,
        manager_position_warnings=manager_position_warnings,
        manager_position_context=manager_position_context,
    )

    return {
        "persistence_contract": extraction_persistence_contract(),
        "staging_core_metrics_rows": persisted_core_metrics,
        "staging_manager_fund_vehicle_relationship_rows": relationship_rows,
        "extraction_warning_rows": warning_rows,
    }


def write_extraction_persistence_artifacts(
    artifacts: Mapping[str, object],
    *,
    output_root: Path,
) -> dict[str, str]:
    """Write extraction persistence artifacts under `extraction_persistence/`."""
    contract = artifacts.get("persistence_contract")
    core_rows = artifacts.get("staging_core_metrics_rows")
    relationship_rows = artifacts.get("staging_manager_fund_vehicle_relationship_rows")
    warning_rows = artifacts.get("extraction_warning_rows")

    if not isinstance(contract, Mapping):
        raise ValueError("artifacts['persistence_contract'] must be a mapping")
    if not isinstance(core_rows, list):
        raise ValueError("artifacts['staging_core_metrics_rows'] must be a list")
    if not isinstance(relationship_rows, list):
        raise ValueError(
            "artifacts['staging_manager_fund_vehicle_relationship_rows'] must be a list"
        )
    if not isinstance(warning_rows, list):
        raise ValueError("artifacts['extraction_warning_rows'] must be a list")

    output_dir = output_root / "extraction_persistence"
    output_dir.mkdir(parents=True, exist_ok=True)

    contract_json = output_dir / "persistence_contract.json"
    core_metrics_json = output_dir / "staging_core_metrics.json"
    relationships_json = output_dir / "staging_manager_fund_vehicle_relationships.json"
    warnings_json = output_dir / "extraction_warnings.json"

    _write_json(contract_json, contract)
    _write_json(core_metrics_json, core_rows)
    _write_json(relationships_json, relationship_rows)
    _write_json(warnings_json, warning_rows)

    return {
        "persistence_contract_json": str(contract_json),
        "staging_core_metrics_json": str(core_metrics_json),
        "staging_manager_fund_vehicle_relationships_json": str(relationships_json),
        "extraction_warnings_json": str(warnings_json),
    }
