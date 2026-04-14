"""Foundation fixture pipeline with run-ledger persistence and failure taxonomy."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal
from urllib.parse import urlparse

from pension_data.coverage.readiness import build_readiness_artifacts
from pension_data.sources.schema import (
    MISMATCH_REASONS,
    OFFICIAL_RESOLUTION_STATES,
    SOURCE_AUTHORITY_TIERS,
    MismatchReason,
    OfficialResolutionState,
    SourceAuthorityTier,
    SourceMapRecord,
)
from pension_data.sources.validate import load_source_map, validate_source_map_entries

StageName = Literal["registry", "source_map", "discovery", "ingestion", "coverage"]
StageStatus = Literal["ok", "error", "skipped"]
RunStatus = Literal["success", "failed"]
FailureCategory = Literal[
    "source_map_breakage",
    "robots_restriction",
    "revised_file_anomaly",
    "discovery_data_error",
    "ingestion_data_error",
    "unexpected_error",
]

_STAGES: tuple[StageName, ...] = ("registry", "source_map", "discovery", "ingestion", "coverage")


@dataclass(frozen=True, slots=True)
class StageLedgerMetric:
    """Structured per-stage metric row for one foundation run."""

    stage: StageName
    status: StageStatus
    record_count: int
    error_count: int
    notes: str


@dataclass(frozen=True, slots=True)
class FailureLedgerRow:
    """Categorized stage failure emitted into the run ledger."""

    stage: StageName
    category: FailureCategory
    message: str


@dataclass(frozen=True, slots=True)
class FoundationRunLedger:
    """Structured run-ledger record for foundation fixture executions."""

    run_id: str
    fixture_path: str
    started_at: str
    completed_at: str
    status: RunStatus
    stage_metrics: tuple[StageLedgerMetric, ...]
    failures: tuple[FailureLedgerRow, ...]


@dataclass(frozen=True, slots=True)
class _IngestionFixtureItem:
    plan_id: str
    plan_period: str
    source_url: str
    fetched_at: str
    byte_size: int
    revised_of_source_url: str | None


def _utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _load_fixture(fixture_path: Path) -> dict[str, object]:
    payload = json.loads(fixture_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("foundation fixture must be a JSON object")
    return payload


def _require_string(mapping: dict[str, object], key: str) -> str:
    value = mapping.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"fixture field '{key}' must be a non-empty string")
    return value.strip()


def _optional_string(mapping: dict[str, object], key: str) -> str | None:
    value = mapping.get(key)
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError(f"fixture field '{key}' must be a string when provided")
    stripped = value.strip()
    return stripped or None


def _require_list_of_objects(mapping: dict[str, object], key: str) -> list[dict[str, object]]:
    value = mapping.get(key)
    if not isinstance(value, list):
        raise ValueError(f"fixture field '{key}' must be a list")
    rows: list[dict[str, object]] = []
    for index, item in enumerate(value):
        if not isinstance(item, dict):
            raise ValueError(f"fixture field '{key}[{index}]' must be an object")
        rows.append(item)
    return rows


def _parse_authority_tier(raw: str) -> SourceAuthorityTier:
    if raw not in SOURCE_AUTHORITY_TIERS:
        raise ValueError(
            "discovered record has invalid source_authority_tier "
            f"'{raw}'; expected one of {', '.join(SOURCE_AUTHORITY_TIERS)}"
        )
    return raw


def _parse_resolution_state(raw: str) -> OfficialResolutionState:
    if raw not in OFFICIAL_RESOLUTION_STATES:
        raise ValueError(
            "discovered record has invalid official_resolution_state "
            f"'{raw}'; expected one of {', '.join(OFFICIAL_RESOLUTION_STATES)}"
        )
    return raw


def _parse_mismatch_reason(raw: str | None) -> MismatchReason | None:
    if raw is None:
        return None
    if raw not in MISMATCH_REASONS:
        raise ValueError(
            f"discovered record has invalid mismatch_reason '{raw}'; "
            f"expected one of {', '.join(MISMATCH_REASONS)}"
        )
    return raw


def categorize_failure(*, stage: StageName, message: str) -> FailureCategory:
    """Map stage errors to actionable failure categories for operators."""
    normalized = message.lower()
    if stage == "source_map":
        return "source_map_breakage"
    if "robot" in normalized or "403" in normalized:
        return "robots_restriction"
    if "revised" in normalized or "supersession" in normalized:
        return "revised_file_anomaly"
    if stage == "discovery":
        return "discovery_data_error"
    if stage == "ingestion":
        return "ingestion_data_error"
    return "unexpected_error"


def _build_discovery_records(
    *,
    fixture: dict[str, object],
    allowed_domains_by_plan: dict[str, tuple[str, ...]],
    registry_plan_ids: set[str],
) -> list[SourceMapRecord]:
    discovered_rows = _require_list_of_objects(fixture, "discovered_records")
    records: list[SourceMapRecord] = []

    for row in sorted(
        discovered_rows,
        key=lambda item: (
            str(item.get("plan_id", "")),
            str(item.get("plan_period", "")),
            str(item.get("source_url", "")),
        ),
    ):
        plan_id = _require_string(row, "plan_id")
        plan_period = _require_string(row, "plan_period")
        cohort = _require_string(row, "cohort")
        source_url = _require_string(row, "source_url")
        authority_tier = _parse_authority_tier(_require_string(row, "source_authority_tier"))
        resolution_state = _parse_resolution_state(
            _require_string(row, "official_resolution_state")
        )
        expected_plan_identity = _require_string(row, "expected_plan_identity")
        observed_plan_identity = _optional_string(row, "observed_plan_identity")
        mismatch_reason = _parse_mismatch_reason(_optional_string(row, "mismatch_reason"))

        if "robot" in source_url.lower():
            raise PermissionError(f"robots restriction blocked discovery URL: {source_url}")

        parsed = urlparse(source_url)
        host = (parsed.hostname or "").lower().strip()
        if not host:
            raise ValueError(f"discovery source_url has no host: {source_url}")

        if plan_id not in registry_plan_ids:
            raise ValueError(f"discovery plan_id '{plan_id}' is not present in registry seed")

        allowed_domains = allowed_domains_by_plan.get(plan_id)
        if allowed_domains is None:
            raise ValueError(f"discovery record references unknown plan_id '{plan_id}'")
        if not any(host == domain or host.endswith(f".{domain}") for domain in allowed_domains):
            raise ValueError(
                f"discovery URL host '{host}' is not in source-map allowed domains for {plan_id}"
            )

        records.append(
            SourceMapRecord(
                plan_id=plan_id,
                plan_period=plan_period,
                cohort=cohort,
                source_url=source_url,
                source_authority_tier=authority_tier,
                official_resolution_state=resolution_state,
                expected_plan_identity=expected_plan_identity,
                observed_plan_identity=observed_plan_identity,
                mismatch_reason=mismatch_reason,
            )
        )
    return records


def _build_ingestion_items(fixture: dict[str, object]) -> list[_IngestionFixtureItem]:
    rows = _require_list_of_objects(fixture, "ingestion_items")
    items: list[_IngestionFixtureItem] = []
    for row in rows:
        byte_size_raw = row.get("byte_size")
        if not isinstance(byte_size_raw, int) or byte_size_raw < 0:
            raise ValueError("ingestion item field 'byte_size' must be a non-negative integer")
        items.append(
            _IngestionFixtureItem(
                plan_id=_require_string(row, "plan_id"),
                plan_period=_require_string(row, "plan_period"),
                source_url=_require_string(row, "source_url"),
                fetched_at=_require_string(row, "fetched_at"),
                byte_size=byte_size_raw,
                revised_of_source_url=_optional_string(row, "revised_of_source_url"),
            )
        )
    return sorted(items, key=lambda item: (item.plan_id, item.plan_period, item.source_url))


def write_run_ledger(
    ledger: FoundationRunLedger,
    *,
    output_root: Path,
) -> dict[str, str]:
    """Persist run-ledger outputs as JSONL history and latest snapshot JSON."""
    output_root.mkdir(parents=True, exist_ok=True)
    ledger_payload = asdict(ledger)
    latest_path = output_root / "latest_run_ledger.json"
    history_path = output_root / "run_ledger.jsonl"

    _write_json(latest_path, ledger_payload)
    with history_path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(ledger_payload, sort_keys=True) + "\n")

    return {
        "latest_run_ledger_json": str(latest_path),
        "run_ledger_jsonl": str(history_path),
    }


def run_foundation_fixture_pipeline(
    *,
    fixture_path: Path,
    output_root: Path,
    run_id: str | None = None,
    started_at: str | None = None,
    completed_at: str | None = None,
) -> tuple[FoundationRunLedger, dict[str, str]]:
    """Execute registry -> source-map -> discovery -> ingestion -> coverage on fixtures."""
    fixture = _load_fixture(fixture_path)
    run_started = started_at or _utc_now_iso()
    effective_run_id = run_id or f"foundation-{datetime.now(UTC).strftime('%Y%m%d%H%M%S')}"

    foundation_dir = output_root / "foundation"
    artifact_paths: dict[str, str] = {}
    stage_metrics: list[StageLedgerMetric] = []
    failures: list[FailureLedgerRow] = []
    has_error = False

    source_map_entries_by_plan: dict[str, tuple[str, ...]] = {}
    discovery_records: list[SourceMapRecord] = []
    registry_plan_ids: set[str] = set()

    for stage in _STAGES:
        if has_error:
            stage_metrics.append(
                StageLedgerMetric(
                    stage=stage,
                    status="skipped",
                    record_count=0,
                    error_count=0,
                    notes="skipped because an earlier stage failed",
                )
            )
            continue

        try:
            if stage == "registry":
                registry_seed = _require_string(fixture, "registry_seed")
                registry_path = (fixture_path.parent / registry_seed).resolve()
                from pension_data.registry.loader import load_registry_from_seed

                registry_records = load_registry_from_seed(registry_path)
                registry_plan_ids = {item.stable_id for item in registry_records}
                stage_metrics.append(
                    StageLedgerMetric(
                        stage="registry",
                        status="ok",
                        record_count=len(registry_records),
                        error_count=0,
                        notes=f"loaded registry rows from {registry_path.name}",
                    )
                )
                continue

            if stage == "source_map":
                source_map_seed = _require_string(fixture, "source_map_seed")
                source_map_path = (fixture_path.parent / source_map_seed).resolve()
                source_map_entries = load_source_map(source_map_path)
                findings = validate_source_map_entries(source_map_entries)
                if findings:
                    finding_lines = [
                        f"{item.code}:{item.plan_id}:{item.message}" for item in findings
                    ]
                    raise ValueError("source-map validation failed: " + " | ".join(finding_lines))
                unknown_plan_ids = sorted(
                    {
                        entry.plan_id
                        for entry in source_map_entries
                        if entry.plan_id not in registry_plan_ids
                    }
                )
                if unknown_plan_ids:
                    raise ValueError(
                        "source-map plan_id values are not present in registry seed: "
                        + ", ".join(unknown_plan_ids)
                    )
                source_map_entries_by_plan = {
                    entry.plan_id: tuple(domain.lower() for domain in entry.allowed_domains)
                    for entry in source_map_entries
                }
                stage_metrics.append(
                    StageLedgerMetric(
                        stage="source_map",
                        status="ok",
                        record_count=len(source_map_entries),
                        error_count=0,
                        notes=f"validated source map entries from {source_map_path.name}",
                    )
                )
                continue

            if stage == "discovery":
                discovery_records = _build_discovery_records(
                    fixture=fixture,
                    allowed_domains_by_plan=source_map_entries_by_plan,
                    registry_plan_ids=registry_plan_ids,
                )
                discovery_rows = [
                    {
                        "plan_id": record.plan_id,
                        "plan_period": record.plan_period,
                        "cohort": record.cohort,
                        "source_url": record.source_url,
                        "source_authority_tier": record.source_authority_tier,
                        "official_resolution_state": record.official_resolution_state,
                        "expected_plan_identity": record.expected_plan_identity,
                        "observed_plan_identity": record.observed_plan_identity or "",
                        "mismatch_reason": record.mismatch_reason or "",
                    }
                    for record in discovery_records
                ]
                discovery_path = foundation_dir / "discovery_rows.json"
                _write_json(discovery_path, discovery_rows)
                artifact_paths["discovery_rows_json"] = str(discovery_path)
                stage_metrics.append(
                    StageLedgerMetric(
                        stage="discovery",
                        status="ok",
                        record_count=len(discovery_records),
                        error_count=0,
                        notes="projected source-resolution discovery records",
                    )
                )
                continue

            if stage == "ingestion":
                ingestion_items = _build_ingestion_items(fixture)
                discovered_urls = {record.source_url for record in discovery_records}
                by_plan_count: dict[str, int] = {}
                total_bytes = 0
                revised_count = 0
                for item in ingestion_items:
                    if item.source_url not in discovered_urls:
                        raise ValueError(
                            "ingestion input URL was not produced by discovery: "
                            f"{item.source_url}"
                        )
                    if item.revised_of_source_url is not None:
                        if item.revised_of_source_url not in discovered_urls:
                            raise RuntimeError(
                                "revised-file anomaly: revised_of_source_url does not map to a "
                                f"discovered record ({item.revised_of_source_url})"
                            )
                        revised_count += 1
                    by_plan_count[item.plan_id] = by_plan_count.get(item.plan_id, 0) + 1
                    total_bytes += item.byte_size
                ingestion_summary = {
                    "total_item_count": len(ingestion_items),
                    "total_bytes": total_bytes,
                    "revised_item_count": revised_count,
                    "by_plan_count": dict(sorted(by_plan_count.items())),
                }
                ingestion_path = foundation_dir / "ingestion_summary.json"
                _write_json(ingestion_path, ingestion_summary)
                artifact_paths["ingestion_summary_json"] = str(ingestion_path)
                stage_metrics.append(
                    StageLedgerMetric(
                        stage="ingestion",
                        status="ok",
                        record_count=len(ingestion_items),
                        error_count=0,
                        notes="validated fixture ingestion rows and revised lineage references",
                    )
                )
                continue

            if stage == "coverage":
                coverage_artifacts = build_readiness_artifacts(discovery_records)
                coverage_path = foundation_dir / "coverage_readiness.json"
                _write_json(coverage_path, coverage_artifacts)
                artifact_paths["coverage_readiness_json"] = str(coverage_path)
                stage_metrics.append(
                    StageLedgerMetric(
                        stage="coverage",
                        status="ok",
                        record_count=len(discovery_records),
                        error_count=0,
                        notes="built readiness coverage outputs from discovery records",
                    )
                )
                continue
        except Exception as error:
            has_error = True
            message = f"{type(error).__name__}: {error}"
            stage_metrics.append(
                StageLedgerMetric(
                    stage=stage,
                    status="error",
                    record_count=0,
                    error_count=1,
                    notes=message,
                )
            )
            failures.append(
                FailureLedgerRow(
                    stage=stage,
                    category=categorize_failure(stage=stage, message=message),
                    message=message,
                )
            )

    ledger = FoundationRunLedger(
        run_id=effective_run_id,
        fixture_path=str(fixture_path),
        started_at=run_started,
        completed_at=completed_at or _utc_now_iso(),
        status="failed" if failures else "success",
        stage_metrics=tuple(stage_metrics),
        failures=tuple(failures),
    )
    ledger_paths = write_run_ledger(ledger, output_root=foundation_dir)
    artifact_paths.update(ledger_paths)
    return ledger, artifact_paths
