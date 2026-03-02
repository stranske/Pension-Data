"""SLA metric catalog and deterministic quality metric calculations."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime


@dataclass(frozen=True, slots=True)
class SLAMetricDefinition:
    """Definition metadata for a single SLA metric."""

    name: str
    description: str
    unit: str
    higher_is_better: bool


SLA_METRIC_CATALOG: dict[str, SLAMetricDefinition] = {
    "completeness_rate": SLAMetricDefinition(
        name="completeness_rate",
        description="Fraction of expected records with complete required fields.",
        unit="ratio",
        higher_is_better=True,
    ),
    "freshness_lag_hours": SLAMetricDefinition(
        name="freshness_lag_hours",
        description="Hours between source publication and pipeline run start.",
        unit="hours",
        higher_is_better=False,
    ),
    "review_queue_latency_hours": SLAMetricDefinition(
        name="review_queue_latency_hours",
        description="Average review queue wait time for flagged records.",
        unit="hours",
        higher_is_better=False,
    ),
    "parse_warning_rate": SLAMetricDefinition(
        name="parse_warning_rate",
        description="Share of records with parser warnings.",
        unit="ratio",
        higher_is_better=False,
    ),
    "citation_density_per_10_pages": SLAMetricDefinition(
        name="citation_density_per_10_pages",
        description="Average cited facts per 10 document pages.",
        unit="count_per_10_pages",
        higher_is_better=True,
    ),
    "source_mismatch_rate": SLAMetricDefinition(
        name="source_mismatch_rate",
        description="Share of records with source mismatch findings.",
        unit="ratio",
        higher_is_better=False,
    ),
    "unresolved_official_source_rate": SLAMetricDefinition(
        name="unresolved_official_source_rate",
        description="Share of records missing resolved official sources.",
        unit="ratio",
        higher_is_better=False,
    ),
    "manager_disclosure_coverage_rate": SLAMetricDefinition(
        name="manager_disclosure_coverage_rate",
        description="Coverage rate for manager-level disclosures.",
        unit="ratio",
        higher_is_better=True,
    ),
    "consultant_disclosure_coverage_rate": SLAMetricDefinition(
        name="consultant_disclosure_coverage_rate",
        description="Coverage rate for consultant disclosures.",
        unit="ratio",
        higher_is_better=True,
    ),
}


@dataclass(frozen=True, slots=True)
class RunQualitySnapshot:
    """Input snapshot for SLA metric computations for a pipeline run."""

    records_total: int
    records_complete: int
    source_published_at: datetime
    run_started_at: datetime
    review_queue_items: int
    review_queue_wait_hours_sum: float
    parse_warning_count: int
    source_mismatch_count: int
    unresolved_official_source_count: int
    total_pages: int
    cited_facts: int
    manager_disclosure_total: int
    manager_disclosure_covered: int
    consultant_disclosure_total: int
    consultant_disclosure_covered: int


@dataclass(frozen=True, slots=True)
class CoverageObservation:
    """Cohort/period observation for disclosure coverage rollups."""

    cohort: str
    period: str
    manager_disclosure_available: bool
    consultant_disclosure_available: bool


def _safe_ratio(numerator: float, denominator: float) -> float:
    if denominator <= 0:
        return 0.0
    return numerator / denominator


def _hours_between(started_at: datetime, published_at: datetime) -> float:
    started = started_at.astimezone(UTC)
    published = published_at.astimezone(UTC)
    hours = (started - published).total_seconds() / 3600
    return max(hours, 0.0)


def compute_sla_metrics(snapshot: RunQualitySnapshot) -> dict[str, float]:
    """Compute run-level SLA metrics from deterministic snapshot inputs."""
    metrics: dict[str, float] = {
        "completeness_rate": _safe_ratio(snapshot.records_complete, snapshot.records_total),
        "freshness_lag_hours": _hours_between(snapshot.run_started_at, snapshot.source_published_at),
        "review_queue_latency_hours": _safe_ratio(
            snapshot.review_queue_wait_hours_sum, snapshot.review_queue_items
        ),
        "parse_warning_rate": _safe_ratio(snapshot.parse_warning_count, snapshot.records_total),
        "citation_density_per_10_pages": _safe_ratio(snapshot.cited_facts * 10, snapshot.total_pages),
        "source_mismatch_rate": _safe_ratio(snapshot.source_mismatch_count, snapshot.records_total),
        "unresolved_official_source_rate": _safe_ratio(
            snapshot.unresolved_official_source_count, snapshot.records_total
        ),
        "manager_disclosure_coverage_rate": _safe_ratio(
            snapshot.manager_disclosure_covered, snapshot.manager_disclosure_total
        ),
        "consultant_disclosure_coverage_rate": _safe_ratio(
            snapshot.consultant_disclosure_covered, snapshot.consultant_disclosure_total
        ),
    }
    return metrics


def aggregate_disclosure_coverage_by_cohort_period(
    observations: list[CoverageObservation],
) -> dict[tuple[str, str], dict[str, float]]:
    """Aggregate manager/consultant disclosure rates by cohort and period."""
    grouped: dict[tuple[str, str], dict[str, float]] = {}
    for row in observations:
        key = (row.cohort, row.period)
        if key not in grouped:
            grouped[key] = {
                "systems_count": 0.0,
                "manager_disclosure_covered_count": 0.0,
                "consultant_disclosure_covered_count": 0.0,
            }
        grouped[key]["systems_count"] += 1.0
        if row.manager_disclosure_available:
            grouped[key]["manager_disclosure_covered_count"] += 1.0
        if row.consultant_disclosure_available:
            grouped[key]["consultant_disclosure_covered_count"] += 1.0

    aggregates: dict[tuple[str, str], dict[str, float]] = {}
    for key in sorted(grouped.keys()):
        stats = grouped[key]
        systems = stats["systems_count"]
        aggregates[key] = {
            "systems_count": systems,
            "manager_disclosure_covered_count": stats["manager_disclosure_covered_count"],
            "consultant_disclosure_covered_count": stats["consultant_disclosure_covered_count"],
            "manager_disclosure_coverage_rate": _safe_ratio(
                stats["manager_disclosure_covered_count"], systems
            ),
            "consultant_disclosure_coverage_rate": _safe_ratio(
                stats["consultant_disclosure_covered_count"], systems
            ),
        }
    return aggregates
