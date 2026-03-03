"""Shared quantitative analysis contracts for web and desktop experiences."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Literal

QuantModule = Literal["metric_engine", "scenario_analysis", "attribution_optimization"]
ChartKind = Literal["line", "bar", "area", "scatter", "table"]


def normalize_provenance_refs(values: Sequence[str]) -> tuple[str, ...]:
    """Return deterministic, deduplicated provenance references."""
    normalized: list[str] = []
    for value in values:
        token = value.strip()
        if token and token not in normalized:
            normalized.append(token)
    return tuple(normalized)


@dataclass(frozen=True, slots=True)
class ReproducibilityEnvelope:
    """Reproducibility fields required for each quant run output."""

    run_id: str
    config_hash: str
    code_version: str
    input_snapshot_id: str
    generated_at: str
    seed: int | None = None
    source_artifact_ids: tuple[str, ...] = ()


def missing_reproducibility_fields(
    envelope: ReproducibilityEnvelope,
    *,
    requires_seed: bool,
) -> tuple[str, ...]:
    """Return missing reproducibility fields for deterministic validation."""
    missing: list[str] = []
    if not envelope.run_id.strip():
        missing.append("run_id")
    if not envelope.config_hash.strip():
        missing.append("config_hash")
    if not envelope.code_version.strip():
        missing.append("code_version")
    if not envelope.input_snapshot_id.strip():
        missing.append("input_snapshot_id")
    if not envelope.generated_at.strip():
        missing.append("generated_at")
    if requires_seed and envelope.seed is None:
        missing.append("seed")
    if not envelope.source_artifact_ids:
        missing.append("source_artifact_ids")
    return tuple(missing)


@dataclass(frozen=True, slots=True)
class QuantDataPoint:
    """One quant datapoint with provenance and confidence context."""

    x_label: str
    y_value: float | None
    y_unit: str
    confidence: float | None
    provenance_refs: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class QuantSeriesContract:
    """Chart-ready time/sequence series emitted by quant modules."""

    series_id: str
    metric_name: str
    label: str
    chart_kind: ChartKind
    points: tuple[QuantDataPoint, ...]


@dataclass(frozen=True, slots=True)
class QuantScenarioContract:
    """Scenario or experiment output contract consumed by UX layers."""

    scenario_id: str
    scenario_label: str
    module: QuantModule
    baseline_scenario_id: str | None
    reproducibility: ReproducibilityEnvelope
    series: tuple[QuantSeriesContract, ...]
    warnings: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class QuantWorkspaceContract:
    """Top-level quant payload for browser and desktop workspaces."""

    plan_id: str
    plan_period: str
    as_of_date: str
    module: QuantModule
    scenarios: tuple[QuantScenarioContract, ...]
