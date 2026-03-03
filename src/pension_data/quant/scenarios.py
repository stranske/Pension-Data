"""Scenario and simulation analysis layer for quantitative exploration."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from random import Random
from statistics import fmean
from typing import Literal

ScenarioMode = Literal["deterministic", "simulation"]


@dataclass(frozen=True, slots=True)
class ScenarioInput:
    """Scenario input schema for deterministic and simulated analysis runs."""

    name: str
    macro_shocks: Mapping[str, float]
    contribution_delta: float = 0.0
    fee_delta_bps: float = 0.0
    return_override: float | None = None


@dataclass(frozen=True, slots=True)
class ScenarioRunConfig:
    """Reproducibility and execution config for scenario runs."""

    module_version: str
    random_seed: int | None = None
    simulation_draws: int = 500


@dataclass(frozen=True, slots=True)
class ReproducibilityMetadata:
    """Reproducibility metadata carried by every scenario output."""

    run_id: str
    config_hash: str
    module_version: str
    random_seed: int | None
    source_snapshot_id: str


@dataclass(frozen=True, slots=True)
class ScenarioResultRow:
    """One scenario result row consumable by chart/table layers."""

    metric_name: str
    baseline_value: float
    scenario_value: float
    delta_value: float


@dataclass(frozen=True, slots=True)
class ScenarioResult:
    """Scenario run output contract."""

    mode: ScenarioMode
    scenario_name: str
    plan_id: str
    plan_period: str
    rows: tuple[ScenarioResultRow, ...]
    reproducibility: ReproducibilityMetadata


def _validate_input(scenario: ScenarioInput, config: ScenarioRunConfig) -> None:
    if not scenario.name.strip():
        raise ValueError("scenario.name is required")
    if not config.module_version.strip():
        raise ValueError("config.module_version is required")
    if config.simulation_draws < 10:
        raise ValueError("config.simulation_draws must be >= 10")
    for metric_name, shock in scenario.macro_shocks.items():
        if not metric_name.strip():
            raise ValueError("macro_shocks keys must be non-empty")
        if not isinstance(shock, float):
            raise ValueError("macro_shocks values must be floats")


def _config_hash(scenario: ScenarioInput, config: ScenarioRunConfig, mode: ScenarioMode) -> str:
    payload = {
        "mode": mode,
        "scenario": asdict(scenario),
        "config": asdict(config),
    }
    encoded = json.dumps(payload, sort_keys=True).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _metadata(
    *,
    scenario: ScenarioInput,
    config: ScenarioRunConfig,
    mode: ScenarioMode,
    source_snapshot_id: str,
) -> ReproducibilityMetadata:
    return ReproducibilityMetadata(
        run_id=f"scenario:{mode}:{scenario.name}",
        config_hash=_config_hash(scenario, config, mode),
        module_version=config.module_version,
        random_seed=config.random_seed,
        source_snapshot_id=source_snapshot_id,
    )


def _scenario_metric_value(
    *,
    metric_name: str,
    baseline_value: float,
    scenario: ScenarioInput,
) -> float:
    value = baseline_value
    if metric_name in scenario.macro_shocks:
        value += scenario.macro_shocks[metric_name]
    if metric_name in {"employer_contributions", "employee_contributions"}:
        value += scenario.contribution_delta
    if metric_name == "fee_rate":
        value += scenario.fee_delta_bps / 10_000.0
    if metric_name == "net_return" and scenario.return_override is not None:
        value = scenario.return_override
    return value


def run_deterministic_scenario(
    *,
    plan_id: str,
    plan_period: str,
    baseline_metrics: Mapping[str, float],
    scenario: ScenarioInput,
    config: ScenarioRunConfig,
    source_snapshot_id: str,
) -> ScenarioResult:
    """Run deterministic baseline-vs-scenario comparison."""
    _validate_input(scenario, config)
    rows: list[ScenarioResultRow] = []
    for metric_name in sorted(baseline_metrics):
        baseline_value = baseline_metrics[metric_name]
        scenario_value = _scenario_metric_value(
            metric_name=metric_name,
            baseline_value=baseline_value,
            scenario=scenario,
        )
        rows.append(
            ScenarioResultRow(
                metric_name=metric_name,
                baseline_value=baseline_value,
                scenario_value=scenario_value,
                delta_value=scenario_value - baseline_value,
            )
        )
    return ScenarioResult(
        mode="deterministic",
        scenario_name=scenario.name,
        plan_id=plan_id,
        plan_period=plan_period,
        rows=tuple(rows),
        reproducibility=_metadata(
            scenario=scenario,
            config=config,
            mode="deterministic",
            source_snapshot_id=source_snapshot_id,
        ),
    )


def run_monte_carlo_scenario(
    *,
    plan_id: str,
    plan_period: str,
    baseline_metrics: Mapping[str, float],
    scenario: ScenarioInput,
    config: ScenarioRunConfig,
    source_snapshot_id: str,
) -> ScenarioResult:
    """Run seeded simulation and return mean/p05/p95-style output rows."""
    _validate_input(scenario, config)
    if config.random_seed is None:
        raise ValueError("config.random_seed is required for simulation mode")
    rng = Random(config.random_seed)
    rows: list[ScenarioResultRow] = []
    for metric_name in sorted(baseline_metrics):
        baseline_value = baseline_metrics[metric_name]
        deterministic_value = _scenario_metric_value(
            metric_name=metric_name,
            baseline_value=baseline_value,
            scenario=scenario,
        )
        draws = [
            deterministic_value + rng.gauss(0.0, max(1e-9, abs(deterministic_value) * 0.01))
            for _ in range(config.simulation_draws)
        ]
        simulated_mean = fmean(draws)
        rows.append(
            ScenarioResultRow(
                metric_name=f"{metric_name}.mean",
                baseline_value=baseline_value,
                scenario_value=simulated_mean,
                delta_value=simulated_mean - baseline_value,
            )
        )
    return ScenarioResult(
        mode="simulation",
        scenario_name=scenario.name,
        plan_id=plan_id,
        plan_period=plan_period,
        rows=tuple(rows),
        reproducibility=_metadata(
            scenario=scenario,
            config=config,
            mode="simulation",
            source_snapshot_id=source_snapshot_id,
        ),
    )
