"""Scenario and simulation analysis layer for quantitative exploration."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from numbers import Real
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
        if isinstance(shock, bool) or not isinstance(shock, Real):
            raise ValueError("macro_shocks values must be numeric")


def _normalized_macro_shocks(values: Mapping[str, float]) -> dict[str, float]:
    normalized: dict[str, float] = {}
    for metric_name, shock in sorted(values.items(), key=lambda item: item[0]):
        normalized[metric_name.strip()] = float(shock)
    return normalized


def _config_hash(scenario: ScenarioInput, config: ScenarioRunConfig, mode: ScenarioMode) -> str:
    scenario_payload = {
        "name": scenario.name,
        "macro_shocks": _normalized_macro_shocks(scenario.macro_shocks),
        "contribution_delta": scenario.contribution_delta,
        "fee_delta_bps": scenario.fee_delta_bps,
        "return_override": scenario.return_override,
    }
    config_payload = asdict(config)
    payload = {
        "mode": mode,
        "scenario": scenario_payload,
        "config": config_payload,
    }
    encoded = json.dumps(payload, sort_keys=True).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _metadata(
    *,
    plan_id: str,
    plan_period: str,
    scenario: ScenarioInput,
    config: ScenarioRunConfig,
    mode: ScenarioMode,
    source_snapshot_id: str,
) -> ReproducibilityMetadata:
    run_token = "|".join(
        (
            plan_id.strip(),
            plan_period.strip(),
            mode,
            scenario.name.strip(),
            source_snapshot_id.strip(),
        )
    )
    run_fingerprint = hashlib.sha256(run_token.encode("utf-8")).hexdigest()[:12]
    return ReproducibilityMetadata(
        run_id=f"scenario:{mode}:{plan_id.strip()}:{plan_period.strip()}:{run_fingerprint}",
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
            plan_id=plan_id,
            plan_period=plan_period,
            scenario=scenario,
            config=config,
            mode="deterministic",
            source_snapshot_id=source_snapshot_id,
        ),
    )


def _percentile(values: list[float], *, q: float) -> float:
    if not values:
        raise ValueError("values must be non-empty")
    if q < 0 or q > 1:
        raise ValueError("q must be between 0 and 1")
    ordered = sorted(values)
    index = int(round((len(ordered) - 1) * q))
    return ordered[index]


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
        simulated_p05 = _percentile(draws, q=0.05)
        simulated_p95 = _percentile(draws, q=0.95)
        rows.extend(
            (
                ScenarioResultRow(
                    metric_name=f"{metric_name}.mean",
                    baseline_value=baseline_value,
                    scenario_value=simulated_mean,
                    delta_value=simulated_mean - baseline_value,
                ),
                ScenarioResultRow(
                    metric_name=f"{metric_name}.p05",
                    baseline_value=baseline_value,
                    scenario_value=simulated_p05,
                    delta_value=simulated_p05 - baseline_value,
                ),
                ScenarioResultRow(
                    metric_name=f"{metric_name}.p95",
                    baseline_value=baseline_value,
                    scenario_value=simulated_p95,
                    delta_value=simulated_p95 - baseline_value,
                ),
            )
        )
    return ScenarioResult(
        mode="simulation",
        scenario_name=scenario.name,
        plan_id=plan_id,
        plan_period=plan_period,
        rows=tuple(rows),
        reproducibility=_metadata(
            plan_id=plan_id,
            plan_period=plan_period,
            scenario=scenario,
            config=config,
            mode="simulation",
            source_snapshot_id=source_snapshot_id,
        ),
    )
