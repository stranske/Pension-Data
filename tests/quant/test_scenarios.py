"""Tests for scenario and simulation contracts."""

from __future__ import annotations

import math

import pytest

from pension_data.quant.scenarios import (
    ScenarioInput,
    ScenarioRunConfig,
    run_deterministic_scenario,
    run_monte_carlo_scenario,
)


def _baseline_metrics() -> dict[str, float]:
    return {
        "funded_ratio": 0.784,
        "employer_contributions": 18.0,
        "employee_contributions": 9.0,
        "fee_rate": 0.0055,
        "net_return": 0.061,
    }


def test_deterministic_scenario_applies_shocks_and_adjustments() -> None:
    result = run_deterministic_scenario(
        plan_id="CA-PERS",
        plan_period="FY2024",
        baseline_metrics=_baseline_metrics(),
        scenario=ScenarioInput(
            name="stress-up",
            macro_shocks={"funded_ratio": -0.05, "employee_contributions": 1},
            contribution_delta=1.0,
            fee_delta_bps=25.0,
            return_override=0.02,
        ),
        config=ScenarioRunConfig(module_version="v0.1.0"),
        source_snapshot_id="snapshot:2026-03-03",
    )

    row_map = {row.metric_name: row for row in result.rows}
    assert result.mode == "deterministic"
    assert row_map["funded_ratio"].scenario_value == pytest.approx(0.734)
    assert row_map["employer_contributions"].scenario_value == pytest.approx(19.0)
    assert row_map["employee_contributions"].scenario_value == pytest.approx(11.0)
    assert row_map["fee_rate"].scenario_value == pytest.approx(0.008)
    assert row_map["net_return"].scenario_value == pytest.approx(0.02)
    assert result.reproducibility.config_hash
    assert result.reproducibility.source_snapshot_id == "snapshot:2026-03-03"
    assert result.reproducibility.run_id.startswith("scenario:deterministic:CA-PERS:FY2024:")


def test_simulation_requires_seed_for_reproducibility() -> None:
    with pytest.raises(ValueError, match="random_seed is required"):
        run_monte_carlo_scenario(
            plan_id="CA-PERS",
            plan_period="FY2024",
            baseline_metrics=_baseline_metrics(),
            scenario=ScenarioInput(name="seed-required", macro_shocks={}),
            config=ScenarioRunConfig(module_version="v0.1.0", random_seed=None),
            source_snapshot_id="snapshot:2026-03-03",
        )


def test_simulation_output_shape_is_chart_ready() -> None:
    result = run_monte_carlo_scenario(
        plan_id="CA-PERS",
        plan_period="FY2024",
        baseline_metrics=_baseline_metrics(),
        scenario=ScenarioInput(name="mc-10", macro_shocks={"funded_ratio": -0.03}),
        config=ScenarioRunConfig(module_version="v0.1.0", random_seed=42, simulation_draws=100),
        source_snapshot_id="snapshot:2026-03-03",
    )

    assert result.mode == "simulation"
    assert result.reproducibility.random_seed == 42
    assert len(result.rows) == (3 * len(_baseline_metrics()))
    suffixes = {row.metric_name.split(".")[-1] for row in result.rows}
    assert suffixes == {"mean", "p05", "p95"}


NON_FINITE_BASELINES = (float("nan"), float("inf"), float("-inf"))


@pytest.mark.parametrize("bad_value", NON_FINITE_BASELINES)
def test_scenarios_reject_non_finite_baseline_metrics(bad_value: float) -> None:
    """A NaN/inf baseline must raise, not flow into chart-ready result rows."""
    baseline = _baseline_metrics()
    baseline["funded_ratio"] = bad_value

    with pytest.raises(ValueError, match=r"baseline_metrics\[funded_ratio\] must be a finite"):
        run_deterministic_scenario(
            plan_id="CA-PERS",
            plan_period="FY2024",
            baseline_metrics=baseline,
            scenario=ScenarioInput(name="bad-baseline", macro_shocks={"funded_ratio": -0.01}),
            config=ScenarioRunConfig(module_version="v0.1.0"),
            source_snapshot_id="snapshot:2026-03-03",
        )

    with pytest.raises(ValueError, match=r"baseline_metrics\[funded_ratio\] must be a finite"):
        run_monte_carlo_scenario(
            plan_id="CA-PERS",
            plan_period="FY2024",
            baseline_metrics=baseline,
            scenario=ScenarioInput(name="bad-baseline", macro_shocks={"funded_ratio": -0.01}),
            config=ScenarioRunConfig(module_version="v0.1.0", random_seed=42, simulation_draws=100),
            source_snapshot_id="snapshot:2026-03-03",
        )


def test_scenarios_reject_empty_baseline_metric_name() -> None:
    with pytest.raises(ValueError, match="baseline_metrics keys must be non-empty"):
        run_deterministic_scenario(
            plan_id="CA-PERS",
            plan_period="FY2024",
            baseline_metrics={"  ": 0.5},
            scenario=ScenarioInput(name="blank-key", macro_shocks={}),
            config=ScenarioRunConfig(module_version="v0.1.0"),
            source_snapshot_id="snapshot:2026-03-03",
        )


def test_finite_baselines_still_produce_rows_in_both_modes() -> None:
    """The guard must reject only non-finite baselines, not valid runs."""
    kwargs = {
        "plan_id": "CA-PERS",
        "plan_period": "FY2024",
        "baseline_metrics": _baseline_metrics(),
        "scenario": ScenarioInput(name="ok", macro_shocks={"funded_ratio": -0.02}),
        "source_snapshot_id": "snapshot:2026-03-03",
    }
    deterministic = run_deterministic_scenario(
        config=ScenarioRunConfig(module_version="v0.1.0"), **kwargs
    )
    simulation = run_monte_carlo_scenario(
        config=ScenarioRunConfig(module_version="v0.1.0", random_seed=7, simulation_draws=50),
        **kwargs,
    )

    assert len(deterministic.rows) == len(_baseline_metrics())
    assert len(simulation.rows) == 3 * len(_baseline_metrics())
    assert all(math.isfinite(row.baseline_value) for row in deterministic.rows)
    assert all(math.isfinite(row.scenario_value) for row in simulation.rows)
