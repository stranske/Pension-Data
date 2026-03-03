"""Tests for scenario and simulation contracts."""

from __future__ import annotations

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
            macro_shocks={"funded_ratio": -0.05},
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
    assert row_map["fee_rate"].scenario_value == pytest.approx(0.008)
    assert row_map["net_return"].scenario_value == pytest.approx(0.02)
    assert result.reproducibility.config_hash
    assert result.reproducibility.source_snapshot_id == "snapshot:2026-03-03"


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
    assert len(result.rows) == len(_baseline_metrics())
    assert all(row.metric_name.endswith(".mean") for row in result.rows)
