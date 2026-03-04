"""Tests for attribution decomposition, optimization, and experiment registry."""

from __future__ import annotations

import pytest

from pension_data.quant.attribution_optimization import (
    OptimizationConstraints,
    QuantExperimentRecord,
    QuantExperimentRegistry,
    QuantExperimentRunner,
    compute_attribution,
    optimize_allocation,
    reconcile_attribution,
)


def test_attribution_reconciles_to_total_return() -> None:
    rows, total_return = compute_attribution(
        weights={"equity": 0.6, "fixed_income": 0.4},
        realized_returns={"equity": 0.08, "fixed_income": 0.03},
    )
    assert len(rows) == 2
    assert round(total_return, 6) == round((0.6 * 0.08) + (0.4 * 0.03), 6)
    assert round(sum(row.contribution for row in rows), 6) == round(total_return, 6)


def test_attribution_reconciliation_supports_source_aggregate_tolerance() -> None:
    rows, total_return = compute_attribution(
        weights={"equity": 0.6, "fixed_income": 0.4},
        realized_returns={"equity": 0.08, "fixed_income": 0.03},
    )
    del rows
    reconciliation = reconcile_attribution(
        computed_total=total_return,
        source_aggregate=0.06001,
        tolerance=0.0001,
    )
    assert reconciliation.within_tolerance is True
    assert reconciliation.delta == pytest.approx(-0.00001, abs=1e-8)


def test_optimizer_respects_weight_constraints() -> None:
    result = optimize_allocation(
        expected_returns={"equity": 0.08, "fixed_income": 0.04},
        risk_penalties={"equity": 0.12, "fixed_income": 0.04},
        constraints=OptimizationConstraints(
            min_weight={"equity": 0.2, "fixed_income": 0.2},
            max_weight={"equity": 0.8, "fixed_income": 0.8},
            target_total_weight=1.0,
            precision_step=0.1,
        ),
        sensitivity_lambda=0.5,
    )

    assert 0.2 <= result.weights["equity"] <= 0.8
    assert 0.2 <= result.weights["fixed_income"] <= 0.8
    assert round(sum(result.weights.values()), 6) == 1.0
    assert result.objective_value == pytest.approx(result.expected_return - (0.5 * result.penalty))
    assert result.diagnostics.within_tolerance is True
    assert result.diagnostics.violated_bounds == ()
    assert result.diagnostics.total_weight_delta == pytest.approx(0.0, abs=1e-6)


def test_optimizer_rejects_invalid_constraint_ranges() -> None:
    with pytest.raises(ValueError, match="min_weight > max_weight"):
        optimize_allocation(
            expected_returns={"equity": 0.08},
            risk_penalties={"equity": 0.12},
            constraints=OptimizationConstraints(
                min_weight={"equity": 0.7},
                max_weight={"equity": 0.4},
                target_total_weight=1.0,
                precision_step=0.1,
            ),
            sensitivity_lambda=0.5,
        )


def test_experiment_registry_rejects_duplicate_experiment_ids() -> None:
    registry = QuantExperimentRegistry()
    record = QuantExperimentRecord(
        experiment_id="exp-a",
        module="optimization",
        seed=42,
        input_hash=registry.stable_hash({"name": "baseline"}),
        output_hash=registry.stable_hash({"objective": 0.021}),
        artifact_links=("artifacts/exp-a.json",),
        objective_value=0.021,
        total_return=0.054,
    )
    registry.add_record(record)
    with pytest.raises(ValueError, match="duplicate experiment_id"):
        registry.add_record(record)


def test_experiment_registry_comparison_emits_deltas() -> None:
    registry = QuantExperimentRegistry()
    registry.add_record(
        QuantExperimentRecord(
            experiment_id="exp-a",
            module="optimization",
            seed=42,
            input_hash=registry.stable_hash({"name": "baseline"}),
            output_hash=registry.stable_hash({"objective": 0.021}),
            artifact_links=("artifacts/exp-a.json",),
            objective_value=0.021,
            total_return=0.054,
        )
    )
    registry.add_record(
        QuantExperimentRecord(
            experiment_id="exp-b",
            module="optimization",
            seed=42,
            input_hash=registry.stable_hash({"name": "stress"}),
            output_hash=registry.stable_hash({"objective": 0.018}),
            artifact_links=("artifacts/exp-b.json",),
            objective_value=0.018,
            total_return=0.05,
        )
    )

    comparison = registry.compare("exp-a", "exp-b")
    assert comparison.objective_delta == pytest.approx(-0.003)
    assert comparison.total_return_delta == pytest.approx(-0.004)


def test_experiment_registry_exports_deterministic_json() -> None:
    registry = QuantExperimentRegistry()
    registry.add_record(
        QuantExperimentRecord(
            experiment_id="exp-a",
            module="optimization",
            seed=1,
            input_hash=registry.stable_hash({"a": 1}),
            output_hash=registry.stable_hash({"b": 2}),
            artifact_links=("artifacts/exp-a.json",),
            objective_value=0.012,
            total_return=None,
        )
    )
    export_payload = registry.export_json()
    assert '"experiment_id": "exp-a"' in export_payload
    assert '"module": "optimization"' in export_payload


def test_quant_experiment_runner_runs_and_compares_optimization_experiments() -> None:
    runner = QuantExperimentRunner()
    constraints = OptimizationConstraints(
        min_weight={"equity": 0.2, "fixed_income": 0.2},
        max_weight={"equity": 0.8, "fixed_income": 0.8},
        target_total_weight=1.0,
        precision_step=0.1,
    )
    baseline = runner.run_optimization_experiment(
        experiment_id="opt-base",
        expected_returns={"equity": 0.08, "fixed_income": 0.04},
        risk_penalties={"equity": 0.12, "fixed_income": 0.04},
        constraints=constraints,
        sensitivity_lambda=0.5,
        artifact_links=("artifacts/opt-base.json",),
        seed=7,
    )
    stress = runner.run_optimization_experiment(
        experiment_id="opt-stress",
        expected_returns={"equity": 0.075, "fixed_income": 0.05},
        risk_penalties={"equity": 0.12, "fixed_income": 0.04},
        constraints=constraints,
        sensitivity_lambda=0.5,
        artifact_links=("artifacts/opt-stress.json",),
        seed=7,
    )

    comparison = runner.registry.compare("opt-base", "opt-stress")
    assert baseline.diagnostics.within_tolerance is True
    assert stress.diagnostics.within_tolerance is True
    assert comparison.objective_delta is not None
