"""Tests for attribution decomposition, optimization, and experiment registry."""

from __future__ import annotations

import pytest

from pension_data.quant.attribution_optimization import (
    OptimizationConstraints,
    QuantExperimentRecord,
    QuantExperimentRegistry,
    compute_attribution,
    optimize_allocation,
)


def test_attribution_reconciles_to_total_return() -> None:
    rows, total_return = compute_attribution(
        weights={"equity": 0.6, "fixed_income": 0.4},
        realized_returns={"equity": 0.08, "fixed_income": 0.03},
    )
    assert len(rows) == 2
    assert round(total_return, 6) == round((0.6 * 0.08) + (0.4 * 0.03), 6)
    assert round(sum(row.contribution for row in rows), 6) == round(total_return, 6)


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
    assert result.objective_value == result.expected_return - (0.5 * result.penalty)


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
