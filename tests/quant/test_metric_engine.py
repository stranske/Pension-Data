"""Tests for quantitative derived metric engine."""

from __future__ import annotations

from pension_data.quant.metric_engine import (
    aggregate_metric_series,
    compute_derived_metrics,
    default_metric_catalog,
)


def _core_rows() -> list[dict[str, object]]:
    return [
        {
            "fact_id": "fact:aal",
            "plan_id": "CA-PERS",
            "plan_period": "FY2024",
            "metric_name": "aal_usd",
            "normalized_value": 640.0,
            "confidence": 0.92,
            "evidence_refs": ["p.40"],
        },
        {
            "fact_id": "fact:ava",
            "plan_id": "CA-PERS",
            "plan_period": "FY2024",
            "metric_name": "ava_usd",
            "normalized_value": 501.8,
            "confidence": 0.88,
            "evidence_refs": ["p.40"],
        },
    ]


def _cash_flow_rows() -> list[dict[str, object]]:
    return [
        {
            "cash_flow_id": "flow:2024",
            "plan_id": "CA-PERS",
            "plan_period": "FY2024",
            "employer_contributions_normalized": 18.0,
            "employee_contributions_normalized": 9.0,
            "benefit_payments_normalized": -5.0,
            "refunds_normalized": -1.0,
            "evidence_refs": ["p.55"],
        }
    ]


def test_default_metric_catalog_contains_documented_formulas() -> None:
    catalog = default_metric_catalog()
    names = tuple(item.metric_name for item in catalog)
    assert names == (
        "funded_gap_usd",
        "unfunded_ratio",
        "net_cash_flow_usd",
        "contribution_to_benefit_ratio",
    )


def test_compute_derived_metrics_emits_expected_observations() -> None:
    observations = compute_derived_metrics(
        core_metric_rows=_core_rows(),
        cash_flow_rows=_cash_flow_rows(),
    )
    assert tuple(item.metric_name for item in observations) == (
        "contribution_to_benefit_ratio",
        "funded_gap_usd",
        "net_cash_flow_usd",
        "unfunded_ratio",
    )

    funded_gap = next(item for item in observations if item.metric_name == "funded_gap_usd")
    assert round(funded_gap.value, 2) == 138.20
    assert funded_gap.source_fact_ids == ("fact:aal", "fact:ava")
    assert funded_gap.confidence == 0.88

    net_cash_flow = next(item for item in observations if item.metric_name == "net_cash_flow_usd")
    assert net_cash_flow.value == 21.0
    assert net_cash_flow.source_fact_ids == ("flow:2024",)


def test_confidence_weighted_aggregation_uses_confidence_when_present() -> None:
    observations = compute_derived_metrics(core_metric_rows=_core_rows(), cash_flow_rows=())
    duplicated = [*observations, *observations]
    unfunded_ratio = next(item for item in observations if item.metric_name == "unfunded_ratio")
    aggregate = aggregate_metric_series(duplicated, metric_name="unfunded_ratio")
    assert aggregate.metric_name == "unfunded_ratio"
    assert aggregate.sample_count == 2
    assert aggregate.confidence_weight_sum == 1.76
    assert round(aggregate.weighted_mean, 6) == round(unfunded_ratio.value, 6)
