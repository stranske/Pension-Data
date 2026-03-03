"""Derived quantitative metric engine with provenance-aware lineage."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from statistics import fmean
from typing import Literal

MetricUnit = Literal["usd", "ratio", "percent", "count"]


@dataclass(frozen=True, slots=True)
class DerivedMetricDefinition:
    """Definition metadata for one derived metric."""

    metric_name: str
    unit: MetricUnit
    formula: str
    required_inputs: tuple[str, ...]
    description: str


@dataclass(frozen=True, slots=True)
class DerivedMetricObservation:
    """One deterministic derived metric value with lineage."""

    metric_name: str
    unit: MetricUnit
    value: float
    plan_id: str
    plan_period: str
    confidence: float | None
    source_fact_ids: tuple[str, ...]
    provenance_refs: tuple[str, ...]
    lineage_formula: str


@dataclass(frozen=True, slots=True)
class AggregatedMetric:
    """Confidence-aware aggregate of multiple derived metric observations."""

    metric_name: str
    weighted_mean: float
    sample_count: int
    confidence_weight_sum: float


@dataclass(frozen=True, slots=True)
class _MetricInput:
    value: float
    confidence: float | None
    fact_id: str
    evidence_refs: tuple[str, ...]
    plan_id: str
    plan_period: str


def default_metric_catalog() -> tuple[DerivedMetricDefinition, ...]:
    """Return the default derived metric catalog for issue #121."""
    return (
        DerivedMetricDefinition(
            metric_name="funded_gap_usd",
            unit="usd",
            formula="aal_usd - ava_usd",
            required_inputs=("aal_usd", "ava_usd"),
            description="Absolute unfunded liability in normalized USD terms.",
        ),
        DerivedMetricDefinition(
            metric_name="unfunded_ratio",
            unit="ratio",
            formula="(aal_usd - ava_usd) / aal_usd",
            required_inputs=("aal_usd", "ava_usd"),
            description="Unfunded share of AAL; lower is better.",
        ),
        DerivedMetricDefinition(
            metric_name="net_cash_flow_usd",
            unit="usd",
            formula=(
                "employer_contributions_normalized + employee_contributions_normalized + "
                "benefit_payments_normalized + refunds_normalized"
            ),
            required_inputs=(
                "employer_contributions_normalized",
                "employee_contributions_normalized",
                "benefit_payments_normalized",
                "refunds_normalized",
            ),
            description="Net cash flow after contributions, benefits, and refunds.",
        ),
        DerivedMetricDefinition(
            metric_name="contribution_to_benefit_ratio",
            unit="ratio",
            formula=(
                "(employer_contributions_normalized + employee_contributions_normalized) / "
                "ABS(benefit_payments_normalized)"
            ),
            required_inputs=(
                "employer_contributions_normalized",
                "employee_contributions_normalized",
                "benefit_payments_normalized",
            ),
            description="Coverage ratio of contributions relative to benefits paid.",
        ),
    )


def _to_float(value: object) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        token = value.strip()
        if not token:
            return None
        try:
            return float(token)
        except ValueError:
            return None
    return None


def _as_refs(values: object) -> tuple[str, ...]:
    if not isinstance(values, list):
        return ()
    refs: list[str] = []
    for value in values:
        if not isinstance(value, str):
            continue
        token = value.strip()
        if token and token not in refs:
            refs.append(token)
    return tuple(refs)


def _build_metric_input_index(
    core_metric_rows: Sequence[Mapping[str, object]],
) -> dict[str, _MetricInput]:
    index: dict[str, _MetricInput] = {}
    for row in core_metric_rows:
        metric_name = row.get("metric_name")
        if not isinstance(metric_name, str) or not metric_name.strip():
            continue
        normalized_value = _to_float(row.get("normalized_value"))
        if normalized_value is None:
            continue
        plan_id = row.get("plan_id")
        plan_period = row.get("plan_period")
        fact_id = row.get("fact_id")
        if not isinstance(plan_id, str) or not plan_id.strip():
            continue
        if not isinstance(plan_period, str) or not plan_period.strip():
            continue
        if not isinstance(fact_id, str) or not fact_id.strip():
            continue
        index[metric_name.strip()] = _MetricInput(
            value=normalized_value,
            confidence=_to_float(row.get("confidence")),
            fact_id=fact_id,
            evidence_refs=_as_refs(row.get("evidence_refs")),
            plan_id=plan_id.strip(),
            plan_period=plan_period.strip(),
        )
    return index


def _cash_flow_input(
    cash_flow_rows: Sequence[Mapping[str, object]],
) -> tuple[str, str, str, dict[str, float], tuple[str, ...]]:
    for row in cash_flow_rows:
        plan_id = row.get("plan_id")
        plan_period = row.get("plan_period")
        cash_flow_id = row.get("cash_flow_id")
        if not isinstance(plan_id, str) or not plan_id.strip():
            continue
        if not isinstance(plan_period, str) or not plan_period.strip():
            continue
        if not isinstance(cash_flow_id, str) or not cash_flow_id.strip():
            continue
        values: dict[str, float] = {}
        for field in (
            "employer_contributions_normalized",
            "employee_contributions_normalized",
            "benefit_payments_normalized",
            "refunds_normalized",
        ):
            parsed = _to_float(row.get(field))
            if parsed is not None:
                values[field] = parsed
        return (
            plan_id.strip(),
            plan_period.strip(),
            cash_flow_id,
            values,
            _as_refs(row.get("evidence_refs")),
        )
    return ("", "", "", {}, ())


def _min_confidence(inputs: Sequence[_MetricInput]) -> float | None:
    confidences = [item.confidence for item in inputs if item.confidence is not None]
    if not confidences:
        return None
    return min(confidences)


def compute_derived_metrics(
    *,
    core_metric_rows: Sequence[Mapping[str, object]],
    cash_flow_rows: Sequence[Mapping[str, object]] = (),
) -> tuple[DerivedMetricObservation, ...]:
    """Compute deterministic derived metrics from staged Pension-Data facts."""
    index = _build_metric_input_index(core_metric_rows)
    observations: list[DerivedMetricObservation] = []

    aal = index.get("aal_usd")
    ava = index.get("ava_usd")
    if aal is not None and ava is not None:
        sources = (aal.fact_id, ava.fact_id)
        refs = tuple(sorted(set(aal.evidence_refs) | set(ava.evidence_refs)))
        confidence = _min_confidence((aal, ava))
        funded_gap = aal.value - ava.value
        observations.append(
            DerivedMetricObservation(
                metric_name="funded_gap_usd",
                unit="usd",
                value=funded_gap,
                plan_id=aal.plan_id,
                plan_period=aal.plan_period,
                confidence=confidence,
                source_fact_ids=sources,
                provenance_refs=refs,
                lineage_formula="aal_usd - ava_usd",
            )
        )
        if aal.value != 0:
            observations.append(
                DerivedMetricObservation(
                    metric_name="unfunded_ratio",
                    unit="ratio",
                    value=funded_gap / aal.value,
                    plan_id=aal.plan_id,
                    plan_period=aal.plan_period,
                    confidence=confidence,
                    source_fact_ids=sources,
                    provenance_refs=refs,
                    lineage_formula="(aal_usd - ava_usd) / aal_usd",
                )
            )

    plan_id, plan_period, cash_flow_id, cash_values, cash_refs = _cash_flow_input(cash_flow_rows)
    if cash_flow_id:
        employer = cash_values.get("employer_contributions_normalized")
        employee = cash_values.get("employee_contributions_normalized")
        benefit = cash_values.get("benefit_payments_normalized")
        refunds = cash_values.get("refunds_normalized")
        if (
            employer is not None
            and employee is not None
            and benefit is not None
            and refunds is not None
        ):
            net_cash_flow = employer + employee + benefit + refunds
            observations.append(
                DerivedMetricObservation(
                    metric_name="net_cash_flow_usd",
                    unit="usd",
                    value=net_cash_flow,
                    plan_id=plan_id,
                    plan_period=plan_period,
                    confidence=None,
                    source_fact_ids=(cash_flow_id,),
                    provenance_refs=cash_refs,
                    lineage_formula=(
                        "employer_contributions_normalized + employee_contributions_normalized + "
                        "benefit_payments_normalized + refunds_normalized"
                    ),
                )
            )
        if employer is not None and employee is not None and benefit is not None and benefit != 0:
            observations.append(
                DerivedMetricObservation(
                    metric_name="contribution_to_benefit_ratio",
                    unit="ratio",
                    value=(employer + employee) / abs(benefit),
                    plan_id=plan_id,
                    plan_period=plan_period,
                    confidence=None,
                    source_fact_ids=(cash_flow_id,),
                    provenance_refs=cash_refs,
                    lineage_formula=(
                        "(employer_contributions_normalized + employee_contributions_normalized) "
                        "/ ABS(benefit_payments_normalized)"
                    ),
                )
            )

    return tuple(
        sorted(
            observations,
            key=lambda row: (row.plan_id, row.plan_period, row.metric_name, row.lineage_formula),
        )
    )


def aggregate_metric_series(
    observations: Sequence[DerivedMetricObservation],
    *,
    metric_name: str,
) -> AggregatedMetric:
    """Aggregate one derived metric with confidence-aware weighting."""
    selected = [row for row in observations if row.metric_name == metric_name]
    if not selected:
        raise ValueError(f"No observations found for metric_name='{metric_name}'")

    weights = [row.confidence if row.confidence is not None else 1.0 for row in selected]
    weighted_sum = sum(row.value * weight for row, weight in zip(selected, weights, strict=True))
    weight_sum = sum(weights)
    if weight_sum <= 0:
        mean_value = fmean(row.value for row in selected)
        return AggregatedMetric(
            metric_name=metric_name,
            weighted_mean=mean_value,
            sample_count=len(selected),
            confidence_weight_sum=0.0,
        )
    return AggregatedMetric(
        metric_name=metric_name,
        weighted_mean=weighted_sum / weight_sum,
        sample_count=len(selected),
        confidence_weight_sum=weight_sum,
    )
