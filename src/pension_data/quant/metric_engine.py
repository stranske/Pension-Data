"""Derived quantitative metric engine with provenance-aware lineage."""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from statistics import fmean
from typing import Literal

from pension_data.quant.contracts import (
    QuantDataPoint,
    QuantSeriesContract,
    normalize_provenance_refs,
)

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


GroupKey = tuple[str, str]


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


def _bounded_confidence(value: object) -> float | None:
    parsed = _to_float(value)
    if parsed is None or not math.isfinite(parsed):
        return None
    if parsed < 0:
        return 0.0
    if parsed > 1:
        return 1.0
    return parsed


def _as_refs(values: object) -> tuple[str, ...]:
    if isinstance(values, (str, bytes, bytearray)):
        return ()
    if not isinstance(values, Sequence):
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
) -> dict[GroupKey, dict[str, _MetricInput]]:
    index: dict[GroupKey, dict[str, _MetricInput]] = {}
    ordered_rows = sorted(
        core_metric_rows,
        key=lambda row: (
            str(row.get("plan_id") or ""),
            str(row.get("plan_period") or ""),
            str(row.get("metric_name") or ""),
            str(row.get("fact_id") or ""),
        ),
    )
    for row in ordered_rows:
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
        group_key = (plan_id.strip(), plan_period.strip())
        group_metrics = index.setdefault(group_key, {})
        group_metrics[metric_name.strip()] = _MetricInput(
            value=normalized_value,
            confidence=_bounded_confidence(row.get("confidence")),
            fact_id=fact_id,
            evidence_refs=_as_refs(row.get("evidence_refs")),
            plan_id=group_key[0],
            plan_period=group_key[1],
        )
    return index


def _cash_flow_inputs(
    cash_flow_rows: Sequence[Mapping[str, object]],
) -> tuple[tuple[str, str, str, dict[str, float], float | None, tuple[str, ...]], ...]:
    entries: list[tuple[str, str, str, dict[str, float], float | None, tuple[str, ...]]] = []
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
        confidence = _bounded_confidence(row.get("confidence"))
        entries.append(
            (
                plan_id.strip(),
                plan_period.strip(),
                cash_flow_id,
                values,
                confidence,
                _as_refs(row.get("evidence_refs")),
            )
        )
    return tuple(sorted(entries, key=lambda item: (item[0], item[1], item[2])))


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

    for _, group_metrics in sorted(index.items()):
        aal = group_metrics.get("aal_usd")
        ava = group_metrics.get("ava_usd")
        if aal is None or ava is None:
            continue
        sources = tuple(sorted((aal.fact_id, ava.fact_id)))
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

    for plan_id, plan_period, cash_flow_id, cash_values, cash_confidence, cash_refs in _cash_flow_inputs(
        cash_flow_rows
    ):
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
                    confidence=cash_confidence,
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
                    confidence=cash_confidence,
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


def _series_label(metric_name: str) -> str:
    return metric_name.replace("_", " ").title()


def build_metric_series_contracts(
    observations: Sequence[DerivedMetricObservation],
) -> tuple[QuantSeriesContract, ...]:
    """Project derived observations into chart-ready quant series contracts."""
    grouped: dict[tuple[str, str], list[DerivedMetricObservation]] = {}
    for row in observations:
        grouped.setdefault((row.plan_id, row.metric_name), []).append(row)

    contracts: list[QuantSeriesContract] = []
    for plan_id, metric_name in sorted(grouped):
        ordered_rows = sorted(
            grouped[(plan_id, metric_name)],
            key=lambda row: (row.plan_period, row.lineage_formula),
        )
        points = tuple(
            QuantDataPoint(
                x_label=row.plan_period,
                y_value=row.value,
                y_unit=row.unit,
                confidence=row.confidence,
                provenance_refs=normalize_provenance_refs(row.provenance_refs),
            )
            for row in ordered_rows
        )
        contracts.append(
            QuantSeriesContract(
                series_id=f"{plan_id}:{metric_name}",
                metric_name=metric_name,
                label=f"{plan_id} {_series_label(metric_name)}",
                chart_kind="line",
                points=points,
            )
        )

    return tuple(contracts)
