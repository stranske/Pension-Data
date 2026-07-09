"""Finite/bounds guards at numeric trust boundaries (issue #636).

Each test targets a boundary where a bare ``<0``/``>1`` guard previously let NaN or
+/-inf through. Reverting the corresponding ``math.isfinite`` guard makes the matching
test here fail (the deliberate-break demonstration required by the issue).
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from pension_data.finite_guards import finite_or_none, is_finite_number, require_finite
from pension_data.normalize.financial_units import normalize_money_to_usd
from pension_data.quality.anomaly_rules import (
    AnomalyThresholds,
    TimeSeriesPoint,
    _detect_funded_shift,
)
from pension_data.quality.confidence import ExtractionConfidenceInput, route_confidence_row
from pension_data.quality.sla_metrics import _safe_ratio
from pension_data.quant.metric_engine import _to_float

NON_FINITE = [float("nan"), float("inf"), float("-inf")]


# --- shared helper -----------------------------------------------------------
def test_finite_guards_helpers() -> None:
    assert is_finite_number(1.0) and is_finite_number(0)
    assert not is_finite_number(True)  # bool excluded
    for v in NON_FINITE + [None, "x"]:
        assert not is_finite_number(v)
    assert require_finite(2.5, field="x") == 2.5
    for v in NON_FINITE:
        with pytest.raises(ValueError):
            require_finite(v, field="x")
    assert finite_or_none(3.0) == 3.0
    assert all(finite_or_none(v) is None for v in NON_FINITE + [None])


# --- confidence routing (the P0: NaN must never auto-accept) ------------------
def _conf(value: float) -> ExtractionConfidenceInput:
    return ExtractionConfidenceInput(
        row_id="r", plan_id="p", plan_period="FY2024", metric_name="funded_ratio", confidence=value
    )


@pytest.mark.parametrize("value", NON_FINITE)
def test_route_confidence_row_non_finite_never_auto_accepts(value: float) -> None:
    d = route_confidence_row(_conf(value))
    assert d.routing_outcome == "high_priority_review"
    assert d.publish_blocked is True
    assert d.confidence == 0.0  # not the false 1.0 the old clamp produced


def test_route_confidence_row_finite_still_auto_accepts() -> None:
    assert route_confidence_row(_conf(0.96)).routing_outcome == "auto_accept"


# --- money normalization ------------------------------------------------------
@pytest.mark.parametrize("value", NON_FINITE)
def test_normalize_money_rejects_non_finite(value: float) -> None:
    with pytest.raises(ValueError):
        normalize_money_to_usd(value, unit_scale="million_usd")


def test_normalize_money_finite_ok_and_none_passthrough() -> None:
    assert normalize_money_to_usd(1.5, unit_scale="million_usd") == 1_500_000.0
    assert normalize_money_to_usd(None, unit_scale="usd") is None


# --- SLA ratios ---------------------------------------------------------------
def test_safe_ratio_flags_non_finite() -> None:
    assert _safe_ratio(float("nan"), 10.0) == 0.0
    assert _safe_ratio(float("inf"), 10.0) == 0.0
    assert _safe_ratio(5.0, float("nan")) == 0.0
    assert _safe_ratio(5.0, float("inf")) == 0.0  # would be 0.0 finite, but guarded anyway
    assert _safe_ratio(3.0, 4.0) == 0.75


# --- derived-metric float coercion -------------------------------------------
@pytest.mark.parametrize("value", NON_FINITE + ["nan", "inf", "-inf"])
def test_to_float_drops_non_finite(value: object) -> None:
    assert _to_float(value) is None


def test_to_float_finite_ok() -> None:
    assert _to_float(2.5) == 2.5
    assert _to_float("2.5") == 2.5


# --- anomaly detection --------------------------------------------------------
def _point(funded_ratio: float) -> TimeSeriesPoint:
    return TimeSeriesPoint(
        plan_id="p",
        period="FY2024",
        observed_at=datetime(2024, 6, 30, tzinfo=UTC),
        funded_ratio=funded_ratio,
        allocations={},
        confidence=0.9,
        evidence_refs=("p.1",),
        provenance={},
    )


@pytest.mark.parametrize("bad", NON_FINITE)
def test_funded_shift_skips_non_finite_points(bad: float) -> None:
    # A corrupt current point must not be treated as anomaly-free (NaN) or emit an
    # inf-scored anomaly that poisons sorting.
    out = _detect_funded_shift(
        previous=_point(0.80), current=_point(bad), thresholds=AnomalyThresholds()
    )
    assert out == []


def test_funded_shift_detects_real_shift() -> None:
    out = _detect_funded_shift(
        previous=_point(0.80), current=_point(0.60), thresholds=AnomalyThresholds()
    )
    assert out  # a real 20pt drop is still flagged (guard skips only non-finite points)
