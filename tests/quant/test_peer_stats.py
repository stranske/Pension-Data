"""Tests for plan-agnostic peer-benchmark statistics."""

from __future__ import annotations

import math

from pension_data.quant.peer_stats import (
    benchmark_metric,
    percentile_rank,
    trend_change,
    z_score,
)


def test_percentile_rank_basic() -> None:
    peers = [10.0, 20.0, 30.0, 40.0]
    assert percentile_rank(30.0, peers) == 75.0
    assert percentile_rank(5.0, peers) == 0.0
    assert percentile_rank(100.0, peers) == 100.0


def test_percentile_rank_rejects_non_finite_subject_and_peers() -> None:
    assert percentile_rank(float("nan"), [1.0, 2.0]) is None
    assert percentile_rank(float("inf"), [1.0, 2.0]) is None
    # non-finite peers are dropped, not propagated: finite peers [1.0, 3.0], 2.0 beats one
    assert percentile_rank(2.0, [1.0, float("nan"), 3.0]) == 50.0
    assert percentile_rank(1.0, []) is None


def test_z_score_and_guards() -> None:
    peers = [1.0, 2.0, 3.0, 4.0, 5.0]  # mean 3, pstdev sqrt(2)
    z = z_score(5.0, peers)
    assert z is not None and math.isclose(z, 2.0 / math.sqrt(2.0), abs_tol=1e-4)
    assert z_score(5.0, [3.0]) is None  # fewer than two peers
    assert z_score(5.0, [2.0, 2.0, 2.0]) is None  # zero spread
    assert z_score(float("nan"), peers) is None


def test_trend_change() -> None:
    assert trend_change([0.78, 0.80, 0.81]) == 0.03
    assert trend_change([0.81]) is None
    assert trend_change([float("nan"), 0.80, 0.85]) == 0.05


def test_benchmark_metric_direction() -> None:
    # funded ratio: higher is better
    res = benchmark_metric("funded_ratio", 0.85, [0.70, 0.80, 0.90, 0.95], higher_is_better=True)
    assert res.subject_is_finite is True
    assert res.peer_median == 0.85
    assert res.percentile == 50.0
    assert res.favorable_percentile == 50.0
    assert res.delta_vs_mean is not None

    # fees: lower is better -> favorable percentile is inverted
    res_low = benchmark_metric("fee_bps", 30.0, [10.0, 20.0, 40.0, 50.0], higher_is_better=False)
    assert res_low.percentile == 50.0
    assert res_low.favorable_percentile == 50.0
    res_low2 = benchmark_metric("fee_bps", 15.0, [10.0, 20.0, 40.0, 50.0], higher_is_better=False)
    assert res_low2.percentile == 25.0
    assert res_low2.favorable_percentile == 75.0  # cheap fee = favorable


def test_benchmark_metric_nonfinite_subject_keeps_peer_context() -> None:
    res = benchmark_metric("funded_ratio", float("nan"), [0.7, 0.8, 0.9])
    assert res.subject_is_finite is False
    assert res.subject_value is None
    assert res.percentile is None and res.z_score is None and res.delta_vs_mean is None
    # peer context still reported
    assert res.peer_mean is not None and res.peer_median == 0.8 and res.n_peers == 3


def test_benchmark_metric_no_finite_peers() -> None:
    res = benchmark_metric("x", 1.0, [float("nan"), float("inf")])
    assert res.n_peers == 0
    assert res.peer_mean is None and res.percentile is None
