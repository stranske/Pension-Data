"""Plan-agnostic peer-benchmark statistics.

Position a single subject plan's metric against a peer universe: percentile rank,
z-score, peer mean/median, and a simple trend. Every input is finite-guarded so a
NaN/inf cannot silently produce a misleading rank (cf. the finite/bounds discipline
used in :mod:`pension_data.quant.metric_engine`).

Pure functions over plain values — no I/O, no plan-specific assumptions. The caller
supplies the subject value and the peer values (sourced however: PPD, internal data,
etc.), so this works for any plan and any numeric metric.
"""

from __future__ import annotations

import math
import statistics
from dataclasses import dataclass

__all__ = [
    "PeerMetricResult",
    "benchmark_metric",
    "percentile_rank",
    "trend_change",
    "z_score",
]


def _finite(values: list[float] | tuple[float, ...]) -> list[float]:
    """Return only the finite values, dropping None/NaN/inf."""
    return [float(v) for v in values if v is not None and math.isfinite(v)]


def percentile_rank(value: float, peers: list[float] | tuple[float, ...]) -> float | None:
    """Percentile of ``value`` within ``peers`` as a 0-100 number.

    Uses the "<=" definition: the fraction of peer observations at or below
    ``value``. Returns ``None`` if ``value`` is non-finite or there are no finite
    peers. The subject is NOT assumed to be a member of ``peers``.
    """
    if value is None or not math.isfinite(value):
        return None
    finite_peers = _finite(peers)
    if not finite_peers:
        return None
    at_or_below = sum(1 for p in finite_peers if p <= value)
    return round(100.0 * at_or_below / len(finite_peers), 4)


def z_score(value: float, peers: list[float] | tuple[float, ...]) -> float | None:
    """Standard score of ``value`` vs the peer distribution.

    Returns ``None`` when ``value`` is non-finite, there are fewer than two finite
    peers, or the peer standard deviation is zero (no spread to normalise against).
    """
    if value is None or not math.isfinite(value):
        return None
    finite_peers = _finite(peers)
    if len(finite_peers) < 2:
        return None
    sigma = statistics.pstdev(finite_peers)
    if sigma == 0.0 or not math.isfinite(sigma):
        return None
    return round((value - statistics.fmean(finite_peers)) / sigma, 4)


def trend_change(series: list[float] | tuple[float, ...]) -> float | None:
    """Net change across an ordered series (last finite minus first finite).

    The caller is responsible for ordering the series (e.g. by fiscal year). Returns
    ``None`` if fewer than two finite points are present.
    """
    finite_points = _finite(series)
    if len(finite_points) < 2:
        return None
    return round(finite_points[-1] - finite_points[0], 6)


@dataclass(frozen=True, slots=True)
class PeerMetricResult:
    """One metric for a subject plan positioned against its peer cohort."""

    metric_name: str
    subject_value: float | None
    n_peers: int
    peer_mean: float | None
    peer_median: float | None
    delta_vs_mean: float | None
    percentile: float | None
    favorable_percentile: float | None
    z_score: float | None
    subject_is_finite: bool


def benchmark_metric(
    metric_name: str,
    subject_value: float | None,
    peer_values: list[float] | tuple[float, ...],
    *,
    higher_is_better: bool = True,
) -> PeerMetricResult:
    """Position one metric for a subject plan against a peer cohort.

    ``higher_is_better`` controls ``favorable_percentile`` only: for a "lower is
    better" metric (e.g. fees, assumed return treated as risk) the favorable
    percentile is ``100 - percentile`` so that a higher favorable percentile always
    means "better than more peers". The raw ``percentile`` is always the <= rank.

    A non-finite subject value yields a result with ``subject_is_finite=False`` and
    ``None`` for every subject-relative statistic, while still reporting the peer
    mean/median (the peer cohort is still informative).
    """
    finite_peers = _finite(peer_values)
    n_peers = len(finite_peers)
    peer_mean = round(statistics.fmean(finite_peers), 6) if finite_peers else None
    peer_median = round(statistics.median(finite_peers), 6) if finite_peers else None

    subject_finite = subject_value is not None and math.isfinite(subject_value)
    if not subject_finite:
        return PeerMetricResult(
            metric_name=metric_name,
            subject_value=None,
            n_peers=n_peers,
            peer_mean=peer_mean,
            peer_median=peer_median,
            delta_vs_mean=None,
            percentile=None,
            favorable_percentile=None,
            z_score=None,
            subject_is_finite=False,
        )

    value = float(subject_value)  # type: ignore[arg-type]
    pct = percentile_rank(value, finite_peers)
    favorable = None if pct is None else round(pct if higher_is_better else 100.0 - pct, 4)
    delta = None if peer_mean is None else round(value - peer_mean, 6)
    return PeerMetricResult(
        metric_name=metric_name,
        subject_value=value,
        n_peers=n_peers,
        peer_mean=peer_mean,
        peer_median=peer_median,
        delta_vs_mean=delta,
        percentile=pct,
        favorable_percentile=favorable,
        z_score=z_score(value, finite_peers),
        subject_is_finite=True,
    )
