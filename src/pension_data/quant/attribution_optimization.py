"""Attribution, optimization sandbox, and experiment tracking contracts."""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import asdict, dataclass
from itertools import product
from typing import Literal

_MAX_GRID_CANDIDATES = 250_000


@dataclass(frozen=True, slots=True)
class AttributionRow:
    """One attribution decomposition row."""

    bucket: str
    weight: float
    return_rate: float
    contribution: float


def compute_attribution(
    *,
    weights: dict[str, float],
    realized_returns: dict[str, float],
) -> tuple[tuple[AttributionRow, ...], float]:
    """Compute contribution attribution and total portfolio return."""
    rows: list[AttributionRow] = []
    for bucket in sorted(weights):
        weight = weights[bucket]
        if bucket not in realized_returns:
            raise ValueError(f"missing realized return for bucket '{bucket}'")
        return_rate = realized_returns[bucket]
        rows.append(
            AttributionRow(
                bucket=bucket,
                weight=weight,
                return_rate=return_rate,
                contribution=weight * return_rate,
            )
        )
    total_return = sum(row.contribution for row in rows)
    return tuple(rows), total_return


@dataclass(frozen=True, slots=True)
class OptimizationConstraints:
    """Per-bucket min/max constraints and target total weight."""

    min_weight: dict[str, float]
    max_weight: dict[str, float]
    target_total_weight: float = 1.0
    precision_step: float = 0.1


@dataclass(frozen=True, slots=True)
class OptimizationResult:
    """Best allocation under objective and constraints."""

    objective_value: float
    sensitivity_lambda: float
    weights: dict[str, float]
    expected_return: float
    penalty: float


def _grid_values(min_weight: float, max_weight: float, step: float) -> tuple[float, ...]:
    span = max_weight - min_weight
    count = int(math.floor((span / step) + 1e-12))
    values = [round(min_weight + (index * step), 10) for index in range(count + 1)]
    if not math.isclose(values[-1], max_weight, rel_tol=0.0, abs_tol=1e-10):
        values.append(round(max_weight, 10))
    return tuple(values)


def _valid_weight_sum(weights: tuple[float, ...], *, target_total_weight: float) -> bool:
    return abs(sum(weights) - target_total_weight) <= 1e-6


def optimize_allocation(
    *,
    expected_returns: dict[str, float],
    risk_penalties: dict[str, float],
    constraints: OptimizationConstraints,
    sensitivity_lambda: float,
) -> OptimizationResult:
    """Optimize bucket weights via deterministic grid search."""
    if not expected_returns:
        raise ValueError("expected_returns is required")
    if sensitivity_lambda < 0:
        raise ValueError("sensitivity_lambda must be >= 0")
    if constraints.precision_step <= 0:
        raise ValueError("precision_step must be > 0")

    buckets = sorted(expected_returns)
    for bucket in buckets:
        if bucket not in constraints.min_weight or bucket not in constraints.max_weight:
            raise ValueError(f"missing constraints for bucket '{bucket}'")
        if bucket not in risk_penalties:
            raise ValueError(f"missing risk penalty for bucket '{bucket}'")
        min_weight = constraints.min_weight[bucket]
        max_weight = constraints.max_weight[bucket]
        if min_weight > max_weight:
            raise ValueError(f"invalid constraints for bucket '{bucket}': min_weight > max_weight")

    value_ranges = [
        _grid_values(
            constraints.min_weight[bucket],
            constraints.max_weight[bucket],
            constraints.precision_step,
        )
        for bucket in buckets
    ]
    candidate_count = 1
    for values in value_ranges:
        candidate_count *= len(values)
    if candidate_count > _MAX_GRID_CANDIDATES:
        raise ValueError(
            "optimization grid too large for sandbox execution; "
            "adjust precision_step or bucket constraints"
        )

    best: OptimizationResult | None = None
    for candidate in product(*value_ranges):
        if not _valid_weight_sum(candidate, target_total_weight=constraints.target_total_weight):
            continue
        weights = dict(zip(buckets, candidate, strict=True))
        expected_return = sum(weights[bucket] * expected_returns[bucket] for bucket in buckets)
        penalty = sum((weights[bucket] ** 2) * risk_penalties[bucket] for bucket in buckets)
        objective = expected_return - (sensitivity_lambda * penalty)
        if best is None or objective > best.objective_value:
            best = OptimizationResult(
                objective_value=objective,
                sensitivity_lambda=sensitivity_lambda,
                weights=weights,
                expected_return=expected_return,
                penalty=penalty,
            )

    if best is None:
        raise ValueError("no feasible allocation for provided constraints")
    return best


@dataclass(frozen=True, slots=True)
class QuantExperimentRecord:
    """Auditable experiment registry record."""

    experiment_id: str
    module: Literal["attribution", "optimization"]
    seed: int | None
    input_hash: str
    output_hash: str
    artifact_links: tuple[str, ...]
    objective_value: float | None = None
    total_return: float | None = None


@dataclass(frozen=True, slots=True)
class ExperimentComparison:
    """Comparison summary between two registry experiments."""

    left_experiment_id: str
    right_experiment_id: str
    objective_delta: float | None
    total_return_delta: float | None


class QuantExperimentRegistry:
    """In-memory deterministic experiment registry."""

    def __init__(self) -> None:
        self._records: dict[str, QuantExperimentRecord] = {}

    @staticmethod
    def stable_hash(payload: dict[str, object]) -> str:
        encoded = json.dumps(payload, sort_keys=True).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()

    def add_record(self, record: QuantExperimentRecord) -> None:
        if record.experiment_id in self._records:
            raise ValueError(f"duplicate experiment_id '{record.experiment_id}'")
        self._records[record.experiment_id] = record

    def compare(self, left_experiment_id: str, right_experiment_id: str) -> ExperimentComparison:
        if left_experiment_id not in self._records:
            raise ValueError(f"unknown experiment_id '{left_experiment_id}'")
        if right_experiment_id not in self._records:
            raise ValueError(f"unknown experiment_id '{right_experiment_id}'")
        left = self._records[left_experiment_id]
        right = self._records[right_experiment_id]

        objective_delta: float | None = None
        if left.objective_value is not None and right.objective_value is not None:
            objective_delta = right.objective_value - left.objective_value

        total_return_delta: float | None = None
        if left.total_return is not None and right.total_return is not None:
            total_return_delta = right.total_return - left.total_return

        return ExperimentComparison(
            left_experiment_id=left_experiment_id,
            right_experiment_id=right_experiment_id,
            objective_delta=objective_delta,
            total_return_delta=total_return_delta,
        )

    def snapshot(self) -> tuple[dict[str, object], ...]:
        return tuple(
            asdict(self._records[experiment_id]) for experiment_id in sorted(self._records)
        )
