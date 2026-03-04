"""Attribution, optimization sandbox, and experiment tracking contracts."""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import asdict, dataclass
from itertools import product
from pathlib import Path
from typing import Literal

_MAX_GRID_CANDIDATES = 250_000


@dataclass(frozen=True, slots=True)
class AttributionRow:
    """One attribution decomposition row."""

    bucket: str
    weight: float
    return_rate: float
    contribution: float


@dataclass(frozen=True, slots=True)
class AttributionReconciliation:
    """Tolerance-based reconciliation of computed and source aggregate returns."""

    source_aggregate: float
    computed_total: float
    delta: float
    tolerance: float
    within_tolerance: bool


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


def reconcile_attribution(
    *,
    computed_total: float,
    source_aggregate: float,
    tolerance: float = 1e-6,
) -> AttributionReconciliation:
    """Reconcile computed attribution total against a source aggregate."""
    if tolerance < 0:
        raise ValueError("tolerance must be >= 0")
    delta = computed_total - source_aggregate
    return AttributionReconciliation(
        source_aggregate=source_aggregate,
        computed_total=computed_total,
        delta=delta,
        tolerance=tolerance,
        within_tolerance=abs(delta) <= tolerance,
    )


@dataclass(frozen=True, slots=True)
class OptimizationConstraints:
    """Per-bucket min/max constraints and target total weight."""

    min_weight: dict[str, float]
    max_weight: dict[str, float]
    target_total_weight: float = 1.0
    precision_step: float = 0.1


@dataclass(frozen=True, slots=True)
class ConstraintDiagnostics:
    """Constraint satisfaction diagnostics for one optimization result."""

    target_total_weight: float
    realized_total_weight: float
    total_weight_delta: float
    violated_bounds: tuple[str, ...]
    within_tolerance: bool


@dataclass(frozen=True, slots=True)
class OptimizationResult:
    """Best allocation under objective and constraints with diagnostics."""

    objective_value: float
    sensitivity_lambda: float
    weights: dict[str, float]
    expected_return: float
    penalty: float
    diagnostics: ConstraintDiagnostics


def _grid_values(min_weight: float, max_weight: float, step: float) -> tuple[float, ...]:
    span = max_weight - min_weight
    count = int(math.floor((span / step) + 1e-12))
    values = [round(min_weight + (index * step), 10) for index in range(count + 1)]
    if not math.isclose(values[-1], max_weight, rel_tol=0.0, abs_tol=1e-10):
        values.append(round(max_weight, 10))
    return tuple(values)


def _valid_weight_sum(weights: tuple[float, ...], *, target_total_weight: float) -> bool:
    return abs(sum(weights) - target_total_weight) <= 1e-6


def _build_constraint_diagnostics(
    *,
    weights: dict[str, float],
    constraints: OptimizationConstraints,
) -> ConstraintDiagnostics:
    total_weight = sum(weights.values())
    total_delta = total_weight - constraints.target_total_weight
    violated_bounds = tuple(
        sorted(
            bucket
            for bucket, weight in weights.items()
            if weight < (constraints.min_weight[bucket] - 1e-9)
            or weight > (constraints.max_weight[bucket] + 1e-9)
        )
    )
    return ConstraintDiagnostics(
        target_total_weight=constraints.target_total_weight,
        realized_total_weight=total_weight,
        total_weight_delta=total_delta,
        violated_bounds=violated_bounds,
        within_tolerance=(abs(total_delta) <= 1e-6 and not violated_bounds),
    )


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
        diagnostics = _build_constraint_diagnostics(weights=weights, constraints=constraints)
        if not diagnostics.within_tolerance:
            continue
        if best is None or objective > best.objective_value:
            best = OptimizationResult(
                objective_value=objective,
                sensitivity_lambda=sensitivity_lambda,
                weights=weights,
                expected_return=expected_return,
                penalty=penalty,
                diagnostics=diagnostics,
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

    def export_json(self, *, indent: int = 2) -> str:
        """Export registry records as a deterministic JSON document."""
        return json.dumps(self.snapshot(), sort_keys=True, indent=indent)

    def export_json_file(self, path: str) -> str:
        """Write deterministic JSON export to disk."""
        target = Path(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(self.export_json(), encoding="utf-8")
        return str(target)


class QuantExperimentRunner:
    """Convenience runner for recording and comparing quant experiments."""

    def __init__(self, registry: QuantExperimentRegistry | None = None) -> None:
        self.registry = registry or QuantExperimentRegistry()

    def run_optimization_experiment(
        self,
        *,
        experiment_id: str,
        expected_returns: dict[str, float],
        risk_penalties: dict[str, float],
        constraints: OptimizationConstraints,
        sensitivity_lambda: float,
        artifact_links: tuple[str, ...] = (),
        seed: int | None = None,
    ) -> OptimizationResult:
        result = optimize_allocation(
            expected_returns=expected_returns,
            risk_penalties=risk_penalties,
            constraints=constraints,
            sensitivity_lambda=sensitivity_lambda,
        )
        input_payload: dict[str, object] = {
            "expected_returns": expected_returns,
            "risk_penalties": risk_penalties,
            "constraints": asdict(constraints),
            "sensitivity_lambda": sensitivity_lambda,
        }
        output_payload: dict[str, object] = asdict(result)
        record = QuantExperimentRecord(
            experiment_id=experiment_id,
            module="optimization",
            seed=seed,
            input_hash=self.registry.stable_hash(input_payload),
            output_hash=self.registry.stable_hash(output_payload),
            artifact_links=artifact_links,
            objective_value=result.objective_value,
            total_return=None,
        )
        self.registry.add_record(record)
        return result

    def run_attribution_experiment(
        self,
        *,
        experiment_id: str,
        weights: dict[str, float],
        realized_returns: dict[str, float],
        source_aggregate: float | None = None,
        tolerance: float = 1e-6,
        artifact_links: tuple[str, ...] = (),
        seed: int | None = None,
    ) -> tuple[tuple[AttributionRow, ...], float, AttributionReconciliation | None]:
        rows, total_return = compute_attribution(
            weights=weights,
            realized_returns=realized_returns,
        )
        reconciliation: AttributionReconciliation | None = None
        if source_aggregate is not None:
            reconciliation = reconcile_attribution(
                computed_total=total_return,
                source_aggregate=source_aggregate,
                tolerance=tolerance,
            )
        input_payload: dict[str, object] = {
            "weights": weights,
            "realized_returns": realized_returns,
            "source_aggregate": source_aggregate,
            "tolerance": tolerance,
        }
        output_payload: dict[str, object] = {
            "rows": [asdict(row) for row in rows],
            "total_return": total_return,
            "reconciliation": asdict(reconciliation) if reconciliation is not None else None,
        }
        record = QuantExperimentRecord(
            experiment_id=experiment_id,
            module="attribution",
            seed=seed,
            input_hash=self.registry.stable_hash(input_payload),
            output_hash=self.registry.stable_hash(output_payload),
            artifact_links=artifact_links,
            objective_value=None,
            total_return=total_return,
        )
        self.registry.add_record(record)
        return rows, total_return, reconciliation
