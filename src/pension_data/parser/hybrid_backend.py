"""Hybrid deterministic/Docling table-backend routing for complex actuarial tables."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Protocol, runtime_checkable

from pension_data.db.models.funded_actuarial import FUNDED_ACTUARIAL_REQUIRED_METRICS
from pension_data.extract.actuarial.metrics import (
    RawFundedActuarialInput,
    extract_funded_and_actuarial_metrics,
)
from pension_data.quality.confidence import ConfidenceRoutingDecision


@dataclass(frozen=True, slots=True)
class BackendMetricValue:
    """One metric extracted by a parser backend with backend-specific provenance."""

    metric_name: str
    normalized_value: float | None
    as_reported_value: float | None
    normalized_unit: str | None
    as_reported_unit: str | None
    confidence: float
    backend: str
    evidence_refs: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class BackendExtraction:
    """Metric extraction result from one parser backend."""

    backend: str
    values: tuple[BackendMetricValue, ...]
    diagnostics: tuple[str, ...] = ()


@runtime_checkable
class ParserBackend(Protocol):
    """Parser-backend interface used by deterministic and self-hosted Docling paths."""

    backend_name: str

    def extract(
        self,
        *,
        plan_id: str,
        plan_period: str,
        raw: RawFundedActuarialInput,
    ) -> BackendExtraction:
        """Return normalized metric values for a raw actuarial parser payload."""


@dataclass(frozen=True, slots=True)
class HybridBackendConfig:
    """Feature flags and routing thresholds for the hybrid backend."""

    enable_docling: bool = False
    complex_confidence_threshold: float = 0.75
    value_tolerance: float = 1e-9


@dataclass(frozen=True, slots=True)
class HybridBackendResult:
    """Resolved hybrid extraction output plus review-routing evidence."""

    selected_values: tuple[BackendMetricValue, ...]
    deterministic: BackendExtraction
    docling: BackendExtraction | None
    docling_attempted: bool
    routed_to_docling: bool
    review_decisions: tuple[ConfidenceRoutingDecision, ...]


DoclingExtractor = Callable[
    [RawFundedActuarialInput], Sequence[BackendMetricValue | Mapping[str, object]]
]


class DeterministicActuarialBackend:
    """Adapter that exposes the existing deterministic parser through the backend contract."""

    backend_name = "deterministic"

    def extract(
        self,
        *,
        plan_id: str,
        plan_period: str,
        raw: RawFundedActuarialInput,
    ) -> BackendExtraction:
        facts, diagnostics = extract_funded_and_actuarial_metrics(
            plan_id=plan_id,
            plan_period=plan_period,
            raw=raw,
        )
        values = tuple(
            BackendMetricValue(
                metric_name=fact.metric_name,
                normalized_value=fact.normalized_value,
                as_reported_value=fact.as_reported_value,
                normalized_unit=fact.normalized_unit,
                as_reported_unit=fact.as_reported_unit,
                confidence=fact.confidence,
                backend=self.backend_name,
                evidence_refs=(*fact.evidence_refs, "backend:deterministic"),
            )
            for fact in facts
        )
        return BackendExtraction(
            backend=self.backend_name,
            values=values,
            diagnostics=tuple(item.code for item in diagnostics),
        )


class SelfHostedDoclingBackend:
    """Docling backend hook that is opt-in and local-only.

    The repository does not call a managed document-AI service here. Production wiring can pass a
    callable backed by a local Docling/TableFormer runtime; tests use the same contract with an
    in-process extractor.
    """

    backend_name = "docling"

    def __init__(self, extractor: DoclingExtractor | None = None) -> None:
        self._extractor = extractor

    def extract(
        self,
        *,
        plan_id: str,
        plan_period: str,
        raw: RawFundedActuarialInput,
    ) -> BackendExtraction:
        del plan_id, plan_period
        if self._extractor is None:
            raise RuntimeError("Docling backend requires a self-hosted local extractor")
        values = tuple(_coerce_docling_value(item) for item in self._extractor(raw))
        return BackendExtraction(backend=self.backend_name, values=values)


def _coerce_docling_value(item: BackendMetricValue | Mapping[str, object]) -> BackendMetricValue:
    if isinstance(item, BackendMetricValue):
        if item.backend != "docling":
            return BackendMetricValue(
                metric_name=item.metric_name,
                normalized_value=item.normalized_value,
                as_reported_value=item.as_reported_value,
                normalized_unit=item.normalized_unit,
                as_reported_unit=item.as_reported_unit,
                confidence=item.confidence,
                backend="docling",
                evidence_refs=(*item.evidence_refs, "backend:docling"),
            )
        if "backend:docling" in item.evidence_refs:
            return item
        return BackendMetricValue(
            metric_name=item.metric_name,
            normalized_value=item.normalized_value,
            as_reported_value=item.as_reported_value,
            normalized_unit=item.normalized_unit,
            as_reported_unit=item.as_reported_unit,
            confidence=item.confidence,
            backend=item.backend,
            evidence_refs=(*item.evidence_refs, "backend:docling"),
        )

    evidence_refs = _coerce_evidence_refs(item.get("evidence_refs"))
    return BackendMetricValue(
        metric_name=str(item["metric_name"]),
        normalized_value=_optional_float(item.get("normalized_value")),
        as_reported_value=_optional_float(item.get("as_reported_value")),
        normalized_unit=_optional_str(item.get("normalized_unit")),
        as_reported_unit=_optional_str(item.get("as_reported_unit")),
        confidence=_required_float(item.get("confidence", 0.86)),
        backend="docling",
        evidence_refs=(*evidence_refs, "backend:docling"),
    )


def _coerce_evidence_refs(value: object) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        return (value,)
    if isinstance(value, Sequence):
        return tuple(str(ref) for ref in value)
    raise TypeError("evidence_refs must be a sequence of strings")


def _optional_float(value: object) -> float | None:
    return None if value is None else _required_float(value)


def _required_float(value: object) -> float:
    if isinstance(value, int | float | str):
        return float(value)
    raise TypeError(f"expected numeric value, got {type(value).__name__}")


def _optional_str(value: object) -> str | None:
    return None if value is None else str(value)


def _complex_table_hint(raw: RawFundedActuarialInput) -> bool:
    for row in raw.table_rows:
        if row.get("complex_table", "").lower() in {"1", "true", "yes"}:
            return True
        year_columns = [key for key in row if key.isdigit() and len(key) == 4]
        if len(year_columns) >= 2:
            return True
        if row.get("header_depth") not in {None, "", "1"}:
            return True
    return False


def _should_route_to_docling(
    *,
    raw: RawFundedActuarialInput,
    deterministic: BackendExtraction,
    config: HybridBackendConfig,
) -> bool:
    by_metric = {value.metric_name: value for value in deterministic.values}
    missing_required = any(
        metric_name not in by_metric for metric_name in FUNDED_ACTUARIAL_REQUIRED_METRICS
    )
    low_confidence = any(
        value.confidence < config.complex_confidence_threshold for value in deterministic.values
    )
    diagnostic_signal = any(
        code in {"missing_metric", "ambiguous_metric"} for code in deterministic.diagnostics
    )
    return _complex_table_hint(raw) or missing_required or low_confidence or diagnostic_signal


def _build_disagreement_decisions(
    *,
    plan_id: str,
    plan_period: str,
    deterministic: BackendExtraction,
    docling: BackendExtraction,
    tolerance: float,
) -> tuple[ConfidenceRoutingDecision, ...]:
    deterministic_by_metric = {value.metric_name: value for value in deterministic.values}
    decisions: list[ConfidenceRoutingDecision] = []
    for docling_value in docling.values:
        deterministic_value = deterministic_by_metric.get(docling_value.metric_name)
        if deterministic_value is None:
            continue
        if deterministic_value.normalized_value is None or docling_value.normalized_value is None:
            continue
        if abs(deterministic_value.normalized_value - docling_value.normalized_value) <= tolerance:
            continue
        metric = docling_value.metric_name
        decisions.append(
            ConfidenceRoutingDecision(
                row_id=f"hybrid-disagreement:{plan_id}:{plan_period}:{metric}",
                plan_id=plan_id,
                plan_period=plan_period,
                metric_name=metric,
                confidence=min(deterministic_value.confidence, docling_value.confidence),
                routing_outcome="high_priority_review",
                review_priority="high",
                publish_blocked=True,
                evidence_refs=(
                    *deterministic_value.evidence_refs,
                    *docling_value.evidence_refs,
                    "hybrid:backend_disagreement",
                ),
            )
        )
    return tuple(decisions)


def _select_primary_values(
    deterministic: BackendExtraction,
    docling: BackendExtraction | None,
) -> tuple[BackendMetricValue, ...]:
    selected_by_metric = {value.metric_name: value for value in deterministic.values}
    if docling is not None:
        for value in docling.values:
            selected_by_metric.setdefault(value.metric_name, value)
    return tuple(selected_by_metric[key] for key in sorted(selected_by_metric))


def run_hybrid_table_extraction(
    *,
    plan_id: str,
    plan_period: str,
    raw: RawFundedActuarialInput,
    config: HybridBackendConfig | None = None,
    deterministic_backend: ParserBackend | None = None,
    docling_backend: ParserBackend | None = None,
) -> HybridBackendResult:
    """Run deterministic extraction and optional self-hosted Docling cross-checking."""
    active_config = config or HybridBackendConfig()
    deterministic = (deterministic_backend or DeterministicActuarialBackend()).extract(
        plan_id=plan_id,
        plan_period=plan_period,
        raw=raw,
    )
    routed_to_docling = active_config.enable_docling and _should_route_to_docling(
        raw=raw,
        deterministic=deterministic,
        config=active_config,
    )
    docling: BackendExtraction | None = None
    review_decisions: tuple[ConfidenceRoutingDecision, ...] = ()
    if routed_to_docling:
        backend = docling_backend or SelfHostedDoclingBackend()
        docling = backend.extract(plan_id=plan_id, plan_period=plan_period, raw=raw)
        review_decisions = _build_disagreement_decisions(
            plan_id=plan_id,
            plan_period=plan_period,
            deterministic=deterministic,
            docling=docling,
            tolerance=active_config.value_tolerance,
        )

    return HybridBackendResult(
        selected_values=_select_primary_values(deterministic, docling),
        deterministic=deterministic,
        docling=docling,
        docling_attempted=routed_to_docling,
        routed_to_docling=routed_to_docling,
        review_decisions=review_decisions,
    )
