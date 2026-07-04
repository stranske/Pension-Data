"""Tests for hybrid deterministic/Docling actuarial table backends."""

from __future__ import annotations

from pension_data.extract.actuarial.metrics import RawFundedActuarialInput
from pension_data.parser.hybrid_backend import (
    BackendMetricValue,
    HybridBackendConfig,
    SelfHostedDoclingBackend,
    run_hybrid_table_extraction,
)
from pension_data.review_queue.extraction import build_extraction_review_queue


def _raw_complex_table(*, deterministic_funded_ratio: str = "50.0%") -> RawFundedActuarialInput:
    return RawFundedActuarialInput(
        source_document_id="doc:calpers:complex-actuarial",
        source_url="file://fixtures/complex-actuarial-table.pdf",
        effective_date="2024-06-30",
        ingestion_date="2026-07-04",
        default_money_unit_scale="million_usd",
        text_blocks=(),
        table_rows=(
            {
                "label": "Funded ratio",
                "value": deterministic_funded_ratio,
                "evidence_ref": "p.12#table",
                "complex_table": "true",
                "2023": "78.0%",
                "2024": deterministic_funded_ratio,
            },
            {
                "label": "AAL",
                "value": "$410.0 million",
                "evidence_ref": "p.12#table",
            },
        ),
    )


def _docling_values(_: RawFundedActuarialInput) -> tuple[BackendMetricValue, ...]:
    return (
        BackendMetricValue(
            metric_name="funded_ratio",
            normalized_value=0.812,
            as_reported_value=81.2,
            normalized_unit="ratio",
            as_reported_unit="percent",
            confidence=0.91,
            backend="docling",
            evidence_refs=("p.12#docling-tableformer",),
        ),
        BackendMetricValue(
            metric_name="aal_usd",
            normalized_value=410_000_000.0,
            as_reported_value=410.0,
            normalized_unit="usd",
            as_reported_unit="million_usd",
            confidence=0.89,
            backend="docling",
            evidence_refs=("p.12#docling-tableformer",),
        ),
    )


def test_docling_backend_is_opt_in_for_complex_tables() -> None:
    result = run_hybrid_table_extraction(
        plan_id="CA-PERS",
        plan_period="FY2024",
        raw=_raw_complex_table(),
        config=HybridBackendConfig(enable_docling=False),
        docling_backend=SelfHostedDoclingBackend(_docling_values),
    )

    assert result.docling_attempted is False
    assert result.docling is None
    assert {value.backend for value in result.selected_values} == {"deterministic"}


def test_complex_table_routes_to_self_hosted_docling_with_backend_provenance() -> None:
    result = run_hybrid_table_extraction(
        plan_id="CA-PERS",
        plan_period="FY2024",
        raw=_raw_complex_table(),
        config=HybridBackendConfig(enable_docling=True),
        docling_backend=SelfHostedDoclingBackend(_docling_values),
    )

    assert result.docling_attempted is True
    assert result.docling is not None
    docling_by_metric = {value.metric_name: value for value in result.docling.values}
    assert docling_by_metric["funded_ratio"].normalized_value == 0.812
    assert docling_by_metric["aal_usd"].normalized_value == 410_000_000.0
    assert docling_by_metric["funded_ratio"].backend == "docling"
    assert "backend:docling" in docling_by_metric["funded_ratio"].evidence_refs


def test_backend_disagreement_routes_to_review_without_silent_overwrite() -> None:
    result = run_hybrid_table_extraction(
        plan_id="CA-PERS",
        plan_period="FY2024",
        raw=_raw_complex_table(deterministic_funded_ratio="50.0%"),
        config=HybridBackendConfig(enable_docling=True),
        docling_backend=SelfHostedDoclingBackend(_docling_values),
    )

    selected_by_metric = {value.metric_name: value for value in result.selected_values}
    assert selected_by_metric["funded_ratio"].backend == "deterministic"
    assert selected_by_metric["funded_ratio"].normalized_value == 0.5
    assert len(result.review_decisions) == 1
    decision = result.review_decisions[0]
    assert decision.metric_name == "funded_ratio"
    assert decision.routing_outcome == "high_priority_review"
    assert decision.review_priority == "high"
    assert decision.publish_blocked is True
    assert "hybrid:backend_disagreement" in decision.evidence_refs

    review_rows = build_extraction_review_queue(result.review_decisions)
    assert len(review_rows) == 1
    assert review_rows[0].priority == "high"
    assert review_rows[0].row_id.startswith("hybrid-disagreement:")


def test_matching_backend_values_do_not_create_review_items() -> None:
    def matching_docling(_: RawFundedActuarialInput) -> tuple[BackendMetricValue, ...]:
        return (
            BackendMetricValue(
                metric_name="funded_ratio",
                normalized_value=0.5,
                as_reported_value=50.0,
                normalized_unit="ratio",
                as_reported_unit="percent",
                confidence=0.93,
                backend="docling",
                evidence_refs=("p.12#docling-tableformer",),
            ),
        )

    result = run_hybrid_table_extraction(
        plan_id="CA-PERS",
        plan_period="FY2024",
        raw=_raw_complex_table(deterministic_funded_ratio="50.0%"),
        config=HybridBackendConfig(enable_docling=True),
        docling_backend=SelfHostedDoclingBackend(matching_docling),
    )

    assert result.docling_attempted is True
    assert result.review_decisions == ()
