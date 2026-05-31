"""Tests for page-level evidence capture, linkage, and citation exports."""

from __future__ import annotations

import pytest

from pension_data.db.models.core_facts import (
    BitemporalFactContext,
    DualReportedValue,
    FundedStatusFact,
)
from pension_data.db.models.provenance import MetricEvidenceLink
from pension_data.provenance import (
    EvidenceValidationError,
    build_core_metric_evidence_artifacts,
    export_citation_ready_provenance_payload,
)


def _context(*, source_document_id: str) -> BitemporalFactContext:
    return BitemporalFactContext(
        plan_id="CA-PERS",
        plan_period="FY2025",
        effective_date="2025-06-30",
        ingestion_date="2026-01-15",
        benchmark_version="v1",
        source_document_id=source_document_id,
    )


def _value() -> DualReportedValue:
    return DualReportedValue(
        as_reported_value=0.8,
        normalized_value=0.8,
        as_reported_unit="ratio",
        normalized_unit="ratio",
    )


def test_core_metric_evidence_artifacts_link_rows_to_page_and_text_references() -> None:
    funded_facts = (
        FundedStatusFact(
            context=_context(source_document_id="doc:ca:2025:funded"),
            metric_name="funded_ratio",
            metric_value=_value(),
            confidence=0.95,
            evidence_refs=("p.45", "text:2"),
        ),
    )
    artifacts = build_core_metric_evidence_artifacts(funded_facts=funded_facts)

    evidence_refs = artifacts["evidence_references"]
    links = artifacts["metric_evidence_links"]
    assert len(evidence_refs) == 2
    assert len(links) == 2
    assert any(ref.page_number == 45 for ref in evidence_refs)
    assert any(ref.snippet_anchor == "text:2" for ref in evidence_refs)
    assert all(link.metric_name == "funded_ratio" for link in links)


def test_missing_evidence_for_high_impact_metric_raises_or_warns() -> None:
    funded_facts = (
        FundedStatusFact(
            context=_context(source_document_id="doc:ca:2025:funded"),
            metric_name="funded_ratio",
            metric_value=_value(),
            confidence=0.92,
            evidence_refs=(),
        ),
    )

    with pytest.raises(EvidenceValidationError, match="missing evidence refs"):
        build_core_metric_evidence_artifacts(funded_facts=funded_facts)

    relaxed = build_core_metric_evidence_artifacts(
        funded_facts=funded_facts,
        strict=False,
    )
    warnings = relaxed["validation_warnings"]
    assert len(warnings) == 1
    assert "funded/funded_ratio" in warnings[0]


def test_metric_evidence_link_confidence_defaults_none_and_holds_per_link_float() -> None:
    # Default is None when unset (back-compat for the many existing call sites).
    bare = MetricEvidenceLink(
        link_id="lnk:1",
        metric_row_id="row:1",
        metric_family="funded",
        metric_name="funded_ratio",
        evidence_ref_id="ev:1",
    )
    assert bare.confidence is None

    # A per-link float can be carried independently of any fact-level confidence.
    scored = MetricEvidenceLink(
        link_id="lnk:2",
        metric_row_id="row:1",
        metric_family="funded",
        metric_name="funded_ratio",
        evidence_ref_id="ev:2",
        confidence=0.42,
    )
    assert scored.confidence == pytest.approx(0.42)


def test_builder_threads_per_link_confidence_from_fact() -> None:
    funded_facts = (
        FundedStatusFact(
            context=_context(source_document_id="doc:ca:2025:funded"),
            metric_name="funded_ratio",
            metric_value=_value(),
            confidence=0.91,
            evidence_refs=("p.45", "text:2"),
        ),
    )
    artifacts = build_core_metric_evidence_artifacts(funded_facts=funded_facts)
    links = artifacts["metric_evidence_links"]
    assert links
    assert all(link.confidence == pytest.approx(0.91) for link in links)


def test_table_derived_finding_populates_method_table() -> None:
    # Parser table rows emit page locators with a table section marker. That
    # must surface method="table" through the real artifact builder path.
    funded_facts = (
        FundedStatusFact(
            context=_context(source_document_id="doc:ca:2025:funded"),
            metric_name="funded_ratio",
            metric_value=_value(),
            confidence=0.93,
            evidence_refs=("p.40#table", "text:2"),
        ),
    )
    artifacts = build_core_metric_evidence_artifacts(funded_facts=funded_facts)
    evidence_refs = artifacts["evidence_references"]
    methods = {ref.method for ref in evidence_refs}
    assert "table" in methods
    assert "text" in methods


def test_citation_export_surfaces_excerpt_method_and_confidence() -> None:
    funded_facts = (
        FundedStatusFact(
            context=_context(source_document_id="doc:ca:2025:funded"),
            metric_name="funded_ratio",
            metric_value=_value(),
            confidence=0.88,
            evidence_refs=("p.40#table",),
        ),
    )
    artifacts = build_core_metric_evidence_artifacts(funded_facts=funded_facts)
    payload = export_citation_ready_provenance_payload(
        metric_evidence_links=artifacts["metric_evidence_links"],
        evidence_references=artifacts["evidence_references"],
    )
    citation = list(payload.values())[0]["citations"][0]
    assert citation["method"] == "table"
    assert citation["confidence"] == pytest.approx(0.88)
    assert "excerpt" in citation


def test_citation_export_includes_source_locator_metadata() -> None:
    funded_facts = (
        FundedStatusFact(
            context=_context(source_document_id="doc:ca:2025:funded"),
            metric_name="funded_ratio",
            metric_value=_value(),
            confidence=0.95,
            evidence_refs=("p.52#funded ratio table",),
        ),
    )
    artifacts = build_core_metric_evidence_artifacts(funded_facts=funded_facts)
    payload = export_citation_ready_provenance_payload(
        metric_evidence_links=artifacts["metric_evidence_links"],
        evidence_references=artifacts["evidence_references"],
    )

    assert len(payload) == 1
    metric_payload = list(payload.values())[0]
    citations = metric_payload["citations"]
    assert len(citations) == 1
    assert citations[0]["page_number"] == 52
    assert citations[0]["artifact_locator"] == "doc:ca:2025:funded#page=52"
