"""Build stable metric-to-evidence linkage artifacts for core facts."""

from __future__ import annotations

from dataclasses import dataclass

from pension_data.db.models.core_facts import (
    ActuarialFact,
    AllocationFact,
    BitemporalFactContext,
    FeeFact,
    FundedStatusFact,
    HoldingFact,
)
from pension_data.db.models.provenance import EvidenceReference, MetricEvidenceLink
from pension_data.extract.common.evidence import build_evidence_reference, canonicalize_evidence_ref
from pension_data.extract.common.ids import stable_id


class EvidenceValidationError(ValueError):
    """Raised when required evidence is missing for high-impact metrics."""


@dataclass(frozen=True, slots=True)
class _MetricEvidenceSource:
    metric_row_id: str
    metric_family: str
    metric_name: str
    report_id: str
    source_document_id: str
    evidence_refs: tuple[str, ...]


_HIGH_IMPACT_FAMILIES: frozenset[str] = frozenset(
    {"funded", "actuarial", "allocation", "holding", "fee"}
)


def _metric_row_id(
    *,
    metric_family: str,
    metric_name: str,
    context: BitemporalFactContext,
    manager_name: str | None = None,
    fund_name: str | None = None,
    vehicle_name: str | None = None,
) -> str:
    return stable_id(
        "metric",
        metric_family,
        metric_name,
        context.plan_id,
        context.plan_period,
        manager_name,
        fund_name,
        vehicle_name,
        context.effective_date,
        context.ingestion_date,
        context.benchmark_version,
        context.source_document_id,
    )


def _canonical_refs(evidence_refs: tuple[str, ...]) -> tuple[str, ...]:
    canonical: list[str] = []
    for evidence_ref in evidence_refs:
        normalized = canonicalize_evidence_ref(evidence_ref)
        if not normalized or normalized in canonical:
            continue
        canonical.append(normalized)
    return tuple(canonical)


def _gather_metric_sources(
    *,
    funded_facts: tuple[FundedStatusFact, ...],
    actuarial_facts: tuple[ActuarialFact, ...],
    allocation_facts: tuple[AllocationFact, ...],
    holding_facts: tuple[HoldingFact, ...],
    fee_facts: tuple[FeeFact, ...],
) -> list[_MetricEvidenceSource]:
    sources: list[_MetricEvidenceSource] = []
    for funded_fact in funded_facts:
        sources.append(
            _MetricEvidenceSource(
                metric_row_id=_metric_row_id(
                    metric_family="funded",
                    metric_name=funded_fact.metric_name,
                    context=funded_fact.context,
                ),
                metric_family="funded",
                metric_name=funded_fact.metric_name,
                report_id=funded_fact.context.source_document_id,
                source_document_id=funded_fact.context.source_document_id,
                evidence_refs=funded_fact.evidence_refs,
            )
        )
    for actuarial_fact in actuarial_facts:
        sources.append(
            _MetricEvidenceSource(
                metric_row_id=_metric_row_id(
                    metric_family="actuarial",
                    metric_name=actuarial_fact.metric_name,
                    context=actuarial_fact.context,
                ),
                metric_family="actuarial",
                metric_name=actuarial_fact.metric_name,
                report_id=actuarial_fact.context.source_document_id,
                source_document_id=actuarial_fact.context.source_document_id,
                evidence_refs=actuarial_fact.evidence_refs,
            )
        )
    for allocation_fact in allocation_facts:
        sources.append(
            _MetricEvidenceSource(
                metric_row_id=_metric_row_id(
                    metric_family="allocation",
                    metric_name=allocation_fact.metric_name,
                    context=allocation_fact.context,
                ),
                metric_family="allocation",
                metric_name=allocation_fact.metric_name,
                report_id=allocation_fact.context.source_document_id,
                source_document_id=allocation_fact.context.source_document_id,
                evidence_refs=allocation_fact.evidence_refs,
            )
        )
    for holding_fact in holding_facts:
        sources.append(
            _MetricEvidenceSource(
                metric_row_id=_metric_row_id(
                    metric_family="holding",
                    metric_name=holding_fact.metric_name,
                    context=holding_fact.context,
                    manager_name=holding_fact.manager_name,
                    fund_name=holding_fact.fund_name,
                    vehicle_name=holding_fact.vehicle_name,
                ),
                metric_family="holding",
                metric_name=holding_fact.metric_name,
                report_id=holding_fact.context.source_document_id,
                source_document_id=holding_fact.context.source_document_id,
                evidence_refs=holding_fact.evidence_refs,
            )
        )
    for fee_fact in fee_facts:
        sources.append(
            _MetricEvidenceSource(
                metric_row_id=_metric_row_id(
                    metric_family="fee",
                    metric_name=fee_fact.fee_category,
                    context=fee_fact.context,
                    manager_name=fee_fact.manager_name,
                ),
                metric_family="fee",
                metric_name=fee_fact.fee_category,
                report_id=fee_fact.context.source_document_id,
                source_document_id=fee_fact.context.source_document_id,
                evidence_refs=fee_fact.evidence_refs,
            )
        )
    return sorted(
        sources,
        key=lambda source: (
            source.metric_row_id,
            source.metric_family,
            source.metric_name,
            source.source_document_id,
        ),
    )


def build_core_metric_evidence_artifacts(
    *,
    funded_facts: tuple[FundedStatusFact, ...] = (),
    actuarial_facts: tuple[ActuarialFact, ...] = (),
    allocation_facts: tuple[AllocationFact, ...] = (),
    holding_facts: tuple[HoldingFact, ...] = (),
    fee_facts: tuple[FeeFact, ...] = (),
    strict: bool = True,
) -> dict[str, object]:
    """Build structured evidence refs + stable metric links for core metric facts."""
    evidence_by_id: dict[str, EvidenceReference] = {}
    links_by_id: dict[str, MetricEvidenceLink] = {}
    validation_warnings: list[str] = []

    for source in _gather_metric_sources(
        funded_facts=funded_facts,
        actuarial_facts=actuarial_facts,
        allocation_facts=allocation_facts,
        holding_facts=holding_facts,
        fee_facts=fee_facts,
    ):
        canonical_refs = _canonical_refs(source.evidence_refs)
        if source.metric_family in _HIGH_IMPACT_FAMILIES and not canonical_refs:
            message = (
                "missing evidence refs for high-impact metric row "
                f"{source.metric_family}/{source.metric_name} ({source.metric_row_id})"
            )
            if strict:
                raise EvidenceValidationError(message)
            validation_warnings.append(message)
            continue

        for evidence_ref in canonical_refs:
            evidence = build_evidence_reference(
                report_id=source.report_id,
                source_document_id=source.source_document_id,
                evidence_ref=evidence_ref,
            )
            evidence_by_id[evidence.evidence_ref_id] = evidence
            link = MetricEvidenceLink(
                link_id=stable_id(
                    "metric-evidence-link", source.metric_row_id, evidence.evidence_ref_id
                ),
                metric_row_id=source.metric_row_id,
                metric_family=source.metric_family,
                metric_name=source.metric_name,
                evidence_ref_id=evidence.evidence_ref_id,
            )
            links_by_id[link.link_id] = link

    return {
        "evidence_references": tuple(
            sorted(evidence_by_id.values(), key=lambda evidence: evidence.evidence_ref_id)
        ),
        "metric_evidence_links": tuple(sorted(links_by_id.values(), key=lambda link: link.link_id)),
        "validation_warnings": tuple(sorted(validation_warnings)),
    }
