"""Citation-ready provenance payload exports."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Sequence
from urllib.parse import quote

from pension_data.db.models.provenance import EvidenceReference, MetricEvidenceLink


def _artifact_locator(evidence: EvidenceReference) -> str:
    if evidence.page_number is not None:
        return f"{evidence.source_document_id}#page={evidence.page_number}"
    if evidence.snippet_anchor is not None:
        return f"{evidence.source_document_id}#anchor={quote(evidence.snippet_anchor, safe='')}"
    if evidence.section_hint is not None:
        return f"{evidence.source_document_id}#section={quote(evidence.section_hint, safe='')}"
    return evidence.source_document_id


def export_citation_ready_provenance_payload(
    *,
    metric_evidence_links: Sequence[MetricEvidenceLink],
    evidence_references: Sequence[EvidenceReference],
) -> dict[str, dict[str, object]]:
    """Export deterministic citation payload grouped by metric row id."""
    evidence_by_id = {
        evidence.evidence_ref_id: evidence
        for evidence in sorted(evidence_references, key=lambda row: row.evidence_ref_id)
    }
    grouped_links: dict[str, list[MetricEvidenceLink]] = defaultdict(list)
    for link in metric_evidence_links:
        grouped_links[link.metric_row_id].append(link)

    payload: dict[str, dict[str, object]] = {}
    for metric_row_id in sorted(grouped_links):
        links = sorted(grouped_links[metric_row_id], key=lambda row: row.link_id)
        first_link = links[0]
        citations: list[dict[str, object]] = []
        for link in links:
            evidence = evidence_by_id.get(link.evidence_ref_id)
            if evidence is None:
                continue
            citations.append(
                {
                    "evidence_ref_id": evidence.evidence_ref_id,
                    "report_id": evidence.report_id,
                    "source_document_id": evidence.source_document_id,
                    "raw_ref": evidence.raw_ref,
                    "page_number": evidence.page_number,
                    "section_hint": evidence.section_hint,
                    "snippet_anchor": evidence.snippet_anchor,
                    "artifact_locator": _artifact_locator(evidence),
                }
            )
        payload[metric_row_id] = {
            "metric_row_id": metric_row_id,
            "metric_family": first_link.metric_family,
            "metric_name": first_link.metric_name,
            "citation_count": len(citations),
            "citations": citations,
        }
    return payload
