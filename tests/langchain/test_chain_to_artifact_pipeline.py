"""End-to-end smoke test: chain step -> export step -> eval-harness schema check.

This exercises the layering documented in
`docs/contracts/reviewable-findings-artifact-contract.md` (Chain Output vs Published
Artifact): the chain step intentionally leaves `metadata.artifact_path=None`, the export
step is the persistence bridge that attaches a non-empty `artifact_path`, and the
eval-harness schema check enforces non-empty `artifact_path` against the published payload.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from pension_data.langchain.eval_harness import evaluate_dataset, load_eval_dataset
from pension_data.langchain.findings_common import FindingSlice
from pension_data.langchain.findings_explain import ExplainRequest, run_findings_explain_chain
from pension_data.langchain.findings_export import build_findings_export_artifact


class _StaticChain:
    def __init__(self, output: Mapping[str, Any]) -> None:
        self.output = output

    def invoke(self, values: Mapping[str, Any]) -> Mapping[str, Any]:
        del values
        return self.output


def _published_payload_from_chain(
    *,
    chain_output: Mapping[str, Any],
    artifact_path: str,
) -> dict[str, Any]:
    """Drive chain -> export -> serializable published payload.

    Mirrors the production pipeline: the chain returns transient metadata, the export
    step attaches the persistence path, and the published payload is what the
    eval-harness schema check sees.
    """
    finding_slice = FindingSlice(
        slice_id="slice:pipeline-smoke",
        plan_id="CA-PERS",
        plan_period="FY2024",
        metrics={"funded_ratio": 0.81},
        citations=("doc:funded#p.52",),
    )
    response = run_findings_explain_chain(
        request=ExplainRequest(
            question="Why did the funded ratio improve in FY2024?",
            finding_slice=finding_slice,
        ),
        chain=_StaticChain(chain_output),
        request_id="fx:pipeline-smoke",
    )

    # Layer-1 invariant: chain metadata never carries an artifact_path.
    assert response.status == "ok"
    assert response.result is not None
    assert response.metadata.artifact_path is None

    export = build_findings_export_artifact(
        artifact_type="explain",
        request_id=response.metadata.request_id,
        payload={
            "summary": response.result.summary,
            "key_drivers": list(response.result.key_drivers),
            "caveats": list(response.result.caveats),
        },
        citations=response.result.citations,
        artifact_path=artifact_path,
    )

    # Layer-2 invariant: export step attaches a non-empty artifact_path.
    assert export.artifact_path == artifact_path

    return {
        "request_id": export.request_id,
        "generated_at": export.generated_at,
        "artifact_path": export.artifact_path,
        "summary": export.payload["summary"],
        "key_drivers": list(export.payload["key_drivers"]),
        "caveats": list(export.payload["caveats"]),
        "citations": list(export.citations),
    }


def _write_dataset_for_recorded_output(
    tmp_path: Path,
    *,
    recorded_filename: str,
    payload: Mapping[str, Any],
    expected_citations: tuple[str, ...],
) -> Path:
    recorded_path = tmp_path / recorded_filename
    recorded_path.write_text(json.dumps(payload), encoding="utf-8")

    dataset = {
        "version": 1,
        "thresholds": {
            "min_schema_validity_rate": 1.0,
            "min_citation_coverage_rate": 1.0,
            "min_no_hallucination_rate": 1.0,
            "min_safety_pass_rate": 1.0,
        },
        "cases": [
            {
                "id": "pipeline-smoke-explain",
                "domain": "funded_ratio",
                "feature": "findings_explain",
                "question": "Why did the funded ratio improve in FY2024?",
                "recorded_output": recorded_filename,
                "expected_citations": list(expected_citations),
            }
        ],
    }
    dataset_path = tmp_path / "pipeline-smoke-dataset.json"
    dataset_path.write_text(json.dumps(dataset), encoding="utf-8")
    return dataset_path


def test_chain_export_eval_pipeline_passes_when_export_step_attaches_artifact_path(
    tmp_path: Path,
) -> None:
    """Full chain -> export -> eval-harness path produces zero artifact_path defects."""
    published = _published_payload_from_chain(
        chain_output={
            "summary": "Funded ratio improved as assets outpaced liabilities.",
            "key_drivers": ["Actuarial assets rose faster than liabilities"],
            "caveats": ["Single period; review with contribution notes"],
            "citations": ["doc:funded#p.52"],
        },
        artifact_path="artifacts/langchain/findings-pipeline-smoke.json",
    )
    dataset_path = _write_dataset_for_recorded_output(
        tmp_path,
        recorded_filename="findings_pipeline_smoke_explain.json",
        payload=published,
        expected_citations=("doc:funded#p.52",),
    )

    dataset = load_eval_dataset(dataset_path)
    report = evaluate_dataset(dataset, mode="mock")

    assert report.status == "pass", report.failures
    assert len(report.case_results) == 1
    case = report.case_results[0]
    assert case.schema_valid, case.details
    assert case.pass_status, case.details
    # No detail should ever mention an artifact_path defect on the published payload.
    assert not any("artifact_path" in detail for detail in case.details), case.details


def test_eval_harness_flags_artifact_path_when_export_step_is_skipped(
    tmp_path: Path,
) -> None:
    """Skipping the export persistence step surfaces the documented schema defect.

    This is the negative side of the chain-vs-published contract: emitting raw chain
    metadata (no `artifact_path`) without first going through `build_findings_export_artifact`
    must trip the eval-harness `'artifact_path'` schema check.
    """
    finding_slice = FindingSlice(
        slice_id="slice:pipeline-smoke",
        plan_id="CA-PERS",
        plan_period="FY2024",
        metrics={"funded_ratio": 0.81},
        citations=("doc:funded#p.52",),
    )
    response = run_findings_explain_chain(
        request=ExplainRequest(
            question="Why did the funded ratio improve in FY2024?",
            finding_slice=finding_slice,
        ),
        chain=_StaticChain(
            {
                "summary": "Funded ratio improved as assets outpaced liabilities.",
                "key_drivers": ["Actuarial assets rose faster than liabilities"],
                "caveats": ["Single period; review with contribution notes"],
                "citations": ["doc:funded#p.52"],
            }
        ),
        request_id="fx:pipeline-smoke-negative",
    )
    assert response.result is not None
    assert response.metadata.artifact_path is None

    # Publish chain metadata directly, bypassing the export step. This is the failure
    # mode the contract subsection warns about.
    raw_chain_payload = {
        "request_id": response.metadata.request_id,
        "generated_at": response.metadata.generated_at,
        "summary": response.result.summary,
        "key_drivers": list(response.result.key_drivers),
        "caveats": list(response.result.caveats),
        "citations": list(response.result.citations),
        "artifact_path": response.metadata.artifact_path,
    }
    dataset_path = _write_dataset_for_recorded_output(
        tmp_path,
        recorded_filename="findings_pipeline_smoke_explain_no_export.json",
        payload=raw_chain_payload,
        expected_citations=("doc:funded#p.52",),
    )

    dataset = load_eval_dataset(dataset_path)
    report = evaluate_dataset(dataset, mode="mock")

    assert report.status == "fail"
    assert len(report.case_results) == 1
    case = report.case_results[0]
    assert not case.schema_valid
    assert any(
        "findings_explain output requires non-empty string field 'artifact_path'" in detail
        for detail in case.details
    ), case.details


def test_contract_doc_documents_chain_output_vs_published_artifact() -> None:
    """The contract doc must carry the layering subsection this test backs up."""
    repo_root = Path(__file__).resolve().parents[2]
    contract = (repo_root / "docs/contracts/reviewable-findings-artifact-contract.md").read_text(
        encoding="utf-8"
    )

    assert "Chain Output vs Published Artifact" in contract
    assert "build_findings_export_artifact" in contract
    assert "tests/langchain/test_chain_to_artifact_pipeline.py" in contract

    audit_report_path = "docs/reports/reviewable-findings-contract-audit.md"
    occurrences = contract.count(audit_report_path)
    assert occurrences == 1, (
        f"audit report path must be cross-referenced exactly once, found {occurrences}"
    )

    section_start = contract.index("### Chain Output vs Published Artifact")
    next_section_marker = "\n## "
    section_end_idx = contract.find(next_section_marker, section_start)
    if section_end_idx == -1:
        section_end_idx = len(contract)
    layering_section = contract[section_start:section_end_idx]
    assert audit_report_path in layering_section, (
        "audit report cross-reference must live inside the layering subsection"
    )
