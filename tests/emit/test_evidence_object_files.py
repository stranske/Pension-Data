"""Evidence-object/v1 backplane emission tests."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator

from pension_data.emit.evidence_object import build_evidence_objects
from pension_data.ops.backplane_emitter import build_backplane_reference_run
from pension_data.ops.one_pdf_pilot import OnePdfPilotInput, run_one_pdf_pilot

_ROOT = Path(__file__).resolve().parents[2]
_FIXTURE = _ROOT / "tests" / "parser" / "fixtures" / "calpers_fy2024_excerpt.pdf"
_SCHEMA = json.loads(
    (_ROOT / "docs" / "contracts" / "schemas" / "evidence-object-v1.schema.json").read_text(
        encoding="utf-8"
    )
)


def _emit(tmp_path: Path) -> dict[str, Path]:
    result = run_one_pdf_pilot(
        pilot_input=OnePdfPilotInput(
            pdf_path=_FIXTURE,
            plan_id="CA-PERS",
            plan_period="FY2024",
            effective_date="2024-06-30",
            ingestion_date="2026-01-01",
        ),
        output_root=tmp_path,
        run_id="evidence-object-test",
    )
    return build_backplane_reference_run(
        pilot_manifest_path=Path(result["run_manifest_json"]),
        output_dir=tmp_path,
        wall_ms=10,
        recorded_at="2026-01-01T00:00:00+00:00",
        repo_root=_ROOT,
    )


def test_emitted_file_validates(tmp_path: Path) -> None:
    """The named acceptance test resolves and validates every emitted evidence file."""
    paths = _emit(tmp_path)
    run_payload = json.loads(paths["run_json"].read_text(encoding="utf-8"))
    manifest = json.loads(paths["manifest"].read_text(encoding="utf-8"))
    by_id = {entry["artifact_id"]: entry for entry in manifest["artifacts"]}

    assert len(run_payload["evidence_refs"]) == 7
    assert len(set(run_payload["evidence_refs"])) == 7
    for evidence_id in run_payload["evidence_refs"]:
        entry = by_id[evidence_id]
        assert entry["kind"] == "evidence"
        evidence_path = tmp_path / entry["path"]
        payload = json.loads(evidence_path.read_text(encoding="utf-8"))
        Draft202012Validator(_SCHEMA).validate(payload)
        assert payload["excerpt"].strip()
        assert payload["entity_ref"] == "pension:ca_pers"
        assert entry["sha256"] == hashlib.sha256(evidence_path.read_bytes()).hexdigest()
        assert entry["bytes"] == evidence_path.stat().st_size


def test_shared_locator_keeps_distinct_fact_evidence() -> None:
    rows = [
        {
            "fact_id": "fact:funded",
            "metric_name": "funded_ratio",
            "source_document_id": "document:one",
            "extraction_method": "table_lookup",
            "evidence_refs": ["p.1#table"],
            "confidence": 0.9,
        },
        {
            "fact_id": "fact:aal",
            "metric_name": "aal_usd",
            "source_document_id": "document:one",
            "extraction_method": "table_lookup",
            "evidence_refs": ["p.1#table"],
            "confidence": 0.9,
        },
    ]
    source_evidence = {
        "table_rows": [
            {"label": "Funded Ratio", "value": "76.8%", "evidence_ref": "p.1#table"},
            {"label": "AAL", "value": "$607.5 million", "evidence_ref": "p.1#table"},
        ],
        "text_blocks": [],
    }
    emitted = build_evidence_objects(
        run_id="run",
        core_rows=rows,
        source_evidence=source_evidence,
        pension_entity_ref="pension:ca_pers",
    )
    assert {item["excerpt"] for item in emitted} == {"76.8%", "$607.5 million"}
    assert len({item["evidence_id"] for item in emitted}) == 2


def test_missing_grounded_excerpt_is_rejected() -> None:
    row = {
        "fact_id": "fact:funded",
        "metric_name": "funded_ratio",
        "source_document_id": "document:one",
        "extraction_method": "table_lookup",
        "evidence_refs": ["p.1#table"],
    }
    with pytest.raises(ValueError, match="no grounded excerpt"):
        build_evidence_objects(
            run_id="run",
            core_rows=[row],
            source_evidence={
                "table_rows": [{"label": "Funded Ratio", "value": "", "evidence_ref": "p.1#table"}]
            },
            pension_entity_ref="pension:ca_pers",
        )
