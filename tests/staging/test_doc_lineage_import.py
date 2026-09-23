"""Acceptance coverage for Doc-Lineage tracked-variable staging imports."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from pension_data.staging.doc_lineage_vars import import_tracked_variables


def _synthetic_variable() -> dict[str, object]:
    return {
        "schema_version": "tracked-variable/v1",
        "variable_id": "var:calpers-discount-rate-2025",
        "ontology_key": "consultant.assumptions.discount_rate",
        "ontology_family": "report_section",
        "entity_ref": "pension:calpers",
        "period": "FY2025",
        "status": "asserted",
        "value_text": "6.8%",
        "confidence": 0.94,
        "provenance": {
            "document": {
                "source_id": "document:calpers-2025",
                "locator": {"page": 42},
            },
            "mirror": {
                "mirror_root": "/archive/calpers",
                "blob_path": "blobs/actuarial-2025.pdf",
                "page_anchor": "#page=42",
            },
        },
        "evidence": {
            "schema_version": "evidence-object/v1",
            "evidence_id": "evidence:calpers-discount-rate-2025",
            "fact_ref": "consultant.assumptions.discount_rate",
            "source_id": "document:calpers-2025",
            "entity_ref": "pension:calpers",
            "method": "parser",
            "excerpt": "The discount rate remains 6.8%.",
        },
    }


def _write_payload(tmp_path: Path, payload: object) -> Path:
    path = tmp_path / "tracked-variables.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def test_variable_staged_with_entity_ref(tmp_path: Path) -> None:
    rows = import_tracked_variables(_write_payload(tmp_path, [_synthetic_variable()]))

    assert len(rows) == 1
    assert rows[0].variable_id == "var:calpers-discount-rate-2025"
    assert rows[0].ontology_key == "consultant.assumptions.discount_rate"
    assert rows[0].entity_ref == "pension:calpers"
    assert rows[0].evidence["excerpt"] == "The discount rate remains 6.8%."
    assert rows[0].provenance["document"]["locator"]["page"] == 42


def test_import_rejects_missing_ontology_key(tmp_path: Path) -> None:
    variable = _synthetic_variable()
    variable.pop("ontology_key")

    with pytest.raises(ValueError, match="requires non-empty ontology_key"):
        import_tracked_variables(_write_payload(tmp_path, variable))


def test_import_rejects_duplicate_variable_ids(tmp_path: Path) -> None:
    variable = _synthetic_variable()

    with pytest.raises(ValueError, match="duplicate tracked variable_id"):
        import_tracked_variables(_write_payload(tmp_path, [variable, variable]))
