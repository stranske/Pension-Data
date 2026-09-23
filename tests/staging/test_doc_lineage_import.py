"""Acceptance coverage for Doc-Lineage tracked-variable staging imports."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from pension_data.staging.doc_lineage_vars import import_tracked_variables, stage_tracked_variables

_REPO_ROOT = Path(__file__).resolve().parents[2]


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


def test_import_rejects_invalid_embedded_evidence(tmp_path: Path) -> None:
    variable = _synthetic_variable()
    evidence = variable["evidence"]
    assert isinstance(evidence, dict)
    del evidence["excerpt"]

    with pytest.raises(ValueError, match="fails tracked-variable/v1"):
        import_tracked_variables(_write_payload(tmp_path, variable))


def test_non_finite_confidence_is_rejected_by_both_entry_points(tmp_path: Path) -> None:
    variable = _synthetic_variable()
    variable["confidence"] = float("nan")

    with pytest.raises(ValueError, match="requires finite confidence"):
        stage_tracked_variables(variable)
    with pytest.raises(ValueError, match="non-standard JSON constant NaN"):
        import_tracked_variables(_write_payload(tmp_path, variable))


def test_staged_nested_values_are_independent_snapshots() -> None:
    variable = _synthetic_variable()
    variable["value_structured"] = {"bands": [{"name": "base"}]}
    row = stage_tracked_variables(variable)[0]

    value_structured = variable["value_structured"]
    provenance = variable["provenance"]
    evidence = variable["evidence"]
    assert isinstance(value_structured, dict)
    assert isinstance(provenance, dict)
    assert isinstance(evidence, dict)
    value_structured["bands"] = []
    provenance["document"] = {}
    evidence["excerpt"] = "mutated"

    assert row.value_structured == {"bands": [{"name": "base"}]}
    assert row.provenance["document"]["source_id"] == "document:calpers-2025"
    assert row.evidence["excerpt"] == "The discount rate remains 6.8%."


def test_non_editable_wheel_loads_packaged_schemas(tmp_path: Path) -> None:
    wheel_dir = tmp_path / "wheel"
    site_dir = tmp_path / "site"
    wheel_dir.mkdir()
    subprocess.run(
        [
            sys.executable,
            "-m",
            "pip",
            "wheel",
            "--no-deps",
            "--wheel-dir",
            str(wheel_dir),
            str(_REPO_ROOT),
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    wheel = next(wheel_dir.glob("pension_data-*.whl"))
    subprocess.run(
        [
            sys.executable,
            "-m",
            "pip",
            "install",
            "--no-deps",
            "--target",
            str(site_dir),
            str(wheel),
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    payload_path = _write_payload(tmp_path, _synthetic_variable())
    environment = os.environ.copy()
    environment["PYTHONPATH"] = str(site_dir)
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            (
                "from pension_data.staging import import_tracked_variables; "
                "import sys; "
                "row = import_tracked_variables(sys.argv[1])[0]; "
                "assert row.ontology_key == 'consultant.assumptions.discount_rate'"
            ),
            str(payload_path),
        ],
        cwd=tmp_path,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
