"""Offline acceptance coverage for the single-sample N-CSR ingest lane."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator

from pension_data.harvest.ncsr_sample import ingest_ncsr_sample

FIXTURE = Path(__file__).parent / "fixtures" / "ncsr_sample.json"
MANIFEST_SCHEMA = (
    Path(__file__).parents[2]
    / "docs"
    / "contracts"
    / "schemas"
    / "artifact-manifest-v1.schema.json"
)


def test_ncsr_fixture_ingests() -> None:
    result = ingest_ncsr_sample(FIXTURE, run_id="ncsr-sample-2025-12-02")

    assert result.filing.filing_date == "2025-12-02"
    assert result.filing.report_date == "2025-09-30"
    assert result.filing.accession_number == "0001104659-25-117601"

    schema = json.loads(MANIFEST_SCHEMA.read_text(encoding="utf-8"))
    Draft202012Validator(schema).validate(result.manifest)
    assert result.manifest["schema_version"] == "artifact-manifest/v1"
    artifacts = result.manifest["artifacts"]
    assert isinstance(artifacts, list)
    assert len(artifacts) == 1
    artifact = artifacts[0]
    assert artifact["filing_date"] == "2025-12-02"
    assert artifact["source_url"].startswith("https://www.sec.gov/Archives/")
    assert artifact["path"] == "ncsr/2025-12-02/000110465925117601.json"
    assert artifact["sha256"] == hashlib.sha256(FIXTURE.read_bytes()).hexdigest()


def test_ncsr_fixture_requires_filing_date(tmp_path: Path) -> None:
    payload = json.loads(FIXTURE.read_text(encoding="utf-8"))
    payload.pop("filing_date")
    missing_date = tmp_path / "ncsr-missing-date.json"
    missing_date.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ValueError, match="requires non-empty filing_date"):
        ingest_ncsr_sample(missing_date, run_id="ncsr-missing-date")


def _write_payload(tmp_path: Path, name: str, payload: dict) -> Path:
    path = tmp_path / name
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def test_ncsr_rejects_unicode_digit_accession(tmp_path: Path) -> None:
    payload = json.loads(FIXTURE.read_text(encoding="utf-8"))
    payload["accession_number"] = "٠٠٠1104659-25-117601"
    path = _write_payload(tmp_path, "unicode-accession.json", payload)
    with pytest.raises(ValueError, match="SEC format"):
        ingest_ncsr_sample(path, run_id="unicode-accession")


def test_ncsr_rejects_malformed_accession_separator(tmp_path: Path) -> None:
    payload = json.loads(FIXTURE.read_text(encoding="utf-8"))
    payload["accession_number"] = "0001104659-25-117601-extra"
    path = _write_payload(tmp_path, "bad-accession.json", payload)
    with pytest.raises(ValueError, match="SEC format"):
        ingest_ncsr_sample(path, run_id="bad-accession")


def test_ncsr_rejects_wrong_form(tmp_path: Path) -> None:
    payload = json.loads(FIXTURE.read_text(encoding="utf-8"))
    payload["form"] = "10-K"
    path = _write_payload(tmp_path, "wrong-form.json", payload)
    with pytest.raises(ValueError, match="expected form N-CSR"):
        ingest_ncsr_sample(path, run_id="wrong-form")


def test_ncsr_rejects_malformed_filing_date(tmp_path: Path) -> None:
    payload = json.loads(FIXTURE.read_text(encoding="utf-8"))
    payload["filing_date"] = "12/02/2025"
    path = _write_payload(tmp_path, "bad-date.json", payload)
    with pytest.raises(ValueError, match="ISO date"):
        ingest_ncsr_sample(path, run_id="bad-date")


def test_ncsr_rejects_sec_url_for_different_filing(tmp_path: Path) -> None:
    payload = json.loads(FIXTURE.read_text(encoding="utf-8"))
    payload["source_url"] = (
        "https://www.sec.gov/Archives/edgar/data/872323/000110465925117602/tm2528754d1_ncsr.htm"
    )
    path = _write_payload(tmp_path, "wrong-url-accession.json", payload)
    with pytest.raises(ValueError, match="same CIK, accession"):
        ingest_ncsr_sample(path, run_id="wrong-url")


def test_ncsr_rejects_non_sec_url(tmp_path: Path) -> None:
    payload = json.loads(FIXTURE.read_text(encoding="utf-8"))
    payload["source_url"] = "https://example.com/filing.json"
    path = _write_payload(tmp_path, "bad-host.json", payload)
    with pytest.raises(ValueError, match="official sec.gov"):
        ingest_ncsr_sample(path, run_id="bad-host")
