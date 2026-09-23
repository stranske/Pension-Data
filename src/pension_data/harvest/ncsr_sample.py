"""Offline ingest for one recorded-shape SEC N-CSR filing sample."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date
from pathlib import Path, PurePosixPath
from urllib.parse import urlparse

FORM_NCSR = "N-CSR"


@dataclass(frozen=True, slots=True)
class NCSRFiling:
    """Validated metadata for one SEC N-CSR filing."""

    accession_number: str
    cik: str
    registrant_name: str
    filing_date: str
    report_date: str
    primary_document: str
    source_url: str


@dataclass(frozen=True, slots=True)
class NCSRSampleIngest:
    """Parsed filing metadata and its artifact-manifest registration."""

    filing: NCSRFiling
    manifest: dict[str, object]


def ingest_ncsr_sample(filing_path: str | Path, *, run_id: str) -> NCSRSampleIngest:
    """Read one offline N-CSR sample and register it in ``artifact-manifest/v1``."""
    normalized_run_id = run_id.strip()
    if not normalized_run_id:
        raise ValueError("run_id must be non-empty")

    path = Path(filing_path)
    content = path.read_bytes()
    if not content:
        raise ValueError("N-CSR sample is empty")
    try:
        raw_payload: object = json.loads(content)
    except json.JSONDecodeError as exc:
        raise ValueError(f"N-CSR sample is not valid JSON: {exc}") from exc
    if not isinstance(raw_payload, dict):
        raise ValueError("N-CSR sample must be a JSON object")

    payload: Mapping[str, object] = raw_payload
    form = _required_text(payload, "form")
    if form != FORM_NCSR:
        raise ValueError(f"expected form {FORM_NCSR}, got {form}")

    filing = NCSRFiling(
        accession_number=_required_text(payload, "accession_number"),
        cik=_required_text(payload, "cik"),
        registrant_name=_required_text(payload, "registrant_name"),
        filing_date=_required_iso_date(payload, "filing_date"),
        report_date=_required_iso_date(payload, "report_date"),
        primary_document=_required_text(payload, "primary_document"),
        source_url=_required_sec_url(payload, "source_url"),
    )

    accession_slug = filing.accession_number.replace("-", "")
    if not accession_slug.isdigit():
        raise ValueError("accession_number must contain only digits and dashes")
    artifact_path = str(PurePosixPath("ncsr", filing.filing_date, f"{accession_slug}.json"))
    artifact = {
        "artifact_id": f"public-doc:sec-ncsr:{accession_slug}",
        "name": f"{filing.registrant_name} N-CSR filed {filing.filing_date}",
        "kind": "data",
        "path": artifact_path,
        "sha256": hashlib.sha256(content).hexdigest(),
        "bytes": len(content),
        "media_type": "application/json",
        "source_url": filing.source_url,
        "accession_number": filing.accession_number,
        "form": FORM_NCSR,
        "filing_date": filing.filing_date,
        "report_date": filing.report_date,
        "registrant_name": filing.registrant_name,
        "cik": filing.cik,
        "primary_document": filing.primary_document,
    }
    manifest: dict[str, object] = {
        "schema_version": "artifact-manifest/v1",
        "run_id": normalized_run_id,
        "tool": "pension-data-ncsr-sample-ingest",
        "artifacts": [artifact],
    }
    return NCSRSampleIngest(filing=filing, manifest=manifest)


def _required_text(payload: Mapping[str, object], key: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"N-CSR sample requires non-empty {key}")
    return value.strip()


def _required_iso_date(payload: Mapping[str, object], key: str) -> str:
    value = _required_text(payload, key)
    try:
        parsed = date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"N-CSR sample {key} must be an ISO date") from exc
    return parsed.isoformat()


def _required_sec_url(payload: Mapping[str, object], key: str) -> str:
    value = _required_text(payload, key)
    parsed = urlparse(value)
    hostname = (parsed.hostname or "").rstrip(".").casefold()
    if parsed.scheme.casefold() != "https" or not (
        hostname == "sec.gov" or hostname.endswith(".sec.gov")
    ):
        raise ValueError("N-CSR sample source_url must use an official sec.gov HTTPS host")
    return value
