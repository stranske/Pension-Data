"""Offline acceptance coverage for the CalPERS IC document harvester."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator

from pension_data.harvest.calpers_ic import build_public_doc_manifest, parse_calpers_ic_page

FIXTURE = Path(__file__).parent / "fixtures" / "calpers_investment_committee.html"
MANIFEST_SCHEMA = (
    Path(__file__).parents[2]
    / "docs"
    / "contracts"
    / "schemas"
    / "artifact-manifest-v1.schema.json"
)
PAGE_URL = "https://www.calpers.ca.gov/about/board/board-meetings/invest-202603-0"


def test_ic_items_parsed() -> None:
    documents = parse_calpers_ic_page(FIXTURE.read_text(encoding="utf-8"), page_url=PAGE_URL)

    assert documents
    assert len(documents) == 4
    assert {document.meeting_date for document in documents} == {"March 17, 2026"}
    assert {document.doc_type_id for document in documents} == {
        "pension.ic.agenda",
        "pension.ic.agenda_item",
        "pension.ic.attachment",
    }
    attachments = [
        document for document in documents if document.doc_type_id == "pension.ic.attachment"
    ]
    assert len(attachments) == 2
    assert {document.parent_title for document in attachments} == {
        "CalPERS Trust Level Review Consultant Report (PDF)"
    }
    assert all(
        document.source_url.startswith("https://www.calpers.ca.gov/documents/")
        for document in documents
    )
    assert all("Building Guide" not in document.title for document in documents)


def test_ic_items_register_as_artifact_manifest() -> None:
    documents = parse_calpers_ic_page(FIXTURE.read_text(encoding="utf-8"), page_url=PAGE_URL)
    content_by_url = {
        document.source_url: f"%PDF-1.7 fixture for {document.title}".encode()
        for document in documents
    }

    manifest = build_public_doc_manifest(
        documents,
        content_by_url=content_by_url,
        run_id="calpers-ic-2026-03-17",
    )

    schema = json.loads(MANIFEST_SCHEMA.read_text(encoding="utf-8"))
    Draft202012Validator(schema).validate(manifest)
    assert manifest["schema_version"] == "artifact-manifest/v1"
    artifacts = manifest["artifacts"]
    assert isinstance(artifacts, list)
    assert len(artifacts) == len(documents)
    for artifact in artifacts:
        source_url = artifact["source_url"]
        assert artifact["sha256"] == hashlib.sha256(content_by_url[source_url]).hexdigest()
        assert artifact["path"].startswith("calpers-ic/march-17-2026/")
        assert artifact["path"].endswith(".pdf")


def test_ic_manifest_rejects_missing_download() -> None:
    document = parse_calpers_ic_page(FIXTURE.read_text(encoding="utf-8"), page_url=PAGE_URL)[0]

    with pytest.raises(ValueError, match="missing downloaded content"):
        build_public_doc_manifest([document], content_by_url={}, run_id="test-run")


@pytest.mark.parametrize(
    ("content", "message"),
    [
        (b"", "downloaded content is empty"),
        (b"<html>temporary upstream error</html>", "downloaded content is not a PDF"),
    ],
)
def test_ic_manifest_rejects_invalid_download(content: bytes, message: str) -> None:
    document = parse_calpers_ic_page(FIXTURE.read_text(encoding="utf-8"), page_url=PAGE_URL)[0]

    with pytest.raises(ValueError, match=message):
        build_public_doc_manifest(
            [document], content_by_url={document.source_url: content}, run_id="test-run"
        )


def test_ic_manifest_rejects_blank_run_id() -> None:
    document = parse_calpers_ic_page(FIXTURE.read_text(encoding="utf-8"), page_url=PAGE_URL)[0]

    with pytest.raises(ValueError, match="run_id must be non-empty"):
        build_public_doc_manifest(
            [document], content_by_url={document.source_url: b"%PDF-1.7"}, run_id="  "
        )
