"""Offline parser for CalPERS Investment Committee per-item PDF pages."""

from __future__ import annotations

import hashlib
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from html.parser import HTMLParser
from pathlib import PurePosixPath
from urllib.parse import ParseResult, urljoin, urlparse

_DATE_PATTERN = re.compile(
    r"\b(January|February|March|April|May|June|July|August|September|October|November|"
    r"December)\s+\d{1,2},\s+\d{4}\b"
)
_WHITESPACE_PATTERN = re.compile(r"\s+")
_SLUG_PATTERN = re.compile(r"[^a-z0-9]+")


@dataclass(frozen=True, slots=True)
class CalpersICDocument:
    """One official PDF linked from a CalPERS Investment Committee agenda page."""

    meeting_date: str
    agenda_section: str
    title: str
    source_url: str
    doc_type_id: str
    parent_title: str | None = None


@dataclass(frozen=True, slots=True)
class _ParsedLink:
    title: str
    href: str
    section: str
    parent_title: str | None


class _CalpersAgendaParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.meeting_date = ""
        self.current_section = ""
        self.links: list[_ParsedLink] = []
        self._heading_tag: str | None = None
        self._heading_parts: list[str] = []
        self._anchor_href: str | None = None
        self._anchor_parts: list[str] = []
        self._list_item_stack: list[list[str]] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        normalized = tag.lower()
        if normalized in {"h1", "h2", "h3", "h4"}:
            self._heading_tag = normalized
            self._heading_parts = []
        elif normalized == "li":
            self._list_item_stack.append([])
        elif normalized == "a":
            self._anchor_href = dict(attrs).get("href")
            self._anchor_parts = []

    def handle_data(self, data: str) -> None:
        if self._heading_tag is not None:
            self._heading_parts.append(data)
        if self._anchor_href is not None:
            self._anchor_parts.append(data)

    def handle_endtag(self, tag: str) -> None:
        normalized = tag.lower()
        if normalized == self._heading_tag:
            heading = _normalize_text("".join(self._heading_parts))
            date_match = _DATE_PATTERN.search(heading)
            if date_match is not None:
                self.meeting_date = date_match.group(0)
            if heading:
                self.current_section = heading
            self._heading_tag = None
            self._heading_parts = []
        elif normalized == "a" and self._anchor_href is not None:
            title = _normalize_text("".join(self._anchor_parts))
            parent_title = _nearest_parent_title(self._list_item_stack)
            if title and self._anchor_href:
                self.links.append(
                    _ParsedLink(
                        title=title,
                        href=self._anchor_href,
                        section=self.current_section,
                        parent_title=parent_title,
                    )
                )
                if self._list_item_stack:
                    self._list_item_stack[-1].append(title)
            self._anchor_href = None
            self._anchor_parts = []
        elif normalized == "li" and self._list_item_stack:
            self._list_item_stack.pop()


def parse_calpers_ic_page(html: str, *, page_url: str) -> tuple[CalpersICDocument, ...]:
    """Parse official per-item PDFs from one saved CalPERS IC meeting page."""
    parsed_page_url = urlparse(page_url)
    if not _is_calpers_host(parsed_page_url.hostname):
        raise ValueError("page_url must use an official calpers.ca.gov host")

    parser = _CalpersAgendaParser()
    parser.feed(html)
    parser.close()
    if not parser.meeting_date:
        raise ValueError("CalPERS IC page does not expose a meeting date")

    documents_by_url: dict[str, CalpersICDocument] = {}
    for link in parser.links:
        if not _is_agenda_section(link.section):
            continue
        source_url = urljoin(page_url, link.href)
        parsed_source_url = urlparse(source_url)
        if not _is_official_pdf_link(parsed_source_url):
            continue
        documents_by_url.setdefault(
            source_url,
            CalpersICDocument(
                meeting_date=parser.meeting_date,
                agenda_section=link.section,
                title=link.title,
                source_url=source_url,
                doc_type_id=_doc_type_id(link.title),
                parent_title=link.parent_title,
            ),
        )

    return tuple(documents_by_url[url] for url in sorted(documents_by_url))


def build_public_doc_manifest(
    documents: Sequence[CalpersICDocument],
    *,
    content_by_url: Mapping[str, bytes],
    run_id: str,
) -> dict[str, object]:
    """Register harvested PDFs in the fleet artifact-manifest/v1 shape."""
    if not run_id.strip():
        raise ValueError("run_id must be non-empty")
    if not documents:
        raise ValueError("at least one harvested CalPERS document is required")

    artifacts: list[dict[str, object]] = []
    for document in sorted(documents, key=lambda item: item.source_url):
        try:
            content = content_by_url[document.source_url]
        except KeyError as exc:
            raise ValueError(f"missing downloaded content for {document.source_url}") from exc
        if not content:
            raise ValueError(f"downloaded content is empty for {document.source_url}")
        if not content.lstrip().startswith(b"%PDF-"):
            raise ValueError(f"downloaded content is not a PDF for {document.source_url}")

        content_sha256 = hashlib.sha256(content).hexdigest()
        url_digest = hashlib.sha256(document.source_url.encode("utf-8")).hexdigest()[:24]
        artifacts.append(
            {
                "artifact_id": f"public-doc:calpers-ic:{url_digest}",
                "name": document.title,
                "kind": "data",
                "path": _manifest_path(document=document, url_digest=url_digest),
                "sha256": content_sha256,
                "bytes": len(content),
                "media_type": "application/pdf",
                "source_url": document.source_url,
                "doc_type_id": document.doc_type_id,
                "meeting_date": document.meeting_date,
                "agenda_section": document.agenda_section,
                "parent_title": document.parent_title,
            }
        )

    return {
        "schema_version": "artifact-manifest/v1",
        "run_id": run_id.strip(),
        "tool": "pension-data-calpers-ic-harvester",
        "artifacts": artifacts,
    }


def _normalize_text(value: str) -> str:
    return _WHITESPACE_PATTERN.sub(" ", value).strip()


def _nearest_parent_title(stack: Sequence[Sequence[str]]) -> str | None:
    for labels in reversed(stack):
        for label in reversed(labels):
            if "attachment" not in label.casefold():
                return label
    return None


def _is_calpers_host(hostname: str | None) -> bool:
    normalized = (hostname or "").rstrip(".").casefold()
    return normalized == "calpers.ca.gov" or normalized.endswith(".calpers.ca.gov")


def _is_agenda_section(section: str) -> bool:
    normalized = section.casefold()
    return "open session" in normalized or "closed session" in normalized


def _is_official_pdf_link(parsed_url: ParseResult) -> bool:
    scheme = parsed_url.scheme.casefold()
    hostname = parsed_url.hostname
    path = parsed_url.path
    return (
        scheme == "https"
        and _is_calpers_host(hostname)
        and path.startswith("/documents/")
        and path.rstrip("/").endswith("/download")
    )


def _doc_type_id(title: str) -> str:
    normalized = title.casefold()
    if "transcript" in normalized:
        return "pension.ic.transcript"
    if "timed agenda" in normalized:
        return "pension.ic.agenda"
    if "attachment" in normalized:
        return "pension.ic.attachment"
    return "pension.ic.agenda_item"


def _manifest_path(*, document: CalpersICDocument, url_digest: str) -> str:
    slug = _SLUG_PATTERN.sub("-", document.title.casefold()).strip("-") or "document"
    date_slug = _SLUG_PATTERN.sub("-", document.meeting_date.casefold()).strip("-")
    return str(PurePosixPath("calpers-ic", date_slug, f"{slug}-{url_digest}.pdf"))
