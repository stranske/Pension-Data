"""EDGAR 13F-HR fetch flow for security-level holdings collection (issue #647).

The SEC's public EDGAR system exposes an institutional manager's filing history
at ``https://data.sec.gov/submissions/CIK{10-digit}.json``. From that document we
select the most recent ``13F-HR`` filing and construct the URL of its INFORMATION
TABLE XML (the security-level holdings), which is parsed by
:func:`pension_data.extract.investment.security_positions.parse_13f_information_table_xml`.

The SEC **requires a descriptive ``User-Agent``** on every request (they return
HTTP 403 without one). This client enforces that at construction.

Network egress is sandboxed in CI and in this workspace, so no live call is made
in tests: the URL builders and the submissions-JSON selector are the tested units,
and the end-to-end flow is driven from **recorded-shape fixtures**
(``tests/ingest/fixtures/``). The live ``_get`` path exists for the documented
acceptance criterion on an unrestricted host and is excluded from coverage.

Public EDGAR shapes referenced (documented at www.sec.gov/os/webmaster-faq#developers):

    GET https://data.sec.gov/submissions/CIK{cik:010d}.json
        -> {"filings": {"recent": {"form": [...], "accessionNumber": [...],
                                    "reportDate": [...], "filingDate": [...],
                                    "primaryDocument": [...]}}}
    GET https://www.sec.gov/Archives/edgar/data/{cik}/{accession_no_dashes}/{document}
"""

from __future__ import annotations

import json
import urllib.error
import urllib.request
from collections.abc import Sequence
from dataclasses import dataclass

SUBMISSIONS_BASE_URL = "https://data.sec.gov/submissions/"
ARCHIVES_BASE_URL = "https://www.sec.gov/Archives/edgar/data/"
DEFAULT_TIMEOUT_SECONDS = 30.0
FORM_13F_HR = "13F-HR"
FORM_13F_HR_AMENDMENT = "13F-HR/A"


class EdgarApiError(RuntimeError):
    """Raised when an EDGAR request cannot be completed or a payload cannot be parsed."""


@dataclass(frozen=True, slots=True)
class Filing13F:
    """A single 13F filing selected from an EDGAR submissions document."""

    accession_number: str
    form: str
    filing_date: str
    report_date: str
    primary_document: str

    @property
    def is_amendment(self) -> bool:
        """Whether this filing is a 13F-HR/A amendment (restatement) rather than an original."""
        return self.form == FORM_13F_HR_AMENDMENT

    @property
    def accession_no_dashes(self) -> str:
        """Accession number with dashes stripped, as used in the Archives path."""
        return self.accession_number.replace("-", "")


def format_cik(cik: str | int) -> str:
    """Zero-pad a CIK to the 10-digit form EDGAR's submissions endpoint requires.

    Accepts an int, a bare numeric string, or an already ``CIK``-prefixed string.
    """
    text = str(cik).strip().upper()
    if text.startswith("CIK"):
        text = text[3:]
    digits = text.lstrip("0") or "0"
    if not digits.isdigit():
        raise EdgarApiError(f"CIK must be numeric, got {cik!r}")
    if len(digits) > 10:
        raise EdgarApiError(f"CIK {cik!r} exceeds 10 digits")
    return digits.zfill(10)


def build_submissions_url(cik: str | int, *, base_url: str = SUBMISSIONS_BASE_URL) -> str:
    """Construct the submissions JSON URL for a CIK: ``.../CIK{10-digit}.json``."""
    return f"{base_url}CIK{format_cik(cik)}.json"


def build_information_table_url(
    cik: str | int,
    *,
    accession_number: str,
    document: str,
    base_url: str = ARCHIVES_BASE_URL,
) -> str:
    """Construct the Archives URL of a filing's INFORMATION TABLE document.

    EDGAR archives paths use the *unpadded* integer CIK and the accession number
    with dashes stripped.
    """
    cik_int = int(format_cik(cik))
    accession_no_dashes = accession_number.replace("-", "")
    document = document.strip().lstrip("/")
    if not document:
        raise EdgarApiError("information table document name is required")
    return f"{base_url}{cik_int}/{accession_no_dashes}/{document}"


def _recent_filings_block(submissions_json: str) -> dict[str, list[object]]:
    try:
        payload = json.loads(submissions_json)
    except json.JSONDecodeError as exc:
        raise EdgarApiError(f"EDGAR submissions response was not valid JSON: {exc}") from exc
    if not isinstance(payload, dict):
        raise EdgarApiError("EDGAR submissions response was not a JSON object")
    filings = payload.get("filings")
    if not isinstance(filings, dict):
        raise EdgarApiError("EDGAR submissions payload has no 'filings' object")
    recent = filings.get("recent")
    if not isinstance(recent, dict):
        raise EdgarApiError("EDGAR submissions payload has no 'filings.recent' object")
    forms = recent.get("form")
    if not isinstance(forms, list):
        raise EdgarApiError("EDGAR submissions 'filings.recent.form' was not an array")
    return {key: value for key, value in recent.items() if isinstance(value, list)}


def parse_13f_hr_filings(
    submissions_json: str, *, include_amendments: bool = True
) -> list[Filing13F]:
    """Extract all 13F-HR (and optionally 13F-HR/A) filings from a submissions payload.

    Filings are returned newest-first, ordered by ``filingDate`` descending (ties
    broken by accession number) so the first element is the latest filing.
    """
    recent = _recent_filings_block(submissions_json)
    forms = recent["form"]
    accessions = recent.get("accessionNumber", [])
    filing_dates = recent.get("filingDate", [])
    report_dates = recent.get("reportDate", [])
    primary_docs = recent.get("primaryDocument", [])

    wanted = {FORM_13F_HR, FORM_13F_HR_AMENDMENT} if include_amendments else {FORM_13F_HR}
    filings: list[Filing13F] = []
    for index, form in enumerate(forms):
        if not isinstance(form, str) or form not in wanted:
            continue

        def _at(seq: Sequence[object], position: int = index) -> str:
            if position < len(seq) and isinstance(seq[position], str):
                return str(seq[position])
            return ""

        accession = _at(accessions)
        if not accession:
            raise EdgarApiError(
                f"EDGAR 13F filing at index {index} is missing its accession number"
            )
        filings.append(
            Filing13F(
                accession_number=accession,
                form=form,
                filing_date=_at(filing_dates),
                report_date=_at(report_dates),
                primary_document=_at(primary_docs),
            )
        )
    return sorted(
        filings,
        key=lambda filing: (filing.filing_date, filing.accession_number),
        reverse=True,
    )


def select_latest_13f_hr(submissions_json: str, *, include_amendments: bool = True) -> Filing13F:
    """Return the single most recent 13F-HR filing, or raise if the plan files none.

    Some plans outsource all public-equity management and barely file 13F, so a
    ``None`` here is a real "no equity sleeve on EDGAR" answer, not a bug -- callers
    should surface it as ``known_not_invested``/``not_disclosed`` rather than an error.
    """
    filings = parse_13f_hr_filings(submissions_json, include_amendments=include_amendments)
    if not filings:
        raise EdgarApiError("EDGAR submissions payload contains no 13F-HR filings")
    return filings[0]


class EdgarClient:
    """Thin EDGAR client over ``urllib`` that enforces the required ``User-Agent``.

    Parameters
    ----------
    user_agent:
        Descriptive contact string (SEC policy: "Sample Company Name
        admin@example.com"). Required; EDGAR returns HTTP 403 without it.
    timeout:
        Per-request timeout in seconds. A finite timeout is mandatory so a hung
        socket never stalls an ingest run.
    """

    def __init__(
        self,
        *,
        user_agent: str,
        submissions_base_url: str = SUBMISSIONS_BASE_URL,
        archives_base_url: str = ARCHIVES_BASE_URL,
        timeout: float = DEFAULT_TIMEOUT_SECONDS,
        opener: urllib.request.OpenerDirector | None = None,
    ) -> None:
        if not user_agent or not user_agent.strip():
            raise EdgarApiError("a descriptive User-Agent is required for EDGAR requests")
        self.user_agent = user_agent.strip()
        self.submissions_base_url = submissions_base_url
        self.archives_base_url = archives_base_url
        self.timeout = timeout
        self._opener = opener or urllib.request.build_opener()

    def _get(self, url: str) -> str:  # pragma: no cover - network path (egress sandboxed)
        # Do not advertise compressed transfer encodings: this deliberately small
        # client decodes the response bytes directly, and opting into gzip/deflate
        # without a matching decompression path can corrupt live EDGAR payloads.
        headers = {"User-Agent": self.user_agent}
        request = urllib.request.Request(url, method="GET", headers=headers)
        try:
            with self._opener.open(request, timeout=self.timeout) as response:
                status = getattr(response, "status", 200)
                if status is not None and int(status) >= 400:
                    raise EdgarApiError(f"EDGAR returned HTTP {status} for {url}")
                charset = response.headers.get_content_charset() or "utf-8"
                body: bytes = response.read()
                return body.decode(charset)
        except urllib.error.HTTPError as exc:
            raise EdgarApiError(f"EDGAR HTTP error {exc.code} for {url}") from exc
        except urllib.error.URLError as exc:
            raise EdgarApiError(f"EDGAR request failed for {url}: {exc.reason}") from exc

    def submissions_url(self, cik: str | int) -> str:
        """Public URL builder mirroring :func:`build_submissions_url` with this base."""
        return build_submissions_url(cik, base_url=self.submissions_base_url)

    def information_table_url(self, cik: str | int, *, accession_number: str, document: str) -> str:
        """Public URL builder mirroring :func:`build_information_table_url` with this base."""
        return build_information_table_url(
            cik,
            accession_number=accession_number,
            document=document,
            base_url=self.archives_base_url,
        )

    def fetch_submissions_raw(self, cik: str | int) -> str:  # pragma: no cover - network path
        """Fetch the raw submissions JSON body for a CIK."""
        return self._get(self.submissions_url(cik))

    def fetch_information_table_xml(
        self, cik: str | int, *, accession_number: str, document: str
    ) -> str:  # pragma: no cover - network path
        """Fetch the raw INFORMATION TABLE XML body for a specific 13F filing document."""
        return self._get(
            self.information_table_url(cik, accession_number=accession_number, document=document)
        )
