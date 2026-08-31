"""HTTP client for the Public Plans Database (PPD) API.

The client only constructs URLs and performs requests via the standard library
``urllib`` (no new third-party dependency, unlike the optional ``requests`` extra).
Network egress is sandboxed in CI and in this workspace, so ingestion is driven
from the on-disk cache (see :mod:`pension_data.sources.ppd.cache`) and recorded
fixtures. The live methods here exist for the documented acceptance criterion
("running the client populates >=200 plans") on an unrestricted host.

API shape (documented at publicplansdata.org/api):

    GET /api/?q=QVariables&variables=<comma-list>&filterfystart=<yr>&filterfyend=<yr>&format=json
    GET /api/?q=gettemplate&template=data-codebook&format=csv
"""

from __future__ import annotations

import json
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Sequence

PPD_API_BASE_URL = "https://publicplansdata.org/api/"
DEFAULT_TIMEOUT_SECONDS = 30.0


class PpdApiError(RuntimeError):
    """Raised when a PPD API request cannot be completed or parsed."""


def build_qvariables_url(
    *,
    variables: Sequence[str],
    fy_start: int,
    fy_end: int,
    fmt: str = "json",
    base_url: str = PPD_API_BASE_URL,
) -> str:
    """Construct the QVariables request URL for the given variables and fy range."""
    if not variables:
        raise PpdApiError("at least one variable is required for a QVariables request")
    if fy_start > fy_end:
        raise PpdApiError(f"fy_start ({fy_start}) must not exceed fy_end ({fy_end})")
    query = urllib.parse.urlencode(
        {
            "q": "QVariables",
            "variables": ",".join(variables),
            "filterfystart": fy_start,
            "filterfyend": fy_end,
            "format": fmt,
        }
    )
    return f"{base_url}?{query}"


def build_codebook_url(*, fmt: str = "csv", base_url: str = PPD_API_BASE_URL) -> str:
    """Construct the one-time codebook (data dictionary) request URL."""
    query = urllib.parse.urlencode({"q": "gettemplate", "template": "data-codebook", "format": fmt})
    return f"{base_url}?{query}"


class PpdClient:
    """Thin PPD API client over ``urllib`` with error handling.

    Parameters
    ----------
    base_url:
        API root; override for tests or a mirror.
    timeout:
        Per-request timeout in seconds. A finite timeout is mandatory so a hung
        socket never stalls an ingest run.
    """

    def __init__(
        self,
        *,
        base_url: str = PPD_API_BASE_URL,
        timeout: float = DEFAULT_TIMEOUT_SECONDS,
        opener: urllib.request.OpenerDirector | None = None,
    ) -> None:
        self.base_url = base_url
        self.timeout = timeout
        self._opener = opener or urllib.request.build_opener()

    def _get(self, url: str) -> str:
        """Perform a GET and return the decoded response body, or raise ``PpdApiError``."""
        request = urllib.request.Request(url, method="GET", headers={"Accept": "*/*"})
        try:
            with self._opener.open(request, timeout=self.timeout) as response:
                status = getattr(response, "status", 200)
                if status is not None and int(status) >= 400:
                    raise PpdApiError(f"PPD API returned HTTP {status} for {url}")
                charset = response.headers.get_content_charset() or "utf-8"
                body: bytes = response.read()
                return body.decode(charset)
        except urllib.error.HTTPError as exc:
            raise PpdApiError(f"PPD API HTTP error {exc.code} for {url}") from exc
        except urllib.error.URLError as exc:
            raise PpdApiError(f"PPD API request failed for {url}: {exc.reason}") from exc

    def qvariables_url(
        self, *, variables: Sequence[str], fy_start: int, fy_end: int, fmt: str = "json"
    ) -> str:
        """Public URL builder mirroring :func:`build_qvariables_url` with this base."""
        return build_qvariables_url(
            variables=variables,
            fy_start=fy_start,
            fy_end=fy_end,
            fmt=fmt,
            base_url=self.base_url,
        )

    def codebook_url(self, *, fmt: str = "csv") -> str:
        """Public URL builder mirroring :func:`build_codebook_url` with this base."""
        return build_codebook_url(fmt=fmt, base_url=self.base_url)

    def fetch_qvariables_raw(self, *, variables: Sequence[str], fy_start: int, fy_end: int) -> str:
        """Fetch the raw QVariables JSON body (string) for the given variables/years."""
        return self._get(self.qvariables_url(variables=variables, fy_start=fy_start, fy_end=fy_end))

    def fetch_codebook_raw(self) -> str:
        """Fetch the raw codebook CSV body (string)."""
        return self._get(self.codebook_url())

    def fetch_qvariables(
        self, *, variables: Sequence[str], fy_start: int, fy_end: int
    ) -> list[dict[str, object]]:
        """Fetch and parse QVariables records into a list of dict rows."""
        body = self.fetch_qvariables_raw(variables=variables, fy_start=fy_start, fy_end=fy_end)
        return parse_qvariables_json(body)


def parse_qvariables_json(body: str) -> list[dict[str, object]]:
    """Parse a QVariables JSON body into a list of record dicts.

    Accepts either a bare JSON array of records or an object wrapping the records
    under a ``data``/``result``/``records`` key (the PPD API has used both shapes).
    """
    try:
        payload = json.loads(body)
    except json.JSONDecodeError as exc:
        raise PpdApiError(f"PPD QVariables response was not valid JSON: {exc}") from exc

    records: object
    if isinstance(payload, list):
        records = payload
    elif isinstance(payload, dict):
        for key in ("data", "result", "records"):
            if isinstance(payload.get(key), list):
                records = payload[key]
                break
        else:
            raise PpdApiError("PPD QVariables object response had no data/result/records array")
    else:
        raise PpdApiError("PPD QVariables response was neither an array nor an object")

    if not isinstance(records, list) or not all(isinstance(row, dict) for row in records):
        raise PpdApiError("PPD QVariables records were not a list of objects")
    return [dict(row) for row in records]
