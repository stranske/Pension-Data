"""On-disk response cache for PPD API bodies.

The cache makes ingestion offline-reusable and idempotent: a recorded response
(from a live pull, or a saved fixture) is stored keyed by the request signature,
and every later ingest reads the same bytes. Because the cache key is a pure
function of the request, re-running a refresh against unchanged source data
reads the identical body and therefore produces the identical dataset.
"""

from __future__ import annotations

import hashlib
from pathlib import Path

from pension_data.sources.ppd.client import (
    PpdClient,
    build_codebook_url,
    build_qvariables_url,
    parse_qvariables_json,
)


def _cache_key(url: str) -> str:
    """Deterministic filename stem for a request URL."""
    return hashlib.sha256(url.encode("utf-8")).hexdigest()[:24]


class PpdResponseCache:
    """Filesystem cache of raw PPD response bodies keyed by request URL."""

    def __init__(self, cache_dir: str | Path) -> None:
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    def _path_for(self, url: str, suffix: str) -> Path:
        return self.cache_dir / f"{_cache_key(url)}{suffix}"

    def has(self, url: str, *, suffix: str) -> bool:
        """Whether a cached body exists for this URL."""
        return self._path_for(url, suffix).exists()

    def read(self, url: str, *, suffix: str) -> str | None:
        """Return the cached body for a URL, or ``None`` if absent."""
        path = self._path_for(url, suffix)
        if not path.exists():
            return None
        return path.read_text(encoding="utf-8")

    def write(self, url: str, body: str, *, suffix: str) -> Path:
        """Store a raw response body under this URL's key; return the path."""
        path = self._path_for(url, suffix)
        path.write_text(body, encoding="utf-8")
        return path

    # -- Convenience seams keyed by the same URL builders the client uses -------

    def record_qvariables(
        self, body: str, *, variables: list[str], fy_start: int, fy_end: int
    ) -> Path:
        """Record a QVariables body (e.g. a fixture) under its canonical URL key."""
        url = build_qvariables_url(variables=variables, fy_start=fy_start, fy_end=fy_end)
        return self.write(url, body, suffix=".json")

    def record_codebook(self, body: str) -> Path:
        """Record a codebook CSV body under its canonical URL key."""
        return self.write(build_codebook_url(), body, suffix=".csv")

    def load_qvariables(
        self,
        *,
        variables: list[str],
        fy_start: int,
        fy_end: int,
        client: PpdClient | None = None,
    ) -> list[dict[str, object]]:
        """Return parsed QVariables records, fetching+caching only on a cache miss.

        With ``client=None`` (the sandboxed default) a cache miss is an error, so
        no accidental network call can happen; provide a client to allow a live
        fetch on an unrestricted host.
        """
        url = build_qvariables_url(variables=variables, fy_start=fy_start, fy_end=fy_end)
        body = self.read(url, suffix=".json")
        if body is None:
            if client is None:
                raise FileNotFoundError(
                    f"no cached PPD QVariables response for {url}; "
                    "record a fixture or supply a client for a live fetch"
                )
            body = client.fetch_qvariables_raw(
                variables=variables, fy_start=fy_start, fy_end=fy_end
            )
            self.write(url, body, suffix=".json")
        return parse_qvariables_json(body)

    def load_codebook_csv(self, *, client: PpdClient | None = None) -> str:
        """Return the codebook CSV body, fetching+caching only on a cache miss."""
        url = build_codebook_url()
        body = self.read(url, suffix=".csv")
        if body is None:
            if client is None:
                raise FileNotFoundError(
                    f"no cached PPD codebook response for {url}; "
                    "record a fixture or supply a client for a live fetch"
                )
            body = client.fetch_codebook_raw()
            self.write(url, body, suffix=".csv")
        return body
