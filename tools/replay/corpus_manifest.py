"""Load replay documents from a pinned external artifact manifest."""

from __future__ import annotations

import hashlib
import json
import re
from io import BytesIO
from pathlib import Path
from typing import Any

from pypdf import PdfReader
from pypdf.errors import PdfReadError

from tools.replay.harness import CorpusDocument

_CONFIG_SCHEMA = "pension-replay-corpus/v1"
_PDF_MEDIA_TYPE = "application/pdf"
_SHA256_PATTERN = re.compile(r"[0-9a-f]{64}")


def _load_json_object(path: Path, *, label: str) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{label} must be a JSON object: {path}")
    return payload


def _required_string(payload: dict[str, Any], key: str, *, label: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{label} requires non-empty string '{key}'")
    return value


def _safe_repo_path(root: Path, relative_path: str, *, label: str) -> Path:
    relative = Path(relative_path)
    if relative.is_absolute():
        raise ValueError(f"{label} must be repository-relative: {relative_path}")

    resolved_root = root.resolve(strict=True)
    resolved = (resolved_root / relative).resolve(strict=True)
    if not resolved.is_relative_to(resolved_root):
        raise ValueError(f"{label} escapes artifact root: {relative_path}")
    return resolved


def _validate_sha256(value: object, *, label: str) -> str:
    if not isinstance(value, str) or _SHA256_PATTERN.fullmatch(value) is None:
        raise ValueError(f"{label} must be a lowercase 64-character SHA-256 digest")
    return value


def _extract_pdf_text(data: bytes, *, entry_id: str) -> str:
    try:
        reader = PdfReader(BytesIO(data))
        return "\n\f\n".join(page.extract_text() or "" for page in reader.pages)
    except (PdfReadError, OSError, ValueError) as exc:
        raise ValueError(f"manifest entry '{entry_id}' is not a readable PDF: {exc}") from exc


def load_manifest_corpus(config_path: Path, *, artifact_root: Path) -> list[CorpusDocument]:
    """Load and verify selected PDF entries from a local upstream checkout."""
    config = _load_json_object(config_path, label="replay corpus config")
    if config.get("schema_version") != _CONFIG_SCHEMA:
        raise ValueError(
            f"replay corpus config schema must be {_CONFIG_SCHEMA!r}; "
            f"got {config.get('schema_version')!r}"
        )

    upstream = config.get("upstream")
    if not isinstance(upstream, dict):
        raise ValueError("replay corpus config requires object 'upstream'")
    _required_string(upstream, "repository", label="upstream")
    _required_string(upstream, "revision", label="upstream")
    manifest_relative_path = _required_string(upstream, "manifest_path", label="upstream")
    expected_manifest_schema = _required_string(upstream, "manifest_schema", label="upstream")

    manifest_path = _safe_repo_path(
        artifact_root,
        manifest_relative_path,
        label="upstream manifest_path",
    )
    manifest = _load_json_object(manifest_path, label="upstream artifact manifest")
    if manifest.get("schema_version") != expected_manifest_schema:
        raise ValueError(
            "upstream artifact manifest schema does not match config: "
            f"expected {expected_manifest_schema!r}, got {manifest.get('schema_version')!r}"
        )

    raw_artifacts = manifest.get("artifacts")
    if not isinstance(raw_artifacts, list) or not all(
        isinstance(artifact, dict) for artifact in raw_artifacts
    ):
        raise ValueError("upstream artifact manifest requires an 'artifacts' object list")

    artifacts_by_id: dict[str, dict[str, Any]] = {}
    for artifact in raw_artifacts:
        artifact_id = _required_string(artifact, "artifact_id", label="manifest artifact")
        if artifact_id in artifacts_by_id:
            raise ValueError(f"duplicate upstream manifest artifact_id: {artifact_id}")
        artifacts_by_id[artifact_id] = artifact

    selections = config.get("entries")
    if not isinstance(selections, list) or not selections:
        raise ValueError("replay corpus config requires a non-empty 'entries' list")

    documents: list[CorpusDocument] = []
    selected_ids: set[str] = set()
    for selection in selections:
        if not isinstance(selection, dict):
            raise ValueError("replay corpus entry selections must be objects")
        entry_id = _required_string(selection, "entry_id", label="replay corpus entry")
        if entry_id in selected_ids:
            raise ValueError(f"duplicate replay corpus entry_id: {entry_id}")
        selected_ids.add(entry_id)

        pinned_sha256 = _validate_sha256(
            selection.get("sha256"), label=f"replay corpus entry '{entry_id}' sha256"
        )
        artifact = artifacts_by_id.get(entry_id)
        if artifact is None:
            raise ValueError(f"replay corpus entry '{entry_id}' is absent from upstream manifest")

        manifest_sha256 = _validate_sha256(
            artifact.get("sha256"), label=f"upstream manifest entry '{entry_id}' sha256"
        )
        if manifest_sha256 != pinned_sha256:
            raise ValueError(
                f"replay corpus entry '{entry_id}' digest does not match upstream manifest"
            )
        if artifact.get("media_type") != _PDF_MEDIA_TYPE:
            raise ValueError(
                f"replay corpus entry '{entry_id}' must use media_type {_PDF_MEDIA_TYPE!r}"
            )

        expected_bytes = artifact.get("bytes")
        if (
            not isinstance(expected_bytes, int)
            or isinstance(expected_bytes, bool)
            or expected_bytes < 0
        ):
            raise ValueError(f"upstream manifest entry '{entry_id}' has invalid byte count")
        artifact_relative_path = _required_string(
            artifact, "path", label=f"upstream manifest entry '{entry_id}'"
        )
        artifact_path = _safe_repo_path(
            artifact_root,
            artifact_relative_path,
            label=f"upstream manifest entry '{entry_id}' path",
        )
        data = artifact_path.read_bytes()
        if len(data) != expected_bytes:
            raise ValueError(
                f"replay corpus entry '{entry_id}' byte count mismatch: "
                f"expected {expected_bytes}, got {len(data)}"
            )
        actual_sha256 = hashlib.sha256(data).hexdigest()
        if actual_sha256 != pinned_sha256:
            raise ValueError(
                f"replay corpus entry '{entry_id}' checksum mismatch: "
                f"expected {pinned_sha256}, got {actual_sha256}"
            )

        documents.append(
            CorpusDocument(
                document_id=entry_id,
                content=_extract_pdf_text(data, entry_id=entry_id),
            )
        )

    return documents
