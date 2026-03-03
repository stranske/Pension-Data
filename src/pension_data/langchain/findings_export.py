"""Export payload helpers for findings explain/compare outputs."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Literal

from pension_data.langchain.findings_common import normalize_string_tuple, normalize_text

ExportType = Literal["explain", "compare"]


@dataclass(frozen=True, slots=True)
class FindingsExportArtifact:
    """Machine-readable + text-renderable findings artifact."""

    artifact_type: ExportType
    request_id: str
    generated_at: str
    trace: dict[str, str]
    payload: dict[str, object]
    citations: tuple[str, ...]


def _utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _normalize_trace(trace: Mapping[str, str] | None) -> dict[str, str]:
    normalized: dict[str, str] = {}
    for key, value in sorted((trace or {}).items()):
        name = normalize_text(key)
        text = normalize_text(value)
        if name and text:
            normalized[name] = text
    return normalized


def build_findings_export_artifact(
    *,
    artifact_type: ExportType,
    request_id: str,
    payload: Mapping[str, Any],
    citations: tuple[str, ...],
    trace: Mapping[str, str] | None = None,
) -> FindingsExportArtifact:
    """Build stable export artifact for downstream JSON/TXT persistence."""
    sanitized_payload: dict[str, object] = {}
    for key, value in sorted(payload.items()):
        field = normalize_text(key)
        if not field:
            continue
        if isinstance(value, str):
            sanitized_payload[field] = normalize_text(value)
            continue
        if isinstance(value, (tuple, list)):
            sanitized_payload[field] = normalize_string_tuple(value)
            continue
        sanitized_payload[field] = value
    normalized_citations = normalize_string_tuple(citations)
    return FindingsExportArtifact(
        artifact_type=artifact_type,
        request_id=request_id,
        generated_at=_utc_now_iso(),
        trace=_normalize_trace(trace),
        payload=sanitized_payload,
        citations=normalized_citations,
    )


def render_findings_export_text(artifact: FindingsExportArtifact) -> str:
    """Render plain-text export for analyst workflows."""
    lines = [
        f"artifact_type: {artifact.artifact_type}",
        f"request_id: {artifact.request_id}",
        f"generated_at: {artifact.generated_at}",
        "",
        "payload:",
    ]
    for key, value in artifact.payload.items():
        lines.append(f"- {key}: {value}")
    lines.extend(["", "citations:"])
    if artifact.citations:
        lines.extend(f"- {citation}" for citation in artifact.citations)
    else:
        lines.append("- none")
    if artifact.trace:
        lines.extend(["", "trace:"])
        lines.extend(f"- {key}: {value}" for key, value in artifact.trace.items())
    return "\n".join(lines) + "\n"
