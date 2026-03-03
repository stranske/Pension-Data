"""Route adapters for citation-ready export generation."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from pension_data.api.auth import (
    SCOPE_EXPORT,
    APIKeyStore,
    authenticate_request,
    build_audit_event,
)
from pension_data.api.auth.audit import RESERVED_AUDIT_KEYS
from pension_data.export import (
    CitationExport,
    CitationReference,
    MetricHistoryExportInput,
    build_metric_history_citation_export,
    build_sql_citation_export,
)


@dataclass(frozen=True, slots=True)
class ExportRouteResult:
    """Route response bundle containing export payload and audit event metadata."""

    export: CitationExport
    audit_event: dict[str, Any]


def _sanitize_event(event: Mapping[str, Any] | None) -> dict[str, Any]:
    payload = dict(event or {})
    for reserved_key in RESERVED_AUDIT_KEYS:
        payload.pop(reserved_key, None)
    return payload


def run_sql_export_endpoint(
    *,
    api_key_header: str | None,
    key_store: APIKeyStore,
    columns: Sequence[str],
    rows: Sequence[Sequence[Any]],
    provenance_by_row_id: Mapping[str, Sequence[CitationReference]] | None = None,
    event: Mapping[str, Any] | None = None,
) -> ExportRouteResult:
    """Authenticate and generate citation-ready SQL export payload."""
    auth_context = authenticate_request(
        api_key_header=api_key_header,
        required_scope=SCOPE_EXPORT,
        key_store=key_store,
    )
    export_payload = build_sql_citation_export(
        columns=columns,
        rows=rows,
        provenance_by_row_id=provenance_by_row_id,
    )
    event_payload = _sanitize_event(event)
    event_payload.update(
        {
            "schema_name": export_payload.schema_name,
            "schema_version": export_payload.schema_version,
            "row_count": len(export_payload.rows),
            "citation_count": len(export_payload.citation_bundle.citations),
            "missing_provenance_rows": len(export_payload.citation_bundle.missing_row_ids),
        }
    )
    return ExportRouteResult(
        export=export_payload,
        audit_event=build_audit_event(
            operation="export.sql",
            auth_context=auth_context,
            event=event_payload,
        ),
    )


def run_metric_history_export_endpoint(
    *,
    api_key_header: str | None,
    key_store: APIKeyStore,
    rows: Sequence[MetricHistoryExportInput],
    event: Mapping[str, Any] | None = None,
) -> ExportRouteResult:
    """Authenticate and generate citation-ready metric-history export payload."""
    auth_context = authenticate_request(
        api_key_header=api_key_header,
        required_scope=SCOPE_EXPORT,
        key_store=key_store,
    )
    export_payload = build_metric_history_citation_export(rows)
    event_payload = _sanitize_event(event)
    event_payload.update(
        {
            "schema_name": export_payload.schema_name,
            "schema_version": export_payload.schema_version,
            "row_count": len(export_payload.rows),
            "citation_count": len(export_payload.citation_bundle.citations),
            "missing_provenance_rows": len(export_payload.citation_bundle.missing_row_ids),
        }
    )
    return ExportRouteResult(
        export=export_payload,
        audit_event=build_audit_event(
            operation="export.metric_history",
            auth_context=auth_context,
            event=event_payload,
        ),
    )
