"""Structured NL operation logging, replay, and summary helpers."""

from __future__ import annotations

import json
import os
import sqlite3
from collections.abc import Mapping
from contextlib import suppress
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from statistics import fmean
from types import ModuleType
from typing import Any, Literal, cast

from pension_data.langchain.nl_sql_chain import (
    NLToSQLError,
    NLToSQLMetadata,
    NLToSQLPolicy,
    NLToSQLProvenanceRow,
    NLToSQLRequest,
    NLToSQLResponse,
    run_nl_sql_chain,
)
from pension_data.query.run_record import (
    QueryRunActor,
    QueryRunArtifact,
    QueryRunRecord,
    default_run_record_root,
    load_rows_artifact,
    record_relative_path,
    write_query_run_record,
)

LogStatus = Literal["ok", "error"]


@dataclass(frozen=True, slots=True)
class NLOperationLogEntry:
    """Structured JSONL row for one NL operation."""

    timestamp: str
    request_id: str
    correlation_id: str
    provider: str
    model: str
    question: str
    generated_sql: str | None
    status: LogStatus
    latency_ms: int
    returned_rows: int
    trace_event_count: int
    error_code: str | None
    error_message: str | None
    max_rows: int
    timeout_ms: int


@dataclass(frozen=True, slots=True)
class NLOperationSummary:
    """Lightweight summary over NL operation logs."""

    total_requests: int
    failed_requests: int
    avg_latency_ms: float
    p95_latency_ms: float


def _utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _project_root(start: Path) -> Path:
    for candidate in (start, *start.parents):
        if (candidate / "pyproject.toml").exists():
            return candidate
    return Path.cwd()


def default_nl_log_path() -> Path:
    """Return default JSONL path for NL operation logs."""
    override = os.getenv("PENSION_DATA_NL_LOG_PATH", "").strip()
    if override:
        return Path(override).expanduser()
    root = _project_root(Path(__file__).resolve())
    return root / "artifacts" / "langchain" / "nl_operations.jsonl"


def build_nl_operation_log_entry(
    *,
    request: NLToSQLRequest,
    response: NLToSQLResponse,
    provider: str,
    model: str,
    correlation_id: str | None = None,
) -> NLOperationLogEntry:
    """Build one structured log row from request/response payloads."""
    return NLOperationLogEntry(
        timestamp=_utc_now_iso(),
        request_id=response.metadata.request_id,
        correlation_id=(correlation_id or response.metadata.request_id).strip(),
        provider=provider.strip() or "unknown",
        model=model.strip() or "unknown",
        question=" ".join(request.question.strip().split()),
        generated_sql=response.sql,
        status=response.status,
        latency_ms=response.metadata.duration_ms,
        returned_rows=response.metadata.returned_rows,
        trace_event_count=response.metadata.trace_event_count,
        error_code=response.error.code if response.error is not None else None,
        error_message=response.error.message if response.error is not None else None,
        max_rows=request.max_rows,
        timeout_ms=request.timeout_ms,
    )


def build_nl_query_run_record(
    *,
    request: NLToSQLRequest,
    response: NLToSQLResponse,
    key_id: str,
    scopes: tuple[str, ...],
    required_scope: str,
    correlation_id: str,
    rows_artifact: QueryRunArtifact | None,
) -> QueryRunRecord:
    """Build the replayable NL query run record artifact payload."""
    provenance = tuple(_provenance_to_dict(row) for row in response.provenance)
    return QueryRunRecord(
        run_id=response.metadata.request_id,
        surface="nl",
        status=response.status,
        who=QueryRunActor(
            key_id=key_id,
            scopes=scopes,
            required_scope=required_scope,
            correlation_id=correlation_id,
        ),
        inputs={
            "question": " ".join(request.question.strip().split()),
            "max_rows": request.max_rows,
            "timeout_ms": request.timeout_ms,
            "params": request.params,
        },
        generated_sql=response.sql,
        executed_sql=response.sql,
        columns=response.columns,
        row_count=response.metadata.returned_rows,
        rows_artifact=rows_artifact,
        provenance=provenance,
        warnings=(),
        error=_error_to_dict(response.error),
        duration_ms=response.metadata.duration_ms,
        cost=response.metadata.cost,
        artifacts=(() if rows_artifact is None else (rows_artifact,)),
    )


def persist_nl_query_run_record(
    *,
    request: NLToSQLRequest,
    response: NLToSQLResponse,
    key_id: str,
    scopes: tuple[str, ...],
    required_scope: str,
    correlation_id: str,
    root: Path | None = None,
) -> QueryRunArtifact:
    """Persist the NL query rows and replayable run record."""
    artifact_root = root or default_run_record_root()
    rows_path = (
        artifact_root
        / "langchain"
        / "nl_runs"
        / "rows"
        / f"{_safe_run_id(response.metadata.request_id)}.json"
    )
    rows_artifact = QueryRunArtifact(
        name="nl-query-rows",
        path=record_relative_path(rows_path, root=artifact_root),
        content_type="application/json",
        row_count=response.metadata.returned_rows,
    )
    record = build_nl_query_run_record(
        request=request,
        response=response,
        key_id=key_id,
        scopes=scopes,
        required_scope=required_scope,
        correlation_id=correlation_id,
        rows_artifact=rows_artifact,
    )
    return write_query_run_record(
        root=artifact_root,
        surface="nl",
        run_id=response.metadata.request_id,
        record=record,
        rows=response.rows,
    )


def append_nl_operation_log(
    *,
    path: Path,
    entry: NLOperationLogEntry,
    retention_limit: int = 2_000,
) -> None:
    """Append one JSONL log row and enforce retention."""
    if retention_limit < 1:
        raise ValueError("retention_limit must be >= 1")
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+", encoding="utf-8") as handle:
        fcntl_module: ModuleType | None = None
        with suppress(ImportError):
            import fcntl as fcntl_module

        if fcntl_module is not None:
            with suppress(OSError):
                fcntl_module.flock(handle.fileno(), fcntl_module.LOCK_EX)

        handle.write(json.dumps(asdict(entry), sort_keys=True) + "\n")
        handle.flush()
        handle.seek(0)
        lines = handle.read().splitlines()
        retention_trigger = retention_limit
        if len(lines) > retention_trigger:
            trimmed = lines[-retention_limit:]
            temp_path = path.with_suffix(path.suffix + ".tmp")
            temp_path.write_text("\n".join(trimmed) + "\n", encoding="utf-8")
            temp_path.replace(path)

        if fcntl_module is not None:
            with suppress(Exception):
                fcntl_module.flock(handle.fileno(), fcntl_module.LOCK_UN)


def load_nl_operation_logs(
    path: Path, *, limit: int | None = None
) -> tuple[NLOperationLogEntry, ...]:
    """Load structured NL operation logs from JSONL."""
    if not path.exists():
        return ()
    rows = path.read_text(encoding="utf-8").splitlines()
    if limit is not None:
        rows = rows[-limit:]
    entries: list[NLOperationLogEntry] = []
    for row in rows:
        if not row.strip():
            continue
        try:
            payload = json.loads(row)
        except json.JSONDecodeError:
            continue
        if not isinstance(payload, Mapping):
            continue
        try:
            entries.append(
                NLOperationLogEntry(
                    timestamp=str(payload.get("timestamp", "")),
                    request_id=str(payload.get("request_id", "")),
                    correlation_id=str(payload.get("correlation_id", "")),
                    provider=str(payload.get("provider", "")),
                    model=str(payload.get("model", "")),
                    question=str(payload.get("question", "")),
                    generated_sql=(
                        None
                        if payload.get("generated_sql") is None
                        else str(payload.get("generated_sql"))
                    ),
                    status="ok" if str(payload.get("status", "")) == "ok" else "error",
                    latency_ms=int(payload.get("latency_ms", 0)),
                    returned_rows=int(payload.get("returned_rows", 0)),
                    trace_event_count=int(payload.get("trace_event_count", 0)),
                    error_code=(
                        None
                        if payload.get("error_code") is None
                        else str(payload.get("error_code"))
                    ),
                    error_message=(
                        None
                        if payload.get("error_message") is None
                        else str(payload.get("error_message"))
                    ),
                    max_rows=int(payload.get("max_rows", 0)),
                    timeout_ms=int(payload.get("timeout_ms", 0)),
                )
            )
        except (TypeError, ValueError):
            continue
    return tuple(entries)


def summarize_nl_operation_logs(entries: tuple[NLOperationLogEntry, ...]) -> NLOperationSummary:
    """Return lightweight failure/latency summary from operation logs."""
    if not entries:
        return NLOperationSummary(
            total_requests=0,
            failed_requests=0,
            avg_latency_ms=0.0,
            p95_latency_ms=0.0,
        )
    latencies = sorted(entry.latency_ms for entry in entries)
    p95_index = max(0, int(round((len(latencies) - 1) * 0.95)))
    return NLOperationSummary(
        total_requests=len(entries),
        failed_requests=sum(1 for entry in entries if entry.status == "error"),
        avg_latency_ms=fmean(latencies),
        p95_latency_ms=float(latencies[p95_index]),
    )


class _ReplayChain:
    def __init__(self, sql: str) -> None:
        self._sql = sql

    def invoke(self, values: Mapping[str, object]) -> str:
        del values
        return self._sql


def replay_logged_request(
    *,
    entry: NLOperationLogEntry,
    connection: sqlite3.Connection,
    policy: NLToSQLPolicy | None = None,
) -> NLToSQLResponse:
    """Replay one logged request deterministically using logged SQL text."""
    if entry.generated_sql is None or not entry.generated_sql.strip():
        raise ValueError("cannot replay entry without generated_sql")
    return run_nl_sql_chain(
        connection=connection,
        request=NLToSQLRequest(
            question=entry.question,
            max_rows=max(1, entry.max_rows),
            timeout_ms=max(1, entry.timeout_ms),
        ),
        chain=_ReplayChain(entry.generated_sql),
        policy=policy,
    )


def replay_run_record(
    *, record: QueryRunRecord | Mapping[str, Any], root: Path | None = None
) -> NLToSQLResponse:
    """Replay a persisted NL query run record from its rows/provenance artifacts."""
    payload = record.to_dict() if isinstance(record, QueryRunRecord) else dict(record)
    rows_artifact = payload.get("rows_artifact")
    if not isinstance(rows_artifact, Mapping):
        raise ValueError("run record has no rows_artifact")
    artifact_root = root or default_run_record_root()
    rows_payload = load_rows_artifact(root=artifact_root, artifact=rows_artifact)
    columns_raw = rows_payload.get("columns", ())
    rows_raw = rows_payload.get("rows", ())
    columns = tuple(str(column) for column in columns_raw if isinstance(column, str))
    rows = tuple(tuple(row) for row in rows_raw if isinstance(row, list))
    provenance_payload = payload.get("provenance", ())
    provenance = tuple(
        _provenance_from_dict(item) for item in provenance_payload if isinstance(item, Mapping)
    )
    error_payload = payload.get("error")
    error = None
    if isinstance(error_payload, Mapping):
        error = NLToSQLError(
            code=str(error_payload.get("code", "")),
            message=str(error_payload.get("message", "")),
        )
    return NLToSQLResponse(
        status="ok" if payload.get("status") == "ok" else "error",
        sql=None if payload.get("executed_sql") is None else str(payload.get("executed_sql")),
        columns=columns,
        rows=rows,
        provenance=provenance,
        metadata=NLToSQLMetadata(
            request_id=str(payload.get("run_id", "")),
            duration_ms=int(payload.get("duration_ms", 0)),
            returned_rows=int(payload.get("row_count", len(rows))),
            trace_event_count=0,
            cost=cast(Mapping[str, Any] | None, payload.get("cost")),
        ),
        error=error,
    )


def _provenance_to_dict(row: NLToSQLProvenanceRow) -> dict[str, Any]:
    return {
        "row_index": row.row_index,
        "source_document_id": row.source_document_id,
        "evidence_refs": row.evidence_refs,
        "confidence": row.confidence,
    }


def _provenance_from_dict(payload: Mapping[str, Any]) -> NLToSQLProvenanceRow:
    refs = payload.get("evidence_refs", ())
    raw_confidence = payload.get("confidence")
    return NLToSQLProvenanceRow(
        row_index=int(payload.get("row_index", 0)),
        source_document_id=(
            None
            if payload.get("source_document_id") is None
            else str(payload.get("source_document_id"))
        ),
        evidence_refs=tuple(str(item) for item in refs if isinstance(item, str)),
        confidence=None if raw_confidence is None else float(raw_confidence),
    )


def _error_to_dict(error: NLToSQLError | None) -> dict[str, str] | None:
    if error is None:
        return None
    return {"code": error.code, "message": error.message}


def _safe_run_id(run_id: str) -> str:
    return "".join(char if char.isalnum() or char in {"-", "_"} else "-" for char in run_id)
