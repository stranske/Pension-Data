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
from typing import Literal

from pension_data.langchain.nl_sql_chain import (
    NLToSQLRequest,
    NLToSQLResponse,
    run_nl_sql_chain,
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
        try:
            import fcntl as fcntl_module
        except ImportError:
            pass

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
    )
