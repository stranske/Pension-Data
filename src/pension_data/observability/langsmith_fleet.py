"""Build dashboard-safe LangSmith fleet records for NL-to-SQL runs.

The shared schema is owned by `stranske/Workflows#2150`; this module emits the
Pension-Data-specific `langsmith-fleet/v1` records for the NL query lifecycle.
Records cover the sql-generation, validation, execution, and (optional) replay
operations registered in `config/langsmith_fleet_registry.json`.

The module deliberately avoids storing raw NL prompts, generated SQL strings,
result rows, or member data; only validation states, row counts, error codes,
and stable identifiers are written. When `LANGSMITH_API_KEY` is absent every
record is emitted with status `no_secret` and the helper performs no network
calls.
"""

from __future__ import annotations

import json
import os
from collections.abc import Iterable, Mapping
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from types import ModuleType
from typing import Any, Final, Literal

SCHEMA_VERSION: Final = "langsmith-fleet/v1"
REPO: Final = "stranske/Pension-Data"
SURFACE: Final = "nl-to-sql"
GITHUB_ISSUE: Final = "stranske/Pension-Data#445"
ARTIFACT_NAME: Final = "langsmith-fleet.ndjson"
DEFAULT_PROJECT: Final = "pension-data"
ENV_LANGSMITH_KEY: Final = "LANGSMITH_API_KEY"
ENV_LANGCHAIN_PROJECT: Final = "LANGCHAIN_PROJECT"
ENV_LANGSMITH_PROJECT: Final = "LANGSMITH_PROJECT"
ENV_LANGCHAIN_TRACING_V2: Final = "LANGCHAIN_TRACING_V2"
ENV_LANGCHAIN_API_KEY: Final = "LANGCHAIN_API_KEY"

Status = Literal["success", "error", "fallback", "no_secret", "skipped"]

_OPERATIONS: Final[tuple[str, ...]] = (
    "sql-generation",
    "validation",
    "execution",
    "replay",
)

_VALIDATION_FAILURE_CODES: Final = frozenset({"AMBIGUOUS_PROMPT", "INVALID_REQUEST", "UNSAFE_SQL"})
_EXECUTION_FAILURE_CODES: Final = frozenset(
    {"MAX_ROWS_EXCEEDED", "TIMEOUT", "SYNTAX_ERROR", "EXECUTION_ERROR"}
)


@dataclass(frozen=True, slots=True)
class FleetRunContext:
    """Shared trace context for one Pension-Data NL-to-SQL run."""

    run_id: str
    query_category: str
    query_intent: str | None = None
    provider: str | None = None
    model: str | None = None
    trace_id: str | None = None
    trace_url: str | None = None
    recorded_at: str | None = None
    github_pr: str | None = None
    latency_ms: int | None = None


class LangSmithClientTraceSink:
    """Trace sink that writes sanitized NL lifecycle events to LangSmith."""

    def __init__(self, *, client: Any | None = None, project_name: str = DEFAULT_PROJECT) -> None:
        if client is None:
            from langsmith import Client

            client = Client()
        self._client = client
        self._project_name = project_name

    def emit(self, event: Any) -> None:
        """Create one ended LangSmith run for a sanitized lifecycle event."""

        stage = str(getattr(event, "stage", "") or "nl.unknown")
        payload = _safe_trace_payload(getattr(event, "payload", {}))
        request_id = str(payload.get("request_id", ""))
        timestamp = datetime.now(UTC)
        self._client.create_run(
            name=f"{SURFACE}.{stage}",
            run_type="chain",
            project_name=self._project_name,
            start_time=timestamp,
            end_time=timestamp,
            inputs={
                "request_id": request_id,
                "stage": stage,
            },
            outputs=payload,
            extra={
                "metadata": {
                    "repo": REPO,
                    "surface": SURFACE,
                    "github_issue": GITHUB_ISSUE,
                    "stage": stage,
                }
            },
            tags=[REPO, SURFACE, stage],
        )


def build_langsmith_trace_sink(
    *,
    client: Any | None = None,
    project_name: str = DEFAULT_PROJECT,
) -> LangSmithClientTraceSink | None:
    """Return a LangSmith-backed trace sink when tracing credentials exist."""

    if not ensure_langsmith_project_defaults():
        return None
    return LangSmithClientTraceSink(client=client, project_name=project_name)


def ensure_langsmith_project_defaults() -> bool:
    """Apply Pension-Data LangSmith defaults when a key is present.

    Returns ``True`` when tracing is configured (key was found and env vars
    initialized). Returns ``False`` (and changes no env vars) when no key is
    configured, signalling callers to emit ``no_secret`` records.
    """

    api_key = os.environ.get(ENV_LANGSMITH_KEY)
    if not api_key:
        return False
    os.environ.setdefault(ENV_LANGCHAIN_TRACING_V2, "true")
    os.environ.setdefault(ENV_LANGCHAIN_PROJECT, DEFAULT_PROJECT)
    os.environ.setdefault(ENV_LANGSMITH_PROJECT, DEFAULT_PROJECT)
    os.environ.setdefault(ENV_LANGCHAIN_API_KEY, api_key)
    return True


def build_fleet_records(
    *,
    context: FleetRunContext,
    sql_validation_status: str,
    read_only_status: str,
    row_count: int,
    max_rows: int | None = None,
    trace_event_count: int | None = None,
    error_code: str | None = None,
    error_stage: str | None = None,
    replay_dataset_id: str | None = None,
    replay_run_id: str | None = None,
    replay_match_status: str | None = None,
    golden_corpus_outcome: str | None = None,
    evidence_availability: str | None = None,
    artifact_ref: str | None = None,
) -> list[dict[str, Any]]:
    """Return Workflows-compatible fleet records for one NL-to-SQL run.

    `sql_validation_status` covers the validation stage outcome (``"pass"``,
    ``"unsafe"``, ``"ambiguous"``, ``"invalid_request"``, or ``"skipped"``).
    `read_only_status` reflects the SQL safety verdict (``"read_only"``,
    ``"blocked"``, or ``"unknown"``). Row count is the executed row count
    (zero on validation/execution failure). Replay fields, when provided,
    surface a deterministic correlation between the NL run and a replay
    dataset/run pair without exposing member data.
    """

    tracing_enabled = ensure_langsmith_project_defaults()
    base_status: Status = "success" if tracing_enabled else "no_secret"
    recorded_at = context.recorded_at or _utc_timestamp()
    sanitized_row_count = max(0, int(row_count))
    sanitized_max_rows = max(0, int(max_rows)) if max_rows is not None else None
    sanitized_trace_count = (
        max(0, int(trace_event_count)) if trace_event_count is not None else None
    )

    shared_domain: dict[str, Any] = {
        "query_category": context.query_category,
        "query_intent": context.query_intent or context.query_category,
        "sql_validation_status": sql_validation_status,
        "read_only_status": read_only_status,
        "row_count": sanitized_row_count,
        "evidence_availability": evidence_availability or "unknown",
    }
    if replay_dataset_id:
        shared_domain["replay_dataset_id"] = replay_dataset_id
    if replay_run_id:
        shared_domain["replay_run_id"] = replay_run_id
    if replay_match_status:
        shared_domain["replay_match_status"] = replay_match_status
    if golden_corpus_outcome:
        shared_domain["golden_corpus_outcome"] = golden_corpus_outcome
    if sanitized_max_rows is not None:
        shared_domain["max_rows"] = sanitized_max_rows
    if context.latency_ms is not None:
        shared_domain["latency_ms"] = max(0, int(context.latency_ms))
    if sanitized_trace_count is not None:
        shared_domain["trace_event_count"] = sanitized_trace_count

    generation_status: Status = _stage_status(
        base_status,
        error_code=error_code,
        stage="sql-generation",
        error_stage=error_stage,
    )
    validation_status: Status = _stage_status(
        base_status,
        error_code=error_code,
        stage="validation",
        error_stage=error_stage,
    )
    execution_status: Status = _stage_status(
        base_status,
        error_code=error_code,
        stage="execution",
        error_stage=error_stage,
    )
    replay_provided = bool(replay_dataset_id or replay_run_id or replay_match_status)
    replay_status: Status = (
        _stage_status(
            base_status,
            error_code=error_code,
            stage="replay",
            error_stage=error_stage,
        )
        if replay_provided
        else "skipped"
    )

    records: list[dict[str, Any]] = [
        _record(
            context=context,
            operation="sql-generation",
            status=generation_status,
            recorded_at=recorded_at,
            domain={**shared_domain, "stage": "sql-generation"},
            error_code=error_code if generation_status == "error" else None,
            artifact_ref=artifact_ref,
        ),
        _record(
            context=context,
            operation="validation",
            status=validation_status,
            recorded_at=recorded_at,
            domain={**shared_domain, "stage": "validation"},
            error_code=error_code if validation_status == "error" else None,
            artifact_ref=artifact_ref,
        ),
        _record(
            context=context,
            operation="execution",
            status=execution_status,
            recorded_at=recorded_at,
            domain={**shared_domain, "stage": "execution"},
            error_code=error_code if execution_status == "error" else None,
            artifact_ref=artifact_ref,
        ),
    ]

    replay_domain: dict[str, Any] = {**shared_domain, "stage": "replay"}
    records.append(
        _record(
            context=context,
            operation="replay",
            status=replay_status,
            recorded_at=recorded_at,
            domain=replay_domain,
            error_code=error_code if replay_status == "error" else None,
            artifact_ref=artifact_ref,
        )
    )

    return records


def build_fleet_records_from_response(
    *,
    context: FleetRunContext,
    response: Any,
    request: Any | None = None,
    replay_dataset_id: str | None = None,
    replay_run_id: str | None = None,
    replay_match_status: str | None = None,
    golden_corpus_outcome: str | None = None,
    artifact_ref: str | None = None,
) -> list[dict[str, Any]]:
    """Build fleet records directly from an NL-to-SQL chain response.

    Derives `sql_validation_status`, `read_only_status`, `row_count`, and
    `error_code` from `NLToSQLResponse` fields. The response's metadata
    supplies request/run identifiers, latency, and trace event counts so
    callers do not have to duplicate that wiring. The chain response is not
    imported at module load time to avoid a circular dependency between
    the langchain and observability layers.
    """

    error_code = _response_error_code(response)
    sql_validation_status = _derive_sql_validation_status(error_code, response)
    read_only_status = _derive_read_only_status(error_code, response)
    row_count = _response_row_count(response)
    max_rows = _request_max_rows(request)
    trace_event_count = _response_trace_event_count(response)
    error_stage = _error_stage_for(error_code)
    latency_ms = _response_latency_ms(response)
    evidence_availability = _derive_evidence_availability(response)
    if context.latency_ms is None and latency_ms is not None:
        context = FleetRunContext(
            run_id=context.run_id,
            query_category=context.query_category,
            query_intent=context.query_intent,
            provider=context.provider,
            model=context.model,
            trace_id=context.trace_id,
            trace_url=context.trace_url,
            recorded_at=context.recorded_at,
            github_pr=context.github_pr,
            latency_ms=latency_ms,
        )

    return build_fleet_records(
        context=context,
        sql_validation_status=sql_validation_status,
        read_only_status=read_only_status,
        row_count=row_count,
        max_rows=max_rows,
        trace_event_count=trace_event_count,
        error_code=error_code,
        error_stage=error_stage,
        replay_dataset_id=replay_dataset_id,
        replay_run_id=replay_run_id,
        replay_match_status=replay_match_status,
        golden_corpus_outcome=golden_corpus_outcome,
        evidence_availability=evidence_availability,
        artifact_ref=artifact_ref,
    )


def write_fleet_records(path: Path, records: Iterable[Mapping[str, Any]]) -> Path:
    """Write fleet records as deterministic NDJSON and return the path."""

    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [json.dumps(dict(record), sort_keys=True, separators=(",", ":")) for record in records]
    payload = "\n".join(lines)
    if lines:
        payload += "\n"
    path.write_text(payload, encoding="utf-8")
    return path


def default_fleet_artifact_path() -> Path:
    """Return the default NDJSON path used by the NL-to-SQL surface."""

    override = os.environ.get("PENSION_DATA_LANGSMITH_FLEET_PATH", "").strip()
    if override:
        return Path(override).expanduser()
    root = _project_root(Path(__file__).resolve())
    return root / "artifacts" / "langsmith" / ARTIFACT_NAME


def append_fleet_records(
    path: Path,
    records: Iterable[Mapping[str, Any]],
    *,
    retention_limit: int = 2_000,
) -> Path:
    """Append fleet records as NDJSON and enforce a bounded retention window.

    Each record is serialized with sorted keys and a trailing newline so that
    re-runs produce diff-stable output. Retention trimming mirrors the NL log
    so that dashboards do not accumulate unbounded history on long-lived
    runners.
    """

    if retention_limit < 1:
        raise ValueError("retention_limit must be >= 1")
    materialized = [dict(record) for record in records]
    if not materialized:
        return path
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+", encoding="utf-8") as handle:
        fcntl_module: ModuleType | None = None
        with suppress(ImportError):
            import fcntl as fcntl_module
        if fcntl_module is not None:
            with suppress(OSError):
                fcntl_module.flock(handle.fileno(), fcntl_module.LOCK_EX)
        for record in materialized:
            handle.write(json.dumps(record, sort_keys=True, separators=(",", ":")) + "\n")
        handle.flush()
        handle.seek(0)
        lines = handle.read().splitlines()
        if len(lines) > retention_limit:
            trimmed = lines[-retention_limit:]
            temp_path = path.with_suffix(path.suffix + ".tmp")
            temp_path.write_text("\n".join(trimmed) + "\n", encoding="utf-8")
            temp_path.replace(path)
        if fcntl_module is not None:
            with suppress(Exception):
                fcntl_module.flock(handle.fileno(), fcntl_module.LOCK_UN)
    return path


def _project_root(start: Path) -> Path:
    for candidate in (start, *start.parents):
        if (candidate / "pyproject.toml").exists():
            return candidate
    return Path.cwd()


def _record(
    *,
    context: FleetRunContext,
    operation: str,
    status: Status,
    recorded_at: str,
    domain: Mapping[str, Any],
    error_code: str | None,
    artifact_ref: str | None,
) -> dict[str, Any]:
    if operation not in _OPERATIONS:
        raise ValueError(f"unsupported operation: {operation}")
    record: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "repo": REPO,
        "surface": SURFACE,
        "operation": operation,
        "run_id": context.run_id,
        "status": status,
        "github_issue": GITHUB_ISSUE,
        "recorded_at": recorded_at,
        "domain": dict(domain),
    }
    if context.github_pr:
        record["github_pr"] = context.github_pr
    if context.provider:
        record["provider"] = context.provider
    if context.model:
        record["model"] = context.model
    if context.trace_id:
        record["trace_id"] = context.trace_id
    if context.trace_url:
        record["trace_url"] = context.trace_url
    if artifact_ref:
        record["artifact_ref"] = artifact_ref
    if error_code:
        record["error_category"] = error_code
    return record


def _safe_trace_payload(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, Mapping):
        return {}
    safe: dict[str, Any] = {}
    for key in ("request_id", "status", "row_count", "error_code"):
        value = payload.get(key)
        if value is not None:
            safe[key] = value
    return safe


def _stage_status(
    base: Status,
    *,
    error_code: str | None,
    stage: str,
    error_stage: str | None,
) -> Status:
    if not error_code:
        return base
    target = error_stage or _error_stage_for(error_code)
    if target == stage:
        return "error"
    stage_order = {"sql-generation": 0, "validation": 1, "execution": 2, "replay": 3}
    target_position = stage_order.get(target, -1) if target is not None else -1
    if stage_order.get(stage, 99) > target_position:
        return "skipped"
    return base


def _error_stage_for(error_code: str | None) -> str | None:
    if not error_code:
        return None
    if error_code == "AMBIGUOUS_PROMPT":
        return "sql-generation"
    if error_code in _VALIDATION_FAILURE_CODES:
        return "validation"
    if error_code in _EXECUTION_FAILURE_CODES:
        return "execution"
    return None


def _derive_sql_validation_status(error_code: str | None, response: Any) -> str:
    if error_code == "AMBIGUOUS_PROMPT":
        return "ambiguous"
    if error_code == "INVALID_REQUEST":
        return "invalid_request"
    if error_code == "UNSAFE_SQL":
        return "unsafe"
    if error_code in _EXECUTION_FAILURE_CODES:
        return "pass"
    if _response_status(response) == "ok":
        return "pass"
    return "unknown"


def _derive_read_only_status(error_code: str | None, response: Any) -> str:
    if error_code == "UNSAFE_SQL":
        return "blocked"
    if error_code in {"AMBIGUOUS_PROMPT", "INVALID_REQUEST"}:
        return "unknown"
    if _response_status(response) == "ok" or error_code in _EXECUTION_FAILURE_CODES:
        return "read_only"
    return "unknown"


def _response_status(response: Any) -> str:
    status = getattr(response, "status", None)
    return str(status) if isinstance(status, str) else ""


def _response_error_code(response: Any) -> str | None:
    error = getattr(response, "error", None)
    if error is None:
        return None
    code = getattr(error, "code", None)
    return str(code) if isinstance(code, str) and code else None


def _response_row_count(response: Any) -> int:
    metadata = getattr(response, "metadata", None)
    if metadata is None:
        return 0
    rows = getattr(metadata, "returned_rows", None)
    if rows is None:
        return 0
    try:
        return max(0, int(rows))
    except (TypeError, ValueError):
        return 0


def _response_trace_event_count(response: Any) -> int | None:
    metadata = getattr(response, "metadata", None)
    if metadata is None:
        return None
    count = getattr(metadata, "trace_event_count", None)
    if count is None:
        return None
    try:
        return max(0, int(count))
    except (TypeError, ValueError):
        return None


def _response_latency_ms(response: Any) -> int | None:
    metadata = getattr(response, "metadata", None)
    if metadata is None:
        return None
    value = getattr(metadata, "duration_ms", None)
    if value is None:
        return None
    try:
        return max(0, int(value))
    except (TypeError, ValueError):
        return None


def _request_max_rows(request: Any) -> int | None:
    if request is None:
        return None
    value = getattr(request, "max_rows", None)
    if value is None:
        return None
    try:
        return max(0, int(value))
    except (TypeError, ValueError):
        return None


def _derive_evidence_availability(response: Any) -> str:
    provenance = getattr(response, "provenance", None)
    if not isinstance(provenance, tuple):
        return "unknown"
    if not provenance:
        return "none"
    with_evidence = 0
    with_source = 0
    for row in provenance:
        evidence_refs = getattr(row, "evidence_refs", ())
        source_document_id = getattr(row, "source_document_id", None)
        if isinstance(evidence_refs, tuple) and evidence_refs:
            with_evidence += 1
        if isinstance(source_document_id, str) and source_document_id.strip():
            with_source += 1
    if with_evidence == len(provenance):
        return "all"
    if with_evidence > 0:
        return "partial"
    if with_source > 0:
        return "source_only"
    return "none"


def _utc_timestamp() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
