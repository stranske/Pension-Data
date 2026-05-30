"""Serializable query run records and local artifact persistence."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, fields, is_dataclass
from pathlib import Path
from typing import Any, Literal, cast

RunSurface = Literal["sql", "nl"]
RunStatus = Literal["ok", "error"]


@dataclass(frozen=True, slots=True)
class QueryRunArtifact:
    """Pointer to a named artifact produced by a query run."""

    name: str
    path: str
    content_type: str
    row_count: int | None = None


@dataclass(frozen=True, slots=True)
class QueryRunActor:
    """Authenticated caller and required authorization scope."""

    key_id: str
    scopes: tuple[str, ...]
    required_scope: str
    correlation_id: str | None = None


@dataclass(frozen=True, slots=True)
class QueryRunRecord:
    """Unified replayable run record for SQL and NL query surfaces."""

    run_id: str
    surface: RunSurface
    status: RunStatus
    who: QueryRunActor
    inputs: Mapping[str, Any]
    generated_sql: str | None
    executed_sql: str | None
    columns: tuple[str, ...]
    row_count: int
    rows_artifact: QueryRunArtifact | None
    provenance: tuple[Mapping[str, Any], ...]
    warnings: tuple[str, ...]
    error: Mapping[str, Any] | None
    duration_ms: int
    cost: Mapping[str, Any] | None
    artifacts: tuple[QueryRunArtifact, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        """Return a deterministic JSON-serializable representation."""
        payload = _jsonable(self)
        assert isinstance(payload, dict)
        return cast(dict[str, Any], payload)


def default_run_record_root() -> Path:
    """Return the default local artifacts root for query run records."""
    return _project_root(Path(__file__).resolve()) / "artifacts"


def record_relative_path(path: Path, *, root: Path) -> str:
    """Return a stable POSIX path relative to the artifact root when possible."""
    try:
        return path.relative_to(root).as_posix()
    except ValueError:
        return path.as_posix()


def write_query_run_record(
    *,
    root: Path,
    surface: RunSurface,
    run_id: str,
    record: QueryRunRecord,
    rows: Sequence[Sequence[Any]],
) -> QueryRunArtifact:
    """Persist rows plus the query run record under deterministic artifact names."""
    safe_run_id = _safe_run_id(run_id)
    rows_path = root / _surface_dir(surface) / "rows" / f"{safe_run_id}.json"
    record_path = root / _surface_dir(surface) / "runs" / f"{safe_run_id}.json"
    _write_json(rows_path, {"columns": list(record.columns), "rows": _jsonable(tuple(rows))})
    _write_json(record_path, record.to_dict())
    return QueryRunArtifact(
        name=f"{surface}-query-run-record",
        path=record_relative_path(record_path, root=root),
        content_type="application/json",
        row_count=None,
    )


def load_rows_artifact(
    *, root: Path, artifact: QueryRunArtifact | Mapping[str, Any]
) -> dict[str, Any]:
    """Load a rows artifact produced by ``write_query_run_record``."""
    raw_path = artifact.path if isinstance(artifact, QueryRunArtifact) else str(artifact["path"])
    path = Path(raw_path)
    if not path.is_absolute():
        path = root / path
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("rows artifact must contain a JSON object")
    return cast(dict[str, Any], payload)


def _jsonable(value: object) -> object:
    if is_dataclass(value):
        return {field.name: _jsonable(getattr(value, field.name)) for field in fields(value)}
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_jsonable(item) for item in value]
    if isinstance(value, list):
        return [_jsonable(item) for item in value]
    return value


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _project_root(start: Path) -> Path:
    for candidate in (start, *start.parents):
        if (candidate / "pyproject.toml").exists():
            return candidate
    return Path.cwd()


def _surface_dir(surface: RunSurface) -> Path:
    if surface == "nl":
        return Path("langchain") / "nl_runs"
    return Path("query") / "sql_runs"


def _safe_run_id(run_id: str) -> str:
    return "".join(char if char.isalnum() or char in {"-", "_"} else "-" for char in run_id)
