"""Deterministic LangChain regression evaluation harness for Pension-Data."""

from __future__ import annotations

import importlib
import json
import re
import shlex
import subprocess
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, cast

from pension_data.query.sql_safety import SQLSafetyValidationError, validate_read_only_sql

EvalMode = Literal["mock", "live"]
CaseFeature = Literal["nl_sql", "findings_explain", "findings_compare"]

_FROM_JOIN_PATTERN = re.compile(r"\b(?:from|join)\s+([a-z_][a-z0-9_\.]*)", re.IGNORECASE)


class DatasetValidationError(ValueError):
    """Raised when the evaluation dataset shape is invalid."""


@dataclass(frozen=True, slots=True)
class EvalThresholds:
    """Evaluation gate thresholds."""

    min_schema_validity_rate: float = 1.0
    min_citation_coverage_rate: float = 0.9
    min_no_hallucination_rate: float = 1.0
    min_safety_pass_rate: float = 1.0


@dataclass(frozen=True, slots=True)
class EvalCase:
    """One NL evaluation case entry."""

    case_id: str
    domain: str
    feature: CaseFeature
    question: str
    recorded_output: str | None
    expected_sql_contains: tuple[str, ...]
    expected_citations: tuple[str, ...]
    allowed_relations: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class EvalDataset:
    """Parsed evaluation dataset."""

    version: int
    thresholds: EvalThresholds
    cases: tuple[EvalCase, ...]
    dataset_path: Path


@dataclass(frozen=True, slots=True)
class CaseEvaluationResult:
    """Per-case evaluation result with diff details."""

    case_id: str
    domain: str
    feature: CaseFeature
    schema_valid: bool
    citation_coverage: float
    no_hallucination: bool
    safety_pass: bool
    pass_status: bool
    details: tuple[str, ...]

    def as_dict(self) -> dict[str, object]:
        return {
            "case_id": self.case_id,
            "domain": self.domain,
            "feature": self.feature,
            "schema_valid": self.schema_valid,
            "citation_coverage": round(self.citation_coverage, 4),
            "no_hallucination": self.no_hallucination,
            "safety_pass": self.safety_pass,
            "pass": self.pass_status,
            "details": list(self.details),
        }


@dataclass(frozen=True, slots=True)
class EvaluationReport:
    """Aggregate regression report."""

    mode: EvalMode
    dataset_path: Path
    case_results: tuple[CaseEvaluationResult, ...]
    schema_validity_rate: float
    citation_coverage_rate: float
    no_hallucination_rate: float
    safety_pass_rate: float
    status: Literal["pass", "fail"]
    failures: tuple[str, ...]

    def as_dict(self) -> dict[str, object]:
        return {
            "mode": self.mode,
            "dataset_path": str(self.dataset_path),
            "status": self.status,
            "metrics": {
                "schema_validity_rate": round(self.schema_validity_rate, 4),
                "citation_coverage_rate": round(self.citation_coverage_rate, 4),
                "no_hallucination_rate": round(self.no_hallucination_rate, 4),
                "safety_pass_rate": round(self.safety_pass_rate, 4),
            },
            "failures": list(self.failures),
            "cases": [case.as_dict() for case in self.case_results],
        }


def _normalize_string_tuple(value: object) -> tuple[str, ...]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        return ()
    normalized: list[str] = []
    for item in value:
        if not isinstance(item, str):
            continue
        token = item.strip()
        if token and token not in normalized:
            normalized.append(token)
    return tuple(normalized)


def _as_mapping(value: object) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise DatasetValidationError("dataset must decode to an object mapping")
    return value


def _load_yaml_or_json(path: Path) -> Mapping[str, object]:
    text = path.read_text(encoding="utf-8")
    yaml_module: Any | None = None
    yaml_error: Exception | None = None
    try:
        yaml_module = importlib.import_module("yaml")
    except ModuleNotFoundError:
        yaml_module = None

    if yaml_module is not None:
        try:
            payload = yaml_module.safe_load(text)
            return _as_mapping(payload)
        except Exception as exc:
            yaml_error = exc

    try:
        payload = json.loads(text)
    except json.JSONDecodeError as exc:
        message = "dataset file must be valid JSON"
        if yaml_error is not None:
            message = f"dataset file must be valid YAML or JSON (YAML parse error: {yaml_error})"
        raise DatasetValidationError(message) from exc
    return _as_mapping(payload)


def _parse_feature(value: object) -> CaseFeature:
    token = str(value).strip() if value is not None else ""
    if token in {"nl_sql", "findings_explain", "findings_compare"}:
        return cast(CaseFeature, token)
    raise DatasetValidationError(
        "case feature must be one of: nl_sql, findings_explain, findings_compare"
    )


def _parse_thresholds(payload: Mapping[str, object]) -> EvalThresholds:
    defaults = EvalThresholds()

    def _threshold(name: str, default: float) -> float:
        raw = payload.get(name, default)
        if isinstance(raw, bool) or not isinstance(raw, (int, float)):
            raise DatasetValidationError(f"threshold {name} must be numeric")
        value = float(raw)
        if value < 0.0 or value > 1.0:
            raise DatasetValidationError(f"threshold {name} must be between 0 and 1")
        return value

    return EvalThresholds(
        min_schema_validity_rate=_threshold(
            "min_schema_validity_rate", defaults.min_schema_validity_rate
        ),
        min_citation_coverage_rate=_threshold(
            "min_citation_coverage_rate", defaults.min_citation_coverage_rate
        ),
        min_no_hallucination_rate=_threshold(
            "min_no_hallucination_rate", defaults.min_no_hallucination_rate
        ),
        min_safety_pass_rate=_threshold("min_safety_pass_rate", defaults.min_safety_pass_rate),
    )


def load_eval_dataset(dataset_path: Path) -> EvalDataset:
    """Load dataset definition for mock/live regression runs."""
    payload = _load_yaml_or_json(dataset_path)
    version_raw = payload.get("version", 1)
    if isinstance(version_raw, bool) or not isinstance(version_raw, int):
        raise DatasetValidationError("dataset version must be an integer")
    thresholds_payload = _as_mapping(payload.get("thresholds", {}))
    thresholds = _parse_thresholds(thresholds_payload)
    cases_payload = payload.get("cases")
    if not isinstance(cases_payload, Sequence) or isinstance(
        cases_payload, (str, bytes, bytearray)
    ):
        raise DatasetValidationError("dataset cases must be a list")

    cases: list[EvalCase] = []
    for item in cases_payload:
        case_payload = _as_mapping(item)
        case_id = str(case_payload.get("id", "")).strip()
        domain = str(case_payload.get("domain", "")).strip()
        question = str(case_payload.get("question", "")).strip()
        if not case_id:
            raise DatasetValidationError("every case must include non-empty id")
        if not domain:
            raise DatasetValidationError(f"case {case_id} must include non-empty domain")
        if not question:
            raise DatasetValidationError(f"case {case_id} must include non-empty question")
        recorded_output_raw = case_payload.get("recorded_output")
        recorded_output = (
            str(recorded_output_raw).strip() if recorded_output_raw is not None else None
        )
        cases.append(
            EvalCase(
                case_id=case_id,
                domain=domain,
                feature=_parse_feature(case_payload.get("feature", "nl_sql")),
                question=question,
                recorded_output=recorded_output if recorded_output else None,
                expected_sql_contains=_normalize_string_tuple(
                    case_payload.get("expected_sql_contains")
                ),
                expected_citations=_normalize_string_tuple(case_payload.get("expected_citations")),
                allowed_relations=_normalize_string_tuple(case_payload.get("allowed_relations")),
            )
        )
    if not cases:
        raise DatasetValidationError("dataset must include at least one case")
    return EvalDataset(
        version=version_raw,
        thresholds=thresholds,
        cases=tuple(cases),
        dataset_path=dataset_path,
    )


def _extract_relations(sql: str) -> tuple[str, ...]:
    relations: list[str] = []
    for match in _FROM_JOIN_PATTERN.finditer(sql):
        token = match.group(1).split(".")[-1].lower().strip()
        if token and token not in relations:
            relations.append(token)
    return tuple(relations)


def _validate_schema(case: EvalCase, output: Mapping[str, object]) -> list[str]:
    details: list[str] = []
    if case.feature == "nl_sql":
        if not isinstance(output.get("sql"), str) or not str(output.get("sql", "")).strip():
            details.append("schema invalid: nl_sql output requires non-empty string field 'sql'")
    elif case.feature == "findings_explain":
        required: tuple[str, ...] = ("summary", "key_drivers", "caveats", "citations")
        for key in required:
            if key not in output:
                details.append(f"schema invalid: findings_explain output missing '{key}'")
    elif case.feature == "findings_compare":
        required = ("summary", "key_differences", "key_drivers", "caveats", "citations")
        for key in required:
            if key not in output:
                details.append(f"schema invalid: findings_compare output missing '{key}'")
    return details


def _citation_metrics(
    expected: tuple[str, ...], actual: tuple[str, ...]
) -> tuple[float, bool, tuple[str, ...]]:
    details: list[str] = []
    if not expected:
        coverage = 1.0
        hallucination_free = len(actual) == 0
        if actual:
            details.append("hallucinated citations detected where none were expected")
        return coverage, hallucination_free, tuple(details)

    expected_set = set(expected)
    actual_set = set(actual)
    matched = len(expected_set.intersection(actual_set))
    coverage = matched / len(expected_set)
    missing = sorted(expected_set - actual_set)
    extras = sorted(actual_set - expected_set)
    if missing:
        details.append("missing expected citations: " + ", ".join(missing))
    if extras:
        details.append("hallucinated citations: " + ", ".join(extras))
    return coverage, not extras, tuple(details)


def _load_mock_output(case: EvalCase, *, dataset_path: Path) -> Mapping[str, object]:
    if not case.recorded_output:
        raise DatasetValidationError(f"case {case.case_id} missing recorded_output for mock mode")
    path = (dataset_path.parent / case.recorded_output).resolve()
    if not path.exists():
        raise DatasetValidationError(
            f"recorded output not found for case {case.case_id}: {case.recorded_output}"
        )
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise DatasetValidationError(f"invalid JSON in recorded output {path}") from exc
    return _as_mapping(payload)


def _load_live_output(
    case: EvalCase,
    *,
    live_command: str,
    timeout_sec: int,
) -> Mapping[str, object]:
    input_payload = json.dumps(
        {
            "id": case.case_id,
            "domain": case.domain,
            "feature": case.feature,
            "question": case.question,
        }
    )
    command_args = shlex.split(live_command)
    if not command_args:
        raise RuntimeError(f"live command is empty for case {case.case_id}")
    completed = subprocess.run(  # noqa: S603
        command_args,
        input=input_payload,
        capture_output=True,
        text=True,
        timeout=timeout_sec,
        check=False,
    )
    if completed.returncode != 0:
        raise RuntimeError(
            f"live command failed for case {case.case_id}: {completed.stderr.strip() or 'exit != 0'}"
        )
    try:
        payload = json.loads(completed.stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"live command returned invalid JSON for case {case.case_id}") from exc
    return _as_mapping(payload)


def _evaluate_case(case: EvalCase, output: Mapping[str, object]) -> CaseEvaluationResult:
    details: list[str] = []
    schema_details = _validate_schema(case, output)
    details.extend(schema_details)
    schema_valid = not schema_details

    sql_value = str(output.get("sql", "")).strip()
    safety_pass = True
    if case.feature == "nl_sql" and sql_value:
        try:
            normalized = validate_read_only_sql(sql_value)
        except SQLSafetyValidationError as exc:
            safety_pass = False
            details.append(f"safety regression: {exc}")
            normalized = sql_value
        for required_token in case.expected_sql_contains:
            if required_token.lower() not in normalized.lower():
                details.append(f"diff: expected SQL token missing '{required_token}'")
        if case.allowed_relations:
            allowed = {token.lower() for token in case.allowed_relations}
            relations = _extract_relations(normalized)
            disallowed = sorted(token for token in relations if token not in allowed)
            if disallowed:
                safety_pass = False
                details.append("safety regression: disallowed relations " + ", ".join(disallowed))
    elif case.feature == "nl_sql":
        safety_pass = False
        details.append("safety regression: no SQL available for nl_sql case")

    actual_citations = _normalize_string_tuple(output.get("citations"))
    coverage, no_hallucination, citation_details = _citation_metrics(
        case.expected_citations,
        actual_citations,
    )
    details.extend(citation_details)

    passed = schema_valid and safety_pass and not details
    return CaseEvaluationResult(
        case_id=case.case_id,
        domain=case.domain,
        feature=case.feature,
        schema_valid=schema_valid,
        citation_coverage=coverage,
        no_hallucination=no_hallucination,
        safety_pass=safety_pass,
        pass_status=passed,
        details=tuple(details),
    )


def evaluate_dataset(
    dataset: EvalDataset,
    *,
    mode: EvalMode,
    live_command: str | None = None,
    live_timeout_sec: int = 45,
) -> EvaluationReport:
    """Evaluate all cases in mock or live mode and produce a gate report."""
    if mode == "live" and not live_command:
        raise ValueError("live_command is required in live mode")
    if live_timeout_sec < 1:
        raise ValueError("live_timeout_sec must be >= 1")

    case_results: list[CaseEvaluationResult] = []
    for case in dataset.cases:
        output = (
            _load_live_output(case, live_command=live_command or "", timeout_sec=live_timeout_sec)
            if mode == "live"
            else _load_mock_output(case, dataset_path=dataset.dataset_path)
        )
        case_results.append(_evaluate_case(case, output))

    total = len(case_results)
    schema_validity_rate = sum(1 for result in case_results if result.schema_valid) / total
    citation_coverage_rate = sum(result.citation_coverage for result in case_results) / total
    no_hallucination_rate = sum(1 for result in case_results if result.no_hallucination) / total
    safety_pass_rate = sum(1 for result in case_results if result.safety_pass) / total

    failures: list[str] = []
    thresholds = dataset.thresholds
    if schema_validity_rate < thresholds.min_schema_validity_rate:
        failures.append(
            "schema_validity_rate below threshold: "
            f"{schema_validity_rate:.4f} < {thresholds.min_schema_validity_rate:.4f}"
        )
    if citation_coverage_rate < thresholds.min_citation_coverage_rate:
        failures.append(
            "citation_coverage_rate below threshold: "
            f"{citation_coverage_rate:.4f} < {thresholds.min_citation_coverage_rate:.4f}"
        )
    if no_hallucination_rate < thresholds.min_no_hallucination_rate:
        failures.append(
            "no_hallucination_rate below threshold: "
            f"{no_hallucination_rate:.4f} < {thresholds.min_no_hallucination_rate:.4f}"
        )
    if safety_pass_rate < thresholds.min_safety_pass_rate:
        failures.append(
            "safety_pass_rate below threshold: "
            f"{safety_pass_rate:.4f} < {thresholds.min_safety_pass_rate:.4f}"
        )
    safety_regressions = [result.case_id for result in case_results if not result.safety_pass]
    if safety_regressions:
        failures.append("safety regressions detected in cases: " + ", ".join(safety_regressions))

    status: Literal["pass", "fail"] = "fail" if failures else "pass"
    return EvaluationReport(
        mode=mode,
        dataset_path=dataset.dataset_path,
        case_results=tuple(case_results),
        schema_validity_rate=schema_validity_rate,
        citation_coverage_rate=citation_coverage_rate,
        no_hallucination_rate=no_hallucination_rate,
        safety_pass_rate=safety_pass_rate,
        status=status,
        failures=tuple(failures),
    )
