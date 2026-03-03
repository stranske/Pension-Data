"""Fixture-driven entity regression harness for alias matching and lineage traversal."""

from __future__ import annotations

import json
import re
from collections import deque
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal, TypedDict

ENTITY_REGRESSION_ARTIFACT_TYPE = "pension_entity_regression"
SUPPORTED_ENTITY_REGRESSION_SCHEMA_VERSION = 1
DEFAULT_DETERMINISTIC_GENERATED_AT = datetime(1970, 1, 1, tzinfo=UTC)

AliasDecision = Literal["matched", "ambiguous_review", "no_match"]
ReviewState = Literal["auto_accept", "review_medium", "review_high"]
LineageEventType = Literal["rename", "merge", "split"]

MATCH_THRESHOLD = 0.82
AMBIGUOUS_THRESHOLD = 0.65
AMBIGUOUS_MARGIN = 0.05


@dataclass(frozen=True, slots=True)
class AliasCandidate:
    """Canonical candidate entity and supported alias variants."""

    entity_id: str
    aliases: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class AliasExpectation:
    """Expected alias regression outcome and queue routing state."""

    decision: AliasDecision
    entity_id: str | None
    review_state: ReviewState


@dataclass(frozen=True, slots=True)
class AliasCase:
    """Single alias matching regression case."""

    case_id: str
    query_alias: str
    candidates: tuple[AliasCandidate, ...]
    expected: AliasExpectation


@dataclass(frozen=True, slots=True)
class LineageEvent:
    """Directed lineage edge used for rename/merge/split traversal checks."""

    event_type: LineageEventType
    from_entity_id: str
    to_entity_id: str


@dataclass(frozen=True, slots=True)
class LineageExpectation:
    """Expected reachable and terminal node sets from one lineage root."""

    reachable_entities: tuple[str, ...]
    terminal_entities: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class LineageCase:
    """Single lineage traversal regression case."""

    case_id: str
    root_entity_id: str
    events: tuple[LineageEvent, ...]
    expected: LineageExpectation


@dataclass(frozen=True, slots=True)
class EntityRegressionFixture:
    """Complete entity regression fixture payload."""

    schema_version: int
    alias_cases: tuple[AliasCase, ...]
    lineage_cases: tuple[LineageCase, ...]


@dataclass(frozen=True, slots=True)
class AliasCaseResult:
    """Observed alias-matching outcome for one case."""

    case_id: str
    decision: AliasDecision
    entity_id: str | None
    review_state: ReviewState
    best_score: float
    second_best_score: float


@dataclass(frozen=True, slots=True)
class LineageCaseResult:
    """Observed lineage traversal outcome for one case."""

    case_id: str
    reachable_entities: tuple[str, ...]
    terminal_entities: tuple[str, ...]


class RegressionMismatch(TypedDict):
    """Field-level mismatch between expected and observed fixture outcomes."""

    suite: str
    case_id: str
    field: str
    expected: object
    observed: object


class EntityRegressionReport(TypedDict):
    """Machine-readable entity regression report."""

    artifact_type: str
    schema_version: int
    generated_at: str
    total_cases: int
    regressions: int
    alias_results: list[dict[str, object]]
    lineage_results: list[dict[str, object]]
    mismatches: list[RegressionMismatch]


def _normalize_alias(value: str) -> str:
    collapsed = re.sub(r"[^a-z0-9]+", " ", value.lower()).strip()
    return " ".join(collapsed.split())


def _alias_tokens(value: str) -> set[str]:
    normalized = _normalize_alias(value)
    return set(normalized.split()) if normalized else set()


def _alias_similarity(*, query_alias: str, candidate_alias: str) -> float:
    query_norm = _normalize_alias(query_alias)
    candidate_norm = _normalize_alias(candidate_alias)
    if not query_norm or not candidate_norm:
        return 0.0
    if query_norm == candidate_norm:
        return 1.0

    query_tokens = _alias_tokens(query_alias)
    candidate_tokens = _alias_tokens(candidate_alias)
    if not query_tokens or not candidate_tokens:
        return 0.0

    overlap = len(query_tokens & candidate_tokens)
    union = len(query_tokens | candidate_tokens)
    jaccard = overlap / union if union else 0.0
    containment_bonus = (
        0.15
        if query_tokens.issubset(candidate_tokens) or candidate_tokens.issubset(query_tokens)
        else 0.0
    )
    return min(0.99, round(jaccard + containment_bonus, 6))


def _candidate_score(*, query_alias: str, candidate: AliasCandidate) -> float:
    return max(
        (_alias_similarity(query_alias=query_alias, candidate_alias=alias) for alias in candidate.aliases),
        default=0.0,
    )


def evaluate_alias_case(case: AliasCase) -> AliasCaseResult:
    """Evaluate one alias case with deterministic tie-breaking and routing."""
    scored = sorted(
        (
            (_candidate_score(query_alias=case.query_alias, candidate=candidate), candidate.entity_id)
            for candidate in case.candidates
        ),
        key=lambda row: (row[0], row[1]),
        reverse=True,
    )
    best_score, best_entity_id = scored[0]
    second_best = scored[1][0] if len(scored) > 1 else 0.0

    if best_score < AMBIGUOUS_THRESHOLD:
        return AliasCaseResult(
            case_id=case.case_id,
            decision="no_match",
            entity_id=None,
            review_state="review_high",
            best_score=round(best_score, 6),
            second_best_score=round(second_best, 6),
        )

    if (
        second_best >= AMBIGUOUS_THRESHOLD
        and abs(best_score - second_best) <= AMBIGUOUS_MARGIN
        and best_score < 0.999999
    ):
        return AliasCaseResult(
            case_id=case.case_id,
            decision="ambiguous_review",
            entity_id=None,
            review_state="review_medium",
            best_score=round(best_score, 6),
            second_best_score=round(second_best, 6),
        )

    if best_score >= MATCH_THRESHOLD:
        return AliasCaseResult(
            case_id=case.case_id,
            decision="matched",
            entity_id=best_entity_id,
            review_state="auto_accept",
            best_score=round(best_score, 6),
            second_best_score=round(second_best, 6),
        )

    return AliasCaseResult(
        case_id=case.case_id,
        decision="ambiguous_review",
        entity_id=None,
        review_state="review_medium",
        best_score=round(best_score, 6),
        second_best_score=round(second_best, 6),
    )


def evaluate_alias_cases(cases: Iterable[AliasCase]) -> list[AliasCaseResult]:
    """Evaluate alias cases with deterministic ordering."""
    return [evaluate_alias_case(case) for case in sorted(cases, key=lambda item: item.case_id)]


def evaluate_lineage_case(case: LineageCase) -> LineageCaseResult:
    """Traverse lineage graph from one root and return reachable + terminal entities."""
    adjacency: dict[str, set[str]] = {}
    for event in case.events:
        adjacency.setdefault(event.from_entity_id, set()).add(event.to_entity_id)

    reachable: set[str] = {case.root_entity_id}
    queue: deque[str] = deque([case.root_entity_id])
    while queue:
        current = queue.popleft()
        for neighbor in sorted(adjacency.get(current, set())):
            if neighbor in reachable:
                continue
            reachable.add(neighbor)
            queue.append(neighbor)

    terminal_entities = sorted(
        entity
        for entity in reachable
        if not adjacency.get(entity)
    )
    return LineageCaseResult(
        case_id=case.case_id,
        reachable_entities=tuple(sorted(reachable)),
        terminal_entities=tuple(terminal_entities),
    )


def evaluate_lineage_cases(cases: Iterable[LineageCase]) -> list[LineageCaseResult]:
    """Evaluate lineage cases with deterministic ordering."""
    return [evaluate_lineage_case(case) for case in sorted(cases, key=lambda item: item.case_id)]


def _expect(value: object, *, location: str, expected_type: type[object]) -> object:
    if not isinstance(value, expected_type):
        raise ValueError(f"{location} must be a {expected_type.__name__}")
    return value


def _parse_alias_candidate(payload: object, *, location: str) -> AliasCandidate:
    if not isinstance(payload, dict):
        raise ValueError(f"{location} must be an object")
    entity_id = payload.get("entity_id")
    aliases_raw = payload.get("aliases")
    if not isinstance(entity_id, str) or not entity_id.strip():
        raise ValueError(f"{location}.entity_id must be a non-empty string")
    if not isinstance(aliases_raw, list) or not aliases_raw:
        raise ValueError(f"{location}.aliases must be a non-empty list")
    aliases: list[str] = []
    for index, item in enumerate(aliases_raw):
        if not isinstance(item, str) or not item.strip():
            raise ValueError(f"{location}.aliases[{index}] must be a non-empty string")
        aliases.append(item)
    return AliasCandidate(entity_id=entity_id, aliases=tuple(aliases))


def _parse_alias_expectation(payload: object, *, location: str) -> AliasExpectation:
    if not isinstance(payload, dict):
        raise ValueError(f"{location} must be an object")
    decision = payload.get("decision")
    entity_id = payload.get("entity_id")
    review_state = payload.get("review_state")
    if decision not in ("matched", "ambiguous_review", "no_match"):
        raise ValueError(f"{location}.decision must be one of matched/ambiguous_review/no_match")
    if entity_id is not None and (not isinstance(entity_id, str) or not entity_id.strip()):
        raise ValueError(f"{location}.entity_id must be null or a non-empty string")
    if review_state not in ("auto_accept", "review_medium", "review_high"):
        raise ValueError(
            f"{location}.review_state must be one of auto_accept/review_medium/review_high"
        )
    return AliasExpectation(
        decision=decision,
        entity_id=entity_id,
        review_state=review_state,
    )


def _parse_alias_case(payload: object, *, location: str) -> AliasCase:
    if not isinstance(payload, dict):
        raise ValueError(f"{location} must be an object")
    case_id = payload.get("case_id")
    query_alias = payload.get("query_alias")
    candidates_raw = payload.get("candidates")
    expected_raw = payload.get("expected")
    if not isinstance(case_id, str) or not case_id.strip():
        raise ValueError(f"{location}.case_id must be a non-empty string")
    if not isinstance(query_alias, str) or not query_alias.strip():
        raise ValueError(f"{location}.query_alias must be a non-empty string")
    if not isinstance(candidates_raw, list) or not candidates_raw:
        raise ValueError(f"{location}.candidates must be a non-empty list")
    candidates = tuple(
        _parse_alias_candidate(item, location=f"{location}.candidates[{index}]")
        for index, item in enumerate(candidates_raw)
    )
    expected = _parse_alias_expectation(expected_raw, location=f"{location}.expected")
    return AliasCase(case_id=case_id, query_alias=query_alias, candidates=candidates, expected=expected)


def _parse_lineage_event(payload: object, *, location: str) -> LineageEvent:
    if not isinstance(payload, dict):
        raise ValueError(f"{location} must be an object")
    event_type = payload.get("event_type")
    from_entity_id = payload.get("from_entity_id")
    to_entity_id = payload.get("to_entity_id")
    if event_type not in ("rename", "merge", "split"):
        raise ValueError(f"{location}.event_type must be one of rename/merge/split")
    if not isinstance(from_entity_id, str) or not from_entity_id.strip():
        raise ValueError(f"{location}.from_entity_id must be a non-empty string")
    if not isinstance(to_entity_id, str) or not to_entity_id.strip():
        raise ValueError(f"{location}.to_entity_id must be a non-empty string")
    return LineageEvent(
        event_type=event_type,
        from_entity_id=from_entity_id,
        to_entity_id=to_entity_id,
    )


def _parse_lineage_expectation(payload: object, *, location: str) -> LineageExpectation:
    if not isinstance(payload, dict):
        raise ValueError(f"{location} must be an object")
    reachable_raw = payload.get("reachable_entities")
    terminals_raw = payload.get("terminal_entities")
    if not isinstance(reachable_raw, list) or not reachable_raw:
        raise ValueError(f"{location}.reachable_entities must be a non-empty list")
    if not isinstance(terminals_raw, list) or not terminals_raw:
        raise ValueError(f"{location}.terminal_entities must be a non-empty list")

    reachable: list[str] = []
    for index, entity in enumerate(reachable_raw):
        if not isinstance(entity, str) or not entity.strip():
            raise ValueError(f"{location}.reachable_entities[{index}] must be a non-empty string")
        reachable.append(entity)

    terminals: list[str] = []
    for index, entity in enumerate(terminals_raw):
        if not isinstance(entity, str) or not entity.strip():
            raise ValueError(f"{location}.terminal_entities[{index}] must be a non-empty string")
        terminals.append(entity)

    return LineageExpectation(
        reachable_entities=tuple(reachable),
        terminal_entities=tuple(terminals),
    )


def _parse_lineage_case(payload: object, *, location: str) -> LineageCase:
    if not isinstance(payload, dict):
        raise ValueError(f"{location} must be an object")
    case_id = payload.get("case_id")
    root_entity_id = payload.get("root_entity_id")
    events_raw = payload.get("events")
    expected_raw = payload.get("expected")
    if not isinstance(case_id, str) or not case_id.strip():
        raise ValueError(f"{location}.case_id must be a non-empty string")
    if not isinstance(root_entity_id, str) or not root_entity_id.strip():
        raise ValueError(f"{location}.root_entity_id must be a non-empty string")
    if not isinstance(events_raw, list):
        raise ValueError(f"{location}.events must be a list")
    events = tuple(
        _parse_lineage_event(item, location=f"{location}.events[{index}]")
        for index, item in enumerate(events_raw)
    )
    expected = _parse_lineage_expectation(expected_raw, location=f"{location}.expected")
    return LineageCase(
        case_id=case_id,
        root_entity_id=root_entity_id,
        events=events,
        expected=expected,
    )


def load_fixture(path: Path) -> EntityRegressionFixture:
    """Load and validate entity regression fixture payload."""
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("entity regression fixture must be a JSON object")

    schema_version = payload.get("schema_version")
    if schema_version != SUPPORTED_ENTITY_REGRESSION_SCHEMA_VERSION:
        raise ValueError(
            "entity regression fixture schema_version must equal "
            f"{SUPPORTED_ENTITY_REGRESSION_SCHEMA_VERSION}"
        )
    alias_cases_raw = payload.get("alias_cases")
    lineage_cases_raw = payload.get("lineage_cases")
    if not isinstance(alias_cases_raw, list):
        raise ValueError("entity regression fixture alias_cases must be a list")
    if not isinstance(lineage_cases_raw, list):
        raise ValueError("entity regression fixture lineage_cases must be a list")

    alias_cases = tuple(
        _parse_alias_case(item, location=f"alias_cases[{index}]")
        for index, item in enumerate(alias_cases_raw)
    )
    lineage_cases = tuple(
        _parse_lineage_case(item, location=f"lineage_cases[{index}]")
        for index, item in enumerate(lineage_cases_raw)
    )
    return EntityRegressionFixture(
        schema_version=schema_version,
        alias_cases=alias_cases,
        lineage_cases=lineage_cases,
    )


def _alias_mismatches(*, fixture: EntityRegressionFixture, observed: list[AliasCaseResult]) -> list[RegressionMismatch]:
    observed_by_case_id = {row.case_id: row for row in observed}
    mismatches: list[RegressionMismatch] = []
    for case in fixture.alias_cases:
        row = observed_by_case_id[case.case_id]
        for field, expected, actual in (
            ("decision", case.expected.decision, row.decision),
            ("entity_id", case.expected.entity_id, row.entity_id),
            ("review_state", case.expected.review_state, row.review_state),
        ):
            if expected == actual:
                continue
            mismatches.append(
                {
                    "suite": "alias",
                    "case_id": case.case_id,
                    "field": field,
                    "expected": expected,
                    "observed": actual,
                }
            )
    return mismatches


def _lineage_mismatches(
    *,
    fixture: EntityRegressionFixture,
    observed: list[LineageCaseResult],
) -> list[RegressionMismatch]:
    observed_by_case_id = {row.case_id: row for row in observed}
    mismatches: list[RegressionMismatch] = []
    for case in fixture.lineage_cases:
        row = observed_by_case_id[case.case_id]
        for field, expected, actual in (
            ("reachable_entities", tuple(sorted(case.expected.reachable_entities)), row.reachable_entities),
            ("terminal_entities", tuple(sorted(case.expected.terminal_entities)), row.terminal_entities),
        ):
            if expected == actual:
                continue
            mismatches.append(
                {
                    "suite": "lineage",
                    "case_id": case.case_id,
                    "field": field,
                    "expected": list(expected),
                    "observed": list(actual),
                }
            )
    return mismatches


def run_entity_regression(
    fixture: EntityRegressionFixture,
    *,
    generated_at: datetime | None = None,
) -> EntityRegressionReport:
    """Evaluate fixture cases and return machine-readable regression report."""
    alias_results = evaluate_alias_cases(fixture.alias_cases)
    lineage_results = evaluate_lineage_cases(fixture.lineage_cases)
    mismatches = [
        *_alias_mismatches(fixture=fixture, observed=alias_results),
        *_lineage_mismatches(fixture=fixture, observed=lineage_results),
    ]
    timestamp = (generated_at or DEFAULT_DETERMINISTIC_GENERATED_AT).astimezone(UTC).isoformat()
    return {
        "artifact_type": ENTITY_REGRESSION_ARTIFACT_TYPE,
        "schema_version": SUPPORTED_ENTITY_REGRESSION_SCHEMA_VERSION,
        "generated_at": timestamp,
        "total_cases": len(fixture.alias_cases) + len(fixture.lineage_cases),
        "regressions": len(mismatches),
        "alias_results": [
            {
                "case_id": row.case_id,
                "decision": row.decision,
                "entity_id": row.entity_id,
                "review_state": row.review_state,
                "best_score": row.best_score,
                "second_best_score": row.second_best_score,
            }
            for row in alias_results
        ],
        "lineage_results": [
            {
                "case_id": row.case_id,
                "reachable_entities": list(row.reachable_entities),
                "terminal_entities": list(row.terminal_entities),
            }
            for row in lineage_results
        ],
        "mismatches": mismatches,
    }


def write_report(path: Path, report: EntityRegressionReport) -> None:
    """Persist regression report as deterministic JSON."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
