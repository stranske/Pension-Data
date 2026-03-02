"""Golden and regression tests for manager/fund lifecycle extraction."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from pension_data.extract.investment.lifecycle import (
    ExplicitLifecycleSignal,
    infer_lifecycle_events,
)
from pension_data.extract.investment.manager_positions import (
    ManagerFundDisclosureInput,
    build_manager_fund_positions,
)

FIXTURE_PATH = Path(__file__).parent / "fixtures" / "manager_lifecycle_golden.json"


def _load_fixture() -> dict[str, Any]:
    with FIXTURE_PATH.open(encoding="utf-8") as handle:
        return json.load(handle)


def _rows(payload_rows: list[dict[str, Any]]) -> list[ManagerFundDisclosureInput]:
    return [
        ManagerFundDisclosureInput(
            plan_id=row["plan_id"],
            plan_period=row["plan_period"],
            manager_name=row.get("manager_name"),
            fund_name=row.get("fund_name"),
            commitment=row.get("commitment"),
            unfunded=row.get("unfunded"),
            market_value=row.get("market_value"),
            explicit_not_disclosed=row.get("explicit_not_disclosed", False),
            known_not_invested=row.get("known_not_invested", False),
            confidence=row.get("confidence", 1.0),
            evidence_refs=tuple(row.get("evidence_refs", [])),
        )
        for row in payload_rows
    ]


def _event_snapshot(events: list[Any]) -> list[dict[str, str | None]]:
    return [
        {
            "plan_id": event.plan_id,
            "plan_period": event.plan_period,
            "manager_name": event.manager_name,
            "fund_name": event.fund_name,
            "event_type": event.event_type,
            "basis": event.basis,
        }
        for event in events
    ]


def test_golden_manager_lifecycle_scenarios() -> None:
    fixture = _load_fixture()
    for scenario in ("entry_exit_retained", "retained_only", "entry_only"):
        payload = fixture[scenario]
        previous_positions, previous_warnings = build_manager_fund_positions(
            _rows(payload["previous"])
        )
        current_positions, current_warnings = build_manager_fund_positions(_rows(payload["current"]))
        events, lifecycle_warnings = infer_lifecycle_events(previous_positions, current_positions)

        assert _event_snapshot(events) == payload["expected_events"]
        assert previous_warnings == []
        assert current_warnings == []
        assert lifecycle_warnings == []


def test_disclosure_warnings_distinguish_non_disclosure_partial_and_ambiguous() -> None:
    rows = [
        ManagerFundDisclosureInput(
            plan_id="plan-q",
            plan_period="2025",
            manager_name="",
            fund_name="",
            commitment=None,
            unfunded=None,
            market_value=None,
            explicit_not_disclosed=True,
            known_not_invested=False,
            confidence=0.7,
            evidence_refs=("p3",),
        ),
        ManagerFundDisclosureInput(
            plan_id="plan-q",
            plan_period="2025",
            manager_name="Known Not Invested",
            fund_name=None,
            commitment=None,
            unfunded=None,
            market_value=None,
            explicit_not_disclosed=True,
            known_not_invested=True,
            confidence=0.9,
            evidence_refs=("p4",),
        ),
        ManagerFundDisclosureInput(
            plan_id="plan-q",
            plan_period="2025",
            manager_name="Alpha   Capital",
            fund_name="Fund X",
            commitment=20.0,
            unfunded=None,
            market_value=18.0,
            confidence=0.8,
            evidence_refs=("p8",),
        ),
        ManagerFundDisclosureInput(
            plan_id="plan-q",
            plan_period="2025",
            manager_name="alpha capital",
            fund_name="fund x",
            commitment=20.0,
            unfunded=5.0,
            market_value=18.0,
            confidence=0.82,
            evidence_refs=("p9",),
        ),
    ]
    positions, warnings = build_manager_fund_positions(rows)
    warning_codes = [warning.code for warning in warnings]

    assert warning_codes.count("non_disclosure") == 1
    assert warning_codes.count("partial_disclosure") == 1
    assert warning_codes.count("ambiguous_naming") == 2
    assert any(
        position.completeness == "not_disclosed" and not position.known_not_invested
        for position in positions
    )
    assert any(
        position.completeness == "not_disclosed" and position.known_not_invested
        for position in positions
    )


def test_explicit_text_signal_overrides_inferred_basis() -> None:
    previous_positions, _ = build_manager_fund_positions(
        [
            ManagerFundDisclosureInput(
                plan_id="plan-r",
                plan_period="2024",
                manager_name="North Ridge",
                fund_name="Opportunities",
                commitment=44.0,
                unfunded=11.0,
                market_value=33.0,
                confidence=0.88,
                evidence_refs=("p1",),
            )
        ]
    )
    current_positions, _ = build_manager_fund_positions(
        [
            ManagerFundDisclosureInput(
                plan_id="plan-r",
                plan_period="2025",
                manager_name="North Ridge",
                fund_name="Opportunities",
                commitment=45.0,
                unfunded=9.0,
                market_value=36.0,
                confidence=0.91,
                evidence_refs=("p2",),
            )
        ]
    )
    explicit_signal = ExplicitLifecycleSignal(
        plan_id="plan-r",
        plan_period="2025",
        manager_name="North Ridge",
        fund_name="Opportunities",
        event_type="still_invested",
        confidence=0.99,
        evidence_refs=("text:p14",),
    )

    events, _ = infer_lifecycle_events(
        previous_positions,
        current_positions,
        explicit_signals=(explicit_signal,),
    )
    assert len(events) == 1
    assert events[0].basis == "explicit_text"
    assert events[0].confidence == 0.99
    assert events[0].evidence_refs == ("text:p14",)


def test_lifecycle_output_is_reproducible_on_rerun() -> None:
    payload = _load_fixture()["entry_exit_retained"]
    previous_positions, _ = build_manager_fund_positions(_rows(payload["previous"]))
    current_positions, _ = build_manager_fund_positions(_rows(payload["current"]))

    first_events, first_warnings = infer_lifecycle_events(previous_positions, current_positions)
    second_events, second_warnings = infer_lifecycle_events(previous_positions, current_positions)

    assert first_events == second_events
    assert first_warnings == second_warnings
