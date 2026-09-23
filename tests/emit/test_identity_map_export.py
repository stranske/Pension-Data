"""Identity-map/v1 projection tests."""

from __future__ import annotations

import pytest

from pension_data.emit.identity_map import (
    _backplane_fund_id,
    build_identity_maps,
    identity_refs_from_maps,
)


def test_identity_maps_cover_pension_fund_and_manager_tokens() -> None:
    maps = build_identity_maps(
        pilot_input={"plan_id": "CA-PERS"},
        core_rows=[
            {"plan_id": "CA-PERS", "manager_name": "Acme Advisers", "fund_name": "Alpha Fund"}
        ],
        relationship_rows=[
            {"plan_id": "CA-PERS", "manager_name": "Beta Capital", "fund_name": "Beta Fund"}
        ],
        published_at="2026-01-01T00:00:00+00:00",
    )
    by_type = {identity_map["entity_type"]: identity_map for identity_map in maps}
    assert set(by_type) == {"pension", "manager", "fund"}
    assert identity_refs_from_maps(maps) == [
        "fund:acme_advisers:alpha_fund",
        "fund:beta_capital:beta_fund",
        "manager:acme_advisers",
        "manager:beta_capital",
        "pension:ca_pers",
    ]
    assert by_type["pension"]["entries"][0]["native_keys"] == {"plan_id": "CA-PERS"}
    assert by_type["manager"]["entries"][0]["confidence"] == 0.5


def test_same_fund_name_remains_scoped_to_each_manager() -> None:
    maps = build_identity_maps(
        pilot_input={"plan_id": "CA-PERS"},
        core_rows=[{"manager_name": "Acme Advisers", "fund_name": "Alpha Fund"}],
        relationship_rows=[{"manager_name": "Beta Capital", "fund_name": "Alpha Fund"}],
        published_at="2026-01-01T00:00:00+00:00",
    )
    fund_map = next(identity_map for identity_map in maps if identity_map["entity_type"] == "fund")

    assert fund_map["entries"] == [
        {
            "canonical_id": "fund:acme_advisers:alpha_fund",
            "aliases": ["alpha fund"],
            "native_keys": {"fund_name": "Alpha Fund", "manager_name": "Acme Advisers"},
            "supersedes_ids": [],
            "confidence": 0.5,
        },
        {
            "canonical_id": "fund:beta_capital:alpha_fund",
            "aliases": ["alpha fund"],
            "native_keys": {"fund_name": "Alpha Fund", "manager_name": "Beta Capital"},
            "supersedes_ids": [],
            "confidence": 0.5,
        },
    ]
    assert "fund:alpha_fund" not in identity_refs_from_maps(maps)


@pytest.mark.parametrize(
    "row",
    [
        {"fund_name": "Alpha Fund"},
        {"manager_name": None, "fund_name": "Alpha Fund"},
        {"manager_name": "", "fund_name": "Alpha Fund"},
        {"manager_name": "   ", "fund_name": "Alpha Fund"},
        {"manager_name": "[unknown_manager]", "fund_name": "Alpha Fund"},
        {"manager_name": "[not_disclosed]", "fund_name": "Alpha Fund"},
        {"manager_name": "unknown", "fund_name": "Alpha Fund"},
    ],
)
def test_missing_manager_retains_unscoped_fund_fallback(row: dict[str, object]) -> None:
    maps = build_identity_maps(
        pilot_input={"plan_id": "CA-PERS"},
        core_rows=[row],
        relationship_rows=[],
        published_at="2026-01-01T00:00:00+00:00",
    )
    fund_map = next(identity_map for identity_map in maps if identity_map["entity_type"] == "fund")

    assert fund_map["entries"][0]["canonical_id"] == "fund:alpha_fund"
    assert fund_map["entries"][0]["native_keys"] == {"fund_name": "Alpha Fund"}


def test_backplane_fund_id_adapts_component_id_without_changing_scope() -> None:
    assert (
        _backplane_fund_id(manager_name="Acme Advisers", fund_name="Alpha Fund")
        == "fund:acme_advisers:alpha_fund"
    )
    assert _backplane_fund_id(manager_name=None, fund_name="Alpha Fund") == "fund:alpha_fund"


def test_identity_maps_exclude_placeholders_and_absent_types() -> None:
    maps = build_identity_maps(
        pilot_input={"plan_id": "CA-PERS"},
        core_rows=[{"plan_id": "CA-PERS", "manager_name": "[unknown_manager]", "fund_name": None}],
        relationship_rows=[
            {
                "plan_id": "CA-PERS",
                "manager_name": "[not_disclosed]",
                "fund_name": "unknown fund",
            }
        ],
        published_at="2026-01-01T00:00:00+00:00",
    )
    assert [identity_map["entity_type"] for identity_map in maps] == ["pension"]
    assert identity_refs_from_maps(maps) == ["pension:ca_pers"]


def test_identity_map_is_deterministic_for_input_order() -> None:
    rows = [
        {"plan_id": "CA-PERS", "manager_name": "Acme Advisers", "fund_name": "Alpha Fund"},
        {"plan_id": "CA-PERS", "manager_name": "Beta Capital", "fund_name": "Beta Fund"},
    ]
    first = build_identity_maps(
        pilot_input={"plan_id": "CA-PERS"},
        core_rows=rows,
        relationship_rows=[],
        published_at="2026-01-01T00:00:00+00:00",
    )
    second = build_identity_maps(
        pilot_input={"plan_id": "CA-PERS"},
        core_rows=list(reversed(rows)),
        relationship_rows=[],
        published_at="2026-01-01T00:00:00+00:00",
    )
    assert first == second
