"""Regression tests for extraction-to-persistence adapter helpers."""

from __future__ import annotations

from pension_data.extract.persistence import (
    NON_DISCLOSED_MANAGER_NAME,
    UNKNOWN_MANAGER_NAME,
    _metric_family_for_funded,
    _normalize_refs,
    _stable_id,
)

# ── _stable_id ──────────────────────────────────────────────────────


class TestStableId:
    def test_deterministic_same_inputs(self) -> None:
        id1 = _stable_id("fact", "CA-PERS", "FY2024", "funded_ratio")
        id2 = _stable_id("fact", "CA-PERS", "FY2024", "funded_ratio")
        assert id1 == id2

    def test_different_prefix_different_id(self) -> None:
        id1 = _stable_id("fact", "CA-PERS", "FY2024")
        id2 = _stable_id("rel", "CA-PERS", "FY2024")
        assert id1 != id2

    def test_different_parts_different_id(self) -> None:
        id1 = _stable_id("fact", "CA-PERS", "FY2024")
        id2 = _stable_id("fact", "CA-PERS", "FY2025")
        assert id1 != id2

    def test_prefix_format(self) -> None:
        result = _stable_id("fact", "something")
        assert result.startswith("fact:")
        # SHA256 hex truncated to 20 chars
        assert len(result.split(":")[1]) == 20

    def test_none_part_handled(self) -> None:
        # None values should produce consistent IDs via json.dumps
        id1 = _stable_id("fact", None, "FY2024")
        id2 = _stable_id("fact", None, "FY2024")
        assert id1 == id2

    def test_order_sensitivity(self) -> None:
        id1 = _stable_id("fact", "A", "B")
        id2 = _stable_id("fact", "B", "A")
        assert id1 != id2


# ── _normalize_refs ─────────────────────────────────────────────────


class TestNormalizeRefs:
    def test_deduplicates(self) -> None:
        result = _normalize_refs(["p.1", "p.2", "p.1"])
        assert result == ("p.1", "p.2")

    def test_strips_whitespace(self) -> None:
        result = _normalize_refs(["  p.1 ", " p.2  "])
        assert result == ("p.1", "p.2")

    def test_drops_empty_strings(self) -> None:
        result = _normalize_refs(["p.1", "", "  ", "p.2"])
        assert result == ("p.1", "p.2")

    def test_preserves_order(self) -> None:
        result = _normalize_refs(["p.3", "p.1", "p.2"])
        assert result == ("p.3", "p.1", "p.2")

    def test_empty_input(self) -> None:
        result = _normalize_refs([])
        assert result == ()


# ── _metric_family_for_funded ───────────────────────────────────────


class TestMetricFamilyForFunded:
    def test_funded_ratio_is_funded(self) -> None:
        assert _metric_family_for_funded("funded_ratio") == "funded"

    def test_aal_usd_is_funded(self) -> None:
        assert _metric_family_for_funded("aal_usd") == "funded"

    def test_ava_usd_is_funded(self) -> None:
        assert _metric_family_for_funded("ava_usd") == "funded"

    def test_other_metric_is_actuarial(self) -> None:
        assert _metric_family_for_funded("uaal_usd") == "actuarial"

    def test_empty_string_is_actuarial(self) -> None:
        assert _metric_family_for_funded("") == "actuarial"

    def test_similar_name_not_funded(self) -> None:
        assert _metric_family_for_funded("funded_ratio_change") == "actuarial"


# ── _manager_name_for_relationship ──────────────────────────────────


class TestManagerNameForRelationship:
    """Test the manager name fallback logic via the exported constants."""

    def test_non_disclosed_constant(self) -> None:
        assert NON_DISCLOSED_MANAGER_NAME == "[not_disclosed]"

    def test_unknown_constant(self) -> None:
        assert UNKNOWN_MANAGER_NAME == "[unknown_manager]"

    def test_constants_are_distinct(self) -> None:
        assert NON_DISCLOSED_MANAGER_NAME != UNKNOWN_MANAGER_NAME
