"""Tests for source authority tier constants and official-tier classification."""

from pension_data.sources.schema import (
    OFFICIAL_SOURCE_AUTHORITY_TIERS,
    SOURCE_AUTHORITY_TIERS,
    is_official_source_authority_tier,
)


def test_source_authority_tiers_include_expected_values() -> None:
    assert SOURCE_AUTHORITY_TIERS == (
        "official",
        "official-mirror",
        "high-confidence-third-party",
    )


def test_official_source_authority_tiers_include_expected_values() -> None:
    assert OFFICIAL_SOURCE_AUTHORITY_TIERS == (
        "official",
        "official-mirror",
    )


def test_is_official_source_authority_tier_classifies_tiers() -> None:
    assert is_official_source_authority_tier("official")
    assert is_official_source_authority_tier("official-mirror")
    assert not is_official_source_authority_tier("high-confidence-third-party")
    assert not is_official_source_authority_tier("unknown")
