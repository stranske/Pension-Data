"""Tests for entity token normalization."""

from __future__ import annotations

from pension_data.normalize.entity_tokens import normalize_entity_token


def test_none_returns_empty_string() -> None:
    assert normalize_entity_token(None) == ""


def test_empty_string_returns_empty_string() -> None:
    assert normalize_entity_token("") == ""


def test_whitespace_only_returns_empty_string() -> None:
    assert normalize_entity_token("   ") == ""


def test_case_conversion_to_lowercase() -> None:
    assert normalize_entity_token("CalPERS") == "calpers"


def test_whitespace_normalization_collapses_runs() -> None:
    assert normalize_entity_token("  Some   Name  ") == "some name"


def test_non_alphanumeric_characters_are_collapsed() -> None:
    assert normalize_entity_token("Smith & Wesson, Inc.") == "smith wesson inc"


def test_mixed_punctuation_and_spaces() -> None:
    assert normalize_entity_token("J.P. Morgan---Chase & Co.") == "j p morgan chase co"


def test_numeric_tokens_are_preserved() -> None:
    assert normalize_entity_token("Fund 42 Alpha") == "fund 42 alpha"
