"""Tests for shared numeric parsing helpers."""

from __future__ import annotations

from pension_data.normalize.numeric_parsing import detect_money_scale, parse_numeric_token


def test_parse_numeric_token_skips_year_like_tokens_by_default() -> None:
    assert parse_numeric_token("FY 2024 funded ratio 77.5%") == 77.5


def test_parse_numeric_token_can_parse_year_like_tokens_when_requested() -> None:
    assert parse_numeric_token("FY 2024 funded ratio 77.5%", skip_year_like=False) == 2024.0


def test_parse_numeric_token_handles_commas() -> None:
    assert parse_numeric_token("assets 1,234.56 million") == 1234.56


def test_detect_money_scale_preserves_existing_keywords() -> None:
    assert detect_money_scale("assets in billions", fallback="million_usd") == "billion_usd"
    assert detect_money_scale("assets in bn", fallback="million_usd") == "billion_usd"
    assert detect_money_scale("assets in mm", fallback="thousand_usd") == "million_usd"
    assert detect_money_scale("assets in k", fallback="million_usd") == "thousand_usd"
    assert detect_money_scale("assets", fallback="million_usd") == "million_usd"


def test_detect_money_scale_handles_adjacent_abbreviations() -> None:
    assert detect_money_scale("$1bn", fallback="million_usd") == "billion_usd"
    assert detect_money_scale("$5mm", fallback="thousand_usd") == "million_usd"
    assert detect_money_scale("$7k", fallback="million_usd") == "thousand_usd"
