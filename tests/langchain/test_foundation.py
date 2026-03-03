"""Tests for LangChain foundation provider, tracing, and prompt helpers."""

from __future__ import annotations

import importlib
import socket
import sys
from types import SimpleNamespace
from typing import cast

import pytest

from pension_data.langchain import foundation
from pension_data.langchain.foundation import (
    LLMProviderConfig,
    MissingLLMAPIKeyError,
    MissingLLMDependencyError,
    create_llm,
    resolve_provider_config,
)
from pension_data.langchain.prompts import (
    DEFAULT_ANALYTICS_DISCLAIMER,
    append_analytics_disclaimer,
    build_findings_explainer_prompt,
    build_nl_query_system_prompt,
)
from pension_data.langchain.tracing import configure_langsmith_env, langsmith_tracing_context


def test_langchain_modules_import_without_network_calls(monkeypatch: pytest.MonkeyPatch) -> None:
    call_count = 0

    def _deny_network(*args: object, **kwargs: object) -> socket.socket:
        nonlocal call_count
        del args, kwargs
        call_count += 1
        raise AssertionError("network call attempted during module import")

    monkeypatch.setattr(socket, "create_connection", _deny_network)
    for module_name in (
        "pension_data.langchain.foundation",
        "pension_data.langchain.prompts",
        "pension_data.langchain.tracing",
    ):
        sys.modules.pop(module_name, None)
        imported = importlib.import_module(module_name)
        assert imported.__name__ == module_name
    assert call_count == 0


def test_resolve_provider_config_uses_global_api_key_fallback() -> None:
    config = resolve_provider_config(
        env={
            "PENSION_DATA_LLM_PROVIDER": "anthropic",
            "PENSION_DATA_ANTHROPIC_MODEL": "claude-3-5-sonnet-latest",
            "PENSION_DATA_LLM_API_KEY": "fallback-key",
        }
    )
    assert config.provider == "anthropic"
    assert config.api_key == "fallback-key"
    assert config.api_key_env_var == "ANTHROPIC_API_KEY"


@pytest.mark.parametrize(
    ("provider", "expected_env_var"),
    [("openai", "OPENAI_API_KEY"), ("anthropic", "ANTHROPIC_API_KEY")],
)
def test_create_llm_missing_api_key_error_is_explicit(provider: str, expected_env_var: str) -> None:
    config = resolve_provider_config(env={}, provider=cast(foundation.LLMProvider, provider))
    with pytest.raises(MissingLLMAPIKeyError, match=expected_env_var):
        create_llm(config)


def test_create_llm_missing_dependency_error_is_explicit(monkeypatch: pytest.MonkeyPatch) -> None:
    config = LLMProviderConfig(
        provider="openai",
        model="gpt-4o-mini",
        temperature=0.0,
        max_tokens=None,
        api_key="test-key",
        base_url=None,
        api_key_env_var="OPENAI_API_KEY",
    )

    def _raise_module_not_found(module_name: str) -> object:
        raise ModuleNotFoundError(module_name)

    monkeypatch.setattr(foundation, "import_module", _raise_module_not_found)
    with pytest.raises(MissingLLMDependencyError, match="langchain-openai"):
        create_llm(config)


def test_create_llm_openai_instantiates_with_expected_kwargs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    class _FakeChatOpenAI:
        def __init__(self, **kwargs: object) -> None:
            captured.update(kwargs)

    monkeypatch.setattr(
        foundation,
        "import_module",
        lambda module_name: SimpleNamespace(ChatOpenAI=_FakeChatOpenAI),
    )

    config = LLMProviderConfig(
        provider="openai",
        model="gpt-4o-mini",
        temperature=0.25,
        max_tokens=512,
        api_key="test-key",
        base_url="https://api.openai.com/v1",
        api_key_env_var="OPENAI_API_KEY",
    )
    model = create_llm(config)
    assert isinstance(model, _FakeChatOpenAI)
    assert captured == {
        "model": "gpt-4o-mini",
        "api_key": "test-key",
        "temperature": 0.25,
        "max_tokens": 512,
        "base_url": "https://api.openai.com/v1",
    }


def test_configure_langsmith_env_gates_on_api_key_presence() -> None:
    disabled_env: dict[str, str] = {}
    assert configure_langsmith_env(disabled_env) is False
    assert "LANGCHAIN_TRACING_V2" not in disabled_env

    enabled_env = {"LANGSMITH_API_KEY": "test-langsmith-key"}
    assert configure_langsmith_env(enabled_env) is True
    assert enabled_env["LANGCHAIN_API_KEY"] == "test-langsmith-key"
    assert enabled_env["LANGCHAIN_TRACING_V2"] == "true"


def test_langsmith_tracing_context_is_noop_when_unconfigured() -> None:
    with langsmith_tracing_context(env={}) as run:
        assert run is None


def test_prompt_helpers_include_required_disclaimer() -> None:
    prompt = build_nl_query_system_prompt(
        schema_context="table pension_metrics(plan_id text, funded_ratio numeric)",
    )
    assert "SAFETY RULES" in prompt

    explanation_prompt = build_findings_explainer_prompt(
        findings_context="Funded ratio improved from 78% to 81% year-over-year.",
        user_question="What likely drove the change?",
    )
    assert DEFAULT_ANALYTICS_DISCLAIMER in explanation_prompt

    output_text = append_analytics_disclaimer("Net inflows rose in the latest period.")
    assert output_text.endswith(DEFAULT_ANALYTICS_DISCLAIMER)
    assert append_analytics_disclaimer(output_text) == output_text
