"""Provider configuration and model factory helpers for LangChain runtimes."""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from importlib import import_module
from typing import Any, Literal, cast

LLMProvider = Literal["openai", "anthropic"]
_GLOBAL_API_KEY_ENV = "PENSION_DATA_LLM_API_KEY"


class MissingLLMDependencyError(RuntimeError):
    """Raised when optional LangChain provider dependencies are missing."""


class MissingLLMAPIKeyError(RuntimeError):
    """Raised when the configured provider API key is missing."""


@dataclass(frozen=True, slots=True)
class LLMProviderConfig:
    """Resolved LLM provider configuration for model construction."""

    provider: LLMProvider
    model: str
    temperature: float
    max_tokens: int | None
    api_key: str | None
    base_url: str | None
    api_key_env_var: str


def _env_lookup(env: Mapping[str, str], key: str) -> str | None:
    value = env.get(key)
    if value is None:
        return None
    token = value.strip()
    return token or None


def _provider_defaults(
    env: Mapping[str, str], provider: LLMProvider
) -> tuple[str, str, str | None]:
    if provider == "openai":
        return (
            _env_lookup(env, "PENSION_DATA_OPENAI_MODEL") or "gpt-4o-mini",
            "OPENAI_API_KEY",
            _env_lookup(env, "OPENAI_BASE_URL"),
        )
    return (
        _env_lookup(env, "PENSION_DATA_ANTHROPIC_MODEL") or "claude-3-5-sonnet-latest",
        "ANTHROPIC_API_KEY",
        _env_lookup(env, "ANTHROPIC_BASE_URL"),
    )


def resolve_provider_config(
    *,
    env: Mapping[str, str] | None = None,
    provider: LLMProvider | None = None,
    model: str | None = None,
    temperature: float = 0.0,
    max_tokens: int | None = None,
    api_key: str | None = None,
    base_url: str | None = None,
) -> LLMProviderConfig:
    """Resolve provider/model/key config from explicit args with env fallback."""
    active_env: Mapping[str, str] = os.environ if env is None else env
    raw_provider = provider or _env_lookup(active_env, "PENSION_DATA_LLM_PROVIDER") or "openai"
    if raw_provider not in {"openai", "anthropic"}:
        raise ValueError("provider must be 'openai' or 'anthropic'")
    resolved_provider = cast(LLMProvider, raw_provider)

    default_model, provider_key_name, default_base_url = _provider_defaults(
        active_env, resolved_provider
    )
    resolved_model = (model or default_model).strip()
    if not resolved_model:
        raise ValueError("model must be a non-empty string")
    if max_tokens is not None and max_tokens <= 0:
        raise ValueError("max_tokens must be > 0 when provided")

    resolved_api_key = (
        (api_key.strip() if api_key is not None else None)
        or _env_lookup(active_env, provider_key_name)
        or _env_lookup(active_env, _GLOBAL_API_KEY_ENV)
    )
    resolved_base_url = (base_url.strip() if base_url is not None else None) or default_base_url

    return LLMProviderConfig(
        provider=resolved_provider,
        model=resolved_model,
        temperature=temperature,
        max_tokens=max_tokens,
        api_key=resolved_api_key,
        base_url=resolved_base_url,
        api_key_env_var=provider_key_name,
    )


def _load_provider_class(module_name: str, class_name: str, package_hint: str) -> Any:
    try:
        module = import_module(module_name)
    except ModuleNotFoundError as exc:
        raise MissingLLMDependencyError(
            f"Missing optional dependency '{package_hint}'. Install with project extra: .[langchain]"
        ) from exc
    model_cls = getattr(module, class_name, None)
    if model_cls is None:
        raise MissingLLMDependencyError(
            f"Provider class '{class_name}' was not found in '{module_name}'. "
            "Reinstall the langchain optional dependencies."
        )
    return model_cls


def create_llm(config: LLMProviderConfig) -> Any:
    """Create provider-specific chat model with lazy imports and safe failures."""
    if config.api_key is None:
        raise MissingLLMAPIKeyError(
            f"Missing API key for provider '{config.provider}'. "
            f"Set {config.api_key_env_var} (or {_GLOBAL_API_KEY_ENV}) or pass api_key explicitly."
        )

    kwargs: dict[str, Any] = {
        "model": config.model,
        "api_key": config.api_key,
        "temperature": config.temperature,
    }
    if config.max_tokens is not None:
        kwargs["max_tokens"] = config.max_tokens
    if config.base_url is not None:
        kwargs["base_url"] = config.base_url

    if config.provider == "openai":
        model_cls = _load_provider_class("langchain_openai", "ChatOpenAI", "langchain-openai")
        return model_cls(**kwargs)

    model_cls = _load_provider_class("langchain_anthropic", "ChatAnthropic", "langchain-anthropic")
    return model_cls(**kwargs)
