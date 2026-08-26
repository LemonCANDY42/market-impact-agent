from __future__ import annotations

import json
import math
import os
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import cast

from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.agent_runtime import (
    ModelProvider,
    ProviderPricing,
    RuntimeBudget,
    RuntimeConfig,
)
from market_impact_agent.minimax_provider import MiniMaxOpenAIProvider, MiniMaxProviderConfig

MODEL_PROVIDER_PROFILE_SCHEMA = "market-impact.model-provider-profile.v1"
ProviderBuilder = Callable[["ModelProviderProfile"], ModelProvider]


@dataclass(frozen=True, slots=True)
class ModelProviderProfile:
    profile_id: str
    adapter_kind: str
    provider_id: str
    origin: str
    api_path: str
    models_path: str
    model: str
    credential_env: str
    context_window_tokens: int
    reserved_output_tokens: int
    temperature: float
    top_p: float
    budget: RuntimeBudget
    pricing: ProviderPricing
    max_attempts: int
    retry_backoff_seconds: float

    def __post_init__(self) -> None:
        for name in (
            "adapter_kind",
            "provider_id",
            "origin",
            "api_path",
            "models_path",
            "model",
            "credential_env",
        ):
            _nonempty(cast(str, getattr(self, name)), name)
        if not self.api_path.startswith("/") or not self.models_path.startswith("/"):
            raise ValueError("Model Provider Profile API paths must be absolute paths")
        if self.context_window_tokens < 128:
            raise ValueError("Model Provider Profile context window is too small")
        if not 1 <= self.reserved_output_tokens < self.context_window_tokens:
            raise ValueError("reserved output tokens must fit the context window")
        if not 0 < self.temperature <= 1 or not 0 < self.top_p <= 1:
            raise ValueError("Model Provider Profile sampling values must be in (0, 1]")
        if not 1 <= self.max_attempts <= 3:
            raise ValueError("Model Provider Profile max_attempts must be between one and three")
        if (
            not math.isfinite(self.retry_backoff_seconds)
            or not 0 <= self.retry_backoff_seconds <= 5
        ):
            raise ValueError("Model Provider Profile retry backoff must be between zero and five")
        if self.profile_id != self.expected_profile_id:
            raise ValueError("Model Provider Profile profile_id does not match content")

    @property
    def profile_hash(self) -> str:
        return canonical_hash(self.core_dict())

    @property
    def expected_profile_id(self) -> str:
        return f"model-provider-{self.profile_hash}"

    def core_dict(self) -> dict[str, object]:
        return {
            "schema_version": MODEL_PROVIDER_PROFILE_SCHEMA,
            "adapter_kind": self.adapter_kind,
            "provider_id": self.provider_id,
            "origin": self.origin,
            "api_path": self.api_path,
            "models_path": self.models_path,
            "model": self.model,
            "credential_env": self.credential_env,
            "context_window_tokens": self.context_window_tokens,
            "reserved_output_tokens": self.reserved_output_tokens,
            "temperature": self.temperature,
            "top_p": self.top_p,
            "budget": self.budget.to_dict(),
            "pricing": self.pricing.to_dict(),
            "max_attempts": self.max_attempts,
            "retry_backoff_seconds": self.retry_backoff_seconds,
        }

    def to_dict(self) -> dict[str, object]:
        return {**self.core_dict(), "profile_id": self.profile_id}

    def runtime_config(self) -> RuntimeConfig:
        return RuntimeConfig(
            provider_id=self.provider_id,
            model=self.model,
            context_window_tokens=self.context_window_tokens,
            reserved_output_tokens=self.reserved_output_tokens,
            temperature=self.temperature,
            top_p=self.top_p,
            budget=self.budget,
            pricing=self.pricing,
        )


class ModelProviderFactory:
    def __init__(self) -> None:
        self._builders: dict[str, ProviderBuilder] = {}

    @classmethod
    def with_builtin_adapters(cls) -> ModelProviderFactory:
        factory = cls()
        factory.register("minimax-openai-compatible", _build_minimax)
        return factory

    def register(self, adapter_kind: str, builder: ProviderBuilder) -> None:
        _nonempty(adapter_kind, "adapter_kind")
        if adapter_kind in self._builders:
            raise ValueError(f"duplicate Model Provider adapter: {adapter_kind}")
        self._builders[adapter_kind] = builder

    def create(self, profile: ModelProviderProfile) -> ModelProvider:
        builder = self._builders.get(profile.adapter_kind)
        if builder is None:
            raise KeyError(f"unknown Model Provider adapter: {profile.adapter_kind}")
        provider = builder(profile)
        if provider.provider_id != profile.provider_id or provider.model != profile.model:
            raise ValueError("Model Provider adapter identity does not match its profile")
        return provider


def load_model_provider_profile(path: Path) -> ModelProviderProfile:
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise TypeError("Model Provider Profile must be an object")
    mapping = cast(dict[object, object], raw)
    if not all(isinstance(key, str) for key in mapping):
        raise TypeError("Model Provider Profile must have string keys")
    payload = cast(dict[str, object], mapping)
    expected = {
        "schema_version",
        "profile_id",
        "adapter_kind",
        "provider_id",
        "origin",
        "api_path",
        "models_path",
        "model",
        "credential_env",
        "context_window_tokens",
        "reserved_output_tokens",
        "temperature",
        "top_p",
        "budget",
        "pricing",
        "max_attempts",
        "retry_backoff_seconds",
    }
    if set(payload) != expected:
        raise ValueError("Model Provider Profile fields are invalid")
    if _string(payload, "schema_version") != MODEL_PROVIDER_PROFILE_SCHEMA:
        raise ValueError("unsupported Model Provider Profile schema_version")
    budget_raw = _object(payload.get("budget"), "Model Provider Profile budget")
    pricing_raw = _object(payload.get("pricing"), "Model Provider Profile pricing")
    budget_expected = {
        "max_turns",
        "max_tool_calls",
        "max_input_tokens",
        "max_output_tokens",
        "max_wall_seconds",
        "max_result_bytes",
        "max_estimated_cost_microusd",
    }
    if set(budget_raw) != budget_expected:
        raise ValueError("Model Provider Profile budget fields are invalid")
    if set(pricing_raw) != {
        "pricing_id",
        "input_microusd_per_million_tokens",
        "output_microusd_per_million_tokens",
    }:
        raise ValueError("Model Provider Profile pricing fields are invalid")
    result = ModelProviderProfile(
        profile_id=_string(payload, "profile_id"),
        adapter_kind=_string(payload, "adapter_kind"),
        provider_id=_string(payload, "provider_id"),
        origin=_string(payload, "origin"),
        api_path=_string(payload, "api_path"),
        models_path=_string(payload, "models_path"),
        model=_string(payload, "model"),
        credential_env=_string(payload, "credential_env"),
        context_window_tokens=_integer(payload, "context_window_tokens"),
        reserved_output_tokens=_integer(payload, "reserved_output_tokens"),
        temperature=_number(payload, "temperature"),
        top_p=_number(payload, "top_p"),
        budget=RuntimeBudget(
            max_turns=_integer(budget_raw, "max_turns"),
            max_tool_calls=_integer(budget_raw, "max_tool_calls"),
            max_input_tokens=_integer(budget_raw, "max_input_tokens"),
            max_output_tokens=_integer(budget_raw, "max_output_tokens"),
            max_wall_seconds=_number(budget_raw, "max_wall_seconds"),
            max_result_bytes=_integer(budget_raw, "max_result_bytes"),
            max_estimated_cost_microusd=_integer(budget_raw, "max_estimated_cost_microusd"),
        ),
        pricing=ProviderPricing(
            pricing_id=_string(pricing_raw, "pricing_id"),
            input_microusd_per_million_tokens=_integer(
                pricing_raw, "input_microusd_per_million_tokens"
            ),
            output_microusd_per_million_tokens=_integer(
                pricing_raw, "output_microusd_per_million_tokens"
            ),
        ),
        max_attempts=_integer(payload, "max_attempts"),
        retry_backoff_seconds=_number(payload, "retry_backoff_seconds"),
    )
    if result.to_dict() != payload:
        raise ValueError("Model Provider Profile does not match canonical contract")
    return result


def default_model_provider_profile_path() -> Path:
    package_root = Path(__file__).resolve().parent
    installed = package_root / "builtin_provider_profiles" / "minimax-m3-research-v1.json"
    if installed.is_file():
        return installed
    return package_root.parents[1] / "examples/providers/minimax-m3-research-v1.json"


def _build_minimax(profile: ModelProviderProfile) -> ModelProvider:
    api_key = os.environ.get(profile.credential_env, "")
    if not api_key:
        raise ValueError(f"Model Provider credential is missing: {profile.credential_env}")
    for name, expected in (
        ("MINIMAX_BASE_URL", profile.origin),
        ("MINIMAX_MODEL", profile.model),
    ):
        configured = os.environ.get(name)
        if configured is not None and configured != expected:
            raise ValueError(f"{name} does not match the frozen Model Provider Profile")
    return MiniMaxOpenAIProvider(
        api_key=api_key,
        config=MiniMaxProviderConfig(
            base_url=profile.origin,
            model=profile.model,
            api_path=profile.api_path,
            models_path=profile.models_path,
            max_attempts=profile.max_attempts,
            retry_backoff_seconds=profile.retry_backoff_seconds,
        ),
    )


def _object(value: object, label: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise TypeError(f"{label} must be an object")
    mapping = cast(dict[object, object], value)
    if not all(isinstance(key, str) for key in mapping):
        raise TypeError(f"{label} must have string keys")
    return cast(dict[str, object], mapping)


def _string(payload: dict[str, object], name: str) -> str:
    value = payload.get(name)
    if not isinstance(value, str) or not value or value != value.strip():
        raise TypeError(f"{name} must be a non-empty trimmed string")
    return value


def _integer(payload: dict[str, object], name: str) -> int:
    value = payload.get(name)
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{name} must be an integer")
    return value


def _number(payload: dict[str, object], name: str) -> float:
    value = payload.get(name)
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise TypeError(f"{name} must be a number")
    return float(value)


def _nonempty(value: str, label: str) -> None:
    if not value or value != value.strip():
        raise ValueError(f"{label} must be a non-empty trimmed string")
