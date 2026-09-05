from __future__ import annotations

import json
import math
import os
import re
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import cast
from urllib.parse import urlsplit

from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.agent_runtime import (
    ModelProvider,
    ProviderPricing,
    RuntimeBudget,
    RuntimeConfig,
)

MODEL_PROVIDER_PROFILE_SCHEMA = "market-impact.model-provider-profile.v1"
PI_MODEL_PROVIDER_PROFILE_SCHEMA = "market-impact.model-provider-profile.v2"
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
    reasoning_effort: str | None
    budget: RuntimeBudget
    pricing: ProviderPricing
    max_attempts: int
    retry_backoff_seconds: float
    retry_received_408_once: bool = False
    runtime: dict[str, object] | None = None
    compaction_trigger_tokens: int | None = None

    def __post_init__(self) -> None:
        if not isinstance(cast(object, self.retry_received_408_once), bool):
            raise TypeError("retry_received_408_once must be boolean")
        if self.retry_received_408_once and self.adapter_kind not in {
            "cliproxyapi-openai-compatible",
            "pi-openai-responses",
        }:
            raise ValueError("received-408 regeneration is accepted only for the CPA adapter")
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
        if self.compaction_trigger_tokens is not None and (
            isinstance(self.compaction_trigger_tokens, bool)
            or not self.reserved_output_tokens
            <= self.context_window_tokens - self.compaction_trigger_tokens
            or self.compaction_trigger_tokens < 128
        ):
            raise ValueError(
                "compaction trigger must leave the reserved output inside the context window"
            )
        if not 0 < self.temperature <= 1 or not 0 < self.top_p <= 1:
            raise ValueError("Model Provider Profile sampling values must be in (0, 1]")
        if self.reasoning_effort is not None:
            _nonempty(self.reasoning_effort, "reasoning_effort")
        if not 1 <= self.max_attempts <= 3:
            raise ValueError("Model Provider Profile max_attempts must be between one and three")
        if (
            not math.isfinite(self.retry_backoff_seconds)
            or not 0 <= self.retry_backoff_seconds <= 5
        ):
            raise ValueError("Model Provider Profile retry backoff must be between zero and five")
        if self.runtime is not None:
            self._validate_pi_runtime()
        if self.profile_id != self.expected_profile_id:
            raise ValueError("Model Provider Profile profile_id does not match content")

    @property
    def profile_hash(self) -> str:
        return canonical_hash(self.core_dict())

    @property
    def expected_profile_id(self) -> str:
        return f"model-provider-{self.profile_hash}"

    def core_dict(self) -> dict[str, object]:
        payload: dict[str, object] = {
            "schema_version": (
                PI_MODEL_PROVIDER_PROFILE_SCHEMA
                if self.runtime is not None
                else MODEL_PROVIDER_PROFILE_SCHEMA
            ),
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
        if self.reasoning_effort is not None:
            payload["reasoning_effort"] = self.reasoning_effort
        if self.retry_received_408_once:
            payload["retry_received_408_once"] = True
        if self.runtime is not None:
            payload["runtime"] = self.runtime
        if self.compaction_trigger_tokens is not None:
            payload["compaction_trigger_tokens"] = self.compaction_trigger_tokens
        return payload

    @property
    def effective_compaction_trigger_tokens(self) -> int:
        return self.compaction_trigger_tokens or (
            self.context_window_tokens - self.reserved_output_tokens
        )

    @property
    def native_api(self) -> str:
        if self.runtime is None:
            raise ValueError("new model dispatch requires a pi v2 Profile")
        return cast(str, self.runtime["api"])

    @property
    def route_identity(self) -> str:
        """Protocol acceptance is independent of a task's smaller budget and tools."""
        return canonical_hash(
            {
                "provider": self.provider_id,
                "origin": self.origin,
                "api_path": self.api_path,
                "model": self.model,
                "effort": self.reasoning_effort,
                "temperature": self.temperature,
                "top_p": self.top_p,
                "runtime": self.runtime,
                "context_window_tokens": self.context_window_tokens,
                "compaction_trigger_tokens": self.effective_compaction_trigger_tokens,
            }
        )

    def _validate_pi_runtime(self) -> None:
        assert self.runtime is not None
        if set(self.runtime) != {
            "api",
            "supported_efforts",
            "request_options",
            "quota_model",
            "cache_namespace",
        }:
            raise ValueError("pi Profile runtime fields are invalid")
        api = self.runtime["api"]
        if api not in {"openai-responses", "openai-completions"}:
            raise ValueError("native API has no accepted public pi factory")
        if self.adapter_kind != f"pi-{api}":
            raise ValueError("Profile adapter must match its native API")
        if not self.api_path.endswith(
            "/responses" if api == "openai-responses" else "/chat/completions"
        ):
            raise ValueError("Profile endpoint does not match its native API")
        origin = urlsplit(self.origin)
        if (
            origin.scheme not in {"http", "https"}
            or not origin.hostname
            or origin.path
            or origin.query
            or origin.fragment
            or origin.username
            or origin.password
            or (
                origin.scheme == "http" and origin.hostname not in {"127.0.0.1", "localhost", "::1"}
            )
        ):
            raise ValueError("model origin must be exact HTTPS or local loopback HTTP")
        efforts = self.runtime["supported_efforts"]
        if not isinstance(efforts, list) or any(
            item not in {"minimal", "low", "medium", "high", "xhigh", "max"}
            for item in cast(list[object], efforts)
        ):
            raise ValueError("Profile effort capabilities are invalid")
        if self.reasoning_effort is not None and self.reasoning_effort not in efforts:
            raise ValueError("Profile does not support the requested effort")
        for name in ("quota_model", "cache_namespace"):
            _string(self.runtime, name)
        options = _object(self.runtime["request_options"], "pi request options")
        protected = {
            "model",
            "messages",
            "input",
            "instructions",
            "tools",
            "functions",
            "tool_choice",
            "function_call",
            "stream",
            "stream_options",
            "max_tokens",
            "max_output_tokens",
            "max_completion_tokens",
            "temperature",
            "top_p",
            "reasoning",
            "reasoning_effort",
            "api_key",
            "headers",
            "base_url",
            "session_id",
            "prompt_cache_key",
        }
        if options.keys() & protected:
            raise ValueError("Provider options cannot override Harness-owned request fields")
        if re.fullmatch(r"[A-Z][A-Z0-9_]*", self.credential_env) is None:
            raise ValueError("credential reference must be an environment variable name")

    def to_dict(self) -> dict[str, object]:
        return {**self.core_dict(), "profile_id": self.profile_id}

    def runtime_config(self) -> RuntimeConfig:
        profile_hash = self.profile_hash if self.retry_received_408_once else None
        if self.adapter_kind.startswith("pi-"):
            from market_impact_agent.pi_runtime import runtime_identity

            profile_hash = canonical_hash(
                {"profile": self.profile_hash, "runtime": runtime_identity()}
            )
        return RuntimeConfig(
            provider_id=self.provider_id,
            model=self.model,
            context_window_tokens=self.context_window_tokens,
            reserved_output_tokens=self.reserved_output_tokens,
            temperature=self.temperature,
            top_p=self.top_p,
            budget=self.budget,
            pricing=self.pricing,
            provider_profile_hash=profile_hash,
        )


class ModelProviderFactory:
    def __init__(self) -> None:
        self._builders: dict[str, ProviderBuilder] = {}

    @classmethod
    def with_builtin_adapters(cls) -> ModelProviderFactory:
        factory = cls()
        from market_impact_agent.pi_runtime import PiRuntimeProvider

        factory.register("pi-openai-responses", PiRuntimeProvider)
        factory.register("pi-openai-completions", PiRuntimeProvider)
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
    return model_provider_profile_from_dict(json.loads(path.read_text(encoding="utf-8")))


def model_provider_profile_from_dict(value: object) -> ModelProviderProfile:
    if not isinstance(value, dict):
        raise TypeError("Model Provider Profile must be an object")
    mapping = cast(dict[object, object], value)
    if not all(isinstance(key, str) for key in mapping):
        raise TypeError("Model Provider Profile must have string keys")
    payload = cast(dict[str, object], mapping)
    required = {
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
    allowed = required | {
        "reasoning_effort",
        "retry_received_408_once",
        "runtime",
        "compaction_trigger_tokens",
    }
    if not required <= set(payload) or not set(payload) <= allowed:
        raise ValueError("Model Provider Profile fields are invalid")
    schema = _string(payload, "schema_version")
    if schema not in {MODEL_PROVIDER_PROFILE_SCHEMA, PI_MODEL_PROVIDER_PROFILE_SCHEMA}:
        raise ValueError("unsupported Model Provider Profile schema_version")
    if (schema == PI_MODEL_PROVIDER_PROFILE_SCHEMA) != ("runtime" in payload):
        raise ValueError("pi Profile v2 requires explicit runtime capabilities")
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
        reasoning_effort=(
            None
            if payload.get("reasoning_effort") is None
            else _string(payload, "reasoning_effort")
        ),
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
        retry_received_408_once=cast(bool, payload.get("retry_received_408_once", False)),
        runtime=None if "runtime" not in payload else _object(payload["runtime"], "pi runtime"),
        compaction_trigger_tokens=(
            None
            if "compaction_trigger_tokens" not in payload
            else _integer(payload, "compaction_trigger_tokens")
        ),
    )
    if result.to_dict() != payload:
        raise ValueError("Model Provider Profile does not match canonical contract")
    return result


def default_model_provider_profile_path() -> Path:
    configured = os.environ.get("MARKET_IMPACT_MODEL_PROFILE")
    if configured and re.fullmatch(r"[a-z0-9][a-z0-9-]*", configured):
        return builtin_model_provider_profile_path(configured)
    return (
        Path(configured).expanduser().resolve()
        if configured
        else builtin_model_provider_profile_path("pi-minimax-m3-v2")
    )


def builtin_model_provider_profile_path(profile_alias: str) -> Path:
    if re.fullmatch(r"[a-z0-9][a-z0-9-]*", profile_alias) is None:
        raise ValueError("Model Provider Profile alias is invalid")
    package_root = Path(__file__).resolve().parent
    installed = package_root / "builtin_provider_profiles" / f"{profile_alias}.json"
    if installed.is_file():
        return installed
    source = package_root.parents[1] / "examples/providers" / f"{profile_alias}.json"
    if not source.is_file():
        raise FileNotFoundError(f"unknown Harness-bundled Model Provider Profile: {profile_alias}")
    return source


def load_builtin_model_provider_profile(profile_alias: str) -> ModelProviderProfile:
    return load_model_provider_profile(builtin_model_provider_profile_path(profile_alias))


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
