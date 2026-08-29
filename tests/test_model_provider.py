from pathlib import Path

import pytest

from market_impact_agent.agent_runtime import ModelTurn
from market_impact_agent.model_provider import (
    ModelProviderFactory,
    load_builtin_model_provider_profile,
    load_model_provider_profile,
)

PROFILE = Path("examples/providers/minimax-m3-research-v1.json")


class FixtureProvider:
    def __init__(self, provider_id: str, model: str) -> None:
        self._provider_id = provider_id
        self._model = model

    @property
    def provider_id(self) -> str:
        return self._provider_id

    @property
    def model(self) -> str:
        return self._model

    async def complete(
        self,
        *,
        messages: tuple[dict[str, object], ...],
        tools: tuple[dict[str, object], ...],
        temperature: float,
        top_p: float,
        max_output_tokens: int,
        timeout_seconds: float,
    ) -> ModelTurn:
        _ = (messages, tools, temperature, top_p, max_output_tokens, timeout_seconds)
        raise AssertionError("factory test provider must not be called")


def test_profile_is_content_identified_and_builds_the_runtime_cost_cap() -> None:
    profile = load_model_provider_profile(PROFILE)

    assert profile.profile_id == f"model-provider-{profile.profile_hash}"
    assert profile.credential_env == "MINIMAX_API_KEY"
    assert profile.runtime_config().budget.max_estimated_cost_microusd == 50_000

    bundled = load_builtin_model_provider_profile("cliproxyapi-luna-xhigh-v1")
    assert bundled.model == "gpt-5.6-luna"
    with pytest.raises(FileNotFoundError, match="unknown Harness-bundled"):
        load_builtin_model_provider_profile("not-registered")


def test_provider_factory_is_adapter_neutral_and_checks_identity() -> None:
    profile = load_model_provider_profile(PROFILE)
    factory = ModelProviderFactory()
    factory.register(
        profile.adapter_kind,
        lambda value: FixtureProvider(value.provider_id, value.model),
    )

    provider = factory.create(profile)

    assert provider.provider_id == profile.provider_id
    assert provider.model == profile.model
    mismatch = ModelProviderFactory()
    mismatch.register(
        profile.adapter_kind,
        lambda value: FixtureProvider("wrong-provider", value.model),
    )
    with pytest.raises(ValueError, match="identity"):
        mismatch.create(profile)
