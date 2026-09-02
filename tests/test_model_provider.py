import json
from pathlib import Path

import pytest

from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.agent_runtime import ModelTurn
from market_impact_agent.agent_schema import validate_agent_contract
from market_impact_agent.cliproxy_provider import CLIProxyLunaProvider
from market_impact_agent.model_provider import (
    ModelProviderFactory,
    load_builtin_model_provider_profile,
    load_model_provider_profile,
    model_provider_profile_from_dict,
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


def test_received_408_policy_has_new_profile_and_runtime_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    old = load_builtin_model_provider_profile("cliproxyapi-luna-max-cpa-v1")
    new = load_builtin_model_provider_profile("cliproxyapi-luna-max-cpa-retry408-v1")
    assert (
        old.profile_id
        == "model-provider-58529b65dc416787ab456b644ec220a57de008fee6e669f6a9263fe96c1a9441"
    )
    assert "retry_received_408_once" not in old.to_dict()
    assert "provider_profile_hash" not in old.runtime_config().to_dict()
    assert new.retry_received_408_once is True
    assert new.profile_id != old.profile_id
    assert new.runtime_config().config_hash != old.runtime_config().config_hash
    assert new.runtime_config().provider_profile_hash == new.profile_hash
    assert new.budget == old.budget
    assert new.max_attempts == old.max_attempts == 2
    assert validate_agent_contract(new.to_dict(), "model-provider-profile.schema.json") == ()
    monkeypatch.setenv(new.credential_env, "synthetic-test-key")
    provider = ModelProviderFactory.with_builtin_adapters().create(new)
    assert isinstance(provider, CLIProxyLunaProvider)
    assert provider.config.retry_received_408_once is True
    bad = json.loads(json.dumps(new.to_dict()))
    bad["retry_received_408_once"] = "true"
    bad.pop("profile_id")
    bad["profile_id"] = "model-provider-" + canonical_hash(bad)
    with pytest.raises(TypeError, match="boolean"):
        model_provider_profile_from_dict(bad)


def test_reassessment_usd1_profile_changes_only_its_cost_cap() -> None:
    old = load_builtin_model_provider_profile("cliproxyapi-luna-max-cpa-retry408-v1")
    new = load_builtin_model_provider_profile("cliproxyapi-luna-max-cpa-reassessment-usd1-v1")
    assert old.profile_id == (
        "model-provider-720923464c56599263ec7f06beb1b353c26f41ed1c532b852f0e870ab2c414a8"
    )
    assert old.budget.max_estimated_cost_microusd == 300_000
    expected = old.core_dict()
    expected["budget"] = {
        **old.budget.to_dict(),
        "max_estimated_cost_microusd": 1_000_000,
    }
    assert new.core_dict() == expected
    assert new.profile_id != old.profile_id
    assert new.runtime_config().provider_profile_hash == new.profile_hash
    assert new.runtime_config().budget.max_estimated_cost_microusd == 1_000_000
    assert validate_agent_contract(new.to_dict(), "model-provider-profile.schema.json") == ()
