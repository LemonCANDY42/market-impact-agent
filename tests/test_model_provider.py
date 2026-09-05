from copy import deepcopy
from typing import cast

import pytest

from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.agent_schema import validate_agent_contract
from market_impact_agent.model_provider import (
    ModelProviderFactory,
    load_builtin_model_provider_profile,
    model_provider_profile_from_dict,
)
from market_impact_agent.pi_runtime import PiRuntimeProvider


def identified(value: dict[str, object]) -> dict[str, object]:
    value.pop("profile_id", None)
    return {**value, "profile_id": "model-provider-" + canonical_hash(value)}


@pytest.mark.parametrize(
    "alias",
    [
        "pi-cpa-luna-max-v2",
        "pi-cpa-terra-high-v2",
        "pi-cpa-sol-high-v2",
        "pi-minimax-m3-v2",
    ],
)
def test_v2_profile_and_factory_bind_native_route(alias: str, monkeypatch: pytest.MonkeyPatch):
    profile = load_builtin_model_provider_profile(alias)
    assert validate_agent_contract(profile.to_dict(), "model-provider-profile.schema.json") == ()
    monkeypatch.setenv(profile.credential_env, "synthetic-fixture-key")
    provider = ModelProviderFactory.with_builtin_adapters().create(profile)
    assert isinstance(provider, PiRuntimeProvider)
    assert provider.model == profile.model
    assert profile.runtime_config().provider_profile_hash is not None
    assert not hasattr(provider, "complete")


def test_compatible_model_registration_needs_no_financial_core_change():
    profile = load_builtin_model_provider_profile("pi-cpa-luna-max-v2")
    data = deepcopy(profile.to_dict())
    data["model"] = "test-compatible-model"
    runtime = cast(dict[str, object], data["runtime"])
    runtime["quota_model"] = "test-compatible-model"
    new = model_provider_profile_from_dict(identified(data))
    provider = ModelProviderFactory.with_builtin_adapters().create(new)
    assert provider.model == "test-compatible-model"
    assert new.route_identity != profile.route_identity
    # Construction is not acceptance and does not dispatch a request.
    assert new.native_api == profile.native_api


def test_smaller_budget_does_not_revoke_protocol_acceptance():
    profile = load_builtin_model_provider_profile("pi-cpa-luna-max-v2")
    data = deepcopy(profile.to_dict())
    budget = cast(dict[str, object], data["budget"])
    budget["max_estimated_cost_microusd"] = 10_000
    new = model_provider_profile_from_dict(identified(data))
    assert profile.route_identity == new.route_identity
    assert profile.profile_hash != new.profile_hash
    assert new.runtime_config().budget.max_estimated_cost_microusd == 10_000


def test_gpt_56_profile_freezes_recommended_compaction_headroom():
    profile = load_builtin_model_provider_profile("pi-cpa-luna-max-v2")
    assert profile.context_window_tokens == 272_000
    assert profile.effective_compaction_trigger_tokens == 258_000
    assert (
        profile.context_window_tokens
        - profile.effective_compaction_trigger_tokens
        - profile.reserved_output_tokens
        == 5_808
    )


@pytest.mark.parametrize(
    "option", ["model", "tools", "api_key", "max_output_tokens", "prompt_cache_key"]
)
def test_provider_options_cannot_override_harness_authority(option: str):
    data = deepcopy(load_builtin_model_provider_profile("pi-cpa-luna-max-v2").to_dict())
    runtime = cast(dict[str, object], data["runtime"])
    runtime["request_options"] = {option: "untrusted override"}
    with pytest.raises(ValueError):
        model_provider_profile_from_dict(identified(data))


def test_old_profile_is_decodable_but_has_no_new_network_path():
    old = load_builtin_model_provider_profile("cliproxyapi-luna-max-cpa-v1")
    assert (
        old.profile_id
        == "model-provider-58529b65dc416787ab456b644ec220a57de008fee6e669f6a9263fe96c1a9441"
    )
    with pytest.raises(KeyError, match="unknown Model Provider adapter"):
        ModelProviderFactory.with_builtin_adapters().create(old)
    with pytest.raises(ValueError, match="v2"):
        PiRuntimeProvider(old)
    assert PiRuntimeProvider(old, dispatch_allowed=False).profile == old
