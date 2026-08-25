import pytest

from market_impact_agent.providers import Capability, MockExecutionProvider
from market_impact_agent.registry import ProviderRegistry


def test_registry_routes_only_verified_enabled_capabilities() -> None:
    registry = ProviderRegistry()
    provider = MockExecutionProvider()
    registry.register(provider)

    assert registry.providers_for(Capability.PAPER_EXECUTION) == (provider,)
    assert registry.providers_for(Capability.LIVE_EXECUTION) == ()


def test_registry_rejects_duplicate_provider_identity() -> None:
    registry = ProviderRegistry()
    registry.register(MockExecutionProvider())
    with pytest.raises(ValueError, match="already registered"):
        registry.register(MockExecutionProvider())
