from __future__ import annotations

from market_impact_agent.providers import Capability, ExecutionProvider, ProviderManifest


class ProviderRegistry:
    def __init__(self) -> None:
        self._providers: dict[str, ExecutionProvider] = {}

    def register(self, provider: ExecutionProvider) -> None:
        manifest = provider.manifest
        manifest.assert_valid()
        if manifest.provider_id in self._providers:
            raise ValueError(f"provider already registered: {manifest.provider_id}")
        self._providers[manifest.provider_id] = provider

    def manifests(self) -> tuple[ProviderManifest, ...]:
        return tuple(
            self._providers[provider_id].manifest for provider_id in sorted(self._providers)
        )

    def providers_for(self, capability: Capability) -> tuple[ExecutionProvider, ...]:
        return tuple(
            provider
            for provider in self._providers.values()
            if provider.manifest.enabled and capability in provider.manifest.verified_capabilities
        )
