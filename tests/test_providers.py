from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any, cast

import pytest

from market_impact_agent.domain import (
    OrderIntent,
    OrderKind,
    Side,
    TradingEnvironment,
)
from market_impact_agent.providers import (
    Capability,
    MockExecutionProvider,
    ProviderManifest,
    ProviderTransport,
    TrustTier,
)

NOW = datetime(2026, 8, 25, 12, tzinfo=UTC)


def test_mock_rejects_direct_raw_order_submission() -> None:
    provider = MockExecutionProvider()
    with pytest.raises(TypeError, match="harness-issued capability"):
        provider.submit(cast(Any, make_order()))


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("schema_version", "market-impact.provider-manifest.v999"),
        ("provider_id", 42),
        ("order_types", ["market", "stop"]),
        ("supports_streaming", "false"),
        ("enabled", 1),
    ],
)
def test_manifest_from_dict_rejects_schema_invalid_values(
    field: str,
    value: object,
) -> None:
    payload = MockExecutionProvider().manifest.to_dict()
    payload[field] = value

    with pytest.raises((TypeError, ValueError)):
        ProviderManifest.from_dict(payload)


def test_manifest_from_dict_rejects_unknown_fields() -> None:
    payload = MockExecutionProvider().manifest.to_dict()
    payload["credential"] = "do-not-accept"

    with pytest.raises(ValueError, match="unknown fields"):
        ProviderManifest.from_dict(payload)


def test_live_capability_requires_stream_reconciliation_and_live_trust() -> None:
    manifest = ProviderManifest(
        schema_version="market-impact.provider-manifest.v1",
        provider_id="unsafe-live",
        provider_version="0.1.0",
        transport=ProviderTransport.MCP,
        environments=frozenset({TradingEnvironment.LIVE}),
        declared_capabilities=frozenset({Capability.LIVE_EXECUTION}),
        verified_capabilities=frozenset({Capability.LIVE_EXECUTION}),
        markets=("US",),
        order_types=("market",),
        supports_streaming=False,
        supports_reconciliation=False,
        enabled=True,
        trust_tier=TrustTier.PAPER_VALIDATED,
    )
    assert manifest.validation_errors() == (
        "verified live_execution requires live_validated trust",
        "verified live_execution requires streaming order events",
        "verified live_execution requires reconciliation",
    )


def test_verified_paper_capability_requires_reconciliation() -> None:
    manifest = MockExecutionProvider().manifest
    unsafe = ProviderManifest(
        schema_version=manifest.schema_version,
        provider_id="paper-without-reconciliation",
        provider_version=manifest.provider_version,
        transport=manifest.transport,
        environments=manifest.environments,
        declared_capabilities=manifest.declared_capabilities,
        verified_capabilities=manifest.verified_capabilities,
        markets=manifest.markets,
        order_types=manifest.order_types,
        supports_streaming=manifest.supports_streaming,
        supports_reconciliation=False,
        enabled=True,
        trust_tier=manifest.trust_tier,
    )
    assert unsafe.validation_errors() == ("verified paper_execution requires reconciliation",)


def make_order(
    *,
    environment: TradingEnvironment = TradingEnvironment.PAPER,
    account_id: str = "paper-account",
    instrument_id: str = "TEST",
    quantity: Decimal = Decimal("10"),
) -> OrderIntent:
    return OrderIntent(
        client_order_id="order-1",
        signal_id="sig-1",
        account_id=account_id,
        environment=environment,
        instrument_id=instrument_id,
        side=Side.BUY,
        quantity=quantity,
        order_kind=OrderKind.MARKET,
        created_at=NOW,
        expires_at=NOW + timedelta(minutes=5),
    )
