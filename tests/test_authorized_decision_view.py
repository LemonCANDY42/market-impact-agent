from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest
from jsonschema import Draft202012Validator, ValidationError

from market_impact_agent.account_state import (
    CashBalance,
    PositionSnapshot,
    capture_account_state_snapshot,
)
from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.agent_runtime import (
    ToolAccessContext,
    ToolCall,
    ToolRegistry,
    ToolSideEffect,
)
from market_impact_agent.authorized_decision_view import (
    POSITION_SNAPSHOT_TOOL_CAPABILITY,
    AuthorizedDecisionView,
    authorized_decision_view_from_dict,
    build_position_snapshot_tool,
)
from market_impact_agent.domain import TradingEnvironment
from market_impact_agent.providers import (
    Capability,
    ProviderManifest,
    ProviderTransport,
    TrustTier,
)
from market_impact_agent.runtime_store import ArtifactStore


def _position_snapshot(*, account_reference: str = "fixture-paper-account") -> PositionSnapshot:
    at = datetime(2026, 9, 1, 8, tzinfo=UTC)
    provider = ProviderManifest(
        schema_version="market-impact.provider-manifest.v1",
        provider_id="fixture-account-read",
        provider_version="1.0.0",
        transport=ProviderTransport.NATIVE,
        environments=frozenset({TradingEnvironment.PAPER}),
        declared_capabilities=frozenset({Capability.ACCOUNT}),
        verified_capabilities=frozenset({Capability.ACCOUNT}),
        markets=("IBKR",),
        order_types=(),
        supports_streaming=False,
        supports_reconciliation=True,
        enabled=True,
        trust_tier=TrustTier.PAPER_VALIDATED,
    )
    account = capture_account_state_snapshot(
        provider=provider,
        account_reference=account_reference,
        account_reference_key=b"fixture-decision-view-key-material",
        environment=TradingEnvironment.PAPER,
        as_of=at,
        reconciled_at=at,
        reconciliation_reference="fixture-complete-read",
        cash=(CashBalance(currency="USD", available=Decimal("10"), settled=Decimal("10")),),
        positions=(),
        open_orders=(),
        recent_fills=(),
        recent_fills_since=at - timedelta(hours=8),
    )
    return account.project_positions(evaluated_at=at, max_age=timedelta(minutes=5))


def test_view_binds_exact_read_only_position_tool_and_never_grants_execution(
    tmp_path: Path,
) -> None:
    position = _position_snapshot()
    view = AuthorizedDecisionView.build(
        cutoff=position.as_of,
        frozen_at=position.as_of + timedelta(seconds=1),
        data_snapshot_ids=("data-snapshot-b", "data-snapshot-a", "data-snapshot-a"),
        decision_input_ids=("decision-input-1",),
        position_snapshot=position,
    )
    tool = build_position_snapshot_tool(view, position)

    assert view.execution_capability is False
    assert view.exposure_increase_ready is True
    assert view.data_snapshot_ids == ("data-snapshot-a", "data-snapshot-b")
    assert view.view_id == view.expected_view_id
    assert view.position_snapshot_tool_manifest_hash == tool.manifest_hash
    assert authorized_decision_view_from_dict(view.to_dict()) == view

    registry = ToolRegistry(ArtifactStore(tmp_path / "artifacts"))
    registry.register(tool)
    access = ToolAccessContext(
        allowed_capabilities=frozenset({POSITION_SNAPSHOT_TOOL_CAPABILITY}),
        allowed_side_effects=frozenset({ToolSideEffect.READ_ONLY}),
        allowed_tools=frozenset({tool.name}),
    )
    result = asyncio.run(
        registry.execute(
            ToolCall(call_id="read-position-1", name=tool.name, arguments={}),
            access=access,
        )
    )
    content = json.loads(result.model_content)
    assert content["result"]["snapshot_id"] == position.snapshot_id
    assert "fixture-paper-account" not in result.model_content


def test_view_rejects_future_account_state() -> None:
    position = _position_snapshot()
    with pytest.raises(ValueError, match="newer than the decision cutoff"):
        AuthorizedDecisionView.build(
            cutoff=position.as_of - timedelta(seconds=1),
            frozen_at=position.as_of,
            data_snapshot_ids=(),
            decision_input_ids=(),
            position_snapshot=position,
        )


def test_view_recomputes_freshness_at_its_own_cutoff() -> None:
    position = _position_snapshot()
    view = AuthorizedDecisionView.build(
        cutoff=position.as_of + timedelta(hours=1),
        frozen_at=position.as_of + timedelta(hours=1, seconds=1),
        data_snapshot_ids=(),
        decision_input_ids=(),
        position_snapshot=position,
    )

    assert view.risk_observation_ready is True
    assert view.exposure_increase_ready is False
    assert view.observation_gaps == ("stale",)


def test_view_identity_normalizes_equivalent_aware_instants_to_utc() -> None:
    position = _position_snapshot()
    utc_view = AuthorizedDecisionView.build(
        cutoff=position.as_of,
        frozen_at=position.as_of + timedelta(seconds=1),
        data_snapshot_ids=(),
        decision_input_ids=(),
        position_snapshot=position,
    )
    plus_eight = timezone(timedelta(hours=8))
    offset_view = AuthorizedDecisionView.build(
        cutoff=position.as_of.astimezone(plus_eight),
        frozen_at=(position.as_of + timedelta(seconds=1)).astimezone(plus_eight),
        data_snapshot_ids=(),
        decision_input_ids=(),
        position_snapshot=position,
    )

    assert offset_view == utc_view
    assert offset_view.to_dict()["cutoff"] == "2026-09-01T08:00:00Z"


def test_position_tool_can_only_be_minted_for_the_view_snapshot() -> None:
    position = _position_snapshot()
    view = AuthorizedDecisionView.build(
        cutoff=position.as_of,
        frozen_at=position.as_of + timedelta(seconds=1),
        data_snapshot_ids=(),
        decision_input_ids=(),
        position_snapshot=position,
    )
    other = _position_snapshot(account_reference="other-fixture-paper-account")

    with pytest.raises(ValueError, match="differs from its Authorized Decision View"):
        build_position_snapshot_tool(view, other)


def test_reopened_view_cannot_claim_exposure_readiness_with_a_gap() -> None:
    position = _position_snapshot()
    view = AuthorizedDecisionView.build(
        cutoff=position.as_of,
        frozen_at=position.as_of + timedelta(seconds=1),
        data_snapshot_ids=(),
        decision_input_ids=(),
        position_snapshot=position,
    )
    invalid_core = {
        **view.core_dict(),
        "observation_gaps": ["stale"],
        "exposure_increase_ready": True,
    }
    payload = {
        **invalid_core,
        "view_id": "authorized-decision-view-" + canonical_hash(invalid_core),
    }
    schema = json.loads(
        (Path(__file__).parents[1] / "schemas" / "authorized-decision-view.schema.json").read_text(
            encoding="utf-8"
        )
    )

    validator: Any = Draft202012Validator(schema)
    with pytest.raises(ValidationError):
        validator.validate(payload)
    with pytest.raises(ValueError, match="gap-free"):
        authorized_decision_view_from_dict(payload)
