from __future__ import annotations

import json
import os
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, Protocol, cast

import pytest
from jsonschema import Draft202012Validator, FormatChecker, ValidationError
from referencing import Registry
from referencing.jsonschema import DRAFT202012, Schema

from market_impact_agent.account_state import (
    AccountPosition,
    AccountStateSection,
    AccountStateSnapshot,
    CashBalance,
    OpenOrder,
    OpenOrderStatus,
    RecentFill,
    account_state_snapshot_from_dict,
    capture_account_state_snapshot,
    load_or_create_account_reference_key,
    opaque_account_reference_hash,
    position_snapshot_from_dict,
)
from market_impact_agent.domain import Side, TradingEnvironment
from market_impact_agent.providers import (
    Capability,
    MockExecutionProvider,
    ProviderManifest,
    ProviderTransport,
    TrustTier,
)

ROOT = Path(__file__).parents[1]
AS_OF = datetime(2026, 9, 1, 9, tzinfo=UTC)
ACCOUNT_REFERENCE_KEY = b"fixture-account-reference-key-32-bytes-minimum"


class Validator(Protocol):
    def validate(self, instance: object) -> None: ...


def _account_provider() -> ProviderManifest:
    return ProviderManifest(
        schema_version="market-impact.provider-manifest.v1",
        provider_id="fixture-account-read",
        provider_version="1.0.0",
        transport=ProviderTransport.NATIVE,
        environments=frozenset({TradingEnvironment.PAPER}),
        declared_capabilities=frozenset({Capability.ACCOUNT}),
        verified_capabilities=frozenset({Capability.ACCOUNT}),
        markets=("XSHG",),
        order_types=(),
        supports_streaming=False,
        supports_reconciliation=True,
        enabled=True,
        trust_tier=TrustTier.PAPER_VALIDATED,
    )


def _complete_snapshot() -> AccountStateSnapshot:
    return capture_account_state_snapshot(
        provider=_account_provider(),
        account_reference="fixture-account-reference",
        account_reference_key=ACCOUNT_REFERENCE_KEY,
        environment=TradingEnvironment.PAPER,
        as_of=AS_OF,
        reconciled_at=AS_OF + timedelta(seconds=3),
        reconciliation_reference="fixture-reconciliation-20260901T090000Z",
        cash=(
            CashBalance(currency="USD", available=Decimal("15000.00"), settled=Decimal("12000")),
        ),
        positions=(
            AccountPosition(
                target_id="600028.SH",
                venue="XSHG",
                instrument_class="equity",
                side=Side.BUY,
                quantity=Decimal("1000"),
                concentration=Decimal("0.12"),
                concentration_gap=None,
            ),
            AccountPosition(
                target_id="510300.SH",
                venue="XSHG",
                instrument_class="etf",
                side=Side.BUY,
                quantity=Decimal("50"),
                concentration=Decimal("0.05"),
                concentration_gap=None,
            ),
        ),
        open_orders=(
            OpenOrder(
                order_reference="broker-order-002",
                target_id="600028.SH",
                venue="XSHG",
                instrument_class="equity",
                side=Side.SELL,
                quantity=Decimal("100"),
                status=OpenOrderStatus.PENDING_CANCEL,
                submitted_at=AS_OF - timedelta(minutes=2),
            ),
        ),
        recent_fills=(
            RecentFill(
                fill_reference="broker-fill-001",
                order_reference="broker-order-001",
                target_id="600028.SH",
                venue="XSHG",
                instrument_class="equity",
                side=Side.BUY,
                quantity=Decimal("50"),
                filled_at=AS_OF - timedelta(minutes=1),
            ),
        ),
        recent_fills_since=AS_OF - timedelta(hours=1),
    )


def test_harness_capture_is_content_identified_and_never_serializes_raw_account_reference() -> None:
    snapshot = _complete_snapshot()
    assert snapshot.account_reference_hash == opaque_account_reference_hash(
        "fixture-account-reference", key=ACCOUNT_REFERENCE_KEY
    )
    assert snapshot.snapshot_id == snapshot.expected_snapshot_id
    serialized = json.dumps(snapshot.to_dict(), sort_keys=True)
    assert "fixture-account-reference" not in serialized
    assert ACCOUNT_REFERENCE_KEY.hex() not in serialized
    assert snapshot.account_reference_hash != opaque_account_reference_hash(
        "fixture-account-reference", key=b"different-fixture-account-key-32-bytes-minimum"
    )

    replay = capture_account_state_snapshot(
        provider=_account_provider(),
        account_reference="fixture-account-reference",
        account_reference_key=ACCOUNT_REFERENCE_KEY,
        environment=TradingEnvironment.PAPER,
        as_of=AS_OF,
        reconciled_at=AS_OF + timedelta(seconds=3),
        reconciliation_reference="fixture-reconciliation-20260901T090000Z",
        cash=(CashBalance(currency="USD", available=Decimal("15000"), settled=Decimal("12000.0")),),
        positions=tuple(reversed(cast(tuple[AccountPosition, ...], snapshot.positions))),
        open_orders=cast(tuple[OpenOrder, ...], snapshot.open_orders),
        recent_fills=cast(tuple[RecentFill, ...], snapshot.recent_fills),
        recent_fills_since=AS_OF - timedelta(hours=1),
    )
    assert replay == snapshot

    with pytest.raises(ValueError, match="must not predate reconciled_at"):
        snapshot.project_positions(evaluated_at=AS_OF, max_age=timedelta(minutes=5))


def test_account_snapshot_parsing_and_schema_reject_tampered_completeness() -> None:
    snapshot = _complete_snapshot()
    payload = snapshot.to_dict()
    _validate("account-state-snapshot.schema.json", payload)
    assert account_state_snapshot_from_dict(payload) == snapshot

    tampered = deepcopy(payload)
    tampered["complete"] = False
    with pytest.raises(ValueError, match="complete must be derived"):
        account_state_snapshot_from_dict(tampered)

    with pytest.raises(ValidationError, match="snapshot_id"):
        invalid = deepcopy(payload)
        invalid.pop("snapshot_id")
        _validate("account-state-snapshot.schema.json", invalid)


def test_missing_or_stale_sections_are_risk_observable_but_not_exposure_increase_ready() -> None:
    missing_cash = capture_account_state_snapshot(
        provider=_account_provider(),
        account_reference="fixture-account-reference",
        account_reference_key=ACCOUNT_REFERENCE_KEY,
        environment=TradingEnvironment.PAPER,
        as_of=AS_OF,
        reconciled_at=AS_OF,
        reconciliation_reference="fixture-missing-cash",
        cash=None,
        positions=(),
        open_orders=(),
        recent_fills=(),
        recent_fills_since=AS_OF - timedelta(hours=1),
    )
    assert missing_cash.complete is False
    assert missing_cash.missing_sections == (AccountStateSection.CASH,)
    incomplete_projection = missing_cash.project_positions(
        evaluated_at=AS_OF,
        max_age=timedelta(minutes=5),
    )
    assert incomplete_projection.risk_observation_ready is True
    assert incomplete_projection.exposure_increase_ready is False
    assert incomplete_projection.observation_gaps == ("missing_section:cash",)

    stale_projection = _complete_snapshot().project_positions(
        evaluated_at=AS_OF + timedelta(minutes=6),
        max_age=timedelta(minutes=5),
    )
    assert stale_projection.risk_observation_ready is True
    assert stale_projection.exposure_increase_ready is False
    assert stale_projection.observation_gaps == ("stale",)

    missing_positions = capture_account_state_snapshot(
        provider=_account_provider(),
        account_reference="fixture-account-reference",
        account_reference_key=ACCOUNT_REFERENCE_KEY,
        environment=TradingEnvironment.PAPER,
        as_of=AS_OF,
        reconciled_at=AS_OF,
        reconciliation_reference="fixture-missing-positions",
        cash=(CashBalance(currency="USD", available=Decimal("1"), settled=Decimal("1")),),
        positions=None,
        open_orders=(),
        recent_fills=(),
        recent_fills_since=AS_OF - timedelta(hours=1),
    )
    position_gap_projection = missing_positions.project_positions(
        evaluated_at=AS_OF,
        max_age=timedelta(minutes=5),
    )
    assert position_gap_projection.risk_observation_ready is False
    assert position_gap_projection.exposure_increase_ready is False
    assert position_gap_projection.observation_gaps == ("missing_section:positions",)


def test_full_position_projection_preserves_account_provenance_and_order_fill_context() -> None:
    account_state = _complete_snapshot()
    projection = account_state.project_positions(
        evaluated_at=AS_OF + timedelta(minutes=1),
        max_age=timedelta(minutes=5),
    )

    assert projection.account_state_snapshot_id == account_state.snapshot_id
    assert projection.positions is not None
    assert projection.positions[0].target_id == "510300.SH"
    assert projection.positions[0].concentration == Decimal("0.05")
    assert projection.open_orders is not None
    assert projection.open_orders[0].status is OpenOrderStatus.PENDING_CANCEL
    assert projection.recent_fills is not None
    assert projection.recent_fills[0].order_reference == "broker-order-001"
    assert projection.exposure_increase_ready is True
    _validate("position-snapshot.schema.json", projection.to_dict())
    assert position_snapshot_from_dict(projection.to_dict()) == projection

    tampered = deepcopy(projection.to_dict())
    tampered["open_orders"] = None
    tampered["snapshot_id"] = "position-snapshot-" + "0" * 64
    with pytest.raises(ValueError, match="missing sections must remain observable"):
        position_snapshot_from_dict(tampered)


def test_explicit_concentration_gap_makes_the_account_incomplete() -> None:
    snapshot = capture_account_state_snapshot(
        provider=_account_provider(),
        account_reference="fixture-account-reference",
        account_reference_key=ACCOUNT_REFERENCE_KEY,
        environment=TradingEnvironment.PAPER,
        as_of=AS_OF,
        reconciled_at=AS_OF,
        reconciliation_reference="fixture-concentration-gap",
        cash=(CashBalance(currency="USD", available=Decimal("1"), settled=Decimal("1")),),
        positions=(
            AccountPosition(
                target_id="510300.SH",
                venue="XSHG",
                instrument_class="etf",
                side=Side.BUY,
                quantity=Decimal("1"),
                concentration=None,
                concentration_gap="mark_notional_not_reconciled",
            ),
        ),
        open_orders=(),
        recent_fills=(),
        recent_fills_since=AS_OF - timedelta(hours=1),
    )
    projection = snapshot.project_positions(evaluated_at=AS_OF, max_age=timedelta(minutes=5))

    assert snapshot.complete is False
    assert snapshot.reconciliation_gaps == ("position_concentration:510300.SH:XSHG:etf:buy",)
    assert projection.risk_observation_ready is True
    assert projection.exposure_increase_ready is False


def test_capture_requires_an_account_capability_and_does_not_upgrade_mock_execution() -> None:
    with pytest.raises(ValueError, match="declare the account capability"):
        capture_account_state_snapshot(
            provider=MockExecutionProvider().manifest,
            account_reference="fixture-account-reference",
            account_reference_key=ACCOUNT_REFERENCE_KEY,
            environment=TradingEnvironment.PAPER,
            as_of=AS_OF,
            reconciled_at=AS_OF,
            reconciliation_reference="mock-must-not-read-account",
            cash=(),
            positions=(),
            open_orders=(),
            recent_fills=(),
            recent_fills_since=AS_OF - timedelta(hours=1),
        )

    with pytest.raises(ValueError, match="verify the account capability"):
        capture_account_state_snapshot(
            provider=replace(_account_provider(), verified_capabilities=frozenset()),
            account_reference="fixture-account-reference",
            account_reference_key=ACCOUNT_REFERENCE_KEY,
            environment=TradingEnvironment.PAPER,
            as_of=AS_OF,
            reconciled_at=AS_OF,
            reconciliation_reference="unverified-account-read",
            cash=(),
            positions=(),
            open_orders=(),
            recent_fills=(),
            recent_fills_since=AS_OF - timedelta(hours=1),
        )

    with pytest.raises(ValueError, match="support reconciliation"):
        capture_account_state_snapshot(
            provider=replace(_account_provider(), supports_reconciliation=False),
            account_reference="fixture-account-reference",
            account_reference_key=ACCOUNT_REFERENCE_KEY,
            environment=TradingEnvironment.PAPER,
            as_of=AS_OF,
            reconciled_at=AS_OF,
            reconciliation_reference="unreconciled-account-read",
            cash=(),
            positions=(),
            open_orders=(),
            recent_fills=(),
            recent_fills_since=AS_OF - timedelta(hours=1),
        )


def test_account_reference_key_store_is_stable_private_and_rejects_unsafe_file(
    tmp_path: Path,
) -> None:
    path = tmp_path / "private" / "account-reference.key"
    first = load_or_create_account_reference_key(path)
    second = load_or_create_account_reference_key(path)
    assert first == second
    assert len(first) == 32
    assert path.stat().st_mode & 0o077 == 0

    os.chmod(path, 0o644)
    with pytest.raises(PermissionError, match="group- or world-accessible"):
        load_or_create_account_reference_key(path)


def test_account_reference_key_store_rejects_symbolic_link(tmp_path: Path) -> None:
    target = tmp_path / "target.key"
    target.write_bytes(b"x" * 32)
    target.chmod(0o600)
    link = tmp_path / "account-reference.key"
    link.symlink_to(target)

    with pytest.raises(ValueError, match="directly readable file"):
        load_or_create_account_reference_key(link)


def test_account_reference_key_store_publishes_one_complete_key_concurrently(
    tmp_path: Path,
) -> None:
    path = tmp_path / "private" / "account-reference.key"

    def load_key(_: int) -> bytes:
        return load_or_create_account_reference_key(path)

    with ThreadPoolExecutor(max_workers=8) as executor:
        keys = tuple(executor.map(load_key, range(32)))

    assert len(set(keys)) == 1
    assert len(keys[0]) == 32


def _validate(schema_name: str, payload: dict[str, object]) -> None:
    registry: Registry[Schema] = Registry()
    for path in (
        ROOT / "schemas" / "account-state-snapshot.schema.json",
        ROOT / "schemas" / "position-snapshot.schema.json",
    ):
        schema = cast(dict[str, Any], json.loads(path.read_text(encoding="utf-8")))
        schema_id = schema["$id"]
        assert isinstance(schema_id, str)
        registry = registry.with_resource(schema_id, DRAFT202012.create_resource(schema))
    schema = cast(dict[str, Any], json.loads((ROOT / "schemas" / schema_name).read_text()))
    validator = Draft202012Validator(schema, registry=registry, format_checker=FormatChecker())
    cast(Validator, validator).validate(payload)
