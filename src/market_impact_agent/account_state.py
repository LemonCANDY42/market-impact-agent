from __future__ import annotations

import hashlib
import hmac
import re
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from typing import cast

from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.domain import Side, TradingEnvironment, require_aware
from market_impact_agent.providers import Capability, ProviderManifest

ACCOUNT_STATE_SNAPSHOT_SCHEMA = "market-impact.account-state-snapshot.v1"
POSITION_SNAPSHOT_SCHEMA = "market-impact.position-snapshot.v1"


class AccountStateSection(StrEnum):
    CASH = "cash"
    POSITIONS = "positions"
    OPEN_ORDERS = "open_orders"
    RECENT_FILLS = "recent_fills"


class OpenOrderStatus(StrEnum):
    WORKING = "working"
    PARTIALLY_FILLED = "partially_filled"
    PENDING_CANCEL = "pending_cancel"


@dataclass(frozen=True, slots=True)
class CashBalance:
    currency: str
    available: Decimal
    settled: Decimal

    def __post_init__(self) -> None:
        _trimmed(self.currency, "cash currency")
        _finite_decimal(self.available, "cash available")
        _finite_decimal(self.settled, "cash settled")

    def to_dict(self) -> dict[str, str]:
        return {
            "currency": self.currency,
            "available": _decimal_text(self.available),
            "settled": _decimal_text(self.settled),
        }


@dataclass(frozen=True, slots=True)
class AccountPosition:
    target_id: str
    venue: str
    instrument_class: str
    side: Side
    quantity: Decimal
    concentration: Decimal | None
    concentration_gap: str | None

    def __post_init__(self) -> None:
        _instrument_identity(self.target_id, self.venue, self.instrument_class, "position")
        _positive_decimal(self.quantity, "position quantity")
        if (self.concentration is None) == (self.concentration_gap is None):
            raise ValueError("position requires concentration or an explicit concentration_gap")
        if self.concentration is not None:
            _finite_decimal(self.concentration, "position concentration")
            if self.concentration < 0 or self.concentration > 1:
                raise ValueError("position concentration must be between zero and one")
        if self.concentration_gap is not None:
            _trimmed(self.concentration_gap, "position concentration_gap")

    def to_dict(self) -> dict[str, str | None]:
        return {
            "target_id": self.target_id,
            "venue": self.venue,
            "instrument_class": self.instrument_class,
            "side": self.side.value,
            "quantity": _decimal_text(self.quantity),
            "concentration": (
                None if self.concentration is None else _decimal_text(self.concentration)
            ),
            "concentration_gap": self.concentration_gap,
        }


@dataclass(frozen=True, slots=True)
class OpenOrder:
    order_reference: str
    target_id: str
    venue: str
    instrument_class: str
    side: Side
    quantity: Decimal
    status: OpenOrderStatus
    submitted_at: datetime

    def __post_init__(self) -> None:
        _trimmed(self.order_reference, "open order reference")
        _instrument_identity(self.target_id, self.venue, self.instrument_class, "open order")
        _positive_decimal(self.quantity, "open order quantity")
        _strict_utc(self.submitted_at, "open order submitted_at")

    def to_dict(self) -> dict[str, str]:
        return {
            "order_reference": self.order_reference,
            "target_id": self.target_id,
            "venue": self.venue,
            "instrument_class": self.instrument_class,
            "side": self.side.value,
            "quantity": _decimal_text(self.quantity),
            "status": self.status.value,
            "submitted_at": _timestamp(self.submitted_at),
        }


@dataclass(frozen=True, slots=True)
class RecentFill:
    fill_reference: str
    order_reference: str
    target_id: str
    venue: str
    instrument_class: str
    side: Side
    quantity: Decimal
    filled_at: datetime

    def __post_init__(self) -> None:
        _trimmed(self.fill_reference, "recent fill reference")
        _trimmed(self.order_reference, "recent fill order reference")
        _instrument_identity(self.target_id, self.venue, self.instrument_class, "recent fill")
        _positive_decimal(self.quantity, "recent fill quantity")
        _strict_utc(self.filled_at, "recent fill filled_at")

    def to_dict(self) -> dict[str, str]:
        return {
            "fill_reference": self.fill_reference,
            "order_reference": self.order_reference,
            "target_id": self.target_id,
            "venue": self.venue,
            "instrument_class": self.instrument_class,
            "side": self.side.value,
            "quantity": _decimal_text(self.quantity),
            "filled_at": _timestamp(self.filled_at),
        }


@dataclass(frozen=True, slots=True)
class AccountStateReadiness:
    risk_observation_ready: bool
    exposure_increase_ready: bool
    gaps: tuple[str, ...]

    def __post_init__(self) -> None:
        _sorted_unique_nonempty(self.gaps, "account-state readiness gaps")
        if self.exposure_increase_ready and not self.risk_observation_ready:
            raise ValueError("exposure-increase readiness requires risk-observation readiness")


@dataclass(frozen=True, slots=True)
class AccountStateSnapshot:
    snapshot_id: str
    account_reference_hash: str
    environment: TradingEnvironment
    provider_id: str
    provider_version: str
    provider_manifest_hash: str
    as_of: datetime
    reconciled_at: datetime
    reconciliation_reference: str
    cash: tuple[CashBalance, ...] | None
    positions: tuple[AccountPosition, ...] | None
    open_orders: tuple[OpenOrder, ...] | None
    recent_fills: tuple[RecentFill, ...] | None
    recent_fills_since: datetime | None
    missing_sections: tuple[AccountStateSection, ...]
    reconciliation_gaps: tuple[str, ...]
    complete: bool
    schema_version: str = ACCOUNT_STATE_SNAPSHOT_SCHEMA

    def __post_init__(self) -> None:
        if self.schema_version != ACCOUNT_STATE_SNAPSHOT_SCHEMA:
            raise ValueError("unsupported Account State Snapshot schema")
        _prefixed_hash(self.account_reference_hash, "account-ref-", "account reference hash")
        _trimmed(self.provider_id, "Account State provider_id")
        _trimmed(self.provider_version, "Account State provider_version")
        _sha256(self.provider_manifest_hash, "Account State provider_manifest_hash")
        _strict_utc(self.as_of, "Account State as_of")
        _strict_utc(self.reconciled_at, "Account State reconciled_at")
        if self.as_of > self.reconciled_at:
            raise ValueError("Account State as_of must not be after reconciled_at")
        _trimmed(self.reconciliation_reference, "Account State reconciliation_reference")
        _validate_section_data(
            cash=self.cash,
            positions=self.positions,
            open_orders=self.open_orders,
            recent_fills=self.recent_fills,
            recent_fills_since=self.recent_fills_since,
            as_of=self.as_of,
        )
        expected_missing = _missing_sections(
            cash=self.cash,
            positions=self.positions,
            open_orders=self.open_orders,
            recent_fills=self.recent_fills,
        )
        if self.missing_sections != expected_missing:
            raise ValueError("Account State missing_sections must be derived from missing data")
        _sorted_unique_nonempty(self.reconciliation_gaps, "Account State reconciliation_gaps")
        if not set(_position_concentration_gaps(self.positions)) <= set(self.reconciliation_gaps):
            raise ValueError("Account State concentration gaps must remain explicit")
        expected_complete = not self.missing_sections and not self.reconciliation_gaps
        if self.complete is not expected_complete:
            raise ValueError("Account State complete must be derived from sections and gaps")
        if self.snapshot_id != self.expected_snapshot_id:
            raise ValueError("Account State Snapshot ID does not match content")

    @property
    def expected_snapshot_id(self) -> str:
        return f"account-state-snapshot-{canonical_hash(self.core_dict())}"

    def core_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "account_reference_hash": self.account_reference_hash,
            "environment": self.environment.value,
            "provider_id": self.provider_id,
            "provider_version": self.provider_version,
            "provider_manifest_hash": self.provider_manifest_hash,
            "as_of": _timestamp(self.as_of),
            "reconciled_at": _timestamp(self.reconciled_at),
            "reconciliation_reference": self.reconciliation_reference,
            "cash": None if self.cash is None else [item.to_dict() for item in self.cash],
            "positions": (
                None if self.positions is None else [item.to_dict() for item in self.positions]
            ),
            "open_orders": (
                None if self.open_orders is None else [item.to_dict() for item in self.open_orders]
            ),
            "recent_fills": (
                None
                if self.recent_fills is None
                else [item.to_dict() for item in self.recent_fills]
            ),
            "recent_fills_since": (
                None if self.recent_fills_since is None else _timestamp(self.recent_fills_since)
            ),
            "missing_sections": [item.value for item in self.missing_sections],
            "reconciliation_gaps": list(self.reconciliation_gaps),
            "complete": self.complete,
        }

    def to_dict(self) -> dict[str, object]:
        return {**self.core_dict(), "snapshot_id": self.snapshot_id}

    @classmethod
    def build(
        cls,
        *,
        provider: ProviderManifest,
        account_reference: str,
        account_reference_key: bytes,
        environment: TradingEnvironment,
        as_of: datetime,
        reconciled_at: datetime,
        reconciliation_reference: str,
        cash: tuple[CashBalance, ...] | None,
        positions: tuple[AccountPosition, ...] | None,
        open_orders: tuple[OpenOrder, ...] | None,
        recent_fills: tuple[RecentFill, ...] | None,
        recent_fills_since: datetime | None,
        reconciliation_gaps: tuple[str, ...] = (),
    ) -> AccountStateSnapshot:
        _assert_account_provider(provider, environment)
        _trimmed(account_reference, "account reference")
        _strict_utc(as_of, "Account State as_of")
        _strict_utc(reconciled_at, "Account State reconciled_at")
        _trimmed(reconciliation_reference, "Account State reconciliation_reference")
        ordered_cash = _order_cash(cash)
        ordered_positions = _order_positions(positions)
        ordered_open_orders = _order_open_orders(open_orders)
        ordered_recent_fills = _order_recent_fills(recent_fills)
        ordered_gaps = tuple(
            sorted(set(reconciliation_gaps) | set(_position_concentration_gaps(ordered_positions)))
        )
        _sorted_unique_nonempty(ordered_gaps, "Account State reconciliation_gaps")
        missing = _missing_sections(
            cash=ordered_cash,
            positions=ordered_positions,
            open_orders=ordered_open_orders,
            recent_fills=ordered_recent_fills,
        )
        complete = not missing and not ordered_gaps
        account_reference_hash = opaque_account_reference_hash(
            account_reference,
            key=account_reference_key,
        )
        provider_manifest_hash = canonical_hash(provider.to_dict())
        core = {
            "schema_version": ACCOUNT_STATE_SNAPSHOT_SCHEMA,
            "account_reference_hash": account_reference_hash,
            "environment": environment.value,
            "provider_id": provider.provider_id,
            "provider_version": provider.provider_version,
            "provider_manifest_hash": provider_manifest_hash,
            "as_of": _timestamp(as_of),
            "reconciled_at": _timestamp(reconciled_at),
            "reconciliation_reference": reconciliation_reference,
            "cash": None if ordered_cash is None else [item.to_dict() for item in ordered_cash],
            "positions": (
                None
                if ordered_positions is None
                else [item.to_dict() for item in ordered_positions]
            ),
            "open_orders": (
                None
                if ordered_open_orders is None
                else [item.to_dict() for item in ordered_open_orders]
            ),
            "recent_fills": (
                None
                if ordered_recent_fills is None
                else [item.to_dict() for item in ordered_recent_fills]
            ),
            "recent_fills_since": (
                None if recent_fills_since is None else _timestamp(recent_fills_since)
            ),
            "missing_sections": [item.value for item in missing],
            "reconciliation_gaps": list(ordered_gaps),
            "complete": complete,
        }
        return cls(
            snapshot_id=f"account-state-snapshot-{canonical_hash(core)}",
            account_reference_hash=account_reference_hash,
            environment=environment,
            provider_id=provider.provider_id,
            provider_version=provider.provider_version,
            provider_manifest_hash=provider_manifest_hash,
            as_of=as_of,
            reconciled_at=reconciled_at,
            reconciliation_reference=reconciliation_reference,
            cash=ordered_cash,
            positions=ordered_positions,
            open_orders=ordered_open_orders,
            recent_fills=ordered_recent_fills,
            recent_fills_since=recent_fills_since,
            missing_sections=missing,
            reconciliation_gaps=ordered_gaps,
            complete=complete,
        )

    def readiness(
        self,
        *,
        evaluated_at: datetime,
        max_age: timedelta,
    ) -> AccountStateReadiness:
        _strict_utc(evaluated_at, "Account State readiness evaluated_at")
        if max_age <= timedelta(0):
            raise ValueError("Account State readiness max_age must be positive")
        if evaluated_at < self.as_of:
            raise ValueError("Account State readiness evaluated_at must not predate as_of")
        if evaluated_at < self.reconciled_at:
            raise ValueError("Account State readiness evaluated_at must not predate reconciled_at")
        gaps = list(self.reconciliation_gaps)
        gaps.extend(f"missing_section:{item.value}" for item in self.missing_sections)
        if evaluated_at - self.as_of > max_age:
            gaps.append("stale")
        ordered_gaps = tuple(sorted(set(gaps)))
        risk_observation_ready = AccountStateSection.POSITIONS not in self.missing_sections
        return AccountStateReadiness(
            risk_observation_ready=risk_observation_ready,
            exposure_increase_ready=risk_observation_ready and not ordered_gaps,
            gaps=ordered_gaps,
        )

    def project_positions(
        self,
        *,
        evaluated_at: datetime,
        max_age: timedelta,
    ) -> PositionSnapshot:
        return PositionSnapshot.build(
            account_state=self,
            evaluated_at=evaluated_at,
            max_age=max_age,
        )


@dataclass(frozen=True, slots=True)
class PositionSnapshot:
    snapshot_id: str
    account_state_snapshot_id: str
    account_reference_hash: str
    environment: TradingEnvironment
    provider_id: str
    as_of: datetime
    reconciled_at: datetime
    evaluated_at: datetime
    max_age_seconds: int
    cash: tuple[CashBalance, ...] | None
    positions: tuple[AccountPosition, ...] | None
    open_orders: tuple[OpenOrder, ...] | None
    recent_fills: tuple[RecentFill, ...] | None
    recent_fills_since: datetime | None
    complete: bool
    observation_gaps: tuple[str, ...]
    risk_observation_ready: bool
    exposure_increase_ready: bool
    schema_version: str = POSITION_SNAPSHOT_SCHEMA

    def __post_init__(self) -> None:
        if self.schema_version != POSITION_SNAPSHOT_SCHEMA:
            raise ValueError("unsupported Position Snapshot schema")
        _prefixed_hash(
            self.account_state_snapshot_id,
            "account-state-snapshot-",
            "Position Snapshot account-state provenance",
        )
        _prefixed_hash(
            self.account_reference_hash,
            "account-ref-",
            "Position Snapshot account hash",
        )
        _trimmed(self.provider_id, "Position Snapshot provider_id")
        _strict_utc(self.as_of, "Position Snapshot as_of")
        _strict_utc(self.reconciled_at, "Position Snapshot reconciled_at")
        _strict_utc(self.evaluated_at, "Position Snapshot evaluated_at")
        if self.as_of > self.reconciled_at:
            raise ValueError("Position Snapshot as_of must not be after reconciled_at")
        if self.evaluated_at < self.reconciled_at:
            raise ValueError("Position Snapshot evaluated_at must not predate reconciled_at")
        if self.max_age_seconds <= 0:
            raise ValueError("Position Snapshot max_age_seconds must be positive")
        _validate_section_data(
            cash=self.cash,
            positions=self.positions,
            open_orders=self.open_orders,
            recent_fills=self.recent_fills,
            recent_fills_since=self.recent_fills_since,
            as_of=self.as_of,
        )
        _sorted_unique_nonempty(self.observation_gaps, "Position Snapshot observation_gaps")
        missing_sections = _missing_sections(
            cash=self.cash,
            positions=self.positions,
            open_orders=self.open_orders,
            recent_fills=self.recent_fills,
        )
        required_missing_gaps = {f"missing_section:{item.value}" for item in missing_sections}
        if not required_missing_gaps <= set(self.observation_gaps):
            raise ValueError("Position Snapshot missing sections must remain observable")
        required_concentration_gaps = set(_position_concentration_gaps(self.positions))
        if not required_concentration_gaps <= set(self.observation_gaps):
            raise ValueError("Position Snapshot concentration gaps must remain observable")
        expected_complete = not missing_sections and not (set(self.observation_gaps) - {"stale"})
        if self.complete is not expected_complete:
            raise ValueError("Position Snapshot complete must be derived")
        expected_risk_ready = self.positions is not None
        if self.risk_observation_ready is not expected_risk_ready:
            raise ValueError("Position Snapshot risk-observation readiness must be derived")
        expected_increase_ready = (
            expected_complete and not self.observation_gaps and expected_risk_ready
        )
        if self.exposure_increase_ready is not expected_increase_ready:
            raise ValueError("Position Snapshot exposure-increase readiness must be derived")
        if self.snapshot_id != self.expected_snapshot_id:
            raise ValueError("Position Snapshot ID does not match content")

    @property
    def expected_snapshot_id(self) -> str:
        return f"position-snapshot-{canonical_hash(self.core_dict())}"

    def core_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "account_state_snapshot_id": self.account_state_snapshot_id,
            "account_reference_hash": self.account_reference_hash,
            "environment": self.environment.value,
            "provider_id": self.provider_id,
            "as_of": _timestamp(self.as_of),
            "reconciled_at": _timestamp(self.reconciled_at),
            "evaluated_at": _timestamp(self.evaluated_at),
            "max_age_seconds": self.max_age_seconds,
            "cash": None if self.cash is None else [item.to_dict() for item in self.cash],
            "positions": (
                None if self.positions is None else [item.to_dict() for item in self.positions]
            ),
            "open_orders": (
                None if self.open_orders is None else [item.to_dict() for item in self.open_orders]
            ),
            "recent_fills": (
                None
                if self.recent_fills is None
                else [item.to_dict() for item in self.recent_fills]
            ),
            "recent_fills_since": (
                None if self.recent_fills_since is None else _timestamp(self.recent_fills_since)
            ),
            "complete": self.complete,
            "observation_gaps": list(self.observation_gaps),
            "risk_observation_ready": self.risk_observation_ready,
            "exposure_increase_ready": self.exposure_increase_ready,
        }

    def to_dict(self) -> dict[str, object]:
        return {**self.core_dict(), "snapshot_id": self.snapshot_id}

    @classmethod
    def build(
        cls,
        *,
        account_state: AccountStateSnapshot,
        evaluated_at: datetime,
        max_age: timedelta,
    ) -> PositionSnapshot:
        readiness = account_state.readiness(evaluated_at=evaluated_at, max_age=max_age)
        max_age_seconds = _whole_positive_seconds(max_age, "Position Snapshot max_age")
        core = {
            "schema_version": POSITION_SNAPSHOT_SCHEMA,
            "account_state_snapshot_id": account_state.snapshot_id,
            "account_reference_hash": account_state.account_reference_hash,
            "environment": account_state.environment.value,
            "provider_id": account_state.provider_id,
            "as_of": _timestamp(account_state.as_of),
            "reconciled_at": _timestamp(account_state.reconciled_at),
            "evaluated_at": _timestamp(evaluated_at),
            "max_age_seconds": max_age_seconds,
            "cash": (
                None
                if account_state.cash is None
                else [item.to_dict() for item in account_state.cash]
            ),
            "positions": (
                None
                if account_state.positions is None
                else [item.to_dict() for item in account_state.positions]
            ),
            "open_orders": (
                None
                if account_state.open_orders is None
                else [item.to_dict() for item in account_state.open_orders]
            ),
            "recent_fills": (
                None
                if account_state.recent_fills is None
                else [item.to_dict() for item in account_state.recent_fills]
            ),
            "recent_fills_since": (
                None
                if account_state.recent_fills_since is None
                else _timestamp(account_state.recent_fills_since)
            ),
            "complete": account_state.complete,
            "observation_gaps": list(readiness.gaps),
            "risk_observation_ready": readiness.risk_observation_ready,
            "exposure_increase_ready": readiness.exposure_increase_ready,
        }
        return cls(
            snapshot_id=f"position-snapshot-{canonical_hash(core)}",
            account_state_snapshot_id=account_state.snapshot_id,
            account_reference_hash=account_state.account_reference_hash,
            environment=account_state.environment,
            provider_id=account_state.provider_id,
            as_of=account_state.as_of,
            reconciled_at=account_state.reconciled_at,
            evaluated_at=evaluated_at,
            max_age_seconds=max_age_seconds,
            cash=account_state.cash,
            positions=account_state.positions,
            open_orders=account_state.open_orders,
            recent_fills=account_state.recent_fills,
            recent_fills_since=account_state.recent_fills_since,
            complete=account_state.complete,
            observation_gaps=readiness.gaps,
            risk_observation_ready=readiness.risk_observation_ready,
            exposure_increase_ready=readiness.exposure_increase_ready,
        )


def capture_account_state_snapshot(
    *,
    provider: ProviderManifest,
    account_reference: str,
    account_reference_key: bytes,
    environment: TradingEnvironment,
    as_of: datetime,
    reconciled_at: datetime,
    reconciliation_reference: str,
    cash: tuple[CashBalance, ...] | None,
    positions: tuple[AccountPosition, ...] | None,
    open_orders: tuple[OpenOrder, ...] | None,
    recent_fills: tuple[RecentFill, ...] | None,
    recent_fills_since: datetime | None,
    reconciliation_gaps: tuple[str, ...] = (),
) -> AccountStateSnapshot:
    """Normalize Provider-reported account facts; the Harness alone mints the snapshot ID."""

    return AccountStateSnapshot.build(
        provider=provider,
        account_reference=account_reference,
        account_reference_key=account_reference_key,
        environment=environment,
        as_of=as_of,
        reconciled_at=reconciled_at,
        reconciliation_reference=reconciliation_reference,
        cash=cash,
        positions=positions,
        open_orders=open_orders,
        recent_fills=recent_fills,
        recent_fills_since=recent_fills_since,
        reconciliation_gaps=reconciliation_gaps,
    )


def account_state_snapshot_from_dict(value: object) -> AccountStateSnapshot:
    payload = _object(value, "Account State Snapshot")
    _exact_keys(payload, _ACCOUNT_STATE_FIELDS, "Account State Snapshot")
    snapshot = AccountStateSnapshot(
        snapshot_id=_string(payload, "snapshot_id"),
        account_reference_hash=_string(payload, "account_reference_hash"),
        environment=TradingEnvironment(_string(payload, "environment")),
        provider_id=_string(payload, "provider_id"),
        provider_version=_string(payload, "provider_version"),
        provider_manifest_hash=_string(payload, "provider_manifest_hash"),
        as_of=_datetime(payload.get("as_of"), "Account State as_of"),
        reconciled_at=_datetime(payload.get("reconciled_at"), "Account State reconciled_at"),
        reconciliation_reference=_string(payload, "reconciliation_reference"),
        cash=_cash_from_optional(payload.get("cash"), "Account State cash"),
        positions=_positions_from_optional(payload.get("positions"), "Account State positions"),
        open_orders=_open_orders_from_optional(
            payload.get("open_orders"), "Account State open_orders"
        ),
        recent_fills=_recent_fills_from_optional(
            payload.get("recent_fills"), "Account State recent_fills"
        ),
        recent_fills_since=_optional_datetime(
            payload.get("recent_fills_since"), "Account State recent_fills_since"
        ),
        missing_sections=tuple(
            AccountStateSection(item)
            for item in _string_list(payload.get("missing_sections"), "missing_sections")
        ),
        reconciliation_gaps=_string_list(payload.get("reconciliation_gaps"), "reconciliation_gaps"),
        complete=_bool(payload, "complete"),
        schema_version=_string(payload, "schema_version"),
    )
    if snapshot.to_dict() != payload:
        raise ValueError("Account State Snapshot does not match the canonical contract")
    return snapshot


def position_snapshot_from_dict(value: object) -> PositionSnapshot:
    payload = _object(value, "Position Snapshot")
    _exact_keys(payload, _POSITION_SNAPSHOT_FIELDS, "Position Snapshot")
    snapshot = PositionSnapshot(
        snapshot_id=_string(payload, "snapshot_id"),
        account_state_snapshot_id=_string(payload, "account_state_snapshot_id"),
        account_reference_hash=_string(payload, "account_reference_hash"),
        environment=TradingEnvironment(_string(payload, "environment")),
        provider_id=_string(payload, "provider_id"),
        as_of=_datetime(payload.get("as_of"), "Position Snapshot as_of"),
        reconciled_at=_datetime(payload.get("reconciled_at"), "Position Snapshot reconciled_at"),
        evaluated_at=_datetime(payload.get("evaluated_at"), "Position Snapshot evaluated_at"),
        max_age_seconds=_integer(payload, "max_age_seconds"),
        cash=_cash_from_optional(payload.get("cash"), "Position Snapshot cash"),
        positions=_positions_from_optional(payload.get("positions"), "Position Snapshot positions"),
        open_orders=_open_orders_from_optional(
            payload.get("open_orders"), "Position Snapshot open_orders"
        ),
        recent_fills=_recent_fills_from_optional(
            payload.get("recent_fills"), "Position Snapshot recent_fills"
        ),
        recent_fills_since=_optional_datetime(
            payload.get("recent_fills_since"), "Position Snapshot recent_fills_since"
        ),
        complete=_bool(payload, "complete"),
        observation_gaps=_string_list(payload.get("observation_gaps"), "observation_gaps"),
        risk_observation_ready=_bool(payload, "risk_observation_ready"),
        exposure_increase_ready=_bool(payload, "exposure_increase_ready"),
        schema_version=_string(payload, "schema_version"),
    )
    if snapshot.to_dict() != payload:
        raise ValueError("Position Snapshot does not match the canonical contract")
    return snapshot


def opaque_account_reference_hash(account_reference: str, *, key: bytes) -> str:
    _trimmed(account_reference, "account reference")
    if len(key) < 32:
        raise ValueError("account reference pseudonymization key must contain at least 32 bytes")
    digest = hmac.new(key, account_reference.encode("utf-8"), hashlib.sha256).hexdigest()
    return "account-ref-" + digest


def _assert_account_provider(provider: ProviderManifest, environment: TradingEnvironment) -> None:
    provider.assert_valid()
    if not provider.enabled:
        raise ValueError("Account State provider must be enabled")
    if Capability.ACCOUNT not in provider.declared_capabilities:
        raise ValueError("Account State provider must declare the account capability")
    if Capability.ACCOUNT not in provider.verified_capabilities:
        raise ValueError("Account State provider must verify the account capability")
    if not provider.supports_reconciliation:
        raise ValueError("Account State provider must support reconciliation")
    if environment not in provider.environments:
        raise ValueError("Account State environment is not supported by the provider")


def _missing_sections(
    *,
    cash: tuple[CashBalance, ...] | None,
    positions: tuple[AccountPosition, ...] | None,
    open_orders: tuple[OpenOrder, ...] | None,
    recent_fills: tuple[RecentFill, ...] | None,
) -> tuple[AccountStateSection, ...]:
    sections: list[AccountStateSection] = []
    if cash is None:
        sections.append(AccountStateSection.CASH)
    if positions is None:
        sections.append(AccountStateSection.POSITIONS)
    if open_orders is None:
        sections.append(AccountStateSection.OPEN_ORDERS)
    if recent_fills is None:
        sections.append(AccountStateSection.RECENT_FILLS)
    return tuple(sections)


def _position_concentration_gaps(
    positions: tuple[AccountPosition, ...] | None,
) -> tuple[str, ...]:
    if positions is None:
        return ()
    return tuple(
        f"position_concentration:{item.target_id}:{item.venue}:{item.instrument_class}:{item.side.value}"
        for item in positions
        if item.concentration_gap is not None
    )


def _validate_section_data(
    *,
    cash: tuple[CashBalance, ...] | None,
    positions: tuple[AccountPosition, ...] | None,
    open_orders: tuple[OpenOrder, ...] | None,
    recent_fills: tuple[RecentFill, ...] | None,
    recent_fills_since: datetime | None,
    as_of: datetime,
) -> None:
    if cash is not None:
        _sorted_unique(
            tuple(item.currency for item in cash),
            "Account State cash currencies",
        )
    if positions is not None:
        _sorted_unique_keys(
            tuple(_position_key(item) for item in positions),
            "Account State positions",
        )
    if open_orders is not None:
        _sorted_unique(
            tuple(item.order_reference for item in open_orders),
            "Account State open orders",
        )
    if recent_fills is not None:
        _sorted_unique(
            tuple(item.fill_reference for item in recent_fills),
            "Account State recent fills",
        )
    if (recent_fills is None) != (recent_fills_since is None):
        raise ValueError("recent_fills and recent_fills_since must be present or absent together")
    if recent_fills_since is not None:
        _strict_utc(recent_fills_since, "recent fills since")
        if recent_fills_since > as_of:
            raise ValueError("recent fills since must not be after Account State as_of")
        if recent_fills is not None and any(
            item.filled_at < recent_fills_since for item in recent_fills
        ):
            raise ValueError("recent fills must not predate recent_fills_since")


def _order_cash(items: tuple[CashBalance, ...] | None) -> tuple[CashBalance, ...] | None:
    if items is None:
        return None
    return tuple(sorted(items, key=lambda item: item.currency))


def _order_positions(
    items: tuple[AccountPosition, ...] | None,
) -> tuple[AccountPosition, ...] | None:
    if items is None:
        return None
    return tuple(sorted(items, key=_position_key))


def _order_open_orders(items: tuple[OpenOrder, ...] | None) -> tuple[OpenOrder, ...] | None:
    if items is None:
        return None
    return tuple(sorted(items, key=lambda item: item.order_reference))


def _order_recent_fills(items: tuple[RecentFill, ...] | None) -> tuple[RecentFill, ...] | None:
    if items is None:
        return None
    return tuple(sorted(items, key=lambda item: item.fill_reference))


def _position_key(item: AccountPosition) -> tuple[str, str, str, str]:
    return (item.target_id, item.venue, item.instrument_class, item.side.value)


def _whole_positive_seconds(value: timedelta, name: str) -> int:
    seconds = value.total_seconds()
    if seconds <= 0 or not seconds.is_integer():
        raise ValueError(f"{name} must be a positive whole number of seconds")
    return int(seconds)


def _instrument_identity(target_id: str, venue: str, instrument_class: str, name: str) -> None:
    _trimmed(target_id, f"{name} target_id")
    _trimmed(venue, f"{name} venue")
    _trimmed(instrument_class, f"{name} instrument_class")


def _finite_decimal(value: object, name: str) -> None:
    if not isinstance(value, Decimal) or not value.is_finite():
        raise ValueError(f"{name} must be a finite Decimal")


def _positive_decimal(value: Decimal, name: str) -> None:
    _finite_decimal(value, name)
    if value <= 0:
        raise ValueError(f"{name} must be positive")


def _decimal_text(value: Decimal) -> str:
    _finite_decimal(value, "decimal")
    if value == 0:
        return "0"
    return format(value.normalize(), "f")


def _strict_utc(value: datetime, name: str) -> None:
    require_aware(value, name)
    if value.utcoffset() != timedelta(0):
        raise ValueError(f"{name} must be UTC")


def _timestamp(value: datetime) -> str:
    _strict_utc(value, "timestamp")
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _trimmed(value: object, name: str) -> None:
    if not isinstance(value, str) or not value or value != value.strip():
        raise ValueError(f"{name} must be a non-empty trimmed string")


def _sha256(value: str, name: str) -> None:
    if re.fullmatch(r"[0-9a-f]{64}", value) is None:
        raise ValueError(f"{name} must be a SHA-256 hash")


def _prefixed_hash(value: str, prefix: str, name: str) -> None:
    if not value.startswith(prefix):
        raise ValueError(f"{name} must start with {prefix}")
    _sha256(value.removeprefix(prefix), name)


def _sorted_unique(values: tuple[str, ...], name: str) -> None:
    if values != tuple(sorted(set(values))):
        raise ValueError(f"{name} must be sorted and unique")


def _sorted_unique_keys(values: tuple[tuple[str, str, str, str], ...], name: str) -> None:
    if values != tuple(sorted(set(values))):
        raise ValueError(f"{name} must be sorted and unique")


def _sorted_unique_nonempty(values: tuple[str, ...], name: str) -> None:
    if any(not item or item != item.strip() for item in values):
        raise ValueError(f"{name} must contain non-empty trimmed strings")
    _sorted_unique(values, name)


def _object(value: object, name: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise TypeError(f"{name} must be an object")
    result: dict[str, object] = {}
    for key, item in cast(dict[object, object], value).items():
        if not isinstance(key, str):
            raise TypeError(f"{name} field names must be strings")
        result[key] = item
    return result


def _exact_keys(payload: dict[str, object], fields: frozenset[str], name: str) -> None:
    unknown = sorted(set(payload) - fields)
    missing = sorted(fields - set(payload))
    if unknown:
        raise ValueError(f"{name} has unknown fields: {', '.join(unknown)}")
    if missing:
        raise ValueError(f"{name} is missing fields: {', '.join(missing)}")


def _string(payload: dict[str, object], name: str) -> str:
    value = payload.get(name)
    if not isinstance(value, str):
        raise TypeError(f"{name} must be a string")
    return value


def _bool(payload: dict[str, object], name: str) -> bool:
    value = payload.get(name)
    if not isinstance(value, bool):
        raise TypeError(f"{name} must be a boolean")
    return value


def _integer(payload: dict[str, object], name: str) -> int:
    value = payload.get(name)
    if not isinstance(value, int) or isinstance(value, bool):
        raise TypeError(f"{name} must be an integer")
    return value


def _datetime(value: object, name: str) -> datetime:
    if not isinstance(value, str):
        raise TypeError(f"{name} must be a timestamp string")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise ValueError(f"{name} must be an ISO-8601 timestamp") from error
    _strict_utc(parsed, name)
    return parsed


def _optional_datetime(value: object, name: str) -> datetime | None:
    return None if value is None else _datetime(value, name)


def _string_list(value: object, name: str) -> tuple[str, ...]:
    if not isinstance(value, list):
        raise TypeError(f"{name} must be an array")
    result: list[str] = []
    for item in cast(list[object], value):
        if not isinstance(item, str):
            raise TypeError(f"{name} items must be strings")
        result.append(item)
    return tuple(result)


def _array(value: object, name: str) -> list[object]:
    if not isinstance(value, list):
        raise TypeError(f"{name} must be an array or null")
    return cast(list[object], value)


def _decimal(value: object, name: str) -> Decimal:
    if not isinstance(value, str):
        raise TypeError(f"{name} must be a decimal string")
    try:
        parsed = Decimal(value)
    except InvalidOperation as error:
        raise ValueError(f"{name} must be a decimal string") from error
    _finite_decimal(parsed, name)
    return parsed


def _cash_from_optional(value: object, name: str) -> tuple[CashBalance, ...] | None:
    if value is None:
        return None
    return tuple(_cash_from_dict(item) for item in _array(value, name))


def _positions_from_optional(value: object, name: str) -> tuple[AccountPosition, ...] | None:
    if value is None:
        return None
    return tuple(_position_from_dict(item) for item in _array(value, name))


def _open_orders_from_optional(value: object, name: str) -> tuple[OpenOrder, ...] | None:
    if value is None:
        return None
    return tuple(_open_order_from_dict(item) for item in _array(value, name))


def _recent_fills_from_optional(value: object, name: str) -> tuple[RecentFill, ...] | None:
    if value is None:
        return None
    return tuple(_recent_fill_from_dict(item) for item in _array(value, name))


def _cash_from_dict(value: object) -> CashBalance:
    payload = _object(value, "cash balance")
    _exact_keys(payload, _CASH_FIELDS, "cash balance")
    return CashBalance(
        currency=_string(payload, "currency"),
        available=_decimal(payload.get("available"), "cash available"),
        settled=_decimal(payload.get("settled"), "cash settled"),
    )


def _position_from_dict(value: object) -> AccountPosition:
    payload = _object(value, "position")
    _exact_keys(payload, _POSITION_FIELDS, "position")
    concentration_value = payload.get("concentration")
    concentration = (
        None
        if concentration_value is None
        else _decimal(concentration_value, "position concentration")
    )
    gap_value = payload.get("concentration_gap")
    if gap_value is not None and not isinstance(gap_value, str):
        raise TypeError("position concentration_gap must be a string or null")
    return AccountPosition(
        target_id=_string(payload, "target_id"),
        venue=_string(payload, "venue"),
        instrument_class=_string(payload, "instrument_class"),
        side=Side(_string(payload, "side")),
        quantity=_decimal(payload.get("quantity"), "position quantity"),
        concentration=concentration,
        concentration_gap=gap_value,
    )


def _open_order_from_dict(value: object) -> OpenOrder:
    payload = _object(value, "open order")
    _exact_keys(payload, _OPEN_ORDER_FIELDS, "open order")
    return OpenOrder(
        order_reference=_string(payload, "order_reference"),
        target_id=_string(payload, "target_id"),
        venue=_string(payload, "venue"),
        instrument_class=_string(payload, "instrument_class"),
        side=Side(_string(payload, "side")),
        quantity=_decimal(payload.get("quantity"), "open order quantity"),
        status=OpenOrderStatus(_string(payload, "status")),
        submitted_at=_datetime(payload.get("submitted_at"), "open order submitted_at"),
    )


def _recent_fill_from_dict(value: object) -> RecentFill:
    payload = _object(value, "recent fill")
    _exact_keys(payload, _RECENT_FILL_FIELDS, "recent fill")
    return RecentFill(
        fill_reference=_string(payload, "fill_reference"),
        order_reference=_string(payload, "order_reference"),
        target_id=_string(payload, "target_id"),
        venue=_string(payload, "venue"),
        instrument_class=_string(payload, "instrument_class"),
        side=Side(_string(payload, "side")),
        quantity=_decimal(payload.get("quantity"), "recent fill quantity"),
        filled_at=_datetime(payload.get("filled_at"), "recent fill filled_at"),
    )


_CASH_FIELDS = frozenset({"currency", "available", "settled"})
_POSITION_FIELDS = frozenset(
    {
        "target_id",
        "venue",
        "instrument_class",
        "side",
        "quantity",
        "concentration",
        "concentration_gap",
    }
)
_OPEN_ORDER_FIELDS = frozenset(
    {
        "order_reference",
        "target_id",
        "venue",
        "instrument_class",
        "side",
        "quantity",
        "status",
        "submitted_at",
    }
)
_RECENT_FILL_FIELDS = frozenset(
    {
        "fill_reference",
        "order_reference",
        "target_id",
        "venue",
        "instrument_class",
        "side",
        "quantity",
        "filled_at",
    }
)
_ACCOUNT_STATE_FIELDS = frozenset(
    {
        "schema_version",
        "snapshot_id",
        "account_reference_hash",
        "environment",
        "provider_id",
        "provider_version",
        "provider_manifest_hash",
        "as_of",
        "reconciled_at",
        "reconciliation_reference",
        "cash",
        "positions",
        "open_orders",
        "recent_fills",
        "recent_fills_since",
        "missing_sections",
        "reconciliation_gaps",
        "complete",
    }
)
_POSITION_SNAPSHOT_FIELDS = frozenset(
    {
        "schema_version",
        "snapshot_id",
        "account_state_snapshot_id",
        "account_reference_hash",
        "environment",
        "provider_id",
        "as_of",
        "reconciled_at",
        "evaluated_at",
        "max_age_seconds",
        "cash",
        "positions",
        "open_orders",
        "recent_fills",
        "recent_fills_since",
        "complete",
        "observation_gaps",
        "risk_observation_ready",
        "exposure_increase_ready",
    }
)
