from __future__ import annotations

import os
import re
import sqlite3
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from typing import Any, Protocol, cast

from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.domain import (
    ExecutionReceipt,
    ExecutionStatus,
    OrderIntent,
    TradingEnvironment,
    require_aware,
)


class Capability(StrEnum):
    MARKET_DATA = "market_data"
    PAPER_EXECUTION = "paper_execution"
    LIVE_EXECUTION = "live_execution"
    ACCOUNT = "account"


class ProviderTransport(StrEnum):
    NATIVE = "native"
    MCP = "mcp"
    HTTP = "http"
    GRPC = "grpc"


class TrustTier(StrEnum):
    UNVERIFIED = "unverified"
    MOCK = "mock"
    PAPER_VALIDATED = "paper_validated"
    LIVE_VALIDATED = "live_validated"


@dataclass(frozen=True, slots=True)
class ProviderManifest:
    schema_version: str
    provider_id: str
    provider_version: str
    transport: ProviderTransport
    environments: frozenset[TradingEnvironment]
    declared_capabilities: frozenset[Capability]
    verified_capabilities: frozenset[Capability]
    markets: tuple[str, ...]
    order_types: tuple[str, ...]
    supports_streaming: bool
    supports_reconciliation: bool
    enabled: bool
    trust_tier: TrustTier

    def validation_errors(self) -> tuple[str, ...]:
        errors: list[str] = []
        if self.schema_version != "market-impact.provider-manifest.v1":
            errors.append("unsupported provider manifest schema_version")
        if not re.fullmatch(r"[a-z0-9][a-z0-9._-]*", self.provider_id):
            errors.append(
                "provider_id must use lowercase letters, digits, dot, dash, or underscore"
            )
        if not self.provider_version:
            errors.append("provider_version is required")
        if any(order_type not in {"market", "limit"} for order_type in self.order_types):
            errors.append("order_types contains an unsupported order type")
        if not self.verified_capabilities <= self.declared_capabilities:
            errors.append("verified_capabilities must be a subset of declared_capabilities")
        if Capability.PAPER_EXECUTION in self.verified_capabilities:
            if TradingEnvironment.PAPER not in self.environments:
                errors.append("verified paper_execution requires the paper environment")
            if self.trust_tier is TrustTier.UNVERIFIED:
                errors.append("verified paper_execution requires a validated trust tier")
            if not self.supports_reconciliation:
                errors.append("verified paper_execution requires reconciliation")
        if Capability.LIVE_EXECUTION in self.verified_capabilities:
            if TradingEnvironment.LIVE not in self.environments:
                errors.append("verified live_execution requires the live environment")
            if self.trust_tier is not TrustTier.LIVE_VALIDATED:
                errors.append("verified live_execution requires live_validated trust")
            if not self.supports_streaming:
                errors.append("verified live_execution requires streaming order events")
            if not self.supports_reconciliation:
                errors.append("verified live_execution requires reconciliation")
        return tuple(errors)

    def assert_valid(self) -> None:
        errors = self.validation_errors()
        if errors:
            raise ValueError("; ".join(errors))

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "provider_id": self.provider_id,
            "provider_version": self.provider_version,
            "transport": self.transport.value,
            "environments": sorted(item.value for item in self.environments),
            "declared_capabilities": sorted(item.value for item in self.declared_capabilities),
            "verified_capabilities": sorted(item.value for item in self.verified_capabilities),
            "markets": list(self.markets),
            "order_types": list(self.order_types),
            "supports_streaming": self.supports_streaming,
            "supports_reconciliation": self.supports_reconciliation,
            "enabled": self.enabled,
            "trust_tier": self.trust_tier.value,
        }

    @classmethod
    def from_dict(cls, payload: object) -> ProviderManifest:
        fields = _strict_manifest_fields(payload)
        manifest = cls(
            schema_version=_string_field(fields, "schema_version"),
            provider_id=_string_field(fields, "provider_id"),
            provider_version=_string_field(fields, "provider_version"),
            transport=ProviderTransport(_string_field(fields, "transport")),
            environments=frozenset(
                TradingEnvironment(item) for item in _string_array(fields, "environments")
            ),
            declared_capabilities=frozenset(
                Capability(item) for item in _string_array(fields, "declared_capabilities")
            ),
            verified_capabilities=frozenset(
                Capability(item) for item in _string_array(fields, "verified_capabilities")
            ),
            markets=_string_array(fields, "markets", require_nonempty_items=True),
            order_types=_string_array(fields, "order_types"),
            supports_streaming=_bool_field(fields, "supports_streaming"),
            supports_reconciliation=_bool_field(fields, "supports_reconciliation"),
            enabled=_bool_field(fields, "enabled"),
            trust_tier=TrustTier(_string_field(fields, "trust_tier")),
        )
        manifest.assert_valid()
        return manifest


_MANIFEST_FIELDS = frozenset(
    {
        "schema_version",
        "provider_id",
        "provider_version",
        "transport",
        "environments",
        "declared_capabilities",
        "verified_capabilities",
        "markets",
        "order_types",
        "supports_streaming",
        "supports_reconciliation",
        "enabled",
        "trust_tier",
    }
)


def _strict_manifest_fields(payload: object) -> dict[str, object]:
    if not isinstance(payload, dict):
        raise TypeError("provider manifest must be a JSON object")
    raw_fields = cast(dict[object, object], payload)
    fields: dict[str, object] = {}
    for key, value in raw_fields.items():
        if not isinstance(key, str):
            raise TypeError("provider manifest field names must be strings")
        fields[key] = value
    missing = sorted(_MANIFEST_FIELDS - fields.keys())
    unknown = sorted(fields.keys() - _MANIFEST_FIELDS)
    if missing:
        raise ValueError(f"provider manifest missing fields: {', '.join(missing)}")
    if unknown:
        raise ValueError(f"provider manifest has unknown fields: {', '.join(unknown)}")
    return fields


def _string_field(fields: dict[str, object], name: str) -> str:
    value = fields[name]
    if not isinstance(value, str):
        raise TypeError(f"{name} must be a string")
    if name in {"provider_id", "provider_version"} and not value:
        raise ValueError(f"{name} must not be empty")
    return value


def _bool_field(fields: dict[str, object], name: str) -> bool:
    value = fields[name]
    if not isinstance(value, bool):
        raise TypeError(f"{name} must be a boolean")
    return value


def _string_array(
    fields: dict[str, object],
    name: str,
    *,
    require_nonempty_items: bool = False,
) -> tuple[str, ...]:
    value = fields[name]
    if not isinstance(value, list):
        raise TypeError(f"{name} must be an array")
    raw_items = cast(list[object], value)
    items: list[str] = []
    for item in raw_items:
        if not isinstance(item, str):
            raise TypeError(f"{name} items must be strings")
        if require_nonempty_items and not item:
            raise ValueError(f"{name} items must not be empty")
        items.append(item)
    if len(items) != len(set(items)):
        raise ValueError(f"{name} items must be unique")
    return tuple(items)


_SUBMISSION_SEAL = object()


@dataclass(frozen=True, slots=True)
class SubmissionCapability:
    """A provider input issued only after the harness approves an exact intent."""

    order: OrderIntent
    submission_id: str
    order_hash: str
    mandate_hash: str
    price_basis_hash: str
    policy_evaluation_hash: str
    approval_hash: str
    _seal: object

    def __post_init__(self) -> None:
        if self._seal is not _SUBMISSION_SEAL:
            raise TypeError("submission capability must be issued by the execution harness")


def _issue_submission_capability(  # pyright: ignore[reportUnusedFunction]
    *,
    order: OrderIntent,
    submission_id: str,
    order_hash: str,
    mandate_hash: str,
    price_basis_hash: str,
    policy_evaluation_hash: str,
    approval_hash: str,
) -> SubmissionCapability:
    """Issue an exact-binding capability from trusted harness composition code."""

    if not submission_id or submission_id != submission_id.strip():
        raise ValueError("submission_id must be non-empty")
    hashes = (
        order_hash,
        mandate_hash,
        price_basis_hash,
        policy_evaluation_hash,
        approval_hash,
    )
    if any(re.fullmatch(r"[0-9a-f]{64}", value) is None for value in hashes):
        raise ValueError("submission capability bindings must be SHA-256 hashes")
    return SubmissionCapability(
        order=order,
        submission_id=submission_id,
        order_hash=order_hash,
        mandate_hash=mandate_hash,
        price_basis_hash=price_basis_hash,
        policy_evaluation_hash=policy_evaluation_hash,
        approval_hash=approval_hash,
        _seal=_SUBMISSION_SEAL,
    )


@dataclass(frozen=True, slots=True)
class ReconciliationSnapshot:
    provider_id: str
    snapshot_id: str
    observed_at: datetime
    complete: bool
    receipts: tuple[ExecutionReceipt, ...]
    gaps: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        require_aware(self.observed_at, "observed_at")
        if self.snapshot_id != f"provider-reconciliation-{canonical_hash(self.core_dict())}":
            raise ValueError("provider reconciliation snapshot_id does not match content")

    def core_dict(self) -> dict[str, object]:
        return {
            "provider_id": self.provider_id,
            "observed_at": self.observed_at.astimezone(UTC).isoformat().replace("+00:00", "Z"),
            "complete": self.complete,
            "receipts": [
                {
                    "client_order_id": receipt.client_order_id,
                    "provider_order_id": receipt.provider_order_id,
                    "status": receipt.status.value,
                    "observed_at": receipt.observed_at.astimezone(UTC)
                    .isoformat()
                    .replace("+00:00", "Z"),
                }
                for receipt in self.receipts
            ],
            "gaps": list(self.gaps),
        }

    @classmethod
    def build(
        cls,
        *,
        provider_id: str,
        observed_at: datetime,
        complete: bool,
        receipts: tuple[ExecutionReceipt, ...],
        gaps: tuple[str, ...] = (),
    ) -> ReconciliationSnapshot:
        core = {
            "provider_id": provider_id,
            "observed_at": observed_at.astimezone(UTC).isoformat().replace("+00:00", "Z"),
            "complete": complete,
            "receipts": [
                {
                    "client_order_id": receipt.client_order_id,
                    "provider_order_id": receipt.provider_order_id,
                    "status": receipt.status.value,
                    "observed_at": receipt.observed_at.astimezone(UTC)
                    .isoformat()
                    .replace("+00:00", "Z"),
                }
                for receipt in receipts
            ],
            "gaps": list(gaps),
        }
        return cls(
            provider_id=provider_id,
            snapshot_id=f"provider-reconciliation-{canonical_hash(core)}",
            observed_at=observed_at,
            complete=complete,
            receipts=receipts,
            gaps=gaps,
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": "market-impact.provider-reconciliation-snapshot.v1",
            "provider_id": self.provider_id,
            "snapshot_id": self.snapshot_id,
            "observed_at": self.observed_at.astimezone(UTC).isoformat().replace("+00:00", "Z"),
            "complete": self.complete,
            "receipts": [
                {
                    "client_order_id": receipt.client_order_id,
                    "provider_order_id": receipt.provider_order_id,
                    "status": receipt.status.value,
                    "observed_at": receipt.observed_at.astimezone(UTC)
                    .isoformat()
                    .replace("+00:00", "Z"),
                }
                for receipt in self.receipts
            ],
            "gaps": list(self.gaps),
        }


class ExecutionProvider(Protocol):
    @property
    def manifest(self) -> ProviderManifest: ...

    def submit(self, capability: SubmissionCapability) -> ExecutionReceipt: ...

    def reconcile(self) -> ReconciliationSnapshot: ...

    def bind_submission_validator(
        self,
        validator: Callable[[SubmissionCapability], bool],
    ) -> None: ...


class SubmissionCapabilityRejected(PermissionError):
    """The Provider rejected Harness authority before any external mutation."""


class MockExecutionProvider:
    """An idempotent paper-only provider used to verify the harness boundary."""

    def __init__(
        self,
        state_path: Path | None = None,
        *,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._receipts: dict[str, ExecutionReceipt] = {}
        self._order_hashes: dict[str, str] = {}
        self._submission_validator: Callable[[SubmissionCapability], bool] | None = None
        self._state_path = state_path.resolve() if state_path is not None else None
        self._clock = clock or (lambda: datetime.now(UTC))
        if self._state_path is not None:
            self._state_path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
            with self._connect() as connection:
                connection.execute(
                    """
                    CREATE TABLE IF NOT EXISTS mock_execution_receipts (
                        client_order_id TEXT PRIMARY KEY,
                        order_hash TEXT NOT NULL,
                        provider_order_id TEXT NOT NULL UNIQUE,
                        status TEXT NOT NULL,
                        observed_at TEXT NOT NULL
                    )
                    """
                )
            os.chmod(self._state_path, 0o600)

    @property
    def manifest(self) -> ProviderManifest:
        return ProviderManifest(
            schema_version="market-impact.provider-manifest.v1",
            provider_id="mock-execution",
            provider_version="0.1.0",
            transport=ProviderTransport.NATIVE,
            environments=frozenset({TradingEnvironment.PAPER}),
            declared_capabilities=frozenset({Capability.PAPER_EXECUTION}),
            verified_capabilities=frozenset({Capability.PAPER_EXECUTION}),
            markets=("SYNTHETIC",),
            order_types=("market", "limit"),
            supports_streaming=False,
            supports_reconciliation=True,
            enabled=True,
            trust_tier=TrustTier.MOCK,
        )

    def submit(self, capability: object) -> ExecutionReceipt:
        if not isinstance(capability, SubmissionCapability):
            raise TypeError("provider submission requires a harness-issued capability")
        if self._submission_validator is None or not self._submission_validator(capability):
            raise SubmissionCapabilityRejected(
                "provider submission is not bound to an active durable outbox lease"
            )
        order = capability.order
        if order.environment is not TradingEnvironment.PAPER:
            raise ValueError("mock execution accepts paper orders only")
        if self._state_path is not None:
            return self._submit_durable(capability)
        existing = self._receipts.get(order.client_order_id)
        if existing is not None:
            if self._order_hashes[order.client_order_id] != capability.order_hash:
                raise ValueError("mock provider order identity conflict")
            return existing
        receipt = ExecutionReceipt(
            client_order_id=order.client_order_id,
            provider_order_id=f"mock-{len(self._receipts) + 1:06d}",
            status=ExecutionStatus.ACCEPTED,
            observed_at=order.created_at,
        )
        self._receipts[order.client_order_id] = receipt
        self._order_hashes[order.client_order_id] = capability.order_hash
        return receipt

    def reconcile(self) -> ReconciliationSnapshot:
        if self._state_path is None:
            receipts = tuple(self._receipts[key] for key in sorted(self._receipts))
        else:
            with self._connect() as connection:
                rows = connection.execute(
                    "SELECT * FROM mock_execution_receipts ORDER BY client_order_id"
                ).fetchall()
            receipts = tuple(
                ExecutionReceipt(
                    client_order_id=cast(str, row["client_order_id"]),
                    provider_order_id=cast(str, row["provider_order_id"]),
                    status=ExecutionStatus(cast(str, row["status"])),
                    observed_at=_provider_datetime(cast(str, row["observed_at"])),
                )
                for row in rows
            )
        observed_at = self._clock()
        require_aware(observed_at, "observed_at")
        return ReconciliationSnapshot.build(
            provider_id=self.manifest.provider_id,
            observed_at=observed_at,
            complete=True,
            receipts=receipts,
        )

    def bind_submission_validator(
        self,
        validator: Callable[[SubmissionCapability], bool],
    ) -> None:
        if self._submission_validator is None:
            self._submission_validator = validator

    def _submit_durable(self, capability: SubmissionCapability) -> ExecutionReceipt:
        order = capability.order
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            existing = connection.execute(
                "SELECT * FROM mock_execution_receipts WHERE client_order_id = ?",
                (order.client_order_id,),
            ).fetchone()
            if existing is not None:
                if cast(str, existing["order_hash"]) != capability.order_hash:
                    raise ValueError("mock provider order identity conflict")
                return ExecutionReceipt(
                    client_order_id=cast(str, existing["client_order_id"]),
                    provider_order_id=cast(str, existing["provider_order_id"]),
                    status=ExecutionStatus(cast(str, existing["status"])),
                    observed_at=_provider_datetime(cast(str, existing["observed_at"])),
                )
            count = cast(
                int,
                connection.execute("SELECT COUNT(*) FROM mock_execution_receipts").fetchone()[0],
            )
            receipt = ExecutionReceipt(
                client_order_id=order.client_order_id,
                provider_order_id=f"mock-{count + 1:06d}",
                status=ExecutionStatus.ACCEPTED,
                observed_at=order.created_at,
            )
            connection.execute(
                """
                INSERT INTO mock_execution_receipts (
                    client_order_id, order_hash, provider_order_id, status, observed_at
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (
                    order.client_order_id,
                    capability.order_hash,
                    receipt.provider_order_id,
                    receipt.status.value,
                    receipt.observed_at.astimezone(UTC).isoformat().replace("+00:00", "Z"),
                ),
            )
            return receipt

    def _connect(self) -> sqlite3.Connection:
        if self._state_path is None:
            raise RuntimeError("durable mock state path is not configured")
        connection = sqlite3.connect(self._state_path, timeout=30)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA synchronous=FULL")
        return connection


def _provider_datetime(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    require_aware(parsed, "observed_at")
    return parsed
