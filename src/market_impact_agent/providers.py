from __future__ import annotations

import re
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any, Protocol, cast

from market_impact_agent.domain import (
    ApprovalMode,
    ExecutionReceipt,
    ExecutionStatus,
    HardPolicyOutcome,
    OrderIntent,
    TradingEnvironment,
    TradingMandate,
)
from market_impact_agent.policy import HardPolicyEvaluator


class Capability(StrEnum):
    MARKET_DATA = "market_data"
    BACKTEST = "backtest"
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


def _missing_reference_price(_order: OrderIntent) -> Decimal | None:
    return None


@dataclass(frozen=True, slots=True)
class SubmissionCapability:
    """A provider input issued only after the harness approves an exact intent."""

    order: OrderIntent
    _seal: object

    def __post_init__(self) -> None:
        if self._seal is not _SUBMISSION_SEAL:
            raise TypeError("submission capability must be issued by the execution harness")


class ExecutionProvider(Protocol):
    @property
    def manifest(self) -> ProviderManifest: ...

    def submit(self, capability: SubmissionCapability) -> ExecutionReceipt: ...

    def cancel(self, client_order_id: str) -> ExecutionReceipt: ...

    def reconcile(self) -> tuple[ExecutionReceipt, ...]: ...


class MockExecutionProvider:
    """An idempotent paper-only provider used to verify the harness boundary."""

    def __init__(self) -> None:
        self._receipts: dict[str, ExecutionReceipt] = {}

    @property
    def manifest(self) -> ProviderManifest:
        return ProviderManifest(
            schema_version="market-impact.provider-manifest.v1",
            provider_id="mock-execution",
            provider_version="0.1.0",
            transport=ProviderTransport.NATIVE,
            environments=frozenset({TradingEnvironment.PAPER}),
            declared_capabilities=frozenset({Capability.PAPER_EXECUTION, Capability.ACCOUNT}),
            verified_capabilities=frozenset({Capability.PAPER_EXECUTION, Capability.ACCOUNT}),
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
        order = capability.order
        if order.environment is not TradingEnvironment.PAPER:
            raise ValueError("mock execution accepts paper orders only")
        existing = self._receipts.get(order.client_order_id)
        if existing is not None:
            return existing
        receipt = ExecutionReceipt(
            client_order_id=order.client_order_id,
            provider_order_id=f"mock-{len(self._receipts) + 1:06d}",
            status=ExecutionStatus.ACCEPTED,
            observed_at=order.created_at,
        )
        self._receipts[order.client_order_id] = receipt
        return receipt

    def cancel(self, client_order_id: str) -> ExecutionReceipt:
        existing = self._receipts.get(client_order_id)
        if existing is None:
            raise KeyError(client_order_id)
        canceled = ExecutionReceipt(
            client_order_id=existing.client_order_id,
            provider_order_id=existing.provider_order_id,
            status=ExecutionStatus.CANCELED,
            observed_at=existing.observed_at,
        )
        self._receipts[client_order_id] = canceled
        return canceled

    def reconcile(self) -> tuple[ExecutionReceipt, ...]:
        return tuple(self._receipts[key] for key in sorted(self._receipts))


class PaperExecutionGateway:
    """The only submission path: hard-policy-gated and deliberately paper-only."""

    def __init__(
        self,
        provider: ExecutionProvider,
        mandate: TradingMandate,
        *,
        policy: HardPolicyEvaluator | None = None,
        clock: Callable[[], datetime] | None = None,
        price_source: Callable[[OrderIntent], Decimal | None] | None = None,
    ) -> None:
        provider.manifest.assert_valid()
        self._provider = provider
        self._mandate = mandate
        self._policy = policy or HardPolicyEvaluator()
        self._clock = clock or (lambda: datetime.now(UTC))
        self._price_source = price_source or _missing_reference_price

    def submit(
        self,
        order: OrderIntent,
    ) -> ExecutionReceipt:
        manifest = self._provider.manifest
        if (
            not manifest.enabled
            or Capability.PAPER_EXECUTION not in manifest.verified_capabilities
            or TradingEnvironment.PAPER not in manifest.environments
        ):
            raise PermissionError("provider is not enabled for verified paper execution")
        if order.environment is not TradingEnvironment.PAPER:
            raise PermissionError("execution gateway is paper-only")

        now = self._clock()
        decision = self._policy.evaluate(
            order,
            self._mandate,
            now=now,
            reference_price=self._price_source(order),
        )
        if decision.outcome is not HardPolicyOutcome.ELIGIBLE:
            reasons = ", ".join(decision.reasons)
            raise PermissionError(f"order intent was not approved: {reasons}")
        if self._mandate.approval_mode is ApprovalMode.POLICY_AUTO:
            raise PermissionError("semantic auto approval is not implemented")

        capability = SubmissionCapability(order=order, _seal=_SUBMISSION_SEAL)
        return self._provider.submit(capability)
