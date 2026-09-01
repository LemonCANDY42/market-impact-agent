from __future__ import annotations

import os
import re
import sqlite3
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum
from pathlib import Path
from types import MappingProxyType
from typing import Any, Protocol, cast

from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.domain import (
    ExecutionReceipt,
    ExecutionStatus,
    OrderKind,
    Side,
    TradingEnvironment,
    require_aware,
)
from market_impact_agent.ibkr_nautilus_paper import (
    IBKR_NAUTILUS_PAPER_PROVIDER_ID,
    IBKR_NAUTILUS_PAPER_PROVIDER_VERSION,
)
from market_impact_agent.providers import (
    CancellationCapability,
    CancellationCapabilityRejected,
    CancellationCommandReceipt,
    CancellationCommandStatus,
    Capability,
    ProviderManifest,
    ProviderTransport,
    ReconciliationSnapshot,
    SubmissionCapability,
    SubmissionCapabilityRejected,
    TrustTier,
)

IBKR_NAUTILUS_PAPER_ACCEPTANCE_SCHEMA = "market-impact.ibkr-nautilus-paper-provider-acceptance.v1"
_REQUIRED_ACCEPTANCE_SCENARIOS = frozenset(
    {
        "account_reconciliation",
        "ambiguous_acknowledgement",
        "cancel",
        "disconnect",
        "duplicate_fill",
        "external_order",
        "gateway_restart",
        "partial_fill",
        "process_restart",
        "replace",
        "submit",
    }
)
_HASH = re.compile(r"[0-9a-f]{64}")
_ACCOUNT_REFERENCE_HASH = re.compile(r"account-ref-[0-9a-f]{64}")


@dataclass(frozen=True, slots=True)
class IbkrNautilusPaperProviderAcceptance:
    """Harness-owned evidence that may enable one exact external Paper provider build."""

    acceptance_id: str
    provider_id: str
    provider_version: str
    configuration_hash: str
    account_reference_hash: str
    instrument_routes_hash: str
    markets: tuple[str, ...]
    order_types: tuple[str, ...]
    accepted_scenarios: tuple[str, ...]
    evidence_hashes: tuple[str, ...]
    accepted_at: datetime
    valid_until: datetime
    complete: bool
    gaps: tuple[str, ...]
    schema_version: str = IBKR_NAUTILUS_PAPER_ACCEPTANCE_SCHEMA

    def __post_init__(self) -> None:
        if self.schema_version != IBKR_NAUTILUS_PAPER_ACCEPTANCE_SCHEMA:
            raise ValueError("unsupported IBKR Nautilus Paper acceptance schema")
        if self.provider_id != IBKR_NAUTILUS_PAPER_PROVIDER_ID:
            raise ValueError("acceptance targets another Provider")
        if self.provider_version != IBKR_NAUTILUS_PAPER_PROVIDER_VERSION:
            raise ValueError("acceptance targets another Provider version")
        if _HASH.fullmatch(self.configuration_hash) is None:
            raise ValueError("configuration_hash must be a SHA-256 hash")
        if _ACCOUNT_REFERENCE_HASH.fullmatch(self.account_reference_hash) is None:
            raise ValueError("account_reference_hash must be opaque")
        if _HASH.fullmatch(self.instrument_routes_hash) is None:
            raise ValueError("instrument_routes_hash must be a SHA-256 hash")
        _sorted_unique(self.markets, "acceptance markets")
        _sorted_unique(self.order_types, "acceptance order_types")
        _sorted_unique(self.accepted_scenarios, "acceptance scenarios")
        _sorted_unique(self.evidence_hashes, "acceptance evidence hashes")
        _sorted_unique(self.gaps, "acceptance gaps")
        if any(_HASH.fullmatch(value) is None for value in self.evidence_hashes):
            raise ValueError("acceptance evidence references must be SHA-256 hashes")
        if not self.markets or not self.order_types or not self.evidence_hashes:
            raise ValueError("acceptance requires markets, order types, and evidence")
        if any(item != item.upper() for item in self.markets):
            raise ValueError("acceptance markets must use uppercase canonical identifiers")
        if any(item not in {"market", "limit"} for item in self.order_types):
            raise ValueError("acceptance contains an unsupported order type")
        require_aware(self.accepted_at, "acceptance accepted_at")
        require_aware(self.valid_until, "acceptance valid_until")
        if self.valid_until <= self.accepted_at:
            raise ValueError("acceptance valid_until must be after accepted_at")
        expected_id = "ibkr-nautilus-paper-acceptance-" + canonical_hash(self.core_dict())
        if self.acceptance_id != expected_id:
            raise ValueError("acceptance_id does not match content")

    @property
    def execution_accepted(self) -> bool:
        return (
            self.complete
            and not self.gaps
            and set(self.accepted_scenarios) >= _REQUIRED_ACCEPTANCE_SCENARIOS
        )

    def is_current(self, now: datetime) -> bool:
        require_aware(now, "acceptance evaluation time")
        return self.execution_accepted and self.accepted_at <= now < self.valid_until

    def allows_risk_reduction(self, now: datetime) -> bool:
        """Keep exact-scope cancel available after submit admission expires."""

        require_aware(now, "acceptance evaluation time")
        return self.execution_accepted and self.accepted_at <= now

    def core_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "provider_id": self.provider_id,
            "provider_version": self.provider_version,
            "configuration_hash": self.configuration_hash,
            "account_reference_hash": self.account_reference_hash,
            "instrument_routes_hash": self.instrument_routes_hash,
            "markets": list(self.markets),
            "order_types": list(self.order_types),
            "accepted_scenarios": list(self.accepted_scenarios),
            "evidence_hashes": list(self.evidence_hashes),
            "accepted_at": _timestamp(self.accepted_at),
            "valid_until": _timestamp(self.valid_until),
            "complete": self.complete,
            "gaps": list(self.gaps),
        }

    def to_dict(self) -> dict[str, object]:
        return {"acceptance_id": self.acceptance_id, **self.core_dict()}

    @classmethod
    def build(
        cls,
        *,
        configuration_hash: str,
        account_reference_hash: str,
        instrument_routes_hash: str,
        markets: tuple[str, ...],
        order_types: tuple[str, ...],
        accepted_scenarios: tuple[str, ...],
        evidence_hashes: tuple[str, ...],
        accepted_at: datetime,
        valid_until: datetime,
        complete: bool,
        gaps: tuple[str, ...] = (),
    ) -> IbkrNautilusPaperProviderAcceptance:
        core = {
            "schema_version": IBKR_NAUTILUS_PAPER_ACCEPTANCE_SCHEMA,
            "provider_id": IBKR_NAUTILUS_PAPER_PROVIDER_ID,
            "provider_version": IBKR_NAUTILUS_PAPER_PROVIDER_VERSION,
            "configuration_hash": configuration_hash,
            "account_reference_hash": account_reference_hash,
            "instrument_routes_hash": instrument_routes_hash,
            "markets": list(markets),
            "order_types": list(order_types),
            "accepted_scenarios": list(accepted_scenarios),
            "evidence_hashes": list(evidence_hashes),
            "accepted_at": _timestamp(accepted_at),
            "valid_until": _timestamp(valid_until),
            "complete": complete,
            "gaps": list(gaps),
        }
        return cls(
            acceptance_id="ibkr-nautilus-paper-acceptance-" + canonical_hash(core),
            provider_id=IBKR_NAUTILUS_PAPER_PROVIDER_ID,
            provider_version=IBKR_NAUTILUS_PAPER_PROVIDER_VERSION,
            configuration_hash=configuration_hash,
            account_reference_hash=account_reference_hash,
            instrument_routes_hash=instrument_routes_hash,
            markets=markets,
            order_types=order_types,
            accepted_scenarios=accepted_scenarios,
            evidence_hashes=evidence_hashes,
            accepted_at=accepted_at,
            valid_until=valid_until,
            complete=complete,
            gaps=gaps,
        )

    @classmethod
    def from_dict(cls, payload: object) -> IbkrNautilusPaperProviderAcceptance:
        if not isinstance(payload, dict):
            raise TypeError("IBKR Nautilus Paper acceptance must be an object")
        fields = cast(dict[str, Any], payload)
        expected = {
            "schema_version",
            "acceptance_id",
            "provider_id",
            "provider_version",
            "configuration_hash",
            "account_reference_hash",
            "instrument_routes_hash",
            "markets",
            "order_types",
            "accepted_scenarios",
            "evidence_hashes",
            "accepted_at",
            "valid_until",
            "complete",
            "gaps",
        }
        if set(fields) != expected:
            raise ValueError("IBKR Nautilus Paper acceptance fields are invalid")
        for name in (
            "schema_version",
            "acceptance_id",
            "provider_id",
            "provider_version",
            "configuration_hash",
            "account_reference_hash",
            "instrument_routes_hash",
            "accepted_at",
            "valid_until",
        ):
            if not isinstance(fields[name], str):
                raise TypeError(f"acceptance {name} must be a string")
        if not isinstance(fields["complete"], bool):
            raise TypeError("acceptance complete must be a boolean")
        return cls(
            schema_version=cast(str, fields["schema_version"]),
            acceptance_id=cast(str, fields["acceptance_id"]),
            provider_id=cast(str, fields["provider_id"]),
            provider_version=cast(str, fields["provider_version"]),
            configuration_hash=cast(str, fields["configuration_hash"]),
            account_reference_hash=cast(str, fields["account_reference_hash"]),
            instrument_routes_hash=cast(str, fields["instrument_routes_hash"]),
            markets=_string_tuple(fields["markets"], "acceptance markets"),
            order_types=_string_tuple(fields["order_types"], "acceptance order_types"),
            accepted_scenarios=_string_tuple(
                fields["accepted_scenarios"],
                "acceptance scenarios",
            ),
            evidence_hashes=_string_tuple(
                fields["evidence_hashes"],
                "acceptance evidence hashes",
            ),
            accepted_at=_datetime(cast(str, fields["accepted_at"])),
            valid_until=_datetime(cast(str, fields["valid_until"])),
            complete=fields["complete"],
            gaps=_string_tuple(fields["gaps"], "acceptance gaps"),
        )


@dataclass(frozen=True, slots=True)
class IbkrNautilusInstrumentRoute:
    nautilus_instrument_id: str
    market: str

    def __post_init__(self) -> None:
        if not self.nautilus_instrument_id or (
            self.nautilus_instrument_id != self.nautilus_instrument_id.strip()
        ):
            raise ValueError("Nautilus instrument identity must be non-empty and trimmed")
        if not self.market or self.market != self.market.strip().upper():
            raise ValueError("instrument market must be an uppercase canonical identifier")


class NautilusPaperRuntimeStatus(StrEnum):
    ACCEPTED = "accepted"
    PENDING_CANCEL = "pending_cancel"
    CANCELED = "canceled"
    PARTIALLY_FILLED = "partially_filled"
    FILLED = "filled"
    REJECTED = "rejected"
    EXPIRED = "expired"
    UNKNOWN = "unknown"


@dataclass(frozen=True, slots=True)
class NautilusPaperSubmitCommand:
    submission_id: str
    nautilus_client_order_id: str
    instrument_id: str
    side: Side
    quantity: Decimal
    order_kind: OrderKind
    limit_price: Decimal | None
    created_at: datetime
    expires_at: datetime

    def __post_init__(self) -> None:
        for value in (self.submission_id, self.nautilus_client_order_id, self.instrument_id):
            if not value or value != value.strip():
                raise ValueError("Nautilus submit command strings must be non-empty and trimmed")
        if not self.quantity.is_finite() or self.quantity <= 0:
            raise ValueError("Nautilus submit quantity must be finite and positive")
        if (self.order_kind is OrderKind.LIMIT) != (self.limit_price is not None):
            raise ValueError("Nautilus limit price does not match order kind")
        require_aware(self.created_at, "Nautilus submit created_at")
        require_aware(self.expires_at, "Nautilus submit expires_at")


@dataclass(frozen=True, slots=True)
class NautilusPaperCancelCommand:
    cancellation_id: str
    nautilus_client_order_id: str
    provider_order_id: str

    def __post_init__(self) -> None:
        for value in (
            self.cancellation_id,
            self.nautilus_client_order_id,
            self.provider_order_id,
        ):
            if not value or value != value.strip():
                raise ValueError("Nautilus cancel command strings must be non-empty and trimmed")


@dataclass(frozen=True, slots=True)
class NautilusPaperOrderObservation:
    nautilus_client_order_id: str
    provider_order_id: str | None
    status: NautilusPaperRuntimeStatus
    observed_at: datetime
    filled_quantity: Decimal = Decimal(0)
    fill_ids: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not self.nautilus_client_order_id or (
            self.nautilus_client_order_id != self.nautilus_client_order_id.strip()
        ):
            raise ValueError("Nautilus observation client order identity is invalid")
        if self.provider_order_id is not None and (
            not self.provider_order_id or self.provider_order_id != self.provider_order_id.strip()
        ):
            raise ValueError("Nautilus observation provider order identity is invalid")
        require_aware(self.observed_at, "Nautilus observation time")
        if not self.filled_quantity.is_finite() or self.filled_quantity < 0:
            raise ValueError("Nautilus filled quantity must be finite and non-negative")
        if len(set(self.fill_ids)) != len(self.fill_ids) or any(
            not fill_id or fill_id != fill_id.strip() for fill_id in self.fill_ids
        ):
            raise ValueError("Nautilus fill identities must be unique, non-empty strings")
        object.__setattr__(self, "fill_ids", tuple(sorted(self.fill_ids)))
        if (self.filled_quantity > 0) != bool(self.fill_ids):
            raise ValueError("Nautilus fill quantity and identities must be present together")
        if (
            self.status
            in {
                NautilusPaperRuntimeStatus.ACCEPTED,
                NautilusPaperRuntimeStatus.PENDING_CANCEL,
                NautilusPaperRuntimeStatus.CANCELED,
                NautilusPaperRuntimeStatus.PARTIALLY_FILLED,
                NautilusPaperRuntimeStatus.FILLED,
            }
            and self.provider_order_id is None
        ):
            raise ValueError("broker-observed order state requires provider_order_id")


@dataclass(frozen=True, slots=True)
class NautilusPaperRuntimeSnapshot:
    observed_at: datetime
    connected: bool
    reconciled: bool
    complete: bool
    orders: tuple[NautilusPaperOrderObservation, ...]
    gaps: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        require_aware(self.observed_at, "Nautilus runtime snapshot time")
        _sorted_unique(self.gaps, "Nautilus runtime gaps")


class NautilusPaperExecutionRuntime(Protocol):
    @property
    def configuration_hash(self) -> str: ...

    @property
    def account_reference_hash(self) -> str: ...

    def submit(self, command: NautilusPaperSubmitCommand) -> NautilusPaperOrderObservation: ...

    def cancel(self, command: NautilusPaperCancelCommand) -> NautilusPaperOrderObservation: ...

    def reconcile(self) -> NautilusPaperRuntimeSnapshot: ...


class IbkrNautilusPaperExecutionProvider:
    """Fail-closed Harness adapter over one long-lived Nautilus-to-IBKR Paper runtime."""

    def __init__(
        self,
        state_path: Path,
        *,
        runtime: NautilusPaperExecutionRuntime,
        instrument_routes: Mapping[str, IbkrNautilusInstrumentRoute],
        acceptance: IbkrNautilusPaperProviderAcceptance | None = None,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        routes = dict(instrument_routes)
        if not routes or any(not key or key != key.strip() for key in routes):
            raise ValueError("IBKR Nautilus instrument routes must use explicit Harness IDs")
        if len({route.nautilus_instrument_id for route in routes.values()}) != len(routes):
            raise ValueError("IBKR Nautilus instrument identities must be one-to-one")
        self._state_path = state_path.resolve()
        self._state_path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        self._runtime = runtime
        if _HASH.fullmatch(runtime.configuration_hash) is None:
            raise ValueError("Nautilus runtime configuration_hash must be a SHA-256 hash")
        if _ACCOUNT_REFERENCE_HASH.fullmatch(runtime.account_reference_hash) is None:
            raise ValueError("Nautilus runtime account reference must be opaque")
        self._instrument_routes = MappingProxyType(routes)
        self._instrument_routes_hash = hash_ibkr_nautilus_instrument_routes(routes)
        self._acceptance = acceptance
        self._clock = clock or (lambda: datetime.now(UTC))
        self._submission_validator: Callable[[SubmissionCapability], bool] | None = None
        self._cancellation_validator: Callable[[CancellationCapability], bool] | None = None
        with self._connect() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS ibkr_nautilus_order_bindings (
                    client_order_id TEXT PRIMARY KEY,
                    order_hash TEXT NOT NULL,
                    submission_id TEXT NOT NULL UNIQUE,
                    nautilus_client_order_id TEXT NOT NULL UNIQUE,
                    acceptance_id TEXT NOT NULL,
                    configuration_hash TEXT NOT NULL,
                    account_reference_hash TEXT NOT NULL,
                    instrument_routes_hash TEXT NOT NULL,
                    market TEXT NOT NULL,
                    order_type TEXT NOT NULL,
                    provider_order_id TEXT,
                    dispatch_state TEXT NOT NULL,
                    accepted_at TEXT
                );
                CREATE TABLE IF NOT EXISTS ibkr_nautilus_cancel_bindings (
                    cancellation_id TEXT PRIMARY KEY,
                    client_order_id TEXT NOT NULL,
                    provider_order_id TEXT NOT NULL,
                    request_hash TEXT NOT NULL,
                    acceptance_id TEXT NOT NULL,
                    configuration_hash TEXT NOT NULL,
                    account_reference_hash TEXT NOT NULL,
                    instrument_routes_hash TEXT NOT NULL,
                    dispatch_state TEXT NOT NULL,
                    observed_at TEXT
                );
                """
            )
        os.chmod(self._state_path, 0o600)

    @property
    def manifest(self) -> ProviderManifest:
        now = self._clock()
        require_aware(now, "provider manifest evaluation time")
        operational = (
            self._acceptance is not None
            and self._acceptance.allows_risk_reduction(now)
            and self._acceptance_scope_matches(self._acceptance)
        )
        acceptance = self._acceptance
        markets = ("US", "HK") if acceptance is None else acceptance.markets
        order_types = ("market", "limit") if acceptance is None else acceptance.order_types
        return ProviderManifest(
            schema_version="market-impact.provider-manifest.v1",
            provider_id=IBKR_NAUTILUS_PAPER_PROVIDER_ID,
            provider_version=IBKR_NAUTILUS_PAPER_PROVIDER_VERSION,
            transport=ProviderTransport.NATIVE,
            environments=frozenset({TradingEnvironment.PAPER}),
            declared_capabilities=frozenset({Capability.PAPER_EXECUTION}),
            verified_capabilities=(
                frozenset({Capability.PAPER_EXECUTION}) if operational else frozenset()
            ),
            markets=markets,
            order_types=order_types,
            supports_streaming=True,
            supports_reconciliation=True,
            enabled=operational,
            trust_tier=TrustTier.PAPER_VALIDATED if operational else TrustTier.UNVERIFIED,
        )

    @property
    def new_order_admission_open(self) -> bool:
        now = self._clock()
        require_aware(now, "provider new-order admission evaluation time")
        return (
            self._acceptance is not None
            and self._acceptance.is_current(now)
            and self._acceptance_scope_matches(self._acceptance)
        )

    def bind_submission_validator(
        self,
        validator: Callable[[SubmissionCapability], bool],
    ) -> None:
        if self._submission_validator is None:
            self._submission_validator = validator

    def bind_cancellation_validator(
        self,
        validator: Callable[[CancellationCapability], bool],
    ) -> None:
        if self._cancellation_validator is None:
            self._cancellation_validator = validator

    def submit(self, capability: SubmissionCapability) -> ExecutionReceipt:
        self._validate_submission(capability)
        existing, should_dispatch = self._prepare_submission(capability)
        if not should_dispatch:
            provider_order_id = cast(str | None, existing["provider_order_id"])
            accepted_at = cast(str | None, existing["accepted_at"])
            if (
                cast(str, existing["dispatch_state"]) == "accepted"
                and provider_order_id is not None
                and accepted_at is not None
            ):
                return ExecutionReceipt(
                    client_order_id=capability.order.client_order_id,
                    provider_order_id=provider_order_id,
                    status=ExecutionStatus.ACCEPTED,
                    observed_at=_datetime(accepted_at),
                )
            raise RuntimeError("earlier Nautilus submission outcome is ambiguous; reconcile only")
        observation = self._runtime.submit(
            NautilusPaperSubmitCommand(
                submission_id=capability.submission_id,
                nautilus_client_order_id=cast(str, existing["nautilus_client_order_id"]),
                instrument_id=self._instrument_routes[
                    capability.order.instrument_id
                ].nautilus_instrument_id,
                side=capability.order.side,
                quantity=capability.order.quantity,
                order_kind=capability.order.order_kind,
                limit_price=capability.order.limit_price,
                created_at=capability.order.created_at,
                expires_at=capability.order.expires_at,
            )
        )
        if observation.nautilus_client_order_id != cast(str, existing["nautilus_client_order_id"]):
            raise RuntimeError("Nautilus submission observation identity mismatch")
        if (
            observation.status is not NautilusPaperRuntimeStatus.ACCEPTED
            or observation.provider_order_id is None
            or observation.filled_quantity != 0
            or observation.fill_ids
        ):
            raise RuntimeError("Nautilus submission did not produce an accepted-without-fill event")
        self._record_submission_acceptance(
            capability.order.client_order_id,
            provider_order_id=observation.provider_order_id,
            observed_at=observation.observed_at,
        )
        return ExecutionReceipt(
            client_order_id=capability.order.client_order_id,
            provider_order_id=observation.provider_order_id,
            status=ExecutionStatus.ACCEPTED,
            observed_at=observation.observed_at,
        )

    def cancel(self, capability: CancellationCapability) -> CancellationCommandReceipt:
        self._validate_cancellation(capability)
        binding = self._order_binding(capability.client_order_id)
        if (
            binding is None
            or not self._binding_scope_matches(binding)
            or cast(str | None, binding["provider_order_id"]) != capability.provider_order_id
        ):
            raise CancellationCapabilityRejected("cancellation target is not an exact known order")
        existing, should_dispatch = self._prepare_cancellation(capability)
        if not should_dispatch:
            state = cast(str, existing["dispatch_state"])
            observed_at = cast(str | None, existing["observed_at"])
            if state in {"dispatched", "canceled"} and observed_at is not None:
                return CancellationCommandReceipt(
                    client_order_id=capability.client_order_id,
                    provider_order_id=capability.provider_order_id,
                    cancellation_id=capability.cancellation_id,
                    status=(
                        CancellationCommandStatus.CANCELED
                        if state == "canceled"
                        else CancellationCommandStatus.DISPATCHED
                    ),
                    observed_at=_datetime(observed_at),
                )
            raise RuntimeError("earlier Nautilus cancellation outcome is ambiguous; reconcile only")
        observation = self._runtime.cancel(
            NautilusPaperCancelCommand(
                cancellation_id=capability.cancellation_id,
                nautilus_client_order_id=cast(str, binding["nautilus_client_order_id"]),
                provider_order_id=capability.provider_order_id,
            )
        )
        if (
            observation.nautilus_client_order_id != cast(str, binding["nautilus_client_order_id"])
            or observation.provider_order_id != capability.provider_order_id
        ):
            raise RuntimeError("Nautilus cancellation observation identity mismatch")
        if observation.status not in {
            NautilusPaperRuntimeStatus.PENDING_CANCEL,
            NautilusPaperRuntimeStatus.CANCELED,
        }:
            raise RuntimeError("Nautilus cancellation did not produce a definitive command event")
        status = (
            CancellationCommandStatus.CANCELED
            if observation.status is NautilusPaperRuntimeStatus.CANCELED
            else CancellationCommandStatus.DISPATCHED
        )
        self._record_cancellation(
            capability.cancellation_id,
            status=status,
            observed_at=observation.observed_at,
        )
        return CancellationCommandReceipt(
            client_order_id=capability.client_order_id,
            provider_order_id=capability.provider_order_id,
            cancellation_id=capability.cancellation_id,
            status=status,
            observed_at=observation.observed_at,
        )

    def reconcile(self) -> ReconciliationSnapshot:
        runtime = self._runtime.reconcile()
        gaps = list(runtime.gaps)
        if not runtime.connected:
            gaps.append("nautilus_runtime_disconnected")
        if not runtime.reconciled:
            gaps.append("nautilus_execution_not_reconciled")
        bindings = self._all_order_bindings()
        by_nautilus_id = {cast(str, row["nautilus_client_order_id"]): row for row in bindings}
        observations: dict[str, NautilusPaperOrderObservation] = {}
        for observation in runtime.orders:
            if observation.nautilus_client_order_id in observations:
                gaps.append(f"duplicate_nautilus_order:{observation.nautilus_client_order_id}")
            observations[observation.nautilus_client_order_id] = observation
        receipts: list[ExecutionReceipt] = []
        for nautilus_id, observation in sorted(observations.items()):
            binding = by_nautilus_id.get(nautilus_id)
            if binding is None:
                gaps.append(f"external_nautilus_order:{nautilus_id}")
                continue
            client_order_id = cast(str, binding["client_order_id"])
            if not self._binding_scope_matches(binding):
                gaps.append(f"order_runtime_scope_mismatch:{client_order_id}")
                continue
            expected_provider_id = cast(str | None, binding["provider_order_id"])
            status = _provider_execution_status(observation.status)
            if expected_provider_id is not None and observation.provider_order_id is None:
                gaps.append(f"provider_order_identity_missing:{client_order_id}")
                continue
            if expected_provider_id is not None and (
                observation.provider_order_id != expected_provider_id
            ):
                gaps.append(f"provider_order_identity_mismatch:{client_order_id}")
                continue
            if observation.provider_order_id is not None:
                self._bind_provider_order_id(
                    client_order_id,
                    provider_order_id=observation.provider_order_id,
                    observed_at=observation.observed_at,
                )
            if status is ExecutionStatus.UNKNOWN:
                gaps.append(
                    f"unsupported_nautilus_order_status:{client_order_id}:{observation.status.value}"
                )
            receipts.append(
                ExecutionReceipt(
                    client_order_id=client_order_id,
                    provider_order_id=observation.provider_order_id,
                    status=status,
                    observed_at=observation.observed_at,
                    filled_quantity=observation.filled_quantity,
                    fill_ids=observation.fill_ids,
                )
            )
        if runtime.complete:
            for row in bindings:
                nautilus_id = cast(str, row["nautilus_client_order_id"])
                if (
                    nautilus_id not in observations
                    and cast(str, row["dispatch_state"]) == "accepted"
                ):
                    gaps.append(f"accepted_nautilus_order_missing:{row['client_order_id']}")
        return ReconciliationSnapshot.build(
            provider_id=IBKR_NAUTILUS_PAPER_PROVIDER_ID,
            observed_at=runtime.observed_at,
            complete=runtime.complete,
            receipts=tuple(sorted(receipts, key=lambda item: item.client_order_id)),
            gaps=tuple(sorted(set(gaps))),
        )

    def _validate_submission(self, capability: object) -> None:
        if not isinstance(capability, SubmissionCapability):
            raise TypeError("provider submission requires a Harness-issued capability")
        if not self.new_order_admission_open:
            raise SubmissionCapabilityRejected("external Paper Provider lacks current acceptance")
        if self._submission_validator is None or not self._submission_validator(capability):
            raise SubmissionCapabilityRejected(
                "provider submission is not bound to an active durable outbox lease"
            )
        if (
            capability.provider_id != IBKR_NAUTILUS_PAPER_PROVIDER_ID
            or capability.provider_version != IBKR_NAUTILUS_PAPER_PROVIDER_VERSION
        ):
            raise SubmissionCapabilityRejected("submission capability targets another Provider")
        if capability.order.environment is not TradingEnvironment.PAPER:
            raise SubmissionCapabilityRejected("IBKR Nautilus Provider accepts Paper orders only")
        route = self._instrument_routes.get(capability.order.instrument_id)
        if route is None:
            raise SubmissionCapabilityRejected(
                "order has no accepted Instrument Master translation"
            )
        acceptance = self._acceptance
        if acceptance is None:  # pragma: no cover - manifest.enabled already rejects
            raise SubmissionCapabilityRejected("external Paper Provider lacks acceptance")
        if route.market not in acceptance.markets:
            raise SubmissionCapabilityRejected("order market is outside accepted Provider scope")
        if capability.order.order_kind.value not in acceptance.order_types:
            raise SubmissionCapabilityRejected("order type is outside accepted Provider scope")

    def _validate_cancellation(self, capability: object) -> None:
        if not isinstance(capability, CancellationCapability):
            raise TypeError("provider cancellation requires a Harness-issued capability")
        now = self._clock()
        require_aware(now, "provider cancellation evaluation time")
        acceptance = self._acceptance
        if (
            acceptance is None
            or not acceptance.allows_risk_reduction(now)
            or not self._acceptance_scope_matches(acceptance)
        ):
            raise CancellationCapabilityRejected(
                "external Paper Provider lacks exact-scope risk-reduction acceptance"
            )
        if self._cancellation_validator is None or not self._cancellation_validator(capability):
            raise CancellationCapabilityRejected(
                "provider cancellation is not bound to an active durable outbox lease"
            )
        if (
            capability.provider_id != IBKR_NAUTILUS_PAPER_PROVIDER_ID
            or capability.provider_version != IBKR_NAUTILUS_PAPER_PROVIDER_VERSION
        ):
            raise CancellationCapabilityRejected("cancellation targets another Provider")

    def _prepare_submission(
        self,
        capability: SubmissionCapability,
    ) -> tuple[sqlite3.Row, bool]:
        nautilus_id = _nautilus_client_order_id(capability)
        acceptance = self._acceptance
        route = self._instrument_routes[capability.order.instrument_id]
        if acceptance is None:  # pragma: no cover - validated before this method
            raise RuntimeError("submission acceptance disappeared")
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT * FROM ibkr_nautilus_order_bindings WHERE client_order_id = ?",
                (capability.order.client_order_id,),
            ).fetchone()
            if row is not None:
                if (
                    cast(str, row["order_hash"]) != capability.order_hash
                    or cast(str, row["nautilus_client_order_id"]) != nautilus_id
                    or cast(str, row["acceptance_id"]) != acceptance.acceptance_id
                    or not self._binding_scope_matches(row)
                    or cast(str, row["market"]) != route.market
                    or cast(str, row["order_type"]) != capability.order.order_kind.value
                ):
                    raise ValueError("IBKR Nautilus order identity or runtime scope conflict")
                return row, False
            connection.execute(
                """
                INSERT INTO ibkr_nautilus_order_bindings (
                    client_order_id, order_hash, submission_id,
                    nautilus_client_order_id, acceptance_id,
                    configuration_hash, account_reference_hash,
                    instrument_routes_hash, market, order_type, provider_order_id,
                    dispatch_state, accepted_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, 'prepared', NULL)
                """,
                (
                    capability.order.client_order_id,
                    capability.order_hash,
                    capability.submission_id,
                    nautilus_id,
                    acceptance.acceptance_id,
                    self._runtime.configuration_hash,
                    self._runtime.account_reference_hash,
                    self._instrument_routes_hash,
                    route.market,
                    capability.order.order_kind.value,
                ),
            )
            row = connection.execute(
                "SELECT * FROM ibkr_nautilus_order_bindings WHERE client_order_id = ?",
                (capability.order.client_order_id,),
            ).fetchone()
            if row is None:  # pragma: no cover - SQLite insert/read invariant
                raise RuntimeError("IBKR Nautilus order binding was not stored")
            return row, True

    def _prepare_cancellation(
        self,
        capability: CancellationCapability,
    ) -> tuple[sqlite3.Row, bool]:
        acceptance = self._acceptance
        if acceptance is None:  # pragma: no cover - validated before this method
            raise RuntimeError("cancellation acceptance disappeared")
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT * FROM ibkr_nautilus_cancel_bindings WHERE cancellation_id = ?",
                (capability.cancellation_id,),
            ).fetchone()
            if row is not None:
                if (
                    cast(str, row["client_order_id"]) != capability.client_order_id
                    or cast(str, row["provider_order_id"]) != capability.provider_order_id
                    or cast(str, row["request_hash"]) != capability.request_hash
                    or not self._binding_scope_matches(row)
                ):
                    raise ValueError(
                        "IBKR Nautilus cancellation identity or runtime scope conflict"
                    )
                return row, False
            connection.execute(
                """
                INSERT INTO ibkr_nautilus_cancel_bindings (
                    cancellation_id, client_order_id, provider_order_id,
                    request_hash, acceptance_id, configuration_hash,
                    account_reference_hash, instrument_routes_hash,
                    dispatch_state, observed_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'prepared', NULL)
                """,
                (
                    capability.cancellation_id,
                    capability.client_order_id,
                    capability.provider_order_id,
                    capability.request_hash,
                    acceptance.acceptance_id,
                    self._runtime.configuration_hash,
                    self._runtime.account_reference_hash,
                    self._instrument_routes_hash,
                ),
            )
            row = connection.execute(
                "SELECT * FROM ibkr_nautilus_cancel_bindings WHERE cancellation_id = ?",
                (capability.cancellation_id,),
            ).fetchone()
            if row is None:  # pragma: no cover - SQLite insert/read invariant
                raise RuntimeError("IBKR Nautilus cancellation binding was not stored")
            return row, True

    def _record_submission_acceptance(
        self,
        client_order_id: str,
        *,
        provider_order_id: str,
        observed_at: datetime,
    ) -> None:
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            updated = connection.execute(
                """
                UPDATE ibkr_nautilus_order_bindings
                SET provider_order_id = ?, dispatch_state = 'accepted', accepted_at = ?
                WHERE client_order_id = ? AND dispatch_state = 'prepared'
                """,
                (provider_order_id, _timestamp(observed_at), client_order_id),
            )
            if updated.rowcount != 1:
                raise RuntimeError("IBKR Nautilus submission binding changed during dispatch")

    def _record_cancellation(
        self,
        cancellation_id: str,
        *,
        status: CancellationCommandStatus,
        observed_at: datetime,
    ) -> None:
        state = "canceled" if status is CancellationCommandStatus.CANCELED else "dispatched"
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            updated = connection.execute(
                """
                UPDATE ibkr_nautilus_cancel_bindings
                SET dispatch_state = ?, observed_at = ?
                WHERE cancellation_id = ? AND dispatch_state = 'prepared'
                """,
                (state, _timestamp(observed_at), cancellation_id),
            )
            if updated.rowcount != 1:
                raise RuntimeError("IBKR Nautilus cancellation binding changed during dispatch")

    def _bind_provider_order_id(
        self,
        client_order_id: str,
        *,
        provider_order_id: str,
        observed_at: datetime,
    ) -> None:
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                """
                SELECT provider_order_id
                FROM ibkr_nautilus_order_bindings
                WHERE client_order_id = ?
                """,
                (client_order_id,),
            ).fetchone()
            if row is None:
                raise RuntimeError("IBKR Nautilus reconciliation binding disappeared")
            existing = cast(str | None, row["provider_order_id"])
            if existing is not None and existing != provider_order_id:
                raise RuntimeError("IBKR Nautilus provider order identity changed")
            connection.execute(
                """
                UPDATE ibkr_nautilus_order_bindings
                SET provider_order_id = ?, accepted_at = COALESCE(accepted_at, ?)
                WHERE client_order_id = ?
                """,
                (provider_order_id, _timestamp(observed_at), client_order_id),
            )

    def _order_binding(self, client_order_id: str) -> sqlite3.Row | None:
        with self._connect() as connection:
            return connection.execute(
                "SELECT * FROM ibkr_nautilus_order_bindings WHERE client_order_id = ?",
                (client_order_id,),
            ).fetchone()

    def _all_order_bindings(self) -> tuple[sqlite3.Row, ...]:
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT * FROM ibkr_nautilus_order_bindings ORDER BY client_order_id"
            ).fetchall()
        return tuple(rows)

    def _acceptance_scope_matches(
        self,
        acceptance: IbkrNautilusPaperProviderAcceptance,
    ) -> bool:
        return (
            acceptance.configuration_hash == self._runtime.configuration_hash
            and acceptance.account_reference_hash == self._runtime.account_reference_hash
            and acceptance.instrument_routes_hash == self._instrument_routes_hash
        )

    def _binding_scope_matches(self, row: sqlite3.Row) -> bool:
        return (
            cast(str, row["configuration_hash"]) == self._runtime.configuration_hash
            and cast(str, row["account_reference_hash"]) == self._runtime.account_reference_hash
            and cast(str, row["instrument_routes_hash"]) == self._instrument_routes_hash
        )

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self._state_path, timeout=30)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA synchronous=FULL")
        return connection


def _nautilus_client_order_id(capability: SubmissionCapability) -> str:
    return (
        "MIA-"
        + canonical_hash(
            {
                "provider_id": capability.provider_id,
                "provider_version": capability.provider_version,
                "client_order_id": capability.order.client_order_id,
                "order_hash": capability.order_hash,
            }
        )[:24]
    )


def hash_ibkr_nautilus_instrument_routes(
    routes: Mapping[str, IbkrNautilusInstrumentRoute],
) -> str:
    return canonical_hash(
        {
            instrument_id: {
                "nautilus_instrument_id": route.nautilus_instrument_id,
                "market": route.market,
            }
            for instrument_id, route in sorted(routes.items())
        }
    )


def _provider_execution_status(status: NautilusPaperRuntimeStatus) -> ExecutionStatus:
    return ExecutionStatus(status.value)


def _sorted_unique(values: tuple[str, ...], name: str) -> None:
    if values != tuple(sorted(set(values))) or any(
        not value or value != value.strip() for value in values
    ):
        raise ValueError(f"{name} must be sorted, unique, non-empty strings")


def _string_tuple(value: object, name: str) -> tuple[str, ...]:
    if not isinstance(value, list):
        raise TypeError(f"{name} must be an array of strings")
    raw = cast(list[object], value)
    if any(not isinstance(item, str) for item in raw):
        raise TypeError(f"{name} must be an array of strings")
    return tuple(cast(list[str], raw))


def _timestamp(value: datetime) -> str:
    require_aware(value, "timestamp")
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _datetime(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    require_aware(parsed, "timestamp")
    return parsed
