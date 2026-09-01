from __future__ import annotations

import fcntl
import json
import os
import sqlite3
import uuid
from collections.abc import Callable, Mapping
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from enum import StrEnum
from pathlib import Path
from threading import Lock
from typing import cast

from market_impact_agent.account_state import AccountStateSnapshot, RecentFill
from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.data_inputs import LocalDataSnapshotStore
from market_impact_agent.domain import (
    ApprovalMode,
    ExecutionReceipt,
    ExecutionStatus,
    OrderIntent,
    OrderKind,
    Side,
    TradingEnvironment,
    TradingMandateV2,
    require_aware,
)
from market_impact_agent.paper_execution import PriceBasis
from market_impact_agent.portfolio_decision import (
    AgentPortfolioProposalV2,
    OrderSizingDecisionV2,
    OrderSizingOutcome,
    PortfolioAction,
    PortfolioDecisionOutcome,
    PortfolioDecisionV2,
    PortfolioExposureViewAuthorityV2,
    PortfolioExposureViewV2,
    PortfolioLegRole,
)
from market_impact_agent.providers import (
    CancelExecutionProvider,
    CancellationCapabilityRejected,
    CancellationCommandStatus,
    Capability,
    ExecutionProvider,
    NewOrderAdmissionProvider,
    ReconciliationSnapshot,
    SubmissionCapability,
    SubmissionCapabilityRejected,
    _issue_cancellation_capability,  # pyright: ignore[reportPrivateUsage]
    _issue_submission_capability,  # pyright: ignore[reportPrivateUsage]
)
from market_impact_agent.runtime_store import ArtifactStore

AUTONOMOUS_PAPER_OPERATION_SCHEMA = "market-impact.autonomous-paper-operation.v2"
AUTONOMOUS_POLICY_EVALUATION_SCHEMA = "market-impact.autonomous-policy-evaluation.v2"
AUTONOMOUS_MANDATE_BINDING_SCHEMA = "market-impact.autonomous-mandate-binding.v2"
AUTONOMOUS_RISK_OBSERVATION_MAX_AGE = timedelta(seconds=30)
_PROVIDER_RECONCILIATION_OPERATIONAL_ERRORS = (
    KeyError,
    OSError,
    RuntimeError,
    TypeError,
    ValueError,
)


@dataclass(frozen=True, slots=True)
class AutonomousPaperProviderLeaseV2:
    lease_id: str
    harness_authority_id: str
    provider_id: str
    provider_version: str
    account_reference_hash: str
    environment: str
    instrument_routes_hash: str
    mandate_hash: str
    markets: tuple[str, ...]
    order_types: tuple[str, ...]
    time_in_force: tuple[str, ...]
    issued_at: datetime
    valid_until: datetime

    def __post_init__(self) -> None:
        if not self.harness_authority_id.startswith("harness-authority-"):
            raise ValueError("provider lease Harness authority identity is invalid")
        if len(self.mandate_hash) != 64 or any(
            character not in "0123456789abcdef" for character in self.mandate_hash
        ):
            raise ValueError("provider lease mandate_hash must be SHA-256")
        require_aware(self.issued_at, "provider lease issued_at")
        require_aware(self.valid_until, "provider lease valid_until")
        if self.valid_until <= self.issued_at:
            raise ValueError("provider lease validity must be positive")
        if self.lease_id != "autonomous-paper-provider-lease-" + canonical_hash(self.core_dict()):
            raise ValueError("provider lease identity does not match content")

    def core_dict(self) -> dict[str, object]:
        return {
            "schema_version": "market-impact.autonomous-paper-provider-lease.v2",
            "harness_authority_id": self.harness_authority_id,
            "provider_id": self.provider_id,
            "provider_version": self.provider_version,
            "account_reference_hash": self.account_reference_hash,
            "environment": self.environment,
            "instrument_routes_hash": self.instrument_routes_hash,
            "mandate_hash": self.mandate_hash,
            "markets": list(self.markets),
            "order_types": list(self.order_types),
            "time_in_force": list(self.time_in_force),
            "issued_at": _timestamp(self.issued_at),
            "valid_until": _timestamp(self.valid_until),
        }

    def to_dict(self) -> dict[str, object]:
        return {"lease_id": self.lease_id, **self.core_dict()}

    def is_current(self, now: datetime) -> bool:
        return self.issued_at <= now < self.valid_until

    def allows_risk_reduction(self, now: datetime) -> bool:
        return self.issued_at <= now


class AutonomousPaperProviderLeaseAuthorityV2:
    """Durable issuer/resolver for exact-scope autonomous Paper capabilities."""

    def __init__(self, store: LocalDataSnapshotStore) -> None:
        self.store = store
        self.harness_authority_id = store.harness_authority_id
        with store.authority_transaction() as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS autonomous_accepted_provider_capabilities (
                    capability_id TEXT PRIMARY KEY,
                    harness_authority_id TEXT NOT NULL,
                    provider_id TEXT NOT NULL,
                    provider_version TEXT NOT NULL,
                    provider_manifest_hash TEXT NOT NULL,
                    account_reference_hash TEXT NOT NULL,
                    account_state_hash TEXT NOT NULL
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS autonomous_provider_acceptances (
                    lease_id TEXT PRIMARY KEY,
                    harness_authority_id TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    provider_manifest_hash TEXT NOT NULL,
                    account_state_hash TEXT NOT NULL,
                    mandate_hash TEXT NOT NULL,
                    active_mutation_id TEXT,
                    active_mutation_kind TEXT,
                    active_mutation_started_at TEXT,
                    revoke_requested INTEGER NOT NULL DEFAULT 0,
                    revoked_at TEXT
                )
                """
            )
            columns = {
                cast(str, row["name"])
                for row in connection.execute("PRAGMA table_info(autonomous_provider_acceptances)")
            }
            migrations = {
                "active_mutation_id": "TEXT",
                "active_mutation_kind": "TEXT",
                "active_mutation_started_at": "TEXT",
                "revoke_requested": "INTEGER NOT NULL DEFAULT 0",
                "revoked_at": "TEXT",
            }
            for name, declaration in migrations.items():
                if name not in columns:
                    connection.execute(
                        f"ALTER TABLE autonomous_provider_acceptances "
                        f"ADD COLUMN {name} {declaration}"
                    )
            connection.execute(
                """
                CREATE TRIGGER IF NOT EXISTS autonomous_provider_lease_active_delete_guard
                BEFORE DELETE ON autonomous_provider_acceptances
                FOR EACH ROW WHEN OLD.active_mutation_id IS NOT NULL
                BEGIN
                    SELECT RAISE(ABORT, 'active provider mutation claim cannot be deleted');
                END
                """
            )

    def resolve(self, lease_id: str) -> AutonomousPaperProviderLeaseV2:
        with self.store.authority_transaction() as connection:
            return self.resolve_in_transaction(connection, lease_id)

    def resolve_in_transaction(
        self, connection: sqlite3.Connection, lease_id: str
    ) -> AutonomousPaperProviderLeaseV2:
        row = connection.execute(
            """
            SELECT * FROM autonomous_provider_acceptances
            WHERE lease_id = ? AND harness_authority_id = ?
            """,
            (lease_id, self.harness_authority_id),
        ).fetchone()
        if (
            row is None
            or bool(row["revoke_requested"])
            or row["revoked_at"] is not None
            or row["active_mutation_id"] is not None
        ):
            raise KeyError("unknown autonomous Paper provider lease")
        return self._lease_from_row(row)

    def resolve_for_recovery(self, lease_id: str) -> AutonomousPaperProviderLeaseV2:
        with self.store.authority_transaction() as connection:
            row = connection.execute(
                """
                SELECT * FROM autonomous_provider_acceptances
                WHERE lease_id = ? AND harness_authority_id = ?
                  AND revoked_at IS NULL AND active_mutation_id IS NOT NULL
                """,
                (lease_id, self.harness_authority_id),
            ).fetchone()
            if row is None:
                raise KeyError("unknown recoverable autonomous Paper provider lease")
            return self._lease_from_row(row)

    def claim_mutation_in_transaction(
        self,
        connection: sqlite3.Connection,
        *,
        lease_id: str,
        mutation_id: str,
        kind: str,
        started_at: datetime,
    ) -> AutonomousPaperProviderLeaseV2:
        row = connection.execute(
            """
            SELECT * FROM autonomous_provider_acceptances
            WHERE lease_id = ? AND harness_authority_id = ?
            """,
            (lease_id, self.harness_authority_id),
        ).fetchone()
        if (
            row is None
            or bool(row["revoke_requested"])
            or row["revoked_at"] is not None
            or row["active_mutation_id"] is not None
        ):
            raise PermissionError("provider lease cannot claim another mutation")
        lease = self._lease_from_row(row)
        if not lease.is_current(started_at):
            raise PermissionError("provider lease is expired for a new mutation")
        updated = connection.execute(
            """
            UPDATE autonomous_provider_acceptances
            SET active_mutation_id = ?, active_mutation_kind = ?,
                active_mutation_started_at = ?
            WHERE lease_id = ? AND harness_authority_id = ?
              AND active_mutation_id IS NULL AND revoke_requested = 0
              AND revoked_at IS NULL
            """,
            (
                mutation_id,
                kind,
                _timestamp(started_at),
                lease_id,
                self.harness_authority_id,
            ),
        )
        if updated.rowcount != 1:
            raise PermissionError("provider lease mutation claim was lost")
        return lease

    def resolve_claimed_mutation_in_transaction(
        self,
        connection: sqlite3.Connection,
        *,
        lease_id: str,
        mutation_id: str,
        kind: str,
    ) -> AutonomousPaperProviderLeaseV2:
        row = connection.execute(
            """
            SELECT * FROM autonomous_provider_acceptances
            WHERE lease_id = ? AND harness_authority_id = ?
              AND active_mutation_id = ? AND active_mutation_kind = ?
              AND revoked_at IS NULL
            """,
            (lease_id, self.harness_authority_id, mutation_id, kind),
        ).fetchone()
        if row is None:
            raise KeyError("provider mutation claim is unavailable")
        return self._lease_from_row(row)

    def finalize_mutation_in_transaction(
        self,
        connection: sqlite3.Connection,
        *,
        lease_id: str,
        mutation_id: str,
        kind: str,
        finished_at: datetime,
    ) -> bool:
        row = connection.execute(
            """
            SELECT revoke_requested FROM autonomous_provider_acceptances
            WHERE lease_id = ? AND harness_authority_id = ?
              AND active_mutation_id = ? AND active_mutation_kind = ?
              AND revoked_at IS NULL
            """,
            (lease_id, self.harness_authority_id, mutation_id, kind),
        ).fetchone()
        if row is None:
            raise RuntimeError("provider mutation claim was lost before finalization")
        revoke_requested = bool(row["revoke_requested"])
        connection.execute(
            """
            UPDATE autonomous_provider_acceptances
            SET active_mutation_id = NULL, active_mutation_kind = NULL,
                active_mutation_started_at = NULL,
                revoked_at = CASE WHEN revoke_requested = 1 THEN ? ELSE revoked_at END
            WHERE lease_id = ? AND active_mutation_id = ?
            """,
            (_timestamp(finished_at), lease_id, mutation_id),
        )
        return revoke_requested

    def request_revocation(self, lease_id: str, *, requested_at: datetime) -> bool:
        """Record revocation without waiting for an already-authorized Provider call."""

        require_aware(requested_at, "provider lease revocation requested_at")
        requested_at = requested_at.astimezone(UTC)
        with self.store.authority_transaction() as connection:
            row = connection.execute(
                """
                SELECT active_mutation_id, revoked_at
                FROM autonomous_provider_acceptances
                WHERE lease_id = ? AND harness_authority_id = ?
                """,
                (lease_id, self.harness_authority_id),
            ).fetchone()
            if row is None:
                raise KeyError("unknown autonomous Paper provider lease")
            if row["revoked_at"] is not None:
                return True
            revoked = row["active_mutation_id"] is None
            connection.execute(
                """
                UPDATE autonomous_provider_acceptances
                SET revoke_requested = 1,
                    revoked_at = CASE WHEN active_mutation_id IS NULL THEN ? ELSE NULL END
                WHERE lease_id = ? AND harness_authority_id = ?
                """,
                (_timestamp(requested_at), lease_id, self.harness_authority_id),
            )
            return revoked

    def _lease_from_row(self, row: sqlite3.Row) -> AutonomousPaperProviderLeaseV2:
        value = json.loads(cast(str, row["payload_json"]))
        if not isinstance(value, dict):
            raise TypeError("durable provider lease is not an object")
        payload = cast(dict[str, object], value)
        lease = AutonomousPaperProviderLeaseV2(
            lease_id=cast(str, payload["lease_id"]),
            harness_authority_id=cast(str, payload["harness_authority_id"]),
            provider_id=cast(str, payload["provider_id"]),
            provider_version=cast(str, payload["provider_version"]),
            account_reference_hash=cast(str, payload["account_reference_hash"]),
            environment=cast(str, payload["environment"]),
            instrument_routes_hash=cast(str, payload["instrument_routes_hash"]),
            mandate_hash=cast(str, payload["mandate_hash"]),
            markets=tuple(cast(list[str], payload["markets"])),
            order_types=tuple(cast(list[str], payload["order_types"])),
            time_in_force=tuple(cast(list[str], payload["time_in_force"])),
            issued_at=_datetime(cast(str, payload["issued_at"])),
            valid_until=_datetime(cast(str, payload["valid_until"])),
        )
        if (
            lease.to_dict() != payload
            or lease.harness_authority_id != self.harness_authority_id
            or cast(str, row["mandate_hash"]) != lease.mandate_hash
        ):
            raise ValueError("durable provider lease payload is non-canonical")
        return lease


def _record_accepted_provider_capability(  # pyright: ignore[reportUnusedFunction]
    store: LocalDataSnapshotStore,
    *,
    provider_acceptance_id: str,
) -> str:
    """Reopen a durable Provider acceptance owned by PaperExecutionService."""

    authority_id = store.harness_authority_id
    AutonomousPaperProviderLeaseAuthorityV2(store)
    with store.authority_transaction() as connection:
        acceptance = connection.execute(
            """
            SELECT * FROM paper_provider_acceptances
            WHERE acceptance_id = ? AND harness_authority_id = ?
            """,
            (provider_acceptance_id, authority_id),
        ).fetchone()
    if acceptance is None:
        raise PermissionError("same-root durable Provider acceptance is unavailable")
    artifact_value = store.artifacts.read_json(cast(str, acceptance["artifact_hash"]))
    if not isinstance(artifact_value, dict):
        raise PermissionError("durable Provider acceptance artifact is invalid")
    artifact = cast(dict[str, object], artifact_value)
    expected_keys = {
        "acceptance_id",
        "schema_version",
        "harness_authority_id",
        "provider_id",
        "provider_version",
        "provider_manifest_hash",
        "account_reference_hash",
        "account_state_hash",
        "accepted_at",
    }
    acceptance_core = {key: value for key, value in artifact.items() if key != "acceptance_id"}
    if (
        set(artifact) != expected_keys
        or artifact.get("acceptance_id") != provider_acceptance_id
        or artifact.get("schema_version") != "market-impact.paper-provider-acceptance.v2"
        or artifact.get("harness_authority_id") != authority_id
        or artifact.get("provider_id") != acceptance["provider_id"]
        or artifact.get("provider_version") != acceptance["provider_version"]
        or artifact.get("provider_manifest_hash") != acceptance["provider_manifest_hash"]
        or artifact.get("account_reference_hash") != acceptance["account_reference_hash"]
        or artifact.get("account_state_hash") != acceptance["account_state_hash"]
        or provider_acceptance_id != "paper-provider-acceptance-" + canonical_hash(acceptance_core)
    ):
        raise PermissionError("durable Provider acceptance cannot be reopened exactly")
    manifest_hash = cast(str, acceptance["provider_manifest_hash"])
    account_hash = cast(str, acceptance["account_state_hash"])
    account_reference_hash = cast(str, acceptance["account_reference_hash"])
    capability_id = "accepted-paper-provider-capability-" + canonical_hash(
        {
            "harness_authority_id": authority_id,
            "provider_acceptance_id": provider_acceptance_id,
            "provider_manifest_hash": manifest_hash,
            "account_state_hash": account_hash,
            "account_reference_hash": account_reference_hash,
        }
    )
    with store.authority_transaction() as connection:
        connection.execute(
            """
            INSERT OR IGNORE INTO autonomous_accepted_provider_capabilities (
                capability_id, harness_authority_id, provider_id, provider_version,
                provider_manifest_hash, account_reference_hash, account_state_hash
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                capability_id,
                authority_id,
                cast(str, acceptance["provider_id"]),
                cast(str, acceptance["provider_version"]),
                manifest_hash,
                account_reference_hash,
                account_hash,
            ),
        )
    return capability_id


def _issue_autonomous_provider_lease(  # pyright: ignore[reportUnusedFunction]
    store: LocalDataSnapshotStore,
    *,
    accepted_capability_id: str,
    provider: ExecutionProvider,
    mandate: TradingMandateV2,
    instrument_routes: Mapping[str, Mapping[str, str]],
) -> AutonomousPaperProviderLeaseV2:
    """Reopen a same-root accepted capability and bind the exact mandate scope."""

    authority_id = store.harness_authority_id
    manifest = provider.manifest
    manifest_hash = canonical_hash(manifest.to_dict())
    routes_hash = canonical_hash(instrument_routes)
    mandate_hash = canonical_hash(mandate.to_dict())
    authority = AutonomousPaperProviderLeaseAuthorityV2(store)
    with store.authority_transaction() as connection:
        capability = connection.execute(
            """
            SELECT * FROM autonomous_accepted_provider_capabilities
            WHERE capability_id = ? AND harness_authority_id = ?
            """,
            (accepted_capability_id, authority_id),
        ).fetchone()
    if (
        capability is None
        or mandate.harness_authority_id != authority_id
        or cast(str, capability["provider_id"]) != manifest.provider_id
        or cast(str, capability["provider_version"]) != manifest.provider_version
        or cast(str, capability["provider_manifest_hash"]) != manifest_hash
        or cast(str, capability["account_reference_hash"]) != mandate.account_id
        or mandate.environment is not TradingEnvironment.PAPER
        or any(route.get("market") not in manifest.markets for route in instrument_routes.values())
    ):
        raise PermissionError(
            "same-root accepted Provider capability cannot authorize this mandate"
        )
    core = {
        "schema_version": "market-impact.autonomous-paper-provider-lease.v2",
        "harness_authority_id": authority_id,
        "provider_id": manifest.provider_id,
        "provider_version": manifest.provider_version,
        "account_reference_hash": mandate.account_id,
        "environment": TradingEnvironment.PAPER.value,
        "instrument_routes_hash": routes_hash,
        "mandate_hash": mandate_hash,
        "markets": list(manifest.markets),
        "order_types": ["market"],
        "time_in_force": ["DAY"],
        "issued_at": _timestamp(mandate.valid_from),
        "valid_until": _timestamp(mandate.valid_until),
    }
    lease = AutonomousPaperProviderLeaseV2(
        lease_id="autonomous-paper-provider-lease-" + canonical_hash(core),
        harness_authority_id=authority_id,
        provider_id=manifest.provider_id,
        provider_version=manifest.provider_version,
        account_reference_hash=mandate.account_id,
        environment=TradingEnvironment.PAPER.value,
        instrument_routes_hash=routes_hash,
        mandate_hash=mandate_hash,
        markets=manifest.markets,
        order_types=("market",),
        time_in_force=("DAY",),
        issued_at=mandate.valid_from,
        valid_until=mandate.valid_until,
    )
    payload = json.dumps(lease.to_dict(), sort_keys=True, separators=(",", ":"))
    with store.authority_transaction() as connection:
        connection.execute(
            """
            INSERT OR IGNORE INTO autonomous_provider_acceptances (
                lease_id, harness_authority_id, payload_json, provider_manifest_hash,
                account_state_hash, mandate_hash
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                lease.lease_id,
                authority.harness_authority_id,
                payload,
                manifest_hash,
                cast(str, capability["account_state_hash"]),
                mandate_hash,
            ),
        )
    try:
        return authority.resolve(lease.lease_id)
    except KeyError:
        return authority.resolve_for_recovery(lease.lease_id)


@dataclass(frozen=True, slots=True)
class AutonomousRiskMeasurementV2:
    measurement_id: str
    mandate_id: str
    account_reference_hash: str
    daily_pnl: Decimal
    strategy_peak_drawdown: Decimal
    source_snapshot_hash: str
    observed_at: datetime
    valid_until: datetime

    def __post_init__(self) -> None:
        if not self.mandate_id or self.mandate_id != self.mandate_id.strip():
            raise ValueError("risk measurement mandate ID is invalid")
        if not self.account_reference_hash.startswith("account-ref-"):
            raise ValueError("risk measurement account identity must be opaque")
        if len(self.source_snapshot_hash) != 64 or any(
            character not in "0123456789abcdef" for character in self.source_snapshot_hash
        ):
            raise ValueError("risk measurement source hash must be SHA-256")
        for value, name in (
            (self.daily_pnl, "daily P&L"),
            (self.strategy_peak_drawdown, "strategy peak drawdown"),
        ):
            if not value.is_finite():
                raise ValueError(f"{name} must be finite")
        if self.strategy_peak_drawdown < 0:
            raise ValueError("strategy peak drawdown must be non-negative")
        require_aware(self.observed_at, "risk measurement observed_at")
        require_aware(self.valid_until, "risk measurement valid_until")
        if self.valid_until <= self.observed_at:
            raise ValueError("risk measurement validity must be positive")
        expected = "autonomous-risk-measurement-" + canonical_hash(self.core_dict())
        if self.measurement_id != expected:
            raise ValueError("risk measurement identity does not match content")

    def core_dict(self) -> dict[str, object]:
        return {
            "schema_version": "market-impact.autonomous-risk-measurement.v2",
            "mandate_id": self.mandate_id,
            "account_reference_hash": self.account_reference_hash,
            "daily_pnl": str(self.daily_pnl),
            "strategy_peak_drawdown": str(self.strategy_peak_drawdown),
            "source_snapshot_hash": self.source_snapshot_hash,
            "observed_at": _timestamp(self.observed_at),
            "valid_until": _timestamp(self.valid_until),
        }

    def to_dict(self) -> dict[str, object]:
        return {"measurement_id": self.measurement_id, **self.core_dict()}


class AutonomousReconciliationAuthorityV2:
    """Harness composition root that rebuilds state from one exact broker snapshot."""

    def __init__(
        self,
        rebuild: Callable[
            [ReconciliationSnapshot], tuple[AccountStateSnapshot, PortfolioExposureViewV2]
        ],
    ) -> None:
        self.__rebuild = rebuild

    def rebuild(
        self, snapshot: ReconciliationSnapshot
    ) -> tuple[AccountStateSnapshot, PortfolioExposureViewV2]:
        account_state, exposure_view = self.__rebuild(snapshot)
        snapshot_hash = canonical_hash(snapshot.to_dict())
        if (
            account_state.reconciliation_reference != snapshot.snapshot_id
            or exposure_view.reconciliation_ledger_snapshot_hash != snapshot_hash
        ):
            raise PermissionError("rebuilt state does not open the exact broker snapshot")
        return account_state, exposure_view


class AutonomousOperationState(StrEnum):
    QUEUED = "queued"
    SUBMITTING = "submitting"
    UNKNOWN = "unknown"
    ACCEPTED = "accepted"
    RECONCILED = "reconciled"
    BLOCKED = "blocked"


class AutonomousCancellationState(StrEnum):
    QUEUED = "queued"
    CANCELING = "canceling"
    UNKNOWN = "unknown"
    ACKNOWLEDGED = "acknowledged"
    RECONCILED = "reconciled"


@dataclass(frozen=True, slots=True)
class AutonomousPaperOperation:
    operation_id: str
    client_order_id: str
    action: PortfolioAction
    order_hash: str
    proposal_hash: str
    portfolio_decision_hash: str
    sizing_decision_hash: str
    mandate_hash: str
    price_basis_hash: str
    provider_acceptance_hash: str
    policy_evaluation_hash: str
    mandate_binding_hash: str
    approval_hash: str
    state: AutonomousOperationState
    provider_order_id: str | None
    provider_status: str | None
    updated_at: datetime


@dataclass(frozen=True, slots=True)
class AutonomousReconciliation:
    reconciliation_hash: str
    complete: bool
    gaps: tuple[str, ...]
    active_kill_reasons: tuple[str, ...]
    observed_at: datetime


@dataclass(frozen=True, slots=True)
class AutonomousCancellation:
    cancellation_id: str
    client_order_id: str
    provider_order_id: str
    state: AutonomousCancellationState
    updated_at: datetime


@dataclass(frozen=True, slots=True)
class _CancellationAuthorityCall:
    lease_id: str
    cancellation_id: str
    attempt_id: str


class _AutonomousPaperRuntimeLease:
    """One process-lifetime autonomous service lease for a canonical Harness root."""

    def __init__(self, root: Path) -> None:
        self.path = root / ".autonomous-paper-service.lock"
        descriptor = os.open(
            self.path,
            os.O_CREAT | os.O_RDWR | getattr(os, "O_CLOEXEC", 0),
            0o600,
        )
        os.fchmod(descriptor, 0o600)
        fcntl.fcntl(
            descriptor,
            fcntl.F_SETFD,
            fcntl.fcntl(descriptor, fcntl.F_GETFD) | fcntl.FD_CLOEXEC,
        )
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as error:
            os.close(descriptor)
            raise RuntimeError("another autonomous Paper service owns this Harness root") from error
        self.owner_pid = os.getpid()
        self._descriptor: int | None = descriptor

    @property
    def owned_by_current_process(self) -> bool:
        return os.getpid() == self.owner_pid

    def close(self) -> None:
        descriptor = self._descriptor
        if descriptor is None:
            return
        self._descriptor = None
        if not self.owned_by_current_process:
            os.close(descriptor)
            return
        try:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
        finally:
            os.close(descriptor)


class AutonomousPaperExecutionServiceV2:
    """Durable, policy-authorized Paper v2 execution owner.

    This service is deliberately additive. ``PaperExecutionService`` retains
    the persisted v1/manual-each contract and continues to reject autonomous
    modes.
    """

    def __init__(
        self,
        store: LocalDataSnapshotStore,
        *,
        provider: ExecutionProvider,
        provider_lease_id: str,
        mandate: TradingMandateV2,
        account_state_source: Callable[[], AccountStateSnapshot],
        exposure_view_source: Callable[[], PortfolioExposureViewV2],
        exposure_view_authority: PortfolioExposureViewAuthorityV2,
        price_basis_source: Callable[[str], PriceBasis | None],
        reconciliation_authority: AutonomousReconciliationAuthorityV2,
        instrument_routes_hash: str,
        instrument_routes: Mapping[str, Mapping[str, str]],
        clock: Callable[[], datetime] | None = None,
        account_state_max_age: timedelta = timedelta(minutes=5),
    ) -> None:
        provider.manifest.assert_valid()
        if (
            not provider.manifest.enabled
            or Capability.PAPER_EXECUTION not in provider.manifest.verified_capabilities
            or Capability.LIVE_EXECUTION in provider.manifest.verified_capabilities
            or TradingEnvironment.PAPER not in provider.manifest.environments
        ):
            raise PermissionError("autonomous execution requires one enabled paper-only Provider")
        _assert_autonomous_mandate(mandate)
        if mandate.harness_authority_id != store.harness_authority_id:
            raise PermissionError("Trading Mandate v2 binds another Harness authority root")
        mandate_hash = canonical_hash(mandate.to_dict())
        provider_lease_authority = AutonomousPaperProviderLeaseAuthorityV2(store)
        try:
            provider_lease = provider_lease_authority.resolve(provider_lease_id)
        except (KeyError, OSError, TypeError, ValueError):
            try:
                provider_lease = provider_lease_authority.resolve_for_recovery(provider_lease_id)
            except (KeyError, OSError, TypeError, ValueError):
                raise PermissionError("provider lease lacks durable Harness authority") from None
        if (
            provider_lease.harness_authority_id != store.harness_authority_id
            or provider_lease.provider_id != provider.manifest.provider_id
            or provider_lease.provider_version != provider.manifest.provider_version
            or provider_lease.account_reference_hash != mandate.account_id
            or provider_lease.environment != TradingEnvironment.PAPER.value
            or provider_lease.instrument_routes_hash != instrument_routes_hash
            or provider_lease.mandate_hash != mandate_hash
            or provider_lease.markets != provider.manifest.markets
            or not set(provider_lease.order_types) <= set(provider.manifest.order_types)
            or "market" not in provider_lease.order_types
            or provider_lease.time_in_force != ("DAY",)
        ):
            raise PermissionError(
                "provider lease does not bind the exact Provider, account, and mandate"
            )
        if account_state_max_age <= timedelta(0):
            raise ValueError("account_state_max_age must be positive")
        self.store = store
        self.harness_authority_id = store.harness_authority_id
        self.root = store.root
        self.database_path = store.index_path
        self.artifacts = store.artifacts
        self.provider = provider
        self.provider_lease = provider_lease
        self.provider_lease_authority = provider_lease_authority
        self.mandate = mandate
        self.mandate_hash = mandate_hash
        self.account_state_source = account_state_source
        self.exposure_view_source = exposure_view_source
        self.exposure_view_authority = exposure_view_authority
        self.price_basis_source = price_basis_source
        self.reconciliation_authority = reconciliation_authority
        self.instrument_routes_hash = instrument_routes_hash
        self.instrument_routes = {
            instrument_id: dict(route) for instrument_id, route in instrument_routes.items()
        }
        if canonical_hash(self.instrument_routes) != instrument_routes_hash:
            raise ValueError("instrument route material does not match its accepted hash")
        if any(
            not isinstance(route.get("market"), str) or not route.get("market")
            for route in self.instrument_routes.values()
        ):
            raise ValueError("every accepted instrument route requires one explicit market")
        if any(
            route["market"] not in provider_lease.markets
            for route in self.instrument_routes.values()
        ):
            raise PermissionError("instrument route map contains a market outside acceptance")
        self.clock = clock or (lambda: datetime.now(UTC))
        self.account_state_max_age = account_state_max_age
        self._runtime_activity_lock = Lock()
        self._closed = True
        self._cancellation_authority_call: ContextVar[_CancellationAuthorityCall | None] = (
            ContextVar(
                f"autonomous-cancellation-authority-{id(self)}",
                default=None,
            )
        )
        runtime_lease = _AutonomousPaperRuntimeLease(self.root)
        self._runtime_lease = runtime_lease
        self._closed = False
        try:
            self._initialize()
            self._initialize_risk_day()
            os.chmod(self.root, 0o700)
            os.chmod(self.database_path, 0o600)
            self.provider.bind_submission_validator(self._validate_submission_capability)
            if isinstance(self.provider, CancelExecutionProvider):
                self.provider.bind_cancellation_validator(self._validate_cancellation_capability)
            self._recover_interrupted_operations()
            self._evaluate_current_risk(self._now(), fail_closed=False)
        except BaseException:
            self._closed = True
            runtime_lease.close()
            raise

    def __enter__(self) -> AutonomousPaperExecutionServiceV2:
        self._assert_open()
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def __del__(self) -> None:
        runtime_lease = getattr(self, "_runtime_lease", None)
        if runtime_lease is not None:
            runtime_lease.close()

    def close(self) -> None:
        if self._closed:
            return
        if not self._runtime_lease.owned_by_current_process:
            self._closed = True
            self._runtime_lease.close()
            return
        if not self._runtime_activity_lock.acquire(blocking=False):
            raise RuntimeError("cannot close autonomous Paper service during Provider mutation")
        try:
            self._closed = True
            self._runtime_lease.close()
        finally:
            self._runtime_activity_lock.release()

    def _assert_open(self) -> None:
        if not self._runtime_lease.owned_by_current_process:
            raise RuntimeError("autonomous Paper service belongs to another process")
        if self._closed:
            raise RuntimeError("autonomous Paper service is closed")

    @property
    def active_kill_reasons(self) -> tuple[str, ...]:
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT reason FROM autonomous_kills WHERE active = 1 ORDER BY reason"
            ).fetchall()
        return tuple(cast(str, row["reason"]) for row in rows)

    def activate_kill(self, reason: str) -> None:
        self._assert_open()
        if reason not in {
            "incomplete_order_coverage",
            "provider_loss",
            "reconciliation_difference",
            "stale_account_snapshot",
            "unknown_ack",
        }:
            raise ValueError("risk-limit kills require authoritative measured P&L evidence")
        self._activate_kill(reason, self._now())

    def admit(
        self,
        *,
        proposal: AgentPortfolioProposalV2,
        portfolio_decision: PortfolioDecisionV2,
        sizing_decision: OrderSizingDecisionV2,
        account_state: AccountStateSnapshot,
        exposure_view: PortfolioExposureViewV2,
        price_bases: Mapping[str, PriceBasis],
    ) -> tuple[AutonomousPaperOperation, ...]:
        self._assert_open()
        now = self._now()
        risk_reduction = proposal.requested_action in {
            PortfolioAction.REDUCE,
            PortfolioAction.CLOSE,
        }
        risk_measurement = self._evaluate_current_risk(now, fail_closed=not risk_reduction)
        if risk_measurement is None and not risk_reduction:
            raise PermissionError("admission requires a fresh authoritative risk measurement")
        self._assert_current_authorities(
            account_state=account_state,
            exposure_view=exposure_view,
            evaluated_at=now,
            risk_reduction=risk_reduction,
        )
        if self.active_kill_reasons and proposal.requested_action in {
            PortfolioAction.OPEN,
            PortfolioAction.INCREASE,
            PortfolioAction.ROTATE,
        }:
            raise PermissionError("active autonomous kill blocks new or increased exposure")
        _assert_chain(
            proposal=proposal,
            portfolio_decision=portfolio_decision,
            sizing_decision=sizing_decision,
            mandate=self.mandate,
            exposure_view=exposure_view,
            price_bases=price_bases,
        )
        if any(instrument_id not in self.instrument_routes for instrument_id in price_bases):
            raise PermissionError("decision instrument lacks an exact accepted Provider route")
        proposal_hash = self.artifacts.put_json(proposal.to_dict()).content_hash
        portfolio_hash = self.artifacts.put_json(portfolio_decision.to_dict()).content_hash
        sizing_hash = self.artifacts.put_json(sizing_decision.to_dict()).content_hash
        mandate_hash = self.artifacts.put_json(self.mandate.to_dict()).content_hash
        acceptance_hash = self.artifacts.put_json(self.provider_lease.to_dict()).content_hash
        risk_payload = (
            risk_measurement.to_dict()
            if risk_measurement is not None
            else {
                "schema_version": "market-impact.autonomous-risk-unavailable.v2",
                "outcome": "risk_reduction_only",
                "mandate_id": self.mandate.mandate_id,
                "account_reference_hash": self.mandate.account_id,
                "evaluated_at": _timestamp(now),
            }
        )
        risk_measurement_hash = self.artifacts.put_json(risk_payload).content_hash
        policy_payload = {
            "schema_version": AUTONOMOUS_POLICY_EVALUATION_SCHEMA,
            "outcome": "eligible",
            "proposal_hash": proposal_hash,
            "portfolio_decision_hash": portfolio_hash,
            "sizing_decision_hash": sizing_hash,
            "account_state_hash": canonical_hash(account_state.to_dict()),
            "exposure_view_hash": canonical_hash(exposure_view.to_dict()),
            "provider_acceptance_hash": acceptance_hash,
            "risk_measurement_hash": risk_measurement_hash,
            "evaluated_at": _timestamp(now),
            "evaluator_version": "autonomous-paper-policy-v2",
        }
        policy_hash = self.artifacts.put_json(policy_payload).content_hash
        binding_payload = {
            "schema_version": AUTONOMOUS_MANDATE_BINDING_SCHEMA,
            "proposal_id": proposal.proposal_id,
            "portfolio_decision_id": portfolio_decision.decision_id,
            "sizing_decision_id": sizing_decision.decision_id,
            "trading_mandate_hash": mandate_hash,
            "provider_acceptance_hash": acceptance_hash,
            "approval_mode": ApprovalMode.AUTONOMOUS.value,
            "human_order_approval_required": False,
            "bound_at": _timestamp(now),
        }
        binding_hash = self.artifacts.put_json(binding_payload).content_hash
        approval_payload = {
            "schema_version": "market-impact.autonomous-policy-approval.v2",
            "actor_kind": "harness_policy",
            "actor_ref": "autonomous-paper-v2",
            "policy_evaluation_hash": policy_hash,
            "mandate_binding_hash": binding_hash,
            "approved": True,
            "decided_at": _timestamp(now),
        }
        approval_hash = self.artifacts.put_json(approval_payload).content_hash

        operations: list[AutonomousPaperOperation] = []
        ready_legs = tuple(
            (decision_leg, sized_leg)
            for decision_leg, sized_leg in zip(
                portfolio_decision.legs, sizing_decision.legs, strict=True
            )
            if sized_leg.outcome is OrderSizingOutcome.READY
        )
        for decision_leg, sized_leg in ready_legs:
            if sized_leg.quantity is None or sized_leg.side is None:
                raise ValueError("ready Order Sizing leg lacks quantity or side")
            basis = price_bases[decision_leg.instrument_id]
            price_hash = self.artifacts.put_json(basis.to_dict()).content_hash
            identity = {
                "sizing_decision_id": sizing_decision.decision_id,
                "leg_role": decision_leg.role.value,
                "instrument_id": decision_leg.instrument_id,
                "quantity": str(sized_leg.quantity),
                "side": sized_leg.side.value,
                "mandate_id": self.mandate.mandate_id,
            }
            client_order_id = "autonomous-paper-order-" + canonical_hash(identity)
            order = OrderIntent(
                client_order_id=client_order_id,
                signal_id=proposal.signal_id,
                account_id=self.mandate.account_id,
                environment=TradingEnvironment.PAPER,
                instrument_id=decision_leg.instrument_id,
                side=sized_leg.side,
                quantity=sized_leg.quantity,
                order_kind=OrderKind.MARKET,
                created_at=now,
                expires_at=min(self.mandate.valid_until, basis.valid_until),
            )
            order_hash = self.artifacts.put_json(order.to_dict()).content_hash
            operation_id = "autonomous-paper-operation-" + canonical_hash(
                {
                    "client_order_id": client_order_id,
                    "order_hash": order_hash,
                    "sizing_decision_hash": sizing_hash,
                    "mandate_binding_hash": binding_hash,
                }
            )
            signed_delta = sized_leg.delta_notional
            target_signed = sized_leg.current_signed_notional + signed_delta
            gross_delta = abs(target_signed) - abs(sized_leg.current_signed_notional)
            turnover_reserved = sized_leg.quantity * basis.price
            cash_reserved = turnover_reserved if sized_leg.side is Side.BUY else Decimal(0)
            position_count_delta = _position_count_delta(
                current=sized_leg.current_signed_notional,
                target=target_signed,
            )
            with self._connect() as connection:
                connection.execute("BEGIN IMMEDIATE")
                existing = connection.execute(
                    "SELECT * FROM autonomous_operations WHERE operation_id = ?",
                    (operation_id,),
                ).fetchone()
                if existing is None:
                    self._assert_reservation_budget(
                        connection,
                        exposure_view=exposure_view,
                        account_state=account_state,
                        instrument_id=decision_leg.instrument_id,
                        signed_delta=signed_delta,
                        gross_delta=gross_delta,
                        turnover_reserved=turnover_reserved,
                        cash_reserved=cash_reserved,
                        position_count_delta=position_count_delta,
                    )
                    connection.execute(
                        """
                        INSERT INTO autonomous_operations (
                            operation_id, client_order_id, action, leg_role, order_hash,
                            proposal_hash, portfolio_decision_hash, sizing_decision_hash,
                            mandate_hash, price_basis_hash, provider_acceptance_hash,
                            policy_evaluation_hash, mandate_binding_hash, approval_hash,
                            risk_measurement_hash, exposure_view_hash, signed_delta,
                            gross_delta, turnover_reserved, cash_reserved,
                            position_count_delta, reservation_active, submission_consumed,
                            state, provider_order_id, provider_status, lease_token,
                            created_at, updated_at
                        ) VALUES (
                            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, 0, ?,
                            NULL, NULL, NULL, ?, ?
                        )
                        """,
                        (
                            operation_id,
                            client_order_id,
                            decision_leg.action.value,
                            decision_leg.role.value,
                            order_hash,
                            proposal_hash,
                            portfolio_hash,
                            sizing_hash,
                            mandate_hash,
                            price_hash,
                            acceptance_hash,
                            policy_hash,
                            binding_hash,
                            approval_hash,
                            risk_measurement_hash,
                            canonical_hash(exposure_view.to_dict()),
                            str(signed_delta),
                            str(gross_delta),
                            str(turnover_reserved),
                            str(cash_reserved),
                            position_count_delta,
                            AutonomousOperationState.QUEUED.value,
                            _timestamp(now),
                            _timestamp(now),
                        ),
                    )
                else:
                    _assert_existing_operation_matches(
                        existing,
                        order_hash=order_hash,
                        sizing_hash=sizing_hash,
                        mandate_hash=mandate_hash,
                        acceptance_hash=acceptance_hash,
                    )
                connection.commit()
            operations.append(self.get(client_order_id))
        return tuple(operations)

    def dispatch_next(self) -> AutonomousPaperOperation | None:
        self._assert_open()
        with self._runtime_activity_lock:
            self._assert_open()
            return self._dispatch_next_owned()

    def _dispatch_next_owned(self) -> AutonomousPaperOperation | None:
        now = self._now()
        try:
            current_exposure = self.exposure_view_source()
            self.exposure_view_authority.assert_authoritative_exposure_view(current_exposure)
            if not current_exposure.observed_at <= now < current_exposure.valid_until:
                raise PermissionError("Portfolio Exposure View is stale")
        except Exception:
            self._activate_kill("stale_account_snapshot", now)
        else:
            for reason in current_exposure.active_kill_reasons:
                if reason not in {
                    "daily_loss_threshold_exceeded",
                    "strategy_peak_drawdown_threshold_exceeded",
                }:
                    self._activate_kill(reason, now)
        self._evaluate_current_risk(now, fail_closed=False)
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                """
                SELECT * FROM autonomous_operations
                WHERE state = ? ORDER BY created_at, operation_id LIMIT 1
                """,
                (AutonomousOperationState.QUEUED.value,),
            ).fetchone()
            if row is None:
                connection.commit()
                return None
            action = PortfolioAction(cast(str, row["action"]))
            risk_reduction = action in {PortfolioAction.REDUCE, PortfolioAction.CLOSE}
            if self.active_kill_reasons and not risk_reduction:
                connection.execute(
                    """
                    UPDATE autonomous_operations
                    SET state = ?, reservation_active = 0, updated_at = ?
                    WHERE operation_id = ?
                    """,
                    (
                        AutonomousOperationState.BLOCKED.value,
                        _timestamp(now),
                        cast(str, row["operation_id"]),
                    ),
                )
                connection.commit()
                return self.get(cast(str, row["client_order_id"]))
            submission_id = "autonomous-submission-" + uuid.uuid4().hex
            connection.execute(
                """
                UPDATE autonomous_operations
                SET state = ?, lease_token = ?, submission_consumed = 1, updated_at = ?
                WHERE operation_id = ? AND state = ?
                """,
                (
                    AutonomousOperationState.SUBMITTING.value,
                    submission_id,
                    _timestamp(now),
                    cast(str, row["operation_id"]),
                    AutonomousOperationState.QUEUED.value,
                ),
            )
            connection.execute(
                """
                INSERT INTO autonomous_submission_attempts
                VALUES (?, ?, 'started', NULL, ?, NULL)
                """,
                (submission_id, cast(str, row["operation_id"]), _timestamp(now)),
            )
            connection.commit()

        try:
            self._assert_dispatch_authorities(row, evaluated_at=now, risk_reduction=risk_reduction)
        except Exception as error:
            reason = (
                "provider_loss"
                if "Provider" in str(error) or "provider" in str(error)
                else "stale_account_snapshot"
            )
            self._activate_kill(reason, now)
            self._block_claim(row, submission_id, now, type(error).__name__)
            return self.get(cast(str, row["client_order_id"]))
        capability = _issue_submission_capability(
            order=_order_from_payload(self.artifacts.read_json(cast(str, row["order_hash"]))),
            submission_id=submission_id,
            provider_id=self.provider.manifest.provider_id,
            provider_version=self.provider.manifest.provider_version,
            order_hash=cast(str, row["order_hash"]),
            mandate_hash=cast(str, row["mandate_hash"]),
            price_basis_hash=cast(str, row["price_basis_hash"]),
            policy_evaluation_hash=cast(str, row["policy_evaluation_hash"]),
            approval_hash=cast(str, row["approval_hash"]),
        )
        try:
            receipt = self.provider.submit(capability)
            if (
                receipt.client_order_id != cast(str, row["client_order_id"])
                or receipt.status is not ExecutionStatus.ACCEPTED
                or receipt.provider_order_id is None
                or receipt.filled_quantity != 0
                or receipt.fill_ids
            ):
                raise ValueError("Provider acknowledgement is not an exact accepted receipt")
        except SubmissionCapabilityRejected:
            self._activate_kill("provider_loss", now)
            self._block_claim(row, submission_id, now, "provider_rejected_capability")
            return self.get(cast(str, row["client_order_id"]))
        except Exception as error:
            self._activate_kill("unknown_ack", now)
            self._activate_kill("incomplete_order_coverage", now)
            self._finish_unknown(row, submission_id, now, type(error).__name__)
            return self.get(cast(str, row["client_order_id"]))
        receipt_hash = self.artifacts.put_json(_receipt_payload(receipt)).content_hash
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            updated = connection.execute(
                """
                UPDATE autonomous_operations
                SET state = ?, provider_order_id = ?, provider_status = ?, lease_token = NULL,
                    updated_at = ? WHERE operation_id = ? AND lease_token = ?
                """,
                (
                    AutonomousOperationState.ACCEPTED.value,
                    receipt.provider_order_id,
                    receipt.status.value,
                    _timestamp(now),
                    cast(str, row["operation_id"]),
                    submission_id,
                ),
            )
            if updated.rowcount != 1:
                connection.rollback()
                raise RuntimeError("autonomous submission lease was lost")
            connection.execute(
                """
                UPDATE autonomous_submission_attempts
                SET state = 'acknowledged', receipt_hash = ?, finished_at = ?
                WHERE submission_id = ?
                """,
                (receipt_hash, _timestamp(now), submission_id),
            )
            connection.commit()
        self._activate_kill("incomplete_order_coverage", now)
        return self.get(receipt.client_order_id)

    def request_cancel(self, client_order_id: str) -> AutonomousCancellation:
        self._assert_open()
        now = self._now()
        if not isinstance(self.provider, CancelExecutionProvider):
            raise PermissionError("Provider does not expose accepted cancellation capability")
        operation = self.get(client_order_id)
        if operation.provider_order_id is None or operation.state not in {
            AutonomousOperationState.ACCEPTED,
            AutonomousOperationState.RECONCILED,
        }:
            raise PermissionError("cancel requires one known accepted Provider order")
        cancellation_id = "autonomous-cancellation-" + canonical_hash(
            {
                "operation_id": operation.operation_id,
                "provider_order_id": operation.provider_order_id,
            }
        )
        with self._connect() as connection:
            connection.execute(
                """
                INSERT OR IGNORE INTO autonomous_cancellations
                (cancellation_id, operation_id, client_order_id, provider_order_id, state,
                 lease_token, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, NULL, ?, ?)
                """,
                (
                    cancellation_id,
                    operation.operation_id,
                    client_order_id,
                    operation.provider_order_id,
                    AutonomousCancellationState.QUEUED.value,
                    _timestamp(now),
                    _timestamp(now),
                ),
            )
        return self.get_cancellation(cancellation_id)

    def dispatch_next_cancellation(self) -> AutonomousCancellation | None:
        self._assert_open()
        with self._runtime_activity_lock:
            self._assert_open()
            return self._dispatch_next_cancellation_owned()

    def _dispatch_next_cancellation_owned(self) -> AutonomousCancellation | None:
        now = self._now()
        if not isinstance(self.provider, CancelExecutionProvider):
            return None
        attempt_id = "autonomous-cancel-attempt-" + uuid.uuid4().hex
        claim_failed = False
        reopened: AutonomousPaperProviderLeaseV2 | None = None
        with self.store.authority_transaction() as authority_connection:
            row = authority_connection.execute(
                """
                SELECT * FROM autonomous_cancellations
                WHERE state = ? ORDER BY created_at LIMIT 1
                """,
                (AutonomousCancellationState.QUEUED.value,),
            ).fetchone()
            if row is None:
                return None
            try:
                reopened = self.provider_lease_authority.claim_mutation_in_transaction(
                    authority_connection,
                    lease_id=self.provider_lease.lease_id,
                    mutation_id=attempt_id,
                    kind="cancel",
                    started_at=now,
                )
            except (KeyError, OSError, PermissionError, TypeError, ValueError):
                claim_failed = True
                authority_connection.execute(
                    "UPDATE autonomous_cancellations SET updated_at = ? WHERE cancellation_id = ?",
                    (_timestamp(now), cast(str, row["cancellation_id"])),
                )
                authority_connection.execute(
                    """
                    INSERT INTO autonomous_kills (reason, active, updated_at)
                    VALUES ('provider_loss', 1, ?)
                    ON CONFLICT(reason) DO UPDATE
                    SET active = 1, updated_at = excluded.updated_at
                    """,
                    (_timestamp(now),),
                )
            if not claim_failed:
                if (
                    reopened is None
                    or reopened != self.provider_lease
                    or reopened.mandate_hash != self.mandate_hash
                ):
                    raise RuntimeError("provider lease claim differs from accepted authority")
                updated = authority_connection.execute(
                    """
                    UPDATE autonomous_cancellations
                    SET state = ?, lease_token = ?, updated_at = ?
                    WHERE cancellation_id = ? AND state = ?
                    """,
                    (
                        AutonomousCancellationState.CANCELING.value,
                        attempt_id,
                        _timestamp(now),
                        cast(str, row["cancellation_id"]),
                        AutonomousCancellationState.QUEUED.value,
                    ),
                )
                if updated.rowcount != 1:
                    raise RuntimeError("cancellation mutation claim was lost")
        if claim_failed:
            return self.get_cancellation(cast(str, row["cancellation_id"]))
        request_hash = canonical_hash(
            {
                "cancellation_id": cast(str, row["cancellation_id"]),
                "client_order_id": cast(str, row["client_order_id"]),
                "provider_order_id": cast(str, row["provider_order_id"]),
            }
        )
        capability = _issue_cancellation_capability(
            client_order_id=cast(str, row["client_order_id"]),
            provider_order_id=cast(str, row["provider_order_id"]),
            cancellation_id=cast(str, row["cancellation_id"]),
            attempt_id=attempt_id,
            provider_id=self.provider.manifest.provider_id,
            provider_version=self.provider.manifest.provider_version,
            request_hash=request_hash,
            approval_hash=canonical_hash(
                {"actor": "harness-risk-reduction", "request": request_hash}
            ),
        )
        authority_call = _CancellationAuthorityCall(
            lease_id=self.provider_lease.lease_id,
            cancellation_id=cast(str, row["cancellation_id"]),
            attempt_id=attempt_id,
        )
        authority_token = self._cancellation_authority_call.set(authority_call)
        kill_reason: str | None = None
        try:
            receipt = self.provider.cancel(capability)
            if (
                receipt.client_order_id != cast(str, row["client_order_id"])
                or receipt.provider_order_id != cast(str, row["provider_order_id"])
                or receipt.cancellation_id != cast(str, row["cancellation_id"])
                or receipt.status
                not in {
                    CancellationCommandStatus.DISPATCHED,
                    CancellationCommandStatus.CANCELED,
                }
            ):
                raise ValueError("Provider cancellation receipt identity or status differs")
            state = AutonomousCancellationState.ACKNOWLEDGED
        except CancellationCapabilityRejected:
            state = AutonomousCancellationState.QUEUED
        except Exception:
            state = AutonomousCancellationState.UNKNOWN
            kill_reason = "unknown_ack"
        finally:
            self._cancellation_authority_call.reset(authority_token)
        finished_at = self._now()
        with self.store.authority_transaction() as authority_connection:
            if kill_reason is not None:
                authority_connection.execute(
                    """
                    INSERT INTO autonomous_kills (reason, active, updated_at) VALUES (?, 1, ?)
                    ON CONFLICT(reason) DO UPDATE
                    SET active = 1, updated_at = excluded.updated_at
                    """,
                    (kill_reason, _timestamp(now)),
                )
            authority_connection.execute(
                """
                UPDATE autonomous_cancellations
                SET state = ?, lease_token = NULL, updated_at = ?
                WHERE cancellation_id = ? AND lease_token = ?
                """,
                (
                    state.value,
                    _timestamp(now),
                    cast(str, row["cancellation_id"]),
                    attempt_id,
                ),
            )
            self.provider_lease_authority.finalize_mutation_in_transaction(
                authority_connection,
                lease_id=self.provider_lease.lease_id,
                mutation_id=attempt_id,
                kind="cancel",
                finished_at=finished_at,
            )
        return self.get_cancellation(cast(str, row["cancellation_id"]))

    def reconcile(self) -> AutonomousReconciliation:
        self._assert_open()
        with self._runtime_activity_lock:
            self._assert_open()
            return self._reconcile_owned()

    def _reconcile_owned(self) -> AutonomousReconciliation:
        now = self._now()
        self._evaluate_current_risk(now, fail_closed=False)
        with self.store.authority_transaction() as authority_connection:
            try:
                reopened = self.provider_lease_authority.resolve_in_transaction(
                    authority_connection, self.provider_lease.lease_id
                )
            except KeyError:
                return self._record_failed_reconciliation_in_transaction(
                    authority_connection,
                    now=now,
                    gap="provider_lease_unavailable",
                )
            if (
                reopened != self.provider_lease
                or reopened.harness_authority_id != self.harness_authority_id
                or reopened.mandate_hash != self.mandate_hash
                or not reopened.allows_risk_reduction(now)
            ):
                return self._record_failed_reconciliation_in_transaction(
                    authority_connection,
                    now=now,
                    gap="provider_lease_unavailable",
                )
            try:
                snapshot = self.provider.reconcile()
            except _PROVIDER_RECONCILIATION_OPERATIONAL_ERRORS:
                return self._record_failed_reconciliation_in_transaction(
                    authority_connection,
                    now=now,
                    gap="provider_reconciliation_failed",
                )
        gaps = set(snapshot.gaps)
        if snapshot.provider_id != self.provider.manifest.provider_id:
            gaps.add("provider_identity_mismatch")
        if not snapshot.complete:
            gaps.add("provider_reconciliation_incomplete")
        receipts = {item.client_order_id: item for item in snapshot.receipts}
        if len(receipts) != len(snapshot.receipts):
            gaps.add("duplicate_provider_order_identity")
        terminal_statuses = {
            ExecutionStatus.CANCELED,
            ExecutionStatus.EXPIRED,
            ExecutionStatus.FILLED,
            ExecutionStatus.REJECTED,
        }
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT * FROM autonomous_operations WHERE state IN (?, ?, ?)",
                (
                    AutonomousOperationState.UNKNOWN.value,
                    AutonomousOperationState.ACCEPTED.value,
                    AutonomousOperationState.RECONCILED.value,
                ),
            ).fetchall()
            cancellation_rows = connection.execute(
                """
                SELECT * FROM autonomous_cancellations WHERE state IN (?, ?)
                """,
                (
                    AutonomousCancellationState.UNKNOWN.value,
                    AutonomousCancellationState.ACKNOWLEDGED.value,
                ),
            ).fetchall()
            known_ids = {cast(str, row["client_order_id"]) for row in rows}
            external = sorted(set(receipts) - known_ids)
            gaps.update(f"external_order:{item}" for item in external)
            try:
                rebuilt_account, rebuilt_exposure = self.reconciliation_authority.rebuild(snapshot)
            except (PermissionError, ValueError):
                rebuilt_account = None
                rebuilt_exposure = None
            fresh_rebuild = self._reconciliation_authorities_are_fresh(
                snapshot_id=snapshot.snapshot_id,
                snapshot_observed_at=snapshot.observed_at,
                snapshot_hash=canonical_hash(snapshot.to_dict()),
                rows=rows,
                account_state=rebuilt_account,
                exposure_view=rebuilt_exposure,
            )
            if not fresh_rebuild:
                gaps.add("fresh_account_exposure_rebuild_required")
            elif not self._rebuilt_state_reflects_provider_snapshot(
                rows=rows,
                receipts=receipts,
                account_state=rebuilt_account,
            ):
                gaps.add("rebuilt_state_does_not_reflect_provider_snapshot")
            resolved_operations: list[tuple[sqlite3.Row, ExecutionReceipt]] = []
            for row in rows:
                client_order_id = cast(str, row["client_order_id"])
                receipt = receipts.get(client_order_id)
                if receipt is None:
                    gaps.add(f"order_coverage_missing:{client_order_id}")
                    continue
                expected_provider_order_id = cast(str | None, row["provider_order_id"])
                if (
                    expected_provider_order_id is not None
                    and receipt.provider_order_id != expected_provider_order_id
                ):
                    gaps.add(f"provider_order_identity_mismatch:{client_order_id}")
                    continue
                if receipt.status is ExecutionStatus.UNKNOWN:
                    gaps.add(f"provider_order_unknown:{client_order_id}")
                    continue
                was_unknown = (
                    AutonomousOperationState(cast(str, row["state"]))
                    is AutonomousOperationState.UNKNOWN
                )
                if was_unknown and receipt.status not in terminal_statuses:
                    gaps.add(f"unknown_ack_not_terminal:{client_order_id}")
                    continue
                resolved_operations.append((row, receipt))
            resolved_cancellations: list[sqlite3.Row] = []
            for cancellation in cancellation_rows:
                client_order_id = cast(str, cancellation["client_order_id"])
                receipt = receipts.get(client_order_id)
                if receipt is None or receipt.status not in terminal_statuses:
                    gaps.add("cancel_not_terminal:" + cast(str, cancellation["cancellation_id"]))
                    continue
                resolved_cancellations.append(cancellation)
            if not gaps:
                for row, receipt in resolved_operations:
                    release_reservation = receipt.status in terminal_statuses
                    connection.execute(
                        """
                        UPDATE autonomous_operations SET state = ?, provider_order_id = ?,
                        provider_status = ?, lease_token = NULL,
                        reservation_active = ?, updated_at = ?
                        WHERE operation_id = ?
                        """,
                        (
                            AutonomousOperationState.RECONCILED.value,
                            receipt.provider_order_id,
                            receipt.status.value,
                            int(not release_reservation),
                            _timestamp(now),
                            cast(str, row["operation_id"]),
                        ),
                    )
                    if receipt.status in terminal_statuses:
                        connection.execute(
                            """
                            UPDATE autonomous_cancellations SET state = ?, updated_at = ?
                            WHERE client_order_id = ?
                            """,
                            (
                                AutonomousCancellationState.RECONCILED.value,
                                _timestamp(now),
                                cast(str, row["client_order_id"]),
                            ),
                        )
                for cancellation in resolved_cancellations:
                    connection.execute(
                        """
                        UPDATE autonomous_cancellations SET state = ?, updated_at = ?
                        WHERE cancellation_id = ?
                        """,
                        (
                            AutonomousCancellationState.RECONCILED.value,
                            _timestamp(now),
                            cast(str, cancellation["cancellation_id"]),
                        ),
                    )
        ordered_gaps = tuple(sorted(gaps))
        if ordered_gaps:
            self._activate_kill("reconciliation_difference", now)
            self._activate_kill("incomplete_order_coverage", now)
        else:
            self._deactivate_kill("reconciliation_difference", now)
            self._deactivate_kill("incomplete_order_coverage", now)
            with self._connect() as connection:
                unresolved_unknown = connection.execute(
                    """
                    SELECT 1 FROM autonomous_operations WHERE state = ?
                    UNION ALL
                    SELECT 1 FROM autonomous_cancellations WHERE state = ?
                    LIMIT 1
                    """,
                    (
                        AutonomousOperationState.UNKNOWN.value,
                        AutonomousCancellationState.UNKNOWN.value,
                    ),
                ).fetchone()
            if unresolved_unknown is None:
                self._deactivate_kill("unknown_ack", now)
        payload = {
            "schema_version": "market-impact.autonomous-paper-reconciliation.v2",
            "provider_snapshot": snapshot.to_dict(),
            "complete": snapshot.complete and not ordered_gaps,
            "gaps": list(ordered_gaps),
            "active_kill_reasons": list(self.active_kill_reasons),
            "observed_at": _timestamp(now),
        }
        artifact = self.artifacts.put_json(payload)
        with self._connect() as connection:
            connection.execute(
                "INSERT OR IGNORE INTO autonomous_reconciliations VALUES (?, ?, ?, ?)",
                (
                    artifact.content_hash,
                    int(snapshot.complete and not ordered_gaps),
                    _timestamp(now),
                    canonical_hash(payload),
                ),
            )
        return AutonomousReconciliation(
            reconciliation_hash=artifact.content_hash,
            complete=snapshot.complete and not ordered_gaps,
            gaps=ordered_gaps,
            active_kill_reasons=self.active_kill_reasons,
            observed_at=now,
        )

    def _record_failed_reconciliation_in_transaction(
        self,
        connection: sqlite3.Connection,
        *,
        now: datetime,
        gap: str,
    ) -> AutonomousReconciliation:
        for reason in ("provider_loss", "incomplete_order_coverage"):
            connection.execute(
                """
                INSERT INTO autonomous_kills (reason, active, updated_at) VALUES (?, 1, ?)
                ON CONFLICT(reason) DO UPDATE SET active = 1, updated_at = excluded.updated_at
                """,
                (reason, _timestamp(now)),
            )
        active_kill_reasons = tuple(
            cast(str, row["reason"])
            for row in connection.execute(
                "SELECT reason FROM autonomous_kills WHERE active = 1 ORDER BY reason"
            ).fetchall()
        )
        payload = {
            "schema_version": "market-impact.autonomous-paper-reconciliation.v2",
            "provider_snapshot": None,
            "complete": False,
            "gaps": [gap],
            "active_kill_reasons": list(active_kill_reasons),
            "observed_at": _timestamp(now),
        }
        artifact = self.artifacts.put_json(payload)
        connection.execute(
            "INSERT OR IGNORE INTO autonomous_reconciliations VALUES (?, 0, ?, ?)",
            (artifact.content_hash, _timestamp(now), canonical_hash(payload)),
        )
        return AutonomousReconciliation(
            reconciliation_hash=artifact.content_hash,
            complete=False,
            gaps=(gap,),
            active_kill_reasons=active_kill_reasons,
            observed_at=now,
        )

    def get(self, client_order_id: str) -> AutonomousPaperOperation:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM autonomous_operations WHERE client_order_id = ?",
                (client_order_id,),
            ).fetchone()
        if row is None:
            raise KeyError(client_order_id)
        return _operation(row)

    def get_cancellation(self, cancellation_id: str) -> AutonomousCancellation:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM autonomous_cancellations WHERE cancellation_id = ?",
                (cancellation_id,),
            ).fetchone()
        if row is None:
            raise KeyError(cancellation_id)
        return AutonomousCancellation(
            cancellation_id=cast(str, row["cancellation_id"]),
            client_order_id=cast(str, row["client_order_id"]),
            provider_order_id=cast(str, row["provider_order_id"]),
            state=AutonomousCancellationState(cast(str, row["state"])),
            updated_at=_datetime(cast(str, row["updated_at"])),
        )

    def _assert_dispatch_authorities(
        self,
        row: sqlite3.Row,
        *,
        evaluated_at: datetime,
        risk_reduction: bool,
    ) -> None:
        order = _order_from_payload(self.artifacts.read_json(cast(str, row["order_hash"])))
        if not self.mandate.valid_from <= evaluated_at < self.mandate.valid_until:
            raise PermissionError("Trading Mandate v2 is not current at dispatch")
        if not order.created_at <= evaluated_at < order.expires_at:
            raise PermissionError("Order Intent is not current at dispatch")
        try:
            reopened_lease = self.provider_lease_authority.resolve(self.provider_lease.lease_id)
        except (KeyError, OSError, TypeError, ValueError):
            raise PermissionError("provider lease lost durable authority") from None
        if (
            reopened_lease != self.provider_lease
            or reopened_lease.mandate_hash != self.mandate_hash
        ):
            raise PermissionError("provider lease differs from durable authority")
        accepted = (
            self.provider_lease.allows_risk_reduction(evaluated_at)
            if risk_reduction
            else self.provider_lease.is_current(evaluated_at)
        )
        if not accepted:
            raise PermissionError("provider lease is not current for this operation")
        if isinstance(self.provider, NewOrderAdmissionProvider) and not (
            self.provider.new_order_admission_open
        ):
            raise PermissionError("Provider new-order admission is closed")
        self._evaluate_current_risk(evaluated_at, fail_closed=not risk_reduction)
        if self.active_kill_reasons and not risk_reduction:
            raise PermissionError("active autonomous kill blocks exposure increase")
        account_state = self.account_state_source()
        exposure_view = self.exposure_view_source()
        self._assert_current_authorities(
            account_state=account_state,
            exposure_view=exposure_view,
            evaluated_at=evaluated_at,
            risk_reduction=risk_reduction,
        )
        sizing_value = self.artifacts.read_json(cast(str, row["sizing_decision_hash"]))
        if not isinstance(sizing_value, dict):
            raise TypeError("persisted Order Sizing Decision is not an object")
        sizing = cast(dict[str, object], sizing_value)
        if sizing.get("exposure_view_hash") != canonical_hash(exposure_view.to_dict()):
            raise PermissionError("current exposure state differs from admitted sizing authority")
        policy_value = self.artifacts.read_json(cast(str, row["policy_evaluation_hash"]))
        if not isinstance(policy_value, dict):
            raise TypeError("persisted Policy Evaluation is not an object")
        policy = cast(dict[str, object], policy_value)
        if policy.get("account_state_hash") != canonical_hash(account_state.to_dict()):
            raise PermissionError("current Account State differs from admitted policy authority")
        basis = self.price_basis_source(
            cast(str, _order_payload(row, self.artifacts)["instrument_id"])
        )
        if (
            basis is None
            or canonical_hash(basis.to_dict()) != cast(str, row["price_basis_hash"])
            or not basis.observed_at <= evaluated_at < basis.valid_until
        ):
            raise PermissionError("raw Price Basis differs or is stale")

    def _assert_current_authorities(
        self,
        *,
        account_state: AccountStateSnapshot,
        exposure_view: PortfolioExposureViewV2,
        evaluated_at: datetime,
        risk_reduction: bool,
    ) -> None:
        current_account = self.account_state_source()
        current_exposure = self.exposure_view_source()
        if current_account.to_dict() != account_state.to_dict():
            raise PermissionError("Account State differs from current Harness source")
        if current_exposure.to_dict() != exposure_view.to_dict():
            raise PermissionError("Exposure View differs from current Harness source")
        self.exposure_view_authority.assert_authoritative_exposure_view(exposure_view)
        if (
            account_state.environment is not TradingEnvironment.PAPER
            or account_state.account_reference_hash != self.mandate.account_id
            or account_state.provider_id != self.provider.manifest.provider_id
            or account_state.provider_version != self.provider.manifest.provider_version
            or account_state.provider_manifest_hash
            != canonical_hash(self.provider.manifest.to_dict())
        ):
            raise PermissionError("Account State does not bind the exact Paper Provider")
        readiness = account_state.readiness(
            evaluated_at=evaluated_at,
            max_age=self.account_state_max_age,
        )
        if risk_reduction:
            if not readiness.risk_observation_ready:
                raise PermissionError("risk-reduction account state is incomplete")
        elif not readiness.exposure_increase_ready:
            raise PermissionError("account state is stale or incomplete")
        if not exposure_view.observed_at <= evaluated_at < exposure_view.valid_until:
            raise PermissionError("Portfolio Exposure View is stale")

    def _reconciliation_authorities_are_fresh(
        self,
        *,
        snapshot_id: str,
        snapshot_observed_at: datetime,
        snapshot_hash: str,
        rows: list[sqlite3.Row],
        account_state: AccountStateSnapshot | None,
        exposure_view: PortfolioExposureViewV2 | None,
    ) -> bool:
        if account_state is None or exposure_view is None:
            return False
        try:
            self.exposure_view_authority.assert_authoritative_exposure_view(exposure_view)
            readiness = account_state.readiness(
                evaluated_at=max(self._now(), account_state.reconciled_at),
                max_age=self.account_state_max_age,
            )
        except Exception:
            return False
        if (
            not readiness.risk_observation_ready
            or account_state.account_reference_hash != self.mandate.account_id
            or account_state.provider_id != self.provider.manifest.provider_id
            or account_state.provider_version != self.provider.manifest.provider_version
            or account_state.provider_manifest_hash
            != canonical_hash(self.provider.manifest.to_dict())
            or account_state.reconciliation_reference != snapshot_id
            or account_state.reconciled_at < snapshot_observed_at
            or exposure_view.observed_at < snapshot_observed_at
            or exposure_view.reconciliation_ledger_snapshot_hash != snapshot_hash
            or not exposure_view.observed_at <= self._now() < exposure_view.valid_until
        ):
            return False
        projected = account_state.project_positions(
            evaluated_at=account_state.reconciled_at,
            max_age=self.account_state_max_age,
        )
        if (
            exposure_view.position_snapshot_id != projected.snapshot_id
            or exposure_view.position_snapshot_hash != canonical_hash(projected.to_dict())
        ):
            return False
        current_hash = canonical_hash(exposure_view.to_dict())
        return all(
            not bool(row["reservation_active"])
            or cast(str, row["exposure_view_hash"]) != current_hash
            for row in rows
        )

    def _rebuilt_state_reflects_provider_snapshot(
        self,
        *,
        rows: list[sqlite3.Row],
        receipts: Mapping[str, ExecutionReceipt],
        account_state: AccountStateSnapshot | None,
    ) -> bool:
        if (
            account_state is None
            or account_state.positions is None
            or account_state.open_orders is None
            or account_state.recent_fills is None
        ):
            return False
        positions = {
            item.target_id: item.quantity if item.side is Side.BUY else -item.quantity
            for item in account_state.positions
        }
        open_order_references = {item.order_reference for item in account_state.open_orders}
        fills = {item.fill_reference: item for item in account_state.recent_fills}
        terminal = {
            ExecutionStatus.CANCELED,
            ExecutionStatus.EXPIRED,
            ExecutionStatus.FILLED,
            ExecutionStatus.REJECTED,
        }
        for row in rows:
            if not bool(row["reservation_active"]):
                continue
            client_order_id = cast(str, row["client_order_id"])
            receipt = receipts.get(client_order_id)
            if receipt is None:
                continue
            provider_order_id = receipt.provider_order_id
            order = _order_from_payload(self.artifacts.read_json(cast(str, row["order_hash"])))
            references = {client_order_id}
            if provider_order_id is not None:
                references.add(provider_order_id)
            if receipt.status in terminal:
                if references & open_order_references:
                    return False
            elif (
                receipt.status is ExecutionStatus.ACCEPTED
                and not references & open_order_references
            ):
                return False
            reflected_fills: list[RecentFill] = []
            for fill_id in receipt.fill_ids:
                fill = fills.get(fill_id)
                if (
                    fill is None
                    or fill.order_reference not in references
                    or fill.target_id != order.instrument_id
                    or fill.side is not order.side
                ):
                    return False
                reflected_fills.append(fill)
            if sum((item.quantity for item in reflected_fills), Decimal(0)) != (
                receipt.filled_quantity
            ):
                return False
            sizing_value = self.artifacts.read_json(cast(str, row["sizing_decision_hash"]))
            if not isinstance(sizing_value, dict):
                return False
            sizing_payload = cast(dict[str, object], sizing_value)
            legs_value = sizing_payload.get("legs")
            if not isinstance(legs_value, list):
                return False
            matching: list[dict[str, object]] = []
            for item_value in cast(list[object], legs_value):
                if not isinstance(item_value, dict):
                    continue
                item = cast(dict[str, object], item_value)
                if item.get("instrument_id") == order.instrument_id and item.get("role") == cast(
                    str, row["leg_role"]
                ):
                    matching.append(item)
            basis_value = self.artifacts.read_json(cast(str, row["price_basis_hash"]))
            if len(matching) != 1 or not isinstance(basis_value, dict):
                return False
            basis_payload = cast(dict[str, object], basis_value)
            try:
                current_notional = Decimal(cast(str, matching[0]["current_signed_notional"]))
                price = Decimal(cast(str, basis_payload["price"]))
            except (KeyError, ValueError):
                return False
            expected_quantity = current_notional / price
            expected_quantity += (
                receipt.filled_quantity if order.side is Side.BUY else -receipt.filled_quantity
            )
            if positions.get(order.instrument_id, Decimal(0)) != expected_quantity:
                return False
        return True

    def _evaluate_current_risk(
        self,
        evaluated_at: datetime,
        *,
        fail_closed: bool,
    ) -> AutonomousRiskMeasurementV2 | None:
        try:
            account_state = self.account_state_source()
            exposure_view = self.exposure_view_source()
            current_equity, valid_until = self._authoritative_equity(
                account_state=account_state,
                exposure_view=exposure_view,
                evaluated_at=evaluated_at,
            )
            account_hash = canonical_hash(account_state.to_dict())
            exposure_hash = canonical_hash(exposure_view.to_dict())
            mandate_hash = canonical_hash(self.mandate.to_dict())
            with self.store.authority_transaction() as connection:
                row = connection.execute(
                    """
                    SELECT * FROM autonomous_risk_days
                    WHERE harness_authority_id = ? AND mandate_hash = ?
                      AND account_reference_hash = ?
                    """,
                    (self.harness_authority_id, mandate_hash, self.mandate.account_id),
                ).fetchone()
                if row is None:
                    raise PermissionError("mandate risk day has not been initialized")
                day_start_equity = Decimal(cast(str, row["day_start_equity"]))
                prior_peak = Decimal(cast(str, row["peak_equity"]))
                peak_equity = max(prior_peak, current_equity)
                connection.execute(
                    """
                    UPDATE autonomous_risk_days
                    SET peak_equity = ?, last_account_state_hash = ?,
                        last_exposure_view_hash = ?, last_observed_at = ?
                    WHERE harness_authority_id = ? AND mandate_hash = ?
                      AND account_reference_hash = ?
                    """,
                    (
                        str(peak_equity),
                        account_hash,
                        exposure_hash,
                        _timestamp(evaluated_at),
                        self.harness_authority_id,
                        mandate_hash,
                        self.mandate.account_id,
                    ),
                )
            daily_pnl = current_equity - day_start_equity
            drawdown = peak_equity - current_equity
            source = {
                "mandate_hash": mandate_hash,
                "account_state_hash": account_hash,
                "exposure_view_hash": exposure_hash,
                "day_start_equity": str(day_start_equity),
                "peak_equity": str(peak_equity),
                "external_cash_flow": "0",
            }
            core = {
                "schema_version": "market-impact.autonomous-risk-measurement.v2",
                "mandate_id": self.mandate.mandate_id,
                "account_reference_hash": self.mandate.account_id,
                "daily_pnl": str(daily_pnl),
                "strategy_peak_drawdown": str(drawdown),
                "source_snapshot_hash": canonical_hash(source),
                "observed_at": _timestamp(evaluated_at),
                "valid_until": _timestamp(valid_until),
            }
            measurement = AutonomousRiskMeasurementV2(
                measurement_id="autonomous-risk-measurement-" + canonical_hash(core),
                mandate_id=self.mandate.mandate_id,
                account_reference_hash=self.mandate.account_id,
                daily_pnl=daily_pnl,
                strategy_peak_drawdown=drawdown,
                source_snapshot_hash=canonical_hash(source),
                observed_at=evaluated_at,
                valid_until=valid_until,
            )
        except Exception:
            self._activate_kill("stale_risk_measurement", evaluated_at)
            if fail_closed:
                raise PermissionError(
                    "fresh authoritative risk measurement is unavailable"
                ) from None
            return None
        self._deactivate_kill("stale_risk_measurement", evaluated_at)
        if measurement.daily_pnl <= -self.mandate.daily_loss_kill_threshold:
            self._activate_kill("daily_loss_threshold_exceeded", evaluated_at)
        if measurement.strategy_peak_drawdown >= self.mandate.strategy_peak_drawdown_kill_threshold:
            self._activate_kill(
                "strategy_peak_drawdown_threshold_exceeded",
                evaluated_at,
            )
        return measurement

    def _initialize_risk_day(self) -> None:
        now = self._now()
        try:
            account_state = self.account_state_source()
            exposure_view = self.exposure_view_source()
            current_equity, _ = self._authoritative_equity(
                account_state=account_state,
                exposure_view=exposure_view,
                evaluated_at=now,
            )
        except Exception:
            return
        mandate_hash = canonical_hash(self.mandate.to_dict())
        account_hash = canonical_hash(account_state.to_dict())
        exposure_hash = canonical_hash(exposure_view.to_dict())
        with self.store.authority_transaction() as connection:
            connection.execute(
                """
                INSERT OR IGNORE INTO autonomous_risk_days (
                    harness_authority_id, mandate_hash, account_reference_hash,
                    day_start_equity, peak_equity,
                    initial_account_state_hash, initial_exposure_view_hash,
                    last_account_state_hash, last_exposure_view_hash,
                    initialized_at, last_observed_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    self.harness_authority_id,
                    mandate_hash,
                    self.mandate.account_id,
                    str(current_equity),
                    str(current_equity),
                    account_hash,
                    exposure_hash,
                    account_hash,
                    exposure_hash,
                    _timestamp(now),
                    _timestamp(now),
                ),
            )

    def _authoritative_equity(
        self,
        *,
        account_state: AccountStateSnapshot,
        exposure_view: PortfolioExposureViewV2,
        evaluated_at: datetime,
    ) -> tuple[Decimal, datetime]:
        self.exposure_view_authority.assert_authoritative_exposure_view(exposure_view)
        if (
            account_state.account_reference_hash != self.mandate.account_id
            or account_state.provider_id != self.provider.manifest.provider_id
            or account_state.provider_version != self.provider.manifest.provider_version
            or account_state.provider_manifest_hash
            != canonical_hash(self.provider.manifest.to_dict())
            or account_state.environment is not TradingEnvironment.PAPER
        ):
            raise PermissionError("risk state does not bind the exact account and Provider")
        readiness = account_state.readiness(
            evaluated_at=evaluated_at,
            max_age=self.account_state_max_age,
        )
        if not readiness.risk_observation_ready:
            raise PermissionError("risk state is stale or incomplete")
        if not exposure_view.observed_at <= evaluated_at < exposure_view.valid_until:
            raise PermissionError("risk Exposure View is stale")
        projected = account_state.project_positions(
            evaluated_at=account_state.reconciled_at,
            max_age=self.account_state_max_age,
        )
        if (
            exposure_view.position_snapshot_id != projected.snapshot_id
            or exposure_view.position_snapshot_hash != canonical_hash(projected.to_dict())
        ):
            raise PermissionError("risk exposure does not derive from exact Account State")
        if account_state.cash is None:
            raise PermissionError("risk state requires complete cash")
        balances = tuple(
            item for item in account_state.cash if item.currency == self.mandate.currency
        )
        if len(balances) != 1:
            raise PermissionError("risk state requires one mandate-currency cash balance")
        valid_until = min(
            exposure_view.valid_until,
            account_state.as_of + self.account_state_max_age,
            max(account_state.reconciled_at, exposure_view.observed_at)
            + AUTONOMOUS_RISK_OBSERVATION_MAX_AGE,
        )
        if evaluated_at >= valid_until:
            raise PermissionError("risk observation is stale")
        return balances[0].settled + exposure_view.current_net_exposure, valid_until

    def _assert_reservation_budget(
        self,
        connection: sqlite3.Connection,
        *,
        exposure_view: PortfolioExposureViewV2,
        account_state: AccountStateSnapshot,
        instrument_id: str,
        signed_delta: Decimal,
        gross_delta: Decimal,
        turnover_reserved: Decimal,
        cash_reserved: Decimal,
        position_count_delta: int,
    ) -> None:
        rows = connection.execute(
            """
            SELECT signed_delta, gross_delta, turnover_reserved, cash_reserved,
                   position_count_delta, order_hash
            FROM autonomous_operations
            WHERE reservation_active = 1 AND mandate_hash = ?
            """,
            (canonical_hash(self.mandate.to_dict()),),
        ).fetchall()
        activity_rows = connection.execute(
            """
            SELECT turnover_reserved
            FROM autonomous_operations
            WHERE mandate_hash = ? AND (reservation_active = 1 OR submission_consumed = 1)
            """,
            (canonical_hash(self.mandate.to_dict()),),
        ).fetchall()
        exposure_hash = canonical_hash(exposure_view.to_dict())
        reused = connection.execute(
            "SELECT 1 FROM autonomous_exposure_generations WHERE exposure_view_hash = ?",
            (exposure_hash,),
        ).fetchone()
        if reused is not None:
            raise PermissionError(
                "Portfolio Exposure View was already consumed by another decision"
            )
        for row in rows:
            payload = self.artifacts.read_json(cast(str, row["order_hash"]))
            if not isinstance(payload, dict):
                raise TypeError("reserved Order Intent is not an object")
            if cast(dict[str, object], payload).get("instrument_id") == instrument_id:
                raise PermissionError(
                    "instrument already has an outstanding autonomous reservation"
                )
        reserved_signed = sum(
            (Decimal(cast(str, row["signed_delta"])) for row in rows),
            Decimal(0),
        )
        reserved_gross = sum(
            (Decimal(cast(str, row["gross_delta"])) for row in rows),
            Decimal(0),
        )
        reserved_cash = sum(
            (Decimal(cast(str, row["cash_reserved"])) for row in rows),
            Decimal(0),
        )
        reserved_count = sum(cast(int, row["position_count_delta"]) for row in rows)
        durable_submissions = len(activity_rows)
        durable_turnover = sum(
            (Decimal(cast(str, row["turnover_reserved"])) for row in activity_rows),
            Decimal(0),
        )
        if (
            exposure_view.daily_submissions_used + durable_submissions + 1
            > self.mandate.daily_submission_limit
        ):
            raise PermissionError("daily submission budget exhausted by durable reservations")
        if (
            exposure_view.daily_turnover_used + durable_turnover + turnover_reserved
            > self.mandate.daily_turnover_limit
        ):
            raise PermissionError("daily turnover budget exhausted by durable reservations")
        projected_gross = exposure_view.current_gross_exposure + reserved_gross + gross_delta
        projected_net = exposure_view.current_net_exposure + reserved_signed + signed_delta
        projected_count = (
            len(exposure_view.marked_positions) + reserved_count + position_count_delta
        )
        if projected_gross > self.mandate.gross_exposure_limit or projected_gross < 0:
            raise PermissionError("gross exposure budget exhausted by durable reservations")
        if (
            not self.mandate.minimum_net_exposure
            <= projected_net
            <= self.mandate.maximum_net_exposure
        ):
            raise PermissionError("net exposure budget exhausted by durable reservations")
        if projected_count > self.mandate.maximum_position_count or projected_count < 0:
            raise PermissionError("position-count budget exhausted by durable reservations")
        balances = (
            ()
            if account_state.cash is None
            else tuple(
                item for item in account_state.cash if item.currency == self.mandate.currency
            )
        )
        if len(balances) != 1:
            raise PermissionError("cash authority is not unique for reservation")
        cash_limit = min(balances[0].available, balances[0].settled)
        if reserved_cash + cash_reserved > cash_limit:
            raise PermissionError("cash budget exhausted by durable reservations")
        connection.execute(
            "INSERT INTO autonomous_exposure_generations VALUES (?, ?)",
            (exposure_hash, _timestamp(self._now())),
        )

    def _validate_submission_capability(self, capability: SubmissionCapability) -> bool:
        if self._closed or not self._runtime_lease.owned_by_current_process:
            return False
        evaluated_at = self._now()
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM autonomous_operations WHERE client_order_id = ?",
                (capability.order.client_order_id,),
            ).fetchone()
        if row is None:
            return False
        action = PortfolioAction(cast(str, row["action"]))
        try:
            reopened_lease = self.provider_lease_authority.resolve(self.provider_lease.lease_id)
        except (KeyError, OSError, TypeError, ValueError):
            return False
        acceptance_current = (
            self.provider_lease.allows_risk_reduction(evaluated_at)
            if action in {PortfolioAction.REDUCE, PortfolioAction.CLOSE}
            else self.provider_lease.is_current(evaluated_at)
        )
        return bool(
            cast(str, row["state"]) == AutonomousOperationState.SUBMITTING.value
            and bool(row["reservation_active"])
            and bool(row["submission_consumed"])
            and cast(str | None, row["lease_token"]) == capability.submission_id
            and capability.provider_id == self.provider.manifest.provider_id
            and capability.provider_version == self.provider.manifest.provider_version
            and reopened_lease == self.provider_lease
            and reopened_lease.mandate_hash == self.mandate_hash
            and acceptance_current
            and cast(str, row["provider_acceptance_hash"])
            == canonical_hash(self.provider_lease.to_dict())
            and canonical_hash(capability.order.to_dict()) == capability.order_hash
            and cast(str, row["order_hash"]) == capability.order_hash
            and cast(str, row["mandate_hash"]) == capability.mandate_hash
            and capability.mandate_hash == canonical_hash(self.mandate.to_dict())
            and cast(str, row["price_basis_hash"]) == capability.price_basis_hash
            and cast(str, row["policy_evaluation_hash"]) == capability.policy_evaluation_hash
            and cast(str, row["approval_hash"]) == capability.approval_hash
            and self.mandate.valid_from <= evaluated_at < self.mandate.valid_until
            and capability.order.created_at <= evaluated_at < capability.order.expires_at
            and capability.order.account_id == self.mandate.account_id
            and capability.order.environment is TradingEnvironment.PAPER
        )

    def _validate_cancellation_capability(self, capability: object) -> bool:
        if self._closed or not self._runtime_lease.owned_by_current_process:
            return False
        attempt_id = getattr(capability, "attempt_id", None)
        cancellation_id = getattr(capability, "cancellation_id", None)
        authority_call = self._cancellation_authority_call.get()
        if (
            authority_call is None
            or authority_call.lease_id != self.provider_lease.lease_id
            or authority_call.cancellation_id != cancellation_id
            or authority_call.attempt_id != attempt_id
        ):
            return False
        try:
            with self._connect() as connection:
                row = connection.execute(
                    "SELECT * FROM autonomous_cancellations WHERE cancellation_id = ?",
                    (cancellation_id,),
                ).fetchone()
                reopened = self.provider_lease_authority.resolve_claimed_mutation_in_transaction(
                    connection,
                    lease_id=authority_call.lease_id,
                    mutation_id=authority_call.attempt_id,
                    kind="cancel",
                )
        except (KeyError, OSError, sqlite3.Error, TypeError, ValueError):
            return False
        evaluated_at = self._now()
        return bool(
            row is not None
            and reopened == self.provider_lease
            and reopened.harness_authority_id == self.harness_authority_id
            and reopened.mandate_hash == self.mandate_hash
            and reopened.allows_risk_reduction(evaluated_at)
            and cast(str, row["state"]) == AutonomousCancellationState.CANCELING.value
            and cast(str | None, row["lease_token"]) == attempt_id
            and cast(str, row["client_order_id"]) == getattr(capability, "client_order_id", None)
            and cast(str, row["provider_order_id"])
            == getattr(capability, "provider_order_id", None)
        )

    def _provider_lease_reopens(self, evaluated_at: datetime, *, risk_reduction: bool) -> bool:
        try:
            reopened = self.provider_lease_authority.resolve(self.provider_lease.lease_id)
        except (KeyError, OSError, TypeError, ValueError):
            return False
        current = (
            reopened.allows_risk_reduction(evaluated_at)
            if risk_reduction
            else reopened.is_current(evaluated_at)
        )
        return bool(
            reopened == self.provider_lease
            and reopened.harness_authority_id == self.harness_authority_id
            and reopened.mandate_hash == self.mandate_hash
            and current
        )

    def _recover_interrupted_operations(self) -> None:
        now = self._now()
        with self.store.authority_transaction() as connection:
            submissions = connection.execute(
                """
                UPDATE autonomous_operations
                SET state = ?, lease_token = NULL, updated_at = ? WHERE state = ?
                """,
                (
                    AutonomousOperationState.UNKNOWN.value,
                    _timestamp(now),
                    AutonomousOperationState.SUBMITTING.value,
                ),
            ).rowcount
            cancellations = connection.execute(
                """
                UPDATE autonomous_cancellations
                SET state = ?, lease_token = NULL, updated_at = ? WHERE state = ?
                """,
                (
                    AutonomousCancellationState.UNKNOWN.value,
                    _timestamp(now),
                    AutonomousCancellationState.CANCELING.value,
                ),
            ).rowcount
            claimed = connection.execute(
                """
                SELECT active_mutation_id FROM autonomous_provider_acceptances
                WHERE lease_id = ? AND harness_authority_id = ?
                  AND active_mutation_id IS NOT NULL
                """,
                (self.provider_lease.lease_id, self.harness_authority_id),
            ).fetchone()
            if claimed is not None:
                connection.execute(
                    """
                    UPDATE autonomous_provider_acceptances
                    SET active_mutation_id = NULL, active_mutation_kind = NULL,
                        active_mutation_started_at = NULL,
                        revoked_at = CASE WHEN revoke_requested = 1 THEN ? ELSE revoked_at END
                    WHERE lease_id = ? AND harness_authority_id = ?
                    """,
                    (
                        _timestamp(now),
                        self.provider_lease.lease_id,
                        self.harness_authority_id,
                    ),
                )
            if submissions or cancellations or claimed is not None:
                for reason in ("unknown_ack", "incomplete_order_coverage"):
                    connection.execute(
                        """
                        INSERT INTO autonomous_kills (reason, active, updated_at)
                        VALUES (?, 1, ?)
                        ON CONFLICT(reason) DO UPDATE
                        SET active = 1, updated_at = excluded.updated_at
                        """,
                        (reason, _timestamp(now)),
                    )

    def _block_claim(
        self,
        row: sqlite3.Row,
        submission_id: str,
        at: datetime,
        error_kind: str,
    ) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                UPDATE autonomous_operations
                SET state = ?, lease_token = NULL, reservation_active = 0,
                    submission_consumed = 0, updated_at = ?
                WHERE operation_id = ? AND lease_token = ?
                """,
                (
                    AutonomousOperationState.BLOCKED.value,
                    _timestamp(at),
                    cast(str, row["operation_id"]),
                    submission_id,
                ),
            )
            connection.execute(
                """
                UPDATE autonomous_submission_attempts SET state = ?, finished_at = ?
                WHERE submission_id = ?
                """,
                (error_kind, _timestamp(at), submission_id),
            )

    def _finish_unknown(
        self,
        row: sqlite3.Row,
        submission_id: str,
        at: datetime,
        error_kind: str,
    ) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                UPDATE autonomous_operations
                SET state = ?, lease_token = NULL, updated_at = ?
                WHERE operation_id = ? AND lease_token = ?
                """,
                (
                    AutonomousOperationState.UNKNOWN.value,
                    _timestamp(at),
                    cast(str, row["operation_id"]),
                    submission_id,
                ),
            )
            connection.execute(
                """
                UPDATE autonomous_submission_attempts SET state = ?, finished_at = ?
                WHERE submission_id = ?
                """,
                (f"unknown:{error_kind}", _timestamp(at), submission_id),
            )

    def _activate_kill(self, reason: str, at: datetime) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO autonomous_kills (reason, active, updated_at) VALUES (?, 1, ?)
                ON CONFLICT(reason) DO UPDATE SET active = 1, updated_at = excluded.updated_at
                """,
                (reason, _timestamp(at)),
            )

    def _deactivate_kill(self, reason: str, at: datetime) -> None:
        with self._connect() as connection:
            connection.execute(
                "UPDATE autonomous_kills SET active = 0, updated_at = ? WHERE reason = ?",
                (_timestamp(at), reason),
            )

    def _now(self) -> datetime:
        value = self.clock()
        require_aware(value, "now")
        return value.astimezone(UTC)

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.database_path)
        connection.row_factory = sqlite3.Row
        return connection

    def _initialize(self) -> None:
        with self._connect() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS autonomous_operations (
                    operation_id TEXT PRIMARY KEY,
                    client_order_id TEXT NOT NULL UNIQUE,
                    action TEXT NOT NULL,
                    leg_role TEXT NOT NULL,
                    order_hash TEXT NOT NULL,
                    proposal_hash TEXT NOT NULL,
                    portfolio_decision_hash TEXT NOT NULL,
                    sizing_decision_hash TEXT NOT NULL,
                    mandate_hash TEXT NOT NULL,
                    price_basis_hash TEXT NOT NULL,
                    provider_acceptance_hash TEXT NOT NULL,
                    policy_evaluation_hash TEXT NOT NULL,
                    mandate_binding_hash TEXT NOT NULL,
                    approval_hash TEXT NOT NULL,
                    risk_measurement_hash TEXT NOT NULL,
                    exposure_view_hash TEXT NOT NULL,
                    signed_delta TEXT NOT NULL,
                    gross_delta TEXT NOT NULL,
                    turnover_reserved TEXT NOT NULL,
                    cash_reserved TEXT NOT NULL,
                    position_count_delta INTEGER NOT NULL,
                    reservation_active INTEGER NOT NULL,
                    submission_consumed INTEGER NOT NULL,
                    state TEXT NOT NULL,
                    provider_order_id TEXT,
                    provider_status TEXT,
                    lease_token TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS autonomous_service_authority (
                    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                    harness_authority_id TEXT NOT NULL UNIQUE
                );
                CREATE TABLE IF NOT EXISTS autonomous_submission_attempts (
                    submission_id TEXT PRIMARY KEY,
                    operation_id TEXT NOT NULL REFERENCES autonomous_operations(operation_id),
                    state TEXT NOT NULL,
                    receipt_hash TEXT,
                    started_at TEXT NOT NULL,
                    finished_at TEXT
                );
                CREATE TABLE IF NOT EXISTS autonomous_cancellations (
                    cancellation_id TEXT PRIMARY KEY,
                    operation_id TEXT NOT NULL REFERENCES autonomous_operations(operation_id),
                    client_order_id TEXT NOT NULL,
                    provider_order_id TEXT NOT NULL,
                    state TEXT NOT NULL,
                    lease_token TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS autonomous_kills (
                    reason TEXT PRIMARY KEY,
                    active INTEGER NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS autonomous_exposure_generations (
                    exposure_view_hash TEXT PRIMARY KEY,
                    first_used_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS autonomous_reconciliations (
                    reconciliation_hash TEXT PRIMARY KEY,
                    complete INTEGER NOT NULL,
                    observed_at TEXT NOT NULL,
                    payload_hash TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS autonomous_risk_days (
                    harness_authority_id TEXT NOT NULL,
                    mandate_hash TEXT NOT NULL,
                    account_reference_hash TEXT NOT NULL,
                    day_start_equity TEXT NOT NULL,
                    peak_equity TEXT NOT NULL,
                    initial_account_state_hash TEXT NOT NULL,
                    initial_exposure_view_hash TEXT NOT NULL,
                    last_account_state_hash TEXT NOT NULL,
                    last_exposure_view_hash TEXT NOT NULL,
                    initialized_at TEXT NOT NULL,
                    last_observed_at TEXT NOT NULL,
                    PRIMARY KEY (
                        harness_authority_id, mandate_hash, account_reference_hash
                    )
                );
                """
            )
            connection.execute(
                """
                INSERT OR IGNORE INTO autonomous_service_authority(
                    singleton, harness_authority_id
                ) VALUES (1, ?)
                """,
                (self.harness_authority_id,),
            )
            authority_row = connection.execute(
                "SELECT harness_authority_id FROM autonomous_service_authority WHERE singleton = 1"
            ).fetchone()
            if (
                authority_row is None
                or cast(str, authority_row["harness_authority_id"]) != self.harness_authority_id
            ):
                raise PermissionError("autonomous tables bind another Harness authority root")
            columns = {
                cast(str, row["name"])
                for row in connection.execute("PRAGMA table_info(autonomous_operations)")
            }
            migrations = {
                "risk_measurement_hash": "TEXT NOT NULL DEFAULT ''",
                "exposure_view_hash": "TEXT NOT NULL DEFAULT ''",
                "signed_delta": "TEXT NOT NULL DEFAULT '0'",
                "gross_delta": "TEXT NOT NULL DEFAULT '0'",
                "turnover_reserved": "TEXT NOT NULL DEFAULT '0'",
                "cash_reserved": "TEXT NOT NULL DEFAULT '0'",
                "position_count_delta": "INTEGER NOT NULL DEFAULT 0",
                "reservation_active": "INTEGER NOT NULL DEFAULT 1",
                "submission_consumed": "INTEGER NOT NULL DEFAULT 0",
            }
            for name, declaration in migrations.items():
                if name not in columns:
                    connection.execute(
                        f"ALTER TABLE autonomous_operations ADD COLUMN {name} {declaration}"
                    )


def _assert_autonomous_mandate(mandate: TradingMandateV2) -> None:
    expected = (
        mandate.environment is TradingEnvironment.PAPER,
        mandate.approval_mode is ApprovalMode.AUTONOMOUS,
        mandate.currency == "USD",
        mandate.gross_exposure_limit == 10_000,
        mandate.minimum_net_exposure == -10_000,
        mandate.maximum_net_exposure == 10_000,
        mandate.maximum_position_count == 10,
        mandate.daily_turnover_limit == 50_000,
        mandate.daily_submission_limit == 50,
        mandate.daily_loss_kill_threshold == 300,
        mandate.strategy_peak_drawdown_kill_threshold == 1_000,
        mandate.valid_until - mandate.valid_from <= timedelta(days=1),
    )
    if not all(expected):
        raise PermissionError("Trading Mandate v2 is not the accepted one-day autonomous envelope")


def _position_count_delta(*, current: Decimal, target: Decimal) -> int:
    if current == 0 and target != 0:
        return 1
    if current != 0 and target == 0:
        return -1
    return 0


def _assert_chain(
    *,
    proposal: AgentPortfolioProposalV2,
    portfolio_decision: PortfolioDecisionV2,
    sizing_decision: OrderSizingDecisionV2,
    mandate: TradingMandateV2,
    exposure_view: PortfolioExposureViewV2,
    price_bases: Mapping[str, PriceBasis],
) -> None:
    if (
        portfolio_decision.outcome is not PortfolioDecisionOutcome.READY_FOR_SIZING
        or sizing_decision.outcome is not OrderSizingOutcome.READY
    ):
        raise PermissionError("autonomous admission requires ready v2 decisions")
    if portfolio_decision.proposal.to_dict() != proposal.to_dict():
        raise ValueError("Portfolio Decision v2 binds another Agent proposal")
    if (
        sizing_decision.portfolio_decision_id != portfolio_decision.decision_id
        or sizing_decision.portfolio_decision_hash != canonical_hash(portfolio_decision.to_dict())
        or sizing_decision.trading_mandate_hash != canonical_hash(mandate.to_dict())
        or sizing_decision.exposure_view_id != exposure_view.exposure_view_id
        or sizing_decision.exposure_view_hash != canonical_hash(exposure_view.to_dict())
        or len(sizing_decision.legs) != len(portfolio_decision.legs)
    ):
        raise ValueError("v2 proposal, decision, sizing, exposure, or mandate binding differs")
    for decision_leg, sized_leg, expected_price_hash in zip(
        portfolio_decision.legs,
        sizing_decision.legs,
        sizing_decision.price_basis_hashes,
        strict=True,
    ):
        if decision_leg.instrument_id != sized_leg.instrument_id:
            raise ValueError("sized leg instrument differs from Portfolio Decision v2")
        basis = price_bases.get(decision_leg.instrument_id)
        actual_hash = None if basis is None else canonical_hash(basis.to_dict())
        if actual_hash != expected_price_hash or sized_leg.price_basis_hash != expected_price_hash:
            raise ValueError("Order Sizing v2 does not bind the exact raw Price Basis")
    if proposal.requested_action is PortfolioAction.ROTATE:
        source = tuple(
            (decision_leg, sized_leg)
            for decision_leg, sized_leg in zip(
                portfolio_decision.legs, sizing_decision.legs, strict=True
            )
            if decision_leg.role is PortfolioLegRole.ROTATION_SOURCE
        )
        destination = tuple(
            (decision_leg, sized_leg)
            for decision_leg, sized_leg in zip(
                portfolio_decision.legs, sizing_decision.legs, strict=True
            )
            if decision_leg.role is PortfolioLegRole.ROTATION_DESTINATION
        )
        if (
            len(source) != 1
            or source[0][1].outcome is not OrderSizingOutcome.READY
            or len(destination) != 1
            or destination[0][1].outcome is not OrderSizingOutcome.REJECTED
            or "blocked_pending_source_reconciliation" not in destination[0][1].blockers
        ):
            raise PermissionError(
                "rotation destination must remain blocked until source reconciliation"
            )


def _assert_existing_operation_matches(
    row: sqlite3.Row,
    *,
    order_hash: str,
    sizing_hash: str,
    mandate_hash: str,
    acceptance_hash: str,
) -> None:
    if (
        cast(str, row["order_hash"]) != order_hash
        or cast(str, row["sizing_decision_hash"]) != sizing_hash
        or cast(str, row["mandate_hash"]) != mandate_hash
        or cast(str, row["provider_acceptance_hash"]) != acceptance_hash
    ):
        raise ValueError("autonomous operation identity conflict")


def _operation(row: sqlite3.Row) -> AutonomousPaperOperation:
    return AutonomousPaperOperation(
        operation_id=cast(str, row["operation_id"]),
        client_order_id=cast(str, row["client_order_id"]),
        action=PortfolioAction(cast(str, row["action"])),
        order_hash=cast(str, row["order_hash"]),
        proposal_hash=cast(str, row["proposal_hash"]),
        portfolio_decision_hash=cast(str, row["portfolio_decision_hash"]),
        sizing_decision_hash=cast(str, row["sizing_decision_hash"]),
        mandate_hash=cast(str, row["mandate_hash"]),
        price_basis_hash=cast(str, row["price_basis_hash"]),
        provider_acceptance_hash=cast(str, row["provider_acceptance_hash"]),
        policy_evaluation_hash=cast(str, row["policy_evaluation_hash"]),
        mandate_binding_hash=cast(str, row["mandate_binding_hash"]),
        approval_hash=cast(str, row["approval_hash"]),
        state=AutonomousOperationState(cast(str, row["state"])),
        provider_order_id=cast(str | None, row["provider_order_id"]),
        provider_status=cast(str | None, row["provider_status"]),
        updated_at=_datetime(cast(str, row["updated_at"])),
    )


def _order_payload(row: sqlite3.Row, artifacts: ArtifactStore) -> dict[str, object]:
    payload = artifacts.read_json(cast(str, row["order_hash"]))
    if not isinstance(payload, dict):
        raise TypeError("persisted Order Intent is not an object")
    return cast(dict[str, object], payload)


def _order_from_payload(payload: object) -> OrderIntent:
    if not isinstance(payload, dict):
        raise TypeError("Order Intent artifact must be an object")
    fields = cast(dict[str, object], payload)
    return OrderIntent(
        client_order_id=_string(fields, "client_order_id"),
        signal_id=_string(fields, "signal_id"),
        account_id=_string(fields, "account_id"),
        environment=TradingEnvironment(_string(fields, "environment")),
        instrument_id=_string(fields, "instrument_id"),
        side=Side(_string(fields, "side")),
        quantity=Decimal(_string(fields, "quantity")),
        order_kind=OrderKind(_string(fields, "order_kind")),
        limit_price=(
            None if fields.get("limit_price") is None else Decimal(_string(fields, "limit_price"))
        ),
        created_at=_datetime(_string(fields, "created_at")),
        expires_at=_datetime(_string(fields, "expires_at")),
    )


def _receipt_payload(receipt: ExecutionReceipt) -> dict[str, object]:
    return {
        "schema_version": "market-impact.execution-receipt.v2",
        "client_order_id": receipt.client_order_id,
        "provider_order_id": receipt.provider_order_id,
        "status": receipt.status.value,
        "observed_at": _timestamp(receipt.observed_at),
        "filled_quantity": str(receipt.filled_quantity),
        "fill_ids": list(receipt.fill_ids),
    }


def _string(fields: Mapping[str, object], name: str) -> str:
    value = fields.get(name)
    if not isinstance(value, str):
        raise TypeError(f"{name} must be a string")
    return value


def _timestamp(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _datetime(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))
