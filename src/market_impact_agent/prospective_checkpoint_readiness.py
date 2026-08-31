from __future__ import annotations

import json
import sqlite3
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from typing import Protocol, cast

from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.data_inputs import LocalDataSnapshotStore
from market_impact_agent.domain import require_aware
from market_impact_agent.observations import ObservationCapability
from market_impact_agent.prospective_collection_runtime import ProspectiveCollectionRuntime
from market_impact_agent.prospective_diagnostic import (
    PROSPECTIVE_DIAGNOSTIC_REGISTRATION_SCHEMA_V2,
    PROSPECTIVE_DIAGNOSTIC_REGISTRATION_SCHEMA_V3,
    PROSPECTIVE_DIAGNOSTIC_REGISTRATION_SCHEMA_V4,
    CapabilityApplicability,
    ProspectiveDiagnosticRegistration,
)

PROSPECTIVE_CHECKPOINT_ROUTE_PLAN_SCHEMA_V1 = "market-impact.prospective-checkpoint-route-plan.v1"
PROSPECTIVE_CHECKPOINT_ROUTE_PLAN_SCHEMA = "market-impact.prospective-checkpoint-route-plan.v2"
PROSPECTIVE_CHECKPOINT_ADMISSION_TIMING_PROTOCOL = "sqlite_begin_immediate_then_harness_clock_v1"
PROSPECTIVE_CHECKPOINT_READINESS_REPORT_SCHEMA = (
    "market-impact.prospective-checkpoint-readiness-report.v1"
)


class CheckpointReadinessStatus(StrEnum):
    TRIGGER_ROUTE_UNCONFIGURED = "trigger_route_unconfigured"
    WAITING_FOR_POST_ADMISSION_TRIGGER = "waiting_for_post_admission_trigger"
    UNCLASSIFIED_TRIGGER_CANDIDATE_OBSERVED = "unclassified_trigger_candidate_observed"


class CompletedTriageClassificationAuthority(Protocol):
    """Read-only boundary for versions formally decided or terminally handled."""

    def classified_version_ids(
        self,
        *,
        registration_id: str,
        checkpoint_key: str,
        route_plan_id: str,
        route_admission_id: str,
        at: datetime,
    ) -> tuple[str, ...]: ...


@dataclass(frozen=True, slots=True)
class ProspectiveCheckpointRouteBinding:
    checkpoint_key: str
    capability: ObservationCapability
    route_kind: str
    job_id: str

    def __post_init__(self) -> None:
        _trimmed(self.checkpoint_key, "checkpoint route binding checkpoint_key")
        _trimmed(self.route_kind, "checkpoint route binding route_kind")
        _prefixed(
            self.job_id,
            "prospective-collection-job-",
            "checkpoint route binding job_id",
        )

    def to_dict(self) -> dict[str, str]:
        return {
            "checkpoint_key": self.checkpoint_key,
            "capability": self.capability.value,
            "route_kind": self.route_kind,
            "job_id": self.job_id,
        }


@dataclass(frozen=True, slots=True)
class ProspectiveCheckpointRoutePlan:
    plan_id: str
    registration_id: str
    bindings: tuple[ProspectiveCheckpointRouteBinding, ...]
    replaces_plan_id: str | None = None
    admission_timing_protocol: str = PROSPECTIVE_CHECKPOINT_ADMISSION_TIMING_PROTOCOL
    historical_pit_claim: bool = False
    model_calls_authorized: bool = False
    execution_capability: bool = False
    schema_version: str = PROSPECTIVE_CHECKPOINT_ROUTE_PLAN_SCHEMA

    def __post_init__(self) -> None:
        if self.schema_version not in {
            PROSPECTIVE_CHECKPOINT_ROUTE_PLAN_SCHEMA_V1,
            PROSPECTIVE_CHECKPOINT_ROUTE_PLAN_SCHEMA,
        }:
            raise ValueError("unsupported prospective checkpoint route plan schema")
        if self.schema_version == PROSPECTIVE_CHECKPOINT_ROUTE_PLAN_SCHEMA_V1:
            if self.replaces_plan_id is not None:
                raise ValueError("v1 checkpoint route plans cannot replace another plan")
        elif self.replaces_plan_id is not None:
            _prefixed(
                self.replaces_plan_id,
                "prospective-checkpoint-route-plan-",
                "checkpoint route plan replaces_plan_id",
            )
            if self.replaces_plan_id == self.plan_id:
                raise ValueError("checkpoint route plan cannot replace itself")
        if self.admission_timing_protocol != PROSPECTIVE_CHECKPOINT_ADMISSION_TIMING_PROTOCOL:
            raise ValueError("unsupported checkpoint route admission timing protocol")
        _prefixed(
            self.registration_id,
            "prospective-diagnostic-registration-",
            "checkpoint route plan registration_id",
        )
        keys = tuple(
            (item.checkpoint_key, item.capability.value, item.route_kind, item.job_id)
            for item in self.bindings
        )
        if keys != tuple(sorted(set(keys))):
            raise ValueError("checkpoint route plan bindings must be sorted and unique")
        if self.historical_pit_claim or self.model_calls_authorized or self.execution_capability:
            raise ValueError(
                "checkpoint route plan cannot grant PIT, model, or execution authority"
            )
        if self.plan_id != self.expected_plan_id:
            raise ValueError("prospective checkpoint route plan_id does not match content")

    @property
    def expected_plan_id(self) -> str:
        return f"prospective-checkpoint-route-plan-{canonical_hash(self.core_dict())}"

    def core_dict(self) -> dict[str, object]:
        core: dict[str, object] = {
            "schema_version": self.schema_version,
            "registration_id": self.registration_id,
            "admission_timing_protocol": self.admission_timing_protocol,
            "bindings": [item.to_dict() for item in self.bindings],
            "historical_pit_claim": self.historical_pit_claim,
            "model_calls_authorized": self.model_calls_authorized,
            "execution_capability": self.execution_capability,
        }
        if self.schema_version == PROSPECTIVE_CHECKPOINT_ROUTE_PLAN_SCHEMA:
            core["replaces_plan_id"] = self.replaces_plan_id
        return core

    def to_dict(self) -> dict[str, object]:
        return {**self.core_dict(), "plan_id": self.plan_id}

    @classmethod
    def build(
        cls,
        *,
        registration_id: str,
        bindings: tuple[ProspectiveCheckpointRouteBinding, ...],
        replaces_plan_id: str | None = None,
    ) -> ProspectiveCheckpointRoutePlan:
        ordered = tuple(
            sorted(
                bindings,
                key=lambda item: (
                    item.checkpoint_key,
                    item.capability.value,
                    item.route_kind,
                    item.job_id,
                ),
            )
        )
        core = {
            "schema_version": PROSPECTIVE_CHECKPOINT_ROUTE_PLAN_SCHEMA,
            "registration_id": registration_id,
            "replaces_plan_id": replaces_plan_id,
            "admission_timing_protocol": (PROSPECTIVE_CHECKPOINT_ADMISSION_TIMING_PROTOCOL),
            "bindings": [item.to_dict() for item in ordered],
            "historical_pit_claim": False,
            "model_calls_authorized": False,
            "execution_capability": False,
        }
        return cls(
            plan_id=f"prospective-checkpoint-route-plan-{canonical_hash(core)}",
            registration_id=registration_id,
            bindings=ordered,
            replaces_plan_id=replaces_plan_id,
        )


@dataclass(frozen=True, slots=True)
class ProspectiveCheckpointRouteAdmission:
    admission_id: str
    route_plan_id: str
    registration_id: str
    recorded_at: datetime

    def __post_init__(self) -> None:
        _prefixed(
            self.admission_id,
            "prospective-checkpoint-route-admission-",
            "checkpoint route admission_id",
        )
        _prefixed(
            self.route_plan_id,
            "prospective-checkpoint-route-plan-",
            "checkpoint route admission route_plan_id",
        )
        _strict_utc(self.recorded_at, "checkpoint route admission recorded_at")
        if self.admission_id != self.expected_admission_id:
            raise ValueError("prospective checkpoint route admission_id does not match content")

    @property
    def expected_admission_id(self) -> str:
        return f"prospective-checkpoint-route-admission-{canonical_hash(self.core_dict())}"

    def core_dict(self) -> dict[str, object]:
        return {
            "route_plan_id": self.route_plan_id,
            "registration_id": self.registration_id,
            "recorded_at": _timestamp(self.recorded_at),
        }

    def to_dict(self) -> dict[str, object]:
        return {**self.core_dict(), "admission_id": self.admission_id}


class ProspectiveCheckpointAdmissionStore:
    """Durable Harness-clock admission and versioned current-route authority."""

    def __init__(
        self,
        state_root: Path,
        *,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.store = LocalDataSnapshotStore(state_root)
        self.index_path = self.store.index_path
        self._clock = (lambda: datetime.now(UTC)) if clock is None else clock
        with self._connect() as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS prospective_checkpoint_route_admissions (
                    route_plan_id TEXT PRIMARY KEY,
                    admission_id TEXT NOT NULL UNIQUE,
                    registration_id TEXT NOT NULL,
                    artifact_hash TEXT NOT NULL UNIQUE,
                    recorded_at TEXT NOT NULL
                )
                """
            )
            _ensure_sqlite_column(
                connection,
                "prospective_checkpoint_route_admissions",
                "route_plan_artifact_hash",
                "TEXT",
            )
            _ensure_sqlite_column(
                connection,
                "prospective_checkpoint_route_admissions",
                "superseded_at",
                "TEXT",
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS prospective_checkpoint_route_heads (
                    registration_id TEXT PRIMARY KEY,
                    route_plan_id TEXT NOT NULL UNIQUE,
                    admission_id TEXT NOT NULL UNIQUE,
                    effective_from TEXT NOT NULL,
                    route_plan_artifact_hash TEXT NOT NULL
                )
                """
            )

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.index_path)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA journal_mode = WAL")
        connection.execute("PRAGMA synchronous = FULL")
        connection.execute("PRAGMA busy_timeout = 5000")
        return connection

    def admit(
        self,
        *,
        route_plan: ProspectiveCheckpointRoutePlan,
        registration: ProspectiveDiagnosticRegistration,
        runtime: ProspectiveCollectionRuntime,
    ) -> ProspectiveCheckpointRouteAdmission:
        if route_plan.registration_id != registration.registration_id:
            raise ValueError("checkpoint route plan belongs to a different registration")
        _validate_route_plan_structure(
            registration=registration,
            route_plan=route_plan,
            runtime=runtime,
        )
        route_plan_artifact = self.store.artifacts.put_json(route_plan.to_dict())
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                """
                SELECT admission_id, registration_id, artifact_hash, recorded_at,
                       route_plan_artifact_hash, superseded_at
                FROM prospective_checkpoint_route_admissions
                WHERE route_plan_id = ?
                """,
                (route_plan.plan_id,),
            ).fetchone()
            if row is not None:
                admission = self._verified_admission(route_plan.plan_id, row)
                self._bind_verified_plan_artifact(
                    connection,
                    route_plan=route_plan,
                    row=row,
                    artifact_hash=route_plan_artifact.content_hash,
                )
                head = self._head_row(connection, route_plan.registration_id)
                if head is None:
                    if row["superseded_at"] is not None:
                        raise ValueError("a superseded checkpoint route cannot become current")
                    connection.execute(
                        """
                        INSERT INTO prospective_checkpoint_route_heads(
                            registration_id, route_plan_id, admission_id, effective_from,
                            route_plan_artifact_hash
                        ) VALUES (?, ?, ?, ?, ?)
                        """,
                        (
                            route_plan.registration_id,
                            route_plan.plan_id,
                            admission.admission_id,
                            _timestamp(admission.recorded_at),
                            route_plan_artifact.content_hash,
                        ),
                    )
                elif cast(str, head["route_plan_id"]) == route_plan.plan_id:
                    self._verify_head(
                        head,
                        admission,
                        route_plan_artifact.content_hash,
                    )
                return admission

            head = self._head_row(connection, route_plan.registration_id)
            if head is None:
                legacy_count = cast(
                    int,
                    connection.execute(
                        """
                        SELECT COUNT(*)
                        FROM prospective_checkpoint_route_admissions
                        WHERE registration_id = ?
                        """,
                        (route_plan.registration_id,),
                    ).fetchone()[0],
                )
                if legacy_count:
                    raise ValueError(
                        "checkpoint route history has no current head; explicitly re-admit "
                        "one existing plan before creating a replacement"
                    )
                if route_plan.replaces_plan_id is not None:
                    raise ValueError("initial checkpoint route plan cannot name a predecessor")
            else:
                current_plan_id = cast(str, head["route_plan_id"])
                if route_plan.schema_version != PROSPECTIVE_CHECKPOINT_ROUTE_PLAN_SCHEMA:
                    raise ValueError("only a v2 checkpoint route plan can replace the current plan")
                if route_plan.replaces_plan_id != current_plan_id:
                    raise ValueError("checkpoint route replacement predecessor is not current")

            recorded_at = self._clock()
            _strict_utc(recorded_at, "checkpoint route admission Harness clock")
            if recorded_at < registration.registered_at:
                raise ValueError("checkpoint routes cannot be admitted before the registration")
            if head is not None and recorded_at <= _datetime(
                head["effective_from"], "checkpoint route predecessor effective_from"
            ):
                raise ValueError("checkpoint route replacement must start after its predecessor")
            core = {
                "route_plan_id": route_plan.plan_id,
                "registration_id": route_plan.registration_id,
                "recorded_at": _timestamp(recorded_at),
            }
            admission = ProspectiveCheckpointRouteAdmission(
                admission_id=("prospective-checkpoint-route-admission-" + canonical_hash(core)),
                route_plan_id=route_plan.plan_id,
                registration_id=route_plan.registration_id,
                recorded_at=recorded_at,
            )
            artifact = self.store.artifacts.put_json(admission.to_dict())
            connection.execute(
                """
                INSERT INTO prospective_checkpoint_route_admissions(
                    route_plan_id, admission_id, registration_id, artifact_hash, recorded_at,
                    route_plan_artifact_hash, superseded_at
                ) VALUES (?, ?, ?, ?, ?, ?, NULL)
                """,
                (
                    route_plan.plan_id,
                    admission.admission_id,
                    admission.registration_id,
                    artifact.content_hash,
                    _timestamp(recorded_at),
                    route_plan_artifact.content_hash,
                ),
            )
            if head is None:
                connection.execute(
                    """
                    INSERT INTO prospective_checkpoint_route_heads(
                        registration_id, route_plan_id, admission_id, effective_from,
                        route_plan_artifact_hash
                    ) VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        route_plan.registration_id,
                        route_plan.plan_id,
                        admission.admission_id,
                        _timestamp(recorded_at),
                        route_plan_artifact.content_hash,
                    ),
                )
            else:
                predecessor_id = cast(str, head["route_plan_id"])
                superseded = connection.execute(
                    """
                    UPDATE prospective_checkpoint_route_admissions
                    SET superseded_at = ?
                    WHERE route_plan_id = ? AND superseded_at IS NULL
                    """,
                    (_timestamp(recorded_at), predecessor_id),
                )
                if superseded.rowcount != 1:
                    raise ValueError("current checkpoint route interval is corrupt")
                swapped = connection.execute(
                    """
                    UPDATE prospective_checkpoint_route_heads
                    SET route_plan_id = ?, admission_id = ?, effective_from = ?,
                        route_plan_artifact_hash = ?
                    WHERE registration_id = ? AND route_plan_id = ?
                    """,
                    (
                        route_plan.plan_id,
                        admission.admission_id,
                        _timestamp(recorded_at),
                        route_plan_artifact.content_hash,
                        route_plan.registration_id,
                        predecessor_id,
                    ),
                )
                if swapped.rowcount != 1:
                    raise ValueError("checkpoint route head changed during replacement")
        return admission

    def admission(self, route_plan_id: str) -> ProspectiveCheckpointRouteAdmission:
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT admission_id, registration_id, artifact_hash, recorded_at,
                       route_plan_artifact_hash, superseded_at
                FROM prospective_checkpoint_route_admissions
                WHERE route_plan_id = ?
                """,
                (route_plan_id,),
            ).fetchone()
        if row is None:
            raise KeyError(f"checkpoint route plan is not durably admitted: {route_plan_id}")
        return self._verified_admission(route_plan_id, row)

    def current_plan_id(self, registration_id: str) -> str:
        with self._connect() as connection:
            row = self._head_row(connection, registration_id)
        if row is None:
            raise KeyError(f"checkpoint route registration has no current head: {registration_id}")
        return cast(str, row["route_plan_id"])

    def assert_effective(
        self,
        *,
        route_plan_id: str,
        admission_id: str,
        registration_id: str,
        at: datetime,
    ) -> None:
        _strict_utc(at, "checkpoint route effective-at timestamp")
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT admission_id, registration_id, artifact_hash, recorded_at,
                       route_plan_artifact_hash, superseded_at
                FROM prospective_checkpoint_route_admissions
                WHERE route_plan_id = ?
                """,
                (route_plan_id,),
            ).fetchone()
            head = self._head_row(connection, registration_id)
        if row is None:
            raise KeyError(f"checkpoint route plan is not durably admitted: {route_plan_id}")
        if cast(str, row["admission_id"]) != admission_id:
            raise ValueError("checkpoint route admission identity does not match durable state")
        if cast(str, row["registration_id"]) != registration_id:
            raise ValueError("checkpoint route admission belongs to a different registration")
        admission = self._verified_admission(route_plan_id, row)
        route_plan_artifact_hash = cast(str | None, row["route_plan_artifact_hash"])
        if route_plan_artifact_hash is None:
            raise ValueError("checkpoint route plan has no durable CAS artifact")
        route_plan = prospective_checkpoint_route_plan_from_dict(
            self.store.artifacts.read_json(route_plan_artifact_hash)
        )
        if (
            route_plan.plan_id != route_plan_id
            or route_plan.registration_id != admission.registration_id
        ):
            raise ValueError("checkpoint route plan CAS artifact does not match durable state")
        effective_from = _datetime(row["recorded_at"], "checkpoint route effective_from")
        effective_to = (
            None
            if row["superseded_at"] is None
            else _datetime(row["superseded_at"], "checkpoint route effective_to")
        )
        if at < effective_from or (effective_to is not None and at >= effective_to):
            raise ValueError("checkpoint route plan is not effective at the requested time")
        if effective_to is None:
            if head is None:
                raise ValueError("checkpoint route plan has no authoritative effective interval")
            if cast(str, head["registration_id"]) != registration_id:
                raise ValueError("checkpoint route head belongs to a different registration")
            self._verify_head(head, admission, route_plan_artifact_hash)
        else:
            with self._connect() as connection:
                successor_rows = connection.execute(
                    """
                    SELECT route_plan_id, admission_id, registration_id, artifact_hash,
                           recorded_at, route_plan_artifact_hash, superseded_at
                    FROM prospective_checkpoint_route_admissions
                    WHERE registration_id = ? AND recorded_at = ? AND route_plan_id != ?
                    """,
                    (registration_id, _timestamp(effective_to), route_plan_id),
                ).fetchall()
            verified_successors = 0
            for successor_row in successor_rows:
                successor_plan_hash = cast(str | None, successor_row["route_plan_artifact_hash"])
                if successor_plan_hash is None:
                    continue
                successor_admission = self._verified_admission(
                    cast(str, successor_row["route_plan_id"]), successor_row
                )
                successor_plan = prospective_checkpoint_route_plan_from_dict(
                    self.store.artifacts.read_json(successor_plan_hash)
                )
                if (
                    successor_plan.plan_id == successor_admission.route_plan_id
                    and successor_plan.registration_id == registration_id
                    and successor_plan.replaces_plan_id == route_plan_id
                ):
                    verified_successors += 1
            if verified_successors != 1:
                raise ValueError(
                    "checkpoint route effective interval lacks one authenticated successor"
                )

    def _verified_admission(
        self,
        route_plan_id: str,
        row: sqlite3.Row,
    ) -> ProspectiveCheckpointRouteAdmission:
        admission = _admission_from_row(route_plan_id, row)
        artifact_hash = cast(str, row["artifact_hash"])
        if self.store.artifacts.read_json(artifact_hash) != admission.to_dict():
            raise ValueError("checkpoint route admission CAS artifact does not match durable state")
        return admission

    @staticmethod
    def _head_row(
        connection: sqlite3.Connection,
        registration_id: str,
    ) -> sqlite3.Row | None:
        return connection.execute(
            """
            SELECT registration_id, route_plan_id, admission_id, effective_from,
                   route_plan_artifact_hash
            FROM prospective_checkpoint_route_heads
            WHERE registration_id = ?
            """,
            (registration_id,),
        ).fetchone()

    def _bind_verified_plan_artifact(
        self,
        connection: sqlite3.Connection,
        *,
        route_plan: ProspectiveCheckpointRoutePlan,
        row: sqlite3.Row,
        artifact_hash: str,
    ) -> None:
        stored_hash = cast(str | None, row["route_plan_artifact_hash"])
        if stored_hash is None:
            connection.execute(
                """
                UPDATE prospective_checkpoint_route_admissions
                SET route_plan_artifact_hash = ?
                WHERE route_plan_id = ? AND route_plan_artifact_hash IS NULL
                """,
                (artifact_hash, route_plan.plan_id),
            )
            return
        if stored_hash != artifact_hash:
            raise ValueError("checkpoint route plan artifact hash does not match durable state")
        if self.store.artifacts.read_json(stored_hash) != route_plan.to_dict():
            raise ValueError("checkpoint route plan CAS artifact does not match durable state")

    @staticmethod
    def _verify_head(
        head: sqlite3.Row,
        admission: ProspectiveCheckpointRouteAdmission,
        route_plan_artifact_hash: str,
    ) -> None:
        if (
            cast(str, head["route_plan_id"]) != admission.route_plan_id
            or cast(str, head["admission_id"]) != admission.admission_id
            or cast(str, head["effective_from"]) != _timestamp(admission.recorded_at)
            or cast(str, head["route_plan_artifact_hash"]) != route_plan_artifact_hash
        ):
            raise ValueError("checkpoint route head does not match its admission")


@dataclass(frozen=True, slots=True)
class ProspectiveCheckpointReadiness:
    checkpoint_key: str
    status: CheckpointReadinessStatus
    operational_trigger_route_job_ids: tuple[str, ...]
    trigger_candidate_version_ids: tuple[str, ...]
    latest_trigger_available_at: datetime | None
    blocking_gaps: tuple[str, ...]
    information_gaps: tuple[str, ...]

    def __post_init__(self) -> None:
        _trimmed(self.checkpoint_key, "checkpoint readiness checkpoint_key")
        _sorted_unique(
            self.operational_trigger_route_job_ids,
            "checkpoint readiness trigger route jobs",
        )
        _sorted_unique(
            self.trigger_candidate_version_ids,
            "checkpoint readiness trigger candidates",
        )
        _sorted_unique(self.blocking_gaps, "checkpoint readiness blocking gaps")
        _sorted_unique(self.information_gaps, "checkpoint readiness information gaps")
        if set(self.blocking_gaps) & set(self.information_gaps):
            raise ValueError("checkpoint readiness gaps cannot be both blocking and informational")
        if self.latest_trigger_available_at is not None:
            _strict_utc(
                self.latest_trigger_available_at,
                "checkpoint readiness latest_trigger_available_at",
            )
        if bool(self.trigger_candidate_version_ids) != (
            self.latest_trigger_available_at is not None
        ):
            raise ValueError("checkpoint readiness candidate time does not match candidates")
        expected_status = (
            CheckpointReadinessStatus.TRIGGER_ROUTE_UNCONFIGURED
            if not self.operational_trigger_route_job_ids
            else (
                CheckpointReadinessStatus.UNCLASSIFIED_TRIGGER_CANDIDATE_OBSERVED
                if self.trigger_candidate_version_ids
                else CheckpointReadinessStatus.WAITING_FOR_POST_ADMISSION_TRIGGER
            )
        )
        if self.status is not expected_status:
            raise ValueError("checkpoint readiness status does not match observed state")

    def to_dict(self) -> dict[str, object]:
        return {
            "checkpoint_key": self.checkpoint_key,
            "status": self.status.value,
            "operational_trigger_route_job_ids": list(self.operational_trigger_route_job_ids),
            "trigger_candidate_version_ids": list(self.trigger_candidate_version_ids),
            "latest_trigger_available_at": (
                None
                if self.latest_trigger_available_at is None
                else _timestamp(self.latest_trigger_available_at)
            ),
            "blocking_gaps": list(self.blocking_gaps),
            "information_gaps": list(self.information_gaps),
        }


@dataclass(frozen=True, slots=True)
class ProspectiveCheckpointReadinessReport:
    report_id: str
    route_plan_id: str
    route_admission_id: str
    registration_id: str
    admitted_at: datetime
    evaluated_at: datetime
    checkpoints: tuple[ProspectiveCheckpointReadiness, ...]
    operational_checkpoint_count: int
    candidate_checkpoint_count: int
    waiting_for_external_event: bool
    model_calls_authorized: bool = False
    historical_pit_claim: bool = False
    execution_capability: bool = False
    schema_version: str = PROSPECTIVE_CHECKPOINT_READINESS_REPORT_SCHEMA

    def __post_init__(self) -> None:
        if self.schema_version != PROSPECTIVE_CHECKPOINT_READINESS_REPORT_SCHEMA:
            raise ValueError("unsupported prospective checkpoint readiness report schema")
        _prefixed(
            self.route_plan_id,
            "prospective-checkpoint-route-plan-",
            "checkpoint readiness route_plan_id",
        )
        _prefixed(
            self.registration_id,
            "prospective-diagnostic-registration-",
            "checkpoint readiness registration_id",
        )
        _prefixed(
            self.route_admission_id,
            "prospective-checkpoint-route-admission-",
            "checkpoint readiness route_admission_id",
        )
        _strict_utc(self.admitted_at, "checkpoint readiness admitted_at")
        _strict_utc(self.evaluated_at, "checkpoint readiness evaluated_at")
        if self.evaluated_at < self.admitted_at:
            raise ValueError("checkpoint readiness cannot precede route admission")
        keys = tuple(item.checkpoint_key for item in self.checkpoints)
        if keys != tuple(sorted(set(keys))):
            raise ValueError("checkpoint readiness entries must be sorted and unique")
        operational = sum(bool(item.operational_trigger_route_job_ids) for item in self.checkpoints)
        candidates = sum(bool(item.trigger_candidate_version_ids) for item in self.checkpoints)
        waiting = any(
            item.status is CheckpointReadinessStatus.WAITING_FOR_POST_ADMISSION_TRIGGER
            for item in self.checkpoints
        )
        if (
            self.operational_checkpoint_count != operational
            or self.candidate_checkpoint_count != candidates
            or self.waiting_for_external_event != waiting
        ):
            raise ValueError("checkpoint readiness aggregate does not match checkpoint state")
        if self.model_calls_authorized or self.historical_pit_claim or self.execution_capability:
            raise ValueError("checkpoint readiness cannot grant model, PIT, or execution authority")
        if self.report_id != self.expected_report_id:
            raise ValueError("prospective checkpoint readiness report_id does not match content")

    @property
    def expected_report_id(self) -> str:
        return f"prospective-checkpoint-readiness-report-{canonical_hash(self.core_dict())}"

    def core_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "route_plan_id": self.route_plan_id,
            "route_admission_id": self.route_admission_id,
            "registration_id": self.registration_id,
            "admitted_at": _timestamp(self.admitted_at),
            "evaluated_at": _timestamp(self.evaluated_at),
            "checkpoints": [item.to_dict() for item in self.checkpoints],
            "operational_checkpoint_count": self.operational_checkpoint_count,
            "candidate_checkpoint_count": self.candidate_checkpoint_count,
            "waiting_for_external_event": self.waiting_for_external_event,
            "model_calls_authorized": self.model_calls_authorized,
            "historical_pit_claim": self.historical_pit_claim,
            "execution_capability": self.execution_capability,
        }

    def to_dict(self) -> dict[str, object]:
        return {**self.core_dict(), "report_id": self.report_id}


@dataclass(frozen=True, slots=True)
class _RouteSourceContract:
    capability: ObservationCapability
    provider_id: str
    upstream_sources: tuple[str, ...]
    semantic_scopes: tuple[str, ...]


_ROUTE_SOURCE_CONTRACTS = {
    "established_news": _RouteSourceContract(
        capability=ObservationCapability.EVENT_REVELATION,
        provider_id="tushare-observation",
        upstream_sources=(
            "tushare-major-news",
            "tushare-news",
            "tushare-news-10jqka",
            "tushare-news-cls",
            "tushare-news-eastmoney",
            "tushare-news-fenghuang",
            "tushare-news-jinrongjie",
            "tushare-news-wallstreetcn",
            "tushare-news-yicai",
            "tushare-news-yuncaijing",
        ),
        semantic_scopes=(
            "aggregated_multi_publisher_observation_actual_receipt_only",
            "aggregated_source_observation_actual_receipt_only",
        ),
    ),
    "official_event": _RouteSourceContract(
        capability=ObservationCapability.EVENT_REVELATION,
        provider_id="csrc-official-news",
        upstream_sources=("csrc-official-news",),
        semantic_scopes=("official_capital_market_policy_publication",),
    ),
    "issuer_event": _RouteSourceContract(
        capability=ObservationCapability.EVENT_REVELATION,
        provider_id="tushare-observation",
        upstream_sources=("tushare-express-vip", "tushare-forecast-vip"),
        semantic_scopes=("aggregated_source_observation_actual_receipt_only",),
    ),
    "official_macro_release": _RouteSourceContract(
        capability=ObservationCapability.EVENT_REVELATION,
        provider_id="nbs-macro-release",
        upstream_sources=("nbs-official-cpi-ppi",),
        semantic_scopes=("official_nbs_cpi_ppi_original_release_actual_receipt_only",),
    ),
    "market_index_price": _RouteSourceContract(
        capability=ObservationCapability.MARKET_CONTEXT,
        provider_id="tushare-observation",
        upstream_sources=("tushare-index-daily",),
        semantic_scopes=("aggregated_source_observation_actual_receipt_only",),
    ),
    "raw_market_price": _RouteSourceContract(
        capability=ObservationCapability.MARKET_CONTEXT,
        provider_id="tushare-observation",
        upstream_sources=("tushare-daily", "tushare-fund-daily"),
        semantic_scopes=("aggregated_source_observation_actual_receipt_only",),
    ),
    "asof_adjustment": _RouteSourceContract(
        capability=ObservationCapability.MARKET_CONTEXT,
        provider_id="tushare-observation",
        upstream_sources=("tushare-adj-factor",),
        semantic_scopes=("aggregated_source_observation_actual_receipt_only",),
    ),
    "industry_to_tradable_mapping": _RouteSourceContract(
        capability=ObservationCapability.EXPOSURE_CANDIDATES,
        provider_id="tushare-observation",
        upstream_sources=("tushare-etf-sh-cons", "tushare-etf-sz-cons"),
        semantic_scopes=("aggregated_exchange_pcf_actual_receipt_only",),
    ),
    "tradability_state": _RouteSourceContract(
        capability=ObservationCapability.EXPOSURE_CANDIDATES,
        provider_id="tushare-observation",
        upstream_sources=("tushare-stk-limit", "tushare-suspend-d"),
        semantic_scopes=("aggregated_source_observation_actual_receipt_only",),
    ),
    "macro_release_calendar": _RouteSourceContract(
        capability=ObservationCapability.MACRO_VINTAGE,
        provider_id="tushare-observation",
        upstream_sources=("tushare-cn-schedule",),
        semantic_scopes=("schedule_observation_only_not_original_release_or_revision",),
    ),
}


def _validate_route_plan_structure(
    *,
    registration: ProspectiveDiagnosticRegistration,
    route_plan: ProspectiveCheckpointRoutePlan,
    runtime: ProspectiveCollectionRuntime,
) -> None:
    for binding in route_plan.bindings:
        checkpoint = registration.checkpoint(binding.checkpoint_key)
        slot = checkpoint.slot(binding.capability)
        if slot.applicability is CapabilityApplicability.NOT_APPLICABLE:
            raise ValueError("checkpoint route plan binds a not_applicable capability")
        if binding.route_kind not in slot.required_route_kinds:
            raise ValueError("checkpoint route plan contains an unregistered route kind")
        job = runtime.job(binding.job_id)
        policy = runtime.journal.policy(job.collection_policy_id)
        report = runtime.source_acceptance_report(binding.job_id)
        if policy.capability is not binding.capability:
            raise ValueError("checkpoint route job capability does not match its binding")
        if report.report_id != job.source_acceptance_report_id or not report.accepted:
            raise ValueError("checkpoint route job lacks its accepted source report")
        declaration = report.declaration
        if declaration.capability is not binding.capability:
            raise ValueError("checkpoint route accepted capability does not match its binding")
        contract = _ROUTE_SOURCE_CONTRACTS.get(binding.route_kind)
        if contract is None or (
            contract.capability is not binding.capability
            or contract.provider_id != declaration.provider_id
            or declaration.upstream_source not in contract.upstream_sources
            or declaration.semantic_scope not in contract.semantic_scopes
        ):
            raise ValueError(
                "checkpoint route kind does not match accepted source semantics and identity"
            )


@dataclass(frozen=True, slots=True)
class _ValidatedRoute:
    job_id: str
    policy_id: str
    source_config_hash: str
    operational: bool
    health_gaps: tuple[str, ...]


def evaluate_prospective_checkpoint_readiness(
    *,
    registration: ProspectiveDiagnosticRegistration,
    route_plan: ProspectiveCheckpointRoutePlan,
    admission_store: ProspectiveCheckpointAdmissionStore,
    runtime: ProspectiveCollectionRuntime,
    evaluated_at: datetime,
    classification_authority: CompletedTriageClassificationAuthority | None = None,
) -> ProspectiveCheckpointReadinessReport:
    if registration.schema_version not in {
        PROSPECTIVE_DIAGNOSTIC_REGISTRATION_SCHEMA_V2,
        PROSPECTIVE_DIAGNOSTIC_REGISTRATION_SCHEMA_V3,
        PROSPECTIVE_DIAGNOSTIC_REGISTRATION_SCHEMA_V4,
    }:
        raise ValueError("checkpoint readiness requires a v2, v3, or v4 registration")
    if route_plan.registration_id != registration.registration_id:
        raise ValueError("checkpoint route plan belongs to a different registration")
    admission = admission_store.admission(route_plan.plan_id)
    if admission.registration_id != registration.registration_id:
        raise ValueError("checkpoint route admission belongs to a different registration")
    _strict_utc(evaluated_at, "checkpoint readiness evaluated_at")
    if evaluated_at < admission.recorded_at:
        raise ValueError("checkpoint readiness cannot precede route admission")
    admission_store.assert_effective(
        route_plan_id=route_plan.plan_id,
        admission_id=admission.admission_id,
        registration_id=registration.registration_id,
        at=evaluated_at,
    )

    validated: dict[
        tuple[str, ObservationCapability, str, str],
        _ValidatedRoute,
    ] = {}
    for binding in route_plan.bindings:
        checkpoint = registration.checkpoint(binding.checkpoint_key)
        slot = checkpoint.slot(binding.capability)
        if slot.applicability is CapabilityApplicability.NOT_APPLICABLE:
            raise ValueError("checkpoint route plan binds a not_applicable capability")
        if binding.route_kind not in slot.required_route_kinds:
            raise ValueError("checkpoint route plan contains an unregistered route kind")
        job = runtime.job(binding.job_id)
        policy = runtime.journal.policy(job.collection_policy_id)
        report = runtime.source_acceptance_report(binding.job_id)
        if policy.capability is not binding.capability:
            raise ValueError("checkpoint route job capability does not match its binding")
        if report.report_id != job.source_acceptance_report_id or not report.accepted:
            raise ValueError("checkpoint route job lacks its accepted source report")
        declaration = report.declaration
        if declaration.capability is not binding.capability:
            raise ValueError("checkpoint route accepted capability does not match its binding")
        contract = _ROUTE_SOURCE_CONTRACTS.get(binding.route_kind)
        if contract is None or (
            contract.capability is not binding.capability
            or contract.provider_id != declaration.provider_id
            or declaration.upstream_source not in contract.upstream_sources
            or declaration.semantic_scope not in contract.semantic_scopes
        ):
            raise ValueError(
                "checkpoint route kind does not match accepted source semantics and identity"
            )
        health = runtime.health(binding.job_id, now=evaluated_at)
        state_updated_at = _runtime_state_updated_at(
            runtime,
            binding.job_id,
            health=health,
        )
        if state_updated_at > evaluated_at:
            raise ValueError(
                "checkpoint readiness cannot reconstruct historical runtime health "
                "after later job updates"
            )
        health_gaps: list[str] = []
        if policy.poll_interval_seconds > slot.poll_interval_seconds:
            health_gaps.append("poll_interval_exceeds_registration")
        if policy.maximum_gap_seconds > slot.maximum_gap_seconds:
            health_gaps.append("maximum_gap_exceeds_registration")
        if health.status != "active" or job.starts_at > evaluated_at:
            health_gaps.append("route_inactive")
        if health.backoff_until is not None and evaluated_at < health.backoff_until:
            health_gaps.append("route_in_backoff")
        if health.lag_seconds > slot.maximum_gap_seconds:
            health_gaps.append("route_lag_exceeds_registration_maximum_gap")
        post_admission_opportunities = tuple(
            item
            for item in runtime.opportunities(binding.job_id)
            if item.scheduled_for >= admission.recorded_at
            and _opportunity_is_visible_at(item, evaluated_at)
        )
        if any(item.outcome == "missed" for item in post_admission_opportunities):
            health_gaps.append("post_admission_missed_opportunity")
        latest_post_admission = (
            max(post_admission_opportunities, key=lambda item: item.scheduled_for)
            if post_admission_opportunities
            else None
        )
        if latest_post_admission is not None and latest_post_admission.outcome in {
            "source_failure",
            "collector_failure",
        }:
            health_gaps.append(f"current_{latest_post_admission.outcome}")
        if latest_post_admission is not None and health.last_outcome == "storage_failure":
            health_gaps.append("current_storage_failure")
        gaps = tuple(sorted(set(health_gaps)))
        validated[
            (binding.checkpoint_key, binding.capability, binding.route_kind, binding.job_id)
        ] = _ValidatedRoute(
            job_id=binding.job_id,
            policy_id=policy.policy_id,
            source_config_hash=declaration.source_config_hash,
            operational=not gaps,
            health_gaps=gaps,
        )

    checkpoint_results: list[ProspectiveCheckpointReadiness] = []
    for checkpoint in sorted(registration.checkpoints, key=lambda item: item.checkpoint_key):
        trigger_slot = checkpoint.slot(ObservationCapability.EVENT_REVELATION)
        trigger_rows = tuple(
            (key, value)
            for key, value in validated.items()
            if key[0] == checkpoint.checkpoint_key
            and key[1] is ObservationCapability.EVENT_REVELATION
            and value.operational
        )
        operational_job_ids = tuple(sorted({value.job_id for _, value in trigger_rows}))
        classified = (
            ()
            if classification_authority is None
            else classification_authority.classified_version_ids(
                registration_id=registration.registration_id,
                checkpoint_key=checkpoint.checkpoint_key,
                route_plan_id=route_plan.plan_id,
                route_admission_id=admission.admission_id,
                at=evaluated_at,
            )
        )
        classified_ids = set(classified)
        candidate_refs = {
            item.version_id: item
            for _, value in trigger_rows
            for item in runtime.journal.observation_version_refs(
                policy_id=value.policy_id,
                capability=ObservationCapability.EVENT_REVELATION,
                not_before=admission.recorded_at,
                not_after=evaluated_at,
            )
            if item.version_id not in classified_ids
        }
        candidate_ids = tuple(sorted(candidate_refs))
        latest = max(
            (item.first_available_at for item in candidate_refs.values()),
            default=None,
        )
        blocking: list[str] = []
        if len(operational_job_ids) < trigger_slot.minimum_data_sources:
            blocking.append("event_revelation:no_active_registered_trigger_route")
            blocking.extend(
                f"event_revelation:{gap}"
                for key, value in validated.items()
                if key[0] == checkpoint.checkpoint_key
                and key[1] is ObservationCapability.EVENT_REVELATION
                for gap in value.health_gaps
            )
            status = CheckpointReadinessStatus.TRIGGER_ROUTE_UNCONFIGURED
        elif not candidate_ids:
            blocking.append("event_revelation:no_post_admission_observation")
            status = CheckpointReadinessStatus.WAITING_FOR_POST_ADMISSION_TRIGGER
        else:
            blocking.append("event_revelation:trigger_candidate_requires_eligibility_selection")
            status = CheckpointReadinessStatus.UNCLASSIFIED_TRIGGER_CANDIDATE_OBSERVED

        information: list[str] = []
        for slot in checkpoint.capability_slots:
            if slot.applicability is CapabilityApplicability.NOT_APPLICABLE:
                continue
            planned = tuple(
                (key, value)
                for key, value in validated.items()
                if key[0] == checkpoint.checkpoint_key and key[1] is slot.capability
            )
            operational = tuple((key, value) for key, value in planned if value.operational)
            planned_route_kinds = {key[2] for key, _ in planned}
            missing_route_kinds = tuple(
                sorted(set(slot.required_route_kinds) - planned_route_kinds)
            )
            if missing_route_kinds:
                information.append(
                    f"{slot.capability.value}:unbound_route_kinds:{','.join(missing_route_kinds)}"
                )
            inactive_route_kinds = tuple(
                sorted({key[2] for key, value in planned if not value.operational})
            )
            if inactive_route_kinds:
                information.append(
                    f"{slot.capability.value}:bound_routes_not_operational:"
                    f"{','.join(inactive_route_kinds)}"
                )
            information.extend(
                f"{slot.capability.value}:{gap}"
                for _, value in planned
                for gap in value.health_gaps
            )
            distinct_sources = {value.source_config_hash for _, value in operational}
            if slot.capability is not ObservationCapability.EVENT_REVELATION and (
                len(distinct_sources) < slot.minimum_data_sources
            ):
                information.append(
                    f"{slot.capability.value}:route_source_coverage:"
                    f"{len(distinct_sources)}/{slot.minimum_data_sources}"
                )

        checkpoint_results.append(
            ProspectiveCheckpointReadiness(
                checkpoint_key=checkpoint.checkpoint_key,
                status=status,
                operational_trigger_route_job_ids=operational_job_ids,
                trigger_candidate_version_ids=candidate_ids,
                latest_trigger_available_at=latest,
                blocking_gaps=tuple(sorted(set(blocking))),
                information_gaps=tuple(sorted(set(information) - set(blocking))),
            )
        )

    checkpoints = tuple(checkpoint_results)
    operational_count = sum(bool(item.operational_trigger_route_job_ids) for item in checkpoints)
    candidate_count = sum(bool(item.trigger_candidate_version_ids) for item in checkpoints)
    waiting = any(
        item.status is CheckpointReadinessStatus.WAITING_FOR_POST_ADMISSION_TRIGGER
        for item in checkpoints
    )
    core = {
        "schema_version": PROSPECTIVE_CHECKPOINT_READINESS_REPORT_SCHEMA,
        "route_plan_id": route_plan.plan_id,
        "route_admission_id": admission.admission_id,
        "registration_id": registration.registration_id,
        "admitted_at": _timestamp(admission.recorded_at),
        "evaluated_at": _timestamp(evaluated_at),
        "checkpoints": [item.to_dict() for item in checkpoints],
        "operational_checkpoint_count": operational_count,
        "candidate_checkpoint_count": candidate_count,
        "waiting_for_external_event": waiting,
        "model_calls_authorized": False,
        "historical_pit_claim": False,
        "execution_capability": False,
    }
    return ProspectiveCheckpointReadinessReport(
        report_id=f"prospective-checkpoint-readiness-report-{canonical_hash(core)}",
        route_plan_id=route_plan.plan_id,
        route_admission_id=admission.admission_id,
        registration_id=registration.registration_id,
        admitted_at=admission.recorded_at,
        evaluated_at=evaluated_at,
        checkpoints=checkpoints,
        operational_checkpoint_count=operational_count,
        candidate_checkpoint_count=candidate_count,
        waiting_for_external_event=waiting,
    )


def load_prospective_checkpoint_route_plan(path: Path) -> ProspectiveCheckpointRoutePlan:
    return prospective_checkpoint_route_plan_from_dict(json.loads(path.read_text(encoding="utf-8")))


def prospective_checkpoint_route_plan_from_dict(
    value: object,
) -> ProspectiveCheckpointRoutePlan:
    payload = _object(value, "prospective checkpoint route plan")
    schema_version = _string(payload, "schema_version")
    expected_keys = {
        "schema_version",
        "plan_id",
        "registration_id",
        "admission_timing_protocol",
        "bindings",
        "historical_pit_claim",
        "model_calls_authorized",
        "execution_capability",
    }
    if schema_version == PROSPECTIVE_CHECKPOINT_ROUTE_PLAN_SCHEMA:
        expected_keys.add("replaces_plan_id")
    _exact_keys(
        payload,
        expected_keys,
        "prospective checkpoint route plan",
    )
    plan = ProspectiveCheckpointRoutePlan(
        plan_id=_string(payload, "plan_id"),
        registration_id=_string(payload, "registration_id"),
        replaces_plan_id=(
            _optional_string(payload, "replaces_plan_id")
            if schema_version == PROSPECTIVE_CHECKPOINT_ROUTE_PLAN_SCHEMA
            else None
        ),
        admission_timing_protocol=_string(payload, "admission_timing_protocol"),
        bindings=tuple(
            _route_binding_from_dict(item) for item in _list(payload.get("bindings"), "bindings")
        ),
        historical_pit_claim=_boolean(payload, "historical_pit_claim"),
        model_calls_authorized=_boolean(payload, "model_calls_authorized"),
        execution_capability=_boolean(payload, "execution_capability"),
        schema_version=schema_version,
    )
    if plan.to_dict() != payload:
        raise ValueError("prospective checkpoint route plan is not canonical")
    return plan


def _admission_from_row(
    route_plan_id: str,
    row: sqlite3.Row,
) -> ProspectiveCheckpointRouteAdmission:
    return ProspectiveCheckpointRouteAdmission(
        admission_id=cast(str, row["admission_id"]),
        route_plan_id=route_plan_id,
        registration_id=cast(str, row["registration_id"]),
        recorded_at=_datetime(cast(str, row["recorded_at"]), "recorded_at"),
    )


def _ensure_sqlite_column(
    connection: sqlite3.Connection,
    table: str,
    column: str,
    declaration: str,
) -> None:
    columns = {cast(str, row[1]) for row in connection.execute(f"PRAGMA table_info({table})")}
    if column not in columns:
        connection.execute(f"ALTER TABLE {table} ADD COLUMN {column} {declaration}")


def _runtime_state_updated_at(
    runtime: ProspectiveCollectionRuntime,
    job_id: str,
    *,
    health: object,
) -> datetime:
    injected = getattr(health, "state_updated_at", None)
    if isinstance(injected, datetime):
        _strict_utc(injected, "checkpoint readiness runtime state_updated_at")
        return injected
    with sqlite3.connect(runtime.index_path) as connection:
        row = connection.execute(
            "SELECT updated_at FROM prospective_collection_jobs WHERE job_id = ?",
            (job_id,),
        ).fetchone()
    if row is None:
        raise KeyError(f"unknown prospective collection job: {job_id}")
    return _datetime(cast(str, row[0]), "checkpoint readiness runtime state_updated_at")


def _opportunity_is_visible_at(item: object, evaluated_at: datetime) -> bool:
    scheduled_for = getattr(item, "scheduled_for", None)
    if not isinstance(scheduled_for, datetime):
        raise TypeError("checkpoint readiness opportunity scheduled_for must be a timestamp")
    started_at = getattr(item, "started_at", None)
    if started_at is None:
        started_at = scheduled_for
    if not isinstance(started_at, datetime):
        raise TypeError("checkpoint readiness opportunity started_at must be a timestamp")
    completed_at = getattr(item, "completed_at", None)
    if completed_at is not None and not isinstance(completed_at, datetime):
        raise TypeError("checkpoint readiness opportunity completed_at must be a timestamp")
    for value, name in (
        (scheduled_for, "scheduled_for"),
        (started_at, "started_at"),
    ):
        _strict_utc(value, f"checkpoint readiness opportunity {name}")
    if completed_at is not None:
        _strict_utc(completed_at, "checkpoint readiness opportunity completed_at")
    return (
        scheduled_for <= evaluated_at
        and started_at <= evaluated_at
        and (completed_at is None or completed_at <= evaluated_at)
    )


def _route_binding_from_dict(value: object) -> ProspectiveCheckpointRouteBinding:
    payload = _object(value, "prospective checkpoint route binding")
    _exact_keys(
        payload,
        {"checkpoint_key", "capability", "route_kind", "job_id"},
        "prospective checkpoint route binding",
    )
    return ProspectiveCheckpointRouteBinding(
        checkpoint_key=_string(payload, "checkpoint_key"),
        capability=ObservationCapability(_string(payload, "capability")),
        route_kind=_string(payload, "route_kind"),
        job_id=_string(payload, "job_id"),
    )


def _object(value: object, name: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise TypeError(f"{name} must be an object")
    typed = cast(dict[object, object], value)
    if any(not isinstance(key, str) for key in typed):
        raise TypeError(f"{name} must be an object")
    return cast(dict[str, object], typed)


def _list(value: object, name: str) -> list[object]:
    if not isinstance(value, list):
        raise TypeError(f"{name} must be a list")
    return cast(list[object], value)


def _exact_keys(value: dict[str, object], keys: set[str], name: str) -> None:
    if set(value) != keys:
        raise ValueError(f"{name} fields are invalid")


def _string(value: dict[str, object], key: str) -> str:
    item = value.get(key)
    if not isinstance(item, str):
        raise TypeError(f"{key} must be a string")
    return item


def _optional_string(value: dict[str, object], key: str) -> str | None:
    item = value.get(key)
    if item is None:
        return None
    if not isinstance(item, str):
        raise TypeError(f"{key} must be a string or null")
    return item


def _boolean(value: dict[str, object], key: str) -> bool:
    item = value.get(key)
    if not isinstance(item, bool):
        raise TypeError(f"{key} must be a boolean")
    return item


def _datetime(value: object, name: str) -> datetime:
    if not isinstance(value, str):
        raise TypeError(f"{name} must be an ISO timestamp")
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    _strict_utc(parsed, name)
    return parsed


def _prefixed(value: str, prefix: str, name: str) -> None:
    if not value.startswith(prefix) or len(value) != len(prefix) + 64:
        raise ValueError(f"{name} is invalid")
    digest = value[len(prefix) :]
    if any(character not in "0123456789abcdef" for character in digest):
        raise ValueError(f"{name} is invalid")


def _sorted_unique(values: tuple[str, ...], name: str) -> None:
    if values != tuple(sorted(set(values))):
        raise ValueError(f"{name} must be sorted and unique")


def _trimmed(value: str, name: str) -> None:
    if not value or value != value.strip():
        raise ValueError(f"{name} must be non-empty trimmed text")


def _strict_utc(value: datetime, name: str) -> None:
    require_aware(value, name)
    if value.utcoffset() != UTC.utcoffset(value):
        raise ValueError(f"{name} must use UTC")


def _timestamp(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")
