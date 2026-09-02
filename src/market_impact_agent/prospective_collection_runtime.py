from __future__ import annotations

import sqlite3
import uuid
from collections import Counter
from collections.abc import Callable, Mapping
from contextlib import closing
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import cast

from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.data_inputs import (
    DataFetchStatus,
    DataPITLane,
    DataSnapshot,
    LocalDataSnapshotStore,
)
from market_impact_agent.domain import require_aware
from market_impact_agent.prospective_data import (
    ProspectiveCollectionPolicy,
    ProspectiveDataJournal,
)
from market_impact_agent.source_acceptance import (
    SourceRouteAcceptanceReport,
    source_route_acceptance_report_from_dict,
)
from market_impact_agent.tushare_observation import (
    summarize_tushare_observation_capture_usage,
)

PROSPECTIVE_COLLECTION_JOB_SCHEMA = "market-impact.prospective-collection-job.v1"
PROSPECTIVE_COLLECTION_JOB_REPLACEMENT_SCHEMA = (
    "market-impact.prospective-collection-job-replacement.v1"
)
PROSPECTIVE_COLLECTION_USAGE_RECORD_SCHEMA_V1 = (
    "market-impact.prospective-collection-usage-record.v1"
)
PROSPECTIVE_COLLECTION_USAGE_RECORD_SCHEMA = "market-impact.prospective-collection-usage-record.v2"
_TERMINAL_OUTCOMES = frozenset(
    {"success", "no_data", "source_failure", "collector_failure", "cancelled", "missed"}
)
_MAX_MISFIRES_PER_RUN = 10_000
_USAGE_MEASUREMENT_DIMENSIONS = (
    "provider_attempt_count",
    "request_count",
    "page_count",
    "response_bytes",
    "raw_artifact_bytes",
    "received_records",
    "accepted_records",
)
_USAGE_COST_BASES = frozenset(
    {"flat_subscription_not_allocated_per_request", "not_applicable", "unknown"}
)


class ProspectiveCollectionAdapterKind(StrEnum):
    CSRC_NEWS = "csrc_news"
    NBS_MACRO_RELEASE = "nbs_macro_release"
    TUSHARE_OBSERVATION = "tushare_observation"


def _usage_cost_basis(adapter_kind: ProspectiveCollectionAdapterKind) -> str:
    if adapter_kind is ProspectiveCollectionAdapterKind.TUSHARE_OBSERVATION:
        return "flat_subscription_not_allocated_per_request"
    if adapter_kind in {
        ProspectiveCollectionAdapterKind.CSRC_NEWS,
        ProspectiveCollectionAdapterKind.NBS_MACRO_RELEASE,
    }:
        return "not_applicable"
    raise ValueError("unsupported prospective collection adapter cost basis")


@dataclass(frozen=True, slots=True)
class ProspectiveCollectionJob:
    job_id: str
    adapter_kind: ProspectiveCollectionAdapterKind
    collection_policy_id: str
    source_acceptance_report_id: str
    source_acceptance_report_hash: str
    source_config_hash: str
    starts_at: datetime
    misfire_grace_seconds: int
    maximum_jitter_seconds: int
    provider_timeout_seconds: float
    execution_capability: bool = False
    schema_version: str = PROSPECTIVE_COLLECTION_JOB_SCHEMA

    def __post_init__(self) -> None:
        if self.schema_version != PROSPECTIVE_COLLECTION_JOB_SCHEMA:
            raise ValueError("unsupported prospective collection job schema")
        _trimmed(self.collection_policy_id, "collection policy ID")
        _trimmed(self.source_acceptance_report_id, "source acceptance report ID")
        _sha256(self.source_acceptance_report_hash, "source acceptance report hash")
        _sha256(self.source_config_hash, "source config hash")
        _strict_utc(self.starts_at, "prospective collection job starts_at")
        if self.misfire_grace_seconds < 0:
            raise ValueError("prospective collection misfire grace cannot be negative")
        if self.maximum_jitter_seconds < 0:
            raise ValueError("prospective collection jitter cannot be negative")
        if self.provider_timeout_seconds <= 0:
            raise ValueError("prospective collection Provider timeout must be positive")
        if self.execution_capability:
            raise ValueError("prospective collection job cannot grant execution capability")
        if self.job_id != self.expected_job_id:
            raise ValueError("prospective collection job_id does not match content")

    @property
    def expected_job_id(self) -> str:
        return f"prospective-collection-job-{canonical_hash(self.core_dict())}"

    def core_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "adapter_kind": self.adapter_kind.value,
            "collection_policy_id": self.collection_policy_id,
            "source_acceptance_report_id": self.source_acceptance_report_id,
            "source_acceptance_report_hash": self.source_acceptance_report_hash,
            "source_config_hash": self.source_config_hash,
            "starts_at": _timestamp(self.starts_at),
            "misfire_grace_seconds": self.misfire_grace_seconds,
            "maximum_jitter_seconds": self.maximum_jitter_seconds,
            "provider_timeout_seconds": self.provider_timeout_seconds,
            "execution_capability": self.execution_capability,
        }

    def to_dict(self) -> dict[str, object]:
        return {**self.core_dict(), "job_id": self.job_id}

    @classmethod
    def build(
        cls,
        *,
        adapter_kind: ProspectiveCollectionAdapterKind,
        collection_policy: ProspectiveCollectionPolicy,
        source_acceptance_report: SourceRouteAcceptanceReport,
        source_config: Mapping[str, object],
        starts_at: datetime,
        misfire_grace_seconds: int,
        maximum_jitter_seconds: int,
        provider_timeout_seconds: float,
    ) -> ProspectiveCollectionJob:
        _validate_adapter_policy_scope(
            adapter_kind,
            collection_policy=collection_policy,
            source_config=source_config,
        )
        report_hash = canonical_hash(source_acceptance_report.to_dict())
        source_config_hash = canonical_hash(source_config)
        core = {
            "schema_version": PROSPECTIVE_COLLECTION_JOB_SCHEMA,
            "adapter_kind": adapter_kind.value,
            "collection_policy_id": collection_policy.policy_id,
            "source_acceptance_report_id": source_acceptance_report.report_id,
            "source_acceptance_report_hash": report_hash,
            "source_config_hash": source_config_hash,
            "starts_at": _timestamp(starts_at),
            "misfire_grace_seconds": misfire_grace_seconds,
            "maximum_jitter_seconds": maximum_jitter_seconds,
            "provider_timeout_seconds": provider_timeout_seconds,
            "execution_capability": False,
        }
        return cls(
            job_id=f"prospective-collection-job-{canonical_hash(core)}",
            adapter_kind=adapter_kind,
            collection_policy_id=collection_policy.policy_id,
            source_acceptance_report_id=source_acceptance_report.report_id,
            source_acceptance_report_hash=report_hash,
            source_config_hash=source_config_hash,
            starts_at=starts_at,
            misfire_grace_seconds=misfire_grace_seconds,
            maximum_jitter_seconds=maximum_jitter_seconds,
            provider_timeout_seconds=provider_timeout_seconds,
        )


@dataclass(frozen=True, slots=True)
class ProspectiveCollectionJobReplacement:
    replacement_id: str
    predecessor_job_id: str
    successor_job_id: str
    replaced_at: datetime
    reason: str
    execution_capability: bool = False
    schema_version: str = PROSPECTIVE_COLLECTION_JOB_REPLACEMENT_SCHEMA

    def __post_init__(self) -> None:
        if self.schema_version != PROSPECTIVE_COLLECTION_JOB_REPLACEMENT_SCHEMA:
            raise ValueError("unsupported prospective collection Job Replacement schema")
        _trimmed(self.predecessor_job_id, "predecessor Job ID")
        _trimmed(self.successor_job_id, "successor Job ID")
        if self.predecessor_job_id == self.successor_job_id:
            raise ValueError("prospective collection Job cannot replace itself")
        _strict_utc(self.replaced_at, "prospective collection Job replaced_at")
        _trimmed(self.reason, "prospective collection Job replacement reason")
        if len(self.reason) > 256:
            raise ValueError("prospective collection Job replacement reason is too long")
        if self.execution_capability:
            raise ValueError("prospective collection Job Replacement cannot grant execution")
        if self.replacement_id != self.expected_replacement_id:
            raise ValueError("prospective collection Job Replacement ID does not match content")

    @property
    def expected_replacement_id(self) -> str:
        return f"prospective-collection-job-replacement-{canonical_hash(self.core_dict())}"

    def core_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "predecessor_job_id": self.predecessor_job_id,
            "successor_job_id": self.successor_job_id,
            "replaced_at": _timestamp(self.replaced_at),
            "reason": self.reason,
            "execution_capability": self.execution_capability,
        }

    def to_dict(self) -> dict[str, object]:
        return {**self.core_dict(), "replacement_id": self.replacement_id}

    @classmethod
    def build(
        cls,
        *,
        predecessor_job_id: str,
        successor_job_id: str,
        replaced_at: datetime,
        reason: str,
    ) -> ProspectiveCollectionJobReplacement:
        core = {
            "schema_version": PROSPECTIVE_COLLECTION_JOB_REPLACEMENT_SCHEMA,
            "predecessor_job_id": predecessor_job_id,
            "successor_job_id": successor_job_id,
            "replaced_at": _timestamp(replaced_at),
            "reason": reason,
            "execution_capability": False,
        }
        return cls(
            replacement_id=(f"prospective-collection-job-replacement-{canonical_hash(core)}"),
            predecessor_job_id=predecessor_job_id,
            successor_job_id=successor_job_id,
            replaced_at=replaced_at,
            reason=reason,
        )


@dataclass(frozen=True, slots=True)
class CollectionOpportunity:
    opportunity_id: str
    job_id: str
    scheduled_for: datetime
    outcome: str
    started_at: datetime
    completed_at: datetime | None
    attempt_count: int
    data_snapshot_id: str | None
    error_kind: str | None

    def to_dict(self) -> dict[str, object]:
        return {
            "opportunity_id": self.opportunity_id,
            "job_id": self.job_id,
            "scheduled_for": _timestamp(self.scheduled_for),
            "outcome": self.outcome,
            "started_at": _timestamp(self.started_at),
            "completed_at": (None if self.completed_at is None else _timestamp(self.completed_at)),
            "attempt_count": self.attempt_count,
            "data_snapshot_id": self.data_snapshot_id,
            "error_kind": self.error_kind,
        }


@dataclass(frozen=True, slots=True)
class CollectionUsageRecord:
    record_id: str
    opportunity_id: str
    job_id: str
    scheduled_for: datetime
    adapter_kind: ProspectiveCollectionAdapterKind
    outcome: str
    collection_attempt_count: int
    provider_attempt_count: int | None
    request_count: int | None
    page_count: int | None
    response_bytes: int | None
    raw_artifact_bytes: int | None
    received_records: int | None
    accepted_records: int | None
    latency_ms: float
    incremental_cost_microusd: int | None
    cost_basis: str | None
    error_kind: str | None
    recorded_at: datetime
    execution_capability: bool = False
    schema_version: str = PROSPECTIVE_COLLECTION_USAGE_RECORD_SCHEMA

    def __post_init__(self) -> None:
        if self.schema_version not in {
            PROSPECTIVE_COLLECTION_USAGE_RECORD_SCHEMA_V1,
            PROSPECTIVE_COLLECTION_USAGE_RECORD_SCHEMA,
        }:
            raise ValueError("unsupported Collection Usage Record schema")
        _trimmed(self.opportunity_id, "collection usage opportunity_id")
        _trimmed(self.job_id, "collection usage job_id")
        _strict_utc(self.scheduled_for, "collection usage scheduled_for")
        _strict_utc(self.recorded_at, "collection usage recorded_at")
        if self.outcome not in _TERMINAL_OUTCOMES - {"missed"}:
            raise ValueError("collection usage outcome is not terminal")
        if self.collection_attempt_count < 1:
            raise ValueError("collection usage attempt count must be positive")
        for value, name in (
            (self.provider_attempt_count, "provider_attempt_count"),
            (self.request_count, "request_count"),
            (self.page_count, "page_count"),
            (self.response_bytes, "response_bytes"),
            (self.raw_artifact_bytes, "raw_artifact_bytes"),
            (self.received_records, "received_records"),
            (self.accepted_records, "accepted_records"),
        ):
            if value is not None and value < 0:
                raise ValueError(f"collection usage {name} cannot be negative")
        if self.request_count is not None and self.request_count < 1:
            raise ValueError("known collection request count must be positive")
        if self.page_count is not None and self.page_count < 1:
            raise ValueError("known collection page count must be positive")
        if self.latency_ms < 0:
            raise ValueError("collection usage latency cannot be negative")
        if self.incremental_cost_microusd is not None and self.incremental_cost_microusd < 0:
            raise ValueError("collection usage incremental cost cannot be negative")
        if self.cost_basis is None:
            if self.schema_version != PROSPECTIVE_COLLECTION_USAGE_RECORD_SCHEMA_V1:
                raise ValueError("collection usage cost basis is required")
            if self.incremental_cost_microusd is not None:
                raise ValueError("legacy collection usage cost must remain unknown")
        else:
            if self.cost_basis not in _USAGE_COST_BASES:
                raise ValueError("collection usage cost basis is unsupported")
            if self.cost_basis != _usage_cost_basis(self.adapter_kind):
                raise ValueError("collection usage cost basis does not match its adapter")
            if self.incremental_cost_microusd is not None:
                raise ValueError("collection usage cost must remain unallocated for its cost basis")
        if self.execution_capability:
            raise ValueError("Collection Usage Record cannot grant execution capability")
        if self.record_id != self.expected_record_id:
            raise ValueError("Collection Usage Record ID does not match content")

    @property
    def expected_record_id(self) -> str:
        return f"prospective-collection-usage-{canonical_hash(self.core_dict())}"

    def core_dict(self) -> dict[str, object]:
        core: dict[str, object] = {
            "schema_version": self.schema_version,
            "opportunity_id": self.opportunity_id,
            "job_id": self.job_id,
            "scheduled_for": _timestamp(self.scheduled_for),
            "adapter_kind": self.adapter_kind.value,
            "outcome": self.outcome,
            "collection_attempt_count": self.collection_attempt_count,
            "provider_attempt_count": self.provider_attempt_count,
            "request_count": self.request_count,
            "page_count": self.page_count,
            "response_bytes": self.response_bytes,
            "raw_artifact_bytes": self.raw_artifact_bytes,
            "received_records": self.received_records,
            "accepted_records": self.accepted_records,
            "latency_ms": self.latency_ms,
            "error_kind": self.error_kind,
            "recorded_at": _timestamp(self.recorded_at),
            "execution_capability": self.execution_capability,
        }
        if self.cost_basis is not None:
            core["incremental_cost_microusd"] = self.incremental_cost_microusd
            core["cost_basis"] = self.cost_basis
        return core

    def to_dict(self) -> dict[str, object]:
        return {**self.core_dict(), "record_id": self.record_id}


@dataclass(frozen=True, slots=True)
class CollectionRunResult:
    job_id: str
    outcome: str
    scheduled_for: datetime | None
    opportunity_id: str | None
    data_snapshot_id: str | None
    missed_opportunities: int
    error_kind: str | None
    usage_record_id: str | None = None
    execution_capability: bool = False

    def to_dict(self) -> dict[str, object]:
        return {
            "job_id": self.job_id,
            "outcome": self.outcome,
            "scheduled_for": (
                None if self.scheduled_for is None else _timestamp(self.scheduled_for)
            ),
            "opportunity_id": self.opportunity_id,
            "data_snapshot_id": self.data_snapshot_id,
            "missed_opportunities": self.missed_opportunities,
            "error_kind": self.error_kind,
            "usage_record_id": self.usage_record_id,
            "execution_capability": self.execution_capability,
        }


@dataclass(frozen=True, slots=True)
class CollectionHealth:
    job_id: str
    status: str
    next_due_at: datetime
    backoff_until: datetime | None
    last_outcome: str | None
    last_error_kind: str | None
    successful_opportunities: int
    no_data_opportunities: int
    source_failures: int
    collector_failures: int
    cancelled_opportunities: int
    missed_opportunities: int
    incomplete_interval: bool
    lag_seconds: int
    execution_capability: bool = False

    def to_dict(self) -> dict[str, object]:
        return {
            "job_id": self.job_id,
            "status": self.status,
            "next_due_at": _timestamp(self.next_due_at),
            "backoff_until": (
                None if self.backoff_until is None else _timestamp(self.backoff_until)
            ),
            "last_outcome": self.last_outcome,
            "last_error_kind": self.last_error_kind,
            "successful_opportunities": self.successful_opportunities,
            "no_data_opportunities": self.no_data_opportunities,
            "source_failures": self.source_failures,
            "collector_failures": self.collector_failures,
            "cancelled_opportunities": self.cancelled_opportunities,
            "missed_opportunities": self.missed_opportunities,
            "incomplete_interval": self.incomplete_interval,
            "lag_seconds": self.lag_seconds,
            "execution_capability": self.execution_capability,
        }


ScheduledCollector = Callable[
    [ProspectiveCollectionPolicy, dict[str, object], datetime],
    DataSnapshot,
]


def _validate_adapter_policy_scope(
    adapter_kind: ProspectiveCollectionAdapterKind,
    *,
    collection_policy: ProspectiveCollectionPolicy,
    source_config: Mapping[str, object],
) -> None:
    if (
        collection_policy.rolling_window is not None
        and adapter_kind is not ProspectiveCollectionAdapterKind.TUSHARE_OBSERVATION
    ):
        raise ValueError("rolling windows require the Tushare observation adapter")
    if adapter_kind is not ProspectiveCollectionAdapterKind.NBS_MACRO_RELEASE:
        return
    configured_indicators = source_config.get("indicators")
    if (
        not isinstance(configured_indicators, list)
        or set(collection_policy.parameters) != {"indicators"}
        or collection_policy.parameters.get("indicators") != configured_indicators
    ):
        raise ValueError(
            "NBS macro release collection policy indicators must exactly match the source config"
        )


class ProspectiveCollectionRuntime:
    """Harness-owned due state for externally invoked one-shot collectors."""

    def __init__(
        self,
        store: LocalDataSnapshotStore,
        *,
        journal: ProspectiveDataJournal | None = None,
        lease_timeout_seconds: int = 300,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        if lease_timeout_seconds < 1:
            raise ValueError("prospective collection lease timeout must be positive")
        self.store = store
        self.journal = ProspectiveDataJournal(store) if journal is None else journal
        if self.journal.store.root != store.root:
            raise ValueError("prospective collection journal must share the Snapshot store")
        self.index_path = store.index_path
        self.lease_timeout_seconds = lease_timeout_seconds
        self._clock = _utc_now if clock is None else clock
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.index_path)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA journal_mode = WAL")
        connection.execute("PRAGMA synchronous = FULL")
        connection.execute("PRAGMA busy_timeout = 5000")
        return connection

    def _initialize(self) -> None:
        with self._connect() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS prospective_collection_jobs (
                    job_id TEXT PRIMARY KEY,
                    artifact_hash TEXT NOT NULL UNIQUE,
                    collection_policy_id TEXT NOT NULL
                        REFERENCES prospective_collection_policies(policy_id),
                    source_config_artifact_hash TEXT NOT NULL,
                    source_acceptance_report_artifact_hash TEXT NOT NULL,
                    status TEXT NOT NULL,
                    registered_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    next_due_at TEXT NOT NULL,
                    backoff_until TEXT,
                    consecutive_failures INTEGER NOT NULL,
                    last_outcome TEXT,
                    last_error_kind TEXT,
                    lease_token TEXT,
                    lease_expires_at TEXT
                );
                CREATE INDEX IF NOT EXISTS prospective_collection_jobs_due
                    ON prospective_collection_jobs(status, next_due_at, job_id);
                CREATE TABLE IF NOT EXISTS prospective_collection_job_replacements (
                    replacement_id TEXT PRIMARY KEY,
                    predecessor_job_id TEXT NOT NULL UNIQUE
                        REFERENCES prospective_collection_jobs(job_id),
                    successor_job_id TEXT NOT NULL UNIQUE
                        REFERENCES prospective_collection_jobs(job_id),
                    replaced_at TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    artifact_hash TEXT NOT NULL UNIQUE
                );
                CREATE TABLE IF NOT EXISTS prospective_collection_opportunities (
                    opportunity_id TEXT PRIMARY KEY,
                    job_id TEXT NOT NULL REFERENCES prospective_collection_jobs(job_id),
                    scheduled_for TEXT NOT NULL,
                    outcome TEXT NOT NULL,
                    started_at TEXT NOT NULL,
                    completed_at TEXT,
                    attempt_count INTEGER NOT NULL,
                    data_snapshot_id TEXT,
                    error_kind TEXT,
                    lease_token TEXT,
                    lease_expires_at TEXT,
                    UNIQUE(job_id, scheduled_for)
                );
                CREATE INDEX IF NOT EXISTS prospective_collection_opportunities_job
                    ON prospective_collection_opportunities(job_id, scheduled_for);
                CREATE TABLE IF NOT EXISTS prospective_collection_usage_records (
                    record_id TEXT PRIMARY KEY,
                    opportunity_id TEXT NOT NULL UNIQUE
                        REFERENCES prospective_collection_opportunities(opportunity_id),
                    job_id TEXT NOT NULL REFERENCES prospective_collection_jobs(job_id),
                    recorded_at TEXT NOT NULL,
                    artifact_hash TEXT NOT NULL UNIQUE
                );
                CREATE INDEX IF NOT EXISTS prospective_collection_usage_job_time
                    ON prospective_collection_usage_records(job_id, recorded_at, record_id);
                """
            )

    def register(
        self,
        job: ProspectiveCollectionJob,
        *,
        collection_policy: ProspectiveCollectionPolicy,
        source_acceptance_report: SourceRouteAcceptanceReport,
        source_config: Mapping[str, object],
        registered_at: datetime,
    ) -> None:
        _strict_utc(registered_at, "prospective collection registered_at")
        if job.starts_at < registered_at:
            raise ValueError("prospective collection job cannot start before registration")
        self._validate_binding(
            job,
            collection_policy=collection_policy,
            source_acceptance_report=source_acceptance_report,
            source_config=source_config,
        )
        self.journal.register_policy(collection_policy)
        config_artifact = self.store.artifacts.put_json(dict(source_config))
        report_artifact = self.store.artifacts.put_json(source_acceptance_report.to_dict())
        job_artifact = self.store.artifacts.put_json(job.to_dict())
        with self._connect() as connection:
            existing = connection.execute(
                "SELECT artifact_hash FROM prospective_collection_jobs WHERE job_id = ?",
                (job.job_id,),
            ).fetchone()
            if existing is not None:
                if cast(str, existing["artifact_hash"]) != job_artifact.content_hash:
                    raise ValueError("prospective collection job identity conflict")
                return
            connection.execute(
                """
                INSERT INTO prospective_collection_jobs(
                    job_id, artifact_hash, collection_policy_id,
                    source_config_artifact_hash, source_acceptance_report_artifact_hash,
                    status, registered_at, updated_at, next_due_at, backoff_until,
                    consecutive_failures, last_outcome, last_error_kind,
                    lease_token, lease_expires_at
                ) VALUES (?, ?, ?, ?, ?, 'active', ?, ?, ?, NULL, 0, NULL, NULL, NULL, NULL)
                """,
                (
                    job.job_id,
                    job_artifact.content_hash,
                    collection_policy.policy_id,
                    config_artifact.content_hash,
                    report_artifact.content_hash,
                    _timestamp(registered_at),
                    _timestamp(registered_at),
                    _timestamp(job.starts_at),
                ),
            )

    def replace_job(
        self,
        predecessor_job_id: str,
        successor_job_id: str,
        *,
        replaced_at: datetime | None = None,
        reason: str,
    ) -> ProspectiveCollectionJobReplacement:
        """Atomically retire one exact Job in favor of one registered successor."""

        caller_supplied_time = replaced_at is not None
        replacement_time = self._clock() if replaced_at is None else replaced_at
        replacement = ProspectiveCollectionJobReplacement.build(
            predecessor_job_id=predecessor_job_id,
            successor_job_id=successor_job_id,
            replaced_at=replacement_time,
            reason=reason,
        )
        predecessor = self.job(predecessor_job_id)
        successor = self.job(successor_job_id)
        if (
            predecessor.adapter_kind is not successor.adapter_kind
            or predecessor.source_acceptance_report_id != successor.source_acceptance_report_id
            or predecessor.source_acceptance_report_hash != successor.source_acceptance_report_hash
            or predecessor.source_config_hash != successor.source_config_hash
        ):
            raise ValueError(
                "prospective collection replacement requires the same accepted source route"
            )
        artifact = self.store.artifacts.put_json(replacement.to_dict())
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            existing = connection.execute(
                """
                SELECT artifact_hash FROM prospective_collection_job_replacements
                WHERE predecessor_job_id = ?
                """,
                (predecessor_job_id,),
            ).fetchone()
            if existing is not None:
                existing_replacement = prospective_collection_job_replacement_from_dict(
                    self.store.artifacts.read_json(cast(str, existing["artifact_hash"]))
                )
                same_logical_transition = (
                    existing_replacement.predecessor_job_id == predecessor_job_id
                    and existing_replacement.successor_job_id == successor_job_id
                    and existing_replacement.reason == reason
                )
                if not same_logical_transition or (
                    caller_supplied_time and existing_replacement != replacement
                ):
                    raise ValueError("prospective collection predecessor was already replaced")
                return existing_replacement
            predecessor_row = connection.execute(
                "SELECT * FROM prospective_collection_jobs WHERE job_id = ?",
                (predecessor_job_id,),
            ).fetchone()
            successor_row = connection.execute(
                "SELECT * FROM prospective_collection_jobs WHERE job_id = ?",
                (successor_job_id,),
            ).fetchone()
            if predecessor_row is None or successor_row is None:
                raise KeyError("prospective collection replacement Job is not registered")
            if cast(str, predecessor_row["status"]) != "active":
                raise ValueError("prospective collection predecessor is not active")
            if cast(str, successor_row["status"]) != "active":
                raise ValueError("prospective collection successor is not active")
            if cast(str | None, predecessor_row["lease_token"]) is not None:
                raise ValueError("prospective collection predecessor is currently in progress")
            terminal_outcomes = tuple(sorted(_TERMINAL_OUTCOMES))
            unsettled = connection.execute(
                """
                SELECT 1 FROM prospective_collection_opportunities
                WHERE job_id = ? AND outcome NOT IN (?, ?, ?, ?, ?, ?) LIMIT 1
                """,
                (predecessor_job_id, *terminal_outcomes),
            ).fetchone()
            if unsettled is not None:
                raise ValueError(
                    "prospective collection replacement cannot abandon an unsettled actual receipt"
                )
            predecessor_registered_at = _datetime(
                cast(str, predecessor_row["registered_at"]), "predecessor registered_at"
            )
            successor_registered_at = _datetime(
                cast(str, successor_row["registered_at"]), "successor registered_at"
            )
            if replacement_time < max(predecessor_registered_at, successor_registered_at):
                raise ValueError("prospective collection replacement predates Job registration")
            if (
                connection.execute(
                    """
                SELECT 1 FROM prospective_collection_job_replacements
                WHERE successor_job_id = ?
                """,
                    (successor_job_id,),
                ).fetchone()
                is not None
            ):
                raise ValueError("prospective collection successor already replaces another Job")
            connection.execute(
                """
                INSERT INTO prospective_collection_job_replacements(
                    replacement_id, predecessor_job_id, successor_job_id,
                    replaced_at, reason, artifact_hash
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    replacement.replacement_id,
                    predecessor_job_id,
                    successor_job_id,
                    _timestamp(replacement_time),
                    reason,
                    artifact.content_hash,
                ),
            )
            updated = connection.execute(
                """
                UPDATE prospective_collection_jobs
                SET status = 'replaced', updated_at = ?, backoff_until = NULL
                WHERE job_id = ? AND status = 'active' AND lease_token IS NULL
                """,
                (_timestamp(replacement_time), predecessor_job_id),
            )
            if updated.rowcount != 1:
                raise RuntimeError("prospective collection replacement lost its atomic gate")
        return replacement

    def job(self, job_id: str) -> ProspectiveCollectionJob:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT artifact_hash FROM prospective_collection_jobs WHERE job_id = ?",
                (job_id,),
            ).fetchone()
        if row is None:
            raise KeyError(f"unknown prospective collection job: {job_id}")
        return prospective_collection_job_from_dict(
            self.store.artifacts.read_json(cast(str, row["artifact_hash"]))
        )

    def source_config(self, job_id: str) -> dict[str, object]:
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT source_config_artifact_hash
                FROM prospective_collection_jobs WHERE job_id = ?
                """,
                (job_id,),
            ).fetchone()
        if row is None:
            raise KeyError(f"unknown prospective collection job: {job_id}")
        payload = self.store.artifacts.read_json(cast(str, row["source_config_artifact_hash"]))
        if not isinstance(payload, dict):
            raise ValueError("prospective collection source config artifact is invalid")
        typed_payload = cast(dict[object, object], payload)
        if not all(isinstance(key, str) for key in typed_payload):
            raise ValueError("prospective collection source config artifact is invalid")
        return cast(dict[str, object], typed_payload)

    def source_acceptance_report(self, job_id: str) -> SourceRouteAcceptanceReport:
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT source_acceptance_report_artifact_hash
                FROM prospective_collection_jobs WHERE job_id = ?
                """,
                (job_id,),
            ).fetchone()
        if row is None:
            raise KeyError(f"unknown prospective collection job: {job_id}")
        return source_route_acceptance_report_from_dict(
            self.store.artifacts.read_json(cast(str, row["source_acceptance_report_artifact_hash"]))
        )

    def due_job_ids(self, *, now: datetime, limit: int = 100) -> tuple[str, ...]:
        _strict_utc(now, "prospective collection due query time")
        if limit < 1:
            raise ValueError("prospective collection due limit must be positive")
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT job_id, next_due_at FROM prospective_collection_jobs
                WHERE status = 'active' AND next_due_at <= ?
                ORDER BY job_id
                """,
                (_timestamp(now),),
            ).fetchall()
        ordered = sorted(
            (
                (
                    _datetime(cast(str, row["next_due_at"]), "next_due_at")
                    + timedelta(seconds=self.job(cast(str, row["job_id"])).misfire_grace_seconds),
                    cast(str, row["job_id"]),
                )
                for row in rows
            ),
        )
        return tuple(job_id for _, job_id in ordered[:limit])

    def job_ids(self, *, limit: int = 100) -> tuple[str, ...]:
        if limit < 1:
            raise ValueError("prospective collection job limit must be positive")
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT job_id FROM prospective_collection_jobs
                ORDER BY registered_at, job_id LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return tuple(cast(str, row["job_id"]) for row in rows)

    def run_due(
        self,
        job_id: str,
        *,
        now: datetime,
        collector: ScheduledCollector,
        cancelled: Callable[[], bool] | None = None,
    ) -> CollectionRunResult:
        _strict_utc(now, "prospective collection run time")
        job = self.job(job_id)
        policy = self.journal.policy(job.collection_policy_id)
        claim = self._claim_due(job, policy=policy, now=now)
        if claim.result is not None:
            return claim.result
        if claim.lease_token is None or claim.opportunity_id is None or claim.scheduled_for is None:
            raise RuntimeError("prospective collection due claim is incomplete")
        if cancelled is not None and cancelled():
            return self._finish_without_snapshot(
                job,
                policy=policy,
                opportunity_id=claim.opportunity_id,
                lease_token=claim.lease_token,
                scheduled_for=claim.scheduled_for,
                now=now,
                completed_at=self._completion_time(),
                outcome="cancelled",
                error_kind="collection_cancelled",
                missed_opportunities=claim.missed_opportunities,
            )

        snapshot: DataSnapshot
        if claim.staged_snapshot_id is not None:
            try:
                snapshot = self.store.get(claim.staged_snapshot_id)
            except (FileNotFoundError, KeyError, ValueError):
                return self._release_staged_failure(
                    job,
                    policy=policy,
                    opportunity_id=claim.opportunity_id,
                    lease_token=claim.lease_token,
                    now=now,
                    error_kind="staged_snapshot_unavailable",
                    missed_opportunities=claim.missed_opportunities,
                )
        else:
            try:
                snapshot = collector(
                    policy,
                    self.source_config(job_id),
                    claim.scheduled_for,
                )
                self._validate_snapshot(
                    snapshot,
                    policy=policy,
                    scheduled_for=claim.scheduled_for,
                )
            except Exception as exc:
                if cancelled is not None and cancelled():
                    return self._finish_without_snapshot(
                        job,
                        policy=policy,
                        opportunity_id=claim.opportunity_id,
                        lease_token=claim.lease_token,
                        scheduled_for=claim.scheduled_for,
                        now=now,
                        completed_at=self._completion_time(),
                        outcome="cancelled",
                        error_kind="collection_cancelled",
                        missed_opportunities=claim.missed_opportunities,
                    )
                return self._finish_without_snapshot(
                    job,
                    policy=policy,
                    opportunity_id=claim.opportunity_id,
                    lease_token=claim.lease_token,
                    scheduled_for=claim.scheduled_for,
                    now=now,
                    completed_at=self._completion_time(),
                    outcome="collector_failure",
                    error_kind=f"collector_{type(exc).__name__}",
                    missed_opportunities=claim.missed_opportunities,
                )
            if cancelled is not None and cancelled():
                return self._finish_without_snapshot(
                    job,
                    policy=policy,
                    opportunity_id=claim.opportunity_id,
                    lease_token=claim.lease_token,
                    scheduled_for=claim.scheduled_for,
                    now=now,
                    completed_at=self._completion_time(not_before=snapshot.completed_at),
                    outcome="cancelled",
                    error_kind="collection_cancelled",
                    missed_opportunities=claim.missed_opportunities,
                )
            if not self._stage_snapshot(
                job_id,
                opportunity_id=claim.opportunity_id,
                lease_token=claim.lease_token,
                snapshot_id=snapshot.snapshot_id,
            ):
                return _run_result(
                    job_id,
                    "stale_claim",
                    scheduled_for=claim.scheduled_for,
                    opportunity_id=claim.opportunity_id,
                    data_snapshot_id=snapshot.snapshot_id,
                    missed_opportunities=claim.missed_opportunities,
                    error_kind="collection_lease_lost",
                )

        if cancelled is not None and cancelled():
            return self._finish_without_snapshot(
                job,
                policy=policy,
                opportunity_id=claim.opportunity_id,
                lease_token=claim.lease_token,
                scheduled_for=claim.scheduled_for,
                now=now,
                completed_at=self._completion_time(not_before=snapshot.completed_at),
                outcome="cancelled",
                error_kind="collection_cancelled",
                missed_opportunities=claim.missed_opportunities,
            )

        try:
            self.journal.record_snapshot(snapshot, policy=policy)
        except Exception as exc:
            return self._release_staged_failure(
                job,
                policy=policy,
                opportunity_id=claim.opportunity_id,
                lease_token=claim.lease_token,
                now=now,
                error_kind=f"journal_{type(exc).__name__}",
                missed_opportunities=claim.missed_opportunities,
            )

        no_data = bool(snapshot.attempts) and all(
            item.status is DataFetchStatus.NO_DATA for item in snapshot.attempts
        )
        outcome = (
            "success" if snapshot.coverage_complete else "no_data" if no_data else "source_failure"
        )
        error_kind = None
        if outcome == "source_failure":
            error_kind = next(
                (item.error_kind for item in snapshot.attempts if item.error_kind is not None),
                "source_coverage_incomplete",
            )
        return self._finish_with_snapshot(
            job,
            policy=policy,
            opportunity_id=claim.opportunity_id,
            lease_token=claim.lease_token,
            scheduled_for=claim.scheduled_for,
            now=now,
            completed_at=self._completion_time(not_before=snapshot.completed_at),
            outcome=outcome,
            snapshot_id=snapshot.snapshot_id,
            error_kind=error_kind,
            missed_opportunities=claim.missed_opportunities,
        )

    def opportunities(self, job_id: str) -> tuple[CollectionOpportunity, ...]:
        self.job(job_id)
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT * FROM prospective_collection_opportunities
                WHERE job_id = ? ORDER BY scheduled_for, opportunity_id
                """,
                (job_id,),
            ).fetchall()
        return tuple(_opportunity_from_row(row) for row in rows)

    def usage_records(
        self,
        job_id: str,
        *,
        since: datetime | None = None,
    ) -> tuple[CollectionUsageRecord, ...]:
        self.job(job_id)
        if since is not None:
            _strict_utc(since, "collection usage since")
        with self._connect() as connection:
            if since is None:
                rows = connection.execute(
                    """
                    SELECT artifact_hash FROM prospective_collection_usage_records
                    WHERE job_id = ? ORDER BY recorded_at, record_id
                    """,
                    (job_id,),
                ).fetchall()
            else:
                rows = connection.execute(
                    """
                    SELECT artifact_hash FROM prospective_collection_usage_records
                    WHERE job_id = ? AND recorded_at >= ?
                    ORDER BY recorded_at, record_id
                    """,
                    (job_id, _timestamp(since)),
                ).fetchall()
        return tuple(
            collection_usage_record_from_dict(
                self.store.artifacts.read_json(cast(str, row["artifact_hash"]))
            )
            for row in rows
        )

    def usage_summary(
        self,
        job_id: str,
        *,
        since: datetime | None = None,
    ) -> dict[str, object]:
        job = self.job(job_id)
        records = self.usage_records(job_id, since=since)

        summary_measurements: dict[str, int | None] = {}
        unknown_measurement_records: dict[str, int] = {}
        for name in _USAGE_MEASUREMENT_DIMENSIONS:
            values = tuple(
                cast(int, value) for item in records if (value := getattr(item, name)) is not None
            )
            summary_measurements[name] = None if not values else sum(values)
            unknown_measurement_records[f"{name}_unknown_records"] = len(records) - len(values)

        latencies = tuple(item.latency_ms for item in records)
        return {
            "job_id": job_id,
            "since": None if since is None else _timestamp(since),
            "record_count": len(records),
            "success_count": sum(item.outcome in {"success", "no_data"} for item in records),
            "no_data_count": sum(item.outcome == "no_data" for item in records),
            "failure_count": sum(
                item.outcome in {"source_failure", "collector_failure"} for item in records
            ),
            "collection_attempt_count": sum(item.collection_attempt_count for item in records),
            **summary_measurements,
            **unknown_measurement_records,
            "average_latency_ms": (None if not latencies else sum(latencies) / len(latencies)),
            "incremental_cost_microusd": None,
            "cost_basis": _usage_cost_basis(job.adapter_kind),
        }

    def health(self, job_id: str, *, now: datetime) -> CollectionHealth:
        _strict_utc(now, "prospective collection health time")
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM prospective_collection_jobs WHERE job_id = ?",
                (job_id,),
            ).fetchone()
            if row is None:
                raise KeyError(f"unknown prospective collection job: {job_id}")
            counts = Counter(
                {
                    cast(str, item["outcome"]): cast(int, item["count"])
                    for item in connection.execute(
                        """
                        SELECT outcome, COUNT(*) AS count
                        FROM prospective_collection_opportunities
                        WHERE job_id = ? GROUP BY outcome
                        """,
                        (job_id,),
                    ).fetchall()
                }
            )
        next_due_at = _datetime(cast(str, row["next_due_at"]), "next_due_at")
        last_error_kind = cast(str | None, row["last_error_kind"])
        return CollectionHealth(
            job_id=job_id,
            status=cast(str, row["status"]),
            next_due_at=next_due_at,
            backoff_until=_optional_datetime(
                cast(str | None, row["backoff_until"]), "backoff_until"
            ),
            last_outcome=cast(str | None, row["last_outcome"]),
            last_error_kind=last_error_kind,
            successful_opportunities=counts["success"] + counts["no_data"],
            no_data_opportunities=counts["no_data"],
            source_failures=counts["source_failure"],
            collector_failures=counts["collector_failure"],
            cancelled_opportunities=counts["cancelled"],
            missed_opportunities=counts["missed"],
            incomplete_interval=counts["missed"] > 0,
            lag_seconds=max(0, int((now - next_due_at).total_seconds())),
        )

    def _validate_binding(
        self,
        job: ProspectiveCollectionJob,
        *,
        collection_policy: ProspectiveCollectionPolicy,
        source_acceptance_report: SourceRouteAcceptanceReport,
        source_config: Mapping[str, object],
    ) -> None:
        if job.collection_policy_id != collection_policy.policy_id:
            raise ValueError("collection job policy identity mismatch")
        if collection_policy.window_start >= job.starts_at:
            raise ValueError("collection policy window must start before its first due time")
        if not source_acceptance_report.accepted:
            raise ValueError("collection job requires an accepted source route")
        if job.source_acceptance_report_id != source_acceptance_report.report_id:
            raise ValueError("collection job source acceptance report identity mismatch")
        if job.source_acceptance_report_hash != canonical_hash(source_acceptance_report.to_dict()):
            raise ValueError("collection job source acceptance report hash mismatch")
        if job.source_config_hash != canonical_hash(source_config):
            raise ValueError("collection job source config hash mismatch")
        _validate_adapter_policy_scope(
            job.adapter_kind,
            collection_policy=collection_policy,
            source_config=source_config,
        )
        if len(collection_policy.sources) != 1:
            raise ValueError("collection job requires exactly one accepted source route")
        source = collection_policy.sources[0]
        declaration = source_acceptance_report.declaration
        if (
            declaration.provider_id != source.provider_id
            or declaration.provider_version != source.provider_version
            or declaration.upstream_source != source.upstream_source
            or declaration.provider_manifest_hash != source.manifest_hash
            or declaration.source_config_hash != source.source_config_hash
            or declaration.source_config_hash != job.source_config_hash
            or declaration.capability is not collection_policy.capability
        ):
            raise ValueError("collection job accepted route does not match its policy")
        if job.maximum_jitter_seconds >= collection_policy.poll_interval_seconds:
            raise ValueError("collection jitter must be shorter than the policy interval")
        if (
            collection_policy.poll_interval_seconds + job.maximum_jitter_seconds
            > collection_policy.maximum_gap_seconds
        ):
            raise ValueError("collection jitter would exceed the policy maximum gap")

    def _completion_time(self, *, not_before: datetime | None = None) -> datetime:
        completed_at = self._clock()
        _strict_utc(completed_at, "prospective collection completion clock")
        if not_before is not None:
            _strict_utc(not_before, "prospective collection completion lower bound")
            completed_at = max(completed_at, not_before)
        return completed_at

    def _validate_snapshot(
        self,
        snapshot: DataSnapshot,
        *,
        policy: ProspectiveCollectionPolicy,
        scheduled_for: datetime,
    ) -> None:
        if self.store.get(snapshot.snapshot_id) != snapshot:
            raise ValueError("collector must persist its immutable Data Snapshot")
        if snapshot.query.pit_lane is not DataPITLane.PROSPECTIVE:
            raise ValueError("collector Snapshot must use the prospective PIT lane")
        expected_window_start, expected_parameters = policy.resolve_query(scheduled_for)
        if (
            snapshot.query.capability is not policy.capability
            or snapshot.query.source_policy_id != policy.policy_id
            or snapshot.query.sources != policy.sources
            or snapshot.query.window_start != expected_window_start
            or snapshot.query.parameters != expected_parameters
        ):
            raise ValueError("collector Snapshot does not match its collection policy")
        if (
            snapshot.query.as_of < scheduled_for
            or snapshot.completed_at < scheduled_for
            or any(item.retrieved_at < scheduled_for for item in snapshot.attempts)
        ):
            raise ValueError("collector Snapshot predates its logical due opportunity")

    def _claim_due(
        self,
        job: ProspectiveCollectionJob,
        *,
        policy: ProspectiveCollectionPolicy,
        now: datetime,
    ) -> _DueClaim:
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT * FROM prospective_collection_jobs WHERE job_id = ?",
                (job.job_id,),
            ).fetchone()
            if row is None:
                raise KeyError(f"unknown prospective collection job: {job.job_id}")
            if cast(str, row["status"]) != "active":
                return _DueClaim(
                    result=_run_result(job.job_id, "not_active", error_kind="job_not_active")
                )
            lease_token = cast(str | None, row["lease_token"])
            lease_expires_at = _optional_datetime(
                cast(str | None, row["lease_expires_at"]), "lease_expires_at"
            )
            if lease_token is not None and lease_expires_at is not None and now < lease_expires_at:
                return _DueClaim(result=_run_result(job.job_id, "in_progress"))
            next_due = _datetime(cast(str, row["next_due_at"]), "next_due_at")
            backoff_until = _optional_datetime(
                cast(str | None, row["backoff_until"]), "backoff_until"
            )
            if backoff_until is not None and now < backoff_until:
                return _DueClaim(
                    result=_run_result(
                        job.job_id,
                        "backing_off",
                        scheduled_for=next_due,
                        error_kind=cast(str | None, row["last_error_kind"]),
                    )
                )
            if now < next_due:
                return _DueClaim(result=_run_result(job.job_id, "not_due"))

            missed = 0
            scheduled_for = next_due
            opportunity_id = _opportunity_id(job.job_id, scheduled_for)
            opportunity = connection.execute(
                """
                SELECT * FROM prospective_collection_opportunities
                WHERE opportunity_id = ?
                """,
                (opportunity_id,),
            ).fetchone()
            if opportunity is not None and cast(str, opportunity["outcome"]) in _TERMINAL_OUTCOMES:
                raise RuntimeError("terminal collection opportunity was not advanced")

            while opportunity is None and now > scheduled_for + timedelta(
                seconds=job.misfire_grace_seconds
            ):
                if missed >= _MAX_MISFIRES_PER_RUN:
                    raise RuntimeError("prospective collection misfire catch-up limit exceeded")
                opportunity_id = _opportunity_id(job.job_id, scheduled_for)
                connection.execute(
                    """
                    INSERT OR IGNORE INTO prospective_collection_opportunities(
                        opportunity_id, job_id, scheduled_for, outcome, started_at,
                        completed_at, attempt_count, data_snapshot_id, error_kind,
                        lease_token, lease_expires_at
                    ) VALUES (?, ?, ?, 'missed', ?, ?, 0, NULL,
                              'collection_misfire', NULL, NULL)
                    """,
                    (
                        opportunity_id,
                        job.job_id,
                        _timestamp(scheduled_for),
                        _timestamp(self._completion_time(not_before=scheduled_for)),
                        _timestamp(now),
                    ),
                )
                missed += 1
                scheduled_for = self._next_due(job, policy=policy, scheduled_for=scheduled_for)
                opportunity_id = _opportunity_id(job.job_id, scheduled_for)
                opportunity = connection.execute(
                    """
                    SELECT * FROM prospective_collection_opportunities
                    WHERE opportunity_id = ?
                    """,
                    (opportunity_id,),
                ).fetchone()
                if (
                    opportunity is not None
                    and cast(str, opportunity["outcome"]) in _TERMINAL_OUTCOMES
                ):
                    raise RuntimeError("terminal collection opportunity was not advanced")
            if scheduled_for > now:
                connection.execute(
                    """
                    UPDATE prospective_collection_jobs
                    SET updated_at = ?, next_due_at = ?, last_outcome = 'missed',
                        last_error_kind = 'collection_misfire', lease_token = NULL,
                        lease_expires_at = NULL
                    WHERE job_id = ?
                    """,
                    (_timestamp(now), _timestamp(scheduled_for), job.job_id),
                )
                return _DueClaim(
                    result=_run_result(
                        job.job_id,
                        "misfires_recorded",
                        scheduled_for=scheduled_for,
                        missed_opportunities=missed,
                        error_kind="collection_misfire",
                    )
                )

            staged_snapshot_id: str | None = None
            attempt_count = 1
            if opportunity is not None:
                staged_snapshot_id = cast(str | None, opportunity["data_snapshot_id"])
                attempt_count = cast(int, opportunity["attempt_count"]) + 1
            new_lease = uuid.uuid4().hex
            lease_expiry = now + timedelta(seconds=self.lease_timeout_seconds)
            if opportunity is None:
                connection.execute(
                    """
                    INSERT INTO prospective_collection_opportunities(
                        opportunity_id, job_id, scheduled_for, outcome, started_at,
                        completed_at, attempt_count, data_snapshot_id, error_kind,
                        lease_token, lease_expires_at
                    ) VALUES (?, ?, ?, 'in_progress', ?, NULL, 1, NULL, NULL, ?, ?)
                    """,
                    (
                        opportunity_id,
                        job.job_id,
                        _timestamp(scheduled_for),
                        _timestamp(now),
                        new_lease,
                        _timestamp(lease_expiry),
                    ),
                )
            else:
                connection.execute(
                    """
                    UPDATE prospective_collection_opportunities
                    SET outcome = CASE WHEN data_snapshot_id IS NULL
                                       THEN 'in_progress' ELSE 'captured' END,
                        started_at = ?, completed_at = NULL, attempt_count = ?,
                        error_kind = NULL, lease_token = ?, lease_expires_at = ?
                    WHERE opportunity_id = ?
                    """,
                    (
                        _timestamp(now),
                        attempt_count,
                        new_lease,
                        _timestamp(lease_expiry),
                        opportunity_id,
                    ),
                )
            connection.execute(
                """
                UPDATE prospective_collection_jobs
                SET updated_at = ?, next_due_at = ?, lease_token = ?, lease_expires_at = ?
                WHERE job_id = ?
                """,
                (
                    _timestamp(now),
                    _timestamp(scheduled_for),
                    new_lease,
                    _timestamp(lease_expiry),
                    job.job_id,
                ),
            )
        return _DueClaim(
            lease_token=new_lease,
            opportunity_id=opportunity_id,
            scheduled_for=scheduled_for,
            staged_snapshot_id=staged_snapshot_id,
            missed_opportunities=missed,
        )

    def _stage_snapshot(
        self,
        job_id: str,
        *,
        opportunity_id: str,
        lease_token: str,
        snapshot_id: str,
    ) -> bool:
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            owner = connection.execute(
                "SELECT lease_token FROM prospective_collection_jobs WHERE job_id = ?",
                (job_id,),
            ).fetchone()
            if owner is None:
                raise KeyError(f"unknown prospective collection job: {job_id}")
            if cast(str | None, owner["lease_token"]) != lease_token:
                return False
            result = connection.execute(
                """
                UPDATE prospective_collection_opportunities
                SET outcome = 'captured', data_snapshot_id = ?
                WHERE opportunity_id = ? AND lease_token = ?
                """,
                (snapshot_id, opportunity_id, lease_token),
            )
        return result.rowcount == 1

    def _finish_with_snapshot(
        self,
        job: ProspectiveCollectionJob,
        *,
        policy: ProspectiveCollectionPolicy,
        opportunity_id: str,
        lease_token: str,
        scheduled_for: datetime,
        now: datetime,
        completed_at: datetime,
        outcome: str,
        snapshot_id: str,
        error_kind: str | None,
        missed_opportunities: int,
    ) -> CollectionRunResult:
        return self._finish(
            job,
            policy=policy,
            opportunity_id=opportunity_id,
            lease_token=lease_token,
            scheduled_for=scheduled_for,
            now=now,
            completed_at=completed_at,
            outcome=outcome,
            snapshot_id=snapshot_id,
            error_kind=error_kind,
            missed_opportunities=missed_opportunities,
        )

    def _finish_without_snapshot(
        self,
        job: ProspectiveCollectionJob,
        *,
        policy: ProspectiveCollectionPolicy,
        opportunity_id: str,
        lease_token: str,
        scheduled_for: datetime,
        now: datetime,
        completed_at: datetime,
        outcome: str,
        error_kind: str,
        missed_opportunities: int,
    ) -> CollectionRunResult:
        return self._finish(
            job,
            policy=policy,
            opportunity_id=opportunity_id,
            lease_token=lease_token,
            scheduled_for=scheduled_for,
            now=now,
            completed_at=completed_at,
            outcome=outcome,
            snapshot_id=None,
            error_kind=error_kind,
            missed_opportunities=missed_opportunities,
        )

    def _finish(
        self,
        job: ProspectiveCollectionJob,
        *,
        policy: ProspectiveCollectionPolicy,
        opportunity_id: str,
        lease_token: str,
        scheduled_for: datetime,
        now: datetime,
        completed_at: datetime,
        outcome: str,
        snapshot_id: str | None,
        error_kind: str | None,
        missed_opportunities: int,
    ) -> CollectionRunResult:
        if outcome not in _TERMINAL_OUTCOMES - {"missed"}:
            raise ValueError("unsupported prospective collection terminal outcome")
        _strict_utc(completed_at, "prospective collection completed_at")
        failed = outcome in {"source_failure", "collector_failure"}

        # Snapshot parsing, raw-capture verification and usage CAS persistence
        # must not monopolize the shared authority DB's writer during preparation.
        with closing(self._connect()) as connection, connection:
            connection.execute("BEGIN")
            owner = connection.execute(
                "SELECT lease_token FROM prospective_collection_jobs WHERE job_id = ?",
                (job.job_id,),
            ).fetchone()
            if owner is None:
                raise KeyError(f"unknown prospective collection job: {job.job_id}")
            if cast(str | None, owner["lease_token"]) != lease_token:
                return _run_result(
                    job.job_id,
                    "stale_claim",
                    scheduled_for=scheduled_for,
                    opportunity_id=opportunity_id,
                    data_snapshot_id=snapshot_id,
                    missed_opportunities=missed_opportunities,
                    error_kind="collection_lease_lost",
                )
            prepared_opportunity = connection.execute(
                """
                SELECT started_at, attempt_count, data_snapshot_id, outcome
                FROM prospective_collection_opportunities
                WHERE opportunity_id = ? AND lease_token = ?
                """,
                (opportunity_id, lease_token),
            ).fetchone()
        if prepared_opportunity is None:
            raise RuntimeError("prospective collection opportunity lease is missing")
        started_at = _datetime(cast(str, prepared_opportunity["started_at"]), "started_at")
        completed_at = max(completed_at, started_at)
        usage = self._build_usage_record(
            job=job,
            opportunity_id=opportunity_id,
            scheduled_for=scheduled_for,
            outcome=outcome,
            collection_attempt_count=cast(int, prepared_opportunity["attempt_count"]),
            started_at=started_at,
            completed_at=completed_at,
            snapshot_id=snapshot_id,
            error_kind=error_kind,
        )
        usage_artifact = self.store.artifacts.put_json(usage.to_dict())

        with closing(self._connect()) as connection, connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                """
                SELECT consecutive_failures, lease_token
                FROM prospective_collection_jobs WHERE job_id = ?
                """,
                (job.job_id,),
            ).fetchone()
            if row is None:
                raise KeyError(f"unknown prospective collection job: {job.job_id}")
            if cast(str | None, row["lease_token"]) != lease_token:
                return _run_result(
                    job.job_id,
                    "stale_claim",
                    scheduled_for=scheduled_for,
                    opportunity_id=opportunity_id,
                    data_snapshot_id=snapshot_id,
                    missed_opportunities=missed_opportunities,
                    error_kind="collection_lease_lost",
                )
            opportunity = connection.execute(
                """
                SELECT started_at, attempt_count, data_snapshot_id, outcome
                FROM prospective_collection_opportunities
                WHERE opportunity_id = ? AND lease_token = ?
                """,
                (opportunity_id, lease_token),
            ).fetchone()
            if opportunity is None:
                raise RuntimeError("prospective collection opportunity lease is missing")
            if tuple(opportunity) != tuple(prepared_opportunity):
                raise ValueError("collection finalization inputs changed during preparation")
            failures = cast(int, row["consecutive_failures"]) + 1 if failed else 0
            backoff_until = (
                now + timedelta(seconds=_backoff_seconds(policy, failures)) if failed else None
            )
            last_error = (
                error_kind
                if error_kind is not None
                else ("collection_misfire" if missed_opportunities else None)
            )
            connection.execute(
                """
                UPDATE prospective_collection_opportunities
                SET outcome = ?, completed_at = ?, data_snapshot_id = ?, error_kind = ?,
                    lease_token = NULL, lease_expires_at = NULL
                WHERE opportunity_id = ? AND lease_token = ?
                """,
                (
                    outcome,
                    _timestamp(completed_at),
                    snapshot_id,
                    error_kind,
                    opportunity_id,
                    lease_token,
                ),
            )
            connection.execute(
                """
                INSERT INTO prospective_collection_usage_records(
                    record_id, opportunity_id, job_id, recorded_at, artifact_hash
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (
                    usage.record_id,
                    opportunity_id,
                    job.job_id,
                    _timestamp(usage.recorded_at),
                    usage_artifact.content_hash,
                ),
            )
            connection.execute(
                """
                UPDATE prospective_collection_jobs
                SET updated_at = ?, next_due_at = ?, backoff_until = ?,
                    consecutive_failures = ?, last_outcome = ?, last_error_kind = ?,
                    lease_token = NULL, lease_expires_at = NULL
                WHERE job_id = ? AND lease_token = ?
                """,
                (
                    _timestamp(now),
                    _timestamp(self._next_due(job, policy=policy, scheduled_for=scheduled_for)),
                    None if backoff_until is None else _timestamp(backoff_until),
                    failures,
                    outcome,
                    last_error,
                    job.job_id,
                    lease_token,
                ),
            )
        return _run_result(
            job.job_id,
            outcome,
            scheduled_for=scheduled_for,
            opportunity_id=opportunity_id,
            data_snapshot_id=snapshot_id,
            missed_opportunities=missed_opportunities,
            error_kind=error_kind,
            usage_record_id=usage.record_id,
        )

    def _build_usage_record(
        self,
        *,
        job: ProspectiveCollectionJob,
        opportunity_id: str,
        scheduled_for: datetime,
        outcome: str,
        collection_attempt_count: int,
        started_at: datetime,
        completed_at: datetime,
        snapshot_id: str | None,
        error_kind: str | None,
    ) -> CollectionUsageRecord:
        provider_attempt_count: int | None = None
        request_count: int | None = None
        page_count: int | None = None
        response_bytes: int | None = None
        raw_artifact_bytes: int | None = None
        received_records: int | None = None
        accepted_records: int | None = None
        if snapshot_id is not None:
            snapshot = self.store.get(snapshot_id)
            provider_attempt_count = len(snapshot.attempts)
            received_records = sum(item.received_count for item in snapshot.attempts)
            accepted_records = sum(item.accepted_count for item in snapshot.attempts)
            raw_payloads: list[bytes] = []
            for attempt in snapshot.attempts:
                if attempt.raw_response_hash is None:
                    continue
                artifact = self.store.artifacts.get(
                    attempt.raw_response_hash,
                    media_type="application/octet-stream",
                )
                raw_payloads.append(artifact.path.read_bytes())
            raw_artifact_bytes = sum(len(item) for item in raw_payloads)
            if (
                job.adapter_kind is ProspectiveCollectionAdapterKind.TUSHARE_OBSERVATION
                and len(raw_payloads) == 1
                and snapshot.attempts[0].provider_id == "tushare-observation"
            ):
                tushare_usage = summarize_tushare_observation_capture_usage(raw_payloads[0])
                request_count = tushare_usage.request_count
                page_count = tushare_usage.request_count
                response_bytes = tushare_usage.response_bytes
        core: dict[str, object] = {
            "schema_version": PROSPECTIVE_COLLECTION_USAGE_RECORD_SCHEMA,
            "opportunity_id": opportunity_id,
            "job_id": job.job_id,
            "scheduled_for": _timestamp(scheduled_for),
            "adapter_kind": job.adapter_kind.value,
            "outcome": outcome,
            "collection_attempt_count": collection_attempt_count,
            "provider_attempt_count": provider_attempt_count,
            "request_count": request_count,
            "page_count": page_count,
            "response_bytes": response_bytes,
            "raw_artifact_bytes": raw_artifact_bytes,
            "received_records": received_records,
            "accepted_records": accepted_records,
            "latency_ms": (completed_at - started_at).total_seconds() * 1000,
            "incremental_cost_microusd": None,
            "cost_basis": _usage_cost_basis(job.adapter_kind),
            "error_kind": error_kind,
            "recorded_at": _timestamp(completed_at),
            "execution_capability": False,
        }
        return CollectionUsageRecord(
            record_id=f"prospective-collection-usage-{canonical_hash(core)}",
            opportunity_id=opportunity_id,
            job_id=job.job_id,
            scheduled_for=scheduled_for,
            adapter_kind=job.adapter_kind,
            outcome=outcome,
            collection_attempt_count=collection_attempt_count,
            provider_attempt_count=provider_attempt_count,
            request_count=request_count,
            page_count=page_count,
            response_bytes=response_bytes,
            raw_artifact_bytes=raw_artifact_bytes,
            received_records=received_records,
            accepted_records=accepted_records,
            latency_ms=(completed_at - started_at).total_seconds() * 1000,
            incremental_cost_microusd=None,
            cost_basis=_usage_cost_basis(job.adapter_kind),
            error_kind=error_kind,
            recorded_at=completed_at,
        )

    def _release_staged_failure(
        self,
        job: ProspectiveCollectionJob,
        *,
        policy: ProspectiveCollectionPolicy,
        opportunity_id: str,
        lease_token: str,
        now: datetime,
        error_kind: str,
        missed_opportunities: int,
    ) -> CollectionRunResult:
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT consecutive_failures, lease_token FROM prospective_collection_jobs "
                "WHERE job_id = ?",
                (job.job_id,),
            ).fetchone()
            if row is None:
                raise KeyError(f"unknown prospective collection job: {job.job_id}")
            if cast(str | None, row["lease_token"]) != lease_token:
                return _run_result(
                    job.job_id,
                    "stale_claim",
                    opportunity_id=opportunity_id,
                    missed_opportunities=missed_opportunities,
                    error_kind="collection_lease_lost",
                )
            failures = cast(int, row["consecutive_failures"]) + 1
            connection.execute(
                """
                UPDATE prospective_collection_opportunities
                SET error_kind = ?, lease_token = NULL, lease_expires_at = NULL
                WHERE opportunity_id = ? AND lease_token = ?
                """,
                (error_kind, opportunity_id, lease_token),
            )
            connection.execute(
                """
                UPDATE prospective_collection_jobs
                SET updated_at = ?, backoff_until = ?, consecutive_failures = ?,
                    last_outcome = 'storage_failure', last_error_kind = ?,
                    lease_token = NULL, lease_expires_at = NULL
                WHERE job_id = ? AND lease_token = ?
                """,
                (
                    _timestamp(now),
                    _timestamp(now + timedelta(seconds=_backoff_seconds(policy, failures))),
                    failures,
                    error_kind,
                    job.job_id,
                    lease_token,
                ),
            )
        return _run_result(
            job.job_id,
            "storage_failure",
            opportunity_id=opportunity_id,
            missed_opportunities=missed_opportunities,
            error_kind=error_kind,
        )

    def _next_due(
        self,
        job: ProspectiveCollectionJob,
        *,
        policy: ProspectiveCollectionPolicy,
        scheduled_for: datetime,
    ) -> datetime:
        nominal = scheduled_for + timedelta(seconds=policy.poll_interval_seconds)
        if job.maximum_jitter_seconds == 0:
            return nominal
        digest = canonical_hash({"job_id": job.job_id, "nominal_due_at": _timestamp(nominal)})
        jitter = int(digest[:8], 16) % (job.maximum_jitter_seconds + 1)
        return nominal + timedelta(seconds=jitter)


@dataclass(frozen=True, slots=True)
class _DueClaim:
    lease_token: str | None = None
    opportunity_id: str | None = None
    scheduled_for: datetime | None = None
    staged_snapshot_id: str | None = None
    missed_opportunities: int = 0
    result: CollectionRunResult | None = None


def prospective_collection_job_from_dict(value: object) -> ProspectiveCollectionJob:
    if not isinstance(value, dict):
        raise ValueError("prospective collection job must be an object")
    untyped_payload = cast(dict[object, object], value)
    if not all(isinstance(key, str) for key in untyped_payload):
        raise ValueError("prospective collection job must be an object")
    payload = cast(dict[str, object], untyped_payload)
    expected = {
        "schema_version",
        "job_id",
        "adapter_kind",
        "collection_policy_id",
        "source_acceptance_report_id",
        "source_acceptance_report_hash",
        "source_config_hash",
        "starts_at",
        "misfire_grace_seconds",
        "maximum_jitter_seconds",
        "provider_timeout_seconds",
        "execution_capability",
    }
    if set(payload) != expected:
        raise ValueError("prospective collection job fields are not closed")
    job = ProspectiveCollectionJob(
        schema_version=_string(payload, "schema_version"),
        job_id=_string(payload, "job_id"),
        adapter_kind=ProspectiveCollectionAdapterKind(_string(payload, "adapter_kind")),
        collection_policy_id=_string(payload, "collection_policy_id"),
        source_acceptance_report_id=_string(payload, "source_acceptance_report_id"),
        source_acceptance_report_hash=_string(payload, "source_acceptance_report_hash"),
        source_config_hash=_string(payload, "source_config_hash"),
        starts_at=_datetime(_string(payload, "starts_at"), "starts_at"),
        misfire_grace_seconds=_integer(payload, "misfire_grace_seconds"),
        maximum_jitter_seconds=_integer(payload, "maximum_jitter_seconds"),
        provider_timeout_seconds=_number(payload, "provider_timeout_seconds"),
        execution_capability=_boolean(payload, "execution_capability"),
    )
    if job.to_dict() != payload:
        raise ValueError("prospective collection job is not canonical")
    return job


def prospective_collection_job_replacement_from_dict(
    value: object,
) -> ProspectiveCollectionJobReplacement:
    if not isinstance(value, dict):
        raise ValueError("prospective collection Job Replacement must be an object")
    raw = cast(dict[object, object], value)
    if not all(isinstance(key, str) for key in raw):
        raise ValueError("prospective collection Job Replacement must be an object")
    payload = cast(dict[str, object], raw)
    expected = {
        "schema_version",
        "replacement_id",
        "predecessor_job_id",
        "successor_job_id",
        "replaced_at",
        "reason",
        "execution_capability",
    }
    if set(payload) != expected:
        raise ValueError("prospective collection Job Replacement fields are not closed")
    replacement = ProspectiveCollectionJobReplacement(
        schema_version=_string(payload, "schema_version"),
        replacement_id=_string(payload, "replacement_id"),
        predecessor_job_id=_string(payload, "predecessor_job_id"),
        successor_job_id=_string(payload, "successor_job_id"),
        replaced_at=_datetime(_string(payload, "replaced_at"), "replaced_at"),
        reason=_string(payload, "reason"),
        execution_capability=_boolean(payload, "execution_capability"),
    )
    if replacement.to_dict() != payload:
        raise ValueError("prospective collection Job Replacement is not canonical")
    return replacement


def collection_usage_record_from_dict(value: object) -> CollectionUsageRecord:
    if not isinstance(value, dict):
        raise ValueError("Collection Usage Record must be an object")
    raw = cast(dict[object, object], value)
    if not all(isinstance(key, str) for key in raw):
        raise ValueError("Collection Usage Record must be an object")
    payload = cast(dict[str, object], raw)
    base_fields = {
        "schema_version",
        "record_id",
        "opportunity_id",
        "job_id",
        "scheduled_for",
        "adapter_kind",
        "outcome",
        "collection_attempt_count",
        "provider_attempt_count",
        "request_count",
        "page_count",
        "response_bytes",
        "raw_artifact_bytes",
        "received_records",
        "accepted_records",
        "latency_ms",
        "error_kind",
        "recorded_at",
        "execution_capability",
    }
    cost_fields = {"incremental_cost_microusd", "cost_basis"}
    schema_version = _string(payload, "schema_version")
    if schema_version == PROSPECTIVE_COLLECTION_USAGE_RECORD_SCHEMA:
        expected = base_fields | cost_fields
    elif schema_version == PROSPECTIVE_COLLECTION_USAGE_RECORD_SCHEMA_V1:
        expected = base_fields if "cost_basis" not in payload else base_fields | cost_fields
    else:
        raise ValueError("unsupported Collection Usage Record schema")
    if set(payload) != expected:
        raise ValueError("Collection Usage Record fields are not closed")

    def optional_integer(name: str) -> int | None:
        raw_value = payload.get(name)
        return None if raw_value is None else _integer(payload, name)

    error_kind = payload.get("error_kind")
    if error_kind is not None and not isinstance(error_kind, str):
        raise ValueError("Collection Usage Record error_kind must be text or null")
    record = CollectionUsageRecord(
        schema_version=schema_version,
        record_id=_string(payload, "record_id"),
        opportunity_id=_string(payload, "opportunity_id"),
        job_id=_string(payload, "job_id"),
        scheduled_for=_datetime(_string(payload, "scheduled_for"), "scheduled_for"),
        adapter_kind=ProspectiveCollectionAdapterKind(_string(payload, "adapter_kind")),
        outcome=_string(payload, "outcome"),
        collection_attempt_count=_integer(payload, "collection_attempt_count"),
        provider_attempt_count=optional_integer("provider_attempt_count"),
        request_count=optional_integer("request_count"),
        page_count=optional_integer("page_count"),
        response_bytes=optional_integer("response_bytes"),
        raw_artifact_bytes=optional_integer("raw_artifact_bytes"),
        received_records=optional_integer("received_records"),
        accepted_records=optional_integer("accepted_records"),
        latency_ms=_number(payload, "latency_ms"),
        incremental_cost_microusd=(
            optional_integer("incremental_cost_microusd")
            if "incremental_cost_microusd" in payload
            else None
        ),
        cost_basis=_string(payload, "cost_basis") if "cost_basis" in payload else None,
        error_kind=error_kind,
        recorded_at=_datetime(_string(payload, "recorded_at"), "recorded_at"),
        execution_capability=_boolean(payload, "execution_capability"),
    )
    if record.to_dict() != payload:
        raise ValueError("Collection Usage Record is not canonical")
    return record


def _opportunity_from_row(row: sqlite3.Row) -> CollectionOpportunity:
    return CollectionOpportunity(
        opportunity_id=cast(str, row["opportunity_id"]),
        job_id=cast(str, row["job_id"]),
        scheduled_for=_datetime(cast(str, row["scheduled_for"]), "scheduled_for"),
        outcome=cast(str, row["outcome"]),
        started_at=_datetime(cast(str, row["started_at"]), "started_at"),
        completed_at=_optional_datetime(cast(str | None, row["completed_at"]), "completed_at"),
        attempt_count=cast(int, row["attempt_count"]),
        data_snapshot_id=cast(str | None, row["data_snapshot_id"]),
        error_kind=cast(str | None, row["error_kind"]),
    )


def _opportunity_id(job_id: str, scheduled_for: datetime) -> str:
    identity = canonical_hash({"job_id": job_id, "scheduled_for": _timestamp(scheduled_for)})
    return f"collection-opportunity-{identity}"


def _backoff_seconds(policy: ProspectiveCollectionPolicy, failures: int) -> int:
    exponent = max(0, min(failures - 1, 5))
    return min(policy.maximum_gap_seconds, policy.poll_interval_seconds * (2**exponent))


def _run_result(
    job_id: str,
    outcome: str,
    *,
    scheduled_for: datetime | None = None,
    opportunity_id: str | None = None,
    data_snapshot_id: str | None = None,
    missed_opportunities: int = 0,
    error_kind: str | None = None,
    usage_record_id: str | None = None,
) -> CollectionRunResult:
    return CollectionRunResult(
        job_id=job_id,
        outcome=outcome,
        scheduled_for=scheduled_for,
        opportunity_id=opportunity_id,
        data_snapshot_id=data_snapshot_id,
        missed_opportunities=missed_opportunities,
        error_kind=error_kind,
        usage_record_id=usage_record_id,
    )


def _strict_utc(value: datetime, name: str) -> None:
    require_aware(value, name)
    if value.utcoffset() != timedelta(0):
        raise ValueError(f"{name} must be UTC")


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _timestamp(value: datetime) -> str:
    _strict_utc(value, "timestamp")
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _datetime(value: str, name: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{name} must be an ISO timestamp") from exc
    _strict_utc(parsed, name)
    return parsed


def _optional_datetime(value: str | None, name: str) -> datetime | None:
    return None if value is None else _datetime(value, name)


def _trimmed(value: str, name: str) -> None:
    if not value or value != value.strip():
        raise ValueError(f"{name} must be non-empty and trimmed")


def _sha256(value: str, name: str) -> None:
    if len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
        raise ValueError(f"{name} must be a SHA-256 digest")


def _string(payload: Mapping[str, object], name: str) -> str:
    value = payload.get(name)
    if not isinstance(value, str):
        raise ValueError(f"prospective collection job {name} must be a string")
    return value


def _integer(payload: Mapping[str, object], name: str) -> int:
    value = payload.get(name)
    if not isinstance(value, int) or isinstance(value, bool):
        raise ValueError(f"prospective collection job {name} must be an integer")
    return value


def _number(payload: Mapping[str, object], name: str) -> float:
    value = payload.get(name)
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        raise ValueError(f"prospective collection job {name} must be numeric")
    return float(value)


def _boolean(payload: Mapping[str, object], name: str) -> bool:
    value = payload.get(name)
    if not isinstance(value, bool):
        raise ValueError(f"prospective collection job {name} must be boolean")
    return value
