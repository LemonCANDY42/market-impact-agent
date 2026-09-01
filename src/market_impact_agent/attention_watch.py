from __future__ import annotations

import sqlite3
import uuid
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import cast

from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.data_inputs import DataSnapshot, LocalDataSnapshotStore
from market_impact_agent.domain import require_aware
from market_impact_agent.monitoring_scope import (
    MonitoringScope,
    RegisteredQueryTemplate,
    RetrievalPlan,
    assert_scope_aware_watch_admission,
    matched_scope_versions,
    monitoring_scope_from_dict,
    query_template_from_matcher_contract,
    retrieval_plan_from_dict,
)
from market_impact_agent.prospective_data import (
    ProspectiveCollectionPolicy,
    ProspectiveDataJournal,
    prospective_observation_version_id,
)

ATTENTION_WATCH_POLICY_SCHEMA = "market-impact.attention-watch-policy.v1"
ATTENTION_WATCH_POLICY_SCHEMA_V2 = "market-impact.attention-watch-policy.v2"
ATTENTION_WATCH_WAKE_SCHEMA = "market-impact.attention-watch-wake.v1"
ATTENTION_WATCH_TRIGGER = "new_observation_version"


class AttentionWatchStatus(StrEnum):
    ACTIVE = "active"
    BACKING_OFF = "backing_off"
    TRIGGERED = "triggered"
    EXPIRED = "expired"
    CANCELLED = "cancelled"

    @property
    def terminal(self) -> bool:
        return self in {
            AttentionWatchStatus.EXPIRED,
            AttentionWatchStatus.CANCELLED,
        }


@dataclass(frozen=True, slots=True)
class AttentionWatchPolicy:
    watch_id: str
    origin_ref: str
    event_cluster_key: str | None
    collection_policy_id: str
    initial_data_snapshot_id: str
    starts_at: datetime
    expires_at: datetime
    maximum_polls: int
    maximum_bytes: int
    maximum_wakes: int
    cooldown_seconds: int
    trigger_kind: str = ATTENTION_WATCH_TRIGGER
    schema_version: str = ATTENTION_WATCH_POLICY_SCHEMA
    monitoring_scope: MonitoringScope | None = None
    retrieval_plan: RetrievalPlan | None = None
    query_template: RegisteredQueryTemplate | None = None

    def __post_init__(self) -> None:
        if self.schema_version not in {
            ATTENTION_WATCH_POLICY_SCHEMA,
            ATTENTION_WATCH_POLICY_SCHEMA_V2,
        }:
            raise ValueError("unsupported Attention Watch policy schema")
        _trimmed(self.origin_ref, "Attention Watch origin_ref")
        if not self.collection_policy_id.startswith("prospective-collection-policy-"):
            raise ValueError("Attention Watch requires a prospective collection policy ID")
        if not self.initial_data_snapshot_id.startswith("data-snapshot-"):
            raise ValueError("Attention Watch requires an initial Data Snapshot ID")
        _strict_utc(self.starts_at, "Attention Watch starts_at")
        _strict_utc(self.expires_at, "Attention Watch expires_at")
        if self.starts_at >= self.expires_at:
            raise ValueError("Attention Watch starts_at must precede expires_at")
        for value, name in (
            (self.maximum_polls, "maximum_polls"),
            (self.maximum_bytes, "maximum_bytes"),
            (self.maximum_wakes, "maximum_wakes"),
            (self.cooldown_seconds, "cooldown_seconds"),
        ):
            if value < 0:
                raise ValueError(f"Attention Watch {name} must be non-negative")
        if self.trigger_kind != ATTENTION_WATCH_TRIGGER:
            raise ValueError(f"Attention Watch trigger_kind must be {ATTENTION_WATCH_TRIGGER}")
        if self.schema_version == ATTENTION_WATCH_POLICY_SCHEMA:
            if self.event_cluster_key is None:
                raise ValueError("Attention Watch v1 requires event_cluster_key")
            _trimmed(self.event_cluster_key, "Attention Watch event_cluster_key")
            if self.monitoring_scope is not None:
                raise ValueError("Attention Watch v1 cannot carry a Monitoring Scope")
            if self.retrieval_plan is not None or self.query_template is not None:
                raise ValueError("Attention Watch v1 cannot carry Retrieval Plan bindings")
        else:
            if self.event_cluster_key is not None:
                raise ValueError("Attention Watch v2 derives its subject from Monitoring Scope")
            if self.monitoring_scope is None:
                raise ValueError("Attention Watch v2 requires a Monitoring Scope")
            if self.retrieval_plan is None or self.query_template is None:
                raise ValueError("Attention Watch v2 requires Retrieval Plan and template bindings")
            if self.origin_ref not in self.monitoring_scope.origin_refs:
                raise ValueError("Attention Watch v2 origin_ref must be bound by Monitoring Scope")
            assert_scope_aware_watch_admission(
                self.monitoring_scope,
                collection_policy_id=self.collection_policy_id,
                retrieval_plan=self.retrieval_plan,
                query_template=self.query_template,
                maximum_polls=self.maximum_polls,
                maximum_bytes=self.maximum_bytes,
            )
        if self.watch_id != self.expected_watch_id:
            raise ValueError("Attention Watch watch_id does not match content")

    @property
    def expected_watch_id(self) -> str:
        return f"attention-watch-{canonical_hash(self.core_dict())}"

    def core_dict(self) -> dict[str, object]:
        result: dict[str, object] = {
            "schema_version": self.schema_version,
            "origin_ref": self.origin_ref,
            "collection_policy_id": self.collection_policy_id,
            "initial_data_snapshot_id": self.initial_data_snapshot_id,
            "starts_at": _timestamp(self.starts_at),
            "expires_at": _timestamp(self.expires_at),
            "maximum_polls": self.maximum_polls,
            "maximum_bytes": self.maximum_bytes,
            "maximum_wakes": self.maximum_wakes,
            "cooldown_seconds": self.cooldown_seconds,
            "trigger_kind": self.trigger_kind,
        }
        if self.event_cluster_key is not None:
            result["event_cluster_key"] = self.event_cluster_key
        if self.monitoring_scope is not None:
            result["monitoring_scope"] = self.monitoring_scope.to_dict()
        if self.retrieval_plan is not None:
            result["retrieval_plan"] = self.retrieval_plan.to_dict()
        if self.query_template is not None:
            result["template_matcher_contract"] = self.query_template.matcher_contract_dict()
        return result

    def to_dict(self) -> dict[str, object]:
        return {**self.core_dict(), "watch_id": self.watch_id}

    @classmethod
    def build(
        cls,
        *,
        origin_ref: str,
        event_cluster_key: str | None = None,
        collection_policy_id: str,
        initial_data_snapshot_id: str,
        starts_at: datetime,
        expires_at: datetime,
        maximum_polls: int,
        maximum_bytes: int,
        maximum_wakes: int,
        cooldown_seconds: int,
        monitoring_scope: MonitoringScope | None = None,
        retrieval_plan: RetrievalPlan | None = None,
        query_template: RegisteredQueryTemplate | None = None,
    ) -> AttentionWatchPolicy:
        schema_version = (
            ATTENTION_WATCH_POLICY_SCHEMA_V2
            if monitoring_scope is not None
            else ATTENTION_WATCH_POLICY_SCHEMA
        )
        core: dict[str, object] = {
            "schema_version": schema_version,
            "origin_ref": origin_ref,
            "collection_policy_id": collection_policy_id,
            "initial_data_snapshot_id": initial_data_snapshot_id,
            "starts_at": _timestamp(starts_at),
            "expires_at": _timestamp(expires_at),
            "maximum_polls": maximum_polls,
            "maximum_bytes": maximum_bytes,
            "maximum_wakes": maximum_wakes,
            "cooldown_seconds": cooldown_seconds,
            "trigger_kind": ATTENTION_WATCH_TRIGGER,
        }
        if event_cluster_key is not None:
            core["event_cluster_key"] = event_cluster_key
        if monitoring_scope is not None:
            core["monitoring_scope"] = monitoring_scope.to_dict()
        if retrieval_plan is not None:
            core["retrieval_plan"] = retrieval_plan.to_dict()
        if query_template is not None:
            core["template_matcher_contract"] = query_template.matcher_contract_dict()
        return cls(
            watch_id=f"attention-watch-{canonical_hash(core)}",
            origin_ref=origin_ref,
            event_cluster_key=event_cluster_key,
            collection_policy_id=collection_policy_id,
            initial_data_snapshot_id=initial_data_snapshot_id,
            starts_at=starts_at,
            expires_at=expires_at,
            maximum_polls=maximum_polls,
            maximum_bytes=maximum_bytes,
            maximum_wakes=maximum_wakes,
            cooldown_seconds=cooldown_seconds,
            schema_version=schema_version,
            monitoring_scope=monitoring_scope,
            retrieval_plan=retrieval_plan,
            query_template=query_template,
        )


@dataclass(frozen=True, slots=True)
class AttentionWake:
    wake_id: str
    watch_id: str
    trigger_kind: str
    data_snapshot_id: str
    prior_data_snapshot_id: str
    new_version_ids: tuple[str, ...]
    created_at: datetime
    execution_capability: bool = False
    schema_version: str = ATTENTION_WATCH_WAKE_SCHEMA

    def __post_init__(self) -> None:
        if self.schema_version != ATTENTION_WATCH_WAKE_SCHEMA:
            raise ValueError("unsupported Attention Watch wake schema")
        if not self.watch_id.startswith("attention-watch-"):
            raise ValueError("Attention Wake requires an Attention Watch ID")
        if self.trigger_kind != ATTENTION_WATCH_TRIGGER:
            raise ValueError("Attention Wake trigger kind is unsupported")
        for value in (self.data_snapshot_id, self.prior_data_snapshot_id):
            if not value.startswith("data-snapshot-"):
                raise ValueError("Attention Wake requires Data Snapshot lineage")
        if not self.new_version_ids or tuple(sorted(set(self.new_version_ids))) != (
            self.new_version_ids
        ):
            raise ValueError("Attention Wake new versions must be non-empty, unique, and sorted")
        if any(
            not item.startswith("prospective-observation-version-") for item in self.new_version_ids
        ):
            raise ValueError("Attention Wake version IDs are invalid")
        _strict_utc(self.created_at, "Attention Wake created_at")
        if self.execution_capability:
            raise ValueError("Attention Wake cannot grant execution capability")
        if self.wake_id != self.expected_wake_id:
            raise ValueError("Attention Wake wake_id does not match content")

    @property
    def expected_wake_id(self) -> str:
        return f"attention-wake-{canonical_hash(self.core_dict())}"

    def core_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "watch_id": self.watch_id,
            "trigger_kind": self.trigger_kind,
            "data_snapshot_id": self.data_snapshot_id,
            "prior_data_snapshot_id": self.prior_data_snapshot_id,
            "new_version_ids": list(self.new_version_ids),
            "created_at": _timestamp(self.created_at),
            "execution_capability": self.execution_capability,
        }

    def to_dict(self) -> dict[str, object]:
        return {**self.core_dict(), "wake_id": self.wake_id}

    @classmethod
    def build(
        cls,
        *,
        watch_id: str,
        data_snapshot_id: str,
        prior_data_snapshot_id: str,
        new_version_ids: tuple[str, ...],
        created_at: datetime,
    ) -> AttentionWake:
        versions = tuple(sorted(set(new_version_ids)))
        core = {
            "schema_version": ATTENTION_WATCH_WAKE_SCHEMA,
            "watch_id": watch_id,
            "trigger_kind": ATTENTION_WATCH_TRIGGER,
            "data_snapshot_id": data_snapshot_id,
            "prior_data_snapshot_id": prior_data_snapshot_id,
            "new_version_ids": list(versions),
            "created_at": _timestamp(created_at),
            "execution_capability": False,
        }
        return cls(
            wake_id=f"attention-wake-{canonical_hash(core)}",
            watch_id=watch_id,
            trigger_kind=ATTENTION_WATCH_TRIGGER,
            data_snapshot_id=data_snapshot_id,
            prior_data_snapshot_id=prior_data_snapshot_id,
            new_version_ids=versions,
            created_at=created_at,
        )


@dataclass(frozen=True, slots=True)
class AttentionWatchState:
    watch_id: str
    status: AttentionWatchStatus
    next_due_at: datetime
    wake_allowed_at: datetime
    poll_count: int
    byte_count: int
    wake_count: int
    last_data_snapshot_id: str
    last_error_kind: str | None
    updated_at: datetime


@dataclass(frozen=True, slots=True)
class AttentionWatchRunResult:
    watch_id: str
    outcome: str
    polled: bool
    collection_snapshot_id: str | None
    frozen_data_snapshot_id: str | None
    wake: AttentionWake | None
    error_kind: str | None

    def to_dict(self) -> dict[str, object]:
        return {
            "watch_id": self.watch_id,
            "outcome": self.outcome,
            "polled": self.polled,
            "collection_snapshot_id": self.collection_snapshot_id,
            "frozen_data_snapshot_id": self.frozen_data_snapshot_id,
            "wake": None if self.wake is None else self.wake.to_dict(),
            "error_kind": self.error_kind,
        }


WatchCollector = Callable[[ProspectiveCollectionPolicy], DataSnapshot]


class AttentionWatchService:
    """Durable read-only watch state and idempotent Agent wake-up outbox."""

    def __init__(
        self,
        store: LocalDataSnapshotStore,
        *,
        journal: ProspectiveDataJournal | None = None,
        lease_timeout_seconds: int = 300,
    ) -> None:
        if lease_timeout_seconds < 1:
            raise ValueError("Attention Watch lease_timeout_seconds must be positive")
        self.store = store
        self.journal = ProspectiveDataJournal(store) if journal is None else journal
        if self.journal.store.root != store.root:
            raise ValueError("Attention Watch journal must share the Data Snapshot store")
        self.index_path = store.index_path
        self.lease_timeout_seconds = lease_timeout_seconds
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
                CREATE TABLE IF NOT EXISTS attention_watch_policies (
                    watch_id TEXT PRIMARY KEY,
                    artifact_hash TEXT NOT NULL UNIQUE,
                    collection_policy_id TEXT NOT NULL
                        REFERENCES prospective_collection_policies(policy_id),
                    status TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    next_due_at TEXT NOT NULL,
                    wake_allowed_at TEXT NOT NULL,
                    poll_count INTEGER NOT NULL,
                    byte_count INTEGER NOT NULL,
                    wake_count INTEGER NOT NULL,
                    last_data_snapshot_id TEXT NOT NULL,
                    last_error_kind TEXT,
                    lease_token TEXT,
                    lease_expires_at TEXT
                );
                CREATE INDEX IF NOT EXISTS attention_watch_due
                    ON attention_watch_policies(status, next_due_at, watch_id);
                CREATE TABLE IF NOT EXISTS attention_watch_seen_versions (
                    watch_id TEXT NOT NULL REFERENCES attention_watch_policies(watch_id),
                    version_id TEXT NOT NULL
                        REFERENCES prospective_observation_versions(version_id),
                    PRIMARY KEY(watch_id, version_id)
                );
                CREATE TABLE IF NOT EXISTS attention_watch_outbox (
                    wake_id TEXT PRIMARY KEY,
                    watch_id TEXT NOT NULL REFERENCES attention_watch_policies(watch_id),
                    trigger_key TEXT NOT NULL UNIQUE,
                    artifact_hash TEXT NOT NULL UNIQUE,
                    data_snapshot_id TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    delivery_status TEXT NOT NULL,
                    delivered_at TEXT
                );
                CREATE INDEX IF NOT EXISTS attention_watch_pending_wakes
                    ON attention_watch_outbox(delivery_status, created_at, wake_id);
                """
            )
            columns = {
                cast(str, row["name"])
                for row in connection.execute(
                    "PRAGMA table_info(attention_watch_policies)"
                ).fetchall()
            }
            if "lease_token" not in columns:
                connection.execute(
                    "ALTER TABLE attention_watch_policies ADD COLUMN lease_token TEXT"
                )
            if "lease_expires_at" not in columns:
                connection.execute(
                    "ALTER TABLE attention_watch_policies ADD COLUMN lease_expires_at TEXT"
                )

    def create(
        self,
        policy: AttentionWatchPolicy,
        *,
        created_at: datetime,
    ) -> AttentionWatchState:
        _strict_utc(created_at, "Attention Watch created_at")
        if created_at >= policy.expires_at:
            raise ValueError("cannot create an already expired Attention Watch")
        collection_policy = self.journal.policy(policy.collection_policy_id)
        if policy.schema_version == ATTENTION_WATCH_POLICY_SCHEMA_V2:
            if (
                policy.monitoring_scope is None
                or policy.retrieval_plan is None
                or policy.query_template is None
            ):
                raise AssertionError("validated v2 Attention Watch bindings are missing")
            assert_scope_aware_watch_admission(
                policy.monitoring_scope,
                collection_policy_id=policy.collection_policy_id,
                retrieval_plan=policy.retrieval_plan,
                query_template=policy.query_template,
                maximum_polls=policy.maximum_polls,
                maximum_bytes=policy.maximum_bytes,
                collection_policy=collection_policy,
            )
        initial = self.store.get(policy.initial_data_snapshot_id)
        self.journal.assert_watch_baseline_snapshot(initial)
        if initial.query.source_policy_id != collection_policy.policy_id:
            raise ValueError("Attention Watch baseline does not match collection policy")
        baseline_cutoff = initial.query.parameters.get("requested_not_after")
        if not isinstance(baseline_cutoff, str):
            raise ValueError("Attention Watch baseline cutoff is missing")
        if _datetime(baseline_cutoff, "baseline cutoff") > created_at:
            raise ValueError("Attention Watch baseline cannot include future receipts")
        version_ids = self._scope_version_ids(policy, initial)
        artifact = self.store.artifacts.put_json(policy.to_dict())
        with self._connect() as connection:
            existing = connection.execute(
                "SELECT artifact_hash FROM attention_watch_policies WHERE watch_id = ?",
                (policy.watch_id,),
            ).fetchone()
            if existing is not None:
                if cast(str, existing["artifact_hash"]) != artifact.content_hash:
                    raise ValueError("Attention Watch identity has conflicting content")
                return self.state(policy.watch_id)
            connection.execute(
                """
                INSERT INTO attention_watch_policies(
                    watch_id, artifact_hash, collection_policy_id, status, created_at,
                    updated_at, next_due_at, wake_allowed_at, poll_count, byte_count, wake_count,
                    last_data_snapshot_id, last_error_kind
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, 0, 0, ?, NULL)
                """,
                (
                    policy.watch_id,
                    artifact.content_hash,
                    collection_policy.policy_id,
                    AttentionWatchStatus.ACTIVE.value,
                    _timestamp(created_at),
                    _timestamp(created_at),
                    _timestamp(policy.starts_at),
                    _timestamp(policy.starts_at),
                    initial.snapshot_id,
                ),
            )
            for version_id in version_ids:
                connection.execute(
                    """
                    INSERT INTO attention_watch_seen_versions(watch_id, version_id)
                    VALUES (?, ?)
                    """,
                    (policy.watch_id, version_id),
                )
        return self.state(policy.watch_id)

    def policy(self, watch_id: str) -> AttentionWatchPolicy:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT artifact_hash FROM attention_watch_policies WHERE watch_id = ?",
                (watch_id,),
            ).fetchone()
        if row is None:
            raise KeyError(f"unknown Attention Watch: {watch_id}")
        return attention_watch_policy_from_dict(
            self.store.artifacts.read_json(cast(str, row["artifact_hash"]))
        )

    def state(self, watch_id: str) -> AttentionWatchState:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM attention_watch_policies WHERE watch_id = ?",
                (watch_id,),
            ).fetchone()
        if row is None:
            raise KeyError(f"unknown Attention Watch: {watch_id}")
        return _state_from_row(row)

    def due_watch_ids(
        self,
        *,
        collection_policy_id: str,
        now: datetime,
        limit: int = 100,
    ) -> tuple[str, ...]:
        """Return due Watches that can reuse one completed collection opportunity."""

        _strict_utc(now, "Attention Watch due query time")
        if not collection_policy_id.startswith("prospective-collection-policy-"):
            raise ValueError("Attention Watch due query requires a Collection Policy ID")
        if limit < 1:
            raise ValueError("Attention Watch due query limit must be positive")
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT watch_id FROM attention_watch_policies
                WHERE collection_policy_id = ?
                  AND status IN (?, ?, ?)
                  AND next_due_at <= ?
                ORDER BY next_due_at, watch_id
                LIMIT ?
                """,
                (
                    collection_policy_id,
                    AttentionWatchStatus.ACTIVE.value,
                    AttentionWatchStatus.BACKING_OFF.value,
                    AttentionWatchStatus.TRIGGERED.value,
                    _timestamp(now),
                    limit,
                ),
            ).fetchall()
        return tuple(cast(str, row["watch_id"]) for row in rows)

    def run_due_from_snapshot(
        self,
        watch_id: str,
        *,
        now: datetime,
        collection_snapshot_id: str,
    ) -> AttentionWatchRunResult:
        """Evaluate one due Watch without issuing a second Provider request.

        The ordinary collection runtime already journaled the exact collection
        Snapshot. Reopening and passing that Snapshot through ``run_due`` keeps
        lease, budget, freeze, matcher, outbox, and replay behavior under the
        existing Watch owner while avoiding duplicate acquisition.
        """

        snapshot = self.store.get(collection_snapshot_id)
        policy = self.policy(watch_id)
        if snapshot.query.source_policy_id != policy.collection_policy_id:
            raise ValueError("shared collection Snapshot does not match Attention Watch policy")
        return self.run_due(
            watch_id,
            now=now,
            collector=lambda _: snapshot,
        )

    def run_due(
        self,
        watch_id: str,
        *,
        now: datetime,
        collector: WatchCollector,
    ) -> AttentionWatchRunResult:
        _strict_utc(now, "Attention Watch run time")
        policy = self.policy(watch_id)
        collection_policy = self.journal.policy(policy.collection_policy_id)
        lease_token, state, early_result = self._claim_due(
            watch_id,
            policy=policy,
            now=now,
        )
        if early_result is not None:
            return early_result
        if lease_token is None:
            raise RuntimeError("Attention Watch due claim is missing its lease")
        try:
            collection_snapshot = collector(collection_policy)
            append_result = self.journal.record_snapshot(
                collection_snapshot,
                policy=collection_policy,
            )
            del append_result
            response_bytes = self._response_bytes(collection_snapshot)
        except Exception as exc:
            error_kind = f"collector_{type(exc).__name__}"
            recorded = self._record_poll(
                watch_id,
                lease_token=lease_token,
                now=now,
                status=AttentionWatchStatus.BACKING_OFF,
                response_bytes=0,
                last_snapshot_id=state.last_data_snapshot_id,
                error_kind=error_kind,
                cooldown_seconds=collection_policy.poll_interval_seconds * 2,
            )
            if not recorded:
                return _run_result(
                    watch_id,
                    "stale_claim",
                    polled=True,
                    error_kind="watch_lease_lost",
                )
            return _run_result(
                watch_id,
                "collector_failure",
                polled=True,
                error_kind=error_kind,
            )

        poll_completed_at = max(now, collection_snapshot.completed_at)
        if poll_completed_at >= policy.expires_at:
            recorded = self._record_poll(
                watch_id,
                lease_token=lease_token,
                now=poll_completed_at,
                status=AttentionWatchStatus.EXPIRED,
                response_bytes=response_bytes,
                last_snapshot_id=state.last_data_snapshot_id,
                error_kind="watch_expired_during_collection",
                cooldown_seconds=0,
            )
            if not recorded:
                return _run_result(
                    watch_id,
                    "stale_claim",
                    polled=True,
                    collection_snapshot_id=collection_snapshot.snapshot_id,
                    error_kind="watch_lease_lost",
                )
            return _run_result(
                watch_id,
                "expired",
                polled=True,
                collection_snapshot_id=collection_snapshot.snapshot_id,
                error_kind="watch_expired_during_collection",
            )
        if collection_snapshot.query.as_of > collection_snapshot.completed_at:
            recorded = self._record_poll(
                watch_id,
                lease_token=lease_token,
                now=poll_completed_at,
                status=AttentionWatchStatus.BACKING_OFF,
                response_bytes=response_bytes,
                last_snapshot_id=state.last_data_snapshot_id,
                error_kind="watch_snapshot_future_cutoff",
                cooldown_seconds=collection_policy.poll_interval_seconds * 2,
            )
            if not recorded:
                return _run_result(
                    watch_id,
                    "stale_claim",
                    polled=True,
                    collection_snapshot_id=collection_snapshot.snapshot_id,
                    error_kind="watch_lease_lost",
                )
            return _run_result(
                watch_id,
                "snapshot_invalid",
                polled=True,
                collection_snapshot_id=collection_snapshot.snapshot_id,
                error_kind="watch_snapshot_future_cutoff",
            )

        if state.byte_count + response_bytes > policy.maximum_bytes:
            recorded = self._record_poll(
                watch_id,
                lease_token=lease_token,
                now=poll_completed_at,
                status=AttentionWatchStatus.EXPIRED,
                response_bytes=response_bytes,
                last_snapshot_id=state.last_data_snapshot_id,
                error_kind="watch_byte_budget_exhausted",
                cooldown_seconds=0,
            )
            if not recorded:
                return _run_result(
                    watch_id,
                    "stale_claim",
                    polled=True,
                    collection_snapshot_id=collection_snapshot.snapshot_id,
                    error_kind="watch_lease_lost",
                )
            return _run_result(
                watch_id,
                "budget_exhausted",
                polled=True,
                collection_snapshot_id=collection_snapshot.snapshot_id,
                error_kind="watch_byte_budget_exhausted",
            )
        if not collection_snapshot.coverage_complete:
            recorded = self._record_poll(
                watch_id,
                lease_token=lease_token,
                now=poll_completed_at,
                status=AttentionWatchStatus.BACKING_OFF,
                response_bytes=response_bytes,
                last_snapshot_id=state.last_data_snapshot_id,
                error_kind="watch_source_incomplete",
                cooldown_seconds=collection_policy.poll_interval_seconds * 2,
            )
            if not recorded:
                return _run_result(
                    watch_id,
                    "stale_claim",
                    polled=True,
                    collection_snapshot_id=collection_snapshot.snapshot_id,
                    error_kind="watch_lease_lost",
                )
            return _run_result(
                watch_id,
                "source_failure",
                polled=True,
                collection_snapshot_id=collection_snapshot.snapshot_id,
                error_kind="watch_source_incomplete",
            )

        initial = self.store.get(policy.initial_data_snapshot_id)
        baseline_window_start = initial.query.window_start
        if baseline_window_start is None:
            raise ValueError("Attention Watch baseline requires a bounded window")
        frozen = self.journal.freeze_snapshot(
            policy_id=collection_policy.policy_id,
            not_after=collection_snapshot.query.as_of,
            window_start=baseline_window_start,
            frozen_at=poll_completed_at,
        )
        if not frozen.coverage_complete:
            recorded = self._record_poll(
                watch_id,
                lease_token=lease_token,
                now=poll_completed_at,
                status=AttentionWatchStatus.BACKING_OFF,
                response_bytes=response_bytes,
                last_snapshot_id=state.last_data_snapshot_id,
                error_kind="watch_snapshot_incomplete",
                cooldown_seconds=collection_policy.poll_interval_seconds * 2,
            )
            if not recorded:
                return _run_result(
                    watch_id,
                    "stale_claim",
                    polled=True,
                    collection_snapshot_id=collection_snapshot.snapshot_id,
                    frozen_data_snapshot_id=frozen.snapshot_id,
                    error_kind="watch_lease_lost",
                )
            return _run_result(
                watch_id,
                "snapshot_incomplete",
                polled=True,
                collection_snapshot_id=collection_snapshot.snapshot_id,
                frozen_data_snapshot_id=frozen.snapshot_id,
                error_kind="watch_snapshot_incomplete",
            )

        version_ids = self._scope_version_ids(policy, frozen)
        seen = self._seen_version_ids(watch_id)
        new_version_ids = tuple(item for item in version_ids if item not in seen)
        if not new_version_ids:
            committed, _ = self._commit_success(
                watch_id,
                lease_token=lease_token,
                now=poll_completed_at,
                status=AttentionWatchStatus.ACTIVE,
                response_bytes=response_bytes,
                frozen=frozen,
                version_ids=version_ids,
                wake=None,
                trigger_key=None,
                next_delay_seconds=collection_policy.poll_interval_seconds,
            )
            if not committed:
                return _run_result(
                    watch_id,
                    "stale_claim",
                    polled=True,
                    collection_snapshot_id=collection_snapshot.snapshot_id,
                    frozen_data_snapshot_id=frozen.snapshot_id,
                    error_kind="watch_lease_lost",
                )
            return _run_result(
                watch_id,
                "no_change",
                polled=True,
                collection_snapshot_id=collection_snapshot.snapshot_id,
                frozen_data_snapshot_id=frozen.snapshot_id,
            )

        wake = AttentionWake.build(
            watch_id=watch_id,
            data_snapshot_id=frozen.snapshot_id,
            prior_data_snapshot_id=state.last_data_snapshot_id,
            new_version_ids=new_version_ids,
            created_at=poll_completed_at,
        )
        if poll_completed_at < state.wake_allowed_at:
            recorded = self._record_poll(
                watch_id,
                lease_token=lease_token,
                now=poll_completed_at,
                status=AttentionWatchStatus.TRIGGERED,
                response_bytes=response_bytes,
                last_snapshot_id=state.last_data_snapshot_id,
                error_kind=None,
                cooldown_seconds=collection_policy.poll_interval_seconds,
            )
            if not recorded:
                return _run_result(
                    watch_id,
                    "stale_claim",
                    polled=True,
                    collection_snapshot_id=collection_snapshot.snapshot_id,
                    frozen_data_snapshot_id=frozen.snapshot_id,
                    error_kind="watch_lease_lost",
                )
            return _run_result(
                watch_id,
                "cooldown",
                polled=True,
                collection_snapshot_id=collection_snapshot.snapshot_id,
                frozen_data_snapshot_id=frozen.snapshot_id,
            )
        trigger_key = canonical_hash(
            {
                "watch_id": watch_id,
                "trigger_kind": ATTENTION_WATCH_TRIGGER,
                "new_version_ids": list(new_version_ids),
            }
        )
        committed, enqueued = self._commit_success(
            watch_id,
            lease_token=lease_token,
            now=poll_completed_at,
            status=AttentionWatchStatus.TRIGGERED,
            response_bytes=response_bytes,
            frozen=frozen,
            version_ids=version_ids,
            wake=wake,
            trigger_key=trigger_key,
            next_delay_seconds=collection_policy.poll_interval_seconds,
            wake_cooldown_seconds=policy.cooldown_seconds,
        )
        if not committed:
            return _run_result(
                watch_id,
                "stale_claim",
                polled=True,
                collection_snapshot_id=collection_snapshot.snapshot_id,
                frozen_data_snapshot_id=frozen.snapshot_id,
                error_kind="watch_lease_lost",
            )
        return _run_result(
            watch_id,
            "triggered" if enqueued else "deduplicated",
            polled=True,
            collection_snapshot_id=collection_snapshot.snapshot_id,
            frozen_data_snapshot_id=frozen.snapshot_id,
            wake=wake if enqueued else None,
        )

    def cancel(self, watch_id: str, *, cancelled_at: datetime) -> AttentionWatchState:
        _strict_utc(cancelled_at, "Attention Watch cancelled_at")
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT status, updated_at FROM attention_watch_policies WHERE watch_id = ?",
                (watch_id,),
            ).fetchone()
            if row is None:
                raise KeyError(f"unknown Attention Watch: {watch_id}")
            status = AttentionWatchStatus(cast(str, row["status"]))
            if not status.terminal:
                if cancelled_at < _datetime(cast(str, row["updated_at"]), "updated_at"):
                    raise ValueError("Attention Watch cannot be cancelled before its latest update")
                connection.execute(
                    """
                    UPDATE attention_watch_policies
                    SET status = ?, updated_at = ?, next_due_at = ?, last_error_kind = NULL,
                        lease_token = NULL, lease_expires_at = NULL
                    WHERE watch_id = ?
                    """,
                    (
                        AttentionWatchStatus.CANCELLED.value,
                        _timestamp(cancelled_at),
                        _timestamp(cancelled_at),
                        watch_id,
                    ),
                )
        return self.state(watch_id)

    def pending_wakes(self, *, watch_id: str | None = None) -> tuple[AttentionWake, ...]:
        where = "WHERE delivery_status = 'pending'"
        parameters: tuple[object, ...] = ()
        if watch_id is not None:
            where += " AND watch_id = ?"
            parameters = (watch_id,)
        with self._connect() as connection:
            rows = tuple(
                connection.execute(
                    f"""
                    SELECT artifact_hash FROM attention_watch_outbox
                    {where}
                    ORDER BY created_at, wake_id
                    """,
                    parameters,
                ).fetchall()
            )
        return tuple(
            attention_wake_from_dict(
                self.store.artifacts.read_json(cast(str, row["artifact_hash"]))
            )
            for row in rows
        )

    def wake(self, wake_id: str) -> AttentionWake:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT artifact_hash FROM attention_watch_outbox WHERE wake_id = ?",
                (wake_id,),
            ).fetchone()
        if row is None:
            raise KeyError(f"unknown Attention Wake: {wake_id}")
        return attention_wake_from_dict(
            self.store.artifacts.read_json(cast(str, row["artifact_hash"]))
        )

    def mark_wake_delivered(self, wake_id: str, *, delivered_at: datetime) -> None:
        _strict_utc(delivered_at, "Attention Wake delivered_at")
        with self._connect() as connection:
            row = connection.execute(
                "SELECT created_at, delivery_status FROM attention_watch_outbox WHERE wake_id = ?",
                (wake_id,),
            ).fetchone()
            if row is None:
                raise KeyError(f"unknown Attention Wake: {wake_id}")
            if delivered_at < _datetime(cast(str, row["created_at"]), "wake created_at"):
                raise ValueError("Attention Wake cannot be delivered before creation")
            if cast(str, row["delivery_status"]) == "delivered":
                return
            connection.execute(
                """
                UPDATE attention_watch_outbox
                SET delivery_status = 'delivered', delivered_at = ?
                WHERE wake_id = ?
                """,
                (_timestamp(delivered_at), wake_id),
            )

    def _response_bytes(self, snapshot: DataSnapshot) -> int:
        total = 0
        for attempt in snapshot.attempts:
            if attempt.raw_response_hash is not None:
                total += self.store.artifacts.get(
                    attempt.raw_response_hash,
                    media_type="application/octet-stream",
                ).size_bytes
        return total

    def _seen_version_ids(self, watch_id: str) -> frozenset[str]:
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT version_id FROM attention_watch_seen_versions WHERE watch_id = ?",
                (watch_id,),
            ).fetchall()
        return frozenset(cast(str, row["version_id"]) for row in rows)

    @staticmethod
    def _scope_version_ids(
        policy: AttentionWatchPolicy,
        snapshot: DataSnapshot,
    ) -> tuple[str, ...]:
        if policy.monitoring_scope is not None:
            return matched_scope_versions(policy.monitoring_scope, snapshot)
        return tuple(
            sorted(prospective_observation_version_id(item) for item in snapshot.observations)
        )

    def _claim_due(
        self,
        watch_id: str,
        *,
        policy: AttentionWatchPolicy,
        now: datetime,
    ) -> tuple[str | None, AttentionWatchState, AttentionWatchRunResult | None]:
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT * FROM attention_watch_policies WHERE watch_id = ?",
                (watch_id,),
            ).fetchone()
            if row is None:
                raise KeyError(f"unknown Attention Watch: {watch_id}")
            state = _state_from_row(row)
            if state.status.terminal:
                return None, state, _run_result(watch_id, state.status.value)
            if now >= policy.expires_at:
                connection.execute(
                    """
                    UPDATE attention_watch_policies
                    SET status = ?, updated_at = ?, next_due_at = ?,
                        last_error_kind = NULL, lease_token = NULL, lease_expires_at = NULL
                    WHERE watch_id = ?
                    """,
                    (
                        AttentionWatchStatus.EXPIRED.value,
                        _timestamp(now),
                        _timestamp(now),
                        watch_id,
                    ),
                )
                return None, state, _run_result(watch_id, "expired")
            if (
                state.poll_count >= policy.maximum_polls
                or state.byte_count >= policy.maximum_bytes
                or state.wake_count >= policy.maximum_wakes
            ):
                connection.execute(
                    """
                    UPDATE attention_watch_policies
                    SET status = ?, updated_at = ?, next_due_at = ?,
                        last_error_kind = ?, lease_token = NULL, lease_expires_at = NULL
                    WHERE watch_id = ?
                    """,
                    (
                        AttentionWatchStatus.EXPIRED.value,
                        _timestamp(now),
                        _timestamp(now),
                        "watch_budget_exhausted",
                        watch_id,
                    ),
                )
                return (
                    None,
                    state,
                    _run_result(
                        watch_id,
                        "budget_exhausted",
                        error_kind="watch_budget_exhausted",
                    ),
                )
            current_lease = cast(str | None, row["lease_token"])
            lease_expires_at = cast(str | None, row["lease_expires_at"])
            if current_lease is not None:
                if lease_expires_at is None:
                    raise ValueError("Attention Watch lease expiry is missing")
                if now < _datetime(lease_expires_at, "lease_expires_at"):
                    return None, state, _run_result(watch_id, "in_progress")
            elif lease_expires_at is not None:
                raise ValueError("Attention Watch lease token is missing")
            if now < state.next_due_at:
                return None, state, _run_result(watch_id, "not_due")
            lease_token = uuid.uuid4().hex
            result = connection.execute(
                """
                UPDATE attention_watch_policies
                SET lease_token = ?, lease_expires_at = ?, updated_at = ?
                WHERE watch_id = ?
                """,
                (
                    lease_token,
                    _timestamp(now + timedelta(seconds=self.lease_timeout_seconds)),
                    _timestamp(now),
                    watch_id,
                ),
            )
            if result.rowcount != 1:
                raise RuntimeError("Attention Watch due claim was not persisted")
            return lease_token, state, None

    def _record_poll(
        self,
        watch_id: str,
        *,
        lease_token: str,
        now: datetime,
        status: AttentionWatchStatus,
        response_bytes: int,
        last_snapshot_id: str,
        error_kind: str | None,
        cooldown_seconds: int,
    ) -> bool:
        with self._connect() as connection:
            result = connection.execute(
                """
                UPDATE attention_watch_policies
                SET status = ?, updated_at = ?, next_due_at = ?,
                    poll_count = poll_count + 1, byte_count = byte_count + ?,
                    last_data_snapshot_id = ?, last_error_kind = ?,
                    lease_token = NULL, lease_expires_at = NULL
                WHERE watch_id = ? AND lease_token = ?
                """,
                (
                    status.value,
                    _timestamp(now),
                    _timestamp(now + timedelta(seconds=cooldown_seconds)),
                    response_bytes,
                    last_snapshot_id,
                    error_kind,
                    watch_id,
                    lease_token,
                ),
            )
        return result.rowcount == 1

    def _commit_success(
        self,
        watch_id: str,
        *,
        lease_token: str,
        now: datetime,
        status: AttentionWatchStatus,
        response_bytes: int,
        frozen: DataSnapshot,
        version_ids: tuple[str, ...],
        wake: AttentionWake | None,
        trigger_key: str | None,
        next_delay_seconds: int,
        wake_cooldown_seconds: int | None = None,
    ) -> tuple[bool, bool]:
        wake_inserted = False
        wake_artifact_hash: str | None = None
        if wake is not None:
            wake_artifact_hash = self.store.artifacts.put_json(wake.to_dict()).content_hash
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            owner = connection.execute(
                "SELECT lease_token FROM attention_watch_policies WHERE watch_id = ?",
                (watch_id,),
            ).fetchone()
            if owner is None:
                raise KeyError(f"unknown Attention Watch: {watch_id}")
            if cast(str | None, owner["lease_token"]) != lease_token:
                return False, False
            for version_id in version_ids:
                connection.execute(
                    """
                    INSERT OR IGNORE INTO attention_watch_seen_versions(watch_id, version_id)
                    VALUES (?, ?)
                    """,
                    (watch_id, version_id),
                )
            if wake is not None and trigger_key is not None and wake_artifact_hash is not None:
                existing = connection.execute(
                    "SELECT wake_id FROM attention_watch_outbox WHERE trigger_key = ?",
                    (trigger_key,),
                ).fetchone()
                if existing is None:
                    connection.execute(
                        """
                        INSERT INTO attention_watch_outbox(
                            wake_id, watch_id, trigger_key, artifact_hash, data_snapshot_id,
                            created_at, delivery_status, delivered_at
                        ) VALUES (?, ?, ?, ?, ?, ?, 'pending', NULL)
                        """,
                        (
                            wake.wake_id,
                            watch_id,
                            trigger_key,
                            wake_artifact_hash,
                            frozen.snapshot_id,
                            _timestamp(wake.created_at),
                        ),
                    )
                    wake_inserted = True
            connection.execute(
                """
                UPDATE attention_watch_policies
                SET status = ?, updated_at = ?, next_due_at = ?,
                    poll_count = poll_count + 1, byte_count = byte_count + ?,
                    wake_count = wake_count + ?, last_data_snapshot_id = ?,
                    wake_allowed_at = COALESCE(?, wake_allowed_at), last_error_kind = NULL,
                    lease_token = NULL, lease_expires_at = NULL
                WHERE watch_id = ? AND lease_token = ?
                """,
                (
                    status.value,
                    _timestamp(now),
                    _timestamp(now + timedelta(seconds=next_delay_seconds)),
                    response_bytes,
                    int(wake_inserted),
                    frozen.snapshot_id,
                    (
                        None
                        if wake_cooldown_seconds is None
                        else _timestamp(now + timedelta(seconds=wake_cooldown_seconds))
                    ),
                    watch_id,
                    lease_token,
                ),
            )
        return True, wake_inserted


def attention_watch_policy_from_dict(value: object) -> AttentionWatchPolicy:
    payload = _object(value, "Attention Watch policy")
    monitoring_scope_value = payload.get("monitoring_scope")
    monitoring_scope = (
        None
        if monitoring_scope_value is None
        else monitoring_scope_from_dict(monitoring_scope_value)
    )
    retrieval_plan_value = payload.get("retrieval_plan")
    retrieval_plan = (
        None if retrieval_plan_value is None else retrieval_plan_from_dict(retrieval_plan_value)
    )
    matcher_contract_value = payload.get("template_matcher_contract")
    query_template = None
    if matcher_contract_value is not None:
        if retrieval_plan is None:
            raise ValueError("Attention Watch matcher contract requires Retrieval Plan")
        query_template = query_template_from_matcher_contract(
            matcher_contract_value,
            template_ref=retrieval_plan.query_template_ref,
            capability=retrieval_plan.capability,
            pit_lane=retrieval_plan.pit_lane,
        )
    return AttentionWatchPolicy(
        watch_id=_string(payload, "watch_id"),
        origin_ref=_string(payload, "origin_ref"),
        event_cluster_key=(
            None
            if payload.get("event_cluster_key") is None
            else _string(payload, "event_cluster_key")
        ),
        collection_policy_id=_string(payload, "collection_policy_id"),
        initial_data_snapshot_id=_string(payload, "initial_data_snapshot_id"),
        starts_at=_datetime(_string(payload, "starts_at"), "starts_at"),
        expires_at=_datetime(_string(payload, "expires_at"), "expires_at"),
        maximum_polls=_integer(payload, "maximum_polls"),
        maximum_bytes=_integer(payload, "maximum_bytes"),
        maximum_wakes=_integer(payload, "maximum_wakes"),
        cooldown_seconds=_integer(payload, "cooldown_seconds"),
        trigger_kind=_string(payload, "trigger_kind"),
        schema_version=_string(payload, "schema_version"),
        monitoring_scope=monitoring_scope,
        retrieval_plan=retrieval_plan,
        query_template=query_template,
    )


def attention_wake_from_dict(value: object) -> AttentionWake:
    payload = _object(value, "Attention Wake")
    return AttentionWake(
        wake_id=_string(payload, "wake_id"),
        watch_id=_string(payload, "watch_id"),
        trigger_kind=_string(payload, "trigger_kind"),
        data_snapshot_id=_string(payload, "data_snapshot_id"),
        prior_data_snapshot_id=_string(payload, "prior_data_snapshot_id"),
        new_version_ids=tuple(
            _string_value(item, "new_version_id")
            for item in _list(payload.get("new_version_ids"), "new_version_ids")
        ),
        created_at=_datetime(_string(payload, "created_at"), "created_at"),
        execution_capability=_boolean(payload, "execution_capability"),
        schema_version=_string(payload, "schema_version"),
    )


def _state_from_row(row: sqlite3.Row) -> AttentionWatchState:
    return AttentionWatchState(
        watch_id=cast(str, row["watch_id"]),
        status=AttentionWatchStatus(cast(str, row["status"])),
        next_due_at=_datetime(cast(str, row["next_due_at"]), "next_due_at"),
        wake_allowed_at=_datetime(cast(str, row["wake_allowed_at"]), "wake_allowed_at"),
        poll_count=cast(int, row["poll_count"]),
        byte_count=cast(int, row["byte_count"]),
        wake_count=cast(int, row["wake_count"]),
        last_data_snapshot_id=cast(str, row["last_data_snapshot_id"]),
        last_error_kind=cast(str | None, row["last_error_kind"]),
        updated_at=_datetime(cast(str, row["updated_at"]), "updated_at"),
    )


def _run_result(
    watch_id: str,
    outcome: str,
    *,
    polled: bool = False,
    collection_snapshot_id: str | None = None,
    frozen_data_snapshot_id: str | None = None,
    wake: AttentionWake | None = None,
    error_kind: str | None = None,
) -> AttentionWatchRunResult:
    return AttentionWatchRunResult(
        watch_id=watch_id,
        outcome=outcome,
        polled=polled,
        collection_snapshot_id=collection_snapshot_id,
        frozen_data_snapshot_id=frozen_data_snapshot_id,
        wake=wake,
        error_kind=error_kind,
    )


def _object(value: object, name: str) -> dict[str, object]:
    if not isinstance(value, Mapping):
        raise TypeError(f"{name} must be an object")
    raw = cast(Mapping[object, object], value)
    if any(not isinstance(key, str) for key in raw):
        raise TypeError(f"{name} keys must be strings")
    return cast(dict[str, object], dict(raw))


def _list(value: object, name: str) -> list[object]:
    if not isinstance(value, list):
        raise TypeError(f"{name} must be an array")
    return cast(list[object], value)


def _string(value: Mapping[str, object], key: str) -> str:
    raw = value.get(key)
    if not isinstance(raw, str):
        raise TypeError(f"{key} must be a string")
    return raw


def _string_value(value: object, name: str) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{name} must be a string")
    return value


def _integer(value: Mapping[str, object], key: str) -> int:
    raw = value.get(key)
    if isinstance(raw, bool) or not isinstance(raw, int):
        raise TypeError(f"{key} must be an integer")
    return raw


def _boolean(value: Mapping[str, object], key: str) -> bool:
    raw = value.get(key)
    if not isinstance(raw, bool):
        raise TypeError(f"{key} must be a boolean")
    return raw


def _trimmed(value: str, name: str) -> None:
    if not value or value != value.strip():
        raise ValueError(f"{name} must be a non-empty trimmed string")


def _strict_utc(value: datetime, name: str) -> None:
    require_aware(value, name)
    if value.utcoffset() != timedelta(0) or value.tzinfo is not UTC:
        raise ValueError(f"{name} must use the UTC singleton")


def _timestamp(value: datetime) -> str:
    _strict_utc(value, "timestamp")
    return value.isoformat().replace("+00:00", "Z")


def _datetime(value: str, name: str) -> datetime:
    if not value.endswith("Z"):
        raise ValueError(f"{name} must be a UTC timestamp")
    try:
        parsed = datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError as exc:
        raise ValueError(f"{name} must be an ISO 8601 timestamp") from exc
    return parsed.astimezone(UTC)
