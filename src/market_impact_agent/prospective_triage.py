from __future__ import annotations

import asyncio
import os
import sqlite3
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Protocol, cast

from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.agent_runtime import ModelProvider, ModelTurn, SkillRegistry
from market_impact_agent.data_inputs import DataSnapshot, LocalDataSnapshotStore
from market_impact_agent.event_impact_triage import (
    EventImpactTriageBatchSelection,
    EventImpactTriageCandidateSet,
    event_impact_triage_batch_selection_from_dict,
    event_impact_triage_candidate_set_from_dict,
    freeze_event_impact_triage_candidate_set,
)
from market_impact_agent.event_impact_triage_evaluation import (
    EventImpactTriageLabelSet,
    TriageLabelExposure,
)
from market_impact_agent.event_impact_triage_runtime import (
    SnapshotTriageCandidateContentResolver,
    TriageComparisonArm,
)
from market_impact_agent.event_impact_triage_store import (
    EventImpactTriageDecisionStore,
    EventImpactTriageTerminalBatch,
)
from market_impact_agent.event_impact_triage_work import (
    EventImpactTriageWorkManifest,
    TriageWorkManifestPolicy,
    build_event_impact_triage_work_manifest,
    event_impact_triage_work_manifest_from_dict,
)
from market_impact_agent.event_impact_triage_work_evaluation import (
    EventImpactTriageWorkComparisonStore,
    TriageWorkArmOutcome,
    evaluate_event_impact_triage_work_comparison,
)
from market_impact_agent.event_impact_triage_work_format_recovery import (
    EventImpactTriageWorkFormatRecoveryStore,
)
from market_impact_agent.event_impact_triage_work_runtime import (
    EVENT_IMPACT_TRIAGE_WORK_EXECUTION_PLAN_SCHEMA_V9,
    EVENT_IMPACT_TRIAGE_WORK_EXECUTION_PLAN_SCHEMA_V10,
    EVENT_IMPACT_TRIAGE_WORK_EXECUTION_PLAN_SCHEMA_V11,
    EventImpactTriageWorkDecisionAuthority,
    EventImpactTriageWorkExecutionPlan,
    EventImpactTriageWorkRunner,
    EventImpactTriageWorkRunResult,
    build_event_impact_triage_work_execution_plan_v8,
    build_event_impact_triage_work_execution_plan_v9,
    build_event_impact_triage_work_execution_plan_v10,
    build_event_impact_triage_work_execution_plan_v11,
    event_impact_triage_work_execution_plan_from_dict,
)
from market_impact_agent.model_provider import (
    ModelProviderFactory,
    ModelProviderProfile,
    load_builtin_model_provider_profile,
)
from market_impact_agent.prospective_checkpoint_readiness import (
    ProspectiveCheckpointAdmissionStore,
    ProspectiveCheckpointRoutePlan,
    evaluate_prospective_checkpoint_readiness,
)
from market_impact_agent.prospective_collection_runtime import ProspectiveCollectionRuntime
from market_impact_agent.prospective_data import ProspectiveDataJournal
from market_impact_agent.prospective_diagnostic import ProspectiveDiagnosticRegistration
from market_impact_agent.provider_reliability import ProviderHealthStore
from market_impact_agent.runtime_store import ArtifactStore, RunJournal, RunStatus
from market_impact_agent.usage_ledger import UsageLedger


class _AvailabilityModelProvider(ModelProvider, Protocol):
    async def assert_model_available(self, *, timeout_seconds: float) -> None: ...


class _NoCallModelProvider:
    def __init__(self, *, provider_id: str, model: str) -> None:
        self._provider_id = provider_id
        self._model = model

    @property
    def provider_id(self) -> str:
        return self._provider_id

    @property
    def model(self) -> str:
        return self._model

    async def assert_model_available(self, *, timeout_seconds: float) -> None:
        _ = timeout_seconds

    async def complete(
        self,
        *,
        messages: tuple[dict[str, object], ...],
        tools: tuple[dict[str, object], ...],
        temperature: float,
        top_p: float,
        max_output_tokens: int,
        timeout_seconds: float,
    ) -> ModelTurn:
        _ = (messages, tools, temperature, top_p, max_output_tokens, timeout_seconds)
        raise AssertionError("format recovery cannot call a Model Provider")


class _LazyAvailabilityModelProvider:
    """Resolve and probe a Provider only when a Work member needs a model call."""

    def __init__(
        self,
        *,
        profile: ModelProviderProfile,
        provider: ModelProvider | None,
    ) -> None:
        self._profile = profile
        self._provider = provider
        self._available = False

    @property
    def provider_id(self) -> str:
        return self._profile.provider_id

    @property
    def model(self) -> str:
        return self._profile.model

    def _resolve(self) -> ModelProvider:
        provider = self._provider
        if provider is None:
            provider = ModelProviderFactory.with_builtin_adapters().create(self._profile)
            self._provider = provider
        if provider.provider_id != self.provider_id or provider.model != self.model:
            raise ValueError("prospective triage Provider differs from the frozen profile")
        return provider

    async def _assert_available(self, provider: ModelProvider) -> None:
        if self._available:
            return
        await cast(_AvailabilityModelProvider, provider).assert_model_available(timeout_seconds=30)
        self._available = True

    async def prepare_for_model_call(self) -> None:
        """Resolve and probe immediately before the Runner records a dispatch."""

        await self._assert_available(self._resolve())

    def __getattr__(self, name: str) -> object:
        if name != "complete_with_observer":
            raise AttributeError(name)
        provider = self._resolve()
        provider_complete = getattr(provider, name)

        async def complete_with_observer(**kwargs: object) -> ModelTurn:
            await self._assert_available(provider)
            return cast(ModelTurn, await provider_complete(**kwargs))

        return complete_with_observer

    async def complete(
        self,
        *,
        messages: tuple[dict[str, object], ...],
        tools: tuple[dict[str, object], ...],
        temperature: float,
        top_p: float,
        max_output_tokens: int,
        timeout_seconds: float,
    ) -> ModelTurn:
        provider = self._resolve()
        await self._assert_available(provider)
        return await provider.complete(
            messages=messages,
            tools=tools,
            temperature=temperature,
            top_p=top_p,
            max_output_tokens=max_output_tokens,
            timeout_seconds=timeout_seconds,
        )


@dataclass(frozen=True, slots=True)
class PreparedProspectiveTriageWork:
    active_batch_id: str
    readiness_report_id: str
    unclassified_candidate_count: int
    selection: EventImpactTriageBatchSelection
    snapshot: DataSnapshot
    candidate_set: EventImpactTriageCandidateSet
    manifest: EventImpactTriageWorkManifest
    plan: EventImpactTriageWorkExecutionPlan
    profile: ModelProviderProfile
    protocol_artifact_hashes: dict[str, str]

    def summary(self) -> dict[str, object]:
        return {
            "active_batch_id": self.active_batch_id,
            "readiness_report_id": self.readiness_report_id,
            "selection_id": self.selection.selection_id,
            "unclassified_candidate_count": self.unclassified_candidate_count,
            "selected_candidate_count": len(self.selection.selected_version_ids),
            "data_snapshot_id": self.snapshot.snapshot_id,
            "candidate_set_id": self.candidate_set.candidate_set_id,
            "manifest_id": self.manifest.manifest_id,
            "work_unit_count": len(self.manifest.work_units),
            "plan_id": self.plan.plan_id,
            "maximum_provider_runs": self.plan.max_total_runs,
            "maximum_estimated_cost_microusd": self.plan.max_total_estimated_cost_microusd,
            "profile_id": self.profile.profile_id,
            "protocol_artifact_hashes": dict(self.protocol_artifact_hashes),
            "labels_present": False,
            "historical_pit_claim": False,
            "judgment_or_execution_authority": False,
        }


_ACTIVE_BATCH_SCHEMA = "market-impact.prospective-triage-active-batch.v1"


@dataclass(frozen=True, slots=True)
class ProspectiveTriageActiveBatchRecord:
    batch_id: str
    registration_id: str
    checkpoint_key: str
    route_plan_id: str
    route_admission_id: str
    readiness_report_id: str
    unclassified_candidate_count: int
    data_snapshot_id: str
    profile_id: str
    protocol_artifact_hashes: dict[str, str]
    created_at: datetime

    def core_dict(self) -> dict[str, object]:
        return {
            "schema_version": _ACTIVE_BATCH_SCHEMA,
            "registration_id": self.registration_id,
            "checkpoint_key": self.checkpoint_key,
            "route_plan_id": self.route_plan_id,
            "route_admission_id": self.route_admission_id,
            "readiness_report_id": self.readiness_report_id,
            "unclassified_candidate_count": self.unclassified_candidate_count,
            "data_snapshot_id": self.data_snapshot_id,
            "profile_id": self.profile_id,
            "protocol_artifact_hashes": dict(self.protocol_artifact_hashes),
            "created_at": self.created_at.isoformat().replace("+00:00", "Z"),
        }

    def to_dict(self) -> dict[str, object]:
        return {**self.core_dict(), "batch_id": self.batch_id}


class ProspectiveTriageActiveBatchStore:
    """One durable active Work graph per admitted route/checkpoint epoch."""

    def __init__(self, run_root: Path) -> None:
        run_root.mkdir(mode=0o700, parents=True, exist_ok=True)
        self.path = run_root / "active-batches.sqlite3"
        self.artifacts = ArtifactStore(run_root / "protocol-artifacts")
        with self._connect() as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS prospective_triage_batches (
                    batch_id TEXT PRIMARY KEY,
                    route_epoch_key TEXT NOT NULL,
                    registration_id TEXT NOT NULL,
                    checkpoint_key TEXT NOT NULL,
                    route_plan_id TEXT NOT NULL,
                    route_admission_id TEXT NOT NULL,
                    artifact_hash TEXT NOT NULL UNIQUE,
                    created_at TEXT NOT NULL
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS prospective_triage_active_heads (
                    route_epoch_key TEXT PRIMARY KEY,
                    batch_id TEXT NOT NULL UNIQUE
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS prospective_triage_completions (
                    batch_id TEXT PRIMARY KEY,
                    decision_id TEXT NOT NULL UNIQUE,
                    completed_at TEXT NOT NULL
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS prospective_triage_epoch_revisions (
                    route_epoch_key TEXT PRIMARY KEY,
                    revision INTEGER NOT NULL CHECK(revision >= 0)
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS prospective_triage_terminalizations (
                    batch_id TEXT PRIMARY KEY,
                    terminal_id TEXT NOT NULL UNIQUE,
                    terminalized_at TEXT NOT NULL
                )
                """
            )

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA journal_mode = WAL")
        connection.execute("PRAGMA synchronous = FULL")
        connection.execute("PRAGMA busy_timeout = 5000")
        return connection

    @staticmethod
    def route_epoch_key(
        *,
        registration_id: str,
        checkpoint_key: str,
        route_plan_id: str,
        route_admission_id: str,
    ) -> str:
        return canonical_hash(
            {
                "registration_id": registration_id,
                "checkpoint_key": checkpoint_key,
                "route_plan_id": route_plan_id,
                "route_admission_id": route_admission_id,
            }
        )

    def active(
        self,
        *,
        registration_id: str,
        checkpoint_key: str,
        route_plan_id: str,
        route_admission_id: str,
    ) -> ProspectiveTriageActiveBatchRecord | None:
        epoch = self.route_epoch_key(
            registration_id=registration_id,
            checkpoint_key=checkpoint_key,
            route_plan_id=route_plan_id,
            route_admission_id=route_admission_id,
        )
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT batches.artifact_hash
                FROM prospective_triage_active_heads AS heads
                JOIN prospective_triage_batches AS batches USING(batch_id)
                WHERE heads.route_epoch_key = ?
                """,
                (epoch,),
            ).fetchone()
        if row is None:
            return None
        return _active_batch_record_from_dict(
            self.artifacts.read_json(cast(str, row["artifact_hash"]))
        )

    def epoch_revision(
        self,
        *,
        registration_id: str,
        checkpoint_key: str,
        route_plan_id: str,
        route_admission_id: str,
    ) -> int:
        epoch = self.route_epoch_key(
            registration_id=registration_id,
            checkpoint_key=checkpoint_key,
            route_plan_id=route_plan_id,
            route_admission_id=route_admission_id,
        )
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT revision FROM prospective_triage_epoch_revisions
                WHERE route_epoch_key = ?
                """,
                (epoch,),
            ).fetchone()
        return 0 if row is None else cast(int, row["revision"])

    def install(
        self,
        prepared: PreparedProspectiveTriageWork,
        *,
        expected_epoch_revision: int,
    ) -> ProspectiveTriageActiveBatchRecord:
        return self._install_record(
            _active_batch_record(prepared),
            expected_epoch_revision=expected_epoch_revision,
        )

    def _install_record(
        self,
        record: ProspectiveTriageActiveBatchRecord,
        *,
        expected_epoch_revision: int,
    ) -> ProspectiveTriageActiveBatchRecord:
        if expected_epoch_revision < 0:
            raise ValueError("prospective Triage epoch revision cannot be negative")
        record = _active_batch_record_from_dict(record.to_dict())
        artifact = self.artifacts.put_json(record.to_dict())
        epoch = self.route_epoch_key(
            registration_id=record.registration_id,
            checkpoint_key=record.checkpoint_key,
            route_plan_id=record.route_plan_id,
            route_admission_id=record.route_admission_id,
        )
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            existing = connection.execute(
                """
                SELECT batches.artifact_hash
                FROM prospective_triage_active_heads AS heads
                JOIN prospective_triage_batches AS batches USING(batch_id)
                WHERE heads.route_epoch_key = ?
                """,
                (epoch,),
            ).fetchone()
            if existing is not None:
                return _active_batch_record_from_dict(
                    self.artifacts.read_json(cast(str, existing["artifact_hash"]))
                )
            revision_row = connection.execute(
                """
                SELECT revision FROM prospective_triage_epoch_revisions
                WHERE route_epoch_key = ?
                """,
                (epoch,),
            ).fetchone()
            current_revision = 0 if revision_row is None else cast(int, revision_row["revision"])
            if current_revision != expected_epoch_revision:
                raise ValueError("prospective Triage epoch advanced while batch was prepared")
            completed = connection.execute(
                """
                SELECT decision_id FROM prospective_triage_completions
                WHERE batch_id = ?
                """,
                (record.batch_id,),
            ).fetchone()
            if completed is not None:
                raise ValueError("completed prospective Triage batch cannot become active")
            terminalized = connection.execute(
                """
                SELECT terminal_id FROM prospective_triage_terminalizations
                WHERE batch_id = ?
                """,
                (record.batch_id,),
            ).fetchone()
            if terminalized is not None:
                raise ValueError("terminalized prospective Triage batch cannot become active")
            connection.execute(
                """
                INSERT OR IGNORE INTO prospective_triage_batches(
                    batch_id, route_epoch_key, registration_id, checkpoint_key,
                    route_plan_id, route_admission_id, artifact_hash, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    record.batch_id,
                    epoch,
                    record.registration_id,
                    record.checkpoint_key,
                    record.route_plan_id,
                    record.route_admission_id,
                    artifact.content_hash,
                    record.created_at.isoformat().replace("+00:00", "Z"),
                ),
            )
            connection.execute(
                """
                INSERT INTO prospective_triage_active_heads(route_epoch_key, batch_id)
                VALUES (?, ?)
                """,
                (epoch, record.batch_id),
            )
        return record

    def complete(
        self,
        *,
        batch_id: str,
        candidate_set: EventImpactTriageCandidateSet,
        state_root: Path,
    ) -> None:
        with self._connect() as connection:
            record_row = connection.execute(
                """
                SELECT artifact_hash FROM prospective_triage_batches
                WHERE batch_id = ?
                """,
                (batch_id,),
            ).fetchone()
        if record_row is None:
            raise KeyError(f"unknown prospective Triage batch: {batch_id}")
        record = _active_batch_record_from_dict(
            self.artifacts.read_json(cast(str, record_row["artifact_hash"]))
        )
        if (
            canonical_hash(candidate_set.to_dict())
            != record.protocol_artifact_hashes["candidate_set"]
            or candidate_set.registration_id != record.registration_id
            or candidate_set.checkpoint_key != record.checkpoint_key
            or candidate_set.route_plan_id != record.route_plan_id
            or candidate_set.route_admission_id != record.route_admission_id
            or candidate_set.readiness_report_id != record.readiness_report_id
        ):
            raise ValueError("prospective Triage completion Candidate Set is not the active batch")
        stored_candidate, _, decision = EventImpactTriageDecisionStore(state_root).get_context(
            candidate_set.candidate_set_id
        )
        if stored_candidate != candidate_set:
            raise ValueError("prospective Triage completion authority Candidate Set differs")
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                """
                SELECT route_epoch_key, artifact_hash FROM prospective_triage_batches
                WHERE batch_id = ?
                """,
                (batch_id,),
            ).fetchone()
            if row is None:
                raise KeyError(f"unknown prospective Triage batch: {batch_id}")
            if cast(str, row["artifact_hash"]) != cast(str, record_row["artifact_hash"]):
                raise ValueError("prospective Triage batch record changed before completion")
            epoch = cast(str, row["route_epoch_key"])
            existing = connection.execute(
                """
                SELECT decision_id FROM prospective_triage_completions
                WHERE batch_id = ?
                """,
                (batch_id,),
            ).fetchone()
            if existing is not None:
                if cast(str, existing["decision_id"]) != decision.decision_id:
                    raise ValueError("prospective Triage active batch completion is inconsistent")
                return
            head = connection.execute(
                """
                SELECT batch_id FROM prospective_triage_active_heads
                WHERE route_epoch_key = ?
                """,
                (epoch,),
            ).fetchone()
            if head is None:
                raise ValueError("prospective Triage active batch has no completion authority")
            if cast(str, head["batch_id"]) != batch_id:
                raise ValueError("prospective Triage active head changed before completion")
            connection.execute(
                """
                INSERT INTO prospective_triage_completions(batch_id, decision_id, completed_at)
                VALUES (?, ?, ?)
                """,
                (
                    batch_id,
                    decision.decision_id,
                    decision.decided_at.isoformat().replace("+00:00", "Z"),
                ),
            )
            connection.execute(
                """
                INSERT INTO prospective_triage_epoch_revisions(route_epoch_key, revision)
                VALUES (?, 1)
                ON CONFLICT(route_epoch_key) DO UPDATE SET revision = revision + 1
                """,
                (epoch,),
            )
            connection.execute(
                """
                DELETE FROM prospective_triage_active_heads
                WHERE route_epoch_key = ? AND batch_id = ?
                """,
                (epoch, batch_id),
            )

    def terminalize(
        self,
        *,
        batch_id: str,
        candidate_set: EventImpactTriageCandidateSet,
        terminal: EventImpactTriageTerminalBatch,
        state_root: Path,
    ) -> None:
        """Release one failed head only after its state-root terminal authority exists."""

        with self._connect() as connection:
            record_row = connection.execute(
                """
                SELECT route_epoch_key, artifact_hash FROM prospective_triage_batches
                WHERE batch_id = ?
                """,
                (batch_id,),
            ).fetchone()
        if record_row is None:
            raise KeyError(f"unknown prospective Triage batch: {batch_id}")
        record = _active_batch_record_from_dict(
            self.artifacts.read_json(cast(str, record_row["artifact_hash"]))
        )
        authoritative = EventImpactTriageDecisionStore(state_root).terminal_batch(
            candidate_set.candidate_set_id
        )
        if authoritative != terminal:
            raise ValueError("prospective Triage terminal differs from state authority")
        if (
            canonical_hash(candidate_set.to_dict())
            != record.protocol_artifact_hashes["candidate_set"]
            or terminal.candidate_set_id != candidate_set.candidate_set_id
            or terminal.registration_id != record.registration_id
            or terminal.checkpoint_key != record.checkpoint_key
            or terminal.route_plan_id != record.route_plan_id
            or terminal.route_admission_id != record.route_admission_id
        ):
            raise ValueError("prospective Triage terminal does not bind the active batch")
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                """
                SELECT route_epoch_key, artifact_hash FROM prospective_triage_batches
                WHERE batch_id = ?
                """,
                (batch_id,),
            ).fetchone()
            if row is None or cast(str, row["artifact_hash"]) != cast(
                str, record_row["artifact_hash"]
            ):
                raise ValueError("prospective Triage batch changed before terminalization")
            existing = connection.execute(
                """
                SELECT terminal_id FROM prospective_triage_terminalizations
                WHERE batch_id = ?
                """,
                (batch_id,),
            ).fetchone()
            if existing is not None:
                if cast(str, existing["terminal_id"]) != terminal.terminal_id:
                    raise ValueError("prospective Triage terminalization is inconsistent")
                return
            completed = connection.execute(
                """
                SELECT decision_id FROM prospective_triage_completions WHERE batch_id = ?
                """,
                (batch_id,),
            ).fetchone()
            if completed is not None:
                raise ValueError("completed prospective Triage batch cannot be terminalized")
            epoch = cast(str, row["route_epoch_key"])
            head = connection.execute(
                """
                SELECT batch_id FROM prospective_triage_active_heads
                WHERE route_epoch_key = ?
                """,
                (epoch,),
            ).fetchone()
            if head is None or cast(str, head["batch_id"]) != batch_id:
                raise ValueError("prospective Triage active head changed before terminalization")
            connection.execute(
                """
                INSERT INTO prospective_triage_terminalizations(
                    batch_id, terminal_id, terminalized_at
                ) VALUES (?, ?, ?)
                """,
                (
                    batch_id,
                    terminal.terminal_id,
                    terminal.terminalized_at.isoformat().replace("+00:00", "Z"),
                ),
            )
            connection.execute(
                """
                INSERT INTO prospective_triage_epoch_revisions(route_epoch_key, revision)
                VALUES (?, 1)
                ON CONFLICT(route_epoch_key) DO UPDATE SET revision = revision + 1
                """,
                (epoch,),
            )
            connection.execute(
                """
                DELETE FROM prospective_triage_active_heads
                WHERE route_epoch_key = ? AND batch_id = ?
                """,
                (epoch, batch_id),
            )


def _active_batch_record(
    prepared: PreparedProspectiveTriageWork,
) -> ProspectiveTriageActiveBatchRecord:
    candidate = prepared.candidate_set
    record = ProspectiveTriageActiveBatchRecord(
        batch_id=prepared.active_batch_id,
        registration_id=candidate.registration_id,
        checkpoint_key=candidate.checkpoint_key,
        route_plan_id=candidate.route_plan_id,
        route_admission_id=candidate.route_admission_id,
        readiness_report_id=prepared.readiness_report_id,
        unclassified_candidate_count=prepared.unclassified_candidate_count,
        data_snapshot_id=prepared.snapshot.snapshot_id,
        profile_id=prepared.profile.profile_id,
        protocol_artifact_hashes=dict(prepared.protocol_artifact_hashes),
        created_at=prepared.selection.selected_at,
    )
    expected = f"prospective-triage-active-batch-{canonical_hash(record.core_dict())}"
    if record.batch_id != expected:
        raise ValueError("prospective Triage active batch identity is invalid")
    return record


def _active_batch_record_from_dict(value: object) -> ProspectiveTriageActiveBatchRecord:
    if not isinstance(value, dict):
        raise TypeError("prospective Triage active batch must be an object")
    payload = cast(dict[str, object], value)
    expected = {
        "schema_version",
        "batch_id",
        "registration_id",
        "checkpoint_key",
        "route_plan_id",
        "route_admission_id",
        "readiness_report_id",
        "unclassified_candidate_count",
        "data_snapshot_id",
        "profile_id",
        "protocol_artifact_hashes",
        "created_at",
    }
    if set(payload) != expected or payload.get("schema_version") != _ACTIVE_BATCH_SCHEMA:
        raise ValueError("prospective Triage active batch fields are invalid")

    def required_string(name: str) -> str:
        item = payload.get(name)
        if not isinstance(item, str) or not item or item != item.strip():
            raise ValueError(f"prospective Triage active batch {name} is invalid")
        return item

    raw_hashes = payload.get("protocol_artifact_hashes")
    if not isinstance(raw_hashes, dict):
        raise ValueError("prospective Triage active batch artifact bindings are invalid")
    hash_payload = cast(dict[str, object], raw_hashes)
    if set(hash_payload) != {
        "readiness",
        "selection",
        "candidate_set",
        "work_manifest",
        "execution_plan",
    }:
        raise ValueError("prospective Triage active batch artifact bindings are invalid")
    hashes: dict[str, str] = {}
    for name, item in hash_payload.items():
        if not isinstance(item, str) or len(item) != 64:
            raise ValueError("prospective Triage active batch artifact hash is invalid")
        int(item, 16)
        hashes[name] = item
    raw_count = payload.get("unclassified_candidate_count")
    if not isinstance(raw_count, int) or isinstance(raw_count, bool) or raw_count < 1:
        raise ValueError("prospective Triage active batch candidate count is invalid")
    created_at = datetime.fromisoformat(required_string("created_at").replace("Z", "+00:00"))
    if created_at.tzinfo is None or created_at.utcoffset() is None:
        raise ValueError("prospective Triage active batch created_at must be timezone-aware")
    record = ProspectiveTriageActiveBatchRecord(
        batch_id=required_string("batch_id"),
        registration_id=required_string("registration_id"),
        checkpoint_key=required_string("checkpoint_key"),
        route_plan_id=required_string("route_plan_id"),
        route_admission_id=required_string("route_admission_id"),
        readiness_report_id=required_string("readiness_report_id"),
        unclassified_candidate_count=raw_count,
        data_snapshot_id=required_string("data_snapshot_id"),
        profile_id=required_string("profile_id"),
        protocol_artifact_hashes=hashes,
        created_at=created_at.astimezone(UTC),
    )
    expected_id = f"prospective-triage-active-batch-{canonical_hash(record.core_dict())}"
    if record.batch_id != expected_id or record.to_dict() != payload:
        raise ValueError("prospective Triage active batch is not canonical")
    return record


def _load_prepared_prospective_triage_work(
    *,
    record: ProspectiveTriageActiveBatchRecord,
    registration: ProspectiveDiagnosticRegistration,
    state_root: Path,
    run_root: Path,
) -> PreparedProspectiveTriageWork:
    state_store = LocalDataSnapshotStore(state_root)
    protocol_store = ArtifactStore(run_root / "protocol-artifacts")
    selection = event_impact_triage_batch_selection_from_dict(
        state_store.artifacts.read_json(record.protocol_artifact_hashes["selection"])
    )
    _assert_readiness_binding(
        state_store.artifacts.read_json(record.protocol_artifact_hashes["readiness"]),
        record=record,
        selection=selection,
    )
    candidate_set = event_impact_triage_candidate_set_from_dict(
        state_store.artifacts.read_json(record.protocol_artifact_hashes["candidate_set"])
    )
    manifest = event_impact_triage_work_manifest_from_dict(
        protocol_store.read_json(record.protocol_artifact_hashes["work_manifest"])
    )
    plan = event_impact_triage_work_execution_plan_from_dict(
        protocol_store.read_json(record.protocol_artifact_hashes["execution_plan"])
    )
    snapshot = state_store.get(record.data_snapshot_id)
    profile = load_builtin_model_provider_profile(registration.model_profile_id)
    if (
        record.registration_id != registration.registration_id
        or candidate_set.registration_id != record.registration_id
        or candidate_set.checkpoint_key != record.checkpoint_key
        or candidate_set.route_plan_id != record.route_plan_id
        or candidate_set.route_admission_id != record.route_admission_id
        or candidate_set.readiness_report_id != record.readiness_report_id
        or selection.readiness_report_id != record.readiness_report_id
        or selection.checkpoint_key != record.checkpoint_key
        or candidate_set.data_snapshot_id != snapshot.snapshot_id
        or manifest.candidate_set_id != candidate_set.candidate_set_id
        or plan.candidate_set_id != candidate_set.candidate_set_id
        or plan.work_manifest_id != manifest.manifest_id
        or plan.registration_id != record.registration_id
        or plan.checkpoint_key != record.checkpoint_key
        or profile.profile_id != record.profile_id
    ):
        raise ValueError("prospective Triage active batch bindings do not reopen")
    prepared = PreparedProspectiveTriageWork(
        active_batch_id=record.batch_id,
        readiness_report_id=record.readiness_report_id,
        unclassified_candidate_count=record.unclassified_candidate_count,
        selection=selection,
        snapshot=snapshot,
        candidate_set=candidate_set,
        manifest=manifest,
        plan=plan,
        profile=profile,
        protocol_artifact_hashes=dict(record.protocol_artifact_hashes),
    )
    _active_batch_record(prepared)
    return prepared


def _assert_readiness_binding(
    value: object,
    *,
    record: ProspectiveTriageActiveBatchRecord,
    selection: EventImpactTriageBatchSelection,
) -> None:
    if not isinstance(value, dict):
        raise TypeError("prospective Triage readiness artifact must be an object")
    payload = cast(dict[str, object], value)
    if (
        payload.get("schema_version") != "market-impact.prospective-checkpoint-readiness-report.v1"
        or payload.get("report_id") != record.readiness_report_id
        or payload.get("registration_id") != record.registration_id
        or payload.get("route_plan_id") != record.route_plan_id
        or payload.get("route_admission_id") != record.route_admission_id
    ):
        raise ValueError("prospective Triage readiness authority differs from active batch")
    raw_checkpoints = payload.get("checkpoints")
    if not isinstance(raw_checkpoints, list):
        raise ValueError("prospective Triage readiness checkpoints are invalid")
    checkpoint_values = cast(list[object], raw_checkpoints)
    checkpoints = tuple(
        cast(dict[str, object], item) for item in checkpoint_values if isinstance(item, dict)
    )
    if len(checkpoints) != len(checkpoint_values):
        raise ValueError("prospective Triage readiness checkpoint entry is invalid")
    matches = tuple(
        item for item in checkpoints if item.get("checkpoint_key") == record.checkpoint_key
    )
    if len(matches) != 1:
        raise ValueError("prospective Triage readiness checkpoint binding is invalid")
    raw_versions = matches[0].get("trigger_candidate_version_ids")
    if not isinstance(raw_versions, list):
        raise ValueError("prospective Triage readiness candidate versions are invalid")
    version_values = cast(list[object], raw_versions)
    if any(not isinstance(item, str) for item in version_values):
        raise ValueError("prospective Triage readiness candidate versions are invalid")
    readiness_versions = cast(list[str], version_values)
    selected_population = [item.version_id for item in selection.candidates]
    if (
        len(readiness_versions) != record.unclassified_candidate_count
        or set(readiness_versions) != set(selected_population)
        or len(readiness_versions) != len(set(readiness_versions))
    ):
        raise ValueError("prospective Triage readiness population differs from active batch")


def prepare_next_prospective_triage_work(
    *,
    registration: ProspectiveDiagnosticRegistration,
    route_plan: ProspectiveCheckpointRoutePlan,
    checkpoint_key: str,
    state_root: Path,
    run_root: Path,
    skill_root: Path,
    evaluated_at: datetime,
    maximum_candidate_count: int = 32,
) -> PreparedProspectiveTriageWork:
    """Freeze the next actual-receipt prefix and its minimal material-ingress plan."""

    if evaluated_at.tzinfo is None or evaluated_at.utcoffset() is None:
        raise ValueError("prospective triage evaluated_at must be timezone-aware")
    evaluation_time = evaluated_at.astimezone(UTC)
    store = LocalDataSnapshotStore(state_root)
    journal = ProspectiveDataJournal(store)
    classification = EventImpactTriageDecisionStore(state_root)
    readiness = evaluate_prospective_checkpoint_readiness(
        registration=registration,
        route_plan=route_plan,
        admission_store=ProspectiveCheckpointAdmissionStore(state_root),
        runtime=ProspectiveCollectionRuntime(store),
        evaluated_at=evaluation_time,
        classification_authority=classification,
    )
    readiness_artifact = store.artifacts.put_json(readiness.to_dict())
    selection = EventImpactTriageBatchSelection.build(
        readiness_report=readiness,
        checkpoint_key=checkpoint_key,
        journal=journal,
        selected_at=evaluation_time,
        maximum_candidate_count=maximum_candidate_count,
    )
    selection_artifact = store.artifacts.put_json(selection.to_dict())
    snapshot = journal.freeze_version_selection_snapshot(
        selection_id=selection.selection_id,
        readiness_report_id=readiness.report_id,
        version_ids=selection.selected_version_ids,
        as_of=readiness.evaluated_at,
        frozen_at=evaluation_time,
    )
    candidate_set = freeze_event_impact_triage_candidate_set(
        readiness_report=readiness,
        checkpoint_key=checkpoint_key,
        snapshot=snapshot,
        snapshot_store=store,
        admission_store=ProspectiveCheckpointAdmissionStore(state_root),
        frozen_at=evaluation_time,
        batch_selection=selection,
        selection_journal=journal,
    )
    candidate_artifact = store.artifacts.put_json(candidate_set.to_dict())
    resolver = SnapshotTriageCandidateContentResolver(store)
    manifest = build_event_impact_triage_work_manifest(
        candidate_set=candidate_set,
        contents=resolver.resolve(candidate_set),
        policy=TriageWorkManifestPolicy(
            max_atoms_per_work_unit=12,
            max_candidate_versions_per_work_unit=12,
            max_estimated_serialized_prompt_utf8_tokens=32_768,
        ),
    )
    profile = load_builtin_model_provider_profile(registration.model_profile_id)
    plan = build_event_impact_triage_work_execution_plan_v11(
        candidate_set=candidate_set,
        work_manifest=manifest,
        registration=registration,
        arm=TriageComparisonArm.TREATMENT,
        model_profile_alias=registration.model_profile_id,
        model_profile=profile,
        skills=SkillRegistry(skill_root),
    )
    registered_limit = int(Decimal(registration.aggregate_model_cost_limit_usd) * 1_000_000)
    if plan.max_total_estimated_cost_microusd > registered_limit:
        raise ValueError("prospective triage Work plan exceeds the registered aggregate cost cap")
    protocol_store = ArtifactStore(run_root / "protocol-artifacts")
    manifest_artifact = protocol_store.put_json(manifest.to_dict())
    plan_artifact = protocol_store.put_json(plan.to_dict())
    protocol_artifact_hashes = {
        "readiness": readiness_artifact.content_hash,
        "selection": selection_artifact.content_hash,
        "candidate_set": candidate_artifact.content_hash,
        "work_manifest": manifest_artifact.content_hash,
        "execution_plan": plan_artifact.content_hash,
    }
    active_core = {
        "schema_version": _ACTIVE_BATCH_SCHEMA,
        "registration_id": candidate_set.registration_id,
        "checkpoint_key": candidate_set.checkpoint_key,
        "route_plan_id": candidate_set.route_plan_id,
        "route_admission_id": candidate_set.route_admission_id,
        "readiness_report_id": readiness.report_id,
        "unclassified_candidate_count": len(selection.candidates),
        "data_snapshot_id": snapshot.snapshot_id,
        "profile_id": profile.profile_id,
        "protocol_artifact_hashes": protocol_artifact_hashes,
        "created_at": selection.selected_at.isoformat().replace("+00:00", "Z"),
    }
    prepared = PreparedProspectiveTriageWork(
        active_batch_id=f"prospective-triage-active-batch-{canonical_hash(active_core)}",
        readiness_report_id=readiness.report_id,
        unclassified_candidate_count=len(selection.candidates),
        selection=selection,
        snapshot=snapshot,
        candidate_set=candidate_set,
        manifest=manifest,
        plan=plan,
        profile=profile,
        protocol_artifact_hashes=protocol_artifact_hashes,
    )
    _active_batch_record(prepared)
    return prepared


def prepare_or_reopen_prospective_triage_work(
    *,
    registration: ProspectiveDiagnosticRegistration,
    route_plan: ProspectiveCheckpointRoutePlan,
    checkpoint_key: str,
    state_root: Path,
    run_root: Path,
    skill_root: Path,
    evaluated_at: datetime,
    maximum_candidate_count: int = 32,
) -> PreparedProspectiveTriageWork:
    """Reopen the route epoch's active graph or atomically reserve its next prefix."""

    admission_store = ProspectiveCheckpointAdmissionStore(state_root)
    if admission_store.current_plan_id(registration.registration_id) != route_plan.plan_id:
        raise ValueError("prospective Triage route plan is not the current admitted plan")
    admission = admission_store.admission(route_plan.plan_id)
    active_store = ProspectiveTriageActiveBatchStore(run_root)
    active = active_store.active(
        registration_id=registration.registration_id,
        checkpoint_key=checkpoint_key,
        route_plan_id=route_plan.plan_id,
        route_admission_id=admission.admission_id,
    )
    if active is not None:
        return _load_prepared_prospective_triage_work(
            record=active,
            registration=registration,
            state_root=state_root,
            run_root=run_root,
        )
    expected_epoch_revision = active_store.epoch_revision(
        registration_id=registration.registration_id,
        checkpoint_key=checkpoint_key,
        route_plan_id=route_plan.plan_id,
        route_admission_id=admission.admission_id,
    )
    prepared = prepare_next_prospective_triage_work(
        registration=registration,
        route_plan=route_plan,
        checkpoint_key=checkpoint_key,
        state_root=state_root,
        run_root=run_root,
        skill_root=skill_root,
        evaluated_at=evaluated_at,
        maximum_candidate_count=maximum_candidate_count,
    )
    installed = active_store.install(
        prepared,
        expected_epoch_revision=expected_epoch_revision,
    )
    if installed.batch_id == prepared.active_batch_id:
        return prepared
    return _load_prepared_prospective_triage_work(
        record=installed,
        registration=registration,
        state_root=state_root,
        run_root=run_root,
    )


def reopen_active_prospective_triage_work(
    *,
    registration: ProspectiveDiagnosticRegistration,
    route_plan: ProspectiveCheckpointRoutePlan,
    checkpoint_key: str,
    state_root: Path,
    run_root: Path,
) -> PreparedProspectiveTriageWork:
    """Reopen an existing active batch without selecting or freezing new receipts."""

    admission_store = ProspectiveCheckpointAdmissionStore(state_root)
    if admission_store.current_plan_id(registration.registration_id) != route_plan.plan_id:
        raise ValueError("prospective Triage route plan is not the current admitted plan")
    admission = admission_store.admission(route_plan.plan_id)
    active = ProspectiveTriageActiveBatchStore(run_root).active(
        registration_id=registration.registration_id,
        checkpoint_key=checkpoint_key,
        route_plan_id=route_plan.plan_id,
        route_admission_id=admission.admission_id,
    )
    if active is None:
        raise ValueError("prospective Triage has no active batch to reopen")
    return _load_prepared_prospective_triage_work(
        record=active,
        registration=registration,
        state_root=state_root,
        run_root=run_root,
    )


def _build_prospective_triage_runner(
    *,
    prepared: PreparedProspectiveTriageWork,
    registration: ProspectiveDiagnosticRegistration,
    state_root: Path,
    run_root: Path,
    skill_root: Path,
    provider: ModelProvider,
) -> EventImpactTriageWorkRunner:
    if (
        provider.provider_id != prepared.profile.provider_id
        or provider.model != prepared.profile.model
    ):
        raise ValueError("prospective triage Provider differs from the frozen profile")
    credential = os.environ.get(prepared.profile.credential_env, "")
    work_root = run_root / "runs" / prepared.plan.plan_id
    return EventImpactTriageWorkRunner(
        plan=prepared.plan,
        candidate_set=prepared.candidate_set,
        work_manifest=prepared.manifest,
        registration=registration,
        provider=provider,
        content_resolver=SnapshotTriageCandidateContentResolver(LocalDataSnapshotStore(state_root)),
        skills=SkillRegistry(skill_root),
        artifact_store=ArtifactStore(work_root / "artifacts"),
        journal=RunJournal(work_root / "runs.sqlite3"),
        usage_ledger=UsageLedger(work_root / "usage.sqlite3"),
        format_recovery_store=EventImpactTriageWorkFormatRecoveryStore(
            work_root / "format-recovery.sqlite3"
        ),
        provider_health_store=ProviderHealthStore(work_root / "provider-health.sqlite3"),
        secret_values=(() if not credential else (credential,)),
    )


def authorize_prepared_prospective_triage_format_recovery(
    *,
    prepared: PreparedProspectiveTriageWork,
    registration: ProspectiveDiagnosticRegistration,
    state_root: Path,
    run_root: Path,
    skill_root: Path,
    original_run_id: str,
    authorized_at: datetime,
    provider: ModelProvider | None = None,
) -> dict[str, object]:
    """Authorize one bounded old-plan parse recovery without a Provider request."""

    selected_provider = (
        _NoCallModelProvider(
            provider_id=prepared.profile.provider_id,
            model=prepared.profile.model,
        )
        if provider is None
        else provider
    )
    runner = _build_prospective_triage_runner(
        prepared=prepared,
        registration=registration,
        state_root=state_root,
        run_root=run_root,
        skill_root=skill_root,
        provider=selected_provider,
    )
    grant = runner.authorize_format_recovery(
        original_run_id=original_run_id,
        authorized_at=authorized_at,
    )
    return {
        **prepared.summary(),
        "grant_id": grant.grant_id,
        "source_run_id": grant.original_run_id,
        "recovery_run_id": grant.recovery_run_id,
        "parser_id": grant.parser_id,
        "repair_policy_id": grant.repair_policy_id,
        "authorized_at": grant.to_dict()["authorized_at"],
        "provider_calls": 0,
        "usage_record_created": False,
        "judgment_or_execution_authority": False,
    }


async def run_prepared_prospective_triage_work(
    *,
    prepared: PreparedProspectiveTriageWork,
    registration: ProspectiveDiagnosticRegistration,
    state_root: Path,
    run_root: Path,
    skill_root: Path,
    provider: ModelProvider | None = None,
) -> dict[str, object]:
    """Run one frozen Work graph and persist a Decision only after full reopening."""

    if prepared.plan.schema_version in {
        EVENT_IMPACT_TRIAGE_WORK_EXECUTION_PLAN_SCHEMA_V9,
        EVENT_IMPACT_TRIAGE_WORK_EXECUTION_PLAN_SCHEMA_V10,
    }:
        raise ValueError(
            "prospective material ingress is comparison-governed; use the comparison run"
        )
    comparison_path = run_root / "comparison" / "registrations.sqlite3"
    if comparison_path.exists() and EventImpactTriageWorkComparisonStore(
        comparison_path
    ).has_registration_for_candidate_set(prepared.candidate_set.candidate_set_id):
        raise ValueError(
            "prospective triage Candidate Set is comparison-bound; use the comparison run"
        )
    selected_provider = _LazyAvailabilityModelProvider(
        profile=prepared.profile,
        provider=provider,
    )
    runner = _build_prospective_triage_runner(
        prepared=prepared,
        registration=registration,
        state_root=state_root,
        run_root=run_root,
        skill_root=skill_root,
        provider=selected_provider,
    )
    result = await runner.run()
    summary: dict[str, object] = {
        **prepared.summary(),
        "status": result.status.value,
        "attempted_member_count": len(result.members),
        "completed_member_count": sum(
            item.status is RunStatus.COMPLETED for item in result.members
        ),
        "provider_attempts": sum(item.metrics.provider_attempts for item in result.members),
        "input_tokens": sum(item.metrics.input_tokens for item in result.members),
        "output_tokens": sum(item.metrics.output_tokens for item in result.members),
        "estimated_cost_microusd": sum(
            item.metrics.estimated_cost_microusd for item in result.members
        ),
        "digest_count": len(result.digests),
        "cluster_count": 0 if result.partition is None else len(result.partition.clusters),
        "decision_id": None,
        "judgment_or_execution_authority": False,
    }
    if result.status is RunStatus.COMPLETED:
        if result.partition is None or result.proposal is None or result.run_evidence is None:
            raise ValueError("completed prospective triage Work lacks terminal artifacts")
        authority = EventImpactTriageWorkDecisionAuthority(
            runner=runner,
            candidate_set=prepared.candidate_set,
            work_manifest=prepared.manifest,
            digests=result.digests,
            partition=result.partition,
            proposal=result.proposal,
            run_evidence=result.run_evidence,
        )
        evidence = authority.decision_evidence()
        decision = EventImpactTriageDecisionStore(state_root).admit_work(
            candidate_set=prepared.candidate_set,
            proposal=result.proposal,
            run_evidence=evidence,
            run_authority=authority,
            decided_at=evidence.finished_at,
        )
        summary.update(
            {
                "decision_id": decision.decision_id,
                "decision_status": decision.status.value,
                "selected_cluster_id": decision.selected_cluster_id,
                "event_assessment_cluster_count": len(decision.event_assessment_cluster_ids),
                "attention_watch_cluster_count": len(decision.attention_watch_cluster_ids),
                "archive_cluster_count": len(decision.archive_cluster_ids),
                "blocking_review_cluster_count": len(decision.blocking_review_cluster_ids),
            }
        )
        ProspectiveTriageActiveBatchStore(run_root).complete(
            batch_id=prepared.active_batch_id,
            candidate_set=prepared.candidate_set,
            state_root=state_root,
        )
    summary_artifact = ArtifactStore(run_root / "protocol-artifacts").put_json(summary)
    return {**summary, "summary_artifact_hash": summary_artifact.content_hash}


async def run_prepared_prospective_triage_comparison(
    *,
    prepared: PreparedProspectiveTriageWork,
    registration: ProspectiveDiagnosticRegistration,
    label_set: EventImpactTriageLabelSet,
    state_root: Path,
    run_root: Path,
    skill_root: Path,
    baseline_provider: ModelProvider | None = None,
    treatment_provider: ModelProvider | None = None,
) -> dict[str, object]:
    """Run one pre-labelled blind, same-contract independent replication."""

    if label_set.exposure is not TriageLabelExposure.PRISTINE_BLIND:
        raise ValueError("prospective triage comparison requires pristine blind labels")
    if label_set.candidate_set_id != prepared.candidate_set.candidate_set_id:
        raise ValueError("prospective triage labels belong to another Candidate Set")
    if prepared.plan.arm is not TriageComparisonArm.TREATMENT:
        raise ValueError("prepared prospective triage plan is not the treatment arm")
    plan_builder = {
        EVENT_IMPACT_TRIAGE_WORK_EXECUTION_PLAN_SCHEMA_V9: (
            build_event_impact_triage_work_execution_plan_v9
        ),
        EVENT_IMPACT_TRIAGE_WORK_EXECUTION_PLAN_SCHEMA_V10: (
            build_event_impact_triage_work_execution_plan_v10
        ),
        EVENT_IMPACT_TRIAGE_WORK_EXECUTION_PLAN_SCHEMA_V11: (
            build_event_impact_triage_work_execution_plan_v11
        ),
    }.get(prepared.plan.schema_version, build_event_impact_triage_work_execution_plan_v8)
    baseline_plan = plan_builder(
        candidate_set=prepared.candidate_set,
        work_manifest=prepared.manifest,
        registration=registration,
        arm=TriageComparisonArm.BASELINE,
        model_profile_alias=registration.model_profile_id,
        model_profile=prepared.profile,
        skills=SkillRegistry(skill_root),
    )
    protocol_store = ArtifactStore(run_root / "protocol-artifacts")
    label_artifact = protocol_store.put_json(label_set.to_dict())
    baseline_plan_artifact = protocol_store.put_json(baseline_plan.to_dict())
    treatment_plan_artifact = protocol_store.put_json(prepared.plan.to_dict())
    comparison_store = EventImpactTriageWorkComparisonStore(
        run_root / "comparison" / "registrations.sqlite3"
    )
    comparison = comparison_store.register(
        candidate_set=prepared.candidate_set,
        label_set=label_set,
        work_manifest=prepared.manifest,
        baseline_plan=baseline_plan,
        treatment_plan=prepared.plan,
    )
    comparison_artifact = protocol_store.put_json(comparison.to_dict())
    selected_baseline_provider = _LazyAvailabilityModelProvider(
        profile=prepared.profile,
        provider=baseline_provider,
    )
    selected_treatment_provider = _LazyAvailabilityModelProvider(
        profile=prepared.profile,
        provider=treatment_provider,
    )
    baseline_prepared = replace(
        prepared,
        plan=baseline_plan,
        protocol_artifact_hashes={
            **prepared.protocol_artifact_hashes,
            "execution_plan": baseline_plan_artifact.content_hash,
        },
    )
    baseline_runner = _build_prospective_triage_runner(
        prepared=baseline_prepared,
        registration=registration,
        state_root=state_root,
        run_root=run_root,
        skill_root=skill_root,
        provider=selected_baseline_provider,
    )
    treatment_runner = _build_prospective_triage_runner(
        prepared=prepared,
        registration=registration,
        state_root=state_root,
        run_root=run_root,
        skill_root=skill_root,
        provider=selected_treatment_provider,
    )
    baseline_result, treatment_result = await asyncio.gather(
        baseline_runner.run(), treatment_runner.run()
    )

    def arm_summary(result: object) -> dict[str, object]:
        work_result = cast(EventImpactTriageWorkRunResult, result)
        return {
            "status": work_result.status.value,
            "attempted_member_count": len(work_result.members),
            "completed_member_count": sum(
                item.status is RunStatus.COMPLETED for item in work_result.members
            ),
            "provider_attempts": sum(
                item.metrics.provider_attempts for item in work_result.members
            ),
            "input_tokens": sum(item.metrics.input_tokens for item in work_result.members),
            "output_tokens": sum(item.metrics.output_tokens for item in work_result.members),
            "estimated_cost_microusd": sum(
                item.metrics.estimated_cost_microusd for item in work_result.members
            ),
        }

    summary: dict[str, object] = {
        **prepared.summary(),
        "label_set_id": label_set.label_set_id,
        "label_exposure": label_set.exposure.value,
        "comparison_id": comparison.comparison_id,
        "baseline_plan_id": baseline_plan.plan_id,
        "treatment_plan_id": prepared.plan.plan_id,
        "protocol_artifact_hashes": {
            **prepared.protocol_artifact_hashes,
            "label_set": label_artifact.content_hash,
            "baseline_plan": baseline_plan_artifact.content_hash,
            "treatment_plan": treatment_plan_artifact.content_hash,
            "comparison_registration": comparison_artifact.content_hash,
        },
        "baseline": arm_summary(baseline_result),
        "treatment": arm_summary(treatment_result),
        "comparison_report_id": None,
        "decision_id": None,
        "terminal_id": None,
        "judgment_or_execution_authority": False,
    }
    if (
        baseline_result.status is not RunStatus.COMPLETED
        or treatment_result.status is not RunStatus.COMPLETED
    ):
        summary["status"] = "incomplete"
        summary_artifact = protocol_store.put_json(summary)
        return {**summary, "summary_artifact_hash": summary_artifact.content_hash}

    def completed_arm(
        *, runner: EventImpactTriageWorkRunner, result: object
    ) -> tuple[TriageWorkArmOutcome, EventImpactTriageWorkDecisionAuthority]:
        work_result = cast(EventImpactTriageWorkRunResult, result)
        if (
            work_result.partition is None
            or work_result.proposal is None
            or work_result.run_evidence is None
        ):
            raise ValueError("completed triage comparison arm lacks terminal artifacts")
        authority = EventImpactTriageWorkDecisionAuthority(
            runner=runner,
            candidate_set=prepared.candidate_set,
            work_manifest=prepared.manifest,
            digests=work_result.digests,
            partition=work_result.partition,
            proposal=work_result.proposal,
            run_evidence=work_result.run_evidence,
        )
        return (
            TriageWorkArmOutcome(
                plan=runner.plan,
                work_manifest=prepared.manifest,
                digests=work_result.digests,
                partition=work_result.partition,
                proposal=work_result.proposal,
                run_evidence=work_result.run_evidence,
            ),
            authority,
        )

    baseline_outcome, _ = completed_arm(runner=baseline_runner, result=baseline_result)
    treatment_outcome, treatment_authority = completed_arm(
        runner=treatment_runner, result=treatment_result
    )
    report = comparison_store.reopen_report(
        registration=comparison,
        candidate_set=prepared.candidate_set,
        label_set=label_set,
        work_manifest=prepared.manifest,
        baseline=baseline_outcome,
        treatment=treatment_outcome,
        baseline_authority=baseline_runner,
        treatment_authority=treatment_runner,
    )
    if report is None:
        report = evaluate_event_impact_triage_work_comparison(
            registration=comparison,
            candidate_set=prepared.candidate_set,
            label_set=label_set,
            work_manifest=prepared.manifest,
            baseline=baseline_outcome,
            treatment=treatment_outcome,
            baseline_authority=baseline_runner,
            treatment_authority=treatment_runner,
            registration_authority=comparison_store,
            evaluated_at=datetime.now(UTC),
        )
        comparison_store.record_report(
            report=report,
            registration=comparison,
            candidate_set=prepared.candidate_set,
            label_set=label_set,
            work_manifest=prepared.manifest,
            baseline=baseline_outcome,
            treatment=treatment_outcome,
            baseline_authority=baseline_runner,
            treatment_authority=treatment_runner,
        )
    report_artifact = protocol_store.put_json(report.to_dict())
    outcome_artifacts = {
        "baseline_outcome": protocol_store.put_json(baseline_outcome.to_dict()).content_hash,
        "treatment_outcome": protocol_store.put_json(treatment_outcome.to_dict()).content_hash,
        "comparison_report": report_artifact.content_hash,
    }
    summary.update(
        {
            "status": "completed",
            "comparison_report_id": report.report_id,
            "batch_gate_passed": report.batch_gate_passed,
            "promotion_eligible": report.promotion_eligible,
            "comparison_blockers": list(report.blockers),
            "baseline_score": report.baseline_score.to_dict(),
            "treatment_score": report.treatment_score.to_dict(),
            "protocol_artifact_hashes": {
                **cast(dict[str, str], summary["protocol_artifact_hashes"]),
                **outcome_artifacts,
            },
        }
    )
    if report.batch_gate_passed:
        evidence = treatment_authority.decision_evidence()
        decision = EventImpactTriageDecisionStore(state_root).admit_work(
            candidate_set=prepared.candidate_set,
            proposal=treatment_outcome.proposal,
            run_evidence=evidence,
            run_authority=treatment_authority,
            decided_at=evidence.finished_at,
        )
        ProspectiveTriageActiveBatchStore(run_root).complete(
            batch_id=prepared.active_batch_id,
            candidate_set=prepared.candidate_set,
            state_root=state_root,
        )
        summary.update(
            {
                "decision_id": decision.decision_id,
                "decision_status": decision.status.value,
                "event_assessment_cluster_count": len(decision.event_assessment_cluster_ids),
                "attention_watch_cluster_count": len(decision.attention_watch_cluster_ids),
                "archive_cluster_count": len(decision.archive_cluster_ids),
            }
        )
    else:
        terminal_store = EventImpactTriageDecisionStore(state_root)
        terminal = terminal_store.reopen_failed_work_comparison_terminal(
            candidate_set=prepared.candidate_set,
            comparison=comparison,
            report=report,
        )
        if terminal is None:
            terminal = terminal_store.terminalize_failed_work_comparison(
                candidate_set=prepared.candidate_set,
                comparison=comparison,
                report=report,
                label_set=label_set,
                work_manifest=prepared.manifest,
                baseline=baseline_outcome,
                treatment=treatment_outcome,
                baseline_authority=baseline_runner,
                treatment_authority=treatment_runner,
                comparison_authority=comparison_store,
                terminalized_at=datetime.now(UTC),
            )
        ProspectiveTriageActiveBatchStore(run_root).terminalize(
            batch_id=prepared.active_batch_id,
            candidate_set=prepared.candidate_set,
            terminal=terminal,
            state_root=state_root,
        )
        summary.update(
            {
                "terminal_id": terminal.terminal_id,
                "active_head_released": True,
                "terminal_version_count": len(terminal.candidate_version_ids),
            }
        )
    summary_artifact = protocol_store.put_json(summary)
    return {**summary, "summary_artifact_hash": summary_artifact.content_hash}
