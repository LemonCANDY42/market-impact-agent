from __future__ import annotations

import importlib
import json
import os
import sqlite3
import tempfile
from collections import defaultdict
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from itertools import pairwise
from pathlib import Path
from typing import Any, cast
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from market_impact_agent.agent_contracts import canonical_hash, canonical_json_bytes
from market_impact_agent.data_inputs import (
    DATA_SNAPSHOT_SCHEMA,
    DataFetchStatus,
    DataPITLane,
    DataProviderAttempt,
    DataQuery,
    DataSnapshot,
    DataSourceBinding,
    LocalDataSnapshotStore,
    SourceObservation,
    data_snapshot_coverage_complete,
    source_observation_from_dict,
)
from market_impact_agent.domain import require_aware
from market_impact_agent.observations import ObservationCapability

PROSPECTIVE_COLLECTION_POLICY_SCHEMA_V1 = "market-impact.prospective-collection-policy.v1"
PROSPECTIVE_COLLECTION_POLICY_SCHEMA_V2 = "market-impact.prospective-collection-policy.v2"
# Backward-compatible default. Rolling request windows select v2 explicitly.
PROSPECTIVE_COLLECTION_POLICY_SCHEMA = PROSPECTIVE_COLLECTION_POLICY_SCHEMA_V1
_SUPPORTED_COLLECTION_POLICY_SCHEMAS = frozenset(
    {PROSPECTIVE_COLLECTION_POLICY_SCHEMA_V1, PROSPECTIVE_COLLECTION_POLICY_SCHEMA_V2}
)
PROSPECTIVE_DATASET_MANIFEST_SCHEMA = "market-impact.prospective-dataset-manifest.v1"
PROSPECTIVE_JOURNAL_SELECTION_SCHEMA = "market-impact.prospective-journal-selection.v1"


@dataclass(frozen=True, slots=True)
class ProspectiveRollingWindow:
    lookback_seconds: int
    timezone: str
    start_parameter: str = "start_date"
    end_parameter: str = "end_date"
    datetime_format: str = "%Y-%m-%d %H:%M:%S"

    def __post_init__(self) -> None:
        if self.lookback_seconds < 1:
            raise ValueError("rolling collection lookback_seconds must be positive")
        if self.start_parameter == self.end_parameter:
            raise ValueError("rolling collection parameter names must be different")
        for value, name in (
            (self.timezone, "timezone"),
            (self.start_parameter, "start_parameter"),
            (self.end_parameter, "end_parameter"),
            (self.datetime_format, "datetime_format"),
        ):
            if not value or value != value.strip():
                raise ValueError(f"rolling collection {name} must be non-empty trimmed text")
        try:
            ZoneInfo(self.timezone)
        except ZoneInfoNotFoundError as error:
            raise ValueError("rolling collection timezone is unknown") from error

    def resolve(self, scheduled_for: datetime) -> tuple[datetime, dict[str, str]]:
        _strict_utc(scheduled_for, "rolling collection scheduled_for")
        timezone = ZoneInfo(self.timezone)
        end = scheduled_for.astimezone(timezone)
        start = end - timedelta(seconds=self.lookback_seconds)
        start_value = start.strftime(self.datetime_format)
        end_value = end.strftime(self.datetime_format)
        represented_start = datetime.strptime(start_value, self.datetime_format).replace(
            tzinfo=timezone
        )
        represented_end = datetime.strptime(end_value, self.datetime_format).replace(
            tzinfo=timezone
        )
        if represented_end - represented_start != timedelta(seconds=self.lookback_seconds):
            raise ValueError(
                "rolling collection datetime_format cannot preserve the configured lookback"
            )
        return (
            represented_start.astimezone(UTC),
            {
                self.start_parameter: start_value,
                self.end_parameter: end_value,
            },
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "lookback_seconds": self.lookback_seconds,
            "timezone": self.timezone,
            "start_parameter": self.start_parameter,
            "end_parameter": self.end_parameter,
            "datetime_format": self.datetime_format,
        }


@dataclass(frozen=True, slots=True)
class ProspectiveCollectionPolicy:
    policy_id: str
    capability: ObservationCapability
    sources: tuple[DataSourceBinding, ...]
    window_start: datetime
    parameters_json: str
    poll_interval_seconds: int
    maximum_gap_seconds: int
    rolling_window: ProspectiveRollingWindow | None = None
    schema_version: str = PROSPECTIVE_COLLECTION_POLICY_SCHEMA

    def __post_init__(self) -> None:
        if self.schema_version not in _SUPPORTED_COLLECTION_POLICY_SCHEMAS:
            raise ValueError("unsupported prospective collection policy schema_version")
        if (
            self.schema_version == PROSPECTIVE_COLLECTION_POLICY_SCHEMA_V1
            and self.rolling_window is not None
        ) or (
            self.schema_version == PROSPECTIVE_COLLECTION_POLICY_SCHEMA_V2
            and self.rolling_window is None
        ):
            raise ValueError("prospective collection policy schema and rolling window disagree")
        if not self.sources:
            raise ValueError("prospective collection policy requires at least one source")
        if any(item.source_config_hash is None for item in self.sources):
            raise ValueError("prospective collection policy requires source configuration hashes")
        if len({item.source_key for item in self.sources}) != len(self.sources):
            raise ValueError("prospective collection policy sources must be unique")
        _strict_utc(self.window_start, "prospective collection policy window_start")
        parameters = _object(json.loads(self.parameters_json), "collection parameters")
        if self.parameters_json != canonical_json_bytes(parameters).decode():
            raise ValueError("prospective collection parameters must use canonical JSON")
        if self.poll_interval_seconds < 1:
            raise ValueError("prospective poll_interval_seconds must be positive")
        if self.maximum_gap_seconds < self.poll_interval_seconds:
            raise ValueError(
                "prospective maximum_gap_seconds must not be shorter than the poll interval"
            )
        if self.rolling_window is not None:
            reserved = {
                self.rolling_window.start_parameter,
                self.rolling_window.end_parameter,
            }
            if reserved & set(parameters):
                raise ValueError("rolling collection parameters cannot override window parameters")
            if self.rolling_window.lookback_seconds < self.poll_interval_seconds:
                raise ValueError(
                    "rolling collection lookback must cover at least one poll interval"
                )
        if self.policy_id != self.expected_policy_id:
            raise ValueError("prospective collection policy_id does not match content")

    @property
    def expected_policy_id(self) -> str:
        return f"prospective-collection-policy-{canonical_hash(self.core_dict())}"

    @property
    def parameters(self) -> dict[str, object]:
        return _object(json.loads(self.parameters_json), "collection parameters")

    def core_dict(self) -> dict[str, object]:
        payload: dict[str, object] = {
            "schema_version": self.schema_version,
            "capability": self.capability.value,
            "sources": [item.to_dict() for item in self.sources],
            "window_start": _timestamp(self.window_start),
            "parameters": self.parameters,
            "poll_interval_seconds": self.poll_interval_seconds,
            "maximum_gap_seconds": self.maximum_gap_seconds,
        }
        if self.rolling_window is not None:
            payload["rolling_window"] = self.rolling_window.to_dict()
        return payload

    def resolve_query(self, scheduled_for: datetime) -> tuple[datetime, dict[str, object]]:
        """Resolve immutable request parameters for one logical due opportunity."""

        if self.rolling_window is None:
            return self.window_start, self.parameters
        window_start, window_parameters = self.rolling_window.resolve(scheduled_for)
        return window_start, {**self.parameters, **window_parameters}

    def matches_snapshot_query(
        self,
        *,
        window_start: datetime | None,
        parameters: Mapping[str, object],
    ) -> bool:
        if window_start is None:
            return False
        if self.rolling_window is None:
            return window_start == self.window_start and dict(parameters) == self.parameters
        rolling = self.rolling_window
        start_value = parameters.get(rolling.start_parameter)
        end_value = parameters.get(rolling.end_parameter)
        if not isinstance(start_value, str) or not isinstance(end_value, str):
            return False
        try:
            timezone = ZoneInfo(rolling.timezone)
            start = datetime.strptime(start_value, rolling.datetime_format).replace(tzinfo=timezone)
            end = datetime.strptime(end_value, rolling.datetime_format).replace(tzinfo=timezone)
        except (ValueError, ZoneInfoNotFoundError):
            return False
        base_parameters = dict(parameters)
        base_parameters.pop(rolling.start_parameter, None)
        base_parameters.pop(rolling.end_parameter, None)
        lookback = timedelta(seconds=rolling.lookback_seconds)
        legacy_window_matches = (
            window_start.astimezone(timezone).strftime(rolling.datetime_format) == start_value
            and (window_start + lookback).astimezone(timezone).strftime(rolling.datetime_format)
            == end_value
        )
        return (
            base_parameters == self.parameters
            and end - start == lookback
            and (window_start == start.astimezone(UTC) or legacy_window_matches)
        )

    def to_dict(self) -> dict[str, object]:
        return {**self.core_dict(), "policy_id": self.policy_id}

    @classmethod
    def build(
        cls,
        *,
        capability: ObservationCapability,
        sources: tuple[DataSourceBinding, ...],
        window_start: datetime,
        parameters: Mapping[str, object],
        poll_interval_seconds: int,
        maximum_gap_seconds: int,
        rolling_window: ProspectiveRollingWindow | None = None,
    ) -> ProspectiveCollectionPolicy:
        schema_version = (
            PROSPECTIVE_COLLECTION_POLICY_SCHEMA_V1
            if rolling_window is None
            else PROSPECTIVE_COLLECTION_POLICY_SCHEMA_V2
        )
        core = {
            "schema_version": schema_version,
            "capability": capability.value,
            "sources": [item.to_dict() for item in sources],
            "window_start": _timestamp(window_start),
            "parameters": parameters,
            "poll_interval_seconds": poll_interval_seconds,
            "maximum_gap_seconds": maximum_gap_seconds,
        }
        if rolling_window is not None:
            core["rolling_window"] = rolling_window.to_dict()
        return cls(
            policy_id=f"prospective-collection-policy-{canonical_hash(core)}",
            capability=capability,
            sources=sources,
            window_start=window_start,
            parameters_json=canonical_json_bytes(parameters).decode(),
            poll_interval_seconds=poll_interval_seconds,
            maximum_gap_seconds=maximum_gap_seconds,
            rolling_window=rolling_window,
            schema_version=schema_version,
        )


@dataclass(frozen=True, slots=True)
class JournalAppendResult:
    snapshot_id: str
    policy_id: str
    already_recorded: bool
    new_observation_versions: int
    duplicate_observation_versions: int
    receipt_count: int

    def to_dict(self) -> dict[str, object]:
        return {
            "snapshot_id": self.snapshot_id,
            "policy_id": self.policy_id,
            "already_recorded": self.already_recorded,
            "new_observation_versions": self.new_observation_versions,
            "duplicate_observation_versions": self.duplicate_observation_versions,
            "receipt_count": self.receipt_count,
        }


@dataclass(frozen=True, slots=True)
class ProspectiveObservationVersionRef:
    version_id: str
    first_available_at: datetime
    provider_id: str
    provider_version: str
    upstream_source: str

    def __post_init__(self) -> None:
        prefix = "prospective-observation-version-"
        if not self.version_id.startswith(prefix):
            raise ValueError("prospective observation version identity is invalid")
        _sha256(
            self.version_id[len(prefix) :],
            "prospective observation version identity",
        )
        _strict_utc(self.first_available_at, "prospective observation first_available_at")
        for value in (self.provider_id, self.provider_version, self.upstream_source):
            if not value or value != value.strip():
                raise ValueError("prospective observation source identity is invalid")

    def to_dict(self) -> dict[str, str]:
        return {
            "version_id": self.version_id,
            "first_available_at": _timestamp(self.first_available_at),
            "provider_id": self.provider_id,
            "provider_version": self.provider_version,
            "upstream_source": self.upstream_source,
        }


@dataclass(frozen=True, slots=True)
class ProspectiveDatasetPartition:
    capability: ObservationCapability
    available_date: str
    content_hash: str
    row_count: int
    size_bytes: int
    relative_path: str

    def __post_init__(self) -> None:
        _sha256(self.content_hash, "prospective dataset partition content_hash")
        if self.row_count < 1 or self.size_bytes < 1:
            raise ValueError("prospective dataset partitions must contain data")
        if not self.available_date or not self.relative_path:
            raise ValueError("prospective dataset partition identity is required")

    def to_dict(self) -> dict[str, object]:
        return {
            "capability": self.capability.value,
            "available_date": self.available_date,
            "content_hash": self.content_hash,
            "row_count": self.row_count,
            "size_bytes": self.size_bytes,
            "relative_path": self.relative_path,
        }


@dataclass(frozen=True, slots=True)
class ProspectiveDatasetManifest:
    dataset_id: str
    data_snapshot_id: str
    policy_id: str
    window_start: datetime
    cutoff_at: datetime
    coverage_complete: bool
    observation_count: int
    collection_snapshot_ids: tuple[str, ...]
    partitions: tuple[ProspectiveDatasetPartition, ...]
    parquet_writer: str
    compression: str = "zstd"
    schema_version: str = PROSPECTIVE_DATASET_MANIFEST_SCHEMA

    def __post_init__(self) -> None:
        if self.schema_version != PROSPECTIVE_DATASET_MANIFEST_SCHEMA:
            raise ValueError("unsupported prospective dataset manifest schema_version")
        if not self.data_snapshot_id.startswith("data-snapshot-"):
            raise ValueError("prospective dataset requires a Data Snapshot ID")
        _strict_utc(self.window_start, "prospective dataset window_start")
        _strict_utc(self.cutoff_at, "prospective dataset cutoff_at")
        if self.window_start >= self.cutoff_at:
            raise ValueError("prospective dataset window_start must precede cutoff_at")
        if not self.coverage_complete:
            raise ValueError("prospective dataset requires complete snapshot coverage")
        if self.observation_count < 0:
            raise ValueError("prospective dataset observation_count must be non-negative")
        if sum(item.row_count for item in self.partitions) != self.observation_count:
            raise ValueError("prospective dataset partition rows do not reconcile")
        if len(set(self.collection_snapshot_ids)) != len(self.collection_snapshot_ids):
            raise ValueError("prospective dataset collection snapshots must be unique")
        if self.compression != "zstd":
            raise ValueError("prospective dataset compression must be zstd")
        if not self.parquet_writer:
            raise ValueError("prospective dataset parquet_writer is required")
        if self.dataset_id != self.expected_dataset_id:
            raise ValueError("prospective dataset_id does not match content")

    @property
    def expected_dataset_id(self) -> str:
        return f"prospective-dataset-{canonical_hash(self.core_dict())}"

    def core_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "data_snapshot_id": self.data_snapshot_id,
            "policy_id": self.policy_id,
            "window_start": _timestamp(self.window_start),
            "cutoff_at": _timestamp(self.cutoff_at),
            "coverage_complete": self.coverage_complete,
            "observation_count": self.observation_count,
            "collection_snapshot_ids": list(self.collection_snapshot_ids),
            "partitions": [item.to_dict() for item in self.partitions],
            "parquet_writer": self.parquet_writer,
            "compression": self.compression,
        }

    def to_dict(self) -> dict[str, object]:
        return {**self.core_dict(), "dataset_id": self.dataset_id}


class ProspectiveDataJournal:
    """Append-only actual-receipt index and compressed analytical projection."""

    def __init__(self, store: LocalDataSnapshotStore) -> None:
        self.store = store
        self.index_path = store.index_path
        self.dataset_root = store.root / "datasets"
        self.dataset_root.mkdir(mode=0o700, parents=True, exist_ok=True)
        os.chmod(self.dataset_root, 0o700)
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
                CREATE TABLE IF NOT EXISTS prospective_collection_policies (
                    policy_id TEXT PRIMARY KEY,
                    artifact_hash TEXT NOT NULL UNIQUE
                );
                CREATE TABLE IF NOT EXISTS prospective_collection_snapshots (
                    snapshot_id TEXT PRIMARY KEY,
                    policy_id TEXT NOT NULL REFERENCES prospective_collection_policies(policy_id),
                    query_id TEXT NOT NULL,
                    cutoff_at TEXT NOT NULL,
                    completed_at TEXT NOT NULL,
                    coverage_complete INTEGER NOT NULL
                );
                CREATE INDEX IF NOT EXISTS prospective_snapshots_policy_cutoff
                    ON prospective_collection_snapshots(policy_id, cutoff_at, snapshot_id);
                CREATE TABLE IF NOT EXISTS prospective_source_receipts (
                    snapshot_id TEXT NOT NULL
                        REFERENCES prospective_collection_snapshots(snapshot_id),
                    source_key TEXT NOT NULL,
                    provider_id TEXT NOT NULL,
                    provider_version TEXT NOT NULL,
                    upstream_source TEXT NOT NULL,
                    retrieved_at TEXT NOT NULL,
                    status TEXT NOT NULL,
                    raw_response_hash TEXT,
                    error_kind TEXT,
                    PRIMARY KEY(snapshot_id, source_key)
                );
                CREATE INDEX IF NOT EXISTS prospective_receipts_source_time
                    ON prospective_source_receipts(source_key, retrieved_at, snapshot_id);
                CREATE TABLE IF NOT EXISTS prospective_observation_versions (
                    version_id TEXT PRIMARY KEY,
                    capability TEXT NOT NULL,
                    provider_id TEXT NOT NULL,
                    provider_version TEXT NOT NULL,
                    upstream_source TEXT NOT NULL,
                    upstream_record_id TEXT NOT NULL,
                    lineage_id TEXT NOT NULL,
                    raw_content_hash TEXT NOT NULL,
                    first_available_at TEXT NOT NULL,
                    published_at TEXT,
                    first_snapshot_id TEXT NOT NULL
                        REFERENCES prospective_collection_snapshots(snapshot_id),
                    version_core_json TEXT NOT NULL,
                    observation_json TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS prospective_versions_capability_time
                    ON prospective_observation_versions(
                        capability, provider_id, upstream_source, first_available_at, version_id
                    );
                CREATE INDEX IF NOT EXISTS prospective_versions_lineage
                    ON prospective_observation_versions(
                        provider_id, upstream_source, lineage_id, first_available_at
                    );
                CREATE TABLE IF NOT EXISTS prospective_observation_sightings (
                    snapshot_id TEXT NOT NULL
                        REFERENCES prospective_collection_snapshots(snapshot_id),
                    version_id TEXT NOT NULL
                        REFERENCES prospective_observation_versions(version_id),
                    observation_id TEXT NOT NULL,
                    retrieved_at TEXT NOT NULL,
                    PRIMARY KEY(snapshot_id, version_id)
                );
                CREATE TABLE IF NOT EXISTS prospective_dataset_manifests (
                    dataset_id TEXT PRIMARY KEY,
                    data_snapshot_id TEXT NOT NULL UNIQUE,
                    policy_id TEXT NOT NULL REFERENCES prospective_collection_policies(policy_id),
                    cutoff_at TEXT NOT NULL,
                    artifact_hash TEXT NOT NULL UNIQUE,
                    observation_count INTEGER NOT NULL
                );
                CREATE INDEX IF NOT EXISTS prospective_datasets_policy_cutoff
                    ON prospective_dataset_manifests(policy_id, cutoff_at, dataset_id);
                """
            )

    def record_snapshot(
        self,
        snapshot: DataSnapshot,
        *,
        policy: ProspectiveCollectionPolicy,
    ) -> JournalAppendResult:
        if snapshot.query.pit_lane is not DataPITLane.PROSPECTIVE:
            raise ValueError("prospective journal accepts only prospective Data Snapshots")
        if snapshot.query.capability is not policy.capability:
            raise ValueError("prospective collection policy capability mismatch")
        if snapshot.query.source_policy_id != policy.policy_id:
            raise ValueError("Data Snapshot source policy does not match collection policy")
        if snapshot.query.sources != policy.sources:
            raise ValueError("Data Snapshot sources do not match collection policy")
        if not policy.matches_snapshot_query(
            window_start=snapshot.query.window_start,
            parameters=snapshot.query.parameters,
        ):
            raise ValueError("Data Snapshot request does not match collection policy")
        stored = self.store.get(snapshot.snapshot_id)
        if stored != snapshot:
            raise ValueError("prospective journal requires the persisted immutable Data Snapshot")
        policy_artifact = self.store.artifacts.put_json(policy.to_dict())

        new_versions = 0
        duplicate_versions = 0
        with self._connect() as connection:
            existing_snapshot = connection.execute(
                "SELECT policy_id FROM prospective_collection_snapshots WHERE snapshot_id = ?",
                (snapshot.snapshot_id,),
            ).fetchone()
            if existing_snapshot is not None:
                if cast(str, existing_snapshot["policy_id"]) != policy.policy_id:
                    raise ValueError("collection snapshot is already bound to another policy")
                duplicate_count = cast(
                    int,
                    connection.execute(
                        """
                        SELECT COUNT(*) AS count
                        FROM prospective_observation_sightings WHERE snapshot_id = ?
                        """,
                        (snapshot.snapshot_id,),
                    ).fetchone()["count"],
                )
                return JournalAppendResult(
                    snapshot_id=snapshot.snapshot_id,
                    policy_id=policy.policy_id,
                    already_recorded=True,
                    new_observation_versions=0,
                    duplicate_observation_versions=duplicate_count,
                    receipt_count=len(snapshot.attempts),
                )

            self._record_policy(connection, policy, policy_artifact.content_hash)
            connection.execute(
                """
                INSERT INTO prospective_collection_snapshots(
                    snapshot_id, policy_id, query_id, cutoff_at, completed_at, coverage_complete
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    snapshot.snapshot_id,
                    policy.policy_id,
                    snapshot.query.query_id,
                    _timestamp(snapshot.query.as_of),
                    _timestamp(snapshot.completed_at),
                    int(snapshot.coverage_complete),
                ),
            )
            for source, attempt in zip(policy.sources, snapshot.attempts, strict=True):
                if attempt.raw_response_hash is not None:
                    self.store.artifacts.get(
                        attempt.raw_response_hash,
                        media_type="application/octet-stream",
                    )
                connection.execute(
                    """
                    INSERT INTO prospective_source_receipts(
                        snapshot_id, source_key, provider_id, provider_version,
                        upstream_source, retrieved_at, status, raw_response_hash, error_kind
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        snapshot.snapshot_id,
                        source.source_key,
                        attempt.provider_id,
                        attempt.provider_version,
                        attempt.upstream_source,
                        _timestamp(attempt.retrieved_at),
                        attempt.status.value,
                        attempt.raw_response_hash,
                        attempt.error_kind,
                    ),
                )

            for observation in snapshot.observations:
                self.store.artifacts.get(
                    observation.raw_content_hash,
                    media_type="application/octet-stream",
                )
                version_core = _observation_version_core(observation)
                version_core_json = canonical_json_bytes(version_core).decode()
                version_id = prospective_observation_version_id(observation)
                available_at = observation.times.available_at
                if available_at is None:
                    raise ValueError("prospective journal observations require availability")
                existing = connection.execute(
                    """
                    SELECT version_core_json, first_available_at
                    FROM prospective_observation_versions WHERE version_id = ?
                    """,
                    (version_id,),
                ).fetchone()
                if existing is None:
                    connection.execute(
                        """
                        INSERT INTO prospective_observation_versions(
                            version_id, capability, provider_id, provider_version,
                            upstream_source, upstream_record_id, lineage_id, raw_content_hash,
                            first_available_at, published_at, first_snapshot_id,
                            version_core_json, observation_json
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            version_id,
                            observation.capability.value,
                            observation.provider_id,
                            observation.provider_version,
                            observation.upstream_source,
                            observation.upstream_record_id,
                            observation.lineage_id,
                            observation.raw_content_hash,
                            _timestamp(available_at),
                            _optional_timestamp(observation.times.published_at),
                            snapshot.snapshot_id,
                            version_core_json,
                            canonical_json_bytes(observation.to_dict()).decode(),
                        ),
                    )
                    new_versions += 1
                else:
                    if cast(str, existing["version_core_json"]) != version_core_json:
                        raise ValueError("prospective observation version has conflicting content")
                    if available_at < _datetime(
                        cast(str, existing["first_available_at"]),
                        "first_available_at",
                    ):
                        raise ValueError("prospective observation versions cannot be backdated")
                    duplicate_versions += 1
                connection.execute(
                    """
                    INSERT INTO prospective_observation_sightings(
                        snapshot_id, version_id, observation_id, retrieved_at
                    ) VALUES (?, ?, ?, ?)
                    """,
                    (
                        snapshot.snapshot_id,
                        version_id,
                        observation.observation_id,
                        _timestamp(observation.times.retrieved_at),
                    ),
                )

        return JournalAppendResult(
            snapshot_id=snapshot.snapshot_id,
            policy_id=policy.policy_id,
            already_recorded=False,
            new_observation_versions=new_versions,
            duplicate_observation_versions=duplicate_versions,
            receipt_count=len(snapshot.attempts),
        )

    def register_policy(self, policy: ProspectiveCollectionPolicy) -> None:
        """Persist a collection policy before its first scheduled receipt."""

        policy_artifact = self.store.artifacts.put_json(policy.to_dict())
        with self._connect() as connection:
            self._record_policy(connection, policy, policy_artifact.content_hash)

    def policy(self, policy_id: str) -> ProspectiveCollectionPolicy:
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT artifact_hash FROM prospective_collection_policies WHERE policy_id = ?
                """,
                (policy_id,),
            ).fetchone()
        if row is None:
            raise KeyError(f"unknown prospective collection policy: {policy_id}")
        return prospective_collection_policy_from_dict(
            self.store.artifacts.read_json(cast(str, row["artifact_hash"]))
        )

    def observation_version_refs(
        self,
        *,
        policy_id: str,
        capability: ObservationCapability,
        not_before: datetime,
        not_after: datetime,
    ) -> tuple[ProspectiveObservationVersionRef, ...]:
        """Return content identities visible in one policy window without exposing payloads."""

        policy = self.policy(policy_id)
        if policy.capability is not capability:
            raise ValueError("prospective observation query capability does not match policy")
        _strict_utc(not_before, "prospective observation query not_before")
        _strict_utc(not_after, "prospective observation query not_after")
        if not_before > not_after:
            raise ValueError("prospective observation query window is inverted")
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT DISTINCT version.version_id, version.first_available_at,
                                version.provider_id, version.provider_version,
                                version.upstream_source
                FROM prospective_observation_versions AS version
                JOIN prospective_observation_sightings AS sighting
                  ON sighting.version_id = version.version_id
                JOIN prospective_collection_snapshots AS snapshot
                  ON snapshot.snapshot_id = sighting.snapshot_id
                WHERE snapshot.policy_id = ?
                  AND version.capability = ?
                  AND version.first_available_at >= ?
                  AND version.first_available_at <= ?
                ORDER BY version.first_available_at, version.version_id
                """,
                (
                    policy_id,
                    capability.value,
                    _timestamp(not_before),
                    _timestamp(not_after),
                ),
            ).fetchall()
        return tuple(
            ProspectiveObservationVersionRef(
                version_id=cast(str, row["version_id"]),
                first_available_at=_datetime(
                    cast(str, row["first_available_at"]), "first_available_at"
                ),
                provider_id=cast(str, row["provider_id"]),
                provider_version=cast(str, row["provider_version"]),
                upstream_source=cast(str, row["upstream_source"]),
            )
            for row in rows
        )

    def observation_version_refs_by_ids(
        self,
        version_ids: tuple[str, ...],
    ) -> tuple[ProspectiveObservationVersionRef, ...]:
        """Resolve exact Journal versions in stable actual-receipt order."""

        if not version_ids or len(set(version_ids)) != len(version_ids):
            raise ValueError("prospective version selection must be non-empty and unique")
        placeholders = ",".join("?" for _ in version_ids)
        with self._connect() as connection:
            rows = connection.execute(
                f"""
                SELECT version_id, first_available_at, provider_id, provider_version,
                       upstream_source
                FROM prospective_observation_versions
                WHERE version_id IN ({placeholders})
                ORDER BY first_available_at, version_id
                """,
                version_ids,
            ).fetchall()
        if {cast(str, row["version_id"]) for row in rows} != set(version_ids):
            raise KeyError("prospective version selection contains an unknown Journal version")
        return tuple(
            ProspectiveObservationVersionRef(
                version_id=cast(str, row["version_id"]),
                first_available_at=_datetime(
                    cast(str, row["first_available_at"]), "first_available_at"
                ),
                provider_id=cast(str, row["provider_id"]),
                provider_version=cast(str, row["provider_version"]),
                upstream_source=cast(str, row["upstream_source"]),
            )
            for row in rows
        )

    def freeze_version_selection_snapshot(
        self,
        *,
        selection_id: str,
        readiness_report_id: str,
        version_ids: tuple[str, ...],
        as_of: datetime,
        frozen_at: datetime,
    ) -> DataSnapshot:
        """Freeze exact Journal versions across accepted collection policies.

        This is a selection Snapshot for formal triage. It performs no network acquisition and
        cannot introduce a version that was not already stored with actual-receipt authority.
        """

        for value, label in (
            (selection_id, "prospective version selection_id"),
            (readiness_report_id, "prospective version readiness_report_id"),
        ):
            if not value or value != value.strip():
                raise ValueError(f"{label} must be non-empty trimmed text")
        _strict_utc(as_of, "prospective version selection as_of")
        _strict_utc(frozen_at, "prospective version selection frozen_at")
        if frozen_at < as_of:
            raise ValueError("prospective version selection cannot freeze before its cutoff")
        refs = self.observation_version_refs_by_ids(version_ids)
        if version_ids != tuple(item.version_id for item in refs):
            raise ValueError("prospective version selection must use stable actual-receipt order")
        if any(item.first_available_at > as_of for item in refs):
            raise ValueError("prospective version selection contains a post-cutoff version")
        requested_as_of = as_of
        effective_cutoff = max(item.first_available_at for item in refs)

        placeholders = ",".join("?" for _ in version_ids)
        with self._connect() as connection:
            rows = connection.execute(
                f"""
                SELECT version_id, first_snapshot_id, observation_json
                FROM prospective_observation_versions
                WHERE version_id IN ({placeholders})
                """,
                version_ids,
            ).fetchall()
        by_id = {
            cast(str, row["version_id"]): (
                cast(str, row["first_snapshot_id"]),
                _observation_from_json(cast(str, row["observation_json"])),
            )
            for row in rows
        }
        observations = tuple(by_id[version_id][1] for version_id in version_ids)

        binding_by_key: dict[str, DataSourceBinding] = {}
        observations_by_key: dict[str, list[SourceObservation]] = defaultdict(list)
        for version_id, observation in zip(version_ids, observations, strict=True):
            first_snapshot = self.store.get(by_id[version_id][0])
            binding = next(
                (
                    item
                    for item in first_snapshot.query.sources
                    if item.provider_id == observation.provider_id
                    and item.provider_version == observation.provider_version
                    and item.upstream_source == observation.upstream_source
                ),
                None,
            )
            if binding is None:
                raise ValueError("Journal version lacks its original source binding")
            existing = binding_by_key.get(binding.source_key)
            if existing is not None and existing != binding:
                raise ValueError("one triage source key has conflicting accepted bindings")
            binding_by_key[binding.source_key] = binding
            observations_by_key[binding.source_key].append(observation)

        sources = tuple(binding_by_key[key] for key in sorted(binding_by_key))
        earliest = min(item.first_available_at for item in refs)
        window_start = (
            earliest if earliest < effective_cutoff else earliest - timedelta(microseconds=1)
        )
        query = DataQuery.build(
            capability=ObservationCapability.EVENT_REVELATION,
            pit_lane=DataPITLane.PROSPECTIVE,
            as_of=effective_cutoff,
            window_start=window_start,
            source_policy_id=selection_id,
            parameters={
                "selection_id": selection_id,
                "readiness_report_id": readiness_report_id,
                "version_count": len(version_ids),
                "requested_as_of": _timestamp(requested_as_of),
            },
            sources=sources,
            minimum_data_sources=len(sources),
        )
        attempts: list[DataProviderAttempt] = []
        for source in sources:
            selected = observations_by_key[source.source_key]
            selected_ids = tuple(
                version_id
                for version_id, observation in zip(version_ids, observations, strict=True)
                if observation.provider_id == source.provider_id
                and observation.provider_version == source.provider_version
                and observation.upstream_source == source.upstream_source
            )
            selection_payload = {
                "schema_version": "market-impact.prospective-version-selection.v1",
                "selection_id": selection_id,
                "readiness_report_id": readiness_report_id,
                "source": source.to_dict(),
                "version_ids": list(selected_ids),
                "requested_as_of": _timestamp(requested_as_of),
                "effective_cutoff_at": _timestamp(effective_cutoff),
                "frozen_at": _timestamp(frozen_at),
            }
            attempts.append(
                DataProviderAttempt(
                    provider_id=source.provider_id,
                    provider_version=source.provider_version,
                    upstream_source=source.upstream_source,
                    required=source.required,
                    status=DataFetchStatus.DATA,
                    retrieved_at=max(item.times.retrieved_at for item in selected),
                    raw_response_hash=self.store.put_raw(canonical_json_bytes(selection_payload)),
                    received_count=len(selected),
                    accepted_count=len(selected),
                    rejected_missing_availability=0,
                    rejected_after_cutoff=0,
                    rejected_missing_authority=0,
                    rejected_authority_after_cutoff=0,
                    rejected_lane_mismatch=0,
                    error_kind=None,
                )
            )
        attempt_tuple = tuple(attempts)
        core = {
            "schema_version": DATA_SNAPSHOT_SCHEMA,
            "query": query.to_dict(),
            "attempts": [item.to_dict() for item in attempt_tuple],
            "observations": [item.to_dict() for item in observations],
            "coverage_complete": True,
            "completed_at": _timestamp(max(item.retrieved_at for item in attempt_tuple)),
        }
        snapshot = DataSnapshot(
            snapshot_id=f"data-snapshot-{canonical_hash(core)}",
            query=query,
            attempts=attempt_tuple,
            observations=observations,
            coverage_complete=True,
            completed_at=max(item.retrieved_at for item in attempt_tuple),
        )
        self.store.put(snapshot)
        return snapshot

    def freeze_snapshot(
        self,
        *,
        policy_id: str,
        not_after: datetime,
        window_start: datetime,
        minimum_data_sources: int | None = None,
        frozen_at: datetime | None = None,
    ) -> DataSnapshot:
        policy = self.policy(policy_id)
        _strict_utc(not_after, "prospective freeze not_after")
        _strict_utc(window_start, "prospective freeze window_start")
        if window_start >= not_after:
            raise ValueError("prospective freeze window_start must be before not_after")
        freeze_operation_at = datetime.now(UTC) if frozen_at is None else frozen_at
        _strict_utc(freeze_operation_at, "prospective freeze frozen_at")
        if freeze_operation_at < not_after:
            raise ValueError("prospective data cannot be frozen before its requested upper bound")
        minimum = len(policy.sources) if minimum_data_sources is None else minimum_data_sources
        coverage: list[tuple[DataSourceBinding, tuple[sqlite3.Row, ...], str | None]] = []
        for source in policy.sources:
            receipts, error = self._coverage_receipts(
                source=source,
                policy=policy,
                window_start=window_start,
                not_after=not_after,
            )
            coverage.append((source, receipts, error))
        successful_receipt_times = [
            _datetime(cast(str, receipts[-1]["retrieved_at"]), "retrieved_at")
            for _, receipts, error in coverage
            if error is None
        ]
        effective_cutoff = max(successful_receipt_times) if successful_receipt_times else not_after
        query = DataQuery.build(
            capability=policy.capability,
            pit_lane=DataPITLane.PROSPECTIVE,
            as_of=effective_cutoff,
            window_start=window_start,
            source_policy_id=policy.policy_id,
            parameters={
                "collection_policy_id": policy.policy_id,
                "requested_not_after": _timestamp(not_after),
            },
            sources=policy.sources,
            minimum_data_sources=minimum,
        )
        attempts: list[DataProviderAttempt] = []
        accepted: list[SourceObservation] = []
        for source, coverage_receipts, coverage_error in coverage:
            if coverage_error is not None:
                attempts.append(
                    DataProviderAttempt(
                        provider_id=source.provider_id,
                        provider_version=source.provider_version,
                        upstream_source=source.upstream_source,
                        required=source.required,
                        status=DataFetchStatus.ERROR,
                        retrieved_at=freeze_operation_at,
                        raw_response_hash=None,
                        received_count=0,
                        accepted_count=0,
                        rejected_missing_availability=0,
                        rejected_after_cutoff=0,
                        rejected_missing_authority=0,
                        rejected_authority_after_cutoff=0,
                        rejected_lane_mismatch=0,
                        error_kind=coverage_error,
                    )
                )
                continue
            rows = self._observation_rows(
                policy_id=policy.policy_id,
                capability=policy.capability,
                source=source,
                window_start=window_start,
                as_of=effective_cutoff,
            )
            observations = tuple(
                _observation_from_json(cast(str, row["observation_json"])) for row in rows
            )
            selection = {
                "schema_version": PROSPECTIVE_JOURNAL_SELECTION_SCHEMA,
                "policy_id": policy.policy_id,
                "query_id": query.query_id,
                "source": source.to_dict(),
                "window_start": _timestamp(window_start),
                "requested_not_after": _timestamp(not_after),
                "effective_cutoff_at": _timestamp(effective_cutoff),
                "frozen_at": _timestamp(freeze_operation_at),
                "receipt_snapshot_ids": [
                    cast(str, item["snapshot_id"]) for item in coverage_receipts
                ],
                "observation_version_ids": [cast(str, item["version_id"]) for item in rows],
            }
            raw_hash = self.store.put_raw(canonical_json_bytes(selection))
            accepted.extend(observations)
            attempts.append(
                DataProviderAttempt(
                    provider_id=source.provider_id,
                    provider_version=source.provider_version,
                    upstream_source=source.upstream_source,
                    required=source.required,
                    status=(DataFetchStatus.DATA if observations else DataFetchStatus.NO_DATA),
                    retrieved_at=_datetime(
                        cast(str, coverage_receipts[-1]["retrieved_at"]),
                        "retrieved_at",
                    ),
                    raw_response_hash=raw_hash,
                    received_count=len(observations),
                    accepted_count=len(observations),
                    rejected_missing_availability=0,
                    rejected_after_cutoff=0,
                    rejected_missing_authority=0,
                    rejected_authority_after_cutoff=0,
                    rejected_lane_mismatch=0,
                    error_kind=None,
                )
            )

        attempt_tuple = tuple(attempts)
        observation_tuple = tuple(
            sorted(
                accepted,
                key=lambda item: (
                    item.times.available_at or item.times.retrieved_at,
                    item.provider_id,
                    item.upstream_source,
                    item.lineage_id,
                    item.observation_id,
                ),
            )
        )
        coverage_complete = data_snapshot_coverage_complete(query, attempt_tuple)
        completed_at = max(item.retrieved_at for item in attempt_tuple)
        core = {
            "schema_version": DATA_SNAPSHOT_SCHEMA,
            "query": query.to_dict(),
            "attempts": [item.to_dict() for item in attempt_tuple],
            "observations": [item.to_dict() for item in observation_tuple],
            "coverage_complete": coverage_complete,
            "completed_at": _timestamp(completed_at),
        }
        snapshot = DataSnapshot(
            snapshot_id=f"data-snapshot-{canonical_hash(core)}",
            query=query,
            attempts=attempt_tuple,
            observations=observation_tuple,
            coverage_complete=coverage_complete,
            completed_at=completed_at,
        )
        self.store.put(snapshot)
        return snapshot

    def assert_frozen_snapshot(self, snapshot: DataSnapshot) -> None:
        """Require a complete aggregate produced from this journal's receipt selections."""

        if not snapshot.coverage_complete:
            raise ValueError("prospective journal baseline requires complete coverage")
        if snapshot.query.pit_lane is not DataPITLane.PROSPECTIVE:
            raise ValueError("prospective journal baseline requires the prospective PIT lane")
        if snapshot.query.window_start is None:
            raise ValueError("prospective journal baseline requires a bounded window")
        policy = self.policy(snapshot.query.source_policy_id)
        if snapshot.query.window_start != policy.window_start:
            raise ValueError("prospective journal baseline window does not match policy")
        if snapshot.query.capability is not policy.capability:
            raise ValueError("prospective journal baseline capability does not match policy")
        if snapshot.query.sources != policy.sources:
            raise ValueError("prospective journal baseline sources do not match policy")
        parameters = snapshot.query.parameters
        if parameters.get("collection_policy_id") != policy.policy_id:
            raise ValueError("prospective journal baseline is not a Journal freeze")
        requested_not_after = parameters.get("requested_not_after")
        if not isinstance(requested_not_after, str) or not requested_not_after.endswith("Z"):
            raise ValueError("prospective journal baseline cutoff is missing")
        collection_snapshot_ids = self._selection_collection_snapshot_ids(snapshot)
        if not collection_snapshot_ids:
            raise ValueError("prospective journal baseline has no receipt selections")
        placeholders = ",".join("?" for _ in collection_snapshot_ids)
        with self._connect() as connection:
            rows = connection.execute(
                f"""
                SELECT snapshot_id FROM prospective_collection_snapshots
                WHERE policy_id = ? AND snapshot_id IN ({placeholders})
                """,
                (policy.policy_id, *collection_snapshot_ids),
            ).fetchall()
        if {cast(str, row["snapshot_id"]) for row in rows} != set(collection_snapshot_ids):
            raise ValueError("prospective journal baseline receipts do not match policy")
        if snapshot.observations:
            self._dataset_rows_for_snapshot(snapshot)

    def materialize_snapshot_parquet(
        self,
        *,
        snapshot_id: str,
    ) -> ProspectiveDatasetManifest:
        snapshot = self.store.get(snapshot_id)
        if not snapshot.coverage_complete:
            raise ValueError("prospective dataset requires a complete frozen Data Snapshot")
        if snapshot.query.pit_lane is not DataPITLane.PROSPECTIVE:
            raise ValueError("prospective dataset requires a prospective Data Snapshot")
        if snapshot.query.window_start is None:
            raise ValueError("prospective dataset requires a bounded Snapshot window")
        policy = self.policy(snapshot.query.source_policy_id)
        if snapshot.query.sources != policy.sources:
            raise ValueError("prospective dataset Snapshot sources do not match policy")
        rows = self._dataset_rows_for_snapshot(snapshot)
        collection_snapshots = self._selection_collection_snapshot_ids(snapshot)
        arrow, parquet = _pyarrow_modules()
        grouped: dict[str, list[dict[str, object]]] = defaultdict(list)
        for row in rows:
            available_at = cast(str, row["first_available_at"])
            grouped[available_at[:10]].append(_parquet_row(row))
        partitions: list[ProspectiveDatasetPartition] = []
        for available_date in sorted(grouped):
            partition_rows = sorted(
                grouped[available_date],
                key=lambda item: cast(str, item["version_id"]),
            )
            table = arrow.Table.from_pylist(partition_rows, schema=_parquet_schema(arrow))
            relative_directory = Path(policy.capability.value) / f"available_date={available_date}"
            destination_directory = self.dataset_root / "parquet" / relative_directory
            destination_directory.mkdir(mode=0o700, parents=True, exist_ok=True)
            descriptor, temporary_name = tempfile.mkstemp(
                prefix=".tmp-prospective-",
                suffix=".parquet",
                dir=destination_directory,
            )
            os.close(descriptor)
            temporary_path = Path(temporary_name)
            try:
                parquet.write_table(
                    table,
                    temporary_path,
                    compression="zstd",
                    compression_level=6,
                    use_dictionary=True,
                    write_statistics=True,
                    row_group_size=65_536,
                )
                if parquet.ParquetFile(temporary_path).metadata.num_rows != len(partition_rows):
                    raise ValueError("prospective Parquet row count mismatch")
                content_hash = sha256(temporary_path.read_bytes()).hexdigest()
                destination = destination_directory / f"{content_hash}.parquet"
                if destination.exists():
                    if sha256(destination.read_bytes()).hexdigest() != content_hash:
                        raise ValueError("prospective Parquet content identity conflict")
                    temporary_path.unlink()
                else:
                    temporary_path.replace(destination)
                    os.chmod(destination, 0o600)
            finally:
                temporary_path.unlink(missing_ok=True)
            relative_path = destination.relative_to(self.dataset_root).as_posix()
            partitions.append(
                ProspectiveDatasetPartition(
                    capability=policy.capability,
                    available_date=available_date,
                    content_hash=content_hash,
                    row_count=len(partition_rows),
                    size_bytes=destination.stat().st_size,
                    relative_path=relative_path,
                )
            )
        core = {
            "schema_version": PROSPECTIVE_DATASET_MANIFEST_SCHEMA,
            "data_snapshot_id": snapshot.snapshot_id,
            "policy_id": policy.policy_id,
            "window_start": _timestamp(snapshot.query.window_start),
            "cutoff_at": _timestamp(snapshot.query.as_of),
            "coverage_complete": True,
            "observation_count": len(rows),
            "collection_snapshot_ids": list(collection_snapshots),
            "partitions": [item.to_dict() for item in partitions],
            "parquet_writer": f"pyarrow-{arrow.__version__}",
            "compression": "zstd",
        }
        manifest = ProspectiveDatasetManifest(
            dataset_id=f"prospective-dataset-{canonical_hash(core)}",
            data_snapshot_id=snapshot.snapshot_id,
            policy_id=policy.policy_id,
            window_start=snapshot.query.window_start,
            cutoff_at=snapshot.query.as_of,
            coverage_complete=True,
            observation_count=len(rows),
            collection_snapshot_ids=collection_snapshots,
            partitions=tuple(partitions),
            parquet_writer=f"pyarrow-{arrow.__version__}",
        )
        artifact = self.store.artifacts.put_json(manifest.to_dict())
        with self._connect() as connection:
            existing = connection.execute(
                """
                SELECT artifact_hash FROM prospective_dataset_manifests WHERE dataset_id = ?
                """,
                (manifest.dataset_id,),
            ).fetchone()
            if existing is not None:
                if cast(str, existing["artifact_hash"]) != artifact.content_hash:
                    raise ValueError("prospective dataset identity has conflicting content")
            else:
                connection.execute(
                    """
                    INSERT INTO prospective_dataset_manifests(
                        dataset_id, data_snapshot_id, policy_id, cutoff_at,
                        artifact_hash, observation_count
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        manifest.dataset_id,
                        snapshot.snapshot_id,
                        policy.policy_id,
                        _timestamp(snapshot.query.as_of),
                        artifact.content_hash,
                        manifest.observation_count,
                    ),
                )
        return manifest

    def stats(self, *, policy_id: str) -> dict[str, object]:
        self.policy(policy_id)
        with self._connect() as connection:
            snapshots = cast(
                int,
                connection.execute(
                    """
                    SELECT COUNT(*) AS count FROM prospective_collection_snapshots
                    WHERE policy_id = ?
                    """,
                    (policy_id,),
                ).fetchone()["count"],
            )
            receipts = cast(
                int,
                connection.execute(
                    """
                    SELECT COUNT(*) AS count
                    FROM prospective_source_receipts AS receipt
                    JOIN prospective_collection_snapshots AS snapshot
                      ON snapshot.snapshot_id = receipt.snapshot_id
                    WHERE snapshot.policy_id = ?
                    """,
                    (policy_id,),
                ).fetchone()["count"],
            )
            sightings = cast(
                int,
                connection.execute(
                    """
                    SELECT COUNT(*) AS count
                    FROM prospective_observation_sightings AS sighting
                    JOIN prospective_collection_snapshots AS snapshot
                      ON snapshot.snapshot_id = sighting.snapshot_id
                    WHERE snapshot.policy_id = ?
                    """,
                    (policy_id,),
                ).fetchone()["count"],
            )
            versions = cast(
                int,
                connection.execute(
                    """
                    SELECT COUNT(DISTINCT sighting.version_id) AS count
                    FROM prospective_observation_sightings AS sighting
                    JOIN prospective_collection_snapshots AS snapshot
                      ON snapshot.snapshot_id = sighting.snapshot_id
                    WHERE snapshot.policy_id = ?
                    """,
                    (policy_id,),
                ).fetchone()["count"],
            )
        return {
            "policy_id": policy_id,
            "collection_snapshot_count": snapshots,
            "source_receipt_count": receipts,
            "observation_sighting_count": sightings,
            "unique_observation_version_count": versions,
            "deduplicated_observation_count": sightings - versions,
        }

    def _record_policy(
        self,
        connection: sqlite3.Connection,
        policy: ProspectiveCollectionPolicy,
        artifact_hash: str,
    ) -> None:
        existing = connection.execute(
            """
            SELECT artifact_hash FROM prospective_collection_policies WHERE policy_id = ?
            """,
            (policy.policy_id,),
        ).fetchone()
        if existing is None:
            connection.execute(
                """
                INSERT INTO prospective_collection_policies(policy_id, artifact_hash)
                VALUES (?, ?)
                """,
                (policy.policy_id, artifact_hash),
            )
        elif cast(str, existing["artifact_hash"]) != artifact_hash:
            raise ValueError("prospective collection policy identity conflict")

    def _coverage_receipts(
        self,
        *,
        source: DataSourceBinding,
        policy: ProspectiveCollectionPolicy,
        window_start: datetime,
        not_after: datetime,
    ) -> tuple[tuple[sqlite3.Row, ...], str | None]:
        margin = timedelta(seconds=policy.maximum_gap_seconds)
        with self._connect() as connection:
            rows = tuple(
                connection.execute(
                    """
                    SELECT receipt.*, snapshot.policy_id
                    FROM prospective_source_receipts AS receipt
                    JOIN prospective_collection_snapshots AS snapshot
                      ON snapshot.snapshot_id = receipt.snapshot_id
                    WHERE snapshot.policy_id = ?
                      AND receipt.source_key = ?
                      AND receipt.retrieved_at >= ?
                      AND receipt.retrieved_at <= ?
                    ORDER BY receipt.retrieved_at, receipt.snapshot_id
                    """,
                    (
                        policy.policy_id,
                        source.source_key,
                        _timestamp(window_start - margin),
                        _timestamp(not_after),
                    ),
                ).fetchall()
            )
        if not rows:
            return (), "journal_no_receipt_before_cutoff"
        times = tuple(_datetime(cast(str, row["retrieved_at"]), "retrieved_at") for row in rows)
        first_gap = abs((times[0] - window_start).total_seconds())
        if first_gap > policy.maximum_gap_seconds:
            return rows, "journal_start_coverage_gap"
        if any(
            (later - earlier).total_seconds() > policy.maximum_gap_seconds
            for earlier, later in pairwise(times)
        ):
            return rows, "journal_internal_coverage_gap"
        if (not_after - times[-1]).total_seconds() > policy.maximum_gap_seconds:
            return rows, "journal_cutoff_coverage_gap"
        if any(DataFetchStatus(cast(str, row["status"])).completed is False for row in rows):
            return rows, "journal_failed_source_receipt"
        return rows, None

    def _observation_rows(
        self,
        *,
        policy_id: str,
        capability: ObservationCapability,
        source: DataSourceBinding,
        window_start: datetime,
        as_of: datetime,
    ) -> tuple[sqlite3.Row, ...]:
        with self._connect() as connection:
            return tuple(
                connection.execute(
                    """
                    SELECT DISTINCT version.version_id, version.observation_json,
                           version.first_available_at
                    FROM prospective_observation_versions AS version
                    JOIN prospective_observation_sightings AS sighting
                      ON sighting.version_id = version.version_id
                    JOIN prospective_collection_snapshots AS snapshot
                      ON snapshot.snapshot_id = sighting.snapshot_id
                    WHERE snapshot.policy_id = ?
                      AND version.capability = ?
                      AND version.provider_id = ?
                      AND version.provider_version = ?
                      AND version.upstream_source = ?
                      AND version.first_available_at >= ?
                      AND version.first_available_at <= ?
                    ORDER BY version.first_available_at, version.version_id
                    """,
                    (
                        policy_id,
                        capability.value,
                        source.provider_id,
                        source.provider_version,
                        source.upstream_source,
                        _timestamp(window_start),
                        _timestamp(as_of),
                    ),
                ).fetchall()
            )

    def _dataset_rows_for_snapshot(
        self,
        snapshot: DataSnapshot,
    ) -> tuple[sqlite3.Row, ...]:
        version_ids = tuple(
            prospective_observation_version_id(item) for item in snapshot.observations
        )
        placeholders = ",".join("?" for _ in version_ids)
        with self._connect() as connection:
            rows = tuple(
                connection.execute(
                    f"""
                    SELECT * FROM prospective_observation_versions
                    WHERE version_id IN ({placeholders})
                    ORDER BY first_available_at, version_id
                    """,
                    version_ids,
                ).fetchall()
            )
        if {cast(str, row["version_id"]) for row in rows} != set(version_ids):
            raise ValueError("frozen Data Snapshot versions are missing from the journal")
        return rows

    def _selection_collection_snapshot_ids(
        self,
        snapshot: DataSnapshot,
    ) -> tuple[str, ...]:
        selected: set[str] = set()
        for attempt in snapshot.attempts:
            if attempt.raw_response_hash is None:
                raise ValueError("complete frozen Snapshot attempts require selection artifacts")
            artifact = self.store.artifacts.get(
                attempt.raw_response_hash,
                media_type="application/octet-stream",
            )
            payload = _object(
                json.loads(artifact.path.read_bytes()),
                "prospective journal selection",
            )
            if (
                _string(payload, "schema_version") != PROSPECTIVE_JOURNAL_SELECTION_SCHEMA
                or _string(payload, "policy_id") != snapshot.query.source_policy_id
                or _string(payload, "query_id") != snapshot.query.query_id
            ):
                raise ValueError("prospective journal selection identity mismatch")
            selected.update(
                _required_strings(payload.get("receipt_snapshot_ids"), "receipt_snapshot_ids")
            )
        return tuple(sorted(selected))


def prospective_collection_policy_from_dict(value: object) -> ProspectiveCollectionPolicy:
    payload = _object(value, "prospective collection policy")
    schema_version = _string(payload, "schema_version")
    rolling_payload = payload.get("rolling_window")
    rolling_window: ProspectiveRollingWindow | None = None
    if rolling_payload is not None:
        rolling = _object(rolling_payload, "rolling collection window")
        if set(rolling) != {
            "lookback_seconds",
            "timezone",
            "start_parameter",
            "end_parameter",
            "datetime_format",
        }:
            raise ValueError("rolling collection window fields are not canonical")
        rolling_window = ProspectiveRollingWindow(
            lookback_seconds=_integer(rolling, "lookback_seconds"),
            timezone=_string(rolling, "timezone"),
            start_parameter=_string(rolling, "start_parameter"),
            end_parameter=_string(rolling, "end_parameter"),
            datetime_format=_string(rolling, "datetime_format"),
        )
    sources = tuple(
        DataSourceBinding(
            provider_id=_string(item, "provider_id"),
            provider_version=_string(item, "provider_version"),
            upstream_source=_string(item, "upstream_source"),
            manifest_hash=_string(item, "manifest_hash"),
            source_config_hash=_string(item, "source_config_hash"),
            required=_boolean(item, "required"),
        )
        for item in (
            _object(raw, "prospective collection source")
            for raw in _list(payload.get("sources"), "sources")
        )
    )
    return ProspectiveCollectionPolicy(
        policy_id=_string(payload, "policy_id"),
        capability=ObservationCapability(_string(payload, "capability")),
        sources=sources,
        window_start=_datetime(_string(payload, "window_start"), "window_start"),
        parameters_json=canonical_json_bytes(
            _object(payload.get("parameters"), "collection parameters")
        ).decode(),
        poll_interval_seconds=_integer(payload, "poll_interval_seconds"),
        maximum_gap_seconds=_integer(payload, "maximum_gap_seconds"),
        rolling_window=rolling_window,
        schema_version=schema_version,
    )


def _observation_version_core(observation: SourceObservation) -> dict[str, object]:
    return {
        "capability": observation.capability.value,
        "provider_id": observation.provider_id,
        "provider_version": observation.provider_version,
        "upstream_source": observation.upstream_source,
        "upstream_record_id": observation.upstream_record_id,
        "source_ref": observation.source_ref,
        "lineage_id": observation.lineage_id,
        "occurred_at": (
            None
            if observation.times.occurrence_basis.value == "retrieval_observed"
            else _timestamp(observation.times.occurred_at)
        ),
        "occurrence_basis": observation.times.occurrence_basis.value,
        "published_at": _optional_timestamp(observation.times.published_at),
        "source_updated_at": _optional_timestamp(observation.times.source_updated_at),
        "raw_content_hash": observation.raw_content_hash,
        "normalized_payload": observation.normalized_payload,
        "license_scope": observation.license_scope,
    }


def prospective_observation_version_id(observation: SourceObservation) -> str:
    content_hash = canonical_hash(_observation_version_core(observation))
    return f"prospective-observation-version-{content_hash}"


def _observation_from_json(value: str) -> SourceObservation:
    payload: object = json.loads(value)
    return source_observation_from_dict(payload)


def _parquet_row(row: sqlite3.Row) -> dict[str, object]:
    observation = _object(json.loads(cast(str, row["observation_json"])), "source observation")
    times = _object(observation.get("times"), "source observation times")
    return {
        "version_id": cast(str, row["version_id"]),
        "observation_id": _string(observation, "observation_id"),
        "capability": cast(str, row["capability"]),
        "provider_id": cast(str, row["provider_id"]),
        "provider_version": cast(str, row["provider_version"]),
        "upstream_source": cast(str, row["upstream_source"]),
        "upstream_record_id": cast(str, row["upstream_record_id"]),
        "lineage_id": cast(str, row["lineage_id"]),
        "source_ref": _string(observation, "source_ref"),
        "published_at": times.get("published_at"),
        "source_updated_at": times.get("source_updated_at"),
        "available_at": cast(str, row["first_available_at"]),
        "authority_at": observation.get("authority_at"),
        "raw_content_hash": cast(str, row["raw_content_hash"]),
        "license_scope": _string(observation, "license_scope"),
        "normalized_payload_json": canonical_json_bytes(
            _object(observation.get("normalized_payload"), "normalized_payload")
        ).decode(),
        "observation_json": cast(str, row["observation_json"]),
        "first_snapshot_id": cast(str, row["first_snapshot_id"]),
    }


def _parquet_schema(arrow: Any) -> Any:
    string = arrow.string()
    return arrow.schema(
        [
            ("version_id", string),
            ("observation_id", string),
            ("capability", string),
            ("provider_id", string),
            ("provider_version", string),
            ("upstream_source", string),
            ("upstream_record_id", string),
            ("lineage_id", string),
            ("source_ref", string),
            ("published_at", string),
            ("source_updated_at", string),
            ("available_at", string),
            ("authority_at", string),
            ("raw_content_hash", string),
            ("license_scope", string),
            ("normalized_payload_json", string),
            ("observation_json", string),
            ("first_snapshot_id", string),
        ]
    )


def _pyarrow_modules() -> tuple[Any, Any]:
    try:
        arrow = cast(Any, importlib.import_module("pyarrow"))
        parquet = cast(Any, importlib.import_module("pyarrow.parquet"))
    except ModuleNotFoundError as exc:
        raise RuntimeError("PyArrow is required; install market-impact-agent[data]") from exc
    return arrow, parquet


def _object(value: object, label: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise TypeError(f"{label} must be an object with string keys")
    raw = cast(dict[object, object], value)
    if any(not isinstance(key, str) for key in raw):
        raise TypeError(f"{label} must be an object with string keys")
    return cast(dict[str, object], raw)


def _list(value: object, label: str) -> list[object]:
    if not isinstance(value, list):
        raise TypeError(f"{label} must be an array")
    return cast(list[object], value)


def _required_strings(value: object, label: str) -> tuple[str, ...]:
    items = _list(value, label)
    if any(not isinstance(item, str) or not item for item in items):
        raise TypeError(f"{label} must contain non-empty strings")
    return tuple(cast(list[str], items))


def _string(value: Mapping[str, object], name: str) -> str:
    item = value.get(name)
    if not isinstance(item, str) or not item:
        raise TypeError(f"{name} must be a non-empty string")
    return item


def _integer(value: Mapping[str, object], name: str) -> int:
    item = value.get(name)
    if isinstance(item, bool) or not isinstance(item, int):
        raise TypeError(f"{name} must be an integer")
    return item


def _boolean(value: Mapping[str, object], name: str) -> bool:
    item = value.get(name)
    if not isinstance(item, bool):
        raise TypeError(f"{name} must be a boolean")
    return item


def _datetime(value: str, label: str) -> datetime:
    try:
        result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{label} must use ISO 8601") from exc
    _strict_utc(result, label)
    return result


def _strict_utc(value: datetime, label: str) -> None:
    require_aware(value, label)
    if value.utcoffset() != timedelta(0):
        raise ValueError(f"{label} must use UTC")


def _timestamp(value: datetime) -> str:
    require_aware(value, "timestamp")
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _optional_timestamp(value: datetime | None) -> str | None:
    return None if value is None else _timestamp(value)


def _sha256(value: str, label: str) -> None:
    if len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
        raise ValueError(f"{label} must be a lowercase SHA-256 hex digest")
