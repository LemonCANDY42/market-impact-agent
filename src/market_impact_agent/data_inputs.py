from __future__ import annotations

import asyncio
import json
import os
import re
import secrets
import sqlite3
import threading
import uuid
from collections import OrderedDict
from collections.abc import Generator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from hashlib import sha256
from pathlib import Path
from typing import Protocol, cast

from market_impact_agent.agent_contracts import canonical_hash, canonical_json_bytes
from market_impact_agent.agent_runtime import ToolDescriptor, ToolSideEffect
from market_impact_agent.data_acquisition import AcquisitionPending, AcquisitionUncertain
from market_impact_agent.domain import require_aware
from market_impact_agent.observations import (
    AvailabilityBasis,
    LatencyModelReference,
    ObservationCapability,
    ObservationProviderManifest,
    ObservationTimes,
    OccurrenceBasis,
)
from market_impact_agent.runtime_store import ArtifactStore

DATA_QUERY_SCHEMA_V1 = "market-impact.data-query.v1"
DATA_QUERY_SCHEMA = "market-impact.data-query.v2"
DATA_SNAPSHOT_SCHEMA_V1 = "market-impact.data-snapshot.v1"
DATA_SNAPSHOT_SCHEMA = "market-impact.data-snapshot.v2"
_PARSED_SNAPSHOT_CACHE_MAX_BYTES = 64 * 1024 * 1024
_PARSED_SNAPSHOT_CACHE_MAX_ENTRIES = 64


class DataFetchStatus(StrEnum):
    DATA = "data"
    NO_DATA = "no_data"
    NOT_CONFIGURED = "not_configured"
    RATE_LIMITED = "rate_limited"
    ERROR = "error"

    @property
    def completed(self) -> bool:
        return self in {DataFetchStatus.DATA, DataFetchStatus.NO_DATA}


class DataQueryMode(StrEnum):
    CACHE_ONLY = "cache_only"
    FETCH_IF_MISSING = "fetch_if_missing"
    DURABLE_FETCH_IF_MISSING = "durable_fetch_if_missing"


class DataPITLane(StrEnum):
    STRICT = "strict"
    MODELED = "modeled"
    PROSPECTIVE = "prospective"
    RETROSPECTIVE = "retrospective"


@dataclass(frozen=True, slots=True)
class DataSourceBinding:
    provider_id: str
    provider_version: str
    upstream_source: str
    manifest_hash: str
    source_config_hash: str | None
    required: bool

    def __post_init__(self) -> None:
        _identifier(self.provider_id, "data source provider_id")
        _nonempty(self.provider_version, "data source provider_version")
        _nonempty(self.upstream_source, "data source upstream_source")
        _sha256(self.manifest_hash, "data source manifest_hash")
        if self.source_config_hash is not None:
            _sha256(self.source_config_hash, "data source source_config_hash")

    @property
    def source_key(self) -> str:
        return f"{self.provider_id}:{self.upstream_source}"

    def to_dict(self) -> dict[str, object]:
        result: dict[str, object] = {
            "provider_id": self.provider_id,
            "provider_version": self.provider_version,
            "upstream_source": self.upstream_source,
            "manifest_hash": self.manifest_hash,
            "required": self.required,
        }
        if self.source_config_hash is not None:
            result["source_config_hash"] = self.source_config_hash
        return result


@dataclass(frozen=True, slots=True)
class DataQuery:
    query_id: str
    capability: ObservationCapability
    pit_lane: DataPITLane
    as_of: datetime
    window_start: datetime | None
    source_policy_id: str
    parameters_json: str = field(repr=False)
    sources: tuple[DataSourceBinding, ...]
    minimum_data_sources: int
    schema_version: str = DATA_QUERY_SCHEMA

    def __post_init__(self) -> None:
        if self.schema_version not in {DATA_QUERY_SCHEMA_V1, DATA_QUERY_SCHEMA}:
            raise ValueError("unsupported data query schema_version")
        _strict_utc(self.as_of, "data query as_of")
        if self.window_start is not None:
            _strict_utc(self.window_start, "data query window_start")
            if self.window_start >= self.as_of:
                raise ValueError("data query window_start must be before as_of")
        _nonempty(self.source_policy_id, "data query source_policy_id")
        parameters = _json_object(self.parameters_json, "data query parameters_json")
        canonical = canonical_json_bytes(parameters).decode()
        if self.parameters_json != canonical:
            raise ValueError("data query parameters_json must use canonical JSON")
        if not self.sources:
            raise ValueError("data query requires at least one source")
        if self.schema_version == DATA_QUERY_SCHEMA and any(
            item.source_config_hash is None for item in self.sources
        ):
            raise ValueError("v2 data query source bindings require source_config_hash")
        if self.schema_version == DATA_QUERY_SCHEMA_V1 and any(
            item.source_config_hash is not None for item in self.sources
        ):
            raise ValueError("v1 data query source bindings cannot carry source_config_hash")
        keys = tuple(item.source_key for item in self.sources)
        if len(keys) != len(set(keys)):
            raise ValueError("data query source bindings must be unique")
        if not 1 <= self.minimum_data_sources <= len(self.sources):
            raise ValueError("minimum_data_sources must fit inside the source set")
        if self.query_id != self.expected_query_id:
            raise ValueError("data query_id does not match content")

    @property
    def parameters(self) -> dict[str, object]:
        return _json_object(self.parameters_json, "data query parameters_json")

    @property
    def expected_query_id(self) -> str:
        return f"data-query-{canonical_hash(self.core_dict())}"

    def core_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "capability": self.capability.value,
            "pit_lane": self.pit_lane.value,
            "as_of": _timestamp(self.as_of),
            "window_start": _optional_timestamp(self.window_start),
            "source_policy_id": self.source_policy_id,
            "parameters": self.parameters,
            "sources": [item.to_dict() for item in self.sources],
            "minimum_data_sources": self.minimum_data_sources,
        }

    def to_dict(self) -> dict[str, object]:
        return {**self.core_dict(), "query_id": self.query_id}

    @classmethod
    def build(
        cls,
        *,
        capability: ObservationCapability,
        pit_lane: DataPITLane,
        as_of: datetime,
        window_start: datetime | None,
        source_policy_id: str,
        parameters: Mapping[str, object],
        sources: tuple[DataSourceBinding, ...],
        minimum_data_sources: int,
    ) -> DataQuery:
        parameters_json = canonical_json_bytes(parameters).decode()
        core = {
            "schema_version": DATA_QUERY_SCHEMA,
            "capability": capability.value,
            "pit_lane": pit_lane.value,
            "as_of": _timestamp(as_of),
            "window_start": _optional_timestamp(window_start),
            "source_policy_id": source_policy_id,
            "parameters": _json_object(parameters_json, "data query parameters"),
            "sources": [item.to_dict() for item in sources],
            "minimum_data_sources": minimum_data_sources,
        }
        return cls(
            query_id=f"data-query-{canonical_hash(core)}",
            capability=capability,
            pit_lane=pit_lane,
            as_of=as_of,
            window_start=window_start,
            source_policy_id=source_policy_id,
            parameters_json=parameters_json,
            sources=sources,
            minimum_data_sources=minimum_data_sources,
        )


@dataclass(frozen=True, slots=True)
class SourceObservation:
    observation_id: str
    capability: ObservationCapability
    provider_id: str
    provider_version: str
    upstream_source: str
    upstream_record_id: str
    source_ref: str
    lineage_id: str
    times: ObservationTimes
    authority_at: datetime | None
    authority_kind: str | None
    raw_content_hash: str
    normalized_payload_json: str = field(repr=False)
    license_scope: str

    def __post_init__(self) -> None:
        for name in (
            "provider_id",
            "provider_version",
            "upstream_source",
            "upstream_record_id",
            "source_ref",
            "lineage_id",
            "license_scope",
        ):
            _nonempty(cast(str, getattr(self, name)), f"source observation {name}")
        _sha256(self.raw_content_hash, "source observation raw_content_hash")
        normalized = _json_object(
            self.normalized_payload_json,
            "source observation normalized_payload_json",
        )
        if self.normalized_payload_json != canonical_json_bytes(normalized).decode():
            raise ValueError("normalized_payload_json must use canonical JSON")
        if self.authority_at is None:
            if self.authority_kind is not None:
                raise ValueError("authority_kind requires authority_at")
        else:
            _strict_utc(self.authority_at, "source observation authority_at")
            _nonempty(self.authority_kind, "source observation authority_kind")
            if self.authority_at > self.times.retrieved_at:
                raise ValueError("authority_at must not be after retrieved_at")
            if self.times.available_at is not None and self.authority_at < self.times.available_at:
                raise ValueError("authority_at must not precede available_at")
        if (
            self.times.source_updated_at is not None
            and self.times.available_at is not None
            and self.times.available_at < self.times.source_updated_at
        ):
            raise ValueError("available_at must not precede source_updated_at")
        if self.observation_id != self.expected_observation_id:
            raise ValueError("source observation_id does not match content")

    @property
    def normalized_payload(self) -> dict[str, object]:
        return _json_object(
            self.normalized_payload_json,
            "source observation normalized_payload_json",
        )

    @property
    def expected_observation_id(self) -> str:
        return f"source-observation-{canonical_hash(self.core_dict())}"

    def core_dict(self) -> dict[str, object]:
        return {
            "capability": self.capability.value,
            "provider_id": self.provider_id,
            "provider_version": self.provider_version,
            "upstream_source": self.upstream_source,
            "upstream_record_id": self.upstream_record_id,
            "source_ref": self.source_ref,
            "lineage_id": self.lineage_id,
            "times": self.times.to_dict(),
            "authority_at": _optional_timestamp(self.authority_at),
            "authority_kind": self.authority_kind,
            "raw_content_hash": self.raw_content_hash,
            "normalized_payload": self.normalized_payload,
            "license_scope": self.license_scope,
        }

    def to_dict(self) -> dict[str, object]:
        return {**self.core_dict(), "observation_id": self.observation_id}

    @classmethod
    def build(
        cls,
        *,
        capability: ObservationCapability,
        provider_id: str,
        provider_version: str,
        upstream_source: str,
        upstream_record_id: str,
        source_ref: str,
        lineage_id: str,
        times: ObservationTimes,
        authority_at: datetime | None,
        authority_kind: str | None,
        raw_content_hash: str,
        normalized_payload: Mapping[str, object],
        license_scope: str,
    ) -> SourceObservation:
        normalized_payload_json = canonical_json_bytes(normalized_payload).decode()
        core = {
            "capability": capability.value,
            "provider_id": provider_id,
            "provider_version": provider_version,
            "upstream_source": upstream_source,
            "upstream_record_id": upstream_record_id,
            "source_ref": source_ref,
            "lineage_id": lineage_id,
            "times": times.to_dict(),
            "authority_at": _optional_timestamp(authority_at),
            "authority_kind": authority_kind,
            "raw_content_hash": raw_content_hash,
            "normalized_payload": _json_object(
                normalized_payload_json,
                "source observation normalized_payload",
            ),
            "license_scope": license_scope,
        }
        return cls(
            observation_id=f"source-observation-{canonical_hash(core)}",
            capability=capability,
            provider_id=provider_id,
            provider_version=provider_version,
            upstream_source=upstream_source,
            upstream_record_id=upstream_record_id,
            source_ref=source_ref,
            lineage_id=lineage_id,
            times=times,
            authority_at=authority_at,
            authority_kind=authority_kind,
            raw_content_hash=raw_content_hash,
            normalized_payload_json=normalized_payload_json,
            license_scope=license_scope,
        )


@dataclass(frozen=True, slots=True)
class ProviderDataResponse:
    status: DataFetchStatus
    provider_id: str
    provider_version: str
    upstream_source: str
    retrieved_at: datetime
    raw_payload: bytes | None = field(repr=False)
    observations: tuple[SourceObservation, ...]
    raw_records: tuple[tuple[str, bytes], ...] = field(default=(), repr=False)
    error_kind: str | None = None

    def __post_init__(self) -> None:
        _identifier(self.provider_id, "provider data response provider_id")
        _nonempty(self.provider_version, "provider data response provider_version")
        _nonempty(self.upstream_source, "provider data response upstream_source")
        _strict_utc(self.retrieved_at, "provider data response retrieved_at")
        if self.status in {DataFetchStatus.DATA, DataFetchStatus.NO_DATA}:
            if self.raw_payload is None:
                raise ValueError("completed provider responses require raw_payload")
            if self.error_kind is not None:
                raise ValueError("completed provider responses cannot carry error_kind")
        else:
            if self.observations or self.raw_records:
                raise ValueError("failed provider responses cannot carry accepted data")
            _nonempty(self.error_kind, "failed provider response error_kind")
        if self.status is DataFetchStatus.DATA and not self.observations:
            raise ValueError("data response requires observations")
        if self.status is DataFetchStatus.NO_DATA and self.observations:
            raise ValueError("no_data response cannot carry observations")
        if self.status is DataFetchStatus.NO_DATA and self.raw_records:
            raise ValueError("no_data response cannot carry raw records")
        if any(item.times.retrieved_at != self.retrieved_at for item in self.observations):
            raise ValueError("observation receipt must match provider response receipt")
        for observation_id, payload in self.raw_records:
            _nonempty(observation_id, "raw record observation_id")
            if not payload:
                raise ValueError("raw record payload must not be empty")

    @property
    def raw_response_hash(self) -> str | None:
        return None if self.raw_payload is None else sha256(self.raw_payload).hexdigest()


class DataProvider(Protocol):
    @property
    def manifest(self) -> ObservationProviderManifest: ...

    def public_source_config(self, upstream_source: str) -> Mapping[str, object]: ...

    async def fetch(
        self,
        *,
        query: DataQuery,
        source: DataSourceBinding,
    ) -> ProviderDataResponse: ...


@dataclass(frozen=True, slots=True)
class DataProviderAttempt:
    provider_id: str
    provider_version: str
    upstream_source: str
    required: bool
    status: DataFetchStatus
    retrieved_at: datetime
    raw_response_hash: str | None
    received_count: int
    accepted_count: int
    rejected_missing_availability: int
    rejected_after_cutoff: int
    rejected_missing_authority: int
    rejected_authority_after_cutoff: int
    rejected_lane_mismatch: int
    error_kind: str | None

    def __post_init__(self) -> None:
        _identifier(self.provider_id, "data provider attempt provider_id")
        _nonempty(self.provider_version, "data provider attempt provider_version")
        _nonempty(self.upstream_source, "data provider attempt upstream_source")
        _strict_utc(self.retrieved_at, "data provider attempt retrieved_at")
        for name in (
            "received_count",
            "accepted_count",
            "rejected_missing_availability",
            "rejected_after_cutoff",
            "rejected_missing_authority",
            "rejected_authority_after_cutoff",
            "rejected_lane_mismatch",
        ):
            if cast(int, getattr(self, name)) < 0:
                raise ValueError(f"{name} must be non-negative")
        if self.accepted_count > self.received_count:
            raise ValueError("accepted_count cannot exceed received_count")
        if (
            self.accepted_count
            + self.rejected_missing_availability
            + self.rejected_after_cutoff
            + self.rejected_missing_authority
            + self.rejected_authority_after_cutoff
            + self.rejected_lane_mismatch
            != self.received_count
        ):
            raise ValueError("data provider attempt counts do not reconcile")
        if self.status.completed:
            if self.raw_response_hash is None:
                raise ValueError("completed data provider attempts require raw_response_hash")
            _sha256(self.raw_response_hash, "data provider attempt raw_response_hash")
            if self.error_kind is not None:
                raise ValueError("completed data provider attempts cannot carry error_kind")
        else:
            if self.received_count != 0:
                raise ValueError("failed data provider attempts cannot carry accepted source data")
            if self.raw_response_hash is not None:
                _sha256(self.raw_response_hash, "failed data provider raw_response_hash")
            _nonempty(self.error_kind, "failed data provider attempt error_kind")
        if self.status is DataFetchStatus.DATA and self.received_count == 0:
            raise ValueError("data provider attempt requires received records")
        if self.status is DataFetchStatus.NO_DATA and self.received_count != 0:
            raise ValueError("no_data provider attempt cannot carry records")

    def to_dict(self) -> dict[str, object]:
        return {
            "provider_id": self.provider_id,
            "provider_version": self.provider_version,
            "upstream_source": self.upstream_source,
            "required": self.required,
            "status": self.status.value,
            "retrieved_at": _timestamp(self.retrieved_at),
            "raw_response_hash": self.raw_response_hash,
            "received_count": self.received_count,
            "accepted_count": self.accepted_count,
            "rejected_missing_availability": self.rejected_missing_availability,
            "rejected_after_cutoff": self.rejected_after_cutoff,
            "rejected_missing_authority": self.rejected_missing_authority,
            "rejected_authority_after_cutoff": self.rejected_authority_after_cutoff,
            "rejected_lane_mismatch": self.rejected_lane_mismatch,
            "error_kind": self.error_kind,
        }


@dataclass(frozen=True, slots=True)
class DataSnapshot:
    snapshot_id: str
    query: DataQuery
    attempts: tuple[DataProviderAttempt, ...]
    observations: tuple[SourceObservation, ...]
    coverage_complete: bool
    completed_at: datetime
    schema_version: str = DATA_SNAPSHOT_SCHEMA

    def __post_init__(self) -> None:
        if self.schema_version not in {DATA_SNAPSHOT_SCHEMA_V1, DATA_SNAPSHOT_SCHEMA}:
            raise ValueError("unsupported data snapshot schema_version")
        expected_query_schema = (
            DATA_QUERY_SCHEMA_V1
            if self.schema_version == DATA_SNAPSHOT_SCHEMA_V1
            else DATA_QUERY_SCHEMA
        )
        if self.query.schema_version != expected_query_schema:
            raise ValueError("data snapshot and query schema_version must match")
        _strict_utc(self.completed_at, "data snapshot completed_at")
        if len(self.attempts) != len(self.query.sources):
            raise ValueError("data snapshot attempts must match the query source set")
        for binding, attempt in zip(self.query.sources, self.attempts, strict=True):
            if (
                binding.provider_id != attempt.provider_id
                or binding.provider_version != attempt.provider_version
                or binding.upstream_source != attempt.upstream_source
                or binding.required != attempt.required
            ):
                raise ValueError("data snapshot attempt order does not match the query")
            observed_count = sum(
                item.provider_id == binding.provider_id
                and item.provider_version == binding.provider_version
                and item.upstream_source == binding.upstream_source
                for item in self.observations
            )
            if observed_count != attempt.accepted_count:
                raise ValueError("data snapshot observations do not reconcile with attempts")
        if any(item.capability is not self.query.capability for item in self.observations):
            raise ValueError("data snapshot observation capability does not match query")
        if len({item.observation_id for item in self.observations}) != len(self.observations):
            raise ValueError("data snapshot observations must be unique")
        for observation in self.observations:
            available_at = observation.times.available_at
            if available_at is None or available_at > self.query.as_of:
                raise ValueError("data snapshot observations must be visible at query cutoff")
            if self.query.pit_lane is DataPITLane.STRICT and (
                observation.times.availability_basis is AvailabilityBasis.MODELED_LATENCY
                or observation.authority_at is None
                or observation.authority_at < available_at
                or observation.authority_at > self.query.as_of
            ):
                raise ValueError(
                    "strict Data Snapshot observations require non-modeled cutoff authority "
                    "no earlier than availability"
                )
            if self.query.pit_lane is DataPITLane.PROSPECTIVE and not _prospective_receipt(
                observation
            ):
                raise ValueError("prospective Data Snapshot requires actual receipt authority")
        expected_complete = data_snapshot_coverage_complete(self.query, self.attempts)
        if self.coverage_complete != expected_complete:
            raise ValueError("data snapshot coverage_complete does not match attempts")
        if self.completed_at != max(item.retrieved_at for item in self.attempts):
            raise ValueError("data snapshot completed_at must equal the latest attempt receipt")
        if self.snapshot_id != self.expected_snapshot_id:
            raise ValueError("data snapshot_id does not match content")

    @property
    def expected_snapshot_id(self) -> str:
        return f"data-snapshot-{canonical_hash(self.core_dict())}"

    def core_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "query": self.query.to_dict(),
            "attempts": [item.to_dict() for item in self.attempts],
            "observations": [item.to_dict() for item in self.observations],
            "coverage_complete": self.coverage_complete,
            "completed_at": _timestamp(self.completed_at),
        }

    def to_dict(self) -> dict[str, object]:
        return {**self.core_dict(), "snapshot_id": self.snapshot_id}


class LocalDataSnapshotStore:
    def __init__(self, root: Path) -> None:
        self._parsed_snapshots: OrderedDict[str, tuple[DataSnapshot, int]] = OrderedDict()
        self._parsed_snapshot_bytes = 0
        self._parsed_snapshot_lock = threading.Lock()
        self.root = root.resolve()
        self.root.mkdir(mode=0o700, parents=True, exist_ok=True)
        os.chmod(self.root, 0o700)
        self.artifacts = ArtifactStore(self.root / "artifacts")
        self.index_path = self.root / "index.sqlite3"
        self._event_signing_key_path = self.root / ".harness-event-hmac.key"
        self._initialize_event_signing_key()
        self._initialize()
        os.chmod(self.index_path, 0o600)

    def _initialize_event_signing_key(self) -> None:
        try:
            descriptor = os.open(
                self._event_signing_key_path,
                os.O_CREAT | os.O_EXCL | os.O_WRONLY,
                0o600,
            )
        except FileExistsError:
            if (
                self._event_signing_key_path.is_symlink()
                or not self._event_signing_key_path.is_file()
            ):
                raise ValueError("Harness event signing key is not a regular file") from None
            os.chmod(self._event_signing_key_path, 0o600)
            key = self._event_signing_key_path.read_bytes()
            if len(key) != 32:
                raise ValueError("Harness event signing key has an invalid length") from None
            return
        try:
            with os.fdopen(descriptor, "wb") as stream:
                stream.write(secrets.token_bytes(32))
                stream.flush()
                os.fsync(stream.fileno())
        except Exception:
            self._event_signing_key_path.unlink(missing_ok=True)
            raise

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.index_path)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA journal_mode = WAL")
        connection.execute("PRAGMA synchronous = FULL")
        return connection

    def _initialize(self) -> None:
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS harness_authority (
                    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                    authority_id TEXT NOT NULL UNIQUE
                );
                CREATE TABLE IF NOT EXISTS data_snapshots (
                    snapshot_id TEXT PRIMARY KEY,
                    query_id TEXT NOT NULL,
                    artifact_hash TEXT NOT NULL,
                    coverage_complete INTEGER NOT NULL,
                    completed_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS data_snapshots_query
                    ON data_snapshots(query_id, coverage_complete, completed_at);
                """
            )
            connection.execute(
                """
                INSERT OR IGNORE INTO harness_authority(singleton, authority_id)
                VALUES (1, ?)
                """,
                (f"harness-authority-{uuid.uuid4().hex}",),
            )

    @property
    def harness_authority_id(self) -> str:
        """Stable identity of the concrete Harness authority rooted at this store."""

        with self._connect() as connection:
            row = connection.execute(
                "SELECT authority_id FROM harness_authority WHERE singleton = 1"
            ).fetchone()
        if row is None:
            raise RuntimeError("Harness authority identity is missing")
        return cast(str, row["authority_id"])

    @contextmanager
    def authority_transaction(self) -> Generator[sqlite3.Connection]:
        """Serialize one dependency-closed mutation in this Harness authority root."""

        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            yield connection
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def put(self, snapshot: DataSnapshot) -> None:
        artifact = self.artifacts.put_json(snapshot.to_dict())
        with self._connect() as connection:
            existing = connection.execute(
                "SELECT artifact_hash FROM data_snapshots WHERE snapshot_id = ?",
                (snapshot.snapshot_id,),
            ).fetchone()
            if existing is not None:
                if cast(str, existing["artifact_hash"]) != artifact.content_hash:
                    raise ValueError("data snapshot identity has conflicting content")
                return
            connection.execute(
                """
                INSERT INTO data_snapshots(
                    snapshot_id, query_id, artifact_hash, coverage_complete, completed_at
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (
                    snapshot.snapshot_id,
                    snapshot.query.query_id,
                    artifact.content_hash,
                    int(snapshot.coverage_complete),
                    _timestamp(snapshot.completed_at),
                ),
            )

    def put_raw(self, payload: bytes) -> str:
        return self.artifacts.put_bytes(
            payload,
            media_type="application/octet-stream",
        ).content_hash

    def put_source_config(
        self,
        payload: Mapping[str, object],
        *,
        expected_hash: str,
    ) -> None:
        _sha256(expected_hash, "source config expected_hash")
        artifact = self.artifacts.put_json(dict(payload))
        if artifact.content_hash != expected_hash:
            raise ValueError("public source config hash mismatch")

    def get(self, snapshot_id: str) -> DataSnapshot:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT artifact_hash FROM data_snapshots WHERE snapshot_id = ?",
                (snapshot_id,),
            ).fetchone()
        if row is None:
            raise KeyError(f"unknown Data Snapshot: {snapshot_id}")
        content_hash = cast(str, row["artifact_hash"])
        with self._parsed_snapshot_lock:
            # The index and current CAS bytes remain authoritative on every hit.
            payload = self.artifacts.read_bytes(content_hash)
            cached = self._parsed_snapshots.get(content_hash)
            snapshot = (
                cached[0]
                if cached is not None
                else data_snapshot_from_dict(json.loads(payload.decode("utf-8")))
            )
            if snapshot.snapshot_id != snapshot_id:
                raise ValueError("data snapshot index identity does not match artifact")
            if cached is not None:
                self._parsed_snapshots.move_to_end(content_hash)
            elif (
                len(payload) <= _PARSED_SNAPSHOT_CACHE_MAX_BYTES
                and _PARSED_SNAPSHOT_CACHE_MAX_ENTRIES > 0
            ):
                while self._parsed_snapshots and (
                    self._parsed_snapshot_bytes + len(payload) > _PARSED_SNAPSHOT_CACHE_MAX_BYTES
                    or len(self._parsed_snapshots) >= _PARSED_SNAPSHOT_CACHE_MAX_ENTRIES
                ):
                    _, (_, size) = self._parsed_snapshots.popitem(last=False)
                    self._parsed_snapshot_bytes -= size
                self._parsed_snapshots[content_hash] = (snapshot, len(payload))
                self._parsed_snapshot_bytes += len(payload)
            return snapshot

    def latest_complete(self, query_id: str) -> DataSnapshot | None:
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT artifact_hash FROM data_snapshots
                WHERE query_id = ? AND coverage_complete = 1
                ORDER BY completed_at DESC, snapshot_id DESC LIMIT 1
                """,
                (query_id,),
            ).fetchone()
        if row is None:
            return None
        payload = self.artifacts.read_json(cast(str, row["artifact_hash"]))
        return data_snapshot_from_dict(payload)


class DataInputHarness:
    def __init__(
        self,
        store: LocalDataSnapshotStore,
        *,
        provider_timeout_seconds: float = 30.0,
    ) -> None:
        if provider_timeout_seconds <= 0:
            raise ValueError("provider_timeout_seconds must be positive")
        self.store = store
        self.provider_timeout_seconds = provider_timeout_seconds
        self._providers: dict[str, DataProvider] = {}
        self._query_locks: dict[str, asyncio.Lock] = {}

    def register(self, provider: DataProvider) -> None:
        manifest = provider.manifest
        manifest.assert_valid()
        if manifest.provider_id in self._providers:
            raise ValueError(f"duplicate data provider: {manifest.provider_id}")
        for upstream_source in manifest.upstream_sources:
            public_config = provider.public_source_config(upstream_source)
            self.store.put_source_config(
                public_config,
                expected_hash=canonical_hash(public_config),
            )
        self._providers[manifest.provider_id] = provider

    async def execute(
        self,
        query: DataQuery,
        *,
        mode: DataQueryMode,
    ) -> DataSnapshot:
        if query.schema_version != DATA_QUERY_SCHEMA and mode is not DataQueryMode.CACHE_ONLY:
            raise ValueError("legacy Data Query is replay-only")
        cached = await asyncio.to_thread(self.store.latest_complete, query.query_id)
        if cached is not None:
            return cached
        if mode is DataQueryMode.CACHE_ONLY:
            raise LookupError(f"no complete cached Data Snapshot for {query.query_id}")
        if mode is DataQueryMode.DURABLE_FETCH_IF_MISSING:
            from market_impact_agent.data_acquisition import DurableDataAcquisition

            acquisition = await asyncio.to_thread(DurableDataAcquisition, self.store)
            return await acquisition.execute(
                query, fetch=self._fetch, lease_seconds=self.provider_timeout_seconds + 60.0
            )
        lock = self._query_locks.setdefault(query.query_id, asyncio.Lock())
        async with lock:
            cached = await asyncio.to_thread(self.store.latest_complete, query.query_id)
            if cached is not None:
                return cached
            snapshot = await self._fetch(query)
            await asyncio.to_thread(self.store.put, snapshot)
            return snapshot

    async def _fetch(self, query: DataQuery) -> DataSnapshot:
        responses = await asyncio.gather(
            *(self._fetch_one(query, source) for source in query.sources)
        )
        attempts: list[DataProviderAttempt] = []
        accepted: list[SourceObservation] = []
        for source, response in zip(query.sources, responses, strict=True):
            if response.raw_payload is not None:
                stored_hash = await asyncio.to_thread(self.store.put_raw, response.raw_payload)
                if stored_hash != response.raw_response_hash:
                    raise ValueError("stored raw Provider response hash mismatch")
            for observation_id, raw_record in response.raw_records:
                stored_hash = await asyncio.to_thread(self.store.put_raw, raw_record)
                observation = next(
                    item for item in response.observations if item.observation_id == observation_id
                )
                if stored_hash != observation.raw_content_hash:
                    raise ValueError("stored raw Source Observation hash mismatch")
            visible: list[SourceObservation] = []
            missing_availability = 0
            after_cutoff = 0
            missing_authority = 0
            authority_after_cutoff = 0
            lane_mismatch = 0
            for observation in response.observations:
                available_at = observation.times.available_at
                if available_at is None:
                    missing_availability += 1
                elif available_at > query.as_of:
                    after_cutoff += 1
                elif (
                    query.pit_lane is DataPITLane.STRICT
                    and observation.times.availability_basis is AvailabilityBasis.MODELED_LATENCY
                ):
                    lane_mismatch += 1
                elif query.pit_lane is DataPITLane.STRICT and observation.authority_at is None:
                    missing_authority += 1
                elif (
                    query.pit_lane is DataPITLane.STRICT
                    and observation.authority_at is not None
                    and observation.authority_at > query.as_of
                ):
                    authority_after_cutoff += 1
                elif (
                    query.pit_lane is DataPITLane.STRICT
                    and observation.authority_at is not None
                    and observation.authority_at < available_at
                ) or (
                    query.pit_lane is DataPITLane.PROSPECTIVE
                    and not _prospective_receipt(observation)
                ):
                    lane_mismatch += 1
                else:
                    visible.append(observation)
            accepted.extend(visible)
            attempts.append(
                DataProviderAttempt(
                    provider_id=source.provider_id,
                    provider_version=source.provider_version,
                    upstream_source=source.upstream_source,
                    required=source.required,
                    status=response.status,
                    retrieved_at=response.retrieved_at,
                    raw_response_hash=response.raw_response_hash,
                    received_count=len(response.observations),
                    accepted_count=len(visible),
                    rejected_missing_availability=missing_availability,
                    rejected_after_cutoff=after_cutoff,
                    rejected_missing_authority=missing_authority,
                    rejected_authority_after_cutoff=authority_after_cutoff,
                    rejected_lane_mismatch=lane_mismatch,
                    error_kind=response.error_kind,
                )
            )
        attempt_tuple = tuple(attempts)
        core = {
            "schema_version": DATA_SNAPSHOT_SCHEMA,
            "query": query.to_dict(),
            "attempts": [item.to_dict() for item in attempt_tuple],
            "observations": [item.to_dict() for item in accepted],
            "coverage_complete": data_snapshot_coverage_complete(query, attempt_tuple),
            "completed_at": _timestamp(max(item.retrieved_at for item in attempt_tuple)),
        }
        return DataSnapshot(
            snapshot_id=f"data-snapshot-{canonical_hash(core)}",
            query=query,
            attempts=attempt_tuple,
            observations=tuple(accepted),
            coverage_complete=cast(bool, core["coverage_complete"]),
            completed_at=max(item.retrieved_at for item in attempt_tuple),
        )

    async def _fetch_one(
        self,
        query: DataQuery,
        source: DataSourceBinding,
    ) -> ProviderDataResponse:
        now = datetime.now(UTC)
        provider = self._providers.get(source.provider_id)
        if provider is None:
            return _failed_response(source, now, DataFetchStatus.NOT_CONFIGURED, "provider_missing")
        manifest = provider.manifest
        if (
            manifest.provider_version != source.provider_version
            or canonical_hash(manifest.to_dict()) != source.manifest_hash
        ):
            return _failed_response(
                source,
                now,
                DataFetchStatus.NOT_CONFIGURED,
                "provider_manifest_mismatch",
            )
        if source.source_config_hash is None:
            return _failed_response(
                source,
                now,
                DataFetchStatus.NOT_CONFIGURED,
                "provider_source_config_required",
            )
        try:
            public_source_config = provider.public_source_config(source.upstream_source)
        except (KeyError, TypeError, ValueError):
            return _failed_response(
                source,
                now,
                DataFetchStatus.NOT_CONFIGURED,
                "provider_source_config_missing",
            )
        if canonical_hash(public_source_config) != source.source_config_hash:
            return _failed_response(
                source,
                now,
                DataFetchStatus.NOT_CONFIGURED,
                "provider_source_config_mismatch",
            )
        if (
            not manifest.enabled
            or query.capability not in manifest.verified_capabilities
            or source.upstream_source not in manifest.upstream_sources
        ):
            return _failed_response(
                source,
                now,
                DataFetchStatus.NOT_CONFIGURED,
                "capability_not_verified",
            )
        try:
            response = await asyncio.wait_for(
                provider.fetch(query=query, source=source),
                timeout=self.provider_timeout_seconds,
            )
        except (AcquisitionPending, AcquisitionUncertain):
            raise
        except Exception as exc:
            return _failed_response(
                source,
                datetime.now(UTC),
                DataFetchStatus.ERROR,
                type(exc).__name__,
            )
        if (
            response.provider_id != source.provider_id
            or response.provider_version != source.provider_version
            or response.upstream_source != source.upstream_source
        ):
            return _failed_response(
                source,
                datetime.now(UTC),
                DataFetchStatus.ERROR,
                "response_identity_mismatch",
            )
        for observation in response.observations:
            if (
                observation.provider_id != source.provider_id
                or observation.provider_version != source.provider_version
                or observation.upstream_source != source.upstream_source
                or observation.capability is not query.capability
            ):
                return _failed_response(
                    source,
                    datetime.now(UTC),
                    DataFetchStatus.ERROR,
                    "observation_identity_mismatch",
                )
        raw_records = dict(response.raw_records)
        observation_ids = {item.observation_id for item in response.observations}
        if len(raw_records) != len(response.raw_records) or set(raw_records) != observation_ids:
            return _failed_response(
                source,
                datetime.now(UTC),
                DataFetchStatus.ERROR,
                "raw_record_set_mismatch",
            )
        if any(
            sha256(raw_records[item.observation_id]).hexdigest() != item.raw_content_hash
            for item in response.observations
        ):
            return _failed_response(
                source,
                datetime.now(UTC),
                DataFetchStatus.ERROR,
                "raw_record_hash_mismatch",
            )
        return response


@dataclass(frozen=True, slots=True)
class DataToolBinding:
    name: str
    version: str
    description: str
    capability: ObservationCapability
    required_capability: str
    input_schema: dict[str, object]
    as_of: datetime
    window_start: datetime | None
    source_policy_id: str
    sources: tuple[DataSourceBinding, ...]
    minimum_data_sources: int
    pit_lane: DataPITLane = DataPITLane.STRICT
    mode: DataQueryMode = DataQueryMode.CACHE_ONLY
    timeout_seconds: float = 30.0
    max_result_bytes: int = 100_000

    def descriptor(self, harness: DataInputHarness) -> ToolDescriptor:
        async def handler(arguments: dict[str, object]) -> object:
            query = DataQuery.build(
                capability=self.capability,
                pit_lane=self.pit_lane,
                as_of=self.as_of,
                window_start=self.window_start,
                source_policy_id=self.source_policy_id,
                parameters=arguments,
                sources=self.sources,
                minimum_data_sources=self.minimum_data_sources,
            )
            snapshot = await harness.execute(query, mode=self.mode)
            return snapshot.to_dict()

        return ToolDescriptor(
            name=self.name,
            version=self.version,
            description=self.description,
            input_schema=self.input_schema,
            required_capabilities=frozenset({self.required_capability}),
            side_effect=ToolSideEffect.READ_ONLY,
            timeout_seconds=self.timeout_seconds,
            max_result_bytes=self.max_result_bytes,
            handler=handler,
        )


@dataclass(frozen=True, slots=True)
class FrozenDataSnapshotToolBinding:
    name: str
    version: str
    description: str
    snapshot_id: str
    required_capability: str
    timeout_seconds: float = 5.0
    max_result_bytes: int = 100_000

    def descriptor(
        self,
        store: LocalDataSnapshotStore,
        *,
        frozen_input: FrozenDataSnapshotInput,
    ) -> ToolDescriptor:
        if self.snapshot_id not in frozen_input.authorized_snapshot_ids:
            raise ValueError("frozen Data Snapshot is not declared by the enclosing run input")
        snapshot = store.get(self.snapshot_id)
        if not snapshot.coverage_complete:
            raise ValueError("frozen Data Snapshot tool requires complete source coverage")

        async def handler(arguments: dict[str, object]) -> object:
            query_text = _optional_argument_string(arguments, "query")
            publisher = _optional_argument_string(arguments, "publisher")
            limit = _optional_argument_integer(arguments, "limit", default=20)
            observations = tuple(
                item
                for item in snapshot.observations
                if _snapshot_observation_matches(
                    item,
                    query_text=query_text,
                    publisher=publisher,
                )
            )[:limit]
            return {
                "schema_version": "market-impact.frozen-data-tool-result.v1",
                "snapshot_id": snapshot.snapshot_id,
                "query_id": snapshot.query.query_id,
                "capability": snapshot.query.capability.value,
                "as_of": _timestamp(snapshot.query.as_of),
                "source_policy_id": snapshot.query.source_policy_id,
                "sources": [item.to_dict() for item in snapshot.query.sources],
                "attempts": [item.to_dict() for item in snapshot.attempts],
                "selection": {
                    "query": query_text,
                    "publisher": publisher,
                    "limit": limit,
                },
                "observations": [item.to_dict() for item in observations],
            }

        return ToolDescriptor(
            name=self.name,
            version=f"{self.version}+{snapshot.snapshot_id}",
            description=(
                f"{self.description} Reads only frozen Data Snapshot {snapshot.snapshot_id}; "
                "arguments cannot change its cutoff, sources, or Provider versions."
            ),
            input_schema={
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "query": {"type": "string", "minLength": 1},
                    "publisher": {"type": "string", "minLength": 1},
                    "limit": {"type": "integer", "minimum": 1, "maximum": 100},
                },
            },
            required_capabilities=frozenset({self.required_capability}),
            side_effect=ToolSideEffect.READ_ONLY,
            timeout_seconds=self.timeout_seconds,
            max_result_bytes=self.max_result_bytes,
            handler=handler,
        )


@dataclass(frozen=True, slots=True)
class FrozenDataSnapshotInput:
    authorized_snapshot_ids: frozenset[str]

    def __post_init__(self) -> None:
        if not self.authorized_snapshot_ids:
            raise ValueError("frozen Data Snapshot input requires at least one snapshot ID")
        for snapshot_id in self.authorized_snapshot_ids:
            _nonempty(snapshot_id, "frozen Data Snapshot input snapshot_id")


def data_snapshot_from_dict(value: object) -> DataSnapshot:
    payload = _object(value, "data snapshot")
    query = _query_from_dict(payload.get("query"))
    attempts_value = _list(payload.get("attempts"), "data snapshot attempts")
    observations_value = _list(payload.get("observations"), "data snapshot observations")
    attempts = tuple(_attempt_from_dict(item) for item in attempts_value)
    observations = tuple(source_observation_from_dict(item) for item in observations_value)
    snapshot = DataSnapshot(
        snapshot_id=_string(payload, "snapshot_id"),
        query=query,
        attempts=attempts,
        observations=observations,
        coverage_complete=_boolean(payload, "coverage_complete"),
        completed_at=_datetime(payload.get("completed_at"), "completed_at"),
        schema_version=_string(payload, "schema_version"),
    )
    if snapshot.to_dict() != payload:
        raise ValueError("data snapshot does not match canonical contract")
    return snapshot


def _query_from_dict(value: object) -> DataQuery:
    payload = _object(value, "data query")
    sources = tuple(
        DataSourceBinding(
            provider_id=_string(item, "provider_id"),
            provider_version=_string(item, "provider_version"),
            upstream_source=_string(item, "upstream_source"),
            manifest_hash=_string(item, "manifest_hash"),
            source_config_hash=_optional_string(item, "source_config_hash"),
            required=_boolean(item, "required"),
        )
        for item in (
            _object(raw, "data query source") for raw in _list(payload.get("sources"), "sources")
        )
    )
    return DataQuery(
        query_id=_string(payload, "query_id"),
        capability=ObservationCapability(_string(payload, "capability")),
        pit_lane=DataPITLane(_string(payload, "pit_lane")),
        as_of=_datetime(payload.get("as_of"), "as_of"),
        window_start=_optional_datetime(payload.get("window_start"), "window_start"),
        source_policy_id=_string(payload, "source_policy_id"),
        parameters_json=canonical_json_bytes(
            _object(payload.get("parameters"), "parameters")
        ).decode(),
        sources=sources,
        minimum_data_sources=_integer(payload, "minimum_data_sources"),
        schema_version=_string(payload, "schema_version"),
    )


def _attempt_from_dict(value: object) -> DataProviderAttempt:
    payload = _object(value, "data provider attempt")
    return DataProviderAttempt(
        provider_id=_string(payload, "provider_id"),
        provider_version=_string(payload, "provider_version"),
        upstream_source=_string(payload, "upstream_source"),
        required=_boolean(payload, "required"),
        status=DataFetchStatus(_string(payload, "status")),
        retrieved_at=_datetime(payload.get("retrieved_at"), "retrieved_at"),
        raw_response_hash=_optional_string(payload, "raw_response_hash"),
        received_count=_integer(payload, "received_count"),
        accepted_count=_integer(payload, "accepted_count"),
        rejected_missing_availability=_integer(payload, "rejected_missing_availability"),
        rejected_after_cutoff=_integer(payload, "rejected_after_cutoff"),
        rejected_missing_authority=_integer(payload, "rejected_missing_authority"),
        rejected_authority_after_cutoff=_integer(payload, "rejected_authority_after_cutoff"),
        rejected_lane_mismatch=_integer(payload, "rejected_lane_mismatch"),
        error_kind=_optional_string(payload, "error_kind"),
    )


def source_observation_from_dict(value: object) -> SourceObservation:
    payload = _object(value, "source observation")
    times_payload = _object(payload.get("times"), "source observation times")
    latency_payload = times_payload.get("latency_model")
    latency_model: LatencyModelReference | None = None
    if latency_payload is not None:
        latency = _object(latency_payload, "latency model")
        latency_model = LatencyModelReference(
            source_class=_string(latency, "source_class"),
            model_id=_string(latency, "model_id"),
            model_version=_string(latency, "model_version"),
            calibration_ref=_string(latency, "calibration_ref"),
        )
    times = ObservationTimes(
        occurred_at=_datetime(times_payload.get("occurred_at"), "occurred_at"),
        published_at=_optional_datetime(times_payload.get("published_at"), "published_at"),
        available_at=_optional_datetime(times_payload.get("available_at"), "available_at"),
        source_updated_at=_optional_datetime(
            times_payload.get("source_updated_at"), "source_updated_at"
        ),
        aggregator_fetched_at=_optional_datetime(
            times_payload.get("aggregator_fetched_at"), "aggregator_fetched_at"
        ),
        retrieved_at=_datetime(times_payload.get("retrieved_at"), "retrieved_at"),
        occurrence_basis=OccurrenceBasis(_string(times_payload, "occurrence_basis")),
        availability_basis=AvailabilityBasis(_string(times_payload, "availability_basis")),
        latency_model=latency_model,
    )
    return SourceObservation(
        observation_id=_string(payload, "observation_id"),
        capability=ObservationCapability(_string(payload, "capability")),
        provider_id=_string(payload, "provider_id"),
        provider_version=_string(payload, "provider_version"),
        upstream_source=_string(payload, "upstream_source"),
        upstream_record_id=_string(payload, "upstream_record_id"),
        source_ref=_string(payload, "source_ref"),
        lineage_id=_string(payload, "lineage_id"),
        times=times,
        authority_at=_optional_datetime(payload.get("authority_at"), "authority_at"),
        authority_kind=_optional_string(payload, "authority_kind"),
        raw_content_hash=_string(payload, "raw_content_hash"),
        normalized_payload_json=canonical_json_bytes(
            _object(payload.get("normalized_payload"), "normalized_payload")
        ).decode(),
        license_scope=_string(payload, "license_scope"),
    )


def data_snapshot_coverage_complete(
    query: DataQuery,
    attempts: tuple[DataProviderAttempt, ...],
) -> bool:
    required_complete = all(not item.required or item.status.completed for item in attempts)
    data_sources = sum(item.accepted_count > 0 for item in attempts)
    receipt_reached_cutoff = (
        query.schema_version == DATA_QUERY_SCHEMA_V1
        or query.pit_lane is not DataPITLane.PROSPECTIVE
        or max(item.retrieved_at for item in attempts) >= query.as_of
    )
    return (
        required_complete and data_sources >= query.minimum_data_sources and receipt_reached_cutoff
    )


def _failed_response(
    source: DataSourceBinding,
    retrieved_at: datetime,
    status: DataFetchStatus,
    error_kind: str,
) -> ProviderDataResponse:
    return ProviderDataResponse(
        status=status,
        provider_id=source.provider_id,
        provider_version=source.provider_version,
        upstream_source=source.upstream_source,
        retrieved_at=retrieved_at,
        raw_payload=None,
        observations=(),
        raw_records=(),
        error_kind=error_kind,
    )


def _prospective_receipt(observation: SourceObservation) -> bool:
    return (
        observation.times.availability_basis is AvailabilityBasis.ACTUAL_RECEIPT
        and observation.times.available_at == observation.times.retrieved_at
        and observation.authority_at == observation.times.retrieved_at
        and observation.authority_kind == "actual_receipt"
    )


def _snapshot_observation_matches(
    observation: SourceObservation,
    *,
    query_text: str | None,
    publisher: str | None,
) -> bool:
    payload = observation.normalized_payload
    if publisher is not None:
        observed_publisher = payload.get("publisher")
        if not isinstance(observed_publisher, str):
            return False
        if observed_publisher.casefold() != publisher.casefold():
            return False
    if query_text is None:
        return True
    searchable = "\n".join(_payload_strings(payload)).casefold()
    return query_text.casefold() in searchable


def _payload_strings(value: object) -> tuple[str, ...]:
    if isinstance(value, str):
        return (value,)
    if isinstance(value, Mapping):
        strings: list[str] = []
        for item in cast(Mapping[object, object], value).values():
            strings.extend(_payload_strings(item))
        return tuple(strings)
    if isinstance(value, list):
        strings = []
        for item in cast(list[object], value):
            strings.extend(_payload_strings(item))
        return tuple(strings)
    return ()


def _optional_argument_string(arguments: Mapping[str, object], name: str) -> str | None:
    value = arguments.get(name)
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise TypeError(f"{name} must be a non-empty string")
    return value.strip()


def _optional_argument_integer(
    arguments: Mapping[str, object],
    name: str,
    *,
    default: int,
) -> int:
    value = arguments.get(name, default)
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{name} must be an integer")
    if not 1 <= value <= 100:
        raise ValueError(f"{name} must be between 1 and 100")
    return value


def _timestamp(value: datetime) -> str:
    require_aware(value, "timestamp")
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _optional_timestamp(value: datetime | None) -> str | None:
    return None if value is None else _timestamp(value)


def _strict_utc(value: datetime, name: str) -> None:
    require_aware(value, name)
    if value.utcoffset() != UTC.utcoffset(value):
        raise ValueError(f"{name} must be UTC")


def _nonempty(value: str | None, name: str) -> None:
    if value is None or not value or value != value.strip():
        raise ValueError(f"{name} must be a non-empty trimmed string")


def _identifier(value: str, name: str) -> None:
    if re.fullmatch(r"[a-z0-9][a-z0-9._-]*", value) is None:
        raise ValueError(f"{name} has invalid characters")


def _sha256(value: str, name: str) -> None:
    if re.fullmatch(r"[0-9a-f]{64}", value) is None:
        raise ValueError(f"{name} must be a lowercase SHA-256 digest")


def _json_object(value: str, name: str) -> dict[str, object]:
    try:
        decoded: object = json.loads(value)
    except json.JSONDecodeError as exc:
        raise ValueError(f"{name} must contain JSON") from exc
    return _object(decoded, name)


def _object(value: object, name: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise TypeError(f"{name} must be an object")
    raw = cast(dict[object, object], value)
    if any(not isinstance(key, str) for key in raw):
        raise TypeError(f"{name} keys must be strings")
    return cast(dict[str, object], value)


def _list(value: object, name: str) -> list[object]:
    if not isinstance(value, list):
        raise TypeError(f"{name} must be an array")
    return cast(list[object], value)


def _string(payload: dict[str, object], key: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str):
        raise TypeError(f"{key} must be a string")
    return value


def _optional_string(payload: dict[str, object], key: str) -> str | None:
    value = payload.get(key)
    if value is None:
        return None
    if not isinstance(value, str):
        raise TypeError(f"{key} must be a string or null")
    return value


def _integer(payload: dict[str, object], key: str) -> int:
    value = payload.get(key)
    if not isinstance(value, int) or isinstance(value, bool):
        raise TypeError(f"{key} must be an integer")
    return value


def _boolean(payload: dict[str, object], key: str) -> bool:
    value = payload.get(key)
    if not isinstance(value, bool):
        raise TypeError(f"{key} must be a boolean")
    return value


def _datetime(value: object, name: str) -> datetime:
    if not isinstance(value, str):
        raise TypeError(f"{name} must be an RFC 3339 string")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{name} must be an RFC 3339 timestamp") from exc
    _strict_utc(parsed, name)
    return parsed


def _optional_datetime(value: object, name: str) -> datetime | None:
    return None if value is None else _datetime(value, name)


def sha256_bytes(value: bytes) -> str:
    return sha256(value).hexdigest()
