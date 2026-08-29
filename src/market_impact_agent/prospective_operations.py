from __future__ import annotations

import json
import os
import re
import shutil
import sqlite3
import tempfile
from dataclasses import dataclass
from datetime import UTC, datetime
from hashlib import sha256
from importlib import import_module
from pathlib import Path
from typing import Any, cast

from market_impact_agent.agent_contracts import canonical_hash, canonical_json_bytes
from market_impact_agent.data_inputs import data_snapshot_from_dict
from market_impact_agent.prospective_data import prospective_collection_policy_from_dict

PROSPECTIVE_OPERATIONS_REGISTRATION_SCHEMA = "market-impact.prospective-operations-registration.v1"
PROSPECTIVE_BACKUP_MANIFEST_SCHEMA = "market-impact.prospective-backup-manifest.v1"
_REQUIRED_FAULTS = (
    "restart",
    "rate_limit",
    "corrupted_backup",
    "stale_source",
    "disk_budget_pressure",
    "restore",
)
_BACKED_UP_DIRECTORIES = ("artifacts", "datasets", "collection-tracers", "operations")
_TERMINAL_OPPORTUNITY_OUTCOMES = frozenset(
    {"success", "source_failure", "collector_failure", "cancelled", "missed"}
)
_RECOVERABLE_OPPORTUNITY_OUTCOMES = frozenset({"in_progress", "captured"})


class StateBudgetExceeded(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class ProspectiveOperationsMetrics:
    measured_at: datetime
    total_state_bytes: int
    sqlite_bytes: int
    artifact_file_count: int
    artifact_bytes: int
    parquet_file_count: int
    parquet_bytes: int
    job_count: int
    opportunity_count: int
    terminal_opportunity_count: int
    recoverable_opportunity_count: int
    unreconciled_opportunity_count: int
    source_receipt_count: int
    observation_sighting_count: int
    unique_observation_version_count: int
    dataset_count: int
    dataset_row_count: int
    execution_capability: bool = False

    def __post_init__(self) -> None:
        _strict_utc(self.measured_at, "operations metrics measured_at")
        for name in (
            "total_state_bytes",
            "sqlite_bytes",
            "artifact_file_count",
            "artifact_bytes",
            "parquet_file_count",
            "parquet_bytes",
            "job_count",
            "opportunity_count",
            "terminal_opportunity_count",
            "recoverable_opportunity_count",
            "unreconciled_opportunity_count",
            "source_receipt_count",
            "observation_sighting_count",
            "unique_observation_version_count",
            "dataset_count",
            "dataset_row_count",
        ):
            if cast(int, getattr(self, name)) < 0:
                raise ValueError(f"{name} cannot be negative")
        if (
            self.terminal_opportunity_count
            + self.recoverable_opportunity_count
            + self.unreconciled_opportunity_count
            != self.opportunity_count
        ):
            raise ValueError("operations opportunity counts do not reconcile")
        if self.execution_capability:
            raise ValueError("operations metrics cannot grant execution capability")

    def to_dict(self) -> dict[str, object]:
        return {
            "measured_at": _timestamp(self.measured_at),
            "total_state_bytes": self.total_state_bytes,
            "sqlite_bytes": self.sqlite_bytes,
            "artifact_file_count": self.artifact_file_count,
            "artifact_bytes": self.artifact_bytes,
            "parquet_file_count": self.parquet_file_count,
            "parquet_bytes": self.parquet_bytes,
            "job_count": self.job_count,
            "opportunity_count": self.opportunity_count,
            "terminal_opportunity_count": self.terminal_opportunity_count,
            "recoverable_opportunity_count": self.recoverable_opportunity_count,
            "unreconciled_opportunity_count": self.unreconciled_opportunity_count,
            "source_receipt_count": self.source_receipt_count,
            "observation_sighting_count": self.observation_sighting_count,
            "unique_observation_version_count": self.unique_observation_version_count,
            "dataset_count": self.dataset_count,
            "dataset_row_count": self.dataset_row_count,
            "execution_capability": self.execution_capability,
        }


@dataclass(frozen=True, slots=True)
class ProspectiveOperationsRegistration:
    registration_id: str
    registered_at: datetime
    required_job_ids: tuple[str, ...]
    required_supervisor_receipt_id: str
    required_checkpoint_snapshot_set_ids: tuple[str, ...]
    required_faults: tuple[str, ...]
    soak_duration_seconds: int
    maximum_state_bytes: int
    maximum_lag_seconds: int
    maximum_freeze_latency_ms: int
    maximum_query_latency_ms: int
    minimum_compression_ratio: float
    backup_retention_count: int
    execution_capability: bool = False
    schema_version: str = PROSPECTIVE_OPERATIONS_REGISTRATION_SCHEMA

    def __post_init__(self) -> None:
        if self.schema_version != PROSPECTIVE_OPERATIONS_REGISTRATION_SCHEMA:
            raise ValueError("unsupported prospective operations registration schema")
        _strict_utc(self.registered_at, "operations registered_at")
        _unique_identifiers(
            self.required_job_ids,
            "prospective-collection-job-",
            "required job IDs",
        )
        if len(self.required_job_ids) < 2:
            raise ValueError("operations registration requires at least two collection Jobs")
        _identifier(
            self.required_supervisor_receipt_id,
            "prospective-supervisor-receipt-",
            "supervisor receipt ID",
        )
        _unique_identifiers(
            self.required_checkpoint_snapshot_set_ids,
            "prospective-checkpoint-snapshot-set-",
            "checkpoint Snapshot set IDs",
        )
        if not self.required_checkpoint_snapshot_set_ids:
            raise ValueError("operations registration requires checkpoint Snapshot set evidence")
        if self.required_faults != _REQUIRED_FAULTS:
            raise ValueError("operations registration fault matrix is incomplete or reordered")
        for name in (
            "soak_duration_seconds",
            "maximum_state_bytes",
            "maximum_lag_seconds",
            "maximum_freeze_latency_ms",
            "maximum_query_latency_ms",
            "backup_retention_count",
        ):
            if cast(int, getattr(self, name)) < 1:
                raise ValueError(f"{name} must be positive")
        if self.minimum_compression_ratio < 1:
            raise ValueError("minimum compression ratio must be at least 1")
        if self.execution_capability:
            raise ValueError("operations registration cannot grant execution capability")
        if self.registration_id != self.expected_registration_id:
            raise ValueError("operations registration_id does not match content")

    @property
    def expected_registration_id(self) -> str:
        return f"prospective-operations-registration-{canonical_hash(self.core_dict())}"

    def core_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "registered_at": _timestamp(self.registered_at),
            "required_job_ids": list(self.required_job_ids),
            "required_supervisor_receipt_id": self.required_supervisor_receipt_id,
            "required_checkpoint_snapshot_set_ids": list(self.required_checkpoint_snapshot_set_ids),
            "required_faults": list(self.required_faults),
            "soak_duration_seconds": self.soak_duration_seconds,
            "maximum_state_bytes": self.maximum_state_bytes,
            "maximum_lag_seconds": self.maximum_lag_seconds,
            "maximum_freeze_latency_ms": self.maximum_freeze_latency_ms,
            "maximum_query_latency_ms": self.maximum_query_latency_ms,
            "minimum_compression_ratio": self.minimum_compression_ratio,
            "backup_retention_count": self.backup_retention_count,
            "execution_capability": self.execution_capability,
        }

    def to_dict(self) -> dict[str, object]:
        return {**self.core_dict(), "registration_id": self.registration_id}

    @classmethod
    def build(
        cls,
        *,
        registered_at: datetime,
        required_job_ids: tuple[str, ...],
        required_supervisor_receipt_id: str,
        required_checkpoint_snapshot_set_ids: tuple[str, ...],
        soak_duration_seconds: int,
        maximum_state_bytes: int,
        maximum_lag_seconds: int,
        maximum_freeze_latency_ms: int,
        maximum_query_latency_ms: int,
        minimum_compression_ratio: float,
        backup_retention_count: int,
    ) -> ProspectiveOperationsRegistration:
        core: dict[str, object] = {
            "schema_version": PROSPECTIVE_OPERATIONS_REGISTRATION_SCHEMA,
            "registered_at": _timestamp(registered_at),
            "required_job_ids": list(required_job_ids),
            "required_supervisor_receipt_id": required_supervisor_receipt_id,
            "required_checkpoint_snapshot_set_ids": list(required_checkpoint_snapshot_set_ids),
            "required_faults": list(_REQUIRED_FAULTS),
            "soak_duration_seconds": soak_duration_seconds,
            "maximum_state_bytes": maximum_state_bytes,
            "maximum_lag_seconds": maximum_lag_seconds,
            "maximum_freeze_latency_ms": maximum_freeze_latency_ms,
            "maximum_query_latency_ms": maximum_query_latency_ms,
            "minimum_compression_ratio": minimum_compression_ratio,
            "backup_retention_count": backup_retention_count,
            "execution_capability": False,
        }
        return cls(
            registration_id=f"prospective-operations-registration-{canonical_hash(core)}",
            registered_at=registered_at,
            required_job_ids=required_job_ids,
            required_supervisor_receipt_id=required_supervisor_receipt_id,
            required_checkpoint_snapshot_set_ids=required_checkpoint_snapshot_set_ids,
            required_faults=_REQUIRED_FAULTS,
            soak_duration_seconds=soak_duration_seconds,
            maximum_state_bytes=maximum_state_bytes,
            maximum_lag_seconds=maximum_lag_seconds,
            maximum_freeze_latency_ms=maximum_freeze_latency_ms,
            maximum_query_latency_ms=maximum_query_latency_ms,
            minimum_compression_ratio=minimum_compression_ratio,
            backup_retention_count=backup_retention_count,
        )


@dataclass(frozen=True, slots=True)
class BackupFile:
    relative_path: str
    content_hash: str
    size_bytes: int

    def __post_init__(self) -> None:
        path = Path(self.relative_path)
        if path.is_absolute() or ".." in path.parts or not path.parts:
            raise ValueError("backup file path must be relative and contained")
        _sha256(self.content_hash, "backup file content hash")
        if self.size_bytes < 0:
            raise ValueError("backup file size cannot be negative")

    def to_dict(self) -> dict[str, object]:
        return {
            "relative_path": self.relative_path,
            "content_hash": self.content_hash,
            "size_bytes": self.size_bytes,
        }


@dataclass(frozen=True, slots=True)
class ProspectiveBackupManifest:
    manifest_id: str
    created_at: datetime
    files: tuple[BackupFile, ...]
    sqlite_table_counts: tuple[tuple[str, int], ...]
    data_snapshot_ids: tuple[str, ...]
    collection_policy_ids: tuple[str, ...]
    dataset_row_counts: tuple[tuple[str, int], ...]
    sqlite_integrity_ok: bool
    foreign_keys_ok: bool
    execution_capability: bool = False
    schema_version: str = PROSPECTIVE_BACKUP_MANIFEST_SCHEMA

    def __post_init__(self) -> None:
        if self.schema_version != PROSPECTIVE_BACKUP_MANIFEST_SCHEMA:
            raise ValueError("unsupported prospective backup manifest schema")
        _strict_utc(self.created_at, "backup created_at")
        if not self.files or self.files[0].relative_path != "index.sqlite3":
            raise ValueError("backup manifest must start with its SQLite snapshot")
        paths = tuple(item.relative_path for item in self.files)
        if paths != tuple(sorted(paths, key=lambda value: (value != "index.sqlite3", value))):
            raise ValueError("backup files must use canonical order")
        if len(paths) != len(set(paths)):
            raise ValueError("backup file paths must be unique")
        _ordered_counts(self.sqlite_table_counts, "SQLite table counts")
        _ordered_counts(self.dataset_row_counts, "dataset row counts")
        if tuple(sorted(self.data_snapshot_ids)) != self.data_snapshot_ids:
            raise ValueError("Data Snapshot IDs must be sorted")
        if tuple(sorted(self.collection_policy_ids)) != self.collection_policy_ids:
            raise ValueError("collection policy IDs must be sorted")
        if not self.sqlite_integrity_ok or not self.foreign_keys_ok:
            raise ValueError("backup manifest cannot claim an invalid SQLite snapshot")
        if self.execution_capability:
            raise ValueError("backup manifest cannot grant execution capability")
        if self.manifest_id != self.expected_manifest_id:
            raise ValueError("backup manifest_id does not match content")

    @property
    def expected_manifest_id(self) -> str:
        return f"prospective-backup-manifest-{canonical_hash(self.core_dict())}"

    def core_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "created_at": _timestamp(self.created_at),
            "files": [item.to_dict() for item in self.files],
            "sqlite_table_counts": dict(self.sqlite_table_counts),
            "data_snapshot_ids": list(self.data_snapshot_ids),
            "collection_policy_ids": list(self.collection_policy_ids),
            "dataset_row_counts": dict(self.dataset_row_counts),
            "sqlite_integrity_ok": self.sqlite_integrity_ok,
            "foreign_keys_ok": self.foreign_keys_ok,
            "execution_capability": self.execution_capability,
        }

    def to_dict(self) -> dict[str, object]:
        return {**self.core_dict(), "manifest_id": self.manifest_id}


@dataclass(frozen=True, slots=True)
class RestoreReceipt:
    manifest_id: str
    destination: Path
    restored_file_count: int
    sqlite_integrity_ok: bool
    foreign_keys_ok: bool
    execution_capability: bool = False


def collect_operations_metrics(
    *,
    state_root: Path,
    measured_at: datetime,
) -> ProspectiveOperationsMetrics:
    _strict_utc(measured_at, "operations metrics measured_at")
    if state_root.is_symlink():
        raise ValueError(f"prospective state root cannot be a symlink: {state_root}")
    root = state_root.resolve()
    _reject_tree_symlinks(root, "prospective state")
    index_path = root / "index.sqlite3"
    if not index_path.is_file():
        raise FileNotFoundError(f"prospective state index is missing: {index_path}")
    all_files = tuple(path for path in root.rglob("*") if path.is_file())
    total_state_bytes = sum(_existing_file_sizes(all_files))
    sqlite_paths = tuple(
        path
        for path in (index_path, root / "index.sqlite3-wal", root / "index.sqlite3-shm")
        if path.is_file()
    )
    artifact_paths = tuple(path for path in (root / "artifacts").rglob("*") if path.is_file())
    parquet_paths = tuple(path for path in (root / "datasets").rglob("*.parquet") if path.is_file())
    sqlite_sizes = _existing_file_sizes(sqlite_paths)
    artifact_sizes = _existing_file_sizes(artifact_paths)
    parquet_sizes = _existing_file_sizes(parquet_paths)
    connection = sqlite3.connect(f"file:{index_path.as_posix()}?mode=ro", uri=True)
    try:
        outcomes = _opportunity_outcome_counts(connection)
        opportunity_count = sum(outcomes.values())
        terminal_count = sum(
            count
            for outcome, count in outcomes.items()
            if outcome in _TERMINAL_OPPORTUNITY_OUTCOMES
        )
        recoverable_count = sum(
            count
            for outcome, count in outcomes.items()
            if outcome in _RECOVERABLE_OPPORTUNITY_OUTCOMES
        )
        unreconciled_count = opportunity_count - terminal_count - recoverable_count
        dataset_count = _table_count(connection, "prospective_dataset_manifests")
        dataset_row_count = _dataset_row_count(connection, root=root)
        metrics = ProspectiveOperationsMetrics(
            measured_at=measured_at,
            total_state_bytes=total_state_bytes,
            sqlite_bytes=sum(sqlite_sizes),
            artifact_file_count=len(artifact_sizes),
            artifact_bytes=sum(artifact_sizes),
            parquet_file_count=len(parquet_sizes),
            parquet_bytes=sum(parquet_sizes),
            job_count=_table_count(connection, "prospective_collection_jobs"),
            opportunity_count=opportunity_count,
            terminal_opportunity_count=terminal_count,
            recoverable_opportunity_count=recoverable_count,
            unreconciled_opportunity_count=unreconciled_count,
            source_receipt_count=_table_count(connection, "prospective_source_receipts"),
            observation_sighting_count=_table_count(
                connection, "prospective_observation_sightings"
            ),
            unique_observation_version_count=_table_count(
                connection, "prospective_observation_versions"
            ),
            dataset_count=dataset_count,
            dataset_row_count=dataset_row_count,
        )
    finally:
        connection.close()
    return metrics


def _existing_file_sizes(paths: tuple[Path, ...]) -> tuple[int, ...]:
    sizes: list[int] = []
    for path in paths:
        try:
            sizes.append(path.stat().st_size)
        except FileNotFoundError:
            # SQLite WAL sidecars may disappear after enumeration when another
            # one-shot collector closes its connection.
            continue
    return tuple(sizes)


def assert_within_state_budget(
    metrics: ProspectiveOperationsMetrics,
    *,
    maximum_state_bytes: int,
) -> None:
    if maximum_state_bytes < 1:
        raise ValueError("maximum state budget must be positive")
    if metrics.total_state_bytes > maximum_state_bytes:
        raise StateBudgetExceeded(
            "prospective state budget exceeded: "
            f"{metrics.total_state_bytes} > {maximum_state_bytes} bytes"
        )


def create_state_backup(
    *,
    state_root: Path,
    backup_parent: Path,
    created_at: datetime,
) -> tuple[ProspectiveBackupManifest, Path]:
    _strict_utc(created_at, "backup created_at")
    if state_root.is_symlink():
        raise ValueError(f"prospective state root cannot be a symlink: {state_root}")
    root = state_root.resolve()
    _reject_tree_symlinks(root, "backup source")
    parent = backup_parent.resolve()
    if parent == root or parent.is_relative_to(root):
        raise ValueError("backup destination must stay outside the authoritative state root")
    index_path = root / "index.sqlite3"
    if not index_path.is_file():
        raise FileNotFoundError(f"prospective state index is missing: {index_path}")
    parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    temporary = Path(tempfile.mkdtemp(prefix=".tmp-prospective-backup-", dir=parent))
    try:
        _backup_sqlite(index_path, temporary / "index.sqlite3")
        for directory_name in _BACKED_UP_DIRECTORIES:
            source_directory = root / directory_name
            if source_directory.exists():
                _copy_immutable_tree(source_directory, temporary / directory_name)
        files = _inventory_files(temporary)
        inventory = _sqlite_inventory(temporary / "index.sqlite3", backup_root=temporary)
        core: dict[str, object] = {
            "schema_version": PROSPECTIVE_BACKUP_MANIFEST_SCHEMA,
            "created_at": _timestamp(created_at),
            "files": [item.to_dict() for item in files],
            "sqlite_table_counts": dict(inventory[0]),
            "data_snapshot_ids": list(inventory[1]),
            "collection_policy_ids": list(inventory[2]),
            "dataset_row_counts": dict(inventory[3]),
            "sqlite_integrity_ok": inventory[4],
            "foreign_keys_ok": inventory[5],
            "execution_capability": False,
        }
        manifest = ProspectiveBackupManifest(
            manifest_id=f"prospective-backup-manifest-{canonical_hash(core)}",
            created_at=created_at,
            files=files,
            sqlite_table_counts=inventory[0],
            data_snapshot_ids=inventory[1],
            collection_policy_ids=inventory[2],
            dataset_row_counts=inventory[3],
            sqlite_integrity_ok=inventory[4],
            foreign_keys_ok=inventory[5],
        )
        (temporary / "manifest.json").write_bytes(canonical_json_bytes(manifest.to_dict()))
        os.chmod(temporary / "manifest.json", 0o600)
        destination = parent / manifest.manifest_id
        if destination.exists():
            existing = verify_state_backup(destination)
            if existing.manifest_id != manifest.manifest_id:
                raise ValueError("backup identity conflict")
            return existing, destination
        temporary.replace(destination)
        os.chmod(destination, 0o700)
        return manifest, destination
    finally:
        if temporary.exists():
            shutil.rmtree(temporary)


def verify_state_backup(backup_path: Path) -> ProspectiveBackupManifest:
    if backup_path.is_symlink():
        raise ValueError(f"backup root cannot be a symlink: {backup_path}")
    root = backup_path.resolve()
    _reject_tree_symlinks(root, "backup")
    payload = json.loads((root / "manifest.json").read_text(encoding="utf-8"))
    manifest = prospective_backup_manifest_from_dict(payload)
    expected_paths = {"manifest.json", *(item.relative_path for item in manifest.files)}
    actual_paths = {path.relative_to(root).as_posix() for path in root.rglob("*") if path.is_file()}
    unmanifested_paths = sorted(actual_paths - expected_paths)
    if unmanifested_paths:
        raise ValueError(f"backup contains unmanifested file: {unmanifested_paths[0]}")
    for item in manifest.files:
        path = root / item.relative_path
        if not path.is_file():
            raise ValueError(f"backup file is missing: {item.relative_path}")
        digest, size = _hash_file(path)
        if digest != item.content_hash or size != item.size_bytes:
            raise ValueError(f"backup file hash mismatch: {item.relative_path}")
    inventory = _sqlite_inventory(root / "index.sqlite3", backup_root=root)
    if inventory[0] != manifest.sqlite_table_counts:
        raise ValueError("backup SQLite table counts do not match the manifest")
    if inventory[1] != manifest.data_snapshot_ids:
        raise ValueError("backup Data Snapshot identities do not match the manifest")
    if inventory[2] != manifest.collection_policy_ids:
        raise ValueError("backup collection policy identities do not match the manifest")
    if inventory[3] != manifest.dataset_row_counts:
        raise ValueError("backup dataset row counts do not match the manifest")
    if inventory[4] != manifest.sqlite_integrity_ok or inventory[5] != manifest.foreign_keys_ok:
        raise ValueError("backup SQLite verification does not match the manifest")
    return manifest


def restore_state_backup(*, backup_path: Path, destination: Path) -> RestoreReceipt:
    backup_root = backup_path.resolve()
    target = destination.resolve()
    if target == backup_root or target.is_relative_to(backup_root):
        raise ValueError("restore destination must stay outside the backup root")
    manifest = verify_state_backup(backup_path)
    if target.exists():
        raise FileExistsError("restore destination must not already exist")
    target.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    temporary = Path(tempfile.mkdtemp(prefix=".tmp-prospective-restore-", dir=target.parent))
    try:
        for item in manifest.files:
            source = backup_root / item.relative_path
            restored = temporary / item.relative_path
            restored.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
            shutil.copy2(source, restored)
            os.chmod(restored, 0o600)
        restored_manifest = _manifest_for_restored_state(manifest, temporary)
        temporary.replace(target)
        os.chmod(target, 0o700)
        return RestoreReceipt(
            manifest_id=restored_manifest.manifest_id,
            destination=target,
            restored_file_count=len(restored_manifest.files),
            sqlite_integrity_ok=restored_manifest.sqlite_integrity_ok,
            foreign_keys_ok=restored_manifest.foreign_keys_ok,
        )
    finally:
        if temporary.exists():
            shutil.rmtree(temporary)


def prospective_backup_manifest_from_dict(value: object) -> ProspectiveBackupManifest:
    payload = _object(value, "prospective backup manifest")
    files_raw = payload.get("files")
    if not isinstance(files_raw, list):
        raise TypeError("backup manifest files must be an array")
    file_values = cast(list[object], files_raw)
    files = tuple(
        BackupFile(
            relative_path=_string(item, "relative_path"),
            content_hash=_string(item, "content_hash"),
            size_bytes=_integer(item, "size_bytes"),
        )
        for item in (_object(raw, "backup file") for raw in file_values)
    )
    return ProspectiveBackupManifest(
        schema_version=_string(payload, "schema_version"),
        manifest_id=_string(payload, "manifest_id"),
        created_at=_datetime(_string(payload, "created_at"), "created_at"),
        files=files,
        sqlite_table_counts=_count_items(payload.get("sqlite_table_counts"), "table counts"),
        data_snapshot_ids=_strings(payload.get("data_snapshot_ids"), "data_snapshot_ids"),
        collection_policy_ids=_strings(
            payload.get("collection_policy_ids"), "collection_policy_ids"
        ),
        dataset_row_counts=_count_items(payload.get("dataset_row_counts"), "dataset counts"),
        sqlite_integrity_ok=_boolean(payload, "sqlite_integrity_ok"),
        foreign_keys_ok=_boolean(payload, "foreign_keys_ok"),
        execution_capability=_boolean(payload, "execution_capability"),
    )


def _manifest_for_restored_state(
    source_manifest: ProspectiveBackupManifest,
    restored_root: Path,
) -> ProspectiveBackupManifest:
    files = _inventory_files(restored_root)
    if files != source_manifest.files:
        raise ValueError("restored files do not match the backup manifest")
    inventory = _sqlite_inventory(restored_root / "index.sqlite3", backup_root=restored_root)
    if (
        inventory[0] != source_manifest.sqlite_table_counts
        or inventory[1] != source_manifest.data_snapshot_ids
        or inventory[2] != source_manifest.collection_policy_ids
        or inventory[3] != source_manifest.dataset_row_counts
    ):
        raise ValueError("restored state relationships do not match the backup manifest")
    return source_manifest


def _backup_sqlite(source_path: Path, destination_path: Path) -> None:
    source = sqlite3.connect(source_path)
    destination = sqlite3.connect(destination_path)
    try:
        source.execute("PRAGMA busy_timeout = 5000")
        source.backup(destination)
        destination.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    finally:
        destination.close()
        source.close()
    os.chmod(destination_path, 0o600)


def _copy_immutable_tree(source: Path, destination: Path) -> None:
    if source.is_symlink():
        raise ValueError(f"backup source cannot contain symlinks: {source}")
    for path in sorted(source.rglob("*")):
        if path.is_symlink():
            raise ValueError(f"backup source cannot contain symlinks: {path}")
        if not path.is_file():
            continue
        relative = path.relative_to(source)
        target = destination / relative
        target.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        shutil.copy2(path, target)
        os.chmod(target, 0o600)


def _reject_tree_symlinks(root: Path, name: str) -> None:
    for path in root.rglob("*"):
        if path.is_symlink():
            raise ValueError(f"{name} cannot contain symlinks: {path}")


def _inventory_files(root: Path) -> tuple[BackupFile, ...]:
    paths = [root / "index.sqlite3"]
    for directory_name in _BACKED_UP_DIRECTORIES:
        directory = root / directory_name
        if directory.exists():
            paths.extend(path for path in directory.rglob("*") if path.is_file())
    entries: list[BackupFile] = []
    for path in paths:
        digest, size = _hash_file(path)
        entries.append(
            BackupFile(
                relative_path=path.relative_to(root).as_posix(),
                content_hash=digest,
                size_bytes=size,
            )
        )
    return tuple(
        sorted(
            entries,
            key=lambda item: (item.relative_path != "index.sqlite3", item.relative_path),
        )
    )


def _sqlite_inventory(
    path: Path,
    *,
    backup_root: Path,
) -> tuple[
    tuple[tuple[str, int], ...],
    tuple[str, ...],
    tuple[str, ...],
    tuple[tuple[str, int], ...],
    bool,
    bool,
]:
    uri = f"file:{path.as_posix()}?mode=ro&immutable=1"
    connection = sqlite3.connect(uri, uri=True)
    connection.row_factory = sqlite3.Row
    try:
        integrity = connection.execute("PRAGMA integrity_check").fetchone()
        integrity_ok = integrity is not None and cast(str, integrity[0]) == "ok"
        foreign_keys_ok = not connection.execute("PRAGMA foreign_key_check").fetchall()
        table_names = tuple(
            cast(str, row[0])
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table' "
                "AND name NOT LIKE 'sqlite_%' ORDER BY name"
            ).fetchall()
        )
        counts = tuple(
            (name, cast(int, connection.execute(f'SELECT COUNT(*) FROM "{name}"').fetchone()[0]))
            for name in table_names
        )
        snapshots = _column_values(connection, "data_snapshots", "snapshot_id")
        policies = _column_values(
            connection,
            "prospective_collection_policies",
            "policy_id",
        )
        _verify_snapshot_reconstruction(connection, backup_root=backup_root)
        _verify_policy_reconstruction(connection, backup_root=backup_root)
        dataset_counts = _dataset_counts(connection, backup_root=backup_root)
    finally:
        connection.close()
    if not integrity_ok or not foreign_keys_ok:
        raise ValueError("SQLite backup failed integrity or foreign-key verification")
    return counts, snapshots, policies, dataset_counts, integrity_ok, foreign_keys_ok


def _column_values(connection: sqlite3.Connection, table: str, column: str) -> tuple[str, ...]:
    exists = connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?", (table,)
    ).fetchone()
    if exists is None:
        return ()
    return tuple(
        cast(str, row[0])
        for row in connection.execute(
            f'SELECT "{column}" FROM "{table}" ORDER BY "{column}"'
        ).fetchall()
    )


def _table_count(connection: sqlite3.Connection, table: str) -> int:
    exists = connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?", (table,)
    ).fetchone()
    if exists is None:
        return 0
    row = connection.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()
    if row is None:
        raise RuntimeError(f"could not count SQLite table: {table}")
    return cast(int, row[0])


def _opportunity_outcome_counts(connection: sqlite3.Connection) -> dict[str, int]:
    table = connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' "
        "AND name = 'prospective_collection_opportunities'"
    ).fetchone()
    if table is None:
        return {}
    return {
        cast(str, row[0]): cast(int, row[1])
        for row in connection.execute(
            "SELECT outcome, COUNT(*) FROM prospective_collection_opportunities GROUP BY outcome"
        ).fetchall()
    }


def _dataset_row_count(connection: sqlite3.Connection, *, root: Path) -> int:
    table = connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' "
        "AND name = 'prospective_dataset_manifests'"
    ).fetchone()
    if table is None:
        return 0
    total = 0
    for row in connection.execute(
        "SELECT artifact_hash FROM prospective_dataset_manifests ORDER BY dataset_id"
    ).fetchall():
        artifact_hash = cast(str, row[0])
        payload = _object(
            json.loads((root / "artifacts" / artifact_hash).read_text(encoding="utf-8")),
            "prospective dataset manifest",
        )
        total += _integer(payload, "observation_count")
    return total


def _dataset_counts(
    connection: sqlite3.Connection,
    *,
    backup_root: Path,
) -> tuple[tuple[str, int], ...]:
    table = connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' "
        "AND name = 'prospective_dataset_manifests'"
    ).fetchone()
    if table is None:
        return ()
    rows = connection.execute(
        "SELECT dataset_id, artifact_hash FROM prospective_dataset_manifests ORDER BY dataset_id"
    ).fetchall()
    result: list[tuple[str, int]] = []
    for row in rows:
        artifact_hash = cast(str, row["artifact_hash"])
        payload = _object(
            json.loads((backup_root / "artifacts" / artifact_hash).read_text(encoding="utf-8")),
            "prospective dataset manifest",
        )
        dataset_id = _string(payload, "dataset_id")
        if dataset_id != cast(str, row["dataset_id"]):
            raise ValueError("dataset manifest identity mismatch during backup")
        observation_count = _integer(payload, "observation_count")
        partitions = payload.get("partitions")
        if not isinstance(partitions, list):
            raise TypeError("dataset manifest partitions must be an array")
        partition_values = cast(list[object], partitions)
        partition_rows = 0
        for raw in partition_values:
            partition = _object(raw, "dataset partition")
            relative_path = _string(partition, "relative_path")
            content_hash = _string(partition, "content_hash")
            row_count = _integer(partition, "row_count")
            parquet_path = backup_root / "datasets" / relative_path
            digest, _ = _hash_file(parquet_path)
            if digest != content_hash:
                raise ValueError("dataset partition hash mismatch during backup")
            try:
                parquet: Any = import_module("pyarrow.parquet")
            except ImportError as exc:
                raise RuntimeError(
                    "pyarrow is required to verify prospective dataset row counts"
                ) from exc
            if parquet.ParquetFile(parquet_path).metadata.num_rows != row_count:
                raise ValueError("dataset Parquet row count mismatch during backup")
            partition_rows += row_count
        if partition_rows != observation_count:
            raise ValueError("dataset row counts do not reconcile during backup")
        result.append((dataset_id, observation_count))
    return tuple(result)


def _verify_snapshot_reconstruction(
    connection: sqlite3.Connection,
    *,
    backup_root: Path,
) -> None:
    table = connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'data_snapshots'"
    ).fetchone()
    if table is None:
        return
    for row in connection.execute(
        "SELECT snapshot_id, artifact_hash FROM data_snapshots ORDER BY snapshot_id"
    ).fetchall():
        artifact_hash = cast(str, row["artifact_hash"])
        payload = json.loads(
            (backup_root / "artifacts" / artifact_hash).read_text(encoding="utf-8")
        )
        snapshot = data_snapshot_from_dict(payload)
        if snapshot.snapshot_id != cast(str, row["snapshot_id"]):
            raise ValueError("Data Snapshot reconstruction identity mismatch during backup")


def _verify_policy_reconstruction(
    connection: sqlite3.Connection,
    *,
    backup_root: Path,
) -> None:
    table = connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' "
        "AND name = 'prospective_collection_policies'"
    ).fetchone()
    if table is None:
        return
    for row in connection.execute(
        "SELECT policy_id, artifact_hash FROM prospective_collection_policies ORDER BY policy_id"
    ).fetchall():
        artifact_hash = cast(str, row["artifact_hash"])
        payload = json.loads(
            (backup_root / "artifacts" / artifact_hash).read_text(encoding="utf-8")
        )
        policy = prospective_collection_policy_from_dict(payload)
        if policy.policy_id != cast(str, row["policy_id"]):
            raise ValueError("Collection Policy reconstruction identity mismatch during backup")


def _hash_file(path: Path) -> tuple[str, int]:
    digest = sha256()
    size = 0
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
            size += len(chunk)
    return digest.hexdigest(), size


def _unique_identifiers(values: tuple[str, ...], prefix: str, name: str) -> None:
    if tuple(sorted(values)) != values or len(values) != len(set(values)):
        raise ValueError(f"{name} must be unique and sorted")
    for value in values:
        _identifier(value, prefix, name)


def _identifier(value: str, prefix: str, name: str) -> None:
    if re.fullmatch(re.escape(prefix) + r"[0-9a-f]{64}", value) is None:
        raise ValueError(f"invalid {name}")


def _ordered_counts(values: tuple[tuple[str, int], ...], name: str) -> None:
    keys = tuple(key for key, _ in values)
    if keys != tuple(sorted(keys)) or len(keys) != len(set(keys)):
        raise ValueError(f"{name} must have unique sorted keys")
    if any(count < 0 for _, count in values):
        raise ValueError(f"{name} cannot be negative")


def _count_items(value: object, name: str) -> tuple[tuple[str, int], ...]:
    payload = _object(value, name)
    items: list[tuple[str, int]] = []
    for key in sorted(payload):
        count = payload[key]
        if isinstance(count, bool) or not isinstance(count, int):
            raise TypeError(f"{name} values must be integers")
        items.append((key, count))
    return tuple(items)


def _strings(value: object, name: str) -> tuple[str, ...]:
    if not isinstance(value, list):
        raise TypeError(f"{name} must be an array of strings")
    items = cast(list[object], value)
    if any(not isinstance(item, str) for item in items):
        raise TypeError(f"{name} must be an array of strings")
    return tuple(cast(list[str], items))


def _object(value: object, name: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise TypeError(f"{name} must be an object")
    payload = cast(dict[object, object], value)
    if any(not isinstance(key, str) for key in payload):
        raise TypeError(f"{name} must be an object")
    return cast(dict[str, object], payload)


def _string(value: dict[str, object], name: str) -> str:
    result = value.get(name)
    if not isinstance(result, str):
        raise TypeError(f"{name} must be a string")
    return result


def _integer(value: dict[str, object], name: str) -> int:
    result = value.get(name)
    if isinstance(result, bool) or not isinstance(result, int):
        raise TypeError(f"{name} must be an integer")
    return result


def _boolean(value: dict[str, object], name: str) -> bool:
    result = value.get(name)
    if not isinstance(result, bool):
        raise TypeError(f"{name} must be a boolean")
    return result


def _sha256(value: str, name: str) -> None:
    if re.fullmatch(r"[0-9a-f]{64}", value) is None:
        raise ValueError(f"{name} must be a sha256 hex digest")


def _strict_utc(value: datetime, name: str) -> None:
    offset = value.utcoffset()
    if value.tzinfo is None or offset is None or offset.total_seconds() != 0:
        raise ValueError(f"{name} must be a UTC timestamp")


def _timestamp(value: datetime) -> str:
    _strict_utc(value, "timestamp")
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _datetime(value: str, name: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    _strict_utc(parsed, name)
    return parsed
