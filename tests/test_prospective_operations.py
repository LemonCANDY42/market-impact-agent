from datetime import UTC, datetime
from pathlib import Path
from shutil import copy2 as real_copy2
from typing import Any

import pytest

import market_impact_agent.prospective_operations as operations_module
from market_impact_agent.agent_schema import validate_agent_contract
from market_impact_agent.data_inputs import LocalDataSnapshotStore
from market_impact_agent.prospective_operations import (
    ProspectiveOperationsRegistration,
    StateBudgetExceeded,
    assert_within_state_budget,
    collect_operations_metrics,
    create_state_backup,
    restore_state_backup,
    verify_state_backup,
)

NOW = datetime(2026, 8, 28, 13, 0, tzinfo=UTC)


def test_operations_registration_freezes_fault_and_scale_acceptance() -> None:
    registration = ProspectiveOperationsRegistration.build(
        registered_at=NOW,
        required_job_ids=(
            "prospective-collection-job-" + "a" * 64,
            "prospective-collection-job-" + "b" * 64,
        ),
        required_supervisor_receipt_id="prospective-supervisor-receipt-" + "c" * 64,
        required_checkpoint_snapshot_set_ids=("prospective-checkpoint-snapshot-set-" + "d" * 64,),
        soak_duration_seconds=3600,
        maximum_state_bytes=1_000_000_000,
        maximum_lag_seconds=900,
        maximum_freeze_latency_ms=2_000,
        maximum_query_latency_ms=500,
        minimum_compression_ratio=1.0,
        backup_retention_count=3,
    )

    assert registration.registration_id == registration.expected_registration_id
    assert registration.required_faults == (
        "restart",
        "rate_limit",
        "corrupted_backup",
        "stale_source",
        "disk_budget_pressure",
        "restore",
    )
    assert (
        validate_agent_contract(
            registration.to_dict(), "prospective-operations-registration.schema.json"
        )
        == ()
    )
    assert registration.execution_capability is False


def test_backup_and_restore_verify_hashes_sqlite_and_artifact_identity(tmp_path: Path) -> None:
    state_root = tmp_path / "state"
    store = LocalDataSnapshotStore(state_root)
    raw_hash = store.put_raw(b"immutable source receipt")
    store.artifacts.put_json({"kind": "acceptance", "value": 1})

    manifest, backup_path = create_state_backup(
        state_root=state_root,
        backup_parent=tmp_path / "backups",
        created_at=NOW,
    )

    assert manifest.manifest_id == manifest.expected_manifest_id
    assert (
        validate_agent_contract(manifest.to_dict(), "prospective-backup-manifest.schema.json") == ()
    )
    verified = verify_state_backup(backup_path)
    assert verified.manifest_id == manifest.manifest_id
    assert verified.sqlite_integrity_ok is True
    assert verified.foreign_keys_ok is True

    restored_root = tmp_path / "restored"
    receipt = restore_state_backup(backup_path=backup_path, destination=restored_root)
    assert receipt.manifest_id == manifest.manifest_id
    assert receipt.restored_file_count == len(manifest.files)
    assert (
        LocalDataSnapshotStore(restored_root)
        .artifacts.get(raw_hash, media_type="application/octet-stream")
        .path.read_bytes()
        == b"immutable source receipt"
    )


def test_corrupted_backup_is_rejected_before_destination_mutation(tmp_path: Path) -> None:
    state_root = tmp_path / "state"
    store = LocalDataSnapshotStore(state_root)
    store.put_raw(b"immutable source receipt")
    _, backup_path = create_state_backup(
        state_root=state_root,
        backup_parent=tmp_path / "backups",
        created_at=NOW,
    )
    artifact = next((backup_path / "artifacts").iterdir())
    artifact.write_bytes(b"corrupted")
    destination = tmp_path / "restored"

    with pytest.raises(ValueError, match="hash mismatch"):
        restore_state_backup(backup_path=backup_path, destination=destination)

    assert not destination.exists()


def test_backup_with_unmanifested_file_is_rejected_and_never_restored(tmp_path: Path) -> None:
    state_root = tmp_path / "state"
    LocalDataSnapshotStore(state_root).put_raw(b"immutable source receipt")
    _, backup_path = create_state_backup(
        state_root=state_root,
        backup_parent=tmp_path / "backups",
        created_at=NOW,
    )
    (backup_path / "unmanifested.bin").write_bytes(b"must not be restored")
    destination = tmp_path / "restored"

    with pytest.raises(ValueError, match="unmanifested"):
        verify_state_backup(backup_path)
    with pytest.raises(ValueError, match="unmanifested"):
        restore_state_backup(backup_path=backup_path, destination=destination)

    assert not destination.exists()


def test_restore_destination_must_stay_outside_backup_root(tmp_path: Path) -> None:
    state_root = tmp_path / "state"
    LocalDataSnapshotStore(state_root).put_raw(b"immutable source receipt")
    _, backup_path = create_state_backup(
        state_root=state_root,
        backup_parent=tmp_path / "backups",
        created_at=NOW,
    )
    destination = backup_path / "restored"

    with pytest.raises(ValueError, match="outside the backup root"):
        restore_state_backup(backup_path=backup_path, destination=destination)

    assert not destination.exists()


def test_restore_final_verification_failure_never_publishes_destination(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state_root = tmp_path / "state"
    LocalDataSnapshotStore(state_root).put_raw(b"immutable source receipt")
    _, backup_path = create_state_backup(
        state_root=state_root,
        backup_parent=tmp_path / "backups",
        created_at=NOW,
    )
    destination = tmp_path / "restored"

    def corrupting_copy(source: Path, target: Path, *args: Any, **kwargs: Any) -> Path:
        copied = Path(real_copy2(source, target, *args, **kwargs))
        if "artifacts" in copied.parts:
            copied.write_bytes(b"corrupted after source verification")
        return copied

    monkeypatch.setattr(operations_module.shutil, "copy2", corrupting_copy)

    with pytest.raises(ValueError, match="restored files"):
        restore_state_backup(backup_path=backup_path, destination=destination)

    assert not destination.exists()


def test_backup_rejects_destination_inside_authoritative_state(tmp_path: Path) -> None:
    state_root = tmp_path / "state"
    LocalDataSnapshotStore(state_root)

    with pytest.raises(ValueError, match="outside"):
        create_state_backup(
            state_root=state_root,
            backup_parent=state_root / "backups",
            created_at=NOW,
        )


def test_operations_metrics_measure_deduplication_and_fail_closed_disk_budget(
    tmp_path: Path,
) -> None:
    state_root = tmp_path / "state"
    store = LocalDataSnapshotStore(state_root)
    first = store.put_raw(b"same receipt")
    second = store.put_raw(b"same receipt")

    metrics = collect_operations_metrics(state_root=state_root, measured_at=NOW)

    assert first == second
    assert metrics.artifact_file_count == 1
    assert metrics.total_state_bytes >= len(b"same receipt")
    assert metrics.execution_capability is False
    assert_within_state_budget(metrics, maximum_state_bytes=metrics.total_state_bytes)
    with pytest.raises(StateBudgetExceeded, match="state budget"):
        assert_within_state_budget(metrics, maximum_state_bytes=metrics.total_state_bytes - 1)


@pytest.mark.parametrize("link_parent", ("state-root", "operations"))
def test_metrics_and_backup_reject_directory_symlinks_without_following_them(
    tmp_path: Path,
    link_parent: str,
) -> None:
    state_root = tmp_path / "state"
    LocalDataSnapshotStore(state_root)
    outside = tmp_path / "outside"
    outside.mkdir()
    outside_file = outside / "must-not-be-read-or-copied"
    outside_file.write_bytes(b"outside authoritative state")
    if link_parent == "state-root":
        link = state_root / "linked-directory"
    else:
        link = state_root / "operations" / "linked-directory"
        link.parent.mkdir()
    link.symlink_to(outside, target_is_directory=True)

    with pytest.raises(ValueError, match="symlink"):
        collect_operations_metrics(state_root=state_root, measured_at=NOW)
    with pytest.raises(ValueError, match="symlink"):
        create_state_backup(
            state_root=state_root,
            backup_parent=tmp_path / "backups",
            created_at=NOW,
        )

    assert outside_file.read_bytes() == b"outside authoritative state"
    assert not list((tmp_path / "backups").glob("prospective-backup-manifest-*"))
