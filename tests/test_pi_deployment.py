"""Host deployment and acceptance controls, without Provider network access."""

from __future__ import annotations

import asyncio
import fcntl
import json
import multiprocessing
from multiprocessing.connection import Connection
from multiprocessing.process import BaseProcess
from pathlib import Path

import pytest

from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.model_provider import (
    ModelProviderProfile,
    load_builtin_model_provider_profile,
)
from market_impact_agent.pi_deployment import (
    install_runtime_acceptance,
    installed_permit,
    prepare_runtime,
    runtime_doctor,
)
from market_impact_agent.pi_runtime import (
    ModelSlots,
    PiRuntimeProvider,
    model_concurrency_limit,
    runtime_identity,
)
from market_impact_agent.runtime_store import ArtifactStore


def test_doctor_is_read_only_and_configuration_is_not_runtime_acceptance(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "model-admission"
    profile = load_builtin_model_provider_profile("pi-cpa-luna-max-v2")
    monkeypatch.setenv(profile.credential_env, "synthetic-doctor-secret")
    report = runtime_doctor((profile,))
    assert not root.exists()
    assert report["network_requests"] == 0 and report["ready"]
    assert "synthetic-doctor-secret" not in json.dumps(report)
    assert report["routes"][0]["credential_present"]  # type: ignore[index]
    assert not report["routes"][0]["runtime_accepted"]  # type: ignore[index]
    assert report["routes"][0]["context_window_tokens"] == 272_000  # type: ignore[index]
    assert report["routes"][0]["compaction_trigger_tokens"] == 258_000  # type: ignore[index]
    assert report["routes"][0]["context_policy_valid"] is True  # type: ignore[index]
    provider = PiRuntimeProvider(profile)
    with pytest.raises(PermissionError, match="accepted route"):
        provider.authorize_dispatch("unaccepted", "unaccepted")


def test_dependency_prepare_refuses_an_active_worker_lease(tmp_path: Path) -> None:
    root = tmp_path / "model-admission"
    root.mkdir()
    with (root / "runtime-build.lock").open("a+b") as active:
        fcntl.flock(active, fcntl.LOCK_SH)
        with pytest.raises(RuntimeError, match="workers are still active"):
            prepare_runtime()


def test_environment_cannot_expand_the_authorized_model_concurrency(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("MARKET_IMPACT_MODEL_MAX_CONCURRENT_REQUESTS", "2")
    assert model_concurrency_limit() == 2
    monkeypatch.setenv("MARKET_IMPACT_MODEL_MAX_CONCURRENT_REQUESTS", "4")
    with pytest.raises(ValueError, match="authorized maximum"):
        PiRuntimeProvider(load_builtin_model_provider_profile("pi-cpa-luna-max-v2"))
    with pytest.raises(ValueError, match="authorized maximum"):
        ModelSlots(tmp_path, "same-model", 4)


def test_admission_reopens_content_identified_route_evidence(tmp_path: Path) -> None:
    profile = load_builtin_model_provider_profile("pi-cpa-luna-max-v2")
    registration = {
        "runtime": runtime_identity(),
        "profiles": {"cpa": profile.to_dict()},
        "registration_hash": "synthetic-registration",
    }
    evidence = ArtifactStore(tmp_path / "acceptance-artifacts").put_json(
        {
            "registration": registration,
            "report": {
                "stage_passed": True,
                "registration_hash": "synthetic-registration",
            },
        }
    )
    record = {
        "build_hash": canonical_hash(runtime_identity()),
        "route_identities": [profile.route_identity],
        "evidence_hash": evidence.content_hash,
        "execution_capability": False,
    }
    record["acceptance_hash"] = canonical_hash(record)
    (tmp_path / "accepted-pi-runtime.json").write_text(json.dumps(record))
    permit = installed_permit(tmp_path)
    assert permit is not None
    permit.authorize(profile, runtime_identity(), "new-run", "new-run")
    evidence.path.write_text("{}")
    with pytest.raises(ValueError, match="evidence changed"):
        installed_permit(tmp_path)


def test_same_build_route_qualification_adds_without_evicting_existing_route(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("market_impact_agent.pi_runtime.shared_admission_root", lambda: tmp_path)
    first = load_builtin_model_provider_profile("pi-minimax-m3-v2")
    second = load_builtin_model_provider_profile("pi-cpa-luna-max-v2")

    def accept(profile: ModelProviderProfile, name: str) -> None:
        registration: dict[str, object] = {
            "runtime": runtime_identity(),
            "profiles": {name: profile.to_dict()},
        }
        registration["registration_hash"] = canonical_hash(registration)
        report = {
            "stage_passed": True,
            "registration_hash": registration["registration_hash"],
            "runtime": registration["runtime"],
            "reconciled": True,
        }
        install_runtime_acceptance(registration=registration, report=report)

    accept(first, "first")
    accept(second, "second")

    permit = installed_permit(tmp_path)
    assert permit is not None
    assert set(permit.route_identities) == {first.route_identity, second.route_identity}
    record = json.loads((tmp_path / "accepted-pi-runtime.json").read_text())
    assert record["schema_version"] == "market-impact.accepted-pi-runtime.v2"
    assert set(record["route_evidence"]) == {first.route_identity, second.route_identity}


def test_install_migrates_legacy_route_identity_only_from_same_build_evidence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("market_impact_agent.pi_runtime.shared_admission_root", lambda: tmp_path)
    legacy_profile = load_builtin_model_provider_profile("pi-minimax-m3-v2")
    legacy_registration = {
        "runtime": runtime_identity(),
        "profiles": {"legacy": legacy_profile.to_dict()},
        "registration_hash": "legacy-registration",
    }
    legacy_evidence = ArtifactStore(tmp_path / "acceptance-artifacts").put_json(
        {
            "registration": legacy_registration,
            "report": {
                "stage_passed": True,
                "registration_hash": "legacy-registration",
            },
        }
    )
    legacy_record: dict[str, object] = {
        "build_hash": canonical_hash(runtime_identity()),
        "route_identities": ["f" * 64],
        "evidence_hash": legacy_evidence.content_hash,
        "execution_capability": False,
    }
    legacy_record["acceptance_hash"] = canonical_hash(legacy_record)
    (tmp_path / "accepted-pi-runtime.json").write_text(json.dumps(legacy_record))

    new_profile = load_builtin_model_provider_profile("pi-cpa-luna-max-v2")
    registration: dict[str, object] = {
        "runtime": runtime_identity(),
        "profiles": {"new": new_profile.to_dict()},
    }
    registration["registration_hash"] = canonical_hash(registration)
    install_runtime_acceptance(
        registration=registration,
        report={
            "stage_passed": True,
            "registration_hash": registration["registration_hash"],
            "runtime": registration["runtime"],
            "reconciled": True,
        },
    )

    permit = installed_permit(tmp_path)
    assert permit is not None
    assert set(permit.route_identities) == {
        legacy_profile.route_identity,
        new_profile.route_identity,
    }
    assert "f" * 64 not in permit.route_identities


def test_new_qualified_build_replaces_but_archives_a_valid_old_acceptance(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("market_impact_agent.pi_runtime.shared_admission_root", lambda: tmp_path)
    old_profile = load_builtin_model_provider_profile("pi-minimax-m3-v2")
    old_runtime = {"adapter": "retired-build", "revision": "old"}
    old_build_hash = canonical_hash(old_runtime)
    old_registration = {
        "runtime": old_runtime,
        "profiles": {"retired": old_profile.to_dict()},
        "registration_hash": "retired-registration",
    }
    old_evidence = ArtifactStore(tmp_path / "acceptance-artifacts").put_json(
        {
            "registration": old_registration,
            "report": {
                "stage_passed": True,
                "registration_hash": "retired-registration",
            },
        }
    )
    old_record: dict[str, object] = {
        "build_hash": old_build_hash,
        "route_identities": ["e" * 64],
        "evidence_hash": old_evidence.content_hash,
        "execution_capability": False,
    }
    old_record["acceptance_hash"] = canonical_hash(old_record)
    (tmp_path / "accepted-pi-runtime.json").write_text(json.dumps(old_record))

    profile = load_builtin_model_provider_profile("pi-cpa-luna-max-v2")
    registration: dict[str, object] = {
        "runtime": runtime_identity(),
        "profiles": {"current": profile.to_dict()},
    }
    registration["registration_hash"] = canonical_hash(registration)
    install_runtime_acceptance(
        registration=registration,
        report={
            "stage_passed": True,
            "registration_hash": registration["registration_hash"],
            "runtime": registration["runtime"],
            "reconciled": True,
        },
    )

    permit = installed_permit(tmp_path)
    assert permit is not None
    assert permit.route_identities == (profile.route_identity,)
    archived = ArtifactStore(tmp_path / "acceptance-artifacts").read_json(
        canonical_hash(old_record)
    )
    assert archived == old_record


def test_new_build_refuses_a_v2_record_with_misbound_route_evidence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("market_impact_agent.pi_runtime.shared_admission_root", lambda: tmp_path)
    old_profile = load_builtin_model_provider_profile("pi-minimax-m3-v2")
    old_runtime = {"adapter": "retired-build", "revision": "old"}
    old_build_hash = canonical_hash(old_runtime)
    old_registration = {
        "runtime": old_runtime,
        "profiles": {"retired": old_profile.to_dict()},
        "registration_hash": "retired-registration",
    }
    old_evidence = ArtifactStore(tmp_path / "acceptance-artifacts").put_json(
        {
            "registration": old_registration,
            "report": {
                "stage_passed": True,
                "registration_hash": "retired-registration",
            },
        }
    )
    wrong_route = "e" * 64
    old_record: dict[str, object] = {
        "schema_version": "market-impact.accepted-pi-runtime.v2",
        "build_hash": old_build_hash,
        "route_identities": [wrong_route],
        "route_evidence": {wrong_route: old_evidence.content_hash},
        "execution_capability": False,
    }
    old_record["acceptance_hash"] = canonical_hash(old_record)
    (tmp_path / "accepted-pi-runtime.json").write_text(json.dumps(old_record))

    profile = load_builtin_model_provider_profile("pi-cpa-luna-max-v2")
    registration: dict[str, object] = {
        "runtime": runtime_identity(),
        "profiles": {"current": profile.to_dict()},
    }
    registration["registration_hash"] = canonical_hash(registration)
    with pytest.raises(ValueError, match="route is not present"):
        install_runtime_acceptance(
            registration=registration,
            report={
                "stage_passed": True,
                "registration_hash": registration["registration_hash"],
                "runtime": registration["runtime"],
                "reconciled": True,
            },
        )


def test_new_build_refuses_a_v2_record_missing_its_route_mapping(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("market_impact_agent.pi_runtime.shared_admission_root", lambda: tmp_path)
    old_runtime = {"adapter": "retired-build", "revision": "old"}
    old_profile = load_builtin_model_provider_profile("pi-minimax-m3-v2")
    old_registration = {
        "runtime": old_runtime,
        "profiles": {"retired": old_profile.to_dict()},
        "registration_hash": "retired-registration",
    }
    old_evidence = ArtifactStore(tmp_path / "acceptance-artifacts").put_json(
        {
            "registration": old_registration,
            "report": {
                "stage_passed": True,
                "registration_hash": "retired-registration",
            },
        }
    )
    malformed: dict[str, object] = {
        "schema_version": "market-impact.accepted-pi-runtime.v2",
        "build_hash": canonical_hash(old_runtime),
        "route_identities": ["e" * 64],
        "evidence_hash": old_evidence.content_hash,
        "execution_capability": False,
    }
    malformed["acceptance_hash"] = canonical_hash(malformed)
    (tmp_path / "accepted-pi-runtime.json").write_text(json.dumps(malformed))

    with pytest.raises(ValueError, match="schema does not match"):
        installed_permit(tmp_path)

    profile = load_builtin_model_provider_profile("pi-cpa-luna-max-v2")
    registration: dict[str, object] = {
        "runtime": runtime_identity(),
        "profiles": {"current": profile.to_dict()},
    }
    registration["registration_hash"] = canonical_hash(registration)
    with pytest.raises(ValueError, match="schema does not match"):
        install_runtime_acceptance(
            registration=registration,
            report={
                "stage_passed": True,
                "registration_hash": registration["registration_hash"],
                "runtime": registration["runtime"],
                "reconciled": True,
            },
        )


def test_same_build_refuses_a_v2_record_missing_its_route_mapping(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("market_impact_agent.pi_runtime.shared_admission_root", lambda: tmp_path)
    profile = load_builtin_model_provider_profile("pi-cpa-luna-max-v2")
    registration: dict[str, object] = {
        "runtime": runtime_identity(),
        "profiles": {"current": profile.to_dict()},
    }
    registration["registration_hash"] = canonical_hash(registration)
    evidence = ArtifactStore(tmp_path / "acceptance-artifacts").put_json(
        {
            "registration": registration,
            "report": {
                "stage_passed": True,
                "registration_hash": registration["registration_hash"],
            },
        }
    )
    malformed: dict[str, object] = {
        "schema_version": "market-impact.accepted-pi-runtime.v2",
        "build_hash": canonical_hash(runtime_identity()),
        "route_identities": [profile.route_identity],
        "evidence_hash": evidence.content_hash,
        "execution_capability": False,
    }
    malformed["acceptance_hash"] = canonical_hash(malformed)
    (tmp_path / "accepted-pi-runtime.json").write_text(json.dumps(malformed))

    with pytest.raises(ValueError, match="schema does not match"):
        install_runtime_acceptance(
            registration=registration,
            report={
                "stage_passed": True,
                "registration_hash": registration["registration_hash"],
                "runtime": registration["runtime"],
                "reconciled": True,
            },
        )


def _hold_slot(root: str, pipe: Connection) -> None:
    async def hold() -> None:
        lease = ModelSlots(Path(root), "shared-model")
        try:
            pipe.send("waiting")
            await lease.acquire()
            pipe.send("acquired")
            # Only this test process waits for an explicit release instruction.
            await asyncio.to_thread(pipe.recv)
        finally:
            lease.release()
            pipe.close()

    asyncio.run(hold())


def test_separate_processes_share_three_slots_and_crash_releases_only_lease(
    tmp_path: Path,
) -> None:
    ctx = multiprocessing.get_context("spawn")
    clients: list[tuple[BaseProcess, Connection]] = []
    try:
        for _ in range(4):
            parent, child = ctx.Pipe()
            process = ctx.Process(target=_hold_slot, args=(str(tmp_path), child))
            process.start()
            child.close()
            clients.append((process, parent))
            assert parent.poll(10) and parent.recv() == "waiting"
            if len(clients) <= 3:
                assert parent.poll(10) and parent.recv() == "acquired"
        assert not clients[3][1].poll(0.15)
        # Real process exit, not manual in-memory release of a fake semaphore.
        clients[0][0].terminate()
        clients[0][0].join(5)
        assert clients[3][1].poll(5) and clients[3][1].recv() == "acquired"
    finally:
        for process, pipe in clients:
            if process.is_alive():
                pipe.send("release")
                process.join(5)
            if process.is_alive():
                process.kill()
                process.join(5)
            pipe.close()
    assert not any(process.is_alive() for process, _ in clients)
