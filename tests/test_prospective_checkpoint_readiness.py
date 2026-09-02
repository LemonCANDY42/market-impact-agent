from __future__ import annotations

import json
import sqlite3
import threading
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, cast

import pytest

from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.agent_schema import validate_agent_contract
from market_impact_agent.cli import build_parser, main
from market_impact_agent.data_inputs import (
    DataFetchStatus,
    DataPITLane,
    DataProviderAttempt,
    DataQuery,
    DataSnapshot,
    DataSourceBinding,
    LocalDataSnapshotStore,
)
from market_impact_agent.observations import ObservationCapability
from market_impact_agent.prospective_checkpoint_readiness import (
    PROSPECTIVE_CHECKPOINT_ADMISSION_TIMING_PROTOCOL,
    CheckpointReadinessStatus,
    ProspectiveCheckpointAdmissionStore,
    ProspectiveCheckpointRouteAdmission,
    ProspectiveCheckpointRouteBinding,
    ProspectiveCheckpointRoutePlan,
    evaluate_prospective_checkpoint_readiness,
    load_prospective_checkpoint_route_plan,
)
from market_impact_agent.prospective_collection_runtime import (
    ProspectiveCollectionAdapterKind,
    ProspectiveCollectionJob,
    ProspectiveCollectionRuntime,
)
from market_impact_agent.prospective_data import (
    ProspectiveCollectionPolicy,
    ProspectiveObservationVersionRef,
)
from market_impact_agent.prospective_diagnostic import (
    ProspectiveDiagnosticRegistration,
    load_prospective_diagnostic_registration,
)
from market_impact_agent.source_acceptance import (
    SourceAcceptanceGate,
    SourceAcceptanceGateResult,
    SourceAcceptanceStatus,
    SourceRouteAcceptanceDeclaration,
    SourceRouteAcceptanceReport,
)

ROOT = Path(__file__).parents[1]
REGISTRATION_PATH = ROOT / "examples/research/prospective-diagnostic-registration-v3.json"
JOB_ID = "prospective-collection-job-" + "a" * 64
POLICY_ID = "prospective-collection-policy-" + "b" * 64
REPORT_ID = "source-route-acceptance-report-" + "c" * 64
ADMITTED_AT = datetime(2026, 8, 29, 8, 22, tzinfo=UTC)


@dataclass(frozen=True)
class _FakePolicy:
    policy_id: str = POLICY_ID
    capability: ObservationCapability = ObservationCapability.EVENT_REVELATION
    poll_interval_seconds: int = 300
    maximum_gap_seconds: int = 900


@dataclass(frozen=True)
class _FakeJob:
    collection_policy_id: str = POLICY_ID
    source_acceptance_report_id: str = REPORT_ID
    starts_at: datetime = ADMITTED_AT


@dataclass(frozen=True)
class _FakeDeclaration:
    capability: ObservationCapability = ObservationCapability.EVENT_REVELATION
    source_config_hash: str = "d" * 64
    provider_id: str = "csrc-official-news"
    upstream_source: str = "csrc-official-news"
    semantic_scope: str = "official_capital_market_policy_publication"


@dataclass(frozen=True)
class _FakeReport:
    report_id: str = REPORT_ID
    accepted: bool = True
    declaration: _FakeDeclaration = _FakeDeclaration()


@dataclass(frozen=True)
class _FakeHealth:
    status: str = "active"
    next_due_at: datetime = ADMITTED_AT + timedelta(minutes=5)
    backoff_until: datetime | None = None
    last_outcome: str | None = None
    lag_seconds: int = 0
    state_updated_at: datetime = ADMITTED_AT


@dataclass(frozen=True)
class _FakeOpportunity:
    scheduled_for: datetime
    outcome: str
    started_at: datetime | None = None
    completed_at: datetime | None = None


class _FakeJournal:
    def __init__(
        self,
        candidates: tuple[ProspectiveObservationVersionRef, ...],
        policy: _FakePolicy,
    ) -> None:
        self._candidates = candidates
        self._policy = policy

    def policy(self, policy_id: str) -> Any:
        assert policy_id == POLICY_ID
        return self._policy

    def receipt_coverage_errors(
        self,
        *,
        policy_id: str,
        window_start: datetime,
        not_after: datetime,
    ) -> tuple[str, ...]:
        assert policy_id == POLICY_ID and window_start == ADMITTED_AT and not_after >= window_start
        return ("journal_no_receipt_before_cutoff",)

    def observation_version_refs(
        self,
        *,
        policy_id: str,
        capability: ObservationCapability,
        not_before: datetime,
        not_after: datetime,
    ) -> tuple[ProspectiveObservationVersionRef, ...]:
        assert policy_id == POLICY_ID
        assert capability is ObservationCapability.EVENT_REVELATION
        return tuple(
            item for item in self._candidates if not_before <= item.first_available_at <= not_after
        )


class _FakeRuntime:
    def __init__(
        self,
        candidates: tuple[ProspectiveObservationVersionRef, ...],
        *,
        health: _FakeHealth | None = None,
        policy: _FakePolicy | None = None,
        opportunities: tuple[_FakeOpportunity, ...] = (),
        declaration: _FakeDeclaration | None = None,
    ) -> None:
        self.journal = _FakeJournal(candidates, _FakePolicy() if policy is None else policy)
        self._health = _FakeHealth() if health is None else health
        self._opportunities = opportunities
        self._declaration = _FakeDeclaration() if declaration is None else declaration

    def job(self, job_id: str) -> Any:
        assert job_id == JOB_ID
        return _FakeJob()

    def source_acceptance_report(self, job_id: str) -> Any:
        assert job_id == JOB_ID
        return _FakeReport(declaration=self._declaration)

    def health(self, job_id: str, *, now: datetime) -> Any:
        assert job_id == JOB_ID
        assert now >= ADMITTED_AT
        return self._health

    def opportunities(self, job_id: str) -> tuple[_FakeOpportunity, ...]:
        assert job_id == JOB_ID
        return self._opportunities


def _registration() -> ProspectiveDiagnosticRegistration:
    return load_prospective_diagnostic_registration(REGISTRATION_PATH)


def _plan() -> ProspectiveCheckpointRoutePlan:
    registration = _registration()
    return ProspectiveCheckpointRoutePlan.build(
        registration_id=registration.registration_id,
        bindings=(
            ProspectiveCheckpointRouteBinding(
                checkpoint_key="next-a-share-policy-event",
                capability=ObservationCapability.EVENT_REVELATION,
                route_kind="official_event",
                job_id=JOB_ID,
            ),
        ),
    )


def _runtime(
    candidates: tuple[ProspectiveObservationVersionRef, ...] = (),
    **kwargs: Any,
) -> ProspectiveCollectionRuntime:
    return cast(ProspectiveCollectionRuntime, _FakeRuntime(candidates, **kwargs))


def _admissions(tmp_path: Path) -> ProspectiveCheckpointAdmissionStore:
    store = ProspectiveCheckpointAdmissionStore(
        tmp_path / "state",
        clock=lambda: ADMITTED_AT,
    )
    store.admit(route_plan=_plan(), registration=_registration(), runtime=_runtime())
    return store


def test_route_plan_is_content_identified_canonical_and_schema_valid(tmp_path: Path) -> None:
    plan = _plan()
    assert plan.plan_id == plan.expected_plan_id
    assert (
        validate_agent_contract(plan.to_dict(), "prospective-checkpoint-route-plan.schema.json")
        == ()
    )
    path = tmp_path / "plan.json"
    path.write_text(json.dumps(plan.to_dict()), encoding="utf-8")
    assert load_prospective_checkpoint_route_plan(path) == plan

    payload = plan.to_dict()
    cast(list[dict[str, object]], payload["bindings"])[0]["route_kind"] = "post_hoc_route"
    path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ValueError, match="plan_id"):
        load_prospective_checkpoint_route_plan(path)


def test_route_plan_allows_multiple_jobs_for_one_semantic_news_route() -> None:
    registration = _registration()
    plan = ProspectiveCheckpointRoutePlan.build(
        registration_id=registration.registration_id,
        bindings=(
            ProspectiveCheckpointRouteBinding(
                checkpoint_key="next-a-share-policy-event",
                capability=ObservationCapability.EVENT_REVELATION,
                route_kind="established_news",
                job_id="prospective-collection-job-" + "1" * 64,
            ),
            ProspectiveCheckpointRouteBinding(
                checkpoint_key="next-a-share-policy-event",
                capability=ObservationCapability.EVENT_REVELATION,
                route_kind="established_news",
                job_id="prospective-collection-job-" + "2" * 64,
            ),
        ),
    )

    assert tuple(item.job_id for item in plan.bindings) == (
        "prospective-collection-job-" + "1" * 64,
        "prospective-collection-job-" + "2" * 64,
    )


@pytest.mark.parametrize("upstream_source", ("tushare-etf-basic", "tushare-stock-basic"))
def test_route_admission_accepts_exact_tradable_instrument_master_sources(
    tmp_path: Path,
    upstream_source: str,
) -> None:
    registration = _registration()
    plan = ProspectiveCheckpointRoutePlan.build(
        registration_id=registration.registration_id,
        bindings=(
            ProspectiveCheckpointRouteBinding(
                checkpoint_key="next-a-share-policy-event",
                capability=ObservationCapability.EXPOSURE_CANDIDATES,
                route_kind="tradable_instrument_master",
                job_id=JOB_ID,
            ),
        ),
    )
    runtime = _runtime(
        policy=_FakePolicy(capability=ObservationCapability.EXPOSURE_CANDIDATES),
        declaration=_FakeDeclaration(
            capability=ObservationCapability.EXPOSURE_CANDIDATES,
            provider_id="tushare-observation",
            upstream_source=upstream_source,
            semantic_scope="aggregated_source_observation_actual_receipt_only",
        ),
    )
    store = ProspectiveCheckpointAdmissionStore(
        tmp_path / "state",
        clock=lambda: ADMITTED_AT,
    )

    admission = store.admit(
        route_plan=plan,
        registration=registration,
        runtime=runtime,
    )

    assert admission.route_plan_id == plan.plan_id


def test_readiness_keeps_each_job_bound_to_a_shared_news_semantic(
    tmp_path: Path,
) -> None:
    registration = _registration()
    job_ids = (
        "prospective-collection-job-" + "1" * 64,
        "prospective-collection-job-" + "2" * 64,
    )
    policy_ids = (
        "prospective-collection-policy-" + "3" * 64,
        "prospective-collection-policy-" + "4" * 64,
    )
    report_ids = (
        "source-route-acceptance-report-" + "5" * 64,
        "source-route-acceptance-report-" + "6" * 64,
    )
    sources = ("tushare-news", "tushare-news-cls")
    candidates = (
        ProspectiveObservationVersionRef(
            version_id="prospective-observation-version-" + "7" * 64,
            first_available_at=ADMITTED_AT + timedelta(seconds=10),
            provider_id="tushare-observation",
            provider_version="1",
            upstream_source=sources[0],
        ),
        ProspectiveObservationVersionRef(
            version_id="prospective-observation-version-" + "8" * 64,
            first_available_at=ADMITTED_AT + timedelta(seconds=20),
            provider_id="tushare-observation",
            provider_version="1",
            upstream_source=sources[1],
        ),
    )

    class Journal:
        def policy(self, policy_id: str) -> _FakePolicy:
            index = policy_ids.index(policy_id)
            return _FakePolicy(policy_id=policy_ids[index])

        def observation_version_refs(self, *, policy_id: str, **_: object) -> tuple[Any, ...]:
            return (candidates[policy_ids.index(policy_id)],)

    class Runtime:
        journal = Journal()

        def job(self, job_id: str) -> _FakeJob:
            index = job_ids.index(job_id)
            return _FakeJob(
                collection_policy_id=policy_ids[index],
                source_acceptance_report_id=report_ids[index],
            )

        def source_acceptance_report(self, job_id: str) -> _FakeReport:
            index = job_ids.index(job_id)
            return _FakeReport(
                report_id=report_ids[index],
                declaration=_FakeDeclaration(
                    capability=ObservationCapability.EVENT_REVELATION,
                    source_config_hash=str(index + 1) * 64,
                    provider_id="tushare-observation",
                    upstream_source=sources[index],
                    semantic_scope="aggregated_source_observation_actual_receipt_only",
                ),
            )

        def health(self, job_id: str, *, now: datetime) -> _FakeHealth:
            assert job_id in job_ids and now >= ADMITTED_AT
            return _FakeHealth()

        def opportunities(self, job_id: str) -> tuple[_FakeOpportunity, ...]:
            assert job_id in job_ids
            return ()

    plan = ProspectiveCheckpointRoutePlan.build(
        registration_id=registration.registration_id,
        bindings=tuple(
            ProspectiveCheckpointRouteBinding(
                checkpoint_key="next-a-share-policy-event",
                capability=ObservationCapability.EVENT_REVELATION,
                route_kind="established_news",
                job_id=job_id,
            )
            for job_id in job_ids
        ),
    )
    runtime = cast(ProspectiveCollectionRuntime, Runtime())
    admissions = ProspectiveCheckpointAdmissionStore(
        tmp_path / "state",
        clock=lambda: ADMITTED_AT,
    )
    admissions.admit(route_plan=plan, registration=registration, runtime=runtime)
    report = evaluate_prospective_checkpoint_readiness(
        registration=registration,
        route_plan=plan,
        admission_store=admissions,
        runtime=runtime,
        evaluated_at=ADMITTED_AT + timedelta(minutes=1),
    )
    checkpoint = next(
        item for item in report.checkpoints if item.checkpoint_key == "next-a-share-policy-event"
    )

    assert checkpoint.operational_trigger_route_job_ids == job_ids
    assert checkpoint.trigger_candidate_version_ids == tuple(
        sorted(item.version_id for item in candidates)
    )


def test_route_plan_rejects_missing_or_legacy_admission_timing_protocol(
    tmp_path: Path,
) -> None:
    plan = _plan()
    old_core = plan.core_dict()
    old_core.pop("admission_timing_protocol")
    old_plan_id = f"prospective-checkpoint-route-plan-{canonical_hash(old_core)}"
    assert plan.plan_id != old_plan_id

    missing = plan.to_dict()
    missing.pop("admission_timing_protocol")
    missing["plan_id"] = old_plan_id
    path = tmp_path / "missing-protocol.json"
    path.write_text(json.dumps(missing), encoding="utf-8")
    with pytest.raises(ValueError, match="fields are invalid"):
        load_prospective_checkpoint_route_plan(path)

    legacy = plan.to_dict()
    legacy["admission_timing_protocol"] = "clock_before_sqlite_write_lock_v0"
    legacy_core = {key: value for key, value in legacy.items() if key != "plan_id"}
    legacy["plan_id"] = f"prospective-checkpoint-route-plan-{canonical_hash(legacy_core)}"
    path.write_text(json.dumps(legacy), encoding="utf-8")
    with pytest.raises(ValueError, match="admission timing protocol"):
        load_prospective_checkpoint_route_plan(path)


def test_new_protocol_admission_does_not_infer_a_head_from_legacy_rows(
    tmp_path: Path,
) -> None:
    plan = _plan()
    old_core = plan.core_dict()
    old_core.pop("admission_timing_protocol")
    old_plan_id = f"prospective-checkpoint-route-plan-{canonical_hash(old_core)}"
    old_admission_core = {
        "route_plan_id": old_plan_id,
        "registration_id": plan.registration_id,
        "recorded_at": "2026-08-29T08:21:00Z",
    }
    old_admission = ProspectiveCheckpointRouteAdmission(
        admission_id=(
            "prospective-checkpoint-route-admission-" + canonical_hash(old_admission_core)
        ),
        route_plan_id=old_plan_id,
        registration_id=plan.registration_id,
        recorded_at=ADMITTED_AT - timedelta(minutes=1),
    )
    store = ProspectiveCheckpointAdmissionStore(tmp_path / "state", clock=lambda: ADMITTED_AT)
    artifact = store.store.artifacts.put_json(old_admission.to_dict())
    with sqlite3.connect(store.index_path) as connection:
        connection.execute(
            """
            INSERT INTO prospective_checkpoint_route_admissions(
                route_plan_id, admission_id, registration_id, artifact_hash, recorded_at
            ) VALUES (?, ?, ?, ?, ?)
            """,
            (
                old_admission.route_plan_id,
                old_admission.admission_id,
                old_admission.registration_id,
                artifact.content_hash,
                "2026-08-29T08:21:00Z",
            ),
        )

    with pytest.raises(KeyError, match="not durably admitted"):
        store.admission(plan.plan_id)

    with pytest.raises(ValueError, match="explicitly re-admit one existing plan"):
        store.admit(route_plan=plan, registration=_registration(), runtime=_runtime())

    assert plan.admission_timing_protocol == PROSPECTIVE_CHECKPOINT_ADMISSION_TIMING_PROTOCOL
    assert store.admission(old_plan_id) == old_admission


def test_route_admission_uses_harness_clock_and_is_durable(tmp_path: Path) -> None:
    store = ProspectiveCheckpointAdmissionStore(
        tmp_path / "state",
        clock=lambda: ADMITTED_AT,
    )

    admission = store.admit(route_plan=_plan(), registration=_registration(), runtime=_runtime())

    assert admission.recorded_at == ADMITTED_AT
    assert store.admission(_plan().plan_id) == admission
    assert (
        store.admit(route_plan=_plan(), registration=_registration(), runtime=_runtime())
        == admission
    )


def test_route_admission_samples_harness_clock_only_after_write_lock(
    tmp_path: Path,
) -> None:
    clock_sampled = threading.Event()
    worker_started = threading.Event()
    completed = threading.Event()
    result: dict[str, object] = {}

    def clock() -> datetime:
        clock_sampled.set()
        return ADMITTED_AT

    store = ProspectiveCheckpointAdmissionStore(tmp_path / "state", clock=clock)

    def admit() -> None:
        worker_started.set()
        try:
            result["admission"] = store.admit(
                route_plan=_plan(), registration=_registration(), runtime=_runtime()
            )
        except BaseException as exc:
            result["error"] = exc
        finally:
            completed.set()

    with sqlite3.connect(store.index_path) as blocker:
        blocker.execute("BEGIN IMMEDIATE")
        worker = threading.Thread(target=admit)
        worker.start()
        assert worker_started.wait(timeout=1)
        assert clock_sampled.wait(timeout=0.2) is False
        assert completed.is_set() is False
        blocker.rollback()

    assert completed.wait(timeout=2)
    worker.join(timeout=2)
    assert "error" not in result
    assert clock_sampled.is_set()
    assert result["admission"] == store.admission(_plan().plan_id)


def test_route_admission_requires_its_cas_artifact(tmp_path: Path) -> None:
    store = ProspectiveCheckpointAdmissionStore(
        tmp_path / "state",
        clock=lambda: ADMITTED_AT,
    )
    admission = store.admit(route_plan=_plan(), registration=_registration(), runtime=_runtime())
    artifact = store.store.artifacts.put_json(admission.to_dict())
    artifact.path.unlink()

    with pytest.raises(FileNotFoundError):
        store.admission(_plan().plan_id)


@pytest.mark.parametrize("artifact_column", ("artifact_hash", "route_plan_artifact_hash"))
def test_assert_effective_requires_its_durable_admission_and_plan_artifacts(
    tmp_path: Path,
    artifact_column: str,
) -> None:
    store = _admissions(tmp_path)
    plan = _plan()
    admission = store.admission(plan.plan_id)
    with sqlite3.connect(store.index_path) as connection:
        artifact_hash = connection.execute(
            f"SELECT {artifact_column} FROM prospective_checkpoint_route_admissions "
            "WHERE route_plan_id = ?",
            (plan.plan_id,),
        ).fetchone()[0]
    store.store.artifacts.get(artifact_hash, media_type="application/json").path.unlink()

    with pytest.raises(FileNotFoundError):
        store.assert_effective(
            route_plan_id=plan.plan_id,
            admission_id=admission.admission_id,
            registration_id=plan.registration_id,
            at=ADMITTED_AT,
        )


@pytest.mark.parametrize("artifact_column", ("artifact_hash", "route_plan_artifact_hash"))
def test_assert_effective_rejects_corrupt_admission_and_plan_artifact_bindings(
    tmp_path: Path,
    artifact_column: str,
) -> None:
    store = _admissions(tmp_path)
    plan = _plan()
    admission = store.admission(plan.plan_id)
    corrupt_artifact = store.store.artifacts.put_json({"corrupt": artifact_column})
    with sqlite3.connect(store.index_path) as connection:
        connection.execute(
            f"UPDATE prospective_checkpoint_route_admissions SET {artifact_column} = ? "
            "WHERE route_plan_id = ?",
            (corrupt_artifact.content_hash, plan.plan_id),
        )

    with pytest.raises((TypeError, ValueError)):
        store.assert_effective(
            route_plan_id=plan.plan_id,
            admission_id=admission.admission_id,
            registration_id=plan.registration_id,
            at=ADMITTED_AT,
        )


@pytest.mark.parametrize(
    ("head_column", "replacement"),
    (
        ("route_plan_id", "prospective-checkpoint-route-plan-" + "0" * 64),
        ("admission_id", "prospective-checkpoint-route-admission-" + "0" * 64),
        ("route_plan_artifact_hash", "0" * 64),
        (
            "effective_from",
            (ADMITTED_AT + timedelta(seconds=1)).isoformat().replace("+00:00", "Z"),
        ),
    ),
)
def test_assert_effective_reconciles_the_current_head_to_durable_route_evidence(
    tmp_path: Path,
    head_column: str,
    replacement: str,
) -> None:
    store = _admissions(tmp_path)
    plan = _plan()
    admission = store.admission(plan.plan_id)
    with sqlite3.connect(store.index_path) as connection:
        connection.execute(
            f"UPDATE prospective_checkpoint_route_heads SET {head_column} = ? "
            "WHERE registration_id = ?",
            (replacement, plan.registration_id),
        )

    with pytest.raises(ValueError, match="head does not match"):
        store.assert_effective(
            route_plan_id=plan.plan_id,
            admission_id=admission.admission_id,
            registration_id=plan.registration_id,
            at=ADMITTED_AT,
        )


def test_route_replacement_creates_fresh_effective_interval_and_lower_bound(
    tmp_path: Path,
) -> None:
    initial = _plan()
    replacement_at = ADMITTED_AT + timedelta(minutes=10)
    clock_values = iter((ADMITTED_AT, replacement_at))
    store = ProspectiveCheckpointAdmissionStore(
        tmp_path / "state",
        clock=lambda: next(clock_values),
    )
    initial_admission = store.admit(
        route_plan=initial,
        registration=_registration(),
        runtime=_runtime(),
    )
    replacement = ProspectiveCheckpointRoutePlan.build(
        registration_id=initial.registration_id,
        bindings=initial.bindings,
        replaces_plan_id=initial.plan_id,
    )

    replacement_admission = store.admit(
        route_plan=replacement,
        registration=_registration(),
        runtime=_runtime(),
    )

    assert replacement.plan_id != initial.plan_id
    assert replacement_admission.recorded_at == replacement_at
    assert store.current_plan_id(initial.registration_id) == replacement.plan_id
    store.assert_effective(
        route_plan_id=initial.plan_id,
        admission_id=initial_admission.admission_id,
        registration_id=initial.registration_id,
        at=replacement_at - timedelta(microseconds=1),
    )
    with pytest.raises(ValueError, match="not effective"):
        store.assert_effective(
            route_plan_id=initial.plan_id,
            admission_id=initial_admission.admission_id,
            registration_id=initial.registration_id,
            at=replacement_at,
        )
    store.assert_effective(
        route_plan_id=replacement.plan_id,
        admission_id=replacement_admission.admission_id,
        registration_id=replacement.registration_id,
        at=replacement_at,
    )

    old_miss = _FakeOpportunity(
        scheduled_for=ADMITTED_AT + timedelta(minutes=5),
        outcome="missed",
    )
    report = evaluate_prospective_checkpoint_readiness(
        registration=_registration(),
        route_plan=replacement,
        admission_store=store,
        runtime=_runtime(opportunities=(old_miss,)),
        evaluated_at=replacement_at + timedelta(minutes=1),
    )
    checkpoint = next(
        item for item in report.checkpoints if item.checkpoint_key == "next-a-share-policy-event"
    )
    assert checkpoint.status is CheckpointReadinessStatus.WAITING_FOR_POST_ADMISSION_TRIGGER
    assert "event_revelation:post_admission_missed_opportunity" not in checkpoint.blocking_gaps
    with pytest.raises(ValueError, match="not effective"):
        evaluate_prospective_checkpoint_readiness(
            registration=_registration(),
            route_plan=initial,
            admission_store=store,
            runtime=_runtime(),
            evaluated_at=replacement_at + timedelta(minutes=1),
        )


def test_historical_route_interval_requires_its_authenticated_successor(
    tmp_path: Path,
) -> None:
    initial = _plan()
    replacement_at = ADMITTED_AT + timedelta(minutes=10)
    clock_values = iter((ADMITTED_AT, replacement_at))
    store = ProspectiveCheckpointAdmissionStore(
        tmp_path / "state",
        clock=lambda: next(clock_values),
    )
    initial_admission = store.admit(
        route_plan=initial,
        registration=_registration(),
        runtime=_runtime(),
    )
    replacement = ProspectiveCheckpointRoutePlan.build(
        registration_id=initial.registration_id,
        bindings=initial.bindings,
        replaces_plan_id=initial.plan_id,
    )
    store.admit(
        route_plan=replacement,
        registration=_registration(),
        runtime=_runtime(),
    )
    with sqlite3.connect(store.index_path) as connection:
        connection.execute(
            """
            UPDATE prospective_checkpoint_route_admissions
            SET superseded_at = ?
            WHERE route_plan_id = ?
            """,
            (
                (replacement_at + timedelta(seconds=1)).isoformat().replace("+00:00", "Z"),
                initial.plan_id,
            ),
        )

    with pytest.raises(ValueError, match="authenticated successor"):
        store.assert_effective(
            route_plan_id=initial.plan_id,
            admission_id=initial_admission.admission_id,
            registration_id=initial.registration_id,
            at=ADMITTED_AT + timedelta(minutes=1),
        )


def test_concurrent_route_replacements_use_current_head_compare_and_swap(
    tmp_path: Path,
) -> None:
    clock_lock = threading.Lock()
    clock_tick = 0

    def clock() -> datetime:
        nonlocal clock_tick
        with clock_lock:
            value = ADMITTED_AT + timedelta(seconds=clock_tick)
            clock_tick += 1
            return value

    store = ProspectiveCheckpointAdmissionStore(tmp_path / "state", clock=clock)
    initial = _plan()
    store.admit(route_plan=initial, registration=_registration(), runtime=_runtime())
    replacements = (
        ProspectiveCheckpointRoutePlan.build(
            registration_id=initial.registration_id,
            bindings=initial.bindings,
            replaces_plan_id=initial.plan_id,
        ),
        ProspectiveCheckpointRoutePlan.build(
            registration_id=initial.registration_id,
            bindings=(),
            replaces_plan_id=initial.plan_id,
        ),
    )
    rendezvous = threading.Barrier(2)
    results: list[ProspectiveCheckpointRouteAdmission] = []
    errors: list[BaseException] = []

    def replace(plan: ProspectiveCheckpointRoutePlan) -> None:
        rendezvous.wait(timeout=2)
        try:
            results.append(
                store.admit(
                    route_plan=plan,
                    registration=_registration(),
                    runtime=_runtime(),
                )
            )
        except BaseException as exc:
            errors.append(exc)

    workers = tuple(threading.Thread(target=replace, args=(plan,)) for plan in replacements)
    for worker in workers:
        worker.start()
    for worker in workers:
        worker.join(timeout=3)

    assert len(results) == 1
    assert len(errors) == 1
    assert isinstance(errors[0], ValueError)
    assert "predecessor is not current" in str(errors[0])
    assert store.current_plan_id(initial.registration_id) == results[0].route_plan_id
    with sqlite3.connect(store.index_path) as connection:
        admission_count = connection.execute(
            "SELECT COUNT(*) FROM prospective_checkpoint_route_admissions"
        ).fetchone()[0]
    assert admission_count == 2


def test_route_admission_cli_exposes_no_caller_timestamp(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    args = build_parser().parse_args(
        [
            "data",
            "checkpoint-route-admit",
            "--registration",
            REGISTRATION_PATH.as_posix(),
            "--route-plan",
            (ROOT / "examples/research/prospective-checkpoint-route-plan-v1.json").as_posix(),
            "--state-root",
            (tmp_path / "state").as_posix(),
        ]
    )
    assert args.data_command == "checkpoint-route-admit"
    assert not hasattr(args, "admitted_at")
    assert (
        main(
            [
                "data",
                "checkpoint-route-admit",
                "--registration",
                REGISTRATION_PATH.as_posix(),
                "--route-plan",
                (ROOT / "examples/research/prospective-checkpoint-route-plan-v1.json").as_posix(),
                "--state-root",
                (tmp_path / "state").as_posix(),
            ]
        )
        == 1
    )
    error = json.loads(capsys.readouterr().err)
    assert error["admitted"] is False
    assert "unknown prospective collection job" in error["error"]


def test_readiness_distinguishes_external_wait_from_structural_route_gaps(
    tmp_path: Path,
) -> None:
    registration = _registration()
    report = evaluate_prospective_checkpoint_readiness(
        registration=registration,
        route_plan=_plan(),
        admission_store=_admissions(tmp_path),
        runtime=_runtime(),
        evaluated_at=ADMITTED_AT + timedelta(minutes=1),
    )

    by_key = {item.checkpoint_key: item for item in report.checkpoints}
    assert (
        by_key["next-a-share-policy-event"].status
        is CheckpointReadinessStatus.WAITING_FOR_POST_ADMISSION_TRIGGER
    )
    assert by_key["next-a-share-policy-event"].blocking_gaps == (
        "event_revelation:no_post_admission_observation",
    )
    assert (
        by_key["next-a-share-earnings-surprise"].status
        is CheckpointReadinessStatus.TRIGGER_ROUTE_UNCONFIGURED
    )
    assert report.operational_checkpoint_count == 1
    assert report.candidate_checkpoint_count == 0
    assert report.waiting_for_external_event is True
    assert report.model_calls_authorized is False
    assert report.execution_capability is False
    assert (
        validate_agent_contract(
            report.to_dict(), "prospective-checkpoint-readiness-report.schema.json"
        )
        == ()
    )


def test_post_admission_candidate_still_requires_explicit_eligibility_selection(
    tmp_path: Path,
) -> None:
    candidate = ProspectiveObservationVersionRef(
        version_id="prospective-observation-version-" + "e" * 64,
        first_available_at=ADMITTED_AT + timedelta(seconds=30),
        provider_id="official-source",
        provider_version="1",
        upstream_source="official-events",
    )
    report = evaluate_prospective_checkpoint_readiness(
        registration=_registration(),
        route_plan=_plan(),
        admission_store=_admissions(tmp_path),
        runtime=_runtime((candidate,)),
        evaluated_at=ADMITTED_AT + timedelta(minutes=1),
    )

    checkpoint = next(
        item for item in report.checkpoints if item.checkpoint_key == "next-a-share-policy-event"
    )
    assert checkpoint.status is CheckpointReadinessStatus.UNCLASSIFIED_TRIGGER_CANDIDATE_OBSERVED
    assert checkpoint.trigger_candidate_version_ids == (candidate.version_id,)
    assert checkpoint.blocking_gaps == (
        "event_revelation:trigger_candidate_requires_eligibility_selection",
    )
    assert report.candidate_checkpoint_count == 1
    assert report.model_calls_authorized is False


def test_readiness_excludes_versions_reopened_from_formal_triage_decisions(
    tmp_path: Path,
) -> None:
    candidate = ProspectiveObservationVersionRef(
        version_id="prospective-observation-version-" + "e" * 64,
        first_available_at=ADMITTED_AT + timedelta(seconds=30),
        provider_id="official-source",
        provider_version="1",
        upstream_source="official-events",
    )

    class ClassificationAuthority:
        def classified_version_ids(self, **kwargs: object) -> tuple[str, ...]:
            assert kwargs["registration_id"] == _registration().registration_id
            assert kwargs["checkpoint_key"] in {
                "next-a-share-earnings-surprise",
                "next-a-share-policy-event",
                "next-nbs-cpi-ppi-release",
            }
            return (
                (candidate.version_id,)
                if kwargs["checkpoint_key"] == "next-a-share-policy-event"
                else ()
            )

    report = evaluate_prospective_checkpoint_readiness(
        registration=_registration(),
        route_plan=_plan(),
        admission_store=_admissions(tmp_path),
        runtime=_runtime((candidate,)),
        evaluated_at=ADMITTED_AT + timedelta(minutes=1),
        classification_authority=ClassificationAuthority(),
    )

    checkpoint = next(
        item for item in report.checkpoints if item.checkpoint_key == "next-a-share-policy-event"
    )
    assert checkpoint.status is CheckpointReadinessStatus.WAITING_FOR_POST_ADMISSION_TRIGGER
    assert checkpoint.trigger_candidate_version_ids == ()
    assert report.candidate_checkpoint_count == 0


def test_route_plan_rejects_unregistered_post_hoc_route_kind(tmp_path: Path) -> None:
    plan = ProspectiveCheckpointRoutePlan.build(
        registration_id=_registration().registration_id,
        bindings=(
            ProspectiveCheckpointRouteBinding(
                checkpoint_key="next-a-share-policy-event",
                capability=ObservationCapability.EVENT_REVELATION,
                route_kind="post_hoc_route",
                job_id=JOB_ID,
            ),
        ),
    )
    admissions = ProspectiveCheckpointAdmissionStore(tmp_path / "state", clock=lambda: ADMITTED_AT)
    with pytest.raises(ValueError, match="unregistered route kind"):
        admissions.admit(route_plan=plan, registration=_registration(), runtime=_runtime())


@pytest.mark.parametrize(
    ("checkpoint_key", "route_kind"),
    (
        ("next-a-share-earnings-surprise", "issuer_event"),
        ("next-nbs-cpi-ppi-release", "official_macro_release"),
    ),
)
def test_csrc_route_cannot_be_relabeled_as_another_event_semantic(
    tmp_path: Path,
    checkpoint_key: str,
    route_kind: str,
) -> None:
    plan = ProspectiveCheckpointRoutePlan.build(
        registration_id=_registration().registration_id,
        bindings=(
            ProspectiveCheckpointRouteBinding(
                checkpoint_key=checkpoint_key,
                capability=ObservationCapability.EVENT_REVELATION,
                route_kind=route_kind,
                job_id=JOB_ID,
            ),
        ),
    )
    admissions = ProspectiveCheckpointAdmissionStore(tmp_path / "state", clock=lambda: ADMITTED_AT)
    with pytest.raises(ValueError, match="accepted source semantics"):
        admissions.admit(
            route_plan=plan,
            registration=_registration(),
            runtime=_runtime(),
        )


def test_forecast_source_cannot_masquerade_as_established_news(tmp_path: Path) -> None:
    registration = _registration()
    plan = ProspectiveCheckpointRoutePlan.build(
        registration_id=registration.registration_id,
        bindings=(
            ProspectiveCheckpointRouteBinding(
                checkpoint_key="next-a-share-policy-event",
                capability=ObservationCapability.EVENT_REVELATION,
                route_kind="established_news",
                job_id=JOB_ID,
            ),
        ),
    )
    admissions = ProspectiveCheckpointAdmissionStore(tmp_path / "state", clock=lambda: ADMITTED_AT)
    with pytest.raises(ValueError, match="accepted source semantics"):
        admissions.admit(
            route_plan=plan,
            registration=registration,
            runtime=_runtime(
                declaration=_FakeDeclaration(
                    provider_id="tushare-observation",
                    upstream_source="tushare-forecast-vip",
                    semantic_scope="aggregated_source_observation_actual_receipt_only",
                )
            ),
        )


def test_readiness_ignores_pre_admission_misses_but_blocks_current_health(
    tmp_path: Path,
) -> None:
    pre_admission_miss = _FakeOpportunity(
        scheduled_for=ADMITTED_AT - timedelta(minutes=5), outcome="missed"
    )
    report = evaluate_prospective_checkpoint_readiness(
        registration=_registration(),
        route_plan=_plan(),
        admission_store=_admissions(tmp_path),
        runtime=_runtime(opportunities=(pre_admission_miss,)),
        evaluated_at=ADMITTED_AT + timedelta(minutes=1),
    )
    checkpoint = next(
        item for item in report.checkpoints if item.checkpoint_key == "next-a-share-policy-event"
    )
    assert checkpoint.status is CheckpointReadinessStatus.WAITING_FOR_POST_ADMISSION_TRIGGER

    unhealthy = evaluate_prospective_checkpoint_readiness(
        registration=_registration(),
        route_plan=_plan(),
        admission_store=_admissions(tmp_path),
        runtime=_runtime(
            health=_FakeHealth(
                next_due_at=ADMITTED_AT,
                backoff_until=ADMITTED_AT + timedelta(minutes=2),
                last_outcome="collector_failure",
                lag_seconds=60,
            )
        ),
        evaluated_at=ADMITTED_AT + timedelta(minutes=1),
    )
    unhealthy_checkpoint = next(
        item for item in unhealthy.checkpoints if item.checkpoint_key == "next-a-share-policy-event"
    )
    assert unhealthy_checkpoint.status is CheckpointReadinessStatus.TRIGGER_ROUTE_UNCONFIGURED
    assert "event_revelation:route_in_backoff" in unhealthy_checkpoint.blocking_gaps
    assert unhealthy.waiting_for_external_event is False


def test_historical_readiness_does_not_use_future_runtime_failure(
    tmp_path: Path,
) -> None:
    evaluated_at = ADMITTED_AT + timedelta(minutes=1)
    future_failure = _FakeOpportunity(
        scheduled_for=ADMITTED_AT + timedelta(minutes=2),
        outcome="collector_failure",
        started_at=ADMITTED_AT + timedelta(minutes=2),
        completed_at=ADMITTED_AT + timedelta(minutes=2, seconds=5),
    )
    report = evaluate_prospective_checkpoint_readiness(
        registration=_registration(),
        route_plan=_plan(),
        admission_store=_admissions(tmp_path),
        runtime=_runtime(opportunities=(future_failure,)),
        evaluated_at=evaluated_at,
    )
    checkpoint = next(
        item for item in report.checkpoints if item.checkpoint_key == "next-a-share-policy-event"
    )
    assert checkpoint.status is CheckpointReadinessStatus.WAITING_FOR_POST_ADMISSION_TRIGGER
    assert "event_revelation:current_collector_failure" not in checkpoint.blocking_gaps

    with pytest.raises(ValueError, match="cannot reconstruct historical runtime health"):
        evaluate_prospective_checkpoint_readiness(
            registration=_registration(),
            route_plan=_plan(),
            admission_store=_admissions(tmp_path),
            runtime=_runtime(
                health=_FakeHealth(
                    last_outcome="collector_failure",
                    state_updated_at=ADMITTED_AT + timedelta(minutes=2),
                ),
                opportunities=(future_failure,),
            ),
            evaluated_at=evaluated_at,
        )


@pytest.mark.parametrize(
    ("runtime_kwargs", "expected_gap"),
    (
        (
            {
                "opportunities": (
                    _FakeOpportunity(
                        scheduled_for=ADMITTED_AT + timedelta(seconds=30),
                        outcome="collector_failure",
                    ),
                )
            },
            "event_revelation:current_collector_failure",
        ),
        (
            {"policy": _FakePolicy(poll_interval_seconds=600)},
            "event_revelation:poll_interval_exceeds_registration",
        ),
        (
            {"policy": _FakePolicy(maximum_gap_seconds=1800)},
            "event_revelation:maximum_gap_exceeds_registration",
        ),
    ),
)
def test_post_admission_health_and_registered_cadence_are_fail_closed(
    tmp_path: Path,
    runtime_kwargs: dict[str, Any],
    expected_gap: str,
) -> None:
    report = evaluate_prospective_checkpoint_readiness(
        registration=_registration(),
        route_plan=_plan(),
        admission_store=_admissions(tmp_path),
        runtime=_runtime(**runtime_kwargs),
        evaluated_at=ADMITTED_AT + timedelta(minutes=1),
    )
    checkpoint = next(
        item for item in report.checkpoints if item.checkpoint_key == "next-a-share-policy-event"
    )
    assert checkpoint.status is CheckpointReadinessStatus.TRIGGER_ROUTE_UNCONFIGURED
    assert expected_gap in checkpoint.blocking_gaps
    assert report.waiting_for_external_event is False


def _receipt_runtime(tmp_path: Path, *, poll: int = 300, gap: int = 900, required: bool = True):
    store = LocalDataSnapshotStore(tmp_path / "receipt-state")
    runtime = ProspectiveCollectionRuntime(store, clock=lambda: ADMITTED_AT)
    source_config = {"source_config_id": "fixture-official-news"}
    source = DataSourceBinding(
        provider_id="csrc-official-news",
        provider_version="1",
        upstream_source="csrc-official-news",
        manifest_hash="e" * 64,
        source_config_hash=canonical_hash(source_config),
        required=required,
    )
    policy = ProspectiveCollectionPolicy.build(
        capability=ObservationCapability.EVENT_REVELATION,
        sources=(source,),
        window_start=ADMITTED_AT - timedelta(hours=1),
        parameters={},
        poll_interval_seconds=poll,
        maximum_gap_seconds=gap,
    )
    declaration = SourceRouteAcceptanceDeclaration.build(
        provider_id=source.provider_id,
        provider_version=source.provider_version,
        provider_manifest_hash=source.manifest_hash,
        source_config_hash=source.source_config_hash or "",
        upstream_source=source.upstream_source,
        capability=policy.capability,
        rights_basis_url="https://fixture.invalid/terms",
        rights_reviewed_at=ADMITTED_AT,
        permitted_use="private_research",
        retention_scope="private_raw_and_normalized",
        redistribution_allowed=False,
        semantic_scope="official_capital_market_policy_publication",
        revision_strategy="append_only_content_versions",
    )
    gates = tuple(
        SourceAcceptanceGateResult(
            gate=gate.value,
            status=SourceAcceptanceStatus.PASS.value,
            reasons=(),
        )
        for gate in SourceAcceptanceGate
    )
    report_core = {
        "schema_version": "market-impact.source-route-acceptance-report.v1",
        "declaration": declaration.to_dict(),
        "rights_evidence": None,
        "data_snapshot_id": "data-snapshot-fixture",
        "deterministic_replay_snapshot_id": None,
        "evaluated_at": ADMITTED_AT.isoformat().replace("+00:00", "Z"),
        "gates": [gate.to_dict() for gate in gates],
        "accepted": True,
        "historical_pit_claim": False,
        "evidence_promoted": False,
        "execution_capability": False,
    }
    acceptance = SourceRouteAcceptanceReport(
        report_id=f"source-route-acceptance-report-{canonical_hash(report_core)}",
        declaration=declaration,
        rights_evidence=None,
        data_snapshot_id="data-snapshot-fixture",
        deterministic_replay_snapshot_id=None,
        evaluated_at=ADMITTED_AT,
        gates=gates,
        accepted=True,
    )
    job = ProspectiveCollectionJob.build(
        adapter_kind=ProspectiveCollectionAdapterKind.CSRC_NEWS,
        collection_policy=policy,
        source_acceptance_report=acceptance,
        source_config=source_config,
        starts_at=ADMITTED_AT,
        misfire_grace_seconds=180,
        maximum_jitter_seconds=0,
        provider_timeout_seconds=30.0,
    )
    runtime.register(
        job,
        collection_policy=policy,
        source_acceptance_report=acceptance,
        source_config=source_config,
        registered_at=ADMITTED_AT,
    )
    plan = ProspectiveCheckpointRoutePlan.build(
        registration_id=_registration().registration_id,
        bindings=(
            ProspectiveCheckpointRouteBinding(
                checkpoint_key="next-a-share-policy-event",
                capability=policy.capability,
                route_kind="official_event",
                job_id=job.job_id,
            ),
        ),
    )
    admissions = ProspectiveCheckpointAdmissionStore(
        tmp_path / "admission", clock=lambda: ADMITTED_AT
    )
    admissions.admit(route_plan=plan, registration=_registration(), runtime=runtime)
    return runtime, policy, job, plan, admissions


def _receipt_snapshot(
    runtime: ProspectiveCollectionRuntime,
    policy: ProspectiveCollectionPolicy,
    seconds: int,
    *,
    failed: bool = False,
) -> DataSnapshot:
    received = ADMITTED_AT + timedelta(seconds=seconds)
    query = DataQuery.build(
        capability=policy.capability,
        pit_lane=DataPITLane.PROSPECTIVE,
        as_of=received,
        window_start=policy.window_start,
        source_policy_id=policy.policy_id,
        parameters=policy.parameters,
        sources=policy.sources,
        minimum_data_sources=1,
    )
    attempts = tuple(
        DataProviderAttempt(
            provider_id=source.provider_id,
            provider_version=source.provider_version,
            upstream_source=source.upstream_source,
            required=source.required,
            status=DataFetchStatus.ERROR if failed else DataFetchStatus.NO_DATA,
            retrieved_at=received,
            raw_response_hash=None if failed else runtime.store.put_raw(b"[]"),
            received_count=0,
            accepted_count=0,
            rejected_missing_availability=0,
            rejected_after_cutoff=0,
            rejected_missing_authority=0,
            rejected_authority_after_cutoff=0,
            rejected_lane_mismatch=0,
            error_kind="fixture_failure" if failed else None,
        )
        for source in policy.sources
    )
    core = {
        "schema_version": "market-impact.data-snapshot.v2",
        "query": query.to_dict(),
        "attempts": [attempt.to_dict() for attempt in attempts],
        "observations": [],
        "coverage_complete": False,
        "completed_at": received.isoformat().replace("+00:00", "Z"),
    }
    snapshot = DataSnapshot(
        snapshot_id=f"data-snapshot-{canonical_hash(core)}",
        query=query,
        attempts=attempts,
        observations=(),
        coverage_complete=False,
        completed_at=received,
    )
    runtime.store.put(snapshot)
    return snapshot


def _evaluate_receipt_readiness(
    runtime: ProspectiveCollectionRuntime,
    plan: ProspectiveCheckpointRoutePlan,
    admissions: ProspectiveCheckpointAdmissionStore,
    seconds: int,
):
    report = evaluate_prospective_checkpoint_readiness(
        registration=_registration(),
        route_plan=plan,
        admission_store=admissions,
        runtime=runtime,
        evaluated_at=ADMITTED_AT + timedelta(seconds=seconds),
    )
    return next(
        item for item in report.checkpoints if item.checkpoint_key == "next-a-share-policy-event"
    )


@pytest.mark.parametrize("required", [True, False])
def test_scheduler_miss_with_full_receipt_coverage_is_information_only(
    tmp_path: Path, required: bool
) -> None:
    runtime, policy, job, plan, admissions = _receipt_runtime(tmp_path, required=required)
    for seconds in (0, 300, 792, 900, 1200):
        result = runtime.run_due(
            job.job_id,
            now=ADMITTED_AT + timedelta(seconds=seconds),
            collector=lambda bound_policy, _config, _scheduled, seconds=seconds: _receipt_snapshot(
                runtime,
                bound_policy,
                926 if seconds == 900 else seconds,
            ),
        )
        if seconds == 792:
            assert result.missed_opportunities == 1
    misses = [item for item in runtime.opportunities(job.job_id) if item.outcome == "missed"]
    assert len(misses) == 1
    assert misses[0].completed_at is not None
    assert (misses[0].completed_at - misses[0].scheduled_for).total_seconds() == 192
    assert job.misfire_grace_seconds == 180
    assert 926 - 300 == 626 < policy.maximum_gap_seconds
    assert (
        runtime.journal.receipt_coverage_errors(
            policy_id=policy.policy_id,
            window_start=ADMITTED_AT,
            not_after=ADMITTED_AT + timedelta(seconds=1200),
        )
        == ()
    )
    with sqlite3.connect(runtime.index_path) as connection:
        before = tuple(connection.iterdump())
    artifacts_before = {
        path.name: path.read_bytes() for path in runtime.store.artifacts.root.iterdir()
    }
    checkpoint = _evaluate_receipt_readiness(runtime, plan, admissions, 1200)
    assert checkpoint.operational_trigger_route_job_ids == (job.job_id,)
    assert checkpoint.status is CheckpointReadinessStatus.WAITING_FOR_POST_ADMISSION_TRIGGER
    assert "event_revelation:post_admission_missed_opportunity" in checkpoint.information_gaps
    assert "event_revelation:post_admission_missed_opportunity" not in checkpoint.blocking_gaps
    assert not any("coverage_pending" in gap for gap in checkpoint.information_gaps)
    with sqlite3.connect(runtime.index_path) as connection:
        assert tuple(connection.iterdump()) == before
    assert {
        path.name: path.read_bytes() for path in runtime.store.artifacts.root.iterdir()
    } == artifacts_before


@pytest.mark.parametrize(
    ("receipts", "cutoff", "failure", "error"),
    [
        ((), 901, None, "journal_no_receipt_before_cutoff"),
        ((901, 1200), 1200, None, "journal_start_coverage_gap"),
        ((0, 1200, 1500, 1800), 1800, None, "journal_internal_coverage_gap"),
        ((0, 300), 1201, None, "journal_cutoff_coverage_gap"),
        ((0, 300, 600), 600, 300, "journal_failed_source_receipt"),
    ],
)
@pytest.mark.parametrize("required", [True, False])
def test_missed_route_uses_entire_admission_window_and_fails_closed_on_receipt_gap(
    tmp_path: Path,
    receipts: tuple[int, ...],
    cutoff: int,
    failure: int | None,
    error: str,
    required: bool,
) -> None:
    runtime, policy, job, plan, admissions = _receipt_runtime(tmp_path, required=required)
    runtime.run_due(
        job.job_id,
        now=ADMITTED_AT + timedelta(seconds=192),
        collector=lambda _policy, _config, _scheduled: pytest.fail(
            "a missed opportunity cannot collect"
        ),
    )
    for seconds in receipts:
        runtime.journal.record_snapshot(
            _receipt_snapshot(runtime, policy, seconds, failed=seconds == failure),
            policy=policy,
        )
    if error == "journal_internal_coverage_gap":
        assert (
            runtime.journal.receipt_coverage_errors(
                policy_id=policy.policy_id,
                window_start=ADMITTED_AT + timedelta(seconds=1200),
                not_after=ADMITTED_AT + timedelta(seconds=cutoff),
            )
            == ()
        )
    checkpoint = _evaluate_receipt_readiness(runtime, plan, admissions, cutoff)
    assert checkpoint.operational_trigger_route_job_ids == ()
    assert f"event_revelation:post_admission_receipt_coverage:{error}" in checkpoint.blocking_gaps
    assert "event_revelation:post_admission_missed_opportunity" in checkpoint.information_gaps


@pytest.mark.parametrize("seconds", [192, 900])
@pytest.mark.parametrize("required", [True, False])
def test_missed_initial_receipt_within_allowed_gap_is_pending_not_proven(
    tmp_path: Path,
    seconds: int,
    required: bool,
) -> None:
    runtime, _, job, plan, admissions = _receipt_runtime(tmp_path, required=required)
    runtime.run_due(
        job.job_id,
        now=ADMITTED_AT + timedelta(seconds=192),
        collector=lambda _policy, _config, _scheduled: pytest.fail(
            "a missed opportunity cannot collect"
        ),
    )
    checkpoint = _evaluate_receipt_readiness(runtime, plan, admissions, seconds)
    assert checkpoint.operational_trigger_route_job_ids == (job.job_id,)
    assert "event_revelation:post_admission_receipt_coverage_pending" in checkpoint.information_gaps
    assert checkpoint.status is CheckpointReadinessStatus.WAITING_FOR_POST_ADMISSION_TRIGGER


def test_wrong_policy_receipts_cannot_cover_a_missed_route(tmp_path: Path) -> None:
    runtime, policy, job, plan, admissions = _receipt_runtime(tmp_path)
    wrong = ProspectiveCollectionPolicy.build(
        capability=policy.capability,
        sources=policy.sources,
        window_start=policy.window_start,
        parameters={"different_scope": True},
        poll_interval_seconds=300,
        maximum_gap_seconds=900,
    )
    runtime.run_due(
        job.job_id,
        now=ADMITTED_AT + timedelta(seconds=192),
        collector=lambda _policy, _config, _scheduled: pytest.fail(
            "a missed opportunity cannot collect"
        ),
    )
    for seconds in (0, 300, 600, 900, 1200):
        runtime.journal.record_snapshot(_receipt_snapshot(runtime, wrong, seconds), policy=wrong)
    checkpoint = _evaluate_receipt_readiness(runtime, plan, admissions, 1200)
    assert (
        "event_revelation:post_admission_receipt_coverage:journal_no_receipt_before_cutoff"
        in checkpoint.blocking_gaps
    )


def test_slow_policy_stays_blocked_even_when_a_miss_has_complete_receipts(tmp_path: Path) -> None:
    runtime, policy, job, plan, admissions = _receipt_runtime(tmp_path, poll=900, gap=2700)
    runtime.run_due(
        job.job_id,
        now=ADMITTED_AT + timedelta(seconds=192),
        collector=lambda _policy, _config, _scheduled: pytest.fail(
            "a missed opportunity cannot collect"
        ),
    )
    runtime.journal.record_snapshot(_receipt_snapshot(runtime, policy, 300), policy=policy)
    checkpoint = _evaluate_receipt_readiness(runtime, plan, admissions, 300)
    assert checkpoint.operational_trigger_route_job_ids == ()
    assert "event_revelation:poll_interval_exceeds_registration" in checkpoint.blocking_gaps
    assert "event_revelation:maximum_gap_exceeds_registration" in checkpoint.blocking_gaps
