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
from market_impact_agent.prospective_collection_runtime import ProspectiveCollectionRuntime
from market_impact_agent.prospective_data import ProspectiveObservationVersionRef
from market_impact_agent.prospective_diagnostic import (
    ProspectiveDiagnosticRegistration,
    load_prospective_diagnostic_registration,
)

ROOT = Path(__file__).parents[1]
REGISTRATION_PATH = ROOT / "examples/research/prospective-diagnostic-registration-v2.json"
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
    store.admit(route_plan=_plan(), registration=_registration())
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


def test_new_protocol_admission_does_not_grandfather_or_overwrite_old_row(
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

    new_admission = store.admit(route_plan=plan, registration=_registration())

    assert plan.admission_timing_protocol == PROSPECTIVE_CHECKPOINT_ADMISSION_TIMING_PROTOCOL
    assert new_admission.route_plan_id == plan.plan_id
    assert new_admission.route_plan_id != old_admission.route_plan_id
    assert store.admission(old_plan_id) == old_admission


def test_route_admission_uses_harness_clock_and_is_durable(tmp_path: Path) -> None:
    store = ProspectiveCheckpointAdmissionStore(
        tmp_path / "state",
        clock=lambda: ADMITTED_AT,
    )

    admission = store.admit(route_plan=_plan(), registration=_registration())

    assert admission.recorded_at == ADMITTED_AT
    assert store.admission(_plan().plan_id) == admission
    assert store.admit(route_plan=_plan(), registration=_registration()) == admission


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
            result["admission"] = store.admit(route_plan=_plan(), registration=_registration())
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
    admission = store.admit(route_plan=_plan(), registration=_registration())
    artifact = store.store.artifacts.put_json(admission.to_dict())
    artifact.path.unlink()

    with pytest.raises(FileNotFoundError):
        store.admission(_plan().plan_id)


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
        == 0
    )
    output = json.loads(capsys.readouterr().out)
    assert output["admitted"] is True
    assert output["route_plan_id"].startswith("prospective-checkpoint-route-plan-")


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
    admissions.admit(route_plan=plan, registration=_registration())
    with pytest.raises(ValueError, match="unregistered route kind"):
        evaluate_prospective_checkpoint_readiness(
            registration=_registration(),
            route_plan=plan,
            admission_store=admissions,
            runtime=_runtime(),
            evaluated_at=ADMITTED_AT + timedelta(minutes=1),
        )


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
    admissions.admit(route_plan=plan, registration=_registration())

    with pytest.raises(ValueError, match="accepted source semantics"):
        evaluate_prospective_checkpoint_readiness(
            registration=_registration(),
            route_plan=plan,
            admission_store=admissions,
            runtime=_runtime(),
            evaluated_at=ADMITTED_AT + timedelta(minutes=1),
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
                        outcome="missed",
                    ),
                )
            },
            "event_revelation:post_admission_missed_opportunity",
        ),
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
