# pyright: reportPrivateUsage=false

import asyncio
import sqlite3
from dataclasses import replace
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest

import market_impact_agent.prospective_triage as prospective_triage
from market_impact_agent.agent_contracts import canonical_hash, canonical_json_bytes
from market_impact_agent.agent_schema import validate_agent_contract
from market_impact_agent.event_impact_triage import (
    CheckpointEligibility,
    EventImpactTriageCandidateSet,
    EventImpactTriageProposal,
    TriageRoute,
)
from market_impact_agent.event_impact_triage_evaluation import (
    EventImpactTriageLabelSet,
    TriageGoldLabel,
    TriageLabelExposure,
)
from market_impact_agent.event_impact_triage_runtime import TriageComparisonArm
from market_impact_agent.event_impact_triage_store import EventImpactTriageDecisionStore
from market_impact_agent.event_impact_triage_work import (
    EventImpactTriageWorkManifest,
    TriageCandidateDigest,
    TriageClusterPartition,
)
from market_impact_agent.event_impact_triage_work_evaluation import (
    EventImpactTriageWorkComparisonRegistration,
    EventImpactTriageWorkComparisonReport,
    EventImpactTriageWorkComparisonStore,
    TriageWorkArmOutcome,
    assert_authoritative_event_impact_triage_work_comparison_report,
    evaluate_event_impact_triage_work_comparison,
)
from market_impact_agent.event_impact_triage_work_runtime import (
    EVENT_IMPACT_TRIAGE_WORK_EXECUTION_PLAN_SCHEMA_V11,
    EventImpactTriageWorkExecutionPlan,
    EventImpactTriageWorkRunEvidence,
    EventImpactTriageWorkRunner,
    EventImpactTriageWorkRunResult,
)
from market_impact_agent.prospective_triage import (
    PreparedProspectiveTriageWork,
    ProspectiveTriageActiveBatchStore,
    run_prepared_prospective_triage_comparison,
    run_prepared_prospective_triage_work,
)
from market_impact_agent.runtime_store import RunStatus
from tests.test_event_impact_triage_work_runtime import (
    NOW,
    CrashAfterDispatchProvider,
    ScriptedWorkProvider,
    SimulatedProcessCrash,
    _material_registration,
    _runtime,
)

RuntimeFixture = tuple[
    EventImpactTriageWorkRunner,
    ScriptedWorkProvider,
    EventImpactTriageCandidateSet,
    EventImpactTriageWorkManifest,
    EventImpactTriageWorkExecutionPlan,
]


class AppendUsageAfterAssertionRunner(EventImpactTriageWorkRunner):
    inject_extra_usage = False

    def assert_authoritative_completed_work_run(
        self,
        *,
        candidate_set: EventImpactTriageCandidateSet,
        work_manifest: EventImpactTriageWorkManifest,
        digests: tuple[TriageCandidateDigest, ...],
        partition: TriageClusterPartition,
        proposal: EventImpactTriageProposal,
        run_evidence: EventImpactTriageWorkRunEvidence,
    ) -> None:
        super().assert_authoritative_completed_work_run(
            candidate_set=candidate_set,
            work_manifest=work_manifest,
            digests=digests,
            partition=partition,
            proposal=proposal,
            run_evidence=run_evidence,
        )
        if self.inject_extra_usage:
            self.inject_extra_usage = False
            stored = self.usage_ledger.records()[0].record
            self.usage_ledger.append(replace(stored, run_id="triage-work-race-extra-usage"))


class AvailabilityOnlyProvider:
    def __init__(self) -> None:
        self.availability_calls = 0

    @property
    def provider_id(self) -> str:
        return "cliproxyapi-openai-compatible"

    @property
    def model(self) -> str:
        return "gpt-5.6-luna"

    async def assert_model_available(self, *, timeout_seconds: float) -> None:
        _ = timeout_seconds
        self.availability_calls += 1
        raise AssertionError("workflow recovery must not probe Provider availability")

    async def complete(self, **_kwargs: object) -> object:
        raise AssertionError("workflow recovery must use the durable completed Runs")


class ForgedReplayRunner(EventImpactTriageWorkRunner):
    authority_calls = 0

    def authoritative_completed_work_run_receipt(self, **_kwargs: object) -> Any:
        self.authority_calls += 1
        raise AssertionError("a Work Runner subclass cannot supply Report authority")


class ForgedComparisonStore(EventImpactTriageWorkComparisonStore):
    authority_calls = 0

    def assert_authoritative_report(self, **_kwargs: object) -> None:
        self.authority_calls += 1
        raise AssertionError("a Comparison Store subclass cannot supply terminal authority")


def _fixture(
    tmp_path: Path,
    *,
    arm: TriageComparisonArm,
    count: int = 121,
    provider: ScriptedWorkProvider | None = None,
    dialect: str = "v2",
) -> RuntimeFixture:
    return _runtime(
        tmp_path,
        arm=arm,
        count=count,
        provider=provider,
        dialect=dialect,
    )


def _race_runner(runner: EventImpactTriageWorkRunner) -> AppendUsageAfterAssertionRunner:
    return AppendUsageAfterAssertionRunner(
        plan=runner.plan,
        candidate_set=runner.candidate_set,
        work_manifest=runner.work_manifest,
        registration=runner.registration,
        provider=runner.provider,
        content_resolver=runner.content_resolver,
        skills=runner.skills,
        artifact_store=runner.artifact_store,
        journal=runner.journal,
        usage_ledger=runner.usage_ledger,
        secret_values=runner.secret_values,
        clock=runner._clock,
    )


def _forged_runner(runner: EventImpactTriageWorkRunner) -> ForgedReplayRunner:
    return ForgedReplayRunner(
        plan=runner.plan,
        candidate_set=runner.candidate_set,
        work_manifest=runner.work_manifest,
        registration=runner.registration,
        provider=runner.provider,
        content_resolver=runner.content_resolver,
        skills=runner.skills,
        artifact_store=runner.artifact_store,
        journal=runner.journal,
        usage_ledger=runner.usage_ledger,
        secret_values=runner.secret_values,
        clock=runner._clock,
    )


def _labels(candidate_set: EventImpactTriageCandidateSet) -> EventImpactTriageLabelSet:
    return EventImpactTriageLabelSet.build(
        candidate_set=candidate_set,
        exposure=TriageLabelExposure.PRISTINE_BLIND,
        labels=tuple(
            TriageGoldLabel(
                version_id=version_id,
                checkpoint_eligibility=CheckpointEligibility.INELIGIBLE,
                expected_route=TriageRoute.ARCHIVE,
                must_catch=False,
                material_transmission_expected=False,
                rationale="The fixture supports no registered checkpoint event.",
            )
            for version_id in candidate_set.version_ids
        ),
        sealed_at=NOW + timedelta(hours=2),
    )


def _failed_labels(candidate_set: EventImpactTriageCandidateSet) -> EventImpactTriageLabelSet:
    labels = list(_labels(candidate_set).labels)
    labels[0] = replace(
        labels[0],
        expected_route=TriageRoute.EVENT_ASSESSMENT,
        must_catch=True,
        material_transmission_expected=True,
        rationale="The fixture marks this event as a must-catch material transmission.",
    )
    return EventImpactTriageLabelSet.build(
        candidate_set=candidate_set,
        exposure=TriageLabelExposure.PRISTINE_BLIND,
        labels=tuple(labels),
        sealed_at=NOW + timedelta(hours=2),
    )


def _outcome(
    runner: EventImpactTriageWorkRunner,
    result: EventImpactTriageWorkRunResult,
) -> TriageWorkArmOutcome:
    assert result.status is RunStatus.COMPLETED
    assert result.partition is not None
    assert result.proposal is not None
    assert result.run_evidence is not None
    return TriageWorkArmOutcome(
        plan=runner.plan,
        work_manifest=runner.work_manifest,
        digests=result.digests,
        partition=result.partition,
        proposal=result.proposal,
        run_evidence=result.run_evidence,
    )


def _workflow_fixture(
    tmp_path: Path,
    *,
    dialect: str = "v9",
) -> tuple[
    PreparedProspectiveTriageWork,
    EventImpactTriageWorkRunner,
    ScriptedWorkProvider,
    EventImpactTriageWorkRunner,
    ScriptedWorkProvider,
]:
    registration = _material_registration()
    baseline_runner, baseline_provider, candidate_set, manifest, _ = _runtime(
        tmp_path / "baseline",
        arm=TriageComparisonArm.BASELINE,
        count=2,
        dialect=dialect,
        registration=registration,
        checkpoint_key="next-material-a-share-event",
    )
    treatment_runner, treatment_provider, treatment_candidate, treatment_manifest, plan = _runtime(
        tmp_path / "treatment",
        arm=TriageComparisonArm.TREATMENT,
        count=2,
        dialect=dialect,
        registration=registration,
        checkpoint_key="next-material-a-share-event",
    )
    assert treatment_candidate == candidate_set
    assert treatment_manifest == manifest
    assert treatment_runner.registration == baseline_runner.registration
    baseline_runner._clock = lambda: NOW + timedelta(hours=4)
    treatment_runner._clock = lambda: NOW + timedelta(hours=4, minutes=30)
    protocol_hashes = {
        "readiness": canonical_hash({"fixture": "readiness"}),
        "selection": canonical_hash({"fixture": "selection"}),
        "candidate_set": canonical_hash(candidate_set.to_dict()),
        "work_manifest": canonical_hash(manifest.to_dict()),
        "execution_plan": canonical_hash(plan.to_dict()),
    }
    selection = SimpleNamespace(
        selection_id="event-impact-triage-batch-selection-fixture",
        selected_at=candidate_set.frozen_at,
        selected_version_ids=candidate_set.version_ids,
    )
    snapshot = SimpleNamespace(snapshot_id=candidate_set.data_snapshot_id)
    active_core = {
        "schema_version": "market-impact.prospective-triage-active-batch.v1",
        "registration_id": candidate_set.registration_id,
        "checkpoint_key": candidate_set.checkpoint_key,
        "route_plan_id": candidate_set.route_plan_id,
        "route_admission_id": candidate_set.route_admission_id,
        "readiness_report_id": candidate_set.readiness_report_id,
        "unclassified_candidate_count": len(candidate_set.version_ids),
        "data_snapshot_id": candidate_set.data_snapshot_id,
        "profile_id": plan.model_provider_profile.profile_id,
        "protocol_artifact_hashes": protocol_hashes,
        "created_at": candidate_set.frozen_at.isoformat().replace("+00:00", "Z"),
    }
    prepared = PreparedProspectiveTriageWork(
        active_batch_id=f"prospective-triage-active-batch-{canonical_hash(active_core)}",
        readiness_report_id=candidate_set.readiness_report_id,
        unclassified_candidate_count=len(candidate_set.version_ids),
        selection=cast(Any, selection),
        snapshot=cast(Any, snapshot),
        candidate_set=candidate_set,
        manifest=manifest,
        plan=plan,
        profile=plan.model_provider_profile,
        protocol_artifact_hashes=protocol_hashes,
    )
    return (
        prepared,
        baseline_runner,
        baseline_provider,
        treatment_runner,
        treatment_provider,
    )


def _patch_workflow_runners(
    monkeypatch: pytest.MonkeyPatch,
    *,
    baseline_runner: EventImpactTriageWorkRunner,
    treatment_runner: EventImpactTriageWorkRunner,
) -> None:
    def frozen_baseline_plan(**_kwargs: object) -> EventImpactTriageWorkExecutionPlan:
        return baseline_runner.plan

    monkeypatch.setattr(
        prospective_triage,
        "build_event_impact_triage_work_execution_plan_v9",
        frozen_baseline_plan,
    )
    monkeypatch.setattr(
        prospective_triage,
        "build_event_impact_triage_work_execution_plan_v11",
        frozen_baseline_plan,
    )

    def build_runner(*, prepared: PreparedProspectiveTriageWork, **_kwargs: object):
        return (
            baseline_runner
            if prepared.plan.plan_id == baseline_runner.plan.plan_id
            else treatment_runner
        )

    monkeypatch.setattr(prospective_triage, "_build_prospective_triage_runner", build_runner)
    comparison_store_type = EventImpactTriageWorkComparisonStore

    def comparison_store(path: Path) -> EventImpactTriageWorkComparisonStore:
        return comparison_store_type(path, clock=lambda: NOW + timedelta(hours=3))

    monkeypatch.setattr(
        prospective_triage,
        "EventImpactTriageWorkComparisonStore",
        comparison_store,
    )


def test_v11_comparison_rebuilds_a_same_version_baseline(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    (
        prepared,
        baseline_runner,
        baseline_provider,
        treatment_runner,
        treatment_provider,
    ) = _workflow_fixture(tmp_path, dialect="v11")
    _patch_workflow_runners(
        monkeypatch,
        baseline_runner=baseline_runner,
        treatment_runner=treatment_runner,
    )
    run_root = tmp_path / "workflow"
    state_root = tmp_path / "state"
    ProspectiveTriageActiveBatchStore(run_root).install(prepared, expected_epoch_revision=0)

    summary = asyncio.run(
        run_prepared_prospective_triage_comparison(
            prepared=prepared,
            registration=treatment_runner.registration,
            label_set=_labels(prepared.candidate_set),
            state_root=state_root,
            run_root=run_root,
            skill_root=tmp_path / "skills",
            baseline_provider=baseline_provider,
            treatment_provider=treatment_provider,
        )
    )

    assert prepared.plan.schema_version == EVENT_IMPACT_TRIAGE_WORK_EXECUTION_PLAN_SCHEMA_V11
    assert baseline_runner.plan.schema_version == EVENT_IMPACT_TRIAGE_WORK_EXECUTION_PLAN_SCHEMA_V11
    assert summary["baseline_plan_id"] == baseline_runner.plan.plan_id
    assert summary["treatment_plan_id"] == prepared.plan.plan_id
    assert summary["status"] == "completed"


def test_v11_operational_ingress_admits_a_triage_decision_without_comparison(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    prepared, _, _, treatment_runner, treatment_provider = _workflow_fixture(
        tmp_path, dialect="v11"
    )

    def build_runner(**_kwargs: object) -> EventImpactTriageWorkRunner:
        return treatment_runner

    monkeypatch.setattr(prospective_triage, "_build_prospective_triage_runner", build_runner)
    run_root = tmp_path / "workflow"
    state_root = tmp_path / "state"
    active_store = ProspectiveTriageActiveBatchStore(run_root)
    active_store.install(prepared, expected_epoch_revision=0)

    summary = asyncio.run(
        run_prepared_prospective_triage_work(
            prepared=prepared,
            registration=treatment_runner.registration,
            state_root=state_root,
            run_root=run_root,
            skill_root=tmp_path / "skills",
            provider=treatment_provider,
        )
    )

    decision_id = cast(str, summary["decision_id"])
    _, _, decision = EventImpactTriageDecisionStore(state_root).get_context(
        prepared.candidate_set.candidate_set_id
    )
    assert summary["status"] == "completed"
    assert decision.decision_id == decision_id
    assert summary["judgment_or_execution_authority"] is False
    assert (
        active_store.active(
            registration_id=prepared.candidate_set.registration_id,
            checkpoint_key=prepared.candidate_set.checkpoint_key,
            route_plan_id=prepared.candidate_set.route_plan_id,
            route_admission_id=prepared.candidate_set.route_admission_id,
        )
        is None
    )


def _rehash_report(
    report: EventImpactTriageWorkComparisonReport,
    *,
    batch_gate_passed: bool | None = None,
    blockers: tuple[str, ...] | None = None,
    baseline_outcome_hash: str | None = None,
    treatment_outcome_hash: str | None = None,
    baseline_authority_receipt_hash: str | None = None,
) -> EventImpactTriageWorkComparisonReport:
    core = report.core_dict()
    resolved_batch_gate = (
        report.batch_gate_passed if batch_gate_passed is None else batch_gate_passed
    )
    resolved_blockers = report.blockers if blockers is None else blockers
    resolved_baseline_outcome = (
        report.baseline_outcome_hash if baseline_outcome_hash is None else baseline_outcome_hash
    )
    resolved_treatment_outcome = (
        report.treatment_outcome_hash if treatment_outcome_hash is None else treatment_outcome_hash
    )
    resolved_baseline_receipt = (
        report.baseline_authority_receipt_hash
        if baseline_authority_receipt_hash is None
        else baseline_authority_receipt_hash
    )
    core.update(
        {
            "batch_gate_passed": resolved_batch_gate,
            "blockers": list(resolved_blockers),
            "baseline_outcome_hash": resolved_baseline_outcome,
            "treatment_outcome_hash": resolved_treatment_outcome,
            "baseline_authority_receipt_hash": resolved_baseline_receipt,
        }
    )
    return replace(
        report,
        report_id=("event-impact-triage-work-comparison-report-" + canonical_hash(core)),
        batch_gate_passed=resolved_batch_gate,
        blockers=resolved_blockers,
        baseline_outcome_hash=resolved_baseline_outcome,
        treatment_outcome_hash=resolved_treatment_outcome,
        baseline_authority_receipt_hash=resolved_baseline_receipt,
    )


def test_121_work_comparison_binds_plans_scores_complete_arms_and_restarts(
    tmp_path: Path,
) -> None:
    baseline_runner, baseline_provider, candidate_set, manifest, baseline_plan = _fixture(
        tmp_path / "baseline", arm=TriageComparisonArm.BASELINE
    )
    (
        treatment_runner,
        treatment_provider,
        treatment_candidates,
        treatment_manifest,
        treatment_plan,
    ) = _fixture(tmp_path / "treatment", arm=TriageComparisonArm.TREATMENT)
    assert treatment_candidates == candidate_set
    assert treatment_manifest == manifest
    labels = _labels(candidate_set)
    store = EventImpactTriageWorkComparisonStore(
        tmp_path / "comparison.sqlite",
        clock=lambda: NOW + timedelta(hours=3),
    )
    registration = store.register(
        candidate_set=candidate_set,
        label_set=labels,
        work_manifest=manifest,
        baseline_plan=baseline_plan,
        treatment_plan=treatment_plan,
    )
    baseline_runner._clock = lambda: NOW + timedelta(hours=4)
    treatment_runner._clock = lambda: NOW + timedelta(hours=4, minutes=30)

    baseline_result = asyncio.run(baseline_runner.run())
    treatment_result = asyncio.run(treatment_runner.run())
    baseline_calls = len(baseline_provider.requests)
    treatment_calls = len(treatment_provider.requests)
    baseline_outcome = _outcome(baseline_runner, baseline_result)
    treatment_outcome = _outcome(treatment_runner, treatment_result)
    report = evaluate_event_impact_triage_work_comparison(
        registration=registration,
        candidate_set=candidate_set,
        label_set=labels,
        work_manifest=manifest,
        baseline=baseline_outcome,
        treatment=treatment_outcome,
        baseline_authority=baseline_runner,
        treatment_authority=treatment_runner,
        registration_authority=store,
        evaluated_at=NOW + timedelta(hours=5),
    )

    assert len(manifest.work_units) == 11
    assert baseline_plan.max_total_runs == 133
    assert treatment_plan.max_total_runs == 166
    assert registration.max_aggregate_estimated_cost_microusd == 299
    assert report.baseline_score.candidate_count == 121
    assert report.treatment_score.candidate_count == 121
    assert report.baseline_score.route_correct == 121
    assert report.treatment_score.route_correct == 121
    assert report.batch_gate_passed
    assert not report.promotion_eligible
    assert "label" not in canonical_json_bytes(manifest.to_dict()).decode().lower()
    assert "label" not in canonical_json_bytes(baseline_plan.to_dict()).decode().lower()
    assert "label" not in canonical_json_bytes(treatment_plan.to_dict()).decode().lower()
    assert not validate_agent_contract(
        registration.to_dict(),
        "event-impact-triage-work-comparison-registration.schema.json",
    )
    assert not validate_agent_contract(
        report.to_dict(), "event-impact-triage-work-comparison-report.schema.json"
    )
    assert_authoritative_event_impact_triage_work_comparison_report(
        report=report,
        registration=registration,
        candidate_set=candidate_set,
        label_set=labels,
        work_manifest=manifest,
        baseline=baseline_outcome,
        treatment=treatment_outcome,
        baseline_authority=baseline_runner,
        treatment_authority=treatment_runner,
        registration_authority=store,
    )
    assert (
        store.record_report(
            report=report,
            registration=registration,
            candidate_set=candidate_set,
            label_set=labels,
            work_manifest=manifest,
            baseline=baseline_outcome,
            treatment=treatment_outcome,
            baseline_authority=baseline_runner,
            treatment_authority=treatment_runner,
        )
        == report
    )
    store.assert_authoritative_report(
        report=report,
        registration=registration,
        candidate_set=candidate_set,
        label_set=labels,
        work_manifest=manifest,
        baseline=baseline_outcome,
        treatment=treatment_outcome,
        baseline_authority=baseline_runner,
        treatment_authority=treatment_runner,
    )

    contradictory_payload = report.to_dict()
    contradictory_payload["batch_gate_passed"] = False
    assert validate_agent_contract(
        contradictory_payload,
        "event-impact-triage-work-comparison-report.schema.json",
    )
    with pytest.raises(ValueError, match="batch gate"):
        _rehash_report(report, batch_gate_passed=False)
    with pytest.raises(ValueError, match="blockers contradict"):
        _rehash_report(
            report,
            blockers=(
                "second_pristine_blind_batch_required",
                "treatment_worse_impact_routing",
            ),
            batch_gate_passed=False,
        )
    unknown_blocker_payload = report.to_dict()
    unknown_blocker_payload["blockers"] = [
        "second_pristine_blind_batch_required",
        "unknown_blocker",
    ]
    assert validate_agent_contract(
        unknown_blocker_payload,
        "event-impact-triage-work-comparison-report.schema.json",
    )
    with pytest.raises(ValueError, match="blockers contradict"):
        _rehash_report(
            report,
            blockers=("second_pristine_blind_batch_required", "unknown_blocker"),
        )

    swapped_outcomes = _rehash_report(
        report,
        baseline_outcome_hash=report.treatment_outcome_hash,
        treatment_outcome_hash=report.baseline_outcome_hash,
    )
    with pytest.raises(ValueError, match="not authoritative"):
        assert_authoritative_event_impact_triage_work_comparison_report(
            report=swapped_outcomes,
            registration=registration,
            candidate_set=candidate_set,
            label_set=labels,
            work_manifest=manifest,
            baseline=baseline_outcome,
            treatment=treatment_outcome,
            baseline_authority=baseline_runner,
            treatment_authority=treatment_runner,
            registration_authority=store,
        )
    tampered_receipt = _rehash_report(
        report,
        baseline_authority_receipt_hash="f" * 64,
    )
    with pytest.raises(ValueError, match="not authoritative"):
        assert_authoritative_event_impact_triage_work_comparison_report(
            report=tampered_receipt,
            registration=registration,
            candidate_set=candidate_set,
            label_set=labels,
            work_manifest=manifest,
            baseline=baseline_outcome,
            treatment=treatment_outcome,
            baseline_authority=baseline_runner,
            treatment_authority=treatment_runner,
            registration_authority=store,
        )

    assert asyncio.run(baseline_runner.run()) == baseline_result
    assert asyncio.run(treatment_runner.run()) == treatment_result
    assert len(baseline_provider.requests) == baseline_calls
    assert len(treatment_provider.requests) == treatment_calls


def test_failed_report_terminalization_replays_durable_report_runs_and_usage(
    tmp_path: Path,
) -> None:
    baseline_runner, _, candidate_set, manifest, baseline_plan = _fixture(
        tmp_path / "baseline", arm=TriageComparisonArm.BASELINE, count=2
    )
    treatment_runner, _, _, _, treatment_plan = _fixture(
        tmp_path / "treatment", arm=TriageComparisonArm.TREATMENT, count=2
    )
    labels = _failed_labels(candidate_set)
    comparison_store = EventImpactTriageWorkComparisonStore(
        tmp_path / "comparison.sqlite",
        clock=lambda: NOW + timedelta(hours=3),
    )
    registration = comparison_store.register(
        candidate_set=candidate_set,
        label_set=labels,
        work_manifest=manifest,
        baseline_plan=baseline_plan,
        treatment_plan=treatment_plan,
    )
    baseline_runner._clock = lambda: NOW + timedelta(hours=4)
    treatment_runner._clock = lambda: NOW + timedelta(hours=4, minutes=30)
    baseline_outcome = _outcome(baseline_runner, asyncio.run(baseline_runner.run()))
    treatment_outcome = _outcome(treatment_runner, asyncio.run(treatment_runner.run()))
    report = evaluate_event_impact_triage_work_comparison(
        registration=registration,
        candidate_set=candidate_set,
        label_set=labels,
        work_manifest=manifest,
        baseline=baseline_outcome,
        treatment=treatment_outcome,
        baseline_authority=baseline_runner,
        treatment_authority=treatment_runner,
        registration_authority=comparison_store,
        evaluated_at=NOW + timedelta(hours=5),
    )
    assert not report.batch_gate_passed
    comparison_store.record_report(
        report=report,
        registration=registration,
        candidate_set=candidate_set,
        label_set=labels,
        work_manifest=manifest,
        baseline=baseline_outcome,
        treatment=treatment_outcome,
        baseline_authority=baseline_runner,
        treatment_authority=treatment_runner,
    )
    forged_runner = _forged_runner(baseline_runner)
    report_arguments = {
        "report": report,
        "registration": registration,
        "candidate_set": candidate_set,
        "label_set": labels,
        "work_manifest": manifest,
        "baseline": baseline_outcome,
        "treatment": treatment_outcome,
        "baseline_authority": forged_runner,
        "treatment_authority": treatment_runner,
    }
    with pytest.raises(TypeError, match="concrete Work Runner authorities"):
        comparison_store.record_report(**cast(Any, report_arguments))
    with pytest.raises(TypeError, match="concrete Work Runner authorities"):
        comparison_store.assert_authoritative_report(**cast(Any, report_arguments))
    reopen_arguments = dict(report_arguments)
    del reopen_arguments["report"]
    with pytest.raises(TypeError, match="concrete Work Runner authorities"):
        comparison_store.reopen_report(**cast(Any, reopen_arguments))
    assert forged_runner.authority_calls == 0
    terminal_store = EventImpactTriageDecisionStore(tmp_path / "terminal-state")
    forged_comparison_store = ForgedComparisonStore(comparison_store.path)

    with pytest.raises(TypeError, match="durable Comparison Store authority"):
        terminal_store.terminalize_failed_work_comparison(
            candidate_set=candidate_set,
            comparison=registration,
            report=report,
            label_set=labels,
            work_manifest=manifest,
            baseline=baseline_outcome,
            treatment=treatment_outcome,
            baseline_authority=baseline_runner,
            treatment_authority=treatment_runner,
            comparison_authority=forged_comparison_store,
            terminalized_at=NOW + timedelta(hours=6),
        )
    assert forged_comparison_store.authority_calls == 0
    with pytest.raises(KeyError, match="unknown triage terminal batch"):
        terminal_store.terminal_batch(candidate_set.candidate_set_id)
    assert (
        terminal_store.classified_version_ids(
            registration_id=candidate_set.registration_id,
            checkpoint_key=candidate_set.checkpoint_key,
            route_plan_id=candidate_set.route_plan_id,
            route_admission_id=candidate_set.route_admission_id,
            at=NOW + timedelta(hours=6),
        )
        == ()
    )

    terminal = terminal_store.terminalize_failed_work_comparison(
        candidate_set=candidate_set,
        comparison=registration,
        report=report,
        label_set=labels,
        work_manifest=manifest,
        baseline=baseline_outcome,
        treatment=treatment_outcome,
        baseline_authority=baseline_runner,
        treatment_authority=treatment_runner,
        comparison_authority=comparison_store,
        terminalized_at=NOW + timedelta(hours=6),
    )

    assert terminal_store.terminal_batch(candidate_set.candidate_set_id) == terminal
    assert (
        terminal_store.reopen_failed_work_comparison_terminal(
            candidate_set=candidate_set,
            comparison=registration,
            report=report,
        )
        == terminal
    )
    assert terminal_store.classified_version_ids(
        registration_id=candidate_set.registration_id,
        checkpoint_key=candidate_set.checkpoint_key,
        route_plan_id=candidate_set.route_plan_id,
        route_admission_id=candidate_set.route_admission_id,
        at=terminal.terminalized_at,
    ) == tuple(sorted(candidate_set.version_ids))


def test_comparison_workflow_reopens_report_after_commit_crash(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    (
        prepared,
        baseline_runner,
        baseline_provider,
        treatment_runner,
        treatment_provider,
    ) = _workflow_fixture(tmp_path)
    _patch_workflow_runners(
        monkeypatch,
        baseline_runner=baseline_runner,
        treatment_runner=treatment_runner,
    )
    run_root = tmp_path / "workflow"
    state_root = tmp_path / "state"
    active_store = ProspectiveTriageActiveBatchStore(run_root)
    active_store.install(prepared, expected_epoch_revision=0)
    baseline_availability = AvailabilityOnlyProvider()
    treatment_availability = AvailabilityOnlyProvider()
    original_record = EventImpactTriageWorkComparisonStore.record_report
    crashed = False

    def record_then_crash(self: EventImpactTriageWorkComparisonStore, **kwargs: object):
        nonlocal crashed
        report = original_record(self, **cast(Any, kwargs))
        if not crashed:
            crashed = True
            raise RuntimeError("simulated crash after Report commit")
        return report

    monkeypatch.setattr(EventImpactTriageWorkComparisonStore, "record_report", record_then_crash)
    with pytest.raises(RuntimeError, match="after Report commit"):
        asyncio.run(
            run_prepared_prospective_triage_comparison(
                prepared=prepared,
                registration=treatment_runner.registration,
                label_set=_labels(prepared.candidate_set),
                state_root=state_root,
                run_root=run_root,
                skill_root=tmp_path / "skills",
                baseline_provider=cast(Any, baseline_availability),
                treatment_provider=cast(Any, treatment_availability),
            )
        )
    comparison_path = run_root / "comparison" / "registrations.sqlite3"
    with sqlite3.connect(comparison_path) as connection:
        stored_report = connection.execute(
            "SELECT report_id, evaluated_at FROM triage_work_comparison_reports"
        ).fetchone()
    assert stored_report is not None
    baseline_calls = len(baseline_provider.requests)
    treatment_calls = len(treatment_provider.requests)
    monkeypatch.setattr(
        EventImpactTriageWorkComparisonStore,
        "record_report",
        original_record,
    )

    summary = asyncio.run(
        run_prepared_prospective_triage_comparison(
            prepared=prepared,
            registration=treatment_runner.registration,
            label_set=_labels(prepared.candidate_set),
            state_root=state_root,
            run_root=run_root,
            skill_root=tmp_path / "skills",
            baseline_provider=cast(Any, baseline_availability),
            treatment_provider=cast(Any, treatment_availability),
        )
    )

    assert summary["comparison_report_id"] == stored_report[0]
    assert baseline_availability.availability_calls == 0
    assert treatment_availability.availability_calls == 0
    assert len(baseline_provider.requests) == baseline_calls
    assert len(treatment_provider.requests) == treatment_calls
    assert (
        active_store.active(
            registration_id=prepared.candidate_set.registration_id,
            checkpoint_key=prepared.candidate_set.checkpoint_key,
            route_plan_id=prepared.candidate_set.route_plan_id,
            route_admission_id=prepared.candidate_set.route_admission_id,
        )
        is None
    )
    _, _, decision = EventImpactTriageDecisionStore(state_root).get_context(
        prepared.candidate_set.candidate_set_id
    )
    assert summary["decision_id"] == decision.decision_id


def test_comparison_workflow_reopens_terminal_after_head_release_crash(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    (
        prepared,
        baseline_runner,
        baseline_provider,
        treatment_runner,
        treatment_provider,
    ) = _workflow_fixture(tmp_path)
    _patch_workflow_runners(
        monkeypatch,
        baseline_runner=baseline_runner,
        treatment_runner=treatment_runner,
    )
    run_root = tmp_path / "workflow"
    state_root = tmp_path / "state"
    active_store = ProspectiveTriageActiveBatchStore(run_root)
    active_store.install(prepared, expected_epoch_revision=0)
    baseline_availability = AvailabilityOnlyProvider()
    treatment_availability = AvailabilityOnlyProvider()
    original_terminalize = ProspectiveTriageActiveBatchStore.terminalize

    def crash_before_head_release(self: ProspectiveTriageActiveBatchStore, **_kwargs: object):
        raise RuntimeError("simulated crash before active head release")

    monkeypatch.setattr(
        ProspectiveTriageActiveBatchStore,
        "terminalize",
        crash_before_head_release,
    )
    failed_labels = _failed_labels(prepared.candidate_set)
    with pytest.raises(RuntimeError, match="before active head release"):
        asyncio.run(
            run_prepared_prospective_triage_comparison(
                prepared=prepared,
                registration=treatment_runner.registration,
                label_set=failed_labels,
                state_root=state_root,
                run_root=run_root,
                skill_root=tmp_path / "skills",
                baseline_provider=cast(Any, baseline_availability),
                treatment_provider=cast(Any, treatment_availability),
            )
        )
    terminal_store = EventImpactTriageDecisionStore(state_root)
    stored_terminal = terminal_store.terminal_batch(prepared.candidate_set.candidate_set_id)
    baseline_calls = len(baseline_provider.requests)
    treatment_calls = len(treatment_provider.requests)
    monkeypatch.setattr(
        ProspectiveTriageActiveBatchStore,
        "terminalize",
        original_terminalize,
    )

    def forbidden_factory(_cls: type[object]) -> object:
        raise AssertionError("terminal recovery must not construct a Provider factory")

    monkeypatch.setattr(
        prospective_triage.ModelProviderFactory,
        "with_builtin_adapters",
        classmethod(forbidden_factory),
    )

    summary = asyncio.run(
        run_prepared_prospective_triage_comparison(
            prepared=prepared,
            registration=treatment_runner.registration,
            label_set=failed_labels,
            state_root=state_root,
            run_root=run_root,
            skill_root=tmp_path / "skills",
        )
    )

    reopened_terminal = terminal_store.terminal_batch(prepared.candidate_set.candidate_set_id)
    assert reopened_terminal == stored_terminal
    assert summary["terminal_id"] == stored_terminal.terminal_id
    assert baseline_availability.availability_calls == 0
    assert treatment_availability.availability_calls == 0
    assert len(baseline_provider.requests) == baseline_calls
    assert len(treatment_provider.requests) == treatment_calls
    lookup = {
        "registration_id": prepared.candidate_set.registration_id,
        "checkpoint_key": prepared.candidate_set.checkpoint_key,
        "route_plan_id": prepared.candidate_set.route_plan_id,
        "route_admission_id": prepared.candidate_set.route_admission_id,
    }
    assert active_store.active(**lookup) is None
    assert active_store.epoch_revision(**lookup) == 1
    active_store.terminalize(
        batch_id=prepared.active_batch_id,
        candidate_set=prepared.candidate_set,
        terminal=stored_terminal,
        state_root=state_root,
    )
    assert active_store.active(**lookup) is None
    assert active_store.epoch_revision(**lookup) == 1
    with pytest.raises(ValueError, match="terminalized prospective Triage batch"):
        active_store.install(prepared, expected_epoch_revision=1)
    assert active_store.active(**lookup) is None
    with pytest.raises(KeyError, match="unknown event impact Triage Candidate Set"):
        terminal_store.get_context(prepared.candidate_set.candidate_set_id)


def test_terminalization_rejects_usage_changed_after_durable_report(tmp_path: Path) -> None:
    baseline_runner, _, candidate_set, manifest, baseline_plan = _fixture(
        tmp_path / "baseline", arm=TriageComparisonArm.BASELINE, count=2
    )
    treatment_runner, _, _, _, treatment_plan = _fixture(
        tmp_path / "treatment", arm=TriageComparisonArm.TREATMENT, count=2
    )
    labels = _failed_labels(candidate_set)
    comparison_store = EventImpactTriageWorkComparisonStore(
        tmp_path / "comparison.sqlite",
        clock=lambda: NOW + timedelta(hours=3),
    )
    registration = comparison_store.register(
        candidate_set=candidate_set,
        label_set=labels,
        work_manifest=manifest,
        baseline_plan=baseline_plan,
        treatment_plan=treatment_plan,
    )
    baseline_runner._clock = lambda: NOW + timedelta(hours=4)
    treatment_runner._clock = lambda: NOW + timedelta(hours=4, minutes=30)
    baseline_outcome = _outcome(baseline_runner, asyncio.run(baseline_runner.run()))
    treatment_outcome = _outcome(treatment_runner, asyncio.run(treatment_runner.run()))
    report = evaluate_event_impact_triage_work_comparison(
        registration=registration,
        candidate_set=candidate_set,
        label_set=labels,
        work_manifest=manifest,
        baseline=baseline_outcome,
        treatment=treatment_outcome,
        baseline_authority=baseline_runner,
        treatment_authority=treatment_runner,
        registration_authority=comparison_store,
        evaluated_at=NOW + timedelta(hours=5),
    )
    comparison_store.record_report(
        report=report,
        registration=registration,
        candidate_set=candidate_set,
        label_set=labels,
        work_manifest=manifest,
        baseline=baseline_outcome,
        treatment=treatment_outcome,
        baseline_authority=baseline_runner,
        treatment_authority=treatment_runner,
    )
    stored_usage = baseline_runner.usage_ledger.records()[0].record
    baseline_runner.usage_ledger.append(
        replace(stored_usage, run_id="triage-work-post-report-extra-usage")
    )
    terminal_store = EventImpactTriageDecisionStore(tmp_path / "terminal-state")

    with pytest.raises(ValueError, match="Usage"):
        terminal_store.terminalize_failed_work_comparison(
            candidate_set=candidate_set,
            comparison=registration,
            report=report,
            label_set=labels,
            work_manifest=manifest,
            baseline=baseline_outcome,
            treatment=treatment_outcome,
            baseline_authority=baseline_runner,
            treatment_authority=treatment_runner,
            comparison_authority=comparison_store,
            terminalized_at=NOW + timedelta(hours=6),
        )
    assert (
        terminal_store.classified_version_ids(
            registration_id=candidate_set.registration_id,
            checkpoint_key=candidate_set.checkpoint_key,
            route_plan_id=candidate_set.route_plan_id,
            route_admission_id=candidate_set.route_admission_id,
            at=NOW + timedelta(hours=6),
        )
        == ()
    )


def test_work_comparison_registration_rejects_backdating_identity_and_forgery(
    tmp_path: Path,
) -> None:
    baseline, _, candidate_set, manifest, baseline_plan = _fixture(
        tmp_path / "baseline", arm=TriageComparisonArm.BASELINE, count=2
    )
    treatment, _, _, _, treatment_plan = _fixture(
        tmp_path / "treatment", arm=TriageComparisonArm.TREATMENT, count=2
    )
    labels = _labels(candidate_set)
    backdated_seal = candidate_set.frozen_at - timedelta(seconds=1)
    backdated_core = {
        "schema_version": labels.schema_version,
        "candidate_set_id": labels.candidate_set_id,
        "exposure": labels.exposure.value,
        "labels": [item.to_dict() for item in labels.labels],
        "sealed_at": backdated_seal.isoformat().replace("+00:00", "Z"),
    }
    backdated_labels = EventImpactTriageLabelSet(
        label_set_id=("event-impact-triage-label-set-" + canonical_hash(backdated_core)),
        candidate_set_id=labels.candidate_set_id,
        exposure=labels.exposure,
        labels=labels.labels,
        sealed_at=backdated_seal,
    )
    with pytest.raises(ValueError, match="predate the frozen Candidate Set"):
        EventImpactTriageWorkComparisonRegistration.build(
            candidate_set=candidate_set,
            label_set=backdated_labels,
            work_manifest=manifest,
            baseline_plan=baseline_plan,
            treatment_plan=treatment_plan,
            registered_at=labels.sealed_at,
        )
    with pytest.raises(ValueError, match="after labels"):
        EventImpactTriageWorkComparisonRegistration.build(
            candidate_set=candidate_set,
            label_set=labels,
            work_manifest=manifest,
            baseline_plan=baseline_plan,
            treatment_plan=treatment_plan,
            registered_at=labels.sealed_at - timedelta(seconds=1),
        )
    store = EventImpactTriageWorkComparisonStore(
        tmp_path / "comparison.sqlite",
        clock=lambda: NOW + timedelta(hours=3),
    )
    registration = store.register(
        candidate_set=candidate_set,
        label_set=labels,
        work_manifest=manifest,
        baseline_plan=baseline_plan,
        treatment_plan=treatment_plan,
    )
    assert store.has_registration_for_candidate_set(candidate_set.candidate_set_id)
    assert not store.has_registration_for_candidate_set(
        "event-impact-triage-candidate-set-" + "f" * 64
    )
    with pytest.raises(ValueError, match="does not match content"):
        replace(registration, candidate_set_hash="f" * 64)
    with pytest.raises(ValueError, match="model_profile_id"):
        replace(registration, model_profile_id="model-provider-fixture")
    with pytest.raises(ValueError, match="does not match content"):
        replace(
            registration,
            registered_at=registration.registered_at + timedelta(seconds=1),
        )
    with sqlite3.connect(store.path) as connection:
        connection.execute("DROP TRIGGER triage_work_comparison_registrations_no_update")
        connection.execute(
            """
            UPDATE triage_work_comparison_registrations
            SET registration_json = ? WHERE comparison_id = ?
            """,
            ("{}", registration.comparison_id),
        )
    with pytest.raises(ValueError, match="differs"):
        store.assert_authoritative_registration(registration)
    _ = (baseline, treatment)


def test_work_authority_receipt_rejects_tampered_journal_timestamp(tmp_path: Path) -> None:
    runner, _, candidate_set, manifest, _ = _fixture(
        tmp_path, arm=TriageComparisonArm.BASELINE, count=2
    )
    result = asyncio.run(runner.run())
    outcome = _outcome(runner, result)
    member = outcome.run_evidence.members[0]
    with sqlite3.connect(runner.journal.path) as connection:
        connection.execute(
            "UPDATE runs SET created_at = ? WHERE run_id = ?",
            ("2026-08-30T05:59:59Z", member.run_id),
        )
    with pytest.raises(ValueError, match="started_at"):
        runner.authoritative_completed_work_run_receipt(
            candidate_set=candidate_set,
            work_manifest=manifest,
            digests=outcome.digests,
            partition=outcome.partition,
            proposal=outcome.proposal,
            run_evidence=outcome.run_evidence,
        )


def test_work_authority_receipt_rejects_usage_appended_after_full_assertion(
    tmp_path: Path,
) -> None:
    runner, _, candidate_set, manifest, _ = _fixture(
        tmp_path, arm=TriageComparisonArm.BASELINE, count=2
    )
    outcome = _outcome(runner, asyncio.run(runner.run()))
    racing = _race_runner(runner)
    racing.inject_extra_usage = True

    with pytest.raises(ValueError, match="changed after authoritative reopening"):
        racing.authoritative_completed_work_run_receipt(
            candidate_set=candidate_set,
            work_manifest=manifest,
            digests=outcome.digests,
            partition=outcome.partition,
            proposal=outcome.proposal,
            run_evidence=outcome.run_evidence,
        )


def test_work_authority_rejects_non_monotonic_timestamp_in_multi_member_run(
    tmp_path: Path,
) -> None:
    runner, _, candidate_set, manifest, _ = _fixture(
        tmp_path, arm=TriageComparisonArm.BASELINE, count=2
    )
    outcome = _outcome(runner, asyncio.run(runner.run()))
    assert len(outcome.run_evidence.members) > 1
    member = outcome.run_evidence.members[1]
    record = runner.journal.get_run(member.run_id)
    non_monotonic_start = record.updated_at + timedelta(seconds=1)
    with sqlite3.connect(runner.journal.path) as connection:
        connection.execute(
            "UPDATE runs SET created_at = ? WHERE run_id = ?",
            (non_monotonic_start.isoformat().replace("+00:00", "Z"), member.run_id),
        )

    with pytest.raises(ValueError, match="finishes before it starts"):
        runner.authoritative_completed_work_run_receipt(
            candidate_set=candidate_set,
            work_manifest=manifest,
            digests=outcome.digests,
            partition=outcome.partition,
            proposal=outcome.proposal,
            run_evidence=outcome.run_evidence,
        )


def test_work_comparison_rejects_pre_registration_start_and_extra_usage(
    tmp_path: Path,
) -> None:
    baseline_runner, _, candidate_set, manifest, baseline_plan = _fixture(
        tmp_path / "baseline", arm=TriageComparisonArm.BASELINE, count=2
    )
    treatment_runner, _, _, _, treatment_plan = _fixture(
        tmp_path / "treatment", arm=TriageComparisonArm.TREATMENT, count=2
    )
    labels = _labels(candidate_set)
    store = EventImpactTriageWorkComparisonStore(
        tmp_path / "comparison.sqlite",
        clock=lambda: NOW + timedelta(hours=3),
    )
    registration = store.register(
        candidate_set=candidate_set,
        label_set=labels,
        work_manifest=manifest,
        baseline_plan=baseline_plan,
        treatment_plan=treatment_plan,
    )
    baseline_runner._clock = lambda: NOW + timedelta(hours=2, minutes=30)
    treatment_runner._clock = lambda: NOW + timedelta(hours=4)
    baseline_result = asyncio.run(baseline_runner.run())
    treatment_result = asyncio.run(treatment_runner.run())
    baseline_outcome = _outcome(baseline_runner, baseline_result)
    treatment_outcome = _outcome(treatment_runner, treatment_result)

    with pytest.raises(ValueError, match="started before registration"):
        evaluate_event_impact_triage_work_comparison(
            registration=registration,
            candidate_set=candidate_set,
            label_set=labels,
            work_manifest=manifest,
            baseline=baseline_outcome,
            treatment=treatment_outcome,
            baseline_authority=baseline_runner,
            treatment_authority=treatment_runner,
            registration_authority=store,
            evaluated_at=NOW + timedelta(hours=5),
        )

    stored = baseline_runner.usage_ledger.records()[0].record
    baseline_runner.usage_ledger.append(replace(stored, run_id="triage-work-extra-usage-record"))
    with pytest.raises(ValueError, match="Usage Ledger"):
        baseline_runner.authoritative_completed_work_run_receipt(
            candidate_set=candidate_set,
            work_manifest=manifest,
            digests=baseline_outcome.digests,
            partition=baseline_outcome.partition,
            proposal=baseline_outcome.proposal,
            run_evidence=baseline_outcome.run_evidence,
        )


def test_v3_comparison_registers_and_reports_but_mixed_revisions_fail(
    tmp_path: Path,
) -> None:
    baseline, _, candidate_set, manifest, baseline_plan = _fixture(
        tmp_path / "baseline-v3",
        arm=TriageComparisonArm.BASELINE,
        count=3,
        dialect="v3",
    )
    treatment, _, treatment_candidates, treatment_manifest, treatment_plan = _fixture(
        tmp_path / "treatment-v3",
        arm=TriageComparisonArm.TREATMENT,
        count=3,
        dialect="v3",
    )
    v2_baseline, _, v2_candidates, v2_manifest, v2_baseline_plan = _fixture(
        tmp_path / "baseline-v2",
        arm=TriageComparisonArm.BASELINE,
        count=3,
    )
    assert treatment_candidates == candidate_set == v2_candidates
    assert treatment_manifest == manifest == v2_manifest
    labels = _labels(candidate_set)

    with pytest.raises(ValueError, match="cannot mix Plan schema revisions"):
        EventImpactTriageWorkComparisonRegistration.build(
            candidate_set=candidate_set,
            label_set=labels,
            work_manifest=manifest,
            baseline_plan=v2_baseline_plan,
            treatment_plan=treatment_plan,
            registered_at=NOW + timedelta(hours=3),
        )
    assert v2_baseline.plan == v2_baseline_plan

    store = EventImpactTriageWorkComparisonStore(
        tmp_path / "comparison-v3.sqlite",
        clock=lambda: NOW + timedelta(hours=3),
    )
    registration = store.register(
        candidate_set=candidate_set,
        label_set=labels,
        work_manifest=manifest,
        baseline_plan=baseline_plan,
        treatment_plan=treatment_plan,
    )
    baseline._clock = lambda: NOW + timedelta(hours=4)
    treatment._clock = lambda: NOW + timedelta(hours=4, minutes=10)
    baseline_result = asyncio.run(baseline.run())
    treatment_result = asyncio.run(treatment.run())
    report = evaluate_event_impact_triage_work_comparison(
        registration=registration,
        candidate_set=candidate_set,
        label_set=labels,
        work_manifest=manifest,
        baseline=_outcome(baseline, baseline_result),
        treatment=_outcome(treatment, treatment_result),
        baseline_authority=baseline,
        treatment_authority=treatment,
        registration_authority=store,
        evaluated_at=NOW + timedelta(hours=5),
    )
    assert registration.schema_version == (
        "market-impact.event-impact-triage-work-comparison-registration.v1"
    )
    assert report.schema_version == "market-impact.event-impact-triage-work-comparison-report.v1"
    assert report.batch_gate_passed


def test_failed_or_ambiguous_work_arm_has_no_scorable_outcome(tmp_path: Path) -> None:
    provider = CrashAfterDispatchProvider()
    runner, _, _, _, _ = _fixture(
        tmp_path,
        arm=TriageComparisonArm.BASELINE,
        count=2,
        provider=provider,
    )
    with pytest.raises(SimulatedProcessCrash):
        asyncio.run(runner.run())
    result = asyncio.run(runner.run())
    assert result.status is RunStatus.HUMAN_INPUT_REQUIRED
    assert result.partition is None
    assert result.proposal is None
    assert result.run_evidence is None
    assert len(provider.requests) == 1
