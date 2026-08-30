# pyright: reportPrivateUsage=false

import asyncio
import sqlite3
from dataclasses import replace
from datetime import timedelta
from pathlib import Path

import pytest

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
    EventImpactTriageWorkExecutionPlan,
    EventImpactTriageWorkRunEvidence,
    EventImpactTriageWorkRunner,
    EventImpactTriageWorkRunResult,
)
from market_impact_agent.runtime_store import RunStatus
from tests.test_event_impact_triage_work_runtime import (
    NOW,
    CrashAfterDispatchProvider,
    ScriptedWorkProvider,
    SimulatedProcessCrash,
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
