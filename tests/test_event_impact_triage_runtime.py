import asyncio
import json
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path

import pytest

from market_impact_agent.agent_contracts import canonical_hash, canonical_json_bytes
from market_impact_agent.agent_runtime import (
    ModelProvider,
    ModelTurn,
    ProviderUsage,
    SkillRegistry,
)
from market_impact_agent.agent_schema import validate_agent_contract
from market_impact_agent.event_impact_triage import (
    CheckpointEligibility,
    EventImpactTriageCandidateSet,
    EventImpactTriageProposal,
    TriageAgentRole,
    TriageClusterProposal,
    TriageObservationRef,
    TriageRoute,
    TriageRunEvidence,
    TriageRunMemberEvidence,
    admit_event_impact_triage,
)
from market_impact_agent.event_impact_triage_evaluation import (
    EventImpactTriageComparisonRegistration,
    EventImpactTriageComparisonStore,
    EventImpactTriageLabelSet,
    TriageArmOutcome,
    TriageGoldLabel,
    TriageLabelExposure,
    evaluate_event_impact_triage_comparison,
    event_impact_triage_label_set_from_dict,
    score_event_impact_triage_proposal,
)
from market_impact_agent.event_impact_triage_runtime import (
    EVENT_IMPACT_TRIAGE_EXECUTION_PLAN_SCHEMA_V1,
    EVENT_IMPACT_TRIAGE_EXECUTION_PLAN_SCHEMA_V2,
    EventImpactTriageExecutionPlan,
    EventImpactTriageRunner,
    TriageCandidateContent,
    TriageComparisonArm,
    TriageFindingType,
    TriageSpecialistArtifact,
    TriageSpecialistFinding,
    build_event_impact_triage_execution_plan,
    build_event_impact_triage_execution_plan_v2,
    event_impact_triage_execution_plan_from_dict,
)
from market_impact_agent.model_provider import load_builtin_model_provider_profile
from market_impact_agent.prospective_diagnostic import (
    ProspectiveDiagnosticRegistration,
    load_prospective_diagnostic_registration,
)
from market_impact_agent.research import EventArchetype, EventStage, TransmissionChannel
from market_impact_agent.runtime_store import ArtifactStore, RunJournal, RunStatus
from market_impact_agent.usage_ledger import UsageLedger

ROOT = Path(__file__).resolve().parents[1]
NOW = datetime(2026, 8, 30, 6, tzinfo=UTC)
PROFILE_ALIAS = "cliproxyapi-luna-xhigh-v1"


class StaticContentResolver:
    def __init__(self, contents: tuple[TriageCandidateContent, ...]) -> None:
        self.contents = contents

    def resolve(
        self, candidate_set: EventImpactTriageCandidateSet
    ) -> tuple[TriageCandidateContent, ...]:
        assert candidate_set.version_ids == tuple(item.version_id for item in self.contents)
        return self.contents


class FixtureComparisonAuthority:
    def __init__(
        self,
        *,
        candidate_set: EventImpactTriageCandidateSet,
        proposal: EventImpactTriageProposal,
        run_evidence: TriageRunEvidence,
        started_at: datetime,
        finished_at: datetime,
        total_cost: int,
    ) -> None:
        self.candidate_set = candidate_set
        self.proposal = proposal
        self.run_evidence = run_evidence
        self.started_at = started_at
        self.finished_at = finished_at
        self.total_cost = total_cost

    def assert_authoritative_completed_triage_run(
        self,
        *,
        candidate_set: EventImpactTriageCandidateSet,
        proposal: EventImpactTriageProposal,
        run_evidence: TriageRunEvidence,
    ) -> None:
        assert candidate_set == self.candidate_set
        assert proposal == self.proposal
        assert run_evidence == self.run_evidence

    def authoritative_started_at(self, run_evidence: TriageRunEvidence) -> datetime:
        assert run_evidence == self.run_evidence
        return self.started_at

    def authoritative_finished_at(self, run_evidence: TriageRunEvidence) -> datetime:
        assert run_evidence == self.run_evidence
        return self.finished_at

    def authoritative_total_estimated_cost_microusd(self, run_evidence: TriageRunEvidence) -> int:
        assert run_evidence == self.run_evidence
        return self.total_cost


class FixtureProvider(ModelProvider):
    def __init__(self, responses: tuple[dict[str, object], ...]) -> None:
        self.responses = list(responses)
        self.requests: list[tuple[dict[str, object], ...]] = []

    @property
    def provider_id(self) -> str:
        return "cliproxyapi-openai-compatible"

    @property
    def model(self) -> str:
        return "gpt-5.6-luna"

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
        _ = (temperature, top_p, max_output_tokens, timeout_seconds)
        assert tools == ()
        self.requests.append(messages)
        payload = self.responses.pop(0)
        content = canonical_json_bytes(payload).decode()
        return ModelTurn(
            response_id=f"fixture-{len(self.requests)}",
            model=self.model,
            assistant_message={"role": "assistant", "content": content},
            tool_calls=(),
            finish_reason="stop",
            usage=ProviderUsage(input_tokens=500, output_tokens=200),
            raw_response={"id": f"fixture-{len(self.requests)}", "content": content},
            latency_ms=15.0,
        )


class SimulatedProcessCrash(BaseException):
    pass


class CrashOnceProvider(FixtureProvider):
    def __init__(self, response: dict[str, object]) -> None:
        super().__init__((response,))
        self.crashed = False

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
        if not self.crashed:
            self.crashed = True
            self.requests.append(messages)
            raise SimulatedProcessCrash
        return await super().complete(
            messages=messages,
            tools=tools,
            temperature=temperature,
            top_p=top_p,
            max_output_tokens=max_output_tokens,
            timeout_seconds=timeout_seconds,
        )


class OverBudgetProvider(FixtureProvider):
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
        turn = await super().complete(
            messages=messages,
            tools=tools,
            temperature=temperature,
            top_p=top_p,
            max_output_tokens=max_output_tokens,
            timeout_seconds=timeout_seconds,
        )
        return replace(
            turn,
            usage=ProviderUsage(input_tokens=10_000_000, output_tokens=10_000_000),
        )


def _registration() -> ProspectiveDiagnosticRegistration:
    return load_prospective_diagnostic_registration(
        ROOT / "examples" / "research" / "prospective-diagnostic-registration-v3.json"
    )


def _candidate_set() -> tuple[EventImpactTriageCandidateSet, tuple[TriageCandidateContent, ...]]:
    registration = _registration()
    payloads: tuple[dict[str, object], ...] = (
        {
            "publisher": "CSRC",
            "headline": "Routine administrative notice",
            "summary": "No capital-market policy or market-structure change is announced.",
        },
        {
            "aggregator": "Tushare",
            "upstream_publisher": "Sina Finance",
            "record": {"title": "Commodity update", "content": "Spot prices changed."},
        },
    )
    refs: list[TriageObservationRef] = []
    contents: list[TriageCandidateContent] = []
    for index, payload in enumerate(payloads, start=1):
        version_id = "prospective-observation-version-" + str(index) * 64
        observed_at = NOW.replace(minute=index)
        refs.append(
            TriageObservationRef(
                version_id=version_id,
                observation_id="source-observation-" + str(index + 2) * 64,
                first_available_at=observed_at,
                authority_at=observed_at,
                provider_id="fixture-provider",
                provider_version="fixture-v1",
                upstream_source="fixture-source",
                source_ref=f"fixture://news/{index}",
                raw_content_hash=str(index + 4) * 64,
                normalized_payload_hash=canonical_hash(payload),
            )
        )
        contents.append(
            TriageCandidateContent(
                version_id=version_id,
                normalized_payload=payload,
                license_scope="private_research_no_redistribution",
            )
        )
    ordered = tuple(refs)
    core = {
        "schema_version": "market-impact.event-impact-triage-candidate-set.v1",
        "registration_id": registration.registration_id,
        "checkpoint_key": "next-a-share-policy-event",
        "route_plan_id": "prospective-checkpoint-route-plan-" + "7" * 64,
        "route_admission_id": "prospective-checkpoint-route-admission-" + "8" * 64,
        "readiness_report_id": "prospective-checkpoint-readiness-report-" + "9" * 64,
        "data_snapshot_id": "data-snapshot-" + "a" * 64,
        "admitted_at": "2026-08-30T06:00:00Z",
        "frozen_at": "2026-08-30T06:03:00Z",
        "observations": [item.to_dict() for item in ordered],
        "historical_pit_claim": False,
        "judgment_model_calls_authorized": False,
        "execution_capability": False,
    }
    return (
        EventImpactTriageCandidateSet(
            candidate_set_id=f"event-impact-triage-candidate-set-{canonical_hash(core)}",
            registration_id=registration.registration_id,
            checkpoint_key="next-a-share-policy-event",
            route_plan_id="prospective-checkpoint-route-plan-" + "7" * 64,
            route_admission_id="prospective-checkpoint-route-admission-" + "8" * 64,
            readiness_report_id="prospective-checkpoint-readiness-report-" + "9" * 64,
            data_snapshot_id="data-snapshot-" + "a" * 64,
            admitted_at=datetime(2026, 8, 30, 6, tzinfo=UTC),
            frozen_at=datetime(2026, 8, 30, 6, 3, tzinfo=UTC),
            observations=ordered,
        ),
        tuple(contents),
    )


def _plan(arm: TriageComparisonArm) -> EventImpactTriageExecutionPlan:
    candidate_set, _ = _candidate_set()
    profile = load_builtin_model_provider_profile(PROFILE_ALIAS)
    return build_event_impact_triage_execution_plan(
        arm=arm,
        candidate_set=candidate_set,
        registration=_registration(),
        model_profile_alias=PROFILE_ALIAS,
        model_profile=profile,
        skills=SkillRegistry(ROOT / "skills"),
    )


def _plan_v2(arm: TriageComparisonArm) -> EventImpactTriageExecutionPlan:
    candidate_set, _ = _candidate_set()
    profile = load_builtin_model_provider_profile(PROFILE_ALIAS)
    return build_event_impact_triage_execution_plan_v2(
        arm=arm,
        candidate_set=candidate_set,
        registration=_registration(),
        model_profile_alias=PROFILE_ALIAS,
        model_profile=profile,
        skills=SkillRegistry(ROOT / "skills"),
    )


def _ineligible_draft(candidate_set: EventImpactTriageCandidateSet) -> dict[str, object]:
    return {
        "candidate_set_id": candidate_set.candidate_set_id,
        "clusters": [
            {
                "candidate_version_ids": list(candidate_set.version_ids),
                "checkpoint_eligibility": "ineligible",
                "recommended_route": "archive",
                "event_archetypes": [],
                "event_stage": "first_observed",
                "changed_facts": [],
                "rule_reasons": [
                    "No candidate reports a capital-market policy or market-structure change."
                ],
                "evidence_version_ids": list(candidate_set.version_ids),
                "uncertainty_notes": [],
                "countercases": [],
                "transmission_channels": [],
                "affected_entity_refs": [],
                "watch_questions": [],
                "triage_confidence": 0.84,
            }
        ],
    }


def _specialist_draft(
    candidate_set: EventImpactTriageCandidateSet,
    *,
    role: TriageAgentRole,
    finding_type: str,
) -> dict[str, object]:
    return {
        "candidate_set_id": candidate_set.candidate_set_id,
        "role": role.value,
        "covered_candidate_version_ids": sorted(candidate_set.version_ids),
        "findings": [
            {
                "finding_type": finding_type,
                "candidate_version_ids": [candidate_set.version_ids[0]],
                "evidence_version_ids": [candidate_set.version_ids[0]],
                "statement": f"Fixture finding for {role.value}.",
                "uncertainty_notes": [],
                "affected_entity_refs": [],
                "transmission_channels": [],
                "evidence_lane": None,
            }
        ],
    }


def test_execution_plans_freeze_distinct_bounded_role_graphs() -> None:
    baseline = _plan(TriageComparisonArm.BASELINE)
    treatment = _plan(TriageComparisonArm.TREATMENT)

    assert tuple(item.role for item in baseline.role_bindings) == (TriageAgentRole.COORDINATOR,)
    assert tuple(item.role for item in treatment.role_bindings) == (
        TriageAgentRole.COORDINATOR,
        TriageAgentRole.COUNTERCASE_REVIEWER,
        TriageAgentRole.FACT_VERIFIER,
        TriageAgentRole.TRANSMISSION_MAPPER,
    )
    assert treatment.max_child_count == 3
    assert treatment.allowed_tools == ()
    assert treatment.allowed_mcp_servers == ()
    assert event_impact_triage_execution_plan_from_dict(treatment.to_dict()) == treatment
    assert not validate_agent_contract(
        treatment.to_dict(), "event-impact-triage-execution-plan.schema.json"
    )


def test_v2_execution_plan_preserves_v1_and_freezes_typed_role_contracts() -> None:
    v1 = _plan(TriageComparisonArm.TREATMENT)
    v2 = _plan_v2(TriageComparisonArm.TREATMENT)

    assert v1.schema_version == EVENT_IMPACT_TRIAGE_EXECUTION_PLAN_SCHEMA_V1
    assert v2.schema_version == EVENT_IMPACT_TRIAGE_EXECUTION_PLAN_SCHEMA_V2
    assert v1.plan_id != v2.plan_id
    assert all(item.prompt_template_id.endswith("-json-v1") for item in v1.role_bindings)
    assert all(item.prompt_template_id.endswith("-json-v2") for item in v2.role_bindings)
    assert event_impact_triage_execution_plan_from_dict(v1.to_dict()) == v1
    assert event_impact_triage_execution_plan_from_dict(v2.to_dict()) == v2
    assert not validate_agent_contract(
        v2.to_dict(), "event-impact-triage-execution-plan.schema.json"
    )


def test_direct_plan_schema_rejects_role_binding_revision_mismatch() -> None:
    plan = _plan_v2(TriageComparisonArm.TREATMENT)
    payload = plan.to_dict()
    payload["schema_version"] = EVENT_IMPACT_TRIAGE_EXECUTION_PLAN_SCHEMA_V1

    with pytest.raises(ValueError, match="Plan and role binding revisions differ"):
        event_impact_triage_execution_plan_from_dict(payload)
    assert validate_agent_contract(payload, "event-impact-triage-execution-plan.schema.json")


def test_direct_runtime_rechecks_plan_and_role_binding_revision(tmp_path: Path) -> None:
    candidate_set, contents = _candidate_set()
    plan = _plan_v2(TriageComparisonArm.BASELINE)
    object.__setattr__(plan, "schema_version", EVENT_IMPACT_TRIAGE_EXECUTION_PLAN_SCHEMA_V1)

    with pytest.raises(ValueError, match="runtime Plan and role binding revisions differ"):
        EventImpactTriageRunner(
            plan=plan,
            candidate_set=candidate_set,
            registration=_registration(),
            provider=FixtureProvider((_ineligible_draft(candidate_set),)),
            content_resolver=StaticContentResolver(contents),
            skills=SkillRegistry(ROOT / "skills"),
            artifact_store=ArtifactStore(tmp_path / "artifacts"),
            journal=RunJournal(tmp_path / "runs.sqlite3"),
            usage_ledger=UsageLedger(tmp_path / "usage.sqlite3"),
        )


def test_specialist_artifact_schema_accepts_harness_minted_content_ids() -> None:
    candidate_set, _ = _candidate_set()
    finding = TriageSpecialistFinding.build(
        finding_type=TriageFindingType.CHANGED_FACT,
        candidate_version_ids=(candidate_set.version_ids[0],),
        evidence_version_ids=(candidate_set.version_ids[0],),
        statement="The source reports a routine administrative notice.",
    )
    core = {
        "schema_version": "market-impact.event-impact-triage-specialist-artifact.v1",
        "candidate_set_id": candidate_set.candidate_set_id,
        "role": "fact_verifier",
        "covered_candidate_version_ids": list(candidate_set.version_ids),
        "findings": [finding.to_dict()],
    }
    artifact = TriageSpecialistArtifact(
        artifact_id=("event-impact-triage-specialist-artifact-" + canonical_hash(core)),
        candidate_set_id=candidate_set.candidate_set_id,
        role=TriageAgentRole.FACT_VERIFIER,
        covered_candidate_version_ids=candidate_set.version_ids,
        findings=(finding,),
    )
    assert not validate_agent_contract(
        artifact.to_dict(), "event-impact-triage-specialist-artifact.schema.json"
    )


def test_baseline_rejects_position_or_history_context_without_treatment_roles() -> None:
    candidate_set, _ = _candidate_set()
    with pytest.raises(ValueError, match="baseline cannot receive treatment-only context"):
        build_event_impact_triage_execution_plan(
            arm=TriageComparisonArm.BASELINE,
            candidate_set=candidate_set,
            registration=_registration(),
            model_profile_alias=PROFILE_ALIAS,
            model_profile=load_builtin_model_provider_profile(PROFILE_ALIAS),
            skills=SkillRegistry(ROOT / "skills"),
            position_snapshot_id="position-snapshot-" + "b" * 64,
        )


def test_treatment_rejects_untyped_position_or_history_context() -> None:
    candidate_set, _ = _candidate_set()
    with pytest.raises(ValueError, match="typed Position Snapshot"):
        build_event_impact_triage_execution_plan(
            arm=TriageComparisonArm.TREATMENT,
            candidate_set=candidate_set,
            registration=_registration(),
            model_profile_alias=PROFILE_ALIAS,
            model_profile=load_builtin_model_provider_profile(PROFILE_ALIAS),
            skills=SkillRegistry(ROOT / "skills"),
            historical_analogy_pack_id="historical-analogy-pack-" + "c" * 64,
        )


def test_runner_seals_usage_and_reopens_authority_before_triage_admission(
    tmp_path: Path,
) -> None:
    candidate_set, contents = _candidate_set()
    plan = _plan(TriageComparisonArm.BASELINE)
    provider = FixtureProvider((_ineligible_draft(candidate_set),))
    runner = EventImpactTriageRunner(
        plan=plan,
        candidate_set=candidate_set,
        registration=_registration(),
        provider=provider,
        content_resolver=StaticContentResolver(contents),
        skills=SkillRegistry(ROOT / "skills"),
        artifact_store=ArtifactStore(tmp_path / "artifacts"),
        journal=RunJournal(tmp_path / "runs.sqlite3"),
        usage_ledger=UsageLedger(tmp_path / "usage.sqlite3"),
        clock=lambda: datetime(2026, 8, 30, 6, 4, tzinfo=UTC),
    )

    result = asyncio.run(runner.run())

    assert result.status is RunStatus.COMPLETED
    assert result.proposal is not None
    assert result.run_evidence is not None
    assert len(result.members) == 1
    assert len(provider.requests) == 1
    assert runner.authoritative_started_at(result.run_evidence) == datetime(
        2026, 8, 30, 6, 4, tzinfo=UTC
    )
    assert runner.authoritative_finished_at(result.run_evidence) == datetime(
        2026, 8, 30, 6, 4, tzinfo=UTC
    )
    assert runner.authoritative_total_estimated_cost_microusd(result.run_evidence) == sum(
        item.metrics.estimated_cost_microusd for item in result.members
    )
    decision = admit_event_impact_triage(
        candidate_set=candidate_set,
        proposal=result.proposal,
        run_evidence=result.run_evidence,
        run_authority=runner,
        decided_at=datetime(2026, 8, 30, 6, 5, tzinfo=UTC),
    )
    assert decision.status.value == "no_eligible_candidate"
    assert decision.archive_cluster_ids

    reopened = asyncio.run(runner.run())
    assert reopened.status is RunStatus.COMPLETED
    assert len(provider.requests) == 1
    assert len(runner.usage_ledger.records()) == 1


def test_treatment_runs_bounded_specialists_before_coordinator(tmp_path: Path) -> None:
    candidate_set, contents = _candidate_set()
    plan = _plan(TriageComparisonArm.TREATMENT)
    provider = FixtureProvider(
        (
            _specialist_draft(
                candidate_set,
                role=TriageAgentRole.COUNTERCASE_REVIEWER,
                finding_type="countercase",
            ),
            _specialist_draft(
                candidate_set,
                role=TriageAgentRole.FACT_VERIFIER,
                finding_type="changed_fact",
            ),
            _specialist_draft(
                candidate_set,
                role=TriageAgentRole.TRANSMISSION_MAPPER,
                finding_type="transmission_path",
            ),
            _ineligible_draft(candidate_set),
        )
    )
    runner = EventImpactTriageRunner(
        plan=plan,
        candidate_set=candidate_set,
        registration=_registration(),
        provider=provider,
        content_resolver=StaticContentResolver(contents),
        skills=SkillRegistry(ROOT / "skills"),
        artifact_store=ArtifactStore(tmp_path / "artifacts"),
        journal=RunJournal(tmp_path / "runs.sqlite3"),
        usage_ledger=UsageLedger(tmp_path / "usage.sqlite3"),
        clock=lambda: datetime(2026, 8, 30, 6, 4, tzinfo=UTC),
    )

    result = asyncio.run(runner.run())

    assert result.status is RunStatus.COMPLETED
    assert tuple(item.role for item in result.members) == (
        TriageAgentRole.COUNTERCASE_REVIEWER,
        TriageAgentRole.FACT_VERIFIER,
        TriageAgentRole.TRANSMISSION_MAPPER,
        TriageAgentRole.COORDINATOR,
    )
    assert len(runner.usage_ledger.records()) == 4
    coordinator_task = provider.requests[-1][-1]["content"]
    assert isinstance(coordinator_task, str)
    assert "event-impact-triage-specialist-artifact-" in coordinator_task


def test_v2_specialist_prompt_exposes_array_enum_and_evidence_lane_types(
    tmp_path: Path,
) -> None:
    candidate_set, contents = _candidate_set()
    provider = FixtureProvider(
        (
            _specialist_draft(
                candidate_set,
                role=TriageAgentRole.COUNTERCASE_REVIEWER,
                finding_type="countercase",
            ),
            _specialist_draft(
                candidate_set,
                role=TriageAgentRole.FACT_VERIFIER,
                finding_type="changed_fact",
            ),
            _specialist_draft(
                candidate_set,
                role=TriageAgentRole.TRANSMISSION_MAPPER,
                finding_type="transmission_path",
            ),
            _ineligible_draft(candidate_set),
        )
    )
    runner = EventImpactTriageRunner(
        plan=_plan_v2(TriageComparisonArm.TREATMENT),
        candidate_set=candidate_set,
        registration=_registration(),
        provider=provider,
        content_resolver=StaticContentResolver(contents),
        skills=SkillRegistry(ROOT / "skills"),
        artifact_store=ArtifactStore(tmp_path / "artifacts"),
        journal=RunJournal(tmp_path / "runs.sqlite3"),
        usage_ledger=UsageLedger(tmp_path / "usage.sqlite3"),
    )

    result = asyncio.run(runner.run())

    assert result.status is RunStatus.COMPLETED
    task = json.loads(str(provider.requests[0][-1]["content"]))
    finding = task["required_output"]["field_schemas"]["findings"]["items"]
    fields = finding["field_schemas"]
    assert fields["uncertainty_notes"]["type"] == "array"
    assert fields["transmission_channels"]["items"]["enum"] == [
        item.value for item in TransmissionChannel
    ]
    assert fields["evidence_lane"] == {"const": None}


def test_interrupted_inference_requires_review_and_is_never_retried(tmp_path: Path) -> None:
    candidate_set, contents = _candidate_set()
    provider = CrashOnceProvider(_ineligible_draft(candidate_set))
    runner = EventImpactTriageRunner(
        plan=_plan(TriageComparisonArm.BASELINE),
        candidate_set=candidate_set,
        registration=_registration(),
        provider=provider,
        content_resolver=StaticContentResolver(contents),
        skills=SkillRegistry(ROOT / "skills"),
        artifact_store=ArtifactStore(tmp_path / "artifacts"),
        journal=RunJournal(tmp_path / "runs.sqlite3"),
        usage_ledger=UsageLedger(tmp_path / "usage.sqlite3"),
        clock=lambda: datetime(2026, 8, 30, 6, 4, tzinfo=UTC),
    )

    with pytest.raises(SimulatedProcessCrash):
        asyncio.run(runner.run())

    resumed = asyncio.run(runner.run())
    assert resumed.status is RunStatus.HUMAN_INPUT_REQUIRED
    assert len(provider.requests) == 1
    assert len(runner.usage_ledger.records()) == 1


def test_provider_reported_usage_over_budget_cannot_complete(tmp_path: Path) -> None:
    candidate_set, contents = _candidate_set()
    runner = EventImpactTriageRunner(
        plan=_plan(TriageComparisonArm.BASELINE),
        candidate_set=candidate_set,
        registration=_registration(),
        provider=OverBudgetProvider((_ineligible_draft(candidate_set),)),
        content_resolver=StaticContentResolver(contents),
        skills=SkillRegistry(ROOT / "skills"),
        artifact_store=ArtifactStore(tmp_path / "artifacts"),
        journal=RunJournal(tmp_path / "runs.sqlite3"),
        usage_ledger=UsageLedger(tmp_path / "usage.sqlite3"),
    )

    result = asyncio.run(runner.run())

    assert result.status is RunStatus.BUDGET_EXHAUSTED
    assert result.proposal is None
    assert runner.usage_ledger.records()[0].record.status is RunStatus.BUDGET_EXHAUSTED


def test_runner_fails_closed_when_content_hash_does_not_match(tmp_path: Path) -> None:
    candidate_set, contents = _candidate_set()
    altered = (
        TriageCandidateContent(
            version_id=contents[0].version_id,
            normalized_payload={"headline": "altered"},
            license_scope=contents[0].license_scope,
        ),
        contents[1],
    )
    runner = EventImpactTriageRunner(
        plan=_plan(TriageComparisonArm.BASELINE),
        candidate_set=candidate_set,
        registration=_registration(),
        provider=FixtureProvider((_ineligible_draft(candidate_set),)),
        content_resolver=StaticContentResolver(altered),
        skills=SkillRegistry(ROOT / "skills"),
        artifact_store=ArtifactStore(tmp_path / "artifacts"),
        journal=RunJournal(tmp_path / "runs.sqlite3"),
        usage_ledger=UsageLedger(tmp_path / "usage.sqlite3"),
    )

    with pytest.raises(ValueError, match="differs from its frozen hash"):
        asyncio.run(runner.run())


def _run_evidence_for_plan(plan: EventImpactTriageExecutionPlan) -> TriageRunEvidence:
    return TriageRunEvidence(
        members=tuple(
            TriageRunMemberEvidence(
                role=item.role,
                run_id=f"fixture-{item.role.value}",
                terminal_artifact_hash="b" * 64,
                metrics_hash="c" * 64,
                validation_event_hash="d" * 64,
                execution_binding_hash="e" * 64,
            )
            for item in plan.role_bindings
        ),
        usage_ledger_hash="f" * 64,
    )


def _comparison_proposals(
    candidate_set: EventImpactTriageCandidateSet,
) -> tuple[EventImpactTriageProposal, EventImpactTriageProposal]:
    first, second = candidate_set.version_ids
    baseline = EventImpactTriageProposal.build(
        candidate_set=candidate_set,
        clusters=(
            TriageClusterProposal.build(
                candidate_version_ids=(first, second),
                checkpoint_eligibility=CheckpointEligibility.INELIGIBLE,
                recommended_route=TriageRoute.ARCHIVE,
                event_archetypes=(),
                event_stage=EventStage.FIRST_OBSERVED,
                changed_facts=(),
                rule_reasons=("No policy change was identified.",),
                evidence_version_ids=(first, second),
                triage_confidence=0.7,
            ),
        ),
    )
    treatment = EventImpactTriageProposal.build(
        candidate_set=candidate_set,
        clusters=(
            TriageClusterProposal.build(
                candidate_version_ids=(first,),
                checkpoint_eligibility=CheckpointEligibility.INELIGIBLE,
                recommended_route=TriageRoute.ARCHIVE,
                event_archetypes=(),
                event_stage=EventStage.FIRST_OBSERVED,
                changed_facts=(),
                rule_reasons=("The notice is routine and changes no market rule.",),
                evidence_version_ids=(first,),
                triage_confidence=0.9,
            ),
            TriageClusterProposal.build(
                candidate_version_ids=(second,),
                checkpoint_eligibility=CheckpointEligibility.ELIGIBLE,
                recommended_route=TriageRoute.CHECKPOINT_CANDIDATE,
                event_archetypes=(EventArchetype.POLICY_REGULATORY,),
                event_stage=EventStage.FIRST_OBSERVED,
                changed_facts=("A capital-market access rule changed.",),
                rule_reasons=("The event satisfies the frozen policy-change rule.",),
                evidence_version_ids=(second,),
                transmission_channels=(TransmissionChannel.POLICY_ACCESS,),
                affected_entity_refs=("a-share-market",),
                triage_confidence=0.86,
            ),
        ),
    )
    return baseline, treatment


def test_comparison_protocol_scores_arms_but_operator_exposed_batch_cannot_promote(
    tmp_path: Path,
) -> None:
    candidate_set, _ = _candidate_set()
    first, second = candidate_set.version_ids
    labels = EventImpactTriageLabelSet.build(
        candidate_set=candidate_set,
        exposure=TriageLabelExposure.OPERATOR_EXPOSED,
        labels=(
            TriageGoldLabel(
                version_id=first,
                checkpoint_eligibility=CheckpointEligibility.INELIGIBLE,
                expected_route=TriageRoute.ARCHIVE,
                must_catch=False,
                material_transmission_expected=False,
                rationale="Routine notice with no changed market rule.",
            ),
            TriageGoldLabel(
                version_id=second,
                checkpoint_eligibility=CheckpointEligibility.ELIGIBLE,
                expected_route=TriageRoute.CHECKPOINT_CANDIDATE,
                must_catch=True,
                material_transmission_expected=True,
                rationale="Explicit change to capital-market access.",
            ),
        ),
        sealed_at=datetime(2026, 8, 30, 6, 6, tzinfo=UTC),
    )
    baseline_plan = _plan(TriageComparisonArm.BASELINE)
    treatment_plan = _plan(TriageComparisonArm.TREATMENT)
    comparison_store = EventImpactTriageComparisonStore(
        tmp_path / "comparison.sqlite3",
        clock=lambda: datetime(2026, 8, 30, 6, 7, tzinfo=UTC),
    )
    comparison = comparison_store.register(
        candidate_set=candidate_set,
        label_set=labels,
        baseline_plan=baseline_plan,
        treatment_plan=treatment_plan,
    )
    baseline_proposal, treatment_proposal = _comparison_proposals(candidate_set)
    baseline_evidence = _run_evidence_for_plan(baseline_plan)
    treatment_evidence = _run_evidence_for_plan(treatment_plan)
    started_at = datetime(2026, 8, 30, 6, 8, tzinfo=UTC)

    report = evaluate_event_impact_triage_comparison(
        registration=comparison,
        candidate_set=candidate_set,
        label_set=labels,
        baseline=TriageArmOutcome(
            plan=baseline_plan,
            proposal=baseline_proposal,
            run_evidence=baseline_evidence,
            total_estimated_cost_microusd=0,
        ),
        treatment=TriageArmOutcome(
            plan=treatment_plan,
            proposal=treatment_proposal,
            run_evidence=treatment_evidence,
            total_estimated_cost_microusd=0,
        ),
        baseline_authority=FixtureComparisonAuthority(
            candidate_set=candidate_set,
            proposal=baseline_proposal,
            run_evidence=baseline_evidence,
            started_at=started_at,
            finished_at=started_at,
            total_cost=0,
        ),
        treatment_authority=FixtureComparisonAuthority(
            candidate_set=candidate_set,
            proposal=treatment_proposal,
            run_evidence=treatment_evidence,
            started_at=started_at,
            finished_at=started_at,
            total_cost=0,
        ),
        registration_authority=comparison_store,
        evaluated_at=datetime(2026, 8, 30, 6, 9, tzinfo=UTC),
    )

    assert report.batch_gate_passed is True
    assert report.treatment_score.must_catch_false_negatives == 0
    assert report.treatment_score.checkpoint_eligibility_accuracy == 1.0
    assert report.promotion_eligible is False
    assert report.blockers == (
        "operator_exposed_batch_cannot_promote",
        "second_pristine_blind_batch_required",
    )
    assert not validate_agent_contract(
        labels.to_dict(), "event-impact-triage-label-set.schema.json"
    )
    assert event_impact_triage_label_set_from_dict(labels.to_dict()) == labels
    assert not validate_agent_contract(
        comparison.to_dict(), "event-impact-triage-comparison-registration.schema.json"
    )
    assert not validate_agent_contract(
        report.to_dict(), "event-impact-triage-comparison-report.schema.json"
    )


def test_label_set_rejects_partial_candidate_coverage() -> None:
    candidate_set, _ = _candidate_set()
    with pytest.raises(ValueError, match="every and only frozen candidate"):
        EventImpactTriageLabelSet.build(
            candidate_set=candidate_set,
            exposure=TriageLabelExposure.PRISTINE_BLIND,
            labels=(
                TriageGoldLabel(
                    version_id=candidate_set.version_ids[0],
                    checkpoint_eligibility=CheckpointEligibility.INELIGIBLE,
                    expected_route=TriageRoute.ARCHIVE,
                    must_catch=False,
                    material_transmission_expected=False,
                    rationale="Fixture label.",
                ),
            ),
            sealed_at=datetime(2026, 8, 30, 6, 6, tzinfo=UTC),
        )


def test_material_event_must_catch_measures_event_assessment_route() -> None:
    candidate_set, _ = _candidate_set()
    first, second = candidate_set.version_ids
    labels = EventImpactTriageLabelSet.build(
        candidate_set=candidate_set,
        exposure=TriageLabelExposure.PRISTINE_BLIND,
        labels=(
            TriageGoldLabel(
                version_id=first,
                checkpoint_eligibility=CheckpointEligibility.NEEDS_REVIEW,
                expected_route=TriageRoute.EVENT_ASSESSMENT,
                must_catch=True,
                material_transmission_expected=True,
                rationale="The frozen event has a supported transmission path to a target.",
            ),
            TriageGoldLabel(
                version_id=second,
                checkpoint_eligibility=CheckpointEligibility.INELIGIBLE,
                expected_route=TriageRoute.ARCHIVE,
                must_catch=False,
                material_transmission_expected=False,
                rationale="The frozen event has no supported target path.",
            ),
        ),
        sealed_at=datetime(2026, 8, 30, 6, 6, tzinfo=UTC),
    )
    proposal = EventImpactTriageProposal.build(
        candidate_set=candidate_set,
        clusters=(
            TriageClusterProposal.build(
                candidate_version_ids=(first,),
                checkpoint_eligibility=CheckpointEligibility.NEEDS_REVIEW,
                recommended_route=TriageRoute.EVENT_ASSESSMENT,
                event_archetypes=(EventArchetype.ISSUER_CORPORATE,),
                event_stage=EventStage.FIRST_OBSERVED,
                changed_facts=("A frozen issuer fact changed.",),
                rule_reasons=("The evidence supports formal impact assessment.",),
                evidence_version_ids=(first,),
                uncertainty_notes=("Material magnitude remains for the gate.",),
                countercases=("The effect may remain operationally immaterial.",),
                transmission_channels=(TransmissionChannel.REVENUE_DEMAND,),
                affected_entity_refs=("issuer-fixture",),
                triage_confidence=0.8,
            ),
            TriageClusterProposal.build(
                candidate_version_ids=(second,),
                checkpoint_eligibility=CheckpointEligibility.INELIGIBLE,
                recommended_route=TriageRoute.ARCHIVE,
                event_archetypes=(),
                event_stage=EventStage.FIRST_OBSERVED,
                changed_facts=(),
                rule_reasons=("No registered target path is supported.",),
                evidence_version_ids=(second,),
                triage_confidence=0.8,
            ),
        ),
    )

    score = score_event_impact_triage_proposal(
        labels=labels,
        proposal=proposal,
        total_estimated_cost_microusd=0,
    )

    assert score.must_catch_false_negatives == 0


def test_comparison_store_rejects_constructor_bypassed_partial_labels(
    tmp_path: Path,
) -> None:
    candidate_set, _ = _candidate_set()
    label = TriageGoldLabel(
        version_id=candidate_set.version_ids[0],
        checkpoint_eligibility=CheckpointEligibility.INELIGIBLE,
        expected_route=TriageRoute.ARCHIVE,
        must_catch=False,
        material_transmission_expected=False,
        rationale="Fixture label.",
    )
    core = {
        "schema_version": "market-impact.event-impact-triage-label-set.v1",
        "candidate_set_id": candidate_set.candidate_set_id,
        "exposure": "operator_exposed",
        "labels": [label.to_dict()],
        "sealed_at": "2026-08-30T06:06:00Z",
    }
    partial = EventImpactTriageLabelSet(
        label_set_id=f"event-impact-triage-label-set-{canonical_hash(core)}",
        candidate_set_id=candidate_set.candidate_set_id,
        exposure=TriageLabelExposure.OPERATOR_EXPOSED,
        labels=(label,),
        sealed_at=datetime(2026, 8, 30, 6, 6, tzinfo=UTC),
    )
    store = EventImpactTriageComparisonStore(
        tmp_path / "comparison.sqlite3",
        clock=lambda: datetime(2026, 8, 30, 6, 7, tzinfo=UTC),
    )

    with pytest.raises(ValueError, match="every frozen candidate"):
        store.register(
            candidate_set=candidate_set,
            label_set=partial,
            baseline_plan=_plan(TriageComparisonArm.BASELINE),
            treatment_plan=_plan(TriageComparisonArm.TREATMENT),
        )


def test_unregistered_backdated_comparison_is_not_authoritative(tmp_path: Path) -> None:
    candidate_set, _ = _candidate_set()
    labels = EventImpactTriageLabelSet.build(
        candidate_set=candidate_set,
        exposure=TriageLabelExposure.OPERATOR_EXPOSED,
        labels=tuple(
            TriageGoldLabel(
                version_id=version_id,
                checkpoint_eligibility=CheckpointEligibility.INELIGIBLE,
                expected_route=TriageRoute.ARCHIVE,
                must_catch=False,
                material_transmission_expected=False,
                rationale="Fixture label.",
            )
            for version_id in candidate_set.version_ids
        ),
        sealed_at=datetime(2026, 8, 30, 6, 6, tzinfo=UTC),
    )
    forged = EventImpactTriageComparisonRegistration.build(
        candidate_set=candidate_set,
        label_set=labels,
        baseline_plan=_plan(TriageComparisonArm.BASELINE),
        treatment_plan=_plan(TriageComparisonArm.TREATMENT),
        registered_at=datetime(2026, 8, 30, 6, 7, tzinfo=UTC),
    )
    store = EventImpactTriageComparisonStore(tmp_path / "comparison.sqlite3")

    with pytest.raises(ValueError, match="not durably registered"):
        store.assert_authoritative_registration(forged)
