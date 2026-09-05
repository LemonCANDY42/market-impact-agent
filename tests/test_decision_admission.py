from __future__ import annotations

import asyncio
import json
import sqlite3
from collections.abc import Sequence
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from hashlib import sha256
from pathlib import Path
from typing import cast

import pytest

from market_impact_agent.account_state import (
    AccountStateSnapshot,
    CashBalance,
    PositionSnapshot,
    capture_account_state_snapshot,
)
from market_impact_agent.agent_contracts import (
    CandidateDirection,
    CandidateImpact,
    EvidencePack,
    EvidenceReference,
    JudgmentArtifact,
    JudgmentDecision,
    JudgmentProposal,
    ProspectiveEvidenceLineage,
    canonical_hash,
    canonical_json_bytes,
)
from market_impact_agent.agent_engine import (
    AgentEngine,
    AgentExecutionBinding,
    AgentRunRequest,
    AgentRunResult,
    RunMetrics,
)
from market_impact_agent.agent_runtime import (
    ModelTurn,
    ProviderUsage,
    SkillRegistry,
    ToolAccessContext,
    ToolRegistry,
)
from market_impact_agent.agent_schema import validate_agent_contract
from market_impact_agent.authorized_decision_view import AuthorizedDecisionView
from market_impact_agent.checkpoint_decision_inputs import project_checkpoint_observation
from market_impact_agent.checkpoint_market_universe import load_exchange_instrument_rule_set
from market_impact_agent.data_inputs import (
    DataFetchStatus,
    DataPITLane,
    DataProviderAttempt,
    DataQuery,
    DataSnapshot,
    DataSourceBinding,
    LocalDataSnapshotStore,
    SourceObservation,
)
from market_impact_agent.decision_admission import (
    DecisionDisposition,
    PairedDecisionRun,
    build_decision_run_manifest,
    build_signal_from_decision_manifest,
    prepare_decision_admission,
    prepare_portfolio_decision_admission,
)
from market_impact_agent.domain import (
    ApprovalMode,
    OrderIntent,
    OrderKind,
    Side,
    TradingEnvironment,
    TradingMandate,
)
from market_impact_agent.model_provider import load_model_provider_profile
from market_impact_agent.observations import (
    AvailabilityBasis,
    ObservationCapability,
    ObservationTimes,
    OccurrenceBasis,
)
from market_impact_agent.paper_execution import (
    ApprovalState,
    OutboxState,
    PaperExecutionService,
    PriceBasis,
)
from market_impact_agent.portfolio_decision import (
    OrderSizingPolicy,
    PortfolioAction,
    build_order_intent_from_sizing,
    evaluate_portfolio_decision,
    size_portfolio_decision,
)
from market_impact_agent.prospective_checkpoint_sets import (
    CheckpointCapabilityBinding,
    CheckpointRouteReconciliation,
    CheckpointToolManifest,
    ProspectiveCheckpointSnapshotSet,
    build_checkpoint_tool_descriptors,
    materialize_checkpoint_decision_inputs,
)
from market_impact_agent.prospective_decision_pipeline import (
    FrozenProspectiveDecisionRefs,
    ProspectiveDecisionPipeline,
    ProspectiveDecisionPipelineStatus,
    ProspectivePortfolioInstruction,
)
from market_impact_agent.prospective_diagnostic import (
    PROSPECTIVE_DIAGNOSTIC_REGISTRATION_SCHEMA_V3,
    PROSPECTIVE_DIAGNOSTIC_REGISTRATION_SCHEMA_V4,
    REQUIRED_DIAGNOSTIC_CAPABILITIES,
    ProspectiveDiagnosticRegistration,
    load_prospective_diagnostic_registration,
)
from market_impact_agent.prospective_execution import (
    PairedArmExecutionBinding,
    ProspectiveExecutionPlan,
)
from market_impact_agent.prospective_query_gate import (
    ProspectiveQueryGateResult,
    build_query_gate_evaluation_material,
    evaluate_prospective_query_gate,
)
from market_impact_agent.prospective_trigger_admission import (
    ProspectiveTriggerAdmission,
    TriggerAdmissionKind,
)
from market_impact_agent.providers import (
    Capability,
    MockExecutionProvider,
    ProviderManifest,
    ProviderTransport,
    TrustTier,
)
from market_impact_agent.research import EvidenceTier, TransmissionDirectness
from market_impact_agent.runtime_store import ArtifactStore, RunJournal, RunStatus, RuntimeEvent
from market_impact_agent.usage_ledger import UsageLedger

from .runtime_fakes import BusinessModelFixture

NOW = datetime(2026, 8, 30, 8, tzinfo=UTC)
REGISTRATION_PATH = Path("examples/research/prospective-diagnostic-registration-v3.json")
MODEL_PROFILE_PATH = Path("examples/providers/cliproxyapi-luna-xhigh-v1.json")
CPA_MODEL_PROFILE_PATH = Path("examples/providers/cliproxyapi-luna-xhigh-cpa-v1.json")
MINIMAX_PROFILE_PATH = Path("examples/providers/minimax-m3-research-v1.json")


class _DecisionRunFixtureProvider(BusinessModelFixture):
    def __init__(
        self,
        *,
        provider_id: str,
        model: str,
        proposals: Sequence[JudgmentProposal],
    ) -> None:
        self._provider_id = provider_id
        self._model = model
        self._proposals = list(proposals)

    @property
    def provider_id(self) -> str:
        return self._provider_id

    @property
    def model(self) -> str:
        return self._model

    async def answer(
        self,
        *,
        messages: tuple[dict[str, object], ...],
        tools: tuple[dict[str, object], ...],
        temperature: float,
        top_p: float,
        max_output_tokens: int,
        timeout_seconds: float,
    ) -> ModelTurn:
        _ = (messages, tools, temperature, top_p, max_output_tokens, timeout_seconds)
        proposal = self._proposals.pop(0)
        assistant: dict[str, object] = {
            "role": "assistant",
            "content": canonical_json_bytes(proposal.to_dict()).decode(),
        }
        return ModelTurn(
            response_id=f"decision-fixture-{len(self._proposals)}",
            model=self._model,
            assistant_message=assistant,
            tool_calls=(),
            finish_reason="stop",
            usage=ProviderUsage(input_tokens=100, output_tokens=50),
            raw_response={"model": self._model, "message": assistant},
            latency_ms=10,
        )


class _TriggerAuthority:
    def __init__(self, trigger: ProspectiveTriggerAdmission) -> None:
        self.trigger = trigger

    def assert_authoritative(self, admission: ProspectiveTriggerAdmission) -> None:
        if admission != self.trigger:
            raise ValueError("Trigger Admission differs from durable authority")

    def get(self, admission_id: str) -> ProspectiveTriggerAdmission:
        if admission_id != self.trigger.admission_id:
            raise KeyError(admission_id)
        return self.trigger


def _registration():
    return load_prospective_diagnostic_registration(REGISTRATION_PATH)


def _binding(arm: str) -> AgentExecutionBinding:
    common = sha256(b"frozen:common").hexdigest()
    prompt = sha256(f"frozen:prompt:{arm}".encode()).hexdigest()
    routed_method = sha256(b"frozen:routed-method").hexdigest()
    skill_hashes = (
        (common, routed_method) if arm == "structured_agent_plus_routed_methods" else (common,)
    )
    return AgentExecutionBinding(
        runtime_ref="market-impact-agent-runtime-v1",
        runtime_config_hash=common,
        prompt_hash=prompt,
        skill_hashes=skill_hashes,
        tool_manifest_hashes=(common,),
        tool_surface_hash=common,
        mcp_server_hashes=(),
        context_estimator_id="context-estimator-v1",
        compactor_id="compactor-v1",
    )


def _adaptive_registration() -> ProspectiveDiagnosticRegistration:
    registration = _registration()
    return ProspectiveDiagnosticRegistration.build(
        registered_at=registration.registered_at,
        checkpoints=registration.checkpoints,
        paired_arms=registration.paired_arms,
        replicates_per_arm=registration.replicates_per_arm,
        model_profile_id=registration.model_profile_id,
        aggregate_model_cost_limit_usd=registration.aggregate_model_cost_limit_usd,
        outcome_opening_rule=registration.outcome_opening_rule,
        stop_conditions=registration.stop_conditions,
        go_conditions=registration.go_conditions,
        claim_scope=registration.claim_scope,
        minimum_replicates_per_arm=2,
        replicate_schedule_rule=(
            "run_two_paired_replicates_then_third_pair_if_either_arm_disagrees"
        ),
        schema_version=PROSPECTIVE_DIAGNOSTIC_REGISTRATION_SCHEMA_V3,
    )


def _adaptive_v4_registration() -> ProspectiveDiagnosticRegistration:
    registration = _registration()
    return ProspectiveDiagnosticRegistration.build(
        registered_at=registration.registered_at,
        checkpoints=registration.checkpoints,
        paired_arms=registration.paired_arms,
        replicates_per_arm=registration.replicates_per_arm,
        model_profile_id="cliproxyapi-luna-xhigh-cpa-v1",
        aggregate_model_cost_limit_usd=registration.aggregate_model_cost_limit_usd,
        outcome_opening_rule=registration.outcome_opening_rule,
        stop_conditions=registration.stop_conditions,
        go_conditions=registration.go_conditions,
        claim_scope=registration.claim_scope,
        minimum_replicates_per_arm=2,
        replicate_schedule_rule=(
            "run_two_paired_replicates_then_third_pair_if_either_arm_disagrees"
        ),
        schema_version=PROSPECTIVE_DIAGNOSTIC_REGISTRATION_SCHEMA_V4,
    )


def _eligible_trigger(
    registration: ProspectiveDiagnosticRegistration,
) -> ProspectiveTriggerAdmission:
    core = {
        "schema_version": "market-impact.prospective-trigger-admission.v1",
        "kind": TriggerAdmissionKind.CHECKPOINT_ELIGIBLE.value,
        "registration_id": registration.registration_id,
        "checkpoint_key": "next-a-share-policy-event",
        "candidate_set_id": "event-impact-triage-candidate-set-" + "1" * 64,
        "proposal_id": "event-impact-triage-proposal-" + "2" * 64,
        "triage_decision_id": "event-impact-triage-decision-" + "3" * 64,
        "cluster_id": "event-impact-triage-cluster-" + "4" * 64,
        "observation_version_ids": ["prospective-observation-version-" + "5" * 64],
        "event_assessment_id": None,
        "materiality_gate_result_id": None,
        "preceding_materiality_gate_result_ids": [],
        "admitted_target_ids": [],
        "held_target_ids": [],
        "admitted_at": (NOW - timedelta(minutes=11)).isoformat().replace("+00:00", "Z"),
        "historical_pit_claim": False,
        "judgment_model_calls_authorized": False,
        "execution_capability": False,
    }
    return ProspectiveTriggerAdmission(
        admission_id=f"prospective-trigger-admission-{canonical_hash(core)}",
        kind=TriggerAdmissionKind.CHECKPOINT_ELIGIBLE,
        registration_id=registration.registration_id,
        checkpoint_key="next-a-share-policy-event",
        candidate_set_id="event-impact-triage-candidate-set-" + "1" * 64,
        proposal_id="event-impact-triage-proposal-" + "2" * 64,
        triage_decision_id="event-impact-triage-decision-" + "3" * 64,
        cluster_id="event-impact-triage-cluster-" + "4" * 64,
        observation_version_ids=("prospective-observation-version-" + "5" * 64,),
        event_assessment_id=None,
        materiality_gate_result_id=None,
        preceding_materiality_gate_result_ids=(),
        admitted_target_ids=(),
        held_target_ids=(),
        admitted_at=NOW - timedelta(minutes=11),
    )


def _execution_plan(
    registration: ProspectiveDiagnosticRegistration | None = None,
) -> ProspectiveExecutionPlan:
    registration = _registration() if registration is None else registration
    profile = load_model_provider_profile(
        CPA_MODEL_PROFILE_PATH
        if registration.schema_version == PROSPECTIVE_DIAGNOSTIC_REGISTRATION_SCHEMA_V4
        else MODEL_PROFILE_PATH
    )
    return ProspectiveExecutionPlan.build(
        registration=registration,
        model_profile_alias=registration.model_profile_id,
        model_profile=profile,
        arm_bindings=tuple(
            PairedArmExecutionBinding(arm=arm, execution_binding=_binding(arm))
            for arm in registration.paired_arms
        ),
    )


def _decision_proposal(
    pack: EvidencePack,
    *,
    target: str,
    direction: CandidateDirection,
) -> JudgmentProposal:
    return JudgmentProposal(
        event_id=pack.event_id,
        decision=JudgmentDecision.PROPOSE,
        summary="Bounded prospective proposal.",
        transmission_steps=(),
        candidates=(
            CandidateImpact(
                target_id=target,
                direction=direction,
                horizon_sessions=5,
                directness=TransmissionDirectness.DIRECT,
                confidence=0.7,
                thesis="Observed event may affect the target.",
                evidence_refs=("event",),
                counterevidence_refs=(),
                invalidation_conditions=("event is retracted",),
            ),
        ),
        blockers=(),
        unresolved_questions=("effect magnitude",),
        stopped_reason="registered checks completed",
        decision_confidence=0.7,
    )


def _write_decision_skill(
    root: Path,
    *,
    name: str,
    instructions: str,
    allowed_tools: tuple[str, ...] = (),
) -> None:
    directory = root / name
    directory.mkdir(parents=True)
    instructions_path = directory / "SKILL.md"
    instructions_path.write_text(instructions, encoding="utf-8")
    manifest: dict[str, object] = {
        "schema_version": "market-impact.skill-manifest.v1",
        "name": name,
        "version": "1.0.0",
        "description": f"Decision admission fixture Skill {name}.",
        "source": f"repo://tests/{name}",
        "instructions_path": "SKILL.md",
        "instructions_hash": sha256(instructions.encode()).hexdigest(),
        "required_capabilities": [],
        "dependencies": [],
        "conflicts": [],
        "allowed_tools": list(allowed_tools),
        "allowed_mcp_servers": [],
    }
    manifest["manifest_hash"] = canonical_hash(manifest)
    (directory / "skill.json").write_text(json.dumps(manifest), encoding="utf-8")


def _authoritative_run_fixture(
    root: Path,
    pack: EvidencePack,
    *,
    registration: ProspectiveDiagnosticRegistration | None = None,
) -> tuple[
    dict[str, tuple[AgentRunResult, ...]],
    ProspectiveExecutionPlan,
    dict[str, AgentEngine],
]:
    registration = _registration() if registration is None else registration
    profile = load_model_provider_profile(
        CPA_MODEL_PROFILE_PATH
        if registration.schema_version == PROSPECTIVE_DIAGNOSTIC_REGISTRATION_SCHEMA_V4
        else MODEL_PROFILE_PATH
    )
    votes = {
        registration.paired_arms[0]: (
            ("510500.XSHG", CandidateDirection.DOWN),
            ("510300.XSHG", CandidateDirection.DOWN),
            ("510500.XSHG", CandidateDirection.UP),
        ),
        registration.paired_arms[1]: (
            ("510300.XSHG", CandidateDirection.UP),
            ("510300.XSHG", CandidateDirection.UP),
            ("510500.XSHG", CandidateDirection.DOWN),
        ),
    }
    engines: dict[str, AgentEngine] = {}
    bindings: list[PairedArmExecutionBinding] = []
    requests: dict[str, tuple[AgentRunRequest, ...]] = {}
    for arm in registration.paired_arms:
        arm_root = root / arm
        skill_root = arm_root / "skills"
        _write_decision_skill(
            skill_root,
            name="decision-core",
            instructions="Assess the frozen evidence and abstain when it is insufficient.",
        )
        _write_decision_skill(
            skill_root,
            name="routed-methods",
            instructions="Apply the registered routed research method to the same evidence.",
        )
        store = ArtifactStore(arm_root / "artifacts")
        provider = _DecisionRunFixtureProvider(
            provider_id=profile.provider_id,
            model=profile.model,
            proposals=tuple(
                _decision_proposal(pack, target=target, direction=direction)
                for target, direction in votes[arm]
            ),
        )
        engine = AgentEngine(
            provider=provider,
            config=profile.runtime_config(),
            artifact_store=store,
            journal=RunJournal(arm_root / "run.sqlite3"),
            tool_registry=ToolRegistry(store),
            skill_registry=SkillRegistry(skill_root),
            clock=lambda: NOW - timedelta(minutes=7),
        )
        selected_skills = (
            ("decision-core", "routed-methods")
            if arm == registration.paired_arms[1]
            else ("decision-core",)
        )
        arm_requests = tuple(
            AgentRunRequest(
                run_id=f"{arm}-{index}",
                evidence_pack=pack,
                research_instruction="Assess this prospective checkpoint.",
                selected_skills=selected_skills,
                tool_access=ToolAccessContext(
                    allowed_capabilities=frozenset(),
                    allowed_side_effects=frozenset(),
                    allowed_tools=frozenset(),
                ),
            )
            for index in range(1, 4)
        )
        binding = engine.execution_binding(
            arm_requests[0],
            runtime_ref="market-impact-agent-runtime-v1",
        )
        engines[arm] = engine
        requests[arm] = arm_requests
        bindings.append(PairedArmExecutionBinding(arm=arm, execution_binding=binding))
    plan = ProspectiveExecutionPlan.build(
        registration=registration,
        model_profile_alias=registration.model_profile_id,
        model_profile=profile,
        arm_bindings=tuple(bindings),
    )

    async def run_all() -> dict[str, tuple[AgentRunResult, ...]]:
        results: dict[str, tuple[AgentRunResult, ...]] = {}
        for arm in registration.paired_arms:
            results[arm] = tuple([await engines[arm].run(request) for request in requests[arm]])
        return results

    runs = asyncio.run(run_all())
    authorities = {
        plan.arm_binding(arm).binding_hash: engines[arm] for arm in registration.paired_arms
    }
    return runs, plan, authorities


def _evaluated_gate_fixture(
    root: Path,
    *,
    execution_plan: ProspectiveExecutionPlan | None = None,
    registration: ProspectiveDiagnosticRegistration | None = None,
    trigger_admission: ProspectiveTriggerAdmission | None = None,
    trigger_authority: _TriggerAuthority | None = None,
) -> tuple[
    EvidencePack,
    ProspectiveQueryGateResult,
    ProspectiveCheckpointSnapshotSet,
    tuple[dict[str, object], ...],
    LocalDataSnapshotStore,
]:
    registration = _registration() if registration is None else registration
    checkpoint = registration.checkpoint("next-a-share-policy-event")
    barrier_at = NOW - timedelta(minutes=10)
    received_at = barrier_at
    digest = sha256(b"evaluated-gate-fixture").hexdigest()
    source = DataSourceBinding(
        provider_id="official-fixture",
        provider_version="1",
        upstream_source="official-event",
        manifest_hash=digest,
        source_config_hash=digest,
        required=True,
    )
    query = DataQuery.build(
        capability=ObservationCapability.EVENT_REVELATION,
        pit_lane=DataPITLane.PROSPECTIVE,
        as_of=barrier_at,
        window_start=barrier_at - timedelta(hours=1),
        source_policy_id="decision-admission-fixture",
        parameters={"checkpoint": checkpoint.checkpoint_key},
        sources=(source,),
        minimum_data_sources=1,
    )
    observation = SourceObservation.build(
        capability=ObservationCapability.EVENT_REVELATION,
        provider_id=source.provider_id,
        provider_version=source.provider_version,
        upstream_source=source.upstream_source,
        upstream_record_id="event-1",
        source_ref="prospective://official/event-1",
        lineage_id="official-event-1",
        times=ObservationTimes(
            occurred_at=received_at,
            published_at=received_at,
            available_at=received_at,
            source_updated_at=received_at,
            aggregator_fetched_at=None,
            retrieved_at=received_at,
            occurrence_basis=OccurrenceBasis.SOURCE_REPORTED,
            availability_basis=AvailabilityBasis.ACTUAL_RECEIPT,
        ),
        authority_at=received_at,
        authority_kind="actual_receipt",
        raw_content_hash=sha256(b"event").hexdigest(),
        normalized_payload={"headline": "Policy event", "publisher": "Official"},
        license_scope="private_research",
    )
    attempt = DataProviderAttempt(
        provider_id=source.provider_id,
        provider_version=source.provider_version,
        upstream_source=source.upstream_source,
        required=True,
        status=DataFetchStatus.DATA,
        retrieved_at=received_at,
        raw_response_hash=digest,
        received_count=1,
        accepted_count=1,
        rejected_missing_availability=0,
        rejected_after_cutoff=0,
        rejected_missing_authority=0,
        rejected_authority_after_cutoff=0,
        rejected_lane_mismatch=0,
        error_kind=None,
    )
    snapshot_core = {
        "schema_version": "market-impact.data-snapshot.v2",
        "query": query.to_dict(),
        "attempts": [attempt.to_dict()],
        "observations": [observation.to_dict()],
        "coverage_complete": True,
        "completed_at": received_at.isoformat().replace("+00:00", "Z"),
    }
    snapshot = DataSnapshot(
        snapshot_id=f"data-snapshot-{canonical_hash(snapshot_core)}",
        query=query,
        attempts=(attempt,),
        observations=(observation,),
        coverage_complete=True,
        completed_at=received_at,
    )
    store = LocalDataSnapshotStore(root / "query-gate-snapshots")
    store.put(snapshot)
    event_slot = checkpoint.slot(ObservationCapability.EVENT_REVELATION)
    route = CheckpointRouteReconciliation(
        route_kind=event_slot.required_route_kinds[0],
        snapshot_id=snapshot.snapshot_id,
        collection_policy_id="prospective-collection-policy-" + "1" * 64,
        source_acceptance_report_id="source-route-acceptance-report-" + "2" * 64,
        provider_id=source.provider_id,
        provider_version=source.provider_version,
        upstream_source=source.upstream_source,
        provider_manifest_hash=source.manifest_hash,
        source_config_hash=digest,
        raw_response_hash=digest,
        observation_ids=(observation.observation_id,),
    )
    capability_gaps = tuple(
        sorted(
            f"{capability.value}:missing_registered_routes"
            for capability in REQUIRED_DIAGNOSTIC_CAPABILITIES
            if capability is not ObservationCapability.EVENT_REVELATION
        )
    )
    bindings = tuple(
        CheckpointCapabilityBinding(
            capability=capability,
            applicability=checkpoint.slot(capability).applicability,
            not_applicable_reason=checkpoint.slot(capability).not_applicable_reason,
            routes=(route,) if capability is ObservationCapability.EVENT_REVELATION else (),
            tool_manifest=(
                CheckpointToolManifest(
                    name="lookup_event_revelation",
                    version="2",
                    snapshot_ids=(snapshot.snapshot_id,),
                    allowed_filter_fields=("headline", "publisher"),
                )
                if capability is ObservationCapability.EVENT_REVELATION
                else None
            ),
        )
        for capability in sorted(REQUIRED_DIAGNOSTIC_CAPABILITIES, key=lambda item: item.value)
    )
    snapshot_set_schema = (
        "market-impact.prospective-checkpoint-snapshot-set.v5"
        if registration.schema_version == PROSPECTIVE_DIAGNOSTIC_REGISTRATION_SCHEMA_V4
        else "market-impact.prospective-checkpoint-snapshot-set.v4"
    )
    snapshot_set_core = {
        "schema_version": snapshot_set_schema,
        "registration_id": registration.registration_id,
        "checkpoint_key": checkpoint.checkpoint_key,
        "barrier_at": barrier_at.isoformat().replace("+00:00", "Z"),
        "reconciled_at": (barrier_at + timedelta(minutes=1)).isoformat().replace("+00:00", "Z"),
        "capability_bindings": [item.to_dict() for item in bindings],
        "authorized_snapshot_ids": [snapshot.snapshot_id],
        "complete": False,
        "historical_pit_claim": False,
        "execution_capability": False,
        "capability_gaps": list(capability_gaps),
    }
    if trigger_admission is not None:
        snapshot_set_core["trigger_admission_id"] = trigger_admission.admission_id
    snapshot_set = ProspectiveCheckpointSnapshotSet(
        snapshot_set_id=(
            f"prospective-checkpoint-snapshot-set-{canonical_hash(snapshot_set_core)}"
        ),
        registration_id=registration.registration_id,
        checkpoint_key=checkpoint.checkpoint_key,
        barrier_at=barrier_at,
        reconciled_at=barrier_at + timedelta(minutes=1),
        capability_bindings=bindings,
        authorized_snapshot_ids=(snapshot.snapshot_id,),
        complete=False,
        capability_gaps=capability_gaps,
        trigger_admission_id=(
            None if trigger_admission is None else trigger_admission.admission_id
        ),
        schema_version=snapshot_set_schema,
    )
    decision_input = project_checkpoint_observation(
        checkpoint_snapshot_set_id=snapshot_set.snapshot_set_id,
        checkpoint_key=snapshot_set.checkpoint_key,
        barrier_at=barrier_at,
        snapshot_id=snapshot.snapshot_id,
        route_kinds=(route.route_kind,),
        observation=observation,
    )
    evidence_pack = EvidencePack.build(
        event_id="prospective-policy-event",
        as_of=barrier_at,
        research_question="Which observed target could be affected?",
        evidence=(
            EvidenceReference(
                evidence_id="event",
                claim_id="event-fact",
                source_ref=observation.source_ref,
                source_tier=EvidenceTier.OFFICIAL,
                available_at=received_at,
                content_hash=observation.raw_content_hash,
                summary="A prospectively received event.",
                prospective_lineage=ProspectiveEvidenceLineage(
                    snapshot_id=snapshot.snapshot_id,
                    observation_id=observation.observation_id,
                    checkpoint_decision_input_id=cast(str, decision_input["record_id"]),
                ),
            ),
        ),
        pattern_packs=(),
        allowed_targets=("510300.XSHG", "510500.XSHG"),
    )
    plan = _execution_plan(registration) if execution_plan is None else execution_plan
    gate = evaluate_prospective_query_gate(
        registration=registration,
        snapshot_set=snapshot_set,
        evidence_pack=evidence_pack,
        decision_inputs=(decision_input,),
        snapshot_store=store,
        execution_plan=plan,
        model_profile_id=registration.model_profile_id,
        model_cost_limit_usd=Decimal("5.00"),
        evaluated_at=barrier_at + timedelta(minutes=2),
        trigger_admission=trigger_admission,
        trigger_admission_authority=trigger_authority,
    )
    return evidence_pack, gate, snapshot_set, (decision_input,), store


def _pipeline_runtime_fixture(
    root: Path,
    *,
    registration: ProspectiveDiagnosticRegistration,
    evidence_pack: EvidencePack,
    snapshot_set: ProspectiveCheckpointSnapshotSet,
    snapshot_store: LocalDataSnapshotStore,
) -> tuple[dict[str, AgentEngine], ProspectiveExecutionPlan, dict[str, tuple[str, ...]]]:
    profile = load_model_provider_profile(CPA_MODEL_PROFILE_PATH)
    decision_inputs = materialize_checkpoint_decision_inputs(
        snapshot_set,
        store=snapshot_store,
    )
    descriptors = build_checkpoint_tool_descriptors(
        snapshot_set,
        store=snapshot_store,
        frozen_input=snapshot_set.frozen_input,
        authorized_decision_input_ids=frozenset(
            cast(str, item["record_id"]) for item in decision_inputs
        ),
        required_capability="market.read",
    )
    access = ToolAccessContext(
        allowed_capabilities=frozenset({"market.read"}),
        allowed_side_effects=frozenset(item.side_effect for item in descriptors),
        allowed_tools=frozenset(item.name for item in descriptors),
    )
    engines: dict[str, AgentEngine] = {}
    bindings: list[PairedArmExecutionBinding] = []
    selected_skills: dict[str, tuple[str, ...]] = {}
    for arm in registration.paired_arms:
        arm_root = root / arm
        skill_root = arm_root / "skills"
        _write_decision_skill(
            skill_root,
            name="decision-core",
            instructions="Assess the frozen evidence and abstain when it is insufficient.",
            allowed_tools=tuple(item.name for item in descriptors),
        )
        _write_decision_skill(
            skill_root,
            name="routed-methods",
            instructions="Apply the registered routed research method to the same evidence.",
        )
        proposals = (
            _decision_proposal(
                evidence_pack,
                target=("510300.XSHG" if "plus" in arm else "510500.XSHG"),
                direction=CandidateDirection.UP,
            ),
            _decision_proposal(
                evidence_pack,
                target=("510300.XSHG" if "plus" in arm else "510500.XSHG"),
                direction=CandidateDirection.UP,
            ),
        )
        artifact_store = ArtifactStore(arm_root / "artifacts")
        tool_registry = ToolRegistry(artifact_store)
        for descriptor in descriptors:
            tool_registry.register(descriptor)
        engine = AgentEngine(
            provider=_DecisionRunFixtureProvider(
                provider_id=profile.provider_id,
                model=profile.model,
                proposals=proposals,
            ),
            config=profile.runtime_config(),
            artifact_store=artifact_store,
            journal=RunJournal(arm_root / "run.sqlite3"),
            tool_registry=tool_registry,
            skill_registry=SkillRegistry(skill_root),
            clock=lambda: NOW - timedelta(minutes=5),
        )
        skills = (
            ("decision-core", "routed-methods")
            if arm == registration.paired_arms[1]
            else ("decision-core",)
        )
        request = AgentRunRequest(
            run_id="binding-fixture",
            evidence_pack=evidence_pack,
            research_instruction="Assess this prospective checkpoint.",
            selected_skills=skills,
            tool_access=access,
        )
        engines[arm] = engine
        selected_skills[arm] = skills
        bindings.append(
            PairedArmExecutionBinding(
                arm=arm,
                execution_binding=engine.execution_binding(
                    request,
                    runtime_ref="market-impact-agent-runtime-v1",
                ),
            )
        )
    plan = ProspectiveExecutionPlan.build(
        registration=registration,
        model_profile_alias=registration.model_profile_id,
        model_profile=profile,
        arm_bindings=tuple(bindings),
    )
    return engines, plan, selected_skills


def _pack() -> EvidencePack:
    return EvidencePack.build(
        event_id="prospective-policy-event",
        as_of=NOW - timedelta(minutes=10),
        research_question="Which observed target could be affected?",
        evidence=(
            EvidenceReference(
                evidence_id="event",
                claim_id="event-fact",
                source_ref="prospective://event",
                source_tier=EvidenceTier.OFFICIAL,
                available_at=NOW - timedelta(minutes=20),
                content_hash=sha256(b"event").hexdigest(),
                summary="A prospectively received event.",
            ),
        ),
        pattern_packs=(),
        allowed_targets=("510300.XSHG", "510500.XSHG"),
    )


def _gate(
    pack: EvidencePack,
    plan: ProspectiveExecutionPlan,
    registration: ProspectiveDiagnosticRegistration | None = None,
) -> ProspectiveQueryGateResult:
    registration = _registration() if registration is None else registration
    core = {
        "schema_version": "market-impact.prospective-query-gate-result.v4",
        "registration_id": registration.registration_id,
        "checkpoint_key": "next-a-share-policy-event",
        "checkpoint_snapshot_set_id": "prospective-checkpoint-snapshot-set-" + "1" * 64,
        "evidence_pack_id": pack.pack_id,
        "evaluation_material_hash": "4" * 64,
        "agent_execution_plan_id": plan.plan_id,
        "agent_execution_plan_hash": canonical_hash(plan.to_dict()),
        "model_profile_id": registration.model_profile_id,
        "model_cost_limit_usd": "5.00",
        "barrier_at": (NOW - timedelta(minutes=10)).isoformat().replace("+00:00", "Z"),
        "evaluated_at": (NOW - timedelta(minutes=9)).isoformat().replace("+00:00", "Z"),
        "authorized_snapshot_ids": ["data-snapshot-" + "2" * 64],
        "authorized_decision_input_ids": ["checkpoint-decision-input-" + "3" * 64],
        "blocking_required_gaps": [],
        "nonblocking_information_gaps": ["positioning:unavailable"],
        "model_run_eligible": True,
        "claim_scope": "process_diagnostic_only_no_alpha_or_execution_claim",
        "historical_pit_claim": False,
        "strategy_promotion_claim": False,
        "execution_capability": False,
    }
    return ProspectiveQueryGateResult(
        result_id=f"prospective-query-gate-{canonical_hash(core)}",
        registration_id=registration.registration_id,
        checkpoint_key="next-a-share-policy-event",
        checkpoint_snapshot_set_id="prospective-checkpoint-snapshot-set-" + "1" * 64,
        evidence_pack_id=pack.pack_id,
        evaluation_material_hash="4" * 64,
        agent_execution_plan_id=plan.plan_id,
        agent_execution_plan_hash=canonical_hash(plan.to_dict()),
        model_profile_id=registration.model_profile_id,
        model_cost_limit_usd="5.00",
        barrier_at=NOW - timedelta(minutes=10),
        evaluated_at=NOW - timedelta(minutes=9),
        authorized_snapshot_ids=("data-snapshot-" + "2" * 64,),
        authorized_decision_input_ids=("checkpoint-decision-input-" + "3" * 64,),
        blocking_required_gaps=(),
        nonblocking_information_gaps=("positioning:unavailable",),
        model_run_eligible=True,
    )


def _artifact(
    run_id: str,
    *,
    arm: str,
    target: str,
    direction: CandidateDirection,
    started_at: datetime = NOW - timedelta(minutes=8),
    pack: EvidencePack | None = None,
    journal_hash: str | None = None,
) -> JudgmentArtifact:
    bound_pack = _pack() if pack is None else pack
    binding = _binding(arm)
    profile = load_model_provider_profile(MODEL_PROFILE_PATH)
    proposal = JudgmentProposal(
        event_id=bound_pack.event_id,
        decision=JudgmentDecision.PROPOSE,
        summary="Bounded prospective proposal.",
        transmission_steps=(),
        candidates=(
            CandidateImpact(
                target_id=target,
                direction=direction,
                horizon_sessions=5,
                directness=TransmissionDirectness.DIRECT,
                confidence=0.7,
                thesis="Observed event may affect the target.",
                evidence_refs=("event",),
                counterevidence_refs=(),
                invalidation_conditions=("event is retracted",),
            ),
        ),
        blockers=(),
        unresolved_questions=("effect magnitude",),
        stopped_reason="registered checks completed",
        decision_confidence=0.7,
    )
    return JudgmentArtifact.build(
        run_id=run_id,
        evidence_pack_id=bound_pack.pack_id,
        provider_id=profile.provider_id,
        model=profile.model,
        runtime_config_hash=binding.runtime_config_hash,
        prompt_hash=binding.prompt_hash,
        skill_hashes=binding.skill_hashes,
        tool_manifest_hashes=binding.tool_manifest_hashes,
        tool_surface_hash=binding.tool_surface_hash,
        mcp_server_hashes=binding.mcp_server_hashes,
        context_estimator_id=binding.context_estimator_id,
        compactor_id=binding.compactor_id,
        journal_hash=(
            sha256(f"journal:{run_id}".encode()).hexdigest()
            if journal_hash is None
            else journal_hash
        ),
        transcript_hash=sha256(f"transcript:{run_id}".encode()).hexdigest(),
        raw_response_hash=sha256(f"response:{run_id}".encode()).hexdigest(),
        started_at=started_at,
        finished_at=started_at + timedelta(seconds=30),
        proposal=proposal,
    )


def _result(
    arm: str,
    index: int,
    target: str,
    direction: CandidateDirection,
    *,
    started_at: datetime = NOW - timedelta(minutes=8),
    pack: EvidencePack | None = None,
) -> AgentRunResult:
    provisional = _artifact(
        f"{arm}-{index}",
        arm=arm,
        target=target,
        direction=direction,
        started_at=started_at,
        pack=pack,
    )
    metrics = RunMetrics(
        turns=1,
        tool_calls=1,
        input_tokens=100,
        output_tokens=50,
        result_bytes=1000,
        latency_ms=10,
        provider_attempts=1,
        estimated_cost_microusd=100_000,
    )
    validation_payload: dict[str, object] = {
        "proposal_hash": canonical_hash(provisional.proposal.to_dict()),
        "transcript_hash": provisional.transcript_hash,
        "metrics_hash": canonical_hash(metrics.to_dict()),
        "metrics": metrics.to_dict(),
    }
    payload_hash = sha256(canonical_json_bytes(validation_payload)).hexdigest()
    validation_event_core = {
        "run_id": provisional.run_id,
        "event_id": f"{provisional.run_id}.proposal.validated",
        "event_type": "judgment.validated",
        "observed_at": (provisional.started_at + timedelta(seconds=20))
        .isoformat()
        .replace("+00:00", "Z"),
        "payload_hash": payload_hash,
        "previous_hash": sha256(f"prior:{provisional.run_id}".encode()).hexdigest(),
    }
    validation_event = RuntimeEvent(
        sequence=2,
        run_id=provisional.run_id,
        event_id=f"{provisional.run_id}.proposal.validated",
        event_type="judgment.validated",
        observed_at=provisional.started_at + timedelta(seconds=20),
        payload=validation_payload,
        payload_hash=payload_hash,
        previous_hash=validation_event_core["previous_hash"],
        event_hash=sha256(canonical_json_bytes(validation_event_core)).hexdigest(),
    )
    artifact = _artifact(
        f"{arm}-{index}",
        arm=arm,
        target=target,
        direction=direction,
        started_at=started_at,
        pack=pack,
        journal_hash=validation_event.event_hash,
    )
    return AgentRunResult(
        run_id=artifact.run_id,
        status=RunStatus.COMPLETED,
        judgment=artifact,
        terminal_store_hash=canonical_hash(artifact.to_dict()),
        metrics=metrics,
        metrics_hash=canonical_hash(metrics.to_dict()),
        validation_event=validation_event,
    )


def _runs(pack: EvidencePack | None = None) -> dict[str, tuple[AgentRunResult, ...]]:
    control = "structured_agent_core"
    treatment = "structured_agent_plus_routed_methods"
    return {
        control: (
            _result(control, 1, "510500.XSHG", CandidateDirection.DOWN, pack=pack),
            _result(control, 2, "510300.XSHG", CandidateDirection.DOWN, pack=pack),
            _result(control, 3, "510500.XSHG", CandidateDirection.UP, pack=pack),
        ),
        treatment: (
            _result(treatment, 1, "510300.XSHG", CandidateDirection.UP, pack=pack),
            _result(treatment, 2, "510300.XSHG", CandidateDirection.UP, pack=pack),
            _result(treatment, 3, "510500.XSHG", CandidateDirection.DOWN, pack=pack),
        ),
    }


def _paired_runs(
    runs: dict[str, tuple[AgentRunResult, ...]] | None = None,
    *,
    registration: ProspectiveDiagnosticRegistration | None = None,
    replicates: int = 3,
) -> tuple[PairedDecisionRun, ...]:
    selected = _runs() if runs is None else runs
    registration = _registration() if registration is None else registration
    return tuple(
        PairedDecisionRun(arm=arm, replicate_index=index, result=result)
        for arm in registration.paired_arms
        for index, result in enumerate(selected[arm][:replicates], start=1)
    )


def test_query_gate_cannot_predate_snapshot_set_reconciliation(tmp_path: Path) -> None:
    pack, _gate_result, snapshot_set, decision_inputs, store = _evaluated_gate_fixture(tmp_path)
    registration = _registration()
    with pytest.raises(ValueError, match="cannot predate Snapshot Set reconciliation"):
        evaluate_prospective_query_gate(
            registration=registration,
            snapshot_set=snapshot_set,
            evidence_pack=pack,
            decision_inputs=decision_inputs,
            snapshot_store=store,
            execution_plan=_execution_plan(),
            model_profile_id=registration.model_profile_id,
            model_cost_limit_usd=Decimal("5.00"),
            evaluated_at=snapshot_set.reconciled_at - timedelta(seconds=1),
        )


def test_treatment_two_of_three_proposes_without_control_agreement() -> None:
    pack = _pack()
    plan = _execution_plan()
    registration = _registration()
    with pytest.raises(ValueError, match="Harness-bundled alias"):
        ProspectiveExecutionPlan.build(
            registration=registration,
            model_profile_alias=registration.model_profile_id,
            model_profile=load_model_provider_profile(MINIMAX_PROFILE_PATH),
            arm_bindings=plan.arm_bindings,
        )
    with pytest.raises(ValueError, match="preserve the core surface"):
        ProspectiveExecutionPlan.build(
            registration=registration,
            model_profile_alias=registration.model_profile_id,
            model_profile=load_model_provider_profile(MODEL_PROFILE_PATH),
            arm_bindings=(
                PairedArmExecutionBinding(
                    arm=registration.paired_arms[0],
                    execution_binding=plan.arm_bindings[1].execution_binding,
                ),
                PairedArmExecutionBinding(
                    arm=registration.paired_arms[1],
                    execution_binding=plan.arm_bindings[0].execution_binding,
                ),
            ),
        )
    assert validate_agent_contract(plan.to_dict(), "prospective-execution-plan.schema.json") == ()
    gate = _gate(pack, plan)
    manifest = build_decision_run_manifest(
        registration=_registration(),
        query_gate=gate,
        evidence_pack=pack,
        execution_plan=plan,
        paired_runs=_paired_runs(),
        created_at=NOW - timedelta(minutes=6),
    )

    assert manifest.disposition is DecisionDisposition.PROPOSE
    assert manifest.agreement_target_id == "510300.XSHG"
    assert manifest.agreement_direction is CandidateDirection.UP
    assert manifest.agreement_count == 2
    assert len(manifest.agreeing_judgment_artifact_ids) == 2
    assert validate_agent_contract(manifest.to_dict(), "decision-run-manifest.schema.json") == ()
    with pytest.raises(ValueError, match="arm roles are not canonical"):
        replace(
            manifest,
            control_arm=manifest.treatment_arm,
            treatment_arm=manifest.control_arm,
        )


def test_adaptive_pair_stops_after_two_only_when_both_arms_agree() -> None:
    registration = _adaptive_registration()
    pack = _pack()
    plan = _execution_plan(registration)
    gate = _gate(pack, plan, registration)
    runs = _runs(pack)
    control = registration.paired_arms[0]
    runs[control] = (
        _result(control, 1, "510500.XSHG", CandidateDirection.DOWN, pack=pack),
        _result(control, 2, "510500.XSHG", CandidateDirection.DOWN, pack=pack),
        _result(control, 3, "510300.XSHG", CandidateDirection.UP, pack=pack),
    )

    manifest = build_decision_run_manifest(
        registration=registration,
        query_gate=gate,
        evidence_pack=pack,
        execution_plan=plan,
        paired_runs=_paired_runs(runs, registration=registration, replicates=2),
        created_at=NOW - timedelta(minutes=6),
    )

    assert manifest.schema_version == "market-impact.decision-run-manifest.v2"
    assert manifest.replicates_executed_per_arm == 2
    assert manifest.replicate_stop_reason == "first_two_agree_in_both_arms"
    assert manifest.disposition is DecisionDisposition.PROPOSE
    assert manifest.agreement_count == 2
    confidence = manifest.to_dict()["confidence_observation"]
    assert isinstance(confidence, dict)
    assert confidence["reported_count"] == 4
    assert confidence["missing_count"] == 0
    assert confidence["overall_mean_confidence_by_arm"] == {
        "structured_agent_core": 0.7,
        "structured_agent_plus_routed_methods": 0.7,
    }
    assert confidence["third_pair_confidence_by_arm"] == {
        "structured_agent_core": None,
        "structured_agent_plus_routed_methods": None,
    }
    assert validate_agent_contract(manifest.to_dict(), "decision-run-manifest.schema.json") == ()

    with pytest.raises(ValueError, match="unnecessary third pair"):
        build_decision_run_manifest(
            registration=registration,
            query_gate=gate,
            evidence_pack=pack,
            execution_plan=plan,
            paired_runs=_paired_runs(runs, registration=registration),
            created_at=NOW - timedelta(minutes=6),
        )


def test_v4_uses_the_same_adaptive_two_then_optional_third_pair_schedule() -> None:
    registration = _adaptive_v4_registration()
    pack = _pack()
    plan = _execution_plan(registration)
    gate = _gate(pack, plan, registration)
    runs = _runs(pack)
    control = registration.paired_arms[0]
    runs[control] = (
        _result(control, 1, "510500.XSHG", CandidateDirection.DOWN, pack=pack),
        _result(control, 2, "510500.XSHG", CandidateDirection.DOWN, pack=pack),
        _result(control, 3, "510300.XSHG", CandidateDirection.UP, pack=pack),
    )

    manifest = build_decision_run_manifest(
        registration=registration,
        query_gate=gate,
        evidence_pack=pack,
        execution_plan=plan,
        paired_runs=_paired_runs(runs, registration=registration, replicates=2),
        created_at=NOW - timedelta(minutes=6),
    )

    assert manifest.replicates_executed_per_arm == 2
    assert manifest.replicate_stop_reason == "first_two_agree_in_both_arms"


def test_adaptive_pair_requires_third_pair_after_either_arm_disagrees() -> None:
    registration = _adaptive_registration()
    pack = _pack()
    plan = _execution_plan(registration)
    gate = _gate(pack, plan, registration)
    runs = _runs(pack)

    with pytest.raises(ValueError, match="third pair after disagreement"):
        build_decision_run_manifest(
            registration=registration,
            query_gate=gate,
            evidence_pack=pack,
            execution_plan=plan,
            paired_runs=_paired_runs(runs, registration=registration, replicates=2),
            created_at=NOW - timedelta(minutes=6),
        )

    manifest = build_decision_run_manifest(
        registration=registration,
        query_gate=gate,
        evidence_pack=pack,
        execution_plan=plan,
        paired_runs=_paired_runs(runs, registration=registration),
        created_at=NOW - timedelta(minutes=6),
    )
    assert manifest.replicates_executed_per_arm == 3
    assert manifest.replicate_stop_reason == ("third_pair_required_after_first_two_disagreement")
    confidence = manifest.to_dict()["confidence_observation"]
    assert isinstance(confidence, dict)
    assert confidence["reported_count"] == 6
    assert confidence["third_pair_confidence_by_arm"] == {
        "structured_agent_core": 0.7,
        "structured_agent_plus_routed_methods": 0.7,
    }
    assert validate_agent_contract(manifest.to_dict(), "decision-run-manifest.schema.json") == ()

    by_id = {
        result.judgment.artifact_id: result.judgment
        for result in _runs()[manifest.treatment_arm]
        if result.judgment is not None
    }
    judgments = tuple(by_id[item] for item in manifest.agreeing_judgment_artifact_ids)
    signal = build_signal_from_decision_manifest(
        manifest=manifest,
        evidence_pack=pack,
        judgments=judgments,
        valid_from=NOW - timedelta(minutes=5),
        expires_at=NOW + timedelta(minutes=30),
    )
    with pytest.raises(ValueError, match="cannot predate"):
        build_signal_from_decision_manifest(
            manifest=manifest,
            evidence_pack=pack,
            judgments=judgments,
            valid_from=manifest.created_at - timedelta(seconds=1),
            expires_at=NOW + timedelta(minutes=30),
        )
    order = OrderIntent(
        client_order_id="decision-order-1",
        signal_id=signal.signal_id,
        account_id="paper-account",
        environment=TradingEnvironment.PAPER,
        instrument_id=signal.instrument_id,
        side=signal.side,
        quantity=Decimal("10"),
        order_kind=OrderKind.MARKET,
        created_at=NOW - timedelta(minutes=4),
        expires_at=NOW + timedelta(minutes=20),
    )
    admission = prepare_decision_admission(
        manifest=manifest,
        query_gate=gate,
        evidence_pack=pack,
        signal=signal,
        order=order,
        created_at=NOW - timedelta(minutes=3),
    )
    assert admission.paper_approval_mode == "manual_each"
    assert validate_agent_contract(admission.to_dict(), "decision-admission.schema.json") == ()

    with pytest.raises(ValueError, match="different Signal"):
        admission.assert_matches(
            manifest=manifest,
            query_gate=gate,
            evidence_pack=pack,
            signal=replace(signal, side=Side.SELL),
            order=order,
        )

    wrong_signal = replace(
        signal,
        signal_id="signal-forged-target",
        instrument_id="510500.XSHG",
        side=Side.SELL,
    )
    wrong_order = replace(
        order,
        signal_id=wrong_signal.signal_id,
        instrument_id=wrong_signal.instrument_id,
        side=wrong_signal.side,
    )
    with pytest.raises(ValueError, match="treatment agreement"):
        prepare_decision_admission(
            manifest=manifest,
            query_gate=gate,
            evidence_pack=pack,
            signal=wrong_signal,
            order=wrong_order,
            created_at=NOW - timedelta(minutes=3),
        )

    expired_signal = replace(signal, expires_at=NOW - timedelta(minutes=4, seconds=30))
    with pytest.raises(PermissionError, match="outside Signal validity"):
        prepare_decision_admission(
            manifest=manifest,
            query_gate=gate,
            evidence_pack=pack,
            signal=expired_signal,
            order=order,
            created_at=NOW - timedelta(minutes=3),
        )


def test_treatment_without_two_matching_votes_abstains_and_cannot_create_signal() -> None:
    pack = _pack()
    plan = _execution_plan()
    gate = _gate(pack, plan)
    runs = _runs()
    treatment = "structured_agent_plus_routed_methods"
    runs[treatment] = (
        _result(treatment, 1, "510300.XSHG", CandidateDirection.UP),
        _result(treatment, 2, "510300.XSHG", CandidateDirection.DOWN),
        _result(treatment, 3, "510500.XSHG", CandidateDirection.DOWN),
    )
    manifest = build_decision_run_manifest(
        registration=_registration(),
        query_gate=gate,
        evidence_pack=pack,
        execution_plan=plan,
        paired_runs=_paired_runs(runs),
        created_at=NOW - timedelta(minutes=6),
    )

    assert manifest.disposition is DecisionDisposition.ABSTAIN
    assert manifest.blockers == ("treatment:no_majority_target_direction_agreement",)
    with pytest.raises(PermissionError, match="cannot create a Signal"):
        build_signal_from_decision_manifest(
            manifest=manifest,
            evidence_pack=pack,
            judgments=(),
            valid_from=NOW,
            expires_at=NOW + timedelta(minutes=1),
        )


def test_reused_judgment_or_binding_mismatch_forces_abstention() -> None:
    pack = _pack()
    plan = _execution_plan()
    gate = _gate(pack, plan)
    runs = _runs()
    treatment = "structured_agent_plus_routed_methods"
    duplicate = runs[treatment][0]
    runs[treatment] = (duplicate, duplicate, runs[treatment][2])
    manifest = build_decision_run_manifest(
        registration=_registration(),
        query_gate=gate,
        evidence_pack=pack,
        execution_plan=plan,
        paired_runs=_paired_runs(runs),
        created_at=NOW - timedelta(minutes=6),
    )
    assert manifest.disposition is DecisionDisposition.ABSTAIN
    assert "paired_runs:duplicate_run_id" in manifest.blockers
    assert "paired_runs:duplicate_judgment_artifact_id" in manifest.blockers

    relabeled = list(_paired_runs())
    relabeled[0] = replace(relabeled[0], result=relabeled[3].result)
    relabeled[3] = replace(relabeled[3], result=relabeled[0].result)
    relabeled_manifest = build_decision_run_manifest(
        registration=_registration(),
        query_gate=gate,
        evidence_pack=pack,
        execution_plan=plan,
        paired_runs=tuple(relabeled),
        created_at=NOW - timedelta(minutes=6),
    )
    assert relabeled_manifest.disposition is DecisionDisposition.ABSTAIN
    assert any("execution_binding_mismatch" in item for item in relabeled_manifest.blockers)

    bad_metrics = _runs()
    control = _registration().paired_arms[0]
    bad_metrics[control] = (
        replace(bad_metrics[control][0], metrics_hash="0" * 64),
        *bad_metrics[control][1:],
    )
    metrics_manifest = build_decision_run_manifest(
        registration=_registration(),
        query_gate=gate,
        evidence_pack=pack,
        execution_plan=plan,
        paired_runs=_paired_runs(bad_metrics),
        created_at=NOW - timedelta(minutes=6),
    )
    assert metrics_manifest.disposition is DecisionDisposition.ABSTAIN
    assert any("cost_metrics_missing_or_invalid" in item for item in metrics_manifest.blockers)

    understated_cost = _runs()
    first = understated_cost[control][0]
    assert first.metrics is not None
    zero_metrics = replace(first.metrics, estimated_cost_microusd=0)
    understated_cost[control] = (
        replace(
            first,
            metrics=zero_metrics,
            metrics_hash=canonical_hash(zero_metrics.to_dict()),
        ),
        *understated_cost[control][1:],
    )
    understated_manifest = build_decision_run_manifest(
        registration=_registration(),
        query_gate=gate,
        evidence_pack=pack,
        execution_plan=plan,
        paired_runs=_paired_runs(understated_cost),
        created_at=NOW - timedelta(minutes=6),
    )
    assert understated_manifest.disposition is DecisionDisposition.ABSTAIN
    assert any("run_validation_evidence_invalid" in item for item in understated_manifest.blockers)

    predating_runs = _runs()
    predating_runs[control] = (
        _result(
            control,
            1,
            "510500.XSHG",
            CandidateDirection.DOWN,
            started_at=gate.evaluated_at - timedelta(minutes=1),
        ),
        *predating_runs[control][1:],
    )
    predating_manifest = build_decision_run_manifest(
        registration=_registration(),
        query_gate=gate,
        evidence_pack=pack,
        execution_plan=plan,
        paired_runs=_paired_runs(predating_runs),
        created_at=NOW - timedelta(minutes=6),
    )
    assert predating_manifest.disposition is DecisionDisposition.ABSTAIN
    assert any("run_predates_query_gate" in item for item in predating_manifest.blockers)


@pytest.mark.parametrize("trigger_bound", [False, True])
def test_decision_admission_is_the_exact_restart_safe_mock_outbox_gate(
    tmp_path: Path,
    trigger_bound: bool,
) -> None:
    registration = _adaptive_v4_registration() if trigger_bound else _registration()
    trigger = _eligible_trigger(registration) if trigger_bound else None
    trigger_authority = None if trigger is None else _TriggerAuthority(trigger)
    seed_pack, _seed_gate, _seed_set, _seed_inputs, _seed_store = _evaluated_gate_fixture(
        tmp_path / "seed",
        registration=registration,
        trigger_admission=trigger,
        trigger_authority=trigger_authority,
    )
    runs, plan, authorities = _authoritative_run_fixture(
        tmp_path / "agent-runtime",
        seed_pack,
        registration=registration,
    )
    pack, gate, snapshot_set, decision_inputs, snapshot_store = _evaluated_gate_fixture(
        tmp_path / "admission-material",
        execution_plan=plan,
        registration=registration,
        trigger_admission=trigger,
        trigger_authority=trigger_authority,
    )
    assert pack == seed_pack
    manifest = build_decision_run_manifest(
        registration=registration,
        query_gate=gate,
        evidence_pack=pack,
        execution_plan=plan,
        paired_runs=_paired_runs(runs, registration=registration),
        created_at=NOW - timedelta(minutes=6),
    )
    treatment_judgments = {
        result.judgment.artifact_id: result.judgment
        for result in runs[manifest.treatment_arm]
        if result.judgment is not None
    }
    agreeing_judgments = tuple(
        treatment_judgments[item] for item in manifest.agreeing_judgment_artifact_ids
    )
    signal = build_signal_from_decision_manifest(
        manifest=manifest,
        evidence_pack=pack,
        judgments=agreeing_judgments,
        valid_from=NOW - timedelta(minutes=5),
        expires_at=NOW + timedelta(minutes=30),
    )
    root = tmp_path / "paper"
    provider_path = tmp_path / "provider.sqlite3"
    account_provider = ProviderManifest(
        schema_version="market-impact.provider-manifest.v1",
        provider_id="fixture-account",
        provider_version="1",
        transport=ProviderTransport.NATIVE,
        environments=frozenset({TradingEnvironment.PAPER}),
        declared_capabilities=frozenset({Capability.ACCOUNT}),
        verified_capabilities=frozenset({Capability.ACCOUNT}),
        markets=("XSHG",),
        order_types=(),
        supports_streaming=False,
        supports_reconciliation=True,
        enabled=True,
        trust_tier=TrustTier.PAPER_VALIDATED,
    )
    account_state = capture_account_state_snapshot(
        provider=account_provider,
        account_reference="fixture-paper-account",
        account_reference_key=b"decision-admission-account-key-32b",
        environment=TradingEnvironment.PAPER,
        as_of=gate.evaluated_at - timedelta(minutes=1),
        reconciled_at=gate.evaluated_at - timedelta(minutes=1),
        reconciliation_reference="fixture-account-reconciliation",
        cash=(
            CashBalance(
                currency="CNY",
                available=Decimal("50000"),
                settled=Decimal("50000"),
            ),
        ),
        positions=(),
        open_orders=(),
        recent_fills=(),
        recent_fills_since=gate.evaluated_at - timedelta(days=1),
    )
    position_snapshot: PositionSnapshot = account_state.project_positions(
        evaluated_at=gate.evaluated_at,
        max_age=timedelta(minutes=5),
    )
    authorized_view = AuthorizedDecisionView.build(
        cutoff=gate.evaluated_at,
        frozen_at=gate.evaluated_at + timedelta(seconds=1),
        data_snapshot_ids=gate.authorized_snapshot_ids,
        decision_input_ids=gate.authorized_decision_input_ids,
        position_snapshot=position_snapshot,
    )
    mandate = TradingMandate(
        mandate_id="paper-manual-v1",
        account_id=position_snapshot.account_reference_hash,
        environment=TradingEnvironment.PAPER,
        approval_mode=ApprovalMode.MANUAL_EACH,
        valid_from=NOW - timedelta(days=1),
        expires_at=NOW + timedelta(days=1),
        allowed_instruments=frozenset({"510300.XSHG"}),
        allowed_sides=frozenset({Side.BUY, Side.SELL}),
        max_order_notional=Decimal("10000"),
    )
    price_basis = PriceBasis(
        instrument_id="510300.XSHG",
        currency="CNY",
        unit="per_share",
        basis_kind="raw_reference_quote",
        price=Decimal("4"),
        source_id="mock-price",
        source_version="1",
        observed_at=NOW - timedelta(minutes=5),
        valid_until=NOW + timedelta(minutes=1),
    )
    portfolio_decision = evaluate_portfolio_decision(
        signal=signal,
        authorized_view=authorized_view,
        position_snapshot=position_snapshot,
        requested_action=PortfolioAction.OPEN,
        venue="XSHG",
        instrument_class="exchange_traded_fund",
        evidence_refs=signal.evidence_refs,
        decided_at=NOW - timedelta(minutes=4, seconds=30),
    )
    rule_set = load_exchange_instrument_rule_set(
        Path(__file__).parents[1]
        / "examples"
        / "research"
        / "a-share-exchange-instrument-rules-v1.json"
    )
    sizing_policy = OrderSizingPolicy(
        max_available_cash_fraction=Decimal("0.20"),
        reduction_fraction=Decimal("0.50"),
    )
    sizing_decision = size_portfolio_decision(
        portfolio_decision=portfolio_decision,
        position_snapshot=position_snapshot,
        mandate=mandate,
        price_basis=price_basis,
        rule_set=rule_set,
        sizing_policy=sizing_policy,
        order_kind=OrderKind.MARKET,
        decided_at=NOW - timedelta(minutes=4),
    )
    order = build_order_intent_from_sizing(
        sizing_decision=sizing_decision,
        signal=signal,
        mandate=mandate,
        expires_at=NOW + timedelta(minutes=20),
    )
    admission = prepare_portfolio_decision_admission(
        manifest=manifest,
        query_gate=gate,
        evidence_pack=pack,
        signal=signal,
        order=order,
        authorized_view=authorized_view,
        account_state_snapshot=account_state,
        position_snapshot=position_snapshot,
        portfolio_decision=portfolio_decision,
        sizing_decision=sizing_decision,
        mandate=mandate,
        price_basis=price_basis,
        created_at=NOW - timedelta(minutes=3),
    )
    assert validate_agent_contract(admission.to_dict(), "decision-admission-v2.schema.json") == ()
    service_now = gate.evaluated_at + timedelta(minutes=4)
    main_clock = [service_now]
    main_account_state = [account_state]

    def service(
        trigger_authority_override: _TriggerAuthority | None = trigger_authority,
    ) -> PaperExecutionService:
        return PaperExecutionService(
            root,
            provider=MockExecutionProvider(provider_path, clock=lambda: main_clock[0]),
            mandate=mandate,
            price_source=lambda _order: price_basis,
            clock=lambda: main_clock[0],
            agent_run_authorities=authorities,
            account_state_snapshots={account_state.snapshot_id: account_state},
            account_state_source=lambda: main_account_state[0],
            instrument_identities={"510300.XSHG": ("XSHG", "exchange_traded_fund")},
            instrument_rule_sets={rule_set.rule_set_id: rule_set},
            order_sizing_policies={sizing_policy.policy_id: sizing_policy},
            trigger_admission_authority=trigger_authority_override,
        )

    def admit_with(target_service: PaperExecutionService):
        return target_service.admit_decision(
            order,
            admission,
            manifest=manifest,
            query_gate=gate,
            evidence_pack=pack,
            registration=registration,
            snapshot_set=snapshot_set,
            decision_inputs=decision_inputs,
            snapshot_store=snapshot_store,
            execution_plan=plan,
            signal=signal,
            paired_runs=_paired_runs(runs, registration=registration),
            authorized_view=authorized_view,
            position_snapshot=position_snapshot,
            portfolio_decision=portfolio_decision,
            sizing_decision=sizing_decision,
            price_basis=price_basis,
            trigger_admission=trigger,
        )

    forged_sizing_core = sizing_decision.core_dict()
    forged_sizing_core["quantity"] = "100"
    forged_sizing_core["order_notional"] = "400"
    forged_sizing = replace(
        sizing_decision,
        decision_id=f"order-sizing-decision-{canonical_hash(forged_sizing_core)}",
        quantity=Decimal("100"),
        order_notional=Decimal("400"),
    )
    forged_order = build_order_intent_from_sizing(
        sizing_decision=forged_sizing,
        signal=signal,
        mandate=mandate,
        expires_at=NOW + timedelta(minutes=20),
    )
    forged_sizing_admission = prepare_portfolio_decision_admission(
        manifest=manifest,
        query_gate=gate,
        evidence_pack=pack,
        signal=signal,
        order=forged_order,
        authorized_view=authorized_view,
        account_state_snapshot=account_state,
        position_snapshot=position_snapshot,
        portfolio_decision=portfolio_decision,
        sizing_decision=forged_sizing,
        mandate=mandate,
        price_basis=price_basis,
        created_at=NOW - timedelta(minutes=3),
    )
    with pytest.raises(
        ValueError,
        match=r"not deterministically evaluated|model profile differs",
    ):
        service().admit_decision(
            forged_order,
            forged_sizing_admission,
            manifest=manifest,
            query_gate=gate,
            evidence_pack=pack,
            registration=registration,
            snapshot_set=snapshot_set,
            decision_inputs=decision_inputs,
            snapshot_store=snapshot_store,
            execution_plan=plan,
            signal=signal,
            paired_runs=_paired_runs(runs, registration=registration),
            authorized_view=authorized_view,
            position_snapshot=position_snapshot,
            portfolio_decision=portfolio_decision,
            sizing_decision=forged_sizing,
            price_basis=price_basis,
            trigger_admission=trigger,
        )

    without_account_authority = PaperExecutionService(
        tmp_path / "paper-without-account-authority",
        provider=MockExecutionProvider(
            tmp_path / "provider-without-account-authority.sqlite3",
            clock=lambda: service_now,
        ),
        mandate=mandate,
        price_source=lambda _order: price_basis,
        clock=lambda: service_now,
        agent_run_authorities=authorities,
        account_state_source=lambda: account_state,
        instrument_identities={"510300.XSHG": ("XSHG", "exchange_traded_fund")},
        instrument_rule_sets={rule_set.rule_set_id: rule_set},
        order_sizing_policies={sizing_policy.policy_id: sizing_policy},
        trigger_admission_authority=trigger_authority,
    )
    with pytest.raises(PermissionError, match="Account State authority"):
        without_account_authority.admit_decision(
            order,
            admission,
            manifest=manifest,
            query_gate=gate,
            evidence_pack=pack,
            registration=registration,
            snapshot_set=snapshot_set,
            decision_inputs=decision_inputs,
            snapshot_store=snapshot_store,
            execution_plan=plan,
            signal=signal,
            paired_runs=_paired_runs(runs, registration=registration),
            authorized_view=authorized_view,
            position_snapshot=position_snapshot,
            portfolio_decision=portfolio_decision,
            sizing_decision=sizing_decision,
            price_basis=price_basis,
            trigger_admission=trigger,
        )

    strict_freshness_service = PaperExecutionService(
        tmp_path / "paper-strict-account-freshness",
        provider=MockExecutionProvider(
            tmp_path / "provider-strict-account-freshness.sqlite3",
            clock=lambda: service_now,
        ),
        mandate=mandate,
        price_source=lambda _order: price_basis,
        clock=lambda: service_now,
        agent_run_authorities=authorities,
        account_state_snapshots={account_state.snapshot_id: account_state},
        account_state_source=lambda: account_state,
        account_state_max_age=timedelta(minutes=1),
        instrument_identities={"510300.XSHG": ("XSHG", "exchange_traded_fund")},
        instrument_rule_sets={rule_set.rule_set_id: rule_set},
        order_sizing_policies={sizing_policy.policy_id: sizing_policy},
        trigger_admission_authority=trigger_authority,
    )
    with pytest.raises(ValueError, match="Position Snapshot is not a trusted projection"):
        admit_with(strict_freshness_service)

    without_instrument_identity = PaperExecutionService(
        tmp_path / "paper-without-instrument-identity",
        provider=MockExecutionProvider(
            tmp_path / "provider-without-instrument-identity.sqlite3",
            clock=lambda: service_now,
        ),
        mandate=mandate,
        price_source=lambda _order: price_basis,
        clock=lambda: service_now,
        agent_run_authorities=authorities,
        account_state_snapshots={account_state.snapshot_id: account_state},
        account_state_source=lambda: account_state,
        instrument_rule_sets={rule_set.rule_set_id: rule_set},
        order_sizing_policies={sizing_policy.policy_id: sizing_policy},
        trigger_admission_authority=trigger_authority,
    )
    with pytest.raises(PermissionError, match="Instrument Master identity"):
        without_instrument_identity.admit_decision(
            order,
            admission,
            manifest=manifest,
            query_gate=gate,
            evidence_pack=pack,
            registration=registration,
            snapshot_set=snapshot_set,
            decision_inputs=decision_inputs,
            snapshot_store=snapshot_store,
            execution_plan=plan,
            signal=signal,
            paired_runs=_paired_runs(runs, registration=registration),
            authorized_view=authorized_view,
            position_snapshot=position_snapshot,
            portfolio_decision=portfolio_decision,
            sizing_decision=sizing_decision,
            price_basis=price_basis,
            trigger_admission=trigger,
        )

    wrong_instrument_identity = PaperExecutionService(
        tmp_path / "paper-wrong-instrument-identity",
        provider=MockExecutionProvider(
            tmp_path / "provider-wrong-instrument-identity.sqlite3",
            clock=lambda: service_now,
        ),
        mandate=mandate,
        price_source=lambda _order: price_basis,
        clock=lambda: service_now,
        agent_run_authorities=authorities,
        account_state_snapshots={account_state.snapshot_id: account_state},
        account_state_source=lambda: account_state,
        instrument_identities={"510300.XSHG": ("XSHG", "equity")},
        instrument_rule_sets={rule_set.rule_set_id: rule_set},
        order_sizing_policies={sizing_policy.policy_id: sizing_policy},
        trigger_admission_authority=trigger_authority,
    )
    with pytest.raises(ValueError, match="differs from Instrument Master"):
        wrong_instrument_identity.admit_decision(
            order,
            admission,
            manifest=manifest,
            query_gate=gate,
            evidence_pack=pack,
            registration=registration,
            snapshot_set=snapshot_set,
            decision_inputs=decision_inputs,
            snapshot_store=snapshot_store,
            execution_plan=plan,
            signal=signal,
            paired_runs=_paired_runs(runs, registration=registration),
            authorized_view=authorized_view,
            position_snapshot=position_snapshot,
            portfolio_decision=portfolio_decision,
            sizing_decision=sizing_decision,
            price_basis=price_basis,
            trigger_admission=trigger,
        )

    without_sizing_authority = PaperExecutionService(
        tmp_path / "paper-without-sizing-authority",
        provider=MockExecutionProvider(
            tmp_path / "provider-without-sizing-authority.sqlite3",
            clock=lambda: service_now,
        ),
        mandate=mandate,
        price_source=lambda _order: price_basis,
        clock=lambda: service_now,
        agent_run_authorities=authorities,
        account_state_snapshots={account_state.snapshot_id: account_state},
        account_state_source=lambda: account_state,
        instrument_identities={"510300.XSHG": ("XSHG", "exchange_traded_fund")},
        trigger_admission_authority=trigger_authority,
    )
    with pytest.raises(PermissionError, match="sizing rule or policy authority"):
        without_sizing_authority.admit_decision(
            order,
            admission,
            manifest=manifest,
            query_gate=gate,
            evidence_pack=pack,
            registration=registration,
            snapshot_set=snapshot_set,
            decision_inputs=decision_inputs,
            snapshot_store=snapshot_store,
            execution_plan=plan,
            signal=signal,
            paired_runs=_paired_runs(runs, registration=registration),
            authorized_view=authorized_view,
            position_snapshot=position_snapshot,
            portfolio_decision=portfolio_decision,
            sizing_decision=sizing_decision,
            price_basis=price_basis,
            trigger_admission=trigger,
        )

    forged_gate = _gate(pack, plan)
    with pytest.raises(
        ValueError,
        match=r"not deterministically evaluated|model profile differs",
    ):
        service().admit_decision(
            order,
            admission,
            manifest=manifest,
            query_gate=forged_gate,
            evidence_pack=pack,
            registration=registration,
            snapshot_set=snapshot_set,
            decision_inputs=decision_inputs,
            snapshot_store=snapshot_store,
            execution_plan=plan,
            signal=signal,
            paired_runs=_paired_runs(runs, registration=registration),
            authorized_view=authorized_view,
            position_snapshot=position_snapshot,
            portfolio_decision=portfolio_decision,
            sizing_decision=sizing_decision,
            price_basis=price_basis,
            trigger_admission=trigger,
        )

    without_authority = PaperExecutionService(
        tmp_path / "paper-without-run-authority",
        provider=MockExecutionProvider(
            tmp_path / "provider-without-run-authority.sqlite3",
            clock=lambda: service_now,
        ),
        mandate=mandate,
        price_source=lambda _order: price_basis,
        clock=lambda: service_now,
        account_state_snapshots={account_state.snapshot_id: account_state},
        account_state_source=lambda: account_state,
        instrument_identities={"510300.XSHG": ("XSHG", "exchange_traded_fund")},
        instrument_rule_sets={rule_set.rule_set_id: rule_set},
        order_sizing_policies={sizing_policy.policy_id: sizing_policy},
        trigger_admission_authority=trigger_authority,
    )
    setattr(without_authority, "agent_run_authorities", authorities)  # noqa: B010
    with pytest.raises(PermissionError, match="Agent run authority"):
        without_authority.admit_decision(
            order,
            admission,
            manifest=manifest,
            query_gate=gate,
            evidence_pack=pack,
            registration=registration,
            snapshot_set=snapshot_set,
            decision_inputs=decision_inputs,
            snapshot_store=snapshot_store,
            execution_plan=plan,
            signal=signal,
            paired_runs=_paired_runs(runs, registration=registration),
            authorized_view=authorized_view,
            position_snapshot=position_snapshot,
            portfolio_decision=portfolio_decision,
            sizing_decision=sizing_decision,
            price_basis=price_basis,
            trigger_admission=trigger,
        )

    record = admit_with(service())
    assert record.approval_state is ApprovalState.PENDING_APPROVAL
    assert record.agent_admission_hash == canonical_hash(admission.to_dict())
    assert service().get(order.client_order_id) == record
    if trigger_bound:
        with pytest.raises(
            PermissionError,
            match="restart lacks prospective Trigger Admission authority",
        ):
            service(None).get(order.client_order_id)
        assert trigger is not None
        forged_core = trigger.core_dict()
        forged_core["candidate_set_id"] = "event-impact-triage-candidate-set-" + "9" * 64
        forged_trigger = replace(
            trigger,
            admission_id=f"prospective-trigger-admission-{canonical_hash(forged_core)}",
            candidate_set_id=cast(str, forged_core["candidate_set_id"]),
        )
        with pytest.raises(
            ValueError,
            match="Trigger Admission differs from durable authority",
        ):
            service(_TriggerAuthority(forged_trigger)).get(order.client_order_id)

    evaluation_material = build_query_gate_evaluation_material(
        registration=registration,
        snapshot_set=snapshot_set,
        decision_inputs=decision_inputs,
        snapshot_store=snapshot_store,
        trigger_admission=trigger,
    )
    service().artifacts.get(
        gate.evaluation_material_hash,
        media_type="application/json",
    ).path.unlink()
    with pytest.raises(FileNotFoundError):
        service().get(order.client_order_id)
    assert (
        service().artifacts.put_json(evaluation_material).content_hash
        == gate.evaluation_material_hash
    )

    first_metrics_hash = manifest.assessments[0].metrics_hash
    assert first_metrics_hash is not None
    metrics = _paired_runs(runs)[0].result.metrics
    assert metrics is not None
    service().artifacts.get(first_metrics_hash, media_type="application/json").path.unlink()
    with pytest.raises(FileNotFoundError):
        service().get(order.client_order_id)
    assert service().artifacts.put_json(metrics.to_dict()).content_hash == first_metrics_hash

    first_validation_hash = manifest.assessments[0].run_validation_evidence_hash
    assert first_validation_hash is not None
    first_validation_event = _paired_runs(runs)[0].result.validation_event
    assert first_validation_event is not None
    service().artifacts.get(
        first_validation_hash,
        media_type="application/json",
    ).path.unlink()
    with pytest.raises(FileNotFoundError):
        service().get(order.client_order_id)
    assert (
        service().artifacts.put_json(first_validation_event.to_dict()).content_hash
        == first_validation_hash
    )

    missing_judgment_hash = next(
        item.judgment_artifact_hash
        for item in manifest.assessments
        if item.judgment_artifact_id in manifest.agreeing_judgment_artifact_ids
    )
    assert missing_judgment_hash is not None
    service().artifacts.get(
        missing_judgment_hash,
        media_type="application/json",
    ).path.unlink()
    with pytest.raises(FileNotFoundError):
        service().get(order.client_order_id)
    missing_judgment = next(
        result.judgment
        for result in (*runs[manifest.control_arm], *runs[manifest.treatment_arm])
        if result.judgment is not None
        and canonical_hash(result.judgment.to_dict()) == missing_judgment_hash
    )
    assert missing_judgment is not None
    assert (
        service().artifacts.put_json(missing_judgment.to_dict()).content_hash
        == missing_judgment_hash
    )

    approved = service().decide(
        order.client_order_id,
        approve=True,
        actor_ref="fixture-human-approver",
    )
    assert approved.approval_state is ApprovalState.APPROVED
    assert approved.outbox_state is OutboxState.QUEUED
    accepted = service().dispatch_next()
    assert accepted is not None
    assert accepted.outbox_state is OutboxState.ACCEPTED
    assert accepted.fill_status is None
    reconciliation = service().reconcile()
    assert reconciliation.complete
    assert reconciliation.gaps == ()
    assert service().get(order.client_order_id).outbox_state is OutboxState.RECONCILED

    legacy_service = PaperExecutionService(
        tmp_path / "paper-legacy-admission",
        provider=MockExecutionProvider(
            tmp_path / "provider-legacy-admission.sqlite3",
            clock=lambda: service_now,
        ),
        mandate=mandate,
        price_source=lambda _order: price_basis,
        clock=lambda: service_now,
        agent_run_authorities=authorities,
        account_state_snapshots={account_state.snapshot_id: account_state},
        account_state_source=lambda: account_state,
        instrument_identities={"510300.XSHG": ("XSHG", "exchange_traded_fund")},
        instrument_rule_sets={rule_set.rule_set_id: rule_set},
        order_sizing_policies={sizing_policy.policy_id: sizing_policy},
        trigger_admission_authority=trigger_authority,
    )
    assert admit_with(legacy_service).approval_state is ApprovalState.PENDING_APPROVAL
    legacy_admission = prepare_decision_admission(
        manifest=manifest,
        query_gate=gate,
        evidence_pack=pack,
        signal=signal,
        order=order,
        created_at=NOW - timedelta(minutes=3),
    )
    legacy_artifact = legacy_service.artifacts.put_json(legacy_admission.to_dict())
    with sqlite3.connect(legacy_service.database_path) as connection:
        connection.execute(
            "UPDATE paper_intents SET agent_admission_hash = ? WHERE client_order_id = ?",
            (legacy_artifact.content_hash, order.client_order_id),
        )
    legacy_result = legacy_service.decide(
        order.client_order_id,
        approve=True,
        actor_ref="fixture-human-approver",
    )
    assert legacy_result.approval_state is ApprovalState.EXPIRED
    assert legacy_result.outbox_state is None

    stale_clock = [service_now]
    stale_service = PaperExecutionService(
        tmp_path / "paper-stale-before-approval",
        provider=MockExecutionProvider(
            tmp_path / "provider-stale-before-approval.sqlite3",
            clock=lambda: stale_clock[0],
        ),
        mandate=mandate,
        price_source=lambda _order: price_basis,
        clock=lambda: stale_clock[0],
        agent_run_authorities=authorities,
        account_state_snapshots={account_state.snapshot_id: account_state},
        account_state_source=lambda: account_state,
        instrument_identities={"510300.XSHG": ("XSHG", "exchange_traded_fund")},
        instrument_rule_sets={rule_set.rule_set_id: rule_set},
        order_sizing_policies={sizing_policy.policy_id: sizing_policy},
        trigger_admission_authority=trigger_authority,
    )
    assert admit_with(stale_service).approval_state is ApprovalState.PENDING_APPROVAL
    stale_clock[0] += timedelta(seconds=1)
    stale_approval = stale_service.decide(
        order.client_order_id,
        approve=True,
        actor_ref="fixture-human-approver",
    )
    assert stale_approval.approval_state is ApprovalState.EXPIRED

    changed_account_state = capture_account_state_snapshot(
        provider=account_provider,
        account_reference="fixture-paper-account",
        account_reference_key=b"decision-admission-account-key-32b",
        environment=TradingEnvironment.PAPER,
        as_of=account_state.as_of,
        reconciled_at=account_state.reconciled_at,
        reconciliation_reference="fixture-account-reconciliation-changed",
        cash=(
            CashBalance(
                currency="CNY",
                available=Decimal("49000"),
                settled=Decimal("49000"),
            ),
        ),
        positions=(),
        open_orders=(),
        recent_fills=(),
        recent_fills_since=gate.evaluated_at - timedelta(days=1),
    )
    dispatch_account_state = [account_state]
    dispatch_service = PaperExecutionService(
        tmp_path / "paper-account-changed-before-dispatch",
        provider=MockExecutionProvider(
            tmp_path / "provider-account-changed-before-dispatch.sqlite3",
            clock=lambda: service_now,
        ),
        mandate=mandate,
        price_source=lambda _order: price_basis,
        clock=lambda: service_now,
        agent_run_authorities=authorities,
        account_state_snapshots={account_state.snapshot_id: account_state},
        account_state_source=lambda: dispatch_account_state[0],
        instrument_identities={"510300.XSHG": ("XSHG", "exchange_traded_fund")},
        instrument_rule_sets={rule_set.rule_set_id: rule_set},
        order_sizing_policies={sizing_policy.policy_id: sizing_policy},
        trigger_admission_authority=trigger_authority,
    )
    assert admit_with(dispatch_service).approval_state is ApprovalState.PENDING_APPROVAL
    assert (
        dispatch_service.decide(
            order.client_order_id,
            approve=True,
            actor_ref="fixture-human-approver",
        ).outbox_state
        is OutboxState.QUEUED
    )
    dispatch_account_state[0] = changed_account_state
    assert dispatch_service.dispatch_next() is None
    assert dispatch_service.get(order.client_order_id).outbox_state is OutboxState.EXPIRED

    race_source_calls = [0]

    def account_state_changes_after_claim() -> AccountStateSnapshot:
        race_source_calls[0] += 1
        if race_source_calls[0] <= 3:
            return account_state
        return changed_account_state

    race_provider = MockExecutionProvider(
        tmp_path / "provider-account-changed-after-claim.sqlite3",
        clock=lambda: service_now,
    )
    race_service = PaperExecutionService(
        tmp_path / "paper-account-changed-after-claim",
        provider=race_provider,
        mandate=mandate,
        price_source=lambda _order: price_basis,
        clock=lambda: service_now,
        agent_run_authorities=authorities,
        account_state_snapshots={account_state.snapshot_id: account_state},
        account_state_source=account_state_changes_after_claim,
        instrument_identities={"510300.XSHG": ("XSHG", "exchange_traded_fund")},
        instrument_rule_sets={rule_set.rule_set_id: rule_set},
        order_sizing_policies={sizing_policy.policy_id: sizing_policy},
        trigger_admission_authority=trigger_authority,
    )
    assert admit_with(race_service).approval_state is ApprovalState.PENDING_APPROVAL
    assert (
        race_service.decide(
            order.client_order_id,
            approve=True,
            actor_ref="fixture-human-approver",
        ).outbox_state
        is OutboxState.QUEUED
    )
    raced = race_service.dispatch_next()
    assert raced is not None
    assert raced.outbox_state is OutboxState.EXPIRED
    assert race_provider.reconcile().receipts == ()
    assert race_service.execution_blocked is False

    source_error_calls = [0]

    def account_state_check_fails_after_claim() -> AccountStateSnapshot:
        source_error_calls[0] += 1
        if source_error_calls[0] <= 3:
            return account_state
        raise RuntimeError("fixture Account State source unavailable")

    source_error_provider = MockExecutionProvider(
        tmp_path / "provider-account-source-error-after-claim.sqlite3",
        clock=lambda: service_now,
    )
    source_error_service = PaperExecutionService(
        tmp_path / "paper-account-source-error-after-claim",
        provider=source_error_provider,
        mandate=mandate,
        price_source=lambda _order: price_basis,
        clock=lambda: service_now,
        agent_run_authorities=authorities,
        account_state_snapshots={account_state.snapshot_id: account_state},
        account_state_source=account_state_check_fails_after_claim,
        instrument_identities={"510300.XSHG": ("XSHG", "exchange_traded_fund")},
        instrument_rule_sets={rule_set.rule_set_id: rule_set},
        order_sizing_policies={sizing_policy.policy_id: sizing_policy},
        trigger_admission_authority=trigger_authority,
    )
    assert admit_with(source_error_service).approval_state is ApprovalState.PENDING_APPROVAL
    assert (
        source_error_service.decide(
            order.client_order_id,
            approve=True,
            actor_ref="fixture-human-approver",
        ).outbox_state
        is OutboxState.QUEUED
    )
    source_error_result = source_error_service.dispatch_next()
    assert source_error_result is not None
    assert source_error_result.outbox_state is OutboxState.EXPIRED
    assert source_error_provider.reconcile().receipts == ()
    assert source_error_service.execution_blocked is False

    validator_race_calls = [0]

    def account_state_changes_inside_provider_validator() -> AccountStateSnapshot:
        validator_race_calls[0] += 1
        if validator_race_calls[0] <= 4:
            return account_state
        return changed_account_state

    validator_race_provider = MockExecutionProvider(
        tmp_path / "provider-account-changed-in-validator.sqlite3",
        clock=lambda: service_now,
    )
    validator_race_service = PaperExecutionService(
        tmp_path / "paper-account-changed-in-validator",
        provider=validator_race_provider,
        mandate=mandate,
        price_source=lambda _order: price_basis,
        clock=lambda: service_now,
        agent_run_authorities=authorities,
        account_state_snapshots={account_state.snapshot_id: account_state},
        account_state_source=account_state_changes_inside_provider_validator,
        instrument_identities={"510300.XSHG": ("XSHG", "exchange_traded_fund")},
        instrument_rule_sets={rule_set.rule_set_id: rule_set},
        order_sizing_policies={sizing_policy.policy_id: sizing_policy},
        trigger_admission_authority=trigger_authority,
    )
    assert admit_with(validator_race_service).approval_state is ApprovalState.PENDING_APPROVAL
    assert (
        validator_race_service.decide(
            order.client_order_id,
            approve=True,
            actor_ref="fixture-human-approver",
        ).outbox_state
        is OutboxState.QUEUED
    )
    validator_rejected = validator_race_service.dispatch_next()
    assert validator_rejected is not None
    assert validator_rejected.outbox_state is OutboxState.EXPIRED
    assert validator_race_provider.reconcile().receipts == ()
    assert validator_race_service.execution_blocked is False


def test_one_shot_pipeline_replays_four_runs_and_mock_admission_idempotently(
    tmp_path: Path,
) -> None:
    registration = _adaptive_v4_registration()
    trigger = _eligible_trigger(registration)
    trigger_authority = _TriggerAuthority(trigger)
    pack, _unused_gate, snapshot_set, _inputs, snapshot_store = _evaluated_gate_fixture(
        tmp_path / "inputs",
        registration=registration,
        trigger_admission=trigger,
        trigger_authority=trigger_authority,
    )
    engines, plan, selected_skills = _pipeline_runtime_fixture(
        tmp_path / "runtime",
        registration=registration,
        evidence_pack=pack,
        snapshot_set=snapshot_set,
        snapshot_store=snapshot_store,
    )
    frozen = ArtifactStore(tmp_path / "frozen")
    refs = FrozenProspectiveDecisionRefs(
        registration_hash=frozen.put_json(registration.to_dict()).content_hash,
        checkpoint_snapshot_set_hash=frozen.put_json(snapshot_set.to_dict()).content_hash,
        evidence_pack_hash=frozen.put_json(pack.to_dict()).content_hash,
        execution_plan_hash=frozen.put_json(plan.to_dict()).content_hash,
        trigger_admission_id=trigger.admission_id,
    )
    account_provider = ProviderManifest(
        schema_version="market-impact.provider-manifest.v1",
        provider_id="fixture-account",
        provider_version="1",
        transport=ProviderTransport.NATIVE,
        environments=frozenset({TradingEnvironment.PAPER}),
        declared_capabilities=frozenset({Capability.ACCOUNT}),
        verified_capabilities=frozenset({Capability.ACCOUNT}),
        markets=("XSHG",),
        order_types=(),
        supports_streaming=False,
        supports_reconciliation=True,
        enabled=True,
        trust_tier=TrustTier.PAPER_VALIDATED,
    )
    account_state = capture_account_state_snapshot(
        provider=account_provider,
        account_reference="pipeline-paper-account",
        account_reference_key=b"pipeline-account-reference-key-32b",
        environment=TradingEnvironment.PAPER,
        as_of=NOW - timedelta(minutes=7),
        reconciled_at=NOW - timedelta(minutes=7),
        reconciliation_reference="pipeline-account-reconciliation",
        cash=(CashBalance(currency="CNY", available=Decimal("50000"), settled=Decimal("50000")),),
        positions=(),
        open_orders=(),
        recent_fills=(),
        recent_fills_since=NOW - timedelta(days=1),
    )
    mandate = TradingMandate(
        mandate_id="pipeline-paper-manual-v1",
        account_id=account_state.account_reference_hash,
        environment=TradingEnvironment.PAPER,
        approval_mode=ApprovalMode.MANUAL_EACH,
        valid_from=NOW - timedelta(days=1),
        expires_at=NOW + timedelta(days=1),
        allowed_instruments=frozenset({"510300.XSHG"}),
        allowed_sides=frozenset({Side.BUY}),
        max_order_notional=Decimal("10000"),
    )
    price_basis = PriceBasis(
        instrument_id="510300.XSHG",
        currency="CNY",
        unit="per_share",
        basis_kind="raw_reference_quote",
        price=Decimal("4"),
        source_id="mock-price",
        source_version="1",
        observed_at=NOW - timedelta(minutes=6),
        valid_until=NOW + timedelta(minutes=1),
    )
    rule_set = load_exchange_instrument_rule_set(
        Path(__file__).parents[1]
        / "examples"
        / "research"
        / "a-share-exchange-instrument-rules-v1.json"
    )
    sizing_policy = OrderSizingPolicy(
        max_available_cash_fraction=Decimal("0.20"),
        reduction_fraction=Decimal("0.50"),
    )
    paper_root = tmp_path / "paper"
    provider_path = tmp_path / "mock-provider.sqlite3"

    def paper_service() -> PaperExecutionService:
        return PaperExecutionService(
            paper_root,
            provider=MockExecutionProvider(provider_path, clock=lambda: NOW - timedelta(minutes=3)),
            mandate=mandate,
            price_source=lambda _order: price_basis,
            clock=lambda: NOW - timedelta(minutes=3),
            agent_run_authorities={
                plan.arm_binding(arm).binding_hash: engines[arm] for arm in registration.paired_arms
            },
            account_state_snapshots={account_state.snapshot_id: account_state},
            account_state_source=lambda: account_state,
            instrument_identities={"510300.XSHG": ("XSHG", "exchange_traded_fund")},
            instrument_rule_sets={rule_set.rule_set_id: rule_set},
            order_sizing_policies={sizing_policy.policy_id: sizing_policy},
            trigger_admission_authority=trigger_authority,
        )

    times = iter(
        (
            NOW - timedelta(minutes=6),
            NOW - timedelta(minutes=4),
            NOW - timedelta(minutes=4),
        )
    )
    pipeline = ProspectiveDecisionPipeline(
        frozen_artifacts=frozen,
        snapshot_store=snapshot_store,
        trigger_store=trigger_authority,
        engines=engines,
        usage_ledger=UsageLedger(tmp_path / "usage.sqlite3"),
        paper_service=paper_service(),
        account_state=account_state,
        instrument_rule_set=rule_set,
        sizing_policy=sizing_policy,
        price_basis=price_basis,
        clock=lambda: next(times),
    )
    instruction = ProspectivePortfolioInstruction(
        requested_action=PortfolioAction.OPEN,
        venue="XSHG",
        instrument_class="exchange_traded_fund",
        order_kind=OrderKind.MARKET,
        signal_valid_for=timedelta(minutes=30),
        order_valid_for=timedelta(minutes=20),
    )
    first = asyncio.run(
        pipeline.run(
            refs=refs,
            selected_skills=selected_skills,
            research_instruction="Assess this prospective checkpoint.",
            model_cost_limit_usd=Decimal("5.00"),
            portfolio=instruction,
        )
    )
    assert first.status is ProspectiveDecisionPipelineStatus.PENDING_MANUAL_APPROVAL
    assert first.manifest is not None
    assert first.manifest.replicates_executed_per_arm == 2
    assert len(first.paired_runs) == 4
    assert first.paper_record is not None
    assert first.paper_record.approval_state is ApprovalState.PENDING_APPROVAL
    assert first.reconciliation is not None and first.reconciliation.complete

    replay_times = iter(
        (
            NOW - timedelta(minutes=6),
            NOW - timedelta(minutes=4),
            NOW - timedelta(minutes=4),
        )
    )
    replay = ProspectiveDecisionPipeline(
        frozen_artifacts=frozen,
        snapshot_store=snapshot_store,
        trigger_store=trigger_authority,
        engines=engines,
        usage_ledger=UsageLedger(tmp_path / "usage.sqlite3"),
        paper_service=paper_service(),
        account_state=account_state,
        instrument_rule_set=rule_set,
        sizing_policy=sizing_policy,
        price_basis=price_basis,
        clock=lambda: next(replay_times),
    )
    second = asyncio.run(
        replay.run(
            refs=refs,
            selected_skills=selected_skills,
            research_instruction="Assess this prospective checkpoint.",
            model_cost_limit_usd=Decimal("5.00"),
            portfolio=instruction,
        )
    )
    assert second.manifest == first.manifest
    assert second.order == first.order
    assert second.paper_record == first.paper_record
    assert len(UsageLedger(tmp_path / "usage.sqlite3").records()) == 4
