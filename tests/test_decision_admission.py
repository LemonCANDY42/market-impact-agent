from __future__ import annotations

import asyncio
import json
from collections.abc import Sequence
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from hashlib import sha256
from pathlib import Path
from typing import cast

import pytest

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
    ModelProvider,
    ModelTurn,
    ProviderUsage,
    SkillRegistry,
    ToolAccessContext,
    ToolRegistry,
)
from market_impact_agent.agent_schema import validate_agent_contract
from market_impact_agent.checkpoint_decision_inputs import project_checkpoint_observation
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
    PaperExecutionService,
    PriceBasis,
)
from market_impact_agent.prospective_checkpoint_sets import (
    CheckpointCapabilityBinding,
    CheckpointRouteReconciliation,
    CheckpointToolManifest,
    ProspectiveCheckpointSnapshotSet,
)
from market_impact_agent.prospective_diagnostic import (
    PROSPECTIVE_DIAGNOSTIC_REGISTRATION_SCHEMA_V3,
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
from market_impact_agent.providers import MockExecutionProvider
from market_impact_agent.research import EvidenceTier, TransmissionDirectness
from market_impact_agent.runtime_store import ArtifactStore, RunJournal, RunStatus, RuntimeEvent

NOW = datetime(2026, 8, 30, 8, tzinfo=UTC)
REGISTRATION_PATH = Path("examples/research/prospective-diagnostic-registration-v3.json")
MODEL_PROFILE_PATH = Path("examples/providers/cliproxyapi-luna-xhigh-v1.json")
MINIMAX_PROFILE_PATH = Path("examples/providers/minimax-m3-research-v1.json")


class _DecisionRunFixtureProvider(ModelProvider):
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


def _execution_plan(
    registration: ProspectiveDiagnosticRegistration | None = None,
) -> ProspectiveExecutionPlan:
    registration = _registration() if registration is None else registration
    profile = load_model_provider_profile(MODEL_PROFILE_PATH)
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


def _write_decision_skill(root: Path, *, name: str, instructions: str) -> None:
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
        "allowed_tools": [],
        "allowed_mcp_servers": [],
    }
    manifest["manifest_hash"] = canonical_hash(manifest)
    (directory / "skill.json").write_text(json.dumps(manifest), encoding="utf-8")


def _authoritative_run_fixture(
    root: Path,
    pack: EvidencePack,
) -> tuple[
    dict[str, tuple[AgentRunResult, ...]],
    ProspectiveExecutionPlan,
    dict[str, AgentEngine],
]:
    registration = _registration()
    profile = load_model_provider_profile(MODEL_PROFILE_PATH)
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
) -> tuple[
    EvidencePack,
    ProspectiveQueryGateResult,
    ProspectiveCheckpointSnapshotSet,
    tuple[dict[str, object], ...],
    LocalDataSnapshotStore,
]:
    registration = _registration()
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
    snapshot_set_core = {
        "schema_version": "market-impact.prospective-checkpoint-snapshot-set.v4",
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
    plan = _execution_plan() if execution_plan is None else execution_plan
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
    )
    return evidence_pack, gate, snapshot_set, (decision_input,), store


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


def test_decision_admission_is_the_exact_restart_safe_mock_outbox_gate(
    tmp_path: Path,
) -> None:
    seed_pack, _seed_gate, _seed_set, _seed_inputs, _seed_store = _evaluated_gate_fixture(
        tmp_path / "seed"
    )
    runs, plan, authorities = _authoritative_run_fixture(
        tmp_path / "agent-runtime",
        seed_pack,
    )
    pack, gate, snapshot_set, decision_inputs, snapshot_store = _evaluated_gate_fixture(
        tmp_path / "admission-material",
        execution_plan=plan,
    )
    assert pack == seed_pack
    manifest = build_decision_run_manifest(
        registration=_registration(),
        query_gate=gate,
        evidence_pack=pack,
        execution_plan=plan,
        paired_runs=_paired_runs(runs),
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
    order = OrderIntent(
        client_order_id="decision-order-persisted",
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
    root = tmp_path / "paper"
    provider_path = tmp_path / "provider.sqlite3"
    mandate = TradingMandate(
        mandate_id="paper-manual-v1",
        account_id="paper-account",
        environment=TradingEnvironment.PAPER,
        approval_mode=ApprovalMode.MANUAL_EACH,
        valid_from=NOW - timedelta(days=1),
        expires_at=NOW + timedelta(days=1),
        allowed_instruments=frozenset({"510300.XSHG"}),
        allowed_sides=frozenset({Side.BUY, Side.SELL}),
        max_order_notional=Decimal("10000"),
    )

    def service() -> PaperExecutionService:
        return PaperExecutionService(
            root,
            provider=MockExecutionProvider(provider_path, clock=lambda: NOW),
            mandate=mandate,
            price_source=lambda _order: PriceBasis(
                instrument_id="510300.XSHG",
                currency="CNY",
                unit="per_share",
                basis_kind="raw_reference_quote",
                price=Decimal("4"),
                source_id="mock-price",
                source_version="1",
                observed_at=NOW - timedelta(seconds=1),
                valid_until=NOW + timedelta(minutes=1),
            ),
            clock=lambda: NOW,
            agent_run_authorities=authorities,
        )

    forged_gate = _gate(pack, plan)
    with pytest.raises(ValueError, match="not deterministically evaluated"):
        service().admit_decision(
            order,
            admission,
            manifest=manifest,
            query_gate=forged_gate,
            evidence_pack=pack,
            registration=_registration(),
            snapshot_set=snapshot_set,
            decision_inputs=decision_inputs,
            snapshot_store=snapshot_store,
            execution_plan=plan,
            signal=signal,
            paired_runs=_paired_runs(runs),
        )

    without_authority = PaperExecutionService(
        tmp_path / "paper-without-run-authority",
        provider=MockExecutionProvider(
            tmp_path / "provider-without-run-authority.sqlite3",
            clock=lambda: NOW,
        ),
        mandate=mandate,
        price_source=lambda _order: PriceBasis(
            instrument_id="510300.XSHG",
            currency="CNY",
            unit="per_share",
            basis_kind="raw_reference_quote",
            price=Decimal("4"),
            source_id="mock-price",
            source_version="1",
            observed_at=NOW - timedelta(seconds=1),
            valid_until=NOW + timedelta(minutes=1),
        ),
        clock=lambda: NOW,
    )
    setattr(without_authority, "agent_run_authorities", authorities)  # noqa: B010
    with pytest.raises(PermissionError, match="Agent run authority"):
        without_authority.admit_decision(
            order,
            admission,
            manifest=manifest,
            query_gate=gate,
            evidence_pack=pack,
            registration=_registration(),
            snapshot_set=snapshot_set,
            decision_inputs=decision_inputs,
            snapshot_store=snapshot_store,
            execution_plan=plan,
            signal=signal,
            paired_runs=_paired_runs(runs),
        )

    record = service().admit_decision(
        order,
        admission,
        manifest=manifest,
        query_gate=gate,
        evidence_pack=pack,
        registration=_registration(),
        snapshot_set=snapshot_set,
        decision_inputs=decision_inputs,
        snapshot_store=snapshot_store,
        execution_plan=plan,
        signal=signal,
        paired_runs=_paired_runs(runs),
    )
    assert record.approval_state is ApprovalState.PENDING_APPROVAL
    assert record.agent_admission_hash == canonical_hash(admission.to_dict())
    assert service().get(order.client_order_id) == record

    evaluation_material = build_query_gate_evaluation_material(
        registration=_registration(),
        snapshot_set=snapshot_set,
        decision_inputs=decision_inputs,
        snapshot_store=snapshot_store,
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
