from dataclasses import replace
from datetime import UTC, datetime, timedelta
from hashlib import sha256

import pytest

from market_impact_agent.agent_contracts import (
    CandidateDirection,
    CandidateImpact,
    EvidencePack,
    EvidenceReference,
    JudgmentArtifact,
    JudgmentDecision,
    JudgmentProposal,
    ProposedTransmissionStep,
    canonical_hash,
)
from market_impact_agent.agent_engine import AgentExecutionBinding
from market_impact_agent.agent_schema import validate_agent_contract
from market_impact_agent.judgment_skill_trace import (
    AgentReportedSkillUse,
    JudgmentSkillTrace,
    JudgmentSkillTraceEntry,
    SkillOfferDisposition,
    SkillRouteDisposition,
    judgment_skill_trace_from_dict,
)
from market_impact_agent.research import EvidenceTier, TransmissionDirectness
from market_impact_agent.research_methods import (
    MethodArm,
    ResearchContext,
    SkillRoute,
)

NOW = datetime(2026, 8, 30, 7, tzinfo=UTC)
ROUTE_REASONS = ("registered event-transmission route",)


def digest(value: str) -> str:
    return sha256(value.encode()).hexdigest()


def pack() -> EvidencePack:
    return EvidencePack.build(
        event_id="event-1",
        as_of=NOW,
        research_question="What is the bounded impact?",
        evidence=(
            EvidenceReference(
                evidence_id="official-event",
                claim_id="event-fact",
                source_ref="official://event",
                source_tier=EvidenceTier.OFFICIAL,
                available_at=NOW - timedelta(minutes=1),
                content_hash=digest("event"),
                summary="An official event occurred.",
            ),
        ),
        pattern_packs=(),
        allowed_targets=("sector-index",),
    )


def proposal() -> JudgmentProposal:
    return JudgmentProposal(
        event_id="event-1",
        decision=JudgmentDecision.PROPOSE,
        summary="The evidence supports a bounded positive industry impact.",
        transmission_steps=(
            ProposedTransmissionStep(
                step_id="step-1",
                from_node="event",
                to_node="sector",
                mechanism="The event changes the available industry supply.",
                directness=TransmissionDirectness.DIRECT,
                horizon_sessions=5,
                evidence_refs=("official-event",),
            ),
        ),
        candidates=(
            CandidateImpact(
                target_id="sector-index",
                direction=CandidateDirection.UP,
                horizon_sessions=5,
                directness=TransmissionDirectness.DIRECT,
                confidence=0.7,
                thesis="The bounded supply change supports the sector.",
                evidence_refs=("official-event",),
                counterevidence_refs=(),
                invalidation_conditions=("the event is reversed",),
            ),
        ),
        blockers=(),
        unresolved_questions=(),
        stopped_reason="The registered question has enough evidence for a proposal.",
        decision_confidence=0.65,
    )


def artifact(selected_pack: EvidencePack, skill_hash: str) -> JudgmentArtifact:
    return JudgmentArtifact.build(
        run_id="run-1",
        evidence_pack_id=selected_pack.pack_id,
        provider_id="fixture-provider",
        model="fixture-model",
        runtime_config_hash=digest("runtime"),
        prompt_hash=digest("prompt"),
        skill_hashes=(skill_hash,),
        tool_manifest_hashes=(digest("tool"),),
        tool_surface_hash=digest("tool-surface"),
        mcp_server_hashes=(),
        context_estimator_id="fixture-counter",
        compactor_id="fixture-compactor",
        journal_hash=digest("journal"),
        transcript_hash=digest("transcript"),
        raw_response_hash=digest("response"),
        started_at=NOW,
        finished_at=NOW + timedelta(seconds=1),
        proposal=proposal(),
    )


def binding(selected_artifact: JudgmentArtifact) -> AgentExecutionBinding:
    return AgentExecutionBinding(
        runtime_ref="runtime-1",
        runtime_config_hash=selected_artifact.runtime_config_hash,
        prompt_hash=selected_artifact.prompt_hash,
        skill_hashes=selected_artifact.skill_hashes,
        tool_manifest_hashes=selected_artifact.tool_manifest_hashes,
        tool_surface_hash=selected_artifact.tool_surface_hash,
        mcp_server_hashes=selected_artifact.mcp_server_hashes,
        context_estimator_id=selected_artifact.context_estimator_id,
        compactor_id=selected_artifact.compactor_id,
    )


def route(skill_hash: str) -> SkillRoute:
    arm = MethodArm.GENERAL_METHODS
    context = ResearchContext(
        mechanism_family="event-transmission",
        asset_class="equity",
        has_pattern_pack=False,
    )
    core = {
        "schema_version": "market-impact.skill-route.v1",
        "arm": arm.value,
        "context": {
            "mechanism_family": context.mechanism_family,
            "asset_class": context.asset_class,
            "has_pattern_pack": context.has_pattern_pack,
        },
        "requested_skills": ["event-transmission", "owner-value-discipline"],
        "loaded_skills": ["event-transmission"],
        "manifest_hashes": [skill_hash],
        "allowed_capabilities": ["evidence.read"],
        "allowed_tools": [],
        "reasons": list(ROUTE_REASONS),
    }
    return SkillRoute(
        route_id=f"skill-route-{canonical_hash(core)}",
        arm=arm,
        context=context,
        requested_skills=("event-transmission", "owner-value-discipline"),
        loaded_skills=("event-transmission",),
        manifest_hashes=(skill_hash,),
        allowed_capabilities=("evidence.read",),
        allowed_tools=(),
        reasons=ROUTE_REASONS,
    )


def test_skill_trace_binds_route_load_and_observational_influence() -> None:
    selected_pack = pack()
    skill_hash = digest("event-transmission-skill")
    selected_artifact = artifact(selected_pack, skill_hash)
    selected_route = route(skill_hash)
    entries = (
        JudgmentSkillTraceEntry(
            skill_name="event-transmission",
            manifest_hash=skill_hash,
            offer_disposition=SkillOfferDisposition.OFFERED,
            route_disposition=SkillRouteDisposition.SELECTED,
            loaded=True,
            route_reasons=ROUTE_REASONS,
            agent_reported_use=AgentReportedSkillUse.APPLIED,
            trigger_evidence_refs=("official-event",),
            influenced_proposal_paths=("/transmission_steps/0", "/candidates/0"),
            agent_rationale="The Skill structured the evidence-linked transmission chain.",
        ),
        JudgmentSkillTraceEntry(
            skill_name="owner-value-discipline",
            manifest_hash=None,
            offer_disposition=SkillOfferDisposition.OFFERED,
            route_disposition=SkillRouteDisposition.REJECTED,
            loaded=False,
            route_reasons=ROUTE_REASONS,
            agent_reported_use=AgentReportedSkillUse.NOT_APPLICABLE,
            trigger_evidence_refs=(),
            influenced_proposal_paths=(),
            agent_rationale="The Skill was not loaded and did not influence the proposal.",
        ),
    )
    trace = JudgmentSkillTrace.build(
        observed_at=NOW + timedelta(seconds=2),
        artifact=selected_artifact,
        route=selected_route,
        execution_binding=binding(selected_artifact),
        evidence_pack=selected_pack,
        entries=entries,
    )

    assert trace.to_dict()["agent_report_authority"] == (
        "observational_self_report_not_causal_evidence"
    )
    assert trace.to_dict()["signal_or_execution_authority"] is False
    assert trace.judgment_artifact_hash == canonical_hash(selected_artifact.to_dict())
    assert not validate_agent_contract(trace.to_dict(), "judgment-skill-trace.schema.json")
    assert judgment_skill_trace_from_dict(trace.to_dict()) == trace


def test_skill_trace_rejects_false_use_and_unknown_influence_claims() -> None:
    with pytest.raises(ValueError, match="unloaded Skill"):
        JudgmentSkillTraceEntry(
            skill_name="not-loaded",
            manifest_hash=None,
            offer_disposition=SkillOfferDisposition.OFFERED,
            route_disposition=SkillRouteDisposition.REJECTED,
            loaded=False,
            route_reasons=("not selected",),
            agent_reported_use=AgentReportedSkillUse.APPLIED,
            trigger_evidence_refs=(),
            influenced_proposal_paths=("/summary",),
            agent_rationale="Invalid self-report.",
        )

    selected_pack = pack()
    skill_hash = digest("event-transmission-skill")
    selected_artifact = artifact(selected_pack, skill_hash)
    valid = JudgmentSkillTraceEntry(
        skill_name="event-transmission",
        manifest_hash=skill_hash,
        offer_disposition=SkillOfferDisposition.OFFERED,
        route_disposition=SkillRouteDisposition.SELECTED,
        loaded=True,
        route_reasons=ROUTE_REASONS,
        agent_reported_use=AgentReportedSkillUse.APPLIED,
        trigger_evidence_refs=("official-event",),
        influenced_proposal_paths=("/candidates/0",),
        agent_rationale="The Skill influenced the candidate thesis.",
    )
    unknown = replace(valid, influenced_proposal_paths=("/candidates/99",))
    rejected = JudgmentSkillTraceEntry(
        skill_name="owner-value-discipline",
        manifest_hash=None,
        offer_disposition=SkillOfferDisposition.OFFERED,
        route_disposition=SkillRouteDisposition.REJECTED,
        loaded=False,
        route_reasons=ROUTE_REASONS,
        agent_reported_use=AgentReportedSkillUse.NOT_APPLICABLE,
        trigger_evidence_refs=(),
        influenced_proposal_paths=(),
        agent_rationale="The Skill was not loaded and did not influence the proposal.",
    )

    with pytest.raises(ValueError, match="unknown proposal path"):
        JudgmentSkillTrace.build(
            observed_at=NOW + timedelta(seconds=2),
            artifact=selected_artifact,
            route=route(skill_hash),
            execution_binding=binding(selected_artifact),
            evidence_pack=selected_pack,
            entries=(unknown, rejected),
        )


def test_skill_trace_rejects_route_or_full_execution_binding_misattribution() -> None:
    selected_pack = pack()
    skill_hash = digest("event-transmission-skill")
    selected_artifact = artifact(selected_pack, skill_hash)
    valid_entry = JudgmentSkillTraceEntry(
        skill_name="event-transmission",
        manifest_hash=skill_hash,
        offer_disposition=SkillOfferDisposition.OFFERED,
        route_disposition=SkillRouteDisposition.SELECTED,
        loaded=True,
        route_reasons=ROUTE_REASONS,
        agent_reported_use=AgentReportedSkillUse.NOT_REPORTED,
        trigger_evidence_refs=(),
        influenced_proposal_paths=(),
        agent_rationale="No causal self-report was supplied.",
    )

    with pytest.raises(ValueError, match="entries differ from the frozen Skill Route"):
        JudgmentSkillTrace.build(
            observed_at=NOW + timedelta(seconds=2),
            artifact=selected_artifact,
            route=route(skill_hash),
            execution_binding=binding(selected_artifact),
            evidence_pack=selected_pack,
            entries=(valid_entry,),
        )

    selected_route = SkillRoute(
        route_id=route(skill_hash).route_id,
        arm=route(skill_hash).arm,
        context=route(skill_hash).context,
        requested_skills=("event-transmission", "owner-value-discipline"),
        loaded_skills=("event-transmission",),
        manifest_hashes=(skill_hash,),
        allowed_capabilities=("evidence.read",),
        allowed_tools=(),
        reasons=ROUTE_REASONS,
    )
    route_entries = (
        valid_entry,
        JudgmentSkillTraceEntry(
            skill_name="owner-value-discipline",
            manifest_hash=None,
            offer_disposition=SkillOfferDisposition.OFFERED,
            route_disposition=SkillRouteDisposition.REJECTED,
            loaded=False,
            route_reasons=ROUTE_REASONS,
            agent_reported_use=AgentReportedSkillUse.NOT_APPLICABLE,
            trigger_evidence_refs=(),
            influenced_proposal_paths=(),
            agent_rationale="The route did not load this Skill.",
        ),
    )
    wrong_binding = replace(binding(selected_artifact), prompt_hash=digest("wrong-prompt"))
    with pytest.raises(ValueError, match="binding differs"):
        JudgmentSkillTrace.build(
            observed_at=NOW + timedelta(seconds=2),
            artifact=selected_artifact,
            route=selected_route,
            execution_binding=wrong_binding,
            evidence_pack=selected_pack,
            entries=route_entries,
        )

    with pytest.raises(ValueError, match="only a loaded Skill"):
        replace(route_entries[1], manifest_hash=digest("fabricated-rejected-version"))

    false_disposition = replace(
        route_entries[1],
        offer_disposition=SkillOfferDisposition.DEPENDENCY_ONLY,
    )
    with pytest.raises(ValueError, match="rejected entry differs"):
        JudgmentSkillTrace.build(
            observed_at=NOW + timedelta(seconds=2),
            artifact=selected_artifact,
            route=selected_route,
            execution_binding=binding(selected_artifact),
            evidence_pack=selected_pack,
            entries=(route_entries[0], false_disposition),
        )

    false_reasons = replace(route_entries[1], route_reasons=("fabricated reason",))
    with pytest.raises(ValueError, match="reasons differ"):
        JudgmentSkillTrace.build(
            observed_at=NOW + timedelta(seconds=2),
            artifact=selected_artifact,
            route=selected_route,
            execution_binding=binding(selected_artifact),
            evidence_pack=selected_pack,
            entries=(route_entries[0], false_reasons),
        )
