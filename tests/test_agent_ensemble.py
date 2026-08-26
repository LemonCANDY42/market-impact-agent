from __future__ import annotations

from copy import deepcopy
from datetime import UTC, datetime, timedelta
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
    ProposedTransmissionStep,
    canonical_hash,
)
from market_impact_agent.agent_engine import AgentRunResult
from market_impact_agent.agent_ensemble import (
    EnsembleDisposition,
    EnsembleReason,
    ReplicateOutcome,
    agent_ensemble_decision_from_dict,
    aggregate_agent_replicates,
    execution_binding_hash,
)
from market_impact_agent.agent_schema import validate_agent_contract
from market_impact_agent.agent_study import load_agent_phase2_preregistration
from market_impact_agent.research import EvidenceTier, TransmissionDirectness
from market_impact_agent.runtime_store import RunStatus

CUTOFF = datetime(2026, 8, 25, 8, tzinfo=UTC)
REGISTRATION_PATH = Path("examples/calibration/agent-physical-energy-prospective-v1.json")
REGISTRY_PATH = Path("examples/research/a-share-energy-exposure-registry-v1.json")


def _pack() -> EvidencePack:
    return EvidencePack.build(
        event_id="ensemble-synthetic-event",
        as_of=CUTOFF,
        research_question="Which eligible synthetic target could be affected?",
        evidence=(
            EvidenceReference(
                evidence_id="support",
                claim_id="event-fact",
                source_ref="synthetic://support",
                source_tier=EvidenceTier.OFFICIAL,
                available_at=CUTOFF - timedelta(minutes=1),
                content_hash=sha256(b"support").hexdigest(),
                summary="Synthetic support evidence.",
            ),
            EvidenceReference(
                evidence_id="counter",
                claim_id="offset",
                source_ref="synthetic://counter",
                source_tier=EvidenceTier.PRIMARY,
                available_at=CUTOFF - timedelta(seconds=30),
                content_hash=sha256(b"counter").hexdigest(),
                summary="Synthetic counterevidence.",
            ),
        ),
        pattern_packs=(),
        allowed_targets=("600028.XSHG", "600938.XSHG"),
    )


def _proposal(
    *,
    target_id: str = "600028.XSHG",
    horizon_sessions: int = 3,
    confidence: float = 0.75,
    second_candidate: bool = False,
) -> JudgmentProposal:
    candidates = [
        CandidateImpact(
            target_id=target_id,
            direction=CandidateDirection.UP,
            horizon_sessions=horizon_sessions,
            directness=TransmissionDirectness.DIRECT,
            confidence=confidence,
            thesis="Synthetic bounded upside.",
            evidence_refs=("support",),
            counterevidence_refs=("counter",),
            invalidation_conditions=("synthetic fact is invalidated",),
        )
    ]
    if second_candidate:
        candidates.append(
            CandidateImpact(
                target_id="600938.XSHG",
                direction=CandidateDirection.UP,
                horizon_sessions=3,
                directness=TransmissionDirectness.SECOND_ORDER,
                confidence=0.7,
                thesis="A second eligible candidate creates an ambiguous vote.",
                evidence_refs=("support",),
                counterevidence_refs=(),
                invalidation_conditions=("synthetic linkage is invalidated",),
            )
        )
    return JudgmentProposal(
        event_id="ensemble-synthetic-event",
        decision=JudgmentDecision.PROPOSE,
        summary="A bounded synthetic Agent proposal.",
        transmission_steps=(
            ProposedTransmissionStep(
                step_id="synthetic-step",
                from_node="event",
                to_node=target_id,
                mechanism="synthetic transmission",
                directness=TransmissionDirectness.DIRECT,
                horizon_sessions=horizon_sessions,
                evidence_refs=("support",),
            ),
        ),
        candidates=tuple(candidates),
        blockers=(),
        unresolved_questions=("synthetic duration",),
        stopped_reason="minimum checks passed",
    )


def _artifact(
    index: int,
    *,
    target_id: str = "600028.XSHG",
    horizon_sessions: int = 3,
    runtime_marker: str = "shared-runtime",
    second_candidate: bool = False,
) -> JudgmentArtifact:
    evidence_pack = _pack()
    return JudgmentArtifact.build(
        run_id=f"ensemble-replicate-{index}",
        evidence_pack_id=evidence_pack.pack_id,
        provider_id="minimax-openai-compatible",
        model="MiniMax-M3",
        runtime_config_hash=sha256(runtime_marker.encode()).hexdigest(),
        prompt_hash=sha256(b"shared-prompt").hexdigest(),
        skill_hashes=(sha256(b"shared-skill").hexdigest(),),
        tool_manifest_hashes=(sha256(b"shared-tool").hexdigest(),),
        tool_surface_hash=sha256(b"shared-tool-surface").hexdigest(),
        mcp_server_hashes=(),
        context_estimator_id="provider-request-utf8-upper-bound-v2:1",
        compactor_id="deterministic-semantic-context-v2",
        journal_hash=sha256(f"journal-{index}".encode()).hexdigest(),
        transcript_hash=sha256(f"transcript-{index}".encode()).hexdigest(),
        raw_response_hash=sha256(f"response-{index}".encode()).hexdigest(),
        started_at=CUTOFF,
        finished_at=CUTOFF + timedelta(seconds=index),
        proposal=_proposal(
            target_id=target_id,
            horizon_sessions=horizon_sessions,
            second_candidate=second_candidate,
        ),
    )


def _result(artifact: JudgmentArtifact) -> AgentRunResult:
    return AgentRunResult(
        run_id=artifact.run_id,
        status=RunStatus.COMPLETED,
        judgment=artifact,
        terminal_store_hash=canonical_hash(artifact.to_dict()),
        metrics=None,
    )


def _registration():
    registration, _ = load_agent_phase2_preregistration(
        REGISTRATION_PATH,
        REGISTRY_PATH,
    )
    return registration


def _frozen_binding(artifact: JudgmentArtifact | None) -> str:
    assert artifact is not None
    return execution_binding_hash(
        artifact,
        runtime_ref=_registration().agent_protocol.runtime_ref,
    )


def test_exact_three_of_five_is_content_bound_and_schema_valid() -> None:
    results = tuple(
        _result(
            _artifact(
                index,
                target_id="600028.XSHG" if index <= 3 else "600938.XSHG",
            )
        )
        for index in range(1, 6)
    )

    decision = aggregate_agent_replicates(
        ensemble_run_id="ensemble-1",
        registration=_registration(),
        evidence_pack=_pack(),
        results=results,
        frozen_execution_binding_hash=_frozen_binding(results[0].judgment),
    )

    assert decision.disposition is EnsembleDisposition.PROPOSE
    assert decision.reason is EnsembleReason.THREE_OF_FIVE_AGREEMENT
    assert decision.selected_vote is not None
    assert decision.selected_vote.key == ("600028.XSHG", "up", 3)
    assert decision.agreement_count == 3
    assert len(decision.agreeing_judgment_artifact_ids) == 3
    assert agent_ensemble_decision_from_dict(decision.to_dict()) == decision
    assert validate_agent_contract(decision.to_dict(), "agent-ensemble-decision.schema.json") == ()


def test_no_three_matching_votes_abstains() -> None:
    choices = (
        ("600028.XSHG", 1),
        ("600028.XSHG", 3),
        ("600028.XSHG", 10),
        ("600938.XSHG", 1),
        ("600938.XSHG", 3),
    )
    results = tuple(
        _result(_artifact(index, target_id=target_id, horizon_sessions=horizon))
        for index, (target_id, horizon) in enumerate(choices, start=1)
    )

    decision = aggregate_agent_replicates(
        ensemble_run_id="ensemble-no-agreement",
        registration=_registration(),
        evidence_pack=_pack(),
        results=results,
        frozen_execution_binding_hash=_frozen_binding(results[0].judgment),
    )

    assert decision.disposition is EnsembleDisposition.ABSTAIN
    assert decision.reason is EnsembleReason.NO_THREE_OF_FIVE_AGREEMENT
    assert decision.agreement_count == 1


def test_ambiguous_replicate_is_invalid_but_three_other_votes_can_agree() -> None:
    results = (
        _result(_artifact(1, second_candidate=True)),
        *tuple(_result(_artifact(index)) for index in range(2, 5)),
        _result(_artifact(5, target_id="600938.XSHG")),
    )

    decision = aggregate_agent_replicates(
        ensemble_run_id="ensemble-one-invalid",
        registration=_registration(),
        evidence_pack=_pack(),
        results=results,
        frozen_execution_binding_hash=_frozen_binding(results[1].judgment),
    )

    assert decision.assessments[0].outcome is ReplicateOutcome.INVALID
    assert decision.assessments[0].reason == "eligible_candidate_count_not_one"
    assert decision.disposition is EnsembleDisposition.PROPOSE
    assert decision.agreement_count == 3


def test_budget_exhausted_replicate_is_recorded_without_erasing_three_votes() -> None:
    results = (
        AgentRunResult(
            run_id="ensemble-replicate-1",
            status=RunStatus.BUDGET_EXHAUSTED,
            judgment=None,
            terminal_store_hash=None,
            metrics=None,
        ),
        *tuple(_result(_artifact(index)) for index in range(2, 5)),
        _result(_artifact(5, target_id="600938.XSHG")),
    )

    decision = aggregate_agent_replicates(
        ensemble_run_id="ensemble-budget-exhausted",
        registration=_registration(),
        evidence_pack=_pack(),
        results=results,
        frozen_execution_binding_hash=_frozen_binding(results[1].judgment),
    )

    assert decision.assessments[0].outcome is ReplicateOutcome.INVALID
    assert decision.assessments[0].reason == "run_budget_exhausted"
    assert decision.disposition is EnsembleDisposition.PROPOSE
    assert decision.agreement_count == 3


def test_execution_binding_mismatch_forces_fail_closed_abstention() -> None:
    results = tuple(
        _result(
            _artifact(
                index,
                runtime_marker="different-runtime" if index == 5 else "shared-runtime",
            )
        )
        for index in range(1, 6)
    )

    decision = aggregate_agent_replicates(
        ensemble_run_id="ensemble-binding-mismatch",
        registration=_registration(),
        evidence_pack=_pack(),
        results=results,
        frozen_execution_binding_hash=_frozen_binding(results[0].judgment),
    )

    assert decision.agreement_count == 5
    assert decision.disposition is EnsembleDisposition.ABSTAIN
    assert decision.reason is EnsembleReason.EXECUTION_BINDING_MISMATCH


def test_reused_artifact_forces_fail_closed_abstention() -> None:
    reused = _artifact(1)
    results = (
        _result(reused),
        AgentRunResult(
            run_id="ensemble-replicate-2",
            status=RunStatus.COMPLETED,
            judgment=reused,
            terminal_store_hash=canonical_hash(reused.to_dict()),
            metrics=None,
        ),
        *tuple(_result(_artifact(index)) for index in range(3, 6)),
    )

    decision = aggregate_agent_replicates(
        ensemble_run_id="ensemble-reused-artifact",
        registration=_registration(),
        evidence_pack=_pack(),
        results=results,
        frozen_execution_binding_hash=_frozen_binding(reused),
    )

    assert decision.disposition is EnsembleDisposition.ABSTAIN
    assert decision.reason is EnsembleReason.REPLICATE_ARTIFACT_REUSED


def test_parser_rejects_a_rehashed_result_inconsistent_with_assessments() -> None:
    results = tuple(_result(_artifact(index)) for index in range(1, 6))
    decision = aggregate_agent_replicates(
        ensemble_run_id="ensemble-tamper",
        registration=_registration(),
        evidence_pack=_pack(),
        results=results,
        frozen_execution_binding_hash=_frozen_binding(results[0].judgment),
    )
    payload = deepcopy(decision.to_dict())
    payload["disposition"] = "abstain"
    payload["reason"] = "no_three_of_five_agreement"
    payload["selected_vote"] = None
    payload["agreeing_judgment_artifact_ids"] = []
    core = deepcopy(payload)
    core.pop("decision_id")
    payload["decision_id"] = f"agent-ensemble-{canonical_hash(core)}"

    with pytest.raises(ValueError, match="does not match assessments"):
        agent_ensemble_decision_from_dict(payload)


def test_registration_validation_rejects_rehashed_votes_outside_protocol() -> None:
    results = tuple(_result(_artifact(index, target_id="600938.XSHG")) for index in range(1, 6))
    registration, registry = load_agent_phase2_preregistration(
        REGISTRATION_PATH,
        REGISTRY_PATH,
    )
    evidence_pack = _pack()
    decision = aggregate_agent_replicates(
        ensemble_run_id="ensemble-direction-tamper",
        registration=registration,
        evidence_pack=evidence_pack,
        results=results,
        frozen_execution_binding_hash=_frozen_binding(results[0].judgment),
    )
    payload = deepcopy(decision.to_dict())
    assessments = payload["assessments"]
    assert isinstance(assessments, list)
    for assessment_value in cast(list[object], assessments):
        assert isinstance(assessment_value, dict)
        assessment = cast(dict[str, object], assessment_value)
        vote = assessment["vote"]
        assert isinstance(vote, dict)
        cast(dict[str, object], vote)["direction"] = "mixed"
    selected_vote = payload["selected_vote"]
    assert isinstance(selected_vote, dict)
    cast(dict[str, object], selected_vote)["direction"] = "mixed"
    core = deepcopy(payload)
    core.pop("decision_id")
    payload["decision_id"] = f"agent-ensemble-{canonical_hash(core)}"
    parsed = agent_ensemble_decision_from_dict(payload)

    with pytest.raises(ValueError, match="vote outside the protocol"):
        parsed.validate_against(registration, evidence_pack, registry)
