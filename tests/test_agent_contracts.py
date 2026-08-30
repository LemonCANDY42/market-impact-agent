from datetime import UTC, datetime, timedelta
from hashlib import sha256
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
    PatternEntry,
    PatternPack,
    PatternPackReference,
    ProposedTransmissionStep,
    admit_candidate_to_signal,
    canonical_hash,
    evidence_pack_from_dict,
    judgment_artifact_from_dict,
    judgment_proposal_from_dict,
    pattern_pack_from_dict,
)
from market_impact_agent.domain import Side
from market_impact_agent.research import EvidenceTier, TransmissionDirectness

NOW = datetime(2026, 8, 26, 4, tzinfo=UTC)


def evidence_ref(evidence_id: str = "official-outage") -> EvidenceReference:
    return EvidenceReference(
        evidence_id=evidence_id,
        claim_id="outage-status",
        source_ref="official://facility/outage",
        source_tier=EvidenceTier.OFFICIAL,
        available_at=NOW - timedelta(minutes=5),
        content_hash=sha256(evidence_id.encode()).hexdigest(),
        summary="Official operator reports a production outage.",
    )


def pattern_pack() -> PatternPack:
    return PatternPack.build(
        version="energy-supply-v1",
        available_at=NOW - timedelta(days=1),
        entries=(
            PatternEntry(
                pattern_id="physical-output-loss",
                mechanism="Lost output can tighten the physical commodity balance.",
                transmission_scales=("facility", "commodity", "industry"),
                applicability_conditions=("loss is material",),
                counterexamples=("spare capacity fully offsets the loss",),
                evidence_refs=("pattern-source-1",),
            ),
        ),
    )


def evidence_pack() -> EvidencePack:
    pattern = pattern_pack()
    return EvidencePack.build(
        event_id="energy-outage-1",
        as_of=NOW,
        research_question="Which eligible A-share exposures could be affected?",
        evidence=(evidence_ref(), evidence_ref("independent-market-price")),
        pattern_packs=(
            PatternPackReference(
                pack_id=pattern.pack_id,
                version=pattern.version,
                available_at=pattern.available_at,
                content_hash=canonical_hash(pattern.to_dict()),
            ),
        ),
        allowed_targets=("600028.XSHG",),
        data_gaps=("shipping confirmation is unavailable",),
    )


def proposal() -> JudgmentProposal:
    return JudgmentProposal(
        event_id="energy-outage-1",
        decision=JudgmentDecision.PROPOSE,
        summary="The outage may tighten supply, subject to offsetting capacity.",
        transmission_steps=(
            ProposedTransmissionStep(
                step_id="supply-step",
                from_node="facility-output",
                to_node="600028.XSHG",
                mechanism="physical output loss changes the commodity balance",
                directness=TransmissionDirectness.DIRECT,
                horizon_sessions=3,
                evidence_refs=("official-outage",),
            ),
        ),
        candidates=(
            CandidateImpact(
                target_id="600028.XSHG",
                direction=CandidateDirection.UP,
                horizon_sessions=3,
                directness=TransmissionDirectness.DIRECT,
                confidence=0.72,
                thesis="Higher benchmark prices may improve upstream realization.",
                evidence_refs=("official-outage",),
                counterevidence_refs=("independent-market-price",),
                invalidation_conditions=("spare capacity replaces lost output",),
            ),
        ),
        blockers=(),
        unresolved_questions=("duration of the outage",),
        stopped_reason="minimum fact, transmission, counterevidence, and cutoff checks passed",
        decision_confidence=0.68,
    )


def artifact(pack: EvidencePack | None = None) -> JudgmentArtifact:
    selected_pack = evidence_pack() if pack is None else pack
    return JudgmentArtifact.build(
        run_id="run-1",
        evidence_pack_id=selected_pack.pack_id,
        provider_id="minimax-openai-compatible",
        model="MiniMax-M3",
        runtime_config_hash=sha256(b"runtime").hexdigest(),
        prompt_hash=sha256(b"prompt").hexdigest(),
        skill_hashes=(sha256(b"skill").hexdigest(),),
        tool_manifest_hashes=(sha256(b"tool").hexdigest(),),
        tool_surface_hash=sha256(b"tool-surface").hexdigest(),
        mcp_server_hashes=(sha256(b"mcp").hexdigest(),),
        context_estimator_id="provider-request-utf8-upper-bound-v2:1",
        compactor_id="deterministic-semantic-context-v2",
        journal_hash=sha256(b"journal").hexdigest(),
        transcript_hash=sha256(b"transcript").hexdigest(),
        raw_response_hash=sha256(b"response").hexdigest(),
        started_at=NOW,
        finished_at=NOW + timedelta(seconds=1),
        proposal=proposal(),
    )


def test_evidence_pack_is_content_identified_and_rejects_future_inputs() -> None:
    pack = evidence_pack()

    assert pack.pack_id == pack.expected_pack_id
    assert pack.to_dict()["schema_version"] == "market-impact.evidence-pack.v1"
    assert evidence_pack_from_dict(pack.to_dict()) == pack

    future = EvidenceReference(
        evidence_id="future",
        claim_id="future-claim",
        source_ref="official://future",
        source_tier=EvidenceTier.OFFICIAL,
        available_at=NOW + timedelta(seconds=1),
        content_hash=sha256(b"future").hexdigest(),
        summary="This was not available at the cutoff.",
    )
    with pytest.raises(ValueError, match="future-available evidence"):
        EvidencePack.build(
            event_id="energy-outage-1",
            as_of=NOW,
            research_question="What was knowable?",
            evidence=(future,),
            pattern_packs=(),
            allowed_targets=("600028.XSHG",),
        )


def test_pattern_pack_is_versioned_and_point_in_time() -> None:
    pack = pattern_pack()

    assert pack.pack_id.startswith("pattern-")
    assert pack.pack_id == pack.expected_pack_id
    assert pack.to_dict()["version"] == "energy-supply-v1"
    assert pattern_pack_from_dict(pack.to_dict()) == pack


def test_judgment_proposal_roundtrips_and_rejects_unknown_evidence() -> None:
    parsed = judgment_proposal_from_dict(proposal().to_dict())

    assert parsed == proposal()
    parsed.validate_against(evidence_pack())

    invalid = parsed.to_dict()
    candidates = invalid["candidates"]
    assert isinstance(candidates, list)
    candidate = cast(list[object], candidates)[0]
    assert isinstance(candidate, dict)
    cast(dict[str, object], candidate)["evidence_refs"] = ["not-in-pack"]
    forged = judgment_proposal_from_dict(invalid)
    with pytest.raises(ValueError, match="unknown Evidence Pack reference"):
        forged.validate_against(evidence_pack())


def test_decision_confidence_is_optional_for_replay_but_bounded_when_present() -> None:
    current = proposal()
    assert judgment_proposal_from_dict(current.to_dict()).decision_confidence == 0.68

    legacy = current.to_dict()
    legacy.pop("decision_confidence")
    assert judgment_proposal_from_dict(legacy).decision_confidence is None

    with pytest.raises(ValueError, match="decision_confidence"):
        JudgmentProposal(
            event_id=current.event_id,
            decision=current.decision,
            summary=current.summary,
            transmission_steps=current.transmission_steps,
            candidates=current.candidates,
            blockers=current.blockers,
            unresolved_questions=current.unresolved_questions,
            stopped_reason=current.stopped_reason,
            decision_confidence=1.01,
        )


def test_abstention_requires_blockers_and_cannot_smuggle_candidates() -> None:
    with pytest.raises(ValueError, match="require at least one blocker"):
        JudgmentProposal(
            event_id="energy-outage-1",
            decision=JudgmentDecision.ABSTAIN,
            summary="Insufficient evidence.",
            transmission_steps=(),
            candidates=(),
            blockers=(),
            unresolved_questions=("outage duration",),
            stopped_reason="critical evidence is unavailable",
        )

    with pytest.raises(ValueError, match="cannot contain candidates"):
        JudgmentProposal(
            event_id="energy-outage-1",
            decision=JudgmentDecision.ABSTAIN,
            summary="Contradictory proposal.",
            transmission_steps=(),
            candidates=proposal().candidates,
            blockers=("critical evidence is unavailable",),
            unresolved_questions=(),
            stopped_reason="critical evidence is unavailable",
        )


def test_judgment_artifact_binds_runtime_and_admits_only_valid_candidate() -> None:
    pack = evidence_pack()
    judgment = artifact(pack)

    judgment.validate_against(pack)
    assert judgment.artifact_id == judgment.expected_artifact_id
    assert judgment_artifact_from_dict(judgment.to_dict()) == judgment

    signal = admit_candidate_to_signal(
        artifact=judgment,
        evidence_pack=pack,
        target_id="600028.XSHG",
        valid_from=NOW + timedelta(minutes=1),
        expires_at=NOW + timedelta(days=1),
        minimum_confidence=0.7,
    )

    assert signal.side is Side.BUY
    assert signal.event_id == pack.event_id
    assert signal.evidence_refs == ("independent-market-price", "official-outage")

    with pytest.raises(ValueError, match="below the deterministic admission threshold"):
        admit_candidate_to_signal(
            artifact=judgment,
            evidence_pack=pack,
            target_id="600028.XSHG",
            valid_from=NOW + timedelta(minutes=1),
            expires_at=NOW + timedelta(days=1),
            minimum_confidence=0.8,
        )
