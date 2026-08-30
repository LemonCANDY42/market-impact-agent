from __future__ import annotations

import json
import math
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from hashlib import sha256
from typing import cast

from market_impact_agent.domain import Side, SignalIntent, require_aware
from market_impact_agent.research import EvidenceTier, TransmissionDirectness

EVIDENCE_PACK_SCHEMA = "market-impact.evidence-pack.v1"
PATTERN_PACK_SCHEMA = "market-impact.pattern-pack.v1"
JUDGMENT_ARTIFACT_SCHEMA = "market-impact.judgment-artifact.v2"


class CandidateDirection(StrEnum):
    UP = "up"
    DOWN = "down"
    MIXED = "mixed"
    UNKNOWN = "unknown"


class JudgmentDecision(StrEnum):
    PROPOSE = "propose"
    ABSTAIN = "abstain"


@dataclass(frozen=True, slots=True)
class ProspectiveEvidenceLineage:
    snapshot_id: str
    observation_id: str
    checkpoint_decision_input_id: str

    def __post_init__(self) -> None:
        _prefixed_hash(self.snapshot_id, "data-snapshot-", "prospective snapshot_id")
        _prefixed_hash(
            self.observation_id,
            "source-observation-",
            "prospective observation_id",
        )
        _prefixed_hash(
            self.checkpoint_decision_input_id,
            "checkpoint-decision-input-",
            "prospective checkpoint decision input ID",
        )

    def to_dict(self) -> dict[str, str]:
        return {
            "snapshot_id": self.snapshot_id,
            "observation_id": self.observation_id,
            "checkpoint_decision_input_id": self.checkpoint_decision_input_id,
        }


@dataclass(frozen=True, slots=True)
class EvidenceReference:
    evidence_id: str
    claim_id: str
    source_ref: str
    source_tier: EvidenceTier
    available_at: datetime
    content_hash: str
    summary: str
    untrusted_text: bool = True
    prospective_lineage: ProspectiveEvidenceLineage | None = None

    def __post_init__(self) -> None:
        for name in ("evidence_id", "claim_id", "source_ref", "summary"):
            _nonempty(getattr(self, name), name)
        require_aware(self.available_at, "available_at")
        _sha256(self.content_hash, "content_hash")

    def to_dict(self) -> dict[str, object]:
        payload: dict[str, object] = {
            "evidence_id": self.evidence_id,
            "claim_id": self.claim_id,
            "source_ref": self.source_ref,
            "source_tier": self.source_tier.value,
            "available_at": _timestamp(self.available_at),
            "content_hash": self.content_hash,
            "summary": self.summary,
            "untrusted_text": self.untrusted_text,
        }
        if self.prospective_lineage is not None:
            payload["prospective_lineage"] = self.prospective_lineage.to_dict()
        return payload


@dataclass(frozen=True, slots=True)
class PatternEntry:
    pattern_id: str
    mechanism: str
    transmission_scales: tuple[str, ...]
    applicability_conditions: tuple[str, ...]
    counterexamples: tuple[str, ...]
    evidence_refs: tuple[str, ...]

    def __post_init__(self) -> None:
        _nonempty(self.pattern_id, "pattern_id")
        _nonempty(self.mechanism, "mechanism")
        _unique_nonempty(self.transmission_scales, "transmission_scales")
        _unique_nonempty(self.applicability_conditions, "applicability_conditions")
        _unique_nonempty(self.counterexamples, "counterexamples")
        _unique_nonempty(self.evidence_refs, "evidence_refs")

    def to_dict(self) -> dict[str, object]:
        return {
            "pattern_id": self.pattern_id,
            "mechanism": self.mechanism,
            "transmission_scales": list(self.transmission_scales),
            "applicability_conditions": list(self.applicability_conditions),
            "counterexamples": list(self.counterexamples),
            "evidence_refs": list(self.evidence_refs),
        }


@dataclass(frozen=True, slots=True)
class PatternPack:
    pack_id: str
    version: str
    available_at: datetime
    entries: tuple[PatternEntry, ...]

    def __post_init__(self) -> None:
        _nonempty(self.version, "version")
        require_aware(self.available_at, "available_at")
        if not self.entries:
            raise ValueError("pattern packs require at least one entry")
        _unique(tuple(item.pattern_id for item in self.entries), "pattern_id")
        if self.pack_id != self.expected_pack_id:
            raise ValueError("pattern pack_id does not match content")

    @property
    def expected_pack_id(self) -> str:
        return f"pattern-{canonical_hash(self.core_dict())}"

    def core_dict(self) -> dict[str, object]:
        return {
            "schema_version": PATTERN_PACK_SCHEMA,
            "version": self.version,
            "available_at": _timestamp(self.available_at),
            "entries": [item.to_dict() for item in self.entries],
        }

    def to_dict(self) -> dict[str, object]:
        return {**self.core_dict(), "pack_id": self.pack_id}

    @classmethod
    def build(
        cls,
        *,
        version: str,
        available_at: datetime,
        entries: tuple[PatternEntry, ...],
    ) -> PatternPack:
        core = {
            "schema_version": PATTERN_PACK_SCHEMA,
            "version": version,
            "available_at": _timestamp(available_at),
            "entries": [item.to_dict() for item in entries],
        }
        return cls(
            pack_id=f"pattern-{canonical_hash(core)}",
            version=version,
            available_at=available_at,
            entries=entries,
        )


@dataclass(frozen=True, slots=True)
class PatternPackReference:
    pack_id: str
    version: str
    available_at: datetime
    content_hash: str

    def __post_init__(self) -> None:
        _nonempty(self.pack_id, "pattern pack_id")
        _nonempty(self.version, "pattern version")
        require_aware(self.available_at, "pattern available_at")
        _sha256(self.content_hash, "pattern content_hash")

    def to_dict(self) -> dict[str, object]:
        return {
            "pack_id": self.pack_id,
            "version": self.version,
            "available_at": _timestamp(self.available_at),
            "content_hash": self.content_hash,
        }


@dataclass(frozen=True, slots=True)
class EvidencePack:
    pack_id: str
    event_id: str
    as_of: datetime
    research_question: str
    evidence: tuple[EvidenceReference, ...]
    pattern_packs: tuple[PatternPackReference, ...]
    allowed_targets: tuple[str, ...]
    data_gaps: tuple[str, ...]

    def __post_init__(self) -> None:
        _nonempty(self.event_id, "event_id")
        _nonempty(self.research_question, "research_question")
        require_aware(self.as_of, "as_of")
        if not self.evidence:
            raise ValueError("evidence packs require at least one evidence reference")
        _unique(tuple(item.evidence_id for item in self.evidence), "evidence_id")
        _unique_nonempty(self.allowed_targets, "allowed_targets")
        _unique(self.data_gaps, "data_gaps")
        _unique(tuple(item.pack_id for item in self.pattern_packs), "pattern pack_id")
        if any(item.available_at > self.as_of for item in self.evidence):
            raise ValueError("evidence pack contains future-available evidence")
        if any(item.available_at > self.as_of for item in self.pattern_packs):
            raise ValueError("evidence pack contains a future-available Pattern Pack")
        if self.pack_id != self.expected_pack_id:
            raise ValueError("evidence pack_id does not match content")

    @property
    def expected_pack_id(self) -> str:
        return f"evidence-pack-{canonical_hash(self.core_dict())}"

    def core_dict(self) -> dict[str, object]:
        return {
            "schema_version": EVIDENCE_PACK_SCHEMA,
            "event_id": self.event_id,
            "as_of": _timestamp(self.as_of),
            "research_question": self.research_question,
            "evidence": [item.to_dict() for item in self.evidence],
            "pattern_packs": [item.to_dict() for item in self.pattern_packs],
            "allowed_targets": list(self.allowed_targets),
            "data_gaps": list(self.data_gaps),
        }

    def to_dict(self) -> dict[str, object]:
        return {**self.core_dict(), "pack_id": self.pack_id}

    @classmethod
    def build(
        cls,
        *,
        event_id: str,
        as_of: datetime,
        research_question: str,
        evidence: tuple[EvidenceReference, ...],
        pattern_packs: tuple[PatternPackReference, ...],
        allowed_targets: tuple[str, ...],
        data_gaps: tuple[str, ...] = (),
    ) -> EvidencePack:
        core = {
            "schema_version": EVIDENCE_PACK_SCHEMA,
            "event_id": event_id,
            "as_of": _timestamp(as_of),
            "research_question": research_question,
            "evidence": [item.to_dict() for item in evidence],
            "pattern_packs": [item.to_dict() for item in pattern_packs],
            "allowed_targets": list(allowed_targets),
            "data_gaps": list(data_gaps),
        }
        return cls(
            pack_id=f"evidence-pack-{canonical_hash(core)}",
            event_id=event_id,
            as_of=as_of,
            research_question=research_question,
            evidence=evidence,
            pattern_packs=pattern_packs,
            allowed_targets=allowed_targets,
            data_gaps=data_gaps,
        )


@dataclass(frozen=True, slots=True)
class ProposedTransmissionStep:
    step_id: str
    from_node: str
    to_node: str
    mechanism: str
    directness: TransmissionDirectness
    horizon_sessions: int
    evidence_refs: tuple[str, ...]

    def __post_init__(self) -> None:
        for name in ("step_id", "from_node", "to_node", "mechanism"):
            _nonempty(getattr(self, name), name)
        if self.horizon_sessions < 1:
            raise ValueError("horizon_sessions must be positive")
        _unique_nonempty(self.evidence_refs, "transmission evidence_refs")

    def to_dict(self) -> dict[str, object]:
        return {
            "step_id": self.step_id,
            "from_node": self.from_node,
            "to_node": self.to_node,
            "mechanism": self.mechanism,
            "directness": self.directness.value,
            "horizon_sessions": self.horizon_sessions,
            "evidence_refs": list(self.evidence_refs),
        }


@dataclass(frozen=True, slots=True)
class CandidateImpact:
    target_id: str
    direction: CandidateDirection
    horizon_sessions: int
    directness: TransmissionDirectness
    confidence: float
    thesis: str
    evidence_refs: tuple[str, ...]
    counterevidence_refs: tuple[str, ...]
    invalidation_conditions: tuple[str, ...]

    def __post_init__(self) -> None:
        _nonempty(self.target_id, "target_id")
        _nonempty(self.thesis, "thesis")
        if self.horizon_sessions < 1:
            raise ValueError("horizon_sessions must be positive")
        if not math.isfinite(self.confidence) or not 0 <= self.confidence <= 1:
            raise ValueError("confidence must be finite and between zero and one")
        _unique_nonempty(self.evidence_refs, "candidate evidence_refs")
        _unique(self.counterevidence_refs, "candidate counterevidence_refs")
        _unique_nonempty(self.invalidation_conditions, "invalidation_conditions")
        if set(self.evidence_refs) & set(self.counterevidence_refs):
            raise ValueError("candidate support and counterevidence must not overlap")

    def to_dict(self) -> dict[str, object]:
        return {
            "target_id": self.target_id,
            "direction": self.direction.value,
            "horizon_sessions": self.horizon_sessions,
            "directness": self.directness.value,
            "confidence": self.confidence,
            "thesis": self.thesis,
            "evidence_refs": list(self.evidence_refs),
            "counterevidence_refs": list(self.counterevidence_refs),
            "invalidation_conditions": list(self.invalidation_conditions),
        }


@dataclass(frozen=True, slots=True)
class JudgmentProposal:
    event_id: str
    decision: JudgmentDecision
    summary: str
    transmission_steps: tuple[ProposedTransmissionStep, ...]
    candidates: tuple[CandidateImpact, ...]
    blockers: tuple[str, ...]
    unresolved_questions: tuple[str, ...]
    stopped_reason: str
    decision_confidence: float | None = None

    def __post_init__(self) -> None:
        _nonempty(self.event_id, "event_id")
        _nonempty(self.summary, "summary")
        _nonempty(self.stopped_reason, "stopped_reason")
        if self.decision_confidence is not None and (
            not math.isfinite(self.decision_confidence) or not 0 <= self.decision_confidence <= 1
        ):
            raise ValueError("decision_confidence must be finite and between zero and one")
        _unique(tuple(item.step_id for item in self.transmission_steps), "step_id")
        _unique(tuple(item.target_id for item in self.candidates), "candidate target_id")
        _unique(self.blockers, "blockers")
        _unique(self.unresolved_questions, "unresolved_questions")
        if self.decision is JudgmentDecision.PROPOSE and not self.candidates:
            raise ValueError("propose decisions require at least one candidate")
        if self.decision is JudgmentDecision.ABSTAIN and self.candidates:
            raise ValueError("abstain decisions cannot contain candidates")
        if self.decision is JudgmentDecision.ABSTAIN and not self.blockers:
            raise ValueError("abstain decisions require at least one blocker")

    def to_dict(self) -> dict[str, object]:
        payload: dict[str, object] = {
            "event_id": self.event_id,
            "decision": self.decision.value,
            "summary": self.summary,
            "transmission_steps": [item.to_dict() for item in self.transmission_steps],
            "candidates": [item.to_dict() for item in self.candidates],
            "blockers": list(self.blockers),
            "unresolved_questions": list(self.unresolved_questions),
            "stopped_reason": self.stopped_reason,
        }
        if self.decision_confidence is not None:
            payload["decision_confidence"] = self.decision_confidence
        return payload

    def validate_against(self, evidence_pack: EvidencePack) -> None:
        if self.event_id != evidence_pack.event_id:
            raise ValueError("judgment event_id must match the Evidence Pack")
        evidence_ids = {item.evidence_id for item in evidence_pack.evidence}
        allowed_targets = set(evidence_pack.allowed_targets)
        for step in self.transmission_steps:
            _known_refs(step.evidence_refs, evidence_ids)
        for candidate in self.candidates:
            if candidate.target_id not in allowed_targets:
                raise ValueError(
                    f"candidate target is outside the Evidence Pack: {candidate.target_id}"
                )
            _known_refs(candidate.evidence_refs, evidence_ids)
            _known_refs(candidate.counterevidence_refs, evidence_ids)


@dataclass(frozen=True, slots=True)
class JudgmentArtifact:
    artifact_id: str
    run_id: str
    evidence_pack_id: str
    provider_id: str
    model: str
    runtime_config_hash: str
    prompt_hash: str
    skill_hashes: tuple[str, ...]
    tool_manifest_hashes: tuple[str, ...]
    tool_surface_hash: str
    mcp_server_hashes: tuple[str, ...]
    context_estimator_id: str
    compactor_id: str
    journal_hash: str
    transcript_hash: str
    raw_response_hash: str
    started_at: datetime
    finished_at: datetime
    proposal: JudgmentProposal

    def __post_init__(self) -> None:
        for name in (
            "run_id",
            "evidence_pack_id",
            "provider_id",
            "model",
            "context_estimator_id",
            "compactor_id",
        ):
            _nonempty(getattr(self, name), name)
        for name in (
            "runtime_config_hash",
            "prompt_hash",
            "tool_surface_hash",
            "journal_hash",
            "transcript_hash",
            "raw_response_hash",
        ):
            _sha256(getattr(self, name), name)
        _unique(self.skill_hashes, "skill_hashes")
        _unique(self.tool_manifest_hashes, "tool_manifest_hashes")
        _unique(self.mcp_server_hashes, "mcp_server_hashes")
        for value in (
            *self.skill_hashes,
            *self.tool_manifest_hashes,
            *self.mcp_server_hashes,
        ):
            _sha256(value, "manifest hash")
        require_aware(self.started_at, "started_at")
        require_aware(self.finished_at, "finished_at")
        if self.finished_at < self.started_at:
            raise ValueError("finished_at must not be before started_at")
        if self.artifact_id != self.expected_artifact_id:
            raise ValueError("judgment artifact_id does not match content")

    @property
    def expected_artifact_id(self) -> str:
        return f"judgment-{canonical_hash(self.core_dict())}"

    def core_dict(self) -> dict[str, object]:
        return {
            "schema_version": JUDGMENT_ARTIFACT_SCHEMA,
            "run_id": self.run_id,
            "evidence_pack_id": self.evidence_pack_id,
            "provider_id": self.provider_id,
            "model": self.model,
            "runtime_config_hash": self.runtime_config_hash,
            "prompt_hash": self.prompt_hash,
            "skill_hashes": list(self.skill_hashes),
            "tool_manifest_hashes": list(self.tool_manifest_hashes),
            "tool_surface_hash": self.tool_surface_hash,
            "mcp_server_hashes": list(self.mcp_server_hashes),
            "context_estimator_id": self.context_estimator_id,
            "compactor_id": self.compactor_id,
            "journal_hash": self.journal_hash,
            "transcript_hash": self.transcript_hash,
            "raw_response_hash": self.raw_response_hash,
            "started_at": _timestamp(self.started_at),
            "finished_at": _timestamp(self.finished_at),
            "proposal": self.proposal.to_dict(),
        }

    def to_dict(self) -> dict[str, object]:
        return {**self.core_dict(), "artifact_id": self.artifact_id}

    @classmethod
    def build(
        cls,
        *,
        run_id: str,
        evidence_pack_id: str,
        provider_id: str,
        model: str,
        runtime_config_hash: str,
        prompt_hash: str,
        skill_hashes: tuple[str, ...],
        tool_manifest_hashes: tuple[str, ...],
        tool_surface_hash: str,
        mcp_server_hashes: tuple[str, ...],
        context_estimator_id: str,
        compactor_id: str,
        journal_hash: str,
        transcript_hash: str,
        raw_response_hash: str,
        started_at: datetime,
        finished_at: datetime,
        proposal: JudgmentProposal,
    ) -> JudgmentArtifact:
        core = {
            "schema_version": JUDGMENT_ARTIFACT_SCHEMA,
            "run_id": run_id,
            "evidence_pack_id": evidence_pack_id,
            "provider_id": provider_id,
            "model": model,
            "runtime_config_hash": runtime_config_hash,
            "prompt_hash": prompt_hash,
            "skill_hashes": list(skill_hashes),
            "tool_manifest_hashes": list(tool_manifest_hashes),
            "tool_surface_hash": tool_surface_hash,
            "mcp_server_hashes": list(mcp_server_hashes),
            "context_estimator_id": context_estimator_id,
            "compactor_id": compactor_id,
            "journal_hash": journal_hash,
            "transcript_hash": transcript_hash,
            "raw_response_hash": raw_response_hash,
            "started_at": _timestamp(started_at),
            "finished_at": _timestamp(finished_at),
            "proposal": proposal.to_dict(),
        }
        return cls(
            artifact_id=f"judgment-{canonical_hash(core)}",
            run_id=run_id,
            evidence_pack_id=evidence_pack_id,
            provider_id=provider_id,
            model=model,
            runtime_config_hash=runtime_config_hash,
            prompt_hash=prompt_hash,
            skill_hashes=skill_hashes,
            tool_manifest_hashes=tool_manifest_hashes,
            tool_surface_hash=tool_surface_hash,
            mcp_server_hashes=mcp_server_hashes,
            context_estimator_id=context_estimator_id,
            compactor_id=compactor_id,
            journal_hash=journal_hash,
            transcript_hash=transcript_hash,
            raw_response_hash=raw_response_hash,
            started_at=started_at,
            finished_at=finished_at,
            proposal=proposal,
        )

    def validate_against(self, evidence_pack: EvidencePack) -> None:
        if self.evidence_pack_id != evidence_pack.pack_id:
            raise ValueError("judgment artifact references a different Evidence Pack")
        self.proposal.validate_against(evidence_pack)


def admit_candidate_to_signal(
    *,
    artifact: JudgmentArtifact,
    evidence_pack: EvidencePack,
    target_id: str,
    valid_from: datetime,
    expires_at: datetime,
    minimum_confidence: float,
) -> SignalIntent:
    artifact.validate_against(evidence_pack)
    if artifact.proposal.decision is JudgmentDecision.ABSTAIN:
        raise ValueError("an abstaining Judgment Artifact cannot create a Signal Intent")
    if not 0 <= minimum_confidence <= 1:
        raise ValueError("minimum_confidence must be between zero and one")
    candidate = next(
        (item for item in artifact.proposal.candidates if item.target_id == target_id),
        None,
    )
    if candidate is None:
        raise ValueError("target_id is not proposed by the Judgment Artifact")
    if candidate.direction in {CandidateDirection.MIXED, CandidateDirection.UNKNOWN}:
        raise ValueError("mixed or unknown candidate direction cannot create a Signal Intent")
    if candidate.confidence < minimum_confidence:
        raise ValueError("candidate confidence is below the deterministic admission threshold")
    side = Side.BUY if candidate.direction is CandidateDirection.UP else Side.SELL
    signal_core = {
        "judgment_artifact_id": artifact.artifact_id,
        "target_id": target_id,
        "side": side.value,
        "valid_from": _timestamp(valid_from),
        "expires_at": _timestamp(expires_at),
    }
    evidence_refs = tuple(
        sorted(set(candidate.evidence_refs) | set(candidate.counterevidence_refs))
    )
    return SignalIntent(
        signal_id=f"signal-{canonical_hash(signal_core)}",
        event_id=evidence_pack.event_id,
        instrument_id=target_id,
        side=side,
        valid_from=valid_from,
        expires_at=expires_at,
        evidence_refs=evidence_refs,
        invalidation_conditions=candidate.invalidation_conditions,
    )


def judgment_proposal_from_dict(value: object) -> JudgmentProposal:
    payload = _mapping(value, "judgment proposal")
    steps = tuple(
        ProposedTransmissionStep(
            step_id=_string(item, "step_id"),
            from_node=_string(item, "from_node"),
            to_node=_string(item, "to_node"),
            mechanism=_string(item, "mechanism"),
            directness=_enum(
                TransmissionDirectness,
                item.get("directness"),
                "transmission directness",
            ),
            horizon_sessions=_integer(item.get("horizon_sessions"), "horizon_sessions"),
            evidence_refs=tuple(_string_list(item.get("evidence_refs"), "evidence_refs")),
        )
        for item in _mapping_list(payload.get("transmission_steps"), "transmission_steps")
    )
    candidates = tuple(
        CandidateImpact(
            target_id=_string(item, "target_id"),
            direction=_enum(CandidateDirection, item.get("direction"), "direction"),
            horizon_sessions=_integer(item.get("horizon_sessions"), "horizon_sessions"),
            directness=_enum(TransmissionDirectness, item.get("directness"), "directness"),
            confidence=_number(item.get("confidence"), "confidence"),
            thesis=_string(item, "thesis"),
            evidence_refs=tuple(_string_list(item.get("evidence_refs"), "evidence_refs")),
            counterevidence_refs=tuple(
                _string_list(item.get("counterevidence_refs"), "counterevidence_refs")
            ),
            invalidation_conditions=tuple(
                _string_list(item.get("invalidation_conditions"), "invalidation_conditions")
            ),
        )
        for item in _mapping_list(payload.get("candidates"), "candidates")
    )
    proposal = JudgmentProposal(
        event_id=_string(payload, "event_id"),
        decision=_enum(JudgmentDecision, payload.get("decision"), "decision"),
        summary=_string(payload, "summary"),
        transmission_steps=steps,
        candidates=candidates,
        blockers=tuple(_string_list(payload.get("blockers"), "blockers")),
        unresolved_questions=tuple(
            _string_list(payload.get("unresolved_questions"), "unresolved_questions")
        ),
        stopped_reason=_string(payload, "stopped_reason"),
        decision_confidence=(
            None
            if "decision_confidence" not in payload
            else _number(payload.get("decision_confidence"), "decision_confidence")
        ),
    )
    if proposal.to_dict() != payload:
        raise ValueError("judgment proposal does not match the canonical contract")
    return proposal


def pattern_pack_from_dict(value: object) -> PatternPack:
    payload = _mapping(value, "Pattern Pack")
    if payload.get("schema_version") != PATTERN_PACK_SCHEMA:
        raise ValueError("unsupported Pattern Pack schema_version")
    entries = tuple(
        PatternEntry(
            pattern_id=_string(item, "pattern_id"),
            mechanism=_string(item, "mechanism"),
            transmission_scales=tuple(
                _string_list(item.get("transmission_scales"), "transmission_scales")
            ),
            applicability_conditions=tuple(
                _string_list(
                    item.get("applicability_conditions"),
                    "applicability_conditions",
                )
            ),
            counterexamples=tuple(_string_list(item.get("counterexamples"), "counterexamples")),
            evidence_refs=tuple(_string_list(item.get("evidence_refs"), "evidence_refs")),
        )
        for item in _mapping_list(payload.get("entries"), "entries")
    )
    pattern = PatternPack(
        pack_id=_string(payload, "pack_id"),
        version=_string(payload, "version"),
        available_at=_datetime(payload.get("available_at"), "available_at"),
        entries=entries,
    )
    if pattern.to_dict() != payload:
        raise ValueError("Pattern Pack does not match the canonical contract")
    return pattern


def evidence_pack_from_dict(value: object) -> EvidencePack:
    payload = _mapping(value, "Evidence Pack")
    if payload.get("schema_version") != EVIDENCE_PACK_SCHEMA:
        raise ValueError("unsupported Evidence Pack schema_version")
    evidence = tuple(
        EvidenceReference(
            evidence_id=_string(item, "evidence_id"),
            claim_id=_string(item, "claim_id"),
            source_ref=_string(item, "source_ref"),
            source_tier=_enum(EvidenceTier, item.get("source_tier"), "source_tier"),
            available_at=_datetime(item.get("available_at"), "available_at"),
            content_hash=_string(item, "content_hash"),
            summary=_string(item, "summary"),
            untrusted_text=_boolean(item.get("untrusted_text"), "untrusted_text"),
            prospective_lineage=(
                None
                if item.get("prospective_lineage") is None
                else _prospective_evidence_lineage(item.get("prospective_lineage"))
            ),
        )
        for item in _mapping_list(payload.get("evidence"), "evidence")
    )
    pattern_packs = tuple(
        PatternPackReference(
            pack_id=_string(item, "pack_id"),
            version=_string(item, "version"),
            available_at=_datetime(item.get("available_at"), "available_at"),
            content_hash=_string(item, "content_hash"),
        )
        for item in _mapping_list(payload.get("pattern_packs"), "pattern_packs")
    )
    pack = EvidencePack(
        pack_id=_string(payload, "pack_id"),
        event_id=_string(payload, "event_id"),
        as_of=_datetime(payload.get("as_of"), "as_of"),
        research_question=_string(payload, "research_question"),
        evidence=evidence,
        pattern_packs=pattern_packs,
        allowed_targets=tuple(_string_list(payload.get("allowed_targets"), "allowed_targets")),
        data_gaps=tuple(_string_list(payload.get("data_gaps"), "data_gaps")),
    )
    if pack.to_dict() != payload:
        raise ValueError("Evidence Pack does not match the canonical contract")
    return pack


def _prospective_evidence_lineage(value: object) -> ProspectiveEvidenceLineage:
    payload = _mapping(value, "prospective evidence lineage")
    return ProspectiveEvidenceLineage(
        snapshot_id=_string(payload, "snapshot_id"),
        observation_id=_string(payload, "observation_id"),
        checkpoint_decision_input_id=_string(payload, "checkpoint_decision_input_id"),
    )


def judgment_artifact_from_dict(value: object) -> JudgmentArtifact:
    payload = _mapping(value, "judgment artifact")
    if payload.get("schema_version") != JUDGMENT_ARTIFACT_SCHEMA:
        raise ValueError("unsupported Judgment Artifact schema_version")
    artifact = JudgmentArtifact(
        artifact_id=_string(payload, "artifact_id"),
        run_id=_string(payload, "run_id"),
        evidence_pack_id=_string(payload, "evidence_pack_id"),
        provider_id=_string(payload, "provider_id"),
        model=_string(payload, "model"),
        runtime_config_hash=_string(payload, "runtime_config_hash"),
        prompt_hash=_string(payload, "prompt_hash"),
        skill_hashes=tuple(_string_list(payload.get("skill_hashes"), "skill_hashes")),
        tool_manifest_hashes=tuple(
            _string_list(payload.get("tool_manifest_hashes"), "tool_manifest_hashes")
        ),
        tool_surface_hash=_string(payload, "tool_surface_hash"),
        mcp_server_hashes=tuple(
            _string_list(payload.get("mcp_server_hashes"), "mcp_server_hashes")
        ),
        context_estimator_id=_string(payload, "context_estimator_id"),
        compactor_id=_string(payload, "compactor_id"),
        journal_hash=_string(payload, "journal_hash"),
        transcript_hash=_string(payload, "transcript_hash"),
        raw_response_hash=_string(payload, "raw_response_hash"),
        started_at=_datetime(payload.get("started_at"), "started_at"),
        finished_at=_datetime(payload.get("finished_at"), "finished_at"),
        proposal=judgment_proposal_from_dict(payload.get("proposal")),
    )
    if artifact.to_dict() != payload:
        raise ValueError("judgment artifact does not match the canonical contract")
    return artifact


def canonical_hash(value: object) -> str:
    return sha256(canonical_json_bytes(value)).hexdigest()


def canonical_json_bytes(value: object) -> bytes:
    return json.dumps(
        _normalize(value),
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
        allow_nan=False,
    ).encode()


def _normalize(value: object) -> object:
    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("JSON floats must be finite")
        return value
    if isinstance(value, datetime):
        return _timestamp(value)
    if isinstance(value, StrEnum):
        return value.value
    if isinstance(value, Mapping):
        mapping = cast(Mapping[object, object], value)
        if any(not isinstance(key, str) for key in mapping):
            raise TypeError("JSON object keys must be strings")
        return {cast(str, key): _normalize(item) for key, item in mapping.items()}
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [_normalize(item) for item in cast(Sequence[object], value)]
    raise TypeError(f"unsupported JSON value: {type(value).__name__}")


def _known_refs(references: tuple[str, ...], known: set[str]) -> None:
    unknown = sorted(set(references) - known)
    if unknown:
        raise ValueError(f"unknown Evidence Pack reference: {', '.join(unknown)}")


def _timestamp(value: datetime) -> str:
    require_aware(value, "timestamp")
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _nonempty(value: str, name: str) -> None:
    if not value or value != value.strip():
        raise ValueError(f"{name} must be a non-empty trimmed string")


def _unique(values: tuple[str, ...], name: str) -> None:
    if len(values) != len(set(values)):
        raise ValueError(f"{name} values must be unique")
    if any(not value for value in values):
        raise ValueError(f"{name} values must not be empty")


def _unique_nonempty(values: tuple[str, ...], name: str) -> None:
    if not values:
        raise ValueError(f"{name} must not be empty")
    _unique(values, name)


def _sha256(value: str, name: str) -> None:
    if re.fullmatch(r"[0-9a-f]{64}", value) is None:
        raise ValueError(f"{name} must be a sha256 hex digest")


def _prefixed_hash(value: str, prefix: str, name: str) -> None:
    if not value.startswith(prefix):
        raise ValueError(f"{name} must start with {prefix}")
    _sha256(value.removeprefix(prefix), name)


def _mapping(value: object, name: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise TypeError(f"{name} must be an object with string keys")
    raw = cast(dict[object, object], value)
    if any(not isinstance(key, str) for key in raw):
        raise TypeError(f"{name} must be an object with string keys")
    return cast(dict[str, object], raw)


def _mapping_list(value: object, name: str) -> list[dict[str, object]]:
    if not isinstance(value, list):
        raise TypeError(f"{name} must be an array")
    return [_mapping(item, f"{name} item") for item in cast(list[object], value)]


def _string(payload: Mapping[str, object], name: str) -> str:
    value = payload.get(name)
    if not isinstance(value, str):
        raise TypeError(f"{name} must be a string")
    _nonempty(value, name)
    return value


def _string_list(value: object, name: str) -> list[str]:
    if not isinstance(value, list):
        raise TypeError(f"{name} must be an array of strings")
    raw = cast(list[object], value)
    if any(not isinstance(item, str) for item in raw):
        raise TypeError(f"{name} must be an array of strings")
    return cast(list[str], raw)


def _integer(value: object, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{name} must be an integer")
    return value


def _number(value: object, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"{name} must be a number")
    return float(value)


def _boolean(value: object, name: str) -> bool:
    if not isinstance(value, bool):
        raise TypeError(f"{name} must be a boolean")
    return value


def _datetime(value: object, name: str) -> datetime:
    if not isinstance(value, str):
        raise TypeError(f"{name} must be an ISO 8601 string")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{name} must be a valid ISO 8601 timestamp") from exc
    require_aware(parsed, name)
    return parsed.astimezone(UTC)


def _enum[T: StrEnum](enum_type: type[T], value: object, name: str) -> T:
    if not isinstance(value, str):
        raise TypeError(f"{name} must be a string")
    try:
        return enum_type(value)
    except ValueError as exc:
        raise ValueError(f"{name} has an unsupported value") from exc
