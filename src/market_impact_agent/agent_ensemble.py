from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from enum import StrEnum
from typing import Protocol, cast

from market_impact_agent.agent_contracts import (
    CandidateDirection,
    EvidencePack,
    JudgmentArtifact,
    JudgmentDecision,
    canonical_hash,
)
from market_impact_agent.agent_study import (
    AgentPhase2Preregistration,
    ExposureRegistry,
)
from market_impact_agent.runtime_store import RunStatus

AGENT_ENSEMBLE_DECISION_SCHEMA = "market-impact.agent-ensemble-decision.v1"


class AgentReplicateResult(Protocol):
    @property
    def run_id(self) -> str: ...

    @property
    def status(self) -> RunStatus: ...

    @property
    def judgment(self) -> JudgmentArtifact | None: ...

    @property
    def terminal_store_hash(self) -> str | None: ...


class ReplicateOutcome(StrEnum):
    VOTE = "vote"
    ABSTAIN = "abstain"
    INVALID = "invalid"


class EnsembleDisposition(StrEnum):
    PROPOSE = "propose"
    ABSTAIN = "abstain"


class EnsembleReason(StrEnum):
    THREE_OF_FIVE_AGREEMENT = "three_of_five_agreement"
    NO_THREE_OF_FIVE_AGREEMENT = "no_three_of_five_agreement"
    EXECUTION_BINDING_MISMATCH = "execution_binding_mismatch"
    REPLICATE_ARTIFACT_REUSED = "replicate_artifact_reused"


@dataclass(frozen=True, slots=True)
class EnsembleVote:
    target_id: str
    direction: CandidateDirection
    horizon_sessions: int

    def __post_init__(self) -> None:
        _nonempty(self.target_id, "ensemble vote target_id")
        if self.horizon_sessions < 1:
            raise ValueError("ensemble vote horizon_sessions must be positive")

    @property
    def key(self) -> tuple[str, str, int]:
        return self.target_id, self.direction.value, self.horizon_sessions

    def to_dict(self) -> dict[str, object]:
        return {
            "target_id": self.target_id,
            "direction": self.direction.value,
            "horizon_sessions": self.horizon_sessions,
        }


@dataclass(frozen=True, slots=True)
class ReplicateAssessment:
    replicate_index: int
    run_id: str
    run_status: RunStatus
    outcome: ReplicateOutcome
    reason: str
    terminal_artifact_hash: str | None
    judgment_artifact_id: str | None
    execution_binding_hash: str | None
    vote: EnsembleVote | None

    def __post_init__(self) -> None:
        if not 1 <= self.replicate_index <= 5:
            raise ValueError("replicate_index must be between one and five")
        _nonempty(self.run_id, "replicate run_id")
        _nonempty(self.reason, "replicate reason")
        for name in (
            "terminal_artifact_hash",
            "execution_binding_hash",
        ):
            value = cast(str | None, getattr(self, name))
            if value is not None:
                _sha256(value, name)
        if self.judgment_artifact_id is not None:
            if not self.judgment_artifact_id.startswith("judgment-"):
                raise ValueError("replicate judgment_artifact_id is invalid")
            _sha256(self.judgment_artifact_id.removeprefix("judgment-"), "artifact id hash")
        if self.outcome is ReplicateOutcome.VOTE:
            if (
                self.run_status is not RunStatus.COMPLETED
                or self.vote is None
                or self.judgment_artifact_id is None
                or self.execution_binding_hash is None
                or self.terminal_artifact_hash is None
                or self.reason != "eligible_vote"
            ):
                raise ValueError("voting replicate assessment is incomplete")
        elif self.outcome is ReplicateOutcome.ABSTAIN:
            if (
                self.run_status is not RunStatus.COMPLETED
                or self.vote is not None
                or self.judgment_artifact_id is None
                or self.execution_binding_hash is None
                or self.terminal_artifact_hash is None
                or self.reason != "agent_abstained"
            ):
                raise ValueError("abstaining replicate assessment is invalid")
        elif self.vote is not None:
            raise ValueError("non-voting replicate assessment cannot contain a vote")
        elif self.reason not in {
            "run_cancelled",
            "run_failed",
            "run_budget_exhausted",
            "run_human_input_required",
            "judgment_contract_invalid",
            "eligible_candidate_count_not_one",
        }:
            raise ValueError("invalid replicate assessment reason is unsupported")

    def to_dict(self) -> dict[str, object]:
        return {
            "replicate_index": self.replicate_index,
            "run_id": self.run_id,
            "run_status": self.run_status.value,
            "outcome": self.outcome.value,
            "reason": self.reason,
            "terminal_artifact_hash": self.terminal_artifact_hash,
            "judgment_artifact_id": self.judgment_artifact_id,
            "execution_binding_hash": self.execution_binding_hash,
            "vote": None if self.vote is None else self.vote.to_dict(),
        }


@dataclass(frozen=True, slots=True)
class AgentEnsembleDecision:
    decision_id: str
    ensemble_run_id: str
    registration_id: str
    registration_hash: str
    evidence_pack_id: str
    evidence_pack_hash: str
    provider_id: str
    model: str
    runtime_ref: str
    frozen_execution_binding_hash: str
    replicate_count: int
    minimum_agreement: int
    assessments: tuple[ReplicateAssessment, ...]
    disposition: EnsembleDisposition
    reason: EnsembleReason
    selected_vote: EnsembleVote | None
    agreement_count: int
    agreeing_judgment_artifact_ids: tuple[str, ...]
    execution_capability: str = "none"

    def __post_init__(self) -> None:
        for name in (
            "ensemble_run_id",
            "registration_id",
            "evidence_pack_id",
            "provider_id",
            "model",
            "runtime_ref",
        ):
            _nonempty(cast(str, getattr(self, name)), name)
        _sha256(self.registration_hash, "registration_hash")
        _sha256(self.evidence_pack_hash, "evidence_pack_hash")
        _sha256(
            self.frozen_execution_binding_hash,
            "frozen_execution_binding_hash",
        )
        if self.replicate_count != 5 or self.minimum_agreement != 3:
            raise ValueError("Agent Ensemble Decision must preserve three-of-five")
        if len(self.assessments) != self.replicate_count:
            raise ValueError("Agent Ensemble Decision requires exactly five assessments")
        if tuple(item.replicate_index for item in self.assessments) != tuple(range(1, 6)):
            raise ValueError("replicate assessments must use canonical index order")
        run_ids = tuple(item.run_id for item in self.assessments)
        if len(run_ids) != len(set(run_ids)):
            raise ValueError("replicate run_id values must be unique")
        if self.execution_capability != "none":
            raise ValueError("Agent Ensemble Decision cannot expose execution capability")
        self._validate_result()
        if self.decision_id != self.expected_decision_id:
            raise ValueError("Agent Ensemble Decision decision_id does not match content")

    @property
    def expected_decision_id(self) -> str:
        return f"agent-ensemble-{canonical_hash(self.core_dict())}"

    def core_dict(self) -> dict[str, object]:
        return {
            "schema_version": AGENT_ENSEMBLE_DECISION_SCHEMA,
            "ensemble_run_id": self.ensemble_run_id,
            "registration_id": self.registration_id,
            "registration_hash": self.registration_hash,
            "evidence_pack_id": self.evidence_pack_id,
            "evidence_pack_hash": self.evidence_pack_hash,
            "provider_id": self.provider_id,
            "model": self.model,
            "runtime_ref": self.runtime_ref,
            "frozen_execution_binding_hash": self.frozen_execution_binding_hash,
            "replicate_count": self.replicate_count,
            "minimum_agreement": self.minimum_agreement,
            "assessments": [item.to_dict() for item in self.assessments],
            "disposition": self.disposition.value,
            "reason": self.reason.value,
            "selected_vote": (None if self.selected_vote is None else self.selected_vote.to_dict()),
            "agreement_count": self.agreement_count,
            "agreeing_judgment_artifact_ids": list(self.agreeing_judgment_artifact_ids),
            "execution_capability": self.execution_capability,
        }

    def to_dict(self) -> dict[str, object]:
        return {**self.core_dict(), "decision_id": self.decision_id}

    def validate_against(
        self,
        registration: AgentPhase2Preregistration,
        evidence_pack: EvidencePack,
        registry: ExposureRegistry,
    ) -> None:
        registration.validate_against(registry)
        if (
            self.registration_id != registration.registration_id
            or self.registration_hash != registration.registration_hash
        ):
            raise ValueError("Agent Ensemble Decision does not match registration")
        if (
            self.evidence_pack_id != evidence_pack.pack_id
            or self.evidence_pack_hash != canonical_hash(evidence_pack.to_dict())
        ):
            raise ValueError("Agent Ensemble Decision does not match Evidence Pack")
        protocol = registration.agent_protocol
        votes = tuple(item.vote for item in self.assessments if item.vote is not None)
        eligible_targets = {
            item.instrument_id for item in registry.entries if item.selection_eligible
        }
        if (
            self.provider_id != protocol.provider_id
            or self.model != protocol.model
            or self.runtime_ref != protocol.runtime_ref
            or self.replicate_count != protocol.replicate_count
            or self.minimum_agreement != protocol.minimum_agreeing_replicates
        ):
            raise ValueError("Agent Ensemble Decision does not match frozen Agent protocol")
        if any(
            vote.target_id not in evidence_pack.allowed_targets
            or vote.target_id not in eligible_targets
            or vote.direction.value not in protocol.allowed_directions
            or vote.horizon_sessions not in protocol.eligible_horizons_sessions
            for vote in votes
        ):
            raise ValueError("Agent Ensemble Decision contains a vote outside the protocol")

    def _validate_result(self) -> None:
        tallies = _tallies(self.assessments)
        maximum = max(tallies.values(), default=0)
        if self.agreement_count != maximum:
            raise ValueError("Agent Ensemble Decision agreement_count is invalid")
        artifact_ids = tuple(
            item.judgment_artifact_id
            for item in self.assessments
            if item.judgment_artifact_id is not None
        )
        binding_hashes = {
            item.execution_binding_hash
            for item in self.assessments
            if item.execution_binding_hash is not None
        }
        if len(artifact_ids) != len(set(artifact_ids)):
            expected_reason = EnsembleReason.REPLICATE_ARTIFACT_REUSED
        elif binding_hashes and binding_hashes != {self.frozen_execution_binding_hash}:
            expected_reason = EnsembleReason.EXECUTION_BINDING_MISMATCH
        elif maximum >= self.minimum_agreement:
            expected_reason = EnsembleReason.THREE_OF_FIVE_AGREEMENT
        else:
            expected_reason = EnsembleReason.NO_THREE_OF_FIVE_AGREEMENT
        expected_disposition = (
            EnsembleDisposition.PROPOSE
            if expected_reason is EnsembleReason.THREE_OF_FIVE_AGREEMENT
            else EnsembleDisposition.ABSTAIN
        )
        if self.reason is not expected_reason or self.disposition is not expected_disposition:
            raise ValueError("Agent Ensemble Decision result does not match assessments")
        if expected_disposition is EnsembleDisposition.PROPOSE:
            if self.selected_vote is None or tallies.get(self.selected_vote.key) != maximum:
                raise ValueError("proposed Agent Ensemble Decision lacks valid agreement")
            expected_ids = tuple(
                item.judgment_artifact_id
                for item in self.assessments
                if item.vote == self.selected_vote
            )
            if (
                any(item is None for item in expected_ids)
                or tuple(cast(tuple[str, ...], expected_ids)) != self.agreeing_judgment_artifact_ids
            ):
                raise ValueError("agreeing Judgment Artifact identities are invalid")
        elif self.selected_vote is not None or self.agreeing_judgment_artifact_ids:
            raise ValueError("abstaining Agent Ensemble Decision contains a proposal")


def aggregate_agent_replicates(
    *,
    ensemble_run_id: str,
    registration: AgentPhase2Preregistration,
    evidence_pack: EvidencePack,
    results: tuple[AgentReplicateResult, ...],
    frozen_execution_binding_hash: str,
) -> AgentEnsembleDecision:
    _nonempty(ensemble_run_id, "ensemble_run_id")
    _sha256(frozen_execution_binding_hash, "frozen_execution_binding_hash")
    protocol = registration.agent_protocol
    if len(results) != protocol.replicate_count:
        raise ValueError("Agent ensemble requires exactly five replicate results")
    run_ids = tuple(item.run_id for item in results)
    if len(run_ids) != len(set(run_ids)):
        raise ValueError("Agent replicate run_id values must be unique")
    assessments = tuple(
        _assess_replicate(
            replicate_index=index,
            result=result,
            evidence_pack=evidence_pack,
            registration=registration,
        )
        for index, result in enumerate(results, start=1)
    )
    artifact_ids = tuple(
        item.judgment_artifact_id for item in assessments if item.judgment_artifact_id is not None
    )
    binding_hashes = {
        item.execution_binding_hash
        for item in assessments
        if item.execution_binding_hash is not None
    }
    forced_reason: EnsembleReason | None = None
    if len(artifact_ids) != len(set(artifact_ids)):
        forced_reason = EnsembleReason.REPLICATE_ARTIFACT_REUSED
    elif binding_hashes and binding_hashes != {frozen_execution_binding_hash}:
        forced_reason = EnsembleReason.EXECUTION_BINDING_MISMATCH
    tallies = _tallies(assessments)
    agreement_count = max(tallies.values(), default=0)
    selected_key = next(
        (
            key
            for key, count in sorted(tallies.items())
            if count >= protocol.minimum_agreeing_replicates
        ),
        None,
    )
    if forced_reason is None and selected_key is not None:
        selected_vote = EnsembleVote(
            target_id=selected_key[0],
            direction=CandidateDirection(selected_key[1]),
            horizon_sessions=selected_key[2],
        )
        agreeing_ids = tuple(
            cast(str, item.judgment_artifact_id)
            for item in assessments
            if item.vote == selected_vote
        )
        disposition = EnsembleDisposition.PROPOSE
        reason = EnsembleReason.THREE_OF_FIVE_AGREEMENT
    else:
        selected_vote = None
        agreeing_ids = ()
        disposition = EnsembleDisposition.ABSTAIN
        reason = forced_reason or EnsembleReason.NO_THREE_OF_FIVE_AGREEMENT
    core = {
        "schema_version": AGENT_ENSEMBLE_DECISION_SCHEMA,
        "ensemble_run_id": ensemble_run_id,
        "registration_id": registration.registration_id,
        "registration_hash": registration.registration_hash,
        "evidence_pack_id": evidence_pack.pack_id,
        "evidence_pack_hash": canonical_hash(evidence_pack.to_dict()),
        "provider_id": protocol.provider_id,
        "model": protocol.model,
        "runtime_ref": protocol.runtime_ref,
        "frozen_execution_binding_hash": frozen_execution_binding_hash,
        "replicate_count": protocol.replicate_count,
        "minimum_agreement": protocol.minimum_agreeing_replicates,
        "assessments": [item.to_dict() for item in assessments],
        "disposition": disposition.value,
        "reason": reason.value,
        "selected_vote": None if selected_vote is None else selected_vote.to_dict(),
        "agreement_count": agreement_count,
        "agreeing_judgment_artifact_ids": list(agreeing_ids),
        "execution_capability": "none",
    }
    return AgentEnsembleDecision(
        decision_id=f"agent-ensemble-{canonical_hash(core)}",
        ensemble_run_id=ensemble_run_id,
        registration_id=registration.registration_id,
        registration_hash=registration.registration_hash,
        evidence_pack_id=evidence_pack.pack_id,
        evidence_pack_hash=canonical_hash(evidence_pack.to_dict()),
        provider_id=protocol.provider_id,
        model=protocol.model,
        runtime_ref=protocol.runtime_ref,
        frozen_execution_binding_hash=frozen_execution_binding_hash,
        replicate_count=protocol.replicate_count,
        minimum_agreement=protocol.minimum_agreeing_replicates,
        assessments=assessments,
        disposition=disposition,
        reason=reason,
        selected_vote=selected_vote,
        agreement_count=agreement_count,
        agreeing_judgment_artifact_ids=agreeing_ids,
    )


def agent_ensemble_decision_from_dict(value: object) -> AgentEnsembleDecision:
    payload = _object(value, "Agent Ensemble Decision")
    expected = {
        "schema_version",
        "decision_id",
        "ensemble_run_id",
        "registration_id",
        "registration_hash",
        "evidence_pack_id",
        "evidence_pack_hash",
        "provider_id",
        "model",
        "runtime_ref",
        "frozen_execution_binding_hash",
        "replicate_count",
        "minimum_agreement",
        "assessments",
        "disposition",
        "reason",
        "selected_vote",
        "agreement_count",
        "agreeing_judgment_artifact_ids",
        "execution_capability",
    }
    if set(payload) != expected:
        raise ValueError("Agent Ensemble Decision fields are invalid")
    if _string(payload, "schema_version") != AGENT_ENSEMBLE_DECISION_SCHEMA:
        raise ValueError("unsupported Agent Ensemble Decision schema_version")
    assessments_raw = payload.get("assessments")
    if not isinstance(assessments_raw, list):
        raise TypeError("Agent Ensemble Decision assessments must be an array")
    assessment_values = cast(list[object], assessments_raw)
    vote_raw = payload.get("selected_vote")
    decision = AgentEnsembleDecision(
        decision_id=_string(payload, "decision_id"),
        ensemble_run_id=_string(payload, "ensemble_run_id"),
        registration_id=_string(payload, "registration_id"),
        registration_hash=_string(payload, "registration_hash"),
        evidence_pack_id=_string(payload, "evidence_pack_id"),
        evidence_pack_hash=_string(payload, "evidence_pack_hash"),
        provider_id=_string(payload, "provider_id"),
        model=_string(payload, "model"),
        runtime_ref=_string(payload, "runtime_ref"),
        frozen_execution_binding_hash=_string(
            payload,
            "frozen_execution_binding_hash",
        ),
        replicate_count=_integer(payload, "replicate_count"),
        minimum_agreement=_integer(payload, "minimum_agreement"),
        assessments=tuple(_assessment(item) for item in assessment_values),
        disposition=EnsembleDisposition(_string(payload, "disposition")),
        reason=EnsembleReason(_string(payload, "reason")),
        selected_vote=None if vote_raw is None else _vote(vote_raw),
        agreement_count=_integer(payload, "agreement_count"),
        agreeing_judgment_artifact_ids=_string_tuple(
            payload,
            "agreeing_judgment_artifact_ids",
        ),
        execution_capability=_string(payload, "execution_capability"),
    )
    if decision.to_dict() != payload:
        raise ValueError("Agent Ensemble Decision does not match canonical contract")
    return decision


def execution_binding_hash(
    artifact: JudgmentArtifact,
    *,
    runtime_ref: str,
) -> str:
    return canonical_hash(
        {
            "runtime_ref": runtime_ref,
            "runtime_config_hash": artifact.runtime_config_hash,
            "prompt_hash": artifact.prompt_hash,
            "skill_hashes": list(artifact.skill_hashes),
            "tool_manifest_hashes": list(artifact.tool_manifest_hashes),
            "tool_surface_hash": artifact.tool_surface_hash,
            "mcp_server_hashes": list(artifact.mcp_server_hashes),
            "context_estimator_id": artifact.context_estimator_id,
            "compactor_id": artifact.compactor_id,
        }
    )


def _assess_replicate(
    *,
    replicate_index: int,
    result: AgentReplicateResult,
    evidence_pack: EvidencePack,
    registration: AgentPhase2Preregistration,
) -> ReplicateAssessment:
    artifact = result.judgment
    if result.status is not RunStatus.COMPLETED or artifact is None:
        return ReplicateAssessment(
            replicate_index=replicate_index,
            run_id=result.run_id,
            run_status=result.status,
            outcome=ReplicateOutcome.INVALID,
            reason=f"run_{result.status.value}",
            terminal_artifact_hash=result.terminal_store_hash,
            judgment_artifact_id=None,
            execution_binding_hash=None,
            vote=None,
        )
    artifact_hash = canonical_hash(artifact.to_dict())
    protocol = registration.agent_protocol
    binding_hash = execution_binding_hash(artifact, runtime_ref=protocol.runtime_ref)
    try:
        artifact.validate_against(evidence_pack)
        if artifact.run_id != result.run_id:
            raise ValueError("Judgment Artifact run_id does not match result")
        if artifact.provider_id != protocol.provider_id or artifact.model != protocol.model:
            raise ValueError("Judgment Artifact Provider does not match protocol")
        if result.terminal_store_hash != artifact_hash:
            raise ValueError("Judgment Artifact store hash does not match content")
    except (TypeError, ValueError):
        return ReplicateAssessment(
            replicate_index=replicate_index,
            run_id=result.run_id,
            run_status=result.status,
            outcome=ReplicateOutcome.INVALID,
            reason="judgment_contract_invalid",
            terminal_artifact_hash=result.terminal_store_hash,
            judgment_artifact_id=artifact.artifact_id,
            execution_binding_hash=binding_hash,
            vote=None,
        )
    if artifact.proposal.decision is JudgmentDecision.ABSTAIN:
        return ReplicateAssessment(
            replicate_index=replicate_index,
            run_id=result.run_id,
            run_status=result.status,
            outcome=ReplicateOutcome.ABSTAIN,
            reason="agent_abstained",
            terminal_artifact_hash=artifact_hash,
            judgment_artifact_id=artifact.artifact_id,
            execution_binding_hash=binding_hash,
            vote=None,
        )
    eligible = tuple(
        item
        for item in artifact.proposal.candidates
        if item.target_id in evidence_pack.allowed_targets
        and item.direction.value in protocol.allowed_directions
        and item.horizon_sessions in protocol.eligible_horizons_sessions
        and Decimal(str(item.confidence)) >= protocol.minimum_candidate_confidence
    )
    if len(eligible) != 1:
        return ReplicateAssessment(
            replicate_index=replicate_index,
            run_id=result.run_id,
            run_status=result.status,
            outcome=ReplicateOutcome.INVALID,
            reason="eligible_candidate_count_not_one",
            terminal_artifact_hash=artifact_hash,
            judgment_artifact_id=artifact.artifact_id,
            execution_binding_hash=binding_hash,
            vote=None,
        )
    candidate = eligible[0]
    return ReplicateAssessment(
        replicate_index=replicate_index,
        run_id=result.run_id,
        run_status=result.status,
        outcome=ReplicateOutcome.VOTE,
        reason="eligible_vote",
        terminal_artifact_hash=artifact_hash,
        judgment_artifact_id=artifact.artifact_id,
        execution_binding_hash=binding_hash,
        vote=EnsembleVote(
            target_id=candidate.target_id,
            direction=candidate.direction,
            horizon_sessions=candidate.horizon_sessions,
        ),
    )


def _tallies(
    assessments: tuple[ReplicateAssessment, ...],
) -> dict[tuple[str, str, int], int]:
    tallies: dict[tuple[str, str, int], int] = {}
    for item in assessments:
        if item.vote is not None:
            tallies[item.vote.key] = tallies.get(item.vote.key, 0) + 1
    return tallies


def _assessment(value: object) -> ReplicateAssessment:
    payload = _object(value, "Replicate Assessment")
    expected = {
        "replicate_index",
        "run_id",
        "run_status",
        "outcome",
        "reason",
        "terminal_artifact_hash",
        "judgment_artifact_id",
        "execution_binding_hash",
        "vote",
    }
    if set(payload) != expected:
        raise ValueError("Replicate Assessment fields are invalid")
    vote_raw = payload.get("vote")
    return ReplicateAssessment(
        replicate_index=_integer(payload, "replicate_index"),
        run_id=_string(payload, "run_id"),
        run_status=RunStatus(_string(payload, "run_status")),
        outcome=ReplicateOutcome(_string(payload, "outcome")),
        reason=_string(payload, "reason"),
        terminal_artifact_hash=_nullable_string(payload, "terminal_artifact_hash"),
        judgment_artifact_id=_nullable_string(payload, "judgment_artifact_id"),
        execution_binding_hash=_nullable_string(payload, "execution_binding_hash"),
        vote=None if vote_raw is None else _vote(vote_raw),
    )


def _vote(value: object) -> EnsembleVote:
    payload = _object(value, "Ensemble Vote")
    if set(payload) != {"target_id", "direction", "horizon_sessions"}:
        raise ValueError("Ensemble Vote fields are invalid")
    return EnsembleVote(
        target_id=_string(payload, "target_id"),
        direction=CandidateDirection(_string(payload, "direction")),
        horizon_sessions=_integer(payload, "horizon_sessions"),
    )


def _object(value: object, name: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise TypeError(f"{name} must be an object")
    raw = cast(dict[object, object], value)
    if any(not isinstance(key, str) for key in raw):
        raise TypeError(f"{name} keys must be strings")
    return cast(dict[str, object], value)


def _string(payload: dict[str, object], field: str) -> str:
    value = payload.get(field)
    if not isinstance(value, str):
        raise TypeError(f"{field} must be a string")
    _nonempty(value, field)
    return value


def _nullable_string(payload: dict[str, object], field: str) -> str | None:
    value = payload.get(field)
    if value is None:
        return None
    if not isinstance(value, str):
        raise TypeError(f"{field} must be a string or null")
    _nonempty(value, field)
    return value


def _integer(payload: dict[str, object], field: str) -> int:
    value = payload.get(field)
    if not isinstance(value, int) or isinstance(value, bool):
        raise TypeError(f"{field} must be an integer")
    return value


def _string_tuple(payload: dict[str, object], field: str) -> tuple[str, ...]:
    value = payload.get(field)
    if not isinstance(value, list):
        raise TypeError(f"{field} must be an array of strings")
    values = cast(list[object], value)
    if any(not isinstance(item, str) for item in values):
        raise TypeError(f"{field} must be an array of strings")
    return tuple(cast(str, item) for item in values)


def _nonempty(value: str, name: str) -> None:
    if not value or value != value.strip():
        raise ValueError(f"{name} must be a non-empty trimmed string")


def _sha256(value: str, name: str) -> None:
    if len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
        raise ValueError(f"{name} must be a lowercase SHA-256")
