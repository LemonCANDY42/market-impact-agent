from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from market_impact_agent.agent_contracts import (
    CandidateImpact,
    EvidencePack,
    JudgmentArtifact,
    admit_candidate_to_signal,
    canonical_hash,
)
from market_impact_agent.agent_ensemble import (
    AgentEnsembleDecision,
    EnsembleDisposition,
    execution_binding_hash,
)
from market_impact_agent.agent_study import (
    AgentPhase2Preregistration,
    ExposureRegistry,
)
from market_impact_agent.backtests import BacktestRequest, SimulationSpec
from market_impact_agent.domain import Side, SignalIntent, require_aware


@dataclass(frozen=True, slots=True)
class JudgmentReplaySpec:
    target_id: str
    data_snapshot_id: str
    start_at: datetime
    end_at: datetime
    signal_expires_at: datetime
    market: str
    minimum_confidence: float
    simulation: SimulationSpec

    def __post_init__(self) -> None:
        for name in ("target_id", "data_snapshot_id", "market"):
            value = getattr(self, name)
            if not value or value != value.strip():
                raise ValueError(f"{name} must be a non-empty trimmed string")
        for name in ("start_at", "end_at", "signal_expires_at"):
            require_aware(getattr(self, name), name)
        if self.end_at <= self.start_at:
            raise ValueError("end_at must be after start_at")
        if self.signal_expires_at <= self.start_at:
            raise ValueError("signal_expires_at must be after start_at")
        if not 0 <= self.minimum_confidence <= 1:
            raise ValueError("minimum_confidence must be between zero and one")


@dataclass(frozen=True, slots=True)
class EnsembleReplaySpec:
    data_snapshot_id: str
    start_at: datetime
    end_at: datetime
    signal_expires_at: datetime
    market: str
    simulation: SimulationSpec

    def __post_init__(self) -> None:
        for name in ("data_snapshot_id", "market"):
            value = getattr(self, name)
            if not value or value != value.strip():
                raise ValueError(f"{name} must be a non-empty trimmed string")
        for name in ("start_at", "end_at", "signal_expires_at"):
            require_aware(getattr(self, name), name)
        if self.end_at <= self.start_at:
            raise ValueError("end_at must be after start_at")
        if self.signal_expires_at <= self.start_at:
            raise ValueError("signal_expires_at must be after start_at")


def build_judgment_backtest_request(
    *,
    artifact: JudgmentArtifact,
    evidence_pack: EvidencePack,
    spec: JudgmentReplaySpec,
) -> BacktestRequest:
    artifact.validate_against(evidence_pack)
    if spec.start_at < evidence_pack.as_of:
        raise ValueError("replay start_at must not be before the Evidence Pack cutoff")
    signal = admit_candidate_to_signal(
        artifact=artifact,
        evidence_pack=evidence_pack,
        target_id=spec.target_id,
        valid_from=evidence_pack.as_of,
        expires_at=spec.signal_expires_at,
        minimum_confidence=spec.minimum_confidence,
    )
    candidate = next(
        item for item in artifact.proposal.candidates if item.target_id == spec.target_id
    )
    target_selection_ref = f"judgment-artifact:{artifact.artifact_id}"
    request_core = {
        "judgment_artifact_id": artifact.artifact_id,
        "evidence_pack_id": evidence_pack.pack_id,
        "target_id": spec.target_id,
        "data_snapshot_id": spec.data_snapshot_id,
        "start_at": spec.start_at,
        "end_at": spec.end_at,
        "signal_expires_at": spec.signal_expires_at,
        "market": spec.market,
        "minimum_confidence": spec.minimum_confidence,
        "candidate_horizon_sessions": candidate.horizon_sessions,
        "simulation": {
            "data_granularity": spec.simulation.data_granularity,
            "book_type": spec.simulation.book_type,
            "fill_model": spec.simulation.fill_model,
            "fee_model": spec.simulation.fee_model,
            "venue_ruleset": spec.simulation.venue_ruleset,
            "base_currency": spec.simulation.base_currency,
            "starting_cash": str(spec.simulation.starting_cash),
            "random_seed": spec.simulation.random_seed,
        },
    }
    return BacktestRequest(
        request_id=f"agent-replay-{canonical_hash(request_core)}",
        signal=signal,
        as_of=evidence_pack.as_of,
        start_at=spec.start_at,
        end_at=spec.end_at,
        market=spec.market,
        instrument_ids=(spec.target_id,),
        data_snapshot_id=spec.data_snapshot_id,
        target_selection_ref=target_selection_ref,
        strategy_ref="event-impact-hold.v1",
        horizons_sessions=(candidate.horizon_sessions,),
        simulation=spec.simulation,
    )


def verify_judgment_backtest_request(
    *,
    request: BacktestRequest,
    artifact: JudgmentArtifact,
    evidence_pack: EvidencePack,
    minimum_confidence: float,
) -> None:
    expected_ref = f"judgment-artifact:{artifact.artifact_id}"
    if request.target_selection_ref != expected_ref:
        raise ValueError("Backtest Request is not bound to the supplied Judgment Artifact")
    if request.as_of != evidence_pack.as_of:
        raise ValueError("Backtest Request cutoff does not match the Evidence Pack")
    expected_signal = admit_candidate_to_signal(
        artifact=artifact,
        evidence_pack=evidence_pack,
        target_id=request.signal.instrument_id,
        valid_from=evidence_pack.as_of,
        expires_at=request.signal.expires_at,
        minimum_confidence=minimum_confidence,
    )
    if request.signal != expected_signal:
        raise ValueError("Backtest Request signal does not match the frozen judgment")
    candidate = next(
        item
        for item in artifact.proposal.candidates
        if item.target_id == request.signal.instrument_id
    )
    if request.horizons_sessions != (candidate.horizon_sessions,):
        raise ValueError("Backtest Request horizon does not match the frozen judgment")


def build_ensemble_backtest_request(
    *,
    decision: AgentEnsembleDecision,
    registration: AgentPhase2Preregistration,
    registry: ExposureRegistry,
    evidence_pack: EvidencePack,
    agreeing_artifacts: tuple[JudgmentArtifact, ...],
    spec: EnsembleReplaySpec,
) -> BacktestRequest:
    decision.validate_against(registration, evidence_pack, registry)
    if decision.disposition is not EnsembleDisposition.PROPOSE:
        raise ValueError("an abstaining Agent Ensemble Decision cannot create a backtest")
    if decision.selected_vote is None:
        raise ValueError("Agent Ensemble Decision does not contain a selected vote")
    if spec.start_at < evidence_pack.as_of:
        raise ValueError("replay start_at must not be before the Evidence Pack cutoff")
    candidates = _validate_agreeing_artifacts(
        decision=decision,
        registration=registration,
        evidence_pack=evidence_pack,
        agreeing_artifacts=agreeing_artifacts,
    )
    evidence_refs = tuple(
        sorted(
            {
                reference
                for candidate in candidates
                for reference in (*candidate.evidence_refs, *candidate.counterevidence_refs)
            }
        )
    )
    invalidation_conditions = tuple(
        sorted(
            {
                condition
                for candidate in candidates
                for condition in candidate.invalidation_conditions
            }
        )
    )
    selected_vote = decision.selected_vote
    signal_core = {
        "agent_ensemble_decision_id": decision.decision_id,
        "agreeing_judgment_artifact_ids": list(decision.agreeing_judgment_artifact_ids),
        "target_id": selected_vote.target_id,
        "direction": selected_vote.direction.value,
        "horizon_sessions": selected_vote.horizon_sessions,
        "valid_from": evidence_pack.as_of.isoformat(),
        "expires_at": spec.signal_expires_at.isoformat(),
    }
    signal = SignalIntent(
        signal_id=f"agent-ensemble-signal-{canonical_hash(signal_core)}",
        event_id=evidence_pack.event_id,
        instrument_id=selected_vote.target_id,
        side=Side.BUY,
        valid_from=evidence_pack.as_of,
        expires_at=spec.signal_expires_at,
        evidence_refs=evidence_refs,
        invalidation_conditions=invalidation_conditions,
    )
    request_core = {
        "agent_ensemble_decision_id": decision.decision_id,
        "data_snapshot_id": spec.data_snapshot_id,
        "start_at": spec.start_at,
        "end_at": spec.end_at,
        "signal_expires_at": spec.signal_expires_at,
        "market": spec.market,
        "simulation": {
            "data_granularity": spec.simulation.data_granularity,
            "book_type": spec.simulation.book_type,
            "fill_model": spec.simulation.fill_model,
            "fee_model": spec.simulation.fee_model,
            "venue_ruleset": spec.simulation.venue_ruleset,
            "base_currency": spec.simulation.base_currency,
            "starting_cash": str(spec.simulation.starting_cash),
            "random_seed": spec.simulation.random_seed,
        },
    }
    return BacktestRequest(
        request_id=f"agent-ensemble-replay-{canonical_hash(request_core)}",
        signal=signal,
        as_of=evidence_pack.as_of,
        start_at=spec.start_at,
        end_at=spec.end_at,
        market=spec.market,
        instrument_ids=(selected_vote.target_id,),
        data_snapshot_id=spec.data_snapshot_id,
        target_selection_ref=f"agent-ensemble:{decision.decision_id}",
        strategy_ref="event-impact-hold.v1",
        horizons_sessions=(selected_vote.horizon_sessions,),
        simulation=spec.simulation,
    )


def verify_ensemble_backtest_request(
    *,
    request: BacktestRequest,
    decision: AgentEnsembleDecision,
    registration: AgentPhase2Preregistration,
    registry: ExposureRegistry,
    evidence_pack: EvidencePack,
    agreeing_artifacts: tuple[JudgmentArtifact, ...],
) -> None:
    expected = build_ensemble_backtest_request(
        decision=decision,
        registration=registration,
        registry=registry,
        evidence_pack=evidence_pack,
        agreeing_artifacts=agreeing_artifacts,
        spec=EnsembleReplaySpec(
            data_snapshot_id=request.data_snapshot_id,
            start_at=request.start_at,
            end_at=request.end_at,
            signal_expires_at=request.signal.expires_at,
            market=request.market,
            simulation=request.simulation,
        ),
    )
    if request != expected:
        raise ValueError("Backtest Request does not match the frozen Agent Ensemble Decision")


def _validate_agreeing_artifacts(
    *,
    decision: AgentEnsembleDecision,
    registration: AgentPhase2Preregistration,
    evidence_pack: EvidencePack,
    agreeing_artifacts: tuple[JudgmentArtifact, ...],
) -> tuple[CandidateImpact, ...]:
    artifact_ids = tuple(item.artifact_id for item in agreeing_artifacts)
    if artifact_ids != decision.agreeing_judgment_artifact_ids:
        raise ValueError("supplied Judgment Artifacts do not match ensemble agreement")
    selected_vote = decision.selected_vote
    if selected_vote is None:
        raise ValueError("Agent Ensemble Decision does not contain a selected vote")
    assessments = {
        item.judgment_artifact_id: item
        for item in decision.assessments
        if item.judgment_artifact_id is not None
    }
    candidates: list[CandidateImpact] = []
    protocol = registration.agent_protocol
    for artifact in agreeing_artifacts:
        artifact.validate_against(evidence_pack)
        assessment = assessments.get(artifact.artifact_id)
        if assessment is None or assessment.vote != selected_vote:
            raise ValueError("Judgment Artifact is not an agreeing ensemble replicate")
        if (
            artifact.provider_id != decision.provider_id
            or artifact.model != decision.model
            or assessment.terminal_artifact_hash != canonical_hash(artifact.to_dict())
            or assessment.execution_binding_hash
            != execution_binding_hash(artifact, runtime_ref=decision.runtime_ref)
        ):
            raise ValueError("Judgment Artifact execution binding does not match ensemble")
        eligible = tuple(
            candidate
            for candidate in artifact.proposal.candidates
            if candidate.target_id == selected_vote.target_id
            and candidate.direction is selected_vote.direction
            and candidate.horizon_sessions == selected_vote.horizon_sessions
            and candidate.confidence >= float(protocol.minimum_candidate_confidence)
        )
        if len(eligible) != 1:
            raise ValueError("Judgment Artifact does not contain the agreeing candidate")
        candidates.append(eligible[0])
    return tuple(candidates)
