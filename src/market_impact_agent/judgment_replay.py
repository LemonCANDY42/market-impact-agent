from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from market_impact_agent.agent_contracts import (
    EvidencePack,
    JudgmentArtifact,
    admit_candidate_to_signal,
    canonical_hash,
)
from market_impact_agent.backtests import BacktestRequest, SimulationSpec
from market_impact_agent.domain import require_aware


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
