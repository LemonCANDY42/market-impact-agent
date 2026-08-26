import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from hashlib import sha256
from pathlib import Path

import pytest

pytest.importorskip("nautilus_trader")

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
    aggregate_agent_replicates,
    execution_binding_hash,
)
from market_impact_agent.agent_study import load_agent_phase2_preregistration
from market_impact_agent.backtests import BacktestRunStatus, SimulationSpec
from market_impact_agent.judgment_replay import (
    EnsembleReplaySpec,
    JudgmentReplaySpec,
    build_ensemble_backtest_request,
    build_judgment_backtest_request,
    verify_ensemble_backtest_request,
    verify_judgment_backtest_request,
)
from market_impact_agent.nautilus_backtest import NautilusBacktestBridge
from market_impact_agent.research import EvidenceTier, TransmissionDirectness
from market_impact_agent.runtime_store import RunStatus

CUTOFF = datetime(2026, 8, 25, 8, tzinfo=UTC)
SNAPSHOT_PATH = (
    Path(__file__).parents[1] / "examples" / "backtests" / "synthetic-xshg-600028-20260825-v1.json"
)
REGISTRATION_PATH = Path("examples/calibration/agent-physical-energy-prospective-v1.json")
REGISTRY_PATH = Path("examples/research/a-share-energy-exposure-registry-v1.json")


def pack(*, target_id: str = "600028.XSHG") -> EvidencePack:
    return EvidencePack.build(
        event_id="agent-synthetic-event",
        as_of=CUTOFF,
        research_question="Could the eligible target be affected?",
        evidence=(
            EvidenceReference(
                evidence_id="agent-evidence",
                claim_id="event-fact",
                source_ref="synthetic://agent-evidence",
                source_tier=EvidenceTier.OFFICIAL,
                available_at=CUTOFF - timedelta(minutes=1),
                content_hash=sha256(b"agent-evidence").hexdigest(),
                summary="Synthetic event fact.",
            ),
            EvidenceReference(
                evidence_id="agent-counterevidence",
                claim_id="offset",
                source_ref="synthetic://agent-counterevidence",
                source_tier=EvidenceTier.PRIMARY,
                available_at=CUTOFF - timedelta(seconds=30),
                content_hash=sha256(b"agent-counterevidence").hexdigest(),
                summary="Synthetic offset evidence.",
            ),
        ),
        pattern_packs=(),
        allowed_targets=(target_id,),
    )


def proposal(
    *,
    direction: CandidateDirection = CandidateDirection.UP,
    target_id: str = "600028.XSHG",
) -> JudgmentProposal:
    return JudgmentProposal(
        event_id="agent-synthetic-event",
        decision=JudgmentDecision.PROPOSE,
        summary="A bounded synthetic Agent proposal.",
        transmission_steps=(
            ProposedTransmissionStep(
                step_id="agent-step",
                from_node="event",
                to_node=target_id,
                mechanism="synthetic transmission",
                directness=TransmissionDirectness.DIRECT,
                horizon_sessions=3,
                evidence_refs=("agent-evidence",),
            ),
        ),
        candidates=(
            CandidateImpact(
                target_id=target_id,
                direction=direction,
                horizon_sessions=3,
                directness=TransmissionDirectness.DIRECT,
                confidence=0.75,
                thesis="Synthetic bounded upside.",
                evidence_refs=("agent-evidence",),
                counterevidence_refs=("agent-counterevidence",),
                invalidation_conditions=("synthetic fact is invalidated",),
            ),
        ),
        blockers=(),
        unresolved_questions=("synthetic duration",),
        stopped_reason="minimum evidence checks passed",
    )


def artifact(*, direction: CandidateDirection = CandidateDirection.UP) -> JudgmentArtifact:
    selected = pack()
    return JudgmentArtifact.build(
        run_id="agent-replay-source",
        evidence_pack_id=selected.pack_id,
        provider_id="fixture-provider",
        model="fixture-model",
        runtime_config_hash=sha256(b"runtime").hexdigest(),
        prompt_hash=sha256(b"prompt").hexdigest(),
        skill_hashes=(sha256(b"skill").hexdigest(),),
        tool_manifest_hashes=(sha256(b"tool").hexdigest(),),
        tool_surface_hash=sha256(b"tool-surface").hexdigest(),
        mcp_server_hashes=(),
        context_estimator_id="provider-request-utf8-upper-bound-v2:1",
        compactor_id="deterministic-semantic-context-v2",
        journal_hash=sha256(b"journal").hexdigest(),
        transcript_hash=sha256(b"transcript").hexdigest(),
        raw_response_hash=sha256(b"raw").hexdigest(),
        started_at=CUTOFF - timedelta(minutes=2),
        finished_at=CUTOFF,
        proposal=proposal(direction=direction),
    )


def ensemble_artifact(
    index: int,
    *,
    target_id: str = "600938.XSHG",
) -> JudgmentArtifact:
    selected = pack(target_id=target_id)
    return JudgmentArtifact.build(
        run_id=f"agent-ensemble-replicate-{index}",
        evidence_pack_id=selected.pack_id,
        provider_id="minimax-openai-compatible",
        model="MiniMax-M3",
        runtime_config_hash=sha256(b"ensemble-runtime").hexdigest(),
        prompt_hash=sha256(b"ensemble-prompt").hexdigest(),
        skill_hashes=(sha256(b"ensemble-skill").hexdigest(),),
        tool_manifest_hashes=(sha256(b"ensemble-tool").hexdigest(),),
        tool_surface_hash=sha256(b"ensemble-tool-surface").hexdigest(),
        mcp_server_hashes=(),
        context_estimator_id="provider-request-utf8-upper-bound-v2:1",
        compactor_id="deterministic-semantic-context-v2",
        journal_hash=sha256(f"journal-{index}".encode()).hexdigest(),
        transcript_hash=sha256(f"transcript-{index}".encode()).hexdigest(),
        raw_response_hash=sha256(f"raw-{index}".encode()).hexdigest(),
        started_at=CUTOFF - timedelta(minutes=2),
        finished_at=CUTOFF,
        proposal=proposal(target_id=target_id),
    )


def spec() -> JudgmentReplaySpec:
    return JudgmentReplaySpec(
        target_id="600028.XSHG",
        data_snapshot_id="synthetic-xshg-600028-20260825-v1",
        start_at=datetime(2026, 8, 26, 1, tzinfo=UTC),
        end_at=datetime(2026, 9, 4, 8, tzinfo=UTC),
        signal_expires_at=datetime(2026, 9, 1, 8, tzinfo=UTC),
        market="CN",
        minimum_confidence=0.7,
        simulation=SimulationSpec(
            data_granularity="daily_bar.v1",
            book_type="top_of_book",
            fill_model="next_executable_open_one_tick_slippage.v1",
            fee_model="a_share_fixture_fee.v1",
            venue_ruleset="xshg_cash_equity_fixture.v1",
            base_currency="CNY",
            starting_cash=Decimal("1000000"),
            random_seed=7,
        ),
    )


def test_frozen_agent_judgment_drives_unchanged_nautilus_replay() -> None:
    evidence = pack()
    judgment = artifact()
    replay_request = build_judgment_backtest_request(
        artifact=judgment,
        evidence_pack=evidence,
        spec=spec(),
    )

    verify_judgment_backtest_request(
        request=replay_request,
        artifact=judgment,
        evidence_pack=evidence,
        minimum_confidence=0.7,
    )
    first = NautilusBacktestBridge(SNAPSHOT_PATH).run(replay_request)
    second = NautilusBacktestBridge(SNAPSHOT_PATH).run(replay_request)

    assert replay_request.target_selection_ref == f"judgment-artifact:{judgment.artifact_id}"
    assert replay_request.horizons_sessions == (3,)
    assert first.status is BacktestRunStatus.COMPLETED
    assert first.result_hash == second.result_hash


def test_three_of_five_agent_ensemble_drives_unchanged_nautilus_replay(
    tmp_path: Path,
) -> None:
    evidence = pack(target_id="600938.XSHG")
    registration, registry = load_agent_phase2_preregistration(
        REGISTRATION_PATH,
        REGISTRY_PATH,
    )
    artifacts = tuple(ensemble_artifact(index) for index in range(1, 4))
    results = (
        *(
            AgentRunResult(
                run_id=item.run_id,
                status=RunStatus.COMPLETED,
                judgment=item,
                terminal_store_hash=canonical_hash(item.to_dict()),
                metrics=None,
            )
            for item in artifacts
        ),
        AgentRunResult(
            run_id="agent-ensemble-replicate-4",
            status=RunStatus.FAILED,
            judgment=None,
            terminal_store_hash=None,
            metrics=None,
        ),
        AgentRunResult(
            run_id="agent-ensemble-replicate-5",
            status=RunStatus.FAILED,
            judgment=None,
            terminal_store_hash=None,
            metrics=None,
        ),
    )
    decision = aggregate_agent_replicates(
        ensemble_run_id="agent-ensemble-replay",
        registration=registration,
        evidence_pack=evidence,
        results=results,
        frozen_execution_binding_hash=execution_binding_hash(
            artifacts[0],
            runtime_ref=registration.agent_protocol.runtime_ref,
        ),
    )
    snapshot_payload = json.loads(SNAPSHOT_PATH.read_text(encoding="utf-8"))
    snapshot_payload["snapshot_id"] = "synthetic-xshg-600938-20260825-v1"
    snapshot_payload["instrument_id"] = "600938.XSHG"
    snapshot_path = tmp_path / "synthetic-600938.json"
    snapshot_path.write_text(json.dumps(snapshot_payload), encoding="utf-8")
    replay_spec = EnsembleReplaySpec(
        data_snapshot_id="synthetic-xshg-600938-20260825-v1",
        start_at=spec().start_at,
        end_at=spec().end_at,
        signal_expires_at=spec().signal_expires_at,
        market=spec().market,
        simulation=spec().simulation,
    )

    replay_request = build_ensemble_backtest_request(
        decision=decision,
        registration=registration,
        registry=registry,
        evidence_pack=evidence,
        agreeing_artifacts=artifacts,
        spec=replay_spec,
    )
    verify_ensemble_backtest_request(
        request=replay_request,
        decision=decision,
        registration=registration,
        registry=registry,
        evidence_pack=evidence,
        agreeing_artifacts=artifacts,
    )
    first = NautilusBacktestBridge(snapshot_path).run(replay_request)
    second = NautilusBacktestBridge(snapshot_path).run(replay_request)

    metrics = {item.name: item.value for item in first.metrics}
    assert replay_request.target_selection_ref == f"agent-ensemble:{decision.decision_id}"
    assert replay_request.horizons_sessions == (3,)
    assert first.status is BacktestRunStatus.COMPLETED
    assert first.result_hash == second.result_hash
    assert metrics["net_pnl"] == Decimal("47.43")
    assert metrics["net_return"] == Decimal("0.04387604070305272895467160037")


def test_ensemble_replay_rejects_registry_control_target() -> None:
    evidence = pack()
    registration, registry = load_agent_phase2_preregistration(
        REGISTRATION_PATH,
        REGISTRY_PATH,
    )
    artifacts = tuple(ensemble_artifact(index, target_id="600028.XSHG") for index in range(1, 4))
    results = (
        *(
            AgentRunResult(
                run_id=item.run_id,
                status=RunStatus.COMPLETED,
                judgment=item,
                terminal_store_hash=canonical_hash(item.to_dict()),
                metrics=None,
            )
            for item in artifacts
        ),
        AgentRunResult(
            run_id="agent-ensemble-replicate-4",
            status=RunStatus.FAILED,
            judgment=None,
            terminal_store_hash=None,
            metrics=None,
        ),
        AgentRunResult(
            run_id="agent-ensemble-replicate-5",
            status=RunStatus.FAILED,
            judgment=None,
            terminal_store_hash=None,
            metrics=None,
        ),
    )
    decision = aggregate_agent_replicates(
        ensemble_run_id="agent-ensemble-control-replay",
        registration=registration,
        evidence_pack=evidence,
        results=results,
        frozen_execution_binding_hash=execution_binding_hash(
            artifacts[0],
            runtime_ref=registration.agent_protocol.runtime_ref,
        ),
    )

    with pytest.raises(ValueError, match="vote outside the protocol"):
        build_ensemble_backtest_request(
            decision=decision,
            registration=registration,
            registry=registry,
            evidence_pack=evidence,
            agreeing_artifacts=artifacts,
            spec=EnsembleReplaySpec(
                data_snapshot_id=spec().data_snapshot_id,
                start_at=spec().start_at,
                end_at=spec().end_at,
                signal_expires_at=spec().signal_expires_at,
                market=spec().market,
                simulation=spec().simulation,
            ),
        )


def test_replay_registration_rejects_low_confidence_and_nondirectional_candidates() -> None:
    with pytest.raises(ValueError, match="below the deterministic admission threshold"):
        build_judgment_backtest_request(
            artifact=artifact(),
            evidence_pack=pack(),
            spec=JudgmentReplaySpec(
                target_id=spec().target_id,
                data_snapshot_id=spec().data_snapshot_id,
                start_at=spec().start_at,
                end_at=spec().end_at,
                signal_expires_at=spec().signal_expires_at,
                market=spec().market,
                minimum_confidence=0.8,
                simulation=spec().simulation,
            ),
        )

    with pytest.raises(ValueError, match="mixed or unknown"):
        build_judgment_backtest_request(
            artifact=artifact(direction=CandidateDirection.MIXED),
            evidence_pack=pack(),
            spec=spec(),
        )
