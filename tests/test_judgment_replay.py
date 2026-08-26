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
)
from market_impact_agent.backtests import BacktestRunStatus, SimulationSpec
from market_impact_agent.judgment_replay import (
    JudgmentReplaySpec,
    build_judgment_backtest_request,
    verify_judgment_backtest_request,
)
from market_impact_agent.nautilus_backtest import NautilusBacktestBridge
from market_impact_agent.research import EvidenceTier, TransmissionDirectness

CUTOFF = datetime(2026, 8, 25, 8, tzinfo=UTC)
SNAPSHOT_PATH = (
    Path(__file__).parents[1] / "examples" / "backtests" / "synthetic-xshg-600028-20260825-v1.json"
)


def pack() -> EvidencePack:
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
        allowed_targets=("600028.XSHG",),
    )


def proposal(*, direction: CandidateDirection = CandidateDirection.UP) -> JudgmentProposal:
    return JudgmentProposal(
        event_id="agent-synthetic-event",
        decision=JudgmentDecision.PROPOSE,
        summary="A bounded synthetic Agent proposal.",
        transmission_steps=(
            ProposedTransmissionStep(
                step_id="agent-step",
                from_node="event",
                to_node="600028.XSHG",
                mechanism="synthetic transmission",
                directness=TransmissionDirectness.DIRECT,
                horizon_sessions=3,
                evidence_refs=("agent-evidence",),
            ),
        ),
        candidates=(
            CandidateImpact(
                target_id="600028.XSHG",
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
