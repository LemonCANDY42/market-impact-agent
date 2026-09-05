from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.continuous_decision import (
    ContinuousCadence,
    ContinuousDecision,
    ContinuousReviewCoordinator,
    PendingReview,
    ReviewFrame,
)
from market_impact_agent.runtime_store import RunJournal


def _frames(count: int) -> tuple[ReviewFrame, ...]:
    return tuple(
        ReviewFrame(
            datetime(2020, 2, 3, 1, 25, tzinfo=UTC) + timedelta(days=i),
            (f"frozen-{i}",),
            canonical_hash(i),
            new_fact_ids=("same-receipt",) if i in (1, 2, 3) else (),
        )
        for i in range(count)
    )


@pytest.mark.parametrize("cadence", list(ContinuousCadence))
def test_full_account_path_renewal_and_replay_do_not_regenerate(
    tmp_path: Path, cadence: ContinuousCadence
) -> None:
    journal = RunJournal(tmp_path / "runs.sqlite3")
    frames = _frames(8)
    calls: list[str] = []
    approvals: set[str] = set()
    account_days: list[int] = []

    async def decide(
        frame: ReviewFrame,
        previous: ContinuousDecision | None,
        run_id: str,
        allowed: frozenset[int],
        resume: bool,
    ) -> ContinuousDecision:
        assert not resume
        if calls:
            assert previous is not None
        assert frame.cutoff < frames[-1].cutoff or allowed == {1}
        calls.append(run_id)
        approvals.add(run_id)
        return ContinuousDecision(
            run_id + ".research", run_id + ".portfolio", max(allowed), "hold", run_id
        )

    def validate(decision: ContinuousDecision, _frame: ReviewFrame) -> None:
        assert decision.decision_ref in approvals

    async def advance(index: int, decision: ContinuousDecision | None) -> dict[str, object]:
        if decision is not None:
            assert journal.event(f"episode.review.{index}.completed") is not None
        account_days.append(index)
        return {"session": index, "nav": "100000", "source": "test-executor"}

    def coordinator() -> ContinuousReviewCoordinator:
        return ContinuousReviewCoordinator(
            journal,
            episode_id="episode",
            registration_hash=canonical_hash("registration"),
            account_scope="test-account",
            model_arm="luna_max",
            cadence=cadence,
            frames=frames,
            decide=decide,
            advance_account=advance,
            validate_decision=validate,
        )

    first = asyncio.run(coordinator().run())
    assert first["status"] == "completed"
    assert first["completed_sessions"] == 8
    assert account_days == list(range(8))
    original_calls = calls.copy()
    assert len(calls) >= 2  # The original five-session thesis expires within this episode.
    if cadence is ContinuousCadence.EXPIRY_ONLY:
        assert calls == ["episode.review.0", "episode.review.5"]
    if cadence is ContinuousCadence.EVENT:
        assert "episode.review.1" in calls
        assert "episode.review.2" not in calls  # Same receipt is not another event.
    assert asyncio.run(coordinator().run()) == first
    assert calls == original_calls


def test_interrupted_judgment_requires_resume_and_never_becomes_hold(tmp_path: Path) -> None:
    journal = RunJournal(tmp_path / "runs.sqlite3")
    resumes: list[bool] = []
    accounts: list[int] = []

    async def decide(
        _frame: ReviewFrame,
        _previous: ContinuousDecision | None,
        _run_id: str,
        _allowed: frozenset[int],
        resume: bool,
    ) -> PendingReview:
        resumes.append(resume)
        if not resume:
            raise RuntimeError("crash after model dispatch")
        return PendingReview("unknown_generation_requires_reconciliation")

    async def advance(index: int, _decision: ContinuousDecision | None) -> dict[str, object]:
        accounts.append(index)
        return {}

    coordinator = ContinuousReviewCoordinator(
        journal,
        episode_id="episode",
        registration_hash=canonical_hash("registration"),
        account_scope="test-account",
        model_arm="luna_max",
        cadence=ContinuousCadence.SCHEDULED,
        frames=_frames(3),
        decide=decide,
        advance_account=advance,
        validate_decision=lambda _decision, _frame: None,
    )
    with pytest.raises(RuntimeError, match="crash"):
        asyncio.run(coordinator.run())
    resumed = asyncio.run(coordinator.run())
    assert resumed["status"] == "incomplete"
    assert resumed["completed_sessions"] == 0
    assert resumes == [False, True]
    assert accounts == []
    assert journal.event("episode.review.0.completed") is None


def test_changed_daily_inputs_cannot_reuse_old_completion(tmp_path: Path) -> None:
    async def decide(*_: object) -> PendingReview:
        return PendingReview("awaiting_data")

    async def advance(*_: object) -> dict[str, object]:
        return {}

    def build(frames: tuple[ReviewFrame, ...]) -> ContinuousReviewCoordinator:
        return ContinuousReviewCoordinator(
            RunJournal(tmp_path / "runs.sqlite3"),
            episode_id="episode",
            registration_hash=canonical_hash("registration"),
            account_scope="test-account",
            model_arm="luna_max",
            cadence=ContinuousCadence.EVENT,
            frames=frames,
            decide=decide,
            advance_account=advance,
            validate_decision=lambda _decision, _frame: None,
        )

    build(_frames(3))
    with pytest.raises(ValueError, match="different configuration"):
        build(_frames(4))


@pytest.mark.parametrize("cadence", [ContinuousCadence.SCHEDULED, ContinuousCadence.EVENT])
def test_long_window_revision_can_keep_h60_without_resetting_event_allowance(
    tmp_path: Path, cadence: ContinuousCadence
) -> None:
    from dataclasses import replace

    frames = tuple(
        replace(frame, new_fact_ids=(f"fact-{i}",)) for i, frame in enumerate(_frames(120))
    )
    calls: list[int] = []

    async def decide(
        frame: ReviewFrame,
        _prior: ContinuousDecision | None,
        run_id: str,
        allowed: frozenset[int],
        _resume: bool,
    ) -> ContinuousDecision:
        index = frames.index(frame)
        calls.append(index)
        if index <= 60:
            assert 60 in allowed
        return ContinuousDecision(run_id, run_id, max(allowed), "hold", run_id)

    async def advance(index: int, _: ContinuousDecision | None) -> dict[str, object]:
        return {"index": index}

    result = asyncio.run(
        ContinuousReviewCoordinator(
            RunJournal(tmp_path / "runs.sqlite3"),
            episode_id="long-window",
            registration_hash=canonical_hash("120-sessions"),
            account_scope="fixture-account",
            model_arm="luna_max",
            cadence=cadence,
            frames=frames,
            decide=decide,
            advance_account=advance,
            validate_decision=lambda _decision, _frame: None,
        ).run()
    )
    assert result["completed_sessions"] == 120
    if cadence is ContinuousCadence.EVENT:
        assert calls[:5] == [0, 1, 2, 3, 63]
    else:
        assert calls[:4] == [0, 5, 10, 15]
