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


def test_live_frontiers_are_linear_and_full_run_still_revalidates(tmp_path: Path) -> None:
    from contextlib import aclosing

    journal = RunJournal(tmp_path / "runs.sqlite3")
    days: list[int] = []
    judgments: list[str] = []
    validations: list[str] = []

    async def decide(
        _frame: ReviewFrame,
        _prior: ContinuousDecision | None,
        run_id: str,
        allowed: frozenset[int],
        _resume: bool,
    ) -> ContinuousDecision:
        judgments.append(run_id)
        return ContinuousDecision(run_id, run_id, max(allowed), "hold", run_id)

    async def advance(index: int, _decision: ContinuousDecision | None) -> dict[str, object]:
        days.append(index)
        return {"index": index}

    coordinator = ContinuousReviewCoordinator(
        journal,
        episode_id="linear",
        registration_hash=canonical_hash("linear"),
        account_scope="fixture-account",
        model_arm="luna_max",
        cadence=ContinuousCadence.EVENT,
        frames=_frames(120),
        decide=decide,
        advance_account=advance,
        validate_decision=lambda decision, _frame: validations.append(decision.decision_ref),
    )

    async def exercise() -> None:
        async with aclosing(coordinator.stream()) as stream:
            for frontier in range(1, 121):
                report = await anext(stream)
                assert report["completed_sessions"] == frontier
                assert report["status"] == ("completed" if frontier == 120 else "prefix_complete")
                assert days == list(range(frontier))
                # Even a suspended stream owns the episode until explicitly closed.
                assert (await coordinator.run())["status"] == "in_progress"
        original_judgments = judgments.copy()
        original_validations = validations.copy()
        assert (await coordinator.run())["status"] == "completed"
        assert days == list(range(120)) * 2
        assert judgments == original_judgments
        assert validations == original_validations * 2

        def reject(_decision: ContinuousDecision, _frame: ReviewFrame) -> None:
            raise PermissionError("replaced authority rejects")

        coordinator.validate_decision = reject
        with pytest.raises(PermissionError, match="replaced authority"):
            await coordinator.run()
        assert days == list(range(120)) * 2
        claim = journal.try_claim_run("linear")
        assert claim is not None
        claim.release()

    asyncio.run(exercise())


@pytest.mark.parametrize("boundary", ["review", "account", "cancel"])
def test_live_interruption_replays_durable_prefix_without_regeneration(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, boundary: str
) -> None:
    from contextlib import aclosing

    journal = RunJournal(tmp_path / "runs.sqlite3")
    resumes: list[tuple[str, bool]] = []
    days: list[int] = []
    interrupted = False
    entered = asyncio.Event()
    original_append = journal.append

    def append(**kwargs: object) -> None:
        nonlocal interrupted
        if boundary == "account" and kwargs["event_id"] == "resume.account.1" and not interrupted:
            interrupted = True
            raise RuntimeError("account commit interrupted")
        original_append(**kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(journal, "append", append)

    async def decide(
        _frame: ReviewFrame,
        _prior: ContinuousDecision | None,
        run_id: str,
        _allowed: frozenset[int],
        resume: bool,
    ) -> ContinuousDecision:
        nonlocal interrupted
        resumes.append((run_id, resume))
        if boundary == "review" and run_id == "resume.review.1" and not interrupted:
            interrupted = True
            raise RuntimeError("review interrupted")
        return ContinuousDecision(run_id, run_id, 1, "hold", run_id)

    async def advance(index: int, _decision: ContinuousDecision | None) -> dict[str, object]:
        nonlocal interrupted
        days.append(index)
        if boundary == "cancel" and index == 1 and not interrupted:
            interrupted = True
            entered.set()
            await asyncio.Event().wait()
        return {"index": index}

    coordinator = ContinuousReviewCoordinator(
        journal,
        episode_id="resume",
        registration_hash=canonical_hash("resume"),
        account_scope="fixture-account",
        model_arm="luna_max",
        cadence=ContinuousCadence.EXPIRY_ONLY,
        frames=_frames(3),
        decide=decide,
        advance_account=advance,
        validate_decision=lambda _decision, _frame: None,
    )

    async def exercise() -> None:
        async with aclosing(coordinator.stream()) as stream:
            assert (await anext(stream))["completed_sessions"] == 1
            if boundary == "cancel":
                task = asyncio.create_task(anext(stream))
                await entered.wait()
                task.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await task
            else:
                with pytest.raises(RuntimeError, match="interrupted"):
                    await anext(stream)
        assert journal.event("resume.account.0") is not None
        assert journal.event("resume.account.1") is None
        async with aclosing(coordinator.stream()) as restarted:
            reports = [report async for report in restarted]
        assert reports[-1]["status"] == "completed"
        assert days == ([0, 0, 1, 2] if boundary == "review" else [0, 1, 0, 1, 2])
        assert resumes == (
            [("resume.review.0", False), ("resume.review.1", False)]
            + ([("resume.review.1", True)] if boundary == "review" else [])
            + [("resume.review.2", False)]
        )
        claim = journal.try_claim_run("resume")
        assert claim is not None
        claim.release()

    asyncio.run(exercise())
