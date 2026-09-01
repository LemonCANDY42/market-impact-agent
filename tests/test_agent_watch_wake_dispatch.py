from __future__ import annotations

import sqlite3
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from pathlib import Path
from threading import Barrier
from typing import cast

import pytest

from market_impact_agent.agent_watch_admission import (
    EventImpactTriageWatchAuthority,
    WatchAdmissionOutcome,
)
from market_impact_agent.agent_watch_wake_dispatch import (
    AGENT_WATCH_WAKE_RUN_BOUND_EVENT,
    AgentWatchWakeDispatcher,
)
from market_impact_agent.attention_watch import AttentionWake
from market_impact_agent.runtime_store import RunJournal

from .test_agent_watch_admission import (
    _triage_request,  # pyright: ignore[reportPrivateUsage]
    _triage_setup,  # pyright: ignore[reportPrivateUsage]
)


def _accepted_wake(
    tmp_path: Path,
    *,
    shared: bool = False,
) -> tuple[AgentWatchWakeDispatcher, AttentionWake, RunJournal]:
    store, _, baseline, profile, service, authority = _triage_setup(tmp_path)
    context = authority.delegation_context()
    admitted_at = context.created_at + timedelta(seconds=1)
    first = service.admit(
        _triage_request(profile_id=profile.profile_id, context=context),
        context=context,
        initial_data_snapshot_id=baseline.snapshot_id,
        decided_at=admitted_at,
    )
    assert first.outcome is WatchAdmissionOutcome.ADMITTED
    if shared:
        reused = service.admit(
            _triage_request(
                profile_id=profile.profile_id,
                context=context,
                rationale="A second accepted proposal shares this exact Watch scope.",
            ),
            context=context,
            initial_data_snapshot_id=baseline.snapshot_id,
            decided_at=admitted_at,
        )
        assert reused.outcome is WatchAdmissionOutcome.REUSED
        assert reused.watch_id == first.watch_id
    if first.watch_id is None:
        raise AssertionError("accepted admission has no Watch")
    wake = AttentionWake.build(
        watch_id=first.watch_id,
        data_snapshot_id=baseline.snapshot_id,
        prior_data_snapshot_id=baseline.snapshot_id,
        new_version_ids=("prospective-observation-version-" + "d" * 64,),
        created_at=admitted_at + timedelta(seconds=1),
    )
    artifact = store.artifacts.put_json(wake.to_dict())
    with service.watch_service._connect() as connection:  # pyright: ignore[reportPrivateUsage]
        connection.execute(
            """
            INSERT INTO attention_watch_outbox(
                wake_id, watch_id, trigger_key, artifact_hash, data_snapshot_id,
                created_at, delivery_status, delivered_at
            ) VALUES (?, ?, ?, ?, ?, ?, 'pending', NULL)
            """,
            (
                wake.wake_id,
                wake.watch_id,
                "fixture:" + wake.wake_id,
                artifact.content_hash,
                wake.data_snapshot_id,
                wake.created_at.isoformat().replace("+00:00", "Z"),
            ),
        )
    run_journal = RunJournal(tmp_path / "runs" / "runs.sqlite3")
    return (
        AgentWatchWakeDispatcher(service, run_journal=run_journal),
        wake,
        run_journal,
    )


def test_one_wake_creates_one_replay_stable_research_run(tmp_path: Path) -> None:
    dispatcher, wake, journal = _accepted_wake(tmp_path)
    dispatched_at = wake.created_at + timedelta(seconds=1)

    first = dispatcher.dispatch_wake(wake, dispatched_at=dispatched_at)
    replay = dispatcher.dispatch_wake(
        wake,
        dispatched_at=dispatched_at + timedelta(seconds=1),
    )

    assert len(first) == 1
    assert tuple(item.run.run_id for item in replay) == tuple(item.run.run_id for item in first)
    result = first[0]
    assert result.run.config_hash == result.binding_artifact_hash
    assert result.binding.data_snapshot_id == wake.data_snapshot_id
    assert result.binding.prior_data_snapshot_id == wake.prior_data_snapshot_id
    assert result.binding.new_version_ids == wake.new_version_ids
    assert result.binding.research_only is True
    assert result.binding.judgment_model_calls_authorized is False
    assert result.binding.execution_capability is False
    assert [item.event_type for item in journal.events(result.run.run_id)] == [
        AGENT_WATCH_WAKE_RUN_BOUND_EVENT
    ]
    assert dispatcher.watch_service.pending_wakes() == ()
    assert not hasattr(dispatcher, "model_provider")
    assert not hasattr(dispatcher, "execution_engine")


def test_shared_wake_fans_out_once_per_accepted_admission(tmp_path: Path) -> None:
    dispatcher, wake, journal = _accepted_wake(tmp_path, shared=True)
    dispatched_at = wake.created_at + timedelta(seconds=1)

    first = dispatcher.dispatch_wake(wake, dispatched_at=dispatched_at)
    replay = dispatcher.dispatch_wake(
        wake,
        dispatched_at=dispatched_at + timedelta(seconds=1),
    )

    assert len(first) == 2
    assert len({item.binding.admission_id for item in first}) == 2
    assert len({item.run.run_id for item in first}) == 2
    assert {item.run.run_id for item in replay} == {item.run.run_id for item in first}
    assert all(len(journal.events(item.run.run_id)) == 1 for item in first)


def test_wake_callback_membership_is_frozen_before_late_reuse(tmp_path: Path) -> None:
    dispatcher, wake, journal = _accepted_wake(tmp_path)
    dispatched_at = wake.created_at + timedelta(seconds=1)
    first = dispatcher.dispatch_wake(wake, dispatched_at=dispatched_at)
    service = dispatcher.admission_service
    authority = cast(EventImpactTriageWatchAuthority, service.delegation_authority)
    context = authority.delegation_context()
    profile = service.offered_profiles(context)[0]

    late = service.admit(
        _triage_request(
            profile_id=profile.profile_id,
            context=context,
            rationale="A subscriber committed only after this Wake was already bound.",
        ),
        context=context,
        initial_data_snapshot_id=wake.data_snapshot_id,
        # Keep the same requested Watch interval as the original admission.  The
        # row is physically committed after the callback set was frozen, which
        # is the ordering this regression exercises.
        decided_at=wake.created_at - timedelta(seconds=1),
    )
    assert late.outcome is WatchAdmissionOutcome.REUSED

    replay = dispatcher.dispatch_wake(
        wake,
        dispatched_at=dispatched_at + timedelta(seconds=2),
    )
    assert tuple(item.run.run_id for item in replay) == tuple(item.run.run_id for item in first)
    with sqlite3.connect(journal.path) as connection:
        assert connection.execute("SELECT COUNT(*) FROM runs").fetchone()[0] == 1


def test_crash_before_and_after_run_creation_remains_pending_and_resumes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    before_root = tmp_path / "before"
    before, before_wake, before_journal = _accepted_wake(before_root)
    original_start = before_journal.start_run

    def fail_before_creation(**_: object) -> None:
        raise RuntimeError("crash before Run creation")

    monkeypatch.setattr(before_journal, "start_run", fail_before_creation)
    with pytest.raises(RuntimeError, match="before Run creation"):
        before.dispatch_wake(
            before_wake,
            dispatched_at=before_wake.created_at + timedelta(seconds=1),
        )
    assert before.watch_service.pending_wakes() == (before_wake,)
    monkeypatch.setattr(before_journal, "start_run", original_start)
    assert (
        len(
            before.dispatch_wake(
                before_wake,
                dispatched_at=before_wake.created_at + timedelta(seconds=2),
            )
        )
        == 1
    )

    after_root = tmp_path / "after"
    after, after_wake, after_journal = _accepted_wake(after_root)
    original_append = after_journal.append
    failed = False

    def fail_after_creation(**kwargs: object):  # type: ignore[no-untyped-def]
        nonlocal failed
        if not failed:
            failed = True
            raise RuntimeError("crash after Run creation")
        return original_append(**kwargs)  # pyright: ignore[reportArgumentType]

    monkeypatch.setattr(after_journal, "append", fail_after_creation)
    with pytest.raises(RuntimeError, match="after Run creation"):
        after.dispatch_wake(
            after_wake,
            dispatched_at=after_wake.created_at + timedelta(seconds=1),
        )
    assert after.watch_service.pending_wakes() == (after_wake,)

    restarted_journal = RunJournal(after_journal.path)
    restarted = AgentWatchWakeDispatcher(
        after.admission_service,
        run_journal=restarted_journal,
    )
    resumed = restarted.dispatch_wake(
        after_wake,
        dispatched_at=after_wake.created_at + timedelta(seconds=2),
    )
    assert len(resumed) == 1
    with sqlite3.connect(restarted_journal.path) as connection:
        assert connection.execute("SELECT COUNT(*) FROM runs").fetchone()[0] == 1


def test_concurrent_dispatch_returns_the_same_run_without_duplicates(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dispatcher, wake, journal = _accepted_wake(tmp_path)
    barrier = Barrier(2)
    original_claim = journal.try_claim_run

    def synchronized_claim(run_id: str):  # type: ignore[no-untyped-def]
        barrier.wait()
        return original_claim(run_id)

    monkeypatch.setattr(journal, "try_claim_run", synchronized_claim)
    dispatched_at = wake.created_at + timedelta(seconds=1)
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = tuple(
            executor.submit(dispatcher.dispatch_wake, wake, dispatched_at=dispatched_at)
            for _ in range(2)
        )
    results = tuple(item.result() for item in futures)

    assert {item[0].run.run_id for item in results} == {results[0][0].run.run_id}
    with sqlite3.connect(journal.path) as connection:
        assert connection.execute("SELECT COUNT(*) FROM runs").fetchone()[0] == 1
        assert connection.execute("SELECT COUNT(*) FROM events").fetchone()[0] == 1
