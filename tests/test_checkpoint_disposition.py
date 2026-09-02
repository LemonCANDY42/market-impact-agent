from __future__ import annotations

import asyncio
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from threading import Barrier
from typing import cast

import pytest

from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.agent_schema import validate_agent_contract
from market_impact_agent.data_inputs import LocalDataSnapshotStore
from market_impact_agent.event_impact_triage import (
    EventImpactTriageCandidateSet,
    EventImpactTriageDecision,
    EventImpactTriageProposal,
    TriageClusterProposal,
    TriageWorkDecisionEvidence,
    event_impact_triage_candidate_set_from_dict,
)
from market_impact_agent.event_impact_triage_store import EventImpactTriageDecisionStore
from market_impact_agent.prospective_diagnostic import DiagnosticCutoffRule
from market_impact_agent.prospective_trigger_admission import (
    CheckpointDisposition,
    ProspectiveTriggerAdmission,
    ProspectiveTriggerAdmissionStore,
    admit_prospective_trigger,
    checkpoint_disposition_from_dict,
)

from .test_agent_watch_wake_judgment import _prepared  # pyright: ignore[reportPrivateUsage]
from .test_event_impact_triage import RecordingWorkRunAuthority
from .test_event_impact_triage_runtime import (
    FixtureProvider,
    _ineligible_draft,  # pyright: ignore[reportPrivateUsage]
)
from .test_prospective_trigger_admission import (
    _registration,  # pyright: ignore[reportPrivateUsage]
    _triage,  # pyright: ignore[reportPrivateUsage]
)

NOW = datetime(2026, 9, 2, 9, tzinfo=UTC)
CHECKPOINT = "next-a-share-policy-event"


def _persist(
    store: EventImpactTriageDecisionStore,
    context: tuple[
        EventImpactTriageCandidateSet,
        EventImpactTriageProposal,
        EventImpactTriageDecision,
        TriageClusterProposal,
    ],
):
    candidate, proposal, expected, cluster = context
    evidence = cast(TriageWorkDecisionEvidence, expected.run_evidence)
    decision = store.admit_work(
        candidate_set=candidate,
        proposal=proposal,
        run_evidence=evidence,
        run_authority=RecordingWorkRunAuthority(
            candidate.candidate_set_id, proposal.proposal_id, evidence
        ),
        decided_at=evidence.finished_at,
    )
    return candidate, proposal, decision, cluster


def test_nonrun_retirement_freezes_epoch_and_replays_without_expanding_denominator(
    tmp_path: Path,
) -> None:
    registration = _registration()
    store = LocalDataSnapshotStore(tmp_path / "state")
    triage = EventImpactTriageDecisionStore(store.root)
    first = _persist(
        triage, _triage(registration, checkpoint_key=CHECKPOINT, needs_review=True, seed_offset=20)
    )
    second = _persist(
        triage,
        _triage(
            registration,
            checkpoint_key=CHECKPOINT,
            selected=True,
            seed_offset=40,
            available_after_minutes=10,
            frozen_after_minutes=15,
        ),
    )
    owner = ProspectiveTriggerAdmissionStore(store, clock=lambda: NOW)
    before = [tuple(item.to_dict() for item in context[:3]) for context in (first, second)]
    preview = owner.inspect_checkpoint(
        registration=registration,
        candidate_set_id=first[0].candidate_set_id,
        triage_authority=triage,
    )
    assert preview["checkpoint_disposition"] is None
    assert preview["admission_allowed"] is False
    disposition = owner.record_legacy_missed_window(
        registration=registration,
        checkpoint_key=CHECKPOINT,
        candidate_set_id=first[0].candidate_set_id,
        triage_authority=triage,
    )
    assert disposition.kind == "missed_window"
    assert disposition.reason == "legacy_session_unanchored"
    assert disposition.proven_deadline is None
    assert disposition.recorded_at == NOW
    assert disposition.candidate_decision_ids == tuple(
        sorted(
            (
                (first[0].candidate_set_id, first[2].decision_id),
                (second[0].candidate_set_id, second[2].decision_id),
            )
        )
    )
    assert (
        validate_agent_contract(disposition.to_dict(), "checkpoint-disposition.schema.json") == ()
    )
    assert checkpoint_disposition_from_dict(disposition.to_dict()) == disposition
    _persist(
        triage,
        _triage(
            registration,
            checkpoint_key=CHECKPOINT,
            selected=True,
            seed_offset=60,
            available_after_minutes=20,
            frozen_after_minutes=25,
        ),
    )
    restarted = ProspectiveTriggerAdmissionStore(store, clock=lambda: NOW + timedelta(days=1))
    replay = restarted.record_legacy_missed_window(
        registration=registration,
        checkpoint_key=CHECKPOINT,
        candidate_set_id=first[0].candidate_set_id,
        triage_authority=EventImpactTriageDecisionStore(store.root),
    )
    assert replay == disposition
    assert before == [
        tuple(item.to_dict() for item in triage.get_context(context[0].candidate_set_id))
        for context in (first, second)
    ]
    after = restarted.inspect_checkpoint(
        registration=registration,
        candidate_set_id=first[0].candidate_set_id,
        triage_authority=triage,
    )
    assert after["checkpoint_disposition"] == disposition.to_dict()
    assert after["selection_ready"] is False
    assert after["admission_allowed"] is False
    with sqlite3.connect(store.index_path) as connection:
        assert connection.execute(
            "SELECT count(*) FROM prospective_trigger_admissions"
        ).fetchone() == (0,)
        assert connection.execute("SELECT count(*) FROM strategy_window_events_v2").fetchone() == (
            0,
        )
        with pytest.raises(sqlite3.IntegrityError, match="append-only"):
            connection.execute("DELETE FROM checkpoint_dispositions")


@pytest.mark.parametrize("retirement_first", [True, False])
def test_disposition_and_admission_are_mutually_exclusive(
    tmp_path: Path, retirement_first: bool
) -> None:
    registration = _registration()
    store = LocalDataSnapshotStore(tmp_path / "state")
    triage = EventImpactTriageDecisionStore(store.root)
    candidate, proposal, decision, cluster = _persist(
        triage, _triage(registration, checkpoint_key=CHECKPOINT, selected=True)
    )
    owner = ProspectiveTriggerAdmissionStore(store, clock=lambda: NOW)
    admission = admit_prospective_trigger(
        registration=registration,
        candidate_set=candidate,
        proposal=proposal,
        decision=decision,
        cluster_id=cluster.cluster_id,
        admitted_at=NOW,
    )

    def retire() -> CheckpointDisposition:
        return owner.record_legacy_missed_window(
            registration=registration,
            checkpoint_key=CHECKPOINT,
            candidate_set_id=candidate.candidate_set_id,
            triage_authority=triage,
        )

    def admit() -> ProspectiveTriggerAdmission:
        return owner.record(
            admission,
            registration=registration,
            candidate_set=candidate,
            proposal=proposal,
            decision=decision,
            triage_authority=triage,
        )

    if retirement_first:
        retire()
        with pytest.raises(ValueError, match="closed by a non-run"):
            admit()
    else:
        admit()
        with pytest.raises(ValueError, match="already has a Trigger Admission"):
            retire()


def test_retirement_admission_race_has_one_terminal_winner(tmp_path: Path) -> None:
    registration = _registration()
    store = LocalDataSnapshotStore(tmp_path / "state")
    triage = EventImpactTriageDecisionStore(store.root)
    candidate, proposal, decision, cluster = _persist(
        triage, _triage(registration, checkpoint_key=CHECKPOINT, selected=True)
    )
    owners = [ProspectiveTriggerAdmissionStore(store, clock=lambda: NOW) for _ in range(2)]
    barrier = Barrier(2)
    admission = admit_prospective_trigger(
        registration=registration,
        candidate_set=candidate,
        proposal=proposal,
        decision=decision,
        cluster_id=cluster.cluster_id,
        admitted_at=NOW,
    )

    def attempt(retire: bool) -> str:
        barrier.wait(timeout=5)
        try:
            if retire:
                owners[0].record_legacy_missed_window(
                    registration=registration,
                    checkpoint_key=CHECKPOINT,
                    candidate_set_id=candidate.candidate_set_id,
                    triage_authority=triage,
                )
            else:
                owners[1].record(
                    admission,
                    registration=registration,
                    candidate_set=candidate,
                    proposal=proposal,
                    decision=decision,
                    triage_authority=triage,
                )
            return "won"
        except ValueError:
            return "closed"

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(attempt, (True, False)))
    assert sorted(results) == ["closed", "won"]
    with sqlite3.connect(store.index_path) as connection:
        count = connection.execute(
            "SELECT count(*) FROM prospective_trigger_admissions"
        ).fetchone()[0]
        count += connection.execute("SELECT count(*) FROM checkpoint_dispositions").fetchone()[0]
        assert count == 1


def test_retirement_rejects_wrong_checkpoint_empty_epoch_and_conflicting_epoch(
    tmp_path: Path,
) -> None:
    registration = _registration()
    store = LocalDataSnapshotStore(tmp_path / "state")
    triage = EventImpactTriageDecisionStore(store.root)
    first = _persist(triage, _triage(registration, checkpoint_key=CHECKPOINT, selected=True))
    owner = ProspectiveTriggerAdmissionStore(store, clock=lambda: NOW)
    with pytest.raises(ValueError, match="another registration/checkpoint"):
        owner.record_legacy_missed_window(
            registration=registration,
            checkpoint_key="next-material-a-share-event",
            candidate_set_id=first[0].candidate_set_id,
            triage_authority=triage,
        )
    empty = ProspectiveTriggerAdmissionStore(
        store, clock=lambda: first[2].decided_at - timedelta(seconds=1)
    )
    with pytest.raises(ValueError, match="nonempty completed route epoch"):
        empty.record_legacy_missed_window(
            registration=registration,
            checkpoint_key=CHECKPOINT,
            candidate_set_id=first[0].candidate_set_id,
            triage_authority=triage,
        )
    owner.record_legacy_missed_window(
        registration=registration,
        checkpoint_key=CHECKPOINT,
        candidate_set_id=first[0].candidate_set_id,
        triage_authority=triage,
    )
    candidate, _, decision, cluster = _triage(
        registration, checkpoint_key=CHECKPOINT, selected=True, seed_offset=400
    )
    core = {
        **candidate.core_dict(),
        "route_admission_id": "prospective-checkpoint-route-admission-" + "a" * 64,
    }
    different = event_impact_triage_candidate_set_from_dict(
        {**core, "candidate_set_id": "event-impact-triage-candidate-set-" + canonical_hash(core)}
    )
    proposal = EventImpactTriageProposal.build(candidate_set=different, clusters=(cluster,))
    evidence = cast(TriageWorkDecisionEvidence, decision.run_evidence)
    triage.admit_work(
        candidate_set=different,
        proposal=proposal,
        run_evidence=evidence,
        run_authority=RecordingWorkRunAuthority(
            different.candidate_set_id, proposal.proposal_id, evidence
        ),
        decided_at=decision.decided_at,
    )
    with pytest.raises(ValueError, match="conflicts with requested anchor/epoch"):
        owner.record_legacy_missed_window(
            registration=registration,
            checkpoint_key=CHECKPOINT,
            candidate_set_id=different.candidate_set_id,
            triage_authority=triage,
        )


@pytest.mark.parametrize(
    "field,value",
    [
        ("kind", "archive"),
        ("reason", "deadline_expired"),
        ("proven_deadline", "2026-09-01T07:30:00Z"),
    ],
)
def test_disposition_cannot_claim_an_unsupported_status_or_deadline(
    tmp_path: Path, field: str, value: str
) -> None:
    registration = _registration()
    store = LocalDataSnapshotStore(tmp_path / "state")
    triage = EventImpactTriageDecisionStore(store.root)
    first = _persist(triage, _triage(registration, checkpoint_key=CHECKPOINT, selected=True))
    disposition = ProspectiveTriggerAdmissionStore(
        store, clock=lambda: NOW
    ).record_legacy_missed_window(
        registration=registration,
        checkpoint_key=CHECKPOINT,
        candidate_set_id=first[0].candidate_set_id,
        triage_authority=triage,
    )
    core = {**disposition.core_dict(), field: value}
    with pytest.raises(ValueError, match=r"unsupported|proven deadline"):
        checkpoint_disposition_from_dict(
            {**core, "disposition_id": "checkpoint-disposition-" + canonical_hash(core)}
        )


def test_retirement_accepts_only_legacy_cutoff_type(tmp_path: Path) -> None:
    class AnchoredCutoff(DiagnosticCutoffRule):
        pass

    registration = _registration()
    checkpoint = registration.checkpoint(CHECKPOINT)
    cutoff = checkpoint.cutoff
    anchored = AnchoredCutoff(
        cutoff.timezone,
        cutoff.session_boundary,
        cutoff.market_close_local,
        cutoff.decision_delay_seconds,
    )
    replacement = replace(checkpoint, cutoff=anchored)
    modified = replace(
        registration,
        checkpoints=tuple(
            replacement if item.checkpoint_key == CHECKPOINT else item
            for item in registration.checkpoints
        ),
    )
    store = LocalDataSnapshotStore(tmp_path / "state")
    with pytest.raises(ValueError, match="legacy unanchored"):
        ProspectiveTriggerAdmissionStore(store).record_legacy_missed_window(
            registration=modified,
            checkpoint_key=CHECKPOINT,
            candidate_set_id="event-impact-triage-candidate-set-" + "0" * 64,
            triage_authority=EventImpactTriageDecisionStore(store.root),
        )


def test_same_ready_review_cannot_be_bypassed_by_decision_hash_order(tmp_path: Path) -> None:
    registration = _registration()
    store = LocalDataSnapshotStore(tmp_path / "state")
    triage = EventImpactTriageDecisionStore(store.root)
    selected = _persist(triage, _triage(registration, checkpoint_key=CHECKPOINT, selected=True))
    review = next(
        context
        for offset in range(100, 200)
        if (
            context := _triage(
                registration, checkpoint_key=CHECKPOINT, needs_review=True, seed_offset=offset
            )
        )[2].decision_id
        > selected[2].decision_id
    )
    _persist(triage, review)
    candidate, proposal, decision, cluster = selected
    assert (
        candidate.observations[0].first_available_at == review[0].observations[0].first_available_at
    )
    admission = admit_prospective_trigger(
        registration=registration,
        candidate_set=candidate,
        proposal=proposal,
        decision=decision,
        cluster_id=cluster.cluster_id,
        admitted_at=NOW,
    )
    owner = ProspectiveTriggerAdmissionStore(store, clock=lambda: NOW)
    with pytest.raises(ValueError, match=review[3].cluster_id):
        owner.record(
            admission,
            registration=registration,
            candidate_set=candidate,
            proposal=proposal,
            decision=decision,
            triage_authority=triage,
        )
    assert owner.inspect_checkpoint(
        registration=registration,
        candidate_set_id=candidate.candidate_set_id,
        triage_authority=triage,
    )["blocking_review_cluster_ids"] == [review[3].cluster_id]


@pytest.mark.parametrize("route", ["archive", "attention_watch", "event_assessment"])
def test_generic_real_wake_never_resolves_original_review(tmp_path: Path, route: str) -> None:
    executor, prepared, _ = _prepared(tmp_path, eligible_remaining=True)
    parent, proposal, decision, review = executor.decision_store.get_cluster_context(
        prepared.plan.parent_cluster_id
    )
    original = tuple(item.to_dict() for item in (parent, proposal, decision))
    response = _ineligible_draft(prepared.candidate_set)
    child = cast(list[dict[str, object]], response["clusters"])[0]
    child["recommended_route"] = route
    child["event_archetypes"] = ["issuer_corporate"]
    child["changed_facts"] = ["A new item does not answer the original parent question."]
    child["transmission_channels"] = ["risk_uncertainty_insurance"]
    child["watch_questions"] = ["Has a binding follow-up arrived?"]
    result = asyncio.run(executor.run(prepared, provider=FixtureProvider((response,))))
    assert result.decision is not None
    now = result.decision.decided_at + timedelta(seconds=1)
    owner = ProspectiveTriggerAdmissionStore(executor.store, clock=lambda: now)
    preview = owner.inspect_checkpoint(
        registration=executor.registration,
        candidate_set_id=parent.candidate_set_id,
        triage_authority=executor.decision_store,
    )
    assert preview["blocking_review_cluster_ids"] == [review.cluster_id]
    assert preview["selection_ready"] is False
    assert preview["admission_allowed"] is False
    assert preview["checkpoint_disposition"] is None
    assert original == tuple(
        item.to_dict() for item in executor.decision_store.get_context(parent.candidate_set_id)
    )
    with sqlite3.connect(executor.store.index_path) as connection:
        assert connection.execute(
            "SELECT count(*) FROM prospective_trigger_admissions"
        ).fetchone() == (0,)
        assert connection.execute("SELECT count(*) FROM checkpoint_dispositions").fetchone() == (0,)
