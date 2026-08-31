from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import cast

import pytest

from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.event_impact_triage import EventImpactTriageCandidateSet
from market_impact_agent.prospective_triage import (
    ProspectiveTriageActiveBatchRecord,
    ProspectiveTriageActiveBatchStore,
)


class _Candidate:
    candidate_set_id = "event-impact-triage-candidate-set-" + "e" * 64
    registration_id = "prospective-diagnostic-registration-" + "a" * 64
    checkpoint_key = "next-material-a-share-event"
    route_plan_id = "prospective-checkpoint-route-plan-" + "b" * 64
    route_admission_id = "prospective-checkpoint-route-admission-" + "c" * 64
    readiness_report_id = "prospective-checkpoint-readiness-report-" + "6" * 64

    def to_dict(self) -> dict[str, object]:
        return {"candidate_set_id": self.candidate_set_id}


def _record(
    *, seed: str, candidate_hash: str, created_at: datetime
) -> ProspectiveTriageActiveBatchRecord:
    hashes = {
        "readiness": "1" * 64,
        "selection": "2" * 64,
        "candidate_set": candidate_hash,
        "work_manifest": "4" * 64,
        "execution_plan": "5" * 64,
    }
    provisional = ProspectiveTriageActiveBatchRecord(
        batch_id="pending",
        registration_id="prospective-diagnostic-registration-" + "a" * 64,
        checkpoint_key="next-material-a-share-event",
        route_plan_id="prospective-checkpoint-route-plan-" + "b" * 64,
        route_admission_id="prospective-checkpoint-route-admission-" + "c" * 64,
        readiness_report_id="prospective-checkpoint-readiness-report-" + seed * 64,
        unclassified_candidate_count=32,
        data_snapshot_id="data-snapshot-" + seed * 64,
        profile_id="model-provider-" + "d" * 64,
        protocol_artifact_hashes=hashes,
        created_at=created_at,
    )
    return replace(
        provisional,
        batch_id=("prospective-triage-active-batch-" + canonical_hash(provisional.core_dict())),
    )


def test_active_batch_prevents_overlap_until_exact_completion(tmp_path: Path) -> None:
    store = ProspectiveTriageActiveBatchStore(tmp_path)
    candidate = _Candidate()
    candidate_hash = canonical_hash(candidate.to_dict())
    first = _record(
        seed="6",
        candidate_hash=candidate_hash,
        created_at=datetime(2026, 8, 31, 8, tzinfo=UTC),
    )
    second = _record(
        seed="7",
        candidate_hash=candidate_hash,
        created_at=first.created_at + timedelta(minutes=1),
    )
    third = _record(
        seed="8",
        candidate_hash=candidate_hash,
        created_at=first.created_at + timedelta(minutes=3),
    )
    lookup = {
        "registration_id": first.registration_id,
        "checkpoint_key": first.checkpoint_key,
        "route_plan_id": first.route_plan_id,
        "route_admission_id": first.route_admission_id,
    }

    assert (
        store._install_record(  # pyright: ignore[reportPrivateUsage]
            first, expected_epoch_revision=0
        )
        == first
    )
    assert (
        store._install_record(  # pyright: ignore[reportPrivateUsage]
            second, expected_epoch_revision=0
        )
        == first
    )
    assert store.active(**lookup) == first

    with pytest.raises(KeyError, match="unknown event impact Triage Candidate Set"):
        store.complete(
            batch_id=first.batch_id,
            candidate_set=cast(EventImpactTriageCandidateSet, candidate),
            state_root=tmp_path / "state",
        )
    assert store.active(**lookup) == first
    with store._connect() as connection:  # pyright: ignore[reportPrivateUsage]
        connection.execute(
            """
            INSERT INTO prospective_triage_completions(batch_id, decision_id, completed_at)
            VALUES (?, ?, ?)
            """,
            (
                first.batch_id,
                "event-impact-triage-decision-" + "8" * 64,
                (first.created_at + timedelta(minutes=2)).isoformat(),
            ),
        )
        connection.execute(
            "DELETE FROM prospective_triage_active_heads WHERE batch_id = ?",
            (first.batch_id,),
        )
        connection.execute(
            """
            INSERT INTO prospective_triage_epoch_revisions(route_epoch_key, revision)
            VALUES (?, 1)
            """,
            (
                store.route_epoch_key(
                    registration_id=first.registration_id,
                    checkpoint_key=first.checkpoint_key,
                    route_plan_id=first.route_plan_id,
                    route_admission_id=first.route_admission_id,
                ),
            ),
        )
    assert store.active(**lookup) is None
    with pytest.raises(ValueError, match="completed prospective Triage batch"):
        store._install_record(  # pyright: ignore[reportPrivateUsage]
            first, expected_epoch_revision=1
        )
    with pytest.raises(ValueError, match="epoch advanced"):
        store._install_record(  # pyright: ignore[reportPrivateUsage]
            second, expected_epoch_revision=0
        )
    assert (
        store._install_record(  # pyright: ignore[reportPrivateUsage]
            third, expected_epoch_revision=1
        )
        == third
    )
    assert store.active(**lookup) == third
