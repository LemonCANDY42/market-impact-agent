import asyncio
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, cast

import pytest

from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.event_impact_triage import (
    EventImpactTriageCandidateSet,
    TriageObservationRef,
)
from market_impact_agent.event_impact_triage_store import EventImpactTriageDecisionStore
from market_impact_agent.event_impact_triage_work_evaluation import (
    EventImpactTriageWorkComparisonRegistration,
    EventImpactTriageWorkComparisonReport,
    EventImpactTriageWorkComparisonStore,
)
from market_impact_agent.event_impact_triage_work_runtime import (
    EVENT_IMPACT_TRIAGE_WORK_EXECUTION_PLAN_SCHEMA_V9,
)
from market_impact_agent.prospective_diagnostic import ProspectiveDiagnosticRegistration
from market_impact_agent.prospective_triage import (
    PreparedProspectiveTriageWork,
    ProspectiveTriageActiveBatchRecord,
    ProspectiveTriageActiveBatchStore,
    run_prepared_prospective_triage_work,
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


class _FailedComparison:
    comparison_id = "event-impact-triage-work-comparison-" + "d" * 64

    def __init__(self, candidate_set_id: str) -> None:
        self.candidate_set_id = candidate_set_id

    def to_dict(self) -> dict[str, object]:
        return {
            "comparison_id": self.comparison_id,
            "candidate_set_id": self.candidate_set_id,
        }


class _FailedReport:
    report_id = "event-impact-triage-work-comparison-report-" + "e" * 64
    comparison_id = _FailedComparison.comparison_id
    batch_gate_passed = False
    blockers = ("treatment_missed_must_catch_eligible",)

    def __init__(self, evaluated_at: datetime) -> None:
        self.evaluated_at = evaluated_at

    def to_dict(self) -> dict[str, object]:
        return {
            "report_id": self.report_id,
            "comparison_id": self.comparison_id,
            "batch_gate_passed": self.batch_gate_passed,
            "blockers": list(self.blockers),
            "evaluated_at": self.evaluated_at.isoformat().replace("+00:00", "Z"),
        }


def _real_candidate(created_at: datetime) -> EventImpactTriageCandidateSet:
    observation = TriageObservationRef(
        version_id="prospective-observation-version-" + "1" * 64,
        observation_id="source-observation-" + "2" * 64,
        first_available_at=created_at,
        authority_at=created_at,
        provider_id="fixture-provider",
        provider_version="fixture-v1",
        upstream_source="fixture-source",
        source_ref="fixture://event/1",
        raw_content_hash="3" * 64,
        normalized_payload_hash="4" * 64,
    )
    core = {
        "schema_version": "market-impact.event-impact-triage-candidate-set.v1",
        "registration_id": "prospective-diagnostic-registration-" + "a" * 64,
        "checkpoint_key": "next-material-a-share-event",
        "route_plan_id": "prospective-checkpoint-route-plan-" + "b" * 64,
        "route_admission_id": "prospective-checkpoint-route-admission-" + "c" * 64,
        "readiness_report_id": "prospective-checkpoint-readiness-report-" + "6" * 64,
        "data_snapshot_id": "data-snapshot-" + "6" * 64,
        "admitted_at": created_at.isoformat().replace("+00:00", "Z"),
        "frozen_at": created_at.isoformat().replace("+00:00", "Z"),
        "observations": [observation.to_dict()],
        "historical_pit_claim": False,
        "judgment_model_calls_authorized": False,
        "execution_capability": False,
    }
    return EventImpactTriageCandidateSet(
        candidate_set_id="event-impact-triage-candidate-set-" + canonical_hash(core),
        registration_id=cast(str, core["registration_id"]),
        checkpoint_key=cast(str, core["checkpoint_key"]),
        route_plan_id=cast(str, core["route_plan_id"]),
        route_admission_id=cast(str, core["route_admission_id"]),
        readiness_report_id=cast(str, core["readiness_report_id"]),
        data_snapshot_id=cast(str, core["data_snapshot_id"]),
        admitted_at=created_at,
        frozen_at=created_at,
        observations=(observation,),
    )


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


def test_unverified_failed_batch_cannot_release_head_or_exclude_versions(
    tmp_path: Path,
) -> None:
    run_root = tmp_path / "runs"
    state_root = tmp_path / "state"
    active_store = ProspectiveTriageActiveBatchStore(run_root)
    created_at = datetime(2026, 8, 31, 8, tzinfo=UTC)
    candidate = _real_candidate(created_at)
    record = _record(
        seed="6",
        candidate_hash=canonical_hash(candidate.to_dict()),
        created_at=created_at,
    )
    active_store._install_record(  # pyright: ignore[reportPrivateUsage]
        record, expected_epoch_revision=0
    )
    comparison = _FailedComparison(candidate.candidate_set_id)
    report = _FailedReport(created_at + timedelta(minutes=1))
    lookup = {
        "registration_id": record.registration_id,
        "checkpoint_key": record.checkpoint_key,
        "route_plan_id": record.route_plan_id,
        "route_admission_id": record.route_admission_id,
    }

    with pytest.raises((TypeError, ValueError), match=r"durable|registered"):
        EventImpactTriageDecisionStore(state_root).terminalize_failed_work_comparison(
            candidate_set=candidate,
            comparison=cast(EventImpactTriageWorkComparisonRegistration, comparison),
            report=cast(EventImpactTriageWorkComparisonReport, report),
            label_set=cast(Any, object()),
            work_manifest=cast(Any, object()),
            baseline=cast(Any, object()),
            treatment=cast(Any, object()),
            baseline_authority=cast(Any, object()),
            treatment_authority=cast(Any, object()),
            comparison_authority=EventImpactTriageWorkComparisonStore(
                run_root / "comparison" / "registrations.sqlite3"
            ),
            terminalized_at=created_at + timedelta(minutes=2),
        )

    assert active_store.active(**lookup) == record
    assert active_store.epoch_revision(**lookup) == 0
    with pytest.raises(KeyError, match="unknown event impact Triage Candidate Set"):
        EventImpactTriageDecisionStore(state_root).get_context(candidate.candidate_set_id)
    assert (
        EventImpactTriageDecisionStore(state_root).classified_version_ids(
            **lookup,
            at=created_at + timedelta(minutes=2),
        )
        == ()
    )


def test_v9_ordinary_path_is_rejected_before_provider_or_decision(tmp_path: Path) -> None:
    prepared = cast(PreparedProspectiveTriageWork, _PreparedComparisonCandidate())
    provider = _ForbiddenProvider()

    with pytest.raises(ValueError, match="v9 is comparison-governed"):
        asyncio.run(
            run_prepared_prospective_triage_work(
                prepared=prepared,
                registration=cast(ProspectiveDiagnosticRegistration, object()),
                state_root=tmp_path / "state",
                run_root=tmp_path,
                skill_root=tmp_path / "skills",
                provider=cast(Any, provider),
            )
        )

    assert provider.availability_calls == 0
    assert provider.completion_calls == 0
    with pytest.raises(KeyError, match="unknown event impact Triage Candidate Set"):
        EventImpactTriageDecisionStore(tmp_path / "state").get_context(_Candidate.candidate_set_id)
    with ProspectiveTriageActiveBatchStore(tmp_path)._connect() as connection:  # pyright: ignore[reportPrivateUsage]
        assert (
            connection.execute("SELECT COUNT(*) FROM prospective_triage_completions").fetchone()[0]
            == 0
        )


class _PreparedComparisonCandidate:
    candidate_set = _Candidate()
    plan = type(
        "V9Plan",
        (),
        {"schema_version": EVENT_IMPACT_TRIAGE_WORK_EXECUTION_PLAN_SCHEMA_V9},
    )()


class _ForbiddenProvider:
    availability_calls = 0
    completion_calls = 0

    async def assert_model_available(self, *, timeout_seconds: float) -> None:
        _ = timeout_seconds
        self.availability_calls += 1

    async def complete(self, **_kwargs: object) -> object:
        self.completion_calls += 1
        raise AssertionError("v9 ordinary run called the Provider")
