import asyncio
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import cast

import pytest

from market_impact_agent.agent_contracts import canonical_hash, canonical_json_bytes
from market_impact_agent.agent_engine import RunMetrics
from market_impact_agent.agent_runtime import ModelProvider, ModelTurn, ProviderUsage
from market_impact_agent.data_inputs import SourceObservation
from market_impact_agent.event_impact_triage import (
    EVENT_IMPACT_TRIAGE_CANDIDATE_SET_SCHEMA,
    EVENT_IMPACT_TRIAGE_DECISION_SCHEMA_V3,
    CheckpointEligibility,
    EventImpactTriageCandidateSet,
    EventImpactTriageDecision,
    EventImpactTriageProposal,
    TriageClusterProposal,
    TriageDecisionStatus,
    TriageObservationRef,
    TriageRoute,
    TriageWorkDecisionEvidence,
)
from market_impact_agent.event_impact_triage_runtime import TriageCandidateContent
from market_impact_agent.model_provider import load_builtin_model_provider_profile
from market_impact_agent.observations import (
    AvailabilityBasis,
    ObservationCapability,
    ObservationTimes,
    OccurrenceBasis,
)
from market_impact_agent.prospective_data import (
    ProspectiveDataJournal,
    ProspectiveObservationVersionRef,
    prospective_observation_version_id,
)
from market_impact_agent.prospective_diagnostic import (
    ProspectiveDiagnosticRegistration,
    load_prospective_diagnostic_registration,
)
from market_impact_agent.prospective_event_assessment import (
    EventAssessmentRunAuthority,
    EventAssessmentRunner,
    EventAssessmentRunResult,
    ExposureCandidate,
    ExposureCandidateView,
    build_exposure_candidate_view,
    run_prospective_event_assessment,
)
from market_impact_agent.prospective_trigger_admission import (
    MaterialityDisposition,
    ProspectiveEventAssessmentArtifact,
)
from market_impact_agent.provider_reliability import (
    ProviderFailure,
    ProviderGenerationState,
    ProviderHealthStore,
    ProviderRetryDisposition,
)
from market_impact_agent.research import EventArchetype, EventStage, TransmissionChannel
from market_impact_agent.runtime_store import RunJournal, RunStatus
from market_impact_agent.usage_ledger import UsageLedger

from .runtime_fakes import BusinessModelFixture

ROOT = Path(__file__).resolve().parents[1]
NOW = datetime(2026, 9, 1, 6, tzinfo=UTC)


class FixtureProvider(BusinessModelFixture):
    def __init__(self, responses: tuple[dict[str, object], ...]) -> None:
        self.responses = list(responses)
        self.calls = 0
        self.closed = 0

    async def close(self) -> None:
        self.closed += 1

    @property
    def provider_id(self) -> str:
        return "cliproxyapi-openai-compatible"

    @property
    def model(self) -> str:
        return "gpt-5.6-luna"

    async def assert_model_available(self, *, timeout_seconds: float) -> None:
        assert timeout_seconds == 30

    async def answer(
        self,
        *,
        messages: tuple[dict[str, object], ...],
        tools: tuple[dict[str, object], ...],
        temperature: float,
        top_p: float,
        max_output_tokens: int,
        timeout_seconds: float,
    ) -> ModelTurn:
        _ = (messages, temperature, top_p, max_output_tokens, timeout_seconds)
        assert tools == ()
        self.calls += 1
        content = canonical_json_bytes(self.responses.pop(0)).decode()
        return ModelTurn(
            response_id=f"fixture-{self.calls}",
            model=self.model,
            assistant_message={"role": "assistant", "content": content},
            tool_calls=(),
            finish_reason="stop",
            usage=ProviderUsage(input_tokens=900, output_tokens=300),
            raw_response={"id": f"fixture-{self.calls}", "content": content},
            latency_ms=12.0,
        )


class UnavailableFixtureProvider(FixtureProvider):
    async def assert_model_available(self, *, timeout_seconds: float) -> None:
        assert timeout_seconds == 30
        raise ProviderFailure(
            "fixture Provider remains unavailable",
            error_class="fixture_auth",
            diagnostic_code="auth_unavailable",
            request_id="fixture-probe-failed",
            generation_state=ProviderGenerationState.NOT_STARTED,
            retry_disposition=ProviderRetryDisposition.SAFE,
        )


class FixtureExposureJournal:
    def __init__(
        self,
        rows: tuple[tuple[ProspectiveObservationVersionRef, SourceObservation], ...],
    ) -> None:
        self.rows = rows

    def observations_as_of(
        self,
        *,
        capability: ObservationCapability,
        not_after: datetime,
        maximum_versions: int = 10_000,
    ) -> tuple[tuple[ProspectiveObservationVersionRef, SourceObservation], ...]:
        assert capability is ObservationCapability.EXPOSURE_CANDIDATES
        assert maximum_versions == 10_000
        return tuple(item for item in self.rows if item[0].first_available_at <= not_after)


class CrashBeforeUsageRunner(EventAssessmentRunner):
    """Test-only crash-gap simulation after terminal journal commit."""

    def _append_usage(
        self,
        *,
        run_id: str,
        status: RunStatus,
        execution_binding_hash: str,
        terminal_hash: str,
        metrics: RunMetrics,
        recorded_at: datetime,
    ) -> None:
        _ = (
            run_id,
            status,
            execution_binding_hash,
            terminal_hash,
            metrics,
            recorded_at,
        )


class UncheckedExposureBindingRunner(EventAssessmentRunner):
    """Create a self-consistent wrong-parent terminal fixture for authority tests."""

    def _validate_static_bindings(self) -> None:
        pass


def test_event_assessment_run_is_durable_and_authoritative(tmp_path: Path) -> None:
    registration, candidate_set, proposal, decision, cluster, contents = _inputs()
    provider = FixtureProvider((_valid_response(),))
    runner = _runner(
        tmp_path,
        registration,
        candidate_set,
        proposal,
        decision,
        cluster,
        contents,
        provider,
    )

    first = asyncio.run(runner.run())

    assert first.status is RunStatus.COMPLETED
    assert first.assessment is not None
    assert first.materiality is not None
    assert first.materiality.admitted_target_ids == ("510300.SH",)
    assert first.metrics.provider_attempts == 1
    assert first.metrics.estimated_cost_microusd == 540
    assert provider.calls == 1
    assert provider.closed == 0  # A caller-owned shared Provider outlives this runner.
    EventAssessmentRunAuthority(
        run_root=tmp_path / "runs",
        registration=registration,
        skill_root=ROOT / "skills",
    ).assert_authoritative_completed_event_assessment(
        candidate_set=candidate_set,
        proposal=proposal,
        decision=decision,
        assessment=first.assessment,
    )

    reopened = asyncio.run(runner.run())
    assert reopened == first
    assert provider.calls == 1

    tampered = ProspectiveEventAssessmentArtifact.build(
        triage_decision=decision,
        cluster=cluster,
        event_assessment_artifact_hash="f" * 64,
        paths=first.assessment.paths,
        counterevidence=(*first.assessment.counterevidence, "A second unsupported countercase."),
        invalidation_conditions=first.assessment.invalidation_conditions,
        assessed_at=first.assessment.assessed_at,
    )
    with pytest.raises(ValueError):
        EventAssessmentRunAuthority(
            run_root=tmp_path / "runs",
            registration=registration,
            skill_root=ROOT / "skills",
        ).assert_authoritative_completed_event_assessment(
            candidate_set=candidate_set,
            proposal=proposal,
            decision=decision,
            assessment=tampered,
        )


@pytest.mark.parametrize("valid", [True, False])
def test_assessment_releases_its_factory_owned_provider_on_every_terminal(
    tmp_path: Path, valid: bool
) -> None:
    registration, candidate, proposal, decision, cluster, contents = _inputs()
    provider = FixtureProvider((_valid_response() if valid else {"invalid": True},))
    runner = _runner(
        tmp_path,
        registration,
        candidate,
        proposal,
        decision,
        cluster,
        contents,
        None,
        provider_factory=lambda: provider,
    )
    result = asyncio.run(runner.run())
    assert result.status.terminal
    assert provider.closed == 1
    assert asyncio.run(runner.run()) == result
    assert provider.calls == 1 and provider.closed == 1


def test_empty_assessment_path_completes_as_watch_without_retry(tmp_path: Path) -> None:
    registration, candidate_set, proposal, decision, cluster, contents = _inputs()
    provider = FixtureProvider(
        (
            {
                "paths": [],
                "counterevidence": [],
                "invalidation_conditions": [],
                "blockers": ["No evidence-bound listed target mapping is available."],
            },
        )
    )
    result = asyncio.run(
        _runner(
            tmp_path,
            registration,
            candidate_set,
            proposal,
            decision,
            cluster,
            contents,
            provider,
        ).run()
    )

    assert result.status is RunStatus.COMPLETED
    assert result.assessment is None
    assert result.materiality is None
    assert result.disposition is MaterialityDisposition.WATCH
    assert result.blockers == ("No evidence-bound listed target mapping is available.",)
    assert provider.calls == 1
    completed_at = EventAssessmentRunAuthority(
        run_root=tmp_path / "runs",
        registration=registration,
        skill_root=ROOT / "skills",
    ).reopen_completed_watch(
        candidate_set=candidate_set,
        proposal=proposal,
        decision=decision,
        cluster=cluster,
    )
    assert (
        completed_at == RunJournal(tmp_path / "runs/runs.sqlite3").get_run(result.run_id).updated_at
    )


@pytest.mark.parametrize("mismatch", ["candidate_set", "cluster", "cutoff"])
def test_completed_watch_rejects_self_consistent_wrong_exposure_binding(
    tmp_path: Path, mismatch: str
) -> None:
    registration, candidate_set, proposal, decision, cluster, contents = _inputs()
    original = _exposure_view(candidate_set, decision, cluster)
    wrong_view = ExposureCandidateView.build(
        candidate_set_id=(
            f"event-impact-triage-candidate-set-{'f' * 64}"
            if mismatch == "candidate_set"
            else original.candidate_set_id
        ),
        cluster_id=(f"triage-cluster-{'f' * 64}" if mismatch == "cluster" else original.cluster_id),
        cutoff_at=(
            original.cutoff_at + timedelta(minutes=1)
            if mismatch == "cutoff"
            else original.cutoff_at
        ),
        candidates=original.candidates,
    )
    provider = FixtureProvider(
        (
            {
                "paths": [],
                "counterevidence": [],
                "invalidation_conditions": [],
                "blockers": ["No evidence-bound listed target mapping is available."],
            },
        )
    )
    result = asyncio.run(
        _runner(
            tmp_path,
            registration,
            candidate_set,
            proposal,
            decision,
            cluster,
            contents,
            provider,
            runner_class=UncheckedExposureBindingRunner,
            exposure_view=wrong_view,
        ).run()
    )
    assert result.status is RunStatus.COMPLETED

    with pytest.raises(ValueError, match="Exposure Candidate View authority is invalid"):
        EventAssessmentRunAuthority(
            run_root=tmp_path / "runs",
            registration=registration,
            skill_root=ROOT / "skills",
        ).reopen_completed_watch(
            candidate_set=candidate_set,
            proposal=proposal,
            decision=decision,
            cluster=cluster,
        )


def test_terminal_replay_skips_provider_creation_and_recovers_usage(tmp_path: Path) -> None:
    registration, candidate_set, proposal, decision, cluster, contents = _inputs()
    provider = FixtureProvider((_valid_response(),))
    runner = _runner(
        tmp_path,
        registration,
        candidate_set,
        proposal,
        decision,
        cluster,
        contents,
        provider,
        runner_class=CrashBeforeUsageRunner,
    )
    first = asyncio.run(runner.run())
    assert first.status is RunStatus.COMPLETED
    usage_path = tmp_path / "runs/usage.sqlite3"
    assert UsageLedger(usage_path).records() == ()

    factory_calls = 0

    def unavailable_factory() -> ModelProvider:
        nonlocal factory_calls
        factory_calls += 1
        raise ValueError("Model Provider credential is missing: TEST_ONLY")

    reopened = asyncio.run(
        _runner(
            tmp_path,
            registration,
            candidate_set,
            proposal,
            decision,
            cluster,
            contents,
            None,
            provider_factory=unavailable_factory,
        ).run()
    )

    assert reopened == first
    assert factory_calls == 0
    records = UsageLedger(tmp_path / "runs/usage.sqlite3").records()
    assert len(records) == 1
    assert records[0].record.run_id == first.run_id


def test_pre_dispatch_failure_can_resume_but_post_dispatch_interruption_cannot(
    tmp_path: Path,
) -> None:
    registration, candidate_set, proposal, decision, cluster, contents = _inputs()
    provider = FixtureProvider((_valid_response(),))
    health = ProviderHealthStore(tmp_path / "provider-health.sqlite3")
    factory_calls = 0

    def flaky_factory() -> ModelProvider:
        nonlocal factory_calls
        factory_calls += 1
        if factory_calls == 1:
            raise ValueError("Model Provider credential is missing: TEST_ONLY")
        return provider

    runner = _runner(
        tmp_path,
        registration,
        candidate_set,
        proposal,
        decision,
        cluster,
        contents,
        None,
        provider_factory=flaky_factory,
        provider_health_store=health,
    )
    pre_dispatch = asyncio.run(runner.run())
    assert pre_dispatch.status is RunStatus.HUMAN_INPUT_REQUIRED
    assert pre_dispatch.terminal_artifact_hash is None
    journal = RunJournal(tmp_path / "runs/runs.sqlite3")
    record = journal.get_run(pre_dispatch.run_id)
    assert record.status is RunStatus.RUNNING
    assert not any(
        item.event_type == "model.request.dispatched"
        for item in journal.events(pre_dispatch.run_id)
    )

    resumed = asyncio.run(runner.run())
    assert resumed.status is RunStatus.COMPLETED
    assert provider.calls == 1
    assert health.admission(
        "cliproxyapi-openai-compatible",
        now=decision.decided_at + timedelta(minutes=2),
    ).allowed

    second_root = tmp_path / "post-dispatch"
    blocked_provider = FixtureProvider((_valid_response(),))
    interrupted_health = ProviderHealthStore(second_root / "provider-health.sqlite3")
    interrupted_runner = _runner(
        second_root,
        registration,
        candidate_set,
        proposal,
        decision,
        cluster,
        contents,
        None,
        provider_factory=lambda: (_ for _ in ()).throw(
            ValueError("Model Provider credential is missing: TEST_ONLY")
        ),
    )
    interrupted = asyncio.run(interrupted_runner.run())
    interrupted_journal = RunJournal(second_root / "runs/runs.sqlite3")
    interrupted_journal.append(
        run_id=interrupted.run_id,
        event_id=f"{interrupted.run_id}.fixture.dispatched",
        event_type="model.request.dispatched",
        observed_at=decision.decided_at + timedelta(minutes=2),
        payload={"fixture": True},
    )
    replay = _runner(
        second_root,
        registration,
        candidate_set,
        proposal,
        decision,
        cluster,
        contents,
        blocked_provider,
        provider_health_store=interrupted_health,
    )
    post_dispatch = asyncio.run(replay.run())
    assert post_dispatch.status is RunStatus.HUMAN_INPUT_REQUIRED
    assert post_dispatch.terminal_artifact_hash is not None
    assert post_dispatch.metrics.provider_attempts == 1
    assert blocked_provider.calls == 0
    assert not interrupted_health.admission(
        "cliproxyapi-openai-compatible",
        now=decision.decided_at + timedelta(minutes=1),
    ).allowed


def test_provider_health_open_circuit_blocks_dispatch(tmp_path: Path) -> None:
    registration, candidate_set, proposal, decision, cluster, contents = _inputs()
    health = ProviderHealthStore(tmp_path / "provider-health.sqlite3")
    health.record_failure(
        provider_id="cliproxyapi-openai-compatible",
        failure=ProviderFailure(
            "fixture credential unavailable",
            error_class="fixture_auth",
            diagnostic_code="auth_unavailable",
            request_id="fixture-health-open",
            generation_state=ProviderGenerationState.NOT_STARTED,
            retry_disposition=ProviderRetryDisposition.SAFE,
        ),
        physical_attempt=1,
        observed_at=decision.decided_at + timedelta(seconds=1),
    )
    provider = UnavailableFixtureProvider((_valid_response(),))
    result = asyncio.run(
        _runner(
            tmp_path,
            registration,
            candidate_set,
            proposal,
            decision,
            cluster,
            contents,
            provider,
            provider_health_store=health,
        ).run()
    )

    assert result.status is RunStatus.HUMAN_INPUT_REQUIRED
    assert result.terminal_artifact_hash is None
    assert provider.calls == 0
    assert any(
        item.event_type == "provider.admission.blocked"
        for item in RunJournal(tmp_path / "runs/runs.sqlite3").events(result.run_id)
    )


def test_aggregate_budget_blocks_trigger_admission_before_authority(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import market_impact_agent.prospective_event_assessment as module

    registration, candidate_set, proposal, decision, cluster, _contents = _inputs(
        aggregate_model_cost_limit_usd="0.01"
    )

    class FakeTriageStore:
        def __init__(self, state_root: Path) -> None:
            _ = state_root

        def get_context(self, candidate_set_id: str) -> tuple[object, object, object]:
            assert candidate_set_id == candidate_set.candidate_set_id
            return candidate_set, proposal, decision

        def route_epoch_contexts(self, **kwargs: object) -> tuple[tuple[object, ...], ...]:
            _ = kwargs
            return ((candidate_set, proposal, decision, cluster),)

    class FakeResolver:
        def __init__(self, store: object) -> None:
            _ = store

        def resolve(self, selected: object) -> tuple[TriageCandidateContent, ...]:
            _ = selected
            raise AssertionError("budget reservation must run before content resolution")

    class FakeRunner:
        def __init__(self, **kwargs: object) -> None:
            _ = kwargs
            raise AssertionError("budget reservation must run before EventAssessment")

    def forbidden_admission(**kwargs: object) -> None:
        _ = kwargs
        raise AssertionError("admission must not run")

    monkeypatch.setattr(module, "EventImpactTriageDecisionStore", FakeTriageStore)
    monkeypatch.setattr(module, "SnapshotTriageCandidateContentResolver", FakeResolver)
    monkeypatch.setattr(module, "EventAssessmentRunner", FakeRunner)
    monkeypatch.setattr(module, "admit_prospective_trigger", forbidden_admission)

    outcome = asyncio.run(
        run_prospective_event_assessment(
            registration=registration,
            candidate_set_id=candidate_set.candidate_set_id,
            state_root=tmp_path / "state",
            run_root=tmp_path / "runs",
            skill_root=ROOT / "skills",
            provider=FixtureProvider((_valid_response(),)),
        )
    )

    assert outcome.status is RunStatus.BUDGET_EXHAUSTED
    assert outcome.attempted_cluster_count == 0
    assert outcome.admission is None
    assert outcome.assessments == ()
    assert outcome.cluster_dispositions == ()


def test_unresolved_watch_does_not_block_later_assessment_throughput(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import market_impact_agent.prospective_event_assessment as module

    registration, candidate_set, proposal, decision, cluster, contents = _inputs()

    class FakeTriageStore:
        def __init__(self, state_root: Path) -> None:
            _ = state_root

        def get_context(self, candidate_set_id: str) -> tuple[object, object, object]:
            assert candidate_set_id == candidate_set.candidate_set_id
            return candidate_set, proposal, decision

        def route_epoch_contexts(self, **kwargs: object) -> tuple[tuple[object, ...], ...]:
            assert kwargs["at"] == decision.decided_at
            return (
                (candidate_set, proposal, decision, cluster),
                (candidate_set, proposal, decision, cluster),
            )

    class FakeResolver:
        def __init__(self, store: object) -> None:
            _ = store

        def resolve(self, selected: object) -> tuple[TriageCandidateContent, ...]:
            assert selected == candidate_set
            return contents

    results = [MaterialityDisposition.WATCH, MaterialityDisposition.ARCHIVE]

    class FakeRunner:
        def __init__(self, **kwargs: object) -> None:
            _ = kwargs

        async def run(self) -> EventAssessmentRunResult:
            disposition = results.pop(0)
            return EventAssessmentRunResult(
                run_id=f"agent-run-{'a' * 64}",
                status=RunStatus.COMPLETED,
                assessment=None,
                materiality=None,
                disposition=disposition,
                blockers=(),
                terminal_artifact_hash="b" * 64,
                metrics=RunMetrics(0, 0, 0, 0, 0, 0.0, 0, 0),
            )

    monkeypatch.setattr(module, "EventImpactTriageDecisionStore", FakeTriageStore)
    monkeypatch.setattr(module, "SnapshotTriageCandidateContentResolver", FakeResolver)
    monkeypatch.setattr(module, "EventAssessmentRunner", FakeRunner)

    def build_view(**kwargs: object) -> ExposureCandidateView:
        assert kwargs["cutoff_at"] == decision.decided_at
        return _exposure_view(candidate_set, decision, cluster)

    monkeypatch.setattr(
        module,
        "build_exposure_candidate_view",
        build_view,
    )

    outcome = asyncio.run(
        run_prospective_event_assessment(
            registration=registration,
            candidate_set_id=candidate_set.candidate_set_id,
            state_root=tmp_path / "state",
            run_root=tmp_path / "runs",
            skill_root=ROOT / "skills",
            provider=FixtureProvider((_valid_response(),)),
        )
    )

    assert outcome.status is RunStatus.COMPLETED
    assert outcome.attempted_cluster_count == 2
    assert outcome.cluster_dispositions == (
        MaterialityDisposition.WATCH,
        MaterialityDisposition.ARCHIVE,
    )
    assert outcome.admission is None
    assert results == []


def test_assessment_rejects_target_outside_frozen_exposure_view(tmp_path: Path) -> None:
    registration, candidate_set, proposal, decision, cluster, contents = _inputs()
    response = _valid_response()
    path = cast(list[dict[str, object]], response["paths"])[0]
    path["target_id"] = "000001.SZ"
    path["venue"] = "XSHE"
    path["instrument_class"] = "equity"
    provider = FixtureProvider((response,))

    result = asyncio.run(
        _runner(
            tmp_path,
            registration,
            candidate_set,
            proposal,
            decision,
            cluster,
            contents,
            provider,
        ).run()
    )

    assert result.status is RunStatus.FAILED
    assert result.assessment is None
    assert result.disposition is None
    assert provider.calls == 1


def test_exposure_view_uses_actual_receipt_catalog_without_leaking_provenance_to_prompt() -> None:
    _registration, candidate_set, _proposal, decision, cluster, contents = _inputs()
    observation = _exposure_observation(decision.decided_at - timedelta(minutes=1))
    inactive = _exposure_observation(
        decision.decided_at - timedelta(minutes=1),
        target_id="159001.SZ",
        name="Inactive Fixture ETF",
        list_status="D",
    )
    rows = tuple((_exposure_ref(item), item) for item in (observation, inactive))

    view = build_exposure_candidate_view(
        journal=cast(ProspectiveDataJournal, FixtureExposureJournal(rows)),
        candidate_set=candidate_set,
        cluster=cluster,
        contents=contents,
        cutoff_at=decision.decided_at,
    )

    assert view.view_id == view.expected_view_id
    assert view.allowed_targets == frozenset({("510300.SH", "XSHG", "exchange_traded_fund")})
    assert "exposure_candidates:no_exact_mapping_broad_catalog_used" in view.information_gaps
    prompt = view.to_prompt_dict()
    prompt_candidate = cast(list[dict[str, object]], prompt["candidates"])[0]
    assert prompt_candidate["labels"] == ["Fixture Broad ETF"]
    assert "supporting_version_ids" not in prompt_candidate


def _runner(
    tmp_path: Path,
    registration: ProspectiveDiagnosticRegistration,
    candidate_set: EventImpactTriageCandidateSet,
    proposal: EventImpactTriageProposal,
    decision: EventImpactTriageDecision,
    cluster: TriageClusterProposal,
    contents: tuple[TriageCandidateContent, ...],
    provider: ModelProvider | None,
    *,
    provider_factory: Callable[[], ModelProvider] | None = None,
    provider_health_store: ProviderHealthStore | None = None,
    runner_class: type[EventAssessmentRunner] = EventAssessmentRunner,
    exposure_view: ExposureCandidateView | None = None,
) -> EventAssessmentRunner:
    return runner_class(
        registration=registration,
        candidate_set=candidate_set,
        proposal=proposal,
        decision=decision,
        cluster=cluster,
        contents=contents,
        exposure_view=exposure_view or _exposure_view(candidate_set, decision, cluster),
        profile=load_builtin_model_provider_profile(registration.model_profile_id),
        provider=provider,
        provider_factory=provider_factory,
        provider_health_store=provider_health_store,
        skill_root=ROOT / "skills",
        run_root=tmp_path / "runs",
        clock=lambda: decision.decided_at + timedelta(minutes=1),
    )


def _exposure_view(
    candidate_set: EventImpactTriageCandidateSet,
    decision: EventImpactTriageDecision,
    cluster: TriageClusterProposal,
) -> ExposureCandidateView:
    return ExposureCandidateView.build(
        candidate_set_id=candidate_set.candidate_set_id,
        cluster_id=cluster.cluster_id,
        cutoff_at=decision.decided_at,
        candidates=(
            ExposureCandidate(
                target_id="510300.SH",
                venue="XSHG",
                instrument_class="exchange_traded_fund",
                supporting_version_ids=(f"prospective-observation-version-{'c' * 64}",),
                mapping_facts=(
                    {
                        "api_name": "etf_basic",
                        "record": {"ts_code": "510300.SH", "name": "Fixture ETF"},
                    },
                ),
            ),
        ),
    )


def _exposure_observation(
    retrieved_at: datetime,
    *,
    target_id: str = "510300.SH",
    name: str = "Fixture Broad ETF",
    list_status: str = "L",
) -> SourceObservation:
    return SourceObservation.build(
        capability=ObservationCapability.EXPOSURE_CANDIDATES,
        provider_id="fixture-provider",
        provider_version="1",
        upstream_source="fixture-exposure-catalog",
        upstream_record_id=target_id,
        source_ref=f"fixture://exposure/{target_id}",
        lineage_id=f"etf_basic:{target_id}",
        times=ObservationTimes(
            occurred_at=retrieved_at,
            published_at=None,
            available_at=retrieved_at,
            source_updated_at=None,
            aggregator_fetched_at=retrieved_at,
            retrieved_at=retrieved_at,
            occurrence_basis=OccurrenceBasis.RETRIEVAL_OBSERVED,
            availability_basis=AvailabilityBasis.ACTUAL_RECEIPT,
        ),
        authority_at=retrieved_at,
        authority_kind="actual_receipt",
        raw_content_hash="d" * 64,
        normalized_payload={
            "api_name": "etf_basic",
            "record": {
                "ts_code": target_id,
                "cname": name,
                "list_status": list_status,
            },
        },
        license_scope="test-fixture",
    )


def _exposure_ref(observation: SourceObservation) -> ProspectiveObservationVersionRef:
    return ProspectiveObservationVersionRef(
        version_id=prospective_observation_version_id(observation),
        first_available_at=cast(datetime, observation.times.available_at),
        provider_id=observation.provider_id,
        provider_version=observation.provider_version,
        upstream_source=observation.upstream_source,
    )


def _valid_response() -> dict[str, object]:
    return {
        "paths": [
            {
                "target_id": "510300.SH",
                "venue": "XSHG",
                "instrument_class": "exchange_traded_fund",
                "channels": ["risk_uncertainty_insurance"],
                "causal_steps": [
                    "The reported shipping disruption raises near-term broad risk premia."
                ],
                "evidence_ordinals": [1],
                "horizon_sessions": 5,
            }
        ],
        "counterevidence": ["Official traffic data may show no sustained disruption."],
        "invalidation_conditions": ["Authorities confirm normal shipping throughput."],
        "blockers": [],
    }


def _inputs(
    *, aggregate_model_cost_limit_usd: str = "20.00"
) -> tuple[
    ProspectiveDiagnosticRegistration,
    EventImpactTriageCandidateSet,
    EventImpactTriageProposal,
    EventImpactTriageDecision,
    TriageClusterProposal,
    tuple[TriageCandidateContent, ...],
]:
    registration = load_prospective_diagnostic_registration(
        ROOT / "examples/research/prospective-diagnostic-registration-v5.json"
    )
    if aggregate_model_cost_limit_usd != registration.aggregate_model_cost_limit_usd:
        registration = ProspectiveDiagnosticRegistration.build(
            registered_at=registration.registered_at,
            checkpoints=registration.checkpoints,
            paired_arms=registration.paired_arms,
            replicates_per_arm=registration.replicates_per_arm,
            model_profile_id=registration.model_profile_id,
            aggregate_model_cost_limit_usd=aggregate_model_cost_limit_usd,
            outcome_opening_rule=registration.outcome_opening_rule,
            stop_conditions=registration.stop_conditions,
            go_conditions=registration.go_conditions,
            claim_scope=registration.claim_scope,
            minimum_replicates_per_arm=registration.minimum_replicates_per_arm,
            replicate_schedule_rule=registration.replicate_schedule_rule,
            schema_version=registration.schema_version,
        )
    payload: dict[str, object] = {
        "event_id": "fixture-shipping-disruption",
        "headline": "Reported shipping disruption",
        "summary": "A primary shipping route reported a physical disruption.",
    }
    content = TriageCandidateContent(
        version_id=f"prospective-observation-version-{'1' * 64}",
        normalized_payload=payload,
        license_scope="test-fixture",
    )
    observation = TriageObservationRef(
        version_id=content.version_id,
        observation_id=f"source-observation-{'2' * 64}",
        first_available_at=NOW + timedelta(minutes=1),
        authority_at=NOW + timedelta(minutes=1),
        provider_id="fixture-provider",
        provider_version="1",
        upstream_source="fixture-primary",
        source_ref="fixture://event/1",
        raw_content_hash="3" * 64,
        normalized_payload_hash=content.payload_hash,
    )
    core = {
        "schema_version": EVENT_IMPACT_TRIAGE_CANDIDATE_SET_SCHEMA,
        "registration_id": registration.registration_id,
        "checkpoint_key": "next-material-a-share-event",
        "route_plan_id": f"prospective-checkpoint-route-plan-{'4' * 64}",
        "route_admission_id": f"prospective-checkpoint-route-admission-{'5' * 64}",
        "readiness_report_id": f"prospective-checkpoint-readiness-report-{'6' * 64}",
        "data_snapshot_id": f"data-snapshot-{'7' * 64}",
        "admitted_at": NOW.isoformat().replace("+00:00", "Z"),
        "frozen_at": (NOW + timedelta(minutes=2)).isoformat().replace("+00:00", "Z"),
        "observations": [observation.to_dict()],
        "historical_pit_claim": False,
        "judgment_model_calls_authorized": False,
        "execution_capability": False,
    }
    candidate_set = EventImpactTriageCandidateSet(
        candidate_set_id=f"event-impact-triage-candidate-set-{canonical_hash(core)}",
        registration_id=registration.registration_id,
        checkpoint_key="next-material-a-share-event",
        route_plan_id=cast(str, core["route_plan_id"]),
        route_admission_id=cast(str, core["route_admission_id"]),
        readiness_report_id=cast(str, core["readiness_report_id"]),
        data_snapshot_id=cast(str, core["data_snapshot_id"]),
        admitted_at=NOW,
        frozen_at=NOW + timedelta(minutes=2),
        observations=(observation,),
    )
    cluster = TriageClusterProposal.build(
        candidate_version_ids=(content.version_id,),
        checkpoint_eligibility=CheckpointEligibility.NEEDS_REVIEW,
        recommended_route=TriageRoute.EVENT_ASSESSMENT,
        event_archetypes=(EventArchetype.PHYSICAL_SUPPLY_LOGISTICS,),
        event_stage=EventStage.FIRST_OBSERVED,
        changed_facts=("A primary shipping route reported a physical disruption.",),
        rule_reasons=("A concrete financial transmission requires target assessment.",),
        evidence_version_ids=(content.version_id,),
        uncertainty_notes=("Duration and target exposure remain uncertain.",),
        countercases=("The disruption may be brief.",),
        transmission_channels=(TransmissionChannel.RISK_UNCERTAINTY_INSURANCE,),
        affected_entity_refs=("a-share-broad-market",),
        triage_confidence=0.7,
    )
    proposal = EventImpactTriageProposal.build(
        candidate_set=candidate_set,
        clusters=(cluster,),
    )
    decided_at = candidate_set.frozen_at + timedelta(minutes=1)
    evidence = TriageWorkDecisionEvidence(
        plan_id=f"event-impact-triage-work-execution-plan-{'8' * 64}",
        work_manifest_id=f"event-impact-triage-work-manifest-{'9' * 64}",
        completed_member_count=1,
        finished_at=decided_at,
        usage_ledger_hash="a" * 64,
        authority_receipt_hash="b" * 64,
    )
    decision_core = {
        "schema_version": EVENT_IMPACT_TRIAGE_DECISION_SCHEMA_V3,
        "candidate_set_id": candidate_set.candidate_set_id,
        "proposal_id": proposal.proposal_id,
        "run_evidence": evidence.to_dict(),
        "status": TriageDecisionStatus.NEEDS_REVIEW.value,
        "selected_cluster_id": None,
        "blocking_review_cluster_ids": [cluster.cluster_id],
        "unselected_eligible_cluster_ids": [],
        "event_assessment_cluster_ids": [cluster.cluster_id],
        "attention_watch_cluster_ids": [],
        "archive_cluster_ids": [],
        "decided_at": decided_at.isoformat().replace("+00:00", "Z"),
        "historical_pit_claim": False,
        "judgment_model_calls_authorized": False,
        "execution_capability": False,
    }
    decision = EventImpactTriageDecision(
        decision_id=f"event-impact-triage-decision-{canonical_hash(decision_core)}",
        candidate_set_id=candidate_set.candidate_set_id,
        proposal_id=proposal.proposal_id,
        run_evidence=evidence,
        status=TriageDecisionStatus.NEEDS_REVIEW,
        selected_cluster_id=None,
        blocking_review_cluster_ids=(cluster.cluster_id,),
        unselected_eligible_cluster_ids=(),
        event_assessment_cluster_ids=(cluster.cluster_id,),
        attention_watch_cluster_ids=(),
        archive_cluster_ids=(),
        decided_at=decided_at,
        schema_version=EVENT_IMPACT_TRIAGE_DECISION_SCHEMA_V3,
    )
    return registration, candidate_set, proposal, decision, cluster, (content,)
