from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass, replace
from datetime import timedelta
from pathlib import Path
from typing import cast

from market_impact_agent.agent_engine import CancellationToken
from market_impact_agent.agent_runtime import ModelTurn
from market_impact_agent.agent_schema import validate_agent_contract
from market_impact_agent.agent_watch_admission import (
    AgentWatchAdmissionService,
    EventImpactTriageWatchAuthorityResolver,
    WatchAdmissionOutcome,
    WatchDelegateProfile,
    build_callback_agent_profile_ref,
)
from market_impact_agent.agent_watch_wake_dispatch import AgentWatchWakeDispatcher
from market_impact_agent.agent_watch_wake_judgment import AgentWatchWakeJudgmentExecutor
from market_impact_agent.attention_watch import AttentionWake
from market_impact_agent.event_impact_triage import (
    EVENT_IMPACT_TRIAGE_CANDIDATE_SET_SCHEMA_V2,
    TriageAgentRole,
    event_impact_triage_candidate_set_from_dict,
)
from market_impact_agent.model_provider import load_builtin_model_provider_profile
from market_impact_agent.prospective_data import ProspectiveCollectionPolicy
from market_impact_agent.prospective_diagnostic import ProspectiveDiagnosticRegistration
from market_impact_agent.runtime_store import ArtifactStore, RunJournal, RunStatus

from .test_agent_watch_admission import (
    _triage_request,  # pyright: ignore[reportPrivateUsage]
    _triage_setup,  # pyright: ignore[reportPrivateUsage]
)
from .test_attention_watch import snapshot_for_monitoring_test
from .test_event_impact_triage_runtime import (
    ROOT,
    FixtureProvider,
    _ineligible_draft,  # pyright: ignore[reportPrivateUsage]
    _registration,  # pyright: ignore[reportPrivateUsage]
)

CPA_ALIAS = "cliproxyapi-luna-xhigh-cpa-v1"


class _RepairFixtureProvider(FixtureProvider):
    async def complete(
        self,
        *,
        messages: tuple[dict[str, object], ...],
        tools: tuple[dict[str, object], ...],
        temperature: float,
        top_p: float,
        max_output_tokens: int,
        timeout_seconds: float,
    ) -> ModelTurn:
        turn = await super().complete(
            messages=messages,
            tools=tools,
            temperature=temperature,
            top_p=top_p,
            max_output_tokens=max_output_tokens,
            timeout_seconds=timeout_seconds,
        )
        content = cast(str, turn.assistant_message["content"])
        malformed = content[:-1]
        return replace(
            turn,
            assistant_message={"role": "assistant", "content": malformed},
            raw_response={"id": turn.response_id, "content": malformed},
        )


@dataclass(frozen=True)
class _RegistrationView:
    registration_id: str
    source: ProspectiveDiagnosticRegistration

    def checkpoint(self, checkpoint_key: str):  # type: ignore[no-untyped-def]
        return self.source.checkpoint(checkpoint_key)


def _real_profile(
    base: WatchDelegateProfile,
    *,
    collection_policy_id: str,
    input_limit: int,
) -> WatchDelegateProfile:
    manifest = json.loads(
        (ROOT / "skills" / "news-evidence-assessment" / "skill.json").read_text(encoding="utf-8")
    )
    manifest_hash = cast(str, manifest["manifest_hash"])
    model_profile = load_builtin_model_provider_profile(CPA_ALIAS)
    callback_agent_profile_ref = build_callback_agent_profile_ref(
        callback_agent_type=base.callback_agent_type,
        model_profile_id=model_profile.profile_id,
        model_profile_hash=model_profile.profile_hash,
        preloaded_skills=("news-evidence-assessment",),
        skill_manifest_hashes=(manifest_hash,),
        max_turns=base.callback_max_turns,
        max_input_tokens=input_limit,
        max_output_tokens=base.callback_max_output_tokens,
        max_cost_microusd=base.callback_max_cost_microusd,
    )
    return WatchDelegateProfile.build(
        name=base.name,
        description=base.description,
        callback_agent_type=base.callback_agent_type,
        callback_agent_profile_ref=callback_agent_profile_ref,
        allowed_parent_agent_types=base.allowed_parent_agent_types,
        allowed_subject_kinds=base.allowed_subject_kinds,
        preloaded_skills=("news-evidence-assessment",),
        skill_manifest_hashes=(manifest_hash,),
        required_capabilities=base.required_capabilities,
        query_template=base.query_template,
        collection_policy_id=collection_policy_id,
        use_class=base.use_class,
        freshness_max_age_seconds=base.freshness_max_age_seconds,
        minimum_coverage_sources=base.minimum_coverage_sources,
        maximum_polls=base.maximum_polls,
        maximum_bytes=base.maximum_bytes,
        maximum_wakes=base.maximum_wakes,
        cooldown_seconds=base.cooldown_seconds,
        active_duration_seconds=base.active_duration_seconds,
        maximum_lineage_depth=base.maximum_lineage_depth,
        maximum_children_per_parent=base.maximum_children_per_parent,
        maximum_active_watches=base.maximum_active_watches,
        callback_max_turns=base.callback_max_turns,
        callback_max_input_tokens=input_limit,
        callback_max_output_tokens=base.callback_max_output_tokens,
        callback_max_cost_microusd=base.callback_max_cost_microusd,
    )


def _prepared(
    tmp_path: Path,
    *,
    input_limit: int = 20_000,
    eligible_remaining: bool = False,
):
    store, journal, _, old_profile, old_service, authority = _triage_setup(
        tmp_path, eligible_remaining=eligible_remaining
    )
    context = authority.delegation_context()
    admitted_at = context.created_at + timedelta(seconds=1)
    old_policy = journal.policy(old_profile.collection_policy_id)
    policy = ProspectiveCollectionPolicy.build(
        capability=old_policy.capability,
        sources=old_policy.sources,
        window_start=admitted_at - timedelta(minutes=1),
        parameters={"keywords": ["alpha", "safety"], "max_items": 20},
        poll_interval_seconds=60,
        maximum_gap_seconds=90,
    )
    journal.register_policy(policy)
    initial_collection = snapshot_for_monitoring_test(
        store,
        policy=policy,
        retrieved_at=admitted_at,
        headline="Alpha safety initial report",
        raw_record=b'{"headline":"Alpha safety initial report"}',
    )
    journal.record_snapshot(initial_collection, policy=policy)
    baseline = journal.freeze_snapshot(
        policy_id=policy.policy_id,
        not_after=admitted_at,
        window_start=policy.window_start,
        frozen_at=admitted_at,
    )
    profile = _real_profile(
        old_profile,
        collection_policy_id=policy.policy_id,
        input_limit=input_limit,
    )
    service = AgentWatchAdmissionService(
        store,
        profiles=(profile,),
        delegation_authority=authority,
        journal=journal,
        watch_service=old_service.watch_service,
    )
    admission = service.admit(
        _triage_request(profile_id=profile.profile_id, context=context),
        context=context,
        initial_data_snapshot_id=baseline.snapshot_id,
        decided_at=admitted_at,
    )
    assert admission.outcome is WatchAdmissionOutcome.ADMITTED
    assert admission.watch_id is not None
    changed_at = admitted_at + timedelta(minutes=1)
    changed = snapshot_for_monitoring_test(
        store,
        policy=journal.policy(profile.collection_policy_id),
        retrieved_at=changed_at,
        headline="Alpha safety binding follow-up",
        raw_record=b'{"headline":"Alpha safety binding follow-up"}',
    )
    wake_result = service.watch_service.run_due(
        admission.watch_id,
        now=changed_at,
        collector=lambda _: changed,
    )
    assert wake_result.wake is not None
    dispatcher = AgentWatchWakeDispatcher(
        service,
        run_journal=RunJournal(tmp_path / "dispatch" / "runs.sqlite3"),
    )
    dispatch = dispatcher.dispatch_wake(
        wake_result.wake,
        dispatched_at=changed_at + timedelta(seconds=1),
    )[0]
    source_registration = _registration()
    registration = cast(
        ProspectiveDiagnosticRegistration,
        _RegistrationView(
            registration_id=authority.decision_store.get_context(authority.candidate_set_id)[
                0
            ].registration_id,
            source=source_registration,
        ),
    )
    executor = AgentWatchWakeJudgmentExecutor(
        dispatcher,
        registration=registration,
        model_profile_alias_by_agent_profile_ref={profile.callback_agent_profile_ref: CPA_ALIAS},
        skill_root=ROOT / "skills",
        runtime_root=tmp_path / "wake-runtime",
    )
    return executor, executor.prepare(dispatch), dispatch


def test_wake_freezes_only_new_versions_and_runs_one_bounded_coordinator(
    tmp_path: Path,
) -> None:
    executor, prepared, _ = _prepared(tmp_path)
    provider = FixtureProvider((_ineligible_draft(prepared.candidate_set),))

    result = asyncio.run(executor.run(prepared, provider=provider))
    replay = asyncio.run(executor.run(prepared, provider=provider))

    assert prepared.candidate_set.schema_version == EVENT_IMPACT_TRIAGE_CANDIDATE_SET_SCHEMA_V2
    assert prepared.candidate_set.origin_wake_id == prepared.plan.wake_id
    assert prepared.candidate_set.parent_cluster_id == prepared.plan.parent_cluster_id
    assert prepared.candidate_set.data_snapshot_id == prepared.plan.frozen_data_snapshot_id
    assert prepared.triage_plan.max_child_count == 0
    coordinator = prepared.triage_plan.binding(TriageAgentRole.COORDINATOR)
    assert coordinator.requested_skills == ("news-evidence-assessment",)
    assert result.triage_result.status is RunStatus.COMPLETED
    assert result.decision is not None
    assert replay.decision == result.decision
    assert len(provider.requests) == 1
    assert prepared.plan.research_only is True
    assert prepared.plan.judgment_model_calls_authorized is True
    assert prepared.plan.execution_capability is False
    assert (
        validate_agent_contract(
            prepared.plan.to_dict(), "agent-watch-wake-judgment-plan.schema.json"
        )
        == ()
    )
    assert (
        validate_agent_contract(
            prepared.candidate_set.to_dict(), "event-impact-triage-candidate-set.schema.json"
        )
        == ()
    )
    assert (
        validate_agent_contract(
            prepared.triage_plan.to_dict(), "event-impact-triage-execution-plan.schema.json"
        )
        == ()
    )
    assert (
        event_impact_triage_candidate_set_from_dict(prepared.candidate_set.to_dict())
        == prepared.candidate_set
    )


def test_wake_callback_reopens_profile_parent_and_terminal_run_after_restart(
    tmp_path: Path,
) -> None:
    executor, prepared, dispatch = _prepared(tmp_path)
    provider = FixtureProvider((_ineligible_draft(prepared.candidate_set),))
    first = asyncio.run(executor.run(prepared, provider=provider))
    assert first.triage_result.status is RunStatus.COMPLETED

    resolver = EventImpactTriageWatchAuthorityResolver(
        executor.store,
        decision_store=executor.decision_store,
    )
    reopened_service = AgentWatchAdmissionService(
        executor.store,
        profiles=(),
        delegation_authority=resolver,
        journal=executor.journal,
        watch_service=executor.dispatcher.watch_service,
    )
    reopened_dispatcher = AgentWatchWakeDispatcher(
        reopened_service,
        run_journal=RunJournal(executor.dispatcher.run_journal.path),
    )
    reopened = reopened_dispatcher.reopen_dispatch(dispatch.binding.run_id)
    assert reopened.run.status is RunStatus.COMPLETED
    assert (
        reopened_service.profiles[prepared.callback.profile.profile_id] == prepared.callback.profile
    )

    restarted_executor = AgentWatchWakeJudgmentExecutor(
        reopened_dispatcher,
        registration=executor.registration,
        model_profile_alias_by_agent_profile_ref={
            prepared.callback.profile.callback_agent_profile_ref: CPA_ALIAS
        },
        skill_root=ROOT / "skills",
        runtime_root=executor.runtime_root,
    )
    replay = asyncio.run(
        restarted_executor.run(restarted_executor.prepare(reopened), provider=provider)
    )

    assert replay.decision == first.decision
    assert len(provider.requests) == 1
    assert reopened_dispatcher.running_dispatches() == ()


def test_wake_callback_repairs_one_structural_json_error_and_preserves_evidence(
    tmp_path: Path,
) -> None:
    executor, prepared, _ = _prepared(tmp_path)
    provider = _RepairFixtureProvider((_ineligible_draft(prepared.candidate_set),))

    result = asyncio.run(executor.run(prepared, provider=provider))

    assert result.triage_result.status is RunStatus.COMPLETED
    member = result.triage_result.members[0]
    artifacts = ArtifactStore(executor.runtime_root / prepared.plan.plan_id / "artifacts")
    terminal = cast(dict[str, object], artifacts.read_json(member.terminal_artifact_hash))
    evidence_hash = cast(str, terminal["json_parse_evidence_hash"])
    evidence = cast(dict[str, object], artifacts.read_json(evidence_hash))
    assert evidence["source_was_strict_json"] is False
    assert evidence["repair_applied"] is True
    assert len(cast(list[object], evidence["structural_edits"])) == 1


def test_wake_callback_budget_exhaustion_is_terminal_without_provider_dispatch(
    tmp_path: Path,
) -> None:
    executor, prepared, _ = _prepared(tmp_path, input_limit=1)
    provider = FixtureProvider((_ineligible_draft(prepared.candidate_set),))

    result = asyncio.run(executor.run(prepared, provider=provider))
    replay = asyncio.run(executor.run(prepared, provider=provider))

    assert result.triage_result.status is RunStatus.BUDGET_EXHAUSTED
    assert result.decision is None
    assert replay.triage_result.status is RunStatus.BUDGET_EXHAUSTED
    assert provider.requests == []


def test_wake_callback_pre_dispatch_cancellation_is_terminal_and_replay_stable(
    tmp_path: Path,
) -> None:
    executor, prepared, _ = _prepared(tmp_path)
    provider = FixtureProvider((_ineligible_draft(prepared.candidate_set),))
    token = CancellationToken()
    token.cancel()

    result = asyncio.run(executor.run(prepared, provider=provider, cancellation=token))
    replay = asyncio.run(executor.run(prepared, provider=provider))

    assert result.triage_result.status is RunStatus.CANCELLED
    assert result.decision is None
    assert replay.triage_result.status is RunStatus.CANCELLED
    assert provider.requests == []


def test_wake_prepare_rejects_a_non_authoritative_wake_projection(tmp_path: Path) -> None:
    executor, _, dispatch = _prepared(tmp_path)
    forged_wake = AttentionWake.build(
        watch_id=dispatch.wake.watch_id,
        data_snapshot_id="data-snapshot-" + "f" * 64,
        prior_data_snapshot_id=dispatch.wake.prior_data_snapshot_id,
        new_version_ids=dispatch.wake.new_version_ids,
        created_at=dispatch.wake.created_at,
    )

    try:
        executor.prepare(replace(dispatch, wake=forged_wake))
    except ValueError as exc:
        assert "different Wake" in str(exc) or "durable authority" in str(exc)
    else:
        raise AssertionError("forged Wake projection was accepted")
