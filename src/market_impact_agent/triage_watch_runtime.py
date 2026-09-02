"""Production bridge from Triage Attention-Watch routes to durable Wake callbacks.

This module deliberately reuses the existing Collection Journal, Attention Watch,
Agent Watch admission, Run Journal, and Triage Decision authorities.  It adds no
parallel scheduler or callback state machine and never grants execution authority.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.agent_runtime import ModelProvider, SkillRegistry
from market_impact_agent.agent_watch_admission import (
    AgentWatchAdmission,
    AgentWatchAdmissionService,
    AgentWatchRequest,
    EventImpactTriageWatchAuthorityResolver,
    WatchAdmissionOutcome,
    WatchDelegateProfile,
    build_callback_agent_profile_ref,
)
from market_impact_agent.agent_watch_wake_dispatch import AgentWatchWakeDispatcher
from market_impact_agent.agent_watch_wake_judgment import AgentWatchWakeJudgmentExecutor
from market_impact_agent.attention_watch import AttentionWatchService
from market_impact_agent.data_inputs import DataPITLane, LocalDataSnapshotStore
from market_impact_agent.event_impact_triage import TriageClusterProposal
from market_impact_agent.event_impact_triage_store import EventImpactTriageDecisionStore
from market_impact_agent.model_provider import (
    ModelProviderFactory,
    ModelProviderProfile,
    load_builtin_model_provider_profile,
)
from market_impact_agent.monitoring_scope import (
    MonitoringMatchMode,
    MonitoringSubjectKind,
    MonitoringUseClass,
    ObservationMatchClause,
    ObservationMatcher,
    RegisteredQueryTemplate,
)
from market_impact_agent.observations import ObservationCapability
from market_impact_agent.prospective_data import ProspectiveCollectionPolicy, ProspectiveDataJournal
from market_impact_agent.prospective_diagnostic import ProspectiveDiagnosticRegistration
from market_impact_agent.prospective_event_assessment import EventAssessmentRunAuthority
from market_impact_agent.runtime_store import RunJournal, RunStatus

TRIAGE_FOLLOW_UP_CALLBACK_AGENT_TYPE = "triage.follow-up-coordinator"
TRIAGE_FOLLOW_UP_SKILL = "news-evidence-assessment"
TRIAGE_FOLLOW_UP_ACTIVE_SECONDS = 7 * 24 * 60 * 60
TRIAGE_FOLLOW_UP_MAX_BYTES = 128 * 1024 * 1024
TRIAGE_FOLLOW_UP_MAX_WAKES = 4
TRIAGE_FOLLOW_UP_CALLBACK_COST_MICROUSD = 20_000
_TERM = re.compile(r"[a-z][a-z0-9._:-]{2,63}|[\u3400-\u9fff]{2,16}")


@dataclass(frozen=True, slots=True)
class TriageWatchAdmissionResult:
    admission: AgentWatchAdmission
    delegate_profile_id: str
    initial_data_snapshot_id: str

    def summary(self) -> dict[str, object]:
        return {
            "admission_id": self.admission.admission_id,
            "outcome": self.admission.outcome.value,
            "blocker": (None if self.admission.blocker is None else self.admission.blocker.value),
            "watch_id": self.admission.watch_id,
            "delegate_profile_id": self.delegate_profile_id,
            "initial_data_snapshot_id": self.initial_data_snapshot_id,
            "research_only": True,
            "execution_capability": False,
        }


def build_triage_follow_up_profile(
    *,
    collection_policy: ProspectiveCollectionPolicy,
    model_profile_alias: str,
    skill_root: Path,
    match_field_path: str,
) -> tuple[WatchDelegateProfile, ModelProviderProfile]:
    if collection_policy.capability is not ObservationCapability.EVENT_REVELATION:
        raise ValueError("Triage follow-up Watch requires an event-revelation route")
    skills = SkillRegistry(skill_root)
    loaded = skills.load(
        (TRIAGE_FOLLOW_UP_SKILL,),
        allowed_capabilities=frozenset({"evidence.read"}),
    )
    direct = tuple(item for item in loaded if item.manifest.name == TRIAGE_FOLLOW_UP_SKILL)
    if len(direct) != 1:
        raise ValueError("Triage follow-up Skill did not resolve uniquely")
    skill_hash = direct[0].manifest.manifest_hash
    model_profile = load_builtin_model_provider_profile(model_profile_alias)
    callback_max_input = min(32_000, model_profile.budget.max_input_tokens)
    callback_max_output = min(6_000, model_profile.budget.max_output_tokens)
    callback_max_cost = min(
        TRIAGE_FOLLOW_UP_CALLBACK_COST_MICROUSD,
        model_profile.budget.max_estimated_cost_microusd or TRIAGE_FOLLOW_UP_CALLBACK_COST_MICROUSD,
    )
    callback_profile_ref = build_callback_agent_profile_ref(
        callback_agent_type=TRIAGE_FOLLOW_UP_CALLBACK_AGENT_TYPE,
        model_profile_id=model_profile.profile_id,
        model_profile_hash=model_profile.profile_hash,
        preloaded_skills=(TRIAGE_FOLLOW_UP_SKILL,),
        skill_manifest_hashes=(skill_hash,),
        max_turns=1,
        max_input_tokens=callback_max_input,
        max_output_tokens=callback_max_output,
        max_cost_microusd=callback_max_cost,
    )
    template_core = {
        "capability": ObservationCapability.EVENT_REVELATION.value,
        "pit_lane": DataPITLane.PROSPECTIVE.value,
        "allowed_match_field_paths": [match_field_path],
        "allowed_match_modes": [MonitoringMatchMode.CONTAINS_ALL.value],
        "maximum_match_clauses": 1,
        "maximum_terms_per_clause": 2,
        "maximum_term_length": 64,
    }
    template = RegisteredQueryTemplate(
        template_ref=f"monitoring-query-template-{canonical_hash(template_core)}",
        capability=ObservationCapability.EVENT_REVELATION,
        pit_lane=DataPITLane.PROSPECTIVE,
        allowed_match_field_paths=(match_field_path,),
        allowed_match_modes=(MonitoringMatchMode.CONTAINS_ALL,),
        maximum_match_clauses=1,
        maximum_terms_per_clause=2,
        maximum_term_length=64,
    )
    maximum_polls = max(
        1,
        TRIAGE_FOLLOW_UP_ACTIVE_SECONDS // collection_policy.poll_interval_seconds,
    )
    return (
        WatchDelegateProfile.build(
            name="event follow-up verifier",
            description=(
                "Track two frozen event anchors and re-open one bounded semantic assessment "
                "only when a new actual-receipt version matches."
            ),
            callback_agent_type=TRIAGE_FOLLOW_UP_CALLBACK_AGENT_TYPE,
            callback_agent_profile_ref=callback_profile_ref,
            allowed_parent_agent_types=(
                "event-assessment.coordinator",
                "triage.coordinator",
            ),
            allowed_subject_kinds=(MonitoringSubjectKind.EVENT_CLUSTER,),
            preloaded_skills=(TRIAGE_FOLLOW_UP_SKILL,),
            skill_manifest_hashes=(skill_hash,),
            required_capabilities=("evidence.read",),
            query_template=template,
            collection_policy_id=collection_policy.policy_id,
            use_class=MonitoringUseClass.LICENSED_INTERNAL,
            freshness_max_age_seconds=collection_policy.maximum_gap_seconds,
            minimum_coverage_sources=1,
            maximum_polls=maximum_polls,
            maximum_bytes=TRIAGE_FOLLOW_UP_MAX_BYTES,
            maximum_wakes=TRIAGE_FOLLOW_UP_MAX_WAKES,
            cooldown_seconds=max(900, collection_policy.poll_interval_seconds),
            active_duration_seconds=TRIAGE_FOLLOW_UP_ACTIVE_SECONDS,
            maximum_lineage_depth=2,
            maximum_children_per_parent=2,
            maximum_active_watches=128,
            callback_max_turns=1,
            callback_max_input_tokens=callback_max_input,
            callback_max_output_tokens=callback_max_output,
            callback_max_cost_microusd=callback_max_cost,
        ),
        model_profile,
    )


def admit_triage_follow_up_watch(
    *,
    state_root: Path,
    cluster_id: str,
    collection_policy_id: str,
    model_profile_alias: str,
    skill_root: Path,
    match_field_path: str,
    admitted_at: datetime,
    registration: ProspectiveDiagnosticRegistration | None = None,
    event_assessment_run_root: Path | None = None,
) -> TriageWatchAdmissionResult:
    _strict_utc(admitted_at, "Triage Watch admitted_at")
    store = LocalDataSnapshotStore(state_root)
    journal = ProspectiveDataJournal(store)
    decision_store = EventImpactTriageDecisionStore(store.root)
    if (registration is None) != (event_assessment_run_root is None):
        raise ValueError("EventAssessment Watch authority requires both registration and run root")
    event_assessment_authority = (
        None
        if registration is None or event_assessment_run_root is None
        else EventAssessmentRunAuthority(
            run_root=event_assessment_run_root,
            registration=registration,
            skill_root=skill_root,
        )
    )
    resolver = EventImpactTriageWatchAuthorityResolver(
        store,
        decision_store=decision_store,
        event_assessment_authority=event_assessment_authority,
    )
    authority = resolver.authority(cluster_id)
    _, _, _, cluster = decision_store.get_cluster_context(cluster_id)
    context = authority.delegation_context()
    policy = journal.policy(collection_policy_id)
    profile, _ = build_triage_follow_up_profile(
        collection_policy=policy,
        model_profile_alias=model_profile_alias,
        skill_root=skill_root,
        match_field_path=match_field_path,
    )
    baseline_window_start = max(
        policy.window_start,
        admitted_at
        - timedelta(
            seconds=max(
                policy.maximum_gap_seconds,
                policy.poll_interval_seconds * 2,
            )
        ),
    )
    baseline = journal.freeze_snapshot(
        policy_id=policy.policy_id,
        not_after=admitted_at,
        window_start=baseline_window_start,
        minimum_data_sources=profile.minimum_coverage_sources,
        frozen_at=admitted_at,
    )
    journal.assert_watch_baseline_snapshot(baseline)
    request = AgentWatchRequest.build(
        delegate_profile_id=profile.profile_id,
        rationale="The authoritative Triage Decision retained this event for bounded follow-up.",
        watch_question=_watch_question(cluster),
        evidence_refs=context.authorized_evidence_refs,
        subject=context.authorized_subjects[0],
        matcher=ObservationMatcher(
            (
                ObservationMatchClause.build(
                    field_path=match_field_path,
                    mode=MonitoringMatchMode.CONTAINS_ALL,
                    terms=_select_matcher_terms(cluster, context.authorized_matcher_terms),
                ),
            )
        ),
    )
    service = AgentWatchAdmissionService(
        store,
        profiles=(profile,),
        delegation_authority=resolver,
        journal=journal,
        watch_service=AttentionWatchService(store, journal=journal),
    )
    admission = service.admit(
        request,
        context=context,
        initial_data_snapshot_id=baseline.snapshot_id,
        decided_at=admitted_at,
    )
    if admission.outcome not in {WatchAdmissionOutcome.ADMITTED, WatchAdmissionOutcome.REUSED}:
        return TriageWatchAdmissionResult(admission, profile.profile_id, baseline.snapshot_id)
    if admission.watch_id is None:
        raise AssertionError("accepted Triage follow-up admission lacks a Watch")
    return TriageWatchAdmissionResult(admission, profile.profile_id, baseline.snapshot_id)


async def run_triage_watch_wake_callbacks(
    *,
    state_root: Path,
    run_root: Path,
    event_assessment_run_root: Path,
    registration: ProspectiveDiagnosticRegistration,
    model_profile_alias: str,
    skill_root: Path,
    dispatched_at: datetime,
    maximum_callbacks: int = 4,
    provider: ModelProvider | None = None,
) -> dict[str, object]:
    _strict_utc(dispatched_at, "Triage Watch callback dispatched_at")
    if not 1 <= maximum_callbacks <= 32:
        raise ValueError("maximum_callbacks must be between one and 32")
    store = LocalDataSnapshotStore(state_root)
    journal = ProspectiveDataJournal(store)
    decision_store = EventImpactTriageDecisionStore(store.root)
    resolver = EventImpactTriageWatchAuthorityResolver(
        store,
        decision_store=decision_store,
        event_assessment_authority=EventAssessmentRunAuthority(
            run_root=event_assessment_run_root,
            registration=registration,
            skill_root=skill_root,
        ),
    )
    service = AgentWatchAdmissionService(
        store,
        profiles=(),
        delegation_authority=resolver,
        journal=journal,
        watch_service=AttentionWatchService(store, journal=journal),
    )
    dispatch_journal = RunJournal(run_root / "dispatch" / "runs.sqlite3")
    dispatcher = AgentWatchWakeDispatcher(service, run_journal=dispatch_journal)
    new_dispatches = dispatcher.dispatch_pending(dispatched_at=dispatched_at)
    by_run_id = {item.binding.run_id: item for item in dispatcher.running_dispatches()}
    for item in new_dispatches:
        by_run_id[item.binding.run_id] = item
    selected = tuple(by_run_id[key] for key in sorted(by_run_id))[:maximum_callbacks]
    if not selected:
        return {
            "dispatched_count": 0,
            "completed_count": 0,
            "results": [],
            "research_only": True,
            "execution_capability": False,
        }

    model_profile = load_builtin_model_provider_profile(model_profile_alias)
    mappings = {
        profile.callback_agent_profile_ref: model_profile_alias
        for profile in service.profiles.values()
        if _profile_matches_model(profile, model_profile)
    }
    selected_refs = {item.binding.callback_agent_profile_ref for item in selected}
    if not selected_refs <= set(mappings):
        raise ValueError("a pending Wake callback does not match the selected model profile")
    selected_provider = (
        ModelProviderFactory.with_builtin_adapters().create(model_profile)
        if provider is None
        else provider
    )
    executor = AgentWatchWakeJudgmentExecutor(
        dispatcher,
        registration=registration,
        model_profile_alias_by_agent_profile_ref=mappings,
        skill_root=skill_root,
        runtime_root=run_root / "judgment",
    )
    summaries: list[dict[str, object]] = []
    for dispatch in selected:
        prepared = executor.prepare(dispatch)
        result = await executor.run(prepared, provider=selected_provider)
        summaries.append(
            {
                "run_id": dispatch.binding.run_id,
                "wake_id": dispatch.binding.wake_id,
                "plan_id": prepared.plan.plan_id,
                "status": result.triage_result.status.value,
                "decision_id": (None if result.decision is None else result.decision.decision_id),
                "execution_capability": False,
            }
        )
    return {
        "dispatched_count": len(new_dispatches),
        "completed_count": sum(item["status"] == RunStatus.COMPLETED.value for item in summaries),
        "results": summaries,
        "research_only": True,
        "execution_capability": False,
    }


def _profile_matches_model(
    profile: WatchDelegateProfile,
    model_profile: ModelProviderProfile,
) -> bool:
    expected = build_callback_agent_profile_ref(
        callback_agent_type=profile.callback_agent_type,
        model_profile_id=model_profile.profile_id,
        model_profile_hash=model_profile.profile_hash,
        preloaded_skills=profile.preloaded_skills,
        skill_manifest_hashes=profile.skill_manifest_hashes,
        max_turns=profile.callback_max_turns,
        max_input_tokens=profile.callback_max_input_tokens,
        max_output_tokens=profile.callback_max_output_tokens,
        max_cost_microusd=profile.callback_max_cost_microusd,
    )
    return expected == profile.callback_agent_profile_ref


def _watch_question(cluster: TriageClusterProposal) -> str:
    if cluster.watch_questions:
        return cluster.watch_questions[0]
    return "Did a new authoritative fact confirm, contradict, or materially change this event?"


def _select_matcher_terms(
    cluster: TriageClusterProposal,
    authorized_terms: tuple[str, ...],
) -> tuple[str, str]:
    if len(authorized_terms) < 2:
        raise ValueError("Triage follow-up Watch requires two authorized event anchors")
    weighted_text = (
        *(item.casefold() for item in cluster.affected_entity_refs),
        *(item.casefold() for item in cluster.changed_facts),
        *(item.casefold() for item in cluster.watch_questions),
    )
    extracted = [term for text in weighted_text for term in _TERM.findall(text)]
    scores = {term: sum(1 for item in extracted if item == term) for term in authorized_terms}
    ordered = sorted(
        authorized_terms,
        key=lambda item: (-scores[item], -len(item), item),
    )
    selected = tuple(sorted(ordered[:2]))
    if len(selected) != 2:
        raise AssertionError("Triage follow-up matcher selection is not bounded")
    return selected[0], selected[1]


def _strict_utc(value: datetime, name: str) -> None:
    if value.tzinfo is not UTC or value.utcoffset() != timedelta(0):
        raise ValueError(f"{name} must use the UTC singleton")
