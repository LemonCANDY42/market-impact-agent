"""Bounded Agent-proposed Attention Watch admission.

Agents see a filtered list of named delegate profiles and may propose only a
subject, matcher, rationale and evidence.  The Harness injects parent lineage and
owns the route, budgets, durable Watch and eventual callback profile.  This module
does not dispatch a model or expose execution.
"""

from __future__ import annotations

import re
import sqlite3
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import TYPE_CHECKING, cast

from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.attention_watch import (
    AttentionWake,
    AttentionWatchPolicy,
    AttentionWatchService,
    AttentionWatchStatus,
    attention_wake_from_dict,
    attention_watch_policy_from_dict,
)
from market_impact_agent.data_inputs import DataPITLane, LocalDataSnapshotStore, SourceObservation
from market_impact_agent.domain import require_aware
from market_impact_agent.event_impact_triage import (
    EventImpactTriageCandidateSet,
    TriageClusterProposal,
    TriageRoute,
)
from market_impact_agent.event_impact_triage_store import EventImpactTriageDecisionStore
from market_impact_agent.monitoring_scope import (
    EffectiveMembershipContext,
    MonitoringMatchMode,
    MonitoringScope,
    MonitoringSubjectKind,
    MonitoringSubjectRef,
    MonitoringUseClass,
    ObservationMatchClause,
    ObservationMatcher,
    RegisteredQueryTemplate,
    RetrievalPlan,
)
from market_impact_agent.observations import ObservationCapability
from market_impact_agent.prospective_data import (
    ProspectiveDataJournal,
    prospective_observation_version_id,
)

if TYPE_CHECKING:
    from market_impact_agent.prospective_event_assessment import EventAssessmentRunAuthority
    from market_impact_agent.research_thesis_watch import ResearchThesisWatchAuthorityResolver

AGENT_WATCH_REQUEST_SCHEMA = "market-impact.agent-watch-request.v1"
AGENT_WATCH_ADMISSION_SCHEMA = "market-impact.agent-watch-admission.v1"

_CANONICAL_REF = re.compile(r"^[a-z][a-z0-9._:-]{1,127}$")
_PROFILE_REF = re.compile(r"^watch-delegate-profile-[0-9a-f]{64}$")
_REGISTERED_ASPECT_REF = re.compile(r"^information-aspect-[0-9a-f]{64}$")
_ACTIVE_WATCH_STATUSES = (
    AttentionWatchStatus.ACTIVE.value,
    AttentionWatchStatus.BACKING_OFF.value,
    AttentionWatchStatus.TRIGGERED.value,
)
_TRIAGE_COORDINATOR_AGENT_TYPE = "triage.coordinator"
_EVENT_ASSESSMENT_COORDINATOR_AGENT_TYPE = "event-assessment.coordinator"
_MATCHER_TERM = re.compile(r"[a-z][a-z0-9._:-]{2,63}|[\u3400-\u9fff]{2,16}")
_MAX_AUTHORIZED_MATCHER_TERMS = 32
_GENERIC_TRIAGE_MATCHER_TERMS = frozenset(
    {
        "announcement",
        "authority",
        "company",
        "event",
        "follow-up",
        "issuer",
        "market",
        "news",
        "update",
        "事件",
        "公司",
        "公告",
        "市场",
        "新闻",
        "更新",
    }
)
_EXACT_TRIAGE_MATCH_FIELDS = frozenset(
    {
        "event_cluster_ids",
        "industry_codes",
        "issuer_ids",
        "instrument_ids",
        "etf_ids",
        "subject_refs",
        "ts_code",
        "record.ts_code",
    }
)


def build_callback_agent_profile_ref(
    *,
    callback_agent_type: str,
    model_profile_id: str,
    model_profile_hash: str,
    preloaded_skills: tuple[str, ...],
    skill_manifest_hashes: tuple[str, ...],
    max_turns: int,
    max_input_tokens: int,
    max_output_tokens: int,
    max_cost_microusd: int,
) -> str:
    """Identify the callback model/Skill/budget bundle without a second registry."""

    if not model_profile_id.startswith("model-provider-"):
        raise ValueError("callback Agent profile requires a model Provider profile")
    _sha256(model_profile_id.removeprefix("model-provider-"), "model profile ID")
    _sha256(model_profile_hash, "model profile hash")
    if len(preloaded_skills) != len(skill_manifest_hashes):
        raise ValueError("callback Agent Skill names and hashes do not reconcile")
    for value in skill_manifest_hashes:
        _sha256(value, "callback Agent Skill manifest hash")
    for value, name in (
        (max_turns, "max_turns"),
        (max_input_tokens, "max_input_tokens"),
        (max_output_tokens, "max_output_tokens"),
        (max_cost_microusd, "max_cost_microusd"),
    ):
        if value < 1:
            raise ValueError(f"callback Agent {name} must be positive")
    core = {
        "callback_agent_type": callback_agent_type,
        "model_profile_id": model_profile_id,
        "model_profile_hash": model_profile_hash,
        "preloaded_skills": list(preloaded_skills),
        "skill_manifest_hashes": list(skill_manifest_hashes),
        "max_turns": max_turns,
        "max_input_tokens": max_input_tokens,
        "max_output_tokens": max_output_tokens,
        "max_cost_microusd": max_cost_microusd,
        "execution_capability": False,
    }
    return f"agent-profile-{canonical_hash(core)}"


class WatchAdmissionOutcome(StrEnum):
    ADMITTED = "admitted"
    REUSED = "reused"
    REJECTED = "rejected"


class WatchAdmissionBlocker(StrEnum):
    PROFILE_NOT_OFFERED = "profile_not_offered"
    SUBJECT_KIND_NOT_ALLOWED = "subject_kind_not_allowed"
    EFFECTIVE_CONTEXT_REQUIRED = "effective_context_required"
    EFFECTIVE_CONTEXT_NOT_ALLOWED = "effective_context_not_allowed"
    MATCHER_NOT_REGISTERED = "matcher_not_registered"
    EVIDENCE_OUTSIDE_PARENT_VIEW = "evidence_outside_parent_view"
    SUBJECT_OUTSIDE_PARENT_VIEW = "subject_outside_parent_view"
    MATCHER_OUTSIDE_PARENT_VIEW = "matcher_outside_parent_view"
    BRANCH_LIMIT_EXHAUSTED = "branch_limit_exhausted"
    ACTIVE_WATCH_LIMIT_EXHAUSTED = "active_watch_limit_exhausted"
    WATCH_BUDGET_EXHAUSTED = "watch_budget_exhausted"


@dataclass(frozen=True, slots=True)
class AgentDelegationContext:
    """Candidate parent projection; it is not authority until a parent owner reopens it."""

    parent_ref: str
    parent_agent_type: str
    lineage_depth: int
    created_at: datetime
    authorized_evidence_refs: tuple[str, ...] = ()
    authorized_subjects: tuple[MonitoringSubjectRef, ...] = ()
    authorized_matcher_terms: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        _canonical_ref(self.parent_ref, "delegation parent_ref")
        _canonical_ref(self.parent_agent_type, "delegation parent_agent_type")
        if self.lineage_depth < 0:
            raise ValueError("delegation lineage_depth must be non-negative")
        _strict_utc(self.created_at, "delegation context created_at")
        _canonical_tuple(
            self.authorized_evidence_refs,
            "authorized_evidence_refs",
            allow_empty=True,
        )
        if self.authorized_subjects != tuple(
            sorted(set(self.authorized_subjects), key=_subject_key)
        ):
            raise ValueError("authorized_subjects must be unique and canonical")
        if self.authorized_matcher_terms != tuple(sorted(set(self.authorized_matcher_terms))):
            raise ValueError("authorized_matcher_terms must be unique and canonical")
        for term in self.authorized_matcher_terms:
            if not term or term != term.strip().casefold():
                raise ValueError("authorized_matcher_terms must be normalized")

    @property
    def authority_hash(self) -> str:
        return canonical_hash(self.to_dict())

    def to_dict(self) -> dict[str, object]:
        return {
            "parent_ref": self.parent_ref,
            "parent_agent_type": self.parent_agent_type,
            "lineage_depth": self.lineage_depth,
            "created_at": _timestamp(self.created_at),
            "authorized_evidence_refs": list(self.authorized_evidence_refs),
            "authorized_subjects": [item.to_dict() for item in self.authorized_subjects],
            "authorized_matcher_terms": list(self.authorized_matcher_terms),
        }


class AgentDelegationContextStore:
    """Fail-closed seam pending one concrete parent Run/Decision projection authority.

    No public minting operation exists.  A content-addressed context artifact alone is
    self-declaration and therefore cannot become delegation authority.
    """

    def __init__(self, store: LocalDataSnapshotStore) -> None:
        self.store = store

    def reopen(self, context: AgentDelegationContext) -> AgentDelegationContext:
        """Reject until a named concrete parent authority can derive this projection."""

        _ = context
        raise ValueError(
            "Agent Watch parent authority integration is not configured; "
            "delegation contexts cannot be minted from caller data"
        )


class EventImpactTriageWatchAuthority:
    """One exact durable Triage cluster authorized to propose an Attention Watch."""

    def __init__(
        self,
        store: LocalDataSnapshotStore,
        *,
        decision_store: EventImpactTriageDecisionStore,
        candidate_set_id: str,
        cluster_id: str,
        event_assessment_authority: EventAssessmentRunAuthority | None = None,
    ) -> None:
        if type(store) is not LocalDataSnapshotStore:
            raise TypeError("Triage Watch authority requires the concrete Data Snapshot store")
        if type(decision_store) is not EventImpactTriageDecisionStore:
            raise TypeError("Triage Watch authority requires the concrete Triage Decision store")
        if decision_store.root != store.root:
            raise ValueError("Triage Watch authority stores must share one exact state root")
        if not candidate_set_id.startswith("event-impact-triage-candidate-set-"):
            raise ValueError("Triage Watch authority requires a Candidate Set ID")
        if not cluster_id.startswith("event-impact-triage-cluster-"):
            raise ValueError("Triage Watch authority requires a cluster ID")
        if event_assessment_authority is not None:
            from market_impact_agent.prospective_event_assessment import (
                EventAssessmentRunAuthority,
            )

            if type(event_assessment_authority) is not EventAssessmentRunAuthority:
                raise TypeError(
                    "Triage Watch authority requires the concrete EventAssessment authority"
                )
        self.store = store
        self.decision_store = decision_store
        self.candidate_set_id = candidate_set_id
        self.cluster_id = cluster_id
        self.event_assessment_authority = event_assessment_authority

    def delegation_context(self) -> AgentDelegationContext:
        """Derive the projection anew from the append-only Triage Decision owner."""

        candidate_set, proposal, decision = self.decision_store.get_context(self.candidate_set_id)
        matches = tuple(item for item in proposal.clusters if item.cluster_id == self.cluster_id)
        if len(matches) != 1:
            raise ValueError("Triage Watch cluster is not in the authoritative Proposal")
        cluster = matches[0]
        direct_watch = (
            cluster.cluster_id in decision.attention_watch_cluster_ids
            and cluster.recommended_route is TriageRoute.ATTENTION_WATCH
        )
        if direct_watch:
            parent_agent_type = _TRIAGE_COORDINATOR_AGENT_TYPE
            created_at = decision.decided_at
        elif (
            cluster.cluster_id in decision.event_assessment_cluster_ids
            and cluster.recommended_route is TriageRoute.EVENT_ASSESSMENT
            and self.event_assessment_authority is not None
        ):
            parent_agent_type = _EVENT_ASSESSMENT_COORDINATOR_AGENT_TYPE
            created_at = self.event_assessment_authority.reopen_completed_watch(
                candidate_set=candidate_set,
                proposal=proposal,
                decision=decision,
                cluster=cluster,
            )
        else:
            raise ValueError("Triage cluster is not authorized for Attention Watch")
        evidence_refs = tuple(
            sorted({*cluster.candidate_version_ids, *cluster.evidence_version_ids})
        )
        return AgentDelegationContext(
            parent_ref=cluster.cluster_id,
            parent_agent_type=parent_agent_type,
            lineage_depth=0,
            created_at=created_at,
            authorized_evidence_refs=evidence_refs,
            authorized_subjects=(
                MonitoringSubjectRef(
                    MonitoringSubjectKind.EVENT_CLUSTER,
                    cluster.cluster_id,
                ),
            ),
            authorized_matcher_terms=_triage_matcher_terms(
                store=self.store,
                candidate_set=candidate_set,
                cluster=cluster,
            ),
        )

    def reopen(self, context: AgentDelegationContext) -> AgentDelegationContext:
        """Compare a caller projection with freshly reopened durable authority."""

        authoritative = self.delegation_context()
        if context != authoritative:
            raise ValueError("Agent Watch parent projection differs from Triage authority")
        return authoritative


class EventImpactTriageWatchAuthorityResolver:
    """Resolve any durable Triage-owned Watch parent in one shared state root."""

    def __init__(
        self,
        store: LocalDataSnapshotStore,
        *,
        decision_store: EventImpactTriageDecisionStore,
        event_assessment_authority: EventAssessmentRunAuthority | None = None,
    ) -> None:
        if type(store) is not LocalDataSnapshotStore:
            raise TypeError("Triage Watch resolver requires the concrete Data Snapshot store")
        if type(decision_store) is not EventImpactTriageDecisionStore:
            raise TypeError("Triage Watch resolver requires the concrete Triage Decision store")
        if decision_store.root != store.root:
            raise ValueError("Triage Watch resolver stores must share one exact state root")
        if event_assessment_authority is not None:
            from market_impact_agent.prospective_event_assessment import (
                EventAssessmentRunAuthority,
            )

            if type(event_assessment_authority) is not EventAssessmentRunAuthority:
                raise TypeError(
                    "Triage Watch resolver requires the concrete EventAssessment authority"
                )
        self.store = store
        self.decision_store = decision_store
        self.event_assessment_authority = event_assessment_authority

    def authority(self, parent_ref: str) -> EventImpactTriageWatchAuthority:
        if self.event_assessment_authority is None:
            candidate, _, _, cluster = self.decision_store.get_watch_context_by_cluster(parent_ref)
        else:
            candidate, _, _, cluster = self.decision_store.get_cluster_context(parent_ref)
        return EventImpactTriageWatchAuthority(
            self.store,
            decision_store=self.decision_store,
            candidate_set_id=candidate.candidate_set_id,
            cluster_id=cluster.cluster_id,
            event_assessment_authority=self.event_assessment_authority,
        )

    def reopen(self, context: AgentDelegationContext) -> AgentDelegationContext:
        return self.authority(context.parent_ref).reopen(context)


@dataclass(frozen=True, slots=True)
class WatchDelegateProfile:
    """Registered profile shown to Agents by name and description."""

    profile_id: str
    name: str
    description: str
    callback_agent_type: str
    callback_agent_profile_ref: str
    allowed_parent_agent_types: tuple[str, ...]
    allowed_subject_kinds: tuple[MonitoringSubjectKind, ...]
    preloaded_skills: tuple[str, ...]
    skill_manifest_hashes: tuple[str, ...]
    required_capabilities: tuple[str, ...]
    query_template: RegisteredQueryTemplate
    collection_policy_id: str
    use_class: MonitoringUseClass
    freshness_max_age_seconds: int
    minimum_coverage_sources: int
    maximum_polls: int
    maximum_bytes: int
    maximum_wakes: int
    cooldown_seconds: int
    active_duration_seconds: int
    maximum_lineage_depth: int
    maximum_children_per_parent: int
    maximum_active_watches: int
    callback_max_turns: int
    callback_max_input_tokens: int
    callback_max_output_tokens: int
    callback_max_cost_microusd: int
    execution_capability: bool = False

    def __post_init__(self) -> None:
        if not _PROFILE_REF.fullmatch(self.profile_id):
            raise ValueError("Watch delegate profile_id is invalid")
        _bounded_text(self.name, "Watch delegate profile name", maximum=80)
        _bounded_text(self.description, "Watch delegate profile description", maximum=1000)
        _canonical_ref(self.callback_agent_type, "callback_agent_type")
        if not re.fullmatch(r"agent-profile-[0-9a-f]{64}", self.callback_agent_profile_ref):
            raise ValueError("Watch delegate callback profile must be registered")
        _canonical_tuple(self.allowed_parent_agent_types, "allowed_parent_agent_types")
        if not self.allowed_subject_kinds or self.allowed_subject_kinds != tuple(
            sorted(set(self.allowed_subject_kinds), key=lambda item: item.value)
        ):
            raise ValueError("allowed_subject_kinds must be non-empty, unique, and sorted")
        _canonical_tuple(self.preloaded_skills, "preloaded_skills", allow_empty=True)
        if len(self.preloaded_skills) != len(self.skill_manifest_hashes):
            raise ValueError("Watch delegate Skill names and hashes do not reconcile")
        for value in self.skill_manifest_hashes:
            _sha256(value, "Watch delegate Skill manifest hash")
        _canonical_tuple(self.required_capabilities, "required_capabilities")
        if not self.collection_policy_id.startswith("prospective-collection-policy-"):
            raise ValueError("Watch delegate profile requires a Collection Policy ID")
        for value, name in (
            (self.freshness_max_age_seconds, "freshness_max_age_seconds"),
            (self.maximum_bytes, "maximum_bytes"),
            (self.cooldown_seconds, "cooldown_seconds"),
            (self.callback_max_cost_microusd, "callback_max_cost_microusd"),
        ):
            if value < 0:
                raise ValueError(f"Watch delegate profile {name} must be non-negative")
        for value, name in (
            (self.minimum_coverage_sources, "minimum_coverage_sources"),
            (self.maximum_polls, "maximum_polls"),
            (self.maximum_wakes, "maximum_wakes"),
            (self.active_duration_seconds, "active_duration_seconds"),
            (self.maximum_lineage_depth, "maximum_lineage_depth"),
            (self.maximum_children_per_parent, "maximum_children_per_parent"),
            (self.maximum_active_watches, "maximum_active_watches"),
            (self.callback_max_turns, "callback_max_turns"),
            (self.callback_max_input_tokens, "callback_max_input_tokens"),
            (self.callback_max_output_tokens, "callback_max_output_tokens"),
        ):
            if value < 1:
                raise ValueError(f"Watch delegate profile {name} must be positive")
        if self.execution_capability:
            raise ValueError("Watch delegate profiles cannot grant execution capability")
        if self.profile_id != self.expected_profile_id:
            raise ValueError("Watch delegate profile_id does not match content")

    @property
    def expected_profile_id(self) -> str:
        return f"watch-delegate-profile-{canonical_hash(self.core_dict())}"

    def core_dict(self) -> dict[str, object]:
        return {
            "name": self.name,
            "description": self.description,
            "callback_agent_type": self.callback_agent_type,
            "callback_agent_profile_ref": self.callback_agent_profile_ref,
            "allowed_parent_agent_types": list(self.allowed_parent_agent_types),
            "allowed_subject_kinds": [item.value for item in self.allowed_subject_kinds],
            "preloaded_skills": list(self.preloaded_skills),
            "skill_manifest_hashes": list(self.skill_manifest_hashes),
            "required_capabilities": list(self.required_capabilities),
            "query_template": _query_template_dict(self.query_template),
            "collection_policy_id": self.collection_policy_id,
            "use_class": self.use_class.value,
            "freshness_max_age_seconds": self.freshness_max_age_seconds,
            "minimum_coverage_sources": self.minimum_coverage_sources,
            "maximum_polls": self.maximum_polls,
            "maximum_bytes": self.maximum_bytes,
            "maximum_wakes": self.maximum_wakes,
            "cooldown_seconds": self.cooldown_seconds,
            "active_duration_seconds": self.active_duration_seconds,
            "maximum_lineage_depth": self.maximum_lineage_depth,
            "maximum_children_per_parent": self.maximum_children_per_parent,
            "maximum_active_watches": self.maximum_active_watches,
            "callback_max_turns": self.callback_max_turns,
            "callback_max_input_tokens": self.callback_max_input_tokens,
            "callback_max_output_tokens": self.callback_max_output_tokens,
            "callback_max_cost_microusd": self.callback_max_cost_microusd,
            "execution_capability": self.execution_capability,
        }

    def to_dict(self) -> dict[str, object]:
        return {**self.core_dict(), "profile_id": self.profile_id}

    @classmethod
    def build(
        cls,
        *,
        name: str,
        description: str,
        callback_agent_type: str,
        callback_agent_profile_ref: str,
        allowed_parent_agent_types: tuple[str, ...],
        allowed_subject_kinds: tuple[MonitoringSubjectKind, ...],
        preloaded_skills: tuple[str, ...],
        skill_manifest_hashes: tuple[str, ...],
        required_capabilities: tuple[str, ...],
        query_template: RegisteredQueryTemplate,
        collection_policy_id: str,
        use_class: MonitoringUseClass,
        freshness_max_age_seconds: int,
        minimum_coverage_sources: int,
        maximum_polls: int,
        maximum_bytes: int,
        maximum_wakes: int,
        cooldown_seconds: int,
        active_duration_seconds: int,
        maximum_lineage_depth: int,
        maximum_children_per_parent: int,
        maximum_active_watches: int,
        callback_max_turns: int,
        callback_max_input_tokens: int,
        callback_max_output_tokens: int,
        callback_max_cost_microusd: int,
    ) -> WatchDelegateProfile:
        parents = tuple(sorted(set(allowed_parent_agent_types)))
        subjects = tuple(sorted(set(allowed_subject_kinds), key=lambda item: item.value))
        if len(set(preloaded_skills)) != len(preloaded_skills):
            raise ValueError("Watch delegate preloaded Skills must be unique")
        if len(preloaded_skills) != len(skill_manifest_hashes):
            raise ValueError("Watch delegate Skill names and hashes do not reconcile")
        skill_hashes = dict(zip(preloaded_skills, skill_manifest_hashes, strict=True))
        skills = tuple(sorted(preloaded_skills))
        hashes = tuple(skill_hashes[name] for name in skills)
        capabilities = tuple(sorted(set(required_capabilities)))
        values: dict[str, object] = {
            "name": name,
            "description": description,
            "callback_agent_type": callback_agent_type,
            "callback_agent_profile_ref": callback_agent_profile_ref,
            "allowed_parent_agent_types": list(parents),
            "allowed_subject_kinds": [item.value for item in subjects],
            "preloaded_skills": list(skills),
            "skill_manifest_hashes": list(hashes),
            "required_capabilities": list(capabilities),
            "query_template": _query_template_dict(query_template),
            "collection_policy_id": collection_policy_id,
            "use_class": use_class.value,
            "freshness_max_age_seconds": freshness_max_age_seconds,
            "minimum_coverage_sources": minimum_coverage_sources,
            "maximum_polls": maximum_polls,
            "maximum_bytes": maximum_bytes,
            "maximum_wakes": maximum_wakes,
            "cooldown_seconds": cooldown_seconds,
            "active_duration_seconds": active_duration_seconds,
            "maximum_lineage_depth": maximum_lineage_depth,
            "maximum_children_per_parent": maximum_children_per_parent,
            "maximum_active_watches": maximum_active_watches,
            "callback_max_turns": callback_max_turns,
            "callback_max_input_tokens": callback_max_input_tokens,
            "callback_max_output_tokens": callback_max_output_tokens,
            "callback_max_cost_microusd": callback_max_cost_microusd,
            "execution_capability": False,
        }
        return cls(
            profile_id=f"watch-delegate-profile-{canonical_hash(values)}",
            name=name,
            description=description,
            callback_agent_type=callback_agent_type,
            callback_agent_profile_ref=callback_agent_profile_ref,
            allowed_parent_agent_types=parents,
            allowed_subject_kinds=subjects,
            preloaded_skills=skills,
            skill_manifest_hashes=hashes,
            required_capabilities=capabilities,
            query_template=query_template,
            collection_policy_id=collection_policy_id,
            use_class=use_class,
            freshness_max_age_seconds=freshness_max_age_seconds,
            minimum_coverage_sources=minimum_coverage_sources,
            maximum_polls=maximum_polls,
            maximum_bytes=maximum_bytes,
            maximum_wakes=maximum_wakes,
            cooldown_seconds=cooldown_seconds,
            active_duration_seconds=active_duration_seconds,
            maximum_lineage_depth=maximum_lineage_depth,
            maximum_children_per_parent=maximum_children_per_parent,
            maximum_active_watches=maximum_active_watches,
            callback_max_turns=callback_max_turns,
            callback_max_input_tokens=callback_max_input_tokens,
            callback_max_output_tokens=callback_max_output_tokens,
            callback_max_cost_microusd=callback_max_cost_microusd,
        )


@dataclass(frozen=True, slots=True)
class AgentWatchRequest:
    """Model proposal; route, budget, Provider and execution fields are absent."""

    request_id: str
    delegate_profile_id: str
    rationale: str
    watch_question: str
    evidence_refs: tuple[str, ...]
    subject: MonitoringSubjectRef
    frozen_members: tuple[MonitoringSubjectRef, ...]
    information_aspect_ref: str | None
    matcher: ObservationMatcher
    schema_version: str = AGENT_WATCH_REQUEST_SCHEMA

    def __post_init__(self) -> None:
        if self.schema_version != AGENT_WATCH_REQUEST_SCHEMA:
            raise ValueError("unsupported Agent Watch request schema")
        if not _PROFILE_REF.fullmatch(self.delegate_profile_id):
            raise ValueError("Agent Watch request delegate profile is invalid")
        _bounded_text(self.rationale, "Agent Watch rationale", maximum=2000)
        _bounded_text(self.watch_question, "Agent Watch question", maximum=1000)
        _canonical_tuple(self.evidence_refs, "Agent Watch evidence_refs")
        if self.subject.kind is MonitoringSubjectKind.FROZEN_SET:
            if not self.frozen_members or self.frozen_members != tuple(
                sorted(set(self.frozen_members), key=_subject_key)
            ):
                raise ValueError("Agent Watch frozen members must be non-empty and canonical")
        elif self.frozen_members:
            raise ValueError("only frozen_set Agent Watch requests may carry frozen members")
        if self.information_aspect_ref is not None and not _REGISTERED_ASPECT_REF.fullmatch(
            self.information_aspect_ref
        ):
            raise ValueError("Agent Watch information aspect must be registered")
        if (
            self.subject.kind is MonitoringSubjectKind.INFORMATION_ASPECT
            and self.information_aspect_ref is None
        ):
            raise ValueError("information_aspect Agent Watch request requires an aspect ref")
        if self.request_id != self.expected_request_id:
            raise ValueError("Agent Watch request_id does not match content")

    @property
    def expected_request_id(self) -> str:
        return f"agent-watch-request-{canonical_hash(self.core_dict())}"

    def core_dict(self) -> dict[str, object]:
        values: dict[str, object] = {
            "schema_version": self.schema_version,
            "delegate_profile_id": self.delegate_profile_id,
            "rationale": self.rationale,
            "watch_question": self.watch_question,
            "evidence_refs": list(self.evidence_refs),
            "subject": self.subject.to_dict(),
            "matcher": self.matcher.to_dict(),
        }
        if self.frozen_members:
            values["frozen_members"] = [item.to_dict() for item in self.frozen_members]
        if self.information_aspect_ref is not None:
            values["information_aspect_ref"] = self.information_aspect_ref
        return values

    def to_dict(self) -> dict[str, object]:
        return {**self.core_dict(), "request_id": self.request_id}

    @classmethod
    def build(
        cls,
        *,
        delegate_profile_id: str,
        rationale: str,
        watch_question: str,
        evidence_refs: tuple[str, ...],
        subject: MonitoringSubjectRef,
        matcher: ObservationMatcher,
        frozen_members: tuple[MonitoringSubjectRef, ...] = (),
        information_aspect_ref: str | None = None,
    ) -> AgentWatchRequest:
        evidence = tuple(sorted(set(evidence_refs)))
        members = tuple(sorted(set(frozen_members), key=_subject_key))
        values: dict[str, object] = {
            "schema_version": AGENT_WATCH_REQUEST_SCHEMA,
            "delegate_profile_id": delegate_profile_id,
            "rationale": rationale,
            "watch_question": watch_question,
            "evidence_refs": list(evidence),
            "subject": subject.to_dict(),
            "matcher": matcher.to_dict(),
        }
        if members:
            values["frozen_members"] = [item.to_dict() for item in members]
        if information_aspect_ref is not None:
            values["information_aspect_ref"] = information_aspect_ref
        return cls(
            request_id=f"agent-watch-request-{canonical_hash(values)}",
            delegate_profile_id=delegate_profile_id,
            rationale=rationale,
            watch_question=watch_question,
            evidence_refs=evidence,
            subject=subject,
            frozen_members=members,
            information_aspect_ref=information_aspect_ref,
            matcher=matcher,
        )


@dataclass(frozen=True, slots=True)
class AgentWatchAdmission:
    admission_id: str
    request_id: str
    parent_ref: str
    parent_agent_type: str
    parent_authority_hash: str
    lineage_depth: int
    delegate_profile_id: str
    outcome: WatchAdmissionOutcome
    blocker: WatchAdmissionBlocker | None
    operational_scope_key: str | None
    monitoring_scope_id: str | None
    retrieval_plan_id: str | None
    watch_id: str | None
    callback_agent_type: str | None
    admitted_at: datetime
    execution_capability: bool = False
    schema_version: str = AGENT_WATCH_ADMISSION_SCHEMA

    def __post_init__(self) -> None:
        if self.schema_version != AGENT_WATCH_ADMISSION_SCHEMA:
            raise ValueError("unsupported Agent Watch admission schema")
        if not self.request_id.startswith("agent-watch-request-"):
            raise ValueError("Agent Watch admission requires a request")
        _canonical_ref(self.parent_ref, "Agent Watch parent_ref")
        _canonical_ref(self.parent_agent_type, "Agent Watch parent_agent_type")
        _sha256(self.parent_authority_hash, "Agent Watch parent authority hash")
        if self.lineage_depth < 1:
            raise ValueError("Agent Watch admission lineage_depth must be positive")
        if not _PROFILE_REF.fullmatch(self.delegate_profile_id):
            raise ValueError("Agent Watch admission profile is invalid")
        bound = (
            self.operational_scope_key,
            self.monitoring_scope_id,
            self.retrieval_plan_id,
            self.watch_id,
            self.callback_agent_type,
        )
        if self.outcome is WatchAdmissionOutcome.REJECTED:
            if self.blocker is None or any(item is not None for item in bound):
                raise ValueError("rejected Agent Watch admission requires only a blocker")
        else:
            if self.blocker is not None or any(item is None for item in bound):
                raise ValueError("accepted Agent Watch admission requires complete bindings")
            _sha256(cast(str, self.operational_scope_key), "operational_scope_key")
            if not cast(str, self.monitoring_scope_id).startswith("monitoring-scope-"):
                raise ValueError("Agent Watch admission requires a Monitoring Scope")
            if not cast(str, self.retrieval_plan_id).startswith("retrieval-plan-"):
                raise ValueError("Agent Watch admission requires a Retrieval Plan")
            if not cast(str, self.watch_id).startswith("attention-watch-"):
                raise ValueError("Agent Watch admission requires an Attention Watch")
            _canonical_ref(cast(str, self.callback_agent_type), "callback_agent_type")
        _strict_utc(self.admitted_at, "Agent Watch admitted_at")
        if self.execution_capability:
            raise ValueError("Agent Watch admission cannot grant execution capability")
        if self.admission_id != self.expected_admission_id:
            raise ValueError("Agent Watch admission_id does not match content")

    @property
    def expected_admission_id(self) -> str:
        return f"agent-watch-admission-{canonical_hash(self.core_dict())}"

    def core_dict(self) -> dict[str, object]:
        values: dict[str, object] = {
            "schema_version": self.schema_version,
            "request_id": self.request_id,
            "parent_ref": self.parent_ref,
            "parent_agent_type": self.parent_agent_type,
            "parent_authority_hash": self.parent_authority_hash,
            "lineage_depth": self.lineage_depth,
            "delegate_profile_id": self.delegate_profile_id,
            "outcome": self.outcome.value,
            "admitted_at": _timestamp(self.admitted_at),
            "execution_capability": self.execution_capability,
        }
        if self.blocker is not None:
            values["blocker"] = self.blocker.value
        else:
            values.update(
                {
                    "operational_scope_key": self.operational_scope_key,
                    "monitoring_scope_id": self.monitoring_scope_id,
                    "retrieval_plan_id": self.retrieval_plan_id,
                    "watch_id": self.watch_id,
                    "callback_agent_type": self.callback_agent_type,
                }
            )
        return values

    def to_dict(self) -> dict[str, object]:
        return {**self.core_dict(), "admission_id": self.admission_id}


@dataclass(frozen=True, slots=True)
class WatchCallbackBinding:
    """Read-only resolution for the later PDI-41 fresh-run dispatcher."""

    admission: AgentWatchAdmission
    request: AgentWatchRequest
    wake: AttentionWake
    profile: WatchDelegateProfile
    authority_watch_id: str | None = None
    rebaseline_grant_ids: tuple[str, ...] = ()
    execution_capability: bool = False

    def __post_init__(self) -> None:
        if self.admission.outcome is WatchAdmissionOutcome.REJECTED:
            raise ValueError("Watch callback requires an accepted admission")
        if self.admission.request_id != self.request.request_id:
            raise ValueError("Watch callback request does not match admission")
        authority_watch_id = (
            self.wake.watch_id if self.authority_watch_id is None else self.authority_watch_id
        )
        if self.admission.watch_id != authority_watch_id:
            raise ValueError("Watch callback Wake does not match admission authority")
        if authority_watch_id == self.wake.watch_id:
            if self.rebaseline_grant_ids:
                raise ValueError("direct Watch callback cannot carry rebaseline grants")
        elif not self.rebaseline_grant_ids:
            raise ValueError("successor Watch callback requires rebaseline grant lineage")
        if any(
            not item.startswith("attention-watch-rebaseline-grant-")
            for item in self.rebaseline_grant_ids
        ):
            raise ValueError("Watch callback rebaseline grant lineage is invalid")
        if self.admission.delegate_profile_id != self.profile.profile_id:
            raise ValueError("Watch callback profile does not match admission")
        if self.execution_capability:
            raise ValueError("Watch callback binding cannot grant execution capability")


class AgentWatchAdmissionService:
    """Durable Harness authority for offers, Watch admission and callback lookup."""

    def __init__(
        self,
        store: LocalDataSnapshotStore,
        *,
        profiles: tuple[WatchDelegateProfile, ...],
        delegation_authority: (
            AgentDelegationContextStore
            | EventImpactTriageWatchAuthority
            | EventImpactTriageWatchAuthorityResolver
            | ResearchThesisWatchAuthorityResolver
        ),
        journal: ProspectiveDataJournal | None = None,
        watch_service: AttentionWatchService | None = None,
    ) -> None:
        from market_impact_agent.research_thesis_watch import ResearchThesisWatchAuthorityResolver

        if len({item.profile_id for item in profiles}) != len(profiles):
            raise ValueError("Agent Watch admission requires unique delegate profiles")
        self.store = store
        self.journal = ProspectiveDataJournal(store) if journal is None else journal
        self.watch_service = (
            AttentionWatchService(store, journal=self.journal)
            if watch_service is None
            else watch_service
        )
        if type(delegation_authority) not in {
            AgentDelegationContextStore,
            ResearchThesisWatchAuthorityResolver,
            EventImpactTriageWatchAuthority,
            EventImpactTriageWatchAuthorityResolver,
        }:
            raise TypeError("Agent Watch admission requires concrete delegation store authority")
        self.delegation_authority = delegation_authority
        if self.journal.store.root != store.root or self.watch_service.store.root != store.root:
            raise ValueError("Agent Watch authorities must share one state root")
        if self.delegation_authority.store.root != store.root:
            raise ValueError("Agent Watch delegation authority must share the state root")
        self.index_path = store.index_path
        self._initialize()
        self._offered_profile_ids = frozenset(item.profile_id for item in profiles)
        for profile in profiles:
            self._record_profile(profile)
        registered_profiles = self._stored_profiles()
        if not registered_profiles:
            raise ValueError("Agent Watch admission requires a delegate profile")
        self.profiles = {item.profile_id: item for item in registered_profiles}
        for profile in registered_profiles:
            collection_policy = self.journal.policy(profile.collection_policy_id)
            if collection_policy.capability is not profile.query_template.capability:
                raise ValueError("Watch delegate profile capability does not match its route")
            if profile.minimum_coverage_sources > len(collection_policy.sources):
                raise ValueError("Watch delegate profile coverage exceeds its route")
        if self._has_parent_authority():
            self._reconcile_pending_watch_activations()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.index_path)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA journal_mode = WAL")
        connection.execute("PRAGMA synchronous = FULL")
        connection.execute("PRAGMA busy_timeout = 5000")
        return connection

    def _initialize(self) -> None:
        with self._connect() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS agent_watch_requests (
                    request_id TEXT PRIMARY KEY,
                    artifact_hash TEXT NOT NULL UNIQUE,
                    received_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS agent_watch_admissions (
                    admission_id TEXT PRIMARY KEY,
                    artifact_hash TEXT NOT NULL UNIQUE,
                    request_id TEXT NOT NULL REFERENCES agent_watch_requests(request_id),
                    parent_ref TEXT NOT NULL,
                    parent_agent_type TEXT NOT NULL,
                    lineage_depth INTEGER NOT NULL,
                    delegate_profile_id TEXT NOT NULL,
                    outcome TEXT NOT NULL,
                    blocker TEXT,
                    operational_scope_key TEXT,
                    watch_id TEXT,
                    watch_policy_hash TEXT,
                    expires_at TEXT,
                    admitted_at TEXT NOT NULL,
                    UNIQUE(request_id, parent_ref, parent_agent_type, lineage_depth)
                );
                CREATE INDEX IF NOT EXISTS agent_watch_admission_scope
                    ON agent_watch_admissions(
                        operational_scope_key, delegate_profile_id, outcome, expires_at
                    );
                CREATE TABLE IF NOT EXISTS agent_watch_wake_callback_sets (
                    wake_id TEXT PRIMARY KEY,
                    artifact_hash TEXT NOT NULL UNIQUE
                );
                CREATE TABLE IF NOT EXISTS agent_watch_delegate_profiles (
                    profile_id TEXT PRIMARY KEY,
                    artifact_hash TEXT NOT NULL UNIQUE,
                    registered_at TEXT NOT NULL
                );
                CREATE TRIGGER IF NOT EXISTS agent_watch_delegate_profiles_no_update
                    BEFORE UPDATE ON agent_watch_delegate_profiles
                    BEGIN SELECT RAISE(
                        ABORT, 'Agent Watch delegate profiles are append-only'
                    ); END;
                CREATE TRIGGER IF NOT EXISTS agent_watch_delegate_profiles_no_delete
                    BEFORE DELETE ON agent_watch_delegate_profiles
                    BEGIN SELECT RAISE(
                        ABORT, 'Agent Watch delegate profiles are append-only'
                    ); END;
                """
            )

    def _record_profile(self, profile: WatchDelegateProfile) -> None:
        artifact = self.store.artifacts.put_json(profile.to_dict())
        with self._connect() as connection:
            existing = connection.execute(
                """
                SELECT artifact_hash FROM agent_watch_delegate_profiles
                WHERE profile_id = ?
                """,
                (profile.profile_id,),
            ).fetchone()
            if existing is not None:
                if cast(str, existing["artifact_hash"]) != artifact.content_hash:
                    raise ValueError("Watch delegate profile identity has conflicting content")
                return
            connection.execute(
                """
                INSERT INTO agent_watch_delegate_profiles(profile_id, artifact_hash, registered_at)
                VALUES (?, ?, ?)
                """,
                (profile.profile_id, artifact.content_hash, _timestamp(datetime.now(UTC))),
            )

    def _stored_profiles(self) -> tuple[WatchDelegateProfile, ...]:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT artifact_hash FROM agent_watch_delegate_profiles
                ORDER BY registered_at, profile_id
                """
            ).fetchall()
        return tuple(
            watch_delegate_profile_from_dict(
                self.store.artifacts.read_json(cast(str, row["artifact_hash"]))
            )
            for row in rows
        )

    def offered_profiles(
        self,
        context: AgentDelegationContext,
    ) -> tuple[WatchDelegateProfile, ...]:
        context = self.delegation_authority.reopen(context)
        return tuple(
            sorted(
                (
                    profile
                    for profile in self.profiles.values()
                    if profile.profile_id in self._offered_profile_ids
                    and self._profile_authorized_by_parent(context, profile.profile_id)
                    and context.parent_agent_type in profile.allowed_parent_agent_types
                    and context.lineage_depth < profile.maximum_lineage_depth
                    and (
                        not self._has_triage_authority()
                        or MonitoringSubjectKind.EVENT_CLUSTER in profile.allowed_subject_kinds
                    )
                ),
                key=lambda item: item.profile_id,
            )
        )

    def admission(self, admission_id: str) -> AgentWatchAdmission:
        self._require_parent_authority_integration("Admissions")
        with self._connect() as connection:
            row = connection.execute(
                "SELECT artifact_hash FROM agent_watch_admissions WHERE admission_id = ?",
                (admission_id,),
            ).fetchone()
        if row is None:
            raise KeyError(f"unknown Agent Watch admission: {admission_id}")
        admission = agent_watch_admission_from_dict(
            self.store.artifacts.read_json(cast(str, row["artifact_hash"]))
        )
        self._reopen_parent_admission(admission)
        return admission

    def admit(
        self,
        request: AgentWatchRequest,
        *,
        context: AgentDelegationContext,
        initial_data_snapshot_id: str,
        decided_at: datetime,
        effective_context: EffectiveMembershipContext | None = None,
    ) -> AgentWatchAdmission:
        _strict_utc(decided_at, "Agent Watch decision time")
        context = self.delegation_authority.reopen(context)
        if decided_at < context.created_at:
            raise ValueError("Agent Watch decision cannot predate its parent context")
        if not initial_data_snapshot_id.startswith("data-snapshot-"):
            raise ValueError("Agent Watch admission requires a Data Snapshot baseline")
        self._record_request(request, received_at=decided_at)
        existing = self._existing_admission(request.request_id, context=context)
        if existing is not None:
            self._ensure_watch_activated(existing)
            return existing
        profile = self.profiles.get(request.delegate_profile_id)
        if profile not in self.offered_profiles(context):
            return self._reject(
                request,
                context,
                decided_at,
                WatchAdmissionBlocker.PROFILE_NOT_OFFERED,
            )
        if request.subject.kind not in profile.allowed_subject_kinds or (
            self._has_triage_authority()
            and request.subject.kind is not MonitoringSubjectKind.EVENT_CLUSTER
        ):
            return self._reject(
                request,
                context,
                decided_at,
                WatchAdmissionBlocker.SUBJECT_KIND_NOT_ALLOWED,
            )
        requires_context = request.subject.kind in {
            MonitoringSubjectKind.INDUSTRY,
            MonitoringSubjectKind.ETF,
        }
        if requires_context and effective_context is None:
            return self._reject(
                request,
                context,
                decided_at,
                WatchAdmissionBlocker.EFFECTIVE_CONTEXT_REQUIRED,
            )
        if not requires_context and effective_context is not None:
            return self._reject(
                request,
                context,
                decided_at,
                WatchAdmissionBlocker.EFFECTIVE_CONTEXT_NOT_ALLOWED,
            )
        try:
            profile.query_template.assert_accepts_matcher(request.matcher)
        except ValueError:
            return self._reject(
                request,
                context,
                decided_at,
                WatchAdmissionBlocker.MATCHER_NOT_REGISTERED,
            )
        if not set(request.evidence_refs) <= set(context.authorized_evidence_refs):
            return self._reject(
                request,
                context,
                decided_at,
                WatchAdmissionBlocker.EVIDENCE_OUTSIDE_PARENT_VIEW,
            )
        requested_subjects = {request.subject, *request.frozen_members}
        authorized_subjects = set(context.authorized_subjects)
        if not requested_subjects <= authorized_subjects or (
            request.information_aspect_ref is not None
            and request.information_aspect_ref
            not in {item.canonical_id for item in authorized_subjects}
        ):
            return self._reject(
                request,
                context,
                decided_at,
                WatchAdmissionBlocker.SUBJECT_OUTSIDE_PARENT_VIEW,
            )
        matcher_terms = {term for clause in request.matcher.clauses for term in clause.terms}
        if not matcher_terms <= set(context.authorized_matcher_terms) or (
            self._has_parent_authority() and not _specific_triage_matcher(request.matcher)
        ):
            return self._reject(
                request,
                context,
                decided_at,
                WatchAdmissionBlocker.MATCHER_OUTSIDE_PARENT_VIEW,
            )
        scope = MonitoringScope.build(
            origin_refs=(context.parent_ref,),
            subject=request.subject,
            frozen_members=request.frozen_members,
            effective_context=effective_context,
            information_aspect_ref=request.information_aspect_ref,
            query_template_ref=profile.query_template.template_ref,
            capability=profile.query_template.capability,
            pit_lane=profile.query_template.pit_lane,
            freshness_max_age_seconds=profile.freshness_max_age_seconds,
            minimum_coverage_sources=profile.minimum_coverage_sources,
            maximum_fetches=profile.maximum_polls,
            maximum_bytes=profile.maximum_bytes,
            use_class=profile.use_class,
            matcher=request.matcher,
        )
        plan = RetrievalPlan.bind(
            scope=scope,
            template=profile.query_template,
            collection_policy=self.journal.policy(profile.collection_policy_id),
        )
        operational_key = _operational_scope_key(profile, scope)
        expires_at = decided_at + timedelta(seconds=profile.active_duration_seconds)
        from market_impact_agent.research_thesis_watch import ResearchThesisWatchAuthorityResolver

        if type(self.delegation_authority) is ResearchThesisWatchAuthorityResolver:
            parent = self.delegation_authority.parent(context.parent_ref)[1]
            if (
                decided_at > self.delegation_authority.clock()
                or decided_at >= parent.episode_deadline
            ):
                raise PermissionError(
                    "research Watch admission exceeds its Episode deadline or current clock"
                )
            expires_at = min(expires_at, parent.episode_deadline)
            operational_key = canonical_hash(
                {
                    "collection_scope": operational_key,
                    "research_scope": self.delegation_authority.operational_scope_identity(),
                }
            )
        watch = AttentionWatchPolicy.build(
            origin_ref=context.parent_ref,
            collection_policy_id=profile.collection_policy_id,
            initial_data_snapshot_id=initial_data_snapshot_id,
            starts_at=decided_at,
            expires_at=expires_at,
            maximum_polls=profile.maximum_polls,
            maximum_bytes=profile.maximum_bytes,
            maximum_wakes=profile.maximum_wakes,
            cooldown_seconds=profile.cooldown_seconds,
            monitoring_scope=scope,
            retrieval_plan=plan,
            query_template=profile.query_template,
        )
        admission = self._commit_acceptance(
            request=request,
            context=context,
            profile=profile,
            scope=scope,
            plan=plan,
            watch=watch,
            operational_key=operational_key,
            decided_at=decided_at,
        )
        self._ensure_watch_activated(admission)
        return admission

    def callback_bindings(self, wake: AttentionWake) -> tuple[WatchCallbackBinding, ...]:
        self._require_parent_authority_integration("callbacks")
        authority_watch_id, rebaseline_grant_ids = self.watch_service.callback_authority_lineage(
            wake.watch_id
        )
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            wake_row = connection.execute(
                "SELECT artifact_hash FROM attention_watch_outbox WHERE wake_id = ?",
                (wake.wake_id,),
            ).fetchone()
            if wake_row is None:
                raise ValueError("Watch callback requires a durable Wake")
            authoritative_wake = attention_wake_from_dict(
                self.store.artifacts.read_json(cast(str, wake_row["artifact_hash"]))
            )
            if authoritative_wake != wake:
                raise ValueError("Watch callback Wake does not match durable authority")
            available_rows = connection.execute(
                """
                SELECT admission_id, artifact_hash, request_id, delegate_profile_id
                FROM agent_watch_admissions
                WHERE watch_id = ? AND outcome IN (?, ?)
                ORDER BY admitted_at, admission_id
                """,
                (
                    authority_watch_id,
                    WatchAdmissionOutcome.ADMITTED.value,
                    WatchAdmissionOutcome.REUSED.value,
                ),
            ).fetchall()
            callback_set_row = connection.execute(
                "SELECT artifact_hash FROM agent_watch_wake_callback_sets WHERE wake_id = ?",
                (wake.wake_id,),
            ).fetchone()
            if callback_set_row is None:
                admission_ids = tuple(cast(str, row["admission_id"]) for row in available_rows)
                if not admission_ids:
                    raise ValueError("Attention Wake has no accepted Agent Watch admission")
                callback_set = {
                    "schema_version": "market-impact.agent-watch-wake-callback-set.v1",
                    "wake_id": wake.wake_id,
                    "watch_id": wake.watch_id,
                    "admission_ids": list(admission_ids),
                }
                artifact = self.store.artifacts.put_json(callback_set)
                connection.execute(
                    """
                    INSERT INTO agent_watch_wake_callback_sets(wake_id, artifact_hash)
                    VALUES (?, ?)
                    """,
                    (wake.wake_id, artifact.content_hash),
                )
            else:
                callback_set = self.store.artifacts.read_json(
                    cast(str, callback_set_row["artifact_hash"])
                )
                admission_ids = _wake_callback_admission_ids(callback_set, wake=wake)
            rows_by_id = {cast(str, row["admission_id"]): row for row in available_rows}
            if any(admission_id not in rows_by_id for admission_id in admission_ids):
                raise ValueError("Wake callback set references a missing accepted admission")
            rows = tuple(rows_by_id[admission_id] for admission_id in admission_ids)
        if not rows:
            raise ValueError("Attention Wake has no accepted Agent Watch admission")
        bindings: list[WatchCallbackBinding] = []
        for row in rows:
            admission = agent_watch_admission_from_dict(
                self.store.artifacts.read_json(cast(str, row["artifact_hash"]))
            )
            self._reopen_parent_admission(admission)
            request = self._request(cast(str, row["request_id"]))
            profile = self.profiles.get(cast(str, row["delegate_profile_id"]))
            if profile is None:
                raise ValueError("Watch callback profile is no longer registered")
            bindings.append(
                WatchCallbackBinding(
                    admission=admission,
                    request=request,
                    wake=wake,
                    profile=profile,
                    authority_watch_id=authority_watch_id,
                    rebaseline_grant_ids=rebaseline_grant_ids,
                )
            )
        return tuple(bindings)

    def _require_parent_authority_integration(self, operation: str) -> None:
        if self._has_parent_authority():
            return
        raise ValueError(
            "Agent Watch parent authority integration is not configured; "
            f"{operation} cannot be authorized"
        )

    def _reopen_parent_admission(
        self,
        admission: AgentWatchAdmission,
    ) -> AgentDelegationContext:
        from market_impact_agent.research_thesis_watch import ResearchThesisWatchAuthorityResolver

        if type(self.delegation_authority) is ResearchThesisWatchAuthorityResolver:
            context = self.delegation_authority.delegation_context(admission.parent_ref)
            if (
                admission.outcome is not WatchAdmissionOutcome.REJECTED
                and not self._profile_authorized_by_parent(context, admission.delegate_profile_id)
            ):
                raise ValueError("Watch admission profile is outside its signed parent offer")
        else:
            context = self._triage_authority(admission.parent_ref).delegation_context()
        if (
            admission.parent_ref != context.parent_ref
            or admission.parent_agent_type != context.parent_agent_type
            or admission.parent_authority_hash != context.authority_hash
            or admission.lineage_depth != context.lineage_depth + 1
        ):
            raise ValueError("Agent Watch admission differs from reopened Triage authority")
        return context

    def _has_parent_authority(self) -> bool:
        from market_impact_agent.research_thesis_watch import ResearchThesisWatchAuthorityResolver

        return (
            self._has_triage_authority()
            or type(self.delegation_authority) is ResearchThesisWatchAuthorityResolver
        )

    def _profile_authorized_by_parent(
        self, context: AgentDelegationContext, profile_id: str
    ) -> bool:
        from market_impact_agent.research_thesis_watch import ResearchThesisWatchAuthorityResolver

        if type(self.delegation_authority) is ResearchThesisWatchAuthorityResolver:
            return profile_id in self.delegation_authority.offered_profile_ids(context.parent_ref)
        return True

    def _has_triage_authority(self) -> bool:
        return type(self.delegation_authority) in {
            EventImpactTriageWatchAuthority,
            EventImpactTriageWatchAuthorityResolver,
        }

    def _triage_authority(self, parent_ref: str) -> EventImpactTriageWatchAuthority:
        authority = self.delegation_authority
        if type(authority) is EventImpactTriageWatchAuthority:
            resolved = authority
            if resolved.cluster_id != parent_ref:
                raise ValueError("Agent Watch parent differs from concrete Triage authority")
            return resolved
        if type(authority) is EventImpactTriageWatchAuthorityResolver:
            return authority.authority(parent_ref)
        raise ValueError("Agent Watch admission has no concrete Triage parent authority")

    def callback_binding(self, wake: AttentionWake) -> WatchCallbackBinding:
        bindings = self.callback_bindings(wake)
        if len(bindings) != 1:
            raise ValueError("shared Attention Wake requires callback_bindings fan-out")
        return bindings[0]

    def _record_request(self, request: AgentWatchRequest, *, received_at: datetime) -> None:
        artifact = self.store.artifacts.put_json(request.to_dict())
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            existing = connection.execute(
                "SELECT artifact_hash FROM agent_watch_requests WHERE request_id = ?",
                (request.request_id,),
            ).fetchone()
            if existing is not None:
                if cast(str, existing["artifact_hash"]) != artifact.content_hash:
                    raise ValueError("Agent Watch request identity has conflicting content")
                return
            connection.execute(
                """
                INSERT INTO agent_watch_requests(request_id, artifact_hash, received_at)
                VALUES (?, ?, ?)
                """,
                (request.request_id, artifact.content_hash, _timestamp(received_at)),
            )

    def _request(self, request_id: str) -> AgentWatchRequest:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT artifact_hash FROM agent_watch_requests WHERE request_id = ?",
                (request_id,),
            ).fetchone()
        if row is None:
            raise KeyError(f"unknown Agent Watch request: {request_id}")
        return agent_watch_request_from_dict(
            self.store.artifacts.read_json(cast(str, row["artifact_hash"]))
        )

    def _existing_admission(
        self,
        request_id: str,
        *,
        context: AgentDelegationContext,
    ) -> AgentWatchAdmission | None:
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT artifact_hash FROM agent_watch_admissions
                WHERE request_id = ? AND parent_ref = ? AND parent_agent_type = ?
                  AND lineage_depth = ?
                """,
                (
                    request_id,
                    context.parent_ref,
                    context.parent_agent_type,
                    context.lineage_depth + 1,
                ),
            ).fetchone()
        if row is None:
            return None
        return agent_watch_admission_from_dict(
            self.store.artifacts.read_json(cast(str, row["artifact_hash"]))
        )

    def _reject(
        self,
        request: AgentWatchRequest,
        context: AgentDelegationContext,
        decided_at: datetime,
        blocker: WatchAdmissionBlocker,
    ) -> AgentWatchAdmission:
        admission = _admission(
            request=request,
            context=context,
            outcome=WatchAdmissionOutcome.REJECTED,
            blocker=blocker,
            operational_scope_key=None,
            monitoring_scope_id=None,
            retrieval_plan_id=None,
            watch_id=None,
            callback_agent_type=None,
            admitted_at=decided_at,
        )
        return self._persist_admission(admission, watch_policy_hash=None, expires_at=None)

    def _commit_acceptance(
        self,
        *,
        request: AgentWatchRequest,
        context: AgentDelegationContext,
        profile: WatchDelegateProfile,
        scope: MonitoringScope,
        plan: RetrievalPlan,
        watch: AttentionWatchPolicy,
        operational_key: str,
        decided_at: datetime,
    ) -> AgentWatchAdmission:
        watch_artifact = self.store.artifacts.put_json(watch.to_dict())
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            existing = connection.execute(
                """
                SELECT artifact_hash FROM agent_watch_admissions
                WHERE request_id = ? AND parent_ref = ? AND parent_agent_type = ?
                  AND lineage_depth = ?
                """,
                (
                    request.request_id,
                    context.parent_ref,
                    context.parent_agent_type,
                    context.lineage_depth + 1,
                ),
            ).fetchone()
            if existing is not None:
                return agent_watch_admission_from_dict(
                    self.store.artifacts.read_json(cast(str, existing["artifact_hash"]))
                )
            branch_count = cast(
                int,
                connection.execute(
                    """
                    SELECT COUNT(*) AS count FROM agent_watch_admissions
                    WHERE parent_ref = ? AND outcome IN (?, ?)
                    """,
                    (
                        context.parent_ref,
                        WatchAdmissionOutcome.ADMITTED.value,
                        WatchAdmissionOutcome.REUSED.value,
                    ),
                ).fetchone()["count"],
            )
            if branch_count >= profile.maximum_children_per_parent:
                return self._reject_in_connection(
                    connection,
                    request,
                    context,
                    decided_at,
                    WatchAdmissionBlocker.BRANCH_LIMIT_EXHAUSTED,
                )
            equivalent = connection.execute(
                """
                SELECT admission.artifact_hash, watch.watch_id AS active_watch_id,
                       watch.poll_count, watch.byte_count, watch.wake_count
                FROM agent_watch_admissions AS admission
                LEFT JOIN attention_watch_policies AS watch
                  ON watch.watch_id = admission.watch_id
                WHERE admission.operational_scope_key = ?
                  AND admission.delegate_profile_id = ?
                  AND admission.outcome = ?
                  AND admission.expires_at >= ?
                  AND (
                    watch.status IN (?, ?, ?)
                    OR (watch.watch_id IS NULL AND admission.watch_policy_hash IS NOT NULL)
                  )
                ORDER BY admission.admitted_at, admission.admission_id
                LIMIT 1
                """,
                (
                    operational_key,
                    profile.profile_id,
                    WatchAdmissionOutcome.ADMITTED.value,
                    _timestamp(watch.expires_at),
                    *_ACTIVE_WATCH_STATUSES,
                ),
            ).fetchone()
            if equivalent is not None:
                if cast(str | None, equivalent["active_watch_id"]) is not None and (
                    cast(int, equivalent["poll_count"]) >= profile.maximum_polls
                    or cast(int, equivalent["byte_count"]) >= profile.maximum_bytes
                    or cast(int, equivalent["wake_count"]) >= profile.maximum_wakes
                ):
                    return self._reject_in_connection(
                        connection,
                        request,
                        context,
                        decided_at,
                        WatchAdmissionBlocker.WATCH_BUDGET_EXHAUSTED,
                    )
                owner = agent_watch_admission_from_dict(
                    self.store.artifacts.read_json(cast(str, equivalent["artifact_hash"]))
                )
                reused = _admission(
                    request=request,
                    context=context,
                    outcome=WatchAdmissionOutcome.REUSED,
                    blocker=None,
                    operational_scope_key=operational_key,
                    monitoring_scope_id=owner.monitoring_scope_id,
                    retrieval_plan_id=owner.retrieval_plan_id,
                    watch_id=owner.watch_id,
                    callback_agent_type=profile.callback_agent_type,
                    admitted_at=decided_at,
                )
                self._insert_admission(connection, reused, expires_at=watch.expires_at)
                return reused
            active_count = cast(
                int,
                connection.execute(
                    """
                    SELECT COUNT(DISTINCT admission.watch_id) AS count
                    FROM agent_watch_admissions AS admission
                    LEFT JOIN attention_watch_policies AS watch
                      ON watch.watch_id = admission.watch_id
                    WHERE admission.outcome = ?
                      AND admission.expires_at >= ?
                      AND (
                        watch.status IN (?, ?, ?)
                        OR (watch.watch_id IS NULL AND admission.watch_policy_hash IS NOT NULL)
                      )
                    """,
                    (
                        WatchAdmissionOutcome.ADMITTED.value,
                        _timestamp(decided_at),
                        *_ACTIVE_WATCH_STATUSES,
                    ),
                ).fetchone()["count"],
            )
            if active_count >= profile.maximum_active_watches:
                return self._reject_in_connection(
                    connection,
                    request,
                    context,
                    decided_at,
                    WatchAdmissionBlocker.ACTIVE_WATCH_LIMIT_EXHAUSTED,
                )
            admitted = _admission(
                request=request,
                context=context,
                outcome=WatchAdmissionOutcome.ADMITTED,
                blocker=None,
                operational_scope_key=operational_key,
                monitoring_scope_id=scope.scope_id,
                retrieval_plan_id=plan.plan_id,
                watch_id=watch.watch_id,
                callback_agent_type=profile.callback_agent_type,
                admitted_at=decided_at,
            )
            self._insert_admission(
                connection,
                admitted,
                watch_policy_hash=watch_artifact.content_hash,
                expires_at=watch.expires_at,
            )
            return admitted

    def _reject_in_connection(
        self,
        connection: sqlite3.Connection,
        request: AgentWatchRequest,
        context: AgentDelegationContext,
        decided_at: datetime,
        blocker: WatchAdmissionBlocker,
    ) -> AgentWatchAdmission:
        admission = _admission(
            request=request,
            context=context,
            outcome=WatchAdmissionOutcome.REJECTED,
            blocker=blocker,
            operational_scope_key=None,
            monitoring_scope_id=None,
            retrieval_plan_id=None,
            watch_id=None,
            callback_agent_type=None,
            admitted_at=decided_at,
        )
        self._insert_admission(connection, admission)
        return admission

    def _insert_admission(
        self,
        connection: sqlite3.Connection,
        admission: AgentWatchAdmission,
        *,
        watch_policy_hash: str | None = None,
        expires_at: datetime | None = None,
    ) -> None:
        artifact = self.store.artifacts.put_json(admission.to_dict())
        connection.execute(
            """
            INSERT INTO agent_watch_admissions(
                admission_id, artifact_hash, request_id, parent_ref, parent_agent_type,
                lineage_depth, delegate_profile_id, outcome, blocker,
                operational_scope_key, watch_id, watch_policy_hash, expires_at, admitted_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                admission.admission_id,
                artifact.content_hash,
                admission.request_id,
                admission.parent_ref,
                admission.parent_agent_type,
                admission.lineage_depth,
                admission.delegate_profile_id,
                admission.outcome.value,
                admission.blocker.value if admission.blocker is not None else None,
                admission.operational_scope_key,
                admission.watch_id,
                watch_policy_hash,
                None if expires_at is None else _timestamp(expires_at),
                _timestamp(admission.admitted_at),
            ),
        )

    def _persist_admission(
        self,
        admission: AgentWatchAdmission,
        *,
        watch_policy_hash: str | None,
        expires_at: datetime | None,
    ) -> AgentWatchAdmission:
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            existing = connection.execute(
                """
                SELECT artifact_hash FROM agent_watch_admissions
                WHERE request_id = ? AND parent_ref = ? AND parent_agent_type = ?
                  AND lineage_depth = ?
                """,
                (
                    admission.request_id,
                    admission.parent_ref,
                    admission.parent_agent_type,
                    admission.lineage_depth,
                ),
            ).fetchone()
            if existing is None:
                self._insert_admission(
                    connection,
                    admission,
                    watch_policy_hash=watch_policy_hash,
                    expires_at=expires_at,
                )
                return admission
            return agent_watch_admission_from_dict(
                self.store.artifacts.read_json(cast(str, existing["artifact_hash"]))
            )

    def _ensure_watch_activated(self, admission: AgentWatchAdmission) -> None:
        if admission.outcome is WatchAdmissionOutcome.REJECTED:
            return
        if self._has_parent_authority():
            self._reopen_parent_admission(admission)
        if admission.watch_id is None:
            raise AssertionError("accepted Agent Watch admission is missing its Watch")
        try:
            self.watch_service.state(admission.watch_id)
            return
        except KeyError:
            pass
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT watch_policy_hash, artifact_hash
                FROM agent_watch_admissions
                WHERE watch_id = ? AND outcome = ? AND watch_policy_hash IS NOT NULL
                ORDER BY admitted_at, admission_id
                LIMIT 1
                """,
                (admission.watch_id, WatchAdmissionOutcome.ADMITTED.value),
            ).fetchone()
        if row is None:
            raise ValueError("Agent Watch admission has no owning Watch policy")
        policy = attention_watch_policy_from_dict(
            self.store.artifacts.read_json(cast(str, row["watch_policy_hash"]))
        )
        owner = agent_watch_admission_from_dict(
            self.store.artifacts.read_json(cast(str, row["artifact_hash"]))
        )
        if self._has_parent_authority():
            self._reopen_parent_admission(owner)
        try:
            self.watch_service.create(policy, created_at=owner.admitted_at)
        except sqlite3.IntegrityError:
            # Two accepted parents may race after committing one shared Watch identity.
            # Accept only the exact durable policy created by the other activator.
            if self.watch_service.policy(policy.watch_id) != policy:
                raise

    def _reconcile_pending_watch_activations(self) -> None:
        """Recover committed owner Admissions whose Watch creation was interrupted."""

        authority = self.delegation_authority
        if type(authority) is EventImpactTriageWatchAuthority:
            authority.delegation_context()

        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT admission.artifact_hash
                FROM agent_watch_admissions AS admission
                LEFT JOIN attention_watch_policies AS watch
                  ON watch.watch_id = admission.watch_id
                WHERE admission.outcome = ?
                  AND admission.watch_policy_hash IS NOT NULL
                  AND watch.watch_id IS NULL
                ORDER BY admission.admitted_at, admission.admission_id
                """,
                (WatchAdmissionOutcome.ADMITTED.value,),
            ).fetchall()
        for row in rows:
            admission = agent_watch_admission_from_dict(
                self.store.artifacts.read_json(cast(str, row["artifact_hash"]))
            )
            if (
                type(authority) is EventImpactTriageWatchAuthority
                and admission.parent_ref != authority.cluster_id
            ):
                continue
            from market_impact_agent.research_thesis_watch import (
                RESEARCH_WATCH_PARENT_TYPE,
                ResearchThesisWatchAuthorityResolver,
            )

            is_research = type(authority) is ResearchThesisWatchAuthorityResolver
            if (admission.parent_agent_type == RESEARCH_WATCH_PARENT_TYPE) != is_research:
                continue
            if type(
                authority
            ) is ResearchThesisWatchAuthorityResolver and not authority.owns_parent(
                admission.parent_ref
            ):
                continue
            self._reopen_parent_admission(admission)
            self._ensure_watch_activated(admission)


def _admission(
    *,
    request: AgentWatchRequest,
    context: AgentDelegationContext,
    outcome: WatchAdmissionOutcome,
    blocker: WatchAdmissionBlocker | None,
    operational_scope_key: str | None,
    monitoring_scope_id: str | None,
    retrieval_plan_id: str | None,
    watch_id: str | None,
    callback_agent_type: str | None,
    admitted_at: datetime,
) -> AgentWatchAdmission:
    values: dict[str, object] = {
        "schema_version": AGENT_WATCH_ADMISSION_SCHEMA,
        "request_id": request.request_id,
        "parent_ref": context.parent_ref,
        "parent_agent_type": context.parent_agent_type,
        "parent_authority_hash": context.authority_hash,
        "lineage_depth": context.lineage_depth + 1,
        "delegate_profile_id": request.delegate_profile_id,
        "outcome": outcome.value,
        "admitted_at": _timestamp(admitted_at),
        "execution_capability": False,
    }
    if blocker is not None:
        values["blocker"] = blocker.value
    else:
        values.update(
            {
                "operational_scope_key": operational_scope_key,
                "monitoring_scope_id": monitoring_scope_id,
                "retrieval_plan_id": retrieval_plan_id,
                "watch_id": watch_id,
                "callback_agent_type": callback_agent_type,
            }
        )
    return AgentWatchAdmission(
        admission_id=f"agent-watch-admission-{canonical_hash(values)}",
        request_id=request.request_id,
        parent_ref=context.parent_ref,
        parent_agent_type=context.parent_agent_type,
        parent_authority_hash=context.authority_hash,
        lineage_depth=context.lineage_depth + 1,
        delegate_profile_id=request.delegate_profile_id,
        outcome=outcome,
        blocker=blocker,
        operational_scope_key=operational_scope_key,
        monitoring_scope_id=monitoring_scope_id,
        retrieval_plan_id=retrieval_plan_id,
        watch_id=watch_id,
        callback_agent_type=callback_agent_type,
        admitted_at=admitted_at,
    )


def _operational_scope_key(profile: WatchDelegateProfile, scope: MonitoringScope) -> str:
    values = scope.core_dict()
    values.pop("origin_refs")
    return canonical_hash({"delegate_profile_id": profile.profile_id, "scope": values})


def _triage_matcher_terms(
    *,
    store: LocalDataSnapshotStore,
    candidate_set: EventImpactTriageCandidateSet,
    cluster: TriageClusterProposal,
) -> tuple[str, ...]:
    """Derive a small allowlist from frozen structured cluster and observation content."""

    snapshot = store.get(candidate_set.data_snapshot_id)
    observations_by_version = {
        prospective_observation_version_id(item): item for item in snapshot.observations
    }
    refs_by_version = {item.version_id: item for item in candidate_set.observations}
    selected: list[SourceObservation] = []
    for version_id in cluster.candidate_version_ids:
        observation = observations_by_version.get(version_id)
        ref = refs_by_version.get(version_id)
        if observation is None or ref is None:
            raise ValueError("Triage Watch cluster version is absent from its frozen Snapshot")
        if (
            observation.observation_id != ref.observation_id
            or observation.raw_content_hash != ref.raw_content_hash
            or canonical_hash(observation.normalized_payload) != ref.normalized_payload_hash
        ):
            raise ValueError("Triage Watch frozen observation differs from its Candidate Set")
        selected.append(observation)
    structured_values: list[object] = [
        cluster.changed_facts,
        cluster.watch_questions,
        cluster.uncertainty_notes,
        cluster.affected_entity_refs,
        tuple(item.value for item in cluster.event_archetypes),
        tuple(item.value for item in cluster.transmission_channels),
        tuple(item.normalized_payload for item in selected),
    ]
    terms: set[str] = set()
    pending: list[object] = list(structured_values)
    while pending:
        value = pending.pop()
        if isinstance(value, str):
            terms.update(_MATCHER_TERM.findall(value.casefold()))
        elif isinstance(value, Mapping):
            pending.extend(cast(Mapping[object, object], value).values())
        elif isinstance(value, (tuple, list)):
            pending.extend(cast(tuple[object, ...] | list[object], value))
    bounded = tuple(sorted(terms - _GENERIC_TRIAGE_MATCHER_TERMS))[:_MAX_AUTHORIZED_MATCHER_TERMS]
    if not bounded:
        raise ValueError("Triage Watch authority has no bounded structured matcher terms")
    return bounded


def _specific_triage_matcher(matcher: ObservationMatcher) -> bool:
    """Require either exact structured identity or multiple co-occurring text anchors."""

    if all(
        clause.mode is MonitoringMatchMode.EXACT and clause.field_path in _EXACT_TRIAGE_MATCH_FIELDS
        for clause in matcher.clauses
    ):
        return True
    terms = {term for clause in matcher.clauses for term in clause.terms}
    return len(terms) >= 2 and all(
        clause.mode is MonitoringMatchMode.CONTAINS_ALL for clause in matcher.clauses
    )


def _wake_callback_admission_ids(
    value: object,
    *,
    wake: AttentionWake,
) -> tuple[str, ...]:
    if not isinstance(value, Mapping):
        raise TypeError("Wake callback set must be an object")
    payload = cast(Mapping[object, object], value)
    if any(not isinstance(key, str) for key in payload):
        raise TypeError("Wake callback set keys must be strings")
    fields = cast(Mapping[str, object], payload)
    if set(fields) != {"schema_version", "wake_id", "watch_id", "admission_ids"}:
        raise ValueError("Wake callback set fields are invalid")
    if fields["schema_version"] != "market-impact.agent-watch-wake-callback-set.v1":
        raise ValueError("Wake callback set schema is unsupported")
    if fields["wake_id"] != wake.wake_id or fields["watch_id"] != wake.watch_id:
        raise ValueError("Wake callback set differs from durable Wake authority")
    raw_ids = fields["admission_ids"]
    if not isinstance(raw_ids, list):
        raise TypeError("Wake callback set admission_ids must be strings")
    raw_items = cast(list[object], raw_ids)
    if any(not isinstance(item, str) for item in raw_items):
        raise TypeError("Wake callback set admission_ids must be strings")
    admission_ids = tuple(cast(list[str], raw_items))
    if not admission_ids or len(set(admission_ids)) != len(admission_ids):
        raise ValueError("Wake callback set requires unique accepted admissions")
    return admission_ids


def agent_delegation_context_from_dict(value: object) -> AgentDelegationContext:
    payload = _object(value, "Agent delegation context")
    context = AgentDelegationContext(
        parent_ref=_string(payload, "parent_ref"),
        parent_agent_type=_string(payload, "parent_agent_type"),
        lineage_depth=_integer(payload, "lineage_depth"),
        created_at=_datetime(_string(payload, "created_at"), "created_at"),
        authorized_evidence_refs=_string_tuple(payload, "authorized_evidence_refs"),
        authorized_subjects=tuple(
            _subject_from_dict(item)
            for item in _list(payload.get("authorized_subjects"), "authorized_subjects")
        ),
        authorized_matcher_terms=_string_tuple(payload, "authorized_matcher_terms"),
    )
    if context.to_dict() != payload:
        raise ValueError("Agent delegation context does not match canonical contract")
    return context


def watch_delegate_profile_from_dict(value: object) -> WatchDelegateProfile:
    payload = _object(value, "Watch delegate profile")
    template_payload = _object(payload.get("query_template"), "Watch query template")
    template = RegisteredQueryTemplate(
        template_ref=_string(template_payload, "template_ref"),
        capability=ObservationCapability(_string(template_payload, "capability")),
        pit_lane=DataPITLane(_string(template_payload, "pit_lane")),
        allowed_match_field_paths=_string_tuple(
            template_payload,
            "allowed_match_field_paths",
        ),
        allowed_match_modes=tuple(
            MonitoringMatchMode(item)
            for item in _string_tuple(template_payload, "allowed_match_modes")
        ),
        maximum_match_clauses=_integer(template_payload, "maximum_match_clauses"),
        maximum_terms_per_clause=_integer(template_payload, "maximum_terms_per_clause"),
        maximum_term_length=_integer(template_payload, "maximum_term_length"),
    )
    profile = WatchDelegateProfile(
        profile_id=_string(payload, "profile_id"),
        name=_string(payload, "name"),
        description=_string(payload, "description"),
        callback_agent_type=_string(payload, "callback_agent_type"),
        callback_agent_profile_ref=_string(payload, "callback_agent_profile_ref"),
        allowed_parent_agent_types=_string_tuple(payload, "allowed_parent_agent_types"),
        allowed_subject_kinds=tuple(
            MonitoringSubjectKind(item) for item in _string_tuple(payload, "allowed_subject_kinds")
        ),
        preloaded_skills=_string_tuple(payload, "preloaded_skills"),
        skill_manifest_hashes=_string_tuple(payload, "skill_manifest_hashes"),
        required_capabilities=_string_tuple(payload, "required_capabilities"),
        query_template=template,
        collection_policy_id=_string(payload, "collection_policy_id"),
        use_class=MonitoringUseClass(_string(payload, "use_class")),
        freshness_max_age_seconds=_integer(payload, "freshness_max_age_seconds"),
        minimum_coverage_sources=_integer(payload, "minimum_coverage_sources"),
        maximum_polls=_integer(payload, "maximum_polls"),
        maximum_bytes=_integer(payload, "maximum_bytes"),
        maximum_wakes=_integer(payload, "maximum_wakes"),
        cooldown_seconds=_integer(payload, "cooldown_seconds"),
        active_duration_seconds=_integer(payload, "active_duration_seconds"),
        maximum_lineage_depth=_integer(payload, "maximum_lineage_depth"),
        maximum_children_per_parent=_integer(payload, "maximum_children_per_parent"),
        maximum_active_watches=_integer(payload, "maximum_active_watches"),
        callback_max_turns=_integer(payload, "callback_max_turns"),
        callback_max_input_tokens=_integer(payload, "callback_max_input_tokens"),
        callback_max_output_tokens=_integer(payload, "callback_max_output_tokens"),
        callback_max_cost_microusd=_integer(payload, "callback_max_cost_microusd"),
        execution_capability=_boolean(payload, "execution_capability"),
    )
    if profile.to_dict() != payload:
        raise ValueError("Watch delegate profile does not match canonical contract")
    return profile


def agent_watch_request_from_dict(value: object) -> AgentWatchRequest:
    payload = _object(value, "Agent Watch request")
    request = AgentWatchRequest(
        request_id=_string(payload, "request_id"),
        delegate_profile_id=_string(payload, "delegate_profile_id"),
        rationale=_string(payload, "rationale"),
        watch_question=_string(payload, "watch_question"),
        evidence_refs=_string_tuple(payload, "evidence_refs"),
        subject=_subject_from_dict(payload.get("subject")),
        frozen_members=tuple(
            _subject_from_dict(item)
            for item in _list(payload.get("frozen_members", []), "frozen_members")
        ),
        information_aspect_ref=_optional_string(payload.get("information_aspect_ref")),
        matcher=_matcher_from_dict(payload.get("matcher")),
        schema_version=_string(payload, "schema_version"),
    )
    if request.to_dict() != payload:
        raise ValueError("Agent Watch request does not match canonical contract")
    return request


def agent_watch_admission_from_dict(value: object) -> AgentWatchAdmission:
    payload = _object(value, "Agent Watch admission")
    admission = AgentWatchAdmission(
        admission_id=_string(payload, "admission_id"),
        request_id=_string(payload, "request_id"),
        parent_ref=_string(payload, "parent_ref"),
        parent_agent_type=_string(payload, "parent_agent_type"),
        parent_authority_hash=_string(payload, "parent_authority_hash"),
        lineage_depth=_integer(payload, "lineage_depth"),
        delegate_profile_id=_string(payload, "delegate_profile_id"),
        outcome=WatchAdmissionOutcome(_string(payload, "outcome")),
        blocker=(
            None
            if payload.get("blocker") is None
            else WatchAdmissionBlocker(_string(payload, "blocker"))
        ),
        operational_scope_key=_optional_string(payload.get("operational_scope_key")),
        monitoring_scope_id=_optional_string(payload.get("monitoring_scope_id")),
        retrieval_plan_id=_optional_string(payload.get("retrieval_plan_id")),
        watch_id=_optional_string(payload.get("watch_id")),
        callback_agent_type=_optional_string(payload.get("callback_agent_type")),
        admitted_at=_datetime(_string(payload, "admitted_at"), "admitted_at"),
        execution_capability=_boolean(payload, "execution_capability"),
        schema_version=_string(payload, "schema_version"),
    )
    if admission.to_dict() != payload:
        raise ValueError("Agent Watch admission does not match canonical contract")
    return admission


def _query_template_dict(template: RegisteredQueryTemplate) -> dict[str, object]:
    return {
        "template_ref": template.template_ref,
        "capability": template.capability.value,
        "pit_lane": template.pit_lane.value,
        **template.matcher_contract_dict(),
    }


def _subject_from_dict(value: object) -> MonitoringSubjectRef:
    payload = _object(value, "monitoring subject")
    subject = MonitoringSubjectRef(
        kind=MonitoringSubjectKind(_string(payload, "kind")),
        canonical_id=_string(payload, "canonical_id"),
    )
    if subject.to_dict() != payload:
        raise ValueError("monitoring subject does not match canonical contract")
    return subject


def _matcher_from_dict(value: object) -> ObservationMatcher:
    payload = _object(value, "monitoring matcher")
    matcher = ObservationMatcher(
        tuple(
            ObservationMatchClause(
                field_path=_string(clause, "field_path"),
                mode=MonitoringMatchMode(_string(clause, "mode")),
                terms=_string_tuple(clause, "terms"),
            )
            for clause in (
                _object(item, "monitoring matcher clause")
                for item in _list(payload.get("clauses"), "clauses")
            )
        )
    )
    if matcher.to_dict() != payload:
        raise ValueError("monitoring matcher does not match canonical contract")
    return matcher


def _canonical_ref(value: str, name: str) -> None:
    if not _CANONICAL_REF.fullmatch(value) or "://" in value:
        raise ValueError(f"{name} must be a canonical non-URL reference")


def _canonical_tuple(values: tuple[str, ...], name: str, *, allow_empty: bool = False) -> None:
    if (not values and not allow_empty) or values != tuple(sorted(set(values))):
        raise ValueError(f"{name} must be unique and sorted")
    for value in values:
        _canonical_ref(value, name)


def _sha256(value: str, name: str) -> None:
    if not re.fullmatch(r"[0-9a-f]{64}", value):
        raise ValueError(f"{name} must be a SHA-256 digest")


def _bounded_text(value: str, name: str, *, maximum: int) -> None:
    if value != value.strip() or not value or len(value) > maximum or "\x00" in value:
        raise ValueError(f"{name} must be bounded trimmed text")


def _subject_key(subject: MonitoringSubjectRef) -> tuple[str, str]:
    return (subject.kind.value, subject.canonical_id)


def _strict_utc(value: datetime, name: str) -> None:
    require_aware(value, name)
    if value.tzinfo is not UTC or value.utcoffset() != timedelta(0):
        raise ValueError(f"{name} must use the UTC singleton")


def _timestamp(value: datetime) -> str:
    _strict_utc(value, "timestamp")
    return value.isoformat().replace("+00:00", "Z")


def _datetime(value: str, name: str) -> datetime:
    if not value.endswith("Z"):
        raise ValueError(f"{name} must be a UTC timestamp")
    try:
        return datetime.fromisoformat(value[:-1] + "+00:00").astimezone(UTC)
    except ValueError as exc:
        raise ValueError(f"{name} must be an ISO 8601 timestamp") from exc


def _object(value: object, name: str) -> dict[str, object]:
    if not isinstance(value, Mapping):
        raise TypeError(f"{name} must be an object")
    raw = cast(Mapping[object, object], value)
    if any(not isinstance(key, str) for key in raw):
        raise TypeError(f"{name} keys must be strings")
    return cast(dict[str, object], dict(raw))


def _list(value: object, name: str) -> list[object]:
    if not isinstance(value, list):
        raise TypeError(f"{name} must be an array")
    return cast(list[object], value)


def _string(value: Mapping[str, object], key: str) -> str:
    item = value.get(key)
    if not isinstance(item, str):
        raise TypeError(f"{key} must be a string")
    return item


def _string_tuple(value: Mapping[str, object], key: str) -> tuple[str, ...]:
    return tuple(_string_value(item, key) for item in _list(value.get(key), key))


def _string_value(value: object, name: str) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{name} must be a string")
    return value


def _optional_string(value: object) -> str | None:
    if value is None:
        return None
    return _string_value(value, "optional string")


def _integer(value: Mapping[str, object], key: str) -> int:
    item = value.get(key)
    if not isinstance(item, int) or isinstance(item, bool):
        raise TypeError(f"{key} must be an integer")
    return item


def _boolean(value: Mapping[str, object], key: str) -> bool:
    item = value.get(key)
    if not isinstance(item, bool):
        raise TypeError(f"{key} must be a boolean")
    return item
