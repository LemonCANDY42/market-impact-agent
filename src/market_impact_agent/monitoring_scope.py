"""Content-identified, read-only monitoring scope and retrieval contracts.

This module deliberately stops at deterministic selection.  It neither schedules
collection nor dispatches an Agent: the existing collection policy and Harness
remain the owners of acquisition and all returned objects contain references,
not raw source bodies.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import cast

from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.data_inputs import (
    DataPITLane,
    DataSnapshot,
    DataSourceBinding,
    LocalDataSnapshotStore,
    SourceObservation,
)
from market_impact_agent.domain import require_aware
from market_impact_agent.observations import ObservationCapability
from market_impact_agent.prospective_data import (
    ProspectiveCollectionPolicy,
    ProspectiveDataJournal,
    prospective_observation_version_id,
)

MONITORING_SCOPE_SCHEMA = "market-impact.monitoring-scope.v1"
RETRIEVAL_PLAN_SCHEMA = "market-impact.retrieval-plan.v1"
RETRIEVAL_RESOLUTION_SCHEMA_V1 = "market-impact.retrieval-resolution.v1"
RETRIEVAL_RESOLUTION_SCHEMA = "market-impact.retrieval-resolution.v2"

_CANONICAL_REF = re.compile(r"^[a-z][a-z0-9._:-]{1,127}$")
_REGISTERED_REF = re.compile(
    r"^(?:monitoring-query-template|information-aspect|taxonomy|membership-mapping)-[0-9a-f]{64}$"
)
_ALLOWED_FIELD_PATHS = frozenset(
    {
        "event_cluster_ids",
        "industry_codes",
        "issuer_ids",
        "instrument_ids",
        "etf_ids",
        "subject_refs",
        "information_aspects",
        "tags",
        "headline",
        "title",
        "content",
        "channels",
        "ts_code",
        "record.title",
        "record.headline",
        "record.content",
        "record.channels",
        "record.ts_code",
    }
)


class MonitoringSubjectKind(StrEnum):
    EVENT_CLUSTER = "event_cluster"
    INDUSTRY = "industry"
    ISSUER = "issuer"
    INSTRUMENT = "instrument"
    ETF = "etf"
    FROZEN_SET = "frozen_set"
    INFORMATION_ASPECT = "information_aspect"


class MonitoringUseClass(StrEnum):
    PUBLIC = "public"
    PRIVATE_INTERNAL = "private_internal"
    LICENSED_INTERNAL = "licensed_internal"


class MonitoringMatchMode(StrEnum):
    EXACT = "exact"
    CONTAINS_ALL = "contains_all"
    CONTAINS_ANY = "contains_any"


class RetrievalOutcome(StrEnum):
    EXACT_CACHE_HIT = "exact_cache_hit"
    JOURNAL_FREEZE = "journal_freeze"
    FETCH_REQUIRED = "fetch_required"
    UNAVAILABLE = "unavailable"


class RetrievalGapKind(StrEnum):
    CACHE_MISS = "cache_miss"
    CAPABILITY_MISMATCH = "capability_mismatch"
    PIT_LANE_MISMATCH = "pit_lane_mismatch"
    COLLECTION_POLICY_MISMATCH = "collection_policy_mismatch"
    SOURCE_SET_MISMATCH = "source_set_mismatch"
    COVERAGE_INCOMPLETE = "coverage_incomplete"
    COVERAGE_TOO_NARROW = "coverage_too_narrow"
    STALE = "stale"
    FETCH_NOT_PERMITTED = "fetch_not_permitted"
    FETCH_BUDGET_EXHAUSTED = "fetch_budget_exhausted"
    BYTE_BUDGET_EXHAUSTED = "byte_budget_exhausted"
    PIT_CUTOFF_EXCEEDED = "pit_cutoff_exceeded"


class RetrievalBarrier(StrEnum):
    NONE = "none"
    CACHE = "cache"
    PIT = "pit"
    COVERAGE = "coverage"
    FRESHNESS = "freshness"
    ACQUISITION = "acquisition"


@dataclass(frozen=True, slots=True)
class MonitoringSubjectRef:
    kind: MonitoringSubjectKind
    canonical_id: str

    def __post_init__(self) -> None:
        _canonical_ref(self.canonical_id, "monitoring subject canonical_id")

    def to_dict(self) -> dict[str, str]:
        return {"kind": self.kind.value, "canonical_id": self.canonical_id}


@dataclass(frozen=True, slots=True)
class EffectiveMembershipContext:
    taxonomy_ref: str
    mapping_ref: str
    effective_at: datetime

    def __post_init__(self) -> None:
        _registered_ref(self.taxonomy_ref, "taxonomy_ref", "taxonomy-")
        _registered_ref(self.mapping_ref, "mapping_ref", "membership-mapping-")
        _strict_utc(self.effective_at, "effective_at")

    def to_dict(self) -> dict[str, object]:
        return {
            "taxonomy_ref": self.taxonomy_ref,
            "mapping_ref": self.mapping_ref,
            "effective_at": _timestamp(self.effective_at),
        }


@dataclass(frozen=True, slots=True)
class ObservationMatchClause:
    field_path: str
    mode: MonitoringMatchMode
    terms: tuple[str, ...]

    def __post_init__(self) -> None:
        if self.field_path not in _ALLOWED_FIELD_PATHS:
            raise ValueError("monitoring matcher field_path is not allowlisted")
        if not self.terms or self.terms != tuple(sorted(set(self.terms))):
            raise ValueError("monitoring matcher terms must be non-empty, unique, and sorted")
        for term in self.terms:
            _normalized_term(term)

    @classmethod
    def build(
        cls,
        *,
        field_path: str,
        mode: MonitoringMatchMode,
        terms: tuple[str, ...],
    ) -> ObservationMatchClause:
        normalized = tuple(sorted({_normalized_term(item) for item in terms}))
        return cls(field_path=field_path, mode=mode, terms=normalized)

    def to_dict(self) -> dict[str, object]:
        return {
            "field_path": self.field_path,
            "mode": self.mode.value,
            "terms": list(self.terms),
        }


@dataclass(frozen=True, slots=True)
class ObservationMatcher:
    clauses: tuple[ObservationMatchClause, ...]

    def __post_init__(self) -> None:
        if not self.clauses:
            raise ValueError("monitoring matcher requires at least one clause")
        if len({(item.field_path, item.mode, item.terms) for item in self.clauses}) != len(
            self.clauses
        ):
            raise ValueError("monitoring matcher clauses must be unique")

    def to_dict(self) -> dict[str, object]:
        return {"clauses": [item.to_dict() for item in self.clauses]}

    def matches(self, normalized_payload: Mapping[str, object]) -> tuple[str, ...] | None:
        """Return matched field paths only; normalized values never leave this boundary."""
        matched_paths: list[str] = []
        for clause in self.clauses:
            values = _field_values(normalized_payload, clause.field_path)
            if not values or not _clause_matches(values, clause):
                return None
            matched_paths.append(clause.field_path)
        return tuple(matched_paths)


@dataclass(frozen=True, slots=True)
class MonitoringScope:
    scope_id: str
    origin_refs: tuple[str, ...]
    subject: MonitoringSubjectRef
    frozen_members: tuple[MonitoringSubjectRef, ...]
    effective_context: EffectiveMembershipContext | None
    query_template_ref: str
    information_aspect_ref: str | None
    capability: ObservationCapability
    pit_lane: DataPITLane
    freshness_max_age_seconds: int
    minimum_coverage_sources: int
    maximum_fetches: int
    maximum_bytes: int
    use_class: MonitoringUseClass
    matcher: ObservationMatcher
    schema_version: str = MONITORING_SCOPE_SCHEMA

    def __post_init__(self) -> None:
        if self.schema_version != MONITORING_SCOPE_SCHEMA:
            raise ValueError("unsupported Monitoring Scope schema")
        if not self.origin_refs or self.origin_refs != tuple(sorted(set(self.origin_refs))):
            raise ValueError("Monitoring Scope origin_refs must be non-empty, unique, and sorted")
        for origin_ref in self.origin_refs:
            _canonical_ref(origin_ref, "Monitoring Scope origin_ref")
        _registered_ref(
            self.query_template_ref,
            "Monitoring Scope query_template_ref",
            "monitoring-query-template-",
        )
        if self.information_aspect_ref is not None:
            _registered_ref(
                self.information_aspect_ref,
                "Monitoring Scope information_aspect_ref",
                "information-aspect-",
            )
        if self.subject.kind is MonitoringSubjectKind.FROZEN_SET:
            if (
                not self.frozen_members
                or len(set(self.frozen_members)) != len(self.frozen_members)
                or self.frozen_members != tuple(sorted(self.frozen_members, key=_subject_key))
            ):
                raise ValueError("frozen Monitoring Scope members must be non-empty and canonical")
        elif self.frozen_members:
            raise ValueError("only frozen_set Monitoring Scopes may carry frozen_members")
        if self.subject.kind in {MonitoringSubjectKind.INDUSTRY, MonitoringSubjectKind.ETF}:
            if self.effective_context is None:
                raise ValueError("industry and ETF Monitoring Scopes require effective context")
        elif self.effective_context is not None:
            raise ValueError(
                "effective context is only valid for industry and ETF Monitoring Scopes"
            )
        if (
            self.subject.kind is MonitoringSubjectKind.INFORMATION_ASPECT
            and self.information_aspect_ref is None
        ):
            raise ValueError("information_aspect Monitoring Scope requires registered aspect")
        for value, name in (
            (self.freshness_max_age_seconds, "freshness_max_age_seconds"),
            (self.minimum_coverage_sources, "minimum_coverage_sources"),
            (self.maximum_fetches, "maximum_fetches"),
            (self.maximum_bytes, "maximum_bytes"),
        ):
            if value < 0:
                raise ValueError(f"Monitoring Scope {name} must be non-negative")
        if self.minimum_coverage_sources < 1:
            raise ValueError("Monitoring Scope minimum_coverage_sources must be positive")
        if self.scope_id != self.expected_scope_id:
            raise ValueError("Monitoring Scope scope_id does not match content")

    @property
    def expected_scope_id(self) -> str:
        return f"monitoring-scope-{canonical_hash(self.core_dict())}"

    def core_dict(self) -> dict[str, object]:
        core: dict[str, object] = {
            "schema_version": self.schema_version,
            "origin_refs": list(self.origin_refs),
            "subject": self.subject.to_dict(),
            "query_template_ref": self.query_template_ref,
            "capability": self.capability.value,
            "pit_lane": self.pit_lane.value,
            "freshness_max_age_seconds": self.freshness_max_age_seconds,
            "minimum_coverage_sources": self.minimum_coverage_sources,
            "maximum_fetches": self.maximum_fetches,
            "maximum_bytes": self.maximum_bytes,
            "use_class": self.use_class.value,
            "matcher": self.matcher.to_dict(),
        }
        if self.frozen_members:
            core["frozen_members"] = [item.to_dict() for item in self.frozen_members]
        if self.effective_context is not None:
            core["effective_context"] = self.effective_context.to_dict()
        if self.information_aspect_ref is not None:
            core["information_aspect_ref"] = self.information_aspect_ref
        return core

    def to_dict(self) -> dict[str, object]:
        return {**self.core_dict(), "scope_id": self.scope_id}

    @classmethod
    def build(
        cls,
        *,
        origin_refs: tuple[str, ...],
        subject: MonitoringSubjectRef,
        query_template_ref: str,
        capability: ObservationCapability,
        pit_lane: DataPITLane,
        freshness_max_age_seconds: int,
        minimum_coverage_sources: int,
        maximum_fetches: int,
        maximum_bytes: int,
        use_class: MonitoringUseClass,
        frozen_members: tuple[MonitoringSubjectRef, ...] = (),
        effective_context: EffectiveMembershipContext | None = None,
        information_aspect_ref: str | None = None,
        matcher: ObservationMatcher | None = None,
    ) -> MonitoringScope:
        origins = tuple(sorted(set(origin_refs)))
        members = tuple(sorted(set(frozen_members), key=_subject_key))
        resolved_matcher = _default_matcher(subject, members) if matcher is None else matcher
        core: dict[str, object] = {
            "schema_version": MONITORING_SCOPE_SCHEMA,
            "origin_refs": list(origins),
            "subject": subject.to_dict(),
            "query_template_ref": query_template_ref,
            "capability": capability.value,
            "pit_lane": pit_lane.value,
            "freshness_max_age_seconds": freshness_max_age_seconds,
            "minimum_coverage_sources": minimum_coverage_sources,
            "maximum_fetches": maximum_fetches,
            "maximum_bytes": maximum_bytes,
            "use_class": use_class.value,
            "matcher": resolved_matcher.to_dict(),
        }
        if members:
            core["frozen_members"] = [item.to_dict() for item in members]
        if effective_context is not None:
            core["effective_context"] = effective_context.to_dict()
        if information_aspect_ref is not None:
            core["information_aspect_ref"] = information_aspect_ref
        return cls(
            scope_id=f"monitoring-scope-{canonical_hash(core)}",
            origin_refs=origins,
            subject=subject,
            frozen_members=members,
            effective_context=effective_context,
            query_template_ref=query_template_ref,
            information_aspect_ref=information_aspect_ref,
            capability=capability,
            pit_lane=pit_lane,
            freshness_max_age_seconds=freshness_max_age_seconds,
            minimum_coverage_sources=minimum_coverage_sources,
            maximum_fetches=maximum_fetches,
            maximum_bytes=maximum_bytes,
            use_class=use_class,
            matcher=resolved_matcher,
        )


@dataclass(frozen=True, slots=True)
class RegisteredQueryTemplate:
    """An allowlisted query surface; it intentionally contains no route or URL."""

    template_ref: str
    capability: ObservationCapability
    pit_lane: DataPITLane
    allowed_match_field_paths: tuple[str, ...] = tuple(sorted(_ALLOWED_FIELD_PATHS))
    allowed_match_modes: tuple[MonitoringMatchMode, ...] = tuple(
        sorted(MonitoringMatchMode, key=lambda item: item.value)
    )
    maximum_match_clauses: int = 8
    maximum_terms_per_clause: int = 8
    maximum_term_length: int = 256

    def __post_init__(self) -> None:
        _registered_ref(
            self.template_ref,
            "query template_ref",
            "monitoring-query-template-",
        )
        if (
            not self.allowed_match_field_paths
            or self.allowed_match_field_paths != tuple(sorted(set(self.allowed_match_field_paths)))
            or not set(self.allowed_match_field_paths) <= _ALLOWED_FIELD_PATHS
        ):
            raise ValueError(
                "query template match field paths must be allowlisted, unique, and sorted"
            )
        if not self.allowed_match_modes or self.allowed_match_modes != tuple(
            sorted(set(self.allowed_match_modes), key=lambda item: item.value)
        ):
            raise ValueError("query template match modes must be non-empty, unique, and sorted")
        for value, name in (
            (self.maximum_match_clauses, "maximum_match_clauses"),
            (self.maximum_terms_per_clause, "maximum_terms_per_clause"),
            (self.maximum_term_length, "maximum_term_length"),
        ):
            if value < 1:
                raise ValueError(f"query template {name} must be positive")
        if self.maximum_term_length > 256:
            raise ValueError("query template maximum_term_length exceeds monitoring bounds")

    @property
    def matcher_contract_hash(self) -> str:
        return canonical_hash(self.matcher_contract_dict())

    def matcher_contract_dict(self) -> dict[str, object]:
        return {
            "allowed_match_field_paths": list(self.allowed_match_field_paths),
            "allowed_match_modes": [item.value for item in self.allowed_match_modes],
            "maximum_match_clauses": self.maximum_match_clauses,
            "maximum_terms_per_clause": self.maximum_terms_per_clause,
            "maximum_term_length": self.maximum_term_length,
        }

    def assert_accepts_matcher(self, matcher: ObservationMatcher) -> None:
        if len(matcher.clauses) > self.maximum_match_clauses:
            raise ValueError("Monitoring Scope matcher exceeds template clause bound")
        for clause in matcher.clauses:
            if clause.field_path not in self.allowed_match_field_paths:
                raise ValueError(
                    "Monitoring Scope matcher field path is not registered by template"
                )
            if clause.mode not in self.allowed_match_modes:
                raise ValueError("Monitoring Scope matcher mode is not registered by template")
            if len(clause.terms) > self.maximum_terms_per_clause:
                raise ValueError("Monitoring Scope matcher exceeds template term bound")
            if any(len(item) > self.maximum_term_length for item in clause.terms):
                raise ValueError("Monitoring Scope matcher term exceeds template length bound")


@dataclass(frozen=True, slots=True)
class RetrievalPlan:
    plan_id: str
    scope_id: str
    query_template_ref: str
    template_matcher_contract_hash: str
    collection_policy_id: str
    capability: ObservationCapability
    pit_lane: DataPITLane
    sources: tuple[DataSourceBinding, ...]
    cadence_seconds: int
    maximum_gap_seconds: int
    freshness_max_age_seconds: int
    minimum_coverage_sources: int
    maximum_fetches: int
    maximum_bytes: int
    schema_version: str = RETRIEVAL_PLAN_SCHEMA

    def __post_init__(self) -> None:
        if self.schema_version != RETRIEVAL_PLAN_SCHEMA:
            raise ValueError("unsupported Retrieval Plan schema")
        if not self.scope_id.startswith("monitoring-scope-"):
            raise ValueError("Retrieval Plan requires a Monitoring Scope ID")
        _registered_ref(
            self.query_template_ref,
            "Retrieval Plan query_template_ref",
            "monitoring-query-template-",
        )
        if not re.fullmatch(r"[0-9a-f]{64}", self.template_matcher_contract_hash):
            raise ValueError("Retrieval Plan requires a matcher contract hash")
        if not self.collection_policy_id.startswith("prospective-collection-policy-"):
            raise ValueError("Retrieval Plan requires a registered collection policy ID")
        if not self.sources:
            raise ValueError("Retrieval Plan requires exact source bindings")
        if any(item.source_config_hash is None for item in self.sources):
            raise ValueError("Retrieval Plan source bindings require source configuration hashes")
        if len({item.source_key for item in self.sources}) != len(self.sources):
            raise ValueError("Retrieval Plan source bindings must be unique")
        if self.cadence_seconds < 1 or self.maximum_gap_seconds < 1:
            raise ValueError("Retrieval Plan cadence must be positive")
        for value, name in (
            (self.freshness_max_age_seconds, "freshness_max_age_seconds"),
            (self.minimum_coverage_sources, "minimum_coverage_sources"),
            (self.maximum_fetches, "maximum_fetches"),
            (self.maximum_bytes, "maximum_bytes"),
        ):
            if value < 0:
                raise ValueError(f"Retrieval Plan {name} must be non-negative")
        if not 1 <= self.minimum_coverage_sources <= len(self.sources):
            raise ValueError("Retrieval Plan minimum coverage must fit source bindings")
        if self.plan_id != self.expected_plan_id:
            raise ValueError("Retrieval Plan plan_id does not match content")

    @property
    def expected_plan_id(self) -> str:
        return f"retrieval-plan-{canonical_hash(self.core_dict())}"

    def core_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "scope_id": self.scope_id,
            "query_template_ref": self.query_template_ref,
            "template_matcher_contract_hash": self.template_matcher_contract_hash,
            "collection_policy_id": self.collection_policy_id,
            "capability": self.capability.value,
            "pit_lane": self.pit_lane.value,
            "sources": [item.to_dict() for item in self.sources],
            "cadence_seconds": self.cadence_seconds,
            "maximum_gap_seconds": self.maximum_gap_seconds,
            "freshness_max_age_seconds": self.freshness_max_age_seconds,
            "minimum_coverage_sources": self.minimum_coverage_sources,
            "maximum_fetches": self.maximum_fetches,
            "maximum_bytes": self.maximum_bytes,
        }

    def to_dict(self) -> dict[str, object]:
        return {**self.core_dict(), "plan_id": self.plan_id}

    @classmethod
    def bind(
        cls,
        *,
        scope: MonitoringScope,
        template: RegisteredQueryTemplate,
        collection_policy: ProspectiveCollectionPolicy,
    ) -> RetrievalPlan:
        if scope.query_template_ref != template.template_ref:
            raise ValueError("Retrieval Plan must bind the scope's registered query template")
        if (
            scope.capability is not template.capability
            or scope.capability is not collection_policy.capability
        ):
            raise ValueError(
                "Retrieval Plan capability must match scope, template, and collection policy"
            )
        if scope.pit_lane is not template.pit_lane:
            raise ValueError("Retrieval Plan PIT lane must match the registered query template")
        template.assert_accepts_matcher(scope.matcher)
        if scope.minimum_coverage_sources > len(collection_policy.sources):
            raise ValueError("Retrieval Plan scope coverage exceeds registered source set")
        core = {
            "schema_version": RETRIEVAL_PLAN_SCHEMA,
            "scope_id": scope.scope_id,
            "query_template_ref": template.template_ref,
            "template_matcher_contract_hash": template.matcher_contract_hash,
            "collection_policy_id": collection_policy.policy_id,
            "capability": scope.capability.value,
            "pit_lane": scope.pit_lane.value,
            "sources": [item.to_dict() for item in collection_policy.sources],
            "cadence_seconds": collection_policy.poll_interval_seconds,
            "maximum_gap_seconds": collection_policy.maximum_gap_seconds,
            "freshness_max_age_seconds": scope.freshness_max_age_seconds,
            "minimum_coverage_sources": scope.minimum_coverage_sources,
            "maximum_fetches": scope.maximum_fetches,
            "maximum_bytes": scope.maximum_bytes,
        }
        return cls(
            plan_id=f"retrieval-plan-{canonical_hash(core)}",
            scope_id=scope.scope_id,
            query_template_ref=template.template_ref,
            template_matcher_contract_hash=template.matcher_contract_hash,
            collection_policy_id=collection_policy.policy_id,
            capability=scope.capability,
            pit_lane=scope.pit_lane,
            sources=collection_policy.sources,
            cadence_seconds=collection_policy.poll_interval_seconds,
            maximum_gap_seconds=collection_policy.maximum_gap_seconds,
            freshness_max_age_seconds=scope.freshness_max_age_seconds,
            minimum_coverage_sources=scope.minimum_coverage_sources,
            maximum_fetches=scope.maximum_fetches,
            maximum_bytes=scope.maximum_bytes,
        )


@dataclass(frozen=True, slots=True)
class RetrievalResolution:
    resolution_id: str
    plan_id: str
    outcome: RetrievalOutcome
    selected_snapshot_ids: tuple[str, ...]
    gaps: tuple[RetrievalGapKind, ...]
    barrier: RetrievalBarrier
    requested_at: datetime | None = None
    schema_version: str = RETRIEVAL_RESOLUTION_SCHEMA_V1

    def __post_init__(self) -> None:
        if self.schema_version not in {
            RETRIEVAL_RESOLUTION_SCHEMA_V1,
            RETRIEVAL_RESOLUTION_SCHEMA,
        }:
            raise ValueError("unsupported Retrieval Resolution schema")
        if self.schema_version == RETRIEVAL_RESOLUTION_SCHEMA_V1:
            if self.requested_at is not None:
                raise ValueError("Retrieval Resolution v1 cannot carry requested_at")
        elif self.requested_at is None:
            raise ValueError("Retrieval Resolution v2 requires requested_at")
        else:
            _strict_utc(self.requested_at, "Retrieval Resolution requested_at")
        if not self.plan_id.startswith("retrieval-plan-"):
            raise ValueError("Retrieval Resolution requires a Retrieval Plan ID")
        if self.selected_snapshot_ids != tuple(sorted(set(self.selected_snapshot_ids))):
            raise ValueError("Retrieval Resolution snapshot IDs must be unique and sorted")
        if any(not item.startswith("data-snapshot-") for item in self.selected_snapshot_ids):
            raise ValueError("Retrieval Resolution requires Data Snapshot IDs")
        if self.gaps != tuple(sorted(set(self.gaps), key=lambda item: item.value)):
            raise ValueError("Retrieval Resolution gaps must be unique and sorted")
        selected = len(self.selected_snapshot_ids)
        if self.outcome in {
            RetrievalOutcome.EXACT_CACHE_HIT,
            RetrievalOutcome.JOURNAL_FREEZE,
        } and (selected != 1 or self.gaps or self.barrier is not RetrievalBarrier.NONE):
            raise ValueError("successful Retrieval Resolution must select one unblocked snapshot")
        if self.outcome is RetrievalOutcome.FETCH_REQUIRED and (
            selected or self.barrier is not RetrievalBarrier.NONE
        ):
            raise ValueError("fetch_required must not select a snapshot or carry a barrier")
        if self.outcome is RetrievalOutcome.UNAVAILABLE and (
            not self.gaps or self.barrier is RetrievalBarrier.NONE
        ):
            raise ValueError("unavailable Retrieval Resolution requires typed gaps and barrier")
        if self.resolution_id != self.expected_resolution_id:
            raise ValueError("Retrieval Resolution resolution_id does not match content")

    @property
    def expected_resolution_id(self) -> str:
        return f"retrieval-resolution-{canonical_hash(self.core_dict())}"

    def core_dict(self) -> dict[str, object]:
        result: dict[str, object] = {
            "schema_version": self.schema_version,
            "plan_id": self.plan_id,
            "outcome": self.outcome.value,
            "selected_snapshot_ids": list(self.selected_snapshot_ids),
            "gaps": [item.value for item in self.gaps],
            "barrier": self.barrier.value,
        }
        if self.schema_version == RETRIEVAL_RESOLUTION_SCHEMA:
            if self.requested_at is None:
                raise AssertionError("v2 Retrieval Resolution requires requested_at")
            result["requested_at"] = _timestamp(self.requested_at)
        return result

    def to_dict(self) -> dict[str, object]:
        return {**self.core_dict(), "resolution_id": self.resolution_id}


@dataclass(frozen=True, slots=True)
class MonitoringObservationMatch:
    """Safe projection of a match.  It deliberately contains no raw or normalized body."""

    observation_id: str
    version_id: str
    matched_field_paths: tuple[str, ...]
    license_scope: str


def match_scope_observation(
    scope: MonitoringScope, observation: SourceObservation
) -> MonitoringObservationMatch | None:
    if observation.capability is not scope.capability:
        return None
    if scope.use_class is MonitoringUseClass.PUBLIC and observation.license_scope != "public":
        return None
    matched_paths = scope.matcher.matches(observation.normalized_payload)
    if matched_paths is None:
        return None
    return MonitoringObservationMatch(
        observation_id=observation.observation_id,
        version_id=prospective_observation_version_id(observation),
        matched_field_paths=matched_paths,
        license_scope=observation.license_scope,
    )


def matched_scope_versions(scope: MonitoringScope, snapshot: DataSnapshot) -> tuple[str, ...]:
    """Find scope matches from allowlisted normalized fields without returning bodies."""
    if (
        snapshot.query.capability is not scope.capability
        or snapshot.query.pit_lane is not scope.pit_lane
    ):
        return ()
    return tuple(
        sorted(
            item.version_id
            for observation in snapshot.observations
            if (item := match_scope_observation(scope, observation)) is not None
        )
    )


def assert_scope_aware_watch_admission(
    scope: MonitoringScope,
    *,
    collection_policy_id: str,
    retrieval_plan: RetrievalPlan | None = None,
    query_template: RegisteredQueryTemplate | None = None,
    maximum_polls: int | None = None,
    maximum_bytes: int | None = None,
    collection_policy: ProspectiveCollectionPolicy | None = None,
) -> None:
    """Fail closed before a watch can use a scope-aware matching path."""
    if not collection_policy_id.startswith("prospective-collection-policy-"):
        raise ValueError("scope-aware watch requires a registered collection policy ID")
    if scope.pit_lane is not DataPITLane.PROSPECTIVE:
        raise ValueError("scope-aware watch requires the prospective PIT lane")
    if not scope.matcher.clauses:
        raise ValueError("scope-aware watch requires an exact deterministic matcher")
    if (
        retrieval_plan is None
        or query_template is None
        or maximum_polls is None
        or maximum_bytes is None
    ):
        raise ValueError("scope-aware watch requires Retrieval Plan and template bindings")
    if retrieval_plan.scope_id != scope.scope_id:
        raise ValueError("scope-aware watch Retrieval Plan must bind the exact Monitoring Scope")
    if retrieval_plan.collection_policy_id != collection_policy_id:
        raise ValueError("scope-aware watch Retrieval Plan must bind the exact collection policy")
    if (
        retrieval_plan.query_template_ref != scope.query_template_ref
        or retrieval_plan.capability is not scope.capability
        or retrieval_plan.pit_lane is not scope.pit_lane
    ):
        raise ValueError(
            "scope-aware watch Retrieval Plan must match scope template and capability"
        )
    if (
        retrieval_plan.freshness_max_age_seconds != scope.freshness_max_age_seconds
        or retrieval_plan.minimum_coverage_sources != scope.minimum_coverage_sources
        or retrieval_plan.maximum_fetches != scope.maximum_fetches
        or retrieval_plan.maximum_bytes != scope.maximum_bytes
    ):
        raise ValueError("scope-aware watch Retrieval Plan must preserve scope retrieval bounds")
    if maximum_polls > retrieval_plan.maximum_fetches:
        raise ValueError("scope-aware watch poll budget exceeds Retrieval Plan fetch budget")
    if maximum_bytes > retrieval_plan.maximum_bytes:
        raise ValueError("scope-aware watch byte budget exceeds Retrieval Plan byte budget")
    if (
        query_template.template_ref != retrieval_plan.query_template_ref
        or query_template.capability is not retrieval_plan.capability
        or query_template.pit_lane is not retrieval_plan.pit_lane
    ):
        raise ValueError("scope-aware watch query template must match Retrieval Plan")
    query_template.assert_accepts_matcher(scope.matcher)
    if query_template.matcher_contract_hash != retrieval_plan.template_matcher_contract_hash:
        raise ValueError("scope-aware watch query template matcher contract does not match plan")
    if collection_policy is not None and (
        collection_policy.policy_id != retrieval_plan.collection_policy_id
        or collection_policy.capability is not retrieval_plan.capability
        or collection_policy.sources != retrieval_plan.sources
        or collection_policy.poll_interval_seconds != retrieval_plan.cadence_seconds
        or collection_policy.maximum_gap_seconds != retrieval_plan.maximum_gap_seconds
    ):
        raise ValueError("scope-aware watch collection policy does not match Retrieval Plan")


def resolve_retrieval(
    plan: RetrievalPlan,
    *,
    requested_at: datetime,
    cache: LocalDataSnapshotStore | None = None,
    cached_snapshot_id: str | None = None,
    journal: ProspectiveDataJournal | None = None,
    journal_snapshot_id: str | None = None,
    fetch_permitted: bool = False,
) -> RetrievalResolution:
    """Resolve frozen inputs or request acquisition; never accept direct fetch output."""
    _strict_utc(requested_at, "retrieval requested_at")
    if (cache is None) != (cached_snapshot_id is None):
        raise ValueError("cache resolution requires both Snapshot store and Snapshot ID")
    cached_snapshot = (
        None if cache is None or cached_snapshot_id is None else cache.get(cached_snapshot_id)
    )
    if (journal is None) != (journal_snapshot_id is None):
        raise ValueError("journal resolution requires both Journal and frozen Snapshot ID")
    journal_snapshot: DataSnapshot | None = None
    if journal is not None and journal_snapshot_id is not None:
        journal_snapshot = journal.store.get(journal_snapshot_id)
        journal.assert_frozen_snapshot(journal_snapshot)
    gaps: set[RetrievalGapKind] = set()
    fetch_budget_gaps = _fetch_budget_gaps(plan)
    for snapshot, outcome in (
        (cached_snapshot, RetrievalOutcome.EXACT_CACHE_HIT),
        (journal_snapshot, RetrievalOutcome.JOURNAL_FREEZE),
    ):
        if snapshot is None:
            continue
        snapshot_gaps = _snapshot_gaps(plan, snapshot, requested_at)
        if not snapshot_gaps:
            return _resolution(
                plan,
                outcome,
                (snapshot.snapshot_id,),
                (),
                RetrievalBarrier.NONE,
                requested_at,
            )
        gaps.update(snapshot_gaps)
    if cached_snapshot is None and journal_snapshot is None:
        gaps.add(RetrievalGapKind.CACHE_MISS)
    if fetch_permitted:
        if fetch_budget_gaps:
            ordered_gaps = tuple(sorted(gaps | fetch_budget_gaps, key=lambda item: item.value))
            return _resolution(
                plan,
                RetrievalOutcome.UNAVAILABLE,
                (),
                ordered_gaps,
                RetrievalBarrier.ACQUISITION,
                requested_at,
            )
        return _resolution(
            plan,
            RetrievalOutcome.FETCH_REQUIRED,
            (),
            tuple(sorted(gaps, key=lambda item: item.value)),
            RetrievalBarrier.NONE,
            requested_at,
        )
    if not fetch_permitted:
        gaps.add(RetrievalGapKind.FETCH_NOT_PERMITTED)
    ordered_gaps = tuple(sorted(gaps, key=lambda item: item.value))
    return _resolution(
        plan,
        RetrievalOutcome.UNAVAILABLE,
        (),
        ordered_gaps,
        _barrier_for(ordered_gaps),
        requested_at,
    )


def monitoring_scope_from_dict(value: object) -> MonitoringScope:
    payload = _object(value, "Monitoring Scope")
    subject = _subject_from_dict(_object(payload.get("subject"), "Monitoring Scope subject"))
    members = tuple(
        _subject_from_dict(_object(item, "Monitoring Scope frozen member"))
        for item in _list(payload.get("frozen_members", []), "Monitoring Scope frozen_members")
    )
    context_value = payload.get("effective_context")
    context = None
    if context_value is not None:
        raw_context = _object(context_value, "Monitoring Scope effective_context")
        context = EffectiveMembershipContext(
            taxonomy_ref=_string(raw_context, "taxonomy_ref"),
            mapping_ref=_string(raw_context, "mapping_ref"),
            effective_at=_datetime(_string(raw_context, "effective_at"), "effective_at"),
        )
    matcher = _matcher_from_dict(_object(payload.get("matcher"), "Monitoring Scope matcher"))
    return MonitoringScope(
        scope_id=_string(payload, "scope_id"),
        origin_refs=tuple(
            _string_value(item, "Monitoring Scope origin_ref")
            for item in _list(payload.get("origin_refs"), "Monitoring Scope origin_refs")
        ),
        subject=subject,
        frozen_members=members,
        effective_context=context,
        query_template_ref=_string(payload, "query_template_ref"),
        information_aspect_ref=_optional_string(payload.get("information_aspect_ref")),
        capability=ObservationCapability(_string(payload, "capability")),
        pit_lane=DataPITLane(_string(payload, "pit_lane")),
        freshness_max_age_seconds=_integer(payload, "freshness_max_age_seconds"),
        minimum_coverage_sources=_integer(payload, "minimum_coverage_sources"),
        maximum_fetches=_integer(payload, "maximum_fetches"),
        maximum_bytes=_integer(payload, "maximum_bytes"),
        use_class=MonitoringUseClass(_string(payload, "use_class")),
        matcher=matcher,
        schema_version=_string(payload, "schema_version"),
    )


def retrieval_plan_from_dict(value: object) -> RetrievalPlan:
    payload = _object(value, "Retrieval Plan")
    sources = tuple(
        DataSourceBinding(
            provider_id=_string(source, "provider_id"),
            provider_version=_string(source, "provider_version"),
            upstream_source=_string(source, "upstream_source"),
            manifest_hash=_string(source, "manifest_hash"),
            source_config_hash=_string(source, "source_config_hash"),
            required=_boolean(source, "required"),
        )
        for source in (
            _object(item, "Retrieval Plan source")
            for item in _list(payload.get("sources"), "Retrieval Plan sources")
        )
    )
    plan = RetrievalPlan(
        plan_id=_string(payload, "plan_id"),
        scope_id=_string(payload, "scope_id"),
        query_template_ref=_string(payload, "query_template_ref"),
        template_matcher_contract_hash=_string(payload, "template_matcher_contract_hash"),
        collection_policy_id=_string(payload, "collection_policy_id"),
        capability=ObservationCapability(_string(payload, "capability")),
        pit_lane=DataPITLane(_string(payload, "pit_lane")),
        sources=sources,
        cadence_seconds=_integer(payload, "cadence_seconds"),
        maximum_gap_seconds=_integer(payload, "maximum_gap_seconds"),
        freshness_max_age_seconds=_integer(payload, "freshness_max_age_seconds"),
        minimum_coverage_sources=_integer(payload, "minimum_coverage_sources"),
        maximum_fetches=_integer(payload, "maximum_fetches"),
        maximum_bytes=_integer(payload, "maximum_bytes"),
        schema_version=_string(payload, "schema_version"),
    )
    if plan.to_dict() != payload:
        raise ValueError("Retrieval Plan does not match canonical contract")
    return plan


def query_template_from_matcher_contract(
    value: object,
    *,
    template_ref: str,
    capability: ObservationCapability,
    pit_lane: DataPITLane,
) -> RegisteredQueryTemplate:
    payload = _object(value, "query template matcher contract")
    template = RegisteredQueryTemplate(
        template_ref=template_ref,
        capability=capability,
        pit_lane=pit_lane,
        allowed_match_field_paths=tuple(
            _string_value(item, "query template allowed_match_field_path")
            for item in _list(
                payload.get("allowed_match_field_paths"),
                "query template allowed_match_field_paths",
            )
        ),
        allowed_match_modes=tuple(
            MonitoringMatchMode(_string_value(item, "query template allowed_match_mode"))
            for item in _list(
                payload.get("allowed_match_modes"),
                "query template allowed_match_modes",
            )
        ),
        maximum_match_clauses=_integer(payload, "maximum_match_clauses"),
        maximum_terms_per_clause=_integer(payload, "maximum_terms_per_clause"),
        maximum_term_length=_integer(payload, "maximum_term_length"),
    )
    if template.matcher_contract_dict() != payload:
        raise ValueError("query template matcher contract does not match canonical contract")
    return template


def _snapshot_gaps(
    plan: RetrievalPlan, snapshot: DataSnapshot, requested_at: datetime
) -> tuple[RetrievalGapKind, ...]:
    gaps: set[RetrievalGapKind] = set()
    if snapshot.query.capability is not plan.capability:
        gaps.add(RetrievalGapKind.CAPABILITY_MISMATCH)
    if snapshot.query.pit_lane is not plan.pit_lane:
        gaps.add(RetrievalGapKind.PIT_LANE_MISMATCH)
    if snapshot.query.as_of > requested_at or snapshot.completed_at > requested_at:
        gaps.add(RetrievalGapKind.PIT_CUTOFF_EXCEEDED)
    if snapshot.query.source_policy_id != plan.collection_policy_id:
        gaps.add(RetrievalGapKind.COLLECTION_POLICY_MISMATCH)
    if snapshot.query.sources != plan.sources:
        gaps.add(RetrievalGapKind.SOURCE_SET_MISMATCH)
    if not snapshot.coverage_complete:
        gaps.add(RetrievalGapKind.COVERAGE_INCOMPLETE)
    completed_sources = sum(item.status.completed for item in snapshot.attempts)
    if completed_sources < plan.minimum_coverage_sources:
        gaps.add(RetrievalGapKind.COVERAGE_TOO_NARROW)
    if snapshot.completed_at < requested_at - timedelta(seconds=plan.freshness_max_age_seconds):
        gaps.add(RetrievalGapKind.STALE)
    return tuple(sorted(gaps, key=lambda item: item.value))


def _resolution(
    plan: RetrievalPlan,
    outcome: RetrievalOutcome,
    selected_snapshot_ids: tuple[str, ...],
    gaps: tuple[RetrievalGapKind, ...],
    barrier: RetrievalBarrier,
    requested_at: datetime,
) -> RetrievalResolution:
    core: dict[str, object] = {
        "schema_version": RETRIEVAL_RESOLUTION_SCHEMA,
        "plan_id": plan.plan_id,
        "outcome": outcome.value,
        "selected_snapshot_ids": list(selected_snapshot_ids),
        "gaps": [item.value for item in gaps],
        "barrier": barrier.value,
        "requested_at": _timestamp(requested_at),
    }
    return RetrievalResolution(
        resolution_id=f"retrieval-resolution-{canonical_hash(core)}",
        plan_id=plan.plan_id,
        outcome=outcome,
        selected_snapshot_ids=selected_snapshot_ids,
        gaps=gaps,
        barrier=barrier,
        requested_at=requested_at,
        schema_version=RETRIEVAL_RESOLUTION_SCHEMA,
    )


def _barrier_for(gaps: tuple[RetrievalGapKind, ...]) -> RetrievalBarrier:
    if {
        RetrievalGapKind.PIT_LANE_MISMATCH,
        RetrievalGapKind.PIT_CUTOFF_EXCEEDED,
    }.intersection(gaps):
        return RetrievalBarrier.PIT
    if {
        RetrievalGapKind.COVERAGE_INCOMPLETE,
        RetrievalGapKind.COVERAGE_TOO_NARROW,
    }.intersection(gaps):
        return RetrievalBarrier.COVERAGE
    if RetrievalGapKind.STALE in gaps:
        return RetrievalBarrier.FRESHNESS
    if RetrievalGapKind.FETCH_NOT_PERMITTED in gaps:
        return RetrievalBarrier.ACQUISITION
    if {
        RetrievalGapKind.FETCH_BUDGET_EXHAUSTED,
        RetrievalGapKind.BYTE_BUDGET_EXHAUSTED,
    }.intersection(gaps):
        return RetrievalBarrier.ACQUISITION
    return RetrievalBarrier.CACHE


def _fetch_budget_gaps(plan: RetrievalPlan) -> set[RetrievalGapKind]:
    gaps: set[RetrievalGapKind] = set()
    if plan.maximum_fetches < 1:
        gaps.add(RetrievalGapKind.FETCH_BUDGET_EXHAUSTED)
    if plan.maximum_bytes < 1:
        gaps.add(RetrievalGapKind.BYTE_BUDGET_EXHAUSTED)
    return gaps


def _default_matcher(
    subject: MonitoringSubjectRef, members: tuple[MonitoringSubjectRef, ...]
) -> ObservationMatcher:
    field_paths = {
        MonitoringSubjectKind.EVENT_CLUSTER: "event_cluster_ids",
        MonitoringSubjectKind.INDUSTRY: "industry_codes",
        MonitoringSubjectKind.ISSUER: "issuer_ids",
        MonitoringSubjectKind.INSTRUMENT: "instrument_ids",
        MonitoringSubjectKind.ETF: "etf_ids",
        MonitoringSubjectKind.INFORMATION_ASPECT: "information_aspects",
    }
    if subject.kind is MonitoringSubjectKind.FROZEN_SET:
        return ObservationMatcher(
            (
                ObservationMatchClause.build(
                    field_path="subject_refs",
                    mode=MonitoringMatchMode.CONTAINS_ANY,
                    terms=tuple(f"{item.kind.value}:{item.canonical_id}" for item in members),
                ),
            )
        )
    return ObservationMatcher(
        (
            ObservationMatchClause.build(
                field_path=field_paths[subject.kind],
                mode=MonitoringMatchMode.EXACT,
                terms=(subject.canonical_id,),
            ),
        )
    )


def _clause_matches(values: tuple[str, ...], clause: ObservationMatchClause) -> bool:
    if clause.mode is MonitoringMatchMode.EXACT:
        return bool(set(values).intersection(clause.terms))
    normalized_blob = "\n".join(values)
    if clause.mode is MonitoringMatchMode.CONTAINS_ALL:
        return all(term in normalized_blob for term in clause.terms)
    return any(term in normalized_blob for term in clause.terms)


def _field_values(payload: Mapping[str, object], field_path: str) -> tuple[str, ...]:
    value: object = payload
    for segment in field_path.split("."):
        if not isinstance(value, dict):
            return ()
        raw_mapping = cast(dict[object, object], value)
        if any(not isinstance(key, str) for key in raw_mapping):
            return ()
        value = cast(dict[str, object], raw_mapping).get(segment)
    if isinstance(value, str):
        return (_normalized_term(value),)
    if not isinstance(value, list):
        return ()
    result: list[str] = []
    for item in cast(list[object], value):
        if not isinstance(item, str):
            return ()
        result.append(_normalized_term(item))
    return tuple(result)


def _matcher_from_dict(value: Mapping[str, object]) -> ObservationMatcher:
    clauses = tuple(
        ObservationMatchClause(
            field_path=_string(_object(item, "Monitoring matcher clause"), "field_path"),
            mode=MonitoringMatchMode(_string(_object(item, "Monitoring matcher clause"), "mode")),
            terms=tuple(
                _string_value(term, "Monitoring matcher term")
                for term in _list(
                    _object(item, "Monitoring matcher clause").get("terms"),
                    "Monitoring matcher terms",
                )
            ),
        )
        for item in _list(value.get("clauses"), "Monitoring matcher clauses")
    )
    return ObservationMatcher(clauses)


def _subject_from_dict(value: Mapping[str, object]) -> MonitoringSubjectRef:
    return MonitoringSubjectRef(
        kind=MonitoringSubjectKind(_string(value, "kind")),
        canonical_id=_string(value, "canonical_id"),
    )


def _subject_key(subject: MonitoringSubjectRef) -> tuple[str, str]:
    return (subject.kind.value, subject.canonical_id)


def _canonical_ref(value: str, name: str) -> None:
    if not _CANONICAL_REF.fullmatch(value) or "://" in value:
        raise ValueError(f"{name} must be a canonical non-URL reference")


def _registered_ref(value: str, name: str, prefix: str) -> None:
    if not value.startswith(prefix) or not _REGISTERED_REF.fullmatch(value):
        raise ValueError(f"{name} must be a registered reference")


def _normalized_term(value: str) -> str:
    result = value.strip().casefold()
    if not result or len(result) > 256 or "\x00" in result:
        raise ValueError("monitoring matcher terms must be bounded non-empty text")
    return result


def _strict_utc(value: datetime, name: str) -> None:
    require_aware(value, name)
    if value.utcoffset() != timedelta(0) or value.tzinfo is not UTC:
        raise ValueError(f"{name} must use the UTC singleton")


def _timestamp(value: datetime) -> str:
    _strict_utc(value, "timestamp")
    return value.isoformat().replace("+00:00", "Z")


def _datetime(value: str, name: str) -> datetime:
    if not value.endswith("Z"):
        raise ValueError(f"{name} must be a UTC timestamp")
    try:
        parsed = datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError as exc:
        raise ValueError(f"{name} must be an ISO 8601 timestamp") from exc
    return parsed.astimezone(UTC)


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
    return _string_value(value.get(key), key)


def _string_value(value: object, name: str) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{name} must be a string")
    return value


def _optional_string(value: object) -> str | None:
    if value is None:
        return None
    return _string_value(value, "optional string")


def _integer(value: Mapping[str, object], key: str) -> int:
    raw = value.get(key)
    if isinstance(raw, bool) or not isinstance(raw, int):
        raise TypeError(f"{key} must be an integer")
    return raw


def _boolean(value: Mapping[str, object], key: str) -> bool:
    raw = value.get(key)
    if not isinstance(raw, bool):
        raise TypeError(f"{key} must be a boolean")
    return raw
