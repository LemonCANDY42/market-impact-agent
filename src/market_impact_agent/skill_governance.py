from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from typing import cast

from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.agent_runtime import SkillManifest
from market_impact_agent.domain import require_aware
from market_impact_agent.method_skills import MethodSkillCatalog
from market_impact_agent.research_methods import ResearchMethodCatalog

SKILL_RESEARCH_STUDY_SCHEMA = "market-impact.skill-research-study.v1"
SKILL_CANDIDATE_GROUP_SCHEMA = "market-impact.skill-candidate-group.v1"
SKILL_GOVERNANCE_REVIEW_SCHEMA = "market-impact.skill-governance-review.v1"


class SkillGroup(StrEnum):
    EVIDENCE_AUTHORITY = "evidence_authority"
    DISCOVERY_TRIAGE = "discovery_triage"
    EVENT_TRANSMISSION = "event_transmission"
    MACRO_REGIME = "macro_regime"
    INDUSTRY_SECTOR = "industry_sector"
    ISSUER_FUNDAMENTAL = "issuer_fundamental"
    PORTFOLIO_RISK = "portfolio_risk"
    MARKET_MICROSTRUCTURE = "market_microstructure"


class SkillChangeKind(StrEnum):
    NEW_METHOD = "new_method"
    STRENGTHENS_INVARIANT = "strengthens_invariant"
    CLARIFIES_INVARIANT = "clarifies_invariant"


class ValidationRole(StrEnum):
    DISCOVERY = "discovery"
    INDEPENDENT_VALIDATION = "independent_validation"


class CounterexampleSeverity(StrEnum):
    MINOR = "minor"
    MATERIAL = "material"


class CounterexampleDisposition(StrEnum):
    NARROWS_SCOPE = "narrows_scope"
    ENCODED_EXCEPTION = "encoded_exception"
    REFUTES_CONCLUSION = "refutes_conclusion"
    UNRESOLVED = "unresolved"


class CatalogSubjectKind(StrEnum):
    ACTIVE_SKILL = "active_skill"
    OPEN_CANDIDATE = "open_candidate"


class CatalogRelationship(StrEnum):
    DUPLICATE = "duplicate"
    CANDIDATE_SUBSUMED_BY_SUBJECT = "candidate_subsumed_by_subject"
    CANDIDATE_EXTENDS_SUBJECT = "candidate_extends_subject"
    CANDIDATE_SPECIALIZES_SUBJECT = "candidate_specializes_subject"
    CONFLICTS_WITH_SUBJECT = "conflicts_with_subject"
    ORTHOGONAL = "orthogonal"


class CatalogResolution(StrEnum):
    REJECT_DUPLICATE = "reject_duplicate"
    KEEP_SUBJECT = "keep_subject"
    REPLACE_SUBJECT = "replace_subject"
    MERGE_AS_NEW_VERSION = "merge_as_new_version"
    COEXIST_SCOPED = "coexist_scoped"
    NARROW_TO_EXCEPTION = "narrow_to_exception"
    REJECT_CANDIDATE = "reject_candidate"
    REVISE_BOTH = "revise_both"
    NO_ACTION = "no_action"


DEFAULT_SKILL_GROUP_ASSIGNMENTS_V1: Mapping[str, SkillGroup] = {
    "adversarial-risk": SkillGroup.PORTFOLIO_RISK,
    "energy-supply": SkillGroup.INDUSTRY_SECTOR,
    "equity-exposure": SkillGroup.ISSUER_FUNDAMENTAL,
    "event-market-context": SkillGroup.EVENT_TRANSMISSION,
    "evidence-core": SkillGroup.EVIDENCE_AUTHORITY,
    "expectations-base-rates": SkillGroup.EVENT_TRANSMISSION,
    "narrative-diffusion-assessment": SkillGroup.EVENT_TRANSMISSION,
    "news-evidence-assessment": SkillGroup.DISCOVERY_TRIAGE,
    "owner-value-discipline": SkillGroup.ISSUER_FUNDAMENTAL,
    "pattern-review": SkillGroup.DISCOVERY_TRIAGE,
    "reflexive-feedback-check": SkillGroup.EVENT_TRANSMISSION,
    "research-discipline": SkillGroup.EVIDENCE_AUTHORITY,
    "second-level-cycle-context": SkillGroup.MACRO_REGIME,
}


@dataclass(frozen=True, slots=True)
class SkillResearchWorkUnit:
    unit_id: str
    event_family: str
    time_block: str
    instrument_scope: tuple[str, ...]
    source_artifact_hashes: tuple[str, ...]

    def __post_init__(self) -> None:
        _identifier(self.unit_id, "research work unit_id")
        _nonempty(self.event_family, "research event_family")
        _nonempty(self.time_block, "research time_block")
        _unique_nonempty(self.instrument_scope, "research instrument_scope")
        _unique_hashes(self.source_artifact_hashes, "research source artifacts", nonempty=True)

    @property
    def independence_key(self) -> str:
        return canonical_hash(self.independence_dict())

    def independence_dict(self) -> dict[str, object]:
        """Return the conservative case/time-family identity used for independence."""
        return {
            "event_family": self.event_family,
            "time_block": self.time_block,
        }

    def to_dict(self) -> dict[str, object]:
        return {
            "unit_id": self.unit_id,
            "event_family": self.event_family,
            "time_block": self.time_block,
            "instrument_scope": list(self.instrument_scope),
            "source_artifact_hashes": list(self.source_artifact_hashes),
        }


@dataclass(frozen=True, slots=True)
class SkillValidationMetricPolicy:
    metric_id: str
    maximum_divergence: Decimal

    def __post_init__(self) -> None:
        _identifier(self.metric_id, "Skill validation metric policy metric_id")
        if not self.maximum_divergence.is_finite() or self.maximum_divergence < 0:
            raise ValueError(
                "Skill validation metric policy maximum_divergence must be finite and non-negative"
            )

    def to_dict(self) -> dict[str, object]:
        return {
            "metric_id": self.metric_id,
            "maximum_divergence": str(self.maximum_divergence),
        }


@dataclass(frozen=True, slots=True)
class SkillResearchStudy:
    study_id: str
    registered_at: datetime
    title: str
    corpus_hashes: tuple[str, ...]
    work_units: tuple[SkillResearchWorkUnit, ...]
    validation_metric_policies: tuple[SkillValidationMetricPolicy, ...]
    specialist_roles: tuple[str, ...]
    provider_profile_hashes: tuple[str, ...]
    skill_surface_hashes: tuple[str, ...]
    maximum_model_cost_microusd: int
    excluded_uses: tuple[str, ...]

    def __post_init__(self) -> None:
        require_aware(self.registered_at, "Skill Research Study registered_at")
        _nonempty(self.title, "Skill Research Study title")
        _unique_hashes(self.corpus_hashes, "Skill Research Study corpus hashes", nonempty=True)
        if not self.work_units:
            raise ValueError("Skill Research Study requires at least one work unit")
        _unique(tuple(item.unit_id for item in self.work_units), "research work unit IDs")
        _unique(
            tuple(item.independence_key for item in self.work_units),
            "research work unit independence keys",
        )
        if not self.validation_metric_policies:
            raise ValueError("Skill Research Study requires validation metric policies")
        _unique(
            tuple(item.metric_id for item in self.validation_metric_policies),
            "Skill Research Study validation metric IDs",
        )
        _unique_nonempty(self.specialist_roles, "Skill Research Study specialist roles")
        _unique_hashes(
            self.provider_profile_hashes,
            "Skill Research Study Provider Profile hashes",
            nonempty=True,
        )
        _unique_hashes(
            self.skill_surface_hashes,
            "Skill Research Study Skill surface hashes",
            nonempty=True,
        )
        if self.maximum_model_cost_microusd < 0:
            raise ValueError("Skill Research Study cost limit cannot be negative")
        _unique_nonempty(self.excluded_uses, "Skill Research Study excluded uses")
        if self.study_id != self.expected_study_id:
            raise ValueError("Skill Research Study study_id does not match content")

    @property
    def expected_study_id(self) -> str:
        return f"skill-research-study-{canonical_hash(self.core_dict())}"

    def core_dict(self) -> dict[str, object]:
        return {
            "schema_version": SKILL_RESEARCH_STUDY_SCHEMA,
            "registered_at": _timestamp(self.registered_at),
            "title": self.title,
            "information_scope": "registered_outcome_opened_full_information",
            "independent_unit_policy": "event_case_or_distinct_time_family_block_not_agent_count",
            "multi_expert_authority": "decomposition_only",
            "inference_eligible": False,
            "strict_pit_eligible": False,
            "execution_capability": "none",
            "corpus_hashes": list(self.corpus_hashes),
            "work_units": [item.to_dict() for item in self.work_units],
            "validation_metric_policies": [
                item.to_dict() for item in self.validation_metric_policies
            ],
            "specialist_roles": list(self.specialist_roles),
            "provider_profile_hashes": list(self.provider_profile_hashes),
            "skill_surface_hashes": list(self.skill_surface_hashes),
            "maximum_model_cost_microusd": self.maximum_model_cost_microusd,
            "excluded_uses": list(self.excluded_uses),
        }

    def to_dict(self) -> dict[str, object]:
        return {**self.core_dict(), "study_id": self.study_id}

    @classmethod
    def build(
        cls,
        *,
        registered_at: datetime,
        title: str,
        corpus_hashes: tuple[str, ...],
        work_units: tuple[SkillResearchWorkUnit, ...],
        validation_metric_policies: tuple[SkillValidationMetricPolicy, ...],
        specialist_roles: tuple[str, ...],
        provider_profile_hashes: tuple[str, ...],
        skill_surface_hashes: tuple[str, ...],
        maximum_model_cost_microusd: int,
        excluded_uses: tuple[str, ...],
    ) -> SkillResearchStudy:
        core = {
            "schema_version": SKILL_RESEARCH_STUDY_SCHEMA,
            "registered_at": _timestamp(registered_at),
            "title": title,
            "information_scope": "registered_outcome_opened_full_information",
            "independent_unit_policy": "event_case_or_distinct_time_family_block_not_agent_count",
            "multi_expert_authority": "decomposition_only",
            "inference_eligible": False,
            "strict_pit_eligible": False,
            "execution_capability": "none",
            "corpus_hashes": list(corpus_hashes),
            "work_units": [item.to_dict() for item in work_units],
            "validation_metric_policies": [item.to_dict() for item in validation_metric_policies],
            "specialist_roles": list(specialist_roles),
            "provider_profile_hashes": list(provider_profile_hashes),
            "skill_surface_hashes": list(skill_surface_hashes),
            "maximum_model_cost_microusd": maximum_model_cost_microusd,
            "excluded_uses": list(excluded_uses),
        }
        return cls(
            study_id=f"skill-research-study-{canonical_hash(core)}",
            registered_at=registered_at,
            title=title,
            corpus_hashes=corpus_hashes,
            work_units=work_units,
            validation_metric_policies=validation_metric_policies,
            specialist_roles=specialist_roles,
            provider_profile_hashes=provider_profile_hashes,
            skill_surface_hashes=skill_surface_hashes,
            maximum_model_cost_microusd=maximum_model_cost_microusd,
            excluded_uses=excluded_uses,
        )


@dataclass(frozen=True, slots=True)
class SkillConclusion:
    proposed_name: str
    proposed_version: str
    group: SkillGroup
    change_kind: SkillChangeKind
    proposition: str
    mechanism_steps: tuple[str, ...]
    applicability_conditions: tuple[str, ...]
    exceptions: tuple[str, ...]
    required_evidence: tuple[str, ...]
    prohibited_uses: tuple[str, ...]

    def __post_init__(self) -> None:
        _identifier(self.proposed_name, "proposed Skill name")
        if re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+", self.proposed_version) is None:
            raise ValueError("proposed Skill version must be an exact semantic version")
        _nonempty(self.proposition, "Skill conclusion proposition")
        _unique_nonempty(self.mechanism_steps, "Skill conclusion mechanism steps")
        _unique_nonempty(
            self.applicability_conditions,
            "Skill conclusion applicability conditions",
        )
        _unique(self.exceptions, "Skill conclusion exceptions")
        _unique_nonempty(self.required_evidence, "Skill conclusion required evidence")
        _unique_nonempty(self.prohibited_uses, "Skill conclusion prohibited uses")

    def to_dict(self) -> dict[str, object]:
        return {
            "proposed_name": self.proposed_name,
            "proposed_version": self.proposed_version,
            "group": self.group.value,
            "change_kind": self.change_kind.value,
            "proposition": self.proposition,
            "mechanism_steps": list(self.mechanism_steps),
            "applicability_conditions": list(self.applicability_conditions),
            "exceptions": list(self.exceptions),
            "required_evidence": list(self.required_evidence),
            "prohibited_uses": list(self.prohibited_uses),
        }


@dataclass(frozen=True, slots=True)
class SkillValidationBlock:
    validation_id: str
    role: ValidationRole
    work_unit: SkillResearchWorkUnit
    metric_id: str
    observed_divergence: Decimal
    supports_conclusion: bool
    evidence_hashes: tuple[str, ...]
    counterexample_search_hash: str
    specialist_artifact_hashes: tuple[str, ...]

    def __post_init__(self) -> None:
        _identifier(self.metric_id, "Skill validation metric_id")
        if not self.observed_divergence.is_finite() or self.observed_divergence < 0:
            raise ValueError("Skill validation observed_divergence must be finite and non-negative")
        _unique_hashes(self.evidence_hashes, "Skill validation evidence", nonempty=True)
        _sha256(self.counterexample_search_hash, "Skill validation counterexample search hash")
        _unique_hashes(
            self.specialist_artifact_hashes,
            "Skill validation specialist artifacts",
            nonempty=True,
        )
        if self.validation_id != self.expected_validation_id:
            raise ValueError("Skill validation_id does not match content")

    @property
    def expected_validation_id(self) -> str:
        return f"skill-validation-{canonical_hash(self.core_dict())}"

    def core_dict(self) -> dict[str, object]:
        return {
            "role": self.role.value,
            "work_unit": self.work_unit.to_dict(),
            "metric_id": self.metric_id,
            "observed_divergence": str(self.observed_divergence),
            "supports_conclusion": self.supports_conclusion,
            "evidence_hashes": list(self.evidence_hashes),
            "counterexample_search_hash": self.counterexample_search_hash,
            "specialist_artifact_hashes": list(self.specialist_artifact_hashes),
        }

    def to_dict(self) -> dict[str, object]:
        return {**self.core_dict(), "validation_id": self.validation_id}

    @classmethod
    def build(
        cls,
        *,
        role: ValidationRole,
        work_unit: SkillResearchWorkUnit,
        metric_id: str,
        observed_divergence: Decimal,
        supports_conclusion: bool,
        evidence_hashes: tuple[str, ...],
        counterexample_search_hash: str,
        specialist_artifact_hashes: tuple[str, ...],
    ) -> SkillValidationBlock:
        core = {
            "role": role.value,
            "work_unit": work_unit.to_dict(),
            "metric_id": metric_id,
            "observed_divergence": str(observed_divergence),
            "supports_conclusion": supports_conclusion,
            "evidence_hashes": list(evidence_hashes),
            "counterexample_search_hash": counterexample_search_hash,
            "specialist_artifact_hashes": list(specialist_artifact_hashes),
        }
        return cls(
            validation_id=f"skill-validation-{canonical_hash(core)}",
            role=role,
            work_unit=work_unit,
            metric_id=metric_id,
            observed_divergence=observed_divergence,
            supports_conclusion=supports_conclusion,
            evidence_hashes=evidence_hashes,
            counterexample_search_hash=counterexample_search_hash,
            specialist_artifact_hashes=specialist_artifact_hashes,
        )


@dataclass(frozen=True, slots=True)
class CounterexampleAssessment:
    counterexample_id: str
    severity: CounterexampleSeverity
    disposition: CounterexampleDisposition
    summary: str
    evidence_hashes: tuple[str, ...]
    scope_effect: str

    def __post_init__(self) -> None:
        _identifier(self.counterexample_id, "counterexample_id")
        _nonempty(self.summary, "counterexample summary")
        _unique_hashes(self.evidence_hashes, "counterexample evidence", nonempty=True)
        _nonempty(self.scope_effect, "counterexample scope_effect")

    def to_dict(self) -> dict[str, object]:
        return {
            "counterexample_id": self.counterexample_id,
            "severity": self.severity.value,
            "disposition": self.disposition.value,
            "summary": self.summary,
            "evidence_hashes": list(self.evidence_hashes),
            "scope_effect": self.scope_effect,
        }


@dataclass(frozen=True, slots=True)
class SkillCatalogSubject:
    subject_ref: str
    kind: CatalogSubjectKind
    name: str
    group: SkillGroup
    content_hash: str

    def __post_init__(self) -> None:
        _identifier(self.subject_ref, "Skill catalog subject_ref")
        _identifier(self.name, "Skill catalog subject name")
        _sha256(self.content_hash, "Skill catalog subject content_hash")

    def to_dict(self) -> dict[str, object]:
        return {
            "subject_ref": self.subject_ref,
            "kind": self.kind.value,
            "name": self.name,
            "group": self.group.value,
            "content_hash": self.content_hash,
        }


@dataclass(frozen=True, slots=True)
class SkillBaselineSnapshot:
    snapshot_id: str
    subjects: tuple[SkillCatalogSubject, ...]

    def __post_init__(self) -> None:
        if not self.subjects:
            raise ValueError("Skill baseline snapshot cannot be empty")
        _unique(tuple(item.subject_ref for item in self.subjects), "Skill baseline subject refs")
        if self.snapshot_id != self.expected_snapshot_id:
            raise ValueError("Skill baseline snapshot_id does not match content")

    @property
    def expected_snapshot_id(self) -> str:
        return f"skill-baseline-{canonical_hash(self.core_dict())}"

    def core_dict(self) -> dict[str, object]:
        return {"subjects": [item.to_dict() for item in self.subjects]}

    def to_dict(self) -> dict[str, object]:
        return {**self.core_dict(), "snapshot_id": self.snapshot_id}

    @classmethod
    def build(cls, subjects: tuple[SkillCatalogSubject, ...]) -> SkillBaselineSnapshot:
        core = {"subjects": [item.to_dict() for item in subjects]}
        return cls(
            snapshot_id=f"skill-baseline-{canonical_hash(core)}",
            subjects=subjects,
        )


_ALLOWED_RESOLUTIONS: dict[CatalogRelationship, frozenset[CatalogResolution]] = {
    CatalogRelationship.DUPLICATE: frozenset({CatalogResolution.REJECT_DUPLICATE}),
    CatalogRelationship.CANDIDATE_SUBSUMED_BY_SUBJECT: frozenset(
        {
            CatalogResolution.KEEP_SUBJECT,
            CatalogResolution.NARROW_TO_EXCEPTION,
            CatalogResolution.REJECT_CANDIDATE,
        }
    ),
    CatalogRelationship.CANDIDATE_EXTENDS_SUBJECT: frozenset(
        {CatalogResolution.MERGE_AS_NEW_VERSION, CatalogResolution.COEXIST_SCOPED}
    ),
    CatalogRelationship.CANDIDATE_SPECIALIZES_SUBJECT: frozenset(
        {CatalogResolution.COEXIST_SCOPED, CatalogResolution.NARROW_TO_EXCEPTION}
    ),
    CatalogRelationship.CONFLICTS_WITH_SUBJECT: frozenset(
        {
            CatalogResolution.REPLACE_SUBJECT,
            CatalogResolution.MERGE_AS_NEW_VERSION,
            CatalogResolution.COEXIST_SCOPED,
            CatalogResolution.REJECT_CANDIDATE,
            CatalogResolution.REVISE_BOTH,
        }
    ),
    CatalogRelationship.ORTHOGONAL: frozenset({CatalogResolution.NO_ACTION}),
}


@dataclass(frozen=True, slots=True)
class SkillCatalogComparison:
    subject_ref: str
    relationship: CatalogRelationship
    resolution: CatalogResolution
    rationale: str
    evidence_hashes: tuple[str, ...]

    def __post_init__(self) -> None:
        _identifier(self.subject_ref, "Skill comparison subject_ref")
        _nonempty(self.rationale, "Skill comparison rationale")
        _unique_hashes(self.evidence_hashes, "Skill comparison evidence", nonempty=True)
        if self.resolution not in _ALLOWED_RESOLUTIONS[self.relationship]:
            raise ValueError("Skill comparison relationship and resolution are inconsistent")

    def to_dict(self) -> dict[str, object]:
        return {
            "subject_ref": self.subject_ref,
            "relationship": self.relationship.value,
            "resolution": self.resolution.value,
            "rationale": self.rationale,
            "evidence_hashes": list(self.evidence_hashes),
        }


@dataclass(frozen=True, slots=True)
class SkillCandidateGroup:
    candidate_group_id: str
    study_id: str
    study_hash: str
    conclusion: SkillConclusion
    validation_blocks: tuple[SkillValidationBlock, ...]
    counterexamples: tuple[CounterexampleAssessment, ...]

    def __post_init__(self) -> None:
        _prefixed_hash(self.study_id, "skill-research-study-", "Skill Candidate study_id")
        _sha256(self.study_hash, "Skill Candidate study_hash")
        if not self.validation_blocks:
            raise ValueError("Skill Candidate Group requires validation blocks")
        _unique(
            tuple(item.validation_id for item in self.validation_blocks),
            "Skill Candidate validation IDs",
        )
        _unique(
            tuple(item.work_unit.independence_key for item in self.validation_blocks),
            "Skill Candidate independent work units",
        )
        _unique(
            tuple(item.counterexample_id for item in self.counterexamples),
            "Skill Candidate counterexample IDs",
        )
        if self.candidate_group_id != self.expected_candidate_group_id:
            raise ValueError("Skill Candidate Group ID does not match content")

    @property
    def expected_candidate_group_id(self) -> str:
        return f"skill-candidate-group-{canonical_hash(self.core_dict())}"

    def core_dict(self) -> dict[str, object]:
        return {
            "schema_version": SKILL_CANDIDATE_GROUP_SCHEMA,
            "study_id": self.study_id,
            "study_hash": self.study_hash,
            "conclusion": self.conclusion.to_dict(),
            "validation_blocks": [item.to_dict() for item in self.validation_blocks],
            "counterexamples": [item.to_dict() for item in self.counterexamples],
            "candidate_status": "non_executable_pending_governance",
            "active_skill": False,
            "inference_eligible": False,
            "strict_pit_eligible": False,
            "execution_capability": "none",
        }

    def to_dict(self) -> dict[str, object]:
        return {**self.core_dict(), "candidate_group_id": self.candidate_group_id}

    @classmethod
    def build(
        cls,
        *,
        study: SkillResearchStudy,
        conclusion: SkillConclusion,
        validation_blocks: tuple[SkillValidationBlock, ...],
        counterexamples: tuple[CounterexampleAssessment, ...],
    ) -> SkillCandidateGroup:
        core = {
            "schema_version": SKILL_CANDIDATE_GROUP_SCHEMA,
            "study_id": study.study_id,
            "study_hash": canonical_hash(study.to_dict()),
            "conclusion": conclusion.to_dict(),
            "validation_blocks": [item.to_dict() for item in validation_blocks],
            "counterexamples": [item.to_dict() for item in counterexamples],
            "candidate_status": "non_executable_pending_governance",
            "active_skill": False,
            "inference_eligible": False,
            "strict_pit_eligible": False,
            "execution_capability": "none",
        }
        return cls(
            candidate_group_id=f"skill-candidate-group-{canonical_hash(core)}",
            study_id=study.study_id,
            study_hash=canonical_hash(study.to_dict()),
            conclusion=conclusion,
            validation_blocks=validation_blocks,
            counterexamples=counterexamples,
        )

    def validate_against(self, study: SkillResearchStudy) -> None:
        if self.study_id != study.study_id or self.study_hash != canonical_hash(study.to_dict()):
            raise ValueError("Skill Candidate Group does not bind the Skill Research Study")
        registered_units = {item.independence_key for item in study.work_units}
        candidate_units = {item.work_unit.independence_key for item in self.validation_blocks}
        if not candidate_units <= registered_units:
            raise ValueError("Skill Candidate Group uses an unregistered research work unit")
        registered_metrics = {item.metric_id for item in study.validation_metric_policies}
        candidate_metrics = {item.metric_id for item in self.validation_blocks}
        if not candidate_metrics <= registered_metrics:
            raise ValueError("Skill Candidate Group uses an unregistered validation metric")


@dataclass(frozen=True, slots=True)
class SkillGovernanceReview:
    review_id: str
    evaluated_at: datetime
    candidate_group_id: str
    candidate_group_hash: str
    baseline_snapshot: SkillBaselineSnapshot
    comparisons: tuple[SkillCatalogComparison, ...]
    admitted_as_candidate: bool
    blockers: tuple[str, ...]
    candidate_id: str | None

    def __post_init__(self) -> None:
        require_aware(self.evaluated_at, "Skill Governance Review evaluated_at")
        _prefixed_hash(
            self.candidate_group_id,
            "skill-candidate-group-",
            "Skill Governance candidate_group_id",
        )
        _sha256(self.candidate_group_hash, "Skill Governance candidate_group_hash")
        _unique(tuple(item.subject_ref for item in self.comparisons), "Skill comparison refs")
        if {item.subject_ref for item in self.comparisons} != {
            item.subject_ref for item in self.baseline_snapshot.subjects
        }:
            raise ValueError("Skill Governance Review must compare every baseline subject once")
        _unique(self.blockers, "Skill Governance blockers")
        if self.admitted_as_candidate:
            if self.blockers or self.candidate_id is None:
                raise ValueError("admitted Skill candidate cannot contain blockers")
            _prefixed_hash(self.candidate_id, "skill-candidate-", "Skill candidate_id")
        elif self.candidate_id is not None or not self.blockers:
            raise ValueError("rejected Skill candidate requires blockers and no candidate_id")
        if self.review_id != self.expected_review_id:
            raise ValueError("Skill Governance review_id does not match content")

    @property
    def expected_review_id(self) -> str:
        return f"skill-governance-review-{canonical_hash(self.core_dict())}"

    def core_dict(self) -> dict[str, object]:
        return {
            "schema_version": SKILL_GOVERNANCE_REVIEW_SCHEMA,
            "evaluated_at": _timestamp(self.evaluated_at),
            "candidate_group_id": self.candidate_group_id,
            "candidate_group_hash": self.candidate_group_hash,
            "baseline_snapshot": self.baseline_snapshot.to_dict(),
            "comparisons": [item.to_dict() for item in self.comparisons],
            "admitted_as_candidate": self.admitted_as_candidate,
            "blockers": list(self.blockers),
            "candidate_id": self.candidate_id,
            "active_skill": False,
            "catalog_mutation": False,
            "execution_capability": "none",
        }

    def to_dict(self) -> dict[str, object]:
        return {**self.core_dict(), "review_id": self.review_id}


def evaluate_skill_candidate(
    *,
    candidate_group: SkillCandidateGroup,
    study: SkillResearchStudy,
    baseline_snapshot: SkillBaselineSnapshot,
    comparisons: tuple[SkillCatalogComparison, ...],
    evaluated_at: datetime,
) -> SkillGovernanceReview:
    candidate_group.validate_against(study)
    if {item.subject_ref for item in comparisons} != {
        item.subject_ref for item in baseline_snapshot.subjects
    } or len(comparisons) != len(baseline_snapshot.subjects):
        raise ValueError("Skill Governance comparison coverage is incomplete")
    blockers: list[str] = []
    discovery = [
        item for item in candidate_group.validation_blocks if item.role is ValidationRole.DISCOVERY
    ]
    validation = [
        item
        for item in candidate_group.validation_blocks
        if item.role is ValidationRole.INDEPENDENT_VALIDATION
    ]
    if len(discovery) != 1:
        blockers.append("requires_exactly_one_discovery_block")
    if len(validation) < 2:
        blockers.append("requires_at_least_two_additional_independent_validation_blocks")
    maximum_divergence_by_metric = {
        item.metric_id: item.maximum_divergence for item in study.validation_metric_policies
    }
    if any(
        not item.supports_conclusion
        or item.observed_divergence > maximum_divergence_by_metric[item.metric_id]
        for item in validation
    ):
        blockers.append("independent_validation_failed_or_exceeded_divergence")
    if any(
        item.disposition is CounterexampleDisposition.REFUTES_CONCLUSION
        or (
            item.severity is CounterexampleSeverity.MATERIAL
            and item.disposition is CounterexampleDisposition.UNRESOLVED
        )
        for item in candidate_group.counterexamples
    ):
        blockers.append("unresolved_material_or_refuting_counterexample")
    conclusion = candidate_group.conclusion
    if conclusion.group is SkillGroup.EVIDENCE_AUTHORITY:
        if conclusion.change_kind not in {
            SkillChangeKind.STRENGTHENS_INVARIANT,
            SkillChangeKind.CLARIFIES_INVARIANT,
        }:
            blockers.append("outcome_opened_research_cannot_weaken_or_invent_evidence_authority")
    elif conclusion.change_kind is not SkillChangeKind.NEW_METHOD:
        blockers.append("research_method_group_requires_new_method_change_kind")
    blocking_resolutions = {
        CatalogResolution.REJECT_DUPLICATE,
        CatalogResolution.KEEP_SUBJECT,
        CatalogResolution.NARROW_TO_EXCEPTION,
        CatalogResolution.REJECT_CANDIDATE,
        CatalogResolution.REVISE_BOTH,
    }
    if any(item.resolution in blocking_resolutions for item in comparisons):
        blockers.append("catalog_review_requires_rejection_or_revision")
    ordered_blockers = tuple(dict.fromkeys(blockers))
    candidate_hash = canonical_hash(candidate_group.to_dict())
    candidate_core = {
        "candidate_group_id": candidate_group.candidate_group_id,
        "candidate_group_hash": candidate_hash,
        "baseline_snapshot_id": baseline_snapshot.snapshot_id,
    }
    candidate_id = None if ordered_blockers else f"skill-candidate-{canonical_hash(candidate_core)}"
    core = {
        "schema_version": SKILL_GOVERNANCE_REVIEW_SCHEMA,
        "evaluated_at": _timestamp(evaluated_at),
        "candidate_group_id": candidate_group.candidate_group_id,
        "candidate_group_hash": candidate_hash,
        "baseline_snapshot": baseline_snapshot.to_dict(),
        "comparisons": [item.to_dict() for item in comparisons],
        "admitted_as_candidate": not ordered_blockers,
        "blockers": list(ordered_blockers),
        "candidate_id": candidate_id,
        "active_skill": False,
        "catalog_mutation": False,
        "execution_capability": "none",
    }
    return SkillGovernanceReview(
        review_id=f"skill-governance-review-{canonical_hash(core)}",
        evaluated_at=evaluated_at,
        candidate_group_id=candidate_group.candidate_group_id,
        candidate_group_hash=candidate_hash,
        baseline_snapshot=baseline_snapshot,
        comparisons=comparisons,
        admitted_as_candidate=not ordered_blockers,
        blockers=ordered_blockers,
        candidate_id=candidate_id,
    )


def build_complete_skill_baseline_snapshot(
    *,
    runtime_manifests: tuple[SkillManifest, ...],
    research_method_catalog: ResearchMethodCatalog,
    method_skill_catalog: MethodSkillCatalog,
    group_assignments: Mapping[str, SkillGroup],
    open_candidates: tuple[tuple[SkillCandidateGroup, SkillGovernanceReview], ...] = (),
) -> SkillBaselineSnapshot:
    manifests_by_name = {item.name: item for item in runtime_manifests}
    if len(manifests_by_name) != len(runtime_manifests):
        raise ValueError("Skill baseline runtime manifests contain duplicate names")
    runtime_names = set(manifests_by_name)
    if set(group_assignments) != runtime_names:
        raise ValueError("Skill baseline group assignments must cover every runtime Skill exactly")
    research_by_name = {item.skill_name: item for item in research_method_catalog.methods}
    method_by_name = {item.skill_name: item for item in method_skill_catalog.methods}
    catalog_names = set(research_by_name) | set(method_by_name)
    missing_runtime = catalog_names - runtime_names
    if missing_runtime:
        raise ValueError("Skill baseline catalog references a missing runtime Skill")
    subjects: list[SkillCatalogSubject] = []
    for name in sorted(runtime_names):
        manifest = manifests_by_name[name]
        content = {
            "manifest_hash": manifest.manifest_hash,
            "instructions_hash": manifest.instructions_hash,
            "research_method": (
                None if name not in research_by_name else research_by_name[name].to_dict()
            ),
            "method_skill": None if name not in method_by_name else method_by_name[name].to_dict(),
        }
        subjects.append(
            SkillCatalogSubject(
                subject_ref=f"active.{name}",
                kind=CatalogSubjectKind.ACTIVE_SKILL,
                name=name,
                group=group_assignments[name],
                content_hash=canonical_hash(content),
            )
        )
    candidate_names: set[str] = set()
    for candidate_group, review in open_candidates:
        if not review.admitted_as_candidate or review.candidate_id is None:
            raise ValueError("Skill baseline accepts only admitted open candidates")
        if (
            review.candidate_group_id != candidate_group.candidate_group_id
            or review.candidate_group_hash != canonical_hash(candidate_group.to_dict())
        ):
            raise ValueError("Skill baseline candidate and Governance Review do not match")
        name = candidate_group.conclusion.proposed_name
        if name in candidate_names:
            raise ValueError("Skill baseline contains duplicate open candidate names")
        candidate_names.add(name)
        subjects.append(
            SkillCatalogSubject(
                subject_ref=f"candidate.{review.candidate_id.removeprefix('skill-candidate-')}",
                kind=CatalogSubjectKind.OPEN_CANDIDATE,
                name=name,
                group=candidate_group.conclusion.group,
                content_hash=canonical_hash(
                    {
                        "candidate_group": candidate_group.to_dict(),
                        "governance_review": review.to_dict(),
                    }
                ),
            )
        )
    return SkillBaselineSnapshot.build(tuple(subjects))


def skill_research_study_from_dict(value: object) -> SkillResearchStudy:
    payload = _object(value, "Skill Research Study")
    if payload.get("schema_version") != SKILL_RESEARCH_STUDY_SCHEMA:
        raise ValueError("unsupported Skill Research Study schema_version")
    study = SkillResearchStudy(
        study_id=_string(payload, "study_id"),
        registered_at=_datetime(payload, "registered_at"),
        title=_string(payload, "title"),
        corpus_hashes=_string_tuple(payload, "corpus_hashes"),
        work_units=tuple(
            _work_unit(item) for item in _object_list(payload.get("work_units"), "work_units")
        ),
        validation_metric_policies=tuple(
            _validation_metric_policy(item)
            for item in _object_list(
                payload.get("validation_metric_policies"),
                "validation_metric_policies",
            )
        ),
        specialist_roles=_string_tuple(payload, "specialist_roles"),
        provider_profile_hashes=_string_tuple(payload, "provider_profile_hashes"),
        skill_surface_hashes=_string_tuple(payload, "skill_surface_hashes"),
        maximum_model_cost_microusd=_integer(payload, "maximum_model_cost_microusd"),
        excluded_uses=_string_tuple(payload, "excluded_uses"),
    )
    if study.to_dict() != payload:
        raise ValueError("Skill Research Study does not match the canonical contract")
    return study


def skill_candidate_group_from_dict(value: object) -> SkillCandidateGroup:
    payload = _object(value, "Skill Candidate Group")
    if payload.get("schema_version") != SKILL_CANDIDATE_GROUP_SCHEMA:
        raise ValueError("unsupported Skill Candidate Group schema_version")
    group = SkillCandidateGroup(
        candidate_group_id=_string(payload, "candidate_group_id"),
        study_id=_string(payload, "study_id"),
        study_hash=_string(payload, "study_hash"),
        conclusion=_conclusion(payload.get("conclusion")),
        validation_blocks=tuple(
            _validation_block(item)
            for item in _object_list(payload.get("validation_blocks"), "validation_blocks")
        ),
        counterexamples=tuple(
            _counterexample(item)
            for item in _object_list(payload.get("counterexamples"), "counterexamples")
        ),
    )
    if group.to_dict() != payload:
        raise ValueError("Skill Candidate Group does not match the canonical contract")
    return group


def skill_governance_review_from_dict(value: object) -> SkillGovernanceReview:
    payload = _object(value, "Skill Governance Review")
    if payload.get("schema_version") != SKILL_GOVERNANCE_REVIEW_SCHEMA:
        raise ValueError("unsupported Skill Governance Review schema_version")
    baseline = _baseline_snapshot(payload.get("baseline_snapshot"))
    review = SkillGovernanceReview(
        review_id=_string(payload, "review_id"),
        evaluated_at=_datetime(payload, "evaluated_at"),
        candidate_group_id=_string(payload, "candidate_group_id"),
        candidate_group_hash=_string(payload, "candidate_group_hash"),
        baseline_snapshot=baseline,
        comparisons=tuple(
            _catalog_comparison(item)
            for item in _object_list(payload.get("comparisons"), "comparisons")
        ),
        admitted_as_candidate=_boolean(payload, "admitted_as_candidate"),
        blockers=_string_tuple(payload, "blockers"),
        candidate_id=_optional_string(payload, "candidate_id"),
    )
    if review.to_dict() != payload:
        raise ValueError("Skill Governance Review does not match the canonical contract")
    return review


def _work_unit(value: object) -> SkillResearchWorkUnit:
    payload = _object(value, "Skill Research Work Unit")
    return SkillResearchWorkUnit(
        unit_id=_string(payload, "unit_id"),
        event_family=_string(payload, "event_family"),
        time_block=_string(payload, "time_block"),
        instrument_scope=_string_tuple(payload, "instrument_scope"),
        source_artifact_hashes=_string_tuple(payload, "source_artifact_hashes"),
    )


def _conclusion(value: object) -> SkillConclusion:
    payload = _object(value, "Skill conclusion")
    return SkillConclusion(
        proposed_name=_string(payload, "proposed_name"),
        proposed_version=_string(payload, "proposed_version"),
        group=_enum(SkillGroup, payload.get("group"), "Skill group"),
        change_kind=_enum(
            SkillChangeKind,
            payload.get("change_kind"),
            "Skill change kind",
        ),
        proposition=_string(payload, "proposition"),
        mechanism_steps=_string_tuple(payload, "mechanism_steps"),
        applicability_conditions=_string_tuple(payload, "applicability_conditions"),
        exceptions=_string_tuple(payload, "exceptions"),
        required_evidence=_string_tuple(payload, "required_evidence"),
        prohibited_uses=_string_tuple(payload, "prohibited_uses"),
    )


def _validation_metric_policy(value: object) -> SkillValidationMetricPolicy:
    payload = _object(value, "Skill validation metric policy")
    return SkillValidationMetricPolicy(
        metric_id=_string(payload, "metric_id"),
        maximum_divergence=_decimal(payload, "maximum_divergence"),
    )


def _validation_block(value: object) -> SkillValidationBlock:
    payload = _object(value, "Skill validation block")
    return SkillValidationBlock(
        validation_id=_string(payload, "validation_id"),
        role=_enum(ValidationRole, payload.get("role"), "Skill validation role"),
        work_unit=_work_unit(payload.get("work_unit")),
        metric_id=_string(payload, "metric_id"),
        observed_divergence=_decimal(payload, "observed_divergence"),
        supports_conclusion=_boolean(payload, "supports_conclusion"),
        evidence_hashes=_string_tuple(payload, "evidence_hashes"),
        counterexample_search_hash=_string(payload, "counterexample_search_hash"),
        specialist_artifact_hashes=_string_tuple(payload, "specialist_artifact_hashes"),
    )


def _counterexample(value: object) -> CounterexampleAssessment:
    payload = _object(value, "Skill counterexample")
    return CounterexampleAssessment(
        counterexample_id=_string(payload, "counterexample_id"),
        severity=_enum(
            CounterexampleSeverity,
            payload.get("severity"),
            "counterexample severity",
        ),
        disposition=_enum(
            CounterexampleDisposition,
            payload.get("disposition"),
            "counterexample disposition",
        ),
        summary=_string(payload, "summary"),
        evidence_hashes=_string_tuple(payload, "evidence_hashes"),
        scope_effect=_string(payload, "scope_effect"),
    )


def _baseline_snapshot(value: object) -> SkillBaselineSnapshot:
    payload = _object(value, "Skill baseline snapshot")
    return SkillBaselineSnapshot(
        snapshot_id=_string(payload, "snapshot_id"),
        subjects=tuple(
            _catalog_subject(item)
            for item in _object_list(payload.get("subjects"), "baseline subjects")
        ),
    )


def _catalog_subject(value: object) -> SkillCatalogSubject:
    payload = _object(value, "Skill catalog subject")
    return SkillCatalogSubject(
        subject_ref=_string(payload, "subject_ref"),
        kind=_enum(
            CatalogSubjectKind,
            payload.get("kind"),
            "catalog subject kind",
        ),
        name=_string(payload, "name"),
        group=_enum(SkillGroup, payload.get("group"), "catalog subject group"),
        content_hash=_string(payload, "content_hash"),
    )


def _catalog_comparison(value: object) -> SkillCatalogComparison:
    payload = _object(value, "Skill catalog comparison")
    return SkillCatalogComparison(
        subject_ref=_string(payload, "subject_ref"),
        relationship=_enum(
            CatalogRelationship,
            payload.get("relationship"),
            "catalog relationship",
        ),
        resolution=_enum(
            CatalogResolution,
            payload.get("resolution"),
            "catalog resolution",
        ),
        rationale=_string(payload, "rationale"),
        evidence_hashes=_string_tuple(payload, "evidence_hashes"),
    )


def _timestamp(value: datetime) -> str:
    require_aware(value, "timestamp")
    return value.isoformat().replace("+00:00", "Z")


def _nonempty(value: str, name: str) -> None:
    if not value or value != value.strip():
        raise ValueError(f"{name} must be a non-empty trimmed string")


def _identifier(value: str, name: str) -> None:
    if re.fullmatch(r"[a-z0-9][a-z0-9._-]*", value) is None:
        raise ValueError(f"{name} must be a lowercase identifier")


def _sha256(value: str, name: str) -> None:
    if re.fullmatch(r"[0-9a-f]{64}", value) is None:
        raise ValueError(f"{name} must be a sha256 hex digest")


def _prefixed_hash(value: str, prefix: str, name: str) -> None:
    if not value.startswith(prefix):
        raise ValueError(f"{name} has an invalid prefix")
    _sha256(value.removeprefix(prefix), name)


def _unique(values: tuple[str, ...], name: str) -> None:
    if len(values) != len(set(values)):
        raise ValueError(f"{name} must be unique")


def _unique_nonempty(values: tuple[str, ...], name: str) -> None:
    _unique(values, name)
    if not values:
        raise ValueError(f"{name} cannot be empty")
    for value in values:
        _nonempty(value, name)


def _unique_hashes(values: tuple[str, ...], name: str, *, nonempty: bool) -> None:
    _unique(values, name)
    if nonempty and not values:
        raise ValueError(f"{name} cannot be empty")
    for value in values:
        _sha256(value, name)


def _object(value: object, name: str) -> dict[str, object]:
    if not isinstance(value, Mapping):
        raise TypeError(f"{name} must be an object")
    raw = cast(Mapping[object, object], value)
    if any(not isinstance(key, str) for key in raw):
        raise TypeError(f"{name} keys must be strings")
    return cast(dict[str, object], dict(raw))


def _object_list(value: object, name: str) -> tuple[dict[str, object], ...]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        raise TypeError(f"{name} must be an array")
    return tuple(_object(item, name) for item in cast(Sequence[object], value))


def _string(payload: Mapping[str, object], name: str) -> str:
    value = payload.get(name)
    if not isinstance(value, str):
        raise TypeError(f"{name} must be a string")
    return value


def _optional_string(payload: Mapping[str, object], name: str) -> str | None:
    value = payload.get(name)
    if value is None:
        return None
    if not isinstance(value, str):
        raise TypeError(f"{name} must be a string or null")
    return value


def _string_tuple(payload: Mapping[str, object], name: str) -> tuple[str, ...]:
    value = payload.get(name)
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        raise TypeError(f"{name} must be an array")
    result: list[str] = []
    for item in cast(Sequence[object], value):
        if not isinstance(item, str):
            raise TypeError(f"{name} items must be strings")
        result.append(item)
    return tuple(result)


def _integer(payload: Mapping[str, object], name: str) -> int:
    value = payload.get(name)
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{name} must be an integer")
    return value


def _boolean(payload: Mapping[str, object], name: str) -> bool:
    value = payload.get(name)
    if not isinstance(value, bool):
        raise TypeError(f"{name} must be a boolean")
    return value


def _datetime(payload: Mapping[str, object], name: str) -> datetime:
    value = _string(payload, name)
    try:
        result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{name} must be an ISO-8601 timestamp") from exc
    require_aware(result, name)
    return result.astimezone(UTC)


def _decimal(payload: Mapping[str, object], name: str) -> Decimal:
    value = _string(payload, name)
    try:
        result = Decimal(value)
    except InvalidOperation as exc:
        raise ValueError(f"{name} must be a decimal string") from exc
    if not result.is_finite():
        raise ValueError(f"{name} must be finite")
    return result


def _enum[EnumT: StrEnum](enum_type: type[EnumT], value: object, name: str) -> EnumT:
    if not isinstance(value, str):
        raise TypeError(f"{name} must be a string")
    try:
        return enum_type(value)
    except ValueError as exc:
        raise ValueError(f"{name} is unsupported") from exc
