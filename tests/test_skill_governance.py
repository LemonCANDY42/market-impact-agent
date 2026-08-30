from datetime import UTC, datetime, timedelta
from decimal import Decimal
from hashlib import sha256
from pathlib import Path

import pytest

from market_impact_agent.agent_runtime import SkillRegistry
from market_impact_agent.agent_schema import validate_agent_contract
from market_impact_agent.method_skills import load_method_skill_catalog
from market_impact_agent.research_methods import load_research_method_catalog
from market_impact_agent.skill_governance import (
    DEFAULT_SKILL_GROUP_ASSIGNMENTS_V1,
    CatalogRelationship,
    CatalogResolution,
    CatalogSubjectKind,
    CounterexampleAssessment,
    CounterexampleDisposition,
    CounterexampleSeverity,
    SkillBaselineSnapshot,
    SkillCandidateGroup,
    SkillCatalogComparison,
    SkillCatalogSubject,
    SkillChangeKind,
    SkillConclusion,
    SkillGroup,
    SkillResearchStudy,
    SkillResearchWorkUnit,
    SkillValidationBlock,
    SkillValidationMetricPolicy,
    ValidationRole,
    build_complete_skill_baseline_snapshot,
    evaluate_skill_candidate,
    skill_candidate_group_from_dict,
    skill_governance_review_from_dict,
    skill_research_study_from_dict,
)

NOW = datetime(2026, 8, 30, 6, tzinfo=UTC)


def digest(value: str) -> str:
    return sha256(value.encode()).hexdigest()


def work_unit(index: int) -> SkillResearchWorkUnit:
    return SkillResearchWorkUnit(
        unit_id=f"case-{index}",
        event_family=f"family-{index}",
        time_block=f"20{20 + index}",
        instrument_scope=(f"sector-{index}", f"asset-{index}"),
        source_artifact_hashes=(digest(f"source-{index}"),),
    )


def study(units: tuple[SkillResearchWorkUnit, ...]) -> SkillResearchStudy:
    return SkillResearchStudy.build(
        registered_at=NOW,
        title="Outcome-opened cross-sector mechanism discovery",
        corpus_hashes=(digest("corpus"),),
        work_units=units,
        validation_metric_policies=(
            SkillValidationMetricPolicy(
                metric_id="normalized-mechanism-divergence",
                maximum_divergence=Decimal("0.15"),
            ),
        ),
        specialist_roles=("event-fact", "transmission", "countercase", "synthesis"),
        provider_profile_hashes=(digest("provider"),),
        skill_surface_hashes=(digest("skills"),),
        maximum_model_cost_microusd=1_000_000,
        excluded_uses=(
            "strict historical PIT",
            "strategy promotion",
            "Signal or Order admission",
        ),
    )


def validation(
    unit: SkillResearchWorkUnit,
    role: ValidationRole,
    *,
    observed: Decimal = Decimal("0.08"),
    supported: bool = True,
) -> SkillValidationBlock:
    return SkillValidationBlock.build(
        role=role,
        work_unit=unit,
        metric_id="normalized-mechanism-divergence",
        observed_divergence=observed,
        supports_conclusion=supported,
        evidence_hashes=(digest(f"evidence-{unit.unit_id}"),),
        counterexample_search_hash=digest(f"counter-search-{unit.unit_id}"),
        specialist_artifact_hashes=(digest(f"specialists-{unit.unit_id}"),),
    )


def candidate_group(
    selected_study: SkillResearchStudy,
    blocks: tuple[SkillValidationBlock, ...],
    *,
    counterexamples: tuple[CounterexampleAssessment, ...] = (),
    group: SkillGroup = SkillGroup.INDUSTRY_SECTOR,
    change_kind: SkillChangeKind = SkillChangeKind.NEW_METHOD,
) -> SkillCandidateGroup:
    conclusion = SkillConclusion(
        proposed_name="channel-restriction-substitution",
        proposed_version="0.1.0",
        group=group,
        change_kind=change_kind,
        proposition="A binding channel restriction can shift demand toward verified substitutes.",
        mechanism_steps=(
            "restriction removes exposed supply",
            "buyers reallocate toward compliant substitutes",
        ),
        applicability_conditions=("restriction is enforced", "substitutes are tradable"),
        exceptions=("supply is immediately restored",),
        required_evidence=("restriction authority", "issuer or industry exposure mapping"),
        prohibited_uses=("headline sentiment alone", "historical fill inference"),
    )
    return SkillCandidateGroup.build(
        study=selected_study,
        conclusion=conclusion,
        validation_blocks=blocks,
        counterexamples=counterexamples,
    )


def baseline() -> SkillBaselineSnapshot:
    return SkillBaselineSnapshot.build(
        (
            SkillCatalogSubject(
                subject_ref="active.equity-exposure",
                kind=CatalogSubjectKind.ACTIVE_SKILL,
                name="equity-exposure",
                group=SkillGroup.ISSUER_FUNDAMENTAL,
                content_hash=digest("equity-exposure-manifest"),
            ),
            SkillCatalogSubject(
                subject_ref="candidate.regulatory-escalation",
                kind=CatalogSubjectKind.OPEN_CANDIDATE,
                name="regulatory-escalation",
                group=SkillGroup.EVENT_TRANSMISSION,
                content_hash=digest("prior-candidate"),
            ),
        )
    )


def comparisons(
    selected_baseline: SkillBaselineSnapshot,
) -> tuple[SkillCatalogComparison, ...]:
    return tuple(
        SkillCatalogComparison(
            subject_ref=item.subject_ref,
            relationship=CatalogRelationship.ORTHOGONAL,
            resolution=CatalogResolution.NO_ACTION,
            rationale="Different proposition, evidence gate, and applicability scope.",
            evidence_hashes=(digest(f"comparison-{item.subject_ref}"),),
        )
        for item in selected_baseline.subjects
    )


def test_outcome_opened_study_and_candidate_are_non_executable_and_schema_valid() -> None:
    units = tuple(work_unit(index) for index in range(1, 4))
    selected_study = study(units)
    group = candidate_group(
        selected_study,
        (
            validation(units[0], ValidationRole.DISCOVERY),
            validation(units[1], ValidationRole.INDEPENDENT_VALIDATION),
            validation(units[2], ValidationRole.INDEPENDENT_VALIDATION),
        ),
    )
    selected_baseline = baseline()
    review = evaluate_skill_candidate(
        candidate_group=group,
        study=selected_study,
        baseline_snapshot=selected_baseline,
        comparisons=comparisons(selected_baseline),
        evaluated_at=NOW + timedelta(hours=1),
    )

    assert review.admitted_as_candidate is True
    assert review.candidate_id is not None
    assert review.to_dict()["active_skill"] is False
    assert review.to_dict()["catalog_mutation"] is False
    assert group.to_dict()["execution_capability"] == "none"
    assert selected_study.to_dict()["multi_expert_authority"] == "decomposition_only"
    assert not validate_agent_contract(selected_study.to_dict(), "skill-research-study.schema.json")
    assert not validate_agent_contract(group.to_dict(), "skill-candidate-group.schema.json")
    assert not validate_agent_contract(review.to_dict(), "skill-governance-review.schema.json")
    assert skill_research_study_from_dict(selected_study.to_dict()) == selected_study
    assert skill_candidate_group_from_dict(group.to_dict()) == group
    assert skill_governance_review_from_dict(review.to_dict()) == review


def test_independent_validations_and_counterexamples_are_hard_candidate_gates() -> None:
    units = tuple(work_unit(index) for index in range(1, 3))
    selected_study = study(units)
    unresolved = CounterexampleAssessment(
        counterexample_id="unresolved-channel-case",
        severity=CounterexampleSeverity.MATERIAL,
        disposition=CounterexampleDisposition.UNRESOLVED,
        summary="A material case moves in the opposite direction.",
        evidence_hashes=(digest("counterexample"),),
        scope_effect="The applicable channel boundary is not yet known.",
    )
    group = candidate_group(
        selected_study,
        (
            validation(units[0], ValidationRole.DISCOVERY),
            validation(
                units[1],
                ValidationRole.INDEPENDENT_VALIDATION,
                observed=Decimal("0.20"),
                supported=False,
            ),
        ),
        counterexamples=(unresolved,),
    )
    selected_baseline = baseline()
    review = evaluate_skill_candidate(
        candidate_group=group,
        study=selected_study,
        baseline_snapshot=selected_baseline,
        comparisons=comparisons(selected_baseline),
        evaluated_at=NOW + timedelta(hours=1),
    )

    assert review.admitted_as_candidate is False
    assert review.candidate_id is None
    assert "requires_at_least_two_additional_independent_validation_blocks" in review.blockers
    assert "independent_validation_failed_or_exceeded_divergence" in review.blockers
    assert "unresolved_material_or_refuting_counterexample" in review.blockers


def test_repeated_agent_work_on_one_case_does_not_create_independent_validation() -> None:
    unit = work_unit(1)
    selected_study = study((unit,))
    first = validation(unit, ValidationRole.DISCOVERY)
    repeated = validation(unit, ValidationRole.INDEPENDENT_VALIDATION)

    with pytest.raises(ValueError, match="independent work units"):
        candidate_group(selected_study, (first, repeated))


def test_relabeling_one_semantic_work_unit_cannot_create_independence() -> None:
    unit = work_unit(1)
    renamed = SkillResearchWorkUnit(
        unit_id="renamed-case",
        event_family=unit.event_family,
        time_block=unit.time_block,
        instrument_scope=unit.instrument_scope,
        source_artifact_hashes=(digest("different-source-selection"),),
    )

    with pytest.raises(ValueError, match="independence keys"):
        study((unit, renamed))


def test_narrow_to_exception_requires_a_revised_candidate_group() -> None:
    units = tuple(work_unit(index) for index in range(1, 4))
    selected_study = study(units)
    group = candidate_group(
        selected_study,
        tuple(
            validation(
                unit,
                ValidationRole.DISCOVERY if index == 0 else ValidationRole.INDEPENDENT_VALIDATION,
            )
            for index, unit in enumerate(units)
        ),
    )
    selected_baseline = baseline()
    narrowed = tuple(
        SkillCatalogComparison(
            subject_ref=item.subject_ref,
            relationship=(
                CatalogRelationship.CANDIDATE_SUBSUMED_BY_SUBJECT
                if index == 0
                else CatalogRelationship.ORTHOGONAL
            ),
            resolution=(
                CatalogResolution.NARROW_TO_EXCEPTION if index == 0 else CatalogResolution.NO_ACTION
            ),
            rationale="Must rewrite and revalidate a narrower proposition.",
            evidence_hashes=(digest(f"narrow-{index}"),),
        )
        for index, item in enumerate(selected_baseline.subjects)
    )

    review = evaluate_skill_candidate(
        candidate_group=group,
        study=selected_study,
        baseline_snapshot=selected_baseline,
        comparisons=narrowed,
        evaluated_at=NOW + timedelta(hours=1),
    )

    assert review.admitted_as_candidate is False
    assert "catalog_review_requires_rejection_or_revision" in review.blockers


def test_catalog_duplicate_and_outcome_learned_evidence_authority_do_not_enter_candidates() -> None:
    units = tuple(work_unit(index) for index in range(1, 4))
    selected_study = study(units)
    group = candidate_group(
        selected_study,
        tuple(
            validation(
                unit,
                ValidationRole.DISCOVERY if index == 0 else ValidationRole.INDEPENDENT_VALIDATION,
            )
            for index, unit in enumerate(units)
        ),
        group=SkillGroup.EVIDENCE_AUTHORITY,
        change_kind=SkillChangeKind.NEW_METHOD,
    )
    selected_baseline = baseline()
    duplicate = tuple(
        SkillCatalogComparison(
            subject_ref=item.subject_ref,
            relationship=(
                CatalogRelationship.DUPLICATE if index == 0 else CatalogRelationship.ORTHOGONAL
            ),
            resolution=(
                CatalogResolution.REJECT_DUPLICATE if index == 0 else CatalogResolution.NO_ACTION
            ),
            rationale="Exact duplicate" if index == 0 else "No overlap",
            evidence_hashes=(digest(f"duplicate-{index}"),),
        )
        for index, item in enumerate(selected_baseline.subjects)
    )
    review = evaluate_skill_candidate(
        candidate_group=group,
        study=selected_study,
        baseline_snapshot=selected_baseline,
        comparisons=duplicate,
        evaluated_at=NOW + timedelta(hours=1),
    )

    assert review.admitted_as_candidate is False
    assert "outcome_opened_research_cannot_weaken_or_invent_evidence_authority" in review.blockers
    assert "catalog_review_requires_rejection_or_revision" in review.blockers


def test_complete_baseline_covers_runtime_and_both_research_catalogs() -> None:
    manifests = SkillRegistry(Path("skills")).discover()
    snapshot = build_complete_skill_baseline_snapshot(
        runtime_manifests=manifests,
        research_method_catalog=load_research_method_catalog(
            Path("examples/research/research-method-catalog-v2.json")
        ),
        method_skill_catalog=load_method_skill_catalog(
            Path("examples/research/famous-method-skill-catalog-v1.json")
        ),
        group_assignments=DEFAULT_SKILL_GROUP_ASSIGNMENTS_V1,
    )

    assert len(snapshot.subjects) == len(manifests) == 13
    assert {item.name for item in snapshot.subjects} == {item.name for item in manifests}
    missing_assignment = dict(DEFAULT_SKILL_GROUP_ASSIGNMENTS_V1)
    missing_assignment.pop("energy-supply")
    with pytest.raises(ValueError, match="cover every runtime Skill"):
        build_complete_skill_baseline_snapshot(
            runtime_manifests=manifests,
            research_method_catalog=load_research_method_catalog(
                Path("examples/research/research-method-catalog-v2.json")
            ),
            method_skill_catalog=load_method_skill_catalog(
                Path("examples/research/famous-method-skill-catalog-v1.json")
            ),
            group_assignments=missing_assignment,
        )
