from pathlib import Path

import pytest

from market_impact_agent.agent_runtime import SkillRegistry
from market_impact_agent.agent_study import load_agent_phase2_preregistration
from market_impact_agent.model_provider import load_model_provider_profile
from market_impact_agent.research_methods import (
    MethodArm,
    MethodArmSpec,
    ResearchContext,
    ResearchMethodRouter,
    build_arm_studies,
    load_method_ablation_registration,
    load_research_method_catalog,
)

CATALOG = Path("examples/research/research-method-catalog-v1.json")
ABLATION = Path("examples/calibration/agent-method-ablation-v1.json")
PARENT = Path("examples/calibration/agent-physical-energy-prospective-v1.json")
REGISTRY = Path("examples/research/a-share-energy-exposure-registry-v1.json")
PROFILE = Path("examples/providers/minimax-m3-research-v1.json")


def test_router_builds_the_four_frozen_persona_free_method_arms() -> None:
    catalog = load_research_method_catalog(CATALOG)
    router = ResearchMethodRouter(catalog=catalog, skills=SkillRegistry(Path("skills")))
    context = ResearchContext(
        mechanism_family="physical_energy_supply_shock",
        asset_class="public_equity",
        has_pattern_pack=True,
    )
    routes = tuple(router.route(arm=arm, context=context) for arm in MethodArm)

    assert tuple(route.requested_skills for route in routes) == (
        ("evidence-core",),
        (
            "evidence-core",
            "event-market-context",
            "equity-exposure",
            "adversarial-risk",
        ),
        (
            "evidence-core",
            "event-market-context",
            "equity-exposure",
            "adversarial-risk",
            "pattern-review",
        ),
        (
            "evidence-core",
            "event-market-context",
            "equity-exposure",
            "adversarial-risk",
            "pattern-review",
            "energy-supply",
        ),
    )
    assert routes[0].allowed_tools == ("read_evidence",)
    assert routes[2].allowed_tools == ("read_evidence", "read_pattern_pack")


def test_pattern_and_family_arms_fail_closed_when_inputs_do_not_support_them() -> None:
    router = ResearchMethodRouter(
        catalog=load_research_method_catalog(CATALOG),
        skills=SkillRegistry(Path("skills")),
    )
    no_pattern = ResearchContext(
        mechanism_family="physical_energy_supply_shock",
        asset_class="public_equity",
        has_pattern_pack=False,
    )
    with pytest.raises(ValueError, match="requires a frozen Pattern Pack"):
        router.route(arm=MethodArm.GENERAL_PATTERN, context=no_pattern)
    unsupported_family = ResearchContext(
        mechanism_family="geopolitical_policy_shock",
        asset_class="public_equity",
        has_pattern_pack=True,
    )
    with pytest.raises(ValueError, match="no applicable family method"):
        router.route(arm=MethodArm.FAMILY_GUIDED, context=unsupported_family)


def test_ablation_registration_binds_routes_parent_catalog_and_provider() -> None:
    parent, registry = load_agent_phase2_preregistration(PARENT, REGISTRY)
    profile = load_model_provider_profile(PROFILE)
    catalog = load_research_method_catalog(CATALOG)
    ablation = load_method_ablation_registration(ABLATION)
    ablation.validate_against(
        parent=parent,
        registry=registry,
        catalog=catalog,
        provider_profile_id=profile.profile_id,
        provider_profile_hash=profile.profile_hash,
    )
    router = ResearchMethodRouter(catalog=catalog, skills=SkillRegistry(Path("skills")))
    context = ResearchContext(
        mechanism_family="physical_energy_supply_shock",
        asset_class="public_equity",
        has_pattern_pack=True,
    )
    assert (
        tuple(
            MethodArmSpec.from_route(router.route(arm=arm.arm, context=context))
            for arm in ablation.arms
        )
        == ablation.arms
    )
    studies = build_arm_studies(ablation=ablation, parent=parent)
    assert len({study.registration_id for study in studies}) == 4
    assert all(study.agent_protocol.replicate_count == 5 for study in studies)
