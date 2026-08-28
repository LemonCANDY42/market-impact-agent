from __future__ import annotations

import asyncio
import copy
import json
import sqlite3
from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import cast

import pytest

from market_impact_agent.agent_contracts import (
    CandidateDirection,
    CandidateImpact,
    JudgmentArtifact,
    JudgmentDecision,
    JudgmentProposal,
    canonical_json_bytes,
    judgment_artifact_from_dict,
)
from market_impact_agent.agent_engine import (
    AgentExecutionBinding,
    AgentRunResult,
    RunMetrics,
)
from market_impact_agent.agent_runtime import (
    ModelProvider,
    ModelTurn,
    ProviderUsage,
    RuntimeConfig,
    SkillRegistry,
)
from market_impact_agent.agent_schema import validate_agent_contract
from market_impact_agent.frozen_research import FrozenResearchRepository
from market_impact_agent.method_skills import (
    CPAUsageKeeperPricing,
    MethodRoutingContext,
    MethodSkillRouter,
    PairedSkillAblationRegistration,
    estimate_paired_skill_ablation_cost,
    load_method_evidence_declaration,
    load_method_skill_catalog,
)
from market_impact_agent.model_provider import load_model_provider_profile
from market_impact_agent.paired_skill_ablation_runner import (
    SkillAblationArm,
    prepare_paired_method_skill_ablation,
    run_paired_method_skill_ablation,
)
from market_impact_agent.paired_skill_execution_audit import (
    audit_paired_execution_state,
    validate_judgment_execution_binding,
)
from market_impact_agent.research import TransmissionDirectness
from market_impact_agent.runtime_store import ArtifactStore, RunJournal, RunStatus
from market_impact_agent.usage_ledger import UsageLedger

CATALOG = Path("examples/research/famous-method-skill-catalog-v1.json")
PROFILE = Path("examples/providers/cliproxyapi-luna-xhigh-cpa-v1.json")
RECOVERY = Path("examples/agent/abqaiq_development")
DECLARATION = Path("examples/research/abqaiq-recovery-method-evidence-v1.json")


def test_point_in_time_router_selects_methods_by_need_and_available_evidence() -> None:
    router = MethodSkillRouter(load_method_skill_catalog(CATALOG))

    fast_feedback = router.route(
        MethodRoutingContext(
            market_state="up_fast",
            narrative_salience="corroborated_obvious",
            analysis_needs=("feedback_loop", "narrative_diffusion"),
            available_evidence=(
                "participant_belief_or_flow",
                "fundamental_feedback",
                "timestamped_narrative_corpus",
            ),
            outcomes_opened=False,
        )
    )
    assert fast_feedback.selected_skills == (
        "reflexive-feedback-check",
        "narrative-diffusion-assessment",
    )

    value_context = router.route(
        MethodRoutingContext(
            market_state="up_mild",
            narrative_salience="diffuse",
            analysis_needs=("owner_value",),
            available_evidence=("cash_flow_or_earning_power", "valuation_or_price"),
            outcomes_opened=False,
        )
    )
    assert value_context.selected_skills == ("owner-value-discipline",)


def test_router_does_not_select_a_method_when_its_evidence_gate_is_missing() -> None:
    router = MethodSkillRouter(load_method_skill_catalog(CATALOG))
    route = router.route(
        MethodRoutingContext(
            market_state="down_fast",
            narrative_salience="contested",
            analysis_needs=("feedback_loop",),
            available_evidence=("participant_belief_or_flow",),
            outcomes_opened=False,
        )
    )

    assert route.selected_skills == ()
    assert route.rejected_methods == (("reflexive-feedback-check", ("fundamental_feedback",)),)


def test_all_famous_method_skills_are_persona_free_and_read_only() -> None:
    catalog = load_method_skill_catalog(CATALOG)
    registry = SkillRegistry(Path("skills"))
    loaded = registry.load(
        tuple(item.skill_name for item in catalog.methods),
        allowed_capabilities=frozenset({"evidence.read"}),
    )

    assert tuple(item.manifest.name for item in loaded) == (
        "evidence-core",
        *(item.skill_name for item in catalog.methods),
    )
    method_loaded = loaded[1:]
    assert all(
        item.manifest.allowed_tools == frozenset({"read_evidence"}) for item in method_loaded
    )
    assert all(item.manifest.allowed_mcp_servers == frozenset() for item in loaded)
    assert all("Do not impersonate" in item.instructions for item in method_loaded)


def _cpa_pricing() -> CPAUsageKeeperPricing:
    return CPAUsageKeeperPricing.from_api_payloads(
        model="gpt-5.6-luna",
        captured_at=datetime(2026, 8, 27, 10, 0, tzinfo=UTC),
        version_payload={"version": "v1.14.5"},
        pricing_payload={
            "pricing": [
                {
                    "model": "gpt-5.6-luna",
                    "pricing_style": "openai",
                    "prompt_price_per_1m": 0.2,
                    "completion_price_per_1m": 1.2,
                    "cache_read_price_per_1m": 0.02,
                    "cache_write_price_per_1m": 0.25,
                    "price_multiplier": 1,
                }
            ]
        },
        rules_payload={"model": "gpt-5.6-luna", "rules": []},
    )


def test_cpa_pricing_preflight_prices_three_paired_runs_below_ten_dollars() -> None:
    profile = load_model_provider_profile(PROFILE)
    pricing = _cpa_pricing()

    estimate = estimate_paired_skill_ablation_cost(
        pricing=pricing,
        profile=profile,
        replicate_count=3,
        arm_count=2,
        safety_multiplier=Decimal("1.25"),
        max_total_cost_microusd=10_000_000,
    )

    assert estimate.agent_run_count == 6
    assert estimate.provider_request_upper_bound == 96
    assert estimate.raw_max_cost_microusd == 985_932
    assert estimate.guarded_max_cost_microusd == 1_232_415
    assert estimate.within_budget is True


def test_cpa_preflight_rejects_price_drift_or_a_total_over_hard_cap() -> None:
    profile = load_model_provider_profile(PROFILE)
    pricing = _cpa_pricing()
    drifted = replace(pricing, prompt_microusd_per_million_tokens=300_000)

    with pytest.raises(ValueError, match="does not match the frozen Provider Profile"):
        estimate_paired_skill_ablation_cost(
            pricing=drifted,
            profile=profile,
            replicate_count=3,
            arm_count=2,
            safety_multiplier=Decimal("1.25"),
            max_total_cost_microusd=10_000_000,
        )


def test_cpa_preflight_rejects_version_or_unfrozen_service_tier_rules() -> None:
    profile = load_model_provider_profile(PROFILE)
    pricing = _cpa_pricing()

    with pytest.raises(ValueError, match="CPA version does not match"):
        estimate_paired_skill_ablation_cost(
            pricing=replace(pricing, keeper_version="v1.14.7"),
            profile=profile,
            replicate_count=3,
            arm_count=2,
            safety_multiplier=Decimal("1.25"),
            max_total_cost_microusd=10_000_000,
        )
    with pytest.raises(ValueError, match="exact semantic version"):
        replace(pricing, keeper_version="v1.14")
    with pytest.raises(ValueError, match="service-tier pricing"):
        estimate_paired_skill_ablation_cost(
            pricing=replace(
                pricing,
                rules=(("service_tier", "priority", Decimal("2")),),
            ),
            profile=profile,
            replicate_count=3,
            arm_count=2,
            safety_multiplier=Decimal("1.25"),
            max_total_cost_microusd=10_000_000,
        )


def test_cpa_preflight_prices_cache_write_at_the_worst_input_rate() -> None:
    profile = load_model_provider_profile(PROFILE)
    pricing = replace(
        _cpa_pricing(),
        cache_write_microusd_per_million_tokens=100_000_000,
    )

    with pytest.raises(ValueError, match="exceeds the experiment hard cap"):
        estimate_paired_skill_ablation_cost(
            pricing=pricing,
            profile=profile,
            replicate_count=3,
            arm_count=2,
            safety_multiplier=Decimal("1.25"),
            max_total_cost_microusd=10_000_000,
        )
    with pytest.raises(ValueError, match="exceeds the experiment hard cap"):
        estimate_paired_skill_ablation_cost(
            pricing=pricing,
            profile=profile,
            replicate_count=3,
            arm_count=2,
            safety_multiplier=Decimal("1.25"),
            max_total_cost_microusd=1_000_000,
        )


def test_paired_registration_changes_only_one_method_skill_and_requires_three_pairs() -> None:
    profile = load_model_provider_profile(PROFILE)
    pricing = _cpa_pricing()
    estimate = estimate_paired_skill_ablation_cost(
        pricing=pricing,
        profile=profile,
        replicate_count=3,
        arm_count=2,
        safety_multiplier=Decimal("1.25"),
        max_total_cost_microusd=10_000_000,
    )
    control = (
        "evidence-core",
        "research-discipline",
        "event-market-context",
        "equity-exposure",
        "adversarial-risk",
    )
    routing_context = MethodRoutingContext(
        market_state="unclassified",
        narrative_salience="authority_obvious",
        analysis_needs=("base_rate_update",),
        available_evidence=("reference_class", "new_evidence"),
        outcomes_opened=True,
    )
    route = MethodSkillRouter(load_method_skill_catalog(CATALOG)).route(routing_context)
    evidence_declaration = load_method_evidence_declaration(DECLARATION)
    skills = SkillRegistry(Path("skills"))
    control_loaded = skills.load(control, allowed_capabilities=frozenset({"evidence.read"}))
    treatment_loaded = skills.load(
        (*control, "expectations-base-rates"),
        allowed_capabilities=frozenset({"evidence.read"}),
    )
    registration = PairedSkillAblationRegistration.build(
        experiment_id="method-skill-mini-ablation-test",
        registered_at=datetime(2026, 8, 27, 10, 1, tzinfo=UTC),
        provider_profile_id=profile.profile_id,
        provider_profile_hash=profile.profile_hash,
        method_catalog_id=load_method_skill_catalog(CATALOG).catalog_id,
        method_evidence_declaration_id=evidence_declaration.declaration_id,
        method_evidence_declaration_hash=evidence_declaration.declaration_hash,
        evidence_pack_id="evidence-pack-" + "a" * 64,
        evidence_pack_hash="b" * 64,
        control_skills=control,
        treatment_skills=(*control, "expectations-base-rates"),
        control_manifest_hashes=tuple(item.manifest.manifest_hash for item in control_loaded),
        treatment_manifest_hashes=tuple(item.manifest.manifest_hash for item in treatment_loaded),
        method_route_id=route.route_id,
        routing_context=routing_context,
        replicate_count=3,
        common_input_hash="c" * 64,
        pricing=pricing,
        cost_estimate=estimate,
        outcomes_opened=True,
    )

    assert registration.registration_id == (
        "method-skill-ablation-" + registration.registration_hash
    )
    assert registration.added_treatment_skill == "expectations-base-rates"
    assert registration.replicate_count == 3
    assert registration.execution_capability == "none"

    with pytest.raises(ValueError, match="exactly three paired replicates"):
        PairedSkillAblationRegistration.build(
            experiment_id="bad",
            registered_at=datetime(2026, 8, 27, 10, 1, tzinfo=UTC),
            provider_profile_id=profile.profile_id,
            provider_profile_hash=profile.profile_hash,
            method_catalog_id=load_method_skill_catalog(CATALOG).catalog_id,
            method_evidence_declaration_id=evidence_declaration.declaration_id,
            method_evidence_declaration_hash=evidence_declaration.declaration_hash,
            evidence_pack_id="evidence-pack-" + "a" * 64,
            evidence_pack_hash="b" * 64,
            control_skills=control,
            treatment_skills=(*control, "expectations-base-rates"),
            control_manifest_hashes=tuple(item.manifest.manifest_hash for item in control_loaded),
            treatment_manifest_hashes=tuple(
                item.manifest.manifest_hash for item in treatment_loaded
            ),
            method_route_id=route.route_id,
            routing_context=routing_context,
            replicate_count=2,
            common_input_hash="c" * 64,
            pricing=pricing,
            cost_estimate=estimate,
            outcomes_opened=True,
        )


def _routing_context() -> MethodRoutingContext:
    return MethodRoutingContext(
        market_state="unclassified",
        narrative_salience="authority_obvious",
        analysis_needs=("base_rate_update",),
        available_evidence=("reference_class", "new_evidence"),
        outcomes_opened=True,
    )


def test_prepared_paired_ablation_is_content_bound_and_schema_valid() -> None:
    prepared = prepare_paired_method_skill_ablation(
        method_catalog_path=CATALOG,
        method_evidence_declaration_path=DECLARATION,
        provider_profile_path=PROFILE,
        evidence_pack_path=RECOVERY / "evidence-pack-recovery.json",
        evidence_documents_path=RECOVERY / "evidence-documents-recovery.json",
        pattern_pack_paths=(RECOVERY / "pattern-pack.json",),
        experiment_id="prepared-method-skill-ablation",
        treatment_skill="expectations-base-rates",
        routing_context=_routing_context(),
        skill_root=Path("skills"),
        max_total_cost_microusd=10_000_000,
        pricing=_cpa_pricing(),
        registered_at=datetime(2026, 8, 27, 10, 5, tzinfo=UTC),
    )

    assert prepared.route.selected_skills == ("expectations-base-rates",)
    assert prepared.registration.cost_estimate.agent_run_count == 6
    assert prepared.registration.cost_estimate.provider_request_upper_bound == 96
    assert prepared.arms[0].selected_skills == prepared.arms[1].selected_skills[:-1]
    assert prepared.arms[1].selected_skills[-1] == "expectations-base-rates"
    assert "only horizon is one trading session" in prepared.research_instruction
    assert prepared.repository.evidence_pack.event_id in prepared.research_instruction
    assert "copy that exact event_id" in prepared.research_instruction
    assert (
        validate_agent_contract(
            prepared.registration.to_dict(),
            "method-skill-ablation-registration.schema.json",
        )
        == ()
    )


def test_prepared_paired_ablation_can_freeze_a_multi_session_horizon() -> None:
    prepared = prepare_paired_method_skill_ablation(
        method_catalog_path=CATALOG,
        method_evidence_declaration_path=DECLARATION,
        provider_profile_path=PROFILE,
        evidence_pack_path=RECOVERY / "evidence-pack-recovery.json",
        evidence_documents_path=RECOVERY / "evidence-documents-recovery.json",
        pattern_pack_paths=(RECOVERY / "pattern-pack.json",),
        experiment_id="prepared-multi-session-method-skill-ablation",
        treatment_skill="expectations-base-rates",
        routing_context=_routing_context(),
        skill_root=Path("skills"),
        max_total_cost_microusd=10_000_000,
        pricing=_cpa_pricing(),
        registered_at=datetime(2026, 8, 27, 10, 5, tzinfo=UTC),
        eligible_horizon_sessions=4,
    )

    assert "only horizon is 4 trading sessions" in prepared.research_instruction
    assert "one-session persistence" not in prepared.research_instruction


class _FakeAvailableProvider:
    def __init__(self) -> None:
        self.available_checked = False

    @property
    def provider_id(self) -> str:
        return "cliproxyapi-openai-compatible"

    @property
    def model(self) -> str:
        return "gpt-5.6-luna"

    async def assert_model_available(self, *, timeout_seconds: float) -> None:
        assert timeout_seconds == 30
        self.available_checked = True

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
        _ = (messages, tools, temperature, top_p, max_output_tokens, timeout_seconds)
        raise AssertionError("fake replicate runner must avoid Provider calls")


def test_paired_runner_records_exactly_six_terminal_runs(tmp_path: Path) -> None:
    provider = _FakeAvailableProvider()
    seen: list[tuple[str, str]] = []

    async def failed_runner(
        *,
        repository: FrozenResearchRepository,
        provider: ModelProvider,
        config: RuntimeConfig,
        arm: SkillAblationArm,
        research_instruction: str,
        run_id: str,
        skill_root: Path,
        state_directory: Path,
        secret_values: tuple[str, ...],
    ) -> AgentRunResult:
        _ = (
            repository,
            provider,
            research_instruction,
            skill_root,
            secret_values,
        )
        seen.append((arm.arm_id, run_id))
        journal = RunJournal(state_directory / "run.sqlite3")
        started = datetime(2026, 8, 27, 10, len(seen), tzinfo=UTC)
        journal.start_run(run_id=run_id, config_hash=config.config_hash, created_at=started)
        journal.finish(
            run_id=run_id,
            status=RunStatus.FAILED,
            finished_at=started,
            terminal_artifact_id=None,
        )
        return AgentRunResult(
            run_id=run_id,
            status=RunStatus.FAILED,
            judgment=None,
            terminal_store_hash=None,
            metrics=RunMetrics(
                turns=1,
                tool_calls=0,
                input_tokens=100,
                output_tokens=10,
                result_bytes=20,
                latency_ms=1,
                provider_attempts=1,
                estimated_cost_microusd=32,
            ),
        )

    result = asyncio.run(
        run_paired_method_skill_ablation(
            method_catalog_path=CATALOG,
            method_evidence_declaration_path=DECLARATION,
            provider_profile_path=PROFILE,
            evidence_pack_path=RECOVERY / "evidence-pack-recovery.json",
            evidence_documents_path=RECOVERY / "evidence-documents-recovery.json",
            pattern_pack_paths=(RECOVERY / "pattern-pack.json",),
            experiment_id="six-terminal-method-skill-ablation",
            treatment_skill="expectations-base-rates",
            routing_context=_routing_context(),
            skill_root=Path("skills"),
            state_root=tmp_path,
            pricing=_cpa_pricing(),
            provider=provider,
            replicate_runner=failed_runner,
            clock=lambda: datetime(2026, 8, 27, 10, 5, tzinfo=UTC),
        )
    )

    assert provider.available_checked is True
    assert len(seen) == 6
    assert [arm for arm, _run_id in seen] == [
        "general_control",
        "general_plus_expectations_base_rates",
    ] * 3
    assert result["diagnostic_valid"] is False
    assert cast(dict[str, object], result["cost"])["ledger_actual_microusd"] == 192
    ledger = UsageLedger(Path(cast(str, result["state_directory"])) / "usage.sqlite3")
    assert len(ledger.records()) == 6


class _CompletedAvailableProvider:
    def __init__(self) -> None:
        self.available_checked = False
        self.call_count = 0

    @property
    def provider_id(self) -> str:
        return "cliproxyapi-openai-compatible"

    @property
    def model(self) -> str:
        return "gpt-5.6-luna"

    async def assert_model_available(self, *, timeout_seconds: float) -> None:
        assert timeout_seconds == 30
        self.available_checked = True

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
        _ = (messages, tools, temperature, top_p, max_output_tokens, timeout_seconds)
        self.call_count += 1
        proposal = JudgmentProposal(
            event_id="masked-physical-supply-case-a",
            decision=JudgmentDecision.ABSTAIN,
            summary="The frozen evidence does not establish a decision-ready target impact.",
            transmission_steps=(),
            candidates=(),
            blockers=("decision-ready target impact is not established",),
            unresolved_questions=("What executable target has a falsifiable transmission path?",),
            stopped_reason="critical target mapping remains unresolved",
        )
        content = canonical_json_bytes(proposal.to_dict()).decode()
        assistant: dict[str, object] = {"role": "assistant", "content": content}
        return ModelTurn(
            response_id=f"completed-response-{self.call_count}",
            model=self.model,
            assistant_message=assistant,
            tool_calls=(),
            finish_reason="stop",
            usage=ProviderUsage(input_tokens=100, output_tokens=40),
            raw_response={
                "id": f"completed-response-{self.call_count}",
                "model": self.model,
                "message": assistant,
            },
            latency_ms=1,
        )


def test_completed_paired_execution_audit_binds_terminal_judgments(tmp_path: Path) -> None:
    provider = _CompletedAvailableProvider()
    result = asyncio.run(
        run_paired_method_skill_ablation(
            method_catalog_path=CATALOG,
            method_evidence_declaration_path=DECLARATION,
            provider_profile_path=PROFILE,
            evidence_pack_path=RECOVERY / "evidence-pack-recovery.json",
            evidence_documents_path=RECOVERY / "evidence-documents-recovery.json",
            pattern_pack_paths=(RECOVERY / "pattern-pack.json",),
            experiment_id="completed-method-skill-ablation",
            treatment_skill="expectations-base-rates",
            routing_context=_routing_context(),
            skill_root=Path("skills"),
            state_root=tmp_path,
            pricing=_cpa_pricing(),
            provider=provider,
            clock=lambda: datetime(2026, 8, 27, 10, 5, tzinfo=UTC),
        )
    )

    assert provider.available_checked is True
    assert provider.call_count == 6
    assert result["diagnostic_valid"] is True
    repository = FrozenResearchRepository.from_files(
        evidence_pack_path=RECOVERY / "evidence-pack-recovery.json",
        evidence_documents_path=RECOVERY / "evidence-documents-recovery.json",
        pattern_pack_paths=(RECOVERY / "pattern-pack.json",),
    )
    experiment_root = Path(cast(str, result["state_directory"]))
    registration = cast(
        dict[str, object],
        json.loads((experiment_root / "registration.json").read_text()),
    )
    bindings = audit_paired_execution_state(
        expected_evidence_pack=repository.evidence_pack,
        eligible_horizon_sessions=1,
        registration=registration,
        report=result,
        experiment_root=experiment_root,
        evidence_pack_path=RECOVERY / "evidence-pack-recovery.json",
        evidence_documents_path=RECOVERY / "evidence-documents-recovery.json",
        pattern_pack_paths=(RECOVERY / "pattern-pack.json",),
        provider_profile_path=PROFILE,
        skill_root=Path("skills"),
    )
    assert set(bindings) == {"general_control", "general_plus_expectations_base_rates"}

    with pytest.raises(ValueError, match="differs from expected prompt"):
        audit_paired_execution_state(
            expected_evidence_pack=repository.evidence_pack,
            eligible_horizon_sessions=2,
            registration=registration,
            report=result,
            experiment_root=experiment_root,
            evidence_pack_path=RECOVERY / "evidence-pack-recovery.json",
            evidence_documents_path=RECOVERY / "evidence-documents-recovery.json",
            pattern_pack_paths=(RECOVERY / "pattern-pack.json",),
            provider_profile_path=PROFILE,
            skill_root=Path("skills"),
        )

    forged_decision = copy.deepcopy(result)
    forged_arms = cast(list[dict[str, object]], forged_decision["arms"])
    forged_runs = cast(list[dict[str, object]], forged_arms[0]["runs"])
    forged_runs[0]["decision"] = "propose"
    with pytest.raises(ValueError, match="differs from terminal run evidence"):
        audit_paired_execution_state(
            expected_evidence_pack=repository.evidence_pack,
            eligible_horizon_sessions=1,
            registration=registration,
            report=forged_decision,
            experiment_root=experiment_root,
            evidence_pack_path=RECOVERY / "evidence-pack-recovery.json",
            evidence_documents_path=RECOVERY / "evidence-documents-recovery.json",
            pattern_pack_paths=(RECOVERY / "pattern-pack.json",),
            provider_profile_path=PROFILE,
            skill_root=Path("skills"),
        )

    forged_identity = copy.deepcopy(result)
    forged_identity_arms = cast(list[dict[str, object]], forged_identity["arms"])
    forged_identity_runs = cast(list[dict[str, object]], forged_identity_arms[0]["runs"])
    forged_identity_runs[0]["run_id"] = "forged-run-id"
    with pytest.raises(ValueError, match="run identity drifted"):
        audit_paired_execution_state(
            expected_evidence_pack=repository.evidence_pack,
            eligible_horizon_sessions=1,
            registration=registration,
            report=forged_identity,
            experiment_root=experiment_root,
            evidence_pack_path=RECOVERY / "evidence-pack-recovery.json",
            evidence_documents_path=RECOVERY / "evidence-documents-recovery.json",
            pattern_pack_paths=(RECOVERY / "pattern-pack.json",),
            provider_profile_path=PROFILE,
            skill_root=Path("skills"),
        )

    tampered = copy.deepcopy(result)
    tampered_arms = cast(list[dict[str, object]], tampered["arms"])
    tampered_arms[0]["execution_binding_hash"] = "f" * 64
    with pytest.raises(ValueError, match="differs from expected prompt"):
        audit_paired_execution_state(
            expected_evidence_pack=repository.evidence_pack,
            eligible_horizon_sessions=1,
            registration=registration,
            report=tampered,
            experiment_root=experiment_root,
            evidence_pack_path=RECOVERY / "evidence-pack-recovery.json",
            evidence_documents_path=RECOVERY / "evidence-documents-recovery.json",
            pattern_pack_paths=(RECOVERY / "pattern-pack.json",),
            provider_profile_path=PROFILE,
            skill_root=Path("skills"),
        )

    first_arm = cast(list[dict[str, object]], result["arms"])[0]
    first_run = cast(list[dict[str, object]], first_arm["runs"])[0]
    first_run_id = cast(str, first_run["run_id"])
    ledger = UsageLedger(experiment_root / "usage.sqlite3")
    usage_record = next(
        item.record for item in ledger.records() if item.record.run_id == first_run_id
    )
    run_artifacts = ArtifactStore(
        experiment_root / "runs" / "general_control" / "replicate-1" / "artifacts"
    )
    terminal_payload = run_artifacts.read_json(cast(str, usage_record.terminal_artifact_hash))
    judgment = judgment_artifact_from_dict(terminal_payload)
    binding_payload = ArtifactStore(experiment_root / "artifacts").read_json(
        cast(str, first_arm["execution_binding_hash"])
    )
    binding_mapping = cast(dict[str, object], binding_payload)
    binding = AgentExecutionBinding(
        runtime_ref=cast(str, binding_mapping["runtime_ref"]),
        runtime_config_hash=cast(str, binding_mapping["runtime_config_hash"]),
        prompt_hash=cast(str, binding_mapping["prompt_hash"]),
        skill_hashes=tuple(cast(list[str], binding_mapping["skill_hashes"])),
        tool_manifest_hashes=tuple(cast(list[str], binding_mapping["tool_manifest_hashes"])),
        tool_surface_hash=cast(str, binding_mapping["tool_surface_hash"]),
        mcp_server_hashes=tuple(cast(list[str], binding_mapping["mcp_server_hashes"])),
        context_estimator_id=cast(str, binding_mapping["context_estimator_id"]),
        compactor_id=cast(str, binding_mapping["compactor_id"]),
    )
    validate_judgment_execution_binding(
        judgment,
        run_id=first_run_id,
        repository=repository,
        provider_id=provider.provider_id,
        model=provider.model,
        expected_binding=binding,
        artifact_store=run_artifacts,
    )
    forged_judgment = JudgmentArtifact.build(
        run_id=judgment.run_id,
        evidence_pack_id=judgment.evidence_pack_id,
        provider_id=judgment.provider_id,
        model=judgment.model,
        runtime_config_hash=judgment.runtime_config_hash,
        prompt_hash="f" * 64,
        skill_hashes=judgment.skill_hashes,
        tool_manifest_hashes=judgment.tool_manifest_hashes,
        tool_surface_hash=judgment.tool_surface_hash,
        mcp_server_hashes=judgment.mcp_server_hashes,
        context_estimator_id=judgment.context_estimator_id,
        compactor_id=judgment.compactor_id,
        journal_hash=judgment.journal_hash,
        transcript_hash=judgment.transcript_hash,
        raw_response_hash=judgment.raw_response_hash,
        started_at=judgment.started_at,
        finished_at=judgment.finished_at,
        proposal=judgment.proposal,
    )
    with pytest.raises(ValueError, match="Judgment Artifact binding drifted"):
        validate_judgment_execution_binding(
            forged_judgment,
            run_id=first_run_id,
            repository=repository,
            provider_id=provider.provider_id,
            model=provider.model,
            expected_binding=binding,
            artifact_store=run_artifacts,
        )

    forged_proposal = JudgmentProposal(
        event_id=judgment.proposal.event_id,
        decision=JudgmentDecision.PROPOSE,
        summary="A forged but contract-valid proposal.",
        transmission_steps=(),
        candidates=(
            CandidateImpact(
                target_id="integrated-upstream-a",
                direction=CandidateDirection.DOWN,
                horizon_sessions=1,
                directness=TransmissionDirectness.DIRECT,
                confidence=0.7,
                thesis="A forged target thesis.",
                evidence_refs=("physical-loss",),
                counterevidence_refs=("shipment-mitigation",),
                invalidation_conditions=("physical supply is restored",),
            ),
        ),
        blockers=(),
        unresolved_questions=(),
        stopped_reason="forged proposal claims completion",
    )
    forged_terminal_judgment = JudgmentArtifact.build(
        run_id=judgment.run_id,
        evidence_pack_id=judgment.evidence_pack_id,
        provider_id=judgment.provider_id,
        model=judgment.model,
        runtime_config_hash=judgment.runtime_config_hash,
        prompt_hash=judgment.prompt_hash,
        skill_hashes=judgment.skill_hashes,
        tool_manifest_hashes=judgment.tool_manifest_hashes,
        tool_surface_hash=judgment.tool_surface_hash,
        mcp_server_hashes=judgment.mcp_server_hashes,
        context_estimator_id=judgment.context_estimator_id,
        compactor_id=judgment.compactor_id,
        journal_hash=judgment.journal_hash,
        transcript_hash=judgment.transcript_hash,
        raw_response_hash=judgment.raw_response_hash,
        started_at=judgment.started_at,
        finished_at=judgment.finished_at,
        proposal=forged_proposal,
    )
    forged_terminal = run_artifacts.put_json(forged_terminal_judgment.to_dict())
    run_database = experiment_root / "runs" / "general_control" / "replicate-1" / "run.sqlite3"
    with sqlite3.connect(run_database) as connection:
        connection.execute(
            "UPDATE runs SET terminal_artifact_id = ? WHERE run_id = ?",
            (forged_terminal.content_hash, first_run_id),
        )
    with pytest.raises(ValueError, match="proposal differs from the validation event"):
        audit_paired_execution_state(
            expected_evidence_pack=repository.evidence_pack,
            eligible_horizon_sessions=1,
            registration=registration,
            report=result,
            experiment_root=experiment_root,
            evidence_pack_path=RECOVERY / "evidence-pack-recovery.json",
            evidence_documents_path=RECOVERY / "evidence-documents-recovery.json",
            pattern_pack_paths=(RECOVERY / "pattern-pack.json",),
            provider_profile_path=PROFILE,
            skill_root=Path("skills"),
        )


def test_preparation_rejects_caller_evidence_labels_not_bound_to_the_bundle() -> None:
    false_context = MethodRoutingContext(
        market_state="unclassified",
        narrative_salience="authority_obvious",
        analysis_needs=("owner_value",),
        available_evidence=("cash_flow_or_earning_power", "valuation_or_price"),
        outcomes_opened=True,
    )

    with pytest.raises(ValueError, match="content-bound declaration"):
        prepare_paired_method_skill_ablation(
            method_catalog_path=CATALOG,
            method_evidence_declaration_path=DECLARATION,
            provider_profile_path=PROFILE,
            evidence_pack_path=RECOVERY / "evidence-pack-recovery.json",
            evidence_documents_path=RECOVERY / "evidence-documents-recovery.json",
            pattern_pack_paths=(RECOVERY / "pattern-pack.json",),
            experiment_id="false-evidence-labels",
            treatment_skill="owner-value-discipline",
            routing_context=false_context,
            skill_root=Path("skills"),
            max_total_cost_microusd=10_000_000,
            pricing=_cpa_pricing(),
            registered_at=datetime(2026, 8, 27, 10, 5, tzinfo=UTC),
        )
