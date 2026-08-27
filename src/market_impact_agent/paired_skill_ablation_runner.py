from __future__ import annotations

import asyncio
import json
import os
from collections import Counter
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Protocol, cast
from urllib.parse import quote

from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.agent_engine import (
    AgentEngine,
    AgentExecutionBinding,
    AgentRunRequest,
    AgentRunResult,
)
from market_impact_agent.agent_runtime import (
    LoadedSkill,
    ModelProvider,
    RuntimeConfig,
    SkillRegistry,
    ToolAccessContext,
    ToolRegistry,
    ToolSideEffect,
)
from market_impact_agent.frozen_research import FrozenResearchRepository
from market_impact_agent.method_skills import (
    CPAUsageKeeperPricing,
    MethodRoutingContext,
    MethodSkillRoute,
    MethodSkillRouter,
    PairedSkillAblationRegistration,
    estimate_paired_skill_ablation_cost,
    load_method_evidence_declaration,
    load_method_skill_catalog,
)
from market_impact_agent.model_provider import (
    ModelProviderFactory,
    ModelProviderProfile,
    load_model_provider_profile,
)
from market_impact_agent.openai_chat_provider import PinnedUrllibJsonTransport
from market_impact_agent.runtime_store import ArtifactStore, RunJournal, RunStatus
from market_impact_agent.usage_ledger import UsageLedger, UsageRecord

CPA_USAGE_KEEPER_ORIGIN = "http://127.0.0.1:8080"
RUNTIME_REF = "market-impact.agent-runtime.local-research.v2"
REPLICATE_COUNT = 3
SAFETY_MULTIPLIER = Decimal("1.25")
CONTROL_SKILLS = (
    "evidence-core",
    "research-discipline",
    "event-market-context",
    "equity-exposure",
    "adversarial-risk",
    "pattern-review",
)
ALLOWED_CAPABILITIES = frozenset({"evidence.read", "pattern.read"})
ALLOWED_TOOLS = frozenset({"read_evidence", "read_pattern_pack"})


class AvailableModelProvider(ModelProvider, Protocol):
    async def assert_model_available(self, *, timeout_seconds: float) -> None: ...


@dataclass(frozen=True, slots=True)
class SkillAblationArm:
    arm_id: str
    selected_skills: tuple[str, ...]


CONTROL_ARM = SkillAblationArm(arm_id="general_control", selected_skills=CONTROL_SKILLS)


class PairedReplicateRunner(Protocol):
    def __call__(
        self,
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
    ) -> Awaitable[AgentRunResult]: ...


@dataclass(frozen=True, slots=True)
class PreparedSkillAblation:
    profile: ModelProviderProfile
    repository: FrozenResearchRepository
    route: MethodSkillRoute
    registration: PairedSkillAblationRegistration
    arms: tuple[SkillAblationArm, SkillAblationArm]
    research_instruction: str


def fetch_cpa_usage_keeper_pricing(
    *,
    model: str,
    captured_at: datetime,
) -> CPAUsageKeeperPricing:
    transport = PinnedUrllibJsonTransport(
        allowed_origin=CPA_USAGE_KEEPER_ORIGIN,
        provider_label="CPA Usage Keeper",
    )
    version = transport.request_json(
        method="GET",
        url=f"{CPA_USAGE_KEEPER_ORIGIN}/api/v1/version",
        headers={},
        payload=None,
        timeout_seconds=5.0,
    )
    pricing = transport.request_json(
        method="GET",
        url=f"{CPA_USAGE_KEEPER_ORIGIN}/api/v1/pricing",
        headers={},
        payload=None,
        timeout_seconds=5.0,
    )
    rules = transport.request_json(
        method="GET",
        url=(f"{CPA_USAGE_KEEPER_ORIGIN}/api/v1/pricing/rules?model={quote(model, safe='')}"),
        headers={},
        payload=None,
        timeout_seconds=5.0,
    )
    return CPAUsageKeeperPricing.from_api_payloads(
        model=model,
        captured_at=captured_at,
        version_payload=version,
        pricing_payload=pricing,
        rules_payload=rules,
    )


def prepare_paired_method_skill_ablation(
    *,
    method_catalog_path: Path,
    method_evidence_declaration_path: Path,
    provider_profile_path: Path,
    evidence_pack_path: Path,
    evidence_documents_path: Path,
    pattern_pack_paths: tuple[Path, ...],
    experiment_id: str,
    treatment_skill: str,
    routing_context: MethodRoutingContext,
    skill_root: Path,
    max_total_cost_microusd: int,
    pricing: CPAUsageKeeperPricing,
    registered_at: datetime,
    eligible_horizon_sessions: int = 1,
) -> PreparedSkillAblation:
    if not experiment_id or experiment_id != experiment_id.strip():
        raise ValueError("paired Skill ablation experiment_id must be non-empty and trimmed")
    catalog = load_method_skill_catalog(method_catalog_path)
    evidence_declaration = load_method_evidence_declaration(method_evidence_declaration_path)
    profile = load_model_provider_profile(provider_profile_path)
    repository = FrozenResearchRepository.from_files(
        evidence_pack_path=evidence_pack_path,
        evidence_documents_path=evidence_documents_path,
        pattern_pack_paths=pattern_pack_paths,
    )
    if not repository.evidence_pack.pattern_packs:
        raise ValueError("paired Skill ablation requires a frozen Pattern Pack")
    evidence_pack_hash = canonical_hash(repository.evidence_pack.to_dict())
    evidence_declaration.validate_against(
        evidence_pack_id=repository.evidence_pack.pack_id,
        evidence_pack_hash=evidence_pack_hash,
        evidence_ids=frozenset(item.evidence_id for item in repository.evidence_pack.evidence),
        pattern_pack_ids=frozenset(item.pack_id for item in repository.evidence_pack.pattern_packs),
        outcomes_opened=routing_context.outcomes_opened,
    )
    if routing_context.available_evidence != evidence_declaration.available_evidence:
        raise ValueError("method routing evidence must exactly match the content-bound declaration")
    route = MethodSkillRouter(catalog).route(routing_context)
    if treatment_skill not in route.selected_skills:
        raise ValueError("treatment Skill is not selected by the frozen point-in-time route")
    treatment_arm = SkillAblationArm(
        arm_id=f"general_plus_{treatment_skill.replace('-', '_')}",
        selected_skills=(*CONTROL_SKILLS, treatment_skill),
    )
    registry = SkillRegistry(skill_root)
    control_loaded = registry.load(
        CONTROL_ARM.selected_skills,
        allowed_capabilities=ALLOWED_CAPABILITIES,
    )
    treatment_loaded = registry.load(
        treatment_arm.selected_skills,
        allowed_capabilities=ALLOWED_CAPABILITIES,
    )
    if tuple(item.manifest.name for item in control_loaded) != CONTROL_ARM.selected_skills:
        raise ValueError("control Skill dependency closure drifted")
    if tuple(item.manifest.name for item in treatment_loaded) != treatment_arm.selected_skills:
        raise ValueError("treatment Skill dependency closure drifted")
    _validate_skill_surfaces(control_loaded, treatment_loaded, treatment_skill)
    cost_estimate = estimate_paired_skill_ablation_cost(
        pricing=pricing,
        profile=profile,
        replicate_count=REPLICATE_COUNT,
        arm_count=2,
        safety_multiplier=SAFETY_MULTIPLIER,
        max_total_cost_microusd=max_total_cost_microusd,
    )
    instruction = _common_research_instruction(
        repository,
        eligible_horizon_sessions=eligible_horizon_sessions,
    )
    common_input_hash = canonical_hash(
        {
            "runtime_ref": RUNTIME_REF,
            "evidence_pack": repository.evidence_pack.to_dict(),
            "research_instruction": instruction,
            "method_evidence_declaration": evidence_declaration.to_dict(),
            "allowed_capabilities": sorted(ALLOWED_CAPABILITIES),
            "allowed_side_effects": [ToolSideEffect.READ_ONLY.value],
            "allowed_tools": sorted(ALLOWED_TOOLS),
        }
    )
    registration = PairedSkillAblationRegistration.build(
        experiment_id=experiment_id,
        registered_at=registered_at,
        provider_profile_id=profile.profile_id,
        provider_profile_hash=profile.profile_hash,
        method_catalog_id=catalog.catalog_id,
        method_evidence_declaration_id=evidence_declaration.declaration_id,
        method_evidence_declaration_hash=evidence_declaration.declaration_hash,
        evidence_pack_id=repository.evidence_pack.pack_id,
        evidence_pack_hash=evidence_pack_hash,
        control_skills=CONTROL_ARM.selected_skills,
        treatment_skills=treatment_arm.selected_skills,
        control_manifest_hashes=tuple(item.manifest.manifest_hash for item in control_loaded),
        treatment_manifest_hashes=tuple(item.manifest.manifest_hash for item in treatment_loaded),
        method_route_id=route.route_id,
        routing_context=routing_context,
        replicate_count=REPLICATE_COUNT,
        common_input_hash=common_input_hash,
        pricing=pricing,
        cost_estimate=cost_estimate,
        outcomes_opened=routing_context.outcomes_opened,
    )
    return PreparedSkillAblation(
        profile=profile,
        repository=repository,
        route=route,
        registration=registration,
        arms=(CONTROL_ARM, treatment_arm),
        research_instruction=instruction,
    )


async def run_paired_method_skill_ablation(
    *,
    method_catalog_path: Path,
    method_evidence_declaration_path: Path,
    provider_profile_path: Path,
    evidence_pack_path: Path,
    evidence_documents_path: Path,
    pattern_pack_paths: tuple[Path, ...],
    experiment_id: str,
    treatment_skill: str,
    routing_context: MethodRoutingContext,
    skill_root: Path,
    state_root: Path,
    max_total_cost_microusd: int = 10_000_000,
    pricing: CPAUsageKeeperPricing | None = None,
    provider: AvailableModelProvider | None = None,
    replicate_runner: PairedReplicateRunner | None = None,
    clock: Callable[[], datetime] | None = None,
    eligible_horizon_sessions: int = 1,
) -> dict[str, object]:
    now = clock or (lambda: datetime.now(UTC))
    profile = load_model_provider_profile(provider_profile_path)
    active_pricing = pricing or fetch_cpa_usage_keeper_pricing(
        model=profile.model,
        captured_at=now(),
    )
    prepared = prepare_paired_method_skill_ablation(
        method_catalog_path=method_catalog_path,
        method_evidence_declaration_path=method_evidence_declaration_path,
        provider_profile_path=provider_profile_path,
        evidence_pack_path=evidence_pack_path,
        evidence_documents_path=evidence_documents_path,
        pattern_pack_paths=pattern_pack_paths,
        experiment_id=experiment_id,
        treatment_skill=treatment_skill,
        routing_context=routing_context,
        skill_root=skill_root,
        max_total_cost_microusd=max_total_cost_microusd,
        pricing=active_pricing,
        registered_at=now(),
        eligible_horizon_sessions=eligible_horizon_sessions,
    )
    experiment_root = state_root / canonical_hash(experiment_id)
    if experiment_root.exists():
        raise ValueError("paired Skill ablation experiment id already exists")
    _write_json(experiment_root / "registration.json", prepared.registration.to_dict())
    selected_provider = provider or cast(
        AvailableModelProvider,
        ModelProviderFactory.with_builtin_adapters().create(prepared.profile),
    )
    if (
        selected_provider.provider_id != prepared.profile.provider_id
        or selected_provider.model != prepared.profile.model
    ):
        raise ValueError("active Model Provider does not match the frozen paired ablation")
    config = prepared.profile.runtime_config()
    secret_values = (os.environ.get(prepared.profile.credential_env, ""),)
    artifacts = ArtifactStore(experiment_root / "artifacts")
    ledger = UsageLedger(experiment_root / "usage.sqlite3")
    bindings = _freeze_bindings(
        prepared=prepared,
        provider=selected_provider,
        config=config,
        experiment_root=experiment_root,
        artifacts=artifacts,
        skill_root=skill_root,
        secret_values=secret_values,
    )
    await selected_provider.assert_model_available(timeout_seconds=30)
    runner = replicate_runner or _run_replicate
    results: dict[str, list[AgentRunResult]] = {arm.arm_id: [] for arm in prepared.arms}
    for replicate_index in range(1, REPLICATE_COUNT + 1):
        paired = await asyncio.gather(
            *(
                runner(
                    repository=prepared.repository,
                    provider=selected_provider,
                    config=config,
                    arm=arm,
                    research_instruction=prepared.research_instruction,
                    run_id=(f"{experiment_id}.{arm.arm_id}.replicate-{replicate_index}"),
                    skill_root=skill_root,
                    state_directory=(
                        experiment_root / "runs" / arm.arm_id / f"replicate-{replicate_index}"
                    ),
                    secret_values=secret_values,
                )
                for arm in prepared.arms
            )
        )
        for arm, result in zip(prepared.arms, paired, strict=True):
            results[arm.arm_id].append(result)
            run_directory = experiment_root / "runs" / arm.arm_id / f"replicate-{replicate_index}"
            journal = RunJournal(run_directory / "run.sqlite3")
            ledger.append(
                UsageRecord.from_result(
                    experiment_id=experiment_id,
                    arm_id=arm.arm_id,
                    recorded_at=journal.get_run(result.run_id).updated_at,
                    provider_profile_id=prepared.profile.profile_id,
                    provider_profile_hash=prepared.profile.profile_hash,
                    execution_binding_hash=bindings[arm.arm_id].binding_hash,
                    run_journal_hash=journal.journal_hash(result.run_id),
                    result=result,
                )
            )
    arm_reports = [
        _arm_report(
            arm=arm,
            binding=bindings[arm.arm_id],
            results=tuple(results[arm.arm_id]),
            run_directories=tuple(
                experiment_root / "runs" / arm.arm_id / f"replicate-{index}"
                for index in range(1, REPLICATE_COUNT + 1)
            ),
            repository=prepared.repository,
        )
        for arm in prepared.arms
    ]
    actual_cost = sum(
        result.metrics.estimated_cost_microusd
        for arm_results in results.values()
        for result in arm_results
        if result.metrics is not None
    )
    provider_request_count = sum(
        result.metrics.provider_attempts
        for arm_results in results.values()
        for result in arm_results
        if result.metrics is not None
    )
    if actual_cost > max_total_cost_microusd:
        raise RuntimeError("paired Skill ablation actual ledger cost exceeded the hard cap")
    diagnostic_valid = all(
        result.status is RunStatus.COMPLETED and result.judgment is not None
        for arm_results in results.values()
        for result in arm_results
    )
    core: dict[str, object] = {
        "schema_version": "market-impact.method-skill-ablation-report.v2",
        "experiment_id": experiment_id,
        "registration_id": prepared.registration.registration_id,
        "registration_hash": prepared.registration.registration_hash,
        "provider_profile_id": prepared.profile.profile_id,
        "provider_profile_hash": prepared.profile.profile_hash,
        "method_route": prepared.route.to_dict(),
        "only_treatment_difference": prepared.registration.added_treatment_skill,
        "replicate_count": REPLICATE_COUNT,
        "arms": arm_reports,
        "cost": {
            "cpa_pricing_snapshot_hash": active_pricing.snapshot_hash,
            "preflight": prepared.registration.cost_estimate.to_dict(),
            "provider_request_count": provider_request_count,
            "ledger_actual_microusd": actual_cost,
            "hard_cap_microusd": max_total_cost_microusd,
        },
        "usage_ledger_hash": ledger.ledger_hash,
        "diagnostic_valid": diagnostic_valid,
        "outcomes_visible_to_agent": False,
        "outcomes_known_to_builder": routing_context.outcomes_opened,
        "identity_masked": True,
        "inference_eligible": False,
        "claim_scope": "opened_development_process_diagnostic_only",
        "broker_reachability": False,
        "execution_capability": "none",
    }
    report = {
        **core,
        "report_id": f"method-skill-ablation-report-{canonical_hash(core)}",
    }
    _write_json(experiment_root / "report.json", report)
    return {**report, "state_directory": experiment_root.as_posix()}


def _validate_skill_surfaces(
    control_loaded: tuple[LoadedSkill, ...],
    treatment_loaded: tuple[LoadedSkill, ...],
    treatment_skill: str,
) -> None:
    control_manifests = [item.manifest for item in control_loaded]
    treatment_manifests = [item.manifest for item in treatment_loaded]
    if treatment_manifests[-1].name != treatment_skill:
        raise ValueError("treatment method Skill must be the only appended Skill")
    if [item.name for item in treatment_manifests[:-1]] != [
        item.name for item in control_manifests
    ]:
        raise ValueError("treatment Skill surface changed the control prefix")
    for manifest in (*control_manifests, treatment_manifests[-1]):
        if manifest.allowed_mcp_servers:
            raise ValueError("paired Skill ablation cannot expose MCP servers")
        if not manifest.allowed_tools <= ALLOWED_TOOLS:
            raise ValueError("paired Skill ablation Skill requests an unsupported tool")


def _freeze_bindings(
    *,
    prepared: PreparedSkillAblation,
    provider: ModelProvider,
    config: RuntimeConfig,
    experiment_root: Path,
    artifacts: ArtifactStore,
    skill_root: Path,
    secret_values: tuple[str, ...],
) -> dict[str, AgentExecutionBinding]:
    bindings: dict[str, AgentExecutionBinding] = {}
    for arm in prepared.arms:
        engine = _engine(
            repository=prepared.repository,
            provider=provider,
            config=config,
            skill_root=skill_root,
            artifact_store=artifacts,
            journal=RunJournal(experiment_root / f"binding-{arm.arm_id}.sqlite3"),
            secret_values=secret_values,
        )
        binding = engine.execution_binding(
            _request(
                run_id=f"{prepared.registration.experiment_id}.{arm.arm_id}.binding-preflight",
                repository=prepared.repository,
                arm=arm,
                research_instruction=prepared.research_instruction,
            ),
            runtime_ref=RUNTIME_REF,
        )
        stored = artifacts.put_json(
            binding.to_dict(),
            media_type="application/vnd.market-impact.agent-execution-binding+json",
        )
        if stored.content_hash != binding.binding_hash:
            raise AssertionError("paired Skill execution binding hash is inconsistent")
        bindings[arm.arm_id] = binding
    if (
        bindings[CONTROL_ARM.arm_id].tool_surface_hash
        != bindings[prepared.arms[1].arm_id].tool_surface_hash
    ):
        raise ValueError("paired Skill ablation tool surfaces differ across arms")
    return bindings


async def _run_replicate(
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
    engine = _engine(
        repository=repository,
        provider=provider,
        config=config,
        skill_root=skill_root,
        artifact_store=ArtifactStore(state_directory / "artifacts"),
        journal=RunJournal(state_directory / "run.sqlite3"),
        secret_values=secret_values,
    )
    return await engine.run(
        _request(
            run_id=run_id,
            repository=repository,
            arm=arm,
            research_instruction=research_instruction,
        )
    )


def _engine(
    *,
    repository: FrozenResearchRepository,
    provider: ModelProvider,
    config: RuntimeConfig,
    skill_root: Path,
    artifact_store: ArtifactStore,
    journal: RunJournal,
    secret_values: tuple[str, ...],
) -> AgentEngine:
    tools = ToolRegistry(artifact_store)
    for descriptor in repository.tool_descriptors():
        tools.register(descriptor)
    return AgentEngine(
        provider=provider,
        config=config,
        artifact_store=artifact_store,
        journal=journal,
        tool_registry=tools,
        skill_registry=SkillRegistry(skill_root),
        secret_values=secret_values,
    )


def _request(
    *,
    run_id: str,
    repository: FrozenResearchRepository,
    arm: SkillAblationArm,
    research_instruction: str,
) -> AgentRunRequest:
    return AgentRunRequest(
        run_id=run_id,
        evidence_pack=repository.evidence_pack,
        research_instruction=research_instruction,
        selected_skills=arm.selected_skills,
        tool_access=ToolAccessContext(
            allowed_capabilities=ALLOWED_CAPABILITIES,
            allowed_side_effects=frozenset({ToolSideEffect.READ_ONLY}),
            allowed_tools=ALLOWED_TOOLS,
        ),
    )


def _common_research_instruction(
    repository: FrozenResearchRepository,
    *,
    eligible_horizon_sessions: int = 1,
) -> str:
    if eligible_horizon_sessions < 1:
        raise ValueError("eligible_horizon_sessions must be positive")
    targets = ", ".join(repository.evidence_pack.allowed_targets)
    event_id = repository.evidence_pack.event_id
    horizon_label = (
        "one trading session"
        if eligible_horizon_sessions == 1
        else f"{eligible_horizon_sessions} trading sessions"
    )
    return (
        "Assess this identity-masked, opened development information state without using "
        "information outside the Evidence Pack. Read every Evidence Item and the complete "
        "Pattern Pack before deciding. Use only the registered read-only tools and selected "
        "research methods. Test material counterevidence and abstain when the event-to-target "
        "link or persistence over the registered horizon is unresolved. Do not infer the "
        "historical identity "
        "or use memorized outcomes. The only eligible targets are "
        f"[{targets}], the only direction is up, and the only horizon is {horizon_label}. "
        f"The proposal event_id is [{event_id}]; copy that exact event_id into the output and "
        "never replace it with an inferred identity or description. "
        "A candidate requires confidence at least 0.5. Return exactly one eligible candidate "
        "or abstain."
    )


def _arm_report(
    *,
    arm: SkillAblationArm,
    binding: AgentExecutionBinding,
    results: tuple[AgentRunResult, ...],
    run_directories: tuple[Path, ...],
    repository: FrozenResearchRepository,
) -> dict[str, object]:
    summaries = tuple(_run_summary(item) for item in results)
    metrics = tuple(item.metrics for item in results if item.metrics is not None)
    expected_evidence = {item.evidence_id for item in repository.evidence_pack.evidence}
    expected_patterns = {item.pack_id for item in repository.evidence_pack.pattern_packs}
    coverage_rows: list[dict[str, object]] = []
    for result, run_directory in zip(results, run_directories, strict=True):
        evidence_reads, pattern_reads = _read_tool_targets(
            RunJournal(run_directory / "run.sqlite3"), result.run_id
        )
        coverage_rows.append(
            {
                "run_id": result.run_id,
                "evidence_reads": list(evidence_reads),
                "pattern_reads": list(pattern_reads),
                "evidence_coverage_complete": set(evidence_reads) == expected_evidence,
                "pattern_coverage_complete": set(pattern_reads) == expected_patterns,
            }
        )
    return {
        "arm_id": arm.arm_id,
        "selected_skills": list(arm.selected_skills),
        "execution_binding_hash": binding.binding_hash,
        "run_statuses": [item.status.value for item in results],
        "runs": list(summaries),
        "decision_counts": dict(Counter(str(item["decision"]) for item in summaries)),
        "coverage": coverage_rows,
        "totals": {
            "turns": sum(item.turns for item in metrics),
            "tool_calls": sum(item.tool_calls for item in metrics),
            "input_tokens": sum(item.input_tokens for item in metrics),
            "output_tokens": sum(item.output_tokens for item in metrics),
            "result_bytes": sum(item.result_bytes for item in metrics),
            "latency_ms": sum(item.latency_ms for item in metrics),
            "provider_attempts": sum(item.provider_attempts for item in metrics),
            "estimated_cost_microusd": sum(item.estimated_cost_microusd for item in metrics),
        },
    }


def _run_summary(result: AgentRunResult) -> dict[str, object]:
    if result.status is not RunStatus.COMPLETED or result.judgment is None:
        return {"run_id": result.run_id, "status": result.status.value, "decision": "invalid"}
    proposal = result.judgment.proposal
    return {
        "run_id": result.run_id,
        "status": result.status.value,
        "decision": proposal.decision.value,
        "summary": proposal.summary,
        "blockers": list(proposal.blockers),
        "unresolved_questions": list(proposal.unresolved_questions),
        "candidates": [item.to_dict() for item in proposal.candidates],
        "artifact_id": result.judgment.artifact_id,
        "artifact_hash": canonical_hash(result.judgment.to_dict()),
        "metrics": None if result.metrics is None else result.metrics.to_dict(),
    }


def _read_tool_targets(journal: RunJournal, run_id: str) -> tuple[tuple[str, ...], tuple[str, ...]]:
    evidence: list[str] = []
    patterns: list[str] = []
    for event in journal.events(run_id):
        if event.event_type != "tool.call.completed":
            continue
        tool_name = event.payload.get("tool_name")
        if tool_name not in ALLOWED_TOOLS:
            continue
        raw_content = event.payload.get("model_content")
        if not isinstance(raw_content, str):
            raise ValueError("stored research tool result is missing model_content")
        envelope = json.loads(raw_content)
        if not isinstance(envelope, dict):
            raise ValueError("stored research tool result is invalid")
        raw_result = cast(dict[str, object], envelope).get("result")
        if not isinstance(raw_result, dict):
            raise ValueError("stored research tool result is invalid")
        payload = cast(dict[str, object], raw_result)
        if tool_name == "read_evidence":
            reference = payload.get("reference")
            if not isinstance(reference, dict):
                raise ValueError("stored evidence tool result lacks evidence_id")
            evidence_id = cast(dict[str, object], reference).get("evidence_id")
            if not isinstance(evidence_id, str):
                raise ValueError("stored evidence tool result lacks evidence_id")
            evidence.append(evidence_id)
        else:
            pack_id = payload.get("pack_id")
            if not isinstance(pack_id, str):
                raise ValueError("stored Pattern Pack tool result lacks pack_id")
            patterns.append(pack_id)
    return tuple(evidence), tuple(patterns)


def _write_json(path: Path, value: object) -> None:
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)
