from __future__ import annotations

import asyncio
import json
import os
from collections.abc import Awaitable
from pathlib import Path
from typing import Protocol, cast

from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.agent_engine import (
    AgentEngine,
    AgentExecutionBinding,
    AgentRunRequest,
    AgentRunResult,
)
from market_impact_agent.agent_ensemble import (
    AgentStudyRegistration,
    aggregate_agent_replicates,
)
from market_impact_agent.agent_runtime import (
    ModelProvider,
    RuntimeConfig,
    SkillRegistry,
    ToolAccessContext,
    ToolRegistry,
    ToolSideEffect,
)
from market_impact_agent.agent_study import ExposureRegistry, load_agent_phase2_preregistration
from market_impact_agent.frozen_research import FrozenResearchRepository
from market_impact_agent.model_provider import (
    ModelProviderFactory,
    load_model_provider_profile,
)
from market_impact_agent.research_methods import (
    AblationArmStudy,
    MethodAblationRegistration,
    MethodArmSpec,
    ResearchContext,
    ResearchMethodCatalog,
    ResearchMethodRouter,
    build_arm_studies,
    load_method_ablation_registration,
    load_research_method_catalog,
)
from market_impact_agent.runtime_store import ArtifactStore, RunJournal
from market_impact_agent.usage_ledger import UsageLedger, UsageRecord


class AvailableModelProvider(ModelProvider, Protocol):
    async def assert_model_available(self, *, timeout_seconds: float) -> None: ...


class MethodReplicateRunner(Protocol):
    def __call__(
        self,
        *,
        repository: FrozenResearchRepository,
        provider: ModelProvider,
        config: RuntimeConfig,
        study: AblationArmStudy,
        research_instruction: str,
        run_id: str,
        skill_root: Path,
        state_directory: Path,
        secret_values: tuple[str, ...],
    ) -> Awaitable[AgentRunResult]: ...


async def run_method_ablation_bundle(
    *,
    ablation_registration_path: Path,
    parent_registration_path: Path,
    exposure_registry_path: Path,
    method_catalog_path: Path,
    provider_profile_path: Path,
    evidence_pack_path: Path,
    evidence_documents_path: Path,
    pattern_pack_paths: tuple[Path, ...],
    experiment_id: str,
    skill_root: Path,
    state_root: Path,
    provider: AvailableModelProvider | None = None,
    replicate_runner: MethodReplicateRunner | None = None,
) -> dict[str, object]:
    parent, registry = load_agent_phase2_preregistration(
        parent_registration_path,
        exposure_registry_path,
    )
    catalog = load_research_method_catalog(method_catalog_path)
    profile = load_model_provider_profile(provider_profile_path)
    ablation = load_method_ablation_registration(ablation_registration_path)
    ablation.validate_against(
        parent=parent,
        registry=registry,
        catalog=catalog,
        provider_profile_id=profile.profile_id,
        provider_profile_hash=profile.profile_hash,
    )
    repository = FrozenResearchRepository.from_files(
        evidence_pack_path=evidence_pack_path,
        evidence_documents_path=evidence_documents_path,
        pattern_pack_paths=pattern_pack_paths,
    )
    _validate_evidence_targets(repository, registry)
    _validate_frozen_routes(
        ablation=ablation,
        catalog=catalog,
        skill_root=skill_root,
        has_pattern_pack=bool(repository.evidence_pack.pattern_packs),
    )
    selected_provider = provider or cast(
        AvailableModelProvider,
        ModelProviderFactory.with_builtin_adapters().create(profile),
    )
    if (
        selected_provider.provider_id != profile.provider_id
        or selected_provider.model != profile.model
        or parent.agent_protocol.provider_id != profile.provider_id
        or parent.agent_protocol.model != profile.model
    ):
        raise ValueError("active Model Provider does not match the frozen ablation")
    config = profile.runtime_config()
    instruction = _common_research_instruction(parent, repository)
    experiment_root = state_root / canonical_hash(experiment_id)
    artifact_store = ArtifactStore(experiment_root / "artifacts")
    usage_ledger = UsageLedger(experiment_root / "usage.sqlite3")
    studies = build_arm_studies(ablation=ablation, parent=parent)
    secret_values = (os.environ.get(profile.credential_env, ""),)

    bindings: dict[str, AgentExecutionBinding] = {}
    for study in studies:
        binding = _freeze_execution_binding(
            repository=repository,
            provider=selected_provider,
            config=config,
            study=study,
            research_instruction=instruction,
            experiment_id=experiment_id,
            skill_root=skill_root,
            state_directory=experiment_root,
            artifact_store=artifact_store,
            secret_values=secret_values,
        )
        stored = artifact_store.put_json(
            binding.to_dict(),
            media_type="application/vnd.market-impact.agent-execution-binding+json",
        )
        if stored.content_hash != binding.binding_hash:
            raise AssertionError("frozen method-arm execution binding is inconsistent")
        bindings[study.arm.arm.value] = binding

    await selected_provider.assert_model_available(timeout_seconds=30)
    runner = replicate_runner or _run_replicate
    results: dict[str, list[AgentRunResult]] = {study.arm.arm.value: [] for study in studies}
    for replicate_index in range(1, ablation.replicate_count + 1):
        round_results = await asyncio.gather(
            *(
                runner(
                    repository=repository,
                    provider=selected_provider,
                    config=config,
                    study=study,
                    research_instruction=instruction,
                    run_id=(f"{experiment_id}.{study.arm.arm.value}.replicate-{replicate_index}"),
                    skill_root=skill_root,
                    state_directory=(
                        experiment_root
                        / "runs"
                        / study.arm.arm.value
                        / f"replicate-{replicate_index}"
                    ),
                    secret_values=secret_values,
                )
                for study in studies
            )
        )
        for study, result in zip(studies, round_results, strict=True):
            arm_id = study.arm.arm.value
            results[arm_id].append(result)
            journal = RunJournal(
                experiment_root / "runs" / arm_id / f"replicate-{replicate_index}" / "run.sqlite3"
            )
            usage_ledger.append(
                UsageRecord.from_result(
                    experiment_id=experiment_id,
                    arm_id=arm_id,
                    recorded_at=journal.get_run(result.run_id).updated_at,
                    provider_profile_id=profile.profile_id,
                    provider_profile_hash=profile.profile_hash,
                    execution_binding_hash=bindings[arm_id].binding_hash,
                    run_journal_hash=journal.journal_hash(result.run_id),
                    result=result,
                )
            )

    arm_reports: list[dict[str, object]] = []
    for study in studies:
        arm_id = study.arm.arm.value
        arm_results = tuple(results[arm_id])
        decision = aggregate_agent_replicates(
            ensemble_run_id=f"{experiment_id}.{arm_id}",
            registration=cast(AgentStudyRegistration, study),
            evidence_pack=repository.evidence_pack,
            results=arm_results,
            frozen_execution_binding_hash=bindings[arm_id].binding_hash,
        )
        decision.validate_against(
            cast(AgentStudyRegistration, study), repository.evidence_pack, registry
        )
        stored_decision = artifact_store.put_json(
            decision.to_dict(),
            media_type="application/vnd.market-impact.agent-ensemble-decision+json",
        )
        arm_reports.append(
            _arm_report(
                study=study,
                binding=bindings[arm_id],
                results=arm_results,
                run_directories=tuple(
                    experiment_root / "runs" / arm_id / f"replicate-{index}"
                    for index in range(1, ablation.replicate_count + 1)
                ),
                repository=repository,
                decision=decision.to_dict(),
                decision_artifact_hash=stored_decision.content_hash,
            )
        )
    report_core = {
        "schema_version": "market-impact.method-ablation-report.v1",
        "experiment_id": experiment_id,
        "ablation_registration_id": ablation.registration_id,
        "ablation_registration_hash": ablation.registration_hash,
        "provider_profile_id": profile.profile_id,
        "provider_profile_hash": profile.profile_hash,
        "evidence_pack_id": repository.evidence_pack.pack_id,
        "evidence_pack_hash": canonical_hash(repository.evidence_pack.to_dict()),
        "run_order": ablation.run_order,
        "arms": arm_reports,
        "usage_ledger_hash": usage_ledger.ledger_hash,
        "market_outcomes_used": False,
        "quality_claim": "process_only_no_alpha_inference",
        "broker_reachability": False,
        "execution_capability": "none",
    }
    report = {**report_core, "report_id": f"method-ablation-report-{canonical_hash(report_core)}"}
    stored_report = artifact_store.put_json(
        report,
        media_type="application/vnd.market-impact.method-ablation-report+json",
    )
    return {
        **report,
        "report_artifact_hash": stored_report.content_hash,
        "state_directory": experiment_root.as_posix(),
    }


async def _run_replicate(
    *,
    repository: FrozenResearchRepository,
    provider: ModelProvider,
    config: RuntimeConfig,
    study: AblationArmStudy,
    research_instruction: str,
    run_id: str,
    skill_root: Path,
    state_directory: Path,
    secret_values: tuple[str, ...],
) -> AgentRunResult:
    artifact_store = ArtifactStore(state_directory / "artifacts")
    engine = _engine(
        repository=repository,
        provider=provider,
        config=config,
        skill_root=skill_root,
        artifact_store=artifact_store,
        journal=RunJournal(state_directory / "run.sqlite3"),
        secret_values=secret_values,
    )
    return await engine.run(
        _run_request(
            run_id=run_id,
            repository=repository,
            study=study,
            research_instruction=research_instruction,
        )
    )


def _freeze_execution_binding(
    *,
    repository: FrozenResearchRepository,
    provider: ModelProvider,
    config: RuntimeConfig,
    study: AblationArmStudy,
    research_instruction: str,
    experiment_id: str,
    skill_root: Path,
    state_directory: Path,
    artifact_store: ArtifactStore,
    secret_values: tuple[str, ...],
) -> AgentExecutionBinding:
    engine = _engine(
        repository=repository,
        provider=provider,
        config=config,
        skill_root=skill_root,
        artifact_store=artifact_store,
        journal=RunJournal(state_directory / f"binding-{study.arm.arm.value}.sqlite3"),
        secret_values=secret_values,
    )
    return engine.execution_binding(
        _run_request(
            run_id=f"{experiment_id}.{study.arm.arm.value}.binding-preflight",
            repository=repository,
            study=study,
            research_instruction=research_instruction,
        ),
        runtime_ref=study.agent_protocol.runtime_ref,
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


def _run_request(
    *,
    run_id: str,
    repository: FrozenResearchRepository,
    study: AblationArmStudy,
    research_instruction: str,
) -> AgentRunRequest:
    return AgentRunRequest(
        run_id=run_id,
        evidence_pack=repository.evidence_pack,
        research_instruction=research_instruction,
        selected_skills=study.arm.requested_skills,
        tool_access=ToolAccessContext(
            allowed_capabilities=frozenset(study.arm.allowed_capabilities),
            allowed_side_effects=frozenset({ToolSideEffect.READ_ONLY}),
            allowed_tools=frozenset(study.arm.allowed_tools),
        ),
    )


def _common_research_instruction(parent: object, repository: FrozenResearchRepository) -> str:
    protocol = cast(AgentStudyRegistration, parent).agent_protocol
    targets = ", ".join(repository.evidence_pack.allowed_targets)
    horizons = ", ".join(str(item) for item in protocol.eligible_horizons_sessions)
    return (
        "Assess the frozen event without using information outside this Evidence Pack. "
        "Read every Evidence Pack evidence item before deciding. Use only the registered "
        "tools and selected research methods, test material counterevidence, and abstain "
        "when a critical link is unresolved. "
        f"Eligible targets are [{targets}], eligible direction is up, and eligible "
        f"horizons in sessions are [{horizons}]. A candidate requires confidence at least "
        f"{protocol.minimum_candidate_confidence}. Return exactly one eligible candidate "
        "or abstain; if multiple candidates remain eligible, abstain."
    )


def _validate_evidence_targets(
    repository: FrozenResearchRepository, registry: ExposureRegistry
) -> None:
    eligible = {item.instrument_id for item in registry.entries if item.selection_eligible}
    outside = sorted(set(repository.evidence_pack.allowed_targets) - eligible)
    if outside:
        raise ValueError(
            "Evidence Pack contains targets outside the frozen Exposure Registry: "
            + ", ".join(outside)
        )


def _validate_frozen_routes(
    *,
    ablation: MethodAblationRegistration,
    catalog: ResearchMethodCatalog,
    skill_root: Path,
    has_pattern_pack: bool,
) -> None:
    router = ResearchMethodRouter(catalog=catalog, skills=SkillRegistry(skill_root))
    context = ResearchContext(
        mechanism_family="physical_energy_supply_shock",
        asset_class="public_equity",
        has_pattern_pack=has_pattern_pack,
    )
    for spec in ablation.arms:
        route = router.route(arm=spec.arm, context=context)
        if MethodArmSpec.from_route(route) != spec:
            raise ValueError(f"frozen Method Arm route drifted: {spec.arm.value}")


def _arm_report(
    *,
    study: AblationArmStudy,
    binding: AgentExecutionBinding,
    results: tuple[AgentRunResult, ...],
    run_directories: tuple[Path, ...],
    repository: FrozenResearchRepository,
    decision: dict[str, object],
    decision_artifact_hash: str,
) -> dict[str, object]:
    metrics = tuple(item.metrics for item in results if item.metrics is not None)
    return {
        "arm": study.arm.arm.value,
        "route_id": study.arm.route_id,
        "requested_skills": list(study.arm.requested_skills),
        "execution_binding_hash": binding.binding_hash,
        "decision": decision,
        "decision_artifact_hash": decision_artifact_hash,
        "run_statuses": [item.status.value for item in results],
        "process_diagnostics": _process_diagnostics(
            study=study,
            results=results,
            run_directories=run_directories,
            repository=repository,
        ),
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


def _process_diagnostics(
    *,
    study: AblationArmStudy,
    results: tuple[AgentRunResult, ...],
    run_directories: tuple[Path, ...],
    repository: FrozenResearchRepository,
) -> dict[str, object]:
    if len(results) != len(run_directories):
        raise ValueError("method diagnostics run directories do not match results")
    expected_evidence = tuple(item.evidence_id for item in repository.evidence_pack.evidence)
    expected_patterns = (
        tuple(item.pack_id for item in repository.evidence_pack.pattern_packs)
        if "read_pattern_pack" in study.arm.allowed_tools
        else ()
    )
    pattern_coverage_applicable = "read_pattern_pack" in study.arm.allowed_tools
    rows: list[dict[str, object]] = []
    for result, state_directory in zip(results, run_directories, strict=True):
        journal = RunJournal(state_directory / "run.sqlite3")
        evidence_reads, pattern_reads = _read_tool_targets(journal, result.run_id)
        proposal = None if result.judgment is None else result.judgment.proposal
        candidate = (
            proposal.candidates[0]
            if proposal is not None and len(proposal.candidates) == 1
            else None
        )
        rows.append(
            {
                "run_id": result.run_id,
                "evidence_reads": list(evidence_reads),
                "pattern_reads": list(pattern_reads),
                "evidence_coverage_complete": set(evidence_reads) == set(expected_evidence),
                "pattern_coverage_complete": (
                    set(pattern_reads) == set(expected_patterns)
                    if pattern_coverage_applicable
                    else None
                ),
                "duplicate_evidence_reads": len(evidence_reads) - len(set(evidence_reads)),
                "duplicate_pattern_reads": len(pattern_reads) - len(set(pattern_reads)),
                "decision": None if proposal is None else proposal.decision.value,
                "horizon_sessions": (None if candidate is None else candidate.horizon_sessions),
                "confidence": None if candidate is None else candidate.confidence,
                "transmission_step_count": (
                    0 if proposal is None else len(proposal.transmission_steps)
                ),
                "support_ref_count": 0 if candidate is None else len(candidate.evidence_refs),
                "counterevidence_ref_count": (
                    0 if candidate is None else len(candidate.counterevidence_refs)
                ),
                "invalidation_count": (
                    0 if candidate is None else len(candidate.invalidation_conditions)
                ),
                "unresolved_question_count": (
                    0 if proposal is None else len(proposal.unresolved_questions)
                ),
            }
        )
    return {
        "expected_evidence_ids": list(expected_evidence),
        "expected_pattern_pack_ids": list(expected_patterns),
        "pattern_coverage_applicable": pattern_coverage_applicable,
        "complete_evidence_coverage_runs": sum(
            item["evidence_coverage_complete"] is True for item in rows
        ),
        "complete_pattern_coverage_runs": (
            sum(item["pattern_coverage_complete"] is True for item in rows)
            if pattern_coverage_applicable
            else None
        ),
        "runs": rows,
    }


def _read_tool_targets(journal: RunJournal, run_id: str) -> tuple[tuple[str, ...], tuple[str, ...]]:
    evidence: list[str] = []
    patterns: list[str] = []
    for event in journal.events(run_id):
        if event.event_type != "tool.call.completed":
            continue
        tool_name = event.payload.get("tool_name")
        if tool_name not in {"read_evidence", "read_pattern_pack"}:
            continue
        raw_content = event.payload.get("model_content")
        if not isinstance(raw_content, str):
            raise ValueError("stored research tool result is missing model_content")
        envelope = json.loads(raw_content)
        if not isinstance(envelope, dict):
            raise ValueError("stored research tool result is invalid")
        envelope_payload = cast(dict[str, object], envelope)
        raw_result = envelope_payload.get("result")
        if not isinstance(raw_result, dict):
            raise ValueError("stored research tool result is invalid")
        payload = cast(dict[str, object], raw_result)
        if tool_name == "read_evidence":
            reference = payload.get("reference")
            if not isinstance(reference, dict):
                raise ValueError("stored evidence tool result lacks evidence_id")
            reference_payload = cast(dict[str, object], reference)
            evidence_id = reference_payload.get("evidence_id")
            if not isinstance(evidence_id, str):
                raise ValueError("stored evidence tool result lacks evidence_id")
            evidence.append(evidence_id)
        else:
            pack_id = payload.get("pack_id")
            if not isinstance(pack_id, str):
                raise ValueError("stored Pattern Pack tool result lacks pack_id")
            patterns.append(pack_id)
    return tuple(evidence), tuple(patterns)
