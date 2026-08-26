from __future__ import annotations

import asyncio
import os
from collections.abc import Awaitable
from pathlib import Path
from typing import Protocol

from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.agent_engine import (
    AgentEngine,
    AgentExecutionBinding,
    AgentRunRequest,
    AgentRunResult,
)
from market_impact_agent.agent_ensemble import aggregate_agent_replicates
from market_impact_agent.agent_runtime import (
    ModelProvider,
    ProviderPricing,
    RuntimeBudget,
    RuntimeConfig,
    SkillRegistry,
    ToolAccessContext,
    ToolRegistry,
    ToolSideEffect,
)
from market_impact_agent.agent_study import (
    AgentPhase2Preregistration,
    load_agent_phase2_preregistration,
)
from market_impact_agent.frozen_research import FrozenResearchRepository
from market_impact_agent.minimax_provider import MiniMaxOpenAIProvider
from market_impact_agent.runtime_store import ArtifactStore, RunJournal

AGENT_RUNTIME_REF = "market-impact.agent-runtime.local-research.v2"


class AvailableModelProvider(ModelProvider, Protocol):
    async def assert_model_available(self, *, timeout_seconds: float) -> None: ...


class ReplicateRunner(Protocol):
    def __call__(
        self,
        *,
        repository: FrozenResearchRepository,
        provider: ModelProvider,
        config: RuntimeConfig,
        registration: AgentPhase2Preregistration,
        research_instruction: str,
        run_id: str,
        skill_root: Path,
        state_directory: Path,
        secret_values: tuple[str, ...],
    ) -> Awaitable[AgentRunResult]: ...


async def run_agent_ensemble_bundle(
    *,
    registration_path: Path,
    exposure_registry_path: Path,
    evidence_pack_path: Path,
    evidence_documents_path: Path,
    pattern_pack_paths: tuple[Path, ...],
    ensemble_run_id: str,
    skill_root: Path,
    state_root: Path,
    ensemble_state_root: Path,
    provider: AvailableModelProvider | None = None,
    replicate_runner: ReplicateRunner | None = None,
) -> dict[str, object]:
    registration, registry = load_agent_phase2_preregistration(
        registration_path,
        exposure_registry_path,
    )
    repository = FrozenResearchRepository.from_files(
        evidence_pack_path=evidence_pack_path,
        evidence_documents_path=evidence_documents_path,
        pattern_pack_paths=pattern_pack_paths,
    )
    eligible_targets = {item.instrument_id for item in registry.entries if item.selection_eligible}
    outside_registry = sorted(set(repository.evidence_pack.allowed_targets) - eligible_targets)
    if outside_registry:
        raise ValueError(
            "Evidence Pack contains targets outside the frozen selection-eligible "
            f"Exposure Registry: {', '.join(outside_registry)}"
        )
    selected_provider = provider or MiniMaxOpenAIProvider.from_environment()
    protocol = registration.agent_protocol
    if (
        selected_provider.provider_id != protocol.provider_id
        or selected_provider.model != protocol.model
        or protocol.runtime_ref != AGENT_RUNTIME_REF
    ):
        raise ValueError("active Agent runtime does not match the frozen study protocol")
    config = _runtime_config(selected_provider)
    instruction = _ensemble_research_instruction(registration, repository)
    run_root = state_root / canonical_hash(ensemble_run_id)
    decision_store = ArtifactStore(
        ensemble_state_root / canonical_hash(ensemble_run_id) / "artifacts"
    )
    binding = _freeze_execution_binding(
        repository=repository,
        provider=selected_provider,
        config=config,
        registration=registration,
        research_instruction=instruction,
        ensemble_run_id=ensemble_run_id,
        skill_root=skill_root,
        state_directory=decision_store.root.parent,
        artifact_store=decision_store,
        secret_values=(os.environ.get("MINIMAX_API_KEY", ""),),
    )
    stored_binding = decision_store.put_json(
        binding.to_dict(),
        media_type="application/vnd.market-impact.agent-execution-binding+json",
    )
    if stored_binding.content_hash != binding.binding_hash:
        raise AssertionError("frozen Agent execution binding hash is inconsistent")
    await selected_provider.assert_model_available(timeout_seconds=30)
    runner = replicate_runner or _run_replicate
    secret_values = (os.environ.get("MINIMAX_API_KEY", ""),)
    tasks = tuple(
        runner(
            repository=repository,
            provider=selected_provider,
            config=config,
            registration=registration,
            research_instruction=instruction,
            run_id=f"{ensemble_run_id}.replicate-{index}",
            skill_root=skill_root,
            state_directory=run_root / f"replicate-{index}",
            secret_values=secret_values,
        )
        for index in range(1, protocol.replicate_count + 1)
    )
    results = tuple(await asyncio.gather(*tasks))
    decision = aggregate_agent_replicates(
        ensemble_run_id=ensemble_run_id,
        registration=registration,
        evidence_pack=repository.evidence_pack,
        results=results,
        frozen_execution_binding_hash=binding.binding_hash,
    )
    decision.validate_against(registration, repository.evidence_pack, registry)
    stored = decision_store.put_json(
        decision.to_dict(),
        media_type="application/vnd.market-impact.agent-ensemble-decision+json",
    )
    metrics = [item.metrics for item in results if item.metrics is not None]
    return {
        "ensemble_run_id": ensemble_run_id,
        "decision": decision.to_dict(),
        "decision_artifact_hash": stored.content_hash,
        "decision_artifact_path": stored.path.as_posix(),
        "execution_binding_hash": binding.binding_hash,
        "execution_binding_artifact_path": stored_binding.path.as_posix(),
        "replicates": [
            {
                "run_id": item.run_id,
                "status": item.status.value,
                "terminal_store_hash": item.terminal_store_hash,
                "state_directory": (run_root / f"replicate-{index}").as_posix(),
                "metrics": None if item.metrics is None else item.metrics.to_dict(),
            }
            for index, item in enumerate(results, start=1)
        ],
        "totals": {
            "turns": sum(item.turns for item in metrics),
            "tool_calls": sum(item.tool_calls for item in metrics),
            "input_tokens": sum(item.input_tokens for item in metrics),
            "output_tokens": sum(item.output_tokens for item in metrics),
            "provider_attempts": sum(item.provider_attempts for item in metrics),
            "estimated_cost_microusd": sum(item.estimated_cost_microusd for item in metrics),
        },
        "broker_reachability": False,
        "execution_capability": "none",
    }


async def _run_replicate(
    *,
    repository: FrozenResearchRepository,
    provider: ModelProvider,
    config: RuntimeConfig,
    registration: AgentPhase2Preregistration,
    research_instruction: str,
    run_id: str,
    skill_root: Path,
    state_directory: Path,
    secret_values: tuple[str, ...],
) -> AgentRunResult:
    artifact_store = ArtifactStore(state_directory / "artifacts")
    tool_registry = ToolRegistry(artifact_store)
    for descriptor in repository.tool_descriptors():
        tool_registry.register(descriptor)
    engine = AgentEngine(
        provider=provider,
        config=config,
        artifact_store=artifact_store,
        journal=RunJournal(state_directory / "run.sqlite3"),
        tool_registry=tool_registry,
        skill_registry=SkillRegistry(skill_root),
        secret_values=secret_values,
    )
    return await engine.run(
        build_agent_run_request(
            run_id=run_id,
            repository=repository,
            registration=registration,
            research_instruction=research_instruction,
        )
    )


def _freeze_execution_binding(
    *,
    repository: FrozenResearchRepository,
    provider: ModelProvider,
    config: RuntimeConfig,
    registration: AgentPhase2Preregistration,
    research_instruction: str,
    ensemble_run_id: str,
    skill_root: Path,
    state_directory: Path,
    artifact_store: ArtifactStore,
    secret_values: tuple[str, ...],
) -> AgentExecutionBinding:
    tool_registry = ToolRegistry(artifact_store)
    for descriptor in repository.tool_descriptors():
        tool_registry.register(descriptor)
    engine = AgentEngine(
        provider=provider,
        config=config,
        artifact_store=artifact_store,
        journal=RunJournal(state_directory / "binding-preflight.sqlite3"),
        tool_registry=tool_registry,
        skill_registry=SkillRegistry(skill_root),
        secret_values=secret_values,
    )
    return engine.execution_binding(
        build_agent_run_request(
            run_id=f"{ensemble_run_id}.binding-preflight",
            repository=repository,
            registration=registration,
            research_instruction=research_instruction,
        ),
        runtime_ref=registration.agent_protocol.runtime_ref,
    )


def build_agent_run_request(
    *,
    run_id: str,
    repository: FrozenResearchRepository,
    registration: AgentPhase2Preregistration,
    research_instruction: str,
) -> AgentRunRequest:
    protocol = registration.agent_protocol
    return AgentRunRequest(
        run_id=run_id,
        evidence_pack=repository.evidence_pack,
        research_instruction=research_instruction,
        selected_skills=protocol.selected_skills,
        tool_access=ToolAccessContext(
            allowed_capabilities=frozenset({"evidence.read", "pattern.read"}),
            allowed_side_effects=frozenset({ToolSideEffect.READ_ONLY}),
            allowed_tools=frozenset(protocol.allowed_tools),
        ),
    )


def _runtime_config(provider: ModelProvider) -> RuntimeConfig:
    return RuntimeConfig(
        provider_id=provider.provider_id,
        model=provider.model,
        context_window_tokens=131_072,
        reserved_output_tokens=8_192,
        temperature=1,
        top_p=0.95,
        budget=RuntimeBudget(
            max_turns=8,
            max_tool_calls=12,
            max_input_tokens=500_000,
            max_output_tokens=32_768,
            max_wall_seconds=300,
            max_result_bytes=256_000,
        ),
        pricing=ProviderPricing(
            pricing_id="minimax-m3-paygo-2026-08-26-context-le-512k",
            input_microusd_per_million_tokens=300_000,
            output_microusd_per_million_tokens=1_200_000,
        ),
    )


def _ensemble_research_instruction(
    registration: AgentPhase2Preregistration,
    repository: FrozenResearchRepository,
) -> str:
    protocol = registration.agent_protocol
    targets = ", ".join(repository.evidence_pack.allowed_targets)
    horizons = ", ".join(str(item) for item in protocol.eligible_horizons_sessions)
    return (
        "Independently assess this physical energy supply shock. Before deciding, call "
        "read_pattern_pack for every referenced Pattern Pack and read_evidence for every "
        "Evidence Pack item. Apply only supported patterns, test offsets and "
        "counterevidence, and use no information outside the frozen inputs. The only "
        f"eligible targets are [{targets}], the only eligible direction is up, and the "
        f"only eligible horizons in sessions are [{horizons}]. A proposed study candidate "
        f"must have confidence at least {protocol.minimum_candidate_confidence}. Return "
        "exactly one eligible candidate or abstain; if more than one candidate remains "
        "eligible, abstain because the replicate has no registered tie-breaker."
    )
