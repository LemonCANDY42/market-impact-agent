from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import cast

from market_impact_agent.agent_contracts import (
    CandidateDirection,
    CandidateImpact,
    JudgmentArtifact,
    JudgmentDecision,
    JudgmentProposal,
    ProposedTransmissionStep,
)
from market_impact_agent.agent_engine import (
    AgentEngine,
    AgentRunRequest,
    AgentRunResult,
    RunMetrics,
)
from market_impact_agent.agent_runtime import (
    ModelProvider,
    ModelTurn,
    RuntimeConfig,
    SkillRegistry,
    ToolAccessContext,
    ToolRegistry,
    ToolSideEffect,
)
from market_impact_agent.frozen_research import FrozenResearchRepository
from market_impact_agent.method_ablation_runner import run_method_ablation_bundle
from market_impact_agent.research import TransmissionDirectness
from market_impact_agent.research_methods import AblationArmStudy
from market_impact_agent.runtime_store import ArtifactStore, RunJournal, RunStatus
from market_impact_agent.usage_ledger import UsageLedger

from .runtime_fakes import BusinessModelFixture

ROOT = Path("examples/agent/energy_supply")


class FakeAvailableProvider(BusinessModelFixture):
    def __init__(self, state_root: Path) -> None:
        self.state_root = state_root
        self.available_checked = False

    @property
    def provider_id(self) -> str:
        return "minimax-openai-compatible"

    @property
    def model(self) -> str:
        return "MiniMax-M3"

    async def assert_model_available(self, *, timeout_seconds: float) -> None:
        assert timeout_seconds == 30
        assert len(tuple(self.state_root.rglob("binding-*.sqlite3"))) == 4
        self.available_checked = True

    async def answer(
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


def _proposal(repository: FrozenResearchRepository) -> JudgmentProposal:
    evidence_ids = tuple(item.evidence_id for item in repository.evidence_pack.evidence)
    return JudgmentProposal(
        event_id=repository.evidence_pack.event_id,
        decision=JudgmentDecision.PROPOSE,
        summary="Synthetic method-ablation acceptance proposal.",
        transmission_steps=(
            ProposedTransmissionStep(
                step_id="ablation-step",
                from_node="event",
                to_node="600938.XSHG",
                mechanism="synthetic transmission",
                directness=TransmissionDirectness.DIRECT,
                horizon_sessions=3,
                evidence_refs=(evidence_ids[0],),
            ),
        ),
        candidates=(
            CandidateImpact(
                target_id="600938.XSHG",
                direction=CandidateDirection.UP,
                horizon_sessions=3,
                directness=TransmissionDirectness.DIRECT,
                confidence=0.7,
                thesis="Synthetic acceptance thesis.",
                evidence_refs=(evidence_ids[0],),
                counterevidence_refs=(evidence_ids[1],),
                invalidation_conditions=("synthetic premise is invalidated",),
            ),
        ),
        blockers=(),
        unresolved_questions=("synthetic duration",),
        stopped_reason="acceptance fixture completed",
    )


def test_four_arm_runner_freezes_all_bindings_and_records_twenty_terminal_runs(
    tmp_path: Path,
) -> None:
    state_root = tmp_path / "ablation-state"
    provider = FakeAvailableProvider(state_root)
    seen: list[tuple[str, int, Path]] = []

    async def fake_runner(
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
        _ = (research_instruction, secret_values)
        replicate_index = int(run_id.rsplit("-", maxsplit=1)[1])
        seen.append((study.arm.arm.value, replicate_index, state_directory))
        artifact_store = ArtifactStore(state_directory / "artifacts")
        tools = ToolRegistry(artifact_store)
        for descriptor in repository.tool_descriptors():
            tools.register(descriptor)
        journal = RunJournal(state_directory / "run.sqlite3")
        engine = AgentEngine(
            provider=provider,
            config=config,
            artifact_store=artifact_store,
            journal=journal,
            tool_registry=tools,
            skill_registry=SkillRegistry(skill_root),
        )
        request = AgentRunRequest(
            run_id=run_id,
            evidence_pack=repository.evidence_pack,
            research_instruction="Common synthetic acceptance instruction.",
            selected_skills=study.arm.requested_skills,
            tool_access=ToolAccessContext(
                allowed_capabilities=frozenset(study.arm.allowed_capabilities),
                allowed_side_effects=frozenset({ToolSideEffect.READ_ONLY}),
                allowed_tools=frozenset(study.arm.allowed_tools),
            ),
        )
        binding = engine.execution_binding(request, runtime_ref=study.agent_protocol.runtime_ref)
        started_at = datetime(2026, 8, 26, 12, replicate_index, tzinfo=UTC)
        journal.start_run(
            run_id=run_id,
            config_hash=config.config_hash,
            created_at=started_at,
        )
        judgment = JudgmentArtifact.build(
            run_id=run_id,
            evidence_pack_id=repository.evidence_pack.pack_id,
            provider_id=provider.provider_id,
            model=provider.model,
            runtime_config_hash=binding.runtime_config_hash,
            prompt_hash=binding.prompt_hash,
            skill_hashes=binding.skill_hashes,
            tool_manifest_hashes=binding.tool_manifest_hashes,
            tool_surface_hash=binding.tool_surface_hash,
            mcp_server_hashes=binding.mcp_server_hashes,
            context_estimator_id=binding.context_estimator_id,
            compactor_id=binding.compactor_id,
            journal_hash=journal.journal_hash(run_id),
            transcript_hash=f"{replicate_index + 10:064x}",
            raw_response_hash=f"{replicate_index + 20:064x}",
            started_at=started_at,
            finished_at=started_at + timedelta(seconds=1),
            proposal=_proposal(repository),
        )
        stored = artifact_store.put_json(judgment.to_dict())
        journal.finish(
            run_id=run_id,
            status=RunStatus.COMPLETED,
            finished_at=started_at + timedelta(seconds=1),
            terminal_artifact_id=stored.content_hash,
        )
        return AgentRunResult(
            run_id=run_id,
            status=RunStatus.COMPLETED,
            judgment=judgment,
            terminal_store_hash=stored.content_hash,
            metrics=RunMetrics(
                turns=1,
                tool_calls=1,
                input_tokens=100,
                output_tokens=20,
                result_bytes=100,
                latency_ms=10,
                provider_attempts=1,
                estimated_cost_microusd=50,
            ),
        )

    report = asyncio.run(
        run_method_ablation_bundle(
            ablation_registration_path=Path("examples/calibration/agent-method-ablation-v1.json"),
            parent_registration_path=Path(
                "examples/calibration/agent-physical-energy-prospective-v1.json"
            ),
            exposure_registry_path=Path(
                "examples/research/a-share-energy-exposure-registry-v1.json"
            ),
            method_catalog_path=Path("examples/research/research-method-catalog-v1.json"),
            provider_profile_path=Path("examples/providers/minimax-m3-research-v1.json"),
            evidence_pack_path=ROOT / "evidence-pack.json",
            evidence_documents_path=ROOT / "evidence-documents.json",
            pattern_pack_paths=(ROOT / "pattern-pack.json",),
            experiment_id="synthetic-method-ablation",
            skill_root=Path("skills"),
            state_root=state_root,
            provider=provider,
            replicate_runner=fake_runner,
        )
    )

    assert provider.available_checked is True
    assert len(seen) == 20
    assert [item[:2] for item in seen[:4]] == [
        ("neutral_evidence", 1),
        ("general_methods", 1),
        ("general_pattern", 1),
        ("family_guided", 1),
    ]
    assert report["market_outcomes_used"] is False
    assert report["quality_claim"] == "process_only_no_alpha_inference"
    arms = cast(list[dict[str, object]], report["arms"])
    assert len(arms) == 4
    assert all(
        cast(dict[str, object], arm["totals"])["estimated_cost_microusd"] == 250 for arm in arms
    )
    ledger = UsageLedger(Path(str(report["state_directory"])) / "usage.sqlite3")
    assert len(ledger.records()) == 20
    assert ledger.ledger_hash == report["usage_ledger_hash"]
