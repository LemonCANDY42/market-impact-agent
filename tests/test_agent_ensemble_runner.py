from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import cast

import pytest

from market_impact_agent.agent_contracts import (
    CandidateDirection,
    CandidateImpact,
    JudgmentArtifact,
    JudgmentDecision,
    JudgmentProposal,
    ProposedTransmissionStep,
    canonical_hash,
)
from market_impact_agent.agent_engine import (
    AgentEngine,
    AgentRunResult,
    RunMetrics,
)
from market_impact_agent.agent_ensemble import (
    EnsembleDisposition,
    agent_ensemble_decision_from_dict,
)
from market_impact_agent.agent_ensemble_runner import (
    build_agent_run_request,
    run_agent_ensemble_bundle,
)
from market_impact_agent.agent_runtime import (
    ModelProvider,
    ModelTurn,
    RuntimeConfig,
    SkillRegistry,
    ToolRegistry,
)
from market_impact_agent.agent_study import AgentPhase2Preregistration
from market_impact_agent.frozen_research import FrozenResearchRepository
from market_impact_agent.research import TransmissionDirectness
from market_impact_agent.runtime_store import ArtifactStore, RunJournal, RunStatus

from .runtime_fakes import BusinessModelFixture

ROOT = Path("examples/agent/energy_supply")
REGISTRATION_PATH = Path("examples/calibration/agent-physical-energy-prospective-v1.json")
REGISTRY_PATH = Path("examples/research/a-share-energy-exposure-registry-v1.json")
SKILL_ROOT = Path("skills")


class FakeAvailableProvider(BusinessModelFixture):
    def __init__(self, binding_root: Path | None = None) -> None:
        self.available_checked = False
        self.binding_root = binding_root

    @property
    def provider_id(self) -> str:
        return "minimax-openai-compatible"

    @property
    def model(self) -> str:
        return "MiniMax-M3"

    async def assert_model_available(self, *, timeout_seconds: float) -> None:
        assert timeout_seconds == 30
        if self.binding_root is not None:
            assert list(self.binding_root.rglob("binding-preflight.sqlite3"))
            artifact_roots = tuple(self.binding_root.rglob("artifacts"))
            assert artifact_roots
            assert any(path.is_file() for root in artifact_roots for path in root.iterdir())
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
        del messages, tools, temperature, top_p, max_output_tokens, timeout_seconds
        raise AssertionError("the fake replicate runner must avoid Provider calls")


def _proposal(repository: FrozenResearchRepository, *, target_id: str, horizon: int):
    evidence_ids = tuple(item.evidence_id for item in repository.evidence_pack.evidence)
    return JudgmentProposal(
        event_id=repository.evidence_pack.event_id,
        decision=JudgmentDecision.PROPOSE,
        summary="Synthetic ensemble runner acceptance proposal.",
        transmission_steps=(
            ProposedTransmissionStep(
                step_id="runner-step",
                from_node="physical-event",
                to_node=target_id,
                mechanism="synthetic acceptance transmission",
                directness=TransmissionDirectness.DIRECT,
                horizon_sessions=horizon,
                evidence_refs=(evidence_ids[0],),
            ),
        ),
        candidates=(
            CandidateImpact(
                target_id=target_id,
                direction=CandidateDirection.UP,
                horizon_sessions=horizon,
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


def test_runner_freezes_binding_before_five_isolated_replicates(
    tmp_path: Path,
) -> None:
    ensemble_state_root = tmp_path / "ensemble-decisions"
    provider = FakeAvailableProvider(ensemble_state_root)
    seen_directories: list[Path] = []

    async def fake_replicate_runner(
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
        del secret_values
        assert list(ensemble_state_root.rglob("binding-preflight.sqlite3"))
        seen_directories.append(state_directory)
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
        )
        request = build_agent_run_request(
            run_id=run_id,
            repository=repository,
            registration=registration,
            research_instruction=research_instruction,
        )
        binding = engine.execution_binding(
            request,
            runtime_ref=registration.agent_protocol.runtime_ref,
        )
        index = int(run_id.rsplit("-", maxsplit=1)[1])
        target_id = "600938.XSHG"
        horizon = 3 if index <= 3 else 1
        started_at = datetime(2026, 8, 26, 8, tzinfo=UTC)
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
            journal_hash=f"{index:064x}",
            transcript_hash=f"{index + 10:064x}",
            raw_response_hash=f"{index + 20:064x}",
            started_at=started_at,
            finished_at=started_at + timedelta(seconds=index),
            proposal=_proposal(repository, target_id=target_id, horizon=horizon),
        )
        stored = artifact_store.put_json(judgment.to_dict())
        return AgentRunResult(
            run_id=run_id,
            status=RunStatus.COMPLETED,
            judgment=judgment,
            terminal_store_hash=stored.content_hash,
            metrics=RunMetrics(
                turns=1,
                tool_calls=5,
                input_tokens=100,
                output_tokens=20,
                result_bytes=1000,
                latency_ms=10,
                provider_attempts=1,
                estimated_cost_microusd=50,
            ),
        )

    result = asyncio.run(
        run_agent_ensemble_bundle(
            registration_path=REGISTRATION_PATH,
            exposure_registry_path=REGISTRY_PATH,
            evidence_pack_path=ROOT / "evidence-pack.json",
            evidence_documents_path=ROOT / "evidence-documents.json",
            pattern_pack_paths=(ROOT / "pattern-pack.json",),
            ensemble_run_id="synthetic-runner-ensemble",
            skill_root=SKILL_ROOT,
            state_root=tmp_path / "runs",
            ensemble_state_root=ensemble_state_root,
            provider=provider,
            replicate_runner=fake_replicate_runner,
        )
    )

    decision = agent_ensemble_decision_from_dict(result["decision"])
    stored_decision = json.loads(Path(cast(str, result["decision_artifact_path"])).read_text())
    assert provider.available_checked is True
    assert len(seen_directories) == 5
    assert len(set(seen_directories)) == 5
    assert decision.disposition is EnsembleDisposition.PROPOSE
    assert decision.selected_vote is not None
    assert decision.selected_vote.key == ("600938.XSHG", "up", 3)
    assert decision.frozen_execution_binding_hash == result["execution_binding_hash"]
    assert stored_decision == decision.to_dict()
    assert result["totals"] == {
        "turns": 5,
        "tool_calls": 25,
        "input_tokens": 500,
        "output_tokens": 100,
        "provider_attempts": 5,
        "estimated_cost_microusd": 250,
    }
    assert result["broker_reachability"] is False
    assert result["execution_capability"] == "none"


def test_runner_rejects_noneligible_registry_target_before_provider_call(
    tmp_path: Path,
) -> None:
    payload = json.loads((ROOT / "evidence-pack.json").read_text())
    payload["allowed_targets"] = ["600028.XSHG"]
    core = dict(payload)
    core.pop("pack_id")
    payload["pack_id"] = f"evidence-pack-{canonical_hash(core)}"
    evidence_pack_path = tmp_path / "evidence-pack.json"
    evidence_pack_path.write_text(json.dumps(payload), encoding="utf-8")
    provider = FakeAvailableProvider()

    with pytest.raises(ValueError, match="outside the frozen selection-eligible"):
        asyncio.run(
            run_agent_ensemble_bundle(
                registration_path=REGISTRATION_PATH,
                exposure_registry_path=REGISTRY_PATH,
                evidence_pack_path=evidence_pack_path,
                evidence_documents_path=ROOT / "evidence-documents.json",
                pattern_pack_paths=(ROOT / "pattern-pack.json",),
                ensemble_run_id="invalid-target-ensemble",
                skill_root=SKILL_ROOT,
                state_root=tmp_path / "runs",
                ensemble_state_root=tmp_path / "decisions",
                provider=provider,
            )
        )

    assert provider.available_checked is False
