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
from market_impact_agent.method_benchmark import BenchmarkTreatmentBinding
from market_impact_agent.method_development_runner import (
    load_method_development_case,
    run_method_development_state,
)
from market_impact_agent.research import TransmissionDirectness
from market_impact_agent.runtime_store import ArtifactStore, RunJournal, RunStatus
from market_impact_agent.usage_ledger import UsageLedger

ROOT = Path("examples/agent/abqaiq_development")


class FakeAvailableProvider:
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


def _proposal(repository: FrozenResearchRepository) -> JudgmentProposal:
    evidence_ids = tuple(item.evidence_id for item in repository.evidence_pack.evidence)
    return JudgmentProposal(
        event_id=repository.evidence_pack.event_id,
        decision=JudgmentDecision.PROPOSE,
        summary="Opened development acceptance proposal.",
        transmission_steps=(
            ProposedTransmissionStep(
                step_id="development-step",
                from_node="physical-loss",
                to_node="integrated-upstream-a",
                mechanism="masked physical supply transmission",
                directness=TransmissionDirectness.SECOND_ORDER,
                horizon_sessions=1,
                evidence_refs=(evidence_ids[1],),
            ),
        ),
        candidates=(
            CandidateImpact(
                target_id="integrated-upstream-a",
                direction=CandidateDirection.UP,
                horizon_sessions=1,
                directness=TransmissionDirectness.SECOND_ORDER,
                confidence=0.7,
                thesis="Masked one-session development thesis.",
                evidence_refs=(evidence_ids[1],),
                counterevidence_refs=(evidence_ids[-1],),
                invalidation_conditions=("masked mitigation dominates",),
            ),
        ),
        blockers=(),
        unresolved_questions=("masked benchmark response",),
        stopped_reason="opened development fixture completed",
    )


async def _fake_result(
    *,
    repository: FrozenResearchRepository,
    provider: ModelProvider,
    config: RuntimeConfig,
    binding: BenchmarkTreatmentBinding,
    research_instruction: str,
    run_id: str,
    runtime_ref: str,
    skill_root: Path,
    state_directory: Path,
    secret_values: tuple[str, ...],
    status: RunStatus,
) -> AgentRunResult:
    _ = secret_values
    replicate_index = int(run_id.rsplit("-", maxsplit=1)[1])
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
        research_instruction=research_instruction,
        selected_skills=binding.requested_skills,
        tool_access=ToolAccessContext(
            allowed_capabilities=frozenset(binding.allowed_capabilities),
            allowed_side_effects=frozenset({ToolSideEffect.READ_ONLY}),
            allowed_tools=frozenset(binding.allowed_tools),
        ),
    )
    execution_binding = engine.execution_binding(request, runtime_ref=runtime_ref)
    started_at = datetime(2026, 8, 27, 3, replicate_index, tzinfo=UTC)
    journal.start_run(run_id=run_id, config_hash=config.config_hash, created_at=started_at)
    metrics = RunMetrics(
        turns=1,
        tool_calls=1,
        input_tokens=100,
        output_tokens=20,
        result_bytes=100,
        latency_ms=10,
        provider_attempts=1,
        estimated_cost_microusd=50,
    )
    if status is not RunStatus.COMPLETED:
        journal.finish(
            run_id=run_id,
            status=status,
            finished_at=started_at + timedelta(seconds=1),
            terminal_artifact_id=None,
        )
        return AgentRunResult(
            run_id=run_id,
            status=status,
            judgment=None,
            terminal_store_hash=None,
            metrics=metrics,
        )
    judgment = JudgmentArtifact.build(
        run_id=run_id,
        evidence_pack_id=repository.evidence_pack.pack_id,
        provider_id=provider.provider_id,
        model=provider.model,
        runtime_config_hash=execution_binding.runtime_config_hash,
        prompt_hash=execution_binding.prompt_hash,
        skill_hashes=execution_binding.skill_hashes,
        tool_manifest_hashes=execution_binding.tool_manifest_hashes,
        tool_surface_hash=execution_binding.tool_surface_hash,
        mcp_server_hashes=execution_binding.mcp_server_hashes,
        context_estimator_id=execution_binding.context_estimator_id,
        compactor_id=execution_binding.compactor_id,
        journal_hash=journal.journal_hash(run_id),
        transcript_hash=f"{replicate_index + 300:064x}",
        raw_response_hash=f"{replicate_index + 400:064x}",
        started_at=started_at,
        finished_at=started_at + timedelta(seconds=1),
        proposal=_proposal(repository),
    )
    stored = artifact_store.put_json(judgment.to_dict())
    journal.finish(
        run_id=run_id,
        status=status,
        finished_at=started_at + timedelta(seconds=1),
        terminal_artifact_id=stored.content_hash,
    )
    return AgentRunResult(
        run_id=run_id,
        status=status,
        judgment=judgment,
        terminal_store_hash=stored.content_hash,
        metrics=metrics,
    )


def test_opened_case_registration_binds_one_event_and_two_states() -> None:
    case = load_method_development_case(
        Path("examples/calibration/method-development-abqaiq-v1.json")
    )

    assert case.independent_unit == "one_event_case_not_two_independent_states"
    assert tuple(item.state_id for item in case.states) == ("attack", "recovery")
    assert not case.inference_eligible
    assert case.eligible_horizons_sessions == (1,)


def test_opened_case_registration_rejects_content_identity_tamper(tmp_path: Path) -> None:
    source = Path("examples/calibration/method-development-abqaiq-v1.json")
    payload = json.loads(source.read_text(encoding="utf-8"))
    payload["case_alias"] = "tampered-opened-case"
    target = tmp_path / "tampered-case.json"
    target.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ValueError, match="case_id does not match content"):
        load_method_development_case(target)


def test_development_runner_rejects_case_arm_binding_not_in_active_benchmark(
    tmp_path: Path,
) -> None:
    source = Path("examples/calibration/method-development-abqaiq-v1.json")
    payload = json.loads(source.read_text(encoding="utf-8"))
    arm_bindings = cast(list[dict[str, object]], payload["arm_bindings"])
    arm_bindings[0]["route_id"] = arm_bindings[1]["route_id"]
    core = {key: value for key, value in payload.items() if key != "case_id"}
    payload["case_id"] = f"method-development-case-{canonical_hash(core)}"
    case_path = tmp_path / "tampered-arm-case.json"
    case_path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ValueError, match="arm bindings do not match active benchmark"):
        asyncio.run(
            run_method_development_state(
                case_path=case_path,
                benchmark_registration_path=Path(
                    "examples/calibration/method-quality-benchmark-v2.json"
                ),
                evaluation_specification_path=Path(
                    "examples/calibration/method-quality-evaluation-specification-v2.json"
                ),
                method_catalog_path=Path("examples/research/research-method-catalog-v2.json"),
                provider_profile_path=Path("examples/providers/minimax-m3-research-v1.json"),
                state_id="attack",
                evidence_pack_path=ROOT / "evidence-pack-attack.json",
                evidence_documents_path=ROOT / "evidence-documents-attack.json",
                pattern_pack_paths=(ROOT / "pattern-pack.json",),
                backtest_request_path=Path(
                    "examples/backtests/real-abqaiq-601857-attack-state-request-v1.json"
                ),
                experiment_id="tampered-arm-binding",
                skill_root=Path("skills"),
                state_root=tmp_path / "state",
                provider=FakeAvailableProvider(tmp_path / "state"),
            )
        )


def test_development_runner_freezes_four_arms_and_records_twenty_runs(
    tmp_path: Path,
) -> None:
    state_root = tmp_path / "development-state"
    provider = FakeAvailableProvider(state_root)
    seen: list[tuple[str, int]] = []

    async def fake_runner(
        *,
        repository: FrozenResearchRepository,
        provider: ModelProvider,
        config: RuntimeConfig,
        binding: BenchmarkTreatmentBinding,
        research_instruction: str,
        run_id: str,
        runtime_ref: str,
        skill_root: Path,
        state_directory: Path,
        secret_values: tuple[str, ...],
    ) -> AgentRunResult:
        _ = secret_values
        replicate_index = int(run_id.rsplit("-", maxsplit=1)[1])
        seen.append((binding.arm.value, replicate_index))
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
            research_instruction=research_instruction,
            selected_skills=binding.requested_skills,
            tool_access=ToolAccessContext(
                allowed_capabilities=frozenset(binding.allowed_capabilities),
                allowed_side_effects=frozenset({ToolSideEffect.READ_ONLY}),
                allowed_tools=frozenset(binding.allowed_tools),
            ),
        )
        execution_binding = engine.execution_binding(request, runtime_ref=runtime_ref)
        started_at = datetime(2026, 8, 27, 2, replicate_index, tzinfo=UTC)
        journal.start_run(run_id=run_id, config_hash=config.config_hash, created_at=started_at)
        judgment = JudgmentArtifact.build(
            run_id=run_id,
            evidence_pack_id=repository.evidence_pack.pack_id,
            provider_id=provider.provider_id,
            model=provider.model,
            runtime_config_hash=execution_binding.runtime_config_hash,
            prompt_hash=execution_binding.prompt_hash,
            skill_hashes=execution_binding.skill_hashes,
            tool_manifest_hashes=execution_binding.tool_manifest_hashes,
            tool_surface_hash=execution_binding.tool_surface_hash,
            mcp_server_hashes=execution_binding.mcp_server_hashes,
            context_estimator_id=execution_binding.context_estimator_id,
            compactor_id=execution_binding.compactor_id,
            journal_hash=journal.journal_hash(run_id),
            transcript_hash=f"{replicate_index + 100:064x}",
            raw_response_hash=f"{replicate_index + 200:064x}",
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
        run_method_development_state(
            case_path=Path("examples/calibration/method-development-abqaiq-v1.json"),
            benchmark_registration_path=Path(
                "examples/calibration/method-quality-benchmark-v2.json"
            ),
            evaluation_specification_path=Path(
                "examples/calibration/method-quality-evaluation-specification-v2.json"
            ),
            method_catalog_path=Path("examples/research/research-method-catalog-v2.json"),
            provider_profile_path=Path("examples/providers/minimax-m3-research-v1.json"),
            state_id="recovery",
            evidence_pack_path=ROOT / "evidence-pack-recovery.json",
            evidence_documents_path=ROOT / "evidence-documents-recovery.json",
            pattern_pack_paths=(ROOT / "pattern-pack.json",),
            backtest_request_path=Path(
                "examples/backtests/real-abqaiq-601857-recovery-state-request-v1.json"
            ),
            experiment_id="opened-development-recovery",
            skill_root=Path("skills"),
            state_root=state_root,
            provider=provider,
            replicate_runner=fake_runner,
        )
    )

    assert provider.available_checked
    assert len(seen) == 20
    assert seen[:4] == [
        ("neutral_evidence", 1),
        ("general_methods", 1),
        ("general_pattern", 1),
        ("family_guided", 1),
    ]
    assert report["outcomes_used_by_agent"] is False
    assert report["inference_eligible"] is False
    arms = cast(list[dict[str, object]], report["arms"])
    assert len(arms) == 4
    assert all(cast(dict[str, object], arm["decision"])["disposition"] == "propose" for arm in arms)
    ledger = UsageLedger(Path(str(report["state_directory"])) / "usage.sqlite3")
    assert len(ledger.records()) == 20
    assert ledger.ledger_hash == report["usage_ledger_hash"]


@pytest.mark.parametrize("terminal_status", [RunStatus.FAILED, RunStatus.BUDGET_EXHAUSTED])
def test_development_runner_records_failed_attempts_but_produces_no_report(
    tmp_path: Path,
    terminal_status: RunStatus,
) -> None:
    state_root = tmp_path / "development-state"
    provider = FakeAvailableProvider(state_root)
    experiment_id = f"incomplete-{terminal_status.value}"

    async def fake_runner(
        *,
        repository: FrozenResearchRepository,
        provider: ModelProvider,
        config: RuntimeConfig,
        binding: BenchmarkTreatmentBinding,
        research_instruction: str,
        run_id: str,
        runtime_ref: str,
        skill_root: Path,
        state_directory: Path,
        secret_values: tuple[str, ...],
    ) -> AgentRunResult:
        status = (
            terminal_status if ".neutral_evidence.replicate-1" in run_id else RunStatus.COMPLETED
        )
        return await _fake_result(
            repository=repository,
            provider=provider,
            config=config,
            binding=binding,
            research_instruction=research_instruction,
            run_id=run_id,
            runtime_ref=runtime_ref,
            skill_root=skill_root,
            state_directory=state_directory,
            secret_values=secret_values,
            status=status,
        )

    with pytest.raises(ValueError, match="requires 20 completed runs with judgments"):
        asyncio.run(
            run_method_development_state(
                case_path=Path("examples/calibration/method-development-abqaiq-v1.json"),
                benchmark_registration_path=Path(
                    "examples/calibration/method-quality-benchmark-v2.json"
                ),
                evaluation_specification_path=Path(
                    "examples/calibration/method-quality-evaluation-specification-v2.json"
                ),
                method_catalog_path=Path("examples/research/research-method-catalog-v2.json"),
                provider_profile_path=Path("examples/providers/minimax-m3-research-v1.json"),
                state_id="attack",
                evidence_pack_path=ROOT / "evidence-pack-attack.json",
                evidence_documents_path=ROOT / "evidence-documents-attack.json",
                pattern_pack_paths=(ROOT / "pattern-pack.json",),
                backtest_request_path=Path(
                    "examples/backtests/real-abqaiq-601857-attack-state-request-v1.json"
                ),
                experiment_id=experiment_id,
                skill_root=Path("skills"),
                state_root=state_root,
                provider=provider,
                replicate_runner=fake_runner,
            )
        )

    experiment_root = state_root / canonical_hash(experiment_id)
    ledger = UsageLedger(experiment_root / "usage.sqlite3")
    assert len(ledger.records()) == 20
    assert any(record.record.status is terminal_status for record in ledger.records())
    assert not any(
        b"market-impact.method-development-report.v1" in path.read_bytes()
        for path in (experiment_root / "artifacts").iterdir()
    )
