from __future__ import annotations

import asyncio
import json
import tempfile
from collections.abc import Mapping
from pathlib import Path
from typing import cast

from market_impact_agent.agent_contracts import (
    EvidencePack,
    JudgmentArtifact,
    canonical_hash,
)
from market_impact_agent.agent_engine import (
    AgentEngine,
    AgentExecutionBinding,
    AgentRunRequest,
    AgentRunResult,
)
from market_impact_agent.agent_runtime import (
    RuntimeConfig,
    SkillRegistry,
    ToolAccessContext,
    ToolRegistry,
    ToolSideEffect,
)
from market_impact_agent.frozen_research import FrozenResearchRepository
from market_impact_agent.model_provider import load_model_provider_profile
from market_impact_agent.paired_skill_ablation_contract import (
    ALLOWED_CAPABILITIES,
    ALLOWED_TOOLS,
    RUNTIME_REF,
    paired_skill_research_instruction,
)
from market_impact_agent.paired_skill_ablation_runner import (
    SkillAblationArm,
    build_paired_arm_report,
)
from market_impact_agent.pi_runtime import PiRuntimeProvider
from market_impact_agent.runtime_store import ArtifactStore, RunJournal
from market_impact_agent.usage_ledger import UsageLedger


def audit_paired_execution_state(
    *,
    expected_evidence_pack: EvidencePack,
    eligible_horizon_sessions: int,
    registration: Mapping[str, object],
    report: Mapping[str, object],
    experiment_root: Path,
    evidence_pack_path: Path,
    evidence_documents_path: Path,
    pattern_pack_paths: tuple[Path, ...],
    provider_profile_path: Path,
    skill_root: Path,
) -> dict[str, str]:
    repository = FrozenResearchRepository.from_files(
        evidence_pack_path=evidence_pack_path,
        evidence_documents_path=evidence_documents_path,
        pattern_pack_paths=pattern_pack_paths,
    )
    if repository.evidence_pack.to_dict() != expected_evidence_pack.to_dict():
        raise ValueError("paired execution audit Evidence Pack drifted")
    profile = load_model_provider_profile(provider_profile_path)
    if (
        registration.get("provider_profile_id") != profile.profile_id
        or registration.get("provider_profile_hash") != profile.profile_hash
    ):
        raise ValueError("paired execution audit Provider Profile drifted")
    instruction = paired_skill_research_instruction(
        repository.evidence_pack,
        eligible_horizon_sessions=eligible_horizon_sessions,
    )
    expected_arms = _expected_arms(registration, report)
    profile_config = profile.runtime_config()
    provider = PiRuntimeProvider(profile, dispatch_allowed=False)
    with tempfile.TemporaryDirectory(prefix="market-impact-binding-audit-") as temporary:
        temporary_root = Path(temporary)
        expected_bindings = _reconstruct_bindings(
            repository=repository,
            profile_config=profile_config,
            provider=provider,
            instruction=instruction,
            expected_arms=expected_arms,
            skill_root=skill_root,
            temporary_root=temporary_root,
            experiment_id=_string(registration, "experiment_id"),
        )
    _validate_binding_artifacts(
        experiment_root=experiment_root,
        report=report,
        expected_bindings=expected_bindings,
    )
    _validate_usage_ledger_bindings(
        experiment_root=experiment_root,
        repository=repository,
        profile_config=profile_config,
        provider=provider,
        instruction=instruction,
        skill_root=skill_root,
        registration=registration,
        report=report,
        expected_arms=expected_arms,
        expected_bindings=expected_bindings,
    )
    return {arm_id: binding.binding_hash for arm_id, binding in expected_bindings.items()}


def _expected_arms(
    registration: Mapping[str, object],
    report: Mapping[str, object],
) -> dict[str, tuple[tuple[str, ...], tuple[str, ...]]]:
    arms = _mapping_list(report, "arms")
    if len(arms) != 2:
        raise ValueError("paired execution audit requires exactly two report arms")
    control_id = _string(arms[0], "arm_id")
    treatment_id = _string(arms[1], "arm_id")
    if control_id != "general_control" or not treatment_id.startswith("general_plus_"):
        raise ValueError("paired execution audit arm identities are invalid")
    return {
        control_id: (
            _string_tuple(registration, "control_skills"),
            _string_tuple(registration, "control_manifest_hashes"),
        ),
        treatment_id: (
            _string_tuple(registration, "treatment_skills"),
            _string_tuple(registration, "treatment_manifest_hashes"),
        ),
    }


def _reconstruct_bindings(
    *,
    repository: FrozenResearchRepository,
    profile_config: RuntimeConfig,
    provider: PiRuntimeProvider,
    instruction: str,
    expected_arms: Mapping[str, tuple[tuple[str, ...], tuple[str, ...]]],
    skill_root: Path,
    temporary_root: Path,
    experiment_id: str,
) -> dict[str, AgentExecutionBinding]:
    artifacts = ArtifactStore(temporary_root / "artifacts")
    tools = ToolRegistry(artifacts)
    for descriptor in repository.tool_descriptors():
        tools.register(descriptor)
    engine = AgentEngine(
        provider=provider,
        config=profile_config,
        artifact_store=artifacts,
        journal=RunJournal(temporary_root / "run.sqlite3"),
        tool_registry=tools,
        skill_registry=SkillRegistry(skill_root),
        secret_values=(),
    )
    bindings: dict[str, AgentExecutionBinding] = {}
    for arm_id, (skills, registered_manifest_hashes) in expected_arms.items():
        binding = engine.execution_binding(
            AgentRunRequest(
                run_id=f"{experiment_id}.{arm_id}.binding-audit",
                evidence_pack=repository.evidence_pack,
                research_instruction=instruction,
                selected_skills=skills,
                tool_access=ToolAccessContext(
                    allowed_capabilities=ALLOWED_CAPABILITIES,
                    allowed_side_effects=frozenset({ToolSideEffect.READ_ONLY}),
                    allowed_tools=ALLOWED_TOOLS,
                ),
            ),
            runtime_ref=RUNTIME_REF,
        )
        if binding.skill_hashes != registered_manifest_hashes:
            raise ValueError("paired execution audit Skill manifests drifted")
        bindings[arm_id] = binding
    return bindings


def _validate_binding_artifacts(
    *,
    experiment_root: Path,
    report: Mapping[str, object],
    expected_bindings: Mapping[str, AgentExecutionBinding],
) -> None:
    for arm in _mapping_list(report, "arms"):
        arm_id = _string(arm, "arm_id")
        reported_hash = _string(arm, "execution_binding_hash")
        expected_binding = expected_bindings.get(arm_id)
        if expected_binding is None or reported_hash != expected_binding.binding_hash:
            raise ValueError("paired execution report binding differs from expected prompt")
        artifact_path = experiment_root / "artifacts" / reported_hash
        if artifact_path.is_symlink() or not artifact_path.is_file():
            raise ValueError("paired execution binding artifact is unavailable")
        raw_payload: object = json.loads(artifact_path.read_text(encoding="utf-8"))
        if not isinstance(raw_payload, dict):
            raise ValueError("paired execution binding artifact is invalid")
        mapping = cast(dict[object, object], raw_payload)
        if any(not isinstance(key, str) for key in mapping):
            raise ValueError("paired execution binding artifact is invalid")
        payload = cast(dict[str, object], mapping)
        if canonical_hash(payload) != reported_hash:
            raise ValueError("paired execution binding artifact is not content-addressed")
        if payload != expected_binding.to_dict():
            raise ValueError("paired execution binding artifact differs from expected prompt")


def _validate_usage_ledger_bindings(
    *,
    experiment_root: Path,
    repository: FrozenResearchRepository,
    profile_config: RuntimeConfig,
    provider: PiRuntimeProvider,
    instruction: str,
    skill_root: Path,
    registration: Mapping[str, object],
    report: Mapping[str, object],
    expected_arms: Mapping[str, tuple[tuple[str, ...], tuple[str, ...]]],
    expected_bindings: Mapping[str, AgentExecutionBinding],
) -> None:
    ledger_path = experiment_root / "usage.sqlite3"
    if ledger_path.is_symlink() or not ledger_path.is_file():
        raise ValueError("paired execution Usage Ledger is unavailable")
    ledger = UsageLedger(ledger_path)
    if ledger.ledger_hash != report.get("usage_ledger_hash"):
        raise ValueError("paired execution report does not bind its Usage Ledger")
    records = tuple(item.record for item in ledger.records())
    if len(records) != 6 or len({item.run_id for item in records}) != 6:
        raise ValueError("paired execution Usage Ledger must contain six unique runs")
    record_by_run_id = {item.run_id: item for item in records}
    reported_run_ids: set[str] = set()
    experiment_id = _string(registration, "experiment_id")
    for arm in _mapping_list(report, "arms"):
        arm_id = _string(arm, "arm_id")
        expected_binding = expected_bindings[arm_id]
        runs = _mapping_list(arm, "runs")
        if len(runs) != 3:
            raise ValueError("paired execution report arm must contain three runs")
        results: list[AgentRunResult] = []
        run_directories: list[Path] = []
        for replicate_index, run in enumerate(runs, start=1):
            run_id = _string(run, "run_id")
            expected_run_id = f"{experiment_id}.{arm_id}.replicate-{replicate_index}"
            if run_id != expected_run_id:
                raise ValueError("paired execution report run identity drifted")
            reported_run_ids.add(run_id)
            record = record_by_run_id.get(run_id)
            if record is None:
                raise ValueError("paired execution report run is missing from its Usage Ledger")
            if (
                record.experiment_id != experiment_id
                or record.arm_id != arm_id
                or record.execution_binding_hash != expected_binding.binding_hash
                or record.provider_profile_id != registration.get("provider_profile_id")
                or record.provider_profile_hash != registration.get("provider_profile_hash")
            ):
                raise ValueError("paired execution Usage Ledger binding drifted")
            run_directory = experiment_root / "runs" / arm_id / f"replicate-{replicate_index}"
            journal = RunJournal(run_directory / "run.sqlite3")
            if journal.journal_hash(run_id) != record.run_journal_hash:
                raise ValueError("paired execution run journal drifted from Usage Ledger")
            replayed = _replay_terminal_result(
                repository=repository,
                profile_config=profile_config,
                provider=provider,
                instruction=instruction,
                selected_skills=expected_arms[arm_id][0],
                skill_root=skill_root,
                run_directory=run_directory,
                run_id=run_id,
            )
            if (
                replayed.status is not record.status
                or replayed.terminal_store_hash != record.terminal_artifact_hash
                or replayed.metrics is None
                or replayed.metrics.to_dict() != record.metrics.to_dict()
            ):
                raise ValueError("paired execution terminal replay drifted from Usage Ledger")
            if replayed.judgment is not None:
                validate_judgment_execution_binding(
                    replayed.judgment,
                    run_id=run_id,
                    repository=repository,
                    provider_id=provider.provider_id,
                    model=provider.model,
                    expected_binding=expected_binding,
                    artifact_store=ArtifactStore(run_directory / "artifacts"),
                )
            elif record.status.value == "completed":
                raise ValueError("paired execution completed run lacks a Judgment Artifact")
            results.append(replayed)
            run_directories.append(run_directory)
        selected_skills = expected_arms[arm_id][0]
        expected_arm = build_paired_arm_report(
            arm=SkillAblationArm(arm_id=arm_id, selected_skills=selected_skills),
            binding=expected_binding,
            results=tuple(results),
            run_directories=tuple(run_directories),
            repository=repository,
        )
        if dict(arm) != expected_arm:
            raise ValueError("paired execution report arm differs from terminal run evidence")
    if reported_run_ids != set(record_by_run_id):
        raise ValueError("paired execution report and Usage Ledger run sets differ")


def _replay_terminal_result(
    *,
    repository: FrozenResearchRepository,
    profile_config: RuntimeConfig,
    provider: PiRuntimeProvider,
    instruction: str,
    selected_skills: tuple[str, ...],
    skill_root: Path,
    run_directory: Path,
    run_id: str,
) -> AgentRunResult:
    artifacts = ArtifactStore(run_directory / "artifacts")
    tools = ToolRegistry(artifacts)
    for descriptor in repository.tool_descriptors():
        tools.register(descriptor)
    engine = AgentEngine(
        provider=provider,
        config=profile_config,
        artifact_store=artifacts,
        journal=RunJournal(run_directory / "run.sqlite3"),
        tool_registry=tools,
        skill_registry=SkillRegistry(skill_root),
        secret_values=(),
    )
    return asyncio.run(
        engine.run(
            AgentRunRequest(
                run_id=run_id,
                evidence_pack=repository.evidence_pack,
                research_instruction=instruction,
                selected_skills=selected_skills,
                tool_access=ToolAccessContext(
                    allowed_capabilities=ALLOWED_CAPABILITIES,
                    allowed_side_effects=frozenset({ToolSideEffect.READ_ONLY}),
                    allowed_tools=ALLOWED_TOOLS,
                ),
            )
        )
    )


def validate_judgment_execution_binding(
    judgment: JudgmentArtifact,
    *,
    run_id: str,
    repository: FrozenResearchRepository,
    provider_id: str,
    model: str,
    expected_binding: AgentExecutionBinding,
    artifact_store: ArtifactStore,
) -> None:
    if (
        judgment.run_id != run_id
        or judgment.evidence_pack_id != repository.evidence_pack.pack_id
        or judgment.provider_id != provider_id
        or judgment.model != model
        or judgment.runtime_config_hash != expected_binding.runtime_config_hash
        or judgment.prompt_hash != expected_binding.prompt_hash
        or judgment.skill_hashes != expected_binding.skill_hashes
        or judgment.tool_manifest_hashes != expected_binding.tool_manifest_hashes
        or judgment.tool_surface_hash != expected_binding.tool_surface_hash
        or judgment.mcp_server_hashes != expected_binding.mcp_server_hashes
        or judgment.context_estimator_id != expected_binding.context_estimator_id
        or judgment.compactor_id != expected_binding.compactor_id
    ):
        raise ValueError("paired execution Judgment Artifact binding drifted")
    judgment.proposal.validate_against(repository.evidence_pack)
    artifact_store.read_json(judgment.transcript_hash)
    artifact_store.read_json(judgment.raw_response_hash)


def _mapping_list(value: Mapping[str, object], key: str) -> list[Mapping[str, object]]:
    raw = value.get(key)
    if not isinstance(raw, list):
        raise TypeError(f"{key} must be a list of objects")
    values = cast(list[object], raw)
    if any(not isinstance(item, Mapping) for item in values):
        raise TypeError(f"{key} must be a list of objects")
    return [cast(Mapping[str, object], item) for item in values]


def _string(value: Mapping[str, object], key: str) -> str:
    raw = value.get(key)
    if not isinstance(raw, str) or not raw:
        raise TypeError(f"{key} must be a non-empty string")
    return raw


def _string_tuple(value: Mapping[str, object], key: str) -> tuple[str, ...]:
    raw = value.get(key)
    if not isinstance(raw, list):
        raise TypeError(f"{key} must be a list of strings")
    values = cast(list[object], raw)
    if any(not isinstance(item, str) for item in values):
        raise TypeError(f"{key} must be a list of strings")
    return tuple(cast(list[str], values))
