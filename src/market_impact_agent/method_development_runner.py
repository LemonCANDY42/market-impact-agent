from __future__ import annotations

import asyncio
import json
import os
from collections.abc import Awaitable
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Protocol, cast

from market_impact_agent.agent_contracts import EvidencePack, canonical_hash
from market_impact_agent.agent_engine import (
    AgentEngine,
    AgentExecutionBinding,
    AgentRunRequest,
    AgentRunResult,
)
from market_impact_agent.agent_ensemble import (
    AgentEnsembleDecision,
    AgentStudyRegistration,
    ReplicateOutcome,
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
from market_impact_agent.agent_schema import validate_agent_contract
from market_impact_agent.backtests import (
    backtest_request_from_dict,
    canonical_backtest_request_hash,
)
from market_impact_agent.domain import require_aware
from market_impact_agent.frozen_research import FrozenResearchRepository
from market_impact_agent.method_benchmark import (
    BenchmarkTreatmentBinding,
    MethodQualityBenchmarkRegistration,
    MethodQualityEvaluationSpecification,
    load_method_quality_benchmark,
    load_method_quality_evaluation_specification,
)
from market_impact_agent.model_provider import (
    ModelProviderFactory,
    load_model_provider_profile,
)
from market_impact_agent.research_methods import (
    MethodArm,
    load_research_method_catalog,
)
from market_impact_agent.runtime_store import ArtifactStore, RunJournal, RunStatus
from market_impact_agent.usage_ledger import UsageLedger, UsageRecord

METHOD_DEVELOPMENT_CASE_SCHEMA = "market-impact.method-development-case.v1"


class AvailableModelProvider(ModelProvider, Protocol):
    async def assert_model_available(self, *, timeout_seconds: float) -> None: ...


class DevelopmentReplicateRunner(Protocol):
    def __call__(
        self,
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
    ) -> Awaitable[AgentRunResult]: ...


@dataclass(frozen=True, slots=True)
class DevelopmentState:
    state_id: str
    evidence_pack_id: str
    evidence_pack_hash: str
    evidence_documents_hash: str
    pattern_pack_ids: tuple[str, ...]
    pattern_pack_hashes: tuple[str, ...]
    actual_cutoff: datetime
    masked_as_of: datetime
    target_alias: str
    actual_target_id: str
    backtest_request_id: str
    backtest_request_hash: str
    data_snapshot_id: str

    def __post_init__(self) -> None:
        _identifier(self.state_id, "development state_id")
        _nonempty(self.evidence_pack_id, "development evidence_pack_id")
        _sha256(self.evidence_pack_hash, "development evidence_pack_hash")
        _sha256(self.evidence_documents_hash, "development evidence_documents_hash")
        if not self.pattern_pack_ids or len(self.pattern_pack_ids) != len(self.pattern_pack_hashes):
            raise ValueError("development Pattern Pack bindings are incomplete")
        for value in self.pattern_pack_hashes:
            _sha256(value, "development Pattern Pack hash")
        require_aware(self.actual_cutoff, "development actual_cutoff")
        require_aware(self.masked_as_of, "development masked_as_of")
        for name in (
            "target_alias",
            "actual_target_id",
            "backtest_request_id",
            "data_snapshot_id",
        ):
            _nonempty(cast(str, getattr(self, name)), f"development {name}")
        _sha256(self.backtest_request_hash, "development backtest_request_hash")

    def to_dict(self) -> dict[str, object]:
        return {
            "state_id": self.state_id,
            "evidence_pack_id": self.evidence_pack_id,
            "evidence_pack_hash": self.evidence_pack_hash,
            "evidence_documents_hash": self.evidence_documents_hash,
            "pattern_pack_ids": list(self.pattern_pack_ids),
            "pattern_pack_hashes": list(self.pattern_pack_hashes),
            "actual_cutoff": _timestamp(self.actual_cutoff),
            "masked_as_of": _timestamp(self.masked_as_of),
            "target_alias": self.target_alias,
            "actual_target_id": self.actual_target_id,
            "backtest_request_id": self.backtest_request_id,
            "backtest_request_hash": self.backtest_request_hash,
            "data_snapshot_id": self.data_snapshot_id,
        }


@dataclass(frozen=True, slots=True)
class DevelopmentArmBinding:
    suite_id: str
    arm: MethodArm
    route_id: str
    requested_skills: tuple[str, ...]
    manifest_hashes: tuple[str, ...]
    allowed_capabilities: tuple[str, ...]
    allowed_tools: tuple[str, ...]

    def __post_init__(self) -> None:
        _identifier(self.suite_id, "development arm suite_id")
        _nonempty(self.route_id, "development arm route_id")
        for name, values in (
            ("requested_skills", self.requested_skills),
            ("manifest_hashes", self.manifest_hashes),
            ("allowed_capabilities", self.allowed_capabilities),
            ("allowed_tools", self.allowed_tools),
        ):
            if not values or len(values) != len(set(values)):
                raise ValueError(f"development arm {name} must be non-empty and unique")
        if len(self.manifest_hashes) != len(self.requested_skills):
            raise ValueError("development arm Skill identities are incomplete")
        for value in self.manifest_hashes:
            _sha256(value, "development arm manifest hash")

    def to_dict(self) -> dict[str, object]:
        return {
            "suite_id": self.suite_id,
            "arm": self.arm.value,
            "route_id": self.route_id,
            "requested_skills": list(self.requested_skills),
            "manifest_hashes": list(self.manifest_hashes),
            "allowed_capabilities": list(self.allowed_capabilities),
            "allowed_tools": list(self.allowed_tools),
        }

    def matches(self, binding: BenchmarkTreatmentBinding) -> bool:
        return (
            self.suite_id == binding.suite_id
            and self.arm is binding.arm
            and self.route_id == binding.route_id
            and self.requested_skills == binding.requested_skills
            and self.manifest_hashes == binding.manifest_hashes
            and self.allowed_capabilities == binding.allowed_capabilities
            and self.allowed_tools == binding.allowed_tools
        )


@dataclass(frozen=True, slots=True)
class MethodDevelopmentCase:
    case_id: str
    registered_at: datetime
    benchmark_registration_id: str
    benchmark_registration_hash: str
    evaluation_specification_id: str
    evaluation_specification_hash: str
    method_catalog_id: str
    method_catalog_hash: str
    provider_profile_id: str
    provider_profile_hash: str
    provider_id: str
    model: str
    case_alias: str
    actual_event_id: str
    suite_id: str
    event_archetype: str
    runtime_ref: str
    replicate_count: int
    minimum_agreement: int
    allowed_directions: tuple[str, ...]
    eligible_horizons_sessions: tuple[int, ...]
    arm_bindings: tuple[DevelopmentArmBinding, ...]
    states: tuple[DevelopmentState, ...]
    outcomes_known_to_builder: bool
    posthoc_patterns_allowed: bool
    identity_masked: bool
    independent_unit: str
    inference_eligible: bool
    execution_capability: str

    def __post_init__(self) -> None:
        require_aware(self.registered_at, "method development registered_at")
        for name in (
            "benchmark_registration_hash",
            "evaluation_specification_hash",
            "method_catalog_hash",
            "provider_profile_hash",
        ):
            _sha256(cast(str, getattr(self, name)), f"method development {name}")
        for name in (
            "benchmark_registration_id",
            "evaluation_specification_id",
            "method_catalog_id",
            "provider_profile_id",
            "provider_id",
            "model",
            "case_alias",
            "actual_event_id",
            "suite_id",
            "event_archetype",
            "runtime_ref",
        ):
            _nonempty(cast(str, getattr(self, name)), f"method development {name}")
        if self.suite_id != "family_increment":
            raise ValueError("opened energy development case must use family_increment suite")
        if self.event_archetype != "physical_supply_logistics":
            raise ValueError("opened energy development case archetype is invalid")
        if self.replicate_count != 5 or self.minimum_agreement != 3:
            raise ValueError("method development requires three-of-five agreement")
        if self.allowed_directions != ("up",):
            raise ValueError("current method development case is long-or-abstain only")
        if self.eligible_horizons_sessions != (1,):
            raise ValueError("current opened development case freezes one session")
        if tuple(item.arm for item in self.arm_bindings) != tuple(MethodArm):
            raise ValueError("opened case requires four ordered canonical arm bindings")
        if any(item.suite_id != self.suite_id for item in self.arm_bindings):
            raise ValueError("development arm bindings must use the opened case suite")
        if len(self.states) != 2 or tuple(item.state_id for item in self.states) != (
            "attack",
            "recovery",
        ):
            raise ValueError("opened case requires ordered attack and recovery states")
        if (
            len({item.target_alias for item in self.states}) != 1
            or len({item.actual_target_id for item in self.states}) != 1
        ):
            raise ValueError("development states must preserve the same target mapping")
        if not (
            self.outcomes_known_to_builder
            and self.posthoc_patterns_allowed
            and self.identity_masked
        ):
            raise ValueError("opened development limitations must be explicit")
        if self.independent_unit != "one_event_case_not_two_independent_states":
            raise ValueError("development information states cannot be independent cases")
        if self.inference_eligible:
            raise ValueError("opened development case cannot be inference eligible")
        if self.execution_capability != "none":
            raise ValueError("method development case grants no execution capability")
        if self.case_id != self.expected_case_id:
            raise ValueError("method development case_id does not match content")

    @property
    def case_hash(self) -> str:
        return canonical_hash(self.core_dict())

    @property
    def expected_case_id(self) -> str:
        return f"method-development-case-{self.case_hash}"

    def core_dict(self) -> dict[str, object]:
        return {
            "schema_version": METHOD_DEVELOPMENT_CASE_SCHEMA,
            "registered_at": _timestamp(self.registered_at),
            "benchmark_registration_id": self.benchmark_registration_id,
            "benchmark_registration_hash": self.benchmark_registration_hash,
            "evaluation_specification_id": self.evaluation_specification_id,
            "evaluation_specification_hash": self.evaluation_specification_hash,
            "method_catalog_id": self.method_catalog_id,
            "method_catalog_hash": self.method_catalog_hash,
            "provider_profile_id": self.provider_profile_id,
            "provider_profile_hash": self.provider_profile_hash,
            "provider_id": self.provider_id,
            "model": self.model,
            "case_alias": self.case_alias,
            "actual_event_id": self.actual_event_id,
            "suite_id": self.suite_id,
            "event_archetype": self.event_archetype,
            "runtime_ref": self.runtime_ref,
            "replicate_count": self.replicate_count,
            "minimum_agreement": self.minimum_agreement,
            "allowed_directions": list(self.allowed_directions),
            "eligible_horizons_sessions": list(self.eligible_horizons_sessions),
            "arm_bindings": [item.to_dict() for item in self.arm_bindings],
            "states": [item.to_dict() for item in self.states],
            "outcomes_known_to_builder": self.outcomes_known_to_builder,
            "posthoc_patterns_allowed": self.posthoc_patterns_allowed,
            "identity_masked": self.identity_masked,
            "independent_unit": self.independent_unit,
            "inference_eligible": self.inference_eligible,
            "execution_capability": self.execution_capability,
        }

    def to_dict(self) -> dict[str, object]:
        return {**self.core_dict(), "case_id": self.case_id}

    def state(self, state_id: str) -> DevelopmentState:
        try:
            return next(item for item in self.states if item.state_id == state_id)
        except StopIteration as exc:
            raise KeyError(f"unknown development state: {state_id}") from exc

    def arm_binding(self, arm: str) -> DevelopmentArmBinding:
        try:
            return next(item for item in self.arm_bindings if item.arm.value == arm)
        except StopIteration as exc:
            raise KeyError(f"unknown development arm: {arm}") from exc

    def validate_treatments(
        self,
        registration: MethodQualityBenchmarkRegistration,
    ) -> tuple[BenchmarkTreatmentBinding, ...]:
        bindings = tuple(
            item for item in registration.treatment_bindings if item.suite_id == self.suite_id
        )
        if len(bindings) != len(self.arm_bindings) or any(
            not frozen.matches(active)
            for frozen, active in zip(self.arm_bindings, bindings, strict=True)
        ):
            raise ValueError("method development arm bindings do not match active benchmark")
        return bindings

    def validate_against(
        self,
        *,
        registration: MethodQualityBenchmarkRegistration,
        specification: MethodQualityEvaluationSpecification,
        evidence_pack: EvidencePack,
        evidence_documents: dict[str, object],
        pattern_payloads: tuple[dict[str, object], ...],
        backtest_request_payload: dict[str, object],
        state_id: str,
    ) -> None:
        state = self.state(state_id)
        if (
            self.benchmark_registration_id != registration.registration_id
            or self.benchmark_registration_hash != registration.registration_hash
            or self.evaluation_specification_id != specification.specification_id
            or self.evaluation_specification_hash != specification.specification_hash
        ):
            raise ValueError("method development case does not match active benchmark")
        if (
            evidence_pack.pack_id != state.evidence_pack_id
            or canonical_hash(evidence_pack.to_dict()) != state.evidence_pack_hash
            or canonical_hash(evidence_documents) != state.evidence_documents_hash
            or evidence_pack.as_of != state.masked_as_of
            or evidence_pack.allowed_targets != (state.target_alias,)
        ):
            raise ValueError("method development state does not match frozen Agent input")
        if tuple(item.pack_id for item in evidence_pack.pattern_packs) != state.pattern_pack_ids:
            raise ValueError("method development state Pattern Pack ids changed")
        if tuple(canonical_hash(item) for item in pattern_payloads) != state.pattern_pack_hashes:
            raise ValueError("method development state Pattern Pack content changed")
        request = backtest_request_from_dict(backtest_request_payload)
        if (
            request.request_id != state.backtest_request_id
            or canonical_backtest_request_hash(request) != state.backtest_request_hash
            or request.data_snapshot_id != state.data_snapshot_id
            or request.instrument_ids != (state.actual_target_id,)
            or request.horizons_sessions != self.eligible_horizons_sessions
        ):
            raise ValueError("method development state does not match frozen Backtest Request")


@dataclass(frozen=True, slots=True)
class _DevelopmentProtocol:
    provider_id: str
    model: str
    runtime_ref: str
    replicate_count: int
    minimum_agreeing_replicates: int
    allowed_directions: tuple[str, ...]
    eligible_horizons_sessions: tuple[int, ...]
    minimum_candidate_confidence: Decimal


@dataclass(frozen=True, slots=True)
class _DevelopmentStudy:
    registration_id: str
    registration_hash: str
    agent_protocol: _DevelopmentProtocol

    def validate_against(self, registry: object) -> None:
        del registry


async def run_method_development_state(
    *,
    case_path: Path,
    benchmark_registration_path: Path,
    evaluation_specification_path: Path,
    method_catalog_path: Path,
    provider_profile_path: Path,
    state_id: str,
    evidence_pack_path: Path,
    evidence_documents_path: Path,
    pattern_pack_paths: tuple[Path, ...],
    backtest_request_path: Path,
    experiment_id: str,
    skill_root: Path,
    state_root: Path,
    provider: AvailableModelProvider | None = None,
    replicate_runner: DevelopmentReplicateRunner | None = None,
) -> dict[str, object]:
    case = load_method_development_case(case_path)
    registration = load_method_quality_benchmark(benchmark_registration_path)
    specification = load_method_quality_evaluation_specification(evaluation_specification_path)
    catalog = load_research_method_catalog(method_catalog_path)
    profile = load_model_provider_profile(provider_profile_path)
    registration.validate_against(
        catalog=catalog,
        provider_profile=profile,
        skills=SkillRegistry(skill_root),
        evaluation_specification=specification,
    )
    if (
        case.method_catalog_id != catalog.catalog_id
        or case.method_catalog_hash != catalog.catalog_hash
        or case.provider_profile_id != profile.profile_id
        or case.provider_profile_hash != profile.profile_hash
        or case.provider_id != profile.provider_id
        or case.model != profile.model
    ):
        raise ValueError("method development case dependency identity changed")
    repository = FrozenResearchRepository.from_files(
        evidence_pack_path=evidence_pack_path,
        evidence_documents_path=evidence_documents_path,
        pattern_pack_paths=pattern_pack_paths,
    )
    documents_payload = _read_object(evidence_documents_path)
    pattern_payloads = tuple(_read_object(path) for path in pattern_pack_paths)
    backtest_request_payload = _read_object(backtest_request_path)
    case.validate_against(
        registration=registration,
        specification=specification,
        evidence_pack=repository.evidence_pack,
        evidence_documents=documents_payload,
        pattern_payloads=pattern_payloads,
        backtest_request_payload=backtest_request_payload,
        state_id=state_id,
    )
    bindings = case.validate_treatments(registration)
    selected_provider = provider or cast(
        AvailableModelProvider,
        ModelProviderFactory.with_builtin_adapters().create(profile),
    )
    if (
        selected_provider.provider_id != profile.provider_id
        or selected_provider.model != profile.model
    ):
        raise ValueError("active Model Provider does not match development case")
    config = profile.runtime_config()
    instruction = _research_instruction(case, repository.evidence_pack)
    experiment_root = state_root / canonical_hash(experiment_id)
    artifact_store = ArtifactStore(experiment_root / "artifacts")
    usage_ledger = UsageLedger(experiment_root / "usage.sqlite3")
    secret_values = (os.environ.get(profile.credential_env, ""),)
    execution_bindings: dict[MethodArm, AgentExecutionBinding] = {}
    for binding in bindings:
        frozen = _freeze_execution_binding(
            repository=repository,
            provider=selected_provider,
            config=config,
            binding=binding,
            research_instruction=instruction,
            experiment_id=experiment_id,
            state_id=state_id,
            runtime_ref=case.runtime_ref,
            skill_root=skill_root,
            state_directory=experiment_root,
            artifact_store=artifact_store,
            secret_values=secret_values,
        )
        stored = artifact_store.put_json(
            frozen.to_dict(),
            media_type="application/vnd.market-impact.agent-execution-binding+json",
        )
        if stored.content_hash != frozen.binding_hash:
            raise AssertionError("development execution binding is inconsistent")
        execution_bindings[binding.arm] = frozen

    await selected_provider.assert_model_available(timeout_seconds=30)
    runner = replicate_runner or _run_replicate
    results: dict[MethodArm, list[AgentRunResult]] = {item.arm: [] for item in bindings}
    for replicate_index in range(1, case.replicate_count + 1):
        round_results = await asyncio.gather(
            *(
                runner(
                    repository=repository,
                    provider=selected_provider,
                    config=config,
                    binding=binding,
                    research_instruction=instruction,
                    run_id=(
                        f"{experiment_id}.{state_id}.{binding.arm.value}."
                        f"replicate-{replicate_index}"
                    ),
                    runtime_ref=case.runtime_ref,
                    skill_root=skill_root,
                    state_directory=(
                        experiment_root
                        / "runs"
                        / binding.arm.value
                        / f"replicate-{replicate_index}"
                    ),
                    secret_values=secret_values,
                )
                for binding in bindings
            )
        )
        for binding, result in zip(bindings, round_results, strict=True):
            results[binding.arm].append(result)
            journal = RunJournal(
                experiment_root
                / "runs"
                / binding.arm.value
                / f"replicate-{replicate_index}"
                / "run.sqlite3"
            )
            usage_ledger.append(
                UsageRecord.from_result(
                    experiment_id=experiment_id,
                    arm_id=binding.arm.value,
                    recorded_at=journal.get_run(result.run_id).updated_at,
                    provider_profile_id=profile.profile_id,
                    provider_profile_hash=profile.profile_hash,
                    execution_binding_hash=execution_bindings[binding.arm].binding_hash,
                    run_journal_hash=journal.journal_hash(result.run_id),
                    result=result,
                )
            )

    incomplete = [
        (arm.value, index, result.status.value)
        for arm, arm_results in results.items()
        for index, result in enumerate(arm_results, start=1)
        if result.status is not RunStatus.COMPLETED or result.judgment is None
    ]
    if incomplete:
        raise ValueError(
            "method development state requires 20 completed runs with judgments; "
            f"invalid replicates: {incomplete}"
        )

    protocol = _DevelopmentProtocol(
        provider_id=profile.provider_id,
        model=profile.model,
        runtime_ref=case.runtime_ref,
        replicate_count=case.replicate_count,
        minimum_agreeing_replicates=case.minimum_agreement,
        allowed_directions=case.allowed_directions,
        eligible_horizons_sessions=case.eligible_horizons_sessions,
        minimum_candidate_confidence=Decimal("0.5"),
    )
    decisions: dict[MethodArm, AgentEnsembleDecision] = {}
    for binding in bindings:
        decision = aggregate_agent_replicates(
            ensemble_run_id=f"{experiment_id}.{state_id}.{binding.arm.value}",
            registration=cast(
                AgentStudyRegistration,
                _DevelopmentStudy(
                    registration_id=case.case_id,
                    registration_hash=case.case_hash,
                    agent_protocol=protocol,
                ),
            ),
            evidence_pack=repository.evidence_pack,
            results=tuple(results[binding.arm]),
            frozen_execution_binding_hash=execution_bindings[binding.arm].binding_hash,
        )
        if any(
            assessment.run_status is not RunStatus.COMPLETED
            or assessment.outcome is ReplicateOutcome.INVALID
            or assessment.judgment_artifact_id is None
            or assessment.execution_binding_hash != execution_bindings[binding.arm].binding_hash
            for assessment in decision.assessments
        ):
            raise ValueError(
                "method development state requires 20 completed runs with valid judgments"
            )
        decisions[binding.arm] = decision

    arm_reports: list[dict[str, object]] = []
    for binding in bindings:
        decision = decisions[binding.arm]
        totals = _totals(tuple(results[binding.arm]))
        replicate_run_ids = [item.run_id for item in results[binding.arm]]
        stored_decision = artifact_store.put_json(
            decision.to_dict(),
            media_type="application/vnd.market-impact.agent-ensemble-decision+json",
        )
        arm_reports.append(
            {
                "arm": binding.arm.value,
                "route_id": binding.route_id,
                "requested_skills": list(binding.requested_skills),
                "allowed_capabilities": list(binding.allowed_capabilities),
                "allowed_tools": list(binding.allowed_tools),
                "execution_binding_run_id": (
                    f"{experiment_id}.{state_id}.{binding.arm.value}.binding-preflight"
                ),
                "execution_binding": execution_bindings[binding.arm].to_dict(),
                "execution_binding_hash": execution_bindings[binding.arm].binding_hash,
                "execution_binding_identity_hash": canonical_hash(
                    {
                        "experiment_id": experiment_id,
                        "state_id": state_id,
                        "arm": binding.arm.value,
                        "preflight_run_id": (
                            f"{experiment_id}.{state_id}.{binding.arm.value}.binding-preflight"
                        ),
                        "binding_hash": execution_bindings[binding.arm].binding_hash,
                    }
                ),
                "decision": decision.to_dict(),
                "decision_artifact_hash": stored_decision.content_hash,
                "run_statuses": [item.status.value for item in results[binding.arm]],
                "totals": totals,
                "totals_binding_hash": canonical_hash(
                    {
                        "experiment_id": experiment_id,
                        "state_id": state_id,
                        "arm": binding.arm.value,
                        "replicate_run_ids": replicate_run_ids,
                        "totals": totals,
                    }
                ),
            }
        )
    report_core = {
        "schema_version": "market-impact.method-development-report.v1",
        "experiment_id": experiment_id,
        "case_id": case.case_id,
        "case_hash": case.case_hash,
        "state_id": state_id,
        "evidence_pack_id": repository.evidence_pack.pack_id,
        "evidence_pack_hash": canonical_hash(repository.evidence_pack.to_dict()),
        "provider_profile_id": profile.profile_id,
        "provider_profile_hash": profile.profile_hash,
        "arms": arm_reports,
        "usage_ledger_hash": usage_ledger.ledger_hash,
        "outcomes_used_by_agent": False,
        "outcomes_known_to_builder": True,
        "identity_masked": True,
        "inference_eligible": False,
        "claim_scope": "opened_development_diagnostic_only",
        "broker_reachability": False,
        "execution_capability": "none",
    }
    report = {
        **report_core,
        "report_id": f"method-development-report-{canonical_hash(report_core)}",
    }
    stored_report = artifact_store.put_json(
        report,
        media_type="application/vnd.market-impact.method-development-report+json",
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
    binding: BenchmarkTreatmentBinding,
    research_instruction: str,
    run_id: str,
    runtime_ref: str,
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
    request = _run_request(
        run_id=run_id,
        repository=repository,
        binding=binding,
        research_instruction=research_instruction,
    )
    del runtime_ref
    return await engine.run(request)


def _freeze_execution_binding(
    *,
    repository: FrozenResearchRepository,
    provider: ModelProvider,
    config: RuntimeConfig,
    binding: BenchmarkTreatmentBinding,
    research_instruction: str,
    experiment_id: str,
    state_id: str,
    runtime_ref: str,
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
        journal=RunJournal(state_directory / f"binding-{binding.arm.value}.sqlite3"),
        secret_values=secret_values,
    )
    return engine.execution_binding(
        _run_request(
            run_id=f"{experiment_id}.{state_id}.{binding.arm.value}.binding-preflight",
            repository=repository,
            binding=binding,
            research_instruction=research_instruction,
        ),
        runtime_ref=runtime_ref,
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
    binding: BenchmarkTreatmentBinding,
    research_instruction: str,
) -> AgentRunRequest:
    return AgentRunRequest(
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


def _research_instruction(case: MethodDevelopmentCase, evidence_pack: EvidencePack) -> str:
    targets = ", ".join(evidence_pack.allowed_targets)
    horizons = ", ".join(str(item) for item in case.eligible_horizons_sessions)
    return (
        "Assess this identity-masked, opened development case without using information outside "
        "the Evidence Pack. Read every evidence item before deciding. Use only the registered "
        "read-only tools and selected research methods. Test material counterevidence and "
        "abstain when the commodity-to-target link or persistence is unresolved. Do not infer "
        "the historical identity or use memorized outcomes. "
        f"Eligible targets are [{targets}], eligible direction is up, and eligible horizons in "
        f"sessions are [{horizons}]. A candidate requires confidence at least 0.5. Return "
        "exactly one eligible candidate or abstain."
    )


def _totals(results: tuple[AgentRunResult, ...]) -> dict[str, object]:
    metrics = tuple(item.metrics for item in results if item.metrics is not None)
    return {
        "turns": sum(item.turns for item in metrics),
        "tool_calls": sum(item.tool_calls for item in metrics),
        "input_tokens": sum(item.input_tokens for item in metrics),
        "output_tokens": sum(item.output_tokens for item in metrics),
        "result_bytes": sum(item.result_bytes for item in metrics),
        "latency_ms": sum(item.latency_ms for item in metrics),
        "provider_attempts": sum(item.provider_attempts for item in metrics),
        "estimated_cost_microusd": sum(item.estimated_cost_microusd for item in metrics),
    }


def load_method_development_case(path: Path) -> MethodDevelopmentCase:
    payload = _read_object(path)
    schema_errors = validate_agent_contract(payload, "method-development-case.schema.json")
    if schema_errors:
        raise ValueError("invalid method development case: " + "; ".join(schema_errors))
    expected = {
        "schema_version",
        "case_id",
        "registered_at",
        "benchmark_registration_id",
        "benchmark_registration_hash",
        "evaluation_specification_id",
        "evaluation_specification_hash",
        "method_catalog_id",
        "method_catalog_hash",
        "provider_profile_id",
        "provider_profile_hash",
        "provider_id",
        "model",
        "case_alias",
        "actual_event_id",
        "suite_id",
        "event_archetype",
        "runtime_ref",
        "replicate_count",
        "minimum_agreement",
        "allowed_directions",
        "eligible_horizons_sessions",
        "arm_bindings",
        "states",
        "outcomes_known_to_builder",
        "posthoc_patterns_allowed",
        "identity_masked",
        "independent_unit",
        "inference_eligible",
        "execution_capability",
    }
    if set(payload) != expected or payload.get("schema_version") != METHOD_DEVELOPMENT_CASE_SCHEMA:
        raise ValueError("method development case fields are invalid")
    raw_states = payload.get("states")
    if not isinstance(raw_states, list):
        raise TypeError("method development states must be an array")
    states = tuple(_development_state(item) for item in cast(list[object], raw_states))
    raw_arm_bindings = payload.get("arm_bindings")
    if not isinstance(raw_arm_bindings, list):
        raise TypeError("method development arm_bindings must be an array")
    arm_bindings = tuple(
        _development_arm_binding(item) for item in cast(list[object], raw_arm_bindings)
    )
    result = MethodDevelopmentCase(
        case_id=_string(payload, "case_id"),
        registered_at=_datetime(payload, "registered_at"),
        benchmark_registration_id=_string(payload, "benchmark_registration_id"),
        benchmark_registration_hash=_string(payload, "benchmark_registration_hash"),
        evaluation_specification_id=_string(payload, "evaluation_specification_id"),
        evaluation_specification_hash=_string(payload, "evaluation_specification_hash"),
        method_catalog_id=_string(payload, "method_catalog_id"),
        method_catalog_hash=_string(payload, "method_catalog_hash"),
        provider_profile_id=_string(payload, "provider_profile_id"),
        provider_profile_hash=_string(payload, "provider_profile_hash"),
        provider_id=_string(payload, "provider_id"),
        model=_string(payload, "model"),
        case_alias=_string(payload, "case_alias"),
        actual_event_id=_string(payload, "actual_event_id"),
        suite_id=_string(payload, "suite_id"),
        event_archetype=_string(payload, "event_archetype"),
        runtime_ref=_string(payload, "runtime_ref"),
        replicate_count=_integer(payload, "replicate_count"),
        minimum_agreement=_integer(payload, "minimum_agreement"),
        allowed_directions=_string_tuple(payload, "allowed_directions"),
        eligible_horizons_sessions=_integer_tuple(payload, "eligible_horizons_sessions"),
        arm_bindings=arm_bindings,
        states=states,
        outcomes_known_to_builder=_boolean(payload, "outcomes_known_to_builder"),
        posthoc_patterns_allowed=_boolean(payload, "posthoc_patterns_allowed"),
        identity_masked=_boolean(payload, "identity_masked"),
        independent_unit=_string(payload, "independent_unit"),
        inference_eligible=_boolean(payload, "inference_eligible"),
        execution_capability=_string(payload, "execution_capability"),
    )
    if result.to_dict() != payload:
        raise ValueError("method development case does not match canonical contract")
    return result


def _development_arm_binding(value: object) -> DevelopmentArmBinding:
    if not isinstance(value, dict):
        raise TypeError("method development arm binding must be an object")
    raw = cast(dict[object, object], value)
    if any(not isinstance(key, str) for key in raw):
        raise TypeError("method development arm binding keys must be strings")
    payload = cast(dict[str, object], raw)
    expected = {
        "suite_id",
        "arm",
        "route_id",
        "requested_skills",
        "manifest_hashes",
        "allowed_capabilities",
        "allowed_tools",
    }
    if set(payload) != expected:
        raise ValueError("method development arm binding fields are invalid")
    return DevelopmentArmBinding(
        suite_id=_string(payload, "suite_id"),
        arm=MethodArm(_string(payload, "arm")),
        route_id=_string(payload, "route_id"),
        requested_skills=_string_tuple(payload, "requested_skills"),
        manifest_hashes=_string_tuple(payload, "manifest_hashes"),
        allowed_capabilities=_string_tuple(payload, "allowed_capabilities"),
        allowed_tools=_string_tuple(payload, "allowed_tools"),
    )


def _development_state(value: object) -> DevelopmentState:
    if not isinstance(value, dict):
        raise TypeError("method development state must be an object")
    raw = cast(dict[object, object], value)
    if any(not isinstance(key, str) for key in raw):
        raise TypeError("method development state keys must be strings")
    payload = cast(dict[str, object], raw)
    expected = {
        "state_id",
        "evidence_pack_id",
        "evidence_pack_hash",
        "evidence_documents_hash",
        "pattern_pack_ids",
        "pattern_pack_hashes",
        "actual_cutoff",
        "masked_as_of",
        "target_alias",
        "actual_target_id",
        "backtest_request_id",
        "backtest_request_hash",
        "data_snapshot_id",
    }
    if set(payload) != expected:
        raise ValueError("method development state fields are invalid")
    return DevelopmentState(
        state_id=_string(payload, "state_id"),
        evidence_pack_id=_string(payload, "evidence_pack_id"),
        evidence_pack_hash=_string(payload, "evidence_pack_hash"),
        evidence_documents_hash=_string(payload, "evidence_documents_hash"),
        pattern_pack_ids=_string_tuple(payload, "pattern_pack_ids"),
        pattern_pack_hashes=_string_tuple(payload, "pattern_pack_hashes"),
        actual_cutoff=_datetime(payload, "actual_cutoff"),
        masked_as_of=_datetime(payload, "masked_as_of"),
        target_alias=_string(payload, "target_alias"),
        actual_target_id=_string(payload, "actual_target_id"),
        backtest_request_id=_string(payload, "backtest_request_id"),
        backtest_request_hash=_string(payload, "backtest_request_hash"),
        data_snapshot_id=_string(payload, "data_snapshot_id"),
    )


def _read_object(path: Path) -> dict[str, object]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise TypeError(f"expected JSON object: {path}")
    raw = cast(dict[object, object], payload)
    if any(not isinstance(key, str) for key in raw):
        raise TypeError(f"expected string-keyed JSON object: {path}")
    return cast(dict[str, object], raw)


def _string(payload: dict[str, object], name: str) -> str:
    value = payload.get(name)
    if not isinstance(value, str):
        raise TypeError(f"{name} must be a string")
    return value


def _integer(payload: dict[str, object], name: str) -> int:
    value = payload.get(name)
    if not isinstance(value, int) or isinstance(value, bool):
        raise TypeError(f"{name} must be an integer")
    return value


def _boolean(payload: dict[str, object], name: str) -> bool:
    value = payload.get(name)
    if not isinstance(value, bool):
        raise TypeError(f"{name} must be a boolean")
    return value


def _datetime(payload: dict[str, object], name: str) -> datetime:
    value = _string(payload, name)
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    require_aware(parsed, name)
    return parsed


def _string_tuple(payload: dict[str, object], name: str) -> tuple[str, ...]:
    value = payload.get(name)
    if not isinstance(value, list):
        raise TypeError(f"{name} must be a string array")
    raw = cast(list[object], value)
    if any(not isinstance(item, str) for item in raw):
        raise TypeError(f"{name} must be a string array")
    return tuple(cast(list[str], raw))


def _integer_tuple(payload: dict[str, object], name: str) -> tuple[int, ...]:
    value = payload.get(name)
    if not isinstance(value, list):
        raise TypeError(f"{name} must be an integer array")
    raw = cast(list[object], value)
    if any(not isinstance(item, int) or isinstance(item, bool) for item in raw):
        raise TypeError(f"{name} must be an integer array")
    return tuple(cast(list[int], raw))


def _timestamp(value: datetime) -> str:
    return value.isoformat().replace("+00:00", "Z")


def _nonempty(value: str, name: str) -> None:
    if not value or value != value.strip():
        raise ValueError(f"{name} must be a non-empty trimmed string")


def _identifier(value: str, name: str) -> None:
    _nonempty(value, name)
    if any(character not in "abcdefghijklmnopqrstuvwxyz0123456789_-" for character in value):
        raise ValueError(f"{name} must use lowercase identifier characters")


def _sha256(value: str, name: str) -> None:
    if len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
        raise ValueError(f"{name} must be a lowercase sha256")
