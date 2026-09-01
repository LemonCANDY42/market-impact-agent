"""Authorize and execute one bounded semantic callback for a durable Attention Wake.

The dispatcher proves callback membership and identity.  This module reopens that
authority, validates the exact frozen monitoring Snapshot, freezes only the newly
received versions, and runs the registered coordinator-only Triage callback.  It
can create a Triage Decision; it cannot create a Signal, Order Intent, or broker
operation.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import cast

from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.agent_engine import CancellationToken
from market_impact_agent.agent_runtime import ModelProvider, SkillRegistry
from market_impact_agent.agent_watch_admission import (
    EventImpactTriageWatchAuthority,
    EventImpactTriageWatchAuthorityResolver,
    WatchCallbackBinding,
    build_callback_agent_profile_ref,
)
from market_impact_agent.agent_watch_wake_dispatch import (
    AGENT_WATCH_WAKE_RUN_BOUND_EVENT,
    AgentWatchWakeDispatch,
    AgentWatchWakeDispatcher,
)
from market_impact_agent.data_inputs import DataPITLane
from market_impact_agent.event_impact_triage import (
    EVENT_IMPACT_TRIAGE_CANDIDATE_SET_SCHEMA_V2,
    EventImpactTriageCandidateSet,
    EventImpactTriageDecision,
    TriageAgentRole,
    TriageObservationRef,
)
from market_impact_agent.event_impact_triage_runtime import (
    EventImpactTriageExecutionPlan,
    EventImpactTriageRunner,
    EventImpactTriageRunResult,
    SnapshotTriageCandidateContentResolver,
    TriageComparisonArm,
    build_event_impact_triage_execution_plan_v3,
)
from market_impact_agent.event_impact_triage_store import EventImpactTriageDecisionStore
from market_impact_agent.model_provider import (
    ModelProviderProfile,
    load_builtin_model_provider_profile,
)
from market_impact_agent.monitoring_scope import RetrievalOutcome, resolve_retrieval
from market_impact_agent.observations import AvailabilityBasis, ObservationCapability
from market_impact_agent.prospective_data import (
    ProspectiveDataJournal,
    prospective_observation_version_id,
)
from market_impact_agent.prospective_diagnostic import ProspectiveDiagnosticRegistration
from market_impact_agent.runtime_store import ArtifactStore, RunJournal, RunStatus
from market_impact_agent.usage_ledger import UsageLedger

AGENT_WATCH_WAKE_JUDGMENT_PLAN_SCHEMA = "market-impact.agent-watch-wake-judgment-plan.v1"
AGENT_WATCH_WAKE_JUDGMENT_FINISHED_EVENT = "agent.watch-wake.judgment-finished"
AGENT_WATCH_WAKE_JUDGMENT_RESULT_SCHEMA = "market-impact.agent-watch-wake-judgment-result.v1"


@dataclass(frozen=True, slots=True)
class AgentWatchWakeJudgmentPlan:
    plan_id: str
    dispatch_binding_id: str
    dispatch_binding_hash: str
    wake_id: str
    admission_id: str
    parent_cluster_id: str
    source_data_snapshot_id: str
    retrieval_resolution_id: str
    frozen_data_snapshot_id: str
    candidate_set_id: str
    candidate_set_hash: str
    triage_execution_plan_id: str
    triage_execution_plan_hash: str
    callback_agent_profile_ref: str
    model_profile_alias: str
    model_profile_id: str
    model_profile_hash: str
    direct_skill_manifest_hashes: tuple[str, ...]
    research_only: bool = True
    judgment_model_calls_authorized: bool = True
    execution_capability: bool = False
    schema_version: str = AGENT_WATCH_WAKE_JUDGMENT_PLAN_SCHEMA

    def __post_init__(self) -> None:
        if self.schema_version != AGENT_WATCH_WAKE_JUDGMENT_PLAN_SCHEMA:
            raise ValueError("unsupported Agent Watch Wake Judgment Plan schema")
        for value, prefix, name in (
            (self.dispatch_binding_id, "agent-watch-wake-run-binding-", "dispatch binding"),
            (self.wake_id, "attention-wake-", "Wake"),
            (self.admission_id, "agent-watch-admission-", "Watch admission"),
            (self.parent_cluster_id, "event-impact-triage-cluster-", "parent cluster"),
            (self.source_data_snapshot_id, "data-snapshot-", "source Data Snapshot"),
            (self.retrieval_resolution_id, "retrieval-resolution-", "retrieval resolution"),
            (self.frozen_data_snapshot_id, "data-snapshot-", "frozen Data Snapshot"),
            (self.candidate_set_id, "event-impact-triage-candidate-set-", "Candidate Set"),
            (
                self.triage_execution_plan_id,
                "event-impact-triage-execution-plan-",
                "Triage execution plan",
            ),
            (self.callback_agent_profile_ref, "agent-profile-", "callback Agent profile"),
            (self.model_profile_id, "model-provider-", "model Provider profile"),
        ):
            _prefixed_hash(value, prefix, name)
        for value, name in (
            (self.dispatch_binding_hash, "dispatch binding hash"),
            (self.candidate_set_hash, "Candidate Set hash"),
            (self.triage_execution_plan_hash, "Triage execution plan hash"),
            (self.model_profile_hash, "model Provider profile hash"),
        ):
            _sha256(value, name)
        _trimmed(self.model_profile_alias, "model profile alias")
        if not self.direct_skill_manifest_hashes:
            raise ValueError("Wake Judgment Plan requires at least one registered Skill")
        for value in self.direct_skill_manifest_hashes:
            _sha256(value, "direct Skill manifest hash")
        if not self.research_only or not self.judgment_model_calls_authorized:
            raise ValueError("Wake Judgment Plan must authorize only the bounded research call")
        if self.execution_capability:
            raise ValueError("Wake Judgment Plan cannot grant execution capability")
        if self.plan_id != self.expected_plan_id:
            raise ValueError("Wake Judgment plan_id does not match content")

    @property
    def expected_plan_id(self) -> str:
        return f"agent-watch-wake-judgment-plan-{canonical_hash(self.core_dict())}"

    def core_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "dispatch_binding_id": self.dispatch_binding_id,
            "dispatch_binding_hash": self.dispatch_binding_hash,
            "wake_id": self.wake_id,
            "admission_id": self.admission_id,
            "parent_cluster_id": self.parent_cluster_id,
            "source_data_snapshot_id": self.source_data_snapshot_id,
            "retrieval_resolution_id": self.retrieval_resolution_id,
            "frozen_data_snapshot_id": self.frozen_data_snapshot_id,
            "candidate_set_id": self.candidate_set_id,
            "candidate_set_hash": self.candidate_set_hash,
            "triage_execution_plan_id": self.triage_execution_plan_id,
            "triage_execution_plan_hash": self.triage_execution_plan_hash,
            "callback_agent_profile_ref": self.callback_agent_profile_ref,
            "model_profile_alias": self.model_profile_alias,
            "model_profile_id": self.model_profile_id,
            "model_profile_hash": self.model_profile_hash,
            "direct_skill_manifest_hashes": list(self.direct_skill_manifest_hashes),
            "research_only": self.research_only,
            "judgment_model_calls_authorized": self.judgment_model_calls_authorized,
            "execution_capability": self.execution_capability,
        }

    def to_dict(self) -> dict[str, object]:
        return {**self.core_dict(), "plan_id": self.plan_id}


@dataclass(frozen=True, slots=True)
class PreparedAgentWatchWakeJudgment:
    plan: AgentWatchWakeJudgmentPlan
    dispatch: AgentWatchWakeDispatch
    callback: WatchCallbackBinding
    candidate_set: EventImpactTriageCandidateSet
    triage_plan: EventImpactTriageExecutionPlan
    model_profile: ModelProviderProfile


@dataclass(frozen=True, slots=True)
class AgentWatchWakeJudgmentResult:
    plan: AgentWatchWakeJudgmentPlan
    triage_result: EventImpactTriageRunResult
    decision: EventImpactTriageDecision | None


class AgentWatchWakeJudgmentExecutor:
    """Harness-owned bridge from a dispatch reservation to semantic Triage authority."""

    def __init__(
        self,
        dispatcher: AgentWatchWakeDispatcher,
        *,
        registration: ProspectiveDiagnosticRegistration,
        model_profile_alias_by_agent_profile_ref: Mapping[str, str],
        skill_root: Path,
        runtime_root: Path,
    ) -> None:
        if type(dispatcher) is not AgentWatchWakeDispatcher:
            raise TypeError("Wake Judgment execution requires the concrete dispatcher authority")
        if not model_profile_alias_by_agent_profile_ref:
            raise ValueError("Wake Judgment execution requires registered Agent profile mappings")
        self.dispatcher = dispatcher
        self.admission_service = dispatcher.admission_service
        self.store = self.admission_service.store
        self.journal = self.admission_service.journal
        self.registration = registration
        self.profile_aliases = dict(model_profile_alias_by_agent_profile_ref)
        self.skills = SkillRegistry(skill_root)
        self.runtime_root = runtime_root.resolve()
        self.runtime_root.mkdir(mode=0o700, parents=True, exist_ok=True)
        self.decision_store = EventImpactTriageDecisionStore(self.store.root)

    def prepare(self, dispatch: AgentWatchWakeDispatch) -> PreparedAgentWatchWakeJudgment:
        binding = dispatch.binding
        callback = self._reopen_dispatch(dispatch)
        authority = self._triage_authority(binding.parent_ref)
        parent_candidate, _, _ = authority.decision_store.get_context(authority.candidate_set_id)
        if (
            binding.parent_ref != authority.cluster_id
            or binding.parent_ref != callback.admission.parent_ref
            or parent_candidate.registration_id != self.registration.registration_id
        ):
            raise ValueError("Wake callback differs from its registered parent route authority")

        policy = self.dispatcher.watch_service.policy(binding.watch_id)
        if policy.retrieval_plan is None or policy.monitoring_scope is None:
            raise ValueError("Wake callback requires a scope-aware Retrieval Plan")
        if (
            policy.retrieval_plan.plan_id != binding.retrieval_plan_id
            or policy.monitoring_scope.scope_id != binding.monitoring_scope_id
            or policy.collection_policy_id != binding.collection_policy_id
        ):
            raise ValueError("Wake callback bindings differ from the authoritative Watch policy")
        resolution = resolve_retrieval(
            policy.retrieval_plan,
            requested_at=callback.wake.created_at,
            journal=self.journal,
            journal_snapshot_id=callback.wake.data_snapshot_id,
            fetch_permitted=False,
        )
        if (
            resolution.outcome is not RetrievalOutcome.JOURNAL_FREEZE
            or resolution.selected_snapshot_ids != (callback.wake.data_snapshot_id,)
        ):
            raise ValueError(
                "Wake source Snapshot does not satisfy its frozen Retrieval Plan: "
                + ",".join(item.value for item in resolution.gaps)
            )

        refs = self.journal.observation_version_refs_by_ids(callback.wake.new_version_ids)
        ordered_version_ids = tuple(item.version_id for item in refs)
        if any(item.first_available_at > callback.wake.created_at for item in refs):
            raise ValueError("Wake contains an Observation Version after its cutoff")
        frozen_at = dispatch.run.created_at
        selection_id = f"agent-watch-wake-selection-{canonical_hash(binding.core_dict())}"
        selected_snapshot = self.journal.freeze_version_selection_snapshot(
            selection_id=selection_id,
            readiness_report_id=parent_candidate.readiness_report_id,
            version_ids=ordered_version_ids,
            as_of=callback.wake.created_at,
            frozen_at=frozen_at,
        )
        candidate_set = _build_wake_candidate_set(
            parent=parent_candidate,
            callback=callback,
            dispatch=dispatch,
            snapshot_id=selected_snapshot.snapshot_id,
            selection_id=selection_id,
            ordered_version_ids=ordered_version_ids,
            journal=self.journal,
            frozen_at=frozen_at,
        )

        alias = self.profile_aliases.get(callback.profile.callback_agent_profile_ref)
        if alias is None:
            raise ValueError("Wake callback Agent profile has no registered model profile mapping")
        model_profile = load_builtin_model_provider_profile(alias)
        if callback.profile.callback_max_cost_microusd < 1:
            raise ValueError("Wake callback requires a positive registered cost ceiling")
        expected_agent_profile_ref = build_callback_agent_profile_ref(
            callback_agent_type=callback.profile.callback_agent_type,
            model_profile_id=model_profile.profile_id,
            model_profile_hash=model_profile.profile_hash,
            preloaded_skills=callback.profile.preloaded_skills,
            skill_manifest_hashes=callback.profile.skill_manifest_hashes,
            max_turns=callback.profile.callback_max_turns,
            max_input_tokens=callback.profile.callback_max_input_tokens,
            max_output_tokens=callback.profile.callback_max_output_tokens,
            max_cost_microusd=callback.profile.callback_max_cost_microusd,
        )
        if callback.profile.callback_agent_profile_ref != expected_agent_profile_ref:
            raise ValueError(
                "Wake callback Agent profile does not bind its model, Skills, and budget"
            )
        triage_plan = build_event_impact_triage_execution_plan_v3(
            arm=TriageComparisonArm.BASELINE,
            candidate_set=candidate_set,
            registration=self.registration,
            model_profile_alias=alias,
            model_profile=model_profile,
            skills=self.skills,
            max_turns=callback.profile.callback_max_turns,
            max_input_tokens=callback.profile.callback_max_input_tokens,
            max_output_tokens=callback.profile.callback_max_output_tokens,
            max_estimated_cost_microusd=callback.profile.callback_max_cost_microusd,
            coordinator_skills=callback.profile.preloaded_skills,
        )
        coordinator = triage_plan.binding(TriageAgentRole.COORDINATOR)
        resolved_hashes = dict(
            zip(
                coordinator.resolved_skill_names,
                coordinator.skill_manifest_hashes,
                strict=True,
            )
        )
        if tuple(resolved_hashes.get(name) for name in callback.profile.preloaded_skills) != (
            callback.profile.skill_manifest_hashes
        ):
            raise ValueError("Wake callback Skills differ from the registered delegate profile")

        core = {
            "schema_version": AGENT_WATCH_WAKE_JUDGMENT_PLAN_SCHEMA,
            "dispatch_binding_id": binding.binding_id,
            "dispatch_binding_hash": dispatch.binding_artifact_hash,
            "wake_id": binding.wake_id,
            "admission_id": binding.admission_id,
            "parent_cluster_id": binding.parent_ref,
            "source_data_snapshot_id": binding.data_snapshot_id,
            "retrieval_resolution_id": resolution.resolution_id,
            "frozen_data_snapshot_id": selected_snapshot.snapshot_id,
            "candidate_set_id": candidate_set.candidate_set_id,
            "candidate_set_hash": canonical_hash(candidate_set.to_dict()),
            "triage_execution_plan_id": triage_plan.plan_id,
            "triage_execution_plan_hash": canonical_hash(triage_plan.to_dict()),
            "callback_agent_profile_ref": binding.callback_agent_profile_ref,
            "model_profile_alias": alias,
            "model_profile_id": model_profile.profile_id,
            "model_profile_hash": model_profile.profile_hash,
            "direct_skill_manifest_hashes": list(callback.profile.skill_manifest_hashes),
            "research_only": True,
            "judgment_model_calls_authorized": True,
            "execution_capability": False,
        }
        plan = AgentWatchWakeJudgmentPlan(
            plan_id=f"agent-watch-wake-judgment-plan-{canonical_hash(core)}",
            dispatch_binding_id=binding.binding_id,
            dispatch_binding_hash=dispatch.binding_artifact_hash,
            wake_id=binding.wake_id,
            admission_id=binding.admission_id,
            parent_cluster_id=binding.parent_ref,
            source_data_snapshot_id=binding.data_snapshot_id,
            retrieval_resolution_id=resolution.resolution_id,
            frozen_data_snapshot_id=selected_snapshot.snapshot_id,
            candidate_set_id=candidate_set.candidate_set_id,
            candidate_set_hash=canonical_hash(candidate_set.to_dict()),
            triage_execution_plan_id=triage_plan.plan_id,
            triage_execution_plan_hash=canonical_hash(triage_plan.to_dict()),
            callback_agent_profile_ref=binding.callback_agent_profile_ref,
            model_profile_alias=alias,
            model_profile_id=model_profile.profile_id,
            model_profile_hash=model_profile.profile_hash,
            direct_skill_manifest_hashes=callback.profile.skill_manifest_hashes,
        )
        self.store.artifacts.put_json(resolution.to_dict())
        self.store.artifacts.put_json(candidate_set.to_dict())
        self.store.artifacts.put_json(triage_plan.to_dict())
        self.store.artifacts.put_json(plan.to_dict())
        return PreparedAgentWatchWakeJudgment(
            plan=plan,
            dispatch=dispatch,
            callback=callback,
            candidate_set=candidate_set,
            triage_plan=triage_plan,
            model_profile=model_profile,
        )

    async def run(
        self,
        prepared: PreparedAgentWatchWakeJudgment,
        *,
        provider: ModelProvider,
        cancellation: CancellationToken | None = None,
    ) -> AgentWatchWakeJudgmentResult:
        if self.prepare(prepared.dispatch) != prepared:
            raise ValueError("Wake Judgment preparation differs from durable Harness authority")
        if not prepared.plan.judgment_model_calls_authorized:
            raise PermissionError("Wake Judgment Plan does not authorize a model call")
        if (
            provider.provider_id != prepared.model_profile.provider_id
            or provider.model != prepared.model_profile.model
        ):
            raise ValueError("Wake callback Provider differs from the frozen model profile")
        runtime_root = self.runtime_root / prepared.plan.plan_id
        artifacts = ArtifactStore(runtime_root / "artifacts")
        run_journal = RunJournal(runtime_root / "runs.sqlite3")
        usage = UsageLedger(runtime_root / "usage-ledger.jsonl")
        runner = EventImpactTriageRunner(
            plan=prepared.triage_plan,
            candidate_set=prepared.candidate_set,
            registration=self.registration,
            provider=provider,
            content_resolver=SnapshotTriageCandidateContentResolver(self.store),
            skills=self.skills,
            artifact_store=artifacts,
            journal=run_journal,
            usage_ledger=usage,
        )
        result = await runner.run(cancellation=cancellation)
        decision: EventImpactTriageDecision | None = None
        if result.status is RunStatus.COMPLETED:
            if result.proposal is None or result.run_evidence is None:
                raise AssertionError("completed Wake callback lacks Triage authority")
            decision = self.decision_store.admit(
                candidate_set=prepared.candidate_set,
                proposal=result.proposal,
                run_evidence=result.run_evidence,
                run_authority=runner,
                decided_at=runner.authoritative_finished_at(result.run_evidence),
            )
        judgment_result = AgentWatchWakeJudgmentResult(
            plan=prepared.plan,
            triage_result=result,
            decision=decision,
        )
        self._finish_dispatch(prepared, judgment_result, runner=runner)
        return judgment_result

    def _reopen_dispatch(self, dispatch: AgentWatchWakeDispatch) -> WatchCallbackBinding:
        binding = dispatch.binding
        artifact = self.store.artifacts.read_json(dispatch.binding_artifact_hash)
        if artifact != binding.to_dict():
            raise ValueError("Wake dispatch binding differs from its durable artifact")
        run = self.dispatcher.run_journal.get_run(binding.run_id)
        if (
            run.run_id != dispatch.run.run_id
            or run.config_hash != dispatch.binding_artifact_hash
            or run.created_at != dispatch.run.created_at
        ):
            raise ValueError("Wake dispatch Run differs from its durable reservation")
        events = self.dispatcher.run_journal.events(binding.run_id)
        if (
            len(events) not in {1, 2}
            or events[0].event_type != AGENT_WATCH_WAKE_RUN_BOUND_EVENT
            or events[0].payload.get("binding_id") != binding.binding_id
        ):
            raise ValueError("Wake dispatch reservation lacks its exact binding event")
        if len(events) == 2 and (
            events[1].event_type != AGENT_WATCH_WAKE_JUDGMENT_FINISHED_EVENT
            or not run.status.terminal
            or events[1].payload.get("terminal_artifact_hash") != run.terminal_artifact_id
        ):
            raise ValueError("Wake dispatch terminal event differs from its Run authority")
        if dispatch.wake.wake_id != binding.wake_id:
            raise ValueError("Wake dispatch carries a different Wake artifact")
        callbacks = self.admission_service.callback_bindings(dispatch.wake)
        matches = tuple(
            item for item in callbacks if item.admission.admission_id == binding.admission_id
        )
        if len(matches) != 1:
            raise ValueError("Wake dispatch binding is outside the frozen callback set")
        callback = matches[0]
        if (
            callback.profile.profile_id != binding.delegate_profile_id
            or callback.request.request_id != binding.request_id
            or callback.wake.wake_id != binding.wake_id
        ):
            raise ValueError("Wake dispatch callback differs from reopened admission authority")
        return callback

    def _finish_dispatch(
        self,
        prepared: PreparedAgentWatchWakeJudgment,
        result: AgentWatchWakeJudgmentResult,
        *,
        runner: EventImpactTriageRunner,
    ) -> None:
        binding = prepared.dispatch.binding
        members = result.triage_result.members
        if not members:
            raise ValueError("Wake Judgment terminal result lacks a member Run")
        finished_at = max(runner.journal.get_run(item.run_id).updated_at for item in members)
        decision_hash = None
        decision_id = None
        if result.decision is not None:
            decision_hash = canonical_hash(result.decision.to_dict())
            decision_id = result.decision.decision_id
        evidence_hash = (
            None
            if result.triage_result.run_evidence is None
            else canonical_hash(result.triage_result.run_evidence.to_dict())
        )
        terminal_payload: dict[str, object] = {
            "schema_version": AGENT_WATCH_WAKE_JUDGMENT_RESULT_SCHEMA,
            "plan_id": prepared.plan.plan_id,
            "dispatch_binding_id": binding.binding_id,
            "triage_execution_plan_id": prepared.triage_plan.plan_id,
            "status": result.triage_result.status.value,
            "decision_id": decision_id,
            "decision_hash": decision_hash,
            "run_evidence_hash": evidence_hash,
            "member_run_ids": [item.run_id for item in members],
            "member_terminal_artifact_hashes": [item.terminal_artifact_hash for item in members],
            "finished_at": _timestamp(finished_at),
            "research_only": True,
            "execution_capability": False,
        }
        artifact = self.store.artifacts.put_json(terminal_payload)
        current = self.dispatcher.run_journal.get_run(binding.run_id)
        if current.status is RunStatus.RUNNING:
            self.dispatcher.run_journal.append(
                run_id=binding.run_id,
                event_id=f"{binding.run_id}.judgment-finished",
                event_type=AGENT_WATCH_WAKE_JUDGMENT_FINISHED_EVENT,
                observed_at=finished_at,
                payload={
                    "plan_id": prepared.plan.plan_id,
                    "status": result.triage_result.status.value,
                    "terminal_artifact_hash": artifact.content_hash,
                    "research_only": True,
                    "execution_capability": False,
                },
            )
            self.dispatcher.run_journal.finish(
                run_id=binding.run_id,
                status=result.triage_result.status,
                finished_at=finished_at,
                terminal_artifact_id=artifact.content_hash,
            )
            return
        if (
            current.status is not result.triage_result.status
            or current.terminal_artifact_id != artifact.content_hash
        ):
            raise ValueError("Wake Judgment replay differs from its terminal Run authority")

    def _triage_authority(self, parent_ref: str) -> EventImpactTriageWatchAuthority:
        authority = self.admission_service.delegation_authority
        if type(authority) is EventImpactTriageWatchAuthority:
            if authority.cluster_id != parent_ref:
                raise ValueError("Wake Judgment parent differs from Triage authority")
            return authority
        if type(authority) is EventImpactTriageWatchAuthorityResolver:
            return authority.authority(parent_ref)
        raise TypeError("Wake Judgment execution requires concrete Triage parent authority")


def _build_wake_candidate_set(
    *,
    parent: EventImpactTriageCandidateSet,
    callback: WatchCallbackBinding,
    dispatch: AgentWatchWakeDispatch,
    snapshot_id: str,
    selection_id: str,
    ordered_version_ids: tuple[str, ...],
    journal: ProspectiveDataJournal,
    frozen_at: datetime,
) -> EventImpactTriageCandidateSet:
    snapshot = journal.store.get(snapshot_id)
    if (
        not snapshot.coverage_complete
        or snapshot.query.pit_lane is not DataPITLane.PROSPECTIVE
        or snapshot.query.capability is not ObservationCapability.EVENT_REVELATION
        or snapshot.completed_at > frozen_at
        or snapshot.query.source_policy_id != selection_id
        or snapshot.query.parameters.get("selection_id") != selection_id
        or snapshot.query.parameters.get("readiness_report_id") != parent.readiness_report_id
    ):
        raise ValueError("Wake callback requires a complete prospective event Snapshot")
    by_version = {prospective_observation_version_id(item): item for item in snapshot.observations}
    if set(by_version) != set(ordered_version_ids):
        raise ValueError("Wake callback Snapshot must contain exactly the new Observation Versions")
    refs: list[TriageObservationRef] = []
    for version_id in ordered_version_ids:
        observation = by_version[version_id]
        available_at = observation.times.available_at
        if (
            observation.times.availability_basis is not AvailabilityBasis.ACTUAL_RECEIPT
            or available_at is None
            or available_at != observation.times.retrieved_at
            or observation.authority_at != observation.times.retrieved_at
            or observation.authority_kind != "actual_receipt"
        ):
            raise ValueError("Wake callback candidates require actual-receipt authority")
        refs.append(
            TriageObservationRef(
                version_id=version_id,
                observation_id=observation.observation_id,
                first_available_at=available_at,
                authority_at=cast(datetime, observation.authority_at),
                provider_id=observation.provider_id,
                provider_version=observation.provider_version,
                upstream_source=observation.upstream_source,
                source_ref=observation.source_ref,
                raw_content_hash=observation.raw_content_hash,
                normalized_payload_hash=canonical_hash(observation.normalized_payload),
            )
        )
    ordered = tuple(refs)
    core: dict[str, object] = {
        "schema_version": EVENT_IMPACT_TRIAGE_CANDIDATE_SET_SCHEMA_V2,
        "registration_id": parent.registration_id,
        "checkpoint_key": parent.checkpoint_key,
        "route_plan_id": parent.route_plan_id,
        "route_admission_id": parent.route_admission_id,
        "readiness_report_id": parent.readiness_report_id,
        "data_snapshot_id": snapshot.snapshot_id,
        "admitted_at": _timestamp(callback.admission.admitted_at),
        "frozen_at": _timestamp(frozen_at),
        "observations": [item.to_dict() for item in ordered],
        "historical_pit_claim": False,
        "judgment_model_calls_authorized": False,
        "execution_capability": False,
        "origin_wake_id": callback.wake.wake_id,
        "parent_cluster_id": callback.admission.parent_ref,
        "wake_dispatch_binding_id": dispatch.binding.binding_id,
    }
    return EventImpactTriageCandidateSet(
        candidate_set_id=f"event-impact-triage-candidate-set-{canonical_hash(core)}",
        registration_id=parent.registration_id,
        checkpoint_key=parent.checkpoint_key,
        route_plan_id=parent.route_plan_id,
        route_admission_id=parent.route_admission_id,
        readiness_report_id=parent.readiness_report_id,
        data_snapshot_id=snapshot.snapshot_id,
        admitted_at=callback.admission.admitted_at,
        frozen_at=frozen_at,
        observations=ordered,
        schema_version=EVENT_IMPACT_TRIAGE_CANDIDATE_SET_SCHEMA_V2,
        origin_wake_id=callback.wake.wake_id,
        parent_cluster_id=callback.admission.parent_ref,
        wake_dispatch_binding_id=dispatch.binding.binding_id,
    )


def _timestamp(value: datetime) -> str:
    _strict_utc(value, "timestamp")
    return value.isoformat().replace("+00:00", "Z")


def _strict_utc(value: datetime, name: str) -> None:
    if value.tzinfo is not UTC or value.utcoffset() != timedelta(0):
        raise ValueError(f"{name} must use the UTC singleton")


def _sha256(value: str, name: str) -> None:
    if len(value) != 64 or any(char not in "0123456789abcdef" for char in value):
        raise ValueError(f"{name} must be a sha256 hex digest")


def _prefixed_hash(value: str, prefix: str, name: str) -> None:
    if not value.startswith(prefix):
        raise ValueError(f"{name} has an invalid identity")
    _sha256(value.removeprefix(prefix), name)


def _trimmed(value: str, name: str) -> None:
    if not value or value != value.strip():
        raise ValueError(f"{name} must be non-empty trimmed text")
