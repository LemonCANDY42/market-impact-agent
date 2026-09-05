"""Idempotent Attention Wake to fresh research Run creation.

This boundary persists one immutable callback binding and creates a RunJournal
record.  It does not invoke a model or grant Judgment or execution authority.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import cast

from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.agent_watch_admission import (
    AgentWatchAdmissionService,
    WatchCallbackBinding,
)
from market_impact_agent.attention_watch import AttentionWake, AttentionWatchService
from market_impact_agent.domain import require_aware
from market_impact_agent.runtime_store import ArtifactStore, RunJournal, RunRecord, RunStatus

AGENT_WATCH_WAKE_RUN_BINDING_SCHEMA = "market-impact.agent-watch-wake-run-binding.v1"
AGENT_WATCH_WAKE_RUN_BOUND_EVENT = "agent.watch-wake.run-bound"
_CONCURRENT_CREATION_TIMEOUT_SECONDS = 5.0


@dataclass(frozen=True, slots=True)
class AgentWatchWakeRunBinding:
    binding_id: str
    run_id: str
    wake_id: str
    watch_id: str
    admission_id: str
    request_id: str
    delegate_profile_id: str
    callback_agent_type: str
    callback_agent_profile_ref: str
    parent_ref: str
    parent_authority_hash: str
    monitoring_scope_id: str
    retrieval_plan_id: str
    data_snapshot_id: str
    prior_data_snapshot_id: str
    new_version_ids: tuple[str, ...]
    collection_policy_id: str
    use_class: str
    preloaded_skills: tuple[str, ...]
    skill_manifest_hashes: tuple[str, ...]
    required_capabilities: tuple[str, ...]
    max_turns: int
    max_input_tokens: int
    max_output_tokens: int
    max_cost_microusd: int
    research_only: bool = True
    judgment_model_calls_authorized: bool = False
    execution_capability: bool = False
    schema_version: str = AGENT_WATCH_WAKE_RUN_BINDING_SCHEMA

    def __post_init__(self) -> None:
        if self.schema_version != AGENT_WATCH_WAKE_RUN_BINDING_SCHEMA:
            raise ValueError("unsupported Agent Watch Wake Run binding schema")
        if not self.run_id.startswith("agent-watch-wake-run-"):
            raise ValueError("Agent Watch Wake binding requires its derived Run ID")
        if not self.binding_id.startswith("agent-watch-wake-run-binding-"):
            raise ValueError("Agent Watch Wake binding requires a binding ID")
        if not self.research_only:
            raise ValueError("Agent Watch Wake Runs must remain research-only")
        if self.judgment_model_calls_authorized or self.execution_capability:
            raise ValueError("Agent Watch Wake binding cannot grant Judgment or execution")
        if self.binding_id != self.expected_binding_id:
            raise ValueError("Agent Watch Wake binding_id does not match content")

    @property
    def expected_binding_id(self) -> str:
        return f"agent-watch-wake-run-binding-{canonical_hash(self.core_dict())}"

    def core_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "run_id": self.run_id,
            "wake_id": self.wake_id,
            "watch_id": self.watch_id,
            "admission_id": self.admission_id,
            "request_id": self.request_id,
            "delegate_profile_id": self.delegate_profile_id,
            "callback_agent_type": self.callback_agent_type,
            "callback_agent_profile_ref": self.callback_agent_profile_ref,
            "parent_ref": self.parent_ref,
            "parent_authority_hash": self.parent_authority_hash,
            "monitoring_scope_id": self.monitoring_scope_id,
            "retrieval_plan_id": self.retrieval_plan_id,
            "data_snapshot_id": self.data_snapshot_id,
            "prior_data_snapshot_id": self.prior_data_snapshot_id,
            "new_version_ids": list(self.new_version_ids),
            "collection_policy_id": self.collection_policy_id,
            "use_class": self.use_class,
            "preloaded_skills": list(self.preloaded_skills),
            "skill_manifest_hashes": list(self.skill_manifest_hashes),
            "required_capabilities": list(self.required_capabilities),
            "limits": {
                "max_turns": self.max_turns,
                "max_input_tokens": self.max_input_tokens,
                "max_output_tokens": self.max_output_tokens,
                "max_cost_microusd": self.max_cost_microusd,
            },
            "research_only": self.research_only,
            "judgment_model_calls_authorized": self.judgment_model_calls_authorized,
            "execution_capability": self.execution_capability,
        }

    def to_dict(self) -> dict[str, object]:
        return {**self.core_dict(), "binding_id": self.binding_id}


@dataclass(frozen=True, slots=True)
class AgentWatchWakeDispatch:
    binding: AgentWatchWakeRunBinding
    binding_artifact_hash: str
    wake: AttentionWake
    run: RunRecord


class AgentWatchWakeDispatcher:
    """Create fresh callback Runs from the durable Attention Watch outbox."""

    def __init__(
        self,
        admission_service: AgentWatchAdmissionService,
        *,
        run_journal: RunJournal,
        artifact_store: ArtifactStore | None = None,
    ) -> None:
        if type(admission_service) is not AgentWatchAdmissionService:
            raise TypeError("Watch Wake dispatch requires concrete admission authority")
        if type(admission_service.watch_service) is not AttentionWatchService:
            raise TypeError("Watch Wake dispatch requires concrete Attention Watch authority")
        if type(run_journal) is not RunJournal:
            raise TypeError("Watch Wake dispatch requires the concrete Run Journal")
        selected_artifacts = (
            admission_service.store.artifacts if artifact_store is None else artifact_store
        )
        if type(selected_artifacts) is not ArtifactStore:
            raise TypeError("Watch Wake dispatch requires the concrete Artifact Store")
        if selected_artifacts.root != admission_service.store.artifacts.root:
            raise ValueError("Watch Wake dispatch must use the admission state Artifact Store")
        self.admission_service = admission_service
        self.watch_service = admission_service.watch_service
        self.run_journal = run_journal
        self.artifacts = selected_artifacts

    def dispatch_wake(
        self,
        wake: AttentionWake,
        *,
        dispatched_at: datetime,
    ) -> tuple[AgentWatchWakeDispatch, ...]:
        _strict_utc(dispatched_at, "Watch Wake dispatch time")
        if dispatched_at < wake.created_at:
            raise ValueError("Watch Wake cannot dispatch before it was created")
        callbacks = self.admission_service.callback_bindings(wake)
        dispatched = tuple(
            self._dispatch_callback(callback, dispatched_at=dispatched_at) for callback in callbacks
        )
        # Delivery means every accepted admission has both a durable Run and binding event.
        self.watch_service.mark_wake_delivered(wake.wake_id, delivered_at=dispatched_at)
        return dispatched

    def dispatch_pending(
        self,
        *,
        dispatched_at: datetime,
    ) -> tuple[AgentWatchWakeDispatch, ...]:
        return tuple(
            result
            for wake in self.watch_service.pending_wakes()
            for result in self.dispatch_wake(wake, dispatched_at=dispatched_at)
        )

    def reopen_dispatch(self, run_id: str) -> AgentWatchWakeDispatch:
        """Reopen one durable callback reservation after process restart."""

        run = self.run_journal.get_run(run_id)
        binding = agent_watch_wake_run_binding_from_dict(self.artifacts.read_json(run.config_hash))
        if binding.run_id != run_id:
            raise ValueError("Watch Wake Run config belongs to another Run")
        events = self.run_journal.events(run_id)
        if (
            not events
            or events[0].event_type != AGENT_WATCH_WAKE_RUN_BOUND_EVENT
            or events[0].payload != _binding_event_payload(binding, run.config_hash)
        ):
            raise ValueError("Watch Wake Run lacks its exact durable binding event")
        wake = self.watch_service.wake(binding.wake_id)
        if wake.watch_id != binding.watch_id:
            raise ValueError("Watch Wake Run carries a Wake from another Watch")
        return AgentWatchWakeDispatch(
            binding=binding,
            binding_artifact_hash=run.config_hash,
            wake=wake,
            run=run,
        )

    def running_dispatches(self) -> tuple[AgentWatchWakeDispatch, ...]:
        """Return unfinished callbacks owned by this dedicated Run Journal."""

        return tuple(
            self.reopen_dispatch(run.run_id)
            for run in self.run_journal.records(status=RunStatus.RUNNING)
        )

    def _dispatch_callback(
        self,
        callback: WatchCallbackBinding,
        *,
        dispatched_at: datetime,
    ) -> AgentWatchWakeDispatch:
        binding = _run_binding(callback)
        artifact = self.artifacts.put_json(binding.to_dict())
        event_id = f"{binding.run_id}.binding"
        claim = self.run_journal.try_claim_run(binding.run_id)
        if claim is None:
            return self._await_concurrent_dispatch(
                binding,
                wake=callback.wake,
                binding_artifact_hash=artifact.content_hash,
                event_id=event_id,
            )
        with claim:
            run = self.run_journal.start_run(
                run_id=binding.run_id,
                config_hash=artifact.content_hash,
                created_at=dispatched_at,
            )
            if run.status.terminal:
                reopened = self.reopen_dispatch(binding.run_id)
                if reopened.binding != binding:
                    raise ValueError("terminal Watch Wake Run differs from its binding")
                return reopened
            event = self.run_journal.append(
                run_id=binding.run_id,
                event_id=event_id,
                event_type=AGENT_WATCH_WAKE_RUN_BOUND_EVENT,
                observed_at=dispatched_at,
                payload=_binding_event_payload(binding, artifact.content_hash),
            )
            if event.payload_hash != canonical_hash(event.payload):
                raise ValueError("Watch Wake binding event payload is not durable")
        return AgentWatchWakeDispatch(
            binding=binding,
            binding_artifact_hash=artifact.content_hash,
            wake=callback.wake,
            run=run,
        )

    def _await_concurrent_dispatch(
        self,
        binding: AgentWatchWakeRunBinding,
        *,
        wake: AttentionWake,
        binding_artifact_hash: str,
        event_id: str,
    ) -> AgentWatchWakeDispatch:
        deadline = time.monotonic() + _CONCURRENT_CREATION_TIMEOUT_SECONDS
        while time.monotonic() < deadline:
            try:
                run = self.run_journal.get_run(binding.run_id)
            except KeyError:
                time.sleep(0.005)
                continue
            event = self.run_journal.event(event_id)
            if event is None:
                time.sleep(0.005)
                continue
            if (
                run.config_hash != binding_artifact_hash
                or event.event_type != AGENT_WATCH_WAKE_RUN_BOUND_EVENT
                or event.payload != _binding_event_payload(binding, binding_artifact_hash)
            ):
                raise ValueError("concurrent Watch Wake Run differs from its binding")
            return AgentWatchWakeDispatch(
                binding=binding,
                binding_artifact_hash=binding_artifact_hash,
                wake=wake,
                run=run,
            )
        raise RuntimeError("concurrent Watch Wake Run creation did not become durable")


def _run_binding(callback: WatchCallbackBinding) -> AgentWatchWakeRunBinding:
    admission = callback.admission
    profile = callback.profile
    if admission.monitoring_scope_id is None or admission.retrieval_plan_id is None:
        raise AssertionError("accepted Agent Watch admission lacks research bindings")
    identity = {
        "wake_id": callback.wake.wake_id,
        "admission_id": admission.admission_id,
        "request_id": callback.request.request_id,
        "delegate_profile_id": profile.profile_id,
        "callback_agent_profile_ref": profile.callback_agent_profile_ref,
    }
    run_id = f"agent-watch-wake-run-{canonical_hash(identity)}"
    core: dict[str, object] = {
        "schema_version": AGENT_WATCH_WAKE_RUN_BINDING_SCHEMA,
        "run_id": run_id,
        "wake_id": callback.wake.wake_id,
        "watch_id": callback.wake.watch_id,
        "admission_id": admission.admission_id,
        "request_id": callback.request.request_id,
        "delegate_profile_id": profile.profile_id,
        "callback_agent_type": profile.callback_agent_type,
        "callback_agent_profile_ref": profile.callback_agent_profile_ref,
        "parent_ref": admission.parent_ref,
        "parent_authority_hash": admission.parent_authority_hash,
        "monitoring_scope_id": admission.monitoring_scope_id,
        "retrieval_plan_id": admission.retrieval_plan_id,
        "data_snapshot_id": callback.wake.data_snapshot_id,
        "prior_data_snapshot_id": callback.wake.prior_data_snapshot_id,
        "new_version_ids": list(callback.wake.new_version_ids),
        "collection_policy_id": profile.collection_policy_id,
        "use_class": profile.use_class.value,
        "preloaded_skills": list(profile.preloaded_skills),
        "skill_manifest_hashes": list(profile.skill_manifest_hashes),
        "required_capabilities": list(profile.required_capabilities),
        "limits": {
            "max_turns": profile.callback_max_turns,
            "max_input_tokens": profile.callback_max_input_tokens,
            "max_output_tokens": profile.callback_max_output_tokens,
            "max_cost_microusd": profile.callback_max_cost_microusd,
        },
        "research_only": True,
        "judgment_model_calls_authorized": False,
        "execution_capability": False,
    }
    return AgentWatchWakeRunBinding(
        binding_id=f"agent-watch-wake-run-binding-{canonical_hash(core)}",
        run_id=run_id,
        wake_id=callback.wake.wake_id,
        watch_id=callback.wake.watch_id,
        admission_id=admission.admission_id,
        request_id=callback.request.request_id,
        delegate_profile_id=profile.profile_id,
        callback_agent_type=profile.callback_agent_type,
        callback_agent_profile_ref=profile.callback_agent_profile_ref,
        parent_ref=admission.parent_ref,
        parent_authority_hash=admission.parent_authority_hash,
        monitoring_scope_id=admission.monitoring_scope_id,
        retrieval_plan_id=admission.retrieval_plan_id,
        data_snapshot_id=callback.wake.data_snapshot_id,
        prior_data_snapshot_id=callback.wake.prior_data_snapshot_id,
        new_version_ids=callback.wake.new_version_ids,
        collection_policy_id=profile.collection_policy_id,
        use_class=profile.use_class.value,
        preloaded_skills=profile.preloaded_skills,
        skill_manifest_hashes=profile.skill_manifest_hashes,
        required_capabilities=profile.required_capabilities,
        max_turns=profile.callback_max_turns,
        max_input_tokens=profile.callback_max_input_tokens,
        max_output_tokens=profile.callback_max_output_tokens,
        max_cost_microusd=profile.callback_max_cost_microusd,
    )


def _binding_event_payload(
    binding: AgentWatchWakeRunBinding,
    binding_artifact_hash: str,
) -> dict[str, object]:
    return {
        "binding_id": binding.binding_id,
        "binding_artifact_hash": binding_artifact_hash,
        "wake_id": binding.wake_id,
        "admission_id": binding.admission_id,
        "request_id": binding.request_id,
        "delegate_profile_id": binding.delegate_profile_id,
        "data_snapshot_id": binding.data_snapshot_id,
        "research_only": True,
        "judgment_model_calls_authorized": False,
        "execution_capability": False,
    }


def agent_watch_wake_run_binding_from_dict(value: object) -> AgentWatchWakeRunBinding:
    if not isinstance(value, dict):
        raise TypeError("Agent Watch Wake Run binding must be an object")
    payload = cast(dict[object, object], value)
    if any(not isinstance(key, str) for key in payload):
        raise TypeError("Agent Watch Wake Run binding keys must be strings")
    fields = cast(dict[str, object], payload)
    raw_limits = fields.get("limits")
    if not isinstance(raw_limits, dict):
        raise TypeError("Agent Watch Wake Run binding limits must be an object")
    untyped_limits = cast(dict[object, object], raw_limits)
    if any(not isinstance(key, str) for key in untyped_limits):
        raise TypeError("Agent Watch Wake Run binding limit keys must be strings")
    limits = cast(dict[str, object], untyped_limits)

    def string(name: str) -> str:
        item = fields.get(name)
        if not isinstance(item, str):
            raise TypeError(f"Agent Watch Wake Run binding {name} must be a string")
        return item

    def strings(name: str) -> tuple[str, ...]:
        item = fields.get(name)
        if not isinstance(item, list):
            raise TypeError(f"Agent Watch Wake Run binding {name} must be strings")
        members = cast(list[object], item)
        if any(not isinstance(member, str) for member in members):
            raise TypeError(f"Agent Watch Wake Run binding {name} must be strings")
        return tuple(cast(list[str], members))

    def integer(name: str) -> int:
        item = limits.get(name)
        if not isinstance(item, int) or isinstance(item, bool):
            raise TypeError(f"Agent Watch Wake Run binding {name} must be an integer")
        return item

    def boolean(name: str) -> bool:
        item = fields.get(name)
        if not isinstance(item, bool):
            raise TypeError(f"Agent Watch Wake Run binding {name} must be a boolean")
        return item

    binding = AgentWatchWakeRunBinding(
        binding_id=string("binding_id"),
        run_id=string("run_id"),
        wake_id=string("wake_id"),
        watch_id=string("watch_id"),
        admission_id=string("admission_id"),
        request_id=string("request_id"),
        delegate_profile_id=string("delegate_profile_id"),
        callback_agent_type=string("callback_agent_type"),
        callback_agent_profile_ref=string("callback_agent_profile_ref"),
        parent_ref=string("parent_ref"),
        parent_authority_hash=string("parent_authority_hash"),
        monitoring_scope_id=string("monitoring_scope_id"),
        retrieval_plan_id=string("retrieval_plan_id"),
        data_snapshot_id=string("data_snapshot_id"),
        prior_data_snapshot_id=string("prior_data_snapshot_id"),
        new_version_ids=strings("new_version_ids"),
        collection_policy_id=string("collection_policy_id"),
        use_class=string("use_class"),
        preloaded_skills=strings("preloaded_skills"),
        skill_manifest_hashes=strings("skill_manifest_hashes"),
        required_capabilities=strings("required_capabilities"),
        max_turns=integer("max_turns"),
        max_input_tokens=integer("max_input_tokens"),
        max_output_tokens=integer("max_output_tokens"),
        max_cost_microusd=integer("max_cost_microusd"),
        research_only=boolean("research_only"),
        judgment_model_calls_authorized=boolean("judgment_model_calls_authorized"),
        execution_capability=boolean("execution_capability"),
        schema_version=string("schema_version"),
    )
    if binding.to_dict() != fields:
        raise ValueError("Agent Watch Wake Run binding is not canonical")
    return binding


def _strict_utc(value: datetime, name: str) -> None:
    require_aware(value, name)
    if value.tzinfo is not UTC or value.utcoffset() != timedelta(0):
        raise ValueError(f"{name} must use the UTC singleton")
