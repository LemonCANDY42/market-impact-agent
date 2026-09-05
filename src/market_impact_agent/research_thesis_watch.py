"""Signed Research Thesis delegation into the existing Attention Watch runtime.

Native tool completions are the proposal queue. Admissions, Watch collection,
Wake dispatch and callback Runs retain their existing owners and durable state.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, cast

from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.agent_runtime import ToolDescriptor, ToolSideEffect
from market_impact_agent.agent_watch_admission import (
    AgentDelegationContext,
    AgentWatchAdmission,
    AgentWatchAdmissionService,
    AgentWatchRequest,
    WatchDelegateProfile,
    agent_watch_request_from_dict,
    watch_delegate_profile_from_dict,
)
from market_impact_agent.agent_watch_wake_dispatch import AgentWatchWakeDispatcher
from market_impact_agent.data_inputs import DataPITLane, LocalDataSnapshotStore
from market_impact_agent.decision_thesis import ResearchThesisV1
from market_impact_agent.model_budget import ModelBudget
from market_impact_agent.monitoring_scope import (
    MonitoringMatchMode,
    MonitoringSubjectKind,
    MonitoringSubjectRef,
    ObservationMatchClause,
    ObservationMatcher,
    matched_scope_versions,
)
from market_impact_agent.on_demand_research import OnDemandResearch
from market_impact_agent.runtime_store import RunJournal, RunStatus

if TYPE_CHECKING:
    from market_impact_agent.research_thesis_runtime import ResearchThesisRunInputs

RESEARCH_WATCH_PARENT_TYPE = "research-thesis.coordinator"
RESEARCH_WATCH_TOOL = "request_research_watch"


@dataclass(frozen=True, slots=True)
class ResearchThesisWatchDelegation:
    """Harness-only offer frozen in the signed source Run; never model-authored."""

    episode_id: str
    episode_binding_event_id: str
    episode_deadline: datetime
    parent_run_id: str
    parent_budget_hash: str
    budget_scope: str | None
    subject: MonitoringSubjectRef
    matcher_terms: tuple[str, ...]
    profiles: tuple[WatchDelegateProfile, ...]

    def __post_init__(self) -> None:
        if self.subject.kind is not MonitoringSubjectKind.EVENT_CLUSTER:
            raise ValueError("research Watch initially supports an explicit root-event scope")
        if self.episode_deadline.tzinfo is not UTC:
            raise ValueError("research Watch deadline must use UTC")
        if not self.profiles or len({p.profile_id for p in self.profiles}) != len(self.profiles):
            raise ValueError("research Watch requires unique Harness-offered profiles")
        if not self.matcher_terms or self.matcher_terms != tuple(sorted(set(self.matcher_terms))):
            raise ValueError("research Watch matcher terms must be nonempty and canonical")
        if any(not term or term != term.strip().casefold() for term in self.matcher_terms):
            raise ValueError("research Watch matcher terms must be normalized")
        for profile in self.profiles:
            if (
                RESEARCH_WATCH_PARENT_TYPE not in profile.allowed_parent_agent_types
                or self.subject.kind not in profile.allowed_subject_kinds
                or profile.query_template.pit_lane is not DataPITLane.PROSPECTIVE
            ):
                raise ValueError("research Watch profile does not allow this prospective parent")

    @classmethod
    def bind(
        cls,
        acquisition: OnDemandResearch,
        *,
        subject: MonitoringSubjectRef,
        matcher_terms: tuple[str, ...],
        profiles: tuple[WatchDelegateProfile, ...],
    ) -> ResearchThesisWatchDelegation:
        if (
            type(acquisition) is not OnDemandResearch
            or acquisition.pit_lane is not DataPITLane.PROSPECTIVE
        ):
            raise PermissionError("research Watch requires a concrete prospective Episode")
        suffix = (
            "research.episode-binding"
            if "episode_id" not in acquisition.binding
            else "research.episode-binding." + canonical_hash(acquisition.episode_id)
        )
        result = cls(
            acquisition.episode_id,
            f"{acquisition.budget.owner_run_id}.{suffix}",
            acquisition.deadline,
            acquisition.budget.owner_run_id,
            canonical_hash(acquisition.budget.binding),
            acquisition.budget.scope,
            subject,
            tuple(sorted(set(matcher_terms))),
            tuple(sorted(profiles, key=lambda p: p.profile_id)),
        )
        result.verify_episode(acquisition.store, acquisition.budget)
        return result

    def verify_episode(self, store: LocalDataSnapshotStore, budget: ModelBudget) -> None:
        if (
            budget.journal.path != store.index_path
            or budget.journal.harness_authority_id != store.harness_authority_id
            or budget.owner_run_id != self.parent_run_id
            or canonical_hash(budget.binding) != self.parent_budget_hash
            or budget.scope != self.budget_scope
        ):
            raise PermissionError("research Watch must retain its original parent budget and scope")
        expected_ids = {
            f"{self.parent_run_id}.research.episode-binding.{canonical_hash(self.episode_id)}"
        }
        if self.episode_id == self.parent_run_id:
            expected_ids.add(f"{self.parent_run_id}.research.episode-binding")
        event = budget.journal.event(self.episode_binding_event_id)
        if (
            self.episode_binding_event_id not in expected_ids
            or event is None
            or event.run_id != self.parent_run_id
            or event.event_type != "research.episode.binding"
            or event.payload
            != {
                "parent_budget": budget.binding,
                "episode_deadline": self.episode_deadline.isoformat(),
            }
        ):
            raise PermissionError("research Watch has no exact durable parent Episode binding")

    def to_dict(self) -> dict[str, object]:
        return {
            "episode_id": self.episode_id,
            "episode_binding_event_id": self.episode_binding_event_id,
            "episode_deadline": self.episode_deadline.isoformat(),
            "parent_run_id": self.parent_run_id,
            "parent_budget_hash": self.parent_budget_hash,
            "budget_scope": self.budget_scope,
            "subject": self.subject.to_dict(),
            "matcher_terms": list(self.matcher_terms),
            "profiles": [p.to_dict() for p in self.profiles],
        }

    @classmethod
    def from_dict(cls, value: object) -> ResearchThesisWatchDelegation:
        fields = _object(value)
        subject = _object(fields["subject"])
        scope = fields["budget_scope"]
        if scope is not None and not isinstance(scope, str):
            raise TypeError("Watch budget scope must be text or absent")
        result = cls(
            _text(fields, "episode_id"),
            _text(fields, "episode_binding_event_id"),
            datetime.fromisoformat(_text(fields, "episode_deadline")).astimezone(UTC),
            _text(fields, "parent_run_id"),
            _text(fields, "parent_budget_hash"),
            scope,
            MonitoringSubjectRef(
                MonitoringSubjectKind(_text(subject, "kind")), _text(subject, "canonical_id")
            ),
            _strings(fields["matcher_terms"]),
            tuple(watch_delegate_profile_from_dict(p) for p in _list(fields["profiles"])),
        )
        if result.to_dict() != fields:
            raise ValueError("research Watch delegation is not canonical")
        return result


class ResearchThesisWatchAuthorityResolver:
    """Reopen signed completed source Runs in one exact account/arm/target/Episode."""

    def __init__(
        self,
        store: LocalDataSnapshotStore,
        *,
        experiment_id: str,
        arm_id: str,
        account_scope: str,
        target_id: str,
        parent_budget: ModelBudget,
        episode_id: str,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        if type(store) is not LocalDataSnapshotStore or not account_scope:
            raise ValueError("research Watch requires concrete state and account authority")
        self.store = store
        self.experiment_id = experiment_id
        self.arm_id = arm_id
        self.account_scope = account_scope
        self.target_id = target_id
        self.parent_budget = parent_budget
        self.episode_id = episode_id
        self.clock = clock
        self.journal = RunJournal.authoritative(store)

    def parent(
        self, run_id: str
    ) -> tuple[ResearchThesisV1, ResearchThesisWatchDelegation, dict[str, object]]:
        from market_impact_agent.research_thesis_runtime import reopen_completed_research_thesis

        thesis, _ = reopen_completed_research_thesis(
            journal=self.journal, artifact_store=self.store.artifacts, run_id=run_id
        )
        record = self.journal.get_run(run_id)
        binding = _object(self.store.artifacts.read_json(record.config_hash))
        inputs = _object(binding["inputs"])
        delegation = ResearchThesisWatchDelegation.from_dict(inputs.get("watch_delegation"))
        if (
            binding.get("harness_authority_id") != self.store.harness_authority_id
            or binding.get("run_id") != run_id
            or binding.get("experiment_id") != self.experiment_id
            or binding.get("arm_id") != self.arm_id
            or binding.get("account_scope") != self.account_scope
            or inputs.get("target_id") != self.target_id
            or inputs.get("as_of") != thesis.as_of.isoformat().replace("+00:00", "Z")
            or delegation.subject.canonical_id != thesis.root_event_id
            or delegation.episode_id != self.episode_id
            or thesis.as_of > self.clock()
            or record.updated_at > self.clock()
            or binding.get("budget_owner")
            != {
                "journal_path": str(self.parent_budget.journal.path),
                "run_id": self.parent_budget.owner_run_id,
                "binding": self.parent_budget.binding,
            }
        ):
            raise PermissionError(
                "research Watch parent differs from its exact signed scope or cutoff"
            )
        delegation.verify_episode(self.store, self.parent_budget)
        return thesis, delegation, inputs

    def delegation_context(self, run_id: str) -> AgentDelegationContext:
        _, delegation, _ = self.parent(run_id)
        binding = _object(self.store.artifacts.read_json(self.journal.get_run(run_id).config_hash))
        selected = _object(
            self.store.artifacts.read_json(_text(binding, "selected_inputs_artifact_hash"))
        )
        return AgentDelegationContext(
            parent_ref=run_id,
            parent_agent_type=RESEARCH_WATCH_PARENT_TYPE,
            lineage_depth=0,
            created_at=self.journal.get_run(run_id).updated_at,
            authorized_evidence_refs=tuple(
                sorted(
                    {
                        _text(_object(_object(item)["reference"]), "evidence_id")
                        for item in _list(selected["evidence"])
                    }
                )
            ),
            authorized_subjects=(delegation.subject,),
            authorized_matcher_terms=delegation.matcher_terms,
        )

    def reopen(self, context: AgentDelegationContext) -> AgentDelegationContext:
        expected = self.delegation_context(context.parent_ref)
        if context != expected:
            raise PermissionError("research Watch parent projection differs from signed authority")
        return expected

    def offered_profile_ids(self, run_id: str) -> frozenset[str]:
        return frozenset(p.profile_id for p in self.parent(run_id)[1].profiles)

    def operational_scope_identity(self) -> dict[str, str]:
        """Collection receipts can be shared, but callback ownership stays exact."""
        return {
            "experiment_id": self.experiment_id,
            "arm_id": self.arm_id,
            "account_scope": self.account_scope,
            "target_id": self.target_id,
            "episode_id": self.episode_id,
        }

    def owns_parent(self, run_id: str) -> bool:
        """Filter shared-root recovery; matching parents are then fully reopened."""
        binding = _object(self.store.artifacts.read_json(self.journal.get_run(run_id).config_hash))
        inputs = _object(binding["inputs"])
        delegation = _object(inputs["watch_delegation"])
        return (
            binding.get("experiment_id") == self.experiment_id
            and binding.get("arm_id") == self.arm_id
            and binding.get("account_scope") == self.account_scope
            and inputs.get("target_id") == self.target_id
            and delegation.get("episode_id") == self.episode_id
        )


def research_thesis_watch_tool(inputs: ResearchThesisRunInputs, run_id: str) -> ToolDescriptor:
    """Pure proposal tool; its existing signed pi completion is the durable queue."""
    delegation = inputs.watch_delegation
    if delegation is None:
        raise PermissionError("research Run has no Harness Watch offer")
    evidence = frozenset(ref.evidence_id for ref in inputs.repository.evidence_pack.evidence)
    version = canonical_hash({"inputs": inputs.identity_dict(), "run_id": run_id})
    return _proposal_tool(delegation, evidence=evidence, version=version)


def _proposal_result(
    delegation: ResearchThesisWatchDelegation,
    *,
    evidence: frozenset[str],
    arguments: dict[str, object],
) -> dict[str, object]:
    if set(arguments) != {
        "delegate_profile_id",
        "rationale",
        "watch_question",
        "evidence_refs",
        "matcher",
    }:
        raise ValueError("research Watch proposal contains unsupported or missing fields")
    matcher = _object(arguments["matcher"])
    if set(matcher) != {"clauses"}:
        raise ValueError("Watch matcher only accepts clauses")
    clauses: list[ObservationMatchClause] = []
    for raw in _list(matcher["clauses"]):
        clause = _object(raw)
        if set(clause) != {"field_path", "mode", "terms"}:
            raise ValueError("Watch clause contains unsupported fields")
        clauses.append(
            ObservationMatchClause.build(
                field_path=_text(clause, "field_path"),
                mode=MonitoringMatchMode(_text(clause, "mode")),
                terms=_strings(clause["terms"]),
            )
        )
    request = AgentWatchRequest.build(
        delegate_profile_id=_text(arguments, "delegate_profile_id"),
        rationale=_text(arguments, "rationale"),
        watch_question=_text(arguments, "watch_question"),
        evidence_refs=_strings(arguments["evidence_refs"]),
        subject=delegation.subject,
        matcher=ObservationMatcher(tuple(clauses)),
    )
    if (
        request.delegate_profile_id not in {p.profile_id for p in delegation.profiles}
        or not set(request.evidence_refs) <= evidence
        or not {term for clause in clauses for term in clause.terms}
        <= set(delegation.matcher_terms)
    ):
        raise PermissionError("Watch proposal exceeds its Harness offer")
    return {
        "status": "proposal_recorded",
        "request": request.to_dict(),
        "execution_capability": False,
    }


def _proposal_tool(
    delegation: ResearchThesisWatchDelegation, *, evidence: frozenset[str], version: str
) -> ToolDescriptor:
    async def propose(arguments: dict[str, object]) -> object:
        return _proposal_result(delegation, evidence=evidence, arguments=arguments)

    return ToolDescriptor(
        name=RESEARCH_WATCH_TOOL,
        description=(
            "Propose a bounded Watch for missing future evidence. "
            "Admission follows completed research. "
            "The Harness fixes subject, sources, callback and budgets. Offered profiles: "
            + str(
                [
                    {
                        "profile_id": p.profile_id,
                        "name": p.name,
                        "query_template": p.to_dict()["query_template"],
                    }
                    for p in delegation.profiles
                ]
            )
            + "; authorized matcher terms: "
            + str(list(delegation.matcher_terms))
        ),
        input_schema={
            "type": "object",
            "additionalProperties": False,
            "required": [
                "delegate_profile_id",
                "rationale",
                "watch_question",
                "evidence_refs",
                "matcher",
            ],
            "properties": {
                "delegate_profile_id": {
                    "type": "string",
                    "enum": [p.profile_id for p in delegation.profiles],
                },
                "rationale": {"type": "string", "maxLength": 2000},
                "watch_question": {"type": "string", "maxLength": 1000},
                "evidence_refs": {
                    "type": "array",
                    "minItems": 1,
                    "items": {"type": "string", "enum": sorted(evidence)},
                },
                "matcher": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": ["clauses"],
                    "properties": {
                        "clauses": {
                            "type": "array",
                            "minItems": 1,
                            "maxItems": 4,
                            "items": {
                                "type": "object",
                                "additionalProperties": False,
                                "required": ["field_path", "mode", "terms"],
                                "properties": {
                                    "field_path": {"type": "string"},
                                    "mode": {
                                        "type": "string",
                                        "enum": [m.value for m in MonitoringMatchMode],
                                    },
                                    "terms": {
                                        "type": "array",
                                        "minItems": 1,
                                        "items": {
                                            "type": "string",
                                            "enum": list(delegation.matcher_terms),
                                        },
                                    },
                                },
                            },
                        },
                    },
                },
            },
        },
        handler=propose,
        version=version,
        required_capabilities=frozenset({"research_watch.propose"}),
        side_effect=ToolSideEffect.READ_ONLY,
        timeout_seconds=5,
        max_result_bytes=16_000,
    )


def admit_research_thesis_watch_proposals(
    *, resolver: ResearchThesisWatchAuthorityResolver, run_id: str, admitted_at: datetime
) -> tuple[AgentWatchAdmission, ...]:
    """Admit only native, signed proposals from the completed source Run."""
    thesis, delegation, inputs = resolver.parent(run_id)
    if admitted_at > resolver.clock():
        raise PermissionError("research Watch admission exceeds the current clock")
    binding = _object(
        resolver.store.artifacts.read_json(resolver.journal.get_run(run_id).config_hash)
    )
    selected = _object(
        resolver.store.artifacts.read_json(_text(binding, "selected_inputs_artifact_hash"))
    )
    evidence = frozenset(
        _text(_object(_object(item)["reference"]), "evidence_id")
        for item in _list(selected["evidence"])
    )
    tool = _proposal_tool(
        delegation, evidence=evidence, version=canonical_hash({"inputs": inputs, "run_id": run_id})
    )
    from market_impact_agent.dynamic_effectiveness import DatePresentation
    from market_impact_agent.research_thesis_runtime import (
        _relative_temporal_view,  # pyright: ignore[reportPrivateUsage]
        _relative_tool,  # pyright: ignore[reportPrivateUsage]
    )

    relative = inputs.get("date_presentation") == DatePresentation.RELATIVE_OFFSET.value
    if relative:
        tool = _relative_tool(tool, thesis.as_of.date())
    if tool.manifest_hash not in _strings(binding["readonly_tool_hashes"]):
        raise PermissionError("research Watch native tool was not frozen in its source Run")
    service = AgentWatchAdmissionService(
        resolver.store, profiles=delegation.profiles, delegation_authority=resolver
    )
    context = resolver.delegation_context(run_id)
    admissions: list[AgentWatchAdmission] = []
    for event in resolver.journal.events(run_id):
        if event.event_type != "pi.role.tool.completed":
            continue
        tool_binding = _object(event.payload["binding"])
        if tool_binding.get("name") != RESEARCH_WATCH_TOOL:
            continue
        if tool_binding.get("manifest_hash") != tool.manifest_hash:
            raise PermissionError("research Watch native completion belongs to a different offer")
        saved = _object(resolver.store.artifacts.read_json(_text(event.payload, "artifact_hash")))
        result = _object(resolver.store.artifacts.read_json(_text(saved, "result_artifact_hash")))
        original_result = _proposal_result(
            delegation, evidence=evidence, arguments=_object(tool_binding["arguments"])
        )
        expected_result = (
            _relative_temporal_view(original_result, thesis.as_of.date())
            if relative
            else original_result
        )
        if result != expected_result:
            raise PermissionError("Watch completion differs from its signed native proposal")
        request = agent_watch_request_from_dict(original_result["request"])
        profile = next(
            p for p in delegation.profiles if p.profile_id == request.delegate_profile_id
        )
        policy = service.journal.policy(profile.collection_policy_id)
        baseline = service.journal.freeze_snapshot(
            policy_id=policy.policy_id,
            not_after=admitted_at,
            window_start=max(
                policy.window_start,
                admitted_at
                - timedelta(
                    seconds=max(policy.maximum_gap_seconds, policy.poll_interval_seconds * 2)
                ),
            ),
            minimum_data_sources=profile.minimum_coverage_sources,
            frozen_at=admitted_at,
        )
        service.journal.assert_watch_baseline_snapshot(baseline)
        admissions.append(
            service.admit(
                request,
                context=context,
                initial_data_snapshot_id=baseline.snapshot_id,
                decided_at=admitted_at,
            )
        )
    return tuple(admissions)


@dataclass(frozen=True, slots=True)
class ResearchThesisWatchReviewContext:
    callback_run_id: str
    episode_id: str
    account_scope: str
    arm_id: str
    target_id: str
    parent_run_id: str
    research_question: str
    watch_question: str
    rationale: str
    thesis: ResearchThesisV1
    new_version_ids: tuple[str, ...]
    data_snapshot_id: str
    cutoff: datetime
    parent_budget: ModelBudget
    episode_deadline: datetime
    delegate_profile: WatchDelegateProfile


async def run_research_thesis_watch_callback(
    *,
    dispatcher: AgentWatchWakeDispatcher,
    resolver: ResearchThesisWatchAuthorityResolver,
    run_id: str,
    review: Callable[[ResearchThesisWatchReviewContext], Awaitable[dict[str, object]]],
) -> dict[str, object]:
    """Use the existing callback Run claim; an unknown execution never regenerates."""
    if dispatcher.admission_service.delegation_authority is not resolver:
        raise PermissionError("research Watch callback requires the same concrete parent resolver")
    claim = dispatcher.run_journal.try_claim_run(run_id)
    if claim is None:
        return {"status": "in_progress", "callback_run_id": run_id}
    with claim:
        dispatch = dispatcher.reopen_dispatch(run_id)
        binding = dispatch.binding
        callbacks = dispatcher.admission_service.callback_bindings(dispatch.wake)
        callback = next(c for c in callbacks if c.admission.admission_id == binding.admission_id)
        # Re-derive the complete binding, including budget, scope and exact Wake.
        from market_impact_agent.agent_watch_wake_dispatch import (
            _run_binding,  # pyright: ignore[reportPrivateUsage]
        )

        if _run_binding(callback) != binding:
            raise PermissionError("research Watch dispatch differs from durable callback authority")
        thesis, delegation, inputs = resolver.parent(binding.parent_ref)
        completed = dispatcher.run_journal.event(f"{run_id}.research-review.completed")
        if completed is not None:
            result_hash = _text(completed.payload, "result_hash")
            result = _object(dispatcher.artifacts.read_json(result_hash))
            dispatcher.run_journal.finish(
                run_id=run_id,
                status=RunStatus.COMPLETED,
                finished_at=completed.observed_at,
                terminal_artifact_id=result_hash,
            )
            return result
        if dispatcher.run_journal.event(f"{run_id}.research-review.started") is not None:
            return {"status": "reconciliation_required", "callback_run_id": run_id}
        now = resolver.clock()
        if now > delegation.episode_deadline or dispatch.wake.created_at > now:
            raise PermissionError(
                "research Watch callback exceeds its Episode deadline or current cutoff"
            )
        policy = dispatcher.watch_service.policy(binding.watch_id)
        scope = policy.monitoring_scope
        if scope is None or scope.scope_id != binding.monitoring_scope_id:
            raise PermissionError("research Watch callback has no exact monitoring scope")
        snapshot = resolver.store.get(binding.data_snapshot_id)
        prior = resolver.store.get(binding.prior_data_snapshot_id)
        expected_delta = set(matched_scope_versions(scope, snapshot)) - set(
            matched_scope_versions(scope, prior)
        )
        refs = dispatcher.admission_service.journal.observation_version_refs_by_ids(
            binding.new_version_ids
        )
        if (
            not refs
            or not set(binding.new_version_ids) <= expected_delta
            or snapshot.query.source_policy_id != binding.collection_policy_id
            or snapshot.query.pit_lane is not DataPITLane.PROSPECTIVE
            or snapshot.query.as_of > dispatch.wake.created_at
            or any(
                not thesis.as_of < ref.first_available_at <= dispatch.wake.created_at
                for ref in refs
            )
        ):
            raise PermissionError(
                "research Watch callback requires an exact genuinely newer receipt delta"
            )
        context = ResearchThesisWatchReviewContext(
            run_id,
            delegation.episode_id,
            resolver.account_scope,
            resolver.arm_id,
            resolver.target_id,
            binding.parent_ref,
            _text(inputs, "research_question"),
            callback.request.watch_question,
            callback.request.rationale,
            thesis,
            binding.new_version_ids,
            binding.data_snapshot_id,
            dispatch.wake.created_at,
            resolver.parent_budget,
            delegation.episode_deadline,
            callback.profile,
        )
        resolver.parent_budget.check_cancel()
        dispatcher.run_journal.append(
            run_id=run_id,
            event_id=f"{run_id}.research-review.started",
            event_type="research.watch.review.started",
            observed_at=now,
            payload={"binding_id": binding.binding_id, "episode_id": delegation.episode_id},
        )
        result = await review(context)
        result_hash = dispatcher.artifacts.put_json(result).content_hash
        finished_at = resolver.clock()
        dispatcher.run_journal.append(
            run_id=run_id,
            event_id=f"{run_id}.research-review.completed",
            event_type="research.watch.review.completed",
            observed_at=finished_at,
            payload={"result_hash": result_hash},
        )
        dispatcher.run_journal.finish(
            run_id=run_id,
            status=RunStatus.COMPLETED,
            finished_at=finished_at,
            terminal_artifact_id=result_hash,
        )
        return result


def _object(value: object) -> dict[str, object]:
    if not isinstance(value, dict):
        raise TypeError("research Watch requires an object")
    return cast(dict[str, object], value)


def _list(value: object) -> list[object]:
    if not isinstance(value, list):
        raise TypeError("research Watch requires an array")
    return cast(list[object], value)


def _text(value: dict[str, object], name: str) -> str:
    item = value.get(name)
    if not isinstance(item, str) or not item:
        raise TypeError(f"research Watch {name} requires text")
    return item


def _strings(value: object) -> tuple[str, ...]:
    items = _list(value)
    if any(not isinstance(item, str) for item in items):
        raise TypeError("research Watch requires strings")
    return tuple(cast(list[str], items))
