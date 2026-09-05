"""Compose signed pi research Runs with outside-Run acquisition continuations.

The existing thesis terminal and parent acquisition Journal remain authoritative.
A requested acquisition ends an incomplete Run; it is never a portfolio hold.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field, replace
from typing import cast

from market_impact_agent.agent_contracts import (
    EvidencePack,
    EvidenceReference,
    canonical_hash,
    pattern_pack_from_dict,
)
from market_impact_agent.agent_runtime import ToolDescriptor
from market_impact_agent.data_inputs import FrozenDataSnapshotInput
from market_impact_agent.frozen_research import FrozenResearchRepository
from market_impact_agent.model_provider import ModelProvider
from market_impact_agent.on_demand_research import OnDemandResearch, ResearchContinuation
from market_impact_agent.research import EvidenceTier
from market_impact_agent.research_thesis_runtime import (
    ResearchThesisAuthority,
    ResearchThesisRunInputs,
)
from market_impact_agent.runtime_store import RunStatus


class ResearchAcquisitionRequired(RuntimeError):
    """A durable semantic request requires a new frozen Run after acquisition."""


@dataclass(frozen=True, slots=True)
class AcquisitionResearchResult:
    status: str
    run_ids: tuple[str, ...]
    terminal: dict[str, object]
    acquisitions: tuple[ResearchContinuation, ...]
    final_inputs: ResearchThesisRunInputs = field(compare=False, repr=False)
    frozen_input: FrozenDataSnapshotInput | None


@dataclass(frozen=True, slots=True)
class PreparedResearchSuccessor:
    inputs: ResearchThesisRunInputs
    frozen_input: FrozenDataSnapshotInput
    acquisitions: tuple[ResearchContinuation, ...]
    provenance: dict[str, object]
    stop_reason: str | None = None


def _yielding_tool(tool: ToolDescriptor) -> ToolDescriptor:
    async def read(arguments: dict[str, object]) -> object:
        result = await tool.handler(arguments)
        if (
            isinstance(result, dict)
            and cast(dict[str, object], result).get("status") == "continuation_required"
        ):
            # OnDemandResearch committed the exact request before returning this status.
            raise ResearchAcquisitionRequired("outside-Run acquisition required")
        return cast(object, result)

    return replace(tool, handler=read, version="yield-v1-" + tool.manifest_hash)


async def freeze_acquired_research(
    inputs: ResearchThesisRunInputs,
    acquisition: OnDemandResearch,
    results: tuple[ResearchContinuation, ...],
) -> tuple[ResearchThesisRunInputs, FrozenDataSnapshotInput]:
    """Project verified parent completions into a new content-identified Evidence Pack."""
    cutoff, frozen = acquisition.successor_input(results)
    previous = inputs.repository.evidence_pack
    documents = {
        reference.evidence_id: cast(
            dict[str, object],
            await inputs.repository.read_evidence({"evidence_id": reference.evidence_id}),
        )["document"]
        for reference in previous.evidence
    }
    references = {reference.evidence_id: reference for reference in previous.evidence}
    for snapshot_id in sorted(frozen.authorized_snapshot_ids):
        snapshot = acquisition.store.get(snapshot_id)
        if acquisition.historical_inputs is None and (
            snapshot.query.as_of > cutoff or snapshot.query.pit_lane != acquisition.pit_lane
        ):
            raise PermissionError("acquired source exceeds successor cutoff or PIT lane")
        if snapshot_id in references:
            continue
        document = acquisition.snapshot_projection(snapshot)
        documents[snapshot_id] = document
        references[snapshot_id] = EvidenceReference(
            evidence_id=snapshot_id,
            claim_id="acquired-source-snapshot",
            source_ref=f"data-snapshot://{snapshot_id}",
            source_tier=EvidenceTier.UNVERIFIED,
            available_at=cutoff if acquisition.historical_inputs else snapshot.query.as_of,
            content_hash=canonical_hash(document),
            summary="Harness-frozen acquired source and coverage; source quality is not upgraded.",
        )
    patterns = {
        reference.pack_id: pattern_pack_from_dict(
            cast(
                dict[str, object],
                await inputs.repository.read_pattern_pack({"pack_id": reference.pack_id}),
            )
        )
        for reference in previous.pattern_packs
    }
    repository = FrozenResearchRepository(
        evidence_pack=EvidencePack.build(
            event_id=previous.event_id,
            as_of=cutoff,
            research_question=previous.research_question,
            evidence=tuple(references.values()),
            pattern_packs=previous.pattern_packs,
            allowed_targets=previous.allowed_targets,
            data_gaps=previous.data_gaps,
        ),
        evidence_documents=documents,
        pattern_packs=patterns,
    )
    return replace(inputs, repository=repository), frozen


def _undispatched_cancellation(
    authority: ResearchThesisAuthority,
    acquisition: OnDemandResearch,
    terminal: dict[str, object],
) -> dict[str, object] | None:
    """Prove a signed cancellation ended before any admitted model dispatch."""
    run_id = acquisition.run_id
    if (
        terminal.get("reason") != "CancelledError"
        or authority.journal.get_run(run_id).status is not RunStatus.CANCELLED
    ):
        return None
    events = authority.journal.events(run_id)
    if any(
        event.event_type
        not in {
            "research.thesis.frozen",
            "pi.role.history.initial",
            "pi.context.frozen",
            "research.thesis.incomplete",
        }
        for event in events
    ):
        return None
    # The parent owns physical admission. Check every invocation/turn/attempt,
    # including a reservation committed before the Run's attempt observer ran.
    prefix = f"{run_id}.pi-invocation."
    if any(
        isinstance(key := event.payload.get("request_key"), str) and key.startswith(prefix)
        for event in acquisition.budget.journal.events(acquisition.budget.owner_run_id)
    ):
        return None
    return {"run_journal_hash": authority.journal.journal_hash(run_id)}


async def analyze_with_acquisition(
    *,
    authority: ResearchThesisAuthority,
    provider: ModelProvider,
    inputs: ResearchThesisRunInputs,
    acquisition: OnDemandResearch,
    maximum_runs: int = 4,
    prior_thesis_run_id: str | None = None,
    prior_adoption_ref: str | None = None,
    successor_transform: Callable[
        [ResearchThesisRunInputs, OnDemandResearch, tuple[ResearchContinuation, ...]],
        Awaitable[PreparedResearchSuccessor],
    ]
    | None = None,
    successor_transform_id: str | None = None,
    tool_factory: Callable[
        [ResearchThesisRunInputs, str], tuple[ToolDescriptor, ...]
    ] = lambda _inputs, _run_id: (),
) -> AcquisitionResearchResult:
    """Run bounded Harness continuations; pi remains the only model/tool loop.

    Recalling this function with the same frozen entrance replays completed Runs
    and acquired receipts. Unknown generation or acquisition is never retried.
    """
    if (successor_transform is None) != (successor_transform_id is None):
        raise ValueError("successor transform requires an immutable Harness policy identity")
    if maximum_runs < 1 or inputs.candidate_theses:
        raise ValueError("acquisition composition requires a positive Run bound and analyst inputs")
    if (
        provider.budget is None
        or provider.budget.owner_run_id != acquisition.budget.owner_run_id
        or provider.budget.journal.path != acquisition.budget.journal.path
        or provider.budget.binding != acquisition.budget.binding
        or provider.budget.scope != acquisition.budget.scope
        or authority.store.harness_authority_id != acquisition.store.harness_authority_id
        or inputs.repository.evidence_pack.as_of != acquisition.cutoff
        or authority.account_scope is None
    ):
        raise PermissionError(
            "research acquisition requires the same parent budget, account authority and cutoff"
        )
    base_run_id = acquisition.run_id
    acquisition._append(  # pyright: ignore[reportPrivateUsage]
        f"research.composition.{canonical_hash(base_run_id)}",
        "research.composition.bound",
        {
            "initial_run_id": base_run_id,
            "initial_inputs": inputs.identity_dict(),
            "account_scope": authority.account_scope,
            "arm_id": authority.arm_id,
            "experiment_id": authority.experiment_id,
            "maximum_runs": maximum_runs,
            "acquisition_binding": acquisition.binding,
            "prior_thesis_run_id": prior_thesis_run_id,
            **({"prior_adoption_ref": prior_adoption_ref} if prior_adoption_ref else {}),
            **(
                {"successor_transform_id": successor_transform_id} if successor_transform_id else {}
            ),
        },
    )
    run_ids: list[str] = []
    receipts: list[ResearchContinuation] = []
    current = acquisition
    frozen = (
        FrozenDataSnapshotInput(frozenset(item.snapshot_id for item in current.snapshots))
        if current.snapshots
        else None
    )

    def outcome(status: str, terminal: dict[str, object]) -> AcquisitionResearchResult:
        return AcquisitionResearchResult(
            status, tuple(run_ids), terminal, tuple(receipts), inputs, frozen
        )

    for number in range(maximum_runs):
        try:
            existing = authority.journal.get_run(current.run_id)
        except KeyError:
            existing = None
        replaying = existing is not None and existing.status.terminal
        remaining = (current.deadline - current.clock()).total_seconds()
        if not replaying:
            current.budget.check_cancel()
            if remaining <= 0:
                raise TimeoutError("research acquisition episode deadline exceeded")
        run_ids.append(current.run_id)
        analysis = authority.analyze(
            run_id=current.run_id,
            provider=provider,
            inputs=inputs,
            prior_thesis_run_id=prior_thesis_run_id,
            prior_adoption_ref=prior_adoption_ref,
            readonly_tools=tuple(_yielding_tool(tool) for tool in current.descriptors())
            + tool_factory(inputs, current.run_id),
        )
        # The authority still verifies the complete immutable binding on replay.
        # Expiry prevents new work; it cannot erase an already signed terminal.
        terminal = await analysis if replaying else await asyncio.wait_for(analysis, remaining)
        if terminal.get("status") == "completed":
            return outcome("completed", terminal)
        cancellation_proof = _undispatched_cancellation(authority, current, terminal)
        if (
            terminal.get("reason") != ResearchAcquisitionRequired.__name__
            and cancellation_proof is None
        ):
            return outcome("incomplete", terminal)
        # The signed old Run is now terminal, and the upstream invocation has ended.
        if not authority.journal.get_run(current.run_id).status.terminal:
            raise PermissionError("acquisition cannot run inside an active research Run")
        if number + 1 == maximum_runs:
            return outcome("continuation_limit", terminal)
        transform_proof: dict[str, object] | None = None
        if cancellation_proof is None:
            results = await current.fulfill_pending()
            if not results or any(item.status in {"pending", "uncertain"} for item in results):
                receipts.extend(results)
                return outcome("acquisition_wait", terminal)
            if successor_transform is None:
                inputs, frozen = await freeze_acquired_research(inputs, current, results)
            else:
                predecessor = inputs
                transformed = await successor_transform(inputs, current, results)
                cutoff, verified = current.successor_input(transformed.acquisitions)
                if (
                    transformed.frozen_input != verified
                    or transformed.inputs.repository.evidence_pack.as_of != cutoff
                    or transformed.inputs.thesis_epoch != inputs.thesis_epoch
                    or transformed.inputs.allowed_horizons != inputs.allowed_horizons
                    or transformed.inputs.date_presentation != inputs.date_presentation
                    or transformed.inputs.candidate_theses != inputs.candidate_theses
                    or transformed.inputs.repository.evidence_pack.event_id
                    != inputs.repository.evidence_pack.event_id
                    or not set(inputs.repository.evidence_pack.allowed_targets)
                    <= set(transformed.inputs.repository.evidence_pack.allowed_targets)
                    or any(
                        ref not in transformed.inputs.repository.evidence_pack.evidence
                        for ref in predecessor.repository.evidence_pack.evidence
                    )
                ):
                    raise PermissionError(
                        "successor transform changed prior authority or receipt cutoff"
                    )
                if transformed.inputs.target_id != predecessor.target_id:
                    prior_thesis_run_id = None
                    prior_adoption_ref = None
                inputs, frozen, results = (
                    transformed.inputs,
                    transformed.frozen_input,
                    transformed.acquisitions,
                )
                transform_proof = transformed.provenance
                if transformed.stop_reason is not None:
                    receipts.extend(results)
                    current._append(  # pyright: ignore[reportPrivateUsage]
                        "research.successor-refused." + canonical_hash(current.run_id),
                        "research.successor.refused",
                        {
                            "predecessor_run_id": current.run_id,
                            "reason": transformed.stop_reason,
                            "provenance": transform_proof,
                        },
                    )
                    return outcome("successor_refused", terminal)
                if any(item.status in {"pending", "uncertain"} for item in results):
                    receipts.extend(results)
                    return outcome("acquisition_wait", terminal)
            receipts.extend(results)
        successor_run_id = f"{base_run_id}.continuation.{number + 1}"
        current._append(  # pyright: ignore[reportPrivateUsage]
            f"research.continuation.{canonical_hash(successor_run_id)}",
            "research.continuation.bound",
            {
                "predecessor_run_id": current.run_id,
                "predecessor_terminal_hash": authority.journal.get_run(
                    current.run_id
                ).terminal_artifact_id,
                "successor_run_id": successor_run_id,
                "successor_inputs": inputs.identity_dict(),
                "snapshot_ids": sorted(() if frozen is None else frozen.authorized_snapshot_ids),
                "account_scope": authority.account_scope,
                "arm_id": authority.arm_id,
                "experiment_id": authority.experiment_id,
                "parent_run_id": current.budget.owner_run_id,
                **({"successor_transform": transform_proof} if transform_proof is not None else {}),
                **(
                    {
                        "continuation_reason": "cancelled_before_dispatch",
                        "cancellation_proof": cancellation_proof,
                    }
                    if cancellation_proof is not None
                    else {}
                ),
            },
        )
        current = OnDemandResearch(
            store=current.store,
            parent_budget=current.budget,
            episode_deadline=current.deadline,
            episode_id=current.episode_id,
            run_id=successor_run_id,
            cutoff=inputs.repository.evidence_pack.as_of,
            pit_lane=current.pit_lane,
            templates=tuple(current.templates.values()),
            frozen_input=frozen,
            historical_inputs=current.historical_inputs,
            clock=current.clock,
        )
    raise AssertionError("positive bounded Run loop did not terminate")
