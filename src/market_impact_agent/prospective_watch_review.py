"""Concrete Watch follow-up through the existing prospective research/portfolio path."""

from __future__ import annotations

from collections.abc import Callable
from datetime import timedelta
from typing import cast

from market_impact_agent.account_state import AccountStateSnapshot
from market_impact_agent.agent_contracts import EvidencePack, EvidenceReference, canonical_hash
from market_impact_agent.agent_watch_admission import build_callback_agent_profile_ref
from market_impact_agent.agent_watch_wake_dispatch import AgentWatchWakeDispatcher
from market_impact_agent.data_inputs import DataPITLane, FrozenDataSnapshotInput
from market_impact_agent.dynamic_ashare_admission import (
    HistoricalSecurityEvidenceAuthority,
    SecurityAdmission,
)
from market_impact_agent.dynamic_effectiveness import DatePresentation
from market_impact_agent.frozen_research import FrozenResearchRepository
from market_impact_agent.model_provider import model_provider_profile_from_dict
from market_impact_agent.on_demand_research import OnDemandResearch, ResearchSourceTemplate
from market_impact_agent.pi_runtime import PiRuntimeProvider
from market_impact_agent.portfolio_review import PortfolioReviewAuthority
from market_impact_agent.prospective_discovery_runtime import run_prospective_discovery
from market_impact_agent.research import EvidenceTier
from market_impact_agent.research_thesis_runtime import (
    ResearchThesisAuthority,
    ResearchThesisRunInputs,
)
from market_impact_agent.research_thesis_watch import (
    ResearchThesisWatchAuthorityResolver,
    ResearchThesisWatchReviewContext,
    run_research_thesis_watch_callback,
)


async def run_prospective_watch_review(
    *,
    dispatcher: AgentWatchWakeDispatcher,
    resolver: ResearchThesisWatchAuthorityResolver,
    run_id: str,
    provider: PiRuntimeProvider,
    account_source: Callable[[], AccountStateSnapshot] | None,
    account_max_age: timedelta,
    admission_authority_factory: Callable[
        [ResearchThesisRunInputs, FrozenDataSnapshotInput], HistoricalSecurityEvidenceAuthority
    ]
    | None,
    portfolio_authority_factory: Callable[
        [ResearchThesisRunInputs, FrozenDataSnapshotInput, AccountStateSnapshot, SecurityAdmission],
        PortfolioReviewAuthority,
    ]
    | None,
    source_templates: tuple[ResearchSourceTemplate, ...] = (),
    source_snapshot_ids: tuple[str, ...] = (),
    maximum_runs: int = 2,
) -> dict[str, object]:
    """Reopen actual new receipts, then prior-aware research and current-account review.

    Configured authorities remain caller-owned; absent current account/source/portfolio
    authority produces an explicit pending result and never fabricates an account.
    """
    if not 2 <= maximum_runs <= 4 or account_max_age <= timedelta(0):
        raise ValueError("Watch portfolio review requires two to four bounded native roles")
    if type(provider) is not PiRuntimeProvider or provider.budget is None:
        raise PermissionError(
            "Watch portfolio review requires the existing native budgeted provider"
        )
    if (
        provider.budget.journal.path != resolver.parent_budget.journal.path
        or provider.budget.owner_run_id != resolver.parent_budget.owner_run_id
        or provider.budget.binding != resolver.parent_budget.binding
        or provider.budget.scope != resolver.parent_budget.scope
    ):
        raise PermissionError("Watch portfolio review must retain its original model budget")

    async def review(context: ResearchThesisWatchReviewContext) -> dict[str, object]:
        missing = tuple(
            name
            for name, value in (
                ("account_authority_missing", account_source),
                ("security_source_authority_missing", admission_authority_factory),
                ("portfolio_authority_missing", portfolio_authority_factory),
            )
            if value is None
        )
        if missing:
            return {
                "status": "pending_authority",
                "gaps": list(missing),
                "execution_dispatched": False,
            }
        assert account_source is not None and admission_authority_factory is not None
        assert portfolio_authority_factory is not None
        delegate = context.delegate_profile
        parent_binding = cast(
            dict[str, object],
            resolver.store.artifacts.read_json(
                resolver.journal.get_run(context.parent_run_id).config_hash
            ),
        )
        if parent_binding.get("profile") != provider.profile.to_dict():
            raise PermissionError("Watch callback must retain the original source model Profile")
        if delegate.preloaded_skills:
            return {
                "status": "pending_authority",
                "gaps": ["callback_skill_authority_missing"],
                "execution_dispatched": False,
            }
        if source_templates:
            source_binding = context.parent_budget.journal.event(
                f"{context.parent_budget.owner_run_id}.binding."
                + canonical_hash(context.parent_run_id)
            )
            if source_binding is None or source_binding.event_type != "research.binding":
                raise PermissionError("Watch callback lacks its original source-template binding")
            allowed = cast(list[str], source_binding.payload["template_ids"])
            if not {template.template_id for template in source_templates} <= set(allowed):
                raise PermissionError("Watch callback cannot expand its original source templates")
        expected = build_callback_agent_profile_ref(
            callback_agent_type=delegate.callback_agent_type,
            model_profile_id=provider.profile.profile_id,
            model_profile_hash=provider.profile.profile_hash,
            preloaded_skills=delegate.preloaded_skills,
            skill_manifest_hashes=delegate.skill_manifest_hashes,
            max_turns=delegate.callback_max_turns,
            max_input_tokens=delegate.callback_max_input_tokens,
            max_output_tokens=delegate.callback_max_output_tokens,
            max_cost_microusd=delegate.callback_max_cost_microusd,
        )
        if expected != delegate.callback_agent_profile_ref:
            raise PermissionError(
                "Watch callback provider differs from its frozen model/Skill profile"
            )
        # Each possible native role receives at most this share. No continuation or
        # portfolio stage can replenish the total frozen callback allowance.
        if (
            delegate.callback_max_turns < maximum_runs
            or delegate.callback_max_output_tokens < maximum_runs * 16
            or delegate.callback_max_input_tokens < maximum_runs * 128
            or delegate.callback_max_cost_microusd < maximum_runs
        ):
            return {
                "status": "pending_authority",
                "gaps": ["delegate_budget_insufficient"],
                "execution_dispatched": False,
            }
        fields = provider.profile.to_dict()
        limits = dict(cast(dict[str, object], fields["budget"]))
        for name, cap in (
            ("max_turns", delegate.callback_max_turns // maximum_runs),
            ("max_input_tokens", delegate.callback_max_input_tokens // maximum_runs),
            ("max_output_tokens", delegate.callback_max_output_tokens // maximum_runs),
            ("max_estimated_cost_microusd", delegate.callback_max_cost_microusd // maximum_runs),
        ):
            original = limits.get(name)
            limits[name] = cap if original is None else min(int(cast(int, original)), cap)
        remaining = (context.episode_deadline - resolver.clock()).total_seconds()
        if remaining <= 0:
            raise PermissionError("Watch callback Episode expired")
        limits["max_wall_seconds"] = min(
            float(cast(float, limits["max_wall_seconds"])), remaining / maximum_runs
        )
        fields["budget"] = limits
        fields["reserved_output_tokens"] = min(
            provider.profile.reserved_output_tokens, cast(int, limits["max_output_tokens"])
        )
        # Narrow output allowance without changing the accepted native compaction route.
        fields["compaction_trigger_tokens"] = provider.profile.effective_compaction_trigger_tokens
        fields.pop("profile_id")
        fields["profile_id"] = "model-provider-" + canonical_hash(fields)
        bounded_profile = model_provider_profile_from_dict(fields)
        bounded = PiRuntimeProvider(
            bounded_profile, budget=context.parent_budget, permit=provider.permit
        )
        journal = dispatcher.admission_service.journal
        refs = journal.observation_version_refs_by_ids(context.new_version_ids)
        selected = journal.freeze_version_selection_snapshot(
            selection_id="research-watch-selection-" + canonical_hash(context.callback_run_id),
            readiness_report_id=dispatcher.reopen_dispatch(run_id).binding.binding_id,
            version_ids=tuple(ref.version_id for ref in refs),
            as_of=context.cutoff,
            frozen_at=resolver.clock(),
        )
        documents: dict[str, object] = {}
        evidence: list[EvidenceReference] = []
        # The model sees newly received source facts here. The prior opinion goes
        # exclusively through prior_thesis_run_id and its shared history bound.
        for observation in selected.observations:
            if observation.times.available_at is None:
                raise PermissionError("Watch receipt lacks actual availability")
            document = observation.to_dict()
            documents[observation.observation_id] = document
            evidence.append(
                EvidenceReference(
                    evidence_id=observation.observation_id,
                    claim_id="watch-new-receipt",
                    source_ref=observation.source_ref,
                    source_tier=EvidenceTier.UNVERIFIED,
                    available_at=observation.times.available_at,
                    content_hash=canonical_hash(document),
                    summary="New actual-receipt source version; quality is not upgraded.",
                )
            )
        repository = FrozenResearchRepository(
            evidence_pack=EvidencePack.build(
                event_id=context.thesis.root_event_id,
                as_of=context.cutoff,
                research_question=context.research_question
                + "\nFollow-up question: "
                + context.watch_question,
                evidence=tuple(evidence),
                pattern_packs=(),
                allowed_targets=(context.target_id,),
            ),
            evidence_documents=documents,
            pattern_packs={},
        )
        frozen = FrozenDataSnapshotInput(frozenset({selected.snapshot_id, *source_snapshot_ids}))
        acquisition = OnDemandResearch(
            store=resolver.store,
            parent_budget=context.parent_budget,
            episode_deadline=context.episode_deadline,
            episode_id=context.episode_id,
            run_id=context.callback_run_id + ".research",
            cutoff=context.cutoff,
            pit_lane=DataPITLane.PROSPECTIVE,
            templates=source_templates,
            frozen_input=frozen,
            clock=resolver.clock,
        )
        inputs = ResearchThesisRunInputs(
            repository,
            context.target_id,
            context.thesis.thesis_epoch,
            frozenset({context.thesis.primary_horizon_sessions}),
            date_presentation=DatePresentation(
                str(cast(dict[str, object], parent_binding["inputs"])["date_presentation"])
            ),
        )
        authority = ResearchThesisAuthority(
            resolver.store,
            experiment_id=resolver.experiment_id,
            arm_id=resolver.arm_id,
            account_scope=resolver.account_scope,
            clock=resolver.clock,
        )
        try:
            result = await run_prospective_discovery(
                authority=authority,
                provider=bounded,
                inputs=inputs,
                acquisition=acquisition,
                account_source=account_source,
                account_max_age=account_max_age,
                admission_authority_factory=admission_authority_factory,
                portfolio_authority_factory=portfolio_authority_factory,
                maximum_runs=maximum_runs,
                prior_thesis_run_id=context.parent_run_id,
            )
            return result.to_dict()
        finally:
            await bounded.close()

    return await run_research_thesis_watch_callback(
        dispatcher=dispatcher, resolver=resolver, run_id=run_id, review=review
    )
