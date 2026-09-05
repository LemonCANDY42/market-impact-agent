"""Production preparation for discovery from real, durable prospective receipts."""

from __future__ import annotations

import json
import os
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import cast

from market_impact_agent.account_state import AccountStateSnapshot
from market_impact_agent.agent_contracts import (
    EvidencePack,
    EvidenceReference,
    canonical_hash,
    evidence_pack_from_dict,
)
from market_impact_agent.continuous_study_runner import (
    load_prepared_continuous_registration,
    study_budget,
)
from market_impact_agent.data_inputs import (
    DataPITLane,
    FrozenDataSnapshotInput,
    LocalDataSnapshotStore,
)
from market_impact_agent.frozen_research import FrozenResearchRepository
from market_impact_agent.historical_ashare_inputs import (
    HistoricalAShareInputs,
    ModeledHistoricalPolicy,
)
from market_impact_agent.model_provider import (
    load_model_provider_profile,
    model_provider_profile_from_dict,
)
from market_impact_agent.observations import ObservationCapability
from market_impact_agent.on_demand_research import (
    OnDemandResearch,
    ResearchContinuation,
    ResearchSourceTemplate,
)
from market_impact_agent.research import EvidenceTier
from market_impact_agent.research_thesis_runtime import (
    ResearchThesisAuthority,
    ResearchThesisRunInputs,
)
from market_impact_agent.tushare_observation import (
    TushareObservationProvider,
    load_tushare_observation_source,
)

_PROJECT = Path(__file__).resolve().parents[2]
_SOURCES = (
    "fund-daily",
    "fund-adj",
    "adj-factor",
    "fund-div",
    "etf-basic",
    "stk-limit",
    "suspend-d",
    "trade-cal",
    "daily",
    "stock-basic",
    "dividend",
    "index-classify",
    "index-member-all",
    "etf-sh-cons",
    "news",
)
_MODELS = ("luna-max", "terra-high", "sol-high")
_MAXIMUM_RUNS = 2


def discovery_source_templates(
    *, current_authority: bool = False
) -> tuple[ResearchSourceTemplate, ...]:
    sources = tuple(
        load_tushare_observation_source(
            _PROJECT / f"examples/providers/tushare-observation-{name}-v1.json"
        )
        for name in _SOURCES
    )
    credential = os.environ.get("TUSHARE_TOKEN")
    if not credential:
        raise PermissionError("existing Tushare credential is unavailable")
    # Retain original source identities for cache reuse when enabling a route.
    original = tuple(item for item in sources if item.api_name != "adj_factor")
    additions = tuple(item for item in sources if item.api_name == "adj_factor")
    provider = TushareObservationProvider(credential, original)
    extra_provider = TushareObservationProvider(credential, additions)
    current = ()
    if current_authority:
        current_sources = tuple(
            load_tushare_observation_source(
                _PROJECT / f"examples/providers/tushare-observation-{name}-v1.json"
            )
            for name in ("fund-basic", "rt-min", "rt-etf-min")
        )
        current_provider = TushareObservationProvider(credential, current_sources)
        current = tuple(
            ResearchSourceTemplate.from_tushare(current_provider, item.source_id)
            for item in current_sources
        )
    return (
        *current,
        *(ResearchSourceTemplate.from_tushare(provider, item.source_id) for item in original),
        *(
            ResearchSourceTemplate.from_tushare(extra_provider, item.source_id)
            for item in additions
        ),
    )


def prepare_prospective_discovery(
    *,
    study_root: Path,
    receipt_binding_path: Path,
    receipt_report_path: Path,
    receipt_episode_id: str,
    receipt_run_id: str,
    templates: tuple[ResearchSourceTemplate, ...] | None = None,
    rule_policy_event_id: str | None = None,
) -> dict[str, object]:
    """Reopen parent completions and freeze a deterministic bounded news panel."""
    budget = study_budget(study_root, "unseen_and_prospective")
    study = load_prepared_continuous_registration(study_root)
    store = LocalDataSnapshotStore(budget.journal.path.parent)
    binding = cast(dict[str, str], json.loads(receipt_binding_path.read_text()))
    report = cast(dict[str, object], json.loads(receipt_report_path.read_text()))
    selected_templates = (
        discovery_source_templates(current_authority=rule_policy_event_id is not None)
        if templates is None
        else templates
    )
    policy = None
    if rule_policy_event_id is not None:
        from market_impact_agent.ashare_security_qualification import SourceBackedAShareRulePolicy

        policy = SourceBackedAShareRulePolicy.from_accepted_event(store, rule_policy_event_id)
    receipt_event = budget.journal.event(
        f"{budget.owner_run_id}.binding.{canonical_hash(receipt_run_id)}"
    )
    if receipt_event is None or receipt_event.event_type != "research.binding":
        raise PermissionError("source receipts have no durable original acquisition binding")
    receipt_template_ids = cast(list[str], receipt_event.payload["template_ids"])
    receipt_templates = tuple(
        item for item in selected_templates if item.template_id in receipt_template_ids
    )
    if sorted(item.template_id for item in receipt_templates) != receipt_template_ids:
        raise PermissionError("original receipt source templates are unavailable")
    acquisition = OnDemandResearch(
        store=store,
        parent_budget=budget,
        episode_id=receipt_episode_id,
        episode_deadline=datetime.fromisoformat(binding["deadline"]),
        run_id=receipt_run_id,
        cutoff=datetime.fromisoformat(binding["cutoff"]),
        pit_lane=DataPITLane.PROSPECTIVE,
        templates=receipt_templates,
    )
    receipts = tuple(
        ResearchContinuation(
            str(item["request_id"]),
            str(item["status"]),
            cast(str | None, item["snapshot_id"]),
            None
            if item["successor_cutoff"] is None
            else datetime.fromisoformat(str(item["successor_cutoff"])),
            cast(str | None, item["error_kind"]),
        )
        for item in cast(list[dict[str, object]], report["results"])
    )
    if not receipts or any(item.status != "fulfilled" for item in receipts):
        raise PermissionError("prospective preparation requires completed source receipts")
    cutoff, frozen = acquisition.successor_input(receipts)
    observations = {
        observation.observation_id: (snapshot.snapshot_id, observation)
        for snapshot_id in sorted(frozen.authorized_snapshot_ids)
        for snapshot in (store.get(snapshot_id),)
        for observation in snapshot.observations
        if observation.capability is ObservationCapability.EVENT_REVELATION
        and observation.times.retrieved_at <= cutoff
        and observation.times.available_at is not None
        and observation.times.available_at <= cutoff
    }
    chosen = sorted(
        observations.values(),
        key=lambda item: (
            item[1].times.published_at or item[1].times.occurred_at,
            item[1].observation_id,
        ),
        reverse=True,
    )[:24]
    if not chosen:
        raise PermissionError("completed receipts contain no cutoff-visible news")
    documents: dict[str, object] = {}
    references: list[EvidenceReference] = []
    for snapshot_id, observation in chosen:
        evidence_id = observation.observation_id
        document = {
            "snapshot_id": snapshot_id,
            "observation_id": observation.observation_id,
            "raw_content_hash": observation.raw_content_hash,
            "published_at": None
            if observation.times.published_at is None
            else observation.times.published_at.isoformat(),
            "retrieved_at": observation.times.retrieved_at.isoformat(),
            "record": observation.normalized_payload["record"],
        }
        documents[evidence_id] = document
        references.append(
            EvidenceReference(
                evidence_id=evidence_id,
                claim_id="prospective-news-record",
                source_ref=f"data-snapshot://{snapshot_id}/{evidence_id}",
                source_tier=EvidenceTier.UNVERIFIED,
                available_at=observation.times.retrieved_at,
                content_hash=canonical_hash(document),
                summary=(
                    "Actually received news; source contents and economic claims "
                    "require verification."
                ),
            )
        )
    pack = EvidencePack.build(
        event_id="prospective-discovery-" + canonical_hash([item.to_dict() for item in receipts]),
        as_of=cutoff,
        research_question=(
            "Research the economic effects of the received news on A-share industries "
            "and companies. "
            "Use the semantic source tools to verify company or ETF identity and missing evidence "
            "when a concrete candidate is supported. Any A-share company or unlevered equity ETF "
            "may be researched; 510300.SH and 510500.SH are only initial seeds. "
            "Explain transmission, expectations, counterevidence and a defensible horizon. "
            "Do not force a candidate or a trade when the evidence is insufficient. "
            "An account proposal requires separately verified account and security authority."
        ),
        evidence=tuple(references),
        pattern_packs=(),
        allowed_targets=("ASHARE.RESEARCH",),
        data_gaps=(
            f"Bounded news panel: latest {len(chosen)} of {len(observations)} "
            "visible received records.",
            "News is unverified source material; current account and execution eligibility "
            "are not established.",
        ),
    )
    artifact = store.artifacts.put_json({"evidence_pack": pack.to_dict(), "documents": documents})
    profiles = tuple(
        load_model_provider_profile(_PROJECT / f"examples/providers/pi-cpa-{name}-v2.json")
        for name in _MODELS
    )
    if [profile.profile_hash for profile in profiles] != [
        item["provider_profile_hash"]
        for item in cast(list[dict[str, object]], study["model_profiles"])
    ]:
        raise PermissionError("prospective profiles differ from the frozen study models")
    maximum_runs = 3 if policy is not None else _MAXIMUM_RUNS
    maximum_cost = sum(
        cast(int, profile.budget.max_estimated_cost_microusd) * maximum_runs for profile in profiles
    )
    registration: dict[str, object] = {
        "schema_version": "market-impact.prospective-discovery-entry.v1",
        "study_registration_id": study["registration_id"],
        "receipt_binding": acquisition.binding,
        "receipts": [item.to_dict() for item in receipts],
        "cutoff": cutoff.isoformat(),
        "episode_deadline": acquisition.deadline.isoformat(),
        "snapshot_ids": sorted(frozen.authorized_snapshot_ids),
        "research_artifact_hash": artifact.content_hash,
        "research_input_bytes": artifact.size_bytes,
        "source_template_ids": sorted(item.template_id for item in selected_templates),
        "profiles": [profile.to_dict() for profile in profiles],
        "maximum_runs_per_model": maximum_runs,
        "maximum_research_cost_microusd": maximum_cost,
        "budget_scope": "unseen_and_prospective",
        "news_selection_policy": "latest-24-by-published-time-and-observation-id-compact-v2",
        "current_account_authority_available": False,
        "portfolio_authority_available": False,
        "broker_access": False,
    }
    if policy is not None:
        registration.update(
            {
                "schema_version": "market-impact.prospective-discovery-entry.v2",
                "rule_policy_event_id": policy.acceptance_event_id,
                "rule_policy_artifact_hash": policy.policy_artifact_hash,
                "current_account_authority_available": True,
                "portfolio_authority_available": True,
                "mock_opening_policy": "100k-half-hs300-overnight-v1",
                "stage_affordability_policy": "sequential-whole-episode-reservation-v1",
                "maximum_stage_cost_microusd": 2_500_000,
                "maximum_research_cost_microusd": min(maximum_cost, 2_500_000),
                "per_model_episode_allowance_microusd": {
                    profile.profile_id: cast(int, profile.budget.max_estimated_cost_microusd)
                    * maximum_runs
                    for profile in profiles
                },
            }
        )
    if policy is None and maximum_cost > 2_500_000:
        raise PermissionError("prospective research batch cannot fit its registered stage cap")
    identity = canonical_hash(registration)
    registration["registration_id"] = "prospective-discovery-" + identity
    saved = store.artifacts.put_json(registration)
    budget.journal.append(
        run_id=budget.owner_run_id,
        event_id=f"{budget.owner_run_id}.prospective.{identity}.prepared",
        event_type="prospective.discovery.prepared",
        observed_at=cutoff,
        payload={"artifact_hash": saved.content_hash},
    )
    return {**registration, "artifact_hash": saved.content_hash, "model_calls": 0}


async def run_prepared_prospective_discovery(
    *,
    study_root: Path,
    registration_path: Path,
) -> dict[str, object]:
    """Run each registered model with the same received facts and separate provenance."""
    from market_impact_agent.pi_runtime import PiRuntimeProvider
    from market_impact_agent.prospective_discovery_runtime import (
        discovery_acquisition_wait,
        latest_discovery_report,
        run_prospective_discovery,
    )

    budget = study_budget(study_root, "unseen_and_prospective")
    store = LocalDataSnapshotStore(budget.journal.path.parent)
    supplied = cast(dict[str, object], json.loads(registration_path.read_text()))
    digest = str(supplied["artifact_hash"])
    registration = cast(dict[str, object], store.artifacts.read_json(digest))
    identity = str(registration["registration_id"]).removeprefix("prospective-discovery-")
    event = budget.journal.event(f"{budget.owner_run_id}.prospective.{identity}.prepared")
    if (
        supplied != {**registration, "artifact_hash": digest, "model_calls": 0}
        or event is None
        or event.payload != {"artifact_hash": digest}
        or canonical_hash({k: v for k, v in registration.items() if k != "registration_id"})
        != identity
        or registration["study_registration_id"]
        != load_prepared_continuous_registration(study_root)["registration_id"]
    ):
        raise PermissionError("prospective inputs differ from their prepared parent authority")
    current_authority = (
        registration["schema_version"] == "market-impact.prospective-discovery-entry.v2"
    )
    templates = (
        discovery_source_templates(current_authority=True)
        if current_authority
        else discovery_source_templates()
    )
    if sorted(item.template_id for item in templates) != registration["source_template_ids"]:
        raise PermissionError("prospective source templates changed after preparation")
    artifact = cast(
        dict[str, object], store.artifacts.read_json(str(registration["research_artifact_hash"]))
    )
    pack = evidence_pack_from_dict(artifact["evidence_pack"])
    if pack.as_of != datetime.fromisoformat(str(registration["cutoff"])):
        raise PermissionError("prospective research cutoff differs from source receipts")
    repository = FrozenResearchRepository(
        evidence_pack=pack,
        evidence_documents=cast(dict[str, object], artifact["documents"]),
        pattern_packs={},
    )
    frozen = FrozenDataSnapshotInput(frozenset(cast(list[str], registration["snapshot_ids"])))
    profiles = tuple(
        model_provider_profile_from_dict(item)
        for item in cast(list[object], registration["profiles"])
    )
    rows: list[dict[str, object]] = []

    def unavailable_account() -> AccountStateSnapshot:
        raise PermissionError("no authoritative current Mock account was supplied")

    def admission_source(
        _inputs: ResearchThesisRunInputs,
        final_frozen: FrozenDataSnapshotInput,
    ) -> HistoricalAShareInputs:
        # Reuse the existing restrictive source adapter. This entry does not mint
        # present-day exchange rules or promote historical eligibility to Paper.
        return HistoricalAShareInputs(
            store=store,
            snapshot_ids=tuple(sorted(final_frozen.authorized_snapshot_ids)),
            rule_artifact_hashes=(),
            policy=ModeledHistoricalPolicy(
                "prospective-inspection-no-rule-authority-v1", Decimal(".001")
            ),
        )

    for profile in profiles:
        arm = profile.profile_id
        episode_id = str(registration["registration_id"]) + "." + arm
        outcome_event_id = f"{budget.owner_run_id}.prospective.model." + canonical_hash(episode_id)
        existing_outcome = (
            latest_discovery_report(budget.journal, outcome_event_id, "artifact_hash")
            if current_authority
            else None
        )
        used_runs = 0
        if existing_outcome is not None:
            prior_row = cast(
                dict[str, object],
                store.artifacts.read_json(str(existing_outcome.payload["artifact_hash"])),
            )
            proof = (
                cast(
                    dict[str, object],
                    store.artifacts.read_json(str(prior_row["proof_artifact_hash"])),
                )
                if prior_row.get("status") == "incomplete" and "proof_artifact_hash" in prior_row
                else {}
            )
            if not discovery_acquisition_wait(proof):
                rows.append(prior_row)
                continue
            run_ids = cast(list[str], proof["research_run_ids"])
            if (
                run_ids
                != [
                    episode_id + ".research" + (f".continuation.{i}" if i else "")
                    for i in range(len(run_ids))
                ]
                or not run_ids
            ):
                raise PermissionError("acquisition wait report differs from its original episode")
            if any(not budget.journal.get_run(run_id).status.terminal for run_id in run_ids):
                raise PermissionError("acquisition wait cannot reopen an active model Run")
            used_runs = len(run_ids)
        composition = None
        if current_authority:
            allowance = cast(dict[str, int], registration["per_model_episode_allowance_microusd"])[
                arm
            ]
            # Sealed Runs are replayed, never regenerated or charged a second full allowance.
            allowance = max(
                0, allowance - used_runs * cast(int, profile.budget.max_estimated_cost_microusd)
            )
            spent = budget.scope_summary()
            if spent["known_cost_microusd"] + spent["reserved_microusd"] + allowance > int(
                str(registration["maximum_stage_cost_microusd"])
            ):
                rows.append(
                    {
                        "model": profile.model,
                        "effort": profile.reasoning_effort,
                        "status": "pending_budget",
                        "gaps": ["registered_stage_episode_allowance_unaffordable"],
                        "episode_allowance_microusd": allowance,
                        "stage_budget": spent,
                        "execution_dispatched": False,
                    }
                )
                pending = store.artifacts.put_json(rows[-1])
                if existing_outcome is not None:
                    # Budget pressure does not replace the durable resumable wait.
                    continue
                budget.journal.append(
                    run_id=budget.owner_run_id,
                    event_id=outcome_event_id,
                    event_type="prospective.discovery.model.reported",
                    observed_at=datetime.now(UTC),
                    payload={"artifact_hash": pending.content_hash},
                )
                continue
            from market_impact_agent.ashare_security_qualification import (
                SourceBackedAShareRulePolicy,
            )
            from market_impact_agent.prospective_ashare_quotes import (
                ExecutableProspectiveAShareInputs,
            )
            from market_impact_agent.prospective_mock_composition import ProspectiveMockComposition

            accepted_policy = SourceBackedAShareRulePolicy.from_accepted_event(
                store, str(registration["rule_policy_event_id"])
            )
            if accepted_policy.policy_artifact_hash != registration["rule_policy_artifact_hash"]:
                raise PermissionError("prospective generic rule policy changed")
            composition = ProspectiveMockComposition(
                store=store,
                profile_id=arm,
                study_registration_id=str(registration["study_registration_id"]),
                opening_authority_ref=str(registration["study_registration_id"]),
                parent_run_id=budget.owner_run_id,
                market_factory=lambda snapshots, policy=accepted_policy: (
                    ExecutableProspectiveAShareInputs(
                        store=store,
                        snapshot_ids=tuple(sorted(snapshots.authorized_snapshot_ids)),
                        qualification_policy=policy,
                    )
                ),
            )
        authority = ResearchThesisAuthority(
            store,
            experiment_id=str(registration["registration_id"]),
            arm_id=arm,
            account_scope="research-context-" + canonical_hash(episode_id)
            if composition is None
            else composition.account_scope,
        )
        acquisition = OnDemandResearch(
            store=store,
            parent_budget=budget,
            episode_id=episode_id,
            episode_deadline=datetime.fromisoformat(str(registration["episode_deadline"])),
            run_id=episode_id + ".research",
            cutoff=pack.as_of,
            pit_lane=DataPITLane.PROSPECTIVE,
            templates=templates,
            frozen_input=frozen,
        )
        provider = PiRuntimeProvider(profile, budget=budget)
        try:
            result = await run_prospective_discovery(
                authority=authority,
                provider=provider,
                inputs=ResearchThesisRunInputs(
                    repository, "ASHARE.RESEARCH", episode_id, frozenset({1, 3, 5, 10, 20, 60})
                ),
                acquisition=acquisition,
                account_source=unavailable_account
                if composition is None
                else composition.account_source,
                account_max_age=timedelta(minutes=5),
                admission_authority_factory=admission_source
                if composition is None
                else composition.admission_source,
                portfolio_authority_factory=None
                if composition is None
                else composition.portfolio_authority,
                portfolio_context_source=None
                if composition is None
                else composition.capture_context,
                maximum_runs=int(str(registration["maximum_runs_per_model"])),
            )
            rows.append(
                {"model": profile.model, "effort": profile.reasoning_effort, **result.to_dict()}
            )
            if composition is not None and result.portfolio_run_id is not None:
                from market_impact_agent.prospective_mock_execution import (
                    dispatch_prospective_mock_review,
                )

                rows[-1].update(
                    dispatch_prospective_mock_review(composition, result.portfolio_run_id)
                )
            if current_authority:
                reported = store.artifacts.put_json(rows[-1])
                payload: dict[str, object] = {"artifact_hash": reported.content_hash}
                if existing_outcome is not None:
                    if existing_outcome.payload["artifact_hash"] == reported.content_hash:
                        continue
                    outcome_event_id = (
                        existing_outcome.event_id
                        + ".revision."
                        + str(existing_outcome.payload["artifact_hash"])
                    )
                    payload["previous_report_event_id"] = existing_outcome.event_id
                budget.journal.append(
                    run_id=budget.owner_run_id,
                    event_id=outcome_event_id,
                    event_type="prospective.discovery.model.reported",
                    observed_at=datetime.now(UTC),
                    payload=payload,
                )
        finally:
            await provider.close()
    report: dict[str, object] = {
        "schema_version": "market-impact.prospective-discovery-batch.v2"
        if current_authority
        else "market-impact.prospective-discovery-batch.v1",
        "registration_id": registration["registration_id"],
        "denominator": 3,
        "results": rows,
        "budget": budget.summary(),
        "stage_budget": budget.scope_summary(),
        "current_account_authority_available": current_authority,
        "broker_access": False,
        "local_mock_accepted": False,
    }
    saved = store.artifacts.put_json(report)
    budget.journal.append(
        run_id=budget.owner_run_id,
        event_id=f"{budget.owner_run_id}.prospective.{identity}.result.{saved.content_hash}",
        event_type="prospective.discovery.batch.reported",
        observed_at=datetime.now(UTC),
        payload={"artifact_hash": saved.content_hash},
    )
    return {**report, "artifact_hash": saved.content_hash}
