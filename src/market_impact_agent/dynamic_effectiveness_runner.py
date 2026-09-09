"""Immutable qualification and opened-case execution for the dynamic study.

The coordinator owns no model protocol.  It freezes Profiles and case inputs,
then delegates every physical request to the accepted pi boundary.  Research
outcomes remain closed here; scoring is a later, separately authorized step.
"""

from __future__ import annotations

import asyncio
import json
import os
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import cast

from market_impact_agent.account_state import AccountPosition, CashBalance
from market_impact_agent.agent_contracts import (
    EvidencePack,
    EvidenceReference,
    canonical_hash,
)
from market_impact_agent.agent_schema import validate_agent_contract
from market_impact_agent.authorized_decision_view import AuthorizedDecisionView
from market_impact_agent.checkpoint_market_universe import (
    ExchangeInstrumentRule,
    ExchangeInstrumentRuleSet,
)
from market_impact_agent.data_inputs import LocalDataSnapshotStore
from market_impact_agent.decision_thesis import BaseCaseDirection, ResearchThesisV1
from market_impact_agent.domain import ApprovalMode, Side, TradingEnvironment, TradingMandateV2
from market_impact_agent.dynamic_effectiveness import (
    AnalysisTopology,
    CaseRole,
    DatePresentation,
    DynamicEffectivenessRegistrationV1,
    MemorySensitivityPair,
    ModelStudyArm,
    StudyCase,
)
from market_impact_agent.frozen_research import FrozenResearchRepository
from market_impact_agent.model_budget import ModelBudget, ModelBudgetGroupMember, ModelBudgetScope
from market_impact_agent.model_provider import (
    ModelProviderProfile,
    model_provider_profile_from_dict,
)
from market_impact_agent.paper_execution import PriceBasis
from market_impact_agent.pi_deployment import PiRuntimePermit
from market_impact_agent.pi_execution import native_turn
from market_impact_agent.pi_runtime import (
    ExperimentSlots,
    PiRuntimeProvider,
    runtime_identity,
    shared_admission_root,
)
from market_impact_agent.portfolio_decision import (
    PortfolioExposureViewV2,
    RawMarkedPositionV2,
    RegisteredPortfolioExposureViewAuthorityV2,
)
from market_impact_agent.portfolio_review import PortfolioReviewAuthority, PortfolioReviewInputs
from market_impact_agent.providers import MockExecutionProvider
from market_impact_agent.research import EvidenceTier
from market_impact_agent.research_thesis_runtime import (
    RESEARCH_THESIS_PROMPT,
    RESEARCH_THESIS_V2_PROMPT,
    ResearchThesisAuthority,
    ResearchThesisRunInputs,
    reopen_completed_research_thesis,
    theses_semantically_disagree,
)
from market_impact_agent.runtime_store import RunJournal, RunStatus
from market_impact_agent.usage_ledger import UsageLedger

_OPENED_INPUTS = (
    "cn-2018-bear-market/2018-07-02",
    "cn-2019-q1-fast-rebound/2019-01-07",
    "cn-2020-covid-closure-shock/2020-02-03",
    "cn-2020-covid-closure-shock/2020-03-23",
    "cn-2021-index-flat-sector-rotation/2021-07-01",
    "cn-2021-index-flat-sector-rotation/2021-12-01",
    "cn-2024-policy-melt-up/2024-09-24",
    "cn-2024-post-rally-whipsaw/2024-10-09",
)
_TOPOLOGIES = (
    AnalysisTopology.LUNA_MAX,
    AnalysisTopology.TERRA_HIGH,
    AnalysisTopology.SOL_HIGH,
)
_PROFILE_EXPECTATIONS = {
    AnalysisTopology.LUNA_MAX: ("gpt-5.6-luna", "max"),
    AnalysisTopology.TERRA_HIGH: ("gpt-5.6-terra", "high"),
    AnalysisTopology.SOL_HIGH: ("gpt-5.6-sol", "high"),
}
_NEUTRAL_QUESTION = (
    "Using only the frozen point-in-time evidence, what is the most defensible base-case "
    "direction and primary horizon for the registered broad-market research proxy?"
)
_PORTFOLIO_SCENARIOS = (
    {
        "scenario_id": "bullish-cash",
        "required_direction": "up",
        "opening_cash": "10000",
        "opening_quantity": "0",
        "expected_actions": ["open", "hold"],
    },
    {
        "scenario_id": "bullish-overconcentrated",
        "required_direction": "up",
        "opening_cash": "2000",
        "opening_quantity": "80",
        "expected_actions": ["reduce", "hold"],
    },
    {
        "scenario_id": "bearish-existing-long",
        "required_direction": "down",
        "opening_cash": "5000",
        "opening_quantity": "50",
        "expected_actions": ["reduce", "close", "hold"],
    },
    {
        "scenario_id": "rangebound-cash",
        "required_direction": "rangebound",
        "opening_cash": "10000",
        "opening_quantity": "0",
        "expected_actions": ["hold"],
    },
)


@dataclass(frozen=True, slots=True)
class OpenedCaseSource:
    case_id: str
    input_ref: str
    target_id: str
    frozen_input_hash: str
    evidence_pack_hash: str
    evidence_documents_hash: str
    pattern_pack_hash: str

    def to_dict(self) -> dict[str, object]:
        return {
            "case_id": self.case_id,
            "input_ref": self.input_ref,
            "target_id": self.target_id,
            "allowed_horizons": [1, 3, 5, 10, 20, 60],
            "research_question": _NEUTRAL_QUESTION,
            "frozen_input_hash": self.frozen_input_hash,
            "evidence_pack_hash": self.evidence_pack_hash,
            "evidence_documents_hash": self.evidence_documents_hash,
            "pattern_pack_hash": self.pattern_pack_hash,
        }


def prepare_dynamic_effectiveness_study(
    root: Path,
    *,
    inputs_root: Path,
    pattern_pack_path: Path | tuple[Path, ...],
    profiles: tuple[ModelProviderProfile, ModelProviderProfile, ModelProviderProfile],
    registered_at: datetime | None = None,
) -> dict[str, object]:
    """Freeze the exact eight opened inputs without opening their outcomes."""

    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(root, 0o700)
    path = root / "registration.json"
    if path.exists():
        return load_dynamic_effectiveness_study(root)
    ordered_profiles = _ordered_profiles(profiles)
    pattern_pack_paths = _pattern_pack_candidates(pattern_pack_path)
    sources = tuple(
        _opened_source(inputs_root, pattern_pack_paths, input_ref) for input_ref in _OPENED_INPUTS
    )
    arms = tuple(
        ModelStudyArm(
            topology=topology,
            model=profile.model,
            reasoning_effort=cast(str, profile.reasoning_effort),
            provider_profile_id=profile.profile_id,
            provider_profile_hash=profile.profile_hash,
            pricing_id=profile.pricing.pricing_id,
        )
        for topology, profile in zip(_TOPOLOGIES, ordered_profiles, strict=True)
    )
    cases = tuple(
        StudyCase(
            case_id=source.case_id,
            role=CaseRole.OPENED_DEVELOPMENT,
            frozen_input_hash=source.frozen_input_hash,
            event_year=int(source.case_id[:4]),
            category=source.input_ref.split("/", maxsplit=1)[0],
        )
        for source in sources
    )
    registration = DynamicEffectivenessRegistrationV1(
        experiment_id="dynamic-horizon-development-20260904",
        registered_at=registered_at or datetime.now(UTC),
        runtime_identity_hash=canonical_hash(runtime_identity()),
        model_arms=cast(
            tuple[ModelStudyArm, ModelStudyArm, ModelStudyArm],
            arms,
        ),
        opened_cases=cases,
        stability_case_ids=("2018-07-02", "2019-01-07", "2020-02-03"),
        memory_sensitivity_pairs=(
            MemorySensitivityPair("2020-02-03", sources[2].frozen_input_hash),
        ),
    )
    if errors := validate_agent_contract(
        registration.to_dict(), "dynamic-effectiveness-registration-v1.schema.json"
    ):
        raise ValueError("dynamic effectiveness registration is invalid: " + "; ".join(errors))
    value: dict[str, object] = {
        "schema_version": "market-impact.dynamic-effectiveness-execution.v1",
        "study": registration.to_dict(),
        "runtime": runtime_identity(),
        "profiles": {
            topology.value: profile.to_dict()
            for topology, profile in zip(_TOPOLOGIES, ordered_profiles, strict=True)
        },
        "opened_case_sources": [source.to_dict() for source in sources],
        "portfolio_scenarios": list(_PORTFOLIO_SCENARIOS),
        "portfolio_thesis_selection": (
            "first completed base run in registered case order for the required direction; "
            "the same bullish thesis is reused across the two account states"
        ),
        "outcomes_visible_to_agents": False,
        "live_execution": False,
    }
    value["registration_hash"] = canonical_hash(value)
    _write_new(path, value)
    return value


def load_dynamic_effectiveness_study(root: Path) -> dict[str, object]:
    value = _read_object(root / "registration.json")
    core = {key: item for key, item in value.items() if key != "registration_hash"}
    if (
        canonical_hash(core) != value.get("registration_hash")
        or value.get("runtime") != runtime_identity()
        or value.get("outcomes_visible_to_agents") is not False
        or value.get("live_execution") is not False
    ):
        raise ValueError("dynamic effectiveness registration changed or belongs to another build")
    study = _object(value["study"])
    if errors := validate_agent_contract(
        study, "dynamic-effectiveness-registration-v1.schema.json"
    ):
        raise ValueError("stored dynamic effectiveness study is invalid: " + "; ".join(errors))
    return value


def _qualification_budget_binding(budget: ModelBudget) -> dict[str, object]:
    if not budget.journal.promotion_eligible or budget.scope != "route_qualification":
        raise ValueError(
            "qualification requires an authoritative registered route_qualification budget"
        )
    limit = next(item for item in budget.scope_limits if item.name == budget.scope)
    if limit.max_cost_microusd > 1_000_000:
        raise ValueError("route qualification scope exceeds its one-dollar authorization")
    record = budget.journal.get_run(budget.owner_run_id)
    budget.summary()
    return {
        "journal_path": str(budget.journal.path),
        "harness_authority_id": budget.journal.harness_authority_id,
        "owner_run_id": budget.owner_run_id,
        "owner_config_hash": record.config_hash,
        "scope": budget.scope,
        "limits": budget.binding,
    }


def _qualification_budget(
    registration: dict[str, object], *, shared_budget: ModelBudget | None = None
) -> ModelBudget | None:
    raw = registration.get("shared_budget")
    if raw is None:
        if shared_budget is not None:
            raise ValueError("qualification registration has no shared parent budget")
        return None
    frozen = _object(raw)
    if shared_budget is None:
        journal_path = Path(_string(frozen, "journal_path"))
        if not journal_path.is_file():
            raise ValueError("qualification parent journal is missing")
        store = LocalDataSnapshotStore(journal_path.parent)
        journal = RunJournal.authoritative(store)
        limits = _object(frozen["limits"])
        scopes = cast(list[dict[str, object]], limits.get("scope_limits", []))
        shared_budget = ModelBudget(
            journal=journal,
            owner_run_id=_string(frozen, "owner_run_id"),
            max_requests=cast(int, limits["max_requests"]),
            max_cost_microusd=cast(int | None, limits["max_cost_microusd"]),
            prior_requests=cast(int, limits["prior_requests"]),
            prior_cost_microusd=cast(int, limits["prior_cost_microusd"]),
            prior_reserved_microusd=cast(int, limits.get("prior_reserved_microusd", 0)),
            prior_unsettled_requests=cast(int, limits.get("prior_unsettled_requests", 0)),
            scope_limits=tuple(
                ModelBudgetScope(
                    name=_string(item, "name"),
                    max_cost_microusd=cast(int, item["max_cost_microusd"]),
                    prior_cost_microusd=cast(int, item["prior_cost_microusd"]),
                    prior_reserved_microusd=cast(int, item["prior_reserved_microusd"]),
                )
                for item in scopes
            ),
            scope=_string(frozen, "scope"),
        )
    if _qualification_budget_binding(shared_budget) != frozen:
        raise ValueError("qualification shared parent budget ancestry changed")
    return shared_budget


def _verify_qualification_native_budget(
    store: LocalDataSnapshotStore, run_id: str, budget: ModelBudget, profile: ModelProviderProfile
) -> None:
    parent_events = budget.journal.events(budget.owner_run_id)
    settlements = {
        event.payload.get("request_key"): event.payload
        for event in parent_events
        if event.event_type == "pi.budget.settled"
    }
    input_tokens = output_tokens = cost = responses = 0
    for event in RunJournal.authoritative(store).events(run_id):
        if event.event_type != "pi.response.received":
            continue
        raw = _object(store.artifacts.read_json(_string(event.payload, "artifact_hash")))
        turn = native_turn(raw, profile.model)
        invocation = event.event_id.rsplit(".pi.response.", 1)[0]
        key = f"{invocation}:{raw['number']}:{turn.attempts}"
        settlement = settlements.get(key)
        charge = profile.pricing.estimate_microusd(turn.usage)
        if (
            settlement is None
            or settlement.get("evidence_ref") != canonical_hash(raw)
            or settlement.get("estimated_cost_microusd") != charge
        ):
            raise ValueError("qualification native response lacks its exact parent settlement")
        responses += 1
        input_tokens += turn.usage.input_tokens
        output_tokens += turn.usage.output_tokens
        cost += charge
    records = [
        item.record
        for item in UsageLedger(store.index_path).records()
        if item.record.run_id == run_id
    ]
    if len(records) != 1 or not responses:
        raise ValueError("qualification has no exact native Usage record")
    metrics = records[0].metrics
    if (
        metrics.input_tokens,
        metrics.output_tokens,
        metrics.turns,
        metrics.estimated_cost_microusd,
    ) != (input_tokens, output_tokens, responses, cost):
        raise ValueError("qualification Usage differs from settled native responses")


def _qualification_batch_summary(
    budget: ModelBudget, run_ids: tuple[str, ...], *, request_keys: frozenset[str] | None = None
) -> dict[str, int]:
    budget.summary()  # Revalidate the whole parent without erasing historical unknowns.
    reserved: dict[str, int] = {}
    settled: dict[str, int] = {}
    for event in budget.journal.events(budget.owner_run_id):
        key = event.payload.get("request_key")
        if not isinstance(key, str) or (
            key not in request_keys
            if request_keys is not None
            else not any(key.startswith(f"{run_id}.pi-invocation.") for run_id in run_ids)
        ):
            continue
        if event.event_type == "pi.budget.reserved":
            if event.payload.get("scope") != "route_qualification":
                raise ValueError("qualification request was charged to another stage")
            reserved[key] = cast(int, event.payload["reserved_microusd"])
        elif event.event_type == "pi.budget.settled":
            settled[key] = cast(int, event.payload["estimated_cost_microusd"])
    if not settled.keys() <= reserved.keys():
        raise ValueError("qualification settlement lacks its exact reservation")
    return {
        "physical_requests": len(reserved),
        "known_cost_microusd": sum(settled.values()),
        "reserved_microusd": sum(value for key, value in reserved.items() if key not in settled),
        "unsettled_requests": len(reserved.keys() - settled.keys()),
    }


def _qualification_panel(
    panel: tuple[AnalysisTopology, ...], profiles: tuple[ModelProviderProfile, ...]
) -> tuple[ModelProviderProfile, ...]:
    if (
        len(panel) not in {2, 3}
        or len(set(panel)) != len(panel)
        or any(topology not in _TOPOLOGIES for topology in panel)
    ):
        raise ValueError("qualification panel requires two or three distinct known routes")
    by_route = {(profile.model, profile.reasoning_effort): profile for profile in profiles}
    if (
        len(profiles) != len(panel)
        or len(by_route) != len(panel)
        or set(by_route) != {_PROFILE_EXPECTATIONS[topology] for topology in panel}
    ):
        raise ValueError("qualification profiles must match the exact registered route panel")
    ordered = tuple(by_route[_PROFILE_EXPECTATIONS[topology]] for topology in panel)
    if any(
        profile.context_window_tokens != 272_000
        or profile.effective_compaction_trigger_tokens != 258_000
        for profile in ordered
    ):
        raise ValueError("GPT-5.6 study Profiles require 272k context and 258k compaction")
    return ordered


def _registered_qualification_panel(
    registration: dict[str, object],
) -> tuple[AnalysisTopology, ...]:
    if registration.get("schema_version") == "market-impact.dynamic-route-qualification.v1":
        if any(
            key in registration
            for key in ("route_panel", "research_inputs_schema_version", "prior_qualification")
        ):
            raise ValueError("legacy qualification cannot acquire a new route panel")
        return _TOPOLOGIES
    if registration.get("schema_version") != "market-impact.dynamic-route-qualification.v2":
        raise ValueError("unknown qualification registration version")
    raw = registration.get("route_panel")
    if not isinstance(raw, list) or any(
        not isinstance(item, str) for item in cast(list[object], raw)
    ):
        raise ValueError("invalid qualification route panel")
    panel = tuple(AnalysisTopology(item) for item in cast(list[str], raw))
    profiles = _object(registration["profiles"])
    if set(profiles) != {topology.value for topology in panel}:
        raise ValueError("qualification profiles differ from registered route panel")
    ordered = _qualification_panel(
        panel, tuple(model_provider_profile_from_dict(_object(item)) for item in profiles.values())
    )
    if any(
        profiles[topology.value] != profile.to_dict()
        for topology, profile in zip(panel, ordered, strict=True)
    ):
        raise ValueError("qualification profile is bound to the wrong route panel member")
    if (
        registration.get("maximum_physical_requests") != 12
        or registration.get("maximum_cost_microusd") != 1_000_000
        or registration.get("maximum_output_tokens_per_run") != 4096
        or registration.get("research_inputs_schema_version")
        != "market-impact.research-thesis-inputs.v2"
    ):
        raise ValueError("qualification panel limits or research contract changed")
    return panel


def _qualification_group_members(
    panel: tuple[AnalysisTopology, ...],
) -> tuple[ModelBudgetGroupMember, ...]:
    return tuple(
        ModelBudgetGroupMember(topology.value, 12 // len(panel), 1_000_000 // len(panel))
        for topology in panel
    )


def _qualification_recovery_reference(
    prior_root: Path,
    *,
    profiles: dict[str, object],
    panel: tuple[AnalysisTopology, ...],
    shared_binding: object,
    legacy: bool = False,
) -> dict[str, object]:
    prior = load_dynamic_route_qualification(prior_root)
    if legacy and "prior_qualification" in prior:
        raise ValueError("legacy qualification recovery must reference the original attempt")
    if (
        "route_panel" not in prior
        or _registered_qualification_panel(prior) != panel
        or prior["profiles"] != profiles
        or shared_binding is None
        or prior.get("shared_budget") != shared_binding
    ):
        raise ValueError(
            "qualification recovery requires the same panel, profiles and shared parent"
        )
    report = _verified_qualification_report(prior_root, require_passed=False)
    if report.get("stage_passed") is not False or report.get("reconciled") is not True:
        raise ValueError("qualification recovery requires a failed reconciled predecessor")
    owner = f"dynamic-route-qualification-{prior['registration_hash']}"
    store = LocalDataSnapshotStore(prior_root / "authority")
    journal = RunJournal.authoritative(store)
    record = journal.get_run(owner)
    if (
        record.status is not RunStatus.FAILED
        or record.config_hash != prior["registration_hash"]
        or store.artifacts.read_json(canonical_hash(report)) != report
    ):
        raise ValueError("qualification predecessor lacks its authoritative failed result")
    raw_cases = report.get("cases")
    if not isinstance(raw_cases, list) or not raw_cases:
        raise ValueError("qualification predecessor has no route terminals")
    cases = tuple(_object(item) for item in cast(list[object], raw_cases))
    if (
        len(cases) > len(panel)
        or [item.get("topology") for item in cases] != [item.value for item in panel[: len(cases)]]
        or any(item.get("status") != "completed" for item in cases[:-1])
        or cases[-1].get("status") == "completed"
    ):
        raise ValueError("qualification predecessor has an invalid failed route panel")
    budget = _qualification_budget(prior)
    assert budget is not None
    run_ids: list[str] = []
    for item in cases:
        topology = _string(item, "topology")
        run_id = f"{owner}.{topology}"
        case_store = LocalDataSnapshotStore(prior_root / "cases" / topology)
        case_journal = RunJournal.authoritative(case_store)
        case_record = case_journal.get_run(run_id)
        if (
            item.get("run_id") != run_id
            or case_record.status
            is not (RunStatus.COMPLETED if item.get("status") == "completed" else RunStatus.FAILED)
            or _object(case_store.artifacts.read_json(_string(item, "terminal_hash"))).get("status")
            != item.get("status")
            or item.get("terminal_hash") != case_record.terminal_artifact_id
            or item.get("journal_hash") != case_journal.journal_hash(run_id)
            or item.get("usage_ledger_hash") != UsageLedger(case_store.index_path).ledger_hash
        ):
            raise ValueError("qualification predecessor route terminal changed")
        _verify_qualification_native_budget(
            case_store,
            run_id,
            budget,
            model_provider_profile_from_dict(_object(profiles[topology])),
        )
        run_ids.append(run_id)
    summary = _qualification_batch_summary(budget, tuple(run_ids))
    if report.get("budget") != summary or summary["unsettled_requests"] != 0:
        raise ValueError("qualification predecessor has unknown or changed spend")
    group_id, group_hash = _qualification_group_identity(prior)
    group = budget.journal.event(f"{budget.owner_run_id}.budget.group.{canonical_hash(group_id)}")
    if group is None or group.payload != {
        "binding": budget.binding,
        "group_id": group_id,
        "members": [member.to_dict() for member in _qualification_group_members(panel)],
        "call_graph_hash": group_hash,
        "scope": budget.scope,
    }:
        raise ValueError("qualification predecessor group changed")
    return {
        "root": str(prior_root.resolve()),
        "registration_hash": prior["registration_hash"],
        "report_hash": report["report_hash"],
        "group_id": group_id,
        **(
            {
                "schema_version": "market-impact.qualification-recovery-reference.v2",
                "group_registration_hash": group_hash,
            }
            if not legacy
            else {}
        ),
    }


def _qualification_group_identity(registration: dict[str, object]) -> tuple[str, str]:
    prior = registration.get("prior_qualification")
    if prior is not None:
        reference = _object(prior)
        return _string(reference, "group_id"), _string(
            reference,
            "group_registration_hash" if "schema_version" in reference else "registration_hash",
        )
    registration_hash = _string(registration, "registration_hash")
    return f"dynamic-route-qualification-{registration_hash}", registration_hash


def _qualification_recovery_claim(
    root: Path,
    registration: dict[str, object],
    budget: ModelBudget | None,
    *,
    create: bool,
) -> None:
    if "prior_qualification" not in registration:
        return
    if budget is None:
        raise ValueError("qualification recovery requires its original shared parent")
    group_id, _ = _qualification_group_identity(registration)
    reference = _object(registration["prior_qualification"])
    predecessor_owner = f"dynamic-route-qualification-{reference['registration_hash']}"
    event_id = f"{budget.owner_run_id}.qualification.recovery.{canonical_hash(predecessor_owner)}"
    payload = {
        "group_id": group_id,
        "registration_hash": registration["registration_hash"],
        "root": str(root.resolve()),
    }
    claim = budget.journal.try_claim_run(event_id) if create else None
    if create and claim is None:
        raise ValueError("qualification recovery claim is busy; retry the same registration")
    try:
        previous = budget.journal.event(event_id)
        if previous is not None:
            if previous.payload != payload:
                raise ValueError(
                    "qualification predecessor already claimed its single recovery attempt"
                )
        elif create:
            budget.journal.append(
                run_id=budget.owner_run_id,
                event_id=event_id,
                event_type="qualification.recovery.claimed",
                observed_at=datetime.now(UTC),
                payload=payload,
            )
        else:
            raise ValueError("qualification recovery lacks its parent claim")
    finally:
        if claim is not None:
            claim.release()


def prepare_dynamic_route_qualification(
    root: Path,
    *,
    profiles: tuple[ModelProviderProfile, ...],
    verification_path: Path,
    route_panel: tuple[AnalysisTopology, ...] | None = None,
    registered_at: datetime | None = None,
    shared_budget: ModelBudget | None = None,
    prior_qualification_root: Path | None = None,
) -> dict[str, object]:
    """Freeze the only paid route-qualification batch for the current build."""

    panel = _TOPOLOGIES if route_panel is None else route_panel
    ordered = _qualification_panel(panel, profiles)
    frozen_profiles = {
        topology.value: profile.to_dict() for topology, profile in zip(panel, ordered, strict=True)
    }
    recovery = None
    if prior_qualification_root is not None:
        if (
            route_panel is None
            or shared_budget is None
            or root.resolve() == prior_qualification_root.resolve()
        ):
            raise ValueError(
                "qualification recovery requires a fresh root, explicit panel and shared parent"
            )
        recovery = _qualification_recovery_reference(
            prior_qualification_root,
            profiles=cast(dict[str, object], frozen_profiles),
            panel=panel,
            shared_binding=_qualification_budget_binding(shared_budget),
        )
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(root, 0o700)
    path = root / "qualification-registration.json"
    if path.exists():
        existing = load_dynamic_route_qualification(root)
        _qualification_budget(existing, shared_budget=shared_budget)
        if recovery is not None and "schema_version" not in _object(
            existing.get("prior_qualification", {})
        ):
            recovery = {
                key: item
                for key, item in recovery.items()
                if key not in {"schema_version", "group_registration_hash"}
            }
        if (
            _registered_qualification_panel(existing) != panel
            or existing["profiles"] != frozen_profiles
            or (route_panel is not None) != ("route_panel" in existing)
            or existing.get("prior_qualification") != recovery
        ):
            raise ValueError("qualification registration cannot change its panel or profiles")
        _qualification_recovery_claim(root, existing, shared_budget, create=True)
        return existing
    verification = _verified_build(verification_path)
    value: dict[str, object] = {
        "schema_version": "market-impact.dynamic-route-qualification.v1",
        "experiment": "dynamic-horizon-three-model-route-qualification-v1",
        "registered_at": _timestamp(registered_at or datetime.now(UTC)),
        "runtime": runtime_identity(),
        "profiles": frozen_profiles,
        "verification_hash": canonical_hash(verification),
        "maximum_cost_microusd": 1_000_000,
        "maximum_physical_requests": 12,
        "maximum_output_tokens_per_run": 4096,
        "execution_capability": False,
        "live_execution": False,
    }
    if route_panel is not None:
        value.update(
            {
                "schema_version": "market-impact.dynamic-route-qualification.v2",
                "experiment": "dynamic-horizon-route-panel-qualification-v2",
                "route_panel": [topology.value for topology in panel],
                "research_inputs_schema_version": "market-impact.research-thesis-inputs.v2",
            }
        )
    if shared_budget is not None:
        value["shared_budget"] = _qualification_budget_binding(shared_budget)
    if recovery is not None:
        value["prior_qualification"] = recovery
    value["registration_hash"] = canonical_hash(value)
    _write_new(path, value)
    _qualification_recovery_claim(root, value, shared_budget, create=True)
    return value


def load_dynamic_route_qualification(root: Path) -> dict[str, object]:
    value = _read_object(root / "qualification-registration.json")
    core = {key: item for key, item in value.items() if key != "registration_hash"}
    if (
        canonical_hash(core) != value.get("registration_hash")
        or value.get("runtime") != runtime_identity()
        or value.get("execution_capability") is not False
        or value.get("live_execution") is not False
    ):
        raise ValueError("dynamic route qualification changed or belongs to another build")
    panel = _registered_qualification_panel(value)
    if "prior_qualification" in value:
        reference = _object(value["prior_qualification"])
        version = reference.get("schema_version")
        if version not in {None, "market-impact.qualification-recovery-reference.v2"}:
            raise ValueError("unknown qualification recovery reference version")
        if reference != _qualification_recovery_reference(
            Path(_string(reference, "root")),
            profiles=_object(value["profiles"]),
            panel=panel,
            shared_binding=value.get("shared_budget"),
            legacy=version is None,
        ):
            raise ValueError("qualification recovery ancestry changed")
    return value


async def run_dynamic_route_qualification(
    root: Path, *, shared_budget: ModelBudget | None = None
) -> dict[str, object]:
    """Qualify model identity, effort route and one real native terminal per Profile."""

    report_path = root / "qualification-report.json"
    registration = load_dynamic_route_qualification(root)
    panel = _registered_qualification_panel(registration)
    registered_budget = _qualification_budget(registration, shared_budget=shared_budget)
    if report_path.exists():
        return _verified_qualification_report(root, require_passed=False)
    if registration.get("shared_budget") is not None and shared_budget is None:
        raise ValueError("shared qualification requires its registered writable parent budget")
    _qualification_recovery_claim(root, registration, registered_budget, create=True)
    store = LocalDataSnapshotStore(root / "authority")
    journal = RunJournal.authoritative(store)
    owner = f"dynamic-route-qualification-{registration['registration_hash']}"
    registered_at = _datetime(_string(registration, "registered_at"))
    journal.start_run(
        run_id=owner,
        config_hash=cast(str, registration["registration_hash"]),
        created_at=registered_at,
    )
    profiles = cast(dict[str, dict[str, object]], registration["profiles"])
    run_ids = tuple(f"{owner}.{topology.value}" for topology in panel)
    permit = PiRuntimePermit(
        canonical_hash(registration["runtime"]),
        tuple(
            model_provider_profile_from_dict(profiles[topology.value]).route_identity
            for topology in panel
        ),
        cast(str, registration["registration_hash"]),
        run_ids,
        owner if registered_budget is None else registered_budget.owner_run_id,
    )
    budget = registered_budget or ModelBudget(
        journal=journal,
        owner_run_id=owner,
        max_requests=cast(int, registration["maximum_physical_requests"]),
        max_cost_microusd=cast(int, registration["maximum_cost_microusd"]),
        scope_limits=(ModelBudgetScope("route_qualification", 1_000_000),)
        if "route_panel" in registration
        else (),
        scope="route_qualification" if "route_panel" in registration else None,
    )
    group_id, group_hash = _qualification_group_identity(registration)
    grouped_budget = (
        await budget.admit_group(
            group_id=group_id,
            members=_qualification_group_members(panel),
            call_graph_hash=group_hash,
        )
        if "route_panel" in registration
        else None
    )
    results: list[dict[str, object]] = []
    for topology, run_id in zip(panel, run_ids, strict=True):
        profile = model_provider_profile_from_dict(profiles[topology.value])
        case_store = LocalDataSnapshotStore(root / "cases" / topology.value)
        provider = PiRuntimeProvider(
            profile,
            budget=budget
            if grouped_budget is None
            else grouped_budget.for_group_member(topology.value),
            permit=permit,
        )
        authority = ResearchThesisAuthority(
            case_store,
            experiment_id=cast(str, registration["registration_hash"]),
            arm_id=topology.value,
        )
        try:
            terminal = await authority.analyze(
                run_id=run_id,
                provider=provider,
                inputs=ResearchThesisRunInputs(
                    repository=_qualification_repository(registered_at),
                    target_id="SYNTHETIC.BROAD.ETF",
                    thesis_epoch="route-qualification-v1",
                    allowed_horizons=frozenset({1, 3, 5}),
                    schema_version=cast(
                        str,
                        registration.get(
                            "research_inputs_schema_version",
                            "market-impact.research-thesis-inputs.v2",
                        ),
                    ),
                    research_question=(
                        "What is the defensible direction and horizon for the synthetic proxy?"
                    ),
                ),
                max_output_tokens=cast(int, registration["maximum_output_tokens_per_run"]),
            )
        finally:
            await provider.close()
        results.append(
            {
                "topology": topology.value,
                "run_id": run_id,
                "status": terminal["status"],
                "terminal_hash": authority.journal.get_run(run_id).terminal_artifact_id,
                "journal_hash": authority.journal.journal_hash(run_id),
                "usage_ledger_hash": UsageLedger(case_store.index_path).ledger_hash,
            }
        )
        if terminal["status"] != "completed":
            break
    summary = (
        budget.summary()
        if registered_budget is None
        else _qualification_batch_summary(budget, run_ids)
    )
    stage_passed = len(results) == len(panel) and all(
        item["status"] == "completed" for item in results
    )
    terminal_status = RunStatus.COMPLETED if stage_passed else RunStatus.FAILED
    report: dict[str, object] = {
        "schema_version": "market-impact.dynamic-route-qualification-report.v1",
        "registration_hash": registration["registration_hash"],
        "runtime": registration["runtime"],
        "cases": results,
        "budget": summary,
        "stage_passed": stage_passed,
        "reconciled": summary["unsettled_requests"] == 0,
        "execution_capability": False,
        "live_execution": False,
    }
    report["report_hash"] = canonical_hash(report)
    artifact = store.artifacts.put_json(report)
    journal.finish(
        run_id=owner,
        status=terminal_status,
        finished_at=datetime.now(UTC),
        terminal_artifact_id=artifact.content_hash if stage_passed else None,
    )
    _write_new(report_path, report)
    return report


def accept_dynamic_route_qualification(root: Path) -> dict[str, object]:
    """Install only a completed, replay-verified registered route qualification."""

    from market_impact_agent.pi_deployment import install_runtime_acceptance

    registration = load_dynamic_route_qualification(root)
    report = _verified_qualification_report(root, require_passed=True)
    if report.get("stage_passed") is not True or report.get("reconciled") is not True:
        raise ValueError("dynamic model routes did not pass qualification")
    return install_runtime_acceptance(registration=registration, report=report)


async def run_opened_analysis_ablation(
    root: Path,
    *,
    inputs_root: Path,
    pattern_pack_path: Path | tuple[Path, ...],
) -> dict[str, object]:
    """Run 8x3 forecasts, registered repeats and conditional non-voting Judges."""

    report_path = root / "opened-analysis-report.json"
    if report_path.exists():
        return _verified_analysis_report(root)
    registration = load_dynamic_effectiveness_study(root)
    pattern_pack_paths = _pattern_pack_candidates(pattern_pack_path)
    study = _object(registration["study"])
    profiles = cast(dict[str, dict[str, object]], registration["profiles"])
    sources = {
        _string(item, "case_id"): item
        for item in cast(list[dict[str, object]], registration["opened_case_sources"])
    }
    authority_store = LocalDataSnapshotStore(root / "analysis-authority")
    owner_journal = RunJournal.authoritative(authority_store)
    owner = f"dynamic-opened-analysis-{registration['registration_hash']}"
    owner_journal.start_run(
        run_id=owner,
        config_hash=cast(str, registration["registration_hash"]),
        created_at=_datetime(_string(study, "registered_at")),
    )
    budget = ModelBudget(owner_journal, owner, 64, 7_000_000)
    study_runs_store = LocalDataSnapshotStore(root / "analysis-runs")
    providers = {
        topology: PiRuntimeProvider(
            model_provider_profile_from_dict(profiles[topology.value]),
            budget=budget,
        )
        for topology in _TOPOLOGIES
    }
    arm_failures = {topology: 0 for topology in _TOPOLOGIES}
    paused: set[AnalysisTopology] = set()
    results: list[dict[str, object]] = []
    experiment_id = _string(study, "experiment_id")

    async def analyze_one(
        topology: AnalysisTopology,
        case_id: str,
        *,
        repetition: str,
        date_presentation: DatePresentation = DatePresentation.TRUE_DATE,
        candidates: tuple[object, ...] = (),
    ) -> dict[str, object]:
        if topology in paused:
            return {
                "case_id": case_id,
                "topology": topology.value,
                "repetition": repetition,
                "status": "not_run_provider_paused",
            }
        source = sources[case_id]
        repository = _repository_for_source(inputs_root, pattern_pack_paths, source)
        suffix = f"{case_id}.{topology.value}.{repetition}.{date_presentation.value}"
        run_id = f"{owner}.{suffix}"
        authority = ResearchThesisAuthority(
            study_runs_store,
            experiment_id=experiment_id,
            arm_id=topology.value,
        )
        typed_candidates = cast(tuple[ResearchThesisV1, ...], candidates)
        slots = ExperimentSlots(shared_admission_root(), experiment_id, 6)
        await slots.acquire()
        try:
            terminal = await authority.analyze(
                run_id=run_id,
                provider=providers[topology],
                inputs=ResearchThesisRunInputs(
                    repository=repository,
                    target_id=_string(source, "target_id"),
                    thesis_epoch=f"dynamic-thesis-{repetition}-v1",
                    allowed_horizons=frozenset({1, 3, 5, 10, 20, 60}),
                    date_presentation=date_presentation,
                    candidate_theses=typed_candidates,
                    research_question=_string(source, "research_question"),
                ),
                max_output_tokens=4096,
            )
        finally:
            slots.release()
        provider_failure = _terminal_has_provider_failure(authority.journal, run_id)
        if terminal["status"] == "completed":
            arm_failures[topology] = 0
        elif provider_failure:
            arm_failures[topology] += 1
            if arm_failures[topology] >= 2:
                paused.add(topology)
        return {
            "case_id": case_id,
            "topology": topology.value,
            "repetition": repetition,
            "date_presentation": date_presentation.value,
            "run_id": run_id,
            "status": terminal["status"],
            "provider_failure": provider_failure,
            "terminal_hash": authority.journal.get_run(run_id).terminal_artifact_id,
            "journal_hash": authority.journal.journal_hash(run_id),
            "thesis": terminal.get("thesis"),
        }

    try:
        for source in cast(list[dict[str, object]], registration["opened_case_sources"]):
            case_id = _string(source, "case_id")
            rows = await asyncio.gather(
                *(analyze_one(topology, case_id, repetition="base") for topology in _TOPOLOGIES)
            )
            results.extend(rows)
            by_topology = {cast(str, row["topology"]): row for row in rows}
            luna = _completed_thesis(study_runs_store, by_topology[AnalysisTopology.LUNA_MAX.value])
            terra = _completed_thesis(
                study_runs_store, by_topology[AnalysisTopology.TERRA_HIGH.value]
            )
            if luna is not None and terra is not None and theses_semantically_disagree(luna, terra):
                results.append(
                    await analyze_one(
                        AnalysisTopology.SOL_HIGH,
                        case_id,
                        repetition="conditional-judge",
                        candidates=(luna, terra),
                    )
                )

        for case_id in cast(list[str], study["stability_case_ids"]):
            results.extend(
                await asyncio.gather(
                    *(
                        analyze_one(topology, case_id, repetition="stability-repeat")
                        for topology in _TOPOLOGIES
                    )
                )
            )
        memory_pair = cast(list[dict[str, object]], study["memory_sensitivity_pairs"])[0]
        results.append(
            await analyze_one(
                AnalysisTopology.LUNA_MAX,
                _string(memory_pair, "case_id"),
                repetition="memory-sensitivity",
                date_presentation=DatePresentation.RELATIVE_OFFSET,
            )
        )
    finally:
        await asyncio.gather(*(provider.close() for provider in providers.values()))

    summary = budget.summary()
    report: dict[str, object] = {
        "schema_version": "market-impact.dynamic-opened-analysis-report.v1",
        "registration_hash": registration["registration_hash"],
        "runtime": registration["runtime"],
        "results": results,
        "paused_model_arms": sorted(item.value for item in paused),
        "budget": summary,
        "outcomes_opened": False,
        "promotion": False,
        "live_execution": False,
        "completed_at": _timestamp(datetime.now(UTC)),
    }
    report["report_hash"] = canonical_hash(report)
    artifact = authority_store.artifacts.put_json(report)
    owner_journal.finish(
        run_id=owner,
        status=RunStatus.COMPLETED,
        finished_at=datetime.now(UTC),
        terminal_artifact_id=artifact.content_hash,
    )
    _write_new(report_path, report)
    return report


async def run_portfolio_ablation(root: Path) -> dict[str, object]:
    """Test one frozen viewpoint against four account states on all model routes."""

    report_path = root / "portfolio-ablation-report.json"
    if report_path.exists():
        return _verified_portfolio_report(root)
    registration = load_dynamic_effectiveness_study(root)
    analysis = _verified_analysis_report(root)
    selected = _select_portfolio_theses(registration, analysis)
    profiles = cast(dict[str, dict[str, object]], registration["profiles"])
    portfolio_owner_store = LocalDataSnapshotStore(root / "portfolio-authority")
    owner_journal = RunJournal.authoritative(portfolio_owner_store)
    owner = f"dynamic-portfolio-ablation-{registration['registration_hash']}"
    started_at = datetime.now(UTC)
    owner_journal.start_run(
        run_id=owner,
        config_hash=cast(str, registration["registration_hash"]),
        created_at=started_at,
    )
    budget = ModelBudget(owner_journal, owner, 24, 2_500_000)
    experiment_id = _string(_object(registration["study"]), "experiment_id")
    run_store = LocalDataSnapshotStore(root / "analysis-runs")
    providers = {
        topology: PiRuntimeProvider(
            model_provider_profile_from_dict(profiles[topology.value]),
            budget=budget,
        )
        for topology in _TOPOLOGIES
    }
    failures = {topology: 0 for topology in _TOPOLOGIES}
    paused: set[AnalysisTopology] = set()
    results: list[dict[str, object]] = []

    async def run_one(
        topology: AnalysisTopology,
        scenario: dict[str, object],
        thesis_run_id: str,
        inputs: PortfolioReviewInputs,
        exposure_authority: RegisteredPortfolioExposureViewAuthorityV2,
        at: datetime,
    ) -> dict[str, object]:
        if topology in paused:
            return {
                "scenario_id": scenario["scenario_id"],
                "topology": topology.value,
                "status": "not_run_provider_paused",
            }
        scenario_id = _string(scenario, "scenario_id")
        authority = PortfolioReviewAuthority(
            run_store,
            input_source=lambda: inputs,
            exposure_authority=exposure_authority,
            clock=lambda: at,
        )
        run_id = f"{owner}.{scenario_id}.{topology.value}"
        slots = ExperimentSlots(shared_admission_root(), experiment_id, 6)
        await slots.acquire()
        try:
            terminal = await authority.review(
                run_id=run_id,
                provider=providers[topology],
                research_run_ids=(),
                research_thesis_run_ids=(thesis_run_id,),
                max_output_tokens=4096,
            )
        finally:
            slots.release()
        provider_failure = _run_has_provider_failure(
            authority.journal,
            run_id,
            event_type="portfolio.model.attempt",
        )
        if terminal["status"] == "completed":
            failures[topology] = 0
        elif provider_failure:
            failures[topology] += 1
            if failures[topology] >= 2:
                paused.add(topology)
        proposal = terminal.get("proposal")
        action = (
            cast(dict[str, object], proposal).get("requested_action")
            if isinstance(proposal, dict)
            else None
        )
        expected = cast(list[str], scenario["expected_actions"])
        return {
            "scenario_id": scenario_id,
            "topology": topology.value,
            "run_id": run_id,
            "research_thesis_run_id": thesis_run_id,
            "status": terminal["status"],
            "provider_failure": provider_failure,
            "requested_action": action,
            "within_preregistered_reasonable_actions": action in expected,
            "terminal_hash": authority.journal.get_run(run_id).terminal_artifact_id,
            "journal_hash": authority.journal.journal_hash(run_id),
        }

    try:
        for raw_scenario in cast(list[dict[str, object]], registration["portfolio_scenarios"]):
            direction = _string(raw_scenario, "required_direction")
            thesis = selected.get(direction)
            if thesis is None:
                results.append(
                    {
                        "scenario_id": raw_scenario["scenario_id"],
                        "status": "not_run_missing_registered_thesis_direction",
                        "required_direction": direction,
                    }
                )
                continue
            at = datetime.now(UTC)
            inputs, exposure_authority = _portfolio_scenario_inputs(
                root,
                scenario=raw_scenario,
                target_id=_string(thesis, "target_id"),
                observed_at=at,
                harness_authority_id=run_store.harness_authority_id,
            )
            results.extend(
                await asyncio.gather(
                    *(
                        run_one(
                            topology,
                            raw_scenario,
                            _string(thesis, "run_id"),
                            inputs,
                            exposure_authority,
                            at,
                        )
                        for topology in _TOPOLOGIES
                    )
                )
            )
    finally:
        await asyncio.gather(*(provider.close() for provider in providers.values()))

    summary = budget.summary()
    report: dict[str, object] = {
        "schema_version": "market-impact.dynamic-portfolio-ablation-report.v1",
        "registration_hash": registration["registration_hash"],
        "runtime": registration["runtime"],
        "selected_theses": selected,
        "results": results,
        "account_actions_complete": portfolio_actions_complete(results),
        "same_bullish_thesis_reused": _same_bullish_thesis_reused(results),
        "same_viewpoint_account_differentiation": _account_differentiation(results),
        "paused_model_arms": sorted(item.value for item in paused),
        "budget": summary,
        "mock_execution": False,
        "promotion": False,
        "live_execution": False,
        "completed_at": _timestamp(datetime.now(UTC)),
    }
    report["report_hash"] = canonical_hash(report)
    artifact = portfolio_owner_store.artifacts.put_json(report)
    owner_journal.finish(
        run_id=owner,
        status=RunStatus.COMPLETED,
        finished_at=datetime.now(UTC),
        terminal_artifact_id=artifact.content_hash,
    )
    _write_new(report_path, report)
    return report


def _select_portfolio_theses(
    registration: dict[str, object],
    analysis: dict[str, object],
) -> dict[str, dict[str, object]]:
    selected: dict[str, dict[str, object]] = {}
    targets = {
        _string(item, "case_id"): _string(item, "target_id")
        for item in cast(list[dict[str, object]], registration["opened_case_sources"])
    }
    for row in cast(list[dict[str, object]], analysis["results"]):
        if row.get("repetition") != "base" or row.get("status") != "completed":
            continue
        thesis = row.get("thesis")
        if not isinstance(thesis, dict):
            continue
        typed_thesis = cast(dict[str, object], thesis)
        direction = typed_thesis.get("base_case_direction")
        if isinstance(direction, str) and direction not in selected:
            selected[direction] = {
                "run_id": row["run_id"],
                "case_id": row["case_id"],
                "topology": row["topology"],
                "target_id": targets[cast(str, row["case_id"])],
                "thesis": typed_thesis,
            }
    return selected


def _portfolio_scenario_inputs(
    root: Path,
    *,
    scenario: dict[str, object],
    target_id: str,
    observed_at: datetime,
    harness_authority_id: str,
) -> tuple[PortfolioReviewInputs, RegisteredPortfolioExposureViewAuthorityV2]:
    scenario_id = _string(scenario, "scenario_id")
    price = PriceBasis(
        instrument_id=target_id,
        currency="USD",
        unit="per_share",
        basis_kind="raw_reference_quote",
        price=Decimal("100"),
        source_id="dynamic-effectiveness-synthetic-price",
        source_version="1",
        observed_at=observed_at - timedelta(minutes=1),
        valid_until=observed_at + timedelta(minutes=10),
    )
    quantity = Decimal(_string(scenario, "opening_quantity"))
    positions = (
        ()
        if quantity == 0
        else (
            AccountPosition(
                target_id,
                "ARCX",
                "exchange_traded_fund",
                Side.BUY,
                quantity,
                Decimal("0.8") if scenario_id == "bullish-overconcentrated" else Decimal("0.5"),
                None,
            ),
        )
    )
    provider = MockExecutionProvider(
        root / "portfolio-scenarios" / scenario_id / "mock-account.sqlite3",
        clock=lambda: observed_at,
    )
    provider.configure_simulated_account(
        seed=f"dynamic-effectiveness-{scenario_id}",
        cash=(
            CashBalance(
                "USD",
                Decimal(_string(scenario, "opening_cash")),
                Decimal(_string(scenario, "opening_cash")),
            ),
        ),
        positions=positions,
        instruments={target_id: ("ARCX", "exchange_traded_fund")},
        opened_at=observed_at - timedelta(minutes=2),
    )
    account = provider.simulated_account_snapshot(price_bases={target_id: price})
    position = account.project_positions(
        evaluated_at=observed_at,
        max_age=timedelta(minutes=5),
    )
    view = AuthorizedDecisionView.build(
        cutoff=observed_at,
        frozen_at=observed_at,
        data_snapshot_ids=(),
        decision_input_ids=("dynamic-effectiveness-portfolio-scenario",),
        position_snapshot=position,
    )
    marks = tuple(
        RawMarkedPositionV2(
            item.target_id,
            item.venue,
            item.instrument_class,
            item.side,
            item.quantity,
            price.price,
            canonical_hash(price.to_dict()),
        )
        for item in account.positions or ()
    )
    exposure = PortfolioExposureViewV2.build(
        authorized_view=view,
        position_snapshot=position,
        raw_mark_set_hash=canonical_hash([item.to_dict() for item in marks]),
        execution_ledger_snapshot_hash=canonical_hash("no-study-executions"),
        reconciliation_ledger_snapshot_hash=canonical_hash(account.to_dict()),
        currency="USD",
        marked_positions=marks,
        daily_turnover_used=Decimal(0),
        daily_submissions_used=0,
        active_kill_reasons=(),
        observed_at=observed_at,
        valid_until=observed_at + timedelta(minutes=5),
    )
    exposure_authority = RegisteredPortfolioExposureViewAuthorityV2(
        {exposure.exposure_view_id: exposure}
    )
    mandate = TradingMandateV2(
        mandate_id=f"dynamic-effectiveness-{scenario_id}",
        account_id=account.account_reference_hash,
        harness_authority_id=harness_authority_id,
        environment=TradingEnvironment.PAPER,
        approval_mode=ApprovalMode.MANUAL_EACH,
        valid_from=observed_at - timedelta(minutes=1),
        valid_until=observed_at + timedelta(hours=8),
        allowed_instruments=frozenset({target_id}),
        allowed_instrument_classes=frozenset({"unlevered_exchange_traded_fund"}),
        allowed_sides=frozenset({Side.BUY, Side.SELL}),
        currency="USD",
        gross_exposure_limit=Decimal("10000"),
        minimum_net_exposure=Decimal("-10000"),
        maximum_net_exposure=Decimal("10000"),
        maximum_position_count=10,
        maximum_single_position_fraction=Decimal(1),
        daily_turnover_limit=Decimal("50000"),
        daily_submission_limit=50,
        daily_loss_kill_threshold=Decimal("300"),
        strategy_peak_drawdown_kill_threshold=Decimal("1000"),
    )
    rules = ExchangeInstrumentRuleSet(
        rule_set_id="exchange-instrument-rule-set-" + canonical_hash("dynamic-arcx-etf-v1"),
        effective_from=date(2026, 1, 1),
        source_documents=(
            {
                "venue": "ARCX",
                "issuer": "synthetic-study",
                "notice_id": "dynamic-arcx-etf-v1",
                "published_on": "2026-01-01",
                "effective_from": "2026-01-01",
                "source_ref": "synthetic://dynamic-effectiveness/rules",
                "rule_references": ["study-only-cash-etf"],
            },
        ),
        rules=(
            ExchangeInstrumentRule(
                "arcx-study-etf-v1",
                "ARCX",
                "exchange_traded_fund",
                1,
                0.01,
                "USD",
                "study_only_no_execution",
                (),
            ),
        ),
    )
    return (
        PortfolioReviewInputs(
            account,
            position,
            view,
            exposure,
            mandate,
            {target_id: price},
            rules,
            observed_at,
            observed_at + timedelta(minutes=5),
        ),
        exposure_authority,
    )


def _same_bullish_thesis_reused(results: list[dict[str, object]]) -> bool:
    runs = {
        cast(str, row["research_thesis_run_id"])
        for row in results
        if row.get("scenario_id") in {"bullish-cash", "bullish-overconcentrated"}
        and "research_thesis_run_id" in row
    }
    return len(runs) == 1


def _account_differentiation(results: list[dict[str, object]]) -> dict[str, bool]:
    result: dict[str, bool] = {}
    for topology in _TOPOLOGIES:
        actions = {
            cast(str, row["scenario_id"]): row.get("requested_action")
            for row in results
            if row.get("topology") == topology.value
            and row.get("scenario_id") in {"bullish-cash", "bullish-overconcentrated"}
            and row.get("status") == "completed"
        }
        result[topology.value] = (
            set(actions) == {"bullish-cash", "bullish-overconcentrated"}
            and actions["bullish-cash"] != actions["bullish-overconcentrated"]
        )
    return result


def _completed_thesis(store: LocalDataSnapshotStore, row: dict[str, object]):
    if row.get("status") != "completed":
        return None
    run_id = _string(row, "run_id")
    thesis, _ = reopen_completed_research_thesis(
        journal=RunJournal.authoritative(store),
        artifact_store=store.artifacts,
        run_id=run_id,
    )
    return thesis


def _opened_source(
    inputs_root: Path, pattern_packs: tuple[Path, ...], input_ref: str
) -> OpenedCaseSource:
    base = inputs_root / input_ref
    pack = _read_object(base / "evidence-pack.json")
    documents = _read_object(base / "evidence-documents.json")
    selected_paths = _select_pattern_pack_paths(pack, pattern_packs)
    if len(selected_paths) != 1:
        raise ValueError("opened dynamic-effectiveness cases require one Pattern Pack")
    pattern = _read_object(selected_paths[0])
    case_id = input_ref.rsplit("/", maxsplit=1)[-1]
    targets = pack.get("allowed_targets")
    if not isinstance(targets, list):
        raise ValueError("opened study case requires exactly one registered target")
    target_items = cast(list[object], targets)
    if len(target_items) != 1 or not isinstance(target_items[0], str):
        raise ValueError("opened study case requires exactly one registered target")
    frozen_hash = canonical_hash(
        {
            "evidence_pack": pack,
            "evidence_documents": documents,
            "pattern_pack": pattern,
            "research_question": _NEUTRAL_QUESTION,
            "allowed_horizons": [1, 3, 5, 10, 20, 60],
        }
    )
    return OpenedCaseSource(
        case_id=case_id,
        input_ref=input_ref,
        target_id=target_items[0],
        frozen_input_hash=frozen_hash,
        evidence_pack_hash=canonical_hash(pack),
        evidence_documents_hash=canonical_hash(documents),
        pattern_pack_hash=canonical_hash(pattern),
    )


def _repository_for_source(
    inputs_root: Path, pattern_pack_paths: tuple[Path, ...], source: dict[str, object]
) -> FrozenResearchRepository:
    base = inputs_root / _string(source, "input_ref")
    current = _opened_source(inputs_root, pattern_pack_paths, _string(source, "input_ref"))
    if current.to_dict() != source:
        raise ValueError("opened study input changed after registration")
    pack = _read_object(base / "evidence-pack.json")
    return FrozenResearchRepository.from_files(
        evidence_pack_path=base / "evidence-pack.json",
        evidence_documents_path=base / "evidence-documents.json",
        pattern_pack_paths=_select_pattern_pack_paths(pack, pattern_pack_paths),
    )


def _pattern_pack_candidates(value: Path | tuple[Path, ...]) -> tuple[Path, ...]:
    paths = (value,) if isinstance(value, Path) else value
    if not paths or len(set(paths)) != len(paths):
        raise ValueError("dynamic-effectiveness Pattern Pack paths must be unique")
    return paths


def _select_pattern_pack_paths(
    evidence_pack: dict[str, object], candidates: tuple[Path, ...]
) -> tuple[Path, ...]:
    references = evidence_pack.get("pattern_packs")
    if not isinstance(references, list):
        raise ValueError("opened study Evidence Pack has invalid Pattern Pack references")
    expected = {
        _string(_object(item), "pack_id"): _string(_object(item), "content_hash")
        for item in cast(list[object], references)
    }
    selected: dict[str, Path] = {}
    for path in candidates:
        payload = _read_object(path)
        pack_id = _string(payload, "pack_id")
        if pack_id not in expected:
            continue
        if pack_id in selected or canonical_hash(payload) != expected[pack_id]:
            raise ValueError("Pattern Pack candidate differs from its Evidence Pack reference")
        selected[pack_id] = path
    if set(selected) != set(expected):
        raise ValueError("required Pattern Pack was not supplied for an opened study case")
    return tuple(selected[pack_id] for pack_id in sorted(selected))


def _qualification_repository(at: datetime) -> FrozenResearchRepository:
    release = {
        "published_at": _timestamp(at - timedelta(minutes=10)),
        "fact": "Synthetic revenue was 112 against a frozen consensus of 100.",
    }
    market = {
        "as_of": _timestamp(at - timedelta(minutes=2)),
        "fact": "The synthetic proxy fell 3 percent over the five sessions before release.",
    }
    evidence = (
        EvidenceReference(
            "synthetic-release",
            "incremental-fact",
            "synthetic://dynamic-route/release",
            EvidenceTier.OFFICIAL,
            at - timedelta(minutes=10),
            canonical_hash(release),
            "A positive revenue surprise is frozen before the decision cutoff.",
        ),
        EvidenceReference(
            "synthetic-market",
            "priced-in-context",
            "synthetic://dynamic-route/market",
            EvidenceTier.REGULATED,
            at - timedelta(minutes=2),
            canonical_hash(market),
            "Pre-release price context is frozen before the cutoff.",
        ),
    )
    return FrozenResearchRepository(
        evidence_pack=EvidencePack.build(
            event_id="synthetic-dynamic-route",
            as_of=at,
            research_question="What is the defensible direction for the synthetic proxy?",
            evidence=evidence,
            pattern_packs=(),
            allowed_targets=("SYNTHETIC.BROAD.ETF",),
            data_gaps=("future management execution is unknown",),
        ),
        evidence_documents={"synthetic-release": release, "synthetic-market": market},
        pattern_packs={},
    )


def _ordered_profiles(
    profiles: tuple[ModelProviderProfile, ModelProviderProfile, ModelProviderProfile],
) -> tuple[ModelProviderProfile, ModelProviderProfile, ModelProviderProfile]:
    by_route = {(profile.model, profile.reasoning_effort): profile for profile in profiles}
    try:
        ordered = tuple(by_route[_PROFILE_EXPECTATIONS[topology]] for topology in _TOPOLOGIES)
    except KeyError:
        raise ValueError("profiles do not contain the three preregistered model routes") from None
    if len(by_route) != 3:
        raise ValueError("profiles must be unique and contain exactly three routes")
    if any(
        profile.context_window_tokens != 272_000
        or profile.effective_compaction_trigger_tokens != 258_000
        for profile in ordered
    ):
        raise ValueError("GPT-5.6 study Profiles require 272k context and 258k compaction")
    return cast(
        tuple[ModelProviderProfile, ModelProviderProfile, ModelProviderProfile],
        ordered,
    )


def _terminal_has_provider_failure(journal: RunJournal, run_id: str) -> bool:
    return _run_has_provider_failure(
        journal,
        run_id,
        event_type="research.thesis.model.attempt",
    )


def _run_has_provider_failure(
    journal: RunJournal,
    run_id: str,
    *,
    event_type: str,
) -> bool:
    attempts = [event for event in journal.events(run_id) if event.event_type == event_type]
    return bool(attempts and attempts[-1].payload.get("phase") == "failed")


def _verified_build(path: Path) -> dict[str, object]:
    value = _read_object(path)
    checks = _object(value.get("checks", {}))
    required = (
        "ruff",
        "format",
        "pyright",
        "pytest",
        "typescript",
        "node_tests",
        "production_entry",
        "independent_review",
    )
    if (
        value.get("runtime") != runtime_identity()
        or not all(checks.get(name) == "passed" for name in required)
        or not value.get("evidence_refs")
    ):
        raise ValueError("dynamic route qualification lacks current offline review evidence")
    return value


def _verified_qualification_report(root: Path, *, require_passed: bool) -> dict[str, object]:
    registration = load_dynamic_route_qualification(root)
    panel = _registered_qualification_panel(registration)
    report = _read_object(root / "qualification-report.json")
    core = {key: item for key, item in report.items() if key != "report_hash"}
    if (
        canonical_hash(core) != report.get("report_hash")
        or report.get("registration_hash") != registration.get("registration_hash")
        or report.get("runtime") != registration.get("runtime")
    ):
        raise ValueError("dynamic route qualification report changed")
    shared_budget = _qualification_budget(registration)
    _qualification_recovery_claim(root, registration, shared_budget, create=False)
    if not require_passed and (
        report.get("stage_passed") is not True
        or (shared_budget is None and "route_panel" not in registration)
    ):
        return report
    owner = f"dynamic-route-qualification-{registration['registration_hash']}"
    authority_store = LocalDataSnapshotStore(root / "authority")
    owner_journal = RunJournal.authoritative(authority_store)
    try:
        owner_record = owner_journal.get_run(owner)
    except KeyError as exc:
        raise ValueError("dynamic route qualification has no authoritative terminal") from exc
    if (
        owner_record.status is not RunStatus.COMPLETED
        or owner_record.terminal_artifact_id is None
        or authority_store.artifacts.read_json(owner_record.terminal_artifact_id) != report
    ):
        raise ValueError("dynamic route qualification has no authoritative terminal")
    if "route_panel" in registration:
        replay_budget = shared_budget or ModelBudget(
            journal=owner_journal,
            owner_run_id=owner,
            max_requests=12,
            max_cost_microusd=1_000_000,
            scope_limits=(ModelBudgetScope("route_qualification", 1_000_000),),
            scope="route_qualification",
        )
        shared_budget = replay_budget
        group_id, group_hash = _qualification_group_identity(registration)
        expected_group = {
            "binding": replay_budget.binding,
            "group_id": group_id,
            "members": [member.to_dict() for member in _qualification_group_members(panel)],
            "call_graph_hash": group_hash,
            **({"scope": replay_budget.scope} if replay_budget.scope_limits else {}),
        }
        group_event = replay_budget.journal.event(
            f"{replay_budget.owner_run_id}.budget.group.{canonical_hash(group_id)}"
        )
        if group_event is None or group_event.payload != expected_group:
            raise ValueError("qualification group admission changed")
        for event in replay_budget.journal.events(replay_budget.owner_run_id):
            key = event.payload.get("request_key")
            for topology in panel:
                if (
                    event.event_type == "pi.budget.reserved"
                    and isinstance(key, str)
                    and key.startswith(f"{owner}.{topology.value}.pi-invocation.")
                    and (
                        event.payload.get("group_id") != group_id
                        or event.payload.get("group_member_id") != topology.value
                    )
                ):
                    raise ValueError("qualification request escaped its route group")
        for member in _qualification_group_members(panel):
            route_keys = frozenset(
                _string(event.payload, "request_key")
                for event in replay_budget.journal.events(replay_budget.owner_run_id)
                if event.event_type == "pi.budget.reserved"
                and event.payload.get("group_id") == group_id
                and event.payload.get("group_member_id") == member.member_id
            )
            route_summary = _qualification_batch_summary(
                replay_budget,
                (),
                request_keys=route_keys,
            )
            if (
                route_summary["physical_requests"] > member.max_requests
                or route_summary["known_cost_microusd"] + route_summary["reserved_microusd"]
                > member.max_cost_microusd
            ):
                raise ValueError("qualification route exceeded its group allowance")
    cases = report.get("cases")
    if not isinstance(cases, list):
        raise ValueError("dynamic route qualification has invalid cases")
    raw_cases = cast(list[object], cases)
    if any(not isinstance(item, dict) for item in raw_cases):
        raise ValueError("dynamic route qualification has invalid cases")
    typed_cases = cast(list[dict[str, object]], raw_cases)
    if (
        len(typed_cases) != len(panel)
        or {item.get("topology") for item in typed_cases} != {topology.value for topology in panel}
        or any(item.get("status") != "completed" for item in typed_cases)
    ):
        raise ValueError("dynamic route qualification did not complete the exact route panel")
    profiles = cast(dict[str, dict[str, object]], registration["profiles"])
    registered_at = _datetime(_string(registration, "registered_at"))
    for item in typed_cases:
        topology = AnalysisTopology(_string(item, "topology"))
        run_id = _string(item, "run_id")
        expected_run_id = f"{owner}.{topology.value}"
        if run_id != expected_run_id:
            raise ValueError("dynamic route case Run identity differs from registration")
        case_store = LocalDataSnapshotStore(root / "cases" / topology.value)
        thesis, source = reopen_completed_research_thesis(
            journal=RunJournal.authoritative(case_store),
            artifact_store=case_store.artifacts,
            run_id=run_id,
        )
        case_authority = ResearchThesisAuthority(
            case_store,
            experiment_id=cast(str, registration["registration_hash"]),
            arm_id=topology.value,
        )
        case_authority.replay(run_id)
        binding = _object(case_store.artifacts.read_json(_string(source, "binding_hash")))
        expected_inputs = ResearchThesisRunInputs(
            repository=_qualification_repository(registered_at),
            target_id="SYNTHETIC.BROAD.ETF",
            thesis_epoch="route-qualification-v1",
            allowed_horizons=frozenset({1, 3, 5}),
            schema_version=cast(
                str,
                registration.get("research_inputs_schema_version")
                or _object(binding["inputs"]).get(
                    "schema_version", "market-impact.research-thesis-inputs.v1"
                ),
            ),
            research_question=(
                "What is the defensible direction and horizon for the synthetic proxy?"
            ),
        ).identity_dict()
        if shared_budget is not None and binding.get("budget_owner") != {
            "journal_path": str(shared_budget.journal.path),
            "run_id": shared_budget.owner_run_id,
            "binding": shared_budget.binding,
        }:
            raise ValueError("qualification case has changed parent budget ancestry")
        if shared_budget is not None:
            _verify_qualification_native_budget(
                case_store,
                run_id,
                shared_budget,
                model_provider_profile_from_dict(profiles[topology.value]),
            )
        if (
            thesis.base_case_direction
            not in {
                BaseCaseDirection.UP,
                BaseCaseDirection.DOWN,
                BaseCaseDirection.RANGEBOUND,
                BaseCaseDirection.UNKNOWN,
            }
            or binding.get("run_id") != expected_run_id
            or binding.get("inputs") != expected_inputs
            or binding.get("profile") != profiles[topology.value]
            or binding.get("runtime") != registration["runtime"]
            or binding.get("prompt")
            != (
                RESEARCH_THESIS_V2_PROMPT
                if expected_inputs.get("schema_version")
                == "market-impact.research-thesis-inputs.v2"
                else RESEARCH_THESIS_PROMPT
            )
            or binding.get("max_output_tokens") != registration["maximum_output_tokens_per_run"]
            or source["terminal_hash"] != item.get("terminal_hash")
            or source["journal_hash"] != item.get("journal_hash")
            or UsageLedger(case_store.index_path).ledger_hash != item.get("usage_ledger_hash")
        ):
            raise ValueError("dynamic route case differs from its signed native terminal")
    if shared_budget is not None:
        run_ids = tuple(f"{owner}.{topology.value}" for topology in panel)
        summary = _qualification_batch_summary(shared_budget, run_ids)
        if report.get("budget") != summary or report.get("reconciled") != (
            summary["unsettled_requests"] == 0
        ):
            raise ValueError("qualification batch does not reconcile with its parent budget")
    return report


def _verified_analysis_report(root: Path) -> dict[str, object]:
    registration = load_dynamic_effectiveness_study(root)
    report = _read_object(root / "opened-analysis-report.json")
    core = {key: item for key, item in report.items() if key != "report_hash"}
    if (
        canonical_hash(core) != report.get("report_hash")
        or report.get("registration_hash") != registration.get("registration_hash")
        or report.get("runtime") != registration.get("runtime")
    ):
        raise ValueError("dynamic opened analysis report changed")
    owner = f"dynamic-opened-analysis-{registration['registration_hash']}"
    authority_store = LocalDataSnapshotStore(root / "analysis-authority")
    owner_journal = RunJournal.authoritative(authority_store)
    try:
        owner_record = owner_journal.get_run(owner)
    except KeyError as exc:
        raise ValueError("dynamic opened analysis report has no authoritative terminal") from exc
    if (
        owner_record.status is not RunStatus.COMPLETED
        or owner_record.terminal_artifact_id is None
        or authority_store.artifacts.read_json(owner_record.terminal_artifact_id) != report
    ):
        raise ValueError("dynamic opened analysis report has no authoritative terminal")
    raw_results = report.get("results")
    if not isinstance(raw_results, list):
        raise ValueError("dynamic opened analysis report has invalid results")
    raw_list = cast(list[object], raw_results)
    if any(not isinstance(item, dict) for item in raw_list):
        raise ValueError("dynamic opened analysis report has invalid results")
    results = cast(list[dict[str, object]], raw_list)
    expected = {
        (_string(source, "case_id"), topology.value)
        for source in cast(list[dict[str, object]], registration["opened_case_sources"])
        for topology in _TOPOLOGIES
    }
    base = [item for item in results if item.get("repetition") == "base"]
    actual = {(item.get("case_id"), item.get("topology")) for item in base}
    if len(base) != len(expected) or actual != expected:
        raise ValueError("dynamic opened analysis fixed denominator is not the exact 8x3 panel")
    run_store = LocalDataSnapshotStore(root / "analysis-runs")
    run_journal = RunJournal.authoritative(run_store)
    for item in base:
        run_id = item.get("run_id")
        if item.get("status") == "completed":
            if not isinstance(run_id, str):
                raise ValueError("completed dynamic result has no Run identity")
            thesis, source = reopen_completed_research_thesis(
                journal=run_journal,
                artifact_store=run_store.artifacts,
                run_id=run_id,
            )
            if (
                thesis.to_dict() != item.get("thesis")
                or source["terminal_hash"] != item.get("terminal_hash")
                or source["journal_hash"] != item.get("journal_hash")
            ):
                raise ValueError("dynamic result differs from its signed native terminal")
    return report


def load_verified_opened_analysis_report(root: Path) -> dict[str, object]:
    """Public replay boundary for an immutable, signed opened-analysis report."""

    return _verified_analysis_report(root)


def portfolio_actions_complete(results: list[dict[str, object]]) -> bool:
    """Require the exact four-account by three-model matrix; empty is never complete."""

    expected = {
        (scenario["scenario_id"], topology.value)
        for scenario in _PORTFOLIO_SCENARIOS
        for topology in _TOPOLOGIES
    }
    actual = {(item.get("scenario_id"), item.get("topology")) for item in results}
    return (
        len(results) == len(expected)
        and actual == expected
        and all(item.get("status") == "completed" for item in results)
    )


def _verified_portfolio_report(root: Path) -> dict[str, object]:
    registration = load_dynamic_effectiveness_study(root)
    report = _read_object(root / "portfolio-ablation-report.json")
    core = {key: item for key, item in report.items() if key != "report_hash"}
    if (
        canonical_hash(core) != report.get("report_hash")
        or report.get("registration_hash") != registration.get("registration_hash")
        or report.get("runtime") != registration.get("runtime")
        or report.get("mock_execution") is not False
        or report.get("live_execution") is not False
    ):
        raise ValueError("dynamic portfolio ablation report changed")
    return report


def _write_new(path: Path, value: dict[str, object]) -> None:
    with path.open("x", encoding="utf-8") as output:
        json.dump(value, output, ensure_ascii=False, indent=2, sort_keys=True)
        output.flush()
        os.fsync(output.fileno())


def _read_object(path: Path) -> dict[str, object]:
    value: object = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise TypeError(f"expected a JSON object: {path}")
    mapping = cast(dict[object, object], value)
    if any(not isinstance(key, str) for key in mapping):
        raise TypeError(f"expected string JSON keys: {path}")
    return cast(dict[str, object], mapping)


def _object(value: object) -> dict[str, object]:
    if not isinstance(value, dict):
        raise TypeError("dynamic effectiveness object is invalid")
    return cast(dict[str, object], value)


def _string(value: dict[str, object], key: str) -> str:
    item = value.get(key)
    if not isinstance(item, str) or not item.strip() or item != item.strip():
        raise ValueError(f"dynamic effectiveness {key} must be nonempty trimmed text")
    return item


def _datetime(value: str) -> datetime:
    result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if result.tzinfo is None:
        raise ValueError("dynamic effectiveness timestamp must be timezone-aware")
    return result


def _timestamp(value: datetime) -> str:
    if value.tzinfo is None:
        raise ValueError("dynamic effectiveness timestamp must be timezone-aware")
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")
