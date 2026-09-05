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
from market_impact_agent.model_budget import ModelBudget
from market_impact_agent.model_provider import (
    ModelProviderProfile,
    model_provider_profile_from_dict,
)
from market_impact_agent.paper_execution import PriceBasis
from market_impact_agent.pi_deployment import PiRuntimePermit
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


def prepare_dynamic_route_qualification(
    root: Path,
    *,
    profiles: tuple[ModelProviderProfile, ModelProviderProfile, ModelProviderProfile],
    verification_path: Path,
    registered_at: datetime | None = None,
) -> dict[str, object]:
    """Freeze the only paid route-qualification batch for the current build."""

    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(root, 0o700)
    path = root / "qualification-registration.json"
    if path.exists():
        return load_dynamic_route_qualification(root)
    verification = _verified_build(verification_path)
    ordered = _ordered_profiles(profiles)
    value: dict[str, object] = {
        "schema_version": "market-impact.dynamic-route-qualification.v1",
        "experiment": "dynamic-horizon-three-model-route-qualification-v1",
        "registered_at": _timestamp(registered_at or datetime.now(UTC)),
        "runtime": runtime_identity(),
        "profiles": {
            topology.value: profile.to_dict()
            for topology, profile in zip(_TOPOLOGIES, ordered, strict=True)
        },
        "verification_hash": canonical_hash(verification),
        "maximum_cost_microusd": 1_000_000,
        "maximum_physical_requests": 12,
        "maximum_output_tokens_per_run": 4096,
        "execution_capability": False,
        "live_execution": False,
    }
    value["registration_hash"] = canonical_hash(value)
    _write_new(path, value)
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
    return value


async def run_dynamic_route_qualification(root: Path) -> dict[str, object]:
    """Qualify model identity, effort route and one real native terminal per Profile."""

    report_path = root / "qualification-report.json"
    if report_path.exists():
        return _verified_qualification_report(root, require_passed=False)
    registration = load_dynamic_route_qualification(root)
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
    run_ids = tuple(f"{owner}.{topology.value}" for topology in _TOPOLOGIES)
    permit = PiRuntimePermit(
        canonical_hash(registration["runtime"]),
        tuple(
            model_provider_profile_from_dict(profiles[topology.value]).route_identity
            for topology in _TOPOLOGIES
        ),
        cast(str, registration["registration_hash"]),
        run_ids,
        owner,
    )
    budget = ModelBudget(
        journal=journal,
        owner_run_id=owner,
        max_requests=cast(int, registration["maximum_physical_requests"]),
        max_cost_microusd=cast(int, registration["maximum_cost_microusd"]),
    )
    results: list[dict[str, object]] = []
    for topology, run_id in zip(_TOPOLOGIES, run_ids, strict=True):
        profile = model_provider_profile_from_dict(profiles[topology.value])
        case_store = LocalDataSnapshotStore(root / "cases" / topology.value)
        provider = PiRuntimeProvider(profile, budget=budget, permit=permit)
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
    summary = budget.summary()
    stage_passed = len(results) == 3 and all(item["status"] == "completed" for item in results)
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
    """Install only a completed, replay-verified three-route qualification."""

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
    report = _read_object(root / "qualification-report.json")
    core = {key: item for key, item in report.items() if key != "report_hash"}
    if (
        canonical_hash(core) != report.get("report_hash")
        or report.get("registration_hash") != registration.get("registration_hash")
        or report.get("runtime") != registration.get("runtime")
    ):
        raise ValueError("dynamic route qualification report changed")
    if not require_passed:
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
    cases = report.get("cases")
    if not isinstance(cases, list):
        raise ValueError("dynamic route qualification has invalid cases")
    raw_cases = cast(list[object], cases)
    if any(not isinstance(item, dict) for item in raw_cases):
        raise ValueError("dynamic route qualification has invalid cases")
    typed_cases = cast(list[dict[str, object]], raw_cases)
    if (
        len(typed_cases) != len(_TOPOLOGIES)
        or {item.get("topology") for item in typed_cases}
        != {topology.value for topology in _TOPOLOGIES}
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
        binding = _object(case_store.artifacts.read_json(_string(source, "binding_hash")))
        expected_inputs = ResearchThesisRunInputs(
            repository=_qualification_repository(registered_at),
            target_id="SYNTHETIC.BROAD.ETF",
            thesis_epoch="route-qualification-v1",
            allowed_horizons=frozenset({1, 3, 5}),
            research_question=(
                "What is the defensible direction and horizon for the synthetic proxy?"
            ),
        ).identity_dict()
        if (
            thesis.base_case_direction
            not in {
                BaseCaseDirection.UP,
                BaseCaseDirection.DOWN,
                BaseCaseDirection.RANGEBOUND,
            }
            or binding.get("run_id") != expected_run_id
            or binding.get("inputs") != expected_inputs
            or binding.get("profile") != profiles[topology.value]
            or binding.get("runtime") != registration["runtime"]
            or binding.get("prompt") != RESEARCH_THESIS_PROMPT
            or binding.get("max_output_tokens") != registration["maximum_output_tokens_per_run"]
            or source["terminal_hash"] != item.get("terminal_hash")
            or source["journal_hash"] != item.get("journal_hash")
            or UsageLedger(case_store.index_path).ledger_hash != item.get("usage_ledger_hash")
        ):
            raise ValueError("dynamic route case differs from its signed native terminal")
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
