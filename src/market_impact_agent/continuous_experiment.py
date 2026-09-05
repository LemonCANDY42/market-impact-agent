"""Frozen 18-window coverage and fair daily-frontier continuous study composition.

No model runs before every source, baseline and planning-cost input has been
reopened. Incomplete fixed-denominator rows are results, never implied holds.
"""

from __future__ import annotations

import json
import os
from contextlib import AsyncExitStack, aclosing
from dataclasses import asdict, dataclass, replace
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from pathlib import Path
from typing import cast
from zoneinfo import ZoneInfo

from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.agent_runtime import ProviderUsage
from market_impact_agent.continuous_baselines import (
    CONTINUOUS_EXECUTABLE_BASELINE_IDS,
    ContinuousBaselineAccountSeed,
    evaluate_continuous_baseline_window,
    registered_baseline_windows,
)
from market_impact_agent.continuous_decision import (
    ContinuousCadence,
    ContinuousDecision,
    ContinuousReviewCoordinator,
    PendingReview,
    ReviewFrame,
)
from market_impact_agent.continuous_initial_adoption import InitialAdoptionAuthority
from market_impact_agent.continuous_metrics import (
    compare_continuous_accounts,
    measure_continuous_account,
)
from market_impact_agent.continuous_portfolio_runtime import (
    ContinuousPortfolioRuntime,
    continuous_frame_input_hash,
)
from market_impact_agent.continuous_study import ContinuousStudyRegistration
from market_impact_agent.continuous_study_runner import (
    continuous_study_scope,
    load_prepared_continuous_registration,
    study_budget,
)
from market_impact_agent.domain import ApprovalMode, Side, TradingEnvironment, TradingMandateV3
from market_impact_agent.frozen_research import FrozenResearchRepository
from market_impact_agent.historical_ashare_inputs import HistoricalAShareInputs
from market_impact_agent.market_regimes import RegimePanel
from market_impact_agent.model_provider import ModelProviderProfile
from market_impact_agent.on_demand_research import ResearchSourceTemplate
from market_impact_agent.pi_runtime import PiRuntimeProvider
from market_impact_agent.portfolio_review import PORTFOLIO_REVIEW_PROMPT
from market_impact_agent.research_thesis_runtime import RESEARCH_THESIS_PROMPT
from market_impact_agent.streaming_nautilus_account import (
    AShareDailyBar,
    HistoricalCorporateAction,
    HistoricalStreamingAccount,
)
from market_impact_agent.usage_ledger import UsageLedger

_SEEDS = ("510300.SH", "510500.SH")
_POLICY = {
    "version": "continuous-preopen-validated-comparisons-and-baselines-v2",
    "pre_open_cutoff": "09:25 Asia/Shanghai",
    "baseline_ids": list(CONTINUOUS_EXECUTABLE_BASELINE_IDS),
    "initial_cash": "100000",
    "gross_limit": "100000",
    "turnover_limit": "100000",
    "position_count": 5,
    "submission_limit": 10,
    "daily_loss_kill": "10000",
    "drawdown_kill": "20000",
    "opening": "prior-session-half-510300-with-real-fees",
}


def registered_frame_cutoff(day: date) -> datetime:
    """The registered historical decision boundary, before the opening auction."""
    return datetime.combine(day, time(9, 25), ZoneInfo("Asia/Shanghai"))


@dataclass(frozen=True, slots=True)
class FrozenContinuousWindow:
    window_id: str
    frames: tuple[ReviewFrame, ...]
    repositories: tuple[FrozenResearchRepository, ...]
    market: HistoricalAShareInputs
    calendar_dates: tuple[date, ...]
    candidate_symbols: tuple[str, ...] = _SEEDS

    def __post_init__(self) -> None:
        if not self.frames or len(self.frames) != len(self.repositories):
            raise ValueError("window requires one frozen repository per frame")
        if tuple(sorted(set(self.calendar_dates))) != self.calendar_dates:
            raise ValueError("window calendar must be ordered and unique")
        if not self.candidate_symbols or len(set(self.candidate_symbols)) != len(
            self.candidate_symbols
        ):
            raise ValueError("window requires a frozen unique candidate pool")


def _key(root: Path) -> bytes:
    path = root / ".continuous-account-key"
    try:
        fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    except FileExistsError:
        pass
    else:
        with os.fdopen(fd, "wb") as output:
            output.write(os.urandom(32))
            output.flush()
            os.fsync(output.fileno())
    value = path.read_bytes()
    if len(value) != 32 or path.stat().st_mode & 0o077:
        raise PermissionError("continuous account key requires private immutable 32-byte material")
    return value


def _persist(root: Path, suffix: str, kind: str, payload: dict[str, object]) -> str:
    budget = study_budget(root, "rolling")
    from market_impact_agent.data_inputs import LocalDataSnapshotStore

    store = LocalDataSnapshotStore(budget.journal.path.parent)
    artifact = store.artifacts.put_json(payload)
    budget.journal.append(
        run_id=budget.owner_run_id,
        event_id=f"{budget.owner_run_id}.{suffix}",
        event_type=kind,
        observed_at=datetime.now().astimezone(),
        payload={"artifact_hash": artifact.content_hash},
    )
    return artifact.content_hash


async def prepare_continuous_experiment(
    *,
    study_root: Path,
    registration: ContinuousStudyRegistration,
    selection_panel: RegimePanel,
    windows: tuple[FrozenContinuousWindow, ...],
    profiles: tuple[ModelProviderProfile, ...],
) -> dict[str, object]:
    if load_prepared_continuous_registration(study_root) != registration.to_dict():
        raise PermissionError("batch differs from prepared study registration")
    if tuple(profile.profile_hash for profile in profiles) != tuple(
        item.provider_profile_hash for item in registration.model_profiles
    ):
        raise PermissionError("batch models differ from frozen study profiles")
    if len({item.window_id for item in windows}) != len(windows):
        raise ValueError("duplicate batch window")
    budget = study_budget(study_root, "rolling")
    registered = registered_baseline_windows(registration, selection_panel)
    supplied = {item.window_id: item for item in windows}
    if set(supplied) - {item.window.window_id for item in registered}:
        raise PermissionError("batch contains an unregistered window")
    deep = {item.coverage_window_id: item for item in registration.deep_cells}
    # Validate every cutoff before reopening any source or executing any baseline.
    for registered_window in registered:
        window = supplied.get(registered_window.window.window_id)
        if window is None:
            continue
        if window.calendar_dates != registered_window.sessions:
            raise PermissionError("window calendar differs from registered source panel")
        definition = registered_window.window
        expected = tuple(
            day
            for day in registered_window.sessions
            if definition.window_id in deep and day <= deep[definition.window_id].outcome_window_end
        )
        if not expected:
            expected = (definition.decision_session,)
        cutoffs = tuple(registered_frame_cutoff(day) for day in expected)
        if tuple(frame.cutoff for frame in window.frames) != cutoffs:
            raise PermissionError(
                "research frames require exact registered 09:25 Asia/Shanghai cutoffs"
            )
    problems: list[dict[str, object]] = []
    frozen: list[dict[str, object]] = []
    candidate_execution_gaps: list[dict[str, object]] = []
    qualified_preflight = False
    baselines: list[dict[str, object]] = []
    estimates = {"analysis_coverage": 0, "portfolio_coverage": 0, "rolling": 0}
    worst = {key: 0 for key in estimates}
    key = _key(study_root)
    for registered_window in registered:
        definition = registered_window.window
        window = supplied.get(definition.window_id)
        if window is None:
            problems.append({"window_id": definition.window_id, "reason": "frozen_window_missing"})
            continue
        if window.market.store.index_path != budget.journal.path:
            raise PermissionError("batch source must reopen the shared study authority")
        selected: list[dict[str, object]] = []
        for frame, repository in zip(window.frames, window.repositories, strict=True):
            if (
                repository.evidence_pack.as_of != frame.cutoff
                or continuous_frame_input_hash(repository, window.market, frame.snapshot_ids)
                != frame.input_hash
            ):
                raise PermissionError("batch frame does not bind its source repository")
            documents = {
                reference.evidence_id: cast(
                    dict[str, object],
                    await repository.read_evidence({"evidence_id": reference.evidence_id}),
                )["document"]
                for reference in repository.evidence_pack.evidence
            }
            selected.append(
                {
                    "frame": frame.to_dict(),
                    "evidence_pack": repository.evidence_pack.to_dict(),
                    "patterns": [
                        await repository.read_pattern_pack({"pack_id": reference.pack_id})
                        for reference in repository.evidence_pack.pattern_packs
                    ],
                    "documents": documents,
                }
            )
            for gap in frame.gaps:
                problems.append({"window_id": definition.window_id, "reason": gap})
        qualified = window.market.policy.limit_basis == "qualified_seed_etf_exchange_rule_v1"
        qualified_preflight = qualified_preflight or qualified
        matched_window = registered_window
        if qualified and definition.window_id in deep:
            end = deep[definition.window_id].outcome_window_end
            matched_window = replace(
                registered_window,
                window=replace(definition, outcome_window_end=end),
                sessions=tuple(day for day in registered_window.sessions if day <= end),
            )
        window_candidate_gaps: list[dict[str, object]] = []
        symbols = (
            tuple(dict.fromkeys(("510300.SH", *window.candidate_symbols))) if qualified else _SEEDS
        )
        for day in (definition.observation_through_session, *matched_window.sessions):
            for symbol in symbols:
                source = window.market.session(symbol, day)
                if not source.execution_ready:
                    source_gap: dict[str, object] = {
                        "window_id": definition.window_id,
                        "day": day.isoformat(),
                        "symbol": symbol,
                        "reason": "execution_source_incomplete",
                        "gaps": list(source.gaps),
                    }
                    if qualified and symbol != "510300.SH":
                        window_candidate_gaps.append(source_gap)
                    else:
                        problems.append(source_gap)
        candidate_execution_gaps.extend(window_candidate_gaps)
        for baseline_id in CONTINUOUS_EXECUTABLE_BASELINE_IDS:
            result = evaluate_continuous_baseline_window(
                registration_id=registration.registration_id,
                baseline_id=baseline_id,
                registered_window=matched_window,
                historical_inputs=window.market,
                account_seed=ContinuousBaselineAccountSeed("baseline-" + definition.window_id, key),
                state_root=(study_root / "baseline-engine" / "qualified-matched-v1")
                if qualified
                else study_root / "baseline-engine",
            )
            baselines.append(result)
            if result["status"] != "complete":
                problems.append(
                    {
                        "window_id": definition.window_id,
                        "reason": "baseline_incomplete",
                        "baseline_id": baseline_id,
                    }
                )
        source_manifest = {
            "window_id": definition.window_id,
            "frames": selected,
            "snapshot_ids": list(window.market.snapshot_ids),
            "rule_artifact_hashes": list(window.market.rule_artifact_hashes),
            **(
                {"fund_halt_artifact_hashes": list(window.market.fund_halt_artifact_hashes)}
                if window.market.fund_halt_artifact_hashes
                else {}
            ),
            "policy": _json(window.market.policy.to_dict()),
            "calendar": [day.isoformat() for day in window.calendar_dates],
            "candidate_symbols": list(window.candidate_symbols),
            **(
                {
                    "preflight_qualification": {
                        "version": "qualified-held-seed-and-registered-matched-horizon-v1",
                        "required_seed_symbol": "510300.SH",
                        "optional_candidates": "diagnostic_only_until_held_or_ordered",
                        "later_held_or_ordered_source_gaps": "runtime_fail_closed",
                        "horizon_basis": "registered_deep_cell"
                        if definition.window_id in deep
                        else "registered_coverage_window",
                        "observation_through_session": (
                            definition.observation_through_session.isoformat()
                        ),
                        "matched_outcome_window_end": (
                            matched_window.window.outcome_window_end.isoformat()
                        ),
                        "matched_execution_sessions": [
                            day.isoformat() for day in matched_window.sessions
                        ],
                        "full_registered_window": definition.to_dict(),
                        "full_registered_calendar": "calendar",
                    },
                    "candidate_execution_gaps": window_candidate_gaps,
                }
                if qualified
                else {}
            ),
        }
        frozen.append(source_manifest)
        # One byte per input token is the conservative tokenizer-independent
        # planning assumption. Admission prices the actual request independently.
        frame_bytes = [len(json.dumps(frame, ensure_ascii=False).encode()) for frame in selected]
        prompt_bytes = {
            "analysis_coverage": len(RESEARCH_THESIS_PROMPT.encode()),
            "portfolio_coverage": len(PORTFOLIO_REVIEW_PROMPT.encode()),
        }
        for profile in profiles:
            for stage, prompt_size in prompt_bytes.items():
                estimate = profile.pricing.estimate_microusd(
                    ProviderUsage(
                        frame_bytes[0] + prompt_size + 12000, profile.reserved_output_tokens
                    )
                )
                estimates[stage] += estimate
                worst[stage] += profile.budget.max_estimated_cost_microusd or estimate
                if definition.window_id in deep:
                    for size in frame_bytes[1:]:
                        renewal = profile.pricing.estimate_microusd(
                            ProviderUsage(
                                size + prompt_size + 12000, profile.reserved_output_tokens
                            )
                        )
                        estimates["rolling"] += renewal * 3
                        worst["rolling"] += (
                            profile.budget.max_estimated_cost_microusd or renewal
                        ) * 3
    result: dict[str, object] = {
        "schema_version": "market-impact.continuous-experiment-preflight.v1",
        "registration_id": registration.registration_id,
        "source_windows": frozen,
        "baselines": baselines,
        "problems": problems,
        "source_and_baseline_ready": not problems,
        **({"candidate_execution_gaps": candidate_execution_gaps} if qualified_preflight else {}),
        "planning_estimated_microusd": estimates,
        "worst_case_role_caps_microusd": worst,
        "planning_assumptions": (
            "one input token per frozen frame byte plus owning role prompt "
            "and 12000 context tokens; "
            "one output reserve per role; every later session reviewed in each cadence; "
            "hard shared-budget admission permits typed partial completion"
        ),
        "execution_policy": _POLICY,
        "partial_batch_on_budget_exhaustion": True,
        "initial_denominator": 54,
        "rolling_denominator": 72,
        "live_execution": False,
    }
    identity = canonical_hash(
        {"registration": registration.registration_id, "sources": frozen, "policy": _POLICY}
    )
    result["batch_id"] = identity
    result["artifact_hash"] = _persist(
        study_root, f"continuous.batch.{identity}", "continuous.batch.frozen", result
    )
    return result


def _json(value: object) -> object:
    return json.loads(
        json.dumps(
            value,
            default=lambda item: (
                item.isoformat() if isinstance(item, (datetime, date)) else str(item)
            ),
        )
    )


def _runtime(
    *,
    study_root: Path,
    registration: ContinuousStudyRegistration,
    window: FrozenContinuousWindow,
    profile_arm: str,
    cadence: str,
    provider: PiRuntimeProvider,
    portfolio_provider: PiRuntimeProvider | None = None,
    source_runtime: ContinuousPortfolioRuntime | None = None,
    historical_research_templates: tuple[ResearchSourceTemplate, ...] = (),
    research_episode_deadline: datetime | None = None,
) -> ContinuousPortfolioRuntime:
    definition = next(
        item for item in registration.coverage_windows if item.window_id == window.window_id
    )
    seed = window.market.session("510300.SH", definition.observation_through_session)
    if not seed.execution_ready or seed.spec is None or seed.bar is None:
        raise ValueError("historical bootstrap source is incomplete")
    experiment, arm = continuous_study_scope(
        registration.registration_id, window.window_id, profile_arm, cadence
    )
    path = (
        study_root
        / "account-engine"
        / (
            canonical_hash({"arm": arm, "frames": [frame.to_dict() for frame in window.frames]})
            + ".jsonl"
        )
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    account = HistoricalStreamingAccount(
        specs=(seed.spec,),
        journal_path=path,
        account_reference=arm,
        account_reference_key=_key(study_root),
    )
    try:
        account.bootstrap_half_hs300(seed.bar)
    except BaseException:
        account.close()
        raise
    repositories = {
        frame.cutoff: repository
        for frame, repository in zip(window.frames, window.repositories, strict=True)
    }

    def mandate(frame: ReviewFrame) -> TradingMandateV3:
        return TradingMandateV3(
            mandate_id="continuous-historical-template-v1",
            account_id=account.account_id,
            harness_authority_id=window.market.store.harness_authority_id,
            environment=TradingEnvironment.BACKTEST,
            approval_mode=ApprovalMode.MANUAL_EACH,
            valid_from=frame.cutoff,
            valid_until=frame.cutoff + timedelta(minutes=10),
            allowed_instruments=frozenset(window.candidate_symbols),
            allowed_instrument_classes=frozenset({"cash_equity", "unlevered_exchange_traded_fund"}),
            allowed_sides=frozenset({Side.BUY, Side.SELL}),
            currency="CNY",
            gross_exposure_limit=Decimal("100000"),
            minimum_net_exposure=Decimal(0),
            maximum_net_exposure=Decimal("100000"),
            maximum_position_count=5,
            maximum_single_position_fraction=Decimal(1),
            daily_turnover_limit=Decimal("100000"),
            daily_submission_limit=10,
            daily_loss_kill_threshold=Decimal("10000"),
            strategy_peak_drawdown_kill_threshold=Decimal("20000"),
            universe_binding_hash="0" * 64,
        )

    def age(frame: ReviewFrame) -> timedelta:
        index = window.calendar_dates.index(frame.cutoff.date())
        prior = (
            definition.observation_through_session
            if index == 0
            else window.calendar_dates[index - 1]
        )
        return timedelta(days=(frame.cutoff.date() - prior).days + 1)

    return ContinuousPortfolioRuntime(
        store=window.market.store,
        experiment_id=experiment,
        arm_id=arm,
        account=account,
        research_repository=lambda frame: repositories[frame.cutoff],
        market_inputs=lambda _: window.market,
        mandate_template=mandate,
        symbols=lambda _: window.candidate_symbols,
        account_max_age=age,
        provider=provider,
        portfolio_provider=portfolio_provider,
        historical_research_templates=historical_research_templates,
        research_episode_deadline=research_episode_deadline,
        initial_adoption_authority=None
        if source_runtime is None
        else InitialAdoptionAuthority(
            study_root=study_root,
            source_runtime=source_runtime,
            coverage_window_id=window.window_id,
            profile_arm=profile_arm,
            cadence=cadence,
        ),
    )


@dataclass
class _RollingArm:
    runtime: ContinuousPortfolioRuntime
    window: FrozenContinuousWindow
    coordinator: ContinuousReviewCoordinator
    row: dict[str, object]


def _coordinator(
    *,
    runtime: ContinuousPortfolioRuntime,
    window: FrozenContinuousWindow,
    initial: ContinuousDecision,
    registration_id: str,
    cadence: ContinuousCadence,
) -> ContinuousReviewCoordinator:
    episode_id = "continuous-episode-" + canonical_hash(
        {"arm": runtime.arm_id, "frames": [frame.to_dict() for frame in window.frames]}
    )

    async def advance(index: int, decision: ContinuousDecision | None) -> dict[str, object]:
        frame = window.frames[index]
        # This reopens signed sizing against the cutoff-filtered old account prefix.
        intents = () if decision is None else runtime.admitted_intents(decision, frame)
        prefix = runtime.account.results[index]
        required = set(prefix.positions) | {order.instrument_id for order in intents}
        symbols = sorted(set(window.candidate_symbols) | required)
        bars: dict[str, AShareDailyBar] = {}
        actions: dict[str, HistoricalCorporateAction] = {}
        market = runtime.source_market(decision, frame)
        for symbol in symbols:
            source = market.session(symbol, frame.cutoff.date())
            if not source.execution_ready or source.spec is None or source.bar is None:
                if symbol in required:
                    raise ValueError("held or ordered security session input is incomplete")
                continue
            runtime.account.register_instrument(source.spec)
            bars[symbol] = source.bar
            for action in source.corporate_actions:
                actions[action.action_id] = action
        ordered_actions = tuple(actions[key] for key in sorted(actions))
        input_hash = canonical_hash(
            _json(
                {
                    "bars": {key: asdict(bar) for key, bar in sorted(bars.items())},
                    "intents": [order.to_dict() for order in intents],
                    "actions": [asdict(action) for action in ordered_actions],
                }
            )
        )
        if len(runtime.account.results) > index + 1:
            result = runtime.account.results[index + 1]
            if result.input_hash != input_hash:
                raise PermissionError(
                    "cached account prefix differs from reopened bars/intents/actions"
                )
        else:
            result = runtime.account.advance_session(
                bars, intents=intents, corporate_actions=ordered_actions
            )
        return {
            "result_hash": result.result_hash,
            "input_hash": result.input_hash,
            "as_of": result.account_state.as_of.isoformat(),
        }

    return ContinuousReviewCoordinator(
        runtime.journal,
        episode_id=episode_id,
        registration_hash=registration_id,
        account_scope=runtime.account.account_id,
        model_arm=runtime.arm_id,
        cadence=cadence,
        frames=window.frames,
        decide=runtime.decide,
        advance_account=advance,
        validate_decision=runtime.validate_decision,
        shared_initial=initial,
    )


def _research_coverage(
    window: FrozenContinuousWindow | None, *, initial: bool
) -> dict[str, object]:
    repositories = (
        () if window is None else window.repositories[:1] if initial else window.repositories
    )
    gaps = tuple(
        dict.fromkeys(
            gap for repository in repositories for gap in repository.evidence_pack.data_gaps
        )
    )
    return {
        "classification": "missing_frozen_research"
        if not repositories
        else "limited_data"
        if gaps
        else "no_declared_gaps",
        "data_gaps": list(gaps),
        "frames": [
            {
                "cutoff": repository.evidence_pack.as_of.isoformat(),
                "data_gaps": list(repository.evidence_pack.data_gaps),
            }
            for repository in repositories
        ],
    }


def _validated_measurement(
    measurement: dict[str, object], *, trajectory_status: object
) -> dict[str, object]:
    validated = trajectory_status == "completed"
    return {
        **measurement,
        "complete": measurement["complete"] is True and validated,
        "trajectory_validated": validated,
        "measurement_status": "validated_trajectory" if validated else "diagnostic_only",
    }


def _compare_rows(reviewed: dict[str, object], control: dict[str, object]) -> dict[str, object]:
    if reviewed["status"] != "completed" or control["status"] != "completed":
        return {
            "status": "incomplete_pair",
            "performance_difference": None,
            "reason": "trajectory_validation_incomplete",
        }
    return compare_continuous_accounts(
        cast(dict[str, object], reviewed["metrics"]), cast(dict[str, object], control["metrics"])
    )


async def run_continuous_experiment(
    *,
    study_root: Path,
    registration: ContinuousStudyRegistration,
    selection_panel: RegimePanel,
    windows: tuple[FrozenContinuousWindow, ...],
    profiles: tuple[ModelProviderProfile, ...],
    historical_research_templates: tuple[ResearchSourceTemplate, ...] = (),
    research_episode_deadline: datetime | None = None,
) -> dict[str, object]:
    preflight = await prepare_continuous_experiment(
        study_root=study_root,
        registration=registration,
        selection_panel=selection_panel,
        windows=windows,
        profiles=profiles,
    )
    supplied = {window.window_id: window for window in windows}
    initial_rows: list[dict[str, object]] = [
        {
            "window_id": window.window_id,
            "profile_arm": profile.arm,
            "status": "pending",
            "reason": "batch_preflight_incomplete",
            "research_coverage": _research_coverage(supplied.get(window.window_id), initial=True),
        }
        for window in registration.coverage_windows
        for profile in registration.model_profiles
    ]
    rows: list[dict[str, object]] = [
        {
            "cell_id": cell.cell_id,
            "window_id": cell.coverage_window_id,
            "profile_arm": profile.arm,
            "cadence": cadence.value,
            "status": "pending",
            "reason": "initial_coverage_unavailable",
            "metrics": None,
            "research_coverage": _research_coverage(
                supplied.get(cell.coverage_window_id), initial=False
            ),
        }
        for cell in registration.deep_cells
        for profile in registration.model_profiles
        for cadence in ContinuousCadence
    ]
    blocked_windows = {
        str(item["window_id"]) for item in cast(list[dict[str, object]], preflight["problems"])
    }
    if not (set(supplied) - blocked_windows):
        incomplete: dict[str, object] = {
            "status": "incomplete",
            "preflight": preflight,
            "initial": initial_rows,
            "rolling": rows,
            "initial_denominator": 54,
            "rolling_denominator": 72,
            "model_dispatched": False,
        }
        incomplete["artifact_hash"] = _persist(
            study_root,
            "continuous.incomplete." + canonical_hash(incomplete),
            "continuous.batch.reported",
            incomplete,
        )
        return incomplete
    providers: list[PiRuntimeProvider] = []
    accounts: list[HistoricalStreamingAccount] = []
    sources: dict[tuple[str, str], tuple[ContinuousPortfolioRuntime, ContinuousDecision]] = {}
    supplied = {window.window_id: window for window in windows}
    rolling: list[_RollingArm] = []
    try:
        stage_providers: dict[tuple[str, str], PiRuntimeProvider] = {}
        for profile, binding in zip(profiles, registration.model_profiles, strict=True):
            for stage in ("analysis_coverage", "portfolio_coverage", "rolling"):
                provider = PiRuntimeProvider(profile, budget=study_budget(study_root, stage))
                stage_providers[binding.arm, stage] = provider
                providers.append(provider)
        for row in initial_rows:
            if str(row["window_id"]) in blocked_windows:
                row.update(reason="window_source_or_baseline_incomplete")
                continue
            window_id, arm = str(row["window_id"]), str(row["profile_arm"])
            window = supplied[window_id]
            runtime = _runtime(
                study_root=study_root,
                registration=registration,
                window=window,
                profile_arm=arm,
                cadence="coverage",
                provider=stage_providers[arm, "analysis_coverage"],
                portfolio_provider=stage_providers[arm, "portfolio_coverage"],
                historical_research_templates=historical_research_templates,
                research_episode_deadline=research_episode_deadline,
            )
            accounts.append(runtime.account)
            try:
                decision = await runtime.decide(
                    window.frames[0],
                    None,
                    "continuous-initial-"
                    + canonical_hash({"batch": preflight["batch_id"], "arm": runtime.arm_id}),
                    frozenset(
                        h
                        for h in (1, 3, 5, 10, 20, 60)
                        if h
                        <= (
                            len(window.frames)
                            if any(
                                cell.coverage_window_id == window_id
                                for cell in registration.deep_cells
                            )
                            else len(window.calendar_dates)
                        )
                    ),
                    False,
                )
            except (ValueError, PermissionError, RuntimeError) as exc:
                row.update(status="incomplete", reason=type(exc).__name__)
                continue
            if isinstance(decision, PendingReview):
                row.update(
                    status="incomplete",
                    reason=decision.reason,
                    continuation_ref=decision.continuation_ref,
                )
            else:
                row.update(status="completed", reason=None, decision=decision.to_dict())
                sources[window_id, arm] = runtime, decision
        for row in rows:
            window_id, arm, cadence = (
                str(row["window_id"]),
                str(row["profile_arm"]),
                str(row["cadence"]),
            )
            source = sources.get((window_id, arm))
            if source is None:
                coverage = next(
                    item
                    for item in initial_rows
                    if item["window_id"] == window_id and item["profile_arm"] == arm
                )
                row.update(
                    reason=coverage["reason"], continuation_ref=coverage.get("continuation_ref")
                )
                continue
            window = supplied[window_id]
            runtime = _runtime(
                study_root=study_root,
                registration=registration,
                window=window,
                profile_arm=arm,
                cadence=cadence,
                provider=stage_providers[arm, "rolling"],
                source_runtime=source[0],
                historical_research_templates=historical_research_templates,
                research_episode_deadline=research_episode_deadline,
            )
            accounts.append(runtime.account)
            initial = runtime.adopt_initial(source[1], window.frames[0])
            coordinator = _coordinator(
                runtime=runtime,
                window=window,
                initial=initial,
                registration_id=registration.registration_id,
                cadence=ContinuousCadence(cadence),
            )
            rolling.append(_RollingArm(runtime, window, coordinator, row))
        # Daily frontier: no market regime consumes its whole window before its peers.
        async with AsyncExitStack() as streams:
            active = [
                (item, await streams.enter_async_context(aclosing(item.coordinator.stream())))
                for item in rolling
            ]
            for _frontier in range(
                max((len(item.window.frames) for item, _stream in active), default=0)
            ):
                for item, stream in tuple(active):
                    try:
                        report = await anext(stream)
                    except (ValueError, PermissionError, RuntimeError) as exc:
                        item.row.update(status="incomplete", reason=type(exc).__name__)
                        active.remove((item, stream))
                        await stream.aclose()
                        continue
                    item.row.update(report)
                    if report["status"] != "prefix_complete":
                        active.remove((item, stream))
                        await stream.aclose()
        for item in rolling:
            seed = item.runtime.account.results[0]
            cost = sum(
                record.record.metrics.estimated_cost_microusd
                for record in UsageLedger(item.runtime.store.index_path).records()
                if record.record.run_id.startswith(item.coordinator.episode_id + ".")
            )
            economic_seed = {
                "nav": str(seed.nav),
                "cash": str(seed.cash),
                "positions": {key: str(value) for key, value in seed.positions.items()},
            }
            measurement = measure_continuous_account(
                initial_nav=seed.nav,
                sessions=item.runtime.account.results[1:],
                expected_sessions=len(item.window.frames),
                execution_policy_hash=canonical_hash(_POLICY),
                initial_account_hash=canonical_hash(economic_seed),
                model_cost_microusd=cost,
            )
            item.row["metrics"] = _validated_measurement(
                measurement, trajectory_status=item.row["status"]
            )
        comparisons: list[dict[str, object]] = []
        for cell in registration.deep_cells:
            for profile in registration.model_profiles:
                group = [
                    row
                    for row in rows
                    if row["cell_id"] == cell.cell_id and row["profile_arm"] == profile.arm
                ]
                control = next(row for row in group if row["cadence"] == "expiry_only")
                for row in group:
                    if row is control or row["metrics"] is None or control["metrics"] is None:
                        continue
                    comparisons.append(
                        {
                            "cell_id": cell.cell_id,
                            "profile_arm": profile.arm,
                            "cadence": row["cadence"],
                            **_compare_rows(row, control),
                        }
                    )
        result: dict[str, object] = {
            "schema_version": "market-impact.continuous-experiment-report.v1",
            "batch_id": preflight["batch_id"],
            "registration_id": registration.registration_id,
            "initial": initial_rows,
            "rolling": rows,
            "comparisons": comparisons,
            "initial_denominator": 54,
            "rolling_denominator": 72,
            "status": "completed"
            if all(row["status"] == "completed" for row in (*initial_rows, *rows))
            else "incomplete",
            "budget": study_budget(study_root, "rolling").summary(),
            "live_execution": False,
        }
        result["artifact_hash"] = _persist(
            study_root,
            "continuous.result." + canonical_hash(result),
            "continuous.batch.reported",
            result,
        )
        return result
    finally:
        for account in accounts:
            account.close()
        for provider in providers:
            await provider.close()
