from __future__ import annotations

import inspect
import json
from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Protocol, cast

import pytest
from jsonschema import Draft202012Validator, FormatChecker
from jsonschema.exceptions import ValidationError

from market_impact_agent.backtests import (
    SimulationSpec,
    StrategyBacktestArm,
    StrategyBacktestRequestTemplate,
    StrategyBacktestVariant,
)
from market_impact_agent.strategy_validation import (
    ProspectiveDenominatorStore,
    ProspectiveValidationCohort,
    StrategyBaselineDefinition,
    StrategyCaseAuthorityBinding,
    StrategyCaseDefinition,
    StrategyCaseOutcome,
    StrategyCaseRole,
    StrategyCaseRunAuthorityStore,
    StrategyEvidenceLane,
    StrategyPortfolioMetrics,
    StrategyValidationDisposition,
    StrategyValidationProgram,
    StrategyValidationRegistration,
    evaluate_strategy_validation,
    revalidate_strategy_validation_report,
)

ROOT = Path(__file__).resolve().parents[1]
HASH = "a" * 64
ECONOMIC_GATE_FIELDS = (
    "candidate_after_cost_return_positive",
    "primary_baseline_beaten_with_confidence",
    "stressed_cost_return_positive",
    "maximum_drawdown_passed",
    "cvar_passed",
    "sharpe_passed",
    "sortino_passed",
    "downside_loss_passed",
    "event_concentration_passed",
    "leave_one_event_passed",
    "leave_one_regime_passed",
)


def _variant(
    arm: StrategyBacktestArm,
    *,
    baseline_id: str | None = None,
    strategy_ref: str = "event-impact-hold.v1",
    target_selection_ref: str = "candidate-targets.v1",
) -> StrategyBacktestVariant:
    request_template = StrategyBacktestRequestTemplate(
        market="CN",
        instrument_ids=("600028.XSHG",),
        horizons_sessions=(3,),
        signal_side="buy",
    )
    return StrategyBacktestVariant.build(
        arm=arm,
        baseline_id=baseline_id,
        strategy_ref=strategy_ref,
        target_selection_ref=target_selection_ref,
        request_template=request_template,
        simulation=SimulationSpec(
            data_granularity="daily_bar.v1",
            book_type="top_of_book",
            fill_model="next_executable_open_one_tick_slippage.v1",
            fee_model="cn_a_share_commission_plus_sell_stamp_tax.v1",
            venue_ruleset="xshg_cash_equity_t_plus_one.v1",
            base_currency="CNY",
            starting_cash=Decimal("1000000"),
            random_seed=0,
        ),
    )


class Validator(Protocol):
    def validate(self, instance: object) -> None: ...


def definitions(
    program: StrategyValidationProgram,
) -> tuple[StrategyCaseDefinition, ...]:
    development = tuple(
        StrategyCaseDefinition(
            case_id=f"development-{index:02d}",
            root_event_id=f"development-root-{index:02d}",
            regime=f"development-regime-{(index - 1) % 4 + 1}",
            role=StrategyCaseRole.DEVELOPMENT,
        )
        for index in range(1, 9)
    )
    if program is StrategyValidationProgram.HISTORICAL_STRICT:
        evaluation = tuple(
            StrategyCaseDefinition(
                case_id=f"holdout-{index:02d}",
                root_event_id=f"holdout-root-{index:02d}",
                regime=f"regime-{(index - 1) % 6 + 1}",
                role=StrategyCaseRole.HISTORICAL_HOLDOUT,
            )
            for index in range(1, 25)
        )
    else:
        evaluation = tuple(
            StrategyCaseDefinition(
                case_id=f"prospective-{index:02d}",
                root_event_id=f"prospective-root-{index:02d}",
                regime=f"regime-{(index - 1) % 4 + 1}",
                role=StrategyCaseRole.PROSPECTIVE_CONFIRMATION,
            )
            for index in range(1, 31)
        )
    return tuple(sorted((*development, *evaluation), key=lambda item: item.case_id))


def registration(
    program: StrategyValidationProgram = StrategyValidationProgram.HISTORICAL_STRICT,
    *,
    case_definitions: tuple[StrategyCaseDefinition, ...] | None = None,
    prospective_cohort: ProspectiveValidationCohort | None = None,
    strategy_epoch_id: str = "strategy-epoch-v1",
) -> StrategyValidationRegistration:
    selected_definitions = definitions(program) if case_definitions is None else case_definitions
    if program is StrategyValidationProgram.PROSPECTIVE_CONFIRMATION and prospective_cohort is None:
        _, prospective_cohort = denominator_store_for(selected_definitions, strategy_epoch_id)
    candidate = _variant(StrategyBacktestArm.CANDIDATE)
    baseline_variants = (
        _variant(
            StrategyBacktestArm.PRIMARY_BASELINE,
            baseline_id="broad-etf-hold",
            strategy_ref="broad-etf-hold.v1",
            target_selection_ref="broad-etf.v1",
        ),
        _variant(
            StrategyBacktestArm.PRIMARY_BASELINE,
            baseline_id="cash",
            strategy_ref="cash-no-action.v1",
            target_selection_ref="cash.v1",
        ),
        _variant(
            StrategyBacktestArm.PRIMARY_BASELINE,
            baseline_id="dca",
            strategy_ref="dca.v1",
            target_selection_ref="dca.v1",
        ),
    )
    return StrategyValidationRegistration.build(
        strategy_epoch_id=strategy_epoch_id,
        program=program,
        model_profile_hash=HASH,
        prompt_hash=HASH,
        skill_catalog_hash=HASH,
        tool_manifest_hash=HASH,
        universe_hash=HASH,
        cost_model_hash=HASH,
        fill_model_hash=HASH,
        candidate_variant=candidate,
        primary_baseline_id="cash",
        baseline_definitions=(
            StrategyBaselineDefinition(
                "broad-etf-hold",
                "b" * 64,
                baseline_variants[0].configuration_hash,
                baseline_variants[0],
            ),
            StrategyBaselineDefinition(
                "cash", "d" * 64, baseline_variants[1].configuration_hash, baseline_variants[1]
            ),
            StrategyBaselineDefinition(
                "dca", "f" * 64, baseline_variants[2].configuration_hash, baseline_variants[2]
            ),
        ),
        development_selection_evidence_hash="1" * 64,
        case_definitions=selected_definitions,
        prospective_cohort=prospective_cohort,
        created_at=datetime(2026, 9, 2, tzinfo=UTC),
    )


def cohort_for(
    case_definitions: tuple[StrategyCaseDefinition, ...],
    strategy_epoch_id: str = "strategy-epoch-v1",
) -> ProspectiveValidationCohort:
    return denominator_store_for(case_definitions, strategy_epoch_id)[1]


def denominator_store_for(
    case_definitions: tuple[StrategyCaseDefinition, ...],
    strategy_epoch_id: str = "strategy-epoch-v1",
) -> tuple[ProspectiveDenominatorStore, ProspectiveValidationCohort]:
    store = ProspectiveDenominatorStore(":memory:")
    window_id = store.register_window(
        strategy_epoch_id=strategy_epoch_id,
        qualification_policy_hash="7" * 64,
        opened_at=datetime(2026, 8, 1, tzinfo=UTC),
        cutoff_at=datetime(2026, 9, 1, tzinfo=UTC),
    )
    prospective = tuple(
        item for item in case_definitions if item.role is StrategyCaseRole.PROSPECTIVE_CONFIRMATION
    )
    for index, item in enumerate(prospective, start=1):
        digest = f"{index:064x}"
        store.append_qualified_event(
            window_id=window_id,
            case_id=item.case_id,
            root_event_id=item.root_event_id,
            qualified_at=datetime(2026, 8, 15, tzinfo=UTC),
            trigger_admission_id=f"prospective-trigger-admission-{digest}",
            trigger_admission_hash=digest,
        )
    cohort = store.seal(window_id, sealed_at=datetime(2026, 9, 2, tzinfo=UTC))
    return store, cohort


def binding(
    active: StrategyValidationRegistration,
    case_id: str,
    *,
    lane: StrategyEvidenceLane,
    metrics: StrategyPortfolioMetrics,
    candidate_return: str = "0.04",
    baseline_return: str = "0.01",
    selected_run_started_at: datetime = datetime(2026, 9, 2, tzinfo=UTC),
    run_manifest_hash: str = "5" * 64,
    qualified: bool = True,
    admitted: bool = True,
    nonempty: bool = True,
) -> StrategyCaseAuthorityBinding:
    return StrategyCaseAuthorityBinding.build(
        registration=active,
        case_id=case_id,
        evidence_lane=lane,
        data_snapshot_hash="2" * 64,
        evidence_lineage_hash="3" * 64,
        qualification_report_hash="4" * 64,
        run_manifest_hash=run_manifest_hash,
        admission_hash="6" * 64,
        selected_run_started_at=selected_run_started_at,
        candidate_net_return=Decimal(candidate_return),
        primary_baseline_net_return=Decimal(baseline_return),
        candidate_absolute_pnl=Decimal("100"),
        portfolio_metrics_hash=metrics.metrics_hash,
        qualification_passed=qualified,
        admission_passed=admitted,
        nonempty_execution=nonempty,
    )


def authority_and_outcomes(
    active: StrategyValidationRegistration,
    *,
    lane: StrategyEvidenceLane | None = None,
    nonempty_count: int | None = None,
    candidate_return: str = "0.04",
    baseline_return: str = "0.01",
    metrics: StrategyPortfolioMetrics | None = None,
) -> tuple[StrategyCaseRunAuthorityStore, tuple[StrategyCaseOutcome, ...]]:
    if metrics is None:
        metrics = portfolio()
    expected_lane = (
        StrategyEvidenceLane.STRICT_PIT
        if active.program is StrategyValidationProgram.HISTORICAL_STRICT
        else StrategyEvidenceLane.PROSPECTIVE
    )
    selected_lane = expected_lane if lane is None else lane
    if nonempty_count is None:
        nonempty_count = len(active.evaluation_cases)
    bindings = tuple(
        binding(
            active,
            item.case_id,
            lane=selected_lane,
            metrics=metrics,
            candidate_return=candidate_return,
            baseline_return=baseline_return,
            nonempty=index < nonempty_count,
        )
        for index, item in enumerate(active.evaluation_cases)
    )
    outcomes = tuple(
        StrategyCaseOutcome(
            case_id=item.case_id,
            root_event_id=item.root_event_id,
            regime=item.regime,
            candidate_net_return=Decimal(candidate_return),
            primary_baseline_net_return=Decimal(baseline_return),
            candidate_absolute_pnl=Decimal("100"),
        )
        for item in active.evaluation_cases
    )
    authority = StrategyCaseRunAuthorityStore(":memory:")
    for item in bindings:
        authority.register_completed_run(active, item)
    return authority, outcomes


def selected_bindings(
    active: StrategyValidationRegistration,
    authority: StrategyCaseRunAuthorityStore,
) -> tuple[StrategyCaseAuthorityBinding, ...]:
    return tuple(
        authority.canonical_selection(active, item.case_id).selected_binding
        for item in active.evaluation_cases
    )


def portfolio(
    *, drawdown: str = "0.08", liquidity_utilization: str = "0.002"
) -> StrategyPortfolioMetrics:
    return StrategyPortfolioMetrics(
        candidate_net_return=Decimal("0.20"),
        primary_baseline_net_return=Decimal("0.10"),
        candidate_max_drawdown=Decimal(drawdown),
        primary_baseline_max_drawdown=Decimal("0.10"),
        candidate_cvar95=Decimal("0.08"),
        primary_baseline_cvar95=Decimal("0.10"),
        candidate_sharpe=Decimal("1.4"),
        primary_baseline_sharpe=Decimal("1.0"),
        candidate_sortino=Decimal("1.8"),
        primary_baseline_sortino=Decimal("1.2"),
        candidate_stressed_net_return=Decimal("0.04"),
        primary_baseline_stressed_net_return=Decimal("-0.01"),
        candidate_turnover=Decimal("2.5"),
        primary_baseline_turnover=Decimal("1.0"),
        candidate_adverse_excursion=Decimal("0.04"),
        primary_baseline_adverse_excursion=Decimal("0.08"),
        candidate_liquidity_utilization=Decimal(liquidity_utilization),
        primary_baseline_liquidity_utilization=Decimal("0.003"),
        avoided_loss=Decimal("0.05"),
        false_avoidance_opportunity_cost=Decimal("0.01"),
    )


def validate_schema(name: str, payload: dict[str, Any]) -> None:
    schema = json.loads((ROOT / "schemas" / name).read_text(encoding="utf-8"))
    validator = Draft202012Validator(schema, format_checker=FormatChecker())
    cast(Validator, validator).validate(payload)


def test_historical_registration_and_report_are_authoritative_and_schema_valid() -> None:
    active = registration()
    authority, outcomes = authority_and_outcomes(active)
    report = evaluate_strategy_validation(active, outcomes, portfolio(), authority=authority)

    assert report.disposition is StrategyValidationDisposition.ACCEPTED
    assert report.program is StrategyValidationProgram.HISTORICAL_STRICT
    assert report.evidence_lane is StrategyEvidenceLane.STRICT_PIT
    assert report.independent_case_count == 24
    assert report.regime_count == 6
    assert report.gate_results.all_passed is True
    assert report.execution_capability == "none"
    revalidate_strategy_validation_report(
        report, active, outcomes, portfolio(), authority=authority
    )
    validate_schema("strategy-validation-registration.schema.json", active.to_dict())
    validate_schema("strategy-validation-report.schema.json", report.to_dict())


def test_registration_rejects_root_reuse_across_development_and_holdout() -> None:
    cases = list(definitions(StrategyValidationProgram.HISTORICAL_STRICT))
    development = next(item for item in cases if item.role is StrategyCaseRole.DEVELOPMENT)
    holdout_index = next(
        index
        for index, item in enumerate(cases)
        if item.role is StrategyCaseRole.HISTORICAL_HOLDOUT
    )
    cases[holdout_index] = replace(cases[holdout_index], root_event_id=development.root_event_id)

    with pytest.raises(ValueError, match="must not reuse root events"):
        registration(case_definitions=tuple(cases))


def test_registration_rejects_candidate_reused_as_a_baseline_configuration() -> None:
    active = registration()
    candidate = active.candidate_variant
    relabeled = StrategyBacktestVariant.build(
        arm=StrategyBacktestArm.PRIMARY_BASELINE,
        baseline_id=active.primary_baseline_id,
        strategy_ref=candidate.strategy_ref,
        target_selection_ref="attacker-metadata-only-baseline.v1",
        request_template=candidate.request_template,
        simulation=SimulationSpec(
            data_granularity=candidate.data_granularity,
            book_type=candidate.book_type,
            fill_model=candidate.fill_model,
            fee_model=candidate.fee_model,
            venue_ruleset=candidate.venue_ruleset,
            base_currency=candidate.base_currency,
            starting_cash=candidate.starting_cash,
            random_seed=candidate.random_seed,
        ),
    )
    duplicate_baseline = StrategyBaselineDefinition(
        active.primary_baseline_id,
        active.primary_baseline.definition_hash,
        relabeled.configuration_hash,
        relabeled,
    )

    with pytest.raises(ValueError, match=r"candidate and baseline.*distinct"):
        replace(active, baseline_definitions=(duplicate_baseline,))


@pytest.mark.parametrize("field", ["root_event_id", "regime"])
def test_evaluator_treats_relabelled_frozen_case_as_inconclusive(field: str) -> None:
    active = registration()
    authority, original = authority_and_outcomes(active)
    changed = list(original)
    changed[0] = replace(changed[0], **{field: "attacker-relabel"})

    report = evaluate_strategy_validation(active, tuple(changed), portfolio(), authority=authority)

    assert report.disposition is StrategyValidationDisposition.INCONCLUSIVE
    assert report.gate_results.complete_denominator is False
    assert report.candidate_mean_case_return is None
    assert f"{field.removesuffix('_id')}_mismatch:{changed[0].case_id}" in report.reasons


@pytest.mark.parametrize("invalid_kind", ["duplicate", "unexpected"])
def test_evaluator_does_not_run_economics_on_invalid_case_denominator(
    invalid_kind: str,
) -> None:
    active = registration()
    authority, outcomes = authority_and_outcomes(active)
    invalid = (
        (*outcomes, outcomes[0])
        if invalid_kind == "duplicate"
        else (
            *outcomes,
            replace(
                outcomes[0],
                case_id="unexpected-case",
                root_event_id="unexpected-root",
            ),
        )
    )

    report = evaluate_strategy_validation(active, invalid, portfolio(), authority=authority)

    assert report.disposition is StrategyValidationDisposition.INCONCLUSIVE
    assert report.gate_results.complete_denominator is False
    assert report.candidate_mean_case_return is None
    for gate in ECONOMIC_GATE_FIELDS:
        assert getattr(report.gate_results, gate) is False
    assert any(reason.startswith(f"{invalid_kind}_") for reason in report.reasons)


def test_baseline_definition_configuration_and_development_selection_are_frozen() -> None:
    active = registration()
    changed_variant = _variant(
        StrategyBacktestArm.PRIMARY_BASELINE,
        baseline_id=active.baseline_definitions[0].baseline_id,
        strategy_ref=active.baseline_definitions[0].variant.strategy_ref,
        target_selection_ref="changed-baseline-targets.v1",
    )
    changed_baselines = (
        replace(
            active.baseline_definitions[0],
            configuration_hash=changed_variant.configuration_hash,
            variant=changed_variant,
        ),
        *active.baseline_definitions[1:],
    )
    changed = StrategyValidationRegistration.build(
        strategy_epoch_id=active.strategy_epoch_id,
        program=active.program,
        model_profile_hash=active.model_profile_hash,
        prompt_hash=active.prompt_hash,
        skill_catalog_hash=active.skill_catalog_hash,
        tool_manifest_hash=active.tool_manifest_hash,
        universe_hash=active.universe_hash,
        cost_model_hash=active.cost_model_hash,
        fill_model_hash=active.fill_model_hash,
        candidate_variant=active.candidate_variant,
        primary_baseline_id=active.primary_baseline_id,
        baseline_definitions=changed_baselines,
        development_selection_evidence_hash="8" * 64,
        case_definitions=active.case_definitions,
        created_at=active.created_at,
    )

    assert changed.registration_id != active.registration_id
    assert active.to_dict()["development_selection_evidence_hash"] == "1" * 64


def test_evidence_lane_cannot_be_asserted_by_evaluator_caller() -> None:
    assert "evidence_lane" not in inspect.signature(evaluate_strategy_validation).parameters
    active = registration()
    authority, outcomes = authority_and_outcomes(active, lane=StrategyEvidenceLane.MODELED_PIT)

    report = evaluate_strategy_validation(active, outcomes, portfolio(), authority=authority)

    assert report.disposition is StrategyValidationDisposition.INCONCLUSIVE
    assert report.evidence_lane is StrategyEvidenceLane.STRICT_PIT
    assert any(reason.startswith("evidence_authority_lane_mismatch:") for reason in report.reasons)


def test_outcome_cannot_select_an_authority_binding() -> None:
    assert "authority_binding_id" not in StrategyCaseOutcome.__dataclass_fields__


def test_favorable_replay_cannot_replace_earliest_complete_losing_run() -> None:
    active = registration()
    authority, favorable_outcomes = authority_and_outcomes(active)
    case_id = favorable_outcomes[0].case_id
    losing = binding(
        active,
        case_id,
        lane=StrategyEvidenceLane.STRICT_PIT,
        metrics=portfolio(),
        candidate_return="-0.40",
        baseline_return="-0.10",
        selected_run_started_at=datetime(2026, 9, 1, tzinfo=UTC),
        run_manifest_hash="7" * 64,
    )
    favorable_replay = binding(
        active,
        case_id,
        lane=StrategyEvidenceLane.STRICT_PIT,
        metrics=portfolio(),
        selected_run_started_at=datetime(2026, 9, 2, tzinfo=UTC),
        run_manifest_hash="8" * 64,
    )
    canonical_authority = StrategyCaseRunAuthorityStore(":memory:")
    canonical_authority.register_completed_run(active, losing)
    canonical_authority.register_completed_run(active, favorable_replay)
    for item in selected_bindings(active, authority)[1:]:
        canonical_authority.register_completed_run(active, item)

    with pytest.raises(ValueError, match="different epoch, run outcome, or portfolio"):
        evaluate_strategy_validation(
            active, favorable_outcomes, portfolio(), authority=canonical_authority
        )

    canonical_outcomes = (
        replace(
            favorable_outcomes[0],
            candidate_net_return=Decimal("-0.40"),
            primary_baseline_net_return=Decimal("-0.10"),
        ),
        *favorable_outcomes[1:],
    )
    report = evaluate_strategy_validation(
        active, canonical_outcomes, portfolio(), authority=canonical_authority
    )

    assert report.disposition is StrategyValidationDisposition.REJECTED
    assert f"downside_loss_not_halved:{case_id}" in report.reasons


def test_case_run_authority_cannot_promote_a_changed_strategy_epoch() -> None:
    original = registration()
    authority, outcomes = authority_and_outcomes(original)
    changed_epoch = registration(strategy_epoch_id="strategy-epoch-v2")

    with pytest.raises(KeyError, match="no completed canonical strategy run"):
        evaluate_strategy_validation(changed_epoch, outcomes, portfolio(), authority=authority)


def test_promotion_boundary_rejects_custom_latest_favorable_authority() -> None:
    active = registration()
    authority, outcomes = authority_and_outcomes(active)

    class LatestFavorableAuthority:
        def canonical_selection(self, *_: object) -> object:
            return authority.canonical_selection(active, outcomes[0].case_id)

    with pytest.raises(TypeError, match="concrete durable run authority store"):
        evaluate_strategy_validation(
            active,
            outcomes,
            portfolio(),
            authority=cast(Any, LatestFavorableAuthority()),
        )


def test_completed_run_authority_is_durable_and_selects_from_full_set(
    tmp_path: Path,
) -> None:
    active = registration()
    original_authority, _ = authority_and_outcomes(active)
    original = selected_bindings(active, original_authority)[0]
    earlier = binding(
        active,
        original.case_id,
        lane=StrategyEvidenceLane.STRICT_PIT,
        metrics=portfolio(),
        candidate_return="-0.25",
        selected_run_started_at=datetime(2026, 9, 1, tzinfo=UTC),
        run_manifest_hash="7" * 64,
    )
    path = tmp_path / "strategy-run-authority.sqlite3"
    authority = StrategyCaseRunAuthorityStore(path)
    authority.register_completed_run(active, original)
    authority.register_completed_run(active, earlier)
    authority.close()

    reopened = StrategyCaseRunAuthorityStore(path)
    selection = reopened.canonical_selection(active, original.case_id)

    assert selection.selected_binding.binding_id == earlier.binding_id
    assert {item.binding_id for item in selection.eligible_bindings} == {
        original.binding_id,
        earlier.binding_id,
    }


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("paired_critical_value", Decimal("1.700")),
        ("maximum_drawdown_ratio", Decimal("0.79")),
        ("maximum_cvar_ratio", Decimal("0.84")),
        ("maximum_downside_loss_ratio", Decimal("0.49")),
        ("maximum_single_event_share", Decimal("0.19")),
        ("run_selection_policy", "latest_complete_run_v1"),
    ],
)
def test_registration_rejects_relaxed_v1_gate_or_run_policy(
    field: str, value: Decimal | str
) -> None:
    active = registration()

    with pytest.raises(ValueError, match=r"frozen v1 value|run-selection policy"):
        replace(active, **{field: value})


@pytest.mark.parametrize("attack", ["return", "portfolio"])
def test_caller_cannot_replace_authority_bound_run_results(attack: str) -> None:
    active = registration()
    authority, outcomes = authority_and_outcomes(active)
    changed_outcomes = outcomes
    changed_portfolio = portfolio()
    if attack == "return":
        changed_outcomes = (
            replace(outcomes[0], candidate_net_return=Decimal("4.00")),
            *outcomes[1:],
        )
    else:
        changed_portfolio = portfolio(liquidity_utilization="0.004")

    with pytest.raises(ValueError, match="different epoch, run outcome, or portfolio"):
        evaluate_strategy_validation(
            active, changed_outcomes, changed_portfolio, authority=authority
        )


@pytest.mark.parametrize(
    ("qualified", "admitted", "reason_prefix"),
    [
        (False, True, "qualification_not_passed"),
        (True, False, "admission_not_passed"),
    ],
)
def test_failed_reopened_authority_is_inconclusive(
    qualified: bool, admitted: bool, reason_prefix: str
) -> None:
    active = registration()
    authority, outcomes = authority_and_outcomes(active)
    bindings = selected_bindings(active, authority)
    failed = binding(
        active,
        bindings[0].case_id,
        lane=bindings[0].evidence_lane,
        metrics=portfolio(),
        qualified=qualified,
        admitted=admitted,
    )
    authority = StrategyCaseRunAuthorityStore(":memory:")
    authority.register_completed_run(active, failed)
    for item in bindings[1:]:
        authority.register_completed_run(active, item)

    report = evaluate_strategy_validation(active, outcomes, portfolio(), authority=authority)

    assert report.disposition is StrategyValidationDisposition.INCONCLUSIVE
    assert f"{reason_prefix}:{failed.case_id}" in report.reasons


@pytest.mark.parametrize(
    "field",
    [
        "data_snapshot_hash",
        "evidence_lineage_hash",
        "qualification_report_hash",
        "run_manifest_hash",
        "admission_hash",
    ],
)
def test_every_reopened_artifact_hash_changes_the_authority_binding(field: str) -> None:
    active = registration()
    values: dict[str, object] = {
        "registration": active,
        "case_id": "holdout-01",
        "evidence_lane": StrategyEvidenceLane.STRICT_PIT,
        "data_snapshot_hash": "2" * 64,
        "evidence_lineage_hash": "3" * 64,
        "qualification_report_hash": "4" * 64,
        "run_manifest_hash": "5" * 64,
        "admission_hash": "6" * 64,
        "selected_run_started_at": datetime(2026, 9, 2, tzinfo=UTC),
        "candidate_net_return": Decimal("0.04"),
        "primary_baseline_net_return": Decimal("0.01"),
        "candidate_absolute_pnl": Decimal("100"),
        "portfolio_metrics_hash": portfolio().metrics_hash,
    }
    original = StrategyCaseAuthorityBinding.build(**cast(Any, values))
    values[field] = "9" * 64
    changed = StrategyCaseAuthorityBinding.build(**cast(Any, values))

    assert changed.binding_id != original.binding_id


def test_prospective_confirmation_accepts_30_clusters_20_nonempty_and_4_regimes() -> None:
    active = registration(StrategyValidationProgram.PROSPECTIVE_CONFIRMATION)
    authority, outcomes = authority_and_outcomes(active, nonempty_count=20)
    denominator_store, cohort = denominator_store_for(
        active.case_definitions, active.strategy_epoch_id
    )
    assert cohort.cohort_id == active.prospective_cohort_id

    report = evaluate_strategy_validation(
        active,
        outcomes,
        portfolio(),
        authority=authority,
        prospective_denominator_store=denominator_store,
    )

    assert report.disposition is StrategyValidationDisposition.ACCEPTED
    assert report.program is StrategyValidationProgram.PROSPECTIVE_CONFIRMATION
    assert report.evidence_lane is StrategyEvidenceLane.PROSPECTIVE
    assert report.independent_case_count == 30
    assert report.nonempty_execution_count == 20
    assert report.regime_count == 4
    assert report.prospective_cohort_seal_hash == active.prospective_cohort_seal_hash
    assert report.execution_capability == "none"
    validate_schema("strategy-validation-registration.schema.json", active.to_dict())
    validate_schema("strategy-validation-report.schema.json", report.to_dict())


def test_prospective_confirmation_with_19_nonempty_is_inconclusive() -> None:
    active = registration(StrategyValidationProgram.PROSPECTIVE_CONFIRMATION)
    authority, outcomes = authority_and_outcomes(active, nonempty_count=19)
    denominator_store, _ = denominator_store_for(active.case_definitions, active.strategy_epoch_id)

    report = evaluate_strategy_validation(
        active,
        outcomes,
        portfolio(),
        authority=authority,
        prospective_denominator_store=denominator_store,
    )

    assert report.disposition is StrategyValidationDisposition.INCONCLUSIVE
    assert report.reasons == ("insufficient_nonempty_executions",)


def test_prospective_registration_cannot_omit_a_qualifying_cohort_case() -> None:
    selected = definitions(StrategyValidationProgram.PROSPECTIVE_CONFIRMATION)
    omitted = StrategyCaseDefinition(
        case_id="prospective-31",
        root_event_id="prospective-root-31",
        regime="regime-3",
        role=StrategyCaseRole.PROSPECTIVE_CONFIRMATION,
    )
    complete_definitions = tuple(sorted((*selected, omitted), key=lambda item: item.case_id))
    denominator_store, complete_cohort = denominator_store_for(complete_definitions)
    attacker_registration = registration(
        StrategyValidationProgram.PROSPECTIVE_CONFIRMATION,
        case_definitions=selected,
        prospective_cohort=complete_cohort,
    )
    authority, outcomes = authority_and_outcomes(attacker_registration, nonempty_count=20)

    with pytest.raises(ValueError, match="omitted or substituted"):
        evaluate_strategy_validation(
            attacker_registration,
            outcomes,
            portfolio(),
            authority=authority,
            prospective_denominator_store=denominator_store,
        )


def test_prospective_denominator_is_durable_complete_and_append_closed(
    tmp_path: Path,
) -> None:
    selected = definitions(StrategyValidationProgram.PROSPECTIVE_CONFIRMATION)
    path = tmp_path / "prospective-denominator.sqlite3"
    store = ProspectiveDenominatorStore(path)
    window_id = store.register_window(
        strategy_epoch_id="strategy-epoch-v1",
        qualification_policy_hash="7" * 64,
        opened_at=datetime(2026, 8, 1, tzinfo=UTC),
        cutoff_at=datetime(2026, 9, 1, tzinfo=UTC),
    )
    prospective = tuple(
        item for item in selected if item.role is StrategyCaseRole.PROSPECTIVE_CONFIRMATION
    )
    for index, item in enumerate(prospective, start=1):
        digest = f"{index:064x}"
        store.append_qualified_event(
            window_id=window_id,
            case_id=item.case_id,
            root_event_id=item.root_event_id,
            qualified_at=datetime(2026, 8, 15, tzinfo=UTC),
            trigger_admission_id=f"prospective-trigger-admission-{digest}",
            trigger_admission_hash=digest,
        )
    cohort = store.seal(window_id, sealed_at=datetime(2026, 9, 2, tzinfo=UTC))
    store.close()
    active = registration(
        StrategyValidationProgram.PROSPECTIVE_CONFIRMATION,
        case_definitions=selected,
        prospective_cohort=cohort,
    )

    reopened = ProspectiveDenominatorStore(path)
    assert reopened.reopen_for_registration(active) == cohort
    with pytest.raises(ValueError, match="append-closed"):
        reopened.append_qualified_event(
            window_id=window_id,
            case_id="prospective-31",
            root_event_id="prospective-root-31",
            qualified_at=datetime(2026, 8, 16, tzinfo=UTC),
            trigger_admission_id=f"prospective-trigger-admission-{'f' * 64}",
            trigger_admission_hash="f" * 64,
        )


def test_promotion_boundary_rejects_caller_denominator_object() -> None:
    active = registration(StrategyValidationProgram.PROSPECTIVE_CONFIRMATION)
    authority, outcomes = authority_and_outcomes(active, nonempty_count=20)

    class CallerDenominator:
        def reopen_for_registration(self, _: object) -> object:
            return cohort_for(active.case_definitions, active.strategy_epoch_id)

    with pytest.raises(TypeError, match="concrete durable denominator store"):
        evaluate_strategy_validation(
            active,
            outcomes,
            portfolio(),
            authority=authority,
            prospective_denominator_store=cast(Any, CallerDenominator()),
        )


def test_omitted_losing_prospective_case_keeps_confirmation_inconclusive() -> None:
    base = definitions(StrategyValidationProgram.PROSPECTIVE_CONFIRMATION)
    losing_definition = StrategyCaseDefinition(
        case_id="prospective-31",
        root_event_id="prospective-root-31",
        regime="regime-3",
        role=StrategyCaseRole.PROSPECTIVE_CONFIRMATION,
    )
    complete_definitions = tuple(sorted((*base, losing_definition), key=lambda item: item.case_id))
    denominator_store, cohort = denominator_store_for(complete_definitions)
    active = registration(
        StrategyValidationProgram.PROSPECTIVE_CONFIRMATION,
        case_definitions=complete_definitions,
        prospective_cohort=cohort,
    )
    economically_rejected = portfolio(drawdown="0.2")
    authority, outcomes = authority_and_outcomes(
        active, metrics=economically_rejected, nonempty_count=20
    )
    bindings = selected_bindings(active, authority)
    losing = binding(
        active,
        losing_definition.case_id,
        lane=StrategyEvidenceLane.PROSPECTIVE,
        metrics=economically_rejected,
        candidate_return="-0.50",
        baseline_return="-0.10",
        nonempty=False,
    )
    authority = StrategyCaseRunAuthorityStore(":memory:")
    for item in (*bindings[:-1], losing):
        authority.register_completed_run(active, item)

    report = evaluate_strategy_validation(
        active,
        outcomes[:-1],
        economically_rejected,
        authority=authority,
        prospective_denominator_store=denominator_store,
    )

    assert report.disposition is StrategyValidationDisposition.INCONCLUSIVE
    assert report.candidate_mean_case_return is None
    assert report.reasons == ("missing_evaluation_case:prospective-31",)


def test_missing_historical_cases_remain_inconclusive_without_effectiveness_metrics() -> None:
    active = registration()
    authority, outcomes = authority_and_outcomes(active)
    report = evaluate_strategy_validation(active, outcomes[:3], portfolio(), authority=authority)

    assert report.disposition is StrategyValidationDisposition.INCONCLUSIVE
    assert report.candidate_mean_case_return is None
    for gate in ECONOMIC_GATE_FIELDS:
        assert getattr(report.gate_results, gate) is False
    assert any(reason.startswith("missing_evaluation_case:") for reason in report.reasons)


def test_failed_drawdown_gate_rejects_otherwise_profitable_strategy() -> None:
    active = registration()
    metrics = portfolio(drawdown="0.081")
    authority, outcomes = authority_and_outcomes(active, metrics=metrics)
    report = evaluate_strategy_validation(active, outcomes, metrics, authority=authority)

    assert report.disposition is StrategyValidationDisposition.REJECTED
    assert report.reasons == ("maximum_drawdown_not_improved",)


def test_downside_case_must_halve_the_registered_baseline_loss() -> None:
    active = registration()
    authority, outcomes = authority_and_outcomes(active)
    changed = list(outcomes)
    changed[0] = replace(
        changed[0],
        candidate_net_return=Decimal("-0.03"),
        primary_baseline_net_return=Decimal("-0.04"),
    )
    bindings = selected_bindings(active, authority)
    failed = binding(
        active,
        changed[0].case_id,
        lane=StrategyEvidenceLane.STRICT_PIT,
        metrics=portfolio(),
        candidate_return="-0.03",
        baseline_return="-0.04",
    )
    authority = StrategyCaseRunAuthorityStore(":memory:")
    authority.register_completed_run(active, failed)
    for item in bindings[1:]:
        authority.register_completed_run(active, item)

    report = evaluate_strategy_validation(active, tuple(changed), portfolio(), authority=authority)

    assert report.disposition is StrategyValidationDisposition.REJECTED
    assert f"downside_loss_not_halved:{changed[0].case_id}" in report.reasons


def test_decimal_serialization_is_always_fixed_point() -> None:
    metrics = portfolio(liquidity_utilization="4E-7")

    assert metrics.to_dict()["candidate_liquidity_utilization"] == "0.0000004"
    assert "E" not in json.dumps(metrics.to_dict())


def test_registration_schema_rejects_case_without_frozen_root_regime_or_role() -> None:
    payload = registration().to_dict()
    first = cast(list[dict[str, str]], payload["case_definitions"])[0]
    first.pop("root_event_id")

    with pytest.raises(ValidationError):
        validate_schema("strategy-validation-registration.schema.json", payload)


def test_report_schema_rejects_historical_acceptance_with_prospective_lane() -> None:
    active = registration()
    authority, outcomes = authority_and_outcomes(active)
    payload = evaluate_strategy_validation(
        active, outcomes, portfolio(), authority=authority
    ).to_dict()
    payload["evidence_lane"] = StrategyEvidenceLane.PROSPECTIVE.value

    with pytest.raises(ValidationError):
        validate_schema("strategy-validation-report.schema.json", payload)


@pytest.mark.parametrize(
    "schema_name",
    [
        "strategy-validation-registration.schema.json",
        "strategy-validation-report.schema.json",
    ],
)
def test_strategy_validation_artifacts_cannot_grant_execution(schema_name: str) -> None:
    active = registration()
    if schema_name.endswith("registration.schema.json"):
        payload = active.to_dict()
    else:
        authority, outcomes = authority_and_outcomes(active)
        payload = evaluate_strategy_validation(
            active, outcomes, portfolio(), authority=authority
        ).to_dict()
    payload["execution_capability"] = "paper"

    with pytest.raises(ValidationError):
        validate_schema(schema_name, payload)


def test_outcome_identity_is_independent_of_input_order() -> None:
    active = registration()
    authority, outcomes = authority_and_outcomes(active)
    forward = evaluate_strategy_validation(active, outcomes, portfolio(), authority=authority)
    reverse = evaluate_strategy_validation(
        active, tuple(reversed(outcomes)), portfolio(), authority=authority
    )

    assert reverse.outcomes_hash == forward.outcomes_hash
    assert reverse.authority_bindings_hash == forward.authority_bindings_hash
    assert reverse.report_id == forward.report_id
