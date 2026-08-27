from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path
from typing import cast

import pytest

from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.regime_agent_validation import (
    RegimeAgentValidationCase,
    RegimeAgentValidationRegistration,
    build_regime_agent_validation_report,
    load_regime_agent_validation_registration,
    select_validation_checkpoints,
    write_regime_agent_validation_report,
)
from market_impact_agent.regime_evidence import RegimeCheckpoint


def _checkpoint(day: date) -> RegimeCheckpoint:
    return RegimeCheckpoint(
        case_key="case-a",
        session_date=day,
        cutoff_at=datetime.combine(day, datetime.min.time(), tzinfo=UTC),
    )


def _case_report(
    *,
    case_key: str,
    treatment_skill: str,
    checkpoints: tuple[date, ...],
    control_return: str,
    treatment_return: str,
    baseline_return: str,
    equal_sector_return: str,
    momentum_return: str,
    control_hits: int,
    treatment_hits: int,
    helpful: int,
    harmful: int,
    same: int,
    cost: int,
    prior_cost: int = 0,
) -> dict[str, object]:
    effects = tuple(
        "helpful"
        if index < helpful
        else "harmful"
        if index < helpful + harmful
        else "same_decision"
        for index in range(len(checkpoints))
    )

    def path_metrics(
        total_return: str,
        max_drawdown: str,
        *,
        baseline_comparison: bool = False,
    ) -> dict[str, object]:
        result: dict[str, object] = {
            "status": "covered",
            "session_count": 20,
            "risk_metrics_eligible": True,
            "total_return": total_return,
            "annualized_return": "0.10000000",
            "annualized_volatility": "0.20000000",
            "sharpe": "0.50000000",
            "max_drawdown": max_drawdown,
            "cvar": "-0.02000000",
            "turnover": "1.00000000",
            "modeled_cost": "0.00100000",
        }
        if baseline_comparison:
            result.update(
                {
                    "information_ratio_vs_primary": "0.10000000",
                    "upside_capture_ratio": "0.90000000",
                    "downside_loss_participation_ratio": "0.80000000",
                }
            )
        return result

    control_decisions = [
        {
            "session_date": day.isoformat(),
            "decision": "propose" if index < control_hits else "abstain",
        }
        for index, day in enumerate(checkpoints)
    ]
    treatment_decisions = [
        {
            "session_date": day.isoformat(),
            "decision": "propose" if index < treatment_hits else "abstain",
        }
        for index, day in enumerate(checkpoints)
    ]
    checkpoint_costs = tuple(
        cost // len(checkpoints) + int(index < cost % len(checkpoints))
        for index in range(len(checkpoints))
    )
    core: dict[str, object] = {
        "schema_version": "market-impact.regime-agent-experiment-report.v1",
        "case_key": case_key,
        "panel_id": "regime-panel-" + "1" * 64,
        "manifest_id": "regime-evidence-manifest-" + "2" * 64,
        "qualification_report_id": "regime-evidence-qualification-report-" + "3" * 64,
        "provider_profile_id": "model-provider-" + "4" * 64,
        "treatment_skill": treatment_skill,
        "evidence_pack_ids": [
            "evidence-pack-" + str(index + 1) * 64 for index in range(len(checkpoints))
        ],
        "checkpoint_count": len(checkpoints),
        "formal_run_count": len(checkpoints) * 6,
        "checkpoint_results": [
            {
                "session_date": day.isoformat(),
                "cutoff_at": datetime.combine(day, datetime.min.time(), tzinfo=UTC).isoformat(),
                "eligible_horizon_sessions": 5,
                "eligible_open_to_close_return": "0.01000000",
                "evidence_pack_id": "evidence-pack-" + str(index + 1) * 64,
                "report_id": "paired-report-" + str(index + 1) * 64,
                "actual_model_cost_microusd": checkpoint_costs[index],
                "arms": [
                    {
                        "arm_id": "general_control",
                        "replicate_count": 3,
                        "valid_run_count": 3,
                        "invalid_run_count": 0,
                        "propose_count": int(index < control_hits) * 3,
                        "abstain_count": int(index >= control_hits) * 3,
                        "majority_decision": ("propose" if index < control_hits else "abstain"),
                        "invalid_reasons": [],
                    },
                    {
                        "arm_id": f"general_plus_{treatment_skill.replace('-', '_')}",
                        "replicate_count": 3,
                        "valid_run_count": 3,
                        "invalid_run_count": 0,
                        "propose_count": int(index < treatment_hits) * 3,
                        "abstain_count": int(index >= treatment_hits) * 3,
                        "majority_decision": ("propose" if index < treatment_hits else "abstain"),
                        "invalid_reasons": [],
                    },
                ],
                "incremental_skill_effect": effects[index],
            }
            for index, day in enumerate(checkpoints)
        ],
        "arms": [
            {
                "arm_id": "general_control",
                "checkpoint_decisions": control_decisions,
                "directional_hit_count": control_hits,
                "directional_hit_rate": "0.33333333",
                "path_metrics": path_metrics(control_return, "-0.10000000"),
            },
            {
                "arm_id": f"general_plus_{treatment_skill.replace('-', '_')}",
                "checkpoint_decisions": treatment_decisions,
                "directional_hit_count": treatment_hits,
                "directional_hit_rate": "0.66666667",
                "path_metrics": path_metrics(treatment_return, "-0.05000000"),
            },
        ],
        "baselines": [
            {
                "baseline_id": "cash",
                "path_metrics": path_metrics("0.00000000", "0.00000000", baseline_comparison=True),
            },
            {
                "baseline_id": "primary_buy_and_hold",
                "path_metrics": path_metrics(
                    baseline_return, "-0.12000000", baseline_comparison=True
                ),
            },
            {
                "baseline_id": "equal_sector_buy_and_hold",
                "path_metrics": path_metrics(
                    equal_sector_return, "-0.09000000", baseline_comparison=True
                ),
            },
            {
                "baseline_id": "lagged_sector_momentum",
                "path_metrics": path_metrics(
                    momentum_return, "-0.08000000", baseline_comparison=True
                ),
            },
        ],
        "market_context": {
            "period_start": checkpoints[0].isoformat(),
            "period_end": checkpoints[-1].isoformat(),
            "main_indices": [{"series_id": "000300.SH", "open_to_close_return": baseline_return}],
            "industry_summary": {
                "industry_count": 1,
                "positive_industry_count": 1,
                "median_open_to_close_return": "0.01000000",
                "leaders": [{"series_id": "801010.SI", "open_to_close_return": "0.01000000"}],
                "laggards": [{"series_id": "801010.SI", "open_to_close_return": "0.01000000"}],
                "all_industries": [
                    {"series_id": "801010.SI", "open_to_close_return": "0.01000000"}
                ],
            },
        },
        "skill_increment": {
            "helpful_checkpoint_count": helpful,
            "harmful_checkpoint_count": harmful,
            "same_decision_checkpoint_count": same,
        },
        "cost": {
            "formal_model_cost_microusd": cost,
            "prior_invalid_or_superseded_diagnostic_cost_microusd": prior_cost,
            "all_actual_model_cost_microusd": cost + prior_cost,
            "hard_cap_microusd": 20_000_000,
            "within_budget": True,
        },
        "limitations": ["synthetic unit fixture"],
        "inference_eligible": False,
        "broker_reachability": False,
        "execution_capability": "none",
    }
    return {
        **core,
        "report_id": f"regime-agent-experiment-report-{canonical_hash(core)}",
    }


def test_validation_checkpoint_selection_is_frozen_first_middle_last() -> None:
    days = tuple(date(2024, 1, day) for day in range(2, 10))
    selected = select_validation_checkpoints(
        tuple(_checkpoint(day) for day in days),
        window_start=days[1],
        window_end=days[-2],
        checkpoint_count=3,
    )

    assert tuple(item.session_date for item in selected) == (days[1], days[4], days[-2])

    with pytest.raises(ValueError, match="at least three"):
        select_validation_checkpoints(
            tuple(_checkpoint(day) for day in days[:2]),
            window_start=days[0],
            window_end=days[1],
            checkpoint_count=3,
        )


def test_multi_case_report_rejects_missing_cases_and_aggregates_without_compounding(
    tmp_path: Path,
) -> None:
    case_a_dates = (date(2020, 2, 3), date(2020, 3, 2), date(2020, 3, 23))
    case_b_dates = (date(2024, 9, 24), date(2024, 9, 30), date(2024, 10, 8))
    registration = RegimeAgentValidationRegistration.build(
        version="1.0.0",
        dataset_id="market-regime-dataset-" + "1" * 64,
        dataset_hash="1" * 64,
        study_registration_id="regime-study-registration-" + "2" * 64,
        study_registration_hash="2" * 64,
        panel_id="regime-panel-" + "1" * 64,
        manifest_id="regime-evidence-manifest-" + "2" * 64,
        qualification_report_id="regime-evidence-qualification-report-" + "3" * 64,
        provider_profile_id="model-provider-" + "4" * 64,
        replicate_count=3,
        total_cost_cap_microusd=20_000_000,
        outcomes_opened=True,
        cases=(
            RegimeAgentValidationCase(
                case_key="case-a",
                treatment_skill="expectations-base-rates",
                window_start=case_a_dates[0],
                window_end=case_a_dates[-1],
                checkpoints=case_a_dates,
            ),
            RegimeAgentValidationCase(
                case_key="case-b",
                treatment_skill="narrative-diffusion-assessment",
                window_start=case_b_dates[0],
                window_end=case_b_dates[-1],
                checkpoints=case_b_dates,
            ),
        ),
    )
    reports = (
        _case_report(
            case_key="case-a",
            treatment_skill="expectations-base-rates",
            checkpoints=case_a_dates,
            control_return="-0.20000000",
            treatment_return="-0.05000000",
            baseline_return="-0.25000000",
            equal_sector_return="-0.10000000",
            momentum_return="-0.02000000",
            control_hits=1,
            treatment_hits=2,
            helpful=2,
            harmful=0,
            same=1,
            cost=100_000,
            prior_cost=20_000,
        ),
        _case_report(
            case_key="case-b",
            treatment_skill="narrative-diffusion-assessment",
            checkpoints=case_b_dates,
            control_return="0.10000000",
            treatment_return="0.30000000",
            baseline_return="0.25000000",
            equal_sector_return="0.20000000",
            momentum_return="0.35000000",
            control_hits=1,
            treatment_hits=3,
            helpful=2,
            harmful=0,
            same=1,
            cost=120_000,
            prior_cost=30_000,
        ),
    )

    result = build_regime_agent_validation_report(
        registration=registration,
        case_reports=reports,
    )

    assert result["case_count"] == 2
    assert result["checkpoint_count"] == 6
    assert result["formal_run_count"] == 36
    assert result["aggregate"] == {
        "control_directional_hit_count": 2,
        "routed_skill_directional_hit_count": 5,
        "checkpoint_count": 6,
        "control_directional_hit_rate": "0.33333333",
        "routed_skill_directional_hit_rate": "0.83333333",
        "control_mean_case_return": "-0.05000000",
        "routed_skill_mean_case_return": "0.12500000",
        "primary_baseline_mean_case_return": "0.00000000",
        "equal_sector_baseline_mean_case_return": "0.05000000",
        "lagged_sector_momentum_mean_case_return": "0.16500000",
        "routed_skill_case_win_count_vs_control": 2,
        "routed_skill_case_win_count_vs_primary": 2,
        "routed_skill_case_win_count_vs_equal_sector": 2,
        "routed_skill_case_win_count_vs_lagged_sector_momentum": 0,
        "helpful_checkpoint_count": 4,
        "harmful_checkpoint_count": 0,
        "same_decision_checkpoint_count": 2,
        "control_worst_case_return": "-0.20000000",
        "routed_skill_worst_case_return": "-0.05000000",
        "control_worst_case_max_drawdown": "-0.10000000",
        "routed_skill_worst_case_max_drawdown": "-0.05000000",
    }
    assert result["cost"] == {
        "formal_model_cost_microusd": 220_000,
        "prior_invalid_or_superseded_diagnostic_cost_microusd": 50_000,
        "all_actual_model_cost_microusd": 270_000,
        "hard_cap_microusd": 20_000_000,
        "within_budget": True,
    }
    assert result["inference_eligible"] is False
    assert result["execution_capability"] == "none"
    path = write_regime_agent_validation_report(result, root=tmp_path)
    assert path.name == f"{result['report_id']}.json"
    assert path.stat().st_mode & 0o777 == 0o600

    with pytest.raises(ValueError, match="exact registered case set"):
        build_regime_agent_validation_report(
            registration=registration,
            case_reports=reports[:1],
        )

    provider_drift = dict(reports[0])
    provider_drift["provider_profile_id"] = "model-provider-" + "9" * 64
    provider_core = {key: value for key, value in provider_drift.items() if key != "report_id"}
    provider_drift["report_id"] = f"regime-agent-experiment-report-{canonical_hash(provider_core)}"
    with pytest.raises(ValueError, match="registered contract"):
        build_regime_agent_validation_report(
            registration=registration,
            case_reports=(provider_drift, reports[1]),
        )

    cost_drift = dict(reports[0])
    drifted_cost = dict(cast(dict[str, object], cost_drift["cost"]))
    drifted_cost["all_actual_model_cost_microusd"] = 100_000
    cost_drift["cost"] = drifted_cost
    cost_core = {key: value for key, value in cost_drift.items() if key != "report_id"}
    cost_drift["report_id"] = f"regime-agent-experiment-report-{canonical_hash(cost_core)}"
    with pytest.raises(ValueError, match="cost ledger does not reconcile"):
        build_regime_agent_validation_report(
            registration=registration,
            case_reports=(cost_drift, reports[1]),
        )


def test_validation_registration_round_trips_and_rejects_identity_tampering(
    tmp_path: Path,
) -> None:
    dates = (date(2024, 9, 24), date(2024, 9, 30), date(2024, 10, 8))
    cases = tuple(
        RegimeAgentValidationCase(
            case_key=f"case-{suffix}",
            treatment_skill="narrative-diffusion-assessment",
            window_start=dates[0],
            window_end=dates[-1],
            checkpoints=dates,
        )
        for suffix in ("a", "b")
    )
    registration = RegimeAgentValidationRegistration.build(
        version="1.0.0",
        dataset_id="market-regime-dataset-" + "1" * 64,
        dataset_hash="1" * 64,
        study_registration_id="regime-study-registration-" + "2" * 64,
        study_registration_hash="2" * 64,
        panel_id="regime-panel-" + "1" * 64,
        manifest_id="regime-evidence-manifest-" + "2" * 64,
        qualification_report_id="regime-evidence-qualification-report-" + "3" * 64,
        provider_profile_id="model-provider-" + "3" * 64,
        replicate_count=3,
        total_cost_cap_microusd=10_000_000,
        outcomes_opened=True,
        cases=cases,
    )
    path = tmp_path / "registration.json"
    path.write_text(json.dumps(registration.to_dict()), encoding="utf-8")

    loaded = load_regime_agent_validation_registration(path)

    assert loaded == registration
    tampered = registration.to_dict()
    tampered["total_cost_cap_microusd"] = 9_000_000
    path.write_text(json.dumps(tampered), encoding="utf-8")
    with pytest.raises(ValueError, match="validation_id does not match"):
        load_regime_agent_validation_registration(path)


def test_validation_registration_allows_20_usd_but_rejects_more() -> None:
    dates = (date(2024, 9, 24), date(2024, 9, 30), date(2024, 10, 8))
    cases = tuple(
        RegimeAgentValidationCase(
            case_key=f"case-{suffix}",
            treatment_skill="narrative-diffusion-assessment",
            window_start=dates[0],
            window_end=dates[-1],
            checkpoints=dates,
        )
        for suffix in ("a", "b")
    )
    registration = RegimeAgentValidationRegistration.build(
        version="1.0.0",
        dataset_id="market-regime-dataset-" + "1" * 64,
        dataset_hash="1" * 64,
        study_registration_id="regime-study-registration-" + "2" * 64,
        study_registration_hash="2" * 64,
        panel_id="regime-panel-" + "1" * 64,
        manifest_id="regime-evidence-manifest-" + "2" * 64,
        qualification_report_id="regime-evidence-qualification-report-" + "3" * 64,
        provider_profile_id="model-provider-" + "3" * 64,
        replicate_count=3,
        total_cost_cap_microusd=20_000_000,
        outcomes_opened=True,
        cases=cases,
    )

    assert registration.total_cost_cap_microusd == 20_000_000
    with pytest.raises(ValueError, match="within 20 USD"):
        RegimeAgentValidationRegistration.build(
            version="1.0.0",
            dataset_id="market-regime-dataset-" + "1" * 64,
            dataset_hash="1" * 64,
            study_registration_id="regime-study-registration-" + "2" * 64,
            study_registration_hash="2" * 64,
            panel_id="regime-panel-" + "1" * 64,
            manifest_id="regime-evidence-manifest-" + "2" * 64,
            qualification_report_id="regime-evidence-qualification-report-" + "3" * 64,
            provider_profile_id="model-provider-" + "3" * 64,
            replicate_count=3,
            total_cost_cap_microusd=20_000_001,
            outcomes_opened=True,
            cases=cases,
        )


def test_public_validation_registration_freezes_six_cases_under_20_usd() -> None:
    registration = load_regime_agent_validation_registration(
        Path("examples/research/regime-agent-validation-v1.json")
    )

    assert len(registration.cases) == 6
    assert registration.total_cost_cap_microusd == 20_000_000
    assert sum(len(case.checkpoints) for case in registration.cases) == 18
    assert registration.outcomes_opened is True
