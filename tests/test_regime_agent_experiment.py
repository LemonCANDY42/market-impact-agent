from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import cast

import pytest

from market_impact_agent.market_regimes import (
    MarketRegimeCase,
    RegimePanel,
    RegimeSeries,
    RegimeTaxonomy,
    ValidatedRegimePanel,
)
from market_impact_agent.regime_agent_experiment import (
    CompletedRegimeCheckpointExperiment,
    aggregate_checkpoint_arm,
    assert_checkpoint_qualified,
    build_regime_agent_experiment_report,
    evaluate_checkpoint_exposure_path,
    method_evidence_bindings,
    write_regime_agent_experiment_report,
)
from market_impact_agent.regime_evidence import RegimeCheckpoint
from market_impact_agent.regime_study import RegimeBaselineProtocol


def _arm_report(*decisions: str, horizon: int = 2) -> dict[str, object]:
    runs: list[dict[str, object]] = []
    coverage: list[dict[str, object]] = []
    for index, decision in enumerate(decisions, start=1):
        run_id = f"run-{index}"
        candidate: list[dict[str, object]] = []
        if decision == "propose":
            candidate = [
                {
                    "target_id": "broad-market-a",
                    "direction": "up",
                    "horizon_sessions": horizon,
                    "confidence": 0.7,
                }
            ]
        runs.append(
            {
                "run_id": run_id,
                "status": "completed",
                "decision": decision,
                "candidates": candidate,
            }
        )
        coverage.append(
            {
                "run_id": run_id,
                "evidence_coverage_complete": True,
                "pattern_coverage_complete": True,
            }
        )
    return {"arm_id": "general_control", "runs": runs, "coverage": coverage}


def test_checkpoint_arm_requires_a_valid_two_of_three_majority() -> None:
    result = aggregate_checkpoint_arm(
        _arm_report("propose", "propose", "abstain"),
        target_id="broad-market-a",
        eligible_horizon_sessions=2,
    )

    assert result["majority_decision"] == "propose"
    assert result["valid_run_count"] == 3
    assert result["propose_count"] == 2

    wrong_horizon = aggregate_checkpoint_arm(
        _arm_report("propose", "propose", "abstain", horizon=1),
        target_id="broad-market-a",
        eligible_horizon_sessions=2,
    )
    assert wrong_horizon["majority_decision"] == "invalid"
    assert wrong_horizon["invalid_run_count"] == 2


def test_checkpoint_exposure_path_switches_only_at_registered_opens() -> None:
    rows = (
        {"trade_date": "20240924", "open": "100", "close": "110"},
        {"trade_date": "20240925", "open": "110", "close": "121"},
        {"trade_date": "20240930", "open": "121", "close": "115"},
        {"trade_date": "20241008", "open": "115", "close": "126.5"},
    )
    decisions = {
        date(2024, 9, 24): "propose",
        date(2024, 9, 30): "abstain",
        date(2024, 10, 8): "propose",
    }

    result = evaluate_checkpoint_exposure_path(
        rows=rows,
        start=date(2024, 9, 24),
        end=date(2024, 10, 8),
        checkpoint_decisions=decisions,
        transaction_cost_bps_one_way=Decimal("0"),
        annualization_sessions=252,
        minimum_risk_sessions=20,
        risk_free_rate_annual=Decimal("0"),
        cvar_confidence=Decimal("0.95"),
    )

    assert result["total_return"] == "0.33100000"
    assert result["turnover"] == "3.00000000"
    assert result["max_drawdown"] == "0.00000000"
    assert result["risk_metrics_eligible"] is False
    assert result["sharpe"] is None


def test_checkpoint_qualification_gate_is_case_local_and_fail_closed() -> None:
    report = {
        "manifest_id": "manifest-a",
        "cases": [
            {
                "case_key": "case-a",
                "all_checkpoints_ready": False,
                "checkpoints": [
                    {
                        "session_date": "2024-09-24",
                        "cutoff_at": "2024-09-24T01:25:00Z",
                        "ready": True,
                    },
                    {
                        "session_date": "2024-09-25",
                        "cutoff_at": "2024-09-25T01:25:00Z",
                        "ready": False,
                    },
                ],
            }
        ],
    }

    checkpoint = assert_checkpoint_qualified(
        report,
        case_key="case-a",
        session_date=date(2024, 9, 24),
        manifest_id="manifest-a",
    )
    assert checkpoint["ready"] is True

    with pytest.raises(ValueError, match="not qualified"):
        assert_checkpoint_qualified(
            report,
            case_key="case-a",
            session_date=date(2024, 9, 25),
            manifest_id="manifest-a",
        )


def test_method_evidence_bindings_follow_the_frozen_treatment_requirements() -> None:
    bindings = method_evidence_bindings(
        required_evidence=("price_or_market_context", "consensus_or_positioning"),
        evidence_refs_by_type={
            "price_or_market_context": ("market-context", "industry-rotation"),
            "consensus_or_positioning": ("positioning-flow",),
            "timestamped_narrative_corpus": ("timestamped-news-corpus",),
        },
    )

    assert tuple(item.evidence_type for item in bindings) == (
        "price_or_market_context",
        "consensus_or_positioning",
    )
    assert bindings[0].evidence_refs == ("market-context", "industry-rotation")
    assert bindings[1].evidence_refs == ("positioning-flow",)

    with pytest.raises(ValueError, match="reference_class"):
        method_evidence_bindings(
            required_evidence=("reference_class",),
            evidence_refs_by_type={},
        )


def test_complete_regime_experiment_reports_skill_increment_and_quant_paths(
    tmp_path: Path,
) -> None:
    dates = (date(2024, 1, 2), date(2024, 1, 3), date(2024, 1, 4))
    primary_rows: tuple[dict[str, object], ...] = (
        {"trade_date": "20231228", "open": "90", "close": "92"},
        {"trade_date": "20231229", "open": "92", "close": "95"},
        {"trade_date": "20240102", "open": "100", "close": "110"},
        {"trade_date": "20240103", "open": "110", "close": "121"},
        {"trade_date": "20240104", "open": "130", "close": "117"},
    )
    sector_rows: tuple[dict[str, object], ...] = (
        {"trade_date": "20231228", "open": "80", "close": "82"},
        {"trade_date": "20231229", "open": "82", "close": "84"},
        {"trade_date": "20240102", "open": "100", "close": "105"},
        {"trade_date": "20240103", "open": "105", "close": "110"},
        {"trade_date": "20240104", "open": "110", "close": "108"},
    )
    panel = ValidatedRegimePanel(
        path=Path("panel.json"),
        panel_id="regime-panel-" + "1" * 64,
        panel_hash="1" * 64,
        panel=RegimePanel(
            dataset_id="market-regime-dataset-" + "2" * 64,
            dataset_hash="2" * 64,
            provider_id="tushare-http",
            provider_version="test",
            historical_vintage="retrieved_historical_not_original_vintage",
            retrieved_at=datetime(2026, 1, 1, tzinfo=UTC),
            industry_taxonomy=RegimeTaxonomy(
                source="SW2021",
                level="L1",
                fields=(),
                rows=(),
                retrieved_at=datetime(2026, 1, 1, tzinfo=UTC),
                content_hash="3" * 64,
            ),
            series=(
                RegimeSeries(
                    "000300.SH", "market", "000300.SH", "index_daily", "price", primary_rows
                ),
                RegimeSeries("sector-a", "industry", "801010.SI", "sw_daily", "price", sector_rows),
            ),
            proxy_resolution=(("sector-a", "801010.SI"),),
        ),
    )
    market_case = MarketRegimeCase(
        case_key="case-a",
        path_start=dates[0],
        event_anchor=None,
        tradable_start=dates[0],
        end=dates[-1],
        axes={},
        capability_targets=("event_latency", "crowding_control"),
        primary_market_index="000300.SH",
        required_market_indices=("000300.SH",),
        required_industry_proxies=("sector-a",),
        source_refs=("source",),
    )
    baseline = RegimeBaselineProtocol(
        annualization_sessions=252,
        minimum_risk_sessions=20,
        risk_free_rate_annual=Decimal("0"),
        cvar_confidence=Decimal("0.95"),
        transaction_cost_bps_one_way=Decimal("0"),
        rebalance_frequency="monthly_first_session",
        momentum_lookback_sessions=1,
        momentum_top_k=1,
        strategies=(
            "cash",
            "primary_buy_and_hold",
            "equal_sector_buy_and_hold",
            "lagged_sector_momentum",
        ),
    )
    controls = ("propose", "abstain", "propose")
    treatments = ("propose", "propose", "abstain")
    completed: list[CompletedRegimeCheckpointExperiment] = []
    for index, day in enumerate(dates):
        report = {
            "report_id": "method-skill-ablation-report-" + str(index) * 64,
            "registration_id": f"registration-{index}",
            "diagnostic_valid": True,
            "replicate_count": 3,
            "arms": [
                _arm_report(*([controls[index]] * 3), horizon=1),
                {
                    **_arm_report(*([treatments[index]] * 3), horizon=1),
                    "arm_id": "general_plus_narrative_diffusion_assessment",
                },
            ],
            "cost": {"ledger_actual_microusd": 100},
            "only_treatment_difference": "narrative-diffusion-assessment",
            "outcomes_visible_to_agent": False,
            "execution_capability": "none",
        }
        completed.append(
            CompletedRegimeCheckpointExperiment(
                checkpoint=RegimeCheckpoint(
                    case_key="case-a",
                    session_date=day,
                    cutoff_at=datetime.combine(day, datetime.min.time(), tzinfo=UTC),
                ),
                eligible_horizon_sessions=1,
                evidence_pack_id=f"evidence-pack-{index}",
                registration={
                    "registration_id": f"registration-{index}",
                    "evidence_pack_id": f"evidence-pack-{index}",
                    "provider_profile_id": "provider-a",
                    "only_treatment_difference": "narrative-diffusion-assessment",
                },
                report=report,
            )
        )

    result = build_regime_agent_experiment_report(
        validated_panel=panel,
        market_case=market_case,
        baseline_protocol=baseline,
        manifest_id="regime-evidence-manifest-" + "4" * 64,
        qualification_report_id="regime-evidence-qualification-report-" + "5" * 64,
        completed=tuple(completed),
        prior_invalid_diagnostic_cost_microusd=50,
        total_cost_cap_microusd=10_000_000,
    )

    assert result["formal_run_count"] == 18
    cost = cast(dict[str, object], result["cost"])
    assert cost["formal_model_cost_microusd"] == 300
    assert cost["all_actual_model_cost_microusd"] == 350
    assert result["skill_increment"] == {
        "helpful_checkpoint_count": 2,
        "harmful_checkpoint_count": 0,
        "same_decision_checkpoint_count": 1,
    }
    arms = {
        cast(str, item["arm_id"]): item for item in cast(list[dict[str, object]], result["arms"])
    }
    assert arms["general_control"]["directional_hit_rate"] == "0.33333333"
    assert (
        arms["general_plus_narrative_diffusion_assessment"]["directional_hit_rate"] == "1.00000000"
    )
    baselines = {
        cast(str, item["baseline_id"]): item
        for item in cast(list[dict[str, object]], result["baselines"])
    }
    assert tuple(baselines) == baseline.strategies
    equal_sector_path = cast(
        dict[str, object], baselines["equal_sector_buy_and_hold"]["path_metrics"]
    )
    momentum_path = cast(dict[str, object], baselines["lagged_sector_momentum"]["path_metrics"])
    assert equal_sector_path["total_return"] == "0.08000000"
    assert momentum_path["total_return"] == "0.08000000"
    assert momentum_path["information_ratio_vs_primary"] is None
    assert result["inference_eligible"] is False
    assert result["execution_capability"] == "none"
    path = write_regime_agent_experiment_report(result, root=tmp_path)
    assert path.name == f"{result['report_id']}.json"
    assert path.stat().st_mode & 0o777 == 0o600
