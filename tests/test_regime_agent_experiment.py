from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import cast

import pytest

from market_impact_agent.agent_contracts import EvidencePack, EvidenceReference, canonical_hash
from market_impact_agent.market_regimes import (
    MarketRegimeCase,
    RegimePanel,
    RegimeSeries,
    RegimeTaxonomy,
    ValidatedRegimePanel,
)
from market_impact_agent.method_skills import MethodEvidenceBinding, MethodEvidenceDeclaration
from market_impact_agent.paired_skill_ablation_contract import paired_skill_common_input_hash
from market_impact_agent.regime_agent_experiment import (
    CompletedRegimeCheckpointExperiment,
    aggregate_checkpoint_arm,
    assert_checkpoint_qualified,
    build_regime_agent_experiment_report,
    evaluate_checkpoint_exposure_path,
    method_evidence_bindings,
    validate_paired_experiment_identity,
    write_regime_agent_experiment_report,
)
from market_impact_agent.regime_evidence import RegimeCheckpoint
from market_impact_agent.regime_modeled_pit import (
    load_regime_modeled_pit_agent_validation_registration,
)
from market_impact_agent.regime_study import RegimeBaselineProtocol
from market_impact_agent.research import EvidenceTier


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


def _paired_artifacts(
    *,
    index: int,
    day: date,
    arms: list[dict[str, object]],
    eligible_horizon_sessions: int,
) -> tuple[
    EvidencePack,
    MethodEvidenceDeclaration,
    dict[str, object],
    dict[str, object],
]:
    as_of = datetime.combine(day, datetime.min.time(), tzinfo=UTC)
    evidence_pack = EvidencePack.build(
        event_id=f"event-{index}",
        as_of=as_of,
        research_question="Should the broad market rise over the registered horizon?",
        evidence=(
            EvidenceReference(
                evidence_id="market-context",
                claim_id="market-context",
                source_ref="test-source",
                source_tier=EvidenceTier.REGULATED,
                available_at=as_of,
                content_hash=str(index + 1) * 64,
                summary="Frozen market context.",
            ),
        ),
        pattern_packs=(),
        allowed_targets=("broad-market-a",),
    )
    declaration_core: dict[str, object] = {
        "schema_version": "market-impact.method-evidence-declaration.v1",
        "evidence_pack_id": evidence_pack.pack_id,
        "evidence_pack_hash": canonical_hash(evidence_pack.to_dict()),
        "evidence_types": [
            {
                "evidence_type": "price_or_market_context",
                "evidence_refs": ["market-context"],
                "pattern_pack_refs": [],
            }
        ],
        "outcomes_opened": True,
    }
    declaration = MethodEvidenceDeclaration(
        declaration_id=f"method-evidence-{canonical_hash(declaration_core)}",
        evidence_pack_id=evidence_pack.pack_id,
        evidence_pack_hash=canonical_hash(evidence_pack.to_dict()),
        bindings=(
            MethodEvidenceBinding(
                evidence_type="price_or_market_context",
                evidence_refs=("market-context",),
                pattern_pack_refs=(),
            ),
        ),
        outcomes_opened=True,
    )
    route_id = "method-skill-route-" + "6" * 64
    common_input_hash = paired_skill_common_input_hash(
        evidence_pack,
        declaration,
        eligible_horizon_sessions=eligible_horizon_sessions,
    )
    registration_core: dict[str, object] = {
        "schema_version": "market-impact.method-skill-ablation.v2",
        "experiment_id": f"experiment-{index}",
        "registered_at": as_of.isoformat().replace("+00:00", "Z"),
        "provider_profile_id": "model-provider-" + "1" * 64,
        "provider_profile_hash": "2" * 64,
        "method_catalog_id": "method-skill-catalog-" + "3" * 64,
        "method_evidence_declaration_id": declaration.declaration_id,
        "method_evidence_declaration_hash": declaration.declaration_hash,
        "evidence_pack_id": evidence_pack.pack_id,
        "evidence_pack_hash": canonical_hash(evidence_pack.to_dict()),
        "control_skills": ["evidence-core"],
        "treatment_skills": ["evidence-core", "narrative-diffusion-assessment"],
        "control_manifest_hashes": ["4" * 64],
        "treatment_manifest_hashes": ["4" * 64, "5" * 64],
        "method_route_id": route_id,
        "routing_context": {
            "market_state": "unclassified",
            "narrative_salience": "contested",
            "analysis_needs": ["narrative_diffusion"],
            "available_evidence": ["price_or_market_context"],
            "outcomes_opened": True,
        },
        "replicate_count": 3,
        "run_order": "interleaved_by_replicate_then_arm",
        "common_input_hash": common_input_hash,
        "cpa_pricing": {
            "schema_version": "market-impact.cpa-pricing-snapshot.v1",
            "keeper_version": "test",
            "model": "test-model",
            "captured_at": as_of.isoformat().replace("+00:00", "Z"),
            "pricing_style": "openai",
            "prompt_microusd_per_million_tokens": 1,
            "completion_microusd_per_million_tokens": 1,
            "cache_read_microusd_per_million_tokens": 0,
            "cache_write_microusd_per_million_tokens": 0,
            "price_multiplier": "1",
            "rules": [],
            "source_origin": "http://127.0.0.1:8080",
        },
        "cost_estimate": {
            "agent_run_count": 6,
            "provider_request_upper_bound": 6,
            "raw_max_cost_microusd": 6,
            "safety_multiplier": "1.25",
            "guarded_max_cost_microusd": 8,
            "hard_cap_microusd": 1000,
            "within_budget": True,
        },
        "outcomes_opened": True,
        "inference_eligible": False,
        "execution_capability": "none",
    }
    registration = {
        **registration_core,
        "registration_id": f"method-skill-ablation-{canonical_hash(registration_core)}",
    }
    report_core: dict[str, object] = {
        "schema_version": "market-impact.method-skill-ablation-report.v2",
        "experiment_id": registration_core["experiment_id"],
        "registration_id": registration["registration_id"],
        "registration_hash": canonical_hash(registration_core),
        "provider_profile_id": registration_core["provider_profile_id"],
        "provider_profile_hash": registration_core["provider_profile_hash"],
        "method_route": {"route_id": route_id},
        "diagnostic_valid": True,
        "replicate_count": 3,
        "arms": arms,
        "cost": {"ledger_actual_microusd": 100},
        "only_treatment_difference": "narrative-diffusion-assessment",
        "outcomes_visible_to_agent": False,
        "execution_capability": "none",
    }
    report = {
        **report_core,
        "report_id": f"method-skill-ablation-report-{canonical_hash(report_core)}",
    }
    return evidence_pack, declaration, registration, report


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
        "schema_version": "market-impact.regime-evidence-qualification-report.v1",
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

    modeled = {
        **report,
        "schema_version": "market-impact.regime-modeled-pit-qualification-report.v1",
    }
    with pytest.raises(ValueError, match="strict PIT"):
        assert_checkpoint_qualified(
            modeled,
            case_key="case-a",
            session_date=date(2024, 9, 24),
            manifest_id="manifest-a",
        )


def test_modeled_pit_agent_validation_registration_is_content_addressed() -> None:
    registration = load_regime_modeled_pit_agent_validation_registration(
        Path("examples/research/regime-modeled-pit-agent-validation-v1.json")
    )

    assert registration["validation_id"] == (
        "regime-modeled-pit-agent-validation-"
        "e43a3e221fdebb8a99189c7ed35e7977e7dba680d43cf093d7de9c712b14e91e"
    )
    assert registration["replicate_count"] == 3
    assert registration["strict_pit_eligible"] is False
    assert registration["execution_capability"] == "none"


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
        evidence_pack, declaration, registration, report = _paired_artifacts(
            index=index,
            day=day,
            arms=[
                _arm_report(*([controls[index]] * 3), horizon=1),
                {
                    **_arm_report(*([treatments[index]] * 3), horizon=1),
                    "arm_id": "general_plus_narrative_diffusion_assessment",
                },
            ],
            eligible_horizon_sessions=1,
        )
        completed.append(
            CompletedRegimeCheckpointExperiment(
                checkpoint=RegimeCheckpoint(
                    case_key="case-a",
                    session_date=day,
                    cutoff_at=datetime.combine(day, datetime.min.time(), tzinfo=UTC),
                ),
                eligible_horizon_sessions=1,
                evidence_pack=evidence_pack,
                method_evidence_declaration=declaration,
                registration=registration,
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
    checkpoint_results = cast(list[dict[str, object]], result["checkpoint_results"])
    assert (
        checkpoint_results[0]["common_input_hash"] == completed[0].registration["common_input_hash"]
    )
    path = write_regime_agent_experiment_report(result, root=tmp_path)
    assert path.name == f"{result['report_id']}.json"
    assert path.stat().st_mode & 0o777 == 0o600


def test_complete_regime_experiment_rejects_tampered_report_and_wrong_horizon() -> None:
    day = date(2024, 1, 2)
    evidence_pack, declaration, registration, report = _paired_artifacts(
        index=0,
        day=day,
        arms=[
            _arm_report("abstain", "abstain", "abstain", horizon=2),
            {
                **_arm_report("abstain", "abstain", "abstain", horizon=2),
                "arm_id": "general_plus_narrative_diffusion_assessment",
            },
        ],
        eligible_horizon_sessions=2,
    )
    checkpoint = RegimeCheckpoint(
        case_key="case-a",
        session_date=day,
        cutoff_at=datetime.combine(day, datetime.min.time(), tzinfo=UTC),
    )
    tampered = dict(report)
    tampered["diagnostic_valid"] = False
    completed = CompletedRegimeCheckpointExperiment(
        checkpoint=checkpoint,
        eligible_horizon_sessions=2,
        evidence_pack=evidence_pack,
        method_evidence_declaration=declaration,
        registration=registration,
        report=tampered,
    )
    with pytest.raises(ValueError, match="not content-addressed"):
        validate_paired_experiment_identity(completed)

    wrong_horizon = CompletedRegimeCheckpointExperiment(
        checkpoint=checkpoint,
        eligible_horizon_sessions=1,
        evidence_pack=evidence_pack,
        method_evidence_declaration=declaration,
        registration=registration,
        report=report,
    )
    with pytest.raises(ValueError, match="registered horizon or common input drifted"):
        validate_paired_experiment_identity(wrong_horizon)
