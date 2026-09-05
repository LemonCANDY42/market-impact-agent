from __future__ import annotations

import json
from dataclasses import replace
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import cast

import pytest

from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.market_regimes import (
    MarketRegimeCase,
    MarketRegimeDataset,
    RegimePanel,
    RegimeSeries,
    RegimeTaxonomy,
)
from market_impact_agent.method_skills import load_method_skill_catalog
from market_impact_agent.regime_study import (
    assess_regime_study_readiness,
    evaluate_regime_case_baselines,
    evaluate_regime_study_baselines,
    load_regime_study_registration,
    write_regime_study_baseline_report,
)

CATALOG = Path("examples/research/famous-method-skill-catalog-v1.json")


def _dataset() -> MarketRegimeDataset:
    case = MarketRegimeCase(
        case_key="synthetic-long-cycle",
        path_start=date(2020, 1, 2),
        event_anchor=None,
        tradable_start=date(2020, 1, 2),
        end=date(2020, 1, 6),
        axes={
            "path_direction": "mixed",
            "path_speed": "unclassified",
            "volatility": "high",
            "drawdown": "material",
            "recovery": "partial",
            "narrative_salience": "contested",
            "causal_complexity": "multi_factor",
            "causal_directness": "indirect",
        },
        capability_targets=("rotation_selection", "whipsaw_control"),
        primary_market_index="000300.SH",
        required_market_indices=("000300.SH",),
        required_industry_proxies=("sw2021_computer",),
        source_refs=("synthetic-source",),
    )
    return MarketRegimeDataset(
        dataset_id="market-regime-dataset-" + "1" * 64,
        dataset_hash="1" * 64,
        version="test-v1",
        detector={
            "primary_index": "000300.SH",
            "direction_short_sessions": 2,
            "direction_long_sessions": 3,
            "volatility_sessions": 2,
            "fast_abs_z_threshold": "1.0",
            "feature_lag": "through_previous_session",
        },
        main_market_indices=("000300.SH",),
        industry_proxy_catalog=(
            {
                "proxy_id": "sw2021_computer",
                "source": "SW2021",
                "industry_name": "计算机",
                "tushare_code": "801750.SI",
            },
        ),
        cases=(case,),
    )


def _registration_payload(dataset: MarketRegimeDataset) -> dict[str, object]:
    catalog = load_method_skill_catalog(CATALOG)
    core: dict[str, object] = {
        "schema_version": "market-impact.regime-study-registration.v1",
        "version": "test-v1",
        "dataset_id": dataset.dataset_id,
        "dataset_hash": dataset.dataset_hash,
        "method_catalog_id": catalog.catalog_id,
        "method_catalog_hash": catalog.catalog_hash,
        "outcomes_opened": True,
        "source_catalog": [
            {
                "source_id": "tushare-market",
                "category": "market_price",
                "provider_id": "tushare-http",
                "source_tier": "regulated",
                "acquisition_mode": "implemented_retrieved_history",
                "point_in_time_authority": False,
                "evidence_types": ["price_or_market_context", "valuation_or_price"],
                "license_note": "Private token-backed historical price rows.",
            },
            {
                "source_id": "tushare-industry",
                "category": "industry_price",
                "provider_id": "tushare-http",
                "source_tier": "regulated",
                "acquisition_mode": "implemented_retrieved_history",
                "point_in_time_authority": False,
                "evidence_types": ["price_or_market_context"],
                "license_note": "Private token-backed SW2021 index rows.",
            },
            {
                "source_id": "official-archive",
                "category": "official_context",
                "provider_id": "official-web-archive",
                "source_tier": "official",
                "acquisition_mode": "planned_archive_capture",
                "point_in_time_authority": False,
                "evidence_types": ["new_evidence", "fundamental_feedback"],
                "license_note": "Store hashes and permitted extracts only.",
            },
            {
                "source_id": "macro-vintage",
                "category": "macro_vintage",
                "provider_id": "official-statistics-archive",
                "source_tier": "official",
                "acquisition_mode": "planned_archive_capture",
                "point_in_time_authority": False,
                "evidence_types": ["reference_class", "cash_flow_or_earning_power"],
                "license_note": "Bind each release and revision vintage.",
            },
            {
                "source_id": "bloomberg-news",
                "category": "established_news",
                "provider_id": "bloomberg-licensed",
                "source_tier": "established_news",
                "acquisition_mode": "planned_entitlement_required",
                "point_in_time_authority": False,
                "evidence_types": ["timestamped_narrative_corpus", "new_evidence"],
                "license_note": "Never commit licensed article content.",
            },
            {
                "source_id": "reuters-news",
                "category": "established_news",
                "provider_id": "reuters-licensed",
                "source_tier": "established_news",
                "acquisition_mode": "planned_entitlement_required",
                "point_in_time_authority": False,
                "evidence_types": ["timestamped_narrative_corpus", "new_evidence"],
                "license_note": "Never commit licensed article content.",
            },
            {
                "source_id": "exchange-positioning",
                "category": "positioning_or_expectations",
                "provider_id": "exchange-flow-archive",
                "source_tier": "regulated",
                "acquisition_mode": "planned_archive_capture",
                "point_in_time_authority": False,
                "evidence_types": [
                    "consensus_or_positioning",
                    "participant_belief_or_flow",
                ],
                "license_note": "Freeze publication and revision identity.",
            },
        ],
        "checkpoint_protocol": {
            "timezone": "Asia/Shanghai",
            "decision_time_local": "09:25:00",
            "price_lookback_sessions": 60,
            "news_lookback_calendar_days": {
                "monthly": 31,
                "weekly": 14,
                "event_then_weekly": 14,
            },
            "maximum_age_calendar_days": {
                "official_context": 365,
                "macro_vintage": 120,
                "positioning_or_expectations": 14,
                "issuer_or_sector_fundamentals": 180,
            },
        },
        "baseline_protocol": {
            "annualization_sessions": 252,
            "minimum_risk_sessions": 2,
            "risk_free_rate_annual": "0",
            "cvar_confidence": "0.95",
            "transaction_cost_bps_one_way": "0",
            "rebalance_frequency": "monthly_first_session",
            "momentum_lookback_sessions": 1,
            "momentum_top_k": 1,
            "strategies": [
                "cash",
                "primary_buy_and_hold",
                "equal_sector_buy_and_hold",
                "lagged_sector_momentum",
            ],
        },
        "cases": [
            {
                "case_key": "synthetic-long-cycle",
                "decision_schedule": "monthly",
                "analysis_needs": ["cycle_position", "narrative_diffusion"],
                "candidate_method_skills": [
                    "second-level-cycle-context",
                    "narrative-diffusion-assessment",
                ],
                "query_terms": ["synthetic market", "computer sector"],
                "evaluation_horizons": ["full_case"],
                "source_requirements": [
                    {
                        "category": "market_price",
                        "source_ids": ["tushare-market"],
                        "minimum_records_per_checkpoint": 1,
                        "minimum_distinct_sources": 1,
                        "authenticated_availability_required": True,
                    },
                    {
                        "category": "industry_price",
                        "source_ids": ["tushare-industry"],
                        "minimum_records_per_checkpoint": 1,
                        "minimum_distinct_sources": 1,
                        "authenticated_availability_required": True,
                    },
                    {
                        "category": "official_context",
                        "source_ids": ["official-archive"],
                        "minimum_records_per_checkpoint": 1,
                        "minimum_distinct_sources": 1,
                        "authenticated_availability_required": True,
                    },
                    {
                        "category": "macro_vintage",
                        "source_ids": ["macro-vintage"],
                        "minimum_records_per_checkpoint": 1,
                        "minimum_distinct_sources": 1,
                        "authenticated_availability_required": True,
                    },
                    {
                        "category": "established_news",
                        "source_ids": ["bloomberg-news", "reuters-news"],
                        "minimum_records_per_checkpoint": 8,
                        "minimum_distinct_sources": 2,
                        "authenticated_availability_required": True,
                    },
                    {
                        "category": "positioning_or_expectations",
                        "source_ids": ["exchange-positioning"],
                        "minimum_records_per_checkpoint": 1,
                        "minimum_distinct_sources": 1,
                        "authenticated_availability_required": True,
                    },
                ],
            }
        ],
    }
    return {
        **core,
        "registration_id": f"regime-study-registration-{canonical_hash(core)}",
    }


def _write_registration(tmp_path: Path, dataset: MarketRegimeDataset) -> Path:
    path = tmp_path / "registration.json"
    path.write_text(json.dumps(_registration_payload(dataset)), encoding="utf-8")
    return path


def _series(
    series_id: str,
    kind: str,
    tushare_code: str,
    rows: tuple[tuple[str, float, float], ...],
) -> RegimeSeries:
    return RegimeSeries(
        series_id=series_id,
        kind=kind,
        tushare_code=tushare_code,
        source="index_daily" if kind == "market" else "sw_daily",
        return_basis="price",
        rows=tuple(
            {"trade_date": day, "open": opening, "close": closing} for day, opening, closing in rows
        ),
    )


def _panel(dataset: MarketRegimeDataset) -> RegimePanel:
    market_rows = (
        ("2019-12-30", 90.0, 90.0),
        ("2019-12-31", 100.0, 100.0),
        ("2020-01-02", 100.0, 110.0),
        ("2020-01-03", 110.0, 121.0),
        ("2020-01-06", 121.0, 108.9),
    )
    industry_rows = (
        ("2019-12-30", 90.0, 90.0),
        ("2019-12-31", 100.0, 100.0),
        ("2020-01-02", 100.0, 105.0),
        ("2020-01-03", 105.0, 115.5),
        ("2020-01-06", 115.5, 115.5),
    )
    return RegimePanel(
        dataset_id=dataset.dataset_id,
        dataset_hash=dataset.dataset_hash,
        provider_id="tushare-http",
        provider_version="0.1.0",
        historical_vintage="retrieved_historical_not_original_vintage",
        retrieved_at=datetime(2026, 8, 27, tzinfo=UTC),
        industry_taxonomy=RegimeTaxonomy(
            source="SW2021",
            level="L1",
            fields=(
                "index_code",
                "industry_name",
                "parent_code",
                "level",
                "industry_code",
                "is_pub",
                "src",
            ),
            rows=(("801750.SI", "计算机", "", "L1", "710000", "1", "SW2021"),),
            retrieved_at=datetime(2026, 8, 27, tzinfo=UTC),
            content_hash="2" * 64,
        ),
        series=(
            _series("000300.SH", "market", "000300.SH", market_rows),
            _series("sw2021_computer", "industry", "801750.SI", industry_rows),
        ),
        proxy_resolution=(("sw2021_computer", "801750.SI"),),
    )


def test_registration_covers_every_case_and_method_evidence_gate(tmp_path: Path) -> None:
    dataset = _dataset()
    catalog = load_method_skill_catalog(CATALOG)

    registration = load_regime_study_registration(
        _write_registration(tmp_path, dataset),
        dataset=dataset,
        method_catalog=catalog,
    )

    assert tuple(item.case_key for item in registration.cases) == ("synthetic-long-cycle",)
    assert registration.checkpoint_protocol.timezone == "Asia/Shanghai"
    readiness = assess_regime_study_readiness(registration)
    assert readiness["agent_effectiveness_claim_eligible"] is False
    assert readiness["case_count"] == 1
    case = cast(dict[str, object], cast(list[object], readiness["cases"])[0])
    assert "established_news:not_implemented" in cast(list[str], case["blockers"])
    assert "market_price:no_point_in_time_authority" in cast(list[str], case["blockers"])


def test_registration_rejects_missing_case_or_single_publisher_news(tmp_path: Path) -> None:
    dataset = _dataset()
    catalog = load_method_skill_catalog(CATALOG)
    payload = _registration_payload(dataset)
    cases = cast(list[dict[str, object]], payload["cases"])
    cases[0]["case_key"] = "unregistered-case"
    core = {key: value for key, value in payload.items() if key != "registration_id"}
    payload["registration_id"] = f"regime-study-registration-{canonical_hash(core)}"
    path = tmp_path / "missing.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ValueError, match="exactly cover the market regime dataset"):
        load_regime_study_registration(path, dataset=dataset, method_catalog=catalog)

    payload = _registration_payload(dataset)
    cases = cast(list[dict[str, object]], payload["cases"])
    requirements = cast(list[dict[str, object]], cases[0]["source_requirements"])
    news = next(item for item in requirements if item["category"] == "established_news")
    news["source_ids"] = ["bloomberg-news"]
    news["minimum_distinct_sources"] = 1
    core = {key: value for key, value in payload.items() if key != "registration_id"}
    payload["registration_id"] = f"regime-study-registration-{canonical_hash(core)}"
    path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ValueError, match="at least two established-news sources"):
        load_regime_study_registration(path, dataset=dataset, method_catalog=catalog)


def test_long_horizon_baselines_use_daily_path_and_report_risk_metrics(tmp_path: Path) -> None:
    dataset = _dataset()
    catalog = load_method_skill_catalog(CATALOG)
    registration = load_regime_study_registration(
        _write_registration(tmp_path, dataset),
        dataset=dataset,
        method_catalog=catalog,
    )

    report = evaluate_regime_study_baselines(dataset, _panel(dataset), registration)

    assert report["schema_version"] == "market-impact.regime-study-baseline-report.v2"
    assert report["research_only"] is True
    assert report["agent_visible"] is False
    assert report["agent_effectiveness_claim_eligible"] is False
    result = cast(dict[str, object], cast(list[object], report["cases"])[0])
    strategies = cast(dict[str, object], result["strategies"])
    primary = cast(dict[str, object], strategies["primary_buy_and_hold"])
    equal_sector = cast(dict[str, object], strategies["equal_sector_buy_and_hold"])
    momentum = cast(dict[str, object], strategies["lagged_sector_momentum"])
    cash = cast(dict[str, object], strategies["cash"])
    assert primary["total_return"] == "0.08900000"
    assert primary["max_drawdown"] == "-0.10000000"
    assert primary["sharpe"] is not None
    assert primary["cvar"] == "-0.10000000"
    assert equal_sector["total_return"] == "0.15500000"
    assert momentum["total_return"] == "0.15500000"
    assert momentum["turnover"] == "1.00000000"
    assert cash["total_return"] == "0.00000000"
    assert cash["sharpe"] is None
    assert cash["information_ratio_vs_primary"] is not None


def test_report_writer_preserves_prior_evidence_and_replays_exact_content(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    dataset = _dataset()
    registration = load_regime_study_registration(
        _write_registration(tmp_path, dataset),
        dataset=dataset,
        method_catalog=load_method_skill_catalog(CATALOG),
    )
    report = evaluate_regime_study_baselines(dataset, _panel(dataset), registration)
    panel_id = cast(str, report["panel_id"])
    registration_id = registration.registration_id
    monkeypatch.chdir(tmp_path)
    root = tmp_path / ".market-impact" / "regime" / "comparisons"
    root.mkdir(parents=True)
    legacy = root / f"{panel_id}--{registration_id}.json"
    legacy_content = json.dumps(
        {**report, "schema_version": "market-impact.regime-study-baseline-report.v1"}
    ).encode()
    legacy.write_bytes(legacy_content)

    destination = write_regime_study_baseline_report(
        report, panel_id=panel_id, registration_id=registration_id
    )
    assert destination.name == f"{panel_id}--{registration_id}--{canonical_hash(report)}.json"
    assert destination != legacy and legacy.read_bytes() == legacy_content
    assert json.loads(destination.read_text()) == report
    assert destination.stat().st_mode & 0o777 == 0o600
    original = destination.read_bytes()
    metadata = destination.stat()
    replay = write_regime_study_baseline_report(
        report, panel_id=panel_id, registration_id=registration_id
    )
    assert replay == destination and replay.read_bytes() == original
    assert replay.stat().st_ino == metadata.st_ino
    assert replay.stat().st_mtime_ns == metadata.st_mtime_ns

    divergent = {**report, "provider_version": "synthetic-corrected-version"}
    another = write_regime_study_baseline_report(
        divergent, panel_id=panel_id, registration_id=registration_id
    )
    assert another != destination and json.loads(another.read_text()) == divergent
    assert destination.read_bytes() == original and legacy.read_bytes() == legacy_content

    # A conflicting file at the exact content-addressed destination fails closed;
    # neither corruption nor a partially written artifact can be silently repaired.
    conflicting_content = b"synthetic conflicting prior evidence\n"
    destination.write_bytes(conflicting_content)
    with pytest.raises(ValueError, match="already exists with different content"):
        write_regime_study_baseline_report(
            report, panel_id=panel_id, registration_id=registration_id
        )
    assert destination.read_bytes() == conflicting_content
    assert legacy.read_bytes() == legacy_content


def test_registration_example_covers_all_representative_cases() -> None:
    from market_impact_agent.market_regimes import load_market_regime_dataset

    dataset = load_market_regime_dataset(Path("examples/research/market-regime-dataset-v1.json"))
    catalog = load_method_skill_catalog(CATALOG)
    registration = load_regime_study_registration(
        Path("examples/research/market-regime-study-registration-v1.json"),
        dataset=dataset,
        method_catalog=catalog,
    )

    assert len(registration.cases) == len(dataset.cases) == 15
    assert {item.case_key for item in registration.cases} == {
        item.case_key for item in dataset.cases
    }


@pytest.mark.parametrize(
    ("switch", "fee_bps", "expected_cost", "expected_turnover"),
    [
        (False, "10", "0.00100000", "1.00000000"),
        (True, "0", "0.00000000", "3.00000000"),
        (True, "10", "0.00299800", "3.00000000"),
        (True, "20", "0.00599200", "3.00000000"),
    ],
)
def test_monthly_rotation_charges_each_traded_leg(
    tmp_path: Path, switch: bool, fee_bps: str, expected_cost: str, expected_turnover: str
) -> None:
    dataset = _dataset()
    registration = load_regime_study_registration(
        _write_registration(tmp_path, dataset),
        dataset=dataset,
        method_catalog=load_method_skill_catalog(CATALOG),
    )
    case = replace(
        dataset.cases[0],
        path_start=date(2020, 1, 30),
        tradable_start=date(2020, 1, 30),
        end=date(2020, 2, 3),
        required_industry_proxies=("a", "b"),
    )
    days = ("2020-01-28", "2020-01-29", "2020-01-30", "2020-01-31", "2020-02-03")
    primary = _series("000300.SH", "market", "000300.SH", tuple((d, 10.0, 10.0) for d in days))
    a = _series(
        "a",
        "industry",
        "a",
        tuple(zip(days, (10.0,) * 5, (10.0, 12.0, 10.0, 10.0, 10.0), strict=True)),
    )
    b = _series(
        "b",
        "industry",
        "b",
        tuple(
            zip(days, (10.0,) * 5, (10.0, 10.0, 10.0, 11.0 if switch else 9.0, 10.0), strict=True)
        ),
    )
    report = evaluate_regime_case_baselines(
        case,
        {s.series_id: s for s in (primary, a, b)},
        replace(registration.baseline_protocol, transaction_cost_bps_one_way=Decimal(fee_bps)),
    )
    result = cast(dict[str, dict[str, object]], report["strategies"])["lagged_sector_momentum"]
    # Entry is one leg. A complete A-to-B rotation sells A and buys B: two more legs.
    # Held A is flat until rotation; the new B holding is flat after its opening purchase.
    assert result["turnover"] == expected_turnover
    assert result["modeled_cost"] == expected_cost
    assert Decimal(str(result["total_return"])) == -Decimal(expected_cost)


def test_cli_reports_source_readiness_without_claiming_agent_effectiveness(
    capsys: pytest.CaptureFixture[str],
) -> None:
    from market_impact_agent.cli import main

    result = main(
        [
            "regime",
            "study-validate",
            "--dataset",
            "examples/research/market-regime-dataset-v1.json",
            "--method-catalog",
            "examples/research/famous-method-skill-catalog-v1.json",
            "--registration",
            "examples/research/market-regime-study-registration-v1.json",
        ]
    )

    assert result == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["valid"] is True
    assert payload["case_count"] == 15
    assert payload["agent_effectiveness_claim_eligible"] is False
