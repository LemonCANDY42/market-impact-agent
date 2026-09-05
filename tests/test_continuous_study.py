from __future__ import annotations

from dataclasses import replace
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path
from typing import cast

import pytest

from market_impact_agent.continuous_study import (
    OrdinaryStratum,
    WindowKind,
    build_continuous_study_registration,
    coverage_report,
    load_pinned_regime_panels,
    load_prior_usage_audit_binding,
    ordinary_candidate_features,
)
from market_impact_agent.market_regimes import RegimePanel, load_market_regime_dataset

_DATASET = Path("examples/research/market-regime-dataset-v1.json")
_REGIME_ROOT = Path(".market-impact/regime")
_PRIOR_USAGE_AUDIT = Path(".market-impact/continuous-20260905/prior-budget-audit.json")
_PRIVATE_CONTINUOUS_STUDY_INPUTS = (
    _REGIME_ROOT
    / "regime-panel-d63c8f98eced67ff86143a82e1db5079460e0b1bf7ecaa8a447176eb20182286"
    / "manifest.json",
    _REGIME_ROOT
    / "regime-panel-e0817b85d8fc33478a1fdf530d159e2f5173769b83f2639456c9e7d2c2c78c8b"
    / "manifest.json",
    _PRIOR_USAGE_AUDIT,
)


def private_continuous_study_inputs_available(
    paths: tuple[Path, ...] = _PRIVATE_CONTINUOUS_STUDY_INPUTS,
) -> bool:
    """Return whether the ignored licensed artifacts needed by real studies exist."""

    return all(path.is_file() for path in paths)


def require_private_continuous_study_inputs(
    paths: tuple[Path, ...] = _PRIVATE_CONTINUOUS_STUDY_INPUTS,
) -> None:
    """Skip only the real private-artifact integration path on portable clones."""

    missing = [str(path) for path in paths if not path.is_file()]
    if missing:
        pytest.skip("requires ignored licensed continuous-study artifacts: " + ", ".join(missing))


def _registration():
    require_private_continuous_study_inputs()
    panels = load_pinned_regime_panels(_REGIME_ROOT)
    dataset = load_market_regime_dataset(_DATASET)
    audit = load_prior_usage_audit_binding(_PRIOR_USAGE_AUDIT)
    return (
        build_continuous_study_registration(dataset, panels, prior_usage_audit=audit),
        panels.selection_panel.panel,
    )


def test_private_study_input_guard_is_portable(tmp_path: Path) -> None:
    present = tmp_path / "present.json"
    present.write_text("{}", encoding="utf-8")

    assert private_continuous_study_inputs_available((present,))
    assert not private_continuous_study_inputs_available((tmp_path / "absent.json",))
    with pytest.raises(pytest.skip.Exception, match="requires ignored licensed"):
        require_private_continuous_study_inputs((tmp_path / "absent.json",))


def test_registration_preserves_legacy_coverage_and_freezes_new_denominators() -> None:
    registration, _ = _registration()

    assert len(registration.coverage_windows) == 18
    assert [item.source_case_key for item in registration.coverage_windows[:15]] == [
        "cn-2014-2015-leveraged-melt-up",
        "cn-2015-disorder-deleveraging",
        "cn-2016-circuit-breaker-microstress",
        "cn-2016-2018-quality-slow-bull",
        "cn-2018-bear-market",
        "cn-2019-q1-fast-rebound",
        "cn-2020-covid-closure-shock",
        "cn-2020-2021-structural-recovery",
        "cn-2021-index-flat-sector-rotation",
        "cn-2022-multishock-bear",
        "cn-2022-reopening-policy",
        "cn-2023-2024-smallcap-liquidity-stress",
        "cn-2024-broad-rebound",
        "cn-2024-policy-melt-up",
        "cn-2024-post-rally-whipsaw",
    ]
    ordinary = registration.coverage_windows[15:]
    assert [item.ordinary_stratum for item in ordinary] == list(OrdinaryStratum)
    assert all(item.kind is WindowKind.ORDINARY for item in ordinary)
    assert all(item.observation_through_session < item.decision_session for item in ordinary)
    assert len(registration.deep_cells) == 8
    assert registration.core_dict()["cadence"] == {
        "arms": ["expiry_only", "scheduled", "event"],
        "planned_observation_denominator": 72,
        "denominator_formula": "8 deep cells x 3 model profiles x 3 cadence arms",
        "gap_policy": "retain every planned observation; record missing or blocked inputs",
    }
    time_contract = cast(dict[str, object], registration.core_dict()["time_contract"])
    assert time_contract["version"] == ("preopen_t0_h1_next_preopen_expiry_v1")
    assert registration.budget.to_dict() == {
        "route_qualification_microusd": 1_000_000,
        "analysis_coverage_microusd": 9_000_000,
        "portfolio_coverage_microusd": 2_500_000,
        "rolling_microusd": 22_000_000,
        "unseen_and_prospective_microusd": 2_500_000,
        "recovery_microusd": 3_000_000,
        "total_microusd": 40_000_000,
    }
    assert [(item.model, item.reasoning_effort) for item in registration.model_profiles] == [
        ("gpt-5.6-luna", "max"),
        ("gpt-5.6-terra", "high"),
        ("gpt-5.6-sol", "high"),
    ]
    assert registration.core_dict()["information_coverage_gaps"] == [
        {
            "gap_id": "bounded-news-audit-pending",
            "coverage_type": "information_coverage",
            "status": "pending_bounded_news_audit",
            "applies_to": "all_18_coverage_windows",
            "reason": (
                "Price paths cannot establish that no material headline was available; "
                "a bounded news audit must determine coverage."
            ),
            "claim_permitted": False,
        }
    ]
    prior_usage = cast(dict[str, object], registration.core_dict()["prior_usage_reconciliation"])
    assert prior_usage == {
        "audit_id": "continuous-20260905-prior-budget-audit-v1",
        "audit_content_hash": ("1d8c43daa5fbc4098f2ae5a53fa6fd2fefcb623a43d680dd670082cff698ebb4"),
        "status": "bound_audit_required_for_final_budget_accounting",
        "stages": {
            "route": {"requests": 9, "known_microusd": 85_194, "reserved_microusd": 0},
            "analysis": {
                "requests": 77,
                "known_microusd": 4_870_788,
                "reserved_microusd": 11_769,
            },
            "portfolio": {
                "requests": 12,
                "known_microusd": 400_923,
                "reserved_microusd": 0,
            },
        },
        "request_count": 98,
        "known_cost_microusd": 5_356_905,
        "reserved_cost_microusd": 11_769,
    }
    assert "included_in_new_study_cap" not in prior_usage


def test_ordinary_selection_is_hash_ordered_and_nonoverlapping() -> None:
    registration, panel = _registration()
    report = coverage_report(registration)

    assert report["coverage_denominator"] == 18
    assert report["deep_denominator"] == 8
    assert report["planned_model_cadence_observation_denominator"] == 72
    assert report["ordinary_overlap"] == [
        {
            "ordinary_window_id": "ordinary-low-volatility",
            "overlaps_fixed_deep_window_ids": [],
            "overlaps_other_ordinary_window_ids": [],
        },
        {
            "ordinary_window_id": "ordinary-mid-volatility",
            "overlaps_fixed_deep_window_ids": [],
            "overlaps_other_ordinary_window_ids": [],
        },
        {
            "ordinary_window_id": "ordinary-higher-nonextreme-volatility",
            "overlaps_fixed_deep_window_ids": [],
            "overlaps_other_ordinary_window_ids": [],
        },
    ]
    inventory = cast(dict[str, object], report["baseline_input_inventory"])
    assert inventory["required_information_audit"] == {
        "status": "pending_bounded_news_audit",
        "reason": "price data cannot prove no-headline coverage or absence",
    }
    selected_sessions = {item.decision_session for item in registration.coverage_windows[15:]}
    assert date(2014, 8, 20) not in selected_sessions
    assert date(2022, 4, 6) not in selected_sessions
    for decision_session in (date(2014, 8, 20), date(2022, 4, 6)):
        features = {
            key: Decimal(value)
            for key, value in ordinary_candidate_features(panel, decision_session).items()
        }
        assert features["log_return_20_sessions"] * features["log_return_60_sessions"] > 0
        assert abs(features["normalized_trend_z_20_sessions"]) >= Decimal("0.5")
        assert abs(features["normalized_trend_z_60_sessions"]) >= Decimal("0.5")


def test_fixed_candidate_features_do_not_read_later_prices() -> None:
    registration, panel = _registration()
    candidate = registration.coverage_windows[15]
    expected = ordinary_candidate_features(panel, candidate.decision_session)
    changed = _change_primary_closes_after(panel, candidate.decision_session)

    assert ordinary_candidate_features(changed, candidate.decision_session) == expected


def _change_primary_closes_after(panel: RegimePanel, start: date) -> RegimePanel:
    primary = next(item for item in panel.series if item.series_id == "000300.SH")
    rows: list[dict[str, object]] = []
    for row in primary.rows:
        trade_date = _row_date(str(row["trade_date"]))
        if start < trade_date < start + timedelta(days=100):
            rows.append({**row, "close": str(Decimal(str(row["close"])) * Decimal("1.4"))})
        else:
            rows.append(row)
    changed_primary = replace(primary, rows=tuple(rows))
    return replace(
        panel,
        series=tuple(
            changed_primary if item.series_id == primary.series_id else item
            for item in panel.series
        ),
    )


def _row_date(raw: str) -> date:
    if len(raw) == 8:
        return date(int(raw[:4]), int(raw[4:6]), int(raw[6:]))
    return date.fromisoformat(raw)
