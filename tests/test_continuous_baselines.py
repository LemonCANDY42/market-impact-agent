# pyright: reportPrivateUsage=false
from __future__ import annotations

import json
from dataclasses import replace
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import cast

import pytest

from market_impact_agent.continuous_baselines import (
    CONTINUOUS_EXECUTABLE_BASELINE_IDS,
    ContinuousBaselineAccountSeed,
    ContinuousBaselineWindow,
    evaluate_continuous_baseline_window,
    evaluate_continuous_baselines,
    evaluate_raw_index_research_baseline,
    registered_baseline_windows,
)
from market_impact_agent.continuous_study import (
    ContinuousStudyWindow,
    WindowKind,
    build_continuous_study_registration,
    load_pinned_regime_panels,
    load_prior_usage_audit_binding,
)
from market_impact_agent.historical_ashare_inputs import (
    HistoricalAShareInputs,
    HistoricalSessionInputs,
)
from market_impact_agent.market_regimes import load_market_regime_dataset
from market_impact_agent.streaming_nautilus_account import HistoricalStreamingAccount

from .test_continuous_study import require_private_continuous_study_inputs
from .test_historical_ashare_inputs import _capture, _source

_DATASET = Path("examples/research/market-regime-dataset-v1.json")
_REGIME_ROOT = Path(".market-impact/regime")
_PRIOR_USAGE_AUDIT = Path(".market-impact/continuous-20260905/prior-budget-audit.json")


def _registered_window(*, sessions: tuple[date, ...]) -> ContinuousBaselineWindow:
    window = ContinuousStudyWindow(
        window_id="fixture-registered-window",
        kind=WindowKind.LEGACY_CASE,
        decision_session=sessions[0],
        observation_through_session=date(2025, 1, 2),
        outcome_window_end=sessions[-1],
        source_case_key="fixture-case",
        ordinary_stratum=None,
        selection_key=None,
        features=(),
    )
    return ContinuousBaselineWindow(window, sessions)


def _seed() -> ContinuousBaselineAccountSeed:
    return ContinuousBaselineAccountSeed("continuous-baseline-fixture", b"b" * 32)


def _momentum_source(tmp_path: Path):
    """Extend the public source fixture with prior closes and a reversal session."""

    source = _source(tmp_path / "source")
    store = source.store
    snapshot_ids = (
        _capture(
            store,
            "fund_daily",
            {"ts_code": "510300.SH", "start_date": "20241227", "end_date": "20250106"},
            [
                {
                    "ts_code": "510300.SH",
                    "trade_date": "20241226",
                    "pre_close": 3,
                    "open": 3,
                    "high": 3,
                    "low": 3,
                    "close": 3,
                    "change": 0,
                    "pct_chg": 0,
                    "vol": 200000,
                    "amount": 60000,
                },
                {
                    "ts_code": "510300.SH",
                    "trade_date": "20241227",
                    "pre_close": 3,
                    "open": 3,
                    "high": 3,
                    "low": 3,
                    "close": 3,
                    "change": 0,
                    "pct_chg": 0,
                    "vol": 200000,
                    "amount": 60000,
                },
                {
                    "ts_code": "510300.SH",
                    "trade_date": "20241230",
                    "pre_close": 3,
                    "open": 4.5,
                    "high": 4.5,
                    "low": 4.5,
                    "close": 4.5,
                    "change": 1.5,
                    "pct_chg": 50,
                    "vol": 200000,
                    "amount": 90000,
                },
                {
                    "ts_code": "510300.SH",
                    "trade_date": "20241231",
                    "pre_close": 4.5,
                    "open": 4.4,
                    "high": 4.4,
                    "low": 4.4,
                    "close": 4.4,
                    "change": -0.1,
                    "pct_chg": -2.22,
                    "vol": 200000,
                    "amount": 88000,
                },
                {
                    "ts_code": "510300.SH",
                    "trade_date": "20250106",
                    "pre_close": 4.2,
                    "open": 4.1,
                    "high": 4.1,
                    "low": 4,
                    "close": 4,
                    "change": -0.2,
                    "pct_chg": -4.76,
                    "vol": 200000,
                    "amount": 80000,
                },
            ],
        ),
        _capture(
            store,
            "trade_cal",
            {"exchange": "SSE", "start_date": "20241226", "end_date": "20250106"},
            [
                {
                    "exchange": "SSE",
                    "cal_date": day,
                    "is_open": 1,
                    "pretrade_date": previous,
                }
                for day, previous in (
                    ("20241227", "20241226"),
                    ("20241230", "20241227"),
                    ("20241231", "20241230"),
                    ("20250106", "20250103"),
                )
            ],
        ),
        _capture(
            store,
            "suspend_d",
            {"ts_code": "510300.SH", "start_date": "20250106", "end_date": "20250106"},
            [
                {
                    "ts_code": "510300.SH",
                    "trade_date": "20250106",
                    "suspend_type": "R",
                    "suspend_timing": None,
                }
            ],
        ),
        _capture(
            store,
            "stk_limit",
            {"ts_code": "510300.SH", "start_date": "20250106", "end_date": "20250106"},
            [
                {
                    "ts_code": "510300.SH",
                    "trade_date": "20250106",
                    "pre_close": 4.2,
                    "up_limit": 4.62,
                    "down_limit": 3.78,
                }
            ],
        ),
        _capture(
            store,
            "fund_adj",
            {"ts_code": "510300.SH", "start_date": "20241227", "end_date": "20250106"},
            [
                {"ts_code": "510300.SH", "trade_date": day, "adj_factor": 1}
                for day in ("20241226", "20241227", "20241230", "20241231", "20250106")
            ],
        ),
    )
    return source.with_snapshots(snapshot_ids)


def _missing_intermediate_momentum_source(tmp_path: Path):
    """Four available closes with a captured calendar link to an absent close."""

    source = _source(tmp_path / "source")
    store = source.store
    snapshot_ids = (
        _capture(
            store,
            "fund_daily",
            {"ts_code": "510300.SH", "start_date": "20241226", "end_date": "20241230"},
            [
                {
                    "ts_code": "510300.SH",
                    "trade_date": day,
                    "pre_close": 3,
                    "open": 3,
                    "high": 3,
                    "low": 3,
                    "close": 3,
                    "change": 0,
                    "pct_chg": 0,
                    "vol": 200000,
                    "amount": 60000,
                }
                for day in ("20241226", "20241227", "20241230")
            ],
        ),
        _capture(
            store,
            "trade_cal",
            {"exchange": "SSE", "start_date": "20241226", "end_date": "20241230"},
            [
                {
                    "exchange": "SSE",
                    "cal_date": day,
                    "is_open": 1,
                    "pretrade_date": previous,
                }
                for day, previous in (("20241227", "20241226"), ("20241230", "20241227"))
            ],
        ),
        _capture(
            store,
            "fund_adj",
            {"ts_code": "510300.SH", "start_date": "20241226", "end_date": "20241230"},
            [
                {"ts_code": "510300.SH", "trade_date": day, "adj_factor": 1}
                for day in ("20241226", "20241227", "20241230")
            ],
        ),
    )
    return source.with_snapshots(snapshot_ids)


def _buy_order_quantities(state_root: Path, action: str) -> list[str]:
    paths = list(state_root.rglob("account.jsonl"))
    assert len(paths) == 1
    records = [json.loads(line) for line in paths[0].read_text().splitlines()]
    return [
        str(intent["quantity"])
        for record in records
        for intent in record.get("intents", [])
        if action in str(intent["client_order_id"])
    ]


def _future_open_variant(
    source: HistoricalAShareInputs, open_price: Decimal
) -> dict[date, HistoricalSessionInputs]:
    seed = source.session("510300.SH", date(2025, 1, 2))
    execution = source.session("510300.SH", date(2025, 1, 3))
    assert execution.bar is not None
    return {
        date(2025, 1, 2): seed,
        date(2025, 1, 3): replace(
            execution,
            bar=replace(
                execution.bar,
                open=open_price,
                low=min(open_price, execution.bar.low),
                high=max(open_price, execution.bar.high),
            ),
        ),
    }


def _registration():
    require_private_continuous_study_inputs()
    panels = load_pinned_regime_panels(_REGIME_ROOT)
    registration = build_continuous_study_registration(
        load_market_regime_dataset(_DATASET),
        panels,
        prior_usage_audit=load_prior_usage_audit_binding(_PRIOR_USAGE_AUDIT),
    )
    return registration, panels.selection_panel


def test_executable_baseline_ids_publish_the_frozen_momentum_binding() -> None:
    assert CONTINUOUS_EXECUTABLE_BASELINE_IDS == (
        "cash_no_action",
        "same_initial_account_hold",
        "broad_etf_hold",
        "phase2_adjusted_close_momentum_510300",
    )


def test_matched_executable_baselines_use_real_seed_fees_and_source_bars(tmp_path: Path) -> None:
    source = _source(tmp_path / "source")
    window = _registered_window(sessions=(date(2025, 1, 3),))
    reports = {
        baseline_id: evaluate_continuous_baseline_window(
            registration_id="fixture-registration",
            baseline_id=baseline_id,  # pyright: ignore[reportArgumentType]
            registered_window=window,
            historical_inputs=source,
            account_seed=_seed(),
            state_root=tmp_path / "state",
        )
        for baseline_id in ("cash_no_action", "same_initial_account_hold", "broad_etf_hold")
    }

    assert {item["status"] for item in reports.values()} == {"complete"}
    initial_hashes = {str(item["initial_account_hash"]) for item in reports.values()}
    assert len(initial_hashes) == 1
    cash = cast(dict[str, object], reports["cash_no_action"]["metrics"])
    hold = cast(dict[str, object], reports["same_initial_account_hold"]["metrics"])
    broad = cast(dict[str, object], reports["broad_etf_hold"]["metrics"])
    assert cash["expected_sessions"] == 1 and cash["observed_sessions"] == 1
    assert cash["residual_positions"] == {}
    assert Decimal(str(cash["execution_fees_cny"])) == Decimal("15")
    assert hold["residual_positions"] == {"510300.SH": "12500"}
    assert Decimal(str(broad["cash_ratio"])) < Decimal("0.05")
    assert reports["broad_etf_hold"]["execution_target"] == {
        "target": "maximum_affordable_510300_whole_lots_after_first_registered_session",
        "comparable_seed": True,
    }


def test_buy_sizing_uses_cutoff_upper_limit_not_future_open(tmp_path: Path) -> None:
    window = _registered_window(sessions=(date(2025, 1, 3),))
    for baseline_id, source in (
        ("broad_etf_hold", _source(tmp_path / "broad-source")),
        ("phase2_adjusted_close_momentum_510300", _momentum_source(tmp_path / "trend-source")),
    ):
        quantities: list[list[str]] = []
        for label, opening_price in (("low-open", Decimal("3.8")), ("high-open", Decimal("4.2"))):
            state_root = tmp_path / baseline_id / label
            evaluate_continuous_baseline_window(
                registration_id="fixture-registration",
                baseline_id=baseline_id,
                registered_window=window,
                historical_inputs=source,
                account_seed=_seed(),
                state_root=state_root,
                source_sessions=_future_open_variant(source, opening_price),
            )
            quantities.append(
                _buy_order_quantities(
                    state_root,
                    "momentum-buy" if baseline_id.startswith("phase2_") else "full-buy",
                )
            )
        assert quantities == [["11300"], ["11300"]]


def test_missing_intermediate_adjusted_close_is_typed_source_gap(tmp_path: Path) -> None:
    report = evaluate_continuous_baseline_window(
        registration_id="fixture-registration",
        baseline_id="phase2_adjusted_close_momentum_510300",
        registered_window=_registered_window(sessions=(date(2025, 1, 3),)),
        historical_inputs=_missing_intermediate_momentum_source(tmp_path / "source"),
        account_seed=_seed(),
        state_root=tmp_path / "state",
    )

    gaps = cast(list[dict[str, object]], report["input_gaps"])
    gap = next(
        item
        for item in gaps
        if item["gap_id"] == "continuous_momentum_calendar_contiguity_unverified"
    )
    assert report["status"] == "incomplete_source_inputs"
    assert gap["expected_prior_session"] == "2024-12-30"
    assert gap["transition_session"] == "2025-01-02"


def test_phase2_momentum_binding_reverses_to_cash_with_real_fees_and_t_plus_one(
    tmp_path: Path,
) -> None:
    report = evaluate_continuous_baseline_window(
        registration_id="fixture-registration",
        baseline_id="phase2_adjusted_close_momentum_510300",
        registered_window=_registered_window(sessions=(date(2025, 1, 3), date(2025, 1, 6))),
        historical_inputs=_momentum_source(tmp_path / "source"),
        account_seed=_seed(),
        state_root=tmp_path / "state",
    )

    assert report["status"] == "complete"
    binding = cast(dict[str, object], report["momentum_binding"])
    assert binding["binding_version"] == "continuous-phase2-adjusted-close-momentum-510300.v1"
    assert binding["phase2_calculation_ref"] == "market_impact_agent.phase2_study._momentum_action"
    assert binding["continuous_abstain_mapping"] == (
        "target_cash_by_selling_existing_510300_at_next_eligible_open"
    )
    signals = cast(list[dict[str, object]], report["momentum_signals"])
    assert [(item["session"], item["phase2_action"]) for item in signals] == [
        ("2025-01-03", "buy"),
        ("2025-01-06", "abstain"),
    ]
    assert signals[0]["adjusted_closes"] == ["3", "4.5", "4.4", "4"]
    assert signals[1]["adjusted_closes"] == ["4.5", "4.4", "4", "4.2"]
    metrics = cast(dict[str, object], report["metrics"])
    assert metrics["expected_sessions"] == 2
    assert metrics["observed_sessions"] == 2
    assert metrics["residual_positions"] == {}
    # The first action buys at the 3 Jan open and the reversal sells all holdings at
    # the next eligible 6 Jan open; both fills pass through the account fee/T+1 engine.
    assert Decimal(str(metrics["execution_fees_cny"])) > Decimal("40")


@pytest.mark.parametrize("bid_quantity", [0, 100])
def test_unfilled_matched_cash_exit_remains_incomplete(tmp_path: Path, bid_quantity: int) -> None:
    source = _source(tmp_path / "source")
    seed_day = source.session("510300.SH", date(2025, 1, 2))
    execution_day = source.session("510300.SH", date(2025, 1, 3))
    assert execution_day.bar is not None
    unfillable = replace(
        execution_day,
        bar=replace(execution_day.bar, open_bid_quantity=bid_quantity),
    )
    report = evaluate_continuous_baseline_window(
        registration_id="fixture-registration",
        baseline_id="cash_no_action",
        registered_window=_registered_window(sessions=(date(2025, 1, 3),)),
        historical_inputs=source,
        account_seed=_seed(),
        state_root=tmp_path / "state",
        source_sessions={date(2025, 1, 2): seed_day, date(2025, 1, 3): unfillable},
    )

    metrics = cast(dict[str, object], report["metrics"])
    gaps = cast(list[dict[str, object]], report["execution_gaps"])
    assert report["status"] == "incomplete_execution"
    assert metrics["complete"] is True
    assert metrics["residual_positions"] == {"510300.SH": str(12500 - bid_quantity)}
    assert gaps[0]["gap_id"] == "first_session_baseline_target_unfilled_or_partial"
    assert gaps[0]["filled_quantity"] == str(bid_quantity)

    replayed = evaluate_continuous_baseline_window(
        registration_id="fixture-registration",
        baseline_id="cash_no_action",
        registered_window=_registered_window(sessions=(date(2025, 1, 3),)),
        historical_inputs=source,
        account_seed=_seed(),
        state_root=tmp_path / "state",
        source_sessions={date(2025, 1, 2): seed_day, date(2025, 1, 3): unfillable},
    )
    replayed_gaps = cast(list[dict[str, object]], replayed["execution_gaps"])
    assert replayed["status"] == "incomplete_execution"
    assert replayed_gaps[0]["gap_id"] == "first_session_baseline_target_unfilled_or_partial"
    assert replayed == report


def test_missing_daily_source_keeps_the_registered_fixed_denominator(tmp_path: Path) -> None:
    source = _source(tmp_path / "source")
    report = evaluate_continuous_baseline_window(
        registration_id="fixture-registration",
        baseline_id="same_initial_account_hold",
        registered_window=_registered_window(sessions=(date(2025, 1, 3), date(2025, 1, 6))),
        historical_inputs=source,
        account_seed=_seed(),
        state_root=tmp_path / "state",
    )

    metrics = cast(dict[str, object], report["metrics"])
    assert report["status"] == "incomplete_source_inputs"
    assert report["expected_sessions"] == 2
    assert report["registered_daily_calendar"] == ["2025-01-03", "2025-01-06"]
    assert metrics["expected_sessions"] == 2
    assert metrics["observed_sessions"] == 1
    assert metrics["complete"] is False
    gaps = cast(list[dict[str, object]], report["input_gaps"])
    assert gaps[0]["gap_id"] == "daily_execution_input_unavailable"
    assert gaps[0]["session"] == "2025-01-06"
    assert set(cast(list[str], gaps[0]["gaps"])) >= {
        "corporate_action_factor_coverage_missing",
        "daily_limits_unverified",
        "halt_status_unverified",
        "raw_daily_bar_missing",
        "raw_daily_session_missing",
        "trading_calendar_session_unverified",
    }


def test_registered_calendar_and_raw_index_diagnostic_do_not_make_execution_claims() -> None:
    registration, selection_panel = _registration()

    windows = registered_baseline_windows(registration, selection_panel)
    research = evaluate_raw_index_research_baseline(registration, selection_panel)

    assert len(windows) == 18
    assert all(item.sessions[0] == item.window.decision_session for item in windows)
    assert all(item.sessions[-1] == item.window.outcome_window_end for item in windows)
    assert research["execution_eligible"] is False
    assert research["non_executable_reason"] == "regime_panel_index_prices_are_not_execution_prices"
    assert research["coverage_denominator"] == 18
    assert len(cast(list[object], research["windows"])) == 18


def test_full_registered_runner_retains_all_windows_when_source_is_not_captured(
    tmp_path: Path,
) -> None:
    registration, selection_panel = _registration()
    report = evaluate_continuous_baselines(
        registration,
        selection_panel,
        historical_inputs=_source(tmp_path / "source"),
        account_seed=_seed(),
        state_root=tmp_path / "state",
    )

    assert report["coverage_denominator"] == 18
    assert report["calendar_source"]["index_prices_used_as_execution_prices"] is False  # type: ignore[index]
    assert report["executable_baselines_complete"] is False
    baselines = cast(list[dict[str, object]], report["baselines"])
    executable = baselines[:4]
    assert all(len(cast(list[object], baseline["windows"])) == 18 for baseline in executable)
    assert all(
        all(
            item["status"] == "incomplete_source_inputs"
            for item in cast(list[dict[str, object]], baseline["windows"])
        )
        for baseline in executable
    )
    momentum = executable[-1]
    assert momentum["baseline_id"] == "phase2_adjusted_close_momentum_510300"
    binding = cast(dict[str, object], momentum["binding"])
    assert binding["binding_version"] == "continuous-phase2-adjusted-close-momentum-510300.v1"
    unsupported = {str(item["baseline_id"]): item for item in baselines[4:]}
    assert set(unsupported) == {
        "lagged_volatility_rule",
        "equal_sector_buy_and_hold",
        "lagged_sector_momentum",
    }
    assert unsupported["lagged_volatility_rule"]["registered_policy_ref"] == (
        "continuous_study.OrdinarySelectionPolicy"
    )
    assert unsupported["lagged_volatility_rule"]["gap_id"] == (
        "continuous_volatility_rebalance_policy_missing"
    )
    for baseline_id in ("equal_sector_buy_and_hold", "lagged_sector_momentum"):
        assert unsupported[baseline_id]["execution_eligible"] is False
        assert unsupported[baseline_id]["gap_id"] == "historical_tradable_sector_membership_missing"
        assert str(unsupported[baseline_id]["registered_policy_ref"]).startswith("regime_study.")
        assert "historical tradable sector membership" in str(unsupported[baseline_id]["reason"])


def test_failed_seed_report_is_identical_after_restart(tmp_path: Path) -> None:
    source = _source(tmp_path / "source")
    seed_day = source.session("510300.SH", date(2025, 1, 2))
    assert seed_day.bar is not None
    failed_seed = replace(seed_day, bar=replace(seed_day.bar, open_ask_quantity=0))
    source_sessions = {
        date(2025, 1, 2): failed_seed,
        date(2025, 1, 3): source.session("510300.SH", date(2025, 1, 3)),
    }
    for baseline_id in CONTINUOUS_EXECUTABLE_BASELINE_IDS:

        def evaluate(baseline_id: str = baseline_id) -> dict[str, object]:
            return evaluate_continuous_baseline_window(
                registration_id="fixture-registration",
                baseline_id=baseline_id,
                registered_window=_registered_window(sessions=(date(2025, 1, 3),)),
                historical_inputs=source,
                account_seed=_seed(),
                state_root=tmp_path / "state",
                source_sessions=source_sessions,
            )

        first = evaluate()
        assert first["status"] == "incomplete_execution"
        assert cast(dict[str, object], first["metrics"])["observed_sessions"] == 0
        assert evaluate() == first
        # A previous faulty reader may already have advanced an unseeded account.
        # Preserve that evidence, but never accept its curve as the registered seed.
        journal = next((tmp_path / "state").glob(f"**/{baseline_id}/**/account.jsonl"))
        assert seed_day.spec is not None
        later_bar = source_sessions[date(2025, 1, 3)].bar
        assert later_bar is not None
        polluted = HistoricalStreamingAccount(
            specs=(seed_day.spec,),
            journal_path=journal,
            account_reference=_seed().account_reference,
            account_reference_key=_seed().account_reference_key,
        )
        try:
            polluted.advance_session({"510300.SH": later_bar})
        finally:
            polluted.close()
        prefix = journal.read_bytes()
        assert evaluate() == first
        assert journal.read_bytes() == prefix


def test_unaffordable_broad_lot_gap_replays_identically(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def unaffordable(*args: object) -> Decimal:
        return Decimal(0)

    monkeypatch.setattr(
        "market_impact_agent.continuous_baselines._maximum_affordable_lot",
        unaffordable,
    )
    source = _source(tmp_path / "source")

    def evaluate():
        return evaluate_continuous_baseline_window(
            registration_id="fixture-registration",
            baseline_id="broad_etf_hold",
            registered_window=_registered_window(sessions=(date(2025, 1, 3),)),
            historical_inputs=source,
            account_seed=_seed(),
            state_root=tmp_path / "state",
        )

    first = evaluate()
    assert first["execution_gaps"] == [
        {"gap_id": "broad_etf_additional_lot_unaffordable", "session": "2025-01-03"}
    ]
    assert evaluate() == first
