# pyright: reportPrivateUsage=false
"""Preflight separates optional candidates and respects pre-registered deep horizons."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import replace
from datetime import date
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest

from market_impact_agent import continuous_experiment as experiment
from market_impact_agent.continuous_baselines import ContinuousBaselineWindow
from market_impact_agent.continuous_study import ContinuousStudyWindow, WindowKind
from market_impact_agent.historical_ashare_inputs import HistoricalSessionInputs
from tests.test_historical_ashare_inputs import _source


def _preflight(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    qualified: bool,
    gap_symbol: str = "510500.SH",
    gap_day: date = date(2025, 1, 3),
    baseline_complete: bool = True,
) -> tuple[dict[str, object], list[ContinuousBaselineWindow]]:
    source = _source(tmp_path)
    if qualified:
        source.policy = replace(source.policy, limit_basis="qualified_seed_etf_exchange_rule_v1")
    seed_session = source.session("510300.SH", date(2025, 1, 3))
    # Use a complete captured source result to isolate the preflight scope boundary.
    seed_session = replace(seed_session, gaps=())

    def session(symbol: str, day: date) -> HistoricalSessionInputs:
        return (
            replace(seed_session, gaps=("fixture_source_gap",))
            if (symbol, day) == (gap_symbol, gap_day)
            else seed_session
        )

    monkeypatch.setattr(source, "session", session)
    definition = ContinuousStudyWindow(
        "fixture",
        WindowKind.LEGACY_CASE,
        date(2025, 1, 3),
        date(2025, 1, 2),
        date(2025, 1, 7),
        "fixture-case",
        None,
        None,
        (),
    )
    full = ContinuousBaselineWindow(
        definition,
        (
            date(2025, 1, 3),
            date(2025, 1, 6),
            date(2025, 1, 7),
        ),
    )
    registration = SimpleNamespace(
        registration_id="fixture-registration",
        model_profiles=(),
        deep_cells=(
            SimpleNamespace(coverage_window_id="fixture", outcome_window_end=date(2025, 1, 6)),
        ),
        to_dict=lambda: {"fixture": True},
    )
    frames: list[Any] = []
    repositories: list[Any] = []
    for day in full.sessions[:2]:
        cutoff = experiment.registered_frame_cutoff(day)
        frames.append(
            SimpleNamespace(
                cutoff=cutoff,
                gaps=(),
                input_hash="fixture-frame",
                snapshot_ids=(),
                to_dict=lambda: {"fixture": "frame"},
            )
        )
        repositories.append(
            SimpleNamespace(
                evidence_pack=SimpleNamespace(
                    as_of=cutoff,
                    evidence=(),
                    pattern_packs=(),
                    to_dict=lambda: {"fixture": "pack"},
                )
            )
        )
    window = experiment.FrozenContinuousWindow(
        "fixture",
        tuple(frames),
        tuple(repositories),
        source,
        full.sessions,
    )

    def fixed(value: object) -> Callable[..., object]:
        def call(*_: object) -> object:
            return value

        return call

    monkeypatch.setattr(
        experiment, "load_prepared_continuous_registration", fixed({"fixture": True})
    )
    monkeypatch.setattr(
        experiment,
        "study_budget",
        fixed(SimpleNamespace(journal=SimpleNamespace(path=source.store.index_path))),
    )
    monkeypatch.setattr(experiment, "registered_baseline_windows", fixed((full,)))
    monkeypatch.setattr(experiment, "continuous_frame_input_hash", fixed("fixture-frame"))
    monkeypatch.setattr(experiment, "_persist", fixed("fixture-artifact"))
    baseline_windows: list[ContinuousBaselineWindow] = []

    def baseline(**kwargs: Any) -> dict[str, object]:
        baseline_windows.append(kwargs["registered_window"])
        return {"status": "complete" if baseline_complete else "incomplete_source_inputs"}

    monkeypatch.setattr(experiment, "evaluate_continuous_baseline_window", baseline)
    result = asyncio.run(
        experiment.prepare_continuous_experiment(
            study_root=tmp_path,
            registration=cast(Any, registration),
            selection_panel=cast(Any, None),
            windows=(window,),
            profiles=(),
        )
    )
    return result, baseline_windows


def test_optional_candidate_gap_preserved_without_blocking_qualified_preflight(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    report, baselines = _preflight(tmp_path, monkeypatch, qualified=True)
    assert report["source_and_baseline_ready"] is True
    diagnostics = cast(list[dict[str, object]], report["candidate_execution_gaps"])
    assert diagnostics == [
        {
            "window_id": "fixture",
            "day": "2025-01-03",
            "symbol": "510500.SH",
            "reason": "execution_source_incomplete",
            "gaps": ["fixture_source_gap"],
        }
    ]
    assert all(item.sessions == (date(2025, 1, 3), date(2025, 1, 6)) for item in baselines)
    source = cast(list[dict[str, object]], report["source_windows"])[0]
    assert source["calendar"] == ["2025-01-03", "2025-01-06", "2025-01-07"]
    scope = cast(dict[str, object], source["preflight_qualification"])
    assert scope["matched_outcome_window_end"] == "2025-01-06"
    assert scope["horizon_basis"] == "registered_deep_cell"


@pytest.mark.parametrize(
    ("gap_day", "ready"),
    [
        (date(2025, 1, 2), False),
        (date(2025, 1, 6), False),
        (date(2025, 1, 7), True),
    ],
)
def test_held_seed_gaps_only_block_registered_matched_path(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    gap_day: date,
    ready: bool,
) -> None:
    report, _ = _preflight(
        tmp_path, monkeypatch, qualified=True, gap_symbol="510300.SH", gap_day=gap_day
    )
    assert report["source_and_baseline_ready"] is ready


def test_incomplete_matched_baseline_still_blocks(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    report, _ = _preflight(tmp_path, monkeypatch, qualified=True, baseline_complete=False)
    assert report["source_and_baseline_ready"] is False
    assert all(
        row["reason"] == "baseline_incomplete"
        for row in cast(list[dict[str, object]], report["problems"])
    )


def test_legacy_keeps_full_horizon_optional_block_and_payload(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    report, baselines = _preflight(tmp_path, monkeypatch, qualified=False)
    assert report["source_and_baseline_ready"] is False
    assert "candidate_execution_gaps" not in report
    source = cast(list[dict[str, object]], report["source_windows"])[0]
    assert "preflight_qualification" not in source and "candidate_execution_gaps" not in source
    assert all(item.window.outcome_window_end == date(2025, 1, 7) for item in baselines)
