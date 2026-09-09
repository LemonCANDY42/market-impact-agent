# pyright: reportPrivateUsage=false
from __future__ import annotations

import json
from collections.abc import Callable
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from itertools import pairwise
from pathlib import Path
from typing import cast

import pytest

from market_impact_agent.agent_contracts import EvidencePack, EvidenceReference, canonical_hash
from market_impact_agent.cli import build_parser, main
from market_impact_agent.continuous_retrospective import ContinuousRetrospectiveResult
from market_impact_agent.data_inputs import LocalDataSnapshotStore
from market_impact_agent.historical_ashare_inputs import HistoricalAShareInputs
from market_impact_agent.research import EvidenceTier
from market_impact_agent.staged_research_study import (
    _validate_signed_seed_inputs,
    freeze_direction_band,
    load_staged_study_spec,
    preflight_staged_study,
    prepare_staged_study,
    report_staged_study,
    score_direction_opportunity,
    staged_call_graph,
    summarize_direction_opportunities,
)
from tests.test_historical_ashare_inputs import _source

PROFILE = Path("examples/providers/pi-cpa-terra-high-v2.json")


def test_source_first_followup_does_not_require_outcome_labels(tmp_path: Path) -> None:
    spec = json.loads(Path("examples/research/staged-study-1-v1.json").read_text())
    spec["selection_policy"] = "fixed_official_sources_before_reading_target_outcomes"
    for window in spec["windows"]:
        window.pop("development_reversal_case")
    path = tmp_path / "source-first.json"
    path.write_text(json.dumps(spec))
    assert load_staged_study_spec(path) == spec


def _prepare_stage_one_with_research_hashes(
    tmp_path: Path, research_hash: Callable[[LocalDataSnapshotStore, datetime], str]
) -> dict[str, object]:
    store = LocalDataSnapshotStore(tmp_path / "authority")
    inputs = tmp_path / "inputs"
    inputs.mkdir()
    spec = json.loads(Path("examples/research/staged-study-1-v1.json").read_text())
    for window in spec["windows"]:
        cutoff = datetime.fromisoformat(window["decision_session"] + "T09:25:00+08:00")
        artifact_hash = research_hash(store, cutoff)
        (inputs / f"{window['window_id']}.json").write_text(
            json.dumps(
                {
                    "window_id": window["window_id"],
                    "source_policy": {
                        "policy_id": "fixture-modeled-pit",
                        "daily_open_volume_fraction": "0.001",
                    },
                    "records": [
                        {
                            "trade_date": window["decision_session"],
                            "cutoff": cutoff.isoformat(),
                            "research_artifact_hash": artifact_hash,
                            "market_snapshot_ids": [],
                            "rule_artifact_hashes": [],
                            "fund_halt_artifact_hashes": [],
                        }
                    ],
                }
            )
        )
    return prepare_staged_study(
        spec_path=Path("examples/research/staged-study-1-v1.json"),
        output_root=tmp_path / "prepared",
        authority_root=store.root,
        input_root=inputs,
        profile_path=PROFILE,
    )


def _projection() -> tuple[dict[str, object], datetime, list[date]]:
    first = date(2026, 1, 1)
    dates = [first + timedelta(days=index) for index in range(21)]
    cutoff = datetime(2026, 1, 22, 1, 25, tzinfo=UTC)
    return (
        {
            "symbol": "510300.SH",
            "cutoff": cutoff.isoformat(),
            "gaps": [],
            "rows": [
                {
                    "trade_date": day.isoformat(),
                    "cutoff_adjusted_close": str(Decimal(100) + index),
                    "source_record_hash": f"source-{index}",
                    "factor_record_hash": f"factor-{index}",
                }
                for index, day in enumerate(dates)
            ],
        },
        cutoff,
        dates,
    )


def test_direction_band_uses_sample_standard_deviation_and_inclusive_boundary() -> None:
    projection, cutoff, dates = _projection()
    band = freeze_direction_band(
        projection, cutoff=cutoff, horizon_sessions=5, calendar_sessions=dates
    )
    closes = [Decimal(100) + index for index in range(21)]
    returns = [current / prior - 1 for prior, current in pairwise(closes)]
    mean = sum(returns) / Decimal(20)
    expected = (
        Decimal("0.5")
        * (sum((value - mean) ** 2 for value in returns) / Decimal(19)).sqrt()
        * Decimal(5).sqrt()
    )
    assert band.half_width == expected
    assert (
        score_direction_opportunity(prediction="rangebound", realized_return=expected, band=band)[
            "hit"
        ]
        is True
    )
    assert (
        score_direction_opportunity(
            prediction="up", realized_return=expected + Decimal("0.00000001"), band=band
        )["hit"]
        is True
    )


def test_direction_summary_separates_unknown_response_from_direction_coverage() -> None:
    projection, cutoff, dates = _projection()
    band = freeze_direction_band(
        projection, cutoff=cutoff, horizon_sessions=1, calendar_sessions=dates
    )
    rows = [
        score_direction_opportunity(
            prediction="unknown", realized_return=Decimal("0.1"), band=band
        ),
        score_direction_opportunity(prediction="up", realized_return=Decimal("0.1"), band=band),
        score_direction_opportunity(
            prediction=None, realized_return=None, band=band, status="source_insufficient"
        ),
        score_direction_opportunity(
            prediction=None, realized_return=None, band=band, status="system_failed"
        ),
        score_direction_opportunity(
            prediction=None, realized_return=None, band=band, status="budget_stopped"
        ),
    ]
    summary = summarize_direction_opportunities(rows)
    assert rows[0]["response_covered"] is True
    assert rows[0]["direction_covered"] is False
    assert rows[0]["scored"] is False
    assert summary["registered_opportunities"] == 5
    assert summary["completed_responses"] == 2
    assert summary["response_completion_rate"] == "0.4"
    assert summary["direction_covered"] == 1
    assert summary["direction_scored"] == 1
    assert summary["direction_hits"] == 1
    assert summary["unknown_direction"] == 1
    assert summary["status_counts"] == {
        "budget_stopped": 1,
        "completed": 2,
        "source_insufficient": 1,
        "system_failed": 1,
    }


def test_spec_requires_exact_fixed_sessions_and_stage_horizons(tmp_path: Path) -> None:
    raw = json.loads(Path("examples/research/staged-study-3-v1.json").read_text())
    raw["windows"][0]["sessions"][-1] = "2020-02-13"
    path = tmp_path / "invalid.json"
    path.write_text(json.dumps(raw))
    with pytest.raises(ValueError, match="exact ordered session list"):
        load_staged_study_spec(path)


def test_initial_account_binding_requires_completed_signed_account_state() -> None:
    cutoff = datetime(2020, 2, 3, 1, 25, tzinfo=UTC)
    account = "account-ref-source"
    source = {
        "economic_seed": {
            "nav": "98948.060",
            "cash": "50196.86",
            "positions": {"510300.SH": "12200"},
        },
        "bar": {
            "open": "4.081",
            "close": "3.996",
            "session_open_at": "2020-01-23T01:30:00+00:00",
            "session_close_at": "2020-01-23T07:00:00+00:00",
        },
        "market_snapshot_ids": ["data-snapshot-source"],
    }
    state = {
        "complete": True,
        "environment": "backtest",
        "missing_sections": [],
        "reconciliation_gaps": [],
        "open_orders": [],
        "account_reference_hash": account,
        "as_of": "2020-01-23T07:00:00Z",
        "snapshot_id": "account-state-source",
        "cash": [{"currency": "CNY", "available": "50196.86", "settled": "50196.86"}],
        "positions": [{"target_id": "510300.SH", "quantity": "12200", "side": "buy"}],
        "recent_fills": [
            {
                "order_reference": "historical-opening-510300",
                "target_id": "510300.SH",
                "quantity": "12200",
                "filled_at": "2020-01-23T01:30:00Z",
            }
        ],
    }
    inputs: dict[str, object] = {
        "account_state": state,
        "position_snapshot": {
            "account_reference_hash": account,
            "account_state_snapshot_id": "account-state-source",
            "snapshot_id": "position-source",
            "complete": True,
            "observation_gaps": [],
        },
        "exposure_view": {
            "current_gross_exposure": "48751.200",
            "marked_positions": [
                {"instrument_id": "510300.SH", "quantity": "12200", "raw_price": "3.996"}
            ],
        },
        "authorized_view": {
            "cutoff": cutoff.isoformat(),
            "data_snapshot_ids": ["data-snapshot-source"],
            "position_snapshot_id": "position-source",
        },
        "mandate": {"account_id": account},
    }
    _validate_signed_seed_inputs(
        inputs,
        source_inputs=source,
        target="510300.SH",
        account_reference=account,
        cutoff=cutoff,
    )
    state["complete"] = False
    with pytest.raises(ValueError, match="signed account receipt"):
        _validate_signed_seed_inputs(
            inputs,
            source_inputs=source,
            target="510300.SH",
            account_reference=account,
            cutoff=cutoff,
        )


def test_missing_sources_never_erase_stage_three_registered_opportunities(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    writable = LocalDataSnapshotStore(tmp_path / "authority")
    authority_root = writable.root
    input_root = tmp_path / "missing-inputs"
    input_root.mkdir()
    output_root = tmp_path / "prepared"

    def forbidden(*_: object, **__: object) -> None:
        raise AssertionError("offline preparation constructed an execution owner")

    import market_impact_agent.model_budget as model_budget
    import market_impact_agent.pi_runtime as pi_runtime
    import market_impact_agent.streaming_nautilus_account as streaming_account

    monkeypatch.setattr(model_budget.ModelBudget, "__init__", forbidden)
    monkeypatch.setattr(pi_runtime.PiRuntimeProvider, "__init__", forbidden)
    monkeypatch.setattr(streaming_account.HistoricalStreamingAccount, "__init__", forbidden)
    arguments = {
        "spec_path": Path("examples/research/staged-study-3-v1.json"),
        "output_root": output_root,
        "authority_root": authority_root,
        "input_root": input_root,
        "profile_path": PROFILE,
    }
    prepared = prepare_staged_study(**arguments)
    assert prepared["registered_opportunities"] == 20
    windows = cast(list[dict[str, object]], prepared["windows"])
    assert [len(cast(list[object], window["opportunities"])) for window in windows] == [10, 10]
    assert prepared["ready"] is False
    assert "frozen_source_manifest_missing" in cast(list[str], prepared["gaps"])
    preflight = preflight_staged_study(**arguments)
    assert preflight["status"] == "not_ready"
    report = report_staged_study(output_root)
    assert report["registered_opportunities"] == 20
    assert report["registered_arm_opportunities"] == 60


def test_missing_research_cas_keeps_present_manifest_opportunities(tmp_path: Path) -> None:
    prepared = _prepare_stage_one_with_research_hashes(tmp_path, lambda _store, _cutoff: "f" * 64)
    windows = cast(list[dict[str, object]], prepared["windows"])
    opportunities = [
        opportunity
        for window in windows
        for opportunity in cast(list[dict[str, object]], window["opportunities"])
    ]
    assert len(opportunities) == 2
    assert all(
        "window_source_artifact_missing:research_artifact" in cast(list[str], item["gaps"])
        for item in opportunities
    )
    assert report_staged_study(tmp_path / "prepared")["registered_arm_opportunities"] == 4


def test_missing_reference_source_cas_keeps_all_report_rows(tmp_path: Path) -> None:
    def research(store: LocalDataSnapshotStore, cutoff: datetime) -> str:
        document: dict[str, object] = {"symbol": "510300.SH", "rows": []}
        reference = EvidenceReference(
            "price-fixture",
            "completed-session-price-history",
            "sha256:" + "e" * 64,
            EvidenceTier.REGULATED,
            cutoff,
            canonical_hash(document),
            "Fixture completed prices.",
        )
        pack = EvidencePack.build(
            event_id="fixture-event",
            as_of=cutoff,
            research_question="Fixture question?",
            evidence=(reference,),
            pattern_packs=(),
            allowed_targets=("510300.SH",),
        )
        return store.artifacts.put_json(
            {"evidence_pack": pack.to_dict(), "documents": {"price-fixture": document}}
        ).content_hash

    prepared = _prepare_stage_one_with_research_hashes(tmp_path, research)
    windows = cast(list[dict[str, object]], prepared["windows"])
    opportunities = [
        opportunity
        for window in windows
        for opportunity in cast(list[dict[str, object]], window["opportunities"])
    ]
    assert len(opportunities) == 2
    assert all(
        "window_source_artifact_missing:reference_source_artifact" in cast(list[str], item["gaps"])
        for item in opportunities
    )
    assert report_staged_study(tmp_path / "prepared")["registered_arm_opportunities"] == 4


def _prepare_stage_one_with_market(
    tmp_path: Path, market: HistoricalAShareInputs
) -> dict[str, object]:
    inputs = tmp_path / "market-inputs"
    inputs.mkdir()
    spec = json.loads(Path("examples/research/staged-study-1-v1.json").read_text())
    for window in spec["windows"]:
        cutoff = datetime.fromisoformat(window["decision_session"] + "T09:25:00+08:00")
        (inputs / f"{window['window_id']}.json").write_text(
            json.dumps(
                {
                    "window_id": window["window_id"],
                    "source_policy": market.policy.to_dict(),
                    "records": [
                        {
                            "trade_date": window["decision_session"],
                            "cutoff": cutoff.isoformat(),
                            "research_artifact_hash": "f" * 64,
                            "market_snapshot_ids": list(market.snapshot_ids),
                            "rule_artifact_hashes": list(market.rule_artifact_hashes),
                            "fund_halt_artifact_hashes": list(market.fund_halt_artifact_hashes),
                        }
                    ],
                }
            )
        )
    return prepare_staged_study(
        spec_path=Path("examples/research/staged-study-1-v1.json"),
        output_root=tmp_path / "market-prepared",
        authority_root=market.store.root,
        input_root=inputs,
        profile_path=PROFILE,
    )


@pytest.mark.parametrize("missing", ("raw", "config"))
def test_missing_captured_market_cas_keeps_entire_two_window_denominator(
    tmp_path: Path, missing: str
) -> None:
    source = _source(tmp_path)
    snapshot = source.store.get(source.snapshot_ids[0])
    content_hash = (
        snapshot.observations[0].raw_content_hash
        if missing == "raw"
        else snapshot.query.sources[0].source_config_hash
    )
    assert content_hash is not None
    (source.store.artifacts.root / content_hash).unlink()

    prepared = _prepare_stage_one_with_market(tmp_path, source)

    assert prepared["registered_opportunities"] == 2
    assert prepared["gaps"] == ["window_source_artifact_missing:market_source_cas"]
    windows = cast(list[dict[str, object]], prepared["windows"])
    assert [len(cast(list[object], window["opportunities"])) for window in windows] == [1, 1]
    assert report_staged_study(tmp_path / "market-prepared")["registered_arm_opportunities"] == 4


def test_tampered_captured_market_cas_remains_a_hard_failure(tmp_path: Path) -> None:
    source = _source(tmp_path)
    snapshot = source.store.get(source.snapshot_ids[0])
    path = source.store.artifacts.root / snapshot.observations[0].raw_content_hash
    path.write_bytes(b"tampered")
    with pytest.raises(ValueError, match="artifact content does not match"):
        _prepare_stage_one_with_market(tmp_path, source)


@pytest.mark.parametrize(
    ("stage", "role_runs", "requests", "group_max_cost"),
    (
        (1, 12, 192, 120_176_640),
        (2, 24, 384, 240_353_280),
        (3, 360, 5_760, 3_605_299_200),
    ),
)
def test_call_graph_bounds_complete_stage_without_authorizing_spend(
    stage: int, role_runs: int, requests: int, group_max_cost: int
) -> None:
    spec = load_staged_study_spec(Path(f"examples/research/staged-study-{stage}-v1.json"))
    graph = staged_call_graph(spec, PROFILE)
    assert graph["physical_requests_per_run"] == 16
    assert graph["physical_input_token_cap"] == 263_808
    assert graph["physical_output_token_cap"] == 8_192
    assert graph["physical_request_token_cap_microusd"] == 625_920
    assert graph["token_envelope_per_run_microusd"] == 10_014_720
    assert graph["successful_model_usage_cap_per_run_microusd"] == 300_000
    assert graph["successful_usage_cap_excludes_unsettled_attempt_reservations"] is True
    assert graph["group_maximum_role_runs"] == role_runs
    assert graph["group_maximum_physical_requests"] == requests
    assert graph["group_max_cost_microusd"] == group_max_cost
    assert graph["budget_authorization_created"] is False
    surface = cast(dict[str, object], graph["runtime_surface"])
    assert surface["surface_hash"] == canonical_hash(
        {key: value for key, value in surface.items() if key != "surface_hash"}
    )


def test_staged_cli_requires_explicit_source_paths_and_never_routes_run(
    capsys: pytest.CaptureFixture[str],
) -> None:
    parsed = build_parser().parse_args(
        [
            "agent",
            "continuous-study",
            "prepare",
            "--study-spec",
            "examples/research/staged-study-1-v1.json",
            "--research-inputs-root",
            "private-inputs",
            "--authority-root",
            "private-authority",
            "--provider-profile",
            str(PROFILE),
        ]
    )
    assert parsed.study_spec == Path("examples/research/staged-study-1-v1.json")
    assert parsed.research_inputs_root == Path("private-inputs")
    assert parsed.authority_root == Path("private-authority")

    assert (
        main(
            [
                "agent",
                "continuous-study",
                "run",
                "--study-spec",
                "examples/research/staged-study-1-v1.json",
            ]
        )
        == 1
    )
    assert json.loads(capsys.readouterr().out)["error_class"] == "PermissionError"


def test_retrospective_cli_persists_private_detail_and_prints_only_aggregates(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    import market_impact_agent.continuous_retrospective as retrospective_module

    private_row: dict[str, object] = {
        "run_id": "private-run",
        "signed_source": "private-source",
    }
    result = ContinuousRetrospectiveResult(
        costs={"current_identity_runs": {"estimated_cost_microusd": 11}},
        theses=[private_row],
        thesis_summary=[{"stage": "rolling", "theses": 1}],
        proxy_price_maps={"private-window": {}},
        proxy_outcomes=[private_row],
        proxy_outcome_summary={"groups": [{"stage": "rolling", "n": 1}]},
        matched_momentum=[
            {
                "stage": "rolling",
                "model": "terra_high",
                "n": 1,
                "model_hits": 1,
                "momentum_hits": 0,
                "pairs": [private_row],
            }
        ],
    )
    observed: list[object] = []

    def fake(paths: object) -> ContinuousRetrospectiveResult:
        observed.append(paths)
        return result

    monkeypatch.setattr(retrospective_module, "run_continuous_retrospective", fake)
    output = tmp_path / "retrospective"
    assert (
        main(
            [
                "agent",
                "continuous-study",
                "retrospective",
                "--state-root",
                str(output),
                "--report-path",
                "completed-report.json",
                "--preflight-hash",
                "a" * 64,
                "--authority-root",
                "runtime-authority",
            ]
        )
        == 0
    )
    public = json.loads(capsys.readouterr().out)
    assert public["stage_passed"] is True
    assert public["costs"] == result.costs
    assert "private-run" not in json.dumps(public)
    private_path = output / "continuous-retrospective-private.json"
    assert json.loads(private_path.read_text())["theses"] == [private_row]
    assert private_path.stat().st_mode & 0o777 == 0o600
    assert observed
