from __future__ import annotations

import json
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any, cast

import pytest

from market_impact_agent.agent_contracts import (
    EvidencePack,
    EvidenceReference,
    PatternPackReference,
    canonical_hash,
    pattern_pack_from_dict,
)
from market_impact_agent.dynamic_effectiveness_cadence import preflight_cadence_evidence
from market_impact_agent.dynamic_effectiveness_outcomes import (
    open_dynamic_analysis_outcomes,
    score_dynamic_result,
    summarize_memory_sensitivity,
)
from market_impact_agent.dynamic_effectiveness_runner import prepare_dynamic_effectiveness_study
from market_impact_agent.market_regimes import (
    RegimePanel,
    RegimeSeries,
    RegimeTaxonomy,
    write_regime_panel,
)
from market_impact_agent.model_provider import load_model_provider_profile
from market_impact_agent.research import EvidenceTier
from market_impact_agent.tushare import tushare_table_content_hash


def _profiles() -> tuple[Any, Any, Any]:
    root = Path(__file__).parents[1] / "examples" / "providers"
    return cast(
        tuple[Any, Any, Any],
        tuple(
            load_model_provider_profile(root / name)
            for name in (
                "pi-cpa-luna-max-v2.json",
                "pi-cpa-terra-high-v2.json",
                "pi-cpa-sol-high-v2.json",
            )
        ),
    )


_INPUTS = (
    "cn-2018-bear-market/2018-07-02",
    "cn-2019-q1-fast-rebound/2019-01-07",
    "cn-2020-covid-closure-shock/2020-02-03",
    "cn-2020-covid-closure-shock/2020-03-23",
    "cn-2021-index-flat-sector-rotation/2021-07-01",
    "cn-2021-index-flat-sector-rotation/2021-12-01",
    "cn-2024-policy-melt-up/2024-09-24",
    "cn-2024-post-rally-whipsaw/2024-10-09",
)


def _inputs(tmp_path: Path) -> tuple[Path, Path]:
    source = Path(__file__).parents[1] / "examples/agent/market_regime/pattern-pack.json"
    pattern_path = tmp_path / "pattern-pack.json"
    pattern_path.write_bytes(source.read_bytes())
    pattern = pattern_pack_from_dict(json.loads(pattern_path.read_text()))
    inputs_root = tmp_path / "inputs"
    for ref in _INPUTS:
        at = datetime.fromisoformat(ref.rsplit("/", maxsplit=1)[-1]).replace(
            hour=1, minute=25, tzinfo=UTC
        )
        release = {"published_at": (at - timedelta(hours=1)).isoformat(), "fact": "fact"}
        evidence = (
            EvidenceReference(
                "release",
                "incremental-fact",
                "official://test/release",
                EvidenceTier.OFFICIAL,
                at - timedelta(hours=1),
                canonical_hash(release),
                "Frozen release evidence.",
            ),
        )
        pack = EvidencePack.build(
            event_id=f"event-{canonical_hash(ref)[:16]}",
            as_of=at,
            research_question="Opened development question.",
            evidence=evidence,
            pattern_packs=(
                PatternPackReference(
                    pattern.pack_id,
                    pattern.version,
                    pattern.available_at,
                    canonical_hash(pattern.to_dict()),
                ),
            ),
            allowed_targets=("broad-market-a",),
            data_gaps=("Modeled-PIT, not Strict-PIT.",),
        )
        destination = inputs_root / ref
        destination.mkdir(parents=True)
        (destination / "evidence-pack.json").write_text(json.dumps(pack.to_dict()))
        (destination / "evidence-documents.json").write_text(
            json.dumps({"documents": {"release": release}})
        )
    return inputs_root, pattern_path


def _panel() -> RegimePanel:
    fields = (
        "index_code",
        "industry_name",
        "parent_code",
        "level",
        "industry_code",
        "is_pub",
        "src",
    )
    taxonomy_rows = (("801750.SI", "Computer", "", "L1", "710000", "1", "SW2021"),)
    taxonomy = RegimeTaxonomy(
        source="SW2021",
        level="L1",
        fields=fields,
        rows=taxonomy_rows,
        retrieved_at=datetime(2026, 8, 27, tzinfo=UTC),
        content_hash=tushare_table_content_hash(
            api_name="index_classify",
            params={"level": "L1", "src": "SW2021"},
            fields=fields,
            rows=taxonomy_rows,
        ),
    )
    current = date(2018, 1, 1)
    rows: list[dict[str, object]] = []
    value = 100
    while current <= date(2025, 4, 1):
        if current.weekday() < 5:
            rows.append(
                {
                    "trade_date": current.strftime("%Y%m%d"),
                    "open": str(value),
                    "close": str(value + 1),
                }
            )
            value += 1
        current += timedelta(days=1)
    dataset_hash = canonical_hash("synthetic-dynamic-panel")
    return RegimePanel(
        dataset_id=f"market-regime-dataset-{dataset_hash}",
        dataset_hash=dataset_hash,
        provider_id="tushare-http",
        provider_version="0.1.0",
        historical_vintage="retrieved_historical_not_original_vintage",
        retrieved_at=datetime(2026, 8, 27, tzinfo=UTC),
        industry_taxonomy=taxonomy,
        series=(
            RegimeSeries(
                series_id="000300.SH",
                kind="market",
                tushare_code="000300.SH",
                source="index_daily",
                return_basis="price",
                rows=tuple(rows),
            ),
        ),
        proxy_resolution=(),
    )


def _thesis(direction: str, horizon: int) -> dict[str, object]:
    return {
        "base_case_direction": direction,
        "primary_horizon_sessions": horizon,
    }


def test_dynamic_outcome_scoring_follows_every_session_and_keeps_memory_claim_narrow(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    inputs_root, pattern = _inputs(tmp_path)
    root = tmp_path / "study"
    registration = prepare_dynamic_effectiveness_study(
        root,
        inputs_root=inputs_root,
        pattern_pack_path=pattern,
        profiles=_profiles(),
        registered_at=datetime(2026, 9, 4, tzinfo=UTC),
    )
    results: list[dict[str, object]] = []
    cases = cast(list[dict[str, object]], registration["opened_case_sources"])
    for source in cases:
        for topology in ("luna_max", "terra_high", "sol_high"):
            results.append(
                {
                    "run_id": f"{source['case_id']}.{topology}",
                    "case_id": source["case_id"],
                    "topology": topology,
                    "repetition": "base",
                    "date_presentation": "true_date",
                    "status": "completed",
                    "thesis": _thesis("up", 3),
                }
            )
    for case_id in ("2018-07-02", "2019-01-07", "2020-02-03"):
        for topology in ("luna_max", "terra_high", "sol_high"):
            results.append(
                {
                    "run_id": f"{case_id}.{topology}.repeat",
                    "case_id": case_id,
                    "topology": topology,
                    "repetition": "stability-repeat",
                    "date_presentation": "true_date",
                    "status": "completed",
                    "thesis": _thesis("up", 3),
                }
            )
    results.append(
        {
            "run_id": "2020-02-03.luna.relative",
            "case_id": "2020-02-03",
            "topology": "luna_max",
            "repetition": "memory-sensitivity",
            "date_presentation": "relative_offset",
            "status": "completed",
            "thesis": _thesis("up", 3),
        }
    )
    analysis: dict[str, object] = {
        "schema_version": "market-impact.dynamic-opened-analysis-report.v1",
        "registration_hash": registration["registration_hash"],
        "runtime": registration["runtime"],
        "results": results,
        "outcomes_opened": False,
        "promotion": False,
        "live_execution": False,
    }
    analysis["report_hash"] = canonical_hash(analysis)
    (root / "opened-analysis-report.json").write_text(json.dumps(analysis), encoding="utf-8")
    series = next(item for item in _panel().series if item.tushare_code == "000300.SH")
    first = score_dynamic_result(results[0], cast(dict[str, object], results[0]["thesis"]), series)
    assert first["primary_horizon_sessions"] == 3
    assert first["window"] == ["20180702", "20180704"]
    assert [
        item["session_offset"] for item in cast(list[dict[str, object]], first["daily_path"])
    ] == [
        1,
        2,
        3,
    ]
    memory_rows = [
        score_dynamic_result(item, cast(dict[str, object], item["thesis"]), series)
        for item in results
        if item["case_id"] == "2020-02-03" and item["topology"] == "luna_max"
    ]
    assert summarize_memory_sensitivity(memory_rows)["direction_and_horizon_match"] is True

    panel_path = write_regime_panel(_panel())
    with pytest.raises(ValueError, match="authoritative terminal"):
        open_dynamic_analysis_outcomes(
            root,
            panel_directory=panel_path,
            index_proxy="000300.SH",
            reviewed_at=datetime(2026, 9, 4, 12, tzinfo=UTC),
        )


def test_existing_outcome_report_reopens_analysis_and_panel_authorities(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    root = tmp_path / "study"
    root.mkdir()
    registration: dict[str, object] = {"registration_hash": "registered-study"}
    analysis: dict[str, object] = {"results": []}

    def load_registration(_: Path) -> dict[str, object]:
        return registration

    def load_analysis(_: Path) -> dict[str, object]:
        return analysis

    monkeypatch.setattr(
        "market_impact_agent.dynamic_effectiveness_outcomes.load_dynamic_effectiveness_study",
        load_registration,
    )
    monkeypatch.setattr(
        "market_impact_agent.dynamic_effectiveness_outcomes.load_verified_opened_analysis_report",
        load_analysis,
    )
    panel_path = write_regime_panel(_panel())
    reviewed_at = datetime(2026, 9, 4, 12, tzinfo=UTC)
    original = open_dynamic_analysis_outcomes(
        root,
        panel_directory=panel_path,
        index_proxy="000300.SH",
        reviewed_at=reviewed_at,
    )
    assert original["analysis_report_hash"] == canonical_hash(analysis)

    changed = {**original, "claims": "fabricated but self-hashed result"}
    changed["report_hash"] = canonical_hash(
        {key: value for key, value in changed.items() if key != "report_hash"}
    )
    (root / "opened-analysis-outcomes.json").write_text(json.dumps(changed), encoding="utf-8")

    with pytest.raises(ValueError, match="differs from its frozen authorities"):
        open_dynamic_analysis_outcomes(
            root,
            panel_directory=panel_path,
            index_proxy="000300.SH",
            reviewed_at=reviewed_at + timedelta(days=1),
        )


def test_cadence_preflight_rejects_sparse_checkpoints_without_model_calls(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    inputs_root, pattern = _inputs(tmp_path)
    # The preflight needs the other registered sparse successors as well.
    for family, dates in {
        "cn-2020-covid-closure-shock": ("2020-03-02",),
        "cn-2024-post-rally-whipsaw": ("2024-11-18", "2024-12-30"),
        "cn-2024-policy-melt-up": ("2024-09-30", "2024-10-08"),
        "cn-2019-q1-fast-rebound": ("2019-02-25", "2019-04-08"),
    }.items():
        template = inputs_root / family / next((inputs_root / family).iterdir()).name
        for day in dates:
            destination = inputs_root / family / day
            if destination.exists():
                continue
            destination.mkdir(parents=True)
            pack = json.loads((template / "evidence-pack.json").read_text())
            pack["as_of"] = f"{day}T01:25:00+00:00"
            (destination / "evidence-pack.json").write_text(json.dumps(pack))
            (destination / "evidence-documents.json").write_bytes(
                (template / "evidence-documents.json").read_bytes()
            )
    # 2020-03-23 is already one of the eight opened inputs.
    root = tmp_path / "study"
    prepare_dynamic_effectiveness_study(
        root,
        inputs_root=inputs_root,
        pattern_pack_path=pattern,
        profiles=_profiles(),
        registered_at=datetime(2026, 9, 4, tzinfo=UTC),
    )
    panel_path = write_regime_panel(_panel())

    report = preflight_cadence_evidence(
        root,
        inputs_root=inputs_root,
        panel_directory=panel_path,
        index_proxy="000300.SH",
    )

    assert report["scheduled_review_ready"] is False
    assert report["paid_cadence_run_admitted"] is False
    assert report["one_shot_ready"] is False
    assert report["model_requests"] == 0
    assert "D1-through-D60" in cast(str, report["blocker"])
