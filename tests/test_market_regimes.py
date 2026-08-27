from __future__ import annotations

import json
import os
from datetime import UTC, datetime
from pathlib import Path
from typing import cast

import pytest

from market_impact_agent.market_regimes import (
    MARKET_REGIME_DATASET_SCHEMA,
    RegimePanel,
    RegimeSeries,
    RegimeTaxonomy,
    capture_regime_panel,
    evaluate_regime_dataset,
    load_market_regime_dataset,
    validate_regime_panel,
    write_regime_panel,
    write_regime_report,
)
from market_impact_agent.tushare import (
    TushareTable,
    tushare_provider_manifest,
    tushare_table_content_hash,
)


def _dataset_payload() -> dict[str, object]:
    core: dict[str, object] = {
        "schema_version": MARKET_REGIME_DATASET_SCHEMA,
        "version": "test-v1",
        "detector": {
            "primary_index": "000300.SH",
            "direction_short_sessions": 20,
            "direction_long_sessions": 60,
            "volatility_sessions": 20,
            "fast_abs_z_threshold": "1.0",
            "feature_lag": "through_previous_session",
        },
        "main_market_indices": ["000300.SH", "000001.SH"],
        "industry_proxy_catalog": [
            {
                "proxy_id": "sw2021_computer",
                "source": "SW2021",
                "industry_name": "计算机",
                "tushare_code": "801750.SI",
            }
        ],
        "cases": [
            {
                "case_key": "synthetic-event",
                "status": "retrospective_research_candidate",
                "identity_sensitive": True,
                "path_start": "2020-04-01",
                "event_anchor": {
                    "observed_at": "2020-04-02T00:00:00Z",
                    "anchor_session": "2020-04-03",
                    "price_anchor": "prior_close",
                    "executable": False,
                },
                "tradable_start": "2020-04-03",
                "end": "2020-04-06",
                "axes": {
                    "path_direction": "up",
                    "path_speed": "fast",
                    "volatility": "high",
                    "drawdown": "material",
                    "recovery": "partial",
                    "narrative_salience": "corroborated_obvious",
                    "causal_complexity": "multi_factor",
                    "causal_directness": "indirect",
                },
                "capability_targets": [
                    "upside_participation",
                    "downside_avoidance",
                    "rotation_selection",
                    "event_latency",
                ],
                "primary_market_index": "000300.SH",
                "required_market_indices": ["000300.SH", "000001.SH"],
                "required_industry_proxies": ["sw2021_computer"],
                "optional_execution_etf_proxies": [],
                "source_refs": ["synthetic-source"],
            },
            {
                "case_key": "synthetic-path",
                "status": "retrospective_research_candidate",
                "identity_sensitive": True,
                "path_start": "2020-04-01",
                "event_anchor": None,
                "tradable_start": "2020-04-02",
                "end": "2020-04-06",
                "axes": {
                    "path_direction": "mixed",
                    "path_speed": "unclassified",
                    "volatility": "elevated",
                    "drawdown": "shallow",
                    "recovery": "unclassified",
                    "narrative_salience": "diffuse",
                    "causal_complexity": "diffuse",
                    "causal_directness": "unavailable",
                },
                "capability_targets": ["whipsaw_control", "ambiguity_abstention"],
                "primary_market_index": "000300.SH",
                "required_market_indices": ["000300.SH", "000001.SH"],
                "required_industry_proxies": ["sw2021_computer"],
                "optional_execution_etf_proxies": [],
                "source_refs": ["synthetic-source"],
            },
        ],
    }
    from market_impact_agent.agent_contracts import canonical_hash

    return {**core, "dataset_id": f"market-regime-dataset-{canonical_hash(core)}"}


def _series(series_id: str, values: tuple[tuple[str, float, float], ...]) -> RegimeSeries:
    return RegimeSeries(
        series_id=series_id,
        kind="market" if series_id.endswith("SH") else "industry",
        tushare_code=series_id,
        source="index_daily" if series_id.endswith("SH") else "sw_daily",
        return_basis="price",
        rows=tuple(
            {"trade_date": day, "open": opening, "close": closing}
            for day, opening, closing in values
        ),
    )


def _taxonomy(*, name: str = "计算机", code: str = "801750.SI") -> RegimeTaxonomy:
    fields = (
        "index_code",
        "industry_name",
        "parent_code",
        "level",
        "industry_code",
        "is_pub",
        "src",
    )
    rows = ((code, name, "", "L1", "710000", "1", "SW2021"),)
    return RegimeTaxonomy(
        source="SW2021",
        level="L1",
        fields=fields,
        rows=rows,
        retrieved_at=datetime(2026, 8, 27, tzinfo=UTC),
        content_hash=tushare_table_content_hash(
            api_name="index_classify",
            params={"level": "L1", "src": "SW2021"},
            fields=fields,
            rows=rows,
        ),
    )


def _panel(dataset_id: str, dataset_hash: str) -> RegimePanel:
    values = (
        ("2020-04-01", 100.0, 100.0),
        ("2020-04-02", 100.0, 110.0),
        ("2020-04-03", 108.0, 115.0),
        ("2020-04-06", 116.0, 120.0),
    )
    return RegimePanel(
        dataset_id=dataset_id,
        dataset_hash=dataset_hash,
        provider_id="tushare-http",
        provider_version="0.1.0",
        historical_vintage="retrieved_historical_not_original_vintage",
        retrieved_at=datetime(2026, 8, 27, tzinfo=UTC),
        industry_taxonomy=_taxonomy(),
        series=(
            _series("000300.SH", values),
            _series(
                "000001.SH",
                tuple((day, opening, closing * 0.9) for day, opening, closing in values),
            ),
            _series(
                "801750.SI",
                tuple((day, opening, closing * 1.1) for day, opening, closing in values),
            ),
        ),
        proxy_resolution=(("sw2021_computer", "801750.SI"),),
    )


def test_dataset_loads_hashes_and_rejects_unanchored_event(tmp_path: Path) -> None:
    path = tmp_path / "dataset.json"
    payload = _dataset_payload()
    path.write_text(json.dumps(payload), encoding="utf-8")

    dataset = load_market_regime_dataset(path)

    assert dataset.dataset_id == payload["dataset_id"]
    assert dataset.dataset_hash
    assert dataset.cases[1].event_anchor is None

    payload["cases"][0]["event_anchor"] = None  # type: ignore[index]
    from market_impact_agent.agent_contracts import canonical_hash

    core = {key: value for key, value in payload.items() if key != "dataset_id"}
    payload["dataset_id"] = f"market-regime-dataset-{canonical_hash(core)}"
    path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ValueError, match="event_anchor"):
        load_market_regime_dataset(path)


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        (
            lambda payload: payload["industry_proxy_catalog"][0].__setitem__("source", "SW2014"),  # type: ignore[index]
            "SW2021",
        ),
        (
            lambda payload: payload["industry_proxy_catalog"][0].pop("tushare_code"),  # type: ignore[index]
            "tushare_code",
        ),
        (
            lambda payload: payload["industry_proxy_catalog"].append(  # type: ignore[index]
                {
                    "proxy_id": "another_computer",
                    "source": "SW2021",
                    "industry_name": "计算机",
                    "tushare_code": "801750.SI",
                }
            ),
            "tushare_codes",
        ),
    ],
)
def test_dataset_rejects_nonconforming_industry_catalog(
    tmp_path: Path,
    mutation: object,
    message: str,
) -> None:
    payload = _dataset_payload()
    cast(object, mutation)(payload)  # type: ignore[operator]
    from market_impact_agent.agent_contracts import canonical_hash

    core = {key: value for key, value in payload.items() if key != "dataset_id"}
    payload["dataset_id"] = f"market-regime-dataset-{canonical_hash(core)}"
    path = tmp_path / "dataset.json"
    path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ValueError, match=message):
        load_market_regime_dataset(path)


def test_private_panel_is_validated_and_evaluation_binds_panel_identity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    path = tmp_path / "dataset.json"
    path.write_text(json.dumps(_dataset_payload()), encoding="utf-8")
    dataset = load_market_regime_dataset(path)
    panel = _panel(dataset.dataset_id, dataset.dataset_hash)

    panel_path = write_regime_panel(panel)
    assert write_regime_panel(panel) == panel_path
    assert panel_path.parent == tmp_path / ".market-impact" / "regime"
    assert oct(panel_path.stat().st_mode & 0o777) == "0o700"
    assert oct((panel_path / "manifest.json").stat().st_mode & 0o777) == "0o600"
    validated = validate_regime_panel(panel_path)
    report = evaluate_regime_dataset(dataset, validated)

    report_cases = cast(list[object], report["cases"])
    event_case = cast(dict[str, object], report_cases[0])
    event_series = cast(dict[str, object], event_case["series"])
    primary_result = cast(dict[str, object], event_series["000300.SH"])
    opportunity = cast(dict[str, object], event_case["sector_opportunity_bounds"])
    assert event_case["status"] == "covered"
    assert primary_result["path_return"] == "0.20000000"
    assert primary_result["event_return"] == "0.09090909"
    assert primary_result["tradable_return"] == "0.11111111"
    assert opportunity["top_return"] == "0.20000000"
    assert opportunity["top_proxy_id"] == "sw2021_computer"
    industry_result = cast(dict[str, object], event_series["sw2021_computer"])
    assert industry_result["path_excess_vs_primary"] == "0.00000000"
    assert report["return_basis"] == "price"
    assert report["historical_vintage"] == "retrieved_historical_not_original_vintage"
    assert report["panel_id"] == validated.panel_id
    assert report["panel_hash"] == validated.panel_hash
    assert report["provider_id"] == "tushare-http"
    assert report["provider_version"] == "0.1.0"
    assert report["retrieved_at"] == "2026-08-27T00:00:00+00:00"
    report_path = write_regime_report(report, validated)
    assert report_path.parent == tmp_path / ".market-impact" / "regime" / "reports"
    assert oct(report_path.stat().st_mode & 0o777) == "0o600"

    incomplete = RegimePanel(
        dataset_id=panel.dataset_id,
        dataset_hash=panel.dataset_hash,
        provider_id=panel.provider_id,
        provider_version=panel.provider_version,
        historical_vintage=panel.historical_vintage,
        retrieved_at=panel.retrieved_at,
        industry_taxonomy=panel.industry_taxonomy,
        series=(
            *panel.series[:2],
            RegimeSeries(
                series_id="sw2021_computer",
                kind="industry",
                tushare_code="801750.SI",
                source="sw_daily",
                return_basis="price",
                rows=panel.series[-1].rows[:-1],
            ),
        ),
        proxy_resolution=panel.proxy_resolution,
    )
    report = evaluate_regime_dataset(dataset, incomplete)
    report_cases = cast(list[object], report["cases"])
    incomplete_case = cast(dict[str, object], report_cases[0])
    incomplete_series = cast(dict[str, object], incomplete_case["series"])
    missing_proxy = cast(dict[str, object], incomplete_series["sw2021_computer"])
    assert incomplete_case["status"] == "incomplete"
    assert missing_proxy["status"] == "missing_anchor"


class _CaptureAdapter:
    def __init__(self, taxonomy: TushareTable) -> None:
        self.manifest = tushare_provider_manifest()
        self.taxonomy = taxonomy
        self.calls: list[str] = []

    def fetch_index_classification(self, *, source: str, level: str) -> TushareTable:
        assert source == "SW2021"
        assert level == "L1"
        self.calls.append("index_classify")
        return self.taxonomy

    def fetch_index_daily(self, **_: str) -> TushareTable:
        self.calls.append("index_daily")
        return _price_table("index_daily")

    def fetch_sw_daily(self, **_: str) -> TushareTable:
        self.calls.append("sw_daily")
        return _price_table("sw_daily")


def _taxonomy_table(*, code: str = "801750.SI", name: str = "计算机") -> TushareTable:
    taxonomy = _taxonomy(name=name, code=code)
    return TushareTable(
        endpoint="https://api.tushare.pro",
        api_name="index_classify",
        params=(("level", "L1"), ("src", "SW2021")),
        fields=taxonomy.fields,
        rows=taxonomy.rows,
        retrieved_at=taxonomy.retrieved_at,
        content_hash=taxonomy.content_hash,
    )


def _price_table(api_name: str) -> TushareTable:
    return TushareTable(
        endpoint="https://api.tushare.pro",
        api_name=api_name,
        params=(),
        fields=("trade_date", "open", "close"),
        rows=(("20200401", "100", "101"),),
        retrieved_at=datetime(2026, 8, 27, tzinfo=UTC),
        content_hash="unused-by-regime-capture",
    )


@pytest.mark.parametrize(
    "taxonomy",
    [
        _taxonomy_table(name="错误名称"),
        _taxonomy_table(code="801760.SI"),
    ],
)
def test_capture_verifies_full_catalog_against_sw2021_before_sw_daily(
    tmp_path: Path,
    taxonomy: TushareTable,
) -> None:
    path = tmp_path / "dataset.json"
    path.write_text(json.dumps(_dataset_payload()), encoding="utf-8")
    adapter = _CaptureAdapter(taxonomy)

    with pytest.raises(ValueError, match="catalog"):
        capture_regime_panel(adapter, load_market_regime_dataset(path))  # type: ignore[arg-type]

    assert adapter.calls == ["index_classify"]


def test_regime_output_root_rejects_escapes_and_uses_fixed_default(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from market_impact_agent.cli import build_parser

    monkeypatch.chdir(tmp_path)
    path = tmp_path / "dataset.json"
    path.write_text(json.dumps(_dataset_payload()), encoding="utf-8")
    dataset = load_market_regime_dataset(path)
    panel = _panel(dataset.dataset_id, dataset.dataset_hash)
    assert write_regime_panel(panel).is_dir()

    parser = build_parser()
    for escaped_root in (".", "..", str(tmp_path)):
        with pytest.raises(SystemExit):
            parser.parse_args(
                [
                    "regime",
                    "capture",
                    "--dataset",
                    str(path),
                    "--output-root",
                    escaped_root,
                ]
            )

    escaped = tmp_path / "escaped"
    escaped.mkdir()
    private_parent = tmp_path / "symlink-worktree" / ".market-impact"
    private_parent.mkdir(parents=True)
    (private_parent / "regime").symlink_to(escaped, target_is_directory=True)
    monkeypatch.chdir(tmp_path / "symlink-worktree")
    with pytest.raises(ValueError, match="symlink"):
        write_regime_panel(panel)


def test_rehashed_semantic_panel_claim_is_rejected(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    path = tmp_path / "dataset.json"
    path.write_text(json.dumps(_dataset_payload()), encoding="utf-8")
    dataset = load_market_regime_dataset(path)
    panel_path = write_regime_panel(_panel(dataset.dataset_id, dataset.dataset_hash))
    manifest = cast(dict[str, object], json.loads((panel_path / "manifest.json").read_text()))
    manifest["provider_version"] = "forged-version"
    from market_impact_agent.agent_contracts import canonical_hash

    core = {key: value for key, value in manifest.items() if key not in {"panel_hash", "panel_id"}}
    panel_hash = canonical_hash(core)
    manifest["panel_hash"] = panel_hash
    manifest["panel_id"] = f"regime-panel-{panel_hash}"
    forged = panel_path.parent / manifest["panel_id"]
    forged.mkdir(mode=0o700)
    os.chmod(forged, 0o700)
    forged_manifest = forged / "manifest.json"
    forged_manifest.write_text(json.dumps(manifest), encoding="utf-8")
    os.chmod(forged_manifest, 0o600)

    with pytest.raises(ValueError, match="provider_version"):
        validate_regime_panel(forged)
