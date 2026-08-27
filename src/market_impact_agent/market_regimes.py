from __future__ import annotations

import json
import math
import os
import re
import stat
import tempfile
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import ROUND_HALF_EVEN, Decimal, InvalidOperation
from pathlib import Path
from typing import Protocol, cast

from jsonschema import Draft202012Validator, FormatChecker, ValidationError

from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.tushare import (
    TUSHARE_ADAPTER_VERSION,
    TUSHARE_PROVIDER_ID,
    TushareHttpAdapter,
    TushareTable,
    tushare_table_content_hash,
)

MARKET_REGIME_DATASET_SCHEMA = "market-impact.market-regime-dataset.v1"
REGIME_PANEL_SCHEMA = "market-impact.regime-panel.v1"
REGIME_REPORT_SCHEMA = "market-impact.regime-capability-report.v1"

_RETURN_QUANTUM = Decimal("0.00000001")
_PRIMARY_STATES = frozenset({"up_fast", "up_mild", "down_fast", "down_mild", "unclassified"})
_PATH_SPEEDS = frozenset({"fast", "mild", "unclassified"})
_EVENT_PRICE_ANCHORS = frozenset({"prior_close", "session_open", "session_close"})
_HISTORICAL_VINTAGE = "retrieved_historical_not_original_vintage"
_SW2021_SOURCE = "SW2021"
_SW2021_LEVEL = "L1"
_MARKET_INDEX_CODE = re.compile(r"^[0-9]{6}\.(?:SH|SZ)$")
_INDUSTRY_INDEX_CODE = re.compile(r"^[0-9]{6}\.SI$")
_CONTENT_HASH = re.compile(r"^[0-9a-f]{64}$")
_REGIME_STORAGE_RELATIVE = Path(".market-impact") / "regime"


class _JsonSchemaValidator(Protocol):
    def iter_errors(self, instance: object) -> Iterable[ValidationError]: ...


@dataclass(frozen=True, slots=True)
class EventAnchor:
    observed_at: datetime
    anchor_session: date
    price_anchor: str
    executable: bool


@dataclass(frozen=True, slots=True)
class MarketRegimeCase:
    case_key: str
    path_start: date
    event_anchor: EventAnchor | None
    tradable_start: date
    end: date
    axes: dict[str, str]
    capability_targets: tuple[str, ...]
    primary_market_index: str
    required_market_indices: tuple[str, ...]
    required_industry_proxies: tuple[str, ...]
    source_refs: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class MarketRegimeDataset:
    dataset_id: str
    dataset_hash: str
    version: str
    detector: dict[str, object]
    main_market_indices: tuple[str, ...]
    industry_proxy_catalog: tuple[dict[str, str], ...]
    cases: tuple[MarketRegimeCase, ...]


@dataclass(frozen=True, slots=True)
class RegimeSeries:
    series_id: str
    kind: str
    tushare_code: str
    source: str
    return_basis: str
    rows: tuple[dict[str, object], ...]


@dataclass(frozen=True, slots=True)
class RegimeTaxonomy:
    source: str
    level: str
    fields: tuple[str, ...]
    rows: tuple[tuple[object, ...], ...]
    retrieved_at: datetime
    content_hash: str


@dataclass(frozen=True, slots=True)
class RegimePanel:
    dataset_id: str
    dataset_hash: str
    provider_id: str
    provider_version: str
    historical_vintage: str
    retrieved_at: datetime
    industry_taxonomy: RegimeTaxonomy
    series: tuple[RegimeSeries, ...]
    proxy_resolution: tuple[tuple[str, str], ...]


@dataclass(frozen=True, slots=True)
class ValidatedRegimePanel:
    path: Path
    panel_id: str
    panel_hash: str
    panel: RegimePanel


def load_market_regime_dataset(path: Path) -> MarketRegimeDataset:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("market regime dataset must be an object")
    body = cast(dict[str, object], payload)
    _validate_json_schema(body, "market-regime-dataset.schema.json")
    if body.get("schema_version") != MARKET_REGIME_DATASET_SCHEMA:
        raise ValueError("unsupported market regime dataset schema_version")
    dataset_id = _required_string(body, "dataset_id")
    core = {key: value for key, value in body.items() if key != "dataset_id"}
    dataset_hash = canonical_hash(core)
    if dataset_id != f"market-regime-dataset-{dataset_hash}":
        raise ValueError("dataset_id does not match canonical dataset content")

    detector = _required_mapping(body, "detector")
    if detector.get("feature_lag") != "through_previous_session":
        raise ValueError("detector feature_lag must be through_previous_session")
    threshold = _decimal(detector.get("fast_abs_z_threshold"), "fast_abs_z_threshold")
    if threshold <= 0:
        raise ValueError("fast_abs_z_threshold must be positive")
    main_indices = _string_tuple(body.get("main_market_indices"), "main_market_indices")
    if len(main_indices) != len(set(main_indices)) or not main_indices:
        raise ValueError("main_market_indices must be non-empty and unique")
    if any(_MARKET_INDEX_CODE.fullmatch(code) is None for code in main_indices):
        raise ValueError("main_market_indices must use six-digit .SH or .SZ Tushare codes")
    if _required_string(detector, "primary_index") not in main_indices:
        raise ValueError("detector primary_index must be a registered market index")

    catalog_raw = body.get("industry_proxy_catalog")
    if not isinstance(catalog_raw, list) or not catalog_raw:
        raise ValueError("industry_proxy_catalog must be a non-empty array")
    catalog: list[dict[str, str]] = []
    proxy_ids: set[str] = set()
    proxy_codes: set[str] = set()
    for raw in cast(list[object], catalog_raw):
        item = _mapping(raw, "industry_proxy_catalog item")
        source = _required_string(item, "source")
        if source != _SW2021_SOURCE:
            raise ValueError("industry proxy source must be SW2021")
        tushare_code = _required_string(item, "tushare_code")
        if _INDUSTRY_INDEX_CODE.fullmatch(tushare_code) is None:
            raise ValueError("industry proxy tushare_code must use six-digit .SI format")
        normalized = {
            "proxy_id": _required_string(item, "proxy_id"),
            "source": source,
            "industry_name": _required_string(item, "industry_name"),
            "tushare_code": tushare_code,
        }
        if normalized["proxy_id"] in proxy_ids:
            raise ValueError("industry proxy ids must be unique")
        if normalized["tushare_code"] in proxy_codes:
            raise ValueError("industry proxy tushare_codes must be unique")
        proxy_ids.add(normalized["proxy_id"])
        proxy_codes.add(normalized["tushare_code"])
        catalog.append(normalized)

    cases_raw = body.get("cases")
    if not isinstance(cases_raw, list) or not cases_raw:
        raise ValueError("cases must be a non-empty array")
    cases: list[MarketRegimeCase] = []
    case_keys: set[str] = set()
    for raw in cast(list[object], cases_raw):
        item = _mapping(raw, "case")
        case_key = _required_string(item, "case_key")
        if case_key in case_keys:
            raise ValueError("case keys must be unique")
        case_keys.add(case_key)
        if item.get("status") != "retrospective_research_candidate":
            raise ValueError("cases must remain retrospective_research_candidate")
        if item.get("identity_sensitive") is not True:
            raise ValueError("cases must remain identity_sensitive")
        path_start = date.fromisoformat(_required_string(item, "path_start"))
        tradable_start = date.fromisoformat(_required_string(item, "tradable_start"))
        end = date.fromisoformat(_required_string(item, "end"))
        if not path_start <= tradable_start <= end:
            raise ValueError("case anchors must satisfy path_start <= tradable_start <= end")
        event_anchor = _event_anchor(item.get("event_anchor"))
        if event_anchor is not None and not (
            path_start <= event_anchor.anchor_session <= tradable_start
        ):
            raise ValueError("event anchor session must fall between path start and tradable start")
        if (
            event_anchor is not None
            and event_anchor.observed_at.date() > event_anchor.anchor_session
        ):
            raise ValueError("event observation cannot follow its price anchor session")
        axes = {
            key: _nonempty_string(value, f"axes.{key}")
            for key, value in _required_mapping(item, "axes").items()
        }
        required_axes = {
            "path_direction",
            "path_speed",
            "volatility",
            "drawdown",
            "recovery",
            "narrative_salience",
            "causal_complexity",
            "causal_directness",
        }
        if set(axes) != required_axes:
            raise ValueError("axes must contain the frozen orthogonal descriptor set")
        if axes["path_speed"] not in _PATH_SPEEDS:
            raise ValueError("path_speed must be fast, mild, or unclassified")
        source_refs = _string_tuple(item.get("source_refs"), "source_refs")
        if not source_refs:
            raise ValueError("source_refs must not be empty")
        if event_anchor is None and axes["narrative_salience"] not in {
            "diffuse",
            "contested",
            "unavailable",
        }:
            raise ValueError("a salient event case requires event_anchor")
        required_indices = (
            main_indices
            if item.get("required_market_indices") is None
            else _string_tuple(item.get("required_market_indices"), "required_market_indices")
        )
        primary_index = _required_string(item, "primary_market_index")
        if primary_index not in required_indices or not set(required_indices) <= set(main_indices):
            raise ValueError("case market indices must be registered and include the primary")
        required_proxies = (
            tuple(proxy["proxy_id"] for proxy in catalog)
            if item.get("required_industry_proxies") is None
            else _string_tuple(item.get("required_industry_proxies"), "required_industry_proxies")
        )
        if not set(required_proxies) <= proxy_ids:
            raise ValueError("case references an unregistered industry proxy")
        cases.append(
            MarketRegimeCase(
                case_key=case_key,
                path_start=path_start,
                event_anchor=event_anchor,
                tradable_start=tradable_start,
                end=end,
                axes=axes,
                capability_targets=_string_tuple(
                    item.get("capability_targets"), "capability_targets"
                ),
                primary_market_index=primary_index,
                required_market_indices=required_indices,
                required_industry_proxies=required_proxies,
                source_refs=source_refs,
            )
        )
    return MarketRegimeDataset(
        dataset_id=dataset_id,
        dataset_hash=dataset_hash,
        version=_required_string(body, "version"),
        detector=dict(detector),
        main_market_indices=main_indices,
        industry_proxy_catalog=tuple(catalog),
        cases=tuple(cases),
    )


def capture_regime_panel(
    adapter: TushareHttpAdapter,
    dataset: MarketRegimeDataset,
) -> RegimePanel:
    start = min(case.path_start for case in dataset.cases) - timedelta(days=400)
    end = max(case.end for case in dataset.cases)
    start_text = start.strftime("%Y%m%d")
    end_text = end.strftime("%Y%m%d")
    series: list[RegimeSeries] = []
    taxonomy_table = adapter.fetch_index_classification(
        source=_SW2021_SOURCE,
        level=_SW2021_LEVEL,
    )
    taxonomy = _taxonomy_from_table(taxonomy_table)
    _verify_catalog_against_taxonomy(dataset.industry_proxy_catalog, taxonomy)
    retrieved: list[datetime] = [taxonomy.retrieved_at]
    for code in dataset.main_market_indices:
        table = adapter.fetch_index_daily(
            tushare_code=code, start_date=start_text, end_date=end_text
        )
        series.append(_series_from_table(code, "market", code, "index_daily", table))
        retrieved.append(table.retrieved_at)
    resolution: list[tuple[str, str]] = []
    for proxy in dataset.industry_proxy_catalog:
        code = proxy["tushare_code"]
        table = adapter.fetch_sw_daily(tushare_code=code, start_date=start_text, end_date=end_text)
        proxy_id = proxy["proxy_id"]
        series.append(_series_from_table(proxy_id, "industry", code, "sw_daily", table))
        resolution.append((proxy_id, code))
        retrieved.append(table.retrieved_at)
    return RegimePanel(
        dataset_id=dataset.dataset_id,
        dataset_hash=dataset.dataset_hash,
        provider_id=adapter.manifest.provider_id,
        provider_version=adapter.manifest.provider_version,
        historical_vintage=_HISTORICAL_VINTAGE,
        retrieved_at=max(retrieved),
        industry_taxonomy=taxonomy,
        series=tuple(series),
        proxy_resolution=tuple(resolution),
    )


def write_regime_panel(panel: RegimePanel) -> Path:
    output_root = _private_regime_root()
    _validate_regime_panel_content(panel)
    core = _panel_core(panel)
    panel_hash = canonical_hash(core)
    panel_id = f"regime-panel-{panel_hash}"
    destination = output_root / panel_id
    if destination.exists() or destination.is_symlink():
        validated = validate_regime_panel(destination)
        if validated.panel_hash != panel_hash:
            raise FileExistsError(f"conflicting regime panel exists: {destination}")
        return destination
    temporary = Path(tempfile.mkdtemp(prefix=".tmp-regime-", dir=output_root))
    os.chmod(temporary, 0o700)
    manifest_path = temporary / "manifest.json"
    manifest_path.write_text(
        json.dumps(
            {**core, "panel_hash": panel_hash, "panel_id": panel_id},
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    os.chmod(manifest_path, 0o600)
    temporary.rename(destination)
    return destination


def write_regime_report(
    report: dict[str, object],
    validated_panel: ValidatedRegimePanel,
) -> Path:
    output_root = _private_regime_root()
    reports = output_root / "reports"
    _create_private_directory(reports)
    destination = reports / f"{validated_panel.panel_id}.json"
    if destination.is_symlink():
        raise ValueError("regime report destination must not be a symlink")
    if destination.exists() and (
        not destination.is_file() or stat.S_IMODE(destination.stat().st_mode) != 0o600
    ):
        raise ValueError("regime report destination must be a real 0600 file")
    descriptor, temporary_name = tempfile.mkstemp(prefix=".tmp-regime-report-", dir=reports)
    os.close(descriptor)
    temporary = Path(temporary_name)
    try:
        os.chmod(temporary, 0o600)
        temporary.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        temporary.replace(destination)
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise
    return destination


def validate_regime_panel(path: Path) -> ValidatedRegimePanel:
    if path.is_symlink() or not path.is_dir() or stat.S_IMODE(path.stat().st_mode) != 0o700:
        raise ValueError("regime panel directory must be a real 0700 directory")
    manifest = path / "manifest.json"
    if (
        manifest.is_symlink()
        or not manifest.is_file()
        or stat.S_IMODE(manifest.stat().st_mode) != 0o600
    ):
        raise ValueError("regime panel manifest must be a real 0600 file")
    payload = json.loads(manifest.read_text(encoding="utf-8"))
    body = _mapping(payload, "regime panel")
    _validate_json_schema(body, "regime-panel.schema.json")
    _require_exact_keys(
        body,
        {
            "schema_version",
            "dataset_id",
            "dataset_hash",
            "provider_id",
            "provider_version",
            "historical_vintage",
            "retrieved_at",
            "industry_taxonomy",
            "series",
            "proxy_resolution",
            "panel_hash",
            "panel_id",
        },
        "regime panel",
    )
    if body.get("schema_version") != REGIME_PANEL_SCHEMA:
        raise ValueError("unsupported regime panel schema_version")
    panel_hash = _required_string(body, "panel_hash")
    panel_id = _required_string(body, "panel_id")
    core = {key: value for key, value in body.items() if key not in {"panel_hash", "panel_id"}}
    if canonical_hash(core) != panel_hash or panel_id != f"regime-panel-{panel_hash}":
        raise ValueError("regime panel identity does not match content")
    if path.name != panel_id:
        raise ValueError("regime panel directory name does not match identity")
    series_raw = body.get("series")
    if not isinstance(series_raw, list) or not series_raw:
        raise ValueError("regime panel series must be a non-empty array")
    series: list[RegimeSeries] = []
    series_ids: set[str] = set()
    for raw in cast(list[object], series_raw):
        item = _mapping(raw, "regime series")
        _require_exact_keys(
            item,
            {"series_id", "kind", "tushare_code", "source", "return_basis", "rows"},
            "regime series",
        )
        series_id = _required_string(item, "series_id")
        if series_id in series_ids:
            raise ValueError("regime panel series ids must be unique")
        series_ids.add(series_id)
        rows_raw = item.get("rows")
        if not isinstance(rows_raw, list):
            raise ValueError("regime series rows must be an array")
        rows: list[dict[str, object]] = []
        for raw_row in cast(list[object], rows_raw):
            row = _mapping(raw_row, "regime row")
            _require_exact_keys(row, {"trade_date", "open", "close"}, "regime row")
            rows.append(dict(row))
        normalized_rows = tuple(rows)
        _validate_rows(normalized_rows)
        series.append(
            RegimeSeries(
                series_id=series_id,
                kind=_required_string(item, "kind"),
                tushare_code=_required_string(item, "tushare_code"),
                source=_required_string(item, "source"),
                return_basis=_required_string(item, "return_basis"),
                rows=normalized_rows,
            )
        )
    proxy_raw = body.get("proxy_resolution")
    if not isinstance(proxy_raw, list):
        raise ValueError("proxy_resolution must be an array")
    proxy_resolution_items: list[tuple[str, str]] = []
    for raw_pair in cast(list[object], proxy_raw):
        if not isinstance(raw_pair, list):
            raise ValueError("proxy_resolution entries must contain two strings")
        pair = cast(list[object], raw_pair)
        if len(pair) != 2:
            raise ValueError("proxy_resolution entries must contain two strings")
        proxy_resolution_items.append(
            (
                _nonempty_string(pair[0], "proxy id"),
                _nonempty_string(pair[1], "proxy code"),
            )
        )
    proxy_resolution = tuple(proxy_resolution_items)
    taxonomy_raw = _mapping(body.get("industry_taxonomy"), "industry_taxonomy")
    _require_exact_keys(
        taxonomy_raw,
        {"source", "level", "fields", "rows", "retrieved_at", "content_hash"},
        "industry_taxonomy",
    )
    fields_raw = taxonomy_raw.get("fields")
    if not isinstance(fields_raw, list):
        raise ValueError("industry_taxonomy fields must be an array")
    taxonomy_rows_raw = taxonomy_raw.get("rows")
    if not isinstance(taxonomy_rows_raw, list):
        raise ValueError("industry_taxonomy rows must be an array")
    taxonomy_rows: list[tuple[object, ...]] = []
    for raw_row in cast(list[object], taxonomy_rows_raw):
        if not isinstance(raw_row, list):
            raise ValueError("industry_taxonomy rows must be arrays")
        taxonomy_rows.append(tuple(cast(list[object], raw_row)))
    taxonomy = RegimeTaxonomy(
        source=_required_string(taxonomy_raw, "source"),
        level=_required_string(taxonomy_raw, "level"),
        fields=tuple(
            _nonempty_string(field, "industry_taxonomy field")
            for field in cast(list[object], fields_raw)
        ),
        rows=tuple(taxonomy_rows),
        retrieved_at=_aware_datetime(_required_string(taxonomy_raw, "retrieved_at"), "taxonomy"),
        content_hash=_required_string(taxonomy_raw, "content_hash"),
    )
    panel = RegimePanel(
        dataset_id=_required_string(body, "dataset_id"),
        dataset_hash=_required_string(body, "dataset_hash"),
        provider_id=_required_string(body, "provider_id"),
        provider_version=_required_string(body, "provider_version"),
        historical_vintage=_required_string(body, "historical_vintage"),
        retrieved_at=_aware_datetime(_required_string(body, "retrieved_at"), "retrieved_at"),
        industry_taxonomy=taxonomy,
        series=tuple(series),
        proxy_resolution=proxy_resolution,
    )
    _validate_regime_panel_content(panel)
    return ValidatedRegimePanel(path=path, panel_id=panel_id, panel_hash=panel_hash, panel=panel)


def evaluate_regime_dataset(
    dataset: MarketRegimeDataset,
    validated_panel: ValidatedRegimePanel | RegimePanel,
) -> dict[str, object]:
    if isinstance(validated_panel, ValidatedRegimePanel):
        panel = validated_panel.panel
        panel_id = validated_panel.panel_id
        panel_hash = validated_panel.panel_hash
    else:
        panel = validated_panel
        _validate_regime_panel_content(panel)
        panel_hash = canonical_hash(_panel_core(panel))
        panel_id = f"regime-panel-{panel_hash}"
    _validate_dataset_panel_binding(dataset, panel)
    by_id = {item.series_id: item for item in panel.series}
    for proxy_id, tushare_code in panel.proxy_resolution:
        resolved = next(
            (item for item in panel.series if item.tushare_code == tushare_code),
            None,
        )
        if resolved is not None:
            by_id[proxy_id] = resolved
    cases: list[dict[str, object]] = []
    for case in dataset.cases:
        required = case.required_market_indices + case.required_industry_proxies
        results: dict[str, object] = {}
        missing = False
        for series_id in required:
            item = by_id.get(series_id)
            if item is None:
                results[series_id] = {"status": "missing_series"}
                missing = True
                continue
            computed = _evaluate_series(case, item)
            results[series_id] = computed
            missing = missing or computed["status"] != "covered"
        primary_result = cast(dict[str, object], results.get(case.primary_market_index, {}))
        for proxy in case.required_industry_proxies:
            proxy_result = cast(dict[str, object], results.get(proxy, {}))
            if proxy_result.get("status") != "covered" or primary_result.get("status") != "covered":
                continue
            for window in ("path", "event", "tradable"):
                proxy_return = proxy_result.get(f"{window}_return")
                primary_return = primary_result.get(f"{window}_return")
                proxy_result[f"{window}_excess_vs_primary"] = (
                    _format_decimal(
                        _decimal(proxy_return, f"{window}_return")
                        - _decimal(primary_return, f"primary_{window}_return")
                    )
                    if proxy_return is not None and primary_return is not None
                    else None
                )
        industry_path_returns = [
            (
                proxy,
                _decimal(
                    cast(dict[str, object], results[proxy]).get("path_return"),
                    "path_return",
                ),
            )
            for proxy in case.required_industry_proxies
            if cast(dict[str, object], results[proxy]).get("status") == "covered"
        ]
        opportunity = _opportunity_bounds(industry_path_returns)
        primary_series = by_id.get(case.primary_market_index)
        detected_state = (
            _detect_state(primary_series, case.tradable_start, dataset.detector)
            if primary_series is not None
            else "unclassified"
        )
        cases.append(
            {
                "case_key": case.case_key,
                "status": "incomplete" if missing else "covered",
                "identity_sensitive": True,
                "detected_state_at_tradable_start": detected_state,
                "registered_axes": case.axes,
                "capability_targets": list(case.capability_targets),
                "series": results,
                "sector_opportunity_bounds": opportunity,
            }
        )
    covered = sum(item["status"] == "covered" for item in cases)
    return {
        "schema_version": REGIME_REPORT_SCHEMA,
        "dataset_id": dataset.dataset_id,
        "dataset_hash": dataset.dataset_hash,
        "panel_id": panel_id,
        "panel_hash": panel_hash,
        "provider_id": panel.provider_id,
        "provider_version": panel.provider_version,
        "retrieved_at": panel.retrieved_at.isoformat(),
        "return_basis": _single_return_basis(panel.series),
        "historical_vintage": panel.historical_vintage,
        "research_only": True,
        "agent_visible": False,
        "opportunity_bounds_are_hindsight_only": True,
        "case_count": len(cases),
        "covered_case_count": covered,
        "incomplete_case_count": len(cases) - covered,
        "cases": cases,
    }


def _evaluate_series(case: MarketRegimeCase, series: RegimeSeries) -> dict[str, object]:
    rows = sorted(series.rows, key=lambda row: _required_string(row, "trade_date"))
    path_start = _row_on(rows, case.path_start)
    tradable_start = _row_on(rows, case.tradable_start)
    end = _row_on(rows, case.end)
    if path_start is None or tradable_start is None or end is None:
        return {"status": "missing_anchor"}
    path_return = _return(_price(path_start, "close"), _price(end, "close"))
    tradable_return = _return(_price(tradable_start, "open"), _price(end, "close"))
    event_return: str | None = None
    if case.event_anchor is not None:
        anchor_price = _event_anchor_price(rows, case.event_anchor)
        if anchor_price is None:
            return {"status": "missing_event_anchor"}
        event_return = _return(anchor_price, _price(end, "close"))
    return {
        "status": "covered",
        "path_return": path_return,
        "event_return": event_return,
        "tradable_return": tradable_return,
        "return_basis": series.return_basis,
    }


def _event_anchor_price(rows: list[dict[str, object]], anchor: EventAnchor) -> Decimal | None:
    target = _row_on(rows, anchor.anchor_session)
    if target is None:
        return None
    if anchor.price_anchor == "session_open":
        return _price(target, "open")
    if anchor.price_anchor == "session_close":
        return _price(target, "close")
    preceding = [row for row in rows if _row_date(row) < anchor.anchor_session]
    return _price(preceding[-1], "close") if preceding else None


def _detect_state(
    series: RegimeSeries,
    tradable_start: date,
    detector: dict[str, object],
) -> str:
    rows = sorted(
        (row for row in series.rows if _row_date(row) < tradable_start),
        key=_row_date,
    )
    short = _positive_int(detector.get("direction_short_sessions"), "short sessions")
    long = _positive_int(detector.get("direction_long_sessions"), "long sessions")
    volatility = _positive_int(detector.get("volatility_sessions"), "volatility sessions")
    if len(rows) <= max(short, long, volatility):
        return "unclassified"
    closes = [float(_price(row, "close")) for row in rows]
    short_return = math.log(closes[-1] / closes[-1 - short])
    long_return = math.log(closes[-1] / closes[-1 - long])
    if short_return == 0 or long_return == 0 or short_return * long_return < 0:
        return "unclassified"
    log_returns = [math.log(closes[index] / closes[index - 1]) for index in range(1, len(closes))]
    sample = log_returns[-volatility:]
    mean = sum(sample) / len(sample)
    variance = sum((item - mean) ** 2 for item in sample) / max(1, len(sample) - 1)
    sigma = math.sqrt(variance)
    if sigma == 0:
        return "unclassified"
    z_score = short_return / (math.sqrt(short) * sigma)
    threshold = float(_decimal(detector.get("fast_abs_z_threshold"), "threshold"))
    direction = "up" if short_return > 0 else "down"
    speed = "fast" if abs(z_score) >= threshold else "mild"
    return f"{direction}_{speed}"


def _opportunity_bounds(values: list[tuple[str, Decimal]]) -> dict[str, object]:
    if not values:
        return {
            "status": "unavailable",
            "top_return": None,
            "top_proxy_id": None,
            "bottom_return": None,
            "bottom_proxy_id": None,
            "dispersion": None,
            "positive_fraction": None,
            "hindsight_only": True,
        }
    ordered = sorted(values, key=lambda item: item[1])
    positive_fraction = Decimal(sum(value > 0 for _, value in values)) / Decimal(len(values))
    return {
        "status": "available",
        "top_return": _format_decimal(ordered[-1][1]),
        "top_proxy_id": ordered[-1][0],
        "bottom_return": _format_decimal(ordered[0][1]),
        "bottom_proxy_id": ordered[0][0],
        "dispersion": _format_decimal(ordered[-1][1] - ordered[0][1]),
        "positive_fraction": _format_decimal(positive_fraction),
        "hindsight_only": True,
    }


def _private_regime_root() -> Path:
    root = Path.cwd().resolve() / _REGIME_STORAGE_RELATIVE
    _create_private_directory(root.parent, require_mode=False)
    _create_private_directory(root)
    return root


def _validate_json_schema(payload: dict[str, object], schema_name: str) -> None:
    package_root = Path(__file__).resolve().parent
    installed_schema = package_root / "schemas" / schema_name
    schema_path = (
        installed_schema
        if installed_schema.is_file()
        else package_root.parents[1] / "schemas" / schema_name
    )
    schema = json.loads(schema_path.read_text(encoding="utf-8"))
    validator = cast(
        _JsonSchemaValidator,
        Draft202012Validator(schema, format_checker=FormatChecker()),
    )
    errors = sorted(validator.iter_errors(payload), key=lambda item: item.json_path)
    first_error = next(iter(errors), None)
    if first_error is not None:
        raise ValueError(
            f"{schema_name} validation failed at {first_error.json_path}: {first_error.message}"
        )


def _create_private_directory(path: Path, *, require_mode: bool = True) -> None:
    if path.is_symlink():
        raise ValueError("private regime output directory must not be a symlink")
    if path.exists():
        if not path.is_dir():
            raise ValueError("private regime output path must be a directory")
        if require_mode and stat.S_IMODE(path.stat().st_mode) != 0o700:
            raise ValueError("private regime output directory must already use mode 0700")
        return
    path.mkdir(mode=0o700)
    os.chmod(path, 0o700)


def _taxonomy_from_table(table: TushareTable) -> RegimeTaxonomy:
    if table.api_name != "index_classify":
        raise ValueError("industry taxonomy must come from index_classify")
    expected_params = (("level", _SW2021_LEVEL), ("src", _SW2021_SOURCE))
    if table.params != expected_params:
        raise ValueError("industry taxonomy must be the SW2021 L1 classification query")
    taxonomy = RegimeTaxonomy(
        source=_SW2021_SOURCE,
        level=_SW2021_LEVEL,
        fields=table.fields,
        rows=table.rows,
        retrieved_at=table.retrieved_at,
        content_hash=table.content_hash,
    )
    _validate_taxonomy(taxonomy)
    return taxonomy


def _verify_catalog_against_taxonomy(
    catalog: tuple[dict[str, str], ...],
    taxonomy: RegimeTaxonomy,
) -> None:
    fields = {field: index for index, field in enumerate(taxonomy.fields)}
    names_by_code: dict[str, str] = {}
    for row in taxonomy.rows:
        code = _nonempty_string(row[fields["index_code"]], "taxonomy index_code")
        name = _nonempty_string(row[fields["industry_name"]], "taxonomy industry_name")
        if code in names_by_code:
            raise ValueError("industry taxonomy index codes must be unique")
        names_by_code[code] = name
    for proxy in catalog:
        code = proxy["tushare_code"]
        if names_by_code.get(code) != proxy["industry_name"]:
            raise ValueError(
                "industry proxy catalog does not match the retrieved SW2021 L1 taxonomy"
            )


def _validate_taxonomy(taxonomy: RegimeTaxonomy) -> None:
    if taxonomy.source != _SW2021_SOURCE or taxonomy.level != _SW2021_LEVEL:
        raise ValueError("industry taxonomy must be SW2021 L1")
    if taxonomy.retrieved_at.tzinfo is None:
        raise ValueError("industry taxonomy retrieval time must be timezone-aware")
    if _CONTENT_HASH.fullmatch(taxonomy.content_hash) is None:
        raise ValueError("industry taxonomy content_hash must be a SHA-256 hash")
    expected_hash = tushare_table_content_hash(
        api_name="index_classify",
        params={"level": _SW2021_LEVEL, "src": _SW2021_SOURCE},
        fields=taxonomy.fields,
        rows=taxonomy.rows,
    )
    if expected_hash != taxonomy.content_hash:
        raise ValueError("industry taxonomy content_hash does not match content")
    fields = {field: index for index, field in enumerate(taxonomy.fields)}
    required = {"index_code", "industry_name", "level", "src"}
    if not required <= set(fields):
        raise ValueError("industry taxonomy is missing required classification fields")
    codes: set[str] = set()
    for row in taxonomy.rows:
        if len(row) != len(taxonomy.fields):
            raise ValueError("industry taxonomy rows must match fields")
        code = _nonempty_string(row[fields["index_code"]], "taxonomy index_code")
        if _INDUSTRY_INDEX_CODE.fullmatch(code) is None:
            raise ValueError("industry taxonomy index_code must use six-digit .SI format")
        if code in codes:
            raise ValueError("industry taxonomy index codes must be unique")
        codes.add(code)
        _nonempty_string(row[fields["industry_name"]], "taxonomy industry_name")
        if _nonempty_string(row[fields["level"]], "taxonomy level") != _SW2021_LEVEL:
            raise ValueError("industry taxonomy level conflicts with query")
        if _nonempty_string(row[fields["src"]], "taxonomy source") != _SW2021_SOURCE:
            raise ValueError("industry taxonomy source conflicts with query")


def _validate_regime_panel_content(panel: RegimePanel) -> None:
    if panel.provider_id != TUSHARE_PROVIDER_ID:
        raise ValueError("regime panel provider_id must be tushare-http")
    if panel.provider_version != TUSHARE_ADAPTER_VERSION:
        raise ValueError("regime panel provider_version does not match the Tushare adapter")
    if panel.historical_vintage != _HISTORICAL_VINTAGE:
        raise ValueError("regime panel historical_vintage is unsupported")
    if panel.retrieved_at.tzinfo is None:
        raise ValueError("regime panel retrieved_at must be timezone-aware")
    _validate_taxonomy(panel.industry_taxonomy)
    if not panel.series:
        raise ValueError("regime panel must contain series")
    series_ids: set[str] = set()
    tushare_codes: set[str] = set()
    industry_codes: set[str] = set()
    for series in panel.series:
        if not series.series_id or series.series_id in series_ids:
            raise ValueError("regime panel series ids must be non-empty and unique")
        series_ids.add(series.series_id)
        if series.tushare_code in tushare_codes:
            raise ValueError("regime panel Tushare codes must be unique")
        tushare_codes.add(series.tushare_code)
        if series.return_basis != "price":
            raise ValueError("regime panel return_basis must be price")
        if series.kind == "market":
            if series.source != "index_daily" or series.series_id != series.tushare_code:
                raise ValueError("market series must be index_daily and identified by its code")
            if _MARKET_INDEX_CODE.fullmatch(series.tushare_code) is None:
                raise ValueError("market series must use six-digit .SH or .SZ Tushare codes")
        elif series.kind == "industry":
            if series.source != "sw_daily":
                raise ValueError("industry series must come from sw_daily")
            if _INDUSTRY_INDEX_CODE.fullmatch(series.tushare_code) is None:
                raise ValueError("industry series must use six-digit .SI Tushare codes")
            industry_codes.add(series.tushare_code)
        else:
            raise ValueError("regime panel series kind must be market or industry")
        _validate_rows(series.rows)
    proxy_ids: set[str] = set()
    proxy_codes: set[str] = set()
    for proxy_id, tushare_code in panel.proxy_resolution:
        if not proxy_id or proxy_id in proxy_ids:
            raise ValueError("proxy_resolution proxy ids must be non-empty and unique")
        if tushare_code in proxy_codes:
            raise ValueError("proxy_resolution proxy codes must be unique")
        proxy_ids.add(proxy_id)
        proxy_codes.add(tushare_code)
    if proxy_codes != industry_codes:
        raise ValueError("proxy_resolution must resolve every industry series exactly once")


def _validate_dataset_panel_binding(dataset: MarketRegimeDataset, panel: RegimePanel) -> None:
    if panel.dataset_id != dataset.dataset_id or panel.dataset_hash != dataset.dataset_hash:
        raise ValueError("regime panel is not bound to the supplied dataset")
    _validate_regime_panel_content(panel)
    market_series = tuple(item for item in panel.series if item.kind == "market")
    if {item.series_id for item in market_series} != set(dataset.main_market_indices):
        raise ValueError("regime panel market series do not exactly match the dataset")
    expected_resolution = {
        item["proxy_id"]: item["tushare_code"] for item in dataset.industry_proxy_catalog
    }
    actual_resolution = dict(panel.proxy_resolution)
    if actual_resolution != expected_resolution:
        raise ValueError("regime panel industry proxy resolution does not match the dataset")
    _verify_catalog_against_taxonomy(dataset.industry_proxy_catalog, panel.industry_taxonomy)


def _single_return_basis(series: tuple[RegimeSeries, ...]) -> str:
    values = {item.return_basis for item in series}
    if len(values) != 1:
        raise ValueError("regime panel series must share one return_basis")
    return next(iter(values))


def _panel_core(panel: RegimePanel) -> dict[str, object]:
    return {
        "schema_version": REGIME_PANEL_SCHEMA,
        "dataset_id": panel.dataset_id,
        "dataset_hash": panel.dataset_hash,
        "provider_id": panel.provider_id,
        "provider_version": panel.provider_version,
        "historical_vintage": panel.historical_vintage,
        "retrieved_at": panel.retrieved_at.isoformat(),
        "industry_taxonomy": {
            "source": panel.industry_taxonomy.source,
            "level": panel.industry_taxonomy.level,
            "fields": list(panel.industry_taxonomy.fields),
            "rows": [list(row) for row in panel.industry_taxonomy.rows],
            "retrieved_at": panel.industry_taxonomy.retrieved_at.isoformat(),
            "content_hash": panel.industry_taxonomy.content_hash,
        },
        "series": [
            {
                "series_id": item.series_id,
                "kind": item.kind,
                "tushare_code": item.tushare_code,
                "source": item.source,
                "return_basis": item.return_basis,
                "rows": list(item.rows),
            }
            for item in panel.series
        ],
        "proxy_resolution": [list(pair) for pair in panel.proxy_resolution],
    }


def _series_from_table(
    series_id: str,
    kind: str,
    tushare_code: str,
    source: str,
    table: TushareTable,
) -> RegimeSeries:
    indexes = {field: table.fields.index(field) for field in ("trade_date", "open", "close")}
    rows: tuple[dict[str, object], ...] = tuple(
        {
            "trade_date": str(row[indexes["trade_date"]]),
            "open": str(row[indexes["open"]]),
            "close": str(row[indexes["close"]]),
        }
        for row in table.rows
    )
    _validate_rows(rows)
    return RegimeSeries(
        series_id=series_id,
        kind=kind,
        tushare_code=tushare_code,
        source=source,
        return_basis="price",
        rows=rows,
    )


def _validate_rows(rows: tuple[dict[str, object], ...]) -> None:
    dates: set[date] = set()
    for row in rows:
        day = _row_date(row)
        if day in dates:
            raise ValueError("regime series trade dates must be unique")
        dates.add(day)
        _price(row, "open")
        _price(row, "close")


def _row_on(rows: list[dict[str, object]], day: date) -> dict[str, object] | None:
    return next((row for row in rows if _row_date(row) == day), None)


def _row_date(row: dict[str, object]) -> date:
    text = _required_string(row, "trade_date")
    if len(text) == 8 and text.isdigit():
        return datetime.strptime(text, "%Y%m%d").date()
    return date.fromisoformat(text)


def _price(row: dict[str, object], field: str) -> Decimal:
    value = _decimal(row.get(field), field)
    if value <= 0:
        raise ValueError(f"{field} must be positive")
    return value


def _return(start: Decimal, end: Decimal) -> str:
    return _format_decimal((end / start) - Decimal(1))


def _format_decimal(value: Decimal) -> str:
    return format(value.quantize(_RETURN_QUANTUM, rounding=ROUND_HALF_EVEN), "f")


def _event_anchor(value: object) -> EventAnchor | None:
    if value is None:
        return None
    item = _mapping(value, "event_anchor")
    price_anchor = _required_string(item, "price_anchor")
    if price_anchor not in _EVENT_PRICE_ANCHORS:
        raise ValueError("unsupported event price_anchor")
    observed_at = datetime.fromisoformat(
        _required_string(item, "observed_at").replace("Z", "+00:00")
    )
    if observed_at.tzinfo is None:
        raise ValueError("event observed_at must be timezone-aware")
    executable = item.get("executable")
    if not isinstance(executable, bool):
        raise ValueError("event executable must be boolean")
    return EventAnchor(
        observed_at=observed_at,
        anchor_session=date.fromisoformat(_required_string(item, "anchor_session")),
        price_anchor=price_anchor,
        executable=executable,
    )


def _required_mapping(value: dict[str, object], key: str) -> dict[str, object]:
    return _mapping(value.get(key), key)


def _mapping(value: object, name: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise ValueError(f"{name} must be an object")
    return cast(dict[str, object], value)


def _require_exact_keys(value: dict[str, object], expected: set[str], name: str) -> None:
    unknown = set(value) - expected
    missing = expected - set(value)
    if unknown or missing:
        raise ValueError(f"{name} must contain exactly its required fields")


def _required_string(value: dict[str, object], key: str) -> str:
    return _nonempty_string(value.get(key), key)


def _nonempty_string(value: object, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a non-empty string")
    return value


def _aware_datetime(value: str, name: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError(f"{name} must be timezone-aware")
    return parsed


def _string_tuple(value: object, name: str) -> tuple[str, ...]:
    if not isinstance(value, list):
        raise ValueError(f"{name} must be an array")
    result = tuple(_nonempty_string(item, name) for item in cast(list[object], value))
    if len(result) != len(set(result)):
        raise ValueError(f"{name} must contain unique values")
    return result


def _decimal(value: object, name: str) -> Decimal:
    if isinstance(value, bool) or value is None:
        raise ValueError(f"{name} must be numeric")
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"{name} must be numeric") from exc


def _positive_int(value: object, name: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise ValueError(f"{name} must be a positive integer")
    return value
