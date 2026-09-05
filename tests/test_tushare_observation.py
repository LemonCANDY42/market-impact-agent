from __future__ import annotations

import asyncio
import json
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import cast

import pytest

import market_impact_agent.tushare_observation as tushare_observation
from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.data_inputs import (
    DataFetchStatus,
    DataInputHarness,
    DataPITLane,
    DataQuery,
    DataQueryMode,
    DataSourceBinding,
    LocalDataSnapshotStore,
)
from market_impact_agent.observations import AvailabilityBasis, ObservationCapability
from market_impact_agent.tushare_observation import (
    TushareObservationProvider,
    TushareObservationSourceConfig,
    load_tushare_observation_capture_bundle,
    load_tushare_observation_source,
    summarize_tushare_observation_capture_usage,
)

RETRIEVED = datetime(2026, 8, 28, 8, 0, tzinfo=UTC)
TOKEN = "tushare-test-token-must-never-appear"
DOCUMENTATION_IDS = {
    "fund_adj": 199,
    "fund_div": 120,
    "dividend": 103,
    "news": 143,
    "major_news": 195,
    "index_daily": 95,
    "daily_basic": 32,
    "index_dailybasic": 128,
    "fund_daily": 127,
    "trade_cal": 26,
    "etf_basic": 385,
    "stock_basic": 25,
    "stk_limit": 183,
    "index_classify": 181,
    "index_member_all": 335,
    "etf_sh_cons": 471,
    "etf_sz_cons": 472,
    "margin": 58,
    "cn_schedule": 461,
    "report_rc": 292,
}
DOCUMENTATION_URLS = {
    **{
        api_name: f"https://tushare.pro/document/2?doc_id={doc_id}"
        for api_name, doc_id in DOCUMENTATION_IDS.items()
    },
    "adj_factor": "https://tushare.pro/document/2?doc_id=28",
    "daily": "https://tushare.pro/document/1?doc_id=27",
    "express_vip": "https://tushare.pro/document/2?doc_id=46",
    "forecast_vip": "https://tushare.pro/document/2?doc_id=45",
    "suspend_d": "https://tushare.pro/document/2?doc_id=214",
}


class FakeTransport:
    def __init__(self, responses: list[dict[str, object]]) -> None:
        self.responses = responses
        self.requests: list[dict[str, object]] = []

    def __call__(self, endpoint: str, body: bytes, timeout_seconds: float) -> bytes:
        assert endpoint == "https://api.tushare.pro"
        assert timeout_seconds > 0
        self.requests.append(json.loads(body))
        return json.dumps(self.responses.pop(0), ensure_ascii=False, separators=(",", ":")).encode()


class _OversizedResponse:
    def __enter__(self) -> _OversizedResponse:
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def read(self, amount: int) -> bytes:
        assert amount == 9
        return b"x" * amount


def _oversized_urlopen(_request: object, timeout: float) -> _OversizedResponse:
    assert timeout == 5.0
    return _OversizedResponse()


def test_default_transport_rejects_oversized_response_before_parsing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(tushare_observation, "_MAX_RESPONSE_BYTES", 8)
    monkeypatch.setattr(tushare_observation, "urlopen", _oversized_urlopen)
    config = _configs()[1]
    parameters = {
        "ts_code": "000300.SH",
        "start_date": "20260828",
        "end_date": "20260828",
    }
    provider = TushareObservationProvider(
        TOKEN,
        (config,),
        timeout_seconds=5.0,
        clock=lambda: RETRIEVED,
    )

    response = asyncio.run(
        provider.fetch(
            query=_query(provider, config, parameters),
            source=_source(provider, config),
        )
    )

    assert response.status is DataFetchStatus.ERROR
    assert response.error_kind == "response_too_large"


def test_collection_rejects_total_capture_bytes_before_accumulating_pages(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _configs()[1]
    response = json.dumps(
        _response(config.fields, [_values(config.api_name, config.fields)]),
        separators=(",", ":"),
    ).encode()
    monkeypatch.setattr(tushare_observation, "_MAX_CAPTURE_BYTES", len(response) - 1)
    provider = TushareObservationProvider(
        TOKEN,
        (config,),
        transport=FakeTransport([json.loads(response)]),
        clock=lambda: RETRIEVED,
    )

    capture = asyncio.run(
        provider.collect(
            source_id=config.source_id,
            parameters={
                "ts_code": "000300.SH",
                "start_date": "20260828",
                "end_date": "20260828",
            },
        )
    )

    assert capture.coverage_complete is False
    assert capture.error_kind == "capture_size_exceeded"


def test_capture_usage_counts_pages_and_network_bytes_without_decoding_content() -> None:
    config = _configs()[0]
    parameters = {
        "start_date": "2026-08-28 07:00:00",
        "end_date": "2026-08-28 08:00:00",
    }
    response = _response(config.fields, [_values(config.api_name, config.fields)])
    encoded_response = json.dumps(
        response,
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode()
    provider = TushareObservationProvider(
        TOKEN,
        (config,),
        transport=FakeTransport([response]),
        clock=lambda: RETRIEVED,
    )
    source = _source(provider, config)
    query = _query(provider, config, parameters)
    capture = asyncio.run(provider.collect(source_id=config.source_id, parameters=parameters))
    provider_response = provider.response_from_capture(
        query=query,
        source=source,
        capture=capture,
    )
    assert provider_response.raw_payload is not None

    usage = summarize_tushare_observation_capture_usage(provider_response.raw_payload)

    assert usage.request_count == 1
    assert usage.response_bytes == len(encoded_response)
    assert usage.capture_bytes == len(provider_response.raw_payload)


def _route_specs() -> tuple[
    tuple[
        str,
        ObservationCapability,
        tuple[str, ...],
        tuple[str, ...],
        tuple[str, ...],
        tuple[str, ...],
        Mapping[str, object],
        Mapping[str, object],
    ],
    ...,
]:
    return (
        (
            "news",
            ObservationCapability.EVENT_REVELATION,
            ("datetime", "content", "title", "channels"),
            ("datetime", "title", "content"),
            (),
            ("datetime",),
            {"src": "sina"},
            {"start_date": "2026-08-28 00:00:00", "end_date": "2026-08-28 08:00:00"},
        ),
        (
            "index_daily",
            ObservationCapability.MARKET_CONTEXT,
            (
                "ts_code",
                "trade_date",
                "open",
                "high",
                "low",
                "close",
                "pre_close",
                "change",
                "pct_chg",
                "vol",
                "amount",
            ),
            ("ts_code", "trade_date"),
            ("trade_date",),
            (),
            {},
            {"ts_code": "000300.SH", "start_date": "20260828", "end_date": "20260828"},
        ),
        (
            "daily_basic",
            ObservationCapability.MARKET_CONTEXT,
            (
                "ts_code",
                "trade_date",
                "close",
                "turnover_rate",
                "turnover_rate_f",
                "volume_ratio",
                "pe",
                "pe_ttm",
                "pb",
                "ps",
                "ps_ttm",
                "dv_ratio",
                "dv_ttm",
                "total_share",
                "float_share",
                "free_share",
                "total_mv",
                "circ_mv",
                "limit_status",
            ),
            ("ts_code", "trade_date"),
            ("trade_date",),
            (),
            {},
            {"trade_date": "20260828"},
        ),
        (
            "index_dailybasic",
            ObservationCapability.MARKET_CONTEXT,
            (
                "ts_code",
                "trade_date",
                "total_mv",
                "float_mv",
                "total_share",
                "float_share",
                "free_share",
                "turnover_rate",
                "turnover_rate_f",
                "pe",
                "pe_ttm",
                "pb",
            ),
            ("ts_code", "trade_date"),
            ("trade_date",),
            (),
            {},
            {"ts_code": "000300.SH", "trade_date": "20260828"},
        ),
        (
            "etf_basic",
            ObservationCapability.EXPOSURE_CANDIDATES,
            (
                "ts_code",
                "csname",
                "extname",
                "cname",
                "index_code",
                "index_name",
                "setup_date",
                "list_date",
                "list_status",
                "exchange",
                "mgr_name",
                "custod_name",
                "mgt_fee",
                "etf_type",
            ),
            ("ts_code",),
            ("setup_date", "list_date"),
            (),
            {},
            {"list_status": "L"},
        ),
        (
            "index_member_all",
            ObservationCapability.EXPOSURE_CANDIDATES,
            (
                "l1_code",
                "l1_name",
                "l2_code",
                "l2_name",
                "l3_code",
                "l3_name",
                "ts_code",
                "name",
                "in_date",
                "out_date",
                "is_new",
            ),
            ("l3_code", "ts_code", "in_date"),
            ("in_date", "out_date"),
            (),
            {},
            {"l3_code": "850531.SI"},
        ),
        (
            "etf_sh_cons",
            ObservationCapability.EXPOSURE_CANDIDATES,
            (
                "trade_date",
                "ts_code",
                "con_code",
                "con_name",
                "qty",
                "sub_flag",
                "cpr",
                "rdr",
                "sca",
                "exchange",
            ),
            ("trade_date", "ts_code", "con_code"),
            ("trade_date",),
            (),
            {},
            {"ts_code": "517030.SH", "trade_date": "20260828"},
        ),
        (
            "etf_sz_cons",
            ObservationCapability.EXPOSURE_CANDIDATES,
            (
                "trade_date",
                "ts_code",
                "con_code",
                "con_name",
                "qty",
                "sub_flag",
                "cpr",
                "rdr",
                "sub_cc",
                "red_cc",
                "exchange",
            ),
            ("trade_date", "ts_code", "con_code"),
            ("trade_date",),
            (),
            {},
            {"ts_code": "159051.SZ", "trade_date": "20260828"},
        ),
        (
            "margin",
            ObservationCapability.POSITIONING,
            ("trade_date", "exchange_id", "rzye", "rzmre", "rzche", "rqye", "rqmcl", "rzrqye"),
            ("trade_date", "exchange_id"),
            ("trade_date",),
            (),
            {},
            {"start_date": "20260828", "end_date": "20260828"},
        ),
        (
            "cn_schedule",
            ObservationCapability.MACRO_VINTAGE,
            ("month", "publish_date", "title", "issuing_org", "data_api"),
            ("month", "publish_date", "title", "issuing_org"),
            ("month", "publish_date"),
            (),
            {},
            {"m": "202609"},
        ),
        (
            "report_rc",
            ObservationCapability.PRIOR_EXPECTATION,
            (
                "ts_code",
                "name",
                "report_date",
                "report_title",
                "report_type",
                "classify",
                "org_name",
                "author_name",
                "quarter",
                "op_rt",
                "op_pr",
                "tp",
                "np",
                "eps",
                "pe",
                "rd",
                "roe",
                "ev_ebitda",
                "rating",
                "max_price",
                "min_price",
                "imp_dg",
                "create_time",
            ),
            ("ts_code", "report_date", "org_name", "author_name", "quarter"),
            ("report_date",),
            ("create_time",),
            {},
            {"start_date": "20260828", "end_date": "20260828"},
        ),
        (
            "trade_cal",
            ObservationCapability.MARKET_CONTEXT,
            ("exchange", "cal_date", "is_open", "pretrade_date"),
            ("exchange", "cal_date"),
            ("cal_date", "pretrade_date"),
            (),
            {},
            {"exchange": "SSE", "start_date": "20260828", "end_date": "20260828"},
        ),
        (
            "fund_daily",
            ObservationCapability.MARKET_CONTEXT,
            (
                "ts_code",
                "trade_date",
                "pre_close",
                "open",
                "high",
                "low",
                "close",
                "change",
                "pct_chg",
                "vol",
                "amount",
            ),
            ("ts_code", "trade_date"),
            ("trade_date",),
            (),
            {},
            {"ts_code": "510300.SH", "start_date": "20260828", "end_date": "20260828"},
        ),
        (
            "stock_basic",
            ObservationCapability.EXPOSURE_CANDIDATES,
            ("ts_code", "symbol", "name", "exchange", "list_status", "list_date", "delist_date"),
            ("ts_code",),
            ("list_date", "delist_date"),
            (),
            {},
            {"exchange": "SSE", "list_status": "L"},
        ),
        (
            "index_classify",
            ObservationCapability.EXPOSURE_CANDIDATES,
            (
                "index_code",
                "industry_name",
                "parent_code",
                "level",
                "industry_code",
                "is_pub",
                "src",
            ),
            ("index_code", "industry_code", "src"),
            (),
            (),
            {},
            {"src": "SW2021"},
        ),
        (
            "stk_limit",
            ObservationCapability.EXPOSURE_CANDIDATES,
            ("ts_code", "trade_date", "pre_close", "up_limit", "down_limit"),
            ("ts_code", "trade_date"),
            ("trade_date",),
            (),
            {},
            {"trade_date": "20260828"},
        ),
    )


def _values(api_name: str, fields: tuple[str, ...]) -> list[object]:
    values: dict[str, object] = {
        "datetime": "2026-08-28 07:30:00",
        "content": "Synthetic private test content",
        "title": "Synthetic test headline",
        "channels": "财经",
        "ts_code": "000300.SH",
        "trade_date": "20260828",
        "open": 10.0,
        "high": 10.2,
        "low": 9.9,
        "close": 10.1,
        "pre_close": 10.0,
        "change": 0.1,
        "pct_chg": 1.0,
        "vol": 1000.0,
        "amount": 10000.0,
        "turnover_rate": 1.1,
        "turnover_rate_f": 1.2,
        "volume_ratio": 1.3,
        "pe_ttm": 10.5,
        "pb": 1.5,
        "ps": 2.0,
        "ps_ttm": 2.1,
        "dv_ratio": 1.8,
        "dv_ttm": 1.9,
        "total_share": 100000.0,
        "float_share": 80000.0,
        "free_share": 70000.0,
        "total_mv": 1010000.0,
        "circ_mv": 808000.0,
        "float_mv": 808000.0,
        "limit_status": "0",
        "csname": "Synthetic ETF",
        "extname": "Synthetic ETF Extension",
        "cname": "Synthetic Exchange Traded Fund",
        "index_name": "Synthetic Index",
        "setup_date": "20200101",
        "list_date": "20200102",
        "delist_date": "",
        "list_status": "L",
        "exchange": "SSE",
        "mgr_name": "Synthetic Manager",
        "custod_name": "Synthetic Custodian",
        "mgt_fee": 0.5,
        "etf_type": "境内",
        "l1_code": "801000.SI",
        "l1_name": "Synthetic L1",
        "l2_code": "801010.SI",
        "l2_name": "Synthetic L2",
        "l3_code": "850531.SI",
        "l3_name": "Synthetic L3",
        "name": "Synthetic Name",
        "in_date": "20200101",
        "out_date": "",
        "is_new": "Y",
        "con_code": "600000.SH",
        "con_name": "Synthetic constituent",
        "qty": 1000.0,
        "sub_flag": "1",
        "cpr": 0.1,
        "rdr": 0.2,
        "sca": 100.0,
        "sub_cc": 90.0,
        "red_cc": 110.0,
        "exchange_id": "SSE",
        "rzye": 1.0,
        "rzmre": 1.0,
        "rzche": 1.0,
        "rqye": 1.0,
        "rqmcl": 1.0,
        "rzrqye": 2.0,
        "month": "202609",
        "publish_date": "20260910",
        "issuing_org": "Synthetic issuing organization",
        "data_api": "synthetic_api",
        "report_date": "20260828",
        "report_title": "Synthetic report",
        "report_type": "research",
        "classify": "general",
        "org_name": "Synthetic broker",
        "author_name": "Synthetic analyst",
        "quarter": "2026Q4",
        "op_rt": 1.0,
        "op_pr": 1.0,
        "tp": 1.0,
        "np": 1.0,
        "eps": 1.0,
        "pe": 1.0,
        "rd": 1.0,
        "roe": 1.0,
        "ev_ebitda": 1.0,
        "rating": "buy",
        "max_price": 12.0,
        "min_price": 9.0,
        "imp_dg": "high",
        "create_time": "2026-08-28 07:00:00",
        "cal_date": "20260828",
        "is_open": "1",
        "pretrade_date": "20260827",
        "symbol": "000300",
        "index_code": "801000.SI",
        "industry_name": "Synthetic Industry",
        "parent_code": "",
        "level": "L1",
        "industry_code": "710000",
        "is_pub": "1",
        "src": "SW2021",
        "up_limit": 11.0,
        "down_limit": 9.0,
    }
    if api_name == "etf_basic":
        values["ts_code"] = "510300.SH"
    if api_name == "index_member_all":
        values["ts_code"] = "000001.SZ"
    if api_name == "etf_sh_cons":
        values["ts_code"] = "517030.SH"
        values["exchange"] = "SH"
    if api_name == "etf_sz_cons":
        values["ts_code"] = "159051.SZ"
        values["exchange"] = "SZ"
    if api_name == "fund_daily":
        values["ts_code"] = "510300.SH"
    if api_name == "stock_basic":
        values["ts_code"] = "600000.SH"
        values["symbol"] = "600000"
    if api_name == "index_classify":
        values["index_code"] = "801000.SI"
    return [values[field] for field in fields]


def _config(
    api_name: str,
    capability: ObservationCapability,
    fields: tuple[str, ...],
    primary_key_fields: tuple[str, ...],
    date_fields: tuple[str, ...],
    datetime_fields: tuple[str, ...],
    fixed_parameters: Mapping[str, object],
) -> TushareObservationSourceConfig:
    allowed_parameters = {
        "news": ("start_date", "end_date"),
        "index_daily": ("ts_code", "trade_date", "start_date", "end_date"),
        "daily_basic": ("ts_code", "trade_date", "start_date", "end_date"),
        "index_dailybasic": ("ts_code", "trade_date", "start_date", "end_date"),
        "etf_basic": ("ts_code", "index_code", "list_date", "list_status", "exchange", "mgr"),
        "index_member_all": ("l1_code", "l2_code", "l3_code", "ts_code", "is_new"),
        "etf_sh_cons": ("ts_code", "trade_date", "con_code", "start_date", "end_date"),
        "etf_sz_cons": ("ts_code", "trade_date", "con_code", "start_date", "end_date"),
        "margin": ("trade_date", "start_date", "end_date", "exchange_id"),
        "cn_schedule": ("m", "title"),
        "report_rc": ("ts_code", "report_date", "start_date", "end_date"),
        "trade_cal": ("exchange", "start_date", "end_date", "is_open"),
        "fund_daily": ("ts_code", "trade_date", "start_date", "end_date"),
        "stock_basic": ("ts_code", "name", "market", "list_status", "exchange", "is_hs"),
        "index_classify": ("index_code", "level", "src"),
        "stk_limit": ("ts_code", "trade_date", "start_date", "end_date"),
    }[api_name]
    return TushareObservationSourceConfig.build(
        source_id=f"tushare-{api_name.replace('_', '-')}",
        api_name=api_name,
        capability=capability,
        upstream_publisher=("Sina Finance" if api_name == "news" else "Tushare Pro"),
        documentation_url=(f"https://tushare.pro/document/2?doc_id={DOCUMENTATION_IDS[api_name]}"),
        rights_url="https://tushare.pro/document/1?doc_id=108",
        license_scope="private_research_no_redistribution",
        content_scope=f"tushare_{api_name}_private_research",
        semantic_scope=(
            "schedule_observation_only_not_original_release_or_revision"
            if api_name == "cn_schedule"
            else "aggregated_source_observation_actual_receipt_only"
        ),
        fields=fields,
        primary_key_fields=primary_key_fields,
        allowed_parameters=allowed_parameters,
        fixed_parameters=fixed_parameters,
        date_fields=date_fields,
        datetime_fields=datetime_fields,
        publisher_time_field="datetime" if api_name == "news" else None,
        aggregator_timestamp_field="create_time" if api_name == "report_rc" else None,
        pagination_page_size=2,
        pagination_max_pages=2,
    )


def _configs() -> tuple[TushareObservationSourceConfig, ...]:
    return tuple(_config(*spec[:7]) for spec in _route_specs())


def _source(
    provider: TushareObservationProvider, config: TushareObservationSourceConfig
) -> DataSourceBinding:
    return DataSourceBinding(
        provider_id=provider.manifest.provider_id,
        provider_version=provider.manifest.provider_version,
        upstream_source=config.source_id,
        manifest_hash=canonical_hash(provider.manifest.to_dict()),
        source_config_hash=config.artifact_hash,
        required=True,
    )


def _query(
    provider: TushareObservationProvider,
    config: TushareObservationSourceConfig,
    parameters: Mapping[str, object],
) -> DataQuery:
    return DataQuery.build(
        capability=config.capability,
        pit_lane=DataPITLane.PROSPECTIVE,
        as_of=RETRIEVED,
        window_start=None,
        source_policy_id=f"{config.source_id}-prospective-v1",
        parameters=parameters,
        sources=(_source(provider, config),),
        minimum_data_sources=1,
    )


def _response(fields: tuple[str, ...], items: list[list[object]]) -> dict[str, object]:
    return {"code": 0, "msg": None, "data": {"fields": list(fields), "items": items}}


@pytest.mark.parametrize("spec", _route_specs(), ids=lambda spec: spec[0])
def test_each_declared_route_collects_and_maps_its_capability(
    spec: tuple[
        str,
        ObservationCapability,
        tuple[str, ...],
        tuple[str, ...],
        tuple[str, ...],
        tuple[str, ...],
        Mapping[str, object],
        Mapping[str, object],
    ],
) -> None:
    api_name, capability, fields, _primary, _dates, _datetimes, _fixed, parameters = spec
    config = _config(*spec[:7])
    transport = FakeTransport([_response(fields, [_values(api_name, fields)])])
    provider = TushareObservationProvider(
        TOKEN,
        (config,),
        transport=transport,
        clock=lambda: RETRIEVED,
    )
    query = _query(provider, config, parameters)

    response = asyncio.run(provider.fetch(query=query, source=_source(provider, config)))

    assert response.status is DataFetchStatus.DATA
    assert response.observations[0].capability is capability
    assert response.observations[0].normalized_payload["aggregator"] == "Tushare Pro"
    request = transport.requests[0]
    request_parameters = cast(dict[str, object], request["params"])
    assert request["api_name"] == api_name
    assert request_parameters["offset"] == 0
    assert request_parameters["limit"] == 2


def test_checked_in_source_configs_are_canonical_schema_valid_and_secret_free() -> None:
    paths = sorted(Path("examples/providers").glob("tushare-observation-*.json"))

    configs = tuple(load_tushare_observation_source(path) for path in paths)
    assert len(configs) >= len(_route_specs())
    assert {spec[0] for spec in _route_specs()} <= {item.api_name for item in configs}
    assert all(item.documentation_url == DOCUMENTATION_URLS[item.api_name] for item in configs)
    assert len({item.source_id for item in configs}) == len(configs)
    assert all("token" not in path.read_text(encoding="utf-8").casefold() for path in paths)


def test_multi_capability_manifest_is_contract_validated_and_enabled() -> None:
    provider = TushareObservationProvider(TOKEN, _configs(), transport=FakeTransport([]))

    assert provider.manifest.enabled is True
    assert provider.manifest.trust_tier.value == "contract_validated"
    assert provider.manifest.declared_capabilities == frozenset(
        {
            ObservationCapability.EVENT_REVELATION,
            ObservationCapability.MARKET_CONTEXT,
            ObservationCapability.EXPOSURE_CANDIDATES,
            ObservationCapability.POSITIONING,
            ObservationCapability.MACRO_VINTAGE,
            ObservationCapability.PRIOR_EXPECTATION,
        }
    )
    assert provider.manifest.verified_capabilities == provider.manifest.declared_capabilities


def test_provider_paginated_capture_is_replayable_and_deterministic() -> None:
    config = _configs()[1]
    fields = config.fields
    first = _values(config.api_name, fields)
    second = [*first]
    second[0] = "000905.SH"
    transport = FakeTransport(
        [
            _response(fields, [first, second]),
            _response(fields, []),
        ]
    )
    provider = TushareObservationProvider(
        TOKEN,
        (config,),
        transport=transport,
        clock=lambda: RETRIEVED,
    )
    parameters = {"ts_code": "000300.SH", "start_date": "20260828", "end_date": "20260828"}
    capture = asyncio.run(provider.collect(source_id=config.source_id, parameters=parameters))
    query = _query(provider, config, parameters)
    first_response = asyncio.run(
        provider.replay((capture,)).fetch(query=query, source=_source(provider, config))
    )
    assert first_response.raw_payload is not None
    restored = load_tushare_observation_capture_bundle(
        first_response.raw_payload,
        config=config,
        parameters=parameters,
        retrieved_at=RETRIEVED,
    )
    second_response = asyncio.run(
        provider.replay((restored,)).fetch(query=query, source=_source(provider, config))
    )

    assert capture.coverage_complete is True
    assert second_response == first_response
    assert TOKEN not in first_response.raw_payload.decode()
    assert [
        cast(dict[str, object], request["params"])["offset"] for request in transport.requests
    ] == [0, 2]


def test_no_data_and_permission_denied_are_distinct_and_token_is_redacted() -> None:
    config = _configs()[0]
    parameters = {"start_date": "2026-08-28 00:00:00", "end_date": "2026-08-28 08:00:00"}
    no_data_provider = TushareObservationProvider(
        TOKEN,
        (config,),
        transport=FakeTransport([_response(config.fields, [])]),
        clock=lambda: RETRIEVED,
    )
    no_data = asyncio.run(
        no_data_provider.fetch(
            query=_query(no_data_provider, config, parameters),
            source=_source(no_data_provider, config),
        )
    )
    permission_provider = TushareObservationProvider(
        TOKEN,
        (config,),
        transport=FakeTransport([{"code": -2001, "msg": f"permission denied for {TOKEN}"}]),
        clock=lambda: RETRIEVED,
    )
    denied = asyncio.run(
        permission_provider.fetch(
            query=_query(permission_provider, config, parameters),
            source=_source(permission_provider, config),
        )
    )

    assert no_data.status is DataFetchStatus.NO_DATA
    assert no_data.raw_payload is not None
    assert denied.status is DataFetchStatus.ERROR
    assert denied.error_kind == "permission_denied"
    assert TOKEN not in repr(permission_provider)
    assert TOKEN not in repr(denied)


def test_major_news_accepts_documented_datetime_window_parameters() -> None:
    config = load_tushare_observation_source(
        Path("examples/providers/tushare-observation-major-news-v1.json")
    )
    transport = FakeTransport(
        [
            _response(
                config.fields,
                [["Example", "Licensed body", "2026-08-28 07:00:00", "Example Wire"]],
            )
        ]
    )
    provider = TushareObservationProvider(
        TOKEN,
        (config,),
        transport=transport,
        clock=lambda: RETRIEVED,
    )

    capture = asyncio.run(
        provider.collect(
            source_id=config.source_id,
            parameters={
                "start_date": "2026-08-28 06:00:00",
                "end_date": "2026-08-28 08:00:00",
            },
        )
    )

    assert capture.coverage_complete is True
    assert capture.error_kind is None
    assert len(transport.requests) == 1


def test_provider_fails_closed_for_malformed_duplicate_and_truncated_pages() -> None:
    config = _configs()[1]
    parameters = {"ts_code": "000300.SH", "start_date": "20260828", "end_date": "20260828"}
    malformed = TushareObservationProvider(
        TOKEN,
        (config,),
        transport=FakeTransport(
            [_response(("trade_date", "ts_code"), [["20260828", "000300.SH"]])]
        ),
        clock=lambda: RETRIEVED,
    )
    duplicate_row = _values(config.api_name, config.fields)
    duplicate = TushareObservationProvider(
        TOKEN,
        (config,),
        transport=FakeTransport([_response(config.fields, [duplicate_row, duplicate_row])]),
        clock=lambda: RETRIEVED,
    )
    full_page = _values(config.api_name, config.fields)
    full_page_second = [*full_page]
    full_page_second[0] = "000905.SH"
    truncated = TushareObservationProvider(
        TOKEN,
        (config,),
        transport=FakeTransport([_response(config.fields, [full_page, full_page_second])] * 2),
        clock=lambda: RETRIEVED,
    )

    malformed_response = asyncio.run(
        malformed.fetch(
            query=_query(malformed, config, parameters), source=_source(malformed, config)
        )
    )
    duplicate_response = asyncio.run(
        duplicate.fetch(
            query=_query(duplicate, config, parameters), source=_source(duplicate, config)
        )
    )
    truncated_response = asyncio.run(
        truncated.fetch(
            query=_query(truncated, config, parameters), source=_source(truncated, config)
        )
    )

    assert malformed_response.error_kind == "response_field_mismatch"
    assert duplicate_response.error_kind == "duplicate_primary_key"
    assert truncated_response.error_kind == "pagination_limit_exceeded"


def test_prospective_actual_receipt_does_not_claim_publisher_or_revision_authority(
    tmp_path: Path,
) -> None:
    config = next(item for item in _configs() if item.api_name == "report_rc")
    parameters = {"start_date": "20260828", "end_date": "20260828"}
    provider = TushareObservationProvider(
        TOKEN,
        (config,),
        transport=FakeTransport(
            [_response(config.fields, [_values(config.api_name, config.fields)])]
        ),
        clock=lambda: RETRIEVED,
    )
    harness = DataInputHarness(LocalDataSnapshotStore(tmp_path / "data"))
    harness.register(provider)

    snapshot = asyncio.run(
        harness.execute(
            _query(provider, config, parameters),
            mode=DataQueryMode.FETCH_IF_MISSING,
        )
    )
    observation = snapshot.observations[0]

    assert snapshot.coverage_complete is True
    assert observation.times.available_at == RETRIEVED
    assert observation.authority_at == RETRIEVED
    assert observation.authority_kind == "actual_receipt"
    assert observation.times.availability_basis is AvailabilityBasis.ACTUAL_RECEIPT
    assert observation.times.published_at is None
    assert observation.times.aggregator_fetched_at == datetime(2026, 8, 27, 23, 0, tzinfo=UTC)
    assert observation.normalized_payload["aggregator"] == "Tushare Pro"
    assert "publisher publication" in cast(str, observation.normalized_payload["time_semantics"])


def test_full_row_fund_div_identity_retains_revisions_and_raw_duplicates(tmp_path: Path) -> None:
    config = load_tushare_observation_source(
        Path("examples/providers/tushare-observation-fund-div-v1.json")
    )
    base: dict[str, object] = {field: None for field in config.fields}
    base.update(
        ts_code="510300.SH",
        ann_date="20250101",
        record_date="20250102",
        ex_date="20250103",
        pay_date="20250103",
        div_proc="实施",
        div_cash=0.1,
        net_ex_date="20250103",
        base_unit=100,
    )
    revision = {**base, "net_ex_date": "20250104", "base_unit": 101}
    competing = {**base, "div_cash": 0.2}
    rows = [[row[field] for field in config.fields] for row in (base, base, revision, competing)]
    transport = FakeTransport([_response(config.fields, rows)])
    provider = TushareObservationProvider(
        TOKEN, (config,), transport=transport, clock=lambda: RETRIEVED
    )
    store = LocalDataSnapshotStore(tmp_path)
    harness = DataInputHarness(store)
    harness.register(provider)
    snapshot = asyncio.run(
        harness.execute(
            _query(provider, config, {"ts_code": "510300.SH"}), mode=DataQueryMode.FETCH_IF_MISSING
        )
    )
    assert snapshot.coverage_complete and len(snapshot.observations) == 3, snapshot.attempts
    assert {o.normalized_payload["record"]["div_cash"] for o in snapshot.observations} == {0.1, 0.2}  # type: ignore[index]
    raw_hash = snapshot.attempts[0].raw_response_hash
    assert raw_hash is not None
    payload = store.artifacts.get(raw_hash, media_type="application/octet-stream").path.read_bytes()
    captured = load_tushare_observation_capture_bundle(
        payload, config=config, parameters={"ts_code": "510300.SH"}, retrieved_at=RETRIEVED
    )
    assert len(json.loads(captured.pages[0].response_body)["data"]["items"]) == 4
    assert len(transport.requests) == 1


def test_failed_parse_retains_received_page_in_snapshot_cas(tmp_path: Path) -> None:
    config = _configs()[1]
    row = _values(config.api_name, config.fields)
    provider = TushareObservationProvider(
        TOKEN,
        (config,),
        transport=FakeTransport([_response(config.fields, [row, row])]),
        clock=lambda: RETRIEVED,
    )
    store = LocalDataSnapshotStore(tmp_path)
    harness = DataInputHarness(store)
    harness.register(provider)
    snapshot = asyncio.run(
        harness.execute(
            _query(
                provider,
                config,
                {"ts_code": "000300.SH", "start_date": "20260828", "end_date": "20260828"},
            ),
            mode=DataQueryMode.FETCH_IF_MISSING,
        )
    )
    attempt = store.get(snapshot.snapshot_id).attempts[0]
    assert not snapshot.coverage_complete and not snapshot.observations
    assert attempt.error_kind == "duplicate_primary_key" and attempt.received_count == 0
    assert attempt.raw_response_hash is not None
    raw = store.artifacts.get(
        attempt.raw_response_hash, media_type="application/octet-stream"
    ).path.read_bytes()
    assert b'"items"' in raw and TOKEN.encode() not in raw
