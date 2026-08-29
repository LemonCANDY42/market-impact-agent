from __future__ import annotations

from copy import deepcopy
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from typing import cast

import pytest

from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.agent_schema import validate_agent_contract
from market_impact_agent.checkpoint_decision_inputs import (
    checkpoint_decision_input_from_dict,
    project_checkpoint_observation,
)
from market_impact_agent.data_inputs import SourceObservation
from market_impact_agent.observations import (
    AvailabilityBasis,
    ObservationCapability,
    ObservationTimes,
    OccurrenceBasis,
)

BARRIER = datetime(2026, 8, 28, 8, 0, tzinfo=UTC)
RECEIVED = BARRIER - timedelta(minutes=30)


def _observation(
    capability: ObservationCapability,
    payload: dict[str, object],
    *,
    source_ref: str = "https://fixture.example/record",
) -> SourceObservation:
    return SourceObservation.build(
        capability=capability,
        provider_id="fixture-provider",
        provider_version="1",
        upstream_source="fixture-source",
        upstream_record_id="fixture-record",
        source_ref=source_ref,
        lineage_id="fixture-source:fixture-record",
        times=ObservationTimes(
            occurred_at=RECEIVED - timedelta(minutes=2),
            published_at=RECEIVED - timedelta(minutes=2),
            available_at=RECEIVED,
            source_updated_at=RECEIVED - timedelta(minutes=2),
            aggregator_fetched_at=None,
            retrieved_at=RECEIVED,
            occurrence_basis=OccurrenceBasis.SOURCE_REPORTED,
            availability_basis=AvailabilityBasis.ACTUAL_RECEIPT,
        ),
        authority_at=RECEIVED,
        authority_kind="actual_receipt",
        raw_content_hash=sha256(b"fixture").hexdigest(),
        normalized_payload=payload,
        license_scope="private_research_no_redistribution",
    )


def _project(observation: SourceObservation) -> dict[str, object]:
    return project_checkpoint_observation(
        checkpoint_snapshot_set_id=("prospective-checkpoint-snapshot-set-" + "a" * 64),
        checkpoint_key="policy-event",
        barrier_at=BARRIER,
        snapshot_id="data-snapshot-" + "b" * 64,
        route_kinds=("official_event",),
        observation=observation,
    )


def _validate_schema(projected: dict[str, object]) -> None:
    assert validate_agent_contract(projected, "checkpoint-decision-input.schema.json") == ()


def test_checkpoint_decision_input_round_trip_rejects_content_drift() -> None:
    projected = _project(
        _observation(
            ObservationCapability.EVENT_REVELATION,
            {
                "publisher": "Official publisher",
                "headline": "Policy event",
                "summary": "A prospectively received official event fact.",
            },
        )
    )

    assert checkpoint_decision_input_from_dict(projected) == projected

    changed = deepcopy(projected)
    data = cast(dict[str, object], changed["data"])
    data["headline"] = "Changed after identity was frozen"
    with pytest.raises(ValueError, match="record_id does not match content"):
        checkpoint_decision_input_from_dict(changed)


def test_checkpoint_decision_input_rejects_reidentified_schema_drift() -> None:
    changed = _project(
        _observation(
            ObservationCapability.EVENT_REVELATION,
            {"publisher": "Official publisher", "headline": "Policy event"},
        )
    )
    data = cast(dict[str, object], changed["data"])
    data["provider_sentiment"] = "bullish"
    core = {key: value for key, value in changed.items() if key != "record_id"}
    changed["record_id"] = f"checkpoint-decision-input-{canonical_hash(core)}"

    with pytest.raises(ValueError, match="does not conform to its schema"):
        checkpoint_decision_input_from_dict(changed)


@pytest.mark.parametrize(
    ("capability", "payload", "expected_type", "expected_gaps"),
    (
        (
            ObservationCapability.PRIOR_EXPECTATION,
            {
                "api_name": "report_rc",
                "upstream_publisher": "Tushare Pro",
                "record": {
                    "ts_code": "600000.SH",
                    "report_date": "20260828",
                    "org_name": "Fixture Research",
                    "author_name": "Fixture Analyst",
                    "quarter": "2026Q3",
                    "eps": 1.25,
                    "tp": 12.5,
                },
            },
            "forecast_observation",
            ["consensus_not_derived", "reported_metric_units_unverified"],
        ),
        (
            ObservationCapability.MARKET_CONTEXT,
            {
                "api_name": "index_daily",
                "upstream_publisher": "Tushare Pro",
                "record": {
                    "ts_code": "000300.SH",
                    "trade_date": "20260828",
                    "open": 4100.0,
                    "high": 4120.0,
                    "low": 4090.0,
                    "close": 4110.0,
                },
            },
            "index_price_bar",
            ["total_return_series_missing"],
        ),
        (
            ObservationCapability.EXPOSURE_CANDIDATES,
            {
                "api_name": "index_member_all",
                "upstream_publisher": "Shenwan Hongyuan Research",
                "record": {
                    "l1_code": "801010.SI",
                    "l1_name": "Agriculture",
                    "ts_code": "600000.SH",
                    "in_date": "20260101",
                    "out_date": "20261231",
                },
            },
            "industry_membership",
            ["industry_to_tradable_mapping_missing", "taxonomy_version_unverified"],
        ),
        (
            ObservationCapability.POSITIONING,
            {
                "api_name": "margin",
                "upstream_publisher": "Tushare Pro",
                "record": {
                    "exchange_id": "SSE",
                    "trade_date": "20260828",
                    "rzye": 10.0,
                    "rqye": 2.0,
                    "rzrqye": 12.0,
                },
            },
            "margin_financing_snapshot",
            [
                "publication_cadence_unverified",
                "reported_units_unverified",
                "revision_policy_unverified",
            ],
        ),
        (
            ObservationCapability.MACRO_VINTAGE,
            {
                "api_name": "cn_schedule",
                "upstream_publisher": "Tushare Pro",
                "record": {
                    "month": "202607",
                    "publish_date": "20260809",
                    "title": "National CPI release",
                    "issuing_org": "NBS",
                    "data_api": "cn_cpi",
                },
            },
            "macro_release_schedule",
            ["original_release_missing", "revision_lineage_missing"],
        ),
    ),
)
def test_provider_specific_rows_project_to_fail_closed_decision_inputs(
    capability: ObservationCapability,
    payload: dict[str, object],
    expected_type: str,
    expected_gaps: list[str],
) -> None:
    projected = _project(_observation(capability, payload))

    assert projected["record_type"] == expected_type
    assert projected["completeness_gaps"] == expected_gaps
    assert projected["historical_pit_claim"] is False
    assert projected["evidence_promoted"] is False
    assert projected["execution_capability"] is False
    assert checkpoint_decision_input_from_dict(projected) == projected
    _validate_schema(projected)
    data = cast(dict[str, object], projected["data"])
    assert "expectation_delta" not in data
    assert "consensus_value" not in data


def test_nbs_original_release_projects_without_a_missing_original_gap() -> None:
    observation = _observation(
        ObservationCapability.MACRO_VINTAGE,
        {
            "record_type": "original_release",
            "indicator": "cpi",
            "reference_period": "2026-07",
            "release_title": "2026年7月份居民消费价格同比上涨0.5%",
            "release_summary": "Official CPI original release.",
            "release_url": "https://www.stats.gov.cn/sj/zxfb/202608/release.html",
            "publisher": "国家统计局",
            "published_at": "2026-08-09T01:30:00Z",
            "attachments": [
                {
                    "url": "https://www.stats.gov.cn/sj/zxfb/202608/table.xlsx",
                    "filename": "table.xlsx",
                    "content_type": (
                        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    ),
                    "size_bytes": 100,
                    "sha256": "a" * 64,
                }
            ],
            "revision_lineage": [],
        },
        source_ref="https://www.stats.gov.cn/sj/zxfb/202608/release.html",
    )

    projected = _project(observation)

    assert projected["record_type"] == "macro_original_release"
    assert projected["completeness_gaps"] == ["revision_lineage_missing"]
    data = cast(dict[str, object], projected["data"])
    assert data["original_release_observation_id"] == observation.observation_id
    assert data["release_summary"] == "Official CPI original release."
    assert data["release_url"] == "https://www.stats.gov.cn/sj/zxfb/202608/release.html"
    assert checkpoint_decision_input_from_dict(projected) == projected
    _validate_schema(projected)


@pytest.mark.parametrize(
    ("field", "invalid_value"),
    (
        ("original_release_observation_id", None),
        ("release_url", None),
        ("release_url", "/not-a-direct-release-url"),
        ("attachments", []),
    ),
)
def test_macro_original_release_loader_rejects_missing_authoritative_bindings(
    field: str,
    invalid_value: object,
) -> None:
    projected = _project(
        _observation(
            ObservationCapability.MACRO_VINTAGE,
            {
                "record_type": "original_release",
                "indicator": "cpi",
                "reference_period": "2026-07",
                "release_title": "2026年7月份居民消费价格同比上涨0.5%",
                "release_url": "https://www.stats.gov.cn/sj/zxfb/202608/release.html",
                "publisher": "国家统计局",
                "published_at": "2026-08-09T01:30:00Z",
                "attachments": [
                    {
                        "url": "https://www.stats.gov.cn/sj/zxfb/202608/table.xlsx",
                        "filename": "table.xlsx",
                        "content_type": (
                            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                        ),
                        "size_bytes": 100,
                        "sha256": "a" * 64,
                    }
                ],
            },
        )
    )
    data = cast(dict[str, object], projected["data"])
    data[field] = invalid_value
    core = {key: value for key, value in projected.items() if key != "record_id"}
    projected["record_id"] = f"checkpoint-decision-input-{canonical_hash(core)}"

    with pytest.raises(ValueError, match="does not conform to its schema"):
        checkpoint_decision_input_from_dict(projected)


@pytest.mark.parametrize(
    ("field", "invalid_value", "message"),
    (
        ("original_release_observation_id", "source-observation-" + "c" * 64, "observation ID"),
        (
            "release_url",
            "https://www.stats.gov.cn/sj/zxfb/202608/different-release.html",
            "direct source reference",
        ),
    ),
)
def test_macro_original_release_loader_rejects_mismatched_authority_bindings(
    field: str,
    invalid_value: str,
    message: str,
) -> None:
    projected = _project(
        _observation(
            ObservationCapability.MACRO_VINTAGE,
            {
                "record_type": "original_release",
                "indicator": "cpi",
                "reference_period": "2026-07",
                "release_title": "2026年7月份居民消费价格同比上涨0.5%",
                "publisher": "国家统计局",
                "published_at": "2026-08-09T01:30:00Z",
                "attachments": [
                    {
                        "url": "https://www.stats.gov.cn/sj/zxfb/202608/table.xlsx",
                        "filename": "table.xlsx",
                        "content_type": (
                            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                        ),
                        "size_bytes": 100,
                        "sha256": "a" * 64,
                    }
                ],
            },
        )
    )
    data = cast(dict[str, object], projected["data"])
    data[field] = invalid_value
    core = {key: value for key, value in projected.items() if key != "record_id"}
    projected["record_id"] = f"checkpoint-decision-input-{canonical_hash(core)}"

    with pytest.raises(ValueError, match=message):
        checkpoint_decision_input_from_dict(projected)


def test_macro_release_schedule_keeps_legacy_nullable_and_missing_fields() -> None:
    projected = _project(
        _observation(
            ObservationCapability.MACRO_VINTAGE,
            {
                "api_name": "cn_schedule",
                "record": {
                    "month": "202607",
                    "publish_date": "20260809",
                    "title": "National CPI release",
                    "issuing_org": "NBS",
                    "data_api": "cn_cpi",
                },
            },
        )
    )

    data = cast(dict[str, object], projected["data"])
    assert data["original_release_observation_id"] is None
    assert "release_url" not in data
    assert "attachments" not in data
    assert checkpoint_decision_input_from_dict(projected) == projected
    _validate_schema(projected)


def test_projected_decision_input_conforms_to_public_schema() -> None:
    projected = _project(
        _observation(
            ObservationCapability.EVENT_REVELATION,
            {
                "publisher": "Official publisher",
                "headline": "Policy event",
                "summary": "A prospectively received official event fact.",
            },
        )
    )
    _validate_schema(projected)


def test_fund_price_projection_does_not_fabricate_an_index_code() -> None:
    projected = _project(
        _observation(
            ObservationCapability.MARKET_CONTEXT,
            {
                "api_name": "fund_daily",
                "upstream_publisher": "Tushare Pro",
                "record": {
                    "ts_code": "510300.SH",
                    "trade_date": "20260828",
                    "close": 4.25,
                },
            },
        )
    )

    assert projected["record_type"] == "fund_price_bar"
    data = cast(dict[str, object], projected["data"])
    assert data["instrument_code"] == "510300.SH"
    assert data["index_code"] is None
    _validate_schema(projected)


def test_index_member_sparse_taxonomy_does_not_mix_incomplete_levels() -> None:
    projected = _project(
        _observation(
            ObservationCapability.EXPOSURE_CANDIDATES,
            {
                "api_name": "index_member_all",
                "upstream_publisher": "Shenwan Hongyuan Research",
                "record": {
                    "l3_code": "850111.SI",
                    "l3_name": None,
                    "l2_code": None,
                    "l2_name": "Incomplete level two",
                    "l1_code": "801010.SI",
                    "l1_name": None,
                    "ts_code": "600000.SH",
                    "in_date": "20260101",
                },
            },
        )
    )

    data = cast(dict[str, object], projected["data"])
    assert data["industry_code"] is None
    assert data["industry_name"] is None
    assert data["taxonomy_level"] is None
    assert projected["completeness_gaps"] == [
        "industry_taxonomy_pair_incomplete",
        "industry_to_tradable_mapping_missing",
        "taxonomy_version_unverified",
    ]
    _validate_schema(projected)


def test_index_member_uses_deepest_complete_taxonomy_tuple() -> None:
    projected = _project(
        _observation(
            ObservationCapability.EXPOSURE_CANDIDATES,
            {
                "api_name": "index_member_all",
                "record": {
                    "l3_code": "850111.SI",
                    "l3_name": None,
                    "l2_code": "850100.SI",
                    "l2_name": "Crop farming",
                    "l1_code": "801010.SI",
                    "l1_name": "Agriculture",
                    "ts_code": "600000.SH",
                },
            },
        )
    )

    data = cast(dict[str, object], projected["data"])
    assert data["industry_code"] == "850100.SI"
    assert data["industry_name"] == "Crop farming"
    assert data["taxonomy_level"] == "l2"
    assert "industry_taxonomy_pair_incomplete" not in cast(
        list[str], projected["completeness_gaps"]
    )
    _validate_schema(projected)


def test_index_member_binds_current_shenwan_route_identity_without_record_src() -> None:
    projected = _project(
        _observation(
            ObservationCapability.EXPOSURE_CANDIDATES,
            {
                "api_name": "index_member_all",
                "record": {
                    "l1_code": "801150.SI",
                    "l1_name": "Medicine and biology",
                    "ts_code": "600000.SH",
                    "in_date": "20260101",
                    "is_new": "Y",
                },
            },
        )
    )

    data = cast(dict[str, object], projected["data"])
    assert data["taxonomy_family"] == "shenwan"
    assert data["taxonomy_source"] is None
    assert "taxonomy_version_unverified" in cast(list[str], projected["completeness_gaps"])
    _validate_schema(projected)


@pytest.mark.parametrize("api_name", ("etf_sh_cons", "etf_sz_cons"))
def test_exchange_pcf_constituent_projects_exact_daily_mapping(api_name: str) -> None:
    projected = _project(
        _observation(
            ObservationCapability.EXPOSURE_CANDIDATES,
            {
                "api_name": api_name,
                "upstream_publisher": "Tushare Pro",
                "record": {
                    "trade_date": "20260828",
                    "ts_code": "517030.SH" if api_name == "etf_sh_cons" else "159051.SZ",
                    "con_code": "600000.SH",
                    "con_name": "Fixture constituent",
                    "qty": 1000.0,
                    "sub_flag": "1",
                    "cpr": 0.1,
                    "rdr": 0.2,
                    "sca": 100.0 if api_name == "etf_sh_cons" else None,
                    "sub_cc": 90.0 if api_name == "etf_sz_cons" else None,
                    "red_cc": 110.0 if api_name == "etf_sz_cons" else None,
                    "exchange": "SH" if api_name == "etf_sh_cons" else "SZ",
                },
            },
        )
    )

    assert projected["record_type"] == "etf_basket_constituent"
    data = cast(dict[str, object], projected["data"])
    assert data["effective_at_barrier"] is True
    assert data["effective_from"] == "20260828"
    assert data["effective_to"] == "20260828"
    assert data["instrument_class"] == "exchange_traded_fund"
    assert data["constituent_code"] == "600000.SH"
    assert data["constituent_quantity"] == 1000.0
    assert projected["completeness_gaps"] == [
        "basket_publication_time_unverified",
        "basket_revision_lineage_missing",
        "basket_weight_missing",
    ]
    _validate_schema(projected)
