from __future__ import annotations

import json
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from pathlib import Path
from typing import cast

import pytest

from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.agent_schema import validate_agent_contract
from market_impact_agent.checkpoint_decision_inputs import project_checkpoint_observation
from market_impact_agent.checkpoint_market_universe import (
    build_checkpoint_market_universe_view,
    checkpoint_market_universe_view_from_dict,
    load_exchange_instrument_rule_set,
)
from market_impact_agent.data_inputs import SourceObservation
from market_impact_agent.observations import (
    AvailabilityBasis,
    ObservationCapability,
    ObservationTimes,
    OccurrenceBasis,
)

ROOT = Path(__file__).parents[1]
BARRIER = datetime(2026, 8, 28, 8, 0, tzinfo=UTC)
RECEIVED = BARRIER - timedelta(minutes=30)
SNAPSHOT_SET_ID = "prospective-checkpoint-snapshot-set-" + "a" * 64


def _observation(
    capability: ObservationCapability,
    *,
    source_id: str,
    payload: dict[str, object],
) -> SourceObservation:
    return SourceObservation.build(
        capability=capability,
        provider_id="fixture-provider",
        provider_version="1",
        upstream_source=source_id,
        upstream_record_id=f"{source_id}:fixture",
        source_ref=f"https://fixture.example/{source_id}",
        lineage_id=f"{source_id}:fixture",
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
        raw_content_hash=sha256(source_id.encode()).hexdigest(),
        normalized_payload=payload,
        license_scope="private_research_no_redistribution",
    )


def _project(
    capability: ObservationCapability,
    *,
    source_id: str,
    route_kind: str,
    payload: dict[str, object],
) -> dict[str, object]:
    return project_checkpoint_observation(
        checkpoint_snapshot_set_id=SNAPSHOT_SET_ID,
        checkpoint_key="policy-event",
        barrier_at=BARRIER,
        snapshot_id="data-snapshot-" + sha256(source_id.encode()).hexdigest(),
        route_kinds=(route_kind,),
        observation=_observation(capability, source_id=source_id, payload=payload),
    )


def _decision_inputs() -> tuple[dict[str, object], ...]:
    market = ObservationCapability.MARKET_CONTEXT
    exposure = ObservationCapability.EXPOSURE_CANDIDATES
    return (
        _project(
            market,
            source_id="trade-cal",
            route_kind="trading_calendar",
            payload={
                "api_name": "trade_cal",
                "record": {
                    "exchange": "SSE",
                    "cal_date": "20260828",
                    "is_open": "1",
                    "pretrade_date": "20260827",
                },
            },
        ),
        _project(
            market,
            source_id="index-daily",
            route_kind="market_index_price",
            payload={
                "api_name": "index_daily",
                "record": {
                    "ts_code": "000300.SH",
                    "trade_date": "20260828",
                    "close": 4100.0,
                },
            },
        ),
        _project(
            market,
            source_id="fund-daily",
            route_kind="etf_price",
            payload={
                "api_name": "fund_daily",
                "record": {
                    "ts_code": "512010.SH",
                    "trade_date": "20260828",
                    "close": 0.82,
                },
            },
        ),
        _project(
            exposure,
            source_id="etf-basic",
            route_kind="tradable_instrument_master",
            payload={
                "api_name": "etf_basic",
                "record": {
                    "ts_code": "512010.SH",
                    "csname": "Medicine ETF",
                    "index_code": "801150.SI",
                    "list_date": "20131028",
                    "list_status": "L",
                    "exchange": "SSE",
                },
            },
        ),
        _project(
            exposure,
            source_id="stock-basic",
            route_kind="tradable_instrument_master",
            payload={
                "api_name": "stock_basic",
                "record": {
                    "ts_code": "600000.SH",
                    "name": "Fixture issuer",
                    "list_date": "19991110",
                    "delist_date": None,
                    "list_status": "L",
                    "exchange": "SSE",
                },
            },
        ),
        _project(
            exposure,
            source_id="stk-limit",
            route_kind="tradable_instrument_master",
            payload={
                "api_name": "stk_limit",
                "record": {
                    "ts_code": "600000.SH",
                    "trade_date": "20260828",
                    "pre_close": 10.0,
                    "up_limit": 11.0,
                    "down_limit": 9.0,
                },
            },
        ),
        _project(
            exposure,
            source_id="index-classify",
            route_kind="industry_to_tradable_mapping",
            payload={
                "api_name": "index_classify",
                "record": {
                    "index_code": "801150.SI",
                    "industry_code": "801150.SI",
                    "industry_name": "Medicine and biology",
                    "level": "L1",
                    "src": "SW2021",
                },
            },
        ),
        _project(
            exposure,
            source_id="index-member-all",
            route_kind="effective_industry_membership",
            payload={
                "api_name": "index_member_all",
                "record": {
                    "l1_code": "801150.SI",
                    "l1_name": "Medicine and biology",
                    "ts_code": "600000.SH",
                    "name": "Fixture issuer",
                    "in_date": "20260101",
                    "out_date": None,
                    "src": "SW2021",
                },
            },
        ),
    )


def _build() -> dict[str, object]:
    return build_checkpoint_market_universe_view(
        decision_inputs=_decision_inputs(),
        rule_set=load_exchange_instrument_rule_set(
            ROOT / "examples/research/a-share-exchange-instrument-rules-v1.json"
        ),
        target_venues=("XSHG", "XSHE"),
        allowed_instrument_classes=("exchange_traded_fund",),
    )


def test_exchange_instrument_rule_set_conforms_to_public_schema() -> None:
    payload = json.loads(
        (ROOT / "examples/research/a-share-exchange-instrument-rules-v1.json").read_text()
    )

    assert validate_agent_contract(payload, "exchange-instrument-rule-set.schema.json") == ()


def test_market_universe_view_binds_price_basis_rules_and_industry_exposure() -> None:
    view = _build()

    assert checkpoint_market_universe_view_from_dict(view) == view
    assert validate_agent_contract(view, "checkpoint-market-universe-view.schema.json") == ()
    assert view["checkpoint_snapshot_set_id"] == SNAPSHOT_SET_ID
    assert view["historical_pit_claim"] is False
    assert view["evidence_promoted"] is False
    assert view["execution_capability"] is False
    assert view["model_call_authorized"] is False

    market_inputs = cast(list[dict[str, object]], view["market_inputs"])
    index_input = next(item for item in market_inputs if item["record_type"] == "index_price_bar")
    fund_input = next(item for item in market_inputs if item["record_type"] == "fund_price_bar")
    assert cast(dict[str, object], index_input["price_basis"])["execution_basis"] is None
    assert (
        cast(dict[str, object], fund_input["price_basis"])["execution_basis"]
        == "raw_tradable_price"
    )

    instruments = cast(list[dict[str, object]], view["instruments"])
    etf = next(item for item in instruments if item["instrument_code"] == "512010.SH")
    assert etf["venue"] == "XSHG"
    assert etf["buy_lot_size"] == 100
    assert etf["price_tick"] == 0.001
    assert etf["decision_time_tradability"] == "unverified"
    assert etf["research_eligible"] is True
    assert "suspension_status_unverified" in cast(list[str], etf["completeness_gaps"])

    exposures = cast(list[dict[str, object]], view["industry_exposures"])
    assert len(exposures) == 1
    mapping = exposures[0]
    assert mapping["taxonomy_version"] == "SW2021"
    assert mapping["index_code"] == "801150.SI"
    assert mapping["instrument_code"] == "512010.SH"
    assert mapping["observed_at_barrier"] is True
    assert mapping["effective_at_barrier"] is None
    assert mapping["constituent_count"] == 1
    assert "taxonomy_effective_interval_unverified" in cast(list[str], mapping["completeness_gaps"])
    assert "rebalance_lineage_missing" in cast(list[str], mapping["completeness_gaps"])


def test_market_universe_view_is_deterministic_and_rejects_content_drift() -> None:
    first = _build()
    assert first == _build()

    changed = deepcopy(first)
    instruments = cast(list[dict[str, object]], changed["instruments"])
    instruments[0]["price_tick"] = 1.0
    with pytest.raises(ValueError, match="view_id does not match content"):
        checkpoint_market_universe_view_from_dict(changed)


def test_market_universe_view_rejects_mixed_snapshot_sets() -> None:
    inputs = list(_decision_inputs())
    changed = deepcopy(inputs[-1])
    changed["checkpoint_snapshot_set_id"] = "prospective-checkpoint-snapshot-set-" + "f" * 64
    core = {key: value for key, value in changed.items() if key != "record_id"}
    changed["record_id"] = f"checkpoint-decision-input-{canonical_hash(core)}"
    inputs[-1] = changed

    with pytest.raises(ValueError, match="one checkpoint Snapshot Set"):
        build_checkpoint_market_universe_view(
            decision_inputs=tuple(inputs),
            rule_set=load_exchange_instrument_rule_set(
                ROOT / "examples/research/a-share-exchange-instrument-rules-v1.json"
            ),
            target_venues=("XSHG", "XSHE"),
            allowed_instrument_classes=("exchange_traded_fund",),
        )


def test_market_universe_view_does_not_apply_future_exchange_rules() -> None:
    inputs = list(_decision_inputs())
    for item in inputs:
        item["barrier_at"] = "2026-06-30T08:00:00Z"
        times = cast(dict[str, object], item["times"])
        times["occurred_at"] = "2026-06-30T07:25:00Z"
        times["published_at"] = "2026-06-30T07:25:00Z"
        times["source_updated_at"] = "2026-06-30T07:25:00Z"
        times["available_at"] = "2026-06-30T07:30:00Z"
        times["authority_at"] = "2026-06-30T07:30:00Z"
        times["retrieved_at"] = "2026-06-30T07:30:00Z"
        core = {key: value for key, value in item.items() if key != "record_id"}
        item["record_id"] = f"checkpoint-decision-input-{canonical_hash(core)}"

    view = build_checkpoint_market_universe_view(
        decision_inputs=tuple(inputs),
        rule_set=load_exchange_instrument_rule_set(
            ROOT / "examples/research/a-share-exchange-instrument-rules-v1.json"
        ),
        target_venues=("XSHG", "XSHE"),
        allowed_instrument_classes=("exchange_traded_fund",),
    )

    instruments = cast(list[dict[str, object]], view["instruments"])
    assert all(item["buy_lot_size"] is None for item in instruments)
    assert "instrument_rule_not_effective_at_barrier" in cast(list[str], view["completeness_gaps"])


def test_market_universe_view_reports_absent_exact_industry_to_etf_mapping() -> None:
    inputs = tuple(
        item for item in _decision_inputs() if item["record_type"] != "industry_taxonomy"
    )

    view = build_checkpoint_market_universe_view(
        decision_inputs=inputs,
        rule_set=load_exchange_instrument_rule_set(
            ROOT / "examples/research/a-share-exchange-instrument-rules-v1.json"
        ),
        target_venues=("XSHG", "XSHE"),
        allowed_instrument_classes=("exchange_traded_fund",),
    )

    assert view["industry_exposures"] == []
    assert "industry_to_tradable_mapping_missing" in cast(list[str], view["completeness_gaps"])


def test_market_universe_view_excludes_future_listed_etf_from_industry_mapping() -> None:
    inputs = list(_decision_inputs())
    etf = next(item for item in inputs if item["record_type"] == "tradable_instrument_mapping")
    etf_data = cast(dict[str, object], etf["data"])
    etf_data["effective_from"] = "20260829"
    etf_data["effective_at_barrier"] = False
    core = {key: value for key, value in etf.items() if key != "record_id"}
    etf["record_id"] = f"checkpoint-decision-input-{canonical_hash(core)}"

    view = build_checkpoint_market_universe_view(
        decision_inputs=tuple(inputs),
        rule_set=load_exchange_instrument_rule_set(
            ROOT / "examples/research/a-share-exchange-instrument-rules-v1.json"
        ),
        target_venues=("XSHG", "XSHE"),
        allowed_instrument_classes=("exchange_traded_fund",),
    )

    instruments = cast(list[dict[str, object]], view["instruments"])
    assert instruments[0]["research_eligible"] is False
    assert view["industry_exposures"] == []
    assert "industry_to_tradable_mapping_missing" in cast(list[str], view["completeness_gaps"])


def test_market_universe_view_rejects_cross_source_industry_membership_join() -> None:
    inputs = list(_decision_inputs())
    membership = next(item for item in inputs if item["record_type"] == "industry_membership")
    membership_data = cast(dict[str, object], membership["data"])
    membership_data["taxonomy_source"] = "CITICS2020"
    core = {key: value for key, value in membership.items() if key != "record_id"}
    membership["record_id"] = f"checkpoint-decision-input-{canonical_hash(core)}"

    view = build_checkpoint_market_universe_view(
        decision_inputs=tuple(inputs),
        rule_set=load_exchange_instrument_rule_set(
            ROOT / "examples/research/a-share-exchange-instrument-rules-v1.json"
        ),
        target_venues=("XSHG", "XSHE"),
        allowed_instrument_classes=("exchange_traded_fund",),
    )

    assert view["industry_exposures"] == []
    assert "industry_to_tradable_mapping_missing" in cast(list[str], view["completeness_gaps"])
    assert "taxonomy_source_mismatch" in cast(list[str], view["completeness_gaps"])


def test_market_universe_view_collapses_duplicate_instrument_versions_deterministically() -> None:
    inputs = list(_decision_inputs())
    original = next(item for item in inputs if item["record_type"] == "tradable_instrument_mapping")
    duplicate = deepcopy(original)
    duplicate["observation_id"] = "source-observation-" + "f" * 64
    duplicate_data = cast(dict[str, object], duplicate["data"])
    duplicate_data["instrument_name"] = "Medicine ETF alternate master version"
    core = {key: value for key, value in duplicate.items() if key != "record_id"}
    duplicate["record_id"] = f"checkpoint-decision-input-{canonical_hash(core)}"
    inputs.append(duplicate)
    rule_set = load_exchange_instrument_rule_set(
        ROOT / "examples/research/a-share-exchange-instrument-rules-v1.json"
    )

    forward = build_checkpoint_market_universe_view(
        decision_inputs=tuple(inputs),
        rule_set=rule_set,
        target_venues=("XSHG", "XSHE"),
        allowed_instrument_classes=("exchange_traded_fund",),
    )
    reverse = build_checkpoint_market_universe_view(
        decision_inputs=tuple(reversed(inputs)),
        rule_set=rule_set,
        target_venues=("XSHG", "XSHE"),
        allowed_instrument_classes=("exchange_traded_fund",),
    )

    assert forward == reverse
    instruments = cast(list[dict[str, object]], forward["instruments"])
    assert len(instruments) == 1
    assert "instrument_master_versions_present" in cast(
        list[str], instruments[0]["completeness_gaps"]
    )


def test_exchange_rule_effective_date_uses_a_share_market_date() -> None:
    inputs = list(_decision_inputs())
    for item in inputs:
        item["barrier_at"] = "2026-07-05T16:30:00Z"
        times = cast(dict[str, object], item["times"])
        times["occurred_at"] = "2026-07-05T15:55:00Z"
        times["published_at"] = "2026-07-05T15:55:00Z"
        times["source_updated_at"] = "2026-07-05T15:55:00Z"
        times["available_at"] = "2026-07-05T16:00:00Z"
        times["authority_at"] = "2026-07-05T16:00:00Z"
        times["retrieved_at"] = "2026-07-05T16:00:00Z"
        core = {key: value for key, value in item.items() if key != "record_id"}
        item["record_id"] = f"checkpoint-decision-input-{canonical_hash(core)}"

    view = build_checkpoint_market_universe_view(
        decision_inputs=tuple(inputs),
        rule_set=load_exchange_instrument_rule_set(
            ROOT / "examples/research/a-share-exchange-instrument-rules-v1.json"
        ),
        target_venues=("XSHG", "XSHE"),
        allowed_instrument_classes=("exchange_traded_fund",),
    )

    instruments = cast(list[dict[str, object]], view["instruments"])
    assert instruments[0]["buy_lot_size"] == 100


def test_market_universe_view_handles_multiple_taxonomy_versions_deterministically() -> None:
    inputs = list(_decision_inputs())
    original = next(item for item in inputs if item["record_type"] == "industry_taxonomy")
    duplicate = deepcopy(original)
    duplicate["observation_id"] = "source-observation-" + "e" * 64
    duplicate_data = cast(dict[str, object], duplicate["data"])
    duplicate_data["industry_name"] = "Alternate taxonomy label"
    core = {key: value for key, value in duplicate.items() if key != "record_id"}
    duplicate["record_id"] = f"checkpoint-decision-input-{canonical_hash(core)}"
    inputs.append(duplicate)
    rule_set = load_exchange_instrument_rule_set(
        ROOT / "examples/research/a-share-exchange-instrument-rules-v1.json"
    )

    forward = build_checkpoint_market_universe_view(
        decision_inputs=tuple(inputs),
        rule_set=rule_set,
        target_venues=("XSHG", "XSHE"),
        allowed_instrument_classes=("exchange_traded_fund",),
    )
    reverse = build_checkpoint_market_universe_view(
        decision_inputs=tuple(reversed(inputs)),
        rule_set=rule_set,
        target_venues=("XSHG", "XSHE"),
        allowed_instrument_classes=("exchange_traded_fund",),
    )

    assert forward == reverse
    exposures = cast(list[dict[str, object]], forward["industry_exposures"])
    assert "taxonomy_versions_present" in cast(list[str], exposures[0]["completeness_gaps"])


def test_market_universe_view_binds_latest_raw_price_record() -> None:
    inputs = list(_decision_inputs())
    latest = next(item for item in inputs if item["record_type"] == "fund_price_bar")
    older = deepcopy(latest)
    older["observation_id"] = "source-observation-" + "d" * 64
    older_data = cast(dict[str, object], older["data"])
    older_data["trade_date"] = "20260827"
    core = {key: value for key, value in older.items() if key != "record_id"}
    older["record_id"] = f"checkpoint-decision-input-{canonical_hash(core)}"
    inputs.append(older)
    rule_set = load_exchange_instrument_rule_set(
        ROOT / "examples/research/a-share-exchange-instrument-rules-v1.json"
    )

    view = build_checkpoint_market_universe_view(
        decision_inputs=tuple(reversed(inputs)),
        rule_set=rule_set,
        target_venues=("XSHG", "XSHE"),
        allowed_instrument_classes=("exchange_traded_fund",),
    )

    instruments = cast(list[dict[str, object]], view["instruments"])
    assert instruments[0]["raw_price_record_id"] == latest["record_id"]
    assert "multiple_raw_price_records_present" in cast(
        list[str], instruments[0]["completeness_gaps"]
    )
