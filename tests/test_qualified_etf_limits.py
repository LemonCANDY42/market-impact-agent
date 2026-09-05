# pyright: reportPrivateUsage=false
"""Qualified limits remain a separate modeled scenario with unchanged legacy bindings."""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, cast

import pytest

from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.continuous_baselines import _source_binding_hash
from market_impact_agent.dynamic_ashare_admission import DynamicAShareAdmission
from market_impact_agent.historical_ashare_inputs import HistoricalAShareInputs
from market_impact_agent.streaming_nautilus_account import HistoricalStreamingAccount
from tests.test_historical_ashare_inputs import _capture, _source

DAY = date(2025, 1, 3)
CUTOFF = datetime(2025, 1, 3, 1, 25, tzinfo=UTC)
SYMBOL = "510300.SH"
BASIS = "qualified_seed_etf_exchange_rule_v1"


@pytest.fixture
def qualified(tmp_path: Path) -> HistoricalAShareInputs:
    source = _source(tmp_path)
    rule = cast(dict[str, Any], source.store.artifacts.read_json(source.rule_artifact_hashes[0]))
    rule["qualified_limit_reference"] = {
        "schema_version": "market-impact.qualified-seed-etf-limit-reference.v1",
        "normal_session_assumption": True,
        "domestic_equity_etf": True,
        "identity_source_artifact_hash": source.store.put_raw(b"Synthetic issuer identity"),
        "identity_source_url": "https://example.test/historical-listing",
        "listing_date": "2010-01-01",
    }
    return HistoricalAShareInputs(
        store=source.store,
        snapshot_ids=source.snapshot_ids,
        rule_artifact_hashes=(
            source.store.artifacts.put_json(rule).content_hash,
            *source.rule_artifact_hashes[1:],
        ),
        policy=replace(source.policy, policy_id="fixture-qualified-v1", limit_basis=BASIS),
    )


def _replace_api(
    source: HistoricalAShareInputs, api: str, rows: list[dict[str, object]]
) -> HistoricalAShareInputs:
    ids = tuple(
        snapshot
        for snapshot in source.snapshot_ids
        if source.store.get(snapshot).query.sources[0].upstream_source
        != "tushare-" + api.replace("_", "-")
    )
    new = _capture(source.store, api, {"ts_code": SYMBOL}, rows)
    return HistoricalAShareInputs(
        store=source.store,
        snapshot_ids=(*ids, new),
        rule_artifact_hashes=source.rule_artifact_hashes,
        policy=source.policy,
    )


def test_legacy_policy_identity_and_evidence_remain_exact(
    qualified: HistoricalAShareInputs,
) -> None:
    legacy_policy = replace(qualified.policy, limit_basis="reported_stk_limit")
    expected = {
        "policy_id": legacy_policy.policy_id,
        "daily_open_volume_fraction": "0.01",
        "lane": "modeled_pit",
        "opening_tick_validity_microseconds": 1,
    }
    assert legacy_policy.to_dict() == expected
    legacy = HistoricalAShareInputs(
        store=qualified.store,
        snapshot_ids=qualified.snapshot_ids,
        rule_artifact_hashes=qualified.rule_artifact_hashes,
        policy=legacy_policy,
    )
    assert _source_binding_hash(legacy) == canonical_hash(
        {
            "snapshot_ids": sorted(legacy.snapshot_ids),
            "rule_artifact_hashes": sorted(legacy.rule_artifact_hashes),
            "policy": expected,
        }
    )
    assert _source_binding_hash(legacy) != _source_binding_hash(qualified)
    evidence = legacy.reopen_security(SYMBOL, CUTOFF)
    assert evidence is not None and "limit_diagnostics" not in evidence.to_dict()
    assert evidence.corporate_action_status == "none"
    assert qualified.policy.to_dict()["limit_basis"] == BASIS


@pytest.mark.parametrize(
    "reported",
    [[], [dict(ts_code=SYMBOL, trade_date="20250103", pre_close=4, up_limit=4.8, down_limit=3.2)]],
)
def test_reported_discrepancy_is_diagnostic_with_same_admission_engine_limits(
    qualified: HistoricalAShareInputs, tmp_path: Path, reported: list[dict[str, object]]
) -> None:
    source = _replace_api(qualified, "stk_limit", reported)
    admission = DynamicAShareAdmission(source).discover((SYMBOL,), CUTOFF)[0]
    session = source.session(SYMBOL, DAY)
    assert admission.execution_ready and session.execution_ready
    assert admission.evidence is not None and session.bar is not None and session.spec is not None
    assert admission.evidence.lower_limit == Decimal("3.600")
    assert admission.evidence.upper_limit == Decimal("4.400")
    assert admission.evidence.raw_price == Decimal(4)
    assert admission.evidence.turnover == Decimal(80000000)  # Prior day's amount.
    assert admission.evidence.raw_price_observed_at == datetime(2025, 1, 2, 7, tzinfo=UTC)
    assert admission.evidence.limit_diagnostics == session.limit_diagnostics
    assert session.limit_diagnostics is not None
    assert session.limit_diagnostics["comparison_status"] == "unresolved_reported_comparison"
    assert session.limit_diagnostics["strict_pit_accepted"] is False
    assert session.bar.previous_close == admission.evidence.raw_price
    assert admission.evidence.corporate_action_status == "modeled_normal_session_assumption"
    engine = HistoricalStreamingAccount(
        specs=(session.spec,),
        journal_path=tmp_path / "engine.jsonl",
        account_reference="qualified-test",
        account_reference_key=b"q" * 32,
    )
    try:
        result = engine.bootstrap_half_hs300(session.bar)
        assert result.positions[SYMBOL] > 0
    finally:
        engine.close()
    legacy = HistoricalAShareInputs(
        store=source.store,
        snapshot_ids=source.snapshot_ids,
        rule_artifact_hashes=source.rule_artifact_hashes,
        policy=replace(source.policy, limit_basis="reported_stk_limit"),
    )
    assert not legacy.session(SYMBOL, DAY).execution_ready


@pytest.mark.parametrize(
    ("api", "field", "value", "gap"),
    [
        ("fund_daily", "pre_close", 4.1, "qualified_limit_prior_close_pre_close_mismatch"),
        ("fund_daily", "pre_close", 4.0001, "qualified_limit_session_pre_close_invalid"),
        ("fund_adj", "adj_factor", 1.1, "qualified_limit_factor_discontinuity"),
        ("fund_adj", "adj_factor", None, "qualified_limit_factor_coverage_invalid"),
    ],
)
def test_qualification_failure_blocks_both_views(
    qualified: HistoricalAShareInputs, api: str, field: str, value: object, gap: str
) -> None:
    rows = [dict(row) for row, _ in qualified._rows(api, SYMBOL)]
    for row in rows:
        if row["trade_date"] == "20250103":
            row[field] = value
    source = _replace_api(qualified, api, rows)
    admission = DynamicAShareAdmission(source).discover((SYMBOL,), CUTOFF)[0]
    session = source.session(SYMBOL, DAY)
    assert gap in admission.gaps and gap in session.gaps
    assert not admission.execution_ready and not session.execution_ready


def test_ca_exclusion_halts_and_unsupported_identity_stay_fail_closed(
    qualified: HistoricalAShareInputs,
) -> None:
    source = _replace_api(
        qualified,
        "fund_div",
        [
            dict(
                ts_code=SYMBOL,
                ann_date="20241230",
                div_proc="实施",
                record_date="20250102",
                ex_date="20250103",
                pay_date="20250103",
                div_cash=0.1,
            )
        ],
    )
    assert "qualified_limit_corporate_action_reference_excluded" in source.session(SYMBOL, DAY).gaps
    assert not DynamicAShareAdmission(source).discover((SYMBOL,), CUTOFF)[0].execution_ready
    no_halts = _replace_api(qualified, "suspend_d", [])
    assert "halt_status_unverified" in no_halts.session(SYMBOL, DAY).gaps
    assert not DynamicAShareAdmission(no_halts).discover((SYMBOL,), CUTOFF)[0].execution_ready
    stock = qualified.session("000001.SZ", DAY)
    assert "qualified_limit_instrument_or_regime_unsupported" in stock.gaps
    assert not stock.execution_ready


@pytest.mark.parametrize("missing", ["rule_qualification", "prior_daily"])
def test_missing_qualified_reference_evidence_never_becomes_admissible(
    qualified: HistoricalAShareInputs, missing: str
) -> None:
    if missing == "prior_daily":
        source = _replace_api(
            qualified,
            "fund_daily",
            [
                dict(row)
                for row, _ in qualified._rows("fund_daily", SYMBOL)
                if row["trade_date"] != "20250102"
            ],
        )
        expected = "qualified_limit_prior_raw_close_invalid"
    else:
        rule = cast(
            dict[str, Any], qualified.store.artifacts.read_json(qualified.rule_artifact_hashes[0])
        )
        del rule["qualified_limit_reference"]
        source = HistoricalAShareInputs(
            store=qualified.store,
            snapshot_ids=qualified.snapshot_ids,
            rule_artifact_hashes=(qualified.store.artifacts.put_json(rule).content_hash,),
            policy=qualified.policy,
        )
        expected = "qualified_limit_identity_and_normal_session_basis_missing"
    admission = DynamicAShareAdmission(source).discover((SYMBOL,), CUTOFF)[0]
    session = source.session(SYMBOL, DAY)
    assert expected in admission.gaps and expected in session.gaps
    assert not admission.execution_ready and not session.execution_ready
