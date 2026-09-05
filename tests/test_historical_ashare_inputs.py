# pyright: reportPrivateUsage=false
from __future__ import annotations

import asyncio
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import cast

import pytest

from market_impact_agent.data_inputs import DataInputHarness, DataQueryMode, LocalDataSnapshotStore
from market_impact_agent.domain import Side
from market_impact_agent.dynamic_ashare_admission import DynamicAShareAdmission
from market_impact_agent.historical_ashare_inputs import (
    HistoricalAShareInputs,
    ModeledHistoricalPolicy,
)
from market_impact_agent.streaming_nautilus_account import HistoricalStreamingAccount
from market_impact_agent.tushare_observation import (
    TushareObservationProvider,
    load_tushare_observation_source,
)
from tests.test_streaming_nautilus_account import intent
from tests.test_tushare_observation import RETRIEVED, TOKEN, FakeTransport, _query, _response

D = Decimal


def _capture(
    store: LocalDataSnapshotStore,
    api: str,
    params: dict[str, object],
    rows: list[dict[str, object]],
) -> str:
    config = load_tushare_observation_source(
        Path(f"examples/providers/tushare-observation-{api.replace('_', '-')}-v1.json")
    )
    response = _response(
        config.fields, [[row.get(field) for field in config.fields] for row in rows]
    )
    provider = TushareObservationProvider(
        TOKEN, (config,), transport=FakeTransport([response]), clock=lambda: RETRIEVED
    )
    harness = DataInputHarness(store)
    harness.register(provider)
    snapshot = asyncio.run(
        harness.execute(_query(provider, config, params), mode=DataQueryMode.FETCH_IF_MISSING)
    )
    assert all(attempt.status.completed for attempt in snapshot.attempts)
    return snapshot.snapshot_id


def _source(tmp_path: Path, *, etf_halt: bool = True) -> HistoricalAShareInputs:
    store = LocalDataSnapshotStore(tmp_path / "source")
    ids: list[str] = []
    rules: list[str] = []
    for symbol, exchange, price, etf in (
        ("510300.SH", "SSE", 4, True),
        ("000001.SZ", "SZSE", 10, False),
    ):
        api = "fund_daily" if etf else "daily"
        ids.append(
            _capture(
                store,
                api,
                {"ts_code": symbol, "start_date": "20250102", "end_date": "20250103"},
                [
                    dict(
                        ts_code=symbol,
                        trade_date=day,
                        pre_close=price,
                        open=price,
                        high=price if day == "20250102" else price * 1.05,
                        low=price,
                        close=price if day == "20250102" else price * 1.05,
                        change=0,
                        pct_chg=0,
                        vol=200000,
                        amount=80000 if day == "20250102" else 999999,
                    )
                    for day in ("20250102", "20250103")
                ],
            )
        )
        ids.append(
            _capture(
                store,
                "etf_basic" if etf else "stock_basic",
                {"ts_code": symbol},
                [
                    dict(
                        ts_code=symbol,
                        symbol=symbol[:6],
                        name="Synthetic",
                        csname="Synthetic",
                        list_date="20100101",
                        exchange=exchange,
                        list_status="L",
                    )
                ],
            )
        )
        ids.append(
            _capture(
                store,
                "trade_cal",
                {"exchange": exchange, "start_date": "20250102", "end_date": "20250103"},
                [
                    dict(exchange=exchange, cal_date=day, is_open=1, pretrade_date=previous)
                    for day, previous in (("20250102", "20241231"), ("20250103", "20250102"))
                ],
            )
        )
        ids.append(
            _capture(
                store,
                "suspend_d",
                {"ts_code": symbol, "start_date": "20250102", "end_date": "20250103"},
                [
                    dict(ts_code=symbol, trade_date=day, suspend_type="R", suspend_timing=None)
                    for day in ("20250102", "20250103")
                ]
                if etf and etf_halt
                else [],
            )
        )
        ids.append(
            _capture(
                store,
                "stk_limit",
                {"ts_code": symbol, "start_date": "20250102", "end_date": "20250103"},
                [
                    dict(
                        ts_code=symbol,
                        trade_date=day,
                        pre_close=price,
                        up_limit=price * 1.1,
                        down_limit=price * 0.9,
                    )
                    for day in ("20250102", "20250103")
                ],
            )
        )
        ids.append(
            _capture(
                store,
                "fund_adj" if etf else "adj_factor",
                {"ts_code": symbol, "start_date": "20241231", "end_date": "20250103"},
                [
                    dict(ts_code=symbol, trade_date=day, adj_factor=1)
                    for day in ("20241231", "20250102", "20250103")
                ],
            )
        )
        ids.append(_capture(store, "fund_div" if etf else "dividend", {"ts_code": symbol}, []))
        raw_hash = store.put_raw(b"Synthetic legally redistributable exchange rule fixture")
        rules.append(
            store.artifacts.put_json(
                dict(
                    schema_version="market-impact.historical-ashare-rule.v1",
                    symbol=symbol,
                    effective_from="2024-01-01T00:00:00+00:00",
                    effective_until="2026-01-01T00:00:00+00:00",
                    version="fixture-v1",
                    instrument_class="exchange_traded_fund" if etf else "equity",
                    source_artifact_hash=raw_hash,
                    source_url="https://example.test/exchange-rules",
                    lot_size=100,
                    price_increment="0.001" if etf else "0.01",
                    price_limit_ratio="0.1",
                    commission_rate="0.0003",
                    minimum_commission="5",
                    sell_stamp_tax_rate="0" if etf else "0.0005",
                )
            ).content_hash
        )
    return HistoricalAShareInputs(
        store=store,
        snapshot_ids=tuple(ids),
        rule_artifact_hashes=tuple(rules),
        policy=ModeledHistoricalPolicy("fixture-modeled-daily-open-v1", D("0.01")),
    )


@pytest.fixture(scope="module")
def source(tmp_path_factory: pytest.TempPathFactory) -> HistoricalAShareInputs:
    return _source(tmp_path_factory.mktemp("historical-source"))


def test_captured_source_positive_etf_seed_dynamic_stock_and_real_engine(
    tmp_path: Path, source: HistoricalAShareInputs
) -> None:
    admitted = DynamicAShareAdmission(source).discover(
        ("510300.SH", "000001.SZ"), datetime(2025, 1, 3, 1, 25, tzinfo=UTC)
    )
    assert all(item.execution_ready for item in admitted), [item.gaps for item in admitted]
    etf_evidence = next(item.evidence for item in admitted if item.symbol == "510300.SH")
    assert etf_evidence is not None
    assert etf_evidence.raw_price == D(4)
    assert etf_evidence.raw_price_observed_at == datetime(2025, 1, 2, 7, tzinfo=UTC)
    assert etf_evidence.turnover == D(80000000)
    assert etf_evidence.effective_until == datetime(2025, 1, 3, 1, 30, 0, 1, tzinfo=UTC)
    seed = source.session("510300.SH", date(2025, 1, 2))
    stock = source.session("000001.SZ", date(2025, 1, 3))
    etf3 = source.session("510300.SH", date(2025, 1, 3))
    assert seed.execution_ready and stock.execution_ready and etf3.execution_ready
    assert (
        seed.spec is not None
        and seed.bar is not None
        and stock.spec is not None
        and stock.bar is not None
        and etf3.bar is not None
    )
    account = HistoricalStreamingAccount(
        specs=(seed.spec,),
        journal_path=tmp_path / "engine.jsonl",
        account_reference="source-backed-test",
        account_reference_key=b"s" * 32,
    )
    account.bootstrap_half_hs300(seed.bar)
    account.register_instrument(stock.spec)
    result = account.advance_session(
        {"510300.SH": etf3.bar, "000001.SZ": stock.bar},
        intents=(intent(account, "source-stock", stock.bar, Side.BUY, "100", "000001.SZ"),),
    )
    assert result.positions["000001.SZ"] == D(100)
    assert result.cash == D(48980)
    account.close()
    assert all(source.store.get(s).completed_at.year == 2026 for s in source.snapshot_ids)
    assert source.policy.lane == "modeled_pit"


def test_missing_etf_halt_proof_and_effective_rule_remain_gaps(
    source: HistoricalAShareInputs,
) -> None:
    source = HistoricalAShareInputs(
        store=source.store,
        snapshot_ids=tuple(
            s
            for s in source.snapshot_ids
            if not (
                source.store.get(s).query.sources[0].upstream_source == "tushare-suspend-d"
                and source.store.get(s).query.parameters.get("ts_code") == "510300.SH"
            )
        ),
        rule_artifact_hashes=source.rule_artifact_hashes,
        policy=source.policy,
    )
    evidence = source.session("510300.SH", date(2025, 1, 2))
    assert "halt_status_unverified" in evidence.gaps and not evidence.execution_ready
    assert source.session("000001.SZ", date(2025, 1, 2)).execution_ready
    assert source.reopen_security("510300.SH", datetime(2023, 1, 2, 8, tzinfo=UTC)) is None
    with pytest.raises(ValueError, match="StrictPIT"):
        ModeledHistoricalPolicy("bad", D("0.01"), lane="strict_pit")


def test_raw_cas_tamper_and_unknown_source_are_not_authority(
    source: HistoricalAShareInputs,
) -> None:
    snapshot = source.store.get(source.snapshot_ids[0])
    raw = snapshot.observations[0].raw_content_hash
    path = source.store.artifacts.root / raw
    original = path.read_bytes()
    try:
        path.write_text("{}")
        with pytest.raises(ValueError, match="content does not match"):
            source.with_snapshots(()).reopen_security(
                "510300.SH", datetime(2025, 1, 3, 1, 25, tzinfo=UTC)
            )
    finally:
        path.write_bytes(original)


def test_source_cash_payment_uses_record_date_engine_inventory(
    source: HistoricalAShareInputs, tmp_path: Path
) -> None:
    payment: dict[str, object] = dict(
        ts_code="510300.SH",
        ann_date="20241230",
        imp_anndate="20241230",
        div_proc="实施",
        record_date="20250102",
        ex_date="20250103",
        pay_date="20250103",
        div_cash=0.1,
    )
    dividend = _capture(
        source.store,
        "fund_div",
        {"ts_code": "510300.SH"},
        [payment, {**payment, "net_ex_date": "20250104", "base_unit": 101}],
    )
    bound = source.with_snapshots((dividend,))
    first, second = (
        bound.session("510300.SH", date(2025, 1, 2)),
        bound.session("510300.SH", date(2025, 1, 3)),
    )
    assert first.execution_ready and second.execution_ready, second.gaps
    assert first.spec is not None and first.bar is not None and second.bar is not None
    assert len(second.corporate_actions) == 1
    assert second.corporate_actions[0].effective_at == second.bar.session_open_at
    assert second.corporate_actions[0].entitlement_at == first.bar.session_close_at
    engine = HistoricalStreamingAccount(
        specs=(first.spec,),
        journal_path=tmp_path / "dividend.jsonl",
        account_reference="dividend-test",
        account_reference_key=b"d" * 32,
    )
    seeded = engine.bootstrap_half_hs300(first.bar)
    paid = engine.advance_session(
        {"510300.SH": second.bar}, corporate_actions=second.corporate_actions
    )
    assert paid.cash - seeded.cash == D(1250)
    expected = paid.result_hash
    engine.close()
    recovered = HistoricalStreamingAccount(
        specs=(first.spec,),
        journal_path=tmp_path / "dividend.jsonl",
        account_reference="dividend-test",
        account_reference_key=b"d" * 32,
    )
    assert recovered.results[-1].result_hash == expected
    recovered.close()
    conflict = _capture(
        source.store,
        "fund_div",
        {"ts_code": "510300.SH", "ann_date": "20241230"},
        [{**payment, "div_cash": 0.2}],
    )
    disputed = bound.with_snapshots((conflict,)).session("510300.SH", date(2025, 1, 3))
    assert "corporate_action_conflicting_payment_effects" in disputed.gaps
    assert not disputed.corporate_actions


def test_frozen_source_verifies_once_under_concurrent_reads(
    source: HistoricalAShareInputs, monkeypatch: pytest.MonkeyPatch
) -> None:
    from concurrent.futures import ThreadPoolExecutor

    bound = source.with_snapshots(())
    original = bound._read_tables
    reads = 0

    def tracked():
        nonlocal reads
        reads += 1
        return original()

    monkeypatch.setattr(bound, "_read_tables", tracked)
    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = [pool.submit(bound.session, "510300.SH", date(2025, 1, 2)) for _ in range(4)]
        results = [future.result() for future in futures]
    assert reads == 1
    assert all(result.execution_ready for result in results)


@pytest.mark.parametrize(
    ("limit_prior", "upper", "expected_gap"),
    [
        (None, 4.4, None),
        (5, 4.4, "daily_limit_previous_close_mismatch"),
        (None, 4.41, "effective_rule_daily_limit_mismatch"),
    ],
)
def test_optional_limit_reference_preserves_raw_absence_and_checks_actual_bounds(
    tmp_path: Path,
    limit_prior: int | None,
    upper: float,
    expected_gap: str | None,
) -> None:
    source = _source(tmp_path)
    kept = tuple(
        sid
        for sid in source.snapshot_ids
        if not (
            source.store.get(sid).query.sources[0].upstream_source == "tushare-stk-limit"
            and source.store.get(sid).query.parameters.get("ts_code") == "510300.SH"
        )
    )
    limits_id = _capture(
        source.store,
        "stk_limit",
        {"ts_code": "510300.SH", "trade_date": "20250102"},
        [
            dict(
                ts_code="510300.SH",
                trade_date="20250102",
                pre_close=limit_prior,
                up_limit=upper,
                down_limit=3.6,
            )
        ],
    )
    bound = HistoricalAShareInputs(
        store=source.store,
        snapshot_ids=(*kept, limits_id),
        rule_artifact_hashes=source.rule_artifact_hashes,
        policy=source.policy,
    )
    session = bound.session("510300.SH", date(2025, 1, 2))
    assert session.bar is not None and session.bar.previous_close == D(4)
    if expected_gap is None:
        assert session.execution_ready
    else:
        assert session.gaps == (expected_gap,)
        assert not session.execution_ready
    observation = source.store.get(limits_id).observations[0]
    record = cast(dict[str, object], observation.normalized_payload["record"])
    assert record["pre_close"] == limit_prior
    raw = cast(dict[str, object], source.store.artifacts.read_json(observation.raw_content_hash))
    fields = cast(list[str], raw["fields"])
    values = cast(list[object], raw["values"])
    assert values[fields.index("pre_close")] == limit_prior


def test_index_reuses_hashes_preserves_duplicate_conflicts_and_is_immutable(
    source: HistoricalAShareInputs, monkeypatch: pytest.MonkeyPatch
) -> None:
    import market_impact_agent.historical_ashare_inputs as inputs

    calls = 0
    original = inputs.canonical_hash

    def tracked(value: object) -> str:
        nonlocal calls
        calls += 1
        return original(value)

    monkeypatch.setattr(inputs, "canonical_hash", tracked)
    bound = source.with_snapshots(())
    rows = bound._rows("fund_daily", "510300.SH")
    first_calls = calls
    assert first_calls > 0
    for _ in range(20):
        assert bound._rows("fund_daily", "510300.SH") is rows
        assert bound._one("fund_daily", "510300.SH", date(2025, 1, 2)) is not None
    assert calls == first_calls
    with pytest.raises(TypeError):
        cast(dict[str, object], rows[0][0])["close"] = 999
    daily = next(dict(row) for row, _ in rows if row["trade_date"] == "20250102")
    duplicate = _capture(
        source.store, "fund_daily", {"ts_code": "510300.SH", "trade_date": "20250102"}, [daily]
    )
    repeated = bound.with_snapshots((duplicate,))
    assert len(repeated._rows("fund_daily", "510300.SH")) == len(rows)
    assert calls > first_calls  # A fresh binding independently verifies and indexes.
    conflict = _capture(
        source.store,
        "fund_daily",
        {"ts_code": "510300.SH", "start_date": "20250102", "end_date": "20250102"},
        [{**daily, "close": 4.1}],
    )
    disputed = repeated.with_snapshots((conflict,))
    with pytest.raises(ValueError, match="conflicting captured historical rows"):
        disputed._one("fund_daily", "510300.SH", date(2025, 1, 2))
