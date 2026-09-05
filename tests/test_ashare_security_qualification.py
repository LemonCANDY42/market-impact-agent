# pyright: reportPrivateUsage=false
from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import cast

import pytest

from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.ashare_security_qualification import (
    _SOURCE_URLS,
    SourceBackedAShareRulePolicy,
    accept_ashare_rule_policy,
    qualify_ashare_security,
)
from market_impact_agent.data_inputs import (
    DataInputHarness,
    DataQuery,
    DataQueryMode,
    LocalDataSnapshotStore,
)
from market_impact_agent.historical_ashare_inputs import (
    HistoricalAShareInputs,
    ModeledHistoricalPolicy,
)
from market_impact_agent.prospective_ashare_inputs import ProspectiveAShareInputs
from market_impact_agent.runtime_store import RunJournal
from market_impact_agent.tushare_observation import (
    TushareObservationProvider,
    load_tushare_observation_source,
)
from tests.test_tushare_observation import TOKEN, FakeTransport, _query, _response

CUTOFF = datetime(2026, 8, 31, 1, 31, tzinfo=UTC)


def accepted_policy(store: LocalDataSnapshotStore, at: datetime) -> SourceBackedAShareRulePolicy:
    """Replace external document I/O only; retain real Harness signing and CAS."""
    journal = RunJournal.authoritative(store)
    run_id = "synthetic-source-acceptance"
    journal.start_run(run_id=run_id, config_hash=canonical_hash(run_id), created_at=at)
    ids: list[str] = []
    for url in sorted(_SOURCE_URLS):
        digest = store.put_raw(("SYNTHETIC external document fixture: " + url).encode())
        event_id = "synthetic-source-" + canonical_hash(url)
        journal.append(
            run_id=run_id,
            event_id=event_id,
            event_type="research.public.received",
            observed_at=at,
            payload={
                "url": url,
                "raw_hash": digest,
                "retrieved_at": at.isoformat(),
                "http_status": 200,
                "content_type": "text/plain",
            },
        )
        ids.append(event_id)
    return accept_ashare_rule_policy(
        store=store,
        run_id=run_id,
        source_receipt_event_ids=tuple(ids),
        effective_from=datetime(2026, 7, 5, 16, tzinfo=UTC),
        effective_until=None,
        accepted_at=at,
    )


def capture_rows(
    store: LocalDataSnapshotStore,
    api: str,
    params: dict[str, object],
    rows: list[dict[str, object]],
    received_at: datetime,
) -> str:
    config = load_tushare_observation_source(
        Path(f"examples/providers/tushare-observation-{api.replace('_', '-')}-v1.json")
    )
    response = _response(
        config.fields, [[row.get(field) for field in config.fields] for row in rows]
    )
    provider = TushareObservationProvider(
        TOKEN, (config,), transport=FakeTransport([response]), clock=lambda: received_at
    )
    harness = DataInputHarness(store)
    harness.register(provider)
    base = _query(provider, config, params)
    query = DataQuery.build(
        capability=base.capability,
        pit_lane=base.pit_lane,
        as_of=received_at,
        window_start=None,
        source_policy_id=base.source_policy_id,
        parameters=base.parameters,
        sources=base.sources,
        minimum_data_sources=1,
    )
    snapshot = asyncio.run(harness.execute(query, mode=DataQueryMode.FETCH_IF_MISSING))
    assert all(
        attempt.status.completed and attempt.accepted_count == attempt.received_count
        for attempt in snapshot.attempts
    )
    return snapshot.snapshot_id


def captured_inputs(
    tmp_path: Path, *, symbol: str = "600519.SH", etf: bool = False, cutoff: datetime = CUTOFF
) -> ProspectiveAShareInputs:
    """Non-seed acquired identity/price source fixtures, with no per-symbol rule injection."""
    store = LocalDataSnapshotStore(tmp_path / "source")
    policy = accepted_policy(store, cutoff - timedelta(days=1))
    received = cutoff - timedelta(seconds=30)
    exchange = "SSE" if symbol.endswith(".SH") else "SZSE"
    ids = [
        capture_rows(
            store,
            "etf_basic" if etf else "stock_basic",
            {"ts_code": symbol},
            [
                {
                    "ts_code": symbol,
                    "symbol": symbol[:6],
                    "name": "Synthetic Mainboard",
                    "csname": "Synthetic ETF",
                    "exchange": exchange,
                    "list_date": "20100101",
                    "list_status": "L",
                    "etf_type": "境内",
                    "index_code": "000300.SH",
                }
            ],
            received,
        )
    ]
    if etf:
        ids.append(
            capture_rows(
                store,
                "fund_basic",
                {"ts_code": symbol},
                [
                    {
                        "ts_code": symbol,
                        "fund_type": "股票型",
                        "market": "E",
                        "status": "L",
                        "list_date": "20100101",
                    }
                ],
                received,
            )
        )
    ids.append(
        capture_rows(
            store,
            "fund_daily" if etf else "daily",
            {"ts_code": symbol, "start_date": "20260828", "end_date": "20260828"},
            [
                {
                    "ts_code": symbol,
                    "trade_date": "20260828",
                    "close": "10",
                    "pre_close": "10",
                    "open": "10",
                    "high": "10",
                    "low": "10",
                    "vol": "100000",
                    "amount": "100000",
                }
            ],
            received,
        )
    )
    ids.append(
        capture_rows(
            store,
            "trade_cal",
            {"exchange": exchange, "start_date": "20260820", "end_date": "20260831"},
            [
                {"exchange": exchange, "cal_date": day, "is_open": 1, "pretrade_date": previous}
                for day, previous in (
                    ("20260820", "20260819"),
                    ("20260821", "20260820"),
                    ("20260824", "20260821"),
                    ("20260825", "20260824"),
                    ("20260826", "20260825"),
                    ("20260827", "20260826"),
                    ("20260828", "20260827"),
                    ("20260831", "20260828"),
                )
            ],
            received,
        )
    )
    return ProspectiveAShareInputs(
        store=store, snapshot_ids=tuple(ids), qualification_policy=policy
    )


@pytest.mark.parametrize(
    ("symbol", "etf", "tick"), [("600519.SH", False, "0.01"), ("159919.SZ", True, "0.001")]
)
def test_acquired_nonseed_static_qualification_and_restart(
    tmp_path: Path, symbol: str, etf: bool, tick: str
) -> None:
    inputs = captured_inputs(tmp_path, symbol=symbol, etf=etf)
    first = inputs.qualification(symbol, CUTOFF)
    assert first.qualified and first.spec is not None
    assert first.spec.lot_size == 100 and first.spec.price_increment == Decimal(tick)
    restarted_store = LocalDataSnapshotStore(inputs.store.root)
    reopened = SourceBackedAShareRulePolicy.from_accepted_event(
        restarted_store, inputs.qualification_policy.acceptance_event_id
    )
    restarted = ProspectiveAShareInputs(
        store=restarted_store, snapshot_ids=inputs.snapshot_ids, qualification_policy=reopened
    )
    assert restarted.qualification(symbol, CUTOFF) == first
    assert (
        restarted.qualification(symbol, CUTOFF + timedelta(minutes=1)).rule_artifact_hash
        == first.rule_artifact_hash
    )
    evidence = restarted.reopen_security(symbol, CUTOFF)
    assert evidence.raw_price == Decimal("10")
    assert evidence.raw_price_observed_at == datetime(2026, 8, 28, 7, tzinfo=UTC)
    assert "fresh_intraday_quote_missing" in evidence.gaps
    assert (
        evidence.limit_diagnostics is not None
        and evidence.limit_diagnostics["static_qualification_ready"] is True
    )


def test_policy_forgery_future_receipt_and_unsupported_board_fail_closed(tmp_path: Path) -> None:
    inputs = captured_inputs(tmp_path, symbol="300750.SZ")
    assert "equity_board_unaccepted" in inputs.qualification("300750.SZ", CUTOFF).gaps
    forged = replace(inputs.qualification_policy, effective_from=datetime(2025, 1, 1, tzinfo=UTC))
    assert (
        "generic_rule_source_authority_unverified"
        in qualify_ashare_security(inputs, "300750.SZ", CUTOFF, forged).gaps
    )
    before = inputs.qualification("300750.SZ", CUTOFF - timedelta(minutes=1))
    assert "security_identity_received_after_cutoff" in before.gaps
    with pytest.raises(PermissionError, match="root-authenticated"):
        RunJournal.authoritative(inputs.store).append(
            run_id="synthetic-source-acceptance",
            event_id="forged-policy",
            event_type="ashare.rule_policy.accepted",
            observed_at=CUTOFF,
            payload={"policy_artifact_hash": forged.policy_artifact_hash},
        )


def test_historical_opt_in_preserves_actual_receipt_and_rejects_class_backfill(
    tmp_path: Path,
) -> None:
    inputs = captured_inputs(tmp_path, symbol="159919.SZ", etf=True)
    history = HistoricalAShareInputs(
        store=inputs.store,
        snapshot_ids=inputs.snapshot_ids,
        rule_artifact_hashes=(),
        qualification_policy=inputs.qualification_policy,
        policy=ModeledHistoricalPolicy(
            "dynamic-source-expansion-2026-v1",
            Decimal("0.01"),
            research_projection="dynamic_ashare_sources_v1",
        ),
    )
    assert history.instrument_spec("159919.SZ", CUTOFF) is not None
    prior = qualify_ashare_security(
        history,
        "159919.SZ",
        CUTOFF - timedelta(days=1),
        inputs.qualification_policy,
        historical=True,
    )
    assert "historical_regime_not_point_in_time" in prior.gaps
    snapshot = inputs.store.get(inputs.snapshot_ids[0])
    projection = history.research_projection(snapshot, "etf_basic", CUTOFF)
    assert projection["rows"] and "current_classification_not_historical_regime_authority" in cast(
        list[str], projection["gaps"]
    )
    assert "etf_type" not in str(projection["rows"])
    assert (
        history.research_query_gap("rt_etf_min", {"ts_code": "159919.SZ", "freq": "1MIN"}, CUTOFF)
        == "historical_source_not_projectable"
    )


def test_changed_source_bytes_invalidate_existing_policy(tmp_path: Path) -> None:
    inputs = captured_inputs(tmp_path)
    source = inputs.store.artifacts.get(
        inputs.qualification_policy.source_artifact_hashes[0], media_type="application/octet-stream"
    )
    source.path.write_bytes(b"changed")
    assert (
        "generic_rule_source_authority_unverified" in inputs.qualification("600519.SH", CUTOFF).gaps
    )


def _captured_historical_session_inputs(tmp_path: Path, cutoff: datetime) -> HistoricalAShareInputs:
    inputs = captured_inputs(tmp_path, cutoff=cutoff)
    store = inputs.store
    symbol = "600519.SH"
    added = [
        capture_rows(
            store,
            "daily",
            {"ts_code": symbol, "start_date": "20260831", "end_date": "20260831"},
            [
                {
                    "ts_code": symbol,
                    "trade_date": "20260831",
                    "pre_close": "10",
                    "open": "10",
                    "high": "10",
                    "low": "10",
                    "close": "10",
                    "vol": "100000",
                    "amount": "100000",
                }
            ],
            cutoff,
        ),
        capture_rows(
            store,
            "stk_limit",
            {"ts_code": symbol, "trade_date": "20260831"},
            [
                {
                    "ts_code": symbol,
                    "trade_date": "20260831",
                    "pre_close": "10",
                    "up_limit": "11",
                    "down_limit": "9",
                }
            ],
            cutoff,
        ),
        capture_rows(
            store,
            "suspend_d",
            {"ts_code": symbol, "start_date": "20260831", "end_date": "20260831"},
            [],
            cutoff,
        ),
        capture_rows(
            store,
            "adj_factor",
            {"ts_code": symbol, "start_date": "20260828", "end_date": "20260831"},
            [
                {"ts_code": symbol, "trade_date": day, "adj_factor": "1"}
                for day in ("20260828", "20260831")
            ],
            cutoff,
        ),
        capture_rows(store, "dividend", {"ts_code": symbol}, [], cutoff),
    ]
    history = HistoricalAShareInputs(
        store=store,
        snapshot_ids=inputs.snapshot_ids + tuple(added),
        rule_artifact_hashes=(),
        qualification_policy=inputs.qualification_policy,
        policy=ModeledHistoricalPolicy(
            "dynamic-source-expansion-2026-v1",
            Decimal("0.01"),
            research_projection="dynamic_ashare_sources_v1",
        ),
    )
    return history


def test_nonseed_historical_source_session_and_action_boundary(tmp_path: Path) -> None:
    cutoff = CUTOFF - timedelta(minutes=6)
    history = _captured_historical_session_inputs(tmp_path, cutoff)
    store = history.store
    symbol = "600519.SH"
    session = history.session(symbol, cutoff.date())
    assert session.execution_ready, session.gaps
    preopen = history.reopen_security(symbol, cutoff)
    assert preopen is not None and not preopen.gaps
    assert (
        history.research_query_gap(
            "stk_limit", {"ts_code": symbol, "trade_date": "20260831"}, cutoff
        )
        is None
    )
    # Same acquired security and rule, but unsupported stock dividend settlement.
    action = capture_rows(
        store,
        "dividend",
        {"ts_code": symbol},
        [
            {
                "ts_code": symbol,
                "ann_date": "20260828",
                "end_date": "20260630",
                "div_proc": "实施",
                "record_date": "20260828",
                "ex_date": "20260831",
                "pay_date": "20260831",
                "cash_div": "1",
            }
        ],
        cutoff + timedelta(seconds=1),
    )
    unsafe = history.with_snapshots((action,)).session(symbol, cutoff.date())
    assert "corporate_action_settlement_unaccepted" in unsafe.gaps
    assert not unsafe.execution_ready


def test_reference_mark_replay_ignores_later_identical_receipts(tmp_path: Path) -> None:
    inputs = captured_inputs(tmp_path)
    symbol = "600519.SH"
    original = inputs.reopen_security(symbol, CUTOFF)
    refetched: list[str] = []
    for snapshot_id in inputs.snapshot_ids[-2:]:
        snapshot = inputs.store.get(snapshot_id)
        rows = [
            cast(dict[str, object], item.normalized_payload["record"])
            for item in snapshot.observations
        ]
        api = str(snapshot.observations[0].normalized_payload["api_name"])
        refetched.append(
            capture_rows(
                inputs.store, api, snapshot.query.parameters, rows, CUTOFF + timedelta(minutes=1)
            )
        )
    for ids in (
        (*inputs.snapshot_ids, *refetched),
        (*reversed(refetched), *reversed(inputs.snapshot_ids)),
    ):
        rebound = ProspectiveAShareInputs(
            store=inputs.store, snapshot_ids=ids, qualification_policy=inputs.qualification_policy
        )
        assert rebound.reopen_security(symbol, CUTOFF) == original
        later = rebound.reopen_security(symbol, CUTOFF + timedelta(minutes=2))
        assert later.raw_price == original.raw_price
        assert later.raw_price_observed_at == original.raw_price_observed_at
        assert later.gaps == original.gaps


def test_historical_generic_identity_uses_qualified_visible_revision(tmp_path: Path) -> None:
    cutoff = CUTOFF - timedelta(minutes=6)
    history = _captured_historical_session_inputs(tmp_path, cutoff)
    symbol = "600519.SH"
    original = history.reopen_security(symbol, cutoff)
    updated_at = cutoff + timedelta(minutes=1)
    updated_row: dict[str, object] = {
        "ts_code": symbol,
        "symbol": symbol[:6],
        "name": "Synthetic Renamed",
        "exchange": "SSE",
        "list_date": "20100101",
        "list_status": "L",
    }
    updated_id = capture_rows(
        history.store, "stock_basic", {"ts_code": symbol}, [updated_row], updated_at
    )
    updated_hash = history.store.get(updated_id).observations[0].raw_content_hash
    updated = history.with_snapshots((updated_id,))
    assert updated.reopen_security(symbol, cutoff) == original
    current = updated.reopen_security(symbol, cutoff + timedelta(minutes=2))
    assert current is not None and not current.gaps
    assert updated_hash in current.source_record_hashes
    assert updated.session(symbol, cutoff.date()).execution_ready
    conflict_id = capture_rows(
        history.store,
        "stock_basic",
        {"ts_code": symbol, "exchange": "SSE"},
        [{**updated_row, "name": "Synthetic Conflicting"}],
        updated_at,
    )
    conflicting = updated.with_snapshots((conflict_id,))
    assert conflicting.reopen_security(symbol, cutoff) == original
    assert conflicting.reopen_security(symbol, cutoff + timedelta(minutes=2)) is None
    assert not conflicting.session(symbol, cutoff.date()).execution_ready
