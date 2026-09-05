# pyright: reportPrivateUsage=false, reportUnusedImport=false
"""Record-date entitlement cannot be acquired by buying after account inception."""

from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any, cast

import pytest

from market_impact_agent.continuous_baselines import (
    ContinuousBaselineAccountSeed,
    ContinuousBaselineWindow,
    evaluate_continuous_baseline_window,
)
from market_impact_agent.continuous_study import ContinuousStudyWindow, WindowKind
from market_impact_agent.dynamic_ashare_admission import DynamicAShareAdmission
from market_impact_agent.historical_ashare_inputs import (
    CASH_ONLY_INCEPTION_BASIS,
    HistoricalAShareInputs,
)
from market_impact_agent.streaming_nautilus_account import HistoricalStreamingAccount
from tests.test_historical_ashare_inputs import _capture
from tests.test_qualified_etf_limits import SYMBOL, _replace_api, qualified  # noqa: F401

INCEPTION = datetime(2018, 1, 24, 1, 30, tzinfo=UTC)


@pytest.fixture
def distribution(qualified: HistoricalAShareInputs) -> HistoricalAShareInputs:  # noqa: F811
    source = qualified
    rule = cast(dict[str, Any], source.store.artifacts.read_json(source.rule_artifact_hashes[0]))
    rule["effective_from"] = "2010-01-01T00:00:00+00:00"
    source = HistoricalAShareInputs(
        store=source.store,
        snapshot_ids=source.snapshot_ids,
        rule_artifact_hashes=(source.store.artifacts.put_json(rule).content_hash,),
        policy=replace(
            source.policy, limit_basis=CASH_ONLY_INCEPTION_BASIS, cash_only_inception_at=INCEPTION
        ),
    )
    for api in ("fund_daily", "fund_adj", "suspend_d"):
        original = dict(source._rows(api, SYMBOL)[0][0])
        rows: list[dict[str, object]] = [
            {**original, "trade_date": f"201801{day}"} for day in range(22, 27)
        ]
        source = _replace_api(source, api, rows)
    ids = tuple(
        s
        for s in source.snapshot_ids
        if source.store.get(s).query.sources[0].upstream_source != "tushare-trade-cal"
    )
    calendar = _capture(
        source.store,
        "trade_cal",
        {"exchange": "SSE", "start_date": "20180122", "end_date": "20180126"},
        [
            dict(
                exchange="SSE", cal_date=f"201801{day}", is_open=1, pretrade_date=f"201801{day - 1}"
            )
            for day in range(22, 27)
        ],
    )
    source = HistoricalAShareInputs(
        store=source.store,
        snapshot_ids=(*ids, calendar),
        rule_artifact_hashes=source.rule_artifact_hashes,
        policy=source.policy,
    )
    return _replace_api(
        source,
        "fund_div",
        [
            dict(
                ts_code=SYMBOL,
                ann_date="20180119",
                div_proc="实施",
                record_date="20180122",
                ex_date="20180123",
                pay_date="20180126",
                div_cash=0.1,
            )
        ],
    )


def test_post_record_buy_has_zero_payment_and_identical_replay(
    distribution: HistoricalAShareInputs, tmp_path: Path
) -> None:
    source = distribution
    sessions = [source.session(SYMBOL, date(2018, 1, day)) for day in (24, 25, 26)]
    for day, session in zip((24, 25, 26), sessions, strict=True):
        admission = DynamicAShareAdmission(source).discover(
            (SYMBOL,), datetime(2018, 1, day, 1, 25, tzinfo=UTC)
        )[0]
        assert session.execution_ready, session.gaps
        assert admission.execution_ready, admission.gaps
        assert admission.evidence is not None
        assert admission.evidence.limit_diagnostics == session.limit_diagnostics
    first, interim, payment = sessions
    assert first.spec is not None and first.bar is not None
    assert interim.bar is not None and payment.bar is not None
    assert not first.corporate_actions and not interim.corporate_actions
    assert len(payment.corporate_actions) == 1
    action = payment.corporate_actions[0]
    assert action.entitlement_at == datetime(2018, 1, 22, 7, tzinfo=UTC)
    assert action.effective_at == payment.bar.session_open_at

    def engine(inception: datetime | None = INCEPTION) -> HistoricalStreamingAccount:
        assert first.spec is not None
        return HistoricalStreamingAccount(
            specs=(first.spec,),
            journal_path=tmp_path / "account.jsonl",
            account_reference="inception-test",
            account_reference_key=b"i" * 32,
            cash_only_inception_at=inception,
        )

    account = engine()
    try:
        seeded = account.bootstrap_half_hs300(first.bar)
        assert seeded.positions[SYMBOL] > 0
        account.advance_session({SYMBOL: interim.bar})
        paid = account.advance_session(
            {SYMBOL: payment.bar}, corporate_actions=payment.corporate_actions
        )
        assert paid.cash == seeded.cash
        expected = [result.result_hash for result in account.results]
    finally:
        account.close()
    restored = engine()
    try:
        assert [result.result_hash for result in restored.results] == expected
    finally:
        restored.close()
    for changed in (None, INCEPTION - timedelta(days=1), INCEPTION + timedelta(days=1)):
        with pytest.raises(ValueError, match="replay configuration differs"):
            engine(changed)
    ex = source.session(SYMBOL, date(2018, 1, 23))
    assert "qualified_limit_corporate_action_reference_excluded" in ex.gaps
    assert not ex.execution_ready
    legacy = HistoricalAShareInputs(
        store=source.store,
        snapshot_ids=source.snapshot_ids,
        rule_artifact_hashes=source.rule_artifact_hashes,
        policy=replace(
            source.policy,
            limit_basis="qualified_seed_etf_exchange_rule_v1",
            cash_only_inception_at=None,
        ),
    )
    assert not legacy.session(SYMBOL, date(2018, 1, 24)).execution_ready


@pytest.mark.parametrize("record", [None, "20180124", "20180125"])
def test_unknown_or_not_preinception_record_blocks_source(
    distribution: HistoricalAShareInputs, record: str | None
) -> None:
    row = dict(distribution._rows("fund_div", SYMBOL)[0][0], record_date=record, ex_date="20180125")
    snapshot = _capture(
        distribution.store, "fund_div", {"ts_code": SYMBOL, "ann_date": "20180119"}, [row]
    )
    ids = tuple(
        s
        for s in distribution.snapshot_ids
        if distribution.store.get(s).query.sources[0].upstream_source != "tushare-fund-div"
    )
    source = HistoricalAShareInputs(
        store=distribution.store,
        snapshot_ids=(*ids, snapshot),
        rule_artifact_hashes=distribution.rule_artifact_hashes,
        policy=distribution.policy,
    )
    assert not source.session(SYMBOL, date(2018, 1, 26)).execution_ready
    assert (
        not DynamicAShareAdmission(source)
        .discover((SYMBOL,), datetime(2018, 1, 26, 1, 25, tzinfo=UTC))[0]
        .execution_ready
    )


@pytest.mark.parametrize("offset", [None, -1, 0, 1])
def test_engine_rejects_missing_entitlement_at_or_after_inception(
    distribution: HistoricalAShareInputs, tmp_path: Path, offset: int | None
) -> None:
    seed = distribution.session(SYMBOL, date(2018, 1, 24))
    pay = distribution.session(SYMBOL, date(2018, 1, 26))
    assert seed.spec is not None and seed.bar is not None and pay.bar is not None
    account = HistoricalStreamingAccount(
        specs=(seed.spec,),
        journal_path=tmp_path / "engine.jsonl",
        account_reference="boundary",
        account_reference_key=b"b" * 32,
        cash_only_inception_at=INCEPTION,
    )
    try:
        before = replace(
            seed.bar,
            session_open_at=INCEPTION - timedelta(days=1),
            session_close_at=seed.bar.session_close_at - timedelta(days=1),
        )
        with pytest.raises(ValueError, match="precedes cash-only inception"):
            account.advance_session({SYMBOL: before})
        account.bootstrap_half_hs300(seed.bar)
        action = replace(
            pay.corporate_actions[0],
            entitlement_at=INCEPTION + timedelta(microseconds=offset)
            if offset is not None
            else None,
        )
        if offset is not None and offset < 0:
            assert (
                account.advance_session({SYMBOL: pay.bar}, corporate_actions=(action,)).cash
                == account.results[0].cash
            )
        else:
            with pytest.raises(ValueError, match="record-date"):
                account.advance_session({SYMBOL: pay.bar}, corporate_actions=(action,))
    finally:
        account.close()


@pytest.mark.parametrize(
    "baseline",
    [
        "cash_no_action",
        "same_initial_account_hold",
        "broad_etf_hold",
        "phase2_adjusted_close_momentum_510300",
    ],
)
def test_all_baselines_bind_registered_inception(
    distribution: HistoricalAShareInputs, tmp_path: Path, baseline: str
) -> None:
    window = ContinuousBaselineWindow(
        ContinuousStudyWindow(
            "inception",
            WindowKind.LEGACY_CASE,
            date(2018, 1, 25),
            date(2018, 1, 23),
            date(2018, 1, 26),
            "fixture",
            None,
            None,
            (),
        ),
        (date(2018, 1, 25), date(2018, 1, 26)),
    )
    with pytest.raises(ValueError, match="registered bootstrap open"):
        evaluate_continuous_baseline_window(
            registration_id="fixture",
            baseline_id=baseline,
            registered_window=window,
            historical_inputs=distribution,
            account_seed=ContinuousBaselineAccountSeed("fixture", b"b" * 32),
            state_root=tmp_path,
        )  # pyright: ignore[reportArgumentType]


@pytest.mark.parametrize("announcement", [None, "20180127"])
def test_undated_or_future_announcement_cannot_relax_qualified_limits(
    distribution: HistoricalAShareInputs,
    announcement: str | None,
) -> None:
    row = dict(distribution._rows("fund_div", SYMBOL)[0][0], ann_date=announcement)
    assert not distribution._zero_entitlement_distribution(row, date(2018, 1, 26))
    snapshot = _capture(
        distribution.store, "fund_div", {"ts_code": SYMBOL, "ann_date": "20180119"}, [row]
    )
    ids = tuple(
        s
        for s in distribution.snapshot_ids
        if distribution.store.get(s).query.sources[0].upstream_source != "tushare-fund-div"
    )
    source = HistoricalAShareInputs(
        store=distribution.store,
        snapshot_ids=(*ids, snapshot),
        rule_artifact_hashes=distribution.rule_artifact_hashes,
        policy=distribution.policy,
    )
    assert (
        "qualified_limit_corporate_action_reference_excluded"
        in source.session(SYMBOL, date(2018, 1, 26)).gaps
    )
    assert (
        not DynamicAShareAdmission(source)
        .discover((SYMBOL,), datetime(2018, 1, 26, 1, 25, tzinfo=UTC))[0]
        .execution_ready
    )
