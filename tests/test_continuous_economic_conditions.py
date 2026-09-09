# pyright: reportPrivateUsage=false
from dataclasses import replace
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import cast

import pytest

from market_impact_agent.continuous_economic_conditions import (
    compare_reopened_continuous_accounts,
    reopen_common_economic_conditions,
)
from market_impact_agent.continuous_metrics import (
    compare_continuous_accounts,
    measure_continuous_account,
)
from market_impact_agent.streaming_nautilus_account import (
    HistoricalCorporateAction,
    HistoricalStreamingAccount,
)

from .test_historical_ashare_inputs import _source


def test_reopened_pair_ignores_strategy_identity_but_verifies_real_sources(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = _source(tmp_path / "source")
    seed = source.session("510300.SH", date(2025, 1, 2))
    day = source.session("510300.SH", date(2025, 1, 3))
    assert seed.spec is not None and seed.bar is not None and day.bar is not None
    accounts: list[HistoricalStreamingAccount] = []
    try:
        for identity in ("reviewed", "control"):
            account = HistoricalStreamingAccount(
                specs=(seed.spec,),
                journal_path=tmp_path / f"{identity}.jsonl",
                account_reference=identity,
                account_reference_key=b"a" * 32,
                initial_cash=Decimal(100000),
            )
            accounts.append(account)
            account.bootstrap_half_hs300(seed.bar)
            account.advance_session({"510300.SH": day.bar}, corporate_actions=day.corporate_actions)
        metrics = [
            measure_continuous_account(
                initial_nav=account.results[0].nav,
                sessions=account.results[1:],
                expected_sessions=1,
                execution_policy_hash=str(index) * 64,
                initial_account_hash=str(index + 2) * 64,
                model_cost_microusd=0,
            )
            for index, account in enumerate(accounts)
        ]
        conditions = reopen_common_economic_conditions(
            account=accounts[0], historical_inputs=source, sessions=(date(2025, 1, 3),)
        )
        for metric in metrics:
            metric["common_economic_conditions"] = conditions
        with pytest.raises(ValueError, match="same account"):
            compare_continuous_accounts(*metrics)
        before = [account.journal_path.read_bytes() for account in accounts]

        control_source = source

        def compare():
            return compare_reopened_continuous_accounts(
                *metrics,
                reviewed_account=accounts[0],
                control_account=accounts[1],
                reviewed_inputs=source,
                control_inputs=control_source,
                sessions=(date(2025, 1, 3),),
                symbols=("510300.SH", "510500.SH"),
            )

        assert Decimal(str(compare()["performance_difference"])) == 0
        assert before == [account.journal_path.read_bytes() for account in accounts]
        assert source.session("510500.SH", date(2025, 1, 3)).execution_ready is False
        control_source = source.with_snapshots(())
        original_control_session = control_source.session

        def changed_optional_source(symbol: str, session: date):
            value = original_control_session(symbol, session)
            return (
                replace(value, gaps=(*value.gaps, "different_missingness"))
                if symbol == "510500.SH"
                else value
            )

        monkeypatch.setattr(control_source, "session", changed_optional_source)
        with pytest.raises(ValueError, match="same reopened economic"):
            compare()

        def missing_required_source(symbol: str, session: date):
            return replace(original_control_session(symbol, session), bar=None)

        monkeypatch.setattr(control_source, "session", missing_required_source)
        with pytest.raises(ValueError, match="complete reopened source inputs"):
            compare()
        control_source = source
        original = metrics[0]["net_return"]
        metrics[0]["net_return"] = "1"
        with pytest.raises(ValueError, match="measurement differs"):
            compare()
        metrics[0]["net_return"] = original
        metrics[0]["complete"] = False
        assert compare()["performance_difference"] is None
        metrics[0]["complete"] = True
        accounts[0].specs["510300.SH"] = replace(seed.spec, minimum_commission=Decimal(999))
        with pytest.raises(ValueError, match="fees/spec"):
            compare()
        accounts[0].specs["510300.SH"] = seed.spec
        original_session = source.session
        action = HistoricalCorporateAction(
            action_id="changed-source-action",
            target_id="510300.SH",
            kind="cash_dividend",
            effective_at=day.bar.session_close_at,
            source_ref="fixture:changed-action",
            cash_per_share=Decimal("0.1"),
        )

        def changed_session(symbol: str, session: date):
            value = original_session(symbol, session)
            return (
                replace(value, corporate_actions=(action,))
                if session == date(2025, 1, 3)
                else value
            )

        monkeypatch.setattr(source, "session", changed_session)
        with pytest.raises(ValueError, match="corporate actions"):
            compare()
        monkeypatch.setattr(source, "session", original_session)
        economic = cast(dict[str, object], conditions["economics"])
        assert economic["sessions"] == ["2025-01-03"]
        with pytest.raises(ValueError, match="calendar"):
            reopen_common_economic_conditions(
                account=accounts[0], historical_inputs=source, sessions=(date(2025, 1, 6),)
            )
    finally:
        for account in accounts:
            account.close()


def test_equal_nav_with_different_real_seed_positions_cannot_compare(tmp_path: Path) -> None:
    source = _source(tmp_path / "source")
    seed = source.session("510300.SH", date(2025, 1, 2))
    day = source.session("510300.SH", date(2025, 1, 3))
    assert seed.spec is not None and seed.bar is not None and day.bar is not None
    accounts: list[HistoricalStreamingAccount] = []
    try:
        for index in range(2):
            account = HistoricalStreamingAccount(
                specs=(seed.spec,),
                journal_path=tmp_path / f"seed-{index}.jsonl",
                account_reference=f"seed-{index}",
                account_reference_key=b"a" * 32,
                initial_cash=Decimal(100000) if index == 0 else accounts[0].results[0].nav,
            )
            accounts.append(account)
            if index == 0:
                account.bootstrap_half_hs300(seed.bar)
            else:
                account.advance_session({"510300.SH": seed.bar})
            account.advance_session({"510300.SH": day.bar}, corporate_actions=day.corporate_actions)
        assert accounts[0].results[0].nav == accounts[1].results[0].nav
        metrics = [
            measure_continuous_account(
                initial_nav=account.results[0].nav,
                sessions=account.results[1:],
                expected_sessions=1,
                execution_policy_hash="a" * 64,
                initial_account_hash="b" * 64,
                model_cost_microusd=0,
            )
            for account in accounts
        ]
        with pytest.raises(ValueError, match="same reopened economic"):
            compare_reopened_continuous_accounts(
                *metrics,
                reviewed_account=accounts[0],
                control_account=accounts[1],
                reviewed_inputs=source,
                control_inputs=source,
                sessions=(date(2025, 1, 3),),
            )
    finally:
        for account in accounts:
            account.close()
