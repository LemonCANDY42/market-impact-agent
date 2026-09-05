from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from hashlib import sha256
from pathlib import Path

import pytest

from market_impact_agent.account_state import AccountStateSnapshot, CashBalance
from market_impact_agent.authorized_decision_view import AuthorizedDecisionView
from market_impact_agent.data_inputs import FrozenDataSnapshotInput
from market_impact_agent.dynamic_ashare_admission import DynamicAShareAdmission
from market_impact_agent.portfolio_decision import (
    PortfolioExposureViewV2,
    RegisteredPortfolioExposureViewAuthorityV2,
)
from market_impact_agent.prospective_mock_composition import ProspectiveMockComposition
from market_impact_agent.prospective_mock_execution import open_prospective_mock_execution
from market_impact_agent.research_thesis_runtime import ResearchThesisRunInputs
from market_impact_agent.runtime_store import RunJournal

from .test_autonomous_paper import (
    AT,
    _fixture,  # pyright: ignore[reportPrivateUsage]
    _service,  # pyright: ignore[reportPrivateUsage]
    _set_equity_change,  # pyright: ignore[reportPrivateUsage]
)
from .test_prospective_ashare_quotes import CUTOFF, executable_inputs
from .test_research_thesis_runtime import _repository  # pyright: ignore[reportPrivateUsage]


def test_usd_risk_retains_exact_mandate_baseline_across_midnight(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path)
    service = _service(tmp_path, fixture)
    try:
        tomorrow = AT + timedelta(days=1)
        fixture.clock_box[0] = tomorrow
        _set_equity_change(
            fixture,
            change=Decimal(-100),
            observed_at=tomorrow,
            valid_until=tomorrow + timedelta(minutes=5),
        )
        risk = service._evaluate_current_risk(tomorrow, fail_closed=True)  # pyright: ignore[reportPrivateUsage]
        assert risk is not None and risk.daily_pnl == -100
        assert risk.strategy_peak_drawdown == 100
        assert "stale_risk_measurement" not in service.active_kill_reasons
        with fixture.store.authority_transaction() as connection:
            rows = connection.execute("SELECT mandate_hash FROM autonomous_risk_days").fetchall()
        assert [row["mandate_hash"] for row in rows] == [service.mandate_hash]
    finally:
        service.close()


def test_cny_renewal_and_fresh_rollover_preserve_peak_without_restart(tmp_path: Path) -> None:
    market = executable_inputs(tmp_path, symbol="510300.SH", etf=True)
    store = market.store
    frozen = FrozenDataSnapshotInput(frozenset(market.snapshot_ids))
    journal = RunJournal.authoritative(store)
    journal.start_run(run_id="risk", config_hash="a" * 64, created_at=CUTOFF)
    clock = [CUTOFF]
    composition = ProspectiveMockComposition(
        store=store,
        profile_id="risk",
        study_registration_id="risk",
        opening_authority_ref="risk",
        parent_run_id="risk",
        market_factory=lambda _: market,
        clock=lambda: clock[0],
    )
    research = ResearchThesisRunInputs(
        _repository("510300.SH", at=CUTOFF, event_id="risk"),
        "510300.SH",
        "epoch",
        frozenset({1}),
    )
    account, cutoff = composition.capture_context(research, frozen)
    security = DynamicAShareAdmission(market).discover(("510300.SH",), cutoff)[0]
    composition.portfolio_authority(research, frozen, account, security)
    service = open_prospective_mock_execution(composition)
    original_exposure = service.exposure_view_source()

    def observe(change: int, at: datetime) -> None:
        assert account.cash is not None
        fresh = AccountStateSnapshot.build(
            provider=service.provider.manifest,
            account_reference="simulated:" + composition.seed,
            account_reference_key=sha256(("synthetic-only:" + composition.seed).encode()).digest(),
            environment=account.environment,
            as_of=at,
            reconciled_at=at,
            reconciliation_reference="synthetic-risk-observation",
            cash=tuple(
                CashBalance(item.currency, item.available + change, item.settled + change)
                for item in account.cash
            ),
            positions=account.positions,
            open_orders=(),
            recent_fills=(),
            recent_fills_since=at - timedelta(days=1),
        )
        position = fresh.project_positions(evaluated_at=at, max_age=timedelta(minutes=5))
        view = AuthorizedDecisionView.build(
            cutoff=at,
            frozen_at=at,
            data_snapshot_ids=("synthetic-risk",),
            decision_input_ids=(),
            position_snapshot=position,
        )
        exposure = PortfolioExposureViewV2.build(
            authorized_view=view,
            position_snapshot=position,
            raw_mark_set_hash=original_exposure.raw_mark_set_hash,
            execution_ledger_snapshot_hash=original_exposure.execution_ledger_snapshot_hash,
            reconciliation_ledger_snapshot_hash=original_exposure.reconciliation_ledger_snapshot_hash,
            currency="CNY",
            marked_positions=original_exposure.marked_positions,
            daily_turnover_used=Decimal(0),
            daily_submissions_used=0,
            active_kill_reasons=(),
            observed_at=at,
            valid_until=at + timedelta(minutes=5),
        )
        service.exposure_view_authority = RegisteredPortfolioExposureViewAuthorityV2(
            {exposure.exposure_view_id: exposure}
        )
        service.account_state_source = lambda: fresh
        service.exposure_view_source = lambda: exposure
        clock[0] = at

    try:
        observe(1000, CUTOFF + timedelta(seconds=1))
        peak = service._evaluate_current_risk(clock[0], fail_closed=True)  # pyright: ignore[reportPrivateUsage]
        assert peak is not None and peak.daily_pnl == 1000
        service.close()
        assert composition.inputs is not None
        renewed_mandate = replace(
            composition.inputs.mandate,
            mandate_id="renewed-risk-universe",
            valid_from=CUTOFF + timedelta(seconds=2),
            valid_until=CUTOFF + timedelta(minutes=10),
            allowed_instruments=frozenset({"510300.SH", "600519.SH"}),
            universe_binding_hash="b" * 64,
        )
        composition.inputs = replace(composition.inputs, mandate=renewed_mandate)
        clock[0] = CUTOFF + timedelta(seconds=2)
        service = open_prospective_mock_execution(composition)
        observe(-100, CUTOFF + timedelta(seconds=2))
        renewed = service._evaluate_current_risk(clock[0], fail_closed=True)  # pyright: ignore[reportPrivateUsage]
        assert renewed is not None and renewed.daily_pnl == -100
        assert renewed.strategy_peak_drawdown == 1100
        tomorrow = datetime.combine(CUTOFF.date() + timedelta(days=1), datetime.min.time(), UTC)
        clock[0] = tomorrow
        with pytest.raises(PermissionError, match="fresh authoritative"):
            service._evaluate_current_risk(tomorrow, fail_closed=True)  # pyright: ignore[reportPrivateUsage]
        with store.authority_transaction() as connection:
            assert (
                connection.execute("SELECT count(*) FROM autonomous_risk_days").fetchone()[0] == 1
            )
        observe(-200, tomorrow)
        rolled = service._evaluate_current_risk(tomorrow, fail_closed=True)  # pyright: ignore[reportPrivateUsage]
        assert rolled is not None and rolled.daily_pnl == 0
        assert rolled.strategy_peak_drawdown == 1200
        assert "stale_risk_measurement" not in service.active_kill_reasons
        observe(-250, tomorrow + timedelta(seconds=1))
        later = service._evaluate_current_risk(clock[0], fail_closed=True)  # pyright: ignore[reportPrivateUsage]
        assert later is not None and later.daily_pnl == -50
        assert later.strategy_peak_drawdown == 1250
        with store.authority_transaction() as connection:
            assert (
                connection.execute("SELECT count(*) FROM autonomous_risk_days").fetchone()[0] == 2
            )
    finally:
        service.close()
