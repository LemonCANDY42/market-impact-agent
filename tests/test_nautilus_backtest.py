import json
import warnings
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import cast

import pytest

pytest.importorskip("nautilus_trader")

from market_impact_agent.backtests import (
    BacktestMetric,
    BacktestRequest,
    BacktestRunStatus,
    SimulationSpec,
)
from market_impact_agent.domain import Side, SignalIntent
from market_impact_agent.nautilus_backtest import (
    NautilusBacktestBridge,
    load_a_share_daily_bar_snapshot,
)

SNAPSHOT_PATH = (
    Path(__file__).parents[1] / "examples" / "backtests" / "synthetic-xshg-600028-20260825-v1.json"
)


def signal(
    *,
    side: Side = Side.BUY,
    evidence_refs: tuple[str, ...] = ("synthetic-evidence-1",),
    expires_at: datetime = datetime(2026, 9, 1, 8, tzinfo=UTC),
) -> SignalIntent:
    return SignalIntent(
        signal_id="synthetic-signal-1",
        event_id="synthetic-event-1",
        instrument_id="600028.XSHG",
        side=side,
        valid_from=datetime(2026, 8, 25, 7, tzinfo=UTC),
        expires_at=expires_at,
        evidence_refs=evidence_refs,
        invalidation_conditions=("synthetic event is invalidated",),
    )


def request(
    *,
    book_type: str = "top_of_book",
    bound_signal: SignalIntent | None = None,
    horizons_sessions: tuple[int, ...] = (3,),
) -> BacktestRequest:
    return BacktestRequest(
        request_id="synthetic-xshg-replay-1",
        signal=signal() if bound_signal is None else bound_signal,
        as_of=datetime(2026, 8, 25, 8, tzinfo=UTC),
        start_at=datetime(2026, 8, 26, 1, tzinfo=UTC),
        end_at=datetime(2026, 9, 4, 8, tzinfo=UTC),
        market="CN",
        instrument_ids=("600028.XSHG",),
        data_snapshot_id="synthetic-xshg-600028-20260825-v1",
        target_selection_ref="manual-integration-fixture:synthetic.v1",
        strategy_ref="event-impact-hold.v1",
        horizons_sessions=horizons_sessions,
        simulation=SimulationSpec(
            data_granularity="daily_bar.v1",
            book_type=book_type,
            fill_model="next_executable_open_one_tick_slippage.v1",
            fee_model="a_share_fixture_fee.v1",
            venue_ruleset="xshg_cash_equity_fixture.v1",
            base_currency="CNY",
            starting_cash=Decimal("1000000"),
            random_seed=7,
        ),
    )


def test_snapshot_encodes_suspension_limit_lock_and_lot_size() -> None:
    snapshot = load_a_share_daily_bar_snapshot(SNAPSHOT_PATH)

    assert snapshot.bars[0].suspended
    assert snapshot.bars[0].open_ask_quantity == 0
    assert snapshot.bars[1].open == Decimal("11.00")
    assert snapshot.bars[1].open_ask_quantity == 0
    assert snapshot.bars[2].open_ask_quantity % snapshot.lot_size == 0


def test_snapshot_rejects_prices_outside_the_configured_limit(tmp_path: Path) -> None:
    payload = cast(dict[str, object], json.loads(SNAPSHOT_PATH.read_text()))
    bars = cast(list[dict[str, object]], payload["bars"])
    bars[1]["high"] = "11.01"
    invalid_path = tmp_path / "invalid-snapshot.json"
    invalid_path.write_text(json.dumps(payload))

    with pytest.raises(ValueError, match="daily price limit"):
        load_a_share_daily_bar_snapshot(invalid_path)


def test_nautilus_bridge_runs_the_first_a_share_replay() -> None:
    result = NautilusBacktestBridge(SNAPSHOT_PATH).run(request())
    metrics = _metrics_by_name(result.metrics)

    assert result.status is BacktestRunStatus.COMPLETED
    assert result.manifest.engine_version == "1.231.0"
    assert metrics["entry_delay_sessions"].value == Decimal(2)
    assert metrics["entry_price"].value == Decimal("10.81")
    assert metrics["exit_price"].value == Decimal("11.39")
    assert metrics["holding_sessions"].value == Decimal(3)
    assert metrics["commission"].value == Decimal("10.57")
    assert metrics["net_pnl"].value == Decimal("47.43")
    assert metrics["net_return"].value == Decimal("47.43") / Decimal("1081")
    assert metrics["order_count"].value == Decimal(2)


def test_nautilus_bridge_runs_each_horizon_in_an_independent_engine() -> None:
    result = NautilusBacktestBridge(SNAPSHOT_PATH).run(request(horizons_sessions=(1, 3)))
    metrics = _metrics_by_name(result.metrics)

    assert result.status is BacktestRunStatus.COMPLETED
    assert metrics["horizon_1.holding_sessions"].value == Decimal(1)
    assert metrics["horizon_3.holding_sessions"].value == Decimal(3)
    assert "net_return" not in metrics
    assert metrics["horizon_1.net_return"].value != metrics["horizon_3.net_return"].value


def test_signal_content_changes_request_and_result_identity() -> None:
    bridge = NautilusBacktestBridge(SNAPSHOT_PATH)
    original = bridge.run(request())
    revised = bridge.run(
        request(bound_signal=signal(evidence_refs=("synthetic-evidence-corrected",)))
    )

    assert revised.manifest.request.signal.signal_id == original.manifest.request.signal.signal_id
    assert revised.manifest.request_hash != original.manifest.request_hash
    assert revised.result_hash != original.result_hash


def test_sell_signal_fails_closed_and_deterministically() -> None:
    bridge = NautilusBacktestBridge(SNAPSHOT_PATH)
    sell_request = request(bound_signal=signal(side=Side.SELL))

    first = bridge.run(sell_request)
    second = bridge.run(sell_request)

    assert first.status is BacktestRunStatus.FAILED
    assert first.failure_reasons == (
        "ValueError: xshg_cash_equity_fixture.v1 is long-only and does not support SELL signals",
    )
    assert first.result_hash == second.result_hash


def test_entry_must_be_executable_before_signal_expiry() -> None:
    expiring = signal(expires_at=datetime(2026, 8, 28, 1, 30, tzinfo=UTC))

    result = NautilusBacktestBridge(SNAPSHOT_PATH).run(request(bound_signal=expiring))

    assert result.status is BacktestRunStatus.FAILED
    assert result.failure_reasons == (
        "ValueError: replay window has no executable buy entry before signal expiry",
    )


def test_nautilus_bridge_replay_is_stable_when_warnings_are_errors() -> None:
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        result = NautilusBacktestBridge(SNAPSHOT_PATH).run(request())

    assert result.status is BacktestRunStatus.COMPLETED
    assert result.result_hash == "604d9aee20f97377b4d7327b0ffd876204a67f7de9789ef9e7ec9dd3c29a3e89"


def test_repeated_frozen_replays_have_the_same_result_identity() -> None:
    bridge = NautilusBacktestBridge(SNAPSHOT_PATH)

    first = bridge.run(request())
    second = bridge.run(request())

    assert first.status is BacktestRunStatus.COMPLETED
    assert second.status is BacktestRunStatus.COMPLETED
    assert first.manifest.run_id != second.manifest.run_id
    assert first.manifest.request_hash == (
        "7b4c27086bdd810aaf1853217df5f92a23010600e22a485c5929fcf267c2690b"
    )
    assert first.manifest.engine_config_hash == (
        "0761605fe3adf6bd300ced939338e57e4c5ebae003b52472c4e0a8aa42fc5a41"
    )
    assert first.result_hash == second.result_hash
    assert first.result_hash == ("604d9aee20f97377b4d7327b0ffd876204a67f7de9789ef9e7ec9dd3c29a3e89")
    assert first.metrics == second.metrics
    assert first.artifact_refs == second.artifact_refs


def test_unsupported_simulation_contract_fails_closed_and_deterministically() -> None:
    bridge = NautilusBacktestBridge(SNAPSHOT_PATH)
    unsupported = request(book_type="bar")

    first = bridge.run(unsupported)
    second = bridge.run(unsupported)

    assert first.status is BacktestRunStatus.FAILED
    assert first.metrics == ()
    assert first.failure_reasons == (
        "ValueError: unsupported book_type: expected 'top_of_book', got 'bar'",
    )
    assert first.result_hash == second.result_hash


def _metrics_by_name(metrics: tuple[BacktestMetric, ...]) -> dict[str, BacktestMetric]:
    return {metric.name: metric for metric in metrics}
