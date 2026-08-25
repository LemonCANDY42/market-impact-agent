from dataclasses import replace
from datetime import UTC, datetime, timedelta, timezone
from decimal import Decimal
from typing import cast

import pytest

from market_impact_agent.backtests import (
    BacktestMetric,
    BacktestRequest,
    BacktestResult,
    BacktestRunManifest,
    BacktestRunStatus,
    SimulationSpec,
    canonical_backtest_request_hash,
    canonical_backtest_result_hash,
)
from market_impact_agent.domain import Side, SignalIntent

AS_OF = datetime(2026, 8, 25, 8, tzinfo=UTC)


def simulation() -> SimulationSpec:
    return SimulationSpec(
        data_granularity="daily_bar.v1",
        book_type="bar",
        fill_model="next_executable_open.v1",
        fee_model="a_share_standard.v1",
        venue_ruleset="sse_szse_cash_equity.v1",
        base_currency="CNY",
        starting_cash=Decimal("1000000"),
        random_seed=7,
    )


def signal() -> SignalIntent:
    return SignalIntent(
        signal_id="signal-1",
        event_id="event-1",
        instrument_id="600028.XSHG",
        side=Side.BUY,
        valid_from=AS_OF - timedelta(hours=1),
        expires_at=AS_OF + timedelta(days=7),
        evidence_refs=("evidence-1",),
        invalidation_conditions=("the event is invalidated",),
    )


def request() -> BacktestRequest:
    return BacktestRequest(
        request_id="backtest-request-1",
        signal=signal(),
        as_of=AS_OF,
        start_at=AS_OF + timedelta(days=1),
        end_at=AS_OF + timedelta(days=14),
        market="CN",
        instrument_ids=("600028.XSHG",),
        data_snapshot_id="tushare-2026-08-25-v1",
        strategy_ref="event-impact-hold.v1",
        horizons_sessions=(1, 3, 10),
        simulation=simulation(),
    )


def manifest(
    values: BacktestRequest | None = None,
    *,
    run_id: str = "backtest-run-1",
    executed_at: datetime = AS_OF + timedelta(days=30),
) -> BacktestRunManifest:
    request_value = request() if values is None else values
    return BacktestRunManifest(
        run_id=run_id,
        request=request_value,
        request_hash=canonical_backtest_request_hash(request_value),
        engine_name="nautilus_trader",
        engine_version="1.231.0",
        bridge_name="nautilus-backtest",
        bridge_version="0.1.0",
        engine_config_hash="b" * 64,
        executed_at=executed_at,
    )


def test_backtest_request_requires_a_frozen_forward_window() -> None:
    values = request()

    with pytest.raises(ValueError, match="start_at must not be before as_of"):
        BacktestRequest(
            request_id=values.request_id,
            signal=values.signal,
            as_of=values.as_of,
            start_at=values.as_of - timedelta(seconds=1),
            end_at=values.end_at,
            market=values.market,
            instrument_ids=values.instrument_ids,
            data_snapshot_id=values.data_snapshot_id,
            strategy_ref=values.strategy_ref,
            horizons_sessions=values.horizons_sessions,
            simulation=values.simulation,
        )

    with pytest.raises(ValueError, match="end_at must be after start_at"):
        BacktestRequest(
            request_id=values.request_id,
            signal=values.signal,
            as_of=values.as_of,
            start_at=values.start_at,
            end_at=values.start_at,
            market=values.market,
            instrument_ids=values.instrument_ids,
            data_snapshot_id=values.data_snapshot_id,
            strategy_ref=values.strategy_ref,
            horizons_sessions=values.horizons_sessions,
            simulation=values.simulation,
        )


def test_backtest_request_binds_signal_target_and_validity() -> None:
    values = request()

    with pytest.raises(ValueError, match="signal instrument_id must belong"):
        BacktestRequest(
            request_id=values.request_id,
            signal=replace(values.signal, instrument_id="601857.XSHG"),
            as_of=values.as_of,
            start_at=values.start_at,
            end_at=values.end_at,
            market=values.market,
            instrument_ids=values.instrument_ids,
            data_snapshot_id=values.data_snapshot_id,
            strategy_ref=values.strategy_ref,
            horizons_sessions=values.horizons_sessions,
            simulation=values.simulation,
        )

    with pytest.raises(ValueError, match="as_of must be within signal validity"):
        BacktestRequest(
            request_id=values.request_id,
            signal=replace(values.signal, valid_from=values.as_of + timedelta(hours=1)),
            as_of=values.as_of,
            start_at=values.start_at,
            end_at=values.end_at,
            market=values.market,
            instrument_ids=values.instrument_ids,
            data_snapshot_id=values.data_snapshot_id,
            strategy_ref=values.strategy_ref,
            horizons_sessions=values.horizons_sessions,
            simulation=values.simulation,
        )

    with pytest.raises(ValueError, match="start_at must be within signal validity"):
        BacktestRequest(
            request_id=values.request_id,
            signal=replace(values.signal, expires_at=values.start_at),
            as_of=values.as_of,
            start_at=values.start_at,
            end_at=values.end_at,
            market=values.market,
            instrument_ids=values.instrument_ids,
            data_snapshot_id=values.data_snapshot_id,
            strategy_ref=values.strategy_ref,
            horizons_sessions=values.horizons_sessions,
            simulation=values.simulation,
        )


def test_backtest_request_rejects_ambiguous_scope() -> None:
    values = request()

    with pytest.raises(ValueError, match="instrument_ids values must be unique"):
        BacktestRequest(
            request_id=values.request_id,
            signal=values.signal,
            as_of=values.as_of,
            start_at=values.start_at,
            end_at=values.end_at,
            market=values.market,
            instrument_ids=("600028.XSHG", "600028.XSHG"),
            data_snapshot_id=values.data_snapshot_id,
            strategy_ref=values.strategy_ref,
            horizons_sessions=values.horizons_sessions,
            simulation=values.simulation,
        )

    with pytest.raises(ValueError, match="positive, unique, and ascending"):
        BacktestRequest(
            request_id=values.request_id,
            signal=values.signal,
            as_of=values.as_of,
            start_at=values.start_at,
            end_at=values.end_at,
            market=values.market,
            instrument_ids=values.instrument_ids,
            data_snapshot_id=values.data_snapshot_id,
            strategy_ref=values.strategy_ref,
            horizons_sessions=(3, 1, 3),
            simulation=values.simulation,
        )


@pytest.mark.parametrize("random_seed", [cast(int, True), cast(int, "7")])
def test_simulation_spec_rejects_non_integer_runtime_random_seed(random_seed: int) -> None:
    with pytest.raises(ValueError, match="random_seed must be a non-negative integer"):
        SimulationSpec(
            data_granularity="daily_bar.v1",
            book_type="bar",
            fill_model="next_executable_open.v1",
            fee_model="a_share_standard.v1",
            venue_ruleset="sse_szse_cash_equity.v1",
            base_currency="CNY",
            starting_cash=Decimal("1000000"),
            random_seed=random_seed,
        )


@pytest.mark.parametrize(
    "horizons_sessions",
    [cast(tuple[int, ...], (1, True)), cast(tuple[int, ...], (1, "3"))],
)
def test_backtest_request_rejects_non_integer_runtime_horizons(
    horizons_sessions: tuple[int, ...],
) -> None:
    values = request()

    with pytest.raises(ValueError, match="positive, unique, and ascending"):
        BacktestRequest(
            request_id=values.request_id,
            signal=values.signal,
            as_of=values.as_of,
            start_at=values.start_at,
            end_at=values.end_at,
            market=values.market,
            instrument_ids=values.instrument_ids,
            data_snapshot_id=values.data_snapshot_id,
            strategy_ref=values.strategy_ref,
            horizons_sessions=horizons_sessions,
            simulation=values.simulation,
        )


@pytest.mark.parametrize("starting_cash", [Decimal("0"), Decimal("NaN")])
def test_simulation_spec_requires_replayable_inputs(starting_cash: Decimal) -> None:
    with pytest.raises(ValueError, match="starting_cash must be finite and positive"):
        SimulationSpec(
            data_granularity="daily_bar.v1",
            book_type="bar",
            fill_model="next_executable_open.v1",
            fee_model="a_share_standard.v1",
            venue_ruleset="sse_szse_cash_equity.v1",
            base_currency="CNY",
            starting_cash=starting_cash,
            random_seed=7,
        )


def test_manifest_binds_exact_engine_bridge_and_configuration() -> None:
    with pytest.raises(ValueError, match="request_hash must be a SHA-256 hex digest"):
        BacktestRunManifest(
            run_id="backtest-run-1",
            request=request(),
            request_hash="not-a-hash",
            engine_name="nautilus_trader",
            engine_version="1.231.0",
            bridge_name="nautilus-backtest",
            bridge_version="0.1.0",
            engine_config_hash="b" * 64,
            executed_at=AS_OF + timedelta(days=30),
        )

    values = request()
    with pytest.raises(ValueError, match="request_hash must match canonical request content"):
        BacktestRunManifest(
            run_id="backtest-run-1",
            request=values,
            request_hash="a" * 64,
            engine_name="nautilus_trader",
            engine_version="1.231.0",
            bridge_name="nautilus-backtest",
            bridge_version="0.1.0",
            engine_config_hash="b" * 64,
            executed_at=AS_OF + timedelta(days=30),
        )


def test_canonical_request_hash_normalizes_timestamps_and_decimals() -> None:
    values = request()
    equivalent = BacktestRequest(
        request_id=values.request_id,
        signal=replace(
            values.signal,
            valid_from=values.signal.valid_from.astimezone(timezone(timedelta(hours=8))),
            expires_at=values.signal.expires_at.astimezone(timezone(timedelta(hours=8))),
        ),
        as_of=values.as_of.astimezone(timezone(timedelta(hours=8))),
        start_at=values.start_at.astimezone(timezone(timedelta(hours=8))),
        end_at=values.end_at.astimezone(timezone(timedelta(hours=8))),
        market=values.market,
        instrument_ids=values.instrument_ids,
        data_snapshot_id=values.data_snapshot_id,
        strategy_ref=values.strategy_ref,
        horizons_sessions=values.horizons_sessions,
        simulation=SimulationSpec(
            data_granularity=values.simulation.data_granularity,
            book_type=values.simulation.book_type,
            fill_model=values.simulation.fill_model,
            fee_model=values.simulation.fee_model,
            venue_ruleset=values.simulation.venue_ruleset,
            base_currency=values.simulation.base_currency,
            starting_cash=Decimal("1000000.00"),
            random_seed=values.simulation.random_seed,
        ),
    )

    assert canonical_backtest_request_hash(equivalent) == canonical_backtest_request_hash(values)


def test_canonical_request_hash_binds_signal_content_not_only_its_id() -> None:
    original = request()
    changed_signal = replace(original.signal, evidence_refs=("evidence-corrected",))
    changed = replace(original, signal=changed_signal)

    assert changed.signal.signal_id == original.signal.signal_id
    assert canonical_backtest_request_hash(changed) != canonical_backtest_request_hash(original)


def test_manifest_rejects_a_reused_hash_for_a_different_request() -> None:
    original = request()
    changed = BacktestRequest(
        request_id=original.request_id,
        signal=replace(original.signal, evidence_refs=("evidence-2",)),
        as_of=original.as_of,
        start_at=original.start_at,
        end_at=original.end_at,
        market=original.market,
        instrument_ids=original.instrument_ids,
        data_snapshot_id=original.data_snapshot_id,
        strategy_ref=original.strategy_ref,
        horizons_sessions=original.horizons_sessions,
        simulation=original.simulation,
    )

    with pytest.raises(ValueError, match="request_hash must match canonical request content"):
        BacktestRunManifest(
            run_id="backtest-run-1",
            request=changed,
            request_hash=canonical_backtest_request_hash(original),
            engine_name="nautilus_trader",
            engine_version="1.231.0",
            bridge_name="nautilus-backtest",
            bridge_version="0.1.0",
            engine_config_hash="b" * 64,
            executed_at=AS_OF + timedelta(days=30),
        )


def test_completed_and_failed_results_are_unambiguous() -> None:
    metric = BacktestMetric(name="return", value=Decimal("0.0125"), unit="ratio")
    completed_manifest = manifest()
    completed = BacktestResult(
        manifest=completed_manifest,
        status=BacktestRunStatus.COMPLETED,
        result_hash=canonical_backtest_result_hash(
            manifest=completed_manifest,
            status=BacktestRunStatus.COMPLETED,
            metrics=(metric,),
            artifact_refs=("artifact://returns.parquet",),
            failure_reasons=(),
        ),
        metrics=(metric,),
        artifact_refs=("artifact://returns.parquet",),
        failure_reasons=(),
    )

    assert completed.metrics == (metric,)

    with pytest.raises(ValueError, match="result_hash must be a SHA-256 hex digest"):
        BacktestResult(
            manifest=manifest(),
            status=BacktestRunStatus.COMPLETED,
            result_hash=cast(str, None),
            metrics=(),
            artifact_refs=(),
            failure_reasons=(),
        )

    with pytest.raises(ValueError, match="failed results require failure_reasons"):
        BacktestResult(
            manifest=manifest(),
            status=BacktestRunStatus.FAILED,
            result_hash="c" * 64,
            metrics=(),
            artifact_refs=(),
            failure_reasons=(),
        )


def test_result_rejects_a_reused_hash_for_different_metrics() -> None:
    manifest_value = manifest()
    original_metrics = (BacktestMetric(name="return", value=Decimal("0.0125"), unit="ratio"),)
    result_hash = canonical_backtest_result_hash(
        manifest=manifest_value,
        status=BacktestRunStatus.COMPLETED,
        metrics=original_metrics,
        artifact_refs=("artifact://returns.parquet",),
        failure_reasons=(),
    )

    with pytest.raises(ValueError, match="result_hash must match canonical result content"):
        BacktestResult(
            manifest=manifest_value,
            status=BacktestRunStatus.COMPLETED,
            result_hash=result_hash,
            metrics=(BacktestMetric(name="return", value=Decimal("0.0126"), unit="ratio"),),
            artifact_refs=("artifact://returns.parquet",),
            failure_reasons=(),
        )


def test_result_hash_omits_per_run_manifest_metadata() -> None:
    first_manifest = manifest(run_id="backtest-run-1", executed_at=AS_OF + timedelta(days=30))
    replay_manifest = manifest(run_id="backtest-run-2", executed_at=AS_OF + timedelta(days=31))
    metrics = (BacktestMetric(name="return", value=Decimal("0.012500"), unit="ratio"),)

    first_hash = canonical_backtest_result_hash(
        manifest=first_manifest,
        status=BacktestRunStatus.COMPLETED,
        metrics=metrics,
        artifact_refs=("artifact://returns.parquet",),
        failure_reasons=(),
    )
    replay_hash = canonical_backtest_result_hash(
        manifest=replay_manifest,
        status=BacktestRunStatus.COMPLETED,
        metrics=(BacktestMetric(name="return", value=Decimal("0.0125"), unit="ratio"),),
        artifact_refs=("artifact://returns.parquet",),
        failure_reasons=(),
    )

    assert replay_hash == first_hash
