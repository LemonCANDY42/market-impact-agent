import json
import warnings
from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import cast

import pytest

pytest.importorskip("nautilus_trader")

from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.backtests import (
    BacktestMetric,
    BacktestRequest,
    BacktestRunStatus,
    SimulationSpec,
    StrategyBacktestArm,
    StrategyBacktestOutcomeMissing,
    StrategyBacktestRequestTemplate,
    StrategyBacktestVariant,
    reopen_strategy_backtest_outcome,
)
from market_impact_agent.data_inputs import (
    DataFetchStatus,
    DataPITLane,
    DataProviderAttempt,
    DataQuery,
    DataSnapshot,
    DataSourceBinding,
    LocalDataSnapshotStore,
    SourceObservation,
)
from market_impact_agent.domain import Side, SignalIntent
from market_impact_agent.nautilus_backtest import (
    NautilusBacktestBridge,
    load_a_share_daily_bar_snapshot,
)
from market_impact_agent.observations import (
    AvailabilityBasis,
    ObservationCapability,
    ObservationTimes,
    OccurrenceBasis,
)

SNAPSHOT_PATH = (
    Path(__file__).parents[1] / "examples" / "backtests" / "synthetic-xshg-600028-20260825-v1.json"
)


def signal(
    *,
    side: Side = Side.BUY,
    evidence_refs: tuple[str, ...] = ("synthetic-evidence-1",),
    expires_at: datetime = datetime(2026, 9, 1, 8, tzinfo=UTC),
    event_id: str = "synthetic-event-1",
) -> SignalIntent:
    return SignalIntent(
        signal_id="synthetic-signal-1",
        event_id=event_id,
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
    data_snapshot_id: str = "synthetic-xshg-600028-20260825-v1",
) -> BacktestRequest:
    return BacktestRequest(
        request_id="synthetic-xshg-replay-1",
        signal=signal() if bound_signal is None else bound_signal,
        as_of=datetime(2026, 8, 25, 8, tzinfo=UTC),
        start_at=datetime(2026, 8, 26, 1, tzinfo=UTC),
        end_at=datetime(2026, 9, 4, 8, tzinfo=UTC),
        market="CN",
        instrument_ids=("600028.XSHG",),
        data_snapshot_id=data_snapshot_id,
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
    assert result.result_hash == "4f0d2925fec2220e0e8108b8366f2fa24313db563ff877af1da7ea0392e34e99"


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
        "e52b47a26f75677d32380b5748a7a484b639ada3314615cecd5781ede4e9de1c"
    )
    assert first.result_hash == second.result_hash
    assert first.result_hash == ("4f0d2925fec2220e0e8108b8366f2fa24313db563ff877af1da7ea0392e34e99")
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


def test_authoritative_bridge_persists_reopenable_outcome_and_actual_stress(
    tmp_path: Path,
) -> None:
    store = LocalDataSnapshotStore(tmp_path / "authority")
    source = _authoritative_source_snapshot(store)
    bridge = NautilusBacktestBridge(
        SNAPSHOT_PATH,
        snapshot_store=store,
        artifact_store=store.artifacts,
    )
    bound = request(data_snapshot_id=source.snapshot_id)
    variant = StrategyBacktestVariant.build(
        arm=StrategyBacktestArm.CANDIDATE,
        baseline_id=None,
        strategy_ref=bound.strategy_ref,
        target_selection_ref=bound.target_selection_ref,
        request_template=StrategyBacktestRequestTemplate.from_request(bound),
        simulation=bound.simulation,
    )

    receipt = bridge.run_strategy_outcome(
        bound,
        case_id="case-1",
        variant=variant,
    )
    assert not isinstance(receipt, StrategyBacktestOutcomeMissing)
    reopened, result = reopen_strategy_backtest_outcome(store, receipt.receipt_id)

    assert reopened == receipt
    assert result.status is BacktestRunStatus.COMPLETED
    assert receipt.source_snapshot_id == source.snapshot_id
    assert tuple(item.side for item in receipt.fills) == ("buy", "sell")
    assert all(item.available_liquidity_quantity is not None for item in receipt.fills)
    assert all(
        cast(Decimal, item.available_liquidity_quantity) >= item.quantity for item in receipt.fills
    )
    assert receipt.capital_path[-1].equity - receipt.capital_path[0].equity == receipt.net_pnl
    assert receipt.adverse_excursion_path
    assert max(item.adverse_excursion for item in receipt.adverse_excursion_path) == (
        receipt.adverse_excursion
    )
    assert receipt.stressed_net_return is not None
    assert receipt.stress_evidence_artifact_hash is not None
    stress = store.artifacts.read_json(receipt.stress_evidence_artifact_hash)
    assert isinstance(stress, dict)
    assert stress["adverse_excursion_path"]
    stress_fills = cast(list[dict[str, object]], stress["fills"])
    assert all("available_liquidity_quantity" in item for item in stress_fills)
    assert receipt.stressed_net_return < receipt.net_return


def test_legacy_backtest_result_has_no_promotion_receipt(tmp_path: Path) -> None:
    result = NautilusBacktestBridge(SNAPSHOT_PATH).run(request())
    store = LocalDataSnapshotStore(tmp_path / "authority")

    with pytest.raises(KeyError, match="unknown strategy backtest outcome receipt"):
        reopen_strategy_backtest_outcome(store, result.result_hash)


def test_unsupported_frozen_baseline_is_typed_missing_not_candidate_reuse(
    tmp_path: Path,
) -> None:
    store = LocalDataSnapshotStore(tmp_path / "authority")
    source = _authoritative_source_snapshot(store)
    bridge = NautilusBacktestBridge(
        SNAPSHOT_PATH,
        snapshot_store=store,
        artifact_store=store.artifacts,
    )
    baseline_request = replace(
        request(data_snapshot_id=source.snapshot_id),
        strategy_ref="broad-etf-hold.v1",
        target_selection_ref="broad-etf.v1",
    )
    variant = StrategyBacktestVariant.build(
        arm=StrategyBacktestArm.PRIMARY_BASELINE,
        baseline_id="broad-etf-hold",
        strategy_ref=baseline_request.strategy_ref,
        target_selection_ref=baseline_request.target_selection_ref,
        request_template=StrategyBacktestRequestTemplate.from_request(baseline_request),
        simulation=baseline_request.simulation,
    )

    outcome = bridge.run_strategy_outcome(
        baseline_request,
        case_id="case-1",
        variant=variant,
    )

    assert outcome == StrategyBacktestOutcomeMissing(
        case_id="case-1",
        arm=StrategyBacktestArm.PRIMARY_BASELINE,
        strategy_variant_hash=variant.strategy_variant_hash,
        reason="unsupported_strategy_ref:broad-etf-hold.v1",
    )


def test_cash_no_action_baseline_executes_as_a_flat_capital_path(tmp_path: Path) -> None:
    store = LocalDataSnapshotStore(tmp_path / "authority")
    source = _authoritative_source_snapshot(store)
    bridge = NautilusBacktestBridge(
        SNAPSHOT_PATH,
        snapshot_store=store,
        artifact_store=store.artifacts,
    )
    baseline_request = replace(
        request(data_snapshot_id=source.snapshot_id),
        strategy_ref="cash-no-action.v1",
        target_selection_ref="cash-baseline-metadata.v1",
    )
    variant = StrategyBacktestVariant.build(
        arm=StrategyBacktestArm.PRIMARY_BASELINE,
        baseline_id="cash",
        strategy_ref=baseline_request.strategy_ref,
        target_selection_ref=baseline_request.target_selection_ref,
        request_template=StrategyBacktestRequestTemplate.from_request(baseline_request),
        simulation=baseline_request.simulation,
    )

    outcome = bridge.run_strategy_outcome(
        baseline_request,
        case_id="case-1",
        variant=variant,
    )

    assert not isinstance(outcome, StrategyBacktestOutcomeMissing)
    assert outcome.fills == ()
    assert outcome.costs == ()
    assert tuple(point.equity for point in outcome.capital_path) == (
        baseline_request.simulation.starting_cash,
        baseline_request.simulation.starting_cash,
    )
    assert outcome.portfolio_net_return == Decimal(0)
    assert outcome.stressed_net_return == Decimal(0)
    assert outcome.turnover == Decimal(0)
    assert outcome.adverse_excursion_path
    assert all(item.adverse_excursion == 0 for item in outcome.adverse_excursion_path)


def _authoritative_source_snapshot(store: LocalDataSnapshotStore) -> DataSnapshot:
    raw_hash = load_a_share_daily_bar_snapshot(SNAPSHOT_PATH).content_hash
    source = DataSourceBinding(
        provider_id="nautilus-fixture",
        provider_version="1.0.0",
        upstream_source="synthetic-a-share-daily-bars",
        manifest_hash="a" * 64,
        source_config_hash="b" * 64,
        required=True,
    )
    as_of = datetime(2026, 8, 25, 8, tzinfo=UTC)
    query = DataQuery.build(
        capability=ObservationCapability.MARKET_CONTEXT,
        pit_lane=DataPITLane.RETROSPECTIVE,
        as_of=as_of,
        window_start=datetime(2026, 8, 24, 8, tzinfo=UTC),
        source_policy_id="nautilus-fixture-source-v1",
        parameters={"instrument_id": "600028.XSHG"},
        sources=(source,),
        minimum_data_sources=1,
    )
    retrieved_at = as_of
    times = ObservationTimes(
        occurred_at=datetime(2026, 8, 24, 7, tzinfo=UTC),
        published_at=datetime(2026, 8, 24, 7, tzinfo=UTC),
        available_at=datetime(2026, 8, 24, 7, tzinfo=UTC),
        source_updated_at=datetime(2026, 8, 24, 7, tzinfo=UTC),
        aggregator_fetched_at=None,
        retrieved_at=retrieved_at,
        occurrence_basis=OccurrenceBasis.SOURCE_REPORTED,
        availability_basis=AvailabilityBasis.SOURCE_REPORTED,
    )
    observation = SourceObservation.build(
        capability=ObservationCapability.MARKET_CONTEXT,
        provider_id=source.provider_id,
        provider_version=source.provider_version,
        upstream_source=source.upstream_source,
        upstream_record_id="synthetic-xshg-600028-20260825-v1",
        source_ref="fixture://synthetic-xshg-600028-20260825-v1",
        lineage_id="synthetic-xshg-600028-20260825-v1",
        times=times,
        authority_at=None,
        authority_kind=None,
        raw_content_hash=raw_hash,
        normalized_payload={"instrument_id": "600028.XSHG"},
        license_scope="test_fixture",
    )
    attempt = DataProviderAttempt(
        provider_id=source.provider_id,
        provider_version=source.provider_version,
        upstream_source=source.upstream_source,
        required=True,
        status=DataFetchStatus.DATA,
        retrieved_at=retrieved_at,
        raw_response_hash=raw_hash,
        received_count=1,
        accepted_count=1,
        rejected_missing_availability=0,
        rejected_after_cutoff=0,
        rejected_missing_authority=0,
        rejected_authority_after_cutoff=0,
        rejected_lane_mismatch=0,
        error_kind=None,
    )
    core = {
        "schema_version": "market-impact.data-snapshot.v2",
        "query": query.to_dict(),
        "attempts": [attempt.to_dict()],
        "observations": [observation.to_dict()],
        "coverage_complete": True,
        "completed_at": retrieved_at.isoformat().replace("+00:00", "Z"),
    }
    snapshot = DataSnapshot(
        snapshot_id=f"data-snapshot-{canonical_hash(core)}",
        query=query,
        attempts=(attempt,),
        observations=(observation,),
        coverage_complete=True,
        completed_at=retrieved_at,
    )
    store.put(snapshot)
    return snapshot


def _metrics_by_name(metrics: tuple[BacktestMetric, ...]) -> dict[str, BacktestMetric]:
    return {metric.name: metric for metric in metrics}
