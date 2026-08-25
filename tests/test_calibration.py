import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Protocol, cast

from jsonschema import Draft202012Validator

from market_impact_agent.backtests import (
    BacktestInputHash,
    BacktestMetric,
    BacktestRequest,
    BacktestResult,
    BacktestRunManifest,
    BacktestRunStatus,
    SimulationSpec,
    backtest_result_to_dict,
    canonical_backtest_request_hash,
    canonical_backtest_result_hash,
)
from market_impact_agent.calibration import (
    PHASE2_REQUIRED_HORIZONS,
    CalibrationPartition,
    CalibrationRunEvidence,
    CalibrationVariant,
    assess_phase2_calibration,
    load_phase2_calibration_evidence,
    phase2_calibration_gate_result_to_dict,
)
from market_impact_agent.domain import Side, SignalIntent

ROOT = Path(__file__).parents[1]


class Validator(Protocol):
    def validate(self, instance: object) -> None: ...


def test_phase2_gate_accepts_complete_deterministic_walk_forward_evidence() -> None:
    evidence: list[CalibrationRunEvidence] = []
    visible_at = datetime(2020, 1, 1, tzinfo=UTC)
    candidate_returns = (
        (Decimal("0.03"), Decimal("0.02"), Decimal("0.01")),
        (Decimal("0.04"), Decimal("0.03"), Decimal("0.02")),
        (Decimal("0.06"), Decimal("0.03"), Decimal("0.02")),
        (Decimal("0.04"), Decimal("0.02"), Decimal("0.01")),
        (Decimal("0.05"), Decimal("0.025"), Decimal("0.015")),
    )
    baseline_returns = {
        CalibrationVariant.SENTIMENT: (Decimal("0.01"), Decimal("0.005"), Decimal("0")),
        CalibrationVariant.MOMENTUM: (Decimal("-0.01"), Decimal("0"), Decimal("0.005")),
        CalibrationVariant.FIXED_MAPPING: (
            Decimal("0.015"),
            Decimal("0.01"),
            Decimal("0.005"),
        ),
        CalibrationVariant.SIMPLE_HOLD: (
            Decimal("0.005"),
            Decimal("0.005"),
            Decimal("0.005"),
        ),
    }
    for index in range(5):
        cluster = f"energy-cluster-{index + 1}"
        partition = CalibrationPartition.TRAIN if index < 2 else CalibrationPartition.TEST
        evidence.append(
            run_evidence(
                cluster,
                visible_at + timedelta(days=index * 30),
                partition,
                CalibrationVariant.EVENT_REASONING,
                candidate_returns[index],
            )
        )
        evidence.extend(
            run_evidence(
                cluster,
                visible_at + timedelta(days=index * 30),
                partition,
                variant,
                returns,
            )
            for variant, returns in baseline_returns.items()
        )

    result = assess_phase2_calibration(tuple(evidence))
    reordered = assess_phase2_calibration(tuple(reversed(evidence)))

    assert result.accepted
    assert result.reasons == ()
    assert result.beat_baselines == tuple(sorted(baseline_returns, key=lambda item: item.value))
    assert result.max_single_event_share is not None
    assert result.max_single_event_share <= Decimal("0.5")
    assert result.evidence_hash == reordered.evidence_hash
    assert result.report_hash == reordered.report_hash
    payload = phase2_calibration_gate_result_to_dict(result)
    assert payload["accepted"] is True
    validate_report(payload)


def test_phase2_gate_rejects_the_current_single_manual_integration_fixture() -> None:
    evidence = run_evidence(
        "abqaiq-khurais-attack-2019",
        datetime(2019, 9, 18, 23, 59, 59, tzinfo=UTC),
        CalibrationPartition.TEST,
        CalibrationVariant.EVENT_REASONING,
        (Decimal("0.01"), Decimal("0.02"), Decimal("0.03")),
        target_selection_ref="manual-integration-fixture:abqaiq-600028.v1",
    )

    result = assess_phase2_calibration((evidence,))

    assert not result.accepted
    assert "insufficient_train_event_clusters" in result.reasons
    assert "insufficient_test_event_clusters" in result.reasons
    assert "manual_fixture_candidate:abqaiq-khurais-attack-2019" in result.reasons
    assert any(
        reason.startswith("missing_variants:abqaiq-khurais-attack-2019:")
        for reason in result.reasons
    )
    assert "no_meaningful_baseline_beaten" in result.reasons
    assert "single_event_dominance_not_cleared" in result.reasons
    validate_report(phase2_calibration_gate_result_to_dict(result))


def test_phase2_gate_rejects_a_nondeterministic_repeat() -> None:
    item = run_evidence(
        "cluster-1",
        datetime(2020, 1, 1, tzinfo=UTC),
        CalibrationPartition.TEST,
        CalibrationVariant.EVENT_REASONING,
        (Decimal("0.01"), Decimal("0.02"), Decimal("0.03")),
    )
    changed_repeat = result(
        event_cluster_id="cluster-1",
        visible_at=item.visible_at,
        variant=CalibrationVariant.EVENT_REASONING,
        returns=(Decimal("0.01"), Decimal("0.02"), Decimal("0.04")),
        run_number=2,
    )

    assessed = assess_phase2_calibration(
        (
            CalibrationRunEvidence(
                event_cluster_id=item.event_cluster_id,
                visible_at=item.visible_at,
                partition=item.partition,
                variant=item.variant,
                first=item.first,
                repeat=changed_repeat,
            ),
        )
    )

    assert "nondeterministic_repeat:cluster-1:event_reasoning" in assessed.reasons


def test_phase2_gate_rejects_non_ratio_net_return_metrics() -> None:
    item = run_evidence(
        "cluster-1",
        datetime(2020, 1, 1, tzinfo=UTC),
        CalibrationPartition.TEST,
        CalibrationVariant.EVENT_REASONING,
        (Decimal("1"), Decimal("2"), Decimal("3")),
        metric_unit="CNY",
    )

    assessed = assess_phase2_calibration((item,))

    assert "net_return_metrics_missing:cluster-1:event_reasoning" in assessed.reasons


def test_phase2_gate_rejects_variants_with_different_runtime_inputs() -> None:
    visible_at = datetime(2020, 1, 1, tzinfo=UTC)
    evidence = [
        run_evidence(
            "cluster-1",
            visible_at,
            CalibrationPartition.TEST,
            CalibrationVariant.EVENT_REASONING,
            (Decimal("0.01"), Decimal("0.02"), Decimal("0.03")),
        )
    ]
    evidence.extend(
        run_evidence(
            "cluster-1",
            visible_at,
            CalibrationPartition.TEST,
            variant,
            (Decimal("0"), Decimal("0"), Decimal("0")),
            engine_name="different-engine",
            engine_config_hash="c" * 64,
            input_hash="d" * 64,
            artifact_ref="snapshot://different",
        )
        for variant in (
            CalibrationVariant.SENTIMENT,
            CalibrationVariant.MOMENTUM,
            CalibrationVariant.FIXED_MAPPING,
            CalibrationVariant.SIMPLE_HOLD,
        )
    )

    assessed = assess_phase2_calibration(tuple(evidence))

    assert "incomparable_variant_windows:cluster-1" in assessed.reasons


def test_calibration_evidence_loader_binds_relative_result_artifacts(tmp_path: Path) -> None:
    item = run_evidence(
        "cluster-1",
        datetime(2020, 1, 1, tzinfo=UTC),
        CalibrationPartition.TEST,
        CalibrationVariant.EVENT_REASONING,
        (Decimal("0.01"), Decimal("0.02"), Decimal("0.03")),
    )
    (tmp_path / "first.json").write_text(json.dumps(backtest_result_to_dict(item.first)))
    (tmp_path / "repeat.json").write_text(json.dumps(backtest_result_to_dict(item.repeat)))
    evidence_path = tmp_path / "evidence.json"
    evidence_path.write_text(
        json.dumps(
            {
                "schema_version": "market-impact.phase2-calibration-evidence.v1",
                "runs": [
                    {
                        "event_cluster_id": "cluster-1",
                        "visible_at": "2020-01-01T00:00:00Z",
                        "partition": "test",
                        "variant": "event_reasoning",
                        "first_result": "first.json",
                        "repeat_result": "repeat.json",
                    }
                ],
            }
        )
    )

    loaded = load_phase2_calibration_evidence(evidence_path)

    assert len(loaded) == 1
    assert loaded[0].first.result_hash == item.first.result_hash
    assert loaded[0].repeat.result_hash == item.repeat.result_hash


def test_calibration_evidence_loader_rejects_path_escape(tmp_path: Path) -> None:
    evidence_path = tmp_path / "evidence.json"
    evidence_path.write_text(
        json.dumps(
            {
                "schema_version": "market-impact.phase2-calibration-evidence.v1",
                "runs": [
                    {
                        "event_cluster_id": "cluster-1",
                        "visible_at": "2020-01-01T00:00:00Z",
                        "partition": "test",
                        "variant": "event_reasoning",
                        "first_result": "../outside.json",
                        "repeat_result": "repeat.json",
                    }
                ],
            }
        )
    )

    try:
        load_phase2_calibration_evidence(evidence_path)
    except ValueError as exc:
        assert "must stay below" in str(exc)
    else:
        raise AssertionError("path escape must fail closed")


def test_calibration_evidence_loader_rejects_empty_runs(tmp_path: Path) -> None:
    evidence_path = tmp_path / "evidence.json"
    evidence_path.write_text(
        json.dumps(
            {
                "schema_version": "market-impact.phase2-calibration-evidence.v1",
                "runs": [],
            }
        )
    )

    try:
        load_phase2_calibration_evidence(evidence_path)
    except ValueError as exc:
        assert "must not be empty" in str(exc)
    else:
        raise AssertionError("empty evidence must fail closed")


def run_evidence(
    event_cluster_id: str,
    visible_at: datetime,
    partition: CalibrationPartition,
    variant: CalibrationVariant,
    returns: tuple[Decimal, Decimal, Decimal],
    *,
    target_selection_ref: str = "registered-transmission-pack:energy.v1",
    metric_unit: str = "ratio",
    engine_name: str = "test-engine",
    engine_config_hash: str = "b" * 64,
    input_hash: str = "a" * 64,
    artifact_ref: str = "snapshot://test",
) -> CalibrationRunEvidence:
    return CalibrationRunEvidence(
        event_cluster_id=event_cluster_id,
        visible_at=visible_at,
        partition=partition,
        variant=variant,
        first=result(
            event_cluster_id=event_cluster_id,
            visible_at=visible_at,
            variant=variant,
            returns=returns,
            run_number=1,
            target_selection_ref=target_selection_ref,
            metric_unit=metric_unit,
            engine_name=engine_name,
            engine_config_hash=engine_config_hash,
            input_hash=input_hash,
            artifact_ref=artifact_ref,
        ),
        repeat=result(
            event_cluster_id=event_cluster_id,
            visible_at=visible_at,
            variant=variant,
            returns=returns,
            run_number=2,
            target_selection_ref=target_selection_ref,
            metric_unit=metric_unit,
            engine_name=engine_name,
            engine_config_hash=engine_config_hash,
            input_hash=input_hash,
            artifact_ref=artifact_ref,
        ),
    )


def result(
    *,
    event_cluster_id: str,
    visible_at: datetime,
    variant: CalibrationVariant,
    returns: tuple[Decimal, Decimal, Decimal],
    run_number: int,
    target_selection_ref: str = "registered-transmission-pack:energy.v1",
    metric_unit: str = "ratio",
    engine_name: str = "test-engine",
    engine_config_hash: str = "b" * 64,
    input_hash: str = "a" * 64,
    artifact_ref: str = "snapshot://test",
) -> BacktestResult:
    request = BacktestRequest(
        request_id=f"{event_cluster_id}-{variant.value}",
        signal=SignalIntent(
            signal_id=f"{event_cluster_id}-{variant.value}-signal",
            event_id=event_cluster_id,
            instrument_id="600028.XSHG",
            side=Side.BUY,
            valid_from=visible_at - timedelta(days=1),
            expires_at=visible_at + timedelta(days=30),
            evidence_refs=(f"{event_cluster_id}-evidence",),
            invalidation_conditions=(f"{event_cluster_id}-invalidated",),
        ),
        as_of=visible_at,
        start_at=visible_at + timedelta(days=1),
        end_at=visible_at + timedelta(days=29),
        market="CN",
        instrument_ids=("600028.XSHG",),
        data_snapshot_id=f"snapshot-{event_cluster_id}",
        target_selection_ref=target_selection_ref,
        strategy_ref=f"{variant.value}.v1",
        horizons_sessions=PHASE2_REQUIRED_HORIZONS,
        simulation=SimulationSpec(
            data_granularity="daily.v1",
            book_type="modeled_open.v1",
            fill_model="modeled_open.v1",
            fee_model="fees.v1",
            venue_ruleset="xshg.v1",
            base_currency="CNY",
            starting_cash=Decimal("1000000"),
            random_seed=0,
        ),
    )
    manifest = BacktestRunManifest(
        run_id=f"{event_cluster_id}-{variant.value}-run-{run_number}",
        request=request,
        request_hash=canonical_backtest_request_hash(request),
        engine_name=engine_name,
        engine_version="1",
        bridge_name="test-bridge",
        bridge_version="1",
        data_adapter_name="test-adapter",
        data_adapter_version="1",
        input_hashes=(BacktestInputHash("snapshot", input_hash),),
        engine_config_hash=engine_config_hash,
        executed_at=visible_at + timedelta(days=40, seconds=run_number),
    )
    metrics = tuple(
        BacktestMetric(f"horizon_{horizon}.net_return", value, metric_unit)
        for horizon, value in zip(PHASE2_REQUIRED_HORIZONS, returns, strict=True)
    )
    result_hash = canonical_backtest_result_hash(
        manifest=manifest,
        status=BacktestRunStatus.COMPLETED,
        metrics=metrics,
        artifact_refs=(artifact_ref,),
        failure_reasons=(),
    )
    return BacktestResult(
        manifest=manifest,
        status=BacktestRunStatus.COMPLETED,
        result_hash=result_hash,
        metrics=metrics,
        artifact_refs=(artifact_ref,),
        failure_reasons=(),
    )


def validate_report(payload: dict[str, object]) -> None:
    schema = json.loads(
        (ROOT / "schemas" / "phase2-calibration-gate-result.schema.json").read_text()
    )
    cast(Validator, Draft202012Validator(schema)).validate(payload)
