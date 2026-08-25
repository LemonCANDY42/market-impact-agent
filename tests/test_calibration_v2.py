import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from hashlib import sha256
from pathlib import Path
from typing import Protocol, cast

import pytest
from jsonschema import Draft202012Validator, FormatChecker
from jsonschema.exceptions import ValidationError

import market_impact_agent.phase2_study as phase2_study
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
from market_impact_agent.calibration import CalibrationPartition, CalibrationVariant
from market_impact_agent.calibration_v2 import (
    CalibrationAction,
    CalibrationCellV2,
    CalibrationDecisionV2,
    CalibrationTradeEvidenceV2,
    Phase2CalibrationEvidenceV2,
    Phase2CalibrationRegistrationV2,
    assess_phase2_calibration_v2,
    canonical_phase2_registration_hash,
    load_phase2_calibration_evidence_v2,
    load_phase2_calibration_registration_v2,
    phase2_calibration_gate_result_v2_to_dict,
    phase2_calibration_registration_v2_to_dict,
)
from market_impact_agent.cli import main
from market_impact_agent.domain import Side, SignalIntent
from market_impact_agent.phase2_study import run_phase2_registration

ROOT = Path(__file__).parents[1]


class Validator(Protocol):
    def validate(self, instance: object) -> None: ...


def gate_report_hash(payload: dict[str, object]) -> str:
    identity = {
        key: value for key, value in payload.items() if key not in {"report_hash", "schema_version"}
    }
    encoded = json.dumps(identity, ensure_ascii=True, separators=(",", ":"), sort_keys=True)
    return sha256(encoded.encode("utf-8")).hexdigest()


def test_v2_gate_accepts_registered_abstentions_without_fabricated_signals() -> None:
    evidence = complete_evidence()

    result = assess_phase2_calibration_v2(evidence)

    assert result.accepted
    assert result.reasons == ()
    assert result.candidate_mean_net_return == Decimal("0.018")
    assert result.max_single_event_share == Decimal(1) / Decimal(3)
    assert CalibrationVariant.SIMPLE_HOLD in result.beat_baselines
    candidate_count = dict(result.test_trade_counts)[CalibrationVariant.EVENT_REASONING]
    assert candidate_count == 3
    validator = Draft202012Validator(
        json.loads(
            (ROOT / "schemas" / "phase2-calibration-gate-result-v2.schema.json").read_text()
        ),
        format_checker=FormatChecker(),
    )
    cast(Validator, validator).validate(phase2_calibration_gate_result_v2_to_dict(result))


@pytest.mark.parametrize(
    ("changes", "match"),
    (
        ({"candidate_mean_net_return": Decimal("-1")}, "frozen gate"),
        ({"max_single_event_share": Decimal("1")}, "frozen gate"),
        ({"beat_baselines": ()}, "frozen gate"),
        (
            {"beat_baselines": (CalibrationVariant.EVENT_REASONING,)},
            "only Phase 2 baselines",
        ),
        ({"train_event_clusters": ("cluster-0",)}, "frozen gate"),
        ({"test_event_clusters": ("cluster-2", "cluster-3")}, "frozen gate"),
    ),
)
def test_v2_accepted_gate_result_cannot_bypass_frozen_gate(
    changes: dict[str, object],
    match: str,
) -> None:
    result = assess_phase2_calibration_v2(complete_evidence())

    with pytest.raises(ValueError, match=match):
        replace(result, **changes)


def test_v2_accepted_gate_result_binds_test_trade_counts() -> None:
    result = assess_phase2_calibration_v2(complete_evidence())
    counts = dict(result.test_trade_counts)
    counts[CalibrationVariant.SIMPLE_HOLD] -= 1
    changed_counts = tuple(sorted(counts.items(), key=lambda item: item[0].value))

    with pytest.raises(ValueError, match="frozen gate"):
        replace(result, test_trade_counts=changed_counts)


@pytest.mark.parametrize("candidate_trade_count", (0, 1))
def test_v2_accepted_gate_result_requires_two_candidate_test_trades(
    candidate_trade_count: int,
) -> None:
    result = assess_phase2_calibration_v2(complete_evidence())
    counts = dict(result.test_trade_counts)
    counts[CalibrationVariant.EVENT_REASONING] = candidate_trade_count
    changed_counts = tuple(sorted(counts.items(), key=lambda item: item[0].value))
    payload = phase2_calibration_gate_result_v2_to_dict(result)
    cast(dict[str, int], payload["test_trade_counts"])["event_reasoning"] = candidate_trade_count
    report_hash = gate_report_hash(payload)

    with pytest.raises(ValueError, match="frozen gate"):
        replace(result, test_trade_counts=changed_counts, report_hash=report_hash)


@pytest.mark.parametrize("invalid_count", (2.5, True))
def test_v2_gate_result_rejects_non_integer_test_trade_count(
    invalid_count: object,
) -> None:
    result = assess_phase2_calibration_v2(complete_evidence())
    forged_count = cast(int, invalid_count)
    changed_counts = tuple(
        (variant, forged_count if variant is CalibrationVariant.EVENT_REASONING else count)
        for variant, count in result.test_trade_counts
    )
    payload = phase2_calibration_gate_result_v2_to_dict(result)
    cast(dict[str, object], payload["test_trade_counts"])["event_reasoning"] = invalid_count
    payload["report_hash"] = gate_report_hash(payload)

    with pytest.raises(ValueError, match="nonnegative integer counts"):
        replace(result, test_trade_counts=changed_counts, report_hash=payload["report_hash"])

    schema = json.loads(
        (ROOT / "schemas" / "phase2-calibration-gate-result-v2.schema.json").read_text()
    )
    validator = Draft202012Validator(schema, format_checker=FormatChecker())
    with pytest.raises(ValidationError):
        cast(Validator, validator).validate(payload)


def test_v2_gate_result_requires_boolean_accepted() -> None:
    result = assess_phase2_calibration_v2(complete_evidence())
    payload = phase2_calibration_gate_result_v2_to_dict(result)
    payload["accepted"] = 1
    payload["report_hash"] = gate_report_hash(payload)

    with pytest.raises(ValueError, match="accepted must be a boolean"):
        replace(
            result,
            accepted=cast(bool, 1),
            report_hash=payload["report_hash"],
        )

    schema = json.loads(
        (ROOT / "schemas" / "phase2-calibration-gate-result-v2.schema.json").read_text()
    )
    validator = Draft202012Validator(schema, format_checker=FormatChecker())
    with pytest.raises(ValidationError):
        cast(Validator, validator).validate(payload)


@pytest.mark.parametrize("invalid_cluster", (7, ""))
def test_v2_gate_result_requires_artifact_component_clusters(
    invalid_cluster: object,
) -> None:
    result = assess_phase2_calibration_v2(complete_evidence())
    changed_clusters = (cast(str, invalid_cluster), *result.test_event_clusters[1:])
    payload = phase2_calibration_gate_result_v2_to_dict(result)
    cast(list[object], payload["test_event_clusters"])[0] = invalid_cluster
    payload["report_hash"] = gate_report_hash(payload)

    with pytest.raises(ValueError, match="artifact component"):
        replace(
            result,
            test_event_clusters=changed_clusters,
            report_hash=payload["report_hash"],
        )

    schema = json.loads(
        (ROOT / "schemas" / "phase2-calibration-gate-result-v2.schema.json").read_text()
    )
    validator = Draft202012Validator(schema, format_checker=FormatChecker())
    with pytest.raises(ValidationError):
        cast(Validator, validator).validate(payload)


@pytest.mark.parametrize("invalid_reason", (7, ""))
def test_v2_rejected_gate_result_requires_nonempty_string_reasons(
    invalid_reason: object,
) -> None:
    evidence = complete_evidence()
    result = assess_phase2_calibration_v2(replace(evidence, trades=evidence.trades[1:]))
    changed_reasons = (cast(str, invalid_reason),)
    payload = phase2_calibration_gate_result_v2_to_dict(result)
    payload["reasons"] = [invalid_reason]
    payload["report_hash"] = gate_report_hash(payload)

    with pytest.raises(ValueError, match="nonempty strings"):
        replace(
            result,
            reasons=changed_reasons,
            report_hash=payload["report_hash"],
        )

    schema = json.loads(
        (ROOT / "schemas" / "phase2-calibration-gate-result-v2.schema.json").read_text()
    )
    validator = Draft202012Validator(schema, format_checker=FormatChecker())
    with pytest.raises(ValidationError):
        cast(Validator, validator).validate(payload)


@pytest.mark.parametrize(
    ("candidate_trade_count", "max_single_event_share"),
    (
        (2, Decimal("0.4")),
        (2, Decimal(0)),
        (3, Decimal("0.2")),
    ),
)
def test_v2_accepted_gate_result_rejects_impossible_low_concentration(
    candidate_trade_count: int,
    max_single_event_share: Decimal,
) -> None:
    result = assess_phase2_calibration_v2(complete_evidence())
    counts = dict(result.test_trade_counts)
    counts[CalibrationVariant.EVENT_REASONING] = candidate_trade_count
    changed_counts = tuple(sorted(counts.items(), key=lambda item: item[0].value))
    payload = phase2_calibration_gate_result_v2_to_dict(result)
    cast(dict[str, int], payload["test_trade_counts"])["event_reasoning"] = candidate_trade_count
    payload["max_single_event_share"] = str(max_single_event_share)
    report_hash = gate_report_hash(payload)

    with pytest.raises(ValueError, match="frozen gate"):
        replace(
            result,
            max_single_event_share=max_single_event_share,
            test_trade_counts=changed_counts,
            report_hash=report_hash,
        )

    counts = dict(result.test_trade_counts)
    counts[CalibrationVariant.SENTIMENT] = 0
    changed_counts = tuple(sorted(counts.items(), key=lambda item: item[0].value))

    with pytest.raises(ValueError, match="frozen gate"):
        replace(result, test_trade_counts=changed_counts)


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("candidate_mean_net_return", "-1"),
        ("candidate_mean_net_return", "0"),
        ("max_single_event_share", "0.5001"),
        ("max_single_event_share", "1"),
    ),
)
def test_v2_gate_result_schema_rejects_accepted_numeric_bypass(
    field: str,
    value: str,
) -> None:
    payload = phase2_calibration_gate_result_v2_to_dict(
        assess_phase2_calibration_v2(complete_evidence())
    )
    payload[field] = value
    schema = json.loads(
        (ROOT / "schemas" / "phase2-calibration-gate-result-v2.schema.json").read_text()
    )
    validator = Draft202012Validator(schema, format_checker=FormatChecker())

    with pytest.raises(ValidationError):
        cast(Validator, validator).validate(payload)


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("train_event_clusters", ["cluster-0"]),
        ("test_event_clusters", ["cluster-2", "cluster-3"]),
        ("beat_baselines", []),
    ),
)
def test_v2_gate_result_schema_rejects_accepted_cohort_bypass(
    field: str,
    value: object,
) -> None:
    result = assess_phase2_calibration_v2(complete_evidence())
    payload = phase2_calibration_gate_result_v2_to_dict(result)
    payload[field] = value
    schema = json.loads(
        (ROOT / "schemas" / "phase2-calibration-gate-result-v2.schema.json").read_text()
    )
    validator = Draft202012Validator(schema, format_checker=FormatChecker())

    with pytest.raises(ValidationError):
        cast(Validator, validator).validate(payload)


def test_v2_gate_result_schema_requires_active_beaten_baseline() -> None:
    result = assess_phase2_calibration_v2(complete_evidence())
    payload = phase2_calibration_gate_result_v2_to_dict(result)
    trade_counts = cast(dict[str, int], payload["test_trade_counts"])
    trade_counts["sentiment"] = 0
    schema = json.loads(
        (ROOT / "schemas" / "phase2-calibration-gate-result-v2.schema.json").read_text()
    )
    validator = Draft202012Validator(schema, format_checker=FormatChecker())

    with pytest.raises(ValidationError):
        cast(Validator, validator).validate(payload)


@pytest.mark.parametrize("candidate_trade_count", (0, 1))
def test_v2_gate_result_schema_requires_two_candidate_test_trades(
    candidate_trade_count: int,
) -> None:
    result = assess_phase2_calibration_v2(complete_evidence())
    payload = phase2_calibration_gate_result_v2_to_dict(result)
    trade_counts = cast(dict[str, int], payload["test_trade_counts"])
    trade_counts["event_reasoning"] = candidate_trade_count
    payload["report_hash"] = gate_report_hash(payload)
    schema = json.loads(
        (ROOT / "schemas" / "phase2-calibration-gate-result-v2.schema.json").read_text()
    )
    validator = Draft202012Validator(schema, format_checker=FormatChecker())

    with pytest.raises(ValidationError):
        cast(Validator, validator).validate(payload)


def test_v2_gate_result_schema_rejects_zero_accepted_concentration() -> None:
    result = assess_phase2_calibration_v2(complete_evidence())
    payload = phase2_calibration_gate_result_v2_to_dict(result)
    payload["max_single_event_share"] = "0"
    payload["report_hash"] = gate_report_hash(payload)
    schema = json.loads(
        (ROOT / "schemas" / "phase2-calibration-gate-result-v2.schema.json").read_text()
    )
    validator = Draft202012Validator(schema, format_checker=FormatChecker())

    with pytest.raises(ValidationError):
        cast(Validator, validator).validate(payload)


def test_v2_rejected_gate_result_remains_representable() -> None:
    evidence = complete_evidence()
    result = assess_phase2_calibration_v2(replace(evidence, trades=evidence.trades[1:]))
    schema = json.loads(
        (ROOT / "schemas" / "phase2-calibration-gate-result-v2.schema.json").read_text()
    )
    validator = Draft202012Validator(schema, format_checker=FormatChecker())

    assert not result.accepted
    assert result.reasons
    cast(Validator, validator).validate(phase2_calibration_gate_result_v2_to_dict(result))


@pytest.mark.parametrize(
    "unsafe_cluster_id",
    ("/absolute", "../traversal", "nested/cluster", "cluster_1", "Cluster-1", "cluster--1"),
)
def test_v2_cluster_id_must_be_a_safe_artifact_component(unsafe_cluster_id: str) -> None:
    item_cell = cell(0)
    request = trade_request(item_cell, CalibrationVariant.EVENT_REASONING)

    with pytest.raises(ValueError, match="artifact component"):
        replace(item_cell, event_cluster_id=unsafe_cluster_id)
    with pytest.raises(ValueError, match="artifact component"):
        CalibrationDecisionV2(
            event_cluster_id=unsafe_cluster_id,
            variant=CalibrationVariant.EVENT_REASONING,
            action=CalibrationAction.BUY,
            rule_ref="event_reasoning.v1",
            decision_input_hashes=("a" * 64,),
            request=request,
            request_hash=canonical_backtest_request_hash(request),
        )


def test_v2_registration_schema_rejects_unsafe_cluster_and_sell_buy_decision() -> None:
    payload = phase2_calibration_registration_v2_to_dict(complete_evidence().registration)
    schema = json.loads(
        (ROOT / "schemas" / "phase2-calibration-registration-v2.schema.json").read_text()
    )
    validator = Draft202012Validator(schema, format_checker=FormatChecker())

    unsafe_payload = json.loads(json.dumps(payload))
    unsafe_payload["cells"][0]["event_cluster_id"] = "../escape"
    with pytest.raises(ValidationError):
        cast(Validator, validator).validate(unsafe_payload)

    sell_payload = json.loads(json.dumps(payload))
    sell_payload["decisions"][0]["request"]["signal"]["side"] = "sell"
    with pytest.raises(ValidationError):
        cast(Validator, validator).validate(sell_payload)


def test_v2_buy_decision_and_gate_reject_sell_signal() -> None:
    item_cell = cell(0)
    buy_request = trade_request(item_cell, CalibrationVariant.EVENT_REASONING)
    sell_request = replace(buy_request, signal=replace(buy_request.signal, side=Side.SELL))
    with pytest.raises(ValueError, match="requires a BUY signal"):
        CalibrationDecisionV2(
            event_cluster_id=item_cell.event_cluster_id,
            variant=CalibrationVariant.EVENT_REASONING,
            action=CalibrationAction.BUY,
            rule_ref="event_reasoning.v1",
            decision_input_hashes=("a" * 64,),
            request=sell_request,
            request_hash=canonical_backtest_request_hash(sell_request),
        )

    evidence = complete_evidence()
    target = evidence.registration.decisions[0]
    assert target.request is not None
    object.__setattr__(target.request.signal, "side", Side.SELL)

    result_value = assess_phase2_calibration_v2(evidence)

    assert (
        f"request_cell_mismatch:{target.event_cluster_id}:{target.variant.value}"
        in result_value.reasons
    )


@pytest.mark.parametrize(
    ("repeat", "field"),
    ((False, "run_id"), (False, "executed_at"), (True, "run_id"), (True, "executed_at")),
)
def test_v2_evidence_and_report_hashes_bind_run_identity_and_time(
    repeat: bool,
    field: str,
) -> None:
    evidence = complete_evidence()
    original = assess_phase2_calibration_v2(evidence)
    reordered = assess_phase2_calibration_v2(
        replace(evidence, trades=tuple(reversed(evidence.trades)))
    )
    assert reordered.evidence_hash == original.evidence_hash
    assert reordered.report_hash == original.report_hash

    target = evidence.trades[0]
    target_result = target.repeat if repeat else target.first
    old_manifest = target_result.manifest
    changed_value = (
        f"{old_manifest.run_id}-changed"
        if field == "run_id"
        else old_manifest.executed_at + timedelta(minutes=1)
    )
    changed_manifest = replace(old_manifest, **{field: changed_value})
    changed_result = replace(target_result, manifest=changed_manifest)
    changed_trade = replace(target, **{"repeat" if repeat else "first": changed_result})
    changed_evidence = replace(
        evidence,
        trades=tuple(changed_trade if item is target else item for item in evidence.trades),
    )

    changed = assess_phase2_calibration_v2(changed_evidence)

    assert changed.evidence_hash != original.evidence_hash
    assert changed.report_hash != original.report_hash


def test_v2_gate_rejects_result_for_registered_abstention() -> None:
    registered_at = datetime(2026, 8, 25, 17, tzinfo=UTC)
    cells = tuple(cell(index) for index in range(5))
    decisions: list[CalibrationDecisionV2] = []
    trades: list[CalibrationTradeEvidenceV2] = []
    abstain_key = (cells[-1].event_cluster_id, CalibrationVariant.MOMENTUM)
    abstain_decision: CalibrationDecisionV2 | None = None
    for item_cell in cells:
        for variant in CalibrationVariant:
            action = (
                CalibrationAction.ABSTAIN
                if (item_cell.event_cluster_id, variant) == abstain_key
                else CalibrationAction.BUY
            )
            request = trade_request(item_cell, variant) if action == CalibrationAction.BUY else None
            decision = CalibrationDecisionV2(
                event_cluster_id=item_cell.event_cluster_id,
                variant=variant,
                action=action,
                rule_ref=f"{variant.value}.v1",
                decision_input_hashes=("a" * 64,),
                request=request,
                request_hash=(canonical_backtest_request_hash(request) if request else None),
            )
            decisions.append(decision)
            if action == CalibrationAction.ABSTAIN:
                abstain_decision = decision
            else:
                trades.append(trade_evidence(decision, item_cell, Decimal("0.01"), registered_at))
    assert abstain_decision is not None
    fabricated_request = trade_request(cells[-1], CalibrationVariant.MOMENTUM)
    fabricated = CalibrationDecisionV2(
        event_cluster_id=abstain_decision.event_cluster_id,
        variant=abstain_decision.variant,
        action=CalibrationAction.BUY,
        rule_ref=abstain_decision.rule_ref,
        decision_input_hashes=abstain_decision.decision_input_hashes,
        request=fabricated_request,
        request_hash=canonical_backtest_request_hash(fabricated_request),
    )
    trades.append(trade_evidence(fabricated, cells[-1], Decimal("0.01"), registered_at))
    registration_hash = canonical_phase2_registration_hash(
        registration_id="generated-v2",
        registered_at=registered_at,
        source_registration_ref="generated.json",
        source_registration_sha256="b" * 64,
        cells=cells,
        decisions=tuple(decisions),
    )
    registration = Phase2CalibrationRegistrationV2(
        registration_id="generated-v2",
        registered_at=registered_at,
        source_registration_ref="generated.json",
        source_registration_sha256="b" * 64,
        cells=cells,
        decisions=tuple(decisions),
        registration_hash=registration_hash,
    )

    result = assess_phase2_calibration_v2(
        Phase2CalibrationEvidenceV2(registration=registration, trades=tuple(trades))
    )

    assert f"unexpected_trade_evidence:{abstain_key[0]}:momentum" in result.reasons


def test_v2_gate_rejects_missing_and_nondeterministic_registered_trades() -> None:
    evidence = complete_evidence()
    missing = evidence.trades[0]
    missing_result = assess_phase2_calibration_v2(replace(evidence, trades=evidence.trades[1:]))
    assert (
        f"missing_trade_evidence:{missing.event_cluster_id}:{missing.variant.value}"
        in missing_result.reasons
    )

    target = next(
        item
        for item in evidence.trades
        if item.variant is CalibrationVariant.EVENT_REASONING
        and item.event_cluster_id == "cluster-2"
    )
    target_cell = next(
        item
        for item in evidence.registration.cells
        if item.event_cluster_id == target.event_cluster_id
    )
    changed_repeat = result(
        target.first.manifest.request,
        target_cell,
        Decimal("0.04"),
        evidence.registration.registered_at,
        run_number=2,
    )
    changed_trades = tuple(
        replace(item, repeat=changed_repeat) if item is target else item for item in evidence.trades
    )
    changed_result = assess_phase2_calibration_v2(replace(evidence, trades=changed_trades))
    assert "nondeterministic_repeat:cluster-2:event_reasoning" in changed_result.reasons


def test_v2_registration_and_evidence_round_trip_are_closed_and_relative(
    tmp_path: Path,
) -> None:
    evidence = complete_evidence()
    registration_path = tmp_path / "registration.json"
    registration_path.write_text(
        json.dumps(phase2_calibration_registration_v2_to_dict(evidence.registration)),
        encoding="utf-8",
    )
    trades: list[dict[str, str]] = []
    for index, item in enumerate(evidence.trades):
        first_name = f"first-{index}.json"
        repeat_name = f"repeat-{index}.json"
        (tmp_path / first_name).write_text(
            json.dumps(backtest_result_to_dict(item.first)),
            encoding="utf-8",
        )
        (tmp_path / repeat_name).write_text(
            json.dumps(backtest_result_to_dict(item.repeat)),
            encoding="utf-8",
        )
        trades.append(
            {
                "event_cluster_id": item.event_cluster_id,
                "variant": item.variant.value,
                "first_result": first_name,
                "repeat_result": repeat_name,
            }
        )
    evidence_path = tmp_path / "evidence.json"
    evidence_path.write_text(
        json.dumps(
            {
                "schema_version": "market-impact.phase2-calibration-evidence.v2",
                "registration": "registration.json",
                "trades": trades,
            }
        ),
        encoding="utf-8",
    )

    loaded_registration = load_phase2_calibration_registration_v2(registration_path)
    loaded_evidence = load_phase2_calibration_evidence_v2(evidence_path)

    assert loaded_registration.registration_hash == evidence.registration.registration_hash
    assert assess_phase2_calibration_v2(loaded_evidence).accepted

    escaped = json.loads(evidence_path.read_text())
    escaped["registration"] = "../registration.json"
    evidence_path.write_text(json.dumps(escaped), encoding="utf-8")
    try:
        load_phase2_calibration_evidence_v2(evidence_path)
    except ValueError as exc:
        assert "must stay below" in str(exc)
    else:
        raise AssertionError("v2 registration path escape must fail closed")


def test_phase2_run_rechecks_artifact_component_before_replay(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    evidence = complete_evidence()
    forged_decision = evidence.registration.decisions[0]
    object.__setattr__(forged_decision, "event_cluster_id", "../escape")
    output_dir = tmp_path / "results"
    output_dir.mkdir()
    registration_path = output_dir / "registration.json"
    registration_path.write_text("{}", encoding="utf-8")

    def load_registration(_: Path) -> Phase2CalibrationRegistrationV2:
        return evidence.registration

    monkeypatch.setattr(
        phase2_study,
        "load_phase2_calibration_registration_v2",
        load_registration,
    )

    def unexpected_call(*_: object) -> None:
        raise AssertionError("unsafe event_cluster_id must fail before private data access")

    monkeypatch.setattr(phase2_study, "validate_tushare_data_bundle", unexpected_call)
    monkeypatch.setattr(phase2_study, "run_validated_tushare_replay", unexpected_call)

    with pytest.raises(ValueError, match="artifact component"):
        run_phase2_registration(
            registration_path=registration_path,
            data_snapshot_root=tmp_path / "snapshots",
            output_dir=output_dir,
        )

    assert not (tmp_path / "escape.event_reasoning.run-1.json").exists()


def test_phase2_run_cli_fails_batch_and_preserves_failed_result(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    evidence = complete_evidence()
    registration_path = tmp_path / "registration.json"
    registration_path.write_text(
        json.dumps(phase2_calibration_registration_v2_to_dict(evidence.registration)),
        encoding="utf-8",
    )
    failed = failed_result(evidence.trades[0].first)

    def validate_bundle(_: Path) -> None:
        return None

    def failed_replay(_request: BacktestRequest, _bundle: Path) -> BacktestResult:
        return failed

    monkeypatch.setattr(phase2_study, "validate_tushare_data_bundle", validate_bundle)
    monkeypatch.setattr(
        phase2_study,
        "run_validated_tushare_replay",
        failed_replay,
    )
    output_dir = tmp_path / "results"

    exit_code = main(
        [
            "backtest",
            "phase2-run",
            "--registration",
            str(registration_path),
            "--data-snapshot-root",
            str(tmp_path / "snapshots"),
            "--output-dir",
            str(output_dir),
        ]
    )

    captured = capsys.readouterr()
    assert exit_code == 1
    assert captured.out == ""
    assert json.loads(captured.err)["completed"] is False
    assert not (output_dir / "evidence.json").exists()
    failed_paths = tuple(output_dir.glob("*.run-1.json"))
    assert len(failed_paths) == 1
    assert json.loads(failed_paths[0].read_text(encoding="utf-8"))["status"] == "failed"


def complete_evidence() -> Phase2CalibrationEvidenceV2:
    registered_at = datetime(2026, 8, 25, 17, tzinfo=UTC)
    cells = tuple(cell(index) for index in range(7))
    decisions: list[CalibrationDecisionV2] = []
    trades: list[CalibrationTradeEvidenceV2] = []
    candidate_actions = (
        CalibrationAction.BUY,
        CalibrationAction.ABSTAIN,
        CalibrationAction.BUY,
        CalibrationAction.ABSTAIN,
        CalibrationAction.BUY,
        CalibrationAction.ABSTAIN,
        CalibrationAction.BUY,
    )
    for index, item_cell in enumerate(cells):
        for variant in CalibrationVariant:
            action = (
                candidate_actions[index]
                if variant is CalibrationVariant.EVENT_REASONING
                else CalibrationAction.BUY
            )
            request = trade_request(item_cell, variant) if action == CalibrationAction.BUY else None
            decision = CalibrationDecisionV2(
                event_cluster_id=item_cell.event_cluster_id,
                variant=variant,
                action=action,
                rule_ref=f"{variant.value}.v1",
                decision_input_hashes=("a" * 64,),
                request=request,
                request_hash=(canonical_backtest_request_hash(request) if request else None),
            )
            decisions.append(decision)
            if request is not None:
                value = (
                    Decimal("0.03")
                    if variant is CalibrationVariant.EVENT_REASONING and index >= 2
                    else Decimal(0)
                )
                trades.append(trade_evidence(decision, item_cell, value, registered_at))
    registration_hash = canonical_phase2_registration_hash(
        registration_id="generated-v2",
        registered_at=registered_at,
        source_registration_ref="examples/calibration/generated.json",
        source_registration_sha256="b" * 64,
        cells=cells,
        decisions=tuple(decisions),
    )
    registration = Phase2CalibrationRegistrationV2(
        registration_id="generated-v2",
        registered_at=registered_at,
        source_registration_ref="examples/calibration/generated.json",
        source_registration_sha256="b" * 64,
        cells=cells,
        decisions=tuple(decisions),
        registration_hash=registration_hash,
    )
    return Phase2CalibrationEvidenceV2(registration=registration, trades=tuple(trades))


def cell(index: int) -> CalibrationCellV2:
    visible_at = datetime(2018, 1, 1, tzinfo=UTC) + timedelta(days=index * 365)
    return CalibrationCellV2(
        event_cluster_id=f"cluster-{index}",
        visible_at=visible_at,
        partition=CalibrationPartition.TRAIN if index < 2 else CalibrationPartition.TEST,
        target_selection_ref="registered-a-share-integrated-oil-proxy:600028.v1",
        as_of=visible_at,
        start_at=visible_at + timedelta(days=1),
        end_at=visible_at + timedelta(days=20),
        market="CN",
        instrument_ids=("600028.XSHG",),
        data_snapshot_id=f"snapshot-{index}",
        horizons_sessions=(1, 3, 10),
        simulation=SimulationSpec(
            data_granularity="tushare_unadjusted_daily_with_source_limits.v2",
            book_type="modeled_open_one_lot.v1",
            fill_model="modeled_open_one_lot_no_slippage.v1",
            fee_model="xshg_2019_fee_assumption.v1",
            venue_ruleset="xshg_main_board_source_limit.v2",
            base_currency="CNY",
            starting_cash=Decimal("1000000"),
            random_seed=0,
        ),
    )


def trade_request(item_cell: CalibrationCellV2, variant: CalibrationVariant) -> BacktestRequest:
    return BacktestRequest(
        request_id=f"{item_cell.event_cluster_id}-{variant.value}",
        signal=SignalIntent(
            signal_id=f"{item_cell.event_cluster_id}-{variant.value}-signal",
            event_id=item_cell.event_cluster_id,
            instrument_id="600028.XSHG",
            side=Side.BUY,
            valid_from=item_cell.as_of,
            expires_at=item_cell.end_at + timedelta(days=1),
            evidence_refs=(f"evidence:{item_cell.event_cluster_id}",),
            invalidation_conditions=("registered rule invalidated",),
        ),
        as_of=item_cell.as_of,
        start_at=item_cell.start_at,
        end_at=item_cell.end_at,
        market=item_cell.market,
        instrument_ids=item_cell.instrument_ids,
        data_snapshot_id=item_cell.data_snapshot_id,
        target_selection_ref=item_cell.target_selection_ref,
        strategy_ref="event-impact-hold.v1",
        horizons_sessions=item_cell.horizons_sessions,
        simulation=item_cell.simulation,
    )


def trade_evidence(
    decision: CalibrationDecisionV2,
    item_cell: CalibrationCellV2,
    value: Decimal,
    registered_at: datetime,
) -> CalibrationTradeEvidenceV2:
    assert decision.request is not None
    first = result(decision.request, item_cell, value, registered_at, run_number=1)
    repeat = result(decision.request, item_cell, value, registered_at, run_number=2)
    return CalibrationTradeEvidenceV2(
        event_cluster_id=item_cell.event_cluster_id,
        variant=decision.variant,
        first=first,
        repeat=repeat,
    )


def result(
    request: BacktestRequest,
    item_cell: CalibrationCellV2,
    value: Decimal,
    registered_at: datetime,
    *,
    run_number: int,
) -> BacktestResult:
    manifest = BacktestRunManifest(
        run_id=f"{request.request_id}-run-{run_number}",
        request=request,
        request_hash=canonical_backtest_request_hash(request),
        engine_name="nautilus_trader",
        engine_version="1.231.0",
        bridge_name="nautilus-backtest",
        bridge_version="0.3.0",
        data_adapter_name="tushare-xshg-modeled-open",
        data_adapter_version="2.0.0",
        input_hashes=(BacktestInputHash("bundle", "c" * 64),),
        engine_config_hash="d" * 64,
        executed_at=registered_at + timedelta(seconds=run_number),
    )
    metrics = tuple(
        BacktestMetric(f"horizon_{horizon}.net_return", value, "ratio") for horizon in (1, 3, 10)
    )
    artifact_refs = (f"snapshot://{item_cell.data_snapshot_id}",)
    result_hash = canonical_backtest_result_hash(
        manifest=manifest,
        status=BacktestRunStatus.COMPLETED,
        metrics=metrics,
        artifact_refs=artifact_refs,
        failure_reasons=(),
    )
    return BacktestResult(
        manifest=manifest,
        status=BacktestRunStatus.COMPLETED,
        result_hash=result_hash,
        metrics=metrics,
        artifact_refs=artifact_refs,
        failure_reasons=(),
    )


def failed_result(source: BacktestResult) -> BacktestResult:
    failure_reasons = ("mock replay failure",)
    result_hash = canonical_backtest_result_hash(
        manifest=source.manifest,
        status=BacktestRunStatus.FAILED,
        metrics=(),
        artifact_refs=source.artifact_refs,
        failure_reasons=failure_reasons,
    )
    return BacktestResult(
        manifest=source.manifest,
        status=BacktestRunStatus.FAILED,
        result_hash=result_hash,
        metrics=(),
        artifact_refs=source.artifact_refs,
        failure_reasons=failure_reasons,
    )
