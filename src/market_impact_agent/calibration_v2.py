from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from hashlib import sha256
from pathlib import Path
from typing import cast

from market_impact_agent.backtests import (
    BacktestRequest,
    BacktestResult,
    BacktestRunStatus,
    SimulationSpec,
    backtest_request_from_dict,
    backtest_request_to_dict,
    backtest_result_from_dict,
    canonical_backtest_request_hash,
)
from market_impact_agent.calibration import (
    PHASE2_MAX_SINGLE_EVENT_SHARE,
    PHASE2_MINIMUM_TEST_CLUSTERS,
    PHASE2_MINIMUM_TRAIN_CLUSTERS,
    PHASE2_REQUIRED_BASELINES,
    PHASE2_REQUIRED_HORIZONS,
    CalibrationPartition,
    CalibrationVariant,
)
from market_impact_agent.domain import Side, require_aware

PHASE2_CALIBRATION_GATE_V2 = "energy-supply-shock-calibration.v2"
PHASE2_REGISTRATION_SCHEMA = "market-impact.phase2-calibration-registration.v2"
PHASE2_EVIDENCE_SCHEMA_V2 = "market-impact.phase2-calibration-evidence.v2"
PHASE2_GATE_RESULT_SCHEMA_V2 = "market-impact.phase2-calibration-gate-result.v2"
_SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_ARTIFACT_COMPONENT_PATTERN = re.compile(r"^[a-z0-9]+(?:-[a-z0-9]+)*$")
_REQUIRED_VARIANTS = (CalibrationVariant.EVENT_REASONING, *PHASE2_REQUIRED_BASELINES)


class CalibrationAction(str):
    BUY = "buy"
    ABSTAIN = "abstain"


@dataclass(frozen=True, slots=True)
class CalibrationCellV2:
    event_cluster_id: str
    visible_at: datetime
    partition: CalibrationPartition
    target_selection_ref: str
    as_of: datetime
    start_at: datetime
    end_at: datetime
    market: str
    instrument_ids: tuple[str, ...]
    data_snapshot_id: str
    horizons_sessions: tuple[int, ...]
    simulation: SimulationSpec

    def __post_init__(self) -> None:
        _require_artifact_component(self.event_cluster_id, "event_cluster_id")
        for name in (
            "target_selection_ref",
            "market",
            "data_snapshot_id",
        ):
            if not getattr(self, name):
                raise ValueError(f"{name} must not be empty")
        for name in ("visible_at", "as_of", "start_at", "end_at"):
            require_aware(getattr(self, name), name)
        if self.visible_at != self.as_of:
            raise ValueError("calibration cell visible_at must equal as_of")
        if self.start_at < self.as_of or self.end_at <= self.start_at:
            raise ValueError("calibration cell evaluation window is invalid")
        if self.horizons_sessions != PHASE2_REQUIRED_HORIZONS:
            raise ValueError("calibration cell must use the Phase 2 horizons")
        if not self.instrument_ids or len(self.instrument_ids) != len(set(self.instrument_ids)):
            raise ValueError("calibration cell instrument_ids must be unique and non-empty")


@dataclass(frozen=True, slots=True)
class CalibrationDecisionV2:
    event_cluster_id: str
    variant: CalibrationVariant
    action: str
    rule_ref: str
    decision_input_hashes: tuple[str, ...]
    request: BacktestRequest | None
    request_hash: str | None

    def __post_init__(self) -> None:
        _require_artifact_component(self.event_cluster_id, "event_cluster_id")
        if self.action not in {CalibrationAction.BUY, CalibrationAction.ABSTAIN}:
            raise ValueError("calibration action must be buy or abstain")
        if not self.rule_ref:
            raise ValueError("rule_ref must not be empty")
        if (
            not self.decision_input_hashes
            or self.decision_input_hashes != tuple(sorted(set(self.decision_input_hashes)))
            or any(_SHA256_PATTERN.fullmatch(value) is None for value in self.decision_input_hashes)
        ):
            raise ValueError("decision_input_hashes must be unique sorted SHA-256 values")
        if self.action == CalibrationAction.BUY:
            if self.request is None or self.request_hash is None:
                raise ValueError("buy decision requires an exact Backtest Request")
            if self.request.signal.side is not Side.BUY:
                raise ValueError("buy decision requires a BUY signal")
            if self.request_hash != canonical_backtest_request_hash(self.request):
                raise ValueError("decision request_hash does not match its request")
        elif self.request is not None or self.request_hash is not None:
            raise ValueError("abstain decision must not fabricate a Backtest Request")


@dataclass(frozen=True, slots=True)
class Phase2CalibrationRegistrationV2:
    registration_id: str
    registered_at: datetime
    source_registration_ref: str
    source_registration_sha256: str
    cells: tuple[CalibrationCellV2, ...]
    decisions: tuple[CalibrationDecisionV2, ...]
    registration_hash: str

    def __post_init__(self) -> None:
        if not self.registration_id or not self.source_registration_ref:
            raise ValueError("registration identity fields must not be empty")
        require_aware(self.registered_at, "registered_at")
        _require_sha256(self.source_registration_sha256, "source_registration_sha256")
        _require_sha256(self.registration_hash, "registration_hash")
        cell_ids = tuple(cell.event_cluster_id for cell in self.cells)
        if not cell_ids or len(cell_ids) != len(set(cell_ids)):
            raise ValueError("registration cells must be unique and non-empty")
        decision_keys = tuple((item.event_cluster_id, item.variant) for item in self.decisions)
        if len(decision_keys) != len(set(decision_keys)):
            raise ValueError("registration decisions must be unique")
        expected_keys = {
            (cell.event_cluster_id, variant)
            for cell in self.cells
            for variant in _REQUIRED_VARIANTS
        }
        if set(decision_keys) != expected_keys:
            raise ValueError("registration must contain every cell and variant decision exactly")
        cells = {cell.event_cluster_id: cell for cell in self.cells}
        rule_refs: dict[CalibrationVariant, str] = {}
        for decision in self.decisions:
            cell = cells[decision.event_cluster_id]
            known_rule = rule_refs.setdefault(decision.variant, decision.rule_ref)
            if known_rule != decision.rule_ref:
                raise ValueError(f"variant rule changed: {decision.variant.value}")
            if decision.variant is CalibrationVariant.SIMPLE_HOLD and (
                decision.action != CalibrationAction.BUY
            ):
                raise ValueError("simple_hold must trade every registered cell")
            if decision.request is not None:
                _validate_request_against_cell(decision.request, cell)
        expected_hash = _canonical_hash(
            _registration_identity_payload(
                registration_id=self.registration_id,
                registered_at=self.registered_at,
                source_registration_ref=self.source_registration_ref,
                source_registration_sha256=self.source_registration_sha256,
                cells=self.cells,
                decisions=self.decisions,
            )
        )
        if self.registration_hash != expected_hash:
            raise ValueError("registration_hash does not match canonical registration content")


@dataclass(frozen=True, slots=True)
class CalibrationTradeEvidenceV2:
    event_cluster_id: str
    variant: CalibrationVariant
    first: BacktestResult
    repeat: BacktestResult


@dataclass(frozen=True, slots=True)
class Phase2CalibrationEvidenceV2:
    registration: Phase2CalibrationRegistrationV2
    trades: tuple[CalibrationTradeEvidenceV2, ...]


@dataclass(frozen=True, slots=True)
class Phase2CalibrationGateResultV2:
    accepted: bool
    registration_hash: str
    evidence_hash: str
    report_hash: str
    train_event_clusters: tuple[str, ...]
    test_event_clusters: tuple[str, ...]
    beat_baselines: tuple[CalibrationVariant, ...]
    candidate_mean_net_return: Decimal | None
    max_single_event_share: Decimal | None
    test_trade_counts: tuple[tuple[CalibrationVariant, int], ...]
    reasons: tuple[str, ...]

    def __post_init__(self) -> None:
        if type(self.accepted) is not bool:
            raise ValueError("accepted must be a boolean")
        for name in ("registration_hash", "evidence_hash", "report_hash"):
            _require_sha256(getattr(self, name), name)
        for name, clusters in (
            ("train_event_clusters", self.train_event_clusters),
            ("test_event_clusters", self.test_event_clusters),
        ):
            for cluster in clusters:
                _require_artifact_component(cluster, f"{name} entries")
        if self.train_event_clusters != tuple(sorted(set(self.train_event_clusters))):
            raise ValueError("train_event_clusters must be unique and sorted")
        if self.test_event_clusters != tuple(sorted(set(self.test_event_clusters))):
            raise ValueError("test_event_clusters must be unique and sorted")
        if set(self.train_event_clusters) & set(self.test_event_clusters):
            raise ValueError("calibration clusters cannot cross partitions")
        if self.beat_baselines != tuple(sorted(set(self.beat_baselines), key=lambda x: x.value)):
            raise ValueError("beat_baselines must be unique and sorted")
        if any(item not in PHASE2_REQUIRED_BASELINES for item in self.beat_baselines):
            raise ValueError("beat_baselines must contain only Phase 2 baselines")
        if any(type(reason) is not str or not reason for reason in self.reasons):
            raise ValueError("gate reasons must be nonempty strings")
        if self.reasons != tuple(sorted(set(self.reasons))):
            raise ValueError("gate reasons must be unique and sorted")
        if self.accepted == bool(self.reasons):
            raise ValueError("accepted must be true exactly when reasons are empty")
        for name, value in (
            ("candidate_mean_net_return", self.candidate_mean_net_return),
            ("max_single_event_share", self.max_single_event_share),
        ):
            if value is not None and not value.is_finite():
                raise ValueError(f"{name} must be finite when present")
        if self.max_single_event_share is not None and not (
            Decimal(0) <= self.max_single_event_share <= Decimal(1)
        ):
            raise ValueError("max_single_event_share must be between zero and one")
        expected_counts = tuple(sorted(_REQUIRED_VARIANTS, key=lambda item: item.value))
        actual_counts = tuple(variant for variant, _ in self.test_trade_counts)
        invalid_count = any(
            type(count) is not int or count < 0 for _, count in self.test_trade_counts
        )
        if actual_counts != expected_counts or invalid_count:
            raise ValueError(
                "test_trade_counts must cover every variant in sorted order with "
                "nonnegative integer counts"
            )
        counts = dict(self.test_trade_counts)
        test_cluster_count = len(self.test_event_clusters)
        candidate_count = counts[CalibrationVariant.EVENT_REASONING]
        if self.accepted and (
            len(self.train_event_clusters) < PHASE2_MINIMUM_TRAIN_CLUSTERS
            or test_cluster_count < PHASE2_MINIMUM_TEST_CLUSTERS
            or not self.beat_baselines
            or self.candidate_mean_net_return is None
            or self.candidate_mean_net_return <= 0
            or self.max_single_event_share is None
            or self.max_single_event_share > PHASE2_MAX_SINGLE_EVENT_SHARE
            or any(count > test_cluster_count for count in counts.values())
            or candidate_count < 2
            or self.max_single_event_share < Decimal(1) / Decimal(candidate_count)
            or counts[CalibrationVariant.SIMPLE_HOLD] != test_cluster_count
            or any(counts[baseline] == 0 for baseline in self.beat_baselines)
        ):
            raise ValueError("accepted calibration result does not clear the frozen gate")
        expected_report_hash = _canonical_hash(_gate_result_identity_payload(self))
        if self.report_hash != expected_report_hash:
            raise ValueError("report_hash does not match canonical gate result content")


def load_phase2_calibration_registration_v2(path: Path) -> Phase2CalibrationRegistrationV2:
    payload = _object(json.loads(path.read_text(encoding="utf-8")), "registration")
    _require_exact_keys(
        payload,
        {
            "cells",
            "decisions",
            "gate_version",
            "registered_at",
            "registration_hash",
            "registration_id",
            "schema_version",
            "source_registration_ref",
            "source_registration_sha256",
        },
        "registration",
    )
    if _string(payload, "schema_version") != PHASE2_REGISTRATION_SCHEMA:
        raise ValueError("unsupported Phase 2 registration schema_version")
    if _string(payload, "gate_version") != PHASE2_CALIBRATION_GATE_V2:
        raise ValueError("unsupported Phase 2 registration gate_version")
    cells = tuple(_cell_from_dict(item) for item in _array(payload, "cells"))
    decisions = tuple(_decision_from_dict(item) for item in _array(payload, "decisions"))
    return Phase2CalibrationRegistrationV2(
        registration_id=_string(payload, "registration_id"),
        registered_at=_timestamp(payload, "registered_at"),
        source_registration_ref=_string(payload, "source_registration_ref"),
        source_registration_sha256=_string(payload, "source_registration_sha256"),
        cells=cells,
        decisions=decisions,
        registration_hash=_string(payload, "registration_hash"),
    )


def load_phase2_calibration_evidence_v2(path: Path) -> Phase2CalibrationEvidenceV2:
    payload = _object(json.loads(path.read_text(encoding="utf-8")), "evidence")
    _require_exact_keys(payload, {"registration", "schema_version", "trades"}, "evidence")
    if _string(payload, "schema_version") != PHASE2_EVIDENCE_SCHEMA_V2:
        raise ValueError("unsupported Phase 2 evidence schema_version")
    registration_path = _safe_relative_file(path.parent, _string(payload, "registration"))
    registration = load_phase2_calibration_registration_v2(registration_path)
    trades: list[CalibrationTradeEvidenceV2] = []
    for raw in _array(payload, "trades"):
        trade = _object(raw, "trade evidence")
        _require_exact_keys(
            trade,
            {"event_cluster_id", "first_result", "repeat_result", "variant"},
            "trade evidence",
        )
        trades.append(
            CalibrationTradeEvidenceV2(
                event_cluster_id=_string(trade, "event_cluster_id"),
                variant=CalibrationVariant(_string(trade, "variant")),
                first=_load_result(path.parent, _string(trade, "first_result")),
                repeat=_load_result(path.parent, _string(trade, "repeat_result")),
            )
        )
    return Phase2CalibrationEvidenceV2(registration=registration, trades=tuple(trades))


def assess_phase2_calibration_v2(
    evidence: Phase2CalibrationEvidenceV2,
) -> Phase2CalibrationGateResultV2:
    registration = evidence.registration
    reasons: set[str] = set()
    cells = {cell.event_cluster_id: cell for cell in registration.cells}
    decisions = {(item.event_cluster_id, item.variant): item for item in registration.decisions}
    trades: dict[tuple[str, CalibrationVariant], CalibrationTradeEvidenceV2] = {}
    for item in evidence.trades:
        key = (item.event_cluster_id, item.variant)
        if key in trades:
            reasons.add(f"duplicate_trade_evidence:{item.event_cluster_id}:{item.variant.value}")
        else:
            trades[key] = item

    expected_trade_keys = {
        key for key, item in decisions.items() if item.action == CalibrationAction.BUY
    }
    for cluster, variant in sorted(expected_trade_keys - set(trades), key=_decision_key_sort):
        reasons.add(f"missing_trade_evidence:{cluster}:{variant.value}")
    for cluster, variant in sorted(set(trades) - expected_trade_keys, key=_decision_key_sort):
        reasons.add(f"unexpected_trade_evidence:{cluster}:{variant.value}")

    returns: dict[tuple[str, CalibrationVariant], dict[int, Decimal]] = {}
    anchors: dict[str, BacktestResult] = {}
    for key, item in trades.items():
        decision = decisions.get(key)
        cell = cells.get(item.event_cluster_id)
        if decision is None or cell is None:
            continue
        _validate_trade_pair(item, decision, cell, registration.registered_at, reasons)
        extracted = _net_returns(item.first)
        if extracted is None:
            reasons.add(f"net_return_metrics_missing:{item.event_cluster_id}:{item.variant.value}")
        else:
            returns[key] = extracted
        if item.variant is CalibrationVariant.SIMPLE_HOLD:
            anchors[item.event_cluster_id] = item.first

    for cell in registration.cells:
        anchor = anchors.get(cell.event_cluster_id)
        if anchor is None:
            reasons.add(f"missing_simple_hold_anchor:{cell.event_cluster_id}")
            continue
        for variant in _REQUIRED_VARIANTS:
            item = trades.get((cell.event_cluster_id, variant))
            if item is not None and _comparison_key(item.first) != _comparison_key(anchor):
                reasons.add(f"incomparable_variant_windows:{cell.event_cluster_id}:{variant.value}")

    for key, decision in decisions.items():
        if decision.action == CalibrationAction.ABSTAIN:
            returns[key] = {horizon: Decimal(0) for horizon in PHASE2_REQUIRED_HORIZONS}

    train_clusters = tuple(
        sorted(
            cell.event_cluster_id
            for cell in registration.cells
            if cell.partition is CalibrationPartition.TRAIN
        )
    )
    test_clusters = tuple(
        sorted(
            cell.event_cluster_id
            for cell in registration.cells
            if cell.partition is CalibrationPartition.TEST
        )
    )
    if len(train_clusters) < PHASE2_MINIMUM_TRAIN_CLUSTERS:
        reasons.add("insufficient_train_event_clusters")
    if len(test_clusters) < PHASE2_MINIMUM_TEST_CLUSTERS:
        reasons.add("insufficient_test_event_clusters")
    if train_clusters and test_clusters:
        latest_train = max(cells[cluster].visible_at for cluster in train_clusters)
        earliest_test = min(cells[cluster].visible_at for cluster in test_clusters)
        if latest_train >= earliest_test:
            reasons.add("walk_forward_order_invalid")

    candidate_values = _variant_values(test_clusters, CalibrationVariant.EVENT_REASONING, returns)
    candidate_mean = _mean(candidate_values) if candidate_values is not None else None
    if candidate_mean is None or candidate_mean <= 0:
        reasons.add("candidate_net_return_not_positive")

    test_trade_counts = tuple(
        (
            variant,
            sum(
                decisions[(cluster, variant)].action == CalibrationAction.BUY
                for cluster in test_clusters
            ),
        )
        for variant in _REQUIRED_VARIANTS
    )
    beat_baselines: list[CalibrationVariant] = []
    candidate_actions = tuple(
        decisions[(cluster, CalibrationVariant.EVENT_REASONING)].action for cluster in test_clusters
    )
    if candidate_values is not None:
        for baseline in PHASE2_REQUIRED_BASELINES:
            baseline_values = _variant_values(test_clusters, baseline, returns)
            baseline_actions = tuple(
                decisions[(cluster, baseline)].action for cluster in test_clusters
            )
            active = any(action == CalibrationAction.BUY for action in baseline_actions)
            differs = baseline_actions != candidate_actions
            if (
                baseline_values is not None
                and active
                and differs
                and _mean(candidate_values) > _mean(baseline_values)
            ):
                beat_baselines.append(baseline)
    if not beat_baselines:
        reasons.add("no_meaningful_baseline_beaten")

    contributions: list[Decimal] = []
    for cluster in test_clusters:
        values = returns.get((cluster, CalibrationVariant.EVENT_REASONING))
        if values is not None:
            contributions.append(sum((abs(value) for value in values.values()), Decimal(0)))
    total = sum(contributions, Decimal(0))
    max_share = max(contributions) / total if contributions and total > 0 else None
    if max_share is None or max_share > PHASE2_MAX_SINGLE_EVENT_SHARE:
        reasons.add("single_event_dominance_not_cleared")

    ordered_reasons = tuple(sorted(reasons))
    evidence_hash = _evidence_hash(evidence)
    ordered_baselines = tuple(sorted(beat_baselines, key=lambda item: item.value))
    counts = tuple(sorted(test_trade_counts, key=lambda item: item[0].value))
    report_payload: dict[str, object] = {
        "accepted": not ordered_reasons,
        "beat_baselines": [item.value for item in ordered_baselines],
        "candidate_mean_net_return": _optional_decimal(candidate_mean),
        "evidence_hash": evidence_hash,
        "gate_version": PHASE2_CALIBRATION_GATE_V2,
        "max_single_event_share": _optional_decimal(max_share),
        "reasons": list(ordered_reasons),
        "registration_hash": registration.registration_hash,
        "test_event_clusters": list(test_clusters),
        "test_trade_counts": {variant.value: count for variant, count in counts},
        "train_event_clusters": list(train_clusters),
    }
    return Phase2CalibrationGateResultV2(
        accepted=not ordered_reasons,
        registration_hash=registration.registration_hash,
        evidence_hash=evidence_hash,
        report_hash=_canonical_hash(report_payload),
        train_event_clusters=train_clusters,
        test_event_clusters=test_clusters,
        beat_baselines=ordered_baselines,
        candidate_mean_net_return=candidate_mean,
        max_single_event_share=max_share,
        test_trade_counts=counts,
        reasons=ordered_reasons,
    )


def phase2_calibration_gate_result_v2_to_dict(
    result: Phase2CalibrationGateResultV2,
) -> dict[str, object]:
    return {
        "schema_version": PHASE2_GATE_RESULT_SCHEMA_V2,
        "gate_version": PHASE2_CALIBRATION_GATE_V2,
        "accepted": result.accepted,
        "registration_hash": result.registration_hash,
        "evidence_hash": result.evidence_hash,
        "report_hash": result.report_hash,
        "train_event_clusters": list(result.train_event_clusters),
        "test_event_clusters": list(result.test_event_clusters),
        "beat_baselines": [item.value for item in result.beat_baselines],
        "candidate_mean_net_return": _optional_decimal(result.candidate_mean_net_return),
        "max_single_event_share": _optional_decimal(result.max_single_event_share),
        "test_trade_counts": {variant.value: count for variant, count in result.test_trade_counts},
        "reasons": list(result.reasons),
    }


def phase2_calibration_registration_v2_to_dict(
    registration: Phase2CalibrationRegistrationV2,
) -> dict[str, object]:
    return {
        "schema_version": PHASE2_REGISTRATION_SCHEMA,
        "gate_version": PHASE2_CALIBRATION_GATE_V2,
        "registration_id": registration.registration_id,
        "registered_at": registration.registered_at.isoformat().replace("+00:00", "Z"),
        "source_registration_ref": registration.source_registration_ref,
        "source_registration_sha256": registration.source_registration_sha256,
        "cells": [_cell_to_dict(cell) for cell in registration.cells],
        "decisions": [_decision_to_dict(item) for item in registration.decisions],
        "registration_hash": registration.registration_hash,
    }


def canonical_phase2_registration_hash(
    *,
    registration_id: str,
    registered_at: datetime,
    source_registration_ref: str,
    source_registration_sha256: str,
    cells: tuple[CalibrationCellV2, ...],
    decisions: tuple[CalibrationDecisionV2, ...],
) -> str:
    return _canonical_hash(
        _registration_identity_payload(
            registration_id=registration_id,
            registered_at=registered_at,
            source_registration_ref=source_registration_ref,
            source_registration_sha256=source_registration_sha256,
            cells=cells,
            decisions=decisions,
        )
    )


def _validate_trade_pair(
    item: CalibrationTradeEvidenceV2,
    decision: CalibrationDecisionV2,
    cell: CalibrationCellV2,
    registered_at: datetime,
    reasons: set[str],
) -> None:
    label = f"{item.event_cluster_id}:{item.variant.value}"
    first, repeat = item.first, item.repeat
    if first.status is not BacktestRunStatus.COMPLETED or repeat.status is not (
        BacktestRunStatus.COMPLETED
    ):
        reasons.add(f"non_completed_repeat:{label}")
    if first.manifest.request_hash != repeat.manifest.request_hash:
        reasons.add(f"repeat_request_mismatch:{label}")
    if first.result_hash != repeat.result_hash or first.metrics != repeat.metrics:
        reasons.add(f"nondeterministic_repeat:{label}")
    if first.manifest.run_id == repeat.manifest.run_id:
        reasons.add(f"repeat_run_id_reused:{label}")
    if first.manifest.executed_at == repeat.manifest.executed_at:
        reasons.add(f"repeat_execution_time_reused:{label}")
    if first.manifest.executed_at <= registered_at or repeat.manifest.executed_at <= registered_at:
        reasons.add(f"result_predates_registration:{label}")
    if decision.request_hash is None or first.manifest.request_hash != decision.request_hash:
        reasons.add(f"unregistered_request:{label}")
    if repeat.manifest.request_hash != first.manifest.request_hash:
        reasons.add(f"unregistered_repeat_request:{label}")
    try:
        _validate_request_against_cell(first.manifest.request, cell)
    except ValueError:
        reasons.add(f"request_cell_mismatch:{label}")


def _validate_request_against_cell(request: BacktestRequest, cell: CalibrationCellV2) -> None:
    if request.signal.side is not Side.BUY:
        raise ValueError("registered trade must use a BUY signal")
    if (
        request.signal.event_id != cell.event_cluster_id
        or request.as_of != cell.as_of
        or request.start_at != cell.start_at
        or request.end_at != cell.end_at
        or request.market != cell.market
        or request.instrument_ids != cell.instrument_ids
        or request.data_snapshot_id != cell.data_snapshot_id
        or request.target_selection_ref != cell.target_selection_ref
        or request.horizons_sessions != cell.horizons_sessions
        or request.simulation != cell.simulation
    ):
        raise ValueError("Backtest Request does not match the registered calibration cell")
    if request.strategy_ref != "event-impact-hold.v1":
        raise ValueError("registered trade must use the common execution strategy")


def _net_returns(result: BacktestResult) -> dict[int, Decimal] | None:
    expected = {f"horizon_{horizon}.net_return" for horizon in PHASE2_REQUIRED_HORIZONS}
    metrics = {metric.name: metric for metric in result.metrics if metric.name in expected}
    if set(metrics) != expected or any(metric.unit != "ratio" for metric in metrics.values()):
        return None
    return {
        horizon: metrics[f"horizon_{horizon}.net_return"].value
        for horizon in PHASE2_REQUIRED_HORIZONS
    }


def _comparison_key(result: BacktestResult) -> tuple[object, ...]:
    manifest = result.manifest
    request = manifest.request
    return (
        request.as_of,
        request.start_at,
        request.end_at,
        request.market,
        request.instrument_ids,
        request.data_snapshot_id,
        request.horizons_sessions,
        request.simulation,
        manifest.engine_name,
        manifest.engine_version,
        manifest.bridge_name,
        manifest.bridge_version,
        manifest.data_adapter_name,
        manifest.data_adapter_version,
        manifest.input_hashes,
        manifest.engine_config_hash,
        result.artifact_refs,
    )


def _variant_values(
    clusters: tuple[str, ...],
    variant: CalibrationVariant,
    returns: dict[tuple[str, CalibrationVariant], dict[int, Decimal]],
) -> tuple[Decimal, ...] | None:
    values: list[Decimal] = []
    for cluster in clusters:
        item = returns.get((cluster, variant))
        if item is None or set(item) != set(PHASE2_REQUIRED_HORIZONS):
            return None
        values.extend(item[horizon] for horizon in PHASE2_REQUIRED_HORIZONS)
    return tuple(values) if values else None


def _mean(values: tuple[Decimal, ...]) -> Decimal:
    return sum(values, Decimal(0)) / Decimal(len(values))


def _evidence_hash(evidence: Phase2CalibrationEvidenceV2) -> str:
    rows: list[dict[str, object]] = [
        {
            "event_cluster_id": item.event_cluster_id,
            "first_executed_at": _canonical_timestamp(item.first.manifest.executed_at),
            "first_request_hash": item.first.manifest.request_hash,
            "first_result_hash": item.first.result_hash,
            "first_run_id": item.first.manifest.run_id,
            "repeat_executed_at": _canonical_timestamp(item.repeat.manifest.executed_at),
            "repeat_request_hash": item.repeat.manifest.request_hash,
            "repeat_result_hash": item.repeat.result_hash,
            "repeat_run_id": item.repeat.manifest.run_id,
            "variant": item.variant.value,
        }
        for item in evidence.trades
    ]
    rows.sort(key=_canonical_hash)
    return _canonical_hash(
        {
            "gate_version": PHASE2_CALIBRATION_GATE_V2,
            "registration_hash": evidence.registration.registration_hash,
            "trades": rows,
        }
    )


def _registration_identity_payload(
    *,
    registration_id: str,
    registered_at: datetime,
    source_registration_ref: str,
    source_registration_sha256: str,
    cells: tuple[CalibrationCellV2, ...],
    decisions: tuple[CalibrationDecisionV2, ...],
) -> dict[str, object]:
    return {
        "cells": [_cell_to_dict(cell) for cell in cells],
        "decisions": [_decision_to_dict(item) for item in decisions],
        "gate_version": PHASE2_CALIBRATION_GATE_V2,
        "registered_at": registered_at.isoformat().replace("+00:00", "Z"),
        "registration_id": registration_id,
        "schema_version": PHASE2_REGISTRATION_SCHEMA,
        "source_registration_ref": source_registration_ref,
        "source_registration_sha256": source_registration_sha256,
    }


def _gate_result_identity_payload(result: Phase2CalibrationGateResultV2) -> dict[str, object]:
    return {
        "accepted": result.accepted,
        "beat_baselines": [item.value for item in result.beat_baselines],
        "candidate_mean_net_return": _optional_decimal(result.candidate_mean_net_return),
        "evidence_hash": result.evidence_hash,
        "gate_version": PHASE2_CALIBRATION_GATE_V2,
        "max_single_event_share": _optional_decimal(result.max_single_event_share),
        "reasons": list(result.reasons),
        "registration_hash": result.registration_hash,
        "test_event_clusters": list(result.test_event_clusters),
        "test_trade_counts": {variant.value: count for variant, count in result.test_trade_counts},
        "train_event_clusters": list(result.train_event_clusters),
    }


def _cell_to_dict(cell: CalibrationCellV2) -> dict[str, object]:
    request_shell = {
        "as_of": cell.as_of.isoformat().replace("+00:00", "Z"),
        "data_snapshot_id": cell.data_snapshot_id,
        "end_at": cell.end_at.isoformat().replace("+00:00", "Z"),
        "horizons_sessions": list(cell.horizons_sessions),
        "instrument_ids": list(cell.instrument_ids),
        "market": cell.market,
        "simulation": {
            "base_currency": cell.simulation.base_currency,
            "book_type": cell.simulation.book_type,
            "data_granularity": cell.simulation.data_granularity,
            "fee_model": cell.simulation.fee_model,
            "fill_model": cell.simulation.fill_model,
            "random_seed": cell.simulation.random_seed,
            "starting_cash": _optional_decimal(cell.simulation.starting_cash),
            "venue_ruleset": cell.simulation.venue_ruleset,
        },
        "start_at": cell.start_at.isoformat().replace("+00:00", "Z"),
        "target_selection_ref": cell.target_selection_ref,
    }
    return {
        "event_cluster_id": cell.event_cluster_id,
        "partition": cell.partition.value,
        "visible_at": cell.visible_at.isoformat().replace("+00:00", "Z"),
        **request_shell,
    }


def _decision_to_dict(item: CalibrationDecisionV2) -> dict[str, object]:
    return {
        "action": item.action,
        "decision_input_hashes": list(item.decision_input_hashes),
        "event_cluster_id": item.event_cluster_id,
        "request": backtest_request_to_dict(item.request) if item.request is not None else None,
        "request_hash": item.request_hash,
        "rule_ref": item.rule_ref,
        "variant": item.variant.value,
    }


def _cell_from_dict(value: object) -> CalibrationCellV2:
    payload = _object(value, "registration cell")
    _require_exact_keys(
        payload,
        {
            "as_of",
            "data_snapshot_id",
            "end_at",
            "event_cluster_id",
            "horizons_sessions",
            "instrument_ids",
            "market",
            "partition",
            "simulation",
            "start_at",
            "target_selection_ref",
            "visible_at",
        },
        "registration cell",
    )
    simulation = _object(payload.get("simulation"), "registration cell simulation")
    return CalibrationCellV2(
        event_cluster_id=_string(payload, "event_cluster_id"),
        visible_at=_timestamp(payload, "visible_at"),
        partition=CalibrationPartition(_string(payload, "partition")),
        target_selection_ref=_string(payload, "target_selection_ref"),
        as_of=_timestamp(payload, "as_of"),
        start_at=_timestamp(payload, "start_at"),
        end_at=_timestamp(payload, "end_at"),
        market=_string(payload, "market"),
        instrument_ids=tuple(_string_array(payload, "instrument_ids")),
        data_snapshot_id=_string(payload, "data_snapshot_id"),
        horizons_sessions=tuple(_integer_array(payload, "horizons_sessions")),
        simulation=SimulationSpec(
            data_granularity=_string(simulation, "data_granularity"),
            book_type=_string(simulation, "book_type"),
            fill_model=_string(simulation, "fill_model"),
            fee_model=_string(simulation, "fee_model"),
            venue_ruleset=_string(simulation, "venue_ruleset"),
            base_currency=_string(simulation, "base_currency"),
            starting_cash=Decimal(_string(simulation, "starting_cash")),
            random_seed=_integer(simulation, "random_seed"),
        ),
    )


def _decision_from_dict(value: object) -> CalibrationDecisionV2:
    payload = _object(value, "registration decision")
    _require_exact_keys(
        payload,
        {
            "action",
            "decision_input_hashes",
            "event_cluster_id",
            "request",
            "request_hash",
            "rule_ref",
            "variant",
        },
        "registration decision",
    )
    request_payload = payload.get("request")
    request = None if request_payload is None else backtest_request_from_dict(request_payload)
    request_hash_value = payload.get("request_hash")
    if request_hash_value is not None and not isinstance(request_hash_value, str):
        raise ValueError("request_hash must be a string or null")
    return CalibrationDecisionV2(
        event_cluster_id=_string(payload, "event_cluster_id"),
        variant=CalibrationVariant(_string(payload, "variant")),
        action=_string(payload, "action"),
        rule_ref=_string(payload, "rule_ref"),
        decision_input_hashes=tuple(_string_array(payload, "decision_input_hashes")),
        request=request,
        request_hash=request_hash_value,
    )


def _load_result(root: Path, relative_path: str) -> BacktestResult:
    path = _safe_relative_file(root, relative_path)
    return backtest_result_from_dict(json.loads(path.read_text(encoding="utf-8")))


def _safe_relative_file(root: Path, relative_path: str) -> Path:
    requested = Path(relative_path)
    if requested.is_absolute() or ".." in requested.parts:
        raise ValueError("calibration paths must stay below the evidence directory")
    root_resolved = root.resolve()
    unresolved = root / requested
    path = unresolved.resolve()
    if root_resolved not in path.parents or unresolved.is_symlink() or not path.is_file():
        raise ValueError("calibration path must be a real file")
    return path


def _decision_key_sort(
    item: tuple[str, CalibrationVariant],
) -> tuple[str, str]:
    return item[0], item[1].value


def _optional_decimal(value: Decimal | None) -> str | None:
    if value is None:
        return None
    normalized = value.normalize()
    return "0" if normalized == 0 else format(normalized, "f")


def _canonical_timestamp(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _require_artifact_component(value: object, name: str) -> None:
    if type(value) is not str or _ARTIFACT_COMPONENT_PATTERN.fullmatch(value) is None:
        raise ValueError(f"{name} must be a lowercase hyphenated artifact component")


def _canonical_hash(payload: dict[str, object]) -> str:
    encoded = json.dumps(payload, ensure_ascii=True, separators=(",", ":"), sort_keys=True)
    return sha256(encoded.encode("utf-8")).hexdigest()


def _object(value: object, name: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise ValueError(f"{name} must be an object")
    return cast(dict[str, object], value)


def _array(fields: dict[str, object], name: str) -> list[object]:
    value = fields.get(name)
    if not isinstance(value, list):
        raise ValueError(f"{name} must be an array")
    return cast(list[object], value)


def _string(fields: dict[str, object], name: str) -> str:
    value = fields.get(name)
    if not isinstance(value, str) or not value:
        raise ValueError(f"{name} must be a non-empty string")
    return value


def _timestamp(fields: dict[str, object], name: str) -> datetime:
    raw = _string(fields, name)
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{name} must be an ISO 8601 timestamp") from exc
    require_aware(parsed, name)
    return parsed


def _integer(fields: dict[str, object], name: str) -> int:
    value = fields.get(name)
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise ValueError(f"{name} must be a non-negative integer")
    return value


def _string_array(fields: dict[str, object], name: str) -> list[str]:
    values = _array(fields, name)
    if not all(isinstance(value, str) and value for value in values):
        raise ValueError(f"{name} must contain non-empty strings")
    return cast(list[str], values)


def _integer_array(fields: dict[str, object], name: str) -> list[int]:
    values = _array(fields, name)
    if not all(isinstance(value, int) and not isinstance(value, bool) for value in values):
        raise ValueError(f"{name} must contain integers")
    return cast(list[int], values)


def _require_exact_keys(fields: dict[str, object], expected: set[str], name: str) -> None:
    if set(fields) != expected:
        raise ValueError(f"{name} fields do not match the contract")


def _require_sha256(value: str, name: str) -> None:
    if _SHA256_PATTERN.fullmatch(value) is None:
        raise ValueError(f"{name} must be a lowercase SHA-256")
