from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from hashlib import sha256
from pathlib import Path
from typing import cast

from market_impact_agent.backtests import (
    BacktestResult,
    BacktestRunStatus,
    backtest_result_from_dict,
)
from market_impact_agent.domain import require_aware

PHASE2_CALIBRATION_GATE_VERSION = "energy-supply-shock-calibration.v1"
PHASE2_REQUIRED_HORIZONS = (1, 3, 10)
PHASE2_MINIMUM_TRAIN_CLUSTERS = 2
PHASE2_MINIMUM_TEST_CLUSTERS = 3
PHASE2_MAX_SINGLE_EVENT_SHARE = Decimal("0.50")
_SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")


class CalibrationPartition(StrEnum):
    TRAIN = "train"
    TEST = "test"


class CalibrationVariant(StrEnum):
    EVENT_REASONING = "event_reasoning"
    SENTIMENT = "sentiment"
    MOMENTUM = "momentum"
    FIXED_MAPPING = "fixed_mapping"
    SIMPLE_HOLD = "simple_hold"


PHASE2_REQUIRED_BASELINES = (
    CalibrationVariant.SENTIMENT,
    CalibrationVariant.MOMENTUM,
    CalibrationVariant.FIXED_MAPPING,
    CalibrationVariant.SIMPLE_HOLD,
)
_PHASE2_REQUIRED_VARIANTS = (CalibrationVariant.EVENT_REASONING, *PHASE2_REQUIRED_BASELINES)


@dataclass(frozen=True, slots=True)
class CalibrationRunEvidence:
    event_cluster_id: str
    visible_at: datetime
    partition: CalibrationPartition
    variant: CalibrationVariant
    first: BacktestResult
    repeat: BacktestResult

    def __post_init__(self) -> None:
        if not self.event_cluster_id:
            raise ValueError("event_cluster_id must not be empty")
        require_aware(self.visible_at, "visible_at")


@dataclass(frozen=True, slots=True)
class Phase2CalibrationGateResult:
    accepted: bool
    evidence_hash: str
    report_hash: str
    train_event_clusters: tuple[str, ...]
    test_event_clusters: tuple[str, ...]
    beat_baselines: tuple[CalibrationVariant, ...]
    candidate_mean_net_return: Decimal | None
    max_single_event_share: Decimal | None
    reasons: tuple[str, ...]

    def __post_init__(self) -> None:
        if _SHA256_PATTERN.fullmatch(self.evidence_hash) is None:
            raise ValueError("evidence_hash must be a lowercase SHA-256")
        if _SHA256_PATTERN.fullmatch(self.report_hash) is None:
            raise ValueError("report_hash must be a lowercase SHA-256")
        if self.train_event_clusters != tuple(sorted(set(self.train_event_clusters))):
            raise ValueError("train_event_clusters must be unique and sorted")
        if self.test_event_clusters != tuple(sorted(set(self.test_event_clusters))):
            raise ValueError("test_event_clusters must be unique and sorted")
        if set(self.train_event_clusters) & set(self.test_event_clusters):
            raise ValueError("calibration clusters cannot cross partitions")
        if self.beat_baselines != tuple(sorted(set(self.beat_baselines), key=lambda x: x.value)):
            raise ValueError("beat_baselines must be unique and sorted")
        if self.reasons != tuple(sorted(set(self.reasons))):
            raise ValueError("reasons must be unique and sorted")
        if self.accepted == bool(self.reasons):
            raise ValueError("accepted calibration results must have no rejection reasons")
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
        if self.accepted and (
            len(self.train_event_clusters) < PHASE2_MINIMUM_TRAIN_CLUSTERS
            or len(self.test_event_clusters) < PHASE2_MINIMUM_TEST_CLUSTERS
            or not self.beat_baselines
            or self.candidate_mean_net_return is None
            or self.candidate_mean_net_return <= 0
            or self.max_single_event_share is None
            or self.max_single_event_share > PHASE2_MAX_SINGLE_EVENT_SHARE
        ):
            raise ValueError("accepted calibration result does not clear the frozen gate")
        expected_hash = _canonical_hash(
            _report_identity_payload(
                accepted=self.accepted,
                evidence_hash=self.evidence_hash,
                train_event_clusters=self.train_event_clusters,
                test_event_clusters=self.test_event_clusters,
                beat_baselines=self.beat_baselines,
                candidate_mean_net_return=self.candidate_mean_net_return,
                max_single_event_share=self.max_single_event_share,
                reasons=self.reasons,
            )
        )
        if self.report_hash != expected_hash:
            raise ValueError("report_hash must match the canonical gate result")


def load_phase2_calibration_evidence(path: Path) -> tuple[CalibrationRunEvidence, ...]:
    payload = _object(json.loads(path.read_text(encoding="utf-8")), "calibration evidence")
    _require_exact_keys(payload, {"runs", "schema_version"}, "calibration evidence")
    if _string(payload, "schema_version") != "market-impact.phase2-calibration-evidence.v1":
        raise ValueError("unsupported calibration evidence schema_version")
    raw_runs = payload.get("runs")
    if not isinstance(raw_runs, list):
        raise ValueError("calibration evidence runs must be an array")
    if not raw_runs:
        raise ValueError("calibration evidence runs must not be empty")
    evidence: list[CalibrationRunEvidence] = []
    for raw_run in cast(list[object], raw_runs):
        run = _object(raw_run, "calibration evidence run")
        _require_exact_keys(
            run,
            {
                "event_cluster_id",
                "first_result",
                "partition",
                "repeat_result",
                "variant",
                "visible_at",
            },
            "calibration evidence run",
        )
        evidence.append(
            CalibrationRunEvidence(
                event_cluster_id=_string(run, "event_cluster_id"),
                visible_at=_timestamp(run, "visible_at"),
                partition=CalibrationPartition(_string(run, "partition")),
                variant=CalibrationVariant(_string(run, "variant")),
                first=_load_result(path.parent, _string(run, "first_result")),
                repeat=_load_result(path.parent, _string(run, "repeat_result")),
            )
        )
    return tuple(evidence)


def assess_phase2_calibration(
    evidence: tuple[CalibrationRunEvidence, ...],
) -> Phase2CalibrationGateResult:
    reasons: set[str] = set()
    keyed: dict[tuple[str, CalibrationVariant], CalibrationRunEvidence] = {}
    cluster_partitions: dict[str, CalibrationPartition] = {}
    cluster_visibility: dict[str, datetime] = {}
    returns_by_key: dict[tuple[str, CalibrationVariant], dict[int, Decimal]] = {}

    for item in evidence:
        key = (item.event_cluster_id, item.variant)
        if key in keyed:
            reasons.add(f"duplicate_variant:{item.event_cluster_id}:{item.variant.value}")
            continue
        keyed[key] = item

        known_partition = cluster_partitions.setdefault(item.event_cluster_id, item.partition)
        if known_partition is not item.partition:
            reasons.add(f"cluster_crosses_partitions:{item.event_cluster_id}")
        known_visibility = cluster_visibility.setdefault(item.event_cluster_id, item.visible_at)
        if known_visibility != item.visible_at:
            reasons.add(f"cluster_visibility_mismatch:{item.event_cluster_id}")

        _validate_repeat_pair(item, reasons)
        first_request = item.first.manifest.request
        repeat_request = item.repeat.manifest.request
        if (
            first_request.signal.event_id != item.event_cluster_id
            or repeat_request.signal.event_id != item.event_cluster_id
        ):
            reasons.add(f"event_identity_mismatch:{item.event_cluster_id}:{item.variant.value}")
        if first_request.as_of != item.visible_at or repeat_request.as_of != item.visible_at:
            reasons.add(f"as_of_visibility_mismatch:{item.event_cluster_id}:{item.variant.value}")
        if first_request.horizons_sessions != PHASE2_REQUIRED_HORIZONS:
            reasons.add(f"required_horizons_missing:{item.event_cluster_id}:{item.variant.value}")
        if (
            item.variant is CalibrationVariant.EVENT_REASONING
            and first_request.target_selection_ref.startswith("manual-integration-fixture:")
        ):
            reasons.add(f"manual_fixture_candidate:{item.event_cluster_id}")
        extracted = _net_returns(item.first)
        if extracted is None:
            reasons.add(f"net_return_metrics_missing:{item.event_cluster_id}:{item.variant.value}")
        else:
            returns_by_key[key] = extracted

    train_clusters = tuple(
        sorted(
            cluster
            for cluster, partition in cluster_partitions.items()
            if partition is CalibrationPartition.TRAIN
        )
    )
    test_clusters = tuple(
        sorted(
            cluster
            for cluster, partition in cluster_partitions.items()
            if partition is CalibrationPartition.TEST
        )
    )
    if len(train_clusters) < PHASE2_MINIMUM_TRAIN_CLUSTERS:
        reasons.add("insufficient_train_event_clusters")
    if len(test_clusters) < PHASE2_MINIMUM_TEST_CLUSTERS:
        reasons.add("insufficient_test_event_clusters")
    if train_clusters and test_clusters:
        latest_train = max(cluster_visibility[cluster] for cluster in train_clusters)
        earliest_test = min(cluster_visibility[cluster] for cluster in test_clusters)
        if latest_train >= earliest_test:
            reasons.add("walk_forward_order_invalid")

    for variant in _PHASE2_REQUIRED_VARIANTS:
        strategy_refs = {
            item.first.manifest.request.strategy_ref
            for item in keyed.values()
            if item.variant is variant
        }
        if len(strategy_refs) > 1:
            reasons.add(f"variant_strategy_changed:{variant.value}")

    for cluster in (*train_clusters, *test_clusters):
        cluster_items = [keyed.get((cluster, variant)) for variant in _PHASE2_REQUIRED_VARIANTS]
        missing = [
            variant.value
            for variant, item in zip(_PHASE2_REQUIRED_VARIANTS, cluster_items, strict=True)
            if item is None
        ]
        if missing:
            reasons.add(f"missing_variants:{cluster}:{','.join(missing)}")
            continue
        comparable = tuple(item for item in cluster_items if item is not None)
        comparison_keys = {_comparison_key(item.first) for item in comparable}
        if len(comparison_keys) != 1:
            reasons.add(f"incomparable_variant_windows:{cluster}")

    candidate_values = _variant_values(
        test_clusters,
        CalibrationVariant.EVENT_REASONING,
        returns_by_key,
    )
    candidate_mean = _mean(candidate_values) if candidate_values is not None else None
    if candidate_mean is None or candidate_mean <= 0:
        reasons.add("candidate_net_return_not_positive")

    beat_baselines: list[CalibrationVariant] = []
    if candidate_values is not None:
        for baseline in PHASE2_REQUIRED_BASELINES:
            baseline_values = _variant_values(test_clusters, baseline, returns_by_key)
            if baseline_values is not None and _mean(candidate_values) > _mean(baseline_values):
                beat_baselines.append(baseline)
    if not beat_baselines:
        reasons.add("no_meaningful_baseline_beaten")

    event_contributions: list[Decimal] = []
    for cluster in test_clusters:
        values = returns_by_key.get((cluster, CalibrationVariant.EVENT_REASONING))
        if values is None or set(values) != set(PHASE2_REQUIRED_HORIZONS):
            continue
        event_contributions.append(sum((abs(value) for value in values.values()), Decimal(0)))
    total_contribution = sum(event_contributions, Decimal(0))
    max_single_event_share = (
        max(event_contributions) / total_contribution
        if event_contributions and total_contribution > 0
        else None
    )
    if max_single_event_share is None or max_single_event_share > PHASE2_MAX_SINGLE_EVENT_SHARE:
        reasons.add("single_event_dominance_not_cleared")

    ordered_reasons = tuple(sorted(reasons))
    evidence_hash = _evidence_hash(evidence)
    ordered_beat_baselines = tuple(sorted(beat_baselines, key=lambda item: item.value))
    report_hash = _canonical_hash(
        _report_identity_payload(
            accepted=not ordered_reasons,
            evidence_hash=evidence_hash,
            train_event_clusters=train_clusters,
            test_event_clusters=test_clusters,
            beat_baselines=ordered_beat_baselines,
            candidate_mean_net_return=candidate_mean,
            max_single_event_share=max_single_event_share,
            reasons=ordered_reasons,
        )
    )
    return Phase2CalibrationGateResult(
        accepted=not ordered_reasons,
        evidence_hash=evidence_hash,
        report_hash=report_hash,
        train_event_clusters=train_clusters,
        test_event_clusters=test_clusters,
        beat_baselines=ordered_beat_baselines,
        candidate_mean_net_return=candidate_mean,
        max_single_event_share=max_single_event_share,
        reasons=ordered_reasons,
    )


def phase2_calibration_gate_result_to_dict(
    result: Phase2CalibrationGateResult,
) -> dict[str, object]:
    return {
        "schema_version": "market-impact.phase2-calibration-gate-result.v1",
        "gate_version": PHASE2_CALIBRATION_GATE_VERSION,
        "accepted": result.accepted,
        "evidence_hash": result.evidence_hash,
        "report_hash": result.report_hash,
        "train_event_clusters": list(result.train_event_clusters),
        "test_event_clusters": list(result.test_event_clusters),
        "beat_baselines": [item.value for item in result.beat_baselines],
        "candidate_mean_net_return": _optional_decimal(result.candidate_mean_net_return),
        "max_single_event_share": _optional_decimal(result.max_single_event_share),
        "reasons": list(result.reasons),
    }


def _validate_repeat_pair(item: CalibrationRunEvidence, reasons: set[str]) -> None:
    label = f"{item.event_cluster_id}:{item.variant.value}"
    first = item.first
    repeat = item.repeat
    if (
        first.status is not BacktestRunStatus.COMPLETED
        or repeat.status is not BacktestRunStatus.COMPLETED
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


def _net_returns(result: BacktestResult) -> dict[int, Decimal] | None:
    expected_names = {f"horizon_{horizon}.net_return" for horizon in PHASE2_REQUIRED_HORIZONS}
    metrics = {metric.name: metric for metric in result.metrics if metric.name in expected_names}
    if set(metrics) != expected_names or any(metric.unit != "ratio" for metric in metrics.values()):
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
    returns_by_key: dict[tuple[str, CalibrationVariant], dict[int, Decimal]],
) -> tuple[Decimal, ...] | None:
    values: list[Decimal] = []
    for cluster in clusters:
        returns = returns_by_key.get((cluster, variant))
        if returns is None or set(returns) != set(PHASE2_REQUIRED_HORIZONS):
            return None
        values.extend(returns[horizon] for horizon in PHASE2_REQUIRED_HORIZONS)
    return tuple(values) if values else None


def _mean(values: tuple[Decimal, ...]) -> Decimal:
    return sum(values, Decimal(0)) / Decimal(len(values))


def _evidence_hash(evidence: tuple[CalibrationRunEvidence, ...]) -> str:
    rows = [
        {
            "event_cluster_id": item.event_cluster_id,
            "first_request_hash": item.first.manifest.request_hash,
            "first_result_hash": item.first.result_hash,
            "partition": item.partition.value,
            "repeat_request_hash": item.repeat.manifest.request_hash,
            "repeat_result_hash": item.repeat.result_hash,
            "variant": item.variant.value,
            "visible_at": item.visible_at.isoformat(),
        }
        for item in evidence
    ]
    rows.sort(key=lambda row: (str(row["event_cluster_id"]), str(row["variant"])))
    return _canonical_hash(
        {
            "gate_version": PHASE2_CALIBRATION_GATE_VERSION,
            "runs": rows,
        }
    )


def _optional_decimal(value: Decimal | None) -> str | None:
    if value is None:
        return None
    normalized = value.normalize()
    return "0" if normalized == 0 else format(normalized, "f")


def _canonical_hash(payload: dict[str, object]) -> str:
    encoded = json.dumps(payload, ensure_ascii=True, separators=(",", ":"), sort_keys=True)
    return sha256(encoded.encode("utf-8")).hexdigest()


def _report_identity_payload(
    *,
    accepted: bool,
    evidence_hash: str,
    train_event_clusters: tuple[str, ...],
    test_event_clusters: tuple[str, ...],
    beat_baselines: tuple[CalibrationVariant, ...],
    candidate_mean_net_return: Decimal | None,
    max_single_event_share: Decimal | None,
    reasons: tuple[str, ...],
) -> dict[str, object]:
    return {
        "accepted": accepted,
        "beat_baselines": [item.value for item in beat_baselines],
        "candidate_mean_net_return": _optional_decimal(candidate_mean_net_return),
        "evidence_hash": evidence_hash,
        "gate_version": PHASE2_CALIBRATION_GATE_VERSION,
        "max_single_event_share": _optional_decimal(max_single_event_share),
        "reasons": list(reasons),
        "test_event_clusters": list(test_event_clusters),
        "train_event_clusters": list(train_event_clusters),
    }


def _load_result(root: Path, relative_path: str) -> BacktestResult:
    requested = Path(relative_path)
    if requested.is_absolute() or ".." in requested.parts:
        raise ValueError("calibration result paths must stay below the evidence directory")
    root_resolved = root.resolve()
    unresolved = root / requested
    path = unresolved.resolve()
    if root_resolved not in path.parents or unresolved.is_symlink() or not path.is_file():
        raise ValueError("calibration result path must be a real file")
    payload = json.loads(path.read_text(encoding="utf-8"))
    return backtest_result_from_dict(payload)


def _object(value: object, name: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise ValueError(f"{name} must be an object")
    return cast(dict[str, object], value)


def _require_exact_keys(fields: dict[str, object], expected: set[str], name: str) -> None:
    if set(fields) != expected:
        raise ValueError(f"{name} fields do not match the closed contract")


def _string(fields: dict[str, object], name: str) -> str:
    value = fields.get(name)
    if not isinstance(value, str) or not value:
        raise ValueError(f"{name} must be a non-empty string")
    return value


def _timestamp(fields: dict[str, object], name: str) -> datetime:
    raw = _string(fields, name)
    try:
        value = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{name} must be an ISO-8601 timestamp") from exc
    require_aware(value, name)
    if value.isoformat().replace("+00:00", "Z") != raw:
        raise ValueError(f"{name} must use canonical UTC format")
    return value
