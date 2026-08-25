from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum
from hashlib import sha256
from typing import Protocol, cast

from market_impact_agent.domain import SignalIntent, require_aware

_SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")


class BacktestRunStatus(StrEnum):
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass(frozen=True, slots=True)
class SimulationSpec:
    data_granularity: str
    book_type: str
    fill_model: str
    fee_model: str
    venue_ruleset: str
    base_currency: str
    starting_cash: Decimal
    random_seed: int

    def __post_init__(self) -> None:
        for name in (
            "data_granularity",
            "book_type",
            "fill_model",
            "fee_model",
            "venue_ruleset",
            "base_currency",
        ):
            _require_nonempty(getattr(self, name), name)
        if not self.starting_cash.is_finite() or self.starting_cash <= 0:
            raise ValueError("starting_cash must be finite and positive")
        if not _is_non_negative_integer(cast(object, self.random_seed)):
            raise ValueError("random_seed must be a non-negative integer")


@dataclass(frozen=True, slots=True)
class BacktestRequest:
    request_id: str
    signal: SignalIntent
    as_of: datetime
    start_at: datetime
    end_at: datetime
    market: str
    instrument_ids: tuple[str, ...]
    data_snapshot_id: str
    strategy_ref: str
    horizons_sessions: tuple[int, ...]
    simulation: SimulationSpec

    def __post_init__(self) -> None:
        for name in ("request_id", "market", "data_snapshot_id", "strategy_ref"):
            _require_nonempty(getattr(self, name), name)
        for name in ("as_of", "start_at", "end_at"):
            require_aware(getattr(self, name), name)
        if self.start_at < self.as_of:
            raise ValueError("start_at must not be before as_of")
        if self.end_at <= self.start_at:
            raise ValueError("end_at must be after start_at")
        _require_unique_nonempty(self.instrument_ids, "instrument_ids")
        if self.signal.instrument_id not in self.instrument_ids:
            raise ValueError("signal instrument_id must belong to the request instrument_ids")
        if not self.signal.valid_from <= self.as_of < self.signal.expires_at:
            raise ValueError("as_of must be within signal validity")
        if not self.signal.valid_from <= self.start_at < self.signal.expires_at:
            raise ValueError("start_at must be within signal validity")
        horizons = cast(tuple[object, ...], self.horizons_sessions)
        if (
            not horizons
            or any(not _is_positive_integer(value) for value in horizons)
            or self.horizons_sessions != tuple(sorted(set(self.horizons_sessions)))
        ):
            raise ValueError("horizons_sessions must be positive, unique, and ascending")


@dataclass(frozen=True, slots=True)
class BacktestRunManifest:
    run_id: str
    request: BacktestRequest
    request_hash: str
    engine_name: str
    engine_version: str
    bridge_name: str
    bridge_version: str
    engine_config_hash: str
    executed_at: datetime

    def __post_init__(self) -> None:
        for name in (
            "run_id",
            "engine_name",
            "engine_version",
            "bridge_name",
            "bridge_version",
        ):
            _require_nonempty(getattr(self, name), name)
        _require_sha256(self.request_hash, "request_hash")
        _require_sha256(self.engine_config_hash, "engine_config_hash")
        require_aware(self.executed_at, "executed_at")
        if self.request_hash != canonical_backtest_request_hash(self.request):
            raise ValueError("request_hash must match canonical request content")


@dataclass(frozen=True, slots=True)
class BacktestMetric:
    name: str
    value: Decimal
    unit: str

    def __post_init__(self) -> None:
        _require_nonempty(self.name, "metric name")
        _require_nonempty(self.unit, "metric unit")
        if not self.value.is_finite():
            raise ValueError("metric value must be finite")


@dataclass(frozen=True, slots=True)
class BacktestResult:
    manifest: BacktestRunManifest
    status: BacktestRunStatus
    result_hash: str
    metrics: tuple[BacktestMetric, ...]
    artifact_refs: tuple[str, ...]
    failure_reasons: tuple[str, ...]

    def __post_init__(self) -> None:
        _require_sha256(self.result_hash, "result_hash")
        if self.status is BacktestRunStatus.COMPLETED:
            if self.failure_reasons:
                raise ValueError("completed results cannot include failure_reasons")
        elif not self.failure_reasons:
            raise ValueError("failed results require failure_reasons")

        metric_names = tuple(metric.name for metric in self.metrics)
        _require_unique(metric_names, "metric names")
        _require_unique_nonempty(self.artifact_refs, "artifact_refs", allow_empty=True)
        _require_unique_nonempty(self.failure_reasons, "failure_reasons", allow_empty=True)
        if self.result_hash != canonical_backtest_result_hash(
            manifest=self.manifest,
            status=self.status,
            metrics=self.metrics,
            artifact_refs=self.artifact_refs,
            failure_reasons=self.failure_reasons,
        ):
            raise ValueError("result_hash must match canonical result content")


class BacktestBridge(Protocol):
    def run(self, request: BacktestRequest) -> BacktestResult: ...


def canonical_backtest_request_hash(request: BacktestRequest) -> str:
    """Return the stable identity for an engine-neutral backtest request."""
    return _canonical_sha256(
        {
            "as_of": _canonical_timestamp(request.as_of),
            "data_snapshot_id": request.data_snapshot_id,
            "end_at": _canonical_timestamp(request.end_at),
            "horizons_sessions": list(request.horizons_sessions),
            "instrument_ids": sorted(request.instrument_ids),
            "market": request.market,
            "request_id": request.request_id,
            "signal": {
                "event_id": request.signal.event_id,
                "evidence_refs": sorted(request.signal.evidence_refs),
                "expires_at": _canonical_timestamp(request.signal.expires_at),
                "invalidation_conditions": sorted(request.signal.invalidation_conditions),
                "instrument_id": request.signal.instrument_id,
                "side": request.signal.side.value,
                "signal_id": request.signal.signal_id,
                "valid_from": _canonical_timestamp(request.signal.valid_from),
            },
            "simulation": {
                "base_currency": request.simulation.base_currency,
                "book_type": request.simulation.book_type,
                "data_granularity": request.simulation.data_granularity,
                "fee_model": request.simulation.fee_model,
                "fill_model": request.simulation.fill_model,
                "random_seed": request.simulation.random_seed,
                "starting_cash": _canonical_decimal(request.simulation.starting_cash),
                "venue_ruleset": request.simulation.venue_ruleset,
            },
            "start_at": _canonical_timestamp(request.start_at),
            "strategy_ref": request.strategy_ref,
        }
    )


def canonical_backtest_result_hash(
    *,
    manifest: BacktestRunManifest,
    status: BacktestRunStatus,
    metrics: tuple[BacktestMetric, ...],
    artifact_refs: tuple[str, ...],
    failure_reasons: tuple[str, ...],
) -> str:
    """Return replay identity without per-run metadata such as run_id or execution time."""
    return _canonical_sha256(
        {
            "artifact_refs": sorted(artifact_refs),
            "engine": {
                "bridge_name": manifest.bridge_name,
                "bridge_version": manifest.bridge_version,
                "engine_config_hash": manifest.engine_config_hash,
                "engine_name": manifest.engine_name,
                "engine_version": manifest.engine_version,
            },
            "failure_reasons": sorted(failure_reasons),
            "metrics": [
                {
                    "name": metric.name,
                    "unit": metric.unit,
                    "value": _canonical_decimal(metric.value),
                }
                for metric in sorted(metrics, key=lambda metric: metric.name)
            ],
            "request_hash": manifest.request_hash,
            "status": status.value,
        }
    )


def _canonical_sha256(payload: dict[str, object]) -> str:
    encoded = json.dumps(payload, ensure_ascii=True, separators=(",", ":"), sort_keys=True)
    return sha256(encoded.encode("utf-8")).hexdigest()


def _canonical_timestamp(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _canonical_decimal(value: Decimal) -> str:
    normalized = value.normalize()
    if normalized == 0:
        return "0"
    return format(normalized, "f")


def _is_non_negative_integer(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value >= 0


def _is_positive_integer(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value > 0


def _require_nonempty(value: str, field_name: str) -> None:
    if not value:
        raise ValueError(f"{field_name} must not be empty")


def _require_sha256(value: str, field_name: str) -> None:
    raw_value = cast(object, value)
    if not isinstance(raw_value, str) or _SHA256_PATTERN.fullmatch(raw_value) is None:
        raise ValueError(f"{field_name} must be a SHA-256 hex digest")


def _require_unique(values: tuple[str, ...], field_name: str) -> None:
    if len(values) != len(set(values)):
        raise ValueError(f"{field_name} values must be unique")


def _require_unique_nonempty(
    values: tuple[str, ...], field_name: str, *, allow_empty: bool = False
) -> None:
    if not values and not allow_empty:
        raise ValueError(f"{field_name} must not be empty")
    _require_unique(values, field_name)
    if any(not value for value in values):
        raise ValueError(f"{field_name} values must not be empty")
