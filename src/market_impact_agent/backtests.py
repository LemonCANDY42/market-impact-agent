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
BACKTEST_REQUEST_SCHEMA_VERSION = "market-impact.backtest-request.v1"
BACKTEST_RESULT_SCHEMA_VERSION = "market-impact.backtest-result.v1"


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
    target_selection_ref: str
    strategy_ref: str
    horizons_sessions: tuple[int, ...]
    simulation: SimulationSpec

    def __post_init__(self) -> None:
        for name in (
            "request_id",
            "market",
            "data_snapshot_id",
            "target_selection_ref",
            "strategy_ref",
        ):
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
class BacktestInputHash:
    name: str
    value: str

    def __post_init__(self) -> None:
        _require_nonempty(self.name, "input hash name")
        _require_sha256(self.value, "input hash value")


@dataclass(frozen=True, slots=True)
class BacktestRunManifest:
    run_id: str
    request: BacktestRequest
    request_hash: str
    engine_name: str
    engine_version: str
    bridge_name: str
    bridge_version: str
    data_adapter_name: str
    data_adapter_version: str
    input_hashes: tuple[BacktestInputHash, ...]
    engine_config_hash: str
    executed_at: datetime

    def __post_init__(self) -> None:
        for name in (
            "run_id",
            "engine_name",
            "engine_version",
            "bridge_name",
            "bridge_version",
            "data_adapter_name",
            "data_adapter_version",
        ):
            _require_nonempty(getattr(self, name), name)
        _require_sha256(self.request_hash, "request_hash")
        _require_sha256(self.engine_config_hash, "engine_config_hash")
        input_names = tuple(item.name for item in self.input_hashes)
        _require_unique_nonempty(input_names, "input hash names")
        if input_names != tuple(sorted(input_names)):
            raise ValueError("input_hashes must use canonical name order")
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
            "target_selection_ref": request.target_selection_ref,
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
                "data_adapter_name": manifest.data_adapter_name,
                "data_adapter_version": manifest.data_adapter_version,
                "engine_config_hash": manifest.engine_config_hash,
                "engine_name": manifest.engine_name,
                "engine_version": manifest.engine_version,
                "input_hashes": [
                    {"name": item.name, "value": item.value} for item in manifest.input_hashes
                ],
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


def backtest_request_to_dict(request: BacktestRequest) -> dict[str, object]:
    return {
        "schema_version": BACKTEST_REQUEST_SCHEMA_VERSION,
        "request_id": request.request_id,
        "signal": {
            "schema_version": "market-impact.signal-intent.v1",
            "signal_id": request.signal.signal_id,
            "event_id": request.signal.event_id,
            "instrument_id": request.signal.instrument_id,
            "side": request.signal.side.value,
            "valid_from": _canonical_timestamp(request.signal.valid_from),
            "expires_at": _canonical_timestamp(request.signal.expires_at),
            "evidence_refs": list(request.signal.evidence_refs),
            "invalidation_conditions": list(request.signal.invalidation_conditions),
        },
        "as_of": _canonical_timestamp(request.as_of),
        "start_at": _canonical_timestamp(request.start_at),
        "end_at": _canonical_timestamp(request.end_at),
        "market": request.market,
        "instrument_ids": list(request.instrument_ids),
        "data_snapshot_id": request.data_snapshot_id,
        "target_selection_ref": request.target_selection_ref,
        "strategy_ref": request.strategy_ref,
        "horizons_sessions": list(request.horizons_sessions),
        "simulation": {
            "data_granularity": request.simulation.data_granularity,
            "book_type": request.simulation.book_type,
            "fill_model": request.simulation.fill_model,
            "fee_model": request.simulation.fee_model,
            "venue_ruleset": request.simulation.venue_ruleset,
            "base_currency": request.simulation.base_currency,
            "starting_cash": _canonical_decimal(request.simulation.starting_cash),
            "random_seed": request.simulation.random_seed,
        },
    }


def backtest_request_from_dict(payload: object) -> BacktestRequest:
    fields = _object(payload, "Backtest Request")
    _require_exact_keys(
        fields,
        {
            "as_of",
            "data_snapshot_id",
            "end_at",
            "horizons_sessions",
            "instrument_ids",
            "market",
            "request_id",
            "schema_version",
            "signal",
            "simulation",
            "start_at",
            "strategy_ref",
            "target_selection_ref",
        },
        "Backtest Request",
    )
    if _string(fields, "schema_version") != BACKTEST_REQUEST_SCHEMA_VERSION:
        raise ValueError("unsupported Backtest Request schema_version")
    signal_fields = _object(fields.get("signal"), "Signal Intent")
    _require_exact_keys(
        signal_fields,
        {
            "event_id",
            "evidence_refs",
            "expires_at",
            "instrument_id",
            "invalidation_conditions",
            "schema_version",
            "side",
            "signal_id",
            "valid_from",
        },
        "Signal Intent",
    )
    if _string(signal_fields, "schema_version") != "market-impact.signal-intent.v1":
        raise ValueError("unsupported Signal Intent schema_version")
    simulation_fields = _object(fields.get("simulation"), "Simulation Specification")
    _require_exact_keys(
        simulation_fields,
        {
            "base_currency",
            "book_type",
            "data_granularity",
            "fee_model",
            "fill_model",
            "random_seed",
            "starting_cash",
            "venue_ruleset",
        },
        "Simulation Specification",
    )
    from market_impact_agent.domain import Side, SignalIntent

    signal = SignalIntent(
        signal_id=_string(signal_fields, "signal_id"),
        event_id=_string(signal_fields, "event_id"),
        instrument_id=_string(signal_fields, "instrument_id"),
        side=Side(_string(signal_fields, "side")),
        valid_from=_parse_timestamp(signal_fields, "valid_from"),
        expires_at=_parse_timestamp(signal_fields, "expires_at"),
        evidence_refs=_string_tuple(signal_fields, "evidence_refs"),
        invalidation_conditions=_string_tuple(signal_fields, "invalidation_conditions"),
    )
    return BacktestRequest(
        request_id=_string(fields, "request_id"),
        signal=signal,
        as_of=_parse_timestamp(fields, "as_of"),
        start_at=_parse_timestamp(fields, "start_at"),
        end_at=_parse_timestamp(fields, "end_at"),
        market=_string(fields, "market"),
        instrument_ids=_string_tuple(fields, "instrument_ids"),
        data_snapshot_id=_string(fields, "data_snapshot_id"),
        target_selection_ref=_string(fields, "target_selection_ref"),
        strategy_ref=_string(fields, "strategy_ref"),
        horizons_sessions=_integer_tuple(fields, "horizons_sessions"),
        simulation=SimulationSpec(
            data_granularity=_string(simulation_fields, "data_granularity"),
            book_type=_string(simulation_fields, "book_type"),
            fill_model=_string(simulation_fields, "fill_model"),
            fee_model=_string(simulation_fields, "fee_model"),
            venue_ruleset=_string(simulation_fields, "venue_ruleset"),
            base_currency=_string(simulation_fields, "base_currency"),
            starting_cash=_decimal_string(simulation_fields, "starting_cash"),
            random_seed=_integer(simulation_fields, "random_seed"),
        ),
    )


def backtest_result_to_dict(result: BacktestResult) -> dict[str, object]:
    manifest = result.manifest
    return {
        "schema_version": BACKTEST_RESULT_SCHEMA_VERSION,
        "manifest": {
            "run_id": manifest.run_id,
            "request": backtest_request_to_dict(manifest.request),
            "request_hash": manifest.request_hash,
            "engine_name": manifest.engine_name,
            "engine_version": manifest.engine_version,
            "bridge_name": manifest.bridge_name,
            "bridge_version": manifest.bridge_version,
            "data_adapter_name": manifest.data_adapter_name,
            "data_adapter_version": manifest.data_adapter_version,
            "input_hashes": [
                {"name": item.name, "value": item.value} for item in manifest.input_hashes
            ],
            "engine_config_hash": manifest.engine_config_hash,
            "executed_at": _canonical_timestamp(manifest.executed_at),
        },
        "status": result.status.value,
        "result_hash": result.result_hash,
        "metrics": [
            {"name": item.name, "value": _canonical_decimal(item.value), "unit": item.unit}
            for item in result.metrics
        ],
        "artifact_refs": list(result.artifact_refs),
        "failure_reasons": list(result.failure_reasons),
    }


def backtest_result_from_dict(payload: object) -> BacktestResult:
    fields = _object(payload, "Backtest Result")
    _require_exact_keys(
        fields,
        {
            "artifact_refs",
            "failure_reasons",
            "manifest",
            "metrics",
            "result_hash",
            "schema_version",
            "status",
        },
        "Backtest Result",
    )
    if _string(fields, "schema_version") != BACKTEST_RESULT_SCHEMA_VERSION:
        raise ValueError("unsupported Backtest Result schema_version")
    manifest_fields = _object(fields.get("manifest"), "Backtest Run Manifest")
    _require_exact_keys(
        manifest_fields,
        {
            "bridge_name",
            "bridge_version",
            "data_adapter_name",
            "data_adapter_version",
            "engine_config_hash",
            "engine_name",
            "engine_version",
            "executed_at",
            "input_hashes",
            "request",
            "request_hash",
            "run_id",
        },
        "Backtest Run Manifest",
    )
    input_hashes = tuple(
        BacktestInputHash(name=_string(item, "name"), value=_string(item, "value"))
        for item in _object_tuple(manifest_fields, "input_hashes", {"name", "value"})
    )
    manifest = BacktestRunManifest(
        run_id=_string(manifest_fields, "run_id"),
        request=backtest_request_from_dict(manifest_fields.get("request")),
        request_hash=_string(manifest_fields, "request_hash"),
        engine_name=_string(manifest_fields, "engine_name"),
        engine_version=_string(manifest_fields, "engine_version"),
        bridge_name=_string(manifest_fields, "bridge_name"),
        bridge_version=_string(manifest_fields, "bridge_version"),
        data_adapter_name=_string(manifest_fields, "data_adapter_name"),
        data_adapter_version=_string(manifest_fields, "data_adapter_version"),
        input_hashes=input_hashes,
        engine_config_hash=_string(manifest_fields, "engine_config_hash"),
        executed_at=_parse_timestamp(manifest_fields, "executed_at"),
    )
    metrics = tuple(
        BacktestMetric(
            name=_string(item, "name"),
            value=_decimal_string(item, "value"),
            unit=_string(item, "unit"),
        )
        for item in _object_tuple(fields, "metrics", {"name", "unit", "value"})
    )
    return BacktestResult(
        manifest=manifest,
        status=BacktestRunStatus(_string(fields, "status")),
        result_hash=_string(fields, "result_hash"),
        metrics=metrics,
        artifact_refs=_string_tuple(fields, "artifact_refs", allow_empty=True),
        failure_reasons=_string_tuple(fields, "failure_reasons", allow_empty=True),
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


def _object(value: object, name: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise ValueError(f"{name} must be an object with string fields")
    raw = cast(dict[object, object], value)
    if any(not isinstance(key, str) for key in raw):
        raise ValueError(f"{name} must be an object with string fields")
    return cast(dict[str, object], value)


def _require_exact_keys(fields: dict[str, object], expected: set[str], name: str) -> None:
    missing = sorted(expected - fields.keys())
    unknown = sorted(fields.keys() - expected)
    if missing:
        raise ValueError(f"{name} missing fields: {', '.join(missing)}")
    if unknown:
        raise ValueError(f"{name} has unknown fields: {', '.join(unknown)}")


def _string(fields: dict[str, object], name: str) -> str:
    value = fields.get(name)
    if not isinstance(value, str) or not value:
        raise ValueError(f"{name} must be a non-empty string")
    return value


def _string_tuple(
    fields: dict[str, object], name: str, *, allow_empty: bool = False
) -> tuple[str, ...]:
    value = fields.get(name)
    if not isinstance(value, list):
        raise ValueError(f"{name} must be an array of non-empty strings")
    raw = cast(list[object], value)
    if any(not isinstance(item, str) or not item for item in raw):
        raise ValueError(f"{name} must be an array of non-empty strings")
    result = tuple(cast(list[str], value))
    if not result and not allow_empty:
        raise ValueError(f"{name} must not be empty")
    return result


def _integer(fields: dict[str, object], name: str) -> int:
    value = fields.get(name)
    if not isinstance(value, int) or isinstance(value, bool):
        raise ValueError(f"{name} must be an integer")
    return value


def _integer_tuple(fields: dict[str, object], name: str) -> tuple[int, ...]:
    value = fields.get(name)
    if not isinstance(value, list):
        raise ValueError(f"{name} must be an array of integers")
    return tuple(_integer({name: item}, name) for item in cast(list[object], value))


def _decimal_string(fields: dict[str, object], name: str) -> Decimal:
    raw = _string(fields, name)
    try:
        value = Decimal(raw)
    except Exception as exc:
        raise ValueError(f"{name} must be a decimal string") from exc
    if not value.is_finite() or _canonical_decimal(value) != raw:
        raise ValueError(f"{name} must be a canonical finite decimal string")
    return value


def _parse_timestamp(fields: dict[str, object], name: str) -> datetime:
    raw = _string(fields, name)
    try:
        value = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{name} must be an ISO-8601 timestamp") from exc
    require_aware(value, name)
    if _canonical_timestamp(value) != raw:
        raise ValueError(f"{name} must use canonical UTC format")
    return value


def _object_tuple(
    fields: dict[str, object], name: str, expected_fields: set[str]
) -> tuple[dict[str, object], ...]:
    value = fields.get(name)
    if not isinstance(value, list):
        raise ValueError(f"{name} must be an array of objects")
    result = tuple(_object(item, f"{name} item") for item in cast(list[object], value))
    for item in result:
        _require_exact_keys(item, expected_fields, f"{name} item")
    return result
