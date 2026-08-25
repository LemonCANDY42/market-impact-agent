from __future__ import annotations

import json
import os
import stat
import tempfile
from collections.abc import Callable
from contextlib import suppress
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from hashlib import sha256
from pathlib import Path
from typing import cast

from market_impact_agent.backtests import (
    BacktestRequest,
    BacktestRunStatus,
    SimulationSpec,
    backtest_result_to_dict,
    canonical_backtest_request_hash,
)
from market_impact_agent.calibration import CalibrationPartition, CalibrationVariant
from market_impact_agent.calibration_v2 import (
    PHASE2_EVIDENCE_SCHEMA_V2,
    CalibrationAction,
    CalibrationCellV2,
    CalibrationDecisionV2,
    Phase2CalibrationRegistrationV2,
    canonical_phase2_registration_hash,
    load_phase2_calibration_registration_v2,
    phase2_calibration_registration_v2_to_dict,
)
from market_impact_agent.domain import Side, SignalIntent
from market_impact_agent.tushare_bundle import (
    ValidatedTushareDataBundle,
    validate_tushare_data_bundle,
)
from market_impact_agent.tushare_replay import (
    TUSHARE_HARDENED_TARGET_SELECTION_REF,
    load_validated_tushare_adjusted_closes,
    load_validated_tushare_modeled_open,
    run_validated_tushare_replay,
)

_PUBLIC_REGISTRATION_SCHEMA = "market-impact.phase2-calibration-registration.v1"
_VARIANT_ORDER = tuple(CalibrationVariant)


def build_phase2_registration(
    *,
    cohort_path: Path,
    data_snapshot_root: Path,
    output_path: Path,
    clock: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> Phase2CalibrationRegistrationV2:
    cohort_bytes = cohort_path.read_bytes()
    cohort = _object(json.loads(cohort_bytes), "public cohort")
    if _string(cohort, "schema_version") != _PUBLIC_REGISTRATION_SCHEMA:
        raise ValueError("unsupported public cohort schema_version")
    if _string(cohort, "gate_version") != "energy-supply-shock-calibration.v2":
        raise ValueError("public cohort must target the v2 calibration gate")
    source_hash = sha256(cohort_bytes).hexdigest()
    target_selection_ref = _string(cohort, "target_selection_ref")
    if target_selection_ref != TUSHARE_HARDENED_TARGET_SELECTION_REF:
        raise ValueError("public cohort target selection does not match the hardened adapter")
    simulation = _simulation(_object(cohort.get("simulation"), "simulation"))
    variants = _object(cohort.get("variants"), "variants")
    event_clusters = _array(cohort, "event_clusters")
    cells: list[CalibrationCellV2] = []
    decisions: list[CalibrationDecisionV2] = []

    for raw_event in event_clusters:
        event = _object(raw_event, "event cluster")
        cluster_id = _string(event, "event_cluster_id")
        visible_at = _timestamp(event, "visible_at")
        bundle = _find_bundle(data_snapshot_root, cohort, event)
        snapshot, _ = load_validated_tushare_modeled_open(bundle.path)
        evaluation_start = date.fromisoformat(_string(event, "evaluation_start_date"))
        evaluation_end = date.fromisoformat(_string(event, "evaluation_end_date"))
        start_bar = next(
            (bar for bar in snapshot.bars if bar.session_open_at.date() == evaluation_start),
            None,
        )
        end_bar = next(
            (bar for bar in snapshot.bars if bar.session_close_at.date() == evaluation_end),
            None,
        )
        if start_bar is None or end_bar is None:
            raise ValueError(f"registered evaluation boundary is not an open session: {cluster_id}")
        cell = CalibrationCellV2(
            event_cluster_id=cluster_id,
            visible_at=visible_at,
            partition=CalibrationPartition(_string(event, "partition")),
            target_selection_ref=target_selection_ref,
            as_of=visible_at,
            start_at=start_bar.session_open_at,
            end_at=end_bar.session_close_at,
            market=_string(cohort, "market"),
            instrument_ids=(_string(cohort, "instrument_id"),),
            data_snapshot_id=bundle.data_snapshot_id,
            horizons_sessions=tuple(_integer_array(cohort, "horizons_sessions")),
            simulation=simulation,
        )
        cells.append(cell)
        registered_decisions = _object(event.get("registered_decisions"), "decisions")
        evidence_refs = tuple(_string_array(event, "evidence_refs"))
        momentum_action = _momentum_action(bundle.path, visible_at=visible_at)
        tables = _object(bundle.manifest.get("tables"), "bundle tables")
        daily_hash = _string(_object(tables.get("daily"), "daily table"), "sha256")
        adj_hash = _string(_object(tables.get("adj_factors"), "adjustment-factor table"), "sha256")
        decision_hashes = tuple(
            sorted({source_hash, bundle.bundle_hash, snapshot.content_hash, daily_hash, adj_hash})
        )
        for variant in _VARIANT_ORDER:
            variant_spec = _object(variants.get(variant.value), f"variant {variant.value}")
            action = (
                momentum_action
                if variant is CalibrationVariant.MOMENTUM
                else _string(registered_decisions, variant.value)
            )
            request = (
                _trade_request(
                    cell=cell,
                    variant=variant,
                    evidence_refs=evidence_refs,
                    decision_basis=_string(event, "decision_basis"),
                )
                if action == CalibrationAction.BUY
                else None
            )
            decisions.append(
                CalibrationDecisionV2(
                    event_cluster_id=cluster_id,
                    variant=variant,
                    action=action,
                    rule_ref=_string(variant_spec, "strategy_ref"),
                    decision_input_hashes=decision_hashes,
                    request=request,
                    request_hash=(
                        canonical_backtest_request_hash(request) if request is not None else None
                    ),
                )
            )

    registered_at = clock()
    registration_id = f"{_string(cohort, 'registration_id')}-execution"
    source_ref = cohort_path.as_posix()
    registration_hash = canonical_phase2_registration_hash(
        registration_id=registration_id,
        registered_at=registered_at,
        source_registration_ref=source_ref,
        source_registration_sha256=source_hash,
        cells=tuple(cells),
        decisions=tuple(decisions),
    )
    registration = Phase2CalibrationRegistrationV2(
        registration_id=registration_id,
        registered_at=registered_at,
        source_registration_ref=source_ref,
        source_registration_sha256=source_hash,
        cells=tuple(cells),
        decisions=tuple(decisions),
        registration_hash=registration_hash,
    )
    _write_private_json(output_path, phase2_calibration_registration_v2_to_dict(registration))
    return registration


def run_phase2_registration(
    *,
    registration_path: Path,
    data_snapshot_root: Path,
    output_dir: Path,
) -> Path:
    registration = load_phase2_calibration_registration_v2(registration_path)
    output_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    output_dir.chmod(0o700)
    copied_registration = output_dir / "registration.json"
    if registration_path.resolve() != copied_registration.resolve():
        _write_private_json(
            copied_registration,
            phase2_calibration_registration_v2_to_dict(registration),
        )
    trades: list[dict[str, object]] = []
    for decision in registration.decisions:
        if decision.action == CalibrationAction.ABSTAIN:
            continue
        if decision.request is None:
            raise ValueError("registered buy decision is missing its Backtest Request")
        artifact_component = _artifact_component(decision.event_cluster_id)
        bundle_path = data_snapshot_root / decision.request.data_snapshot_id
        validate_tushare_data_bundle(bundle_path)
        result_paths: list[str] = []
        for run_number in (1, 2):
            result = run_validated_tushare_replay(decision.request, bundle_path)
            file_name = f"{artifact_component}.{decision.variant.value}.run-{run_number}.json"
            _write_private_json(output_dir / file_name, backtest_result_to_dict(result))
            if result.status is not BacktestRunStatus.COMPLETED:
                raise RuntimeError(
                    "Phase 2 replay failed for "
                    f"{decision.event_cluster_id}:{decision.variant.value}:run-{run_number}"
                )
            result_paths.append(file_name)
        trades.append(
            {
                "event_cluster_id": decision.event_cluster_id,
                "variant": decision.variant.value,
                "first_result": result_paths[0],
                "repeat_result": result_paths[1],
            }
        )
    evidence_path = output_dir / "evidence.json"
    _write_private_json(
        evidence_path,
        {
            "schema_version": PHASE2_EVIDENCE_SCHEMA_V2,
            "registration": "registration.json",
            "trades": trades,
        },
    )
    return evidence_path


def _trade_request(
    *,
    cell: CalibrationCellV2,
    variant: CalibrationVariant,
    evidence_refs: tuple[str, ...],
    decision_basis: str,
) -> BacktestRequest:
    signal = SignalIntent(
        signal_id=f"{cell.event_cluster_id}-{variant.value}-signal-v1",
        event_id=cell.event_cluster_id,
        instrument_id=cell.instrument_ids[0],
        side=Side.BUY,
        valid_from=cell.as_of,
        expires_at=cell.end_at + timedelta(days=1),
        evidence_refs=evidence_refs,
        invalidation_conditions=(f"registered decision invalidated: {decision_basis}",),
    )
    return BacktestRequest(
        request_id=f"{cell.event_cluster_id}-{variant.value}-request-v1",
        signal=signal,
        as_of=cell.as_of,
        start_at=cell.start_at,
        end_at=cell.end_at,
        market=cell.market,
        instrument_ids=cell.instrument_ids,
        data_snapshot_id=cell.data_snapshot_id,
        target_selection_ref=cell.target_selection_ref,
        strategy_ref="event-impact-hold.v1",
        horizons_sessions=cell.horizons_sessions,
        simulation=cell.simulation,
    )


def _momentum_action(bundle_path: Path, *, visible_at: datetime) -> str:
    adjusted_closes = load_validated_tushare_adjusted_closes(
        bundle_path,
        visible_at=visible_at,
    )
    if len(adjusted_closes) < 4:
        raise ValueError("momentum baseline requires four pre-cutoff open-session closes")
    return (
        CalibrationAction.BUY
        if adjusted_closes[-1][1] > adjusted_closes[-4][1]
        else CalibrationAction.ABSTAIN
    )


def _find_bundle(
    root: Path,
    cohort: dict[str, object],
    event: dict[str, object],
) -> ValidatedTushareDataBundle:
    wanted = {
        "as_of_date": _timestamp(event, "visible_at").date().isoformat(),
        "end_date": _string(event, "evaluation_end_date"),
        "evaluation_start_date": _string(event, "evaluation_start_date"),
        "start_date": _string(event, "data_start_date"),
        "tushare_code": _string(cohort, "tushare_code"),
    }
    matches: list[ValidatedTushareDataBundle] = []
    for path in sorted(root.glob("tushare-*")):
        try:
            bundle = validate_tushare_data_bundle(path)
        except (OSError, TypeError, ValueError, json.JSONDecodeError):
            continue
        request = _object(bundle.manifest.get("request"), "bundle request")
        if all(request.get(name) == value for name, value in wanted.items()):
            matches.append(bundle)
    if len(matches) != 1:
        raise ValueError(
            f"expected one hardened bundle for {_string(event, 'event_cluster_id')}, "
            f"found {len(matches)}"
        )
    return matches[0]


def _simulation(payload: dict[str, object]) -> SimulationSpec:
    return SimulationSpec(
        data_granularity=_string(payload, "data_granularity"),
        book_type=_string(payload, "book_type"),
        fill_model=_string(payload, "fill_model"),
        fee_model=_string(payload, "fee_model"),
        venue_ruleset=_string(payload, "venue_ruleset"),
        base_currency=_string(payload, "base_currency"),
        starting_cash=Decimal(_string(payload, "starting_cash")),
        random_seed=_integer(payload, "random_seed"),
    )


def _write_private_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    path.parent.chmod(0o700)
    encoded = (json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n").encode()
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temporary = Path(temporary_name)
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
        temporary.replace(path)
        path.chmod(0o600)
    except Exception:
        with suppress(OSError):
            os.close(descriptor)
        temporary.unlink(missing_ok=True)
        raise
    if stat.S_IMODE(path.stat().st_mode) != 0o600:
        raise ValueError("private Phase 2 artifact must use mode 0600")


def _artifact_component(value: str) -> str:
    allowed = "abcdefghijklmnopqrstuvwxyz0123456789-"
    if not value or any(character not in allowed for character in value):
        raise ValueError("event_cluster_id must be a lowercase hyphenated artifact component")
    if value.startswith("-") or value.endswith("-") or "--" in value:
        raise ValueError("event_cluster_id must be a lowercase hyphenated artifact component")
    return value


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
    parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
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
