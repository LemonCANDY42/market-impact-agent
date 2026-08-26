from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import date, datetime
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path
from typing import TYPE_CHECKING, cast

from market_impact_agent.agent_contracts import JudgmentArtifact, canonical_hash
from market_impact_agent.domain import require_aware
from market_impact_agent.research import EventArchetype

if TYPE_CHECKING:
    from market_impact_agent.method_benchmark import (
        MethodQualityBenchmarkRegistration,
        MethodQualityEvaluationSpecification,
    )

MARKET_SNAPSHOT_SCHEMA = "market-impact.method-quality-market-snapshot.v1"
OUTCOME_SEAL_SCHEMA = "market-impact.method-quality-outcome-seal.v1"
OUTCOME_OPENING_SCHEMA = "market-impact.method-quality-outcome-opening.v1"

_DECIMAL = re.compile(r"^-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?$")
_QUANTITY = re.compile(r"^(?:0|[1-9][0-9]*)$")


@dataclass(frozen=True, slots=True)
class MarketSnapshot:
    snapshot_id: str
    canonical_snapshot_json: str

    def __post_init__(self) -> None:
        core = self.core_dict()
        _validate_market_snapshot_core(core)
        if self.snapshot_id != f"method-quality-market-snapshot-{canonical_hash(core)}":
            raise ValueError("market snapshot identity does not match content")

    @property
    def snapshot_hash(self) -> str:
        return canonical_hash(self.core_dict())

    @property
    def case_alias(self) -> str:
        return _string(self.core_dict(), "case_alias")

    @property
    def evaluation_specification_id(self) -> str:
        return _string(self.core_dict(), "evaluation_specification_id")

    @property
    def evaluation_specification_hash(self) -> str:
        return _string(self.core_dict(), "evaluation_specification_hash")

    def core_dict(self) -> dict[str, object]:
        return _object(json.loads(self.canonical_snapshot_json), "market snapshot")

    def to_dict(self) -> dict[str, object]:
        return {**self.core_dict(), "snapshot_id": self.snapshot_id}


@dataclass(frozen=True, slots=True)
class OutcomeSeal:
    seal_id: str
    canonical_seal_json: str

    def __post_init__(self) -> None:
        core = self.core_dict()
        _validate_outcome_seal_core(core)
        if self.seal_id != f"method-quality-outcome-seal-{canonical_hash(core)}":
            raise ValueError("outcome seal identity does not match content")

    @property
    def seal_hash(self) -> str:
        return canonical_hash(self.core_dict())

    def core_dict(self) -> dict[str, object]:
        return _object(json.loads(self.canonical_seal_json), "outcome seal")

    def to_dict(self) -> dict[str, object]:
        return {**self.core_dict(), "seal_id": self.seal_id}

    def validate_against(
        self,
        *,
        registration: MethodQualityBenchmarkRegistration,
        specification: MethodQualityEvaluationSpecification,
        snapshot: MarketSnapshot,
    ) -> None:
        core = self.core_dict()
        if (
            _string(core, "registration_id") != registration.registration_id
            or _string(core, "registration_hash") != registration.registration_hash
        ):
            raise ValueError("outcome seal does not match benchmark registration")
        if (
            _string(core, "evaluation_specification_id") != specification.specification_id
            or _string(core, "evaluation_specification_hash") != specification.specification_hash
            or snapshot.evaluation_specification_id != specification.specification_id
            or snapshot.evaluation_specification_hash != specification.specification_hash
        ):
            raise ValueError("outcome seal does not match evaluation specification")
        if (
            _string(core, "market_snapshot_id") != snapshot.snapshot_id
            or _string(core, "market_snapshot_hash") != snapshot.snapshot_hash
            or _string(core, "case_alias") != snapshot.case_alias
        ):
            raise ValueError("outcome seal does not match pre-run market snapshot")
        snapshot_created_at = _datetime(snapshot.core_dict(), "created_at")
        sealed_at = _datetime(core, "sealed_at")
        if snapshot_created_at > sealed_at or registration.registered_at > snapshot_created_at:
            raise ValueError("market snapshot and seal must follow registration and precede runs")
        archetype = EventArchetype(_string(core, "event_archetype"))
        expected_arms = {
            arm.value
            for suite in registration.suites
            if archetype in suite.eligible_archetypes
            for arm in suite.arms
        }
        expected = {
            (_string(core, "case_alias"), replicate, arm)
            for replicate in range(1, registration.replicate_count + 1)
            for arm in expected_arms
        }
        actual = {
            (
                _string(item, "case_alias"),
                _integer(item, "replicate"),
                _string(item, "arm"),
            )
            for item in _object_array(core, "expected_judgments")
        }
        if actual != expected or len(actual) != len(_object_array(core, "expected_judgments")):
            raise ValueError("outcome seal judgment matrix does not match registration")


@dataclass(frozen=True, slots=True)
class OutcomeOpening:
    opening_id: str
    canonical_opening_json: str

    def __post_init__(self) -> None:
        core = self.core_dict()
        _validate_outcome_opening_core(core)
        if self.opening_id != f"method-quality-outcome-opening-{canonical_hash(core)}":
            raise ValueError("outcome opening identity does not match content")

    @property
    def opening_hash(self) -> str:
        return canonical_hash(self.core_dict())

    def core_dict(self) -> dict[str, object]:
        return _object(json.loads(self.canonical_opening_json), "outcome opening")

    def to_dict(self) -> dict[str, object]:
        return {**self.core_dict(), "opening_id": self.opening_id}

    def validate_against(
        self,
        *,
        seal: OutcomeSeal,
        snapshot: MarketSnapshot,
        registration: MethodQualityBenchmarkRegistration,
        specification: MethodQualityEvaluationSpecification,
        judgment_artifacts: tuple[JudgmentArtifact, ...],
    ) -> None:
        seal.validate_against(
            registration=registration,
            specification=specification,
            snapshot=snapshot,
        )
        core = self.core_dict()
        seal_core = seal.core_dict()
        repeated = {
            "registration_id": registration.registration_id,
            "registration_hash": registration.registration_hash,
            "evaluation_specification_id": specification.specification_id,
            "evaluation_specification_hash": specification.specification_hash,
            "market_snapshot_id": snapshot.snapshot_id,
            "market_snapshot_hash": snapshot.snapshot_hash,
            "case_alias": _string(seal_core, "case_alias"),
        }
        if any(_string(core, key) != value for key, value in repeated.items()):
            raise ValueError("outcome opening repeated bindings do not match seal")
        if _string(core, "seal_id") != seal.seal_id or _string(core, "seal_hash") != seal.seal_hash:
            raise ValueError("outcome opening does not match sealed commitment")

        expected_items = _object_array(seal_core, "expected_judgments")
        expected_by_key = {_judgment_key(item): item for item in expected_items}
        opening_items = _object_array(core, "judgments")
        opening_by_key = {_judgment_key(item): item for item in opening_items}
        if set(opening_by_key) != set(expected_by_key) or len(opening_by_key) != len(opening_items):
            raise ValueError("outcome opening does not bind the complete judgment matrix")
        artifacts = {item.artifact_id: item for item in judgment_artifacts}
        if len(artifacts) != len(judgment_artifacts):
            raise ValueError("judgment artifacts must be unique")
        sealed_at = _datetime(seal_core, "sealed_at")
        opened_at = _datetime(core, "opened_at")
        for key, expected in expected_by_key.items():
            binding = opening_by_key[key]
            if _string(binding, "run_id") != _string(expected, "run_id") or _string(
                binding, "evidence_pack_id"
            ) != _string(expected, "evidence_pack_id"):
                raise ValueError("outcome judgment execution binding changed after sealing")
            artifact_id = _string(binding, "artifact_id")
            artifact = artifacts.get(artifact_id)
            if artifact is None:
                raise ValueError("outcome opening references an unknown Judgment Artifact")
            if (
                artifact.run_id != _string(expected, "run_id")
                or artifact.evidence_pack_id != _string(expected, "evidence_pack_id")
                or _string(binding, "artifact_hash") != canonical_hash(artifact.to_dict())
            ):
                raise ValueError("outcome opening Judgment Artifact binding is invalid")
            if artifact.started_at < sealed_at or artifact.finished_at > opened_at:
                raise ValueError("outcome data was not sealed before Agent execution and opening")
        if set(artifacts) != {_string(item, "artifact_id") for item in opening_items}:
            raise ValueError("outcome opening requires exactly the registered Judgment Artifacts")
        _validate_results_and_case_values(
            core,
            snapshot,
            specification,
            opening_by_key,
            artifacts,
        )


def load_market_snapshot(path: Path) -> MarketSnapshot:
    payload = _object(json.loads(path.read_text(encoding="utf-8")), "market snapshot")
    return _market_snapshot(payload)


def load_outcome_seal(path: Path) -> OutcomeSeal:
    payload = _object(json.loads(path.read_text(encoding="utf-8")), "outcome seal")
    return _outcome_seal(payload)


def load_outcome_opening(path: Path) -> OutcomeOpening:
    payload = _object(json.loads(path.read_text(encoding="utf-8")), "outcome opening")
    return _outcome_opening(payload)


def market_snapshot_from_dict(payload: object) -> MarketSnapshot:
    return _market_snapshot(_object(payload, "market snapshot"))


def outcome_seal_from_dict(payload: object) -> OutcomeSeal:
    return _outcome_seal(_object(payload, "outcome seal"))


def outcome_opening_from_dict(payload: object) -> OutcomeOpening:
    return _outcome_opening(_object(payload, "outcome opening"))


def validate_outcome_result(
    payload: object,
    *,
    snapshot: MarketSnapshot,
    specification: MethodQualityEvaluationSpecification,
) -> None:
    result = _object(payload, "outcome result")
    _validate_result_shape(result)
    result_id = _string(result, "result_id")
    core = {key: value for key, value in result.items() if key != "result_id"}
    if result_id != f"method-quality-result-{canonical_hash(core)}":
        raise ValueError("outcome result identity does not match content")
    _validate_specification_binding(snapshot, specification)
    _validate_result_equations(result, snapshot, specification)


def validate_case_value(
    payload: object, *, component_results: tuple[dict[str, object], ...]
) -> None:
    value = _object(payload, "case value")
    _closed(
        value,
        {"case_alias", "replicate", "arm", "status", "component_result_ids", "value"},
        "case value",
    )
    component_ids = tuple(_string(item, "result_id") for item in component_results)
    if _string_array(value, "component_result_ids") != component_ids:
        raise ValueError("case value components must preserve every result in order")
    expected = (
        Decimal("0")
        if not component_results
        else sum(
            (_decimal(item, "benchmark_adjusted_directional_score") for item in component_results),
            Decimal("0"),
        )
        / Decimal(len(component_results))
    )
    if _decimal(value, "value") != expected:
        raise ValueError(
            "case value must be the equal-weight mean of all candidate-horizon results"
        )
    if _string(value, "status") != _expected_case_status(component_results):
        raise ValueError("case value status does not match its deterministic result states")


def _market_snapshot(payload: dict[str, object]) -> MarketSnapshot:
    snapshot_id = _string(payload, "snapshot_id")
    core = {key: value for key, value in payload.items() if key != "snapshot_id"}
    result = MarketSnapshot(snapshot_id, _canonical_json(core))
    if result.to_dict() != payload:
        raise ValueError("Market Snapshot does not match canonical contract")
    return result


def _outcome_seal(payload: dict[str, object]) -> OutcomeSeal:
    seal_id = _string(payload, "seal_id")
    core = {key: value for key, value in payload.items() if key != "seal_id"}
    result = OutcomeSeal(seal_id, _canonical_json(core))
    if result.to_dict() != payload:
        raise ValueError("Outcome Seal does not match canonical contract")
    return result


def _outcome_opening(payload: dict[str, object]) -> OutcomeOpening:
    opening_id = _string(payload, "opening_id")
    core = {key: value for key, value in payload.items() if key != "opening_id"}
    result = OutcomeOpening(opening_id, _canonical_json(core))
    if result.to_dict() != payload:
        raise ValueError("Outcome Opening does not match canonical contract")
    return result


def _validate_market_snapshot_core(core: dict[str, object]) -> None:
    _closed(
        core,
        {
            "schema_version",
            "case_alias",
            "case_cutoff_session",
            "case_as_of",
            "evaluation_specification_id",
            "evaluation_specification_hash",
            "created_at",
            "sealed_before_agent_runs",
            "source_vintage_id",
            "source_vintage_hash",
            "venue",
            "timezone",
            "currency",
            "calendar_id",
            "calendar_sessions",
            "corporate_actions",
            "instrument_prices",
            "benchmark_id",
            "benchmark_prices",
            "fee_schedule",
            "venue_rules",
            "execution_capability",
        },
        "market snapshot",
    )
    if _string(core, "schema_version") != MARKET_SNAPSHOT_SCHEMA:
        raise ValueError("unsupported Market Snapshot schema_version")
    if not _boolean(core, "sealed_before_agent_runs"):
        raise ValueError("market snapshot must be sealed before Agent runs")
    if _string(core, "execution_capability") != "none":
        raise ValueError("market snapshot grants no execution capability")
    require_aware(_datetime(core, "created_at"), "market snapshot created_at")
    case_as_of = _datetime(core, "case_as_of")
    require_aware(case_as_of, "market snapshot case_as_of")
    if case_as_of > _datetime(core, "created_at"):
        raise ValueError("market snapshot case_as_of must not follow snapshot creation")
    _identifier(_string(core, "case_alias"), "market snapshot case_alias")
    for name in ("venue", "timezone", "currency", "calendar_id", "benchmark_id"):
        _string(core, name)
    _sha256(_string(core, "evaluation_specification_hash"), "evaluation specification hash")
    if _string(core, "evaluation_specification_id") != (
        f"method-quality-evaluation-{_string(core, 'evaluation_specification_hash')}"
    ):
        raise ValueError("market snapshot evaluation identity is inconsistent")
    _sha256(_string(core, "source_vintage_hash"), "source vintage hash")
    if _string(core, "source_vintage_id") != (
        f"source-vintage-{_string(core, 'source_vintage_hash')}"
    ):
        raise ValueError("market snapshot source vintage identity is inconsistent")
    sessions = _string_array(core, "calendar_sessions")
    if not sessions or len(sessions) != len(set(sessions)):
        raise ValueError("market snapshot calendar sessions must be non-empty and unique")
    session_dates = tuple(_date(value, "calendar session") for value in sessions)
    if tuple(sorted(session_dates)) != session_dates:
        raise ValueError("market snapshot calendar sessions must be ordered")
    session_set = set(sessions)
    cutoff = _string(core, "case_cutoff_session")
    if cutoff not in session_set or sessions.index(cutoff) == len(sessions) - 1:
        raise ValueError("market snapshot case cutoff must precede evaluation sessions")
    if case_as_of.date().isoformat() != cutoff:
        raise ValueError("market snapshot case cutoff must bind the case_as_of date")
    keys: set[tuple[str, str]] = set()
    for row in _object_array(core, "instrument_prices"):
        _validate_price_row(row, session_set, include_target=True)
        key = (_string(row, "target_id"), _string(row, "session_date"))
        if key in keys:
            raise ValueError("instrument price rows must be unique by target and session")
        keys.add(key)
    benchmark_sessions: set[str] = set()
    for row in _object_array(core, "benchmark_prices"):
        _validate_price_row(row, session_set, include_target=False)
        session = _string(row, "session_date")
        if session in benchmark_sessions:
            raise ValueError("benchmark price rows must be unique by session")
        benchmark_sessions.add(session)
    action_keys: set[tuple[str, str, str]] = set()
    for row in _object_array(core, "corporate_actions"):
        _closed(
            row,
            {"target_id", "effective_session", "action_type", "factor", "source_version_id"},
            "corporate action",
        )
        key = (
            _string(row, "target_id"),
            _string(row, "effective_session"),
            _string(row, "action_type"),
        )
        if key in action_keys or key[1] not in session_set:
            raise ValueError("corporate action identity or session is invalid")
        action_keys.add(key)
        _positive_decimal(row, "factor")
        _string(row, "source_version_id")
    fee_rows = _object_array(core, "fee_schedule")
    if not fee_rows:
        raise ValueError("market snapshot fee schedule must be non-empty")
    fee_ids: set[str] = set()
    for row in fee_rows:
        _closed(
            row,
            {
                "fee_id",
                "effective_from",
                "effective_through",
                "side",
                "component",
                "rate",
                "minimum_amount",
                "rounding_quantum",
                "rounding_mode",
            },
            "fee schedule row",
        )
        fee_id = _string(row, "fee_id")
        if fee_id in fee_ids:
            raise ValueError("fee schedule ids must be unique")
        fee_ids.add(fee_id)
        if _string(row, "side") not in {"entry", "exit"}:
            raise ValueError("fee schedule side is invalid")
        if _string(row, "rounding_mode") != "half_up":
            raise ValueError("fee schedule rounding mode is invalid")
        _decimal(row, "rate", minimum=Decimal("0"))
        _decimal(row, "minimum_amount", minimum=Decimal("0"))
        _positive_decimal(row, "rounding_quantum")
        _date(_string(row, "effective_from"), "fee effective_from")
        if row.get("effective_through") is not None:
            _date(_nullable_string(row, "effective_through"), "fee effective_through")
        _validate_effective_range(row, "fee schedule")
    _reject_overlaps(fee_rows, key_fields=("side", "component"), label="fee schedule")
    rule_rows = _object_array(core, "venue_rules")
    if not rule_rows:
        raise ValueError("market snapshot venue rules must be non-empty")
    rule_ids: set[str] = set()
    for row in rule_rows:
        _closed(
            row,
            {
                "rule_id",
                "effective_from",
                "effective_through",
                "board_lot_size",
                "price_tick",
                "price_limit_basis",
                "suspension_fill_policy",
                "missing_bar_policy",
            },
            "venue rule row",
        )
        rule_id = _string(row, "rule_id")
        if rule_id in rule_ids:
            raise ValueError("venue rule ids must be unique")
        rule_ids.add(rule_id)
        if _integer(row, "board_lot_size") < 1:
            raise ValueError("board lot size must be positive")
        _positive_decimal(row, "price_tick")
        if _string(row, "price_limit_basis") != "snapshot_limits":
            raise ValueError("venue rule price limit basis is invalid")
        if _string(row, "suspension_fill_policy") != "no_fill":
            raise ValueError("suspension fill policy must fail closed")
        if _string(row, "missing_bar_policy") != "missing_zero_value_in_denominator":
            raise ValueError("missing bar policy must retain the all-event denominator")
        _date(_string(row, "effective_from"), "venue rule effective_from")
        if row.get("effective_through") is not None:
            _date(_nullable_string(row, "effective_through"), "venue rule effective_through")
        _validate_effective_range(row, "venue rule")
    _reject_overlaps(rule_rows, key_fields=(), label="venue rule")
    for session in sessions[sessions.index(cutoff) + 1 :]:
        if len([row for row in rule_rows if _effective(row, session)]) != 1:
            raise ValueError("every evaluation session requires exactly one effective venue rule")
        for side in ("entry", "exit"):
            if not any(
                _string(row, "side") == side and _effective(row, session) for row in fee_rows
            ):
                raise ValueError("every evaluation session requires effective entry and exit fees")
    for row in _object_array(core, "instrument_prices"):
        session = _string(row, "session_date")
        matching_rules = [rule for rule in rule_rows if _effective(rule, session)]
        if (
            _string(row, "trade_status") == "open"
            and len(matching_rules) == 1
            and not _bar_respects_rule(row, matching_rules[0])
        ):
            raise ValueError("instrument price row violates effective tick or price limits")


def _validate_price_row(
    row: dict[str, object], session_set: set[str], *, include_target: bool
) -> None:
    fields = {
        "session_date",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "adjustment_factor",
        "trade_status",
        "limit_up",
        "limit_down",
        "source_version_id",
    }
    if include_target:
        fields.add("target_id")
    _closed(row, fields, "price row")
    if include_target:
        _string(row, "target_id")
    session = _string(row, "session_date")
    if session not in session_set:
        raise ValueError("price row session is outside the registered calendar")
    prices = {name: _positive_decimal(row, name) for name in ("open", "high", "low", "close")}
    if prices["low"] > min(prices["open"], prices["close"]) or prices["high"] < max(
        prices["open"], prices["close"]
    ):
        raise ValueError("price row OHLC values are inconsistent")
    _decimal(row, "volume", minimum=Decimal("0"))
    _positive_decimal(row, "adjustment_factor")
    if _string(row, "trade_status") not in {"open", "suspended", "missing"}:
        raise ValueError("price row trade status is invalid")
    _nullable_decimal(row, "limit_up", positive=True)
    _nullable_decimal(row, "limit_down", positive=True)
    _string(row, "source_version_id")


def _validate_outcome_seal_core(core: dict[str, object]) -> None:
    _closed(
        core,
        {
            "schema_version",
            "registration_id",
            "registration_hash",
            "evaluation_specification_id",
            "evaluation_specification_hash",
            "case_alias",
            "event_archetype",
            "market_snapshot_id",
            "market_snapshot_hash",
            "sealed_at",
            "sealed_before_agent_runs",
            "seal_status",
            "expected_judgments",
            "outcome_payload",
            "execution_capability",
        },
        "outcome seal",
    )
    if _string(core, "schema_version") != OUTCOME_SEAL_SCHEMA:
        raise ValueError("unsupported Outcome Seal schema_version")
    for name in ("registration_hash", "evaluation_specification_hash", "market_snapshot_hash"):
        _sha256(_string(core, name), f"outcome seal {name}")
    require_aware(_datetime(core, "sealed_at"), "outcome seal sealed_at")
    if not _boolean(core, "sealed_before_agent_runs") or _string(core, "seal_status") != "sealed":
        raise ValueError("outcome seal must be an unopened pre-run commitment")
    if core.get("outcome_payload") is not None:
        raise ValueError("outcome seal cannot contain outcome data")
    if _string(core, "execution_capability") != "none":
        raise ValueError("outcome seal grants no execution capability")
    seen: set[tuple[str, int, str]] = set()
    for item in _object_array(core, "expected_judgments"):
        _closed(
            item,
            {"case_alias", "replicate", "arm", "run_id", "evidence_pack_id"},
            "expected judgment",
        )
        if _string(item, "case_alias") != _string(core, "case_alias"):
            raise ValueError("expected judgment case does not match seal")
        key = _judgment_key(item)
        if key in seen or key[1] < 1:
            raise ValueError("expected judgment bindings must be unique")
        seen.add(key)
        _string(item, "run_id")
        _string(item, "evidence_pack_id")


def _validate_outcome_opening_core(core: dict[str, object]) -> None:
    _closed(
        core,
        {
            "schema_version",
            "seal_id",
            "seal_hash",
            "registration_id",
            "registration_hash",
            "evaluation_specification_id",
            "evaluation_specification_hash",
            "case_alias",
            "market_snapshot_id",
            "market_snapshot_hash",
            "opened_at",
            "opening_sequence",
            "prior_opening_id",
            "seal_status",
            "judgments",
            "results",
            "case_values",
            "execution_capability",
        },
        "outcome opening",
    )
    if _string(core, "schema_version") != OUTCOME_OPENING_SCHEMA:
        raise ValueError("unsupported Outcome Opening schema_version")
    if _integer(core, "opening_sequence") != 1 or core.get("prior_opening_id") is not None:
        raise ValueError("outcome opening must be the first append-only opening record")
    if _string(core, "seal_status") != "opened":
        raise ValueError("outcome opening status is invalid")
    if _string(core, "execution_capability") != "none":
        raise ValueError("outcome opening grants no execution capability")
    require_aware(_datetime(core, "opened_at"), "outcome opening opened_at")
    for name in (
        "seal_hash",
        "registration_hash",
        "evaluation_specification_hash",
        "market_snapshot_hash",
    ):
        _sha256(_string(core, name), f"outcome opening {name}")
    for item in _object_array(core, "judgments"):
        _closed(
            item,
            {
                "case_alias",
                "replicate",
                "arm",
                "run_id",
                "evidence_pack_id",
                "artifact_id",
                "artifact_hash",
            },
            "opening judgment",
        )
        _sha256(_string(item, "artifact_hash"), "opening artifact_hash")
    result_ids: set[str] = set()
    for item in _object_array(core, "results"):
        _validate_result_shape(item)
        result_id = _string(item, "result_id")
        if result_id in result_ids:
            raise ValueError("outcome result ids must be unique")
        result_ids.add(result_id)
        result_core = {key: value for key, value in item.items() if key != "result_id"}
        if result_id != f"method-quality-result-{canonical_hash(result_core)}":
            raise ValueError("outcome result identity does not match content")
    value_keys: set[tuple[str, int, str]] = set()
    for item in _object_array(core, "case_values"):
        _closed(
            item,
            {"case_alias", "replicate", "arm", "status", "component_result_ids", "value"},
            "case value",
        )
        key = _judgment_key(item)
        if key in value_keys:
            raise ValueError("case values must be unique by case, replicate, and arm")
        value_keys.add(key)
        if _string(item, "status") not in {
            "valued",
            "abstain",
            "no_fill",
            "unknown_or_mixed",
            "missing",
        }:
            raise ValueError("case value status is invalid")
        _decimal(item, "value")
        _string_array(item, "component_result_ids")


def _validate_result_shape(item: dict[str, object]) -> None:
    _closed(
        item,
        {
            "result_id",
            "case_alias",
            "replicate",
            "arm",
            "artifact_id",
            "artifact_hash",
            "target_id",
            "horizon_sessions",
            "direction",
            "fill_status",
            "entry_session",
            "entry_price",
            "quantity",
            "entry_reference_value",
            "exit_session",
            "exit_price",
            "exit_reference_value",
            "cost_components",
            "total_cost_proxy_amount",
            "price_move_ratio",
            "directional_score",
            "cost_proxy",
            "benchmark_move_ratio",
            "benchmark_adjusted_directional_score",
        },
        "outcome result",
    )
    _sha256(_string(item, "artifact_hash"), "outcome result artifact_hash")
    _identifier(_string(item, "case_alias"), "outcome result case_alias")
    if _integer(item, "replicate") < 1:
        raise ValueError("outcome result replicate must be positive")
    if _string(item, "direction") not in {"up", "down", "mixed", "unknown"}:
        raise ValueError("outcome result direction is invalid")
    if _integer(item, "horizon_sessions") not in {1, 3, 10}:
        raise ValueError("outcome result horizon is invalid")
    if _string(item, "fill_status") not in {
        "filled",
        "no_fill",
        "missing_market_data",
        "unscored_direction",
    }:
        raise ValueError("outcome result fill status is invalid")
    for name in (
        "entry_price",
        "entry_reference_value",
        "exit_price",
        "exit_reference_value",
    ):
        _nullable_decimal(item, name, positive=True)
    for name in (
        "total_cost_proxy_amount",
        "price_move_ratio",
        "directional_score",
        "cost_proxy",
        "benchmark_move_ratio",
        "benchmark_adjusted_directional_score",
    ):
        _decimal(item, name)
    quantity = _string(item, "quantity")
    if not _QUANTITY.fullmatch(quantity):
        raise ValueError("outcome quantity must be a canonical non-negative integer string")
    for cost in _object_array(item, "cost_components"):
        _closed(
            cost,
            {"fee_id", "side", "component", "reference_value", "amount"},
            "outcome cost component",
        )
        if _string(cost, "side") not in {"entry", "exit"}:
            raise ValueError("outcome cost side is invalid")
        _positive_decimal(cost, "reference_value")
        _decimal(cost, "amount", minimum=Decimal("0"))


def _validate_results_and_case_values(
    opening: dict[str, object],
    snapshot: MarketSnapshot,
    specification: MethodQualityEvaluationSpecification,
    opening_bindings: dict[tuple[str, int, str], dict[str, object]],
    artifacts: dict[str, JudgmentArtifact],
) -> None:
    results = _object_array(opening, "results")
    results_by_key: dict[tuple[str, int, str], list[dict[str, object]]] = {
        key: [] for key in opening_bindings
    }
    for result in results:
        key = _judgment_key(result)
        if key not in opening_bindings:
            raise ValueError("outcome result is outside the registered judgment matrix")
        binding = opening_bindings[key]
        if _string(result, "artifact_id") != _string(binding, "artifact_id") or _string(
            result, "artifact_hash"
        ) != _string(binding, "artifact_hash"):
            raise ValueError("outcome result does not match Judgment Artifact binding")
        results_by_key[key].append(result)
        _validate_result_equations(result, snapshot, specification)
    values = {_judgment_key(item): item for item in _object_array(opening, "case_values")}
    if set(values) != set(opening_bindings) or len(values) != len(
        _object_array(opening, "case_values")
    ):
        raise ValueError("outcome opening must retain every judgment in the all-event denominator")
    for key, binding in opening_bindings.items():
        artifact = artifacts[_string(binding, "artifact_id")]
        rows = results_by_key[key]
        candidates = {
            (item.target_id, item.horizon_sessions, item.direction.value)
            for item in artifact.proposal.candidates
        }
        actual = {
            (
                _string(item, "target_id"),
                _integer(item, "horizon_sessions"),
                _string(item, "direction"),
            )
            for item in rows
        }
        if actual != candidates or len(actual) != len(rows):
            raise ValueError("outcome results must exactly match proposed candidates and horizons")
        value = values[key]
        component_ids = tuple(_string(item, "result_id") for item in rows)
        if tuple(_string_array(value, "component_result_ids")) != component_ids:
            raise ValueError("case value components must preserve all candidate results in order")
        expected_value = (
            Decimal("0")
            if not rows
            else sum(
                (_decimal(item, "benchmark_adjusted_directional_score") for item in rows),
                Decimal("0"),
            )
            / Decimal(len(rows))
        )
        if _decimal(value, "value") != expected_value:
            raise ValueError(
                "case value must be the equal-weight mean of all candidate-horizon results"
            )
        if _string(value, "status") != _expected_case_status(tuple(rows)):
            raise ValueError("case value status does not match its deterministic result states")


def _validate_result_equations(
    result: dict[str, object],
    snapshot: MarketSnapshot,
    specification: MethodQualityEvaluationSpecification,
) -> None:
    status = _string(result, "fill_status")
    direction = _string(result, "direction")
    snapshot_core = snapshot.core_dict()
    if _string(result, "case_alias") != _string(snapshot_core, "case_alias"):
        raise ValueError("outcome result case does not match market snapshot")
    scoring = _object(specification.core_dict().get("scoring"), "evaluation scoring")
    expected_status, entry_row, exit_row = _derive_result_state(
        result,
        snapshot_core,
        scoring,
    )
    if status != expected_status:
        raise ValueError("outcome fill status does not match deterministic scoring state")
    zero_fields = (
        "total_cost_proxy_amount",
        "price_move_ratio",
        "directional_score",
        "cost_proxy",
        "benchmark_move_ratio",
        "benchmark_adjusted_directional_score",
    )
    if status != "filled":
        if any(_decimal(result, name) != 0 for name in zero_fields):
            raise ValueError("unfilled, missing, mixed, and unknown results must contribute zero")
        if _string(result, "quantity") != "0" or _object_array(result, "cost_components"):
            raise ValueError("non-filled results cannot contain quantity or cost proxies")
        if any(
            result.get(name) is not None
            for name in (
                "entry_session",
                "entry_price",
                "entry_reference_value",
                "exit_session",
                "exit_price",
                "exit_reference_value",
            )
        ):
            raise ValueError("non-filled results cannot contain reference-value data")
        if direction in {"mixed", "unknown"} and status != "unscored_direction":
            raise ValueError("mixed and unknown directions must use unscored_direction")
        return
    if direction not in {"up", "down"}:
        raise ValueError("filled results require up or down direction")
    entry_session = _nullable_string(result, "entry_session")
    exit_session = _nullable_string(result, "exit_session")
    entry_price = _required_nullable_decimal(result, "entry_price")
    exit_price = _required_nullable_decimal(result, "exit_price")
    entry_reference = _required_nullable_decimal(result, "entry_reference_value")
    exit_reference = _required_nullable_decimal(result, "exit_reference_value")
    quantity = Decimal(_string(result, "quantity"))
    if (
        quantity <= 0
        or entry_reference != entry_price * quantity
        or exit_reference != exit_price * quantity
    ):
        raise ValueError("outcome prices, quantity, and reference values are inconsistent")
    assert entry_row is not None and exit_row is not None
    if entry_session != _string(entry_row, "session_date") or exit_session != _string(
        exit_row, "session_date"
    ):
        raise ValueError("filled result does not use the derived entry and exact-horizon exit")
    if entry_price != _decimal(entry_row, "open") or exit_price != _decimal(exit_row, "close"):
        raise ValueError("filled result prices do not match market snapshot")
    multiplier = Decimal("1") if direction == "up" else Decimal("-1")
    price_move = (exit_reference - entry_reference) / entry_reference
    directional_score = multiplier * price_move
    costs = sum(
        (_decimal(item, "amount") for item in _object_array(result, "cost_components")),
        Decimal("0"),
    )
    cost_proxy = costs / entry_reference
    benchmark_rows = {
        _string(row, "session_date"): row
        for row in _object_array(snapshot_core, "benchmark_prices")
    }
    benchmark_entry = benchmark_rows.get(entry_session)
    benchmark_exit = benchmark_rows.get(exit_session)
    if benchmark_entry is None or benchmark_exit is None:
        raise ValueError("benchmark rows are missing for filled result")
    benchmark_move = (
        _decimal(benchmark_exit, "close") - _decimal(benchmark_entry, "open")
    ) / _decimal(benchmark_entry, "open")
    expected = {
        "total_cost_proxy_amount": costs,
        "price_move_ratio": price_move,
        "directional_score": directional_score,
        "cost_proxy": cost_proxy,
        "benchmark_move_ratio": benchmark_move,
        "benchmark_adjusted_directional_score": (
            directional_score - cost_proxy - multiplier * benchmark_move
        ),
    }
    if any(_decimal(result, name) != value for name, value in expected.items()):
        raise ValueError("outcome directional score or cost proxy equation is inconsistent")
    _validate_cost_components(
        result,
        snapshot_core,
        entry_session,
        exit_session,
        entry_reference,
        exit_reference,
    )
    rules = _object_array(snapshot_core, "venue_rules")
    matching = [row for row in rules if _effective(row, entry_session)]
    assert len(matching) == 1
    expected_quantity = _largest_affordable_quantity(
        entry_price=entry_price,
        board_lot_size=_integer(matching[0], "board_lot_size"),
        budget=_decimal(scoring, "research_notional_budget"),
        fee_rows=_effective_fees(snapshot_core, side="entry", session=entry_session),
    )
    if quantity != expected_quantity:
        raise ValueError("filled quantity is not the largest affordable effective board lot")


def _validate_cost_components(
    result: dict[str, object],
    snapshot: dict[str, object],
    entry_session: str,
    exit_session: str,
    entry_reference: Decimal,
    exit_reference: Decimal,
) -> None:
    schedules = {_string(row, "fee_id"): row for row in _object_array(snapshot, "fee_schedule")}
    actual = _object_array(result, "cost_components")
    expected_rows = [
        row
        for row in schedules.values()
        if (_string(row, "side") == "entry" and _effective(row, entry_session))
        or (_string(row, "side") == "exit" and _effective(row, exit_session))
    ]
    if {_string(row, "fee_id") for row in actual} != {
        _string(row, "fee_id") for row in expected_rows
    } or len(actual) != len(expected_rows):
        raise ValueError("outcome cost components do not match effective snapshot schedule")
    for item in actual:
        schedule = schedules[_string(item, "fee_id")]
        side = _string(schedule, "side")
        reference_value = entry_reference if side == "entry" else exit_reference
        quantum = _decimal(schedule, "rounding_quantum")
        amount = max(
            _decimal(schedule, "rate") * reference_value,
            _decimal(schedule, "minimum_amount"),
        ).quantize(quantum, rounding=ROUND_HALF_UP)
        if (
            _string(item, "side") != side
            or _string(item, "component") != _string(schedule, "component")
            or _decimal(item, "reference_value") != reference_value
            or _decimal(item, "amount") != amount
        ):
            raise ValueError("outcome cost component does not match snapshot fee rule")


def _expected_case_status(component_results: tuple[dict[str, object], ...]) -> str:
    if not component_results:
        return "abstain"
    statuses = {_string(item, "fill_status") for item in component_results}
    if statuses == {"unscored_direction"}:
        return "unknown_or_mixed"
    if statuses == {"no_fill"}:
        return "no_fill"
    if "missing_market_data" in statuses:
        return "missing"
    return "valued"


def _validate_specification_binding(
    snapshot: MarketSnapshot,
    specification: MethodQualityEvaluationSpecification,
) -> None:
    if (
        snapshot.evaluation_specification_id != specification.specification_id
        or snapshot.evaluation_specification_hash != specification.specification_hash
    ):
        raise ValueError("market snapshot does not bind the supplied evaluation specification")
    scoring = _object(specification.core_dict().get("scoring"), "evaluation scoring")
    if _string(snapshot.core_dict(), "currency") != _string(scoring, "notional_currency"):
        raise ValueError("market snapshot currency does not match evaluation specification")


def _derive_result_state(
    result: dict[str, object],
    snapshot: dict[str, object],
    scoring: dict[str, object],
) -> tuple[str, dict[str, object] | None, dict[str, object] | None]:
    direction = _string(result, "direction")
    if direction in {"mixed", "unknown"}:
        return "unscored_direction", None, None
    sessions = _string_array(snapshot, "calendar_sessions")
    cutoff_index = sessions.index(_string(snapshot, "case_cutoff_session"))
    search_limit = _integer(scoring, "entry_search_limit_sessions")
    search_sessions = sessions[cutoff_index + 1 : cutoff_index + 1 + search_limit]
    target = _string(result, "target_id")
    instrument = {
        (_string(row, "target_id"), _string(row, "session_date")): row
        for row in _object_array(snapshot, "instrument_prices")
    }
    venue_rules = _object_array(snapshot, "venue_rules")
    entry_row: dict[str, object] | None = None
    saw_nonmissing = False
    for session in search_sessions:
        row = instrument.get((target, session))
        if row is None or _string(row, "trade_status") == "missing":
            continue
        saw_nonmissing = True
        matching_rules = [rule for rule in venue_rules if _effective(rule, session)]
        if len(matching_rules) != 1:
            raise ValueError("entry session does not have exactly one effective venue rule")
        if _string(row, "trade_status") == "open" and _bar_respects_rule(row, matching_rules[0]):
            entry_row = row
            break
    if entry_row is None:
        return ("no_fill" if saw_nonmissing else "missing_market_data"), None, None
    entry_session = _string(entry_row, "session_date")
    entry_index = sessions.index(entry_session)
    exit_index = entry_index + _integer(result, "horizon_sessions")
    if exit_index >= len(sessions):
        return "missing_market_data", None, None
    exit_session = sessions[exit_index]
    exit_row = instrument.get((target, exit_session))
    if exit_row is None or _string(exit_row, "trade_status") != "open":
        return "missing_market_data", None, None
    exit_rules = [rule for rule in venue_rules if _effective(rule, exit_session)]
    if len(exit_rules) != 1:
        raise ValueError("exit session does not have exactly one effective venue rule")
    if not _bar_respects_rule(exit_row, exit_rules[0]):
        return "missing_market_data", None, None
    benchmark = {
        _string(row, "session_date"): row for row in _object_array(snapshot, "benchmark_prices")
    }
    benchmark_entry = benchmark.get(entry_session)
    benchmark_exit = benchmark.get(exit_session)
    if (
        benchmark_entry is None
        or benchmark_exit is None
        or _string(benchmark_entry, "trade_status") != "open"
        or _string(benchmark_exit, "trade_status") != "open"
    ):
        return "missing_market_data", None, None
    entry_rule = next(rule for rule in venue_rules if _effective(rule, entry_session))
    quantity = _largest_affordable_quantity(
        entry_price=_decimal(entry_row, "open"),
        board_lot_size=_integer(entry_rule, "board_lot_size"),
        budget=_decimal(scoring, "research_notional_budget"),
        fee_rows=_effective_fees(snapshot, side="entry", session=entry_session),
    )
    if quantity == 0:
        return "no_fill", None, None
    return "filled", entry_row, exit_row


def _effective_fees(
    snapshot: dict[str, object], *, side: str, session: str
) -> tuple[dict[str, object], ...]:
    rows = tuple(
        row
        for row in _object_array(snapshot, "fee_schedule")
        if _string(row, "side") == side and _effective(row, session)
    )
    if not rows:
        raise ValueError("scoring session has no effective cost proxy rule")
    return rows


def _largest_affordable_quantity(
    *,
    entry_price: Decimal,
    board_lot_size: int,
    budget: Decimal,
    fee_rows: tuple[dict[str, object], ...],
) -> Decimal:
    lot = Decimal(board_lot_size)
    quantity = (budget // (entry_price * lot)) * lot
    while quantity > 0:
        reference_value = entry_price * quantity
        entry_costs = sum(
            (_cost_amount(row, reference_value) for row in fee_rows),
            Decimal("0"),
        )
        if reference_value + entry_costs <= budget:
            return quantity
        quantity -= lot
    return Decimal("0")


def _cost_amount(row: dict[str, object], reference_value: Decimal) -> Decimal:
    return max(
        _decimal(row, "rate") * reference_value,
        _decimal(row, "minimum_amount"),
    ).quantize(_decimal(row, "rounding_quantum"), rounding=ROUND_HALF_UP)


def _validate_effective_range(row: dict[str, object], label: str) -> None:
    through = row.get("effective_through")
    if through is not None and _nullable_string(row, "effective_through") < _string(
        row, "effective_from"
    ):
        raise ValueError(f"{label} effective range is invalid")


def _reject_overlaps(
    rows: tuple[dict[str, object], ...],
    *,
    key_fields: tuple[str, ...],
    label: str,
) -> None:
    for index, first in enumerate(rows):
        first_key = tuple(_string(first, name) for name in key_fields)
        for second in rows[index + 1 :]:
            if first_key != tuple(_string(second, name) for name in key_fields):
                continue
            first_end = first.get("effective_through")
            second_end = second.get("effective_through")
            if (
                first_end is None
                or _string(second, "effective_from") <= _nullable_string(first, "effective_through")
            ) and (
                second_end is None
                or _string(first, "effective_from") <= _nullable_string(second, "effective_through")
            ):
                raise ValueError(f"{label} effective ranges overlap")


def _bar_respects_rule(row: dict[str, object], rule: dict[str, object]) -> bool:
    if _string(row, "trade_status") != "open":
        return False
    tick = _decimal(rule, "price_tick")
    prices = tuple(_decimal(row, name) for name in ("open", "high", "low", "close"))
    if any(price % tick != 0 for price in prices):
        return False
    limit_up = _nullable_decimal(row, "limit_up", positive=True)
    limit_down = _nullable_decimal(row, "limit_down", positive=True)
    return not (
        (limit_up is not None and any(price > limit_up for price in prices))
        or (limit_down is not None and any(price < limit_down for price in prices))
    )


def _effective(row: dict[str, object], session: str) -> bool:
    through = row.get("effective_through")
    return _string(row, "effective_from") <= session and (
        through is None or session <= _nullable_string(row, "effective_through")
    )


def _judgment_key(item: dict[str, object]) -> tuple[str, int, str]:
    return (_string(item, "case_alias"), _integer(item, "replicate"), _string(item, "arm"))


def _canonical_json(value: object) -> str:
    return json.dumps(
        value, ensure_ascii=True, separators=(",", ":"), sort_keys=True, allow_nan=False
    )


def _object(value: object, label: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise TypeError(f"{label} must be an object")
    mapping = cast(dict[object, object], value)
    if any(not isinstance(key, str) for key in mapping):
        raise TypeError(f"{label} must have string keys")
    return cast(dict[str, object], value)


def _closed(payload: dict[str, object], expected: set[str], label: str) -> None:
    if set(payload) != expected:
        raise ValueError(f"{label} fields are invalid")


def _string(payload: dict[str, object], name: str) -> str:
    value = payload.get(name)
    if not isinstance(value, str) or not value or value != value.strip():
        raise TypeError(f"{name} must be a non-empty trimmed string")
    return value


def _nullable_string(payload: dict[str, object], name: str) -> str:
    value = payload.get(name)
    if not isinstance(value, str) or not value or value != value.strip():
        raise TypeError(f"{name} must be a non-empty string in this state")
    return value


def _string_array(payload: dict[str, object], name: str) -> tuple[str, ...]:
    value = payload.get(name)
    if not isinstance(value, list):
        raise TypeError(f"{name} must be an array of strings")
    items = cast(list[object], value)
    if any(not isinstance(item, str) for item in items):
        raise TypeError(f"{name} must be an array of strings")
    return tuple(cast(list[str], items))


def _object_array(payload: dict[str, object], name: str) -> tuple[dict[str, object], ...]:
    value = payload.get(name)
    if not isinstance(value, list):
        raise TypeError(f"{name} must be an array of objects")
    return tuple(_object(item, name) for item in cast(list[object], value))


def _integer(payload: dict[str, object], name: str) -> int:
    value = payload.get(name)
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{name} must be an integer")
    return value


def _boolean(payload: dict[str, object], name: str) -> bool:
    value = payload.get(name)
    if not isinstance(value, bool):
        raise TypeError(f"{name} must be a boolean")
    return value


def _decimal(payload: dict[str, object], name: str, *, minimum: Decimal | None = None) -> Decimal:
    value = _string(payload, name)
    if not _DECIMAL.fullmatch(value):
        raise ValueError(f"{name} must be a canonical decimal string")
    result = Decimal(value)
    if not result.is_finite() or (minimum is not None and result < minimum):
        raise ValueError(f"{name} is outside its allowed decimal range")
    return result


def _positive_decimal(payload: dict[str, object], name: str) -> Decimal:
    result = _decimal(payload, name)
    if result <= 0:
        raise ValueError(f"{name} must be positive")
    return result


def _nullable_decimal(payload: dict[str, object], name: str, *, positive: bool) -> Decimal | None:
    if payload.get(name) is None:
        return None
    result = _decimal(payload, name)
    if positive and result <= 0:
        raise ValueError(f"{name} must be positive when present")
    return result


def _required_nullable_decimal(payload: dict[str, object], name: str) -> Decimal:
    result = _nullable_decimal(payload, name, positive=True)
    if result is None:
        raise ValueError(f"{name} is required for a filled result")
    return result


def _datetime(payload: dict[str, object], name: str) -> datetime:
    return datetime.fromisoformat(_string(payload, name).replace("Z", "+00:00"))


def _date(value: str, label: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as error:
        raise ValueError(f"{label} must be an ISO calendar date") from error


def _identifier(value: str, label: str) -> None:
    if any(character not in "abcdefghijklmnopqrstuvwxyz0123456789._-" for character in value):
        raise ValueError(f"{label} must be a lowercase identifier")


def _sha256(value: str, label: str) -> None:
    if len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
        raise ValueError(f"{label} must be a lowercase SHA-256")
