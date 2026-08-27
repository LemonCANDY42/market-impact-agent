from __future__ import annotations

import json
import os
import stat
import tempfile
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date
from decimal import ROUND_HALF_EVEN, Decimal
from pathlib import Path
from typing import cast

from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.agent_schema import validate_agent_contract
from market_impact_agent.regime_evidence import RegimeCheckpoint

_REGISTRATION_SCHEMA = "market-impact.regime-agent-validation-registration.v1"
_REPORT_SCHEMA = "market-impact.regime-agent-validation-report.v1"
_DECIMAL_QUANTUM = Decimal("0.00000001")


@dataclass(frozen=True, slots=True)
class RegimeAgentValidationCase:
    case_key: str
    treatment_skill: str
    window_start: date
    window_end: date
    checkpoints: tuple[date, ...]

    def __post_init__(self) -> None:
        if not self.case_key or not self.treatment_skill:
            raise ValueError("regime Agent validation case identities cannot be empty")
        if self.window_start > self.window_end:
            raise ValueError("regime Agent validation window is reversed")
        if len(self.checkpoints) != 3 or len(set(self.checkpoints)) != 3:
            raise ValueError("regime Agent validation case requires three unique checkpoints")
        if tuple(sorted(self.checkpoints)) != self.checkpoints:
            raise ValueError("regime Agent validation checkpoints must be ordered")
        if self.checkpoints[0] < self.window_start or self.checkpoints[-1] > self.window_end:
            raise ValueError("regime Agent validation checkpoint is outside its window")

    def to_dict(self) -> dict[str, object]:
        return {
            "case_key": self.case_key,
            "treatment_skill": self.treatment_skill,
            "window_start": self.window_start.isoformat(),
            "window_end": self.window_end.isoformat(),
            "checkpoints": [item.isoformat() for item in self.checkpoints],
        }


@dataclass(frozen=True, slots=True)
class RegimeAgentValidationRegistration:
    validation_id: str
    version: str
    dataset_id: str
    dataset_hash: str
    study_registration_id: str
    study_registration_hash: str
    panel_id: str
    manifest_id: str
    qualification_report_id: str
    provider_profile_id: str
    replicate_count: int
    total_cost_cap_microusd: int
    outcomes_opened: bool
    cases: tuple[RegimeAgentValidationCase, ...]

    @classmethod
    def build(
        cls,
        *,
        version: str,
        dataset_id: str,
        dataset_hash: str,
        study_registration_id: str,
        study_registration_hash: str,
        panel_id: str,
        manifest_id: str,
        qualification_report_id: str,
        provider_profile_id: str,
        replicate_count: int,
        total_cost_cap_microusd: int,
        outcomes_opened: bool,
        cases: tuple[RegimeAgentValidationCase, ...],
    ) -> RegimeAgentValidationRegistration:
        if replicate_count != 3:
            raise ValueError("regime Agent validation freezes three replicates per arm")
        if not 0 < total_cost_cap_microusd <= 20_000_000:
            raise ValueError("regime Agent validation cost cap must be within 20 USD")
        if outcomes_opened is not True:
            raise ValueError("regime Agent validation cases must be declared outcome-opened")
        case_keys = tuple(item.case_key for item in cases)
        if len(cases) < 2 or len(set(case_keys)) != len(case_keys):
            raise ValueError("regime Agent validation requires unique multiple cases")
        core: dict[str, object] = {
            "schema_version": _REGISTRATION_SCHEMA,
            "version": version,
            "dataset_id": dataset_id,
            "dataset_hash": dataset_hash,
            "study_registration_id": study_registration_id,
            "study_registration_hash": study_registration_hash,
            "panel_id": panel_id,
            "manifest_id": manifest_id,
            "qualification_report_id": qualification_report_id,
            "provider_profile_id": provider_profile_id,
            "replicate_count": replicate_count,
            "total_cost_cap_microusd": total_cost_cap_microusd,
            "outcomes_opened": outcomes_opened,
            "selection_rule": "registered_window_first_middle_last",
            "cases": [item.to_dict() for item in cases],
            "inference_eligible": False,
            "broker_reachability": False,
            "execution_capability": "none",
        }
        return cls(
            validation_id=f"regime-agent-validation-{canonical_hash(core)}",
            version=version,
            dataset_id=dataset_id,
            dataset_hash=dataset_hash,
            study_registration_id=study_registration_id,
            study_registration_hash=study_registration_hash,
            panel_id=panel_id,
            manifest_id=manifest_id,
            qualification_report_id=qualification_report_id,
            provider_profile_id=provider_profile_id,
            replicate_count=replicate_count,
            total_cost_cap_microusd=total_cost_cap_microusd,
            outcomes_opened=outcomes_opened,
            cases=cases,
        )

    def to_dict(self) -> dict[str, object]:
        core: dict[str, object] = {
            "schema_version": _REGISTRATION_SCHEMA,
            "version": self.version,
            "dataset_id": self.dataset_id,
            "dataset_hash": self.dataset_hash,
            "study_registration_id": self.study_registration_id,
            "study_registration_hash": self.study_registration_hash,
            "panel_id": self.panel_id,
            "manifest_id": self.manifest_id,
            "qualification_report_id": self.qualification_report_id,
            "provider_profile_id": self.provider_profile_id,
            "replicate_count": self.replicate_count,
            "total_cost_cap_microusd": self.total_cost_cap_microusd,
            "outcomes_opened": self.outcomes_opened,
            "selection_rule": "registered_window_first_middle_last",
            "cases": [item.to_dict() for item in self.cases],
            "inference_eligible": False,
            "broker_reachability": False,
            "execution_capability": "none",
        }
        return {**core, "validation_id": self.validation_id}


def load_regime_agent_validation_registration(
    path: Path,
) -> RegimeAgentValidationRegistration:
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise TypeError("regime Agent validation registration must be an object")
    payload = cast(dict[str, object], raw)
    errors = validate_agent_contract(payload, "regime-agent-validation-registration.schema.json")
    if errors:
        raise ValueError("; ".join(errors))
    raw_cases = payload.get("cases")
    if not isinstance(raw_cases, list):
        raise TypeError("regime Agent validation cases must be an array")
    case_items = cast(list[object], raw_cases)
    cases = tuple(_validation_case(cast(Mapping[str, object], item)) for item in case_items)
    registration = RegimeAgentValidationRegistration.build(
        version=_string(payload, "version"),
        dataset_id=_string(payload, "dataset_id"),
        dataset_hash=_string(payload, "dataset_hash"),
        study_registration_id=_string(payload, "study_registration_id"),
        study_registration_hash=_string(payload, "study_registration_hash"),
        panel_id=_string(payload, "panel_id"),
        manifest_id=_string(payload, "manifest_id"),
        qualification_report_id=_string(payload, "qualification_report_id"),
        provider_profile_id=_string(payload, "provider_profile_id"),
        replicate_count=_integer(payload, "replicate_count"),
        total_cost_cap_microusd=_integer(payload, "total_cost_cap_microusd"),
        outcomes_opened=payload.get("outcomes_opened") is True,
        cases=cases,
    )
    if payload.get("validation_id") != registration.validation_id:
        raise ValueError("regime Agent validation_id does not match canonical content")
    return registration


def select_validation_checkpoints(
    checkpoints: tuple[RegimeCheckpoint, ...],
    *,
    window_start: date,
    window_end: date,
    checkpoint_count: int,
) -> tuple[RegimeCheckpoint, ...]:
    if checkpoint_count != 3:
        raise ValueError("regime Agent validation freezes exactly three checkpoints")
    eligible = tuple(
        sorted(
            (item for item in checkpoints if window_start <= item.session_date <= window_end),
            key=lambda item: item.session_date,
        )
    )
    if len(eligible) < checkpoint_count:
        raise ValueError("regime Agent validation window requires at least three checkpoints")
    return (eligible[0], eligible[len(eligible) // 2], eligible[-1])


def build_regime_agent_validation_report(
    *,
    registration: RegimeAgentValidationRegistration,
    case_reports: tuple[Mapping[str, object], ...],
) -> dict[str, object]:
    expected = {item.case_key: item for item in registration.cases}
    observed = {cast(str, item.get("case_key")): item for item in case_reports}
    if len(observed) != len(case_reports) or set(observed) != set(expected):
        raise ValueError("regime Agent validation requires the exact registered case set")

    case_results: list[dict[str, object]] = []
    formal_model_cost = 0
    prior_diagnostic_cost = 0
    checkpoint_count = 0
    formal_run_count = 0
    control_hits = 0
    treatment_hits = 0
    helpful = 0
    harmful = 0
    same = 0
    control_returns: list[Decimal] = []
    treatment_returns: list[Decimal] = []
    baseline_returns: list[Decimal] = []
    equal_sector_returns: list[Decimal] = []
    momentum_returns: list[Decimal] = []
    control_drawdowns: list[Decimal] = []
    treatment_drawdowns: list[Decimal] = []
    treatment_control_wins = 0
    treatment_baseline_wins = 0
    treatment_equal_sector_wins = 0
    treatment_momentum_wins = 0
    for case_key in sorted(expected):
        case = expected[case_key]
        report = observed[case_key]
        _validate_case_report(registration, case, report)
        report_checkpoint_count = _integer(report, "checkpoint_count")
        report_run_count = _integer(report, "formal_run_count")
        checkpoint_count += report_checkpoint_count
        formal_run_count += report_run_count
        control, treatment = _arms(report)
        control_return = _path_decimal(control, "total_return")
        treatment_return = _path_decimal(treatment, "total_return")
        baseline_return = _baseline_return(report, "primary_buy_and_hold")
        equal_sector_return = _baseline_return(report, "equal_sector_buy_and_hold")
        momentum_return = _baseline_return(report, "lagged_sector_momentum")
        control_drawdown = _path_decimal(control, "max_drawdown")
        treatment_drawdown = _path_decimal(treatment, "max_drawdown")
        control_returns.append(control_return)
        treatment_returns.append(treatment_return)
        baseline_returns.append(baseline_return)
        equal_sector_returns.append(equal_sector_return)
        momentum_returns.append(momentum_return)
        control_drawdowns.append(control_drawdown)
        treatment_drawdowns.append(treatment_drawdown)
        control_hits += _integer(control, "directional_hit_count")
        treatment_hits += _integer(treatment, "directional_hit_count")
        treatment_control_wins += int(treatment_return > control_return)
        treatment_baseline_wins += int(treatment_return > baseline_return)
        treatment_equal_sector_wins += int(treatment_return > equal_sector_return)
        treatment_momentum_wins += int(treatment_return > momentum_return)
        increment = _mapping(report, "skill_increment")
        case_helpful = _integer(increment, "helpful_checkpoint_count")
        case_harmful = _integer(increment, "harmful_checkpoint_count")
        case_same = _integer(increment, "same_decision_checkpoint_count")
        helpful += case_helpful
        harmful += case_harmful
        same += case_same
        cost = _mapping(report, "cost")
        case_cost = _integer(cost, "formal_model_cost_microusd")
        case_prior_cost = _integer(
            cost,
            "prior_invalid_or_superseded_diagnostic_cost_microusd",
        )
        case_all_actual_cost = _integer(cost, "all_actual_model_cost_microusd")
        if case_all_actual_cost != case_cost + case_prior_cost:
            raise ValueError("regime Agent validation case cost ledger does not reconcile")
        if _integer(cost, "hard_cap_microusd") != registration.total_cost_cap_microusd:
            raise ValueError("regime Agent validation case cost cap drifted")
        checkpoint_actual_cost = sum(
            _integer(cast(Mapping[str, object], item), "actual_model_cost_microusd")
            for item in cast(list[object], report["checkpoint_results"])
            if isinstance(item, Mapping)
        )
        if checkpoint_actual_cost != case_cost:
            raise ValueError("regime Agent validation formal model cost does not reconcile")
        formal_model_cost += case_cost
        prior_diagnostic_cost += case_prior_cost
        case_results.append(
            {
                "case_key": case_key,
                "treatment_skill": case.treatment_skill,
                "checkpoints": [item.isoformat() for item in case.checkpoints],
                "case_report_id": _string(report, "report_id"),
                "control_total_return": _decimal_text(control_return),
                "routed_skill_total_return": _decimal_text(treatment_return),
                "primary_baseline_total_return": _decimal_text(baseline_return),
                "equal_sector_baseline_total_return": _decimal_text(equal_sector_return),
                "lagged_sector_momentum_total_return": _decimal_text(momentum_return),
                "control_directional_hit_count": _integer(control, "directional_hit_count"),
                "routed_skill_directional_hit_count": _integer(treatment, "directional_hit_count"),
                "formal_model_cost_microusd": case_cost,
            }
        )
    all_actual_cost = formal_model_cost + prior_diagnostic_cost
    if all_actual_cost > registration.total_cost_cap_microusd:
        raise ValueError("regime Agent validation exceeds the registered cost cap")
    aggregate: dict[str, object] = {
        "control_directional_hit_count": control_hits,
        "routed_skill_directional_hit_count": treatment_hits,
        "checkpoint_count": checkpoint_count,
        "control_directional_hit_rate": _ratio(control_hits, checkpoint_count),
        "routed_skill_directional_hit_rate": _ratio(treatment_hits, checkpoint_count),
        "control_mean_case_return": _mean(control_returns),
        "routed_skill_mean_case_return": _mean(treatment_returns),
        "primary_baseline_mean_case_return": _mean(baseline_returns),
        "equal_sector_baseline_mean_case_return": _mean(equal_sector_returns),
        "lagged_sector_momentum_mean_case_return": _mean(momentum_returns),
        "routed_skill_case_win_count_vs_control": treatment_control_wins,
        "routed_skill_case_win_count_vs_primary": treatment_baseline_wins,
        "routed_skill_case_win_count_vs_equal_sector": treatment_equal_sector_wins,
        "routed_skill_case_win_count_vs_lagged_sector_momentum": treatment_momentum_wins,
        "helpful_checkpoint_count": helpful,
        "harmful_checkpoint_count": harmful,
        "same_decision_checkpoint_count": same,
        "control_worst_case_return": _decimal_text(min(control_returns)),
        "routed_skill_worst_case_return": _decimal_text(min(treatment_returns)),
        "control_worst_case_max_drawdown": _decimal_text(min(control_drawdowns)),
        "routed_skill_worst_case_max_drawdown": _decimal_text(min(treatment_drawdowns)),
    }
    core: dict[str, object] = {
        "schema_version": _REPORT_SCHEMA,
        "validation_id": registration.validation_id,
        "dataset_id": registration.dataset_id,
        "study_registration_id": registration.study_registration_id,
        "panel_id": registration.panel_id,
        "manifest_id": registration.manifest_id,
        "qualification_report_id": registration.qualification_report_id,
        "provider_profile_id": registration.provider_profile_id,
        "case_count": len(case_results),
        "checkpoint_count": checkpoint_count,
        "formal_run_count": formal_run_count,
        "cases": case_results,
        "aggregate": aggregate,
        "cost": {
            "formal_model_cost_microusd": formal_model_cost,
            "prior_invalid_or_superseded_diagnostic_cost_microusd": (prior_diagnostic_cost),
            "all_actual_model_cost_microusd": all_actual_cost,
            "hard_cap_microusd": registration.total_cost_cap_microusd,
            "within_budget": True,
        },
        "limitations": [
            "opened retrospective development cases do not provide out-of-sample evidence",
            "case returns are summarized independently and are never compounded across overlaps",
            "routed Skills differ by registered case and are not one pooled treatment",
        ],
        "inference_eligible": False,
        "broker_reachability": False,
        "execution_capability": "none",
    }
    return {
        **core,
        "report_id": f"regime-agent-validation-report-{canonical_hash(core)}",
    }


def write_regime_agent_validation_report(
    report: Mapping[str, object],
    *,
    root: Path = Path(".market-impact") / "regime" / "agent-validations" / "reports",
) -> Path:
    if report.get("schema_version") != _REPORT_SCHEMA:
        raise ValueError("unsupported regime Agent validation report schema")
    errors = validate_agent_contract(report, "regime-agent-validation-report.schema.json")
    if errors:
        raise ValueError("; ".join(errors))
    report_id = _string(report, "report_id")
    core = {key: value for key, value in report.items() if key != "report_id"}
    if report_id != f"regime-agent-validation-report-{canonical_hash(core)}":
        raise ValueError("regime Agent validation report_id does not match content")
    if (
        report.get("inference_eligible") is not False
        or report.get("execution_capability") != "none"
    ):
        raise ValueError("regime Agent validation report exceeds diagnostic authority")
    destination = root / f"{report_id}.json"
    _write_private_json(destination, dict(report))
    return destination


def _validate_case_report(
    registration: RegimeAgentValidationRegistration,
    case: RegimeAgentValidationCase,
    report: Mapping[str, object],
) -> None:
    schema_errors = validate_agent_contract(
        dict(report), "regime-agent-experiment-report.schema.json"
    )
    if schema_errors:
        raise ValueError("; ".join(schema_errors))
    report_id = _string(report, "report_id")
    core = {key: value for key, value in report.items() if key != "report_id"}
    if report_id != f"regime-agent-experiment-report-{canonical_hash(core)}":
        raise ValueError("regime Agent validation case report_id does not match content")
    if (
        report.get("schema_version") != "market-impact.regime-agent-experiment-report.v1"
        or report.get("treatment_skill") != case.treatment_skill
        or report.get("provider_profile_id") != registration.provider_profile_id
        or report.get("panel_id") != registration.panel_id
        or report.get("manifest_id") != registration.manifest_id
        or report.get("qualification_report_id") != registration.qualification_report_id
        or report.get("inference_eligible") is not False
        or report.get("broker_reachability") is not False
        or report.get("execution_capability") != "none"
    ):
        raise ValueError("regime Agent validation case report exceeds its registered contract")
    raw_checkpoints = report.get("checkpoint_results")
    if not isinstance(raw_checkpoints, list):
        raise TypeError("regime Agent validation case checkpoints are invalid")
    checkpoint_items = cast(list[object], raw_checkpoints)
    observed_dates = tuple(
        date.fromisoformat(_string(cast(Mapping[str, object], item), "session_date"))
        for item in checkpoint_items
        if isinstance(item, Mapping)
    )
    if observed_dates != case.checkpoints:
        raise ValueError(
            "regime Agent validation case report checkpoints do not match registration"
        )
    if _integer(report, "checkpoint_count") != len(case.checkpoints):
        raise ValueError("regime Agent validation checkpoint count is invalid")
    if _integer(report, "formal_run_count") != len(case.checkpoints) * 6:
        raise ValueError("regime Agent validation formal run count is invalid")


def _validation_case(payload: Mapping[str, object]) -> RegimeAgentValidationCase:
    raw_checkpoints = payload.get("checkpoints")
    if not isinstance(raw_checkpoints, list):
        raise TypeError("regime Agent validation checkpoints must be date strings")
    checkpoint_items = cast(list[object], raw_checkpoints)
    if not all(isinstance(item, str) for item in checkpoint_items):
        raise TypeError("regime Agent validation checkpoints must be date strings")
    return RegimeAgentValidationCase(
        case_key=_string(payload, "case_key"),
        treatment_skill=_string(payload, "treatment_skill"),
        window_start=date.fromisoformat(_string(payload, "window_start")),
        window_end=date.fromisoformat(_string(payload, "window_end")),
        checkpoints=tuple(date.fromisoformat(cast(str, item)) for item in checkpoint_items),
    )


def _arms(report: Mapping[str, object]) -> tuple[Mapping[str, object], Mapping[str, object]]:
    raw = report.get("arms")
    if not isinstance(raw, list):
        raise TypeError("regime Agent validation case arms are invalid")
    arm_items = cast(list[object], raw)
    if len(arm_items) != 2 or not all(isinstance(item, Mapping) for item in arm_items):
        raise TypeError("regime Agent validation case arms are invalid")
    typed = cast(list[Mapping[str, object]], arm_items)
    if typed[0].get("arm_id") != "general_control" or not str(
        typed[1].get("arm_id", "")
    ).startswith("general_plus_"):
        raise ValueError("regime Agent validation case arms do not match the paired contract")
    return typed[0], typed[1]


def _baseline_return(report: Mapping[str, object], baseline_id: str) -> Decimal:
    raw = report.get("baselines")
    if not isinstance(raw, list):
        raise TypeError("regime Agent validation baselines are invalid")
    baseline_items = cast(list[object], raw)
    for item in baseline_items:
        if isinstance(item, Mapping):
            baseline = cast(Mapping[str, object], item)
            if baseline.get("baseline_id") == baseline_id:
                return _path_decimal(baseline, "total_return")
    raise ValueError(f"regime Agent validation lacks baseline: {baseline_id}")


def _path_decimal(item: Mapping[str, object], field: str) -> Decimal:
    path = _mapping(item, "path_metrics")
    return Decimal(_string(path, field))


def _mapping(item: Mapping[str, object], field: str) -> Mapping[str, object]:
    value = item.get(field)
    if not isinstance(value, Mapping):
        raise TypeError(f"regime Agent validation {field} must be an object")
    return cast(Mapping[str, object], value)


def _string(item: Mapping[str, object], field: str) -> str:
    value = item.get(field)
    if not isinstance(value, str) or not value:
        raise TypeError(f"regime Agent validation {field} must be a string")
    return value


def _integer(item: Mapping[str, object], field: str) -> int:
    value = item.get(field)
    if not isinstance(value, int) or isinstance(value, bool):
        raise TypeError(f"regime Agent validation {field} must be an integer")
    return value


def _decimal_text(value: Decimal) -> str:
    return format(value.quantize(_DECIMAL_QUANTUM, rounding=ROUND_HALF_EVEN), "f")


def _ratio(numerator: int, denominator: int) -> str:
    return _decimal_text(Decimal(numerator) / Decimal(denominator))


def _mean(values: list[Decimal]) -> str:
    return _decimal_text(sum(values, start=Decimal(0)) / Decimal(len(values)))


def _write_private_json(path: Path, payload: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(path.parent, 0o700)
    descriptor, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temporary_path = Path(temporary)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, indent=2, sort_keys=True)
            handle.write("\n")
        os.chmod(temporary_path, stat.S_IRUSR | stat.S_IWUSR)
        os.replace(temporary_path, path)
        os.chmod(path, stat.S_IRUSR | stat.S_IWUSR)
    finally:
        if temporary_path.exists():
            temporary_path.unlink()
