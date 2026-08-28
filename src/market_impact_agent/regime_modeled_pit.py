from __future__ import annotations

import json
import os
import stat
import tempfile
from collections.abc import Mapping
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, cast

from market_impact_agent.agent_contracts import PatternPack, canonical_hash
from market_impact_agent.agent_schema import validate_agent_contract
from market_impact_agent.market_regimes import (
    MarketRegimeCase,
    MarketRegimeDataset,
    RegimeSeries,
    ValidatedRegimePanel,
)
from market_impact_agent.method_skills import MethodSkill
from market_impact_agent.regime_agent_experiment import (
    CompletedRegimeCheckpointExperiment,
    PairedExecutionAuditPaths,
    RegimeCheckpointBundle,
    build_regime_agent_experiment_report,
    eligible_horizon_sessions,
    materialize_regime_checkpoint_bundle_from_visible_records,
)
from market_impact_agent.regime_evidence import (
    RegimeCheckpoint,
    RegimeEvidenceAvailabilityBasis,
    RegimeEvidenceManifest,
    RegimeEvidenceRecord,
    generate_regime_checkpoints,
)
from market_impact_agent.regime_market_evidence import (
    panel_authority_source_ref,
    panel_series_as_of_hash,
)
from market_impact_agent.regime_study import (
    RegimeSourceRequirement,
    RegimeStudyCase,
    RegimeStudyRegistration,
    RegimeStudySource,
)

if TYPE_CHECKING:
    from market_impact_agent.usage_ledger import UsageLedgerUnion

REGIME_MODELED_PIT_POLICY_SCHEMA = "market-impact.regime-modeled-pit-policy.v1"
REGIME_MODELED_PIT_QUALIFICATION_SCHEMA = "market-impact.regime-modeled-pit-qualification-report.v1"
REGIME_MODELED_PIT_AGENT_VALIDATION_SCHEMA = (
    "market-impact.regime-modeled-pit-agent-validation-registration.v1"
)
REGIME_MODELED_PIT_AGENT_VALIDATION_REPORT_SCHEMA = (
    "market-impact.regime-modeled-pit-agent-validation-report.v1"
)
_CATEGORIES = (
    "market_price",
    "industry_price",
    "official_context",
    "macro_vintage",
    "established_news",
    "positioning_or_expectations",
    "issuer_or_sector_fundamentals",
)
_PRIVATE_ROOT = Path(".market-impact") / "regime" / "evidence" / "modeled-qualifications"


@dataclass(frozen=True, slots=True)
class RegimeModeledPitCategoryRule:
    category: str
    visibility_basis: str
    allowed_availability_bases: tuple[RegimeEvidenceAvailabilityBasis, ...]
    safety_delay_seconds: int

    def __post_init__(self) -> None:
        if self.category not in _CATEGORIES:
            raise ValueError("modeled-PIT rule category is unsupported")
        expected_basis = (
            "prior_session_panel_snapshot"
            if self.category in {"market_price", "industry_price"}
            else "record_available_at_plus_safety_delay"
        )
        if self.visibility_basis != expected_basis:
            raise ValueError("modeled-PIT rule visibility basis does not match its category")
        if not self.allowed_availability_bases or len(self.allowed_availability_bases) != len(
            set(self.allowed_availability_bases)
        ):
            raise ValueError("modeled-PIT rule requires unique availability bases")
        if not 0 <= self.safety_delay_seconds <= 86_400:
            raise ValueError("modeled-PIT safety delay must be within one day")

    def to_dict(self) -> dict[str, object]:
        return {
            "category": self.category,
            "visibility_basis": self.visibility_basis,
            "allowed_availability_bases": [item.value for item in self.allowed_availability_bases],
            "safety_delay_seconds": self.safety_delay_seconds,
        }


@dataclass(frozen=True, slots=True)
class RegimeModeledPitPolicy:
    policy_id: str
    version: str
    description: str
    category_rules: tuple[RegimeModeledPitCategoryRule, ...]

    def __post_init__(self) -> None:
        for name in ("version", "description"):
            value = cast(str, getattr(self, name))
            if not value or value != value.strip():
                raise ValueError(f"modeled-PIT policy {name} must be non-empty and trimmed")
        categories = tuple(item.category for item in self.category_rules)
        if len(categories) != len(set(categories)) or set(categories) != set(_CATEGORIES):
            raise ValueError("modeled-PIT policy must define every category exactly once")
        if self.policy_id != self.expected_policy_id:
            raise ValueError("modeled-PIT policy_id does not match content")

    @property
    def expected_policy_id(self) -> str:
        return f"regime-modeled-pit-policy-{canonical_hash(self.core_dict())}"

    @property
    def rule_by_category(self) -> dict[str, RegimeModeledPitCategoryRule]:
        return {item.category: item for item in self.category_rules}

    def core_dict(self) -> dict[str, object]:
        return {
            "schema_version": REGIME_MODELED_PIT_POLICY_SCHEMA,
            "version": self.version,
            "description": self.description,
            "category_rules": [item.to_dict() for item in self.category_rules],
            "authority_treatment": ("reported_as_gap_not_required_for_exploratory_replay"),
            "latency_assumptions_calibrated": False,
            "inference_eligible": False,
            "broker_reachability": False,
            "execution_capability": "none",
        }

    def to_dict(self) -> dict[str, object]:
        return {**self.core_dict(), "policy_id": self.policy_id}

    @classmethod
    def build(
        cls,
        *,
        version: str,
        description: str,
        category_rules: tuple[RegimeModeledPitCategoryRule, ...],
    ) -> RegimeModeledPitPolicy:
        provisional = cls.__new__(cls)
        object.__setattr__(provisional, "policy_id", "")
        object.__setattr__(provisional, "version", version)
        object.__setattr__(provisional, "description", description)
        object.__setattr__(provisional, "category_rules", category_rules)
        policy_id = f"regime-modeled-pit-policy-{canonical_hash(provisional.core_dict())}"
        return cls(
            policy_id=policy_id,
            version=version,
            description=description,
            category_rules=category_rules,
        )


def load_regime_modeled_pit_policy(path: Path) -> RegimeModeledPitPolicy:
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise TypeError("modeled-PIT policy must be an object")
    payload = cast(dict[str, object], raw)
    errors = validate_agent_contract(payload, "regime-modeled-pit-policy.schema.json")
    if errors:
        raise ValueError("; ".join(errors))
    rules = tuple(_rule_from_dict(item) for item in _object_list(payload, "category_rules"))
    policy = RegimeModeledPitPolicy(
        policy_id=_string(payload, "policy_id"),
        version=_string(payload, "version"),
        description=_string(payload, "description"),
        category_rules=rules,
    )
    if policy.to_dict() != payload:
        raise ValueError("modeled-PIT policy does not match canonical content")
    return policy


def qualify_regime_evidence_modeled_pit(
    dataset: MarketRegimeDataset,
    validated_panel: ValidatedRegimePanel,
    registration: RegimeStudyRegistration,
    manifest: RegimeEvidenceManifest,
    strict_qualification_report: Mapping[str, object],
    policy: RegimeModeledPitPolicy,
) -> dict[str, object]:
    _validate_bindings(
        dataset,
        validated_panel,
        registration,
        manifest,
        strict_qualification_report,
    )
    source_by_id = {item.source_id: item for item in registration.source_catalog}
    study_by_case = {item.case_key: item for item in registration.cases}
    strict_by_case = {
        _string(item, "case_key"): item
        for item in _mapping_list(strict_qualification_report, "cases")
    }
    series_by_id = _series_by_id(validated_panel)
    cases: list[dict[str, object]] = []
    checkpoint_count = 0
    eligible_checkpoint_count = 0
    for market_case in dataset.cases:
        study_case = study_by_case[market_case.case_key]
        primary = series_by_id.get(market_case.primary_market_index)
        checkpoints = (
            ()
            if primary is None
            else generate_regime_checkpoints(
                market_case,
                study_case,
                protocol=registration.checkpoint_protocol,
                trading_dates=tuple(_row_date(row) for row in primary.rows),
            )
        )
        strict_checkpoints = {
            _string(item, "session_date"): item
            for item in _mapping_list(strict_by_case[market_case.case_key], "checkpoints")
        }
        results: list[dict[str, object]] = []
        for checkpoint in checkpoints:
            strict_checkpoint = strict_checkpoints.get(checkpoint.session_date.isoformat())
            if strict_checkpoint is None:
                raise ValueError("strict qualification does not cover a modeled checkpoint")
            strict_requirements = {
                _string(item, "category"): item
                for item in _mapping_list(strict_checkpoint, "requirements")
            }
            requirements = [
                _qualify_requirement(
                    requirement,
                    checkpoint=checkpoint,
                    market_case=market_case,
                    study_case=study_case,
                    registration=registration,
                    validated_panel=validated_panel,
                    source_by_id=source_by_id,
                    records=manifest.records,
                    rule=policy.rule_by_category[requirement.category],
                    strict_result=strict_requirements[requirement.category],
                )
                for requirement in study_case.source_requirements
            ]
            event_revelation = _qualify_event_revelation(
                checkpoint=checkpoint,
                market_case=market_case,
                study_case=study_case,
                source_by_id=source_by_id,
                records=manifest.records,
                rules=policy.rule_by_category,
                strict_result=_mapping(strict_checkpoint, "event_revelation"),
            )
            ready = all(item["ready"] is True for item in requirements) and bool(
                event_revelation["modeled_ready"]
            )
            eligible_checkpoint_count += int(ready)
            checkpoint_count += 1
            results.append(
                {
                    "session_date": checkpoint.session_date.isoformat(),
                    "cutoff_at": _timestamp(checkpoint.cutoff_at),
                    "ready": ready,
                    "strict_ready": strict_checkpoint.get("ready") is True,
                    "event_revelation": event_revelation,
                    "requirements": requirements,
                }
            )
        case_eligible = sum(item["ready"] is True for item in results)
        cases.append(
            {
                "case_key": market_case.case_key,
                "checkpoint_count": len(checkpoints),
                "eligible_checkpoint_count": case_eligible,
                "all_checkpoints_modeled_ready": bool(checkpoints)
                and case_eligible == len(checkpoints),
                "checkpoints": results,
            }
        )
    all_ready = checkpoint_count > 0 and eligible_checkpoint_count == checkpoint_count
    core: dict[str, object] = {
        "schema_version": REGIME_MODELED_PIT_QUALIFICATION_SCHEMA,
        "policy_id": policy.policy_id,
        "strict_qualification_report_id": _string(strict_qualification_report, "report_id"),
        "dataset_id": dataset.dataset_id,
        "registration_id": registration.registration_id,
        "panel_id": validated_panel.panel_id,
        "manifest_id": manifest.manifest_id,
        "outcomes_opened": registration.outcomes_opened,
        "case_count": len(cases),
        "checkpoint_count": checkpoint_count,
        "eligible_checkpoint_count": eligible_checkpoint_count,
        "all_checkpoints_modeled_ready": all_ready,
        "exploratory_agent_run_eligible": eligible_checkpoint_count > 0,
        "strict_pit_eligible": False,
        "agent_effectiveness_claim_eligible": False,
        "inference_eligible": False,
        "claim_scope": "opened_modeled_pit_process_diagnostic_only",
        "cases": cases,
        "limitations": [
            "historical authority gaps remain and are reported separately for every category",
            "safety delays are frozen stress assumptions and are not prospectively calibrated",
            "retrospective opened cases support process diagnostics only, not alpha claims",
            "modeled-PIT qualification cannot authorize paper or live execution",
        ],
        "broker_reachability": False,
        "execution_capability": "none",
    }
    return {
        **core,
        "report_id": f"regime-modeled-pit-qualification-report-{canonical_hash(core)}",
    }


def write_regime_modeled_pit_qualification_report(
    report: Mapping[str, object],
    *,
    root: Path = _PRIVATE_ROOT,
) -> Path:
    errors = validate_agent_contract(
        dict(report), "regime-modeled-pit-qualification-report.schema.json"
    )
    if errors:
        raise ValueError("; ".join(errors))
    report_id = _string(report, "report_id")
    core = {key: value for key, value in report.items() if key != "report_id"}
    expected = f"regime-modeled-pit-qualification-report-{canonical_hash(core)}"
    if report_id != expected:
        raise ValueError("modeled-PIT qualification report_id does not match content")
    if (
        report.get("strict_pit_eligible") is not False
        or report.get("inference_eligible") is not False
        or report.get("agent_effectiveness_claim_eligible") is not False
        or report.get("execution_capability") != "none"
    ):
        raise ValueError("modeled-PIT qualification exceeds exploratory authority")
    destination = root / f"{report_id}.json"
    _write_private_json(destination, dict(report))
    return destination


def load_regime_modeled_pit_qualification_report(path: Path) -> dict[str, object]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise TypeError("modeled-PIT qualification report must be an object")
    report = cast(dict[str, object], raw)
    errors = validate_agent_contract(report, "regime-modeled-pit-qualification-report.schema.json")
    if errors:
        raise ValueError("; ".join(errors))
    report_id = _string(report, "report_id")
    core = {key: value for key, value in report.items() if key != "report_id"}
    if report_id != f"regime-modeled-pit-qualification-report-{canonical_hash(core)}":
        raise ValueError("modeled-PIT qualification report_id does not match content")
    return report


def load_regime_modeled_pit_agent_validation_registration(
    path: Path,
) -> dict[str, object]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise TypeError("modeled-PIT Agent validation registration must be an object")
    registration = cast(dict[str, object], raw)
    errors = validate_agent_contract(
        registration,
        "regime-modeled-pit-agent-validation-registration.schema.json",
    )
    if errors:
        raise ValueError("; ".join(errors))
    validation_id = _string(registration, "validation_id")
    core = {key: value for key, value in registration.items() if key != "validation_id"}
    expected = f"regime-modeled-pit-agent-validation-{canonical_hash(core)}"
    if validation_id != expected:
        raise ValueError("modeled-PIT Agent validation_id does not match content")
    cases = _mapping_list(registration, "cases")
    case_keys = tuple(_string(item, "case_key") for item in cases)
    if len(case_keys) != len(set(case_keys)):
        raise ValueError("modeled-PIT Agent validation case keys must be unique")
    for case in cases:
        checkpoints = tuple(date.fromisoformat(item) for item in _string_list(case, "checkpoints"))
        if checkpoints != tuple(sorted(checkpoints)):
            raise ValueError("modeled-PIT Agent validation checkpoints must be ordered")
        if not (
            date.fromisoformat(_string(case, "window_start"))
            <= checkpoints[0]
            <= checkpoints[-1]
            <= date.fromisoformat(_string(case, "window_end"))
        ):
            raise ValueError("modeled-PIT Agent validation checkpoints leave their window")
    return registration


def build_regime_modeled_pit_agent_validation_report(
    *,
    validation_registration: Mapping[str, object],
    dataset: MarketRegimeDataset,
    validated_panel: ValidatedRegimePanel,
    study_registration: RegimeStudyRegistration,
    manifest: RegimeEvidenceManifest,
    strict_qualification_report: Mapping[str, object],
    modeled_qualification_report: Mapping[str, object],
    policy: RegimeModeledPitPolicy,
    completed_by_case: Mapping[str, tuple[CompletedRegimeCheckpointExperiment, ...]],
    usage_union: UsageLedgerUnion,
    invalid_horizon_report: Mapping[str, object],
) -> dict[str, object]:
    registration_errors = validate_agent_contract(
        dict(validation_registration),
        "regime-modeled-pit-agent-validation-registration.schema.json",
    )
    if registration_errors:
        raise ValueError("; ".join(registration_errors))
    validation_id = _string(validation_registration, "validation_id")
    registration_core = {
        key: value for key, value in validation_registration.items() if key != "validation_id"
    }
    if validation_id != (
        f"regime-modeled-pit-agent-validation-{canonical_hash(registration_core)}"
    ):
        raise ValueError("modeled-PIT Agent validation registration is not content-addressed")
    _validate_bindings(
        dataset,
        validated_panel,
        study_registration,
        manifest,
        strict_qualification_report,
    )
    modeled_errors = validate_agent_contract(
        dict(modeled_qualification_report),
        "regime-modeled-pit-qualification-report.schema.json",
    )
    if modeled_errors:
        raise ValueError("; ".join(modeled_errors))
    modeled_report_id = _string(modeled_qualification_report, "report_id")
    modeled_core = {
        key: value for key, value in modeled_qualification_report.items() if key != "report_id"
    }
    if modeled_report_id != (
        f"regime-modeled-pit-qualification-report-{canonical_hash(modeled_core)}"
    ):
        raise ValueError("modeled-PIT qualification report is not content-addressed")
    bindings = {
        "dataset_id": dataset.dataset_id,
        "dataset_hash": dataset.dataset_hash,
        "study_registration_id": study_registration.registration_id,
        "study_registration_hash": study_registration.registration_hash,
        "panel_id": validated_panel.panel_id,
        "manifest_id": manifest.manifest_id,
        "strict_qualification_report_id": _string(strict_qualification_report, "report_id"),
        "modeled_qualification_report_id": modeled_report_id,
        "policy_id": policy.policy_id,
    }
    for field, expected in bindings.items():
        if validation_registration.get(field) != expected:
            raise ValueError(f"modeled-PIT Agent validation registration drifted at {field}")
    if (
        modeled_qualification_report.get("strict_qualification_report_id")
        != bindings["strict_qualification_report_id"]
        or modeled_qualification_report.get("manifest_id") != manifest.manifest_id
        or modeled_qualification_report.get("policy_id") != policy.policy_id
    ):
        raise ValueError("modeled-PIT Agent validation qualification bindings drifted")
    if (
        validation_registration.get("strict_pit_eligible") is not False
        or validation_registration.get("inference_eligible") is not False
        or validation_registration.get("execution_capability") != "none"
    ):
        raise ValueError("modeled-PIT Agent validation exceeds diagnostic authority")
    invalid_horizon_experiment_id = _string(invalid_horizon_report, "experiment_id")
    invalid_horizon_report_id = _string(invalid_horizon_report, "report_id")
    invalid_horizon_core = {
        key: value for key, value in invalid_horizon_report.items() if key != "report_id"
    }
    if (
        invalid_horizon_report.get("schema_version")
        != "market-impact.method-skill-ablation-report.v2"
        or invalid_horizon_report.get("diagnostic_valid") is not True
        or not invalid_horizon_experiment_id.startswith("regime-modeled-pit-v1b.")
        or invalid_horizon_report_id
        != f"method-skill-ablation-report-{canonical_hash(invalid_horizon_core)}"
    ):
        raise ValueError("modeled-PIT invalid-horizon diagnostic report is not canonical")
    invalid_horizon_run_ids = _paired_report_run_ids(invalid_horizon_report)
    if len(invalid_horizon_run_ids) != 6:
        raise ValueError("modeled-PIT invalid-horizon diagnostic must contain six runs")
    hard_cap = _positive_integer(
        validation_registration.get("total_cost_cap_microusd"),
        "modeled-PIT total cost cap",
    )
    registered_cases = _mapping_list(validation_registration, "cases")
    registered_case_keys = tuple(_string(item, "case_key") for item in registered_cases)
    if set(completed_by_case) != set(registered_case_keys):
        raise ValueError("modeled-PIT completed cases do not match the registration")
    market_by_case = {item.case_key: item for item in dataset.cases}
    registered_by_case = {_string(item, "case_key"): item for item in registered_cases}

    case_results: list[dict[str, object]] = []
    formal_cost = 0
    formal_run_count = 0
    checkpoint_count = 0
    control_proposals = 0
    treatment_proposals = 0
    helpful = 0
    harmful = 0
    same = 0
    provider_profile_ids: set[str] = set()
    formal_run_ids: set[str] = set()
    formal_execution_binding_by_run_id: dict[str, str] = {}
    for case_key in registered_case_keys:
        completed = completed_by_case[case_key]
        registered_case = registered_by_case[case_key]
        selected_dates = tuple(
            date.fromisoformat(item) for item in _string_list(registered_case, "checkpoints")
        )
        if tuple(item.checkpoint.session_date for item in completed) != selected_dates:
            raise ValueError("modeled-PIT completed checkpoints do not match the registration")
        execution_bindings = {
            item.checkpoint.session_date: _audit_completed_execution_bindings(item)
            for item in completed
        }
        case_report = build_regime_agent_experiment_report(
            validated_panel=validated_panel,
            market_case=market_by_case[case_key],
            baseline_protocol=study_registration.baseline_protocol,
            manifest_id=manifest.manifest_id,
            qualification_report_id=_string(modeled_qualification_report, "report_id"),
            completed=completed,
            prior_invalid_diagnostic_cost_microusd=0,
            total_cost_cap_microusd=hard_cap,
        )
        if case_report.get("treatment_skill") != _string(registered_case, "treatment_skill"):
            raise ValueError("modeled-PIT treatment Skill drifted from the registration")
        provider_profile_ids.add(_string(case_report, "provider_profile_id"))
        raw_cost = _mapping(case_report, "cost")
        case_cost = _positive_integer(
            raw_cost.get("formal_model_cost_microusd"),
            "modeled-PIT case model cost",
        )
        raw_checkpoint_results = _mapping_list(case_report, "checkpoint_results")
        checkpoint_results: list[dict[str, object]] = []
        for checkpoint, completed_checkpoint in zip(raw_checkpoint_results, completed, strict=True):
            arms = _mapping_list(checkpoint, "arms")
            checkpoint_bindings = execution_bindings[completed_checkpoint.checkpoint.session_date]
            for paired_arm in _mapping_list(completed_checkpoint.report, "arms"):
                arm_id = _string(paired_arm, "arm_id")
                binding_hash = checkpoint_bindings[arm_id]
                for paired_run in _mapping_list(paired_arm, "runs"):
                    run_id = _string(paired_run, "run_id")
                    if run_id in formal_execution_binding_by_run_id:
                        raise ValueError("modeled-PIT formal run identity is duplicated")
                    formal_execution_binding_by_run_id[run_id] = binding_hash
            formal_run_ids.update(_paired_report_run_ids(completed_checkpoint.report))
            control_proposals += int(arms[0].get("majority_decision") == "propose")
            treatment_proposals += int(arms[1].get("majority_decision") == "propose")
            checkpoint_results.append(
                {
                    **{key: value for key, value in checkpoint.items() if key != "report_id"},
                    "paired_report_id": _string(checkpoint, "report_id"),
                    "execution_bindings": [
                        {"arm_id": arm_id, "binding_hash": binding_hash}
                        for arm_id, binding_hash in checkpoint_bindings.items()
                    ],
                }
            )
        skill_increment = _mapping(case_report, "skill_increment")
        helpful += _nonnegative_integer(
            skill_increment.get("helpful_checkpoint_count"),
            "modeled-PIT helpful checkpoint count",
        )
        harmful += _nonnegative_integer(
            skill_increment.get("harmful_checkpoint_count"),
            "modeled-PIT harmful checkpoint count",
        )
        same += _nonnegative_integer(
            skill_increment.get("same_decision_checkpoint_count"),
            "modeled-PIT same-decision checkpoint count",
        )
        case_run_count = _positive_integer(
            case_report.get("formal_run_count"),
            "modeled-PIT case formal run count",
        )
        case_checkpoint_count = _positive_integer(
            case_report.get("checkpoint_count"),
            "modeled-PIT case checkpoint count",
        )
        formal_cost += case_cost
        formal_run_count += case_run_count
        checkpoint_count += case_checkpoint_count
        case_results.append(
            {
                "case_key": case_key,
                "treatment_skill": case_report["treatment_skill"],
                "evidence_pack_ids": case_report["evidence_pack_ids"],
                "checkpoint_count": case_checkpoint_count,
                "formal_run_count": case_run_count,
                "checkpoint_results": checkpoint_results,
                "arms": case_report["arms"],
                "baselines": case_report["baselines"],
                "market_context": case_report["market_context"],
                "skill_increment": skill_increment,
                "formal_model_cost_microusd": case_cost,
            }
        )
    expected_profile = _string(validation_registration, "provider_profile_id")
    if provider_profile_ids != {expected_profile}:
        raise ValueError("modeled-PIT provider profile drifted across checkpoint reports")
    expected_checkpoint_count = sum(
        len(_string_list(item, "checkpoints")) for item in registered_cases
    )
    if checkpoint_count != expected_checkpoint_count or formal_run_count != checkpoint_count * 6:
        raise ValueError("modeled-PIT checkpoint or run coverage is incomplete")
    if len(formal_run_ids) != formal_run_count:
        raise ValueError("modeled-PIT formal run identities are incomplete or duplicated")
    if helpful + harmful + same != checkpoint_count:
        raise ValueError("modeled-PIT Skill-effect counts do not cover every checkpoint")
    if formal_run_ids & invalid_horizon_run_ids:
        raise ValueError("modeled-PIT formal and invalid-horizon run identities overlap")
    record_by_run_id = usage_union.record_by_run_id
    required_run_ids = formal_run_ids | invalid_horizon_run_ids
    if not required_run_ids <= set(record_by_run_id):
        raise ValueError("modeled-PIT usage union is missing a required run")
    if any(
        record_by_run_id[run_id].execution_binding_hash != binding_hash
        for run_id, binding_hash in formal_execution_binding_by_run_id.items()
    ):
        raise ValueError("modeled-PIT Usage Ledger Union execution binding drifted")
    union_formal_run_ids = {
        run_id
        for run_id, record in record_by_run_id.items()
        if record.experiment_id.startswith("regime-modeled-pit-v1c.")
    }
    if union_formal_run_ids != formal_run_ids:
        raise ValueError("modeled-PIT usage union has unreported formal runs")
    union_formal_cost = sum(
        record_by_run_id[run_id].metrics.estimated_cost_microusd for run_id in formal_run_ids
    )
    if union_formal_cost != formal_cost:
        raise ValueError("modeled-PIT formal report cost does not match the usage union")
    invalid_horizon = sum(
        record_by_run_id[run_id].metrics.estimated_cost_microusd
        for run_id in invalid_horizon_run_ids
    )
    invalid_report_cost = _positive_integer(
        _mapping(invalid_horizon_report, "cost").get("ledger_actual_microusd"),
        "invalid-horizon report cost",
    )
    if invalid_horizon != invalid_report_cost:
        raise ValueError("modeled-PIT invalid-horizon cost does not match the usage union")
    all_actual_cost = usage_union.total_estimated_cost_microusd
    preexisting = all_actual_cost - invalid_horizon - formal_cost
    if preexisting < 0:
        raise ValueError("modeled-PIT usage union cost cannot reconcile")
    if all_actual_cost > hard_cap:
        raise ValueError("modeled-PIT validation exceeds the shared model budget")
    core: dict[str, object] = {
        "schema_version": REGIME_MODELED_PIT_AGENT_VALIDATION_REPORT_SCHEMA,
        "validation_id": validation_id,
        "dataset_id": dataset.dataset_id,
        "panel_id": validated_panel.panel_id,
        "manifest_id": manifest.manifest_id,
        "strict_qualification_report_id": bindings["strict_qualification_report_id"],
        "modeled_qualification_report_id": bindings["modeled_qualification_report_id"],
        "policy_id": policy.policy_id,
        "provider_profile_id": expected_profile,
        "case_count": len(case_results),
        "checkpoint_count": checkpoint_count,
        "completed_checkpoint_count": checkpoint_count,
        "formal_run_count": formal_run_count,
        "case_results": case_results,
        "decision_summary": {
            "control_propose_checkpoint_count": control_proposals,
            "treatment_propose_checkpoint_count": treatment_proposals,
            "helpful_checkpoint_count": helpful,
            "harmful_checkpoint_count": harmful,
            "same_decision_checkpoint_count": same,
        },
        "cost": {
            "usage_ledger_count": usage_union.ledger_count,
            "usage_unique_run_count": usage_union.unique_run_count,
            "usage_duplicate_record_count": usage_union.duplicate_record_count,
            "usage_completed_run_count": usage_union.completed_run_count,
            "usage_failed_run_count": usage_union.failed_run_count,
            "usage_union_hash": usage_union.union_hash,
            "invalid_horizon_experiment_id": invalid_horizon_experiment_id,
            "preexisting_actual_diagnostic_cost_microusd": preexisting,
            "invalid_horizon_diagnostic_cost_microusd": invalid_horizon,
            "formal_modeled_pit_cost_microusd": formal_cost,
            "all_actual_model_cost_microusd": all_actual_cost,
            "hard_cap_microusd": hard_cap,
            "within_budget": True,
        },
        "limitations": [
            "all cases and outcomes were opened before this modeled-PIT diagnostic",
            "historical authority gaps remain and strict PIT qualification is still separate",
            "frozen safety delays are conservative assumptions, not prospective calibration",
            "price indices are research proxies without registered executable ETF mappings",
            "this report cannot establish Agent effectiveness, alpha, paper, or live readiness",
        ],
        "claim_scope": "opened_modeled_pit_process_diagnostic_only",
        "strict_pit_eligible": False,
        "agent_effectiveness_claim_eligible": False,
        "inference_eligible": False,
        "broker_reachability": False,
        "execution_capability": "none",
    }
    return {
        **core,
        "report_id": f"regime-modeled-pit-agent-validation-report-{canonical_hash(core)}",
    }


def _audit_completed_execution_bindings(
    item: CompletedRegimeCheckpointExperiment,
) -> dict[str, str]:
    paths = item.execution_audit_paths
    if not isinstance(paths, PairedExecutionAuditPaths):
        raise ValueError("modeled-PIT completed checkpoint lacks execution audit paths")
    from market_impact_agent.paired_skill_execution_audit import audit_paired_execution_state

    return audit_paired_execution_state(
        expected_evidence_pack=item.evidence_pack,
        eligible_horizon_sessions=item.eligible_horizon_sessions,
        registration=item.registration,
        report=item.report,
        experiment_root=paths.experiment_root,
        evidence_pack_path=paths.evidence_pack_path,
        evidence_documents_path=paths.evidence_documents_path,
        pattern_pack_paths=paths.pattern_pack_paths,
        provider_profile_path=paths.provider_profile_path,
        skill_root=paths.skill_root,
    )


def write_regime_modeled_pit_agent_validation_report(
    report: Mapping[str, object],
    *,
    root: Path = Path(".market-impact") / "regime" / "modeled-pit-agent-experiments" / "reports",
) -> Path:
    errors = validate_agent_contract(
        dict(report), "regime-modeled-pit-agent-validation-report.schema.json"
    )
    if errors:
        raise ValueError("; ".join(errors))
    report_id = _string(report, "report_id")
    core = {key: value for key, value in report.items() if key != "report_id"}
    expected = f"regime-modeled-pit-agent-validation-report-{canonical_hash(core)}"
    if report_id != expected:
        raise ValueError("modeled-PIT Agent validation report_id does not match content")
    if (
        report.get("strict_pit_eligible") is not False
        or report.get("agent_effectiveness_claim_eligible") is not False
        or report.get("inference_eligible") is not False
        or report.get("execution_capability") != "none"
    ):
        raise ValueError("modeled-PIT Agent validation report exceeds diagnostic authority")
    destination = root / f"{report_id}.json"
    _write_private_json(destination, dict(report))
    return destination


def load_regime_modeled_pit_agent_validation_report(path: Path) -> dict[str, object]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise TypeError("modeled-PIT Agent validation report must be an object")
    report = cast(dict[str, object], raw)
    errors = validate_agent_contract(
        report, "regime-modeled-pit-agent-validation-report.schema.json"
    )
    if errors:
        raise ValueError("; ".join(errors))
    report_id = _string(report, "report_id")
    core = {key: value for key, value in report.items() if key != "report_id"}
    expected = f"regime-modeled-pit-agent-validation-report-{canonical_hash(core)}"
    if report_id != expected:
        raise ValueError("modeled-PIT Agent validation report_id does not match content")
    if (
        report.get("strict_pit_eligible") is not False
        or report.get("agent_effectiveness_claim_eligible") is not False
        or report.get("inference_eligible") is not False
        or report.get("execution_capability") != "none"
    ):
        raise ValueError("modeled-PIT Agent validation report exceeds diagnostic authority")
    return report


def materialize_regime_modeled_checkpoint_bundle(
    *,
    validated_panel: ValidatedRegimePanel,
    market_case: MarketRegimeCase,
    study_case: RegimeStudyCase,
    registration: RegimeStudyRegistration,
    manifest: RegimeEvidenceManifest,
    qualification_report: Mapping[str, object],
    policy: RegimeModeledPitPolicy,
    checkpoint: RegimeCheckpoint,
    next_checkpoint_date: date | None,
    treatment_method: MethodSkill,
    pattern_pack: PatternPack,
    pattern_pack_path: Path,
    official_documents_by_hash: Mapping[str, Mapping[str, object]],
    news_documents_by_hash: Mapping[str, Mapping[str, object]],
    positioning_documents_by_hash: Mapping[str, Mapping[str, object]],
    output_root: Path,
) -> RegimeCheckpointBundle:
    if market_case.case_key != study_case.case_key or checkpoint.case_key != market_case.case_key:
        raise ValueError("modeled-PIT bundle case identities do not match")
    if manifest.registration_id != registration.registration_id:
        raise ValueError("modeled-PIT bundle manifest does not bind the registration")
    if treatment_method.skill_name not in study_case.candidate_method_skills:
        raise ValueError("modeled-PIT treatment method is not registered for the case")
    qualified = assert_modeled_checkpoint_qualified(
        qualification_report,
        case_key=market_case.case_key,
        session_date=checkpoint.session_date,
        manifest_id=manifest.manifest_id,
        policy_id=policy.policy_id,
    )
    if qualified.get("cutoff_at") != _timestamp(checkpoint.cutoff_at):
        raise ValueError("modeled-PIT checkpoint cutoff does not match qualification")
    primary = _series_by_id(validated_panel).get(market_case.primary_market_index)
    if primary is None:
        raise ValueError("modeled-PIT checkpoint primary market series is unavailable")
    horizon_sessions = eligible_horizon_sessions(
        primary,
        checkpoint.session_date,
        next_checkpoint_date=next_checkpoint_date,
        case_end=market_case.end,
    )
    visible = modeled_visible_records(
        manifest.records,
        market_case=market_case,
        study_case=study_case,
        registration=registration,
        checkpoint=checkpoint,
        policy=policy,
    )
    qualified_record_ids = {
        record_id
        for requirement in _mapping_list(qualified, "requirements")
        for record_id in _string_list(requirement, "record_ids")
    }
    if {item.record_id for item in visible} != qualified_record_ids:
        raise ValueError("modeled-PIT bundle records drifted from qualification")
    report_id = _string(qualification_report, "report_id")
    return materialize_regime_checkpoint_bundle_from_visible_records(
        validated_panel=validated_panel,
        market_case=market_case,
        study_case=study_case,
        registration=registration,
        manifest=manifest,
        checkpoint=checkpoint,
        horizon_sessions=horizon_sessions,
        treatment_method=treatment_method,
        pattern_pack=pattern_pack,
        pattern_pack_path=pattern_pack_path,
        official_documents_by_hash=official_documents_by_hash,
        news_documents_by_hash=news_documents_by_hash,
        positioning_documents_by_hash=positioning_documents_by_hash,
        output_root=output_root,
        visible=visible,
        evidence_scope_ref=(f"modeled-pit/{manifest.manifest_id}/{policy.policy_id}/{report_id}"),
        safety_delay_seconds_by_category={
            category: rule.safety_delay_seconds
            for category, rule in policy.rule_by_category.items()
        },
        data_gaps=(
            "the price index is a non-executable research proxy with no registered ETF mapping",
            "this pack is an opened-outcome modeled-PIT reconstruction, not strict PIT evidence",
            "historical authority gaps remain bound to strict report "
            f"{qualification_report['strict_qualification_report_id']}",
            f"uncalibrated safety delays are frozen by policy {policy.policy_id}",
            "the opened development case may be recognizable despite target aliasing",
        ),
    )


def assert_modeled_checkpoint_qualified(
    qualification_report: Mapping[str, object],
    *,
    case_key: str,
    session_date: date,
    manifest_id: str,
    policy_id: str,
) -> Mapping[str, object]:
    if (
        qualification_report.get("schema_version") != REGIME_MODELED_PIT_QUALIFICATION_SCHEMA
        or qualification_report.get("manifest_id") != manifest_id
        or qualification_report.get("policy_id") != policy_id
        or qualification_report.get("strict_pit_eligible") is not False
        or qualification_report.get("inference_eligible") is not False
        or qualification_report.get("agent_effectiveness_claim_eligible") is not False
        or qualification_report.get("execution_capability") != "none"
    ):
        raise ValueError("modeled-PIT checkpoint requires the exact exploratory qualification")
    cases = _mapping_list(qualification_report, "cases")
    case = next((item for item in cases if item.get("case_key") == case_key), None)
    if case is None:
        raise ValueError("modeled-PIT case is not qualified")
    checkpoint = next(
        (
            item
            for item in _mapping_list(case, "checkpoints")
            if item.get("session_date") == session_date.isoformat()
        ),
        None,
    )
    if checkpoint is None or checkpoint.get("ready") is not True:
        raise ValueError("modeled-PIT checkpoint is not qualified")
    return checkpoint


def modeled_visible_records(
    records: tuple[RegimeEvidenceRecord, ...],
    *,
    market_case: MarketRegimeCase,
    study_case: RegimeStudyCase,
    registration: RegimeStudyRegistration,
    checkpoint: RegimeCheckpoint,
    policy: RegimeModeledPitPolicy,
) -> tuple[RegimeEvidenceRecord, ...]:
    requirement_by_category = {item.category: item for item in study_case.source_requirements}
    selected: list[RegimeEvidenceRecord] = []
    for record in records:
        if market_case.case_key not in record.case_keys:
            continue
        requirement = requirement_by_category.get(record.category)
        if requirement is None or record.source_id not in requirement.source_ids:
            continue
        rule = policy.rule_by_category[record.category]
        if record.availability_basis not in rule.allowed_availability_bases:
            continue
        if record.category in {"market_price", "industry_price"}:
            expected_suffix = f"checkpoint={checkpoint.session_date.isoformat()}"
            if record.source_ref.endswith(expected_suffix) and _visible(record, checkpoint, rule):
                selected.append(record)
            continue
        if _inside_window(record, checkpoint, study_case, registration, rule):
            selected.append(record)
    by_lineage: dict[tuple[str, str], RegimeEvidenceRecord] = {}
    for record in sorted(selected, key=lambda item: (item.available_at, item.record_id)):
        by_lineage[(record.category, record.lineage_id)] = record
    return tuple(sorted(by_lineage.values(), key=lambda item: (item.category, item.record_id)))


def _qualify_requirement(
    requirement: RegimeSourceRequirement,
    *,
    checkpoint: RegimeCheckpoint,
    market_case: MarketRegimeCase,
    study_case: RegimeStudyCase,
    registration: RegimeStudyRegistration,
    validated_panel: ValidatedRegimePanel,
    source_by_id: Mapping[str, RegimeStudySource],
    records: tuple[RegimeEvidenceRecord, ...],
    rule: RegimeModeledPitCategoryRule,
    strict_result: Mapping[str, object],
) -> dict[str, object]:
    series_by_id = _series_by_id(validated_panel)
    if requirement.category == "market_price":
        series = series_by_id.get(market_case.primary_market_index)
        record_count = (
            0
            if series is None
            else sum(_row_date(row) < checkpoint.session_date for row in series.rows)
        )
        distinct = int(series is not None and record_count > 0)
        content_complete = (
            record_count >= requirement.minimum_records_per_checkpoint
            and distinct >= requirement.minimum_distinct_sources
        )
        matched = _price_records(
            requirement,
            checkpoint=checkpoint,
            market_case=market_case,
            source_by_id=source_by_id,
            records=records,
            series=() if series is None else (series,),
            rule=rule,
        )
        modeled_visibility = bool(series is not None and matched)
        return _requirement_result(
            requirement,
            record_count=record_count,
            modeled_record_count=record_count if modeled_visibility else 0,
            distinct_source_count=distinct,
            content_complete=content_complete,
            modeled_visibility=modeled_visibility,
            record_ids=tuple(item.record_id for item in matched),
            strict_result=strict_result,
        )
    if requirement.category == "industry_price":
        series = tuple(
            series_by_id[proxy_id]
            for proxy_id in market_case.required_industry_proxies
            if proxy_id in series_by_id
            and any(_row_date(row) < checkpoint.session_date for row in series_by_id[proxy_id].rows)
        )
        record_count = len(series)
        distinct = int(record_count > 0)
        content_complete = (
            record_count >= requirement.minimum_records_per_checkpoint
            and distinct >= requirement.minimum_distinct_sources
        )
        matched = _price_records(
            requirement,
            checkpoint=checkpoint,
            market_case=market_case,
            source_by_id=source_by_id,
            records=records,
            series=series,
            rule=rule,
        )
        modeled_visibility = record_count > 0 and len(matched) == record_count
        return _requirement_result(
            requirement,
            record_count=record_count,
            modeled_record_count=len(matched),
            distinct_source_count=distinct,
            content_complete=content_complete,
            modeled_visibility=modeled_visibility,
            record_ids=tuple(item.record_id for item in matched),
            strict_result=strict_result,
        )
    registered_ids = set(requirement.source_ids)
    base = _latest_versions(
        tuple(
            item
            for item in records
            if market_case.case_key in item.case_keys
            and item.category == requirement.category
            and item.source_id in registered_ids
            and item.provider_id == source_by_id[item.source_id].provider_id
            and item.availability_basis in rule.allowed_availability_bases
            and _inside_window(
                item,
                checkpoint,
                study_case,
                registration,
                RegimeModeledPitCategoryRule(
                    category=rule.category,
                    visibility_basis=rule.visibility_basis,
                    allowed_availability_bases=rule.allowed_availability_bases,
                    safety_delay_seconds=0,
                ),
            )
        )
    )
    modeled = _latest_versions(
        tuple(
            item
            for item in records
            if market_case.case_key in item.case_keys
            and item.category == requirement.category
            and item.source_id in registered_ids
            and item.provider_id == source_by_id[item.source_id].provider_id
            and item.availability_basis in rule.allowed_availability_bases
            and _inside_window(item, checkpoint, study_case, registration, rule)
        )
    )
    content_distinct = _distinct_sources(base, requirement.category)
    modeled_distinct = _distinct_sources(modeled, requirement.category)
    content_complete = (
        len(base) >= requirement.minimum_records_per_checkpoint
        and content_distinct >= requirement.minimum_distinct_sources
    )
    modeled_visibility = (
        len(modeled) >= requirement.minimum_records_per_checkpoint
        and modeled_distinct >= requirement.minimum_distinct_sources
    )
    return _requirement_result(
        requirement,
        record_count=len(base),
        modeled_record_count=len(modeled),
        distinct_source_count=modeled_distinct,
        content_complete=content_complete,
        modeled_visibility=modeled_visibility,
        record_ids=tuple(item.record_id for item in modeled),
        strict_result=strict_result,
    )


def _qualify_event_revelation(
    *,
    checkpoint: RegimeCheckpoint,
    market_case: MarketRegimeCase,
    study_case: RegimeStudyCase,
    source_by_id: Mapping[str, RegimeStudySource],
    records: tuple[RegimeEvidenceRecord, ...],
    rules: Mapping[str, RegimeModeledPitCategoryRule],
    strict_result: Mapping[str, object],
) -> dict[str, object]:
    anchor = market_case.event_anchor
    required = bool(
        anchor is not None
        and study_case.decision_schedule == "event_then_weekly"
        and checkpoint.session_date == market_case.tradable_start
    )
    if not required:
        return {
            "required": False,
            "modeled_ready": True,
            "strict_ready": True,
            "record_ids": [],
            "blockers": [],
        }
    if anchor is None:  # pragma: no cover - narrowed by required
        raise AssertionError("modeled event revelation requires an event anchor")
    registered: dict[str, str] = {}
    for requirement in study_case.source_requirements:
        if requirement.category not in {"official_context", "established_news"}:
            continue
        for source_id in requirement.source_ids:
            registered[source_id] = source_by_id[source_id].provider_id
    candidates = _latest_versions(
        tuple(
            record
            for record in records
            if market_case.case_key in record.case_keys
            and record.category in {"official_context", "established_news"}
            and registered.get(record.source_id) == record.provider_id
            and record.availability_basis in rules[record.category].allowed_availability_bases
            and (record.occurred_at or record.published_at) >= anchor.observed_at
            and record.published_at < checkpoint.cutoff_at
            and _visible(record, checkpoint, rules[record.category])
        )
    )
    record_ids = tuple(item.record_id for item in candidates)
    ready = bool(record_ids)
    return {
        "required": True,
        "modeled_ready": ready,
        "strict_ready": strict_result.get("ready") is True,
        "record_ids": list(record_ids),
        "blockers": [] if ready else ["missing_modeled_event_revelation"],
    }


def _price_records(
    requirement: RegimeSourceRequirement,
    *,
    checkpoint: RegimeCheckpoint,
    market_case: MarketRegimeCase,
    source_by_id: Mapping[str, RegimeStudySource],
    records: tuple[RegimeEvidenceRecord, ...],
    series: tuple[RegimeSeries, ...],
    rule: RegimeModeledPitCategoryRule,
) -> tuple[RegimeEvidenceRecord, ...]:
    registered_ids = set(requirement.source_ids)
    matched: list[RegimeEvidenceRecord] = []
    for item in series:
        expected_ref = panel_authority_source_ref(
            source=item.source,
            tushare_code=item.tushare_code,
            case_key=market_case.case_key,
            checkpoint_date=checkpoint.session_date,
        )
        expected_hash = panel_series_as_of_hash(item, checkpoint.session_date)
        candidates = tuple(
            record
            for record in records
            if market_case.case_key in record.case_keys
            and record.category == requirement.category
            and record.source_id in registered_ids
            and record.provider_id == source_by_id[record.source_id].provider_id
            and record.source_ref == expected_ref
            and record.content_hash == expected_hash
            and record.availability_basis in rule.allowed_availability_bases
            and _visible(record, checkpoint, rule)
        )
        if len(candidates) == 1:
            matched.append(candidates[0])
    return tuple(matched)


def _requirement_result(
    requirement: RegimeSourceRequirement,
    *,
    record_count: int,
    modeled_record_count: int,
    distinct_source_count: int,
    content_complete: bool,
    modeled_visibility: bool,
    record_ids: tuple[str, ...],
    strict_result: Mapping[str, object],
) -> dict[str, object]:
    blockers: list[str] = []
    if record_count < requirement.minimum_records_per_checkpoint:
        blockers.append("insufficient_records")
    if distinct_source_count < requirement.minimum_distinct_sources:
        blockers.append("insufficient_distinct_sources")
    if not modeled_visibility:
        blockers.append("no_modeled_visibility")
    authority = strict_result.get("point_in_time_authority") is True
    authority_count = strict_result.get("authority_record_count")
    if not isinstance(authority_count, int) or isinstance(authority_count, bool):
        raise TypeError("strict qualification authority_record_count must be an integer")
    return {
        "category": requirement.category,
        "record_count": record_count,
        "modeled_record_count": modeled_record_count,
        "authority_record_count": authority_count,
        "distinct_source_count": distinct_source_count,
        "minimum_records": requirement.minimum_records_per_checkpoint,
        "minimum_distinct_sources": requirement.minimum_distinct_sources,
        "content_complete": content_complete,
        "modeled_visibility": modeled_visibility,
        "point_in_time_authority": authority,
        "authority_gap": not authority,
        "ready": content_complete and modeled_visibility,
        "record_ids": list(record_ids),
        "blockers": blockers,
    }


def _inside_window(
    record: RegimeEvidenceRecord,
    checkpoint: RegimeCheckpoint,
    study_case: RegimeStudyCase,
    registration: RegimeStudyRegistration,
    rule: RegimeModeledPitCategoryRule,
) -> bool:
    if record.published_at >= checkpoint.cutoff_at or not _visible(record, checkpoint, rule):
        return False
    protocol = registration.checkpoint_protocol
    if record.category == "established_news":
        lookback = dict(protocol.news_lookback_calendar_days)[study_case.decision_schedule]
    else:
        lookback = dict(protocol.maximum_age_calendar_days).get(record.category)
    return lookback is None or record.available_at >= checkpoint.cutoff_at - timedelta(
        days=lookback
    )


def _visible(
    record: RegimeEvidenceRecord,
    checkpoint: RegimeCheckpoint,
    rule: RegimeModeledPitCategoryRule,
) -> bool:
    effective_available = record.available_at + timedelta(seconds=rule.safety_delay_seconds)
    if effective_available > checkpoint.cutoff_at:
        return False
    if record.source_updated_at is not None:
        effective_update = record.source_updated_at + timedelta(seconds=rule.safety_delay_seconds)
        if effective_update > checkpoint.cutoff_at:
            return False
    return True


def _validate_bindings(
    dataset: MarketRegimeDataset,
    panel: ValidatedRegimePanel,
    registration: RegimeStudyRegistration,
    manifest: RegimeEvidenceManifest,
    strict_report: Mapping[str, object],
) -> None:
    report_id = _string(strict_report, "report_id")
    core = {key: value for key, value in strict_report.items() if key != "report_id"}
    if report_id != f"regime-evidence-qualification-report-{canonical_hash(core)}":
        raise ValueError("strict qualification report_id does not match content")
    if (
        manifest.dataset_id != dataset.dataset_id
        or manifest.dataset_hash != dataset.dataset_hash
        or manifest.registration_id != registration.registration_id
        or manifest.registration_hash != registration.registration_hash
        or manifest.panel_id != panel.panel_id
        or manifest.panel_hash != panel.panel_hash
        or strict_report.get("dataset_id") != dataset.dataset_id
        or strict_report.get("registration_id") != registration.registration_id
        or strict_report.get("panel_id") != panel.panel_id
        or strict_report.get("manifest_id") != manifest.manifest_id
    ):
        raise ValueError("modeled-PIT inputs do not share one frozen study")


def _rule_from_dict(payload: Mapping[str, object]) -> RegimeModeledPitCategoryRule:
    raw_bases = payload.get("allowed_availability_bases")
    if not isinstance(raw_bases, list):
        raise TypeError("modeled-PIT allowed availability bases must be strings")
    base_items = cast(list[object], raw_bases)
    if not all(isinstance(item, str) for item in base_items):
        raise TypeError("modeled-PIT allowed availability bases must be strings")
    delay = payload.get("safety_delay_seconds")
    if not isinstance(delay, int) or isinstance(delay, bool):
        raise TypeError("modeled-PIT safety delay must be an integer")
    return RegimeModeledPitCategoryRule(
        category=_string(payload, "category"),
        visibility_basis=_string(payload, "visibility_basis"),
        allowed_availability_bases=tuple(
            RegimeEvidenceAvailabilityBasis(cast(str, item)) for item in base_items
        ),
        safety_delay_seconds=delay,
    )


def _series_by_id(panel: ValidatedRegimePanel) -> dict[str, RegimeSeries]:
    result = {item.series_id: item for item in panel.panel.series}
    by_code = {item.tushare_code: item for item in panel.panel.series}
    for proxy_id, code in panel.panel.proxy_resolution:
        if code in by_code:
            result[proxy_id] = by_code[code]
    return result


def _latest_versions(
    records: tuple[RegimeEvidenceRecord, ...],
) -> tuple[RegimeEvidenceRecord, ...]:
    by_lineage: dict[str, RegimeEvidenceRecord] = {}
    for item in sorted(records, key=lambda value: (value.available_at, value.record_id)):
        by_lineage[item.lineage_id] = item
    return tuple(sorted(by_lineage.values(), key=lambda item: item.record_id))


def _distinct_sources(records: tuple[RegimeEvidenceRecord, ...], category: str) -> int:
    if category == "established_news":
        return len({item.publisher_id for item in records})
    return len({item.source_id for item in records})


def _row_date(row: Mapping[str, object]) -> date:
    value = row.get("trade_date")
    if not isinstance(value, str):
        raise TypeError("regime price row trade_date must be a string")
    return date.fromisoformat(value) if "-" in value else datetime.strptime(value, "%Y%m%d").date()


def _timestamp(value: datetime) -> str:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("modeled-PIT timestamp must be timezone-aware")
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _string(payload: Mapping[str, object], key: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value:
        raise TypeError(f"modeled-PIT {key} must be a non-empty string")
    return value


def _nonnegative_integer(value: object, label: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise TypeError(f"{label} must be a non-negative integer")
    return value


def _positive_integer(value: object, label: str) -> int:
    result = _nonnegative_integer(value, label)
    if result == 0:
        raise ValueError(f"{label} must be positive")
    return result


def _mapping(payload: Mapping[str, object], key: str) -> Mapping[str, object]:
    value = payload.get(key)
    if not isinstance(value, Mapping):
        raise TypeError(f"modeled-PIT {key} must be an object")
    return cast(Mapping[str, object], value)


def _mapping_list(payload: Mapping[str, object], key: str) -> tuple[Mapping[str, object], ...]:
    value = payload.get(key)
    if not isinstance(value, list):
        raise TypeError(f"modeled-PIT {key} must be an array of objects")
    items = cast(list[object], value)
    if not all(isinstance(item, Mapping) for item in items):
        raise TypeError(f"modeled-PIT {key} must be an array of objects")
    return tuple(cast(Mapping[str, object], item) for item in items)


def _paired_report_run_ids(report: Mapping[str, object]) -> set[str]:
    arms = _mapping_list(report, "arms")
    if len(arms) != 2:
        raise ValueError("modeled-PIT paired report must contain exactly two arms")
    run_ids = {_string(run, "run_id") for arm in arms for run in _mapping_list(arm, "runs")}
    run_count = sum(len(_mapping_list(arm, "runs")) for arm in arms)
    if len(run_ids) != run_count:
        raise ValueError("modeled-PIT paired report run identities must be unique")
    return run_ids


def _object_list(payload: Mapping[str, object], key: str) -> tuple[Mapping[str, object], ...]:
    return _mapping_list(payload, key)


def _string_list(payload: Mapping[str, object], key: str) -> tuple[str, ...]:
    value = payload.get(key)
    if not isinstance(value, list):
        raise TypeError(f"modeled-PIT {key} must be an array of strings")
    items = cast(list[object], value)
    if not all(isinstance(item, str) for item in items):
        raise TypeError(f"modeled-PIT {key} must be an array of strings")
    return tuple(cast(str, item) for item in items)


def _write_private_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    path.parent.chmod(0o700)
    if path.is_symlink():
        raise ValueError("modeled-PIT destination must not be a symlink")
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
        raise ValueError("private modeled-PIT report must use mode 0600")
