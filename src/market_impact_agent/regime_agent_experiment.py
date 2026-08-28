from __future__ import annotations

import json
import math
import os
import stat
import tempfile
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from decimal import ROUND_HALF_EVEN, Decimal
from pathlib import Path
from typing import cast

from market_impact_agent.agent_contracts import (
    EvidencePack,
    EvidenceReference,
    PatternPack,
    PatternPackReference,
    canonical_hash,
)
from market_impact_agent.agent_schema import validate_agent_contract
from market_impact_agent.market_regimes import (
    MarketRegimeCase,
    RegimeSeries,
    ValidatedRegimePanel,
)
from market_impact_agent.method_skills import (
    METHOD_EVIDENCE_DECLARATION_SCHEMA,
    MethodEvidenceBinding,
    MethodEvidenceDeclaration,
    MethodSkill,
)
from market_impact_agent.paired_skill_ablation_contract import paired_skill_common_input_hash
from market_impact_agent.regime_evidence import (
    RegimeCheckpoint,
    RegimeEvidenceManifest,
    RegimeEvidenceRecord,
    has_point_in_time_authority,
)
from market_impact_agent.regime_study import (
    RegimeBaselineProtocol,
    RegimeStudyCase,
    RegimeStudyRegistration,
    evaluate_regime_case_baselines,
)
from market_impact_agent.research import EvidenceTier

_RETURN_QUANTUM = Decimal("0.00000001")
_TEN_THOUSAND = Decimal(10_000)
_TARGET_ALIAS = "broad-market-a"
_DOCUMENT_SCHEMA = "market-impact.regime-agent-evidence-documents.v1"
_EXPERIMENT_REPORT_SCHEMA = "market-impact.regime-agent-experiment-report.v1"


@dataclass(frozen=True, slots=True)
class RegimeCheckpointBundle:
    case_key: str
    checkpoint: RegimeCheckpoint
    eligible_horizon_sessions: int
    evidence_pack_path: Path
    evidence_documents_path: Path
    method_evidence_declaration_path: Path
    pattern_pack_path: Path


@dataclass(frozen=True, slots=True)
class CompletedRegimeCheckpointExperiment:
    checkpoint: RegimeCheckpoint
    eligible_horizon_sessions: int
    evidence_pack: EvidencePack
    method_evidence_declaration: MethodEvidenceDeclaration
    registration: Mapping[str, object]
    report: Mapping[str, object]
    execution_audit_paths: PairedExecutionAuditPaths | None = None


@dataclass(frozen=True, slots=True)
class PairedExecutionAuditPaths:
    experiment_root: Path
    evidence_pack_path: Path
    evidence_documents_path: Path
    pattern_pack_paths: tuple[Path, ...]
    provider_profile_path: Path
    skill_root: Path


def build_regime_agent_experiment_report(
    *,
    validated_panel: ValidatedRegimePanel,
    market_case: MarketRegimeCase,
    baseline_protocol: RegimeBaselineProtocol,
    manifest_id: str,
    qualification_report_id: str,
    completed: tuple[CompletedRegimeCheckpointExperiment, ...],
    prior_invalid_diagnostic_cost_microusd: int,
    total_cost_cap_microusd: int,
) -> dict[str, object]:
    if not completed:
        raise ValueError("complete regime experiment requires checkpoint runs")
    if prior_invalid_diagnostic_cost_microusd < 0 or total_cost_cap_microusd <= 0:
        raise ValueError("regime experiment cost values are invalid")
    ordered = tuple(sorted(completed, key=lambda item: item.checkpoint.session_date))
    if len({item.checkpoint.session_date for item in ordered}) != len(ordered):
        raise ValueError("regime experiment checkpoints must be unique")
    if any(item.checkpoint.case_key != market_case.case_key for item in ordered):
        raise ValueError("regime experiment checkpoint belongs to another case")
    if ordered[0].checkpoint.session_date != market_case.tradable_start:
        raise ValueError("regime experiment must begin at the registered tradable start")

    series_by_id = _series_by_id(validated_panel)
    primary = series_by_id.get(market_case.primary_market_index)
    if primary is None:
        raise ValueError("regime experiment primary market series is unavailable")
    path_rows = tuple(
        sorted(
            (
                row
                for row in primary.rows
                if market_case.tradable_start <= _row_date(row) <= market_case.end
            ),
            key=_row_date,
        )
    )
    checkpoint_dates = {item.checkpoint.session_date for item in ordered}
    if not checkpoint_dates <= {_row_date(row) for row in path_rows}:
        raise ValueError("regime experiment checkpoint lacks a primary price row")

    arm_decisions: dict[str, dict[date, str]] = {}
    arm_hits: dict[str, int] = {}
    checkpoint_results: list[dict[str, object]] = []
    formal_model_cost = 0
    formal_run_count = 0
    provider_profile_ids: set[str] = set()
    treatment_differences: set[str] = set()
    evidence_pack_ids: list[str] = []
    helpful = 0
    harmful = 0
    same = 0
    for item in ordered:
        report = item.report
        registration = item.registration
        registration_hash, common_input_hash = validate_paired_experiment_identity(item)
        if (
            report.get("diagnostic_valid") is not True
            or report.get("replicate_count") != 3
            or report.get("outcomes_visible_to_agent") is not False
            or report.get("execution_capability") != "none"
        ):
            raise ValueError("regime experiment contains an invalid checkpoint report")
        provider_profile_id = registration.get("provider_profile_id")
        if not isinstance(provider_profile_id, str) or not provider_profile_id:
            raise TypeError("regime experiment provider profile identity is invalid")
        provider_profile_ids.add(provider_profile_id)
        treatment_difference = report.get("only_treatment_difference")
        if not isinstance(treatment_difference, str) or not treatment_difference:
            raise TypeError("regime experiment treatment difference is invalid")
        treatment_differences.add(treatment_difference)
        evidence_pack_ids.append(item.evidence_pack.pack_id)
        raw_cost = report.get("cost")
        if not isinstance(raw_cost, Mapping):
            raise TypeError("regime experiment cost record is invalid")
        actual_cost = cast(Mapping[str, object], raw_cost).get("ledger_actual_microusd")
        if not isinstance(actual_cost, int) or isinstance(actual_cost, bool) or actual_cost < 0:
            raise TypeError("regime experiment actual cost is invalid")
        formal_model_cost += actual_cost
        raw_arms = report.get("arms")
        if not isinstance(raw_arms, list):
            raise ValueError("regime checkpoint must contain exactly two paired arms")
        typed_arms = cast(list[object], raw_arms)
        if len(typed_arms) != 2:
            raise ValueError("regime checkpoint must contain exactly two paired arms")
        aggregates: list[dict[str, object]] = []
        for raw_arm in typed_arms:
            if not isinstance(raw_arm, Mapping):
                raise TypeError("regime checkpoint arm is invalid")
            aggregate = aggregate_checkpoint_arm(
                cast(Mapping[str, object], raw_arm),
                target_id=_TARGET_ALIAS,
                eligible_horizon_sessions=item.eligible_horizon_sessions,
            )
            if aggregate["majority_decision"] == "invalid":
                raise ValueError("regime checkpoint arm has no valid majority")
            arm_id = cast(str, aggregate["arm_id"])
            arm_decisions.setdefault(arm_id, {})[item.checkpoint.session_date] = cast(
                str, aggregate["majority_decision"]
            )
            arm_hits.setdefault(arm_id, 0)
            aggregates.append(aggregate)
            formal_run_count += cast(int, aggregate["replicate_count"])
        if aggregates[0]["arm_id"] != "general_control":
            raise ValueError("regime experiment first arm must be the general control")
        if not cast(str, aggregates[1]["arm_id"]).startswith("general_plus_"):
            raise ValueError("regime experiment second arm must add one method Skill")
        eligible_return = _checkpoint_eligible_return(
            path_rows,
            checkpoint_date=item.checkpoint.session_date,
            horizon_sessions=item.eligible_horizon_sessions,
        )
        decisions = {
            cast(str, aggregate["arm_id"]): cast(str, aggregate["majority_decision"])
            for aggregate in aggregates
        }
        for arm_id, decision in decisions.items():
            if _decision_is_correct(decision, eligible_return):
                arm_hits[arm_id] += 1
        control = decisions["general_control"]
        treatment_id = cast(str, aggregates[1]["arm_id"])
        treatment = decisions[treatment_id]
        effect = _incremental_decision_effect(control, treatment, eligible_return)
        if effect == "helpful":
            helpful += 1
        elif effect == "harmful":
            harmful += 1
        else:
            same += 1
        checkpoint_results.append(
            {
                "session_date": item.checkpoint.session_date.isoformat(),
                "cutoff_at": _timestamp(item.checkpoint.cutoff_at),
                "eligible_horizon_sessions": item.eligible_horizon_sessions,
                "eligible_open_to_close_return": _format(eligible_return),
                "evidence_pack_id": item.evidence_pack.pack_id,
                "paired_registration_id": registration.get("registration_id"),
                "paired_registration_hash": registration_hash,
                "common_input_hash": common_input_hash,
                "report_id": report.get("report_id"),
                "actual_model_cost_microusd": actual_cost,
                "arms": aggregates,
                "incremental_skill_effect": effect,
            }
        )
    if len(provider_profile_ids) != 1 or len(treatment_differences) != 1:
        raise ValueError("regime experiment provider or treatment drifted across checkpoints")
    if len(arm_decisions) != 2 or any(
        len(value) != len(ordered) for value in arm_decisions.values()
    ):
        raise ValueError("regime experiment arm coverage is incomplete")

    arms: list[dict[str, object]] = []
    for arm_id, decisions in arm_decisions.items():
        arms.append(
            {
                "arm_id": arm_id,
                "checkpoint_decisions": [
                    {"session_date": day.isoformat(), "decision": decision}
                    for day, decision in sorted(decisions.items())
                ],
                "directional_hit_count": arm_hits[arm_id],
                "directional_hit_rate": _format(Decimal(arm_hits[arm_id]) / Decimal(len(ordered))),
                "path_metrics": evaluate_checkpoint_exposure_path(
                    rows=primary.rows,
                    start=market_case.tradable_start,
                    end=market_case.end,
                    checkpoint_decisions=decisions,
                    transaction_cost_bps_one_way=(baseline_protocol.transaction_cost_bps_one_way),
                    annualization_sessions=baseline_protocol.annualization_sessions,
                    minimum_risk_sessions=baseline_protocol.minimum_risk_sessions,
                    risk_free_rate_annual=baseline_protocol.risk_free_rate_annual,
                    cvar_confidence=baseline_protocol.cvar_confidence,
                ),
            }
        )
    baseline_result = evaluate_regime_case_baselines(
        market_case,
        series_by_id,
        baseline_protocol,
    )
    if baseline_result.get("status") != "covered":
        raise ValueError("regime experiment registered baselines are not covered")
    strategy_results = cast(dict[str, object], baseline_result["strategies"])
    baselines: list[dict[str, object]] = []
    for baseline_id in baseline_protocol.strategies:
        path_metrics = cast(dict[str, object], strategy_results[baseline_id])
        if path_metrics.get("status") != "covered":
            raise ValueError(f"regime experiment baseline is not covered: {baseline_id}")
        baselines.append({"baseline_id": baseline_id, "path_metrics": path_metrics})
    all_actual_cost = formal_model_cost + prior_invalid_diagnostic_cost_microusd
    if all_actual_cost > total_cost_cap_microusd:
        raise ValueError("regime experiment total actual model cost exceeds the hard cap")
    core: dict[str, object] = {
        "schema_version": _EXPERIMENT_REPORT_SCHEMA,
        "case_key": market_case.case_key,
        "panel_id": validated_panel.panel_id,
        "manifest_id": manifest_id,
        "qualification_report_id": qualification_report_id,
        "provider_profile_id": next(iter(provider_profile_ids)),
        "treatment_skill": next(iter(treatment_differences)),
        "evidence_pack_ids": evidence_pack_ids,
        "checkpoint_count": len(ordered),
        "formal_run_count": formal_run_count,
        "checkpoint_results": checkpoint_results,
        "arms": arms,
        "baselines": baselines,
        "market_context": _market_context(
            series_by_id=series_by_id,
            market_case=market_case,
        ),
        "skill_increment": {
            "helpful_checkpoint_count": helpful,
            "harmful_checkpoint_count": harmful,
            "same_decision_checkpoint_count": same,
        },
        "cost": {
            "formal_model_cost_microusd": formal_model_cost,
            "prior_invalid_or_superseded_diagnostic_cost_microusd": (
                prior_invalid_diagnostic_cost_microusd
            ),
            "all_actual_model_cost_microusd": all_actual_cost,
            "hard_cap_microusd": total_cost_cap_microusd,
            "within_budget": True,
        },
        "limitations": [
            "opened retrospective development case; the result is not out-of-sample",
            "one case and three checkpoints do not establish general method effectiveness",
            "risk metrics remain unavailable when the registered minimum session count is unmet",
            "the broad-market index is a research proxy and is not an executable instrument",
        ],
        "inference_eligible": False,
        "broker_reachability": False,
        "execution_capability": "none",
    }
    return {
        **core,
        "report_id": f"regime-agent-experiment-report-{canonical_hash(core)}",
    }


def validate_paired_experiment_identity(
    item: CompletedRegimeCheckpointExperiment,
) -> tuple[str, str]:
    registration = dict(item.registration)
    registration_errors = validate_agent_contract(
        registration,
        "method-skill-ablation-registration.schema.json",
    )
    if registration_errors:
        raise ValueError("; ".join(registration_errors))
    registration_core = {
        key: value for key, value in registration.items() if key != "registration_id"
    }
    registration_hash = canonical_hash(registration_core)
    if registration.get("registration_id") != f"method-skill-ablation-{registration_hash}":
        raise ValueError("regime experiment registration is not content-addressed")

    report = dict(item.report)
    if report.get("schema_version") != "market-impact.method-skill-ablation-report.v2":
        raise ValueError("regime experiment paired report schema is unsupported")
    report_core = {key: value for key, value in report.items() if key != "report_id"}
    if report.get("report_id") != f"method-skill-ablation-report-{canonical_hash(report_core)}":
        raise ValueError("regime experiment paired report is not content-addressed")
    if (
        report.get("registration_id") != registration.get("registration_id")
        or report.get("registration_hash") != registration_hash
        or report.get("experiment_id") != registration.get("experiment_id")
    ):
        raise ValueError("regime experiment report does not bind its registration")
    if report.get("provider_profile_id") != registration.get("provider_profile_id") or report.get(
        "provider_profile_hash"
    ) != registration.get("provider_profile_hash"):
        raise ValueError("regime experiment report provider binding drifted")
    method_route = report.get("method_route")
    if not isinstance(method_route, Mapping):
        raise TypeError("regime experiment report method route is invalid")
    typed_method_route = cast(Mapping[str, object], method_route)
    if typed_method_route.get("route_id") != registration.get("method_route_id"):
        raise ValueError("regime experiment report method route binding drifted")
    treatment_skills = registration.get("treatment_skills")
    if not isinstance(treatment_skills, list) or not treatment_skills:
        raise TypeError("regime experiment treatment Skills are invalid")
    if report.get("only_treatment_difference") != treatment_skills[-1]:
        raise ValueError("regime experiment treatment binding drifted")

    evidence_pack_hash = canonical_hash(item.evidence_pack.to_dict())
    outcomes_opened = registration.get("outcomes_opened")
    if not isinstance(outcomes_opened, bool):
        raise TypeError("regime experiment outcome visibility is invalid")
    item.method_evidence_declaration.validate_against(
        evidence_pack_id=item.evidence_pack.pack_id,
        evidence_pack_hash=evidence_pack_hash,
        evidence_ids=frozenset(value.evidence_id for value in item.evidence_pack.evidence),
        pattern_pack_ids=frozenset(value.pack_id for value in item.evidence_pack.pattern_packs),
        outcomes_opened=outcomes_opened,
    )
    if (
        registration.get("evidence_pack_id") != item.evidence_pack.pack_id
        or registration.get("evidence_pack_hash") != evidence_pack_hash
        or registration.get("method_evidence_declaration_id")
        != item.method_evidence_declaration.declaration_id
        or registration.get("method_evidence_declaration_hash")
        != item.method_evidence_declaration.declaration_hash
    ):
        raise ValueError("regime experiment registration input bindings drifted")
    common_input_hash = paired_skill_common_input_hash(
        item.evidence_pack,
        item.method_evidence_declaration,
        eligible_horizon_sessions=item.eligible_horizon_sessions,
    )
    if registration.get("common_input_hash") != common_input_hash:
        raise ValueError("regime experiment registered horizon or common input drifted")
    return registration_hash, common_input_hash


def write_regime_agent_experiment_report(
    report: Mapping[str, object],
    *,
    root: Path = Path(".market-impact") / "regime" / "agent-experiments" / "reports",
) -> Path:
    if report.get("schema_version") != _EXPERIMENT_REPORT_SCHEMA:
        raise ValueError("unsupported regime Agent experiment report schema")
    schema_errors = validate_agent_contract(
        dict(report), "regime-agent-experiment-report.schema.json"
    )
    if schema_errors:
        raise ValueError("; ".join(schema_errors))
    report_id = report.get("report_id")
    if not isinstance(report_id, str):
        raise TypeError("regime Agent experiment report_id must be a string")
    core = {key: value for key, value in report.items() if key != "report_id"}
    expected_id = f"regime-agent-experiment-report-{canonical_hash(core)}"
    if report_id != expected_id:
        raise ValueError("regime Agent experiment report_id does not match content")
    if (
        report.get("execution_capability") != "none"
        or report.get("inference_eligible") is not False
    ):
        raise ValueError("regime Agent experiment report exceeds diagnostic authority")
    destination = root / f"{report_id}.json"
    _write_private_json(destination, dict(report))
    return destination


def materialize_regime_checkpoint_bundle(
    *,
    validated_panel: ValidatedRegimePanel,
    market_case: MarketRegimeCase,
    study_case: RegimeStudyCase,
    registration: RegimeStudyRegistration,
    manifest: RegimeEvidenceManifest,
    qualification_report: Mapping[str, object],
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
        raise ValueError("regime checkpoint bundle case identities do not match")
    if manifest.registration_id != registration.registration_id:
        raise ValueError("regime checkpoint bundle manifest does not bind the registration")
    if treatment_method.skill_name not in study_case.candidate_method_skills:
        raise ValueError("regime checkpoint treatment method is not registered for the case")
    qualified = assert_checkpoint_qualified(
        qualification_report,
        case_key=market_case.case_key,
        session_date=checkpoint.session_date,
        manifest_id=manifest.manifest_id,
    )
    if qualified.get("cutoff_at") != _timestamp(checkpoint.cutoff_at):
        raise ValueError("regime checkpoint cutoff does not match qualification")
    series_by_id = _series_by_id(validated_panel)
    primary = series_by_id.get(market_case.primary_market_index)
    if primary is None:
        raise ValueError("regime checkpoint primary market series is unavailable")
    horizon_sessions = eligible_horizon_sessions(
        primary,
        checkpoint.session_date,
        next_checkpoint_date=next_checkpoint_date,
        case_end=market_case.end,
    )
    visible = select_checkpoint_records(
        manifest.records,
        market_case=market_case,
        study_case=study_case,
        registration=registration,
        checkpoint=checkpoint,
    )
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
        evidence_scope_ref=manifest.manifest_id,
        safety_delay_seconds_by_category={},
        data_gaps=(
            "the price index is a non-executable research proxy with no registered ETF mapping",
            "historical provider rows authenticate modeled availability, not original receipt",
            "the opened development case may be recognizable despite target aliasing",
        ),
    )


def materialize_regime_checkpoint_bundle_from_visible_records(
    *,
    validated_panel: ValidatedRegimePanel,
    market_case: MarketRegimeCase,
    study_case: RegimeStudyCase,
    registration: RegimeStudyRegistration,
    manifest: RegimeEvidenceManifest,
    checkpoint: RegimeCheckpoint,
    horizon_sessions: int,
    treatment_method: MethodSkill,
    pattern_pack: PatternPack,
    pattern_pack_path: Path,
    official_documents_by_hash: Mapping[str, Mapping[str, object]],
    news_documents_by_hash: Mapping[str, Mapping[str, object]],
    positioning_documents_by_hash: Mapping[str, Mapping[str, object]],
    output_root: Path,
    visible: tuple[RegimeEvidenceRecord, ...],
    evidence_scope_ref: str,
    safety_delay_seconds_by_category: Mapping[str, int],
    data_gaps: tuple[str, ...],
) -> RegimeCheckpointBundle:
    by_category = {
        category: tuple(item for item in visible if item.category == category)
        for category in (
            "market_price",
            "industry_price",
            "official_context",
            "macro_vintage",
            "established_news",
            "positioning_or_expectations",
        )
    }
    if any(not records for records in by_category.values()):
        missing = sorted(category for category, records in by_category.items() if not records)
        raise ValueError(f"regime checkpoint bundle lacks qualified categories: {missing}")

    documents = _checkpoint_documents(
        validated_panel=validated_panel,
        market_case=market_case,
        checkpoint=checkpoint,
        records_by_category=by_category,
        official_documents_by_hash=official_documents_by_hash,
        news_documents_by_hash=news_documents_by_hash,
        positioning_documents_by_hash=positioning_documents_by_hash,
    )
    pattern_reference = PatternPackReference(
        pack_id=pattern_pack.pack_id,
        version=pattern_pack.version,
        available_at=pattern_pack.available_at,
        content_hash=canonical_hash(pattern_pack.to_dict()),
    )
    reference_specs = (
        (
            "market-context",
            "market-context-through-previous-session",
            EvidenceTier.REGULATED,
            "Prior-session broad-market price and trend context.",
            "market_price",
        ),
        (
            "industry-rotation",
            "industry-breadth-through-previous-session",
            EvidenceTier.REGULATED,
            "Prior-session industry breadth, dispersion, and leadership context.",
            "industry_price",
        ),
        (
            "official-policy-context",
            "official-context-visible-before-cutoff",
            EvidenceTier.OFFICIAL,
            "Official policy versions visible before the checkpoint.",
            "official_context",
        ),
        (
            "macro-vintage",
            "macro-vintages-visible-before-cutoff",
            EvidenceTier.OFFICIAL,
            "Macro releases and vintages visible before the checkpoint.",
            "macro_vintage",
        ),
        (
            "timestamped-news-corpus",
            "established-news-visible-before-cutoff",
            EvidenceTier.ESTABLISHED_NEWS,
            "Timestamped multi-publisher narrative corpus visible before the checkpoint.",
            "established_news",
        ),
        (
            "positioning-flow",
            "exchange-positioning-visible-before-cutoff",
            EvidenceTier.REGULATED,
            "Exchange financing-flow observations visible before the checkpoint.",
            "positioning_or_expectations",
        ),
    )
    references: list[EvidenceReference] = []
    for evidence_id, claim_id, tier, summary, category in reference_specs:
        records = by_category[category]
        document = documents[evidence_id]
        references.append(
            EvidenceReference(
                evidence_id=evidence_id,
                claim_id=claim_id,
                source_ref=(
                    f"regime-manifest://{evidence_scope_ref}/{market_case.case_key}/"
                    f"{checkpoint.session_date.isoformat()}/{category}"
                ),
                source_tier=tier,
                available_at=max(
                    item.available_at
                    + timedelta(seconds=safety_delay_seconds_by_category.get(category, 0))
                    for item in records
                ),
                content_hash=canonical_hash(document),
                summary=summary,
                untrusted_text=True,
            )
        )
    event_id = (
        "masked-regime-development-"
        + canonical_hash(
            {
                "manifest_id": manifest.manifest_id,
                "evidence_scope_ref": evidence_scope_ref,
                "case_key": market_case.case_key,
                "checkpoint": checkpoint.session_date.isoformat(),
            }
        )[:20]
    )
    pack = EvidencePack.build(
        event_id=event_id,
        as_of=checkpoint.cutoff_at,
        research_question=(
            "Using only information available at the registered cutoff, should the eligible "
            f"broad-market proxy be proposed up for {horizon_sessions} trading session"
            f"{'s' if horizon_sessions != 1 else ''}, or should the analysis abstain?"
        ),
        evidence=tuple(references),
        pattern_packs=(pattern_reference,),
        allowed_targets=(_TARGET_ALIAS,),
        data_gaps=data_gaps,
    )
    pack_hash = canonical_hash(pack.to_dict())
    source_by_id = {item.source_id: item for item in registration.source_catalog}
    evidence_refs_by_type: dict[str, list[str]] = {}
    for evidence_id, _claim_id, _tier, _summary, category in reference_specs:
        evidence_types = {
            evidence_type
            for record in by_category[category]
            for evidence_type in source_by_id[record.source_id].evidence_types
        }
        for evidence_type in evidence_types:
            evidence_refs_by_type.setdefault(evidence_type, []).append(evidence_id)
    bindings = method_evidence_bindings(
        required_evidence=treatment_method.required_evidence,
        evidence_refs_by_type={key: tuple(value) for key, value in evidence_refs_by_type.items()},
    )
    declaration_core: dict[str, object] = {
        "schema_version": METHOD_EVIDENCE_DECLARATION_SCHEMA,
        "evidence_pack_id": pack.pack_id,
        "evidence_pack_hash": pack_hash,
        "evidence_types": [binding.to_dict() for binding in bindings],
        "outcomes_opened": True,
    }
    declaration = MethodEvidenceDeclaration(
        declaration_id=f"method-evidence-{canonical_hash(declaration_core)}",
        evidence_pack_id=pack.pack_id,
        evidence_pack_hash=pack_hash,
        bindings=bindings,
        outcomes_opened=True,
    )
    destination = output_root / market_case.case_key / checkpoint.session_date.isoformat()
    evidence_pack_path = destination / "evidence-pack.json"
    evidence_documents_path = destination / "evidence-documents.json"
    declaration_path = destination / "method-evidence-declaration.json"
    _write_private_json(evidence_pack_path, pack.to_dict())
    _write_private_json(
        evidence_documents_path,
        {"schema_version": _DOCUMENT_SCHEMA, "documents": documents},
    )
    _write_private_json(declaration_path, declaration.to_dict())
    return RegimeCheckpointBundle(
        case_key=market_case.case_key,
        checkpoint=checkpoint,
        eligible_horizon_sessions=horizon_sessions,
        evidence_pack_path=evidence_pack_path,
        evidence_documents_path=evidence_documents_path,
        method_evidence_declaration_path=declaration_path,
        pattern_pack_path=pattern_pack_path,
    )


def method_evidence_bindings(
    *,
    required_evidence: tuple[str, ...],
    evidence_refs_by_type: Mapping[str, tuple[str, ...]],
) -> tuple[MethodEvidenceBinding, ...]:
    bindings: list[MethodEvidenceBinding] = []
    for evidence_type in required_evidence:
        evidence_refs = evidence_refs_by_type.get(evidence_type, ())
        if not evidence_refs:
            raise ValueError(f"regime checkpoint lacks treatment evidence type: {evidence_type}")
        bindings.append(
            MethodEvidenceBinding(
                evidence_type=evidence_type,
                evidence_refs=evidence_refs,
                pattern_pack_refs=(),
            )
        )
    return tuple(bindings)


def assert_checkpoint_qualified(
    qualification_report: Mapping[str, object],
    *,
    case_key: str,
    session_date: date,
    manifest_id: str,
) -> dict[str, object]:
    if (
        qualification_report.get("schema_version")
        != "market-impact.regime-evidence-qualification-report.v1"
    ):
        raise ValueError("regime experiment requires a strict PIT qualification report")
    if qualification_report.get("manifest_id") != manifest_id:
        raise ValueError("regime experiment qualification does not bind the evidence manifest")
    raw_cases = qualification_report.get("cases")
    if not isinstance(raw_cases, list):
        raise TypeError("regime qualification cases must be a list")
    case = next(
        (
            cast(dict[str, object], item)
            for item in cast(list[object], raw_cases)
            if isinstance(item, dict) and cast(dict[str, object], item).get("case_key") == case_key
        ),
        None,
    )
    if case is None:
        raise ValueError("regime experiment case is not qualified")
    raw_checkpoints = case.get("checkpoints")
    if not isinstance(raw_checkpoints, list):
        raise TypeError("regime qualification checkpoints must be a list")
    checkpoint = next(
        (
            cast(dict[str, object], item)
            for item in cast(list[object], raw_checkpoints)
            if isinstance(item, dict)
            and cast(dict[str, object], item).get("session_date") == session_date.isoformat()
        ),
        None,
    )
    if checkpoint is None or checkpoint.get("ready") is not True:
        raise ValueError("regime experiment checkpoint is not qualified")
    return checkpoint


def aggregate_checkpoint_arm(
    arm_report: Mapping[str, object],
    *,
    target_id: str,
    eligible_horizon_sessions: int,
) -> dict[str, object]:
    if eligible_horizon_sessions < 1:
        raise ValueError("eligible checkpoint horizon must be positive")
    raw_runs = arm_report.get("runs")
    raw_coverage = arm_report.get("coverage")
    if not isinstance(raw_runs, list):
        raise ValueError("regime checkpoint arm requires exactly three paired runs")
    typed_runs = cast(list[object], raw_runs)
    if len(typed_runs) != 3:
        raise ValueError("regime checkpoint arm requires exactly three paired runs")
    if not isinstance(raw_coverage, list):
        raise ValueError("regime checkpoint arm requires exactly three coverage records")
    typed_coverage = cast(list[object], raw_coverage)
    if len(typed_coverage) != 3:
        raise ValueError("regime checkpoint arm requires exactly three coverage records")
    coverage_by_run: dict[str, dict[str, object]] = {}
    for raw in typed_coverage:
        if not isinstance(raw, dict):
            raise TypeError("regime checkpoint coverage record is invalid")
        coverage_record = cast(dict[str, object], raw)
        if not isinstance(coverage_record.get("run_id"), str):
            raise TypeError("regime checkpoint coverage record is invalid")
        coverage_by_run[cast(str, coverage_record["run_id"])] = coverage_record

    normalized: list[str] = []
    invalid_reasons: list[str] = []
    for raw in typed_runs:
        if not isinstance(raw, dict):
            normalized.append("invalid")
            invalid_reasons.append("invalid_run_shape")
            continue
        run = cast(dict[str, object], raw)
        if not isinstance(run.get("run_id"), str):
            normalized.append("invalid")
            invalid_reasons.append("invalid_run_shape")
            continue
        run_id = cast(str, run["run_id"])
        coverage = coverage_by_run.get(run_id)
        if (
            coverage is None
            or coverage.get("evidence_coverage_complete") is not True
            or coverage.get("pattern_coverage_complete") is not True
        ):
            normalized.append("invalid")
            invalid_reasons.append(f"{run_id}:incomplete_frozen_input_coverage")
            continue
        decision = run.get("decision")
        if run.get("status") != "completed" or decision not in {"propose", "abstain"}:
            normalized.append("invalid")
            invalid_reasons.append(f"{run_id}:nonterminal_or_invalid_decision")
            continue
        if decision == "abstain":
            if run.get("candidates") not in ([], ()):
                normalized.append("invalid")
                invalid_reasons.append(f"{run_id}:abstain_with_candidate")
            else:
                normalized.append("abstain")
            continue
        raw_candidates = run.get("candidates")
        if not isinstance(raw_candidates, list):
            normalized.append("invalid")
            invalid_reasons.append(f"{run_id}:proposal_candidate_count")
            continue
        typed_candidates = cast(list[object], raw_candidates)
        if len(typed_candidates) != 1:
            normalized.append("invalid")
            invalid_reasons.append(f"{run_id}:proposal_candidate_count")
            continue
        candidate = typed_candidates[0]
        if not isinstance(candidate, dict):
            normalized.append("invalid")
            invalid_reasons.append(f"{run_id}:invalid_candidate_shape")
            continue
        typed_candidate = cast(dict[str, object], candidate)
        if (
            typed_candidate.get("target_id") != target_id
            or typed_candidate.get("direction") != "up"
            or typed_candidate.get("horizon_sessions") != eligible_horizon_sessions
        ):
            normalized.append("invalid")
            invalid_reasons.append(f"{run_id}:candidate_outside_frozen_eligibility")
            continue
        normalized.append("propose")

    propose_count = normalized.count("propose")
    abstain_count = normalized.count("abstain")
    invalid_count = normalized.count("invalid")
    if invalid_count:
        majority = "invalid"
    elif propose_count >= 2:
        majority = "propose"
    elif abstain_count >= 2:
        majority = "abstain"
    else:
        majority = "invalid"
    return {
        "arm_id": arm_report.get("arm_id"),
        "replicate_count": 3,
        "valid_run_count": 3 - invalid_count,
        "invalid_run_count": invalid_count,
        "propose_count": propose_count,
        "abstain_count": abstain_count,
        "majority_decision": majority,
        "invalid_reasons": invalid_reasons,
    }


def evaluate_checkpoint_exposure_path(
    *,
    rows: Sequence[Mapping[str, object]],
    start: date,
    end: date,
    checkpoint_decisions: Mapping[date, str],
    transaction_cost_bps_one_way: Decimal,
    annualization_sessions: int,
    minimum_risk_sessions: int,
    risk_free_rate_annual: Decimal,
    cvar_confidence: Decimal,
) -> dict[str, object]:
    if not checkpoint_decisions or start not in checkpoint_decisions:
        raise ValueError("regime Agent path requires a decision at the first session")
    if any(value not in {"propose", "abstain"} for value in checkpoint_decisions.values()):
        raise ValueError("regime Agent path decisions must be propose or abstain")
    path_rows = tuple(
        sorted(
            (item for item in rows if start <= _row_date(item) <= end),
            key=_row_date,
        )
    )
    if not path_rows or _row_date(path_rows[0]) != start or _row_date(path_rows[-1]) != end:
        raise ValueError("regime Agent path does not cover the registered interval")
    dates = tuple(_row_date(item) for item in path_rows)
    if not set(checkpoint_decisions) <= set(dates):
        raise ValueError("regime Agent checkpoint is outside the registered price path")
    if annualization_sessions < 1 or minimum_risk_sessions < 2:
        raise ValueError("regime Agent risk-session parameters are invalid")
    if not Decimal(0) < cvar_confidence < Decimal(1):
        raise ValueError("regime Agent cvar confidence must be between zero and one")
    cost_rate = transaction_cost_bps_one_way / _TEN_THOUSAND
    if cost_rate < 0:
        raise ValueError("regime Agent transaction cost cannot be negative")

    cash = Decimal(1)
    units = Decimal(0)
    invested = False
    values: list[Decimal] = []
    returns: list[Decimal] = []
    turnover = Decimal(0)
    modeled_cost = Decimal(0)
    prior_value = Decimal(1)
    active_decision = "abstain"
    for row in path_rows:
        day = _row_date(row)
        opening = _price(row, "open")
        closing = _price(row, "close")
        if day in checkpoint_decisions:
            active_decision = checkpoint_decisions[day]
            should_invest = active_decision == "propose"
            if should_invest and not invested:
                cost = cash * cost_rate
                modeled_cost += cost
                units = (cash - cost) / opening
                cash = Decimal(0)
                invested = True
                turnover += Decimal(1)
            elif not should_invest and invested:
                gross = units * opening
                cost = gross * cost_rate
                modeled_cost += cost
                cash = gross - cost
                units = Decimal(0)
                invested = False
                turnover += Decimal(1)
        value = units * closing if invested else cash
        returns.append(value / prior_value - Decimal(1))
        values.append(value)
        prior_value = value

    return _path_metrics(
        returns=tuple(returns),
        values=tuple(values),
        turnover=turnover,
        modeled_cost=modeled_cost,
        annualization_sessions=annualization_sessions,
        minimum_risk_sessions=minimum_risk_sessions,
        risk_free_rate_annual=risk_free_rate_annual,
        cvar_confidence=cvar_confidence,
    )


def eligible_horizon_sessions(
    primary: RegimeSeries,
    checkpoint_date: date,
    *,
    next_checkpoint_date: date | None,
    case_end: date,
) -> int:
    dates = tuple(
        _row_date(row)
        for row in primary.rows
        if checkpoint_date <= _row_date(row) <= case_end
        and (next_checkpoint_date is None or _row_date(row) < next_checkpoint_date)
    )
    if not dates or dates[0] != checkpoint_date:
        raise ValueError("regime checkpoint has no eligible trading-session horizon")
    return len(dates)


def select_checkpoint_records(
    records: tuple[RegimeEvidenceRecord, ...],
    *,
    market_case: MarketRegimeCase,
    study_case: RegimeStudyCase,
    registration: RegimeStudyRegistration,
    checkpoint: RegimeCheckpoint,
) -> tuple[RegimeEvidenceRecord, ...]:
    requirement_by_category = {item.category: item for item in study_case.source_requirements}
    selected: list[RegimeEvidenceRecord] = []
    for record in records:
        if market_case.case_key not in record.case_keys:
            continue
        requirement = requirement_by_category.get(record.category)
        if requirement is None or record.source_id not in requirement.source_ids:
            continue
        if not has_point_in_time_authority(record, checkpoint.cutoff_at):
            continue
        if record.category in {"market_price", "industry_price"}:
            expected_suffix = f"checkpoint={checkpoint.session_date.isoformat()}"
            if not record.source_ref.endswith(expected_suffix):
                continue
            if record.available_at <= checkpoint.cutoff_at:
                selected.append(record)
            continue
        if _inside_checkpoint_window(
            record,
            checkpoint=checkpoint,
            study_case=study_case,
            registration=registration,
        ):
            selected.append(record)
    by_lineage: dict[tuple[str, str], RegimeEvidenceRecord] = {}
    for record in sorted(selected, key=lambda item: (item.available_at, item.record_id)):
        by_lineage[(record.category, record.lineage_id)] = record
    return tuple(sorted(by_lineage.values(), key=lambda item: (item.category, item.record_id)))


def _inside_checkpoint_window(
    record: RegimeEvidenceRecord,
    *,
    checkpoint: RegimeCheckpoint,
    study_case: RegimeStudyCase,
    registration: RegimeStudyRegistration,
) -> bool:
    if record.published_at >= checkpoint.cutoff_at or record.available_at > checkpoint.cutoff_at:
        return False
    if record.source_updated_at is not None and record.source_updated_at > checkpoint.cutoff_at:
        return False
    protocol = registration.checkpoint_protocol
    if record.category == "established_news":
        lookback = dict(protocol.news_lookback_calendar_days)[study_case.decision_schedule]
    else:
        lookback = dict(protocol.maximum_age_calendar_days).get(record.category)
    return lookback is None or record.available_at >= checkpoint.cutoff_at - timedelta(
        days=lookback
    )


def _checkpoint_documents(
    *,
    validated_panel: ValidatedRegimePanel,
    market_case: MarketRegimeCase,
    checkpoint: RegimeCheckpoint,
    records_by_category: Mapping[str, tuple[RegimeEvidenceRecord, ...]],
    official_documents_by_hash: Mapping[str, Mapping[str, object]],
    news_documents_by_hash: Mapping[str, Mapping[str, object]],
    positioning_documents_by_hash: Mapping[str, Mapping[str, object]],
) -> dict[str, object]:
    series_by_id = _series_by_id(validated_panel)
    primary = series_by_id[market_case.primary_market_index]
    prior_primary_rows = tuple(
        row for row in primary.rows if _row_date(row) < checkpoint.session_date
    )
    if len(prior_primary_rows) < 60:
        raise ValueError("regime checkpoint document requires sixty prior market sessions")
    market_document: dict[str, object] = {
        "cutoff_at": _timestamp(checkpoint.cutoff_at),
        "feature_lag": "through_previous_session",
        "series_alias": _TARGET_ALIAS,
        "lookback_session_count": 60,
        "returns": {
            str(window): _window_return(prior_primary_rows, window) for window in (5, 20, 60)
        },
        "recent_close_to_close_returns": _recent_returns(prior_primary_rows, 10),
        "evidence_record_ids": [item.record_id for item in records_by_category["market_price"]],
        "authority_hashes": [item.authority_hash for item in records_by_category["market_price"]],
        "outcome_values_included": False,
    }
    industry_rows: list[dict[str, object]] = []
    for proxy_id in market_case.required_industry_proxies:
        series = series_by_id.get(proxy_id)
        if series is None:
            raise ValueError(f"regime checkpoint industry series is unavailable: {proxy_id}")
        rows = tuple(row for row in series.rows if _row_date(row) < checkpoint.session_date)
        if len(rows) < 20:
            raise ValueError("regime checkpoint industry document requires twenty prior sessions")
        industry_rows.append(
            {
                "series_alias": proxy_id,
                "return_5_sessions": _window_return(rows, 5),
                "return_20_sessions": _window_return(rows, 20),
            }
        )
    twenty = [Decimal(cast(str, item["return_20_sessions"])) for item in industry_rows]
    sorted_industries = sorted(
        industry_rows,
        key=lambda item: Decimal(cast(str, item["return_20_sessions"])),
        reverse=True,
    )
    industry_document: dict[str, object] = {
        "cutoff_at": _timestamp(checkpoint.cutoff_at),
        "feature_lag": "through_previous_session",
        "industry_count": len(industry_rows),
        "positive_breadth_20_sessions": sum(item > 0 for item in twenty),
        "median_return_20_sessions": _format(_median(tuple(twenty))),
        "dispersion_20_sessions": _format(max(twenty) - min(twenty)),
        "leaders": sorted_industries[:5],
        "laggards": sorted_industries[-5:],
        "all_industries": sorted_industries,
        "evidence_record_ids": [item.record_id for item in records_by_category["industry_price"]],
        "outcome_values_included": False,
    }
    news_articles: list[dict[str, object]] = []
    for record in sorted(
        records_by_category["established_news"],
        key=lambda item: (item.available_at, item.record_id),
    ):
        raw = news_documents_by_hash.get(record.content_hash)
        if raw is None:
            document: dict[str, object] = {
                "title": record.title,
                "description": None,
                "published_at": _timestamp(record.published_at),
                "source_updated_at": (
                    None
                    if record.source_updated_at is None
                    else _timestamp(record.source_updated_at)
                ),
                "publisher_id": record.publisher_id,
                "source_ref": record.source_ref,
                "article_excerpt": None,
                "content_hash": record.content_hash,
                "payload_status": "registered_version_not_retrievable_from_current_publisher_page",
            }
        else:
            document = dict(raw)
            if document.get("content_hash") != record.content_hash:
                raise ValueError("regime news research document does not bind the evidence record")
            document["payload_status"] = "exact_registered_version"
        excerpt = document.get("article_excerpt")
        if isinstance(excerpt, str) and len(excerpt) > 650:
            document["article_excerpt"] = excerpt[:649].rstrip() + "…"
        document["evidence_record_id"] = record.record_id
        document["available_at"] = _timestamp(record.available_at)
        news_articles.append(document)
    news_document: dict[str, object] = {
        "cutoff_at": _timestamp(checkpoint.cutoff_at),
        "article_count": len(news_articles),
        "publisher_count": len(
            {item.publisher_id for item in records_by_category["established_news"]}
        ),
        "exact_payload_count": sum(
            item.get("payload_status") == "exact_registered_version" for item in news_articles
        ),
        "articles": news_articles,
        "future_articles_included": False,
    }
    if len(json.dumps(news_document, ensure_ascii=False)) > 14_000:
        for article in news_articles:
            excerpt = article.get("article_excerpt")
            if isinstance(excerpt, str) and len(excerpt) > 300:
                article["article_excerpt"] = excerpt[:299].rstrip() + "…"
    positioning_rows: list[dict[str, object]] = []
    for record in sorted(
        records_by_category["positioning_or_expectations"],
        key=lambda item: (item.available_at, item.record_id),
    ):
        payload = positioning_documents_by_hash.get(record.content_hash)
        if payload is None:
            raise ValueError("regime margin payload does not bind the evidence record")
        if payload.get("content_hash") != record.content_hash:
            raise ValueError("regime margin research document has the wrong content hash")
        positioning_rows.append(
            {
                **{key: value for key, value in payload.items() if key != "content_hash"},
                "evidence_record_id": record.record_id,
                "available_at": _timestamp(record.available_at),
            }
        )
    positioning_document = {
        "cutoff_at": _timestamp(checkpoint.cutoff_at),
        "row_count": len(positioning_rows),
        "rows": positioning_rows,
        "unit_note": "provider-native exchange summary units",
        "future_rows_included": False,
    }
    return {
        "market-context": market_document,
        "industry-rotation": industry_document,
        "official-policy-context": _record_metadata_document(
            checkpoint,
            records_by_category["official_context"],
            documents_by_hash=official_documents_by_hash,
        ),
        "macro-vintage": _record_metadata_document(
            checkpoint, records_by_category["macro_vintage"]
        ),
        "timestamped-news-corpus": news_document,
        "positioning-flow": positioning_document,
    }


def _record_metadata_document(
    checkpoint: RegimeCheckpoint,
    records: tuple[RegimeEvidenceRecord, ...],
    *,
    documents_by_hash: Mapping[str, Mapping[str, object]] | None = None,
) -> dict[str, object]:
    materialized: list[dict[str, object]] = []
    for item in sorted(records, key=lambda value: (value.available_at, value.record_id)):
        raw = None if documents_by_hash is None else documents_by_hash.get(item.content_hash)
        if raw is None:
            document: dict[str, object] = {
                "title": item.title,
                "published_at": _timestamp(item.published_at),
                "source_updated_at": (
                    None if item.source_updated_at is None else _timestamp(item.source_updated_at)
                ),
                "publisher_alias": item.publisher_id,
                "source_ref": item.source_ref,
                "content_hash": item.content_hash,
                "payload_status": "metadata_only",
            }
        else:
            document = dict(raw)
            if document.get("content_hash") != item.content_hash:
                raise ValueError("regime official research document does not bind evidence record")
            document["payload_status"] = "exact_verified_archive_segment"
        document["evidence_record_id"] = item.record_id
        document["available_at"] = _timestamp(item.available_at)
        document["authority_hash"] = item.authority_hash
        materialized.append(document)
    return {
        "cutoff_at": _timestamp(checkpoint.cutoff_at),
        "records": materialized,
        "future_versions_included": False,
    }


def _checkpoint_eligible_return(
    path_rows: tuple[dict[str, object], ...],
    *,
    checkpoint_date: date,
    horizon_sessions: int,
) -> Decimal:
    if horizon_sessions < 1:
        raise ValueError("regime checkpoint outcome horizon must be positive")
    eligible = tuple(row for row in path_rows if _row_date(row) >= checkpoint_date)
    if len(eligible) < horizon_sessions or _row_date(eligible[0]) != checkpoint_date:
        raise ValueError("regime checkpoint outcome lacks its registered price horizon")
    return _price(eligible[horizon_sessions - 1], "close") / _price(eligible[0], "open") - 1


def _decision_is_correct(decision: str, realized_return: Decimal) -> bool:
    if decision == "propose":
        return realized_return > 0
    if decision == "abstain":
        return realized_return <= 0
    raise ValueError("regime checkpoint decision is unsupported")


def _incremental_decision_effect(
    control: str,
    treatment: str,
    realized_return: Decimal,
) -> str:
    if control == treatment or realized_return == 0:
        return "same_decision"
    return "helpful" if _decision_is_correct(treatment, realized_return) else "harmful"


def _market_context(
    *,
    series_by_id: Mapping[str, RegimeSeries],
    market_case: MarketRegimeCase,
) -> dict[str, object]:
    indices: list[dict[str, str]] = []
    for series_id in market_case.required_market_indices:
        series = series_by_id.get(series_id)
        if series is None:
            raise ValueError(f"regime market context lacks index series: {series_id}")
        indices.append(
            {
                "series_id": series_id,
                "open_to_close_return": _format(
                    _period_return(
                        series,
                        start=market_case.tradable_start,
                        end=market_case.end,
                    )
                ),
            }
        )
    industries: list[dict[str, str]] = []
    for proxy_id in market_case.required_industry_proxies:
        series = series_by_id.get(proxy_id)
        if series is None:
            raise ValueError(f"regime market context lacks industry series: {proxy_id}")
        industries.append(
            {
                "series_id": proxy_id,
                "open_to_close_return": _format(
                    _period_return(
                        series,
                        start=market_case.tradable_start,
                        end=market_case.end,
                    )
                ),
            }
        )
    ordered_industries = sorted(
        industries,
        key=lambda item: Decimal(item["open_to_close_return"]),
        reverse=True,
    )
    industry_returns = tuple(Decimal(item["open_to_close_return"]) for item in industries)
    return {
        "period_start": market_case.tradable_start.isoformat(),
        "period_end": market_case.end.isoformat(),
        "main_indices": indices,
        "industry_summary": {
            "industry_count": len(industries),
            "positive_industry_count": sum(value > 0 for value in industry_returns),
            "median_open_to_close_return": _format(_median(industry_returns)),
            "leaders": ordered_industries[:5],
            "laggards": ordered_industries[-5:],
            "all_industries": ordered_industries,
        },
    }


def _period_return(series: RegimeSeries, *, start: date, end: date) -> Decimal:
    rows = tuple(
        sorted(
            (row for row in series.rows if start <= _row_date(row) <= end),
            key=_row_date,
        )
    )
    if not rows or _row_date(rows[0]) != start or _row_date(rows[-1]) != end:
        raise ValueError("regime market context series does not cover the full case interval")
    return _price(rows[-1], "close") / _price(rows[0], "open") - 1


def _window_return(rows: tuple[dict[str, object], ...], window: int) -> str:
    if len(rows) <= window:
        raise ValueError("regime market return window lacks history")
    start = _price(rows[-1 - window], "close")
    end = _price(rows[-1], "close")
    return _format(end / start - Decimal(1))


def _recent_returns(rows: tuple[dict[str, object], ...], count: int) -> list[dict[str, str]]:
    if len(rows) <= count:
        raise ValueError("regime recent-return document lacks history")
    output: list[dict[str, str]] = []
    for previous, current in zip(rows[-count - 1 : -1], rows[-count:], strict=True):
        output.append(
            {
                "trade_date": _row_date(current).isoformat(),
                "return": _format(
                    _price(current, "close") / _price(previous, "close") - Decimal(1)
                ),
            }
        )
    return output


def _series_by_id(validated_panel: ValidatedRegimePanel) -> dict[str, RegimeSeries]:
    panel = validated_panel.panel
    result = {item.series_id: item for item in panel.series}
    by_code = {item.tushare_code: item for item in panel.series}
    for proxy_id, code in panel.proxy_resolution:
        if code in by_code:
            result[proxy_id] = by_code[code]
    return result


def _median(values: tuple[Decimal, ...]) -> Decimal:
    ordered = sorted(values)
    middle = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[middle]
    return (ordered[middle - 1] + ordered[middle]) / Decimal(2)


def _write_private_json(path: Path, payload: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    path.parent.chmod(0o700)
    if path.is_symlink():
        raise ValueError("regime Agent artifact destination cannot be a symlink")
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temporary = Path(temporary_name)
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        temporary.replace(path)
        path.chmod(0o600)
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise
    if stat.S_IMODE(path.stat().st_mode) != 0o600:
        raise ValueError("regime Agent artifact must use mode 0600")


def _timestamp(value: datetime) -> str:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("regime Agent timestamp must be timezone-aware")
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _path_metrics(
    *,
    returns: tuple[Decimal, ...],
    values: tuple[Decimal, ...],
    turnover: Decimal,
    modeled_cost: Decimal,
    annualization_sessions: int,
    minimum_risk_sessions: int,
    risk_free_rate_annual: Decimal,
    cvar_confidence: Decimal,
) -> dict[str, object]:
    total_return = values[-1] - Decimal(1)
    peak = Decimal(1)
    max_drawdown = Decimal(0)
    for value in values:
        peak = max(peak, value)
        max_drawdown = min(max_drawdown, value / peak - Decimal(1))
    tail_count = max(1, math.ceil((1 - float(cvar_confidence)) * len(returns)))
    cvar = sum(sorted(returns)[:tail_count], Decimal(0)) / Decimal(tail_count)
    enough = len(returns) >= minimum_risk_sessions
    annualized_return: Decimal | None = None
    annualized_volatility: Decimal | None = None
    sharpe: Decimal | None = None
    if enough:
        annualized_return = Decimal(
            str((1 + float(total_return)) ** (annualization_sessions / len(returns)) - 1)
        )
        volatility = _sample_std(returns)
        if volatility > 0:
            annualized_volatility = volatility * Decimal(str(math.sqrt(annualization_sessions)))
            daily_risk_free = Decimal(
                str((1 + float(risk_free_rate_annual)) ** (1 / annualization_sessions) - 1)
            )
            sharpe = (
                (_mean(returns) - daily_risk_free)
                / volatility
                * Decimal(str(math.sqrt(annualization_sessions)))
            )
    return {
        "status": "covered",
        "session_count": len(returns),
        "risk_metrics_eligible": enough,
        "total_return": _format(total_return),
        "annualized_return": _optional_format(annualized_return),
        "annualized_volatility": _optional_format(annualized_volatility),
        "sharpe": _optional_format(sharpe),
        "max_drawdown": _format(max_drawdown),
        "cvar": _format(cvar),
        "turnover": _format(turnover),
        "modeled_cost": _format(modeled_cost),
    }


def _row_date(row: Mapping[str, object]) -> date:
    value = row.get("trade_date")
    if not isinstance(value, str):
        raise TypeError("regime Agent price row trade_date must be a string")
    return date.fromisoformat(value) if "-" in value else datetime.strptime(value, "%Y%m%d").date()


def _price(row: Mapping[str, object], key: str) -> Decimal:
    value = row.get(key)
    if isinstance(value, bool) or not isinstance(value, (int, float, str)):
        raise TypeError(f"regime Agent price row {key} must be numeric")
    result = Decimal(str(value))
    if not result.is_finite() or result <= 0:
        raise ValueError(f"regime Agent price row {key} must be positive")
    return result


def _mean(values: tuple[Decimal, ...]) -> Decimal:
    return sum(values, Decimal(0)) / Decimal(len(values))


def _sample_std(values: tuple[Decimal, ...]) -> Decimal:
    if len(values) < 2:
        return Decimal(0)
    mean = _mean(values)
    variance = sum(((item - mean) ** 2 for item in values), Decimal(0)) / Decimal(len(values) - 1)
    return variance.sqrt()


def _format(value: Decimal) -> str:
    return f"{value.quantize(_RETURN_QUANTUM, rounding=ROUND_HALF_EVEN):.8f}"


def _optional_format(value: Decimal | None) -> str | None:
    return None if value is None else _format(value)
