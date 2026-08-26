from __future__ import annotations

import argparse
import asyncio
import json
import os
import platform
import sys
from collections.abc import Iterable, Sequence
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Protocol, cast

from jsonschema import Draft202012Validator, FormatChecker, ValidationError

from market_impact_agent import __version__
from market_impact_agent.accrual import (
    AccrualDisposition,
    AccrualLedger,
    candidate_event_observation_from_dict,
)
from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.agent_runtime import (
    ProviderPricing,
    RuntimeBudget,
    RuntimeConfig,
    SkillRegistry,
    ToolAccessContext,
    ToolRegistry,
    ToolSideEffect,
)
from market_impact_agent.agent_schema import validate_agent_contract
from market_impact_agent.agent_study import load_agent_phase2_preregistration
from market_impact_agent.backtests import (
    BacktestRunStatus,
    backtest_request_from_dict,
    backtest_result_to_dict,
)
from market_impact_agent.calibration import (
    assess_phase2_calibration,
    load_phase2_calibration_evidence,
    phase2_calibration_gate_result_to_dict,
)
from market_impact_agent.energy_monitor import EnergySourceMonitor
from market_impact_agent.events import event_transmission_chronology_errors
from market_impact_agent.evidence_freeze import freeze_due_evidence_packs
from market_impact_agent.frozen_research import FrozenResearchRepository
from market_impact_agent.minimax_provider import MiniMaxOpenAIProvider
from market_impact_agent.observations import (
    ValidatedObservationBundle,
    validate_prediction_market_batch,
    write_prediction_market_batch,
)
from market_impact_agent.prediction_markets import (
    KalshiPublicAdapter,
    PolymarketPublicAdapter,
    PredictionMarketAdapter,
    WorldMonitorPredictionAdapter,
    kalshi_provider_manifest,
    polymarket_provider_manifest,
    world_monitor_provider_manifest,
)
from market_impact_agent.providers import MockExecutionProvider, ProviderManifest
from market_impact_agent.registry import ProviderRegistry
from market_impact_agent.runtime_store import ArtifactStore, RunJournal, RunStatus
from market_impact_agent.source_coverage import (
    coverage_receipt_from_dict,
    load_source_coverage_registration,
)
from market_impact_agent.tushare import TushareHttpAdapter
from market_impact_agent.tushare_bundle import (
    TushareDataRequest,
    ValidatedTushareDataBundle,
    capture_tushare_data_bundle,
    validate_tushare_data_bundle,
    write_tushare_data_bundle,
)


class EventTransmissionValidator(Protocol):
    def iter_errors(self, instance: object) -> Iterable[ValidationError]: ...


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="market-impact")
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("status", help="Print fail-closed runtime and provider status")

    provider_parser = subparsers.add_parser("provider", help="Inspect provider manifests")
    provider_subparsers = provider_parser.add_subparsers(dest="provider_command", required=True)
    validate_parser = provider_subparsers.add_parser(
        "validate", help="Validate a provider manifest"
    )
    validate_parser.add_argument("path", type=Path)

    event_parser = subparsers.add_parser(
        "event", help="Validate point-in-time event transmission records"
    )
    event_subparsers = event_parser.add_subparsers(dest="event_command", required=True)
    event_validate_parser = event_subparsers.add_parser(
        "validate", help="Validate a point-in-time event assessment"
    )
    event_validate_parser.add_argument("path", type=Path)

    prediction_parser = subparsers.add_parser(
        "prediction", help="Capture or validate read-only prediction-market observations"
    )
    prediction_subparsers = prediction_parser.add_subparsers(
        dest="prediction_command",
        required=True,
    )
    prediction_capture_parser = prediction_subparsers.add_parser(
        "capture", help="Capture one current public or aggregated market snapshot"
    )
    prediction_capture_parser.add_argument(
        "--provider",
        required=True,
        choices=("polymarket", "kalshi", "world-monitor"),
    )
    prediction_capture_parser.add_argument("--limit", type=int, default=20)
    prediction_capture_parser.add_argument("--query")
    prediction_capture_parser.add_argument(
        "--output-root",
        type=Path,
        default=Path(".market-impact/observations"),
    )
    prediction_validate_parser = prediction_subparsers.add_parser(
        "validate", help="Validate one local prediction-market observation bundle"
    )
    prediction_validate_parser.add_argument("path", type=Path)

    tushare_parser = subparsers.add_parser(
        "tushare", help="Capture or validate local Tushare data bundles"
    )
    tushare_subparsers = tushare_parser.add_subparsers(dest="tushare_command", required=True)
    tushare_capture_parser = tushare_subparsers.add_parser(
        "capture", help="Capture one token-backed read-only data window"
    )
    tushare_capture_parser.add_argument("--instrument", required=True)
    tushare_capture_parser.add_argument("--as-of-date", required=True, type=_compact_date)
    tushare_capture_parser.add_argument("--data-start-date", type=_compact_date)
    tushare_capture_parser.add_argument("--start-date", required=True, type=_compact_date)
    tushare_capture_parser.add_argument("--end-date", required=True, type=_compact_date)
    tushare_validate_parser = tushare_subparsers.add_parser(
        "validate", help="Validate one local Tushare data bundle"
    )
    tushare_validate_parser.add_argument("path", type=Path)

    backtest_parser = subparsers.add_parser("backtest", help="Run deterministic backtests")
    backtest_subparsers = backtest_parser.add_subparsers(dest="backtest_command", required=True)
    backtest_run_parser = backtest_subparsers.add_parser(
        "run", help="Replay one strict request from a validated private Data Snapshot"
    )
    backtest_run_parser.add_argument("--request", required=True, type=Path)
    backtest_run_parser.add_argument("--data-snapshot", required=True, type=Path)
    phase2_gate_parser = backtest_subparsers.add_parser(
        "phase2-gate", help="Evaluate frozen repeated results against the Phase 2 exit gate"
    )
    phase2_gate_parser.add_argument("--evidence", required=True, type=Path)
    phase2_register_parser = backtest_subparsers.add_parser(
        "phase2-register", help="Bind the frozen public cohort to exact private snapshots"
    )
    phase2_register_parser.add_argument("--cohort", required=True, type=Path)
    phase2_register_parser.add_argument("--data-snapshot-root", required=True, type=Path)
    phase2_register_parser.add_argument("--output", required=True, type=Path)
    phase2_run_parser = backtest_subparsers.add_parser(
        "phase2-run", help="Execute every registered long decision twice"
    )
    phase2_run_parser.add_argument("--registration", required=True, type=Path)
    phase2_run_parser.add_argument("--data-snapshot-root", required=True, type=Path)
    phase2_run_parser.add_argument("--output-dir", required=True, type=Path)

    agent_parser = subparsers.add_parser(
        "agent", help="Validate or run frozen Agent research without broker reachability"
    )
    agent_subparsers = agent_parser.add_subparsers(dest="agent_command", required=True)
    agent_validate_parser = agent_subparsers.add_parser(
        "validate", help="Validate one frozen Evidence Pack and its bound local content"
    )
    _add_agent_bundle_arguments(agent_validate_parser)
    agent_run_parser = agent_subparsers.add_parser(
        "run", help="Run one local MiniMax judgment against a frozen Evidence Pack"
    )
    _add_agent_bundle_arguments(agent_run_parser)
    agent_run_parser.add_argument("--run-id", required=True)
    agent_run_parser.add_argument(
        "--skill-root",
        type=Path,
        default=_default_agent_skill_root(),
    )
    agent_run_parser.add_argument(
        "--state-root",
        type=Path,
        default=Path(".market-impact/agent-runs"),
    )
    agent_study_parser = agent_subparsers.add_parser(
        "study-validate",
        help="Validate the prospective Agent Phase 2 study and Exposure Registry",
    )
    agent_study_parser.add_argument("--registration", required=True, type=Path)
    agent_study_parser.add_argument("--exposure-registry", required=True, type=Path)
    agent_study_parser.add_argument(
        "--source-coverage-registration",
        required=True,
        type=Path,
    )
    agent_observe_parser = agent_subparsers.add_parser(
        "study-observe",
        help="Append one Candidate Event Observation to the prospective accrual ledger",
    )
    agent_observe_parser.add_argument("--registration", required=True, type=Path)
    agent_observe_parser.add_argument("--exposure-registry", required=True, type=Path)
    agent_observe_parser.add_argument(
        "--source-coverage-registration",
        required=True,
        type=Path,
    )
    agent_observe_parser.add_argument("--coverage-receipt", required=True, type=Path)
    agent_observe_parser.add_argument("--observation", required=True, type=Path)
    agent_observe_parser.add_argument("--raw-source", required=True, type=Path)
    agent_observe_parser.add_argument("--regional-denominator-source", type=Path)
    agent_observe_parser.add_argument("--ledger", type=Path)
    agent_ledger_parser = agent_subparsers.add_parser(
        "study-ledger-validate",
        help="Validate and summarize an existing prospective accrual ledger",
    )
    agent_ledger_parser.add_argument("--registration", required=True, type=Path)
    agent_ledger_parser.add_argument("--exposure-registry", required=True, type=Path)
    agent_ledger_parser.add_argument(
        "--source-coverage-registration",
        required=True,
        type=Path,
    )
    agent_ledger_parser.add_argument("--ledger", required=True, type=Path)
    source_poll_parser = agent_subparsers.add_parser(
        "study-source-poll",
        help="Poll frozen energy sources, retain receipts, and record candidate observations",
    )
    source_poll_parser.add_argument("--registration", required=True, type=Path)
    source_poll_parser.add_argument("--exposure-registry", required=True, type=Path)
    source_poll_parser.add_argument(
        "--source-coverage-registration",
        required=True,
        type=Path,
    )
    source_poll_parser.add_argument("--ledger", type=Path)
    source_poll_parser.add_argument(
        "--monitor-root",
        type=Path,
        default=Path(".market-impact/source-monitor"),
    )
    freeze_due_parser = agent_subparsers.add_parser(
        "study-freeze-due",
        help="Freeze point-in-time Evidence Packs whose registered cutoff has passed",
    )
    freeze_due_parser.add_argument("--registration", required=True, type=Path)
    freeze_due_parser.add_argument("--exposure-registry", required=True, type=Path)
    freeze_due_parser.add_argument(
        "--source-coverage-registration",
        required=True,
        type=Path,
    )
    freeze_due_parser.add_argument("--ledger", required=True, type=Path)
    freeze_due_parser.add_argument(
        "--pattern-pack",
        action="append",
        required=True,
        type=Path,
    )
    freeze_due_parser.add_argument(
        "--output-root",
        type=Path,
        default=Path(".market-impact/prospective-evidence"),
    )
    return parser


def default_registry() -> ProviderRegistry:
    registry = ProviderRegistry()
    registry.register(MockExecutionProvider())
    return registry


def status_payload() -> dict[str, object]:
    return {
        "project": "market-impact-agent",
        "version": __version__,
        "python": platform.python_version(),
        "live_trading": "disabled",
        "agent_runtime": {
            "status": "accepted_local_research_v2",
            "provider": "minimax-openai-compatible",
            "model": "MiniMax-M3",
            "tool_authority": "read_only",
            "broker_reachability": False,
            "provider_portability": "not_established",
        },
        "providers": [manifest.to_dict() for manifest in default_registry().manifests()],
        "observation_providers": [
            manifest.to_dict()
            for manifest in (
                polymarket_provider_manifest(),
                kalshi_provider_manifest(),
                world_monitor_provider_manifest(),
            )
        ],
    }


def validate_provider(path: Path) -> dict[str, object]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    manifest = ProviderManifest.from_dict(payload)
    errors = manifest.validation_errors()
    return {
        "path": path.as_posix(),
        "provider_id": manifest.provider_id,
        "valid": not errors,
        "errors": list(errors),
        "verified_capabilities": sorted(
            capability.value for capability in manifest.verified_capabilities
        ),
    }


def validate_event(path: Path) -> dict[str, object]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    schema_errors = _event_transmission_schema_errors(payload)
    errors = schema_errors or event_transmission_chronology_errors(payload)
    return {
        "path": path.as_posix(),
        "valid": not errors,
        "errors": list(errors),
    }


def capture_tushare(
    *,
    token: str,
    tushare_code: str,
    as_of_date: date,
    start_date: date,
    end_date: date,
    data_start_date: date | None = None,
    output_root: Path = Path(".market-impact/tushare"),
) -> ValidatedTushareDataBundle:
    request = TushareDataRequest(
        tushare_code=tushare_code,
        as_of_date=as_of_date,
        start_date=start_date if data_start_date is None else data_start_date,
        end_date=end_date,
        evaluation_start_date=start_date if data_start_date is not None else None,
    )
    capture = capture_tushare_data_bundle(TushareHttpAdapter(token), request)
    path = write_tushare_data_bundle(capture, output_root)
    return validate_tushare_data_bundle(path)


def capture_prediction_markets(
    adapter: PredictionMarketAdapter,
    *,
    limit: int,
    query: str | None,
    output_root: Path,
) -> ValidatedObservationBundle:
    batch = adapter.fetch_markets(limit=limit, query=query)
    return write_prediction_market_batch(batch, output_root)


def validate_agent_bundle(
    *,
    evidence_pack_path: Path,
    evidence_documents_path: Path,
    pattern_pack_paths: tuple[Path, ...],
) -> dict[str, object]:
    evidence_payload = json.loads(evidence_pack_path.read_text(encoding="utf-8"))
    evidence_errors = validate_agent_contract(
        evidence_payload,
        "evidence-pack.schema.json",
    )
    pattern_errors: list[str] = []
    for path in pattern_pack_paths:
        payload = json.loads(path.read_text(encoding="utf-8"))
        pattern_errors.extend(
            f"{path}: {error}"
            for error in validate_agent_contract(payload, "pattern-pack.schema.json")
        )
    errors = tuple(evidence_errors) + tuple(pattern_errors)
    if errors:
        return {"valid": False, "errors": list(errors)}
    repository = FrozenResearchRepository.from_files(
        evidence_pack_path=evidence_pack_path,
        evidence_documents_path=evidence_documents_path,
        pattern_pack_paths=pattern_pack_paths,
    )
    return {
        "valid": True,
        "errors": [],
        "event_id": repository.evidence_pack.event_id,
        "evidence_pack_id": repository.evidence_pack.pack_id,
        "evidence_count": len(repository.evidence_pack.evidence),
        "pattern_pack_count": len(repository.evidence_pack.pattern_packs),
        "allowed_targets": list(repository.evidence_pack.allowed_targets),
        "synthetic_or_licensed_data_must_remain_local": True,
    }


def validate_agent_phase2_study(
    *,
    registration_path: Path,
    exposure_registry_path: Path,
    source_coverage_registration_path: Path,
) -> dict[str, object]:
    registration_payload = json.loads(registration_path.read_text(encoding="utf-8"))
    registry_payload = json.loads(exposure_registry_path.read_text(encoding="utf-8"))
    coverage_payload = json.loads(source_coverage_registration_path.read_text(encoding="utf-8"))
    errors = (
        tuple(
            f"registration {error}"
            for error in validate_agent_contract(
                registration_payload,
                "agent-phase2-preregistration.schema.json",
            )
        )
        + tuple(
            f"exposure_registry {error}"
            for error in validate_agent_contract(
                registry_payload,
                "exposure-registry.schema.json",
            )
        )
        + tuple(
            f"source_coverage {error}"
            for error in validate_agent_contract(
                coverage_payload,
                "source-coverage-registration.schema.json",
            )
        )
    )
    if errors:
        return {"valid": False, "errors": list(errors)}
    registration, registry = load_agent_phase2_preregistration(
        registration_path,
        exposure_registry_path,
    )
    coverage = load_source_coverage_registration(source_coverage_registration_path)
    if (
        coverage.prospective_registration_id != registration.registration_id
        or coverage.prospective_registration_hash != registration.registration_hash
    ):
        raise ValueError("Source Coverage Registration does not match prospective study")
    if coverage.registered_at >= registration.accrual.opens_after:
        raise ValueError("Source Coverage Registration was not frozen before accrual")
    return {
        "valid": True,
        "errors": [],
        "registration_id": registration.registration_id,
        "registration_hash": registration.registration_hash,
        "exposure_registry_id": registry.registry_id,
        "exposure_registry_hash": registry.registry_hash,
        "source_coverage_registration_id": coverage.coverage_registration_id,
        "source_coverage_registration_hash": coverage.coverage_registration_hash,
        "required_source_count": sum(item.required for item in coverage.sources),
        "selection_eligible_target_count": sum(
            item.selection_eligible for item in registry.entries
        ),
        "target_event_count": registration.accrual.target_event_count,
        "replicate_count": registration.agent_protocol.replicate_count,
        "all_event_denominator": registration.evaluation.all_event_denominator,
        "holdout_outcomes_opened": registration.holdout_outcomes_opened,
        "execution_capability": registration.execution_capability,
    }


def observe_agent_phase2_study(
    *,
    registration_path: Path,
    exposure_registry_path: Path,
    source_coverage_registration_path: Path,
    coverage_receipt_path: Path,
    observation_path: Path,
    raw_source_path: Path,
    regional_denominator_source_path: Path | None,
    ledger_path: Path | None,
    recorded_at: datetime,
) -> dict[str, object]:
    study_result = validate_agent_phase2_study(
        registration_path=registration_path,
        exposure_registry_path=exposure_registry_path,
        source_coverage_registration_path=source_coverage_registration_path,
    )
    if not study_result["valid"]:
        errors = study_result.get("errors", [])
        raise ValueError(f"prospective study contracts are invalid: {errors}")
    observation_payload = json.loads(observation_path.read_text(encoding="utf-8"))
    observation_errors = validate_agent_contract(
        observation_payload,
        "candidate-event-observation.schema.json",
    )
    if observation_errors:
        raise ValueError(
            "Candidate Event Observation schema validation failed: " + "; ".join(observation_errors)
        )
    observation = candidate_event_observation_from_dict(observation_payload)
    receipt_payload = json.loads(coverage_receipt_path.read_text(encoding="utf-8"))
    receipt_errors = validate_agent_contract(
        receipt_payload,
        "coverage-receipt.schema.json",
    )
    if receipt_errors:
        raise ValueError("Coverage Receipt schema validation failed: " + "; ".join(receipt_errors))
    coverage_receipt = coverage_receipt_from_dict(receipt_payload)
    raw_source = _read_source_artifact(raw_source_path, "raw source")
    regional_denominator_source = (
        None
        if regional_denominator_source_path is None
        else _read_source_artifact(
            regional_denominator_source_path,
            "regional denominator source",
        )
    )
    registration, registry = load_agent_phase2_preregistration(
        registration_path,
        exposure_registry_path,
    )
    coverage_registration = load_source_coverage_registration(source_coverage_registration_path)
    resolved_ledger_path = (
        ledger_path
        if ledger_path is not None
        else Path(".market-impact/accrual") / registration.registration_hash / "ledger.sqlite3"
    )
    ledger = AccrualLedger(
        resolved_ledger_path,
        registration=registration,
        registry=registry,
        coverage_registration=coverage_registration,
        created_at=recorded_at,
    )
    decision = ledger.record(
        observation,
        recorded_at=recorded_at,
        raw_source=raw_source,
        coverage_receipt=coverage_receipt,
        regional_denominator_source=regional_denominator_source,
    )
    return {
        "recorded": True,
        "observation_id": observation.observation_id,
        "event_id": observation.event_id,
        "sequence": decision.sequence,
        "disposition": decision.disposition.value,
        "accrued": decision.disposition is AccrualDisposition.ACCRUED,
        "reasons": [item.value for item in decision.reasons],
        "qualifying_visible_at": (
            None
            if decision.qualifying_visible_at is None
            else decision.qualifying_visible_at.isoformat().replace("+00:00", "Z")
        ),
        "evidence_cutoff_at": (
            None
            if decision.evidence_cutoff_at is None
            else decision.evidence_cutoff_at.isoformat().replace("+00:00", "Z")
        ),
        "accrued_event_id": decision.accrued_event_id,
        "decision_hash": decision.decision_hash,
        "ledger_hash": ledger.ledger_hash,
        "accrued_event_count": ledger.accrued_event_count,
        "target_event_count": registration.accrual.target_event_count,
        "ledger_path": ledger.path.as_posix(),
        "source_artifact_root": ledger.source_artifacts.root.as_posix(),
        "execution_capability": "none",
    }


def validate_agent_phase2_ledger(
    *,
    registration_path: Path,
    exposure_registry_path: Path,
    source_coverage_registration_path: Path,
    ledger_path: Path,
    inspected_at: datetime,
) -> dict[str, object]:
    if not ledger_path.is_file():
        raise FileNotFoundError(f"Accrual Ledger does not exist: {ledger_path}")
    registration, registry = load_agent_phase2_preregistration(
        registration_path,
        exposure_registry_path,
    )
    coverage_registration = load_source_coverage_registration(source_coverage_registration_path)
    ledger = AccrualLedger(
        ledger_path,
        registration=registration,
        registry=registry,
        coverage_registration=coverage_registration,
        created_at=inspected_at,
    )
    decisions = ledger.decisions()
    return {
        "valid": True,
        "registration_id": registration.registration_id,
        "ledger_path": ledger.path.as_posix(),
        "ledger_hash": ledger.ledger_hash,
        "decision_count": len(decisions),
        "accrued_event_count": ledger.accrued_event_count,
        "target_event_count": registration.accrual.target_event_count,
        "cohort_complete": (ledger.accrued_event_count >= registration.accrual.target_event_count),
        "last_decision_hash": None if not decisions else decisions[-1].decision_hash,
        "execution_capability": "none",
    }


def poll_agent_phase2_sources(
    *,
    registration_path: Path,
    exposure_registry_path: Path,
    source_coverage_registration_path: Path,
    ledger_path: Path | None,
    monitor_root: Path,
    started_at: datetime,
) -> dict[str, object]:
    study = validate_agent_phase2_study(
        registration_path=registration_path,
        exposure_registry_path=exposure_registry_path,
        source_coverage_registration_path=source_coverage_registration_path,
    )
    if not study["valid"]:
        raise ValueError(f"prospective study contracts are invalid: {study['errors']}")
    registration, registry = load_agent_phase2_preregistration(
        registration_path,
        exposure_registry_path,
    )
    coverage = load_source_coverage_registration(source_coverage_registration_path)
    resolved_ledger_path = (
        ledger_path
        if ledger_path is not None
        else Path(".market-impact/accrual") / registration.registration_hash / "ledger.sqlite3"
    )
    ledger = AccrualLedger(
        resolved_ledger_path,
        registration=registration,
        registry=registry,
        coverage_registration=coverage,
        created_at=started_at,
    )
    latest = {item.observation.event_id: item.observation for item in ledger.decisions()}
    monitor = EnergySourceMonitor(
        registration=coverage,
        root=monitor_root / coverage.coverage_registration_hash,
    )
    cycle = monitor.poll(latest_observations=latest)
    decisions = tuple(
        ledger.record(
            observation,
            recorded_at=cycle.receipt.cycle_completed_at,
            raw_source=cycle.raw_source_for(observation),
            coverage_receipt=cycle.receipt,
        )
        for observation in cycle.candidates
    )
    return {
        "polled": True,
        "coverage_receipt_id": cycle.receipt.receipt_id,
        "coverage_receipt_hash": cycle.receipt.receipt_hash,
        "coverage_complete": cycle.receipt.is_complete(coverage),
        "attempts": [
            {
                "provider_id": item.provider_id,
                "succeeded": item.succeeded,
                "record_count": item.record_count,
                "error_class": item.error_class,
            }
            for item in cycle.receipt.attempts
        ],
        "candidate_count": len(cycle.candidates),
        "decisions": [
            {
                "event_id": item.observation.event_id,
                "observation_id": item.observation.observation_id,
                "disposition": item.disposition.value,
                "reasons": [reason.value for reason in item.reasons],
                "accrued_event_id": item.accrued_event_id,
                "evidence_cutoff_at": (
                    None
                    if item.evidence_cutoff_at is None
                    else item.evidence_cutoff_at.isoformat().replace("+00:00", "Z")
                ),
            }
            for item in decisions
        ],
        "ledger_path": ledger.path.as_posix(),
        "receipt_path": cycle.receipt_path.as_posix(),
        "source_artifact_root": cycle.artifact_root.as_posix(),
        "execution_capability": "none",
    }


def freeze_agent_phase2_due(
    *,
    registration_path: Path,
    exposure_registry_path: Path,
    source_coverage_registration_path: Path,
    ledger_path: Path,
    pattern_pack_paths: tuple[Path, ...],
    output_root: Path,
    now: datetime,
) -> dict[str, object]:
    if not ledger_path.is_file():
        raise FileNotFoundError(f"Accrual Ledger does not exist: {ledger_path}")
    registration, registry = load_agent_phase2_preregistration(
        registration_path,
        exposure_registry_path,
    )
    coverage = load_source_coverage_registration(source_coverage_registration_path)
    ledger = AccrualLedger(
        ledger_path,
        registration=registration,
        registry=registry,
        coverage_registration=coverage,
        created_at=now,
    )
    batch = freeze_due_evidence_packs(
        ledger=ledger,
        registry=registry,
        pattern_pack_paths=pattern_pack_paths,
        output_root=output_root / registration.registration_hash,
        now=now,
    )
    return {
        "frozen_count": len(batch.frozen),
        "frozen": [
            {
                "accrued_event_id": item.accrued_event_id,
                "evidence_pack_id": item.evidence_pack.pack_id,
                "evidence_cutoff_at": item.evidence_pack.as_of.isoformat().replace("+00:00", "Z"),
                "root": item.root.as_posix(),
                "already_existed": item.already_existed,
            }
            for item in batch.frozen
        ],
        "pending_event_ids": list(batch.pending_event_ids),
        "execution_capability": "none",
    }


async def run_agent_bundle(
    *,
    evidence_pack_path: Path,
    evidence_documents_path: Path,
    pattern_pack_paths: tuple[Path, ...],
    run_id: str,
    skill_root: Path,
    state_root: Path,
) -> dict[str, object]:
    try:
        from market_impact_agent.agent_engine import AgentEngine, AgentRunRequest
    except ModuleNotFoundError as exc:
        if exc.name == "mcp":
            raise RuntimeError(
                "Agent execution requires the optional dependency group; "
                "install market-impact-agent[agent]"
            ) from None
        raise
    repository = FrozenResearchRepository.from_files(
        evidence_pack_path=evidence_pack_path,
        evidence_documents_path=evidence_documents_path,
        pattern_pack_paths=pattern_pack_paths,
    )
    provider = MiniMaxOpenAIProvider.from_environment()
    await provider.assert_model_available(timeout_seconds=30)
    state_directory = state_root / canonical_hash(run_id)
    artifact_store = ArtifactStore(state_directory / "artifacts")
    tool_registry = ToolRegistry(artifact_store)
    for descriptor in repository.tool_descriptors():
        tool_registry.register(descriptor)
    config = RuntimeConfig(
        provider_id=provider.provider_id,
        model=provider.model,
        context_window_tokens=131_072,
        reserved_output_tokens=8_192,
        temperature=1,
        top_p=0.95,
        budget=RuntimeBudget(
            max_turns=8,
            max_tool_calls=12,
            max_input_tokens=500_000,
            max_output_tokens=32_768,
            max_wall_seconds=300,
            max_result_bytes=256_000,
        ),
        pricing=ProviderPricing(
            pricing_id="minimax-m3-paygo-2026-08-26-context-le-512k",
            input_microusd_per_million_tokens=300_000,
            output_microusd_per_million_tokens=1_200_000,
        ),
    )
    api_key = os.environ.get("MINIMAX_API_KEY", "")
    engine = AgentEngine(
        provider=provider,
        config=config,
        artifact_store=artifact_store,
        journal=RunJournal(state_directory / "run.sqlite3"),
        tool_registry=tool_registry,
        skill_registry=SkillRegistry(skill_root),
        secret_values=(api_key,),
    )
    result = await engine.run(
        AgentRunRequest(
            run_id=run_id,
            evidence_pack=repository.evidence_pack,
            research_instruction=(
                "Assess this physical energy supply shock. Before deciding, call "
                "read_pattern_pack for every referenced Pattern Pack and call read_evidence "
                "for every Evidence Pack item. Apply only patterns whose conditions are "
                "supported, test offsets and counterevidence, and abstain if a critical link "
                "is unresolved."
            ),
            selected_skills=("energy-supply",),
            tool_access=ToolAccessContext(
                allowed_capabilities=frozenset({"evidence.read", "pattern.read"}),
                allowed_side_effects=frozenset({ToolSideEffect.READ_ONLY}),
                allowed_tools=frozenset({"read_evidence", "read_pattern_pack"}),
            ),
        )
    )
    payload: dict[str, object] = {
        "run_id": result.run_id,
        "status": result.status.value,
        "terminal_store_hash": result.terminal_store_hash,
        "state_directory": state_directory.as_posix(),
        "broker_reachability": False,
    }
    if result.metrics is not None:
        payload["metrics"] = result.metrics.to_dict()
    if result.judgment is not None:
        payload["judgment"] = result.judgment.to_dict()
    return payload


def _add_agent_bundle_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--evidence-pack", required=True, type=Path)
    parser.add_argument("--evidence-documents", required=True, type=Path)
    parser.add_argument(
        "--pattern-pack",
        required=True,
        action="append",
        type=Path,
        dest="pattern_packs",
    )


def _default_agent_skill_root() -> Path:
    package_root = Path(__file__).resolve().parent
    installed = package_root / "builtin_skills"
    if installed.is_dir():
        return installed
    return package_root.parents[1] / "skills"


def _event_transmission_schema_errors(payload: object) -> tuple[str, ...]:
    schema_path = _event_transmission_schema_path()
    schema = json.loads(schema_path.read_text(encoding="utf-8"))
    validator = cast(
        EventTransmissionValidator,
        Draft202012Validator(schema, format_checker=FormatChecker()),
    )
    errors = sorted(
        validator.iter_errors(payload), key=lambda error: (error.json_path, error.message)
    )
    return tuple(_format_schema_error(error) for error in errors)


def _event_transmission_schema_path() -> Path:
    package_root = Path(__file__).resolve().parent
    installed_schema = package_root / "schemas" / "event-transmission.schema.json"
    if installed_schema.is_file():
        return installed_schema
    return package_root.parents[1] / "schemas" / "event-transmission.schema.json"


def _format_schema_error(error: ValidationError) -> str:
    return f"{error.json_path}: {error.message}"


def _read_source_artifact(path: Path, name: str) -> bytes:
    if path.is_symlink() or not path.is_file():
        raise ValueError(f"{name} must be a regular file")
    size_bytes = path.stat().st_size
    if size_bytes < 1 or size_bytes > 20 * 1024 * 1024:
        raise ValueError(f"{name} must contain between 1 byte and 20 MiB")
    return path.read_bytes()


def _compact_date(value: str) -> date:
    try:
        return datetime.strptime(value, "%Y%m%d").date()
    except ValueError as exc:
        raise argparse.ArgumentTypeError("dates must use valid YYYYMMDD values") from exc


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.command == "status":
        print(json.dumps(status_payload(), indent=2, sort_keys=True))
        return 0
    if args.command == "provider" and args.provider_command == "validate":
        try:
            result = validate_provider(args.path)
        except (KeyError, OSError, TypeError, ValueError, json.JSONDecodeError) as exc:
            print(json.dumps({"valid": False, "error": str(exc)}), file=sys.stderr)
            return 1
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0 if result["valid"] else 1
    if args.command == "event" and args.event_command == "validate":
        try:
            result = validate_event(args.path)
        except (KeyError, OSError, TypeError, ValueError, json.JSONDecodeError) as exc:
            print(json.dumps({"valid": False, "error": str(exc)}), file=sys.stderr)
            return 1
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0 if result["valid"] else 1
    if args.command == "prediction" and args.prediction_command == "capture":
        try:
            if args.provider == "polymarket":
                adapter: PredictionMarketAdapter = PolymarketPublicAdapter()
            elif args.provider == "kalshi":
                adapter = KalshiPublicAdapter()
            else:
                world_monitor_key = os.environ.get("WORLD_MONITOR_API_KEY", "")
                if not world_monitor_key:
                    raise ValueError("WORLD_MONITOR_API_KEY is not configured")
                adapter = WorldMonitorPredictionAdapter(world_monitor_key)
            bundle = capture_prediction_markets(
                adapter,
                limit=args.limit,
                query=args.query,
                output_root=args.output_root,
            )
        except (OSError, RuntimeError, TypeError, ValueError, json.JSONDecodeError) as exc:
            print(
                json.dumps({"captured": False, "error": str(exc)}),
                file=sys.stderr,
            )
            return 1
        print(
            json.dumps(
                {
                    "batch_id": bundle.batch_id,
                    "bundle_hash": bundle.bundle_hash,
                    "captured": True,
                    "data_available": bundle.data_available,
                    "evidence_ready_count": bundle.evidence_ready_count,
                    "observation_count": bundle.observation_count,
                    "path": bundle.path.as_posix(),
                    "provider_id": bundle.provider_id,
                    "provider_verified": False,
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0
    if args.command == "prediction" and args.prediction_command == "validate":
        try:
            bundle = validate_prediction_market_batch(args.path)
        except (OSError, TypeError, ValueError, json.JSONDecodeError) as exc:
            print(json.dumps({"valid": False, "error": str(exc)}), file=sys.stderr)
            return 1
        print(
            json.dumps(
                {
                    "batch_id": bundle.batch_id,
                    "bundle_hash": bundle.bundle_hash,
                    "data_available": bundle.data_available,
                    "evidence_ready_count": bundle.evidence_ready_count,
                    "observation_count": bundle.observation_count,
                    "path": bundle.path.as_posix(),
                    "provider_id": bundle.provider_id,
                    "valid": True,
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0
    if args.command == "tushare" and args.tushare_command == "capture":
        token = os.environ.get("TUSHARE_TOKEN", "")
        if not token:
            print(
                json.dumps({"captured": False, "error": "TUSHARE_TOKEN is not configured"}),
                file=sys.stderr,
            )
            return 1
        try:
            bundle = capture_tushare(
                token=token,
                tushare_code=args.instrument,
                as_of_date=args.as_of_date,
                start_date=args.start_date,
                end_date=args.end_date,
                data_start_date=args.data_start_date,
            )
        except (OSError, RuntimeError, TypeError, ValueError, json.JSONDecodeError) as exc:
            print(
                json.dumps({"captured": False, "error": str(exc)}),
                file=sys.stderr,
            )
            return 1
        print(
            json.dumps(
                {
                    "bundle_hash": bundle.bundle_hash,
                    "captured": True,
                    "data_snapshot_id": bundle.data_snapshot_id,
                    "instrument_id": bundle.instrument_id,
                    "listing_anomaly_count": bundle.listing_anomaly_count,
                    "path": bundle.path.as_posix(),
                    "provider_verified": False,
                    "universe_id": bundle.universe_id,
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0
    if args.command == "tushare" and args.tushare_command == "validate":
        try:
            bundle = validate_tushare_data_bundle(args.path)
        except (OSError, RuntimeError, TypeError, ValueError, json.JSONDecodeError) as exc:
            print(json.dumps({"valid": False, "error": str(exc)}), file=sys.stderr)
            return 1
        print(
            json.dumps(
                {
                    "bundle_hash": bundle.bundle_hash,
                    "data_snapshot_id": bundle.data_snapshot_id,
                    "instrument_id": bundle.instrument_id,
                    "listing_anomaly_count": bundle.listing_anomaly_count,
                    "path": bundle.path.as_posix(),
                    "universe_id": bundle.universe_id,
                    "valid": True,
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0
    if args.command == "agent" and args.agent_command == "validate":
        try:
            result = validate_agent_bundle(
                evidence_pack_path=args.evidence_pack,
                evidence_documents_path=args.evidence_documents,
                pattern_pack_paths=tuple(args.pattern_packs),
            )
        except (KeyError, OSError, TypeError, ValueError, json.JSONDecodeError) as exc:
            print(
                json.dumps({"valid": False, "error": f"{type(exc).__name__}: {exc}"}),
                file=sys.stderr,
            )
            return 1
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0 if result["valid"] else 1
    if args.command == "agent" and args.agent_command == "run":
        try:
            result = asyncio.run(
                run_agent_bundle(
                    evidence_pack_path=args.evidence_pack,
                    evidence_documents_path=args.evidence_documents,
                    pattern_pack_paths=tuple(args.pattern_packs),
                    run_id=args.run_id,
                    skill_root=args.skill_root,
                    state_root=args.state_root,
                )
            )
        except (
            KeyError,
            OSError,
            RuntimeError,
            TypeError,
            ValueError,
            json.JSONDecodeError,
        ) as exc:
            print(
                json.dumps({"completed": False, "error": f"{type(exc).__name__}: {exc}"}),
                file=sys.stderr,
            )
            return 1
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0 if result["status"] == RunStatus.COMPLETED.value else 1
    if args.command == "agent" and args.agent_command == "study-validate":
        try:
            result = validate_agent_phase2_study(
                registration_path=args.registration,
                exposure_registry_path=args.exposure_registry,
                source_coverage_registration_path=args.source_coverage_registration,
            )
        except (KeyError, OSError, TypeError, ValueError, json.JSONDecodeError) as exc:
            print(
                json.dumps({"valid": False, "error": f"{type(exc).__name__}: {exc}"}),
                file=sys.stderr,
            )
            return 1
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0 if result["valid"] else 1
    if args.command == "agent" and args.agent_command == "study-observe":
        try:
            result = observe_agent_phase2_study(
                registration_path=args.registration,
                exposure_registry_path=args.exposure_registry,
                source_coverage_registration_path=args.source_coverage_registration,
                coverage_receipt_path=args.coverage_receipt,
                observation_path=args.observation,
                raw_source_path=args.raw_source,
                regional_denominator_source_path=args.regional_denominator_source,
                ledger_path=args.ledger,
                recorded_at=datetime.now(UTC),
            )
        except (KeyError, OSError, TypeError, ValueError, json.JSONDecodeError) as exc:
            print(
                json.dumps({"recorded": False, "error": f"{type(exc).__name__}: {exc}"}),
                file=sys.stderr,
            )
            return 1
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0
    if args.command == "agent" and args.agent_command == "study-ledger-validate":
        try:
            result = validate_agent_phase2_ledger(
                registration_path=args.registration,
                exposure_registry_path=args.exposure_registry,
                source_coverage_registration_path=args.source_coverage_registration,
                ledger_path=args.ledger,
                inspected_at=datetime.now(UTC),
            )
        except (KeyError, OSError, TypeError, ValueError, json.JSONDecodeError) as exc:
            print(
                json.dumps({"valid": False, "error": f"{type(exc).__name__}: {exc}"}),
                file=sys.stderr,
            )
            return 1
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0
    if args.command == "agent" and args.agent_command == "study-source-poll":
        try:
            result = poll_agent_phase2_sources(
                registration_path=args.registration,
                exposure_registry_path=args.exposure_registry,
                source_coverage_registration_path=args.source_coverage_registration,
                ledger_path=args.ledger,
                monitor_root=args.monitor_root,
                started_at=datetime.now(UTC),
            )
        except (KeyError, OSError, TypeError, ValueError, json.JSONDecodeError) as exc:
            print(
                json.dumps({"polled": False, "error": f"{type(exc).__name__}: {exc}"}),
                file=sys.stderr,
            )
            return 1
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0 if result["coverage_complete"] else 1
    if args.command == "agent" and args.agent_command == "study-freeze-due":
        try:
            result = freeze_agent_phase2_due(
                registration_path=args.registration,
                exposure_registry_path=args.exposure_registry,
                source_coverage_registration_path=args.source_coverage_registration,
                ledger_path=args.ledger,
                pattern_pack_paths=tuple(args.pattern_pack),
                output_root=args.output_root,
                now=datetime.now(UTC),
            )
        except (KeyError, OSError, TypeError, ValueError, json.JSONDecodeError) as exc:
            print(
                json.dumps({"frozen": False, "error": f"{type(exc).__name__}: {exc}"}),
                file=sys.stderr,
            )
            return 1
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0
    if args.command == "backtest" and args.backtest_command == "run":
        try:
            request_payload = json.loads(args.request.read_text(encoding="utf-8"))
            request = backtest_request_from_dict(request_payload)
            from market_impact_agent.tushare_replay import run_validated_tushare_replay

            result = run_validated_tushare_replay(request, args.data_snapshot)
        except (
            ImportError,
            KeyError,
            ModuleNotFoundError,
            OSError,
            RuntimeError,
            TypeError,
            ValueError,
            json.JSONDecodeError,
        ) as exc:
            print(
                json.dumps({"completed": False, "error": f"{type(exc).__name__}: {exc}"}),
                file=sys.stderr,
            )
            return 1
        print(json.dumps(backtest_result_to_dict(result), indent=2, sort_keys=True))
        return 0 if result.status is BacktestRunStatus.COMPLETED else 1
    if args.command == "backtest" and args.backtest_command == "phase2-gate":
        try:
            evidence_payload = json.loads(args.evidence.read_text(encoding="utf-8"))
            if (
                isinstance(evidence_payload, dict)
                and cast(dict[str, object], evidence_payload).get("schema_version")
                == "market-impact.phase2-calibration-evidence.v2"
            ):
                from market_impact_agent.calibration_v2 import (
                    assess_phase2_calibration_v2,
                    load_phase2_calibration_evidence_v2,
                    phase2_calibration_gate_result_v2_to_dict,
                )

                v2_result = assess_phase2_calibration_v2(
                    load_phase2_calibration_evidence_v2(args.evidence)
                )
                print(
                    json.dumps(
                        phase2_calibration_gate_result_v2_to_dict(v2_result),
                        indent=2,
                        sort_keys=True,
                    )
                )
                return 0 if v2_result.accepted else 1
            evidence = load_phase2_calibration_evidence(args.evidence)
            gate_result = assess_phase2_calibration(evidence)
        except (
            KeyError,
            OSError,
            TypeError,
            ValueError,
            json.JSONDecodeError,
        ) as exc:
            print(
                json.dumps({"accepted": False, "error": f"{type(exc).__name__}: {exc}"}),
                file=sys.stderr,
            )
            return 1
        print(
            json.dumps(
                phase2_calibration_gate_result_to_dict(gate_result),
                indent=2,
                sort_keys=True,
            )
        )
        return 0 if gate_result.accepted else 1
    if args.command == "backtest" and args.backtest_command == "phase2-register":
        try:
            from market_impact_agent.phase2_study import build_phase2_registration

            registration = build_phase2_registration(
                cohort_path=args.cohort,
                data_snapshot_root=args.data_snapshot_root,
                output_path=args.output,
            )
        except (
            ImportError,
            KeyError,
            ModuleNotFoundError,
            OSError,
            RuntimeError,
            TypeError,
            ValueError,
            json.JSONDecodeError,
        ) as exc:
            print(
                json.dumps({"registered": False, "error": f"{type(exc).__name__}: {exc}"}),
                file=sys.stderr,
            )
            return 1
        print(
            json.dumps(
                {
                    "registered": True,
                    "registration_hash": registration.registration_hash,
                    "path": args.output.as_posix(),
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0
    if args.command == "backtest" and args.backtest_command == "phase2-run":
        try:
            from market_impact_agent.phase2_study import run_phase2_registration

            evidence_path = run_phase2_registration(
                registration_path=args.registration,
                data_snapshot_root=args.data_snapshot_root,
                output_dir=args.output_dir,
            )
        except (
            ImportError,
            KeyError,
            ModuleNotFoundError,
            OSError,
            RuntimeError,
            TypeError,
            ValueError,
            json.JSONDecodeError,
        ) as exc:
            print(
                json.dumps({"completed": False, "error": f"{type(exc).__name__}: {exc}"}),
                file=sys.stderr,
            )
            return 1
        print(
            json.dumps(
                {"completed": True, "evidence": evidence_path.as_posix()},
                indent=2,
                sort_keys=True,
            )
        )
        return 0
    raise AssertionError("unreachable command")
