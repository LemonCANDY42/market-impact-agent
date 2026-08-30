from __future__ import annotations

import argparse
import asyncio
import json
import os
import platform
import signal
import sys
import tempfile
import time
from collections.abc import Callable, Generator, Iterable, Mapping, Sequence
from contextlib import contextmanager
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from threading import Event, current_thread, main_thread
from types import FrameType
from typing import Protocol, cast
from urllib.parse import urlsplit
from urllib.request import Request, urlopen

from jsonschema import Draft202012Validator, FormatChecker, ValidationError

from market_impact_agent import __version__
from market_impact_agent.accrual import (
    AccrualDisposition,
    AccrualLedger,
    candidate_event_observation_from_dict,
)
from market_impact_agent.agent_contracts import (
    canonical_hash,
    evidence_pack_from_dict,
    pattern_pack_from_dict,
)
from market_impact_agent.agent_runtime import (
    ModelProvider,
    SkillRegistry,
    ToolAccessContext,
    ToolRegistry,
    ToolSideEffect,
)
from market_impact_agent.agent_schema import validate_agent_contract
from market_impact_agent.agent_study import load_agent_phase2_preregistration
from market_impact_agent.archive_authority import (
    COMMON_CRAWL_LOCATOR_SCHEMA,
    CommonCrawlArchiveAdapter,
    CommonCrawlIndexAdapter,
    VerifiedArchiveRecord,
    load_common_crawl_locator,
)
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
from market_impact_agent.csrc_news import (
    CsrcNewsProvider,
    UrllibCsrcNewsHTTPClient,
    load_csrc_news_capture_bundle,
    load_csrc_news_source,
)
from market_impact_agent.data_inputs import (
    DataInputHarness,
    DataPITLane,
    DataQuery,
    DataQueryMode,
    DataSnapshot,
    DataSourceBinding,
    LocalDataSnapshotStore,
)
from market_impact_agent.energy_monitor import EnergySourceMonitor
from market_impact_agent.events import event_transmission_chronology_errors
from market_impact_agent.evidence_freeze import freeze_due_evidence_packs
from market_impact_agent.frozen_research import FrozenResearchRepository
from market_impact_agent.internet_archive import (
    INTERNET_ARCHIVE_LOCATOR_SCHEMA,
    InternetArchiveAdapter,
    InternetArchiveIndexAdapter,
    VerifiedInternetArchiveRecord,
    load_internet_archive_locator,
)
from market_impact_agent.market_regimes import (
    capture_regime_panel,
    evaluate_regime_dataset,
    load_market_regime_dataset,
    validate_regime_panel,
    write_regime_panel,
    write_regime_report,
)
from market_impact_agent.method_benchmark import (
    load_historical_evidence_manifest,
    load_masked_agent_input_manifest,
    load_method_quality_benchmark,
    load_method_quality_evaluation_specification,
)
from market_impact_agent.method_skills import load_method_skill_catalog
from market_impact_agent.model_provider import (
    ModelProviderFactory,
    default_model_provider_profile_path,
    load_model_provider_profile,
)
from market_impact_agent.nbs_macro_release import (
    NBS_MACRO_RELEASE_REVISION_STRATEGY,
    NBS_MACRO_RELEASE_SEMANTIC_SCOPE,
    NbsMacroReleaseHTTPClient,
    NbsMacroReleaseProvider,
    UrllibNbsMacroReleaseHTTPClient,
    load_nbs_macro_release_capture_bundle,
    load_nbs_macro_release_source,
)
from market_impact_agent.observations import (
    ObservationCapability,
    ObservationProviderManifest,
    ValidatedObservationBundle,
    validate_prediction_market_batch,
    write_prediction_market_batch,
)
from market_impact_agent.official_archive import (
    extract_csrc_regime_evidence,
    extract_nbs_macro_vintage,
    extract_state_council_regime_evidence,
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
from market_impact_agent.prospective_checkpoint_readiness import (
    ProspectiveCheckpointAdmissionStore,
    evaluate_prospective_checkpoint_readiness,
    load_prospective_checkpoint_route_plan,
)
from market_impact_agent.prospective_collection_runtime import (
    ProspectiveCollectionAdapterKind,
    ProspectiveCollectionJob,
    ProspectiveCollectionRuntime,
    ScheduledCollector,
)
from market_impact_agent.prospective_collection_tracer import (
    qualify_prospective_collection_tracer,
    write_prospective_collection_tracer_report,
)
from market_impact_agent.prospective_collectors import collect_prospective_source_snapshot
from market_impact_agent.prospective_data import (
    ProspectiveCollectionPolicy,
    ProspectiveDataJournal,
    ProspectiveRollingWindow,
)
from market_impact_agent.prospective_diagnostic import (
    load_prospective_diagnostic_registration,
)
from market_impact_agent.prospective_operations import (
    assert_within_state_budget,
    collect_operations_metrics,
    create_state_backup,
    restore_state_backup,
    verify_state_backup,
)
from market_impact_agent.prospective_supervisor import (
    ProspectiveSupervisorPlan,
    assert_clean_supervisor_environment,
    load_supervisor_environment,
    render_launchd_plist,
)
from market_impact_agent.providers import MockExecutionProvider, ProviderManifest
from market_impact_agent.regime_archive_recovery import (
    audit_publisher_archive_recovery,
    load_qualification_report,
    recover_publisher_archive_snapshot,
    write_publisher_archive_recovery_report,
    write_publisher_archive_research_document,
)
from market_impact_agent.regime_evidence import (
    RegimeEvidenceManifest,
    load_regime_evidence_manifest,
    load_regime_evidence_record,
    qualify_regime_evidence,
    write_regime_evidence_manifest,
    write_regime_evidence_qualification_report,
    write_regime_evidence_record,
)
from market_impact_agent.regime_modeled_pit import (
    load_regime_modeled_pit_policy,
    qualify_regime_evidence_modeled_pit,
    write_regime_modeled_pit_qualification_report,
)
from market_impact_agent.regime_study import (
    assess_regime_study_readiness,
    evaluate_regime_study_baselines,
    load_regime_study_registration,
    write_regime_study_baseline_report,
)
from market_impact_agent.registry import ProviderRegistry
from market_impact_agent.research_methods import load_research_method_catalog
from market_impact_agent.runtime_store import ArtifactStore, RunJournal, RunStatus
from market_impact_agent.source_acceptance import (
    SourceRightsEvidence,
    SourceRouteAcceptanceDeclaration,
    SourceRouteReplayRequest,
    SourceRouteReplayResult,
    load_source_route_acceptance_report,
    qualify_source_route,
    write_source_route_acceptance_report,
)
from market_impact_agent.source_coverage import (
    coverage_receipt_from_dict,
    load_source_coverage_registration,
)
from market_impact_agent.syndication_feed import (
    SyndicationFeedProvider,
    SyndicationFeedSourceConfig,
    load_syndication_feed_source,
)
from market_impact_agent.tushare import TushareHttpAdapter
from market_impact_agent.tushare_bundle import (
    TushareDataRequest,
    ValidatedTushareDataBundle,
    capture_tushare_data_bundle,
    validate_tushare_data_bundle,
    write_tushare_data_bundle,
)
from market_impact_agent.tushare_observation import (
    TushareObservationProvider,
    TushareObservationTransport,
    load_tushare_observation_capture_bundle,
    load_tushare_observation_source,
)


class EventTransmissionValidator(Protocol):
    def iter_errors(self, instance: object) -> Iterable[ValidationError]: ...


class AvailableModelProvider(ModelProvider, Protocol):
    async def assert_model_available(self, *, timeout_seconds: float) -> None: ...


@contextmanager
def _collection_cancellation_signal() -> Generator[Callable[[], bool]]:
    requested = Event()
    if current_thread() is not main_thread():
        yield requested.is_set
        return

    def request_cancellation(_signum: int, _frame: FrameType | None) -> None:
        requested.set()

    signals = (signal.SIGINT, signal.SIGTERM)
    previous = {item: signal.getsignal(item) for item in signals}
    try:
        for item in signals:
            signal.signal(item, request_cancellation)
        yield requested.is_set
    finally:
        for item, handler in previous.items():
            signal.signal(item, handler)


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

    data_parser = subparsers.add_parser("data", help="Capture immutable read-only Data Snapshots")
    data_subparsers = data_parser.add_subparsers(dest="data_command", required=True)
    feed_capture_parser = data_subparsers.add_parser(
        "capture-feed",
        help="Capture prospective actual receipts from registered RSS or Atom sources",
    )
    feed_capture_parser.add_argument(
        "--source-config",
        required=True,
        action="append",
        type=Path,
        dest="source_configs",
    )
    feed_capture_parser.add_argument("--window-start", required=True, type=_aware_timestamp)
    feed_capture_parser.add_argument("--source-policy-id", required=True)
    feed_capture_parser.add_argument("--keyword", action="append", default=[])
    feed_capture_parser.add_argument("--max-items", type=int, default=50)
    feed_capture_parser.add_argument("--minimum-data-sources", type=int)
    feed_capture_parser.add_argument("--provider-timeout-seconds", type=float, default=30.0)
    feed_capture_parser.add_argument(
        "--state-root",
        type=Path,
        default=Path(".market-impact/data-inputs"),
    )
    feed_collect_parser = data_subparsers.add_parser(
        "collect-feed",
        help="Continuously journal prospective feed receipts and content revisions",
    )
    feed_collect_parser.add_argument(
        "--source-config",
        required=True,
        action="append",
        type=Path,
        dest="source_configs",
    )
    feed_collect_parser.add_argument("--window-start", required=True, type=_aware_timestamp)
    feed_collect_parser.add_argument("--keyword", action="append", default=[])
    feed_collect_parser.add_argument("--max-items", type=int, default=50)
    feed_collect_parser.add_argument("--minimum-data-sources", type=int)
    feed_collect_parser.add_argument("--provider-timeout-seconds", type=float, default=30.0)
    feed_collect_parser.add_argument("--poll-interval-seconds", type=int, default=300)
    feed_collect_parser.add_argument("--maximum-gap-seconds", type=int)
    feed_collect_parser.add_argument(
        "--cycles",
        type=int,
        default=1,
        help="Number of cycles; use 0 for continuous collection until interrupted",
    )
    feed_collect_parser.add_argument(
        "--state-root",
        type=Path,
        default=Path(".market-impact/data-inputs"),
    )
    feed_freeze_parser = data_subparsers.add_parser(
        "freeze-feed-dataset",
        help="Freeze journaled receipts into an Agent-readable snapshot and Parquet dataset",
    )
    feed_freeze_parser.add_argument("--policy-id", required=True)
    feed_freeze_parser.add_argument("--window-start", required=True, type=_aware_timestamp)
    feed_freeze_parser.add_argument("--not-after", required=True, type=_aware_timestamp)
    feed_freeze_parser.add_argument("--minimum-data-sources", type=int)
    feed_freeze_parser.add_argument(
        "--state-root",
        type=Path,
        default=Path(".market-impact/data-inputs"),
    )
    csrc_accept_parser = data_subparsers.add_parser(
        "accept-csrc-news",
        help="Run the seven-gate source acceptance trial for one CSRC news route",
    )
    csrc_accept_parser.add_argument("--source-config", required=True, type=Path)
    csrc_accept_parser.add_argument("--window-start", required=True, type=_aware_timestamp)
    csrc_accept_parser.add_argument("--keyword", action="append", default=[])
    csrc_accept_parser.add_argument("--max-items", type=int, default=50)
    csrc_accept_parser.add_argument("--provider-timeout-seconds", type=float, default=30.0)
    csrc_accept_parser.add_argument(
        "--state-root",
        type=Path,
        default=Path(".market-impact/data-inputs"),
    )
    nbs_accept_parser = data_subparsers.add_parser(
        "accept-nbs-macro-release",
        help="Capture, journal, persist, and replay the direct NBS CPI/PPI release route",
    )
    nbs_accept_parser.add_argument("--source-config", required=True, type=Path)
    nbs_accept_parser.add_argument("--window-start", required=True, type=_aware_timestamp)
    nbs_accept_parser.add_argument(
        "--indicator",
        action="append",
        choices=("cpi", "ppi"),
        default=[],
    )
    nbs_accept_parser.add_argument("--poll-interval-seconds", type=int, default=3600)
    nbs_accept_parser.add_argument("--maximum-gap-seconds", type=int, default=90000)
    nbs_accept_parser.add_argument("--provider-timeout-seconds", type=float, default=30.0)
    nbs_accept_parser.add_argument(
        "--state-root",
        type=Path,
        default=Path(".market-impact/data-inputs"),
    )
    prospective_diagnostic_parser = data_subparsers.add_parser(
        "validate-prospective-diagnostic",
        help="Validate the frozen PDI-01 prospective checkpoint registration",
    )
    prospective_diagnostic_parser.add_argument("--registration", required=True, type=Path)
    checkpoint_admit_parser = data_subparsers.add_parser(
        "checkpoint-route-admit",
        help="Durably admit a no-authority checkpoint route plan at the Harness clock",
    )
    checkpoint_admit_parser.add_argument("--registration", required=True, type=Path)
    checkpoint_admit_parser.add_argument("--route-plan", required=True, type=Path)
    checkpoint_admit_parser.add_argument(
        "--state-root",
        type=Path,
        default=Path(".market-impact/data-inputs"),
    )
    checkpoint_readiness_parser = data_subparsers.add_parser(
        "checkpoint-readiness",
        help=("Audit pre-bound prospective checkpoint trigger routes without starting a model"),
    )
    checkpoint_readiness_parser.add_argument("--registration", required=True, type=Path)
    checkpoint_readiness_parser.add_argument("--route-plan", required=True, type=Path)
    checkpoint_readiness_parser.add_argument("--evaluated-at", type=_aware_timestamp)
    checkpoint_readiness_parser.add_argument(
        "--state-root",
        type=Path,
        default=Path(".market-impact/data-inputs"),
    )
    tushare_observation_accept_parser = data_subparsers.add_parser(
        "accept-tushare-observation",
        help="Capture, journal, persist, and replay one prospective Tushare route",
    )
    tushare_observation_accept_parser.add_argument("--source-config", required=True, type=Path)
    tushare_observation_accept_parser.add_argument("--parameters-json", required=True)
    tushare_observation_accept_parser.add_argument(
        "--window-start", required=True, type=_aware_timestamp
    )
    tushare_observation_accept_parser.add_argument(
        "--poll-interval-seconds", required=True, type=int
    )
    tushare_observation_accept_parser.add_argument("--maximum-gap-seconds", required=True, type=int)
    tushare_observation_accept_parser.add_argument(
        "--provider-timeout-seconds", type=float, default=30.0
    )
    tushare_observation_accept_parser.add_argument(
        "--state-root",
        type=Path,
        default=Path(".market-impact/data-inputs"),
    )
    collection_register_parser = data_subparsers.add_parser(
        "collection-register",
        help="Register one accepted route for Harness-owned prospective scheduling",
    )
    collection_register_parser.add_argument(
        "--adapter-kind",
        required=True,
        choices=tuple(item.value for item in ProspectiveCollectionAdapterKind),
    )
    collection_register_parser.add_argument("--source-config", required=True, type=Path)
    collection_register_parser.add_argument("--acceptance-report", required=True, type=Path)
    collection_register_parser.add_argument("--parameters-json", required=True)
    collection_register_parser.add_argument(
        "--rolling-lookback-seconds",
        type=int,
        help=(
            "Resolve start_date/end_date from each logical due time using an overlapping "
            "rolling window"
        ),
    )
    collection_register_parser.add_argument(
        "--rolling-window-timezone",
        default="Asia/Shanghai",
    )
    collection_register_parser.add_argument("--window-start", required=True, type=_aware_timestamp)
    collection_register_parser.add_argument("--starts-at", required=True, type=_aware_timestamp)
    collection_register_parser.add_argument("--poll-interval-seconds", required=True, type=int)
    collection_register_parser.add_argument("--maximum-gap-seconds", required=True, type=int)
    collection_register_parser.add_argument("--misfire-grace-seconds", required=True, type=int)
    collection_register_parser.add_argument("--maximum-jitter-seconds", type=int, default=0)
    collection_register_parser.add_argument("--provider-timeout-seconds", type=float, default=30.0)
    collection_register_parser.add_argument(
        "--state-root",
        type=Path,
        default=Path(".market-impact/data-inputs"),
    )
    collection_run_parser = data_subparsers.add_parser(
        "collection-run-due",
        help="Run each currently due prospective collection job once",
    )
    collection_run_parser.add_argument("--job-id", action="append", default=[])
    collection_run_parser.add_argument("--limit", type=int, default=100)
    collection_run_parser.add_argument("--maximum-state-bytes", required=True, type=int)
    collection_run_parser.add_argument(
        "--now",
        type=_aware_timestamp,
        help="Override logical due and scheduling time, not actual completion time",
    )
    collection_run_parser.add_argument(
        "--state-root",
        type=Path,
        default=Path(".market-impact/data-inputs"),
    )
    collection_health_parser = data_subparsers.add_parser(
        "collection-health",
        help="Read durable prospective collection health without contacting a Provider",
    )
    collection_health_parser.add_argument("--job-id", action="append", default=[])
    collection_health_parser.add_argument("--limit", type=int, default=100)
    collection_health_parser.add_argument("--now", type=_aware_timestamp)
    collection_health_parser.add_argument(
        "--state-root",
        type=Path,
        default=Path(".market-impact/data-inputs"),
    )
    collection_tracer_parser = data_subparsers.add_parser(
        "collection-qualify-tracer",
        help="Qualify one bounded CSRC-plus-market prospective collection tracer",
    )
    collection_tracer_parser.add_argument("--job-id", action="append", required=True)
    collection_tracer_parser.add_argument("--evaluated-at", type=_aware_timestamp)
    collection_tracer_parser.add_argument(
        "--state-root",
        type=Path,
        default=Path(".market-impact/data-inputs"),
    )
    collection_service_parser = data_subparsers.add_parser(
        "collection-service-run",
        help="Run due collection Jobs with secrets loaded from a private service boundary",
    )
    collection_service_parser.add_argument("--environment-file", required=True, type=Path)
    collection_service_parser.add_argument("--job-id", action="append", default=[])
    collection_service_parser.add_argument("--limit", type=int, default=100)
    collection_service_parser.add_argument("--maximum-state-bytes", required=True, type=int)
    collection_service_parser.add_argument("--require-clean-environment", action="store_true")
    collection_service_parser.add_argument("--now", type=_aware_timestamp)
    collection_service_parser.add_argument(
        "--state-root",
        type=Path,
        default=Path(".market-impact/data-inputs"),
    )
    supervisor_plan_parser = data_subparsers.add_parser(
        "collection-supervisor-plan",
        help="Render a secret-free, disabled launchd pre-install plan",
    )
    supervisor_plan_parser.add_argument("--host-name", required=True)
    supervisor_plan_parser.add_argument("--host-uid", required=True, type=int)
    supervisor_plan_parser.add_argument(
        "--launchd-label",
        default="com.lemoncandy42.market-impact-agent.collection",
    )
    supervisor_plan_parser.add_argument("--service-definition-path", required=True, type=Path)
    supervisor_plan_parser.add_argument("--executable-path", required=True, type=Path)
    supervisor_plan_parser.add_argument("--working-directory", required=True, type=Path)
    supervisor_plan_parser.add_argument("--state-root", required=True, type=Path)
    supervisor_plan_parser.add_argument("--environment-file", required=True, type=Path)
    supervisor_plan_parser.add_argument("--stdout-path", required=True, type=Path)
    supervisor_plan_parser.add_argument("--stderr-path", required=True, type=Path)
    supervisor_plan_parser.add_argument("--invocation-interval-seconds", type=int, default=60)
    supervisor_plan_parser.add_argument("--maximum-state-bytes", required=True, type=int)
    supervisor_plan_parser.add_argument(
        "--notification-policy",
        choices=("health_log_only", "failed_runs_only"),
        default="health_log_only",
    )
    state_backup_parser = data_subparsers.add_parser(
        "state-backup",
        help="Create a verified content-identified prospective state backup",
    )
    state_backup_parser.add_argument("--state-root", required=True, type=Path)
    state_backup_parser.add_argument("--backup-parent", required=True, type=Path)
    state_verify_parser = data_subparsers.add_parser(
        "state-verify-backup",
        help="Verify a prospective state backup without restoring it",
    )
    state_verify_parser.add_argument("--backup", required=True, type=Path)
    state_restore_parser = data_subparsers.add_parser(
        "state-restore",
        help="Restore a verified prospective state backup into a new destination",
    )
    state_restore_parser.add_argument("--backup", required=True, type=Path)
    state_restore_parser.add_argument("--destination", required=True, type=Path)

    event_parser = subparsers.add_parser(
        "event", help="Validate point-in-time event transmission records"
    )
    event_subparsers = event_parser.add_subparsers(dest="event_command", required=True)
    event_validate_parser = event_subparsers.add_parser(
        "validate", help="Validate a point-in-time event assessment"
    )
    event_validate_parser.add_argument("path", type=Path)

    archive_parser = subparsers.add_parser(
        "archive", help="Verify immutable historical archive captures"
    )
    archive_subparsers = archive_parser.add_subparsers(
        dest="archive_command",
        required=True,
    )
    common_crawl_parser = archive_subparsers.add_parser(
        "common-crawl-verify",
        help="Range-fetch and verify one fixed Common Crawl WARC record",
    )
    common_crawl_parser.add_argument("--locator", required=True, type=Path)
    common_crawl_locate_parser = archive_subparsers.add_parser(
        "common-crawl-locate",
        help="Locate the latest exact Common Crawl capture before a fixed cutoff",
    )
    common_crawl_locate_parser.add_argument("--collection", required=True)
    common_crawl_locate_parser.add_argument("--url", required=True)
    common_crawl_locate_parser.add_argument("--not-after", required=True, type=_aware_timestamp)
    internet_archive_parser = archive_subparsers.add_parser(
        "internet-archive-verify",
        help="Fetch and digest-verify one fixed Internet Archive replay",
    )
    internet_archive_parser.add_argument("--locator", required=True, type=Path)
    internet_archive_locate_parser = archive_subparsers.add_parser(
        "internet-archive-locate",
        help="Locate the latest exact Internet Archive capture before a fixed cutoff",
    )
    internet_archive_locate_parser.add_argument("--url", required=True)
    internet_archive_locate_parser.add_argument("--not-after", required=True, type=_aware_timestamp)

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

    regime_parser = subparsers.add_parser(
        "regime", help="Build research-only market-state and sector context diagnostics"
    )
    regime_subparsers = regime_parser.add_subparsers(dest="regime_command", required=True)
    regime_validate_parser = regime_subparsers.add_parser(
        "validate", help="Validate one public retrospective case registry"
    )
    regime_validate_parser.add_argument("--dataset", required=True, type=Path)
    regime_capture_parser = regime_subparsers.add_parser(
        "capture", help="Capture a private Tushare index and industry panel"
    )
    regime_capture_parser.add_argument("--dataset", required=True, type=Path)
    regime_evaluate_parser = regime_subparsers.add_parser(
        "evaluate", help="Evaluate three frozen return windows without Agent access"
    )
    regime_evaluate_parser.add_argument("--dataset", required=True, type=Path)
    regime_evaluate_parser.add_argument("--panel", required=True, type=Path)
    regime_study_validate_parser = regime_subparsers.add_parser(
        "study-validate", help="Validate rich-source and long-horizon study registration"
    )
    regime_study_validate_parser.add_argument("--dataset", required=True, type=Path)
    regime_study_validate_parser.add_argument("--method-catalog", required=True, type=Path)
    regime_study_validate_parser.add_argument("--registration", required=True, type=Path)
    regime_study_evaluate_parser = regime_subparsers.add_parser(
        "study-evaluate", help="Evaluate frozen long-horizon market and rotation baselines"
    )
    regime_study_evaluate_parser.add_argument("--dataset", required=True, type=Path)
    regime_study_evaluate_parser.add_argument("--method-catalog", required=True, type=Path)
    regime_study_evaluate_parser.add_argument("--registration", required=True, type=Path)
    regime_study_evaluate_parser.add_argument("--panel", required=True, type=Path)
    regime_evidence_capture_parser = regime_subparsers.add_parser(
        "evidence-capture-csrc",
        help="Verify one archived CSRC page and store its public evidence metadata",
    )
    regime_evidence_capture_parser.add_argument("--locator", required=True, type=Path)
    regime_evidence_capture_parser.add_argument("--case-key", required=True, action="append")
    regime_evidence_capture_parser.add_argument("--claim-id", required=True)
    regime_evidence_capture_parser.add_argument("--lineage-id", required=True)
    regime_state_council_capture_parser = regime_subparsers.add_parser(
        "evidence-capture-state-council",
        help="Verify one archived State Council page and store its public evidence metadata",
    )
    regime_state_council_capture_parser.add_argument("--locator", required=True, type=Path)
    regime_state_council_capture_parser.add_argument("--case-key", required=True, action="append")
    regime_state_council_capture_parser.add_argument("--claim-id", required=True)
    regime_state_council_capture_parser.add_argument("--lineage-id", required=True)
    regime_nbs_capture_parser = regime_subparsers.add_parser(
        "evidence-capture-nbs",
        help="Verify one archived NBS release and store its public macro-vintage metadata",
    )
    regime_nbs_capture_parser.add_argument("--locator", required=True, type=Path)
    regime_nbs_capture_parser.add_argument("--case-key", required=True, action="append")
    regime_nbs_capture_parser.add_argument("--claim-id", required=True)
    regime_nbs_capture_parser.add_argument("--lineage-id", required=True)
    regime_evidence_manifest_parser = regime_subparsers.add_parser(
        "evidence-manifest",
        help="Bind evidence records to one frozen dataset, registration, and market panel",
    )
    regime_evidence_manifest_parser.add_argument("--dataset", required=True, type=Path)
    regime_evidence_manifest_parser.add_argument("--method-catalog", required=True, type=Path)
    regime_evidence_manifest_parser.add_argument("--registration", required=True, type=Path)
    regime_evidence_manifest_parser.add_argument("--panel", required=True, type=Path)
    regime_evidence_manifest_parser.add_argument(
        "--record", required=True, action="append", type=Path
    )
    regime_evidence_qualify_parser = regime_subparsers.add_parser(
        "evidence-qualify",
        help="Evaluate every frozen checkpoint against the registered source minima",
    )
    regime_evidence_qualify_parser.add_argument("--dataset", required=True, type=Path)
    regime_evidence_qualify_parser.add_argument("--method-catalog", required=True, type=Path)
    regime_evidence_qualify_parser.add_argument("--registration", required=True, type=Path)
    regime_evidence_qualify_parser.add_argument("--panel", required=True, type=Path)
    regime_evidence_qualify_parser.add_argument("--manifest", required=True, type=Path)
    regime_modeled_qualify_parser = regime_subparsers.add_parser(
        "evidence-qualify-modeled",
        help="Evaluate exploratory modeled-PIT visibility without promoting strict authority",
    )
    regime_modeled_qualify_parser.add_argument("--dataset", required=True, type=Path)
    regime_modeled_qualify_parser.add_argument("--method-catalog", required=True, type=Path)
    regime_modeled_qualify_parser.add_argument("--registration", required=True, type=Path)
    regime_modeled_qualify_parser.add_argument("--panel", required=True, type=Path)
    regime_modeled_qualify_parser.add_argument("--manifest", required=True, type=Path)
    regime_modeled_qualify_parser.add_argument("--strict-qualification", required=True, type=Path)
    regime_modeled_qualify_parser.add_argument("--policy", required=True, type=Path)
    regime_archive_audit_parser = regime_subparsers.add_parser(
        "evidence-audit-publisher-archives",
        help="Locate exact pre-cutoff publisher captures without promoting their content",
    )
    regime_archive_audit_parser.add_argument("--dataset", required=True, type=Path)
    regime_archive_audit_parser.add_argument("--method-catalog", required=True, type=Path)
    regime_archive_audit_parser.add_argument("--registration", required=True, type=Path)
    regime_archive_audit_parser.add_argument("--panel", required=True, type=Path)
    regime_archive_audit_parser.add_argument("--manifest", required=True, type=Path)
    regime_archive_audit_parser.add_argument("--qualification", required=True, type=Path)
    regime_archive_audit_parser.add_argument("--case-key", action="append")
    regime_archive_audit_parser.add_argument("--max-lookups", type=int, default=500)
    regime_archive_capture_parser = regime_subparsers.add_parser(
        "evidence-capture-publisher-archive",
        help="Verify one exact publisher archive and write a PIT evidence record",
    )
    regime_archive_capture_parser.add_argument("--record", required=True, type=Path)
    regime_archive_capture_parser.add_argument("--locator", required=True, type=Path)
    regime_archive_capture_parser.add_argument("--not-after", required=True, type=_aware_timestamp)
    regime_archive_capture_parser.add_argument(
        "--supersedes-record",
        type=Path,
        help="Link a later archived publisher version to a verified prior record",
    )

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
        "run", help="Run one local model judgment against a frozen Evidence Pack"
    )
    _add_agent_bundle_arguments(agent_run_parser)
    agent_run_parser.add_argument("--run-id", required=True)
    agent_run_parser.add_argument(
        "--provider-profile",
        type=Path,
        default=default_model_provider_profile_path(),
    )
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
    agent_ensemble_parser = agent_subparsers.add_parser(
        "study-run-ensemble",
        help="Run five independent frozen Agent replicates and aggregate three-of-five",
    )
    agent_ensemble_parser.add_argument("--registration", required=True, type=Path)
    agent_ensemble_parser.add_argument("--exposure-registry", required=True, type=Path)
    _add_agent_bundle_arguments(agent_ensemble_parser)
    agent_ensemble_parser.add_argument("--ensemble-run-id", required=True)
    agent_ensemble_parser.add_argument(
        "--skill-root",
        type=Path,
        default=_default_agent_skill_root(),
    )
    agent_ensemble_parser.add_argument(
        "--state-root",
        type=Path,
        default=Path(".market-impact/agent-ensemble-runs"),
    )
    agent_ensemble_parser.add_argument(
        "--ensemble-state-root",
        type=Path,
        default=Path(".market-impact/agent-ensemble-decisions"),
    )
    method_ablation_parser = agent_subparsers.add_parser(
        "method-ablation-run",
        help="Run the frozen four-arm research-method ablation without broker reachability",
    )
    method_ablation_parser.add_argument("--ablation-registration", required=True, type=Path)
    method_ablation_parser.add_argument("--parent-registration", required=True, type=Path)
    method_ablation_parser.add_argument("--exposure-registry", required=True, type=Path)
    method_ablation_parser.add_argument("--method-catalog", required=True, type=Path)
    method_ablation_parser.add_argument("--provider-profile", required=True, type=Path)
    _add_agent_bundle_arguments(method_ablation_parser)
    method_ablation_parser.add_argument("--experiment-id", required=True)
    method_ablation_parser.add_argument(
        "--skill-root",
        type=Path,
        default=_default_agent_skill_root(),
    )
    method_ablation_parser.add_argument(
        "--state-root",
        type=Path,
        default=Path(".market-impact/method-ablation-runs"),
    )
    method_skill_ablation_parser = agent_subparsers.add_parser(
        "method-skill-ablation-run",
        help="Run a three-pair method Skill diagnostic with CPA cost preflight",
    )
    method_skill_ablation_parser.add_argument("--method-catalog", required=True, type=Path)
    method_skill_ablation_parser.add_argument(
        "--method-evidence-declaration", required=True, type=Path
    )
    method_skill_ablation_parser.add_argument("--provider-profile", required=True, type=Path)
    _add_agent_bundle_arguments(method_skill_ablation_parser)
    method_skill_ablation_parser.add_argument("--experiment-id", required=True)
    method_skill_ablation_parser.add_argument("--treatment-skill", required=True)
    method_skill_ablation_parser.add_argument(
        "--market-state",
        required=True,
        choices=("up_fast", "up_mild", "down_fast", "down_mild", "unclassified"),
    )
    method_skill_ablation_parser.add_argument(
        "--narrative-salience",
        required=True,
        choices=(
            "corroborated_obvious",
            "authority_obvious",
            "diffuse",
            "contested",
            "unavailable",
        ),
    )
    method_skill_ablation_parser.add_argument(
        "--analysis-need",
        required=True,
        action="append",
        dest="analysis_needs",
    )
    method_skill_ablation_parser.add_argument(
        "--eligible-horizon-sessions",
        required=True,
        type=int,
        help="Exact registered forward horizon for the only eligible candidate",
    )
    method_skill_ablation_parser.add_argument("--outcomes-opened", action="store_true")
    method_skill_ablation_parser.add_argument(
        "--max-total-cost-usd",
        type=Decimal,
        default=Decimal("10"),
    )
    method_skill_ablation_parser.add_argument(
        "--skill-root",
        type=Path,
        default=_default_agent_skill_root(),
    )
    method_skill_ablation_parser.add_argument(
        "--state-root",
        type=Path,
        default=Path(".market-impact/method-skill-ablation-runs"),
    )
    method_development_parser = agent_subparsers.add_parser(
        "method-development-run",
        help="Run one identity-masked opened development state without inferential claims",
    )
    method_development_parser.add_argument("--case", required=True, type=Path)
    method_development_parser.add_argument("--benchmark-registration", required=True, type=Path)
    method_development_parser.add_argument("--evaluation-specification", required=True, type=Path)
    method_development_parser.add_argument("--method-catalog", required=True, type=Path)
    method_development_parser.add_argument("--provider-profile", required=True, type=Path)
    method_development_parser.add_argument("--state", required=True, choices=("attack", "recovery"))
    _add_agent_bundle_arguments(method_development_parser)
    method_development_parser.add_argument("--backtest-request", required=True, type=Path)
    method_development_parser.add_argument("--experiment-id", required=True)
    method_development_parser.add_argument(
        "--skill-root",
        type=Path,
        default=_default_agent_skill_root(),
    )
    method_development_parser.add_argument(
        "--state-root",
        type=Path,
        default=Path(".market-impact/method-development-runs"),
    )
    method_development_evaluate_parser = agent_subparsers.add_parser(
        "method-development-evaluate",
        help="Open deterministic outcomes for one completed opened development case",
    )
    method_development_evaluate_parser.add_argument("--case", required=True, type=Path)
    method_development_evaluate_parser.add_argument("--attack-report", required=True, type=Path)
    method_development_evaluate_parser.add_argument("--recovery-report", required=True, type=Path)
    method_development_evaluate_parser.add_argument(
        "--attack-backtest-request", required=True, type=Path
    )
    method_development_evaluate_parser.add_argument(
        "--recovery-backtest-request", required=True, type=Path
    )
    method_development_evaluate_parser.add_argument(
        "--attack-data-snapshot", required=True, type=Path
    )
    method_development_evaluate_parser.add_argument(
        "--recovery-data-snapshot", required=True, type=Path
    )
    method_development_evaluate_parser.add_argument("--evaluation-id", required=True)
    method_development_evaluate_parser.add_argument(
        "--state-root",
        type=Path,
        default=Path(".market-impact/method-development-evaluations"),
    )
    method_benchmark_parser = agent_subparsers.add_parser(
        "method-benchmark-validate",
        help="Validate the frozen method-quality protocol and one point-in-time case",
    )
    method_benchmark_parser.add_argument("--registration", required=True, type=Path)
    method_benchmark_parser.add_argument("--method-catalog", required=True, type=Path)
    method_benchmark_parser.add_argument("--provider-profile", required=True, type=Path)
    method_benchmark_parser.add_argument("--evaluation-specification", required=True, type=Path)
    method_benchmark_parser.add_argument("--historical-manifest", required=True, type=Path)
    method_benchmark_parser.add_argument("--evidence-pack", required=True, type=Path)
    method_benchmark_parser.add_argument("--evidence-documents", required=True, type=Path)
    method_benchmark_parser.add_argument("--masked-input-manifest", required=True, type=Path)
    method_benchmark_parser.add_argument("--masked-evidence-pack", required=True, type=Path)
    method_benchmark_parser.add_argument("--masked-evidence-documents", required=True, type=Path)
    method_benchmark_parser.add_argument(
        "--pattern-pack", required=True, action="append", type=Path, dest="pattern_packs"
    )
    method_benchmark_parser.add_argument(
        "--masked-pattern-pack",
        required=True,
        action="append",
        type=Path,
        dest="masked_pattern_packs",
    )
    method_benchmark_parser.add_argument(
        "--skill-root",
        type=Path,
        default=_default_agent_skill_root(),
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


def validate_prospective_diagnostic(path: Path) -> dict[str, object]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    schema_errors = validate_agent_contract(
        payload,
        "prospective-diagnostic-registration.schema.json",
    )
    if schema_errors:
        return {
            "path": path.as_posix(),
            "valid": False,
            "errors": list(schema_errors),
        }
    registration = load_prospective_diagnostic_registration(path)
    return {
        "path": path.as_posix(),
        "valid": True,
        "errors": [],
        "registration_id": registration.registration_id,
        "registered_at": _utc_timestamp(registration.registered_at),
        "checkpoint_keys": [item.checkpoint_key for item in registration.checkpoints],
        "mechanisms": [item.mechanism.value for item in registration.checkpoints],
        "replicates_per_arm": registration.replicates_per_arm,
        "aggregate_model_cost_limit_usd": registration.aggregate_model_cost_limit_usd,
        "historical_pit_claim": False,
        "model_calls_authorized": False,
        "execution_capability": False,
    }


def prospective_checkpoint_readiness(
    *,
    registration_path: Path,
    route_plan_path: Path,
    state_root: Path,
    evaluated_at: datetime | None,
) -> dict[str, object]:
    registration = load_prospective_diagnostic_registration(registration_path)
    route_plan = load_prospective_checkpoint_route_plan(route_plan_path)
    store = LocalDataSnapshotStore(state_root)
    runtime = ProspectiveCollectionRuntime(store)
    admission_store = ProspectiveCheckpointAdmissionStore(state_root)
    report = evaluate_prospective_checkpoint_readiness(
        registration=registration,
        route_plan=route_plan,
        admission_store=admission_store,
        runtime=runtime,
        evaluated_at=datetime.now(UTC) if evaluated_at is None else evaluated_at,
    )
    artifact = store.artifacts.put_json(report.to_dict())
    return {
        **report.to_dict(),
        "artifact_hash": artifact.content_hash,
        "artifact_media_type": artifact.media_type,
    }


def admit_prospective_checkpoint_routes(
    *,
    registration_path: Path,
    route_plan_path: Path,
    state_root: Path,
) -> dict[str, object]:
    registration = load_prospective_diagnostic_registration(registration_path)
    route_plan = load_prospective_checkpoint_route_plan(route_plan_path)
    admission = ProspectiveCheckpointAdmissionStore(state_root).admit(
        route_plan=route_plan,
        registration=registration,
    )
    return admission.to_dict()


def verify_common_crawl_archive(
    locator_path: Path,
    *,
    adapter: CommonCrawlArchiveAdapter | None = None,
) -> dict[str, object]:
    payload = json.loads(locator_path.read_text(encoding="utf-8"))
    errors = validate_agent_contract(payload, "common-crawl-locator.schema.json")
    if errors:
        raise ValueError("; ".join(errors))
    locator = load_common_crawl_locator(locator_path)
    record = (CommonCrawlArchiveAdapter() if adapter is None else adapter).fetch(locator)
    return record.to_dict()


def locate_common_crawl_archive(
    *,
    collection: str,
    target_url: str,
    not_after: datetime,
    adapter: CommonCrawlIndexAdapter | None = None,
) -> dict[str, object] | None:
    locator = (CommonCrawlIndexAdapter() if adapter is None else adapter).locate_latest(
        collection=collection,
        target_url=target_url,
        not_after=not_after,
    )
    return None if locator is None else locator.to_dict()


def verify_internet_archive(
    locator_path: Path,
    *,
    adapter: InternetArchiveAdapter | None = None,
) -> dict[str, object]:
    payload = json.loads(locator_path.read_text(encoding="utf-8"))
    errors = validate_agent_contract(payload, "internet-archive-locator.schema.json")
    if errors:
        raise ValueError("; ".join(errors))
    locator = load_internet_archive_locator(locator_path)
    record = (InternetArchiveAdapter() if adapter is None else adapter).fetch(locator)
    return record.to_dict()


def locate_internet_archive(
    *,
    target_url: str,
    not_after: datetime,
    adapter: InternetArchiveIndexAdapter | None = None,
) -> dict[str, object] | None:
    locator = (InternetArchiveIndexAdapter() if adapter is None else adapter).locate_latest(
        target_url=target_url,
        not_after=not_after,
    )
    return None if locator is None else locator.to_dict()


def _fetch_verified_archive(
    locator_path: Path,
    *,
    common_crawl_adapter: CommonCrawlArchiveAdapter | None = None,
    internet_archive_adapter: InternetArchiveAdapter | None = None,
) -> VerifiedArchiveRecord | VerifiedInternetArchiveRecord:
    payload = json.loads(locator_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise TypeError("archive locator must be an object")
    typed_payload = cast(dict[str, object], payload)
    schema_version = typed_payload.get("schema_version")
    if schema_version == COMMON_CRAWL_LOCATOR_SCHEMA:
        locator = load_common_crawl_locator(locator_path)
        adapter = (
            CommonCrawlArchiveAdapter() if common_crawl_adapter is None else common_crawl_adapter
        )
        return adapter.fetch(locator)
    if schema_version == INTERNET_ARCHIVE_LOCATOR_SCHEMA:
        locator = load_internet_archive_locator(locator_path)
        adapter = (
            InternetArchiveAdapter()
            if internet_archive_adapter is None
            else internet_archive_adapter
        )
        return adapter.fetch(locator)
    raise ValueError("unsupported archive locator schema version")


def capture_csrc_regime_evidence(
    *,
    locator_path: Path,
    case_keys: tuple[str, ...],
    claim_id: str,
    lineage_id: str,
    archive_adapter: CommonCrawlArchiveAdapter | None = None,
    internet_archive_adapter: InternetArchiveAdapter | None = None,
) -> tuple[Path, dict[str, object]]:
    archive_record = _fetch_verified_archive(
        locator_path,
        common_crawl_adapter=archive_adapter,
        internet_archive_adapter=internet_archive_adapter,
    )
    evidence = extract_csrc_regime_evidence(
        archive_record,
        case_keys=case_keys,
        claim_id=claim_id,
        lineage_id=lineage_id,
    )
    return write_regime_evidence_record(evidence), evidence.to_dict()


def capture_state_council_regime_evidence(
    *,
    locator_path: Path,
    case_keys: tuple[str, ...],
    claim_id: str,
    lineage_id: str,
    archive_adapter: CommonCrawlArchiveAdapter | None = None,
    internet_archive_adapter: InternetArchiveAdapter | None = None,
) -> tuple[Path, dict[str, object]]:
    archive_record = _fetch_verified_archive(
        locator_path,
        common_crawl_adapter=archive_adapter,
        internet_archive_adapter=internet_archive_adapter,
    )
    evidence = extract_state_council_regime_evidence(
        archive_record,
        case_keys=case_keys,
        claim_id=claim_id,
        lineage_id=lineage_id,
    )
    return write_regime_evidence_record(evidence), evidence.to_dict()


def capture_nbs_macro_vintage(
    *,
    locator_path: Path,
    case_keys: tuple[str, ...],
    claim_id: str,
    lineage_id: str,
    archive_adapter: CommonCrawlArchiveAdapter | None = None,
    internet_archive_adapter: InternetArchiveAdapter | None = None,
) -> tuple[Path, dict[str, object]]:
    archive_record = _fetch_verified_archive(
        locator_path,
        common_crawl_adapter=archive_adapter,
        internet_archive_adapter=internet_archive_adapter,
    )
    evidence = extract_nbs_macro_vintage(
        archive_record,
        case_keys=case_keys,
        claim_id=claim_id,
        lineage_id=lineage_id,
    )
    return write_regime_evidence_record(evidence), evidence.to_dict()


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


def capture_syndication_data_snapshot(
    *,
    source_config_paths: tuple[Path, ...],
    window_start: datetime,
    source_policy_id: str,
    keywords: tuple[str, ...],
    max_items: int,
    minimum_data_sources: int | None,
    state_root: Path,
    provider_timeout_seconds: float,
) -> DataSnapshot:
    configs = tuple(load_syndication_feed_source(path) for path in source_config_paths)
    provider = SyndicationFeedProvider(configs)
    captures = asyncio.run(provider.collect())
    as_of = max(item.retrieved_at for item in captures)
    replay_provider = provider.replay(captures)
    store = LocalDataSnapshotStore(state_root)
    harness = DataInputHarness(
        store,
        provider_timeout_seconds=provider_timeout_seconds,
    )
    harness.register(replay_provider)
    sources = _syndication_source_bindings(replay_provider.manifest, configs)
    minimum = len(sources) if minimum_data_sources is None else minimum_data_sources
    query = DataQuery.build(
        capability=ObservationCapability.EVENT_REVELATION,
        pit_lane=DataPITLane.PROSPECTIVE,
        as_of=as_of,
        window_start=window_start.astimezone(UTC),
        source_policy_id=source_policy_id,
        parameters={"keywords": list(keywords), "max_items": max_items},
        sources=sources,
        minimum_data_sources=minimum,
    )
    return asyncio.run(harness.execute(query, mode=DataQueryMode.FETCH_IF_MISSING))


def collect_syndication_feed_journal(
    *,
    source_config_paths: tuple[Path, ...],
    window_start: datetime,
    keywords: tuple[str, ...],
    max_items: int,
    minimum_data_sources: int | None,
    state_root: Path,
    provider_timeout_seconds: float,
    poll_interval_seconds: int,
    maximum_gap_seconds: int | None,
    cycles: int,
    sleeper: Callable[[float], None] = time.sleep,
) -> dict[str, object]:
    if cycles < 0:
        raise ValueError("cycles must be non-negative")
    configs = tuple(load_syndication_feed_source(path) for path in source_config_paths)
    provider = SyndicationFeedProvider(configs)
    gap_seconds = poll_interval_seconds * 2 if maximum_gap_seconds is None else maximum_gap_seconds
    policy = ProspectiveCollectionPolicy.build(
        capability=ObservationCapability.EVENT_REVELATION,
        sources=_syndication_source_bindings(provider.manifest, configs),
        window_start=window_start.astimezone(UTC),
        parameters={"keywords": list(keywords), "max_items": max_items},
        poll_interval_seconds=poll_interval_seconds,
        maximum_gap_seconds=gap_seconds,
    )
    journal = ProspectiveDataJournal(LocalDataSnapshotStore(state_root))
    completed = 0
    last_snapshot: DataSnapshot | None = None
    last_append: dict[str, object] | None = None
    interrupted = False
    try:
        while cycles == 0 or completed < cycles:
            snapshot = capture_syndication_data_snapshot(
                source_config_paths=source_config_paths,
                window_start=window_start,
                source_policy_id=policy.policy_id,
                keywords=keywords,
                max_items=max_items,
                minimum_data_sources=minimum_data_sources,
                state_root=state_root,
                provider_timeout_seconds=provider_timeout_seconds,
            )
            append = journal.record_snapshot(snapshot, policy=policy)
            completed += 1
            last_snapshot = snapshot
            last_append = append.to_dict()
            if cycles != 0 and completed >= cycles:
                break
            sleeper(float(policy.poll_interval_seconds))
    except KeyboardInterrupt:
        interrupted = True
    return {
        "collected": completed > 0,
        "interrupted": interrupted,
        "cycles_completed": completed,
        "policy": policy.to_dict(),
        "last_data_snapshot_id": (None if last_snapshot is None else last_snapshot.snapshot_id),
        "last_capture_cutoff_at": (
            None if last_snapshot is None else _utc_timestamp(last_snapshot.query.as_of)
        ),
        "last_append": last_append,
        "journal_stats": journal.stats(policy_id=policy.policy_id) if completed else None,
        "state_root": state_root.resolve().as_posix(),
        "historical_pit_claim": False,
        "evidence_promoted": False,
        "execution_capability": False,
    }


def freeze_syndication_feed_dataset(
    *,
    policy_id: str,
    window_start: datetime,
    not_after: datetime,
    minimum_data_sources: int | None,
    state_root: Path,
) -> dict[str, object]:
    store = LocalDataSnapshotStore(state_root)
    journal = ProspectiveDataJournal(store)
    snapshot = journal.freeze_snapshot(
        policy_id=policy_id,
        window_start=window_start.astimezone(UTC),
        not_after=not_after.astimezone(UTC),
        minimum_data_sources=minimum_data_sources,
    )
    dataset = (
        journal.materialize_snapshot_parquet(snapshot_id=snapshot.snapshot_id)
        if snapshot.coverage_complete
        else None
    )
    return {
        "frozen": snapshot.coverage_complete,
        "requested_not_after": _utc_timestamp(not_after),
        "effective_cutoff_at": _utc_timestamp(snapshot.query.as_of),
        "data_snapshot_id": snapshot.snapshot_id,
        "coverage_complete": snapshot.coverage_complete,
        "observation_count": len(snapshot.observations),
        "dataset": None if dataset is None else dataset.to_dict(),
        "state_root": state_root.resolve().as_posix(),
        "agent_tool_eligible": snapshot.coverage_complete,
        "historical_pit_claim": False,
        "evidence_promoted": False,
        "execution_capability": False,
    }


def accept_csrc_news_source(
    *,
    source_config_path: Path,
    window_start: datetime,
    keywords: tuple[str, ...],
    max_items: int,
    state_root: Path,
    provider_timeout_seconds: float,
) -> dict[str, object]:
    config = load_csrc_news_source(source_config_path)
    http_client = UrllibCsrcNewsHTTPClient(timeout_seconds=provider_timeout_seconds)
    provider = CsrcNewsProvider(
        (config,),
        http_client=http_client,
    )
    parameters: dict[str, object] = {
        "keywords": list(keywords),
        "max_items": max_items,
    }
    captures = asyncio.run(
        provider.collect(
            window_start=window_start.astimezone(UTC),
            parameters=parameters,
        )
    )
    capture_cutoff = max(item.retrieved_at for item in captures)
    manifest_hash = canonical_hash(provider.manifest.to_dict())
    source = DataSourceBinding(
        provider_id=provider.manifest.provider_id,
        provider_version=provider.manifest.provider_version,
        upstream_source=config.source_id,
        manifest_hash=manifest_hash,
        source_config_hash=config.artifact_hash,
        required=True,
    )
    declaration = SourceRouteAcceptanceDeclaration.build(
        provider_id=provider.manifest.provider_id,
        provider_version=provider.manifest.provider_version,
        provider_manifest_hash=manifest_hash,
        source_config_hash=config.artifact_hash,
        upstream_source=config.source_id,
        capability=ObservationCapability.EVENT_REVELATION,
        rights_basis_url=config.rights_basis_url,
        rights_reviewed_at=config.rights_reviewed_at,
        permitted_use="private_research",
        retention_scope="private_raw_and_normalized",
        redistribution_allowed=config.redistribution_allowed,
        semantic_scope="official_capital_market_policy_publication",
        revision_strategy="append_only_content_versions",
    )
    query = DataQuery.build(
        capability=ObservationCapability.EVENT_REVELATION,
        pit_lane=DataPITLane.PROSPECTIVE,
        as_of=capture_cutoff,
        window_start=window_start.astimezone(UTC),
        source_policy_id=declaration.declaration_id,
        parameters=parameters,
        sources=(source,),
        minimum_data_sources=1,
    )
    capture_provider = provider.replay(captures)
    store = LocalDataSnapshotStore(state_root)
    rights_response = http_client.get(
        config.rights_basis_url,
        max_response_bytes=2_000_000,
    )
    rights_retrieved_at = datetime.now(UTC)
    rights_hash = store.put_raw(rights_response.body)
    rights_evidence = SourceRightsEvidence.build(
        source_ref=config.rights_basis_url,
        final_url=rights_response.final_url,
        retrieved_at=rights_retrieved_at,
        raw_content_hash=rights_hash,
    )
    harness = DataInputHarness(store, provider_timeout_seconds=provider_timeout_seconds)
    harness.register(capture_provider)
    snapshot = asyncio.run(harness.execute(query, mode=DataQueryMode.FETCH_IF_MISSING))
    state_root.mkdir(mode=0o700, parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=".acceptance-replay-", dir=state_root) as replay_root:
        replay_store = LocalDataSnapshotStore(Path(replay_root))

        def replay_from_stored_artifacts(
            request: SourceRouteReplayRequest,
        ) -> SourceRouteReplayResult:
            if request.source_snapshot_id != snapshot.snapshot_id:
                raise ValueError("CSRC replay source Snapshot identity mismatch")
            stored_capture = load_csrc_news_capture_bundle(
                request.raw_response_payload,
                config=config,
                retrieved_at=snapshot.attempts[0].retrieved_at,
            )
            replay_provider = provider.replay((stored_capture,))
            replay_harness = DataInputHarness(
                replay_store,
                provider_timeout_seconds=provider_timeout_seconds,
            )
            replay_harness.register(replay_provider)
            replay_snapshot = asyncio.run(
                replay_harness.execute(query, mode=DataQueryMode.FETCH_IF_MISSING)
            )
            return SourceRouteReplayResult(
                snapshot_id=replay_snapshot.snapshot_id,
                store=replay_store,
            )

        report = qualify_source_route(
            declaration=declaration,
            rights_evidence=rights_evidence,
            snapshot=snapshot,
            source_store=store,
            replay_from_stored_artifacts=replay_from_stored_artifacts,
            evaluated_at=datetime.now(UTC),
        )
    report_path = write_source_route_acceptance_report(
        report,
        state_root / "source-acceptance",
    )
    return {
        "accepted": report.accepted,
        "source_route_acceptance_report_id": report.report_id,
        "source_route_acceptance_report_path": report_path.as_posix(),
        "data_snapshot_id": snapshot.snapshot_id,
        "capture_cutoff_at": _utc_timestamp(snapshot.query.as_of),
        "coverage_complete": snapshot.coverage_complete,
        "observation_count": len(snapshot.observations),
        "gates": [item.to_dict() for item in report.gates],
        "historical_pit_claim": report.historical_pit_claim,
        "evidence_promoted": report.evidence_promoted,
        "execution_capability": report.execution_capability,
    }


def accept_nbs_macro_release_source(
    *,
    source_config_path: Path,
    window_start: datetime,
    indicators: tuple[str, ...],
    poll_interval_seconds: int,
    maximum_gap_seconds: int,
    state_root: Path,
    provider_timeout_seconds: float,
    http_client: NbsMacroReleaseHTTPClient | None = None,
    clock: Callable[[], datetime] | None = None,
) -> dict[str, object]:
    config = load_nbs_macro_release_source(source_config_path)
    if indicators and indicators != config.indicators:
        raise ValueError(
            "NBS macro release acceptance indicators must exactly match the source config"
        )
    parameters: dict[str, object] = {"indicators": list(config.indicators)}
    client = (
        UrllibNbsMacroReleaseHTTPClient(timeout_seconds=provider_timeout_seconds)
        if http_client is None
        else http_client
    )
    provider = NbsMacroReleaseProvider(
        (config,),
        http_client=client,
        clock=clock,
    )
    captures = asyncio.run(
        provider.collect(
            window_start=window_start.astimezone(UTC),
            parameters=parameters,
        )
    )
    if any(not item.coverage_complete for item in captures):
        error_kind = next(
            (
                item.error_kind
                for item in captures
                if not item.coverage_complete and item.error_kind is not None
            ),
            "unknown_source_error",
        )
        raise RuntimeError(f"NBS macro release capture failed: {error_kind}")
    capture_cutoff = max(item.retrieved_at for item in captures)
    manifest_hash = canonical_hash(provider.manifest.to_dict())
    source = DataSourceBinding(
        provider_id=provider.manifest.provider_id,
        provider_version=provider.manifest.provider_version,
        upstream_source=config.source_id,
        manifest_hash=manifest_hash,
        source_config_hash=config.artifact_hash,
        required=True,
    )
    policy = ProspectiveCollectionPolicy.build(
        capability=ObservationCapability.MACRO_VINTAGE,
        sources=(source,),
        window_start=window_start.astimezone(UTC),
        parameters=parameters,
        poll_interval_seconds=poll_interval_seconds,
        maximum_gap_seconds=maximum_gap_seconds,
    )
    query = DataQuery.build(
        capability=ObservationCapability.MACRO_VINTAGE,
        pit_lane=DataPITLane.PROSPECTIVE,
        as_of=capture_cutoff,
        window_start=policy.window_start,
        source_policy_id=policy.policy_id,
        parameters=parameters,
        sources=(source,),
        minimum_data_sources=1,
    )
    store = LocalDataSnapshotStore(state_root)
    harness = DataInputHarness(store, provider_timeout_seconds=provider_timeout_seconds)
    harness.register(provider.replay(captures))
    snapshot = asyncio.run(harness.execute(query, mode=DataQueryMode.FETCH_IF_MISSING))
    observed_indicators = tuple(
        cast(str, item.normalized_payload.get("indicator")) for item in snapshot.observations
    )
    if observed_indicators != config.indicators:
        raise RuntimeError(
            "NBS macro release acceptance requires one observation for every configured indicator"
        )
    journal_result = ProspectiveDataJournal(store).record_snapshot(snapshot, policy=policy)

    rights_response = client.get(
        config.rights_basis_url,
        max_response_bytes=config.max_article_bytes,
    )
    if rights_response.final_url != config.rights_basis_url:
        raise ValueError("NBS macro release rights evidence redirect target drifted")
    if rights_response.content_type.casefold().split(";", 1)[0].strip() != "text/html":
        raise ValueError("NBS macro release rights evidence content type drifted")
    current_time = datetime.now(UTC) if clock is None else clock()
    if current_time.utcoffset() != UTC.utcoffset(current_time):
        raise ValueError("NBS macro release acceptance clock must use UTC")
    rights_hash = store.put_raw(rights_response.body)
    rights_evidence = SourceRightsEvidence.build(
        source_ref=config.rights_basis_url,
        final_url=rights_response.final_url,
        retrieved_at=current_time,
        raw_content_hash=rights_hash,
    )
    declaration = SourceRouteAcceptanceDeclaration.build(
        provider_id=provider.manifest.provider_id,
        provider_version=provider.manifest.provider_version,
        provider_manifest_hash=manifest_hash,
        source_config_hash=config.artifact_hash,
        upstream_source=config.source_id,
        capability=ObservationCapability.MACRO_VINTAGE,
        rights_basis_url=config.rights_basis_url,
        rights_reviewed_at=config.rights_reviewed_at,
        permitted_use="private_research",
        retention_scope="private_raw_and_normalized",
        redistribution_allowed=config.redistribution_allowed,
        semantic_scope=NBS_MACRO_RELEASE_SEMANTIC_SCOPE,
        revision_strategy=NBS_MACRO_RELEASE_REVISION_STRATEGY,
    )

    state_root.mkdir(mode=0o700, parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(
        prefix=".nbs-macro-release-acceptance-replay-",
        dir=state_root,
    ) as replay_root:
        replay_store = LocalDataSnapshotStore(Path(replay_root))

        def replay_from_stored_artifacts(
            request: SourceRouteReplayRequest,
        ) -> SourceRouteReplayResult:
            if request.source_snapshot_id != snapshot.snapshot_id:
                raise ValueError("NBS macro release replay source Snapshot identity mismatch")
            stored_capture = load_nbs_macro_release_capture_bundle(
                request.raw_response_payload,
                config=config,
                retrieved_at=snapshot.attempts[0].retrieved_at,
            )
            replay_harness = DataInputHarness(
                replay_store,
                provider_timeout_seconds=provider_timeout_seconds,
            )
            replay_harness.register(provider.replay((stored_capture,)))
            replay_snapshot = asyncio.run(
                replay_harness.execute(query, mode=DataQueryMode.FETCH_IF_MISSING)
            )
            return SourceRouteReplayResult(
                snapshot_id=replay_snapshot.snapshot_id,
                store=replay_store,
            )

        report = qualify_source_route(
            declaration=declaration,
            rights_evidence=rights_evidence,
            snapshot=snapshot,
            source_store=store,
            replay_from_stored_artifacts=replay_from_stored_artifacts,
            evaluated_at=current_time,
        )
    report_path = write_source_route_acceptance_report(
        report,
        state_root / "source-acceptance",
    )
    return {
        "accepted": report.accepted,
        "source_route_acceptance_report_id": report.report_id,
        "source_route_acceptance_report_path": report_path.as_posix(),
        "collection_policy_id": policy.policy_id,
        "journal": journal_result.to_dict(),
        "data_snapshot_id": snapshot.snapshot_id,
        "capture_cutoff_at": _utc_timestamp(snapshot.query.as_of),
        "coverage_complete": snapshot.coverage_complete,
        "observation_count": len(snapshot.observations),
        "gates": [item.to_dict() for item in report.gates],
        "semantic_scope": NBS_MACRO_RELEASE_SEMANTIC_SCOPE,
        "revision_strategy": NBS_MACRO_RELEASE_REVISION_STRATEGY,
        "historical_pit_claim": report.historical_pit_claim,
        "evidence_promoted": report.evidence_promoted,
        "execution_capability": report.execution_capability,
    }


def accept_tushare_observation_source(
    *,
    token: str,
    source_config_path: Path,
    parameters: Mapping[str, object],
    window_start: datetime,
    poll_interval_seconds: int,
    maximum_gap_seconds: int,
    state_root: Path,
    provider_timeout_seconds: float,
    transport: TushareObservationTransport | None = None,
    rights_fetcher: Callable[[str], tuple[str, bytes]] | None = None,
    clock: Callable[[], datetime] | None = None,
) -> dict[str, object]:
    if not token or token != token.strip():
        raise ValueError("TUSHARE_TOKEN is not configured")
    config = load_tushare_observation_source(source_config_path)
    provider = TushareObservationProvider(
        token,
        (config,),
        timeout_seconds=provider_timeout_seconds,
        transport=transport,
        clock=clock,
    )
    capture = asyncio.run(provider.collect(source_id=config.source_id, parameters=parameters))
    if not capture.coverage_complete:
        raise RuntimeError(
            f"Tushare route capture failed: {capture.error_kind or 'unknown_source_error'}"
        )
    manifest_hash = canonical_hash(provider.manifest.to_dict())
    source = DataSourceBinding(
        provider_id=provider.manifest.provider_id,
        provider_version=provider.manifest.provider_version,
        upstream_source=config.source_id,
        manifest_hash=manifest_hash,
        source_config_hash=config.artifact_hash,
        required=True,
    )
    policy = ProspectiveCollectionPolicy.build(
        capability=config.capability,
        sources=(source,),
        window_start=window_start.astimezone(UTC),
        parameters=parameters,
        poll_interval_seconds=poll_interval_seconds,
        maximum_gap_seconds=maximum_gap_seconds,
    )
    query = DataQuery.build(
        capability=config.capability,
        pit_lane=DataPITLane.PROSPECTIVE,
        as_of=capture.retrieved_at,
        window_start=policy.window_start,
        source_policy_id=policy.policy_id,
        parameters=parameters,
        sources=(source,),
        minimum_data_sources=1,
    )
    capture_provider = provider.replay((capture,))
    store = LocalDataSnapshotStore(state_root)
    harness = DataInputHarness(store, provider_timeout_seconds=provider_timeout_seconds)
    harness.register(capture_provider)
    snapshot = asyncio.run(harness.execute(query, mode=DataQueryMode.FETCH_IF_MISSING))
    journal = ProspectiveDataJournal(store)
    journal_result = journal.record_snapshot(snapshot, policy=policy)

    if rights_fetcher is None:
        rights_final_url, rights_payload = _fetch_public_https_document(
            config.rights_url,
            timeout_seconds=provider_timeout_seconds,
            max_response_bytes=2_000_000,
        )
    else:
        rights_final_url, rights_payload = rights_fetcher(config.rights_url)
    if not rights_payload:
        raise ValueError("Tushare route rights evidence is empty")
    current_time = datetime.now(UTC) if clock is None else clock()
    rights_hash = store.put_raw(rights_payload)
    rights_evidence = SourceRightsEvidence.build(
        source_ref=config.rights_url,
        final_url=rights_final_url,
        retrieved_at=current_time,
        raw_content_hash=rights_hash,
    )
    declaration = SourceRouteAcceptanceDeclaration.build(
        provider_id=provider.manifest.provider_id,
        provider_version=provider.manifest.provider_version,
        provider_manifest_hash=manifest_hash,
        source_config_hash=config.artifact_hash,
        upstream_source=config.source_id,
        capability=config.capability,
        rights_basis_url=config.rights_url,
        rights_reviewed_at=current_time,
        permitted_use="private_research",
        retention_scope="private_raw_and_normalized",
        redistribution_allowed=False,
        semantic_scope=config.semantic_scope,
        revision_strategy="append_only_content_versions",
    )

    state_root.mkdir(mode=0o700, parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(
        prefix=".tushare-acceptance-replay-",
        dir=state_root,
    ) as replay_root:
        replay_store = LocalDataSnapshotStore(Path(replay_root))

        def replay_from_stored_artifacts(
            request: SourceRouteReplayRequest,
        ) -> SourceRouteReplayResult:
            if request.source_snapshot_id != snapshot.snapshot_id:
                raise ValueError("Tushare replay source Snapshot identity mismatch")
            stored_capture = load_tushare_observation_capture_bundle(
                request.raw_response_payload,
                config=config,
                parameters=parameters,
                retrieved_at=snapshot.attempts[0].retrieved_at,
            )
            replay_provider = provider.replay((stored_capture,))
            replay_harness = DataInputHarness(
                replay_store,
                provider_timeout_seconds=provider_timeout_seconds,
            )
            replay_harness.register(replay_provider)
            replay_snapshot = asyncio.run(
                replay_harness.execute(query, mode=DataQueryMode.FETCH_IF_MISSING)
            )
            return SourceRouteReplayResult(
                snapshot_id=replay_snapshot.snapshot_id,
                store=replay_store,
            )

        report = qualify_source_route(
            declaration=declaration,
            rights_evidence=rights_evidence,
            snapshot=snapshot,
            source_store=store,
            replay_from_stored_artifacts=replay_from_stored_artifacts,
            evaluated_at=current_time,
        )
    report_path = write_source_route_acceptance_report(
        report,
        state_root / "source-acceptance",
    )
    return {
        "accepted": report.accepted,
        "source_route_acceptance_report_id": report.report_id,
        "source_route_acceptance_report_path": report_path.as_posix(),
        "collection_policy_id": policy.policy_id,
        "data_snapshot_id": snapshot.snapshot_id,
        "capture_cutoff_at": _utc_timestamp(snapshot.query.as_of),
        "coverage_complete": snapshot.coverage_complete,
        "observation_count": len(snapshot.observations),
        "journal_already_recorded": journal_result.already_recorded,
        "pit_lane": snapshot.query.pit_lane.value,
        "historical_pit_claim": report.historical_pit_claim,
        "evidence_promoted": report.evidence_promoted,
        "execution_capability": report.execution_capability,
        "gates": [item.to_dict() for item in report.gates],
    }


def register_prospective_collection_job(
    *,
    adapter_kind: ProspectiveCollectionAdapterKind,
    source_config_path: Path,
    acceptance_report_path: Path,
    parameters: Mapping[str, object],
    window_start: datetime,
    starts_at: datetime,
    poll_interval_seconds: int,
    maximum_gap_seconds: int,
    misfire_grace_seconds: int,
    maximum_jitter_seconds: int,
    provider_timeout_seconds: float,
    state_root: Path,
    registered_at: datetime | None = None,
    rolling_lookback_seconds: int | None = None,
    rolling_window_timezone: str = "Asia/Shanghai",
) -> dict[str, object]:
    source_config, provider_manifest, capability, source_id, source_config_hash = (
        _prospective_collection_source_binding(adapter_kind, source_config_path)
    )
    source = DataSourceBinding(
        provider_id=provider_manifest.provider_id,
        provider_version=provider_manifest.provider_version,
        upstream_source=source_id,
        manifest_hash=canonical_hash(provider_manifest.to_dict()),
        source_config_hash=source_config_hash,
        required=True,
    )
    policy = ProspectiveCollectionPolicy.build(
        capability=capability,
        sources=(source,),
        window_start=window_start.astimezone(UTC),
        parameters=parameters,
        poll_interval_seconds=poll_interval_seconds,
        maximum_gap_seconds=maximum_gap_seconds,
        rolling_window=(
            None
            if rolling_lookback_seconds is None
            else ProspectiveRollingWindow(
                lookback_seconds=rolling_lookback_seconds,
                timezone=rolling_window_timezone,
            )
        ),
    )
    report = load_source_route_acceptance_report(acceptance_report_path)
    job = ProspectiveCollectionJob.build(
        adapter_kind=adapter_kind,
        collection_policy=policy,
        source_acceptance_report=report,
        source_config=source_config,
        starts_at=starts_at.astimezone(UTC),
        misfire_grace_seconds=misfire_grace_seconds,
        maximum_jitter_seconds=maximum_jitter_seconds,
        provider_timeout_seconds=provider_timeout_seconds,
    )
    store = LocalDataSnapshotStore(state_root)
    runtime = ProspectiveCollectionRuntime(store)
    registration_time = (
        datetime.now(UTC) if registered_at is None else registered_at.astimezone(UTC)
    )
    runtime.register(
        job,
        collection_policy=policy,
        source_acceptance_report=report,
        source_config=source_config,
        registered_at=registration_time,
    )
    return {
        "registered": True,
        "job": job.to_dict(),
        "collection_policy": policy.to_dict(),
        "health": runtime.health(job.job_id, now=registration_time).to_dict(),
        "historical_pit_claim": False,
        "evidence_promoted": False,
        "execution_capability": False,
    }


def run_due_prospective_collection_jobs(
    *,
    state_root: Path,
    now: datetime | None = None,
    job_ids: tuple[str, ...] = (),
    limit: int = 100,
    tushare_token: str | None = None,
    collector_factory: Callable[
        [ProspectiveCollectionJob, LocalDataSnapshotStore], ScheduledCollector
    ]
    | None = None,
    cancelled: Callable[[], bool] | None = None,
    maximum_state_bytes: int | None = None,
) -> dict[str, object]:
    if maximum_state_bytes is not None and maximum_state_bytes < 1:
        raise ValueError("maximum state budget must be positive")
    run_at = datetime.now(UTC) if now is None else now.astimezone(UTC)
    store = LocalDataSnapshotStore(state_root)
    runtime = ProspectiveCollectionRuntime(store)
    selected_job_ids = job_ids if job_ids else runtime.due_job_ids(now=run_at, limit=limit)
    results: list[dict[str, object]] = []
    for job_id in selected_job_ids:
        job = runtime.job(job_id)
        if collector_factory is None:
            collector = _bound_prospective_collector(
                job=job,
                store=store,
                tushare_token=tushare_token,
            )
        else:
            collector = collector_factory(job, store)
        if maximum_state_bytes is not None:
            collector = _state_budget_guarded_collector(
                collector,
                state_root=state_root,
                maximum_state_bytes=maximum_state_bytes,
            )
        results.append(
            runtime.run_due(
                job_id,
                now=run_at,
                collector=collector,
                cancelled=cancelled,
            ).to_dict()
        )
    health = [runtime.health(job_id, now=run_at).to_dict() for job_id in selected_job_ids]
    return {
        "completed": True,
        "run_at": _utc_timestamp(run_at),
        "job_count": len(selected_job_ids),
        "results": results,
        "health": health,
        "historical_pit_claim": False,
        "evidence_promoted": False,
        "execution_capability": False,
    }


def _state_budget_guarded_collector(
    collector: ScheduledCollector,
    *,
    state_root: Path,
    maximum_state_bytes: int,
) -> ScheduledCollector:
    def guarded(
        policy: ProspectiveCollectionPolicy,
        source_config: dict[str, object],
        scheduled_for: datetime,
    ) -> DataSnapshot:
        metrics = collect_operations_metrics(
            state_root=state_root,
            measured_at=datetime.now(UTC),
        )
        assert_within_state_budget(metrics, maximum_state_bytes=maximum_state_bytes)
        return collector(policy, source_config, scheduled_for)

    return guarded


def _bound_prospective_collector(
    *,
    job: ProspectiveCollectionJob,
    store: LocalDataSnapshotStore,
    tushare_token: str | None,
) -> ScheduledCollector:
    def collector(
        policy: ProspectiveCollectionPolicy,
        source_config: dict[str, object],
        scheduled_for: datetime,
    ) -> DataSnapshot:
        return collect_prospective_source_snapshot(
            job=job,
            policy=policy,
            source_config=source_config,
            store=store,
            tushare_token=tushare_token,
            scheduled_for=scheduled_for,
        )

    return collector


def prospective_collection_health(
    *,
    state_root: Path,
    now: datetime | None = None,
    job_ids: tuple[str, ...] = (),
    limit: int = 100,
) -> dict[str, object]:
    observed_at = datetime.now(UTC) if now is None else now.astimezone(UTC)
    runtime = ProspectiveCollectionRuntime(LocalDataSnapshotStore(state_root))
    selected_job_ids = job_ids if job_ids else runtime.job_ids(limit=limit)
    rolling_since = observed_at - timedelta(hours=24)
    return {
        "observed_at": _utc_timestamp(observed_at),
        "job_count": len(selected_job_ids),
        "health": [
            runtime.health(job_id, now=observed_at).to_dict() for job_id in selected_job_ids
        ],
        "collection_usage": [
            {
                "job_id": job_id,
                "lifetime": runtime.usage_summary(job_id),
                "rolling_24h": runtime.usage_summary(job_id, since=rolling_since),
            }
            for job_id in selected_job_ids
        ],
        "historical_pit_claim": False,
        "execution_capability": False,
    }


def qualify_prospective_collection_tracer_jobs(
    *,
    state_root: Path,
    job_ids: tuple[str, ...],
    evaluated_at: datetime | None = None,
) -> tuple[dict[str, object], Path]:
    evaluation_time = datetime.now(UTC) if evaluated_at is None else evaluated_at.astimezone(UTC)
    runtime = ProspectiveCollectionRuntime(LocalDataSnapshotStore(state_root))
    report = qualify_prospective_collection_tracer(
        runtime=runtime,
        job_ids=job_ids,
        evaluated_at=evaluation_time,
    )
    report_path = write_prospective_collection_tracer_report(
        report,
        state_root=state_root,
    )
    return report.to_dict(), report_path


def _prospective_collection_source_binding(
    adapter_kind: ProspectiveCollectionAdapterKind,
    source_config_path: Path,
) -> tuple[
    dict[str, object],
    ObservationProviderManifest,
    ObservationCapability,
    str,
    str,
]:
    if adapter_kind is ProspectiveCollectionAdapterKind.CSRC_NEWS:
        config = load_csrc_news_source(source_config_path)
        provider = CsrcNewsProvider((config,))
        return (
            config.to_dict(),
            provider.manifest,
            ObservationCapability.EVENT_REVELATION,
            config.source_id,
            config.artifact_hash,
        )
    if adapter_kind is ProspectiveCollectionAdapterKind.NBS_MACRO_RELEASE:
        config = load_nbs_macro_release_source(source_config_path)
        provider = NbsMacroReleaseProvider((config,))
        return (
            config.to_dict(),
            provider.manifest,
            ObservationCapability.MACRO_VINTAGE,
            config.source_id,
            config.artifact_hash,
        )
    if adapter_kind is ProspectiveCollectionAdapterKind.TUSHARE_OBSERVATION:
        config = load_tushare_observation_source(source_config_path)
        provider = TushareObservationProvider("manifest-construction-only", (config,))
        return (
            config.to_dict(),
            provider.manifest,
            config.capability,
            config.source_id,
            config.artifact_hash,
        )
    raise ValueError("unsupported prospective collection adapter kind")


def _syndication_source_bindings(
    manifest: ObservationProviderManifest,
    configs: tuple[SyndicationFeedSourceConfig, ...],
) -> tuple[DataSourceBinding, ...]:
    manifest_hash = canonical_hash(manifest.to_dict())
    return tuple(
        DataSourceBinding(
            provider_id=manifest.provider_id,
            provider_version=manifest.provider_version,
            upstream_source=config.source_id,
            manifest_hash=manifest_hash,
            source_config_hash=config.artifact_hash,
            required=True,
        )
        for config in configs
    )


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


def validate_method_quality_benchmark(
    *,
    registration_path: Path,
    method_catalog_path: Path,
    provider_profile_path: Path,
    evaluation_specification_path: Path,
    historical_manifest_path: Path,
    evidence_pack_path: Path,
    evidence_documents_path: Path,
    masked_input_manifest_path: Path,
    masked_evidence_pack_path: Path,
    masked_evidence_documents_path: Path,
    pattern_pack_paths: tuple[Path, ...],
    masked_pattern_pack_paths: tuple[Path, ...],
    skill_root: Path,
) -> dict[str, object]:
    registration_preview = json.loads(registration_path.read_text(encoding="utf-8"))
    evaluation_preview = json.loads(evaluation_specification_path.read_text(encoding="utf-8"))
    typed_registration_preview = (
        cast(dict[str, object], registration_preview)
        if isinstance(registration_preview, dict)
        else None
    )
    typed_evaluation_preview = (
        cast(dict[str, object], evaluation_preview)
        if isinstance(evaluation_preview, dict)
        else None
    )
    registration_schema = (
        "method-quality-benchmark-registration-v2.schema.json"
        if typed_registration_preview is not None
        and typed_registration_preview.get("schema_version")
        == "market-impact.method-quality-benchmark-registration.v2"
        else "method-quality-benchmark-registration.schema.json"
    )
    evaluation_schema = (
        "method-quality-evaluation-specification-v2.schema.json"
        if typed_evaluation_preview is not None
        and typed_evaluation_preview.get("schema_version")
        == "market-impact.method-quality-evaluation-specification.v2"
        else "method-quality-evaluation-specification.schema.json"
    )
    schema_inputs = (
        (
            registration_path,
            registration_schema,
        ),
        (method_catalog_path, "research-method-catalog.schema.json"),
        (provider_profile_path, "model-provider-profile.schema.json"),
        (
            evaluation_specification_path,
            evaluation_schema,
        ),
        (historical_manifest_path, "historical-evidence-manifest.schema.json"),
        (evidence_pack_path, "evidence-pack.schema.json"),
        (masked_input_manifest_path, "masked-agent-input-manifest.schema.json"),
        (masked_evidence_pack_path, "evidence-pack.schema.json"),
    )
    errors: list[str] = []
    payloads: dict[Path, object] = {}
    for path, schema_name in schema_inputs:
        payload = json.loads(path.read_text(encoding="utf-8"))
        payloads[path] = payload
        errors.extend(f"{path}: {error}" for error in validate_agent_contract(payload, schema_name))
    if errors:
        return {"valid": False, "errors": errors}
    registration = load_method_quality_benchmark(registration_path)
    catalog = load_research_method_catalog(method_catalog_path)
    profile = load_model_provider_profile(provider_profile_path)
    evaluation_specification = load_method_quality_evaluation_specification(
        evaluation_specification_path
    )
    registration.validate_against(
        catalog=catalog,
        provider_profile=profile,
        skills=SkillRegistry(skill_root),
        evaluation_specification=evaluation_specification,
    )
    historical_manifest = load_historical_evidence_manifest(historical_manifest_path)
    evidence_pack = evidence_pack_from_dict(payloads[evidence_pack_path])
    historical_manifest.validate_against(evidence_pack)
    masked_manifest = load_masked_agent_input_manifest(masked_input_manifest_path)
    if (
        historical_manifest.masked_agent_input_manifest_id != masked_manifest.manifest_id
        or historical_manifest.masked_agent_input_manifest_hash != masked_manifest.manifest_hash
    ):
        raise ValueError("historical manifest does not match masked Agent Input Manifest")
    masked_pack = evidence_pack_from_dict(payloads[masked_evidence_pack_path])
    original_documents = json.loads(evidence_documents_path.read_text(encoding="utf-8"))
    masked_documents = json.loads(masked_evidence_documents_path.read_text(encoding="utf-8"))
    if not isinstance(original_documents, dict) or not isinstance(masked_documents, dict):
        raise TypeError("evidence document files must be objects")
    original_pattern_packs = tuple(
        pattern_pack_from_dict(json.loads(path.read_text(encoding="utf-8")))
        for path in pattern_pack_paths
    )
    masked_pattern_packs = tuple(
        pattern_pack_from_dict(json.loads(path.read_text(encoding="utf-8")))
        for path in masked_pattern_pack_paths
    )
    masked_manifest.validate_against(
        original_pack=evidence_pack,
        original_documents=cast(dict[str, object], original_documents),
        original_pattern_packs=original_pattern_packs,
        masked_pack=masked_pack,
        masked_documents=cast(dict[str, object], masked_documents),
        masked_pattern_packs=masked_pattern_packs,
    )
    result: dict[str, object] = {
        "errors": [],
        "registration_id": registration.registration_id,
        "registration_hash": registration.registration_hash,
        "method_catalog_id": catalog.catalog_id,
        "provider_profile_id": profile.profile_id,
        "evaluation_specification_id": evaluation_specification.specification_id,
        "development_case_count": registration.development_case_count,
        "retrospective_holdout_case_count": (registration.retrospective_holdout_case_count),
        "suite_ids": [item.suite_id for item in registration.suites],
        "case_alias": historical_manifest.case_alias,
        "case_split": historical_manifest.split.value,
        "historical_manifest_id": historical_manifest.manifest_id,
        "provenance_trust_status": historical_manifest.provenance_trust_status.value,
        "source_authentication": (
            "not_available_for_supplied_case"
            if registration.schema_version
            == "market-impact.method-quality-benchmark-registration.v2"
            else "not_available_in_v1"
        ),
        "retrospective_holdout_admission": (
            "unavailable_until_publisher_time_and_latency_acceptance"
            if registration.schema_version
            == "market-impact.method-quality-benchmark-registration.v2"
            else "unavailable_in_v1"
        ),
        "independent_statistical_unit": (
            "event_case"
            if registration.schema_version
            == "market-impact.method-quality-benchmark-registration.v2"
            else "case_replicate_retired"
        ),
        "evidence_pack_id": evidence_pack.pack_id,
        "masked_input_manifest_id": masked_manifest.manifest_id,
        "masked_evidence_pack_id": masked_pack.pack_id,
        "evidence_version_count": len(historical_manifest.evidence_versions),
        "outcomes_opened": registration.outcomes_opened,
        "execution_capability": registration.execution_capability,
    }
    if registration.schema_version != "market-impact.method-quality-benchmark-registration.v2":
        return {
            **result,
            "valid": False,
            "audit_valid": True,
            "claim_eligible": False,
            "validation_status": "retired_v1_audit_only",
        }
    return {**result, "valid": True}


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
    provider_profile_path: Path | None = None,
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
    profile = load_model_provider_profile(
        provider_profile_path or default_model_provider_profile_path()
    )
    provider = cast(
        AvailableModelProvider,
        ModelProviderFactory.with_builtin_adapters().create(profile),
    )
    await provider.assert_model_available(timeout_seconds=30)
    state_directory = state_root / canonical_hash(run_id)
    artifact_store = ArtifactStore(state_directory / "artifacts")
    tool_registry = ToolRegistry(artifact_store)
    for descriptor in repository.tool_descriptors():
        tool_registry.register(descriptor)
    config = profile.runtime_config()
    api_key = os.environ.get(profile.credential_env, "")
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


def _aware_timestamp(value: str) -> datetime:
    try:
        result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise argparse.ArgumentTypeError("timestamps must use ISO 8601") from exc
    if result.tzinfo is None or result.utcoffset() is None:
        raise argparse.ArgumentTypeError("timestamps must include an explicit timezone")
    return result


def _utc_timestamp(value: datetime) -> str:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("timestamp must be timezone-aware")
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _json_object_argument(value: str) -> dict[str, object]:
    payload: object = json.loads(value)
    if not isinstance(payload, dict):
        raise ValueError("parameters JSON must be an object")
    untyped = cast(dict[object, object], payload)
    if any(not isinstance(key, str) for key in untyped):
        raise ValueError("parameters JSON keys must be strings")
    return cast(dict[str, object], untyped)


def _fetch_public_https_document(
    url: str,
    *,
    timeout_seconds: float,
    max_response_bytes: int,
) -> tuple[str, bytes]:
    parsed = urlsplit(url)
    if (
        parsed.scheme != "https"
        or not parsed.netloc
        or parsed.username is not None
        or parsed.password is not None
        or parsed.fragment
    ):
        raise ValueError("rights evidence URL must be a fixed public HTTPS URL")
    if timeout_seconds <= 0 or max_response_bytes < 1:
        raise ValueError("rights evidence fetch bounds are invalid")
    request = Request(url, headers={"User-Agent": "market-impact-agent/0.1"})
    with urlopen(request, timeout=timeout_seconds) as response:
        final_url = cast(str, response.geturl())
        body = response.read(max_response_bytes + 1)
    final = urlsplit(final_url)
    if (
        final.scheme != "https"
        or not final.netloc
        or final.username is not None
        or final.password is not None
        or final.fragment
    ):
        raise ValueError("rights evidence redirect must remain public HTTPS")
    if len(body) > max_response_bytes:
        raise ValueError("rights evidence response exceeds its byte limit")
    return final_url, body


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
    if args.command == "data" and args.data_command == "capture-feed":
        try:
            snapshot = capture_syndication_data_snapshot(
                source_config_paths=tuple(args.source_configs),
                window_start=args.window_start,
                source_policy_id=args.source_policy_id,
                keywords=tuple(args.keyword),
                max_items=args.max_items,
                minimum_data_sources=args.minimum_data_sources,
                state_root=args.state_root,
                provider_timeout_seconds=args.provider_timeout_seconds,
            )
        except (OSError, RuntimeError, TypeError, ValueError, json.JSONDecodeError) as exc:
            print(
                json.dumps({"captured": False, "error": f"{type(exc).__name__}: {exc}"}),
                file=sys.stderr,
            )
            return 1
        result = {
            "captured": True,
            "coverage_complete": snapshot.coverage_complete,
            "capture_cutoff_at": snapshot.query.as_of.isoformat().replace("+00:00", "Z"),
            "data_query_id": snapshot.query.query_id,
            "data_snapshot_id": snapshot.snapshot_id,
            "evidence_promoted": False,
            "execution_capability": False,
            "historical_pit_claim": False,
            "observation_count": len(snapshot.observations),
            "pit_lane": snapshot.query.pit_lane.value,
            "source_attempts": [item.to_dict() for item in snapshot.attempts],
            "state_root": args.state_root.resolve().as_posix(),
        }
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0 if snapshot.coverage_complete else 1
    if args.command == "data" and args.data_command == "collect-feed":
        try:
            result = collect_syndication_feed_journal(
                source_config_paths=tuple(args.source_configs),
                window_start=args.window_start,
                keywords=tuple(args.keyword),
                max_items=args.max_items,
                minimum_data_sources=args.minimum_data_sources,
                state_root=args.state_root,
                provider_timeout_seconds=args.provider_timeout_seconds,
                poll_interval_seconds=args.poll_interval_seconds,
                maximum_gap_seconds=args.maximum_gap_seconds,
                cycles=args.cycles,
            )
        except (OSError, RuntimeError, TypeError, ValueError, json.JSONDecodeError) as exc:
            print(
                json.dumps({"collected": False, "error": f"{type(exc).__name__}: {exc}"}),
                file=sys.stderr,
            )
            return 1
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0 if result["collected"] is True else 1
    if args.command == "data" and args.data_command == "freeze-feed-dataset":
        try:
            result = freeze_syndication_feed_dataset(
                policy_id=args.policy_id,
                window_start=args.window_start,
                not_after=args.not_after,
                minimum_data_sources=args.minimum_data_sources,
                state_root=args.state_root,
            )
        except (KeyError, OSError, RuntimeError, TypeError, ValueError) as exc:
            print(
                json.dumps({"frozen": False, "error": f"{type(exc).__name__}: {exc}"}),
                file=sys.stderr,
            )
            return 1
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0 if result["coverage_complete"] is True else 1
    if args.command == "data" and args.data_command == "accept-csrc-news":
        try:
            result = accept_csrc_news_source(
                source_config_path=args.source_config,
                window_start=args.window_start,
                keywords=tuple(args.keyword),
                max_items=args.max_items,
                state_root=args.state_root,
                provider_timeout_seconds=args.provider_timeout_seconds,
            )
        except (OSError, RuntimeError, TypeError, ValueError, json.JSONDecodeError) as exc:
            print(
                json.dumps({"accepted": False, "error": f"{type(exc).__name__}: {exc}"}),
                file=sys.stderr,
            )
            return 1
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0 if result["accepted"] is True else 1
    if args.command == "data" and args.data_command == "accept-nbs-macro-release":
        try:
            result = accept_nbs_macro_release_source(
                source_config_path=args.source_config,
                window_start=args.window_start,
                indicators=tuple(args.indicator),
                poll_interval_seconds=args.poll_interval_seconds,
                maximum_gap_seconds=args.maximum_gap_seconds,
                state_root=args.state_root,
                provider_timeout_seconds=args.provider_timeout_seconds,
            )
        except (OSError, RuntimeError, TypeError, ValueError, json.JSONDecodeError) as exc:
            print(
                json.dumps({"accepted": False, "error": f"{type(exc).__name__}: {exc}"}),
                file=sys.stderr,
            )
            return 1
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0 if result["accepted"] is True else 1
    if args.command == "data" and args.data_command == "validate-prospective-diagnostic":
        try:
            result = validate_prospective_diagnostic(args.registration)
        except (OSError, TypeError, ValueError, json.JSONDecodeError) as exc:
            print(
                json.dumps({"valid": False, "error": f"{type(exc).__name__}: {exc}"}),
                file=sys.stderr,
            )
            return 1
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0 if result["valid"] is True else 1
    if args.command == "data" and args.data_command == "checkpoint-readiness":
        try:
            result = prospective_checkpoint_readiness(
                registration_path=args.registration,
                route_plan_path=args.route_plan,
                state_root=args.state_root,
                evaluated_at=args.evaluated_at,
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
                json.dumps({"ready": False, "error": f"{type(exc).__name__}: {exc}"}),
                file=sys.stderr,
            )
            return 1
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0
    if args.command == "data" and args.data_command == "checkpoint-route-admit":
        try:
            result = admit_prospective_checkpoint_routes(
                registration_path=args.registration,
                route_plan_path=args.route_plan,
                state_root=args.state_root,
            )
        except (KeyError, OSError, TypeError, ValueError, json.JSONDecodeError) as exc:
            print(
                json.dumps({"admitted": False, "error": f"{type(exc).__name__}: {exc}"}),
                file=sys.stderr,
            )
            return 1
        print(json.dumps({**result, "admitted": True}, indent=2, sort_keys=True))
        return 0
    if args.command == "data" and args.data_command == "accept-tushare-observation":
        token = os.environ.get("TUSHARE_TOKEN", "")
        if not token:
            print(
                json.dumps({"accepted": False, "error": "TUSHARE_TOKEN is not configured"}),
                file=sys.stderr,
            )
            return 1
        try:
            result = accept_tushare_observation_source(
                token=token,
                source_config_path=args.source_config,
                parameters=_json_object_argument(args.parameters_json),
                window_start=args.window_start,
                poll_interval_seconds=args.poll_interval_seconds,
                maximum_gap_seconds=args.maximum_gap_seconds,
                state_root=args.state_root,
                provider_timeout_seconds=args.provider_timeout_seconds,
            )
        except (OSError, RuntimeError, TypeError, ValueError, json.JSONDecodeError) as exc:
            print(
                json.dumps({"accepted": False, "error": f"{type(exc).__name__}: {exc}"}),
                file=sys.stderr,
            )
            return 1
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0 if result["accepted"] is True else 1
    if args.command == "data" and args.data_command == "collection-register":
        try:
            result = register_prospective_collection_job(
                adapter_kind=ProspectiveCollectionAdapterKind(args.adapter_kind),
                source_config_path=args.source_config,
                acceptance_report_path=args.acceptance_report,
                parameters=_json_object_argument(args.parameters_json),
                window_start=args.window_start,
                starts_at=args.starts_at,
                poll_interval_seconds=args.poll_interval_seconds,
                maximum_gap_seconds=args.maximum_gap_seconds,
                misfire_grace_seconds=args.misfire_grace_seconds,
                maximum_jitter_seconds=args.maximum_jitter_seconds,
                provider_timeout_seconds=args.provider_timeout_seconds,
                state_root=args.state_root,
                rolling_lookback_seconds=args.rolling_lookback_seconds,
                rolling_window_timezone=args.rolling_window_timezone,
            )
        except (OSError, RuntimeError, TypeError, ValueError, json.JSONDecodeError) as exc:
            print(
                json.dumps({"registered": False, "error": f"{type(exc).__name__}: {exc}"}),
                file=sys.stderr,
            )
            return 1
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0
    if args.command == "data" and args.data_command == "collection-run-due":
        try:
            with _collection_cancellation_signal() as cancelled:
                result = run_due_prospective_collection_jobs(
                    state_root=args.state_root,
                    now=args.now,
                    job_ids=tuple(args.job_id),
                    limit=args.limit,
                    tushare_token=os.environ.get("TUSHARE_TOKEN"),
                    cancelled=cancelled,
                    maximum_state_bytes=args.maximum_state_bytes,
                )
        except (KeyError, OSError, RuntimeError, TypeError, ValueError) as exc:
            print(
                json.dumps({"completed": False, "error": f"{type(exc).__name__}: {exc}"}),
                file=sys.stderr,
            )
            return 1
        print(json.dumps(result, indent=2, sort_keys=True))
        successful_outcomes = {"success", "no_data", "not_due", "in_progress", "backing_off"}
        raw_results = result.get("results")
        result_items = cast(list[object], raw_results) if isinstance(raw_results, list) else []

        def succeeded(item: object) -> bool:
            if not isinstance(item, dict):
                return False
            return cast(dict[object, object], item).get("outcome") in successful_outcomes

        return 0 if all(succeeded(item) for item in result_items) else 1
    if args.command == "data" and args.data_command == "collection-health":
        try:
            result = prospective_collection_health(
                state_root=args.state_root,
                now=args.now,
                job_ids=tuple(args.job_id),
                limit=args.limit,
            )
        except (KeyError, OSError, RuntimeError, TypeError, ValueError) as exc:
            print(
                json.dumps({"completed": False, "error": f"{type(exc).__name__}: {exc}"}),
                file=sys.stderr,
            )
            return 1
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0
    if args.command == "data" and args.data_command == "collection-qualify-tracer":
        try:
            report, report_path = qualify_prospective_collection_tracer_jobs(
                state_root=args.state_root,
                job_ids=tuple(args.job_id),
                evaluated_at=args.evaluated_at,
            )
        except (KeyError, OSError, RuntimeError, TypeError, ValueError) as exc:
            print(
                json.dumps({"accepted": False, "error": f"{type(exc).__name__}: {exc}"}),
                file=sys.stderr,
            )
            return 1
        print(
            json.dumps(
                {**report, "report_path": report_path.as_posix()},
                indent=2,
                sort_keys=True,
            )
        )
        return 0 if report["accepted"] is True else 1
    if args.command == "data" and args.data_command == "collection-service-run":
        try:
            if args.require_clean_environment:
                assert_clean_supervisor_environment(os.environ)
            secrets = load_supervisor_environment(
                args.environment_file,
                state_root=args.state_root,
            )
            with _collection_cancellation_signal() as cancelled:
                result = run_due_prospective_collection_jobs(
                    state_root=args.state_root,
                    now=args.now,
                    job_ids=tuple(args.job_id),
                    limit=args.limit,
                    tushare_token=secrets["TUSHARE_TOKEN"],
                    cancelled=cancelled,
                    maximum_state_bytes=args.maximum_state_bytes,
                )
        except (KeyError, OSError, RuntimeError, TypeError, ValueError) as exc:
            print(
                json.dumps({"completed": False, "error": f"{type(exc).__name__}: {exc}"}),
                file=sys.stderr,
            )
            return 1
        output = {**result, "service_environment_loaded": True}
        print(json.dumps(output, indent=2, sort_keys=True))
        successful_outcomes = {"success", "no_data", "not_due", "in_progress", "backing_off"}
        raw_results = result.get("results")
        result_items = cast(list[object], raw_results) if isinstance(raw_results, list) else []

        def service_succeeded(item: object) -> bool:
            if not isinstance(item, dict):
                return False
            return cast(dict[object, object], item).get("outcome") in successful_outcomes

        return 0 if all(service_succeeded(item) for item in result_items) else 1
    if args.command == "data" and args.data_command == "collection-supervisor-plan":
        try:
            plan = ProspectiveSupervisorPlan.build(
                host_name=args.host_name,
                host_uid=args.host_uid,
                launchd_label=args.launchd_label,
                service_definition_path=args.service_definition_path,
                executable_path=args.executable_path,
                working_directory=args.working_directory,
                state_root=args.state_root,
                environment_file=args.environment_file,
                stdout_path=args.stdout_path,
                stderr_path=args.stderr_path,
                invocation_interval_seconds=args.invocation_interval_seconds,
                notification_policy=args.notification_policy,
                maximum_state_bytes=args.maximum_state_bytes,
            )
        except (TypeError, ValueError) as exc:
            print(
                json.dumps({"planned": False, "error": f"{type(exc).__name__}: {exc}"}),
                file=sys.stderr,
            )
            return 1
        print(
            json.dumps(
                {
                    "planned": True,
                    "plan": plan.to_dict(),
                    "launchd_plist": render_launchd_plist(plan),
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0
    if args.command == "data" and args.data_command == "state-backup":
        try:
            manifest, backup_path = create_state_backup(
                state_root=args.state_root,
                backup_parent=args.backup_parent,
                created_at=datetime.now(UTC),
            )
        except (OSError, RuntimeError, TypeError, ValueError, json.JSONDecodeError) as exc:
            print(
                json.dumps({"backed_up": False, "error": f"{type(exc).__name__}: {exc}"}),
                file=sys.stderr,
            )
            return 1
        print(
            json.dumps(
                {
                    "backed_up": True,
                    "backup_path": backup_path.as_posix(),
                    "manifest_id": manifest.manifest_id,
                    "file_count": len(manifest.files),
                    "execution_capability": False,
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0
    if args.command == "data" and args.data_command == "state-verify-backup":
        try:
            manifest = verify_state_backup(args.backup)
        except (OSError, RuntimeError, TypeError, ValueError, json.JSONDecodeError) as exc:
            print(
                json.dumps({"verified": False, "error": f"{type(exc).__name__}: {exc}"}),
                file=sys.stderr,
            )
            return 1
        print(
            json.dumps(
                {
                    "verified": True,
                    "manifest_id": manifest.manifest_id,
                    "file_count": len(manifest.files),
                    "sqlite_integrity_ok": manifest.sqlite_integrity_ok,
                    "foreign_keys_ok": manifest.foreign_keys_ok,
                    "execution_capability": False,
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0
    if args.command == "data" and args.data_command == "state-restore":
        try:
            receipt = restore_state_backup(
                backup_path=args.backup,
                destination=args.destination,
            )
        except (OSError, RuntimeError, TypeError, ValueError, json.JSONDecodeError) as exc:
            print(
                json.dumps({"restored": False, "error": f"{type(exc).__name__}: {exc}"}),
                file=sys.stderr,
            )
            return 1
        print(
            json.dumps(
                {
                    "restored": True,
                    "manifest_id": receipt.manifest_id,
                    "destination": receipt.destination.as_posix(),
                    "restored_file_count": receipt.restored_file_count,
                    "sqlite_integrity_ok": receipt.sqlite_integrity_ok,
                    "foreign_keys_ok": receipt.foreign_keys_ok,
                    "execution_capability": receipt.execution_capability,
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0
    if args.command == "archive" and args.archive_command == "common-crawl-verify":
        try:
            result = verify_common_crawl_archive(args.locator)
        except (OSError, RuntimeError, TypeError, ValueError, json.JSONDecodeError) as exc:
            print(
                json.dumps({"verified": False, "error": f"{type(exc).__name__}: {exc}"}),
                file=sys.stderr,
            )
            return 1
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0 if result["archive_capture_accepted"] is True else 1
    if args.command == "archive" and args.archive_command == "common-crawl-locate":
        try:
            result = locate_common_crawl_archive(
                collection=args.collection,
                target_url=args.url,
                not_after=args.not_after,
            )
        except (OSError, RuntimeError, TypeError, ValueError, json.JSONDecodeError) as exc:
            print(
                json.dumps({"located": False, "error": f"{type(exc).__name__}: {exc}"}),
                file=sys.stderr,
            )
            return 1
        if result is None:
            print(json.dumps({"located": False, "reason": "no_capture_before_cutoff"}))
            return 1
        print(json.dumps({"located": True, "locator": result}, indent=2, sort_keys=True))
        return 0
    if args.command == "archive" and args.archive_command == "internet-archive-verify":
        try:
            result = verify_internet_archive(args.locator)
        except (OSError, RuntimeError, TypeError, ValueError, json.JSONDecodeError) as exc:
            print(
                json.dumps({"verified": False, "error": f"{type(exc).__name__}: {exc}"}),
                file=sys.stderr,
            )
            return 1
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0 if result["archive_capture_accepted"] is True else 1
    if args.command == "archive" and args.archive_command == "internet-archive-locate":
        try:
            result = locate_internet_archive(
                target_url=args.url,
                not_after=args.not_after,
            )
        except (OSError, RuntimeError, TypeError, ValueError, json.JSONDecodeError) as exc:
            print(
                json.dumps({"located": False, "error": f"{type(exc).__name__}: {exc}"}),
                file=sys.stderr,
            )
            return 1
        if result is None:
            print(json.dumps({"located": False, "reason": "no_capture_before_cutoff"}))
            return 1
        print(json.dumps({"located": True, "locator": result}, indent=2, sort_keys=True))
        return 0
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
    if args.command == "regime" and args.regime_command == "validate":
        try:
            dataset = load_market_regime_dataset(args.dataset)
        except (OSError, TypeError, ValueError, json.JSONDecodeError) as exc:
            print(json.dumps({"valid": False, "error": str(exc)}), file=sys.stderr)
            return 1
        print(
            json.dumps(
                {
                    "valid": True,
                    "dataset_id": dataset.dataset_id,
                    "case_count": len(dataset.cases),
                    "market_index_count": len(dataset.main_market_indices),
                    "industry_proxy_count": len(dataset.industry_proxy_catalog),
                    "research_only": True,
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0
    if args.command == "regime" and args.regime_command == "capture":
        token = os.environ.get("TUSHARE_TOKEN", "")
        if not token:
            print(
                json.dumps({"captured": False, "error": "TUSHARE_TOKEN is not configured"}),
                file=sys.stderr,
            )
            return 1
        try:
            dataset = load_market_regime_dataset(args.dataset)
            panel = capture_regime_panel(TushareHttpAdapter(token), dataset)
            panel_path = write_regime_panel(panel)
            validated = validate_regime_panel(panel_path)
        except (OSError, RuntimeError, TypeError, ValueError, json.JSONDecodeError) as exc:
            print(json.dumps({"captured": False, "error": str(exc)}), file=sys.stderr)
            return 1
        print(
            json.dumps(
                {
                    "captured": True,
                    "panel_id": validated.panel_id,
                    "panel_hash": validated.panel_hash,
                    "path": validated.path.as_posix(),
                    "series_count": len(validated.panel.series),
                    "historical_vintage": validated.panel.historical_vintage,
                    "provider_verified": False,
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0
    if args.command == "regime" and args.regime_command == "evaluate":
        try:
            dataset = load_market_regime_dataset(args.dataset)
            panel = validate_regime_panel(args.panel)
            result = evaluate_regime_dataset(dataset, panel)
            report_path = write_regime_report(result, panel)
        except (OSError, RuntimeError, TypeError, ValueError, json.JSONDecodeError) as exc:
            print(json.dumps({"valid": False, "error": str(exc)}), file=sys.stderr)
            return 1
        print(
            json.dumps(
                {**result, "report_path": report_path.as_posix()},
                indent=2,
                sort_keys=True,
            )
        )
        return 0
    if args.command == "regime" and args.regime_command == "study-validate":
        try:
            dataset = load_market_regime_dataset(args.dataset)
            method_catalog = load_method_skill_catalog(args.method_catalog)
            registration = load_regime_study_registration(
                args.registration,
                dataset=dataset,
                method_catalog=method_catalog,
            )
            readiness = assess_regime_study_readiness(registration)
        except (OSError, RuntimeError, TypeError, ValueError, json.JSONDecodeError) as exc:
            print(json.dumps({"valid": False, "error": str(exc)}), file=sys.stderr)
            return 1
        print(json.dumps({"valid": True, **readiness}, indent=2, sort_keys=True))
        return 0
    if args.command == "regime" and args.regime_command == "study-evaluate":
        try:
            dataset = load_market_regime_dataset(args.dataset)
            method_catalog = load_method_skill_catalog(args.method_catalog)
            registration = load_regime_study_registration(
                args.registration,
                dataset=dataset,
                method_catalog=method_catalog,
            )
            panel = validate_regime_panel(args.panel)
            report = evaluate_regime_study_baselines(dataset, panel, registration)
            report_path = write_regime_study_baseline_report(
                report,
                panel_id=panel.panel_id,
                registration_id=registration.registration_id,
            )
        except (OSError, RuntimeError, TypeError, ValueError, json.JSONDecodeError) as exc:
            print(json.dumps({"valid": False, "error": str(exc)}), file=sys.stderr)
            return 1
        print(
            json.dumps(
                {
                    "valid": True,
                    "registration_id": registration.registration_id,
                    "panel_id": panel.panel_id,
                    "case_count": report["case_count"],
                    "agent_effectiveness_claim_eligible": False,
                    "report_path": report_path.as_posix(),
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0
    if args.command == "regime" and args.regime_command == "evidence-capture-csrc":
        try:
            path, record = capture_csrc_regime_evidence(
                locator_path=args.locator,
                case_keys=tuple(args.case_key),
                claim_id=args.claim_id,
                lineage_id=args.lineage_id,
            )
        except (OSError, RuntimeError, TypeError, ValueError, json.JSONDecodeError) as exc:
            print(json.dumps({"captured": False, "error": str(exc)}), file=sys.stderr)
            return 1
        print(
            json.dumps(
                {
                    "captured": True,
                    "record_id": record["record_id"],
                    "published_at": record["published_at"],
                    "authority_at": record["authority_at"],
                    "path": path.as_posix(),
                    "licensed_payload_committed": False,
                    "execution_capability": "none",
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0
    if args.command == "regime" and args.regime_command == "evidence-capture-state-council":
        try:
            path, record = capture_state_council_regime_evidence(
                locator_path=args.locator,
                case_keys=tuple(args.case_key),
                claim_id=args.claim_id,
                lineage_id=args.lineage_id,
            )
        except (OSError, RuntimeError, TypeError, ValueError, json.JSONDecodeError) as exc:
            print(json.dumps({"captured": False, "error": str(exc)}), file=sys.stderr)
            return 1
        print(
            json.dumps(
                {
                    "captured": True,
                    "record_id": record["record_id"],
                    "published_at": record["published_at"],
                    "authority_at": record["authority_at"],
                    "path": path.as_posix(),
                    "licensed_payload_committed": False,
                    "execution_capability": "none",
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0
    if args.command == "regime" and args.regime_command == "evidence-capture-nbs":
        try:
            path, record = capture_nbs_macro_vintage(
                locator_path=args.locator,
                case_keys=tuple(args.case_key),
                claim_id=args.claim_id,
                lineage_id=args.lineage_id,
            )
        except (OSError, RuntimeError, TypeError, ValueError, json.JSONDecodeError) as exc:
            print(json.dumps({"captured": False, "error": str(exc)}), file=sys.stderr)
            return 1
        print(
            json.dumps(
                {
                    "captured": True,
                    "record_id": record["record_id"],
                    "published_at": record["published_at"],
                    "authority_at": record["authority_at"],
                    "path": path.as_posix(),
                    "licensed_payload_committed": False,
                    "execution_capability": "none",
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0
    if args.command == "regime" and args.regime_command in {
        "evidence-manifest",
        "evidence-qualify",
        "evidence-qualify-modeled",
    }:
        try:
            dataset = load_market_regime_dataset(args.dataset)
            method_catalog = load_method_skill_catalog(args.method_catalog)
            registration = load_regime_study_registration(
                args.registration,
                dataset=dataset,
                method_catalog=method_catalog,
            )
            panel = validate_regime_panel(args.panel)
            if args.regime_command == "evidence-manifest":
                records = tuple(load_regime_evidence_record(path) for path in args.record)
                manifest = RegimeEvidenceManifest.build(
                    dataset_id=dataset.dataset_id,
                    dataset_hash=dataset.dataset_hash,
                    registration_id=registration.registration_id,
                    registration_hash=registration.registration_hash,
                    panel_id=panel.panel_id,
                    panel_hash=panel.panel_hash,
                    outcomes_opened=registration.outcomes_opened,
                    records=records,
                )
                manifest_path = write_regime_evidence_manifest(manifest)
                result = {
                    "valid": True,
                    "manifest_id": manifest.manifest_id,
                    "record_count": len(records),
                    "path": manifest_path.as_posix(),
                    "execution_capability": "none",
                }
            elif args.regime_command == "evidence-qualify":
                manifest = load_regime_evidence_manifest(
                    args.manifest,
                    dataset=dataset,
                    validated_panel=panel,
                    registration=registration,
                )
                report = qualify_regime_evidence(dataset, panel, registration, manifest)
                report_path = write_regime_evidence_qualification_report(report)
                result = {
                    "valid": True,
                    "report_id": report["report_id"],
                    "case_count": report["case_count"],
                    "all_source_requirements_ready": report["all_source_requirements_ready"],
                    "diagnostic_agent_run_eligible": report["diagnostic_agent_run_eligible"],
                    "agent_effectiveness_claim_eligible": report[
                        "agent_effectiveness_claim_eligible"
                    ],
                    "path": report_path.as_posix(),
                    "execution_capability": "none",
                }
            else:
                manifest = load_regime_evidence_manifest(
                    args.manifest,
                    dataset=dataset,
                    validated_panel=panel,
                    registration=registration,
                )
                strict_qualification = load_qualification_report(args.strict_qualification)
                policy = load_regime_modeled_pit_policy(args.policy)
                report = qualify_regime_evidence_modeled_pit(
                    dataset,
                    panel,
                    registration,
                    manifest,
                    strict_qualification,
                    policy,
                )
                report_path = write_regime_modeled_pit_qualification_report(report)
                result = {
                    "valid": True,
                    "report_id": report["report_id"],
                    "policy_id": report["policy_id"],
                    "checkpoint_count": report["checkpoint_count"],
                    "eligible_checkpoint_count": report["eligible_checkpoint_count"],
                    "all_checkpoints_modeled_ready": report["all_checkpoints_modeled_ready"],
                    "strict_pit_eligible": False,
                    "inference_eligible": False,
                    "path": report_path.as_posix(),
                    "execution_capability": "none",
                }
        except (OSError, RuntimeError, TypeError, ValueError, json.JSONDecodeError) as exc:
            print(json.dumps({"valid": False, "error": str(exc)}), file=sys.stderr)
            return 1
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0
    if args.command == "regime" and args.regime_command == "evidence-audit-publisher-archives":
        try:
            dataset = load_market_regime_dataset(args.dataset)
            method_catalog = load_method_skill_catalog(args.method_catalog)
            registration = load_regime_study_registration(
                args.registration,
                dataset=dataset,
                method_catalog=method_catalog,
            )
            panel = validate_regime_panel(args.panel)
            manifest = load_regime_evidence_manifest(
                args.manifest,
                dataset=dataset,
                validated_panel=panel,
                registration=registration,
            )
            qualification = load_qualification_report(args.qualification)
            report = audit_publisher_archive_recovery(
                manifest,
                registration,
                qualification,
                case_keys=None if args.case_key is None else tuple(args.case_key),
                max_lookups=args.max_lookups,
            )
            path = write_publisher_archive_recovery_report(report)
        except (OSError, RuntimeError, TypeError, ValueError, json.JSONDecodeError) as exc:
            print(json.dumps({"audited": False, "error": str(exc)}), file=sys.stderr)
            return 1
        print(
            json.dumps(
                {
                    "audited": True,
                    "report_id": report["report_id"],
                    "checkpoint_count": report["checkpoint_count"],
                    "found_count": report["found_count"],
                    "not_found_count": report["not_found_count"],
                    "source_error_count": report["source_error_count"],
                    "path": path.as_posix(),
                    "candidate_only": True,
                    "execution_capability": "none",
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0
    if args.command == "regime" and args.regime_command == "evidence-capture-publisher-archive":
        try:
            original = load_regime_evidence_record(args.record)
            locator = load_internet_archive_locator(args.locator)
            supersedes = (
                None
                if args.supersedes_record is None
                else load_regime_evidence_record(args.supersedes_record)
            )
            snapshot = recover_publisher_archive_snapshot(
                original,
                locator,
                not_after=args.not_after,
                supersedes=supersedes,
            )
            record = snapshot.record
            path = write_regime_evidence_record(record)
            document_path = write_publisher_archive_research_document(snapshot)
        except (OSError, RuntimeError, TypeError, ValueError, json.JSONDecodeError) as exc:
            print(json.dumps({"captured": False, "error": str(exc)}), file=sys.stderr)
            return 1
        print(
            json.dumps(
                {
                    "captured": True,
                    "record_id": record.record_id,
                    "authority_kind": record.authority_kind.value,
                    "authority_at": record.to_dict()["authority_at"],
                    "path": path.as_posix(),
                    "research_document_path": document_path.as_posix(),
                    "licensed_payload_committed": False,
                    "execution_capability": "none",
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
                    provider_profile_path=args.provider_profile,
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
    if args.command == "agent" and args.agent_command == "study-run-ensemble":
        try:
            from market_impact_agent.agent_ensemble_runner import (
                run_agent_ensemble_bundle,
            )

            result = asyncio.run(
                run_agent_ensemble_bundle(
                    registration_path=args.registration,
                    exposure_registry_path=args.exposure_registry,
                    evidence_pack_path=args.evidence_pack,
                    evidence_documents_path=args.evidence_documents,
                    pattern_pack_paths=tuple(args.pattern_packs),
                    ensemble_run_id=args.ensemble_run_id,
                    skill_root=args.skill_root,
                    state_root=args.state_root,
                    ensemble_state_root=args.ensemble_state_root,
                )
            )
        except ModuleNotFoundError as exc:
            if exc.name != "mcp":
                raise
            print(
                json.dumps(
                    {
                        "completed": False,
                        "error": (
                            "Agent execution requires the optional dependency group; "
                            "install market-impact-agent[agent]"
                        ),
                    }
                ),
                file=sys.stderr,
            )
            return 1
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
        return 0
    if args.command == "agent" and args.agent_command == "method-skill-ablation-run":
        try:
            from market_impact_agent.method_skills import (
                MethodRoutingContext,
                load_method_evidence_declaration,
            )
            from market_impact_agent.paired_skill_ablation_runner import (
                run_paired_method_skill_ablation,
            )

            requested_usd = cast(Decimal, args.max_total_cost_usd)
            requested_microusd = requested_usd * Decimal("1000000")
            if (
                not requested_usd.is_finite()
                or requested_microusd != requested_microusd.to_integral_value()
                or not Decimal("0") < requested_usd <= Decimal("10")
            ):
                raise ValueError(
                    "max-total-cost-usd must be positive, at most 10, and use at most "
                    "six decimal places"
                )
            evidence_declaration = load_method_evidence_declaration(
                args.method_evidence_declaration
            )
            result = asyncio.run(
                run_paired_method_skill_ablation(
                    method_catalog_path=args.method_catalog,
                    method_evidence_declaration_path=args.method_evidence_declaration,
                    provider_profile_path=args.provider_profile,
                    evidence_pack_path=args.evidence_pack,
                    evidence_documents_path=args.evidence_documents,
                    pattern_pack_paths=tuple(args.pattern_packs),
                    experiment_id=args.experiment_id,
                    treatment_skill=args.treatment_skill,
                    routing_context=MethodRoutingContext(
                        market_state=args.market_state,
                        narrative_salience=args.narrative_salience,
                        analysis_needs=tuple(args.analysis_needs),
                        available_evidence=evidence_declaration.available_evidence,
                        outcomes_opened=args.outcomes_opened,
                    ),
                    skill_root=args.skill_root,
                    state_root=args.state_root,
                    max_total_cost_microusd=int(requested_microusd),
                    eligible_horizon_sessions=args.eligible_horizon_sessions,
                )
            )
        except ModuleNotFoundError as exc:
            if exc.name != "mcp":
                raise
            print(
                json.dumps(
                    {
                        "completed": False,
                        "error": (
                            "Agent execution requires the optional dependency group; "
                            "install market-impact-agent[agent]"
                        ),
                    }
                ),
                file=sys.stderr,
            )
            return 1
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
        return 0
    if args.command == "agent" and args.agent_command == "method-ablation-run":
        try:
            from market_impact_agent.method_ablation_runner import (
                run_method_ablation_bundle,
            )

            result = asyncio.run(
                run_method_ablation_bundle(
                    ablation_registration_path=args.ablation_registration,
                    parent_registration_path=args.parent_registration,
                    exposure_registry_path=args.exposure_registry,
                    method_catalog_path=args.method_catalog,
                    provider_profile_path=args.provider_profile,
                    evidence_pack_path=args.evidence_pack,
                    evidence_documents_path=args.evidence_documents,
                    pattern_pack_paths=tuple(args.pattern_packs),
                    experiment_id=args.experiment_id,
                    skill_root=args.skill_root,
                    state_root=args.state_root,
                )
            )
        except ModuleNotFoundError as exc:
            if exc.name != "mcp":
                raise
            print(
                json.dumps(
                    {
                        "completed": False,
                        "error": (
                            "Agent execution requires the optional dependency group; "
                            "install market-impact-agent[agent]"
                        ),
                    }
                ),
                file=sys.stderr,
            )
            return 1
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
        return 0
    if args.command == "agent" and args.agent_command == "method-benchmark-validate":
        try:
            result = validate_method_quality_benchmark(
                registration_path=args.registration,
                method_catalog_path=args.method_catalog,
                provider_profile_path=args.provider_profile,
                evaluation_specification_path=args.evaluation_specification,
                historical_manifest_path=args.historical_manifest,
                evidence_pack_path=args.evidence_pack,
                evidence_documents_path=args.evidence_documents,
                masked_input_manifest_path=args.masked_input_manifest,
                masked_evidence_pack_path=args.masked_evidence_pack,
                masked_evidence_documents_path=args.masked_evidence_documents,
                pattern_pack_paths=tuple(args.pattern_packs),
                masked_pattern_pack_paths=tuple(args.masked_pattern_packs),
                skill_root=args.skill_root,
            )
        except (KeyError, OSError, TypeError, ValueError, json.JSONDecodeError) as exc:
            print(
                json.dumps({"valid": False, "error": f"{type(exc).__name__}: {exc}"}),
                file=sys.stderr,
            )
            return 1
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0 if result["valid"] else 1
    if args.command == "agent" and args.agent_command == "method-development-run":
        try:
            from market_impact_agent.method_development_runner import (
                run_method_development_state,
            )

            result = asyncio.run(
                run_method_development_state(
                    case_path=args.case,
                    benchmark_registration_path=args.benchmark_registration,
                    evaluation_specification_path=args.evaluation_specification,
                    method_catalog_path=args.method_catalog,
                    provider_profile_path=args.provider_profile,
                    state_id=args.state,
                    evidence_pack_path=args.evidence_pack,
                    evidence_documents_path=args.evidence_documents,
                    pattern_pack_paths=tuple(args.pattern_packs),
                    backtest_request_path=args.backtest_request,
                    experiment_id=args.experiment_id,
                    skill_root=args.skill_root,
                    state_root=args.state_root,
                )
            )
        except ModuleNotFoundError as exc:
            if exc.name != "mcp":
                raise
            print(
                json.dumps(
                    {
                        "completed": False,
                        "error": (
                            "Agent execution requires the optional dependency group; "
                            "install market-impact-agent[agent]"
                        ),
                    }
                ),
                file=sys.stderr,
            )
            return 1
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
        return 0
    if args.command == "agent" and args.agent_command == "method-development-evaluate":
        try:
            from market_impact_agent.method_development_evaluation import (
                evaluate_method_development_case,
            )

            result = evaluate_method_development_case(
                case_path=args.case,
                attack_report_path=args.attack_report,
                recovery_report_path=args.recovery_report,
                attack_backtest_request_path=args.attack_backtest_request,
                recovery_backtest_request_path=args.recovery_backtest_request,
                attack_data_snapshot_path=args.attack_data_snapshot,
                recovery_data_snapshot_path=args.recovery_data_snapshot,
                evaluation_id=args.evaluation_id,
                state_root=args.state_root,
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
        return 0
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
