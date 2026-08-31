from __future__ import annotations

import json
import os
import sqlite3
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Protocol, cast

from market_impact_agent.agent_contracts import canonical_hash, canonical_json_bytes
from market_impact_agent.domain import require_aware
from market_impact_agent.event_impact_triage import (
    EventImpactTriageCandidateSet,
    EventImpactTriageProposal,
)
from market_impact_agent.event_impact_triage_evaluation import (
    TRIAGE_COMPARISON_METRICS,
    EventImpactTriageLabelSet,
    TriageArmScore,
    TriageLabelExposure,
    score_event_impact_triage_proposal,
)
from market_impact_agent.event_impact_triage_runtime import TriageComparisonArm
from market_impact_agent.event_impact_triage_work import (
    EventImpactTriageWorkManifest,
    TriageCandidateDigest,
    TriageClusterPartition,
)
from market_impact_agent.event_impact_triage_work_runtime import (
    EventImpactTriageWorkExecutionPlan,
    EventImpactTriageWorkRunAuthorityReceipt,
    EventImpactTriageWorkRunEvidence,
    EventImpactTriageWorkRunner,
)

TRIAGE_WORK_COMPARISON_REGISTRATION_SCHEMA = (
    "market-impact.event-impact-triage-work-comparison-registration.v1"
)
TRIAGE_WORK_COMPARISON_REPORT_SCHEMA = "market-impact.event-impact-triage-work-comparison-report.v1"

_TRIAGE_WORK_PROMOTION_ONLY_BLOCKERS = frozenset(
    {
        "operator_exposed_batch_cannot_promote",
        "second_pristine_blind_batch_required",
    }
)
_TRIAGE_WORK_SEMANTIC_BLOCKERS = frozenset(
    {
        "aggregate_model_cost_exceeded",
        "treatment_has_unsupported_material_route",
        "treatment_missed_must_catch_eligible",
        "treatment_worse_checkpoint_eligibility",
        "treatment_worse_impact_routing",
    }
)
TRIAGE_WORK_COMPARISON_BLOCKERS = tuple(
    sorted(_TRIAGE_WORK_PROMOTION_ONLY_BLOCKERS | _TRIAGE_WORK_SEMANTIC_BLOCKERS)
)


class TriageWorkComparisonRunAuthority(Protocol):
    def authoritative_completed_work_run_receipt(
        self,
        *,
        candidate_set: EventImpactTriageCandidateSet,
        work_manifest: EventImpactTriageWorkManifest,
        digests: tuple[TriageCandidateDigest, ...],
        partition: TriageClusterPartition,
        proposal: EventImpactTriageProposal,
        run_evidence: EventImpactTriageWorkRunEvidence,
    ) -> EventImpactTriageWorkRunAuthorityReceipt: ...


class TriageWorkComparisonRegistrationAuthority(Protocol):
    def assert_authoritative_registration(
        self, registration: EventImpactTriageWorkComparisonRegistration
    ) -> datetime: ...


class TriageWorkComparisonReportAuthority(Protocol):
    def assert_authoritative_report(
        self,
        *,
        report: EventImpactTriageWorkComparisonReport,
        registration: EventImpactTriageWorkComparisonRegistration,
        candidate_set: EventImpactTriageCandidateSet,
        label_set: EventImpactTriageLabelSet,
        work_manifest: EventImpactTriageWorkManifest,
        baseline: TriageWorkArmOutcome,
        treatment: TriageWorkArmOutcome,
        baseline_authority: TriageWorkComparisonRunAuthority,
        treatment_authority: TriageWorkComparisonRunAuthority,
    ) -> None: ...


@dataclass(frozen=True, slots=True)
class EventImpactTriageWorkComparisonRegistration:
    comparison_id: str
    candidate_set_id: str
    candidate_set_hash: str
    label_set_id: str
    work_manifest_id: str
    work_manifest_hash: str
    baseline_plan_id: str
    treatment_plan_id: str
    registration_id: str
    checkpoint_key: str
    checkpoint_contract_hash: str
    model_profile_id: str
    registered_at: datetime
    metric_ids: tuple[str, ...]
    max_must_catch_false_negatives: int
    max_unsupported_material_routes: int
    require_treatment_not_worse_eligibility: bool
    require_treatment_not_worse_routing: bool
    max_aggregate_estimated_cost_microusd: int
    second_pristine_blind_batch_required: bool
    labels_hidden_from_arms: bool
    historical_pit_claim: bool = False
    strategy_or_execution_authority: bool = False
    schema_version: str = TRIAGE_WORK_COMPARISON_REGISTRATION_SCHEMA

    def __post_init__(self) -> None:
        if self.schema_version != TRIAGE_WORK_COMPARISON_REGISTRATION_SCHEMA:
            raise ValueError("unsupported Triage Work Comparison Registration schema")
        for value, prefix, label in (
            (
                self.candidate_set_id,
                "event-impact-triage-candidate-set-",
                "work comparison Candidate Set",
            ),
            (
                self.label_set_id,
                "event-impact-triage-label-set-",
                "work comparison Label Set",
            ),
            (
                self.work_manifest_id,
                "event-impact-triage-work-manifest-",
                "work comparison Manifest",
            ),
            (
                self.baseline_plan_id,
                "event-impact-triage-work-execution-plan-",
                "work comparison baseline plan",
            ),
            (
                self.treatment_plan_id,
                "event-impact-triage-work-execution-plan-",
                "work comparison treatment plan",
            ),
            (
                self.registration_id,
                "prospective-diagnostic-registration-",
                "work comparison registration",
            ),
        ):
            _prefixed_hash(value, prefix, label)
        _sha256(self.candidate_set_hash, "work comparison Candidate Set hash")
        _sha256(self.work_manifest_hash, "work comparison Manifest hash")
        _sha256(self.checkpoint_contract_hash, "work comparison checkpoint hash")
        _trimmed(self.checkpoint_key, "work comparison checkpoint_key")
        _prefixed_hash(
            self.model_profile_id,
            "model-provider-",
            "work comparison model_profile_id",
        )
        _strict_utc(self.registered_at, "work comparison registered_at")
        if self.metric_ids != TRIAGE_COMPARISON_METRICS:
            raise ValueError("triage work comparison metrics are not the frozen v1 set")
        if self.max_must_catch_false_negatives != 0:
            raise ValueError("triage work comparison cannot tolerate a must-catch false negative")
        if self.max_unsupported_material_routes != 0:
            raise ValueError("triage work comparison cannot promote unsupported material routes")
        if not (
            self.require_treatment_not_worse_eligibility
            and self.require_treatment_not_worse_routing
            and self.second_pristine_blind_batch_required
            and self.labels_hidden_from_arms
        ):
            raise ValueError("triage work comparison must retain all fail-closed gates")
        if self.max_aggregate_estimated_cost_microusd < 0:
            raise ValueError("triage work comparison cost cap must be non-negative")
        if self.historical_pit_claim or self.strategy_or_execution_authority:
            raise ValueError("triage work comparison cannot grant PIT, strategy, or execution")
        if self.comparison_id != self.expected_comparison_id:
            raise ValueError("Triage Work Comparison Registration ID does not match content")

    @property
    def expected_comparison_id(self) -> str:
        return f"event-impact-triage-work-comparison-{canonical_hash(self.core_dict())}"

    def core_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "candidate_set_id": self.candidate_set_id,
            "candidate_set_hash": self.candidate_set_hash,
            "label_set_id": self.label_set_id,
            "work_manifest_id": self.work_manifest_id,
            "work_manifest_hash": self.work_manifest_hash,
            "baseline_plan_id": self.baseline_plan_id,
            "treatment_plan_id": self.treatment_plan_id,
            "registration_id": self.registration_id,
            "checkpoint_key": self.checkpoint_key,
            "checkpoint_contract_hash": self.checkpoint_contract_hash,
            "model_profile_id": self.model_profile_id,
            "registered_at": _timestamp(self.registered_at),
            "metric_ids": list(self.metric_ids),
            "max_must_catch_false_negatives": self.max_must_catch_false_negatives,
            "max_unsupported_material_routes": self.max_unsupported_material_routes,
            "require_treatment_not_worse_eligibility": (
                self.require_treatment_not_worse_eligibility
            ),
            "require_treatment_not_worse_routing": self.require_treatment_not_worse_routing,
            "max_aggregate_estimated_cost_microusd": (self.max_aggregate_estimated_cost_microusd),
            "second_pristine_blind_batch_required": self.second_pristine_blind_batch_required,
            "labels_hidden_from_arms": self.labels_hidden_from_arms,
            "historical_pit_claim": self.historical_pit_claim,
            "strategy_or_execution_authority": self.strategy_or_execution_authority,
        }

    def to_dict(self) -> dict[str, object]:
        return {**self.core_dict(), "comparison_id": self.comparison_id}

    @classmethod
    def build(
        cls,
        *,
        candidate_set: EventImpactTriageCandidateSet,
        label_set: EventImpactTriageLabelSet,
        work_manifest: EventImpactTriageWorkManifest,
        baseline_plan: EventImpactTriageWorkExecutionPlan,
        treatment_plan: EventImpactTriageWorkExecutionPlan,
        registered_at: datetime,
    ) -> EventImpactTriageWorkComparisonRegistration:
        work_manifest.validate_against(candidate_set)
        candidate_hash = canonical_hash(candidate_set.to_dict())
        manifest_hash = canonical_hash(work_manifest.to_dict())
        if label_set.candidate_set_id != candidate_set.candidate_set_id:
            raise ValueError("triage work comparison Label Set belongs to another Candidate Set")
        if label_set.sealed_at < candidate_set.frozen_at:
            raise ValueError("triage work comparison labels predate the frozen Candidate Set")
        if tuple(item.version_id for item in label_set.labels) != tuple(
            sorted(candidate_set.version_ids)
        ):
            raise ValueError("triage work comparison requires every frozen label")
        if (
            baseline_plan.arm is not TriageComparisonArm.BASELINE
            or treatment_plan.arm is not TriageComparisonArm.TREATMENT
        ):
            raise ValueError("triage work comparison plans have incorrect arms")
        if baseline_plan.schema_version != treatment_plan.schema_version:
            raise ValueError("triage work comparison cannot mix Plan schema revisions")
        for plan in (baseline_plan, treatment_plan):
            if (
                plan.candidate_set_id != candidate_set.candidate_set_id
                or plan.candidate_set_hash != candidate_hash
                or plan.work_manifest_id != work_manifest.manifest_id
                or plan.work_manifest_hash != manifest_hash
            ):
                raise ValueError("triage work comparison plan has another frozen input")
        if (
            baseline_plan.registration_id != treatment_plan.registration_id
            or baseline_plan.registration_id != candidate_set.registration_id
            or baseline_plan.checkpoint_key != treatment_plan.checkpoint_key
            or baseline_plan.checkpoint_key != candidate_set.checkpoint_key
            or baseline_plan.checkpoint_contract_hash != treatment_plan.checkpoint_contract_hash
            or baseline_plan.model_provider_profile.to_dict()
            != treatment_plan.model_provider_profile.to_dict()
        ):
            raise ValueError(
                "triage work comparison arms must share registration, checkpoint, and profile"
            )
        _strict_utc(registered_at, "triage work comparison registered_at")
        if registered_at < label_set.sealed_at:
            raise ValueError("triage work comparison must be registered after labels are sealed")
        cost_cap = (
            baseline_plan.max_total_estimated_cost_microusd
            + treatment_plan.max_total_estimated_cost_microusd
        )
        core = {
            "schema_version": TRIAGE_WORK_COMPARISON_REGISTRATION_SCHEMA,
            "candidate_set_id": candidate_set.candidate_set_id,
            "candidate_set_hash": candidate_hash,
            "label_set_id": label_set.label_set_id,
            "work_manifest_id": work_manifest.manifest_id,
            "work_manifest_hash": manifest_hash,
            "baseline_plan_id": baseline_plan.plan_id,
            "treatment_plan_id": treatment_plan.plan_id,
            "registration_id": baseline_plan.registration_id,
            "checkpoint_key": baseline_plan.checkpoint_key,
            "checkpoint_contract_hash": baseline_plan.checkpoint_contract_hash,
            "model_profile_id": baseline_plan.model_provider_profile.profile_id,
            "registered_at": _timestamp(registered_at),
            "metric_ids": list(TRIAGE_COMPARISON_METRICS),
            "max_must_catch_false_negatives": 0,
            "max_unsupported_material_routes": 0,
            "require_treatment_not_worse_eligibility": True,
            "require_treatment_not_worse_routing": True,
            "max_aggregate_estimated_cost_microusd": cost_cap,
            "second_pristine_blind_batch_required": True,
            "labels_hidden_from_arms": True,
            "historical_pit_claim": False,
            "strategy_or_execution_authority": False,
        }
        return cls(
            comparison_id=f"event-impact-triage-work-comparison-{canonical_hash(core)}",
            candidate_set_id=candidate_set.candidate_set_id,
            candidate_set_hash=candidate_hash,
            label_set_id=label_set.label_set_id,
            work_manifest_id=work_manifest.manifest_id,
            work_manifest_hash=manifest_hash,
            baseline_plan_id=baseline_plan.plan_id,
            treatment_plan_id=treatment_plan.plan_id,
            registration_id=baseline_plan.registration_id,
            checkpoint_key=baseline_plan.checkpoint_key,
            checkpoint_contract_hash=baseline_plan.checkpoint_contract_hash,
            model_profile_id=baseline_plan.model_provider_profile.profile_id,
            registered_at=registered_at,
            metric_ids=TRIAGE_COMPARISON_METRICS,
            max_must_catch_false_negatives=0,
            max_unsupported_material_routes=0,
            require_treatment_not_worse_eligibility=True,
            require_treatment_not_worse_routing=True,
            max_aggregate_estimated_cost_microusd=cost_cap,
            second_pristine_blind_batch_required=True,
            labels_hidden_from_arms=True,
        )


class EventImpactTriageWorkComparisonStore:
    """Append-only Harness-clock authority for a work comparison registration."""

    def __init__(self, path: Path, *, clock: Callable[[], datetime] | None = None) -> None:
        if path.is_symlink():
            raise ValueError("triage work comparison store path must not be a symlink")
        path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        os.chmod(path.parent, 0o700)
        self.path = path.resolve()
        self._clock = clock or (lambda: datetime.now(UTC))
        self._initialize()
        os.chmod(self.path, 0o600)

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA journal_mode = WAL")
        connection.execute("PRAGMA synchronous = FULL")
        return connection

    def _initialize(self) -> None:
        with self._connect() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS triage_work_comparison_registrations (
                    binding_key TEXT PRIMARY KEY,
                    comparison_id TEXT NOT NULL UNIQUE,
                    registered_at TEXT NOT NULL,
                    registration_json TEXT NOT NULL
                );
                CREATE TRIGGER IF NOT EXISTS triage_work_comparison_registrations_no_update
                BEFORE UPDATE ON triage_work_comparison_registrations
                BEGIN
                    SELECT RAISE(ABORT, 'work comparison registrations are append-only');
                END;
                CREATE TRIGGER IF NOT EXISTS triage_work_comparison_registrations_no_delete
                BEFORE DELETE ON triage_work_comparison_registrations
                BEGIN
                    SELECT RAISE(ABORT, 'work comparison registrations are append-only');
                END;
                CREATE TABLE IF NOT EXISTS triage_work_comparison_reports (
                    comparison_id TEXT PRIMARY KEY,
                    report_id TEXT NOT NULL UNIQUE,
                    evaluated_at TEXT NOT NULL,
                    report_json TEXT NOT NULL,
                    FOREIGN KEY(comparison_id)
                        REFERENCES triage_work_comparison_registrations(comparison_id)
                );
                CREATE TRIGGER IF NOT EXISTS triage_work_comparison_reports_no_update
                BEFORE UPDATE ON triage_work_comparison_reports
                BEGIN
                    SELECT RAISE(ABORT, 'work comparison reports are append-only');
                END;
                CREATE TRIGGER IF NOT EXISTS triage_work_comparison_reports_no_delete
                BEFORE DELETE ON triage_work_comparison_reports
                BEGIN
                    SELECT RAISE(ABORT, 'work comparison reports are append-only');
                END;
                """
            )

    def register(
        self,
        *,
        candidate_set: EventImpactTriageCandidateSet,
        label_set: EventImpactTriageLabelSet,
        work_manifest: EventImpactTriageWorkManifest,
        baseline_plan: EventImpactTriageWorkExecutionPlan,
        treatment_plan: EventImpactTriageWorkExecutionPlan,
    ) -> EventImpactTriageWorkComparisonRegistration:
        binding_key = canonical_hash(
            {
                "candidate_set_id": candidate_set.candidate_set_id,
                "label_set_id": label_set.label_set_id,
                "work_manifest_id": work_manifest.manifest_id,
                "baseline_plan_id": baseline_plan.plan_id,
                "treatment_plan_id": treatment_plan.plan_id,
            }
        )
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            existing = connection.execute(
                """
                SELECT comparison_id, registered_at, registration_json
                FROM triage_work_comparison_registrations WHERE binding_key = ?
                """,
                (binding_key,),
            ).fetchone()
            if existing is not None:
                registration = EventImpactTriageWorkComparisonRegistration.build(
                    candidate_set=candidate_set,
                    label_set=label_set,
                    work_manifest=work_manifest,
                    baseline_plan=baseline_plan,
                    treatment_plan=treatment_plan,
                    registered_at=_datetime(str(existing["registered_at"])),
                )
                if registration.comparison_id != str(
                    existing["comparison_id"]
                ) or canonical_json_bytes(registration.to_dict()).decode() != str(
                    existing["registration_json"]
                ):
                    raise ValueError("stored triage work comparison registration is inconsistent")
                return registration
            registered_at = self._clock()
            _strict_utc(registered_at, "triage work comparison Harness clock")
            registration = EventImpactTriageWorkComparisonRegistration.build(
                candidate_set=candidate_set,
                label_set=label_set,
                work_manifest=work_manifest,
                baseline_plan=baseline_plan,
                treatment_plan=treatment_plan,
                registered_at=registered_at,
            )
            connection.execute(
                """
                INSERT INTO triage_work_comparison_registrations(
                    binding_key, comparison_id, registered_at, registration_json
                ) VALUES (?, ?, ?, ?)
                """,
                (
                    binding_key,
                    registration.comparison_id,
                    _timestamp(registered_at),
                    canonical_json_bytes(registration.to_dict()).decode(),
                ),
            )
        return registration

    def assert_authoritative_registration(
        self, registration: EventImpactTriageWorkComparisonRegistration
    ) -> datetime:
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT registered_at, registration_json
                FROM triage_work_comparison_registrations WHERE comparison_id = ?
                """,
                (registration.comparison_id,),
            ).fetchone()
        if row is None:
            raise ValueError("triage work comparison is not durably registered")
        if canonical_json_bytes(registration.to_dict()).decode() != str(row["registration_json"]):
            raise ValueError("triage work comparison differs from its durable registration")
        registered_at = _datetime(str(row["registered_at"]))
        if registered_at != registration.registered_at:
            raise ValueError("triage work comparison Harness timestamp differs from registration")
        return registered_at

    def has_registration_for_candidate_set(self, candidate_set_id: str) -> bool:
        """Return whether a Candidate Set is durably bound to comparison-only admission."""

        with self._connect() as connection:
            rows = connection.execute(
                "SELECT registration_json FROM triage_work_comparison_registrations"
            ).fetchall()
        for row in rows:
            decoded = json.loads(str(row["registration_json"]))
            if not isinstance(decoded, dict):
                raise ValueError("stored triage work comparison registration is not an object")
            payload = cast(dict[str, object], decoded)
            if payload.get("candidate_set_id") == candidate_set_id:
                return True
        return False

    def record_report(
        self,
        *,
        report: EventImpactTriageWorkComparisonReport,
        registration: EventImpactTriageWorkComparisonRegistration,
        candidate_set: EventImpactTriageCandidateSet,
        label_set: EventImpactTriageLabelSet,
        work_manifest: EventImpactTriageWorkManifest,
        baseline: TriageWorkArmOutcome,
        treatment: TriageWorkArmOutcome,
        baseline_authority: TriageWorkComparisonRunAuthority,
        treatment_authority: TriageWorkComparisonRunAuthority,
    ) -> EventImpactTriageWorkComparisonReport:
        """Persist only a Report reproduced from the exact durable Runs and Usage."""

        _require_concrete_work_runners(baseline_authority, treatment_authority)
        assert_authoritative_event_impact_triage_work_comparison_report(
            report=report,
            registration=registration,
            candidate_set=candidate_set,
            label_set=label_set,
            work_manifest=work_manifest,
            baseline=baseline,
            treatment=treatment,
            baseline_authority=baseline_authority,
            treatment_authority=treatment_authority,
            registration_authority=self,
        )
        report_json = canonical_json_bytes(report.to_dict()).decode()
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            existing = connection.execute(
                """
                SELECT report_id, evaluated_at, report_json
                FROM triage_work_comparison_reports WHERE comparison_id = ?
                """,
                (registration.comparison_id,),
            ).fetchone()
            if existing is not None:
                if (
                    str(existing["report_id"]) != report.report_id
                    or _datetime(str(existing["evaluated_at"])) != report.evaluated_at
                    or str(existing["report_json"]) != report_json
                ):
                    raise ValueError("stored triage work comparison Report is inconsistent")
                return report
            connection.execute(
                """
                INSERT INTO triage_work_comparison_reports(
                    comparison_id, report_id, evaluated_at, report_json
                ) VALUES (?, ?, ?, ?)
                """,
                (
                    registration.comparison_id,
                    report.report_id,
                    _timestamp(report.evaluated_at),
                    report_json,
                ),
            )
        return report

    def reopen_report(
        self,
        *,
        registration: EventImpactTriageWorkComparisonRegistration,
        candidate_set: EventImpactTriageCandidateSet,
        label_set: EventImpactTriageLabelSet,
        work_manifest: EventImpactTriageWorkManifest,
        baseline: TriageWorkArmOutcome,
        treatment: TriageWorkArmOutcome,
        baseline_authority: TriageWorkComparisonRunAuthority,
        treatment_authority: TriageWorkComparisonRunAuthority,
    ) -> EventImpactTriageWorkComparisonReport | None:
        """Reopen the exact stored Report after replaying registration, Runs, and Usage."""

        _require_concrete_work_runners(baseline_authority, treatment_authority)
        self.assert_authoritative_registration(registration)
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT report_json FROM triage_work_comparison_reports
                WHERE comparison_id = ?
                """,
                (registration.comparison_id,),
            ).fetchone()
        if row is None:
            return None
        decoded = json.loads(str(row["report_json"]))
        report = event_impact_triage_work_comparison_report_from_dict(decoded)
        self.assert_authoritative_report(
            report=report,
            registration=registration,
            candidate_set=candidate_set,
            label_set=label_set,
            work_manifest=work_manifest,
            baseline=baseline,
            treatment=treatment,
            baseline_authority=baseline_authority,
            treatment_authority=treatment_authority,
        )
        return report

    def assert_authoritative_report(
        self,
        *,
        report: EventImpactTriageWorkComparisonReport,
        registration: EventImpactTriageWorkComparisonRegistration,
        candidate_set: EventImpactTriageCandidateSet,
        label_set: EventImpactTriageLabelSet,
        work_manifest: EventImpactTriageWorkManifest,
        baseline: TriageWorkArmOutcome,
        treatment: TriageWorkArmOutcome,
        baseline_authority: TriageWorkComparisonRunAuthority,
        treatment_authority: TriageWorkComparisonRunAuthority,
    ) -> None:
        """Reopen the durable Report and replay both completed Run/Usage authorities."""

        _require_concrete_work_runners(baseline_authority, treatment_authority)
        self.assert_authoritative_registration(registration)
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT report_id, evaluated_at, report_json
                FROM triage_work_comparison_reports WHERE comparison_id = ?
                """,
                (registration.comparison_id,),
            ).fetchone()
        if row is None:
            raise ValueError("triage work comparison Report is not durably recorded")
        if (
            str(row["report_id"]) != report.report_id
            or _datetime(str(row["evaluated_at"])) != report.evaluated_at
            or str(row["report_json"]) != canonical_json_bytes(report.to_dict()).decode()
        ):
            raise ValueError("triage work comparison Report differs from durable authority")
        assert_authoritative_event_impact_triage_work_comparison_report(
            report=report,
            registration=registration,
            candidate_set=candidate_set,
            label_set=label_set,
            work_manifest=work_manifest,
            baseline=baseline,
            treatment=treatment,
            baseline_authority=baseline_authority,
            treatment_authority=treatment_authority,
            registration_authority=self,
        )


def _require_concrete_work_runners(
    baseline_authority: TriageWorkComparisonRunAuthority,
    treatment_authority: TriageWorkComparisonRunAuthority,
) -> None:
    if (
        type(baseline_authority) is not EventImpactTriageWorkRunner
        or type(treatment_authority) is not EventImpactTriageWorkRunner
    ):
        raise TypeError("durable comparison Reports require concrete Work Runner authorities")


@dataclass(frozen=True, slots=True)
class TriageWorkArmOutcome:
    plan: EventImpactTriageWorkExecutionPlan
    work_manifest: EventImpactTriageWorkManifest
    digests: tuple[TriageCandidateDigest, ...]
    partition: TriageClusterPartition
    proposal: EventImpactTriageProposal
    run_evidence: EventImpactTriageWorkRunEvidence

    def __post_init__(self) -> None:
        if (
            self.plan.work_manifest_id != self.work_manifest.manifest_id
            or self.plan.work_manifest_hash != canonical_hash(self.work_manifest.to_dict())
        ):
            raise ValueError("triage work arm outcome belongs to another Manifest")
        if self.proposal.candidate_set_id != self.plan.candidate_set_id:
            raise ValueError("triage work arm outcome belongs to another Candidate Set")
        if self.run_evidence.plan_id != self.plan.plan_id:
            raise ValueError("triage work arm evidence belongs to another plan")

    @property
    def plan_id(self) -> str:
        return self.plan.plan_id

    def core_dict(self) -> dict[str, object]:
        return {
            "plan_id": self.plan.plan_id,
            "work_manifest_id": self.work_manifest.manifest_id,
            "work_manifest_hash": canonical_hash(self.work_manifest.to_dict()),
            "digest_ids": [item.digest_id for item in self.digests],
            "partition_id": self.partition.partition_id,
            "proposal_id": self.proposal.proposal_id,
            "run_evidence_hash": canonical_hash(_work_run_evidence_dict(self.run_evidence)),
        }

    @property
    def outcome_hash(self) -> str:
        return canonical_hash(self.core_dict())

    def to_dict(self) -> dict[str, object]:
        return {**self.core_dict(), "outcome_hash": self.outcome_hash}


@dataclass(frozen=True, slots=True)
class EventImpactTriageWorkComparisonReport:
    report_id: str
    comparison_id: str
    label_set_id: str
    label_exposure: TriageLabelExposure
    baseline_plan_id: str
    treatment_plan_id: str
    baseline_outcome_hash: str
    treatment_outcome_hash: str
    baseline_authority_receipt_hash: str
    treatment_authority_receipt_hash: str
    max_aggregate_estimated_cost_microusd: int
    baseline_score: TriageArmScore
    treatment_score: TriageArmScore
    batch_gate_passed: bool
    promotion_eligible: bool
    blockers: tuple[str, ...]
    evaluated_at: datetime
    historical_pit_claim: bool = False
    strategy_or_execution_authority: bool = False
    schema_version: str = TRIAGE_WORK_COMPARISON_REPORT_SCHEMA

    def __post_init__(self) -> None:
        if self.schema_version != TRIAGE_WORK_COMPARISON_REPORT_SCHEMA:
            raise ValueError("unsupported Triage Work Comparison Report schema")
        _prefixed_hash(
            self.comparison_id,
            "event-impact-triage-work-comparison-",
            "triage work comparison ID",
        )
        _prefixed_hash(self.label_set_id, "event-impact-triage-label-set-", "label set ID")
        _prefixed_hash(
            self.baseline_plan_id,
            "event-impact-triage-work-execution-plan-",
            "baseline plan ID",
        )
        _prefixed_hash(
            self.treatment_plan_id,
            "event-impact-triage-work-execution-plan-",
            "treatment plan ID",
        )
        if self.baseline_plan_id == self.treatment_plan_id:
            raise ValueError("triage work comparison Report requires distinct arm plans")
        for value, label in (
            (self.baseline_outcome_hash, "baseline outcome hash"),
            (self.treatment_outcome_hash, "treatment outcome hash"),
            (self.baseline_authority_receipt_hash, "baseline authority receipt hash"),
            (self.treatment_authority_receipt_hash, "treatment authority receipt hash"),
        ):
            _sha256(value, label)
        if self.baseline_outcome_hash == self.treatment_outcome_hash:
            raise ValueError("triage work comparison Report requires distinct arm outcomes")
        if self.baseline_authority_receipt_hash == self.treatment_authority_receipt_hash:
            raise ValueError("triage work comparison Report requires distinct arm receipts")
        if self.max_aggregate_estimated_cost_microusd < 0:
            raise ValueError("triage work comparison report cost cap must be non-negative")
        _validate_arm_score(self.baseline_score, "baseline")
        _validate_arm_score(self.treatment_score, "treatment")
        if self.baseline_score.candidate_count != self.treatment_score.candidate_count:
            raise ValueError("triage work comparison scores have different candidate counts")
        if self.blockers != tuple(sorted(set(self.blockers))):
            raise ValueError("triage work comparison blockers must be sorted and unique")
        expected_blockers = _comparison_blockers(
            label_exposure=self.label_exposure,
            baseline_score=self.baseline_score,
            treatment_score=self.treatment_score,
            max_aggregate_estimated_cost_microusd=(self.max_aggregate_estimated_cost_microusd),
        )
        if self.blockers != expected_blockers:
            raise ValueError("triage work comparison blockers contradict its scores or gates")
        expected_batch_gate = not bool(set(expected_blockers) & _TRIAGE_WORK_SEMANTIC_BLOCKERS)
        if self.batch_gate_passed is not expected_batch_gate:
            raise ValueError("triage work comparison batch gate contradicts its blockers")
        if self.promotion_eligible:
            raise ValueError("one triage work comparison batch cannot promote a method")
        _strict_utc(self.evaluated_at, "triage work comparison evaluated_at")
        if self.historical_pit_claim or self.strategy_or_execution_authority:
            raise ValueError("triage work comparison report cannot grant authority")
        if self.report_id != self.expected_report_id:
            raise ValueError("Triage Work Comparison Report ID does not match content")

    @property
    def expected_report_id(self) -> str:
        return f"event-impact-triage-work-comparison-report-{canonical_hash(self.core_dict())}"

    def core_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "comparison_id": self.comparison_id,
            "label_set_id": self.label_set_id,
            "label_exposure": self.label_exposure.value,
            "baseline_plan_id": self.baseline_plan_id,
            "treatment_plan_id": self.treatment_plan_id,
            "baseline_outcome_hash": self.baseline_outcome_hash,
            "treatment_outcome_hash": self.treatment_outcome_hash,
            "baseline_authority_receipt_hash": self.baseline_authority_receipt_hash,
            "treatment_authority_receipt_hash": self.treatment_authority_receipt_hash,
            "max_aggregate_estimated_cost_microusd": (self.max_aggregate_estimated_cost_microusd),
            "baseline_score": self.baseline_score.to_dict(),
            "treatment_score": self.treatment_score.to_dict(),
            "batch_gate_passed": self.batch_gate_passed,
            "promotion_eligible": self.promotion_eligible,
            "blockers": list(self.blockers),
            "evaluated_at": _timestamp(self.evaluated_at),
            "historical_pit_claim": self.historical_pit_claim,
            "strategy_or_execution_authority": self.strategy_or_execution_authority,
        }

    def to_dict(self) -> dict[str, object]:
        return {**self.core_dict(), "report_id": self.report_id}


def event_impact_triage_work_comparison_report_from_dict(
    value: object,
) -> EventImpactTriageWorkComparisonReport:
    payload = _object(value, "triage work comparison Report")
    report = EventImpactTriageWorkComparisonReport(
        report_id=_string(payload, "report_id"),
        comparison_id=_string(payload, "comparison_id"),
        label_set_id=_string(payload, "label_set_id"),
        label_exposure=TriageLabelExposure(_string(payload, "label_exposure")),
        baseline_plan_id=_string(payload, "baseline_plan_id"),
        treatment_plan_id=_string(payload, "treatment_plan_id"),
        baseline_outcome_hash=_string(payload, "baseline_outcome_hash"),
        treatment_outcome_hash=_string(payload, "treatment_outcome_hash"),
        baseline_authority_receipt_hash=_string(payload, "baseline_authority_receipt_hash"),
        treatment_authority_receipt_hash=_string(payload, "treatment_authority_receipt_hash"),
        max_aggregate_estimated_cost_microusd=_integer(
            payload, "max_aggregate_estimated_cost_microusd"
        ),
        baseline_score=_arm_score_from_dict(payload.get("baseline_score")),
        treatment_score=_arm_score_from_dict(payload.get("treatment_score")),
        batch_gate_passed=_boolean(payload, "batch_gate_passed"),
        promotion_eligible=_boolean(payload, "promotion_eligible"),
        blockers=_string_tuple(payload.get("blockers"), "blockers"),
        evaluated_at=_datetime(_string(payload, "evaluated_at")),
        historical_pit_claim=_boolean(payload, "historical_pit_claim"),
        strategy_or_execution_authority=_boolean(payload, "strategy_or_execution_authority"),
        schema_version=_string(payload, "schema_version"),
    )
    if report.to_dict() != payload:
        raise ValueError("triage work comparison Report is not canonical")
    return report


def evaluate_event_impact_triage_work_comparison(
    *,
    registration: EventImpactTriageWorkComparisonRegistration,
    candidate_set: EventImpactTriageCandidateSet,
    label_set: EventImpactTriageLabelSet,
    work_manifest: EventImpactTriageWorkManifest,
    baseline: TriageWorkArmOutcome,
    treatment: TriageWorkArmOutcome,
    baseline_authority: TriageWorkComparisonRunAuthority,
    treatment_authority: TriageWorkComparisonRunAuthority,
    registration_authority: TriageWorkComparisonRegistrationAuthority,
    evaluated_at: datetime,
) -> EventImpactTriageWorkComparisonReport:
    _strict_utc(evaluated_at, "triage work comparison evaluated_at")
    authoritative_registered_at = registration_authority.assert_authoritative_registration(
        registration
    )
    if evaluated_at < authoritative_registered_at:
        raise ValueError("triage work comparison cannot be evaluated before registration")
    candidate_hash = canonical_hash(candidate_set.to_dict())
    manifest_hash = canonical_hash(work_manifest.to_dict())
    work_manifest.validate_against(candidate_set)
    if (
        registration.candidate_set_id != candidate_set.candidate_set_id
        or registration.candidate_set_hash != candidate_hash
        or registration.label_set_id != label_set.label_set_id
        or label_set.candidate_set_id != candidate_set.candidate_set_id
        or registration.work_manifest_id != work_manifest.manifest_id
        or registration.work_manifest_hash != manifest_hash
    ):
        raise ValueError("triage work comparison inputs belong to different frozen batches")
    if baseline.plan_id != registration.baseline_plan_id:
        raise ValueError("triage work baseline outcome belongs to another plan")
    if treatment.plan_id != registration.treatment_plan_id:
        raise ValueError("triage work treatment outcome belongs to another plan")
    receipts: list[EventImpactTriageWorkRunAuthorityReceipt] = []
    for outcome, authority in (
        (baseline, baseline_authority),
        (treatment, treatment_authority),
    ):
        if outcome.work_manifest != work_manifest:
            raise ValueError("triage work comparison arm belongs to another Manifest")
        receipt = authority.authoritative_completed_work_run_receipt(
            candidate_set=candidate_set,
            work_manifest=work_manifest,
            digests=outcome.digests,
            partition=outcome.partition,
            proposal=outcome.proposal,
            run_evidence=outcome.run_evidence,
        )
        if receipt.plan_id != outcome.plan_id:
            raise ValueError("triage work authority receipt belongs to another plan")
        if receipt.started_at < authoritative_registered_at:
            raise ValueError("triage work comparison arm started before registration")
        if receipt.total_estimated_cost_microusd > outcome.plan.max_total_estimated_cost_microusd:
            raise ValueError("triage work comparison arm exceeded its frozen cost ceiling")
        receipts.append(receipt)
    if evaluated_at < max(item.finished_at for item in receipts):
        raise ValueError("triage work comparison cannot be evaluated before both arms finish")
    baseline.proposal.validate_against(candidate_set)
    treatment.proposal.validate_against(candidate_set)
    baseline_score = score_event_impact_triage_proposal(
        labels=label_set,
        proposal=baseline.proposal,
        total_estimated_cost_microusd=receipts[0].total_estimated_cost_microusd,
    )
    treatment_score = score_event_impact_triage_proposal(
        labels=label_set,
        proposal=treatment.proposal,
        total_estimated_cost_microusd=receipts[1].total_estimated_cost_microusd,
    )
    ordered_blockers = _comparison_blockers(
        label_exposure=label_set.exposure,
        baseline_score=baseline_score,
        treatment_score=treatment_score,
        max_aggregate_estimated_cost_microusd=(registration.max_aggregate_estimated_cost_microusd),
    )
    batch_gate_passed = not bool(set(ordered_blockers) & _TRIAGE_WORK_SEMANTIC_BLOCKERS)
    core = {
        "schema_version": TRIAGE_WORK_COMPARISON_REPORT_SCHEMA,
        "comparison_id": registration.comparison_id,
        "label_set_id": label_set.label_set_id,
        "label_exposure": label_set.exposure.value,
        "baseline_plan_id": baseline.plan_id,
        "treatment_plan_id": treatment.plan_id,
        "baseline_outcome_hash": baseline.outcome_hash,
        "treatment_outcome_hash": treatment.outcome_hash,
        "baseline_authority_receipt_hash": receipts[0].receipt_hash,
        "treatment_authority_receipt_hash": receipts[1].receipt_hash,
        "max_aggregate_estimated_cost_microusd": (
            registration.max_aggregate_estimated_cost_microusd
        ),
        "baseline_score": baseline_score.to_dict(),
        "treatment_score": treatment_score.to_dict(),
        "batch_gate_passed": batch_gate_passed,
        "promotion_eligible": False,
        "blockers": list(ordered_blockers),
        "evaluated_at": _timestamp(evaluated_at),
        "historical_pit_claim": False,
        "strategy_or_execution_authority": False,
    }
    return EventImpactTriageWorkComparisonReport(
        report_id=f"event-impact-triage-work-comparison-report-{canonical_hash(core)}",
        comparison_id=registration.comparison_id,
        label_set_id=label_set.label_set_id,
        label_exposure=label_set.exposure,
        baseline_plan_id=baseline.plan_id,
        treatment_plan_id=treatment.plan_id,
        baseline_outcome_hash=baseline.outcome_hash,
        treatment_outcome_hash=treatment.outcome_hash,
        baseline_authority_receipt_hash=receipts[0].receipt_hash,
        treatment_authority_receipt_hash=receipts[1].receipt_hash,
        max_aggregate_estimated_cost_microusd=(registration.max_aggregate_estimated_cost_microusd),
        baseline_score=baseline_score,
        treatment_score=treatment_score,
        batch_gate_passed=batch_gate_passed,
        promotion_eligible=False,
        blockers=ordered_blockers,
        evaluated_at=evaluated_at,
    )


def assert_authoritative_event_impact_triage_work_comparison_report(
    *,
    report: EventImpactTriageWorkComparisonReport,
    registration: EventImpactTriageWorkComparisonRegistration,
    candidate_set: EventImpactTriageCandidateSet,
    label_set: EventImpactTriageLabelSet,
    work_manifest: EventImpactTriageWorkManifest,
    baseline: TriageWorkArmOutcome,
    treatment: TriageWorkArmOutcome,
    baseline_authority: TriageWorkComparisonRunAuthority,
    treatment_authority: TriageWorkComparisonRunAuthority,
    registration_authority: TriageWorkComparisonRegistrationAuthority,
) -> None:
    """Fully replay authority and require byte-identical evaluator output."""

    authoritative = evaluate_event_impact_triage_work_comparison(
        registration=registration,
        candidate_set=candidate_set,
        label_set=label_set,
        work_manifest=work_manifest,
        baseline=baseline,
        treatment=treatment,
        baseline_authority=baseline_authority,
        treatment_authority=treatment_authority,
        registration_authority=registration_authority,
        evaluated_at=report.evaluated_at,
    )
    if canonical_json_bytes(authoritative.to_dict()) != canonical_json_bytes(report.to_dict()):
        raise ValueError("triage work comparison Report is not authoritative evaluator output")


def _comparison_blockers(
    *,
    label_exposure: TriageLabelExposure,
    baseline_score: TriageArmScore,
    treatment_score: TriageArmScore,
    max_aggregate_estimated_cost_microusd: int,
) -> tuple[str, ...]:
    blockers = {"second_pristine_blind_batch_required"}
    if label_exposure is not TriageLabelExposure.PRISTINE_BLIND:
        blockers.add("operator_exposed_batch_cannot_promote")
    if treatment_score.must_catch_false_negatives > 0:
        blockers.add("treatment_missed_must_catch_eligible")
    if treatment_score.unsupported_material_routes > 0:
        blockers.add("treatment_has_unsupported_material_route")
    if (
        treatment_score.checkpoint_eligibility_accuracy
        < baseline_score.checkpoint_eligibility_accuracy
    ):
        blockers.add("treatment_worse_checkpoint_eligibility")
    if treatment_score.route_accuracy < baseline_score.route_accuracy:
        blockers.add("treatment_worse_impact_routing")
    if (
        baseline_score.total_estimated_cost_microusd + treatment_score.total_estimated_cost_microusd
        > max_aggregate_estimated_cost_microusd
    ):
        blockers.add("aggregate_model_cost_exceeded")
    return tuple(sorted(blockers))


def _validate_arm_score(score: TriageArmScore, arm: str) -> None:
    if score.candidate_count < 1 or score.classified_count != score.candidate_count:
        raise ValueError(f"triage work {arm} score requires complete candidate coverage")
    bounded_counts = (
        score.checkpoint_eligibility_correct,
        score.eligible_false_negatives,
        score.eligible_false_positives,
        score.must_catch_false_negatives,
        score.needs_review_correct,
        score.needs_review_label_count,
        score.route_correct,
        score.unsupported_material_routes,
    )
    if any(value < 0 or value > score.candidate_count for value in bounded_counts):
        raise ValueError(f"triage work {arm} score counts are outside candidate coverage")
    if score.needs_review_correct > score.needs_review_label_count:
        raise ValueError(f"triage work {arm} needs-review score is contradictory")
    if score.total_estimated_cost_microusd < 0:
        raise ValueError(f"triage work {arm} cost must be non-negative")


def _work_run_evidence_dict(evidence: EventImpactTriageWorkRunEvidence) -> dict[str, object]:
    return {
        "plan_id": evidence.plan_id,
        "members": [
            {
                "phase": item.phase.value,
                "unit_id": item.unit_id,
                "role": item.role.value,
                "run_id": item.run_id,
                "status": item.status.value,
                "terminal_artifact_hash": item.terminal_artifact_hash,
                "execution_binding_hash": item.execution_binding_hash,
                "metrics": item.metrics.to_dict(),
                "metrics_hash": item.metrics_hash,
                "validation_event_hash": item.validation_event_hash,
                "output": item.output,
            }
            for item in evidence.members
        ],
        "usage_ledger_hash": evidence.usage_ledger_hash,
    }


def _arm_score_from_dict(value: object) -> TriageArmScore:
    payload = _object(value, "triage work arm score")
    score = TriageArmScore(
        candidate_count=_integer(payload, "candidate_count"),
        classified_count=_integer(payload, "classified_count"),
        checkpoint_eligibility_correct=_integer(payload, "checkpoint_eligibility_correct"),
        eligible_false_negatives=_integer(payload, "eligible_false_negatives"),
        eligible_false_positives=_integer(payload, "eligible_false_positives"),
        must_catch_false_negatives=_integer(payload, "must_catch_false_negatives"),
        needs_review_correct=_integer(payload, "needs_review_correct"),
        needs_review_label_count=_integer(payload, "needs_review_label_count"),
        route_correct=_integer(payload, "route_correct"),
        unsupported_material_routes=_integer(payload, "unsupported_material_routes"),
        total_estimated_cost_microusd=_integer(payload, "estimated_cost_microusd"),
    )
    if score.to_dict() != payload:
        raise ValueError("triage work arm score is not canonical")
    return score


def _object(value: object, label: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise TypeError(f"{label} must be an object with string keys")
    raw = cast(dict[object, object], value)
    if any(not isinstance(key, str) for key in raw):
        raise TypeError(f"{label} must be an object with string keys")
    return cast(dict[str, object], raw)


def _string(payload: dict[str, object], name: str) -> str:
    value = payload.get(name)
    if not isinstance(value, str) or not value or value != value.strip():
        raise TypeError(f"{name} must be a non-empty trimmed string")
    return value


def _integer(payload: dict[str, object], name: str) -> int:
    value = payload.get(name)
    if not isinstance(value, int) or isinstance(value, bool):
        raise TypeError(f"{name} must be an integer")
    return value


def _boolean(payload: dict[str, object], name: str) -> bool:
    value = payload.get(name)
    if not isinstance(value, bool):
        raise TypeError(f"{name} must be a boolean")
    return value


def _string_tuple(value: object, name: str) -> tuple[str, ...]:
    if not isinstance(value, list):
        raise TypeError(f"{name} must be an array of strings")
    raw = cast(list[object], value)
    if any(not isinstance(item, str) for item in raw):
        raise TypeError(f"{name} must be an array of strings")
    return tuple(cast(list[str], raw))


def _strict_utc(value: datetime, label: str) -> None:
    require_aware(value, label)
    if value.utcoffset() != UTC.utcoffset(value):
        raise ValueError(f"{label} must use UTC")


def _timestamp(value: datetime) -> str:
    return value.isoformat().replace("+00:00", "Z")


def _datetime(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    _strict_utc(parsed, "triage work comparison stored timestamp")
    return parsed


def _trimmed(value: str, label: str) -> None:
    if not value or value != value.strip():
        raise ValueError(f"{label} must be non-empty trimmed text")


def _sha256(value: str, label: str) -> None:
    if len(value) != 64 or any(item not in "0123456789abcdef" for item in value):
        raise ValueError(f"{label} must be a lowercase SHA-256")


def _prefixed_hash(value: str, prefix: str, label: str) -> None:
    if not value.startswith(prefix):
        raise ValueError(f"{label} must start with {prefix}")
    _sha256(value[len(prefix) :], label)
