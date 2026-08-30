from __future__ import annotations

import os
import sqlite3
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from typing import Protocol, cast

from market_impact_agent.agent_contracts import canonical_hash, canonical_json_bytes
from market_impact_agent.domain import require_aware
from market_impact_agent.event_impact_triage import (
    CheckpointEligibility,
    EventImpactTriageCandidateSet,
    EventImpactTriageProposal,
    TriageRoute,
    TriageRunEvidence,
)
from market_impact_agent.event_impact_triage_runtime import (
    EventImpactTriageExecutionPlan,
    TriageComparisonArm,
)

TRIAGE_LABEL_SET_SCHEMA = "market-impact.event-impact-triage-label-set.v1"
TRIAGE_COMPARISON_REGISTRATION_SCHEMA = (
    "market-impact.event-impact-triage-comparison-registration.v1"
)
TRIAGE_COMPARISON_REPORT_SCHEMA = "market-impact.event-impact-triage-comparison-report.v1"

TRIAGE_COMPARISON_METRICS = (
    "candidate_coverage",
    "checkpoint_eligibility_accuracy",
    "eligible_false_negatives",
    "eligible_false_positives",
    "must_catch_false_negatives",
    "needs_review_accuracy",
    "route_accuracy",
    "unsupported_material_routes",
    "estimated_cost_microusd",
)


class TriageLabelExposure(StrEnum):
    PRISTINE_BLIND = "pristine_blind"
    OPERATOR_EXPOSED = "operator_exposed"


class TriageComparisonRunAuthority(Protocol):
    def assert_authoritative_completed_triage_run(
        self,
        *,
        candidate_set: EventImpactTriageCandidateSet,
        proposal: EventImpactTriageProposal,
        run_evidence: TriageRunEvidence,
    ) -> None: ...

    def authoritative_started_at(self, run_evidence: TriageRunEvidence) -> datetime: ...

    def authoritative_finished_at(self, run_evidence: TriageRunEvidence) -> datetime: ...

    def authoritative_total_estimated_cost_microusd(
        self, run_evidence: TriageRunEvidence
    ) -> int: ...


class TriageComparisonRegistrationAuthority(Protocol):
    def assert_authoritative_registration(
        self, registration: EventImpactTriageComparisonRegistration
    ) -> datetime: ...


@dataclass(frozen=True, slots=True)
class TriageGoldLabel:
    version_id: str
    checkpoint_eligibility: CheckpointEligibility
    expected_route: TriageRoute
    must_catch: bool
    material_transmission_expected: bool
    rationale: str

    def __post_init__(self) -> None:
        _prefixed_hash(
            self.version_id,
            "prospective-observation-version-",
            "triage label version_id",
        )
        _trimmed(self.rationale, "triage label rationale")
        if self.checkpoint_eligibility is CheckpointEligibility.ELIGIBLE:
            if self.expected_route is not TriageRoute.CHECKPOINT_CANDIDATE:
                raise ValueError("eligible gold labels must route to checkpoint selection")
        elif self.expected_route is TriageRoute.CHECKPOINT_CANDIDATE:
            raise ValueError("non-eligible gold labels cannot route to checkpoint selection")
        if self.must_catch and self.checkpoint_eligibility is not CheckpointEligibility.ELIGIBLE:
            raise ValueError("must_catch is reserved for clearly eligible labels")
        if (
            self.expected_route is TriageRoute.EVENT_ASSESSMENT
            and not self.material_transmission_expected
        ):
            raise ValueError("EventAssessment gold labels require a material transmission path")

    def to_dict(self) -> dict[str, object]:
        return {
            "version_id": self.version_id,
            "checkpoint_eligibility": self.checkpoint_eligibility.value,
            "expected_route": self.expected_route.value,
            "must_catch": self.must_catch,
            "material_transmission_expected": self.material_transmission_expected,
            "rationale": self.rationale,
        }


@dataclass(frozen=True, slots=True)
class EventImpactTriageLabelSet:
    label_set_id: str
    candidate_set_id: str
    exposure: TriageLabelExposure
    labels: tuple[TriageGoldLabel, ...]
    sealed_at: datetime
    schema_version: str = TRIAGE_LABEL_SET_SCHEMA

    def __post_init__(self) -> None:
        if self.schema_version != TRIAGE_LABEL_SET_SCHEMA:
            raise ValueError("unsupported Event Impact Triage Label Set schema")
        _prefixed_hash(
            self.candidate_set_id,
            "event-impact-triage-candidate-set-",
            "triage label Candidate Set",
        )
        version_ids = tuple(item.version_id for item in self.labels)
        if not version_ids:
            raise ValueError("triage Label Set requires at least one label")
        if version_ids != tuple(sorted(set(version_ids))):
            raise ValueError("triage labels must be sorted and unique")
        _strict_utc(self.sealed_at, "triage labels sealed_at")
        if self.label_set_id != self.expected_label_set_id:
            raise ValueError("Event Impact Triage Label Set ID does not match content")

    @property
    def expected_label_set_id(self) -> str:
        return f"event-impact-triage-label-set-{canonical_hash(self.core_dict())}"

    def core_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "candidate_set_id": self.candidate_set_id,
            "exposure": self.exposure.value,
            "labels": [item.to_dict() for item in self.labels],
            "sealed_at": _timestamp(self.sealed_at),
        }

    def to_dict(self) -> dict[str, object]:
        return {**self.core_dict(), "label_set_id": self.label_set_id}

    @classmethod
    def build(
        cls,
        *,
        candidate_set: EventImpactTriageCandidateSet,
        exposure: TriageLabelExposure,
        labels: tuple[TriageGoldLabel, ...],
        sealed_at: datetime,
    ) -> EventImpactTriageLabelSet:
        ordered = tuple(sorted(labels, key=lambda item: item.version_id))
        if {item.version_id for item in ordered} != set(candidate_set.version_ids):
            raise ValueError("triage Label Set must label every and only frozen candidate")
        _strict_utc(sealed_at, "triage labels sealed_at")
        if sealed_at < candidate_set.frozen_at:
            raise ValueError("triage labels cannot be sealed before the Candidate Set")
        core = {
            "schema_version": TRIAGE_LABEL_SET_SCHEMA,
            "candidate_set_id": candidate_set.candidate_set_id,
            "exposure": exposure.value,
            "labels": [item.to_dict() for item in ordered],
            "sealed_at": _timestamp(sealed_at),
        }
        return cls(
            label_set_id=f"event-impact-triage-label-set-{canonical_hash(core)}",
            candidate_set_id=candidate_set.candidate_set_id,
            exposure=exposure,
            labels=ordered,
            sealed_at=sealed_at,
        )


def event_impact_triage_label_set_from_dict(value: object) -> EventImpactTriageLabelSet:
    payload = _object(value, "Event Impact Triage Label Set")
    expected = {
        "schema_version",
        "label_set_id",
        "candidate_set_id",
        "exposure",
        "labels",
        "sealed_at",
    }
    if set(payload) != expected:
        raise ValueError("Event Impact Triage Label Set fields are invalid")
    label_fields = {
        "version_id",
        "checkpoint_eligibility",
        "expected_route",
        "must_catch",
        "material_transmission_expected",
        "rationale",
    }
    labels: list[TriageGoldLabel] = []
    for raw in _array(payload.get("labels"), "triage labels"):
        item = _object(raw, "triage label")
        if set(item) != label_fields:
            raise ValueError("Event Impact Triage label fields are invalid")
        labels.append(
            TriageGoldLabel(
                version_id=_string(item, "version_id"),
                checkpoint_eligibility=CheckpointEligibility(
                    _string(item, "checkpoint_eligibility")
                ),
                expected_route=TriageRoute(_string(item, "expected_route")),
                must_catch=_boolean(item, "must_catch"),
                material_transmission_expected=_boolean(item, "material_transmission_expected"),
                rationale=_string(item, "rationale"),
            )
        )
    result = EventImpactTriageLabelSet(
        label_set_id=_string(payload, "label_set_id"),
        candidate_set_id=_string(payload, "candidate_set_id"),
        exposure=TriageLabelExposure(_string(payload, "exposure")),
        labels=tuple(labels),
        sealed_at=_datetime(_string(payload, "sealed_at")),
        schema_version=_string(payload, "schema_version"),
    )
    if result.to_dict() != payload:
        raise ValueError("Event Impact Triage Label Set is not canonical")
    return result


@dataclass(frozen=True, slots=True)
class EventImpactTriageComparisonRegistration:
    comparison_id: str
    candidate_set_id: str
    label_set_id: str
    baseline_plan_id: str
    treatment_plan_id: str
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
    schema_version: str = TRIAGE_COMPARISON_REGISTRATION_SCHEMA

    def __post_init__(self) -> None:
        if self.schema_version != TRIAGE_COMPARISON_REGISTRATION_SCHEMA:
            raise ValueError("unsupported Triage Comparison Registration schema")
        for value, prefix, label in (
            (
                self.candidate_set_id,
                "event-impact-triage-candidate-set-",
                "comparison Candidate Set",
            ),
            (self.label_set_id, "event-impact-triage-label-set-", "comparison Label Set"),
            (
                self.baseline_plan_id,
                "event-impact-triage-execution-plan-",
                "baseline plan",
            ),
            (
                self.treatment_plan_id,
                "event-impact-triage-execution-plan-",
                "treatment plan",
            ),
        ):
            _prefixed_hash(value, prefix, label)
        _strict_utc(self.registered_at, "triage comparison registered_at")
        if self.metric_ids != TRIAGE_COMPARISON_METRICS:
            raise ValueError("triage comparison metrics are not the frozen v1 set")
        if self.max_must_catch_false_negatives != 0:
            raise ValueError("triage comparison cannot tolerate a must-catch false negative")
        if self.max_unsupported_material_routes != 0:
            raise ValueError("triage comparison cannot promote unsupported material routes")
        if not (
            self.require_treatment_not_worse_eligibility
            and self.require_treatment_not_worse_routing
            and self.second_pristine_blind_batch_required
            and self.labels_hidden_from_arms
        ):
            raise ValueError("triage comparison v1 must retain all fail-closed gates")
        if self.max_aggregate_estimated_cost_microusd < 0:
            raise ValueError("triage comparison cost cap must be non-negative")
        if self.historical_pit_claim or self.strategy_or_execution_authority:
            raise ValueError("triage comparison cannot grant PIT, strategy, or execution authority")
        if self.comparison_id != self.expected_comparison_id:
            raise ValueError("Triage Comparison Registration ID does not match content")

    @property
    def expected_comparison_id(self) -> str:
        return f"event-impact-triage-comparison-{canonical_hash(self.core_dict())}"

    def core_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "candidate_set_id": self.candidate_set_id,
            "label_set_id": self.label_set_id,
            "baseline_plan_id": self.baseline_plan_id,
            "treatment_plan_id": self.treatment_plan_id,
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
            "second_pristine_blind_batch_required": (self.second_pristine_blind_batch_required),
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
        baseline_plan: EventImpactTriageExecutionPlan,
        treatment_plan: EventImpactTriageExecutionPlan,
        registered_at: datetime,
    ) -> EventImpactTriageComparisonRegistration:
        if label_set.candidate_set_id != candidate_set.candidate_set_id:
            raise ValueError("triage comparison Label Set belongs to another Candidate Set")
        if tuple(item.version_id for item in label_set.labels) != tuple(
            sorted(candidate_set.version_ids)
        ):
            raise ValueError("triage comparison requires labels for every frozen candidate")
        if (
            baseline_plan.arm is not TriageComparisonArm.BASELINE
            or treatment_plan.arm is not TriageComparisonArm.TREATMENT
        ):
            raise ValueError("triage comparison plans have incorrect arms")
        for plan in (baseline_plan, treatment_plan):
            if plan.candidate_set_id != candidate_set.candidate_set_id:
                raise ValueError("triage comparison plan belongs to another Candidate Set")
        if (
            baseline_plan.model_provider_profile.to_dict()
            != treatment_plan.model_provider_profile.to_dict()
            or baseline_plan.checkpoint_contract_hash != treatment_plan.checkpoint_contract_hash
            or baseline_plan.data_snapshot_id != treatment_plan.data_snapshot_id
            or baseline_plan.candidate_content_view != treatment_plan.candidate_content_view
        ):
            raise ValueError("triage comparison arms must share model, rule, and frozen inputs")
        _strict_utc(registered_at, "triage comparison registered_at")
        if registered_at < label_set.sealed_at:
            raise ValueError("triage comparison must be registered after labels are sealed")
        cost_cap = (
            baseline_plan.max_total_estimated_cost_microusd
            + treatment_plan.max_total_estimated_cost_microusd
        )
        core = {
            "schema_version": TRIAGE_COMPARISON_REGISTRATION_SCHEMA,
            "candidate_set_id": candidate_set.candidate_set_id,
            "label_set_id": label_set.label_set_id,
            "baseline_plan_id": baseline_plan.plan_id,
            "treatment_plan_id": treatment_plan.plan_id,
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
            comparison_id=f"event-impact-triage-comparison-{canonical_hash(core)}",
            candidate_set_id=candidate_set.candidate_set_id,
            label_set_id=label_set.label_set_id,
            baseline_plan_id=baseline_plan.plan_id,
            treatment_plan_id=treatment_plan.plan_id,
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


class EventImpactTriageComparisonStore:
    """Durable Harness-clock authority for one pre-execution comparison registration."""

    def __init__(
        self,
        path: Path,
        *,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        if path.is_symlink():
            raise ValueError("triage comparison store path must not be a symlink")
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
                CREATE TABLE IF NOT EXISTS triage_comparison_registrations (
                    binding_key TEXT PRIMARY KEY,
                    comparison_id TEXT NOT NULL UNIQUE,
                    registered_at TEXT NOT NULL,
                    registration_json TEXT NOT NULL
                );
                CREATE TRIGGER IF NOT EXISTS triage_comparison_registrations_no_update
                BEFORE UPDATE ON triage_comparison_registrations
                BEGIN SELECT RAISE(ABORT, 'triage comparison registrations are append-only'); END;
                CREATE TRIGGER IF NOT EXISTS triage_comparison_registrations_no_delete
                BEFORE DELETE ON triage_comparison_registrations
                BEGIN SELECT RAISE(ABORT, 'triage comparison registrations are append-only'); END;
                """
            )

    def register(
        self,
        *,
        candidate_set: EventImpactTriageCandidateSet,
        label_set: EventImpactTriageLabelSet,
        baseline_plan: EventImpactTriageExecutionPlan,
        treatment_plan: EventImpactTriageExecutionPlan,
    ) -> EventImpactTriageComparisonRegistration:
        binding_key = canonical_hash(
            {
                "candidate_set_id": candidate_set.candidate_set_id,
                "label_set_id": label_set.label_set_id,
                "baseline_plan_id": baseline_plan.plan_id,
                "treatment_plan_id": treatment_plan.plan_id,
            }
        )
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            existing = connection.execute(
                """
                SELECT comparison_id, registered_at, registration_json
                FROM triage_comparison_registrations WHERE binding_key = ?
                """,
                (binding_key,),
            ).fetchone()
            if existing is not None:
                registration = EventImpactTriageComparisonRegistration.build(
                    candidate_set=candidate_set,
                    label_set=label_set,
                    baseline_plan=baseline_plan,
                    treatment_plan=treatment_plan,
                    registered_at=_datetime(str(existing["registered_at"])),
                )
                if registration.comparison_id != str(
                    existing["comparison_id"]
                ) or canonical_json_bytes(registration.to_dict()).decode() != str(
                    existing["registration_json"]
                ):
                    raise ValueError("stored triage comparison registration is inconsistent")
                return registration
            registered_at = self._clock()
            _strict_utc(registered_at, "triage comparison Harness clock")
            registration = EventImpactTriageComparisonRegistration.build(
                candidate_set=candidate_set,
                label_set=label_set,
                baseline_plan=baseline_plan,
                treatment_plan=treatment_plan,
                registered_at=registered_at,
            )
            connection.execute(
                """
                INSERT INTO triage_comparison_registrations(
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
        self, registration: EventImpactTriageComparisonRegistration
    ) -> datetime:
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT registered_at, registration_json
                FROM triage_comparison_registrations WHERE comparison_id = ?
                """,
                (registration.comparison_id,),
            ).fetchone()
        if row is None:
            raise ValueError("triage comparison is not durably registered")
        if canonical_json_bytes(registration.to_dict()).decode() != str(row["registration_json"]):
            raise ValueError("triage comparison differs from its durable registration")
        registered_at = _datetime(str(row["registered_at"]))
        if registered_at != registration.registered_at:
            raise ValueError("triage comparison Harness timestamp differs from registration")
        return registered_at


@dataclass(frozen=True, slots=True)
class TriageArmOutcome:
    plan: EventImpactTriageExecutionPlan
    proposal: EventImpactTriageProposal
    run_evidence: TriageRunEvidence
    total_estimated_cost_microusd: int

    def __post_init__(self) -> None:
        if self.proposal.candidate_set_id != self.plan.candidate_set_id:
            raise ValueError("triage arm outcome belongs to another Candidate Set")
        expected_roles = tuple(item.role.value for item in self.plan.role_bindings)
        observed_roles = tuple(item.role.value for item in self.run_evidence.members)
        if observed_roles != expected_roles:
            raise ValueError("triage arm outcome run roles differ from its plan")
        if self.total_estimated_cost_microusd < 0:
            raise ValueError("triage outcome cost must be non-negative")
        if self.total_estimated_cost_microusd > self.plan.max_total_estimated_cost_microusd:
            raise ValueError("triage arm outcome exceeded its frozen cost ceiling")

    @property
    def plan_id(self) -> str:
        return self.plan.plan_id


@dataclass(frozen=True, slots=True)
class TriageArmScore:
    candidate_count: int
    classified_count: int
    checkpoint_eligibility_correct: int
    eligible_false_negatives: int
    eligible_false_positives: int
    must_catch_false_negatives: int
    needs_review_correct: int
    needs_review_label_count: int
    route_correct: int
    unsupported_material_routes: int
    total_estimated_cost_microusd: int

    @property
    def checkpoint_eligibility_accuracy(self) -> float:
        return self.checkpoint_eligibility_correct / self.candidate_count

    @property
    def route_accuracy(self) -> float:
        return self.route_correct / self.candidate_count

    def to_dict(self) -> dict[str, object]:
        return {
            "candidate_count": self.candidate_count,
            "classified_count": self.classified_count,
            "candidate_coverage": self.classified_count / self.candidate_count,
            "checkpoint_eligibility_correct": self.checkpoint_eligibility_correct,
            "checkpoint_eligibility_accuracy": self.checkpoint_eligibility_accuracy,
            "eligible_false_negatives": self.eligible_false_negatives,
            "eligible_false_positives": self.eligible_false_positives,
            "must_catch_false_negatives": self.must_catch_false_negatives,
            "needs_review_correct": self.needs_review_correct,
            "needs_review_label_count": self.needs_review_label_count,
            "needs_review_accuracy": (
                None
                if self.needs_review_label_count == 0
                else self.needs_review_correct / self.needs_review_label_count
            ),
            "route_correct": self.route_correct,
            "route_accuracy": self.route_accuracy,
            "unsupported_material_routes": self.unsupported_material_routes,
            "estimated_cost_microusd": self.total_estimated_cost_microusd,
        }


@dataclass(frozen=True, slots=True)
class EventImpactTriageComparisonReport:
    report_id: str
    comparison_id: str
    label_set_id: str
    label_exposure: TriageLabelExposure
    baseline_score: TriageArmScore
    treatment_score: TriageArmScore
    batch_gate_passed: bool
    promotion_eligible: bool
    blockers: tuple[str, ...]
    evaluated_at: datetime
    historical_pit_claim: bool = False
    strategy_or_execution_authority: bool = False
    schema_version: str = TRIAGE_COMPARISON_REPORT_SCHEMA

    def __post_init__(self) -> None:
        if self.schema_version != TRIAGE_COMPARISON_REPORT_SCHEMA:
            raise ValueError("unsupported Triage Comparison Report schema")
        _prefixed_hash(
            self.comparison_id,
            "event-impact-triage-comparison-",
            "triage comparison ID",
        )
        _prefixed_hash(self.label_set_id, "event-impact-triage-label-set-", "triage label set ID")
        if self.blockers != tuple(sorted(set(self.blockers))):
            raise ValueError("triage comparison blockers must be sorted and unique")
        if self.promotion_eligible:
            raise ValueError("one triage comparison batch cannot promote a method")
        _strict_utc(self.evaluated_at, "triage comparison evaluated_at")
        if self.historical_pit_claim or self.strategy_or_execution_authority:
            raise ValueError("triage comparison report cannot grant authority")
        if self.report_id != self.expected_report_id:
            raise ValueError("Triage Comparison Report ID does not match content")

    @property
    def expected_report_id(self) -> str:
        return f"event-impact-triage-comparison-report-{canonical_hash(self.core_dict())}"

    def core_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "comparison_id": self.comparison_id,
            "label_set_id": self.label_set_id,
            "label_exposure": self.label_exposure.value,
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


def evaluate_event_impact_triage_comparison(
    *,
    registration: EventImpactTriageComparisonRegistration,
    candidate_set: EventImpactTriageCandidateSet,
    label_set: EventImpactTriageLabelSet,
    baseline: TriageArmOutcome,
    treatment: TriageArmOutcome,
    baseline_authority: TriageComparisonRunAuthority,
    treatment_authority: TriageComparisonRunAuthority,
    registration_authority: TriageComparisonRegistrationAuthority,
    evaluated_at: datetime,
) -> EventImpactTriageComparisonReport:
    _strict_utc(evaluated_at, "triage comparison evaluated_at")
    authoritative_registered_at = registration_authority.assert_authoritative_registration(
        registration
    )
    if evaluated_at < authoritative_registered_at:
        raise ValueError("triage comparison cannot be evaluated before registration")
    if (
        registration.candidate_set_id != candidate_set.candidate_set_id
        or registration.label_set_id != label_set.label_set_id
        or label_set.candidate_set_id != candidate_set.candidate_set_id
    ):
        raise ValueError("triage comparison inputs belong to different frozen batches")
    if baseline.plan_id != registration.baseline_plan_id:
        raise ValueError("triage baseline outcome belongs to another plan")
    if treatment.plan_id != registration.treatment_plan_id:
        raise ValueError("triage treatment outcome belongs to another plan")
    finished_at: list[datetime] = []
    for outcome, authority in (
        (baseline, baseline_authority),
        (treatment, treatment_authority),
    ):
        authority.assert_authoritative_completed_triage_run(
            candidate_set=candidate_set,
            proposal=outcome.proposal,
            run_evidence=outcome.run_evidence,
        )
        if authority.authoritative_started_at(outcome.run_evidence) < authoritative_registered_at:
            raise ValueError("triage comparison arm started before its protocol was registered")
        finished_at.append(authority.authoritative_finished_at(outcome.run_evidence))
        authoritative_cost = authority.authoritative_total_estimated_cost_microusd(
            outcome.run_evidence
        )
        if authoritative_cost != outcome.total_estimated_cost_microusd:
            raise ValueError("triage comparison cost differs from the authoritative Usage Ledger")
    if evaluated_at < max(finished_at):
        raise ValueError("triage comparison cannot be evaluated before both arms finish")
    baseline.proposal.validate_against(candidate_set)
    treatment.proposal.validate_against(candidate_set)
    baseline_score = _score(label_set, baseline)
    treatment_score = _score(label_set, treatment)
    blockers: set[str] = {"second_pristine_blind_batch_required"}
    if label_set.exposure is not TriageLabelExposure.PRISTINE_BLIND:
        blockers.add("operator_exposed_batch_cannot_promote")
    if treatment_score.must_catch_false_negatives > registration.max_must_catch_false_negatives:
        blockers.add("treatment_missed_must_catch_eligible")
    if treatment_score.unsupported_material_routes > registration.max_unsupported_material_routes:
        blockers.add("treatment_has_unsupported_material_route")
    if (
        registration.require_treatment_not_worse_eligibility
        and treatment_score.checkpoint_eligibility_accuracy
        < baseline_score.checkpoint_eligibility_accuracy
    ):
        blockers.add("treatment_worse_checkpoint_eligibility")
    if (
        registration.require_treatment_not_worse_routing
        and treatment_score.route_accuracy < baseline_score.route_accuracy
    ):
        blockers.add("treatment_worse_impact_routing")
    aggregate_cost = (
        baseline.total_estimated_cost_microusd + treatment.total_estimated_cost_microusd
    )
    if aggregate_cost > registration.max_aggregate_estimated_cost_microusd:
        blockers.add("aggregate_model_cost_exceeded")
    batch_blockers = blockers - {
        "second_pristine_blind_batch_required",
        "operator_exposed_batch_cannot_promote",
    }
    batch_gate_passed = not batch_blockers
    ordered_blockers = tuple(sorted(blockers))
    core = {
        "schema_version": TRIAGE_COMPARISON_REPORT_SCHEMA,
        "comparison_id": registration.comparison_id,
        "label_set_id": label_set.label_set_id,
        "label_exposure": label_set.exposure.value,
        "baseline_score": baseline_score.to_dict(),
        "treatment_score": treatment_score.to_dict(),
        "batch_gate_passed": batch_gate_passed,
        "promotion_eligible": False,
        "blockers": list(ordered_blockers),
        "evaluated_at": _timestamp(evaluated_at),
        "historical_pit_claim": False,
        "strategy_or_execution_authority": False,
    }
    return EventImpactTriageComparisonReport(
        report_id=f"event-impact-triage-comparison-report-{canonical_hash(core)}",
        comparison_id=registration.comparison_id,
        label_set_id=label_set.label_set_id,
        label_exposure=label_set.exposure,
        baseline_score=baseline_score,
        treatment_score=treatment_score,
        batch_gate_passed=batch_gate_passed,
        promotion_eligible=False,
        blockers=ordered_blockers,
        evaluated_at=evaluated_at,
    )


def _score(labels: EventImpactTriageLabelSet, outcome: TriageArmOutcome) -> TriageArmScore:
    predicted: dict[str, tuple[CheckpointEligibility, TriageRoute, bool]] = {}
    for cluster in outcome.proposal.clusters:
        material = bool(cluster.changed_facts and cluster.transmission_channels)
        for version_id in cluster.candidate_version_ids:
            predicted[version_id] = (
                cluster.checkpoint_eligibility,
                cluster.recommended_route,
                material,
            )
    correct_eligibility = 0
    eligible_false_negatives = 0
    eligible_false_positives = 0
    must_catch_false_negatives = 0
    needs_review_correct = 0
    needs_review_count = 0
    route_correct = 0
    unsupported_material_routes = 0
    for label in labels.labels:
        eligibility, route, material = predicted[label.version_id]
        if eligibility is label.checkpoint_eligibility:
            correct_eligibility += 1
        if (
            label.checkpoint_eligibility is CheckpointEligibility.ELIGIBLE
            and eligibility is not CheckpointEligibility.ELIGIBLE
        ):
            eligible_false_negatives += 1
            if label.must_catch:
                must_catch_false_negatives += 1
        if (
            label.checkpoint_eligibility is not CheckpointEligibility.ELIGIBLE
            and eligibility is CheckpointEligibility.ELIGIBLE
        ):
            eligible_false_positives += 1
        if label.checkpoint_eligibility is CheckpointEligibility.NEEDS_REVIEW:
            needs_review_count += 1
            if eligibility is CheckpointEligibility.NEEDS_REVIEW:
                needs_review_correct += 1
        if route is label.expected_route:
            route_correct += 1
        if material and not label.material_transmission_expected:
            unsupported_material_routes += 1
    return TriageArmScore(
        candidate_count=len(labels.labels),
        classified_count=len(predicted),
        checkpoint_eligibility_correct=correct_eligibility,
        eligible_false_negatives=eligible_false_negatives,
        eligible_false_positives=eligible_false_positives,
        must_catch_false_negatives=must_catch_false_negatives,
        needs_review_correct=needs_review_correct,
        needs_review_label_count=needs_review_count,
        route_correct=route_correct,
        unsupported_material_routes=unsupported_material_routes,
        total_estimated_cost_microusd=outcome.total_estimated_cost_microusd,
    )


def _strict_utc(value: datetime, label: str) -> None:
    require_aware(value, label)
    if value.utcoffset() != UTC.utcoffset(value):
        raise ValueError(f"{label} must use UTC")


def _timestamp(value: datetime) -> str:
    return value.isoformat().replace("+00:00", "Z")


def _datetime(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    _strict_utc(parsed, "triage comparison stored timestamp")
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
    _sha256(value.removeprefix(prefix), label)


def _object(value: object, label: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise TypeError(f"{label} must be an object with string keys")
    mapping = cast(dict[object, object], value)
    if not all(isinstance(key, str) for key in mapping):
        raise TypeError(f"{label} must be an object with string keys")
    return cast(dict[str, object], mapping)


def _array(value: object, label: str) -> list[object]:
    if not isinstance(value, list):
        raise TypeError(f"{label} must be an array")
    return cast(list[object], value)


def _string(payload: dict[str, object], name: str) -> str:
    value = payload.get(name)
    if not isinstance(value, str) or not value or value != value.strip():
        raise TypeError(f"{name} must be a non-empty trimmed string")
    return value


def _boolean(payload: dict[str, object], name: str) -> bool:
    value = payload.get(name)
    if not isinstance(value, bool):
        raise TypeError(f"{name} must be a boolean")
    return value
