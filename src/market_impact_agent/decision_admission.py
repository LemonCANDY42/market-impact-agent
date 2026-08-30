from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum
from typing import cast

from market_impact_agent.agent_contracts import (
    CandidateDirection,
    EvidencePack,
    JudgmentArtifact,
    JudgmentDecision,
    canonical_hash,
)
from market_impact_agent.agent_engine import AgentRunResult
from market_impact_agent.agent_ensemble import execution_binding_hash
from market_impact_agent.domain import (
    OrderIntent,
    Side,
    SignalIntent,
    TradingEnvironment,
    require_aware,
)
from market_impact_agent.prospective_diagnostic import (
    PROSPECTIVE_DIAGNOSTIC_REGISTRATION_SCHEMA_V3,
    ProspectiveDiagnosticRegistration,
)
from market_impact_agent.prospective_execution import ProspectiveExecutionPlan
from market_impact_agent.prospective_query_gate import ProspectiveQueryGateResult
from market_impact_agent.runtime_store import RunStatus, runtime_event_from_dict

DECISION_RUN_MANIFEST_SCHEMA_V1 = "market-impact.decision-run-manifest.v1"
DECISION_RUN_MANIFEST_SCHEMA_V2 = "market-impact.decision-run-manifest.v2"
DECISION_RUN_MANIFEST_SCHEMA = DECISION_RUN_MANIFEST_SCHEMA_V1
_SUPPORTED_DECISION_RUN_MANIFEST_SCHEMAS = frozenset(
    {DECISION_RUN_MANIFEST_SCHEMA_V1, DECISION_RUN_MANIFEST_SCHEMA_V2}
)
DECISION_ADMISSION_SCHEMA = "market-impact.decision-admission.v1"
DECISION_CLAIM_SCOPE = "execution_diagnostic_only_no_alpha_or_live_claim"


class DecisionDisposition(StrEnum):
    PROPOSE = "propose"
    ABSTAIN = "abstain"


class RunAssessmentOutcome(StrEnum):
    PROPOSE = "propose"
    ABSTAIN = "abstain"
    INVALID = "invalid"


@dataclass(frozen=True, slots=True)
class PairedDecisionRun:
    arm: str
    replicate_index: int
    result: AgentRunResult


@dataclass(frozen=True, slots=True)
class DecisionRunAssessment:
    arm: str
    replicate_index: int
    run_id: str
    run_status: RunStatus
    outcome: RunAssessmentOutcome
    reason: str
    terminal_store_hash: str | None
    judgment_artifact_id: str | None
    judgment_artifact_hash: str | None
    execution_binding_hash: str | None
    metrics_hash: str | None
    run_validation_evidence_hash: str | None
    estimated_cost_microusd: int | None
    vote_target_id: str | None
    vote_direction: CandidateDirection | None
    decision_confidence: float | None

    def to_dict(self) -> dict[str, object]:
        return {
            "arm": self.arm,
            "replicate_index": self.replicate_index,
            "run_id": self.run_id,
            "run_status": self.run_status.value,
            "outcome": self.outcome.value,
            "reason": self.reason,
            "terminal_store_hash": self.terminal_store_hash,
            "judgment_artifact_id": self.judgment_artifact_id,
            "judgment_artifact_hash": self.judgment_artifact_hash,
            "execution_binding_hash": self.execution_binding_hash,
            "metrics_hash": self.metrics_hash,
            "run_validation_evidence_hash": self.run_validation_evidence_hash,
            "estimated_cost_microusd": self.estimated_cost_microusd,
            "vote_target_id": self.vote_target_id,
            "vote_direction": (None if self.vote_direction is None else self.vote_direction.value),
            "decision_confidence": self.decision_confidence,
        }


@dataclass(frozen=True, slots=True)
class DecisionRunManifest:
    manifest_id: str
    registration_id: str
    registration_hash: str
    checkpoint_key: str
    query_gate_result_id: str
    query_gate_result_hash: str
    evidence_pack_id: str
    evidence_pack_hash: str
    agent_execution_plan_id: str
    agent_execution_plan_hash: str
    control_arm: str
    treatment_arm: str
    replicates_per_arm: int
    assessments: tuple[DecisionRunAssessment, ...]
    disposition: DecisionDisposition
    agreement_target_id: str | None
    agreement_direction: CandidateDirection | None
    agreement_count: int
    agreeing_judgment_artifact_ids: tuple[str, ...]
    total_estimated_cost_microusd: int
    blockers: tuple[str, ...]
    created_at: datetime
    replicates_executed_per_arm: int = 3
    replicate_stop_reason: str = "fixed_three_paired_replicates"
    claim_scope: str = DECISION_CLAIM_SCOPE
    historical_pit_claim: bool = False
    strategy_promotion_claim: bool = False
    execution_capability: bool = False
    schema_version: str = DECISION_RUN_MANIFEST_SCHEMA

    def __post_init__(self) -> None:
        if self.schema_version not in _SUPPORTED_DECISION_RUN_MANIFEST_SCHEMAS:
            raise ValueError("unsupported Decision Run Manifest schema")
        _prefixed_hash(
            self.registration_id,
            "prospective-diagnostic-registration-",
            "Decision Run registration ID",
        )
        _sha256(self.registration_hash, "Decision Run registration hash")
        _prefixed_hash(
            self.query_gate_result_id,
            "prospective-query-gate-",
            "Decision Run Query Gate ID",
        )
        _sha256(self.query_gate_result_hash, "Decision Run Query Gate hash")
        _prefixed_hash(
            self.evidence_pack_id,
            "evidence-pack-",
            "Decision Run Evidence Pack ID",
        )
        _sha256(self.evidence_pack_hash, "Decision Run Evidence Pack hash")
        _prefixed_hash(
            self.agent_execution_plan_id,
            "prospective-execution-plan-",
            "Decision Run Agent execution plan ID",
        )
        _sha256(self.agent_execution_plan_hash, "Decision Run Agent execution plan hash")
        _strict_utc(self.created_at, "Decision Run Manifest created_at")
        if self.replicates_per_arm != 3:
            raise ValueError("Decision Run Manifest maximum replicate count must be three")
        if self.schema_version == DECISION_RUN_MANIFEST_SCHEMA_V1:
            if len(self.assessments) != 6 or self.replicates_executed_per_arm != 3:
                raise ValueError("Decision Run Manifest v1 requires three runs per paired arm")
            if self.replicate_stop_reason != "fixed_three_paired_replicates":
                raise ValueError("Decision Run Manifest v1 replicate stop reason is invalid")
        elif self.replicates_executed_per_arm not in {2, 3} or len(self.assessments) != (
            self.replicates_executed_per_arm * 2
        ):
            raise ValueError("Decision Run Manifest v2 requires two or three runs per paired arm")
        if (self.control_arm, self.treatment_arm) != (
            "structured_agent_core",
            "structured_agent_plus_routed_methods",
        ):
            raise ValueError("Decision Run Manifest paired arm roles are not canonical")
        keys = tuple((item.arm, item.replicate_index) for item in self.assessments)
        expected = tuple(
            (arm, index)
            for arm in (self.control_arm, self.treatment_arm)
            for index in range(1, self.replicates_executed_per_arm + 1)
        )
        if keys != expected:
            raise ValueError("Decision Run Manifest assessments are not canonically ordered")
        if self.schema_version == DECISION_RUN_MANIFEST_SCHEMA_V2:
            first_two_agree = all(
                _first_two_assessments_agree(self.assessments, arm=arm)
                for arm in (self.control_arm, self.treatment_arm)
            )
            if self.replicates_executed_per_arm == 2 and (
                not first_two_agree or self.replicate_stop_reason != "first_two_agree_in_both_arms"
            ):
                raise ValueError("adaptive Decision Run stopped before required third pair")
            if self.replicates_executed_per_arm == 3 and (
                first_two_agree
                or self.replicate_stop_reason != "third_pair_required_after_first_two_disagreement"
            ):
                raise ValueError("adaptive Decision Run executed an unnecessary third pair")
        if self.agreeing_judgment_artifact_ids != tuple(
            sorted(set(self.agreeing_judgment_artifact_ids))
        ):
            raise ValueError("agreeing Judgment IDs must be sorted and unique")
        if self.blockers != tuple(sorted(set(self.blockers))):
            raise ValueError("Decision Run Manifest blockers must be sorted and unique")
        expected_total_cost = sum(item.estimated_cost_microusd or 0 for item in self.assessments)
        if self.total_estimated_cost_microusd != expected_total_cost:
            raise ValueError("Decision Run Manifest total cost does not match its runs")
        if self.disposition is DecisionDisposition.PROPOSE:
            if (
                self.agreement_target_id is None
                or self.agreement_direction
                not in {
                    CandidateDirection.UP,
                    CandidateDirection.DOWN,
                }
                or self.agreement_count < 2
                or len(self.agreeing_judgment_artifact_ids) != self.agreement_count
                or self.blockers
            ):
                raise ValueError("proposed Decision Run Manifest lacks treatment agreement")
            if any(
                item.outcome is RunAssessmentOutcome.INVALID
                or item.run_status is not RunStatus.COMPLETED
                or item.judgment_artifact_id is None
                or item.judgment_artifact_hash is None
                or item.execution_binding_hash is None
                or item.metrics_hash is None
                or item.run_validation_evidence_hash is None
                or item.estimated_cost_microusd is None
                for item in self.assessments
            ):
                raise ValueError("proposed Decision Run Manifest requires all valid sealed runs")
            agreeing_assessments = tuple(
                item
                for item in self.assessments
                if item.judgment_artifact_id in self.agreeing_judgment_artifact_ids
            )
            if len(agreeing_assessments) != self.agreement_count or any(
                item.arm != self.treatment_arm
                or item.outcome is not RunAssessmentOutcome.PROPOSE
                or item.vote_target_id != self.agreement_target_id
                or item.vote_direction is not self.agreement_direction
                for item in agreeing_assessments
            ):
                raise ValueError("Decision Run agreement is not exact treatment-arm consensus")
        elif any(
            (
                self.agreement_target_id is not None,
                self.agreement_direction is not None,
                self.agreement_count != 0,
                bool(self.agreeing_judgment_artifact_ids),
                not self.blockers,
            )
        ):
            raise ValueError("abstaining Decision Run Manifest must record only blockers")
        if self.claim_scope != DECISION_CLAIM_SCOPE:
            raise ValueError("Decision Run Manifest claim scope is invalid")
        if self.historical_pit_claim or self.strategy_promotion_claim or self.execution_capability:
            raise ValueError("Decision Run Manifest cannot grant research or execution authority")
        if self.manifest_id != self.expected_manifest_id:
            raise ValueError("Decision Run Manifest ID does not match content")

    @property
    def expected_manifest_id(self) -> str:
        return f"decision-run-manifest-{canonical_hash(self.core_dict())}"

    def core_dict(self) -> dict[str, object]:
        payload: dict[str, object] = {
            "schema_version": self.schema_version,
            "registration_id": self.registration_id,
            "registration_hash": self.registration_hash,
            "checkpoint_key": self.checkpoint_key,
            "query_gate_result_id": self.query_gate_result_id,
            "query_gate_result_hash": self.query_gate_result_hash,
            "evidence_pack_id": self.evidence_pack_id,
            "evidence_pack_hash": self.evidence_pack_hash,
            "agent_execution_plan_id": self.agent_execution_plan_id,
            "agent_execution_plan_hash": self.agent_execution_plan_hash,
            "control_arm": self.control_arm,
            "treatment_arm": self.treatment_arm,
            "replicates_per_arm": self.replicates_per_arm,
            "assessments": [item.to_dict() for item in self.assessments],
            "disposition": self.disposition.value,
            "agreement_target_id": self.agreement_target_id,
            "agreement_direction": (
                None if self.agreement_direction is None else self.agreement_direction.value
            ),
            "agreement_count": self.agreement_count,
            "agreeing_judgment_artifact_ids": list(self.agreeing_judgment_artifact_ids),
            "total_estimated_cost_microusd": self.total_estimated_cost_microusd,
            "blockers": list(self.blockers),
            "created_at": _timestamp(self.created_at),
            "claim_scope": self.claim_scope,
            "historical_pit_claim": self.historical_pit_claim,
            "strategy_promotion_claim": self.strategy_promotion_claim,
            "execution_capability": self.execution_capability,
        }
        if self.schema_version == DECISION_RUN_MANIFEST_SCHEMA_V2:
            payload["replicates_executed_per_arm"] = self.replicates_executed_per_arm
            payload["replicate_stop_reason"] = self.replicate_stop_reason
            payload["confidence_observation"] = _decision_confidence_observation(
                self.assessments,
                control_arm=self.control_arm,
                treatment_arm=self.treatment_arm,
            )
        return payload

    def to_dict(self) -> dict[str, object]:
        return {**self.core_dict(), "manifest_id": self.manifest_id}


def build_decision_run_manifest(
    *,
    registration: ProspectiveDiagnosticRegistration,
    query_gate: ProspectiveQueryGateResult,
    evidence_pack: EvidencePack,
    execution_plan: ProspectiveExecutionPlan,
    paired_runs: tuple[PairedDecisionRun, ...],
    created_at: datetime,
) -> DecisionRunManifest:
    if not query_gate.model_run_eligible:
        raise PermissionError("Decision Run Manifest requires an eligible Query Gate")
    if query_gate.registration_id != registration.registration_id:
        raise ValueError("Query Gate belongs to a different registration")
    registration.checkpoint(query_gate.checkpoint_key)
    if query_gate.evidence_pack_id != evidence_pack.pack_id:
        raise ValueError("Query Gate binds a different Evidence Pack")
    if (
        query_gate.agent_execution_plan_id != execution_plan.plan_id
        or query_gate.agent_execution_plan_hash != canonical_hash(execution_plan.to_dict())
    ):
        raise ValueError("Query Gate binds a different Agent execution plan")
    if execution_plan.registration_id != registration.registration_id:
        raise ValueError("Agent execution plan belongs to a different registration")
    adaptive = registration.schema_version == PROSPECTIVE_DIAGNOSTIC_REGISTRATION_SCHEMA_V3
    if adaptive:
        if len(paired_runs) not in {4, 6}:
            raise ValueError("adaptive Decision runs require two or three complete paired runs")
        replicates_executed = len(paired_runs) // 2
    else:
        replicates_executed = registration.replicates_per_arm
    expected_keys = tuple(
        (arm, index)
        for arm in registration.paired_arms
        for index in range(1, replicates_executed + 1)
    )
    if tuple((item.arm, item.replicate_index) for item in paired_runs) != expected_keys:
        raise ValueError("Decision runs must be the exact canonically ordered paired replicates")
    _strict_utc(created_at, "Decision Run Manifest created_at")

    assessments = tuple(
        _assess_run(
            arm=item.arm,
            replicate_index=item.replicate_index,
            result=item.result,
            runtime_ref=execution_plan.arm_binding(item.arm).runtime_ref,
            expected_binding_hash=execution_plan.arm_binding(item.arm).binding_hash,
            expected_provider_id=execution_plan.provider_id,
            expected_model=execution_plan.model,
            evidence_pack=evidence_pack,
            gate_evaluated_at=query_gate.evaluated_at,
            created_at=created_at,
        )
        for item in paired_runs
    )
    first_two_agree = all(
        _first_two_assessments_agree(assessments, arm=arm) for arm in registration.paired_arms
    )
    if adaptive and replicates_executed == 2 and not first_two_agree:
        raise ValueError("adaptive Decision runs require a third pair after disagreement")
    if adaptive and replicates_executed == 3 and first_two_agree:
        raise ValueError("adaptive Decision runs must not execute an unnecessary third pair")
    replicate_stop_reason = (
        "first_two_agree_in_both_arms"
        if adaptive and replicates_executed == 2
        else "third_pair_required_after_first_two_disagreement"
        if adaptive
        else "fixed_three_paired_replicates"
    )
    blockers = [
        f"{item.arm}:{item.replicate_index}:{item.reason}"
        for item in assessments
        if item.outcome is RunAssessmentOutcome.INVALID
    ]
    run_ids = tuple(item.run_id for item in assessments)
    judgment_ids = tuple(
        item.judgment_artifact_id for item in assessments if item.judgment_artifact_id is not None
    )
    if len(run_ids) != len(set(run_ids)):
        blockers.append("paired_runs:duplicate_run_id")
    if len(judgment_ids) != len(set(judgment_ids)):
        blockers.append("paired_runs:duplicate_judgment_artifact_id")
    total_cost = sum(item.estimated_cost_microusd or 0 for item in assessments)
    gate_cost_limit = int(Decimal(query_gate.model_cost_limit_usd) * 1_000_000)
    if total_cost > gate_cost_limit:
        blockers.append("paired_runs:model_cost_limit_exceeded")

    treatment = tuple(
        item
        for item in assessments
        if item.arm == registration.paired_arms[1]
        and item.outcome is RunAssessmentOutcome.PROPOSE
        and item.vote_target_id is not None
        and item.vote_direction is not None
    )
    votes: Counter[tuple[str, CandidateDirection]] = Counter()
    for item in treatment:
        votes[
            (
                cast(str, item.vote_target_id),
                cast(CandidateDirection, item.vote_direction),
            )
        ] += 1
    selected: tuple[str, CandidateDirection] | None = (
        max(votes.items(), key=lambda item: item[1])[0] if votes else None
    )
    selected_count = 0 if selected is None else votes[selected]
    if selected_count < 2:
        blockers.append(
            "treatment:no_majority_target_direction_agreement"
            if adaptive
            else "treatment:no_two_of_three_target_direction_agreement"
        )

    if blockers:
        disposition = DecisionDisposition.ABSTAIN
        target = None
        direction = None
        agreement_count = 0
        agreeing_ids: tuple[str, ...] = ()
    else:
        assert selected is not None
        disposition = DecisionDisposition.PROPOSE
        target, direction = selected
        agreement_count = selected_count
        agreeing_ids = tuple(
            sorted(
                item.judgment_artifact_id
                for item in treatment
                if (item.vote_target_id, item.vote_direction) == selected
                and item.judgment_artifact_id is not None
            )
        )

    core: dict[str, object] = {
        "schema_version": (
            DECISION_RUN_MANIFEST_SCHEMA_V2 if adaptive else DECISION_RUN_MANIFEST_SCHEMA_V1
        ),
        "registration_id": registration.registration_id,
        "registration_hash": canonical_hash(registration.to_dict()),
        "checkpoint_key": query_gate.checkpoint_key,
        "query_gate_result_id": query_gate.result_id,
        "query_gate_result_hash": canonical_hash(query_gate.to_dict()),
        "evidence_pack_id": evidence_pack.pack_id,
        "evidence_pack_hash": canonical_hash(evidence_pack.to_dict()),
        "agent_execution_plan_id": execution_plan.plan_id,
        "agent_execution_plan_hash": canonical_hash(execution_plan.to_dict()),
        "control_arm": registration.paired_arms[0],
        "treatment_arm": registration.paired_arms[1],
        "replicates_per_arm": registration.replicates_per_arm,
        "assessments": [item.to_dict() for item in assessments],
        "disposition": disposition.value,
        "agreement_target_id": target,
        "agreement_direction": None if direction is None else direction.value,
        "agreement_count": agreement_count,
        "agreeing_judgment_artifact_ids": list(agreeing_ids),
        "total_estimated_cost_microusd": total_cost,
        "blockers": sorted(set(blockers)),
        "created_at": _timestamp(created_at),
        "claim_scope": DECISION_CLAIM_SCOPE,
        "historical_pit_claim": False,
        "strategy_promotion_claim": False,
        "execution_capability": False,
    }
    if adaptive:
        core["replicates_executed_per_arm"] = replicates_executed
        core["replicate_stop_reason"] = replicate_stop_reason
        core["confidence_observation"] = _decision_confidence_observation(
            assessments,
            control_arm=registration.paired_arms[0],
            treatment_arm=registration.paired_arms[1],
        )
    return DecisionRunManifest(
        manifest_id=f"decision-run-manifest-{canonical_hash(core)}",
        registration_id=registration.registration_id,
        registration_hash=canonical_hash(registration.to_dict()),
        checkpoint_key=query_gate.checkpoint_key,
        query_gate_result_id=query_gate.result_id,
        query_gate_result_hash=canonical_hash(query_gate.to_dict()),
        evidence_pack_id=evidence_pack.pack_id,
        evidence_pack_hash=canonical_hash(evidence_pack.to_dict()),
        agent_execution_plan_id=execution_plan.plan_id,
        agent_execution_plan_hash=canonical_hash(execution_plan.to_dict()),
        control_arm=registration.paired_arms[0],
        treatment_arm=registration.paired_arms[1],
        replicates_per_arm=registration.replicates_per_arm,
        assessments=assessments,
        disposition=disposition,
        agreement_target_id=target,
        agreement_direction=direction,
        agreement_count=agreement_count,
        agreeing_judgment_artifact_ids=agreeing_ids,
        total_estimated_cost_microusd=total_cost,
        blockers=tuple(sorted(set(blockers))),
        created_at=created_at,
        replicates_executed_per_arm=replicates_executed,
        replicate_stop_reason=replicate_stop_reason,
        schema_version=(
            DECISION_RUN_MANIFEST_SCHEMA_V2 if adaptive else DECISION_RUN_MANIFEST_SCHEMA_V1
        ),
    )


def _first_two_assessments_agree(
    assessments: tuple[DecisionRunAssessment, ...],
    *,
    arm: str,
) -> bool:
    selected = tuple(
        item for item in assessments if item.arm == arm and item.replicate_index in {1, 2}
    )
    if len(selected) != 2 or any(item.outcome is RunAssessmentOutcome.INVALID for item in selected):
        return False
    if selected[0].outcome is not selected[1].outcome:
        return False
    if selected[0].outcome is RunAssessmentOutcome.ABSTAIN:
        return True
    return (
        selected[0].vote_target_id,
        selected[0].vote_direction,
    ) == (
        selected[1].vote_target_id,
        selected[1].vote_direction,
    )


def _assess_run(
    *,
    arm: str,
    replicate_index: int,
    result: AgentRunResult,
    runtime_ref: str,
    expected_binding_hash: str,
    expected_provider_id: str,
    expected_model: str,
    evidence_pack: EvidencePack,
    gate_evaluated_at: datetime,
    created_at: datetime,
) -> DecisionRunAssessment:
    artifact = result.judgment
    artifact_hash = None if artifact is None else canonical_hash(artifact.to_dict())
    binding_hash = (
        None if artifact is None else execution_binding_hash(artifact, runtime_ref=runtime_ref)
    )
    cost = None if result.metrics is None else result.metrics.estimated_cost_microusd
    metrics_hash = None if result.metrics is None else canonical_hash(result.metrics.to_dict())
    validation_evidence_hash = (
        None
        if result.validation_event is None
        else canonical_hash(result.validation_event.to_dict())
    )
    reason = "valid_abstention"
    outcome = RunAssessmentOutcome.ABSTAIN
    target: str | None = None
    direction: CandidateDirection | None = None
    invalid_reason: str | None = None
    if not result.status.terminal:
        invalid_reason = "run_not_terminal"
    elif result.status is not RunStatus.COMPLETED:
        invalid_reason = f"run_{result.status.value}"
    elif artifact is None:
        invalid_reason = "judgment_missing"
    elif result.run_id != artifact.run_id:
        invalid_reason = "run_id_mismatch"
    elif result.terminal_store_hash != artifact_hash:
        invalid_reason = "terminal_artifact_hash_mismatch"
    elif binding_hash != expected_binding_hash:
        invalid_reason = "execution_binding_mismatch"
    elif artifact.provider_id != expected_provider_id or artifact.model != expected_model:
        invalid_reason = "provider_or_model_mismatch"
    elif artifact.started_at < gate_evaluated_at:
        invalid_reason = "run_predates_query_gate"
    elif artifact.finished_at > created_at:
        invalid_reason = "manifest_predates_judgment"
    elif cost is None or cost < 0 or result.metrics_hash != metrics_hash:
        invalid_reason = "cost_metrics_missing_or_invalid"
    else:
        try:
            completed_run_validation_evidence_hash(result)
        except (KeyError, TypeError, ValueError):
            invalid_reason = "run_validation_evidence_invalid"
    if invalid_reason is None and artifact is not None:
        try:
            artifact.validate_against(evidence_pack)
        except ValueError:
            invalid_reason = "judgment_evidence_binding_invalid"
    if invalid_reason is None and artifact is not None:
        if artifact.proposal.decision is JudgmentDecision.ABSTAIN:
            outcome = RunAssessmentOutcome.ABSTAIN
        elif len(artifact.proposal.candidates) != 1:
            invalid_reason = "eligible_candidate_count_not_one"
        else:
            candidate = artifact.proposal.candidates[0]
            if candidate.direction not in {CandidateDirection.UP, CandidateDirection.DOWN}:
                invalid_reason = "candidate_direction_not_tradable"
            else:
                outcome = RunAssessmentOutcome.PROPOSE
                reason = "valid_proposal"
                target = candidate.target_id
                direction = candidate.direction
    if invalid_reason is not None:
        outcome = RunAssessmentOutcome.INVALID
        reason = invalid_reason
        target = None
        direction = None
    return DecisionRunAssessment(
        arm=arm,
        replicate_index=replicate_index,
        run_id=result.run_id,
        run_status=result.status,
        outcome=outcome,
        reason=reason,
        terminal_store_hash=result.terminal_store_hash,
        judgment_artifact_id=None if artifact is None else artifact.artifact_id,
        judgment_artifact_hash=artifact_hash,
        execution_binding_hash=binding_hash,
        metrics_hash=metrics_hash,
        run_validation_evidence_hash=validation_evidence_hash,
        estimated_cost_microusd=cost,
        vote_target_id=target,
        vote_direction=direction,
        decision_confidence=(None if artifact is None else artifact.proposal.decision_confidence),
    )


def _decision_confidence_observation(
    assessments: tuple[DecisionRunAssessment, ...],
    *,
    control_arm: str,
    treatment_arm: str,
) -> dict[str, object]:
    def mean(values: tuple[float, ...]) -> float | None:
        return None if not values else round(sum(values) / len(values), 6)

    def arm_values(arm: str, indexes: set[int] | None = None) -> tuple[float, ...]:
        return tuple(
            item.decision_confidence
            for item in assessments
            if item.arm == arm
            and (indexes is None or item.replicate_index in indexes)
            and item.decision_confidence is not None
        )

    def decision_key(item: DecisionRunAssessment) -> tuple[str, ...] | None:
        if item.outcome is RunAssessmentOutcome.INVALID:
            return None
        if item.outcome is RunAssessmentOutcome.ABSTAIN:
            return (RunAssessmentOutcome.ABSTAIN.value,)
        if item.vote_target_id is None or item.vote_direction is None:
            return None
        return (
            RunAssessmentOutcome.PROPOSE.value,
            item.vote_target_id,
            item.vote_direction.value,
        )

    treatment = tuple(item for item in assessments if item.arm == treatment_arm)
    decision_counts = Counter(key for item in treatment if (key := decision_key(item)) is not None)
    majority_key: tuple[str, ...] | None = None
    if decision_counts:
        candidate_key, candidate_count = max(
            decision_counts.items(),
            key=lambda item: (item[1], item[0]),
        )
        if candidate_count >= 2:
            majority_key = candidate_key
    majority_confidences = tuple(
        item.decision_confidence
        for item in treatment
        if majority_key is not None
        and decision_key(item) == majority_key
        and item.decision_confidence is not None
    )
    minority_confidences = tuple(
        item.decision_confidence
        for item in treatment
        if majority_key is not None
        and decision_key(item) is not None
        and decision_key(item) != majority_key
        and item.decision_confidence is not None
    )
    majority_mean = mean(majority_confidences)
    minority_mean = mean(minority_confidences)
    confidence_gap = (
        None
        if majority_mean is None or minority_mean is None
        else round(majority_mean - minority_mean, 6)
    )
    return {
        "scope": "observational_only_no_sizing_approval_or_policy_effect",
        "reported_count": sum(item.decision_confidence is not None for item in assessments),
        "missing_count": sum(item.decision_confidence is None for item in assessments),
        "first_two_agree_by_arm": {
            control_arm: _first_two_assessments_agree(assessments, arm=control_arm),
            treatment_arm: _first_two_assessments_agree(assessments, arm=treatment_arm),
        },
        "first_two_mean_confidence_by_arm": {
            control_arm: mean(arm_values(control_arm, {1, 2})),
            treatment_arm: mean(arm_values(treatment_arm, {1, 2})),
        },
        "overall_mean_confidence_by_arm": {
            control_arm: mean(arm_values(control_arm)),
            treatment_arm: mean(arm_values(treatment_arm)),
        },
        "third_pair_confidence_by_arm": {
            control_arm: mean(arm_values(control_arm, {3})),
            treatment_arm: mean(arm_values(treatment_arm, {3})),
        },
        "treatment_majority_mean_confidence": majority_mean,
        "treatment_minority_mean_confidence": minority_mean,
        "treatment_majority_minus_minority_confidence": confidence_gap,
        "outcome_calibration_status": "pending_registered_outcome_opening",
    }


def completed_run_validation_evidence_hash(result: AgentRunResult) -> str:
    judgment = result.judgment
    metrics = result.metrics
    event = result.validation_event
    if judgment is None or metrics is None or event is None:
        raise ValueError("completed run lacks Judgment, metrics, or validation event")
    canonical_event = runtime_event_from_dict(event.to_dict())
    if (
        canonical_event.run_id != result.run_id
        or canonical_event.event_id != f"{result.run_id}.proposal.validated"
        or canonical_event.event_type != "judgment.validated"
        or canonical_event.event_hash != judgment.journal_hash
        or not judgment.started_at <= canonical_event.observed_at <= judgment.finished_at
    ):
        raise ValueError("run validation event does not bind the Judgment")
    payload = canonical_event.payload
    if set(payload) != {"proposal_hash", "transcript_hash", "metrics_hash", "metrics"}:
        raise ValueError("run validation event has an unexpected contract")
    expected_metrics = metrics.to_dict()
    if (
        payload.get("proposal_hash") != canonical_hash(judgment.proposal.to_dict())
        or payload.get("transcript_hash") != judgment.transcript_hash
        or payload.get("metrics") != expected_metrics
        or payload.get("metrics_hash") != canonical_hash(expected_metrics)
        or result.metrics_hash != canonical_hash(expected_metrics)
    ):
        raise ValueError("run validation event does not bind exact metrics and Judgment content")
    return canonical_hash(canonical_event.to_dict())


def build_signal_from_decision_manifest(
    *,
    manifest: DecisionRunManifest,
    evidence_pack: EvidencePack,
    judgments: tuple[JudgmentArtifact, ...],
    valid_from: datetime,
    expires_at: datetime,
) -> SignalIntent:
    if manifest.disposition is not DecisionDisposition.PROPOSE:
        raise PermissionError("abstaining Decision Run Manifest cannot create a Signal")
    if manifest.evidence_pack_id != evidence_pack.pack_id:
        raise ValueError("Decision Run Manifest binds a different Evidence Pack")
    if valid_from < manifest.created_at:
        raise ValueError("Signal cannot predate its Decision Run Manifest")
    by_id = {item.artifact_id: item for item in judgments}
    if set(by_id) != set(manifest.agreeing_judgment_artifact_ids):
        raise ValueError("Signal requires the exact agreeing treatment Judgments")
    evidence_refs: set[str] = set()
    invalidations: set[str] = set()
    for artifact_id in manifest.agreeing_judgment_artifact_ids:
        artifact = by_id[artifact_id]
        if canonical_hash(artifact.to_dict()) != next(
            item.judgment_artifact_hash
            for item in manifest.assessments
            if item.judgment_artifact_id == artifact_id
        ):
            raise ValueError("agreeing Judgment content differs from the Decision Run Manifest")
        artifact.validate_against(evidence_pack)
        candidates = tuple(
            item
            for item in artifact.proposal.candidates
            if item.target_id == manifest.agreement_target_id
            and item.direction is manifest.agreement_direction
        )
        if len(candidates) != 1:
            raise ValueError("agreeing Judgment no longer contains the selected vote")
        candidate = candidates[0]
        evidence_refs.update(candidate.evidence_refs)
        evidence_refs.update(candidate.counterevidence_refs)
        invalidations.update(candidate.invalidation_conditions)
    side = Side.BUY if manifest.agreement_direction is CandidateDirection.UP else Side.SELL
    core = {
        "decision_run_manifest_id": manifest.manifest_id,
        "agreeing_judgment_artifact_ids": list(manifest.agreeing_judgment_artifact_ids),
        "target_id": manifest.agreement_target_id,
        "side": side.value,
        "valid_from": _timestamp(valid_from),
        "expires_at": _timestamp(expires_at),
    }
    return SignalIntent(
        signal_id=f"signal-{canonical_hash(core)}",
        event_id=evidence_pack.event_id,
        instrument_id=manifest.agreement_target_id or "",
        side=side,
        valid_from=valid_from,
        expires_at=expires_at,
        evidence_refs=tuple(sorted(evidence_refs)),
        invalidation_conditions=tuple(sorted(invalidations)),
    )


@dataclass(frozen=True, slots=True)
class DecisionAdmission:
    admission_id: str
    disposition: DecisionDisposition
    decision_run_manifest_id: str
    decision_run_manifest_hash: str
    query_gate_result_id: str
    query_gate_result_hash: str
    evidence_pack_id: str
    evidence_pack_hash: str
    signal_id: str | None
    signal_intent_hash: str | None
    order_intent_hash: str | None
    agreeing_judgment_artifact_ids: tuple[str, ...]
    paper_approval_mode: str
    created_at: datetime
    claim_scope: str = DECISION_CLAIM_SCOPE
    alpha_claim: bool = False
    live_capability: bool = False
    execution_authority: bool = False
    schema_version: str = DECISION_ADMISSION_SCHEMA

    def __post_init__(self) -> None:
        if self.schema_version != DECISION_ADMISSION_SCHEMA:
            raise ValueError("unsupported Decision Admission schema")
        _prefixed_hash(
            self.decision_run_manifest_id,
            "decision-run-manifest-",
            "Decision Admission run manifest ID",
        )
        _sha256(self.decision_run_manifest_hash, "Decision Admission run manifest hash")
        _prefixed_hash(
            self.query_gate_result_id,
            "prospective-query-gate-",
            "Decision Admission Query Gate ID",
        )
        _sha256(self.query_gate_result_hash, "Decision Admission Query Gate hash")
        _prefixed_hash(
            self.evidence_pack_id,
            "evidence-pack-",
            "Decision Admission Evidence Pack ID",
        )
        _sha256(self.evidence_pack_hash, "Decision Admission Evidence Pack hash")
        if self.agreeing_judgment_artifact_ids != tuple(
            sorted(set(self.agreeing_judgment_artifact_ids))
        ):
            raise ValueError("Decision Admission agreeing Judgment IDs must be sorted and unique")
        for artifact_id in self.agreeing_judgment_artifact_ids:
            _prefixed_hash(
                artifact_id,
                "judgment-",
                "Decision Admission agreeing Judgment ID",
            )
        _strict_utc(self.created_at, "Decision Admission created_at")
        if self.paper_approval_mode != "manual_each":
            raise ValueError("initial Decision Admission requires manual_each approval")
        if self.disposition is DecisionDisposition.PROPOSE:
            if not all((self.signal_id, self.signal_intent_hash, self.order_intent_hash)):
                raise ValueError("proposed Decision Admission requires Signal and Order bindings")
            assert self.signal_id is not None
            assert self.signal_intent_hash is not None
            assert self.order_intent_hash is not None
            if not self.signal_id.startswith("signal-"):
                raise ValueError("Decision Admission Signal ID is invalid")
            _sha256(self.signal_intent_hash, "Decision Admission Signal hash")
            _sha256(self.order_intent_hash, "Decision Admission Order hash")
        elif any((self.signal_id, self.signal_intent_hash, self.order_intent_hash)):
            raise ValueError("abstaining Decision Admission cannot bind Signal or Order")
        if self.claim_scope != DECISION_CLAIM_SCOPE:
            raise ValueError("Decision Admission claim scope is invalid")
        if self.alpha_claim or self.live_capability or self.execution_authority:
            raise ValueError("Decision Admission cannot grant alpha, live, or execution authority")
        if self.admission_id != self.expected_admission_id:
            raise ValueError("Decision Admission ID does not match content")

    @property
    def expected_admission_id(self) -> str:
        return f"decision-admission-{canonical_hash(self.core_dict())}"

    def core_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "disposition": self.disposition.value,
            "decision_run_manifest_id": self.decision_run_manifest_id,
            "decision_run_manifest_hash": self.decision_run_manifest_hash,
            "query_gate_result_id": self.query_gate_result_id,
            "query_gate_result_hash": self.query_gate_result_hash,
            "evidence_pack_id": self.evidence_pack_id,
            "evidence_pack_hash": self.evidence_pack_hash,
            "signal_id": self.signal_id,
            "signal_intent_hash": self.signal_intent_hash,
            "order_intent_hash": self.order_intent_hash,
            "agreeing_judgment_artifact_ids": list(self.agreeing_judgment_artifact_ids),
            "paper_approval_mode": self.paper_approval_mode,
            "created_at": _timestamp(self.created_at),
            "claim_scope": self.claim_scope,
            "alpha_claim": self.alpha_claim,
            "live_capability": self.live_capability,
            "execution_authority": self.execution_authority,
        }

    def to_dict(self) -> dict[str, object]:
        return {**self.core_dict(), "admission_id": self.admission_id}

    def assert_matches(
        self,
        *,
        manifest: DecisionRunManifest,
        query_gate: ProspectiveQueryGateResult,
        evidence_pack: EvidencePack,
        signal: SignalIntent | None,
        order: OrderIntent | None,
    ) -> None:
        if (
            self.decision_run_manifest_id != manifest.manifest_id
            or self.decision_run_manifest_hash != canonical_hash(manifest.to_dict())
        ):
            raise ValueError("Decision Admission binds different Decision Run content")
        if (
            self.query_gate_result_id != query_gate.result_id
            or self.query_gate_result_hash != canonical_hash(query_gate.to_dict())
        ):
            raise ValueError("Decision Admission binds different Query Gate content")
        if (
            self.evidence_pack_id != evidence_pack.pack_id
            or self.evidence_pack_hash != canonical_hash(evidence_pack.to_dict())
        ):
            raise ValueError("Decision Admission binds different Evidence Pack content")
        if (
            manifest.query_gate_result_id != query_gate.result_id
            or manifest.query_gate_result_hash != canonical_hash(query_gate.to_dict())
            or manifest.evidence_pack_id != evidence_pack.pack_id
            or manifest.evidence_pack_hash != canonical_hash(evidence_pack.to_dict())
        ):
            raise ValueError("Decision Admission transitive bindings are inconsistent")
        if not query_gate.model_run_eligible:
            raise PermissionError("Decision Admission requires an eligible Query Gate")
        if self.disposition is not manifest.disposition:
            raise ValueError("Decision Admission disposition differs from its run manifest")
        if self.agreeing_judgment_artifact_ids != (manifest.agreeing_judgment_artifact_ids):
            raise ValueError("Decision Admission agreeing Judgments differ from its run manifest")
        if self.disposition is DecisionDisposition.ABSTAIN:
            if signal is not None or order is not None:
                raise PermissionError("abstaining Decision Admission cannot reach paper execution")
            return
        if signal is None or order is None:
            raise ValueError("proposed Decision Admission requires Signal and Order")
        if self.signal_id != signal.signal_id or self.signal_intent_hash != canonical_hash(
            signal.to_dict()
        ):
            raise ValueError("Decision Admission binds different Signal content")
        if self.order_intent_hash != canonical_hash(order.to_dict()):
            raise ValueError("Decision Admission binds different Order content")
        expected_side = (
            Side.BUY if manifest.agreement_direction is CandidateDirection.UP else Side.SELL
        )
        if (
            signal.event_id != evidence_pack.event_id
            or signal.instrument_id != manifest.agreement_target_id
            or signal.side is not expected_side
        ):
            raise ValueError("Decision Admission Signal differs from treatment agreement")
        if not set(signal.evidence_refs) <= {item.evidence_id for item in evidence_pack.evidence}:
            raise ValueError("Decision Admission Signal evidence is outside its Evidence Pack")
        if (
            order.signal_id != signal.signal_id
            or order.instrument_id != signal.instrument_id
            or order.side is not signal.side
        ):
            raise ValueError("Decision Admission Signal and Order differ")
        if not signal.valid_from <= order.created_at < signal.expires_at:
            raise PermissionError("Decision Admission Order was created outside Signal validity")
        if signal.valid_from < manifest.created_at or order.created_at < manifest.created_at:
            raise ValueError("Decision Admission Signal and Order cannot predate consensus")
        if order.expires_at > signal.expires_at:
            raise PermissionError("Decision Admission Order outlives its Signal")
        if self.created_at < order.created_at:
            raise ValueError("Decision Admission cannot predate its Order")
        if order.environment is not TradingEnvironment.PAPER:
            raise PermissionError("Decision Admission is paper-only")


def prepare_decision_admission(
    *,
    manifest: DecisionRunManifest,
    query_gate: ProspectiveQueryGateResult,
    evidence_pack: EvidencePack,
    signal: SignalIntent | None,
    order: OrderIntent | None,
    created_at: datetime,
) -> DecisionAdmission:
    _strict_utc(created_at, "Decision Admission created_at")
    if created_at < manifest.created_at:
        raise ValueError("Decision Admission cannot predate its run manifest")
    signal_hash = None if signal is None else canonical_hash(signal.to_dict())
    order_hash = None if order is None else canonical_hash(order.to_dict())
    core = {
        "schema_version": DECISION_ADMISSION_SCHEMA,
        "disposition": manifest.disposition.value,
        "decision_run_manifest_id": manifest.manifest_id,
        "decision_run_manifest_hash": canonical_hash(manifest.to_dict()),
        "query_gate_result_id": query_gate.result_id,
        "query_gate_result_hash": canonical_hash(query_gate.to_dict()),
        "evidence_pack_id": evidence_pack.pack_id,
        "evidence_pack_hash": canonical_hash(evidence_pack.to_dict()),
        "signal_id": None if signal is None else signal.signal_id,
        "signal_intent_hash": signal_hash,
        "order_intent_hash": order_hash,
        "agreeing_judgment_artifact_ids": list(manifest.agreeing_judgment_artifact_ids),
        "paper_approval_mode": "manual_each",
        "created_at": _timestamp(created_at),
        "claim_scope": DECISION_CLAIM_SCOPE,
        "alpha_claim": False,
        "live_capability": False,
        "execution_authority": False,
    }
    admission = DecisionAdmission(
        admission_id=f"decision-admission-{canonical_hash(core)}",
        disposition=manifest.disposition,
        decision_run_manifest_id=manifest.manifest_id,
        decision_run_manifest_hash=canonical_hash(manifest.to_dict()),
        query_gate_result_id=query_gate.result_id,
        query_gate_result_hash=canonical_hash(query_gate.to_dict()),
        evidence_pack_id=evidence_pack.pack_id,
        evidence_pack_hash=canonical_hash(evidence_pack.to_dict()),
        signal_id=None if signal is None else signal.signal_id,
        signal_intent_hash=signal_hash,
        order_intent_hash=order_hash,
        agreeing_judgment_artifact_ids=manifest.agreeing_judgment_artifact_ids,
        paper_approval_mode="manual_each",
        created_at=created_at,
    )
    admission.assert_matches(
        manifest=manifest,
        query_gate=query_gate,
        evidence_pack=evidence_pack,
        signal=signal,
        order=order,
    )
    return admission


def _strict_utc(value: datetime, name: str) -> None:
    require_aware(value, name)
    if value.utcoffset() != UTC.utcoffset(value):
        raise ValueError(f"{name} must use UTC")


def _sha256(value: str, name: str) -> None:
    if len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
        raise ValueError(f"{name} must be lowercase SHA-256 text")


def _prefixed_hash(value: str, prefix: str, name: str) -> None:
    if not value.startswith(prefix):
        raise ValueError(f"{name} must start with {prefix}")
    _sha256(value.removeprefix(prefix), name)


def _timestamp(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")
