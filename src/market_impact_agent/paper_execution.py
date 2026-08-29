from __future__ import annotations

import os
import sqlite3
import uuid
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from enum import StrEnum
from pathlib import Path
from tempfile import TemporaryDirectory
from types import MappingProxyType
from typing import cast

from market_impact_agent.agent_contracts import (
    EvidencePack,
    JudgmentArtifact,
    canonical_hash,
    evidence_pack_from_dict,
)
from market_impact_agent.agent_engine import CompletedAgentRunAuthority
from market_impact_agent.agent_ensemble import execution_binding_hash
from market_impact_agent.data_inputs import (
    LocalDataSnapshotStore,
    data_snapshot_from_dict,
)
from market_impact_agent.decision_admission import (
    DecisionAdmission,
    DecisionDisposition,
    DecisionRunManifest,
    PairedDecisionRun,
    build_signal_from_decision_manifest,
    completed_run_validation_evidence_hash,
)
from market_impact_agent.domain import (
    ApprovalMode,
    ExecutionReceipt,
    HardPolicyOutcome,
    OrderIntent,
    SignalIntent,
    TradingEnvironment,
    TradingMandate,
    require_aware,
)
from market_impact_agent.policy import HardPolicyEvaluator
from market_impact_agent.prospective_checkpoint_sets import (
    ProspectiveCheckpointSnapshotSet,
    prospective_checkpoint_snapshot_set_from_dict,
)
from market_impact_agent.prospective_diagnostic import (
    ProspectiveDiagnosticRegistration,
    prospective_diagnostic_registration_from_dict,
)
from market_impact_agent.prospective_execution import (
    ProspectiveExecutionPlan,
    prospective_execution_plan_from_dict,
)
from market_impact_agent.prospective_query_gate import (
    ProspectiveQueryGateResult,
    build_query_gate_evaluation_material,
    evaluate_prospective_query_gate,
)
from market_impact_agent.providers import (
    Capability,
    ExecutionProvider,
    SubmissionCapability,
    _issue_submission_capability,  # pyright: ignore[reportPrivateUsage]
)
from market_impact_agent.runtime_store import ArtifactStore, runtime_event_from_dict


class ApprovalState(StrEnum):
    DENIED = "denied"
    PENDING_APPROVAL = "pending_approval"
    APPROVED = "approved"
    REJECTED = "rejected"
    EXPIRED = "expired"


class OutboxState(StrEnum):
    QUEUED = "queued"
    SUBMITTING = "submitting"
    UNKNOWN = "unknown"
    ACCEPTED = "accepted"
    RECONCILED = "reconciled"
    EXPIRED = "expired"


@dataclass(frozen=True, slots=True)
class PriceBasis:
    instrument_id: str
    currency: str
    unit: str
    basis_kind: str
    price: Decimal
    source_id: str
    source_version: str
    observed_at: datetime
    valid_until: datetime

    def __post_init__(self) -> None:
        for name in (
            "instrument_id",
            "currency",
            "unit",
            "basis_kind",
            "source_id",
            "source_version",
        ):
            value = cast(str, getattr(self, name))
            if not value or value != value.strip():
                raise ValueError(f"{name} must be a non-empty trimmed string")
        if not self.price.is_finite() or self.price <= 0:
            raise ValueError("price must be finite and positive")
        require_aware(self.observed_at, "observed_at")
        require_aware(self.valid_until, "valid_until")
        if self.valid_until <= self.observed_at:
            raise ValueError("valid_until must be after observed_at")

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": "market-impact.price-basis.v1",
            "instrument_id": self.instrument_id,
            "currency": self.currency,
            "unit": self.unit,
            "basis_kind": self.basis_kind,
            "price": str(self.price),
            "source_id": self.source_id,
            "source_version": self.source_version,
            "observed_at": _timestamp(self.observed_at),
            "valid_until": _timestamp(self.valid_until),
        }


@dataclass(frozen=True, slots=True)
class PaperIntentRecord:
    client_order_id: str
    order_hash: str
    agent_admission_hash: str | None
    mandate_hash: str
    price_basis_hash: str
    policy_evaluation_hash: str
    approval_hash: str | None
    approval_state: ApprovalState
    outbox_state: OutboxState | None
    provider_order_id: str | None
    provider_status: str | None
    fill_status: str | None
    updated_at: datetime


@dataclass(frozen=True, slots=True)
class ReconciliationRun:
    reconciliation_hash: str
    complete: bool
    gaps: tuple[str, ...]
    observed_at: datetime


class PaperExecutionService:
    """Harness-owned durable paper admission, outbox, and reconciliation seam."""

    def __init__(
        self,
        root: Path,
        *,
        provider: ExecutionProvider,
        mandate: TradingMandate,
        price_source: Callable[[OrderIntent], PriceBasis | None],
        policy: HardPolicyEvaluator | None = None,
        clock: Callable[[], datetime] | None = None,
        lease_timeout_seconds: int = 30,
        agent_run_authorities: Mapping[str, CompletedAgentRunAuthority] | None = None,
    ) -> None:
        if lease_timeout_seconds < 1:
            raise ValueError("lease_timeout_seconds must be positive")
        provider.manifest.assert_valid()
        if (
            not provider.manifest.enabled
            or TradingEnvironment.PAPER not in provider.manifest.environments
            or Capability.PAPER_EXECUTION not in provider.manifest.verified_capabilities
            or Capability.LIVE_EXECUTION in provider.manifest.verified_capabilities
        ):
            raise PermissionError("provider is not an enabled paper-only execution provider")
        if mandate.environment is not TradingEnvironment.PAPER:
            raise PermissionError("paper execution requires a paper Trading Mandate")
        self.root = root.resolve()
        self.root.mkdir(mode=0o700, parents=True, exist_ok=True)
        self.artifacts = ArtifactStore(self.root / "artifacts")
        self.database_path = self.root / "paper-execution.sqlite3"
        self.provider = provider
        self.mandate = mandate
        self.price_source = price_source
        self.policy = policy or HardPolicyEvaluator()
        self.clock = clock or (lambda: datetime.now(UTC))
        self.lease_timeout_seconds = lease_timeout_seconds
        self.__agent_run_authorities = MappingProxyType(dict(agent_run_authorities or {}))
        self._initialize()
        os.chmod(self.root, 0o700)
        os.chmod(self.database_path, 0o600)
        self._block_for_unreconciled_state()
        self.provider.bind_submission_validator(self._validate_submission_capability)

    @property
    def execution_blocked(self) -> bool:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT blocked FROM paper_execution_gate WHERE singleton = 1"
            ).fetchone()
        if row is None:
            raise RuntimeError("paper execution gate is missing")
        return bool(row["blocked"])

    def admit(self, order: OrderIntent) -> PaperIntentRecord:
        return self._admit(order, agent_admission_hash=None)

    def admit_decision(
        self,
        order: OrderIntent,
        admission: DecisionAdmission,
        *,
        manifest: DecisionRunManifest,
        query_gate: ProspectiveQueryGateResult,
        evidence_pack: EvidencePack,
        registration: ProspectiveDiagnosticRegistration,
        snapshot_set: ProspectiveCheckpointSnapshotSet,
        decision_inputs: tuple[Mapping[str, object], ...],
        snapshot_store: LocalDataSnapshotStore,
        execution_plan: ProspectiveExecutionPlan,
        signal: SignalIntent,
        paired_runs: tuple[PairedDecisionRun, ...],
    ) -> PaperIntentRecord:
        if admission.disposition is not DecisionDisposition.PROPOSE:
            raise PermissionError("abstaining Decision Admission cannot reach paper execution")
        if self.mandate.approval_mode is not ApprovalMode.MANUAL_EACH:
            raise PermissionError("initial Agent-directed paper requires manual_each approval")
        recomputed_query_gate = evaluate_prospective_query_gate(
            registration=registration,
            snapshot_set=snapshot_set,
            evidence_pack=evidence_pack,
            decision_inputs=decision_inputs,
            snapshot_store=snapshot_store,
            execution_plan=execution_plan,
            model_profile_id=query_gate.model_profile_id,
            model_cost_limit_usd=Decimal(query_gate.model_cost_limit_usd),
            evaluated_at=query_gate.evaluated_at,
        )
        if recomputed_query_gate.to_dict() != query_gate.to_dict():
            raise ValueError("paper admission Query Gate was not deterministically evaluated")
        admission.assert_matches(
            manifest=manifest,
            query_gate=query_gate,
            evidence_pack=evidence_pack,
            signal=signal,
            order=order,
        )
        if (
            query_gate.agent_execution_plan_id != execution_plan.plan_id
            or query_gate.agent_execution_plan_hash != canonical_hash(execution_plan.to_dict())
            or manifest.agent_execution_plan_id != execution_plan.plan_id
            or manifest.agent_execution_plan_hash != canonical_hash(execution_plan.to_dict())
        ):
            raise ValueError("paper admission Agent execution plan is not exact")
        if (
            manifest.registration_id != registration.registration_id
            or manifest.registration_hash != canonical_hash(registration.to_dict())
            or (manifest.control_arm, manifest.treatment_arm) != registration.paired_arms
        ):
            raise ValueError("paper admission Decision Run registration is not exact")
        expected_keys = tuple((item.arm, item.replicate_index) for item in manifest.assessments)
        if tuple((item.arm, item.replicate_index) for item in paired_runs) != expected_keys:
            raise ValueError("paper admission requires all six canonical paired runs")
        judgments: list[JudgmentArtifact] = []
        for paired, assessment in zip(paired_runs, manifest.assessments, strict=True):
            result = paired.result
            judgment = result.judgment
            if judgment is None or result.metrics is None:
                raise ValueError("paper admission requires sealed Judgment and metrics artifacts")
            binding = execution_plan.arm_binding(paired.arm)
            authority = self.__agent_run_authorities.get(binding.binding_hash)
            if authority is None:
                raise PermissionError("paper admission lacks the Harness-bound Agent run authority")
            authority.assert_authoritative_completed_run(
                result,
                execution_binding=binding,
            )
            if (
                assessment.run_id != result.run_id
                or assessment.judgment_artifact_id != judgment.artifact_id
                or assessment.judgment_artifact_hash != canonical_hash(judgment.to_dict())
                or assessment.execution_binding_hash
                != execution_binding_hash(judgment, runtime_ref=binding.runtime_ref)
                or assessment.execution_binding_hash != binding.binding_hash
                or judgment.provider_id != execution_plan.provider_id
                or judgment.model != execution_plan.model
                or judgment.started_at < query_gate.evaluated_at
                or judgment.started_at > judgment.finished_at
                or judgment.finished_at > manifest.created_at
                or assessment.metrics_hash != canonical_hash(result.metrics.to_dict())
                or result.metrics_hash != assessment.metrics_hash
                or assessment.run_validation_evidence_hash
                != completed_run_validation_evidence_hash(result)
                or assessment.estimated_cost_microusd != result.metrics.estimated_cost_microusd
            ):
                raise ValueError("paper admission paired run content is not exact")
            judgments.append(judgment)
        agreeing_judgments = tuple(
            item
            for item in judgments
            if item.artifact_id in manifest.agreeing_judgment_artifact_ids
        )
        expected_signal = build_signal_from_decision_manifest(
            manifest=manifest,
            evidence_pack=evidence_pack,
            judgments=agreeing_judgments,
            valid_from=signal.valid_from,
            expires_at=signal.expires_at,
        )
        if canonical_hash(expected_signal.to_dict()) != canonical_hash(signal.to_dict()):
            raise ValueError("paper admission Signal is not the exact treatment consensus")
        query_gate_artifact = self.artifacts.put_json(query_gate.to_dict())
        evaluation_material_artifact = self.artifacts.put_json(
            build_query_gate_evaluation_material(
                registration=registration,
                snapshot_set=snapshot_set,
                decision_inputs=decision_inputs,
                snapshot_store=snapshot_store,
            )
        )
        evidence_pack_artifact = self.artifacts.put_json(evidence_pack.to_dict())
        execution_plan_artifact = self.artifacts.put_json(execution_plan.to_dict())
        manifest_artifact = self.artifacts.put_json(manifest.to_dict())
        signal_artifact = self.artifacts.put_json(signal.to_dict())
        for paired, judgment, assessment in zip(
            paired_runs,
            judgments,
            manifest.assessments,
            strict=True,
        ):
            self.artifacts.put_json(judgment.to_dict())
            assert paired.result.metrics is not None
            metrics_artifact = self.artifacts.put_json(paired.result.metrics.to_dict())
            if metrics_artifact.content_hash != paired.result.metrics_hash:
                raise ValueError("paper admission metrics hash is not exact")
            assert paired.result.validation_event is not None
            validation_artifact = self.artifacts.put_json(paired.result.validation_event.to_dict())
            if validation_artifact.content_hash != assessment.run_validation_evidence_hash:
                raise ValueError("paper admission run validation evidence is not exact")
        if query_gate_artifact.content_hash != admission.query_gate_result_hash:
            raise ValueError("Decision Admission Query Gate hash is not exact")
        if evaluation_material_artifact.content_hash != query_gate.evaluation_material_hash:
            raise ValueError("Decision Admission Query Gate evaluation material is not exact")
        if signal_artifact.content_hash != admission.signal_intent_hash:
            raise ValueError("Decision Admission Signal hash is not exact")
        if evidence_pack_artifact.content_hash != admission.evidence_pack_hash:
            raise ValueError("Decision Admission Evidence Pack hash is not exact")
        if manifest_artifact.content_hash != admission.decision_run_manifest_hash:
            raise ValueError("Decision Admission run manifest hash is not exact")
        if execution_plan_artifact.content_hash != query_gate.agent_execution_plan_hash:
            raise ValueError("Decision Admission execution plan hash is not exact")
        admission_artifact = self.artifacts.put_json(admission.to_dict())
        return self._admit(order, agent_admission_hash=admission_artifact.content_hash)

    def _admit(
        self,
        order: OrderIntent,
        *,
        agent_admission_hash: str | None,
    ) -> PaperIntentRecord:
        now = self.clock()
        require_aware(now, "now")
        if order.environment is not TradingEnvironment.PAPER:
            raise PermissionError("paper execution accepts paper Order Intents only")

        order_artifact = self.artifacts.put_json(_order_dict(order))
        mandate_artifact = self.artifacts.put_json(_mandate_dict(self.mandate))
        with self._connect() as connection:
            existing = connection.execute(
                "SELECT * FROM paper_intents WHERE client_order_id = ?",
                (order.client_order_id,),
            ).fetchone()
        if existing is not None:
            return self._matching_existing_admission(
                existing,
                order_hash=order_artifact.content_hash,
                mandate_hash=mandate_artifact.content_hash,
                agent_admission_hash=agent_admission_hash,
            )

        basis = self.price_source(order)
        if basis is None:
            raise PermissionError("a complete price basis is required before admission")
        if (
            basis.instrument_id != order.instrument_id
            or basis.observed_at > now
            or not basis.observed_at <= now < basis.valid_until
        ):
            raise PermissionError("price basis is mismatched, future-dated, or stale")

        price_artifact = self.artifacts.put_json(basis.to_dict())
        decision = self.policy.evaluate(
            order,
            self.mandate,
            now=now,
            reference_price=basis.price,
        )
        reasons = decision.reasons
        outcome = decision.outcome
        if (
            self.mandate.approval_mode
            in {
                ApprovalMode.POLICY_AUTO,
                ApprovalMode.AUTONOMOUS,
            }
            and outcome is HardPolicyOutcome.ELIGIBLE
        ):
            outcome = HardPolicyOutcome.DENY
            reasons = (f"{self.mandate.approval_mode.value}_not_implemented",)
        policy_payload = {
            "schema_version": "market-impact.hard-policy-evaluation.v1",
            "outcome": outcome.value,
            "reasons": list(reasons),
            "evaluated_at": _timestamp(now),
            "evaluator_version": "hard-policy-v1",
        }
        policy_artifact = self.artifacts.put_json(policy_payload)

        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            existing = connection.execute(
                "SELECT * FROM paper_intents WHERE client_order_id = ?",
                (order.client_order_id,),
            ).fetchone()
            if existing is not None:
                connection.commit()
                return self._matching_existing_admission(
                    existing,
                    order_hash=order_artifact.content_hash,
                    mandate_hash=mandate_artifact.content_hash,
                    agent_admission_hash=agent_admission_hash,
                )
            gate = connection.execute(
                "SELECT blocked FROM paper_execution_gate WHERE singleton = 1"
            ).fetchone()
            if gate is None:
                connection.rollback()
                raise RuntimeError("paper execution gate is missing")
            if bool(gate["blocked"]):
                connection.rollback()
                raise PermissionError("paper execution is blocked pending reconciliation")

            if outcome is HardPolicyOutcome.DENY:
                approval_state = ApprovalState.DENIED
                outbox_state = None
                approval_hash = None
            elif outcome is HardPolicyOutcome.REQUIRE_MANUAL:
                approval_state = ApprovalState.PENDING_APPROVAL
                outbox_state = None
                approval_hash = None
            else:
                approval_state = ApprovalState.APPROVED
                outbox_state = OutboxState.QUEUED
                approval_hash = self._approval_artifact(
                    order_hash=order_artifact.content_hash,
                    mandate_hash=mandate_artifact.content_hash,
                    price_basis_hash=price_artifact.content_hash,
                    policy_evaluation_hash=policy_artifact.content_hash,
                    approve=True,
                    actor_kind="harness_policy",
                    actor_ref="timeboxed-mandate",
                    decided_at=now,
                )
            connection.execute(
                """
                INSERT INTO paper_intents (
                    client_order_id, order_hash, agent_admission_hash,
                    mandate_hash, price_basis_hash,
                    policy_evaluation_hash, approval_hash, approval_state,
                    outbox_state, provider_order_id, provider_status, fill_status,
                    order_expires_at, mandate_expires_at, price_valid_until, lease_token,
                    lease_expires_at, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, NULL, ?, ?, ?, NULL, NULL, ?, ?)
                """,
                (
                    order.client_order_id,
                    order_artifact.content_hash,
                    agent_admission_hash,
                    mandate_artifact.content_hash,
                    price_artifact.content_hash,
                    policy_artifact.content_hash,
                    approval_hash,
                    approval_state.value,
                    outbox_state.value if outbox_state is not None else None,
                    _timestamp(order.expires_at),
                    _timestamp(self.mandate.expires_at),
                    _timestamp(basis.valid_until),
                    _timestamp(now),
                    _timestamp(now),
                ),
            )
            self._append_event(
                connection,
                order.client_order_id,
                "intent_admitted",
                policy_artifact.content_hash,
                now,
            )
            connection.commit()
        return self.get(order.client_order_id)

    def decide(
        self,
        client_order_id: str,
        *,
        approve: bool,
        actor_ref: str,
    ) -> PaperIntentRecord:
        decided_at = self.clock()
        require_aware(decided_at, "now")
        if not actor_ref or actor_ref != actor_ref.strip():
            raise ValueError("actor_ref must be a non-empty trimmed string")
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT * FROM paper_intents WHERE client_order_id = ?",
                (client_order_id,),
            ).fetchone()
            if row is None:
                raise KeyError(client_order_id)
            self._validate_binding_artifacts(row)
            if (
                ApprovalState(cast(str, row["approval_state"]))
                is not ApprovalState.PENDING_APPROVAL
            ):
                raise ValueError("intent is not pending manual approval")
            if (
                decided_at >= _datetime(cast(str, row["order_expires_at"]))
                or decided_at >= _datetime(cast(str, row["mandate_expires_at"]))
                or decided_at >= _datetime(cast(str, row["price_valid_until"]))
            ):
                connection.execute(
                    """
                    UPDATE paper_intents
                    SET approval_state = ?, updated_at = ?
                    WHERE client_order_id = ?
                    """,
                    (ApprovalState.EXPIRED.value, _timestamp(decided_at), client_order_id),
                )
                connection.commit()
                return self.get(client_order_id)
            approval_hash = self._approval_artifact(
                order_hash=cast(str, row["order_hash"]),
                mandate_hash=cast(str, row["mandate_hash"]),
                price_basis_hash=cast(str, row["price_basis_hash"]),
                policy_evaluation_hash=cast(str, row["policy_evaluation_hash"]),
                approve=approve,
                actor_kind="human",
                actor_ref=actor_ref,
                decided_at=decided_at,
            )
            state = ApprovalState.APPROVED if approve else ApprovalState.REJECTED
            outbox = OutboxState.QUEUED.value if approve else None
            connection.execute(
                """
                UPDATE paper_intents
                SET approval_hash = ?, approval_state = ?, outbox_state = ?, updated_at = ?
                WHERE client_order_id = ?
                """,
                (
                    approval_hash,
                    state.value,
                    outbox,
                    _timestamp(decided_at),
                    client_order_id,
                ),
            )
            self._append_event(
                connection,
                client_order_id,
                "manual_approval_decided",
                approval_hash,
                decided_at,
            )
            connection.commit()
        return self.get(client_order_id)

    def dispatch_next(self) -> PaperIntentRecord | None:
        current = self.clock()
        require_aware(current, "now")
        self._recover_expired_leases(current)
        claimed = self._claim_next(current)
        if claimed is None:
            return None
        row, submission_id = claimed
        capability = _issue_submission_capability(
            order=_order_from_dict(self.artifacts.read_json(cast(str, row["order_hash"]))),
            submission_id=submission_id,
            order_hash=cast(str, row["order_hash"]),
            mandate_hash=cast(str, row["mandate_hash"]),
            price_basis_hash=cast(str, row["price_basis_hash"]),
            policy_evaluation_hash=cast(str, row["policy_evaluation_hash"]),
            approval_hash=cast(str, row["approval_hash"]),
        )
        try:
            receipt = self.provider.submit(capability)
            if receipt.client_order_id != cast(str, row["client_order_id"]):
                raise ValueError("provider receipt client_order_id mismatch")
        except Exception as error:
            self._finish_ambiguous(
                cast(str, row["client_order_id"]),
                submission_id,
                current,
                type(error).__name__,
            )
            return self.get(cast(str, row["client_order_id"]))
        receipt_artifact = self.artifacts.put_json(_receipt_dict(receipt))
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            updated = connection.execute(
                """
                UPDATE paper_intents
                SET outbox_state = ?, provider_order_id = ?, provider_status = ?,
                    lease_token = NULL, lease_expires_at = NULL, updated_at = ?
                WHERE client_order_id = ? AND lease_token = ?
                """,
                (
                    OutboxState.ACCEPTED.value,
                    receipt.provider_order_id,
                    receipt.status.value,
                    _timestamp(current),
                    receipt.client_order_id,
                    submission_id,
                ),
            )
            if updated.rowcount != 1:
                connection.rollback()
                raise RuntimeError("paper submission lease was lost")
            connection.execute(
                """
                UPDATE paper_submission_attempts
                SET status = 'acknowledged', receipt_hash = ?, finished_at = ?
                WHERE submission_id = ?
                """,
                (receipt_artifact.content_hash, _timestamp(current), submission_id),
            )
            self._set_gate(connection, True, "reconciliation_required", current)
            self._append_event(
                connection,
                receipt.client_order_id,
                "provider_acknowledged",
                receipt_artifact.content_hash,
                current,
            )
            connection.commit()
        return self.get(receipt.client_order_id)

    def reconcile(self) -> ReconciliationRun:
        current = self.clock()
        require_aware(current, "now")
        self._recover_expired_leases(current)
        snapshot = self.provider.reconcile()
        receipts: dict[str, ExecutionReceipt] = {}
        gaps = list(snapshot.gaps)
        for receipt in snapshot.receipts:
            if receipt.client_order_id in receipts:
                gaps.append(f"duplicate_provider_order:{receipt.client_order_id}")
            receipts[receipt.client_order_id] = receipt
        snapshot_artifact = self.artifacts.put_json(snapshot.to_dict())
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            provider_matches = snapshot.provider_id == self.provider.manifest.provider_id
            if not provider_matches:
                gaps.append(f"provider_identity_mismatch:{snapshot.provider_id}")
            known_ids = {
                cast(str, row["client_order_id"])
                for row in connection.execute("SELECT client_order_id FROM paper_intents")
            }
            if provider_matches:
                for external_id in sorted(receipts.keys() - known_ids):
                    gaps.append(f"external_provider_order:{external_id}")
            rows = connection.execute(
                """
                SELECT i.client_order_id, i.outbox_state, MAX(a.started_at) AS started_at
                FROM paper_intents AS i
                LEFT JOIN paper_submission_attempts AS a
                    ON a.client_order_id = i.client_order_id
                WHERE i.outbox_state IN (?, ?, ?)
                GROUP BY i.client_order_id, i.outbox_state
                """,
                (
                    OutboxState.SUBMITTING.value,
                    OutboxState.UNKNOWN.value,
                    OutboxState.ACCEPTED.value,
                ),
            ).fetchall()
            for row in rows:
                client_order_id = cast(str, row["client_order_id"])
                if not provider_matches:
                    continue
                started_at_value = cast(str | None, row["started_at"])
                if started_at_value is None:
                    gaps.append(f"submission_attempt_missing:{client_order_id}")
                    continue
                if snapshot.observed_at < _datetime(started_at_value):
                    gaps.append(f"stale_provider_snapshot:{client_order_id}")
                    continue
                receipt = receipts.get(client_order_id)
                if receipt is not None:
                    provider_order_id = receipt.provider_order_id
                    provider_status = receipt.status.value
                elif snapshot.complete:
                    if OutboxState(cast(str, row["outbox_state"])) is OutboxState.ACCEPTED:
                        gaps.append(f"acknowledged_order_missing:{client_order_id}")
                        continue
                    provider_order_id = None
                    provider_status = "not_found"
                else:
                    continue
                connection.execute(
                    """
                    UPDATE paper_intents
                    SET outbox_state = ?, provider_order_id = ?, provider_status = ?,
                        lease_token = NULL, lease_expires_at = NULL, updated_at = ?
                    WHERE client_order_id = ?
                    """,
                    (
                        OutboxState.RECONCILED.value,
                        provider_order_id,
                        provider_status,
                        _timestamp(current),
                        client_order_id,
                    ),
                )
            unique_gaps = tuple(sorted(set(gaps)))
            complete = snapshot.complete and not unique_gaps
            self._set_gate(
                connection,
                not complete,
                None if complete else "reconciliation_incomplete",
                current,
            )
            run_payload = {
                "schema_version": "market-impact.execution-reconciliation.v1",
                "provider_id": self.provider.manifest.provider_id,
                "provider_snapshot_hash": snapshot_artifact.content_hash,
                "complete": complete,
                "gaps": list(unique_gaps),
                "observed_at": _timestamp(current),
            }
            run_artifact = self.artifacts.put_json(run_payload)
            connection.execute(
                """
                INSERT INTO paper_reconciliation_runs (
                    reconciliation_hash, complete, gaps_json, observed_at
                ) VALUES (?, ?, ?, ?)
                """,
                (
                    run_artifact.content_hash,
                    int(complete),
                    "\n".join(unique_gaps),
                    _timestamp(current),
                ),
            )
            connection.commit()
        return ReconciliationRun(
            reconciliation_hash=run_artifact.content_hash,
            complete=complete,
            gaps=unique_gaps,
            observed_at=current,
        )

    def get(self, client_order_id: str) -> PaperIntentRecord:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM paper_intents WHERE client_order_id = ?",
                (client_order_id,),
            ).fetchone()
        if row is None:
            raise KeyError(client_order_id)
        self._validate_binding_artifacts(row)
        return _record(row)

    def _matching_existing_admission(
        self,
        row: sqlite3.Row,
        *,
        order_hash: str,
        mandate_hash: str,
        agent_admission_hash: str | None,
    ) -> PaperIntentRecord:
        if cast(str, row["order_hash"]) != order_hash:
            raise ValueError("client_order_id already binds different content")
        if cast(str, row["mandate_hash"]) != mandate_hash:
            raise ValueError("client_order_id already binds a different binding")
        if cast(str | None, row["agent_admission_hash"]) != agent_admission_hash:
            raise ValueError("client_order_id already binds a different Agent admission binding")
        self._validate_binding_artifacts(row)
        return _record(row)

    def _validate_submission_capability(
        self,
        capability: SubmissionCapability,
    ) -> bool:
        now = self.clock()
        require_aware(now, "now")
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM paper_intents WHERE client_order_id = ?",
                (capability.order.client_order_id,),
            ).fetchone()
        if row is None:
            return False
        approval_hash = cast(str | None, row["approval_hash"])
        lease_expires_at = cast(str | None, row["lease_expires_at"])
        expected = (
            cast(str, row["order_hash"]),
            cast(str, row["mandate_hash"]),
            cast(str, row["price_basis_hash"]),
            cast(str, row["policy_evaluation_hash"]),
            approval_hash,
        )
        actual = (
            capability.order_hash,
            capability.mandate_hash,
            capability.price_basis_hash,
            capability.policy_evaluation_hash,
            capability.approval_hash,
        )
        if (
            OutboxState(cast(str, row["outbox_state"])) is not OutboxState.SUBMITTING
            or cast(str | None, row["lease_token"]) != capability.submission_id
            or lease_expires_at is None
            or now >= _datetime(lease_expires_at)
            or approval_hash is None
            or actual != expected
            or canonical_hash(_order_dict(capability.order)) != capability.order_hash
        ):
            return False
        try:
            self._validate_binding_artifacts(row)
        except (KeyError, OSError, TypeError, ValueError):
            return False
        return True

    def _validate_binding_artifacts(self, row: sqlite3.Row) -> None:
        for field in (
            "order_hash",
            "mandate_hash",
            "price_basis_hash",
            "policy_evaluation_hash",
        ):
            self.artifacts.get(cast(str, row[field]), media_type="application/json")
        approval_hash = cast(str | None, row["approval_hash"])
        if approval_hash is not None:
            self.artifacts.get(approval_hash, media_type="application/json")
        agent_admission_hash = cast(str | None, row["agent_admission_hash"])
        if agent_admission_hash is not None:
            admission_payload = _json_object(
                self.artifacts.read_json(agent_admission_hash),
                "Decision Admission artifact",
            )
            if _json_string(admission_payload, "schema_version") != (
                "market-impact.decision-admission.v1"
            ):
                raise ValueError("paper intent does not bind the canonical Decision Admission")
            if _json_string(admission_payload, "disposition") != "propose":
                raise ValueError("abstaining Decision Admission reached the paper outbox")
            manifest_hash = _json_string(
                admission_payload,
                "decision_run_manifest_hash",
            )
            query_gate_hash = _json_string(
                admission_payload,
                "query_gate_result_hash",
            )
            evidence_pack_hash = _json_string(
                admission_payload,
                "evidence_pack_hash",
            )
            signal_hash = _json_string(admission_payload, "signal_intent_hash")
            manifest_payload = _json_object(
                self.artifacts.read_json(manifest_hash),
                "Decision Run Manifest artifact",
            )
            query_gate_payload = _json_object(
                self.artifacts.read_json(query_gate_hash),
                "prospective Query Gate artifact",
            )
            evaluation_material_payload = _json_object(
                self.artifacts.read_json(
                    _json_string(query_gate_payload, "evaluation_material_hash")
                ),
                "prospective Query Gate evaluation material",
            )
            if _json_string(evaluation_material_payload, "schema_version") != (
                "market-impact.prospective-query-gate-evaluation-material.v1"
            ):
                raise ValueError("Query Gate evaluation material schema is not canonical")
            registration_payload = _json_object(
                evaluation_material_payload.get("registration"),
                "prospective diagnostic registration",
            )
            snapshot_set_payload = _json_object(
                evaluation_material_payload.get("checkpoint_snapshot_set"),
                "prospective checkpoint Snapshot Set",
            )
            evidence_pack = evidence_pack_from_dict(self.artifacts.read_json(evidence_pack_hash))
            execution_plan_hash = _json_string(
                query_gate_payload,
                "agent_execution_plan_hash",
            )
            execution_plan_payload = _json_object(
                self.artifacts.read_json(execution_plan_hash),
                "prospective execution plan artifact",
            )
            model_provider_profile_payload = _json_object(
                execution_plan_payload.get("model_provider_profile"),
                "Model Provider Profile",
            )
            signal_payload = _json_object(
                self.artifacts.read_json(signal_hash),
                "Signal Intent artifact",
            )
            order_payload = _json_object(
                self.artifacts.read_json(cast(str, row["order_hash"])),
                "Order Intent artifact",
            )
            if _json_string(admission_payload, "query_gate_result_id") != _json_string(
                query_gate_payload,
                "result_id",
            ):
                raise ValueError("Decision Admission Query Gate identity is not exact")
            if (
                _json_string(registration_payload, "registration_id")
                != _json_string(query_gate_payload, "registration_id")
                or _json_string(snapshot_set_payload, "snapshot_set_id")
                != _json_string(query_gate_payload, "checkpoint_snapshot_set_id")
                or _json_string(snapshot_set_payload, "registration_id")
                != _json_string(query_gate_payload, "registration_id")
                or _json_string(snapshot_set_payload, "checkpoint_key")
                != _json_string(query_gate_payload, "checkpoint_key")
            ):
                raise ValueError("Query Gate evaluation material identity is not exact")
            material_snapshots = evaluation_material_payload.get("snapshots")
            material_inputs = evaluation_material_payload.get("decision_inputs")
            if not isinstance(material_snapshots, list) or not isinstance(material_inputs, list):
                raise TypeError("Query Gate evaluation material arrays are invalid")
            snapshot_ids = tuple(
                sorted(
                    _json_string(
                        _json_object(item, "Query Gate material Snapshot"),
                        "snapshot_id",
                    )
                    for item in cast(list[object], material_snapshots)
                )
            )
            decision_input_payloads = tuple(
                _json_object(item, "Query Gate material Decision Input")
                for item in cast(list[object], material_inputs)
            )
            decision_input_ids = tuple(
                sorted(_json_string(item, "record_id") for item in decision_input_payloads)
            )
            if snapshot_ids != tuple(
                sorted(_json_string_list(query_gate_payload, "authorized_snapshot_ids"))
            ) or decision_input_ids != tuple(
                sorted(_json_string_list(query_gate_payload, "authorized_decision_input_ids"))
            ):
                raise ValueError("Query Gate evaluation material authorization is not exact")
            if any(
                _json_string(item, "checkpoint_snapshot_set_id")
                != _json_string(snapshot_set_payload, "snapshot_set_id")
                or _json_string(item, "snapshot_id") not in snapshot_ids
                for item in decision_input_payloads
            ):
                raise ValueError("Query Gate material Decision Input lineage is invalid")
            registration = prospective_diagnostic_registration_from_dict(registration_payload)
            snapshot_set = prospective_checkpoint_snapshot_set_from_dict(snapshot_set_payload)
            execution_plan = prospective_execution_plan_from_dict(execution_plan_payload)
            with TemporaryDirectory(
                prefix="query-gate-revalidation-",
                dir=self.root,
            ) as revalidation_root:
                snapshot_store = LocalDataSnapshotStore(Path(revalidation_root))
                for raw_snapshot in cast(list[object], material_snapshots):
                    snapshot_store.put(data_snapshot_from_dict(raw_snapshot))
                recomputed_query_gate = evaluate_prospective_query_gate(
                    registration=registration,
                    snapshot_set=snapshot_set,
                    evidence_pack=evidence_pack,
                    decision_inputs=decision_input_payloads,
                    snapshot_store=snapshot_store,
                    execution_plan=execution_plan,
                    model_profile_id=_json_string(query_gate_payload, "model_profile_id"),
                    model_cost_limit_usd=Decimal(
                        _json_string(query_gate_payload, "model_cost_limit_usd")
                    ),
                    evaluated_at=_datetime(_json_string(query_gate_payload, "evaluated_at")),
                )
            if recomputed_query_gate.to_dict() != query_gate_payload:
                raise ValueError("persisted prospective Query Gate does not re-evaluate exactly")
            if _json_string(admission_payload, "decision_run_manifest_id") != _json_string(
                manifest_payload,
                "manifest_id",
            ):
                raise ValueError("Decision Admission run manifest identity is not exact")
            if _json_string(manifest_payload, "query_gate_result_id") != _json_string(
                query_gate_payload,
                "result_id",
            ):
                raise ValueError("Decision Run Manifest Query Gate identity is not exact")
            if (
                _json_string(query_gate_payload, "agent_execution_plan_id")
                != _json_string(execution_plan_payload, "plan_id")
                or _json_string(manifest_payload, "agent_execution_plan_id")
                != _json_string(execution_plan_payload, "plan_id")
                or _json_string(manifest_payload, "agent_execution_plan_hash")
                != execution_plan_hash
                or _json_string(query_gate_payload, "model_profile_id")
                != _json_string(execution_plan_payload, "model_profile_alias")
            ):
                raise ValueError("Decision Run execution plan identity is not exact")
            if _json_string(query_gate_payload, "evidence_pack_id") != evidence_pack.pack_id:
                raise ValueError("Decision Admission Query Gate Evidence Pack is not exact")
            if _json_string(admission_payload, "evidence_pack_id") != evidence_pack.pack_id:
                raise ValueError("Decision Admission Evidence Pack identity is not exact")
            if _json_string(manifest_payload, "evidence_pack_id") != evidence_pack.pack_id:
                raise ValueError("Decision Run Manifest Evidence Pack identity is not exact")
            agreeing_ids = _json_string_list(
                admission_payload,
                "agreeing_judgment_artifact_ids",
            )
            if agreeing_ids != _json_string_list(
                manifest_payload,
                "agreeing_judgment_artifact_ids",
            ):
                raise ValueError("Decision Admission agreeing Judgment IDs are not exact")
            assessments = manifest_payload.get("assessments")
            if not isinstance(assessments, list):
                raise TypeError("Decision Run Manifest assessments must be an array")
            arm_bindings_value = execution_plan_payload.get("arm_bindings")
            if not isinstance(arm_bindings_value, list):
                raise TypeError("prospective execution plan arm_bindings must be an array")
            arm_bindings: dict[str, tuple[str, str]] = {}
            for raw_arm_binding in cast(list[object], arm_bindings_value):
                arm_binding = _json_object(raw_arm_binding, "execution plan arm binding")
                arm = _json_string(arm_binding, "arm")
                binding_payload = _json_object(
                    arm_binding.get("execution_binding"),
                    "Agent execution binding",
                )
                binding_hash = _json_string(arm_binding, "execution_binding_hash")
                if canonical_hash(binding_payload) != binding_hash:
                    raise ValueError("Agent execution binding hash is not exact")
                arm_bindings[arm] = (
                    binding_hash,
                    _json_string(binding_payload, "runtime_ref"),
                )
            if (
                set(arm_bindings)
                != {
                    "structured_agent_core",
                    "structured_agent_plus_routed_methods",
                }
                or len({item[0] for item in arm_bindings.values()}) != 2
            ):
                raise ValueError("prospective execution plan paired arms are invalid")
            judgment_hashes: dict[str, str] = {}
            for raw_assessment in cast(list[object], assessments):
                assessment = _json_object(raw_assessment, "Decision Run assessment")
                artifact_id = assessment.get("judgment_artifact_id")
                artifact_hash = assessment.get("judgment_artifact_hash")
                metrics_hash = assessment.get("metrics_hash")
                validation_evidence_hash = assessment.get("run_validation_evidence_hash")
                arm = _json_string(assessment, "arm")
                if (
                    not isinstance(artifact_id, str)
                    or not isinstance(artifact_hash, str)
                    or not isinstance(metrics_hash, str)
                    or not isinstance(validation_evidence_hash, str)
                ):
                    raise ValueError("proposed Decision Run lacks sealed run artifacts")
                if _json_string(assessment, "execution_binding_hash") != arm_bindings[arm][0]:
                    raise ValueError("Decision Run arm provenance is not exact")
                metrics_payload = _json_object(
                    self.artifacts.read_json(metrics_hash),
                    "Agent Run metrics artifact",
                )
                estimated_cost = metrics_payload.get("estimated_cost_microusd")
                if (
                    not isinstance(estimated_cost, int)
                    or isinstance(estimated_cost, bool)
                    or assessment.get("estimated_cost_microusd") != estimated_cost
                ):
                    raise ValueError("Decision Run cost evidence is not exact")
                artifact_payload = _json_object(
                    self.artifacts.read_json(artifact_hash),
                    "Judgment Artifact",
                )
                validation_event = runtime_event_from_dict(
                    self.artifacts.read_json(validation_evidence_hash)
                )
                if (
                    validation_event.run_id != _json_string(assessment, "run_id")
                    or validation_event.event_id
                    != f"{_json_string(assessment, 'run_id')}.proposal.validated"
                    or validation_event.event_type != "judgment.validated"
                    or validation_event.event_hash != _json_string(artifact_payload, "journal_hash")
                    or set(validation_event.payload)
                    != {"proposal_hash", "transcript_hash", "metrics_hash", "metrics"}
                    or validation_event.payload.get("proposal_hash")
                    != canonical_hash(
                        _json_object(artifact_payload.get("proposal"), "Judgment proposal")
                    )
                    or validation_event.payload.get("transcript_hash")
                    != _json_string(artifact_payload, "transcript_hash")
                    or validation_event.payload.get("metrics_hash") != metrics_hash
                    or validation_event.payload.get("metrics") != metrics_payload
                    or not _datetime(_json_string(artifact_payload, "started_at"))
                    <= validation_event.observed_at
                    <= _datetime(_json_string(artifact_payload, "finished_at"))
                ):
                    raise ValueError("Decision Run validation event is not exact")
                judgment_hashes[artifact_id] = artifact_hash
            for artifact_id, artifact_hash in judgment_hashes.items():
                artifact_payload = _json_object(
                    self.artifacts.read_json(artifact_hash),
                    "Judgment Artifact",
                )
                if _json_string(artifact_payload, "artifact_id") != artifact_id:
                    raise ValueError("paired Judgment identity is not exact")
                if _json_string(artifact_payload, "provider_id") != _json_string(
                    model_provider_profile_payload, "provider_id"
                ) or _json_string(artifact_payload, "model") != _json_string(
                    model_provider_profile_payload, "model"
                ):
                    raise ValueError("paired Judgment provider/model is not exact")
                started_at = _datetime(_json_string(artifact_payload, "started_at"))
                finished_at = _datetime(_json_string(artifact_payload, "finished_at"))
                if not (
                    _datetime(_json_string(query_gate_payload, "evaluated_at"))
                    <= started_at
                    <= finished_at
                    <= _datetime(_json_string(manifest_payload, "created_at"))
                ):
                    raise ValueError("paired Judgment chronology is not exact")
            if not set(agreeing_ids) <= set(judgment_hashes):
                raise ValueError("agreeing Judgment is absent from the run manifest")
            if _json_string(admission_payload, "signal_id") != _json_string(
                signal_payload,
                "signal_id",
            ) or _json_string(signal_payload, "signal_id") != _json_string(
                order_payload,
                "signal_id",
            ):
                raise ValueError("Decision Admission Signal identity is not exact")
            if _json_string(admission_payload, "order_intent_hash") != cast(
                str,
                row["order_hash"],
            ):
                raise ValueError("Decision Admission Order Intent hash is not exact")
            if _json_string(signal_payload, "event_id") != evidence_pack.event_id:
                raise ValueError("Decision Admission Signal event is not in the Evidence Pack")
            expected_side = (
                "buy" if _json_string(manifest_payload, "agreement_direction") == "up" else "sell"
            )
            if (
                _json_string(signal_payload, "instrument_id")
                != _json_string(manifest_payload, "agreement_target_id")
                or _json_string(signal_payload, "side") != expected_side
                or _json_string(order_payload, "instrument_id")
                != _json_string(signal_payload, "instrument_id")
                or _json_string(order_payload, "side") != expected_side
            ):
                raise ValueError("Decision Admission Signal differs from treatment agreement")
            if not (
                _datetime(_json_string(signal_payload, "valid_from"))
                <= _datetime(_json_string(order_payload, "created_at"))
                < _datetime(_json_string(signal_payload, "expires_at"))
            ) or _datetime(_json_string(order_payload, "expires_at")) > _datetime(
                _json_string(signal_payload, "expires_at")
            ):
                raise ValueError("Decision Admission Order is outside Signal validity")
            manifest_created_at = _datetime(_json_string(manifest_payload, "created_at"))
            if (
                _datetime(_json_string(signal_payload, "valid_from")) < manifest_created_at
                or _datetime(_json_string(order_payload, "created_at")) < manifest_created_at
            ):
                raise ValueError("Decision Admission Signal and Order predate consensus")
            if not set(_json_string_list(signal_payload, "evidence_refs")) <= {
                item.evidence_id for item in evidence_pack.evidence
            }:
                raise ValueError("Decision Admission Signal evidence_refs are not in Evidence Pack")
            if _json_string(signal_payload, "instrument_id") not in evidence_pack.allowed_targets:
                raise ValueError(
                    "Decision Admission Signal instrument is not an allowed Evidence Pack target"
                )
            if _json_string(signal_payload, "instrument_id") != _json_string(
                order_payload,
                "instrument_id",
            ):
                raise ValueError("Decision Admission Signal instrument differs from Order Intent")

    def _initialize(self) -> None:
        with self._connect() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS paper_intents (
                    client_order_id TEXT PRIMARY KEY,
                    order_hash TEXT NOT NULL,
                    agent_admission_hash TEXT,
                    mandate_hash TEXT NOT NULL,
                    price_basis_hash TEXT NOT NULL,
                    policy_evaluation_hash TEXT NOT NULL,
                    approval_hash TEXT,
                    approval_state TEXT NOT NULL,
                    outbox_state TEXT,
                    provider_order_id TEXT,
                    provider_status TEXT,
                    fill_status TEXT,
                    order_expires_at TEXT NOT NULL,
                    mandate_expires_at TEXT NOT NULL,
                    price_valid_until TEXT NOT NULL,
                    lease_token TEXT,
                    lease_expires_at TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS paper_submission_attempts (
                    submission_id TEXT PRIMARY KEY,
                    client_order_id TEXT NOT NULL REFERENCES paper_intents(client_order_id),
                    status TEXT NOT NULL,
                    receipt_hash TEXT,
                    error_kind TEXT,
                    started_at TEXT NOT NULL,
                    finished_at TEXT
                );
                CREATE TABLE IF NOT EXISTS paper_execution_events (
                    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                    client_order_id TEXT NOT NULL REFERENCES paper_intents(client_order_id),
                    event_type TEXT NOT NULL,
                    artifact_hash TEXT NOT NULL,
                    observed_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS paper_reconciliation_runs (
                    reconciliation_hash TEXT PRIMARY KEY,
                    complete INTEGER NOT NULL,
                    gaps_json TEXT NOT NULL,
                    observed_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS paper_execution_gate (
                    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                    blocked INTEGER NOT NULL,
                    reason TEXT,
                    updated_at TEXT NOT NULL
                );
                INSERT OR IGNORE INTO paper_execution_gate (
                    singleton, blocked, reason, updated_at
                ) VALUES (1, 0, NULL, '1970-01-01T00:00:00Z');
                """
            )
            columns = {
                cast(str, row["name"])
                for row in connection.execute("PRAGMA table_info(paper_intents)").fetchall()
            }
            if "agent_admission_hash" not in columns:
                connection.execute("ALTER TABLE paper_intents ADD COLUMN agent_admission_hash TEXT")

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.database_path, timeout=30)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA synchronous=FULL")
        connection.execute("PRAGMA foreign_keys=ON")
        return connection

    def _block_for_unreconciled_state(self) -> None:
        now = self.clock()
        require_aware(now, "now")
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT 1 FROM paper_intents
                WHERE outbox_state IN (?, ?, ?)
                LIMIT 1
                """,
                (
                    OutboxState.SUBMITTING.value,
                    OutboxState.UNKNOWN.value,
                    OutboxState.ACCEPTED.value,
                ),
            ).fetchone()
            if row is not None:
                self._set_gate(connection, True, "reconciliation_required", now)
                connection.commit()

    def _claim_next(self, now: datetime) -> tuple[sqlite3.Row, str] | None:
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            gate = connection.execute(
                "SELECT blocked FROM paper_execution_gate WHERE singleton = 1"
            ).fetchone()
            if gate is None:
                connection.rollback()
                raise RuntimeError("paper execution gate is missing")
            if bool(gate["blocked"]):
                connection.commit()
                return None
            row = connection.execute(
                """
                SELECT * FROM paper_intents
                WHERE outbox_state = ?
                ORDER BY created_at, client_order_id
                LIMIT 1
                """,
                (OutboxState.QUEUED.value,),
            ).fetchone()
            if row is None:
                connection.commit()
                return None
            self._validate_binding_artifacts(row)
            client_order_id = cast(str, row["client_order_id"])
            if (
                now >= _datetime(cast(str, row["order_expires_at"]))
                or now >= _datetime(cast(str, row["mandate_expires_at"]))
                or now >= _datetime(cast(str, row["price_valid_until"]))
            ):
                connection.execute(
                    """
                    UPDATE paper_intents
                    SET outbox_state = ?, updated_at = ? WHERE client_order_id = ?
                    """,
                    (OutboxState.EXPIRED.value, _timestamp(now), client_order_id),
                )
                connection.commit()
                return None
            submission_id = uuid.uuid4().hex
            connection.execute(
                """
                UPDATE paper_intents
                SET outbox_state = ?, lease_token = ?, lease_expires_at = ?, updated_at = ?
                WHERE client_order_id = ?
                """,
                (
                    OutboxState.SUBMITTING.value,
                    submission_id,
                    _timestamp(now + timedelta(seconds=self.lease_timeout_seconds)),
                    _timestamp(now),
                    client_order_id,
                ),
            )
            connection.execute(
                """
                INSERT INTO paper_submission_attempts (
                    submission_id, client_order_id, status, receipt_hash,
                    error_kind, started_at, finished_at
                ) VALUES (?, ?, 'submitting', NULL, NULL, ?, NULL)
                """,
                (submission_id, client_order_id, _timestamp(now)),
            )
            self._append_event(
                connection,
                client_order_id,
                "submission_claimed",
                cast(str, row["approval_hash"]),
                now,
            )
            self._set_gate(connection, True, "submission_in_flight", now)
            connection.commit()
            claimed = connection.execute(
                "SELECT * FROM paper_intents WHERE client_order_id = ?",
                (client_order_id,),
            ).fetchone()
        if claimed is None:
            raise RuntimeError("claimed paper intent disappeared")
        return claimed, submission_id

    def _recover_expired_leases(self, now: datetime) -> None:
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            rows = connection.execute(
                """
                SELECT client_order_id, lease_token FROM paper_intents
                WHERE outbox_state = ? AND lease_expires_at <= ?
                """,
                (OutboxState.SUBMITTING.value, _timestamp(now)),
            ).fetchall()
            for row in rows:
                client_order_id = cast(str, row["client_order_id"])
                submission_id = cast(str, row["lease_token"])
                connection.execute(
                    """
                    UPDATE paper_intents
                    SET outbox_state = ?, lease_token = NULL, lease_expires_at = NULL,
                        updated_at = ? WHERE client_order_id = ?
                    """,
                    (OutboxState.UNKNOWN.value, _timestamp(now), client_order_id),
                )
                connection.execute(
                    """
                    UPDATE paper_submission_attempts
                    SET status = 'unknown', error_kind = 'lease_expired', finished_at = ?
                    WHERE submission_id = ?
                    """,
                    (_timestamp(now), submission_id),
                )
            if rows:
                self._set_gate(connection, True, "ambiguous_submission", now)
            connection.commit()

    def _finish_ambiguous(
        self,
        client_order_id: str,
        submission_id: str,
        now: datetime,
        error_kind: str,
    ) -> None:
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            connection.execute(
                """
                UPDATE paper_intents
                SET outbox_state = ?, lease_token = NULL, lease_expires_at = NULL,
                    updated_at = ?
                WHERE client_order_id = ? AND lease_token = ?
                """,
                (
                    OutboxState.UNKNOWN.value,
                    _timestamp(now),
                    client_order_id,
                    submission_id,
                ),
            )
            connection.execute(
                """
                UPDATE paper_submission_attempts
                SET status = 'unknown', error_kind = ?, finished_at = ?
                WHERE submission_id = ?
                """,
                (error_kind, _timestamp(now), submission_id),
            )
            self._set_gate(connection, True, "ambiguous_submission", now)
            connection.commit()

    def _approval_artifact(
        self,
        *,
        order_hash: str,
        mandate_hash: str,
        price_basis_hash: str,
        policy_evaluation_hash: str,
        approve: bool,
        actor_kind: str,
        actor_ref: str,
        decided_at: datetime,
    ) -> str:
        artifact = self.artifacts.put_json(
            {
                "schema_version": "market-impact.approval-decision.v1",
                "decision": "approve" if approve else "reject",
                "order_intent_hash": order_hash,
                "mandate_hash": mandate_hash,
                "price_basis_hash": price_basis_hash,
                "policy_evaluation_hash": policy_evaluation_hash,
                "actor_kind": actor_kind,
                "actor_ref": actor_ref,
                "decided_at": _timestamp(decided_at),
            }
        )
        return artifact.content_hash

    @staticmethod
    def _append_event(
        connection: sqlite3.Connection,
        client_order_id: str,
        event_type: str,
        artifact_hash: str,
        observed_at: datetime,
    ) -> None:
        connection.execute(
            """
            INSERT INTO paper_execution_events (
                client_order_id, event_type, artifact_hash, observed_at
            ) VALUES (?, ?, ?, ?)
            """,
            (client_order_id, event_type, artifact_hash, _timestamp(observed_at)),
        )

    @staticmethod
    def _set_gate(
        connection: sqlite3.Connection,
        blocked: bool,
        reason: str | None,
        now: datetime,
    ) -> None:
        connection.execute(
            """
            UPDATE paper_execution_gate
            SET blocked = ?, reason = ?, updated_at = ? WHERE singleton = 1
            """,
            (int(blocked), reason, _timestamp(now)),
        )


def _record(row: sqlite3.Row) -> PaperIntentRecord:
    outbox_value = cast(str | None, row["outbox_state"])
    return PaperIntentRecord(
        client_order_id=cast(str, row["client_order_id"]),
        order_hash=cast(str, row["order_hash"]),
        agent_admission_hash=cast(str | None, row["agent_admission_hash"]),
        mandate_hash=cast(str, row["mandate_hash"]),
        price_basis_hash=cast(str, row["price_basis_hash"]),
        policy_evaluation_hash=cast(str, row["policy_evaluation_hash"]),
        approval_hash=cast(str | None, row["approval_hash"]),
        approval_state=ApprovalState(cast(str, row["approval_state"])),
        outbox_state=OutboxState(outbox_value) if outbox_value is not None else None,
        provider_order_id=cast(str | None, row["provider_order_id"]),
        provider_status=cast(str | None, row["provider_status"]),
        fill_status=cast(str | None, row["fill_status"]),
        updated_at=_datetime(cast(str, row["updated_at"])),
    )


def _order_dict(order: OrderIntent) -> dict[str, object]:
    return order.to_dict()


def _json_object(value: object, name: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise TypeError(f"{name} must be an object")
    return cast(dict[str, object], value)


def _json_string(payload: dict[str, object], field: str) -> str:
    value = payload.get(field)
    if not isinstance(value, str):
        raise ValueError(f"artifact lacks string {field}")
    return value


def _json_string_list(payload: dict[str, object], field: str) -> tuple[str, ...]:
    value = payload.get(field)
    if not isinstance(value, list):
        raise ValueError(f"artifact lacks string list {field}")
    items: list[str] = []
    for item in cast(list[object], value):
        if not isinstance(item, str):
            raise ValueError(f"artifact lacks string list {field}")
        items.append(item)
    return tuple(items)


def _order_from_dict(payload: object) -> OrderIntent:
    if not isinstance(payload, dict):
        raise TypeError("stored Order Intent must be an object")
    fields = cast(dict[str, object], payload)
    from market_impact_agent.domain import OrderKind, Side

    limit = fields.get("limit_price")
    return OrderIntent(
        client_order_id=cast(str, fields["client_order_id"]),
        signal_id=cast(str, fields["signal_id"]),
        account_id=cast(str, fields["account_id"]),
        environment=TradingEnvironment(cast(str, fields["environment"])),
        instrument_id=cast(str, fields["instrument_id"]),
        side=Side(cast(str, fields["side"])),
        quantity=Decimal(cast(str, fields["quantity"])),
        order_kind=OrderKind(cast(str, fields["order_kind"])),
        limit_price=Decimal(cast(str, limit)) if limit is not None else None,
        created_at=_datetime(cast(str, fields["created_at"])),
        expires_at=_datetime(cast(str, fields["expires_at"])),
    )


def _mandate_dict(mandate: TradingMandate) -> dict[str, object]:
    return {
        "schema_version": "market-impact.trading-mandate.v1",
        "mandate_id": mandate.mandate_id,
        "account_id": mandate.account_id,
        "environment": mandate.environment.value,
        "approval_mode": mandate.approval_mode.value,
        "valid_from": _timestamp(mandate.valid_from),
        "expires_at": _timestamp(mandate.expires_at),
        "allowed_instruments": sorted(mandate.allowed_instruments),
        "allowed_sides": sorted(side.value for side in mandate.allowed_sides),
        "max_order_notional": str(mandate.max_order_notional),
    }


def _receipt_dict(receipt: ExecutionReceipt) -> dict[str, object]:
    return {
        "schema_version": "market-impact.execution-receipt.v1",
        "client_order_id": receipt.client_order_id,
        "provider_order_id": receipt.provider_order_id,
        "status": receipt.status.value,
        "observed_at": _timestamp(receipt.observed_at),
    }


def _timestamp(value: datetime) -> str:
    require_aware(value, "timestamp")
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _datetime(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    require_aware(parsed, "timestamp")
    return parsed
