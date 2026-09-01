from __future__ import annotations

import json
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

from market_impact_agent.account_state import (
    AccountStateSnapshot,
    PositionSnapshot,
    account_state_snapshot_from_dict,
    position_snapshot_from_dict,
)
from market_impact_agent.agent_contracts import (
    EvidencePack,
    JudgmentArtifact,
    canonical_hash,
    evidence_pack_from_dict,
)
from market_impact_agent.agent_engine import CompletedAgentRunAuthority
from market_impact_agent.agent_ensemble import execution_binding_hash
from market_impact_agent.authorized_decision_view import (
    AuthorizedDecisionView,
    authorized_decision_view_from_dict,
)
from market_impact_agent.checkpoint_market_universe import ExchangeInstrumentRuleSet
from market_impact_agent.data_inputs import (
    LocalDataSnapshotStore,
    data_snapshot_from_dict,
)
from market_impact_agent.decision_admission import (
    DECISION_ADMISSION_SCHEMA_V2,
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
    ExecutionStatus,
    HardPolicyOutcome,
    OrderIntent,
    SignalIntent,
    TradingEnvironment,
    TradingMandate,
    require_aware,
)
from market_impact_agent.policy import HardPolicyEvaluator
from market_impact_agent.portfolio_decision import (
    OrderSizingDecision,
    OrderSizingPolicy,
    PortfolioDecision,
    evaluate_portfolio_decision,
    size_portfolio_decision,
)
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
from market_impact_agent.prospective_trigger_admission import (
    ProspectiveTriggerAdmission,
    TriggerAdmissionAuthority,
    prospective_trigger_admission_from_dict,
)
from market_impact_agent.providers import (
    CancelExecutionProvider,
    CancellationCapability,
    CancellationCapabilityRejected,
    CancellationCommandReceipt,
    Capability,
    ExecutionProvider,
    NewOrderAdmissionProvider,
    SubmissionCapability,
    SubmissionCapabilityRejected,
    _issue_cancellation_capability,  # pyright: ignore[reportPrivateUsage]
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


class CancellationState(StrEnum):
    PENDING_APPROVAL = "pending_approval"
    REJECTED = "rejected"
    QUEUED = "queued"
    CANCELING = "canceling"
    UNKNOWN = "unknown"
    ACKNOWLEDGED = "acknowledged"
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
    filled_quantity: Decimal
    fill_ids: tuple[str, ...]
    provider_observed_at: datetime | None
    updated_at: datetime


@dataclass(frozen=True, slots=True)
class ReconciliationRun:
    reconciliation_hash: str
    complete: bool
    gaps: tuple[str, ...]
    observed_at: datetime


@dataclass(frozen=True, slots=True)
class PaperCancellationRecord:
    cancellation_id: str
    client_order_id: str
    provider_order_id: str
    provider_id: str
    provider_version: str
    request_hash: str
    approval_hash: str | None
    state: CancellationState
    updated_at: datetime


@dataclass(frozen=True, slots=True)
class PaperReplacementRecord:
    replacement_id: str
    canceled_client_order_id: str
    cancellation_id: str
    replacement_order_hash: str
    admitted_client_order_id: str | None
    created_at: datetime


@dataclass(frozen=True, slots=True)
class _ReconciledIntentUpdate:
    client_order_id: str
    provider_order_id: str | None
    provider_status: str
    fill_status: str | None
    filled_quantity: Decimal
    fill_ids: tuple[str, ...]
    provider_observed_at: datetime


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
        account_state_snapshots: Mapping[str, AccountStateSnapshot] | None = None,
        account_state_source: Callable[[], AccountStateSnapshot] | None = None,
        account_state_max_age: timedelta = timedelta(minutes=5),
        instrument_identities: Mapping[str, tuple[str, str]] | None = None,
        instrument_rule_sets: Mapping[str, ExchangeInstrumentRuleSet] | None = None,
        order_sizing_policies: Mapping[str, OrderSizingPolicy] | None = None,
        trigger_admission_authority: TriggerAdmissionAuthority | None = None,
    ) -> None:
        if lease_timeout_seconds < 1:
            raise ValueError("lease_timeout_seconds must be positive")
        if account_state_max_age <= timedelta(0):
            raise ValueError("account_state_max_age must be positive")
        if account_state_max_age != timedelta(seconds=int(account_state_max_age.total_seconds())):
            raise ValueError("account_state_max_age must use whole seconds")
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
        accepted_account_states = dict(account_state_snapshots or {})
        if any(key != value.snapshot_id for key, value in accepted_account_states.items()):
            raise ValueError("Account State Snapshot registry key differs from content identity")
        accepted_instrument_identities = dict(instrument_identities or {})
        for instrument_id, identity in accepted_instrument_identities.items():
            if (
                not instrument_id
                or instrument_id != instrument_id.strip()
                or len(identity) != 2
                or any(not value or value != value.strip() for value in identity)
            ):
                raise ValueError("Instrument identity registry contains invalid content")
        self.__account_state_snapshots = MappingProxyType(accepted_account_states)
        self.__account_state_source = account_state_source
        self.__account_state_max_age = account_state_max_age
        self.__instrument_identities = MappingProxyType(accepted_instrument_identities)
        accepted_rule_sets = dict(instrument_rule_sets or {})
        if any(key != value.rule_set_id for key, value in accepted_rule_sets.items()):
            raise ValueError("instrument rule-set registry key differs from content identity")
        accepted_sizing_policies = dict(order_sizing_policies or {})
        if any(key != value.policy_id for key, value in accepted_sizing_policies.items()):
            raise ValueError("Order Sizing Policy registry key differs from content identity")
        self.__instrument_rule_sets = MappingProxyType(accepted_rule_sets)
        self.__order_sizing_policies = MappingProxyType(accepted_sizing_policies)
        self.__trigger_admission_authority = trigger_admission_authority
        self._initialize()
        os.chmod(self.root, 0o700)
        os.chmod(self.database_path, 0o600)
        self._block_for_unreconciled_state()
        self.provider.bind_submission_validator(self._validate_submission_capability)
        if isinstance(self.provider, CancelExecutionProvider):
            self.provider.bind_cancellation_validator(self._validate_cancellation_capability)

    @property
    def execution_blocked(self) -> bool:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT blocked FROM paper_execution_gate WHERE singleton = 1"
            ).fetchone()
            kill_row = connection.execute(
                "SELECT active FROM paper_kill_switch WHERE singleton = 1"
            ).fetchone()
        if row is None:
            raise RuntimeError("paper execution gate is missing")
        if kill_row is None:
            raise RuntimeError("paper kill switch is missing")
        return bool(row["blocked"]) or bool(kill_row["active"])

    def record_provider_acceptance(self, store: LocalDataSnapshotStore) -> str:
        """Persist this concrete execution owner's accepted Provider in one Harness root."""

        source = self.__account_state_source
        if source is None:
            raise PermissionError("Provider acceptance requires a current Account State source")
        account_state = source()
        now = self.clock().astimezone(UTC)
        readiness = account_state.readiness(
            evaluated_at=now,
            max_age=self.__account_state_max_age,
        )
        manifest_hash = canonical_hash(self.provider.manifest.to_dict())
        if (
            not readiness.exposure_increase_ready
            or account_state.provider_id != self.provider.manifest.provider_id
            or account_state.provider_version != self.provider.manifest.provider_version
            or account_state.provider_manifest_hash != manifest_hash
            or account_state.environment is not TradingEnvironment.PAPER
        ):
            raise PermissionError("Paper execution owner cannot reopen accepted Provider state")
        payload = {
            "schema_version": "market-impact.paper-provider-acceptance.v2",
            "harness_authority_id": store.harness_authority_id,
            "provider_id": self.provider.manifest.provider_id,
            "provider_version": self.provider.manifest.provider_version,
            "provider_manifest_hash": manifest_hash,
            "account_reference_hash": account_state.account_reference_hash,
            "account_state_hash": canonical_hash(account_state.to_dict()),
            "accepted_at": _timestamp(now),
        }
        acceptance_id = "paper-provider-acceptance-" + canonical_hash(payload)
        artifact = store.artifacts.put_json({"acceptance_id": acceptance_id, **payload})
        with store.authority_transaction() as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS paper_provider_acceptances (
                    acceptance_id TEXT PRIMARY KEY,
                    harness_authority_id TEXT NOT NULL,
                    provider_id TEXT NOT NULL,
                    provider_version TEXT NOT NULL,
                    provider_manifest_hash TEXT NOT NULL,
                    account_reference_hash TEXT NOT NULL,
                    account_state_hash TEXT NOT NULL,
                    artifact_hash TEXT NOT NULL
                )
                """
            )
            connection.execute(
                """
                INSERT OR IGNORE INTO paper_provider_acceptances VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    acceptance_id,
                    store.harness_authority_id,
                    self.provider.manifest.provider_id,
                    self.provider.manifest.provider_version,
                    manifest_hash,
                    account_state.account_reference_hash,
                    canonical_hash(account_state.to_dict()),
                    artifact.content_hash,
                ),
            )
        return acceptance_id

    @property
    def kill_switch_active(self) -> bool:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT active FROM paper_kill_switch WHERE singleton = 1"
            ).fetchone()
        if row is None:
            raise RuntimeError("paper kill switch is missing")
        return bool(row["active"])

    def activate_kill_switch(self, *, actor_ref: str, reason: str) -> None:
        changed_at = self.clock()
        require_aware(changed_at, "now")
        for name, value in (("actor_ref", actor_ref), ("reason", reason)):
            if not value or value != value.strip():
                raise ValueError(f"{name} must be a non-empty trimmed string")
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            connection.execute(
                """
                UPDATE paper_kill_switch
                SET active = 1, reason = ?, actor_ref = ?,
                    generation = generation + 1, updated_at = ?
                WHERE singleton = 1
                """,
                (reason, actor_ref, _timestamp(changed_at)),
            )
            connection.commit()

    def clear_kill_switch(self, *, actor_ref: str) -> None:
        changed_at = self.clock()
        require_aware(changed_at, "now")
        if not actor_ref or actor_ref != actor_ref.strip():
            raise ValueError("actor_ref must be a non-empty trimmed string")
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            gate = connection.execute(
                "SELECT blocked FROM paper_execution_gate WHERE singleton = 1"
            ).fetchone()
            if gate is None:
                raise RuntimeError("paper execution gate is missing")
            if bool(gate["blocked"]):
                raise PermissionError("kill switch cannot clear before complete reconciliation")
            kill = connection.execute(
                """
                SELECT active, generation
                FROM paper_kill_switch WHERE singleton = 1
                """
            ).fetchone()
            if kill is None:
                raise RuntimeError("paper kill switch is missing")
            if not bool(kill["active"]):
                connection.commit()
                return
            current_generation = cast(int, kill["generation"])
            post_activation = connection.execute(
                """
                SELECT 1 FROM paper_reconciliation_runs
                WHERE complete = 1 AND kill_generation = ?
                LIMIT 1
                """,
                (current_generation,),
            ).fetchone()
            if post_activation is None:
                raise PermissionError(
                    "kill switch requires a new complete reconciliation before clearing"
                )
            connection.execute(
                """
                UPDATE paper_kill_switch
                SET active = 0, reason = NULL, actor_ref = ?,
                    updated_at = ?
                WHERE singleton = 1
                """,
                (actor_ref, _timestamp(changed_at)),
            )
            connection.commit()

    def admit(self, order: OrderIntent) -> PaperIntentRecord:
        return self._admit(
            order,
            agent_admission_hash=None,
            expected_price_basis_hash=None,
        )

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
        authorized_view: AuthorizedDecisionView,
        position_snapshot: PositionSnapshot,
        portfolio_decision: PortfolioDecision,
        sizing_decision: OrderSizingDecision,
        price_basis: PriceBasis,
        trigger_admission: ProspectiveTriggerAdmission | None = None,
    ) -> PaperIntentRecord:
        if admission.disposition is not DecisionDisposition.PROPOSE:
            raise PermissionError("abstaining Decision Admission cannot reach paper execution")
        if admission.schema_version != DECISION_ADMISSION_SCHEMA_V2:
            raise PermissionError(
                "Agent-directed paper requires portfolio-bound Decision Admission v2"
            )
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
            trigger_admission=trigger_admission,
            trigger_admission_authority=self.__trigger_admission_authority,
        )
        if recomputed_query_gate.to_dict() != query_gate.to_dict():
            raise ValueError("paper admission Query Gate was not deterministically evaluated")
        account_state = self.__account_state_snapshots.get(
            position_snapshot.account_state_snapshot_id
        )
        if account_state is None:
            raise PermissionError("paper admission lacks accepted Account State authority")
        expected_position_snapshot = account_state.project_positions(
            evaluated_at=position_snapshot.evaluated_at,
            max_age=self.__account_state_max_age,
        )
        if expected_position_snapshot.to_dict() != position_snapshot.to_dict():
            raise ValueError("paper admission Position Snapshot is not a trusted projection")
        self._require_current_account_state(
            account_state=account_state,
            position_snapshot=position_snapshot,
            evaluated_at=self.clock(),
        )
        expected_authorized_view = AuthorizedDecisionView.build(
            cutoff=authorized_view.cutoff,
            frozen_at=authorized_view.frozen_at,
            data_snapshot_ids=query_gate.authorized_snapshot_ids,
            decision_input_ids=query_gate.authorized_decision_input_ids,
            position_snapshot=position_snapshot,
        )
        if expected_authorized_view.to_dict() != authorized_view.to_dict():
            raise ValueError("paper admission Authorized Decision View is not deterministic")
        identity = self.__instrument_identities.get(signal.instrument_id)
        if identity is None:
            raise PermissionError("paper admission lacks accepted Instrument Master identity")
        if identity != (portfolio_decision.venue, portfolio_decision.instrument_class):
            raise ValueError(
                "paper admission instrument venue/class differs from Instrument Master"
            )
        expected_portfolio_decision = evaluate_portfolio_decision(
            signal=signal,
            authorized_view=authorized_view,
            position_snapshot=position_snapshot,
            requested_action=portfolio_decision.requested_action,
            venue=portfolio_decision.venue,
            instrument_class=portfolio_decision.instrument_class,
            evidence_refs=portfolio_decision.evidence_refs,
            decided_at=portfolio_decision.decided_at,
        )
        if expected_portfolio_decision.to_dict() != portfolio_decision.to_dict():
            raise ValueError(
                "paper admission Portfolio Decision was not deterministically evaluated"
            )
        rule_set = self.__instrument_rule_sets.get(sizing_decision.instrument_rule_set_id)
        sizing_policy = self.__order_sizing_policies.get(sizing_decision.sizing_policy_id)
        if rule_set is None or sizing_policy is None:
            raise PermissionError("paper admission lacks accepted sizing rule or policy authority")
        expected_sizing_decision = size_portfolio_decision(
            portfolio_decision=portfolio_decision,
            position_snapshot=position_snapshot,
            mandate=self.mandate,
            price_basis=price_basis,
            rule_set=rule_set,
            sizing_policy=sizing_policy,
            order_kind=sizing_decision.order_kind,
            decided_at=sizing_decision.decided_at,
        )
        if expected_sizing_decision.to_dict() != sizing_decision.to_dict():
            raise ValueError("paper admission Order Sizing was not deterministically evaluated")
        admission.assert_matches(
            manifest=manifest,
            query_gate=query_gate,
            evidence_pack=evidence_pack,
            signal=signal,
            order=order,
            authorized_view=authorized_view,
            account_state_snapshot=account_state,
            position_snapshot=position_snapshot,
            portfolio_decision=portfolio_decision,
            sizing_decision=sizing_decision,
            mandate=self.mandate,
            price_basis=price_basis,
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
                trigger_admission=trigger_admission,
            )
        )
        evidence_pack_artifact = self.artifacts.put_json(evidence_pack.to_dict())
        execution_plan_artifact = self.artifacts.put_json(execution_plan.to_dict())
        manifest_artifact = self.artifacts.put_json(manifest.to_dict())
        signal_artifact = self.artifacts.put_json(signal.to_dict())
        authorized_view_artifact = self.artifacts.put_json(authorized_view.to_dict())
        account_state_artifact = self.artifacts.put_json(account_state.to_dict())
        position_snapshot_artifact = self.artifacts.put_json(position_snapshot.to_dict())
        portfolio_decision_artifact = self.artifacts.put_json(portfolio_decision.to_dict())
        sizing_decision_artifact = self.artifacts.put_json(sizing_decision.to_dict())
        price_basis_artifact = self.artifacts.put_json(price_basis.to_dict())
        instrument_rule_set_artifact = self.artifacts.put_json(rule_set.to_dict())
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
        if authorized_view_artifact.content_hash != admission.authorized_decision_view_hash:
            raise ValueError("Decision Admission Authorized Decision View hash is not exact")
        if account_state_artifact.content_hash != admission.account_state_snapshot_hash:
            raise ValueError("Decision Admission Account State Snapshot hash is not exact")
        if position_snapshot_artifact.content_hash != admission.position_snapshot_hash:
            raise ValueError("Decision Admission Position Snapshot hash is not exact")
        if portfolio_decision_artifact.content_hash != admission.portfolio_decision_hash:
            raise ValueError("Decision Admission Portfolio Decision hash is not exact")
        if sizing_decision_artifact.content_hash != admission.order_sizing_decision_hash:
            raise ValueError("Decision Admission Order Sizing Decision hash is not exact")
        if price_basis_artifact.content_hash != sizing_decision.price_basis_hash:
            raise ValueError("Order Sizing Decision Price Basis hash is not exact")
        if instrument_rule_set_artifact.content_hash != sizing_decision.instrument_rule_set_hash:
            raise ValueError("Order Sizing Decision instrument rule-set hash is not exact")
        admission_artifact = self.artifacts.put_json(admission.to_dict())
        return self._admit(
            order,
            agent_admission_hash=admission_artifact.content_hash,
            expected_price_basis_hash=sizing_decision.price_basis_hash,
        )

    def _admit(
        self,
        order: OrderIntent,
        *,
        agent_admission_hash: str | None,
        expected_price_basis_hash: str | None,
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
        if not _provider_accepts_new_orders(self.provider):
            raise PermissionError("execution Provider admission is closed for new orders")

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
        if (
            expected_price_basis_hash is not None
            and price_artifact.content_hash != expected_price_basis_hash
        ):
            raise ValueError("paper admission price differs from deterministic sizing")
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
                    provider_id, provider_version,
                    order_expires_at, mandate_expires_at, price_valid_until, lease_token,
                    lease_expires_at, created_at, updated_at
                ) VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, NULL, ?, ?,
                    ?, ?, ?, NULL, NULL, ?, ?
                )
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
                    self.provider.manifest.provider_id,
                    self.provider.manifest.provider_version,
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
            row = connection.execute(
                "SELECT * FROM paper_intents WHERE client_order_id = ?",
                (client_order_id,),
            ).fetchone()
        if row is None:
            raise KeyError(client_order_id)
        self._validate_binding_artifacts(row)
        authority_current = not approve or (
            self._provider_binding_matches(row)
            and self._agent_account_state_is_current(
                row,
                evaluated_at=decided_at,
            )
        )
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
                or not authority_current
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

    def request_cancel(
        self,
        client_order_id: str,
        *,
        cancellation_id: str,
        reason: str,
    ) -> PaperCancellationRecord:
        requested_at = self.clock()
        require_aware(requested_at, "now")
        for name, value in (
            ("client_order_id", client_order_id),
            ("cancellation_id", cancellation_id),
            ("reason", reason),
        ):
            if not value or value != value.strip():
                raise ValueError(f"{name} must be a non-empty trimmed string")
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            intent = connection.execute(
                "SELECT * FROM paper_intents WHERE client_order_id = ?",
                (client_order_id,),
            ).fetchone()
            if intent is None:
                raise KeyError(client_order_id)
            provider_order_id = cast(str | None, intent["provider_order_id"])
            if provider_order_id is None:
                raise PermissionError("cancel requires one reconciled open provider order")
            request_artifact = self.artifacts.put_json(
                self._cancellation_request_payload(
                    client_order_id=client_order_id,
                    provider_order_id=provider_order_id,
                    cancellation_id=cancellation_id,
                    reason=reason,
                    requested_at=requested_at,
                )
            )
            row = self._request_cancel_locked(
                connection,
                client_order_id=client_order_id,
                cancellation_id=cancellation_id,
                reason=reason,
                requested_at=requested_at,
                request_hash=request_artifact.content_hash,
            )
            connection.commit()
        return _cancellation_record(row)

    def _cancellation_request_payload(
        self,
        *,
        client_order_id: str,
        provider_order_id: str,
        cancellation_id: str,
        reason: str,
        requested_at: datetime,
    ) -> dict[str, object]:
        return {
            "schema_version": "market-impact.cancellation-request.v1",
            "cancellation_id": cancellation_id,
            "client_order_id": client_order_id,
            "provider_order_id": provider_order_id,
            "provider_id": self.provider.manifest.provider_id,
            "provider_version": self.provider.manifest.provider_version,
            "reason": reason,
            "requested_at": _timestamp(requested_at),
        }

    def _request_cancel_locked(
        self,
        connection: sqlite3.Connection,
        *,
        client_order_id: str,
        cancellation_id: str,
        reason: str,
        requested_at: datetime,
        request_hash: str,
    ) -> sqlite3.Row:
        intent = connection.execute(
            "SELECT * FROM paper_intents WHERE client_order_id = ?",
            (client_order_id,),
        ).fetchone()
        if intent is None:
            raise KeyError(client_order_id)
        self._validate_binding_artifacts(intent)
        provider_order_id = cast(str | None, intent["provider_order_id"])
        existing = connection.execute(
            "SELECT * FROM paper_cancellations WHERE cancellation_id = ?",
            (cancellation_id,),
        ).fetchone()
        if existing is not None:
            existing_request = _json_object(
                self.artifacts.read_json(cast(str, existing["request_hash"])),
                "Cancellation Request artifact",
            )
            if (
                cast(str, existing["client_order_id"]) != client_order_id
                or provider_order_id is None
                or cast(str, existing["provider_order_id"]) != provider_order_id
                or cast(str | None, existing["provider_id"])
                != cast(str | None, intent["provider_id"])
                or cast(str | None, existing["provider_version"])
                != cast(str | None, intent["provider_version"])
                or _json_string(existing_request, "reason") != reason
            ):
                raise ValueError("cancellation_id already binds different content")
            return existing
        if not isinstance(self.provider, CancelExecutionProvider):
            raise PermissionError("execution Provider has no accepted cancel operation")
        if not self._provider_binding_matches(intent):
            raise PermissionError("open order is bound to another execution Provider")
        if (
            OutboxState(cast(str, intent["outbox_state"])) is not OutboxState.RECONCILED
            or provider_order_id is None
            or cast(str | None, intent["provider_status"])
            not in {
                ExecutionStatus.ACCEPTED.value,
                ExecutionStatus.PARTIALLY_FILLED.value,
            }
        ):
            raise PermissionError("cancel requires one reconciled open provider order")
        duplicate = connection.execute(
            """
            SELECT cancellation_id FROM paper_cancellations
            WHERE client_order_id = ? AND state IN (?, ?, ?, ?, ?)
            LIMIT 1
            """,
            (
                client_order_id,
                CancellationState.PENDING_APPROVAL.value,
                CancellationState.QUEUED.value,
                CancellationState.CANCELING.value,
                CancellationState.UNKNOWN.value,
                CancellationState.ACKNOWLEDGED.value,
            ),
        ).fetchone()
        if duplicate is not None:
            raise PermissionError("order already has a non-terminal cancellation")
        gate = connection.execute(
            "SELECT blocked FROM paper_execution_gate WHERE singleton = 1"
        ).fetchone()
        if gate is None:
            raise RuntimeError("paper execution gate is missing")
        if bool(gate["blocked"]):
            raise PermissionError("cancel request requires a reconciled execution state")
        connection.execute(
            """
            INSERT INTO paper_cancellations (
                cancellation_id, client_order_id, provider_order_id,
                provider_id, provider_version, request_hash,
                approval_hash, state, lease_token, lease_expires_at, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, NULL, ?, NULL, NULL, ?, ?)
            """,
            (
                cancellation_id,
                client_order_id,
                provider_order_id,
                self.provider.manifest.provider_id,
                self.provider.manifest.provider_version,
                request_hash,
                CancellationState.PENDING_APPROVAL.value,
                _timestamp(requested_at),
                _timestamp(requested_at),
            ),
        )
        row = connection.execute(
            "SELECT * FROM paper_cancellations WHERE cancellation_id = ?",
            (cancellation_id,),
        ).fetchone()
        if row is None:
            raise RuntimeError("paper cancellation disappeared after insertion")
        return row

    def decide_cancellation(
        self,
        cancellation_id: str,
        *,
        approve: bool,
        actor_ref: str,
    ) -> PaperCancellationRecord:
        decided_at = self.clock()
        require_aware(decided_at, "now")
        if not actor_ref or actor_ref != actor_ref.strip():
            raise ValueError("actor_ref must be a non-empty trimmed string")
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT * FROM paper_cancellations WHERE cancellation_id = ?",
                (cancellation_id,),
            ).fetchone()
            if row is None:
                raise KeyError(cancellation_id)
            if CancellationState(cast(str, row["state"])) is not CancellationState.PENDING_APPROVAL:
                raise ValueError("cancellation is not pending manual approval")
            if approve and not self._provider_binding_matches(row):
                connection.execute(
                    """
                    UPDATE paper_cancellations
                    SET state = ?, updated_at = ?
                    WHERE cancellation_id = ?
                    """,
                    (
                        CancellationState.EXPIRED.value,
                        _timestamp(decided_at),
                        cancellation_id,
                    ),
                )
                connection.commit()
                return self.get_cancellation(cancellation_id)
            approval_artifact = self.artifacts.put_json(
                {
                    "schema_version": "market-impact.cancellation-approval.v1",
                    "cancellation_request_hash": cast(str, row["request_hash"]),
                    "decision": "approve" if approve else "reject",
                    "actor_kind": "human",
                    "actor_ref": actor_ref,
                    "decided_at": _timestamp(decided_at),
                }
            )
            state = CancellationState.QUEUED if approve else CancellationState.REJECTED
            connection.execute(
                """
                UPDATE paper_cancellations
                SET approval_hash = ?, state = ?, updated_at = ?
                WHERE cancellation_id = ?
                """,
                (
                    approval_artifact.content_hash,
                    state.value,
                    _timestamp(decided_at),
                    cancellation_id,
                ),
            )
            connection.commit()
        return self.get_cancellation(cancellation_id)

    def dispatch_next_cancellation(self) -> PaperCancellationRecord | None:
        current = self.clock()
        require_aware(current, "now")
        self._recover_expired_cancellation_leases(current)
        claimed = self._claim_next_cancellation(current)
        if claimed is None:
            return None
        row, attempt_id = claimed
        capability = _issue_cancellation_capability(
            client_order_id=cast(str, row["client_order_id"]),
            provider_order_id=cast(str, row["provider_order_id"]),
            cancellation_id=cast(str, row["cancellation_id"]),
            attempt_id=attempt_id,
            provider_id=cast(str, row["provider_id"]),
            provider_version=cast(str, row["provider_version"]),
            request_hash=cast(str, row["request_hash"]),
            approval_hash=cast(str, row["approval_hash"]),
        )
        provider = self.provider
        if not isinstance(provider, CancelExecutionProvider):
            self._expire_cancellation_before_provider(
                row,
                attempt_id=attempt_id,
                expired_at=current,
                error_kind="provider_cancel_capability_unavailable",
            )
            return self.get_cancellation(capability.cancellation_id)
        try:
            receipt = provider.cancel(capability)
            _validate_cancellation_receipt(receipt, capability)
        except CancellationCapabilityRejected:
            rejected_at = self.clock()
            require_aware(rejected_at, "now")
            self._expire_cancellation_before_provider(
                row,
                attempt_id=attempt_id,
                expired_at=rejected_at,
                error_kind="provider_rejected_cancellation_capability",
            )
            return self.get_cancellation(capability.cancellation_id)
        except Exception as error:
            self._finish_ambiguous_cancellation(
                capability.cancellation_id,
                attempt_id=attempt_id,
                observed_at=current,
                error_kind=type(error).__name__,
            )
            return self.get_cancellation(capability.cancellation_id)
        receipt_artifact = self.artifacts.put_json(_cancellation_receipt_dict(receipt))
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            updated = connection.execute(
                """
                UPDATE paper_cancellations
                SET state = ?, lease_token = NULL, lease_expires_at = NULL, updated_at = ?
                WHERE cancellation_id = ? AND state = ? AND lease_token = ?
                """,
                (
                    CancellationState.ACKNOWLEDGED.value,
                    _timestamp(current),
                    capability.cancellation_id,
                    CancellationState.CANCELING.value,
                    attempt_id,
                ),
            )
            if updated.rowcount != 1:
                raise RuntimeError("paper cancellation lease was lost")
            connection.execute(
                """
                UPDATE paper_cancellation_attempts
                SET status = 'acknowledged', receipt_hash = ?, finished_at = ?
                WHERE attempt_id = ?
                """,
                (receipt_artifact.content_hash, _timestamp(current), attempt_id),
            )
            self._set_gate(connection, True, "reconciliation_required", current)
            connection.commit()
        return self.get_cancellation(capability.cancellation_id)

    def get_cancellation(self, cancellation_id: str) -> PaperCancellationRecord:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM paper_cancellations WHERE cancellation_id = ?",
                (cancellation_id,),
            ).fetchone()
        if row is None:
            raise KeyError(cancellation_id)
        self.artifacts.get(cast(str, row["request_hash"]), media_type="application/json")
        approval_hash = cast(str | None, row["approval_hash"])
        if approval_hash is not None:
            self.artifacts.get(approval_hash, media_type="application/json")
        return _cancellation_record(row)

    def request_replace(
        self,
        client_order_id: str,
        replacement_order: OrderIntent,
        *,
        replacement_id: str,
        cancellation_id: str,
        reason: str,
    ) -> PaperReplacementRecord:
        requested_at = self.clock()
        require_aware(requested_at, "now")
        for name, value in (
            ("replacement_id", replacement_id),
            ("cancellation_id", cancellation_id),
            ("reason", reason),
        ):
            if not value or value != value.strip():
                raise ValueError(f"{name} must be a non-empty trimmed string")
        replacement_artifact = self.artifacts.put_json(_order_dict(replacement_order))
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            original = connection.execute(
                "SELECT * FROM paper_intents WHERE client_order_id = ?",
                (client_order_id,),
            ).fetchone()
            if original is None:
                raise KeyError(client_order_id)
            original_order = _order_from_dict(
                self.artifacts.read_json(cast(str, original["order_hash"]))
            )
            if replacement_order.client_order_id == client_order_id:
                raise ValueError("replacement requires a new client_order_id")
            if (
                replacement_order.account_id != original_order.account_id
                or replacement_order.environment is not original_order.environment
                or replacement_order.instrument_id != original_order.instrument_id
                or replacement_order.side is not original_order.side
                or replacement_order.signal_id != original_order.signal_id
            ):
                raise ValueError(
                    "replacement may change only identity, time, quantity, kind, and price"
                )
            existing = connection.execute(
                "SELECT * FROM paper_replacements WHERE replacement_id = ?",
                (replacement_id,),
            ).fetchone()
            if existing is not None:
                if (
                    cast(str, existing["canceled_client_order_id"]) != client_order_id
                    or cast(str, existing["cancellation_id"]) != cancellation_id
                    or cast(str, existing["replacement_order_hash"])
                    != replacement_artifact.content_hash
                ):
                    raise ValueError("replacement_id already binds different content")
                connection.commit()
                return _replacement_record(existing)
            provider_order_id = cast(str | None, original["provider_order_id"])
            if provider_order_id is None:
                raise PermissionError("replacement requires one reconciled open provider order")
            request_artifact = self.artifacts.put_json(
                self._cancellation_request_payload(
                    client_order_id=client_order_id,
                    provider_order_id=provider_order_id,
                    cancellation_id=cancellation_id,
                    reason=reason,
                    requested_at=requested_at,
                )
            )
            self._request_cancel_locked(
                connection,
                client_order_id=client_order_id,
                cancellation_id=cancellation_id,
                reason=reason,
                requested_at=requested_at,
                request_hash=request_artifact.content_hash,
            )
            linked = connection.execute(
                "SELECT replacement_id FROM paper_replacements WHERE cancellation_id = ?",
                (cancellation_id,),
            ).fetchone()
            if linked is not None:
                raise ValueError("cancellation already binds another replacement")
            connection.execute(
                """
                INSERT INTO paper_replacements (
                    replacement_id, canceled_client_order_id, cancellation_id,
                    replacement_order_hash, admitted_client_order_id, created_at
                ) VALUES (?, ?, ?, ?, NULL, ?)
                """,
                (
                    replacement_id,
                    client_order_id,
                    cancellation_id,
                    replacement_artifact.content_hash,
                    _timestamp(requested_at),
                ),
            )
            connection.commit()
        return self.get_replacement(replacement_id)

    def replacement_order(self, replacement_id: str) -> OrderIntent:
        record = self.get_replacement(replacement_id)
        return _order_from_dict(self.artifacts.read_json(record.replacement_order_hash))

    def admit_replacement(self, replacement_id: str) -> PaperIntentRecord:
        replacement = self.get_replacement(replacement_id)
        cancellation = self.get_cancellation(replacement.cancellation_id)
        if cancellation.state is not CancellationState.RECONCILED:
            raise PermissionError("replacement cannot admit before cancellation reconciliation")
        original = self.get(replacement.canceled_client_order_id)
        if original.provider_status != ExecutionStatus.CANCELED.value:
            raise PermissionError("replacement target is not reconciled canceled")
        if original.agent_admission_hash is not None:
            raise PermissionError(
                "Agent-directed replacement requires a fresh Decision Admission for the new intent"
            )
        order = self.replacement_order(replacement_id)
        admitted = self.admit(order)
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT admitted_client_order_id FROM paper_replacements WHERE replacement_id = ?",
                (replacement_id,),
            ).fetchone()
            if row is None:
                raise KeyError(replacement_id)
            existing = cast(str | None, row["admitted_client_order_id"])
            if existing is not None and existing != admitted.client_order_id:
                raise ValueError("replacement already admitted a different Order Intent")
            connection.execute(
                """
                UPDATE paper_replacements
                SET admitted_client_order_id = ? WHERE replacement_id = ?
                """,
                (admitted.client_order_id, replacement_id),
            )
            connection.commit()
        return admitted

    def get_replacement(self, replacement_id: str) -> PaperReplacementRecord:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM paper_replacements WHERE replacement_id = ?",
                (replacement_id,),
            ).fetchone()
        if row is None:
            raise KeyError(replacement_id)
        self.artifacts.get(
            cast(str, row["replacement_order_hash"]),
            media_type="application/json",
        )
        return _replacement_record(row)

    def dispatch_next(self) -> PaperIntentRecord | None:
        current = self.clock()
        require_aware(current, "now")
        self._recover_expired_leases(current)
        claimed = self._claim_next(current)
        if claimed is None:
            return None
        row, submission_id = claimed
        submit_checked_at = self.clock()
        require_aware(submit_checked_at, "now")
        if not _provider_accepts_new_orders(self.provider):
            self._expire_claim_before_submit(
                row,
                submission_id=submission_id,
                expired_at=submit_checked_at,
                error_kind="provider_new_order_admission_closed",
            )
            return self.get(cast(str, row["client_order_id"]))
        try:
            authorities_current = self._submission_authorities_are_current(
                row,
                evaluated_at=submit_checked_at,
            )
        except Exception as error:
            self._expire_claim_before_submit(
                row,
                submission_id=submission_id,
                expired_at=submit_checked_at,
                error_kind=f"submission_authority_check_failed:{type(error).__name__}",
            )
            return self.get(cast(str, row["client_order_id"]))
        if not authorities_current:
            self._expire_claim_before_submit(
                row,
                submission_id=submission_id,
                expired_at=submit_checked_at,
                error_kind="submission_authority_expired_or_changed",
            )
            return self.get(cast(str, row["client_order_id"]))
        capability = _issue_submission_capability(
            order=_order_from_dict(self.artifacts.read_json(cast(str, row["order_hash"]))),
            submission_id=submission_id,
            provider_id=cast(str, row["provider_id"]),
            provider_version=cast(str, row["provider_version"]),
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
            if (
                receipt.status is not ExecutionStatus.ACCEPTED
                or receipt.provider_order_id is None
                or receipt.filled_quantity != 0
                or receipt.fill_ids
            ):
                raise ValueError(
                    "provider submission receipt must be accepted without fill evidence"
                )
        except SubmissionCapabilityRejected:
            rejected_at = self.clock()
            require_aware(rejected_at, "now")
            self._expire_claim_before_submit(
                row,
                submission_id=submission_id,
                expired_at=rejected_at,
                error_kind="provider_rejected_submission_capability",
            )
            return self.get(cast(str, row["client_order_id"]))
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
                    provider_observed_at = ?, lease_token = NULL,
                    lease_expires_at = NULL, updated_at = ?
                WHERE client_order_id = ? AND lease_token = ?
                """,
                (
                    OutboxState.ACCEPTED.value,
                    receipt.provider_order_id,
                    receipt.status.value,
                    _timestamp(receipt.observed_at),
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
        self._recover_expired_cancellation_leases(current)
        with self._connect() as connection:
            kill = connection.execute(
                "SELECT generation FROM paper_kill_switch WHERE singleton = 1"
            ).fetchone()
        if kill is None:
            raise RuntimeError("paper kill switch is missing")
        kill_generation = cast(int, kill["generation"])
        snapshot = self.provider.reconcile()
        receipts: dict[str, ExecutionReceipt] = {}
        fill_owners: dict[str, str] = {}
        gaps = list(snapshot.gaps)
        for receipt in snapshot.receipts:
            if receipt.client_order_id in receipts:
                gaps.append(f"duplicate_provider_order:{receipt.client_order_id}")
            receipts[receipt.client_order_id] = receipt
            for fill_id in receipt.fill_ids:
                previous_owner = fill_owners.get(fill_id)
                if previous_owner is not None:
                    gaps.append(f"duplicate_provider_fill:{fill_id}")
                else:
                    fill_owners[fill_id] = receipt.client_order_id
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
            cancellation_rows = connection.execute(
                """
                SELECT c.cancellation_id, c.client_order_id, c.provider_order_id,
                       c.provider_id, c.provider_version, c.state,
                       MAX(a.started_at) AS started_at
                FROM paper_cancellations AS c
                LEFT JOIN paper_cancellation_attempts AS a
                    ON a.cancellation_id = c.cancellation_id
                WHERE c.state IN (?, ?, ?)
                GROUP BY c.cancellation_id, c.client_order_id, c.provider_order_id,
                         c.provider_id, c.provider_version, c.state
                """,
                (
                    CancellationState.CANCELING.value,
                    CancellationState.UNKNOWN.value,
                    CancellationState.ACKNOWLEDGED.value,
                ),
            ).fetchall()
            cancellation_target_ids = {
                cast(str, row["client_order_id"]) for row in cancellation_rows
            }
            rows = connection.execute(
                """
                SELECT i.client_order_id, i.order_hash, i.outbox_state,
                       i.provider_order_id, i.provider_status,
                       i.filled_quantity, i.fill_ids_json, i.provider_observed_at,
                       i.provider_id, i.provider_version,
                       MAX(a.started_at) AS started_at
                FROM paper_intents AS i
                LEFT JOIN paper_submission_attempts AS a
                    ON a.client_order_id = i.client_order_id
                WHERE i.outbox_state IN (?, ?, ?, ?)
                GROUP BY i.client_order_id, i.order_hash, i.outbox_state,
                         i.provider_order_id, i.provider_status,
                         i.filled_quantity, i.fill_ids_json, i.provider_observed_at,
                         i.provider_id, i.provider_version
                """,
                (
                    OutboxState.SUBMITTING.value,
                    OutboxState.UNKNOWN.value,
                    OutboxState.ACCEPTED.value,
                    OutboxState.RECONCILED.value,
                ),
            ).fetchall()
            pending_intent_updates: list[_ReconciledIntentUpdate] = []
            for row in rows:
                client_order_id = cast(str, row["client_order_id"])
                if not provider_matches:
                    continue
                if not self._provider_binding_matches(row):
                    gaps.append(f"intent_provider_binding_mismatch:{client_order_id}")
                    continue
                started_at_value = cast(str | None, row["started_at"])
                if started_at_value is None:
                    gaps.append(f"submission_attempt_missing:{client_order_id}")
                    continue
                if snapshot.observed_at < _datetime(started_at_value):
                    gaps.append(f"stale_provider_snapshot:{client_order_id}")
                    continue
                receipt = receipts.get(client_order_id)
                outbox_state = OutboxState(cast(str, row["outbox_state"]))
                expected_provider_order_id = cast(str | None, row["provider_order_id"])
                if (
                    receipt is not None
                    and expected_provider_order_id is not None
                    and receipt.provider_order_id != expected_provider_order_id
                ):
                    gaps.append(f"provider_order_identity_mismatch:{client_order_id}")
                    continue
                fill_status: str | None = None
                receipt_valid = True
                if receipt is not None:
                    order = _order_from_dict(self.artifacts.read_json(cast(str, row["order_hash"])))
                    if receipt.filled_quantity > order.quantity:
                        gaps.append(f"provider_order_overfilled:{client_order_id}")
                        receipt_valid = False
                    elif (
                        receipt.status is ExecutionStatus.FILLED
                        and receipt.filled_quantity != order.quantity
                    ):
                        gaps.append(f"provider_filled_quantity_mismatch:{client_order_id}")
                        receipt_valid = False
                    elif (
                        receipt.status is ExecutionStatus.PARTIALLY_FILLED
                        and receipt.filled_quantity >= order.quantity
                    ):
                        gaps.append(f"provider_partial_fill_not_partial:{client_order_id}")
                        receipt_valid = False
                    stored_quantity = Decimal(cast(str, row["filled_quantity"]))
                    stored_fill_ids = tuple(
                        cast(list[str], json.loads(cast(str, row["fill_ids_json"])))
                    )
                    if receipt.filled_quantity < stored_quantity:
                        gaps.append(f"provider_fill_quantity_regressed:{client_order_id}")
                        receipt_valid = False
                    if not set(stored_fill_ids).issubset(receipt.fill_ids):
                        gaps.append(f"provider_fill_identity_regressed:{client_order_id}")
                        receipt_valid = False
                    previous_observed_at = cast(str | None, row["provider_observed_at"])
                    if previous_observed_at is not None and receipt.observed_at < _datetime(
                        previous_observed_at
                    ):
                        gaps.append(f"provider_order_observation_regressed:{client_order_id}")
                        receipt_valid = False
                    if not _execution_transition_allowed(
                        cast(str | None, row["provider_status"]),
                        receipt.status,
                    ):
                        gaps.append(f"provider_order_status_regressed:{client_order_id}")
                        receipt_valid = False
                    if receipt.status is ExecutionStatus.FILLED:
                        fill_status = ExecutionStatus.FILLED.value
                    elif receipt.filled_quantity > 0:
                        fill_status = ExecutionStatus.PARTIALLY_FILLED.value
                    if not receipt_valid:
                        continue
                if outbox_state is OutboxState.RECONCILED:
                    if receipt is None:
                        if snapshot.complete:
                            gaps.append(f"reconciled_open_order_missing:{client_order_id}")
                        continue
                    if receipt.status is ExecutionStatus.UNKNOWN:
                        gaps.append(f"provider_order_unknown:{client_order_id}")
                    elif (
                        receipt.status is ExecutionStatus.PENDING_CANCEL
                        and client_order_id not in cancellation_target_ids
                    ) or (
                        receipt.status is ExecutionStatus.CANCELED
                        and client_order_id not in cancellation_target_ids
                        and cast(str | None, row["provider_status"])
                        != ExecutionStatus.CANCELED.value
                    ):
                        gaps.append(f"unexpected_provider_status:{client_order_id}")
                    pending_intent_updates.append(
                        _ReconciledIntentUpdate(
                            client_order_id=client_order_id,
                            provider_order_id=receipt.provider_order_id,
                            provider_status=receipt.status.value,
                            fill_status=fill_status,
                            filled_quantity=receipt.filled_quantity,
                            fill_ids=receipt.fill_ids,
                            provider_observed_at=receipt.observed_at,
                        )
                    )
                    continue
                if receipt is not None:
                    if receipt.status is ExecutionStatus.UNKNOWN:
                        gaps.append(f"provider_order_unknown:{client_order_id}")
                        continue
                    provider_order_id = receipt.provider_order_id
                    provider_status = receipt.status.value
                    filled_quantity = receipt.filled_quantity
                    fill_ids = receipt.fill_ids
                    provider_observed_at = receipt.observed_at
                elif snapshot.complete:
                    if outbox_state is OutboxState.ACCEPTED:
                        gaps.append(f"acknowledged_order_missing:{client_order_id}")
                        continue
                    provider_order_id = None
                    provider_status = "not_found"
                    filled_quantity = Decimal(cast(str, row["filled_quantity"]))
                    fill_ids = tuple(cast(list[str], json.loads(cast(str, row["fill_ids_json"]))))
                    provider_observed_at = snapshot.observed_at
                else:
                    continue
                pending_intent_updates.append(
                    _ReconciledIntentUpdate(
                        client_order_id=client_order_id,
                        provider_order_id=provider_order_id,
                        provider_status=provider_status,
                        fill_status=fill_status,
                        filled_quantity=filled_quantity,
                        fill_ids=fill_ids,
                        provider_observed_at=provider_observed_at,
                    )
                )
            confirmed_cancellations: list[tuple[str, str]] = []
            for cancellation in cancellation_rows:
                cancellation_id = cast(str, cancellation["cancellation_id"])
                client_order_id = cast(str, cancellation["client_order_id"])
                if not provider_matches:
                    continue
                if not self._provider_binding_matches(cancellation):
                    gaps.append(f"cancellation_provider_binding_mismatch:{cancellation_id}")
                    continue
                started_at_value = cast(str | None, cancellation["started_at"])
                if started_at_value is None:
                    gaps.append(f"cancellation_attempt_missing:{cancellation_id}")
                    continue
                if snapshot.observed_at < _datetime(started_at_value):
                    gaps.append(f"stale_cancellation_snapshot:{cancellation_id}")
                    continue
                receipt = receipts.get(client_order_id)
                if receipt is None:
                    if snapshot.complete:
                        gaps.append(f"cancellation_target_missing:{cancellation_id}")
                    continue
                if receipt.provider_order_id != cast(str, cancellation["provider_order_id"]):
                    gaps.append(f"cancellation_provider_order_mismatch:{cancellation_id}")
                    continue
                if receipt.status is not ExecutionStatus.CANCELED:
                    gaps.append(f"cancellation_not_confirmed:{cancellation_id}")
                    continue
                confirmed_cancellations.append((cancellation_id, client_order_id))
            unique_gaps = tuple(sorted(set(gaps)))
            complete = snapshot.complete and not unique_gaps
            if complete:
                for update in pending_intent_updates:
                    connection.execute(
                        """
                        UPDATE paper_intents
                        SET outbox_state = ?, provider_order_id = ?, provider_status = ?,
                            fill_status = ?, filled_quantity = ?, fill_ids_json = ?,
                            provider_observed_at = ?, lease_token = NULL,
                            lease_expires_at = NULL, updated_at = ?
                        WHERE client_order_id = ?
                        """,
                        (
                            OutboxState.RECONCILED.value,
                            update.provider_order_id,
                            update.provider_status,
                            update.fill_status,
                            str(update.filled_quantity),
                            json.dumps(list(update.fill_ids), separators=(",", ":")),
                            _timestamp(update.provider_observed_at),
                            _timestamp(current),
                            update.client_order_id,
                        ),
                    )
                for cancellation_id, client_order_id in confirmed_cancellations:
                    connection.execute(
                        """
                        UPDATE paper_cancellations
                        SET state = ?, lease_token = NULL, lease_expires_at = NULL, updated_at = ?
                        WHERE cancellation_id = ?
                        """,
                        (
                            CancellationState.RECONCILED.value,
                            _timestamp(current),
                            cancellation_id,
                        ),
                    )
                    connection.execute(
                        """
                        UPDATE paper_intents
                        SET provider_status = ?, updated_at = ?
                        WHERE client_order_id = ?
                        """,
                        (ExecutionStatus.CANCELED.value, _timestamp(current), client_order_id),
                    )
            self._set_gate(
                connection,
                not complete,
                None if complete else "reconciliation_incomplete",
                current,
            )
            run_payload = {
                "schema_version": "market-impact.execution-reconciliation.v2",
                "provider_id": self.provider.manifest.provider_id,
                "provider_snapshot_hash": snapshot_artifact.content_hash,
                "kill_generation": kill_generation,
                "complete": complete,
                "gaps": list(unique_gaps),
                "observed_at": _timestamp(current),
            }
            run_artifact = self.artifacts.put_json(run_payload)
            connection.execute(
                """
                INSERT OR IGNORE INTO paper_reconciliation_runs (
                    reconciliation_hash, complete, gaps_json, kill_generation, observed_at
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (
                    run_artifact.content_hash,
                    int(complete),
                    "\n".join(unique_gaps),
                    kill_generation,
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
        if not self._provider_binding_matches(row):
            raise PermissionError("client_order_id is bound to another execution Provider")
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
        if (
            not self._provider_binding_matches(row)
            or capability.provider_id != self.provider.manifest.provider_id
            or capability.provider_version != self.provider.manifest.provider_version
        ):
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
            if not self._submission_authorities_are_current(row, evaluated_at=now):
                return False
        except (KeyError, OSError, TypeError, ValueError):
            return False
        return True

    def _provider_binding_matches(self, row: sqlite3.Row) -> bool:
        return (
            cast(str | None, row["provider_id"]) == self.provider.manifest.provider_id
            and cast(str | None, row["provider_version"]) == self.provider.manifest.provider_version
        )

    def _validate_cancellation_capability(
        self,
        capability: CancellationCapability,
    ) -> bool:
        now = self.clock()
        require_aware(now, "now")
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM paper_cancellations WHERE cancellation_id = ?",
                (capability.cancellation_id,),
            ).fetchone()
            intent = connection.execute(
                "SELECT * FROM paper_intents WHERE client_order_id = ?",
                (capability.client_order_id,),
            ).fetchone()
        if row is None or intent is None:
            return False
        if (
            not self._provider_binding_matches(row)
            or not self._provider_binding_matches(intent)
            or capability.provider_id != self.provider.manifest.provider_id
            or capability.provider_version != self.provider.manifest.provider_version
        ):
            return False
        lease_expires_at = cast(str | None, row["lease_expires_at"])
        approval_hash = cast(str | None, row["approval_hash"])
        if (
            CancellationState(cast(str, row["state"])) is not CancellationState.CANCELING
            or cast(str | None, row["lease_token"]) != capability.attempt_id
        ):
            return False
        lease_token = cast(str | None, row["lease_token"])
        if (
            lease_token is None
            or lease_expires_at is None
            or now >= _datetime(lease_expires_at)
            or approval_hash is None
            or cast(str, row["client_order_id"]) != capability.client_order_id
            or cast(str, row["provider_order_id"]) != capability.provider_order_id
            or cast(str, row["request_hash"]) != capability.request_hash
            or approval_hash != capability.approval_hash
            or cast(str | None, intent["provider_order_id"]) != capability.provider_order_id
            or cast(str | None, intent["provider_status"])
            not in {
                ExecutionStatus.ACCEPTED.value,
                ExecutionStatus.PARTIALLY_FILLED.value,
            }
            or OutboxState(cast(str, intent["outbox_state"])) is not OutboxState.RECONCILED
        ):
            return False
        try:
            self.artifacts.get(capability.request_hash, media_type="application/json")
            self.artifacts.get(capability.approval_hash, media_type="application/json")
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
            admission_schema = _json_string(admission_payload, "schema_version")
            if admission_schema not in {
                "market-impact.decision-admission.v1",
                "market-impact.decision-admission.v2",
            }:
                raise ValueError("paper intent does not bind the canonical Decision Admission")
            _validate_content_id(
                admission_payload,
                id_field="admission_id",
                prefix="decision-admission-",
            )
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
            evaluation_material_schema = _json_string(
                evaluation_material_payload,
                "schema_version",
            )
            if evaluation_material_schema not in {
                "market-impact.prospective-query-gate-evaluation-material.v1",
                "market-impact.prospective-query-gate-evaluation-material.v2",
            }:
                raise ValueError("Query Gate evaluation material schema is not canonical")
            registration_payload = _json_object(
                evaluation_material_payload.get("registration"),
                "prospective diagnostic registration",
            )
            snapshot_set_payload = _json_object(
                evaluation_material_payload.get("checkpoint_snapshot_set"),
                "prospective checkpoint Snapshot Set",
            )
            trigger_admission = None
            if evaluation_material_schema == (
                "market-impact.prospective-query-gate-evaluation-material.v2"
            ):
                trigger_admission = prospective_trigger_admission_from_dict(
                    evaluation_material_payload.get("trigger_admission")
                )
                if self.__trigger_admission_authority is None:
                    raise PermissionError(
                        "paper restart lacks prospective Trigger Admission authority"
                    )
                self.__trigger_admission_authority.assert_authoritative(trigger_admission)
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
            if admission_schema == "market-impact.decision-admission.v2":
                self._validate_portfolio_binding_artifacts(
                    row=row,
                    admission_payload=admission_payload,
                    query_gate_payload=query_gate_payload,
                    signal_payload=signal_payload,
                    order_payload=order_payload,
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
                    trigger_admission=trigger_admission,
                    trigger_admission_authority=self.__trigger_admission_authority,
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

    def _validate_portfolio_binding_artifacts(
        self,
        *,
        row: sqlite3.Row,
        admission_payload: dict[str, object],
        query_gate_payload: dict[str, object],
        signal_payload: dict[str, object],
        order_payload: dict[str, object],
    ) -> None:
        bindings = (
            (
                "authorized_decision_view_hash",
                "authorized_decision_view_id",
                "view_id",
                "authorized-decision-view-",
                "Authorized Decision View",
            ),
            (
                "account_state_snapshot_hash",
                "account_state_snapshot_id",
                "snapshot_id",
                "account-state-snapshot-",
                "Account State Snapshot",
            ),
            (
                "position_snapshot_hash",
                "position_snapshot_id",
                "snapshot_id",
                "position-snapshot-",
                "Position Snapshot",
            ),
            (
                "portfolio_decision_hash",
                "portfolio_decision_id",
                "decision_id",
                "portfolio-decision-",
                "Portfolio Decision",
            ),
            (
                "order_sizing_decision_hash",
                "order_sizing_decision_id",
                "decision_id",
                "order-sizing-decision-",
                "Order Sizing Decision",
            ),
        )
        payloads: dict[str, dict[str, object]] = {}
        for hash_field, admission_id_field, artifact_id_field, prefix, label in bindings:
            artifact_hash = _json_string(admission_payload, hash_field)
            payload = _json_object(
                self.artifacts.read_json(artifact_hash),
                f"{label} artifact",
            )
            if canonical_hash(payload) != artifact_hash:
                raise ValueError(f"{label} artifact hash is not exact")
            _validate_content_id(payload, id_field=artifact_id_field, prefix=prefix)
            if _json_string(admission_payload, admission_id_field) != _json_string(
                payload,
                artifact_id_field,
            ):
                raise ValueError(f"Decision Admission {label} identity is not exact")
            payloads[hash_field] = payload

        view = payloads["authorized_decision_view_hash"]
        account_state = payloads["account_state_snapshot_hash"]
        position = payloads["position_snapshot_hash"]
        portfolio = payloads["portfolio_decision_hash"]
        sizing = payloads["order_sizing_decision_hash"]
        instrument_rule_set_hash = _json_string(sizing, "instrument_rule_set_hash")
        instrument_rule_set = _json_object(
            self.artifacts.read_json(instrument_rule_set_hash),
            "instrument rule-set artifact",
        )
        if canonical_hash(instrument_rule_set) != instrument_rule_set_hash or _json_string(
            instrument_rule_set, "rule_set_id"
        ) != _json_string(sizing, "instrument_rule_set_id"):
            raise ValueError("Order Sizing instrument rule-set artifact is not exact")
        raw_rules = instrument_rule_set.get("rules")
        if (
            not isinstance(raw_rules, list)
            or sum(
                1
                for item in cast(list[object], raw_rules)
                if _json_string(_json_object(item, "instrument rule"), "rule_key")
                == _json_string(sizing, "instrument_rule_key")
            )
            != 1
        ):
            raise ValueError("Order Sizing instrument rule is absent or ambiguous")
        sizing_policy = _json_object(sizing.get("sizing_policy"), "Order Sizing Policy")
        if _json_string(sizing, "sizing_policy_id") != (
            f"order-sizing-policy-{canonical_hash(sizing_policy)}"
        ):
            raise ValueError("Order Sizing Policy identity is not exact")
        mandate = _json_object(
            self.artifacts.read_json(cast(str, row["mandate_hash"])),
            "Trading Mandate artifact",
        )
        price_basis = _json_object(
            self.artifacts.read_json(cast(str, row["price_basis_hash"])),
            "Price Basis artifact",
        )
        trusted_account_state = self.__account_state_snapshots.get(
            _json_string(account_state, "snapshot_id")
        )
        if trusted_account_state is None or trusted_account_state.to_dict() != account_state:
            raise ValueError("persisted Account State lacks current trusted authority")
        parsed_account_state = account_state_snapshot_from_dict(account_state)
        parsed_position = position_snapshot_from_dict(position)
        if (
            parsed_account_state.project_positions(
                evaluated_at=parsed_position.evaluated_at,
                max_age=timedelta(seconds=parsed_position.max_age_seconds),
            ).to_dict()
            != position
        ):
            raise ValueError("persisted Position Snapshot is not a trusted projection")
        if (
            authorized_decision_view_from_dict(view).to_dict()
            != AuthorizedDecisionView.build(
                cutoff=_datetime(_json_string(view, "cutoff")),
                frozen_at=_datetime(_json_string(view, "frozen_at")),
                data_snapshot_ids=tuple(
                    sorted(_json_string_list(query_gate_payload, "authorized_snapshot_ids"))
                ),
                decision_input_ids=tuple(
                    sorted(
                        _json_string_list(
                            query_gate_payload,
                            "authorized_decision_input_ids",
                        )
                    )
                ),
                position_snapshot=parsed_position,
            ).to_dict()
        ):
            raise ValueError("persisted Authorized Decision View is not deterministic")
        trusted_identity = self.__instrument_identities.get(
            _json_string(portfolio, "instrument_id")
        )
        if trusted_identity != (
            _json_string(portfolio, "venue"),
            _json_string(portfolio, "instrument_class"),
        ):
            raise ValueError("persisted Portfolio Decision lacks Instrument Master authority")
        if (
            tuple(sorted(_json_string_list(view, "data_snapshot_ids")))
            != tuple(sorted(_json_string_list(query_gate_payload, "authorized_snapshot_ids")))
            or tuple(sorted(_json_string_list(view, "decision_input_ids")))
            != tuple(sorted(_json_string_list(query_gate_payload, "authorized_decision_input_ids")))
            or _json_string(view, "position_snapshot_id") != _json_string(position, "snapshot_id")
            or _json_string(position, "account_state_snapshot_id")
            != _json_string(account_state, "snapshot_id")
        ):
            raise ValueError("Authorized Decision View Query Gate lineage is not exact")
        if (
            _json_string(portfolio, "outcome") != "ready_for_sizing"
            or _json_string(portfolio, "signal_id") != _json_string(signal_payload, "signal_id")
            or _json_string(portfolio, "signal_hash") != canonical_hash(signal_payload)
            or _json_string(portfolio, "authorized_decision_view_id")
            != _json_string(view, "view_id")
            or _json_string(portfolio, "authorized_decision_view_hash") != canonical_hash(view)
            or _json_string(portfolio, "position_snapshot_id")
            != _json_string(position, "snapshot_id")
            or _json_string(portfolio, "position_snapshot_hash") != canonical_hash(position)
            or portfolio.get("blockers") != []
            or portfolio.get("execution_capability") is not False
        ):
            raise ValueError("Portfolio Decision persisted lineage is not exact")
        if (
            _json_string(sizing, "outcome") != "ready"
            or _json_string(sizing, "portfolio_decision_id")
            != _json_string(portfolio, "decision_id")
            or _json_string(sizing, "portfolio_decision_hash") != canonical_hash(portfolio)
            or _json_string(sizing, "position_snapshot_id") != _json_string(position, "snapshot_id")
            or _json_string(sizing, "position_snapshot_hash") != canonical_hash(position)
            or _json_string(sizing, "trading_mandate_hash") != cast(str, row["mandate_hash"])
            or _json_string(sizing, "price_basis_hash") != cast(str, row["price_basis_hash"])
            or sizing.get("blockers") != []
            or sizing.get("execution_capability") is not False
        ):
            raise ValueError("Order Sizing Decision persisted lineage is not exact")
        if (
            canonical_hash(mandate) != cast(str, row["mandate_hash"])
            or canonical_hash(price_basis) != cast(str, row["price_basis_hash"])
            or _json_string(order_payload, "account_id") != _json_string(mandate, "account_id")
            or _json_string(position, "account_reference_hash")
            != _json_string(mandate, "account_id")
            or _json_string(order_payload, "instrument_id") != _json_string(sizing, "instrument_id")
            or _json_string(order_payload, "side") != _json_string(sizing, "side")
            or _json_string(order_payload, "quantity") != _json_string(sizing, "quantity")
            or _json_string(order_payload, "order_kind") != _json_string(sizing, "order_kind")
            or order_payload.get("limit_price") != sizing.get("limit_price")
            or _json_string(order_payload, "created_at") != _json_string(sizing, "decided_at")
        ):
            raise ValueError("Order Intent is not the persisted deterministic sizing output")

    def _require_current_account_state(
        self,
        *,
        account_state: AccountStateSnapshot,
        position_snapshot: PositionSnapshot,
        evaluated_at: datetime,
    ) -> None:
        require_aware(evaluated_at, "account-state currency time")
        source = self.__account_state_source
        if source is None:
            raise PermissionError("Agent-directed paper requires a current Account State source")
        current = source()
        if current.to_dict() != account_state.to_dict():
            raise PermissionError("current Account State differs from the admitted snapshot")
        if evaluated_at < current.reconciled_at:
            raise PermissionError("current Account State is future-dated")
        if position_snapshot.max_age_seconds != int(self.__account_state_max_age.total_seconds()):
            raise PermissionError("Position Snapshot freshness policy differs from Harness policy")
        if evaluated_at - current.as_of > self.__account_state_max_age:
            raise PermissionError("current Account State is stale")

    def _agent_account_state_is_current(
        self,
        row: sqlite3.Row,
        *,
        evaluated_at: datetime,
    ) -> bool:
        admission_hash = cast(str | None, row["agent_admission_hash"])
        if admission_hash is None:
            return True
        admission = _json_object(
            self.artifacts.read_json(admission_hash),
            "Decision Admission artifact",
        )
        if _json_string(admission, "schema_version") != DECISION_ADMISSION_SCHEMA_V2:
            return False
        account_state = account_state_snapshot_from_dict(
            self.artifacts.read_json(_json_string(admission, "account_state_snapshot_hash"))
        )
        position_snapshot = position_snapshot_from_dict(
            self.artifacts.read_json(_json_string(admission, "position_snapshot_hash"))
        )
        try:
            self._require_current_account_state(
                account_state=account_state,
                position_snapshot=position_snapshot,
                evaluated_at=evaluated_at,
            )
        except PermissionError:
            return False
        return True

    def _submission_authorities_are_current(
        self,
        row: sqlite3.Row,
        *,
        evaluated_at: datetime,
    ) -> bool:
        require_aware(evaluated_at, "submission authority time")
        if (
            evaluated_at >= _datetime(cast(str, row["order_expires_at"]))
            or evaluated_at >= _datetime(cast(str, row["mandate_expires_at"]))
            or evaluated_at >= _datetime(cast(str, row["price_valid_until"]))
        ):
            return False
        return self._agent_account_state_is_current(row, evaluated_at=evaluated_at)

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
                    filled_quantity TEXT NOT NULL DEFAULT '0',
                    fill_ids_json TEXT NOT NULL DEFAULT '[]',
                    provider_observed_at TEXT,
                    provider_id TEXT NOT NULL,
                    provider_version TEXT NOT NULL,
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
                    kill_generation INTEGER NOT NULL,
                    observed_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS paper_cancellations (
                    cancellation_id TEXT PRIMARY KEY,
                    client_order_id TEXT NOT NULL REFERENCES paper_intents(client_order_id),
                    provider_order_id TEXT NOT NULL,
                    provider_id TEXT NOT NULL,
                    provider_version TEXT NOT NULL,
                    request_hash TEXT NOT NULL,
                    approval_hash TEXT,
                    state TEXT NOT NULL,
                    lease_token TEXT,
                    lease_expires_at TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS paper_cancellation_attempts (
                    attempt_id TEXT PRIMARY KEY,
                    cancellation_id TEXT NOT NULL
                        REFERENCES paper_cancellations(cancellation_id),
                    status TEXT NOT NULL,
                    receipt_hash TEXT,
                    error_kind TEXT,
                    started_at TEXT NOT NULL,
                    finished_at TEXT
                );
                CREATE TABLE IF NOT EXISTS paper_replacements (
                    replacement_id TEXT PRIMARY KEY,
                    canceled_client_order_id TEXT NOT NULL
                        REFERENCES paper_intents(client_order_id),
                    cancellation_id TEXT NOT NULL UNIQUE
                        REFERENCES paper_cancellations(cancellation_id),
                    replacement_order_hash TEXT NOT NULL,
                    admitted_client_order_id TEXT,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS paper_execution_gate (
                    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                    blocked INTEGER NOT NULL,
                    reason TEXT,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS paper_kill_switch (
                    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                    active INTEGER NOT NULL,
                    reason TEXT,
                    actor_ref TEXT,
                    generation INTEGER NOT NULL,
                    updated_at TEXT NOT NULL
                );
                INSERT OR IGNORE INTO paper_execution_gate (
                    singleton, blocked, reason, updated_at
                ) VALUES (1, 0, NULL, '1970-01-01T00:00:00Z');
                INSERT OR IGNORE INTO paper_kill_switch (
                    singleton, active, reason, actor_ref, generation, updated_at
                ) VALUES (1, 0, NULL, NULL, 0, '1970-01-01T00:00:00Z');
                """
            )
            columns = {
                cast(str, row["name"])
                for row in connection.execute("PRAGMA table_info(paper_intents)").fetchall()
            }
            if "agent_admission_hash" not in columns:
                connection.execute("ALTER TABLE paper_intents ADD COLUMN agent_admission_hash TEXT")
            if "provider_id" not in columns:
                connection.execute("ALTER TABLE paper_intents ADD COLUMN provider_id TEXT")
            if "provider_version" not in columns:
                connection.execute("ALTER TABLE paper_intents ADD COLUMN provider_version TEXT")
            if "filled_quantity" not in columns:
                connection.execute(
                    "ALTER TABLE paper_intents ADD COLUMN filled_quantity TEXT NOT NULL DEFAULT '0'"
                )
            if "fill_ids_json" not in columns:
                connection.execute(
                    "ALTER TABLE paper_intents ADD COLUMN fill_ids_json TEXT NOT NULL DEFAULT '[]'"
                )
            if "provider_observed_at" not in columns:
                connection.execute("ALTER TABLE paper_intents ADD COLUMN provider_observed_at TEXT")
            cancellation_columns = {
                cast(str, row["name"])
                for row in connection.execute("PRAGMA table_info(paper_cancellations)").fetchall()
            }
            if "provider_id" not in cancellation_columns:
                connection.execute("ALTER TABLE paper_cancellations ADD COLUMN provider_id TEXT")
            if "provider_version" not in cancellation_columns:
                connection.execute(
                    "ALTER TABLE paper_cancellations ADD COLUMN provider_version TEXT"
                )
            reconciliation_columns = {
                cast(str, row["name"])
                for row in connection.execute(
                    "PRAGMA table_info(paper_reconciliation_runs)"
                ).fetchall()
            }
            if "kill_generation" not in reconciliation_columns:
                connection.execute(
                    """
                    ALTER TABLE paper_reconciliation_runs
                    ADD COLUMN kill_generation INTEGER
                    """
                )
            kill_columns = {
                cast(str, row["name"])
                for row in connection.execute("PRAGMA table_info(paper_kill_switch)").fetchall()
            }
            if "generation" not in kill_columns:
                connection.execute(
                    "ALTER TABLE paper_kill_switch ADD COLUMN generation INTEGER NOT NULL DEFAULT 0"
                )

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
                   OR (outbox_state = ? AND provider_status = ?)
                LIMIT 1
                """,
                (
                    OutboxState.SUBMITTING.value,
                    OutboxState.UNKNOWN.value,
                    OutboxState.ACCEPTED.value,
                    OutboxState.RECONCILED.value,
                    ExecutionStatus.ACCEPTED.value,
                ),
            ).fetchone()
            cancellation = connection.execute(
                """
                SELECT 1 FROM paper_cancellations
                WHERE state IN (?, ?, ?)
                LIMIT 1
                """,
                (
                    CancellationState.CANCELING.value,
                    CancellationState.UNKNOWN.value,
                    CancellationState.ACKNOWLEDGED.value,
                ),
            ).fetchone()
            if row is not None or cancellation is not None:
                self._set_gate(connection, True, "reconciliation_required", now)
                connection.commit()

    def _claim_next(self, now: datetime) -> tuple[sqlite3.Row, str] | None:
        with self._connect() as connection:
            candidate = connection.execute(
                """
                SELECT * FROM paper_intents
                WHERE outbox_state = ?
                ORDER BY created_at, client_order_id
                LIMIT 1
                """,
                (OutboxState.QUEUED.value,),
            ).fetchone()
        if candidate is None:
            return None
        self._validate_binding_artifacts(candidate)
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            gate = connection.execute(
                "SELECT blocked FROM paper_execution_gate WHERE singleton = 1"
            ).fetchone()
            if gate is None:
                connection.rollback()
                raise RuntimeError("paper execution gate is missing")
            kill = connection.execute(
                "SELECT active FROM paper_kill_switch WHERE singleton = 1"
            ).fetchone()
            if kill is None:
                connection.rollback()
                raise RuntimeError("paper kill switch is missing")
            pending_cancellation = connection.execute(
                """
                SELECT 1 FROM paper_cancellations
                WHERE state IN (?, ?, ?, ?, ?)
                LIMIT 1
                """,
                (
                    CancellationState.PENDING_APPROVAL.value,
                    CancellationState.QUEUED.value,
                    CancellationState.CANCELING.value,
                    CancellationState.UNKNOWN.value,
                    CancellationState.ACKNOWLEDGED.value,
                ),
            ).fetchone()
            if bool(gate["blocked"]) or bool(kill["active"]) or pending_cancellation is not None:
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
            if row["client_order_id"] != candidate["client_order_id"]:
                connection.commit()
                return None
            self._validate_binding_artifacts(row)
            client_order_id = cast(str, row["client_order_id"])
            if not self._provider_binding_matches(row):
                connection.execute(
                    """
                    UPDATE paper_intents
                    SET outbox_state = ?, updated_at = ?
                    WHERE client_order_id = ? AND outbox_state = ?
                    """,
                    (
                        OutboxState.EXPIRED.value,
                        _timestamp(now),
                        client_order_id,
                        OutboxState.QUEUED.value,
                    ),
                )
                connection.commit()
                return None
            if not self._submission_authorities_are_current(
                row,
                evaluated_at=now,
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

    def _claim_next_cancellation(self, now: datetime) -> tuple[sqlite3.Row, str] | None:
        with self._connect() as connection:
            candidate = connection.execute(
                """
                SELECT * FROM paper_cancellations
                WHERE state = ?
                ORDER BY created_at, cancellation_id
                LIMIT 1
                """,
                (CancellationState.QUEUED.value,),
            ).fetchone()
        if candidate is None:
            return None
        self.get_cancellation(cast(str, candidate["cancellation_id"]))
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            gate = connection.execute(
                "SELECT blocked FROM paper_execution_gate WHERE singleton = 1"
            ).fetchone()
            if gate is None:
                raise RuntimeError("paper execution gate is missing")
            if bool(gate["blocked"]):
                connection.commit()
                return None
            row = connection.execute(
                """
                SELECT * FROM paper_cancellations
                WHERE state = ?
                ORDER BY created_at, cancellation_id
                LIMIT 1
                """,
                (CancellationState.QUEUED.value,),
            ).fetchone()
            if row is None or row["cancellation_id"] != candidate["cancellation_id"]:
                connection.commit()
                return None
            if not self._provider_binding_matches(row):
                connection.execute(
                    """
                    UPDATE paper_cancellations
                    SET state = ?, updated_at = ?
                    WHERE cancellation_id = ? AND state = ?
                    """,
                    (
                        CancellationState.EXPIRED.value,
                        _timestamp(now),
                        cast(str, row["cancellation_id"]),
                        CancellationState.QUEUED.value,
                    ),
                )
                connection.commit()
                return None
            attempt_id = uuid.uuid4().hex
            connection.execute(
                """
                UPDATE paper_cancellations
                SET state = ?, lease_token = ?, lease_expires_at = ?, updated_at = ?
                WHERE cancellation_id = ? AND state = ?
                """,
                (
                    CancellationState.CANCELING.value,
                    attempt_id,
                    _timestamp(now + timedelta(seconds=self.lease_timeout_seconds)),
                    _timestamp(now),
                    cast(str, row["cancellation_id"]),
                    CancellationState.QUEUED.value,
                ),
            )
            connection.execute(
                """
                INSERT INTO paper_cancellation_attempts (
                    attempt_id, cancellation_id, status, receipt_hash,
                    error_kind, started_at, finished_at
                ) VALUES (?, ?, 'canceling', NULL, NULL, ?, NULL)
                """,
                (attempt_id, cast(str, row["cancellation_id"]), _timestamp(now)),
            )
            self._set_gate(connection, True, "cancellation_in_flight", now)
            connection.commit()
            claimed = connection.execute(
                "SELECT * FROM paper_cancellations WHERE cancellation_id = ?",
                (cast(str, row["cancellation_id"]),),
            ).fetchone()
        if claimed is None:
            raise RuntimeError("claimed paper cancellation disappeared")
        return claimed, attempt_id

    def _expire_cancellation_before_provider(
        self,
        row: sqlite3.Row,
        *,
        attempt_id: str,
        expired_at: datetime,
        error_kind: str,
    ) -> None:
        cancellation_id = cast(str, row["cancellation_id"])
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            updated = connection.execute(
                """
                UPDATE paper_cancellations
                SET state = ?, lease_token = NULL, lease_expires_at = NULL, updated_at = ?
                WHERE cancellation_id = ? AND state = ? AND lease_token = ?
                """,
                (
                    CancellationState.EXPIRED.value,
                    _timestamp(expired_at),
                    cancellation_id,
                    CancellationState.CANCELING.value,
                    attempt_id,
                ),
            )
            if updated.rowcount != 1:
                raise RuntimeError("paper cancellation lease was lost before Provider call")
            connection.execute(
                """
                UPDATE paper_cancellation_attempts
                SET status = 'expired_before_provider', error_kind = ?, finished_at = ?
                WHERE attempt_id = ?
                """,
                (error_kind, _timestamp(expired_at), attempt_id),
            )
            self._set_gate(connection, False, None, expired_at)
            connection.commit()

    def _finish_ambiguous_cancellation(
        self,
        cancellation_id: str,
        *,
        attempt_id: str,
        observed_at: datetime,
        error_kind: str,
    ) -> None:
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            updated = connection.execute(
                """
                UPDATE paper_cancellations
                SET state = ?, lease_token = NULL, lease_expires_at = NULL, updated_at = ?
                WHERE cancellation_id = ? AND state = ? AND lease_token = ?
                """,
                (
                    CancellationState.UNKNOWN.value,
                    _timestamp(observed_at),
                    cancellation_id,
                    CancellationState.CANCELING.value,
                    attempt_id,
                ),
            )
            if updated.rowcount != 1:
                raise RuntimeError("paper cancellation lease was lost after Provider call")
            connection.execute(
                """
                UPDATE paper_cancellation_attempts
                SET status = 'unknown', error_kind = ?, finished_at = ?
                WHERE attempt_id = ?
                """,
                (error_kind, _timestamp(observed_at), attempt_id),
            )
            self._set_gate(connection, True, "ambiguous_cancellation", observed_at)
            connection.commit()

    def _recover_expired_cancellation_leases(self, now: datetime) -> None:
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            rows = connection.execute(
                """
                SELECT cancellation_id, lease_token FROM paper_cancellations
                WHERE state = ? AND lease_expires_at <= ?
                """,
                (CancellationState.CANCELING.value, _timestamp(now)),
            ).fetchall()
            for row in rows:
                cancellation_id = cast(str, row["cancellation_id"])
                attempt_id = cast(str, row["lease_token"])
                connection.execute(
                    """
                    UPDATE paper_cancellations
                    SET state = ?, lease_token = NULL, lease_expires_at = NULL, updated_at = ?
                    WHERE cancellation_id = ?
                    """,
                    (
                        CancellationState.UNKNOWN.value,
                        _timestamp(now),
                        cancellation_id,
                    ),
                )
                connection.execute(
                    """
                    UPDATE paper_cancellation_attempts
                    SET status = 'unknown', error_kind = 'lease_expired', finished_at = ?
                    WHERE attempt_id = ?
                    """,
                    (_timestamp(now), attempt_id),
                )
            if rows:
                self._set_gate(connection, True, "ambiguous_cancellation", now)
            connection.commit()

    def _expire_claim_before_submit(
        self,
        row: sqlite3.Row,
        *,
        submission_id: str,
        expired_at: datetime,
        error_kind: str,
    ) -> None:
        client_order_id = cast(str, row["client_order_id"])
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            updated = connection.execute(
                """
                UPDATE paper_intents
                SET outbox_state = ?, lease_token = NULL, lease_expires_at = NULL,
                    updated_at = ?
                WHERE client_order_id = ? AND outbox_state = ? AND lease_token = ?
                """,
                (
                    OutboxState.EXPIRED.value,
                    _timestamp(expired_at),
                    client_order_id,
                    OutboxState.SUBMITTING.value,
                    submission_id,
                ),
            )
            if updated.rowcount != 1:
                connection.rollback()
                raise RuntimeError("paper submission lease was lost before provider submit")
            connection.execute(
                """
                UPDATE paper_submission_attempts
                SET status = 'expired_before_submit',
                    error_kind = ?,
                    finished_at = ?
                WHERE submission_id = ?
                """,
                (error_kind, _timestamp(expired_at), submission_id),
            )
            self._append_event(
                connection,
                client_order_id,
                "submission_expired_before_provider",
                cast(str, row["approval_hash"]),
                expired_at,
            )
            self._set_gate(connection, False, None, expired_at)
            connection.commit()

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
        filled_quantity=Decimal(cast(str, row["filled_quantity"])),
        fill_ids=tuple(cast(list[str], json.loads(cast(str, row["fill_ids_json"])))),
        provider_observed_at=(
            _datetime(cast(str, row["provider_observed_at"]))
            if row["provider_observed_at"] is not None
            else None
        ),
        updated_at=_datetime(cast(str, row["updated_at"])),
    )


def _cancellation_record(row: sqlite3.Row) -> PaperCancellationRecord:
    return PaperCancellationRecord(
        cancellation_id=cast(str, row["cancellation_id"]),
        client_order_id=cast(str, row["client_order_id"]),
        provider_order_id=cast(str, row["provider_order_id"]),
        provider_id=cast(str, row["provider_id"]),
        provider_version=cast(str, row["provider_version"]),
        request_hash=cast(str, row["request_hash"]),
        approval_hash=cast(str | None, row["approval_hash"]),
        state=CancellationState(cast(str, row["state"])),
        updated_at=_datetime(cast(str, row["updated_at"])),
    )


def _replacement_record(row: sqlite3.Row) -> PaperReplacementRecord:
    return PaperReplacementRecord(
        replacement_id=cast(str, row["replacement_id"]),
        canceled_client_order_id=cast(str, row["canceled_client_order_id"]),
        cancellation_id=cast(str, row["cancellation_id"]),
        replacement_order_hash=cast(str, row["replacement_order_hash"]),
        admitted_client_order_id=cast(str | None, row["admitted_client_order_id"]),
        created_at=_datetime(cast(str, row["created_at"])),
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


def _validate_content_id(
    payload: dict[str, object],
    *,
    id_field: str,
    prefix: str,
) -> None:
    artifact_id = _json_string(payload, id_field)
    core = {key: value for key, value in payload.items() if key != id_field}
    if artifact_id != f"{prefix}{canonical_hash(core)}":
        raise ValueError(f"artifact {id_field} does not match content")


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
    return mandate.to_dict()


def _receipt_dict(receipt: ExecutionReceipt) -> dict[str, object]:
    return {
        "schema_version": "market-impact.execution-receipt.v1",
        "client_order_id": receipt.client_order_id,
        "provider_order_id": receipt.provider_order_id,
        "status": receipt.status.value,
        "observed_at": _timestamp(receipt.observed_at),
    }


def _execution_transition_allowed(
    previous_value: str | None,
    current: ExecutionStatus,
) -> bool:
    if current is ExecutionStatus.UNKNOWN:
        return False
    if previous_value is None:
        return True
    try:
        previous = ExecutionStatus(previous_value)
    except ValueError:
        return True
    allowed: dict[ExecutionStatus, frozenset[ExecutionStatus]] = {
        ExecutionStatus.ACCEPTED: frozenset(
            {
                ExecutionStatus.ACCEPTED,
                ExecutionStatus.PENDING_CANCEL,
                ExecutionStatus.CANCELED,
                ExecutionStatus.PARTIALLY_FILLED,
                ExecutionStatus.FILLED,
                ExecutionStatus.REJECTED,
                ExecutionStatus.EXPIRED,
            }
        ),
        ExecutionStatus.PENDING_CANCEL: frozenset(
            {
                ExecutionStatus.ACCEPTED,
                ExecutionStatus.PENDING_CANCEL,
                ExecutionStatus.CANCELED,
                ExecutionStatus.PARTIALLY_FILLED,
                ExecutionStatus.FILLED,
            }
        ),
        ExecutionStatus.PARTIALLY_FILLED: frozenset(
            {
                ExecutionStatus.PARTIALLY_FILLED,
                ExecutionStatus.PENDING_CANCEL,
                ExecutionStatus.CANCELED,
                ExecutionStatus.FILLED,
                ExecutionStatus.EXPIRED,
            }
        ),
        ExecutionStatus.CANCELED: frozenset({ExecutionStatus.CANCELED}),
        ExecutionStatus.FILLED: frozenset({ExecutionStatus.FILLED}),
        ExecutionStatus.REJECTED: frozenset({ExecutionStatus.REJECTED}),
        ExecutionStatus.EXPIRED: frozenset({ExecutionStatus.EXPIRED}),
        ExecutionStatus.UNKNOWN: frozenset(),
    }
    return current in allowed[previous]


def _provider_accepts_new_orders(provider: ExecutionProvider) -> bool:
    if isinstance(provider, NewOrderAdmissionProvider):
        return provider.new_order_admission_open
    return True


def _cancellation_receipt_dict(receipt: CancellationCommandReceipt) -> dict[str, object]:
    return {
        "schema_version": "market-impact.cancellation-command-receipt.v1",
        "client_order_id": receipt.client_order_id,
        "provider_order_id": receipt.provider_order_id,
        "cancellation_id": receipt.cancellation_id,
        "status": receipt.status.value,
        "observed_at": _timestamp(receipt.observed_at),
    }


def _validate_cancellation_receipt(
    receipt: CancellationCommandReceipt,
    capability: CancellationCapability,
) -> None:
    if (
        receipt.client_order_id != capability.client_order_id
        or receipt.provider_order_id != capability.provider_order_id
        or receipt.cancellation_id != capability.cancellation_id
    ):
        raise ValueError("Provider cancellation receipt identity mismatch")


def _timestamp(value: datetime) -> str:
    require_aware(value, "timestamp")
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _datetime(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    require_aware(parsed, "timestamp")
    return parsed
