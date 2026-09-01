from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from enum import StrEnum
from typing import Protocol, cast

from market_impact_agent.account_state import AccountStateSnapshot, PositionSnapshot
from market_impact_agent.agent_contracts import (
    EvidencePack,
    JudgmentArtifact,
    canonical_hash,
    evidence_pack_from_dict,
)
from market_impact_agent.agent_engine import AgentEngine, AgentRunRequest, AgentRunResult
from market_impact_agent.agent_runtime import ToolAccessContext, ToolSideEffect
from market_impact_agent.authorized_decision_view import AuthorizedDecisionView
from market_impact_agent.checkpoint_market_universe import ExchangeInstrumentRuleSet
from market_impact_agent.data_inputs import LocalDataSnapshotStore
from market_impact_agent.decision_admission import (
    DecisionAdmission,
    DecisionDisposition,
    DecisionRunManifest,
    PairedDecisionRun,
    build_decision_run_manifest,
    build_signal_from_decision_manifest,
    prepare_portfolio_decision_admission,
)
from market_impact_agent.domain import (
    ApprovalMode,
    OrderIntent,
    OrderKind,
    SignalIntent,
    TradingEnvironment,
)
from market_impact_agent.modeled_pit_readiness import (
    _materialize_modeled_pit_readiness_checkpoints,  # pyright: ignore[reportPrivateUsage]
    _record_pipeline_modeled_pit_readiness,  # pyright: ignore[reportPrivateUsage]
    _reopen_pipeline_modeled_pit_readiness,  # pyright: ignore[reportPrivateUsage]
)
from market_impact_agent.paper_execution import (
    PaperExecutionService,
    PaperIntentRecord,
    PriceBasis,
    ReconciliationRun,
)
from market_impact_agent.portfolio_decision import (
    OrderSizingDecision,
    OrderSizingOutcome,
    OrderSizingPolicy,
    PortfolioAction,
    PortfolioDecision,
    PortfolioDecisionOutcome,
    build_order_intent_from_sizing,
    evaluate_portfolio_decision,
    size_portfolio_decision,
)
from market_impact_agent.prospective_checkpoint_sets import (
    ProspectiveCheckpointSnapshotSet,
    build_checkpoint_tool_descriptors,
    materialize_checkpoint_decision_inputs,
    prospective_checkpoint_snapshot_set_from_dict,
)
from market_impact_agent.prospective_diagnostic import (
    PROSPECTIVE_DIAGNOSTIC_REGISTRATION_SCHEMA_V4,
    ProspectiveDiagnosticRegistration,
    prospective_diagnostic_registration_from_dict,
)
from market_impact_agent.prospective_execution import (
    ProspectiveExecutionPlan,
    prospective_execution_plan_from_dict,
)
from market_impact_agent.prospective_query_gate import (
    ProspectiveQueryGateResult,
    evaluate_prospective_query_gate,
)
from market_impact_agent.prospective_trigger_admission import (
    ProspectiveEventAssessmentArtifact,
    ProspectiveTriggerAdmission,
    ProspectiveTriggerAdmissionStore,
    TriggerAdmissionAuthority,
)
from market_impact_agent.providers import MockExecutionProvider
from market_impact_agent.runtime_store import ArtifactStore
from market_impact_agent.usage_ledger import UsageLedger, UsageRecord


class ProspectiveDecisionPipelineStatus(StrEnum):
    QUERY_BLOCKED = "query_blocked"
    JUDGMENT_TERMINAL = "judgment_terminal"
    PORTFOLIO_TERMINAL = "portfolio_terminal"
    PENDING_MANUAL_APPROVAL = "pending_manual_approval"


class TriggerAdmissionRepository(TriggerAdmissionAuthority, Protocol):
    def get(self, admission_id: str) -> ProspectiveTriggerAdmission: ...


@dataclass(frozen=True, slots=True)
class FrozenProspectiveDecisionRefs:
    registration_hash: str
    checkpoint_snapshot_set_hash: str
    evidence_pack_hash: str
    execution_plan_hash: str
    trigger_admission_id: str


@dataclass(frozen=True, slots=True)
class ProspectivePortfolioInstruction:
    requested_action: PortfolioAction
    venue: str
    instrument_class: str
    order_kind: OrderKind
    signal_valid_for: timedelta
    order_valid_for: timedelta
    account_state_max_age: timedelta = timedelta(minutes=5)

    def __post_init__(self) -> None:
        if self.signal_valid_for <= timedelta(0):
            raise ValueError("Signal validity must be positive")
        if not timedelta(0) < self.order_valid_for <= self.signal_valid_for:
            raise ValueError("Order validity must be positive and fit inside Signal validity")
        if self.account_state_max_age <= timedelta(0):
            raise ValueError("Account State maximum age must be positive")


@dataclass(frozen=True, slots=True)
class ProspectiveDecisionPipelineResult:
    status: ProspectiveDecisionPipelineStatus
    query_gate: ProspectiveQueryGateResult
    paired_runs: tuple[PairedDecisionRun, ...] = ()
    manifest: DecisionRunManifest | None = None
    signal: SignalIntent | None = None
    authorized_view: AuthorizedDecisionView | None = None
    position_snapshot: PositionSnapshot | None = None
    portfolio_decision: PortfolioDecision | None = None
    sizing_decision: OrderSizingDecision | None = None
    order: OrderIntent | None = None
    admission: DecisionAdmission | None = None
    paper_record: PaperIntentRecord | None = None
    reconciliation: ReconciliationRun | None = None


@dataclass(slots=True)
class ProspectiveDecisionPipeline:
    frozen_artifacts: ArtifactStore
    snapshot_store: LocalDataSnapshotStore
    trigger_store: TriggerAdmissionRepository
    engines: Mapping[str, AgentEngine]
    usage_ledger: UsageLedger
    paper_service: PaperExecutionService
    account_state: AccountStateSnapshot
    instrument_rule_set: ExchangeInstrumentRuleSet
    sizing_policy: OrderSizingPolicy
    price_basis: PriceBasis
    clock: Callable[[], datetime] = lambda: datetime.now(UTC)
    runtime_ref: str = "market-impact-agent-runtime-v1"
    checkpoint_tool_capability: str = "market.read"

    def materialize_modeled_pit_readiness(
        self,
        *,
        refs: FrozenProspectiveDecisionRefs,
    ) -> tuple[dict[str, object], ...]:
        """Materialize Judgment-only readiness from this root's durable state.

        A custom Trigger authority is sufficient for ordinary pipeline tests but
        cannot establish the full EventAssessment provenance required here.
        """

        registration, snapshot_set, trigger, assessment = self._reopen_modeled_pit_context(refs)
        checkpoints = _materialize_modeled_pit_readiness_checkpoints(
            registration=registration,
            snapshot_set=snapshot_set,
            snapshot_store=self.snapshot_store,
            trigger=trigger,
            assessment=assessment,
            rule_set=self.instrument_rule_set,
        )
        rule_set_payload = self.instrument_rule_set.to_dict()
        rule_set_artifact = self.snapshot_store.artifacts.put_json(rule_set_payload)
        for checkpoint in checkpoints:
            artifact = self.snapshot_store.artifacts.put_json(checkpoint)
            _record_pipeline_modeled_pit_readiness(
                store=self.snapshot_store,
                checkpoint=checkpoint,
                artifact_hash=artifact.content_hash,
                registration_artifact_hash=refs.registration_hash,
                snapshot_set_artifact_hash=refs.checkpoint_snapshot_set_hash,
                registration_id=registration.registration_id,
                snapshot_set_id=snapshot_set.snapshot_set_id,
                admission_id=trigger.admission_id,
                assessment_id=assessment.assessment_id,
                rule_set_id=self.instrument_rule_set.rule_set_id,
                rule_set_artifact_hash=rule_set_artifact.content_hash,
            )
        return checkpoints

    def reopen_modeled_pit_readiness(
        self,
        *,
        refs: FrozenProspectiveDecisionRefs,
        checkpoint_id: str,
    ) -> dict[str, object]:
        """Reopen one checkpoint from this Harness root and reconstruct its result."""

        registration, snapshot_set, trigger, assessment = self._reopen_modeled_pit_context(refs)
        checkpoints = _materialize_modeled_pit_readiness_checkpoints(
            registration=registration,
            snapshot_set=snapshot_set,
            snapshot_store=self.snapshot_store,
            trigger=trigger,
            assessment=assessment,
            rule_set=self.instrument_rule_set,
        )
        expected = next(
            (item for item in checkpoints if item["checkpoint_id"] == checkpoint_id),
            None,
        )
        if expected is None:
            raise PermissionError("modeled-PIT checkpoint is absent from current source derivation")
        return _reopen_pipeline_modeled_pit_readiness(
            store=self.snapshot_store,
            checkpoint_id=checkpoint_id,
            expected_checkpoint=expected,
            registration_artifact_hash=refs.registration_hash,
            snapshot_set_artifact_hash=refs.checkpoint_snapshot_set_hash,
            registration_id=registration.registration_id,
            snapshot_set_id=snapshot_set.snapshot_set_id,
            admission_id=trigger.admission_id,
            assessment_id=assessment.assessment_id,
            rule_set_id=self.instrument_rule_set.rule_set_id,
            rule_set_payload=self.instrument_rule_set.to_dict(),
        )

    def _reopen_modeled_pit_context(
        self,
        refs: FrozenProspectiveDecisionRefs,
    ) -> tuple[
        ProspectiveDiagnosticRegistration,
        ProspectiveCheckpointSnapshotSet,
        ProspectiveTriggerAdmission,
        ProspectiveEventAssessmentArtifact,
    ]:
        if self.trigger_store.__class__ is not ProspectiveTriggerAdmissionStore:
            raise PermissionError(
                "Modeled-PIT readiness requires the concrete durable Trigger store"
            )
        durable_store = cast(ProspectiveTriggerAdmissionStore, self.trigger_store)
        if (
            durable_store.store.root != self.snapshot_store.root
            or durable_store.store.harness_authority_id != self.snapshot_store.harness_authority_id
        ):
            raise PermissionError("Modeled-PIT sources must share one Harness authority root")
        registration, snapshot_set, _evidence_pack, _execution_plan, trigger = self._reopen_inputs(
            refs
        )
        reopened_trigger, assessment, _materiality = durable_store.get_context(trigger.admission_id)
        if reopened_trigger != trigger or assessment is None:
            raise PermissionError(
                "Modeled-PIT readiness requires a durable material EventAssessment context"
            )
        return registration, snapshot_set, trigger, assessment

    async def run(
        self,
        *,
        refs: FrozenProspectiveDecisionRefs,
        selected_skills: Mapping[str, tuple[str, ...]],
        research_instruction: str,
        model_cost_limit_usd: Decimal,
        portfolio: ProspectivePortfolioInstruction,
    ) -> ProspectiveDecisionPipelineResult:
        registration, snapshot_set, evidence_pack, execution_plan, trigger = self._reopen_inputs(
            refs
        )
        decision_inputs = materialize_checkpoint_decision_inputs(
            snapshot_set,
            store=self.snapshot_store,
        )
        evaluated_at = self._now()
        query_gate = evaluate_prospective_query_gate(
            registration=registration,
            snapshot_set=snapshot_set,
            evidence_pack=evidence_pack,
            decision_inputs=decision_inputs,
            snapshot_store=self.snapshot_store,
            execution_plan=execution_plan,
            model_profile_id=registration.model_profile_id,
            model_cost_limit_usd=model_cost_limit_usd,
            evaluated_at=evaluated_at,
            trigger_admission=trigger,
            trigger_admission_authority=self.trigger_store,
        )
        if not query_gate.model_run_eligible:
            return ProspectiveDecisionPipelineResult(
                status=ProspectiveDecisionPipelineStatus.QUERY_BLOCKED,
                query_gate=query_gate,
            )

        requests = self._prepare_requests(
            registration=registration,
            snapshot_set=snapshot_set,
            evidence_pack=evidence_pack,
            execution_plan=execution_plan,
            query_gate=query_gate,
            selected_skills=selected_skills,
            research_instruction=research_instruction,
        )
        results: dict[str, list[AgentRunResult]] = {arm: [] for arm in registration.paired_arms}
        for replicate_index in (1, 2):
            for arm in registration.paired_arms:
                results[arm].append(await self.engines[arm].run(requests[(arm, replicate_index)]))
        paired_runs = self._paired_runs(registration, results)
        manifest_at = self._now()
        try:
            manifest = build_decision_run_manifest(
                registration=registration,
                query_gate=query_gate,
                evidence_pack=evidence_pack,
                execution_plan=execution_plan,
                paired_runs=paired_runs,
                created_at=manifest_at,
            )
        except ValueError as exc:
            if str(exc) != "adaptive Decision runs require a third pair after disagreement":
                raise
            for arm in registration.paired_arms:
                results[arm].append(await self.engines[arm].run(requests[(arm, 3)]))
            paired_runs = self._paired_runs(registration, results)
            manifest_at = self._now()
            manifest = build_decision_run_manifest(
                registration=registration,
                query_gate=query_gate,
                evidence_pack=evidence_pack,
                execution_plan=execution_plan,
                paired_runs=paired_runs,
                created_at=manifest_at,
            )
        self._record_usage(execution_plan, manifest, paired_runs)
        if manifest.disposition is DecisionDisposition.ABSTAIN:
            return ProspectiveDecisionPipelineResult(
                status=ProspectiveDecisionPipelineStatus.JUDGMENT_TERMINAL,
                query_gate=query_gate,
                paired_runs=paired_runs,
                manifest=manifest,
            )

        agreeing_judgments = self._agreeing_judgments(manifest, paired_runs)
        decision_at = max(self._now(), manifest.created_at)
        signal = build_signal_from_decision_manifest(
            manifest=manifest,
            evidence_pack=evidence_pack,
            judgments=agreeing_judgments,
            valid_from=decision_at,
            expires_at=decision_at + portfolio.signal_valid_for,
        )
        position_snapshot = self.account_state.project_positions(
            evaluated_at=decision_at,
            max_age=portfolio.account_state_max_age,
        )
        authorized_view = AuthorizedDecisionView.build(
            cutoff=decision_at,
            frozen_at=decision_at,
            data_snapshot_ids=query_gate.authorized_snapshot_ids,
            decision_input_ids=query_gate.authorized_decision_input_ids,
            position_snapshot=position_snapshot,
        )
        portfolio_decision = evaluate_portfolio_decision(
            signal=signal,
            authorized_view=authorized_view,
            position_snapshot=position_snapshot,
            requested_action=portfolio.requested_action,
            venue=portfolio.venue,
            instrument_class=portfolio.instrument_class,
            evidence_refs=signal.evidence_refs,
            decided_at=decision_at,
        )
        if portfolio_decision.outcome is not PortfolioDecisionOutcome.READY_FOR_SIZING:
            return ProspectiveDecisionPipelineResult(
                status=ProspectiveDecisionPipelineStatus.PORTFOLIO_TERMINAL,
                query_gate=query_gate,
                paired_runs=paired_runs,
                manifest=manifest,
                signal=signal,
                authorized_view=authorized_view,
                position_snapshot=position_snapshot,
                portfolio_decision=portfolio_decision,
            )
        sizing_decision = size_portfolio_decision(
            portfolio_decision=portfolio_decision,
            position_snapshot=position_snapshot,
            mandate=self.paper_service.mandate,
            price_basis=self.price_basis,
            rule_set=self.instrument_rule_set,
            sizing_policy=self.sizing_policy,
            order_kind=portfolio.order_kind,
            decided_at=decision_at,
        )
        if sizing_decision.outcome is not OrderSizingOutcome.READY:
            return ProspectiveDecisionPipelineResult(
                status=ProspectiveDecisionPipelineStatus.PORTFOLIO_TERMINAL,
                query_gate=query_gate,
                paired_runs=paired_runs,
                manifest=manifest,
                signal=signal,
                authorized_view=authorized_view,
                position_snapshot=position_snapshot,
                portfolio_decision=portfolio_decision,
                sizing_decision=sizing_decision,
            )
        order = build_order_intent_from_sizing(
            sizing_decision=sizing_decision,
            signal=signal,
            mandate=self.paper_service.mandate,
            expires_at=decision_at + portfolio.order_valid_for,
        )
        admission = prepare_portfolio_decision_admission(
            manifest=manifest,
            query_gate=query_gate,
            evidence_pack=evidence_pack,
            signal=signal,
            order=order,
            authorized_view=authorized_view,
            account_state_snapshot=self.account_state,
            position_snapshot=position_snapshot,
            portfolio_decision=portfolio_decision,
            sizing_decision=sizing_decision,
            mandate=self.paper_service.mandate,
            price_basis=self.price_basis,
            created_at=decision_at,
        )
        paper_record = self.paper_service.admit_decision(
            order,
            admission,
            manifest=manifest,
            query_gate=query_gate,
            evidence_pack=evidence_pack,
            registration=registration,
            snapshot_set=snapshot_set,
            decision_inputs=decision_inputs,
            snapshot_store=self.snapshot_store,
            execution_plan=execution_plan,
            signal=signal,
            paired_runs=paired_runs,
            authorized_view=authorized_view,
            position_snapshot=position_snapshot,
            portfolio_decision=portfolio_decision,
            sizing_decision=sizing_decision,
            price_basis=self.price_basis,
            trigger_admission=trigger,
        )
        reconciliation = self.paper_service.reconcile()
        if not reconciliation.complete:
            raise RuntimeError("mock paper reconciliation did not complete")
        return ProspectiveDecisionPipelineResult(
            status=ProspectiveDecisionPipelineStatus.PENDING_MANUAL_APPROVAL,
            query_gate=query_gate,
            paired_runs=paired_runs,
            manifest=manifest,
            signal=signal,
            authorized_view=authorized_view,
            position_snapshot=position_snapshot,
            portfolio_decision=portfolio_decision,
            sizing_decision=sizing_decision,
            order=order,
            admission=admission,
            paper_record=paper_record,
            reconciliation=reconciliation,
        )

    def _reopen_inputs(
        self,
        refs: FrozenProspectiveDecisionRefs,
    ) -> tuple[
        ProspectiveDiagnosticRegistration,
        ProspectiveCheckpointSnapshotSet,
        EvidencePack,
        ProspectiveExecutionPlan,
        ProspectiveTriggerAdmission,
    ]:
        registration = prospective_diagnostic_registration_from_dict(
            self.frozen_artifacts.read_json(refs.registration_hash)
        )
        snapshot_set = prospective_checkpoint_snapshot_set_from_dict(
            self.frozen_artifacts.read_json(refs.checkpoint_snapshot_set_hash)
        )
        evidence_pack = evidence_pack_from_dict(
            self.frozen_artifacts.read_json(refs.evidence_pack_hash)
        )
        execution_plan = prospective_execution_plan_from_dict(
            self.frozen_artifacts.read_json(refs.execution_plan_hash)
        )
        trigger = self.trigger_store.get(refs.trigger_admission_id)
        self.trigger_store.assert_authoritative(trigger)
        if registration.schema_version != PROSPECTIVE_DIAGNOSTIC_REGISTRATION_SCHEMA_V4:
            raise PermissionError("one-shot prospective decisions require Trigger-bound v4 inputs")
        if (
            trigger.registration_id != registration.registration_id
            or snapshot_set.trigger_admission_id != trigger.admission_id
            or snapshot_set.registration_id != registration.registration_id
            or execution_plan.registration_id != registration.registration_id
        ):
            raise ValueError("frozen prospective decision inputs do not share one exact Trigger")
        return registration, snapshot_set, evidence_pack, execution_plan, trigger

    def _prepare_requests(
        self,
        *,
        registration: ProspectiveDiagnosticRegistration,
        snapshot_set: ProspectiveCheckpointSnapshotSet,
        evidence_pack: EvidencePack,
        execution_plan: ProspectiveExecutionPlan,
        query_gate: ProspectiveQueryGateResult,
        selected_skills: Mapping[str, tuple[str, ...]],
        research_instruction: str,
    ) -> dict[tuple[str, int], AgentRunRequest]:
        descriptors = build_checkpoint_tool_descriptors(
            snapshot_set,
            store=self.snapshot_store,
            frozen_input=query_gate.frozen_input,
            authorized_decision_input_ids=query_gate.frozen_decision_input_ids,
            required_capability=self.checkpoint_tool_capability,
        )
        tool_names = frozenset(item.name for item in descriptors)
        access = ToolAccessContext(
            allowed_capabilities=frozenset({self.checkpoint_tool_capability}),
            allowed_side_effects=frozenset({ToolSideEffect.READ_ONLY}),
            allowed_tools=tool_names,
        )
        requests: dict[tuple[str, int], AgentRunRequest] = {}
        for arm in registration.paired_arms:
            engine = self.engines.get(arm)
            if engine is None:
                raise KeyError(f"missing prospective Agent engine for arm: {arm}")
            for descriptor in descriptors:
                try:
                    engine.tool_registry.register(descriptor)
                except ValueError as exc:
                    if str(exc) != f"duplicate tool name: {descriptor.name}" or (
                        engine.tool_registry.manifest_hash(descriptor.name, access)
                        != descriptor.manifest_hash
                    ):
                        raise
            skills = selected_skills.get(arm)
            if skills is None:
                raise KeyError(f"missing prospective Skill selection for arm: {arm}")
            for index in (1, 2, 3):
                run_identity = {
                    "query_gate_result_id": query_gate.result_id,
                    "arm": arm,
                    "replicate_index": index,
                }
                request = AgentRunRequest(
                    run_id=f"prospective-decision-{canonical_hash(run_identity)}",
                    evidence_pack=evidence_pack,
                    research_instruction=research_instruction,
                    selected_skills=skills,
                    tool_access=access,
                )
                if engine.execution_binding(request, runtime_ref=self.runtime_ref) != (
                    execution_plan.arm_binding(arm)
                ):
                    raise ValueError("Agent engine differs from the frozen execution plan")
                requests[(arm, index)] = request
        return requests

    def _record_usage(
        self,
        execution_plan: ProspectiveExecutionPlan,
        manifest: DecisionRunManifest,
        paired_runs: tuple[PairedDecisionRun, ...],
    ) -> None:
        profile = execution_plan.model_provider_profile
        for item in paired_runs:
            engine = self.engines[item.arm]
            self.usage_ledger.append(
                UsageRecord.from_result(
                    experiment_id=manifest.manifest_id,
                    arm_id=item.arm,
                    recorded_at=manifest.created_at,
                    provider_profile_id=profile.profile_id,
                    provider_profile_hash=profile.profile_hash,
                    execution_binding_hash=execution_plan.arm_binding(item.arm).binding_hash,
                    run_journal_hash=engine.journal.journal_hash(item.result.run_id),
                    result=item.result,
                )
            )
        records = {item.record.run_id: item.record for item in self.usage_ledger.records()}
        selected = tuple(records[item.result.run_id] for item in paired_runs)
        if (
            len(selected) != len(paired_runs)
            or sum(item.metrics.estimated_cost_microusd for item in selected)
            != manifest.total_estimated_cost_microusd
        ):
            raise ValueError("prospective Decision Usage Ledger does not reconcile")

    @staticmethod
    def _paired_runs(
        registration: ProspectiveDiagnosticRegistration,
        results: Mapping[str, list[AgentRunResult]],
    ) -> tuple[PairedDecisionRun, ...]:
        return tuple(
            PairedDecisionRun(arm=arm, replicate_index=index, result=result)
            for arm in registration.paired_arms
            for index, result in enumerate(results[arm], start=1)
        )

    @staticmethod
    def _agreeing_judgments(
        manifest: DecisionRunManifest,
        paired_runs: tuple[PairedDecisionRun, ...],
    ) -> tuple[JudgmentArtifact, ...]:
        by_id = {
            result.judgment.artifact_id: result.judgment
            for item in paired_runs
            if (result := item.result).judgment is not None
        }
        return tuple(by_id[item] for item in manifest.agreeing_judgment_artifact_ids)

    def _now(self) -> datetime:
        value = self.clock()
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("prospective Decision pipeline clock must be timezone-aware")
        return value.astimezone(UTC)

    def __post_init__(self) -> None:
        if self.paper_service.mandate.environment is not TradingEnvironment.PAPER:
            raise PermissionError("prospective Decision pipeline is paper-only")
        if self.paper_service.mandate.approval_mode is not ApprovalMode.MANUAL_EACH:
            raise PermissionError("prospective Decision pipeline requires manual_each approval")
        if not isinstance(self.paper_service.provider, MockExecutionProvider):
            raise PermissionError("prospective Decision pipeline accepts the durable mock only")
