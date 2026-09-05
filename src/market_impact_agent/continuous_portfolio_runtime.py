"""One historical cutoff through signed research, scoped Recall and portfolio sizing.

The streaming engine owns account truth. Its completed prefix is projected here;
source records keep their actual observed times. This adapter never executes an
order or creates a replacement Run for interrupted unknown model work.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import ROUND_CEILING, Decimal
from typing import TYPE_CHECKING, cast
from zoneinfo import ZoneInfo

from market_impact_agent.agent_contracts import (
    EvidencePack,
    canonical_hash,
    evidence_pack_from_dict,
    pattern_pack_from_dict,
)
from market_impact_agent.authorized_decision_view import AuthorizedDecisionView
from market_impact_agent.checkpoint_market_universe import (
    ExchangeInstrumentRule,
    ExchangeInstrumentRuleSet,
)
from market_impact_agent.continuous_decision import ContinuousDecision, PendingReview, ReviewFrame
from market_impact_agent.data_inputs import (
    DataPITLane,
    FrozenDataSnapshotInput,
    LocalDataSnapshotStore,
)
from market_impact_agent.decision_recall import (
    DecisionRecallProjection,
    RecallProjectionEntry,
    decision_recall_tools,
)
from market_impact_agent.domain import ExecutableOrder, Side, TradingEnvironment, TradingMandateV3
from market_impact_agent.dynamic_ashare_admission import DynamicAShareAdmission
from market_impact_agent.frozen_research import FrozenResearchRepository
from market_impact_agent.historical_ashare_inputs import HistoricalAShareInputs
from market_impact_agent.model_provider import ModelProvider
from market_impact_agent.on_demand_research import (
    OnDemandResearch,
    ResearchContinuation,
    ResearchSourceTemplate,
)
from market_impact_agent.paper_execution import PriceBasis
from market_impact_agent.pi_execution import native_turn
from market_impact_agent.portfolio_decision import (
    PortfolioExposureViewV2,
    RawMarkedPositionV2,
    RegisteredPortfolioExposureViewAuthorityV2,
)
from market_impact_agent.portfolio_review import (
    PortfolioReviewAuthority,
    PortfolioReviewInputs,
    RotationReconciliationAuthority,
    RotationSourceCompletion,
)
from market_impact_agent.research_acquisition_runtime import (
    AcquisitionResearchResult,
    PreparedResearchSuccessor,
    analyze_with_acquisition,
    freeze_acquired_research,
)
from market_impact_agent.research_thesis_runtime import (
    ResearchThesisAuthority,
    ResearchThesisRunInputs,
    reopen_completed_research_thesis,
)
from market_impact_agent.runtime_store import RunJournal, RunStatus
from market_impact_agent.streaming_nautilus_account import (
    HistoricalSessionResult,
    HistoricalStreamingAccount,
)

if TYPE_CHECKING:
    from market_impact_agent.continuous_initial_adoption import InitialAdoptionAuthority

_SHANGHAI = ZoneInfo("Asia/Shanghai")


def continuous_frame_input_hash(
    repository: FrozenResearchRepository,
    market: HistoricalAShareInputs,
    snapshot_ids: tuple[str, ...],
) -> str:
    """No account state: cadence arms produce their own account prefix later."""
    return canonical_hash(
        {
            "research_pack_hash": canonical_hash(repository.evidence_pack.to_dict()),
            "market_snapshot_ids": sorted(market.snapshot_ids),
            "snapshot_ids": sorted(snapshot_ids),
            "rule_artifact_hashes": sorted(market.rule_artifact_hashes),
            **(
                {"fund_halt_artifact_hashes": sorted(market.fund_halt_artifact_hashes)}
                if market.fund_halt_artifact_hashes
                else {}
            ),
            "modeled_policy": market.policy.to_dict(),
        }
    )


def build_continuous_review_frame(
    *,
    repository: FrozenResearchRepository,
    market: HistoricalAShareInputs,
    new_fact_ids: tuple[str, ...] = (),
) -> ReviewFrame:
    snapshots = tuple(sorted(market.snapshot_ids))
    return ReviewFrame(
        repository.evidence_pack.as_of,
        snapshots,
        continuous_frame_input_hash(repository, market, snapshots),
        new_fact_ids,
    )


class _InputGap(ValueError):
    pass


class HistoricalRotationReconciliationAuthority:
    """Durable same-engine prefix evidence, independent of the latest fill window."""

    def __init__(
        self,
        account: HistoricalStreamingAccount,
        source_order: Callable[[str], ExecutableOrder],
        cutoff: datetime,
    ) -> None:
        self.account, self.source_order, self.cutoff = account, source_order, cutoff

    def reopen_source_completion(self, source_run_id: str) -> RotationSourceCompletion:
        order = self.source_order(source_run_id)
        prefix = tuple(
            item for item in self.account.results if item.account_state.as_of <= self.cutoff
        )
        if (
            not prefix
            or order.account_id != self.account.account_id
            or order.side is not Side.SELL
            or order.environment is not TradingEnvironment.BACKTEST
            or self.account._poisoned  # pyright: ignore[reportPrivateUsage]
            or any(
                item.account_state.account_reference_hash != order.account_id
                or not item.account_state.complete
                or item.account_state.missing_sections
                or item.account_state.reconciliation_gaps
                for item in prefix
            )
            or any(
                no_fill.order_id == order.client_order_id
                for item in prefix
                for no_fill in item.no_fills
            )
        ):
            raise PermissionError("rotation source lacks a complete authoritative engine prefix")
        fills = tuple(
            (item, fill)
            for item in prefix
            for fill in item.fills
            if fill.order_id == order.client_order_id
        )
        if (
            not fills
            or any(
                fill.target_id != order.instrument_id
                or fill.side is not Side.SELL
                or not order.created_at <= fill.filled_at < order.expires_at
                for _, fill in fills
            )
            or sum((fill.quantity for _, fill in fills), Decimal(0)) != order.quantity
        ):
            raise PermissionError("rotation source durable fill coverage is partial or unknown")
        completed_at = max(fill.filled_at for _, fill in fills)
        source_state = next(
            item.account_state for item, fill in reversed(fills) if fill.filled_at == completed_at
        )
        if any(item.target_id == order.instrument_id for item in source_state.positions or ()):
            raise PermissionError("rotation source did not close the entire held position")
        return RotationSourceCompletion(
            source_run_id,
            order.account_id,
            order.instrument_id,
            order.client_order_id,
            order.quantity,
            completed_at,
            source_state,
            prefix[-1].account_state.snapshot_id,
            canonical_hash([item.result_hash for item in prefix]),
        )


class ContinuousPortfolioRuntime:
    def __init__(
        self,
        *,
        store: LocalDataSnapshotStore,
        experiment_id: str,
        arm_id: str,
        account: HistoricalStreamingAccount,
        research_repository: Callable[[ReviewFrame], FrozenResearchRepository],
        market_inputs: Callable[[ReviewFrame], HistoricalAShareInputs],
        mandate_template: Callable[[ReviewFrame], TradingMandateV3],
        symbols: Callable[[ReviewFrame], tuple[str, ...]],
        account_max_age: Callable[[ReviewFrame], timedelta],
        provider: ModelProvider,
        portfolio_provider: ModelProvider | None = None,
        rotation_authority: RotationReconciliationAuthority | None = None,
        initial_adoption_authority: InitialAdoptionAuthority | None = None,
        historical_research_templates: tuple[ResearchSourceTemplate, ...] = (),
        research_episode_deadline: datetime | None = None,
        acquisition_clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        if not experiment_id or not arm_id:
            raise ValueError("continuous runtime needs registered experiment/arm scope")
        if provider.budget is None:
            raise ValueError("continuous model stages require one configured parent ModelBudget")
        self.store = store
        self.journal = RunJournal.authoritative(store)
        self.experiment_id, self.arm_id = experiment_id, arm_id
        self.account = account
        self.repository_source = research_repository
        self.market_source = market_inputs
        self.mandate_source = mandate_template
        self.symbol_source = symbols
        self.account_max_age = account_max_age
        self.provider = provider
        self.portfolio_provider = provider if portfolio_provider is None else portfolio_provider
        portfolio_budget = self.portfolio_provider.budget
        if (
            self.portfolio_provider.profile.to_dict() != provider.profile.to_dict()
            or self.portfolio_provider.runtime_identity != provider.runtime_identity
            or portfolio_budget is None
            or portfolio_budget.owner_run_id != provider.budget.owner_run_id
            or portfolio_budget.journal.path != provider.budget.journal.path
        ):
            raise PermissionError(
                "portfolio provider must retain exact profile and shared budget owner"
            )
        if historical_research_templates and research_episode_deadline is None:
            raise ValueError("historical acquisition requires a fixed episode deadline")
        self.historical_research_templates = historical_research_templates
        self.research_episode_deadline = research_episode_deadline
        self.acquisition_clock = acquisition_clock
        self.rotation_authority = rotation_authority
        self.initial_adoption_authority = initial_adoption_authority
        self.recall = DecisionRecallProjection(
            store.root / "decision-recall.sqlite3",
            artifact_store=store.artifacts,
            journal=self.journal,
        )

    def _with_sources(
        self,
        repository: FrozenResearchRepository,
        market: HistoricalAShareInputs,
        symbols: tuple[str, ...],
    ) -> ContinuousPortfolioRuntime:
        """A scoped view over the same authorities, never mutation of registered frames."""
        return ContinuousPortfolioRuntime(
            store=self.store,
            experiment_id=self.experiment_id,
            arm_id=self.arm_id,
            account=self.account,
            research_repository=lambda _: repository,
            market_inputs=lambda _: market,
            mandate_template=self.mandate_source,
            symbols=lambda _: symbols,
            account_max_age=self.account_max_age,
            provider=self.provider,
            portfolio_provider=self.portfolio_provider,
            rotation_authority=self.rotation_authority
            or HistoricalRotationReconciliationAuthority(
                self.account, self._rotation_source_order, repository.evidence_pack.as_of
            ),
            initial_adoption_authority=self.initial_adoption_authority,
            historical_research_templates=self.historical_research_templates,
            research_episode_deadline=self.research_episode_deadline,
            acquisition_clock=self.acquisition_clock,
        )

    async def _persist_successor(
        self,
        result: AcquisitionResearchResult,
        original: ReviewFrame,
        portfolio_id: str,
    ) -> str:
        pack = result.final_inputs.repository.evidence_pack
        documents = {
            ref.evidence_id: _object(
                await result.final_inputs.repository.read_evidence({"evidence_id": ref.evidence_id})
            )["document"]
            for ref in pack.evidence
        }
        value = {
            "schema_version": "market-impact.continuous-research-successor.v1",
            "experiment_id": self.experiment_id,
            "arm_id": self.arm_id,
            "account_id": self.account.account_id,
            "original_frame": original.to_dict(),
            "research_run_ids": list(result.run_ids),
            "portfolio_run_id": portfolio_id,
            "evidence_pack": pack.to_dict(),
            "documents": documents,
            "patterns": [
                await result.final_inputs.repository.read_pattern_pack({"pack_id": ref.pack_id})
                for ref in pack.pattern_packs
            ],
            "snapshots": sorted(result.frozen_input.authorized_snapshot_ids)
            if result.frozen_input
            else [],
            "acquisitions": [item.to_dict() for item in result.acquisitions],
        }
        ref = self.store.artifacts.put_json(value).content_hash
        budget = self.provider.budget
        assert budget is not None
        # Existing budget journal is the only continuation index; signed child packs
        # and durable source completions are reopened before this reference is used.
        event_id = budget.owner_run_id + ".continuous.successor." + canonical_hash(portfolio_id)
        prior = self.journal.event(event_id)
        if prior is not None and prior.payload != {"successor_ref": ref}:
            raise PermissionError("continuous successor identity changed")
        if prior is None:
            self.journal.append(
                run_id=budget.owner_run_id,
                event_id=event_id,
                event_type="continuous.research.successor",
                observed_at=original.cutoff,
                payload={"successor_ref": ref},
            )
        return ref

    def resolve_decision_context(
        self,
        decision: ContinuousDecision,
        frame: ReviewFrame,
    ) -> tuple[ContinuousPortfolioRuntime, ReviewFrame]:
        if decision.research_successor_ref is None:
            return self, frame
        original, market = self._frame_sources(frame)
        value = _object(self.store.artifacts.read_json(decision.research_successor_ref))
        if (
            value.get("experiment_id") != self.experiment_id
            or value.get("arm_id") != self.arm_id
            or value.get("account_id") != self.account.account_id
            or value.get("original_frame") != frame.to_dict()
            or value.get("portfolio_run_id") != decision.portfolio_run_id
        ):
            raise PermissionError("research successor crosses its original frame/account/arm")
        runs = cast(list[str], value["research_run_ids"])
        if not runs or runs[-1] != decision.research_run_id:
            raise PermissionError("research successor has no exact signed terminal")
        authority = ResearchThesisAuthority(
            self.store,
            experiment_id=self.experiment_id,
            arm_id=self.arm_id,
            account_scope=self.account.account_id,
            clock=lambda: frame.cutoff,
        )
        native_queries: list[tuple[str, str, dict[str, object]]] = []
        for run_id in runs:
            authority.replay(run_id)
            binding = _object(
                self.store.artifacts.read_json(self.journal.get_run(run_id).config_hash)
            )
            if (
                binding.get("experiment_id") != self.experiment_id
                or binding.get("arm_id") != self.arm_id
                or binding.get("account_scope") != self.account.account_id
            ):
                raise PermissionError("successor predecessor crosses research scope")
            for event in self.journal.events(run_id):
                if event.event_type != "pi.role.response.completed":
                    continue
                response = _object(
                    self.store.artifacts.read_json(str(event.payload["artifact_hash"]))
                )
                for call in native_turn(
                    response, str(_object(binding["profile"])["model"])
                ).tool_calls:
                    native_queries.append(
                        (
                            run_id,
                            call.name,
                            {
                                key: argument
                                for key, argument in call.arguments.items()
                                if key not in {"offset", "limit"}
                            },
                        )
                    )
        first = _object(self.store.artifacts.read_json(self.journal.get_run(runs[0]).config_hash))
        if _object(first["inputs"]).get("evidence_pack_hash") != canonical_hash(
            original.evidence_pack.to_dict()
        ):
            raise PermissionError("successor does not descend from registered evidence")
        pack = evidence_pack_from_dict(value["evidence_pack"])
        if pack.as_of != frame.cutoff or pack.event_id != original.evidence_pack.event_id:
            raise PermissionError("modeled successor changed historical cutoff or event")
        if any(ref not in pack.evidence for ref in original.evidence_pack.evidence):
            raise PermissionError("successor changed prior frozen evidence")
        final = self._research_scope(decision.research_run_id)
        if _object(final["inputs"]).get("evidence_pack_hash") != canonical_hash(pack.to_dict()):
            raise PermissionError("successor repository differs from signed thesis")
        budget = self.provider.budget
        assert budget is not None
        acquired: set[str] = set()
        executable_candidates = set(self.symbol_source(frame))
        expected_policy: dict[str, object] = {
            "policy": {key: str(item) for key, item in market.policy.to_dict().items()},
            "base_snapshot_ids": list(market.snapshot_ids),
            "rule_artifact_hashes": list(market.rule_artifact_hashes),
            **(
                {"fund_halt_artifact_hashes": list(market.fund_halt_artifact_hashes)}
                if market.fund_halt_artifact_hashes
                else {}
            ),
        }
        templates = {item.template_id: item for item in self.historical_research_templates}
        for receipt in cast(list[dict[str, object]], value["acquisitions"]):
            prefix = budget.owner_run_id + "." + str(receipt["request_id"])
            completed, requested = (
                self.journal.event(prefix + ".completed"),
                self.journal.event(prefix + ".requested"),
            )
            if completed is None or completed.payload != receipt or requested is None:
                raise PermissionError("successor lacks durable acquisition receipt")
            request_binding = _object(requested.payload["binding"])
            if (
                request_binding.get("run_id") not in runs
                or request_binding.get("cutoff") != frame.cutoff.isoformat()
                or request_binding.get("historical_policy") != expected_policy
                or request_binding.get("episode_id")
                != self.arm_id + ":" + runs[0].removesuffix(".research")
            ):
                raise PermissionError("successor acquisition differs from historical ancestry")
            template = templates.get(str(requested.payload["template_id"]))
            arguments = _object(requested.payload["parameters"])
            if template is None or market.research_query_gap(
                template.api_name, arguments, frame.cutoff
            ):
                raise PermissionError("successor acquisition route/window is not accepted")
            if receipt.get("snapshot_id") is not None:
                snapshot = self.store.get(str(receipt["snapshot_id"]))
                if (
                    snapshot.query.sources != (template.source,)
                    or snapshot.query.parameters != arguments
                ):
                    raise PermissionError(
                        "successor raw receipt differs from its exact source request"
                    )
                acquired.add(snapshot.snapshot_id)
                if (
                    receipt.get("status") == "fulfilled"
                    and requested.payload.get("origin") == "agent_tool"
                    and (str(request_binding["run_id"]), template.tool_name, arguments)
                    in native_queries
                    and str(arguments["ts_code"]) in pack.allowed_targets
                ):
                    # Research scopes can contain aggregate IDs. Only an exact signed
                    # native security query adds a new executable candidate.
                    executable_candidates.add(str(arguments["ts_code"]))
        snapshots = tuple(cast(list[str], value["snapshots"]))
        if set(snapshots) != set(frame.snapshot_ids) | acquired:
            raise PermissionError("successor source set contains unbound snapshots")
        repository = FrozenResearchRepository(
            evidence_pack=pack,
            evidence_documents=_object(value["documents"]),
            pattern_packs={
                item.pack_id: item
                for item in (
                    pattern_pack_from_dict(raw) for raw in cast(list[object], value["patterns"])
                )
            },
        )
        updated = market.with_snapshots(tuple(sorted(acquired)))
        effective = ReviewFrame(
            frame.cutoff, snapshots, continuous_frame_input_hash(repository, updated, snapshots)
        )
        runtime = self._with_sources(
            repository,
            updated,
            tuple(sorted(executable_candidates)),
        )
        return runtime, effective

    def source_market(
        self,
        decision: ContinuousDecision | None,
        frame: ReviewFrame,
    ) -> HistoricalAShareInputs:
        if decision is not None and decision.initial_adoption_ref is not None:
            self._reopen_initial(decision, frame)
            assert self.initial_adoption_authority is not None
            source = self.initial_adoption_authority.source_runtime
            return source.source_market(replace(decision, initial_adoption_ref=None), frame)
        runtime, effective = (
            (self, frame) if decision is None else self.resolve_decision_context(decision, frame)
        )
        return runtime._frame_sources(effective)[1]

    def adopt_initial(
        self, source_decision: ContinuousDecision, frame: ReviewFrame
    ) -> ContinuousDecision:
        if self.initial_adoption_authority is None:
            raise PermissionError("initial adoption requires registered Harness authority")
        return self.initial_adoption_authority.adopt(
            destination=self, source_decision=source_decision, frame=frame
        )

    def _reopen_initial(
        self, decision: ContinuousDecision, frame: ReviewFrame | None = None
    ) -> tuple[dict[str, object], ExecutableOrder | None]:
        if self.initial_adoption_authority is None or decision.initial_adoption_ref is None:
            raise PermissionError("initial adoption requires exact receipt authority")
        receipt, order = self.initial_adoption_authority.reopen(
            decision.initial_adoption_ref, self, frame=frame
        )
        original = ContinuousDecision.from_dict(_object(receipt["source_decision"]))
        if (
            decision.research_run_id,
            decision.portfolio_run_id,
            decision.horizon_sessions,
            decision.action,
            decision.decision_ref,
            decision.research_successor_ref,
        ) != (
            original.research_run_id,
            original.portfolio_run_id,
            original.horizon_sessions,
            original.action,
            original.decision_ref,
            original.research_successor_ref,
        ):
            raise PermissionError("initial adoption decision references differ from receipt")
        return receipt, order

    def _verify_adopted_prior(
        self, reference: str, run_id: str, target: str, cutoff: datetime
    ) -> dict[str, object]:
        if self.initial_adoption_authority is None:
            raise PermissionError("adopted prior has no registered authority")
        receipt, _ = self.initial_adoption_authority.reopen(reference, self)
        source = ContinuousDecision.from_dict(_object(receipt["source_decision"]))
        binding = _object(self.store.artifacts.read_json(self.journal.get_run(run_id).config_hash))
        thesis, _ = reopen_completed_research_thesis(
            journal=self.journal, artifact_store=self.store.artifacts, run_id=run_id
        )
        if (
            source.research_run_id != run_id
            or thesis.as_of >= cutoff
            or _object(binding["inputs"]).get("target_id") != target
        ):
            raise PermissionError("adopted prior differs from exact initial source")
        return {
            "receipt_ref": reference,
            "source_run_id": run_id,
            "source_terminal_hash": self.journal.get_run(run_id).terminal_artifact_id,
        }

    def _adopted_rotation_source(self, reference: str) -> dict[str, object]:
        if self.initial_adoption_authority is None:
            raise PermissionError("adopted rotation has no registered authority")
        receipt, _ = self.initial_adoption_authority.reopen(reference, self)
        return receipt

    def _frame_sources(
        self, frame: ReviewFrame
    ) -> tuple[FrozenResearchRepository, HistoricalAShareInputs]:
        repository, market = self.repository_source(frame), self.market_source(frame)
        if market.store.root.resolve() != self.store.root.resolve():
            raise PermissionError("continuous source is outside the Harness root")
        if (
            repository.evidence_pack.as_of != frame.cutoff
            or not set(market.snapshot_ids) <= set(frame.snapshot_ids)
            or continuous_frame_input_hash(repository, market, frame.snapshot_ids)
            != frame.input_hash
        ):
            raise PermissionError("continuous frame differs from frozen research/source policy")
        for snapshot_id in frame.snapshot_ids:
            self.store.get(snapshot_id)
        return repository, market

    def _account_prefix(self, frame: ReviewFrame) -> tuple[HistoricalSessionResult, ...]:
        prefix = tuple(
            item for item in self.account.results if item.account_state.as_of <= frame.cutoff
        )
        if not prefix:
            raise _InputGap("authoritative_historical_account_prefix_missing")
        if any(
            item.account_state.account_reference_hash != self.account.account_id for item in prefix
        ):
            raise PermissionError("streaming account prefix crosses account scope")
        return prefix

    def _portfolio_inputs(self, frame: ReviewFrame) -> PortfolioReviewInputs:
        _, market = self._frame_sources(frame)
        prefix = self._account_prefix(frame)
        state = prefix[-1].account_state
        mandate = self.mandate_source(frame)
        if (
            mandate.account_id != self.account.account_id
            or mandate.harness_authority_id != self.store.harness_authority_id
            or mandate.environment is not TradingEnvironment.BACKTEST
            or mandate.execution_scope != "historical_backtest"
            or state.environment is not TradingEnvironment.BACKTEST
        ):
            raise PermissionError(
                "continuous runtime requires exact same-root CNY backtest account"
            )
        max_age = self.account_max_age(frame)
        if max_age <= timedelta(0):
            raise ValueError("registered calendar account maximum age must be positive")
        if not state.readiness(evaluated_at=frame.cutoff, max_age=max_age).exposure_increase_ready:
            raise _InputGap("current_account_authority_incomplete_or_stale")
        held = tuple(item.target_id for item in state.positions or ())
        symbols = tuple(sorted(set(self.symbol_source(frame) + held)))
        admission = DynamicAShareAdmission(market)
        discovered = admission.discover(symbols, frame.cutoff)
        if not any(item.execution_ready for item in discovered):
            raise _InputGap(
                "security_evidence_incomplete:"
                + ",".join(sorted({gap for item in discovered for gap in item.gaps}))
            )
        bound = admission.bind(symbols, frame.cutoff, mandate)
        evidence = {item.symbol: item.evidence for item in bound.securities if item.execution_ready}
        if not set(held) <= evidence.keys():
            raise _InputGap("held_security_raw_mark_authority_missing")
        prices: dict[str, PriceBasis] = {}
        rules: dict[tuple[str, str], ExchangeInstrumentRule] = {}
        for symbol, item in evidence.items():
            assert item is not None and item.raw_price is not None
            assert item.buy_lot_size is not None and item.price_tick is not None
            if item.raw_price_observed_at is None or item.raw_price_observed_at > frame.cutoff:
                raise _InputGap("raw_price_observation_time_unverified")
            if item.raw_price_observed_at > state.as_of:
                raise _InputGap("account_prefix_missing_latest_completed_session")
            prices[symbol] = PriceBasis(
                symbol,
                "CNY",
                "per_share",
                "raw_reference_quote",
                item.raw_price,
                "modeled-historical-prior-close",
                canonical_hash(item.to_dict()),
                item.raw_price_observed_at,
                item.effective_until,
            )
            pair = item.venue, item.instrument_class
            rule = ExchangeInstrumentRule(
                "historical-" + "-".join(pair),
                item.venue,
                item.instrument_class,
                item.buy_lot_size,
                float(item.price_tick),
                "CNY",
                "ordinary_auction_buy_and_sell_order",
                (),
            )
            if pair in rules and rules[pair] != rule:
                raise _InputGap("venue_class_rule_variants_require_exact_sizing_support")
            rules[pair] = rule
        expiry = min(bound.mandate.valid_until, *(item.valid_until for item in prices.values()))
        position = state.project_positions(evaluated_at=frame.cutoff, max_age=max_age)
        view = AuthorizedDecisionView.build(
            cutoff=frame.cutoff,
            frozen_at=frame.cutoff,
            data_snapshot_ids=tuple(sorted(frame.snapshot_ids)),
            decision_input_ids=(),
            position_snapshot=position,
        )
        today = frame.cutoff.astimezone(_SHANGHAI).date()
        day_fills = tuple(
            fill
            for item in prefix
            for fill in item.fills
            if fill.filled_at.astimezone(_SHANGHAI).date() == today
        )
        peak = max(self.account.initial_cash, *(item.nav for item in prefix))
        start_nav = next(
            (
                item.nav
                for item in reversed(prefix)
                if item.account_state.as_of.astimezone(_SHANGHAI).date() < today
            ),
            self.account.initial_cash,
        )
        kills: list[str] = []
        if peak - prefix[-1].nav >= mandate.strategy_peak_drawdown_kill_threshold:
            kills.append("strategy_peak_drawdown_threshold_exceeded")
        if start_nav - prefix[-1].nav >= mandate.daily_loss_kill_threshold:
            kills.append("daily_loss_threshold_exceeded")
        exposure = PortfolioExposureViewV2.build(
            authorized_view=view,
            position_snapshot=position,
            raw_mark_set_hash=canonical_hash(
                {key: value.to_dict() for key, value in prices.items()}
            ),
            execution_ledger_snapshot_hash=canonical_hash([item.input_hash for item in prefix]),
            reconciliation_ledger_snapshot_hash=canonical_hash(
                [item.result_hash for item in prefix]
            ),
            currency="CNY",
            marked_positions=tuple(
                RawMarkedPositionV2(
                    item.target_id,
                    item.venue,
                    item.instrument_class,
                    item.side,
                    item.quantity,
                    prices[item.target_id].price,
                    canonical_hash(prices[item.target_id].to_dict()),
                )
                for item in state.positions or ()
            ),
            daily_turnover_used=sum((fill.quantity * fill.price for fill in day_fills), Decimal(0)),
            daily_submissions_used=len(
                {fill.order_id for fill in day_fills}
                | {
                    no_fill.order_id
                    for item in prefix
                    for no_fill in item.no_fills
                    if item.account_state.as_of.astimezone(_SHANGHAI).date() == today
                }
            ),
            active_kill_reasons=tuple(kills),
            observed_at=frame.cutoff,
            valid_until=expiry,
        )
        sources: tuple[dict[str, object], ...] = tuple(
            {
                "symbol": item.symbol,
                "source_record_hashes": list(item.evidence.source_record_hashes),
            }
            for item in bound.securities
            if item.execution_ready and item.evidence is not None
        )
        rule_set = ExchangeInstrumentRuleSet(
            "exchange-instrument-rule-set-"
            + canonical_hash(
                {"sources": sources, "rules": [item.to_dict() for item in rules.values()]}
            ),
            frame.cutoff.date(),
            sources,
            tuple(rules.values()),
        )
        return PortfolioReviewInputs(
            state, position, view, exposure, bound.mandate, prices, rule_set, frame.cutoff, expiry
        )

    def _portfolio_authority(self, frame: ReviewFrame) -> PortfolioReviewAuthority:
        inputs = self._portfolio_inputs(frame)
        exposure = inputs.exposure_view
        authority = PortfolioReviewAuthority(
            self.store,
            input_source=lambda: self._portfolio_inputs(frame),
            exposure_authority=RegisteredPortfolioExposureViewAuthorityV2(
                {exposure.exposure_view_id: exposure}
            ),
            clock=lambda: frame.cutoff,
            proposal_version="v5",
            rotation_authority=self.rotation_authority
            or HistoricalRotationReconciliationAuthority(
                self.account, self._rotation_source_order, frame.cutoff
            ),
            initial_rotation_source=self._adopted_rotation_source,
        )
        inputs.assert_complete(
            self.store.harness_authority_id, authority.exposure_authority, frame.cutoff
        )
        return authority

    def _rotation_source_order(self, source_run_id: str) -> ExecutableOrder:
        """Reopen exact native sizing or its registered adopted destination order."""
        record = self.journal.get_run(source_run_id)
        binding = _object(self.store.artifacts.read_json(record.config_hash))
        inputs = _object(binding["inputs"])
        if _object(inputs["mandate"]).get("account_id") != self.account.account_id:
            if self.initial_adoption_authority is None:
                raise PermissionError("rotation source order belongs to another account")
            return self.initial_adoption_authority.rotation_order(self, source_run_id)
        budget = self.provider.budget
        assert budget is not None
        successor = self.journal.event(
            budget.owner_run_id + ".continuous.successor." + canonical_hash(source_run_id)
        )
        if successor is not None:
            ref = str(successor.payload["successor_ref"])
            value = _object(self.store.artifacts.read_json(ref))
            raw_frame = _object(value["original_frame"])
            original_frame = ReviewFrame(
                datetime.fromisoformat(str(raw_frame["cutoff"])),
                tuple(cast(list[str], raw_frame["snapshot_ids"])),
                str(raw_frame["input_hash"]),
                tuple(cast(list[str], raw_frame["new_fact_ids"])),
                tuple(cast(list[str], raw_frame["gaps"])),
            )
            run_id = cast(list[str], value["research_run_ids"])[-1]
            thesis, _ = reopen_completed_research_thesis(
                journal=self.journal, artifact_store=self.store.artifacts, run_id=run_id
            )
            decision = ContinuousDecision(
                run_id,
                source_run_id,
                thesis.primary_horizon_sessions,
                "rotate",
                cast(str, record.terminal_artifact_id),
                research_successor_ref=ref,
            )
            runtime, effective = self.resolve_decision_context(decision, original_frame)
            return runtime._portfolio_authority(effective).execution_admission(source_run_id).order
        cutoff = datetime.fromisoformat(str(inputs["cutoff"]).replace("Z", "+00:00"))
        snapshots = tuple(cast(list[str], _object(inputs["authorized_view"])["data_snapshot_ids"]))
        provisional = ReviewFrame(cutoff, snapshots, "0" * 64)
        repository, market = self.repository_source(provisional), self.market_source(provisional)
        frame = ReviewFrame(
            cutoff, snapshots, continuous_frame_input_hash(repository, market, snapshots)
        )
        return self._portfolio_authority(frame).execution_admission(source_run_id).order

    def _research_scope(self, run_id: str, *, before: datetime | None = None) -> dict[str, object]:
        record = self.journal.get_run(run_id)
        binding = _object(self.store.artifacts.read_json(record.config_hash))
        if (
            binding.get("experiment_id") != self.experiment_id
            or binding.get("arm_id") != self.arm_id
            or binding.get("account_scope") != self.account.account_id
            or binding.get("profile") != self.provider.profile.to_dict()
        ):
            raise PermissionError("research recall crosses experiment, model arm or account scope")
        thesis, _ = reopen_completed_research_thesis(
            journal=self.journal, artifact_store=self.store.artifacts, run_id=run_id
        )
        if before is not None and thesis.as_of >= before:
            raise PermissionError("prior research is not earlier than the new cutoff")
        return binding

    def _recall_runs(self, frame: ReviewFrame, adopted_run_id: str | None = None) -> frozenset[str]:
        if self.initial_adoption_authority is not None:
            registered_initial = self.initial_adoption_authority.recall_source(self, frame.cutoff)
            if adopted_run_id is not None and adopted_run_id != registered_initial:
                raise PermissionError(
                    "Recall adopted source differs from registered initial receipt"
                )
            adopted_run_id = registered_initial
        allowed: set[str] = set()
        for record in self.journal.records(status=RunStatus.COMPLETED):
            if self.journal.event(record.run_id + ".research-thesis.terminal") is None:
                continue
            binding = _object(self.store.artifacts.read_json(record.config_hash))
            if (
                binding.get("schema_version") != "market-impact.research-thesis-binding.v1"
                or binding.get("experiment_id") != self.experiment_id
                or (
                    record.run_id != adopted_run_id
                    and (
                        binding.get("arm_id") != self.arm_id
                        or binding.get("account_scope") != self.account.account_id
                    )
                )
                or binding.get("profile") != self.provider.profile.to_dict()
            ):
                continue
            thesis, _ = reopen_completed_research_thesis(
                journal=self.journal, artifact_store=self.store.artifacts, run_id=record.run_id
            )
            if thesis.as_of >= frame.cutoff:
                continue
            terminal = _object(
                self.store.artifacts.read_json(cast(str, record.terminal_artifact_id))
            )
            target = str(_object(binding["inputs"])["target_id"])
            self.recall.add(
                RecallProjectionEntry(
                    thesis.root_event_id,
                    thesis.thesis_epoch,
                    "research_thesis",
                    record.run_id,
                    str(terminal["thesis_artifact_hash"]),
                    thesis.as_of,
                    (target,),
                    (),
                    f"research_thesis direction={thesis.base_case_direction.value} "
                    f"horizon_sessions={thesis.primary_horizon_sessions}",
                )
            )
            allowed.add(record.run_id)
        return frozenset(allowed)

    async def decide(
        self,
        frame: ReviewFrame,
        prior: ContinuousDecision | None,
        run_id: str,
        allowed_horizons: frozenset[int],
        resume: bool,
    ) -> ContinuousDecision | PendingReview:
        del (
            resume
        )  # Child authorities reopen their exact identities; unknown dispatch never retries.
        repository, _ = self._frame_sources(frame)
        research_id, portfolio_id = run_id + ".research", run_id + ".portfolio"
        prior_id = None
        if prior is not None:
            if prior.initial_adoption_ref is not None:
                self._reopen_initial(prior)
            else:
                self._research_scope(prior.research_run_id, before=frame.cutoff)
            prior_id = prior.research_run_id
        # Scope checks precede model work; tradability gaps do not erase research.
        mandate = self.mandate_source(frame)
        if (
            mandate.account_id != self.account.account_id
            or mandate.harness_authority_id != self.store.harness_authority_id
            or mandate.environment is not TradingEnvironment.BACKTEST
            or mandate.execution_scope != "historical_backtest"
        ):
            raise PermissionError(
                "continuous runtime requires exact same-root CNY backtest account"
            )
        if prior is not None and prior.initial_adoption_ref is None:
            # Replay verifies signed lineage without consulting today's execution inputs.
            replay_authority = PortfolioReviewAuthority(
                self.store,
                input_source=lambda: self._portfolio_inputs(frame),
                exposure_authority=RegisteredPortfolioExposureViewAuthorityV2({}),
                clock=lambda: frame.cutoff,
                proposal_version="v5",
            )
            old_terminal = replay_authority.replay(prior.portfolio_run_id)
            old_record = self.journal.get_run(prior.portfolio_run_id)
            old_binding = _object(self.store.artifacts.read_json(old_record.config_hash))
            old_refs = cast(list[dict[str, object]], old_binding.get("research_theses", []))
            if (
                old_record.terminal_artifact_id != prior.decision_ref
                or old_terminal.get("status") != "completed"
                or _object(old_terminal["proposal"]).get("requested_action") != prior.action
                or len(old_refs) != 1
                or old_refs[0].get("run_id") != prior.research_run_id
                or _object(_object(old_binding["inputs"])["mandate"]).get("account_id")
                != self.account.account_id
            ):
                raise PermissionError(
                    "prior continuous decision differs from signed account authority"
                )
        target = repository.evidence_pack.allowed_targets[0]
        if prior_id is not None:
            prior_binding = _object(
                self.store.artifacts.read_json(self.journal.get_run(prior_id).config_hash)
            )
            if _object(prior_binding["inputs"]).get("target_id") != target:
                # The exact prior remains available through scoped Recall; a different
                # target cannot be passed as a direct thesis update.
                prior_id = None
        research = ResearchThesisAuthority(
            self.store,
            experiment_id=self.experiment_id,
            arm_id=self.arm_id,
            account_scope=self.account.account_id,
            prior_adoption_validator=self._verify_adopted_prior,
            clock=lambda: frame.cutoff,
        )
        inputs = ResearchThesisRunInputs(
            repository, target, self.experiment_id + ":" + self.arm_id, allowed_horizons
        )
        allowed_runs = self._recall_runs(
            frame,
            prior_id if prior is not None and prior.initial_adoption_ref is not None else None,
        )
        tools = decision_recall_tools(
            self.recall,
            as_of=frame.cutoff,
            current_root_event_id=repository.evidence_pack.event_id,
            allowed_source_run_ids=allowed_runs,
        )
        for child_id, terminal_event in (
            (research_id, research_id + ".research-thesis.terminal"),
            (portfolio_id, portfolio_id + ".portfolio.terminal"),
        ):
            try:
                child = self.journal.get_run(child_id)
            except KeyError:
                continue
            if not child.status.terminal and self.journal.event(terminal_event) is None:
                return PendingReview("unknown_child_requires_reconciliation", child_id)
        successor_ref = None
        operation_runtime, operation_frame = self, frame
        if self.historical_research_templates:
            assert self.research_episode_deadline is not None and self.provider.budget is not None
            _, market = self._frame_sources(frame)
            acquisition = OnDemandResearch(
                store=self.store,
                parent_budget=self.provider.budget,
                episode_id=self.arm_id + ":" + run_id,
                episode_deadline=self.research_episode_deadline,
                run_id=research_id,
                cutoff=frame.cutoff,
                pit_lane=DataPITLane.MODELED,
                templates=self.historical_research_templates,
                frozen_input=FrozenDataSnapshotInput(frozenset(frame.snapshot_ids)),
                historical_inputs=market,
                clock=self.acquisition_clock,
            )

            async def successor(
                current_inputs: ResearchThesisRunInputs,
                current: OnDemandResearch,
                receipts: tuple[ResearchContinuation, ...],
            ) -> PreparedResearchSuccessor:
                research.replay(current.run_id)
                native_symbols: set[str] = set()
                for event in self.journal.events(current.run_id):
                    if event.event_type == "pi.role.response.completed":
                        raw = _object(
                            self.store.artifacts.read_json(str(event.payload["artifact_hash"]))
                        )
                        for call in native_turn(raw, self.provider.profile.model).tool_calls:
                            if call.name in {"lookup_stock_prices", "lookup_fund_prices"}:
                                native_symbols.add(str(call.arguments.get("ts_code", "")))
                candidates: set[str] = set()
                for receipt in receipts:
                    requested = self.journal.event(
                        current.budget.owner_run_id + "." + receipt.request_id + ".requested"
                    )
                    if requested is None or requested.payload.get("origin") != "agent_tool":
                        continue
                    symbol = str(_object(requested.payload["parameters"]).get("ts_code", ""))
                    if receipt.status == "fulfilled" and symbol in native_symbols:
                        candidates.add(symbol)
                updated_inputs, frozen = await freeze_acquired_research(
                    current_inputs, current, receipts
                )
                pack = updated_inputs.repository.evidence_pack
                documents = {
                    ref.evidence_id: _object(
                        await updated_inputs.repository.read_evidence(
                            {"evidence_id": ref.evidence_id}
                        )
                    )["document"]
                    for ref in pack.evidence
                }
                promoted = EvidencePack.build(
                    event_id=pack.event_id,
                    as_of=pack.as_of,
                    research_question=pack.research_question,
                    evidence=pack.evidence,
                    pattern_packs=pack.pattern_packs,
                    allowed_targets=tuple(sorted(set(pack.allowed_targets) | candidates)),
                    data_gaps=pack.data_gaps,
                )
                repo = FrozenResearchRepository(
                    evidence_pack=promoted,
                    evidence_documents=documents,
                    pattern_packs={
                        ref.pack_id: pattern_pack_from_dict(
                            await updated_inputs.repository.read_pattern_pack(
                                {"pack_id": ref.pack_id}
                            )
                        )
                        for ref in pack.pattern_packs
                    },
                )
                new = sorted(
                    candidates - set(current_inputs.repository.evidence_pack.allowed_targets)
                )
                return PreparedResearchSuccessor(
                    replace(
                        updated_inputs,
                        repository=repo,
                        target_id=new[0] if len(new) == 1 else updated_inputs.target_id,
                    ),
                    frozen,
                    receipts,
                    {
                        "policy": "historical-native-price-successor-v1",
                        "native_symbols": sorted(candidates),
                    },
                )

            acquired = await analyze_with_acquisition(
                authority=research,
                provider=self.provider,
                inputs=inputs,
                acquisition=acquisition,
                maximum_runs=3,
                prior_thesis_run_id=prior_id,
                prior_adoption_ref=None
                if prior is None or prior_id is None
                else prior.initial_adoption_ref,
                successor_transform=successor,
                successor_transform_id="historical-native-price-successor-v1",
                tool_factory=lambda _inputs, _run: tools,
            )
            terminal = acquired.terminal
            research_id = acquired.run_ids[-1]
            if len(acquired.run_ids) > 1 and terminal.get("status") == "completed":
                successor_ref = await self._persist_successor(acquired, frame, portfolio_id)
                temporary = ContinuousDecision(
                    research_id,
                    portfolio_id,
                    1,
                    "hold",
                    "pending",
                    research_successor_ref=successor_ref,
                )
                operation_runtime, operation_frame = self.resolve_decision_context(temporary, frame)
                new_target = acquired.final_inputs.target_id
                if new_target not in repository.evidence_pack.allowed_targets:
                    candidate = DynamicAShareAdmission(
                        operation_runtime.market_source(operation_frame)
                    ).discover((new_target,), frame.cutoff)[0]
                    if not candidate.execution_ready:
                        return PendingReview(
                            "dynamic_security_not_admitted:"
                            + new_target
                            + ":"
                            + ",".join(candidate.gaps),
                            research_id,
                        )
        else:
            terminal = await research.analyze(
                run_id=research_id,
                provider=self.provider,
                inputs=inputs,
                prior_thesis_run_id=prior_id,
                prior_adoption_ref=None
                if prior is None or prior_id is None
                else prior.initial_adoption_ref,
                readonly_tools=tools,
            )
        if terminal.get("status") != "completed":
            return PendingReview("research_run_incomplete", research_id)
        try:
            portfolio = operation_runtime._portfolio_authority(operation_frame)
        except _InputGap as gap:
            return PendingReview(str(gap), research_id)
        original_portfolio_id = portfolio_id
        portfolio_id = portfolio.projection_recovery_run_id(original_portfolio_id)
        review = await portfolio.review(
            run_id=portfolio_id,
            provider=self.portfolio_provider,
            research_run_ids=(),
            research_thesis_run_ids=(research_id,),
            projection_recovery_of=(
                original_portfolio_id if portfolio_id != original_portfolio_id else None
            ),
            rotation_source_adoption_ref=(
                prior.initial_adoption_ref
                if prior is not None and prior.action == "rotate"
                else None
            ),
            rotation_source_run_id=(
                prior.portfolio_run_id if prior is not None and prior.action == "rotate" else None
            ),
        )
        if review.get("status") != "completed":
            return PendingReview("portfolio_run_incomplete", portfolio_id)
        thesis, _ = reopen_completed_research_thesis(
            journal=self.journal, artifact_store=self.store.artifacts, run_id=research_id
        )
        decision = ContinuousDecision(
            research_id,
            portfolio_id,
            thesis.primary_horizon_sessions,
            str(_object(review["proposal"])["requested_action"]),
            cast(str, self.journal.get_run(portfolio_id).terminal_artifact_id),
            research_successor_ref=successor_ref,
        )
        if _object(review["decision"]).get("outcome") == "rejected":
            return PendingReview("portfolio_action_not_admitted", portfolio_id)
        self.validate_decision(decision, frame)
        if decision.action != "hold":
            try:
                execution = portfolio.execution_admission(portfolio_id)
            except PermissionError:
                return PendingReview("portfolio_sizing_not_admitted", portfolio_id)
            try:
                operation_runtime._assert_opening_buy_bounds(execution.order, operation_frame)
            except PermissionError:
                return PendingReview("opening_buy_bounds_not_admitted", portfolio_id)
        return decision

    def validate_decision(self, decision: ContinuousDecision, frame: ReviewFrame) -> None:
        if decision.initial_adoption_ref is not None:
            self._reopen_initial(decision, frame)
            return
        if decision.research_successor_ref is not None:
            runtime, effective = self.resolve_decision_context(decision, frame)
            runtime.validate_decision(replace(decision, research_successor_ref=None), effective)
            return
        if frame.gaps:
            raise PermissionError("continuous frame evidence is incomplete")
        repository, _ = self._frame_sources(frame)
        binding = self._research_scope(decision.research_run_id)
        if _object(binding["inputs"]).get("evidence_pack_hash") != canonical_hash(
            repository.evidence_pack.to_dict()
        ) or _object(binding["inputs"]).get("as_of") != frame.cutoff.isoformat().replace(
            "+00:00", "Z"
        ):
            raise PermissionError("continuous research does not bind the frozen frame")
        portfolio = self._portfolio_authority(frame)
        terminal = portfolio.replay(decision.portfolio_run_id)
        record = self.journal.get_run(decision.portfolio_run_id)
        portfolio_binding = _object(self.store.artifacts.read_json(record.config_hash))
        research_refs = cast(list[dict[str, object]], portfolio_binding.get("research_theses", []))
        if (
            terminal.get("status") != "completed"
            or record.terminal_artifact_id != decision.decision_ref
            or portfolio_binding.get("inputs") != portfolio.input_source().to_dict()
            or len(research_refs) != 1
            or research_refs[0].get("run_id") != decision.research_run_id
            or _object(terminal["proposal"]).get("requested_action") != decision.action
            or _object(terminal["decision"]).get("outcome") == "rejected"
        ):
            raise PermissionError(
                "continuous decision differs from signed current portfolio authority"
            )
        thesis, _ = reopen_completed_research_thesis(
            journal=self.journal,
            artifact_store=self.store.artifacts,
            run_id=decision.research_run_id,
        )
        if thesis.primary_horizon_sessions != decision.horizon_sessions:
            raise PermissionError("continuous horizon differs from signed research")

    def _assert_opening_buy_bounds(self, order: ExecutableOrder, frame: ReviewFrame) -> None:
        """Reject unsafe signed quantity at cutoff-known limits; never resize it."""
        if order.side is not Side.BUY:
            return
        inputs = self._portfolio_inputs(frame)
        _, market = self._frame_sources(frame)
        if order.account_id != inputs.account_state.account_reference_hash:
            raise PermissionError("opening BUY account differs from authoritative cash")
        spec = market.instrument_spec(order.instrument_id, frame.cutoff)
        target = market.reopen_security(order.instrument_id, frame.cutoff)
        if (
            spec is None
            or target is None
            or target.upper_limit is None
            or spec.source_ref.removeprefix("sha256:") not in target.source_record_hashes
        ):
            raise PermissionError("opening BUY lacks bound effective fee rules and upper limit")
        if (
            order.instrument_id in self.account.specs
            and self.account.specs[order.instrument_id] != spec
        ):
            raise PermissionError("opening BUY execution fee/instrument rules differ from source")
        balances = tuple(item for item in inputs.account_state.cash or () if item.currency == "CNY")
        if len(balances) != 1:
            raise PermissionError("opening BUY lacks one authoritative CNY cash balance")
        quantities = {
            item.target_id: item.quantity for item in inputs.account_state.positions or ()
        }
        if any(item.side is not Side.BUY for item in inputs.account_state.positions or ()):
            raise PermissionError("opening BUY bound supports long-only cash positions")
        quantities[order.instrument_id] = (
            quantities.get(order.instrument_id, Decimal(0)) + order.quantity
        )
        gross_ceiling = Decimal(0)
        net_floor = Decimal(0)
        mandate = inputs.mandate
        for symbol, quantity in quantities.items():
            evidence = market.reopen_security(symbol, frame.cutoff)
            basis = inputs.price_bases.get(symbol)
            if (
                evidence is None
                or evidence.gaps
                or evidence.upper_limit is None
                or evidence.lower_limit is None
                or basis is None
                or canonical_hash(evidence.to_dict()) != basis.source_version
            ):
                raise PermissionError("opening BUY market bounds differ from frozen source proof")
            notional_ceiling = quantity * evidence.upper_limit
            gross_ceiling += notional_ceiling
            net_floor += quantity * evidence.lower_limit
            if (
                notional_ceiling
                > mandate.maximum_single_position_fraction * mandate.gross_exposure_limit
            ):
                raise PermissionError("opening BUY upper limit exceeds single-position cap")
        buy_notional = order.quantity * target.upper_limit
        # The pinned engine uses one opening IOC fill and this same fee rule.
        # Round upward so cent rounding cannot make the cash check optimistic.
        fee_ceiling = max(spec.minimum_commission, buy_notional * spec.commission_rate).quantize(
            Decimal("0.01"),
            rounding=ROUND_CEILING,
        )
        if buy_notional + fee_ceiling > min(balances[0].available, balances[0].settled):
            raise PermissionError(
                "opening BUY upper limit plus fees exceeds available settled cash"
            )
        if gross_ceiling > min(mandate.gross_exposure_limit, mandate.maximum_net_exposure):
            raise PermissionError("opening BUY upper limits exceed gross or net exposure cap")
        if net_floor < mandate.minimum_net_exposure:
            raise PermissionError("opening BUY lower limits breach the minimum net exposure")
        if inputs.exposure_view.daily_turnover_used + buy_notional > mandate.daily_turnover_limit:
            raise PermissionError("opening BUY upper limit exceeds daily turnover cap")

    def admitted_intents(
        self, decision: ContinuousDecision, frame: ReviewFrame
    ) -> tuple[ExecutableOrder, ...]:
        if decision.initial_adoption_ref is not None:
            _, adopted_order = self._reopen_initial(decision, frame)
            return () if adopted_order is None else (adopted_order,)
        if decision.research_successor_ref is not None:
            runtime, effective = self.resolve_decision_context(decision, frame)
            return runtime.admitted_intents(
                replace(decision, research_successor_ref=None), effective
            )
        self.validate_decision(decision, frame)
        if decision.action == "hold":
            return ()
        admission = self._portfolio_authority(frame).execution_admission(decision.portfolio_run_id)
        self._assert_opening_buy_bounds(admission.order, frame)
        return (admission.order,)


def _object(value: object) -> dict[str, object]:
    if not isinstance(value, dict):
        raise ValueError("expected frozen object")
    return cast(dict[str, object], value)
