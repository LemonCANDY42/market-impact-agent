"""Account-aware pi review and same-root execution ancestry, additive to v1/v2.

The configured input source is the Harness composition boundary. Model output
contains recommendations only; account identity, provenance and quantities are
injected or computed by the Harness. No broker or data retrieval occurs here.
"""

from __future__ import annotations

import asyncio
import hmac
import json
from collections.abc import Callable, Mapping
from dataclasses import dataclass, fields, replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from hashlib import sha256
from pathlib import Path
from typing import Protocol, cast

from market_impact_agent.account_state import AccountStateSnapshot, PositionSnapshot
from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.agent_engine import (
    RunMetrics,
    _PrivilegedEventSink,  # pyright: ignore[reportPrivateUsage]
    reopen_authoritative_agent_terminal,
)
from market_impact_agent.authorized_decision_view import AuthorizedDecisionView
from market_impact_agent.checkpoint_market_universe import ExchangeInstrumentRuleSet
from market_impact_agent.data_inputs import LocalDataSnapshotStore
from market_impact_agent.decision_thesis import HorizonBand, validate_horizon
from market_impact_agent.domain import OrderKind, PortfolioOrderIntent, Side, TradingMandateV2
from market_impact_agent.model_json import load_model_json
from market_impact_agent.model_provider import ModelProvider
from market_impact_agent.paper_execution import PriceBasis
from market_impact_agent.pi_execution import (
    PiInvocationContext,
    PiRoleJournal,
    execute_pi_once,
    native_turn,
)
from market_impact_agent.portfolio_decision import (
    OrderSizingDecisionV2,
    OrderSizingOutcome,
    PortfolioAction,
    PortfolioDecisionLegV2,
    PortfolioDecisionOutcome,
    PortfolioExposureViewAuthorityV2,
    PortfolioExposureViewV2,
    PortfolioLegRole,
    TargetExposureDirection,
    size_portfolio_decision_v2,
)
from market_impact_agent.provider_reliability import ProviderAttemptEvent
from market_impact_agent.research_thesis_runtime import reopen_completed_research_thesis
from market_impact_agent.runtime_store import RunJournal, RunStatus
from market_impact_agent.usage_ledger import UsageLedger, UsageRecord

PORTFOLIO_REVIEW_PROMPT = """Recommend an action for the entire supplied account.
Research is evidence, not an order side. Positive research can justify reducing
concentrated holdings. Uncertain research still requires a reasoned recommendation.
Return one JSON object with requested_action (hold/open/increase/reduce/close/rotate),
rationale, horizon_band (immediate/tactical/swing), primary_horizon_sessions
(immediate: 1 or 3; tactical: 5 or 10; swing: 20 or 60), priced_in_assessment,
transmission (nonempty strings), counter_scenario, review_after_sessions (positive and
not beyond the primary horizon), evidence_refs (input evidence IDs),
counterevidence_refs (optional IDs), and invalidation_conditions (nonempty strings).
A frozen evidence item may appear in both evidence lists when the same fact supports
competing interpretations; explain the competing use in the rationale or counter-scenario.
hold means maintain the entire observed account including cash: explain why changing
exposure is less appropriate and when to review. For hold omit all target fields.
For other actions also provide instrument_id, venue, instrument_class, direction
(long/short), target_gross_exposure_ratio (0..1 of mandate.gross_exposure_limit,
not NAV; close must be zero). Never supply quantity, Run/proposal/approval IDs or hashes.
No abstain or standalone observe. Rotation and unproved short openings may be
recommended but are blocked at execution until their independent gates are accepted.
"""


PORTFOLIO_PROMPT_PROJECTION_VERSION = "market-impact.portfolio-prompt-projection.v1"
PORTFOLIO_EVIDENCE_SCOPE_VERSION = "market-impact.portfolio-evidence-scope.v2"
PORTFOLIO_REVIEW_PROMPT_V2 = PORTFOLIO_REVIEW_PROMPT.replace(
    "Never supply quantity, Run/proposal/approval IDs or hashes.",
    "Never supply quantity or invent Run/proposal/approval IDs or hashes. "
    "For evidence_refs and counterevidence_refs, select only from the supplied "
    "evidence_ids list, including its research Run IDs and bound thesis evidence IDs.",
)


def portfolio_prompt_projection(binding: Mapping[str, object]) -> dict[str, object]:
    """Derive model context; only explicit record-hash provenance is compacted.

    Full inputs remain the validation authority. The artifact hash and JSON pointer
    locate the original list for offline validator/replay reopening, not a model tool.
    """
    full_inputs = _object(binding["inputs"])
    inputs_hash = canonical_hash(full_inputs)
    inputs = dict(full_inputs)
    rule_set = dict(_object(inputs["rule_set"]))
    sources: list[dict[str, object]] = []
    for index, raw in enumerate(cast(list[object], rule_set["source_documents"])):
        source = dict(_object(raw))
        if "source_record_hashes" in source:
            if "source_record_hashes_provenance" in source:
                raise ValueError("portfolio source has conflicting provenance projection")
            hashes = source.pop("source_record_hashes")
            if not isinstance(hashes, list):
                raise ValueError("portfolio rule provenance must be a list of record hashes")
            hashes = cast(list[object], hashes)
            if any(not isinstance(item, str) for item in hashes):
                raise ValueError("portfolio rule provenance must be a list of record hashes")
            source["source_record_hashes_provenance"] = {
                "content_hash": canonical_hash(hashes),
                "count": len(hashes),
                "inputs_artifact_hash": inputs_hash,
                "json_pointer": f"/rule_set/source_documents/{index}/source_record_hashes",
            }
        sources.append(source)
    rule_set["source_documents"] = sources
    inputs["rule_set"] = rule_set
    return {
        "schema_version": PORTFOLIO_PROMPT_PROJECTION_VERSION,
        "inputs": inputs,
        "research": binding["research"],
        "research_theses": binding["research_theses"],
        "evidence_ids": sorted(
            PortfolioReviewAuthority._evidence_ids(dict(binding))  # pyright: ignore[reportPrivateUsage]
        ),
        **(
            {"rotation_completion": binding["rotation_completion"]}
            if "rotation_completion" in binding
            else {}
        ),
    }


@dataclass(frozen=True, slots=True)
class PortfolioReviewInputs:
    account_state: AccountStateSnapshot
    position_snapshot: PositionSnapshot
    authorized_view: AuthorizedDecisionView
    exposure_view: PortfolioExposureViewV2
    mandate: TradingMandateV2
    price_bases: Mapping[str, PriceBasis]
    rule_set: ExchangeInstrumentRuleSet
    cutoff: datetime
    expires_at: datetime

    def to_dict(self) -> dict[str, object]:
        return {
            "account_state": self.account_state.to_dict(),
            "position_snapshot": self.position_snapshot.to_dict(),
            "authorized_view": self.authorized_view.to_dict(),
            "exposure_view": self.exposure_view.to_dict(),
            "mandate": self.mandate.to_dict(),
            "price_bases": {
                key: value.to_dict() for key, value in sorted(self.price_bases.items())
            },
            "rule_set": self.rule_set.to_dict(),
            "cutoff": _timestamp(self.cutoff),
            "expires_at": _timestamp(self.expires_at),
        }

    def assert_complete(
        self,
        authority_id: str,
        exposure_authority: PortfolioExposureViewAuthorityV2,
        evaluated_at: datetime,
    ) -> None:
        exposure_authority.assert_authoritative_exposure_view(self.exposure_view)
        position = self.position_snapshot
        if not self.cutoff <= evaluated_at < self.expires_at:
            raise PermissionError("portfolio review input is not current")
        if self.mandate.harness_authority_id != authority_id:
            raise PermissionError("portfolio review mandate belongs to another root")
        if not self.mandate.valid_from <= evaluated_at < self.mandate.valid_until:
            raise PermissionError("portfolio review mandate is not current")
        if (
            self.account_state.account_reference_hash != self.mandate.account_id
            or self.account_state.environment is not self.mandate.environment
        ):
            raise PermissionError("portfolio review account differs from mandate")
        expected_position = self.account_state.project_positions(
            evaluated_at=position.evaluated_at,
            max_age=timedelta(seconds=position.max_age_seconds),
        )
        expected_view = AuthorizedDecisionView.build(
            cutoff=self.cutoff,
            frozen_at=self.authorized_view.frozen_at,
            data_snapshot_ids=self.authorized_view.data_snapshot_ids,
            decision_input_ids=self.authorized_view.decision_input_ids,
            position_snapshot=position,
        )
        if expected_position != position or expected_view != self.authorized_view:
            raise PermissionError("portfolio review projection differs from account authority")
        if (
            self.account_state.cash is None
            or self.account_state.positions is None
            or self.account_state.open_orders is None
            or self.account_state.recent_fills is None
            or self.authorized_view.observation_gaps
            or not self.account_state.readiness(
                evaluated_at=evaluated_at, max_age=timedelta(seconds=position.max_age_seconds)
            ).exposure_increase_ready
        ):
            raise PermissionError("incomplete account authority cannot complete a portfolio review")
        exposure = self.exposure_view
        if (
            exposure.authorized_decision_view_id != self.authorized_view.view_id
            or exposure.authorized_decision_view_hash
            != canonical_hash(self.authorized_view.to_dict())
            or exposure.position_snapshot_id != position.snapshot_id
            or exposure.position_snapshot_hash != canonical_hash(position.to_dict())
            or not exposure.observed_at <= evaluated_at < exposure.valid_until
        ):
            raise PermissionError("portfolio exposure does not bind the exact current review")


@dataclass(frozen=True, slots=True)
class AgentPortfolioProposalV3:
    review_binding_hash: str
    requested_action: PortfolioAction
    rationale: str
    horizon_sessions: int
    evidence_refs: tuple[str, ...]
    counterevidence_refs: tuple[str, ...]
    invalidation_conditions: tuple[str, ...]
    instrument_id: str | None = None
    venue: str | None = None
    instrument_class: str | None = None
    direction: TargetExposureDirection | None = None
    target_gross_exposure_ratio: Decimal | None = None

    @property
    def proposal_id(self) -> str:
        return "agent-portfolio-proposal-v3-" + canonical_hash(self.core_dict())

    def core_dict(self) -> dict[str, object]:
        return {
            "schema_version": "market-impact.agent-portfolio-proposal.v3",
            "review_binding_hash": self.review_binding_hash,
            "requested_action": self.requested_action.value,
            "rationale": self.rationale,
            "horizon_sessions": self.horizon_sessions,
            "evidence_refs": list(self.evidence_refs),
            "counterevidence_refs": list(self.counterevidence_refs),
            "invalidation_conditions": list(self.invalidation_conditions),
            "instrument_id": self.instrument_id,
            "venue": self.venue,
            "instrument_class": self.instrument_class,
            "direction": None if self.direction is None else self.direction.value,
            "target_gross_exposure_ratio": (
                None
                if self.target_gross_exposure_ratio is None
                else format(self.target_gross_exposure_ratio, "f")
            ),
        }

    def to_dict(self) -> dict[str, object]:
        return {**self.core_dict(), "proposal_id": self.proposal_id}


@dataclass(frozen=True, slots=True)
class AgentPortfolioProposalV4:
    """Dynamic-horizon proposal for new Runs; v3 remains available for replay."""

    review_binding_hash: str
    requested_action: PortfolioAction
    rationale: str
    horizon_band: HorizonBand
    primary_horizon_sessions: int
    priced_in_assessment: str
    transmission: tuple[str, ...]
    counter_scenario: str
    review_after_sessions: int
    evidence_refs: tuple[str, ...]
    counterevidence_refs: tuple[str, ...]
    invalidation_conditions: tuple[str, ...]
    instrument_id: str | None = None
    venue: str | None = None
    instrument_class: str | None = None
    direction: TargetExposureDirection | None = None
    target_gross_exposure_ratio: Decimal | None = None

    @property
    def horizon_sessions(self) -> int:
        """Compatibility surface for deterministic sizing and execution policy."""

        return self.primary_horizon_sessions

    @property
    def proposal_id(self) -> str:
        return "agent-portfolio-proposal-v4-" + canonical_hash(self.core_dict())

    def core_dict(self) -> dict[str, object]:
        return {
            "schema_version": "market-impact.agent-portfolio-proposal.v4",
            "review_binding_hash": self.review_binding_hash,
            "requested_action": self.requested_action.value,
            "rationale": self.rationale,
            "horizon_band": self.horizon_band.value,
            "primary_horizon_sessions": self.primary_horizon_sessions,
            "priced_in_assessment": self.priced_in_assessment,
            "transmission": list(self.transmission),
            "counter_scenario": self.counter_scenario,
            "review_after_sessions": self.review_after_sessions,
            "evidence_refs": list(self.evidence_refs),
            "counterevidence_refs": list(self.counterevidence_refs),
            "invalidation_conditions": list(self.invalidation_conditions),
            "instrument_id": self.instrument_id,
            "venue": self.venue,
            "instrument_class": self.instrument_class,
            "direction": None if self.direction is None else self.direction.value,
            "target_gross_exposure_ratio": (
                None
                if self.target_gross_exposure_ratio is None
                else format(self.target_gross_exposure_ratio, "f")
            ),
        }

    def to_dict(self) -> dict[str, object]:
        return {**self.core_dict(), "proposal_id": self.proposal_id}


@dataclass(frozen=True, slots=True, kw_only=True)
class AgentPortfolioProposalV5(AgentPortfolioProposalV4):
    """Explicit long-only source-close rotation; destination needs a later decision."""

    rotation_source_instrument_id: str | None = None

    @property
    def proposal_id(self) -> str:
        return "agent-portfolio-proposal-v5-" + canonical_hash(self.core_dict())

    def core_dict(self) -> dict[str, object]:
        return {
            **AgentPortfolioProposalV4.core_dict(self),
            "schema_version": "market-impact.agent-portfolio-proposal.v5",
            "rotation_source_instrument_id": self.rotation_source_instrument_id,
        }


def parse_portfolio_proposal_v5(
    value: object, *, binding_hash: str, evidence_ids: frozenset[str]
) -> AgentPortfolioProposalV5:
    payload = dict(_object(value))
    source = payload.pop("rotation_source_instrument_id", None)
    base = parse_portfolio_proposal_v4(
        payload, binding_hash=binding_hash, evidence_ids=evidence_ids
    )
    if base.requested_action is PortfolioAction.ROTATE:
        if not isinstance(source, str) or not source or source != source.strip():
            raise ValueError("rotation requires an explicit source instrument")
        if source == base.instrument_id or base.direction is not TargetExposureDirection.LONG:
            raise ValueError("rotation requires distinct long-only source and destination")
    elif source is not None:
        raise ValueError("only rotation may name a source")
    return AgentPortfolioProposalV5(
        **{field.name: getattr(base, field.name) for field in fields(base)},
        rotation_source_instrument_id=source,
    )


def parse_portfolio_proposal(
    value: object,
    *,
    binding_hash: str,
    evidence_ids: frozenset[str],
) -> AgentPortfolioProposalV3:
    fields = _object(value)
    common = {
        "requested_action",
        "rationale",
        "horizon_sessions",
        "evidence_refs",
        "counterevidence_refs",
        "invalidation_conditions",
    }
    target = {
        "instrument_id",
        "venue",
        "instrument_class",
        "direction",
        "target_gross_exposure_ratio",
    }
    if set(fields) - common - target:
        raise ValueError("portfolio proposal contains unauthorized fields")
    action = PortfolioAction(_string(fields, "requested_action"))
    if action in {PortfolioAction.ABSTAIN, PortfolioAction.OBSERVE}:
        raise ValueError("completed portfolio recommendations cannot abstain or observe")
    horizon = fields.get("horizon_sessions")
    if type(horizon) is not int or horizon <= 0:
        raise ValueError("portfolio horizon must be a positive integer")
    evidence = _strings(fields.get("evidence_refs"))
    counter = _strings(fields.get("counterevidence_refs", []))
    invalidation = _strings(fields.get("invalidation_conditions"))
    if not evidence or not invalidation or not set(evidence + counter) <= evidence_ids:
        raise ValueError("portfolio proposal requires bound evidence and invalidation")
    if set(evidence) & set(counter):
        raise ValueError("portfolio evidence and counterevidence overlap")
    instrument = venue = instrument_class = None
    direction = None
    ratio = None
    if action is PortfolioAction.HOLD:
        if any(fields.get(name) is not None for name in target):
            raise ValueError("whole-account hold must not invent a target")
    else:
        instrument = _string(fields, "instrument_id")
        venue = _string(fields, "venue")
        instrument_class = _string(fields, "instrument_class")
        direction = TargetExposureDirection(_string(fields, "direction"))
        ratio = Decimal(str(fields.get("target_gross_exposure_ratio")))
        if not ratio.is_finite() or not 0 <= ratio <= 1:
            raise ValueError("portfolio target ratio must be finite and in [0, 1]")
        if (action is PortfolioAction.CLOSE) != (ratio == 0):
            raise ValueError("only close may propose a zero target")
    return AgentPortfolioProposalV3(
        binding_hash,
        action,
        _string(fields, "rationale"),
        horizon,
        evidence,
        counter,
        invalidation,
        instrument,
        venue,
        instrument_class,
        direction,
        ratio,
    )


def parse_portfolio_proposal_v4(
    value: object,
    *,
    binding_hash: str,
    evidence_ids: frozenset[str],
) -> AgentPortfolioProposalV4:
    fields = _object(value)
    common = {
        "requested_action",
        "rationale",
        "horizon_band",
        "primary_horizon_sessions",
        "priced_in_assessment",
        "transmission",
        "counter_scenario",
        "review_after_sessions",
        "evidence_refs",
        "counterevidence_refs",
        "invalidation_conditions",
    }
    target = {
        "instrument_id",
        "venue",
        "instrument_class",
        "direction",
        "target_gross_exposure_ratio",
    }
    if set(fields) - common - target:
        raise ValueError("portfolio proposal contains unauthorized fields")
    action = PortfolioAction(_string(fields, "requested_action"))
    if action in {PortfolioAction.ABSTAIN, PortfolioAction.OBSERVE}:
        raise ValueError("completed portfolio recommendations cannot abstain or observe")
    band = HorizonBand(_string(fields, "horizon_band"))
    horizon = fields.get("primary_horizon_sessions")
    if type(horizon) is not int:
        raise ValueError("portfolio primary horizon must be an integer")
    validate_horizon(band, horizon)
    review_after = fields.get("review_after_sessions")
    if type(review_after) is not int or not 1 <= review_after <= horizon:
        raise ValueError("portfolio review point must fit the primary horizon")
    evidence = _strings(fields.get("evidence_refs"), require_trimmed=True)
    counter = _strings(fields.get("counterevidence_refs", []), require_trimmed=True)
    invalidation = _strings(
        fields.get("invalidation_conditions"), preserve_order=True, trim_items=True
    )
    transmission = _strings(fields.get("transmission"), preserve_order=True, trim_items=True)
    if (
        not evidence
        or not invalidation
        or not transmission
        or not set(evidence + counter) <= evidence_ids
    ):
        raise ValueError(
            "portfolio proposal requires bound evidence, transmission, and invalidation"
        )
    instrument = venue = instrument_class = None
    direction = None
    ratio = None
    if action is PortfolioAction.HOLD:
        if any(fields.get(name) is not None for name in target):
            raise ValueError("whole-account hold must not invent a target")
    else:
        instrument = _string(fields, "instrument_id")
        venue = _string(fields, "venue")
        instrument_class = _string(fields, "instrument_class")
        direction = TargetExposureDirection(_string(fields, "direction"))
        ratio = Decimal(str(fields.get("target_gross_exposure_ratio")))
        if not ratio.is_finite() or not 0 <= ratio <= 1:
            raise ValueError("portfolio target ratio must be finite and in [0, 1]")
        if (action is PortfolioAction.CLOSE) != (ratio == 0):
            raise ValueError("only close may propose a zero target")
    return AgentPortfolioProposalV4(
        review_binding_hash=binding_hash,
        requested_action=action,
        rationale=_narrative_string(fields, "rationale"),
        horizon_band=band,
        primary_horizon_sessions=horizon,
        priced_in_assessment=_narrative_string(fields, "priced_in_assessment"),
        transmission=transmission,
        counter_scenario=_narrative_string(fields, "counter_scenario"),
        review_after_sessions=review_after,
        evidence_refs=evidence,
        counterevidence_refs=counter,
        invalidation_conditions=invalidation,
        instrument_id=instrument,
        venue=venue,
        instrument_class=instrument_class,
        direction=direction,
        target_gross_exposure_ratio=ratio,
    )


def portfolio_proposal_text_normalizations(value: object) -> tuple[dict[str, str], ...]:
    """Describe bounded narrative whitespace normalization without exposing content."""

    fields = _object(value)
    edits: list[dict[str, str]] = []
    for name in ("rationale", "priced_in_assessment", "counter_scenario"):
        item = fields.get(name)
        if isinstance(item, str) and item != item.strip():
            edits.append({"path": name, "operation": "trim_surrounding_whitespace"})
    for name in ("transmission", "invalidation_conditions"):
        item = fields.get(name)
        if not isinstance(item, list):
            continue
        for index, entry in enumerate(cast(list[object], item)):
            if isinstance(entry, str) and entry != entry.strip():
                edits.append(
                    {
                        "path": f"{name}[{index}]",
                        "operation": "trim_surrounding_whitespace",
                    }
                )
    return tuple(edits)


@dataclass(frozen=True, slots=True)
class PortfolioDecisionV3:
    proposal: AgentPortfolioProposalV3 | AgentPortfolioProposalV4
    authorized_decision_view_id: str
    authorized_decision_view_hash: str
    position_snapshot_id: str
    position_snapshot_hash: str
    legs: tuple[PortfolioDecisionLegV2, ...]
    outcome: PortfolioDecisionOutcome
    blockers: tuple[str, ...]
    decided_at: datetime
    bearish_expression_binding: None = None

    @property
    def decision_id(self) -> str:
        return "portfolio-decision-v3-" + canonical_hash(self.core_dict())

    def core_dict(self) -> dict[str, object]:
        return {
            "schema_version": "market-impact.portfolio-decision.v3",
            "proposal": self.proposal.to_dict(),
            "authorized_decision_view_id": self.authorized_decision_view_id,
            "authorized_decision_view_hash": self.authorized_decision_view_hash,
            "position_snapshot_id": self.position_snapshot_id,
            "position_snapshot_hash": self.position_snapshot_hash,
            "legs": [leg.to_dict() for leg in self.legs],
            "outcome": self.outcome.value,
            "blockers": list(self.blockers),
            "decided_at": _timestamp(self.decided_at),
            "execution_capability": False,
        }

    def to_dict(self) -> dict[str, object]:
        return {**self.core_dict(), "decision_id": self.decision_id}


def evaluate_portfolio_decision_v3(
    proposal: AgentPortfolioProposalV3 | AgentPortfolioProposalV4,
    inputs: PortfolioReviewInputs,
    *,
    decided_at: datetime,
) -> PortfolioDecisionV3:
    if (
        isinstance(proposal, AgentPortfolioProposalV5)
        and proposal.requested_action is PortfolioAction.ROTATE
    ):
        source = next(
            (
                item
                for item in inputs.position_snapshot.positions or ()
                if item.target_id == proposal.rotation_source_instrument_id
            ),
            None,
        )
        if source is not None:
            close = replace(
                proposal,
                requested_action=PortfolioAction.CLOSE,
                instrument_id=source.target_id,
                venue=source.venue,
                instrument_class=source.instrument_class,
                target_gross_exposure_ratio=Decimal(0),
                rotation_source_instrument_id=None,
            )
            decision = evaluate_portfolio_decision_v3(close, inputs, decided_at=decided_at)
            return replace(
                decision,
                proposal=proposal,
                legs=tuple(
                    replace(
                        leg, role=PortfolioLegRole.ROTATION_SOURCE, physical_target_side=Side.SELL
                    )
                    for leg in decision.legs
                ),
            )
    blockers: set[str] = set()
    legs: tuple[PortfolioDecisionLegV2, ...] = ()
    action = proposal.requested_action
    if action is not PortfolioAction.HOLD:
        assert proposal.instrument_id is not None and proposal.venue is not None
        assert proposal.instrument_class is not None and proposal.direction is not None
        assert proposal.target_gross_exposure_ratio is not None
        positions = inputs.position_snapshot.positions or ()
        current = next(
            (item for item in positions if item.target_id == proposal.instrument_id), None
        )
        if action is PortfolioAction.ROTATE:
            blockers.add("rotation_requires_source_reconciliation_acceptance")
        if action is PortfolioAction.OPEN and current is not None:
            blockers.add("open_requires_no_existing_position")
        if (
            action in {PortfolioAction.INCREASE, PortfolioAction.REDUCE, PortfolioAction.CLOSE}
            and current is None
        ):
            blockers.add("action_requires_existing_position")
        if current is not None and (
            current.venue != proposal.venue or current.instrument_class != proposal.instrument_class
        ):
            blockers.add("target_identity_differs_from_position")
        if proposal.direction is TargetExposureDirection.SHORT:
            blockers.add("bearish_expression_authority_not_accepted_for_v3")
        if any(
            item.target_id == proposal.instrument_id
            for item in inputs.account_state.open_orders or ()
        ):
            blockers.add("target_has_conflicting_open_order")
        if current is not None and current.side is not Side.BUY:
            blockers.add("short_position_review_execution_not_accepted")
        if not blockers:
            legs = (
                PortfolioDecisionLegV2(
                    role=PortfolioLegRole.PRIMARY,
                    action=action,
                    instrument_id=proposal.instrument_id,
                    venue=proposal.venue,
                    instrument_class=proposal.instrument_class,
                    direction=proposal.direction,
                    target_gross_exposure_ratio=proposal.target_gross_exposure_ratio,
                    current_side=None if current is None else current.side,
                    current_quantity=Decimal(0) if current is None else current.quantity,
                    current_concentration=None if current is None else current.concentration,
                    current_concentration_gap=None
                    if current is None
                    else current.concentration_gap,
                    position_snapshot_position_hash=None
                    if current is None
                    else canonical_hash(current.to_dict()),
                    physical_target_side=Side.BUY,
                    gate=None,
                ),
            )
    outcome = (
        PortfolioDecisionOutcome.REJECTED
        if blockers
        else PortfolioDecisionOutcome.NO_ACTION
        if action is PortfolioAction.HOLD
        else PortfolioDecisionOutcome.READY_FOR_SIZING
    )
    return PortfolioDecisionV3(
        proposal,
        inputs.authorized_view.view_id,
        canonical_hash(inputs.authorized_view.to_dict()),
        inputs.position_snapshot.snapshot_id,
        canonical_hash(inputs.position_snapshot.to_dict()),
        legs,
        outcome,
        tuple(sorted(blockers)),
        decided_at,
    )


@dataclass(frozen=True, slots=True)
class PortfolioExecutionAdmissionV3:
    run_id: str
    terminal_hash: str
    binding_hash: str
    portfolio_decision: PortfolioDecisionV3
    sizing_decision: OrderSizingDecisionV2
    order: PortfolioOrderIntent

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": "market-impact.portfolio-execution-admission.v3",
            "run_id": self.run_id,
            "terminal_hash": self.terminal_hash,
            "binding_hash": self.binding_hash,
            "portfolio_decision": self.portfolio_decision.to_dict(),
            "sizing_decision": self.sizing_decision.to_dict(),
            "order": self.order.to_dict(),
        }


@dataclass(frozen=True, slots=True)
class RotationSourceCompletion:
    source_run_id: str
    account_reference_hash: str
    source_instrument_id: str
    source_order_reference: str
    source_quantity: Decimal
    completed_at: datetime
    reconciled_source_account: AccountStateSnapshot | None = None
    current_account_snapshot_id: str | None = None
    account_prefix_hash: str | None = None


class RotationReconciliationAuthority(Protocol):
    def reopen_source_completion(self, source_run_id: str) -> RotationSourceCompletion:
        """Reopen authoritative fully-filled source order and complete reconciliation."""
        ...


class PortfolioReviewAuthority:
    """A same-root producer; its sources are composed by the Harness, not the model."""

    def __init__(
        self,
        store: LocalDataSnapshotStore,
        *,
        input_source: Callable[[], PortfolioReviewInputs],
        exposure_authority: PortfolioExposureViewAuthorityV2,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        proposal_version: str = "v4",
        rotation_authority: RotationReconciliationAuthority | None = None,
        initial_rotation_source: Callable[[str], dict[str, object]] | None = None,
    ) -> None:
        if proposal_version not in {"v4", "v5"}:
            raise ValueError("unsupported portfolio producer version")
        self.proposal_version = proposal_version
        self.rotation_authority = rotation_authority
        self.initial_rotation_source = initial_rotation_source
        self.store = store
        self.journal = RunJournal.authoritative(store)
        self.input_source = input_source
        self.exposure_authority = exposure_authority
        self.clock = clock
        self.usage_ledger = UsageLedger(store.index_path)
        key = (store.root / ".harness-event-hmac.key").read_bytes()
        self._events = _PrivilegedEventSink(
            journal=self.journal,
            authority_id=store.harness_authority_id,
            signer=lambda value: hmac.new(key, value, sha256).hexdigest(),
        )

    def _rotation_completion(
        self, source_run_id: str, inputs: PortfolioReviewInputs, adoption_ref: str | None = None
    ) -> dict[str, object]:
        if self.rotation_authority is None:
            raise PermissionError("rotation requires source reconciliation authority")
        terminal = self.replay(source_run_id)
        source_binding = _object(
            self.store.artifacts.read_json(self.journal.get_run(source_run_id).config_hash)
        )
        adopted_order: dict[str, object] | None = None
        if adoption_ref is not None:
            if self.initial_rotation_source is None:
                raise PermissionError("adopted rotation requires registered receipt authority")
            receipt = self.initial_rotation_source(adoption_ref)
            if _object(receipt["source_decision"]).get("portfolio_run_id") != source_run_id:
                raise PermissionError("adopted rotation receipt belongs to another source")
            source_binding = {"inputs": receipt["inputs"]}
            terminal = {**terminal, "decision": receipt["decision"]}
            adopted_order = _object(receipt["order"])
        proposal = _object(terminal.get("proposal"))
        previous_account = _object(_object(source_binding["inputs"])["account_state"])
        source_id = proposal.get("rotation_source_instrument_id")
        completion = self.rotation_authority.reopen_source_completion(source_run_id)
        if (
            terminal.get("status") != "completed"
            or proposal.get("schema_version") != "market-impact.agent-portfolio-proposal.v5"
            or proposal.get("requested_action") != "rotate"
            or (
                adopted_order is not None
                and (
                    completion.source_order_reference != adopted_order.get("client_order_id")
                    or completion.source_quantity != Decimal(_string(adopted_order, "quantity"))
                )
            )
            or completion.source_run_id != source_run_id
            or completion.source_instrument_id != source_id
            or completion.account_reference_hash != inputs.account_state.account_reference_hash
            or previous_account.get("account_reference_hash") != completion.account_reference_hash
            or inputs.account_state.snapshot_id == previous_account.get("snapshot_id")
            or not datetime.fromisoformat(_string(terminal, "completed_at").replace("Z", "+00:00"))
            <= completion.completed_at
            < inputs.cutoff
            or completion.completed_at > inputs.account_state.as_of
            or any(item.target_id == source_id for item in inputs.account_state.positions or ())
            or any(item.target_id == source_id for item in inputs.account_state.open_orders or ())
        ):
            raise PermissionError("rotation source lacks exact fresh account reconciliation")
        legs = cast(list[dict[str, object]], _object(terminal["decision"])["legs"])
        if (
            len(legs) != 1
            or Decimal(_string(legs[0], "current_quantity")) != completion.source_quantity
        ):
            raise PermissionError("rotation source completion differs from full-close decision")
        fill_account = inputs.account_state
        prefix_proof: dict[str, object] = {}
        if completion.reconciled_source_account is not None:
            fill_account = completion.reconciled_source_account
            if (
                completion.current_account_snapshot_id != inputs.account_state.snapshot_id
                or completion.account_prefix_hash is None
                or len(completion.account_prefix_hash) != 64
                or fill_account.account_reference_hash != completion.account_reference_hash
                or fill_account.environment is not inputs.account_state.environment
                or not fill_account.complete
                or fill_account.missing_sections
                or fill_account.reconciliation_gaps
                or not completion.completed_at <= fill_account.as_of <= inputs.account_state.as_of
                or fill_account.snapshot_id == previous_account.get("snapshot_id")
                or any(item.target_id == source_id for item in fill_account.positions or ())
                or any(item.target_id == source_id for item in fill_account.open_orders or ())
            ):
                raise PermissionError(
                    "rotation historical fill receipt lacks exact reconciled prefix"
                )
            prefix_proof = {
                "reconciled_source_snapshot_hash": canonical_hash(fill_account.to_dict()),
                "account_prefix_hash": completion.account_prefix_hash,
            }
        elif (
            completion.current_account_snapshot_id is not None
            or completion.account_prefix_hash is not None
        ):
            raise PermissionError("rotation historical fill receipt is incomplete")
        filled = sum(
            (
                item.quantity
                for item in fill_account.recent_fills or ()
                if item.order_reference == completion.source_order_reference
                and item.target_id == source_id
                and item.side is Side.SELL
            ),
            Decimal(0),
        )
        if filled != completion.source_quantity or filled <= 0:
            raise PermissionError("rotation source fill coverage is partial or unknown")
        return {
            "source_run_id": source_run_id,
            **({"initial_adoption_ref": adoption_ref} if adoption_ref else {}),
            "source_terminal_hash": self.journal.get_run(source_run_id).terminal_artifact_id,
            "source_order_reference": completion.source_order_reference,
            "completed_at": completion.completed_at.isoformat(),
            "account_snapshot_id": inputs.account_state.snapshot_id,
            **prefix_proof,
        }

    async def review_after_rotation(
        self, *, run_id: str, source_run_id: str, provider: ModelProvider
    ) -> dict[str, object]:
        if self.proposal_version != "v5" or run_id == source_run_id:
            raise PermissionError("rotation destination requires a fresh v5 model Run")
        return await self.review(
            run_id=run_id,
            provider=provider,
            research_run_ids=(),
            rotation_source_run_id=source_run_id,
        )

    @staticmethod
    def _parse_bound_proposal(binding: Mapping[str, object]):
        return (
            parse_portfolio_proposal_v5
            if binding.get("schema_version") == "market-impact.portfolio-review-binding.v5"
            else parse_portfolio_proposal_v4
        )

    def _research(self, run_ids: tuple[str, ...], cutoff: datetime) -> list[dict[str, object]]:
        research: list[dict[str, object]] = []
        if len(set(run_ids)) != len(run_ids):
            raise ValueError("research runs must be unique")
        for run_id in run_ids:
            record = self.journal.get_run(run_id)
            events = self.journal.events(run_id)
            terminal_hash = record.terminal_artifact_id
            if (
                record.status is not RunStatus.COMPLETED
                or terminal_hash is None
                or record.harness_authority_id != self.store.harness_authority_id
                or record.updated_at > cutoff
            ):
                raise PermissionError("research must be a completed same-root pre-cutoff run")
            reopened = reopen_authoritative_agent_terminal(
                journal=self.journal,
                artifact_store=self.store.artifacts,
                run_id=run_id,
                status=record.status,
                finished_at=record.updated_at,
                terminal_artifact_hash=terminal_hash,
            )
            if reopened is None:
                raise PermissionError("research has no completed authoritative Judgment")
            judgment = reopened.to_dict()
            validation = next(
                (event for event in events if event.event_type == "judgment.validated"), None
            )
            if (
                validation is None
                or judgment.get("journal_hash") != validation.event_hash
                or canonical_hash(judgment.get("proposal"))
                != validation.payload.get("proposal_hash")
            ):
                raise PermissionError("research has no authoritative completed Judgment")
            research.append(
                {
                    "run_id": run_id,
                    "terminal_hash": terminal_hash,
                    "judgment": judgment,
                    "journal_hash": self.journal.journal_hash(run_id),
                }
            )
        return research

    def _research_theses(
        self, run_ids: tuple[str, ...], cutoff: datetime
    ) -> list[dict[str, object]]:
        theses: list[dict[str, object]] = []
        if len(set(run_ids)) != len(run_ids):
            raise ValueError("research thesis runs must be unique")
        for run_id in run_ids:
            record = self.journal.get_run(run_id)
            if (
                record.harness_authority_id != self.store.harness_authority_id
                or record.updated_at > cutoff
            ):
                raise PermissionError(
                    "research thesis must be a same-root pre-cutoff completed Run"
                )
            thesis, reopened = reopen_completed_research_thesis(
                journal=self.journal,
                artifact_store=self.store.artifacts,
                run_id=run_id,
            )
            if thesis.as_of > cutoff:
                raise PermissionError("research thesis evidence is after the portfolio cutoff")
            theses.append(reopened)
        return theses

    async def review_account(self, *, run_id: str, provider: ModelProvider) -> dict[str, object]:
        return await self.review(
            run_id=run_id,
            provider=provider,
            research_run_ids=(),
            research_thesis_run_ids=(),
        )

    def projection_recovery_run_id(self, original_run_id: str) -> str:
        """Allow one projection repair only for a proven undispatched legacy attempt."""
        try:
            record = self.journal.get_run(original_run_id)
        except KeyError:
            return original_run_id
        if record.status is not RunStatus.FAILED:
            return original_run_id
        terminal = self.replay(original_run_id)
        binding = _object(self.store.artifacts.read_json(record.config_hash))
        if terminal.get("reason") != "_BudgetExceeded" or "prompt_projection" in binding:
            return original_run_id
        self._projection_recovery_source(original_run_id)
        return original_run_id + ".projection-recovery"

    def _projection_recovery_source(
        self, original_run_id: str
    ) -> tuple[dict[str, object], dict[str, object]]:
        terminal = self.replay(original_run_id)
        record = self.journal.get_run(original_run_id)
        binding = _object(self.store.artifacts.read_json(record.config_hash))
        allowed_events = {
            "portfolio.review.frozen",
            "pi.role.history.initial",
            "pi.context.frozen",
            "portfolio.review.incomplete",
        }
        if (
            record.status is not RunStatus.FAILED
            or terminal.get("reason") != "_BudgetExceeded"
            or "prompt_projection" in binding
            or "projection_recovery" in binding
            or any(
                event.event_type not in allowed_events
                for event in self.journal.events(original_run_id)
            )
        ):
            raise PermissionError("projection recovery requires an undispatched legacy failure")
        owner = _object(binding["budget_owner"])
        if Path(_string(owner, "journal_path")) != self.journal.path:
            raise PermissionError("projection recovery requires the same budget journal")
        if any(
            event.event_type == "pi.budget.reserved"
            and str(event.payload.get("request_key", "")).startswith(
                original_run_id + ".pi-invocation."
            )
            for event in self.journal.events(_string(owner, "run_id"))
        ):
            raise PermissionError("projection recovery cannot retry an admitted request")
        return binding, {
            "run_id": original_run_id,
            "binding_hash": record.config_hash,
            "terminal_hash": record.terminal_artifact_id,
            "physical_dispatches": 0,
        }

    def _verify_projection_recovery(self, binding: dict[str, object]) -> None:
        reference = _object(binding["projection_recovery"])
        original_id = _string(reference, "run_id")
        old, expected = self._projection_recovery_source(original_id)
        comparable = {
            k: v
            for k, v in binding.items()
            if k
            not in {
                "run_id",
                "prompt_projection",
                "projection_recovery",
            }
        }
        if (
            reference != expected
            or binding.get("run_id") != original_id + ".projection-recovery"
            or comparable != {k: v for k, v in old.items() if k != "run_id"}
        ):
            raise PermissionError("projection recovery changed its original authority or budget")

    async def review(
        self,
        *,
        run_id: str,
        provider: ModelProvider,
        research_run_ids: tuple[str, ...],
        research_thesis_run_ids: tuple[str, ...] = (),
        max_output_tokens: int | None = None,
        rotation_source_run_id: str | None = None,
        rotation_source_adoption_ref: str | None = None,
        projection_recovery_of: str | None = None,
    ) -> dict[str, object]:
        output_limit = (
            provider.profile.reserved_output_tokens
            if max_output_tokens is None
            else max_output_tokens
        )
        if not 16 <= output_limit <= provider.profile.reserved_output_tokens:
            raise ValueError("portfolio output limit exceeds the accepted model Profile")
        claim = self.journal.try_claim_run(run_id)
        if claim is None:
            raise RuntimeError("portfolio review run already has an owner")
        with claim:
            try:
                previous = self.journal.get_run(run_id)
            except KeyError:
                previous = None
            if previous is not None:
                if previous.status.terminal:
                    self._record_usage(run_id)
                    return self.replay(run_id)
                terminal_event = self.journal.event(f"{run_id}.portfolio.terminal")
                if terminal_event is not None:
                    terminal_hash = _string(terminal_event.payload, "terminal_hash")
                    self.store.artifacts.read_json(terminal_hash)
                    status = (
                        RunStatus(_string(terminal_event.payload, "run_status"))
                        if "run_status" in terminal_event.payload
                        else (
                            RunStatus.COMPLETED
                            if terminal_event.event_type == "portfolio.review.validated"
                            else RunStatus.FAILED
                        )
                    )
                    self.journal.finish(
                        run_id=run_id,
                        status=status,
                        finished_at=terminal_event.observed_at,
                        terminal_artifact_id=terminal_hash,
                    )
                    self._record_usage(run_id)
                    return self.replay(run_id)
                # An interrupted physical attempt is never silently regenerated.
                raise PermissionError(
                    "interrupted portfolio review requires reconciliation, not regeneration"
                )
            inputs = self.input_source()
            inputs.assert_complete(
                self.store.harness_authority_id, self.exposure_authority, self.clock()
            )
            rotation_completion = (
                None
                if rotation_source_run_id is None
                else self._rotation_completion(
                    rotation_source_run_id, inputs, rotation_source_adoption_ref
                )
            )
            research = self._research(research_run_ids, inputs.cutoff)
            research_theses = self._research_theses(research_thesis_run_ids, inputs.cutoff)
            binding: dict[str, object] = {
                "schema_version": "market-impact.portfolio-review-binding." + self.proposal_version,
                "harness_authority_id": self.store.harness_authority_id,
                "run_id": run_id,
                "inputs": inputs.to_dict(),
                "research": research,
                "research_theses": research_theses,
                "profile": provider.profile.to_dict(),
                "runtime": provider.runtime_identity,
                "prompt": PORTFOLIO_REVIEW_PROMPT
                + (
                    " For v5 rotate, instrument_id is the proposed destination; provide "
                    "rotation_source_instrument_id explicitly. Only the full source close "
                    "can execute now. Destination requires reconciliation and a fresh "
                    "model review which may choose cash. Rotation is long-only."
                    if self.proposal_version == "v5"
                    else ""
                ),
                "budget_owner": {
                    "journal_path": str(
                        self.journal.path
                        if provider.budget is None
                        else provider.budget.journal.path
                    ),
                    "run_id": run_id if provider.budget is None else provider.budget.owner_run_id,
                    "binding": None if provider.budget is None else provider.budget.binding,
                },
            }
            if projection_recovery_of is None:
                binding["evidence_scope_version"] = PORTFOLIO_EVIDENCE_SCOPE_VERSION
                binding["prompt"] = cast(str, binding["prompt"]).replace(
                    PORTFOLIO_REVIEW_PROMPT, PORTFOLIO_REVIEW_PROMPT_V2, 1
                )
            if rotation_completion is not None:
                binding["rotation_completion"] = rotation_completion
            if projection_recovery_of is not None:
                _, reference = self._projection_recovery_source(projection_recovery_of)
                binding["projection_recovery"] = reference
                self._verify_projection_recovery(binding)
            self.store.artifacts.put_json(binding["inputs"])
            binding["prompt_projection"] = portfolio_prompt_projection(binding)
            binding_hash = self.store.artifacts.put_json(binding).content_hash
            self.journal.start_run(run_id=run_id, config_hash=binding_hash, created_at=self.clock())
            self._events.append(
                run_id=run_id,
                event_id=f"{run_id}.portfolio.frozen",
                event_type="portfolio.review.frozen",
                observed_at=self.clock(),
                payload={"binding_hash": binding_hash},
            )
            evidence_ids = self._evidence_ids(binding)
            invocation_journal = cast(PiRoleJournal, PiRoleJournal.authoritative(self.store))
            invocation_journal.bind(run_id=run_id, writer=self._events)
            cancellation: asyncio.CancelledError | None = None
            try:
                turn = await execute_pi_once(
                    provider,
                    context=PiInvocationContext(
                        run_id, 1, invocation_journal, self.store.artifacts, self.clock
                    ),
                    messages=(
                        {"role": "system", "content": binding["prompt"]},
                        {
                            "role": "user",
                            "content": json.dumps(
                                binding["prompt_projection"],
                                sort_keys=True,
                            ),
                        },
                    ),
                    max_output_tokens=output_limit,
                    timeout_seconds=provider.profile.budget.max_wall_seconds,
                    attempt_observer=lambda event: self._observe_attempt(run_id, event),
                )
                content = _string(turn.assistant_message, "content")
                parsed = load_model_json(content)
                proposal = self._parse_bound_proposal(binding)(
                    parsed.value, binding_hash=binding_hash, evidence_ids=evidence_ids
                )
                inputs.assert_complete(
                    self.store.harness_authority_id, self.exposure_authority, self.clock()
                )
                if self.input_source().to_dict() != binding["inputs"]:
                    raise PermissionError("authoritative account changed during portfolio review")
                decision = evaluate_portfolio_decision_v3(proposal, inputs, decided_at=self.clock())
                payload = {
                    "schema_version": "market-impact.portfolio-review-terminal."
                    + self.proposal_version,
                    "run_id": run_id,
                    "status": "completed",
                    "binding_hash": binding_hash,
                    "proposal": proposal.to_dict(),
                    "proposal_artifact_hash": self.store.artifacts.put_json(
                        proposal.to_dict()
                    ).content_hash,
                    "decision": decision.to_dict(),
                    "raw_response_hash": self.store.artifacts.put_json(
                        turn.raw_response
                    ).content_hash,
                    "parsed_proposal": parsed.value,
                    "parse_evidence": parsed.evidence.to_dict(),
                    "text_normalizations": list(
                        portfolio_proposal_text_normalizations(parsed.value)
                    ),
                    "usage": turn.usage.to_dict(),
                    "completed_at": _timestamp(decision.decided_at),
                }
                status = RunStatus.COMPLETED
            except (Exception, asyncio.CancelledError) as error:
                if isinstance(error, asyncio.CancelledError):
                    cancellation = error
                payload = {
                    "schema_version": "market-impact.portfolio-review-terminal."
                    + self.proposal_version,
                    "run_id": run_id,
                    "status": "incomplete",
                    "binding_hash": binding_hash,
                    "reason": type(error).__name__,
                    "completed_at": _timestamp(self.clock()),
                }
                status = RunStatus.CANCELLED if cancellation is not None else RunStatus.FAILED
            # On cancellation pi has already joined its child. Close terminal and
            # usage synchronously before re-raising; unknown reservations stay open.
            artifact = self.store.artifacts.put_json(payload)
            self._events.append(
                run_id=run_id,
                event_id=f"{run_id}.portfolio.terminal",
                event_type="portfolio.review.validated"
                if status is RunStatus.COMPLETED
                else "portfolio.review.incomplete",
                observed_at=self.clock(),
                payload={
                    "terminal_hash": artifact.content_hash,
                    "binding_hash": binding_hash,
                    "journal_hash": self.journal.journal_hash(run_id),
                    "run_status": status.value,
                },
            )
            self.journal.finish(
                run_id=run_id,
                status=status,
                finished_at=self.clock(),
                terminal_artifact_id=artifact.content_hash,
            )
            self._record_usage(run_id)
            if cancellation is not None:
                raise cancellation
            return self.replay(run_id)

    def _observe_attempt(self, run_id: str, event: ProviderAttemptEvent) -> None:
        self._events.append(
            run_id=run_id,
            event_id=f"{run_id}.portfolio.attempt.{event.physical_attempt}.{event.phase.value}",
            event_type="portfolio.model.attempt",
            observed_at=self.clock(),
            payload={
                "request_id": event.request_id,
                "attempt": event.physical_attempt,
                "phase": event.phase.value,
                "latency_ms": event.elapsed_latency_ms,
                "failure": None if event.failure is None else event.failure.safe_fields(),
            },
        )

    def _usage_record(self, run_id: str) -> UsageRecord:
        record = self.journal.get_run(run_id)
        binding = _object(self.store.artifacts.read_json(record.config_hash))
        profile = _object(binding["profile"])
        owner = _object(binding["budget_owner"])
        budget_path = _string(owner, "journal_path")
        budget_journal = (
            self.journal if budget_path == str(self.journal.path) else RunJournal(Path(budget_path))
        )
        events = budget_journal.events(_string(owner, "run_id"))
        reserved: dict[str, int] = {}
        settled: dict[str, int] = {}
        for event in events:
            key = event.payload.get("request_key")
            if not isinstance(key, str) or not key.startswith(f"{run_id}.pi-invocation."):
                continue
            if event.event_type == "pi.budget.reserved":
                reserved[key] = cast(int, event.payload["reserved_microusd"])
            elif event.event_type == "pi.budget.settled":
                settled[key] = cast(int, event.payload["estimated_cost_microusd"])
        if not settled.keys() <= reserved.keys():
            raise PermissionError("portfolio usage settlement lacks its physical reservation")
        turns = input_tokens = output_tokens = attempts = 0
        latency = 0.0
        for event in self.journal.events(run_id):
            if event.event_type == "portfolio.model.attempt":
                attempts += int(event.payload["phase"] == "dispatched")
                if event.payload["phase"] != "dispatched":
                    latency += float(cast(float, event.payload["latency_ms"]))
            elif event.event_type == "pi.role.response.completed":
                raw = _object(
                    self.store.artifacts.read_json(_string(event.payload, "artifact_hash"))
                )
                turn = native_turn(raw, _string(profile, "model"))
                turns += 1
                input_tokens += turn.usage.input_tokens
                output_tokens += turn.usage.output_tokens
        # Unknown generations retain their existing conservative reservation;
        # aggregate estimated usage must not describe them as free requests.
        estimated_cost = sum(settled.values()) + sum(
            cost for key, cost in reserved.items() if key not in settled
        )
        metrics = RunMetrics(
            turns, 0, input_tokens, output_tokens, 0, latency, attempts, estimated_cost
        )
        return UsageRecord(
            experiment_id=(
                "portfolio-review-v5"
                if binding.get("schema_version") == "market-impact.portfolio-review-binding.v5"
                else "portfolio-review-v4"
                if binding.get("schema_version")
                in {
                    "market-impact.portfolio-review-binding.v4",
                    "market-impact.portfolio-review-binding.v5",
                }
                else "portfolio-review-v3"
            ),
            arm_id="portfolio",
            run_id=run_id,
            recorded_at=record.updated_at,
            status=record.status,
            provider_profile_id=_string(profile, "profile_id"),
            provider_profile_hash=canonical_hash(profile),
            execution_binding_hash=record.config_hash,
            terminal_artifact_hash=record.terminal_artifact_id,
            run_journal_hash=self.journal.journal_hash(run_id),
            metrics=metrics,
        )

    def _record_usage(self, run_id: str) -> None:
        self.usage_ledger.append(self._usage_record(run_id))

    @staticmethod
    def _evidence_ids(binding: dict[str, object]) -> frozenset[str]:
        inputs = _object(binding["inputs"])
        refs = {"account_state", "position_snapshot", "authorized_view", "exposure_view", "mandate"}
        refs.update(_object(inputs["price_bases"]))
        refs.update(
            _string(_object(item), "run_id") for item in cast(list[object], binding["research"])
        )
        refs.update(
            _string(_object(item), "run_id")
            for item in cast(list[object], binding.get("research_theses", []))
        )
        scope = binding.get("evidence_scope_version")
        if scope is not None:
            if scope != PORTFOLIO_EVIDENCE_SCOPE_VERSION:
                raise PermissionError("unsupported portfolio evidence scope")
            for item in cast(list[object], binding.get("research_theses", [])):
                thesis = _object(_object(item)["thesis"])
                for field in ("evidence_refs", "counterevidence_refs"):
                    refs.update(_strings(thesis.get(field, []), require_trimmed=True))
        return frozenset(refs)

    def replay(self, run_id: str) -> dict[str, object]:
        record = self.journal.get_run(run_id)
        self.journal.events(run_id)  # Verify every privileged signature and journal link.
        if not record.status.terminal or record.terminal_artifact_id is None:
            raise PermissionError("portfolio review has no durable terminal")
        usage = tuple(
            item.record for item in self.usage_ledger.records() if item.record.run_id == run_id
        )
        if usage != (self._usage_record(run_id),):
            raise PermissionError("portfolio terminal lacks exact aggregate physical usage")
        binding = _object(self.store.artifacts.read_json(record.config_hash))
        if (
            binding.get("harness_authority_id") != self.store.harness_authority_id
            or binding.get("run_id") != run_id
        ):
            raise PermissionError("portfolio review belongs to another root or run")
        if "prompt_projection" in binding:
            if binding["prompt_projection"] != portfolio_prompt_projection(binding):
                raise PermissionError("portfolio prompt projection differs from frozen inputs")
            if (
                self.store.artifacts.read_json(canonical_hash(binding["inputs"]))
                != binding["inputs"]
            ):
                raise PermissionError("portfolio prompt source artifact differs from frozen inputs")
        if "projection_recovery" in binding:
            self._verify_projection_recovery(binding)
        frozen = self.journal.event(f"{run_id}.portfolio.frozen")
        terminal_event = self.journal.event(f"{run_id}.portfolio.terminal")
        if (
            frozen is None
            or frozen.event_type != "portfolio.review.frozen"
            or frozen.payload.get("binding_hash") != record.config_hash
            or terminal_event is None
            or terminal_event.payload.get("terminal_hash") != record.terminal_artifact_id
            or terminal_event.payload.get("binding_hash") != record.config_hash
            or terminal_event.previous_hash != terminal_event.payload.get("journal_hash")
        ):
            raise PermissionError("portfolio review lacks a signed frozen/terminal lineage")
        terminal = _object(self.store.artifacts.read_json(record.terminal_artifact_id))
        if terminal.get("run_id") != run_id or terminal.get("binding_hash") != record.config_hash:
            raise PermissionError("portfolio terminal does not bind the actual run")
        if record.status is RunStatus.COMPLETED:
            if (
                terminal_event.event_type != "portfolio.review.validated"
                or terminal.get("status") != "completed"
            ):
                raise PermissionError("portfolio completion lacks privileged validation")
            native = self.journal.event(f"{run_id}.pi-invocation.1.turn.1")
            if (
                native is None
                or native.payload.get("artifact_hash") != terminal.get("raw_response_hash")
                or native.payload.get("runtime") != binding.get("runtime")
            ):
                raise PermissionError("portfolio terminal lacks its actual native model response")
            raw = _object(self.store.artifacts.read_json(_string(terminal, "raw_response_hash")))
            profile = _object(binding["profile"])
            turn = native_turn(raw, _string(profile, "model"))
            parsed = load_model_json(_string(turn.assistant_message, "content"))
            proposal = (
                self._parse_bound_proposal(binding)(
                    parsed.value,
                    binding_hash=record.config_hash,
                    evidence_ids=self._evidence_ids(binding),
                )
                if binding.get("schema_version")
                in {
                    "market-impact.portfolio-review-binding.v4",
                    "market-impact.portfolio-review-binding.v5",
                }
                else parse_portfolio_proposal(
                    parsed.value,
                    binding_hash=record.config_hash,
                    evidence_ids=self._evidence_ids(binding),
                )
            )
            if (
                proposal.to_dict() != terminal.get("proposal")
                or (
                    "proposal_artifact_hash" in terminal
                    and self.store.artifacts.read_json(_string(terminal, "proposal_artifact_hash"))
                    != proposal.to_dict()
                )
                or parsed.value != terminal.get("parsed_proposal")
                or parsed.evidence.to_dict() != terminal.get("parse_evidence")
                or list(portfolio_proposal_text_normalizations(parsed.value))
                != terminal.get("text_normalizations", [])
                or turn.usage.to_dict() != terminal.get("usage")
            ):
                raise PermissionError(
                    "portfolio proposal or usage differs from the native response"
                )
        return terminal

    def derive_initial_adoption(
        self,
        *,
        parsed_proposal: object,
        binding_hash: str,
        research_run_id: str,
        source_portfolio_run_id: str | None = None,
    ) -> tuple[PortfolioDecisionV3, OrderSizingDecisionV2 | None, PortfolioOrderIntent | None]:
        """Harness-only deterministic rebind; this creates no native model terminal."""
        inputs = self.input_source()
        inputs.assert_complete(
            self.store.harness_authority_id, self.exposure_authority, self.clock()
        )
        if self.proposal_version != "v5":
            raise PermissionError("initial adoption requires explicit v5 policy")
        evidence_ids = frozenset(
            {
                "account_state",
                "position_snapshot",
                "authorized_view",
                "exposure_view",
                "mandate",
                research_run_id,
                *inputs.price_bases,
            }
        )
        if source_portfolio_run_id is not None:
            terminal = self.replay(source_portfolio_run_id)
            source_binding = _object(
                self.store.artifacts.read_json(
                    self.journal.get_run(source_portfolio_run_id).config_hash
                )
            )
            if (
                terminal.get("status") != "completed"
                or terminal.get("parsed_proposal") != parsed_proposal
            ):
                raise PermissionError("initial adoption differs from its signed source proposal")
            if source_binding.get("evidence_scope_version") is not None:
                # Scope comes from the signed source, never an arbitrary caller option.
                bound_theses = cast(list[dict[str, object]], source_binding["research_theses"])
                run_ids = tuple(_string(item, "run_id") for item in bound_theses)
                if (
                    research_run_id not in run_ids
                    or self._research_theses(run_ids, inputs.cutoff) != bound_theses
                ):
                    raise PermissionError("initial adoption research differs from signed source")
                scoped = {**source_binding, "inputs": inputs.to_dict()}
                evidence_ids = self._evidence_ids(scoped)
        proposal = parse_portfolio_proposal_v5(
            parsed_proposal, binding_hash=binding_hash, evidence_ids=evidence_ids
        )
        decision = evaluate_portfolio_decision_v3(proposal, inputs, decided_at=self.clock())
        if decision.outcome is PortfolioDecisionOutcome.REJECTED:
            raise PermissionError("adopted initial recommendation violates destination policy")
        if proposal.requested_action is PortfolioAction.HOLD:
            return decision, None, None
        sizing = size_portfolio_decision_v2(
            portfolio_decision=decision,
            authorized_view=inputs.authorized_view,
            position_snapshot=inputs.position_snapshot,
            mandate=inputs.mandate,
            exposure_view=inputs.exposure_view,
            exposure_view_authority=self.exposure_authority,
            price_bases=inputs.price_bases,
            rule_set=inputs.rule_set,
            decided_at=self.clock(),
        )
        if len(sizing.legs) != 1 or sizing.outcome is not OrderSizingOutcome.READY:
            raise PermissionError("adopted initial recommendation is not executable")
        leg = sizing.legs[0]
        assert leg.side is not None and leg.quantity is not None
        basis = inputs.price_bases[leg.instrument_id]
        order = PortfolioOrderIntent(
            client_order_id="portfolio-adopted-order-"
            + canonical_hash({"adoption_binding": binding_hash, "sizing": sizing.to_dict()}),
            portfolio_decision_id=decision.decision_id,
            account_id=inputs.mandate.account_id,
            environment=inputs.mandate.environment,
            instrument_id=leg.instrument_id,
            side=leg.side,
            quantity=leg.quantity,
            order_kind=OrderKind.MARKET,
            created_at=self.clock(),
            expires_at=min(inputs.expires_at, inputs.mandate.valid_until, basis.valid_until),
        )
        return decision, sizing, order

    def execution_admission(self, run_id: str) -> PortfolioExecutionAdmissionV3:
        terminal = self.replay(run_id)
        if terminal["status"] != "completed":
            raise PermissionError("incomplete portfolio review cannot authorize an order")
        record = self.journal.get_run(run_id)
        binding = _object(self.store.artifacts.read_json(record.config_hash))
        inputs = self.input_source()
        inputs.assert_complete(
            self.store.harness_authority_id, self.exposure_authority, self.clock()
        )
        if inputs.to_dict() != binding["inputs"]:
            raise PermissionError("portfolio review input authority changed")
        if "rotation_completion" in binding:
            rotation = _object(binding["rotation_completion"])
            if (
                self._rotation_completion(
                    _string(rotation, "source_run_id"),
                    inputs,
                    cast(str | None, rotation.get("initial_adoption_ref")),
                )
                != rotation
            ):
                raise PermissionError("rotation source reconciliation changed")
        research = cast(list[dict[str, object]], binding["research"])
        research_theses = cast(list[dict[str, object]], binding.get("research_theses", []))
        if (
            self._research(tuple(_string(item, "run_id") for item in research), inputs.cutoff)
            != research
            or self._research_theses(
                tuple(_string(item, "run_id") for item in research_theses), inputs.cutoff
            )
            != research_theses
        ):
            raise PermissionError("portfolio research provenance changed")
        proposal = (
            self._parse_bound_proposal(binding)(
                terminal["parsed_proposal"],
                binding_hash=record.config_hash,
                evidence_ids=self._evidence_ids(binding),
            )
            if binding.get("schema_version")
            in {
                "market-impact.portfolio-review-binding.v4",
                "market-impact.portfolio-review-binding.v5",
            }
            else parse_portfolio_proposal(
                terminal["parsed_proposal"],
                binding_hash=record.config_hash,
                evidence_ids=self._evidence_ids(binding),
            )
        )
        decided_at = datetime.fromisoformat(
            _string(terminal, "completed_at").replace("Z", "+00:00")
        )
        decision = evaluate_portfolio_decision_v3(proposal, inputs, decided_at=decided_at)
        if decision.to_dict() != terminal["decision"]:
            raise PermissionError("portfolio decision differs from deterministic evaluation")
        sizing = size_portfolio_decision_v2(
            portfolio_decision=decision,
            authorized_view=inputs.authorized_view,
            position_snapshot=inputs.position_snapshot,
            mandate=inputs.mandate,
            exposure_view=inputs.exposure_view,
            exposure_view_authority=self.exposure_authority,
            price_bases=inputs.price_bases,
            rule_set=inputs.rule_set,
            decided_at=decided_at,
        )
        if len(sizing.legs) != 1 or sizing.outcome is not OrderSizingOutcome.READY:
            raise PermissionError("portfolio recommendation has no accepted executable target")
        leg = sizing.legs[0]
        assert leg.quantity is not None and leg.side is not None
        basis = inputs.price_bases[leg.instrument_id]
        order = PortfolioOrderIntent(
            client_order_id="portfolio-order-"
            + canonical_hash({"run_id": run_id, "sizing": sizing.to_dict()}),
            portfolio_decision_id=decision.decision_id,
            account_id=inputs.mandate.account_id,
            environment=inputs.mandate.environment,
            instrument_id=leg.instrument_id,
            side=leg.side,
            quantity=leg.quantity,
            order_kind=OrderKind.MARKET,
            created_at=decided_at,
            expires_at=min(inputs.expires_at, inputs.mandate.valid_until, basis.valid_until),
        )
        assert record.terminal_artifact_id is not None
        return PortfolioExecutionAdmissionV3(
            run_id, record.terminal_artifact_id, record.config_hash, decision, sizing, order
        )


def _object(value: object) -> dict[str, object]:
    if not isinstance(value, dict):
        raise TypeError("portfolio artifact must be a JSON object")
    return cast(dict[str, object], value)


def _string(value: Mapping[str, object], key: str) -> str:
    result = value.get(key)
    if not isinstance(result, str) or not result.strip() or result != result.strip():
        raise ValueError(f"portfolio {key} must be nonempty trimmed text")
    return result


def _narrative_string(value: Mapping[str, object], key: str) -> str:
    result = value.get(key)
    if not isinstance(result, str) or not result.strip():
        raise ValueError(f"portfolio {key} must be nonempty text")
    return result.strip()


def _strings(
    value: object,
    *,
    preserve_order: bool = False,
    trim_items: bool = False,
    require_trimmed: bool = False,
) -> tuple[str, ...]:
    if not isinstance(value, list):
        raise ValueError("portfolio references must be strings")
    if any(
        not isinstance(item, str) or not item.strip() or (require_trimmed and item != item.strip())
        for item in cast(list[object], value)
    ):
        raise ValueError("portfolio references must be strings")
    strings = [item.strip() if trim_items else item for item in cast(list[str], value)]
    if preserve_order:
        if len(set(strings)) != len(strings):
            raise ValueError("portfolio references must be unique")
        return tuple(strings)
    return tuple(sorted(set(strings)))


def _timestamp(value: datetime) -> str:
    if value.tzinfo is None:
        raise ValueError("portfolio timestamps must be timezone-aware")
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")
