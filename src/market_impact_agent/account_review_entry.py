"""Explicit Watch/schedule account review; the portfolio authority owns each Run.

Import and preparation create no tasks. This entry never dispatches an order.
"""

from __future__ import annotations

from datetime import datetime
from typing import cast

from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.domain import require_aware
from market_impact_agent.model_provider import ModelProvider
from market_impact_agent.portfolio_review import PortfolioReviewAuthority
from market_impact_agent.prospective_ashare_inputs import ProspectiveAShareInputs


async def run_once(
    *,
    authority: PortfolioReviewAuthority,
    provider: ModelProvider,
    reasons: tuple[str, ...],
    research_run_ids: tuple[str, ...] = (),
    research_thesis_run_ids: tuple[str, ...] = (),
    scheduled_for: datetime | None = None,
    calendar_source: ProspectiveAShareInputs | None = None,
    exchanges: tuple[str, ...] = (),
) -> dict[str, object]:
    """Merge trigger reasons without making triggers a decision identity.

    A scheduled invocation needs received calendar facts for its frozen cutoff.
    A verified closed day can review a valid account valuation. Neither calendar
    state nor a completed review grants executable-price or execution authority.
    """
    if not reasons or any(not reason.strip() for reason in reasons):
        raise ValueError("account review requires an explicit trigger reason")
    if provider.budget is None or provider.budget.journal.path != authority.journal.path:
        raise PermissionError("account review entry requires its existing same-root budget owner")
    inputs = authority.input_source()
    if calendar_source is not None and (
        calendar_source.store.root.resolve() != authority.store.root.resolve()
        or not set(calendar_source.snapshot_ids) <= set(inputs.authorized_view.data_snapshot_ids)
    ):
        raise PermissionError("review calendar must belong to the frozen same-root input")
    calendar = (
        []
        if calendar_source is None
        else [calendar_source.calendar_state(exchange, inputs.cutoff) for exchange in exchanges]
    )
    if scheduled_for is not None:
        require_aware(scheduled_for, "scheduled account review")
        if inputs.cutoff < scheduled_for:
            return {
                "status": "not_due",
                "research_status": "not_evaluated",
                "account_review_status": "not_due",
                "execution_readiness": "not_evaluated",
                "execution_dispatched": False,
            }
        if not calendar or any(item["state"] == "unknown" for item in calendar):
            return {
                "status": "pending_calendar",
                "research_status": "not_evaluated",
                "account_review_status": "pending_calendar",
                "execution_readiness": "calendar_unknown",
                "gaps": ["scheduled_review_calendar_unknown"],
                "calendar": calendar,
                "execution_dispatched": False,
            }
    run_id = authority.review_opportunity_id(
        provider=provider,
        research_run_ids=research_run_ids,
        research_thesis_run_ids=research_thesis_run_ids,
        resolved_inputs=inputs,
    )
    # Reasons belong to the existing episode/budget journal, so a later schedule
    # can join a completed opportunity without altering its sealed decision Run.
    parent_run_id = provider.budget.owner_run_id
    for reason in sorted(set(reasons)):
        event_id = run_id + ".trigger." + canonical_hash(reason)
        if authority.journal.event(event_id) is None:
            authority.journal.append(
                run_id=parent_run_id,
                event_id=event_id,
                event_type="account.review.trigger",
                observed_at=authority.clock(),
                payload={"reason": reason, "opportunity_id": run_id},
            )
    result: dict[str, object]
    try:
        result = await authority.review(
            run_id=run_id,
            provider=provider,
            research_run_ids=research_run_ids,
            research_thesis_run_ids=research_thesis_run_ids,
        )
    except RuntimeError as error:
        if str(error) != "portfolio review run already has an owner":
            raise
        result = {"status": "in_progress"}
    proposal = cast(dict[str, object], result.get("proposal", {}))
    selected = proposal.get("instrument_id")
    basis = inputs.price_bases.get(selected) if isinstance(selected, str) else None
    if result["status"] != "completed":
        execution = "account_review_incomplete"
    elif any(item["state"] == "closed" for item in calendar):
        execution = "closed_market"
    elif proposal.get("requested_action") == "hold":
        execution = "hold"
    elif (
        basis is None
        or basis.basis_kind not in {"reference_quote", "raw_reference_quote", "limit_price"}
        or not basis.observed_at <= authority.clock() < basis.valid_until
    ):
        execution = "awaiting_quotes"
    elif not calendar or any(item["state"] == "unknown" for item in calendar):
        execution = "calendar_unknown"
    else:
        execution = "completed_without_dispatch"
    return {
        **result,
        "opportunity_id": run_id,
        "trigger_reasons": sorted(
            str(event.payload["reason"])
            for event in authority.journal.events(parent_run_id)
            if event.event_type == "account.review.trigger"
            and event.payload["opportunity_id"] == run_id
        ),
        "calendar": calendar,
        "research_status": "completed"
        if research_run_ids or research_thesis_run_ids
        else "not_requested",
        "account_review_status": result["status"],
        "execution_readiness": execution,
        "execution_dispatched": False,
    }
