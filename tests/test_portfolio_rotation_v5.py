"""Opt-in v5 rotates by a source close, retaining legacy fail-closed v4."""

import asyncio
from decimal import Decimal
from pathlib import Path
from typing import cast

import pytest

from market_impact_agent.agent_schema import validate_agent_contract
from market_impact_agent.portfolio_decision import PortfolioAction, PortfolioLegRole
from market_impact_agent.portfolio_review import (
    PortfolioReviewAuthority,
    parse_portfolio_proposal_v4,
    parse_portfolio_proposal_v5,
)

from .test_autonomous_paper import TARGET
from .test_portfolio_review import (
    NativePortfolio,
    _answer,  # pyright: ignore[reportPrivateUsage]
    _setup,  # pyright: ignore[reportPrivateUsage]
    native_portfolio,  # noqa: F401  # pyright: ignore[reportUnusedImport]
)


def test_real_v5_producer_replays_only_source_close(
    tmp_path: Path,
    native_portfolio: NativePortfolio,  # noqa: F811
) -> None:
    profile, answers, spawns = native_portfolio
    old_authority, inputs, _, _, clock, open_service, _ = _setup(
        tmp_path, action=PortfolioAction.REDUCE
    )
    authority = PortfolioReviewAuthority(
        old_authority.store,
        input_source=lambda: inputs[0],
        exposure_authority=old_authority.exposure_authority,
        clock=lambda: clock[0],
        proposal_version="v5",
    )
    answers[0] = {
        **_answer("rotate"),
        "instrument_id": "OTHER.ARCX",
        "rotation_source_instrument_id": TARGET,
    }
    from market_impact_agent.pi_runtime import PiRuntimeProvider

    provider = PiRuntimeProvider(profile)
    result = asyncio.run(authority.review_account(run_id="rotation-v5-source", provider=provider))
    assert result["status"] == "completed", result
    assert (
        validate_agent_contract(result["proposal"], "agent-portfolio-proposal-v5.schema.json") == ()
    )
    admission = authority.execution_admission("rotation-v5-source")
    assert admission.order.instrument_id == TARGET
    assert len(admission.portfolio_decision.legs) == 1
    assert admission.portfolio_decision.legs[0].role is PortfolioLegRole.ROTATION_SOURCE
    assert admission.order.quantity == admission.portfolio_decision.legs[0].current_quantity
    assert (
        asyncio.run(authority.review_account(run_id="rotation-v5-source", provider=provider))
        == result
    )
    assert len(spawns) == 1
    from dataclasses import replace

    from market_impact_agent.autonomous_paper import (
        _assert_chain,  # pyright: ignore[reportPrivateUsage]
    )
    from market_impact_agent.portfolio_decision import size_portfolio_decision_v2

    legacy_answer = dict(answers[0])
    legacy_answer.pop("rotation_source_instrument_id")
    legacy = parse_portfolio_proposal_v4(
        legacy_answer,
        binding_hash=admission.binding_hash,
        evidence_ids=frozenset({"account_state", "exposure_view"}),
    )
    forged = replace(admission.portfolio_decision, proposal=legacy)
    forged_sizing = size_portfolio_decision_v2(
        portfolio_decision=forged,
        authorized_view=inputs[0].authorized_view,
        position_snapshot=inputs[0].position_snapshot,
        mandate=inputs[0].mandate,
        exposure_view=inputs[0].exposure_view,
        exposure_view_authority=authority.exposure_authority,
        price_bases=inputs[0].price_bases,
        rule_set=inputs[0].rule_set,
        decided_at=clock[0],
    )
    with pytest.raises(PermissionError, match="destination must remain blocked"):
        _assert_chain(
            proposal=legacy,
            portfolio_decision=forged,
            sizing_decision=forged_sizing,
            mandate=inputs[0].mandate,
            exposure_view=inputs[0].exposure_view,
            price_bases=inputs[0].price_bases,
        )
    service, _mock = open_service()
    service.portfolio_review_authority = authority
    try:
        operation = service.admit_portfolio_review("rotation-v5-source")
        assert service.admit_portfolio_review("rotation-v5-source") == operation
        service.decide_portfolio_approval(
            operation.client_order_id, approved=True, actor_ref="test-human"
        )
        assert service.dispatch_next() is not None
        assert service.dispatch_next() is None
    finally:
        service.close()
    with pytest.raises(PermissionError, match="source reconciliation authority"):
        asyncio.run(
            authority.review_after_rotation(
                run_id="rotation-v5-destination",
                source_run_id="rotation-v5-source",
                provider=provider,
            )
        )
    assert len(spawns) == 1


def test_rotation_contract_does_not_reinterpret_v4() -> None:
    answer = {**_answer("rotate"), "rotation_source_instrument_id": "600519.SH"}
    with pytest.raises(ValueError, match="unauthorized"):
        parse_portfolio_proposal_v4(
            answer,
            binding_hash="a" * 64,
            evidence_ids=frozenset({"account_state", "exposure_view"}),
        )
    for source in (None, answer["instrument_id"]):
        with pytest.raises(ValueError, match="rotation"):
            parse_portfolio_proposal_v5(
                {**answer, "rotation_source_instrument_id": source},
                binding_hash="a" * 64,
                evidence_ids=frozenset({"account_state", "exposure_view"}),
            )


def test_source_followup_rejects_partial_and_requires_fresh_account(
    tmp_path: Path,
    native_portfolio: NativePortfolio,  # noqa: F811
) -> None:
    from dataclasses import replace
    from datetime import timedelta

    from market_impact_agent.account_state import account_state_snapshot_from_dict
    from market_impact_agent.agent_contracts import canonical_hash
    from market_impact_agent.pi_runtime import PiRuntimeProvider
    from market_impact_agent.portfolio_review import RotationSourceCompletion

    profile, answers, spawns = native_portfolio
    old, inputs, _, exposures, clock, *_ = _setup(tmp_path, action=PortfolioAction.REDUCE)
    authority = PortfolioReviewAuthority(
        old.store,
        input_source=lambda: inputs[0],
        exposure_authority=old.exposure_authority,
        clock=lambda: clock[0],
        proposal_version="v5",
    )
    answers[0] = {
        **_answer("rotate"),
        "instrument_id": "OTHER.ARCX",
        "rotation_source_instrument_id": TARGET,
    }
    model = PiRuntimeProvider(profile)
    asyncio.run(authority.review_account(run_id="rotation-gate-source", provider=model))
    admission = authority.execution_admission("rotation-gate-source")
    closed_at = clock[0] + timedelta(seconds=1)
    clock[0] += timedelta(seconds=2)
    completion = RotationSourceCompletion(
        "rotation-gate-source",
        inputs[0].account_state.account_reference_hash,
        TARGET,
        admission.order.client_order_id,
        admission.order.quantity,
        closed_at,
    )

    class Reconciliation:
        def reopen_source_completion(self, source_run_id: str) -> RotationSourceCompletion:
            assert source_run_id == completion.source_run_id
            return completion

    authority.rotation_authority = Reconciliation()
    verify_completion = authority._rotation_completion  # pyright: ignore[reportPrivateUsage]
    with pytest.raises(PermissionError, match="fresh account"):
        verify_completion(completion.source_run_id, inputs[0])  # pyright: ignore[reportPrivateUsage]
    original = inputs[0].account_state.core_dict()
    fill = {
        "fill_reference": "reconciled-source-fill",
        "order_reference": completion.source_order_reference,
        "target_id": TARGET,
        "venue": "ARCX",
        "instrument_class": "exchange_traded_fund",
        "side": "sell",
        "quantity": str(completion.source_quantity),
        "filled_at": closed_at.isoformat().replace("+00:00", "Z"),
    }
    original.update(
        positions=[],
        open_orders=[],
        recent_fills=[fill],
        as_of=clock[0].isoformat().replace("+00:00", "Z"),
        reconciled_at=clock[0].isoformat().replace("+00:00", "Z"),
    )

    def account_with(payload: dict[str, object]):
        return account_state_snapshot_from_dict(
            {**payload, "snapshot_id": "account-state-snapshot-" + canonical_hash(payload)}
        )

    current = replace(inputs[0], account_state=account_with(original), cutoff=clock[0])
    assert (
        verify_completion(completion.source_run_id, current)["source_run_id"]
        == completion.source_run_id
    )  # pyright: ignore[reportPrivateUsage]
    mutations: tuple[dict[str, object], ...] = (
        {**original, "recent_fills": [{**fill, "quantity": "1"}]},
        {**original, "recent_fills": []},
        {**original, "account_reference_hash": "account-ref-" + "b" * 64},
    )
    for payload in mutations:
        with pytest.raises(PermissionError):
            verify_completion(
                completion.source_run_id, replace(current, account_state=account_with(payload))
            )  # pyright: ignore[reportPrivateUsage]
    assert len(spawns) == 1
    from market_impact_agent.authorized_decision_view import AuthorizedDecisionView
    from market_impact_agent.portfolio_decision import PortfolioExposureViewV2

    position = current.account_state.project_positions(
        evaluated_at=clock[0], max_age=timedelta(minutes=5)
    )
    view = AuthorizedDecisionView.build(
        cutoff=clock[0],
        frozen_at=clock[0],
        data_snapshot_ids=(),
        decision_input_ids=(),
        position_snapshot=position,
    )
    exposure = PortfolioExposureViewV2.build(
        authorized_view=view,
        position_snapshot=position,
        raw_mark_set_hash=canonical_hash("source-closed"),
        execution_ledger_snapshot_hash=canonical_hash("source-filled"),
        reconciliation_ledger_snapshot_hash=canonical_hash(current.account_state.to_dict()),
        currency="USD",
        marked_positions=(),
        daily_turnover_used=Decimal(0),
        daily_submissions_used=1,
        active_kill_reasons=(),
        observed_at=clock[0],
        valid_until=current.expires_at,
    )
    exposures[0] = exposure
    inputs[0] = replace(
        current, position_snapshot=position, authorized_view=view, exposure_view=exposure
    )
    answers[0] = _answer("hold")
    result = asyncio.run(
        authority.review_after_rotation(
            run_id="rotation-new-cash-decision",
            source_run_id=completion.source_run_id,
            provider=PiRuntimeProvider(profile),
        )
    )
    assert result["status"] == "completed", result
    assert len(spawns) == 2
    assert isinstance(result["proposal"], dict)
    assert cast(dict[str, object], result["proposal"]).get("requested_action") == "hold"
