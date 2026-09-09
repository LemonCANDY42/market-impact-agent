from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import timedelta
from pathlib import Path
from typing import Any, cast

import pytest

from market_impact_agent.account_review_entry import run_once
from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.authorized_decision_view import AuthorizedDecisionView
from market_impact_agent.model_budget import ModelBudget
from market_impact_agent.pi_runtime import PiRuntimeProvider
from market_impact_agent.portfolio_decision import PortfolioExposureViewV2
from market_impact_agent.portfolio_review import PortfolioReviewAuthority, PortfolioReviewInputs
from market_impact_agent.prospective_ashare_inputs import ProspectiveAShareInputs
from market_impact_agent.runtime_store import RunJournal

from .test_ashare_security_qualification import accepted_policy, capture_rows
from .test_portfolio_review import (
    NativePortfolio,
    _setup,  # pyright: ignore[reportPrivateUsage]
    native_portfolio,  # noqa: F401 # pyright: ignore[reportUnusedImport]
)


def bind_calendar(
    authority: PortfolioReviewAuthority,
    inputs: list[PortfolioReviewInputs],
    exposures: list[PortfolioExposureViewV2],
    flag: str | None,
) -> ProspectiveAShareInputs:
    current = inputs[0]
    received = current.cutoff - timedelta(seconds=1)
    stamp = current.cutoff.strftime("%Y%m%d")
    snapshot = capture_rows(
        authority.store,
        "trade_cal",
        {"exchange": "SSE", "start_date": stamp, "end_date": stamp},
        [] if flag is None else [{"exchange": "SSE", "cal_date": stamp, "is_open": flag}],
        received,
    )
    view = AuthorizedDecisionView.build(
        cutoff=current.cutoff,
        frozen_at=current.cutoff,
        data_snapshot_ids=(snapshot,),
        decision_input_ids=(),
        position_snapshot=current.position_snapshot,
    )
    core = {
        **current.exposure_view.core_dict(),
        "authorized_decision_view_id": view.view_id,
        "authorized_decision_view_hash": canonical_hash(view.to_dict()),
    }
    exposure = replace(
        current.exposure_view,
        exposure_view_id="portfolio-exposure-view-" + canonical_hash(core),
        authorized_decision_view_id=view.view_id,
        authorized_decision_view_hash=canonical_hash(view.to_dict()),
    )
    inputs[0] = replace(current, authorized_view=view, exposure_view=exposure)
    exposures[0] = exposure
    return ProspectiveAShareInputs(
        store=authority.store,
        snapshot_ids=(snapshot,),
        qualification_policy=accepted_policy(authority.store, received),
    )


@pytest.mark.parametrize(
    "calendar_flag,unknown_generation", [("0", False), ("1", True), (None, False)]
)
def test_calendar_account_opportunity_merges_reasons_and_replays(
    tmp_path: Path,
    native_portfolio: NativePortfolio,  # noqa: F811
    calendar_flag: str | None,
    unknown_generation: bool,
) -> None:
    profile, answer, spawns = native_portfolio
    authority, inputs, _, exposures, clock, _, _ = _setup(tmp_path)
    calendar = bind_calendar(authority, inputs, exposures, calendar_flag)
    journal = authority.journal
    journal.start_run(run_id="episode", config_hash=canonical_hash("episode"), created_at=clock[0])
    budget = ModelBudget(journal, "episode", 5, 1_000_000)
    if unknown_generation:
        answer[0] = {"__network_failure": True}

    async def scenario() -> None:
        provider = PiRuntimeProvider(profile, budget=budget)
        try:
            scheduled = await run_once(
                authority=authority,
                provider=provider,
                reasons=("schedule",),
                scheduled_for=inputs[0].cutoff,
                calendar_source=calendar,
                exchanges=("SSE",),
            )
            if calendar_flag is None:
                assert scheduled["status"] == "pending_calendar"
                assert scheduled["execution_readiness"] == "calendar_unknown"
                assert not spawns
                return
            assert scheduled["status"] == ("incomplete" if unknown_generation else "completed")
            assert cast(list[dict[str, object]], scheduled["calendar"])[0]["state"] == (
                "closed" if calendar_flag == "0" else "open"
            )
            assert scheduled["execution_dispatched"] is False
            assert scheduled["account_review_status"] == scheduled["status"]
            assert scheduled["execution_readiness"] == (
                "account_review_incomplete" if unknown_generation else "closed_market"
            )
            assert len(spawns) == 1
            clock[0] += timedelta(days=1)
            reopened = PortfolioReviewAuthority(
                authority.store,
                input_source=lambda: inputs[0],
                exposure_authority=authority.exposure_authority,
                clock=lambda: clock[0],
            )
            watch = await run_once(authority=reopened, provider=provider, reasons=("watch",))
            assert watch["opportunity_id"] == scheduled["opportunity_id"]
            assert watch["trigger_reasons"] == ["schedule", "watch"]
            assert watch["status"] == scheduled["status"]
            assert len(spawns) == 1
            # A changed full input cannot reuse the completed decision, even if its
            # account, cutoff and provider profile are unchanged.
            before = watch["opportunity_id"]
            inputs[0] = replace(
                inputs[0], rule_set=replace(inputs[0].rule_set, rule_set_id="changed")
            )
            assert reopened.review_opportunity_id(provider=provider) != before
            with pytest.raises(PermissionError, match="not current"):
                await run_once(authority=reopened, provider=provider, reasons=("watch",))
            assert len(spawns) == 1
        finally:
            await provider.close()

    asyncio.run(scenario())


@pytest.mark.parametrize(
    "action,cash_only,has_candidate,quote_condition",
    [
        ("hold", False, True, "stale"),
        ("open", False, True, "stale"),
        ("hold", True, True, "stale"),
        ("hold", False, False, "stale"),
        ("hold", False, True, "expires_during_review"),
        ("open", False, True, "expires_after_review"),
    ],
)
def test_optional_stale_candidate_can_be_reviewed_but_never_executed(
    tmp_path: Path,
    native_portfolio: NativePortfolio,  # noqa: F811
    action: str,
    cash_only: bool,
    has_candidate: bool,
    quote_condition: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from decimal import Decimal

    from market_impact_agent.account_state import CashBalance
    from market_impact_agent.agent_contracts import EvidencePack, EvidenceReference
    from market_impact_agent.data_inputs import FrozenDataSnapshotInput
    from market_impact_agent.dynamic_ashare_admission import DynamicAShareAdmission
    from market_impact_agent.frozen_research import FrozenResearchRepository
    from market_impact_agent.prospective_ashare_quotes import ExecutableProspectiveAShareInputs
    from market_impact_agent.prospective_mock_composition import ProspectiveMockComposition
    from market_impact_agent.research import EvidenceTier
    from market_impact_agent.research_thesis_runtime import ResearchThesisRunInputs

    from .test_prospective_ashare_quotes import CUTOFF, executable_inputs
    from .test_research_thesis_runtime import _repository  # pyright: ignore[reportPrivateUsage]

    market = executable_inputs(
        tmp_path,
        symbol="600519.SH",
        quote_time="2026-08-28 15:00:00" if quote_condition == "stale" else "2026-08-31 09:29:10",
    )
    seed = executable_inputs(tmp_path, symbol="510300.SH", etf=True)
    frozen = FrozenDataSnapshotInput(frozenset((*market.snapshot_ids, *seed.snapshot_ids)))
    store = market.store
    journal = RunJournal.authoritative(store)
    journal.start_run(
        run_id="optional-episode", config_hash=canonical_hash("optional"), created_at=CUTOFF
    )
    budget = ModelBudget(journal, "optional-episode", 2, 1_000_000)

    def source(value: FrozenDataSnapshotInput) -> ExecutableProspectiveAShareInputs:
        return ExecutableProspectiveAShareInputs(
            store=store,
            snapshot_ids=tuple(sorted(value.authorized_snapshot_ids)),
            qualification_policy=market.qualification_policy,
        )

    clock = [CUTOFF]
    composition = ProspectiveMockComposition(
        store=store,
        profile_id="optional",
        study_registration_id="optional",
        opening_authority_ref="optional",
        parent_run_id="optional-episode",
        market_factory=source,
        clock=lambda: clock[0],
    )
    if cash_only:
        composition.provider.configure_simulated_account(
            seed=composition.seed,
            cash=(CashBalance("CNY", Decimal(100000), Decimal(100000)),),
            positions=(),
            instruments={},
            opened_at=CUTOFF,
            opening_authority={
                "version": "cny-local-mock.v1",
                "source_reference": "synthetic-cash-opening",
                "opening_inventory": "overnight_sellable",
            },
        )
    repository = _repository("600519.SH", at=CUTOFF)
    profile_snapshot = next(
        table.snapshot
        for table in market._tables()  # pyright: ignore[reportPrivateUsage]
        if table.api == "stock_basic"
    )
    document = profile_snapshot.to_dict()
    pack = repository.evidence_pack
    repository = FrozenResearchRepository(
        evidence_pack=EvidencePack.build(
            event_id=pack.event_id,
            as_of=pack.as_of,
            research_question=pack.research_question,
            evidence=(
                *pack.evidence,
                EvidenceReference(
                    evidence_id=profile_snapshot.snapshot_id,
                    claim_id="candidate-identity",
                    source_ref="source://stock-basic",
                    source_tier=EvidenceTier.UNVERIFIED,
                    available_at=profile_snapshot.completed_at,
                    content_hash=canonical_hash(document),
                    summary="Actually captured synthetic company profile.",
                ),
            ),
            pattern_packs=pack.pattern_packs,
            allowed_targets=pack.allowed_targets,
        ),
        evidence_documents={
            **repository._evidence_documents,  # pyright: ignore[reportPrivateUsage]
            profile_snapshot.snapshot_id: document,
        },
        pattern_packs=repository._pattern_packs,  # pyright: ignore[reportPrivateUsage]
    )
    research = ResearchThesisRunInputs(
        repository,
        "600519.SH",
        "epoch",
        frozenset({1}),
        candidate_proofs={"600519.SH": (profile_snapshot.snapshot_id,)} if has_candidate else {},
    )
    account, cutoff = composition.capture_context(research, frozen)
    security = DynamicAShareAdmission(source(frozen)).discover(("600519.SH",), cutoff)[0]
    assert ("current_quote_stale" in security.gaps) == (quote_condition == "stale")
    authority = composition.portfolio_authority(research, frozen, account, security)
    assert authority.proposal_version == "v6"
    assert [candidate.symbol for candidate in authority.input_source().admitted_candidates] == (
        ["600519.SH"] if has_candidate else []
    )
    assert ("600519.SH" not in authority.input_source().price_bases) == (quote_condition == "stale")
    if quote_condition != "stale":
        assert (
            authority.input_source().expires_at
            > authority.input_source().price_bases["600519.SH"].valid_until
        )
    if cash_only:
        assert not authority.input_source().account_state.positions
        assert not authority.input_source().price_bases
    profile, answers, spawns = native_portfolio
    answers[0].pop("horizon_band")
    answers[0]["evidence_refs"] = ["Current account cash and positions"]
    if action == "open":
        answers[0].update(
            requested_action="open",
            target_ref="candidate:600519.SH",
            target_gross_exposure_ratio="0.30",
        )
    if quote_condition == "expires_during_review":
        spawn = asyncio.create_subprocess_exec

        async def slow_response(program: str, *args: str, **kwargs: Any):
            process = await spawn(program, *args, **kwargs)
            clock[0] += timedelta(seconds=20)
            return process

        monkeypatch.setattr(asyncio, "create_subprocess_exec", slow_response)

    async def scenario() -> None:
        provider = PiRuntimeProvider(profile, budget=budget)
        try:
            review = await run_once(authority=authority, provider=provider, reasons=("watch",))
            assert review["status"] == "completed", review
            assert review["execution_readiness"] == (
                "hold"
                if action == "hold"
                else "calendar_unknown"
                if quote_condition == "expires_after_review"
                else "awaiting_quotes"
            )
            assert cast(dict[str, object], review["proposal"])["requested_action"] == action
            if action == "open":
                if quote_condition == "expires_after_review":
                    clock[0] += timedelta(seconds=20)
                with pytest.raises(PermissionError, match="no accepted executable target"):
                    authority.execution_admission(str(review["opportunity_id"]))
            assert len(spawns) == 1
        finally:
            await provider.close()

    asyncio.run(scenario())


def test_schedule_input_drift_and_existing_claim_do_not_dispatch(
    tmp_path: Path,
    native_portfolio: NativePortfolio,  # noqa: F811
) -> None:
    profile, _, spawns = native_portfolio
    authority, inputs, _, exposures, clock, _, _ = _setup(tmp_path)
    calendar = bind_calendar(authority, inputs, exposures, "1")
    original_input = inputs[0]
    journal = authority.journal
    journal.start_run(
        run_id="concurrent-episode", config_hash=canonical_hash("concurrent"), created_at=clock[0]
    )
    budget = ModelBudget(journal, "concurrent-episode", 2, 1_000_000)
    reads = [0]

    def drifting_input() -> PortfolioReviewInputs:
        reads[0] += 1
        return (
            original_input
            if reads[0] == 1
            else replace(
                original_input, rule_set=replace(original_input.rule_set, rule_set_id="changed")
            )
        )

    async def scenario() -> None:
        provider = PiRuntimeProvider(profile, budget=budget)
        try:
            authority.input_source = drifting_input
            with pytest.raises(PermissionError, match="opportunity"):
                await run_once(
                    authority=authority,
                    provider=provider,
                    reasons=("schedule",),
                    scheduled_for=original_input.cutoff,
                    calendar_source=calendar,
                    exchanges=("SSE",),
                )
            assert not spawns
            authority.input_source = lambda: original_input
            run_id = authority.review_opportunity_id(provider=provider)
            claim = journal.try_claim_run(run_id)
            assert claim is not None
            try:
                waiting = await run_once(authority=authority, provider=provider, reasons=("watch",))
                assert waiting["status"] == "in_progress"
                assert waiting["execution_dispatched"] is False
                assert not spawns
            finally:
                claim.release()
        finally:
            await provider.close()

    asyncio.run(scenario())


@pytest.mark.parametrize(
    "action,calendar_known,missing_day",
    [("hold", True, False), ("close", True, False), ("hold", False, False), ("hold", True, True)],
)
def test_closed_calendar_values_actual_holding_without_executable_quote(
    tmp_path: Path,
    native_portfolio: NativePortfolio,  # noqa: F811
    action: str,
    calendar_known: bool,
    missing_day: bool,
) -> None:
    from datetime import UTC, datetime
    from decimal import Decimal

    from market_impact_agent.data_inputs import FrozenDataSnapshotInput
    from market_impact_agent.prospective_ashare_quotes import ExecutableProspectiveAShareInputs
    from market_impact_agent.prospective_mock_composition import ProspectiveMockComposition
    from market_impact_agent.research_thesis_runtime import ResearchThesisRunInputs

    from .test_prospective_ashare_quotes import CUTOFF, executable_inputs
    from .test_research_thesis_runtime import _repository  # pyright: ignore[reportPrivateUsage]

    seed = executable_inputs(tmp_path, symbol="510300.SH", etf=True)
    store = seed.store
    clock = [CUTOFF]
    journal = RunJournal.authoritative(store)
    journal.start_run(
        run_id="closed-review", config_hash=canonical_hash("closed"), created_at=CUTOFF
    )
    budget = ModelBudget(journal, "closed-review", 2, 1_000_000)

    def source(frozen: FrozenDataSnapshotInput) -> ExecutableProspectiveAShareInputs:
        return ExecutableProspectiveAShareInputs(
            store=store,
            snapshot_ids=tuple(sorted(frozen.authorized_snapshot_ids)),
            qualification_policy=seed.qualification_policy,
        )

    composition = ProspectiveMockComposition(
        store=store,
        profile_id="closed",
        study_registration_id="closed",
        opening_authority_ref="closed",
        parent_run_id="closed-review",
        market_factory=source,
        clock=lambda: clock[0],
    )
    initial = ResearchThesisRunInputs(
        _repository("ASHARE.RESEARCH", at=CUTOFF),
        "ASHARE.RESEARCH",
        "epoch",
        frozenset({1}),
    )
    opening, _ = composition.capture_context(
        initial, FrozenDataSnapshotInput(frozenset(seed.snapshot_ids))
    )
    assert opening.positions and opening.positions[0].quantity > 0
    clock[0] += timedelta(days=2 if missing_day else 1)
    # Synthetic received holiday calendar and an actually completed raw close.
    receipt = clock[0] - timedelta(seconds=1)
    close = capture_rows(
        store,
        "fund_daily",
        {"ts_code": "510300.SH", "start_date": "20260831", "end_date": "20260831"},
        [{"ts_code": "510300.SH", "trade_date": "20260831", "close": "12", "amount": "100000"}],
        receipt,
    )
    calendar_day = clock[0].strftime("%Y%m%d")
    calendar = capture_rows(
        store,
        "trade_cal",
        {"exchange": "SSE", "start_date": calendar_day, "end_date": calendar_day},
        [{"exchange": "SSE", "cal_date": calendar_day, "is_open": "0", "pretrade_date": "20260831"}]
        if calendar_known
        else [],
        receipt,
    )
    frozen = FrozenDataSnapshotInput(frozenset((*seed.snapshot_ids, close, calendar)))
    research = ResearchThesisRunInputs(
        _repository("ASHARE.RESEARCH", at=clock[0]),
        "ASHARE.RESEARCH",
        "epoch",
        frozenset({1}),
    )
    if not calendar_known or missing_day:
        with pytest.raises(PermissionError, match="held_position_valuation"):
            composition.capture_context(research, frozen)
        return
    account, cutoff = composition.capture_context(research, frozen)
    assert account.positions and account.positions[0].quantity == opening.positions[0].quantity
    authority = composition.portfolio_authority(research, frozen, account, None)
    basis = authority.input_source().price_bases["510300.SH"]
    assert basis.basis_kind == "raw_completed_session_valuation"
    assert basis.observed_at == datetime(2026, 8, 31, 7, tzinfo=UTC)
    assert basis.price == Decimal(12)
    profile, answers, spawns = native_portfolio
    answers[0].pop("horizon_band")
    answers[0]["evidence_refs"] = ["Current account cash and positions"]
    if action == "close":
        answers[0].update(
            requested_action="close",
            target_ref="holding:510300.SH:buy",
            target_gross_exposure_ratio="0",
        )

    async def scenario() -> None:
        provider = PiRuntimeProvider(profile, budget=budget)
        try:
            review = await run_once(
                authority=authority,
                provider=provider,
                reasons=("schedule",),
                scheduled_for=cutoff,
                calendar_source=source(frozen),
                exchanges=("SSE",),
            )
            assert review["status"] == "completed", review
            assert review["execution_readiness"] == "closed_market"
            assert review["execution_dispatched"] is False
            if action == "close":
                with pytest.raises(PermissionError, match="no accepted executable target"):
                    authority.execution_admission(str(review["opportunity_id"]))
            assert len(spawns) == 1
        finally:
            await provider.close()

    asyncio.run(scenario())
