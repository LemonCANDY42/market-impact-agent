from __future__ import annotations

import asyncio
import json
from datetime import timedelta
from decimal import Decimal
from pathlib import Path
from typing import cast

import pytest

from market_impact_agent.account_review_entry import run_once
from market_impact_agent.agent_contracts import EvidencePack, EvidenceReference, canonical_hash
from market_impact_agent.autonomous_paper import AutonomousOperationState
from market_impact_agent.data_inputs import FrozenDataSnapshotInput
from market_impact_agent.frozen_research import FrozenResearchRepository
from market_impact_agent.model_budget import ModelBudget
from market_impact_agent.pi_runtime import PiRuntimeProvider
from market_impact_agent.prospective_ashare_quotes import ExecutableProspectiveAShareInputs
from market_impact_agent.prospective_mock_composition import ProspectiveMockComposition
from market_impact_agent.prospective_mock_execution import (
    dispatch_prospective_mock_review,
    open_prospective_mock_execution,
    reconcile_prospective_mock_review,
)
from market_impact_agent.providers import MockExecutionProvider
from market_impact_agent.research import EvidenceTier
from market_impact_agent.research_thesis_runtime import ResearchThesisRunInputs
from market_impact_agent.runtime_store import RunJournal

from .test_ashare_security_qualification import capture_rows
from .test_portfolio_review import (
    NativePortfolio,
    _answer,  # pyright: ignore[reportPrivateUsage]
    native_portfolio,  # noqa: F401 # pyright: ignore[reportUnusedImport]
)
from .test_prospective_ashare_quotes import CUTOFF, executable_inputs
from .test_research_thesis_runtime import _repository  # pyright: ignore[reportPrivateUsage]


def test_two_scheduled_mock_reviews_observe_source_fill_and_revocation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    native_portfolio: NativePortfolio,  # noqa: F811
) -> None:
    """Callable schedule cycles only: native owners with synthetic external I/O."""
    profile, answer, spawns = native_portfolio
    market = executable_inputs(tmp_path, quote_time="2026-08-31 09:30:30")
    seed = executable_inputs(
        tmp_path, symbol="510300.SH", etf=True, quote_time="2026-08-31 09:30:30"
    )
    store = market.store
    clock = [CUTOFF]

    def now():
        clock[0] += timedelta(microseconds=1)
        return clock[0]

    calendar_id = capture_rows(
        store,
        "trade_cal",
        {"exchange": "SSE", "start_date": "20260831", "end_date": "20260901"},
        [
            {"exchange": "SSE", "cal_date": "20260831", "is_open": 1, "pretrade_date": "20260828"},
            {
                "exchange": "SSE",
                "cal_date": "20260901",
                "is_open": 1,
                "pretrade_date": "20260831",
            },
        ],
        CUTOFF - timedelta(seconds=1),
    )
    frozen = FrozenDataSnapshotInput(
        frozenset((*market.snapshot_ids, *seed.snapshot_ids, calendar_id))
    )
    journal = RunJournal.authoritative(store)
    journal.start_run(
        run_id="scheduled-budget", config_hash=canonical_hash("study"), created_at=now()
    )
    budget = ModelBudget(journal, "scheduled-budget", 2, 1000000)
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
        candidate_proofs={"600519.SH": (profile_snapshot.snapshot_id,)},
    )

    def factory(value: FrozenDataSnapshotInput) -> ExecutableProspectiveAShareInputs:
        return ExecutableProspectiveAShareInputs(
            store=store,
            snapshot_ids=tuple(sorted(value.authorized_snapshot_ids)),
            qualification_policy=market.qualification_policy,
        )

    def compose(value: FrozenDataSnapshotInput) -> ProspectiveMockComposition:
        composition = ProspectiveMockComposition(
            store=store,
            profile_id="model",
            study_registration_id="study",
            opening_authority_ref="study",
            parent_run_id="scheduled-budget",
            market_factory=factory,
            clock=now,
        )
        account, _ = composition.capture_context(research, value)
        composition.portfolio_authority(research, value, account, None)
        return composition

    async def scheduled(composition: ProspectiveMockComposition, value: FrozenDataSnapshotInput):
        assert composition.portfolio is not None and composition.inputs is not None
        provider = PiRuntimeProvider(profile, budget=budget)
        try:
            return await run_once(
                authority=composition.portfolio,
                provider=provider,
                reasons=("schedule",),
                scheduled_for=composition.inputs.cutoff,
                calendar_source=factory(value),
                exchanges=("SSE",),
            )
        finally:
            await provider.close()

    answer[0] = {
        **_answer("open"),
        "target_ref": "candidate:600519.SH",
        "evidence_refs": ["Current account cash and positions"],
        "target_gross_exposure_ratio": "0.30",
    }
    for field in ("horizon_band", "instrument_id", "venue", "instrument_class", "direction"):
        answer[0].pop(field, None)
    first = compose(frozen)
    assert first.inputs is not None
    original = first.inputs
    review = asyncio.run(scheduled(first, frozen))
    assert review["status"] == "completed", review
    assert review["execution_readiness"] == "completed_without_dispatch", review
    assert review["execution_dispatched"] is False
    run_id = str(review["opportunity_id"])
    ack = dispatch_prospective_mock_review(first, run_id)
    assert ack["execution_status"] == "accepted", ack
    before = first.provider.simulated_account_snapshot(price_bases=original.price_bases)
    assert before.cash == original.account_state.cash
    assert all(item.target_id != "600519.SH" for item in before.positions or ())

    # A later RECEIVED minute, not ACK, gives the fill adapter authority.
    clock[0] += timedelta(minutes=1)
    fresh_ids: list[str] = []
    for symbol, api in (("600519.SH", "rt_min"), ("510300.SH", "rt_etf_min")):
        fresh_ids.append(
            capture_rows(
                store,
                api,
                {"ts_code": symbol, "freq": "1MIN"},
                [
                    {
                        "ts_code": symbol,
                        "time": (clock[0] + timedelta(hours=8)).strftime("%Y-%m-%d %H:%M:00"),
                        "open": "10",
                        "close": "10.01",
                        "high": "10.02",
                        "low": "9.99",
                        "vol": "10000",
                        "amount": "100100",
                    }
                ],
                now(),
            )
        )
    fresh = FrozenDataSnapshotInput(frozenset((*frozen.authorized_snapshot_ids, *fresh_ids)))
    filled = reconcile_prospective_mock_review(first, run_id, fresh)
    assert filled["fill_status"] == "filled", filled
    assert filled["reconciliation_complete"] is True, filled
    evidence = cast(
        dict[str, object], store.artifacts.read_json(str(filled["fill_evidence_artifact_hash"]))
    )
    assert Decimal(str(evidence["fee"])) == Decimal("8.71")
    assert evidence["sellable_at"] == "2026-09-01T09:30:00+08:00"
    assert first.provider.simulated_sellable_quantity("600519.SH") == 0
    after = first.provider.simulated_account_snapshot(price_bases=original.price_bases)
    assert after.cash != before.cash
    assert any(
        item.target_id == "600519.SH" and item.quantity > 0 for item in after.positions or ()
    )

    reopened = compose(frozen)
    replay = reconcile_prospective_mock_review(reopened, run_id, fresh)
    assert replay["fill_evidence_artifact_hash"] == filled["fill_evidence_artifact_hash"]
    assert replay["reconciliation_complete"] is True, replay
    unchanged = reopened.provider.simulated_account_snapshot(price_bases=original.price_bases)
    assert unchanged.cash == after.cash and unchanged.positions == after.positions

    answer[0] = _answer("hold")
    answer[0].pop("horizon_band")
    answer[0]["evidence_refs"] = ["Current account cash and positions"]
    request_path = tmp_path / "second-request.json"
    monkeypatch.setenv("PORTFOLIO_FIXTURE_REQUEST_PATH", str(request_path))
    second = compose(fresh)
    assert second.inputs is not None and second.inputs.cutoff > original.cutoff
    assert second.inputs.account_state.cash == after.cash
    assert second.inputs.account_state.positions == after.positions
    hold = asyncio.run(scheduled(second, fresh))
    assert hold["status"] == "completed" and hold["execution_readiness"] == "hold", hold
    assert hold["opportunity_id"] != run_id
    assert dispatch_prospective_mock_review(second, str(hold["opportunity_id"])) == {
        "execution_status": "completed_hold",
        "execution_dispatched": False,
    }
    request = json.loads(request_path.read_text())
    user_message = next(item for item in reversed(request["input"]) if item.get("role") == "user")
    content = user_message["content"]
    prompt = json.loads(
        content if isinstance(content, str) else "".join(item.get("text", "") for item in content)
    )
    assert prompt["inputs"]["account_state"] == second.inputs.account_state.to_dict()
    assert len(spawns) == 2 and budget.summary()["physical_requests"] == 2

    calls: list[MockExecutionProvider] = []
    reconcile = MockExecutionProvider.reconcile

    def observed_reconcile(self: MockExecutionProvider):
        calls.append(self)
        return reconcile(self)

    monkeypatch.setattr(MockExecutionProvider, "reconcile", observed_reconcile)
    for revoke in (False, True):
        service = open_prospective_mock_execution(reopened, reconciliation_input=fresh)
        lease_authority, lease_id = (
            service.provider_lease_authority,
            service.provider_lease.lease_id,
        )
        try:
            # Persist the exact boundary a killed process leaves: claimed mutation
            # and SUBMITTING operation, while the venue's durable fill already exists.
            with store.authority_transaction() as connection:
                lease_authority.claim_mutation_in_transaction(
                    connection,
                    lease_id=lease_id,
                    mutation_id="interrupted-reconcile",
                    kind="reconcile",
                    started_at=now(),
                )
                connection.execute(
                    "UPDATE autonomous_operations SET state = ?, lease_token = ? "
                    "WHERE client_order_id = ?",
                    ("submitting", "interrupted-submit", str(ack["client_order_id"])),
                )
            if revoke:
                assert not lease_authority.request_revocation(lease_id, requested_at=now())
        finally:
            service.close()
        calls.clear()
        recovered = open_prospective_mock_execution(reopened, reconciliation_input=fresh)
        try:
            assert not calls
            assert (
                recovered.get(str(ack["client_order_id"])).state is AutonomousOperationState.UNKNOWN
            )
            assert recovered.dispatch_next() is None
            with pytest.raises(PermissionError, match="provider_claim_recovery"):
                recovered.account_state_source()
            result = recovered.reconcile()
            if revoke:
                assert result.gaps == ("provider_lease_unavailable",)
                assert not calls
                with pytest.raises(KeyError):
                    lease_authority.resolve_for_recovery(lease_id)
            else:
                assert result.complete, result
                assert calls
                assert (
                    recovered.get(str(ack["client_order_id"])).state
                    is AutonomousOperationState.RECONCILED
                )
                assert lease_authority.resolve(lease_id).lease_id == lease_id
        finally:
            recovered.close()
    calls.clear()
    with pytest.raises(PermissionError):
        open_prospective_mock_execution(reopened, reconciliation_input=fresh)
    assert not calls
