# pyright: reportPrivateUsage=false
"""Legacy terminal fixtures followed by real pi repair under one shared budget."""

from __future__ import annotations

import asyncio
from dataclasses import replace
from pathlib import Path

import pytest

from market_impact_agent.model_budget import ModelBudget
from market_impact_agent.pi_runtime import PiRuntimeProvider
from market_impact_agent.portfolio_review import PORTFOLIO_REVIEW_PROMPT, PortfolioReviewAuthority
from market_impact_agent.runtime_store import RunStatus

from .test_portfolio_review import NativePortfolio, _setup
from .test_portfolio_review import native_portfolio as native_portfolio


def _freeze_legacy_failure(authority: PortfolioReviewAuthority, provider: PiRuntimeProvider) -> str:
    run_id = "legacy-portfolio"
    budget = provider.budget
    assert budget is not None
    binding: dict[str, object] = {
        "schema_version": "market-impact.portfolio-review-binding." + authority.proposal_version,
        "harness_authority_id": authority.store.harness_authority_id,
        "run_id": run_id,
        "inputs": authority.input_source().to_dict(),
        "research": [],
        "research_theses": [],
        "profile": provider.profile.to_dict(),
        "runtime": provider.runtime_identity,
        "prompt": PORTFOLIO_REVIEW_PROMPT,
        "budget_owner": {
            "journal_path": str(budget.journal.path),
            "run_id": budget.owner_run_id,
            "binding": budget.binding,
        },
    }
    digest = authority.store.artifacts.put_json(binding).content_hash
    authority.journal.start_run(run_id=run_id, config_hash=digest, created_at=authority.clock())
    authority._events.append(
        run_id=run_id,
        event_id=run_id + ".portfolio.frozen",
        event_type="portfolio.review.frozen",
        observed_at=authority.clock(),
        payload={"binding_hash": digest},
    )
    terminal = authority.store.artifacts.put_json(
        {
            "schema_version": "market-impact.portfolio-review-terminal."
            + authority.proposal_version,
            "run_id": run_id,
            "status": "incomplete",
            "binding_hash": digest,
            "reason": "_BudgetExceeded",
            "completed_at": authority.clock().isoformat(),
        }
    )
    authority._events.append(
        run_id=run_id,
        event_id=run_id + ".portfolio.terminal",
        event_type="portfolio.review.incomplete",
        observed_at=authority.clock(),
        payload={
            "binding_hash": digest,
            "terminal_hash": terminal.content_hash,
            "journal_hash": authority.journal.journal_hash(run_id),
            "run_status": "failed",
        },
    )
    authority.journal.finish(
        run_id=run_id,
        status=RunStatus.FAILED,
        finished_at=authority.clock(),
        terminal_artifact_id=terminal.content_hash,
    )
    authority._record_usage(run_id)
    return run_id


@pytest.mark.parametrize("obstacle", [None, "reserved", "inputs_changed"])
def test_projection_recovery_preserves_authority_and_single_dispatch(
    tmp_path: Path,
    native_portfolio: NativePortfolio,
    obstacle: str | None,
) -> None:
    profile, _, _ = native_portfolio
    authority, inputs, _, _, _, _, _ = _setup(tmp_path)
    authority.journal.start_run(run_id="parent", config_hash="c" * 64, created_at=authority.clock())
    budget = ModelBudget(authority.journal, "parent", 1, 1_000_000)

    async def scenario() -> None:
        provider = PiRuntimeProvider(profile, budget=budget)
        try:
            if obstacle == "reserved":
                await budget.reserve("legacy-portfolio.pi-invocation.1:1:1", 100)
            old_id = _freeze_legacy_failure(authority, provider)
            old = authority.replay(old_id)
            old_hash = authority.journal.journal_hash(old_id)
            if obstacle == "reserved":
                with pytest.raises(PermissionError, match="admitted request"):
                    authority.projection_recovery_run_id(old_id)
                return
            repaired_id = authority.projection_recovery_run_id(old_id)
            assert repaired_id == old_id + ".projection-recovery"
            if obstacle == "inputs_changed":
                inputs[0] = replace(
                    inputs[0],
                    rule_set=replace(inputs[0].rule_set, source_documents=({"changed": True},)),
                )
                with pytest.raises(PermissionError, match="original authority"):
                    await authority.review(
                        run_id=repaired_id,
                        provider=provider,
                        research_run_ids=(),
                        projection_recovery_of=old_id,
                    )
                assert budget.summary()["physical_requests"] == 0
                return
            result = await authority.review(
                run_id=repaired_id,
                provider=provider,
                research_run_ids=(),
                projection_recovery_of=old_id,
            )
            assert result["status"] == "completed"
            assert authority.replay(old_id) == old
            assert authority.journal.journal_hash(old_id) == old_hash
            assert authority.replay(repaired_id) == result
            assert budget.summary()["physical_requests"] == 1
            assert (
                await authority.review(
                    run_id=repaired_id,
                    provider=provider,
                    research_run_ids=(),
                    projection_recovery_of=old_id,
                )
                == result
            )
            assert budget.summary()["physical_requests"] == 1
            assert authority.projection_recovery_run_id(old_id) == repaired_id
        finally:
            await provider.close()

    asyncio.run(scenario())
