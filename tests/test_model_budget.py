from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path

import pytest

from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.model_budget import ModelBudget, ModelBudgetScope
from market_impact_agent.runtime_store import RunJournal


def test_atomic_parent_reservations_retain_unknown_and_do_not_reset(tmp_path: Path) -> None:
    journal = RunJournal(tmp_path / "runs.sqlite3")
    journal.start_run(
        run_id="parent", config_hash=canonical_hash("fixed"), created_at=datetime.now(UTC)
    )
    budget = ModelBudget(journal, "parent", 4, 100, prior_requests=1, prior_cost_microusd=10)

    async def scenario() -> None:
        outcomes = await asyncio.gather(
            *(budget.reserve(f"child-{index}:1:1", 40) for index in range(3)),
            return_exceptions=True,
        )
        assert sum(item is None for item in outcomes) == 2
        assert sum(isinstance(item, RuntimeError) for item in outcomes) == 1
        assert budget.summary()["reserved_microusd"] == 80
        budget.settle("child-0:1:1", cost_microusd=5, evidence_ref="response-0")
        # Restart does not release child-1's unknown response reservation.
        reopened = replace(budget, journal=RunJournal(journal.path))
        await reopened.reserve("child-2:1:1", 40)
        assert reopened.summary() == {
            "physical_requests": 4,
            "known_cost_microusd": 15,
            "reserved_microusd": 80,
            "unsettled_requests": 2,
        }
        with pytest.raises(PermissionError, match="already admitted"):
            await reopened.reserve("child-0:1:1", 1)
        with pytest.raises(ValueError, match="changed"):
            await replace(reopened, max_requests=100).reserve("new", 1)
        with pytest.raises(RuntimeError, match="budget"):
            await reopened.reserve("new", 1)
        budget.settle("child-0:1:1", cost_microusd=5, evidence_ref="response-0")
        with pytest.raises(ValueError, match="different content"):
            budget.settle("child-0:1:1", cost_microusd=0, evidence_ref="response-0")

    asyncio.run(scenario())


def test_stages_share_one_atomic_parent_and_retain_prior_unknown(tmp_path: Path) -> None:
    journal = RunJournal(tmp_path / "runs.sqlite3")
    journal.start_run(
        run_id="study", config_hash=canonical_hash("fixed"), created_at=datetime.now(UTC)
    )
    budget = ModelBudget(
        journal,
        "study",
        10,
        100,
        prior_requests=2,
        prior_cost_microusd=10,
        prior_reserved_microusd=5,
        prior_unsettled_requests=1,
        scope_limits=(ModelBudgetScope("analysis", 50, 10, 5), ModelBudgetScope("rolling", 50)),
        scope="analysis",
    )

    async def scenario() -> None:
        await budget.reserve("analysis-1", 35)
        with pytest.raises(RuntimeError, match="stage"):
            await budget.reserve("analysis-2", 1)
        rolling = replace(budget, scope="rolling", journal=RunJournal(journal.path))
        await rolling.reserve("rolling-1", 50)
        assert rolling.summary() == {
            "physical_requests": 4,
            "known_cost_microusd": 10,
            "reserved_microusd": 90,
            "unsettled_requests": 3,
        }
        with pytest.raises(RuntimeError, match="parent"):
            await rolling.reserve("rolling-2", 1)
        rolling.settle("rolling-1", cost_microusd=20, evidence_ref="native-1")
        assert budget.scope_summary()["reserved_microusd"] == 40
        assert rolling.scope_summary()["known_cost_microusd"] == 20
        with pytest.raises(RuntimeError, match="stage"):
            await budget.reserve("analysis-3", 1)
        # A new child or stage cannot reinterpret existing stage authorization.
        with pytest.raises(ValueError, match="changed"):
            await replace(
                rolling,
                scope_limits=(
                    ModelBudgetScope("analysis", 40, 10, 5),
                    ModelBudgetScope("rolling", 60),
                ),
            ).reserve("changed", 1)

    asyncio.run(scenario())
