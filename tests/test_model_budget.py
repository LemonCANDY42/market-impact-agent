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


def test_complete_groups_protect_each_arm_without_counting_allocation_as_spend(
    tmp_path: Path,
) -> None:
    from market_impact_agent.model_budget import ModelBudgetGroupMember

    journal = RunJournal(tmp_path / "runs.sqlite3")
    journal.start_run(
        run_id="paired", config_hash=canonical_hash("fixed"), created_at=datetime.now(UTC)
    )
    budget = ModelBudget(journal, "paired", 6, 120)
    members = (
        ModelBudgetGroupMember("price", 2, 50),
        ModelBudgetGroupMember("event", 2, 50),
    )

    async def scenario() -> None:
        group = await budget.admit_group(
            group_id="window-1", members=members, call_graph_hash="a" * 64
        )
        assert budget.summary() == {
            "physical_requests": 0,
            "known_cost_microusd": 0,
            "reserved_microusd": 0,
            "unsettled_requests": 0,
        }
        assert budget.group_allocation_summary()["allocated_microusd_remaining"] == 100
        with pytest.raises(RuntimeError, match="complete group"):
            await budget.admit_group(group_id="window-2", members=members, call_graph_hash="a" * 64)
        with pytest.raises(PermissionError, match="group member"):
            await group.reserve("unassigned", 1)
        await budget.reserve("ungrouped", 20)
        with pytest.raises(RuntimeError, match="parent"):
            await budget.reserve("steal", 1)
        price = group.for_group_member("price")
        await price.reserve("price:1", 50)
        with pytest.raises(RuntimeError, match="member allowance"):
            await price.reserve("price:2", 1)
        price.settle("price:1", cost_microusd=10, evidence_ref="native-price")
        await price.reserve("price:2", 40)
        # Reopening the admitted group does not recreate the call graph allocation.
        restored = replace(budget, journal=RunJournal(journal.path))
        reopened = await restored.admit_group(
            group_id="window-1", members=members, call_graph_hash="a" * 64
        )
        await reopened.for_group_member("event").reserve("event:1", 50)
        assert restored.summary() == {
            "physical_requests": 4,
            "known_cost_microusd": 10,
            "reserved_microusd": 110,
            "unsettled_requests": 3,
        }
        with pytest.raises(ValueError, match="cannot change"):
            await restored.admit_group(
                group_id="window-1", members=members, call_graph_hash="b" * 64
            )
        with pytest.raises(PermissionError, match="member"):
            reopened.for_group_member("not-registered")

    asyncio.run(scenario())


def test_group_admission_is_atomic_and_cannot_transfer_stage_allowance(tmp_path: Path) -> None:
    from market_impact_agent.model_budget import ModelBudgetGroupMember

    journal = RunJournal(tmp_path / "runs.sqlite3")
    journal.start_run(
        run_id="groups", config_hash=canonical_hash("fixed"), created_at=datetime.now(UTC)
    )
    budget = ModelBudget(
        journal,
        "groups",
        10,
        100,
        scope_limits=(ModelBudgetScope("one", 60), ModelBudgetScope("two", 40)),
        scope="one",
    )
    members = (ModelBudgetGroupMember("a", 1, 30), ModelBudgetGroupMember("b", 1, 30))

    async def scenario() -> None:
        outcomes = await asyncio.gather(
            *(
                budget.admit_group(group_id=f"pair-{i}", members=members, call_graph_hash="a" * 64)
                for i in range(2)
            ),
            return_exceptions=True,
        )
        assert sum(isinstance(item, ModelBudget) for item in outcomes) == 1
        assert sum(isinstance(item, RuntimeError) for item in outcomes) == 1
        group = next(item for item in outcomes if isinstance(item, ModelBudget))
        with pytest.raises(PermissionError, match="stage"):
            replace(group, scope="two").for_group_member("a")
        assert budget.summary()["physical_requests"] == 0
        assert not any(
            event.event_type == "pi.budget.reserved" for event in journal.events("groups")
        )

    asyncio.run(scenario())


@pytest.mark.parametrize("operation", ["reserve", "admit_group"])
def test_settlement_between_reads_does_not_reject_a_funded_group(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, operation: str
) -> None:
    from market_impact_agent.model_budget import ModelBudgetGroupMember

    journal = RunJournal(tmp_path / "runs.sqlite3")
    journal.start_run(
        run_id="study", config_hash=canonical_hash("fixed"), created_at=datetime.now(UTC)
    )
    ceiling = 100 if operation == "reserve" else 200
    budget = ModelBudget(
        journal,
        "study",
        8,
        ceiling,
        scope_limits=(ModelBudgetScope("stage", ceiling),),
        scope="stage",
    )
    members = (ModelBudgetGroupMember("a", 2, 50), ModelBudgetGroupMember("b", 2, 50))

    async def scenario() -> None:
        group = await budget.admit_group(group_id="one", members=members, call_graph_hash="a" * 64)
        first, second = group.for_group_member("a"), group.for_group_member("b")
        await first.reserve("a:1", 50)
        original = journal.events
        settled = False

        def settle_after_snapshot(run_id: str):
            nonlocal settled
            snapshot = original(run_id)
            if run_id == "study" and not settled:
                settled = True
                first.settle("a:1", cost_microusd=10, evidence_ref="native-a")
            return snapshot

        monkeypatch.setattr(journal, "events", settle_after_snapshot)
        if operation == "reserve":
            await second.reserve("b:1", 50)
            assert budget.summary()["reserved_microusd"] == 50
        else:
            await budget.admit_group(group_id="two", members=members, call_graph_hash="a" * 64)
        assert settled
        assert budget.summary()["known_cost_microusd"] == 10

    asyncio.run(scenario())
