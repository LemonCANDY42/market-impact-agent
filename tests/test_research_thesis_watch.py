# pyright: reportPrivateUsage=false
from __future__ import annotations

import asyncio
import json
from dataclasses import replace
from datetime import timedelta
from pathlib import Path
from typing import Any, cast

import pytest

from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.agent_watch_admission import (
    AgentWatchAdmissionService,
    WatchAdmissionOutcome,
    WatchDelegateProfile,
)
from market_impact_agent.agent_watch_wake_dispatch import AgentWatchWakeDispatcher
from market_impact_agent.data_inputs import DataPITLane, LocalDataSnapshotStore
from market_impact_agent.dynamic_effectiveness import DatePresentation
from market_impact_agent.model_budget import ModelBudget
from market_impact_agent.monitoring_scope import MonitoringSubjectKind, MonitoringSubjectRef
from market_impact_agent.on_demand_research import OnDemandResearch
from market_impact_agent.pi_deployment import PiRuntimePermit
from market_impact_agent.pi_runtime import PiRuntimeProvider, runtime_identity
from market_impact_agent.prospective_data import ProspectiveDataJournal
from market_impact_agent.research_thesis_runtime import (
    ResearchThesisAuthority,
    ResearchThesisRunInputs,
)
from market_impact_agent.research_thesis_watch import (
    RESEARCH_WATCH_PARENT_TYPE,
    ResearchThesisWatchAuthorityResolver,
    ResearchThesisWatchDelegation,
    ResearchThesisWatchReviewContext,
    admit_research_thesis_watch_proposals,
    research_thesis_watch_tool,
    run_research_thesis_watch_callback,
)
from market_impact_agent.runtime_store import RunJournal, RunStatus
from tests.test_agent_watch_admission import _event_cluster_profile
from tests.test_attention_watch import (
    FIRST_RECEIPT,
    SECOND_RECEIPT,
    THIRD_RECEIPT,
    collection_policy_for_monitoring_test,
    snapshot_for_monitoring_test,
)
from tests.test_pi_runtime import pi_profile
from tests.test_research_thesis_runtime import _answer, _repository


@pytest.mark.parametrize(
    "interrupted,presentation",
    [
        (False, DatePresentation.TRUE_DATE),
        (True, DatePresentation.TRUE_DATE),
        (False, DatePresentation.RELATIVE_OFFSET),
    ],
)
def test_signed_native_proposal_to_receipt_watch_and_same_account_callback(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    interrupted: bool,
    presentation: DatePresentation,
) -> None:
    store = LocalDataSnapshotStore(tmp_path / "harness")
    journal = RunJournal.authoritative(store)
    journal.start_run(
        run_id="parent-budget", config_hash=canonical_hash("study"), created_at=FIRST_RECEIPT
    )
    budget = ModelBudget(journal, "parent-budget", 10, 40_000_000)
    now = [FIRST_RECEIPT + timedelta(seconds=1)]
    deadline = FIRST_RECEIPT + timedelta(minutes=30)
    acquisition = OnDemandResearch(
        store=store,
        parent_budget=budget,
        episode_id="decision-episode-one",
        episode_deadline=deadline,
        run_id="research-parent",
        cutoff=FIRST_RECEIPT,
        pit_lane=DataPITLane.PROSPECTIVE,
        templates=(),
        clock=lambda: now[0],
    )
    collection = ProspectiveDataJournal(store)
    policy = collection_policy_for_monitoring_test()
    collection.register_policy(policy)
    first = snapshot_for_monitoring_test(store, policy=policy, retrieved_at=FIRST_RECEIPT)
    collection.record_snapshot(first, policy=policy)
    original_profile = _event_cluster_profile(collection_policy_id=policy.policy_id)
    profile_fields = {
        key: getattr(original_profile, key)
        for key in original_profile.__dataclass_fields__
        if key not in {"profile_id", "execution_capability"}
    }
    profile_fields["allowed_parent_agent_types"] = (RESEARCH_WATCH_PARENT_TYPE,)
    profile = WatchDelegateProfile.build(**cast(Any, profile_fields))
    delegation = ResearchThesisWatchDelegation.bind(
        acquisition,
        subject=MonitoringSubjectRef(MonitoringSubjectKind.EVENT_CLUSTER, "earnings-1"),
        matcher_terms=("policy", "decision"),
        profiles=(profile,),
    )
    inputs = ResearchThesisRunInputs(
        _repository(at=FIRST_RECEIPT),
        "INDEX.ETF",
        "watch-epoch",
        frozenset({1, 3, 5}),
        watch_delegation=delegation,
        date_presentation=presentation,
    )
    provider_profile = pi_profile()
    monkeypatch.setenv(provider_profile.credential_env, "synthetic-watch-key")

    def installed(_root: Path) -> PiRuntimePermit:
        return PiRuntimePermit(
            canonical_hash(runtime_identity()),
            (provider_profile.route_identity,),
            "synthetic-watch-proof",
        )

    monkeypatch.setattr("market_impact_agent.pi_deployment.installed_permit", installed)
    original_spawn = asyncio.create_subprocess_exec
    arguments = {
        "delegate_profile_id": profile.profile_id,
        "rationale": "Wait for the announced decision before updating the thesis.",
        "watch_question": "Did the 2026-08-28 policy decision change the counter-scenario?",
        "evidence_refs": ["release"],
        "matcher": {
            "clauses": [
                {"field_path": "headline", "mode": "contains_all", "terms": ["policy", "decision"]}
            ]
        },
    }

    async def spawn(program: str, *args: str, **kwargs: Any):
        kwargs["env"]["WATCH_RELATIVE"] = (
            "1" if presentation is DatePresentation.RELATIVE_OFFSET else "0"
        )
        kwargs["env"]["RESEARCH_WATCH_ANSWER"] = json.dumps(_answer())
        kwargs["env"]["RESEARCH_WATCH_ARGUMENTS"] = json.dumps(arguments)
        return await original_spawn(
            program,
            "--import",
            str(Path(__file__).with_name("research_watch_network.mjs")),
            *args,
            **kwargs,
        )

    monkeypatch.setattr(asyncio, "create_subprocess_exec", spawn)
    authority = ResearchThesisAuthority(
        store,
        experiment_id="study",
        arm_id="luna",
        account_scope="account-one",
        clock=lambda: now[0],
    )
    resolver_kwargs: dict[str, Any] = dict(
        experiment_id="study",
        arm_id="luna",
        account_scope="account-one",
        target_id="INDEX.ETF",
        parent_budget=budget,
        episode_id=delegation.episode_id,
        clock=lambda: now[0],
    )
    resolver = ResearchThesisWatchAuthorityResolver(store, **resolver_kwargs)

    async def scenario() -> None:
        provider = PiRuntimeProvider(provider_profile, budget=budget)
        try:
            result = await authority.analyze(
                run_id=acquisition.run_id,
                provider=provider,
                inputs=inputs,
                readonly_tools=(research_thesis_watch_tool(inputs, acquisition.run_id),),
            )
            assert result["status"] == "completed"
        finally:
            await provider.close()
        assert any(
            e.event_type == "pi.role.tool.completed" for e in journal.events(acquisition.run_id)
        )
        signed_parent = journal.get_run(acquisition.run_id)
        journal.start_run(
            run_id="forged-parent", config_hash=signed_parent.config_hash, created_at=now[0]
        )
        journal.finish(
            run_id="forged-parent",
            status=RunStatus.COMPLETED,
            finished_at=now[0],
            terminal_artifact_id=signed_parent.terminal_artifact_id,
        )
        with pytest.raises(PermissionError, match="signed completed"):
            resolver.parent("forged-parent")
        with pytest.raises(ValueError, match="unsupported"):
            await research_thesis_watch_tool(inputs, acquisition.run_id).handler(
                {**arguments, "max_cost": 999}
            )
        context = resolver.delegation_context(acquisition.run_id)
        with pytest.raises(PermissionError, match="projection"):
            resolver.reopen(replace(context, authorized_matcher_terms=("forged",)))
        for field, invalid in (
            ("account_scope", "another-account"),
            ("arm_id", "other-arm"),
            ("target_id", "OTHER.ETF"),
            ("episode_id", "other-episode"),
        ):
            wrong = ResearchThesisWatchAuthorityResolver(
                store, **cast(dict[str, Any], {**resolver_kwargs, field: invalid})
            )
            with pytest.raises(PermissionError, match="scope"):
                wrong.parent(acquisition.run_id)
        with pytest.raises(PermissionError, match="budget"):
            delegation.verify_episode(store, replace(budget, max_requests=11))
        admissions = admit_research_thesis_watch_proposals(
            resolver=resolver, run_id=acquisition.run_id, admitted_at=now[0]
        )
        assert len(admissions) == 1
        admitted = admissions[0]
        assert admitted.outcome is WatchAdmissionOutcome.ADMITTED
        assert admitted.watch_id is not None
        service = AgentWatchAdmissionService(store, profiles=(), delegation_authority=resolver)
        assert service.watch_service.policy(admitted.watch_id).expires_at == deadline
        other_service = None
        other_admitted = None
        other_resolver = None
        if not interrupted and presentation is DatePresentation.TRUE_DATE:
            other_authority = ResearchThesisAuthority(
                store,
                experiment_id="study",
                arm_id="luna",
                account_scope="account-two",
                clock=lambda: now[0],
            )
            other_provider = PiRuntimeProvider(provider_profile, budget=budget)
            try:
                other_terminal = await other_authority.analyze(
                    run_id="research-parent-two", provider=other_provider, inputs=inputs
                )
                assert other_terminal["status"] == "completed"
            finally:
                await other_provider.close()
            other_resolver = ResearchThesisWatchAuthorityResolver(
                store, **cast(dict[str, Any], {**resolver_kwargs, "account_scope": "account-two"})
            )
            other_admitted = admit_research_thesis_watch_proposals(
                resolver=other_resolver, run_id="research-parent-two", admitted_at=now[0]
            )[0]
            assert other_admitted.outcome is WatchAdmissionOutcome.ADMITTED
            assert other_admitted.watch_id != admitted.watch_id
            other_service = AgentWatchAdmissionService(
                store, profiles=(), delegation_authority=other_resolver
            )

        unrelated = snapshot_for_monitoring_test(
            store,
            policy=policy,
            retrieved_at=SECOND_RECEIPT,
            raw_record=b'{"headline":"Unrelated notice"}',
            headline="Unrelated notice",
        )
        collection.record_snapshot(unrelated, policy=policy)
        now[0] = SECOND_RECEIPT + timedelta(seconds=1)
        unmatched = service.watch_service.run_due_from_snapshot(
            admitted.watch_id, now=now[0], collection_snapshot_id=unrelated.snapshot_id
        )
        assert unmatched.wake is None
        assert service.watch_service.pending_wakes() == ()
        second = snapshot_for_monitoring_test(
            store,
            policy=policy,
            retrieved_at=THIRD_RECEIPT,
            raw_record=b'{"headline":"Policy decision revised"}',
            headline="Policy decision revised",
        )
        collection.record_snapshot(second, policy=policy)
        now[0] = THIRD_RECEIPT + timedelta(seconds=1)
        poll = service.watch_service.run_due_from_snapshot(
            admitted.watch_id, now=now[0], collection_snapshot_id=second.snapshot_id
        )
        assert poll.wake is not None
        wake = poll.wake
        dispatcher = AgentWatchWakeDispatcher(
            service, run_journal=RunJournal(tmp_path / "dispatch.sqlite3")
        )
        dispatches = dispatcher.dispatch_wake(wake, dispatched_at=now[0])
        if other_service is not None:
            assert (
                other_admitted is not None
                and other_admitted.watch_id is not None
                and other_resolver is not None
            )
            other_poll = other_service.watch_service.run_due_from_snapshot(
                other_admitted.watch_id, now=now[0], collection_snapshot_id=second.snapshot_id
            )
            assert other_poll.wake is not None
            other_dispatcher = AgentWatchWakeDispatcher(
                other_service, run_journal=RunJournal(tmp_path / "dispatch.sqlite3")
            )
            other_dispatch = other_dispatcher.dispatch_wake(other_poll.wake, dispatched_at=now[0])
            assert len(other_dispatch) == 1

            async def other_review(context: ResearchThesisWatchReviewContext) -> dict[str, object]:
                assert context.account_scope == "account-two"
                return {"status": "second-scope-reviewed"}

            assert (
                await run_research_thesis_watch_callback(
                    dispatcher=other_dispatcher,
                    resolver=other_resolver,
                    run_id=other_dispatch[0].run.run_id,
                    review=other_review,
                )
            )["status"] == "second-scope-reviewed"

        assert len(dispatches) == 1
        calls: list[ResearchThesisWatchReviewContext] = []

        async def review(context: ResearchThesisWatchReviewContext) -> dict[str, object]:
            calls.append(context)
            assert context.account_scope == "account-one"
            assert context.parent_budget is budget
            assert context.parent_run_id == acquisition.run_id
            assert context.episode_id == delegation.episode_id
            assert context.research_question == inputs.repository.evidence_pack.research_question
            assert context.watch_question == arguments["watch_question"]
            assert context.thesis.invalidation_conditions == tuple(
                cast(list[str], _answer()["invalidation_conditions"])
            )
            assert context.new_version_ids == wake.new_version_ids
            if interrupted:
                raise RuntimeError("callback interrupted after entering review")
            return {"status": "reviewed", "prior_thesis_run_id": context.parent_run_id}

        run_id = dispatches[0].binding.run_id
        now[0] = FIRST_RECEIPT
        with pytest.raises(PermissionError):
            await run_research_thesis_watch_callback(
                dispatcher=dispatcher, resolver=resolver, run_id=run_id, review=review
            )
        now[0] = deadline + timedelta(seconds=1)
        with pytest.raises(PermissionError, match="deadline"):
            await run_research_thesis_watch_callback(
                dispatcher=dispatcher, resolver=resolver, run_id=run_id, review=review
            )
        now[0] = THIRD_RECEIPT + timedelta(seconds=1)
        if interrupted:
            with pytest.raises(RuntimeError, match="interrupted"):
                await run_research_thesis_watch_callback(
                    dispatcher=dispatcher, resolver=resolver, run_id=run_id, review=review
                )
            replay = await run_research_thesis_watch_callback(
                dispatcher=dispatcher, resolver=resolver, run_id=run_id, review=review
            )
            assert replay["status"] == "reconciliation_required"
            assert len(calls) == 1
            return
        completed = await run_research_thesis_watch_callback(
            dispatcher=dispatcher, resolver=resolver, run_id=run_id, review=review
        )
        restarted = AgentWatchWakeDispatcher(
            AgentWatchAdmissionService(store, profiles=(), delegation_authority=resolver),
            run_journal=RunJournal(tmp_path / "dispatch.sqlite3"),
        )
        assert (
            await run_research_thesis_watch_callback(
                dispatcher=restarted, resolver=resolver, run_id=run_id, review=review
            )
            == completed
        )
        assert len(calls) == 1
        assert restarted.run_journal.get_run(run_id).status is RunStatus.COMPLETED
        assert len(restarted.dispatch_wake(wake, dispatched_at=now[0])) == 1
        assert service.watch_service.pending_wakes() == ()

    asyncio.run(scenario())
