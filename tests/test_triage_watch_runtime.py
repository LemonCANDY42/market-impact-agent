from __future__ import annotations

from datetime import timedelta
from pathlib import Path

from market_impact_agent.agent_watch_admission import WatchAdmissionOutcome
from market_impact_agent.prospective_data import ProspectiveCollectionPolicy
from market_impact_agent.triage_watch_runtime import admit_triage_follow_up_watch

from .test_agent_watch_admission import (
    _triage_setup,  # pyright: ignore[reportPrivateUsage]
)
from .test_agent_watch_wake_judgment import CPA_ALIAS, ROOT
from .test_attention_watch import snapshot_for_monitoring_test


def test_triage_follow_up_watch_materializes_authorized_route_without_model_call(
    tmp_path: Path,
) -> None:
    store, journal, _, original_profile, _, authority = _triage_setup(tmp_path)
    context = authority.delegation_context()
    admitted_at = context.created_at + timedelta(minutes=1)
    original = journal.policy(original_profile.collection_policy_id)
    policy = ProspectiveCollectionPolicy.build(
        capability=original.capability,
        sources=original.sources,
        window_start=context.created_at,
        parameters={"keywords": ["alpha", "safety"], "max_items": 20},
        poll_interval_seconds=60,
        maximum_gap_seconds=90,
    )
    journal.register_policy(policy)
    collection = snapshot_for_monitoring_test(
        store,
        policy=policy,
        retrieved_at=admitted_at,
        headline="Alpha safety initial report",
        raw_record=b'{"headline":"Alpha safety initial report"}',
    )
    journal.record_snapshot(collection, policy=policy)

    result = admit_triage_follow_up_watch(
        state_root=store.root,
        cluster_id=authority.cluster_id,
        collection_policy_id=policy.policy_id,
        model_profile_alias=CPA_ALIAS,
        skill_root=ROOT / "skills",
        match_field_path="headline",
        admitted_at=admitted_at,
    )

    assert result.admission.outcome is WatchAdmissionOutcome.ADMITTED
    assert result.admission.watch_id is not None
    watch = result.admission.watch_id
    service = authority.store
    assert service.root == store.root
    assert result.summary()["execution_capability"] is False
    assert watch.startswith("attention-watch-")
