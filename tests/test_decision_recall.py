import asyncio
import json
import sqlite3
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from market_impact_agent.agent_runtime import ModelTurn, ProviderUsage
from market_impact_agent.data_inputs import LocalDataSnapshotStore
from market_impact_agent.decision_recall import (
    DecisionRecallProjection,
    RecallProjectionEntry,
    decision_recall_tools,
)
from market_impact_agent.decision_thesis import (
    BaseCaseDirection,
    HorizonBand,
    ResearchThesisV1,
)
from market_impact_agent.research_thesis_runtime import (
    ResearchThesisAuthority,
    ResearchThesisRunInputs,
)
from market_impact_agent.runtime_store import RunJournal

from .test_agent_engine import FixtureProvider
from .test_pi_runtime import pi_profile
from .test_research_thesis_runtime import _repository  # pyright: ignore[reportPrivateUsage]

NOW = datetime(2026, 9, 4, 8, 0, tzinfo=UTC)


def _thesis(as_of: datetime, epoch: str, text: str = "A tactical earnings rerating."):
    return ResearchThesisV1(
        root_event_id="earnings-root",
        thesis_epoch=epoch,
        as_of=as_of,
        horizon_band=HorizonBand.TACTICAL,
        primary_horizon_sessions=5,
        base_case_direction=BaseCaseDirection.UP,
        thesis=text,
        priced_in_assessment="The reported surprise was not in the prior price.",
        transmission=("earnings -> estimates -> price",),
        counter_scenario="The margin guide could reverse the rerating.",
        evidence_refs=("release", "market"),
        counterevidence_refs=(),
        invalidation_conditions=("Revenue guidance is cut.",),
        review_after_sessions=1,
    )


def _entry(store: LocalDataSnapshotStore, thesis: ResearchThesisV1) -> RecallProjectionEntry:
    authored = thesis.to_dict()
    for key in ("schema_version", "root_event_id", "thesis_epoch", "as_of", "thesis_id"):
        authored.pop(key)
    profile = pi_profile()
    assistant: dict[str, object] = {"role": "assistant", "content": json.dumps(authored)}
    provider = FixtureProvider(
        [
            ModelTurn(
                response_id=f"response-{thesis.thesis_epoch}",
                model=profile.model,
                assistant_message=assistant,
                tool_calls=(),
                finish_reason="stop",
                usage=ProviderUsage(input_tokens=10, output_tokens=5),
                raw_response={
                    "id": f"response-{thesis.thesis_epoch}",
                    "model": profile.model,
                    "message": assistant,
                },
                latency_ms=1,
            )
        ]
    )
    provider.profile = profile
    run_id = f"thesis-{thesis.thesis_epoch}"
    authority = ResearchThesisAuthority(
        store,
        experiment_id="decision-recall-tests",
        arm_id=thesis.thesis_epoch,
        clock=lambda: thesis.as_of,
    )
    terminal = asyncio.run(
        authority.analyze(
            run_id=run_id,
            provider=provider,
            inputs=ResearchThesisRunInputs(
                repository=_repository(
                    at=thesis.as_of,
                    event_id=thesis.root_event_id,
                ),
                target_id="INDEX.ETF",
                thesis_epoch=thesis.thesis_epoch,
                allowed_horizons=frozenset({thesis.primary_horizon_sessions}),
            ),
        )
    )
    return RecallProjectionEntry(
        root_event_id=thesis.root_event_id,
        thesis_epoch=thesis.thesis_epoch,
        source_kind="research_thesis",
        source_run_id=run_id,
        source_artifact_hash=str(terminal["thesis_artifact_hash"]),
        source_as_of=thesis.as_of,
        instrument_ids=("ETF-1",),
        industry_tags=("technology",),
        summary="research_thesis direction=up horizon_sessions=5",
    )


def test_recall_rejects_unsigned_journal(tmp_path: Path) -> None:
    store = LocalDataSnapshotStore(tmp_path / "harness")
    with pytest.raises(ValueError, match="authoritative signed Run Journal"):
        DecisionRecallProjection(
            tmp_path / "recall.sqlite3",
            artifact_store=store.artifacts,
            journal=RunJournal(tmp_path / "unsigned.sqlite3"),
        )


def test_recall_search_is_navigation_and_reopen_is_authoritative(tmp_path: Path) -> None:
    store = LocalDataSnapshotStore(tmp_path / "harness")
    recall = DecisionRecallProjection(
        tmp_path / "recall.sqlite3",
        artifact_store=store.artifacts,
        journal=RunJournal.authoritative(store),
    )
    first = _entry(store, _thesis(NOW, "epoch-1"))
    future = _entry(store, _thesis(NOW + timedelta(days=1), "epoch-2"))
    recall.rebuild((first, future))

    current = recall.read_current_thesis(root_event_id="earnings-root", as_of=NOW)
    assert current == first
    hits = recall.search_prior_decisions(
        as_of=NOW,
        instrument_id="ETF-1",
        industry_tag="technology",
        query="direction=up",
    )
    assert hits == (first,)
    assert hits[0].to_dict()["evidence"] is False

    reopened = recall.read_prior_decisions((first.recall_id,), as_of=NOW)
    assert reopened[0].source == _thesis(NOW, "epoch-1").to_dict()
    assert reopened[0].to_dict()["evidence"] is False
    with pytest.raises(PermissionError, match="after the decision cutoff"):
        recall.read_prior_decisions((future.recall_id,), as_of=NOW)


def test_recall_projection_is_idempotent_and_rebuildable(tmp_path: Path) -> None:
    store = LocalDataSnapshotStore(tmp_path / "harness")
    recall = DecisionRecallProjection(
        tmp_path / "recall.sqlite3",
        artifact_store=store.artifacts,
        journal=RunJournal.authoritative(store),
    )
    entry = _entry(store, _thesis(NOW, "epoch-1"))
    recall.add(entry)
    recall.add(entry)
    assert recall.search_prior_decisions(as_of=NOW) == (entry,)

    recall.rebuild(())
    assert recall.search_prior_decisions(as_of=NOW) == ()
    recall.rebuild((entry,))
    assert recall.read_current_thesis(root_event_id="earnings-root", as_of=NOW) == entry


def test_recall_reopen_rejects_projection_cutoff_tampering(tmp_path: Path) -> None:
    store = LocalDataSnapshotStore(tmp_path / "harness")
    recall = DecisionRecallProjection(
        tmp_path / "recall.sqlite3",
        artifact_store=store.artifacts,
        journal=RunJournal.authoritative(store),
    )
    future = _entry(store, _thesis(NOW + timedelta(days=1), "epoch-future"))
    recall.add(future)
    with sqlite3.connect(recall.path) as connection:
        connection.execute(
            "UPDATE decision_recall_entries SET source_as_of = ? WHERE recall_id = ?",
            (NOW.isoformat().replace("+00:00", "Z"), future.recall_id),
        )
    with pytest.raises(ValueError, match="cutoff differs"):
        recall.read_prior_decisions((future.recall_id,), as_of=NOW)
    with sqlite3.connect(recall.path) as connection:
        connection.execute(
            "UPDATE decision_recall_entries SET summary = ? WHERE recall_id = ?",
            ("future outcome was positive", future.recall_id),
        )
    with pytest.raises(ValueError, match="not derived"):
        recall.read_current_thesis(root_event_id="earnings-root", as_of=NOW)
    with pytest.raises(ValueError, match="not derived"):
        recall.search_prior_decisions(as_of=NOW)


def test_recall_rejects_private_outcome_and_oversized_reopen(tmp_path: Path) -> None:
    store = LocalDataSnapshotStore(tmp_path / "harness")
    recall = DecisionRecallProjection(
        tmp_path / "recall.sqlite3",
        artifact_store=store.artifacts,
        journal=RunJournal.authoritative(store),
    )
    unsafe = _thesis(NOW, "epoch-unsafe").to_dict()
    unsafe["account_id"] = "private-account"
    artifact = store.artifacts.put_json(unsafe)
    entry = RecallProjectionEntry(
        root_event_id="earnings-root",
        thesis_epoch="epoch-unsafe",
        source_kind="research_thesis",
        source_run_id="unsigned-run",
        source_artifact_hash=artifact.content_hash,
        source_as_of=NOW,
        instrument_ids=(),
        industry_tags=(),
        summary="research_thesis direction=up horizon_sessions=5",
    )
    with pytest.raises(ValueError, match="excluded"):
        recall.add(entry)

    malformed = {
        "schema_version": "market-impact.research-thesis.v1",
        "root_event_id": "earnings-root",
        "thesis_epoch": "epoch-malformed",
        "primary_horizon_sessions": 5,
        "base_case_direction": "up",
    }
    malformed_artifact = store.artifacts.put_json(malformed)
    malformed_entry = RecallProjectionEntry(
        root_event_id="earnings-root",
        thesis_epoch="epoch-malformed",
        source_kind="research_thesis",
        source_run_id="unsigned-malformed-run",
        source_artifact_hash=malformed_artifact.content_hash,
        source_as_of=NOW,
        instrument_ids=(),
        industry_tags=(),
        summary="research_thesis direction=up horizon_sessions=5",
    )
    with pytest.raises(ValueError, match="signed completed Run"):
        recall.add(malformed_entry)

    leaked_summary = _entry(store, _thesis(NOW, "epoch-summary"))
    leaked_summary = RecallProjectionEntry(
        root_event_id=leaked_summary.root_event_id,
        thesis_epoch=leaked_summary.thesis_epoch,
        source_kind=leaked_summary.source_kind,
        source_run_id=leaked_summary.source_run_id,
        source_artifact_hash=leaked_summary.source_artifact_hash,
        source_as_of=leaked_summary.source_as_of,
        instrument_ids=leaked_summary.instrument_ids,
        industry_tags=leaked_summary.industry_tags,
        summary="future outcome was positive",
    )
    with pytest.raises(ValueError, match="not derived"):
        recall.add(leaked_summary)

    safe = _entry(store, _thesis(NOW, "epoch-safe", text="x" * 3000))
    recall.add(safe)
    with pytest.raises(ValueError, match="context token bound"):
        recall.read_prior_decisions((safe.recall_id,), as_of=NOW, max_tokens=500)


def test_recall_tools_hide_cutoff_identity_and_separate_search_from_read(tmp_path: Path) -> None:
    store = LocalDataSnapshotStore(tmp_path / "harness")
    recall = DecisionRecallProjection(
        tmp_path / "recall.sqlite3",
        artifact_store=store.artifacts,
        journal=RunJournal.authoritative(store),
    )
    entry = _entry(store, _thesis(NOW, "epoch-safe"))
    recall.add(entry)
    current, search, read = decision_recall_tools(
        recall, as_of=NOW, current_root_event_id="earnings-root"
    )

    assert current.input_schema == {
        "type": "object",
        "properties": {},
        "additionalProperties": False,
    }
    assert "as_of" not in search.input_schema["properties"]  # type: ignore[operator]
    assert "root_event_id" not in read.input_schema["properties"]  # type: ignore[operator]

    async def invoke(
        handler: Callable[[dict[str, object]], Awaitable[object]],
        payload: dict[str, object],
    ) -> object:
        return await handler(payload)

    current_value = asyncio.run(invoke(current.handler, {}))
    search_value = asyncio.run(invoke(search.handler, {"instrument_id": "ETF-1"}))
    read_value = asyncio.run(invoke(read.handler, {"ids": [entry.recall_id]}))
    assert current_value["current_thesis"]["recall_id"] == entry.recall_id  # type: ignore[index]
    assert search_value["evidence"] is False  # type: ignore[index]
    assert read_value["evidence"] is False  # type: ignore[index]


def test_recall_tools_enforce_scope_without_owning_run_cumulative_context(tmp_path: Path) -> None:
    async def invoke(
        handler: Callable[[dict[str, object]], Awaitable[object]], arguments: dict[str, object]
    ) -> object:
        return await handler(arguments)

    store = LocalDataSnapshotStore(tmp_path / "harness")
    recall = DecisionRecallProjection(
        tmp_path / "recall.sqlite3",
        artifact_store=store.artifacts,
        journal=RunJournal.authoritative(store),
    )
    entry = _entry(store, _thesis(NOW, "allowed"))
    recall.add(entry)
    current, search, read = decision_recall_tools(
        recall, as_of=NOW, current_root_event_id="earnings-root", allowed_source_run_ids=frozenset()
    )
    assert asyncio.run(invoke(current.handler, {})) == {"current_thesis": None}
    assert asyncio.run(invoke(search.handler, {})) == {"hits": [], "evidence": False}
    with pytest.raises(PermissionError, match="account/arm scope"):
        asyncio.run(invoke(read.handler, {"ids": [entry.recall_id]}))
    _, _, read = decision_recall_tools(
        recall,
        as_of=NOW,
        current_root_event_id="earnings-root",
        allowed_source_run_ids=frozenset({entry.source_run_id}),
    )
    # Cumulative allowance belongs to the pi Run, never this reusable projection.
    for _ in range(30):
        assert asyncio.run(invoke(read.handler, {"ids": [entry.recall_id]}))
