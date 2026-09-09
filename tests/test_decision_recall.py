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
    ResearchThesisV2,
    parse_research_thesis_v2,
)
from market_impact_agent.research_thesis_runtime import (
    ResearchThesisAuthority,
    ResearchThesisRunInputs,
    _injected_prior_reference,  # pyright: ignore[reportPrivateUsage]
    reopen_completed_research_thesis,
)
from market_impact_agent.runtime_store import RunJournal

from .test_agent_engine import FixtureProvider
from .test_pi_runtime import pi_profile
from .test_research_thesis_runtime import _repository  # pyright: ignore[reportPrivateUsage]

NOW = datetime(2026, 9, 4, 8, 0, tzinfo=UTC)


def _run_awaitable(value: Awaitable[object]) -> object:
    async def invoke() -> object:
        return await value

    return asyncio.run(invoke())


def test_injected_opinion_returns_verified_reference_without_charging_hidden_text(
    tmp_path: Path,
) -> None:
    store = LocalDataSnapshotStore(tmp_path / "harness")
    journal = RunJournal.authoritative(store)
    recall = DecisionRecallProjection(
        tmp_path / "recall.sqlite3", artifact_store=store.artifacts, journal=journal
    )
    first = _entry(store, _thesis(NOW, "unicode", "行业消息需要继续验证。" * 150))
    recall.rebuild((first,))
    _, prior = reopen_completed_research_thesis(
        journal=journal, artifact_store=store.artifacts, run_id=first.source_run_id
    )
    selected = store.artifacts.put_json({"prior_thesis": prior}).content_hash
    raw_tools = decision_recall_tools(
        recall,
        as_of=NOW,
        current_root_event_id="earnings-root",
        allowed_source_run_ids=frozenset({first.source_run_id}),
    )
    tools = {tool.name: _injected_prior_reference(tool, prior, selected) for tool in raw_tools}
    first_read = _run_awaitable(tools["read_current_thesis"].handler({}))
    assert first_read == _run_awaitable(tools["read_current_thesis"].handler({}))
    assert "already_supplied" in json.dumps(first_read)
    assert len(json.dumps(first_read).encode()) < 1000
    assert "行业消息" not in json.dumps(first_read, ensure_ascii=False)
    assert selected in json.dumps(first_read)
    older_read = _run_awaitable(tools["read_prior_decisions"].handler({"ids": [first.recall_id]}))
    assert "already_supplied" in json.dumps(older_read)
    # No exemption for another injected opinion or for the legacy tool version.
    other = {**prior, "run_id": "another-run", "thesis": {"different": True}}
    legacy = _run_awaitable(raw_tools[0].handler({}))
    assert (
        _run_awaitable(_injected_prior_reference(raw_tools[0], other, selected).handler({}))
        == legacy
    )
    assert "行业消息" in json.dumps(legacy, ensure_ascii=False)
    # The compact response still reopens source authority on every call.
    store.artifacts.get(first.source_artifact_hash, media_type="application/json").path.write_text(
        "{}"
    )
    with pytest.raises((ValueError, PermissionError)):
        _run_awaitable(tools["read_current_thesis"].handler({}))


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
    authored.pop("target_id", None)
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
                schema_version=(
                    "market-impact.research-thesis-inputs.v2"
                    if isinstance(thesis, ResearchThesisV2)
                    else "market-impact.research-thesis-inputs.v1"
                ),
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
        summary=(
            f"research_thesis direction={thesis.base_case_direction.value} "
            f"horizon_sessions={thesis.primary_horizon_sessions}"
        ),
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


def test_scope_precedes_limit_and_empty_scope_is_empty(tmp_path: Path) -> None:
    store = LocalDataSnapshotStore(tmp_path / "harness")
    journal = RunJournal.authoritative(store)
    projection = DecisionRecallProjection(
        tmp_path / "recall.sqlite3", artifact_store=store.artifacts, journal=journal
    )
    authorized = _entry(store, _thesis(NOW - timedelta(hours=2), "allowed"))
    newer = tuple(
        _entry(store, _thesis(NOW - timedelta(minutes=i), f"other-{i}")) for i in range(9)
    )
    projection.rebuild((authorized, *newer))
    scope = frozenset({authorized.source_run_id})
    hits = projection.search_prior_decisions(as_of=NOW, allowed_source_run_ids=scope, limit=1)
    assert hits == (authorized,)
    assert (
        projection.read_current_thesis(
            root_event_id="earnings-root", as_of=NOW, allowed_source_run_ids=scope
        )
        == authorized
    )
    tools = {
        item.name: item
        for item in decision_recall_tools(
            projection,
            as_of=NOW,
            current_root_event_id="earnings-root",
            allowed_source_run_ids=scope,
        )
    }
    assert authorized.recall_id in json.dumps(
        _run_awaitable(tools["read_current_thesis"].handler({}))
    )
    assert projection.search_prior_decisions(as_of=NOW, allowed_source_run_ids=frozenset()) == ()
    assert (
        projection.read_current_thesis(
            root_event_id="earnings-root", as_of=NOW, allowed_source_run_ids=frozenset()
        )
        is None
    )


def test_v2_unknown_recall_indexes_reads_and_reopens_signed_thesis(tmp_path: Path) -> None:
    store = LocalDataSnapshotStore(tmp_path / "harness")
    journal = RunJournal.authoritative(store)
    projection = DecisionRecallProjection(
        tmp_path / "recall.sqlite3", artifact_store=store.artifacts, journal=journal
    )
    authored = _thesis(NOW, "unknown-v2").core_dict()
    for key in ("schema_version", "root_event_id", "thesis_epoch", "as_of"):
        authored.pop(key)
    authored.update(
        {
            "base_case_direction": "unknown",
            "event_support": "unsupported",
            "expectations": "No event surprise is established.",
            "revision_conclusion": "Await event evidence.",
            "evidence_refs": [],
            "transmission": [],
            "typed_unknowns": ["No event-specific transmission evidence."],
        }
    )
    thesis = parse_research_thesis_v2(
        authored,
        root_event_id="earnings-root",
        thesis_epoch="unknown-v2",
        as_of=NOW,
        target_id="INDEX.ETF",
        evidence_ids=frozenset(),
    )
    entry = _entry(store, thesis)
    projection.rebuild((entry,))
    scoped = frozenset({entry.source_run_id})
    assert projection.search_prior_decisions(as_of=NOW, allowed_source_run_ids=scoped) == (entry,)
    tools = {
        item.name: item
        for item in decision_recall_tools(
            projection,
            as_of=NOW,
            current_root_event_id="earnings-root",
            allowed_source_run_ids=scoped,
        )
    }
    current = _run_awaitable(tools["read_current_thesis"].handler({}))
    assert '"base_case_direction": "unknown"' in json.dumps(current)
    assert '"target_id": "INDEX.ETF"' in json.dumps(current)
    reopened = projection.read_prior_decisions((entry.recall_id,), as_of=NOW)
    assert reopened[0].source == thesis.to_dict()
