"""Entry provenance gates using real durable receipts and synthetic source transport."""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import cast

import pytest

from market_impact_agent import pi_runtime
from market_impact_agent import prospective_discovery_entry as entry
from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.data_inputs import DataPITLane, LocalDataSnapshotStore
from market_impact_agent.model_budget import ModelBudget
from market_impact_agent.model_provider import load_model_provider_profile
from market_impact_agent.on_demand_research import OnDemandResearch, ResearchSourceTemplate
from market_impact_agent.runtime_store import RunJournal
from market_impact_agent.tushare_observation import (
    TushareObservationProvider,
    load_tushare_observation_source,
)

from .test_tushare_observation import (
    RETRIEVED,
    TOKEN,
    FakeTransport,
    _response,  # pyright: ignore[reportPrivateUsage]
)

_PARAMS: dict[str, object] = {
    "start_date": "2026-08-28 15:00:00",
    "end_date": "2026-08-28 15:59:00",
}


@dataclass
class ReceiptFixture:
    root: Path
    acquisition: OnDemandResearch
    transport: FakeTransport
    binding_path: Path
    report_path: Path

    def prepare(self) -> dict[str, object]:
        return entry.prepare_prospective_discovery(
            study_root=self.root,
            receipt_binding_path=self.binding_path,
            receipt_report_path=self.report_path,
            receipt_episode_id="received-news",
            receipt_run_id="received-news.research",
            templates=tuple(self.acquisition.templates.values()),
        )


@pytest.fixture
def receipts(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> ReceiptFixture:
    config = load_tushare_observation_source(
        Path("examples/providers/tushare-observation-news-v1.json")
    )
    rows: list[list[object]] = [
        [f"2026-08-28 15:{index:02d}:00", f"Synthetic content {index}", f"Story {index}", "test"]
        for index in range(30)
    ]
    transport = FakeTransport([_response(config.fields, rows), _response(config.fields, rows)])
    provider = TushareObservationProvider(
        TOKEN, (config,), transport=transport, clock=lambda: RETRIEVED
    )
    store = LocalDataSnapshotStore(tmp_path / "authority")
    journal = RunJournal.authoritative(store)
    journal.start_run(run_id="study", config_hash=canonical_hash("study"), created_at=RETRIEVED)
    budget = ModelBudget(journal, "study", 10, 40_000_000)
    acquisition = OnDemandResearch(
        store=store,
        parent_budget=budget,
        episode_id="received-news",
        episode_deadline=RETRIEVED + timedelta(hours=1),
        run_id="received-news.research",
        cutoff=RETRIEVED - timedelta(minutes=1),
        pit_lane=DataPITLane.PROSPECTIVE,
        templates=(ResearchSourceTemplate.from_tushare(provider, config.source_id),),
        clock=lambda: RETRIEVED,
    )

    async def collect() -> tuple[dict[str, object], ...]:
        await acquisition.descriptors()[0].handler(_PARAMS)
        return tuple(item.to_dict() for item in await acquisition.fulfill_pending())

    results = asyncio.run(collect())
    assert len(results) == 1 and results[0]["status"] == "fulfilled"
    binding_path, report_path = tmp_path / "binding.json", tmp_path / "receipts.json"
    binding_path.write_text(
        json.dumps(
            {
                "cutoff": acquisition.cutoff.isoformat(),
                "deadline": acquisition.deadline.isoformat(),
            }
        )
    )
    report_path.write_text(json.dumps({"results": results}))

    def shared_budget(*_: object) -> ModelBudget:
        return budget

    def study_registration(_: Path) -> dict[str, object]:
        return {
            "registration_id": "registered-study",
            "model_profiles": [
                {
                    "provider_profile_hash": load_model_provider_profile(
                        Path(f"examples/providers/pi-cpa-{name}-v2.json")
                    ).profile_hash
                }
                for name in ("luna-max", "terra-high", "sol-high")
            ],
        }

    monkeypatch.setattr(entry, "study_budget", shared_budget)
    monkeypatch.setattr(entry, "load_prepared_continuous_registration", study_registration)
    monkeypatch.setattr(
        entry, "discovery_source_templates", lambda: tuple(acquisition.templates.values())
    )

    def forbidden_pi(*_: object, **__: object) -> None:
        raise AssertionError("Pi must not be constructed during preparation or failed validation")

    monkeypatch.setattr(pi_runtime, "PiRuntimeProvider", forbidden_pi)
    return ReceiptFixture(tmp_path, acquisition, transport, binding_path, report_path)


def test_latest24_compact_projection_reopens_actual_receipts_without_fetch(
    receipts: ReceiptFixture,
) -> None:
    before = len(receipts.transport.requests)
    prepared = receipts.prepare()
    assert receipts.prepare() == prepared
    assert len(receipts.transport.requests) == before == 1
    assert receipts.acquisition.budget.summary()["physical_requests"] == 0
    assert prepared["model_calls"] == 0
    assert prepared["cutoff"] == RETRIEVED.isoformat()
    results = cast(dict[str, object], json.loads(receipts.report_path.read_text()))
    snapshot_id = str(cast(list[dict[str, object]], results["results"])[0]["snapshot_id"])
    assert prepared["snapshot_ids"] == [snapshot_id]
    store = receipts.acquisition.store
    artifact = cast(
        dict[str, object], store.artifacts.read_json(str(prepared["research_artifact_hash"]))
    )
    documents = cast(dict[str, dict[str, object]], artifact["documents"])
    snapshot = store.get(snapshot_id)
    chosen = sorted(
        snapshot.observations,
        key=lambda item: (item.times.published_at or item.times.occurred_at, item.observation_id),
        reverse=True,
    )[:24]
    assert set(documents) == {item.observation_id for item in chosen}
    for observation in chosen:
        assert documents[observation.observation_id] == {
            "snapshot_id": snapshot_id,
            "observation_id": observation.observation_id,
            "raw_content_hash": observation.raw_content_hash,
            "published_at": observation.times.published_at.isoformat()
            if observation.times.published_at
            else None,
            "retrieved_at": RETRIEVED.isoformat(),
            "record": observation.normalized_payload["record"],
        }
    assert len(documents) == 24
    assert all(
        cast(dict[str, object], item.normalized_payload["record"])["title"] != "Story 0"
        for item in chosen
    )


@pytest.mark.parametrize("kind", ["forged", "sibling"])
def test_preparation_rejects_forged_or_sibling_completion(
    receipts: ReceiptFixture, kind: str
) -> None:
    report = cast(dict[str, object], json.loads(receipts.report_path.read_text()))
    if kind == "forged":
        cast(list[dict[str, object]], report["results"])[0]["request_id"] = "forged-request"
    else:
        acquisition = receipts.acquisition
        sibling = OnDemandResearch(
            store=acquisition.store,
            parent_budget=acquisition.budget,
            episode_id="sibling",
            episode_deadline=acquisition.deadline,
            run_id="sibling.research",
            cutoff=acquisition.cutoff,
            pit_lane=acquisition.pit_lane,
            templates=tuple(acquisition.templates.values()),
            clock=lambda: RETRIEVED,
        )

        async def collect() -> tuple[dict[str, object], ...]:
            await sibling.descriptors()[0].handler(_PARAMS)
            return tuple(item.to_dict() for item in await sibling.fulfill_pending())

        report["results"] = asyncio.run(collect())
    receipts.report_path.write_text(json.dumps(report))
    before = len(receipts.transport.requests)
    with pytest.raises(ValueError, match="parent durable completion"):
        receipts.prepare()
    assert len(receipts.transport.requests) == before


@pytest.mark.parametrize(
    "kind", ["private_registration", "registration_cas", "research_cas", "study_parent"]
)
def test_runner_refuses_changed_authority_before_pi(
    receipts: ReceiptFixture,
    monkeypatch: pytest.MonkeyPatch,
    kind: str,
) -> None:
    prepared = receipts.prepare()
    registration_path = receipts.root / "prepared.json"
    registration_path.write_text(json.dumps(prepared))
    if kind == "private_registration":
        registration_path.write_text(
            json.dumps({**prepared, "cutoff": "2026-09-01T00:00:00+00:00"})
        )
    elif kind == "study_parent":

        def sibling_registration(_: Path) -> dict[str, object]:
            return {"registration_id": "sibling-study"}

        monkeypatch.setattr(entry, "load_prepared_continuous_registration", sibling_registration)
    else:
        digest = str(
            prepared["artifact_hash" if kind == "registration_cas" else "research_artifact_hash"]
        )
        (receipts.acquisition.store.artifacts.root / digest).write_text('{"tampered":true}')
    before = len(receipts.transport.requests)
    with pytest.raises((ValueError, PermissionError), match=r"identity|parent authority"):
        asyncio.run(
            entry.run_prepared_prospective_discovery(
                study_root=receipts.root, registration_path=registration_path
            )
        )
    assert len(receipts.transport.requests) == before
    assert receipts.acquisition.budget.summary()["physical_requests"] == 0
