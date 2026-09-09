# pyright: reportPrivateUsage=false
"""Native pi cache proof, cap feedback and immutable source identity acceptance."""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.data_inputs import DataPITLane, LocalDataSnapshotStore
from market_impact_agent.historical_ashare_inputs import (
    HistoricalAShareInputs,
    ModeledHistoricalPolicy,
)
from market_impact_agent.model_budget import ModelBudget
from market_impact_agent.native_candidate_proof import prove_native_candidates
from market_impact_agent.on_demand_research import OnDemandResearch, ResearchSourceTemplate
from market_impact_agent.pi_deployment import PiRuntimePermit
from market_impact_agent.pi_runtime import PiRuntimeProvider, runtime_identity
from market_impact_agent.prospective_discovery_runtime import _CandidateSuccessor
from market_impact_agent.research_acquisition_runtime import analyze_with_acquisition
from market_impact_agent.research_thesis_runtime import (
    ResearchThesisAuthority,
    ResearchThesisRunInputs,
)
from market_impact_agent.runtime_store import RunJournal
from market_impact_agent.tushare_observation import (
    TushareObservationProvider,
    load_tushare_observation_source,
)
from tests.test_ashare_security_qualification import accepted_policy
from tests.test_pi_runtime import pi_profile
from tests.test_research_thesis_runtime import _answer, _repository
from tests.test_tushare_observation import TOKEN, FakeTransport, _response


@pytest.mark.parametrize(
    "count,conflict,mode",
    [
        (0, False, "current"),
        (1, False, "current"),
        (5, False, "current"),
        (6, False, "current"),
        (1, True, "current"),
        (1, False, "modeled_cache"),
        (1, False, "modeled_acquisition"),
        (1, False, "future_cache"),
        (1, False, "future_acquisition"),
    ],
)
def test_native_cache_candidates_and_sixth_feedback(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, count: int, conflict: bool, mode: str
) -> None:
    cutoff = datetime(2026, 9, 5, 4, 7, tzinfo=UTC)
    modeled = mode != "current"
    historical_cutoff = datetime(2025, 1, 3, 4, 7, tzinfo=UTC) if modeled else cutoff
    store = LocalDataSnapshotStore(tmp_path / "authority")
    journal = RunJournal.authoritative(store)
    journal.start_run(
        run_id="budget", config_hash=canonical_hash("candidate-proof"), created_at=cutoff
    )
    budget = ModelBudget(journal, "budget", 20, 10_000_000)
    config = load_tushare_observation_source(
        Path("examples/providers/tushare-observation-stock-basic-v1.json")
    )
    symbols = [f"{i:06d}.SZ" for i in range(1, count + 1)]
    cache_symbols = [*symbols, "510300.SH"]
    responses: list[dict[str, object]] = []
    for symbol in cache_symbols:
        row = {
            "ts_code": symbol,
            "symbol": symbol[:6],
            "name": "Synthetic company",
            "exchange": "SSE" if symbol.endswith(".SH") else "SZSE",
            "list_status": "L",
            "list_date": "20260101" if mode.startswith("future") else "20100101",
        }
        rows: list[list[object]] = [[row.get(field) for field in config.fields]]
        if conflict and symbol == symbols[0]:
            rows.append(
                [{**row, "name": "Conflicting company"}.get(field) for field in config.fields]
            )
        responses.append(_response(config.fields, rows))
    transport = FakeTransport(responses)
    source = TushareObservationProvider(TOKEN, (config,), transport=transport, clock=lambda: cutoff)
    template = ResearchSourceTemplate.from_tushare(source, config.source_id)
    profile = pi_profile()
    monkeypatch.setenv(profile.credential_env, "synthetic-key")

    def installed(_root: Path) -> PiRuntimePermit:
        return PiRuntimePermit(
            canonical_hash(runtime_identity()), (profile.route_identity,), "fixture"
        )

    monkeypatch.setattr("market_impact_agent.pi_deployment.installed_permit", installed)
    original = asyncio.create_subprocess_exec

    async def spawn(program: str, *args: str, **kwargs: Any):
        # A holding and duplicate reads do not consume new-security slots.
        calls = (
            ["510300.SH"] if count == 6 else ["510300.SH", symbols[0]] if symbols else []
        ) + symbols
        if modeled:
            calls = symbols
        kwargs["env"]["EXPECT_MODELED"] = "1" if modeled else "0"
        kwargs["env"]["CANDIDATE_SYMBOLS"] = json.dumps(calls)
        kwargs["env"]["CANDIDATE_ANSWER"] = json.dumps(_answer())
        kwargs["env"]["EXPECT_CANDIDATE_LIMIT"] = "1" if count == 6 else "0"
        return await original(
            program,
            "--import",
            str(Path(__file__).with_name("native_candidates_network.mjs")),
            *args,
            **kwargs,
        )

    monkeypatch.setattr(asyncio, "create_subprocess_exec", spawn)

    async def scenario() -> None:
        seed = OnDemandResearch(
            store=store,
            parent_budget=budget,
            episode_deadline=cutoff + timedelta(hours=1),
            run_id="seed",
            cutoff=cutoff,
            pit_lane=DataPITLane.PROSPECTIVE,
            templates=(template,),
            clock=lambda: cutoff,
        )
        for symbol in () if mode.endswith("acquisition") else cache_symbols:
            await seed.request("lookup_company_profile", {"ts_code": symbol})
        results = await seed.fulfill_pending()
        frozen = seed.successor_input(results)[1] if results else None
        historical = (
            HistoricalAShareInputs(
                store=store,
                snapshot_ids=() if frozen is None else tuple(frozen.authorized_snapshot_ids),
                rule_artifact_hashes=(),
                qualification_policy=accepted_policy(store, cutoff),
                policy=ModeledHistoricalPolicy(
                    "native-candidate-test-v1",
                    Decimal("0.01"),
                    research_projection="dynamic_ashare_sources_v1",
                ),
            )
            if modeled
            else None
        )
        acquisition = OnDemandResearch(
            store=store,
            parent_budget=budget,
            episode_deadline=seed.deadline,
            run_id="native",
            cutoff=historical_cutoff,
            pit_lane=DataPITLane.MODELED if modeled else seed.pit_lane,
            historical_inputs=historical,
            templates=(template,),
            frozen_input=frozen,
            clock=lambda: cutoff,
        )
        authority = ResearchThesisAuthority(
            store,
            experiment_id="proof",
            arm_id="model",
            account_scope="account",
            clock=lambda: cutoff,
        )
        inputs = ResearchThesisRunInputs(
            _repository("510300.SH", at=historical_cutoff), "510300.SH", "epoch", frozenset({5})
        )
        provider = PiRuntimeProvider(profile, budget=budget)
        try:
            transform = _CandidateSuccessor(
                authority, inputs, current_sources=True, held_targets=("510300.SH",)
            )
            result = await analyze_with_acquisition(
                authority=authority,
                provider=provider,
                inputs=inputs,
                acquisition=acquisition,
                maximum_runs=2,
                successor_transform=transform,
                successor_transform_id=transform.policy,
                candidate_excluded_targets=("510300.SH",),
            )
            if conflict or mode.startswith("future"):
                assert not result.final_inputs.candidate_proofs
                assert not prove_native_candidates(
                    authority, acquisition, result.acquisitions, excluded_targets=("510300.SH",)
                )
                assert len(result.run_ids) == 1
                return
            assert result.status == "completed", result.terminal
            if count == 1:
                usage = budget.summary()
                reopened_transform = _CandidateSuccessor(
                    authority, inputs, current_sources=True, held_targets=("510300.SH",)
                )
                reopened = await analyze_with_acquisition(
                    authority=authority,
                    provider=provider,
                    inputs=inputs,
                    acquisition=acquisition,
                    maximum_runs=2,
                    successor_transform=reopened_transform,
                    successor_transform_id=reopened_transform.policy,
                    candidate_excluded_targets=("510300.SH",),
                )
                assert reopened.terminal == result.terminal
                assert reopened.final_inputs.identity_dict() == result.final_inputs.identity_dict()
                assert budget.summary() == usage
            assert tuple(result.final_inputs.candidate_proofs) == tuple(symbols[:5])
            assert len(result.run_ids) == (2 if count else 1)
            proofs = prove_native_candidates(
                authority, acquisition, result.acquisitions, excluded_targets=("510300.SH",)
            )
            assert tuple(item.symbol for item in proofs) == tuple(symbols[:5])
            if mode.endswith("acquisition"):
                proofs = prove_native_candidates(
                    authority, acquisition, result.acquisitions, excluded_targets=("510300.SH",)
                )
                assert proofs and proofs[0].provenance["request_id"] is not None
            else:
                assert all(item.provenance["native_result_hash"] for item in proofs)
                assert all(item.provenance["request_id"] is None for item in proofs)
            if modeled:
                identity = proofs[0].provenance["modeled_identity"]
                assert isinstance(identity, dict) and identity["list_date"] == "20100101"
                assert "name" not in identity and "list_status" not in identity
                assert proofs[0].provenance["modeled_cutoff"] == historical_cutoff.isoformat()
                assert proofs[0].provenance["metadata_raw_record_hash"] in {
                    item.raw_content_hash for item in store.get(proofs[0].snapshot_id).observations
                }
            assert len(transport.requests) == (
                1 if mode.endswith("acquisition") else len(cache_symbols)
            )
            assert (
                prove_native_candidates(
                    authority, acquisition, result.acquisitions, excluded_targets=("510300.SH",)
                )
                == proofs
            )
        finally:
            await provider.close()

    asyncio.run(scenario())
