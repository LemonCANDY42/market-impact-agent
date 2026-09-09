"""The new model contract selects authority; deterministic sizing retains concrete identity."""

import asyncio
from dataclasses import replace
from decimal import Decimal
from pathlib import Path
from typing import cast

import pytest

from market_impact_agent.agent_schema import validate_agent_contract
from market_impact_agent.pi_runtime import PiRuntimeProvider
from market_impact_agent.portfolio_decision import PortfolioAction
from market_impact_agent.portfolio_review import (
    PortfolioReviewAuthority,
    PortfolioReviewCandidate,
    parse_portfolio_proposal_v6,
    portfolio_prompt_projection_v2,
)

from .test_autonomous_paper import TARGET
from .test_portfolio_review import (
    NativePortfolio,
    _answer,  # pyright: ignore[reportPrivateUsage]
    _setup,  # pyright: ignore[reportPrivateUsage]
    native_portfolio,  # noqa: F401  # pyright: ignore[reportUnusedImport]
)


def answer(action: str) -> dict[str, object]:
    value = _answer(action)
    for field in ("instrument_id", "venue", "instrument_class", "direction", "horizon_band"):
        value.pop(field, None)
    value["evidence_refs"] = [
        "Current account cash and positions",
        "Current portfolio exposure and risk limits",
    ]
    if action != "hold":
        value["target_ref"] = f"candidate:{TARGET}" if action == "open" else f"holding:{TARGET}:buy"
    return value


@pytest.mark.parametrize("action", ["open", "increase", "reduce", "hold", "rotate"])
def test_v6_real_producer_binds_and_sizes(
    tmp_path: Path,
    native_portfolio: NativePortfolio,  # noqa: F811
    action: str,
) -> None:
    profile, answers, spawns = native_portfolio
    old, inputs, _, _, clock, *_ = _setup(
        tmp_path,
        action=PortfolioAction.REDUCE
        if action in {"increase", "reduce", "rotate"}
        else PortfolioAction.OPEN,
    )
    if action != "hold":
        inputs[0] = replace(
            inputs[0],
            admitted_candidates=(
                PortfolioReviewCandidate(TARGET, "ARCX", "exchange_traded_fund", ("a" * 64,)),
                PortfolioReviewCandidate("OTHER.ARCX", "ARCX", "exchange_traded_fund", ("b" * 64,)),
            ),
        )
    authority = PortfolioReviewAuthority(
        old.store,
        input_source=lambda: inputs[0],
        exposure_authority=old.exposure_authority,
        clock=lambda: clock[0],
        proposal_version="v6",
    )
    answers[0] = answer(action)
    if action == "hold":
        answers[0]["transmission"] = []
    if action == "rotate":
        answers[0].update(
            target_ref="candidate:OTHER.ARCX", rotation_source_ref=f"holding:{TARGET}:buy"
        )
    if action == "increase":
        answers[0]["target_gross_exposure_ratio"] = "0.80"
    if action == "reduce":
        answers[0]["transmission"] = " Existing exposure amplifies market losses. "
        answers[0]["invalidation_conditions"] = "Market recovery invalidates the reduction."
    result = asyncio.run(
        authority.review_account(run_id="v6-" + action, provider=PiRuntimeProvider(profile))
    )
    assert result["status"] == "completed", result
    assert (
        validate_agent_contract(result["proposal"], "agent-portfolio-proposal-v6.schema.json") == ()
    )
    proposal = cast(dict[str, object], result["proposal"])
    assert proposal["horizon_band"] == "immediate"
    assert proposal["evidence_refs"] == ["account_state", "exposure_view"]
    if action == "reduce":
        assert proposal["transmission"] == ["Existing exposure amplifies market losses."]
        assert proposal["invalidation_conditions"] == ["Market recovery invalidates the reduction."]
        assert {"path": "transmission", "operation": "wrap_singleton_narrative"} in cast(
            list[dict[str, str]], result["text_normalizations"]
        )
    if action != "hold":
        admission = authority.execution_admission("v6-" + action)
        assert admission.order.instrument_id == TARGET
        assert admission.order.quantity == Decimal(
            "400" if action == "open" else "600" if action == "rotate" else "200"
        )
        assert proposal["instrument_class"] == "exchange_traded_fund"
    assert authority.replay("v6-" + action) == result
    assert len(spawns) == 1
    if action == "open":
        inputs[0] = replace(
            inputs[0],
            admitted_candidates=(
                PortfolioReviewCandidate(TARGET, "ARCX", "exchange_traded_fund", ("c" * 64,)),
            ),
        )
        with pytest.raises(PermissionError, match="input authority changed"):
            authority.execution_admission("v6-open")


@pytest.mark.parametrize("action", ["hold", "reduce"])
def test_v6_evidence_labels_and_authorized_ids_bind_identically(
    tmp_path: Path, action: str
) -> None:
    _, inputs, *_ = _setup(tmp_path, action=PortfolioAction.REDUCE)
    binding: dict[str, object] = {
        "inputs": inputs[0].to_dict(),
        "research": [],
        "research_theses": [],
    }
    evidence_ids = frozenset({"account_state", "exposure_view", "position_snapshot"})
    labeled = {
        **answer(action),
        "counterevidence_refs": ["Current position quantities and concentration"],
    }
    exact_ids = {
        **labeled,
        "evidence_refs": ["account_state", "exposure_view"],
        "counterevidence_refs": ["position_snapshot"],
    }
    proposals = [
        parse_portfolio_proposal_v6(
            value, binding_hash="a" * 64, evidence_ids=evidence_ids, binding=binding
        )
        for value in (labeled, exact_ids)
    ]
    assert proposals[0] == proposals[1]
    assert proposals[0].evidence_refs == ("account_state", "exposure_view")
    assert proposals[0].counterevidence_refs == ("position_snapshot",)


def test_v6_ambiguous_label_and_id_is_refused(tmp_path: Path) -> None:
    _, inputs, *_ = _setup(tmp_path, action=PortfolioAction.REDUCE)
    conflicting_id = "Current account cash and positions"
    binding: dict[str, object] = {
        "inputs": inputs[0].to_dict(),
        "research": [{"run_id": conflicting_id}],
        "research_theses": [],
    }
    with pytest.raises(ValueError, match="ambiguous portfolio evidence choice"):
        parse_portfolio_proposal_v6(
            answer("hold"),
            binding_hash="a" * 64,
            evidence_ids=frozenset({"account_state", "exposure_view", conflicting_id}),
            binding=binding,
        )


def test_v6_unknown_citation_identity_and_rotation_source_refused(tmp_path: Path) -> None:
    _, inputs, *_ = _setup(tmp_path, action=PortfolioAction.REDUCE)
    binding: dict[str, object] = {
        "inputs": inputs[0].to_dict(),
        "research": [],
        "research_theses": [],
    }
    projection = portfolio_prompt_projection_v2(binding)
    assert f"holding:{TARGET}:buy" in cast(dict[str, object], projection["targets"])
    for patch in (
        {"evidence_refs": ["invented fact"]},
        {"evidence_refs": ["account_state", "invented-id"]},
        {"counterevidence_refs": ["invented-id"]},
        {"venue": "ARCX"},
        {"target_ref": "holding:invented:buy"},
        {"requested_action": "rotate", "rotation_source_ref": "candidate:invented"},
    ):
        with pytest.raises(ValueError):
            parse_portfolio_proposal_v6(
                {**answer("reduce"), **patch},
                binding_hash="a" * 64,
                evidence_ids=frozenset({"account_state", "exposure_view"}),
                binding=binding,
            )


def test_opportunity_changed_before_freeze_is_not_started(
    tmp_path: Path,
    native_portfolio: NativePortfolio,  # noqa: F811
) -> None:
    profile, answers, spawns = native_portfolio
    old, inputs, _, _, clock, *_ = _setup(tmp_path)
    authority = PortfolioReviewAuthority(
        old.store,
        input_source=lambda: inputs[0],
        exposure_authority=old.exposure_authority,
        clock=lambda: clock[0],
        proposal_version="v6",
    )
    provider = PiRuntimeProvider(profile)
    opportunity = authority.review_opportunity_id(provider=provider)
    inputs[0] = replace(
        inputs[0],
        admitted_candidates=(
            PortfolioReviewCandidate("OTHER.ARCX", "ARCX", "exchange_traded_fund", ("b" * 64,)),
        ),
    )
    with pytest.raises(PermissionError, match="opportunity authority changed before freeze"):
        asyncio.run(authority.review_account(run_id=opportunity, provider=provider))
    with pytest.raises(KeyError):
        authority.journal.get_run(opportunity)
    assert spawns == []
    current = authority.review_opportunity_id(provider=provider)
    assert current != opportunity
    answers[0] = answer("hold")
    result = asyncio.run(authority.review_account(run_id=current, provider=provider))
    assert result["status"] == "completed"
    assert len(spawns) == 1


def test_research_only_candidate_completes_without_execution_qualification(
    tmp_path: Path,
    native_portfolio: NativePortfolio,  # noqa: F811
) -> None:
    profile, answers, _ = native_portfolio
    old, inputs, _, _, clock, *_ = _setup(tmp_path)
    inputs[0] = replace(
        inputs[0],
        admitted_candidates=(
            PortfolioReviewCandidate("OTHER.ARCX", "ARCX", "exchange_traded_fund", ("b" * 64,)),
        ),
    )
    authority = PortfolioReviewAuthority(
        old.store,
        input_source=lambda: inputs[0],
        exposure_authority=old.exposure_authority,
        clock=lambda: clock[0],
        proposal_version="v6",
    )
    answers[0] = {**answer("open"), "target_ref": "candidate:OTHER.ARCX"}
    result = asyncio.run(
        authority.review_account(run_id="unqualified-open", provider=PiRuntimeProvider(profile))
    )
    assert result["status"] == "completed"
    assert authority.replay("unqualified-open") == result
    with pytest.raises(PermissionError):
        authority.execution_admission("unqualified-open")


def test_singleton_narrative_policy_preserves_legacy_and_authority_boundaries(
    tmp_path: Path,
) -> None:
    _, inputs, *_ = _setup(tmp_path, action=PortfolioAction.REDUCE)
    binding: dict[str, object] = {
        "inputs": inputs[0].to_dict(),
        "research": [],
        "research_theses": [],
    }
    evidence_ids = frozenset({"account_state", "exposure_view", "position_snapshot"})
    value = {**answer("reduce"), "transmission": "One unchanged causal statement."}
    with pytest.raises(ValueError, match="references must be strings"):
        parse_portfolio_proposal_v6(
            value, binding_hash="a" * 64, binding=binding, evidence_ids=evidence_ids
        )
    binding["narrative_input_policy"] = "singleton-narrative-v1"
    for field, invalid in (
        ("transmission", " "),
        ("transmission", {"text": "do not unwrap objects"}),
        ("evidence_refs", "Current account cash and positions"),
        ("evidence_refs", ["invented evidence"]),
    ):
        with pytest.raises(ValueError):
            parse_portfolio_proposal_v6(
                {**value, field: invalid},
                binding_hash="a" * 64,
                binding=binding,
                evidence_ids=evidence_ids,
            )
    binding["narrative_input_policy"] = "unknown"
    with pytest.raises(PermissionError, match="narrative input policy"):
        parse_portfolio_proposal_v6(
            value, binding_hash="a" * 64, binding=binding, evidence_ids=evidence_ids
        )
