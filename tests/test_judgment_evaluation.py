from datetime import UTC, datetime, timedelta
from decimal import Decimal
from hashlib import sha256
from pathlib import Path

import pytest

from market_impact_agent.agent_contracts import (
    CandidateDirection,
    CandidateImpact,
    EvidencePack,
    EvidenceReference,
    JudgmentArtifact,
    JudgmentDecision,
    JudgmentProposal,
)
from market_impact_agent.agent_schema import validate_agent_contract
from market_impact_agent.judgment_evaluation import (
    JudgmentEvaluationBandSpecification,
    JudgmentEvaluationPolicyCatalog,
    JudgmentEvaluationPolicyCatalogStore,
    JudgmentEvaluationTolerancePolicy,
    evaluate_judgment_band,
    judgment_evaluation_band_from_dict,
    judgment_evaluation_result_from_dict,
)
from market_impact_agent.research import EvidenceTier, TransmissionDirectness

NOW = datetime(2026, 8, 30, 8, tzinfo=UTC)


def digest(value: str) -> str:
    return sha256(value.encode()).hexdigest()


def artifact() -> JudgmentArtifact:
    pack = EvidencePack.build(
        event_id="event-1",
        as_of=NOW,
        research_question="What is the bounded impact?",
        evidence=(
            EvidenceReference(
                evidence_id="event-fact",
                claim_id="event-fact",
                source_ref="official://event",
                source_tier=EvidenceTier.OFFICIAL,
                available_at=NOW - timedelta(minutes=1),
                content_hash=digest("event"),
                summary="An official event occurred.",
            ),
        ),
        pattern_packs=(),
        allowed_targets=("sector-index",),
    )
    proposal = JudgmentProposal(
        event_id="event-1",
        decision=JudgmentDecision.PROPOSE,
        summary="The event supports a bounded positive impact.",
        transmission_steps=(),
        candidates=(
            CandidateImpact(
                target_id="sector-index",
                direction=CandidateDirection.UP,
                horizon_sessions=5,
                directness=TransmissionDirectness.DIRECT,
                confidence=0.7,
                thesis="The event supports the sector.",
                evidence_refs=("event-fact",),
                counterevidence_refs=(),
                invalidation_conditions=("the event is reversed",),
            ),
        ),
        blockers=(),
        unresolved_questions=(),
        stopped_reason="The registered evidence requirement passed.",
        decision_confidence=0.65,
    )
    return JudgmentArtifact.build(
        run_id="run-1",
        evidence_pack_id=pack.pack_id,
        provider_id="fixture-provider",
        model="fixture-model",
        runtime_config_hash=digest("runtime"),
        prompt_hash=digest("prompt"),
        skill_hashes=(digest("skill"),),
        tool_manifest_hashes=(),
        tool_surface_hash=digest("surface"),
        mcp_server_hashes=(),
        context_estimator_id="counter",
        compactor_id="compactor",
        journal_hash=digest("journal"),
        transcript_hash=digest("transcript"),
        raw_response_hash=digest("response"),
        started_at=NOW,
        finished_at=NOW + timedelta(seconds=1),
        proposal=proposal,
    )


def policy() -> JudgmentEvaluationTolerancePolicy:
    return JudgmentEvaluationTolerancePolicy.build(
        policy_name="sector-event-band",
        policy_version="1.0.0",
        maximum_latest_horizon_sessions=20,
        maximum_horizon_span_sessions=10,
        maximum_return_band_width=Decimal("0.15"),
        maximum_volatility_band_width=Decimal("0.40"),
        maximum_adverse_excursion=Decimal("0.10"),
        price_basis="as_of_adjusted_total_return",
        volatility_basis="annualized_daily_total_return_volatility",
    )


def policy_store(
    tmp_path: Path,
    *,
    registered_at: datetime = NOW - timedelta(days=1),
) -> JudgmentEvaluationPolicyCatalogStore:
    return JudgmentEvaluationPolicyCatalogStore(
        tmp_path / f"policy-{registered_at.timestamp()}.sqlite3",
        clock=lambda: registered_at,
    )


def specification(
    selected_artifact: JudgmentArtifact,
    store: JudgmentEvaluationPolicyCatalogStore,
) -> JudgmentEvaluationBandSpecification:
    catalog = store.register((policy(),))
    return JudgmentEvaluationBandSpecification.build(
        registered_at=NOW + timedelta(seconds=2),
        outcome_open_not_before=NOW + timedelta(days=7),
        policy_catalog=catalog,
        policy_registration_authority=store,
        tolerance_policy_id=catalog.policies[0].policy_id,
        artifact=selected_artifact,
        target_id="sector-index",
        earliest_horizon_sessions=3,
        latest_horizon_sessions=7,
        terminal_return_lower=Decimal("0.01"),
        terminal_return_upper=Decimal("0.12"),
        realized_volatility_lower=Decimal("0.10"),
        realized_volatility_upper=Decimal("0.45"),
        maximum_adverse_excursion=Decimal("0.08"),
        price_basis="as_of_adjusted_total_return",
        volatility_basis="annualized_daily_total_return_volatility",
    )


def test_precommitted_tolerant_evaluation_is_bounded_and_observational(
    tmp_path: Path,
) -> None:
    selected_artifact = artifact()
    store = policy_store(tmp_path)
    selected_specification = specification(selected_artifact, store)
    result = evaluate_judgment_band(
        specification=selected_specification,
        artifact=selected_artifact,
        policy_registration_authority=store,
        evaluated_at=NOW + timedelta(days=8),
        outcome_hash=digest("outcome"),
        horizon_sessions=5,
        realized_terminal_return=Decimal("0.06"),
        realized_volatility=Decimal("0.30"),
        adverse_excursion=Decimal("0.04"),
    )

    assert result.broadly_correct is True
    assert selected_specification.to_dict()["changes_signal_or_execution_admission"] is False
    assert result.to_dict()["changes_signal_or_execution_admission"] is False
    assert not validate_agent_contract(
        selected_specification.to_dict(),
        "judgment-evaluation-band-specification.schema.json",
    )
    assert not validate_agent_contract(result.to_dict(), "judgment-evaluation-result.schema.json")
    assert (
        judgment_evaluation_band_from_dict(selected_specification.to_dict())
        == selected_specification
    )
    assert judgment_evaluation_result_from_dict(result.to_dict()) == result


def test_tolerance_cannot_be_widened_after_outcome_or_reward_wrong_direction(
    tmp_path: Path,
) -> None:
    selected_artifact = artifact()
    store = policy_store(tmp_path)
    catalog = store.register((policy(),))
    with pytest.raises(ValueError, match="return band exceeds"):
        JudgmentEvaluationBandSpecification.build(
            registered_at=NOW + timedelta(seconds=2),
            outcome_open_not_before=NOW + timedelta(days=7),
            policy_catalog=catalog,
            policy_registration_authority=store,
            tolerance_policy_id=catalog.policies[0].policy_id,
            artifact=selected_artifact,
            target_id="sector-index",
            earliest_horizon_sessions=1,
            latest_horizon_sessions=20,
            terminal_return_lower=Decimal("-1"),
            terminal_return_upper=Decimal("1"),
            realized_volatility_lower=Decimal("0"),
            realized_volatility_upper=Decimal("1"),
            maximum_adverse_excursion=Decimal("1"),
            price_basis="as_of_adjusted_total_return",
            volatility_basis="annualized_daily_total_return_volatility",
        )

    selected_specification = specification(selected_artifact, store)
    with pytest.raises(ValueError, match="cannot open before"):
        evaluate_judgment_band(
            specification=selected_specification,
            artifact=selected_artifact,
            policy_registration_authority=store,
            evaluated_at=NOW + timedelta(days=1),
            outcome_hash=digest("early-outcome"),
            horizon_sessions=5,
            realized_terminal_return=Decimal("0.06"),
            realized_volatility=Decimal("0.30"),
            adverse_excursion=Decimal("0.04"),
        )

    wrong_direction = evaluate_judgment_band(
        specification=selected_specification,
        artifact=selected_artifact,
        policy_registration_authority=store,
        evaluated_at=NOW + timedelta(days=8),
        outcome_hash=digest("wrong-direction"),
        horizon_sessions=5,
        realized_terminal_return=Decimal("-0.02"),
        realized_volatility=Decimal("0.30"),
        adverse_excursion=Decimal("0.04"),
    )
    assert wrong_direction.direction_passed is False
    assert wrong_direction.broadly_correct is False


def test_tolerance_policy_is_pre_run_registered_and_system_bounded(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="horizon exceeds the system bound"):
        JudgmentEvaluationTolerancePolicy.build(
            policy_name="unbounded-band",
            policy_version="1.0.0",
            maximum_latest_horizon_sessions=10_000,
            maximum_horizon_span_sessions=10_000,
            maximum_return_band_width=Decimal("100"),
            maximum_volatility_band_width=Decimal("100"),
            maximum_adverse_excursion=Decimal("1"),
            price_basis="as_of_adjusted_total_return",
            volatility_basis="annualized_daily_total_return_volatility",
        )

    selected_artifact = artifact()
    selected_policy = policy()
    late_store = policy_store(tmp_path, registered_at=NOW + timedelta(milliseconds=1))
    late_catalog = late_store.register((selected_policy,))
    with pytest.raises(ValueError, match="before the Agent run"):
        JudgmentEvaluationBandSpecification.build(
            registered_at=NOW + timedelta(seconds=2),
            outcome_open_not_before=NOW + timedelta(days=7),
            policy_catalog=late_catalog,
            policy_registration_authority=late_store,
            tolerance_policy_id=selected_policy.policy_id,
            artifact=selected_artifact,
            target_id="sector-index",
            earliest_horizon_sessions=3,
            latest_horizon_sessions=7,
            terminal_return_lower=Decimal("0.01"),
            terminal_return_upper=Decimal("0.12"),
            realized_volatility_lower=Decimal("0.10"),
            realized_volatility_upper=Decimal("0.45"),
            maximum_adverse_excursion=Decimal("0.08"),
            price_basis="as_of_adjusted_total_return",
            volatility_basis="annualized_daily_total_return_volatility",
        )


def test_backdated_unregistered_policy_catalog_cannot_evaluate(tmp_path: Path) -> None:
    selected_artifact = artifact()
    selected_policy = policy()
    forged = JudgmentEvaluationPolicyCatalog.build(
        registered_at=NOW - timedelta(days=1),
        policies=(selected_policy,),
    )
    authority = policy_store(tmp_path)

    with pytest.raises(ValueError, match="not durably registered"):
        JudgmentEvaluationBandSpecification.build(
            registered_at=NOW + timedelta(seconds=2),
            outcome_open_not_before=NOW + timedelta(days=7),
            policy_catalog=forged,
            policy_registration_authority=authority,
            tolerance_policy_id=selected_policy.policy_id,
            artifact=selected_artifact,
            target_id="sector-index",
            earliest_horizon_sessions=3,
            latest_horizon_sessions=7,
            terminal_return_lower=Decimal("0.01"),
            terminal_return_upper=Decimal("0.12"),
            realized_volatility_lower=Decimal("0.10"),
            realized_volatility_upper=Decimal("0.45"),
            maximum_adverse_excursion=Decimal("0.08"),
            price_basis="as_of_adjusted_total_return",
            volatility_basis="annualized_daily_total_return_volatility",
        )
