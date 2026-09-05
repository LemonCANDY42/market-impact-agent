from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from market_impact_agent.agent_schema import validate_agent_contract
from market_impact_agent.dynamic_effectiveness import (
    AnalysisTopology,
    CadenceMetrics,
    CaseRole,
    DynamicEffectivenessRegistrationV1,
    MemorySensitivityPair,
    ModelStudyArm,
    StudyBudgetV1,
    StudyCase,
    cadence_eligible,
    choose_review_cadence,
)


def _arm(topology: AnalysisTopology, model: str, effort: str, index: int) -> ModelStudyArm:
    return ModelStudyArm(
        topology=topology,
        model=model,
        reasoning_effort=effort,
        provider_profile_id=f"profile-{index}",
        provider_profile_hash=str(index) * 64,
        pricing_id=f"pricing-{index}",
    )


def _registration() -> DynamicEffectivenessRegistrationV1:
    dates = (
        "2018-07-02",
        "2019-01-07",
        "2020-02-03",
        "2020-03-23",
        "2021-07-01",
        "2021-12-01",
        "2024-09-24",
        "2024-10-09",
    )
    cases = tuple(
        StudyCase(date, CaseRole.OPENED_DEVELOPMENT, f"{index:x}" * 64, int(date[:4]), "regime")
        for index, date in enumerate(dates, 1)
    )
    return DynamicEffectivenessRegistrationV1(
        experiment_id="dynamic-horizon-development-20260904",
        registered_at=datetime(2026, 9, 4, 8, 0, tzinfo=UTC),
        runtime_identity_hash="a" * 64,
        model_arms=(
            _arm(AnalysisTopology.LUNA_MAX, "gpt-5.6-luna", "max", 1),
            _arm(AnalysisTopology.TERRA_HIGH, "gpt-5.6-terra", "high", 2),
            _arm(AnalysisTopology.SOL_HIGH, "gpt-5.6-sol", "high", 3),
        ),
        opened_cases=cases,
        stability_case_ids=("2018-07-02", "2019-01-07", "2020-02-03"),
        memory_sensitivity_pairs=(MemorySensitivityPair("2020-02-03", cases[2].frozen_input_hash),),
    )


def _metrics(
    *,
    net: str = "0.02",
    drawdown: str = "-0.04",
    adverse: tuple[str, str] = ("-0.03", "-0.04"),
    corrected: int = 1,
    trend_delta: int = 0,
    stressed: str = "0.001",
    turnover: str = "1",
    cost: int = 100,
) -> CadenceMetrics:
    return CadenceMetrics(
        net_return=Decimal(net),
        maximum_drawdown=Decimal(drawdown),
        adverse_excursions=tuple(Decimal(item) for item in adverse),  # type: ignore[arg-type]
        corrected_reversal_count=corrected,
        trend_error_delta=trend_delta,
        stressed_incremental_return=Decimal(stressed),
        turnover=Decimal(turnover),
        model_cost_microusd=cost,
    )


def test_registration_freezes_three_routes_budget_and_light_memory_diagnostic() -> None:
    registration = _registration()
    payload = registration.to_dict()

    assert payload["registration_id"] == registration.registration_id
    assert registration.budget.total_microusd == 20_000_000
    assert [item.model for item in registration.model_arms] == [
        "gpt-5.6-luna",
        "gpt-5.6-terra",
        "gpt-5.6-sol",
    ]
    memory = payload["memory_sensitivity_pairs"][0]  # type: ignore[index]
    assert memory["facts_unchanged"] and memory["outcomes_hidden"]  # type: ignore[index]
    assert "not proof" in memory["inference_scope"]  # type: ignore[operator]
    assert (
        validate_agent_contract(payload, "dynamic-effectiveness-registration-v1.schema.json") == ()
    )


def test_registration_rejects_shared_pricing_and_wrong_model_effort() -> None:
    registration = _registration()
    shared = replace(registration.model_arms[1], pricing_id=registration.model_arms[0].pricing_id)
    with pytest.raises(ValueError, match="pricing"):
        replace(
            registration,
            model_arms=(registration.model_arms[0], shared, registration.model_arms[2]),
        )
    with pytest.raises(ValueError, match="preregistered route"):
        _arm(AnalysisTopology.TERRA_HIGH, "gpt-5.6-terra", "max", 2)
    with pytest.raises(ValueError, match="USD 20"):
        StudyBudgetV1(recovery_reserve_microusd=1)


def test_cadence_must_improve_reversal_without_harming_trend_or_stress() -> None:
    one_shot = _metrics(net="0.01", drawdown="-0.10", adverse=("-0.08", "-0.09"), corrected=0)
    scheduled = _metrics(drawdown="-0.06", adverse=("-0.06", "-0.07"), cost=300)
    event = _metrics(drawdown="-0.05", adverse=("-0.05", "-0.06"), cost=200)

    assert cadence_eligible(scheduled, one_shot)
    assert (
        choose_review_cadence(one_shot=one_shot, scheduled=scheduled, event_driven=event)
        == "material_event_driven_review"
    )

    harmful = _metrics(
        net="0.03",
        drawdown="-0.03",
        adverse=("-0.02", "-0.03"),
        trend_delta=1,
    )
    assert not cadence_eligible(harmful, one_shot)
    assert (
        choose_review_cadence(
            one_shot=one_shot,
            scheduled=harmful,
            event_driven=replace(harmful, stressed_incremental_return=Decimal("-0.01")),
        )
        == "dynamic_horizon_one_shot"
    )
