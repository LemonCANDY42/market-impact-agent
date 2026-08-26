from __future__ import annotations

from collections.abc import Callable
from decimal import Decimal, localcontext
from pathlib import Path
from typing import cast

import pytest

from market_impact_agent.agent_schema import validate_agent_contract
from market_impact_agent.method_benchmark import (
    MethodQualityBenchmarkRegistration,
    MethodQualityEvaluationSpecification,
    load_method_quality_benchmark,
    load_method_quality_evaluation_specification,
)
from market_impact_agent.method_statistics import (
    CaseReplicateValue,
    ClusteredPairedEstimate,
    compute_clustered_paired_estimate,
)
from market_impact_agent.research_methods import MethodArm

ROOT = Path(__file__).resolve().parents[1]


def active_contracts() -> tuple[
    MethodQualityBenchmarkRegistration,
    MethodQualityEvaluationSpecification,
]:
    return (
        load_method_quality_benchmark(
            ROOT / "examples/calibration/method-quality-benchmark-v2.json"
        ),
        load_method_quality_evaluation_specification(
            ROOT / "examples/calibration/method-quality-evaluation-specification-v2.json"
        ),
    )


def values_for_case(
    case_alias: str,
    *,
    candidate: tuple[str, ...],
    comparator: tuple[str, ...],
    candidate_arm: MethodArm = MethodArm.GENERAL_METHODS,
    comparator_arm: MethodArm = MethodArm.NEUTRAL_EVIDENCE,
) -> tuple[CaseReplicateValue, ...]:
    result: list[CaseReplicateValue] = []
    for replicate, value in enumerate(candidate, start=1):
        result.append(
            CaseReplicateValue(
                case_alias=case_alias,
                replicate=replicate,
                arm=candidate_arm,
                value=Decimal(value),
            )
        )
    for replicate, value in enumerate(comparator, start=1):
        result.append(
            CaseReplicateValue(
                case_alias=case_alias,
                replicate=replicate,
                arm=comparator_arm,
                value=Decimal(value),
            )
        )
    return tuple(result)


def registered_values(
    case_count: int,
    *,
    start_index: int = 1,
    candidate: str = "0",
    comparator: str = "0",
    candidate_arm: MethodArm = MethodArm.GENERAL_METHODS,
    comparator_arm: MethodArm = MethodArm.NEUTRAL_EVIDENCE,
) -> tuple[CaseReplicateValue, ...]:
    return tuple(
        item
        for index in range(case_count)
        for item in values_for_case(
            f"case-{start_index + index:02d}",
            candidate=(candidate,) * 5,
            comparator=(comparator,) * 5,
            candidate_arm=candidate_arm,
            comparator_arm=comparator_arm,
        )
    )


def compute_primary(
    values: tuple[CaseReplicateValue, ...],
) -> ClusteredPairedEstimate:
    registration, specification = active_contracts()
    return compute_clustered_paired_estimate(
        values,
        registration=registration,
        specification=specification,
        suite_id="general_methods",
        candidate_arm=MethodArm.GENERAL_METHODS,
        comparator_arm=MethodArm.NEUTRAL_EVIDENCE,
    )


def test_clustered_estimate_treats_cases_not_replicates_as_independent() -> None:
    values = tuple(
        item
        for index in range(24)
        for item in values_for_case(
            f"case-{index + 1:02d}",
            candidate=("0.10" if index < 12 else "0.20",) * 5,
            comparator=("0",) * 5,
        )
    )

    estimate = compute_primary(values)

    registration, specification = active_contracts()
    assert estimate.independent_case_count == 24
    assert len(estimate.case_differences) == 24
    assert estimate.point_estimate == Decimal("0.15")
    assert estimate.positive_case_count == 24
    assert estimate.zero_case_count == 0
    assert estimate.negative_case_count == 0
    assert estimate.critical_value == Decimal("2.069")
    assert estimate.interval_lower > 0
    assert estimate.contrast_role == "primary_promotion"
    assert estimate.promotion_eligible is True
    assert estimate.registration_id == registration.registration_id
    assert estimate.registration_hash == registration.registration_hash
    assert estimate.evaluation_specification_id == specification.specification_id
    assert estimate.evaluation_specification_hash == specification.specification_hash
    assert estimate.to_dict()["execution_capability"] == "none"
    assert (
        validate_agent_contract(
            estimate.to_dict(),
            "method-quality-clustered-estimate.schema.json",
        )
        == ()
    )


def test_clustered_estimate_averages_model_noise_within_each_case() -> None:
    values = values_for_case(
        "case-01",
        candidate=("1", "0", "0", "0", "0"),
        comparator=("0", "0", "0", "0", "0"),
    ) + registered_values(23, start_index=2)

    estimate = compute_primary(values)

    assert estimate.case_differences[0].difference == Decimal("0.2")
    assert all(item.difference == 0 for item in estimate.case_differences[1:])
    with localcontext() as context:
        context.prec = 50
        assert estimate.point_estimate == Decimal("0.2") / Decimal(24)


def test_clustered_estimate_fails_closed_on_missing_or_duplicate_cells() -> None:
    complete = registered_values(24)

    with pytest.raises(ValueError, match="no pair deletion"):
        compute_primary(complete[:-1])

    with pytest.raises(ValueError, match="duplicate"):
        compute_primary((*complete, complete[0]))


def test_clustered_estimate_rejects_unregistered_contrast_and_arm_values() -> None:
    registration, specification = active_contracts()
    complete = registered_values(24)
    unrelated = CaseReplicateValue(
        case_alias="case-01",
        replicate=1,
        arm=MethodArm.GENERAL_PATTERN,
        value=Decimal("0"),
    )

    with pytest.raises(ValueError, match="unrelated"):
        compute_primary((*complete, unrelated))

    with pytest.raises(ValueError, match="registered contrast"):
        compute_clustered_paired_estimate(
            complete,
            registration=registration,
            specification=specification,
            suite_id="general_methods",
            candidate_arm=MethodArm.GENERAL_PATTERN,
            comparator_arm=MethodArm.NEUTRAL_EVIDENCE,
        )


def test_clustered_estimate_uses_the_registered_contrast_roles() -> None:
    registration, specification = active_contracts()
    cases = (
        (
            "general_methods",
            MethodArm.GENERAL_METHODS,
            MethodArm.NEUTRAL_EVIDENCE,
            24,
            "primary_promotion",
            True,
            Decimal("2.069"),
        ),
        (
            "general_methods",
            MethodArm.GENERAL_PATTERN,
            MethodArm.GENERAL_METHODS,
            24,
            "secondary_diagnostic",
            False,
            Decimal("2.069"),
        ),
        (
            "family_increment",
            MethodArm.FAMILY_GUIDED,
            MethodArm.GENERAL_PATTERN,
            8,
            "secondary_diagnostic",
            False,
            Decimal("2.365"),
        ),
    )

    for (
        suite_id,
        candidate_arm,
        comparator_arm,
        case_count,
        contrast_role,
        promotion_eligible,
        critical_value,
    ) in cases:
        estimate = compute_clustered_paired_estimate(
            registered_values(
                case_count,
                candidate_arm=candidate_arm,
                comparator_arm=comparator_arm,
            ),
            registration=registration,
            specification=specification,
            suite_id=suite_id,
            candidate_arm=candidate_arm,
            comparator_arm=comparator_arm,
        )

        assert estimate.contrast_role == contrast_role
        assert estimate.promotion_eligible is promotion_eligible
        assert estimate.critical_value == critical_value


def test_clustered_estimate_rejects_unbound_v1_and_arbitrary_critical_value() -> None:
    registration, specification = active_contracts()
    v1_registration = load_method_quality_benchmark(
        ROOT / "examples/calibration/method-quality-benchmark-v1.json"
    )
    v1_specification = load_method_quality_evaluation_specification(
        ROOT / "examples/calibration/method-quality-evaluation-specification-v1.json"
    )
    values = registered_values(24)

    with pytest.raises(ValueError, match="v2"):
        compute_clustered_paired_estimate(
            values,
            registration=v1_registration,
            specification=v1_specification,
            suite_id="general_methods",
            candidate_arm=MethodArm.GENERAL_METHODS,
            comparator_arm=MethodArm.NEUTRAL_EVIDENCE,
        )

    with pytest.raises(ValueError, match="does not match evaluation specification"):
        compute_clustered_paired_estimate(
            values,
            registration=registration,
            specification=v1_specification,
            suite_id="general_methods",
            candidate_arm=MethodArm.GENERAL_METHODS,
            comparator_arm=MethodArm.NEUTRAL_EVIDENCE,
        )

    untyped_compute = cast(
        Callable[..., ClusteredPairedEstimate],
        compute_clustered_paired_estimate,
    )
    with pytest.raises(TypeError, match="critical_value"):
        untyped_compute(
            values,
            registration=registration,
            specification=specification,
            suite_id="general_methods",
            candidate_arm=MethodArm.GENERAL_METHODS,
            comparator_arm=MethodArm.NEUTRAL_EVIDENCE,
            critical_value=Decimal("0.001"),
        )


def test_cluster_correction_blocks_replicate_inflated_false_positive() -> None:
    values: tuple[CaseReplicateValue, ...] = ()
    for index in range(24):
        case_value = "0.10" if index < 12 else "-0.06"
        values += values_for_case(
            f"case-{index + 1:02d}",
            candidate=(case_value,) * 5,
            comparator=("0",) * 5,
        )

    estimate = compute_primary(values)

    replicate_differences = tuple(
        item.value for item in values if item.arm is MethodArm.GENERAL_METHODS
    )
    replicate_count = Decimal(len(replicate_differences))
    replicate_mean = sum(replicate_differences, Decimal(0)) / replicate_count
    replicate_variance = sum(
        ((item - replicate_mean) ** 2 for item in replicate_differences),
        Decimal(0),
    ) / Decimal(len(replicate_differences) - 1)
    replicate_lower = (
        replicate_mean - Decimal("1.980") * (replicate_variance / replicate_count).sqrt()
    )

    assert replicate_lower > 0
    assert estimate.point_estimate == Decimal("0.02")
    assert estimate.interval_lower < 0
    assert estimate.lower_bound_positive is False
