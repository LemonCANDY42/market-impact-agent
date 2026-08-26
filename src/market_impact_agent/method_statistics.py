from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, localcontext

from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.method_benchmark import (
    MethodQualityBenchmarkRegistration,
    MethodQualityEvaluationSpecification,
)
from market_impact_agent.research_methods import MethodArm

CLUSTERED_PAIRED_ESTIMATE_SCHEMA = "market-impact.method-quality-clustered-estimate.v1"


@dataclass(frozen=True, slots=True)
class CaseReplicateValue:
    case_alias: str
    replicate: int
    arm: MethodArm
    value: Decimal

    def __post_init__(self) -> None:
        if not self.case_alias or self.case_alias != self.case_alias.strip():
            raise ValueError("case_alias must be a non-empty stable identifier")
        if self.replicate < 1:
            raise ValueError("replicate must be positive")
        if not self.value.is_finite():
            raise ValueError("case replicate value must be finite")


@dataclass(frozen=True, slots=True)
class CaseClusterDifference:
    case_alias: str
    candidate_case_mean: Decimal
    comparator_case_mean: Decimal
    difference: Decimal

    def to_dict(self) -> dict[str, str]:
        return {
            "case_alias": self.case_alias,
            "candidate_case_mean": str(self.candidate_case_mean),
            "comparator_case_mean": str(self.comparator_case_mean),
            "difference": str(self.difference),
        }


@dataclass(frozen=True, slots=True)
class ClusteredPairedEstimate:
    estimate_id: str
    registration_id: str
    registration_hash: str
    evaluation_specification_id: str
    evaluation_specification_hash: str
    suite_id: str
    candidate_arm: MethodArm
    comparator_arm: MethodArm
    contrast_role: str
    promotion_eligible: bool
    replicate_count: int
    independent_case_count: int
    case_differences: tuple[CaseClusterDifference, ...]
    point_estimate: Decimal
    sample_variance: Decimal
    standard_error: Decimal
    critical_value: Decimal
    confidence_level: Decimal
    interval_lower: Decimal
    interval_upper: Decimal
    positive_case_count: int
    zero_case_count: int
    negative_case_count: int
    execution_capability: str = "none"

    def __post_init__(self) -> None:
        if self.registration_id != f"method-quality-benchmark-{self.registration_hash}":
            raise ValueError("clustered estimate registration identity is inconsistent")
        if self.evaluation_specification_id != (
            f"method-quality-evaluation-{self.evaluation_specification_hash}"
        ):
            raise ValueError("clustered estimate evaluation specification identity is inconsistent")
        if self.contrast_role not in {"primary_promotion", "secondary_diagnostic"}:
            raise ValueError("clustered estimate contrast role is invalid")
        if self.promotion_eligible != (self.contrast_role == "primary_promotion"):
            raise ValueError("clustered estimate promotion eligibility is inconsistent")
        if self.independent_case_count != len(self.case_differences):
            raise ValueError("independent case count does not match case differences")
        if self.execution_capability != "none":
            raise ValueError("method-quality estimates grant no execution capability")
        if self.estimate_id != self.expected_estimate_id:
            raise ValueError("clustered estimate identity does not match content")

    @property
    def estimate_hash(self) -> str:
        return canonical_hash(self.core_dict())

    @property
    def expected_estimate_id(self) -> str:
        return f"method-quality-clustered-estimate-{self.estimate_hash}"

    @property
    def lower_bound_positive(self) -> bool:
        return self.interval_lower > 0

    def core_dict(self) -> dict[str, object]:
        return {
            "schema_version": CLUSTERED_PAIRED_ESTIMATE_SCHEMA,
            "registration_id": self.registration_id,
            "registration_hash": self.registration_hash,
            "evaluation_specification_id": self.evaluation_specification_id,
            "evaluation_specification_hash": self.evaluation_specification_hash,
            "suite_id": self.suite_id,
            "candidate_arm": self.candidate_arm.value,
            "comparator_arm": self.comparator_arm.value,
            "contrast_role": self.contrast_role,
            "promotion_eligible": self.promotion_eligible,
            "replicate_count": self.replicate_count,
            "independent_case_count": self.independent_case_count,
            "case_differences": [item.to_dict() for item in self.case_differences],
            "point_estimate": str(self.point_estimate),
            "sample_variance": str(self.sample_variance),
            "standard_error": str(self.standard_error),
            "critical_value": str(self.critical_value),
            "confidence_level": str(self.confidence_level),
            "interval_lower": str(self.interval_lower),
            "interval_upper": str(self.interval_upper),
            "positive_case_count": self.positive_case_count,
            "zero_case_count": self.zero_case_count,
            "negative_case_count": self.negative_case_count,
            "lower_bound_positive": self.lower_bound_positive,
            "execution_capability": self.execution_capability,
        }

    def to_dict(self) -> dict[str, object]:
        return {**self.core_dict(), "estimate_id": self.estimate_id}


def compute_clustered_paired_estimate(
    values: tuple[CaseReplicateValue, ...],
    *,
    registration: MethodQualityBenchmarkRegistration,
    specification: MethodQualityEvaluationSpecification,
    suite_id: str,
    candidate_arm: MethodArm,
    comparator_arm: MethodArm,
) -> ClusteredPairedEstimate:
    policy = registration.clustered_paired_estimate_policy(
        specification=specification,
        suite_id=suite_id,
        candidate_arm=candidate_arm,
        comparator_arm=comparator_arm,
    )
    replicate_count = policy.replicate_count
    independent_case_count = policy.independent_case_count
    critical_value = policy.critical_value

    allowed_arms = {candidate_arm, comparator_arm}
    if any(item.arm not in allowed_arms for item in values):
        raise ValueError("clustered estimate input contains an unrelated method arm")

    by_key: dict[tuple[str, MethodArm, int], Decimal] = {}
    case_aliases: set[str] = set()
    for item in values:
        if item.replicate > replicate_count:
            raise ValueError("replicate exceeds the registered replicate count")
        key = (item.case_alias, item.arm, item.replicate)
        if key in by_key:
            raise ValueError("duplicate case-replicate-arm value")
        by_key[key] = item.value
        case_aliases.add(item.case_alias)

    if len(case_aliases) != independent_case_count:
        raise ValueError("clustered estimate does not contain the registered case count")

    expected_replicates = range(1, replicate_count + 1)
    differences: list[CaseClusterDifference] = []
    with localcontext() as context:
        context.prec = 50
        divisor = Decimal(replicate_count)
        for case_alias in sorted(case_aliases):
            candidate_values: list[Decimal] = []
            comparator_values: list[Decimal] = []
            for replicate in expected_replicates:
                candidate_key = (case_alias, candidate_arm, replicate)
                comparator_key = (case_alias, comparator_arm, replicate)
                if candidate_key not in by_key or comparator_key not in by_key:
                    raise ValueError(
                        "missing case-replicate-arm value; no pair deletion is allowed"
                    )
                candidate_values.append(by_key[candidate_key])
                comparator_values.append(by_key[comparator_key])
            candidate_mean = sum(candidate_values, Decimal(0)) / divisor
            comparator_mean = sum(comparator_values, Decimal(0)) / divisor
            differences.append(
                CaseClusterDifference(
                    case_alias=case_alias,
                    candidate_case_mean=candidate_mean,
                    comparator_case_mean=comparator_mean,
                    difference=candidate_mean - comparator_mean,
                )
            )

        independent_n = Decimal(independent_case_count)
        point_estimate = sum((item.difference for item in differences), Decimal(0)) / independent_n
        sample_variance = sum(
            ((item.difference - point_estimate) ** 2 for item in differences),
            Decimal(0),
        ) / Decimal(independent_case_count - 1)
        standard_error = (sample_variance / independent_n).sqrt()
        margin = critical_value * standard_error
        interval_lower = point_estimate - margin
        interval_upper = point_estimate + margin

    positive = sum(item.difference > 0 for item in differences)
    zero = sum(item.difference == 0 for item in differences)
    negative = sum(item.difference < 0 for item in differences)
    core: dict[str, object] = {
        "schema_version": CLUSTERED_PAIRED_ESTIMATE_SCHEMA,
        "registration_id": policy.registration_id,
        "registration_hash": policy.registration_hash,
        "evaluation_specification_id": policy.evaluation_specification_id,
        "evaluation_specification_hash": policy.evaluation_specification_hash,
        "suite_id": policy.suite_id,
        "candidate_arm": policy.candidate_arm.value,
        "comparator_arm": policy.comparator_arm.value,
        "contrast_role": policy.contrast_role,
        "promotion_eligible": policy.promotion_eligible,
        "replicate_count": replicate_count,
        "independent_case_count": independent_case_count,
        "case_differences": [item.to_dict() for item in differences],
        "point_estimate": str(point_estimate),
        "sample_variance": str(sample_variance),
        "standard_error": str(standard_error),
        "critical_value": str(critical_value),
        "confidence_level": "0.95",
        "interval_lower": str(interval_lower),
        "interval_upper": str(interval_upper),
        "positive_case_count": positive,
        "zero_case_count": zero,
        "negative_case_count": negative,
        "lower_bound_positive": interval_lower > 0,
        "execution_capability": "none",
    }
    return ClusteredPairedEstimate(
        estimate_id=f"method-quality-clustered-estimate-{canonical_hash(core)}",
        registration_id=policy.registration_id,
        registration_hash=policy.registration_hash,
        evaluation_specification_id=policy.evaluation_specification_id,
        evaluation_specification_hash=policy.evaluation_specification_hash,
        suite_id=policy.suite_id,
        candidate_arm=policy.candidate_arm,
        comparator_arm=policy.comparator_arm,
        contrast_role=policy.contrast_role,
        promotion_eligible=policy.promotion_eligible,
        replicate_count=replicate_count,
        independent_case_count=independent_case_count,
        case_differences=tuple(differences),
        point_estimate=point_estimate,
        sample_variance=sample_variance,
        standard_error=standard_error,
        critical_value=critical_value,
        confidence_level=Decimal("0.95"),
        interval_lower=interval_lower,
        interval_upper=interval_upper,
        positive_case_count=positive,
        zero_case_count=zero,
        negative_case_count=negative,
    )
