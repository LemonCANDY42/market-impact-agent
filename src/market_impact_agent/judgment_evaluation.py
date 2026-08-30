from __future__ import annotations

import json
import re
import sqlite3
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Protocol, cast

from market_impact_agent.agent_contracts import (
    CandidateDirection,
    JudgmentArtifact,
    canonical_hash,
    canonical_json_bytes,
)
from market_impact_agent.domain import require_aware

JUDGMENT_EVALUATION_BAND_SCHEMA = "market-impact.judgment-evaluation-band-specification.v1"
JUDGMENT_EVALUATION_RESULT_SCHEMA = "market-impact.judgment-evaluation-result.v1"

_SYSTEM_MAX_RETURN_BAND_WIDTH = Decimal("1")
_SYSTEM_MAX_VOLATILITY_BAND_WIDTH = Decimal("5")
_SYSTEM_MAX_LATEST_HORIZON_SESSIONS = 1260


@dataclass(frozen=True, slots=True)
class JudgmentEvaluationTolerancePolicy:
    policy_id: str
    policy_name: str
    policy_version: str
    maximum_latest_horizon_sessions: int
    maximum_horizon_span_sessions: int
    maximum_return_band_width: Decimal
    maximum_volatility_band_width: Decimal
    maximum_adverse_excursion: Decimal
    price_basis: str
    volatility_basis: str

    def __post_init__(self) -> None:
        _identifier(self.policy_name, "Judgment Evaluation policy_name")
        if re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+", self.policy_version) is None:
            raise ValueError("Judgment Evaluation policy_version must be semantic")
        if not 1 <= self.maximum_latest_horizon_sessions <= (_SYSTEM_MAX_LATEST_HORIZON_SESSIONS):
            raise ValueError("Judgment Evaluation policy horizon exceeds the system bound")
        if not 1 <= self.maximum_horizon_span_sessions <= (self.maximum_latest_horizon_sessions):
            raise ValueError("Judgment Evaluation policy horizon span is invalid")
        _finite_nonnegative(self.maximum_return_band_width, "maximum_return_band_width")
        _finite_nonnegative(
            self.maximum_volatility_band_width,
            "maximum_volatility_band_width",
        )
        _finite_nonnegative(self.maximum_adverse_excursion, "maximum_adverse_excursion")
        if self.maximum_return_band_width > _SYSTEM_MAX_RETURN_BAND_WIDTH:
            raise ValueError("Judgment Evaluation policy return width exceeds the system bound")
        if self.maximum_volatility_band_width > _SYSTEM_MAX_VOLATILITY_BAND_WIDTH:
            raise ValueError("Judgment Evaluation policy volatility width exceeds the system bound")
        if self.maximum_adverse_excursion > Decimal("1"):
            raise ValueError("Judgment Evaluation policy adverse excursion exceeds one")
        _nonempty(self.price_basis, "Judgment Evaluation policy price_basis")
        _nonempty(self.volatility_basis, "Judgment Evaluation policy volatility_basis")
        if self.policy_id != self.expected_policy_id:
            raise ValueError("Judgment Evaluation policy_id does not match content")

    @property
    def expected_policy_id(self) -> str:
        return f"judgment-evaluation-policy-{canonical_hash(self.core_dict())}"

    def core_dict(self) -> dict[str, object]:
        return {
            "policy_name": self.policy_name,
            "policy_version": self.policy_version,
            "maximum_latest_horizon_sessions": self.maximum_latest_horizon_sessions,
            "maximum_horizon_span_sessions": self.maximum_horizon_span_sessions,
            "maximum_return_band_width": str(self.maximum_return_band_width),
            "maximum_volatility_band_width": str(self.maximum_volatility_band_width),
            "maximum_adverse_excursion": str(self.maximum_adverse_excursion),
            "price_basis": self.price_basis,
            "volatility_basis": self.volatility_basis,
        }

    def to_dict(self) -> dict[str, object]:
        return {**self.core_dict(), "policy_id": self.policy_id}

    @classmethod
    def build(
        cls,
        *,
        policy_name: str,
        policy_version: str,
        maximum_latest_horizon_sessions: int,
        maximum_horizon_span_sessions: int,
        maximum_return_band_width: Decimal,
        maximum_volatility_band_width: Decimal,
        maximum_adverse_excursion: Decimal,
        price_basis: str,
        volatility_basis: str,
    ) -> JudgmentEvaluationTolerancePolicy:
        core = {
            "policy_name": policy_name,
            "policy_version": policy_version,
            "maximum_latest_horizon_sessions": maximum_latest_horizon_sessions,
            "maximum_horizon_span_sessions": maximum_horizon_span_sessions,
            "maximum_return_band_width": str(maximum_return_band_width),
            "maximum_volatility_band_width": str(maximum_volatility_band_width),
            "maximum_adverse_excursion": str(maximum_adverse_excursion),
            "price_basis": price_basis,
            "volatility_basis": volatility_basis,
        }
        return cls(
            policy_id=f"judgment-evaluation-policy-{canonical_hash(core)}",
            policy_name=policy_name,
            policy_version=policy_version,
            maximum_latest_horizon_sessions=maximum_latest_horizon_sessions,
            maximum_horizon_span_sessions=maximum_horizon_span_sessions,
            maximum_return_band_width=maximum_return_band_width,
            maximum_volatility_band_width=maximum_volatility_band_width,
            maximum_adverse_excursion=maximum_adverse_excursion,
            price_basis=price_basis,
            volatility_basis=volatility_basis,
        )


@dataclass(frozen=True, slots=True)
class JudgmentEvaluationPolicyCatalog:
    catalog_id: str
    registered_at: datetime
    policies: tuple[JudgmentEvaluationTolerancePolicy, ...]

    def __post_init__(self) -> None:
        require_aware(self.registered_at, "Judgment Evaluation Policy Catalog registered_at")
        if not self.policies:
            raise ValueError("Judgment Evaluation Policy Catalog cannot be empty")
        _unique(tuple(item.policy_name for item in self.policies), "Evaluation policy names")
        _unique(tuple(item.policy_id for item in self.policies), "Evaluation policy IDs")
        if self.catalog_id != self.expected_catalog_id:
            raise ValueError("Judgment Evaluation Policy Catalog ID does not match content")

    @property
    def expected_catalog_id(self) -> str:
        return f"judgment-evaluation-policy-catalog-{canonical_hash(self.core_dict())}"

    def core_dict(self) -> dict[str, object]:
        return {
            "registered_at": _timestamp(self.registered_at),
            "policies": [item.to_dict() for item in self.policies],
        }

    def to_dict(self) -> dict[str, object]:
        return {**self.core_dict(), "catalog_id": self.catalog_id}

    @classmethod
    def build(
        cls,
        *,
        registered_at: datetime,
        policies: tuple[JudgmentEvaluationTolerancePolicy, ...],
    ) -> JudgmentEvaluationPolicyCatalog:
        core = {
            "registered_at": _timestamp(registered_at),
            "policies": [item.to_dict() for item in policies],
        }
        return cls(
            catalog_id=f"judgment-evaluation-policy-catalog-{canonical_hash(core)}",
            registered_at=registered_at,
            policies=policies,
        )

    def require(self, policy_id: str) -> JudgmentEvaluationTolerancePolicy:
        selected = [item for item in self.policies if item.policy_id == policy_id]
        if len(selected) != 1:
            raise ValueError("Judgment Evaluation policy is not registered in the catalog")
        return selected[0]


class JudgmentEvaluationPolicyRegistrationAuthority(Protocol):
    def assert_authoritative_registration(
        self,
        catalog: JudgmentEvaluationPolicyCatalog,
    ) -> datetime: ...


class JudgmentEvaluationPolicyCatalogStore:
    """Durable Harness authority for pre-run tolerance-policy registration."""

    def __init__(
        self,
        path: Path,
        *,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.path = path
        self._clock = clock or (lambda: datetime.now(UTC))
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as connection:
            connection.execute("PRAGMA journal_mode=WAL")
            connection.execute("PRAGMA synchronous=FULL")
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS judgment_evaluation_policy_catalogs(
                    registration_key TEXT PRIMARY KEY,
                    catalog_id TEXT NOT NULL UNIQUE,
                    registered_at TEXT NOT NULL,
                    catalog_json TEXT NOT NULL
                )
                """
            )

    def register(
        self,
        policies: tuple[JudgmentEvaluationTolerancePolicy, ...],
    ) -> JudgmentEvaluationPolicyCatalog:
        if not policies:
            raise ValueError("Judgment Evaluation Policy Catalog cannot be empty")
        registration_key = canonical_hash([item.to_dict() for item in policies])
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                """
                SELECT catalog_json FROM judgment_evaluation_policy_catalogs
                WHERE registration_key = ?
                """,
                (registration_key,),
            ).fetchone()
            if row is not None:
                return _policy_catalog(_json_object(str(row["catalog_json"])))
            registered_at = self._clock()
            require_aware(
                registered_at,
                "Judgment Evaluation Policy Catalog Harness clock",
            )
            catalog = JudgmentEvaluationPolicyCatalog.build(
                registered_at=registered_at.astimezone(UTC),
                policies=policies,
            )
            connection.execute(
                """
                INSERT INTO judgment_evaluation_policy_catalogs(
                    registration_key, catalog_id, registered_at, catalog_json
                ) VALUES (?, ?, ?, ?)
                """,
                (
                    registration_key,
                    catalog.catalog_id,
                    _timestamp(catalog.registered_at),
                    canonical_json_bytes(catalog.to_dict()).decode(),
                ),
            )
        return catalog

    def assert_authoritative_registration(
        self,
        catalog: JudgmentEvaluationPolicyCatalog,
    ) -> datetime:
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT registered_at, catalog_json
                FROM judgment_evaluation_policy_catalogs WHERE catalog_id = ?
                """,
                (catalog.catalog_id,),
            ).fetchone()
        if row is None:
            raise ValueError("Judgment Evaluation policy catalog is not durably registered")
        if canonical_json_bytes(catalog.to_dict()).decode() != str(row["catalog_json"]):
            raise ValueError("Judgment Evaluation policy catalog differs from durable registration")
        registered_at = _parse_datetime(str(row["registered_at"]), "registered_at")
        if registered_at != catalog.registered_at:
            raise ValueError("Judgment Evaluation policy catalog Harness time differs")
        return registered_at

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path)
        connection.row_factory = sqlite3.Row
        return connection


@dataclass(frozen=True, slots=True)
class JudgmentEvaluationBandSpecification:
    specification_id: str
    registered_at: datetime
    outcome_open_not_before: datetime
    policy_catalog: JudgmentEvaluationPolicyCatalog
    tolerance_policy: JudgmentEvaluationTolerancePolicy
    judgment_artifact_id: str
    judgment_artifact_hash: str
    judgment_started_at: datetime
    judgment_finished_at: datetime
    target_id: str
    expected_direction: CandidateDirection
    earliest_horizon_sessions: int
    latest_horizon_sessions: int
    terminal_return_lower: Decimal
    terminal_return_upper: Decimal
    realized_volatility_lower: Decimal
    realized_volatility_upper: Decimal
    maximum_adverse_excursion: Decimal
    price_basis: str
    volatility_basis: str

    def __post_init__(self) -> None:
        require_aware(self.registered_at, "Judgment Evaluation registered_at")
        require_aware(self.outcome_open_not_before, "Judgment Evaluation outcome open time")
        if self.registered_at >= self.outcome_open_not_before:
            raise ValueError("Judgment Evaluation band must be registered before outcome opening")
        if self.policy_catalog.registered_at > self.registered_at:
            raise ValueError("Judgment Evaluation policy catalog was registered too late")
        registered_policy = self.policy_catalog.require(self.tolerance_policy.policy_id)
        if registered_policy != self.tolerance_policy:
            raise ValueError("Judgment Evaluation policy differs from the registered catalog")
        require_aware(self.judgment_started_at, "Judgment Evaluation Judgment started_at")
        require_aware(self.judgment_finished_at, "Judgment Evaluation Judgment finished_at")
        if self.judgment_started_at > self.judgment_finished_at:
            raise ValueError("Judgment Evaluation Judgment interval is invalid")
        if self.policy_catalog.registered_at > self.judgment_started_at:
            raise ValueError("Judgment Evaluation policy catalog was registered after the run")
        if self.registered_at < self.judgment_finished_at:
            raise ValueError("Judgment Evaluation band predates the Judgment")
        _prefixed_hash(
            self.judgment_artifact_id,
            "judgment-",
            "Judgment Evaluation artifact ID",
        )
        _sha256(self.judgment_artifact_hash, "Judgment Evaluation artifact hash")
        _nonempty(self.target_id, "Judgment Evaluation target_id")
        if self.expected_direction not in {CandidateDirection.UP, CandidateDirection.DOWN}:
            raise ValueError("Judgment Evaluation requires an up or down candidate direction")
        if (
            self.earliest_horizon_sessions < 1
            or self.latest_horizon_sessions < self.earliest_horizon_sessions
        ):
            raise ValueError("Judgment Evaluation horizon interval is invalid")
        _finite(self.terminal_return_lower, "terminal_return_lower")
        _finite(self.terminal_return_upper, "terminal_return_upper")
        if self.terminal_return_lower > self.terminal_return_upper:
            raise ValueError("Judgment Evaluation return interval is reversed")
        if (
            self.terminal_return_upper - self.terminal_return_lower
            > self.tolerance_policy.maximum_return_band_width
        ):
            raise ValueError("Judgment Evaluation return band exceeds its policy width")
        for name in (
            "realized_volatility_lower",
            "realized_volatility_upper",
            "maximum_adverse_excursion",
        ):
            _finite_nonnegative(getattr(self, name), name)
        if self.realized_volatility_lower > self.realized_volatility_upper:
            raise ValueError("Judgment Evaluation volatility interval is reversed")
        if (
            self.realized_volatility_upper - self.realized_volatility_lower
            > self.tolerance_policy.maximum_volatility_band_width
        ):
            raise ValueError("Judgment Evaluation volatility band exceeds its policy width")
        if self.maximum_adverse_excursion > self.tolerance_policy.maximum_adverse_excursion:
            raise ValueError("Judgment Evaluation adverse excursion exceeds its policy")
        _nonempty(self.price_basis, "Judgment Evaluation price_basis")
        _nonempty(self.volatility_basis, "Judgment Evaluation volatility_basis")
        if (
            self.latest_horizon_sessions > self.tolerance_policy.maximum_latest_horizon_sessions
            or self.latest_horizon_sessions - self.earliest_horizon_sessions + 1
            > self.tolerance_policy.maximum_horizon_span_sessions
        ):
            raise ValueError("Judgment Evaluation horizon exceeds its policy")
        if (
            self.price_basis != self.tolerance_policy.price_basis
            or self.volatility_basis != self.tolerance_policy.volatility_basis
        ):
            raise ValueError("Judgment Evaluation basis differs from its policy")
        if self.specification_id != self.expected_specification_id:
            raise ValueError("Judgment Evaluation specification_id does not match content")

    @property
    def expected_specification_id(self) -> str:
        return f"judgment-evaluation-band-{canonical_hash(self.core_dict())}"

    def core_dict(self) -> dict[str, object]:
        return {
            "schema_version": JUDGMENT_EVALUATION_BAND_SCHEMA,
            "registered_at": _timestamp(self.registered_at),
            "outcome_open_not_before": _timestamp(self.outcome_open_not_before),
            "policy_catalog": self.policy_catalog.to_dict(),
            "tolerance_policy": self.tolerance_policy.to_dict(),
            "judgment_artifact_id": self.judgment_artifact_id,
            "judgment_artifact_hash": self.judgment_artifact_hash,
            "judgment_started_at": _timestamp(self.judgment_started_at),
            "judgment_finished_at": _timestamp(self.judgment_finished_at),
            "target_id": self.target_id,
            "expected_direction": self.expected_direction.value,
            "earliest_horizon_sessions": self.earliest_horizon_sessions,
            "latest_horizon_sessions": self.latest_horizon_sessions,
            "terminal_return_lower": str(self.terminal_return_lower),
            "terminal_return_upper": str(self.terminal_return_upper),
            "realized_volatility_lower": str(self.realized_volatility_lower),
            "realized_volatility_upper": str(self.realized_volatility_upper),
            "maximum_adverse_excursion": str(self.maximum_adverse_excursion),
            "price_basis": self.price_basis,
            "volatility_basis": self.volatility_basis,
            "overall_rule": "all_direction_horizon_return_volatility_and_adverse_excursion",
            "evaluation_only": True,
            "changes_signal_or_execution_admission": False,
        }

    def to_dict(self) -> dict[str, object]:
        return {**self.core_dict(), "specification_id": self.specification_id}

    @classmethod
    def build(
        cls,
        *,
        registered_at: datetime,
        outcome_open_not_before: datetime,
        policy_catalog: JudgmentEvaluationPolicyCatalog,
        policy_registration_authority: JudgmentEvaluationPolicyRegistrationAuthority,
        tolerance_policy_id: str,
        artifact: JudgmentArtifact,
        target_id: str,
        earliest_horizon_sessions: int,
        latest_horizon_sessions: int,
        terminal_return_lower: Decimal,
        terminal_return_upper: Decimal,
        realized_volatility_lower: Decimal,
        realized_volatility_upper: Decimal,
        maximum_adverse_excursion: Decimal,
        price_basis: str,
        volatility_basis: str,
    ) -> JudgmentEvaluationBandSpecification:
        if registered_at < artifact.finished_at:
            raise ValueError("Judgment Evaluation band cannot predate the Judgment Artifact")
        authoritative_registered_at = (
            policy_registration_authority.assert_authoritative_registration(policy_catalog)
        )
        if authoritative_registered_at != policy_catalog.registered_at:
            raise ValueError("Judgment Evaluation policy authority returned a different time")
        if authoritative_registered_at > artifact.started_at:
            raise ValueError(
                "Judgment Evaluation policy catalog must be registered before the Agent run"
            )
        tolerance_policy = policy_catalog.require(tolerance_policy_id)
        candidates = [item for item in artifact.proposal.candidates if item.target_id == target_id]
        if len(candidates) != 1:
            raise ValueError("Judgment Evaluation target must be one proposed candidate")
        expected_direction = candidates[0].direction
        artifact_hash = canonical_hash(artifact.to_dict())
        values = {
            "schema_version": JUDGMENT_EVALUATION_BAND_SCHEMA,
            "registered_at": _timestamp(registered_at),
            "outcome_open_not_before": _timestamp(outcome_open_not_before),
            "policy_catalog": policy_catalog.to_dict(),
            "tolerance_policy": tolerance_policy.to_dict(),
            "judgment_artifact_id": artifact.artifact_id,
            "judgment_artifact_hash": artifact_hash,
            "judgment_started_at": _timestamp(artifact.started_at),
            "judgment_finished_at": _timestamp(artifact.finished_at),
            "target_id": target_id,
            "expected_direction": expected_direction.value,
            "earliest_horizon_sessions": earliest_horizon_sessions,
            "latest_horizon_sessions": latest_horizon_sessions,
            "terminal_return_lower": str(terminal_return_lower),
            "terminal_return_upper": str(terminal_return_upper),
            "realized_volatility_lower": str(realized_volatility_lower),
            "realized_volatility_upper": str(realized_volatility_upper),
            "maximum_adverse_excursion": str(maximum_adverse_excursion),
            "price_basis": price_basis,
            "volatility_basis": volatility_basis,
            "overall_rule": "all_direction_horizon_return_volatility_and_adverse_excursion",
            "evaluation_only": True,
            "changes_signal_or_execution_admission": False,
        }
        return cls(
            specification_id=f"judgment-evaluation-band-{canonical_hash(values)}",
            registered_at=registered_at,
            outcome_open_not_before=outcome_open_not_before,
            policy_catalog=policy_catalog,
            tolerance_policy=tolerance_policy,
            judgment_artifact_id=artifact.artifact_id,
            judgment_artifact_hash=artifact_hash,
            judgment_started_at=artifact.started_at,
            judgment_finished_at=artifact.finished_at,
            target_id=target_id,
            expected_direction=expected_direction,
            earliest_horizon_sessions=earliest_horizon_sessions,
            latest_horizon_sessions=latest_horizon_sessions,
            terminal_return_lower=terminal_return_lower,
            terminal_return_upper=terminal_return_upper,
            realized_volatility_lower=realized_volatility_lower,
            realized_volatility_upper=realized_volatility_upper,
            maximum_adverse_excursion=maximum_adverse_excursion,
            price_basis=price_basis,
            volatility_basis=volatility_basis,
        )


@dataclass(frozen=True, slots=True)
class JudgmentEvaluationResult:
    result_id: str
    evaluated_at: datetime
    specification_id: str
    specification_hash: str
    outcome_hash: str
    horizon_sessions: int
    realized_terminal_return: Decimal
    realized_volatility: Decimal
    adverse_excursion: Decimal
    direction_passed: bool
    horizon_passed: bool
    return_band_passed: bool
    volatility_band_passed: bool
    adverse_excursion_passed: bool
    broadly_correct: bool

    def __post_init__(self) -> None:
        require_aware(self.evaluated_at, "Judgment Evaluation Result evaluated_at")
        _prefixed_hash(
            self.specification_id,
            "judgment-evaluation-band-",
            "Judgment Evaluation specification ID",
        )
        _sha256(self.specification_hash, "Judgment Evaluation specification hash")
        _sha256(self.outcome_hash, "Judgment Evaluation outcome hash")
        if self.horizon_sessions < 1:
            raise ValueError("Judgment Evaluation outcome horizon must be positive")
        _finite(self.realized_terminal_return, "realized_terminal_return")
        _finite_nonnegative(self.realized_volatility, "realized_volatility")
        _finite_nonnegative(self.adverse_excursion, "adverse_excursion")
        expected = all(
            (
                self.direction_passed,
                self.horizon_passed,
                self.return_band_passed,
                self.volatility_band_passed,
                self.adverse_excursion_passed,
            )
        )
        if self.broadly_correct != expected:
            raise ValueError("Judgment Evaluation broadly_correct is inconsistent")
        if self.result_id != self.expected_result_id:
            raise ValueError("Judgment Evaluation result_id does not match content")

    @property
    def expected_result_id(self) -> str:
        return f"judgment-evaluation-result-{canonical_hash(self.core_dict())}"

    def core_dict(self) -> dict[str, object]:
        return {
            "schema_version": JUDGMENT_EVALUATION_RESULT_SCHEMA,
            "evaluated_at": _timestamp(self.evaluated_at),
            "specification_id": self.specification_id,
            "specification_hash": self.specification_hash,
            "outcome_hash": self.outcome_hash,
            "horizon_sessions": self.horizon_sessions,
            "realized_terminal_return": str(self.realized_terminal_return),
            "realized_volatility": str(self.realized_volatility),
            "adverse_excursion": str(self.adverse_excursion),
            "direction_passed": self.direction_passed,
            "horizon_passed": self.horizon_passed,
            "return_band_passed": self.return_band_passed,
            "volatility_band_passed": self.volatility_band_passed,
            "adverse_excursion_passed": self.adverse_excursion_passed,
            "broadly_correct": self.broadly_correct,
            "evaluation_only": True,
            "changes_signal_or_execution_admission": False,
        }

    def to_dict(self) -> dict[str, object]:
        return {**self.core_dict(), "result_id": self.result_id}


def evaluate_judgment_band(
    *,
    specification: JudgmentEvaluationBandSpecification,
    artifact: JudgmentArtifact,
    policy_registration_authority: JudgmentEvaluationPolicyRegistrationAuthority,
    evaluated_at: datetime,
    outcome_hash: str,
    horizon_sessions: int,
    realized_terminal_return: Decimal,
    realized_volatility: Decimal,
    adverse_excursion: Decimal,
) -> JudgmentEvaluationResult:
    authoritative_registered_at = policy_registration_authority.assert_authoritative_registration(
        specification.policy_catalog
    )
    if authoritative_registered_at != specification.policy_catalog.registered_at:
        raise ValueError("Judgment Evaluation policy authority returned a different time")
    if specification.judgment_artifact_id != artifact.artifact_id or (
        specification.judgment_artifact_hash != canonical_hash(artifact.to_dict())
    ):
        raise ValueError("Judgment Evaluation specification does not bind the Judgment Artifact")
    if evaluated_at < specification.outcome_open_not_before:
        raise ValueError("Judgment Evaluation outcome cannot open before the registered time")
    _sha256(outcome_hash, "Judgment Evaluation outcome hash")
    direction_passed = (
        realized_terminal_return > 0
        if specification.expected_direction is CandidateDirection.UP
        else realized_terminal_return < 0
    )
    horizon_passed = (
        specification.earliest_horizon_sessions
        <= horizon_sessions
        <= specification.latest_horizon_sessions
    )
    return_band_passed = (
        specification.terminal_return_lower
        <= realized_terminal_return
        <= specification.terminal_return_upper
    )
    volatility_band_passed = (
        specification.realized_volatility_lower
        <= realized_volatility
        <= specification.realized_volatility_upper
    )
    adverse_excursion_passed = adverse_excursion <= specification.maximum_adverse_excursion
    broadly_correct = all(
        (
            direction_passed,
            horizon_passed,
            return_band_passed,
            volatility_band_passed,
            adverse_excursion_passed,
        )
    )
    specification_hash = canonical_hash(specification.to_dict())
    core = {
        "schema_version": JUDGMENT_EVALUATION_RESULT_SCHEMA,
        "evaluated_at": _timestamp(evaluated_at),
        "specification_id": specification.specification_id,
        "specification_hash": specification_hash,
        "outcome_hash": outcome_hash,
        "horizon_sessions": horizon_sessions,
        "realized_terminal_return": str(realized_terminal_return),
        "realized_volatility": str(realized_volatility),
        "adverse_excursion": str(adverse_excursion),
        "direction_passed": direction_passed,
        "horizon_passed": horizon_passed,
        "return_band_passed": return_band_passed,
        "volatility_band_passed": volatility_band_passed,
        "adverse_excursion_passed": adverse_excursion_passed,
        "broadly_correct": broadly_correct,
        "evaluation_only": True,
        "changes_signal_or_execution_admission": False,
    }
    return JudgmentEvaluationResult(
        result_id=f"judgment-evaluation-result-{canonical_hash(core)}",
        evaluated_at=evaluated_at,
        specification_id=specification.specification_id,
        specification_hash=specification_hash,
        outcome_hash=outcome_hash,
        horizon_sessions=horizon_sessions,
        realized_terminal_return=realized_terminal_return,
        realized_volatility=realized_volatility,
        adverse_excursion=adverse_excursion,
        direction_passed=direction_passed,
        horizon_passed=horizon_passed,
        return_band_passed=return_band_passed,
        volatility_band_passed=volatility_band_passed,
        adverse_excursion_passed=adverse_excursion_passed,
        broadly_correct=broadly_correct,
    )


def judgment_evaluation_band_from_dict(
    value: object,
) -> JudgmentEvaluationBandSpecification:
    payload = _object(value, "Judgment Evaluation Band Specification")
    if payload.get("schema_version") != JUDGMENT_EVALUATION_BAND_SCHEMA:
        raise ValueError("unsupported Judgment Evaluation Band schema_version")
    specification = JudgmentEvaluationBandSpecification(
        specification_id=_string(payload, "specification_id"),
        registered_at=_datetime(payload, "registered_at"),
        outcome_open_not_before=_datetime(payload, "outcome_open_not_before"),
        policy_catalog=_policy_catalog(payload.get("policy_catalog")),
        tolerance_policy=_tolerance_policy(payload.get("tolerance_policy")),
        judgment_artifact_id=_string(payload, "judgment_artifact_id"),
        judgment_artifact_hash=_string(payload, "judgment_artifact_hash"),
        judgment_started_at=_datetime(payload, "judgment_started_at"),
        judgment_finished_at=_datetime(payload, "judgment_finished_at"),
        target_id=_string(payload, "target_id"),
        expected_direction=_direction(payload.get("expected_direction")),
        earliest_horizon_sessions=_integer(payload, "earliest_horizon_sessions"),
        latest_horizon_sessions=_integer(payload, "latest_horizon_sessions"),
        terminal_return_lower=_decimal(payload, "terminal_return_lower"),
        terminal_return_upper=_decimal(payload, "terminal_return_upper"),
        realized_volatility_lower=_decimal(payload, "realized_volatility_lower"),
        realized_volatility_upper=_decimal(payload, "realized_volatility_upper"),
        maximum_adverse_excursion=_decimal(payload, "maximum_adverse_excursion"),
        price_basis=_string(payload, "price_basis"),
        volatility_basis=_string(payload, "volatility_basis"),
    )
    if specification.to_dict() != payload:
        raise ValueError("Judgment Evaluation Band does not match the canonical contract")
    return specification


def judgment_evaluation_result_from_dict(value: object) -> JudgmentEvaluationResult:
    payload = _object(value, "Judgment Evaluation Result")
    if payload.get("schema_version") != JUDGMENT_EVALUATION_RESULT_SCHEMA:
        raise ValueError("unsupported Judgment Evaluation Result schema_version")
    result = JudgmentEvaluationResult(
        result_id=_string(payload, "result_id"),
        evaluated_at=_datetime(payload, "evaluated_at"),
        specification_id=_string(payload, "specification_id"),
        specification_hash=_string(payload, "specification_hash"),
        outcome_hash=_string(payload, "outcome_hash"),
        horizon_sessions=_integer(payload, "horizon_sessions"),
        realized_terminal_return=_decimal(payload, "realized_terminal_return"),
        realized_volatility=_decimal(payload, "realized_volatility"),
        adverse_excursion=_decimal(payload, "adverse_excursion"),
        direction_passed=_boolean(payload, "direction_passed"),
        horizon_passed=_boolean(payload, "horizon_passed"),
        return_band_passed=_boolean(payload, "return_band_passed"),
        volatility_band_passed=_boolean(payload, "volatility_band_passed"),
        adverse_excursion_passed=_boolean(payload, "adverse_excursion_passed"),
        broadly_correct=_boolean(payload, "broadly_correct"),
    )
    if result.to_dict() != payload:
        raise ValueError("Judgment Evaluation Result does not match the canonical contract")
    return result


def _policy_catalog(value: object) -> JudgmentEvaluationPolicyCatalog:
    payload = _object(value, "Judgment Evaluation Policy Catalog")
    return JudgmentEvaluationPolicyCatalog(
        catalog_id=_string(payload, "catalog_id"),
        registered_at=_datetime(payload, "registered_at"),
        policies=tuple(
            _tolerance_policy(item)
            for item in _object_list(payload.get("policies"), "Evaluation policies")
        ),
    )


def _tolerance_policy(value: object) -> JudgmentEvaluationTolerancePolicy:
    payload = _object(value, "Judgment Evaluation Tolerance Policy")
    return JudgmentEvaluationTolerancePolicy(
        policy_id=_string(payload, "policy_id"),
        policy_name=_string(payload, "policy_name"),
        policy_version=_string(payload, "policy_version"),
        maximum_latest_horizon_sessions=_integer(
            payload,
            "maximum_latest_horizon_sessions",
        ),
        maximum_horizon_span_sessions=_integer(payload, "maximum_horizon_span_sessions"),
        maximum_return_band_width=_decimal(payload, "maximum_return_band_width"),
        maximum_volatility_band_width=_decimal(payload, "maximum_volatility_band_width"),
        maximum_adverse_excursion=_decimal(payload, "maximum_adverse_excursion"),
        price_basis=_string(payload, "price_basis"),
        volatility_basis=_string(payload, "volatility_basis"),
    )


def _timestamp(value: datetime) -> str:
    require_aware(value, "timestamp")
    return value.isoformat().replace("+00:00", "Z")


def _nonempty(value: str, name: str) -> None:
    if not value or value != value.strip():
        raise ValueError(f"{name} must be a non-empty trimmed string")


def _identifier(value: str, name: str) -> None:
    if re.fullmatch(r"[a-z0-9][a-z0-9._-]*", value) is None:
        raise ValueError(f"{name} must be a lowercase identifier")


def _sha256(value: str, name: str) -> None:
    if re.fullmatch(r"[0-9a-f]{64}", value) is None:
        raise ValueError(f"{name} must be a sha256 hex digest")


def _prefixed_hash(value: str, prefix: str, name: str) -> None:
    if not value.startswith(prefix):
        raise ValueError(f"{name} has an invalid prefix")
    _sha256(value.removeprefix(prefix), name)


def _finite(value: Decimal, name: str) -> None:
    if not value.is_finite():
        raise ValueError(f"{name} must be finite")


def _finite_nonnegative(value: Decimal, name: str) -> None:
    _finite(value, name)
    if value < 0:
        raise ValueError(f"{name} must be non-negative")


def _object(value: object, name: str) -> dict[str, object]:
    if not isinstance(value, Mapping):
        raise TypeError(f"{name} must be an object")
    raw = cast(Mapping[object, object], value)
    if any(not isinstance(key, str) for key in raw):
        raise TypeError(f"{name} keys must be strings")
    return cast(dict[str, object], dict(raw))


def _object_list(value: object, name: str) -> tuple[dict[str, object], ...]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        raise TypeError(f"{name} must be an array")
    return tuple(_object(item, name) for item in cast(Sequence[object], value))


def _string(payload: Mapping[str, object], name: str) -> str:
    value = payload.get(name)
    if not isinstance(value, str):
        raise TypeError(f"{name} must be a string")
    return value


def _integer(payload: Mapping[str, object], name: str) -> int:
    value = payload.get(name)
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{name} must be an integer")
    return value


def _boolean(payload: Mapping[str, object], name: str) -> bool:
    value = payload.get(name)
    if not isinstance(value, bool):
        raise TypeError(f"{name} must be a boolean")
    return value


def _unique(values: tuple[str, ...], name: str) -> None:
    if len(values) != len(set(values)):
        raise ValueError(f"{name} must be unique")


def _datetime(payload: Mapping[str, object], name: str) -> datetime:
    return _parse_datetime(_string(payload, name), name)


def _parse_datetime(value: str, name: str) -> datetime:
    try:
        result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{name} must be an ISO-8601 timestamp") from exc
    require_aware(result, name)
    return result.astimezone(UTC)


def _json_object(value: str) -> dict[str, object]:
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError as exc:
        raise ValueError("stored Judgment Evaluation policy catalog is invalid JSON") from exc
    return _object(parsed, "stored Judgment Evaluation policy catalog")


def _decimal(payload: Mapping[str, object], name: str) -> Decimal:
    value = _string(payload, name)
    try:
        result = Decimal(value)
    except InvalidOperation as exc:
        raise ValueError(f"{name} must be a decimal string") from exc
    if not result.is_finite():
        raise ValueError(f"{name} must be finite")
    return result


def _direction(value: object) -> CandidateDirection:
    if not isinstance(value, str):
        raise TypeError("expected_direction must be a string")
    try:
        return CandidateDirection(value)
    except ValueError as exc:
        raise ValueError("expected_direction is unsupported") from exc
