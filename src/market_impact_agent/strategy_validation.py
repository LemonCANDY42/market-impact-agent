from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, localcontext
from enum import StrEnum
from itertools import pairwise
from pathlib import Path
from typing import cast

from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.backtests import (
    StrategyAdverseExcursionPoint,
    StrategyBacktestArm,
    StrategyBacktestFill,
    StrategyBacktestOutcomeReceipt,
    StrategyBacktestVariant,
    StrategyCapitalPoint,
    reopen_strategy_backtest_outcome,
)
from market_impact_agent.data_inputs import LocalDataSnapshotStore
from market_impact_agent.domain import require_aware
from market_impact_agent.runtime_store import ArtifactStore, RunJournal, RunRecord, RunStatus

STRATEGY_VALIDATION_REGISTRATION_SCHEMA = "market-impact.strategy-validation-registration.v1"
STRATEGY_VALIDATION_REPORT_SCHEMA = "market-impact.strategy-validation-report.v1"
STRATEGY_CASE_RUN_PLAN_SCHEMA = "market-impact.strategy-case-run-plan.v2"
STRATEGY_CASE_TERMINAL_SCHEMA = "market-impact.strategy-case-terminal.v2"
STRATEGY_RUN_SET_SEAL_SCHEMA = "market-impact.strategy-run-set-seal.v2"
STRATEGY_VALIDATION_REPORT_SCHEMA_V2 = "market-impact.strategy-validation-report.v2"
STRATEGY_VALIDATION_DEVELOPMENT_CASES = 8
STRATEGY_VALIDATION_HISTORICAL_CASES = 24
STRATEGY_VALIDATION_HISTORICAL_MINIMUM_REGIMES = 6
STRATEGY_VALIDATION_PROSPECTIVE_CASES = 30
STRATEGY_VALIDATION_PROSPECTIVE_NONEMPTY = 20
STRATEGY_VALIDATION_PROSPECTIVE_MINIMUM_REGIMES = 4
STRATEGY_VALIDATION_RUN_SELECTION_POLICY = "earliest_complete_run_per_case_v1"
STRATEGY_PORTFOLIO_AGGREGATION_POLICY = "equal_weight_active_cases_v1"
STRATEGY_PORTFOLIO_CASE_ORDERING = "root_event_id_then_case_id_v1"
STRATEGY_PORTFOLIO_STARTING_CAPITAL = Decimal("1000000")
STRATEGY_PORTFOLIO_MAXIMUM_SIMULTANEOUS_POSITIONS = 32


class StrategyEvidenceLane(StrEnum):
    RETROSPECTIVE = "retrospective"
    MODELED_PIT = "modeled_pit"
    STRICT_PIT = "strict_pit"
    PROSPECTIVE = "prospective_actual_receipt"


class StrategyValidationProgram(StrEnum):
    HISTORICAL_STRICT = "historical_strict"
    PROSPECTIVE_CONFIRMATION = "prospective_confirmation"


class StrategyCaseRole(StrEnum):
    DEVELOPMENT = "development"
    HISTORICAL_HOLDOUT = "historical_holdout"
    PROSPECTIVE_CONFIRMATION = "prospective_confirmation"


class StrategyValidationDisposition(StrEnum):
    ACCEPTED = "accepted"
    REJECTED = "rejected"
    INCONCLUSIVE = "inconclusive"


@dataclass(frozen=True, slots=True)
class ProspectiveCohortCase:
    case_id: str
    root_event_id: str

    def __post_init__(self) -> None:
        _stable_identifier(self.case_id, "prospective cohort case_id")
        _stable_identifier(self.root_event_id, "prospective cohort root_event_id")

    def to_dict(self) -> dict[str, object]:
        return {"case_id": self.case_id, "root_event_id": self.root_event_id}


@dataclass(frozen=True, slots=True)
class ProspectiveValidationCohort:
    cohort_id: str
    cohort_seal_hash: str
    strategy_epoch_id: str
    qualification_window_id: str
    qualification_policy_hash: str
    qualification_window_open_at: datetime
    cohort_cutoff_at: datetime
    sealed_at: datetime
    append_only_journal_hash: str
    qualification_digest_hash: str
    eligible_cases: tuple[ProspectiveCohortCase, ...]

    def __post_init__(self) -> None:
        _stable_identifier(self.strategy_epoch_id, "prospective cohort strategy_epoch_id")
        if not self.qualification_window_id.startswith("prospective-qualification-window-"):
            raise ValueError("prospective cohort qualification window ID is invalid")
        _sha256(
            self.qualification_window_id.removeprefix("prospective-qualification-window-"),
            "qualification_window_id",
        )
        for name in (
            "qualification_policy_hash",
            "append_only_journal_hash",
            "qualification_digest_hash",
        ):
            _sha256(getattr(self, name), name)
        for name in (
            "qualification_window_open_at",
            "cohort_cutoff_at",
            "sealed_at",
        ):
            require_aware(getattr(self, name), name)
        if self.qualification_window_open_at >= self.cohort_cutoff_at:
            raise ValueError("prospective qualification window must precede its cutoff")
        if self.sealed_at < self.cohort_cutoff_at:
            raise ValueError("prospective cohort cannot be sealed before its cutoff")
        case_ids = tuple(item.case_id for item in self.eligible_cases)
        _require_sorted_unique(case_ids, "prospective cohort cases")
        root_ids = tuple(item.root_event_id for item in self.eligible_cases)
        if len(root_ids) != len(set(root_ids)):
            raise ValueError("prospective cohort root events must be unique")
        expected_hash = canonical_hash(self.core_dict())
        if self.cohort_seal_hash != expected_hash:
            raise ValueError("prospective cohort seal does not match complete cohort content")
        if self.cohort_id != f"prospective-validation-cohort-{expected_hash}":
            raise ValueError("prospective cohort identity does not match its seal")

    def core_dict(self) -> dict[str, object]:
        return {
            "strategy_epoch_id": self.strategy_epoch_id,
            "qualification_window_id": self.qualification_window_id,
            "qualification_policy_hash": self.qualification_policy_hash,
            "qualification_window_open_at": self.qualification_window_open_at.isoformat(),
            "cohort_cutoff_at": self.cohort_cutoff_at.isoformat(),
            "sealed_at": self.sealed_at.isoformat(),
            "append_only_journal_hash": self.append_only_journal_hash,
            "qualification_digest_hash": self.qualification_digest_hash,
            "eligible_cases": [item.to_dict() for item in self.eligible_cases],
        }

    def to_dict(self) -> dict[str, object]:
        return {
            **self.core_dict(),
            "cohort_id": self.cohort_id,
            "cohort_seal_hash": self.cohort_seal_hash,
        }

    @classmethod
    def build(
        cls,
        *,
        strategy_epoch_id: str,
        qualification_window_id: str,
        qualification_policy_hash: str,
        qualification_window_open_at: datetime,
        cohort_cutoff_at: datetime,
        sealed_at: datetime,
        append_only_journal_hash: str,
        qualification_digest_hash: str,
        eligible_cases: tuple[ProspectiveCohortCase, ...],
    ) -> ProspectiveValidationCohort:
        values: dict[str, object] = {
            "strategy_epoch_id": strategy_epoch_id,
            "qualification_window_id": qualification_window_id,
            "qualification_policy_hash": qualification_policy_hash,
            "qualification_window_open_at": qualification_window_open_at.isoformat(),
            "cohort_cutoff_at": cohort_cutoff_at.isoformat(),
            "sealed_at": sealed_at.isoformat(),
            "append_only_journal_hash": append_only_journal_hash,
            "qualification_digest_hash": qualification_digest_hash,
            "eligible_cases": [item.to_dict() for item in eligible_cases],
        }
        seal = canonical_hash(values)
        return cls(
            cohort_id=f"prospective-validation-cohort-{seal}",
            cohort_seal_hash=seal,
            strategy_epoch_id=strategy_epoch_id,
            qualification_window_id=qualification_window_id,
            qualification_policy_hash=qualification_policy_hash,
            qualification_window_open_at=qualification_window_open_at,
            cohort_cutoff_at=cohort_cutoff_at,
            sealed_at=sealed_at,
            append_only_journal_hash=append_only_journal_hash,
            qualification_digest_hash=qualification_digest_hash,
            eligible_cases=eligible_cases,
        )


@dataclass(frozen=True, slots=True)
class StrategyBaselineDefinition:
    baseline_id: str
    definition_hash: str
    configuration_hash: str
    variant: StrategyBacktestVariant

    def __post_init__(self) -> None:
        _stable_identifier(self.baseline_id, "baseline_id")
        _sha256(self.definition_hash, "definition_hash")
        _sha256(self.configuration_hash, "configuration_hash")
        if self.variant.arm is not StrategyBacktestArm.PRIMARY_BASELINE:
            raise ValueError("baseline definition requires a baseline strategy variant")
        if self.variant.baseline_id != self.baseline_id:
            raise ValueError("baseline definition and strategy variant IDs differ")
        if self.configuration_hash != self.variant.configuration_hash:
            raise ValueError("baseline configuration hash differs from exact strategy config")

    def to_dict(self) -> dict[str, object]:
        return {
            "baseline_id": self.baseline_id,
            "definition_hash": self.definition_hash,
            "configuration_hash": self.configuration_hash,
            "variant": self.variant.to_dict(),
        }


@dataclass(frozen=True, slots=True)
class StrategyCaseDefinition:
    case_id: str
    root_event_id: str
    regime: str
    role: StrategyCaseRole
    source_snapshot_id: str | None = None
    evidence_binding_ref: str | None = None

    def __post_init__(self) -> None:
        _stable_identifier(self.case_id, "case_id")
        _stable_identifier(self.root_event_id, "root_event_id")
        _stable_identifier(self.regime, "regime")
        for name in ("source_snapshot_id", "evidence_binding_ref"):
            value = getattr(self, name)
            if value is not None:
                _stable_identifier(value, f"strategy case {name}")

    def to_dict(self) -> dict[str, str | None]:
        return {
            "case_id": self.case_id,
            "root_event_id": self.root_event_id,
            "regime": self.regime,
            "role": self.role.value,
            "source_snapshot_id": self.source_snapshot_id,
            "evidence_binding_ref": self.evidence_binding_ref,
        }


@dataclass(frozen=True, slots=True)
class StrategyValidationRegistration:
    registration_id: str
    strategy_epoch_id: str
    program: StrategyValidationProgram
    model_profile_hash: str
    prompt_hash: str
    skill_catalog_hash: str
    tool_manifest_hash: str
    universe_hash: str
    cost_model_hash: str
    fill_model_hash: str
    candidate_variant: StrategyBacktestVariant
    primary_baseline_id: str
    baseline_definitions: tuple[StrategyBaselineDefinition, ...]
    development_selection_evidence_hash: str
    run_selection_policy: str
    portfolio_aggregation_policy: str
    portfolio_case_ordering: str
    portfolio_starting_capital: Decimal
    portfolio_maximum_simultaneous_positions: int
    case_definitions: tuple[StrategyCaseDefinition, ...]
    prospective_cohort_id: str | None
    prospective_cohort_seal_hash: str | None
    created_at: datetime
    paired_critical_value: Decimal = Decimal("1.714")
    maximum_drawdown_ratio: Decimal = Decimal("0.80")
    maximum_cvar_ratio: Decimal = Decimal("0.85")
    maximum_downside_loss_ratio: Decimal = Decimal("0.50")
    maximum_single_event_share: Decimal = Decimal("0.20")
    execution_capability: str = "none"

    def __post_init__(self) -> None:
        require_aware(self.created_at, "created_at")
        _stable_identifier(self.strategy_epoch_id, "strategy_epoch_id")
        for name in (
            "model_profile_hash",
            "prompt_hash",
            "skill_catalog_hash",
            "tool_manifest_hash",
            "universe_hash",
            "cost_model_hash",
            "fill_model_hash",
            "development_selection_evidence_hash",
        ):
            _sha256(getattr(self, name), name)
        if self.run_selection_policy != STRATEGY_VALIDATION_RUN_SELECTION_POLICY:
            raise ValueError("strategy validation run-selection policy is unsupported")
        if self.portfolio_aggregation_policy != STRATEGY_PORTFOLIO_AGGREGATION_POLICY:
            raise ValueError("strategy portfolio aggregation policy is unsupported")
        if self.portfolio_case_ordering != STRATEGY_PORTFOLIO_CASE_ORDERING:
            raise ValueError("strategy portfolio case ordering is unsupported")
        if self.portfolio_starting_capital != STRATEGY_PORTFOLIO_STARTING_CAPITAL:
            raise ValueError("strategy portfolio starting capital differs from frozen v1 value")
        if (
            self.portfolio_maximum_simultaneous_positions
            != STRATEGY_PORTFOLIO_MAXIMUM_SIMULTANEOUS_POSITIONS
        ):
            raise ValueError("strategy portfolio position cap differs from frozen v1 value")
        if not self.baseline_definitions:
            raise ValueError("strategy validation requires frozen baseline definitions")
        baseline_ids = tuple(item.baseline_id for item in self.baseline_definitions)
        _require_sorted_unique(baseline_ids, "baseline definitions")
        if self.primary_baseline_id not in baseline_ids:
            raise ValueError("primary baseline must be one frozen baseline definition")
        if self.candidate_variant.arm is not StrategyBacktestArm.CANDIDATE:
            raise ValueError("strategy registration requires a candidate strategy variant")
        configuration_hashes = tuple(item.configuration_hash for item in self.baseline_definitions)
        if len(configuration_hashes) != len(set(configuration_hashes)):
            raise ValueError("baseline strategy configurations must be distinct")
        if self.candidate_variant.configuration_hash in configuration_hashes:
            raise ValueError("candidate and baseline strategy configurations must be distinct")
        if not self.case_definitions:
            raise ValueError("strategy validation requires frozen case definitions")
        case_ids = tuple(item.case_id for item in self.case_definitions)
        if case_ids != tuple(sorted(case_ids)) or len(case_ids) != len(set(case_ids)):
            raise ValueError("case definitions must be unique and sorted by case_id")
        root_ids = tuple(item.root_event_id for item in self.case_definitions)
        if len(root_ids) != len(set(root_ids)):
            raise ValueError("development and evaluation cases must not reuse root events")
        development = self.development_cases
        evaluation = self.evaluation_cases
        if len(development) != STRATEGY_VALIDATION_DEVELOPMENT_CASES:
            raise ValueError("strategy validation requires exactly 8 frozen development cases")
        if self.program is StrategyValidationProgram.HISTORICAL_STRICT:
            if (
                self.prospective_cohort_id is not None
                or self.prospective_cohort_seal_hash is not None
            ):
                raise ValueError("historical validation cannot bind a prospective cohort")
            if any(item.role is not StrategyCaseRole.HISTORICAL_HOLDOUT for item in evaluation):
                raise ValueError("historical program requires only historical holdout cases")
            if len(evaluation) != STRATEGY_VALIDATION_HISTORICAL_CASES:
                raise ValueError("historical validation requires exactly 24 frozen holdout cases")
            minimum_regimes = STRATEGY_VALIDATION_HISTORICAL_MINIMUM_REGIMES
        else:
            if self.prospective_cohort_id is None or self.prospective_cohort_seal_hash is None:
                raise ValueError("prospective validation requires a sealed denominator cohort")
            if not self.prospective_cohort_id.startswith("prospective-validation-cohort-"):
                raise ValueError("prospective cohort ID is invalid")
            _sha256(self.prospective_cohort_seal_hash, "prospective_cohort_seal_hash")
            if (
                self.prospective_cohort_id.removeprefix("prospective-validation-cohort-")
                != self.prospective_cohort_seal_hash
            ):
                raise ValueError("prospective cohort ID and seal must match")
            if any(
                item.role is not StrategyCaseRole.PROSPECTIVE_CONFIRMATION for item in evaluation
            ):
                raise ValueError("prospective program requires only prospective confirmation cases")
            if len(evaluation) < STRATEGY_VALIDATION_PROSPECTIVE_CASES:
                raise ValueError("prospective confirmation requires at least 30 frozen clusters")
            minimum_regimes = STRATEGY_VALIDATION_PROSPECTIVE_MINIMUM_REGIMES
        if len({item.regime for item in evaluation}) < minimum_regimes:
            raise ValueError("registered evaluation cohort has insufficient regime coverage")
        exact_thresholds = {
            "paired_critical_value": Decimal("1.714"),
            "maximum_drawdown_ratio": Decimal("0.80"),
            "maximum_cvar_ratio": Decimal("0.85"),
            "maximum_downside_loss_ratio": Decimal("0.50"),
            "maximum_single_event_share": Decimal("0.20"),
        }
        for name, expected in exact_thresholds.items():
            if getattr(self, name) != expected:
                raise ValueError(f"{name} must equal the frozen v1 value {_decimal(expected)}")
        if self.execution_capability != "none":
            raise ValueError("strategy validation registration grants no execution capability")
        if self.registration_id != self.expected_registration_id:
            raise ValueError("strategy validation registration identity does not match content")

    @property
    def development_cases(self) -> tuple[StrategyCaseDefinition, ...]:
        return tuple(
            item for item in self.case_definitions if item.role is StrategyCaseRole.DEVELOPMENT
        )

    @property
    def evaluation_cases(self) -> tuple[StrategyCaseDefinition, ...]:
        return tuple(
            item for item in self.case_definitions if item.role is not StrategyCaseRole.DEVELOPMENT
        )

    @property
    def primary_baseline(self) -> StrategyBaselineDefinition:
        return next(
            item
            for item in self.baseline_definitions
            if item.baseline_id == self.primary_baseline_id
        )

    @property
    def registration_hash(self) -> str:
        return canonical_hash(self.core_dict())

    @property
    def expected_registration_id(self) -> str:
        return f"strategy-validation-registration-{self.registration_hash}"

    def core_dict(self) -> dict[str, object]:
        return {
            "schema_version": STRATEGY_VALIDATION_REGISTRATION_SCHEMA,
            "strategy_epoch_id": self.strategy_epoch_id,
            "program": self.program.value,
            "model_profile_hash": self.model_profile_hash,
            "prompt_hash": self.prompt_hash,
            "skill_catalog_hash": self.skill_catalog_hash,
            "tool_manifest_hash": self.tool_manifest_hash,
            "universe_hash": self.universe_hash,
            "cost_model_hash": self.cost_model_hash,
            "fill_model_hash": self.fill_model_hash,
            "candidate_variant": self.candidate_variant.to_dict(),
            "primary_baseline_id": self.primary_baseline_id,
            "baseline_definitions": [item.to_dict() for item in self.baseline_definitions],
            "development_selection_evidence_hash": self.development_selection_evidence_hash,
            "run_selection_policy": self.run_selection_policy,
            "portfolio_aggregation_policy": self.portfolio_aggregation_policy,
            "portfolio_case_ordering": self.portfolio_case_ordering,
            "portfolio_starting_capital": _decimal(self.portfolio_starting_capital),
            "portfolio_maximum_simultaneous_positions": (
                self.portfolio_maximum_simultaneous_positions
            ),
            "case_definitions": [item.to_dict() for item in self.case_definitions],
            "prospective_cohort_id": self.prospective_cohort_id,
            "prospective_cohort_seal_hash": self.prospective_cohort_seal_hash,
            "created_at": self.created_at.isoformat(),
            "paired_critical_value": _decimal(self.paired_critical_value),
            "maximum_drawdown_ratio": _decimal(self.maximum_drawdown_ratio),
            "maximum_cvar_ratio": _decimal(self.maximum_cvar_ratio),
            "maximum_downside_loss_ratio": _decimal(self.maximum_downside_loss_ratio),
            "maximum_single_event_share": _decimal(self.maximum_single_event_share),
            "execution_capability": self.execution_capability,
        }

    def to_dict(self) -> dict[str, object]:
        return {**self.core_dict(), "registration_id": self.registration_id}

    @classmethod
    def build(
        cls,
        *,
        strategy_epoch_id: str,
        program: StrategyValidationProgram,
        model_profile_hash: str,
        prompt_hash: str,
        skill_catalog_hash: str,
        tool_manifest_hash: str,
        universe_hash: str,
        cost_model_hash: str,
        fill_model_hash: str,
        candidate_variant: StrategyBacktestVariant,
        primary_baseline_id: str,
        baseline_definitions: tuple[StrategyBaselineDefinition, ...],
        development_selection_evidence_hash: str,
        run_selection_policy: str = STRATEGY_VALIDATION_RUN_SELECTION_POLICY,
        portfolio_aggregation_policy: str = STRATEGY_PORTFOLIO_AGGREGATION_POLICY,
        portfolio_case_ordering: str = STRATEGY_PORTFOLIO_CASE_ORDERING,
        portfolio_starting_capital: Decimal = STRATEGY_PORTFOLIO_STARTING_CAPITAL,
        portfolio_maximum_simultaneous_positions: int = (
            STRATEGY_PORTFOLIO_MAXIMUM_SIMULTANEOUS_POSITIONS
        ),
        case_definitions: tuple[StrategyCaseDefinition, ...],
        prospective_cohort: ProspectiveValidationCohort | None = None,
        created_at: datetime,
    ) -> StrategyValidationRegistration:
        values = {
            "schema_version": STRATEGY_VALIDATION_REGISTRATION_SCHEMA,
            "strategy_epoch_id": strategy_epoch_id,
            "program": program.value,
            "model_profile_hash": model_profile_hash,
            "prompt_hash": prompt_hash,
            "skill_catalog_hash": skill_catalog_hash,
            "tool_manifest_hash": tool_manifest_hash,
            "universe_hash": universe_hash,
            "cost_model_hash": cost_model_hash,
            "fill_model_hash": fill_model_hash,
            "candidate_variant": candidate_variant.to_dict(),
            "primary_baseline_id": primary_baseline_id,
            "baseline_definitions": [item.to_dict() for item in baseline_definitions],
            "development_selection_evidence_hash": development_selection_evidence_hash,
            "run_selection_policy": run_selection_policy,
            "portfolio_aggregation_policy": portfolio_aggregation_policy,
            "portfolio_case_ordering": portfolio_case_ordering,
            "portfolio_starting_capital": _decimal(portfolio_starting_capital),
            "portfolio_maximum_simultaneous_positions": (portfolio_maximum_simultaneous_positions),
            "case_definitions": [item.to_dict() for item in case_definitions],
            "prospective_cohort_id": (
                None if prospective_cohort is None else prospective_cohort.cohort_id
            ),
            "prospective_cohort_seal_hash": (
                None if prospective_cohort is None else prospective_cohort.cohort_seal_hash
            ),
            "created_at": created_at.isoformat(),
            "paired_critical_value": "1.714",
            "maximum_drawdown_ratio": "0.80",
            "maximum_cvar_ratio": "0.85",
            "maximum_downside_loss_ratio": "0.50",
            "maximum_single_event_share": "0.20",
            "execution_capability": "none",
        }
        return cls(
            registration_id=f"strategy-validation-registration-{canonical_hash(values)}",
            strategy_epoch_id=strategy_epoch_id,
            program=program,
            model_profile_hash=model_profile_hash,
            prompt_hash=prompt_hash,
            skill_catalog_hash=skill_catalog_hash,
            tool_manifest_hash=tool_manifest_hash,
            universe_hash=universe_hash,
            cost_model_hash=cost_model_hash,
            fill_model_hash=fill_model_hash,
            candidate_variant=candidate_variant,
            primary_baseline_id=primary_baseline_id,
            baseline_definitions=baseline_definitions,
            development_selection_evidence_hash=development_selection_evidence_hash,
            run_selection_policy=run_selection_policy,
            portfolio_aggregation_policy=portfolio_aggregation_policy,
            portfolio_case_ordering=portfolio_case_ordering,
            portfolio_starting_capital=portfolio_starting_capital,
            portfolio_maximum_simultaneous_positions=(portfolio_maximum_simultaneous_positions),
            case_definitions=case_definitions,
            prospective_cohort_id=(
                None if prospective_cohort is None else prospective_cohort.cohort_id
            ),
            prospective_cohort_seal_hash=(
                None if prospective_cohort is None else prospective_cohort.cohort_seal_hash
            ),
            created_at=created_at,
        )


class ProspectiveDenominatorStore:
    """Durable Harness authority for qualified actual-receipt denominator rows."""

    def __init__(self, path: str | Path) -> None:
        self._connection = sqlite3.connect(str(path))
        self._connection.row_factory = sqlite3.Row
        self._connection.executescript(
            """
            PRAGMA foreign_keys = ON;
            CREATE TABLE IF NOT EXISTS prospective_denominator_cas (
                artifact_hash TEXT PRIMARY KEY,
                payload TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS prospective_qualification_windows (
                window_id TEXT PRIMARY KEY,
                strategy_epoch_id TEXT NOT NULL,
                qualification_policy_hash TEXT NOT NULL,
                opened_at TEXT NOT NULL,
                cutoff_at TEXT NOT NULL,
                artifact_hash TEXT NOT NULL REFERENCES prospective_denominator_cas(artifact_hash)
            );
            CREATE TABLE IF NOT EXISTS prospective_qualified_events (
                window_id TEXT NOT NULL REFERENCES prospective_qualification_windows(window_id),
                case_id TEXT NOT NULL,
                root_event_id TEXT NOT NULL,
                qualified_at TEXT NOT NULL,
                trigger_admission_id TEXT NOT NULL,
                trigger_admission_hash TEXT NOT NULL,
                artifact_hash TEXT NOT NULL REFERENCES prospective_denominator_cas(artifact_hash),
                PRIMARY KEY (window_id, case_id),
                UNIQUE (window_id, root_event_id),
                UNIQUE (window_id, trigger_admission_id)
            );
            CREATE TABLE IF NOT EXISTS prospective_denominator_cohorts (
                cohort_id TEXT PRIMARY KEY,
                window_id TEXT NOT NULL UNIQUE
                    REFERENCES prospective_qualification_windows(window_id),
                cohort_seal_hash TEXT NOT NULL,
                artifact_hash TEXT NOT NULL REFERENCES prospective_denominator_cas(artifact_hash)
            );
            """
        )

    def close(self) -> None:
        self._connection.close()

    def register_window(
        self,
        *,
        strategy_epoch_id: str,
        qualification_policy_hash: str,
        opened_at: datetime,
        cutoff_at: datetime,
    ) -> str:
        _stable_identifier(strategy_epoch_id, "prospective window strategy_epoch_id")
        _sha256(qualification_policy_hash, "qualification_policy_hash")
        require_aware(opened_at, "opened_at")
        require_aware(cutoff_at, "cutoff_at")
        if opened_at >= cutoff_at:
            raise ValueError("prospective qualification window must precede its cutoff")
        core = {
            "strategy_epoch_id": strategy_epoch_id,
            "qualification_policy_hash": qualification_policy_hash,
            "opened_at": opened_at.isoformat(),
            "cutoff_at": cutoff_at.isoformat(),
        }
        window_id = f"prospective-qualification-window-{canonical_hash(core)}"
        artifact_hash = self._put_cas({**core, "window_id": window_id})
        with self._connection:
            self._connection.execute(
                "INSERT OR IGNORE INTO prospective_qualification_windows VALUES (?, ?, ?, ?, ?, ?)",
                (
                    window_id,
                    strategy_epoch_id,
                    qualification_policy_hash,
                    opened_at.isoformat(),
                    cutoff_at.isoformat(),
                    artifact_hash,
                ),
            )
        return window_id

    def append_qualified_event(
        self,
        *,
        window_id: str,
        case_id: str,
        root_event_id: str,
        qualified_at: datetime,
        trigger_admission_id: str,
        trigger_admission_hash: str,
    ) -> None:
        _stable_identifier(case_id, "qualified prospective case_id")
        _stable_identifier(root_event_id, "qualified prospective root_event_id")
        require_aware(qualified_at, "qualified_at")
        if not trigger_admission_id.startswith("prospective-trigger-admission-"):
            raise ValueError("qualified event Trigger Admission ID is invalid")
        _sha256(
            trigger_admission_id.removeprefix("prospective-trigger-admission-"),
            "trigger_admission_id",
        )
        _sha256(trigger_admission_hash, "trigger_admission_hash")
        window = self._window(window_id)
        if self._sealed_cohort_row(window_id) is not None:
            raise ValueError("sealed prospective denominator is append-closed")
        opened_at = datetime.fromisoformat(cast(str, window["opened_at"]))
        cutoff_at = datetime.fromisoformat(cast(str, window["cutoff_at"]))
        if qualified_at < opened_at or qualified_at > cutoff_at:
            raise ValueError("qualified event falls outside the registered prospective window")
        core = {
            "window_id": window_id,
            "strategy_epoch_id": cast(str, window["strategy_epoch_id"]),
            "case_id": case_id,
            "root_event_id": root_event_id,
            "qualified_at": qualified_at.isoformat(),
            "trigger_admission_id": trigger_admission_id,
            "trigger_admission_hash": trigger_admission_hash,
        }
        artifact_hash = self._put_cas(cast(dict[str, object], core))
        with self._connection:
            self._connection.execute(
                """
                INSERT OR IGNORE INTO prospective_qualified_events
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    window_id,
                    case_id,
                    root_event_id,
                    qualified_at.isoformat(),
                    trigger_admission_id,
                    trigger_admission_hash,
                    artifact_hash,
                ),
            )
        row = self._connection.execute(
            """
            SELECT artifact_hash FROM prospective_qualified_events
            WHERE window_id = ? AND case_id = ?
            """,
            (window_id, case_id),
        ).fetchone()
        if row is None or cast(str, row["artifact_hash"]) != artifact_hash:
            raise ValueError("qualified prospective event conflicts with append-only history")

    def seal(self, window_id: str, *, sealed_at: datetime) -> ProspectiveValidationCohort:
        require_aware(sealed_at, "sealed_at")
        existing = self._sealed_cohort_row(window_id)
        if existing is not None:
            return self._cohort_from_row(existing)
        window = self._window(window_id)
        cutoff_at = datetime.fromisoformat(cast(str, window["cutoff_at"]))
        if sealed_at < cutoff_at:
            raise ValueError("prospective denominator cannot seal before its cutoff")
        rows = self._event_rows(window_id)
        if not rows:
            raise ValueError("prospective denominator cannot seal an empty qualified cohort")
        event_payloads = [
            cast(dict[str, object], json.loads(cast(str, row["payload"]))) for row in rows
        ]
        eligible_cases = tuple(
            ProspectiveCohortCase(
                case_id=cast(str, payload["case_id"]),
                root_event_id=cast(str, payload["root_event_id"]),
            )
            for payload in event_payloads
        )
        cohort = ProspectiveValidationCohort.build(
            strategy_epoch_id=cast(str, window["strategy_epoch_id"]),
            qualification_window_id=window_id,
            qualification_policy_hash=cast(str, window["qualification_policy_hash"]),
            qualification_window_open_at=datetime.fromisoformat(cast(str, window["opened_at"])),
            cohort_cutoff_at=cutoff_at,
            sealed_at=sealed_at,
            append_only_journal_hash=canonical_hash(
                [cast(str, row["artifact_hash"]) for row in rows]
            ),
            qualification_digest_hash=canonical_hash(event_payloads),
            eligible_cases=eligible_cases,
        )
        artifact_hash = self._put_cas(cohort.to_dict())
        with self._connection:
            self._connection.execute(
                "INSERT INTO prospective_denominator_cohorts VALUES (?, ?, ?, ?)",
                (cohort.cohort_id, window_id, cohort.cohort_seal_hash, artifact_hash),
            )
        return cohort

    def reopen_for_registration(
        self, registration: StrategyValidationRegistration
    ) -> ProspectiveValidationCohort:
        if type(registration) is not StrategyValidationRegistration:
            raise TypeError("prospective denominator requires the concrete Registration")
        cohort_id = registration.prospective_cohort_id
        cohort_seal = registration.prospective_cohort_seal_hash
        if cohort_id is None or cohort_seal is None:
            raise ValueError("Registration does not bind a prospective denominator")
        row = self._connection.execute(
            """
            SELECT cohort.*, cas.payload
            FROM prospective_denominator_cohorts AS cohort
            JOIN prospective_denominator_cas AS cas USING (artifact_hash)
            WHERE cohort.cohort_id = ? AND cohort.cohort_seal_hash = ?
            """,
            (cohort_id, cohort_seal),
        ).fetchone()
        if row is None:
            raise KeyError("prospective denominator cohort is not stored by this Harness")
        cohort = self._cohort_from_row(row)
        if cohort.strategy_epoch_id != registration.strategy_epoch_id:
            raise ValueError("prospective denominator belongs to a different strategy epoch")
        current_rows = self._event_rows(cohort.qualification_window_id)
        current_payloads = [
            cast(dict[str, object], json.loads(cast(str, item["payload"]))) for item in current_rows
        ]
        if cohort.qualification_digest_hash != canonical_hash(current_payloads):
            raise ValueError("prospective denominator qualification digest is stale")
        if cohort.append_only_journal_hash != canonical_hash(
            [cast(str, item["artifact_hash"]) for item in current_rows]
        ):
            raise ValueError("prospective denominator Journal digest is stale")
        return cohort

    def _put_cas(self, payload: dict[str, object]) -> str:
        artifact_hash = canonical_hash(payload)
        encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        with self._connection:
            self._connection.execute(
                "INSERT OR IGNORE INTO prospective_denominator_cas VALUES (?, ?)",
                (artifact_hash, encoded),
            )
        row = self._connection.execute(
            "SELECT payload FROM prospective_denominator_cas WHERE artifact_hash = ?",
            (artifact_hash,),
        ).fetchone()
        if row is None or cast(str, row["payload"]) != encoded:
            raise ValueError("prospective denominator CAS hash collision")
        return artifact_hash

    def _window(self, window_id: str) -> sqlite3.Row:
        row = self._connection.execute(
            "SELECT * FROM prospective_qualification_windows WHERE window_id = ?",
            (window_id,),
        ).fetchone()
        if row is None:
            raise KeyError(f"unknown prospective qualification window: {window_id}")
        return row

    def _sealed_cohort_row(self, window_id: str) -> sqlite3.Row | None:
        return self._connection.execute(
            """
            SELECT cohort.*, cas.payload
            FROM prospective_denominator_cohorts AS cohort
            JOIN prospective_denominator_cas AS cas USING (artifact_hash)
            WHERE cohort.window_id = ?
            """,
            (window_id,),
        ).fetchone()

    def _event_rows(self, window_id: str) -> list[sqlite3.Row]:
        return self._connection.execute(
            """
            SELECT event.artifact_hash, cas.payload
            FROM prospective_qualified_events AS event
            JOIN prospective_denominator_cas AS cas USING (artifact_hash)
            WHERE event.window_id = ?
            ORDER BY event.case_id
            """,
            (window_id,),
        ).fetchall()

    @staticmethod
    def _cohort_from_row(row: sqlite3.Row) -> ProspectiveValidationCohort:
        return _prospective_cohort_from_dict(
            cast(dict[str, object], json.loads(cast(str, row["payload"])))
        )


@dataclass(frozen=True, slots=True)
class StrategyCaseAuthorityBinding:
    binding_id: str
    case_id: str
    evidence_lane: StrategyEvidenceLane
    registration_id: str
    registration_hash: str
    strategy_epoch_id: str
    model_profile_hash: str
    prompt_hash: str
    skill_catalog_hash: str
    tool_manifest_hash: str
    universe_hash: str
    cost_model_hash: str
    fill_model_hash: str
    primary_baseline_id: str
    primary_baseline_definition_hash: str
    primary_baseline_configuration_hash: str
    development_selection_evidence_hash: str
    run_selection_policy: str
    selected_run_started_at: datetime
    data_snapshot_hash: str
    evidence_lineage_hash: str
    qualification_report_hash: str
    run_manifest_hash: str
    admission_hash: str
    candidate_net_return: Decimal
    primary_baseline_net_return: Decimal
    candidate_absolute_pnl: Decimal
    portfolio_metrics_hash: str
    qualification_passed: bool
    admission_passed: bool
    nonempty_execution: bool

    def __post_init__(self) -> None:
        _stable_identifier(self.case_id, "case_id")
        _stable_identifier(self.strategy_epoch_id, "strategy_epoch_id")
        _stable_identifier(self.primary_baseline_id, "primary_baseline_id")
        require_aware(self.selected_run_started_at, "selected_run_started_at")
        if self.run_selection_policy != STRATEGY_VALIDATION_RUN_SELECTION_POLICY:
            raise ValueError("authority binding run-selection policy is unsupported")
        if self.registration_id != f"strategy-validation-registration-{self.registration_hash}":
            raise ValueError("authority binding registration ID and hash do not match")
        for name in (
            "registration_hash",
            "model_profile_hash",
            "prompt_hash",
            "skill_catalog_hash",
            "tool_manifest_hash",
            "universe_hash",
            "cost_model_hash",
            "fill_model_hash",
            "primary_baseline_definition_hash",
            "primary_baseline_configuration_hash",
            "development_selection_evidence_hash",
            "data_snapshot_hash",
            "evidence_lineage_hash",
            "qualification_report_hash",
            "run_manifest_hash",
            "admission_hash",
            "portfolio_metrics_hash",
        ):
            _sha256(getattr(self, name), name)
        for name in (
            "candidate_net_return",
            "primary_baseline_net_return",
            "candidate_absolute_pnl",
        ):
            if not getattr(self, name).is_finite():
                raise ValueError(f"authority binding {name} must be finite")
        if self.candidate_absolute_pnl < 0:
            raise ValueError("authority binding candidate_absolute_pnl must be non-negative")
        if self.binding_id != self.expected_binding_id:
            raise ValueError("strategy case authority binding identity does not match content")

    @property
    def expected_binding_id(self) -> str:
        return f"strategy-case-authority-{canonical_hash(self.core_dict())}"

    def core_dict(self) -> dict[str, object]:
        return {
            "case_id": self.case_id,
            "evidence_lane": self.evidence_lane.value,
            "registration_id": self.registration_id,
            "registration_hash": self.registration_hash,
            "strategy_epoch_id": self.strategy_epoch_id,
            "model_profile_hash": self.model_profile_hash,
            "prompt_hash": self.prompt_hash,
            "skill_catalog_hash": self.skill_catalog_hash,
            "tool_manifest_hash": self.tool_manifest_hash,
            "universe_hash": self.universe_hash,
            "cost_model_hash": self.cost_model_hash,
            "fill_model_hash": self.fill_model_hash,
            "primary_baseline_id": self.primary_baseline_id,
            "primary_baseline_definition_hash": self.primary_baseline_definition_hash,
            "primary_baseline_configuration_hash": self.primary_baseline_configuration_hash,
            "development_selection_evidence_hash": self.development_selection_evidence_hash,
            "run_selection_policy": self.run_selection_policy,
            "selected_run_started_at": self.selected_run_started_at.isoformat(),
            "data_snapshot_hash": self.data_snapshot_hash,
            "evidence_lineage_hash": self.evidence_lineage_hash,
            "qualification_report_hash": self.qualification_report_hash,
            "run_manifest_hash": self.run_manifest_hash,
            "admission_hash": self.admission_hash,
            "candidate_net_return": _decimal(self.candidate_net_return),
            "primary_baseline_net_return": _decimal(self.primary_baseline_net_return),
            "candidate_absolute_pnl": _decimal(self.candidate_absolute_pnl),
            "portfolio_metrics_hash": self.portfolio_metrics_hash,
            "qualification_passed": self.qualification_passed,
            "admission_passed": self.admission_passed,
            "nonempty_execution": self.nonempty_execution,
        }

    @classmethod
    def build(
        cls,
        *,
        registration: StrategyValidationRegistration,
        case_id: str,
        evidence_lane: StrategyEvidenceLane,
        data_snapshot_hash: str,
        evidence_lineage_hash: str,
        qualification_report_hash: str,
        run_manifest_hash: str,
        admission_hash: str,
        selected_run_started_at: datetime,
        candidate_net_return: Decimal,
        primary_baseline_net_return: Decimal,
        candidate_absolute_pnl: Decimal,
        portfolio_metrics_hash: str,
        qualification_passed: bool = True,
        admission_passed: bool = True,
        nonempty_execution: bool = True,
    ) -> StrategyCaseAuthorityBinding:
        values = {
            "case_id": case_id,
            "evidence_lane": evidence_lane.value,
            "registration_id": registration.registration_id,
            "registration_hash": registration.registration_hash,
            "strategy_epoch_id": registration.strategy_epoch_id,
            "model_profile_hash": registration.model_profile_hash,
            "prompt_hash": registration.prompt_hash,
            "skill_catalog_hash": registration.skill_catalog_hash,
            "tool_manifest_hash": registration.tool_manifest_hash,
            "universe_hash": registration.universe_hash,
            "cost_model_hash": registration.cost_model_hash,
            "fill_model_hash": registration.fill_model_hash,
            "primary_baseline_id": registration.primary_baseline_id,
            "primary_baseline_definition_hash": (registration.primary_baseline.definition_hash),
            "primary_baseline_configuration_hash": (
                registration.primary_baseline.configuration_hash
            ),
            "development_selection_evidence_hash": (
                registration.development_selection_evidence_hash
            ),
            "run_selection_policy": registration.run_selection_policy,
            "selected_run_started_at": selected_run_started_at.isoformat(),
            "data_snapshot_hash": data_snapshot_hash,
            "evidence_lineage_hash": evidence_lineage_hash,
            "qualification_report_hash": qualification_report_hash,
            "run_manifest_hash": run_manifest_hash,
            "admission_hash": admission_hash,
            "candidate_net_return": _decimal(candidate_net_return),
            "primary_baseline_net_return": _decimal(primary_baseline_net_return),
            "candidate_absolute_pnl": _decimal(candidate_absolute_pnl),
            "portfolio_metrics_hash": portfolio_metrics_hash,
            "qualification_passed": qualification_passed,
            "admission_passed": admission_passed,
            "nonempty_execution": nonempty_execution,
        }
        return cls(
            binding_id=f"strategy-case-authority-{canonical_hash(values)}",
            case_id=case_id,
            evidence_lane=evidence_lane,
            registration_id=registration.registration_id,
            registration_hash=registration.registration_hash,
            strategy_epoch_id=registration.strategy_epoch_id,
            model_profile_hash=registration.model_profile_hash,
            prompt_hash=registration.prompt_hash,
            skill_catalog_hash=registration.skill_catalog_hash,
            tool_manifest_hash=registration.tool_manifest_hash,
            universe_hash=registration.universe_hash,
            cost_model_hash=registration.cost_model_hash,
            fill_model_hash=registration.fill_model_hash,
            primary_baseline_id=registration.primary_baseline_id,
            primary_baseline_definition_hash=registration.primary_baseline.definition_hash,
            primary_baseline_configuration_hash=(registration.primary_baseline.configuration_hash),
            development_selection_evidence_hash=(registration.development_selection_evidence_hash),
            run_selection_policy=registration.run_selection_policy,
            selected_run_started_at=selected_run_started_at,
            data_snapshot_hash=data_snapshot_hash,
            evidence_lineage_hash=evidence_lineage_hash,
            qualification_report_hash=qualification_report_hash,
            run_manifest_hash=run_manifest_hash,
            admission_hash=admission_hash,
            candidate_net_return=candidate_net_return,
            primary_baseline_net_return=primary_baseline_net_return,
            candidate_absolute_pnl=candidate_absolute_pnl,
            portfolio_metrics_hash=portfolio_metrics_hash,
            qualification_passed=qualification_passed,
            admission_passed=admission_passed,
            nonempty_execution=nonempty_execution,
        )

    def to_dict(self) -> dict[str, object]:
        return {**self.core_dict(), "binding_id": self.binding_id}


@dataclass(frozen=True, slots=True)
class StrategyCaseRunSelection:
    registration_id: str
    case_id: str
    run_selection_policy: str
    eligible_bindings: tuple[StrategyCaseAuthorityBinding, ...]
    eligible_run_set_hash: str
    selected_binding: StrategyCaseAuthorityBinding

    def __post_init__(self) -> None:
        eligible_ids = tuple(item.binding_id for item in self.eligible_bindings)
        _require_sorted_unique(eligible_ids, "eligible strategy run bindings")
        _sha256(self.eligible_run_set_hash, "eligible_run_set_hash")
        if self.eligible_run_set_hash != canonical_hash(list(eligible_ids)):
            raise ValueError("eligible run-set hash does not match the complete stored set")
        if self.selected_binding.binding_id not in eligible_ids:
            raise ValueError("canonical run is not in the complete eligible run set")
        if self.selected_binding.case_id != self.case_id:
            raise ValueError("canonical run belongs to a different case")
        if self.selected_binding.registration_id != self.registration_id:
            raise ValueError("canonical run belongs to a different registration")
        if self.run_selection_policy != STRATEGY_VALIDATION_RUN_SELECTION_POLICY:
            raise ValueError("canonical run selection uses an unsupported policy")

    def core_dict(self) -> dict[str, object]:
        return {
            "registration_id": self.registration_id,
            "case_id": self.case_id,
            "run_selection_policy": self.run_selection_policy,
            "eligible_binding_ids": [item.binding_id for item in self.eligible_bindings],
            "eligible_run_set_hash": self.eligible_run_set_hash,
            "selected_binding": self.selected_binding.to_dict(),
        }


class StrategyCaseRunAuthorityStore:
    """Durable Harness authority for immutable completed strategy-case runs."""

    def __init__(self, path: str | Path) -> None:
        self._connection = sqlite3.connect(str(path))
        self._connection.row_factory = sqlite3.Row
        self._connection.executescript(
            """
            PRAGMA foreign_keys = ON;
            CREATE TABLE IF NOT EXISTS strategy_validation_cas (
                artifact_hash TEXT PRIMARY KEY,
                payload TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS strategy_case_runs (
                registration_id TEXT NOT NULL,
                registration_hash TEXT NOT NULL,
                case_id TEXT NOT NULL,
                binding_id TEXT NOT NULL,
                selected_run_started_at TEXT NOT NULL,
                run_manifest_hash TEXT NOT NULL,
                artifact_hash TEXT NOT NULL REFERENCES strategy_validation_cas(artifact_hash),
                PRIMARY KEY (registration_id, case_id, binding_id),
                UNIQUE (registration_id, case_id, run_manifest_hash)
            );
            """
        )

    def close(self) -> None:
        self._connection.close()

    def register_completed_run(
        self,
        registration: StrategyValidationRegistration,
        binding: StrategyCaseAuthorityBinding,
    ) -> None:
        _verify_binding_registration(binding, registration)
        if binding.case_id not in {item.case_id for item in registration.evaluation_cases}:
            raise ValueError("completed strategy run is not a registered evaluation case")
        payload = binding.to_dict()
        artifact_hash = canonical_hash(payload)
        encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        with self._connection:
            self._connection.execute(
                "INSERT OR IGNORE INTO strategy_validation_cas VALUES (?, ?)",
                (artifact_hash, encoded),
            )
            stored = self._connection.execute(
                "SELECT payload FROM strategy_validation_cas WHERE artifact_hash = ?",
                (artifact_hash,),
            ).fetchone()
            if stored is None or cast(str, stored["payload"]) != encoded:
                raise ValueError("strategy validation CAS hash collision")
            try:
                self._connection.execute(
                    """
                    INSERT OR IGNORE INTO strategy_case_runs VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        registration.registration_id,
                        registration.registration_hash,
                        binding.case_id,
                        binding.binding_id,
                        binding.selected_run_started_at.isoformat(),
                        binding.run_manifest_hash,
                        artifact_hash,
                    ),
                )
            except sqlite3.IntegrityError as error:
                raise ValueError(
                    "Run Manifest was already registered with different content"
                ) from error
            row = self._connection.execute(
                """
                SELECT binding_id FROM strategy_case_runs
                WHERE registration_id = ? AND case_id = ? AND run_manifest_hash = ?
                """,
                (
                    registration.registration_id,
                    binding.case_id,
                    binding.run_manifest_hash,
                ),
            ).fetchone()
            if row is None or cast(str, row["binding_id"]) != binding.binding_id:
                raise ValueError("Run Manifest was already registered with different content")

    def canonical_selection(
        self,
        registration: StrategyValidationRegistration,
        case_id: str,
    ) -> StrategyCaseRunSelection:
        if type(registration) is not StrategyValidationRegistration:
            raise TypeError("strategy run authority requires the concrete Registration")
        rows = self._connection.execute(
            """
            SELECT run.artifact_hash, cas.payload
            FROM strategy_case_runs AS run
            JOIN strategy_validation_cas AS cas USING (artifact_hash)
            WHERE run.registration_id = ? AND run.registration_hash = ? AND run.case_id = ?
            ORDER BY run.selected_run_started_at, run.run_manifest_hash, run.binding_id
            """,
            (registration.registration_id, registration.registration_hash, case_id),
        ).fetchall()
        if not rows:
            raise KeyError(f"no completed canonical strategy run for case: {case_id}")
        bindings = tuple(
            _strategy_case_binding_from_dict(
                cast(dict[str, object], json.loads(cast(str, row["payload"])))
            )
            for row in rows
        )
        for binding in bindings:
            _verify_binding_registration(binding, registration)
        selected = min(
            bindings,
            key=lambda item: (
                item.selected_run_started_at,
                item.run_manifest_hash,
                item.binding_id,
            ),
        )
        eligible_ids = tuple(sorted(item.binding_id for item in bindings))
        return StrategyCaseRunSelection(
            registration_id=registration.registration_id,
            case_id=case_id,
            run_selection_policy=registration.run_selection_policy,
            eligible_bindings=tuple(sorted(bindings, key=lambda item: item.binding_id)),
            eligible_run_set_hash=canonical_hash(list(eligible_ids)),
            selected_binding=selected,
        )


@dataclass(frozen=True, slots=True)
class StrategyCaseOutcome:
    case_id: str
    root_event_id: str
    regime: str
    candidate_net_return: Decimal
    primary_baseline_net_return: Decimal
    candidate_absolute_pnl: Decimal

    def __post_init__(self) -> None:
        for name in ("case_id", "root_event_id", "regime"):
            _stable_identifier(getattr(self, name), name)
        for name in (
            "candidate_net_return",
            "primary_baseline_net_return",
            "candidate_absolute_pnl",
        ):
            if not getattr(self, name).is_finite():
                raise ValueError(f"{name} must be finite")
        if self.candidate_absolute_pnl < 0:
            raise ValueError("candidate_absolute_pnl must be non-negative")


@dataclass(frozen=True, slots=True)
class StrategyPortfolioMetrics:
    candidate_net_return: Decimal
    primary_baseline_net_return: Decimal
    candidate_max_drawdown: Decimal
    primary_baseline_max_drawdown: Decimal
    candidate_cvar95: Decimal
    primary_baseline_cvar95: Decimal
    candidate_sharpe: Decimal
    primary_baseline_sharpe: Decimal
    candidate_sortino: Decimal
    primary_baseline_sortino: Decimal
    candidate_stressed_net_return: Decimal
    primary_baseline_stressed_net_return: Decimal
    candidate_turnover: Decimal
    primary_baseline_turnover: Decimal
    candidate_adverse_excursion: Decimal
    primary_baseline_adverse_excursion: Decimal
    candidate_liquidity_utilization: Decimal
    primary_baseline_liquidity_utilization: Decimal
    avoided_loss: Decimal
    false_avoidance_opportunity_cost: Decimal

    def __post_init__(self) -> None:
        for name in self.__dataclass_fields__:
            value = getattr(self, name)
            if not value.is_finite():
                raise ValueError(f"{name} must be finite")
        for name in (
            "candidate_max_drawdown",
            "primary_baseline_max_drawdown",
            "candidate_cvar95",
            "primary_baseline_cvar95",
            "candidate_turnover",
            "primary_baseline_turnover",
            "candidate_adverse_excursion",
            "primary_baseline_adverse_excursion",
            "candidate_liquidity_utilization",
            "primary_baseline_liquidity_utilization",
            "avoided_loss",
            "false_avoidance_opportunity_cost",
        ):
            if getattr(self, name) < 0:
                raise ValueError(f"{name} must be a non-negative loss magnitude")

    @property
    def metrics_hash(self) -> str:
        return canonical_hash(self.to_dict())

    def to_dict(self) -> dict[str, str]:
        return {name: _decimal(getattr(self, name)) for name in self.__dataclass_fields__}


@dataclass(frozen=True, slots=True)
class StrategyValidationGateResults:
    complete_denominator: bool
    evidence_authority_passed: bool
    prospective_denominator_complete: bool
    regime_coverage: bool
    minimum_nonempty_executions: bool
    candidate_after_cost_return_positive: bool
    primary_baseline_beaten_with_confidence: bool
    stressed_cost_return_positive: bool
    maximum_drawdown_passed: bool
    cvar_passed: bool
    sharpe_passed: bool
    sortino_passed: bool
    downside_loss_passed: bool
    event_concentration_passed: bool
    leave_one_event_passed: bool
    leave_one_regime_passed: bool

    @property
    def all_passed(self) -> bool:
        return all(getattr(self, name) for name in self.__dataclass_fields__)

    def to_dict(self) -> dict[str, bool]:
        return {name: getattr(self, name) for name in self.__dataclass_fields__}


@dataclass(frozen=True, slots=True)
class StrategyValidationReport:
    report_id: str
    registration_id: str
    registration_hash: str
    program: StrategyValidationProgram
    disposition: StrategyValidationDisposition
    evidence_lane: StrategyEvidenceLane
    independent_case_count: int
    nonempty_execution_count: int
    regime_count: int
    candidate_mean_case_return: Decimal | None
    mean_excess_return: Decimal | None
    paired_interval_lower: Decimal | None
    maximum_single_event_share: Decimal | None
    outcomes_hash: str
    authority_bindings_hash: str
    prospective_cohort_seal_hash: str | None
    portfolio_metrics_hash: str
    portfolio_metrics: StrategyPortfolioMetrics
    downside_failure_case_ids: tuple[str, ...]
    gate_results: StrategyValidationGateResults
    reasons: tuple[str, ...]
    execution_capability: str = "none"

    def __post_init__(self) -> None:
        for name in (
            "registration_hash",
            "outcomes_hash",
            "authority_bindings_hash",
            "portfolio_metrics_hash",
        ):
            _sha256(getattr(self, name), name)
        if self.program is StrategyValidationProgram.HISTORICAL_STRICT:
            if self.prospective_cohort_seal_hash is not None:
                raise ValueError("historical report cannot bind a prospective cohort")
        elif self.prospective_cohort_seal_hash is None:
            raise ValueError("prospective report requires the reopened cohort seal")
        else:
            _sha256(self.prospective_cohort_seal_hash, "prospective_cohort_seal_hash")
        if self.portfolio_metrics_hash != self.portfolio_metrics.metrics_hash:
            raise ValueError("portfolio metrics hash does not match content")
        if self.registration_id != f"strategy-validation-registration-{self.registration_hash}":
            raise ValueError("registration ID does not match registration hash")
        _require_sorted_unique_or_empty(self.downside_failure_case_ids, "downside_failure_case_ids")
        if self.reasons != tuple(sorted(set(self.reasons))):
            raise ValueError("reasons must be unique and sorted")
        if self.disposition is StrategyValidationDisposition.ACCEPTED and self.reasons:
            raise ValueError("accepted strategy validation report cannot contain reasons")
        if self.disposition is not StrategyValidationDisposition.ACCEPTED and not self.reasons:
            raise ValueError("non-accepted strategy validation report must explain its disposition")
        if self.execution_capability != "none":
            raise ValueError("strategy validation report grants no execution capability")
        if self.disposition is StrategyValidationDisposition.ACCEPTED:
            _validate_accepted_report(self)
        if self.report_id != self.expected_report_id:
            raise ValueError("strategy validation report identity does not match content")

    @property
    def report_hash(self) -> str:
        return canonical_hash(self.core_dict())

    @property
    def expected_report_id(self) -> str:
        return f"strategy-validation-report-{self.report_hash}"

    def core_dict(self) -> dict[str, object]:
        return {
            "schema_version": STRATEGY_VALIDATION_REPORT_SCHEMA,
            "registration_id": self.registration_id,
            "registration_hash": self.registration_hash,
            "program": self.program.value,
            "disposition": self.disposition.value,
            "evidence_lane": self.evidence_lane.value,
            "independent_case_count": self.independent_case_count,
            "nonempty_execution_count": self.nonempty_execution_count,
            "regime_count": self.regime_count,
            "candidate_mean_case_return": _optional_decimal(self.candidate_mean_case_return),
            "mean_excess_return": _optional_decimal(self.mean_excess_return),
            "paired_interval_lower": _optional_decimal(self.paired_interval_lower),
            "maximum_single_event_share": _optional_decimal(self.maximum_single_event_share),
            "outcomes_hash": self.outcomes_hash,
            "authority_bindings_hash": self.authority_bindings_hash,
            "prospective_cohort_seal_hash": self.prospective_cohort_seal_hash,
            "portfolio_metrics_hash": self.portfolio_metrics_hash,
            "portfolio_metrics": self.portfolio_metrics.to_dict(),
            "downside_failure_case_ids": list(self.downside_failure_case_ids),
            "gate_results": self.gate_results.to_dict(),
            "reasons": list(self.reasons),
            "execution_capability": self.execution_capability,
        }

    def to_dict(self) -> dict[str, object]:
        return {**self.core_dict(), "report_id": self.report_id}


def evaluate_strategy_validation(
    registration: StrategyValidationRegistration,
    outcomes: tuple[StrategyCaseOutcome, ...],
    portfolio: StrategyPortfolioMetrics,
    *,
    authority: StrategyCaseRunAuthorityStore,
    prospective_denominator_store: ProspectiveDenominatorStore | None = None,
) -> StrategyValidationReport:
    """Evaluate one frozen program from Harness-reopened case authorities."""
    if type(authority) is not StrategyCaseRunAuthorityStore:
        raise TypeError("strategy promotion requires the concrete durable run authority store")
    reasons: set[str] = set()
    definitions = {item.case_id: item for item in registration.evaluation_cases}
    by_case: dict[str, StrategyCaseOutcome] = {}
    bindings: dict[str, StrategyCaseAuthorityBinding] = {}
    selections: dict[str, StrategyCaseRunSelection] = {}
    expected_lane = _expected_lane(registration.program)
    prospective_cohort_seal_hash: str | None = None
    prospective_denominator_complete = True
    denominator_valid = True
    if registration.program is StrategyValidationProgram.PROSPECTIVE_CONFIRMATION:
        if type(prospective_denominator_store) is not ProspectiveDenominatorStore:
            raise TypeError(
                "prospective validation requires the concrete durable denominator store"
            )
        cohort_id = registration.prospective_cohort_id
        cohort_seal = registration.prospective_cohort_seal_hash
        if cohort_id is None or cohort_seal is None:
            raise AssertionError("validated prospective registration lacks its cohort")
        cohort = prospective_denominator_store.reopen_for_registration(registration)
        if cohort.cohort_id != cohort_id or cohort.cohort_seal_hash != cohort_seal:
            raise ValueError("Harness denominator authority reopened a different cohort")
        if cohort.strategy_epoch_id != registration.strategy_epoch_id:
            raise ValueError("prospective cohort belongs to a different strategy epoch")
        if registration.created_at < cohort.sealed_at:
            raise ValueError("prospective registration predates its sealed denominator cohort")
        registered_cases = tuple(
            (item.case_id, item.root_event_id) for item in registration.evaluation_cases
        )
        cohort_cases = tuple((item.case_id, item.root_event_id) for item in cohort.eligible_cases)
        if registered_cases != cohort_cases:
            raise ValueError(
                "prospective registration omitted or substituted a qualifying cohort case"
            )
        prospective_cohort_seal_hash = cohort.cohort_seal_hash
    for outcome in outcomes:
        if outcome.case_id in by_case:
            reasons.add(f"duplicate_case:{outcome.case_id}")
            denominator_valid = False
            continue
        by_case[outcome.case_id] = outcome
        definition = definitions.get(outcome.case_id)
        if definition is None:
            reasons.add(f"unexpected_evaluation_case:{outcome.case_id}")
            denominator_valid = False
            continue
        if outcome.root_event_id != definition.root_event_id:
            reasons.add(f"root_event_mismatch:{outcome.case_id}")
            denominator_valid = False
        if outcome.regime != definition.regime:
            reasons.add(f"regime_mismatch:{outcome.case_id}")
            denominator_valid = False
        selection = authority.canonical_selection(registration, outcome.case_id)
        _verify_run_selection(selection)
        binding = selection.selected_binding
        if binding.case_id != outcome.case_id:
            raise ValueError("Harness authority selected a different strategy case")
        _verify_case_run_binding(binding, registration, outcome, portfolio)
        selections[outcome.case_id] = selection
        bindings[outcome.case_id] = binding
        if binding.evidence_lane is not expected_lane:
            reasons.add(f"evidence_authority_lane_mismatch:{outcome.case_id}")
        if not binding.qualification_passed:
            reasons.add(f"qualification_not_passed:{outcome.case_id}")
        if not binding.admission_passed:
            reasons.add(f"admission_not_passed:{outcome.case_id}")

    expected_ids = set(definitions)
    actual_ids = set(by_case)
    for missing in sorted(expected_ids - actual_ids):
        reasons.add(f"missing_evaluation_case:{missing}")
        denominator_valid = False
    regimes = {definitions[item].regime for item in actual_ids & expected_ids}
    complete = tuple(
        by_case[item.case_id] for item in registration.evaluation_cases if item.case_id in by_case
    )
    minimum_cases, minimum_regimes, minimum_nonempty = _program_thresholds(registration.program)
    complete_denominator = (
        denominator_valid
        and actual_ids == expected_ids
        and len(outcomes) == len(registration.evaluation_cases)
    )
    authority_passed = len(bindings) == len(registration.evaluation_cases) and all(
        item.evidence_lane is expected_lane and item.qualification_passed and item.admission_passed
        for item in bindings.values()
    )
    nonempty_count = sum(item.nonempty_execution for item in bindings.values())
    if len(regimes) < minimum_regimes:
        reasons.add("insufficient_regime_coverage")
    if nonempty_count < minimum_nonempty:
        reasons.add("insufficient_nonempty_executions")

    candidate_mean: Decimal | None = None
    mean_excess: Decimal | None = None
    interval_lower: Decimal | None = None
    event_share: Decimal | None = None
    downside_failure_case_ids: set[str] = set()
    leave_one_event_passed = False
    leave_one_regime_passed = False
    if complete_denominator and len(complete) >= minimum_cases:
        with localcontext() as context:
            context.prec = 50
            count = Decimal(len(complete))
            candidate_mean = (
                sum((item.candidate_net_return for item in complete), Decimal(0)) / count
            )
            differences = tuple(
                item.candidate_net_return - item.primary_baseline_net_return for item in complete
            )
            mean_excess = sum(differences, Decimal(0)) / count
            variance = sum(
                ((item - mean_excess) ** 2 for item in differences), Decimal(0)
            ) / Decimal(len(complete) - 1)
            interval_lower = (
                mean_excess - registration.paired_critical_value * (variance / count).sqrt()
            )
            total_absolute_pnl = sum((item.candidate_absolute_pnl for item in complete), Decimal(0))
            event_share = (
                Decimal(0)
                if total_absolute_pnl == 0
                else max(item.candidate_absolute_pnl for item in complete) / total_absolute_pnl
            )
            _apply_economic_failures(
                reasons,
                registration,
                complete,
                portfolio,
                candidate_mean,
                mean_excess,
                interval_lower,
                event_share,
                downside_failure_case_ids,
            )
            leave_one_event_passed = _leave_one_out_positive(complete)
            if not leave_one_event_passed:
                reasons.add("leave_one_event_conclusion_flips")
            leave_one_regime_passed = _leave_one_regime_positive(complete)
            if not leave_one_regime_passed:
                reasons.add("leave_one_regime_conclusion_flips")

    ordered_reasons = tuple(sorted(reasons))
    if not ordered_reasons:
        disposition = StrategyValidationDisposition.ACCEPTED
    elif _inconclusive_reasons_only(ordered_reasons):
        disposition = StrategyValidationDisposition.INCONCLUSIVE
    else:
        disposition = StrategyValidationDisposition.REJECTED
    outcomes_hash = canonical_hash(
        [_outcome_dict(item) for item in sorted(outcomes, key=lambda item: item.case_id)]
    )
    authority_bindings_hash = canonical_hash(
        [selections[item].core_dict() for item in sorted(selections)]
    )
    gate_results = _gate_results(
        registration,
        portfolio,
        candidate_mean,
        mean_excess,
        interval_lower,
        event_share,
        complete_denominator,
        authority_passed,
        prospective_denominator_complete,
        len(regimes),
        nonempty_count,
        minimum_regimes,
        minimum_nonempty,
        downside_failure_case_ids,
        leave_one_event_passed,
        leave_one_regime_passed,
    )
    core: dict[str, object] = {
        "schema_version": STRATEGY_VALIDATION_REPORT_SCHEMA,
        "registration_id": registration.registration_id,
        "registration_hash": registration.registration_hash,
        "program": registration.program.value,
        "disposition": disposition.value,
        "evidence_lane": expected_lane.value,
        "independent_case_count": len(by_case),
        "nonempty_execution_count": nonempty_count,
        "regime_count": len(regimes),
        "candidate_mean_case_return": _optional_decimal(candidate_mean),
        "mean_excess_return": _optional_decimal(mean_excess),
        "paired_interval_lower": _optional_decimal(interval_lower),
        "maximum_single_event_share": _optional_decimal(event_share),
        "outcomes_hash": outcomes_hash,
        "authority_bindings_hash": authority_bindings_hash,
        "prospective_cohort_seal_hash": prospective_cohort_seal_hash,
        "portfolio_metrics_hash": portfolio.metrics_hash,
        "portfolio_metrics": portfolio.to_dict(),
        "downside_failure_case_ids": sorted(downside_failure_case_ids),
        "gate_results": gate_results.to_dict(),
        "reasons": list(ordered_reasons),
        "execution_capability": "none",
    }
    return StrategyValidationReport(
        report_id=f"strategy-validation-report-{canonical_hash(core)}",
        registration_id=registration.registration_id,
        registration_hash=registration.registration_hash,
        program=registration.program,
        disposition=disposition,
        evidence_lane=expected_lane,
        independent_case_count=len(by_case),
        nonempty_execution_count=nonempty_count,
        regime_count=len(regimes),
        candidate_mean_case_return=candidate_mean,
        mean_excess_return=mean_excess,
        paired_interval_lower=interval_lower,
        maximum_single_event_share=event_share,
        outcomes_hash=outcomes_hash,
        authority_bindings_hash=authority_bindings_hash,
        prospective_cohort_seal_hash=prospective_cohort_seal_hash,
        portfolio_metrics_hash=portfolio.metrics_hash,
        portfolio_metrics=portfolio,
        downside_failure_case_ids=tuple(sorted(downside_failure_case_ids)),
        gate_results=gate_results,
        reasons=ordered_reasons,
    )


def revalidate_strategy_validation_report(
    report: StrategyValidationReport,
    registration: StrategyValidationRegistration,
    outcomes: tuple[StrategyCaseOutcome, ...],
    portfolio: StrategyPortfolioMetrics,
    *,
    authority: StrategyCaseRunAuthorityStore,
    prospective_denominator_store: ProspectiveDenominatorStore | None = None,
) -> None:
    expected = evaluate_strategy_validation(
        registration,
        outcomes,
        portfolio,
        authority=authority,
        prospective_denominator_store=prospective_denominator_store,
    )
    if report != expected:
        raise ValueError("strategy validation report does not match recomputed Harness authority")


def _verify_run_selection(selection: StrategyCaseRunSelection) -> None:
    eligible_ids = tuple(item.binding_id for item in selection.eligible_bindings)
    if eligible_ids != tuple(sorted(set(eligible_ids))):
        raise ValueError("Harness run selection did not reopen the complete sorted eligible set")
    if selection.eligible_run_set_hash != canonical_hash(list(eligible_ids)):
        raise ValueError("Harness run selection eligible-set hash is invalid")
    expected = min(
        selection.eligible_bindings,
        key=lambda item: (
            item.selected_run_started_at,
            item.run_manifest_hash,
            item.binding_id,
        ),
    )
    if selection.selected_binding != expected:
        raise ValueError("Harness run authority did not select the earliest complete run")


def _verify_binding_registration(
    binding: StrategyCaseAuthorityBinding,
    registration: StrategyValidationRegistration,
) -> None:
    primary = registration.primary_baseline
    expected: tuple[object, ...] = (
        registration.registration_id,
        registration.registration_hash,
        registration.strategy_epoch_id,
        registration.model_profile_hash,
        registration.prompt_hash,
        registration.skill_catalog_hash,
        registration.tool_manifest_hash,
        registration.universe_hash,
        registration.cost_model_hash,
        registration.fill_model_hash,
        registration.primary_baseline_id,
        primary.definition_hash,
        primary.configuration_hash,
        registration.development_selection_evidence_hash,
        registration.run_selection_policy,
    )
    actual: tuple[object, ...] = (
        binding.registration_id,
        binding.registration_hash,
        binding.strategy_epoch_id,
        binding.model_profile_hash,
        binding.prompt_hash,
        binding.skill_catalog_hash,
        binding.tool_manifest_hash,
        binding.universe_hash,
        binding.cost_model_hash,
        binding.fill_model_hash,
        binding.primary_baseline_id,
        binding.primary_baseline_definition_hash,
        binding.primary_baseline_configuration_hash,
        binding.development_selection_evidence_hash,
        binding.run_selection_policy,
    )
    if actual != expected:
        raise ValueError("completed strategy run belongs to a different frozen Registration")


def _verify_case_run_binding(
    binding: StrategyCaseAuthorityBinding,
    registration: StrategyValidationRegistration,
    outcome: StrategyCaseOutcome,
    portfolio: StrategyPortfolioMetrics,
) -> None:
    _verify_binding_registration(binding, registration)
    primary = registration.primary_baseline
    expected: tuple[object, ...] = (
        registration.registration_id,
        registration.registration_hash,
        registration.strategy_epoch_id,
        registration.model_profile_hash,
        registration.prompt_hash,
        registration.skill_catalog_hash,
        registration.tool_manifest_hash,
        registration.universe_hash,
        registration.cost_model_hash,
        registration.fill_model_hash,
        registration.primary_baseline_id,
        primary.definition_hash,
        primary.configuration_hash,
        registration.development_selection_evidence_hash,
        registration.run_selection_policy,
        outcome.candidate_net_return,
        outcome.primary_baseline_net_return,
        outcome.candidate_absolute_pnl,
        portfolio.metrics_hash,
    )
    actual: tuple[object, ...] = (
        binding.registration_id,
        binding.registration_hash,
        binding.strategy_epoch_id,
        binding.model_profile_hash,
        binding.prompt_hash,
        binding.skill_catalog_hash,
        binding.tool_manifest_hash,
        binding.universe_hash,
        binding.cost_model_hash,
        binding.fill_model_hash,
        binding.primary_baseline_id,
        binding.primary_baseline_definition_hash,
        binding.primary_baseline_configuration_hash,
        binding.development_selection_evidence_hash,
        binding.run_selection_policy,
        binding.candidate_net_return,
        binding.primary_baseline_net_return,
        binding.candidate_absolute_pnl,
        binding.portfolio_metrics_hash,
    )
    if actual != expected:
        raise ValueError(
            "Harness case authority is bound to a different epoch, run outcome, or portfolio"
        )


def _apply_economic_failures(
    reasons: set[str],
    registration: StrategyValidationRegistration,
    outcomes: tuple[StrategyCaseOutcome, ...],
    portfolio: StrategyPortfolioMetrics,
    candidate_mean: Decimal,
    mean_excess: Decimal,
    interval_lower: Decimal,
    event_share: Decimal,
    downside_failure_case_ids: set[str],
) -> None:
    if candidate_mean <= 0 or portfolio.candidate_net_return <= 0:
        reasons.add("candidate_after_cost_return_not_positive")
    if mean_excess <= 0 or interval_lower <= 0:
        reasons.add("primary_baseline_not_beaten_with_confidence")
    if portfolio.candidate_stressed_net_return <= 0:
        reasons.add("stressed_after_cost_return_not_positive")
    if event_share > registration.maximum_single_event_share:
        reasons.add("single_event_concentration_exceeded")
    if not _ratio_passes(
        portfolio.candidate_max_drawdown,
        portfolio.primary_baseline_max_drawdown,
        registration.maximum_drawdown_ratio,
    ):
        reasons.add("maximum_drawdown_not_improved")
    if not _ratio_passes(
        portfolio.candidate_cvar95,
        portfolio.primary_baseline_cvar95,
        registration.maximum_cvar_ratio,
    ):
        reasons.add("cvar_not_improved")
    if portfolio.candidate_sharpe <= portfolio.primary_baseline_sharpe:
        reasons.add("sharpe_not_improved")
    if portfolio.candidate_sortino <= portfolio.primary_baseline_sortino:
        reasons.add("sortino_not_improved")
    for item in outcomes:
        baseline_loss = max(Decimal(0), -item.primary_baseline_net_return)
        candidate_loss = max(Decimal(0), -item.candidate_net_return)
        if (
            baseline_loss > 0
            and candidate_loss > baseline_loss * registration.maximum_downside_loss_ratio
        ):
            downside_failure_case_ids.add(item.case_id)
            reasons.add(f"downside_loss_not_halved:{item.case_id}")


def _gate_results(
    registration: StrategyValidationRegistration,
    portfolio: StrategyPortfolioMetrics,
    candidate_mean: Decimal | None,
    mean_excess: Decimal | None,
    interval_lower: Decimal | None,
    event_share: Decimal | None,
    complete_denominator: bool,
    authority_passed: bool,
    prospective_denominator_complete: bool,
    regime_count: int,
    nonempty_count: int,
    minimum_regimes: int,
    minimum_nonempty: int,
    downside_failures: set[str],
    leave_one_event_passed: bool,
    leave_one_regime_passed: bool,
) -> StrategyValidationGateResults:
    economics_evaluated = (
        complete_denominator
        and candidate_mean is not None
        and mean_excess is not None
        and interval_lower is not None
        and event_share is not None
    )
    candidate_after_cost_return_positive = False
    primary_baseline_beaten_with_confidence = False
    stressed_cost_return_positive = False
    maximum_drawdown_passed = False
    cvar_passed = False
    sharpe_passed = False
    sortino_passed = False
    downside_loss_passed = False
    event_concentration_passed = False
    evaluated_leave_one_event_passed = False
    evaluated_leave_one_regime_passed = False
    if economics_evaluated:
        assert candidate_mean is not None
        assert mean_excess is not None
        assert interval_lower is not None
        assert event_share is not None
        candidate_after_cost_return_positive = (
            candidate_mean > 0 and portfolio.candidate_net_return > 0
        )
        primary_baseline_beaten_with_confidence = mean_excess > 0 and interval_lower > 0
        stressed_cost_return_positive = portfolio.candidate_stressed_net_return > 0
        maximum_drawdown_passed = _ratio_passes(
            portfolio.candidate_max_drawdown,
            portfolio.primary_baseline_max_drawdown,
            registration.maximum_drawdown_ratio,
        )
        cvar_passed = _ratio_passes(
            portfolio.candidate_cvar95,
            portfolio.primary_baseline_cvar95,
            registration.maximum_cvar_ratio,
        )
        sharpe_passed = portfolio.candidate_sharpe > portfolio.primary_baseline_sharpe
        sortino_passed = portfolio.candidate_sortino > portfolio.primary_baseline_sortino
        downside_loss_passed = not downside_failures
        event_concentration_passed = event_share <= registration.maximum_single_event_share
        evaluated_leave_one_event_passed = leave_one_event_passed
        evaluated_leave_one_regime_passed = leave_one_regime_passed
    return StrategyValidationGateResults(
        complete_denominator=complete_denominator,
        evidence_authority_passed=authority_passed,
        prospective_denominator_complete=prospective_denominator_complete,
        regime_coverage=regime_count >= minimum_regimes,
        minimum_nonempty_executions=nonempty_count >= minimum_nonempty,
        candidate_after_cost_return_positive=candidate_after_cost_return_positive,
        primary_baseline_beaten_with_confidence=primary_baseline_beaten_with_confidence,
        stressed_cost_return_positive=stressed_cost_return_positive,
        maximum_drawdown_passed=maximum_drawdown_passed,
        cvar_passed=cvar_passed,
        sharpe_passed=sharpe_passed,
        sortino_passed=sortino_passed,
        downside_loss_passed=downside_loss_passed,
        event_concentration_passed=event_concentration_passed,
        leave_one_event_passed=evaluated_leave_one_event_passed,
        leave_one_regime_passed=evaluated_leave_one_regime_passed,
    )


def _validate_accepted_report(report: StrategyValidationReport) -> None:
    expected_lane = _expected_lane(report.program)
    minimum_cases, minimum_regimes, minimum_nonempty = _program_thresholds(report.program)
    if report.evidence_lane is not expected_lane:
        raise ValueError("accepted strategy validation has the wrong evidence lane")
    if report.independent_case_count < minimum_cases:
        raise ValueError("accepted strategy validation has an incomplete denominator")
    if (
        report.program is StrategyValidationProgram.HISTORICAL_STRICT
        and report.independent_case_count != minimum_cases
    ):
        raise ValueError("historical acceptance requires exactly 24 independent cases")
    if report.regime_count < minimum_regimes or report.nonempty_execution_count < minimum_nonempty:
        raise ValueError("accepted strategy validation misses its program thresholds")
    required = (
        report.candidate_mean_case_return,
        report.mean_excess_return,
        report.paired_interval_lower,
        report.maximum_single_event_share,
    )
    if any(value is None for value in required):
        raise ValueError("accepted strategy validation requires complete gate metrics")
    candidate_mean, mean_excess, interval_lower, event_share = required
    if (
        candidate_mean is None
        or mean_excess is None
        or interval_lower is None
        or event_share is None
    ):
        raise AssertionError("unreachable optional narrowing")
    metrics = report.portfolio_metrics
    if candidate_mean <= 0 or mean_excess <= 0 or interval_lower <= 0:
        raise ValueError("accepted strategy validation requires positive return evidence")
    if event_share > Decimal("0.20"):
        raise ValueError("accepted strategy validation exceeds event concentration")
    if metrics.candidate_net_return <= 0 or metrics.candidate_stressed_net_return <= 0:
        raise ValueError("accepted strategy validation requires positive after-cost returns")
    if not _ratio_passes(
        metrics.candidate_max_drawdown,
        metrics.primary_baseline_max_drawdown,
        Decimal("0.80"),
    ):
        raise ValueError("accepted strategy validation exceeds drawdown gate")
    if not _ratio_passes(
        metrics.candidate_cvar95,
        metrics.primary_baseline_cvar95,
        Decimal("0.85"),
    ):
        raise ValueError("accepted strategy validation exceeds CVaR gate")
    if (
        metrics.candidate_sharpe <= metrics.primary_baseline_sharpe
        or metrics.candidate_sortino <= metrics.primary_baseline_sortino
    ):
        raise ValueError("accepted strategy validation requires improved risk-adjusted returns")
    if report.downside_failure_case_ids or not report.gate_results.all_passed:
        raise ValueError("accepted strategy validation requires every registered gate to pass")


def _program_thresholds(program: StrategyValidationProgram) -> tuple[int, int, int]:
    if program is StrategyValidationProgram.HISTORICAL_STRICT:
        return (
            STRATEGY_VALIDATION_HISTORICAL_CASES,
            STRATEGY_VALIDATION_HISTORICAL_MINIMUM_REGIMES,
            0,
        )
    return (
        STRATEGY_VALIDATION_PROSPECTIVE_CASES,
        STRATEGY_VALIDATION_PROSPECTIVE_MINIMUM_REGIMES,
        STRATEGY_VALIDATION_PROSPECTIVE_NONEMPTY,
    )


def _expected_lane(program: StrategyValidationProgram) -> StrategyEvidenceLane:
    if program is StrategyValidationProgram.HISTORICAL_STRICT:
        return StrategyEvidenceLane.STRICT_PIT
    return StrategyEvidenceLane.PROSPECTIVE


def _outcome_dict(item: StrategyCaseOutcome) -> dict[str, str]:
    return {
        "case_id": item.case_id,
        "root_event_id": item.root_event_id,
        "regime": item.regime,
        "candidate_net_return": _decimal(item.candidate_net_return),
        "primary_baseline_net_return": _decimal(item.primary_baseline_net_return),
        "candidate_absolute_pnl": _decimal(item.candidate_absolute_pnl),
    }


def _leave_one_out_positive(outcomes: tuple[StrategyCaseOutcome, ...]) -> bool:
    for omitted in outcomes:
        retained = tuple(item for item in outcomes if item.case_id != omitted.case_id)
        excess = sum(
            (item.candidate_net_return - item.primary_baseline_net_return for item in retained),
            Decimal(0),
        )
        if not retained or excess <= 0:
            return False
    return True


def _leave_one_regime_positive(outcomes: tuple[StrategyCaseOutcome, ...]) -> bool:
    for regime in {item.regime for item in outcomes}:
        retained = tuple(item for item in outcomes if item.regime != regime)
        excess = sum(
            (item.candidate_net_return - item.primary_baseline_net_return for item in retained),
            Decimal(0),
        )
        if not retained or excess <= 0:
            return False
    return True


def _ratio_passes(candidate: Decimal, baseline: Decimal, maximum_ratio: Decimal) -> bool:
    if baseline == 0:
        return candidate == 0
    return candidate <= baseline * maximum_ratio


def _inconclusive_reasons_only(reasons: tuple[str, ...]) -> bool:
    prefixes = (
        "duplicate_case:",
        "unexpected_evaluation_case:",
        "missing_evaluation_case:",
        "root_event_mismatch:",
        "regime_mismatch:",
        "evidence_authority_lane_mismatch:",
        "qualification_not_passed:",
        "admission_not_passed:",
        "insufficient_regime_coverage",
        "insufficient_nonempty_executions",
    )
    return all(reason.startswith(prefixes) for reason in reasons)


def _decimal(value: Decimal) -> str:
    """Canonical fixed-point Decimal form; never emit exponent notation."""
    return format(value, "f")


def _optional_decimal(value: Decimal | None) -> str | None:
    return None if value is None else _decimal(value)


def _strategy_case_binding_from_dict(
    payload: dict[str, object],
) -> StrategyCaseAuthorityBinding:
    return StrategyCaseAuthorityBinding(
        binding_id=cast(str, payload["binding_id"]),
        case_id=cast(str, payload["case_id"]),
        evidence_lane=StrategyEvidenceLane(cast(str, payload["evidence_lane"])),
        registration_id=cast(str, payload["registration_id"]),
        registration_hash=cast(str, payload["registration_hash"]),
        strategy_epoch_id=cast(str, payload["strategy_epoch_id"]),
        model_profile_hash=cast(str, payload["model_profile_hash"]),
        prompt_hash=cast(str, payload["prompt_hash"]),
        skill_catalog_hash=cast(str, payload["skill_catalog_hash"]),
        tool_manifest_hash=cast(str, payload["tool_manifest_hash"]),
        universe_hash=cast(str, payload["universe_hash"]),
        cost_model_hash=cast(str, payload["cost_model_hash"]),
        fill_model_hash=cast(str, payload["fill_model_hash"]),
        primary_baseline_id=cast(str, payload["primary_baseline_id"]),
        primary_baseline_definition_hash=cast(str, payload["primary_baseline_definition_hash"]),
        primary_baseline_configuration_hash=cast(
            str, payload["primary_baseline_configuration_hash"]
        ),
        development_selection_evidence_hash=cast(
            str, payload["development_selection_evidence_hash"]
        ),
        run_selection_policy=cast(str, payload["run_selection_policy"]),
        selected_run_started_at=datetime.fromisoformat(
            cast(str, payload["selected_run_started_at"])
        ),
        data_snapshot_hash=cast(str, payload["data_snapshot_hash"]),
        evidence_lineage_hash=cast(str, payload["evidence_lineage_hash"]),
        qualification_report_hash=cast(str, payload["qualification_report_hash"]),
        run_manifest_hash=cast(str, payload["run_manifest_hash"]),
        admission_hash=cast(str, payload["admission_hash"]),
        candidate_net_return=Decimal(cast(str, payload["candidate_net_return"])),
        primary_baseline_net_return=Decimal(cast(str, payload["primary_baseline_net_return"])),
        candidate_absolute_pnl=Decimal(cast(str, payload["candidate_absolute_pnl"])),
        portfolio_metrics_hash=cast(str, payload["portfolio_metrics_hash"]),
        qualification_passed=cast(bool, payload["qualification_passed"]),
        admission_passed=cast(bool, payload["admission_passed"]),
        nonempty_execution=cast(bool, payload["nonempty_execution"]),
    )


def _prospective_cohort_from_dict(
    payload: dict[str, object],
) -> ProspectiveValidationCohort:
    raw_cases = cast(list[dict[str, object]], payload["eligible_cases"])
    return ProspectiveValidationCohort(
        cohort_id=cast(str, payload["cohort_id"]),
        cohort_seal_hash=cast(str, payload["cohort_seal_hash"]),
        strategy_epoch_id=cast(str, payload["strategy_epoch_id"]),
        qualification_window_id=cast(str, payload["qualification_window_id"]),
        qualification_policy_hash=cast(str, payload["qualification_policy_hash"]),
        qualification_window_open_at=datetime.fromisoformat(
            cast(str, payload["qualification_window_open_at"])
        ),
        cohort_cutoff_at=datetime.fromisoformat(cast(str, payload["cohort_cutoff_at"])),
        sealed_at=datetime.fromisoformat(cast(str, payload["sealed_at"])),
        append_only_journal_hash=cast(str, payload["append_only_journal_hash"]),
        qualification_digest_hash=cast(str, payload["qualification_digest_hash"]),
        eligible_cases=tuple(
            ProspectiveCohortCase(
                case_id=cast(str, item["case_id"]),
                root_event_id=cast(str, item["root_event_id"]),
            )
            for item in raw_cases
        ),
    )


def _stable_identifier(value: str, name: str) -> None:
    if not value or value != value.strip():
        raise ValueError(f"{name} must be a non-empty stable identifier")


def _sha256(value: str, name: str) -> None:
    if len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
        raise ValueError(f"{name} must be a lowercase SHA-256")


def _require_sorted_unique(values: tuple[str, ...], name: str) -> None:
    if not values or values != tuple(sorted(set(values))):
        raise ValueError(f"{name} must be non-empty, unique, and sorted")


def _require_sorted_unique_or_empty(values: tuple[str, ...], name: str) -> None:
    if values != tuple(sorted(set(values))):
        raise ValueError(f"{name} must be unique and sorted")


# V2 is the promotion-capable path. The v1 objects above remain replayable only.


@dataclass(frozen=True, slots=True)
class StrategyMeasurementArtifact:
    """Harness-derived projection of one authoritative backtest outcome receipt."""

    case_id: str
    arm: str
    outcome_receipt_id: str
    outcome_receipt_hash: str
    net_return: Decimal | None
    absolute_pnl: Decimal | None
    portfolio_net_return: Decimal | None
    max_drawdown: Decimal | None
    cvar95: Decimal | None
    sharpe: Decimal | None
    sortino: Decimal | None
    stressed_net_return: Decimal | None
    turnover: Decimal | None
    adverse_excursion: Decimal | None
    liquidity_cost: Decimal | None
    avoided_loss: Decimal = Decimal(0)
    false_avoidance_opportunity_cost: Decimal = Decimal(0)
    nonempty_execution: bool = False
    missing_reasons: tuple[str, ...] = ()
    schema_version: str = "market-impact.strategy-measurement.v2"

    def __post_init__(self) -> None:
        _stable_identifier(self.case_id, "strategy measurement case_id")
        if self.arm not in {"candidate", "primary_baseline"}:
            raise ValueError("strategy measurement arm is invalid")
        if self.outcome_receipt_id != f"strategy-backtest-outcome-{self.outcome_receipt_hash}":
            raise ValueError("strategy measurement outcome receipt identity is invalid")
        _sha256(self.outcome_receipt_hash, "strategy measurement outcome_receipt_hash")
        for name in (
            "net_return",
            "absolute_pnl",
            "portfolio_net_return",
            "max_drawdown",
            "cvar95",
            "sharpe",
            "sortino",
            "stressed_net_return",
            "turnover",
            "adverse_excursion",
            "liquidity_cost",
            "avoided_loss",
            "false_avoidance_opportunity_cost",
        ):
            value = getattr(self, name)
            if value is not None and not value.is_finite():
                raise ValueError(f"strategy measurement {name} must be finite")
        for name in (
            "absolute_pnl",
            "max_drawdown",
            "cvar95",
            "turnover",
            "adverse_excursion",
            "liquidity_cost",
            "avoided_loss",
            "false_avoidance_opportunity_cost",
        ):
            value = getattr(self, name)
            if value is not None and value < 0:
                raise ValueError(f"strategy measurement {name} must be non-negative")
        if self.missing_reasons != tuple(sorted(set(self.missing_reasons))):
            raise ValueError("strategy measurement missing_reasons must be unique and sorted")

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "case_id": self.case_id,
            "arm": self.arm,
            "outcome_receipt_id": self.outcome_receipt_id,
            "outcome_receipt_hash": self.outcome_receipt_hash,
            **{
                name: (
                    None
                    if getattr(self, name) is None
                    else _decimal(cast(Decimal, getattr(self, name)))
                )
                for name in (
                    "net_return",
                    "absolute_pnl",
                    "portfolio_net_return",
                    "max_drawdown",
                    "cvar95",
                    "sharpe",
                    "sortino",
                    "stressed_net_return",
                    "turnover",
                    "adverse_excursion",
                    "liquidity_cost",
                    "avoided_loss",
                    "false_avoidance_opportunity_cost",
                )
            },
            "nonempty_execution": self.nonempty_execution,
            "missing_reasons": list(self.missing_reasons),
        }


@dataclass(frozen=True, slots=True)
class StrategyCaseRunPlan:
    plan_id: str
    run_id: str
    harness_authority_id: str
    registration_id: str
    registration_hash: str
    strategy_epoch_id: str
    case_id: str
    root_event_id: str
    regime: str
    role: StrategyCaseRole
    evidence_lane: StrategyEvidenceLane
    input_hash: str
    data_snapshot_hash: str
    evidence_lineage_hash: str
    qualification_report_hash: str
    admission_hash: str
    evidence_owner_id: str | None
    evidence_owner_hash: str
    evidence_unavailable_reason: str | None
    model_profile_hash: str
    prompt_hash: str
    skill_catalog_hash: str
    tool_manifest_hash: str
    universe_hash: str
    cost_model_hash: str
    fill_model_hash: str
    primary_baseline_id: str
    primary_baseline_definition_hash: str
    primary_baseline_configuration_hash: str
    development_selection_evidence_hash: str
    schema_version: str = STRATEGY_CASE_RUN_PLAN_SCHEMA

    def __post_init__(self) -> None:
        if not self.harness_authority_id.startswith("harness-authority-"):
            raise ValueError("strategy run plan requires a concrete Harness authority")
        for name in ("case_id", "root_event_id", "regime", "strategy_epoch_id"):
            _stable_identifier(getattr(self, name), name)
        _stable_identifier(self.run_id, "strategy run plan run_id")
        for name in (
            "registration_hash",
            "input_hash",
            "data_snapshot_hash",
            "evidence_lineage_hash",
            "qualification_report_hash",
            "admission_hash",
            "evidence_owner_hash",
            "model_profile_hash",
            "prompt_hash",
            "skill_catalog_hash",
            "tool_manifest_hash",
            "universe_hash",
            "cost_model_hash",
            "fill_model_hash",
            "primary_baseline_definition_hash",
            "primary_baseline_configuration_hash",
            "development_selection_evidence_hash",
        ):
            _sha256(getattr(self, name), name)
        if self.evidence_owner_id is not None:
            _stable_identifier(self.evidence_owner_id, "strategy plan evidence_owner_id")
        if self.evidence_unavailable_reason is not None:
            _stable_identifier(
                self.evidence_unavailable_reason,
                "strategy plan evidence_unavailable_reason",
            )
        if self.evidence_lane is StrategyEvidenceLane.STRICT_PIT:
            raise ValueError(
                "Strict-PIT plan construction is unavailable without a reopenable "
                "qualification owner"
            )
        if self.registration_id != f"strategy-validation-registration-{self.registration_hash}":
            raise ValueError("strategy run plan registration identity is invalid")
        if self.plan_id != self.expected_plan_id:
            raise ValueError("strategy run plan identity does not match content")

    @property
    def expected_plan_id(self) -> str:
        return f"strategy-case-run-plan-{canonical_hash(self.core_dict())}"

    @property
    def plan_hash(self) -> str:
        return canonical_hash(self.to_dict())

    def core_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "run_id": self.run_id,
            "harness_authority_id": self.harness_authority_id,
            "registration_id": self.registration_id,
            "registration_hash": self.registration_hash,
            "strategy_epoch_id": self.strategy_epoch_id,
            "case_id": self.case_id,
            "root_event_id": self.root_event_id,
            "regime": self.regime,
            "role": self.role.value,
            "evidence_lane": self.evidence_lane.value,
            "input_hash": self.input_hash,
            "data_snapshot_hash": self.data_snapshot_hash,
            "evidence_lineage_hash": self.evidence_lineage_hash,
            "qualification_report_hash": self.qualification_report_hash,
            "admission_hash": self.admission_hash,
            "evidence_owner_id": self.evidence_owner_id,
            "evidence_owner_hash": self.evidence_owner_hash,
            "evidence_unavailable_reason": self.evidence_unavailable_reason,
            "model_profile_hash": self.model_profile_hash,
            "prompt_hash": self.prompt_hash,
            "skill_catalog_hash": self.skill_catalog_hash,
            "tool_manifest_hash": self.tool_manifest_hash,
            "universe_hash": self.universe_hash,
            "cost_model_hash": self.cost_model_hash,
            "fill_model_hash": self.fill_model_hash,
            "primary_baseline_id": self.primary_baseline_id,
            "primary_baseline_definition_hash": self.primary_baseline_definition_hash,
            "primary_baseline_configuration_hash": self.primary_baseline_configuration_hash,
            "development_selection_evidence_hash": self.development_selection_evidence_hash,
        }

    def to_dict(self) -> dict[str, object]:
        return {**self.core_dict(), "plan_id": self.plan_id}

    @classmethod
    def build(
        cls,
        *,
        store: LocalDataSnapshotStore,
        registration: StrategyValidationRegistration,
        run_id: str,
        case_id: str,
    ) -> StrategyCaseRunPlan:
        authority = StrategyValidationAuthorityStore(store)
        reopened = authority.reopen_registration(registration.registration_id)
        if reopened != registration:
            raise ValueError("strategy run plan Registration differs from durable authority")
        return authority.build_case_run_plan(
            registration_id=registration.registration_id,
            run_id=run_id,
            case_id=case_id,
        )


@dataclass(frozen=True, slots=True)
class StrategyCaseTerminal:
    terminal_id: str
    harness_authority_id: str
    plan_id: str
    plan_hash: str
    run_id: str
    run_status: RunStatus
    started_at: datetime
    finished_at: datetime
    judgment_artifact_hash: str | None
    run_manifest_hash: str
    candidate_measurement_artifact_hash: str | None
    candidate_measurement_artifact_path: str | None
    baseline_measurement_artifact_hash: str | None
    baseline_measurement_artifact_path: str | None
    schema_version: str = STRATEGY_CASE_TERMINAL_SCHEMA

    def __post_init__(self) -> None:
        require_aware(self.started_at, "strategy terminal started_at")
        require_aware(self.finished_at, "strategy terminal finished_at")
        if not self.run_status.terminal:
            raise ValueError("strategy terminal requires a terminal disposition")
        _sha256(self.plan_hash, "strategy terminal plan_hash")
        _sha256(self.run_manifest_hash, "strategy terminal run_manifest_hash")
        for name in (
            "judgment_artifact_hash",
            "candidate_measurement_artifact_hash",
            "baseline_measurement_artifact_hash",
        ):
            value = getattr(self, name)
            if value is not None:
                _sha256(value, name)
        if self.run_status is RunStatus.COMPLETED and self.judgment_artifact_hash is None:
            raise ValueError("completed strategy terminal requires a Judgment artifact")
        if self.terminal_id != self.expected_terminal_id:
            raise ValueError("strategy terminal identity does not match content")

    @property
    def expected_terminal_id(self) -> str:
        return f"strategy-case-terminal-{canonical_hash(self.core_dict())}"

    def core_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "harness_authority_id": self.harness_authority_id,
            "plan_id": self.plan_id,
            "plan_hash": self.plan_hash,
            "run_id": self.run_id,
            "run_status": self.run_status.value,
            "started_at": self.started_at.isoformat(),
            "finished_at": self.finished_at.isoformat(),
            "judgment_artifact_hash": self.judgment_artifact_hash,
            "run_manifest_hash": self.run_manifest_hash,
            "candidate_measurement_artifact_hash": self.candidate_measurement_artifact_hash,
            "candidate_measurement_artifact_path": self.candidate_measurement_artifact_path,
            "baseline_measurement_artifact_hash": self.baseline_measurement_artifact_hash,
            "baseline_measurement_artifact_path": self.baseline_measurement_artifact_path,
        }

    def to_dict(self) -> dict[str, object]:
        return {**self.core_dict(), "terminal_id": self.terminal_id}


@dataclass(frozen=True, slots=True)
class StrategyRunSetSeal:
    seal_id: str
    harness_authority_id: str
    registration_id: str
    sealed_at: datetime
    terminal_ids: tuple[str, ...]
    selected_terminal_ids: tuple[str, ...]
    run_set_hash: str
    schema_version: str = STRATEGY_RUN_SET_SEAL_SCHEMA

    @property
    def seal_hash(self) -> str:
        return canonical_hash(self.to_dict())

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "seal_id": self.seal_id,
            "harness_authority_id": self.harness_authority_id,
            "registration_id": self.registration_id,
            "sealed_at": self.sealed_at.isoformat(),
            "terminal_ids": list(self.terminal_ids),
            "selected_terminal_ids": list(self.selected_terminal_ids),
            "run_set_hash": self.run_set_hash,
        }


@dataclass(frozen=True, slots=True)
class StrategyValidationReportV2:
    report_id: str
    harness_authority_id: str
    registration_id: str
    registration_hash: str
    run_set_seal_hash: str
    prospective_window_seal_hash: str | None
    disposition: StrategyValidationDisposition
    evidence_lane: StrategyEvidenceLane
    independent_case_count: int
    nonempty_execution_count: int
    outcomes_hash: str
    portfolio_metrics_hash: str | None
    reasons: tuple[str, ...]
    execution_capability: str = "none"
    schema_version: str = STRATEGY_VALIDATION_REPORT_SCHEMA_V2

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "report_id": self.report_id,
            "harness_authority_id": self.harness_authority_id,
            "registration_id": self.registration_id,
            "registration_hash": self.registration_hash,
            "run_set_seal_hash": self.run_set_seal_hash,
            "prospective_window_seal_hash": self.prospective_window_seal_hash,
            "disposition": self.disposition.value,
            "evidence_lane": self.evidence_lane.value,
            "independent_case_count": self.independent_case_count,
            "nonempty_execution_count": self.nonempty_execution_count,
            "outcomes_hash": self.outcomes_hash,
            "portfolio_metrics_hash": self.portfolio_metrics_hash,
            "reasons": list(self.reasons),
            "execution_capability": self.execution_capability,
        }


def _assert_authoritative_artifacts(journal: RunJournal, artifact_store: ArtifactStore) -> None:
    if not journal.promotion_eligible:
        raise ValueError("legacy path Run Journal is replay-only and promotion-ineligible")
    if artifact_store.root != (journal.path.parent / "artifacts").resolve():
        raise ValueError("strategy artifacts and Run Journal must share one Harness root")


def bind_strategy_case_run_plan(
    *, journal: RunJournal, artifact_store: ArtifactStore, run_id: str, plan: StrategyCaseRunPlan
) -> str:
    """Persist an immutable plan before the corresponding Agent run starts."""

    _assert_authoritative_artifacts(journal, artifact_store)
    authority_store = LocalDataSnapshotStore(journal.path.parent)
    StrategyValidationAuthorityStore(authority_store).assert_case_run_plan_authoritative(plan)
    if plan.harness_authority_id != journal.harness_authority_id:
        raise ValueError("strategy run plan belongs to a different Harness authority")
    if plan.run_id != run_id:
        raise ValueError("strategy run plan belongs to a different run_id")
    artifact = artifact_store.put_json(plan.to_dict())
    if artifact.content_hash != plan.plan_hash:
        raise AssertionError("strategy run plan CAS hash diverged")
    _initialize_journal_v2_tables(journal)
    with journal.authority_transaction() as connection:
        if (
            connection.execute(
                "SELECT 1 FROM strategy_run_set_seals_v2 WHERE registration_id = ?",
                (plan.registration_id,),
            ).fetchone()
            is not None
        ):
            raise ValueError("sealed strategy run set is append-closed")
        try:
            connection.execute(
                """
                INSERT INTO strategy_case_run_plans_v2(
                    run_id, plan_id, plan_hash, registration_id, case_id, artifact_hash
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    plan.plan_id,
                    plan.plan_hash,
                    plan.registration_id,
                    plan.case_id,
                    artifact.content_hash,
                ),
            )
        except sqlite3.IntegrityError as exc:
            row = connection.execute(
                "SELECT plan_hash FROM strategy_case_run_plans_v2 WHERE run_id = ?", (run_id,)
            ).fetchone()
            if row is None or cast(str, row["plan_hash"]) != plan.plan_hash:
                raise ValueError("strategy run_id already has a different frozen plan") from exc
    return artifact.content_hash


def start_strategy_case_run(
    *,
    journal: RunJournal,
    artifact_store: ArtifactStore,
    run_id: str,
    plan: StrategyCaseRunPlan,
    config_hash: str,
    created_at: datetime,
) -> RunRecord:
    """Atomically bind the pre-run plan and create its authoritative Run row."""

    _assert_authoritative_artifacts(journal, artifact_store)
    authority_store = LocalDataSnapshotStore(journal.path.parent)
    StrategyValidationAuthorityStore(authority_store).assert_case_run_plan_authoritative(plan)
    _sha256(config_hash, "strategy run config_hash")
    require_aware(created_at, "strategy run created_at")
    if plan.run_id != run_id or plan.harness_authority_id != journal.harness_authority_id:
        raise ValueError("strategy run plan differs from the authoritative run")
    artifact = artifact_store.put_json(plan.to_dict())
    if artifact.content_hash != plan.plan_hash:
        raise AssertionError("strategy run plan CAS hash diverged")
    _initialize_journal_v2_tables(journal)
    timestamp = created_at.astimezone(UTC).isoformat().replace("+00:00", "Z")
    with journal.authority_transaction() as connection:
        if (
            connection.execute(
                "SELECT 1 FROM strategy_run_set_seals_v2 WHERE registration_id = ?",
                (plan.registration_id,),
            ).fetchone()
            is not None
        ):
            raise ValueError("sealed strategy run set is append-closed")
        existing_run = connection.execute(
            "SELECT * FROM runs WHERE run_id = ?", (run_id,)
        ).fetchone()
        if existing_run is None:
            connection.execute(
                """
                INSERT INTO strategy_case_run_plans_v2(
                    run_id, plan_id, plan_hash, registration_id, case_id, artifact_hash
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    plan.plan_id,
                    plan.plan_hash,
                    plan.registration_id,
                    plan.case_id,
                    artifact.content_hash,
                ),
            )
            connection.execute(
                """
                INSERT INTO runs(
                    run_id, status, config_hash, created_at, updated_at,
                    terminal_artifact_id, harness_authority_id,
                    strategy_plan_artifact_hash
                ) VALUES (?, ?, ?, ?, ?, NULL, ?, ?)
                """,
                (
                    run_id,
                    RunStatus.RUNNING.value,
                    config_hash,
                    timestamp,
                    timestamp,
                    journal.harness_authority_id,
                    plan.plan_hash,
                ),
            )
        else:
            existing_plan = connection.execute(
                "SELECT plan_hash FROM strategy_case_run_plans_v2 WHERE run_id = ?",
                (run_id,),
            ).fetchone()
            if (
                existing_run["config_hash"] != config_hash
                or existing_run["harness_authority_id"] != journal.harness_authority_id
                or existing_run["strategy_plan_artifact_hash"] != plan.plan_hash
                or existing_plan is None
                or existing_plan["plan_hash"] != plan.plan_hash
            ):
                raise ValueError("existing strategy run has a different frozen binding")
    return journal.get_run(run_id)


class StrategyCaseMeasurementWriter:
    """Project authoritative receipt IDs into non-caller-authored measurements."""

    def __init__(self, store: LocalDataSnapshotStore) -> None:
        if type(store) is not LocalDataSnapshotStore:
            raise TypeError("strategy measurements require the concrete Harness store")
        self.store = store
        _initialize_journal_v2_tables(RunJournal.authoritative(store))

    def record(
        self,
        *,
        run_id: str,
        candidate_receipt_id: str,
        baseline_receipt_id: str,
        measured_at: datetime,
    ) -> None:
        require_aware(measured_at, "strategy measurement measured_at")
        receipts = (
            reopen_strategy_backtest_outcome(self.store, candidate_receipt_id),
            reopen_strategy_backtest_outcome(self.store, baseline_receipt_id),
        )
        with self.store.authority_transaction() as connection:
            plan_row = connection.execute(
                "SELECT artifact_hash FROM strategy_case_run_plans_v2 WHERE run_id = ?",
                (run_id,),
            ).fetchone()
        if plan_row is None:
            raise KeyError("strategy measurement run has no frozen pre-run plan")
        plan = _strategy_run_plan_from_dict(
            cast(
                dict[str, object],
                self.store.artifacts.read_json(cast(str, plan_row["artifact_hash"])),
            )
        )
        authority = StrategyValidationAuthorityStore(self.store)
        authority.assert_case_run_plan_authoritative(plan)
        registration = authority.reopen_registration(plan.registration_id)
        with self.store.authority_transaction() as connection:
            row = connection.execute(
                """
                SELECT plan.case_id, plan.registration_id, plan.artifact_hash,
                       run.status, run.harness_authority_id
                FROM strategy_case_run_plans_v2 AS plan
                JOIN runs AS run USING (run_id)
                WHERE run_id = ?
                """,
                (run_id,),
            ).fetchone()
            if row is None:
                raise KeyError("strategy measurement run has no frozen pre-run plan")
            if (
                row["status"] != RunStatus.RUNNING.value
                or row["harness_authority_id"] != self.store.harness_authority_id
            ):
                raise ValueError("strategy measurements require the running authoritative run")
            if row["artifact_hash"] != plan_row["artifact_hash"]:
                raise ValueError("strategy measurement plan changed during receipt verification")
            if (
                connection.execute(
                    "SELECT 1 FROM strategy_run_set_seals_v2 WHERE registration_id = ?",
                    (row["registration_id"],),
                ).fetchone()
                is not None
            ):
                raise ValueError("sealed strategy run set is measurement-closed")
            measurements: list[StrategyMeasurementArtifact] = []
            for expected_arm, (receipt, result) in zip(
                (StrategyBacktestArm.CANDIDATE, StrategyBacktestArm.PRIMARY_BASELINE),
                receipts,
                strict=True,
            ):
                _verify_receipt_for_plan(
                    receipt=receipt,
                    result=result,
                    expected_arm=expected_arm,
                    plan=plan,
                    registration=registration,
                )
                measurements.append(_measurement_from_receipt(receipt))
            artifacts = tuple(
                self.store.artifacts.put_json(item.to_dict()) for item in measurements
            )
            for measurement, artifact in zip(measurements, artifacts, strict=True):
                connection.execute(
                    """
                    INSERT OR IGNORE INTO strategy_case_measurements_v2(
                        run_id, arm, case_id, measured_at, artifact_hash
                    ) VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        run_id,
                        measurement.arm,
                        measurement.case_id,
                        measured_at.isoformat(),
                        artifact.content_hash,
                    ),
                )
                stored = connection.execute(
                    "SELECT artifact_hash FROM strategy_case_measurements_v2 "
                    "WHERE run_id = ? AND arm = ?",
                    (run_id, measurement.arm),
                ).fetchone()
                if stored is None or stored["artifact_hash"] != artifact.content_hash:
                    raise ValueError("strategy measurement conflicts with actual run history")


def write_strategy_case_terminal(
    *,
    journal: RunJournal,
    artifact_store: ArtifactStore,
    run_id: str,
    status: RunStatus,
    finished_at: datetime,
    run_terminal_artifact_hash: str,
    judgment_artifact_hash: str | None,
) -> StrategyCaseTerminal | None:
    """Actual Agent completion writer; caller values never enter evaluation."""

    if journal.get_run(run_id).strategy_plan_artifact_hash is None:
        return None
    _sha256(run_terminal_artifact_hash, "run terminal artifact hash")
    if status is RunStatus.COMPLETED and judgment_artifact_hash != run_terminal_artifact_hash:
        raise ValueError("completed strategy terminal must bind the actual Judgment artifact")
    _assert_authoritative_artifacts(journal, artifact_store)
    from market_impact_agent.agent_engine import reopen_authoritative_agent_terminal

    judgment = reopen_authoritative_agent_terminal(
        journal=journal,
        artifact_store=artifact_store,
        run_id=run_id,
        status=status,
        finished_at=finished_at,
        terminal_artifact_hash=run_terminal_artifact_hash,
    )
    _initialize_journal_v2_tables(journal)
    with journal.authority_transaction() as connection:
        row = connection.execute(
            """
            SELECT plan.artifact_hash, run.created_at, run.harness_authority_id,
                   run.strategy_plan_artifact_hash
            FROM strategy_case_run_plans_v2 AS plan
            JOIN runs AS run USING (run_id)
            WHERE run_id = ?
            """,
            (run_id,),
        ).fetchone()
        if row is None:
            return None
        plan = _strategy_run_plan_from_dict(
            cast(dict[str, object], artifact_store.read_json(cast(str, row["artifact_hash"])))
        )
        if (
            row["harness_authority_id"] != journal.harness_authority_id
            or row["strategy_plan_artifact_hash"] != plan.plan_hash
        ):
            raise ValueError("Run row differs from its frozen strategy plan")
        if judgment is not None and (
            judgment.runtime_config_hash != plan.model_profile_hash
            or judgment.prompt_hash != plan.prompt_hash
            or canonical_hash(list(judgment.skill_hashes)) != plan.skill_catalog_hash
            or canonical_hash(list(judgment.tool_manifest_hashes)) != plan.tool_manifest_hash
        ):
            raise ValueError("Judgment Artifact differs from the frozen strategy run plan")
        measurement_rows = {
            cast(str, item["arm"]): cast(str, item["artifact_hash"])
            for item in connection.execute(
                "SELECT arm, artifact_hash FROM strategy_case_measurements_v2 WHERE run_id = ?",
                (run_id,),
            ).fetchall()
        }
        measurement_values: list[tuple[str | None, str | None]] = []
        for arm in ("candidate", "primary_baseline"):
            measurement_hash = measurement_rows.get(arm)
            if measurement_hash is None:
                measurement_values.append((None, None))
            else:
                stored = artifact_store.get(measurement_hash, media_type="application/json")
                measurement_values.append((stored.content_hash, str(stored.path)))
        started_at = datetime.fromisoformat(cast(str, row["created_at"]))
        run_manifest = {
            "schema_version": "market-impact.strategy-run-manifest.v2",
            "harness_authority_id": journal.harness_authority_id,
            "run_id": run_id,
            "plan_id": plan.plan_id,
            "plan_hash": plan.plan_hash,
            "status": status.value,
            "started_at": started_at.isoformat(),
            "finished_at": finished_at.isoformat(),
            "judgment_artifact_hash": judgment_artifact_hash,
            "journal_hash_before_terminal": journal.journal_hash(run_id),
        }
        manifest_artifact = artifact_store.put_json(run_manifest)
        candidate, baseline = measurement_values
        values: dict[str, object] = {
            "schema_version": STRATEGY_CASE_TERMINAL_SCHEMA,
            "harness_authority_id": journal.harness_authority_id,
            "plan_id": plan.plan_id,
            "plan_hash": plan.plan_hash,
            "run_id": run_id,
            "run_status": status.value,
            "started_at": started_at.isoformat(),
            "finished_at": finished_at.isoformat(),
            "judgment_artifact_hash": judgment_artifact_hash,
            "run_manifest_hash": manifest_artifact.content_hash,
            "candidate_measurement_artifact_hash": candidate[0],
            "candidate_measurement_artifact_path": candidate[1],
            "baseline_measurement_artifact_hash": baseline[0],
            "baseline_measurement_artifact_path": baseline[1],
        }
        terminal = StrategyCaseTerminal(
            terminal_id=f"strategy-case-terminal-{canonical_hash(values)}",
            harness_authority_id=cast(str, journal.harness_authority_id),
            plan_id=plan.plan_id,
            plan_hash=plan.plan_hash,
            run_id=run_id,
            run_status=status,
            started_at=started_at,
            finished_at=finished_at,
            judgment_artifact_hash=judgment_artifact_hash,
            run_manifest_hash=manifest_artifact.content_hash,
            candidate_measurement_artifact_hash=candidate[0],
            candidate_measurement_artifact_path=candidate[1],
            baseline_measurement_artifact_hash=baseline[0],
            baseline_measurement_artifact_path=baseline[1],
        )
        terminal_artifact = artifact_store.put_json(terminal.to_dict())
        connection.execute(
            """
            INSERT OR IGNORE INTO strategy_case_terminals_v2(
                terminal_id, run_id, plan_id, registration_id, case_id,
                started_at, finished_at, run_status, run_manifest_hash, artifact_hash
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                terminal.terminal_id,
                run_id,
                plan.plan_id,
                plan.registration_id,
                plan.case_id,
                terminal.started_at.isoformat(),
                terminal.finished_at.isoformat(),
                status.value,
                terminal.run_manifest_hash,
                terminal_artifact.content_hash,
            ),
        )
        stored = connection.execute(
            "SELECT terminal_id, artifact_hash FROM strategy_case_terminals_v2 WHERE run_id = ?",
            (run_id,),
        ).fetchone()
        if stored is None or (
            stored["terminal_id"] != terminal.terminal_id
            or stored["artifact_hash"] != terminal_artifact.content_hash
        ):
            raise ValueError("strategy run already has a different terminal binding")
        current = connection.execute(
            "SELECT status, terminal_artifact_id FROM runs WHERE run_id = ?", (run_id,)
        ).fetchone()
        if current is None:
            raise KeyError(f"unknown run_id: {run_id}")
        if current["status"] == RunStatus.RUNNING.value:
            connection.execute(
                """
                UPDATE runs SET status = ?, updated_at = ?, terminal_artifact_id = ?
                WHERE run_id = ?
                """,
                (
                    status.value,
                    finished_at.isoformat().replace("+00:00", "Z"),
                    run_terminal_artifact_hash,
                    run_id,
                ),
            )
        elif (
            current["status"] != status.value
            or current["terminal_artifact_id"] != run_terminal_artifact_hash
        ):
            raise ValueError("strategy Run row is already terminal with different content")
    return terminal


class StrategyValidationAuthorityStore:
    """Only promotion-capable strategy authority, rooted in LocalDataSnapshotStore."""

    def __init__(self, store: LocalDataSnapshotStore) -> None:
        if type(store) is not LocalDataSnapshotStore:
            raise TypeError("strategy validation requires the concrete Harness store")
        self.store = store
        self.harness_authority_id = store.harness_authority_id
        RunJournal.authoritative(store)
        with self.store.authority_transaction() as connection:
            _initialize_v2_tables(connection)

    def register(self, registration: StrategyValidationRegistration) -> None:
        artifact = self.store.artifacts.put_json(registration.to_dict())
        window_seal_hash = self._validated_prospective_window(
            registration, expected_artifact_hash=None
        )
        with self.store.authority_transaction() as connection:
            connection.execute(
                """
                INSERT OR IGNORE INTO strategy_validation_registrations_v2(
                    registration_id, registration_hash, strategy_epoch_id, artifact_hash,
                    harness_authority_id, prospective_window_seal_hash
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    registration.registration_id,
                    registration.registration_hash,
                    registration.strategy_epoch_id,
                    artifact.content_hash,
                    self.harness_authority_id,
                    window_seal_hash,
                ),
            )
            row = connection.execute(
                "SELECT artifact_hash, harness_authority_id, prospective_window_seal_hash "
                "FROM strategy_validation_registrations_v2 "
                "WHERE registration_id = ?",
                (registration.registration_id,),
            ).fetchone()
            if row is None or (
                row["artifact_hash"] != artifact.content_hash
                or row["harness_authority_id"] != self.harness_authority_id
                or row["prospective_window_seal_hash"] != window_seal_hash
            ):
                raise ValueError("strategy registration identity conflicts with stored content")

    def reopen_registration(self, registration_id: str) -> StrategyValidationRegistration:
        return self._registration(registration_id)

    def build_case_run_plan(
        self, *, registration_id: str, run_id: str, case_id: str
    ) -> StrategyCaseRunPlan:
        """Derive all plan evidence from frozen owners in this Harness root."""

        registration = self._registration(registration_id)
        definition = next(
            (item for item in registration.evaluation_cases if item.case_id == case_id), None
        )
        if definition is None:
            raise ValueError("strategy run plan case is not in the evaluation registration")
        evidence = self._derive_plan_evidence(registration, definition)
        primary = registration.primary_baseline
        values: dict[str, object] = {
            "schema_version": STRATEGY_CASE_RUN_PLAN_SCHEMA,
            "run_id": run_id,
            "harness_authority_id": self.harness_authority_id,
            "registration_id": registration.registration_id,
            "registration_hash": registration.registration_hash,
            "strategy_epoch_id": registration.strategy_epoch_id,
            "case_id": definition.case_id,
            "root_event_id": definition.root_event_id,
            "regime": definition.regime,
            "role": definition.role.value,
            **evidence,
            "model_profile_hash": registration.model_profile_hash,
            "prompt_hash": registration.prompt_hash,
            "skill_catalog_hash": registration.skill_catalog_hash,
            "tool_manifest_hash": registration.tool_manifest_hash,
            "universe_hash": registration.universe_hash,
            "cost_model_hash": registration.cost_model_hash,
            "fill_model_hash": registration.fill_model_hash,
            "primary_baseline_id": registration.primary_baseline_id,
            "primary_baseline_definition_hash": primary.definition_hash,
            "primary_baseline_configuration_hash": primary.configuration_hash,
            "development_selection_evidence_hash": (
                registration.development_selection_evidence_hash
            ),
        }
        return StrategyCaseRunPlan(
            plan_id=f"strategy-case-run-plan-{canonical_hash(values)}",
            run_id=run_id,
            harness_authority_id=self.harness_authority_id,
            registration_id=registration.registration_id,
            registration_hash=registration.registration_hash,
            strategy_epoch_id=registration.strategy_epoch_id,
            case_id=definition.case_id,
            root_event_id=definition.root_event_id,
            regime=definition.regime,
            role=definition.role,
            evidence_lane=StrategyEvidenceLane(cast(str, evidence["evidence_lane"])),
            input_hash=cast(str, evidence["input_hash"]),
            data_snapshot_hash=cast(str, evidence["data_snapshot_hash"]),
            evidence_lineage_hash=cast(str, evidence["evidence_lineage_hash"]),
            qualification_report_hash=cast(str, evidence["qualification_report_hash"]),
            admission_hash=cast(str, evidence["admission_hash"]),
            evidence_owner_id=cast(str | None, evidence["evidence_owner_id"]),
            evidence_owner_hash=cast(str, evidence["evidence_owner_hash"]),
            evidence_unavailable_reason=cast(str | None, evidence["evidence_unavailable_reason"]),
            model_profile_hash=registration.model_profile_hash,
            prompt_hash=registration.prompt_hash,
            skill_catalog_hash=registration.skill_catalog_hash,
            tool_manifest_hash=registration.tool_manifest_hash,
            universe_hash=registration.universe_hash,
            cost_model_hash=registration.cost_model_hash,
            fill_model_hash=registration.fill_model_hash,
            primary_baseline_id=registration.primary_baseline_id,
            primary_baseline_definition_hash=primary.definition_hash,
            primary_baseline_configuration_hash=primary.configuration_hash,
            development_selection_evidence_hash=(registration.development_selection_evidence_hash),
        )

    def assert_case_run_plan_authoritative(self, plan: StrategyCaseRunPlan) -> None:
        expected = self.build_case_run_plan(
            registration_id=plan.registration_id,
            run_id=plan.run_id,
            case_id=plan.case_id,
        )
        if plan != expected:
            raise ValueError("strategy run plan evidence differs from frozen Harness owners")

    def _derive_plan_evidence(
        self,
        registration: StrategyValidationRegistration,
        definition: StrategyCaseDefinition,
    ) -> dict[str, object]:
        registration_hash = self._registration_artifact_hash(registration.registration_id)
        source_hash: str | None = None
        lineage_hash: str | None = None
        qualification_hash: str | None = None
        if definition.source_snapshot_id is not None:
            source = self.store.get(definition.source_snapshot_id)
            source_hash = self.store.artifacts.put_json(source.to_dict()).content_hash
            lineage_hash = canonical_hash(sorted(item.lineage_id for item in source.observations))
            qualification_hash = canonical_hash(
                {
                    "snapshot_id": source.snapshot_id,
                    "query_id": source.query.query_id,
                    "coverage_complete": source.coverage_complete,
                }
            )

        if registration.program is StrategyValidationProgram.PROSPECTIVE_CONFIRMATION:
            return self._derive_prospective_plan_evidence(
                registration,
                definition,
                source_hash=source_hash,
                lineage_hash=lineage_hash,
                registration_hash=registration_hash,
            )
        binding = definition.evidence_binding_ref
        if binding is not None and binding.startswith("modeled-pit-readiness-checkpoint-"):
            return self._derive_modeled_plan_evidence(
                binding,
                source_hash=source_hash,
                lineage_hash=lineage_hash,
            )
        absence = {
            "registration_id": registration.registration_id,
            "case_id": definition.case_id,
            "reason": "strict_qualification_lineage_owner_unavailable",
        }
        return {
            "evidence_lane": StrategyEvidenceLane.RETROSPECTIVE.value,
            "input_hash": source_hash or registration_hash,
            "data_snapshot_hash": source_hash or canonical_hash({**absence, "kind": "snapshot"}),
            "evidence_lineage_hash": lineage_hash or canonical_hash({**absence, "kind": "lineage"}),
            "qualification_report_hash": qualification_hash
            or canonical_hash({**absence, "kind": "qualification"}),
            "admission_hash": canonical_hash({**absence, "kind": "admission"}),
            "evidence_owner_id": definition.source_snapshot_id,
            "evidence_owner_hash": source_hash or registration_hash,
            "evidence_unavailable_reason": "strict_qualification_lineage_owner_unavailable",
        }

    def _derive_prospective_plan_evidence(
        self,
        registration: StrategyValidationRegistration,
        definition: StrategyCaseDefinition,
        *,
        source_hash: str | None,
        lineage_hash: str | None,
        registration_hash: str,
    ) -> dict[str, object]:
        with self.store.authority_transaction() as connection:
            rows = connection.execute(
                """
                SELECT event.admission_id, event.admission_hash, event.event_hash,
                       event.root_event_id, event.regime,
                       seal.artifact_hash AS seal_artifact_hash,
                       admission.artifact_hash AS admission_artifact_hash
                FROM strategy_window_events_v2 AS event
                JOIN strategy_window_mappings_v2 AS mapping
                  ON mapping.window_id = event.window_id AND mapping.case_id = event.case_id
                JOIN strategy_window_seals_v2 AS seal
                  ON seal.window_id = event.window_id AND seal.stale = 0
                JOIN prospective_trigger_admissions AS admission
                  ON admission.admission_id = event.admission_id
                WHERE mapping.registration_id = ? AND event.case_id = ?
                """,
                (registration.registration_id, definition.case_id),
            ).fetchall()
        if len(rows) != 1:
            raise ValueError(
                "prospective strategy case requires exactly one sealed Trigger Admission"
            )
        row = rows[0]
        if (
            row["root_event_id"] != definition.root_event_id
            or row["regime"] != definition.regime
            or row["admission_hash"] != row["admission_artifact_hash"]
        ):
            raise ValueError("prospective strategy case differs from its sealed admission")
        from market_impact_agent.prospective_trigger_admission import (
            ProspectiveTriggerAdmissionStore,
        )

        admission = ProspectiveTriggerAdmissionStore(self.store).get(cast(str, row["admission_id"]))
        if admission.admission_id != row["admission_id"]:
            raise ValueError("prospective Trigger Admission could not be reopened")
        return {
            "evidence_lane": StrategyEvidenceLane.PROSPECTIVE.value,
            "input_hash": cast(str, row["admission_hash"]),
            "data_snapshot_hash": source_hash or registration_hash,
            "evidence_lineage_hash": lineage_hash or cast(str, row["event_hash"]),
            "qualification_report_hash": cast(str, row["seal_artifact_hash"]),
            "admission_hash": cast(str, row["admission_hash"]),
            "evidence_owner_id": cast(str, row["admission_id"]),
            "evidence_owner_hash": cast(str, row["admission_hash"]),
            "evidence_unavailable_reason": (
                None if source_hash is not None else "backtest_source_snapshot_not_registered"
            ),
        }

    def _derive_modeled_plan_evidence(
        self,
        checkpoint_id: str,
        *,
        source_hash: str | None,
        lineage_hash: str | None,
    ) -> dict[str, object]:
        try:
            with self.store.authority_transaction() as connection:
                row = connection.execute(
                    "SELECT * FROM modeled_pit_readiness_authority "
                    "WHERE checkpoint_id = ? AND harness_authority_id = ?",
                    (checkpoint_id, self.harness_authority_id),
                ).fetchone()
        except sqlite3.OperationalError as exc:
            raise ValueError(
                "modeled strategy case has no authoritative readiness checkpoint"
            ) from exc
        if row is None:
            raise ValueError("modeled strategy case has no authoritative readiness checkpoint")
        checkpoint = cast(
            dict[str, object],
            self.store.artifacts.read_json(cast(str, row["artifact_hash"])),
        )
        if (
            checkpoint.get("checkpoint_id") != checkpoint_id
            or checkpoint.get("trigger_admission_id") != row["admission_id"]
            or checkpoint.get("historical_pit_claim") is not False
        ):
            raise ValueError("modeled readiness checkpoint differs from its authority row")
        with self.store.authority_transaction() as connection:
            admission = connection.execute(
                "SELECT artifact_hash FROM prospective_trigger_admissions WHERE admission_id = ?",
                (row["admission_id"],),
            ).fetchone()
        if admission is None:
            raise ValueError("modeled readiness Trigger Admission cannot be reopened")
        return {
            "evidence_lane": StrategyEvidenceLane.MODELED_PIT.value,
            "input_hash": cast(str, row["artifact_hash"]),
            "data_snapshot_hash": source_hash or cast(str, row["snapshot_set_artifact_hash"]),
            "evidence_lineage_hash": lineage_hash or cast(str, row["checkpoint_hash"]),
            "qualification_report_hash": cast(str, row["registration_artifact_hash"]),
            "admission_hash": cast(str, admission["artifact_hash"]),
            "evidence_owner_id": checkpoint_id,
            "evidence_owner_hash": cast(str, row["artifact_hash"]),
            "evidence_unavailable_reason": None,
        }

    def _registration_artifact_hash(self, registration_id: str) -> str:
        with self.store.authority_transaction() as connection:
            row = connection.execute(
                "SELECT artifact_hash FROM strategy_validation_registrations_v2 "
                "WHERE registration_id = ?",
                (registration_id,),
            ).fetchone()
        if row is None:
            raise KeyError(f"unknown strategy validation registration: {registration_id}")
        return cast(str, row["artifact_hash"])

    def seal_run_set(self, registration_id: str, *, sealed_at: datetime) -> StrategyRunSetSeal:
        require_aware(sealed_at, "strategy run-set sealed_at")
        registration = self._registration(registration_id)
        with self.store.authority_transaction() as connection:
            existing = connection.execute(
                "SELECT artifact_hash FROM strategy_run_set_seals_v2 WHERE registration_id = ?",
                (registration_id,),
            ).fetchone()
            if existing is not None:
                return _strategy_run_set_seal_from_dict(
                    cast(
                        dict[str, object],
                        self.store.artifacts.read_json(cast(str, existing["artifact_hash"])),
                    )
                )
            unfinished = connection.execute(
                """
                SELECT 1 FROM strategy_case_run_plans_v2 AS plan
                LEFT JOIN runs AS run USING (run_id)
                LEFT JOIN strategy_case_terminals_v2 AS terminal USING (run_id, plan_id)
                WHERE plan.registration_id = ? AND (
                    run.run_id IS NULL OR run.status = ? OR terminal.terminal_id IS NULL
                    OR terminal.run_status != run.status
                ) LIMIT 1
                """,
                (registration_id, RunStatus.RUNNING.value),
            ).fetchone()
            if unfinished is not None:
                raise ValueError(
                    "cannot seal while a pre-bound strategy run is missing or unfinished"
                )
            rows = connection.execute(
                """
                SELECT terminal.*, plan.plan_hash, plan.artifact_hash AS plan_artifact_hash
                FROM strategy_case_terminals_v2 AS terminal
                JOIN strategy_case_run_plans_v2 AS plan USING (run_id, plan_id)
                JOIN runs AS run USING (run_id)
                WHERE terminal.registration_id = ? AND run.status = terminal.run_status
                  AND run.harness_authority_id = ?
                ORDER BY terminal.case_id, terminal.started_at,
                         terminal.run_manifest_hash, terminal.terminal_id
                """,
                (registration_id, self.harness_authority_id),
            ).fetchall()
            expected_case_ids = {item.case_id for item in registration.evaluation_cases}
            observed_case_ids = {cast(str, row["case_id"]) for row in rows}
            if observed_case_ids != expected_case_ids:
                raise ValueError(
                    "cannot seal before every registered evaluation case has a typed terminal"
                )
            for row in rows:
                terminal = _strategy_terminal_from_dict(
                    cast(
                        dict[str, object],
                        self.store.artifacts.read_json(cast(str, row["artifact_hash"])),
                    )
                )
                if (
                    terminal.terminal_id != row["terminal_id"]
                    or terminal.run_id != row["run_id"]
                    or terminal.plan_id != row["plan_id"]
                    or terminal.run_status.value != row["run_status"]
                    or terminal.run_manifest_hash != row["run_manifest_hash"]
                ):
                    raise ValueError("strategy terminal index differs from its typed artifact")
                plan = _strategy_run_plan_from_dict(
                    cast(
                        dict[str, object],
                        self.store.artifacts.read_json(cast(str, row["plan_artifact_hash"])),
                    )
                )
                definition = next(
                    item for item in registration.evaluation_cases if item.case_id == plan.case_id
                )
                _verify_plan_registration(plan, registration, definition)
                self._verify_terminal_owner(terminal, plan=plan)
            terminal_ids = tuple(cast(str, row["terminal_id"]) for row in rows)
            selected: list[str] = []
            for definition in registration.evaluation_cases:
                candidates = [row for row in rows if row["case_id"] == definition.case_id]
                if candidates:
                    selected.append(cast(str, candidates[0]["terminal_id"]))
            selected_ids = tuple(selected)
            run_set_hash = canonical_hash(list(terminal_ids))
            core: dict[str, object] = {
                "schema_version": STRATEGY_RUN_SET_SEAL_SCHEMA,
                "harness_authority_id": self.harness_authority_id,
                "registration_id": registration_id,
                "sealed_at": sealed_at.isoformat(),
                "terminal_ids": list(terminal_ids),
                "selected_terminal_ids": list(selected_ids),
                "run_set_hash": run_set_hash,
            }
            seal_id = f"strategy-run-set-seal-{canonical_hash(core)}"
            seal = StrategyRunSetSeal(
                seal_id=seal_id,
                harness_authority_id=self.harness_authority_id,
                registration_id=registration_id,
                sealed_at=sealed_at,
                terminal_ids=terminal_ids,
                selected_terminal_ids=selected_ids,
                run_set_hash=run_set_hash,
            )
            artifact = self.store.artifacts.put_json(seal.to_dict())
            connection.execute(
                "INSERT INTO strategy_run_set_seals_v2 VALUES (?, ?, ?, ?)",
                (registration_id, seal_id, artifact.content_hash, sealed_at.isoformat()),
            )
        return seal

    def evaluate(self, registration_id: str) -> StrategyValidationReportV2:
        registration = self._registration(registration_id)
        seal = self._seal(registration_id)
        reasons: set[str] = set()
        if seal.harness_authority_id != self.harness_authority_id:
            raise ValueError("strategy run-set seal belongs to a different Harness authority")
        terminals = {item.terminal_id: item for item in self._terminals(registration_id)}
        selected_by_case: dict[str, StrategyCaseTerminal] = {}
        for terminal_id in seal.selected_terminal_ids:
            terminal = terminals[terminal_id]
            case_id = self._plan(terminal.run_id).case_id
            if case_id in selected_by_case:
                raise ValueError("run-set seal selects multiple terminals for one case")
            selected_by_case[case_id] = terminal
        outcomes: list[StrategyCaseOutcome] = []
        candidate_receipts: list[tuple[StrategyCaseDefinition, StrategyBacktestOutcomeReceipt]] = []
        baseline_receipts: list[tuple[StrategyCaseDefinition, StrategyBacktestOutcomeReceipt]] = []
        economic_reasons: set[str] = set()
        nonempty = 0
        for definition in registration.evaluation_cases:
            terminal = selected_by_case.get(definition.case_id)
            if terminal is None:
                continue
            plan = self._plan(terminal.run_id)
            if plan.case_id != definition.case_id:
                raise ValueError("sealed terminal case differs from its frozen Registration")
            if plan.harness_authority_id != self.harness_authority_id:
                raise ValueError("sealed run plan belongs to a different Harness authority")
            _verify_plan_registration(plan, registration, definition)
            self._verify_terminal_owner(terminal)
            if plan.evidence_lane is not _expected_lane(registration.program):
                reasons.add(f"evidence_authority_lane_mismatch:{definition.case_id}")
            if plan.evidence_unavailable_reason is not None:
                reasons.add(
                    f"evidence_authority_unavailable:{definition.case_id}:"
                    f"{plan.evidence_unavailable_reason}"
                )
            if terminal.run_status is not RunStatus.COMPLETED:
                reasons.add(f"terminal_run_not_completed:{definition.case_id}")
                continue
            pair, measurement_reason = self._measurements(
                terminal,
                definition.case_id,
                plan=plan,
                registration=registration,
            )
            if pair is None:
                reasons.add(
                    f"{measurement_reason or 'missing_actual_measurement'}:{definition.case_id}"
                )
                continue
            candidate, baseline, candidate_receipt, baseline_receipt = pair
            required_metrics = (
                "net_return",
                "absolute_pnl",
                "portfolio_net_return",
                "max_drawdown",
                "cvar95",
                "sharpe",
                "sortino",
                "stressed_net_return",
                "turnover",
                "adverse_excursion",
                "liquidity_cost",
            )
            if (
                candidate.missing_reasons
                or baseline.missing_reasons
                or any(getattr(candidate, name) is None for name in required_metrics)
                or any(getattr(baseline, name) is None for name in required_metrics)
            ):
                reasons.add(f"missing_required_outcome_evidence:{definition.case_id}")
                continue
            assert candidate.net_return is not None
            assert candidate.absolute_pnl is not None
            assert baseline.net_return is not None
            candidate_receipts.append((definition, candidate_receipt))
            baseline_receipts.append((definition, baseline_receipt))
            nonempty += int(candidate.nonempty_execution)
            outcomes.append(
                StrategyCaseOutcome(
                    case_id=definition.case_id,
                    root_event_id=definition.root_event_id,
                    regime=definition.regime,
                    candidate_net_return=candidate.net_return,
                    primary_baseline_net_return=baseline.net_return,
                    candidate_absolute_pnl=candidate.absolute_pnl,
                )
            )
        missing = len(registration.evaluation_cases) - len(selected_by_case)
        if missing:
            reasons.add("incomplete_terminal_denominator")
        portfolio, portfolio_reason = _portfolio_from_receipts(
            registration,
            candidate_receipts,
            baseline_receipts,
            artifact_store=self.store.artifacts,
        )
        if portfolio is None:
            reasons.add(portfolio_reason or "missing_actual_portfolio_measurement")
        if len(outcomes) != len(registration.evaluation_cases):
            reasons.add("incomplete_measurement_denominator")
        complete_denominator = (
            len(selected_by_case) == len(registration.evaluation_cases)
            and len(outcomes) == len(registration.evaluation_cases)
            and len(candidate_receipts) == len(registration.evaluation_cases)
            and len(baseline_receipts) == len(registration.evaluation_cases)
        )
        minimum_cases, minimum_regimes, minimum_nonempty = _program_thresholds(registration.program)
        if len({item.regime for item in outcomes}) < minimum_regimes:
            reasons.add("insufficient_regime_coverage")
        if nonempty < minimum_nonempty:
            reasons.add("insufficient_nonempty_executions")
        if complete_denominator and len(outcomes) >= minimum_cases and portfolio is not None:
            with localcontext() as context:
                context.prec = 50
                count = Decimal(len(outcomes))
                candidate_mean = (
                    sum((item.candidate_net_return for item in outcomes), Decimal(0)) / count
                )
                differences = tuple(
                    item.candidate_net_return - item.primary_baseline_net_return
                    for item in outcomes
                )
                mean_excess = sum(differences, Decimal(0)) / count
                variance = sum(
                    ((item - mean_excess) ** 2 for item in differences), Decimal(0)
                ) / Decimal(len(outcomes) - 1)
                interval_lower = (
                    mean_excess - registration.paired_critical_value * (variance / count).sqrt()
                )
                total_pnl = sum((item.candidate_absolute_pnl for item in outcomes), Decimal(0))
                event_share = (
                    Decimal(0)
                    if total_pnl == 0
                    else max(item.candidate_absolute_pnl for item in outcomes) / total_pnl
                )
                downside: set[str] = set()
                _apply_economic_failures(
                    economic_reasons,
                    registration,
                    tuple(outcomes),
                    portfolio,
                    candidate_mean,
                    mean_excess,
                    interval_lower,
                    event_share,
                    downside,
                )
                if not _leave_one_out_positive(tuple(outcomes)):
                    economic_reasons.add("leave_one_event_conclusion_flips")
                if not _leave_one_regime_positive(tuple(outcomes)):
                    economic_reasons.add("leave_one_regime_conclusion_flips")
        reasons.update(economic_reasons)
        if not complete_denominator:
            disposition = StrategyValidationDisposition.INCONCLUSIVE
        elif economic_reasons:
            disposition = StrategyValidationDisposition.REJECTED
        elif reasons:
            disposition = StrategyValidationDisposition.INCONCLUSIVE
        else:
            disposition = StrategyValidationDisposition.ACCEPTED
        outcomes_hash = canonical_hash([_outcome_dict(item) for item in outcomes])
        window_seal_hash = self._prospective_window_seal_hash(registration)
        core: dict[str, object] = {
            "schema_version": STRATEGY_VALIDATION_REPORT_SCHEMA_V2,
            "harness_authority_id": self.harness_authority_id,
            "registration_id": registration.registration_id,
            "registration_hash": registration.registration_hash,
            "run_set_seal_hash": seal.seal_hash,
            "prospective_window_seal_hash": window_seal_hash,
            "disposition": disposition.value,
            "evidence_lane": _expected_lane(registration.program).value,
            "independent_case_count": len(outcomes),
            "nonempty_execution_count": nonempty,
            "outcomes_hash": outcomes_hash,
            "portfolio_metrics_hash": None if portfolio is None else portfolio.metrics_hash,
            "reasons": sorted(reasons),
            "execution_capability": "none",
        }
        report = StrategyValidationReportV2(
            report_id=f"strategy-validation-report-{canonical_hash(core)}",
            harness_authority_id=self.harness_authority_id,
            registration_id=registration.registration_id,
            registration_hash=registration.registration_hash,
            run_set_seal_hash=seal.seal_hash,
            prospective_window_seal_hash=window_seal_hash,
            disposition=disposition,
            evidence_lane=_expected_lane(registration.program),
            independent_case_count=len(outcomes),
            nonempty_execution_count=nonempty,
            outcomes_hash=outcomes_hash,
            portfolio_metrics_hash=None if portfolio is None else portfolio.metrics_hash,
            reasons=tuple(sorted(reasons)),
        )
        self.store.artifacts.put_json(report.to_dict())
        return report

    def revalidate(self, report: StrategyValidationReportV2) -> None:
        if report.harness_authority_id != self.harness_authority_id:
            raise ValueError("strategy report belongs to a different Harness authority")
        if report != self.evaluate(report.registration_id):
            raise ValueError("strategy report does not match the current Harness authority")

    def _registration(self, registration_id: str) -> StrategyValidationRegistration:
        with self.store.authority_transaction() as connection:
            row = connection.execute(
                "SELECT artifact_hash FROM strategy_validation_registrations_v2 "
                "WHERE registration_id = ?",
                (registration_id,),
            ).fetchone()
        if row is None:
            raise KeyError(f"unknown strategy validation registration: {registration_id}")
        return _strategy_registration_from_dict(
            cast(dict[str, object], self.store.artifacts.read_json(cast(str, row["artifact_hash"])))
        )

    def _seal(self, registration_id: str) -> StrategyRunSetSeal:
        with self.store.authority_transaction() as connection:
            row = connection.execute(
                "SELECT artifact_hash FROM strategy_run_set_seals_v2 WHERE registration_id = ?",
                (registration_id,),
            ).fetchone()
        if row is None:
            raise KeyError("strategy run set is not sealed")
        return _strategy_run_set_seal_from_dict(
            cast(dict[str, object], self.store.artifacts.read_json(cast(str, row["artifact_hash"])))
        )

    def _terminals(self, registration_id: str) -> tuple[StrategyCaseTerminal, ...]:
        with self.store.authority_transaction() as connection:
            rows = connection.execute(
                "SELECT artifact_hash FROM strategy_case_terminals_v2 WHERE registration_id = ?",
                (registration_id,),
            ).fetchall()
        return tuple(
            _strategy_terminal_from_dict(
                cast(
                    dict[str, object],
                    self.store.artifacts.read_json(cast(str, row["artifact_hash"])),
                )
            )
            for row in rows
        )

    def _plan(self, run_id: str) -> StrategyCaseRunPlan:
        with self.store.authority_transaction() as connection:
            row = connection.execute(
                "SELECT artifact_hash FROM strategy_case_run_plans_v2 WHERE run_id = ?", (run_id,)
            ).fetchone()
        if row is None:
            raise KeyError("strategy terminal has no frozen pre-run plan")
        return _strategy_run_plan_from_dict(
            cast(dict[str, object], self.store.artifacts.read_json(cast(str, row["artifact_hash"])))
        )

    def _measurements(
        self,
        terminal: StrategyCaseTerminal,
        case_id: str,
        *,
        plan: StrategyCaseRunPlan,
        registration: StrategyValidationRegistration,
    ) -> tuple[
        tuple[
            StrategyMeasurementArtifact,
            StrategyMeasurementArtifact,
            StrategyBacktestOutcomeReceipt,
            StrategyBacktestOutcomeReceipt,
        ]
        | None,
        str | None,
    ]:
        hashes = (
            terminal.candidate_measurement_artifact_hash,
            terminal.baseline_measurement_artifact_hash,
        )
        paths = (
            terminal.candidate_measurement_artifact_path,
            terminal.baseline_measurement_artifact_path,
        )
        if None in hashes or None in paths:
            return None, "missing_actual_measurement"
        loaded: list[StrategyMeasurementArtifact] = []
        for arm_name, expected_hash, expected_path in zip(
            ("candidate", "primary_baseline"), hashes, paths, strict=True
        ):
            assert expected_hash is not None and expected_path is not None
            stored = self.store.artifacts.get(expected_hash, media_type="application/json")
            if str(stored.path) != expected_path:
                raise ValueError("terminal measurement path differs from Harness CAS")
            payload = self.store.artifacts.read_json(expected_hash)
            if not isinstance(payload, dict):
                return None, f"{arm_name}_measurement_invalid"
            try:
                loaded.append(_strategy_measurement_from_dict(cast(dict[str, object], payload)))
            except (KeyError, TypeError, ValueError):
                return None, f"{arm_name}_measurement_invalid"
        candidate, baseline = loaded
        if candidate.case_id != case_id:
            return None, "candidate_measurement_case_mismatch"
        if baseline.case_id != case_id:
            return None, "primary_baseline_measurement_case_mismatch"
        if candidate.arm != "candidate":
            return None, "candidate_measurement_arm_mismatch"
        if baseline.arm != "primary_baseline":
            return None, "primary_baseline_measurement_arm_mismatch"
        receipts: list[StrategyBacktestOutcomeReceipt] = []
        for expected_arm, measurement in (
            (StrategyBacktestArm.CANDIDATE, candidate),
            (StrategyBacktestArm.PRIMARY_BASELINE, baseline),
        ):
            try:
                receipt, result = reopen_strategy_backtest_outcome(
                    self.store, measurement.outcome_receipt_id
                )
            except KeyError:
                return None, f"{expected_arm.value}_outcome_receipt_missing"
            try:
                _verify_receipt_for_plan(
                    receipt=receipt,
                    result=result,
                    expected_arm=expected_arm,
                    plan=plan,
                    registration=registration,
                )
            except ValueError:
                return None, f"{expected_arm.value}_outcome_receipt_ownership_mismatch"
            receipts.append(receipt)
            if measurement != _measurement_from_receipt(receipt):
                return None, f"{expected_arm.value}_measurement_receipt_divergence"
        candidate_receipt, baseline_receipt = receipts
        return (candidate, baseline, candidate_receipt, baseline_receipt), None

    def _verify_terminal_owner(
        self,
        terminal: StrategyCaseTerminal,
        *,
        plan: StrategyCaseRunPlan | None = None,
    ) -> None:
        from market_impact_agent.agent_engine import reopen_authoritative_agent_terminal

        journal = RunJournal.authoritative(self.store)
        run = journal.get_run(terminal.run_id)
        if (
            run.harness_authority_id != self.harness_authority_id
            or run.status is not terminal.run_status
            or run.created_at != terminal.started_at
            or run.updated_at != terminal.finished_at
        ):
            raise ValueError("strategy terminal differs from its authoritative Run row")
        if terminal.run_status is RunStatus.COMPLETED and (
            run.terminal_artifact_id != terminal.judgment_artifact_hash
        ):
            raise ValueError("strategy terminal differs from the actual Judgment artifact")
        if run.terminal_artifact_id is None:
            raise ValueError("strategy terminal Run row has no terminal artifact")
        judgment = reopen_authoritative_agent_terminal(
            journal=journal,
            artifact_store=self.store.artifacts,
            run_id=terminal.run_id,
            status=terminal.run_status,
            finished_at=terminal.finished_at,
            terminal_artifact_hash=run.terminal_artifact_id,
        )
        if plan is None:
            plan = self._plan(terminal.run_id)
        if judgment is not None and (
            judgment.runtime_config_hash != plan.model_profile_hash
            or judgment.prompt_hash != plan.prompt_hash
            or canonical_hash(list(judgment.skill_hashes)) != plan.skill_catalog_hash
            or canonical_hash(list(judgment.tool_manifest_hashes)) != plan.tool_manifest_hash
        ):
            raise ValueError("Judgment Artifact differs from the frozen strategy run plan")
        expected_manifest = {
            "schema_version": "market-impact.strategy-run-manifest.v2",
            "harness_authority_id": self.harness_authority_id,
            "run_id": terminal.run_id,
            "plan_id": terminal.plan_id,
            "plan_hash": terminal.plan_hash,
            "status": terminal.run_status.value,
            "started_at": terminal.started_at.isoformat(),
            "finished_at": terminal.finished_at.isoformat(),
            "judgment_artifact_hash": terminal.judgment_artifact_hash,
            "journal_hash_before_terminal": journal.journal_hash(terminal.run_id),
        }
        if self.store.artifacts.read_json(terminal.run_manifest_hash) != expected_manifest:
            raise ValueError("strategy Run Manifest differs from its authoritative owners")

    def _prospective_window_seal_hash(
        self, registration: StrategyValidationRegistration
    ) -> str | None:
        if registration.program is StrategyValidationProgram.HISTORICAL_STRICT:
            return None
        with self.store.authority_transaction() as connection:
            row = connection.execute(
                "SELECT prospective_window_seal_hash "
                "FROM strategy_validation_registrations_v2 WHERE registration_id = ?",
                (registration.registration_id,),
            ).fetchone()
        if row is None or row["prospective_window_seal_hash"] is None:
            raise ValueError("prospective Registration has no bound strategy window")
        expected_hash = cast(str, row["prospective_window_seal_hash"])
        return self._validated_prospective_window(
            registration, expected_artifact_hash=expected_hash
        )

    def _validated_prospective_window(
        self,
        registration: StrategyValidationRegistration,
        *,
        expected_artifact_hash: str | None,
    ) -> str | None:
        if registration.program is StrategyValidationProgram.HISTORICAL_STRICT:
            return None
        with self.store.authority_transaction() as connection:
            rows = connection.execute(
                """
                SELECT * FROM strategy_window_seals_v2
                WHERE strategy_epoch_id = ? AND stale = 0
                ORDER BY sealed_at DESC
                """,
                (registration.strategy_epoch_id,),
            ).fetchall()
            if len(rows) != 1:
                raise ValueError(
                    "prospective Registration requires exactly one non-stale strategy window"
                )
            seal_row = rows[0]
            artifact_hash = cast(str, seal_row["artifact_hash"])
            if expected_artifact_hash is not None and artifact_hash != expected_artifact_hash:
                raise ValueError("prospective Registration window seal changed after binding")
            seal_payload = cast(dict[str, object], self.store.artifacts.read_json(artifact_hash))
            if (
                seal_payload.get("harness_authority_id") != self.harness_authority_id
                or seal_payload.get("strategy_epoch_id") != registration.strategy_epoch_id
                or seal_payload.get("stale") is not False
            ):
                raise ValueError(
                    "prospective window seal belongs to a different authority or epoch"
                )
            window_row = connection.execute(
                "SELECT * FROM strategy_windows_v2 WHERE window_id = ?",
                (seal_row["window_id"],),
            ).fetchone()
            mapping_rows = connection.execute(
                "SELECT registration_id, case_id, root_event_id, regime "
                "FROM strategy_window_mappings_v2 WHERE window_id = ? "
                "ORDER BY registration_id",
                (seal_row["window_id"],),
            ).fetchall()
            if window_row is None or not mapping_rows:
                raise ValueError("prospective window is missing its frozen mapping")
            window_core = {
                "harness_authority_id": cast(str, window_row["harness_authority_id"]),
                "strategy_epoch_id": cast(str, window_row["strategy_epoch_id"]),
                "qualification_policy_hash": cast(str, window_row["qualification_policy_hash"]),
                "opened_at": cast(str, window_row["opened_at"]),
                "cutoff_at": cast(str, window_row["cutoff_at"]),
                "registration_mapping": [
                    {
                        "registration_id": cast(str, item["registration_id"]),
                        "case_id": cast(str, item["case_id"]),
                        "root_event_id": cast(str, item["root_event_id"]),
                        "regime": cast(str, item["regime"]),
                    }
                    for item in mapping_rows
                ],
            }
            if (
                window_row["harness_authority_id"] != self.harness_authority_id
                or window_row["strategy_epoch_id"] != registration.strategy_epoch_id
                or window_row["window_id"] != f"strategy-window-{canonical_hash(window_core)}"
            ):
                raise ValueError("prospective window identity or mapping was altered")
            sealed_at = datetime.fromisoformat(cast(str, seal_payload["sealed_at"]))
            if registration.created_at < sealed_at:
                raise ValueError("prospective Registration predates its strategy window seal")
            event_rows = connection.execute(
                """
                SELECT event.*,
                       mapping.registration_id AS mapping_registration_id,
                       mapping.case_id AS mapped_case_id,
                       mapping.root_event_id AS mapped_root_event_id,
                       mapping.regime AS mapped_regime,
                       admission.artifact_hash AS stored_admission_hash,
                       admission.registration_id AS admission_registration_id,
                       admission.admitted_at AS stored_admitted_at
                FROM strategy_window_events_v2 AS event
                JOIN strategy_window_mappings_v2 AS mapping
                  ON mapping.window_id = event.window_id
                 AND mapping.case_id = event.case_id
                JOIN prospective_trigger_admissions AS admission
                  ON admission.admission_id = event.admission_id
                WHERE event.window_id = ? ORDER BY event.sequence
                """,
                (seal_row["window_id"],),
            ).fetchall()
            if not event_rows:
                raise ValueError("prospective strategy window has an empty denominator")
            previous_hash: str | None = None
            observed_cases: list[tuple[str, str, str]] = []
            admission_ids: list[str] = []
            for expected_sequence, event in enumerate(event_rows, start=1):
                if (
                    event["sequence"] != expected_sequence
                    or event["previous_hash"] != previous_hash
                ):
                    raise ValueError("prospective strategy window sequence is invalid")
                core = {
                    "window_id": cast(str, event["window_id"]),
                    "sequence": cast(int, event["sequence"]),
                    "admission_id": cast(str, event["admission_id"]),
                    "admission_hash": cast(str, event["admission_hash"]),
                    "case_id": cast(str, event["case_id"]),
                    "root_event_id": cast(str, event["root_event_id"]),
                    "regime": cast(str, event["regime"]),
                    "admitted_at": cast(str, event["admitted_at"]),
                    "previous_hash": previous_hash,
                }
                event_hash = canonical_hash(core)
                if event["event_hash"] != event_hash:
                    raise ValueError("prospective strategy window hash chain is invalid")
                if (
                    event["mapping_registration_id"] != event["admission_registration_id"]
                    or event["case_id"] != event["mapped_case_id"]
                    or event["root_event_id"] != event["mapped_root_event_id"]
                    or event["regime"] != event["mapped_regime"]
                    or event["admission_hash"] != event["stored_admission_hash"]
                    or event["admitted_at"] != event["stored_admitted_at"]
                ):
                    raise ValueError(
                        "prospective strategy event differs from its Trigger Admission mapping"
                    )
                self.store.artifacts.get(
                    cast(str, event["admission_hash"]), media_type="application/json"
                )
                previous_hash = event_hash
                admission_ids.append(cast(str, event["admission_id"]))
                observed_cases.append(
                    (
                        cast(str, event["case_id"]),
                        cast(str, event["root_event_id"]),
                        cast(str, event["regime"]),
                    )
                )
            if (
                seal_payload.get("last_sequence") != len(event_rows)
                or seal_payload.get("journal_head_hash") != previous_hash
                or seal_payload.get("admission_ids") != admission_ids
            ):
                raise ValueError("prospective window seal differs from its complete Journal")
            registered_cases = [
                (item.case_id, item.root_event_id, item.regime)
                for item in registration.evaluation_cases
            ]
            if sorted(observed_cases) != sorted(registered_cases):
                raise ValueError(
                    "prospective Registration differs from the complete admission denominator"
                )
        return artifact_hash


def _initialize_v2_tables(connection: sqlite3.Connection) -> None:
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS strategy_validation_registrations_v2 (
            registration_id TEXT PRIMARY KEY,
            registration_hash TEXT NOT NULL,
            strategy_epoch_id TEXT NOT NULL,
            artifact_hash TEXT NOT NULL,
            harness_authority_id TEXT NOT NULL,
            prospective_window_seal_hash TEXT
        );
        CREATE TABLE IF NOT EXISTS strategy_case_run_plans_v2 (
            run_id TEXT PRIMARY KEY,
            plan_id TEXT NOT NULL UNIQUE,
            plan_hash TEXT NOT NULL,
            registration_id TEXT NOT NULL,
            case_id TEXT NOT NULL,
            artifact_hash TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS strategy_plans_registration_case_v2
            ON strategy_case_run_plans_v2(registration_id, case_id);
        CREATE TABLE IF NOT EXISTS strategy_case_measurements_v2 (
            run_id TEXT NOT NULL,
            arm TEXT NOT NULL,
            case_id TEXT NOT NULL,
            measured_at TEXT NOT NULL,
            artifact_hash TEXT NOT NULL,
            PRIMARY KEY(run_id, arm)
        );
        CREATE TABLE IF NOT EXISTS strategy_case_terminals_v2 (
            terminal_id TEXT PRIMARY KEY,
            run_id TEXT NOT NULL UNIQUE,
            plan_id TEXT NOT NULL,
            registration_id TEXT NOT NULL,
            case_id TEXT NOT NULL,
            started_at TEXT NOT NULL,
            finished_at TEXT NOT NULL,
            run_status TEXT NOT NULL,
            run_manifest_hash TEXT NOT NULL,
            artifact_hash TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS strategy_run_set_seals_v2 (
            registration_id TEXT PRIMARY KEY,
            seal_id TEXT NOT NULL UNIQUE,
            artifact_hash TEXT NOT NULL,
            sealed_at TEXT NOT NULL
        );
        """
    )
    registration_columns = {
        cast(str, row[1])
        for row in connection.execute("PRAGMA table_info(strategy_validation_registrations_v2)")
    }
    if "harness_authority_id" not in registration_columns:
        connection.execute(
            "ALTER TABLE strategy_validation_registrations_v2 ADD COLUMN harness_authority_id TEXT"
        )
    if "prospective_window_seal_hash" not in registration_columns:
        connection.execute(
            "ALTER TABLE strategy_validation_registrations_v2 "
            "ADD COLUMN prospective_window_seal_hash TEXT"
        )


def _initialize_journal_v2_tables(journal: RunJournal) -> None:
    with sqlite3.connect(journal.path) as connection:
        _initialize_v2_tables(connection)


def _strategy_run_plan_from_dict(payload: dict[str, object]) -> StrategyCaseRunPlan:
    return StrategyCaseRunPlan(
        plan_id=cast(str, payload["plan_id"]),
        run_id=cast(str, payload["run_id"]),
        harness_authority_id=cast(str, payload["harness_authority_id"]),
        registration_id=cast(str, payload["registration_id"]),
        registration_hash=cast(str, payload["registration_hash"]),
        strategy_epoch_id=cast(str, payload["strategy_epoch_id"]),
        case_id=cast(str, payload["case_id"]),
        root_event_id=cast(str, payload["root_event_id"]),
        regime=cast(str, payload["regime"]),
        role=StrategyCaseRole(cast(str, payload["role"])),
        evidence_lane=StrategyEvidenceLane(cast(str, payload["evidence_lane"])),
        input_hash=cast(str, payload["input_hash"]),
        data_snapshot_hash=cast(str, payload["data_snapshot_hash"]),
        evidence_lineage_hash=cast(str, payload["evidence_lineage_hash"]),
        qualification_report_hash=cast(str, payload["qualification_report_hash"]),
        admission_hash=cast(str, payload["admission_hash"]),
        evidence_owner_id=cast(str | None, payload["evidence_owner_id"]),
        evidence_owner_hash=cast(str, payload["evidence_owner_hash"]),
        evidence_unavailable_reason=cast(str | None, payload["evidence_unavailable_reason"]),
        model_profile_hash=cast(str, payload["model_profile_hash"]),
        prompt_hash=cast(str, payload["prompt_hash"]),
        skill_catalog_hash=cast(str, payload["skill_catalog_hash"]),
        tool_manifest_hash=cast(str, payload["tool_manifest_hash"]),
        universe_hash=cast(str, payload["universe_hash"]),
        cost_model_hash=cast(str, payload["cost_model_hash"]),
        fill_model_hash=cast(str, payload["fill_model_hash"]),
        primary_baseline_id=cast(str, payload["primary_baseline_id"]),
        primary_baseline_definition_hash=cast(str, payload["primary_baseline_definition_hash"]),
        primary_baseline_configuration_hash=cast(
            str, payload["primary_baseline_configuration_hash"]
        ),
        development_selection_evidence_hash=cast(
            str, payload["development_selection_evidence_hash"]
        ),
    )


def _strategy_terminal_from_dict(payload: dict[str, object]) -> StrategyCaseTerminal:
    return StrategyCaseTerminal(
        terminal_id=cast(str, payload["terminal_id"]),
        harness_authority_id=cast(str, payload["harness_authority_id"]),
        plan_id=cast(str, payload["plan_id"]),
        plan_hash=cast(str, payload["plan_hash"]),
        run_id=cast(str, payload["run_id"]),
        run_status=RunStatus(cast(str, payload["run_status"])),
        started_at=datetime.fromisoformat(cast(str, payload["started_at"])),
        finished_at=datetime.fromisoformat(cast(str, payload["finished_at"])),
        judgment_artifact_hash=cast(str | None, payload["judgment_artifact_hash"]),
        run_manifest_hash=cast(str, payload["run_manifest_hash"]),
        candidate_measurement_artifact_hash=cast(
            str | None, payload["candidate_measurement_artifact_hash"]
        ),
        candidate_measurement_artifact_path=cast(
            str | None, payload["candidate_measurement_artifact_path"]
        ),
        baseline_measurement_artifact_hash=cast(
            str | None, payload["baseline_measurement_artifact_hash"]
        ),
        baseline_measurement_artifact_path=cast(
            str | None, payload["baseline_measurement_artifact_path"]
        ),
    )


def _strategy_run_set_seal_from_dict(payload: dict[str, object]) -> StrategyRunSetSeal:
    return StrategyRunSetSeal(
        seal_id=cast(str, payload["seal_id"]),
        harness_authority_id=cast(str, payload["harness_authority_id"]),
        registration_id=cast(str, payload["registration_id"]),
        sealed_at=datetime.fromisoformat(cast(str, payload["sealed_at"])),
        terminal_ids=tuple(cast(list[str], payload["terminal_ids"])),
        selected_terminal_ids=tuple(cast(list[str], payload["selected_terminal_ids"])),
        run_set_hash=cast(str, payload["run_set_hash"]),
    )


def _strategy_measurement_from_dict(payload: dict[str, object]) -> StrategyMeasurementArtifact:
    def decimal_or_none(name: str) -> Decimal | None:
        value = payload[name]
        return None if value is None else Decimal(cast(str, value))

    return StrategyMeasurementArtifact(
        case_id=cast(str, payload["case_id"]),
        arm=cast(str, payload["arm"]),
        outcome_receipt_id=cast(str, payload["outcome_receipt_id"]),
        outcome_receipt_hash=cast(str, payload["outcome_receipt_hash"]),
        net_return=decimal_or_none("net_return"),
        absolute_pnl=decimal_or_none("absolute_pnl"),
        portfolio_net_return=decimal_or_none("portfolio_net_return"),
        max_drawdown=decimal_or_none("max_drawdown"),
        cvar95=decimal_or_none("cvar95"),
        sharpe=decimal_or_none("sharpe"),
        sortino=decimal_or_none("sortino"),
        stressed_net_return=decimal_or_none("stressed_net_return"),
        turnover=decimal_or_none("turnover"),
        adverse_excursion=decimal_or_none("adverse_excursion"),
        liquidity_cost=decimal_or_none("liquidity_cost"),
        avoided_loss=Decimal(cast(str, payload["avoided_loss"])),
        false_avoidance_opportunity_cost=Decimal(
            cast(str, payload["false_avoidance_opportunity_cost"])
        ),
        nonempty_execution=cast(bool, payload["nonempty_execution"]),
        missing_reasons=tuple(cast(list[str], payload["missing_reasons"])),
    )


def _verify_receipt_for_plan(
    *,
    receipt: StrategyBacktestOutcomeReceipt,
    result: object,
    expected_arm: StrategyBacktestArm,
    plan: StrategyCaseRunPlan,
    registration: StrategyValidationRegistration,
) -> None:
    from market_impact_agent.backtests import BacktestResult, BacktestRunStatus

    if not isinstance(result, BacktestResult):
        raise TypeError("strategy outcome owner did not reopen a Backtest Result")
    request = result.manifest.request
    expected_variant = (
        registration.candidate_variant
        if expected_arm is StrategyBacktestArm.CANDIDATE
        else registration.primary_baseline.variant
    )
    if (
        receipt.harness_authority_id != plan.harness_authority_id
        or receipt.case_id != plan.case_id
        or receipt.arm is not expected_arm
        or receipt.strategy_variant_hash != expected_variant.strategy_variant_hash
        or not expected_variant.matches_request(request)
        or receipt.strategy_ref != expected_variant.strategy_ref
        or receipt.target_selection_ref != expected_variant.target_selection_ref
        or receipt.engine_config_hash != result.manifest.engine_config_hash
        or receipt.simulation_data_granularity != expected_variant.data_granularity
        or receipt.simulation_book_type != expected_variant.book_type
        or receipt.simulation_fill_model != expected_variant.fill_model
        or receipt.simulation_fee_model != expected_variant.fee_model
        or receipt.simulation_venue_ruleset != expected_variant.venue_ruleset
        or receipt.simulation_base_currency != expected_variant.base_currency
        or receipt.simulation_starting_cash != expected_variant.starting_cash
        or receipt.simulation_random_seed != expected_variant.random_seed
        or result.status is not BacktestRunStatus.COMPLETED
        or request.signal.event_id != plan.root_event_id
        or receipt.source_snapshot_artifact_hash != plan.data_snapshot_hash
        or receipt.universe_hash != plan.universe_hash
        or receipt.cost_model_hash != plan.cost_model_hash
        or receipt.fill_model_hash != plan.fill_model_hash
        or tuple(item.side for item in receipt.fills)
        != (() if expected_variant.strategy_ref == "cash-no-action.v1" else ("buy", "sell"))
    ):
        raise ValueError(
            "strategy outcome receipt differs from frozen case, variant, cost, fill, or universe"
        )


def _measurement_from_receipt(
    receipt: StrategyBacktestOutcomeReceipt,
) -> StrategyMeasurementArtifact:
    return StrategyMeasurementArtifact(
        case_id=receipt.case_id,
        arm=receipt.arm.value,
        outcome_receipt_id=receipt.receipt_id,
        outcome_receipt_hash=receipt.receipt_hash,
        net_return=receipt.net_return,
        absolute_pnl=abs(receipt.net_pnl),
        portfolio_net_return=receipt.portfolio_net_return,
        max_drawdown=receipt.max_drawdown,
        cvar95=receipt.cvar95,
        sharpe=receipt.sharpe,
        sortino=receipt.sortino,
        stressed_net_return=receipt.stressed_net_return,
        turnover=receipt.turnover,
        adverse_excursion=receipt.adverse_excursion,
        liquidity_cost=receipt.liquidity_cost,
        avoided_loss=Decimal(0),
        false_avoidance_opportunity_cost=Decimal(0),
        nonempty_execution=bool(receipt.fills),
        missing_reasons=tuple(
            sorted(f"{item.name}:{item.reason}" for item in receipt.missing_metrics)
        ),
    )


def _portfolio_from_receipts(
    registration: StrategyValidationRegistration,
    candidates: list[tuple[StrategyCaseDefinition, StrategyBacktestOutcomeReceipt]],
    baselines: list[tuple[StrategyCaseDefinition, StrategyBacktestOutcomeReceipt]],
    *,
    artifact_store: ArtifactStore,
) -> tuple[StrategyPortfolioMetrics | None, str | None]:
    if not candidates or len(candidates) != len(baselines):
        return None, "portfolio_aggregation_missing_case_receipt"
    ordered_candidates = tuple(
        sorted(candidates, key=lambda item: (item[0].root_event_id, item[0].case_id))
    )
    ordered_baselines = tuple(
        sorted(baselines, key=lambda item: (item[0].root_event_id, item[0].case_id))
    )
    if tuple(item[0].case_id for item in ordered_candidates) != tuple(
        item[0].case_id for item in ordered_baselines
    ):
        return None, "portfolio_aggregation_case_mismatch"

    candidate_path, reason = _aggregate_capital_paths(
        registration, ordered_candidates, stressed=False, artifact_store=artifact_store
    )
    if candidate_path is None:
        return None, reason
    baseline_path, reason = _aggregate_capital_paths(
        registration, ordered_baselines, stressed=False, artifact_store=artifact_store
    )
    if baseline_path is None:
        return None, reason
    candidate_stress_path, reason = _aggregate_capital_paths(
        registration, ordered_candidates, stressed=True, artifact_store=artifact_store
    )
    if candidate_stress_path is None:
        return None, reason
    baseline_stress_path, reason = _aggregate_capital_paths(
        registration, ordered_baselines, stressed=True, artifact_store=artifact_store
    )
    if baseline_stress_path is None:
        return None, reason

    candidate_metrics = _metrics_from_portfolio_path(candidate_path)
    baseline_metrics = _metrics_from_portfolio_path(baseline_path)
    candidate_stress = _portfolio_path_return(candidate_stress_path)
    baseline_stress = _portfolio_path_return(baseline_stress_path)
    candidate_observations, reason = _aggregate_portfolio_observations(
        registration,
        ordered_candidates,
        candidate_path,
        stressed=False,
        artifact_store=artifact_store,
    )
    if candidate_observations is None:
        return None, reason
    baseline_observations, reason = _aggregate_portfolio_observations(
        registration,
        ordered_baselines,
        baseline_path,
        stressed=False,
        artifact_store=artifact_store,
    )
    if baseline_observations is None:
        return None, reason
    # The doubled-fee evidence is a second actual path, so its execution and
    # marked-position observations must be complete under the same policy even
    # though v2 exposes only its common-capital return.
    for items, path in (
        (ordered_candidates, candidate_stress_path),
        (ordered_baselines, baseline_stress_path),
    ):
        stress_observations, reason = _aggregate_portfolio_observations(
            registration,
            items,
            path,
            stressed=True,
            artifact_store=artifact_store,
        )
        if stress_observations is None:
            return None, reason

    return (
        StrategyPortfolioMetrics(
            candidate_net_return=candidate_metrics["net_return"],
            primary_baseline_net_return=baseline_metrics["net_return"],
            candidate_max_drawdown=candidate_metrics["max_drawdown"],
            primary_baseline_max_drawdown=baseline_metrics["max_drawdown"],
            candidate_cvar95=candidate_metrics["cvar95"],
            primary_baseline_cvar95=baseline_metrics["cvar95"],
            candidate_sharpe=candidate_metrics["sharpe"],
            primary_baseline_sharpe=baseline_metrics["sharpe"],
            candidate_sortino=candidate_metrics["sortino"],
            primary_baseline_sortino=baseline_metrics["sortino"],
            candidate_stressed_net_return=candidate_stress,
            primary_baseline_stressed_net_return=baseline_stress,
            candidate_turnover=candidate_observations["turnover"],
            primary_baseline_turnover=baseline_observations["turnover"],
            candidate_adverse_excursion=candidate_observations["adverse_excursion"],
            primary_baseline_adverse_excursion=baseline_observations["adverse_excursion"],
            candidate_liquidity_utilization=candidate_observations["liquidity"],
            primary_baseline_liquidity_utilization=baseline_observations["liquidity"],
            avoided_loss=Decimal(0),
            false_avoidance_opportunity_cost=Decimal(0),
        ),
        None,
    )


def _aggregate_portfolio_observations(
    registration: StrategyValidationRegistration,
    items: tuple[tuple[StrategyCaseDefinition, StrategyBacktestOutcomeReceipt], ...],
    portfolio_path: tuple[StrategyCapitalPoint, ...],
    *,
    stressed: bool,
    artifact_store: ArtifactStore,
) -> tuple[dict[str, Decimal] | None, str | None]:
    evidence: list[
        tuple[
            tuple[StrategyCapitalPoint, ...],
            tuple[StrategyBacktestFill, ...],
            tuple[StrategyAdverseExcursionPoint, ...],
            tuple[datetime, datetime] | None,
        ]
    ] = []
    for _, receipt in items:
        if not stressed:
            capital_path = receipt.capital_path
            fills = receipt.fills
            adverse_path = receipt.adverse_excursion_path
        else:
            if receipt.stress_evidence_artifact_hash is None:
                return None, "portfolio_aggregation_missing_stress_path"
            payload = artifact_store.read_json(receipt.stress_evidence_artifact_hash)
            if not isinstance(payload, dict):
                return None, "portfolio_aggregation_missing_stress_path"
            try:
                capital_path = tuple(
                    StrategyCapitalPoint(
                        datetime.fromisoformat(
                            cast(str, point["observed_at"]).replace("Z", "+00:00")
                        ),
                        Decimal(cast(str, point["equity"])),
                    )
                    for point in cast(list[dict[str, object]], payload["capital_path"])
                )
                fills = tuple(
                    StrategyBacktestFill(
                        side=cast(str, fill["side"]),
                        filled_at=datetime.fromisoformat(
                            cast(str, fill["filled_at"]).replace("Z", "+00:00")
                        ),
                        quantity=Decimal(cast(str, fill["quantity"])),
                        price=Decimal(cast(str, fill["price"])),
                        commission=Decimal(cast(str, fill["commission"])),
                        available_liquidity_quantity=(
                            None
                            if fill["available_liquidity_quantity"] is None
                            else Decimal(cast(str, fill["available_liquidity_quantity"]))
                        ),
                    )
                    for fill in cast(list[dict[str, object]], payload["fills"])
                )
                adverse_path = tuple(
                    StrategyAdverseExcursionPoint(
                        observed_at=datetime.fromisoformat(
                            cast(str, point["observed_at"]).replace("Z", "+00:00")
                        ),
                        adverse_excursion=Decimal(cast(str, point["adverse_excursion"])),
                    )
                    for point in cast(list[dict[str, object]], payload["adverse_excursion_path"])
                )
            except (KeyError, TypeError, ValueError):
                return None, "portfolio_aggregation_invalid_stress_observations"
        if not adverse_path:
            return None, (
                "portfolio_aggregation_missing_stress_adverse_excursion_path"
                if stressed
                else "portfolio_aggregation_missing_adverse_excursion_path"
            )
        if any(
            previous.observed_at >= current.observed_at
            for previous, current in pairwise(adverse_path)
        ):
            return None, (
                "portfolio_aggregation_invalid_stress_adverse_excursion_path"
                if stressed
                else "portfolio_aggregation_invalid_adverse_excursion_path"
            )
        if any(fill.available_liquidity_quantity is None for fill in fills):
            return None, (
                "portfolio_aggregation_missing_stress_liquidity_observation"
                if stressed
                else "portfolio_aggregation_missing_liquidity_observation"
            )
        position_interval: tuple[datetime, datetime] | None = None
        if fills:
            if (
                len(fills) != 2
                or fills[0].side != "buy"
                or fills[1].side != "sell"
                or fills[0].filled_at > fills[1].filled_at
            ):
                return None, (
                    "portfolio_aggregation_invalid_stress_position_interval"
                    if stressed
                    else "portfolio_aggregation_invalid_position_interval"
                )
            position_interval = (fills[0].filled_at, fills[1].filled_at)
        evidence.append((capital_path, fills, adverse_path, position_interval))

    observation_times = tuple(
        sorted(
            {
                *(point.observed_at for point in portfolio_path),
                *(
                    point.observed_at
                    for _, _, adverse_path, _ in evidence
                    for point in adverse_path
                ),
            }
        )
    )
    for _, _, adverse_path, position_interval in evidence:
        if position_interval is None:
            continue
        opened_at, closed_at = position_interval
        observed_times = {point.observed_at for point in adverse_path}
        required_times = {
            observed_at
            for observed_at in observation_times
            if opened_at <= observed_at <= closed_at
        }
        if not required_times.issubset(observed_times):
            return None, (
                "portfolio_aggregation_incomplete_stress_adverse_excursion_coverage"
                if stressed
                else "portfolio_aggregation_incomplete_adverse_excursion_coverage"
            )

    with localcontext() as context:
        context.prec = 50
        total_traded_notional = Decimal(0)
        total_available_liquidity = Decimal(0)
        for capital_path, fills, _, _ in evidence:
            for fill in fills:
                active_count = sum(
                    path[0].observed_at <= fill.filled_at <= path[-1].observed_at
                    for path, _, _, _ in evidence
                )
                if active_count == 0:
                    return None, "portfolio_aggregation_fill_outside_capital_path"
                try:
                    common_equity = _path_equity_before(portfolio_path, fill.filled_at)
                    receipt_equity = _path_equity_before(capital_path, fill.filled_at)
                except StopIteration:
                    return None, "portfolio_aggregation_fill_outside_capital_path"
                scale = common_equity / Decimal(active_count) / receipt_equity
                traded_notional = abs(fill.quantity * fill.price) * scale
                assert fill.available_liquidity_quantity is not None
                available_notional = fill.available_liquidity_quantity * fill.price * scale
                total_traded_notional += traded_notional
                total_available_liquidity += available_notional
        turnover = total_traded_notional / registration.portfolio_starting_capital
        liquidity = (
            Decimal(0)
            if total_traded_notional == 0
            else total_traded_notional / total_available_liquidity
        )

        adverse_excursion = Decimal(0)
        for observed_at in observation_times:
            active = tuple(
                adverse_path
                for _, _, adverse_path, position_interval in evidence
                if position_interval is not None
                and position_interval[0] <= observed_at <= position_interval[1]
            )
            if not active:
                continue
            weighted = sum(
                (
                    next(
                        point.adverse_excursion
                        for point in adverse_path
                        if point.observed_at == observed_at
                    )
                    for adverse_path in active
                ),
                Decimal(0),
            ) / Decimal(len(active))
            adverse_excursion = max(adverse_excursion, weighted)
    return {
        "turnover": turnover,
        "liquidity": liquidity,
        "adverse_excursion": adverse_excursion,
    }, None


def _aggregate_capital_paths(
    registration: StrategyValidationRegistration,
    items: tuple[tuple[StrategyCaseDefinition, StrategyBacktestOutcomeReceipt], ...],
    *,
    stressed: bool,
    artifact_store: ArtifactStore,
) -> tuple[tuple[StrategyCapitalPoint, ...] | None, str | None]:
    paths: list[tuple[StrategyCaseDefinition, tuple[StrategyCapitalPoint, ...]]] = []
    for definition, receipt in items:
        path = receipt.capital_path
        if stressed:
            if receipt.stress_evidence_artifact_hash is None:
                return None, "portfolio_aggregation_missing_stress_path"
            payload = artifact_store.read_json(receipt.stress_evidence_artifact_hash)
            if not isinstance(payload, dict):
                return None, "portfolio_aggregation_missing_stress_path"
            payload_fields = cast(dict[str, object], payload)
            capital_path = payload_fields.get("capital_path")
            if not isinstance(capital_path, list):
                return None, "portfolio_aggregation_missing_stress_path"
            try:
                path = tuple(
                    StrategyCapitalPoint(
                        datetime.fromisoformat(
                            cast(str, point["observed_at"]).replace("Z", "+00:00")
                        ),
                        Decimal(cast(str, point["equity"])),
                    )
                    for point in cast(list[dict[str, object]], capital_path)
                )
            except (KeyError, TypeError, ValueError):
                return None, "portfolio_aggregation_invalid_stress_path"
        if len(path) < 2 or any(
            previous.observed_at >= current.observed_at for previous, current in pairwise(path)
        ):
            return None, "portfolio_aggregation_missing_capital_path"
        paths.append((definition, path))

    timestamps = tuple(sorted({point.observed_at for _, path in paths for point in path}))
    if len(timestamps) < 2:
        return None, "portfolio_aggregation_missing_capital_path"
    equity = registration.portfolio_starting_capital
    result = [StrategyCapitalPoint(timestamps[0], equity)]
    for previous_at, current_at in pairwise(timestamps):
        active = tuple(
            (definition, path)
            for definition, path in paths
            if path[0].observed_at <= previous_at and current_at <= path[-1].observed_at
        )
        if len(active) > registration.portfolio_maximum_simultaneous_positions:
            return None, "portfolio_aggregation_overlap_exceeds_policy"
        if active:
            returns = tuple(
                _path_equity_at(path, current_at) / _path_equity_at(path, previous_at) - Decimal(1)
                for _, path in active
            )
            equity *= Decimal(1) + sum(returns, Decimal(0)) / Decimal(len(returns))
            if equity <= 0:
                return None, "portfolio_aggregation_nonpositive_capital"
        result.append(StrategyCapitalPoint(current_at, equity))
    return tuple(result), None


def _path_equity_at(path: tuple[StrategyCapitalPoint, ...], observed_at: datetime) -> Decimal:
    return next(point.equity for point in reversed(path) if point.observed_at <= observed_at)


def _path_equity_before(path: tuple[StrategyCapitalPoint, ...], observed_at: datetime) -> Decimal:
    return next(point.equity for point in reversed(path) if point.observed_at < observed_at)


def _portfolio_path_return(path: tuple[StrategyCapitalPoint, ...]) -> Decimal:
    return path[-1].equity / path[0].equity - Decimal(1)


def _metrics_from_portfolio_path(
    path: tuple[StrategyCapitalPoint, ...],
) -> dict[str, Decimal]:
    returns = tuple(
        current.equity / previous.equity - Decimal(1) for previous, current in pairwise(path)
    )
    max_drawdown = Decimal(0)
    peak = path[0].equity
    for point in path[1:]:
        peak = max(peak, point.equity)
        max_drawdown = max(max_drawdown, (peak - point.equity) / peak)
    tail_count = max(1, (len(returns) + 19) // 20)
    cvar95 = max(
        Decimal(0),
        sum(sorted((-item for item in returns), reverse=True)[:tail_count], Decimal(0))
        / Decimal(tail_count),
    )
    with localcontext() as context:
        context.prec = 50
        mean = sum(returns, Decimal(0)) / Decimal(len(returns))
        variance = (
            Decimal(0)
            if len(returns) < 2
            else sum(((item - mean) ** 2 for item in returns), Decimal(0))
            / Decimal(len(returns) - 1)
        )
        sharpe = Decimal(0) if variance == 0 else mean / variance.sqrt() * Decimal(252).sqrt()
        downside = tuple(min(item, Decimal(0)) for item in returns)
        downside_deviation = (
            sum((item * item for item in downside), Decimal(0)) / Decimal(len(downside))
        ).sqrt()
        sortino = (
            Decimal(0)
            if downside_deviation == 0
            else mean / downside_deviation * Decimal(252).sqrt()
        )
    return {
        "net_return": _portfolio_path_return(path),
        "max_drawdown": max_drawdown,
        "cvar95": cvar95,
        "sharpe": sharpe,
        "sortino": sortino,
    }


def _strategy_registration_from_dict(payload: dict[str, object]) -> StrategyValidationRegistration:
    baselines = tuple(
        StrategyBaselineDefinition(
            baseline_id=cast(str, item["baseline_id"]),
            definition_hash=cast(str, item["definition_hash"]),
            configuration_hash=cast(str, item["configuration_hash"]),
            variant=_strategy_variant_from_dict(cast(dict[str, object], item["variant"])),
        )
        for item in cast(list[dict[str, object]], payload["baseline_definitions"])
    )
    cases = tuple(
        StrategyCaseDefinition(
            case_id=cast(str, item["case_id"]),
            root_event_id=cast(str, item["root_event_id"]),
            regime=cast(str, item["regime"]),
            role=StrategyCaseRole(cast(str, item["role"])),
            source_snapshot_id=cast(str | None, item.get("source_snapshot_id")),
            evidence_binding_ref=cast(str | None, item.get("evidence_binding_ref")),
        )
        for item in cast(list[dict[str, object]], payload["case_definitions"])
    )
    return StrategyValidationRegistration(
        registration_id=cast(str, payload["registration_id"]),
        strategy_epoch_id=cast(str, payload["strategy_epoch_id"]),
        program=StrategyValidationProgram(cast(str, payload["program"])),
        model_profile_hash=cast(str, payload["model_profile_hash"]),
        prompt_hash=cast(str, payload["prompt_hash"]),
        skill_catalog_hash=cast(str, payload["skill_catalog_hash"]),
        tool_manifest_hash=cast(str, payload["tool_manifest_hash"]),
        universe_hash=cast(str, payload["universe_hash"]),
        cost_model_hash=cast(str, payload["cost_model_hash"]),
        fill_model_hash=cast(str, payload["fill_model_hash"]),
        candidate_variant=_strategy_variant_from_dict(
            cast(dict[str, object], payload["candidate_variant"])
        ),
        primary_baseline_id=cast(str, payload["primary_baseline_id"]),
        baseline_definitions=baselines,
        development_selection_evidence_hash=cast(
            str, payload["development_selection_evidence_hash"]
        ),
        run_selection_policy=cast(str, payload["run_selection_policy"]),
        portfolio_aggregation_policy=cast(str, payload["portfolio_aggregation_policy"]),
        portfolio_case_ordering=cast(str, payload["portfolio_case_ordering"]),
        portfolio_starting_capital=Decimal(cast(str, payload["portfolio_starting_capital"])),
        portfolio_maximum_simultaneous_positions=cast(
            int, payload["portfolio_maximum_simultaneous_positions"]
        ),
        case_definitions=cases,
        prospective_cohort_id=cast(str | None, payload["prospective_cohort_id"]),
        prospective_cohort_seal_hash=cast(str | None, payload["prospective_cohort_seal_hash"]),
        created_at=datetime.fromisoformat(cast(str, payload["created_at"])),
        paired_critical_value=Decimal(cast(str, payload["paired_critical_value"])),
        maximum_drawdown_ratio=Decimal(cast(str, payload["maximum_drawdown_ratio"])),
        maximum_cvar_ratio=Decimal(cast(str, payload["maximum_cvar_ratio"])),
        maximum_downside_loss_ratio=Decimal(cast(str, payload["maximum_downside_loss_ratio"])),
        maximum_single_event_share=Decimal(cast(str, payload["maximum_single_event_share"])),
        execution_capability=cast(str, payload["execution_capability"]),
    )


def _strategy_variant_from_dict(payload: dict[str, object]) -> StrategyBacktestVariant:
    simulation = cast(dict[str, object], payload["simulation"])
    return StrategyBacktestVariant(
        strategy_variant_hash=cast(str, payload["strategy_variant_hash"]),
        arm=StrategyBacktestArm(cast(str, payload["arm"])),
        baseline_id=cast(str | None, payload["baseline_id"]),
        strategy_ref=cast(str, payload["strategy_ref"]),
        target_selection_ref=cast(str, payload["target_selection_ref"]),
        request_market=cast(str, cast(dict[str, object], payload["request_template"])["market"]),
        request_instrument_ids=tuple(
            cast(list[str], cast(dict[str, object], payload["request_template"])["instrument_ids"])
        ),
        request_horizons_sessions=tuple(
            cast(
                list[int],
                cast(dict[str, object], payload["request_template"])["horizons_sessions"],
            )
        ),
        request_signal_side=cast(
            str, cast(dict[str, object], payload["request_template"])["signal_side"]
        ),
        data_granularity=cast(str, simulation["data_granularity"]),
        book_type=cast(str, simulation["book_type"]),
        fill_model=cast(str, simulation["fill_model"]),
        fee_model=cast(str, simulation["fee_model"]),
        venue_ruleset=cast(str, simulation["venue_ruleset"]),
        base_currency=cast(str, simulation["base_currency"]),
        starting_cash=Decimal(cast(str, simulation["starting_cash"])),
        random_seed=cast(int, simulation["random_seed"]),
    )


def _verify_plan_registration(
    plan: StrategyCaseRunPlan,
    registration: StrategyValidationRegistration,
    definition: StrategyCaseDefinition,
) -> None:
    primary = registration.primary_baseline
    expected: tuple[object, ...] = (
        registration.registration_id,
        registration.registration_hash,
        registration.strategy_epoch_id,
        definition.case_id,
        definition.root_event_id,
        definition.regime,
        definition.role,
        registration.model_profile_hash,
        registration.prompt_hash,
        registration.skill_catalog_hash,
        registration.tool_manifest_hash,
        registration.universe_hash,
        registration.cost_model_hash,
        registration.fill_model_hash,
        registration.primary_baseline_id,
        primary.definition_hash,
        primary.configuration_hash,
        registration.development_selection_evidence_hash,
    )
    actual: tuple[object, ...] = (
        plan.registration_id,
        plan.registration_hash,
        plan.strategy_epoch_id,
        plan.case_id,
        plan.root_event_id,
        plan.regime,
        plan.role,
        plan.model_profile_hash,
        plan.prompt_hash,
        plan.skill_catalog_hash,
        plan.tool_manifest_hash,
        plan.universe_hash,
        plan.cost_model_hash,
        plan.fill_model_hash,
        plan.primary_baseline_id,
        plan.primary_baseline_definition_hash,
        plan.primary_baseline_configuration_hash,
        plan.development_selection_evidence_hash,
    )
    if actual != expected:
        raise ValueError("strategy run plan differs from its frozen Registration")
