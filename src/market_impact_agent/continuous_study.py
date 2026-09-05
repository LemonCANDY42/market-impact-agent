"""Frozen coverage registration for the next market-regime study.

This module deliberately owns only registration and deterministic price-window
selection.  It does not invoke a model, retrieve news, or execute a trade.
"""

from __future__ import annotations

import json
import stat
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import date
from decimal import Decimal, localcontext
from enum import StrEnum
from pathlib import Path
from typing import cast

from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.market_regimes import (
    MarketRegimeCase,
    MarketRegimeDataset,
    RegimePanel,
    RegimeSeries,
    ValidatedRegimePanel,
    validate_regime_panel,
)

CONTINUOUS_STUDY_REGISTRATION_SCHEMA = "market-impact.continuous-study-registration.v1"
CONTINUOUS_STUDY_COVERAGE_MATRIX_SCHEMA = "market-impact.continuous-study-coverage-matrix.v1"

_DATASET_ID = (
    "market-regime-dataset-5c7e9826138ad0cb8f68f0e2d3c48fed2118712ad5b0d3bf94dc5483e801eca7"
)
_PINNED_PANEL_HASHES = (
    "d63c8f98eced67ff86143a82e1db5079460e0b1bf7ecaa8a447176eb20182286",
    "e0817b85d8fc33478a1fdf530d159e2f5173769b83f2639456c9e7d2c2c78c8b",
)
_SELECTION_PANEL_HASH = _PINNED_PANEL_HASHES[1]
_PRIOR_USAGE_AUDIT_HASH = "1d8c43daa5fbc4098f2ae5a53fa6fd2fefcb623a43d680dd670082cff698ebb4"
_PRIMARY_SERIES_ID = "000300.SH"
_ORDINARY_SELECTION_CUTOFF = date(2024, 12, 31)
_ORDINARY_WINDOW_SESSIONS = 60
_FEATURE_SESSIONS = (20, 60)
_THESIS_HORIZONS = (1, 3, 5, 10, 20, 60)
_LEGACY_CASE_KEYS = (
    "cn-2014-2015-leveraged-melt-up",
    "cn-2015-disorder-deleveraging",
    "cn-2016-circuit-breaker-microstress",
    "cn-2016-2018-quality-slow-bull",
    "cn-2018-bear-market",
    "cn-2019-q1-fast-rebound",
    "cn-2020-covid-closure-shock",
    "cn-2020-2021-structural-recovery",
    "cn-2021-index-flat-sector-rotation",
    "cn-2022-multishock-bear",
    "cn-2022-reopening-policy",
    "cn-2023-2024-smallcap-liquidity-stress",
    "cn-2024-broad-rebound",
    "cn-2024-policy-melt-up",
    "cn-2024-post-rally-whipsaw",
)


class WindowKind(StrEnum):
    LEGACY_CASE = "legacy_case"
    ORDINARY = "ordinary_price_selected"


class OrdinaryStratum(StrEnum):
    LOW = "low_volatility"
    MID = "mid_volatility"
    HIGHER_NONEXTREME = "higher_nonextreme_volatility"


@dataclass(frozen=True, slots=True)
class PinnedRegimePanelSet:
    """Two verified immutable panel identities and the semantically current one."""

    panel_bindings: tuple[tuple[str, str], ...]
    selection_panel: ValidatedRegimePanel


@dataclass(frozen=True, slots=True)
class OrdinarySelectionPolicy:
    """Price-only, pre-outcome selection policy for three ordinary windows."""

    selection_cutoff: date = _ORDINARY_SELECTION_CUTOFF
    window_sessions: int = _ORDINARY_WINDOW_SESSIONS
    return_20_abs_max: Decimal = Decimal("0.08000000")
    return_60_abs_max: Decimal = Decimal("0.15000000")
    low_volatility_max: Decimal = Decimal("0.01000000")
    mid_volatility_max: Decimal = Decimal("0.02000000")
    higher_nonextreme_volatility_max: Decimal = Decimal("0.03000000")
    clear_trend_z_min: Decimal = Decimal("0.50000000")

    def __post_init__(self) -> None:
        if self.window_sessions != 60:
            raise ValueError("ordinary selection must retain sixty-session windows")
        if (
            min(
                self.return_20_abs_max,
                self.return_60_abs_max,
                self.low_volatility_max,
                self.mid_volatility_max,
                self.higher_nonextreme_volatility_max,
                self.clear_trend_z_min,
            )
            <= 0
        ):
            raise ValueError("ordinary selection thresholds must be positive")
        if not (
            self.low_volatility_max
            < self.mid_volatility_max
            < self.higher_nonextreme_volatility_max
        ):
            raise ValueError("ordinary volatility thresholds must be strictly increasing")

    def to_dict(self) -> dict[str, object]:
        return {
            "policy": "ordinary-price-window-hash-stratified-v1",
            "selection_cutoff": self.selection_cutoff.isoformat(),
            "window_sessions": self.window_sessions,
            "feature_lag": "through_previous_session",
            "features": {
                "log_return_20_sessions": "ln(close[t-1] / close[t-21])",
                "log_return_60_sessions": "ln(close[t-1] / close[t-61])",
                "realized_volatility_20_sessions": "sqrt(mean(log_return^2, t-20..t-1))",
                "normalized_trend_z_20_sessions": (
                    "log_return_20_sessions / (realized_volatility_20_sessions * sqrt(20))"
                ),
                "normalized_trend_z_60_sessions": (
                    "log_return_60_sessions / (realized_volatility_20_sessions * sqrt(60))"
                ),
            },
            "ordinary_thresholds": {
                "absolute_log_return_20_sessions_max": _decimal_text(self.return_20_abs_max),
                "absolute_log_return_60_sessions_max": _decimal_text(self.return_60_abs_max),
                "clear_same_direction_trend_normalized_z_min": _decimal_text(
                    self.clear_trend_z_min
                ),
                "ordinary_requires": (
                    "20 and 60 session returns are not both same-direction clear trends"
                ),
            },
            "volatility_strata": [
                {
                    "stratum": OrdinaryStratum.LOW.value,
                    "minimum_inclusive": "0.00000000",
                    "maximum_exclusive": _decimal_text(self.low_volatility_max),
                },
                {
                    "stratum": OrdinaryStratum.MID.value,
                    "minimum_inclusive": _decimal_text(self.low_volatility_max),
                    "maximum_exclusive": _decimal_text(self.mid_volatility_max),
                },
                {
                    "stratum": OrdinaryStratum.HIGHER_NONEXTREME.value,
                    "minimum_inclusive": _decimal_text(self.mid_volatility_max),
                    "maximum_exclusive": _decimal_text(self.higher_nonextreme_volatility_max),
                },
            ],
            "selection_order": [item.value for item in OrdinaryStratum],
            "within_stratum_order": "sha256(policy, stratum, decision_session) ascending",
            "exclusions": "overlap_with_fixed_deep_windows_or_previously_selected_ordinary_window",
        }


@dataclass(frozen=True, slots=True)
class ContinuousStudyWindow:
    window_id: str
    kind: WindowKind
    decision_session: date
    observation_through_session: date
    outcome_window_end: date
    source_case_key: str | None
    ordinary_stratum: OrdinaryStratum | None
    selection_key: str | None
    features: tuple[tuple[str, str], ...]

    def __post_init__(self) -> None:
        if not self.window_id or self.window_id != self.window_id.strip():
            raise ValueError("window_id must be non-empty trimmed text")
        if self.observation_through_session >= self.decision_session:
            raise ValueError("observation window must end before its decision session")
        if self.outcome_window_end < self.decision_session:
            raise ValueError("outcome window cannot end before its decision session")
        if self.kind is WindowKind.LEGACY_CASE:
            if self.source_case_key is None or self.ordinary_stratum is not None:
                raise ValueError("legacy windows must bind exactly one legacy case")
            if self.selection_key is not None or self.features:
                raise ValueError("legacy windows do not carry ordinary selection features")
        else:
            if (
                self.source_case_key is not None
                or self.ordinary_stratum is None
                or self.selection_key is None
                or len(self.features) != 5
            ):
                raise ValueError("ordinary windows must carry their frozen stratum and features")
            _sha256(self.selection_key, "ordinary selection key")

    def to_dict(self) -> dict[str, object]:
        return {
            "window_id": self.window_id,
            "kind": self.kind.value,
            "decision_session": self.decision_session.isoformat(),
            "observation_through_session": self.observation_through_session.isoformat(),
            "outcome_window_end": self.outcome_window_end.isoformat(),
            "source_case_key": self.source_case_key,
            "ordinary_stratum": (
                None if self.ordinary_stratum is None else self.ordinary_stratum.value
            ),
            "selection_key": self.selection_key,
            "features": {key: value for key, value in self.features},
        }


@dataclass(frozen=True, slots=True)
class DeepStudyCell:
    cell_id: str
    coverage_window_id: str
    sessions: int | None
    outcome_window_end: date

    def to_dict(self) -> dict[str, object]:
        return {
            "cell_id": self.cell_id,
            "coverage_window_id": self.coverage_window_id,
            "sessions": self.sessions,
            "outcome_window_end": self.outcome_window_end.isoformat(),
        }


@dataclass(frozen=True, slots=True)
class ModelProfileBinding:
    arm: str
    model: str
    reasoning_effort: str
    provider_profile_id: str
    provider_profile_hash: str
    pricing_id: str

    def __post_init__(self) -> None:
        expected = {
            "luna_max": ("gpt-5.6-luna", "max"),
            "terra_high": ("gpt-5.6-terra", "high"),
            "sol_high": ("gpt-5.6-sol", "high"),
        }
        if expected.get(self.arm) != (self.model, self.reasoning_effort):
            raise ValueError("model profile differs from the approved three-model panel")
        _sha256(self.provider_profile_hash, "provider profile hash")
        if self.provider_profile_id != f"model-provider-{self.provider_profile_hash}":
            raise ValueError("provider profile identity does not match its content hash")
        if not self.pricing_id:
            raise ValueError("pricing_id must be non-empty")

    def to_dict(self) -> dict[str, str]:
        return {
            "arm": self.arm,
            "model": self.model,
            "reasoning_effort": self.reasoning_effort,
            "provider_profile_id": self.provider_profile_id,
            "provider_profile_hash": self.provider_profile_hash,
            "pricing_id": self.pricing_id,
        }


@dataclass(frozen=True, slots=True)
class ContinuousStudyBudget:
    route_qualification_microusd: int = 1_000_000
    analysis_coverage_microusd: int = 9_000_000
    portfolio_coverage_microusd: int = 2_500_000
    rolling_microusd: int = 22_000_000
    unseen_and_prospective_microusd: int = 2_500_000
    recovery_microusd: int = 3_000_000

    @property
    def total_microusd(self) -> int:
        return sum(
            (
                self.route_qualification_microusd,
                self.analysis_coverage_microusd,
                self.portfolio_coverage_microusd,
                self.rolling_microusd,
                self.unseen_and_prospective_microusd,
                self.recovery_microusd,
            )
        )

    def __post_init__(self) -> None:
        if self.total_microusd != 40_000_000:
            raise ValueError("continuous study budget must equal USD 40")

    def to_dict(self) -> dict[str, int]:
        return {
            "route_qualification_microusd": self.route_qualification_microusd,
            "analysis_coverage_microusd": self.analysis_coverage_microusd,
            "portfolio_coverage_microusd": self.portfolio_coverage_microusd,
            "rolling_microusd": self.rolling_microusd,
            "unseen_and_prospective_microusd": self.unseen_and_prospective_microusd,
            "recovery_microusd": self.recovery_microusd,
            "total_microusd": self.total_microusd,
        }


@dataclass(frozen=True, slots=True)
class PriorUsageAuditBinding:
    """Immutable reconciliation evidence for completed paid model work."""

    audit_content_hash: str
    route_requests: int
    route_known_microusd: int
    analysis_requests: int
    analysis_known_microusd: int
    analysis_reserved_microusd: int
    portfolio_requests: int
    portfolio_known_microusd: int

    def __post_init__(self) -> None:
        _sha256(self.audit_content_hash, "prior usage audit content hash")
        if (
            self.route_requests,
            self.route_known_microusd,
            self.analysis_requests,
            self.analysis_known_microusd,
            self.analysis_reserved_microusd,
            self.portfolio_requests,
            self.portfolio_known_microusd,
        ) != (9, 85_194, 77, 4_870_788, 11_769, 12, 400_923):
            raise ValueError("prior usage audit totals differ from the verified audit")

    @property
    def request_count(self) -> int:
        return self.route_requests + self.analysis_requests + self.portfolio_requests

    @property
    def known_cost_microusd(self) -> int:
        return (
            self.route_known_microusd + self.analysis_known_microusd + self.portfolio_known_microusd
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "audit_id": "continuous-20260905-prior-budget-audit-v1",
            "audit_content_hash": self.audit_content_hash,
            "status": "bound_audit_required_for_final_budget_accounting",
            "stages": {
                "route": {
                    "requests": self.route_requests,
                    "known_microusd": self.route_known_microusd,
                    "reserved_microusd": 0,
                },
                "analysis": {
                    "requests": self.analysis_requests,
                    "known_microusd": self.analysis_known_microusd,
                    "reserved_microusd": self.analysis_reserved_microusd,
                },
                "portfolio": {
                    "requests": self.portfolio_requests,
                    "known_microusd": self.portfolio_known_microusd,
                    "reserved_microusd": 0,
                },
            },
            "request_count": self.request_count,
            "known_cost_microusd": self.known_cost_microusd,
            "reserved_cost_microusd": self.analysis_reserved_microusd,
        }


@dataclass(frozen=True, slots=True)
class ContinuousStudyRegistration:
    dataset_id: str
    dataset_hash: str
    panel_bindings: tuple[tuple[str, str], ...]
    selection_panel_id: str
    ordinary_selection_policy: OrdinarySelectionPolicy
    coverage_windows: tuple[ContinuousStudyWindow, ...]
    deep_cells: tuple[DeepStudyCell, ...]
    model_profiles: tuple[ModelProfileBinding, ModelProfileBinding, ModelProfileBinding]
    budget: ContinuousStudyBudget
    prior_usage_audit: PriorUsageAuditBinding | None = None

    def __post_init__(self) -> None:
        if self.dataset_id != _DATASET_ID:
            raise ValueError("continuous study binds the frozen 15-case dataset")
        _sha256(self.dataset_hash, "dataset hash")
        expected_bindings = tuple((f"regime-panel-{item}", item) for item in _PINNED_PANEL_HASHES)
        if self.panel_bindings != expected_bindings:
            raise ValueError("continuous study binds the two approved regime panels in fixed order")
        if self.selection_panel_id != f"regime-panel-{_SELECTION_PANEL_HASH}":
            raise ValueError("continuous study must use the corrected pinned selection panel")
        if len(self.coverage_windows) != 18:
            raise ValueError(
                "continuous study coverage denominator must be exactly eighteen windows"
            )
        window_ids = tuple(item.window_id for item in self.coverage_windows)
        if len(window_ids) != len(set(window_ids)):
            raise ValueError("coverage window ids must be unique")
        legacy = tuple(
            item for item in self.coverage_windows if item.kind is WindowKind.LEGACY_CASE
        )
        ordinary = tuple(item for item in self.coverage_windows if item.kind is WindowKind.ORDINARY)
        if tuple(item.source_case_key for item in legacy) != _LEGACY_CASE_KEYS:
            raise ValueError("continuous study must preserve the legacy 15-case coverage order")
        if tuple(item.ordinary_stratum for item in ordinary) != tuple(OrdinaryStratum):
            raise ValueError("continuous study needs one ordinary window in each frozen stratum")
        if len(self.deep_cells) != 8:
            raise ValueError("continuous study deep denominator must be exactly eight cells")
        if tuple((item.cell_id, item.sessions) for item in self.deep_cells) != (
            ("2024_policy_fast_bull_full", None),
            ("2016_slow_bull_120_sessions", 120),
            ("2018_bear_120_sessions", 120),
            ("2015_fast_bear_full", None),
            ("2020_covid_full", None),
            ("2021_rotation_60_sessions", 60),
            ("ordinary_low_60_sessions", 60),
            ("ordinary_higher_nonextreme_60_sessions", 60),
        ):
            raise ValueError("continuous study deep cells differ from the approved fixed panel")
        if any(item.coverage_window_id not in set(window_ids) for item in self.deep_cells):
            raise ValueError("deep cells must reference registered coverage windows")
        if tuple(item.arm for item in self.model_profiles) != (
            "luna_max",
            "terra_high",
            "sol_high",
        ):
            raise ValueError("continuous study profiles must use Luna, Terra, Sol in fixed order")

    @property
    def registration_id(self) -> str:
        return f"continuous-study-registration-{canonical_hash(self.core_dict())}"

    def core_dict(self) -> dict[str, object]:
        return {
            "schema_version": CONTINUOUS_STUDY_REGISTRATION_SCHEMA,
            "dataset_id": self.dataset_id,
            "dataset_hash": self.dataset_hash,
            "panel_bindings": [
                {"panel_id": panel_id, "panel_hash": panel_hash}
                for panel_id, panel_hash in self.panel_bindings
            ],
            "selection_panel_id": self.selection_panel_id,
            "ordinary_selection_policy": self.ordinary_selection_policy.to_dict(),
            "coverage_windows": [item.to_dict() for item in self.coverage_windows],
            "deep_cells": [item.to_dict() for item in self.deep_cells],
            "model_profiles": [item.to_dict() for item in self.model_profiles],
            "cadence": {
                "arms": ["expiry_only", "scheduled", "event"],
                "planned_observation_denominator": 72,
                "denominator_formula": "8 deep cells x 3 model profiles x 3 cadence arms",
                "gap_policy": "retain every planned observation; record missing or blocked inputs",
            },
            "time_contract": {
                "version": "preopen_t0_h1_next_preopen_expiry_v1",
                "timezone": "Asia/Shanghai",
                "observation_window": (
                    "price-only features and any model-visible evidence end no later than the "
                    "session immediately preceding decision_session"
                ),
                "decision_time": "before decision_session market open unless an existing "
                "event anchor is independently evidenced before that open",
                "thesis_horizons_sessions": list(_THESIS_HORIZONS),
                "label_window": (
                    "a thesis at horizon h is evaluated from decision_session open through the "
                    "h-th executable session close; labels remain evaluator-only"
                ),
                "outcomes_visible_to_models": False,
            },
            "baseline_input_inventory": baseline_input_inventory(self),
            "information_coverage_gaps": [
                {
                    "gap_id": "bounded-news-audit-pending",
                    "coverage_type": "information_coverage",
                    "status": "pending_bounded_news_audit",
                    "applies_to": "all_18_coverage_windows",
                    "reason": (
                        "Price paths cannot establish that no material headline was available; "
                        "a bounded news audit must determine coverage."
                    ),
                    "claim_permitted": False,
                }
            ],
            "prior_usage_reconciliation": (
                self.prior_usage_audit.to_dict()
                if self.prior_usage_audit is not None
                else {
                    "audit_id": "continuous-20260905-prior-budget-audit-v1",
                    "status": "pending_immutable_audit_binding",
                }
            ),
            "budget": self.budget.to_dict(),
            "outcomes_visible_to_models": False,
            "model_or_network_invocation": False,
            "broker_access": False,
        }

    def to_dict(self) -> dict[str, object]:
        return {**self.core_dict(), "registration_id": self.registration_id}


def load_pinned_regime_panels(regime_root: Path) -> PinnedRegimePanelSet:
    """Validate the two immutable public-to-this-workspace panel manifests."""

    legacy_panel = _validate_pre_taxonomy_pinned_manifest(
        regime_root / f"regime-panel-{_PINNED_PANEL_HASHES[0]}",
        _PINNED_PANEL_HASHES[0],
    )
    selection_panel = validate_regime_panel(regime_root / f"regime-panel-{_PINNED_PANEL_HASHES[1]}")
    if selection_panel.panel_hash != _SELECTION_PANEL_HASH:
        raise ValueError("pinned selection panel identity differs from registration")
    if legacy_panel[2] != selection_panel.panel.dataset_id or (
        legacy_panel[3] != selection_panel.panel.dataset_hash
    ):
        raise ValueError("pinned panels do not bind the same market regime dataset")
    if legacy_panel[4] != _primary_series(selection_panel.panel).rows:
        raise ValueError("pinned panels disagree on the primary selection series")
    return PinnedRegimePanelSet(
        panel_bindings=(
            (legacy_panel[0], legacy_panel[1]),
            (selection_panel.panel_id, selection_panel.panel_hash),
        ),
        selection_panel=selection_panel,
    )


def load_prior_usage_audit_binding(path: Path) -> PriorUsageAuditBinding:
    """Bind the exact settled-and-reserved prior-usage audit to a registration.

    The registration stores the canonical content hash, not the source path, so
    a relocated audit can be verified while any substantive edit changes the
    registered identity.
    """

    with path.open(encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise ValueError("prior usage audit has an unexpected top-level shape")
    raw_audit = cast(dict[object, object], payload)
    if set(raw_audit) != {"requests", "settlements", "stages"}:
        raise ValueError("prior usage audit has an unexpected top-level shape")
    audit = cast(dict[str, object], raw_audit)
    if not isinstance(audit["requests"], dict) or not isinstance(audit["settlements"], dict):
        raise ValueError("prior usage audit must retain request and settlement ledgers")
    stages = audit["stages"]
    if not isinstance(stages, dict):
        raise ValueError("prior usage audit must retain the three verified stages")
    raw_stages = cast(dict[object, object], stages)
    if set(raw_stages) != {"route", "analysis", "portfolio"}:
        raise ValueError("prior usage audit must retain the three verified stages")
    stage_map = cast(dict[str, object], raw_stages)
    route = _audit_stage(stage_map, "route")
    analysis = _audit_stage(stage_map, "analysis")
    portfolio = _audit_stage(stage_map, "portfolio")
    audit_hash = canonical_hash(audit)
    if audit_hash != _PRIOR_USAGE_AUDIT_HASH:
        raise ValueError("prior usage audit content differs from the verified reconciliation")
    return PriorUsageAuditBinding(
        audit_content_hash=audit_hash,
        route_requests=_audit_int(route, "requests"),
        route_known_microusd=_audit_int(route, "known"),
        analysis_requests=_audit_int(analysis, "requests"),
        analysis_known_microusd=_audit_int(analysis, "known"),
        analysis_reserved_microusd=_audit_int(analysis, "reserved"),
        portfolio_requests=_audit_int(portfolio, "requests"),
        portfolio_known_microusd=_audit_int(portfolio, "known"),
    )


def build_continuous_study_registration(
    dataset: MarketRegimeDataset,
    pinned_panels: PinnedRegimePanelSet,
    *,
    prior_usage_audit: PriorUsageAuditBinding | None = None,
) -> ContinuousStudyRegistration:
    """Build the content-addressed 18-window/8-cell registration from pinned inputs."""

    if (
        dataset.dataset_id != _DATASET_ID
        or dataset.dataset_hash != pinned_panels.selection_panel.panel.dataset_hash
    ):
        raise ValueError("dataset and pinned panels do not match the approved research input")
    if tuple(item.case_key for item in dataset.cases) != _LEGACY_CASE_KEYS:
        raise ValueError("the legacy dataset case order changed")
    policy = OrdinarySelectionPolicy()
    selection_panel = pinned_panels.selection_panel
    legacy_windows = _legacy_windows(dataset.cases, selection_panel.panel)
    fixed_deep_windows = _fixed_deep_windows(legacy_windows, selection_panel.panel)
    ordinary_windows = select_ordinary_windows(
        selection_panel.panel,
        fixed_deep_windows=fixed_deep_windows,
        policy=policy,
    )
    coverage_windows = legacy_windows + ordinary_windows
    by_id = {item.window_id: item for item in coverage_windows}
    fixed_by_id = {item.window_id: item for item in fixed_deep_windows}
    ordinary_by_stratum = {item.ordinary_stratum: item for item in ordinary_windows}
    deep_cells = (
        DeepStudyCell(
            "2024_policy_fast_bull_full",
            "cn-2024-policy-melt-up",
            None,
            fixed_by_id["cn-2024-policy-melt-up"].outcome_window_end,
        ),
        DeepStudyCell(
            "2016_slow_bull_120_sessions",
            "cn-2016-2018-quality-slow-bull",
            120,
            fixed_by_id["cn-2016-2018-quality-slow-bull"].outcome_window_end,
        ),
        DeepStudyCell(
            "2018_bear_120_sessions",
            "cn-2018-bear-market",
            120,
            fixed_by_id["cn-2018-bear-market"].outcome_window_end,
        ),
        DeepStudyCell(
            "2015_fast_bear_full",
            "cn-2015-disorder-deleveraging",
            None,
            fixed_by_id["cn-2015-disorder-deleveraging"].outcome_window_end,
        ),
        DeepStudyCell(
            "2020_covid_full",
            "cn-2020-covid-closure-shock",
            None,
            fixed_by_id["cn-2020-covid-closure-shock"].outcome_window_end,
        ),
        DeepStudyCell(
            "2021_rotation_60_sessions",
            "cn-2021-index-flat-sector-rotation",
            60,
            fixed_by_id["cn-2021-index-flat-sector-rotation"].outcome_window_end,
        ),
        DeepStudyCell(
            "ordinary_low_60_sessions",
            "ordinary-low-volatility",
            60,
            ordinary_by_stratum[OrdinaryStratum.LOW].outcome_window_end,
        ),
        DeepStudyCell(
            "ordinary_higher_nonextreme_60_sessions",
            "ordinary-higher-nonextreme-volatility",
            60,
            ordinary_by_stratum[OrdinaryStratum.HIGHER_NONEXTREME].outcome_window_end,
        ),
    )
    _validate_deep_cells_against_windows(deep_cells, by_id, selection_panel.panel)
    return ContinuousStudyRegistration(
        dataset_id=dataset.dataset_id,
        dataset_hash=dataset.dataset_hash,
        panel_bindings=pinned_panels.panel_bindings,
        selection_panel_id=selection_panel.panel_id,
        ordinary_selection_policy=policy,
        coverage_windows=coverage_windows,
        deep_cells=deep_cells,
        model_profiles=_model_profiles(),
        budget=ContinuousStudyBudget(),
        prior_usage_audit=prior_usage_audit,
    )


def select_ordinary_windows(
    panel: RegimePanel,
    *,
    fixed_deep_windows: Sequence[ContinuousStudyWindow],
    policy: OrdinarySelectionPolicy | None = None,
) -> tuple[ContinuousStudyWindow, ContinuousStudyWindow, ContinuousStudyWindow]:
    """Choose one hash-ordered eligible price window per volatility stratum.

    Candidate features are calculated only through the session before the candidate
    decision session.  The selection cutoff is fixed independently of panel rows
    that may be appended later.
    """

    policy = policy or OrdinarySelectionPolicy()
    dates, closes = _primary_dates_and_closes(panel)
    candidates: dict[OrdinaryStratum, list[ContinuousStudyWindow]] = {
        item: [] for item in OrdinaryStratum
    }
    for decision_index in range(max(_FEATURE_SESSIONS) + 1, len(dates)):
        decision_session = dates[decision_index]
        outcome_end_index = decision_index + policy.window_sessions - 1
        if decision_session > policy.selection_cutoff or outcome_end_index >= len(dates):
            continue
        if dates[outcome_end_index] > policy.selection_cutoff:
            continue
        features = _ordinary_features(closes, decision_index)
        stratum = _ordinary_stratum(features, policy)
        if stratum is None:
            continue
        selection_key = canonical_hash(
            {
                "policy": "ordinary-price-window-hash-stratified-v1",
                "stratum": stratum.value,
                "decision_session": decision_session.isoformat(),
            }
        )
        candidates[stratum].append(
            ContinuousStudyWindow(
                window_id=f"ordinary-{stratum.value.replace('_', '-')}",
                kind=WindowKind.ORDINARY,
                decision_session=decision_session,
                observation_through_session=dates[decision_index - 1],
                outcome_window_end=dates[outcome_end_index],
                source_case_key=None,
                ordinary_stratum=stratum,
                selection_key=selection_key,
                features=tuple((name, _decimal_text(value)) for name, value in features.items()),
            )
        )
    selected: list[ContinuousStudyWindow] = []
    exclusions = tuple(fixed_deep_windows)
    for stratum in OrdinaryStratum:
        match = next(
            (
                candidate
                for candidate in sorted(
                    candidates[stratum],
                    key=lambda item: (cast(str, item.selection_key), item.decision_session),
                )
                if not any(_overlaps(candidate, existing) for existing in (*exclusions, *selected))
            ),
            None,
        )
        if match is None:
            raise ValueError(f"no eligible ordinary window for {stratum.value}")
        selected.append(match)
    return cast(
        tuple[ContinuousStudyWindow, ContinuousStudyWindow, ContinuousStudyWindow], tuple(selected)
    )


def ordinary_candidate_features(
    panel: RegimePanel,
    decision_session: date,
) -> dict[str, str]:
    """Return the price-only features available before one decision session.

    This narrow inspection surface makes the no-future-price boundary testable
    without claiming that perturbing later prices cannot affect other candidate
    sessions in an offline selection universe.
    """

    dates, closes = _primary_dates_and_closes(panel)
    decision_index = _date_index(dates, decision_session)
    return {
        name: _decimal_text(value)
        for name, value in _ordinary_features(closes, decision_index).items()
    }


def coverage_report(registration: ContinuousStudyRegistration) -> dict[str, object]:
    """Return the fixed denominator, overlap evidence, and typed input gaps."""

    ordinary = tuple(
        item for item in registration.coverage_windows if item.kind is WindowKind.ORDINARY
    )
    deep_by_window = {item.coverage_window_id: item for item in registration.deep_cells}
    fixed_deep = tuple(
        replace(window, outcome_window_end=deep_by_window[window.window_id].outcome_window_end)
        for window in registration.coverage_windows
        if window.window_id in deep_by_window and window.kind is WindowKind.LEGACY_CASE
    )
    ordinary_overlap = [
        {
            "ordinary_window_id": ordinary_window.window_id,
            "overlaps_fixed_deep_window_ids": [
                fixed.window_id for fixed in fixed_deep if _overlaps(ordinary_window, fixed)
            ],
            "overlaps_other_ordinary_window_ids": [
                other.window_id
                for other in ordinary
                if other.window_id != ordinary_window.window_id
                and _overlaps(ordinary_window, other)
            ],
        }
        for ordinary_window in ordinary
    ]
    return {
        "registration_id": registration.registration_id,
        "coverage_denominator": len(registration.coverage_windows),
        "legacy_coverage_denominator": 15,
        "ordinary_coverage_denominator": len(ordinary),
        "deep_denominator": len(registration.deep_cells),
        "planned_model_cadence_observation_denominator": 72,
        "ordinary_overlap": ordinary_overlap,
        "baseline_input_inventory": baseline_input_inventory(registration),
        "information_coverage_gaps": registration.core_dict()["information_coverage_gaps"],
    }


def build_continuous_study_coverage_matrix(
    registration: ContinuousStudyRegistration,
    dataset: MarketRegimeDataset,
) -> dict[str, object]:
    """Build an evaluator-only, descriptor-preserving coverage matrix.

    The matrix is deliberately separate from the frozen registration and its
    original coverage artifact.  It reorganizes registered case descriptors and
    pre-cutoff ordinary-window features for inspection only; it does not derive
    labels or make unavailable liquidity, dispersion, transition, or news data
    appear covered.
    """

    if (
        dataset.dataset_id != registration.dataset_id
        or dataset.dataset_hash != registration.dataset_hash
    ):
        raise ValueError("coverage matrix dataset does not match the frozen registration")
    cases_by_key = {item.case_key: item for item in dataset.cases}
    if tuple(cases_by_key) != _LEGACY_CASE_KEYS:
        raise ValueError("coverage matrix dataset case order differs from the frozen study")

    rows = [_coverage_matrix_row(window, cases_by_key) for window in registration.coverage_windows]
    if len(rows) != 18:
        raise ValueError("coverage matrix must retain all eighteen registered windows")
    by_window_id = {item.window_id: item for item in registration.coverage_windows}
    deep_selection = [
        {
            "deep_cell": cell.to_dict(),
            "coverage_row_id": cell.coverage_window_id,
            "selection_window": {
                "decision_session": by_window_id[
                    cell.coverage_window_id
                ].decision_session.isoformat(),
                "outcome_window_end": cell.outcome_window_end.isoformat(),
            },
        }
        for cell in registration.deep_cells
    ]
    if len(deep_selection) != 8:
        raise ValueError("coverage matrix must retain all eight registered deep cells")
    overlaps = [
        {
            "window_id": window.window_id,
            "overlaps_coverage_window_ids": [
                other.window_id
                for other in registration.coverage_windows
                if other.window_id != window.window_id and _overlaps(window, other)
            ],
        }
        for window in registration.coverage_windows
    ]
    core: dict[str, object] = {
        "schema_version": CONTINUOUS_STUDY_COVERAGE_MATRIX_SCHEMA,
        "registration_id": registration.registration_id,
        "registration_content_hash": canonical_hash(registration.to_dict()),
        "dataset_id": dataset.dataset_id,
        "dataset_hash": dataset.dataset_hash,
        "coverage_denominator": len(rows),
        "dimension_order": [
            "direction_speed",
            "volatility_liquidity",
            "cross_section_differences",
            "transitions",
            "information_shape",
        ],
        "rows": rows,
        "deep_selection": deep_selection,
        "overlaps": overlaps,
        "ordinary_overlap": coverage_report(registration)["ordinary_overlap"],
        "dimension_gaps": _coverage_matrix_dimension_gaps(registration),
        "evaluator_only": True,
        "labels_access": "evaluation_only",
        "labels_are_model_inputs": False,
        "model_or_network_invocation": False,
        "broker_access": False,
    }
    return {
        **core,
        "coverage_matrix_id": f"continuous-study-coverage-matrix-{canonical_hash(core)}",
    }


def _coverage_matrix_row(
    window: ContinuousStudyWindow,
    cases_by_key: Mapping[str, MarketRegimeCase],
) -> dict[str, object]:
    if window.kind is WindowKind.ORDINARY:
        features = {key: value for key, value in window.features}
        return {
            "row_id": window.window_id,
            "window": window.to_dict(),
            "support": {
                "ordinary_selection": {
                    "ordinary_stratum": window.ordinary_stratum.value
                    if window.ordinary_stratum is not None
                    else None,
                    "selection_key": window.selection_key,
                    "pre_cutoff_features": features,
                    "feature_lag": "through_previous_session",
                }
            },
            "dimensions": _ordinary_matrix_dimensions(features, window),
        }

    case_key = window.source_case_key
    if case_key is None or case_key not in cases_by_key:
        raise ValueError("legacy coverage matrix row has no registered source case")
    case = cases_by_key[case_key]
    return {
        "row_id": window.window_id,
        "window": window.to_dict(),
        "support": {
            "registered_case": {
                "axes": dict(case.axes),
                "capability_targets": list(case.capability_targets),
                "primary_market_index": case.primary_market_index,
                "required_market_indices": list(case.required_market_indices),
                "required_industry_proxies": list(case.required_industry_proxies),
                "source_refs": list(case.source_refs),
                "event_anchor": _event_anchor_descriptor(case),
            }
        },
        "dimensions": _legacy_matrix_dimensions(case),
    }


def _legacy_matrix_dimensions(case: MarketRegimeCase) -> dict[str, object]:
    axes = case.axes
    capabilities = list(case.capability_targets)
    return {
        "direction_speed": {
            "registered_axes": _axis_subset(axes, "path_direction", "path_speed"),
        },
        "volatility_liquidity": {
            "registered_axes": _axis_subset(axes, "volatility", "drawdown"),
            "capability_targets": capabilities,
            "liquidity": _unknown_matrix_value(
                "No registered liquidity measure is available for this coverage window."
            ),
        },
        "cross_section_differences": {
            "required_market_indices": list(case.required_market_indices),
            "required_industry_proxies": list(case.required_industry_proxies),
            "capability_targets": capabilities,
            "dispersion": _unknown_matrix_value(
                "Registered series and capability targets do not establish realized dispersion."
            ),
        },
        "transitions": {
            "registered_axes": _axis_subset(axes, "recovery"),
            "capability_targets": capabilities,
            "transition_state": _unknown_matrix_value(
                "Registered path descriptors do not establish a transition state at decision time."
            ),
        },
        "information_shape": {
            "registered_axes": _axis_subset(
                axes,
                "narrative_salience",
                "causal_complexity",
                "causal_directness",
            ),
            "event_anchor": _event_anchor_descriptor(case),
            "source_refs": list(case.source_refs),
            "news": _unknown_matrix_value(
                "Registered source references do not prove bounded news coverage or absence."
            ),
        },
    }


def _ordinary_matrix_dimensions(
    features: Mapping[str, str],
    window: ContinuousStudyWindow,
) -> dict[str, object]:
    raw_selection = {
        "ordinary_stratum": window.ordinary_stratum.value
        if window.ordinary_stratum is not None
        else None,
        "pre_cutoff_features": dict(features),
    }
    return {
        "direction_speed": {
            "pre_cutoff_features": dict(features),
            "path_direction_and_speed": _unknown_matrix_value(
                "Ordinary price-selection features are preserved without deriving direction "
                "or speed labels."
            ),
        },
        "volatility_liquidity": {
            "ordinary_selection": raw_selection,
            "liquidity": _unknown_matrix_value(
                "The ordinary selection uses only the primary index price series, not "
                "liquidity data."
            ),
        },
        "cross_section_differences": {
            "selection_series_id": _PRIMARY_SERIES_ID,
            "dispersion": _unknown_matrix_value(
                "The ordinary selection has no registered cross-sectional dispersion input."
            ),
        },
        "transitions": {
            "pre_cutoff_features": dict(features),
            "transition_state": _unknown_matrix_value(
                "Ordinary price-selection features are not converted into a transition label."
            ),
        },
        "information_shape": {
            "price_only_selection": raw_selection,
            "news": _unknown_matrix_value(
                "Price-only ordinary selection does not establish news coverage or absence."
            ),
        },
    }


def _coverage_matrix_dimension_gaps(
    registration: ContinuousStudyRegistration,
) -> list[dict[str, object]]:
    window_ids = [item.window_id for item in registration.coverage_windows]
    ordinary_ids = [
        item.window_id for item in registration.coverage_windows if item.kind is WindowKind.ORDINARY
    ]
    return [
        {
            "dimension": "direction_speed",
            "field": "path_direction_and_path_speed",
            "status": "unknown_not_inferred",
            "applies_to_window_ids": ordinary_ids,
            "reason": (
                "Ordinary pre-cutoff features are retained raw and are not converted to labels."
            ),
        },
        {
            "dimension": "volatility_liquidity",
            "field": "liquidity",
            "status": "unknown_not_inferred",
            "applies_to_window_ids": window_ids,
            "reason": "No registered liquidity measure supports a liquidity claim.",
        },
        {
            "dimension": "cross_section_differences",
            "field": "dispersion",
            "status": "unknown_not_inferred",
            "applies_to_window_ids": window_ids,
            "reason": "Registered proxies and targets do not establish realized dispersion.",
        },
        {
            "dimension": "transitions",
            "field": "transition_state",
            "status": "unknown_not_inferred",
            "applies_to_window_ids": window_ids,
            "reason": (
                "Path descriptors and ordinary features do not establish decision-time state "
                "transitions."
            ),
        },
        {
            "dimension": "information_shape",
            "field": "news",
            "status": "unknown_not_inferred",
            "applies_to_window_ids": window_ids,
            "reason": (
                "Price data and registered source references do not prove news coverage or absence."
            ),
        },
    ]


def _axis_subset(axes: Mapping[str, str], *keys: str) -> dict[str, str]:
    return {key: axes[key] for key in keys}


def _unknown_matrix_value(reason: str) -> dict[str, object]:
    return {"value": None, "status": "unknown_not_inferred", "reason": reason}


def _event_anchor_descriptor(case: MarketRegimeCase) -> dict[str, object] | None:
    anchor = case.event_anchor
    if anchor is None:
        return None
    return {
        "observed_at": anchor.observed_at.isoformat(),
        "anchor_session": anchor.anchor_session.isoformat(),
        "price_anchor": anchor.price_anchor,
        "executable": anchor.executable,
    }


def baseline_input_inventory(registration: ContinuousStudyRegistration) -> dict[str, object]:
    """List required baseline inputs without treating a price panel as news coverage."""

    return {
        "market_price_panels": [
            {"panel_id": panel_id, "panel_hash": panel_hash}
            for panel_id, panel_hash in registration.panel_bindings
        ],
        "primary_selection_series": _PRIMARY_SERIES_ID,
        "industry_price_basis": "SW2021 Level-1 index price proxies",
        "baseline_contract": "existing regime-study baselines; descriptive and non-executable",
        "required_information_audit": {
            "status": "pending_bounded_news_audit",
            "reason": "price data cannot prove no-headline coverage or absence",
        },
    }


def _legacy_windows(
    cases: Sequence[MarketRegimeCase], panel: RegimePanel
) -> tuple[ContinuousStudyWindow, ...]:
    dates, _ = _primary_dates_and_closes(panel)
    result: list[ContinuousStudyWindow] = []
    for case in cases:
        start_index = _date_index(dates, case.tradable_start)
        result.append(
            ContinuousStudyWindow(
                window_id=case.case_key,
                kind=WindowKind.LEGACY_CASE,
                decision_session=case.tradable_start,
                observation_through_session=dates[start_index - 1],
                outcome_window_end=case.end,
                source_case_key=case.case_key,
                ordinary_stratum=None,
                selection_key=None,
                features=(),
            )
        )
    return tuple(result)


def _fixed_deep_windows(
    legacy_windows: Sequence[ContinuousStudyWindow], panel: RegimePanel
) -> tuple[ContinuousStudyWindow, ...]:
    by_id = {item.window_id: item for item in legacy_windows}
    dates, _ = _primary_dates_and_closes(panel)
    specifications = (
        ("cn-2024-policy-melt-up", None),
        ("cn-2016-2018-quality-slow-bull", 120),
        ("cn-2018-bear-market", 120),
        ("cn-2015-disorder-deleveraging", None),
        ("cn-2020-covid-closure-shock", None),
        ("cn-2021-index-flat-sector-rotation", 60),
    )
    result: list[ContinuousStudyWindow] = []
    for window_id, sessions in specifications:
        source = by_id[window_id]
        end = source.outcome_window_end
        if sessions is not None:
            start_index = _date_index(dates, source.decision_session)
            end = dates[start_index + sessions - 1]
        result.append(
            ContinuousStudyWindow(
                window_id=source.window_id,
                kind=source.kind,
                decision_session=source.decision_session,
                observation_through_session=source.observation_through_session,
                outcome_window_end=end,
                source_case_key=source.source_case_key,
                ordinary_stratum=None,
                selection_key=None,
                features=(),
            )
        )
    return tuple(result)


def _validate_deep_cells_against_windows(
    deep_cells: Sequence[DeepStudyCell],
    by_id: Mapping[str, ContinuousStudyWindow],
    panel: RegimePanel,
) -> None:
    dates, _ = _primary_dates_and_closes(panel)
    for cell in deep_cells:
        window = by_id[cell.coverage_window_id]
        if cell.outcome_window_end < window.decision_session:
            raise ValueError("deep cell outcome window ends before its decision session")
        if cell.sessions is None:
            if cell.outcome_window_end != window.outcome_window_end:
                raise ValueError("full deep cell must retain its complete coverage window")
            continue
        actual = (
            _date_index(dates, cell.outcome_window_end)
            - _date_index(dates, window.decision_session)
            + 1
        )
        if actual != cell.sessions:
            raise ValueError("deep cell session count differs from its registered window")


def _validate_pre_taxonomy_pinned_manifest(
    panel_directory: Path,
    expected_hash: str,
) -> tuple[str, str, str, str, tuple[dict[str, object], ...]]:
    """Validate the old content-addressed manifest without rewriting its old shape.

    The d63 panel predates the later ``industry_taxonomy`` requirement.  Its
    hash and restrictive filesystem permissions still authenticate its exact
    recorded bytes; asking the current semantic loader to parse it would
    incorrectly make a preserved historical input disappear.
    """

    expected_id = f"regime-panel-{expected_hash}"
    if (
        panel_directory.name != expected_id
        or panel_directory.is_symlink()
        or not panel_directory.is_dir()
        or stat.S_IMODE(panel_directory.stat().st_mode) != 0o700
    ):
        raise ValueError("legacy pinned panel directory is not a real 0700 manifest directory")
    manifest = panel_directory / "manifest.json"
    if (
        manifest.is_symlink()
        or not manifest.is_file()
        or stat.S_IMODE(manifest.stat().st_mode) != 0o600
    ):
        raise ValueError("legacy pinned panel manifest is not a real 0600 file")
    payload = json.loads(manifest.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("legacy pinned panel manifest must be an object")
    body = cast(dict[str, object], payload)
    expected_keys = {
        "schema_version",
        "dataset_id",
        "dataset_hash",
        "provider_id",
        "provider_version",
        "historical_vintage",
        "retrieved_at",
        "series",
        "proxy_resolution",
        "panel_hash",
        "panel_id",
    }
    if set(body) != expected_keys:
        raise ValueError("legacy pinned panel manifest shape changed")
    panel_hash = _string(body, "panel_hash")
    panel_id = _string(body, "panel_id")
    core = {key: value for key, value in body.items() if key not in {"panel_hash", "panel_id"}}
    if (
        panel_hash != expected_hash
        or panel_id != expected_id
        or canonical_hash(core) != expected_hash
        or _string(body, "schema_version") != "market-impact.regime-panel.v1"
    ):
        raise ValueError("legacy pinned panel identity does not match its manifest content")
    if (
        _string(body, "provider_id") != "tushare-http"
        or _string(body, "provider_version") != "0.1.0"
    ):
        raise ValueError("legacy pinned panel provider identity changed")
    series_value = body.get("series")
    if not isinstance(series_value, list):
        raise ValueError("legacy pinned panel has no series")
    series_raw = cast(list[object], series_value)
    primary: dict[str, object] | None = None
    for item in series_raw:
        if not isinstance(item, dict):
            continue
        series = cast(dict[str, object], item)
        if series.get("series_id") == _PRIMARY_SERIES_ID:
            primary = series
            break
    if primary is None:
        raise ValueError("legacy pinned panel has no primary selection series")
    rows_value = primary.get("rows")
    if not isinstance(rows_value, list):
        raise ValueError("legacy pinned panel has no primary selection series")
    rows_raw = cast(list[object], rows_value)
    rows = tuple(cast(dict[str, object], item) for item in rows_raw if isinstance(item, dict))
    if len(rows) != len(rows_raw):
        raise ValueError("legacy primary selection rows must be objects")
    return (
        panel_id,
        panel_hash,
        _string(body, "dataset_id"),
        _string(body, "dataset_hash"),
        rows,
    )


def _model_profiles() -> tuple[ModelProfileBinding, ModelProfileBinding, ModelProfileBinding]:
    return (
        ModelProfileBinding(
            arm="luna_max",
            model="gpt-5.6-luna",
            reasoning_effort="max",
            provider_profile_id=(
                "model-provider-dddfa35322a03a3bfe92f1186a1ec04fc77075d9f6038de16ade304f85f8add0"
            ),
            provider_profile_hash=(
                "dddfa35322a03a3bfe92f1186a1ec04fc77075d9f6038de16ade304f85f8add0"
            ),
            pricing_id="cpa-usage-keeper-gpt-5.6-luna-v1.14.5-2026-08-27",
        ),
        ModelProfileBinding(
            arm="terra_high",
            model="gpt-5.6-terra",
            reasoning_effort="high",
            provider_profile_id=(
                "model-provider-7d3c04afa0b04a1a6466da7918d585c7e7df630857016b99504b35a973d34034"
            ),
            provider_profile_hash=(
                "7d3c04afa0b04a1a6466da7918d585c7e7df630857016b99504b35a973d34034"
            ),
            pricing_id="cpa-usage-keeper-gpt-5.6-terra-v1.14.5-2026-09-04",
        ),
        ModelProfileBinding(
            arm="sol_high",
            model="gpt-5.6-sol",
            reasoning_effort="high",
            provider_profile_id=(
                "model-provider-c963b814206d3fcbd9e596ac5486a06c5300c1c7d9157dcc6f816ec1b056d129"
            ),
            provider_profile_hash=(
                "c963b814206d3fcbd9e596ac5486a06c5300c1c7d9157dcc6f816ec1b056d129"
            ),
            pricing_id="cpa-usage-keeper-gpt-5.6-sol-v1.14.5-2026-09-04",
        ),
    )


def _primary_dates_and_closes(panel: RegimePanel) -> tuple[tuple[date, ...], tuple[Decimal, ...]]:
    primary = _primary_series(panel)
    dates = tuple(_row_date(item) for item in primary.rows)
    closes = tuple(Decimal(str(item["close"])) for item in primary.rows)
    if any(value <= 0 for value in closes):
        raise ValueError("primary selection series contains a nonpositive close")
    return dates, closes


def _primary_series(panel: RegimePanel) -> RegimeSeries:
    match = next((item for item in panel.series if item.series_id == _PRIMARY_SERIES_ID), None)
    if match is None:
        raise ValueError("pinned panel has no CSI 300 primary selection series")
    return match


def _ordinary_features(closes: Sequence[Decimal], decision_index: int) -> dict[str, Decimal]:
    if decision_index < max(_FEATURE_SESSIONS) + 1:
        raise ValueError("ordinary candidate does not have strictly preceding 60-session features")
    with localcontext() as context:
        context.prec = 50
        return_20 = (closes[decision_index - 1] / closes[decision_index - 21]).ln()
        return_60 = (closes[decision_index - 1] / closes[decision_index - 61]).ln()
        daily_returns = tuple(
            (closes[index] / closes[index - 1]).ln()
            for index in range(decision_index - 20, decision_index)
        )
        volatility = (
            sum((value * value for value in daily_returns), Decimal(0)) / Decimal(20)
        ).sqrt()
        normalized_trend_20 = return_20 / (volatility * Decimal(20).sqrt())
        normalized_trend_60 = return_60 / (volatility * Decimal(60).sqrt())
    return {
        "log_return_20_sessions": return_20,
        "log_return_60_sessions": return_60,
        "realized_volatility_20_sessions": volatility,
        "normalized_trend_z_20_sessions": normalized_trend_20,
        "normalized_trend_z_60_sessions": normalized_trend_60,
    }


def _ordinary_stratum(
    features: Mapping[str, Decimal], policy: OrdinarySelectionPolicy
) -> OrdinaryStratum | None:
    if (
        abs(features["log_return_20_sessions"]) > policy.return_20_abs_max
        or abs(features["log_return_60_sessions"]) > policy.return_60_abs_max
    ):
        return None
    same_direction = (
        features["log_return_20_sessions"] > 0 and features["log_return_60_sessions"] > 0
    ) or (features["log_return_20_sessions"] < 0 and features["log_return_60_sessions"] < 0)
    if same_direction and (
        abs(features["normalized_trend_z_20_sessions"]) >= policy.clear_trend_z_min
        and abs(features["normalized_trend_z_60_sessions"]) >= policy.clear_trend_z_min
    ):
        return None
    volatility = features["realized_volatility_20_sessions"]
    if volatility < policy.low_volatility_max:
        return OrdinaryStratum.LOW
    if volatility < policy.mid_volatility_max:
        return OrdinaryStratum.MID
    if volatility < policy.higher_nonextreme_volatility_max:
        return OrdinaryStratum.HIGHER_NONEXTREME
    return None


def _overlaps(left: ContinuousStudyWindow, right: ContinuousStudyWindow) -> bool:
    return not (
        left.outcome_window_end < right.decision_session
        or right.outcome_window_end < left.decision_session
    )


def _date_index(dates: Sequence[date], target: date) -> int:
    try:
        return dates.index(target)
    except ValueError as exc:
        raise ValueError(
            f"primary series is missing required session {target.isoformat()}"
        ) from exc


def _row_date(row: Mapping[str, object]) -> date:
    raw = row.get("trade_date")
    if not isinstance(raw, str):
        raise ValueError("primary series row has no trade_date")
    if len(raw) == 8 and raw.isdigit():
        return date(int(raw[:4]), int(raw[4:6]), int(raw[6:]))
    return date.fromisoformat(raw)


def _decimal_text(value: Decimal) -> str:
    return format(value.quantize(Decimal("0.00000001")), "f")


def _audit_stage(audit_stages: Mapping[str, object], stage_name: str) -> Mapping[str, object]:
    stage = audit_stages.get(stage_name)
    if not isinstance(stage, dict):
        raise ValueError(f"prior usage audit {stage_name} stage has an unexpected shape")
    raw_stage = cast(dict[object, object], stage)
    if set(raw_stage) != {"requests", "known", "reserved"}:
        raise ValueError(f"prior usage audit {stage_name} stage has an unexpected shape")
    return cast(Mapping[str, object], raw_stage)


def _audit_int(stage: Mapping[str, object], key: str) -> int:
    value = stage.get(key)
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"prior usage audit {key} must be a non-negative integer")
    return value


def _string(value: Mapping[str, object], key: str) -> str:
    item = value.get(key)
    if not isinstance(item, str) or not item:
        raise ValueError(f"{key} must be a non-empty string")
    return item


def _sha256(value: str, name: str) -> None:
    if len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
        raise ValueError(f"{name} must be a SHA-256 digest")
