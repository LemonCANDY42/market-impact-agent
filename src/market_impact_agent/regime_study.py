from __future__ import annotations

import json
import math
import os
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date, time
from decimal import ROUND_HALF_EVEN, Decimal
from pathlib import Path
from typing import Protocol, cast
from zoneinfo import ZoneInfo

from jsonschema import Draft202012Validator, FormatChecker, ValidationError

from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.market_regimes import (
    MarketRegimeCase,
    MarketRegimeDataset,
    RegimePanel,
    RegimeSeries,
    ValidatedRegimePanel,
)
from market_impact_agent.method_skills import MethodSkillCatalog

REGIME_STUDY_REGISTRATION_SCHEMA = "market-impact.regime-study-registration.v1"
REGIME_STUDY_REPORT_SCHEMA = "market-impact.regime-study-baseline-report.v1"

_BASE_SOURCE_CATEGORIES = frozenset(
    {
        "market_price",
        "industry_price",
        "official_context",
        "macro_vintage",
        "established_news",
        "positioning_or_expectations",
    }
)
_STRATEGIES = (
    "cash",
    "primary_buy_and_hold",
    "equal_sector_buy_and_hold",
    "lagged_sector_momentum",
)
_RETURN_QUANTUM = Decimal("0.00000001")
_HUNDRED = Decimal(100)
_TEN_THOUSAND = Decimal(10_000)
_PRIVATE_ROOT = Path(".market-impact") / "regime" / "comparisons"


class _JsonSchemaValidator(Protocol):
    def iter_errors(self, instance: object) -> Iterable[ValidationError]: ...


@dataclass(frozen=True, slots=True)
class RegimeStudySource:
    source_id: str
    category: str
    provider_id: str
    source_tier: str
    acquisition_mode: str
    point_in_time_authority: bool
    evidence_types: tuple[str, ...]
    license_note: str


@dataclass(frozen=True, slots=True)
class RegimeSourceRequirement:
    category: str
    source_ids: tuple[str, ...]
    minimum_records_per_checkpoint: int
    minimum_distinct_sources: int
    authenticated_availability_required: bool


@dataclass(frozen=True, slots=True)
class RegimeStudyCase:
    case_key: str
    decision_schedule: str
    analysis_needs: tuple[str, ...]
    candidate_method_skills: tuple[str, ...]
    query_terms: tuple[str, ...]
    evaluation_horizons: tuple[str, ...]
    source_requirements: tuple[RegimeSourceRequirement, ...]


@dataclass(frozen=True, slots=True)
class RegimeCheckpointProtocol:
    timezone: str
    decision_time_local: str
    price_lookback_sessions: int
    news_lookback_calendar_days: tuple[tuple[str, int], ...]
    maximum_age_calendar_days: tuple[tuple[str, int], ...]


@dataclass(frozen=True, slots=True)
class RegimeBaselineProtocol:
    annualization_sessions: int
    minimum_risk_sessions: int
    risk_free_rate_annual: Decimal
    cvar_confidence: Decimal
    transaction_cost_bps_one_way: Decimal
    rebalance_frequency: str
    momentum_lookback_sessions: int
    momentum_top_k: int
    strategies: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class RegimeStudyRegistration:
    registration_id: str
    version: str
    dataset_id: str
    dataset_hash: str
    method_catalog_id: str
    method_catalog_hash: str
    outcomes_opened: bool
    source_catalog: tuple[RegimeStudySource, ...]
    checkpoint_protocol: RegimeCheckpointProtocol
    baseline_protocol: RegimeBaselineProtocol
    cases: tuple[RegimeStudyCase, ...]
    core: dict[str, object]

    @property
    def registration_hash(self) -> str:
        return canonical_hash(self.core)


def load_regime_study_registration(
    path: Path,
    *,
    dataset: MarketRegimeDataset,
    method_catalog: MethodSkillCatalog,
) -> RegimeStudyRegistration:
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError("regime study registration must be an object")
    payload = cast(dict[str, object], raw)
    _validate_json_schema(payload, "regime-study-registration.schema.json")
    if payload.get("schema_version") != REGIME_STUDY_REGISTRATION_SCHEMA:
        raise ValueError("unsupported regime study registration schema_version")
    registration_id = _required_string(payload, "registration_id")
    core = {key: value for key, value in payload.items() if key != "registration_id"}
    expected_id = f"regime-study-registration-{canonical_hash(core)}"
    if registration_id != expected_id:
        raise ValueError("regime study registration_id does not match canonical content")
    if (
        payload.get("dataset_id") != dataset.dataset_id
        or payload.get("dataset_hash") != dataset.dataset_hash
    ):
        raise ValueError("regime study registration does not bind the market regime dataset")
    if (
        payload.get("method_catalog_id") != method_catalog.catalog_id
        or payload.get("method_catalog_hash") != method_catalog.catalog_hash
    ):
        raise ValueError("regime study registration does not bind the method Skill catalog")

    source_catalog = tuple(_source(item) for item in _object_list(payload, "source_catalog"))
    source_ids = tuple(item.source_id for item in source_catalog)
    _unique(source_ids, "regime study source ids")
    source_by_id = {item.source_id: item for item in source_catalog}
    checkpoint_protocol = _checkpoint_protocol(_required_object(payload, "checkpoint_protocol"))
    baseline_protocol = _baseline_protocol(_required_object(payload, "baseline_protocol"))
    cases = tuple(_case(item) for item in _object_list(payload, "cases"))
    case_keys = tuple(item.case_key for item in cases)
    _unique(case_keys, "regime study case keys")
    if set(case_keys) != {item.case_key for item in dataset.cases}:
        raise ValueError("regime study cases must exactly cover the market regime dataset")

    method_by_name = {item.skill_name: item for item in method_catalog.methods}
    for case in cases:
        requirement_categories = {item.category for item in case.source_requirements}
        if not requirement_categories >= _BASE_SOURCE_CATEGORIES:
            missing = sorted(_BASE_SOURCE_CATEGORIES - requirement_categories)
            raise ValueError(f"regime study case lacks required source categories: {missing}")
        requirements_by_category = {item.category: item for item in case.source_requirements}
        if len(requirements_by_category) != len(case.source_requirements):
            raise ValueError("regime study source requirement categories must be unique")
        for requirement in case.source_requirements:
            sources = _resolve_requirement_sources(requirement, source_by_id)
            if any(source.category != requirement.category for source in sources):
                raise ValueError("regime study requirement references a source in another category")
            if requirement.minimum_distinct_sources > len(sources):
                raise ValueError("minimum_distinct_sources exceeds registered source count")
        news = requirements_by_category["established_news"]
        news_sources = _resolve_requirement_sources(news, source_by_id)
        if news.minimum_distinct_sources < 2 or len(news_sources) < 2:
            raise ValueError("each case requires at least two established-news sources")
        if any(item.source_tier != "established_news" for item in news_sources):
            raise ValueError("established-news requirements must use established-news sources")

        evidence_types = {
            evidence_type
            for requirement in case.source_requirements
            for source in _resolve_requirement_sources(requirement, source_by_id)
            for evidence_type in source.evidence_types
        }
        if not case.candidate_method_skills:
            raise ValueError("regime study case requires at least one candidate method Skill")
        for method_name in case.candidate_method_skills:
            method = method_by_name.get(method_name)
            if method is None:
                raise ValueError(
                    f"regime study case references unknown method Skill: {method_name}"
                )
            if not set(method.analysis_needs).intersection(case.analysis_needs):
                raise ValueError("case analysis needs do not activate its candidate method Skill")
            missing_evidence = set(method.required_evidence) - evidence_types
            if missing_evidence:
                raise ValueError(
                    "case source plan cannot satisfy method evidence types: "
                    + ", ".join(sorted(missing_evidence))
                )

    return RegimeStudyRegistration(
        registration_id=registration_id,
        version=_required_string(payload, "version"),
        dataset_id=dataset.dataset_id,
        dataset_hash=dataset.dataset_hash,
        method_catalog_id=method_catalog.catalog_id,
        method_catalog_hash=method_catalog.catalog_hash,
        outcomes_opened=_required_bool(payload, "outcomes_opened"),
        source_catalog=source_catalog,
        checkpoint_protocol=checkpoint_protocol,
        baseline_protocol=baseline_protocol,
        cases=cases,
        core=core,
    )


def assess_regime_study_readiness(
    registration: RegimeStudyRegistration,
) -> dict[str, object]:
    source_by_id = {item.source_id: item for item in registration.source_catalog}
    case_results: list[dict[str, object]] = []
    all_ready = True
    for case in registration.cases:
        blockers: list[str] = []
        for requirement in case.source_requirements:
            sources = _resolve_requirement_sources(requirement, source_by_id)
            if sum(item.acquisition_mode.startswith("implemented_") for item in sources) < (
                requirement.minimum_distinct_sources
            ):
                blockers.append(f"{requirement.category}:not_implemented")
            if (
                requirement.authenticated_availability_required
                and sum(item.point_in_time_authority for item in sources)
                < requirement.minimum_distinct_sources
            ):
                blockers.append(f"{requirement.category}:no_point_in_time_authority")
        all_ready = all_ready and not blockers
        case_results.append(
            {
                "case_key": case.case_key,
                "ready_for_outcome_blinded_agent_run": not blockers
                and not registration.outcomes_opened,
                "blockers": blockers,
            }
        )
    return {
        "registration_id": registration.registration_id,
        "registration_hash": registration.registration_hash,
        "case_count": len(registration.cases),
        "outcomes_opened": registration.outcomes_opened,
        "all_source_requirements_ready": all_ready,
        "agent_effectiveness_claim_eligible": all_ready and not registration.outcomes_opened,
        "cases": case_results,
    }


def evaluate_regime_study_baselines(
    dataset: MarketRegimeDataset,
    validated_panel: ValidatedRegimePanel | RegimePanel,
    registration: RegimeStudyRegistration,
) -> dict[str, object]:
    panel, panel_id, panel_hash = _resolve_panel(validated_panel)
    if panel.dataset_id != dataset.dataset_id or panel.dataset_hash != dataset.dataset_hash:
        raise ValueError("regime panel does not bind the market regime dataset")
    if (
        registration.dataset_id != dataset.dataset_id
        or registration.dataset_hash != dataset.dataset_hash
    ):
        raise ValueError("regime study registration does not bind the market regime dataset")
    source_readiness = assess_regime_study_readiness(registration)
    readiness_by_case = {
        cast(dict[str, object], item)["case_key"]: cast(dict[str, object], item)
        for item in cast(list[object], source_readiness["cases"])
    }
    by_id = _series_by_id(panel)
    plan_by_case = {item.case_key: item for item in registration.cases}
    results: list[dict[str, object]] = []
    for case in dataset.cases:
        plan = plan_by_case[case.case_key]
        result = evaluate_regime_case_baselines(case, by_id, registration.baseline_protocol)
        results.append(
            {
                **result,
                "analysis_needs": list(plan.analysis_needs),
                "candidate_method_skills": list(plan.candidate_method_skills),
                "source_readiness": readiness_by_case[case.case_key],
            }
        )
    report: dict[str, object] = {
        "schema_version": REGIME_STUDY_REPORT_SCHEMA,
        "registration_id": registration.registration_id,
        "registration_hash": registration.registration_hash,
        "dataset_id": dataset.dataset_id,
        "dataset_hash": dataset.dataset_hash,
        "panel_id": panel_id,
        "panel_hash": panel_hash,
        "provider_id": panel.provider_id,
        "provider_version": panel.provider_version,
        "historical_vintage": panel.historical_vintage,
        "outcomes_opened": registration.outcomes_opened,
        "research_only": True,
        "agent_visible": False,
        "agent_effectiveness_claim_eligible": False,
        "price_indices_are_non_executable_proxies": True,
        "case_count": len(results),
        "cases": results,
    }
    _validate_json_schema(report, "regime-study-baseline-report.schema.json")
    return report


def write_regime_study_baseline_report(
    report: dict[str, object],
    *,
    panel_id: str,
    registration_id: str,
) -> Path:
    root = (Path.cwd().resolve() / _PRIVATE_ROOT).resolve()
    expected_parent = (Path.cwd().resolve() / ".market-impact" / "regime").resolve()
    if expected_parent not in root.parents:
        raise ValueError("regime study output root escaped the private regime directory")
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(root, 0o700)
    destination = root / f"{panel_id}--{registration_id}.json"
    if destination.is_symlink():
        raise ValueError("regime study report destination must not be a symlink")
    destination.write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    os.chmod(destination, 0o600)
    return destination


def evaluate_regime_case_baselines(
    case: MarketRegimeCase,
    by_id: dict[str, RegimeSeries],
    protocol: RegimeBaselineProtocol,
) -> dict[str, object]:
    primary = by_id.get(case.primary_market_index)
    industries = [by_id.get(proxy_id) for proxy_id in case.required_industry_proxies]
    if primary is None or any(item is None for item in industries):
        return {
            "case_key": case.case_key,
            "status": "missing_series",
            "session_count": 0,
            "strategies": {},
        }
    industry_series = cast(list[RegimeSeries], industries)
    dates = _common_dates((primary, *industry_series), case.tradable_start, case.end)
    primary_dates = tuple(
        _row_date(row) for row in primary.rows if case.tradable_start <= _row_date(row) <= case.end
    )
    if (
        not dates
        or dates != primary_dates
        or dates[0] != case.tradable_start
        or dates[-1] != case.end
    ):
        return {
            "case_key": case.case_key,
            "status": "incomplete_common_path",
            "session_count": len(dates),
            "strategies": {},
        }
    primary_path = _buy_and_hold_path(
        primary,
        dates,
        cost_bps=protocol.transaction_cost_bps_one_way,
    )
    equal_sector_path = _equal_sector_buy_and_hold_path(
        industry_series,
        dates,
        cost_bps=protocol.transaction_cost_bps_one_way,
    )
    momentum_path = _lagged_sector_momentum_path(industry_series, dates, protocol)
    cash_returns = tuple(Decimal(0) for _ in dates)
    paths: dict[str, _StrategyPath | None] = {
        "cash": _StrategyPath(returns=cash_returns, values=tuple(Decimal(1) for _ in dates)),
        "primary_buy_and_hold": primary_path,
        "equal_sector_buy_and_hold": equal_sector_path,
        "lagged_sector_momentum": momentum_path,
    }
    strategies: dict[str, object] = {}
    for name in protocol.strategies:
        path = paths[name]
        if path is None:
            strategies[name] = {"status": "insufficient_history"}
            continue
        strategies[name] = _path_metrics(
            path,
            benchmark_returns=primary_path.returns,
            protocol=protocol,
            is_cash=name == "cash",
        )
    return {
        "case_key": case.case_key,
        "status": "covered",
        "session_count": len(dates),
        "strategies": strategies,
    }


@dataclass(frozen=True, slots=True)
class _StrategyPath:
    returns: tuple[Decimal, ...]
    values: tuple[Decimal, ...]
    turnover: Decimal = Decimal(0)
    cost: Decimal = Decimal(0)


def _buy_and_hold_path(
    series: RegimeSeries,
    dates: tuple[date, ...],
    *,
    cost_bps: Decimal,
) -> _StrategyPath:
    rows = {_row_date(row): row for row in series.rows}
    start_open = _price(rows[dates[0]], "open")
    cost_rate = cost_bps / _TEN_THOUSAND
    units = (Decimal(1) - cost_rate) / start_open
    values = tuple(units * _price(rows[day], "close") for day in dates)
    returns = _returns_from_values(values)
    return _StrategyPath(
        returns=returns,
        values=values,
        turnover=Decimal(1),
        cost=cost_rate,
    )


def _equal_sector_buy_and_hold_path(
    series: list[RegimeSeries],
    dates: tuple[date, ...],
    *,
    cost_bps: Decimal,
) -> _StrategyPath:
    rows_by_series = [{_row_date(row): row for row in item.rows} for item in series]
    cost_rate = cost_bps / _TEN_THOUSAND
    capital = Decimal(1) - cost_rate
    allocation = capital / Decimal(len(series))
    units = tuple(allocation / _price(rows[dates[0]], "open") for rows in rows_by_series)
    values = tuple(
        sum(
            (
                units[index] * _price(rows[day], "close")
                for index, rows in enumerate(rows_by_series)
            ),
            Decimal(0),
        )
        for day in dates
    )
    return _StrategyPath(
        returns=_returns_from_values(values),
        values=values,
        turnover=Decimal(1),
        cost=cost_rate,
    )


def _lagged_sector_momentum_path(
    series: list[RegimeSeries],
    dates: tuple[date, ...],
    protocol: RegimeBaselineProtocol,
) -> _StrategyPath | None:
    rows_by_id = {item.series_id: {_row_date(row): row for row in item.rows} for item in series}
    ordered_rows = {item.series_id: sorted(item.rows, key=_row_date) for item in series}
    holdings: dict[str, Decimal] = {}
    prior_value = Decimal(1)
    values: list[Decimal] = []
    returns: list[Decimal] = []
    turnover_total = Decimal(0)
    cost_total = Decimal(0)
    last_month: tuple[int, int] | None = None
    cost_rate = protocol.transaction_cost_bps_one_way / _TEN_THOUSAND
    for day in dates:
        current_month = (day.year, day.month)
        rebalance = current_month != last_month
        open_value = (
            prior_value
            if not holdings
            else sum(
                (
                    units * _price(rows_by_id[series_id][day], "open")
                    for series_id, units in holdings.items()
                ),
                Decimal(0),
            )
        )
        if rebalance:
            scores: list[tuple[Decimal, str]] = []
            for item in series:
                history = [row for row in ordered_rows[item.series_id] if _row_date(row) < day]
                lookback = protocol.momentum_lookback_sessions
                if len(history) <= lookback:
                    return None
                score = _price(history[-1], "close") / _price(
                    history[-1 - lookback], "close"
                ) - Decimal(1)
                scores.append((score, item.series_id))
            selected = tuple(
                series_id
                for _, series_id in sorted(scores, reverse=True)[: protocol.momentum_top_k]
            )
            if len(selected) < protocol.momentum_top_k:
                return None
            target_weight = Decimal(1) / Decimal(len(selected))
            if not holdings:
                turnover = Decimal(1)
            else:
                current_weights = {
                    series_id: (units * _price(rows_by_id[series_id][day], "open") / open_value)
                    for series_id, units in holdings.items()
                }
                union = set(current_weights) | set(selected)
                turnover = sum(
                    (
                        abs(
                            (target_weight if series_id in selected else Decimal(0))
                            - current_weights.get(series_id, Decimal(0))
                        )
                        for series_id in union
                    ),
                    Decimal(0),
                ) / Decimal(2)
            cost = open_value * turnover * cost_rate
            investable = open_value - cost
            holdings = {
                series_id: (investable * target_weight) / _price(rows_by_id[series_id][day], "open")
                for series_id in selected
            }
            turnover_total += turnover
            cost_total += cost
            last_month = current_month
        close_value = sum(
            (
                units * _price(rows_by_id[series_id][day], "close")
                for series_id, units in holdings.items()
            ),
            Decimal(0),
        )
        returns.append(close_value / prior_value - Decimal(1))
        values.append(close_value)
        prior_value = close_value
    return _StrategyPath(
        returns=tuple(returns),
        values=tuple(values),
        turnover=turnover_total,
        cost=cost_total,
    )


def _path_metrics(
    path: _StrategyPath,
    *,
    benchmark_returns: tuple[Decimal, ...],
    protocol: RegimeBaselineProtocol,
    is_cash: bool,
) -> dict[str, object]:
    returns = path.returns
    total_return = path.values[-1] - Decimal(1)
    peak = Decimal(1)
    max_drawdown = Decimal(0)
    for value in path.values:
        peak = max(peak, value)
        max_drawdown = min(max_drawdown, value / peak - Decimal(1))
    tail_count = max(1, math.ceil((1 - float(protocol.cvar_confidence)) * len(returns)))
    cvar = sum(sorted(returns)[:tail_count], Decimal(0)) / Decimal(tail_count)
    enough = len(returns) >= protocol.minimum_risk_sessions
    annualized_return: Decimal | None = None
    annualized_volatility: Decimal | None = None
    sharpe: Decimal | None = None
    information_ratio: Decimal | None = None
    if enough:
        annualized_return = Decimal(
            str((1 + float(total_return)) ** (protocol.annualization_sessions / len(returns)) - 1)
        )
        volatility = _sample_std(returns)
        if volatility > 0:
            annualized_volatility = volatility * Decimal(
                str(math.sqrt(protocol.annualization_sessions))
            )
            daily_risk_free = Decimal(
                str(
                    (1 + float(protocol.risk_free_rate_annual))
                    ** (1 / protocol.annualization_sessions)
                    - 1
                )
            )
            sharpe = (
                (_mean(returns) - daily_risk_free)
                / volatility
                * Decimal(str(math.sqrt(protocol.annualization_sessions)))
            )
        active = tuple(
            item - benchmark for item, benchmark in zip(returns, benchmark_returns, strict=True)
        )
        active_volatility = _sample_std(active)
        if active_volatility > 0:
            information_ratio = (
                _mean(active)
                / active_volatility
                * Decimal(str(math.sqrt(protocol.annualization_sessions)))
            )
    upside_benchmark = sum((item for item in benchmark_returns if item > 0), Decimal(0))
    downside_benchmark = sum((item for item in benchmark_returns if item < 0), Decimal(0))
    upside_strategy = sum(
        (item for item, benchmark in zip(returns, benchmark_returns, strict=True) if benchmark > 0),
        Decimal(0),
    )
    downside_strategy = sum(
        (item for item, benchmark in zip(returns, benchmark_returns, strict=True) if benchmark < 0),
        Decimal(0),
    )
    return {
        "status": "covered",
        "session_count": len(returns),
        "risk_metrics_eligible": enough,
        "total_return": _format(total_return),
        "annualized_return": _optional_format(annualized_return),
        "annualized_volatility": _optional_format(annualized_volatility),
        "sharpe": None if is_cash else _optional_format(sharpe),
        "max_drawdown": _format(max_drawdown),
        "cvar": _format(cvar),
        "information_ratio_vs_primary": _optional_format(information_ratio),
        "upside_capture_ratio": (
            None if upside_benchmark == 0 else _format(upside_strategy / upside_benchmark)
        ),
        "downside_loss_participation_ratio": (
            None if downside_benchmark == 0 else _format(downside_strategy / downside_benchmark)
        ),
        "turnover": _format(path.turnover),
        "modeled_cost": _format(path.cost),
    }


def _returns_from_values(values: tuple[Decimal, ...]) -> tuple[Decimal, ...]:
    prior = Decimal(1)
    returns: list[Decimal] = []
    for value in values:
        returns.append(value / prior - Decimal(1))
        prior = value
    return tuple(returns)


def _common_dates(series: tuple[RegimeSeries, ...], start: date, end: date) -> tuple[date, ...]:
    date_sets: list[set[date]] = [
        {_row_date(row) for row in item.rows if start <= _row_date(row) <= end} for item in series
    ]
    if not date_sets:
        return ()
    common = date_sets[0].intersection(*date_sets[1:])
    return tuple(sorted(common))


def _series_by_id(panel: RegimePanel) -> dict[str, RegimeSeries]:
    result = {item.series_id: item for item in panel.series}
    by_code = {item.tushare_code: item for item in panel.series}
    for proxy_id, code in panel.proxy_resolution:
        if code in by_code:
            result[proxy_id] = by_code[code]
    return result


def _resolve_panel(
    validated_panel: ValidatedRegimePanel | RegimePanel,
) -> tuple[RegimePanel, str, str]:
    if isinstance(validated_panel, ValidatedRegimePanel):
        return validated_panel.panel, validated_panel.panel_id, validated_panel.panel_hash
    core = {
        "dataset_id": validated_panel.dataset_id,
        "dataset_hash": validated_panel.dataset_hash,
        "provider_id": validated_panel.provider_id,
        "provider_version": validated_panel.provider_version,
        "historical_vintage": validated_panel.historical_vintage,
        "retrieved_at": validated_panel.retrieved_at.isoformat(),
        "series": [item.series_id for item in validated_panel.series],
    }
    panel_hash = canonical_hash(core)
    return validated_panel, f"regime-panel-{panel_hash}", panel_hash


def _source(payload: dict[str, object]) -> RegimeStudySource:
    return RegimeStudySource(
        source_id=_required_string(payload, "source_id"),
        category=_required_string(payload, "category"),
        provider_id=_required_string(payload, "provider_id"),
        source_tier=_required_string(payload, "source_tier"),
        acquisition_mode=_required_string(payload, "acquisition_mode"),
        point_in_time_authority=_required_bool(payload, "point_in_time_authority"),
        evidence_types=_string_tuple(payload.get("evidence_types"), "evidence_types"),
        license_note=_required_string(payload, "license_note"),
    )


def _case(payload: dict[str, object]) -> RegimeStudyCase:
    return RegimeStudyCase(
        case_key=_required_string(payload, "case_key"),
        decision_schedule=_required_string(payload, "decision_schedule"),
        analysis_needs=_string_tuple(payload.get("analysis_needs"), "analysis_needs"),
        candidate_method_skills=_string_tuple(
            payload.get("candidate_method_skills"), "candidate_method_skills"
        ),
        query_terms=_string_tuple(payload.get("query_terms"), "query_terms"),
        evaluation_horizons=_string_tuple(
            payload.get("evaluation_horizons"), "evaluation_horizons"
        ),
        source_requirements=tuple(
            _source_requirement(item) for item in _object_list(payload, "source_requirements")
        ),
    )


def _source_requirement(payload: dict[str, object]) -> RegimeSourceRequirement:
    return RegimeSourceRequirement(
        category=_required_string(payload, "category"),
        source_ids=_string_tuple(payload.get("source_ids"), "source_ids"),
        minimum_records_per_checkpoint=_required_int(payload, "minimum_records_per_checkpoint"),
        minimum_distinct_sources=_required_int(payload, "minimum_distinct_sources"),
        authenticated_availability_required=_required_bool(
            payload, "authenticated_availability_required"
        ),
    )


def _baseline_protocol(payload: dict[str, object]) -> RegimeBaselineProtocol:
    protocol = RegimeBaselineProtocol(
        annualization_sessions=_required_int(payload, "annualization_sessions"),
        minimum_risk_sessions=_required_int(payload, "minimum_risk_sessions"),
        risk_free_rate_annual=_decimal(
            payload.get("risk_free_rate_annual"), "risk_free_rate_annual"
        ),
        cvar_confidence=_decimal(payload.get("cvar_confidence"), "cvar_confidence"),
        transaction_cost_bps_one_way=_decimal(
            payload.get("transaction_cost_bps_one_way"), "transaction_cost_bps_one_way"
        ),
        rebalance_frequency=_required_string(payload, "rebalance_frequency"),
        momentum_lookback_sessions=_required_int(payload, "momentum_lookback_sessions"),
        momentum_top_k=_required_int(payload, "momentum_top_k"),
        strategies=_string_tuple(payload.get("strategies"), "strategies"),
    )
    if protocol.strategies != _STRATEGIES:
        raise ValueError("regime study baseline strategies must match the frozen ordered set")
    if not Decimal("0.5") < protocol.cvar_confidence < Decimal(1):
        raise ValueError("cvar_confidence must be between 0.5 and 1")
    if protocol.risk_free_rate_annual <= Decimal(-1):
        raise ValueError("risk_free_rate_annual must exceed -1")
    if protocol.transaction_cost_bps_one_way < 0:
        raise ValueError("transaction cost cannot be negative")
    return protocol


def _checkpoint_protocol(payload: dict[str, object]) -> RegimeCheckpointProtocol:
    timezone = _required_string(payload, "timezone")
    if timezone != "Asia/Shanghai":
        raise ValueError("regime checkpoint timezone must be Asia/Shanghai")
    ZoneInfo(timezone)
    decision_time = _required_string(payload, "decision_time_local")
    try:
        parsed_time = time.fromisoformat(decision_time)
    except ValueError as exc:
        raise ValueError("decision_time_local must use HH:MM:SS") from exc
    if parsed_time.tzinfo is not None or parsed_time.isoformat() != decision_time:
        raise ValueError("decision_time_local must be an unzoned HH:MM:SS value")
    news_windows = _positive_int_mapping(
        payload, "news_lookback_calendar_days", {"monthly", "weekly", "event_then_weekly"}
    )
    maximum_ages = _positive_int_mapping(
        payload,
        "maximum_age_calendar_days",
        {
            "official_context",
            "macro_vintage",
            "positioning_or_expectations",
            "issuer_or_sector_fundamentals",
        },
    )
    price_lookback_sessions = _required_int(payload, "price_lookback_sessions")
    if price_lookback_sessions < 1:
        raise ValueError("price_lookback_sessions must be positive")
    return RegimeCheckpointProtocol(
        timezone=timezone,
        decision_time_local=decision_time,
        price_lookback_sessions=price_lookback_sessions,
        news_lookback_calendar_days=tuple(sorted(news_windows.items())),
        maximum_age_calendar_days=tuple(sorted(maximum_ages.items())),
    )


def _positive_int_mapping(
    payload: dict[str, object], name: str, expected_keys: set[str]
) -> dict[str, int]:
    values = _required_object(payload, name)
    if set(values) != expected_keys:
        raise ValueError(f"{name} must contain exactly {sorted(expected_keys)}")
    result: dict[str, int] = {}
    for key, value in values.items():
        if isinstance(value, bool) or not isinstance(value, int) or value < 1:
            raise ValueError(f"{name}.{key} must be a positive integer")
        result[key] = value
    return result


def _resolve_requirement_sources(
    requirement: RegimeSourceRequirement,
    source_by_id: dict[str, RegimeStudySource],
) -> tuple[RegimeStudySource, ...]:
    try:
        return tuple(source_by_id[source_id] for source_id in requirement.source_ids)
    except KeyError as exc:
        raise ValueError(
            f"regime study requirement references unknown source: {exc.args[0]}"
        ) from exc


def _validate_json_schema(payload: dict[str, object], schema_name: str) -> None:
    package_root = Path(__file__).resolve().parent
    installed = package_root / "schemas" / schema_name
    path = installed if installed.is_file() else package_root.parents[1] / "schemas" / schema_name
    schema = json.loads(path.read_text(encoding="utf-8"))
    validator = cast(
        _JsonSchemaValidator,
        Draft202012Validator(schema, format_checker=FormatChecker()),
    )
    errors = sorted(validator.iter_errors(payload), key=lambda item: list(item.path))
    if errors:
        detail = "; ".join(error.message for error in errors)
        raise ValueError(f"{schema_name} validation failed: {detail}")


def _object_list(payload: dict[str, object], key: str) -> list[dict[str, object]]:
    value = payload.get(key)
    if not isinstance(value, list):
        raise TypeError(f"{key} must be an array")
    return [_object(item, f"{key} item") for item in cast(list[object], value)]


def _required_object(payload: dict[str, object], key: str) -> dict[str, object]:
    return _object(payload.get(key), key)


def _object(value: object, name: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise TypeError(f"{name} must be an object")
    return cast(dict[str, object], value)


def _required_string(payload: dict[str, object], key: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value.strip():
        raise TypeError(f"{key} must be a non-empty string")
    return value


def _required_bool(payload: dict[str, object], key: str) -> bool:
    value = payload.get(key)
    if not isinstance(value, bool):
        raise TypeError(f"{key} must be a boolean")
    return value


def _required_int(payload: dict[str, object], key: str) -> int:
    value = payload.get(key)
    if not isinstance(value, int) or isinstance(value, bool) or value < 1:
        raise TypeError(f"{key} must be a positive integer")
    return value


def _string_tuple(value: object, name: str) -> tuple[str, ...]:
    if not isinstance(value, list):
        raise TypeError(f"{name} must be an array")
    raw_items = cast(list[object], value)
    items = tuple(item for item in raw_items if isinstance(item, str) and item.strip())
    if len(items) != len(raw_items) or not items:
        raise TypeError(f"{name} must contain non-empty strings")
    _unique(items, name)
    return items


def _unique(values: tuple[str, ...], name: str) -> None:
    if len(values) != len(set(values)):
        raise ValueError(f"{name} must be unique")


def _decimal(value: object, name: str) -> Decimal:
    try:
        return Decimal(str(value))
    except Exception as exc:
        raise TypeError(f"{name} must be decimal-compatible") from exc


def _row_date(row: dict[str, object]) -> date:
    value = _required_string(row, "trade_date")
    return date.fromisoformat(value if "-" in value else f"{value[:4]}-{value[4:6]}-{value[6:]}")


def _price(row: dict[str, object], field: str) -> Decimal:
    value = _decimal(row.get(field), field)
    if value <= 0:
        raise ValueError(f"{field} must be positive")
    return value


def _mean(values: tuple[Decimal, ...]) -> Decimal:
    return sum(values, Decimal(0)) / Decimal(len(values))


def _sample_std(values: tuple[Decimal, ...]) -> Decimal:
    if len(values) < 2:
        return Decimal(0)
    mean = _mean(values)
    variance = sum(((item - mean) ** 2 for item in values), Decimal(0)) / Decimal(len(values) - 1)
    return variance.sqrt()


def _format(value: Decimal) -> str:
    quantized = value.quantize(_RETURN_QUANTUM, rounding=ROUND_HALF_EVEN)
    if quantized == 0:
        quantized = Decimal(0).quantize(_RETURN_QUANTUM)
    return f"{quantized:.8f}"


def _optional_format(value: Decimal | None) -> str | None:
    return None if value is None else _format(value)
