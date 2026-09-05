"""Account-path measurements; forecast endpoint sums are never portfolio returns."""

from __future__ import annotations

from collections.abc import Sequence
from decimal import Decimal, localcontext
from itertools import pairwise
from typing import cast

from market_impact_agent.streaming_nautilus_account import HistoricalSessionResult


def measure_continuous_account(
    *,
    initial_nav: Decimal,
    sessions: Sequence[HistoricalSessionResult],
    expected_sessions: int,
    execution_policy_hash: str,
    initial_account_hash: str,
    model_cost_microusd: int,
) -> dict[str, object]:
    if not initial_nav.is_finite() or initial_nav <= 0 or expected_sessions < 1:
        raise ValueError("account metrics require positive initial NAV and registered length")
    if len(sessions) > expected_sessions or model_cost_microusd < 0:
        raise ValueError("account observations or model cost exceed their domain")
    if len(execution_policy_hash) != 64 or len(initial_account_hash) != 64:
        raise ValueError("account metrics require bound execution policy and initial account")
    dates = [item.account_state.as_of.isoformat() for item in sessions]
    if dates != sorted(set(dates)):
        raise ValueError("account observations must be unique and chronological")
    navs = [initial_nav, *(item.nav for item in sessions)]
    if any(not nav.is_finite() or nav <= 0 for nav in navs):
        raise ValueError("nonpositive NAV requires a separate insolvency report")
    with localcontext() as context:
        context.prec = 28
        returns = [current / prior - 1 for prior, current in pairwise(navs)]
        peak = initial_nav
        drawdown = Decimal(0)
        for nav in navs:
            peak = max(peak, nav)
            drawdown = max(drawdown, 1 - nav / peak)
        # Empirical expected shortfall: fractional mass keeps the tail exactly
        # five percent instead of silently changing confidence for short paths.
        tail_mass = Decimal(len(returns)) * Decimal("0.05")
        remaining = tail_mass
        weighted_tail = Decimal(0)
        for value in sorted(returns):
            weight = min(remaining, Decimal(1))
            weighted_tail += value * weight
            remaining -= weight
            if remaining == 0:
                break
        cvar = None if not returns else weighted_tail / tail_mass
        turnover = sum(
            (abs(fill.price * fill.quantity) for item in sessions for fill in item.fills),
            start=Decimal(0),
        )
        fees = sum((fill.commission for item in sessions for fill in item.fills), start=Decimal(0))
        result: dict[str, object] = {
            "schema_version": "market-impact.continuous-account-measurement.v1",
            "execution_policy_hash": execution_policy_hash,
            "initial_account_hash": initial_account_hash,
            "initial_nav": str(initial_nav),
            "currency": "CNY",
            "observed_sessions": len(sessions),
            "expected_sessions": expected_sessions,
            "complete": len(sessions) == expected_sessions,
            "as_of": dates[-1] if dates else None,
            "net_return": str(navs[-1] / initial_nav - 1) if sessions else None,
            "maximum_drawdown": str(drawdown) if sessions else None,
            "daily_return_cvar_95": None if cvar is None else str(cvar),
            "tail_observation_mass": str(tail_mass),
            "tail_precision": "descriptive_small_sample" if len(returns) < 100 else "empirical",
            "turnover_over_initial_nav": str(turnover / initial_nav),
            "execution_fees_cny": str(fees),
            "model_cost_microusd": model_cost_microusd,
            "model_cost_deducted_from_cny_nav": False,
            "unfilled_order_count": sum(len(item.no_fills) for item in sessions),
            "cash_ratio": str(sessions[-1].cash / sessions[-1].nav) if sessions else None,
            "residual_positions": {}
            if not sessions
            else {key: str(value) for key, value in sessions[-1].positions.items() if value},
            "equity_curve": [
                {"as_of": at, "nav": str(item.nav), "cash": str(item.cash)}
                for at, item in zip(dates, sessions, strict=True)
            ],
            "investment_effectiveness_accepted": False,
        }
    return result


def compare_continuous_accounts(
    reviewed: dict[str, object], control: dict[str, object]
) -> dict[str, object]:
    for key in (
        "execution_policy_hash",
        "initial_account_hash",
        "initial_nav",
        "currency",
        "expected_sessions",
    ):
        if reviewed[key] != control[key]:
            raise ValueError("cadence comparison requires the same account/execution conditions")
    if reviewed["complete"] is not True or control["complete"] is not True:
        return {"status": "incomplete_pair", "performance_difference": None}
    if reviewed["as_of"] != control["as_of"]:
        raise ValueError("cadence comparison requires the same observation endpoint")
    reviewed_dates = [
        item["as_of"] for item in cast(list[dict[str, object]], reviewed["equity_curve"])
    ]
    control_dates = [
        item["as_of"] for item in cast(list[dict[str, object]], control["equity_curve"])
    ]
    if reviewed_dates != control_dates:
        raise ValueError("cadence comparison requires the same daily observation schedule")
    reviewed_return = Decimal(str(reviewed["net_return"]))
    control_return = Decimal(str(control["net_return"]))
    difference = reviewed_return - control_return
    return {
        "status": "measured_pair",
        "performance_difference": str(difference),
        "avoided_loss_relative_to_control": str(min(-control_return, max(Decimal(0), difference)))
        if control_return < 0
        else None,
        "missed_upside_relative_to_control": str(min(control_return, max(Decimal(0), -difference)))
        if control_return > 0
        else None,
        "maximum_drawdown_difference": str(
            Decimal(str(reviewed["maximum_drawdown"])) - Decimal(str(control["maximum_drawdown"]))
        ),
        "causal_correction_claim": "requires_reopened_thesis_and_trigger_evidence",
    }
