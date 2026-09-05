from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace
from typing import cast

import pytest

from market_impact_agent.continuous_metrics import (
    compare_continuous_accounts,
    measure_continuous_account,
)
from market_impact_agent.streaming_nautilus_account import HistoricalSessionResult


def _measure(values: list[str], expected: int = 2) -> dict[str, object]:
    sessions = [
        cast(
            HistoricalSessionResult,
            SimpleNamespace(
                account_state=SimpleNamespace(
                    as_of=datetime(2020, 2, 3, 7, tzinfo=UTC) + timedelta(days=i)
                ),
                nav=Decimal(value),
                cash=Decimal(value),
                fills=(),
                no_fills=(),
                positions={},
            ),
        )
        for i, value in enumerate(values)
    ]
    return measure_continuous_account(
        initial_nav=Decimal(100),
        sessions=sessions,
        expected_sessions=expected,
        execution_policy_hash="a" * 64,
        initial_account_hash="b" * 64,
        model_cost_microusd=100,
    )


def test_net_account_path_and_small_sample_tail_do_not_sum_forecasts() -> None:
    value = _measure(["90", "94.5"])
    assert Decimal(str(value["net_return"])) == Decimal("-.055")
    assert Decimal(str(value["maximum_drawdown"])) == Decimal(".1")
    assert Decimal(str(value["daily_return_cvar_95"])) == Decimal("-.1")
    assert value["tail_observation_mass"] == "0.10"
    assert value["model_cost_deducted_from_cny_nav"] is False
    assert value["complete"] is True
    assert compare_continuous_accounts(_measure(["90"]), value)["status"] == "incomplete_pair"


def test_paired_control_is_bound_and_loss_avoidance_is_not_all_outperformance() -> None:
    reviewed, control = _measure(["100", "120"]), _measure(["100", "90"])
    value = compare_continuous_accounts(reviewed, control)
    assert Decimal(str(value["performance_difference"])) == Decimal(".3")
    assert Decimal(str(value["avoided_loss_relative_to_control"])) == Decimal(".1")
    assert value["missed_upside_relative_to_control"] is None
    with pytest.raises(ValueError, match="same account"):
        compare_continuous_accounts(reviewed, {**control, "initial_account_hash": "c" * 64})
