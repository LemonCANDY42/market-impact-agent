# pyright: reportPrivateUsage=false
from __future__ import annotations

from datetime import UTC, datetime

from market_impact_agent.agent_engine import RunMetrics
from market_impact_agent.continuous_retrospective import _cumulative_attribution, summarize_usage
from market_impact_agent.runtime_store import RunStatus
from market_impact_agent.usage_ledger import UsageRecord


def _record(run_id: str, *, attempts: int, cost: int, latency_ms: float) -> UsageRecord:
    return UsageRecord(
        experiment_id="retrospective-fixture",
        arm_id="fixture",
        run_id=run_id,
        recorded_at=datetime(2026, 9, 6, 1, tzinfo=UTC),
        status=RunStatus.FAILED,
        provider_profile_id="fixture",
        provider_profile_hash="a" * 64,
        execution_binding_hash="b" * 64,
        terminal_artifact_hash=None,
        run_journal_hash="c" * 64,
        metrics=RunMetrics(
            turns=attempts,
            tool_calls=attempts + 1,
            input_tokens=attempts + 2,
            output_tokens=attempts + 3,
            result_bytes=0,
            latency_ms=latency_ms,
            provider_attempts=attempts,
            estimated_cost_microusd=cost,
        ),
    )


def test_usage_summary_keeps_unpaid_terminal_runs_in_the_denominator() -> None:
    summary = summarize_usage(
        (
            _record("paid-first", attempts=1, cost=7, latency_ms=5),
            _record("unpaid-terminal", attempts=0, cost=0, latency_ms=2),
            _record("paid-second", attempts=2, cost=13, latency_ms=9),
        )
    )

    assert summary == {
        "terminal_runs": 3,
        "paid_terminal_runs": 2,
        "estimated_cost_microusd": 20,
        "input_tokens": 9,
        "output_tokens": 12,
        "latency_ms": 16,
        "provider_attempts": 3,
        "tool_calls": 6,
        "turns": 3,
        "median_paid_run_cost_microusd": 10.0,
        "median_paid_run_latency_ms": 7.0,
    }


def test_cumulative_attribution_keeps_reservations_outside_known_spend() -> None:
    attribution = _cumulative_attribution(
        {
            "budget": {
                "known_cost_microusd": 30,
                "physical_requests": 5,
                "reserved_microusd": 7,
                "unsettled_requests": 1,
            }
        },
        (_record("current", attempts=2, cost=11, latency_ms=3),),
    )

    assert attribution == {
        "current_identity": {"known_cost_microusd": 11, "physical_requests": 2},
        "cumulative_report": {"known_cost_microusd": 30, "physical_requests": 5},
        "prior_identities": {"known_cost_microusd": 19, "physical_requests": 3},
        "unsettled_reservation": {
            "reserved_microusd": 7,
            "unsettled_requests": 1,
            "included_in_known_spend": False,
        },
    }
