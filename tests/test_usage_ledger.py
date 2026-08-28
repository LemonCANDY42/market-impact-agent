import sqlite3
from datetime import UTC, datetime
from pathlib import Path

import pytest

from market_impact_agent.agent_engine import AgentRunResult, RunMetrics
from market_impact_agent.runtime_store import RunStatus
from market_impact_agent.usage_ledger import (
    UsageLedger,
    UsageRecord,
    reconcile_usage_ledgers,
)

NOW = datetime(2026, 8, 26, 12, tzinfo=UTC)


def _result(run_id: str, status: RunStatus, cost: int) -> AgentRunResult:
    return AgentRunResult(
        run_id=run_id,
        status=status,
        judgment=None,
        terminal_store_hash=f"{cost + 1:064x}",
        metrics=RunMetrics(
            turns=1,
            tool_calls=0,
            input_tokens=10,
            output_tokens=2,
            result_bytes=0,
            latency_ms=5,
            provider_attempts=1,
            estimated_cost_microusd=cost,
        ),
    )


def _record(run_id: str, status: RunStatus, cost: int) -> UsageRecord:
    return UsageRecord.from_result(
        experiment_id="ablation-fixture",
        arm_id="neutral_evidence",
        recorded_at=NOW,
        provider_profile_id="model-provider-fixture",
        provider_profile_hash="1" * 64,
        execution_binding_hash="2" * 64,
        run_journal_hash="3" * 64,
        result=_result(run_id, status, cost),
    )


def test_usage_ledger_covers_success_and_failure_with_an_append_only_hash_chain(
    tmp_path: Path,
) -> None:
    ledger = UsageLedger(tmp_path / "usage.sqlite3")
    completed = ledger.append(_record("completed-run", RunStatus.COMPLETED, 10))
    failed = ledger.append(_record("failed-run", RunStatus.FAILED, 3))

    assert ledger.append(_record("completed-run", RunStatus.COMPLETED, 10)) == completed
    assert failed.previous_hash == completed.record_hash
    assert tuple(item.record.status for item in ledger.records()) == (
        RunStatus.COMPLETED,
        RunStatus.FAILED,
    )
    assert len(ledger.ledger_hash) == 64
    with pytest.raises(ValueError, match="different content"):
        ledger.append(_record("completed-run", RunStatus.COMPLETED, 11))
    with (
        sqlite3.connect(ledger.path) as connection,
        pytest.raises(sqlite3.IntegrityError, match="append-only"),
    ):
        connection.execute(
            "UPDATE usage_records SET payload_hash = ? WHERE run_id = ?",
            ("0" * 64, "completed-run"),
        )


def test_usage_ledger_union_deduplicates_identical_runs_and_reconciles_cost(
    tmp_path: Path,
) -> None:
    first = UsageLedger(tmp_path / "first" / "usage.sqlite3")
    second = UsageLedger(tmp_path / "second" / "usage.sqlite3")
    shared = _record("shared-run", RunStatus.COMPLETED, 10)
    first.append(shared)
    first.append(_record("failed-run", RunStatus.FAILED, 3))
    second.append(shared)
    second.append(_record("other-run", RunStatus.COMPLETED, 7))

    result = reconcile_usage_ledgers((first.path, second.path))

    assert result.ledger_count == 2
    assert result.duplicate_record_count == 1
    assert result.unique_run_count == 3
    assert result.completed_run_count == 2
    assert result.failed_run_count == 1
    assert result.total_estimated_cost_microusd == 20
    assert result.status_counts == {"completed": 2, "failed": 1}
    assert tuple(item.run_id for item in result.records) == (
        "failed-run",
        "other-run",
        "shared-run",
    )
    assert len(result.union_hash) == 64


def test_usage_ledger_union_rejects_conflicting_duplicate_run_ids(tmp_path: Path) -> None:
    first = UsageLedger(tmp_path / "first" / "usage.sqlite3")
    second = UsageLedger(tmp_path / "second" / "usage.sqlite3")
    first.append(_record("shared-run", RunStatus.COMPLETED, 10))
    second.append(_record("shared-run", RunStatus.COMPLETED, 11))

    with pytest.raises(ValueError, match="conflicting content"):
        reconcile_usage_ledgers((first.path, second.path))


def test_usage_ledger_union_rejects_symlinked_ledgers(tmp_path: Path) -> None:
    ledger = UsageLedger(tmp_path / "real" / "usage.sqlite3")
    ledger.append(_record("completed-run", RunStatus.COMPLETED, 10))
    link = tmp_path / "linked.sqlite3"
    link.symlink_to(ledger.path)

    with pytest.raises(ValueError, match="real ledger files"):
        reconcile_usage_ledgers((link,))
