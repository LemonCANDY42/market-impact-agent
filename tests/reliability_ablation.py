"""Run a fixed, offline control/mutation matrix against existing production tests.

Usage: uv run python tests/reliability_ablation.py OUTPUT_DIRECTORY
Mutations exist only in disposable source copies. No production admission,
credentials, model API or broker is used. This is not a strategy evaluator.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
import xml.etree.ElementTree as ET
from dataclasses import asdict, dataclass
from hashlib import sha256
from pathlib import Path


@dataclass(frozen=True)
class Mutation:
    name: str
    module: str
    before: str
    after: str
    test: str
    occurrences: int = 1


MUTATIONS = (
    Mutation(
        "cache-double-count",
        "pi_execution",
        "        input_tokens,\n        output_tokens,",
        "        input_tokens + (cached or 0),\n        output_tokens,",
        "test_pi_runtime.py::test_usage_total_includes_cache_once_and_unknown_stays_unknown",
    ),
    Mutation(
        "drop-unknown-budget-reservation",
        "model_budget",
        '+ state["reserved_microusd"]',
        "+ 0",
        "test_model_budget.py::test_atomic_parent_reservations_retain_unknown_and_do_not_reset",
    ),
    Mutation(
        "disable-transient-408-recovery",
        "pi_execution",
        "status == 408",
        "False",
        "test_pi_runtime.py::test_pi_physical_failure_policy[received408-completed-3]",
    ),
    Mutation(
        "accept-empty-compaction",
        "pi_execution",
        'if purpose == "compaction" and (',
        "if False and (",
        "test_pi_runtime.py::test_pi_summary_shares_budget_and_committed_compaction_replays[empty-summary-True]",
    ),
    Mutation(
        "skip-tool-authorization",
        "agent_runtime",
        "if call.name not in access.allowed_tools or not self._allowed(descriptor, access):",
        "if False:",
        "test_agent_runtime.py::test_tool_registry_enforces_schema_permissions_redaction_and_artifact_indirection",
    ),
    Mutation(
        "skip-account-freshness",
        "authorized_decision_view",
        "if cutoff - position_snapshot.as_of > "
        "timedelta(seconds=position_snapshot.max_age_seconds):",
        "if False:",
        "test_authorized_decision_view.py::test_view_recomputes_freshness_at_its_own_cutoff",
    ),
    Mutation(
        "allow-adjusted-fill-price",
        "portfolio_decision",
        "or price_basis.basis_kind\n                "
        'not in {"reference_quote", "raw_reference_quote", "limit_price"}',
        "or False",
        "test_portfolio_decision_v2.py::test_raw_price_is_required_and_target_delta_is_idempotent",
    ),
    Mutation(
        "ignore-existing-holding",
        "portfolio_decision",
        "delta = target_signed - current_signed",
        "delta = target_signed",
        "test_portfolio_decision_v2.py::test_raw_price_is_required_and_target_delta_is_idempotent",
    ),
    Mutation(
        "ignore-order-notional-limit",
        "policy",
        "elif order.quantity * price > mandate.max_order_notional:",
        "elif False:",
        "test_policy.py::test_notional_and_account_violations_are_denied_together",
    ),
    Mutation(
        "bypass-dispatch-kill",
        "autonomous_paper",
        "if self.active_kill_reasons and not risk_reduction:",
        "if False:",
        "test_autonomous_paper.py::test_kill_blocks_increase_but_keeps_exact_cancel_and_reconcile_open",
        occurrences=2,
    ),
    Mutation(
        "requeue-unknown-submission",
        "autonomous_paper",
        "AutonomousOperationState.UNKNOWN.value,\n                    _timestamp(now),\n"
        "                    AutonomousOperationState.SUBMITTING.value,",
        "AutonomousOperationState.QUEUED.value,\n                    _timestamp(now),\n"
        "                    AutonomousOperationState.SUBMITTING.value,",
        "test_autonomous_paper.py::test_process_crash_recovers_submitting_lease_as_unknown_without_resubmit",
    ),
    Mutation(
        "forget-open-order-during-reconciliation",
        "paper_execution",
        'gaps.append(f"reconciled_open_order_missing:{client_order_id}")',
        "pass",
        "test_paper_execution.py::test_complete_reconciliation_keeps_checking_reconciled_open_orders",
    ),
)


def run(output: Path) -> int:
    repo = Path(__file__).resolve().parents[1]
    output.mkdir(parents=True, exist_ok=False)
    # Freeze the denominator before any test result. No adaptive sample replacement.
    (output / "matrix.json").write_text(json.dumps([asdict(m) for m in MUTATIONS], indent=2))
    results: list[dict[str, object]] = []
    with tempfile.TemporaryDirectory(prefix="market-impact-ablation-") as temporary:
        copy = Path(temporary)
        shutil.copytree(repo / "src", copy / "src", ignore=shutil.ignore_patterns("__pycache__"))
        for name in ("runtime", "schemas", "skills", "examples", "tests", "pyproject.toml"):
            (copy / name).symlink_to(repo / name)
        env = {k: os.environ[k] for k in ("PATH", "HOME", "TMPDIR") if k in os.environ}
        env.update(PYTHONDONTWRITEBYTECODE="1", PYTEST_DISABLE_PLUGIN_AUTOLOAD="1")
        bootstrap = (
            "import sys; from pathlib import Path; sys.path.insert(0, str(Path.cwd()/'src')); "
            "import market_impact_agent; "
            "assert Path(market_impact_agent.__file__).is_relative_to(Path.cwd()/'src'); "
            "import pytest; raise SystemExit(pytest.main(sys.argv[1:]))"
        )

        def execute(name: str, selectors: list[str]) -> dict[str, object]:
            xml = output / f"{name}.xml"
            start = time.monotonic()
            completed = subprocess.run(
                [
                    sys.executable,
                    "-c",
                    bootstrap,
                    "-q",
                    "--import-mode=importlib",
                    "-p",
                    "no:cacheprovider",
                    f"--junitxml={xml}",
                    *[str(repo / "tests" / s) for s in selectors],
                ],
                cwd=copy,
                env=env,
                capture_output=True,
                text=True,
                timeout=120,
            )
            (output / f"{name}.log").write_text(completed.stdout + completed.stderr)
            cases = ET.parse(xml).getroot().findall(".//testcase") if xml.exists() else []
            failures = [case for case in cases if case.find("failure") is not None]
            errors = [case for case in cases if case.find("error") is not None]
            return {
                "name": name,
                "exit_code": completed.returncode,
                "tests": len(cases),
                "failures": len(failures),
                "errors": len(errors),
                "skipped": sum(case.find("skipped") is not None for case in cases),
                "failure_messages": [case.find("failure").get("message", "") for case in failures],  # type: ignore[union-attr]
                "seconds": round(time.monotonic() - start, 3),
            }

        control = execute("control", list(dict.fromkeys(m.test for m in MUTATIONS)))
        results.append(control)
        if (
            control["exit_code"] == 0
            and control["tests"] == len({m.test for m in MUTATIONS})
            and not control["skipped"]
        ):
            for mutation in MUTATIONS:
                path = copy / "src" / "market_impact_agent" / f"{mutation.module}.py"
                original = path.read_text()
                if original.count(mutation.before) != mutation.occurrences:
                    raise ValueError(f"source drift at {mutation.name}; do not guess a replacement")
                changed = original.replace(mutation.before, mutation.after)
                try:
                    path.write_text(changed)
                    result = execute(mutation.name, [mutation.test])
                    result.update(
                        original_hash=sha256(original.encode()).hexdigest(),
                        mutated_hash=sha256(changed.encode()).hexdigest(),
                        detected=(
                            result["exit_code"] == 1
                            and result["tests"] == 1
                            and result["failures"] == 1
                            and result["errors"] == 0
                            and not result["skipped"]
                        ),
                    )
                    results.append(result)
                    print(json.dumps(result), flush=True)
                finally:
                    path.write_text(original)
    report = {
        "results": results,
        "model_requests": 0,
        "broker_requests": 0,
        "passed": len(results) == len(MUTATIONS) + 1
        and all(r.get("detected") for r in results[1:]),
    }
    (output / "report.json").write_text(json.dumps(report, indent=2))
    print(json.dumps(report), flush=True)
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(run(Path(sys.argv[1]).resolve()))
