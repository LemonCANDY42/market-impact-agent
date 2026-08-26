import json
import subprocess
import sys
import textwrap
from datetime import UTC, datetime
from pathlib import Path
from typing import cast

import pytest

from market_impact_agent.cli import main, status_payload
from market_impact_agent.energy_monitor import EnergyMonitorCycle, EnergySourceMonitor
from tests.test_energy_monitor import build_monitor


def test_status_is_fail_closed() -> None:
    payload = status_payload()
    assert payload["live_trading"] == "disabled"
    assert payload["agent_runtime"] == {
        "status": "accepted_local_research_v2",
        "provider": "minimax-openai-compatible",
        "model": "MiniMax-M3",
        "tool_authority": "read_only",
        "broker_reachability": False,
        "provider_portability": "not_established",
    }
    providers = payload["providers"]
    assert isinstance(providers, list)
    assert providers[0]["provider_id"] == "mock-execution"
    observation_providers = payload["observation_providers"]
    assert isinstance(observation_providers, list)
    observation_manifests = cast(list[dict[str, object]], observation_providers)
    assert {manifest["provider_id"] for manifest in observation_manifests} == {
        "kalshi-public",
        "polymarket-public",
        "world-monitor-predictions",
    }
    assert all(manifest["enabled"] is False for manifest in observation_manifests)


def test_base_install_cli_imports_without_mcp_and_agent_run_reports_optional_dependency() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    script = textwrap.dedent(
        """
        import importlib.abc
        import sys

        class BlockMcp(importlib.abc.MetaPathFinder):
            def find_spec(self, fullname, path, target=None):
                if fullname == "mcp" or fullname.startswith("mcp."):
                    raise ModuleNotFoundError("No module named 'mcp'", name="mcp")
                return None

        sys.meta_path.insert(0, BlockMcp())
        from market_impact_agent.cli import main
        from market_impact_agent.judgment_replay import JudgmentReplaySpec

        assert main(["status"]) == 0
        assert JudgmentReplaySpec is not None
        root = "examples/agent/energy_supply"
        result = main([
            "agent", "run",
            "--evidence-pack", f"{root}/evidence-pack.json",
            "--evidence-documents", f"{root}/evidence-documents.json",
            "--pattern-pack", f"{root}/pattern-pack.json",
            "--run-id", "missing-agent-dependency",
        ])
        assert result == 1
        ensemble_result = main([
            "agent", "study-run-ensemble",
            "--registration", "examples/calibration/agent-physical-energy-prospective-v1.json",
            "--exposure-registry", "examples/research/a-share-energy-exposure-registry-v1.json",
            "--evidence-pack", f"{root}/evidence-pack.json",
            "--evidence-documents", f"{root}/evidence-documents.json",
            "--pattern-pack", f"{root}/pattern-pack.json",
            "--ensemble-run-id", "missing-ensemble-dependency",
        ])
        assert ensemble_result == 1
        """
    )
    result = subprocess.run(
        [sys.executable, "-c", script],
        cwd=repo_root,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    assert "install market-impact-agent[agent]" in result.stderr


def test_validate_provider_command(tmp_path: Path) -> None:
    source = Path("examples/providers/veighna-external-bridge.json")
    target = tmp_path / "provider.json"
    target.write_text(source.read_text(encoding="utf-8"), encoding="utf-8")

    assert main(["provider", "validate", str(target)]) == 0


def test_validate_provider_reports_parse_failure(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    target = tmp_path / "provider.json"
    target.write_text("not-json", encoding="utf-8")

    assert main(["provider", "validate", str(target)]) == 1
    captured = capsys.readouterr()
    assert json.loads(captured.err)["valid"] is False


def test_validate_provider_rejects_malicious_live_manifest(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    target = tmp_path / "malicious-live.json"
    target.write_text(
        json.dumps(
            {
                "schema_version": "market-impact.provider-manifest.v1",
                "provider_id": "malicious-live",
                "provider_version": "1.0.0",
                "transport": "http",
                "environments": ["live"],
                "declared_capabilities": ["live_execution"],
                "verified_capabilities": ["live_execution"],
                "markets": ["US"],
                "order_types": ["market"],
                "supports_streaming": "false",
                "supports_reconciliation": "false",
                "enabled": "true",
                "trust_tier": "live_validated",
            }
        ),
        encoding="utf-8",
    )

    assert main(["provider", "validate", str(target)]) == 1
    captured = capsys.readouterr()
    assert "must be a boolean" in json.loads(captured.err)["error"]


def test_validate_event_rejects_future_evidence(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    source = Path("examples/events/synthetic-energy-supply-shock.json")
    payload = json.loads(source.read_text(encoding="utf-8"))
    payload["envelope"]["evidence"][0]["visible_at"] = "2026-08-24T02:06:00Z"
    payload["envelope"]["evidence"][0]["retrieved_at"] = "2026-08-24T02:07:00Z"
    target = tmp_path / "future-evidence.json"
    target.write_text(json.dumps(payload), encoding="utf-8")

    assert main(["event", "validate", str(target)]) == 1
    captured = capsys.readouterr()
    result = json.loads(captured.out)
    assert result["errors"] == ["envelope.evidence[0].visible_at must not be after envelope.as_of"]


def test_validate_event_rejects_invalid_archetype_and_missing_required_field(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    source = Path("examples/events/synthetic-energy-supply-shock.json")
    payload = json.loads(source.read_text(encoding="utf-8"))
    payload["event_archetype"] = "unsupported_archetype"
    del payload["envelope"]["evidence"][0]["claim_hash"]
    target = tmp_path / "invalid-event.json"
    target.write_text(json.dumps(payload), encoding="utf-8")

    assert main(["event", "validate", str(target)]) == 1

    result = json.loads(capsys.readouterr().out)
    assert any(error.startswith("$.event_archetype:") for error in result["errors"])
    assert any("'claim_hash' is a required property" in error for error in result["errors"])


def test_archive_verify_rejects_unbound_locator_before_network(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    locator = tmp_path / "locator.json"
    locator.write_text(
        json.dumps(
            {
                "schema_version": "market-impact.common-crawl-locator.v1",
                "collection": "CC-MAIN-2025-43",
                "target_url": "https://commoncrawl.org/get-started",
                "timestamp": "20251016192109",
                "filename": ("crawl-data/CC-MAIN-2025-43/segments/fixed/warc/record.warc.gz"),
                "offset": 1,
                "length": 10,
                "digest": "sha1:" + "A" * 32,
                "http_status": 200,
                "source_version_id": "common-crawl-record-" + "0" * 64,
            }
        ),
        encoding="utf-8",
    )

    assert main(["archive", "common-crawl-verify", "--locator", str(locator)]) == 1

    result = json.loads(capsys.readouterr().err)
    assert result["verified"] is False
    assert "source_version_id does not match content" in result["error"]


def test_tushare_capture_requires_environment_token_before_creating_local_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("TUSHARE_TOKEN", raising=False)

    result = main(
        [
            "tushare",
            "capture",
            "--instrument",
            "600028.SH",
            "--as-of-date",
            "20190918",
            "--start-date",
            "20190919",
            "--end-date",
            "20191010",
        ]
    )

    assert result == 1
    assert json.loads(capsys.readouterr().err) == {
        "captured": False,
        "error": "TUSHARE_TOKEN is not configured",
    }
    assert not (tmp_path / ".market-impact").exists()


def test_world_monitor_capture_requires_environment_key_before_creating_local_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("WORLD_MONITOR_API_KEY", raising=False)

    result = main(
        [
            "prediction",
            "capture",
            "--provider",
            "world-monitor",
            "--limit",
            "1",
        ]
    )

    assert result == 1
    assert json.loads(capsys.readouterr().err) == {
        "captured": False,
        "error": "WORLD_MONITOR_API_KEY is not configured",
    }
    assert not (tmp_path / ".market-impact").exists()


def test_phase2_gate_command_fails_closed_on_invalid_evidence(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    evidence = tmp_path / "invalid-evidence.json"
    evidence.write_text("{}", encoding="utf-8")

    result = main(["backtest", "phase2-gate", "--evidence", str(evidence)])

    assert result == 1
    payload = json.loads(capsys.readouterr().err)
    assert payload["accepted"] is False
    assert "closed contract" in payload["error"]


def test_agent_validate_accepts_content_bound_synthetic_fixture(
    capsys: pytest.CaptureFixture[str],
) -> None:
    root = Path("examples/agent/energy_supply")

    result = main(
        [
            "agent",
            "validate",
            "--evidence-pack",
            str(root / "evidence-pack.json"),
            "--evidence-documents",
            str(root / "evidence-documents.json"),
            "--pattern-pack",
            str(root / "pattern-pack.json"),
        ]
    )

    assert result == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["valid"] is True
    assert payload["evidence_count"] == 4
    assert payload["pattern_pack_count"] == 1


def test_agent_validate_rejects_tampered_evidence(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    root = Path("examples/agent/energy_supply")
    documents = json.loads((root / "evidence-documents.json").read_text(encoding="utf-8"))
    documents["documents"]["official-outage"]["fact"] = "tampered"
    tampered = tmp_path / "tampered.json"
    tampered.write_text(json.dumps(documents), encoding="utf-8")

    result = main(
        [
            "agent",
            "validate",
            "--evidence-pack",
            str(root / "evidence-pack.json"),
            "--evidence-documents",
            str(tampered),
            "--pattern-pack",
            str(root / "pattern-pack.json"),
        ]
    )

    assert result == 1
    payload = json.loads(capsys.readouterr().err)
    assert "content hash mismatch" in payload["error"]


def test_method_benchmark_validate_reports_retired_v1_as_audit_only(
    capsys: pytest.CaptureFixture[str],
) -> None:
    result = main(
        [
            "agent",
            "method-benchmark-validate",
            "--registration",
            "examples/calibration/method-quality-benchmark-v1.json",
            "--method-catalog",
            "examples/research/research-method-catalog-v2.json",
            "--provider-profile",
            "examples/providers/minimax-m3-research-v1.json",
            "--evaluation-specification",
            "examples/calibration/method-quality-evaluation-specification-v1.json",
            "--historical-manifest",
            "examples/research/synthetic-energy-historical-evidence-v1.json",
            "--evidence-pack",
            "examples/agent/energy_supply/evidence-pack.json",
            "--evidence-documents",
            "examples/agent/energy_supply/evidence-documents.json",
            "--masked-input-manifest",
            "examples/research/synthetic-energy-masked-input-manifest-v1.json",
            "--masked-evidence-pack",
            "examples/agent/energy_supply/masked-evidence-pack.json",
            "--masked-evidence-documents",
            "examples/agent/energy_supply/masked-evidence-documents.json",
            "--pattern-pack",
            "examples/agent/energy_supply/pattern-pack.json",
            "--masked-pattern-pack",
            "examples/agent/energy_supply/masked-pattern-pack.json",
            "--skill-root",
            "skills",
        ]
    )

    assert result == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["valid"] is False
    assert payload["audit_valid"] is True
    assert payload["claim_eligible"] is False
    assert payload["validation_status"] == "retired_v1_audit_only"
    assert payload["retrospective_holdout_case_count"] == 24
    assert payload["case_split"] == "development"
    assert payload["provenance_trust_status"] == "synthetic_contract_only"
    assert payload["source_authentication"] == "not_available_in_v1"
    assert payload["retrospective_holdout_admission"] == "unavailable_in_v1"
    assert payload["masked_evidence_pack_id"].startswith("evidence-pack-")
    assert payload["outcomes_opened"] is False
    assert payload["execution_capability"] == "none"


def test_method_benchmark_validate_accepts_cluster_corrected_v2_protocol(
    capsys: pytest.CaptureFixture[str],
) -> None:
    result = main(
        [
            "agent",
            "method-benchmark-validate",
            "--registration",
            "examples/calibration/method-quality-benchmark-v2.json",
            "--method-catalog",
            "examples/research/research-method-catalog-v2.json",
            "--provider-profile",
            "examples/providers/minimax-m3-research-v1.json",
            "--evaluation-specification",
            "examples/calibration/method-quality-evaluation-specification-v2.json",
            "--historical-manifest",
            "examples/research/synthetic-energy-historical-evidence-v1.json",
            "--evidence-pack",
            "examples/agent/energy_supply/evidence-pack.json",
            "--evidence-documents",
            "examples/agent/energy_supply/evidence-documents.json",
            "--masked-input-manifest",
            "examples/research/synthetic-energy-masked-input-manifest-v1.json",
            "--masked-evidence-pack",
            "examples/agent/energy_supply/masked-evidence-pack.json",
            "--masked-evidence-documents",
            "examples/agent/energy_supply/masked-evidence-documents.json",
            "--pattern-pack",
            "examples/agent/energy_supply/pattern-pack.json",
            "--masked-pattern-pack",
            "examples/agent/energy_supply/masked-pattern-pack.json",
            "--skill-root",
            "skills",
        ]
    )

    assert result == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["valid"] is True
    assert payload["independent_statistical_unit"] == "event_case"
    assert payload["source_authentication"] == "not_available_for_supplied_case"
    assert payload["retrospective_holdout_admission"] == (
        "unavailable_until_publisher_time_and_latency_acceptance"
    )
    assert payload["outcomes_opened"] is False
    assert payload["execution_capability"] == "none"


def test_agent_study_validate_accepts_frozen_prospective_registration(
    capsys: pytest.CaptureFixture[str],
) -> None:
    result = main(
        [
            "agent",
            "study-validate",
            "--registration",
            "examples/calibration/agent-physical-energy-prospective-v1.json",
            "--exposure-registry",
            "examples/research/a-share-energy-exposure-registry-v1.json",
            "--source-coverage-registration",
            "examples/research/physical-energy-source-coverage-v1.json",
        ]
    )

    assert result == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["valid"] is True
    assert payload["replicate_count"] == 5
    assert payload["target_event_count"] == 5
    assert payload["selection_eligible_target_count"] == 2
    assert payload["all_event_denominator"] is True
    assert payload["holdout_outcomes_opened"] is False
    assert payload["execution_capability"] == "none"


def test_agent_study_validate_rejects_event_deletion_policy(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    source = Path("examples/calibration/agent-physical-energy-prospective-v1.json")
    payload = json.loads(source.read_text(encoding="utf-8"))
    payload["event_eligibility"]["missing_critical_data_action"] = "drop_event"
    registration = tmp_path / "invalid-registration.json"
    registration.write_text(json.dumps(payload), encoding="utf-8")

    result = main(
        [
            "agent",
            "study-validate",
            "--registration",
            str(registration),
            "--exposure-registry",
            "examples/research/a-share-energy-exposure-registry-v1.json",
            "--source-coverage-registration",
            "examples/research/physical-energy-source-coverage-v1.json",
        ]
    )

    assert result == 1
    output = json.loads(capsys.readouterr().out)
    assert output["valid"] is False
    assert any("retain_and_abstain" in error for error in output["errors"])


def test_source_poll_and_due_freeze_commands_complete_the_research_path(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cycle = build_monitor(tmp_path / "fixture").poll()

    def fixed_poll(
        self: EnergySourceMonitor,
        *,
        latest_observations: object = None,
    ) -> EnergyMonitorCycle:
        del self, latest_observations
        return cycle

    class FixedClock:
        @staticmethod
        def now(timezone: object) -> datetime:
            del timezone
            return datetime(2026, 8, 28, 2, 0, 6, tzinfo=UTC)

    monkeypatch.setattr(EnergySourceMonitor, "poll", fixed_poll)
    monkeypatch.setattr("market_impact_agent.cli.datetime", FixedClock)
    ledger = tmp_path / "ledger.sqlite3"
    common = [
        "--registration",
        "examples/calibration/agent-physical-energy-prospective-v1.json",
        "--exposure-registry",
        "examples/research/a-share-energy-exposure-registry-v1.json",
        "--source-coverage-registration",
        "examples/research/physical-energy-source-coverage-v1.json",
    ]

    poll_result = main(
        [
            "agent",
            "study-source-poll",
            *common,
            "--ledger",
            str(ledger),
            "--monitor-root",
            str(tmp_path / "monitor"),
        ]
    )
    poll_payload = json.loads(capsys.readouterr().out)
    freeze_result = main(
        [
            "agent",
            "study-freeze-due",
            *common,
            "--ledger",
            str(ledger),
            "--pattern-pack",
            "examples/agent/energy_supply/pattern-pack.json",
            "--output-root",
            str(tmp_path / "evidence"),
        ]
    )
    freeze_payload = json.loads(capsys.readouterr().out)

    assert poll_result == 0
    assert poll_payload["coverage_complete"] is True
    assert poll_payload["candidate_count"] == 1
    assert poll_payload["decisions"][0]["disposition"] == "accrued"
    assert freeze_result == 0
    assert freeze_payload["frozen_count"] == 1
    assert freeze_payload["execution_capability"] == "none"


def test_source_poll_command_returns_failure_and_retains_candidate_on_coverage_gap(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cycle = build_monitor(
        tmp_path / "fixture",
        fail_provider="gdelt-energy-discovery",
    ).poll()

    def fixed_poll(
        self: EnergySourceMonitor,
        *,
        latest_observations: object = None,
    ) -> EnergyMonitorCycle:
        del self, latest_observations
        return cycle

    monkeypatch.setattr(EnergySourceMonitor, "poll", fixed_poll)
    result = main(
        [
            "agent",
            "study-source-poll",
            "--registration",
            "examples/calibration/agent-physical-energy-prospective-v1.json",
            "--exposure-registry",
            "examples/research/a-share-energy-exposure-registry-v1.json",
            "--source-coverage-registration",
            "examples/research/physical-energy-source-coverage-v1.json",
            "--ledger",
            str(tmp_path / "ledger.sqlite3"),
            "--monitor-root",
            str(tmp_path / "monitor"),
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert result == 1
    assert payload["coverage_complete"] is False
    assert payload["candidate_count"] == 1
    assert payload["decisions"][0]["reasons"] == ["source_coverage_incomplete"]
