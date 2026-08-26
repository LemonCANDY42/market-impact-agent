import json
import subprocess
import sys
import textwrap
from pathlib import Path
from typing import cast

import pytest

from market_impact_agent.cli import main, status_payload


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

        assert main(["status"]) == 0
        root = "examples/agent/energy_supply"
        result = main([
            "agent", "run",
            "--evidence-pack", f"{root}/evidence-pack.json",
            "--evidence-documents", f"{root}/evidence-documents.json",
            "--pattern-pack", f"{root}/pattern-pack.json",
            "--run-id", "missing-agent-dependency",
        ])
        assert result == 1
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
