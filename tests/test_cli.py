import json
from pathlib import Path
from typing import cast

import pytest

from market_impact_agent.cli import main, status_payload


def test_status_is_fail_closed() -> None:
    payload = status_payload()
    assert payload["live_trading"] == "disabled"
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
