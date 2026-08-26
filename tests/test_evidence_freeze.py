import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from market_impact_agent.accrual import AccrualDisposition, AccrualLedger
from market_impact_agent.agent_study import load_agent_phase2_preregistration
from market_impact_agent.evidence_freeze import freeze_due_evidence_packs
from market_impact_agent.source_coverage import load_source_coverage_registration
from tests.test_energy_monitor import build_monitor

REGISTRATION_PATH = Path("examples/calibration/agent-physical-energy-prospective-v1.json")
REGISTRY_PATH = Path("examples/research/a-share-energy-exposure-registry-v1.json")
COVERAGE_PATH = Path("examples/research/physical-energy-source-coverage-v1.json")
PATTERN_PATH = Path("examples/agent/energy_supply/pattern-pack.json")


def _accrued_ledger(tmp_path: Path):
    registration, registry = load_agent_phase2_preregistration(
        REGISTRATION_PATH,
        REGISTRY_PATH,
    )
    coverage = load_source_coverage_registration(COVERAGE_PATH)
    cycle = build_monitor(tmp_path / "poll").poll()
    observation = cycle.candidates[0]
    ledger = AccrualLedger(
        tmp_path / "ledger" / "ledger.sqlite3",
        registration=registration,
        registry=registry,
        coverage_registration=coverage,
        created_at=datetime(2026, 8, 26, 9, tzinfo=UTC),
    )
    decision = ledger.record(
        observation,
        recorded_at=cycle.receipt.cycle_completed_at,
        raw_source=cycle.raw_source_for(observation),
        coverage_receipt=cycle.receipt,
    )
    assert decision.disposition is AccrualDisposition.ACCRUED
    assert decision.evidence_cutoff_at is not None
    return ledger, registry, decision


def test_freezer_waits_for_exact_cutoff_then_writes_content_bound_bundle(
    tmp_path: Path,
) -> None:
    ledger, registry, decision = _accrued_ledger(tmp_path)
    assert decision.evidence_cutoff_at is not None
    output_root = tmp_path / "freezes"

    pending = freeze_due_evidence_packs(
        ledger=ledger,
        registry=registry,
        pattern_pack_paths=(PATTERN_PATH,),
        output_root=output_root,
        now=decision.evidence_cutoff_at - timedelta(microseconds=1),
    )
    completed = freeze_due_evidence_packs(
        ledger=ledger,
        registry=registry,
        pattern_pack_paths=(PATTERN_PATH,),
        output_root=output_root,
        now=decision.evidence_cutoff_at,
    )

    assert pending.frozen == ()
    assert pending.pending_event_ids == (decision.accrued_event_id,)
    assert len(completed.frozen) == 1
    bundle = completed.frozen[0]
    assert bundle.evidence_pack.as_of == decision.evidence_cutoff_at
    assert bundle.evidence_pack.event_id == decision.accrued_event_id
    assert len(bundle.evidence_pack.evidence) == 2
    assert bundle.evidence_pack.allowed_targets
    assert any("Bloomberg" in gap for gap in bundle.evidence_pack.data_gaps)
    assert bundle.manifest_path.is_file()
    assert bundle.manifest_path.stat().st_mode & 0o777 == 0o600
    manifest = json.loads(bundle.manifest_path.read_text(encoding="utf-8"))
    assert manifest["execution_capability"] == "none"
    assert manifest["evidence_cutoff_at"] == "2026-08-28T02:00:06Z"


def test_freezer_is_idempotent_and_revalidates_existing_manifest(tmp_path: Path) -> None:
    ledger, registry, decision = _accrued_ledger(tmp_path)
    assert decision.evidence_cutoff_at is not None
    output_root = tmp_path / "freezes"
    first = freeze_due_evidence_packs(
        ledger=ledger,
        registry=registry,
        pattern_pack_paths=(PATTERN_PATH,),
        output_root=output_root,
        now=decision.evidence_cutoff_at,
    )
    second = freeze_due_evidence_packs(
        ledger=ledger,
        registry=registry,
        pattern_pack_paths=(PATTERN_PATH,),
        output_root=output_root,
        now=decision.evidence_cutoff_at + timedelta(minutes=1),
    )

    assert first.frozen[0].already_existed is False
    assert second.frozen[0].already_existed is True
    manifest_path = first.frozen[0].manifest_path
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["prospective_registration_hash"] = "0" * 64
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    with pytest.raises(ValueError, match="manifest identity"):
        freeze_due_evidence_packs(
            ledger=ledger,
            registry=registry,
            pattern_pack_paths=(PATTERN_PATH,),
            output_root=output_root,
            now=decision.evidence_cutoff_at + timedelta(minutes=2),
        )
