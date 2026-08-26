import json
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from pathlib import Path

import pytest

from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.agent_schema import validate_agent_contract
from market_impact_agent.source_coverage import (
    CoverageAttempt,
    CoverageReceipt,
    coverage_receipt_from_dict,
    load_source_coverage_registration,
    source_coverage_registration_from_dict,
)

COVERAGE_PATH = Path("examples/research/physical-energy-source-coverage-v1.json")


def _receipt(*, failed_provider: str | None = None) -> CoverageReceipt:
    registration = load_source_coverage_registration(COVERAGE_PATH)
    started_at = datetime(2026, 8, 28, 1, tzinfo=UTC)
    attempts: list[CoverageAttempt] = []
    for index, source in enumerate(registration.sources):
        requested_at = started_at + timedelta(seconds=index * 2)
        failed = source.provider_id == failed_provider
        attempts.append(
            CoverageAttempt(
                provider_id=source.provider_id,
                requested_at=requested_at,
                retrieved_at=None if failed else requested_at + timedelta(seconds=1),
                succeeded=not failed,
                content_hash=(
                    None if failed else sha256(f"raw-{source.provider_id}".encode()).hexdigest()
                ),
                record_count=None if failed else index,
                error_class="TimeoutError" if failed else None,
                error_summary="source timed out" if failed else None,
            )
        )
    core = {
        "schema_version": "market-impact.coverage-receipt.v1",
        "coverage_registration_id": registration.coverage_registration_id,
        "coverage_registration_hash": registration.coverage_registration_hash,
        "cycle_started_at": "2026-08-28T01:00:00Z",
        "cycle_completed_at": "2026-08-28T01:00:08Z",
        "attempts": [item.to_dict() for item in attempts],
    }
    return CoverageReceipt(
        receipt_id=f"coverage-receipt-{canonical_hash(core)}",
        coverage_registration_id=registration.coverage_registration_id,
        coverage_registration_hash=registration.coverage_registration_hash,
        cycle_started_at=started_at,
        cycle_completed_at=started_at + timedelta(seconds=8),
        attempts=tuple(attempts),
    )


def test_frozen_source_coverage_is_canonical_and_schema_valid() -> None:
    payload = json.loads(COVERAGE_PATH.read_text(encoding="utf-8"))
    registration = source_coverage_registration_from_dict(payload)

    assert validate_agent_contract(payload, "source-coverage-registration.schema.json") == ()
    assert registration.to_dict() == payload
    assert registration.sources[-1].occurrence_eligible is True
    assert registration.known_blind_spots


def test_coverage_receipt_distinguishes_complete_and_failed_cycles() -> None:
    registration = load_source_coverage_registration(COVERAGE_PATH)
    complete = _receipt()
    mandatory_failure = _receipt(failed_provider="gdelt-energy-discovery")
    optional_failure = _receipt(failed_provider="eia-today-in-energy")

    assert complete.is_complete(registration) is True
    assert mandatory_failure.is_complete(registration) is False
    assert optional_failure.is_complete(registration) is True
    assert validate_agent_contract(complete.to_dict(), "coverage-receipt.schema.json") == ()
    assert coverage_receipt_from_dict(complete.to_dict()) == complete


def test_source_coverage_and_receipt_identity_fail_closed_on_tampering() -> None:
    coverage_payload = json.loads(COVERAGE_PATH.read_text(encoding="utf-8"))
    coverage_payload["polling_interval_minutes"] = 11
    with pytest.raises(ValueError, match="does not match content"):
        source_coverage_registration_from_dict(coverage_payload)

    receipt_payload = _receipt().to_dict()
    receipt_payload["cycle_completed_at"] = "2026-08-28T01:00:09Z"
    with pytest.raises(ValueError, match="does not match content"):
        coverage_receipt_from_dict(receipt_payload)
