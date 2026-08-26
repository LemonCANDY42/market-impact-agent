import json
import os
import sqlite3
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from pathlib import Path
from typing import TypedDict, Unpack, cast

import pytest

from market_impact_agent.accrual import (
    AccrualDecision,
    AccrualDisposition,
    AccrualLedger,
    AccrualReason,
    CandidateEventObservation,
    candidate_event_observation_from_dict,
)
from market_impact_agent.agent_contracts import canonical_hash
from market_impact_agent.agent_schema import validate_agent_contract
from market_impact_agent.agent_study import load_agent_phase2_preregistration
from market_impact_agent.cli import (
    main,
    observe_agent_phase2_study,
    validate_agent_phase2_ledger,
)
from market_impact_agent.source_coverage import (
    CoverageAttempt,
    CoverageReceipt,
    load_source_coverage_registration,
)

REGISTRATION_PATH = Path("examples/calibration/agent-physical-energy-prospective-v1.json")
REGISTRY_PATH = Path("examples/research/a-share-energy-exposure-registry-v1.json")
COVERAGE_PATH = Path("examples/research/physical-energy-source-coverage-v1.json")
BASE_AVAILABLE_AT = datetime(2026, 8, 28, 1, tzinfo=UTC)


class ObservationOverrides(TypedDict, total=False):
    event_id: str
    available_at: datetime
    occurred_at: datetime | None
    source_tier: str
    event_nature: str
    commodity: str
    loss_amount: str
    loss_unit: str
    duration: str
    supersedes: str | None
    revision: str
    coverage_complete: bool


def _receipt_for(
    *,
    available_at: datetime,
    provider_id: str,
    raw_content_hash: str,
    complete: bool = True,
) -> CoverageReceipt:
    coverage = load_source_coverage_registration(COVERAGE_PATH)
    attempts: list[CoverageAttempt] = []
    for source in coverage.sources:
        succeeds = complete or source.provider_id != "gdelt-energy-discovery"
        content_hash = (
            raw_content_hash
            if source.provider_id == provider_id
            else sha256(f"raw-{source.provider_id}-{available_at.isoformat()}".encode()).hexdigest()
        )
        attempts.append(
            CoverageAttempt(
                provider_id=source.provider_id,
                requested_at=available_at - timedelta(seconds=5),
                retrieved_at=available_at if succeeds else None,
                succeeded=succeeds,
                content_hash=content_hash if succeeds else None,
                record_count=1 if succeeds else None,
                error_class=None if succeeds else "TimeoutError",
                error_summary=None if succeeds else "synthetic timeout",
            )
        )
    core = {
        "schema_version": "market-impact.coverage-receipt.v1",
        "coverage_registration_id": coverage.coverage_registration_id,
        "coverage_registration_hash": coverage.coverage_registration_hash,
        "cycle_started_at": _timestamp(available_at - timedelta(seconds=5)),
        "cycle_completed_at": _timestamp(available_at + timedelta(seconds=1)),
        "attempts": [item.to_dict() for item in attempts],
    }
    return CoverageReceipt(
        receipt_id=f"coverage-receipt-{canonical_hash(core)}",
        coverage_registration_id=coverage.coverage_registration_id,
        coverage_registration_hash=coverage.coverage_registration_hash,
        cycle_started_at=available_at - timedelta(seconds=5),
        cycle_completed_at=available_at + timedelta(seconds=1),
        attempts=tuple(attempts),
    )


def _observation_payload(
    *,
    event_id: str = "event-1",
    available_at: datetime = BASE_AVAILABLE_AT,
    occurred_at: datetime | None = None,
    source_tier: str = "primary",
    event_nature: str = "physical_production_loss",
    commodity: str = "crude_oil",
    loss_amount: str = "600000",
    loss_unit: str = "boe_per_day",
    duration: str = "48",
    supersedes: str | None = None,
    revision: str = "v1",
    coverage_complete: bool = True,
) -> dict[str, object]:
    resolved_occurred_at = available_at - timedelta(hours=1) if occurred_at is None else occurred_at
    published_at = available_at - timedelta(minutes=55)
    claim_summary = f"{event_id} {revision} point-in-time physical loss estimate"
    provider_id = "gdelt-energy-discovery" if source_tier == "established_news" else "entsog-umm"
    raw_hash = sha256(f"raw-{event_id}-{revision}".encode()).hexdigest()
    receipt = _receipt_for(
        available_at=available_at,
        provider_id=provider_id,
        raw_content_hash=raw_hash,
        complete=coverage_complete,
    )
    coverage = load_source_coverage_registration(COVERAGE_PATH)
    payload: dict[str, object] = {
        "schema_version": "market-impact.candidate-event-observation.v1",
        "event_id": event_id,
        "source_coverage_registration_id": coverage.coverage_registration_id,
        "source_coverage_registration_hash": coverage.coverage_registration_hash,
        "coverage_receipt_id": receipt.receipt_id,
        "coverage_receipt_hash": receipt.receipt_hash,
        "event_nature": event_nature,
        "affected_commodity": commodity,
        "loss_amount": loss_amount,
        "loss_unit": loss_unit,
        "regional_denominator_source_ref": None,
        "regional_denominator_source_tier": None,
        "regional_denominator_available_at": None,
        "regional_denominator_raw_content_hash": None,
        "expected_duration_hours": duration,
        "source": {
            "provider_id": provider_id,
            "upstream_source": "synthetic-operator",
            "upstream_record_id": f"{event_id}-{revision}",
            "source_ref": f"https://operator.example/{event_id}/{revision}",
            "source_tier": source_tier,
            "occurred_at": _timestamp(resolved_occurred_at),
            "published_at": _timestamp(published_at),
            "source_updated_at": _timestamp(published_at),
            "available_at": _timestamp(available_at),
            "retrieved_at": _timestamp(available_at),
            "availability_basis": "actual_receipt",
            "raw_content_hash": raw_hash,
            "claim_summary": claim_summary,
            "claim_hash": sha256(claim_summary.encode()).hexdigest(),
        },
        "supersedes_observation_id": supersedes,
    }
    payload["observation_id"] = f"candidate-observation-{canonical_hash(payload)}"
    return payload


def _observation(**kwargs: Unpack[ObservationOverrides]) -> CandidateEventObservation:
    return candidate_event_observation_from_dict(_observation_payload(**kwargs))


def _ledger(tmp_path: Path) -> AccrualLedger:
    registration, registry = load_agent_phase2_preregistration(
        REGISTRATION_PATH,
        REGISTRY_PATH,
    )
    coverage = load_source_coverage_registration(COVERAGE_PATH)
    return AccrualLedger(
        tmp_path / "accrual" / "ledger.sqlite3",
        registration=registration,
        registry=registry,
        coverage_registration=coverage,
        created_at=datetime(2026, 8, 26, 8, tzinfo=UTC),
    )


def _record(
    ledger: AccrualLedger,
    observation: CandidateEventObservation,
    *,
    recorded_at: datetime,
) -> AccrualDecision:
    receipt = _receipt_for(
        available_at=observation.source.retrieved_at,
        provider_id=observation.source.provider_id,
        raw_content_hash=observation.source.raw_content_hash,
    )
    return ledger.record(
        observation,
        recorded_at=recorded_at,
        raw_source=f"raw-{observation.source.upstream_record_id}".encode(),
        coverage_receipt=receipt,
    )


def test_candidate_observation_schema_and_typed_parser_accept_canonical_payload() -> None:
    payload = _observation_payload()

    assert validate_agent_contract(payload, "candidate-event-observation.schema.json") == ()
    observation = candidate_event_observation_from_dict(payload)

    assert observation.observation_id == observation.expected_observation_id
    assert observation.source.available_at == observation.source.retrieved_at
    assert observation.to_dict() == payload


def test_qualifying_observation_accrues_with_private_hash_chained_ledger(
    tmp_path: Path,
) -> None:
    ledger = _ledger(tmp_path)
    observation = _observation()

    decision = _record(
        ledger,
        observation,
        recorded_at=observation.source.retrieved_at + timedelta(minutes=1),
    )

    assert decision.disposition is AccrualDisposition.ACCRUED
    assert decision.reasons == ()
    assert decision.qualifying_visible_at == BASE_AVAILABLE_AT
    assert decision.evidence_cutoff_at == BASE_AVAILABLE_AT + timedelta(minutes=60)
    assert decision.accrued_event_id is not None
    assert ledger.accrued_event_count == 1
    assert ledger.ledger_hash == decision.decision_hash
    assert ledger.path.stat().st_mode & 0o777 == 0o600
    assert ledger.path.parent.stat().st_mode & 0o777 == 0o700
    raw_artifact = ledger.source_artifacts.root / observation.source.raw_content_hash
    assert raw_artifact.is_file()
    assert raw_artifact.stat().st_mode & 0o777 == 0o600


def test_mandatory_source_failure_blocks_accrual_and_cannot_be_waited_out(
    tmp_path: Path,
) -> None:
    ledger = _ledger(tmp_path)
    incomplete = _observation(coverage_complete=False)
    incomplete_receipt = _receipt_for(
        available_at=incomplete.source.retrieved_at,
        provider_id=incomplete.source.provider_id,
        raw_content_hash=incomplete.source.raw_content_hash,
        complete=False,
    )
    first = ledger.record(
        incomplete,
        recorded_at=incomplete.source.retrieved_at + timedelta(minutes=1),
        raw_source=b"raw-event-1-v1",
        coverage_receipt=incomplete_receipt,
    )
    later = _observation(
        available_at=BASE_AVAILABLE_AT + timedelta(hours=1),
        occurred_at=incomplete.source.occurred_at,
        supersedes=incomplete.observation_id,
        revision="v2",
    )
    second = _record(
        ledger,
        later,
        recorded_at=later.source.retrieved_at + timedelta(minutes=1),
    )

    assert first.reasons == (AccrualReason.SOURCE_COVERAGE_INCOMPLETE,)
    assert second.reasons == (AccrualReason.SOURCE_COVERAGE_INCOMPLETE,)
    assert ledger.accrued_event_count == 0


def test_nonqualifying_news_can_be_superseded_by_later_official_confirmation(
    tmp_path: Path,
) -> None:
    ledger = _ledger(tmp_path)
    discovery = _observation(source_tier="established_news")
    first = _record(
        ledger,
        discovery,
        recorded_at=discovery.source.retrieved_at + timedelta(minutes=1),
    )
    confirmation = _observation(
        source_tier="primary",
        available_at=BASE_AVAILABLE_AT + timedelta(hours=1),
        occurred_at=discovery.source.occurred_at,
        supersedes=discovery.observation_id,
        revision="v2",
    )
    second = _record(
        ledger,
        confirmation,
        recorded_at=confirmation.source.retrieved_at + timedelta(minutes=1),
    )
    duplicate = _record(
        ledger,
        confirmation,
        recorded_at=confirmation.source.retrieved_at + timedelta(minutes=2),
    )

    assert first.disposition is AccrualDisposition.NOT_ACCRUED
    assert first.reasons == (AccrualReason.SOURCE_TIER_NOT_QUALIFYING,)
    assert second.disposition is AccrualDisposition.ACCRUED
    assert duplicate == second
    assert len(ledger.decisions()) == 2


def test_missing_critical_data_is_retained_and_a_revision_can_fill_it(
    tmp_path: Path,
) -> None:
    ledger = _ledger(tmp_path)
    missing_payload = _observation_payload()
    missing_payload["affected_commodity"] = None
    missing_payload["loss_amount"] = None
    missing_payload["loss_unit"] = None
    missing_payload["expected_duration_hours"] = None
    source = cast(dict[str, object], missing_payload["source"])
    source["occurred_at"] = None
    missing_payload["observation_id"] = (
        f"candidate-observation-{canonical_hash(_without_id(missing_payload))}"
    )
    missing = candidate_event_observation_from_dict(missing_payload)

    first = _record(
        ledger,
        missing,
        recorded_at=missing.source.retrieved_at + timedelta(minutes=1),
    )
    complete = _observation(
        available_at=BASE_AVAILABLE_AT + timedelta(hours=1),
        occurred_at=BASE_AVAILABLE_AT,
        supersedes=missing.observation_id,
        revision="v2",
    )
    second = _record(
        ledger,
        complete,
        recorded_at=complete.source.retrieved_at + timedelta(minutes=1),
    )

    assert first.disposition is AccrualDisposition.NOT_ACCRUED
    assert first.reasons == (AccrualReason.MISSING_CRITICAL_DATA,)
    assert second.disposition is AccrualDisposition.ACCRUED
    assert ledger.accrued_event_count == 1


def test_loss_amount_and_unit_must_be_missing_together() -> None:
    payload = _observation_payload()
    payload["loss_unit"] = None
    payload["observation_id"] = f"candidate-observation-{canonical_hash(_without_id(payload))}"

    assert validate_agent_contract(payload, "candidate-event-observation.schema.json")
    with pytest.raises(ValueError, match="present or missing together"):
        candidate_event_observation_from_dict(payload)


@pytest.mark.parametrize(
    ("changes", "reason"),
    [
        ({"event_nature": "policy_only"}, AccrualReason.EVENT_NATURE_EXCLUDED),
        ({"loss_amount": "499999"}, AccrualReason.LOSS_THRESHOLD_NOT_MET),
        ({"duration": "23.9"}, AccrualReason.DURATION_THRESHOLD_NOT_MET),
        ({"commodity": "coal"}, AccrualReason.UNSUPPORTED_COMMODITY),
        ({"source_tier": "established_news"}, AccrualReason.SOURCE_TIER_NOT_QUALIFYING),
        (
            {"available_at": datetime(2026, 8, 26, 23, tzinfo=UTC)},
            AccrualReason.OUTSIDE_ACCRUAL_WINDOW,
        ),
    ],
)
def test_nonqualifying_observations_are_retained_with_explicit_reasons(
    tmp_path: Path,
    changes: ObservationOverrides,
    reason: AccrualReason,
) -> None:
    ledger = _ledger(tmp_path)
    observation = _observation(**changes)

    decision = _record(
        ledger,
        observation,
        recorded_at=observation.source.retrieved_at + timedelta(minutes=1),
    )

    assert decision.disposition is AccrualDisposition.NOT_ACCRUED
    assert reason in decision.reasons
    assert ledger.decisions() == (decision,)


def test_separation_rule_and_cohort_limit_are_replayed_from_frozen_history(
    tmp_path: Path,
) -> None:
    ledger = _ledger(tmp_path)
    first = _observation(event_id="event-1", available_at=BASE_AVAILABLE_AT)
    first_decision = _record(
        ledger,
        first,
        recorded_at=first.source.retrieved_at + timedelta(minutes=1),
    )
    too_close = _observation(
        event_id="event-2",
        available_at=BASE_AVAILABLE_AT + timedelta(days=5),
    )
    close_decision = _record(
        ledger,
        too_close,
        recorded_at=too_close.source.retrieved_at + timedelta(minutes=1),
    )

    assert first_decision.disposition is AccrualDisposition.ACCRUED
    assert close_decision.reasons == (AccrualReason.SEPARATION_WINDOW_NOT_MET,)

    for index in range(2, 6):
        observation = _observation(
            event_id=f"event-{index + 1}",
            available_at=BASE_AVAILABLE_AT + timedelta(days=11 * index),
        )
        _record(
            ledger,
            observation,
            recorded_at=observation.source.retrieved_at + timedelta(minutes=1),
        )

    sixth = _observation(
        event_id="event-7",
        available_at=BASE_AVAILABLE_AT + timedelta(days=66),
    )
    sixth_decision = _record(
        ledger,
        sixth,
        recorded_at=sixth.source.retrieved_at + timedelta(minutes=1),
    )

    assert ledger.accrued_event_count == 5
    assert AccrualReason.COHORT_FULL in sixth_decision.reasons


def test_late_revision_cannot_wait_out_event_occurrence_separation(tmp_path: Path) -> None:
    ledger = _ledger(tmp_path)
    first = _observation(event_id="event-1")
    _record(
        ledger,
        first,
        recorded_at=first.source.retrieved_at + timedelta(minutes=1),
    )
    close_occurrence = BASE_AVAILABLE_AT + timedelta(days=5)
    close = _observation(
        event_id="event-2",
        available_at=close_occurrence + timedelta(hours=1),
        occurred_at=close_occurrence,
    )
    close_decision = _record(
        ledger,
        close,
        recorded_at=close.source.retrieved_at + timedelta(minutes=1),
    )
    late_revision = _observation(
        event_id="event-2",
        available_at=BASE_AVAILABLE_AT + timedelta(days=12),
        occurred_at=close_occurrence,
        supersedes=close.observation_id,
        revision="v2",
    )
    revision_decision = _record(
        ledger,
        late_revision,
        recorded_at=late_revision.source.retrieved_at + timedelta(minutes=1),
    )

    assert close_decision.reasons == (AccrualReason.SEPARATION_WINDOW_NOT_MET,)
    assert revision_decision.reasons == (AccrualReason.SEPARATION_WINDOW_NOT_MET,)
    assert ledger.accrued_event_count == 1


def test_revision_lineage_and_receipt_order_fail_closed(tmp_path: Path) -> None:
    ledger = _ledger(tmp_path)
    first = _observation(source_tier="established_news")
    _record(
        ledger,
        first,
        recorded_at=first.source.retrieved_at + timedelta(minutes=1),
    )

    missing_lineage = _observation(
        available_at=BASE_AVAILABLE_AT + timedelta(hours=1),
        revision="v2",
    )
    with pytest.raises(ValueError, match="supersede the latest"):
        _record(
            ledger,
            missing_lineage,
            recorded_at=missing_lineage.source.retrieved_at + timedelta(minutes=1),
        )

    older = _observation(
        event_id="older-event",
        available_at=BASE_AVAILABLE_AT - timedelta(minutes=1),
    )
    with pytest.raises(ValueError, match="receipt order"):
        _record(
            ledger,
            older,
            recorded_at=BASE_AVAILABLE_AT + timedelta(minutes=2),
        )


def test_recording_time_cannot_move_backwards(tmp_path: Path) -> None:
    ledger = _ledger(tmp_path)
    first = _observation()
    _record(
        ledger,
        first,
        recorded_at=BASE_AVAILABLE_AT + timedelta(hours=2),
    )
    second = _observation(
        event_id="event-2",
        available_at=BASE_AVAILABLE_AT + timedelta(hours=1),
    )

    with pytest.raises(ValueError, match="recording order"):
        _record(
            ledger,
            second,
            recorded_at=BASE_AVAILABLE_AT + timedelta(hours=1, minutes=1),
        )

    assert len(ledger.decisions()) == 1


def test_regional_fraction_requires_point_in_time_official_denominator() -> None:
    payload = _observation_payload(loss_amount="0.06", loss_unit="regional_supply_fraction")
    payload["regional_denominator_source_ref"] = "https://official.example/denominator"
    payload["regional_denominator_source_tier"] = "official"
    payload["regional_denominator_available_at"] = "2026-08-27T00:00:00Z"
    payload["regional_denominator_raw_content_hash"] = sha256(b"denominator").hexdigest()
    payload["observation_id"] = f"candidate-observation-{canonical_hash(_without_id(payload))}"

    observation = candidate_event_observation_from_dict(payload)
    assert observation.loss_amount is not None
    assert observation.loss_amount.as_tuple().exponent == -2

    invalid = deepcopy(payload)
    invalid["regional_denominator_source_tier"] = "primary"
    invalid["observation_id"] = f"candidate-observation-{canonical_hash(_without_id(invalid))}"
    with pytest.raises(ValueError, match="must be official"):
        candidate_event_observation_from_dict(invalid)


def test_ledger_detects_stored_decision_tampering(tmp_path: Path) -> None:
    ledger = _ledger(tmp_path)
    observation = _observation()
    _record(
        ledger,
        observation,
        recorded_at=observation.source.retrieved_at + timedelta(minutes=1),
    )

    with sqlite3.connect(ledger.path) as connection:
        connection.execute(
            "UPDATE accrual_decisions SET decision_hash = ? WHERE sequence = 1",
            ("0" * 64,),
        )

    with pytest.raises(ValueError, match="decision_hash"):
        ledger.decisions()


def test_ledger_requires_and_revalidates_exact_raw_source_bytes(tmp_path: Path) -> None:
    ledger = _ledger(tmp_path)
    observation = _observation()
    receipt = _receipt_for(
        available_at=observation.source.retrieved_at,
        provider_id=observation.source.provider_id,
        raw_content_hash=observation.source.raw_content_hash,
    )

    with pytest.raises(ValueError, match="bytes do not match"):
        ledger.record(
            observation,
            recorded_at=observation.source.retrieved_at + timedelta(minutes=1),
            raw_source=b"wrong",
            coverage_receipt=receipt,
        )
    assert ledger.decisions() == ()

    decision = _record(
        ledger,
        observation,
        recorded_at=observation.source.retrieved_at + timedelta(minutes=1),
    )
    artifact = ledger.source_artifacts.root / observation.source.raw_content_hash
    artifact.write_bytes(b"tampered")

    with pytest.raises(ValueError, match="does not match its identity"):
        ledger.decisions()
    assert decision.disposition is AccrualDisposition.ACCRUED


def test_ledger_rejects_symlink_substitution(tmp_path: Path) -> None:
    registration, registry = load_agent_phase2_preregistration(
        REGISTRATION_PATH,
        REGISTRY_PATH,
    )
    coverage = load_source_coverage_registration(COVERAGE_PATH)
    outside = tmp_path / "outside.sqlite3"
    outside.touch()
    link = tmp_path / "ledger.sqlite3"
    os.symlink(outside, link)

    with pytest.raises(ValueError, match="regular file"):
        AccrualLedger(
            link,
            registration=registration,
            registry=registry,
            coverage_registration=coverage,
            created_at=datetime(2026, 8, 26, 8, tzinfo=UTC),
        )


def test_cli_service_records_and_validates_qualifying_observation(tmp_path: Path) -> None:
    observation = _observation()
    observation_path = tmp_path / "observation.json"
    observation_path.write_text(
        json.dumps(observation.to_dict()),
        encoding="utf-8",
    )
    coverage_receipt_path = tmp_path / "coverage-receipt.json"
    coverage_receipt_path.write_text(
        json.dumps(
            _receipt_for(
                available_at=observation.source.retrieved_at,
                provider_id=observation.source.provider_id,
                raw_content_hash=observation.source.raw_content_hash,
            ).to_dict()
        ),
        encoding="utf-8",
    )
    raw_source_path = tmp_path / "raw-source.bin"
    raw_source_path.write_bytes(b"raw-event-1-v1")
    ledger_path = tmp_path / "ledger.sqlite3"

    recorded = observe_agent_phase2_study(
        registration_path=REGISTRATION_PATH,
        exposure_registry_path=REGISTRY_PATH,
        source_coverage_registration_path=COVERAGE_PATH,
        coverage_receipt_path=coverage_receipt_path,
        observation_path=observation_path,
        raw_source_path=raw_source_path,
        regional_denominator_source_path=None,
        ledger_path=ledger_path,
        recorded_at=BASE_AVAILABLE_AT + timedelta(minutes=1),
    )
    validated = validate_agent_phase2_ledger(
        registration_path=REGISTRATION_PATH,
        exposure_registry_path=REGISTRY_PATH,
        source_coverage_registration_path=COVERAGE_PATH,
        ledger_path=ledger_path,
        inspected_at=BASE_AVAILABLE_AT + timedelta(minutes=2),
    )

    assert recorded["recorded"] is True
    assert recorded["accrued"] is True
    assert recorded["evidence_cutoff_at"] == "2026-08-28T02:00:00Z"
    assert recorded["execution_capability"] == "none"
    assert validated["valid"] is True
    assert validated["accrued_event_count"] == 1
    assert validated["cohort_complete"] is False


def test_main_records_pre_window_observation_without_false_accrual(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    available_at = datetime(2026, 8, 26, 7, 9, 49, tzinfo=UTC)
    observation_path = tmp_path / "pre-window.json"
    observation_path.write_text(
        json.dumps(_observation_payload(available_at=available_at)),
        encoding="utf-8",
    )
    observation = candidate_event_observation_from_dict(
        json.loads(observation_path.read_text(encoding="utf-8"))
    )
    coverage_receipt_path = tmp_path / "coverage-receipt.json"
    coverage_receipt_path.write_text(
        json.dumps(
            _receipt_for(
                available_at=observation.source.retrieved_at,
                provider_id=observation.source.provider_id,
                raw_content_hash=observation.source.raw_content_hash,
            ).to_dict()
        ),
        encoding="utf-8",
    )
    raw_source_path = tmp_path / "raw-source.bin"
    raw_source_path.write_bytes(b"raw-event-1-v1")
    ledger_path = tmp_path / "ledger.sqlite3"

    result = main(
        [
            "agent",
            "study-observe",
            "--registration",
            str(REGISTRATION_PATH),
            "--exposure-registry",
            str(REGISTRY_PATH),
            "--source-coverage-registration",
            str(COVERAGE_PATH),
            "--coverage-receipt",
            str(coverage_receipt_path),
            "--observation",
            str(observation_path),
            "--raw-source",
            str(raw_source_path),
            "--ledger",
            str(ledger_path),
        ]
    )

    assert result == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["recorded"] is True
    assert payload["accrued"] is False
    assert payload["reasons"] == ["outside_accrual_window"]
    assert ledger_path.is_file()


def test_schema_rejects_extra_fields_and_non_actual_availability() -> None:
    payload = _observation_payload()
    payload["unexpected"] = True
    assert validate_agent_contract(payload, "candidate-event-observation.schema.json")

    modeled = _observation_payload()
    source = modeled["source"]
    assert isinstance(source, dict)
    cast(dict[str, object], source)["availability_basis"] = "modeled_latency"
    assert validate_agent_contract(modeled, "candidate-event-observation.schema.json")


def _without_id(payload: dict[str, object]) -> dict[str, object]:
    core = deepcopy(payload)
    core.pop("observation_id")
    return core


def _timestamp(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")
