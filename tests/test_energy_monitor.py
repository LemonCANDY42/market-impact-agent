import json
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

from market_impact_agent.energy_monitor import EnergySourceMonitor
from market_impact_agent.source_coverage import load_source_coverage_registration

COVERAGE_PATH = Path("examples/research/physical-energy-source-coverage-v1.json")


class _TickingClock:
    def __init__(self, start: datetime) -> None:
        self.current = start

    def __call__(self) -> datetime:
        value = self.current
        self.current += timedelta(seconds=1)
        return value


def _umm_record(
    *,
    message_id: str = "message-v1",
    publication: str = "2026-08-28T00:30:00Z",
    unavailability_type: str = "Unplanned",
) -> dict[str, object]:
    return {
        "messageType": "Gas",
        "messageId": message_id,
        "threadId": "thread-1",
        "versionNumber": "1" if message_id == "message-v1" else "2",
        "publicationDateTime": publication,
        "lastUpdateDateTime": "2026-08-28T00:45:00Z",
        "eventStart": "2026-08-28T00:00:00Z",
        "eventStop": "2026-08-30T01:00:00Z",
        "unavailabilityType": unavailability_type,
        "eventType": "Transmission pipeline outage",
        "unavailableCapacity": "1000000000",
        "unitMeasure": "kWh/d",
        "marketParticipantName": "Example TSO",
        "marketParticipantKey": "example-tso",
        "affectedAssetName": "Example pipeline",
    }


def _transport(
    *,
    record: dict[str, object] | None = None,
    fail_provider: str | None = None,
):
    resolved_record = _umm_record() if record is None else record

    def get(
        endpoint: str,
        params: Mapping[str, object],
        timeout_seconds: float,
    ) -> bytes:
        del timeout_seconds
        if "gdeltproject" in endpoint:
            if fail_provider == "gdelt-energy-discovery":
                raise TimeoutError("discovery timed out")
            assert params["timespan"] == "30min"
            return json.dumps({"articles": []}).encode()
        if "eia.gov" in endpoint:
            if fail_provider == "eia-today-in-energy":
                raise TimeoutError("rss timed out")
            assert params == {}
            return b"<rss><channel><item/><item/></channel></rss>"
        if "entsog.eu" in endpoint:
            if fail_provider == "entsog-umm":
                raise TimeoutError("umm timed out")
            assert params["limit"] == 1000
            return json.dumps({"urgentMarketMessages": [resolved_record]}).encode()
        raise AssertionError(f"unexpected endpoint: {endpoint}")

    return get


def build_monitor(
    tmp_path: Path,
    *,
    record: dict[str, object] | None = None,
    fail_provider: str | None = None,
) -> EnergySourceMonitor:
    return EnergySourceMonitor(
        registration=load_source_coverage_registration(COVERAGE_PATH),
        root=tmp_path / "monitor",
        transport=_transport(record=record, fail_provider=fail_provider),
        clock=_TickingClock(datetime(2026, 8, 28, 1, tzinfo=UTC)),
    )


def test_monitor_retains_raw_batches_and_derives_point_in_time_candidate(
    tmp_path: Path,
) -> None:
    registration = load_source_coverage_registration(COVERAGE_PATH)
    cycle = build_monitor(tmp_path).poll()

    assert cycle.receipt.is_complete(registration) is True
    assert cycle.receipt_path.is_file()
    assert cycle.receipt_path.stat().st_mode & 0o777 == 0o600
    assert set(cycle.raw_by_provider) == {
        "gdelt-energy-discovery",
        "eia-today-in-energy",
        "entsog-umm",
    }
    assert len(cycle.candidates) == 1
    observation = cycle.candidates[0]
    assert observation.source.provider_id == "entsog-umm"
    assert observation.event_nature.value == "physical_transport_loss"
    assert observation.loss_amount == Decimal("588275.862068")
    assert observation.expected_duration_hours == Decimal("49.000000")
    assert observation.source.available_at == datetime(2026, 8, 28, 1, 0, 6, tzinfo=UTC)
    assert cycle.raw_source_for(observation) == cycle.raw_by_provider["entsog-umm"]
    assert (cycle.artifact_root / observation.source.raw_content_hash).is_file()


def test_monitor_retains_candidate_but_receipt_marks_mandatory_discovery_failure(
    tmp_path: Path,
) -> None:
    registration = load_source_coverage_registration(COVERAGE_PATH)
    cycle = build_monitor(tmp_path, fail_provider="gdelt-energy-discovery").poll()

    assert cycle.receipt.is_complete(registration) is False
    assert cycle.receipt.attempt("gdelt-energy-discovery").succeeded is False
    assert len(cycle.candidates) == 1


def test_monitor_skips_same_upstream_revision_and_classifies_planned_event(
    tmp_path: Path,
) -> None:
    first = build_monitor(tmp_path).poll()
    observation = first.candidates[0]
    latest = {observation.event_id: observation}

    duplicate = build_monitor(tmp_path).poll(latest_observations=latest)
    planned = build_monitor(
        tmp_path / "planned",
        record=_umm_record(unavailability_type="Planned"),
    ).poll()

    assert duplicate.candidates == ()
    assert planned.candidates[0].event_nature.value == "planned_maintenance"


def test_monitor_fails_closed_when_umm_temporal_fields_are_impossible(
    tmp_path: Path,
) -> None:
    registration = load_source_coverage_registration(COVERAGE_PATH)
    future = _umm_record(publication="2026-08-28T02:00:00Z")
    future["lastUpdateDateTime"] = "2026-08-28T02:00:00Z"

    cycle = build_monitor(tmp_path, record=future).poll()

    attempt = cycle.receipt.attempt("entsog-umm")
    assert attempt.succeeded is False
    assert attempt.error_class == "ValueError"
    assert cycle.receipt.is_complete(registration) is False
    assert cycle.candidates == ()
